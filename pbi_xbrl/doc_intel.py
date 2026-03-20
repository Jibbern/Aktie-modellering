from __future__ import annotations

import json
import hashlib
import html
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .filing_evidence_shared import (
    build_parent_subject_key as _build_parent_subject_key,
    derive_lifecycle_state as _derive_lifecycle_state,
    evidence_role as _evidence_role,
    infer_target_period_norm as _infer_target_period_norm,
    route_to_measurable_promise_candidate as _route_to_measurable_promise_candidate,
    source_class as _source_class,
    statement_class as _statement_class,
    confidence_rank as _confidence_rank,
    coerce_next_quarter_end as _coerce_next_quarter_end,
    extract_document_text as _extract_text,
    filing_quarter_end as _filing_quarter_end,
    format_pct as _pct,
    history_quarter_ends as _history_quarter_ends,
    iter_submission_batches as _iter_submission_batches,
    pick_filing_docs as _pick_filing_docs,
    qualify_promise_candidate as _qualify_promise_candidate,
    split_sentences as _split_sentences,
)
from .pdf_utils import silence_pdfminer_warnings
from .quarter_notes import build_quarter_notes_v2, validate_quarter_notes
from .sec_xbrl import SecClient, normalize_accession, parse_date


CATEGORIES: Dict[str, List[str]] = {
    "Strategy / segment": ["saas", "transition", "exit", "divest", "cost out", "restructur", "segment"],
    "Guidance / targets": ["expects", "guidance", "outlook", "target", "fiscal", "fy", "will be", "plan to"],
    "Debt / refi / covenants": [
        "revolver",
        "revolving credit facility",
        "credit facility",
        "covenant",
        "refinanc",
        "maturity",
        "amend",
        "headroom",
        "liquidity",
    ],
    "One-time items": ["restructuring", "impair", "fx", "foreign currency", "one-time", "special", "redemption"],
    "KPIs (ARR/retention/churn osv om det finns)": ["arr", "arpu", "retention", "churn", "subscriber"],
    "Risks": ["lawsuit", "legal", "usps", "contract", "litigation", "regulatory", "risk"],
}

PROMISE_VERBS = ["expect", "expects", "guidance", "target", "will", "plan", "intend", "aim", "reduce", "increase"]
PROMISE_FORWARD_TERMS = [
    "expect",
    "target",
    "on track",
    "will",
    "by end of",
    "in 2026",
    "this year",
    "next quarter",
]
PROMISE_FORWARD_RE = re.compile(
    r"\b(expect(?:s|ed)?|target(?:s|ed)?|on track|will|by end of|in 20\d{2}|this year|next quarter)\b",
    re.I,
)
COST_SAVINGS_KEY_RE = re.compile(
    r"\b(cost savings|savings initiatives|run[- ]?rate savings|net annualized savings|annualized costs?)\b",
    re.I,
)
PROMISE_BOILERPLATE_RE = re.compile(
    r"\b(forward-looking statements?|private securities litigation reform act|pslra|safe harbor|"
    r"no obligation to update|including, but not limited to|securities act|registration exempt|"
    r"section 3\(a\)\(9\)|cautionary statement|no assurance|may not|indenture|loan documents|"
    r"administrative agent|registration statement|rule 144|risk factors?)\b",
    re.I,
)

NON_GAAP_UNIT_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ("millions", re.compile(r"\(\s*\$\s*millions?\s*\)", re.I | re.S)),
    ("millions", re.compile(r"\(\s*\$?\s*in\s+millions?\s*\)", re.I | re.S)),
    ("millions", re.compile(r"\$\s*millions?\b", re.I | re.S)),
    ("millions", re.compile(r"dollars?\s+in\s+millions?\b", re.I | re.S)),
    (
        "thousands",
        re.compile(
            r"in\s+thousands(?:\s*[,;]\s*except\s+per\s+share\s+amounts?)?",
            re.I | re.S,
        ),
    ),
    ("millions", re.compile(r"in\s+millions(?:\s*,\s*except\s+per\s+share\s+amounts?)?", re.I | re.S)),
]

DOC_TEXT_EXTRACTOR_VERSION = "v2"


@contextmanager
def _timed_substage(stage_timings: Optional[Dict[str, float]], name: str, *, enabled: bool = False):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt_s = time.perf_counter() - t0
        if stage_timings is not None:
            stage_timings[name] = stage_timings.get(name, 0.0) + dt_s
        if enabled:
            print(f"[timing] {name}={dt_s:.2f}s", flush=True)


def _empty_promises_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "promise_id",
            "promise_key",
            "promise_text",
            "metric_tag",
            "target_time",
            "units",
            "created_quarter",
            "first_seen_quarter",
            "last_seen_quarter",
            "first_seen_evidence_quarter",
            "last_seen_evidence_quarter",
            "carried_to_quarter",
            "last_seen_numeric_quarter",
            "last_seen_text_quarter",
            "category",
            "statement",
            "metric",
            "target_value",
            "target_high",
            "target_unit",
            "target_kind",
            "promise_type",
            "guidance_type",
            "target_year",
            "deadline",
            "scorable",
            "soft_promise",
            "target_bucket",
            "qualitative",
            "source_evidence_json",
            "evidence_history_json",
            "evidence_snippet",
            "accn",
            "doc",
            "method",
            "confidence",
            "qa_severity",
            "qa_message",
        ]
    )


def _empty_progress_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "quarter",
            "promise_id",
            "promise_key",
            "source_evidence_json",
            "status",
            "status_score",
            "rationale",
            "metric_refs",
            "actual",
            "target",
            "target_bucket",
            "promise_type",
            "scorable",
            "guidance_type",
            "deadline",
            "status_changed",
            "status_history_json",
            "first_seen_evidence_quarter",
            "last_seen_evidence_quarter",
            "last_seen_numeric_quarter",
            "last_seen_text_quarter",
            "carried_to_quarter",
            "numeric_update_this_quarter",
            "qa_severity",
            "qa_message",
        ]
    )

TEXT_SIGNAL_PATTERNS: Dict[str, re.Pattern[str]] = {
    "Guidance": re.compile(r"\b(expects?|target|guidance|outlook|forecast|will|plan(?:s|ned)? to)\b", re.I),
    "One-offs": re.compile(r"\b(restructur\w*|headwind|one[- ]?time|special|impair\w*|redemption)\b", re.I),
    "Margin": re.compile(r"\b(pricing|margin|cost (?:out|save)|efficien\w*)\b", re.I),
    "Revenue": re.compile(r"\b(revenue|demand|volume|contract|growth|decline)\b", re.I),
    "Risks": re.compile(r"\b(litigation|lawsuit|regulatory|contract risk|headwind|uncertain)\b", re.I),
}

TOPIC_TO_CATEGORY: Dict[str, str] = {
    "Debt": "Debt / refi / covenants",
    "Margin": "Strategy / segment",
    "Revenue": "Strategy / segment",
    "FCF": "Strategy / segment",
    "Guidance": "Guidance / targets",
    "One-offs": "One-time items",
    "Risks": "Risks",
    "Equity": "Strategy / segment",
}

TOPIC_MATERIALITY: Dict[str, float] = {
    "Debt": 35.0,
    "Margin": 30.0,
    "Revenue": 30.0,
    "FCF": 30.0,
    "Guidance": 20.0,
    "One-offs": 20.0,
    "Risks": 20.0,
    "Equity": 20.0,
}


@dataclass
class NoteCandidate:
    note_id: str
    quarter_end: date
    topic: str
    metric: str
    headline: str
    body: str
    severity_score: float
    confidence: str
    evidence: List[Dict[str, Any]]
    method: str
    metric_value: Optional[float] = None
def _doc_text_cache_path(
    pdf_path: Path,
    cache_root: Optional[Path],
    *,
    extractor_version: str = DOC_TEXT_EXTRACTOR_VERSION,
) -> Optional[Path]:
    if cache_root is None:
        return None
    try:
        st = pdf_path.stat()
    except Exception:
        return None
    cache_dir = Path(cache_root) / "doc_text_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    key_src = f"{pdf_path.name}|{int(st.st_size)}|{int(st.st_mtime)}|{extractor_version}"
    key = hashlib.sha1(key_src.encode("utf-8", errors="ignore")).hexdigest()
    return cache_dir / f"{key}.txt"


def _extract_pdf_text_cached(
    pdf_path: Path,
    *,
    cache_root: Optional[Path] = None,
    rebuild_cache: bool = False,
    quiet_pdf_warnings: bool = True,
) -> str:
    cache_path = _doc_text_cache_path(pdf_path, cache_root)
    if cache_path is not None and cache_path.exists() and not rebuild_cache:
        try:
            return cache_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            pass
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ""
    txt = ""
    try:
        with silence_pdfminer_warnings(enabled=quiet_pdf_warnings):
            with pdfplumber.open(str(pdf_path)) as pdf:
                txt = "\n".join((pg.extract_text() or "") for pg in pdf.pages)
    except Exception:
        txt = ""
    if cache_path is not None and txt is not None:
        try:
            cache_path.write_text(txt, encoding="utf-8", errors="ignore")
        except Exception:
            pass
    return txt


def extract_pdf_text_cached(
    pdf_path: Path,
    *,
    cache_root: Optional[Path] = None,
    rebuild_cache: bool = False,
    quiet_pdf_warnings: bool = True,
) -> str:
    return _extract_pdf_text_cached(
        pdf_path,
        cache_root=cache_root,
        rebuild_cache=rebuild_cache,
        quiet_pdf_warnings=quiet_pdf_warnings,
    )


def _norm_sentence(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower()).strip()


def _is_boilerplate_sentence(sentence: str) -> bool:
    s = str(sentence or "").strip()
    if not s:
        return False
    return bool(PROMISE_BOILERPLATE_RE.search(s))


def _is_historical_results_sentence(sentence: str) -> bool:
    s = str(sentence or "").strip().lower()
    if not s:
        return False
    if not re.search(r"\bresults?\s+for\s+the\s+(first|second|third|fourth)\s+quarter\b", s):
        return False
    if re.search(r"\b(expect|target|guidance|outlook|plan|intend|next year|next quarter|full year|fy\s*20\d{2}|in\s+20\d{2})\b", s):
        return False
    return True


def _has_cost_savings_numeric(parsed: Dict[str, Any]) -> bool:
    if str(parsed.get("metric") or "") != "cost_savings_run_rate":
        return False
    if pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce")):
        return True
    if pd.notna(pd.to_numeric(parsed.get("target_high"), errors="coerce")):
        return True
    if pd.notna(pd.to_numeric(parsed.get("observed_runrate"), errors="coerce")):
        return True
    if pd.notna(pd.to_numeric(parsed.get("observed_increment"), errors="coerce")):
        return True
    return False


def _quarter_end_from_qn_year(qn: int, year: int) -> Optional[date]:
    if qn == 1:
        return date(year, 3, 31)
    if qn == 2:
        return date(year, 6, 30)
    if qn == 3:
        return date(year, 9, 30)
    if qn == 4:
        return date(year, 12, 31)
    return None


def _infer_quarter_from_filename(name: str) -> Optional[date]:
    s = str(name or "")
    if not s:
        return None
    m = re.search(r"\bq\s*([1-4])[\s_\-]*(20\d{2})\b", s, re.I)
    if not m:
        return None
    return _quarter_end_from_qn_year(int(m.group(1)), int(m.group(2)))


def _sentence_score(sentence: str, keywords: List[str]) -> int:
    s = sentence.lower()
    hits = sum(1 for k in keywords if k in s)
    if hits == 0:
        return 0
    has_num = bool(re.search(r"[$%]|\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b", sentence))
    return hits * 3 + (2 if has_num else 0) + (1 if re.search(r"expect|guidance|target|covenant|restructur|refinanc|maturity|risk", s) else 0)


def _build_quarter_notes(
    sec: SecClient,
    cik_int: int,
    submissions: Dict[str, Any],
    hist: pd.DataFrame,
    max_docs: int = 80,
    max_quarters: int = 24,
) -> pd.DataFrame:
    hq = _history_quarter_ends(hist, max_quarters=max_quarters)
    if not hq:
        return _empty_promises_df()
    target_quarters = set(hq)
    min_q = min(target_quarters)

    cand_rows: List[Dict[str, Any]] = []
    docs_scanned = 0
    min_filing_date = datetime.utcnow().date() - timedelta(days=365 * 8)

    for batch in _iter_submission_batches(sec, submissions):
        forms = batch.get("form", []) or []
        accns = batch.get("accessionNumber", []) or []
        reports = batch.get("reportDate", []) or []
        filed = batch.get("filingDate", []) or []
        primary = batch.get("primaryDocument", []) or []
        n = min(len(forms), len(accns))
        for i in range(n):
            form = str(forms[i] or "")
            if form not in ("8-K", "8-K/A"):
                continue
            accn = str(accns[i] or "")
            rep = reports[i] if i < len(reports) else None
            fdt = filed[i] if i < len(filed) else None
            filed_d = parse_date(fdt)
            if filed_d and filed_d < min_filing_date:
                continue
            q_end = _filing_quarter_end(form, rep, filed_d)
            if q_end is None or q_end < min_q:
                continue
            if q_end not in target_quarters:
                continue

            accn_nd = normalize_accession(accn)
            try:
                idx = sec.accession_index_json(cik_int, accn_nd)
            except Exception:
                idx = {}
            items = idx.get("directory", {}).get("item", []) if isinstance(idx, dict) else []
            pdn = primary[i] if i < len(primary) else None
            docs = _pick_filing_docs(pdn, items, max_docs=8, penalize_admin_docs=True)

            for doc in docs:
                if docs_scanned >= max_docs:
                    break
                try:
                    blob = sec.download_document(cik_int, accn_nd, doc)
                except Exception:
                    continue
                txt = _extract_text(doc, blob)
                if len(txt) < 200 and str(doc).lower().endswith((".htm", ".html", ".xhtml")):
                    try:
                        ocr_txt = sec.ocr_html_assets(
                            accn_nd,
                            blob,
                            context={
                                "doc": doc,
                                "quarter": q_end.isoformat() if q_end is not None else "",
                                "purpose": "doc_intel_quarter_notes",
                                "report_date": str(rep or ""),
                                "filing_date": str(filed_d or ""),
                            },
                        )
                    except Exception:
                        ocr_txt = ""
                    if len(ocr_txt or "") > len(txt):
                        txt = re.sub(r"\s+", " ", html.unescape(str(ocr_txt))).strip()
                if len(txt) < 200:
                    continue
                docs_scanned += 1
                sentences = _split_sentences(txt)
                for sent in sentences:
                    norm = _norm_sentence(sent)
                    for cat, keywords in CATEGORIES.items():
                        score = _sentence_score(sent, keywords)
                        if score <= 0:
                            continue
                        method = "liquidity_diff" if cat == "Debt / refi / covenants" else "mda_diff"
                        cand_rows.append(
                            {
                                "quarter": q_end,
                                "category": cat,
                                "sentence": sent,
                                "norm": norm,
                                "score": score,
                                "accn": accn,
                                "form": form,
                                "doc": doc,
                                "method": method,
                            }
                        )
            if docs_scanned >= max_docs:
                break
        if docs_scanned >= max_docs:
            break

    if not cand_rows:
        return pd.DataFrame()
    cand = pd.DataFrame(cand_rows)
    cand["quarter"] = pd.to_datetime(cand["quarter"], errors="coerce")
    cand = cand[cand["quarter"].notna()].sort_values(["quarter", "category", "score"], ascending=[True, True, False])
    if cand.empty:
        return pd.DataFrame()

    out_rows: List[Dict[str, Any]] = []
    prev_norm_by_cat: Dict[str, set[str]] = {}
    for q in sorted(cand["quarter"].unique()):
        cq = cand[cand["quarter"] == q]
        for cat in CATEGORIES.keys():
            sub = cq[cq["category"] == cat].copy()
            if sub.empty:
                continue
            sub = sub.drop_duplicates(subset=["norm"], keep="first")
            prev_norm = prev_norm_by_cat.get(cat, set())
            sub_new = sub[~sub["norm"].isin(prev_norm)]
            pick = sub_new if not sub_new.empty else sub
            pick = pick.sort_values("score", ascending=False).head(3)
            for _, r in pick.iterrows():
                sent = str(r["sentence"])
                out_rows.append(
                    {
                        "quarter": pd.Timestamp(q).date(),
                        "category": cat,
                        "note": sent[:180],
                        "evidence_snippet": sent[:380],
                        "accn": r["accn"],
                        "form": r["form"],
                        "doc": r["doc"],
                        "method": r["method"],
                    }
                )
            prev_norm_by_cat[cat] = set(sub["norm"].head(120).tolist())

    out = pd.DataFrame(out_rows)
    if out.empty:
        return out
    out = out.sort_values(["quarter", "category"]).reset_index(drop=True)
    return out


def _parse_target(statement: str) -> Tuple[Optional[float], Optional[str], Optional[int]]:
    s = statement.lower()
    target_year = None
    m_y = re.search(r"\b(20\d{2})\b", s)
    if m_y:
        target_year = int(m_y.group(1))

    m_x = re.search(r"(\d+(?:\.\d+)?)\s*x\b", s)
    if m_x:
        return float(m_x.group(1)), "x", target_year

    m_pct = re.search(r"(\d+(?:\.\d+)?)\s*%", s)
    if m_pct:
        return float(m_pct.group(1)), "%", target_year

    m_money = re.search(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*(million|billion|m|bn)?", s)
    if m_money:
        v = float(m_money.group(1).replace(",", ""))
        unit = (m_money.group(2) or "").lower()
        if unit in ("billion", "bn"):
            v *= 1_000_000_000
            return v, "USD", target_year
        if unit in ("million", "m"):
            v *= 1_000_000
            return v, "USD", target_year
        return v, "raw", target_year
    return None, None, target_year


def _build_promises(
    quarter_notes: pd.DataFrame,
    sec: Optional[SecClient],
    cik_int: int,
    submissions: Dict[str, Any],
    hist: pd.DataFrame,
    earnings_release_dir: Optional[Path] = None,
    max_docs: int = 80,
    max_quarters: int = 24,
    cache_dir: Optional[Path] = None,
    rebuild_doc_text_cache: bool = False,
    quiet_pdf_warnings: bool = True,
    doc_registry: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    hq = _history_quarter_ends(hist, max_quarters=max_quarters)
    if not hq:
        return pd.DataFrame()
    target_quarters = set(hq)

    docs_df = _extract_promise_candidates_from_docs(
        sec=sec,
        cik_int=cik_int,
        submissions=submissions,
        target_quarters=target_quarters,
        max_docs=max_docs,
        quiet_pdf_warnings=quiet_pdf_warnings,
        doc_registry=doc_registry,
    )
    qn_df = _extract_promise_candidates_from_notes(quarter_notes)
    er_df = _extract_promise_candidates_from_local_release_dir(
        release_dir=earnings_release_dir,
        target_quarters=target_quarters,
        cache_dir=cache_dir,
        rebuild_doc_text_cache=rebuild_doc_text_cache,
        quiet_pdf_warnings=quiet_pdf_warnings,
        doc_registry=doc_registry,
    )
    parts = [x for x in [docs_df, qn_df, er_df] if x is not None and not x.empty]
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        return _empty_promises_df()
    return _dedupe_promises(df)
def _fuzzy_ratio(a: str, b: str) -> float:
    aa = str(a or "")
    bb = str(b or "")
    if not aa or not bb:
        return 0.0
    try:
        from rapidfuzz import fuzz  # type: ignore

        return float(fuzz.ratio(aa, bb))
    except Exception:
        from difflib import SequenceMatcher

        return float(SequenceMatcher(None, aa, bb).ratio() * 100.0)


def _keyword_hits(text: str) -> set[str]:
    toks = re.findall(r"[a-z]{3,}", str(text or "").lower())
    keep = {
        "revenue",
        "margin",
        "ebitda",
        "debt",
        "leverage",
        "delever",
        "fcf",
        "guidance",
        "target",
        "expect",
        "track",
        "reduction",
        "run",
        "rate",
        "recurring",
        "2026",
        "2027",
    }
    return {t for t in toks if t in keep}


def _source_evidence(candidate: Dict[str, Any]) -> Dict[str, Any]:
    q_raw = candidate.get("quarter")
    q_ts = pd.to_datetime(q_raw, errors="coerce")
    q_iso = pd.Timestamp(q_ts).date().isoformat() if pd.notna(q_ts) else ""
    return {
        "doc_path": candidate.get("doc_path") or candidate.get("doc"),
        "doc_type": candidate.get("doc_type") or candidate.get("form") or "filing",
        "section_or_page": candidate.get("section_or_page") or candidate.get("method") or "unknown",
        "source_doc_end": q_iso,
        "snippet": str(candidate.get("evidence_snippet") or candidate.get("statement") or "")[:380],
        "metric": candidate.get("metric"),
        "target_low": candidate.get("target_value"),
        "target_high": candidate.get("target_high"),
        "observed_runrate": candidate.get("observed_runrate"),
        "observed_increment": candidate.get("observed_increment"),
        "mention_kind": candidate.get("mention_kind"),
    }


def _parse_money_target(s: str) -> Optional[float]:
    m = re.search(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?", s, re.I)
    if not m:
        return None
    v = float(m.group(1).replace(",", ""))
    unit = (m.group(2) or "").lower()
    if unit in ("billion", "bn"):
        return v * 1_000_000_000
    if unit in ("million", "m"):
        return v * 1_000_000
    return v


def _money_token_to_usd(value_raw: str, unit_raw: Optional[str]) -> Optional[float]:
    try:
        val = float(str(value_raw).replace(",", ""))
    except Exception:
        return None
    unit = str(unit_raw or "").lower().strip()
    if unit in ("billion", "bn"):
        return val * 1_000_000_000
    if unit in ("million", "m"):
        return val * 1_000_000
    # In cost-savings guidance, plain numbers are almost always $m-scale.
    if abs(val) <= 10_000:
        return val * 1_000_000
    return val


def _parse_cost_savings_range_target(statement: str) -> Tuple[Optional[float], Optional[float]]:
    s = str(statement or "")
    if not COST_SAVINGS_KEY_RE.search(s):
        return None, None
    pat = re.compile(
        r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*(million|billion|m|bn)?\s*(?:to|[-–—])\s*"
        r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*(million|billion|m|bn)?",
        re.I,
    )
    best: Optional[Tuple[float, float]] = None
    best_mid = -1.0
    for m in pat.finditer(s):
        lo = _money_token_to_usd(m.group(1), m.group(2))
        hi = _money_token_to_usd(m.group(3), m.group(4) or m.group(2))
        if lo is None or hi is None:
            continue
        lo_f = float(min(lo, hi))
        hi_f = float(max(lo, hi))
        mid = (lo_f + hi_f) / 2.0
        if mid > best_mid:
            best_mid = mid
            best = (lo_f, hi_f)
    if best is None:
        return None, None
    return best


def _parse_cost_savings_runrate_point(statement: str) -> Optional[float]:
    s = str(statement or "")
    if not COST_SAVINGS_KEY_RE.search(s):
        return None
    pat = re.compile(
        r"(run[- ]?rate|net annualized savings)[^.]{0,120}?\b(?:to|at|of|is|now)\s*"
        r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*(million|billion|m|bn)?",
        re.I,
    )
    m = pat.search(s)
    if not m:
        return None
    return _money_token_to_usd(m.group(2), m.group(3))


def _parse_cost_savings_increment(statement: str) -> Optional[float]:
    s = str(statement or "")
    if not COST_SAVINGS_KEY_RE.search(s):
        return None
    pat = re.compile(
        r"\b(eliminated|realized|achieved|delivered)\b[^.]{0,60}?\$?\s*"
        r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*(million|billion|m|bn)?[^.]{0,80}?\b(annualized|run[- ]?rate)\b",
        re.I,
    )
    m = pat.search(s)
    if not m:
        return None
    return _money_token_to_usd(m.group(2), m.group(3))


def _parse_deadline(statement: str, q_end: Optional[date]) -> Optional[date]:
    s = statement.lower()
    m = re.search(
        r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+([0-9]{1,2}),\s*(20\d{2})\b",
        s,
        re.I,
    )
    if m:
        mon = m.group(1).lower()
        day = int(m.group(2))
        yr = int(m.group(3))
        months = {
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }
        try:
            return date(yr, months[mon], day)
        except Exception:
            pass
    m = re.search(r"\b(?:by\s+end\s+of|in)\s+(20\d{2})\b", s)
    if m:
        return date(int(m.group(1)), 12, 31)
    m = re.search(r"\bq([1-4])\s*(20\d{2})\b", s)
    if m:
        qn = int(m.group(1))
        yr = int(m.group(2))
        return date(yr, 3, 31) if qn == 1 else date(yr, 6, 30) if qn == 2 else date(yr, 9, 30) if qn == 3 else date(yr, 12, 31)
    if q_end is None:
        return None
    if "over the next 12 months" in s or "in the next 12 months" in s or "over the next year" in s:
        return _coerce_next_quarter_end(date(q_end.year + 1, q_end.month, q_end.day))
    if "this year" in s:
        return date(q_end.year, 12, 31)
    if "next year" in s:
        return date(q_end.year + 1, 12, 31)
    if "next quarter" in s:
        return _coerce_next_quarter_end(q_end + timedelta(days=1))
    return None


def _target_bucket(metric: Optional[str], target_value: Optional[float], target_unit: Optional[str], target_kind: Optional[str]) -> Optional[str]:
    if metric is None or target_value is None:
        return None
    if metric in {"revenue_yoy", "adj_ebitda_margin_ttm", "corporate_net_leverage"}:
        bps = round((float(target_value) * 10_000.0) / 50.0) * 50
        return f"bps:{int(bps)}:{target_kind or ''}"
    if str(target_unit or "").upper() == "USD":
        step = 25_000_000.0
        b = round(float(target_value) / step) * step
        return f"usd:{int(b)}:{target_kind or ''}"
    return f"raw:{round(float(target_value), 4)}:{target_kind or ''}"


def _promise_guidance_bucket(metric: str, category: str, statement: str) -> str:
    m = str(metric or "").lower()
    c = str(category or "").lower()
    s = str(statement or "").lower()
    if "cost_savings_run_rate" in m or "program" in c:
        return "run-rate"
    if "leverage" in m or "net_debt" in m or "debt" in m:
        return "ratio"
    if re.search(r"\bone[- ]?time\b[^.]{0,24}\b(charge|charges|cost|costs)\b", s, re.I):
        return "one-time"
    return "period"


def _deadline_bucket(deadline_v: Any, target_year: Any) -> str:
    d = pd.to_datetime(deadline_v, errors="coerce")
    if pd.notna(d):
        q = pd.Timestamp(d).to_period("Q")
        return f"Q{int(q.year)}Q{int(q.quarter)}"
    y = pd.to_numeric(target_year, errors="coerce")
    if pd.notna(y):
        return f"FY{int(y)}"
    return "NA"


def _canonical_promise_key(
    metric: Any,
    target_kind: Any,
    target_low: Any,
    target_high: Any,
    target_unit: Any,
    deadline_v: Any,
    target_year: Any,
    guidance_bucket: Any,
) -> str:
    m = str(metric or "").strip().lower()
    kind = str(target_kind or "").strip().lower()
    unit = str(target_unit or "").strip().lower()
    g_bucket = str(guidance_bucket or "").strip().lower()
    lo = pd.to_numeric(target_low, errors="coerce")
    hi = pd.to_numeric(target_high, errors="coerce")
    if pd.isna(lo) and pd.notna(hi):
        lo = hi
    if pd.notna(lo) and pd.notna(hi):
        lo_f = float(min(float(lo), float(hi)))
        hi_f = float(max(float(lo), float(hi)))
        if unit == "usd":
            tgt = f"{int(round(lo_f / 1_000_000))}:{int(round(hi_f / 1_000_000))}:m"
        else:
            tgt = f"{round(lo_f, 6)}:{round(hi_f, 6)}"
    elif pd.notna(lo):
        lo_f = float(lo)
        if unit == "usd":
            tgt = f"{int(round(lo_f / 1_000_000))}:m"
        else:
            tgt = f"{round(lo_f, 6)}"
    else:
        tgt = "na"
    period_bucket = _deadline_bucket(deadline_v, target_year)
    seed = f"{m}|{kind}|{tgt}|{period_bucket}|{g_bucket}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]


def _parse_promise_candidate(statement: str, q_end: Optional[date]) -> Dict[str, Any]:
    s = statement.lower()
    out: Dict[str, Any] = {
        "metric": None,
        "target_value": None,
        "target_high": None,
        "target_unit": None,
        "target_kind": None,
        "promise_type": "operational",
        "category": "Guidance / targets",
        "observed_runrate": None,
        "observed_increment": None,
    }
    deadline = _parse_deadline(statement, q_end)
    out["deadline"] = deadline
    out["target_year"] = deadline.year if deadline else None

    pct_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%", s)
    bps_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*bps", s)
    x_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*x\b", s)
    money = _parse_money_target(statement)

    cs_low, cs_high = _parse_cost_savings_range_target(statement)
    cs_runrate = _parse_cost_savings_runrate_point(statement)
    cs_incr = _parse_cost_savings_increment(statement)
    if COST_SAVINGS_KEY_RE.search(statement):
        has_target_ctx = bool(
            re.search(r"\b(target|guidance|outlook|expect(?:s|ed)?|plan(?:s|ned)?|increase(?:d|ing)?)\b", s)
        )
        has_annualized_ctx = bool(re.search(r"\b(annualized|run[- ]?rate|net annualized)\b", s))
        row_style_ctx = bool(re.match(r"^\s*(cost savings|net annualized cost savings|annualized cost savings)\b", s))
        has_year_ctx = bool(re.search(r"\b20\d{2}\b", s))
        allow_target_range = bool(
            cs_low is not None
            and (has_annualized_ctx or row_style_ctx)
            and (has_target_ctx or row_style_ctx or has_year_ctx)
        )
        out["metric"] = "cost_savings_run_rate"
        out["category"] = "Programs / initiatives"
        out["target_unit"] = "USD"
        out["observed_runrate"] = cs_runrate
        out["observed_increment"] = cs_incr
        if allow_target_range:
            out["target_value"] = float(cs_low)
            out["target_high"] = float(cs_high) if cs_high is not None else None
            out["target_kind"] = "gte_abs"
        if out.get("deadline") is None and q_end is not None and (
            "next year" in s or "next 12 months" in s or "over the next year" in s
        ):
            out["deadline"] = _coerce_next_quarter_end(date(q_end.year + 1, q_end.month, q_end.day))
            out["target_year"] = out["deadline"].year if out.get("deadline") else None
    elif ("leverage" in s or "delever" in s) and x_m:
        out.update({
            "metric": "corporate_net_leverage",
            "target_value": float(x_m.group(1)),
            "target_unit": "x",
            "target_kind": "lte_abs",
            "category": "Debt / refi / covenants",
        })
    elif any(k in s for k in ["delever", "debt reduction", "reduce debt", "pay down debt", "net debt"]):
        out["metric"] = "corporate_net_debt"
        out["category"] = "Debt / refi / covenants"
        if pct_m:
            out["target_value"] = float(pct_m.group(1)) / 100.0
            out["target_unit"] = "ratio"
            out["target_kind"] = "delta_down_pct"
        elif money is not None:
            out["target_value"] = float(money)
            out["target_unit"] = "USD"
            out["target_kind"] = "delta_down_abs"
    elif any(k in s for k in ["buyback", "share repurchase", "repurchase", "dividend", "capital allocation", "return capital"]):
        out["metric"] = "capital_allocation"
        out["category"] = "Programs / initiatives"
        if money is not None:
            out["target_value"] = float(abs(money))
            out["target_unit"] = "USD"
            out["target_kind"] = "gte_abs"
        elif pct_m:
            out["target_value"] = float(abs(float(pct_m.group(1)) / 100.0))
            out["target_unit"] = "ratio"
            out["target_kind"] = "gte_abs"
    elif any(k in s for k in ["decline", "stabilize", "stable", "flat", "revenue", "sales"]):
        out["metric"] = "revenue_yoy"
        out["category"] = "Guidance / targets"
        if "flat" in s or "stabilize" in s or "stable" in s or "0%" in s:
            out["target_value"] = 0.0
            out["target_unit"] = "ratio"
            out["target_kind"] = "abs_le"
        elif pct_m:
            pct = float(pct_m.group(1)) / 100.0
            out["target_value"] = -abs(pct) if ("decline" in s or "down" in s) else abs(pct)
            out["target_unit"] = "ratio"
            out["target_kind"] = "gte_abs"
    elif any(k in s for k in ["margin expansion", "margin", "profitability", "improve profitability"]):
        out["metric"] = "adj_ebitda_margin_ttm"
        out["category"] = "Guidance / targets"
        if bps_m:
            out["target_value"] = float(bps_m.group(1)) / 10_000.0
            out["target_unit"] = "ratio"
            out["target_kind"] = "delta_up_abs"
        elif pct_m:
            out["target_value"] = float(pct_m.group(1)) / 100.0
            out["target_unit"] = "ratio"
            out["target_kind"] = "gte_abs"
    elif re.search(r"\b(complete|completed|launch|launched|close|closed|finalize|implemented|deliver|delivered|online|ramping|fully operational|begin|began|execute|executed|commissioning)\b", s) and deadline is not None:
        out["metric"] = "milestone"
        out["category"] = "Programs / initiatives"
        out["target_value"] = 1.0
        out["target_unit"] = "event"
        out["target_kind"] = "gte_abs"
        out["promise_type"] = "milestone"

    out["scorable"] = bool(out.get("metric") and out.get("deadline") and out.get("target_value") is not None and out.get("target_kind"))
    out["soft_promise"] = not out["scorable"]
    out["target_bucket"] = _target_bucket(
        out.get("metric"),
        out.get("target_value"),
        out.get("target_unit"),
        out.get("target_kind"),
    )
    return out


def _extract_promise_candidates_from_docs(
    sec: Optional[SecClient],
    cik_int: int,
    submissions: Dict[str, Any],
    target_quarters: set[date],
    max_docs: int = 80,
    quiet_pdf_warnings: bool = True,
    doc_registry: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if sec is None:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    docs_scanned = 0
    min_filing_date = datetime.utcnow().date() - timedelta(days=365 * 8)
    for batch in _iter_submission_batches(sec, submissions):
        forms = batch.get("form", []) or []
        accns = batch.get("accessionNumber", []) or []
        reports = batch.get("reportDate", []) or []
        filed = batch.get("filingDate", []) or []
        primary = batch.get("primaryDocument", []) or []
        n = min(len(forms), len(accns))
        for i in range(n):
            form = str(forms[i] or "")
            if form not in ("10-Q", "10-Q/A", "10-K", "10-K/A", "8-K", "8-K/A"):
                continue
            accn = str(accns[i] or "")
            rep = reports[i] if i < len(reports) else None
            fdt = filed[i] if i < len(filed) else None
            filed_d = parse_date(fdt)
            if filed_d and filed_d < min_filing_date:
                continue
            q_end = _filing_quarter_end(form, rep, filed_d)
            if q_end is None or q_end not in target_quarters:
                continue
            accn_nd = normalize_accession(accn)
            try:
                idx = sec.accession_index_json(cik_int, accn_nd)
            except Exception:
                idx = {}
            items = idx.get("directory", {}).get("item", []) if isinstance(idx, dict) else []
            pdn = primary[i] if i < len(primary) else None
            docs = _pick_filing_docs(pdn, items, max_docs=8, penalize_admin_docs=True)

            for doc in docs:
                if docs_scanned >= max_docs:
                    break
                try:
                    blob = sec.download_document(cik_int, accn_nd, doc)
                except Exception:
                    continue
                txt = _extract_text(doc, blob, quiet_pdf_warnings=quiet_pdf_warnings)
                if len(txt) < 180 and str(doc).lower().endswith((".htm", ".html", ".xhtml")):
                    try:
                        ocr_txt = sec.ocr_html_assets(
                            accn_nd,
                            blob,
                            context={
                                "doc": doc,
                                "quarter": q_end.isoformat() if q_end is not None else "",
                                "purpose": "doc_intel_promises",
                                "report_date": str(rep or ""),
                                "filing_date": str(filed_d or ""),
                            },
                        )
                    except Exception:
                        ocr_txt = ""
                    if len(ocr_txt or "") > len(txt):
                        txt = re.sub(r"\s+", " ", html.unescape(str(ocr_txt))).strip()
                if len(txt) < 180:
                    continue
                docs_scanned += 1
                conf = "high" if form.startswith(("10-Q", "10-K")) else ("med" if form.startswith("8-K") else "low")
                safe_doc = re.sub(r"[^\w\-.]+", "_", str(doc or "doc"))
                doc_path = str(sec.cache_dir / f"doc_{accn_nd}_{safe_doc}")
                if doc_registry is not None:
                    _doc_registry_seed_text(doc_path, txt, doc_registry)
                    doc_path, _, _ = _doc_registry_paths(doc_path, doc_registry)
                    doc_path = doc_path or str(sec.cache_dir / f"doc_{accn_nd}_{safe_doc}")
                doc_type = "pdf" if str(doc).lower().endswith(".pdf") else "html"
                for sent in _split_sentences(txt):
                    has_forward = bool(PROMISE_FORWARD_RE.search(sent))
                    has_cost_signal = bool(COST_SAVINGS_KEY_RE.search(sent))
                    if not has_forward and not has_cost_signal:
                        continue
                    qualified = _qualify_promise_candidate(sent, source_type=doc_type or form, metric_hint="")
                    if qualified is None:
                        continue
                    parsed = _parse_promise_candidate(sent, q_end)
                    if qualified.scope == "hard_target" and not (
                        parsed.get("metric")
                        and parsed.get("deadline") is not None
                        and (
                            pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce"))
                            or pd.notna(pd.to_numeric(parsed.get("target_high"), errors="coerce"))
                            or _has_cost_savings_numeric(parsed)
                        )
                    ):
                        continue
                    if qualified.scope == "clean_milestone" and not (
                        str(parsed.get("promise_type") or "").strip().lower() == "milestone"
                        and parsed.get("deadline") is not None
                    ):
                        continue
                    if has_cost_signal and parsed.get("metric") == "cost_savings_run_rate":
                        parsed["category"] = "Programs / initiatives"
                        if not _has_cost_savings_numeric(parsed):
                            continue
                    if _is_historical_results_sentence(sent) and not parsed.get("scorable"):
                        continue
                    if _is_boilerplate_sentence(sent):
                        has_numeric_target = pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce"))
                        if not has_numeric_target and not _has_cost_savings_numeric(parsed):
                            continue
                    subject_meta = _promise_subject_metadata(
                        text=sent,
                        quarter_end=q_end,
                        source_type=doc_type or form,
                        source_doc=doc_path,
                        metric_hint="",
                        parsed=parsed,
                        qualified_scope=qualified.scope,
                        base_score=_confidence_rank(conf),
                    )
                    rows.append(
                        {
                            "quarter": pd.Timestamp(q_end),
                            "category": parsed.get("category"),
                            "statement": sent[:300],
                            "statement_norm": _norm_sentence(sent),
                            "metric": parsed.get("metric"),
                            "target_value": parsed.get("target_value"),
                            "target_high": parsed.get("target_high"),
                            "target_unit": parsed.get("target_unit"),
                            "target_kind": parsed.get("target_kind"),
                            "promise_type": parsed.get("promise_type") or "operational",
                            "target_year": parsed.get("target_year"),
                            "deadline": parsed.get("deadline"),
                            "observed_runrate": parsed.get("observed_runrate"),
                            "observed_increment": parsed.get("observed_increment"),
                            "scorable": bool(parsed.get("scorable")),
                            "soft_promise": bool(parsed.get("soft_promise")),
                            "target_bucket": parsed.get("target_bucket"),
                            "evidence_snippet": sent[:380],
                            "accn": accn,
                            "form": form,
                            "doc": doc,
                            "doc_path": doc_path,
                            "doc_type": doc_type,
                            "section_or_page": "doc_scan",
                            "method": "doc_scan",
                            "confidence": conf,
                            "candidate_scope": qualified.scope,
                            "statement_summary": qualified.summary,
                            "preferred_narrative_source": bool(qualified.preferred_source),
                            **subject_meta,
                        }
                    )
            if docs_scanned >= max_docs:
                break
        if docs_scanned >= max_docs:
            break
    return pd.DataFrame(rows)


def _promise_subject_metadata(
    *,
    text: str,
    quarter_end: date,
    source_type: Any,
    source_doc: Any,
    metric_hint: Any,
    parsed: Dict[str, Any],
    qualified_scope: str,
    base_score: float = 0.0,
) -> Dict[str, Any]:
    period_norm = _infer_target_period_norm(
        period_norm=parsed.get("target_period_norm"),
        deadline=parsed.get("deadline"),
        target_year=parsed.get("target_year"),
        quarter=quarter_end,
        text=text,
    )
    source_class_txt = _source_class(source_type)
    statement_class_txt = _statement_class(text, source_type=source_type, metric_hint=parsed.get("metric") or metric_hint or "")
    routed = _route_to_measurable_promise_candidate(
        text,
        quarter=quarter_end,
        source_type=source_type,
        metric_hint=parsed.get("metric") or metric_hint or "",
        source_doc=source_doc,
        target_period_norm=period_norm,
        promise_type_hint=qualified_scope,
        base_score=base_score,
    )
    if routed is None:
        return {
            "candidate_type": "measurable_promise_candidate",
            "source_class": source_class_txt,
            "statement_class": statement_class_txt,
            "metric_family": "",
            "entity_scope": "",
            "target_period_norm": period_norm,
            "parent_subject_key": "",
            "canonical_subject_key": "",
            "lifecycle_subject_key": "",
            "promise_lifecycle_key": "",
            "evidence_role": _evidence_role("measurable_promise_candidate", route_reason="", promise_type=qualified_scope),
            "route_reason": "",
            "routing_reason": "",
            "topic_family": "",
            "confidence_score": float(base_score or 0.0),
            "lifecycle_state": "stated",
        }
    return {
        "candidate_type": str(routed.candidate_type or "measurable_promise_candidate"),
        "source_class": str(routed.source_class or source_class_txt),
        "statement_class": str(routed.statement_class or statement_class_txt),
        "metric_family": str(routed.metric_family or ""),
        "entity_scope": str(routed.entity_scope or ""),
        "target_period_norm": str(routed.target_period_norm or period_norm or ""),
        "parent_subject_key": str(routed.parent_subject_key or _build_parent_subject_key(entity_scope=routed.entity_scope or "", metric_family=routed.metric_family or "", program_token=routed.entity_scope or "", topic_family=routed.topic_family or "")),
        "canonical_subject_key": str(routed.canonical_subject_key or ""),
        "lifecycle_subject_key": str(routed.lifecycle_subject_key or routed.lifecycle_key or ""),
        "promise_lifecycle_key": str(routed.lifecycle_key or ""),
        "evidence_role": str(routed.evidence_role or _evidence_role(routed.candidate_type, route_reason=routed.route_reason or routed.routing_reason or "", promise_type=qualified_scope)),
        "route_reason": str(routed.route_reason or routed.routing_reason or ""),
        "routing_reason": str(routed.routing_reason or ""),
        "topic_family": str(routed.topic_family or ""),
        "confidence_score": float(routed.confidence_score or base_score or 0.0),
        "lifecycle_state": str(routed.lifecycle_state or "stated"),
    }


def _extract_promise_candidates_from_notes(quarter_notes: pd.DataFrame) -> pd.DataFrame:
    if quarter_notes is None or quarter_notes.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    qn = quarter_notes.copy()
    qn["quarter"] = pd.to_datetime(qn["quarter"], errors="coerce")
    qn = qn[qn["quarter"].notna()]
    for _, r in qn.iterrows():
        sent = str(r.get("evidence_snippet") or r.get("note") or "")
        if not sent:
            continue
        has_forward = bool(PROMISE_FORWARD_RE.search(sent))
        has_cost_signal = bool(COST_SAVINGS_KEY_RE.search(sent))
        if not has_forward and not has_cost_signal:
            continue
        source_type = r.get("doc_type") or r.get("form") or ""
        qualified = _qualify_promise_candidate(sent, source_type=source_type, metric_hint=r.get("metric_ref") or r.get("category") or "")
        if qualified is None:
            continue
        qd = pd.Timestamp(r["quarter"]).date()
        parsed = _parse_promise_candidate(sent, qd)
        if qualified.scope == "hard_target" and not (
            parsed.get("metric")
            and parsed.get("deadline") is not None
            and (
                pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce"))
                or pd.notna(pd.to_numeric(parsed.get("target_high"), errors="coerce"))
                or _has_cost_savings_numeric(parsed)
            )
        ):
            continue
        if qualified.scope == "clean_milestone" and not (
            str(parsed.get("promise_type") or "").strip().lower() == "milestone"
            and parsed.get("deadline") is not None
        ):
            continue
        if has_cost_signal and parsed.get("metric") == "cost_savings_run_rate":
            parsed["category"] = "Programs / initiatives"
            if not _has_cost_savings_numeric(parsed):
                continue
        if _is_historical_results_sentence(sent) and not parsed.get("scorable"):
            continue
        if _is_boilerplate_sentence(sent):
            has_numeric_target = pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce"))
            if not has_numeric_target and not _has_cost_savings_numeric(parsed):
                continue
        subject_meta = _promise_subject_metadata(
            text=sent,
            quarter_end=qd,
            source_type=source_type,
            source_doc=r.get("doc_path") or r.get("doc"),
            metric_hint=r.get("metric_ref") or r.get("category") or "",
            parsed=parsed,
            qualified_scope=qualified.scope,
            base_score=60.0,
        )
        rows.append(
            {
                "quarter": pd.Timestamp(qd),
                "category": parsed.get("category") or r.get("category"),
                "statement": sent[:300],
                "statement_norm": _norm_sentence(sent),
                "metric": parsed.get("metric"),
                "target_value": parsed.get("target_value"),
                "target_high": parsed.get("target_high"),
                "target_unit": parsed.get("target_unit"),
                "target_kind": parsed.get("target_kind"),
                "promise_type": parsed.get("promise_type") or "operational",
                "target_year": parsed.get("target_year"),
                "deadline": parsed.get("deadline"),
                "observed_runrate": parsed.get("observed_runrate"),
                "observed_increment": parsed.get("observed_increment"),
                "scorable": bool(parsed.get("scorable")),
                "soft_promise": bool(parsed.get("soft_promise")),
                "target_bucket": parsed.get("target_bucket"),
                "evidence_snippet": sent[:380],
                "accn": r.get("accn"),
                "form": r.get("form"),
                "doc": r.get("doc"),
                "doc_path": r.get("doc_path") or r.get("doc"),
                "doc_type": r.get("doc_type") or r.get("form"),
                "section_or_page": r.get("section_or_page"),
                "method": "quarter_notes_scan",
                "confidence": "med",
                "candidate_scope": qualified.scope,
                "statement_summary": qualified.summary,
                "preferred_narrative_source": bool(qualified.preferred_source),
                **subject_meta,
            }
        )
    return pd.DataFrame(rows)


def _extract_promise_candidates_from_local_release_dir(
    release_dir: Optional[Path],
    target_quarters: set[date],
    cache_dir: Optional[Path] = None,
    rebuild_doc_text_cache: bool = False,
    quiet_pdf_warnings: bool = True,
    doc_registry: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if release_dir is None:
        return pd.DataFrame()
    pdir = Path(release_dir)
    if not pdir.exists() or not pdir.is_dir():
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    pdf_files = sorted([p for p in pdir.glob("*.pdf") if p.is_file()], key=lambda p: p.name.lower())
    local_registry = doc_registry if doc_registry is not None else _make_doc_intel_doc_registry()
    for p in pdf_files:
        q_end = _infer_quarter_from_filename(p.name)
        if q_end is None or q_end not in target_quarters:
            continue
        txt = _doc_registry_load_text(
            p,
            local_registry,
            cache_dir=cache_dir,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
        )
        if len(txt or "") < 120:
            continue
        doc_abs, doc_rel, _ = _doc_registry_paths(p, local_registry)
        for sent in _split_sentences(txt):
            has_forward = bool(PROMISE_FORWARD_RE.search(sent))
            has_cost_signal = bool(COST_SAVINGS_KEY_RE.search(sent))
            if not has_forward and not has_cost_signal:
                continue
            qualified = _qualify_promise_candidate(sent, source_type="earnings_release_pdf", metric_hint="")
            if qualified is None:
                continue
            parsed = _parse_promise_candidate(sent, q_end)
            if qualified.scope == "hard_target" and not (
                parsed.get("metric")
                and parsed.get("deadline") is not None
                and (
                    pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce"))
                    or pd.notna(pd.to_numeric(parsed.get("target_high"), errors="coerce"))
                    or _has_cost_savings_numeric(parsed)
                )
            ):
                continue
            if qualified.scope == "clean_milestone" and not (
                str(parsed.get("promise_type") or "").strip().lower() == "milestone"
                and parsed.get("deadline") is not None
            ):
                continue
            if has_cost_signal and parsed.get("metric") == "cost_savings_run_rate":
                parsed["category"] = "Programs / initiatives"
                if not _has_cost_savings_numeric(parsed):
                    continue
            if _is_historical_results_sentence(sent) and not parsed.get("scorable"):
                continue
            if _is_boilerplate_sentence(sent):
                has_numeric_target = pd.notna(pd.to_numeric(parsed.get("target_value"), errors="coerce"))
                if not has_numeric_target and not _has_cost_savings_numeric(parsed):
                    continue
            subject_meta = _promise_subject_metadata(
                text=sent,
                quarter_end=q_end,
                source_type="earnings_release_pdf",
                source_doc=doc_abs or doc_rel or str(p.resolve()),
                metric_hint="",
                parsed=parsed,
                qualified_scope=qualified.scope,
                base_score=85.0,
            )
            rows.append(
                {
                    "quarter": pd.Timestamp(q_end),
                    "category": parsed.get("category"),
                    "statement": sent[:300],
                    "statement_norm": _norm_sentence(sent),
                    "metric": parsed.get("metric"),
                    "target_value": parsed.get("target_value"),
                    "target_high": parsed.get("target_high"),
                    "target_unit": parsed.get("target_unit"),
                    "target_kind": parsed.get("target_kind"),
                    "promise_type": parsed.get("promise_type") or "operational",
                    "target_year": parsed.get("target_year"),
                    "deadline": parsed.get("deadline"),
                    "observed_runrate": parsed.get("observed_runrate"),
                    "observed_increment": parsed.get("observed_increment"),
                    "scorable": bool(parsed.get("scorable")),
                    "soft_promise": bool(parsed.get("soft_promise")),
                    "target_bucket": parsed.get("target_bucket"),
                    "evidence_snippet": sent[:380],
                    "accn": None,
                    "form": "EARNINGS_RELEASE",
                    "doc": p.name,
                    "doc_path": doc_abs or doc_rel or str(p.resolve()),
                    "doc_type": "pdf",
                    "section_or_page": "earnings_release_pdf",
                    "method": "earnings_release_pdf_scan",
                    "confidence": "high",
                    "candidate_scope": qualified.scope,
                    "statement_summary": qualified.summary,
                    "preferred_narrative_source": bool(qualified.preferred_source),
                    **subject_meta,
                }
            )
    return pd.DataFrame(rows)


def _dedupe_promises(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_promises_df()
    d = df.copy()
    d["quarter"] = pd.to_datetime(d["quarter"], errors="coerce")
    d["deadline"] = pd.to_datetime(d["deadline"], errors="coerce")
    d["target_value"] = pd.to_numeric(d["target_value"], errors="coerce")
    if "promise_type" not in d.columns:
        d["promise_type"] = "operational"
    d["promise_type"] = d["promise_type"].astype(str).replace("", "operational")
    if "target_high" in d.columns:
        d["target_high"] = pd.to_numeric(d["target_high"], errors="coerce")
    else:
        d["target_high"] = pd.NA
    if "observed_runrate" in d.columns:
        d["observed_runrate"] = pd.to_numeric(d["observed_runrate"], errors="coerce")
    else:
        d["observed_runrate"] = pd.NA
    if "observed_increment" in d.columns:
        d["observed_increment"] = pd.to_numeric(d["observed_increment"], errors="coerce")
    else:
        d["observed_increment"] = pd.NA
    d["conf_rank"] = d["confidence"].map(_confidence_rank)
    d = d[d["quarter"].notna()].sort_values(["quarter", "conf_rank"], ascending=[True, False]).reset_index(drop=True)
    if d.empty:
        return _empty_promises_df()

    def _mention_kind_from_candidate(cand: Dict[str, Any]) -> str:
        if (
            pd.notna(pd.to_numeric(cand.get("target_value"), errors="coerce"))
            or pd.notna(pd.to_numeric(cand.get("target_high"), errors="coerce"))
            or pd.notna(pd.to_numeric(cand.get("observed_runrate"), errors="coerce"))
            or pd.notna(pd.to_numeric(cand.get("observed_increment"), errors="coerce"))
        ):
            return "numeric"
        return "text"

    def _guidance_bucket_from_candidate(cand: Dict[str, Any]) -> str:
        return _promise_guidance_bucket(
            metric=str(cand.get("metric") or ""),
            category=str(cand.get("category") or ""),
            statement=str(cand.get("statement") or ""),
        )

    def _match_score(cand: Dict[str, Any], cur: Dict[str, Any]) -> float:
        score = 0.0
        c_metric = str(cand.get("metric") or "")
        p_metric = str(cur.get("metric_tag") or "")
        c_deadline = pd.to_datetime(cand.get("deadline"), errors="coerce")
        p_deadline = pd.to_datetime(cur.get("target_time"), errors="coerce")
        c_bucket = str(cand.get("target_bucket") or "")
        p_bucket = str(cur.get("target_bucket") or "")
        if c_metric and p_metric and c_metric == p_metric:
            score += 35.0
            if c_metric == "cost_savings_run_rate":
                score += 18.0
        elif not c_metric and not p_metric:
            score += 8.0
        else:
            score -= 20.0
        if pd.notna(c_deadline) and pd.notna(p_deadline):
            if pd.Timestamp(c_deadline).date() == pd.Timestamp(p_deadline).date():
                score += 28.0
            elif abs((pd.Timestamp(c_deadline) - pd.Timestamp(p_deadline)).days) <= 92:
                score += 12.0
        elif pd.isna(c_deadline) and pd.isna(p_deadline):
            score += 5.0
        if c_bucket and p_bucket and c_bucket == p_bucket:
            score += 15.0
        kw_c = _keyword_hits(str(cand.get("statement_norm") or cand.get("statement") or ""))
        kw_p = cur.get("keyword_hits") or set()
        if kw_c and kw_p:
            overlap = len(kw_c & kw_p) / max(1, len(kw_c | kw_p))
            score += overlap * 12.0
        fuzz = _fuzzy_ratio(str(cand.get("statement_norm") or ""), str(cur.get("statement_norm") or ""))
        score += (fuzz / 100.0) * 10.0
        return score

    promise_objs: List[Dict[str, Any]] = []
    key_to_idx: Dict[str, int] = {}
    for _, row in d.iterrows():
        cand = row.to_dict()
        cand["statement_norm"] = str(cand.get("statement_norm") or "")
        cand["keyword_hits"] = _keyword_hits(cand["statement_norm"] or cand.get("statement") or "")
        cand["mention_kind"] = _mention_kind_from_candidate(cand)
        cand["guidance_type"] = _guidance_bucket_from_candidate(cand)
        cand["promise_key"] = _canonical_promise_key(
            metric=cand.get("metric"),
            target_kind=cand.get("target_kind"),
            target_low=cand.get("target_value"),
            target_high=cand.get("target_high"),
            target_unit=cand.get("target_unit"),
            deadline_v=cand.get("deadline"),
            target_year=cand.get("target_year"),
            guidance_bucket=cand.get("guidance_type"),
        )
        evidence = _source_evidence(cand)
        quarter_d = pd.Timestamp(cand["quarter"]).date()

        best_idx: Optional[int] = None
        best_score = -1.0
        pkey = str(cand.get("promise_key") or "").strip()
        if pkey and pkey in key_to_idx:
            best_idx = int(key_to_idx[pkey])
            best_score = 999.0
        else:
            for idx, p in enumerate(promise_objs):
                s = _match_score(cand, p)
                if s > best_score:
                    best_score = s
                    best_idx = idx
        threshold = 70.0 if cand.get("metric") else 82.0
        if str(cand.get("metric") or "") == "cost_savings_run_rate":
            threshold = 50.0
        if best_idx is not None and best_score >= threshold:
            p = promise_objs[best_idx]
            last_ev_ts = pd.to_datetime(p.get("last_seen_evidence_quarter"), errors="coerce")
            if pd.isna(last_ev_ts) or quarter_d >= pd.Timestamp(last_ev_ts).date():
                p["last_seen_evidence_quarter"] = quarter_d
            p["last_seen_quarter"] = p.get("last_seen_evidence_quarter")
            p["carried_to_quarter"] = p.get("last_seen_evidence_quarter")
            p["confidence"] = p.get("confidence") if _confidence_rank(str(p.get("confidence"))) >= _confidence_rank(str(cand.get("confidence"))) else cand.get("confidence")
            if (not p.get("metric_tag")) and cand.get("metric"):
                p["metric_tag"] = cand.get("metric")
                p["metric"] = cand.get("metric")
            if p.get("target_value") is None and pd.notna(cand.get("target_value")):
                p["target_value"] = float(cand["target_value"])
                p["units"] = cand.get("target_unit")
                p["target_kind"] = cand.get("target_kind")
            if p.get("target_high") is None and pd.notna(cand.get("target_high")):
                p["target_high"] = float(cand["target_high"])
            if p.get("target_time") is None and pd.notna(cand.get("deadline")):
                p["target_time"] = pd.Timestamp(cand["deadline"]).date()
                p["deadline"] = p["target_time"]
            if pd.notna(cand.get("target_value")):
                # Latest explicit target range should win on subsequent updates.
                p["target_value"] = float(cand["target_value"])
                p["target_high"] = float(cand["target_high"]) if pd.notna(cand.get("target_high")) else p.get("target_high")
                if pd.notna(cand.get("deadline")):
                    p["target_time"] = pd.Timestamp(cand["deadline"]).date()
                    p["deadline"] = p["target_time"]
                if str(cand.get("metric") or "") == "cost_savings_run_rate":
                    # Keep promise headline aligned to the latest explicit range statement.
                    p["promise_text"] = str(cand.get("statement") or p.get("promise_text") or "")[:300]
                    p["statement"] = str(cand.get("statement") or p.get("statement") or "")[:260]
                    p["evidence_snippet"] = str(cand.get("evidence_snippet") or p.get("evidence_snippet") or "")[:380]
                    p["accn"] = cand.get("accn") or p.get("accn")
                    p["doc"] = cand.get("doc") or p.get("doc")
                    p["method"] = cand.get("method") or p.get("method")
                    p["category"] = cand.get("category") or p.get("category")
            mk = str(cand.get("mention_kind") or "")
            if mk == "numeric":
                p["last_seen_numeric_quarter"] = quarter_d
            elif mk == "text":
                p["last_seen_text_quarter"] = quarter_d
            p["evidence_history"].append(
                {
                    "quarter": quarter_d.isoformat(),
                    "mention_kind": mk,
                    **evidence,
                }
            )
            p["status_history"].append({"quarter": quarter_d.isoformat(), "status": "open", "evidence": evidence})
            p["keyword_hits"] = p.get("keyword_hits", set()) | cand["keyword_hits"]
            p["scorable"] = bool(p.get("metric_tag") and p.get("target_time") is not None and p.get("target_value") is not None and p.get("target_kind"))
            p["soft_promise"] = not bool(p.get("scorable"))
            p["qualitative"] = bool(p.get("target_time") is not None and p.get("target_value") is None)
            p["promise_key"] = pkey or p.get("promise_key") or ""
            p["guidance_type"] = str(cand.get("guidance_type") or p.get("guidance_type") or "")
            p["promise_type"] = str(cand.get("promise_type") or p.get("promise_type") or "operational")
            p["candidate_scope"] = str(cand.get("candidate_scope") or p.get("candidate_scope") or "")
            p["candidate_type"] = str(cand.get("candidate_type") or p.get("candidate_type") or "measurable_promise_candidate")
            p["statement_summary"] = str(cand.get("statement_summary") or p.get("statement_summary") or "")
            p["preferred_narrative_source"] = bool(cand.get("preferred_narrative_source") or p.get("preferred_narrative_source"))
            p["metric_family"] = str(cand.get("metric_family") or p.get("metric_family") or "")
            p["entity_scope"] = str(cand.get("entity_scope") or p.get("entity_scope") or "")
            p["target_period_norm"] = str(cand.get("target_period_norm") or p.get("target_period_norm") or "")
            p["canonical_subject_key"] = str(cand.get("canonical_subject_key") or p.get("canonical_subject_key") or "")
            p["promise_lifecycle_key"] = str(cand.get("promise_lifecycle_key") or p.get("promise_lifecycle_key") or "")
            p["route_reason"] = str(cand.get("route_reason") or cand.get("routing_reason") or p.get("route_reason") or "")
            p["routing_reason"] = str(cand.get("routing_reason") or p.get("routing_reason") or "")
            p["topic_family"] = str(cand.get("topic_family") or p.get("topic_family") or "")
            p["confidence_score"] = float(cand.get("confidence_score") or p.get("confidence_score") or 0.0)
            if pkey:
                key_to_idx[pkey] = best_idx
            continue

        metric_tag = str(cand.get("metric") or "")
        target_time = pd.Timestamp(cand["deadline"]).date() if pd.notna(cand.get("deadline")) else None
        pid_seed = pkey if pkey else f"{metric_tag}|{target_time}|{cand.get('statement_norm') or cand.get('statement')}"
        promise_id = hashlib.sha1(pid_seed.encode("utf-8")).hexdigest()[:12]
        mk = str(cand.get("mention_kind") or "")
        last_num_q = quarter_d if mk == "numeric" else None
        last_txt_q = quarter_d if mk == "text" else None
        promise_objs.append(
            {
                "promise_id": promise_id,
                "promise_key": pkey,
                "promise_text": str(cand.get("statement") or "")[:300],
                "statement_norm": cand.get("statement_norm") or "",
                "metric_tag": metric_tag or None,
                "metric": metric_tag or None,
                "target_time": target_time,
                "deadline": target_time,
                "target_value": float(cand["target_value"]) if pd.notna(cand.get("target_value")) else None,
                "target_high": float(cand["target_high"]) if pd.notna(cand.get("target_high")) else None,
                "units": cand.get("target_unit"),
                "target_unit": cand.get("target_unit"),
                "target_kind": cand.get("target_kind"),
                "promise_type": str(cand.get("promise_type") or "operational"),
                "target_bucket": cand.get("target_bucket"),
                "created_quarter": quarter_d,
                "first_seen_quarter": quarter_d,
                "last_seen_quarter": quarter_d,
                "category": cand.get("category"),
                "statement": str(cand.get("statement") or "")[:260],
                "target_year": cand.get("target_year"),
                "scorable": bool(cand.get("metric") and pd.notna(cand.get("deadline")) and pd.notna(cand.get("target_value")) and cand.get("target_kind")),
                "soft_promise": not bool(cand.get("metric") and pd.notna(cand.get("deadline")) and pd.notna(cand.get("target_value")) and cand.get("target_kind")),
                "qualitative": bool(pd.notna(cand.get("deadline")) and pd.isna(cand.get("target_value"))),
                "source_evidence": evidence,
                "evidence_history": [{"quarter": quarter_d.isoformat(), "mention_kind": mk, **evidence}],
                "status_history": [{"quarter": quarter_d.isoformat(), "status": "open", "evidence": evidence}],
                "evidence_snippet": evidence.get("snippet"),
                "accn": cand.get("accn"),
                "doc": cand.get("doc"),
                "method": cand.get("method"),
                "confidence": cand.get("confidence"),
                "keyword_hits": cand["keyword_hits"],
                "guidance_type": str(cand.get("guidance_type") or ""),
                "candidate_scope": str(cand.get("candidate_scope") or ""),
                "candidate_type": str(cand.get("candidate_type") or "measurable_promise_candidate"),
                "statement_summary": str(cand.get("statement_summary") or ""),
                "preferred_narrative_source": bool(cand.get("preferred_narrative_source")),
                "metric_family": str(cand.get("metric_family") or ""),
                "entity_scope": str(cand.get("entity_scope") or ""),
                "target_period_norm": str(cand.get("target_period_norm") or ""),
                "canonical_subject_key": str(cand.get("canonical_subject_key") or ""),
                "promise_lifecycle_key": str(cand.get("promise_lifecycle_key") or ""),
                "route_reason": str(cand.get("route_reason") or cand.get("routing_reason") or ""),
                "routing_reason": str(cand.get("routing_reason") or ""),
                "topic_family": str(cand.get("topic_family") or ""),
                "confidence_score": float(cand.get("confidence_score") or 0.0),
                "lifecycle_state": str(
                    _derive_lifecycle_state(
                        target_period_norm=cand.get("target_period_norm"),
                        stated_quarter=quarter_d,
                        latest_evidence_quarter=quarter_d,
                        evaluated_through_quarter=quarter_d,
                        carried_to_quarter=quarter_d,
                        current_status="open",
                    )
                ),
                "first_seen_evidence_quarter": quarter_d,
                "last_seen_evidence_quarter": quarter_d,
                "carried_to_quarter": quarter_d,
                "last_seen_numeric_quarter": last_num_q,
                "last_seen_text_quarter": last_txt_q,
                "qa_severity": "",
                "qa_message": "",
            }
        )
        if pkey:
            key_to_idx[pkey] = len(promise_objs) - 1

    out_rows: List[Dict[str, Any]] = []
    for p in promise_objs:
        qa_severity = str(p.get("qa_severity") or "")
        qa_message = str(p.get("qa_message") or "")
        if bool(p.get("scorable")) and (not p.get("metric_tag") or p.get("target_time") is None):
            qa_severity = "FAIL"
            qa_message = "scorable promise missing metric or deadline"
        elif bool(p.get("qualitative")):
            qa_severity = "WARN"
            qa_message = "qualitative promise (deadline set, target_value missing)"
        out_rows.append(
            {
                "promise_id": p.get("promise_id"),
                "promise_key": p.get("promise_key"),
                "promise_text": p.get("promise_text"),
                "metric_tag": p.get("metric_tag"),
                "target_time": p.get("target_time"),
                "units": p.get("units"),
                "created_quarter": p.get("created_quarter"),
                "first_seen_quarter": p.get("first_seen_quarter"),
                "last_seen_quarter": p.get("last_seen_quarter"),
                "first_seen_evidence_quarter": p.get("first_seen_evidence_quarter"),
                "last_seen_evidence_quarter": p.get("last_seen_evidence_quarter"),
                "carried_to_quarter": p.get("carried_to_quarter"),
                "last_seen_numeric_quarter": p.get("last_seen_numeric_quarter"),
                "last_seen_text_quarter": p.get("last_seen_text_quarter"),
                "category": p.get("category"),
                "statement": p.get("statement"),
                "metric": p.get("metric"),
                "target_value": p.get("target_value"),
                "target_high": p.get("target_high"),
                "target_unit": p.get("target_unit"),
                "target_kind": p.get("target_kind"),
                "promise_type": p.get("promise_type"),
                "target_year": p.get("target_year"),
                "guidance_type": p.get("guidance_type"),
                "candidate_scope": p.get("candidate_scope"),
                "candidate_type": p.get("candidate_type"),
                "statement_summary": p.get("statement_summary"),
                "preferred_narrative_source": p.get("preferred_narrative_source"),
                "metric_family": p.get("metric_family"),
                "entity_scope": p.get("entity_scope"),
                "target_period_norm": p.get("target_period_norm"),
                "canonical_subject_key": p.get("canonical_subject_key"),
                "promise_lifecycle_key": p.get("promise_lifecycle_key"),
                "route_reason": p.get("route_reason"),
                "routing_reason": p.get("routing_reason"),
                "topic_family": p.get("topic_family"),
                "confidence_score": p.get("confidence_score"),
                "lifecycle_state": p.get("lifecycle_state")
                or _derive_lifecycle_state(
                    target_period_norm=p.get("target_period_norm"),
                    stated_quarter=p.get("first_seen_evidence_quarter"),
                    latest_evidence_quarter=p.get("last_seen_evidence_quarter"),
                    evaluated_through_quarter=p.get("carried_to_quarter"),
                    carried_to_quarter=p.get("carried_to_quarter"),
                    current_status="open",
                ),
                "deadline": p.get("deadline"),
                "scorable": p.get("scorable"),
                "soft_promise": p.get("soft_promise"),
                "target_bucket": p.get("target_bucket"),
                "qualitative": p.get("qualitative"),
                "source_evidence_json": json.dumps(p.get("source_evidence"), ensure_ascii=True),
                "evidence_history_json": json.dumps(p.get("evidence_history", []), ensure_ascii=True),
                "status_history_json": json.dumps(p.get("status_history", []), ensure_ascii=True),
                "evidence_snippet": p.get("evidence_snippet"),
                "accn": p.get("accn"),
                "doc": p.get("doc"),
                "method": p.get("method"),
                "confidence": p.get("confidence"),
                "qa_severity": qa_severity,
                "qa_message": qa_message,
            }
        )
    out = pd.DataFrame(out_rows)
    if out.empty:
        return _empty_promises_df()
    return out.sort_values(["created_quarter", "promise_id"]).reset_index(drop=True)


def _build_metric_panel(hist: pd.DataFrame, adj_metrics: pd.DataFrame) -> pd.DataFrame:
    h = hist.copy()
    h["quarter"] = pd.to_datetime(h["quarter"], errors="coerce")
    h = h[h["quarter"].notna()].sort_values("quarter").drop_duplicates("quarter", keep="last").reset_index(drop=True)
    if h.empty:
        return pd.DataFrame()
    out = pd.DataFrame({"quarter": h["quarter"]})
    rev = pd.to_numeric(h.get("revenue"), errors="coerce")
    ebitda = pd.to_numeric(h.get("ebitda"), errors="coerce")
    debt = pd.to_numeric(h.get("debt_core"), errors="coerce")
    cash = pd.to_numeric(h.get("cash"), errors="coerce")
    buybacks = pd.to_numeric(h.get("buybacks_cash"), errors="coerce").abs()
    dividends = pd.to_numeric(h.get("dividends_cash"), errors="coerce").abs()
    out["corporate_net_debt"] = debt - cash
    rev_lag = rev.shift(4)
    out["revenue_yoy"] = (rev - rev_lag) / rev_lag.abs()
    out.loc[(rev_lag == 0) | rev_lag.isna() | rev.isna(), "revenue_yoy"] = pd.NA

    rev_ttm = rev.rolling(4, min_periods=4).sum()
    ebitda_ttm = ebitda.rolling(4, min_periods=4).sum()
    out["corporate_net_leverage"] = out["corporate_net_debt"] / ebitda_ttm.replace({0.0: pd.NA})
    out["ebitda_margin_ttm"] = ebitda_ttm / rev_ttm.replace({0.0: pd.NA})

    if adj_metrics is not None and not adj_metrics.empty and "quarter" in adj_metrics.columns and "adj_ebitda" in adj_metrics.columns:
        am = adj_metrics.copy()
        am["quarter"] = pd.to_datetime(am["quarter"], errors="coerce")
        am["adj_ebitda"] = pd.to_numeric(am["adj_ebitda"], errors="coerce")
        am = am[am["quarter"].notna() & am["adj_ebitda"].notna()].sort_values("quarter")
        merged = out[["quarter"]].merge(am[["quarter", "adj_ebitda"]], on="quarter", how="left")
        out["adj_ebitda_ttm"] = merged["adj_ebitda"].rolling(4, min_periods=4).sum()
        out["adj_ebitda_margin_ttm"] = out["adj_ebitda_ttm"] / rev_ttm.replace({0.0: pd.NA})
    else:
        out["adj_ebitda_margin_ttm"] = out["ebitda_margin_ttm"]
    cap_alloc_q = buybacks.fillna(0.0) + dividends.fillna(0.0)
    out["capital_allocation"] = cap_alloc_q.rolling(4, min_periods=4).sum()
    # Filled from promise evidence history when available.
    out["cost_savings_run_rate"] = pd.NA
    return out


def _promise_tolerance(metric: str, target: float, target_unit: str, target_kind: str = "") -> float:
    m = str(metric or "").strip().lower()
    k = str(target_kind or "").strip().lower()
    # Guidance range promises use explicit low/high boundaries and should not rely on tolerance drift.
    if k == "range_within":
        return 0.0
    tolerance_map: Dict[str, float] = {
        "revenue_yoy": 0.005,
        "adj_ebitda_margin_ttm": 0.005,
        "ebitda_margin_ttm": 0.005,
        "corporate_net_leverage": 0.05,
    }
    if m in tolerance_map:
        return float(tolerance_map[m])
    if m == "cost_savings_run_rate":
        return max(abs(float(target)) * 0.03, 3_000_000.0)
    if str(target_unit or "").upper() == "USD":
        return max(abs(float(target)) * 0.05, 5_000_000.0)
    return max(abs(float(target)) * 0.05, 0.01)


def _target_level(metric: str, target_kind: str, target_value: float, baseline_value: Optional[float]) -> Optional[float]:
    if target_kind in ("lte_abs", "gte_abs", "abs_le"):
        return target_value
    if baseline_value is None:
        return None
    if target_kind == "delta_down_abs":
        return baseline_value - target_value
    if target_kind == "delta_down_pct":
        return baseline_value * (1.0 - target_value)
    if target_kind == "delta_up_abs":
        return baseline_value + target_value
    return None


def _is_met(actual: float, target_kind: str, target_level: float, tol: float) -> bool:
    if target_kind == "lte_abs":
        return actual <= target_level + tol
    if target_kind == "gte_abs":
        return actual >= target_level - tol
    if target_kind == "abs_le":
        return abs(actual - target_level) <= tol
    if target_kind in ("delta_down_abs", "delta_down_pct"):
        return actual <= target_level + tol
    if target_kind == "delta_up_abs":
        return actual >= target_level - tol
    return False


def _safe_float_optional(value: Any) -> Optional[float]:
    coerced = pd.to_numeric(value, errors="coerce")
    if pd.isna(coerced):
        return None
    return float(coerced)


def _parse_json_list_of_dicts(raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        return [raw]
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def _parse_json_first_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return item
        return {}
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict):
                return item
    return {}


def _progress_event_numeric_rank(event_obj: Dict[str, Any]) -> int:
    if not isinstance(event_obj, dict):
        return -1
    score = 0
    if _safe_float_optional(event_obj.get("observed_runrate")) is not None:
        score += 4
    if _safe_float_optional(event_obj.get("observed_increment")) is not None:
        score += 3
    if _safe_float_optional(event_obj.get("target_low")) is not None:
        score += 2
    if _safe_float_optional(event_obj.get("target_high")) is not None:
        score += 1
    snippet = str(event_obj.get("snippet") or "")
    if _is_boilerplate_sentence(snippet):
        score -= 3
    return score


def _extract_cost_events_from_prepared_promise_row(prepared_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    metric_name = str(prepared_row.get("metric_tag") or prepared_row.get("metric") or "")
    if metric_name != "cost_savings_run_rate":
        return events

    def _append_event(q_key: str, event_obj: Dict[str, Any]) -> None:
        q_ts = pd.to_datetime(q_key, errors="coerce")
        if pd.isna(q_ts) or not isinstance(event_obj, dict):
            return
        qd_e = pd.Timestamp(q_ts).date()
        snippet = str(event_obj.get("snippet") or "")
        lo = _safe_float_optional(event_obj.get("target_low"))
        hi = _safe_float_optional(event_obj.get("target_high"))
        if lo is None:
            lo_parsed, hi_parsed = _parse_cost_savings_range_target(snippet)
            lo = lo_parsed
            if hi is None:
                hi = hi_parsed
        runrate = _safe_float_optional(event_obj.get("observed_runrate"))
        if runrate is None:
            runrate = _parse_cost_savings_runrate_point(snippet)
        increment = _safe_float_optional(event_obj.get("observed_increment"))
        if increment is None:
            increment = _parse_cost_savings_increment(snippet)
        events.append(
            {
                "quarter": qd_e,
                "target_low": lo,
                "target_high": hi,
                "runrate": runrate,
                "increment": increment,
                "snippet": snippet,
            }
        )

    for event_obj in prepared_row.get("_evidence_history") or []:
        q_key = str(event_obj.get("quarter") or "")
        if q_key:
            _append_event(q_key, event_obj)

    source_fallback = prepared_row.get("_source_evidence") or {}
    if source_fallback and (
        _safe_float_optional(source_fallback.get("target_low")) is not None
        or _safe_float_optional(source_fallback.get("target_high")) is not None
        or _safe_float_optional(source_fallback.get("observed_runrate")) is not None
        or _safe_float_optional(source_fallback.get("observed_increment")) is not None
    ):
        first_seen_q = pd.to_datetime(
            prepared_row.get("first_seen_evidence_quarter")
            or prepared_row.get("first_seen_quarter")
            or prepared_row.get("created_quarter"),
            errors="coerce",
        )
        if pd.notna(first_seen_q):
            _append_event(str(pd.Timestamp(first_seen_q).date().isoformat()), source_fallback)

    return events


def _build_progress_promise_bundle(promises: pd.DataFrame) -> Dict[str, Any]:
    if promises is None or promises.empty:
        return {
            "all_records": [],
            "records": [],
            "global_cost_events": [],
        }

    p_all = promises.copy()
    if "scorable" in p_all.columns:
        p_all["scorable"] = p_all["scorable"].fillna(False).astype(bool)
    else:
        p_all["scorable"] = False

    prepared_all: List[Dict[str, Any]] = []
    for rec in p_all.to_dict("records"):
        prepared = dict(rec)
        evidence_history = _parse_json_list_of_dicts(rec.get("evidence_history_json"))
        evidence_by_quarter: Dict[str, Dict[str, Any]] = {}
        for event_obj in evidence_history:
            q_key = str(event_obj.get("quarter") or "")
            if not q_key:
                continue
            prev = evidence_by_quarter.get(q_key)
            if prev is None or _progress_event_numeric_rank(event_obj) >= _progress_event_numeric_rank(prev):
                evidence_by_quarter[q_key] = event_obj
        prepared["_evidence_history"] = evidence_history
        prepared["_source_evidence"] = _parse_json_first_dict(rec.get("source_evidence_json"))
        prepared["_evidence_by_quarter"] = evidence_by_quarter
        prepared["_cost_events"] = _extract_cost_events_from_prepared_promise_row(prepared)
        prepared_all.append(prepared)

    global_cost_events: List[Dict[str, Any]] = []
    for prepared in prepared_all:
        global_cost_events.extend(list(prepared.get("_cost_events") or []))
    if global_cost_events:
        best_by_q: Dict[date, Dict[str, Any]] = {}
        for event_obj in global_cost_events:
            qd_ev = event_obj.get("quarter")
            if qd_ev is None:
                continue
            row = best_by_q.get(qd_ev)
            if row is None:
                row = {
                    "quarter": qd_ev,
                    "target_low": None,
                    "target_high": None,
                    "runrate": None,
                    "increment": None,
                    "snippet": "",
                }
            lo_ev = _safe_float_optional(event_obj.get("target_low"))
            hi_ev = _safe_float_optional(event_obj.get("target_high"))
            rr_ev = _safe_float_optional(event_obj.get("runrate"))
            inc_ev = _safe_float_optional(event_obj.get("increment"))
            if lo_ev is not None:
                row["target_low"] = lo_ev
            if hi_ev is not None:
                row["target_high"] = hi_ev
            if rr_ev is not None:
                row["runrate"] = rr_ev
            if inc_ev is not None:
                prev_inc = _safe_float_optional(row.get("increment"))
                if prev_inc is None or abs(float(inc_ev)) > abs(float(prev_inc)):
                    row["increment"] = float(inc_ev)
            snippet = str(event_obj.get("snippet") or "")
            if len(snippet) > len(str(row.get("snippet") or "")):
                row["snippet"] = snippet
            best_by_q[qd_ev] = row
        global_cost_events = sorted(best_by_q.values(), key=lambda item: item.get("quarter") or date.min)

    return {
        "all_records": prepared_all,
        "records": [prepared for prepared in prepared_all if bool(prepared.get("scorable"))],
        "global_cost_events": global_cost_events,
    }


def _build_progress(promises: pd.DataFrame, hist: pd.DataFrame, adj_metrics: pd.DataFrame) -> pd.DataFrame:
    if promises is None or promises.empty or hist is None or hist.empty:
        return _empty_progress_df()
    promise_bundle = _build_progress_promise_bundle(promises)
    prepared_records = list(promise_bundle.get("records") or [])
    if not prepared_records:
        return _empty_progress_df()

    panel = _build_metric_panel(hist, adj_metrics)
    if panel.empty:
        return _empty_progress_df()
    panel["quarter"] = pd.to_datetime(panel["quarter"], errors="coerce")
    qs = [pd.Timestamp(q) for q in panel["quarter"].dropna().tolist()]
    if not qs:
        return _empty_progress_df()
    panel_idx = panel.set_index("quarter")
    score_map = {
        "achieved": 2,
        "on_track": 1,
        "open": 0,
        "pending": 0,
        "unclear": 0,
        "no_actual_available": 0,
        "at_risk": -1,
        "broken": -2,
    }
    out_rows: List[Dict[str, Any]] = []

    def _safe_float(v: Any) -> Optional[float]:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return None
        return float(x)

    def _q_ord(v: date) -> int:
        return (int(v.year) * 4) + (((int(v.month) - 1) // 3) + 1)

    def _q_label(v: Optional[date]) -> str:
        if v is None:
            return "N/A"
        qn = ((int(v.month) - 1) // 3) + 1
        return f"Q{qn} {int(v.year)}"

    hist_proxy = hist.copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame()
    if not hist_proxy.empty and "quarter" in hist_proxy.columns:
        hist_proxy["quarter"] = pd.to_datetime(hist_proxy["quarter"], errors="coerce")
        hist_proxy = hist_proxy[hist_proxy["quarter"].notna()].sort_values("quarter")
    proxy_sga_col = next(
        (
            c
            for c in ["sga", "sg_and_a", "selling_general_and_administrative"]
            if c in hist_proxy.columns
        ),
        None,
    )
    proxy_rd_col = "research_and_development" if "research_and_development" in hist_proxy.columns else None
    if proxy_sga_col is not None:
        hist_proxy[proxy_sga_col] = pd.to_numeric(hist_proxy[proxy_sga_col], errors="coerce")
    if proxy_rd_col is not None:
        hist_proxy[proxy_rd_col] = pd.to_numeric(hist_proxy[proxy_rd_col], errors="coerce")
    if not hist_proxy.empty:
        hist_proxy["quarter_end"] = hist_proxy["quarter"].dt.to_period("Q").dt.end_time.dt.date

    def _cost_proxy_opex_yoy(qd_cur: date) -> Optional[float]:
        if hist_proxy.empty or proxy_sga_col is None or proxy_rd_col is None:
            return None
        row_cur = hist_proxy[hist_proxy["quarter_end"] == qd_cur]
        row_prev = hist_proxy[hist_proxy["quarter_end"] == date(qd_cur.year - 1, qd_cur.month, qd_cur.day)]
        if row_cur.empty or row_prev.empty:
            return None
        try:
            cur_sum = float(pd.to_numeric(row_cur.iloc[-1][proxy_sga_col], errors="coerce")) + float(
                pd.to_numeric(row_cur.iloc[-1][proxy_rd_col], errors="coerce")
            )
            prev_sum = float(pd.to_numeric(row_prev.iloc[-1][proxy_sga_col], errors="coerce")) + float(
                pd.to_numeric(row_prev.iloc[-1][proxy_rd_col], errors="coerce")
            )
        except Exception:
            return None
        if pd.isna(cur_sum) or pd.isna(prev_sum):
            return None
        return float(cur_sum - prev_sum)

    def _timing_note_phrase(text: str) -> str:
        s = str(text or "")
        m = re.search(
            r"\b(over\s+the\s+next\s+12\s+months|in\s+the\s+next\s+12\s+months|over\s+the\s+next\s+year|"
            r"into\s+20\d{2}|through\s+20\d{2}|remainder[^.]{0,80}(?:next year|20\d{2}))\b",
            s,
            re.I,
        )
        if not m:
            return ""
        return re.sub(r"\s+", " ", m.group(0)).strip()

    global_cost_events = list(promise_bundle.get("global_cost_events") or [])

    for pr in prepared_records:
        pid = str(pr.get("promise_id"))
        metric = str(pr.get("metric_tag") or pr.get("metric") or "")
        promise_key = str(pr.get("promise_key") or "")
        kind = str(pr.get("target_kind") or "")
        promise_type = str(pr.get("promise_type") or "operational")
        guidance_type = str(pr.get("guidance_type") or "")
        target = pd.to_numeric(pr.get("target_value"), errors="coerce")
        target_high_raw = pd.to_numeric(pr.get("target_high"), errors="coerce")
        target_high = None if pd.isna(target_high_raw) else float(target_high_raw)
        deadline = pd.to_datetime(pr.get("target_time") or pr.get("deadline"), errors="coerce")
        first_q = pd.to_datetime(pr.get("created_quarter") or pr.get("first_seen_quarter"), errors="coerce")
        first_ev_q = pd.to_datetime(
            pr.get("first_seen_evidence_quarter")
            or pr.get("first_seen_quarter")
            or pr.get("created_quarter"),
            errors="coerce",
        )
        last_ev_q = pd.to_datetime(
            pr.get("last_seen_evidence_quarter")
            or pr.get("last_seen_quarter")
            or pr.get("created_quarter"),
            errors="coerce",
        )
        last_num_q_seed = pd.to_datetime(pr.get("last_seen_numeric_quarter"), errors="coerce")
        last_txt_q_seed = pd.to_datetime(pr.get("last_seen_text_quarter"), errors="coerce")
        if not metric or pd.isna(target) or pd.isna(deadline) or pd.isna(first_q):
            continue
        if metric == "cost_savings_run_rate" and not guidance_type:
            guidance_type = "run-rate"

        tol = _promise_tolerance(metric, float(target), str(pr.get("target_unit") or ""), kind)
        baseline = None
        if first_q in panel_idx.index and metric in panel_idx.columns:
            v0 = pd.to_numeric(panel_idx.loc[first_q, metric], errors="coerce")
            baseline = None if pd.isna(v0) else float(v0)
        target_level_static = _target_level(metric, kind, float(target), baseline)
        base_target_low = float(target) if pd.notna(target) else None
        base_target_high = float(target_high) if target_high is not None else None
        prev_actual: Optional[float] = None
        prev_status: Optional[str] = None
        status_hist: List[Dict[str, Any]] = []
        ev_map = dict(pr.get("_evidence_by_quarter") or {})
        source_fallback = dict(pr.get("_source_evidence") or {})

        cost_events: List[Dict[str, Any]] = []
        if metric == "cost_savings_run_rate":
            # Use global numeric evidence timeline (all cost-savings promises) for as-of snapshots.
            if global_cost_events:
                cost_events = list(global_cost_events)
            else:
                cost_events = list(pr.get("_cost_events") or [])
            cost_events = sorted(cost_events, key=lambda z: z.get("quarter") or date.min)

        for q in qs:
            if q < first_q:
                continue
            actual = None
            target_level = target_level_static
            target_high_q = target_high
            range_src_q: Optional[date] = None
            numeric_update_this_q = False
            carried_forward = False
            if q in panel_idx.index and metric in panel_idx.columns:
                vv = pd.to_numeric(panel_idx.loc[q, metric], errors="coerce")
                actual = None if pd.isna(vv) else float(vv)

            if metric == "cost_savings_run_rate":
                qd_cur = pd.Timestamp(q).date()
                latest_runrate: Optional[float] = None
                cum_increment = 0.0
                has_increment = False
                active_low = base_target_low
                active_high = base_target_high
                runrate_by_q: Dict[date, float] = {}
                increment_by_q: Dict[date, float] = {}
                timing_note: str = ""
                timing_note_q: Optional[date] = None
                for ev in cost_events:
                    qd_ev = ev.get("quarter")
                    if qd_ev is None or qd_ev > qd_cur:
                        continue
                    lo_ev = _safe_float(ev.get("target_low"))
                    hi_ev = _safe_float(ev.get("target_high"))
                    if lo_ev is not None:
                        active_low = lo_ev
                        range_src_q = qd_ev
                    if hi_ev is not None:
                        active_high = hi_ev
                        range_src_q = qd_ev
                    rr_ev = _safe_float(ev.get("runrate"))
                    if rr_ev is not None:
                        latest_runrate = rr_ev
                        runrate_by_q[qd_ev] = rr_ev
                    inc_ev = _safe_float(ev.get("increment"))
                    if inc_ev is not None:
                        cum_increment += float(inc_ev)
                        has_increment = True
                        increment_by_q[qd_ev] = float(increment_by_q.get(qd_ev, 0.0)) + float(inc_ev)
                    phrase = _timing_note_phrase(str(ev.get("snippet") or ""))
                    if phrase:
                        timing_note = phrase
                        timing_note_q = qd_ev
                if latest_runrate is not None:
                    actual = latest_runrate
                elif has_increment:
                    actual = cum_increment
                if active_low is not None:
                    target_level = active_low
                target_high_q = active_high if active_high is not None else active_low
                latest_runrate_q = max(runrate_by_q.keys()) if runrate_by_q else None
                prev_runrate_q = max([k for k in runrate_by_q.keys() if latest_runrate_q is not None and k < latest_runrate_q], default=None)
                latest_increment_q = max(increment_by_q.keys()) if increment_by_q else None
                prev_increment_q = max([k for k in increment_by_q.keys() if latest_increment_q is not None and k < latest_increment_q], default=None)
                newest_numeric_q = latest_runrate_q or latest_increment_q
                numeric_update_this_q = bool(newest_numeric_q is not None and newest_numeric_q == qd_cur)
                carried_forward = bool(newest_numeric_q is not None and qd_cur > newest_numeric_q)
            else:
                latest_runrate_q = None
                prev_runrate_q = None
                latest_increment_q = None
                prev_increment_q = None
                timing_note = ""
                timing_note_q = None

            first_ev_date = pd.Timestamp(first_ev_q).date() if pd.notna(first_ev_q) else pd.Timestamp(first_q).date()
            last_ev_date = pd.Timestamp(last_ev_q).date() if pd.notna(last_ev_q) else first_ev_date
            last_num_date = pd.Timestamp(last_num_q_seed).date() if pd.notna(last_num_q_seed) else None
            last_txt_date = pd.Timestamp(last_txt_q_seed).date() if pd.notna(last_txt_q_seed) else None
            if metric == "cost_savings_run_rate":
                newest_numeric_q = latest_runrate_q or latest_increment_q
                if newest_numeric_q is not None:
                    last_num_date = newest_numeric_q

            status = "unclear"
            rationale = "No actual metric available."
            qa_severity = ""
            qa_message = ""
            evidence_obj_current = ev_map.get(q.date().isoformat())
            evidence_obj = evidence_obj_current or source_fallback
            if actual is None:
                if promise_type == "milestone":
                    if q <= deadline:
                        status = "pending"
                        rationale = "Milestone pending until stated deadline."
                    else:
                        qa_severity = "WARN"
                        qa_message = "milestone deadline passed without measurable completion signal"
                        status = "no_actual_available"
                        rationale = "Deadline passed; no measurable completion signal available."
                elif q <= deadline:
                    status = "open"
                    rationale = "Awaiting metric update before deadline."
                elif q > deadline:
                    qa_severity = "WARN"
                    qa_message = "promise has no actual at/after deadline"
                    status = "unclear"
                status_changed = bool(prev_status and prev_status != status)
                if status_changed and evidence_obj_current is None:
                    qa_severity = "FAIL"
                    qa_message = "status changed without new evidence"
                out_rows.append({
                    "quarter": q.date(),
                    "promise_id": pid,
                    "promise_key": promise_key,
                    "status": status,
                    "status_score": score_map[status],
                    "rationale": rationale,
                    "metric_refs": metric,
                    "actual": actual,
                    "target": target_level,
                    "target_bucket": pr.get("target_bucket"),
                    "promise_type": promise_type,
                    "scorable": bool(pr.get("scorable")),
                    "guidance_type": guidance_type,
                    "deadline": deadline.date(),
                    "source_evidence_json": json.dumps(evidence_obj, ensure_ascii=True) if evidence_obj else "",
                    "status_changed": status_changed,
                    "status_history_json": "",
                    "first_seen_evidence_quarter": first_ev_date,
                    "last_seen_evidence_quarter": last_ev_date,
                    "last_seen_numeric_quarter": last_num_date,
                    "last_seen_text_quarter": last_txt_date,
                    "carried_to_quarter": q.date(),
                    "numeric_update_this_quarter": bool(numeric_update_this_q),
                    "qa_severity": qa_severity,
                    "qa_message": qa_message,
                })
                status_hist.append({"quarter": q.date().isoformat(), "status": status, "evidence": evidence_obj})
                prev_status = status
                continue

            if target_level is None:
                status = "unclear"
                rationale = "Target level not derivable."
            else:
                if kind == "range_within":
                    lo_bound = float(target_level)
                    hi_bound = float(target_high_q) if target_high_q is not None else lo_bound
                    lo_bound, hi_bound = (min(lo_bound, hi_bound), max(lo_bound, hi_bound))
                    met = (float(actual) >= (lo_bound - tol)) and (float(actual) <= (hi_bound + tol))
                else:
                    met = _is_met(actual, kind, float(target_level), tol)
                if q >= deadline:
                    status = "achieved" if met else "broken"
                    if kind == "range_within":
                        hi_txt = float(target_high_q) if target_high_q is not None else float(target_level)
                        rationale = (
                            f"Actual {actual:.4f} {'inside' if met else 'outside'} guidance range "
                            f"[{float(target_level):.4f}, {hi_txt:.4f}] (tol {tol:.4f}) at deadline."
                        )
                    else:
                        rationale = f"Actual {actual:.4f} {'meets' if met else 'misses'} target {float(target_level):.4f} (tol {tol:.4f}) at deadline."
                else:
                    if met:
                        status = "achieved"
                        rationale = "Target already met before deadline."
                    else:
                        toward = None
                        if prev_actual is not None:
                            if kind in {"lte_abs", "delta_down_abs", "delta_down_pct"}:
                                toward = actual < prev_actual
                            elif kind in {"gte_abs", "delta_up_abs"}:
                                toward = actual > prev_actual
                            elif kind == "abs_le":
                                toward = abs(actual - float(target_level)) < abs(prev_actual - float(target_level))
                        if toward is None:
                            status = "open"
                            rationale = "Insufficient trend history yet."
                        else:
                            status = "on_track" if toward else "at_risk"
                            rationale = "Moving toward target." if toward else "Not moving toward target."

            if metric == "cost_savings_run_rate":
                if actual is None:
                    rationale = "No quarterly run-rate realization found in evidence text."
                elif target_level is not None:
                    hi_ref = target_high_q if target_high_q is not None else float(target_level)
                    rem_lo = max(0.0, float(target_level) - float(actual))
                    rem_hi = max(0.0, float(hi_ref) - float(actual))
                    realized_lbl = _q_label(latest_runrate_q) if latest_runrate_q is not None else "N/A"
                    rationale = (
                        f"Realized run-rate (stated {realized_lbl}): ${float(actual)/1_000_000:.1f}m. "
                        f"Remaining to target (low/high): ${rem_lo/1_000_000:.1f}m-${rem_hi/1_000_000:.1f}m."
                    )
                    delta_txt = ""
                    no_prior_numeric = False
                    if latest_runrate_q is not None and prev_runrate_q is not None:
                        prev_rr = runrate_by_q.get(prev_runrate_q)
                        curr_rr = runrate_by_q.get(latest_runrate_q)
                        if prev_rr is not None and curr_rr is not None:
                            d_rr = float(curr_rr) - float(prev_rr)
                            d_lbl = f"{'+' if d_rr >= 0 else '-'}${abs(d_rr)/1_000_000:.1f}m"
                            if (_q_ord(latest_runrate_q) - _q_ord(prev_runrate_q)) <= 1:
                                delta_txt = f" Delta QoQ: {d_lbl}."
                            else:
                                delta_txt = f" Delta since last numeric mention ({_q_label(prev_runrate_q)}): {d_lbl}."
                    elif latest_runrate_q is not None and prev_runrate_q is None:
                        no_prior_numeric = True
                    elif latest_increment_q is not None:
                        inc_cur = increment_by_q.get(latest_increment_q)
                        if inc_cur is not None:
                            if prev_increment_q is not None and (_q_ord(latest_increment_q) - _q_ord(prev_increment_q)) <= 1:
                                delta_txt = f" Delta QoQ (increment): {'+' if inc_cur >= 0 else '-'}${abs(float(inc_cur))/1_000_000:.1f}m."
                            elif prev_increment_q is not None:
                                delta_txt = (
                                    f" Delta since last numeric mention ({_q_label(prev_increment_q)}): "
                                    f"{'+' if inc_cur >= 0 else '-'}${abs(float(inc_cur))/1_000_000:.1f}m."
                                )
                            else:
                                no_prior_numeric = True
                    if no_prior_numeric:
                        delta_txt = " No prior numeric run-rate disclosed."
                    if delta_txt:
                        rationale = f"{rationale} {delta_txt}".strip()
                    if range_src_q is not None:
                        rationale = f"{rationale} Active target range source: {_q_label(range_src_q)}."
                    if timing_note:
                        tn_lbl = _q_label(timing_note_q)
                        rationale = f"{rationale} Timing note (stated {tn_lbl}): {timing_note}."
                    if carried_forward and latest_runrate_q is not None:
                        rationale = (
                            f"{rationale} Carried forward: no new numeric run-rate disclosure since {_q_label(latest_runrate_q)}."
                        )
                    proxy_delta = _cost_proxy_opex_yoy(qd_cur)
                    if proxy_delta is not None:
                        rationale = (
                            f"{rationale} Proxy indicator (not disclosed run-rate): "
                            f"(SG&A+R&D) YoY delta {proxy_delta/1_000_000:+.1f}m."
                        )

            status_changed = bool(prev_status and prev_status != status)
            if status_changed and evidence_obj_current is None:
                qa_severity = "FAIL"
                qa_message = "status changed without new evidence"

            out_rows.append({
                "quarter": q.date(),
                "promise_id": pid,
                "promise_key": promise_key,
                "status": status,
                "status_score": score_map[status],
                "rationale": rationale,
                "metric_refs": metric,
                "actual": actual,
                "target": target_level,
                "target_bucket": pr.get("target_bucket"),
                "promise_type": promise_type,
                "scorable": bool(pr.get("scorable")),
                "guidance_type": guidance_type,
                "deadline": deadline.date(),
                "source_evidence_json": json.dumps(evidence_obj, ensure_ascii=True) if evidence_obj else "",
                "status_changed": status_changed,
                "status_history_json": "",
                "first_seen_evidence_quarter": first_ev_date,
                "last_seen_evidence_quarter": last_ev_date,
                "last_seen_numeric_quarter": last_num_date,
                "last_seen_text_quarter": last_txt_date,
                "carried_to_quarter": q.date(),
                "numeric_update_this_quarter": bool(numeric_update_this_q),
                "qa_severity": qa_severity,
                "qa_message": qa_message,
            })
            prev_actual = actual
            prev_status = status
            status_hist.append({"quarter": q.date().isoformat(), "status": status, "evidence": evidence_obj})

        if status_hist:
            status_hist_json = json.dumps(status_hist, ensure_ascii=True)
            for i in range(len(out_rows) - 1, -1, -1):
                if out_rows[i].get("promise_id") == pid:
                    out_rows[i]["status_history_json"] = status_hist_json
                else:
                    break

    if not out_rows:
        return _empty_progress_df()
    out_df = pd.DataFrame(out_rows)
    if not out_df.empty:
        count_df = (
            out_df.groupby(["quarter", "status"]).size().unstack(fill_value=0).sort_index()
            if {"quarter", "status"}.issubset(out_df.columns)
            else pd.DataFrame()
        )
        if not count_df.empty:
            for q, row in count_df.iterrows():
                print(
                    "[promise_progress] "
                    f"{q} open={int(row.get('open', 0))} on_track={int(row.get('on_track', 0))} "
                    f"at_risk={int(row.get('at_risk', 0))} achieved={int(row.get('achieved', 0))} "
                    f"broken={int(row.get('broken', 0))} unclear={int(row.get('unclear', 0))}",
                    flush=True,
                )
    return out_df
def _clean_doc_source(doc: Any) -> Optional[str]:
    if doc is None:
        return None
    try:
        if pd.isna(doc):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    s = str(doc).strip()
    if not s:
        return None
    # Fix malformed source strings like "None/C:\\..."
    s = re.sub(r"^(?:none|nan)[/\\]+", "", s, flags=re.I)
    return s


def _normalize_doc_paths(doc: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    s = _clean_doc_source(doc)
    if not s:
        return None, None, None
    if re.match(r"^[a-z]+://", s, re.I):
        name = Path(s.split("?")[0]).name
        return s, s, name or None
    p = Path(s).expanduser()
    try:
        p_abs = p.resolve() if p.is_absolute() else (Path.cwd() / p).resolve()
    except Exception:
        p_abs = p
    abs_s = str(p_abs)
    try:
        rel_s = str(p_abs.relative_to(Path.cwd()))
    except Exception:
        rel_s = p_abs.name
    return abs_s, rel_s, p_abs.name if p_abs.name else None


def _make_doc_intel_doc_registry() -> Dict[str, Any]:
    return {
        "paths": {},
        "text": {},
        "units": {},
    }


def _doc_registry_paths(
    doc: Any,
    doc_registry: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    raw = _clean_doc_source(doc)
    if not raw:
        return None, None, None
    path_cache = doc_registry.setdefault("paths", {})
    if raw not in path_cache:
        path_cache[raw] = _normalize_doc_paths(raw)
    return path_cache[raw]


def _doc_registry_text_key(
    doc: Any,
    doc_registry: Dict[str, Any],
) -> Optional[str]:
    doc_abs, doc_rel, _ = _doc_registry_paths(doc, doc_registry)
    return doc_abs or doc_rel or _clean_doc_source(doc)


def _doc_registry_seed_text(
    doc: Any,
    text: str,
    doc_registry: Dict[str, Any],
) -> None:
    cache_key = _doc_registry_text_key(doc, doc_registry)
    if not cache_key:
        return
    doc_registry.setdefault("text", {})[cache_key] = text or ""


def _doc_registry_load_text(
    doc: Any,
    doc_registry: Dict[str, Any],
    *,
    cache_dir: Optional[Path] = None,
    rebuild_doc_text_cache: bool = False,
    quiet_pdf_warnings: bool = True,
) -> str:
    cache_key = _doc_registry_text_key(doc, doc_registry)
    if not cache_key:
        return ""
    text_cache = doc_registry.setdefault("text", {})
    if cache_key in text_cache:
        return text_cache[cache_key]
    doc_abs, _, _ = _doc_registry_paths(doc, doc_registry)
    if not doc_abs:
        text_cache[cache_key] = ""
        return ""
    p = Path(doc_abs)
    if not p.exists() or p.is_dir():
        text_cache[cache_key] = ""
        return ""
    txt = ""
    try:
        if p.suffix.lower() == ".pdf":
            txt = _extract_pdf_text_cached(
                p,
                cache_root=cache_dir,
                rebuild_cache=rebuild_doc_text_cache,
                quiet_pdf_warnings=quiet_pdf_warnings,
            )
        else:
            txt = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        txt = ""
    text_cache[cache_key] = txt
    return txt


def _doc_registry_units_for(
    doc: Any,
    snippet_text: str,
    doc_registry: Dict[str, Any],
    *,
    cache_dir: Optional[Path] = None,
    rebuild_doc_text_cache: bool = False,
    quiet_pdf_warnings: bool = True,
) -> Optional[str]:
    cache_key = _doc_registry_text_key(doc, doc_registry)
    if cache_key:
        units_cache = doc_registry.setdefault("units", {})
        if cache_key in units_cache:
            unit = units_cache[cache_key]
            return unit if unit is not None else _detect_units(snippet_text)
        txt = _doc_registry_load_text(
            doc,
            doc_registry,
            cache_dir=cache_dir,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
        )
        unit = _detect_units(txt)
        units_cache[cache_key] = unit
        if unit is not None:
            return unit
    return _detect_units(snippet_text)


def _source_kind_from_row(row: pd.Series) -> str:
    src = str(row.get("source") or "").lower()
    src_type = str(row.get("source_type") or "").lower()
    method = str(row.get("method") or "").lower()
    col = str(row.get("col") or "").lower()
    conf = str(row.get("confidence") or "").lower()
    doc = str(row.get("doc") or row.get("evidence_doc") or "").lower()
    doc_type = str(row.get("doc_type") or "").lower()
    if not src and not src_type and not method and not doc:
        return "no_source"
    if "derived_ytd" in src or "derived_ytd" in method:
        return "derived_ytd"
    slides_hit = (
        ("earnings_deck" in src_type)
        or ("slides" in src_type)
        or ("slides" in src)
        or ("presentation" in src)
        or ("slides" in doc)
        or ("presentation" in doc)
        or (doc.endswith(".pdf") and ("slides" in doc_type or "pdf" in doc_type))
    )
    if slides_hit and "derived" not in src and "fallback" not in src and "heuristic" not in method:
        return "slides_direct"
    if "derived" in src or "heuristic" in src or "fallback" in src or "fallback" in method:
        return "fallback_unknown"
    if "ex99" in src or "filing" in src_type or src in {"direct", "xbrl"}:
        return "filing_direct"
    if "ocr" in col and conf in {"low", ""}:
        return "fallback_unknown"
    return "fallback_unknown"


def _detect_units(text: str) -> Optional[str]:
    t = re.sub(r"\s+", " ", str(text or "")).strip()
    if not t:
        return None
    for unit, pat in NON_GAAP_UNIT_PATTERNS:
        if pat.search(t):
            return unit
    return None


def _build_non_gaap_cred(
    hist: pd.DataFrame,
    adj_metrics: pd.DataFrame,
    adj_breakdown: pd.DataFrame,
    non_gaap_files: Optional[pd.DataFrame] = None,
    cache_dir: Optional[Path] = None,
    rebuild_doc_text_cache: bool = False,
    quiet_pdf_warnings: bool = True,
    doc_registry: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if hist is None or hist.empty or adj_metrics is None or adj_metrics.empty:
        return pd.DataFrame()
    shared_doc_registry = doc_registry if doc_registry is not None else _make_doc_intel_doc_registry()

    def _qa_label(score: float) -> str:
        if score >= 80:
            return "high"
        if score >= 60:
            return "med"
        return "low"

    h = hist.copy()
    h["quarter"] = pd.to_datetime(h["quarter"], errors="coerce")
    h = h[h["quarter"].notna()]
    a = adj_metrics.copy()
    a["quarter"] = pd.to_datetime(a["quarter"], errors="coerce")
    a = a[a["quarter"].notna()]
    if h.empty or a.empty:
        return pd.DataFrame()

    hist_lookup: Dict[pd.Timestamp, Dict[str, Any]] = {}
    for rec in h.to_dict("records"):
        q_ts = pd.to_datetime(rec.get("quarter"), errors="coerce")
        if pd.isna(q_ts):
            continue
        q_key = pd.Timestamp(q_ts)
        if q_key not in hist_lookup:
            hist_lookup[q_key] = {
                "gaap_ebit": pd.to_numeric(rec.get("op_income"), errors="coerce"),
                "revenue": pd.to_numeric(rec.get("revenue"), errors="coerce"),
            }

    a["_row_order"] = range(len(a))
    if "confidence" in a.columns:
        a["_rank"] = a["confidence"].astype(str).map(_confidence_rank).fillna(0)
    else:
        a["_rank"] = 0
    a = a.sort_values(["quarter", "_rank", "_row_order"], kind="stable")
    adj_rows = [dict(rec) for rec in a.groupby("quarter", sort=True).tail(1).drop(columns=["_rank", "_row_order"]).to_dict("records")]
    if not adj_rows:
        return pd.DataFrame()

    breakdown_summary: Dict[pd.Timestamp, Dict[str, Any]] = {}
    bd = adj_breakdown.copy() if adj_breakdown is not None else pd.DataFrame()
    if not bd.empty and "quarter" in bd.columns:
        bd["quarter"] = pd.to_datetime(bd["quarter"], errors="coerce")
        bd = bd[bd["quarter"].notna()]
        if "value" in bd.columns:
            bd["value"] = pd.to_numeric(bd["value"], errors="coerce")
        if "label" in bd.columns and "value" in bd.columns:
            bd_valid = bd[bd["value"].notna()].copy()
            if not bd_valid.empty:
                grouped = (
                    bd_valid.groupby(["quarter", "label"], dropna=False)["value"]
                    .sum()
                    .reset_index()
                )
                for q_ts, sub in grouped.groupby("quarter", sort=True):
                    ranked = sub.sort_values("value", key=lambda ser: ser.abs(), ascending=False, kind="stable").head(3)
                    top_labels = [(None, None), (None, None), (None, None)]
                    for i, rec in enumerate(ranked.to_dict("records")):
                        top_labels[i] = (
                            str(rec.get("label")) if pd.notna(rec.get("label")) else None,
                            float(rec.get("value")) if pd.notna(rec.get("value")) else None,
                        )
                    q_key = pd.Timestamp(q_ts)
                    breakdown_summary[q_key] = {
                        "top_labels": top_labels,
                        "recon_sum": float(pd.to_numeric(ranked["value"], errors="coerce").sum()) if not ranked.empty else 0.0,
                        "has_values": True,
                    }
            if not bd.empty:
                for q_ts, sub in bd.groupby("quarter", sort=True):
                    q_key = pd.Timestamp(q_ts)
                    entry = breakdown_summary.setdefault(
                        q_key,
                        {
                            "top_labels": [(None, None), (None, None), (None, None)],
                            "recon_sum": 0.0,
                            "has_values": False,
                        },
                    )
                    valid_values = pd.to_numeric(sub.get("value"), errors="coerce")
                    if valid_values.notna().any():
                        entry["recon_sum"] = float(valid_values.fillna(0.0).sum())
                        entry["has_values"] = True

    nf_ok_quarters: set[pd.Timestamp] = set()
    nf = non_gaap_files.copy() if non_gaap_files is not None else pd.DataFrame()
    if not nf.empty and "quarter" in nf.columns:
        nf["quarter"] = pd.to_datetime(nf["quarter"], errors="coerce")
        nf = nf[nf["quarter"].notna()]
        if "status" in nf.columns:
            nf["status"] = nf["status"].astype(str)
            ok_mask = nf["status"].str.contains(r"\bok\b", case=False, regex=True)
            nf_ok_quarters = {pd.Timestamp(q) for q in nf.loc[ok_mask, "quarter"].tolist() if pd.notna(q)}

    rows: List[Dict[str, Any]] = []
    for row_a in adj_rows:
        q_ts = pd.to_datetime(row_a.get("quarter"), errors="coerce")
        if pd.isna(q_ts):
            continue
        q_key = pd.Timestamp(q_ts)
        qd = q_key.date()
        hist_row = hist_lookup.get(q_key, {})
        gaap_ebit = hist_row.get("gaap_ebit")
        revenue = hist_row.get("revenue")
        adj_ebit = pd.to_numeric(row_a.get("adj_ebit"), errors="coerce")
        adj_ebitda = pd.to_numeric(row_a.get("adj_ebitda"), errors="coerce")
        total_adj = float(adj_ebit) - float(gaap_ebit) if pd.notna(adj_ebit) and pd.notna(gaap_ebit) else None

        breakdown_info = breakdown_summary.get(
            q_key,
            {
                "top_labels": [(None, None), (None, None), (None, None)],
                "recon_sum": 0.0,
                "has_values": False,
            },
        )
        top_labels = list(breakdown_info.get("top_labels") or [(None, None), (None, None), (None, None)])

        pct = abs(float(total_adj)) / abs(float(adj_ebit)) if pd.notna(adj_ebit) and float(adj_ebit) != 0 and total_adj is not None else None
        accn = row_a.get("accn")
        doc_raw = row_a.get("doc")
        doc_abs, doc_rel, pdf_name = _doc_registry_paths(doc_raw, shared_doc_registry)
        source_kind = _source_kind_from_row(row_a)
        page = row_a.get("page")
        snippet = str(row_a.get("source_snippet") or row_a.get("note") or "")
        snippet_short = re.sub(r"\s+", " ", snippet).strip()[:220]

        score = 100.0
        warn_count = 0
        fail_count = 0
        reasons: List[str] = []

        # 1) Source existence
        source_exists = False
        if doc_abs:
            p = Path(doc_abs)
            source_exists = p.exists()
        if (not source_exists) and doc_rel:
            source_exists = bool(doc_rel)
        if (not source_exists) and q_key in nf_ok_quarters:
            source_exists = True
        if not source_exists:
            score -= 15
            warn_count += 1
            reasons.append("WARN: source not found for adjusted metric in sec_cache/non_gaap files")

        # 2) Quarter alignment
        quarter_aligned = True
        m_snip_q = re.search(r"three\s+months\s+ended\s+([A-Za-z]+)\s+([0-9]{1,2}),\s*(20\d{2})", snippet, re.I)
        if m_snip_q:
            mon = m_snip_q.group(1).lower()
            months = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6, "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12}
            if months.get(mon, 0) != qd.month or int(m_snip_q.group(2)) != qd.day or int(m_snip_q.group(3)) != qd.year:
                quarter_aligned = False
        elif pdf_name:
            m_q = re.search(r"_Q([1-4])[_\-](20\d{2})", pdf_name, re.I)
            if m_q:
                qn = int(m_q.group(1))
                yy = int(m_q.group(2))
                expected = date(yy, 3, 31) if qn == 1 else date(yy, 6, 30) if qn == 2 else date(yy, 9, 30) if qn == 3 else date(yy, 12, 31)
                if expected != qd:
                    quarter_aligned = False
        if not quarter_aligned:
            score -= 35
            fail_count += 1
            reasons.append("FAIL: extracted adjusted row does not align with quarter_end")

        # 3) Units sanity
        unit_text = " ".join(
            [
                str(row_a.get("source_snippet") or ""),
                str(row_a.get("note") or ""),
                str(row_a.get("doc") or ""),
            ]
        )
        units_detected = _doc_registry_units_for(
            doc_abs or doc_rel or doc_raw,
            unit_text,
            shared_doc_registry,
            cache_dir=cache_dir,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
        )
        units_known = units_detected is not None
        if not units_known:
            # Slides often omit an explicit unit token in OCR text; keep as warning but smaller penalty.
            score -= 3 if source_kind == "slides_direct" else 8
            warn_count += 1
            if source_kind == "slides_direct":
                reasons.append("WARN: units not explicitly detected in slides source text")
            else:
                reasons.append("WARN: units not explicitly detected in source text")
        if pd.notna(revenue):
            rev_v = float(revenue)
            for metric_name, metric_val in [("adj_ebit", adj_ebit), ("adj_ebitda", adj_ebitda)]:
                if pd.notna(metric_val):
                    mv = abs(float(metric_val))
                    if rev_v > 0 and mv > rev_v * 1.2:
                        score -= 35
                        fail_count += 1
                        reasons.append(f"FAIL: {metric_name} exceeds revenue (units sanity)")
                    if rev_v > 0 and mv > rev_v * 10 and (mv / 1000.0) <= rev_v * 1.5:
                        score -= 25
                        fail_count += 1
                        reasons.append(f"FAIL: {metric_name} appears 1,000x scaled vs GAAP size")

        # 4) Reconciliation check (if recon exists)
        recon_exists = bool(breakdown_info.get("has_values"))
        recon_diff = None
        if recon_exists and pd.notna(gaap_ebit) and pd.notna(adj_ebit):
            recon_sum = float(breakdown_info.get("recon_sum") or 0.0)
            expected = float(gaap_ebit) + recon_sum
            recon_diff = expected - float(adj_ebit)
            tol = max(5_000_000.0, 0.05 * abs(float(adj_ebit)))
            if abs(recon_diff) > tol:
                score -= 30
                fail_count += 1
                reasons.append(f"FAIL: reconciliation mismatch > tolerance (diff={recon_diff:,.0f})")

        risky_source = source_kind in {"derived_ytd", "fallback_unknown", "no_source"}

        rows.append(
            {
                "quarter": qd,
                "gaap_ebit": float(gaap_ebit) if pd.notna(gaap_ebit) else None,
                "adj_ebit": float(adj_ebit) if pd.notna(adj_ebit) else None,
                "adj_ebitda": float(adj_ebitda) if pd.notna(adj_ebitda) else None,
                "revenue": float(revenue) if pd.notna(revenue) else None,
                "total_adjustments": total_adj,
                "top_adjustment_1_label": top_labels[0][0],
                "top_adjustment_1_value": top_labels[0][1],
                "top_adjustment_2_label": top_labels[1][0],
                "top_adjustment_2_value": top_labels[1][1],
                "top_adjustment_3_label": top_labels[2][0],
                "top_adjustment_3_value": top_labels[2][1],
                "adjustments_pct_of_adj_ebit": pct,
                "trend_flag": "",
                "evidence_accn": accn,
                "evidence_doc": doc_abs or doc_rel or _clean_doc_source(doc_raw),
                "evidence_doc_abs": doc_abs,
                "evidence_doc_rel": doc_rel,
                "evidence_pdf_name": pdf_name,
                "evidence_page": page,
                "evidence_snippet": snippet_short,
                "source_kind": source_kind,
                "units_detected": units_detected,
                "qa_score": max(0.0, min(100.0, score)),
                "qa_label": _qa_label(max(0.0, min(100.0, score))),
                "qa_status": "FAIL" if fail_count > 0 else ("WARN" if warn_count > 0 else "PASS"),
                "qa_warn_count": warn_count,
                "qa_fail_count": fail_count,
                "qa_reasons_json": json.dumps(reasons, ensure_ascii=True),
                "qa_reasons_text": " | ".join(reasons),
                "qa_source_exists": source_exists,
                "qa_quarter_alignment": quarter_aligned,
                "qa_units_known": units_known,
                "qa_recon_diff": recon_diff,
                "qa_fallback_source": risky_source,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values("quarter").reset_index(drop=True)

    # 5) Consistency over time (count streak only for derived_ytd/fallback_unknown)
    run = 0
    for i in range(len(out)):
        risky = str(out.loc[i, "source_kind"] or "") in {"derived_ytd", "fallback_unknown", "no_source"}
        if risky:
            run += 1
        else:
            run = 0
        if run == 3:
            if out.loc[i, "qa_status"] == "PASS":
                out.loc[i, "qa_status"] = "WARN"
            out.loc[i, "qa_warn_count"] = int(out.loc[i, "qa_warn_count"]) + 1
            out.loc[i, "qa_score"] = max(0.0, float(out.loc[i, "qa_score"]) - 8.0)
            out.loc[i, "qa_reasons_text"] = (str(out.loc[i, "qa_reasons_text"]) + " | WARN: >2 quarters derived/fallback source in a row").strip()
        elif run == 5:
            out.loc[i, "qa_status"] = "FAIL"
            out.loc[i, "qa_fail_count"] = int(out.loc[i, "qa_fail_count"]) + 1
            out.loc[i, "qa_score"] = max(0.0, float(out.loc[i, "qa_score"]) - 15.0)
            out.loc[i, "qa_reasons_text"] = (str(out.loc[i, "qa_reasons_text"]) + " | FAIL: >4 quarters derived/fallback source in a row").strip()

    for i in range(1, len(out)):
        prev_adj = pd.to_numeric(out.loc[i - 1, "adj_ebitda"], errors="coerce")
        curr_adj = pd.to_numeric(out.loc[i, "adj_ebitda"], errors="coerce")
        prev_gaap = pd.to_numeric(out.loc[i - 1, "gaap_ebit"], errors="coerce")
        curr_gaap = pd.to_numeric(out.loc[i, "gaap_ebit"], errors="coerce")
        if pd.notna(prev_adj) and pd.notna(curr_adj) and abs(prev_adj) > 1:
            adj_jump = abs(float(curr_adj - prev_adj)) / abs(float(prev_adj))
            gaap_jump = None
            if pd.notna(prev_gaap) and pd.notna(curr_gaap) and abs(prev_gaap) > 1:
                gaap_jump = abs(float(curr_gaap - prev_gaap)) / abs(float(prev_gaap))
            if adj_jump > 1.5 and (gaap_jump is None or gaap_jump < 0.3):
                if out.loc[i, "qa_status"] == "PASS":
                    out.loc[i, "qa_status"] = "WARN"
                out.loc[i, "qa_warn_count"] = int(out.loc[i, "qa_warn_count"]) + 1
                out.loc[i, "qa_score"] = max(0.0, float(out.loc[i, "qa_score"]) - 10.0)
                out.loc[i, "qa_reasons_text"] = str(out.loc[i, "qa_reasons_text"]) + " | WARN: adjusted metric jump unsupported by GAAP delta"

    out["qa_label"] = out["qa_score"].apply(lambda x: _qa_label(float(x)))
    pct_series = pd.to_numeric(out["adjustments_pct_of_adj_ebit"], errors="coerce")
    for i in range(len(out)):
        flag = ""
        if i >= 3:
            w = pct_series.iloc[i - 3 : i + 1]
            if w.notna().all() and all(float(w.iloc[j]) < float(w.iloc[j - 1]) for j in range(1, 4)):
                flag = "adjustments shrinking (4Q)"
        out.loc[i, "trend_flag"] = flag
        if out.loc[i, "qa_status"] in ("WARN", "FAIL"):
            print(
                f"[non_gaap_cred] {out.loc[i, 'quarter']} status={out.loc[i, 'qa_status']} score={float(out.loc[i, 'qa_score']):.1f} "
                f"source_kind={out.loc[i, 'source_kind']} pdf={out.loc[i, 'evidence_pdf_name']} page={out.loc[i, 'evidence_page']} "
                f"units={out.loc[i, 'units_detected'] or 'unknown'} parsed_value={out.loc[i, 'adj_ebitda']} "
                f"snippet={str(out.loc[i, 'evidence_snippet'])[:140]} reasons={out.loc[i, 'qa_reasons_text']}",
                flush=True,
            )
    return out


def build_doc_intel_outputs(
    sec: SecClient,
    cik_int: int,
    submissions: Dict[str, Any],
    hist: pd.DataFrame,
    adj_metrics: pd.DataFrame,
    adj_breakdown: pd.DataFrame,
    non_gaap_files: Optional[pd.DataFrame] = None,
    revolver_history: Optional[pd.DataFrame] = None,
    debt_buckets: Optional[pd.DataFrame] = None,
    earnings_release_dir: Optional[Path] = None,
    max_docs: int = 80,
    max_quarters: int = 24,
    cache_dir: Optional[Path] = None,
    rebuild_doc_text_cache: bool = False,
    quiet_pdf_warnings: bool = True,
    stage_timings: Optional[Dict[str, float]] = None,
    profile_timings: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    doc_registry = _make_doc_intel_doc_registry()
    with _timed_substage(stage_timings, "doc_intel.quarter_notes", enabled=profile_timings):
        quarter_notes = build_quarter_notes_v2(
            sec=sec,
            cik_int=cik_int,
            submissions=submissions,
            hist=hist,
            adj_metrics=adj_metrics,
            adj_breakdown=adj_breakdown,
            revolver_history=revolver_history,
            debt_buckets=debt_buckets,
            max_docs=max_docs,
            max_quarters=max_quarters,
            quiet_pdf_warnings=quiet_pdf_warnings,
        )
    with _timed_substage(stage_timings, "doc_intel.promises", enabled=profile_timings):
        promises = _build_promises(
            quarter_notes=quarter_notes,
            sec=sec,
            cik_int=cik_int,
            submissions=submissions,
            hist=hist,
            earnings_release_dir=earnings_release_dir,
            max_docs=max_docs,
            max_quarters=max_quarters,
            cache_dir=cache_dir,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
            doc_registry=doc_registry,
        )
    with _timed_substage(stage_timings, "doc_intel.promise_progress", enabled=profile_timings):
        promise_progress = _build_progress(promises, hist, adj_metrics)
    with _timed_substage(stage_timings, "doc_intel.non_gaap_cred", enabled=profile_timings):
        non_gaap_cred = _build_non_gaap_cred(
            hist,
            adj_metrics,
            adj_breakdown,
            non_gaap_files=non_gaap_files,
            cache_dir=cache_dir,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
            doc_registry=doc_registry,
        )
    return quarter_notes, promises, promise_progress, non_gaap_cred
