"""Quarter-note construction helpers and supporting text-shaping logic."""
from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .filing_evidence_shared import (
    confidence_rank as _confidence_rank,
    extract_document_text as _extract_text,
    filing_quarter_end as _filing_quarter_end,
    format_pct as _pct,
    history_quarter_ends as _history_quarter_ends,
    iter_submission_batches as _iter_submission_batches,
    pick_filing_docs as _pick_filing_docs,
    qualify_renderable_note as _qualify_renderable_note,
    split_sentences as _split_sentences,
)
from .sec_xbrl import SecClient, normalize_accession, parse_date


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

NOTE_ANCHORS: Tuple[str, ...] = (
    "we expect",
    "we will",
    "target",
    "remain on track",
    "by 2026",
    "reduction",
    "run-rate",
    "exit rate",
    "recurring revenue",
)

TIME_ANCHOR_RE = re.compile(
    r"\b("
    r"this quarter|next quarter|this year|next year|"
    r"by\s+(?:the\s+end\s+of\s+)?20\d{2}|during\s+20\d{2}|in\s+20\d{2}|"
    r"q[1-4]\s*20\d{2}|fy\s*20\d{2}|"
    r"march|april|may|june|july|august|september|october|november|december"
    r")\b",
    re.I,
)

HIGH_SIGNAL_SECTIONS: Dict[str, Tuple[str, ...]] = {
    "Outlook / Guidance": ("outlook", "guidance", "target", "expect", "on track", "forecast"),
    "Debt / Refinancing": ("debt", "refinancing", "credit facility", "revolver", "liquidity", "covenant"),
    "Restructuring / Transformation": ("restructuring", "transformation", "cost out", "cost savings", "transition"),
    "Segment performance highlights": ("segment", "sendtech", "presort", "performance highlights"),
    "One-time items / adjustments": ("one-time", "special", "adjusted", "reconciliation", "impairment", "redemption"),
}

BOILERPLATE_RE = re.compile(
    r"\b("
    r"safe harbor|forward[- ]looking statements?|"
    r"risk factors?|no obligation to update|"
    r"defined terms?|for additional information|"
    r"this press release contains|"
    r"cautionary statement"
    r")\b",
    re.I,
)


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
def _stable_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _normalize_note_text(text: str) -> str:
    s = (text or "").lower()
    s = re.sub(r"\d+(?:[.,]\d+)?", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _anchor_hit(text: str) -> Optional[str]:
    s = (text or "").lower()
    for anchor in NOTE_ANCHORS:
        if anchor in s:
            return anchor
    return None


def _has_time_anchor(text: str) -> bool:
    return bool(TIME_ANCHOR_RE.search(text or ""))


def _doc_path_from_cache(sec: SecClient, accn_no_dashes: str, doc_name: str) -> str:
    safe_doc = re.sub(r"[^\w\-.]+", "_", str(doc_name or "doc"))
    p = sec.cache_dir / f"doc_{accn_no_dashes}_{safe_doc}"
    if p.exists():
        return str(p)
    return str(sec.cache_dir / f"doc_{accn_no_dashes}_{doc_name}")


def _doc_type(form: str, doc_name: str) -> str:
    d = (doc_name or "").lower()
    f = (form or "").upper()
    if d.endswith(".pdf"):
        return "pdf"
    if d.endswith(".htm") or d.endswith(".html") or d.endswith(".xhtml"):
        return "html"
    if "slide" in d or "presentation" in d:
        return "slides"
    if f.startswith("8-K"):
        return "8-k"
    if f.startswith("10-Q"):
        return "10-q"
    if f.startswith("10-K"):
        return "10-k"
    return "filing"


def _extract_high_signal_windows(text: str) -> List[Tuple[str, str]]:
    txt = text or ""
    if not txt:
        return []
    low = txt.lower()
    windows: List[Tuple[int, int, str]] = []
    for section, keywords in HIGH_SIGNAL_SECTIONS.items():
        for kw in keywords:
            for m in re.finditer(re.escape(kw.lower()), low):
                start = max(0, m.start() - 220)
                end = min(len(txt), m.end() + 2400)
                windows.append((start, end, section))
    if not windows:
        return []
    windows.sort(key=lambda x: (x[0], x[1]))
    merged: List[Tuple[int, int, str]] = []
    for start, end, sec in windows:
        if not merged:
            merged.append((start, end, sec))
            continue
        p_start, p_end, p_sec = merged[-1]
        if start <= p_end:
            merged[-1] = (p_start, max(p_end, end), p_sec)
        else:
            merged.append((start, end, sec))
    out: List[Tuple[str, str]] = []
    for start, end, sec in merged:
        chunk = txt[start:end].strip()
        if len(chunk) >= 120:
            out.append((sec, chunk))
    return out


def _none_if_nan(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except Exception:
        return None
    if pd.isna(f):
        return None
    return f
def _fmt_money(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    return f"${float(v) / 1_000_000:,.1f}m"


def _metric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _rolling_ttm(s: pd.Series) -> pd.Series:
    return s.rolling(4, min_periods=4).sum()


def _topic_confidence(form: str, doc: str) -> str:
    f = (form or "").upper()
    d = (doc or "").lower()
    if f.startswith("10-Q") or f.startswith("10-K"):
        return "high"
    if "slide" in d or "presentation" in d or d.endswith(".pdf"):
        return "low"
    if f.startswith("8-K"):
        return "med"
    return "med"


def _score_text_sentence(sentence: str) -> Tuple[Optional[str], float]:
    s = sentence.lower()
    best_topic: Optional[str] = None
    best_hits = 0
    for topic, pat in TEXT_SIGNAL_PATTERNS.items():
        hits = len(pat.findall(s))
        if hits > best_hits:
            best_hits = hits
            best_topic = topic
    if not best_topic or best_hits <= 0:
        return None, 0.0
    has_num = bool(re.search(r"[$%]|\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b", sentence))
    sev = min(100.0, 16.0 + best_hits * 18.0 + (14.0 if has_num else 0.0))
    return best_topic, sev


def _make_candidate(
    quarter_end: date,
    topic: str,
    metric: str,
    headline: str,
    body: str,
    severity_score: float,
    confidence: str,
    evidence: List[Dict[str, Any]],
    method: str,
    metric_value: Optional[float] = None,
) -> NoteCandidate:
    note_id = _stable_hash(f"{topic}|{metric}|{quarter_end.isoformat()}")
    return NoteCandidate(
        note_id=note_id,
        quarter_end=quarter_end,
        topic=topic,
        metric=metric,
        headline=headline.strip()[:140],
        body=body.strip()[:420],
        severity_score=max(0.0, min(100.0, float(severity_score))),
        confidence=confidence if confidence in {"high", "med", "low"} else "med",
        evidence=evidence[:6],
        method=method,
        metric_value=metric_value,
    )


def _generate_numeric_candidates(hist: pd.DataFrame) -> List[NoteCandidate]:
    if hist is None or hist.empty or "quarter" not in hist.columns:
        return []
    h = hist.copy()
    h["quarter"] = pd.to_datetime(h["quarter"], errors="coerce")
    h = h[h["quarter"].notna()].sort_values("quarter").drop_duplicates("quarter", keep="last").reset_index(drop=True)
    if h.empty:
        return []

    rev = _metric_series(h, "revenue")
    ebitda = _metric_series(h, "ebitda")
    cfo = _metric_series(h, "cfo")
    capex = _metric_series(h, "capex")
    debt = _metric_series(h, "debt_core")
    cash = _metric_series(h, "cash")
    int_exp = _metric_series(h, "interest_expense_net")
    shares_out = _metric_series(h, "shares_outstanding")
    shares_dil = _metric_series(h, "shares_diluted")
    shares = shares_out if shares_out.notna().any() else shares_dil

    revenue_ttm = _rolling_ttm(rev)
    ebitda_ttm = _rolling_ttm(ebitda)
    fcf_q = cfo - capex
    fcf_ttm = _rolling_ttm(fcf_q)
    int_ttm = _rolling_ttm(int_exp)

    rev_ttm_lag = revenue_ttm.shift(4)
    rev_ttm_yoy = (revenue_ttm - rev_ttm_lag) / rev_ttm_lag.abs()
    rev_ttm_yoy[(rev_ttm_lag == 0) | rev_ttm_lag.isna() | revenue_ttm.isna()] = pd.NA

    margin_ttm = ebitda_ttm / revenue_ttm.replace({0.0: pd.NA})
    margin_ttm_yoy_bps = (margin_ttm - margin_ttm.shift(4)) * 10_000.0

    fcf_ttm_lag = fcf_ttm.shift(4)
    fcf_ttm_yoy = (fcf_ttm - fcf_ttm_lag) / fcf_ttm_lag.abs()
    fcf_ttm_yoy[(fcf_ttm_lag == 0) | fcf_ttm_lag.isna() | fcf_ttm.isna()] = pd.NA
    fcf_ttm_delta_yoy = fcf_ttm - fcf_ttm_lag

    net_debt = debt - cash
    net_debt_qoq = net_debt - net_debt.shift(1)
    net_debt_yoy = net_debt - net_debt.shift(4)

    interest_cov = pd.Series([float("nan")] * len(h), index=h.index, dtype=float)
    mask = int_ttm < 0
    interest_cov[mask] = ebitda_ttm[mask] / int_ttm[mask].abs()
    interest_cov_yoy = interest_cov - interest_cov.shift(4)

    shares_lag = shares.shift(4)
    shares_yoy = (shares - shares_lag) / shares_lag.abs()
    shares_yoy[(shares_lag == 0) | shares_lag.isna() | shares.isna()] = pd.NA

    out: List[NoteCandidate] = []
    for i, r in h.iterrows():
        q = pd.Timestamp(r["quarter"]).date()

        y_rev = _none_if_nan(rev_ttm_yoy.iloc[i])
        if y_rev is not None and abs(y_rev) >= 0.04:
            out.append(
                _make_candidate(
                    q,
                    "Revenue",
                    "revenue_ttm_yoy",
                    "Revenue TTM trend inflected" if y_rev > 0 else "Revenue TTM still under pressure",
                    (
                        f"Revenue TTM YoY at {q}: {_pct(y_rev)} "
                        f"(TTM {_fmt_money(_none_if_nan(revenue_ttm.iloc[i]))}, "
                        f"LY {_fmt_money(_none_if_nan(rev_ttm_lag.iloc[i]))})."
                    ),
                    min(100.0, abs(y_rev) * 320.0),
                    "high",
                    [{
                        "doc_path": "History_Q",
                        "doc_type": "model_metric",
                        "section_or_page": q.isoformat(),
                        "metric_ref": "revenue_ttm_yoy",
                        "extracted_value": y_rev,
                        "snippet": f"Revenue TTM YoY {_pct(y_rev)}",
                    }],
                    "metric_delta",
                    metric_value=y_rev,
                )
            )

        bps = _none_if_nan(margin_ttm_yoy_bps.iloc[i])
        if bps is not None and abs(bps) >= 100:
            out.append(
                _make_candidate(
                    q,
                    "Margin",
                    "ebitda_margin_ttm_yoy_bps",
                    "EBITDA margin expanded" if bps > 0 else "EBITDA margin compressed",
                    (
                        f"EBITDA margin TTM at {q}: {_pct(_none_if_nan(margin_ttm.iloc[i]))} "
                        f"vs LY {_pct(_none_if_nan(margin_ttm.shift(4).iloc[i]))} "
                        f"({bps:+.0f} bps)."
                    ),
                    min(100.0, abs(bps) / 6.0),
                    "high",
                    [{
                        "doc_path": "History_Q",
                        "doc_type": "model_metric",
                        "section_or_page": q.isoformat(),
                        "metric_ref": "ebitda_margin_ttm_yoy_bps",
                        "extracted_value": bps,
                        "snippet": f"EBITDA margin delta {bps:+.0f} bps",
                    }],
                    "metric_delta",
                    metric_value=bps,
                )
            )

        y_fcf = _none_if_nan(fcf_ttm_yoy.iloc[i])
        d_fcf = _none_if_nan(fcf_ttm_delta_yoy.iloc[i])
        if (y_fcf is not None and abs(y_fcf) >= 0.20) or (d_fcf is not None and abs(d_fcf) >= 25_000_000):
            sev = min(100.0, max(abs(y_fcf or 0.0) * 250.0, abs((d_fcf or 0.0) / 5_000_000.0)))
            out.append(
                _make_candidate(
                    q,
                    "FCF",
                    "fcf_ttm_delta_yoy",
                    "FCF TTM accelerated" if (d_fcf or 0.0) > 0 else "FCF TTM weakened",
                    f"FCF TTM at {q}: {_fmt_money(_none_if_nan(fcf_ttm.iloc[i]))}, YoY {_pct(y_fcf)}, delta {_fmt_money(d_fcf)}.",
                    sev,
                    "high",
                    [{
                        "doc_path": "History_Q",
                        "doc_type": "model_metric",
                        "section_or_page": q.isoformat(),
                        "metric_ref": "fcf_ttm_delta_yoy",
                        "extracted_value": d_fcf,
                        "snippet": f"FCF TTM YoY delta {_fmt_money(d_fcf)}",
                    }],
                    "metric_delta",
                    metric_value=d_fcf,
                )
            )

        d_nd_yoy = _none_if_nan(net_debt_yoy.iloc[i])
        d_nd_qoq = _none_if_nan(net_debt_qoq.iloc[i])
        if (d_nd_yoy is not None and abs(d_nd_yoy) >= 50_000_000) or (d_nd_qoq is not None and abs(d_nd_qoq) >= 25_000_000):
            sev = min(100.0, max(abs((d_nd_yoy or 0.0) / 12_000_000.0), abs((d_nd_qoq or 0.0) / 8_000_000.0)))
            metric_name = "net_debt_yoy_delta" if d_nd_yoy is not None else "net_debt_qoq_delta"
            metric_val = d_nd_yoy if d_nd_yoy is not None else d_nd_qoq
            out.append(
                _make_candidate(
                    q,
                    "Debt",
                    metric_name,
                    "Net debt declined" if (d_nd_yoy or 0.0) < 0 else "Net debt increased",
                    (
                        f"Net debt at {q}: {_fmt_money(_none_if_nan(net_debt.iloc[i]))}; "
                        f"QoQ delta {_fmt_money(d_nd_qoq)}, YoY delta {_fmt_money(d_nd_yoy)}."
                    ),
                    sev,
                    "high",
                    [{
                        "doc_path": "History_Q",
                        "doc_type": "model_metric",
                        "section_or_page": q.isoformat(),
                        "metric_ref": metric_name,
                        "extracted_value": metric_val,
                        "snippet": f"Net debt delta {_fmt_money(metric_val)}",
                    }],
                    "metric_delta",
                    metric_value=metric_val,
                )
            )

        d_cov = _none_if_nan(interest_cov_yoy.iloc[i])
        cov_now = _none_if_nan(interest_cov.iloc[i])
        if d_cov is not None and abs(d_cov) >= 0.5 and cov_now is not None:
            out.append(
                _make_candidate(
                    q,
                    "Debt",
                    "interest_coverage_yoy_delta",
                    "Interest coverage improved" if d_cov > 0 else "Interest coverage weakened",
                    f"Interest coverage at {q}: {cov_now:.2f}x (YoY delta {d_cov:+.2f}x).",
                    min(100.0, abs(d_cov) * 35.0),
                    "high",
                    [{
                        "doc_path": "Leverage_Liquidity",
                        "doc_type": "model_metric",
                        "section_or_page": q.isoformat(),
                        "metric_ref": "interest_coverage_yoy_delta",
                        "extracted_value": d_cov,
                        "snippet": f"Coverage YoY delta {d_cov:+.2f}x",
                    }],
                    "metric_delta",
                    metric_value=d_cov,
                )
            )

        y_sh = _none_if_nan(shares_yoy.iloc[i])
        sh_now = _none_if_nan(shares.iloc[i])
        sh_ly = _none_if_nan(shares_lag.iloc[i])
        if y_sh is not None and abs(y_sh) >= 0.01 and sh_now is not None and sh_ly is not None:
            out.append(
                _make_candidate(
                    q,
                    "Equity",
                    "shares_yoy",
                    "Share count reduced" if y_sh < 0 else "Share count increased",
                    f"Shares YoY at {q}: {_pct(y_sh)} (now {sh_now / 1_000_000:.2f}m, LY {sh_ly / 1_000_000:.2f}m).",
                    min(100.0, abs(y_sh) * 350.0),
                    "high",
                    [{
                        "doc_path": "History_Q",
                        "doc_type": "model_metric",
                        "section_or_page": q.isoformat(),
                        "metric_ref": "shares_yoy",
                        "extracted_value": y_sh,
                        "snippet": f"Shares YoY {_pct(y_sh)}",
                    }],
                    "metric_delta",
                    metric_value=y_sh,
                )
            )
    return out


def _generate_text_candidates(
    sec: SecClient,
    cik_int: int,
    submissions: Dict[str, Any],
    target_quarters: set[date],
    max_docs: int,
    quiet_pdf_warnings: bool = True,
) -> Tuple[List[NoteCandidate], Dict[date, Dict[str, int]]]:
    docs_scanned = 0
    min_filing_date = datetime.utcnow().date() - timedelta(days=365 * 8)
    out: List[NoteCandidate] = []
    stats: Dict[date, Dict[str, int]] = {}

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
            docs = _pick_filing_docs(pdn, items, max_docs=8)

            for doc in docs:
                if docs_scanned >= max_docs:
                    break
                try:
                    blob = sec.download_document(cik_int, accn_nd, doc)
                except Exception:
                    continue
                txt = _extract_text(doc, blob, quiet_pdf_warnings=quiet_pdf_warnings)
                if len(txt) < 220 and str(doc).lower().endswith((".htm", ".html", ".xhtml")):
                    try:
                        ocr_txt = sec.ocr_html_assets(
                            accn_nd,
                            blob,
                            context={
                                "doc": doc,
                                "quarter": q_end.isoformat() if q_end is not None else "",
                                "purpose": "quarter_notes_v2",
                                "report_date": str(rep or ""),
                                "filing_date": str(filed_d or ""),
                            },
                        )
                    except Exception:
                        ocr_txt = ""
                    if len(ocr_txt or "") > len(txt):
                        txt = re.sub(r"\s+", " ", html.unescape(str(ocr_txt))).strip()
                if len(txt) < 220:
                    continue
                docs_scanned += 1
                q_stats = stats.setdefault(q_end, {"raw": 0, "filtered_boilerplate": 0, "anchor_miss": 0})
                doc_t = _doc_type(form, doc)
                doc_path = _doc_path_from_cache(sec, accn_nd, doc)
                windows = _extract_high_signal_windows(txt)
                if not windows:
                    continue
                per_doc: List[NoteCandidate] = []
                for section_name, section_text in windows:
                    for sent in _split_sentences(section_text):
                        q_stats["raw"] += 1
                        if BOILERPLATE_RE.search(sent):
                            q_stats["filtered_boilerplate"] += 1
                            continue
                        anchor_hit = _anchor_hit(sent)
                        if not anchor_hit:
                            q_stats["anchor_miss"] += 1
                            continue
                        topic, score = _score_text_sentence(sent)
                        if not topic:
                            continue
                        conf = _topic_confidence(form, doc)
                        score = max(0.0, score - (12.0 if conf == "low" else 0.0))
                        per_doc.append(
                            _make_candidate(
                                q_end,
                                topic,
                                f"text:{topic.lower()}",
                                f"{topic} signal in filing text",
                                sent[:320],
                                score,
                                conf,
                                [
                                    {
                                        "accn": accn,
                                        "form": form,
                                        "doc_name": doc,
                                        "doc_path": doc_path,
                                        "doc_type": doc_t,
                                        "section_or_page": section_name,
                                        "anchor_hit": anchor_hit,
                                        "table_idx": None,
                                        "extracted_value": None,
                                        "metric_ref": f"text:{topic.lower()}",
                                        "snippet": sent[:320],
                                    }
                                ],
                                "keyword_scan",
                            )
                        )
                if per_doc:
                    out.extend(sorted(per_doc, key=lambda x: x.severity_score, reverse=True)[:8])
            if docs_scanned >= max_docs:
                break
        if docs_scanned >= max_docs:
            break
    return out, stats


def _generate_non_gaap_candidates(adj_metrics: pd.DataFrame, adj_breakdown: pd.DataFrame) -> List[NoteCandidate]:
    out: List[NoteCandidate] = []
    if adj_breakdown is not None and not adj_breakdown.empty and "quarter" in adj_breakdown.columns:
        bd = adj_breakdown.copy()
        bd["quarter"] = pd.to_datetime(bd["quarter"], errors="coerce")
        bd["value"] = pd.to_numeric(bd.get("value"), errors="coerce")
        bd = bd[bd["quarter"].notna() & bd["value"].notna()]
        for q, grp in bd.groupby("quarter"):
            grp = grp.copy()
            grp["abs_v"] = grp["value"].abs()
            top = grp.sort_values("abs_v", ascending=False).head(2)
            for _, r in top.iterrows():
                v = float(r["value"])
                lbl = str(r.get("label") or "adjustment")
                out.append(
                    _make_candidate(
                        pd.Timestamp(q).date(),
                        "One-offs",
                        f"adj_breakdown:{lbl}",
                        "Material non-GAAP adjustment",
                        f"Non-GAAP reconciliation includes {lbl}: {_fmt_money(v)} for {pd.Timestamp(q).date()}.",
                        min(100.0, 18.0 + abs(v) / 8_000_000.0),
                        "med",
                        [
                            {
                                "accn": r.get("accn"),
                                "doc_name": r.get("doc"),
                                "doc_path": r.get("doc"),
                                "doc_type": "non_gaap",
                                "section_or_page": pd.Timestamp(q).date().isoformat(),
                                "table_idx": None,
                                "extracted_value": v,
                                "metric_ref": f"adj_breakdown:{lbl}",
                                "snippet": str(r.get("note") or lbl)[:240],
                            }
                        ],
                        "non_gaap_recon",
                        metric_value=v,
                    )
                )
    if adj_metrics is not None and not adj_metrics.empty and "quarter" in adj_metrics.columns and "adj_ebitda" in adj_metrics.columns:
        am = adj_metrics.copy()
        am["quarter"] = pd.to_datetime(am["quarter"], errors="coerce")
        am["adj_ebitda"] = pd.to_numeric(am["adj_ebitda"], errors="coerce")
        am = am[am["quarter"].notna() & am["adj_ebitda"].notna()].sort_values("quarter")
        if not am.empty:
            am["adj_ebitda_yoy"] = (am["adj_ebitda"] - am["adj_ebitda"].shift(4)) / am["adj_ebitda"].shift(4).abs()
            am.loc[
                (am["adj_ebitda"].shift(4) == 0) | am["adj_ebitda"].shift(4).isna() | am["adj_ebitda"].isna(),
                "adj_ebitda_yoy",
            ] = pd.NA
            for _, r in am.iterrows():
                y = _none_if_nan(r.get("adj_ebitda_yoy"))
                if y is None or abs(y) < 0.20:
                    continue
                q = pd.Timestamp(r["quarter"]).date()
                out.append(
                    _make_candidate(
                        q,
                        "Margin",
                        "adj_ebitda_yoy",
                        "Adjusted EBITDA moved materially",
                        f"Adjusted EBITDA YoY at {q}: {_pct(y)} (value {_fmt_money(_none_if_nan(r['adj_ebitda']))}).",
                        min(100.0, abs(y) * 260.0),
                        "med",
                        [{
                            "accn": r.get("accn"),
                            "doc_name": r.get("doc"),
                            "doc_path": r.get("doc"),
                            "doc_type": "non_gaap",
                            "section_or_page": q.isoformat(),
                            "metric_ref": "adj_ebitda_yoy",
                            "extracted_value": y,
                            "snippet": f"Adjusted EBITDA YoY {_pct(y)}",
                        }],
                        "non_gaap_recon",
                        metric_value=y,
                    )
                )
    return out


def _generate_financing_candidates(revolver_history: Optional[pd.DataFrame], debt_buckets: Optional[pd.DataFrame]) -> List[NoteCandidate]:
    out: List[NoteCandidate] = []
    if revolver_history is not None and not revolver_history.empty and "quarter" in revolver_history.columns:
        rh = revolver_history.copy()
        rh["quarter"] = pd.to_datetime(rh["quarter"], errors="coerce")
        rh = rh[rh["quarter"].notna()].sort_values("quarter").reset_index(drop=True)
        for i, r in rh.iterrows():
            q = pd.Timestamp(r["quarter"]).date()
            commit = _none_if_nan(r.get("revolver_commitment"))
            drawn = _none_if_nan(r.get("revolver_drawn"))
            avail = _none_if_nan(r.get("revolver_availability"))
            util = _none_if_nan(r.get("revolver_utilization"))
            prev = rh.iloc[i - 1] if i > 0 else None
            p_commit = _none_if_nan(prev.get("revolver_commitment")) if prev is not None else None
            p_drawn = _none_if_nan(prev.get("revolver_drawn")) if prev is not None else None
            p_avail = _none_if_nan(prev.get("revolver_availability")) if prev is not None else None
            snippet = str(r.get("source_snippet") or r.get("commitment_snippet") or r.get("note") or "")[:260]
            evidence = {
                "accn": r.get("accn"),
                "doc_name": r.get("doc"),
                "doc_path": r.get("doc"),
                "doc_type": "revolver",
                "section_or_page": q.isoformat(),
                "table_idx": None,
                "snippet": snippet,
            }

            if commit is not None and p_commit is not None and abs(commit - p_commit) >= 50_000_000:
                delta = commit - p_commit
                out.append(
                    _make_candidate(
                        q,
                        "Debt",
                        "revolver_capacity_change",
                        "Revolver capacity changed",
                        f"Revolver capacity changed to {_fmt_money(commit)} at {q} from {_fmt_money(p_commit)}.",
                        min(100.0, 30.0 + abs(delta) / 8_000_000.0),
                        "high",
                        [dict(evidence, metric_ref="revolver_commitment", extracted_value=commit)],
                        "financing_event",
                        metric_value=delta,
                    )
                )
            if drawn is not None and p_drawn is not None and abs(drawn - p_drawn) >= 25_000_000:
                delta = drawn - p_drawn
                out.append(
                    _make_candidate(
                        q,
                        "Debt",
                        "revolver_drawn_change",
                        "Revolver usage changed",
                        f"Revolver drawn changed to {_fmt_money(drawn)} at {q} (prev {_fmt_money(p_drawn)}).",
                        min(100.0, 25.0 + abs(delta) / 6_000_000.0),
                        "high",
                        [dict(evidence, metric_ref="revolver_drawn", extracted_value=drawn)],
                        "financing_event",
                        metric_value=delta,
                    )
                )
            if avail is not None and p_avail is not None and abs(avail - p_avail) >= 50_000_000:
                delta = avail - p_avail
                out.append(
                    _make_candidate(
                        q,
                        "Debt",
                        "revolver_availability_change",
                        "Revolver availability changed",
                        f"Revolver availability moved to {_fmt_money(avail)} at {q} (delta {_fmt_money(delta)}).",
                        min(100.0, 20.0 + abs(delta) / 8_000_000.0),
                        "med",
                        [dict(evidence, metric_ref="revolver_availability", extracted_value=avail)],
                        "financing_event",
                        metric_value=delta,
                    )
                )
            if util is not None and util > 0.10:
                out.append(
                    _make_candidate(
                        q,
                        "Debt",
                        "revolver_utilization",
                        "Revolver utilization notable",
                        f"Revolver utilization at {q} is {util * 100:.1f}%.",
                        min(100.0, 20.0 + util * 120.0),
                        "med",
                        [dict(evidence, metric_ref="revolver_utilization", extracted_value=util)],
                        "financing_event",
                        metric_value=util,
                    )
                )

    if debt_buckets is not None and not debt_buckets.empty:
        db = debt_buckets.copy()
        if "as_of" in db.columns:
            db["as_of"] = pd.to_datetime(db["as_of"], errors="coerce")
        if "Unknown_pct" in db.columns:
            db["Unknown_pct"] = pd.to_numeric(db["Unknown_pct"], errors="coerce")
        if "Bucket_coverage_pct" in db.columns:
            db["Bucket_coverage_pct"] = pd.to_numeric(db["Bucket_coverage_pct"], errors="coerce")
        for _, r in db.iterrows():
            q = pd.to_datetime(r.get("as_of"), errors="coerce")
            uq = _none_if_nan(r.get("Unknown_pct"))
            cov = _none_if_nan(r.get("Bucket_coverage_pct"))
            if pd.isna(q) or uq is None or uq <= 0.10:
                continue
            out.append(
                _make_candidate(
                    pd.Timestamp(q).date(),
                    "Debt",
                    "debt_bucket_unknown_pct",
                    "Debt maturity unknown bucket elevated",
                    f"Unknown maturity bucket is {_pct(uq)} at {pd.Timestamp(q).date()} (coverage {_pct(cov)}).",
                    min(100.0, 20.0 + uq * 220.0),
                    "med",
                    [{
                        "doc_path": "Debt_Buckets",
                        "doc_type": "model_metric",
                        "section_or_page": pd.Timestamp(q).date().isoformat(),
                        "metric_ref": "Unknown_pct",
                        "extracted_value": uq,
                        "snippet": f"Unknown maturity {_pct(uq)}",
                    }],
                    "financing_event",
                    metric_value=uq,
                )
            )
    return out


def _dedupe_candidates(candidates: List[NoteCandidate]) -> Tuple[List[NoteCandidate], Dict[date, int]]:
    if not candidates:
        return [], {}
    by_key: Dict[Tuple[date, str], NoteCandidate] = {}
    deduped_counts: Dict[date, int] = {}

    def _evidence_score(c: NoteCandidate) -> int:
        ev0 = c.evidence[0] if c.evidence else {}
        score = 0
        if ev0.get("doc_path"):
            score += 2
        if ev0.get("section_or_page"):
            score += 2
        if ev0.get("snippet"):
            score += 1
        if ev0.get("anchor_hit"):
            score += 1
        return score

    for c in candidates:
        norm = _normalize_note_text(c.body or c.headline)
        if not norm:
            norm = _normalize_note_text(c.headline)
        key = (c.quarter_end, norm)
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = c
            continue
        pick_new = False
        if c.severity_score > prev.severity_score:
            pick_new = True
        elif c.severity_score == prev.severity_score and _evidence_score(c) > _evidence_score(prev):
            pick_new = True
        if pick_new:
            by_key[key] = c
        deduped_counts[c.quarter_end] = deduped_counts.get(c.quarter_end, 0) + 1

    out = sorted(by_key.values(), key=lambda x: (x.quarter_end, -x.severity_score, x.note_id))
    return out, deduped_counts


def _select_candidates(candidates: List[NoteCandidate]) -> List[NoteCandidate]:
    if not candidates:
        return []
    by_q: Dict[date, List[NoteCandidate]] = {}
    for c in candidates:
        by_q.setdefault(c.quarter_end, []).append(c)

    selected: List[NoteCandidate] = []
    prev_severity: Dict[str, float] = {}
    for q in sorted(by_q.keys()):
        cands = sorted(by_q[q], key=lambda x: (x.severity_score, _confidence_rank(x.confidence)), reverse=True)
        picked: List[NoteCandidate] = []
        picked_ids: set[str] = set()
        topic_counts: Dict[str, int] = {}
        fallback_pool: List[NoteCandidate] = []
        deferred_pool: List[NoteCandidate] = []

        for c in cands:
            key = f"{c.topic}|{c.metric}"
            if c.severity_score < TOPIC_MATERIALITY.get(c.topic, 25.0):
                fallback_pool.append(c)
                continue
            if key in prev_severity and c.severity_score <= prev_severity[key]:
                deferred_pool.append(c)
                continue
            if topic_counts.get(c.topic, 0) >= 2:
                deferred_pool.append(c)
                continue
            if c.confidence == "low" and len(picked) >= 3:
                deferred_pool.append(c)
                continue
            if c.note_id in picked_ids:
                continue
            picked.append(c)
            picked_ids.add(c.note_id)
            topic_counts[c.topic] = topic_counts.get(c.topic, 0) + 1
            if len(picked) >= 8:
                break

        if len(picked) < 4:
            for c in fallback_pool:
                if c.note_id in picked_ids:
                    continue
                if topic_counts.get(c.topic, 0) >= 2:
                    continue
                if c.confidence == "low" and len(picked) >= 3:
                    continue
                picked.append(c)
                picked_ids.add(c.note_id)
                topic_counts[c.topic] = topic_counts.get(c.topic, 0) + 1
                if len(picked) >= 4:
                    break
        if len(picked) < 4:
            for c in deferred_pool:
                if c.note_id in picked_ids:
                    continue
                if c.confidence == "low" and len(picked) >= 3:
                    continue
                picked.append(c)
                picked_ids.add(c.note_id)
                if len(picked) >= 4:
                    break

        picked = sorted(picked, key=lambda x: (x.severity_score, _confidence_rank(x.confidence)), reverse=True)[:8]
        selected.extend(picked)
        for c in picked:
            prev_severity[f"{c.topic}|{c.metric}"] = c.severity_score
    return selected


def _to_rows(candidates: List[NoteCandidate]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    per_q_rank: Dict[date, int] = {}
    for c in sorted(candidates, key=lambda x: (x.quarter_end, -x.severity_score, x.note_id)):
        per_q_rank[c.quarter_end] = per_q_rank.get(c.quarter_end, 0) + 1
        ev = c.evidence or []
        ev0 = ev[0] if ev else {}
        severity_label = "high" if c.severity_score >= 70 else ("med" if c.severity_score >= 40 else "low")
        claim = c.headline if c.headline else c.body
        evidence_doc = str(ev0.get("doc_path") or "").strip()
        evidence_loc = str(ev0.get("section_or_page") or "").strip()
        evidence_snippet = str(ev0.get("snippet") or c.body or c.headline)[:380]
        qa_flags: List[str] = []
        if not evidence_doc or not evidence_loc or not evidence_snippet:
            qa_flags.append("FAIL:evidence_missing")
        anchor_payload = " ".join([str(claim or ""), str(c.body or ""), evidence_snippet, str(ev0.get("anchor_hit") or "")])
        if not _has_time_anchor(anchor_payload):
            qa_flags.append("WARN:time_anchor_missing")
        category = TOPIC_TO_CATEGORY.get(c.topic, "Strategy / segment")
        renderable = _qualify_renderable_note(
            c.body or c.headline or evidence_snippet,
            source_type=ev0.get("doc_type") or "",
            metric_hint=c.metric,
            theme_hint=c.topic,
            base_score=c.severity_score,
        )
        rows.append(
            {
                "quarter": c.quarter_end,
                "tag": c.topic,
                "severity": severity_label,
                "claim": claim,
                "evidence": json.dumps(ev0, ensure_ascii=True),
                "evidence_doc": evidence_doc or None,
                "evidence_loc": evidence_loc or None,
                "qa_flags": " | ".join(qa_flags),
                "quarter_end": c.quarter_end,
                "rank": per_q_rank[c.quarter_end],
                "note_id": c.note_id,
                "topic": c.topic,
                "category": category,
                "headline": c.headline,
                "body": c.body,
                "note": c.headline,
                "severity_score": c.severity_score,
                "confidence": c.confidence,
                "metric_ref": c.metric,
                "metric_value": c.metric_value,
                "evidence_json": json.dumps(ev, ensure_ascii=True),
                "evidence_snippet": evidence_snippet,
                "anchor_hit": ev0.get("anchor_hit"),
                "section_or_page": ev0.get("section_or_page"),
                "doc_path": ev0.get("doc_path"),
                "doc_type": ev0.get("doc_type"),
                "accn": ev0.get("accn"),
                "form": ev0.get("form"),
                "doc": ev0.get("doc_name"),
                "method": c.method,
                "renderable_note": bool(renderable),
                "render_summary": renderable.summary if renderable else "",
                "render_bucket": renderable.bucket if renderable else "",
                "render_score": renderable.display_score if renderable else pd.NA,
                "render_change": renderable.change_state if renderable else "",
                "render_drop_reason": "" if renderable else "non_renderable_note",
                "render_preferred_source": bool(renderable.preferred_source) if renderable else False,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["quarter", "rank"]).reset_index(drop=True)


def build_quarter_notes_v2(
    sec: SecClient,
    cik_int: int,
    submissions: Dict[str, Any],
    hist: pd.DataFrame,
    adj_metrics: Optional[pd.DataFrame] = None,
    adj_breakdown: Optional[pd.DataFrame] = None,
    revolver_history: Optional[pd.DataFrame] = None,
    debt_buckets: Optional[pd.DataFrame] = None,
    max_docs: int = 80,
    max_quarters: int = 24,
    quiet_pdf_warnings: bool = True,
) -> pd.DataFrame:
    hq = _history_quarter_ends(hist, max_quarters=max_quarters)
    if not hq:
        return pd.DataFrame()
    target_quarters = set(hq)

    candidates: List[NoteCandidate] = []
    candidates.extend(_generate_numeric_candidates(hist))
    text_candidates, text_stats = _generate_text_candidates(
        sec,
        cik_int,
        submissions,
        target_quarters,
        max_docs=max_docs,
        quiet_pdf_warnings=quiet_pdf_warnings,
    )
    candidates.extend(text_candidates)
    candidates.extend(_generate_non_gaap_candidates(adj_metrics if adj_metrics is not None else pd.DataFrame(), adj_breakdown if adj_breakdown is not None else pd.DataFrame()))
    candidates.extend(_generate_financing_candidates(revolver_history, debt_buckets))
    deduped, dedup_counts = _dedupe_candidates(candidates)
    selected = _select_candidates(deduped)
    out = _to_rows(selected)
    if not out.empty:
        per_q = out.groupby("quarter").size().to_dict()
        for q in sorted(per_q.keys()):
            tstat = text_stats.get(q, {})
            print(
                f"[quarter_notes] {q} n_notes={int(per_q.get(q, 0))} "
                f"n_filtered_boilerplate={int(tstat.get('filtered_boilerplate', 0))} "
                f"n_deduped={int(dedup_counts.get(q, 0))}",
                flush=True,
            )
    return out


def validate_quarter_notes(quarter_notes: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if quarter_notes is None or quarter_notes.empty:
        return pd.DataFrame(rows)

    h = hist.copy() if hist is not None else pd.DataFrame()
    if not h.empty and "quarter" in h.columns:
        h["quarter"] = pd.to_datetime(h["quarter"], errors="coerce").dt.date
    else:
        h = pd.DataFrame(columns=["quarter"])

    for _, r in quarter_notes.iterrows():
        q = pd.to_datetime(r.get("quarter"), errors="coerce")
        if pd.isna(q):
            continue
        qd = q.date()
        note_id = str(r.get("note_id") or "")
        ev_list: List[Dict[str, Any]] = []
        ev_raw = r.get("evidence_json")
        if isinstance(ev_raw, str) and ev_raw.strip():
            try:
                parsed = json.loads(ev_raw)
                if isinstance(parsed, list):
                    ev_list = parsed
            except Exception:
                ev_list = []
        if not ev_list:
            rows.append(
                {
                    "quarter": qd,
                    "metric": "quarter_notes",
                    "check": "quarter_note_evidence_missing",
                    "status": "fail",
                    "message": f"quarter note {note_id or '<unknown>'} has no evidence payload.",
                    "note_id": note_id,
                }
            )
            continue

        ev0 = ev_list[0] if ev_list else {}
        doc_path = str(ev0.get("doc_path") or "").strip()
        section_or_page = str(ev0.get("section_or_page") or "").strip()
        snippet = str(ev0.get("snippet") or "").strip()
        if not doc_path or not section_or_page or not snippet:
            rows.append(
                {
                    "quarter": qd,
                    "metric": "quarter_notes",
                    "check": "quarter_note_evidence_incomplete",
                    "status": "fail",
                    "message": (
                        f"quarter note {note_id or '<unknown>'} evidence missing required fields "
                        f"(doc_path/section_or_page/snippet)."
                    ),
                    "note_id": note_id,
                }
            )

        anchor_payload = " ".join(
            [
                str(r.get("claim") or ""),
                str(r.get("body") or ""),
                str(ev0.get("anchor_hit") or ""),
                str(ev0.get("snippet") or ""),
            ]
        )
        if not _has_time_anchor(anchor_payload):
            rows.append(
                {
                    "quarter": qd,
                    "metric": "quarter_notes",
                    "check": "quarter_note_missing_time_anchor",
                    "status": "warn",
                    "message": f"quarter note {note_id or '<unknown>'} has no explicit time anchor.",
                    "note_id": note_id,
                }
            )

        metric_ref = str(r.get("metric_ref") or "").strip()
        if not metric_ref or metric_ref.startswith("text:"):
            continue
        metric_ok = _none_if_nan(r.get("metric_value")) is not None
        if not metric_ok:
            metric_name = metric_ref.split(":", 1)[-1]
            hq = h[h["quarter"] == qd]
            if (not hq.empty) and metric_name in hq.columns:
                metric_ok = _none_if_nan(hq.iloc[0].get(metric_name)) is not None
        if not metric_ok:
            rows.append(
                {
                    "quarter": qd,
                    "metric": metric_ref,
                    "check": "quarter_note_metric_nan",
                    "status": "fail",
                    "message": f"quarter note {note_id or '<unknown>'} references metric '{metric_ref}' with NA value.",
                    "note_id": note_id,
                }
            )
    return pd.DataFrame(rows)
