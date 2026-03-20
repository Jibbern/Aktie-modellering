from __future__ import annotations

import json
import re
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .doc_intel import extract_pdf_text_cached
from .cache_layout import ticker_cache_roots_from_base_dir
from .legacy_support import _coerce_prev_quarter_end, _is_quarter_end, _path_belongs_to_ticker
from .non_gaap import infer_quarter_end_from_text, strip_html
from .sec_xbrl import normalize_accession, parse_date


def path_cache_key(path_in: Path) -> str:
    try:
        return str(path_in.resolve())
    except Exception:
        return str(path_in)


def read_cached_doc_raw(
    path_in: Path,
    *,
    document_cache: Any,
    pdf_text_cache_root: Path,
    rebuild_doc_text_cache: bool,
    quiet_pdf_warnings: bool,
) -> str:
    cache_key = path_cache_key(path_in)
    cached = document_cache.raw_text_by_path.get(cache_key)
    if cached is not None:
        return cached
    raw = ""
    suf = path_in.suffix.lower()
    try:
        if suf == ".pdf":
            raw = extract_pdf_text_cached(
                path_in,
                cache_root=pdf_text_cache_root,
                rebuild_cache=rebuild_doc_text_cache,
                quiet_pdf_warnings=quiet_pdf_warnings,
            )
        else:
            try:
                raw = path_in.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                raw = path_in.read_text(errors="ignore")
    except Exception:
        raw = ""
    document_cache.raw_text_by_path[cache_key] = raw
    return raw


def read_cached_doc_text(
    path_in: Path,
    *,
    document_cache: Any,
    pdf_text_cache_root: Path,
    rebuild_doc_text_cache: bool,
    quiet_pdf_warnings: bool,
    normalize: bool = False,
) -> str:
    cache_key = path_cache_key(path_in)
    cache_bucket = document_cache.normalized_text_by_path if normalize else document_cache.plain_text_by_path
    cached = cache_bucket.get(cache_key)
    if cached is not None:
        return cached
    raw = read_cached_doc_raw(
        path_in,
        document_cache=document_cache,
        pdf_text_cache_root=pdf_text_cache_root,
        rebuild_doc_text_cache=rebuild_doc_text_cache,
        quiet_pdf_warnings=quiet_pdf_warnings,
    )
    if not raw:
        cache_bucket[cache_key] = ""
        return ""
    txt = strip_html(raw) if path_in.suffix.lower() in {".htm", ".html", ".xml"} else raw
    if normalize:
        txt = re.sub(r"\s+", " ", txt).strip()
    cache_bucket[cache_key] = txt
    return txt


def infer_q_from_name(name: str) -> Optional[date]:
    s = str(name or "").lower()
    m = re.search(r"(?:fy|q)([1-4])[\s_-]*([12][0-9]{3})", s, re.I)
    if m:
        qn = int(m.group(1))
        yr = int(m.group(2))
        md = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}.get(qn)
        if md:
            return date(yr, md[0], md[1])
    m2 = re.search(r"([12][0-9]{3}).{0,12}q([1-4])", s, re.I)
    if m2:
        yr = int(m2.group(1))
        qn = int(m2.group(2))
        md = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}.get(qn)
        if md:
            return date(yr, md[0], md[1])
    return None


def infer_cached_doc_quarter(
    path_in: Path,
    *,
    document_cache: Any,
    parse_quarter_from_filename: Callable[[str], Optional[date]],
    parse_quarter_from_follow_text: Callable[[str], Optional[date]],
    text: Any = None,
    latest_q_hint: Any = None,
    include_follow_text: bool = False,
) -> Optional[date]:
    cache_key = path_cache_key(path_in)
    cached = document_cache.inferred_quarter_by_path.get(cache_key)
    if cached is not None:
        return cached
    raw_txt = str(text or "")
    q_guess: Optional[date] = None
    if include_follow_text:
        q_guess = parse_quarter_from_filename(path_in.name)
        if q_guess is None and raw_txt:
            q_guess = parse_quarter_from_follow_text(raw_txt)
    if q_guess is None:
        q_guess = infer_quarter_end_from_text(raw_txt) if raw_txt else None
    if q_guess is None:
        q_guess = infer_q_from_name(path_in.name)
    if isinstance(q_guess, date):
        q_guess = q_guess if _is_quarter_end(q_guess) else _coerce_prev_quarter_end(q_guess)
        document_cache.inferred_quarter_by_path[cache_key] = q_guess
        return q_guess
    fallback = pd.to_datetime(latest_q_hint, errors="coerce")
    if pd.notna(fallback):
        q_fallback = pd.Timestamp(fallback).date()
        return q_fallback if _is_quarter_end(q_fallback) else _coerce_prev_quarter_end(q_fallback)
    return None


def sec_docs_for_accession(
    accn_in: Any,
    *,
    cache_root: Path,
    document_cache: Any,
) -> List[Path]:
    accn_nd = normalize_accession(accn_in) if accn_in else ""
    if not accn_nd or not cache_root.exists():
        return []
    cached = document_cache.accession_doc_paths.get(accn_nd)
    if cached is not None:
        return list(cached)
    cands: List[Path] = []
    for pat in [f"doc_{accn_nd}_*.htm", f"doc_{accn_nd}_*.html", f"doc_{accn_nd}_*.txt"]:
        cands.extend(sorted(cache_root.glob(pat)))
    seen: set[str] = set()
    uniq: List[Path] = []
    for path_in in cands:
        doc_key = path_cache_key(path_in)
        if doc_key in seen:
            continue
        seen.add(doc_key)
        uniq.append(path_in)
    document_cache.accession_doc_paths[accn_nd] = list(uniq)
    return list(uniq)


def submission_cache_files(
    *,
    cache_roots: Tuple[Path, ...],
    document_cache: Any,
    max_files: Optional[int] = None,
) -> List[Path]:
    cached = document_cache.submission_files
    if cached is None:
        files: List[Path] = []
        seen: set[str] = set()
        for root_in in cache_roots:
            if not root_in.exists():
                continue
            for sub_path in sorted(root_in.glob("submissions_*.json")):
                sub_key = path_cache_key(sub_path)
                if sub_key in seen:
                    continue
                seen.add(sub_key)
                files.append(sub_path)
        files = sorted(files, key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
        document_cache.submission_files = files
        cached = files
    out = list(cached or [])
    if max_files is not None:
        out = out[: max(0, int(max_files))]
    return out


def submission_recent_row_quarter(row: Dict[str, Any]) -> Optional[date]:
    rep_d = parse_date(row.get("report"))
    if rep_d is not None:
        return rep_d if _is_quarter_end(rep_d) else _coerce_prev_quarter_end(rep_d)
    filed_d = parse_date(row.get("filed"))
    if filed_d is not None:
        return _coerce_prev_quarter_end(filed_d)
    return None


def submission_recent_rows(
    *,
    cache_roots: Tuple[Path, ...],
    document_cache: Any,
    raw_reader: Callable[[Path], str],
    max_files: Optional[int] = None,
) -> List[Dict[str, Any]]:
    limit_key = "all" if max_files is None else str(int(max_files))
    cached = document_cache.submission_recent_rows_by_limit.get(limit_key)
    if cached is not None:
        return list(cached)
    out_rows: List[Dict[str, Any]] = []
    for sub_path in submission_cache_files(
        cache_roots=cache_roots,
        document_cache=document_cache,
        max_files=max_files,
    ):
        path_key = path_cache_key(sub_path)
        per_file = document_cache.submission_recent_rows_by_file.get(path_key)
        if per_file is None:
            raw = raw_reader(sub_path)
            try:
                payload = json.loads(raw) if raw else {}
            except Exception:
                payload = {}
            recent = dict(payload.get("filings", {}).get("recent", {}) or {})
            accns = list(recent.get("accessionNumber") or [])
            forms = list(recent.get("form") or [])
            filed = list(recent.get("filingDate") or [])
            report = list(recent.get("reportDate") or [])
            docs = list(recent.get("primaryDocument") or [])
            n = max(len(accns), len(forms), len(filed), len(report), len(docs))
            per_file = []
            for i in range(n):
                per_file.append(
                    {
                        "accn": accns[i] if i < len(accns) else "",
                        "form": forms[i] if i < len(forms) else "",
                        "filed": filed[i] if i < len(filed) else "",
                        "report": report[i] if i < len(report) else "",
                        "doc": docs[i] if i < len(docs) else "",
                    }
                )
            document_cache.submission_recent_rows_by_file[path_key] = list(per_file)
        out_rows.extend(per_file)

    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in out_rows:
        key = (str(row.get("accn") or ""), str(row.get("doc") or ""))
        cur = dedup.get(key)
        filed_cur = pd.to_datetime(cur.get("filed"), errors="coerce") if cur else pd.NaT
        filed_new = pd.to_datetime(row.get("filed"), errors="coerce")
        if cur is None or (pd.notna(filed_new) and (pd.isna(filed_cur) or filed_new >= filed_cur)):
            dedup[key] = row
    rows = sorted(
        dedup.values(),
        key=lambda z: pd.to_datetime(z.get("filed"), errors="coerce")
        if pd.notna(pd.to_datetime(z.get("filed"), errors="coerce"))
        else pd.Timestamp.min,
        reverse=True,
    )
    document_cache.submission_recent_rows_by_limit[limit_key] = list(rows)
    return list(rows)


def resolve_cached_doc_path(
    *,
    cache_roots: Tuple[Path, ...],
    accession_doc_lookup: Callable[[Any], List[Path]],
    accn: Any = "",
    doc_name: Any = "",
    path_hint: Any = "",
) -> Optional[Path]:
    hint_txt = str(path_hint or doc_name or "").strip()
    if hint_txt:
        hint_path = Path(hint_txt)
        if hint_path.exists():
            return hint_path
        for root_in in cache_roots:
            cand = root_in / hint_path.name
            if cand.exists():
                return cand
    docs = accession_doc_lookup(accn)
    if not docs:
        return None
    doc_base = Path(str(doc_name or path_hint or "")).name.lower()
    if doc_base:
        for doc_path in docs:
            if doc_path.name.lower() == doc_base or doc_path.name.lower().endswith(doc_base):
                return doc_path
    return docs[0]


def slide_text_paths(
    *,
    material_roots: Tuple[Path, ...],
    document_cache: Any,
    parse_quarter_from_filename: Callable[[str], Optional[date]],
    kind: str = "all",
    quarter: Optional[date] = None,
) -> List[Path]:
    kind_key = str(kind or "all").strip().lower() or "all"
    if kind_key not in {"all", "text", "ocr"}:
        return []
    if quarter is not None:
        quarter_key = (kind_key, quarter.isoformat())
        cached_quarter = document_cache.slide_paths_by_kind_and_quarter.get(quarter_key)
        if cached_quarter is not None:
            return list(cached_quarter)
        out_quarter = [
            path_in
            for path_in in slide_text_paths(
                material_roots=material_roots,
                document_cache=document_cache,
                parse_quarter_from_filename=parse_quarter_from_filename,
                kind=kind_key,
            )
            if parse_quarter_from_filename(path_in.name) == quarter
        ]
        document_cache.slide_paths_by_kind_and_quarter[quarter_key] = list(out_quarter)
        return list(out_quarter)

    cached_kind = document_cache.slide_paths_by_kind.get(kind_key)
    if cached_kind is not None:
        return list(cached_kind)
    if kind_key == "all":
        combined = slide_text_paths(
            material_roots=material_roots,
            document_cache=document_cache,
            parse_quarter_from_filename=parse_quarter_from_filename,
            kind="text",
        ) + slide_text_paths(
            material_roots=material_roots,
            document_cache=document_cache,
            parse_quarter_from_filename=parse_quarter_from_filename,
            kind="ocr",
        )
        document_cache.slide_paths_by_kind[kind_key] = list(combined)
        return list(combined)

    dir_name = "slides_text" if kind_key == "text" else "slides_ocr"
    out_paths: List[Path] = []
    seen: set[str] = set()
    for root in material_roots:
        for cache_root in ticker_cache_roots_from_base_dir(root):
            slide_dir = cache_root / dir_name
            if not slide_dir.exists() or not slide_dir.is_dir():
                continue
            for path_in in sorted(slide_dir.glob("*.txt")):
                path_key = path_cache_key(path_in)
                if path_key in seen:
                    continue
                seen.add(path_key)
                out_paths.append(path_in)
    document_cache.slide_paths_by_kind[kind_key] = list(out_paths)
    return list(out_paths)


def docs_for_valuation_accn(
    accn_in: str,
    *,
    accession_doc_lookup: Callable[[Any], List[Path]],
) -> List[Path]:
    uniq = accession_doc_lookup(accn_in)
    if not uniq:
        return []

    def _score(path_obj: Path) -> Tuple[int, int]:
        n = path_obj.name.lower()
        s = 0
        if "q4" in n or "q3" in n or "q2" in n or "q1" in n:
            s += 1
        if "press" in n or "ex99" in n or "earnings" in n or "news" in n:
            s += 8
        if "_pbi-" in n:
            s += 7
        if "10q" in n or "10k" in n:
            s += 5
        if "annualletter" in n or "shareholderletter" in n:
            s += 4
        if "ex10" in n or "agreement" in n or "amendment" in n:
            s -= 12
        if "ex31" in n or "ex32" in n:
            s -= 8
        return (s, -len(n))

    return list(sorted(uniq, key=_score, reverse=True)[:12])


def build_valuation_filing_docs_by_quarter(
    qs_local: Tuple[pd.Timestamp, ...],
    audit_df: pd.DataFrame,
    *,
    cache_root: Path,
    resolve_col: Callable[[pd.DataFrame, List[str]], Optional[str]],
    submission_recent_rows_fn: Callable[..., List[Dict[str, Any]]],
    docs_for_valuation_accn_fn: Callable[[str], List[Path]],
    extract_doc_text_fn: Callable[[Path], str],
) -> Dict[pd.Timestamp, List[Dict[str, Any]]]:
    quarter_key = tuple(pd.Timestamp(q).normalize() for q in qs_local if pd.notna(q))
    docs_by_quarter: Dict[pd.Timestamp, List[Dict[str, Any]]] = {pd.Timestamp(q): [] for q in quarter_key}
    q_set = set(docs_by_quarter)
    relevance_re = re.compile(
        r"(repurch|buyback|dividend|authorization|capital allocation|return of capital|treasury\s+stock|issuer purchases of equity securities|common stock purchases)",
        re.I,
    )

    def _fallback_scan_docs() -> Dict[pd.Timestamp, List[Dict[str, Any]]]:
        if not cache_root.exists():
            return docs_by_quarter
        date_to_quarter: Dict[str, pd.Timestamp] = {
            pd.Timestamp(q).strftime("%Y%m%d"): pd.Timestamp(q)
            for q in quarter_key
        }
        for dp in sorted(cache_root.rglob("doc_*")):
            name_low = dp.name.lower()
            qts = None
            for ymd, qv in date_to_quarter.items():
                if ymd in name_low:
                    qts = qv
                    break
            if qts is None or qts not in q_set:
                continue
            txt = extract_doc_text_fn(dp)
            if not txt:
                continue
            txt_low = txt.lower()
            if not relevance_re.search(txt_low):
                continue
            docs_by_quarter[qts].append(
                {
                    "accn_rank": 99,
                    "path": dp,
                    "name": name_low,
                    "text": txt,
                    "text_low": txt_low,
                }
            )
        return docs_by_quarter

    if audit_df is None or audit_df.empty or not cache_root.exists():
        return _fallback_scan_docs()

    accn_col = resolve_col(audit_df, ["accn"])
    quarter_col = resolve_col(audit_df, ["quarter", "quarter_end", "period_end"])
    filed_col = resolve_col(audit_df, ["filed"])
    if accn_col is None or quarter_col is None:
        return _fallback_scan_docs()

    keep_cols = [quarter_col, accn_col] + ([filed_col] if filed_col is not None else [])
    tmp = audit_df.loc[:, keep_cols].copy()
    tmp[quarter_col] = pd.to_datetime(tmp[quarter_col], errors="coerce")
    if filed_col is not None:
        tmp[filed_col] = pd.to_datetime(tmp[filed_col], errors="coerce")
    tmp = tmp.dropna(subset=[quarter_col, accn_col])
    q_to_accns: Dict[pd.Timestamp, List[str]] = {}
    if not tmp.empty:
        for qv, grp in tmp.groupby(quarter_col):
            qts = pd.Timestamp(qv).normalize()
            if qts not in docs_by_quarter:
                continue
            if filed_col is not None and grp[filed_col].notna().any():
                grp_near = grp[
                    (grp[filed_col] >= qts - pd.Timedelta(days=5))
                    & (grp[filed_col] <= qts + pd.Timedelta(days=200))
                ]
                if not grp_near.empty:
                    grp = grp_near
            if filed_col is not None and grp[filed_col].notna().any():
                grp = grp.sort_values(by=filed_col, ascending=False)
            accns: List[str] = []
            for av in grp[accn_col].astype(str).tolist():
                accn_txt = str(av or "").strip()
                if not accn_txt or accn_txt in accns:
                    continue
                accns.append(accn_txt)
            if accns:
                q_to_accns[qts] = accns

    for fr in submission_recent_rows_fn(max_files=8):
        form = str(fr.get("form") or "").upper().strip()
        if not (form.startswith("10-Q") or form.startswith("10-K") or form.startswith("8-K")):
            continue
        accn_txt = str(fr.get("accn") or "").strip()
        if not accn_txt:
            continue
        q_guess = submission_recent_row_quarter(fr)
        if q_guess is None:
            continue
        qts = pd.Timestamp(q_guess).normalize()
        if qts not in q_set:
            continue
        bucket = q_to_accns.setdefault(qts, [])
        if accn_txt not in bucket:
            bucket.append(accn_txt)

    for qts, accns in q_to_accns.items():
        rows: List[Dict[str, Any]] = []
        seen_docs: set[str] = set()
        for accn_rank, accn_txt in enumerate(accns):
            for dp in docs_for_valuation_accn_fn(accn_txt):
                doc_key = path_cache_key(dp)
                if doc_key in seen_docs:
                    continue
                seen_docs.add(doc_key)
                txt = extract_doc_text_fn(dp)
                if not txt:
                    continue
                txt_low = txt.lower()
                if not relevance_re.search(txt_low):
                    continue
                rows.append(
                    {
                        "accn_rank": accn_rank,
                        "path": dp,
                        "name": dp.name.lower(),
                        "text": txt,
                        "text_low": txt_low,
                    }
                )
        docs_by_quarter[qts] = rows
    if not any(docs_by_quarter.get(q) for q in q_set):
        return _fallback_scan_docs()
    return docs_by_quarter


def normalize_leverage_text(text_in: Any) -> str:
    return re.sub(r"\s+", " ", str(text_in or "").strip())


def normalize_leverage_quarter(qv: Any) -> Optional[pd.Timestamp]:
    q = pd.to_datetime(qv, errors="coerce")
    if pd.isna(q):
        return None
    return pd.Timestamp(q).to_period("Q").end_time.normalize()


def hist_quarter_whitelist(hist: Optional[pd.DataFrame]) -> set[pd.Timestamp]:
    if hist is None or hist.empty or "quarter" not in hist.columns:
        return set()
    return {
        pd.Timestamp(q).to_period("Q").end_time.normalize()
        for q in pd.to_datetime(hist["quarter"], errors="coerce").dropna().tolist()
    }


def looks_like_leverage_text(text_in: Any) -> bool:
    low = str(text_in or "").lower()
    if not low:
        return False
    return bool(
        re.search(
            r"(adjusted\s+net\s+leverage|net\s+leverage|net\s+debt\s*(?:to|/)\s*ebitda)",
            low,
            re.I,
        )
    )


def build_leverage_local_material_index(
    *,
    hist: Optional[pd.DataFrame],
    material_roots: Tuple[Path, ...],
    ticker: Optional[str],
    ticker_roots: Tuple[Path, ...],
    read_cached_doc_text_fn: Callable[..., str],
    infer_cached_doc_quarter_fn: Callable[..., Optional[date]],
) -> List[Dict[str, Any]]:
    latest_q_hint = None
    if hist is not None and not hist.empty and "quarter" in hist.columns:
        try:
            latest_q_hint = pd.to_datetime(hist["quarter"], errors="coerce").max()
        except Exception:
            latest_q_hint = None

    local_roots: List[Tuple[Path, str]] = []
    seen_local_roots: set[Tuple[str, str]] = set()

    def _first_existing_material_dir(name: str) -> Optional[Path]:
        for root_in in material_roots:
            cand = root_in / name
            if cand.exists() and cand.is_dir():
                return cand
        return None

    def _add_local_root(path_obj: Optional[Path], src_type: str) -> None:
        if path_obj is None:
            return
        if not _path_belongs_to_ticker(path_obj, ticker, ticker_roots):
            return
        key = (path_cache_key(path_obj), src_type)
        if key in seen_local_roots:
            return
        seen_local_roots.add(key)
        local_roots.append((path_obj, src_type))

    for nm in ["earnings_release", "Earnings Release", "Earnings Releases", "press_release", "Press Release"]:
        _add_local_root(_first_existing_material_dir(nm), "earnings_release")
    for nm in ["slides", "earnings_presentation", "Earnings Presentation"]:
        _add_local_root(_first_existing_material_dir(nm), "slides")
    for nm in ["CEO letters", "ceo_letters", "earnings_transcripts", "Earnings Transcripts", "transcripts"]:
        _add_local_root(_first_existing_material_dir(nm), "ceo_letter" if "ceo" in nm.lower() else "transcripts")

    rows: List[Dict[str, Any]] = []
    for root, src_type in local_roots:
        if not root.exists():
            continue
        files = sorted(
            [p for p in root.glob("*") if p.is_file()],
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
            reverse=True,
        )[:40]
        for fp in files:
            if not _path_belongs_to_ticker(fp, ticker, ticker_roots):
                continue
            if fp.suffix.lower() == ".pdf":
                # Leverage-local materials are a best-effort narrative supplement.
                # Skip raw PDF parsing here to avoid pdfminer/pdfplumber crash paths
                # when HTML/TXT/SEC docs can still supply the authoritative output.
                continue
            raw_txt = read_cached_doc_text_fn(fp, normalize=False)
            txt = normalize_leverage_text(raw_txt)
            if not txt or not looks_like_leverage_text(txt):
                continue
            q_guess = infer_cached_doc_quarter_fn(fp, text=raw_txt, latest_q_hint=latest_q_hint)
            if q_guess is None:
                continue
            rows.append(
                {
                    "quarter": normalize_leverage_quarter(q_guess),
                    "text": txt,
                    "filed": pd.to_datetime(fp.stat().st_mtime, unit="s", errors="coerce"),
                    "source_type": src_type,
                    "form": "",
                    "doc": fp.name,
                }
            )
    return rows


def build_leverage_audit_doc_index(
    *,
    audit: Optional[pd.DataFrame],
    cache_root: Path,
    resolve_col: Callable[[pd.DataFrame, List[str]], Optional[str]],
    accession_doc_lookup: Callable[[Any], List[Path]],
    read_leverage_material_text_fn: Callable[[Path], str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if audit is None or audit.empty:
        return rows
    q_col = resolve_col(audit, ["quarter", "quarter_end", "period_end"])
    accn_col = resolve_col(audit, ["accn"])
    form_col = resolve_col(audit, ["form"])
    filed_col = resolve_col(audit, ["filed"])
    if not q_col or not accn_col or not cache_root.exists():
        return rows
    use_cols = [col for col in dict.fromkeys([q_col, accn_col, form_col, filed_col]) if col and col in audit.columns]
    frame = audit.loc[:, use_cols]
    idx_map = {col: idx for idx, col in enumerate(frame.columns)}
    seen_docs: set[Tuple[pd.Timestamp, str]] = set()
    for row in frame.itertuples(index=False, name=None):
        quarter_val = normalize_leverage_quarter(row[idx_map[q_col]])
        accn = str(row[idx_map[accn_col]] or "").strip()
        if quarter_val is None or not accn:
            continue
        docs = accession_doc_lookup(accn)
        filed_val = row[idx_map[filed_col]] if filed_col and filed_col in idx_map else None
        form_val = str(row[idx_map[form_col]] or "") if form_col and form_col in idx_map else ""
        for dp in docs[:10]:
            name_l = dp.name.lower()
            if not re.search(r"(ex99|ex-99|earnings|press|letter|presentation|10q|10k)", name_l):
                continue
            plain = read_leverage_material_text_fn(dp)
            if not plain or not looks_like_leverage_text(plain):
                continue
            doc_key = (quarter_val, path_cache_key(dp))
            if doc_key in seen_docs:
                continue
            seen_docs.add(doc_key)
            rows.append(
                {
                    "quarter": quarter_val,
                    "text": plain,
                    "filed": filed_val,
                    "source_type": "sec_doc",
                    "form": form_val,
                    "doc": dp.name,
                }
            )
    return rows


def extract_adj_net_leverage_text_map(
    *,
    promises: Optional[pd.DataFrame],
    quarter_notes: Optional[pd.DataFrame],
    slides_guidance: Optional[pd.DataFrame],
    ocr_log: Optional[pd.DataFrame],
    hist: Optional[pd.DataFrame],
    resolve_col: Callable[[pd.DataFrame, List[str]], Optional[str]],
    load_local_material_index_fn: Callable[[], List[Dict[str, Any]]],
    load_audit_doc_index_fn: Callable[[], List[Dict[str, Any]]],
    timed_substage: Callable[[str], Any],
) -> Dict[pd.Timestamp, float]:
    out_map: Dict[pd.Timestamp, float] = {}
    cands: List[Dict[str, Any]] = []
    quarter_whitelist = hist_quarter_whitelist(hist)
    ratio_re_list = [
        (re.compile(r"(adjusted\s+net\s+leverage(?:\s+ratio)?)\s*(?:of|:|was|is|at|=)?\s*(\d+(?:\.\d+)?)\s*x", re.I), True),
        (re.compile(r"(adjusted\s+net\s+debt\s*(?:to|/)\s*ebitda)\s*(?:of|:|was|is|at|=)?\s*(\d+(?:\.\d+)?)\s*x", re.I), True),
        (re.compile(r"(net\s+leverage\s+ratio|net\s+leverage)\s*(?:of|:|was|is|at|=)?\s*(\d+(?:\.\d+)?)\s*x", re.I), False),
        (re.compile(r"(net\s+debt\s*(?:to|/)\s*ebitda)\s*(?:of|:|was|is|at|=)?\s*(\d+(?:\.\d+)?)\s*x", re.I), False),
    ]

    def _pick_leverage_ratio(txt: Any) -> Optional[Tuple[float, bool]]:
        s = str(txt or "")
        if not s:
            return None
        s_l = s.lower()
        for rr, explicit_adjusted in ratio_re_list:
            m = rr.search(s)
            if not m:
                continue
            if not explicit_adjusted and "adjusted" not in s_l:
                continue
            try:
                ratio_val = float(m.group(2))
                return float(ratio_val), bool(explicit_adjusted or ("adjusted" in str(m.group(1)).lower()))
            except Exception:
                return None
        return None

    def _src_pri(src_type: str, form: str, doc: str) -> int:
        sl = str(src_type or "").lower()
        fl = str(form or "").upper()
        dl = str(doc or "").lower()
        if "ex99" in dl or "press" in dl or "earnings" in dl:
            return 100
        if "slides" in sl or "presentation" in dl:
            return 90
        if "letter" in dl or "ceo" in dl:
            return 80
        if fl.startswith("10-Q"):
            return 70
        if fl.startswith("10-K"):
            return 60
        return 10

    def _candidate_from_parts(
        qv: Any,
        txt: Any,
        filed: Any,
        source_type: str,
        form: str = "",
        doc: str = "",
    ) -> Optional[Dict[str, Any]]:
        q = normalize_leverage_quarter(qv)
        if q is None:
            return None
        if quarter_whitelist and q not in quarter_whitelist:
            return None
        ratio_pick = _pick_leverage_ratio(txt)
        if ratio_pick is None:
            return None
        ratio, is_adjusted = ratio_pick
        return {
            "quarter": q,
            "ratio": float(ratio),
            "is_adjusted": bool(is_adjusted),
            "filed": pd.to_datetime(filed, errors="coerce"),
            "pri": _src_pri(source_type, form, doc),
        }

    with timed_substage("write_excel.derive.valuation_inputs.net_leverage_text_map.frames"):
        for df_in, q_aliases, text_aliases, src_fallback in [
            (promises, ["last_seen_quarter", "quarter", "created_quarter"], ["statement", "promise_text", "evidence_snippet"], "promise"),
            (quarter_notes, ["quarter", "quarter_end"], ["claim", "headline", "note", "body", "evidence_snippet"], "quarter_notes"),
        ]:
            if df_in is None or df_in.empty:
                continue
            q_col = resolve_col(df_in, q_aliases)
            t_col = resolve_col(df_in, text_aliases)
            if not q_col or not t_col:
                continue
            frame = df_in.loc[:, [col for col in dict.fromkeys([q_col, t_col, "filed", "source_type", "method", "form", "doc", "doc_path"]) if col in df_in.columns]]
            idx_map = {col: idx for idx, col in enumerate(frame.columns)}
            for row in frame.itertuples(index=False, name=None):
                candidate = _candidate_from_parts(
                    qv=row[idx_map[q_col]],
                    txt=row[idx_map[t_col]],
                    filed=row[idx_map["filed"]] if "filed" in idx_map else None,
                    source_type=str((row[idx_map["source_type"]] if "source_type" in idx_map else "") or (row[idx_map["method"]] if "method" in idx_map else "") or src_fallback),
                    form=str(row[idx_map["form"]] or "") if "form" in idx_map else "",
                    doc=str((row[idx_map["doc"]] if "doc" in idx_map else "") or (row[idx_map["doc_path"]] if "doc_path" in idx_map else "") or ""),
                )
                if candidate is not None:
                    cands.append(candidate)

        if slides_guidance is not None and not slides_guidance.empty:
            q_col = resolve_col(slides_guidance, ["quarter", "quarter_end"])
            t_col = resolve_col(slides_guidance, ["line", "text"])
            if q_col and t_col:
                frame = slides_guidance.loc[:, [col for col in dict.fromkeys([q_col, t_col, "numbers", "filed", "source_type", "source", "form", "doc", "doc_path"]) if col in slides_guidance.columns]]
                idx_map = {col: idx for idx, col in enumerate(frame.columns)}
                for row in frame.itertuples(index=False, name=None):
                    txt = normalize_leverage_text(
                        f"{row[idx_map[t_col]] if t_col in idx_map else ''} {row[idx_map['numbers']] if 'numbers' in idx_map else ''}"
                    )
                    candidate = _candidate_from_parts(
                        qv=row[idx_map[q_col]],
                        txt=txt,
                        filed=row[idx_map["filed"]] if "filed" in idx_map else None,
                        source_type=str((row[idx_map["source_type"]] if "source_type" in idx_map else "") or (row[idx_map["source"]] if "source" in idx_map else "") or "slides"),
                        form=str(row[idx_map["form"]] or "") if "form" in idx_map else "",
                        doc=str((row[idx_map["doc"]] if "doc" in idx_map else "") or (row[idx_map["doc_path"]] if "doc_path" in idx_map else "") or ""),
                    )
                    if candidate is not None:
                        cands.append(candidate)

        if ocr_log is not None and not ocr_log.empty:
            q_col = resolve_col(ocr_log, ["quarter", "quarter_end", "as_of_quarter", "period_end"])
            t_col = resolve_col(ocr_log, ["text", "ocr_text", "content", "snippet"])
            src_col = resolve_col(ocr_log, ["source_file", "path", "doc", "doc_path", "file"])
            if q_col and t_col:
                frame = ocr_log.loc[:, [col for col in dict.fromkeys([q_col, t_col, src_col, "filed", "source_type", "form", "doc", "doc_path"]) if col in ocr_log.columns]]
                idx_map = {col: idx for idx, col in enumerate(frame.columns)}
                for row in frame.itertuples(index=False, name=None):
                    src_txt = str(row[idx_map[src_col]] or "").lower() if src_col and src_col in idx_map else ""
                    if src_txt and not any(k in src_txt for k in ("earnings_release", "slides", "presentation", "ceo", "letter", "ex99", "press")):
                        continue
                    candidate = _candidate_from_parts(
                        qv=row[idx_map[q_col]],
                        txt=row[idx_map[t_col]],
                        filed=row[idx_map["filed"]] if "filed" in idx_map else None,
                        source_type=str(row[idx_map["source_type"]] or "ocr") if "source_type" in idx_map else "ocr",
                        form=str(row[idx_map["form"]] or "") if "form" in idx_map else "",
                        doc=str((row[idx_map[src_col]] if src_col and src_col in idx_map else "") or (row[idx_map["doc"]] if "doc" in idx_map else "") or (row[idx_map["doc_path"]] if "doc_path" in idx_map else "") or ""),
                    )
                    if candidate is not None:
                        cands.append(candidate)

    with timed_substage("write_excel.derive.valuation_inputs.net_leverage_text_map.local_docs"):
        for rec in load_local_material_index_fn():
            candidate = _candidate_from_parts(
                qv=rec.get("quarter"),
                txt=rec.get("text"),
                filed=rec.get("filed"),
                source_type=str(rec.get("source_type") or ""),
                form=str(rec.get("form") or ""),
                doc=str(rec.get("doc") or ""),
            )
            if candidate is not None:
                cands.append(candidate)

    with timed_substage("write_excel.derive.valuation_inputs.net_leverage_text_map.audit_docs"):
        for rec in load_audit_doc_index_fn():
            candidate = _candidate_from_parts(
                qv=rec.get("quarter"),
                txt=rec.get("text"),
                filed=rec.get("filed"),
                source_type=str(rec.get("source_type") or ""),
                form=str(rec.get("form") or ""),
                doc=str(rec.get("doc") or ""),
            )
            if candidate is not None:
                cands.append(candidate)

    best_by_q: Dict[pd.Timestamp, Dict[str, Any]] = {}
    for c in cands:
        q = pd.Timestamp(c["quarter"]).to_period("Q").end_time.normalize()
        prev = best_by_q.get(q)
        if prev is None:
            best_by_q[q] = c
            continue
        if bool(c.get("is_adjusted")) and not bool(prev.get("is_adjusted")):
            best_by_q[q] = c
            continue
        if bool(c.get("is_adjusted")) == bool(prev.get("is_adjusted")):
            cdt = pd.to_datetime(c.get("filed"), errors="coerce")
            pdt = pd.to_datetime(prev.get("filed"), errors="coerce")
            if pd.notna(cdt) and (pd.isna(pdt) or cdt > pdt):
                best_by_q[q] = c
                continue
            if (c.get("pri") or 0) > (prev.get("pri") or 0):
                best_by_q[q] = c
                continue
            continue
        cdt = pd.to_datetime(c.get("filed"), errors="coerce")
        pdt = pd.to_datetime(prev.get("filed"), errors="coerce")
        if pd.notna(cdt) and (pd.isna(pdt) or cdt > pdt):
            best_by_q[q] = c
            continue
        if (c.get("pri") or 0) > (prev.get("pri") or 0):
            best_by_q[q] = c

    for q, c in best_by_q.items():
        out_map[pd.Timestamp(q)] = float(c["ratio"])
    return out_map


def maybe_timed_substage(timed_substage: Callable[[str], Any], label: str) -> Any:
    try:
        return timed_substage(label)
    except Exception:
        return nullcontext()
