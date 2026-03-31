"""Non-GAAP text parsing, HTML stripping, and adjusted-metric extraction helpers."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .debt_parser import coerce_number, read_html_tables_any
from .metrics import _ADJ_EBIT_SYNONYMS, _GAAP_EBIT_SYNONYMS
from .sec_xbrl import normalize_accession, parse_date, strip_html


def find_ex99_docs(index_json: Dict[str, Any]) -> List[str]:
    items = index_json.get("directory", {}).get("item", [])
    names = [it.get("name", "") for it in items]
    cand = []
    for n in names:
        ln = n.lower()
        if not ln.endswith((".htm", ".html", ".txt", ".pdf")):
            continue
        if re.search(
            r"(ex[-_]?99|99[-_.]?[12]|earnings(?:press)?releas|pressrelea|shareholderletter|stockholderletter|ceoletter|investorletter)",
            ln,
        ):
            cand.append(n)
    return sorted(set(cand))


def infer_quarter_end_from_text(txt: str) -> Optional[pd.Timestamp]:
    patterns = [
        r"Three\s+Months\s+Ended\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        r"Quarter\s+Ended\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        r"Fourth\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"Third\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"Second\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"First\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"Fourth\s+Quarter\s+(\d{4})",
        r"Third\s+Quarter\s+(\d{4})",
        r"Second\s+Quarter\s+(\d{4})",
        r"First\s+Quarter\s+(\d{4})",
        r"Q([1-4])\s*(20\d{2})",
    ]
    for pat in patterns:
        m = re.search(pat, txt, re.IGNORECASE)
        if not m:
            continue
        if len(m.groups()) == 2:
            try:
                q = int(m.group(1))
                year = int(m.group(2))
                if 1 <= q <= 4:
                    return pd.Timestamp(year=year, month=3 * q, day=30 if q in (2, 3) else 31).date()
            except Exception:
                pass
        if len(m.groups()) == 1:
            year = int(m.group(1))
            if "Fourth" in pat:
                return pd.Timestamp(year=year, month=12, day=31).date()
            if "Third" in pat:
                return pd.Timestamp(year=year, month=9, day=30).date()
            if "Second" in pat:
                return pd.Timestamp(year=year, month=6, day=30).date()
            if "First" in pat:
                return pd.Timestamp(year=year, month=3, day=31).date()
        if len(m.groups()) >= 3:
            month, day, year = m.group(1), m.group(2), m.group(3)
            try:
                return pd.Timestamp(f"{month} {day} {year}").date()
            except Exception:
                continue
    return None


def normalize_number_spacing(s: str) -> str:
    s = re.sub(r"(\d)\s+,(\s+)?(\d)", r"\1,\3", s)
    s = re.sub(r"(\d)\s+(\d{2,3},\d{3})", r"\1\2", s)
    return s


def _slice_three_month_block(lines: List[str]) -> List[str]:
    """Return a slice of lines that appear to belong to the 'Three Months Ended' section."""
    start = None
    end = None
    for i, ln in enumerate(lines):
        if re.search(r"three\s+months\s+ended|quarter\s+ended", ln, re.I):
            start = i
            continue
        if start is not None and re.search(r"six\s+months|nine\s+months|twelve\s+months|year\s+ended|fiscal\s+year", ln, re.I):
            # Allow a few header lines where 3M/6M appear together before the data rows.
            if i - start <= 3:
                continue
            end = i
            break
    if start is not None:
        return lines[start:end] if end is not None else lines[start:]
    return lines


def _detect_scale(html: str) -> float:
    scale = 1.0
    if re.search(r"in\s+thousands", html, re.IGNORECASE):
        scale = 1000.0
    if re.search(r"in\s+millions", html, re.IGNORECASE):
        scale = 1_000_000.0
    return scale


def _detect_local_scale(text: str, default_scale: float = 1.0) -> float:
    txt = str(text or "")
    if re.search(r"in\s+thousands|\$\s*k\b|\bthousand\b", txt, re.I):
        return max(default_scale, 1000.0)
    if re.search(r"in\s+millions|\$\s*m\b|\bmillion(s)?\b", txt, re.I):
        return max(default_scale, 1_000_000.0)
    return default_scale


def _label_matches(label: str, needles: List[str]) -> bool:
    ln = label.lower()
    return any(n in ln for n in needles)


def _parse_date_from_text(text: str) -> Optional[pd.Timestamp]:
    m = re.search(
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
        r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2},?\s+\d{4}",
        text,
        re.IGNORECASE,
    )
    if not m:
        return None
    try:
        return pd.to_datetime(m.group(0), errors="coerce").date()
    except Exception:
        return None


def _period_hint_from_text(text: str) -> Optional[str]:
    t = re.sub(r"[\s\-]+", " ", str(text or "").lower())
    hits: List[str] = []
    if re.search(r"three months|quarter ended|quarterly", t):
        hits.append("3M")
    if re.search(r"six months", t):
        hits.append("6M")
    if re.search(r"nine months", t):
        hits.append("9M")
    if re.search(r"twelve months|year ended|fiscal year|annual", t):
        hits.append("FY")
    hits = list(dict.fromkeys(hits))
    if len(hits) == 1:
        return hits[0]
    return None


def _flatten_cols(cols: Any) -> List[str]:
    if isinstance(cols, pd.MultiIndex):
        return [" ".join([str(c) for c in tup if str(c) != "nan"]).strip() for tup in cols]
    return [str(c) for c in cols]


def _find_header_dates(
    df: pd.DataFrame,
) -> Tuple[List[str], Optional[int], Dict[int, pd.Timestamp], Optional[str]]:
    cols = _flatten_cols(df.columns)
    col_dates: Dict[int, pd.Timestamp] = {}
    for i, c in enumerate(cols):
        d = _parse_date_from_text(c)
        if d:
            col_dates[i] = d
    if col_dates:
        return cols, None, col_dates, _period_hint_from_text(" ".join(cols))

    if not df.empty:
        row0 = [str(x) for x in df.iloc[0].tolist()]
        row0_hint = _period_hint_from_text(" ".join(row0))
        row_dates: Dict[int, pd.Timestamp] = {}
        for i, c in enumerate(row0):
            d = _parse_date_from_text(c)
            if d:
                row_dates[i] = d
        if row_dates:
            return row0, 0, row_dates, row0_hint

    for ridx in range(min(6, len(df))):
        row = [str(x) for x in df.iloc[ridx].tolist()]
        row_hint = _period_hint_from_text(" ".join(row))
        row_dates: Dict[int, pd.Timestamp] = {}
        for i, c in enumerate(row):
            d = _parse_date_from_text(c)
            if d:
                row_dates[i] = d
        row_text = " ".join(row).lower()
        if row_dates and ("month" in row_text or "ended" in row_text or "three months" in row_text or "quarter" in row_text):
            return row, ridx, row_dates, row_hint
        if row_dates:
            return row, ridx, row_dates, row_hint

        if "three months" in row_text or "quarter ended" in row_text:
            if ridx + 1 < len(df):
                row2 = [str(x) for x in df.iloc[ridx + 1].tolist()]
                row_dates2: Dict[int, pd.Timestamp] = {}
                for i, c in enumerate(row2):
                    d = _parse_date_from_text(c)
                    if d:
                        row_dates2[i] = d
                if row_dates2:
                    return row2, ridx + 1, row_dates2, row_hint

    return cols, None, {}, None


def _is_eps_label(label: str) -> bool:
    ln = str(label or "").lower()
    if "per share" in ln or "eps" in ln:
        if "shares used" in ln or "weighted-average shares" in ln:
            return False
        return True
    return False


def _is_adj_eps_label(label: str) -> bool:
    ln = str(label or "").lower()
    if "adjusted" in ln and _is_eps_label(ln):
        return True
    return False


def _parse_adjusted_from_text(
    txt: str,
    quarter_end: Optional[pd.Timestamp],
    mode: str,
) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, float], str, Optional[str]]:
    if not txt:
        return None, None, None, {}, "ocr_no_text", None
    if quarter_end is None:
        return None, None, None, {}, "no_quarter_end", None

    t_low = txt.lower()
    # Guard: skip segment-only pages (Adjusted Segment EBIT/EBITDA tables)
    if ("adjusted segment" in t_low or "reportable segments" in t_low) and "reconciliation of reported" not in t_low:
        return None, None, None, {}, "segment_page", None

    txt = normalize_number_spacing(txt)
    scale = _detect_scale(txt)

    if mode == "strict":
        q_detect = infer_quarter_end_from_text(txt)
        if q_detect is None or q_detect != quarter_end:
            return None, None, None, {}, "ocr_no_quarter_end", None

    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
    # Restrict parsing to the "Three Months Ended" block when present.
    lines_3m = _slice_three_month_block(lines)

    # Try to detect "Three Months Ended" header years to pick correct column
    years_3m: List[int] = []
    has_6m_block = any(re.search(r"six\s+months\s+ended", ln, re.I) for ln in lines[:80])
    for i, ln in enumerate(lines_3m[:40]):
        if re.search(r"three months|quarter ended", ln, re.I):
            yrs = [int(y) for y in re.findall(r"(20\d{2})", ln)]
            if not yrs:
                # check next couple of lines for year headers
                for j in range(1, 3):
                    if i + j < len(lines_3m):
                        yrs.extend([int(y) for y in re.findall(r"(20\d{2})", lines_3m[i + j])])
            if yrs:
                # keep order, unique
                seen = set()
                for y in yrs:
                    if y not in seen:
                        years_3m.append(y)
                        seen.add(y)
            if years_3m:
                break

    def _pick_number_by_year(nums: List[float]) -> Optional[float]:
        if not nums:
            return None
        # If a 6M block exists and we have 4+ numbers, assume first two are 3M (current/prior)
        if has_6m_block and len(nums) >= 4:
            nums = nums[:2]
        elif has_6m_block and len(nums) >= 3:
            nums = nums[:2]
        if quarter_end is None or not years_3m or len(nums) < 2:
            return nums[0]
        y = int(pd.Timestamp(quarter_end).year)
        if y == years_3m[0]:
            return nums[0]
        if len(years_3m) > 1 and y == years_3m[1]:
            return nums[1]
        return nums[0]

    def _extract_nums_from_line(line: str) -> List[float]:
        tokens = re.findall(r"\(?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\)?", line)
        nums: List[float] = []
        local_scale = _detect_local_scale(line, scale)
        for t in tokens:
            v = coerce_number(t)
            if v is None:
                continue
            # skip year-like tokens
            if isinstance(v, (int, float)) and 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                continue
            nums.append(float(v) * local_scale)
        return nums

    def _find_value(keys: List[str], exclude_terms: Optional[List[str]] = None) -> Optional[float]:
        for i, ln in enumerate(lines_3m):
            if _label_matches(ln, keys):
                ln_low = ln.lower()
                if exclude_terms and any(term in ln_low for term in exclude_terms):
                    continue
                if "reconciliation of reported" in ln_low:
                    # Skip section headers that mention adjusted metrics but have no numbers.
                    continue
                # Avoid picking segment tables when we need consolidated adjusted metrics
                if "segment" in ln_low:
                    continue
                # Prefer numbers that appear after the label to avoid cross-line OCR bleed.
                match_key = None
                match_pos = None
                for k in keys:
                    pos = ln_low.find(k)
                    if pos >= 0 and (match_pos is None or pos < match_pos):
                        match_pos = pos
                        match_key = k
                if match_key is not None and match_pos is not None:
                    ln_use = ln[match_pos + len(match_key):]
                else:
                    ln_use = ln
                nums = _extract_nums_from_line(ln_use)
                if not nums:
                    # numbers may be on the next line(s)
                    for j in range(1, 3):
                        if i + j < len(lines_3m):
                            nums = _extract_nums_from_line(lines_3m[i + j])
                            if nums:
                                break
                if not nums:
                    continue
                return _pick_number_by_year(nums)
        return None

    # "Adjusted earnings before interest" is an EBIT label, not EBITDA.
    adj_ebitda = _find_value(
        [
            "adjusted ebitda",
            "adjusted earnings before interest taxes depreciation and amortization",
        ]
    )
    adj_ebit = _find_value(_ADJ_EBIT_SYNONYMS, exclude_terms=["ebitda", "depreciation and amortization"])
    adj_eps: Optional[float] = None

    for i, ln in enumerate(lines_3m):
        if not _is_adj_eps_label(ln):
            continue
        ln_low = ln.lower()
        if "reconciliation of" in ln_low and "adjusted" in ln_low and "per share" in ln_low:
            # Header row; the next line is often GAAP EPS, not adjusted EPS.
            continue
        def _extract_eps_nums(s: str) -> List[float]:
            tokens = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", s)
            eps_nums: List[float] = []
            for t in tokens:
                v = coerce_number(t)
                if v is None:
                    continue
                if isinstance(v, (int, float)) and 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                    continue
                # EPS should be a small magnitude
                if abs(float(v)) > 100:
                    continue
                eps_nums.append(float(v))
            return eps_nums

        # Prefer tokens after the adjusted-EPS label to avoid picking GAAP line numbers.
        use_ln = ln
        pos_adj = ln_low.find("adjusted")
        if pos_adj >= 0:
            use_ln = ln[pos_adj:]
        eps_nums = _extract_eps_nums(use_ln)
        if not eps_nums:
            for j in range(1, 3):
                if i + j < len(lines_3m):
                    eps_nums = _extract_eps_nums(lines_3m[i + j])
                    if eps_nums:
                        break
        if not eps_nums:
            continue
        adj_eps = _pick_number_by_year(eps_nums)
        break

    if adj_ebitda is None and adj_ebit is None and adj_eps is None:
        return None, None, None, {}, "ocr_no_metrics", None

    status = "ok_ocr" if mode == "strict" else "ok_relaxed_ocr"
    return adj_ebit, adj_ebitda, adj_eps, {}, status, "ocr"


def parse_adjusted_from_plain_text(
    txt: str,
    quarter_end: Optional[pd.Timestamp],
    mode: str = "relaxed",
) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, float], str, Optional[str]]:
    return _parse_adjusted_from_text(txt, quarter_end, mode)


def parse_adjusted_from_ex99(
    html_bytes: bytes,
    quarter_end: Optional[pd.Timestamp],
    mode: str = "strict",
) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, float], str, Optional[str]]:
    html = html_bytes.decode("utf-8", errors="ignore")
    html = normalize_number_spacing(html)
    scale = _detect_scale(html)

    tables = read_html_tables_any(html.encode("utf-8"))
    adj_ebit = None
    adj_ebitda = None
    adj_eps: Optional[float] = None
    adjustments: Dict[str, float] = {}
    adj_fcf: Optional[float] = None

    def to_num(x: Any) -> Optional[float]:
        v = coerce_number(x)
        return None if v is None else float(v) * scale

    def to_num_eps(x: Any) -> Optional[float]:
        v = coerce_number(x)
        if v is None:
            return None
        return float(v)

    adjustments_keywords = [
        "restruct",
        "pension",
        "impair",
        "litigation",
        "integration",
        "foreign exchange",
        "fx",
        "refinanc",
        "gain",
        "loss",
        "other",
        "non-cash",
        "stock-based",
        "amortization",
        "depreciation",
    ]

    if quarter_end is None:
        return None, None, {}, "no_quarter_end", None

    for t in tables:
        if t is None or t.empty:
            continue

        t2 = t.copy()
        cols, header_row_idx, col_dates, table_hint = _find_header_dates(t2)
        if header_row_idx is not None:
            t2.columns = cols
            t2 = t2.drop(t2.index[header_row_idx]).reset_index(drop=True)
        else:
            t2.columns = cols

        col_periods = {i: _period_hint_from_text(c) for i, c in enumerate(cols)}
        if table_hint:
            for i in range(len(cols)):
                if col_periods.get(i) is None:
                    col_periods[i] = table_hint

        col_quarters: Dict[int, Tuple[int, int]] = {}
        for i, c in enumerate(cols):
            m = re.search(r"Q([1-4])\s*(20\d{2})", c, re.IGNORECASE)
            if m:
                col_quarters[i] = (int(m.group(2)), int(m.group(1)))

        col_idx = None
        col_label = None
        if mode == "strict":
            match_cols = [i for i, d in col_dates.items() if d == quarter_end] if col_dates else []
            if not match_cols and quarter_end is not None:
                q = (int(quarter_end.month) - 1) // 3 + 1
                y = int(quarter_end.year)
                match_cols = [i for i, (yy, qq) in col_quarters.items() if yy == y and qq == q]
            if match_cols:
                # Require 3M only if explicitly labeled otherwise allow unknown period
                match_cols = [i for i in match_cols if col_periods.get(i) in (None, "3M")]
            if not match_cols:
                continue
            col_idx = match_cols[0]
            col_label = cols[col_idx]
        else:
            candidate_cols = [i for i in range(1, len(cols))]
            if col_dates:
                match_cols = [i for i, d in col_dates.items() if d == quarter_end]
                if not match_cols and quarter_end is not None:
                    q = (int(quarter_end.month) - 1) // 3 + 1
                    y = int(quarter_end.year)
                    match_cols = [i for i, (yy, qq) in col_quarters.items() if yy == y and qq == q]
                if match_cols:
                    candidate_cols = match_cols
            if quarter_end is not None:
                y = int(quarter_end.year)
                year_match = [i for i in candidate_cols if str(y) in str(cols[i])]
                if year_match:
                    candidate_cols = year_match

            def _score_col(i: int) -> Tuple[int, float]:
                nums = []
                for _, row in t2.iterrows():
                    v = to_num(row.iloc[i]) if i < len(row) else None
                    if v is not None:
                        nums.append(abs(v))
                if not nums:
                    return (0, 0.0)
                nums.sort()
                return (len(nums), nums[len(nums) // 2])

            scored = [(i,) + _score_col(i) for i in candidate_cols]
            scored = [s for s in scored if s[1] > 0]
            if not scored:
                continue
            scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
            col_idx = scored[0][0]
            col_label = cols[col_idx]

        first = t2.columns[0]
        t2[first] = t2[first].astype(str)

        has_adjusted = False
        has_recon = False
        has_eps = False
        has_adj_eps = False

        for _, row in t2.iterrows():
            label = str(row.get(first, "")).strip().lower()
            if not label or label == "nan":
                continue
            v = to_num(row.iloc[col_idx]) if col_idx < len(row) else None
            if v is None:
                # still allow EPS parse without scale
                v_eps = to_num_eps(row.iloc[col_idx]) if col_idx < len(row) else None
            else:
                v_eps = to_num_eps(row.iloc[col_idx]) if col_idx < len(row) else None

            if "adjusted ebitda" in label:
                adj_ebitda = v
                has_adjusted = True
            if _label_matches(label, _ADJ_EBIT_SYNONYMS) and "ebitda" not in label:
                adj_ebit = v
                has_adjusted = True
            if _is_eps_label(label):
                has_eps = True
            if _is_adj_eps_label(label):
                if v_eps is not None and abs(v_eps) <= 100:
                    adj_eps = v_eps
                    has_adj_eps = True

            if "free cash flow" in label and v is not None:
                adj_fcf = v
            if _label_matches(label, adjustments_keywords):
                adjustments[label] = v
                has_recon = True

            if _label_matches(label, _GAAP_EBIT_SYNONYMS):
                has_recon = True

        if (has_adjusted and has_recon and (adj_ebit is not None or adj_ebitda is not None)) or (has_adj_eps and has_eps) or (adj_fcf is not None):
            if adj_fcf is not None:
                adjustments["__adj_fcf"] = adj_fcf
            return adj_ebit, adj_ebitda, adj_eps, adjustments, ("ok" if mode == "strict" else "ok_relaxed"), col_label

    return None, None, None, {}, "no_matching_column", None


def build_non_gaap_tier3(
    sec: Any,
    cik_int: int,
    submissions: Dict[str, Any],
    max_quarters: int,
    mode: str = "strict",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accns = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", []) or []
    report_dates = recent.get("reportDate", []) or []

    rows_m: List[Dict[str, Any]] = []
    rows_b: List[Dict[str, Any]] = []
    rows_f: List[Dict[str, Any]] = []

    n = min(len(forms), len(accns))
    for i in range(n):
        form = forms[i]
        accn = accns[i]
        fdate = filing_dates[i] if i < len(filing_dates) else None
        rdate = report_dates[i] if i < len(report_dates) else None

        if form != "8-K":
            continue

        accn_nd = normalize_accession(accn)
        try:
            idx = sec.accession_index_json(cik_int, accn_nd)
        except Exception:
            continue

        exdocs = find_ex99_docs(idx)
        if not exdocs:
            rows_f.append({"accn": accn, "filed": fdate, "status": "no_ex99"})
            continue

        picked = None
        q_end = None

        for fn in exdocs[:8]:
            try:
                b = sec.download_document(cik_int, accn_nd, fn)
            except Exception:
                continue
            try:
                sec.download_html_assets(cik_int, accn_nd, b)
            except Exception:
                pass
            try:
                sec.download_index_images(cik_int, accn_nd, idx)
            except Exception:
                pass

            txt = strip_html(b.decode("utf-8", errors="ignore"))
            q_end = infer_quarter_end_from_text(txt) or parse_date(rdate) or parse_date(fdate)

            aebit, aebitda, aeps, adj, status, col_label = parse_adjusted_from_ex99(b, q_end, mode=mode)
            if status not in ("ok", "ok_relaxed"):
                try:
                    ocr_txt = sec.ocr_html_assets(
                        accn_nd,
                        b,
                        context={"doc": fn, "quarter": q_end, "purpose": "non_gaap_ocr", "report_date": rdate, "filing_date": fdate, "save_text": True},
                    )
                except Exception:
                    ocr_txt = ""
                if ocr_txt:
                    aebit, aebitda, aeps, adj, status, col_label = _parse_adjusted_from_text(ocr_txt, q_end, mode=mode)
                if status not in ("ok", "ok_relaxed", "ok_ocr", "ok_relaxed_ocr"):
                    rows_f.append({"accn": accn, "filed": fdate, "status": status, "doc": fn})
                    continue

            picked = (fn, aebit, aebitda, aeps, adj, col_label)
            break

        if picked is None:
            rows_f.append({"accn": accn, "filed": fdate, "status": "ex99_no_metrics"})
            continue

        fn, aebit, aebitda, aeps, adj, col_label = picked
        adj_fcf = None
        if isinstance(adj, dict) and "__adj_fcf" in adj:
            adj_fcf = adj.pop("__adj_fcf", None)
        if q_end is None:
            q_end = parse_date(rdate) or parse_date(fdate)

        rows_m.append({
            "quarter": q_end,
            "adj_ebit": aebit,
            "adj_ebitda": aebitda,
            "adj_eps": aeps,
            "adj_fcf": adj_fcf,
            "source": "ex99",
            "accn": accn,
            "filed": parse_date(fdate),
            "doc": fn,
            "confidence": "low" if mode == "relaxed" else "high",
            "col": col_label,
        })

        for lab, val in adj.items():
            if str(lab).startswith("__"):
                continue
            rows_b.append({
                "quarter": q_end,
                "label": lab,
                "value": val,
                "source": "ex99",
                "accn": accn,
                "doc": fn,
                "confidence": "low" if mode == "relaxed" else "high",
                "col": col_label,
            })

        rows_f.append({
            "accn": accn,
            "filed": fdate,
            "status": "ok" if mode == "strict" else "ok_relaxed",
            "doc": fn,
            "quarter": str(q_end),
            "col": col_label,
        })

    m = pd.DataFrame(rows_m)
    b = pd.DataFrame(rows_b)
    f = pd.DataFrame(rows_f)

    if not m.empty:
        m = m.sort_values("quarter").drop_duplicates(subset=["quarter"], keep="last")
        qs = sorted(m["quarter"].unique())[-max_quarters:]
        m = m[m["quarter"].isin(qs)].copy()
        if not b.empty:
            b = b[b["quarter"].isin(qs)].copy()

    return m, b, f
