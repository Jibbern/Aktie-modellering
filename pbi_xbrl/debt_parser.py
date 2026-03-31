"""Debt-table parsing helpers for SEC HTML, plain text, and local materials."""
from __future__ import annotations

import datetime as dt
import html as html_lib
import io
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .sec_xbrl import normalize_accession, parse_date


STRONG_DEBT_KEYWORDS = [
    "senior notes",
    "secured notes",
    "senior unsecured",
    "notes due",
    "term loan",
    "credit facility",
    "revolver",
    "revolving",
    "convertible notes",
    "loan agreement",
    "debentures",
]
WEAK_DEBT_KEYWORDS = [
    "borrowings",
    "long-term debt",
    "long term debt",
    "total debt",
]


def _stringify_table_cells(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for value in values:
        if pd.isna(value):
            out.append("")
        else:
            out.append(str(value))
    return out


def coerce_number(x: Any) -> Optional[float]:
    try:
        s = str(x).strip()
        if not s or s.lower() in ("nan", "none"):
            return None
        # Remove common trailing footnote markers before alpha checks
        # (e.g., 150,000(a), 150,000*, 150,000¹, 150,000[1]).
        s = re.sub(r"[\u00B9\u00B2\u00B3\u2070-\u209F]", "", s)
        for _ in range(4):
            s2 = re.sub(r"\s*(?:\([A-Za-z0-9]{1,4}\)|\[[A-Za-z0-9]{1,4}\]|[*†‡]+)\s*$", "", s).strip()
            if s2 == s:
                break
            s = s2
        if re.search(r"[A-Za-z]", s):
            return None
        s = s.replace("$", "").replace(",", "")
        s = s.replace("(", "-").replace(")", "")
        s = re.sub(r"[^0-9\.\-]", "", s)
        if s in ("", "-", "."):
            return None
        return float(s)
    except Exception:
        return None


def _is_blankish_amount_cell(x: Any) -> bool:
    s = str(x or "").strip()
    if not s:
        return True
    s_norm = s.replace("\xa0", " ").strip().lower()
    if s_norm in {"-", "--", "---", "—", "–", "na", "n/a", "nm"}:
        return True
    s_compact = re.sub(r"[\s\$\(\),]", "", s_norm)
    if s_compact in {"", "-", "--", "---", "—", "–", "0", "0.0", "0.00"}:
        return True
    return False


def _is_currency_placeholder_cell(x: Any) -> bool:
    s = str(x or "").replace("\xa0", " ").strip().lower()
    if not s:
        return False
    s = re.sub(r"[\s\(\)]", "", s)
    return s in {"$", "$$", "usd", "us$"}




def _is_quarter_end(d: Optional[dt.date]) -> bool:
    if d is None:
        return False
    return (d.month, d.day) in {(3, 31), (6, 30), (9, 30), (12, 31)}


def _coerce_prev_quarter_end(d: Optional[dt.date]) -> Optional[dt.date]:
    if d is None:
        return None
    if d.month <= 3:
        q_end = dt.date(d.year, 3, 31)
    elif d.month <= 6:
        q_end = dt.date(d.year, 6, 30)
    elif d.month <= 9:
        q_end = dt.date(d.year, 9, 30)
    else:
        q_end = dt.date(d.year, 12, 31)
    if d >= q_end:
        return q_end
    # previous quarter end
    if q_end.month == 3:
        return dt.date(d.year - 1, 12, 31)
    if q_end.month == 6:
        return dt.date(d.year, 3, 31)
    if q_end.month == 9:
        return dt.date(d.year, 6, 30)
    return dt.date(d.year, 9, 30)


def _parse_header_dates_from_table(df: pd.DataFrame) -> Dict[int, dt.date]:
    def _parse_date(s: str) -> Optional[pd.Timestamp]:
        if not s:
            return None
        m = re.search(
            r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
            r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2},?\s+\d{4}",
            s,
            re.IGNORECASE,
        )
        if m:
            try:
                dd = pd.to_datetime(m.group(0), errors="coerce")
                if pd.notna(dd):
                    return dd.date()
            except Exception:
                pass
        m2 = re.search(r"\b(0?[1-9]|1[0-2])[\/\-](0?[1-9]|[12]\d|3[01])[\/\-](\d{2}|\d{4})\b", s)
        if m2:
            mm = int(m2.group(1))
            dd = int(m2.group(2))
            yy_raw = int(m2.group(3))
            yy = yy_raw if yy_raw >= 100 else (2000 + yy_raw if yy_raw <= 69 else 1900 + yy_raw)
            try:
                return dt.date(yy, mm, dd)
            except Exception:
                return None
        return None

    col_dates: Dict[int, dt.date] = {}
    cols = [str(c) for c in df.columns]
    for i, c in enumerate(cols):
        d = _parse_date(c)
        if d:
            col_dates[i] = d

    # Parse top header rows (multi-row headers are common in debt profile tables).
    if not df.empty:
        head_n = min(5, len(df))
        for ridx in range(head_n):
            row_vals = [str(x) for x in df.iloc[ridx].tolist()]
            for i, c in enumerate(row_vals):
                d = _parse_date(c)
                if d and i not in col_dates:
                    col_dates[i] = d

            # Support "As of December 31, 2025 and December 31, 2024" style text.
            joined = " | ".join(row_vals)
            asof_hits = list(
                re.finditer(
                    r"As\s+of\s+("
                    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
                    r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2},?\s+\d{4}"
                    r")",
                    joined,
                    flags=re.I,
                )
            )
            if asof_hits:
                # Assign sequentially to numeric columns from left to right as a fallback.
                date_vals: List[dt.date] = []
                for mm in asof_hits:
                    dd = _parse_date(mm.group(1))
                    if dd:
                        date_vals.append(dd)
                if date_vals:
                    for i, c in enumerate(cols):
                        if i in col_dates:
                            continue
                        if i >= len(date_vals):
                            break
                        col_dates[i] = date_vals[i]
    return col_dates


def _iter_submission_batches(sec: Any, submissions: Dict[str, Any]) -> List[Dict[str, Any]]:
    def _coerce_batch(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(data, dict):
            return None
        if "filings" in data and isinstance(data.get("filings"), dict):
            recent = data.get("filings", {}).get("recent", {})
            if recent:
                return recent
        if "accessionNumber" in data and "form" in data:
            return data
        return None

    batches: List[Dict[str, Any]] = []
    base = _coerce_batch(submissions)
    if base:
        batches.append(base)

    files = submissions.get("filings", {}).get("files", []) or []
    for f in files:
        name = f.get("name")
        if not name:
            continue
        url = f"https://data.sec.gov/submissions/{name}"
        try:
            data = sec.get(url, as_json=True, cache_key=f"submissions_{name}")
        except Exception:
            continue
        rec = _coerce_batch(data)
        if rec:
            batches.append(rec)
    return batches


def _extract_tables_from_html_simple(html: str) -> List[pd.DataFrame]:
    tables: List[pd.DataFrame] = []
    for table_html in re.findall(r"<table[^>]*>.*?</table>", html, flags=re.IGNORECASE | re.DOTALL):
        rows: List[List[str]] = []
        header_idx: Optional[int] = None
        for idx, row_html in enumerate(re.findall(r"<tr[^>]*>.*?</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)):
            cells = re.findall(r"<t[dh][^>]*>.*?</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if not cells:
                continue
            vals = [html_lib.unescape(re.sub(r"<[^>]+>", " ", c)) for c in cells]
            vals = [v.replace("\xa0", " ").strip() for v in vals]
            vals = [re.sub(r"\s+", " ", v) for v in vals]
            rows.append(vals)
            if header_idx is None and re.search(r"<th", row_html, flags=re.IGNORECASE):
                header_idx = len(rows) - 1

        if not rows:
            continue

        width = max(len(r) for r in rows)
        rows = [r + [""] * (width - len(r)) for r in rows]
        if header_idx is None:
            cols = [f"col{i}" for i in range(width)]
            data = rows
        else:
            cols = rows[header_idx]
            data = rows[header_idx + 1 :]
            if not data:
                data = rows
                cols = [f"col{i}" for i in range(width)]
        tables.append(pd.DataFrame(data, columns=cols))
    return tables


def read_html_tables_any(html_bytes: bytes) -> List[pd.DataFrame]:
    html = html_bytes.decode("utf-8", errors="ignore")
    try:
        return pd.read_html(io.StringIO(html))
    except Exception:
        return _extract_tables_from_html_simple(html)


def _debt_table_score(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    header = " ".join([str(c).lower() for c in df.columns])
    body = " ".join(_stringify_table_cells(df.head(12).values.ravel().tolist())).lower()
    hay = header + " " + body
    strong = sum(1 for k in STRONG_DEBT_KEYWORDS if k in hay)
    weak = sum(1 for k in WEAK_DEBT_KEYWORDS if k in hay)
    return strong * 2 + weak


def looks_like_debt_table(df: pd.DataFrame) -> bool:
    return _debt_table_score(df) >= 2


def _detect_scale(text: str) -> float:
    if not text:
        return 1.0
    t = text.lower()
    if re.search(r"\(\s*\$?\s*0{3}s?\s*\)|\$\s*0{3}s?\b|in\s+\$?0{3}s?", t):
        return 1000.0
    if re.search(r"in\s+thousands|\$\s*in\s+thousands|dollars\s+in\s+thousands", t):
        return 1000.0
    if re.search(r"in\s+millions|\$\s*in\s+millions|dollars\s+in\s+millions", t):
        return 1_000_000.0
    return 1.0


def parse_debt_tranches_from_primary_doc(
    html_bytes: bytes,
    *,
    quarter_end: Optional[dt.date] = None,
) -> Tuple[List[Dict[str, Any]], int, Optional[float], Optional[float], Optional[str], float, Optional[bool]]:
    html = html_bytes.decode("utf-8", errors="ignore")
    tables = read_html_tables_any(html_bytes)
    out: List[Dict[str, Any]] = []

    table_total_debt: Optional[float] = None
    table_total_long_term_debt: Optional[float] = None
    table_total_label: Optional[str] = None
    global_scale = _detect_scale(html)

    for t in tables:
        score = _debt_table_score(t)
        if score < 2:
            continue

        local_table_total_debt: Optional[float] = None
        local_table_total_long_term_debt: Optional[float] = None
        local_table_total_label: Optional[str] = None
        t2 = t.copy()
        table_text = " ".join(_stringify_table_cells(t2.values.ravel().tolist())).lower()
        table_has_strong = any(k in table_text for k in STRONG_DEBT_KEYWORDS)
        table_has_weak = any(k in table_text for k in WEAK_DEBT_KEYWORDS)
        table_has_maturity_shape = bool(
            re.search(r"\b(notes?\s+due|term\s+loan|revolver|revolving|debentures?)\b", table_text)
            or (re.search(r"\b20\d{2}\b", table_text) and "due" in table_text)
        )
        if not table_has_strong and not (table_has_weak and table_has_maturity_shape):
            continue
        scale = _detect_scale(table_text)
        if scale == 1.0 and global_scale != 1.0:
            scale = global_scale

        def _col_series(df: pd.DataFrame, idx: int) -> pd.Series:
            col = df.iloc[:, idx]
            if isinstance(col, pd.DataFrame):
                col = col.iloc[:, 0]
            return col

        num_cols: List[int] = []
        parsed_by_col: Dict[int, List[float]] = {}
        for idx, c in enumerate(t2.columns):
            vals = _col_series(t2, idx).head(30).tolist()
            parsed = [coerce_number(v) for v in vals]
            parsed_vals = [float(v) for v in parsed if v is not None]
            parsed_by_col[idx] = parsed_vals
            if sum(v is not None for v in parsed) >= max(3, len(vals) // 4):
                num_cols.append(idx)

        if not num_cols:
            continue

        alpha_scores: Dict[int, int] = {}
        for idx, c in enumerate(t2.columns):
            vals = _stringify_table_cells(_col_series(t2, idx).head(30).tolist())
            alpha_scores[idx] = sum(1 for v in vals if re.search(r"[A-Za-z]", v))
        name_col_idx = max(alpha_scores, key=alpha_scores.get)

        def _is_row_index_col(col_idx: int) -> bool:
            vals = parsed_by_col.get(col_idx, [])
            ints = [int(round(v)) for v in vals if v is not None and abs(float(v) - round(float(v))) < 1e-9]
            if len(ints) < 5:
                return False
            small = [v for v in ints if 0 <= v <= 200]
            if len(small) < max(5, int(0.8 * len(ints))):
                return False
            inc = 0
            dec = 0
            for a, b in zip(small[:-1], small[1:]):
                if b >= a:
                    inc += 1
                if b <= a:
                    dec += 1
            monotone = max(inc, dec) >= max(3, int(0.8 * (len(small) - 1)))
            uniq = len(set(small)) >= max(4, int(0.6 * len(small)))
            return monotone and uniq

        def _col_median_abs(col_idx: int) -> float:
            nums = [abs(v) for v in parsed_by_col.get(col_idx, [])]
            if not nums:
                return 0.0
            nums.sort()
            return float(nums[len(nums) // 2])

        row_values: List[List[Any]] = [list(x) for x in t2.itertuples(index=False, name=None)]
        debt_row_flags: List[bool] = []
        for rv in row_values:
            row_txt = " ".join(str(v) for v in rv).lower()
            debt_row_flags.append(any(k in row_txt for k in STRONG_DEBT_KEYWORDS))

        def _col_big_rate_counts(col_idx: int) -> Tuple[int, int]:
            big = 0
            rate = 0
            for rv, is_debt in zip(row_values, debt_row_flags):
                if not is_debt or col_idx >= len(rv):
                    continue
                v = coerce_number(rv[col_idx])
                if v is None:
                    continue
                av = abs(float(v))
                if av >= 1000:
                    big += 1
                elif 0 < av < 100:
                    rate += 1
            return big, rate

        def _best_amount_col(cands: List[int]) -> Optional[int]:
            if not cands:
                return None
            ranked: List[Tuple[int, float, int]] = []
            for cidx in cands:
                if cidx == name_col_idx:
                    continue
                big, _rate = _col_big_rate_counts(cidx)
                ranked.append((big, _col_median_abs(cidx), cidx))
            ranked.sort(key=lambda x: (x[0], x[1], -x[2]), reverse=True)
            return int(ranked[0][2]) if ranked else None

        def _header_year_cols(df: pd.DataFrame) -> Dict[int, int]:
            out: Dict[int, int] = {}
            for idx, c in enumerate(df.columns):
                m = re.search(r"\b(20\d{2})\b", str(c))
                if m:
                    out[idx] = int(m.group(1))
            head_n = min(4, len(df))
            for ridx in range(head_n):
                row = [str(x) for x in df.iloc[ridx].tolist()]
                for cidx, cell in enumerate(row):
                    m = re.search(r"\b(20\d{2})\b", cell)
                    if m:
                        out.setdefault(cidx, int(m.group(1)))
            if out:
                ncols = df.shape[1]

                def _amt_candidate(col_idx: int) -> bool:
                    vals = parsed_by_col.get(col_idx, [])
                    if len(vals) < 2:
                        return False
                    non_year = [
                        abs(v)
                        for v in vals
                        if not (1900 <= abs(v) <= 2100 and float(v).is_integer())
                    ]
                    if not non_year:
                        return False
                    non_year.sort()
                    med = non_year[len(non_year) // 2]
                    return bool(max(non_year) >= 100 or med >= 50)

                shifted: Dict[int, int] = {}
                for idx, yy in out.items():
                    # Keep direct mapping if it already looks amount-like.
                    if _amt_candidate(idx):
                        shifted[idx] = yy
                    # Handle split-currency headers: year token often sits 1-2 cols left of amount.
                    for off in (1, 2):
                        j = idx + off
                        if j >= ncols:
                            break
                        if _amt_candidate(j):
                            shifted.setdefault(j, yy)
                            break
                for idx, yy in out.items():
                    shifted.setdefault(idx, yy)
                out = shifted
            return out
        def _is_amount_like(col_idx: int) -> bool:
            vals = parsed_by_col.get(col_idx, [])
            if not vals:
                return False
            non_year = [
                abs(v)
                for v in vals
                if not (1900 <= abs(v) <= 2100 and float(v).is_integer())
            ]
            if not non_year:
                return False
            non_year.sort()
            med = non_year[len(non_year) // 2]
            mx = max(non_year)
            return bool(mx >= 1000 or med >= 100)
        row_index_cols = {idx for idx in num_cols if _is_row_index_col(idx)}

        def _is_rate_col(col_idx: int) -> bool:
            htxt = str(t2.columns[col_idx]).lower()
            if re.search(r"(rate|coupon|interest|yield|%)", htxt):
                return True
            vals = [abs(float(v)) for v in parsed_by_col.get(col_idx, []) if v is not None]
            if not vals:
                return False
            lt100 = sum(1 for v in vals if 0 < v < 100)
            return lt100 >= max(4, int(0.8 * len(vals)))

        amount_cols = [
            idx
            for idx in num_cols
            if idx != name_col_idx
            and idx not in row_index_cols
            and _is_amount_like(idx)
            and not _is_rate_col(idx)
        ]
        amt_col_idx: Optional[int] = None
        period_match: Optional[bool] = None
        asof_col_date: Optional[dt.date] = None
        asof_select_method: str = "fallback"

        def _choose_asof_col() -> Tuple[Optional[int], Optional[bool], Optional[dt.date], str]:
            if quarter_end is None:
                return None, None, None, "no_quarter_end"
            col_dates = _parse_header_dates_from_table(t2)
            if col_dates:
                exact_cols: List[int] = []
                near_cols: List[int] = []
                year_cols: List[int] = []
                best_near_days = 9999
                for idx, d in col_dates.items():
                    if idx not in num_cols:
                        continue
                    if idx not in amount_cols:
                        # Guardrail: never pick obvious rate columns as amount.
                        continue
                    vals = parsed_by_col.get(idx, [])
                    if len(vals) < 2:
                        continue
                    if d == quarter_end:
                        exact_cols.append(idx)
                    days = abs((d - quarter_end).days)
                    if days <= 7:
                        if days < best_near_days:
                            near_cols = [idx]
                            best_near_days = days
                        elif days == best_near_days:
                            near_cols.append(idx)
                    if d.year == int(quarter_end.year):
                        year_cols.append(idx)
                if exact_cols:
                    pick = _best_amount_col(exact_cols) or min(exact_cols)
                    return pick, True, col_dates.get(pick), "header_date_exact"
                if near_cols:
                    pick = _best_amount_col(near_cols) or min(near_cols)
                    return pick, True, col_dates.get(pick), "header_date_near"
                if year_cols:
                    pick = _best_amount_col(year_cols) or min(year_cols)
                    return pick, True, col_dates.get(pick), "header_date_year"
                return None, False, None, "header_date_no_match"

            hdr_year_cols = _header_year_cols(t2)
            if hdr_year_cols:
                year_match = [
                    idx
                    for idx, yy in hdr_year_cols.items()
                    if yy == int(quarter_end.year) and idx in amount_cols
                ]
                if year_match:
                    pick = _best_amount_col(year_match) or min(year_match)
                    return pick, True, None, "header_year_match"
                # Split-currency debt profile headers often place current-year amount in the
                # left-most amount column with year labels detached from numeric columns.
                head_txt = " ".join([str(c) for c in t2.columns])
                if not t2.empty:
                    for ridx in range(min(4, len(t2))):
                        head_txt += " " + " ".join(str(v) for v in t2.iloc[ridx].tolist())
                if re.search(rf"\b{int(quarter_end.year)}\b", head_txt) and amount_cols:
                    return min(amount_cols), True, None, "header_year_leftmost_amount"
                return None, False, None, "header_year_no_match"
            return None, False, None, "no_header_dates"

        pick_idx, pick_match, pick_date, pick_method = _choose_asof_col()
        if pick_idx is not None:
            amt_col_idx = int(pick_idx)
            period_match = bool(pick_match)
            asof_col_date = pick_date
            asof_select_method = str(pick_method)
        if amt_col_idx is None:
            # For quarter-specific parsing, avoid low-quality fallback into wrong comparator columns.
            if quarter_end is not None:
                continue
            cand = [c for c in (amount_cols if amount_cols else num_cols) if c != name_col_idx and c not in row_index_cols]
            if not cand:
                continue
            amt_col_idx = _best_amount_col(cand) or min(cand)
            period_match = None
            asof_select_method = "fallback_best_amount_col"
        if amt_col_idx == name_col_idx:
            continue
        selected_col_is_asof = bool(period_match)

        def _extract_row_amount(row_vals: List[Any]) -> Tuple[Optional[float], int]:
            used_col_idx = int(amt_col_idx)
            amt_cell_local = row_vals[used_col_idx] if used_col_idx < len(row_vals) else None
            amt_local = coerce_number(amt_cell_local)
            if selected_col_is_asof and (_is_blankish_amount_cell(amt_cell_local) or amt_local is None or amt_local == 0):
                if _is_currency_placeholder_cell(amt_cell_local):
                    for next_idx in range(used_col_idx + 1, min(len(row_vals), used_col_idx + 3)):
                        next_cell = row_vals[next_idx]
                        if _is_blankish_amount_cell(next_cell):
                            continue
                        next_amt = coerce_number(next_cell)
                        if next_amt is None:
                            continue
                        return float(next_amt), int(next_idx)
                return None, used_col_idx
            if (
                selected_col_is_asof
                and amt_local is not None
                and abs(float(amt_local)) < 100.0
            ):
                row_text_local = " ".join(str(v) for v in row_vals)
                has_rate_context = bool(
                    re.search(r"\b(%|convertible|notes?\s+due|term\s+loan)\b", row_text_local, re.I)
                )
                if has_rate_context:
                    for next_idx in range(used_col_idx + 1, min(len(row_vals), used_col_idx + 3)):
                        next_cell = row_vals[next_idx]
                        if _is_blankish_amount_cell(next_cell):
                            continue
                        next_amt = coerce_number(next_cell)
                        if next_amt is None:
                            continue
                        if abs(float(next_amt)) >= 1_000.0 and abs(float(next_amt)) >= abs(float(amt_local)) * 100.0:
                            return float(next_amt), int(next_idx)
            if amt_local is None and not selected_col_is_asof:
                nums = [coerce_number(v) for v in row_vals]
                nums = [v for v in nums if v is not None]
                if nums:
                    amt_local = max(nums, key=lambda x: abs(x))
            return (float(amt_local) if amt_local is not None else None), used_col_idx

        for row_vals in row_values:
            name = str(row_vals[name_col_idx] if name_col_idx < len(row_vals) else "").strip()
            if not re.search(r"[A-Za-z]", name):
                continue
            ln = name.lower()
            row_text = " ".join(str(v) for v in row_vals).lower()
            if "total" in ln and "debt" in ln:
                amt, used_amt_col_idx = _extract_row_amount(row_vals)
                if amt is not None:
                    local_table_total_label = name
                    if "long-term" in ln or "long term" in ln or "longterm" in ln:
                        local_table_total_long_term_debt = float(amt) * scale
                    else:
                        local_table_total_debt = float(amt) * scale
                continue

            is_other_row = bool(re.fullmatch(r"other", ln))
            if not is_other_row and not any(k in row_text for k in STRONG_DEBT_KEYWORDS):
                continue
            amt, used_amt_col_idx = _extract_row_amount(row_vals)
            if not name or amt is None:
                continue
            if "total" in ln or "subtotal" in ln:
                continue
            if len(name) < 4:
                continue
            out.append({
                "name": name,
                "amount": float(amt) * scale,
                "row_text": " ".join(str(v) for v in row_vals),
                "amount_col_idx": int(used_amt_col_idx) if amt_col_idx is not None else None,
                "asof_col_date": asof_col_date,
                "asof_select_method": asof_select_method,
                "parse_quality": (
                    "asof_matched_adjacent_numeric"
                    if bool(period_match) and int(used_amt_col_idx) != int(amt_col_idx)
                    else ("asof_matched" if bool(period_match) else "fallback_amount_col")
                ),
            })

        if out:
            due_like = [
                r for r in out if re.search(r"\b(due\s+20\d{2}|term\s+loan|notes?\s+due|convertible)\b", str(r.get("name") or ""), re.I)
            ]
            if len(due_like) < 2:
                out = []
                continue
            if local_table_total_debt is None and local_table_total_long_term_debt is None:
                out = []
                continue
            return out, score, local_table_total_debt, local_table_total_long_term_debt, local_table_total_label, scale, period_match

    return [], 0, table_total_debt, table_total_long_term_debt, table_total_label, global_scale, None


def parse_scheduled_debt_repayments_from_primary_doc(
    html_bytes: bytes,
    *,
    quarter_end: Optional[dt.date] = None,
) -> List[Dict[str, Any]]:
    html = html_bytes.decode("utf-8", errors="ignore")
    tables = read_html_tables_any(html_bytes)
    out: List[Dict[str, Any]] = []
    global_scale = _detect_scale(html)
    for t in tables:
        if t is None or t.empty:
            continue
        table_text = " ".join([*(str(c) for c in t.columns), *_stringify_table_cells(t.values.ravel().tolist())]).lower()
        if not re.search(
            r"scheduled\s+(?:long-term\s+)?debt\s+repayments|long-term\s+debt\s+repayments|scheduled\s+repayments",
            table_text,
            re.I,
        ):
            continue
        scale = _detect_scale(table_text)
        if scale == 1.0 and global_scale != 1.0:
            scale = global_scale
        if t.shape[1] < 2:
            continue
        df = t.copy().dropna(axis=0, how="all").dropna(axis=1, how="all")
        if df.empty or df.shape[1] < 2:
            continue
        name_col_idx = 0
        amount_col_idx = None
        for idx in range(1, df.shape[1]):
            vals = [coerce_number(v) for v in df.iloc[:, idx].tolist()]
            parsed = [float(v) for v in vals if v is not None]
            if len(parsed) >= 3 and max(abs(v) for v in parsed) >= 100:
                amount_col_idx = idx
                break
        if amount_col_idx is None:
            continue
        rows_local: List[Dict[str, Any]] = []
        for row_vals in df.itertuples(index=False, name=None):
            label = re.sub(r"\s+", " ", str(row_vals[name_col_idx] or "")).strip()
            if not label:
                continue
            label_l = label.lower()
            if "total" in label_l:
                continue
            maturity_year = None
            maturity_label = None
            ym = re.search(r"\b(20\d{2})\b", label)
            if ym:
                maturity_year = int(ym.group(1))
                maturity_label = str(maturity_year)
            elif "thereafter" in label_l:
                maturity_label = "Thereafter"
            else:
                continue
            amt_cell = row_vals[amount_col_idx]
            if _is_blankish_amount_cell(amt_cell):
                continue
            amt = coerce_number(amt_cell)
            if amt is None or float(amt) <= 0:
                continue
            rows_local.append(
                {
                    "quarter": quarter_end,
                    "maturity_year": maturity_year,
                    "maturity_label": maturity_label,
                    "amount_total": float(amt) * scale,
                    "source_kind": "scheduled_repayments_fallback",
                    "row_text": " ".join(str(v) for v in row_vals),
                }
            )
        if rows_local:
            return rows_local
    low = html.lower()
    phrase_match = re.search(
        r"scheduled\s+(?:long-term\s+)?debt\s+repayments[^:]{0,120}:",
        low,
        re.I,
    )
    if phrase_match:
        snippet = html[phrase_match.start() : min(len(html), phrase_match.start() + 8000)]
        plain = re.sub(r"<[^>]+>", " ", snippet)
        plain = html_lib.unescape(re.sub(r"\s+", " ", plain)).strip()
        rows_local = []
        for label in ["2026", "2027", "2028", "2029", "2030", "Thereafter"]:
            m = re.search(rf"\b{label}\b\s*\$?\s*([0-9]{{1,3}}(?:,[0-9]{{3}})*)", plain, re.I)
            if not m:
                continue
            amt = coerce_number(m.group(1))
            if amt is None or float(amt) <= 0:
                continue
            rows_local.append(
                {
                    "quarter": quarter_end,
                    "maturity_year": int(label) if label.isdigit() else None,
                    "maturity_label": label,
                    "amount_total": float(amt) * global_scale,
                    "source_kind": "scheduled_repayments_fallback",
                    "row_text": plain[:1000],
                }
            )
        if rows_local:
            return rows_local
    return []


def build_debt_tranches_tier2(
    sec: Any,
    cik_int: int,
    submissions: Dict[str, Any],
    max_quarters: int,
    min_year: Optional[int] = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    seen_accn: set[str] = set()
    for batch in _iter_submission_batches(sec, submissions):
        forms = batch.get("form", [])
        accns = batch.get("accessionNumber", [])
        report_dates = batch.get("reportDate", []) or []
        filing_dates = batch.get("filingDate", []) or []
        primary_docs = batch.get("primaryDocument", []) or []

        n = min(len(forms), len(accns))
        for i in range(n):
            form = forms[i]
            if form not in ("10-Q", "10-Q/A", "10-K", "10-K/A"):
                continue
            accn = accns[i]
            if accn in seen_accn:
                continue
            seen_accn.add(accn)
            doc = primary_docs[i] if i < len(primary_docs) else None
            if not doc:
                continue
            rep = report_dates[i] if i < len(report_dates) else None
            fdate = filing_dates[i] if i < len(filing_dates) else None

            q_end = parse_date(rep) or parse_date(fdate)
            if not _is_quarter_end(q_end):
                q_end = _coerce_prev_quarter_end(q_end)
            if q_end is None:
                continue
            if min_year is not None and q_end.year < min_year:
                continue

            accn_nd = normalize_accession(accn)
            try:
                html_bytes = sec.download_document(cik_int, accn_nd, doc)
            except Exception:
                continue
            tr, score, table_total_debt, table_total_long_term_debt, table_total_label, scale_applied, period_match = (
                parse_debt_tranches_from_primary_doc(
                    html_bytes,
                    quarter_end=q_end,
                )
            )
            if not tr:
                continue

            confidence = "high" if score >= 2 else "medium"
            for item in tr:
                rows.append({
                    "quarter": q_end,
                    "tranche_name": item.get("name"),
                    "amount": float(item.get("amount")) if item.get("amount") is not None else None,
                    "row_text": item.get("row_text"),
                    "amount_col_idx": item.get("amount_col_idx"),
                    "asof_col_date": item.get("asof_col_date"),
                    "asof_select_method": item.get("asof_select_method"),
                    "parse_quality": item.get("parse_quality"),
                    "accn": accn,
                    "form": form,
                    "filed": parse_date(fdate),
                    "report_date": parse_date(rep),
                    "doc": doc,
                    "confidence": confidence,
                    "table_total_debt": table_total_debt,
                    "table_total_long_term_debt": table_total_long_term_debt,
                    "table_total_label": table_total_label,
                    "scale_applied": scale_applied,
                    "period_match": period_match,
                })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    qs = sorted(df["quarter"].unique())[-max_quarters:]
    return df[df["quarter"].isin(qs)].sort_values(["quarter", "tranche_name"]).copy()


def build_debt_schedule_tier2(
    sec: Any,
    cik_int: int,
    submissions: Dict[str, Any],
    max_quarters: int,
    min_year: Optional[int] = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    seen_accn: set[str] = set()
    for batch in _iter_submission_batches(sec, submissions):
        forms = batch.get("form", []) or []
        accns = batch.get("accessionNumber", []) or []
        report_dates = batch.get("reportDate", []) or []
        filing_dates = batch.get("filingDate", []) or []
        primary_docs = batch.get("primaryDocument", []) or []
        n = min(len(forms), len(accns))
        for i in range(n):
            form = str(forms[i] or "").upper().strip()
            if form not in ("10-Q", "10-Q/A", "10-K", "10-K/A"):
                continue
            accn = str(accns[i] or "").strip()
            if not accn or accn in seen_accn:
                continue
            doc = str(primary_docs[i] or "") if i < len(primary_docs) else ""
            if not doc:
                continue
            rep = report_dates[i] if i < len(report_dates) else None
            fdate = filing_dates[i] if i < len(filing_dates) else None
            q_end = parse_date(rep) or parse_date(fdate)
            if not _is_quarter_end(q_end):
                q_end = _coerce_prev_quarter_end(q_end)
            if q_end is None:
                continue
            if min_year is not None and q_end.year < min_year:
                continue
            seen_accn.add(accn)
            try:
                html_bytes = sec.download_document(cik_int, normalize_accession(accn), doc)
            except Exception:
                continue
            parsed = parse_scheduled_debt_repayments_from_primary_doc(html_bytes, quarter_end=q_end)
            if not parsed:
                continue
            for item in parsed:
                item["accn"] = accn
                item["form"] = form
                item["filed"] = parse_date(fdate)
                item["report_date"] = parse_date(rep)
                item["doc"] = doc
                rows.append(item)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    qs = sorted(df["quarter"].dropna().unique())[-max_quarters:]
    return df[df["quarter"].isin(qs)].sort_values(["quarter", "maturity_year", "maturity_label"]).copy()
