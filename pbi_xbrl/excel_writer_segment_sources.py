"""Shared segment-source helpers for workbook writer surfaces."""
from __future__ import annotations

import math
import re
from datetime import date
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from .guidance_lexicon import normalize_text as glx_normalize_text


def _anf_fiscal_year_from_quarter_end(qd: Any) -> Optional[int]:
    q_ts = pd.to_datetime(qd, errors="coerce")
    if pd.isna(q_ts):
        return None
    q_date = pd.Timestamp(q_ts).date()
    return int(q_date.year) - 1 if q_date.month in (1, 2) else int(q_date.year)


def _anf_fiscal_quarter_from_quarter_end(qd: Any) -> Optional[int]:
    q_ts = pd.to_datetime(qd, errors="coerce")
    if pd.isna(q_ts):
        return None
    month = int(pd.Timestamp(q_ts).month)
    if month <= 2 or month == 12:
        return 4
    if month <= 5:
        return 1
    if month <= 8:
        return 2
    if month <= 11:
        return 3
    return None


def _annual_segment_latest_year_for_qa(
    annual_revenue_values: Dict[str, Dict[int, Any]],
    fy_source_q: Any,
    *,
    is_anf_profile: bool = False,
) -> Optional[int]:
    available_years = sorted(
        {
            int(year)
            for by_year in dict(annual_revenue_values or {}).values()
            for year in dict(by_year or {}).keys()
            if str(year).isdigit()
        }
    )
    if not available_years:
        return None
    source_year: Optional[int] = None
    if is_anf_profile:
        source_year = _anf_fiscal_year_from_quarter_end(fy_source_q)
    if source_year is None:
        q_ts = pd.to_datetime(fy_source_q, errors="coerce")
        if pd.notna(q_ts):
            source_year = int(pd.Timestamp(q_ts).year)
    if source_year in available_years:
        return int(source_year)
    return int(available_years[-1])


def _anf_history_revenue_map(src_in: Any) -> Dict[date, float]:
    out: Dict[date, float] = {}
    if src_in is None:
        return out
    try:
        if isinstance(src_in, pd.DataFrame):
            if "quarter" not in src_in.columns or "revenue" not in src_in.columns:
                return out
            for rec in src_in.to_dict("records"):
                q_ts = pd.to_datetime(rec.get("quarter"), errors="coerce")
                val_num = pd.to_numeric(rec.get("revenue"), errors="coerce")
                if pd.notna(q_ts) and pd.notna(val_num):
                    out[pd.Timestamp(q_ts).date()] = float(val_num)
            return out
        if isinstance(src_in, pd.Series):
            for q_raw, val_raw in src_in.items():
                q_ts = pd.to_datetime(q_raw, errors="coerce")
                val_num = pd.to_numeric(val_raw, errors="coerce")
                if pd.notna(q_ts) and pd.notna(val_num):
                    out[pd.Timestamp(q_ts).date()] = float(val_num)
            return out
        for q_raw, val_raw in dict(src_in or {}).items():
            q_ts = pd.to_datetime(q_raw, errors="coerce")
            val_num = pd.to_numeric(val_raw, errors="coerce")
            if pd.notna(q_ts) and pd.notna(val_num):
                out[pd.Timestamp(q_ts).date()] = float(val_num)
    except Exception:
        return out
    return out


def _filter_anf_quarterly_segment_actual_rows(
    slides_segments: pd.DataFrame,
    history_revenue_by_quarter: Optional[Any] = None,
) -> pd.DataFrame:
    if slides_segments is None or slides_segments.empty:
        return pd.DataFrame() if slides_segments is None else slides_segments
    df = slides_segments.copy()
    if "quarter" not in df.columns or "metric" not in df.columns or "value" not in df.columns:
        return df.iloc[0:0].copy()
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["quarter"].notna() & df["value"].notna()].copy()
    metric_ser = df["metric"].astype(str).str.strip().str.lower()
    annual_like = pd.Series([False] * len(df), index=df.index)
    if "period_type" in df.columns:
        period_ser = df["period_type"].astype(str).str.strip().str.lower()
        annual_like = annual_like | period_ser.isin({"annual", "year", "fy", "full_year", "full year", "ytd"})
    if "source_period_label" in df.columns:
        source_period_ser = df["source_period_label"].astype(str).str.strip().str.lower()
        annual_like = annual_like | source_period_ser.isin({"annual", "year", "fy", "full_year", "full year", "ytd"})
        annual_like = annual_like | source_period_ser.str.fullmatch(r"fy(?:\s*20\d{2})?", na=False)
    if annual_like.any():
        # Keep annual retail-driver facts (stores, digital mix, buybacks) anchored
        # to the fiscal Q4 date, but never leak annual segment revenue totals into
        # the quarterly actuals grid.
        df = df[~(annual_like & metric_ser.isin({"revenue", "adj_segment_ebit", "adj_segment_da", "adj_segment_ebitda"}))].copy()
        metric_ser = df["metric"].astype(str).str.strip().str.lower()
    segment_ser = df.get("segment", pd.Series([""] * len(df), index=df.index)).astype(str).str.strip()
    tiny_revenue = metric_ser.eq("revenue") & segment_ser.isin({"Americas", "EMEA", "APAC"}) & (df["value"].abs() < 750_000.0)
    df = df[~tiny_revenue].copy()

    hist_rev = _anf_history_revenue_map(history_revenue_by_quarter)
    if hist_rev and not df.empty:
        keep = pd.Series([True] * len(df), index=df.index)
        rev_rows = df[df["metric"].astype(str).str.strip().str.lower().eq("revenue")].copy()

        def _close_to_hist(val_in: Any, hist_in: float) -> bool:
            val_num = pd.to_numeric(val_in, errors="coerce")
            if pd.isna(val_num) or abs(float(hist_in)) < 1.0:
                return False
            return abs(float(val_num) - float(hist_in)) <= max(25_000_000.0, abs(float(hist_in)) * 0.08)

        for q_ts, q_sub in rev_rows.groupby("quarter", sort=False):
            qd = pd.Timestamp(q_ts).date()
            hist_val = hist_rev.get(qd)
            if hist_val is None or abs(float(hist_val)) < 1.0:
                continue
            q_segments = q_sub["segment"].astype(str).str.strip()
            for idx, rec in q_sub.iterrows():
                seg_txt = str(rec.get("segment") or "").strip()
                val_num = pd.to_numeric(rec.get("value"), errors="coerce")
                if pd.isna(val_num):
                    continue
                val_f = float(val_num)
                if seg_txt == "Total Company" and not _close_to_hist(val_f, float(hist_val)):
                    keep.loc[idx] = False
                elif seg_txt in {"Americas", "EMEA", "APAC", "Abercrombie", "Hollister"} and val_f > abs(float(hist_val)) * 1.10:
                    keep.loc[idx] = False

            for family in ({"Americas", "EMEA", "APAC"}, {"Abercrombie", "Hollister"}):
                fam_idx = q_sub[q_segments.isin(family)].index
                fam_segments = set(q_segments.loc[fam_idx].tolist())
                if not family.issubset(fam_segments):
                    continue
                fam_sum = float(pd.to_numeric(q_sub.loc[fam_idx, "value"], errors="coerce").dropna().sum())
                if fam_sum > abs(float(hist_val)) * 1.25 and not _close_to_hist(fam_sum, float(hist_val)):
                    keep.loc[fam_idx] = False
        df = df[keep].copy()
    return df.reset_index(drop=True)


def _anf_add_total_company_quarter_revenue_from_history(
    quarterly_metrics: Dict[str, Any],
    history_revenue_by_quarter: Optional[Any],
    quarters: Optional[Sequence[Any]] = None,
) -> Dict[str, Any]:
    if not quarterly_metrics or history_revenue_by_quarter is None:
        return quarterly_metrics
    hist_rev = _anf_history_revenue_map(history_revenue_by_quarter)
    if not hist_rev:
        return quarterly_metrics
    if quarters:
        quarter_keys: Set[date] = set()
        for q_raw in quarters:
            q_ts = pd.to_datetime(q_raw, errors="coerce")
            if pd.notna(q_ts):
                quarter_keys.add(pd.Timestamp(q_ts).date())
    else:
        quarter_keys = set(hist_rev.keys())

    out: Dict[str, Any] = dict(quarterly_metrics)
    revenue_metric = dict(out.get("Revenue") or {})
    total_company = dict(revenue_metric.get("Total Company") or {})
    changed = False
    for qd, hist_val in hist_rev.items():
        if quarter_keys and qd not in quarter_keys:
            continue
        q_key = pd.Timestamp(qd)
        existing = pd.to_numeric(total_company.get(q_key), errors="coerce")
        if pd.notna(existing) and abs(float(existing)) > 1e-9:
            continue
        total_company[q_key] = float(hist_val)
        changed = True
    if changed:
        revenue_metric["Total Company"] = total_company
        out["Revenue"] = revenue_metric
    return out


def _anf_fill_brand_quarter_revenue_from_annual_segments_for_bs(
    quarterly_metrics: Dict[str, Any],
    slides_segments: Optional[pd.DataFrame],
    history_revenue_by_quarter: Optional[Any],
) -> Dict[str, Any]:
    """Fill one missing ANF segment quarter from source-backed FY minus Q1-Q3.

    ANF segment slides sometimes anchor annual brand/geography revenue to the
    fiscal Q4 date.  The quarterly grid must not treat that annual total as Q4,
    but if the same source provides FY revenue and the other three quarters are
    present, Q4 can be derived without inventing data.
    """
    if not quarterly_metrics or slides_segments is None or slides_segments.empty:
        return quarterly_metrics
    required = {"quarter", "segment", "metric", "value"}
    if not required.issubset(set(slides_segments.columns)):
        return quarterly_metrics
    hist_rev = _anf_history_revenue_map(history_revenue_by_quarter)
    if not hist_rev:
        return quarterly_metrics

    df = slides_segments.copy()
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["quarter"].notna() & df["value"].notna()].copy()
    if df.empty:
        return quarterly_metrics
    metric_ser = df["metric"].astype(str).str.strip().str.lower()
    segment_ser = df["segment"].astype(str).str.strip()
    annual_like = pd.Series([False] * len(df), index=df.index)
    if "period_type" in df.columns:
        period_ser = df["period_type"].astype(str).str.strip().str.lower()
        annual_like = annual_like | period_ser.isin({"annual", "year", "fy", "full_year", "full year"})
    if "source_period_label" in df.columns:
        source_period_ser = df["source_period_label"].astype(str).str.strip().str.lower()
        annual_like = annual_like | source_period_ser.isin({"annual", "year", "fy", "full_year", "full year"})
        annual_like = annual_like | source_period_ser.str.fullmatch(r"fy(?:\s*20\d{2})?", na=False)
    derivable_segments = {"Abercrombie", "Hollister", "Americas", "EMEA", "APAC"}
    annual_df = df[
        annual_like
        & metric_ser.eq("revenue")
        & segment_ser.isin(derivable_segments)
        & (df["value"].abs() >= 20_000_000.0)
    ].copy()
    if annual_df.empty:
        return quarterly_metrics

    annual_values: Dict[Tuple[str, int], float] = {}
    for rec in annual_df.sort_values(["quarter", "segment"], kind="stable").to_dict("records"):
        fy = _anf_fiscal_year_from_quarter_end(rec.get("quarter"))
        seg = str(rec.get("segment") or "").strip()
        val = pd.to_numeric(rec.get("value"), errors="coerce")
        if fy is None or seg not in derivable_segments or pd.isna(val):
            continue
        annual_values[(seg, int(fy))] = float(val)
    if not annual_values:
        return quarterly_metrics

    out = dict(quarterly_metrics)
    revenue_metric = {
        str(seg_name): {pd.Timestamp(q): float(v) for q, v in dict(q_map or {}).items()}
        for seg_name, q_map in dict(out.get("Revenue") or {}).items()
    }
    if not revenue_metric:
        return quarterly_metrics

    fiscal_quarters_by_year: Dict[int, List[pd.Timestamp]] = {}
    for qd in sorted(hist_rev):
        fy = _anf_fiscal_year_from_quarter_end(qd)
        fq = _anf_fiscal_quarter_from_quarter_end(qd)
        if fy is None or fq is None:
            continue
        fiscal_quarters_by_year.setdefault(int(fy), []).append(pd.Timestamp(qd))

    changed = False
    for (seg_name, fy), annual_val in sorted(annual_values.items()):
        q_list = fiscal_quarters_by_year.get(fy) or []
        if len(q_list) != 4:
            continue
        seg_bucket = revenue_metric.setdefault(seg_name, {})
        present_vals: List[float] = []
        missing_qs: List[pd.Timestamp] = []
        for q_ts in sorted(q_list, key=lambda q: _anf_fiscal_quarter_from_quarter_end(q.date()) or 0):
            existing = pd.to_numeric(seg_bucket.get(pd.Timestamp(q_ts)), errors="coerce")
            if pd.notna(existing):
                present_vals.append(float(existing))
            else:
                missing_qs.append(pd.Timestamp(q_ts))
        if len(missing_qs) != 1 or len(present_vals) != 3:
            continue
        derived = float(annual_val) - float(sum(present_vals))
        missing_q = missing_qs[0]
        hist_total = hist_rev.get(missing_q.date())
        if not math.isfinite(derived) or derived <= 0:
            continue
        if hist_total is not None and derived > float(hist_total) * 1.05:
            continue
        seg_bucket[missing_q] = derived
        changed = True

    if not changed:
        return quarterly_metrics
    out["Revenue"] = revenue_metric
    return out


def _pbi_repair_total_reportable_segment_quarterly_totals_for_bs(quarterly_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Rebuild PBI reportable-segment totals from component rows.

    PBI source extraction can occasionally capture the table's reportable
    segment revenue total under adjacent EBIT/D&A/EBITDA metric labels.  The
    component rows are cleaner and reconcile, so for the BS_Segments quarterly
    grid we use SendTech/Presort (and Other operations if present) to repair
    the total row instead of trusting a suspicious parsed total.
    """
    if not quarterly_metrics:
        return quarterly_metrics

    def _copy_metric_store(src: Mapping[str, Any]) -> Dict[str, Dict[str, Dict[pd.Timestamp, float]]]:
        copied: Dict[str, Dict[str, Dict[pd.Timestamp, float]]] = {}
        for metric_name, seg_map in dict(src or {}).items():
            metric_bucket: Dict[str, Dict[pd.Timestamp, float]] = {}
            for seg_name, q_map in dict(seg_map or {}).items():
                q_bucket: Dict[pd.Timestamp, float] = {}
                for q_raw, value_in in dict(q_map or {}).items():
                    q_ts = pd.to_datetime(q_raw, errors="coerce")
                    value_num = pd.to_numeric(value_in, errors="coerce")
                    if pd.notna(q_ts) and pd.notna(value_num):
                        q_bucket[pd.Timestamp(q_ts)] = float(value_num)
                if q_bucket:
                    metric_bucket[str(seg_name)] = q_bucket
            if metric_bucket:
                copied[str(metric_name)] = metric_bucket
        return copied

    out = _copy_metric_store(quarterly_metrics)

    def _is_component_segment(segment_name: Any) -> bool:
        low = glx_normalize_text(str(segment_name or "")).lower()
        if not low or "total reportable" in low or "corporate" in low or "intersegment" in low:
            return False
        return (
            "sendtech" in low
            or "sending technology" in low
            or "presort" in low
            or "other operations" in low
            or low in {"sendtech solutions", "presort services"}
        )

    adj_ebit_by_seg = dict(out.get("Adjusted EBIT") or {})
    da_by_seg = dict(out.get("Depreciation & amortization") or {})
    if adj_ebit_by_seg and da_by_seg:
        ebitda_by_seg = {
            str(seg_name): dict(q_map or {})
            for seg_name, q_map in dict(out.get("Adjusted EBITDA") or {}).items()
        }
        for seg_name in sorted(set(adj_ebit_by_seg) | set(da_by_seg)):
            ebit_series = dict(adj_ebit_by_seg.get(seg_name) or {})
            da_series = dict(da_by_seg.get(seg_name) or {})
            if not ebit_series or not da_series:
                continue
            seg_bucket = ebitda_by_seg.setdefault(str(seg_name), {})
            for q_key in sorted(set(ebit_series) & set(da_series)):
                q_ts = pd.Timestamp(q_key)
                existing = pd.to_numeric(seg_bucket.get(q_ts), errors="coerce")
                if pd.notna(existing):
                    continue
                ebit_num = pd.to_numeric(ebit_series.get(q_key), errors="coerce")
                da_num = pd.to_numeric(da_series.get(q_key), errors="coerce")
                if pd.notna(ebit_num) and pd.notna(da_num):
                    seg_bucket[q_ts] = float(ebit_num) + float(da_num)
        if ebitda_by_seg:
            out["Adjusted EBITDA"] = ebitda_by_seg

    repair_metrics = {"Revenue", "Adjusted EBIT", "Depreciation & amortization", "Adjusted EBITDA"}
    for metric_name in repair_metrics:
        seg_map = out.get(metric_name)
        if not seg_map:
            continue
        total_bucket = dict(seg_map.get("Total reportable segments") or {})
        if not total_bucket:
            continue
        send_bucket = dict(seg_map.get("SendTech Solutions") or {})
        presort_bucket = dict(seg_map.get("Presort Services") or {})
        other_bucket = dict(seg_map.get("Other operations") or {})
        changed_other = False
        for q_key, total_in in total_bucket.items():
            q_ts = pd.Timestamp(q_key)
            if pd.notna(pd.to_numeric(other_bucket.get(q_ts), errors="coerce")):
                continue
            total_num = pd.to_numeric(total_in, errors="coerce")
            send_num = pd.to_numeric(send_bucket.get(q_ts), errors="coerce")
            presort_num = pd.to_numeric(presort_bucket.get(q_ts), errors="coerce")
            if pd.isna(total_num) or pd.isna(send_num) or pd.isna(presort_num):
                continue
            residual = float(total_num) - float(send_num) - float(presort_num)
            tolerance = max(abs(float(total_num)) * 0.002, 1_000.0)
            if abs(residual) <= tolerance:
                other_bucket[q_ts] = 0.0
                changed_other = True
            elif residual > 0 and residual <= max(abs(float(total_num)) * 0.05, 5_000_000.0):
                other_bucket[q_ts] = float(residual)
                changed_other = True
        if changed_other:
            seg_map["Other operations"] = other_bucket
            out[metric_name] = seg_map

    for metric_name in repair_metrics:
        seg_map = out.get(metric_name)
        if not seg_map:
            continue
        by_quarter: Dict[pd.Timestamp, List[float]] = {}
        for seg_name, q_map in dict(seg_map).items():
            if not _is_component_segment(seg_name):
                continue
            for q_key, value_in in dict(q_map or {}).items():
                value_num = pd.to_numeric(value_in, errors="coerce")
                if pd.notna(value_num):
                    by_quarter.setdefault(pd.Timestamp(q_key), []).append(float(value_num))
        if not by_quarter:
            continue
        total_bucket = dict(seg_map.get("Total reportable segments") or {})
        changed = False
        for q_key, values in by_quarter.items():
            # PBI currently has SendTech + Presort.  Require at least two
            # component values so a lone segment is not mislabeled as total.
            if len(values) < 2:
                continue
            total_bucket[pd.Timestamp(q_key)] = float(sum(values))
            changed = True
        if changed:
            seg_map["Total reportable segments"] = total_bucket
            out[metric_name] = seg_map

    revenue_by_seg = dict(out.get("Revenue") or {})
    ebit_by_seg = dict(out.get("Adjusted EBIT") or {})
    if revenue_by_seg and ebit_by_seg:
        for margin_metric in ("EBIT margin %", "Segment operating margin %"):
            margin_map = {
                str(seg_name): dict(q_map or {})
                for seg_name, q_map in dict(out.get(margin_metric) or {}).items()
            }
            changed = False
            for seg_name, ebit_series in ebit_by_seg.items():
                rev_series = dict(revenue_by_seg.get(seg_name) or {})
                if not rev_series:
                    continue
                seg_bucket = margin_map.setdefault(str(seg_name), {})
                for q_key, ebit_val in dict(ebit_series or {}).items():
                    q_ts = pd.Timestamp(q_key)
                    existing = pd.to_numeric(seg_bucket.get(q_ts), errors="coerce")
                    if pd.notna(existing):
                        continue
                    rev_num = pd.to_numeric(rev_series.get(q_ts), errors="coerce")
                    ebit_num = pd.to_numeric(ebit_val, errors="coerce")
                    if pd.notna(rev_num) and pd.notna(ebit_num) and abs(float(rev_num)) > 1e-9:
                        seg_bucket[q_ts] = float(ebit_num) / float(rev_num)
                        changed = True
            if changed:
                out[margin_metric] = margin_map
    return out


def _pbi_add_corporate_reconciliation_from_release_text(
    store: Dict[str, Dict[str, Dict[pd.Timestamp, float]]],
    txt: str,
    q_ts: pd.Timestamp,
    parse_money_thousands: Callable[[Any], Optional[float]],
) -> None:
    """Add PBI corporate expense rows when the release exposes the reconciliation."""
    if not store or not txt:
        return

    def _get(metric_name: str, segment_name: str) -> Optional[float]:
        value = pd.to_numeric(
            dict(dict(store.get(metric_name) or {}).get(segment_name) or {}).get(pd.Timestamp(q_ts)),
            errors="coerce",
        )
        if pd.isna(value):
            return None
        return float(value)

    def _put(metric_name: str, segment_name: str, value: Optional[float]) -> None:
        value_num = pd.to_numeric(value, errors="coerce")
        if pd.isna(value_num):
            return
        store.setdefault(metric_name, {}).setdefault(segment_name, {})[pd.Timestamp(q_ts)] = float(value_num)

    seg_ebit = _get("Adjusted EBIT", "Total reportable segments")
    seg_da = _get("Depreciation & amortization", "Total reportable segments")
    seg_ebitda = _get("Adjusted EBITDA", "Total reportable segments")
    if seg_ebit is None or seg_da is None or seg_ebitda is None:
        return

    recon_match = re.search(
        r"Reconciliation\s+of\s+Reported\s+Consolidated\s+Results\s+to\s+Adjusted\s+Results.*?"
        r"(?:Reconciliation\s+of\s+diluted\s+earnings\s+per\s+share|Reconciliation\s+of\s+net\s+cash|$)",
        txt,
        flags=re.I | re.S,
    )
    recon_txt = recon_match.group(0) if recon_match else ""
    company_match = re.search(
        r"\bAdjusted\s+EBIT\s+\$?\s*([\(\)0-9,.\-]+).*?"
        r"\bDepreciation\s+and\s+amortization\s+\$?\s*([\(\)0-9,.\-]+).*?"
        r"\bAdjusted\s+EBITDA\s+\$?\s*([\(\)0-9,.\-]+)",
        recon_txt,
        flags=re.I | re.S,
    )
    if not company_match:
        return
    company_ebit = parse_money_thousands(company_match.group(1))
    company_da = parse_money_thousands(company_match.group(2))
    company_ebitda = parse_money_thousands(company_match.group(3))
    if company_ebit is None or company_da is None or company_ebitda is None:
        return

    corp_ebit = company_ebit - seg_ebit
    corp_line = re.search(r"\bCorporate\s+expenses\s+\$?\s*([\(\)0-9,.\-]+)", txt, flags=re.I)
    corp_line_val = parse_money_thousands(corp_line.group(1)) if corp_line else None
    if corp_line_val is not None and abs(corp_line_val - corp_ebit) <= max(1_000_000.0, abs(corp_ebit) * 0.1):
        corp_ebit = corp_line_val

    _put("Adjusted EBIT", "Corporate expense", corp_ebit)
    _put("Depreciation & amortization", "Corporate expense", company_da - seg_da)
    _put("Adjusted EBITDA", "Corporate expense", company_ebitda - seg_ebitda)


def _anf_annual_segment_data_from_slides_segments(slides_segments: pd.DataFrame) -> Dict[str, Any]:
    if slides_segments is None or slides_segments.empty:
        return {}
    required_cols = {"quarter", "segment", "metric", "value"}
    if not required_cols.issubset(set(slides_segments.columns)):
        return {}
    df = slides_segments.copy()
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "period_type" not in df.columns:
        return {}
    period_ser = df["period_type"].astype(str).str.strip().str.lower()
    df = df[
        df["quarter"].notna()
        & df["value"].notna()
        & period_ser.isin({"annual", "year", "fy", "full_year"})
        & df["metric"].astype(str).str.strip().str.lower().eq("revenue")
        & df["segment"].astype(str).str.strip().isin({"Americas", "EMEA", "APAC"})
        & (df["value"].abs() >= 750_000.0)
    ].copy()
    if df.empty:
        return {}
    df["_fy"] = df["quarter"].map(_anf_fiscal_year_from_quarter_end)
    df = df[df["_fy"].notna()].copy()
    if df.empty:
        return {}
    metrics: Dict[str, Dict[str, Dict[int, float]]] = {"Revenues": {}}
    source_docs: List[str] = []
    source_qd: Optional[date] = None
    for rec in df.sort_values(["_fy", "segment", "value"], kind="stable").to_dict("records"):
        seg = str(rec.get("segment") or "").strip()
        fy = int(rec.get("_fy"))
        value = float(rec.get("value"))
        metrics["Revenues"].setdefault(seg, {})[fy] = value
        doc = str(rec.get("doc") or "").strip()
        if doc and doc not in source_docs:
            source_docs.append(doc)
        qd = pd.Timestamp(rec.get("quarter")).date()
        if source_qd is None or qd > source_qd:
            source_qd = qd
    years = sorted({int(y) for seg_map in metrics["Revenues"].values() for y in seg_map.keys()})
    if not years:
        return {}
    return {
        "metrics": metrics,
        "assets": {},
        "years": years,
        "source_doc": " | ".join(source_docs[:3]) if source_docs else "Slides_Segments",
        "source_qd": source_qd,
    }
