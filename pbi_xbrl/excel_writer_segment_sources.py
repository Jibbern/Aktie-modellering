"""Shared segment-source helpers for workbook writer surfaces."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from .guidance_lexicon import normalize_text as glx_normalize_text
from .segment_normalization import (
    SEGMENT_RESIDUAL_LEDGER_CONTRACT_ID,
    SegmentNormalizationError,
    SegmentResidualInputFact,
    derive_exact_zero_segment_residual,
    validate_segment_residual_ledger_payload,
)


def _segment_residual_ledger_payload(
    source_facts: Iterable[SegmentResidualInputFact],
    derivations: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    fact_rows = {fact.record_id: fact.to_dict() for fact in source_facts}
    derivation_rows = {
        str(row.get("derivation_id") or ""): dict(row)
        for row in derivations
        if str(row.get("derivation_id") or "").strip()
    }
    referenced_ids = {
        str(input_id)
        for row in derivation_rows.values()
        for input_id in row.get("input_record_ids", ())
    }
    return {
        "contract_id": SEGMENT_RESIDUAL_LEDGER_CONTRACT_ID,
        "source_facts": [fact_rows[record_id] for record_id in sorted(referenced_ids) if record_id in fact_rows],
        "derivations": [derivation_rows[record_id] for record_id in sorted(derivation_rows)],
    }


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


_SEGMENT_LEDGER_KEYS = frozenset({"contract_id", "source_facts", "derivations"})
_SegmentTarget = Tuple[str, str, pd.Timestamp]


@dataclass(frozen=True)
class _SegmentPackageLedgerMergeValidation:
    """Atomic ledger/package decision consumed by the segment merge owner."""

    ledger_present: bool
    ledger_valid: bool
    target_enumeration_valid: bool
    declared_targets: frozenset[_SegmentTarget]
    validated_targets: frozenset[_SegmentTarget]
    invalid_reason: str
    package_merge_allowed: bool
    validated_ledger: Optional[Dict[str, Any]]


def _enumerate_segment_ledger_targets(
    raw_ledger: Any,
) -> Tuple[bool, Set[_SegmentTarget], str]:
    """Enumerate declared targets only after strict container-shape validation."""

    if not isinstance(raw_ledger, Mapping):
        return False, set(), "ledger_container_not_mapping"
    ledger = dict(raw_ledger)
    if set(ledger) != _SEGMENT_LEDGER_KEYS:
        return False, set(), "ledger_top_level_fields_invalid"
    if not isinstance(ledger.get("contract_id"), str):
        return False, set(), "ledger_contract_id_type_invalid"
    if not isinstance(ledger.get("source_facts"), list):
        return False, set(), "ledger_source_facts_type_invalid"
    derivations = ledger.get("derivations")
    if not isinstance(derivations, list):
        return False, set(), "ledger_derivations_type_invalid"

    targets: Set[_SegmentTarget] = set()
    for row_in in derivations:
        if not isinstance(row_in, Mapping):
            return False, set(), "ledger_derivation_row_type_invalid"
        row = dict(row_in)
        period_ts = pd.to_datetime(row.get("period_end"), errors="coerce")
        metric_name = str(row.get("metric_label") or "").strip()
        target_member = str(row.get("target_member") or "").strip()
        if pd.isna(period_ts) or not metric_name or not target_member:
            return False, set(), "ledger_derivation_target_identity_invalid"
        targets.add((metric_name, target_member, pd.Timestamp(period_ts)))
    return True, targets, ""


def _validated_ledger_targets_match_package(
    package: Mapping[str, Any],
    ledger: Mapping[str, Any],
) -> Tuple[bool, Set[_SegmentTarget]]:
    """Prove every validated derivation and input equals its package economic cell."""

    package_metrics = dict(package.get("metrics") or {})
    facts_by_id = {str(row["record_id"]): dict(row) for row in ledger["source_facts"]}
    targets: Set[_SegmentTarget] = set()
    for row_in in ledger.get("derivations", ()):
        row = dict(row_in or {})
        value_row = dict(row.get("value") or {})
        period_ts = pd.to_datetime(row.get("period_end"), errors="coerce")
        if pd.isna(period_ts):
            return False, set()
        period = pd.Timestamp(period_ts)
        expected_cells: List[Tuple[str, str, Decimal]] = [
            (
                str(row.get("metric_label") or ""),
                str(row.get("target_member") or ""),
                Decimal(str(value_row.get("value"))) * Decimal("1000000"),
            )
        ]
        for record_id in row.get("input_record_ids", ()):
            fact = facts_by_id[str(record_id)]
            expected_cells.append(
                (
                    str(fact.get("metric_label") or ""),
                    str(fact.get("segment_member") or ""),
                    Decimal(str(dict(fact.get("value") or {}).get("value")))
                    * Decimal("1000000"),
                )
            )
        for metric_name, segment_name, expected_value in expected_cells:
            q_map = dict(dict(package_metrics.get(metric_name) or {}).get(segment_name) or {})
            matching_values = [
                value
                for q_key, value in q_map.items()
                if pd.notna(pd.to_datetime(q_key, errors="coerce"))
                and pd.Timestamp(pd.to_datetime(q_key)) == period
            ]
            if len(matching_values) != 1:
                return False, set()
            try:
                actual_value = Decimal(str(matching_values[0]))
            except (InvalidOperation, ValueError):
                return False, set()
            if actual_value != expected_value:
                return False, set()
        targets.add(
            (
                str(row.get("metric_label") or ""),
                str(row.get("target_member") or ""),
                period,
            )
        )
    return True, targets


def _segment_package_ledger_merge_validation(
    package: Mapping[str, Any],
) -> _SegmentPackageLedgerMergeValidation:
    """Classify absent, valid, enumerable-invalid, and unenumerable-invalid ledgers.

    The current package schema has no independent per-cell direct-fact owner outside
    this ledger.  A present-but-invalid ledger therefore rejects its package atomically,
    even when its target rows happen to be structurally enumerable.
    """

    if "segment_derivation_ledger" not in package:
        return _SegmentPackageLedgerMergeValidation(
            ledger_present=False,
            ledger_valid=False,
            target_enumeration_valid=True,
            declared_targets=frozenset(),
            validated_targets=frozenset(),
            invalid_reason="",
            package_merge_allowed=True,
            validated_ledger=None,
        )

    raw_ledger = package.get("segment_derivation_ledger")
    enumeration_valid, declared_targets, enumeration_reason = _enumerate_segment_ledger_targets(
        raw_ledger
    )
    try:
        ledger = validate_segment_residual_ledger_payload(raw_ledger)
    except SegmentNormalizationError as exc:
        return _SegmentPackageLedgerMergeValidation(
            ledger_present=True,
            ledger_valid=False,
            target_enumeration_valid=enumeration_valid,
            declared_targets=frozenset(declared_targets),
            validated_targets=frozenset(),
            invalid_reason=enumeration_reason or str(exc),
            package_merge_allowed=False,
            validated_ledger=None,
        )

    package_matches, validated_targets = _validated_ledger_targets_match_package(package, ledger)
    if not package_matches:
        return _SegmentPackageLedgerMergeValidation(
            ledger_present=True,
            ledger_valid=True,
            target_enumeration_valid=enumeration_valid,
            declared_targets=frozenset(declared_targets),
            validated_targets=frozenset(),
            invalid_reason="ledger_package_economics_mismatch",
            package_merge_allowed=False,
            validated_ledger=None,
        )
    return _SegmentPackageLedgerMergeValidation(
        ledger_present=True,
        ledger_valid=True,
        target_enumeration_valid=enumeration_valid,
        declared_targets=frozenset(declared_targets),
        validated_targets=frozenset(validated_targets),
        invalid_reason="",
        package_merge_allowed=True,
        validated_ledger=ledger,
    )


def _merge_quarterly_segment_packages_per_period(
    *,
    primary: Mapping[str, Any] | None,
    authoritative_overlay: Mapping[str, Any] | None,
    supplemental_overlay: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """Merge quarterly segment packages without requiring the overlay's latest period.

    Source roles, rather than caller/list order, define precedence: the primary
    package wins existing cells, the authoritative overlay fills primary gaps,
    and the supplemental overlay fills any remaining gaps.  A lagging overlay
    can therefore contribute a supported quarter while later unsupported
    quarters remain absent.
    """

    packages = (primary or {}, authoritative_overlay or {}, supplemental_overlay or {})
    ledger_decisions = tuple(_segment_package_ledger_merge_validation(package) for package in packages)
    validated_ledgers = [decision.validated_ledger for decision in ledger_decisions]
    lineaged_zero_targets = [set(decision.validated_targets) for decision in ledger_decisions]
    rejected_packages = [not decision.package_merge_allowed for decision in ledger_decisions]
    merged_metrics: Dict[str, Dict[str, Dict[pd.Timestamp, float]]] = {}
    source_docs: List[str] = []
    selected_owner: Dict[Tuple[str, str, pd.Timestamp], int] = {}

    for package_index, package in enumerate(packages):
        if rejected_packages[package_index]:
            continue
        package_metrics = dict(package.get("metrics") or {})
        if not package_metrics:
            continue
        for metric_name, seg_map in package_metrics.items():
            metric_bucket = merged_metrics.setdefault(str(metric_name), {})
            for seg_name, q_map in dict(seg_map or {}).items():
                seg_bucket = metric_bucket.setdefault(str(seg_name), {})
                normalized_rows: List[Tuple[pd.Timestamp, float]] = []
                for q_key, value_in in dict(q_map or {}).items():
                    q_ts = pd.to_datetime(q_key, errors="coerce")
                    value_num = pd.to_numeric(value_in, errors="coerce")
                    if pd.isna(q_ts) or pd.isna(value_num):
                        continue
                    normalized_rows.append((pd.Timestamp(q_ts), float(value_num)))
                for q_ts, value_num in sorted(normalized_rows, key=lambda item: item[0]):
                    target_key = (str(metric_name), str(seg_name), q_ts)
                    if q_ts not in seg_bucket:
                        seg_bucket[q_ts] = value_num
                        selected_owner[target_key] = package_index
                    elif target_key in lineaged_zero_targets[package_index]:
                        prior_owner = selected_owner.get(target_key)
                        prior_is_lineaged = (
                            prior_owner is not None
                            and target_key in lineaged_zero_targets[prior_owner]
                        )
                        existing_num = pd.to_numeric(seg_bucket.get(q_ts), errors="coerce")
                        if (
                            not prior_is_lineaged
                            and pd.notna(existing_num)
                            and float(existing_num) == 0.0
                            and value_num == 0.0
                        ):
                            # Preserve the normal source-role precedence for economic
                            # values, but never let an unlineaged zero suppress an
                            # otherwise identical, typed exact-zero derivation.
                            seg_bucket[q_ts] = value_num
                            selected_owner[target_key] = package_index
        for source_doc in str(package.get("source_doc") or "").split(" | "):
            source_doc = source_doc.strip()
            if source_doc and source_doc not in source_docs:
                source_docs.append(source_doc)

    merged_metrics = {
        metric_name: {
            segment_name: q_map
            for segment_name, q_map in segment_map.items()
            if q_map
        }
        for metric_name, segment_map in merged_metrics.items()
    }
    merged_metrics = {
        metric_name: segment_map
        for metric_name, segment_map in merged_metrics.items()
        if segment_map
    }
    quarters = sorted(
        {
            pd.Timestamp(q_ts).date()
            for seg_map in merged_metrics.values()
            for q_map in seg_map.values()
            for q_ts in q_map
        }
    )
    if not merged_metrics or not quarters:
        return {}
    result: Dict[str, Any] = {
        "metrics": merged_metrics,
        "quarters": quarters,
        "source_doc": " | ".join(source_docs),
        "source_qd": max(quarters),
    }
    selected_derivations: Dict[str, Dict[str, Any]] = {}
    selected_source_facts: Dict[str, Dict[str, Any]] = {}
    for package_index, package in enumerate(packages):
        ledger = validated_ledgers[package_index]
        if ledger is None:
            continue
        fact_rows = {
            str(row.get("record_id") or ""): dict(row)
            for row in ledger.get("source_facts", ())
            if str(row.get("record_id") or "").strip()
        }
        for row_in in ledger.get("derivations", ()):
            row = dict(row_in or {})
            period_ts = pd.to_datetime(row.get("period_end"), errors="coerce")
            target_key = (
                str(row.get("metric_label") or ""),
                str(row.get("target_member") or ""),
                pd.Timestamp(period_ts) if pd.notna(period_ts) else pd.NaT,
            )
            if (
                pd.isna(target_key[2])
                or target_key not in lineaged_zero_targets[package_index]
                or selected_owner.get(target_key) != package_index
            ):
                continue
            derivation_id = str(row.get("derivation_id") or "")
            if not derivation_id:
                continue
            selected_derivations[derivation_id] = row
            for record_id in row.get("input_record_ids", ()):
                record_id = str(record_id)
                if record_id in fact_rows:
                    selected_source_facts[record_id] = fact_rows[record_id]
    if selected_derivations:
        result["segment_derivation_ledger"] = {
            "contract_id": SEGMENT_RESIDUAL_LEDGER_CONTRACT_ID,
            "source_facts": [selected_source_facts[key] for key in sorted(selected_source_facts)],
            "derivations": [selected_derivations[key] for key in sorted(selected_derivations)],
        }
    return result


def _pbi_repair_total_reportable_segment_quarterly_totals_for_bs(
    quarterly_metrics: Dict[str, Any],
    *,
    source_facts: Sequence[SegmentResidualInputFact],
    derivations_out: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Reconcile PBI reportable-segment totals without converting missing to zero.

    PBI source extraction can occasionally capture the table's reportable
    segment revenue total under adjacent EBIT/D&A/EBITDA metric labels.  The
    component rows can repair an existing total only when the complete
    SendTech/Presort/Other component set is present.  A missing Other value is
    minted as an explicit zero only when an independently present total exactly
    equals the complete known component sum.  Missing totals are never built
    from components and then reused as circular evidence for a zero residual.
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

    repair_metrics = ("Revenue", "Adjusted EBIT", "Depreciation & amortization", "Adjusted EBITDA")
    fact_index = {
        (fact.metric_label, fact.segment_member, pd.Timestamp(fact.period_end)): fact
        for fact in source_facts
    }
    reconciled_total_periods: Dict[str, Set[pd.Timestamp]] = {}
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
            try:
                residual = Decimal(str(total_num)) - Decimal(str(send_num)) - Decimal(str(presort_num))
            except (InvalidOperation, ValueError):
                continue
            if residual == Decimal("0"):
                total_fact = fact_index.get((metric_name, "Total reportable segments", q_ts))
                send_fact = fact_index.get((metric_name, "SendTech Solutions", q_ts))
                presort_fact = fact_index.get((metric_name, "Presort Services", q_ts))
                if total_fact is None or send_fact is None or presort_fact is None:
                    continue
                try:
                    derivation = derive_exact_zero_segment_residual(
                        total=total_fact,
                        components=(send_fact, presort_fact),
                        target_member="Other operations",
                    )
                except SegmentNormalizationError:
                    continue
                if derivation is None:
                    continue
                derivation_row = derivation.to_dict()
                other_bucket[q_ts] = 0.0
                changed_other = True
                if all(
                    str(existing.get("derivation_id") or "") != derivation_row["derivation_id"]
                    for existing in derivations_out
                ):
                    derivations_out.append(derivation_row)
        if changed_other:
            seg_map["Other operations"] = other_bucket
            out[metric_name] = seg_map

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
        for q_key in sorted(total_bucket):
            q_ts = pd.Timestamp(q_key)
            component_values = [
                pd.to_numeric(send_bucket.get(q_ts), errors="coerce"),
                pd.to_numeric(presort_bucket.get(q_ts), errors="coerce"),
                pd.to_numeric(other_bucket.get(q_ts), errors="coerce"),
            ]
            if any(pd.isna(value) for value in component_values):
                continue
            try:
                total_decimal = Decimal(str(total_bucket[q_ts]))
                component_decimal = sum((Decimal(str(value)) for value in component_values), Decimal("0"))
            except (InvalidOperation, ValueError):
                continue
            if total_decimal == component_decimal:
                reconciled_total_periods.setdefault(metric_name, set()).add(q_ts)

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
                    if str(seg_name) == "Total reportable segments" and (
                        q_ts not in reconciled_total_periods.get("Revenue", set())
                        or q_ts not in reconciled_total_periods.get("Adjusted EBIT", set())
                    ):
                        continue
                    existing = pd.to_numeric(seg_bucket.get(q_ts), errors="coerce")
                    if pd.notna(existing):
                        continue
                    rev_num = pd.to_numeric(rev_series.get(q_ts), errors="coerce")
                    ebit_num = pd.to_numeric(ebit_val, errors="coerce")
                    if pd.notna(rev_num) and pd.notna(ebit_num) and abs(float(rev_num)) > 1e-9:
                        seg_bucket[q_ts] = float(ebit_num) / float(rev_num)
                        changed = True
                if not seg_bucket:
                    margin_map.pop(str(seg_name), None)
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
