"""Valuation source-map and history helpers extracted from excel_writer_context."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


def _safe_float_or_none(value: Any) -> Optional[float]:
    try:
        coerced = pd.to_numeric(value, errors="coerce")
    except Exception:
        coerced = np.nan
    if pd.isna(coerced):
        return None
    try:
        return float(coerced)
    except Exception:
        return None


def normalize_capex_for_valuation(src: Dict[pd.Timestamp, Any]) -> Dict[pd.Timestamp, Any]:
    if not src:
        return src
    out: Dict[pd.Timestamp, Any] = {}
    has_negative = False
    for k, v in src.items():
        if v is None:
            out[k] = None
            continue
        try:
            if pd.isna(v):
                out[k] = None
                continue
        except Exception:
            pass
        fv = float(v)
        if fv < 0:
            has_negative = True
        out[k] = abs(fv)
    return out if has_negative else src


def quarter_key_union(all_quarters: Iterable[Any], *maps: Mapping[Any, Any]) -> List[pd.Timestamp]:
    keys = {pd.Timestamp(q).normalize() for q in all_quarters}
    for mp in maps:
        for raw_q in dict(mp or {}).keys():
            q_ts = pd.to_datetime(raw_q, errors="coerce")
            if pd.isna(q_ts):
                continue
            keys.add(pd.Timestamp(q_ts).to_period("Q").end_time.normalize())
    return sorted(keys)


def _margin_map(
    num: Dict[pd.Timestamp, Any],
    denom: Dict[pd.Timestamp, Any],
    *,
    all_quarters: Iterable[Any] = (),
) -> Dict[pd.Timestamp, Any]:
    out: Dict[pd.Timestamp, Any] = {}
    for q in quarter_key_union(all_quarters, num, denom):
        q = pd.Timestamp(q).normalize()
        n = _safe_float_or_none(num.get(q))
        d = _safe_float_or_none(denom.get(q))
        if n is None or d is None or d == 0:
            out[q] = None
        else:
            out[q] = n / d
    return out


def ttm_map(
    last4_quarters_map: Mapping[pd.Timestamp, Sequence[pd.Timestamp]],
    src: Dict[pd.Timestamp, Any],
    all_quarters: Iterable[Any] = (),
) -> Dict[pd.Timestamp, Any]:
    out: Dict[pd.Timestamp, Any] = {}
    for q in quarter_key_union(all_quarters, last4_quarters_map, src):
        q = pd.Timestamp(q).normalize()
        last4 = last4_quarters_map.get(pd.Timestamp(q))
        if not last4:
            continue
        vals = [_safe_float_or_none(src.get(qq)) for qq in last4]
        if any(v is None for v in vals):
            out[pd.Timestamp(q)] = None
        else:
            out[pd.Timestamp(q)] = float(sum(vals))
    return out


def ttm_sparse_cashflow_map(
    last4_quarters_map: Mapping[pd.Timestamp, Sequence[pd.Timestamp]],
    src: Dict[pd.Timestamp, Any],
    all_quarters: Iterable[Any] = (),
) -> Dict[pd.Timestamp, Any]:
    out: Dict[pd.Timestamp, Any] = {}
    for q in quarter_key_union(all_quarters, last4_quarters_map, src):
        q = pd.Timestamp(q).normalize()
        last4 = last4_quarters_map.get(pd.Timestamp(q))
        if not last4:
            continue
        vals: List[float] = []
        any_supported = False
        for qq in last4:
            raw_v = _safe_float_or_none(src.get(qq))
            if raw_v is None:
                vals.append(0.0)
            else:
                vals.append(float(raw_v))
                any_supported = True
        out[pd.Timestamp(q)] = float(sum(vals)) if any_supported else None
    return out


def display_m_source_map(src: Dict[pd.Timestamp, Any]) -> Dict[pd.Timestamp, Any]:
    out: Dict[pd.Timestamp, Any] = {}
    for raw_q, raw_v in dict(src or {}).items():
        num = _safe_float_or_none(raw_v)
        if num is None:
            continue
        out[pd.Timestamp(raw_q).normalize()] = float(num) / 1e6
    return out


def history_numeric_source_map(hist: Optional[pd.DataFrame], col: str) -> Dict[pd.Timestamp, Any]:
    if hist is None or hist.empty or "quarter" not in hist.columns or col not in hist.columns:
        return {}
    out: Dict[pd.Timestamp, Any] = {}
    for _, src_row in hist.iterrows():
        q_raw = pd.to_datetime(src_row.get("quarter"), errors="coerce")
        if pd.isna(q_raw):
            continue
        val = _safe_float_or_none(src_row.get(col))
        if val is None:
            continue
        out[pd.Timestamp(q_raw).normalize()] = float(val)
    return out


def history_margin_source_map(
    hist: Optional[pd.DataFrame],
    num_col: str,
    denom_col: str = "revenue",
    all_quarters: Iterable[Any] = (),
) -> Dict[pd.Timestamp, Any]:
    num_map = history_numeric_source_map(hist, num_col)
    denom_map = history_numeric_source_map(hist, denom_col)
    return _margin_map(num_map, denom_map, all_quarters=all_quarters) if num_map and denom_map else {}


def build_valuation_history_source_maps(
    hist: Optional[pd.DataFrame],
    all_quarters: Iterable[Any] = (),
) -> Dict[str, Dict[pd.Timestamp, Any]]:
    history_revenue_source_map = history_numeric_source_map(hist, "revenue")
    history_gross_margin_source_map = history_margin_source_map(hist, "gross_profit", all_quarters=all_quarters)
    history_ebitda_margin_source_map = history_margin_source_map(hist, "ebitda", all_quarters=all_quarters)
    history_ebit_margin_source_map = history_margin_source_map(hist, "op_income", all_quarters=all_quarters)
    history_net_income_margin_source_map = history_margin_source_map(hist, "net_income", all_quarters=all_quarters)
    history_capex_pct_source_map = history_margin_source_map(hist, "capex", all_quarters=all_quarters)
    history_cfo_source_map = history_numeric_source_map(hist, "cfo")
    history_capex_source_map = history_numeric_source_map(hist, "capex")

    history_fcf_source_map: Dict[pd.Timestamp, Any] = {}
    for q_key in quarter_key_union(all_quarters, history_cfo_source_map, history_capex_source_map):
        cfo_val = _safe_float_or_none(history_cfo_source_map.get(q_key))
        capex_val = _safe_float_or_none(history_capex_source_map.get(q_key))
        if cfo_val is None or capex_val is None:
            continue
        history_fcf_source_map[pd.Timestamp(q_key).normalize()] = float(cfo_val) - float(capex_val)
    history_fcf_margin_source_map = (
        _margin_map(history_fcf_source_map, history_revenue_source_map, all_quarters=all_quarters)
        if history_fcf_source_map
        else {}
    )

    history_owner_earnings_source_map: Dict[pd.Timestamp, Any] = {}
    for q_key in quarter_key_union(all_quarters, history_cfo_source_map, history_capex_source_map):
        cfo_val = _safe_float_or_none(history_cfo_source_map.get(q_key))
        capex_val = _safe_float_or_none(history_capex_source_map.get(q_key))
        if cfo_val is None or capex_val is None:
            continue
        history_owner_earnings_source_map[pd.Timestamp(q_key).normalize()] = float(cfo_val) - float(capex_val) * 0.70

    history_assets_current_source_map = history_numeric_source_map(hist, "assets_current")
    history_liabilities_current_source_map = history_numeric_source_map(hist, "liabilities_current")
    history_current_ratio_source_map = (
        _margin_map(history_assets_current_source_map, history_liabilities_current_source_map, all_quarters=all_quarters)
        if history_assets_current_source_map and history_liabilities_current_source_map
        else {}
    )
    history_eps_gaap_source_map = history_numeric_source_map(hist, "eps_diluted")
    history_net_income_source_map = history_numeric_source_map(hist, "net_income")
    history_share_denom_source_map = history_numeric_source_map(hist, "shares_diluted")
    if not history_share_denom_source_map:
        history_share_denom_source_map = history_numeric_source_map(hist, "shares_outstanding")
    for q_key in quarter_key_union(all_quarters, history_net_income_source_map, history_share_denom_source_map):
        q_norm = pd.Timestamp(q_key).normalize()
        if history_eps_gaap_source_map.get(q_norm) is not None:
            continue
        ni_val = _safe_float_or_none(history_net_income_source_map.get(q_key))
        shares_val = _safe_float_or_none(history_share_denom_source_map.get(q_key))
        if ni_val is None or shares_val in (None, 0):
            continue
        history_eps_gaap_source_map[q_norm] = float(ni_val) / float(shares_val)

    history_equity_source_map = history_numeric_source_map(hist, "total_equity")
    history_shares_source_map = history_numeric_source_map(hist, "shares_diluted")
    history_bv_share_source_map = (
        _margin_map(history_equity_source_map, history_shares_source_map, all_quarters=all_quarters)
        if history_equity_source_map and history_shares_source_map
        else {}
    )
    history_debt_core_source_map = history_numeric_source_map(hist, "debt_core")
    history_cash_source_map = history_numeric_source_map(hist, "cash")
    history_net_debt_source_map: Dict[pd.Timestamp, Any] = {}
    for q_key in quarter_key_union(all_quarters, history_debt_core_source_map, history_cash_source_map):
        debt_val = _safe_float_or_none(history_debt_core_source_map.get(q_key))
        cash_val = _safe_float_or_none(history_cash_source_map.get(q_key))
        if debt_val is None or cash_val is None:
            continue
        history_net_debt_source_map[pd.Timestamp(q_key).normalize()] = float(debt_val) - float(cash_val)

    return {
        "history_revenue_source_map": history_revenue_source_map,
        "history_gross_margin_source_map": history_gross_margin_source_map,
        "history_ebitda_margin_source_map": history_ebitda_margin_source_map,
        "history_ebit_margin_source_map": history_ebit_margin_source_map,
        "history_net_income_margin_source_map": history_net_income_margin_source_map,
        "history_capex_pct_source_map": history_capex_pct_source_map,
        "history_fcf_source_map": history_fcf_source_map,
        "history_cfo_source_map": history_cfo_source_map,
        "history_capex_source_map": history_capex_source_map,
        "history_fcf_margin_source_map": history_fcf_margin_source_map,
        "history_owner_earnings_source_map": history_owner_earnings_source_map,
        "history_assets_current_source_map": history_assets_current_source_map,
        "history_liabilities_current_source_map": history_liabilities_current_source_map,
        "history_current_ratio_source_map": history_current_ratio_source_map,
        "history_eps_gaap_source_map": history_eps_gaap_source_map,
        "history_net_income_source_map": history_net_income_source_map,
        "history_share_denom_source_map": history_share_denom_source_map,
        "history_equity_source_map": history_equity_source_map,
        "history_shares_source_map": history_shares_source_map,
        "history_bv_share_source_map": history_bv_share_source_map,
        "history_debt_core_source_map": history_debt_core_source_map,
        "history_cash_source_map": history_cash_source_map,
        "history_net_debt_source_map": history_net_debt_source_map,
    }


def valuation_hidden_comparison_metric(
    source_map: Mapping[pd.Timestamp, Any],
    *,
    current_q: pd.Timestamp,
    current_value: Any,
    visible_idx: int,
    comparison_basis: str,
    directionality: str,
) -> Optional[float]:
    if directionality not in {"higher_better", "lower_better"}:
        return None
    if comparison_basis == "direct_delta":
        return None
    step = 1 if comparison_basis == "qoq" else 4
    if visible_idx >= step:
        return None
    current_num = pd.to_numeric(current_value, errors="coerce")
    if pd.isna(current_num):
        return None
    source_map = dict(source_map or {})
    if not source_map:
        return None
    try:
        prev_period = pd.Timestamp(current_q).to_period("Q") - step
        previous_q = prev_period.end_time.normalize()
    except Exception:
        return None

    def _ordered_previous_value_local() -> Optional[float]:
        current_norm = pd.Timestamp(current_q).normalize()
        source_keys = sorted(source_map)
        try:
            current_pos = source_keys.index(current_norm)
            matched_current = current_norm
        except ValueError:
            nearest = sorted(
                (
                    (abs((current_norm - source_key).days), idx_key, source_key)
                    for idx_key, source_key in enumerate(source_keys)
                    if abs((current_norm - source_key).days) <= 45
                ),
                key=lambda item: item[0],
            )
            if not nearest:
                return None
            _, current_pos, matched_current = nearest[0]
        if current_pos < step:
            return None
        candidate_key = source_keys[current_pos - step]
        days_delta = abs((matched_current - candidate_key).days)
        if step == 1:
            if not 45 <= days_delta <= 125:
                return None
        else:
            if not 330 <= days_delta <= 400:
                return None
        candidate = pd.to_numeric(source_map.get(candidate_key), errors="coerce")
        if pd.isna(candidate):
            return None
        return float(candidate)

    previous = pd.to_numeric(source_map.get(previous_q), errors="coerce")
    if pd.isna(previous):
        previous = pd.to_numeric(_ordered_previous_value_local(), errors="coerce")
    if pd.isna(previous) or abs(float(previous)) <= 1e-12:
        return None
    metric = (float(current_num) - float(previous)) / abs(float(previous))
    if directionality == "lower_better":
        metric *= -1.0
    return metric
