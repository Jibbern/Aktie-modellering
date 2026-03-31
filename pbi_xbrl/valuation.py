"""Valuation-engine helpers and dataframe builders for valuation surfaces."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _safe_div(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den is None:
        return None
    if den == 0:
        return None
    return float(num) / float(den)


def _default_scenarios(target_multiple: float) -> Dict[str, Dict[str, float]]:
    return {
        "base": {
            "rev_growth": 0.00,
            "margin_delta": 0.00,
            "refi_norm_m": 0.0,
            "buyback_m": 0.0,
            "ev_multiple": target_multiple,
        },
        "bull": {
            "rev_growth": 0.02,
            "margin_delta": 0.01,
            "refi_norm_m": 15.0,
            "buyback_m": 2.0,
            "ev_multiple": target_multiple + 1.0,
        },
        "bear": {
            "rev_growth": -0.03,
            "margin_delta": -0.01,
            "refi_norm_m": -10.0,
            "buyback_m": -1.0,
            "ev_multiple": max(1.0, target_multiple - 1.0),
        },
    }


def _build_sensitivity_rows(
    *,
    target_multiple: float,
    net_debt_m: Optional[float],
    shares_diluted_m: Optional[float],
    ebitda_levels_m: Dict[str, Optional[float]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if net_debt_m is None or shares_diluted_m in (None, 0):
        return rows
    start = max(1.0, round(target_multiple - 2.0, 1))
    stop = round(target_multiple + 2.0, 1)
    mul = start
    while mul <= stop + 1e-9:
        row: Dict[str, Any] = {"multiple": round(mul, 2)}
        for name, ebitda_m in ebitda_levels_m.items():
            if ebitda_m is None:
                row[f"{name}_eq_share"] = None
                continue
            eq_m = (mul * ebitda_m) - net_debt_m
            row[f"{name}_eq_share"] = eq_m / shares_diluted_m if shares_diluted_m != 0 else None
        rows.append(row)
        mul += 0.5
    return rows


def valuation_engine(
    price: Optional[float],
    scenario_inputs: Dict[str, Any],
    hist_latest: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Source-of-truth valuation engine.
    Units:
      - money: USD millions
      - shares: millions
      - price: USD/share
    """

    sanity_flags: List[str] = []
    warns: List[str] = []
    fails: List[str] = []

    p = _to_float(price)
    shares_out_m = _to_float(hist_latest.get("shares_outstanding_m"))
    shares_dil_m = _to_float(hist_latest.get("shares_diluted_m"))
    if shares_dil_m is None:
        shares_dil_m = shares_out_m
    debt_core_m = _to_float(hist_latest.get("debt_core_m"))
    cash_m = _to_float(hist_latest.get("cash_m"))
    net_debt_m = _to_float(hist_latest.get("net_debt_m"))
    if net_debt_m is None and debt_core_m is not None and cash_m is not None:
        net_debt_m = debt_core_m - cash_m

    ebitda_ttm_m = _to_float(hist_latest.get("ebitda_ttm_m"))
    adj_ebitda_ttm_m = _to_float(hist_latest.get("adj_ebitda_ttm_m"))
    if adj_ebitda_ttm_m is None:
        adj_ebitda_ttm_m = ebitda_ttm_m
    fcf_ttm_m = _to_float(hist_latest.get("fcf_ttm_m"))
    int_paid_ttm_m = _to_float(hist_latest.get("interest_paid_ttm_m"))
    revenue_ttm_m = _to_float(hist_latest.get("revenue_ttm_m"))
    capex_ttm_m = _to_float(hist_latest.get("capex_ttm_m"))

    target_multiple = _to_float(scenario_inputs.get("target_ev_ebitda")) or 6.0
    target_yield = _to_float(scenario_inputs.get("target_ev_yield")) or 0.10
    maint_ratio = _to_float(scenario_inputs.get("maint_capex_ratio"))
    if maint_ratio is None:
        maint_ratio = 0.70
    recurring_costs_m = _to_float(scenario_inputs.get("recurring_cash_costs_m"))
    if recurring_costs_m is None:
        recurring_costs_m = 0.0
    wc_norm_m = _to_float(scenario_inputs.get("wc_normalization_m"))
    if wc_norm_m is None:
        wc_norm_m = 0.0

    if p is None or p <= 0:
        warns.append("WARN: price missing/<=0")
    if shares_out_m is None or shares_out_m <= 0:
        warns.append("WARN: shares_outstanding missing/<=0")
    if shares_dil_m is None or shares_dil_m <= 0:
        warns.append("WARN: shares_diluted missing/<=0")
    if net_debt_m is None:
        warns.append("WARN: net debt missing")

    market_cap_m = p * shares_out_m if (p is not None and shares_out_m is not None) else None
    implied_ev_m = market_cap_m + net_debt_m if (market_cap_m is not None and net_debt_m is not None) else None

    ev_tieout_diff_m = None
    if implied_ev_m is not None and market_cap_m is not None and debt_core_m is not None and cash_m is not None:
        ev_from_components = market_cap_m + debt_core_m - cash_m
        ev_tieout_diff_m = implied_ev_m - ev_from_components
        if abs(ev_tieout_diff_m) > 0.001:
            warns.append(f"WARN: EV tieout mismatch {ev_tieout_diff_m:.6f}m")
    else:
        warns.append("WARN: EV tieout incomplete (missing debt/cash)")

    implied_ev_ebitda = None
    if implied_ev_m is not None:
        if ebitda_ttm_m is None or ebitda_ttm_m <= 0:
            warns.append("WARN: EBITDA_TTM<=0; implied EV/EBITDA set NA")
        else:
            implied_ev_ebitda = implied_ev_m / ebitda_ttm_m

    fcff_proxy_ttm_m = None
    if fcf_ttm_m is not None and int_paid_ttm_m is not None:
        fcff_proxy_ttm_m = fcf_ttm_m + int_paid_ttm_m

    implied_fcf_yield = None
    if implied_ev_m is not None:
        if fcff_proxy_ttm_m is None or fcff_proxy_ttm_m <= 0:
            warns.append("WARN: FCF_TTM<=0 or FCFF proxy missing; implied FCF yield set NA")
        else:
            implied_fcf_yield = _safe_div(fcff_proxy_ttm_m, implied_ev_m)

    if implied_ev_m is not None and implied_ev_m < 0:
        fails.append("FAIL: EV < 0")

    curr_adj_margin = None
    if adj_ebitda_ttm_m is not None and revenue_ttm_m not in (None, 0):
        curr_adj_margin = adj_ebitda_ttm_m / revenue_ttm_m

    owner_earnings_ttm_m = None
    if fcf_ttm_m is not None and capex_ttm_m is not None:
        owner_earnings_ttm_m = (
            fcf_ttm_m
            + (1.0 - maint_ratio) * capex_ttm_m
            - recurring_costs_m
            + wc_norm_m
        )

    scenarios = scenario_inputs.get("scenarios") or _default_scenarios(target_multiple)
    scenario_out: Dict[str, Dict[str, Any]] = {}
    scenario_order = ["base", "bull", "bear"]
    scenario_used: Dict[str, Dict[str, float]] = {}
    for name in scenario_order:
        cfg = scenarios.get(name, {})
        rev_growth = _to_float(cfg.get("rev_growth")) or 0.0
        margin_delta = _to_float(cfg.get("margin_delta")) or 0.0
        refi_norm_m = _to_float(cfg.get("refi_norm_m")) or 0.0
        buyback_m = _to_float(cfg.get("buyback_m")) or 0.0
        ev_multiple = _to_float(cfg.get("ev_multiple")) or target_multiple
        scenario_used[name] = {
            "rev_growth": rev_growth,
            "margin_delta": margin_delta,
            "refi_norm_m": refi_norm_m,
            "buyback_m": buyback_m,
            "ev_multiple": ev_multiple,
        }

        adj_ebitda_scn_m = None
        if revenue_ttm_m is not None and curr_adj_margin is not None:
            scn_margin = curr_adj_margin + margin_delta
            adj_ebitda_scn_m = revenue_ttm_m * (1.0 + rev_growth) * scn_margin

        owner_earnings_scn_m = None
        if owner_earnings_ttm_m is not None and adj_ebitda_ttm_m is not None and adj_ebitda_scn_m is not None:
            owner_earnings_scn_m = owner_earnings_ttm_m + (adj_ebitda_scn_m - adj_ebitda_ttm_m) + refi_norm_m

        scenario_shares_m = None
        if shares_dil_m is not None:
            scenario_shares_m = max(0.001, shares_dil_m - buyback_m)

        target_price = None
        expected_return = None
        target_ev_m = None
        if adj_ebitda_scn_m is not None and net_debt_m is not None and scenario_shares_m not in (None, 0):
            target_ev_m = ev_multiple * adj_ebitda_scn_m
            target_equity_m = target_ev_m - net_debt_m
            target_price = target_equity_m / scenario_shares_m
            if p is not None and p > 0:
                expected_return = (target_price / p) - 1.0

        scenario_out[name] = {
            "ev_multiple": ev_multiple,
            "rev_growth": rev_growth,
            "margin_delta": margin_delta,
            "refi_norm_m": refi_norm_m,
            "buyback_m": buyback_m,
            "adj_ebitda_scn_m": adj_ebitda_scn_m,
            "owner_earnings_scn_m": owner_earnings_scn_m,
            "target_ev_m": target_ev_m,
            "target_price": target_price,
            "expected_return": expected_return,
        }

    if "bull" in scenario_used and "base" in scenario_used:
        if scenario_used["bull"]["ev_multiple"] < scenario_used["base"]["ev_multiple"]:
            warns.append("WARN: bull multiple below base multiple")
    if "bear" in scenario_used and "base" in scenario_used:
        if scenario_used["bear"]["ev_multiple"] > scenario_used["base"]["ev_multiple"]:
            warns.append("WARN: bear multiple above base multiple")

    sens_levels = {
        "bear": scenario_out.get("bear", {}).get("adj_ebitda_scn_m"),
        "base": scenario_out.get("base", {}).get("adj_ebitda_scn_m"),
        "bull": scenario_out.get("bull", {}).get("adj_ebitda_scn_m"),
    }
    sensitivity_rows = _build_sensitivity_rows(
        target_multiple=target_multiple,
        net_debt_m=net_debt_m,
        shares_diluted_m=shares_dil_m,
        ebitda_levels_m=sens_levels,
    )

    sanity_flags.extend(fails + warns)
    return {
        "as_of_quarter": hist_latest.get("quarter"),
        "inputs": {
            "price": p,
            "shares_outstanding_m": shares_out_m,
            "shares_diluted_m": shares_dil_m,
            "debt_core_m": debt_core_m,
            "cash_m": cash_m,
            "net_debt_m": net_debt_m,
            "ebitda_ttm_m": ebitda_ttm_m,
            "adj_ebitda_ttm_m": adj_ebitda_ttm_m,
            "fcf_ttm_m": fcf_ttm_m,
            "fcff_proxy_ttm_m": fcff_proxy_ttm_m,
            "revenue_ttm_m": revenue_ttm_m,
            "capex_ttm_m": capex_ttm_m,
            "owner_earnings_ttm_m": owner_earnings_ttm_m,
            "target_ev_ebitda": target_multiple,
            "target_ev_yield": target_yield,
        },
        "implied_ev": implied_ev_m,
        "implied_ev_ebitda": implied_ev_ebitda,
        "implied_fcf_yield": implied_fcf_yield,
        "ev_tieout_diff_m": ev_tieout_diff_m,
        "base_target_price": scenario_out.get("base", {}).get("target_price"),
        "base_expected_return": scenario_out.get("base", {}).get("expected_return"),
        "bull_target_price": scenario_out.get("bull", {}).get("target_price"),
        "bull_expected_return": scenario_out.get("bull", {}).get("expected_return"),
        "bear_target_price": scenario_out.get("bear", {}).get("target_price"),
        "bear_expected_return": scenario_out.get("bear", {}).get("expected_return"),
        "scenarios": scenario_out,
        "scenario_inputs_used": scenario_used,
        "sensitivity_rows": sensitivity_rows,
        "sanity_flags": sanity_flags,
        "warn_count": len(warns),
        "fail_count": len(fails),
    }


def valuation_to_frames(valuation_outputs: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not valuation_outputs:
        return pd.DataFrame(), pd.DataFrame()

    as_of = valuation_outputs.get("as_of_quarter")
    inp = valuation_outputs.get("inputs", {})
    rows = [
        {"metric": "as_of_quarter", "value": as_of, "unit": "", "section": "meta"},
        {"metric": "price", "value": inp.get("price"), "unit": "$/share", "section": "inputs"},
        {"metric": "shares_outstanding_m", "value": inp.get("shares_outstanding_m"), "unit": "m", "section": "inputs"},
        {"metric": "shares_diluted_m", "value": inp.get("shares_diluted_m"), "unit": "m", "section": "inputs"},
        {"metric": "net_debt_m", "value": inp.get("net_debt_m"), "unit": "$m", "section": "inputs"},
        {"metric": "ebitda_ttm_m", "value": inp.get("ebitda_ttm_m"), "unit": "$m", "section": "inputs"},
        {"metric": "adj_ebitda_ttm_m", "value": inp.get("adj_ebitda_ttm_m"), "unit": "$m", "section": "inputs"},
        {"metric": "fcf_ttm_m", "value": inp.get("fcf_ttm_m"), "unit": "$m", "section": "inputs"},
        {"metric": "fcff_proxy_ttm_m", "value": inp.get("fcff_proxy_ttm_m"), "unit": "$m", "section": "inputs"},
        {"metric": "implied_ev_m", "value": valuation_outputs.get("implied_ev"), "unit": "$m", "section": "implied"},
        {"metric": "implied_ev_ebitda", "value": valuation_outputs.get("implied_ev_ebitda"), "unit": "x", "section": "implied"},
        {"metric": "implied_fcf_yield", "value": valuation_outputs.get("implied_fcf_yield"), "unit": "%", "section": "implied"},
        {"metric": "ev_tieout_diff_m", "value": valuation_outputs.get("ev_tieout_diff_m"), "unit": "$m", "section": "qa"},
        {"metric": "base_target_price", "value": valuation_outputs.get("base_target_price"), "unit": "$/share", "section": "scenarios"},
        {"metric": "base_expected_return", "value": valuation_outputs.get("base_expected_return"), "unit": "%", "section": "scenarios"},
        {"metric": "bull_target_price", "value": valuation_outputs.get("bull_target_price"), "unit": "$/share", "section": "scenarios"},
        {"metric": "bull_expected_return", "value": valuation_outputs.get("bull_expected_return"), "unit": "%", "section": "scenarios"},
        {"metric": "bear_target_price", "value": valuation_outputs.get("bear_target_price"), "unit": "$/share", "section": "scenarios"},
        {"metric": "bear_expected_return", "value": valuation_outputs.get("bear_expected_return"), "unit": "%", "section": "scenarios"},
        {"metric": "warn_count", "value": valuation_outputs.get("warn_count"), "unit": "count", "section": "qa"},
        {"metric": "fail_count", "value": valuation_outputs.get("fail_count"), "unit": "count", "section": "qa"},
        {"metric": "sanity_flags", "value": " | ".join(valuation_outputs.get("sanity_flags", [])), "unit": "", "section": "qa"},
    ]
    summary_df = pd.DataFrame(rows)

    grid_rows = valuation_outputs.get("sensitivity_rows", [])
    grid_df = pd.DataFrame(grid_rows)
    if not grid_df.empty:
        grid_df = grid_df.rename(
            columns={
                "multiple": "ev_ebitda_multiple",
                "bear_eq_share": "bear_price",
                "base_eq_share": "base_price",
                "bull_eq_share": "bull_price",
            }
        )
    return summary_df, grid_df
