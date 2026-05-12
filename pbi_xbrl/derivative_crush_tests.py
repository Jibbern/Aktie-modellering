"""Diagnostic GPRE derivative-to-crush test builder.

This module is deliberately downstream of source extraction. It accepts the
Derivative_OCI_Bridge rows, open hedge exposure rows, operating-driver history,
and the GPRE basis model quarterly frame, then returns table-shaped data for the
Derivative_Crush_Tests workbook sheet.

The core accounting boundary is important: current-quarter model tests use only
income-statement derivative P&L fields. OCI, AOCI, and net derivative balances
are deferred/exposure signals and are only used in lead/lag diagnostics.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import numpy as np
import pandas as pd


# Keep this order aligned with the custom workbook writer. These names are the
# public contract returned by DerivativeCrushTestResult.as_dict().
DERIVATIVE_CRUSH_TEST_TABLES: Tuple[str, ...] = (
    "model_summary",
    "ex_derivative_margin_test",
    "clean_margin_bridge",
    "target_specific_model_accuracy",
    "reconciliation",
    "quarterly_derivative_impact",
    "coefficient_diagnostic",
    "lagged_derivative_pnl_tests",
    "lead_lag_summary",
    "lead_lag_detail",
    "residual_driver_screen",
    "slippage",
    "exposure_buckets",
    "residual",
)


@dataclass(frozen=True)
class DerivativeCrushTestResult:
    """Named diagnostic tables consumed by the Derivative_Crush_Tests writer."""

    model_summary: pd.DataFrame
    ex_derivative_margin_test: pd.DataFrame
    clean_margin_bridge: pd.DataFrame
    target_specific_model_accuracy: pd.DataFrame
    reconciliation: pd.DataFrame
    quarterly_derivative_impact: pd.DataFrame
    coefficient_diagnostic: pd.DataFrame
    lagged_derivative_pnl_tests: pd.DataFrame
    lead_lag_summary: pd.DataFrame
    lead_lag_detail: pd.DataFrame
    residual_driver_screen: pd.DataFrame
    slippage: pd.DataFrame
    exposure_buckets: pd.DataFrame
    residual: pd.DataFrame

    def as_dict(self) -> Dict[str, pd.DataFrame]:
        return {name: getattr(self, name) for name in DERIVATIVE_CRUSH_TEST_TABLES}


def _to_quarter_end(value: Any) -> Optional[date]:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    try:
        return pd.Timestamp(ts).to_period("Q").end_time.date()
    except Exception:
        return pd.Timestamp(ts).date()


def _quarter_label(value: Any) -> str:
    qd = _to_quarter_end(value)
    if qd is None:
        return ""
    period = pd.Timestamp(qd).to_period("Q")
    return f"{int(period.year)}-Q{int(period.quarter)}"


def _num(value: Any) -> Optional[float]:
    out = pd.to_numeric(value, errors="coerce")
    if pd.isna(out):
        return None
    return float(out)


def _usd_to_m(value: Any) -> Optional[float]:
    num = _num(value)
    if num is None:
        return None
    return num / 1_000_000.0


def _per_gal(amount_m: Optional[float], gallons_m: Optional[float]) -> Optional[float]:
    if amount_m is None or gallons_m is None or abs(float(gallons_m)) < 1e-12:
        return None
    return float(amount_m) / float(gallons_m)


def _none_if_nan(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def _rmse(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(math.sqrt(sum(v * v for v in values) / len(values)))


def _median_abs(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(pd.Series([abs(v) for v in values]).median())


def _safe_abs(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return abs(float(value))


def _correlation(xs: List[float], ys: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if len(xs) < 3 or len(ys) < 3 or len(xs) != len(ys):
        return None, None, None
    x = pd.Series(xs, dtype="float64")
    y = pd.Series(ys, dtype="float64")
    x_var = float(x.var(ddof=0))
    if abs(x_var) < 1e-12:
        return None, None, None
    corr = float(x.corr(y))
    if math.isnan(corr):
        return None, None, None
    slope = float(((x - x.mean()) * (y - y.mean())).mean() / x_var)
    return corr, slope, corr * corr


def _first_numeric(mapping: Mapping[str, Any], keys: Iterable[str]) -> Optional[float]:
    for key in keys:
        value = _num(mapping.get(key))
        if value is not None:
            return float(value)
    return None


def _bridge_component_from_keys(
    driver_rec: Mapping[str, float],
    basis_rec: Mapping[str, Any],
    keys: Iterable[str],
) -> Optional[float]:
    driver_value = _first_numeric(driver_rec, keys)
    if driver_value is not None:
        return driver_value
    return _first_numeric(basis_rec, keys)


def _bridge_component_difference(
    reported_m: Optional[float],
    driver_rec: Mapping[str, float],
    basis_rec: Mapping[str, Any],
    explicit_keys: Iterable[str],
    ex_keys: Iterable[str],
) -> Optional[float]:
    explicit_value = _bridge_component_from_keys(driver_rec, basis_rec, explicit_keys)
    if explicit_value is not None:
        return explicit_value
    ex_value = _bridge_component_from_keys(driver_rec, basis_rec, ex_keys)
    if reported_m is not None and ex_value is not None:
        return float(reported_m) - float(ex_value)
    return None


def _driver_records_by_quarter(
    operating_driver_history_rows: Optional[Iterable[Mapping[str, Any]]],
) -> Dict[date, Dict[str, float]]:
    by_quarter: Dict[date, Dict[str, float]] = {}
    for rec in operating_driver_history_rows or []:
        key = str(rec.get("_driver_key") or "").strip()
        qd = _to_quarter_end(rec.get("Quarter"))
        val = _num(rec.get("Value"))
        if not key or qd is None or val is None:
            continue
        by_quarter.setdefault(qd, {})[key] = float(val)
    return by_quarter


def _basis_records_by_quarter(quarterly_df: Optional[pd.DataFrame]) -> Dict[date, Dict[str, Any]]:
    if not isinstance(quarterly_df, pd.DataFrame) or quarterly_df.empty:
        return {}
    out: Dict[date, Dict[str, Any]] = {}
    for _, rec in quarterly_df.iterrows():
        qd = _to_quarter_end(rec.get("quarter"))
        if qd is None:
            continue
        out[qd] = dict(rec)
    return out


def _derivative_records_by_quarter(bridge_df: Optional[pd.DataFrame]) -> Dict[date, Dict[str, Any]]:
    if not isinstance(bridge_df, pd.DataFrame) or bridge_df.empty:
        return {}
    out: Dict[date, Dict[str, Any]] = {}
    for _, rec in bridge_df.iterrows():
        qd = _to_quarter_end(rec.get("quarter"))
        if qd is None:
            continue
        out[qd] = dict(rec)
    return out


def _resolve_denominator_m(
    qd: date,
    driver_by_quarter: Mapping[date, Mapping[str, float]],
    basis_by_quarter: Mapping[date, Mapping[str, Any]],
) -> Tuple[Optional[float], str, str]:
    # Match Derivative_OCI_Bridge diagnostics so $/gal readbacks reconcile across
    # the accounting source sheet and the crush-testing sheet.
    driver_rec = driver_by_quarter.get(qd, {})
    for key, label in (
        ("ethanol_gallons_produced", "Ethanol gallons produced"),
        ("ethanol_gallons_sold", "Ethanol gallons sold"),
    ):
        value = _num(driver_rec.get(key))
        if value is not None and abs(value) > 1e-12:
            return float(value), label, "operating drivers"
    basis_rec = basis_by_quarter.get(qd, {})
    for key, label in (
        ("reported_ethanol_gallons_produced_raw", "Ethanol gallons produced"),
        ("reported_ethanol_gallons_sold_raw", "Ethanol gallons sold"),
    ):
        value = _num(basis_rec.get(key))
        if value is not None and abs(value) > 1e-12:
            return float(value) / 1_000_000.0, label, "basis quarterly_df fallback"
    return None, "denominator not available", "denominator not available"


def _reported_margin_per_gal(
    qd: date,
    gallons_m: Optional[float],
    driver_by_quarter: Mapping[date, Mapping[str, float]],
    basis_by_quarter: Mapping[date, Mapping[str, Any]],
) -> Tuple[Optional[float], str]:
    # Prefer reported dollars divided by the same gallon denominator used for
    # derivative P&L. The basis-model fallback is only for quarters where the
    # already-computed reported per-gallon field is all that exists.
    if gallons_m is not None and abs(float(gallons_m)) > 1e-12:
        crush_m = _num(driver_by_quarter.get(qd, {}).get("consolidated_ethanol_crush_margin"))
        if crush_m is not None:
            return float(crush_m) / float(gallons_m), "reported crush $m / ethanol gallons denominator"
    basis_value = _num(basis_by_quarter.get(qd, {}).get("reported_consolidated_crush_margin_usd_per_gal"))
    if basis_value is not None:
        return float(basis_value), "reported_consolidated_crush_margin_usd_per_gal fallback"
    return None, "reported margin not available"


def _component_residual_m(rec: Mapping[str, Any]) -> Optional[float]:
    """Return any disclosed-total-vs-component gap in $m, zeroing noise."""

    total = _usd_to_m(rec.get("derivative_gain_loss_pnl_total_usd"))
    if total is None:
        return None
    components = [
        _usd_to_m(rec.get("non_designated_derivative_pnl_total_usd")),
        _usd_to_m(rec.get("cash_flow_hedge_reclass_total_usd")),
        _usd_to_m(rec.get("fair_value_hedge_total_pnl_usd")),
    ]
    available = [float(v) for v in components if v is not None]
    if not available:
        return None
    residual = float(total) - sum(available)
    if abs(residual) < 0.0005:
        residual = 0.0
    return float(residual)


def _derivative_features(
    rec: Mapping[str, Any],
    gallons_m: Optional[float],
) -> Dict[str, Optional[float]]:
    field_to_name = {
        "derivative_gain_loss_pnl_total_usd": "Total derivative P&L",
        "derivative_gain_loss_revenue_usd": "Derivative P&L in revenue",
        "derivative_gain_loss_cogs_usd": "Derivative P&L in COGS",
        "cash_flow_hedge_reclass_total_usd": "Cash-flow hedge reclass to P&L",
        "fair_value_hedge_total_pnl_usd": "Fair-value hedge P&L",
        "non_designated_derivative_pnl_total_usd": "Non-designated derivative P&L",
        "derivative_oci_current_period_usd": "Derivative OCI movement",
        "derivative_aoci_ending_balance_usd": "Derivative AOCI",
        "derivative_net_asset_liability_usd": "Net derivative asset/liability",
    }
    out: Dict[str, Optional[float]] = {}
    for field, name in field_to_name.items():
        amount_m = _usd_to_m(rec.get(field))
        out[f"{name} ($m)"] = amount_m
        out[f"{name} / gal"] = _per_gal(amount_m, gallons_m)
    residual_m = _component_residual_m(rec)
    out["P&L component residual / unallocated ($m)"] = residual_m
    out["P&L component residual / unallocated / gal"] = _per_gal(residual_m, gallons_m)
    return out


def _base_rows(
    derivative_bridge_df: Optional[pd.DataFrame],
    operating_driver_history_rows: Optional[Iterable[Mapping[str, Any]]],
    quarterly_df: Optional[pd.DataFrame],
) -> List[Dict[str, Any]]:
    """Join derivative, operating-driver, and market-proxy records by quarter."""

    derivative_by_quarter = _derivative_records_by_quarter(derivative_bridge_df)
    driver_by_quarter = _driver_records_by_quarter(operating_driver_history_rows)
    basis_by_quarter = _basis_records_by_quarter(quarterly_df)
    quarters = sorted(set(derivative_by_quarter) | set(driver_by_quarter) | set(basis_by_quarter))
    rows: List[Dict[str, Any]] = []
    for qd in quarters:
        gallons_m, denom_label, denom_source = _resolve_denominator_m(qd, driver_by_quarter, basis_by_quarter)
        reported_margin, reported_note = _reported_margin_per_gal(qd, gallons_m, driver_by_quarter, basis_by_quarter)
        derivative_rec = derivative_by_quarter.get(qd, {})
        driver_rec = driver_by_quarter.get(qd, {})
        basis_rec = basis_by_quarter.get(qd, {})
        reported_crush_m = _num(driver_rec.get("consolidated_ethanol_crush_margin"))
        if reported_crush_m is None and reported_margin is not None and gallons_m is not None:
            reported_crush_m = float(reported_margin) * float(gallons_m)
        impact_45z_m = _bridge_component_difference(
            reported_crush_m,
            driver_rec,
            basis_rec,
            (
                "45z_impact",
                "45z_impact_usd_m",
                "45z_cogs_impact",
                "45z_cogs_impact_usd_m",
                "ethanol_production_45z_cogs_reduction",
            ),
            ("crush_margin_ex_45z", "underlying_crush_margin", "reported_crush_ex_45z"),
        )
        if impact_45z_m is None and reported_margin is not None and gallons_m is not None:
            underlying_per_gal = _first_numeric(
                basis_rec,
                ("underlying_crush_margin_usd_per_gal", "crush_margin_ex_45z_usd_per_gal"),
            )
            if underlying_per_gal is not None:
                impact_45z_m = (float(reported_margin) - float(underlying_per_gal)) * float(gallons_m)
        rin_impact_m = _bridge_component_difference(
            reported_crush_m,
            driver_rec,
            basis_rec,
            ("rin_impact", "rin_sale", "rin_impact_usd_m", "rin_sale_usd_m"),
            ("crush_margin_ex_rin", "reported_crush_ex_rin"),
        )
        if rin_impact_m is None and reported_margin is not None and gallons_m is not None:
            ex_rin_per_gal = _first_numeric(
                basis_rec,
                ("crush_margin_ex_rin_usd_per_gal", "reported_crush_ex_rin_usd_per_gal"),
            )
            if ex_rin_per_gal is not None:
                rin_impact_m = (float(reported_margin) - float(ex_rin_per_gal)) * float(gallons_m)
        inventory_nrv_m = _bridge_component_from_keys(
            driver_rec,
            basis_rec,
            ("inventory_nrv", "inventory_lcnrv", "inventory_nrv_usd_m", "inventory_lcnrv_usd_m"),
        )
        non_ethanol_m = _bridge_component_from_keys(
            driver_rec,
            basis_rec,
            (
                "non_ethanol_operating_activities",
                "non_ethanol_operating_activities_usd_m",
                "intercompany_nonethanol_net",
                "intercompany_nonethanol_net_usd_m",
            ),
        )
        impairment_m = _bridge_component_from_keys(
            driver_rec,
            basis_rec,
            (
                "impairment_assets_held_for_sale",
                "impairment_held_for_sale",
                "impairment_assets_held_for_sale_usd_m",
            ),
        )
        other_explicit_m = _bridge_component_from_keys(
            driver_rec,
            basis_rec,
            ("other_bridge_items", "other_explicit_items", "other_bridge_items_usd_m"),
        )
        utilization = _bridge_component_from_keys(
            driver_rec,
            basis_rec,
            ("plant_utilization", "operating_utilization", "utilization", "utilization_pct"),
        )
        corn_basis = _first_numeric(
            basis_rec,
            (
                "weighted_basis_recommended_usd_per_bu",
                "weighted_basis_official_usd_per_bu",
                "official_corn_basis_usd_per_bu",
                "corn_basis_usd_per_bu",
            ),
        )
        natural_gas = _first_numeric(
            basis_rec,
            ("natural_gas_price_usd_per_mmbtu", "natural_gas_usd_per_mmbtu", "gas_price_usd_per_mmbtu"),
        )
        coproduct_credit = _first_numeric(
            basis_rec,
            (
                "coproduct_approximate_credit_usd_per_gal",
                "approximate_coproduct_credit_per_gal",
                "coproduct_credit_usd_per_gal",
            ),
        )
        row: Dict[str, Any] = {
            "Quarter": pd.Timestamp(qd),
            "_quarter_end": qd,
            "Quarter label": _quarter_label(qd),
            "Denominator": denom_label,
            "Gallons (m)": gallons_m,
            "Denominator source": denom_source,
            "Reported consolidated crush margin ($m)": reported_crush_m,
            "Reported margin / gal": reported_margin,
            "Reported margin note": reported_note,
            "Approximate market crush / gal": _num(basis_rec.get("official_simple_proxy_usd_per_gal")),
            "GPRE crush proxy / gal": _num(basis_rec.get("gpre_proxy_official_usd_per_gal")),
            "Best forward lens / gal": _num(basis_rec.get("best_forward_lens_proxy_usd_per_gal")),
            "45Z impact ($m)": impact_45z_m,
            "45Z impact / gal": _per_gal(impact_45z_m, gallons_m),
            "RIN impact ($m)": rin_impact_m,
            "RIN impact / gal": _per_gal(rin_impact_m, gallons_m),
            "Inventory NRV / lower-of-cost ($m)": inventory_nrv_m,
            "Inventory NRV / gal": _per_gal(inventory_nrv_m, gallons_m),
            "Non-ethanol operating activities ($m)": non_ethanol_m,
            "Non-ethanol operating activities / gal": _per_gal(non_ethanol_m, gallons_m),
            "Impairment / held-for-sale ($m)": impairment_m,
            "Impairment / held-for-sale / gal": _per_gal(impairment_m, gallons_m),
            "Other explicit items ($m)": other_explicit_m,
            "Other explicit items / gal": _per_gal(other_explicit_m, gallons_m),
            "Utilization": utilization,
            "Corn basis proxy": corn_basis,
            "Natural gas proxy": natural_gas,
            "Coproduct value proxy / gal": coproduct_credit,
            "Q4 quarterization flag": 1.0 if (qd.month == 12 or str(derivative_rec.get("quarterization_status") or "").lower().startswith("annual_minus")) else 0.0,
            "quarterization_status": derivative_rec.get("quarterization_status"),
            "quarterization_note": derivative_rec.get("quarterization_note"),
        }
        row.update(_derivative_features(derivative_rec, gallons_m))
        deriv = _num(row.get("Total derivative P&L / gal"))
        row["Reported margin ex derivative / gal"] = (
            None if reported_margin is None or deriv is None else float(reported_margin) - float(deriv)
        )
        clean_items = [
            row.get("Total derivative P&L / gal"),
            row.get("45Z impact / gal"),
            row.get("RIN impact / gal"),
            row.get("Inventory NRV / gal"),
            row.get("Non-ethanol operating activities / gal"),
            row.get("Impairment / held-for-sale / gal"),
            row.get("Other explicit items / gal"),
        ]
        available_clean_items = [float(v) for v in clean_items if _num(v) is not None]
        row["Clean margin / gal"] = (
            None if reported_margin is None else float(reported_margin) - sum(available_clean_items)
        )
        missing_clean_labels = [
            label
            for label, value in (
                ("derivative P&L", row.get("Total derivative P&L / gal")),
                ("45Z", row.get("45Z impact / gal")),
                ("RIN", row.get("RIN impact / gal")),
                ("inventory NRV", row.get("Inventory NRV / gal")),
                ("non-ethanol", row.get("Non-ethanol operating activities / gal")),
                ("impairment", row.get("Impairment / held-for-sale / gal")),
                ("other explicit", row.get("Other explicit items / gal")),
            )
            if _num(value) is None
        ]
        row["Clean margin note"] = (
            "diagnostic clean margin; missing explicit items: " + ", ".join(missing_clean_labels)
            if missing_clean_labels
            else "diagnostic clean margin subtracts all available explicit bridge items"
        )
        rows.append(row)
    return rows


def _model_adjustment(row: Mapping[str, Any], variant: str) -> Optional[float]:
    # The variants are additive diagnostics, not fitted regressions. That keeps
    # the sheet explainable and prevents a tiny sample from becoming production
    # model logic by accident.
    if variant == "Model A: baseline only":
        return 0.0
    if variant == "Model B: baseline + total derivative P&L":
        return _num(row.get("Total derivative P&L / gal"))
    if variant == "Model C: baseline + revenue/COGS derivative split":
        rev = _num(row.get("Derivative P&L in revenue / gal"))
        cogs = _num(row.get("Derivative P&L in COGS / gal"))
        if rev is None and cogs is None:
            return None
        return float(rev or 0.0) + float(cogs or 0.0)
    if variant == "Model D: baseline + component split":
        parts = [
            _num(row.get("Non-designated derivative P&L / gal")),
            _num(row.get("Cash-flow hedge reclass to P&L / gal")),
            _num(row.get("Fair-value hedge P&L / gal")),
        ]
        if all(part is None for part in parts):
            return None
        return float(sum(part or 0.0 for part in parts))
    return None


def _available_lens_specs(base_rows: List[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
    specs: List[Tuple[str, str]] = [
        ("Approximate market crush", "Approximate market crush / gal"),
        ("GPRE crush proxy", "GPRE crush proxy / gal"),
    ]
    if any(_num(row.get("Best forward lens / gal")) is not None for row in base_rows):
        specs.append(("Best forward lens", "Best forward lens / gal"))
    return tuple(specs)


def _reconciliation_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lens_specs = _available_lens_specs(base_rows)
    rows: List[Dict[str, Any]] = []
    for row in base_rows:
        reported = _num(row.get("Reported margin / gal"))
        deriv = _num(row.get("Total derivative P&L / gal"))
        for lens, baseline_field in lens_specs:
            baseline = _num(row.get(baseline_field))
            if reported is None or baseline is None:
                note = "missing reported or baseline margin"
                adjusted = baseline_error = adjusted_error = improvement = None
            elif deriv is None:
                note = "missing derivative P&L"
                adjusted = baseline
                baseline_error = reported - baseline
                adjusted_error = None
                improvement = None
            else:
                note = str(row.get("Reported margin note") or "").strip()
                adjusted = baseline + deriv
                baseline_error = reported - baseline
                adjusted_error = reported - adjusted
                improvement = abs(baseline_error) - abs(adjusted_error)
            rows.append(
                {
                    "Baseline lens": lens,
                    "Quarter": row.get("Quarter"),
                    "Reported margin / gal": reported,
                    "Market/proxy crush margin / gal": baseline,
                    "Total derivative P&L / gal": deriv,
                    "Derivative-adjusted proxy margin / gal": adjusted,
                    "Baseline error / gal": baseline_error,
                    "Derivative-adjusted error / gal": adjusted_error,
                    "Error improvement / gal": improvement,
                    "Notes / flags": _quality_note(row, note),
                }
            )
    return rows


def _quality_note(row: Mapping[str, Any], base_note: str = "") -> str:
    notes = [base_note] if base_note else []
    q_status = str(row.get("quarterization_status") or "").strip()
    if q_status and q_status.lower() not in {"reported", "direct", "standalone"}:
        notes.append(f"derivative quarterization: {q_status}")
    if row.get("P&L component residual / unallocated ($m)") is None:
        notes.append("component disclosure incomplete")
    if row.get("Gallons (m)") is None:
        notes.append("denominator not available")
    return "; ".join(dict.fromkeys(notes))


def _directional_hit_rate(records: List[Tuple[pd.Timestamp, float, float]]) -> Optional[float]:
    if len(records) < 3:
        return None
    records = sorted(records, key=lambda item: item[0])
    hits = 0
    total = 0
    for idx in range(1, len(records)):
        actual_delta = records[idx][1] - records[idx - 1][1]
        pred_delta = records[idx][2] - records[idx - 1][2]
        if abs(actual_delta) < 1e-12 or abs(pred_delta) < 1e-12:
            continue
        total += 1
        if (actual_delta > 0) == (pred_delta > 0):
            hits += 1
    if total == 0:
        return None
    return float(hits / total)


def _model_summary_rows(reconciliation_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    variants = (
        ("Model A: baseline only", "Reported margin / gal ~= baseline margin / gal"),
        ("Model B: baseline + total derivative P&L", "Reported margin / gal ~= baseline + total derivative P&L / gal"),
        ("Model C: baseline + revenue/COGS derivative split", "Reported margin / gal ~= baseline + revenue derivative / gal + COGS derivative / gal"),
        ("Model D: baseline + component split", "Reported margin / gal ~= baseline + non-designated + cash-flow reclass + fair-value hedge / gal"),
    )
    by_lens: Dict[str, List[Dict[str, Any]]] = {}
    for rec in reconciliation_rows:
        by_lens.setdefault(str(rec.get("Baseline lens") or ""), []).append(rec)
    out: List[Dict[str, Any]] = []
    for lens, lens_rows in by_lens.items():
        base_mae: Optional[float] = None
        for variant, formula in variants:
            errors: List[float] = []
            directional_records: List[Tuple[pd.Timestamp, float, float]] = []
            for rec in lens_rows:
                reported = _num(rec.get("Reported margin / gal"))
                baseline = _num(rec.get("Market/proxy crush margin / gal"))
                if reported is None or baseline is None:
                    continue
                if variant == "Model A: baseline only":
                    prediction = baseline
                elif variant == "Model B: baseline + total derivative P&L":
                    adj = _num(rec.get("Total derivative P&L / gal"))
                    if adj is None:
                        continue
                    prediction = baseline + adj
                else:
                    # Recompute from detail is not available in reconciliation, so
                    # Model C/D are populated by build_derivative_crush_tests below.
                    continue
                error = reported - prediction
                errors.append(error)
                directional_records.append((pd.Timestamp(rec.get("Quarter")), reported, prediction))
            mae = _mean([abs(err) for err in errors])
            if variant == "Model A: baseline only":
                base_mae = mae
            improvement = None if mae is None or base_mae is None else float(base_mae - mae)
            out.append(
                {
                    "Baseline lens": lens,
                    "Model variant": variant,
                    "Formula": formula,
                    "Valid quarters": len(errors),
                    "MAE": mae,
                    "RMSE": _rmse(errors),
                    "Median absolute error": _median_abs(errors),
                    "Bias / avg error": _mean(errors),
                    "Directional hit rate": _directional_hit_rate(directional_records),
                    "Improvement vs Model A": improvement,
                    "Interpretation": _model_interpretation(len(errors), improvement),
                }
            )
    return out


def _model_summary_rows_from_base(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lens_specs = _available_lens_specs(base_rows)
    variants = (
        ("Model A: baseline only", "Reported margin / gal ~= baseline margin / gal"),
        ("Model B: baseline + total derivative P&L", "Reported margin / gal ~= baseline + total derivative P&L / gal"),
        ("Model C: baseline + revenue/COGS derivative split", "Reported margin / gal ~= baseline + revenue derivative / gal + COGS derivative / gal"),
        ("Model D: baseline + component split", "Reported margin / gal ~= baseline + non-designated + cash-flow reclass + fair-value hedge / gal"),
    )
    out: List[Dict[str, Any]] = []
    for lens, baseline_field in lens_specs:
        lens_metrics: Dict[str, Optional[float]] = {}
        for variant, formula in variants:
            errors: List[float] = []
            directional_records: List[Tuple[pd.Timestamp, float, float]] = []
            for row in base_rows:
                reported = _num(row.get("Reported margin / gal"))
                baseline = _num(row.get(baseline_field))
                adjustment = _model_adjustment(row, variant)
                if reported is None or baseline is None or adjustment is None:
                    continue
                prediction = baseline + adjustment
                errors.append(reported - prediction)
                directional_records.append((pd.Timestamp(row.get("Quarter")), reported, prediction))
            mae = _mean([abs(err) for err in errors])
            if variant == "Model A: baseline only":
                lens_metrics["base_mae"] = mae
            base_mae = lens_metrics.get("base_mae")
            improvement = None if mae is None or base_mae is None else float(base_mae - mae)
            out.append(
                {
                    "Baseline lens": lens,
                    "Model variant": variant,
                    "Formula": formula,
                    "Valid quarters": len(errors),
                    "MAE": mae,
                    "RMSE": _rmse(errors),
                    "Median absolute error": _median_abs(errors),
                    "Bias / avg error": _mean(errors),
                    "Directional hit rate": _directional_hit_rate(directional_records),
                    "Improvement vs Model A": improvement,
                    "Interpretation": _model_interpretation(len(errors), improvement),
                }
            )
    return out


def _model_interpretation(valid_count: int, improvement: Optional[float]) -> str:
    if valid_count < 3:
        return "diagnostic only; small sample"
    if improvement is None:
        return "diagnostic only; incomplete comparison"
    if improvement > 0:
        return "derivative P&L improved fit versus baseline"
    if improvement < 0:
        return "derivative P&L worsened fit versus baseline"
    return "no change versus baseline"


def _prediction_stats(records: List[Tuple[pd.Timestamp, float, float]]) -> Dict[str, Any]:
    errors = [float(actual) - float(pred) for _, actual, pred in records]
    return {
        "Valid quarters": len(records),
        "MAE": _mean([abs(err) for err in errors]),
        "RMSE": _rmse(errors),
        "Median absolute error": _median_abs(errors),
        "Bias / avg error": _mean(errors),
        "Directional hit rate": _directional_hit_rate(records),
    }


def _ex_derivative_margin_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in base_rows:
        reported = _num(row.get("Reported margin / gal"))
        deriv = _num(row.get("Total derivative P&L / gal"))
        ex_deriv = _num(row.get("Reported margin ex derivative / gal"))
        for lens, baseline_field in _available_lens_specs(base_rows):
            baseline = _num(row.get(baseline_field))
            err_reported = None if reported is None or baseline is None else float(reported) - float(baseline)
            err_ex = None if ex_deriv is None or baseline is None else float(ex_deriv) - float(baseline)
            improvement = None if err_reported is None or err_ex is None else abs(err_reported) - abs(err_ex)
            rows.append(
                {
                    "Baseline lens": lens,
                    "Quarter": row.get("Quarter"),
                    "Reported margin / gal": reported,
                    "Total derivative P&L / gal": deriv,
                    "Reported margin ex derivative / gal": ex_deriv,
                    "Market/proxy crush margin / gal": baseline,
                    "Error vs reported margin": err_reported,
                    "Error vs ex-derivative margin": err_ex,
                    "Improvement when targeting ex-derivative margin": improvement,
                    "Notes / flags": _quality_note(row, "physical-margin diagnostic; not reported earnings model"),
                }
            )
    return rows


def _clean_margin_bridge_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in base_rows:
        clean = _num(row.get("Clean margin / gal"))
        gpre_proxy = _num(row.get("GPRE crush proxy / gal"))
        approx_proxy = _num(row.get("Approximate market crush / gal"))
        residual_basis = gpre_proxy if gpre_proxy is not None else approx_proxy
        rows.append(
            {
                "Quarter": row.get("Quarter"),
                "Reported consolidated crush margin ($m)": row.get("Reported consolidated crush margin ($m)"),
                "Ethanol gallons produced (m)": row.get("Gallons (m)"),
                "Reported margin / gal": row.get("Reported margin / gal"),
                "Total derivative P&L / gal": row.get("Total derivative P&L / gal"),
                "45Z impact / gal": row.get("45Z impact / gal"),
                "RIN impact / gal": row.get("RIN impact / gal"),
                "Inventory NRV / gal": row.get("Inventory NRV / gal"),
                "Non-ethanol operating activities / gal": row.get("Non-ethanol operating activities / gal"),
                "Other explicit items / gal": row.get("Other explicit items / gal"),
                "Clean margin / gal": clean,
                "Market/proxy crush margin / gal": approx_proxy,
                "GPRE crush proxy / gal": gpre_proxy,
                "Clean-margin residual / gal": None if clean is None or residual_basis is None else clean - residual_basis,
                "Notes / flags": _quality_note(row, str(row.get("Clean margin note") or "")),
            }
        )
    return rows


def _target_specific_model_accuracy_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    target_specs = (
        ("Reported margin / gal", "Reported margin / gal"),
        ("Reported margin ex derivative / gal", "Reported margin ex derivative / gal"),
        ("Clean margin / gal", "Clean margin / gal"),
    )
    rows: List[Dict[str, Any]] = []
    for lens, baseline_field in _available_lens_specs(base_rows):
        reported_mae: Optional[float] = None
        for target_label, target_field in target_specs:
            records: List[Tuple[pd.Timestamp, float, float]] = []
            for row in base_rows:
                target = _num(row.get(target_field))
                baseline = _num(row.get(baseline_field))
                if target is None or baseline is None:
                    continue
                records.append((pd.Timestamp(row.get("Quarter")), float(target), float(baseline)))
            stats = _prediction_stats(records)
            if target_label == "Reported margin / gal":
                reported_mae = stats["MAE"]
            improvement = None if stats["MAE"] is None or reported_mae is None else float(reported_mae) - float(stats["MAE"])
            rows.append(
                {
                    "Baseline lens": lens,
                    "Target": target_label,
                    **stats,
                    "Improvement vs reported-target MAE": improvement,
                    "Interpretation": (
                        "diagnostic only; small sample"
                        if int(stats["Valid quarters"] or 0) < 3
                        else "positive improvement means this target fits the lens better than reported margin"
                    ),
                }
            )
    return rows


def _fit_ols(y_values: List[float], x_rows: List[List[float]]) -> Optional[Dict[str, Any]]:
    if len(y_values) < 3 or len(y_values) != len(x_rows):
        return None
    x = np.asarray(x_rows, dtype=float)
    y = np.asarray(y_values, dtype=float)
    if x.ndim != 2 or x.shape[0] <= x.shape[1]:
        return None
    design = np.column_stack([np.ones(x.shape[0]), x])
    try:
        coeffs, *_ = np.linalg.lstsq(design, y, rcond=None)
    except Exception:
        return None
    pred = design @ coeffs
    errors = y - pred
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    ss_res = float(np.sum(errors ** 2))
    r2 = None if abs(ss_tot) < 1e-12 else float(1.0 - (ss_res / ss_tot))
    loo_errors: List[float] = []
    if len(y_values) > x.shape[1] + 2:
        for idx in range(len(y_values)):
            train_mask = np.ones(len(y_values), dtype=bool)
            train_mask[idx] = False
            train_x = design[train_mask]
            train_y = y[train_mask]
            if train_x.shape[0] <= x.shape[1]:
                continue
            try:
                train_coeffs, *_ = np.linalg.lstsq(train_x, train_y, rcond=None)
            except Exception:
                continue
            loo_pred = float(design[idx] @ train_coeffs)
            loo_errors.append(float(y[idx] - loo_pred))
    return {
        "coefficients": [float(c) for c in coeffs],
        "R^2": r2,
        "MAE": _mean([abs(float(err)) for err in errors.tolist()]),
        "RMSE": _rmse([float(err) for err in errors.tolist()]),
        "Leave-one-out MAE": _mean([abs(err) for err in loo_errors]) if loo_errors else None,
    }


def _coefficient_diagnostic_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    model_specs = (
        ("Model 1: reported = alpha + beta * proxy", "Reported margin / gal", ("proxy",)),
        ("Model 2: reported = alpha + beta * proxy + gamma * derivative P&L", "Reported margin / gal", ("proxy", "derivative")),
        ("Model 3: ex-derivative = alpha + beta * proxy", "Reported margin ex derivative / gal", ("proxy",)),
    )
    for lens, baseline_field in _available_lens_specs(base_rows):
        for model_label, target_field, features in model_specs:
            y_values: List[float] = []
            x_rows: List[List[float]] = []
            for row in base_rows:
                target = _num(row.get(target_field))
                proxy = _num(row.get(baseline_field))
                deriv = _num(row.get("Total derivative P&L / gal"))
                if target is None or proxy is None:
                    continue
                feature_row = [float(proxy)]
                if "derivative" in features:
                    if deriv is None:
                        continue
                    feature_row.append(float(deriv))
                y_values.append(float(target))
                x_rows.append(feature_row)
            fit = _fit_ols(y_values, x_rows)
            coeffs = list((fit or {}).get("coefficients") or [])
            gamma = coeffs[2] if len(coeffs) > 2 else None
            if fit is None:
                interpretation = "insufficient sample"
            elif gamma is None:
                interpretation = "diagnostic proxy coefficient; do not promote automatically"
            elif abs(gamma - 1.0) <= 0.25:
                interpretation = "derivative P&L behaves like a missing reported-margin adjustment"
            elif abs(gamma) <= 0.25:
                interpretation = "derivative P&L adds little incremental signal in this sample"
            elif gamma < 0:
                interpretation = "possible timing, double-count, sign or target mismatch"
            else:
                interpretation = "diagnostic relationship; do not promote automatically"
            rows.append(
                {
                    "Baseline lens": lens,
                    "Regression model": model_label,
                    "Valid quarters": len(y_values),
                    "Alpha": coeffs[0] if len(coeffs) > 0 else None,
                    "Beta on proxy": coeffs[1] if len(coeffs) > 1 else None,
                    "Gamma on derivative P&L": gamma,
                    "R^2": (fit or {}).get("R^2"),
                    "MAE": (fit or {}).get("MAE"),
                    "RMSE": (fit or {}).get("RMSE"),
                    "Leave-one-out MAE": (fit or {}).get("Leave-one-out MAE"),
                    "Interpretation": interpretation,
                }
            )
    return rows


def _lagged_derivative_pnl_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    sorted_rows = sorted(base_rows, key=lambda row: pd.Timestamp(row.get("Quarter")))
    derivatives = [_num(row.get("Total derivative P&L / gal")) for row in sorted_rows]
    variant_values: Dict[str, List[Optional[float]]] = {
        "No derivative baseline": [0.0 for _ in sorted_rows],
        "Current quarter derivative P&L": derivatives,
        "Prior quarter derivative P&L": [None] + derivatives[:-1],
        "Rolling 2Q derivative P&L avg": [
            _mean([float(v) for v in derivatives[max(0, idx - 1): idx + 1] if v is not None])
            for idx in range(len(sorted_rows))
        ],
        "Rolling 4Q derivative P&L avg": [
            _mean([float(v) for v in derivatives[max(0, idx - 3): idx + 1] if v is not None])
            if idx >= 3
            else None
            for idx in range(len(sorted_rows))
        ],
    }
    for lens, baseline_field in _available_lens_specs(base_rows):
        base_records: List[Tuple[pd.Timestamp, float, float]] = []
        for row in sorted_rows:
            reported = _num(row.get("Reported margin / gal"))
            baseline = _num(row.get(baseline_field))
            if reported is None or baseline is None:
                continue
            base_records.append((pd.Timestamp(row.get("Quarter")), float(reported), float(baseline)))
        base_mae = _prediction_stats(base_records)["MAE"]
        for variant, values in variant_values.items():
            records: List[Tuple[pd.Timestamp, float, float]] = []
            for row, deriv_value in zip(sorted_rows, values):
                reported = _num(row.get("Reported margin / gal"))
                baseline = _num(row.get(baseline_field))
                if reported is None or baseline is None or deriv_value is None:
                    continue
                records.append((pd.Timestamp(row.get("Quarter")), float(reported), float(baseline) + float(deriv_value)))
            stats = _prediction_stats(records)
            improvement = None if stats["MAE"] is None or base_mae is None else float(base_mae) - float(stats["MAE"])
            rows.append(
                {
                    "Baseline lens": lens,
                    "Derivative timing variant": variant,
                    "Valid quarters": stats["Valid quarters"],
                    "MAE": stats["MAE"],
                    "RMSE": stats["RMSE"],
                    "Improvement vs no-derivative baseline": improvement,
                    "Notes": "rolling variants use per-gallon average; diagnostic timing test only",
                }
            )
    return rows


def _residual_driver_screen_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    driver_specs = (
        ("45Z impact / gal", "45Z impact / gal"),
        ("RIN impact / gal", "RIN impact / gal"),
        ("Inventory NRV / gal", "Inventory NRV / gal"),
        ("Utilization", "Utilization"),
        ("Ethanol gallons produced (m)", "Gallons (m)"),
        ("Corn basis proxy", "Corn basis proxy"),
        ("Natural gas proxy", "Natural gas proxy"),
        ("Coproduct value proxy / gal", "Coproduct value proxy / gal"),
        ("Non-ethanol operating activities / gal", "Non-ethanol operating activities / gal"),
        ("Q4 quarterization flag", "Q4 quarterization flag"),
    )
    rows: List[Dict[str, Any]] = []
    for lens, baseline_field in _available_lens_specs(base_rows):
        residuals_by_row: List[Tuple[Dict[str, Any], Optional[float]]] = []
        for row in base_rows:
            reported = _num(row.get("Reported margin / gal"))
            baseline = _num(row.get(baseline_field))
            deriv = _num(row.get("Total derivative P&L / gal"))
            residual = None if reported is None or baseline is None or deriv is None else float(reported) - float(baseline) - float(deriv)
            residuals_by_row.append((row, residual))
        for driver_label, driver_field in driver_specs:
            xs: List[float] = []
            ys: List[float] = []
            for row, residual in residuals_by_row:
                driver_value = _num(row.get(driver_field))
                if residual is None or driver_value is None:
                    continue
                xs.append(float(driver_value))
                ys.append(float(residual))
            corr, slope, r2 = _correlation(xs, ys)
            rows.append(
                {
                    "Baseline lens": lens,
                    "Driver": driver_label,
                    "Valid observations": len(xs),
                    "Correlation": corr,
                    "R^2": r2,
                    "Simple slope": slope,
                    "Interpretation": "insufficient sample" if len(xs) < 3 else "residual diagnostic only; do not use as production model",
                }
            )
    return rows


def _quarterly_impact_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    metrics = (
        ("Total derivative P&L / gal", "Total derivative P&L / gal"),
        ("Derivative P&L in revenue / gal", "Derivative P&L in revenue / gal"),
        ("Derivative P&L in COGS / gal", "Derivative P&L in COGS / gal"),
        ("Cash-flow hedge reclass / gal", "Cash-flow hedge reclass to P&L / gal"),
        ("Fair-value hedge P&L / gal", "Fair-value hedge P&L / gal"),
        ("Non-designated derivative P&L / gal", "Non-designated derivative P&L / gal"),
        ("P&L component residual / unallocated / gal", "P&L component residual / unallocated / gal"),
        ("Ethanol gallons produced (m)", "Gallons (m)"),
    )
    quarter_labels = [str(row.get("Quarter label") or "") for row in base_rows if row.get("Quarter label")]
    out: List[Dict[str, Any]] = []
    for display, field in metrics:
        rec: Dict[str, Any] = {"Metric": display}
        for row in base_rows:
            label = str(row.get("Quarter label") or "")
            if not label:
                continue
            rec[label] = row.get(field)
        for label in quarter_labels:
            rec.setdefault(label, None)
        out.append(rec)
    return out


def _lead_lag_rows(base_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows_sorted = sorted(base_rows, key=lambda row: pd.Timestamp(row.get("Quarter")))
    lead_specs = (
        ("Derivative AOCI / gal", "Derivative AOCI / gal"),
        ("Derivative OCI movement / gal", "Derivative OCI movement / gal"),
        ("Net derivative asset/liability / gal", "Net derivative asset/liability / gal"),
    )
    outcomes = (
        ("Total derivative P&L / gal t+1", "Total derivative P&L / gal", 1),
        ("Total derivative P&L / gal t+2", "Total derivative P&L / gal", 2),
        ("Cash-flow hedge reclass / gal t+1", "Cash-flow hedge reclass to P&L / gal", 1),
        ("Cash-flow hedge reclass / gal t+2", "Cash-flow hedge reclass to P&L / gal", 2),
    )
    detail: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows_sorted):
        for lead_label, lead_field in lead_specs:
            lead_value = _num(row.get(lead_field))
            for outcome_label, outcome_field, offset in outcomes:
                future_idx = idx + offset
                future_value = _num(rows_sorted[future_idx].get(outcome_field)) if future_idx < len(rows_sorted) else None
                detail.append(
                    {
                        "Quarter": row.get("Quarter"),
                        "Lead variable": lead_label,
                        "Lead value / gal": lead_value,
                        "Future outcome": outcome_label,
                        "Future value / gal": future_value,
                        "Lag quarters": offset,
                        "Interpretation": "lead/lag diagnostic only; not current-quarter P&L",
                    }
                )
            next4: List[float] = []
            for future_idx in range(idx + 1, min(idx + 5, len(rows_sorted))):
                value = _num(rows_sorted[future_idx].get("Cash-flow hedge reclass to P&L / gal"))
                if value is not None:
                    next4.append(value)
            detail.append(
                {
                    "Quarter": row.get("Quarter"),
                    "Lead variable": lead_label,
                    "Lead value / gal": lead_value,
                    "Future outcome": "Cumulative next 4Q cash-flow hedge reclass / gal",
                    "Future value / gal": sum(next4) if len(next4) == 4 else None,
                    "Lag quarters": "next 4Q",
                    "Interpretation": "insufficient sample" if len(next4) < 4 else "lead/lag diagnostic only; not current-quarter P&L",
                }
            )
    summary: List[Dict[str, Any]] = []
    for lead_label, _ in lead_specs:
        for outcome_label, _, _ in outcomes + (("Cumulative next 4Q cash-flow hedge reclass / gal", "", 0),):
            xs: List[float] = []
            ys: List[float] = []
            for rec in detail:
                if rec.get("Lead variable") != lead_label or rec.get("Future outcome") != outcome_label:
                    continue
                x = _num(rec.get("Lead value / gal"))
                y = _num(rec.get("Future value / gal"))
                if x is not None and y is not None:
                    xs.append(x)
                    ys.append(y)
            corr, slope, r2 = _correlation(xs, ys)
            summary.append(
                {
                    "Lead variable": lead_label,
                    "Future outcome": outcome_label,
                    "Valid observations": len(xs),
                    "Correlation": corr,
                    "Simple slope": slope,
                    "R^2": r2,
                    "Interpretation": "insufficient sample" if len(xs) < 3 else "diagnostic relationship; do not use as current-quarter P&L",
                }
            )
    return detail, summary


def _slippage_rows(base_rows: List[Dict[str, Any]], *, threshold: float) -> List[Dict[str, Any]]:
    lens_specs = _available_lens_specs(base_rows)
    rows: List[Dict[str, Any]] = []
    sorted_rows = sorted(base_rows, key=lambda row: pd.Timestamp(row.get("Quarter")))
    for lens, field in lens_specs:
        prev_value: Optional[float] = None
        for row in sorted_rows:
            baseline = _num(row.get(field))
            change = None if baseline is None or prev_value is None else baseline - prev_value
            deriv = _num(row.get("Total derivative P&L / gal"))
            if change is None or deriv is None:
                flag = "insufficient data"
            elif change > threshold and deriv < -threshold:
                flag = "Potential upside capped by hedges"
            elif change < -threshold and deriv > threshold:
                flag = "Hedges cushioned downside"
            else:
                flag = "No clear hedge slippage signal"
            rows.append(
                {
                    "Baseline lens": lens,
                    "Quarter": row.get("Quarter"),
                    "Change in market/proxy crush margin / gal": change,
                    "Total derivative P&L / gal": deriv,
                    "Revenue derivative P&L / gal": row.get("Derivative P&L in revenue / gal"),
                    "COGS derivative P&L / gal": row.get("Derivative P&L in COGS / gal"),
                    "Reported margin / gal": row.get("Reported margin / gal"),
                    "Slippage flag": flag,
                    "Interpretation": "risk-management diagnostic; negative P&L may reflect timing or hedging, not poor execution",
                }
            )
            if baseline is not None:
                prev_value = baseline
    return rows


def _margin_bucket_for_commodity(commodity: Any) -> str:
    low = str(commodity or "").strip().lower()
    if "natural gas" in low:
        return "Production energy input"
    if "corn oil" in low or "renewable corn oil" in low:
        return "Coproduct output"
    if "distiller" in low:
        return "Coproduct output"
    if low == "corn" or "corn" in low:
        return "Core crush input"
    if "ethanol" in low:
        return "Core crush output"
    return "Other / not classified"


def _exposure_bucket_rows(exposure_df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if not isinstance(exposure_df, pd.DataFrame) or exposure_df.empty:
        return []
    rows: List[Dict[str, Any]] = []
    for _, rec in exposure_df.iterrows():
        commodity = rec.get("Commodity")
        bucket = _margin_bucket_for_commodity(commodity)
        rows.append(
            {
                "Quarter": rec.get("Quarter"),
                "Commodity": commodity,
                "Instrument": rec.get("Instrument"),
                "Accounting bucket": rec.get("Accounting bucket"),
                "Direction": rec.get("Direction"),
                "Net notional": rec.get("Net notional"),
                "Unit": rec.get("Unit"),
                "Scale": rec.get("Scale"),
                "Margin bucket": bucket,
                "Likely P&L line": rec.get("Likely P&L line"),
                "Coverage ratio": "not available",
                "Interpretation": "coverage ratio not available; physical denominators by commodity are not consistently scale-compatible",
            }
        )
    return rows


def _residual_rows(reconciliation_rows: List[Dict[str, Any]], *, threshold: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for rec in reconciliation_rows:
        residual = _num(rec.get("Derivative-adjusted error / gal"))
        if residual is None:
            explanation = "missing reported, baseline or derivative P&L"
        elif residual > threshold:
            explanation = "Reported margin exceeded proxy after derivative adjustment; possible basis/coproduct/operations/timing effect"
        elif residual < -threshold:
            explanation = "Reported margin below proxy after derivative adjustment; possible basis/coproduct/operations/timing effect"
        else:
            explanation = "Residual within materiality threshold"
        rows.append(
            {
                "Baseline lens": rec.get("Baseline lens"),
                "Quarter": rec.get("Quarter"),
                "Reported margin / gal": rec.get("Reported margin / gal"),
                "Market/proxy crush margin / gal": rec.get("Market/proxy crush margin / gal"),
                "Total derivative P&L / gal": rec.get("Total derivative P&L / gal"),
                "Residual after derivative adjustment / gal": residual,
                "Possible explanation": explanation,
                "Data quality flag": rec.get("Notes / flags"),
            }
        )
    return rows


def build_derivative_crush_tests(
    derivative_bridge_df: Optional[pd.DataFrame],
    derivative_exposure_df: Optional[pd.DataFrame],
    operating_driver_history_rows: Optional[Iterable[Mapping[str, Any]]],
    quarterly_df: Optional[pd.DataFrame],
    *,
    slippage_threshold: float = 0.03,
) -> DerivativeCrushTestResult:
    """Build diagnostic tables that test derivative P&L against GPRE crush margins.

    Inputs are normalized dataframes/lists from earlier pipeline and writer
    steps. The output is presentation-only. Current-quarter model variants use
    only income-statement derivative P&L fields; OCI/AOCI and net derivative
    balances are kept to lead/lag and exposure diagnostics.
    """
    base_rows = _base_rows(derivative_bridge_df, operating_driver_history_rows, quarterly_df)
    reconciliation = _reconciliation_rows(base_rows)
    lead_lag_detail, lead_lag_summary = _lead_lag_rows(base_rows)
    return DerivativeCrushTestResult(
        model_summary=pd.DataFrame(_model_summary_rows_from_base(base_rows)),
        ex_derivative_margin_test=pd.DataFrame(_ex_derivative_margin_rows(base_rows)),
        clean_margin_bridge=pd.DataFrame(_clean_margin_bridge_rows(base_rows)),
        target_specific_model_accuracy=pd.DataFrame(_target_specific_model_accuracy_rows(base_rows)),
        reconciliation=pd.DataFrame(reconciliation),
        quarterly_derivative_impact=pd.DataFrame(_quarterly_impact_rows(base_rows)),
        coefficient_diagnostic=pd.DataFrame(_coefficient_diagnostic_rows(base_rows)),
        lagged_derivative_pnl_tests=pd.DataFrame(_lagged_derivative_pnl_rows(base_rows)),
        lead_lag_summary=pd.DataFrame(lead_lag_summary),
        lead_lag_detail=pd.DataFrame(lead_lag_detail),
        residual_driver_screen=pd.DataFrame(_residual_driver_screen_rows(base_rows)),
        slippage=pd.DataFrame(_slippage_rows(base_rows, threshold=slippage_threshold)),
        exposure_buckets=pd.DataFrame(_exposure_bucket_rows(derivative_exposure_df)),
        residual=pd.DataFrame(_residual_rows(reconciliation, threshold=slippage_threshold)),
    )
