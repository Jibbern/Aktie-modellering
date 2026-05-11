from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd


DERIVATIVE_CRUSH_TEST_TABLES: Tuple[str, ...] = (
    "model_summary",
    "reconciliation",
    "quarterly_derivative_impact",
    "lead_lag_summary",
    "lead_lag_detail",
    "slippage",
    "exposure_buckets",
    "residual",
)


@dataclass(frozen=True)
class DerivativeCrushTestResult:
    model_summary: pd.DataFrame
    reconciliation: pd.DataFrame
    quarterly_derivative_impact: pd.DataFrame
    lead_lag_summary: pd.DataFrame
    lead_lag_detail: pd.DataFrame
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
    if gallons_m is not None and abs(float(gallons_m)) > 1e-12:
        crush_m = _num(driver_by_quarter.get(qd, {}).get("consolidated_ethanol_crush_margin"))
        if crush_m is not None:
            return float(crush_m) / float(gallons_m), "reported crush $m / ethanol gallons denominator"
    basis_value = _num(basis_by_quarter.get(qd, {}).get("reported_consolidated_crush_margin_usd_per_gal"))
    if basis_value is not None:
        return float(basis_value), "reported_consolidated_crush_margin_usd_per_gal fallback"
    return None, "reported margin not available"


def _component_residual_m(rec: Mapping[str, Any]) -> Optional[float]:
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
    derivative_by_quarter = _derivative_records_by_quarter(derivative_bridge_df)
    driver_by_quarter = _driver_records_by_quarter(operating_driver_history_rows)
    basis_by_quarter = _basis_records_by_quarter(quarterly_df)
    quarters = sorted(set(derivative_by_quarter) | set(driver_by_quarter) | set(basis_by_quarter))
    rows: List[Dict[str, Any]] = []
    for qd in quarters:
        gallons_m, denom_label, denom_source = _resolve_denominator_m(qd, driver_by_quarter, basis_by_quarter)
        reported_margin, reported_note = _reported_margin_per_gal(qd, gallons_m, driver_by_quarter, basis_by_quarter)
        derivative_rec = derivative_by_quarter.get(qd, {})
        basis_rec = basis_by_quarter.get(qd, {})
        row: Dict[str, Any] = {
            "Quarter": pd.Timestamp(qd),
            "_quarter_end": qd,
            "Quarter label": _quarter_label(qd),
            "Denominator": denom_label,
            "Gallons (m)": gallons_m,
            "Denominator source": denom_source,
            "Reported margin / gal": reported_margin,
            "Reported margin note": reported_note,
            "Approximate market crush / gal": _num(basis_rec.get("official_simple_proxy_usd_per_gal")),
            "GPRE crush proxy / gal": _num(basis_rec.get("gpre_proxy_official_usd_per_gal")),
            "quarterization_status": derivative_rec.get("quarterization_status"),
            "quarterization_note": derivative_rec.get("quarterization_note"),
        }
        row.update(_derivative_features(derivative_rec, gallons_m))
        rows.append(row)
    return rows


def _model_adjustment(row: Mapping[str, Any], variant: str) -> Optional[float]:
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


def _reconciliation_rows(base_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lens_specs = (
        ("Approximate market crush", "Approximate market crush / gal"),
        ("GPRE crush proxy", "GPRE crush proxy / gal"),
    )
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
    lens_specs = (
        ("Approximate market crush", "Approximate market crush / gal"),
        ("GPRE crush proxy", "GPRE crush proxy / gal"),
    )
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
    lens_specs = (
        ("Approximate market crush", "Approximate market crush / gal"),
        ("GPRE crush proxy", "GPRE crush proxy / gal"),
    )
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

    The output is presentation-only. Current-quarter model variants use only
    income-statement derivative P&L fields; OCI/AOCI and net derivative balances
    are kept to lead/lag and exposure diagnostics.
    """
    base_rows = _base_rows(derivative_bridge_df, operating_driver_history_rows, quarterly_df)
    reconciliation = _reconciliation_rows(base_rows)
    lead_lag_detail, lead_lag_summary = _lead_lag_rows(base_rows)
    return DerivativeCrushTestResult(
        model_summary=pd.DataFrame(_model_summary_rows_from_base(base_rows)),
        reconciliation=pd.DataFrame(reconciliation),
        quarterly_derivative_impact=pd.DataFrame(_quarterly_impact_rows(base_rows)),
        lead_lag_summary=pd.DataFrame(lead_lag_summary),
        lead_lag_detail=pd.DataFrame(lead_lag_detail),
        slippage=pd.DataFrame(_slippage_rows(base_rows, threshold=slippage_threshold)),
        exposure_buckets=pd.DataFrame(_exposure_bucket_rows(derivative_exposure_df)),
        residual=pd.DataFrame(_residual_rows(reconciliation, threshold=slippage_threshold)),
    )
