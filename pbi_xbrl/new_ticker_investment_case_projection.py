"""Ticker-neutral Investment Case product inputs and support-row projection."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from pbi_xbrl.new_ticker_guidance_scope import build_valuation_guidance_projection
from pbi_xbrl.segment_normalization import (
    canonical_segment_dimension_member,
    canonical_segment_display_member,
)


class InvestmentCaseProjectionError(ValueError):
    """Fail-closed error for an ambiguous Investment Case product identity."""


def _canonical_digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if number == number and abs(number) != float("inf") else None


def _field_value(field: Any) -> Any:
    if isinstance(field, Mapping):
        return field.get("value")
    return field


def _field_number(field: Any) -> float | None:
    if not isinstance(field, Mapping) or str(field.get("status") or "") != "populated":
        return None
    return _finite_number(field.get("value"))


def _field_source(field: Any) -> str:
    return str(field.get("source_ref") or "") if isinstance(field, Mapping) else ""


def _field_period(field: Any) -> str:
    return str(field.get("period") or "") if isinstance(field, Mapping) else ""


def _joined(values: Sequence[str]) -> str:
    return " | ".join(dict.fromkeys(value for value in values if value))


def _source_alias(source_ref: str) -> str:
    aliases: list[str] = []
    for raw in str(source_ref or "").split(" | "):
        token = raw.strip()
        if not token:
            continue
        transcript = re.search(
            r"(?:^|[/\\])[^/\\]*[_-]Q([1-4])[_-](\d{4})[_-]transcript(?:\.[^/#\\]+)?",
            token,
            flags=re.IGNORECASE,
        )
        if transcript:
            aliases.append(f"Q{transcript.group(1)} {transcript.group(2)} earnings call")
            continue
        release = re.search(
            r"(?:^|[/\\])(?:8-K_)?(\d{4}-\d{2}-\d{2})_earnings_release",
            token,
            flags=re.IGNORECASE,
        )
        if release:
            aliases.append(f"Earnings release {release.group(1)}")
            continue
        if token.startswith("normalized-package:"):
            aliases.append("Normalized package")
            continue
        if "ANF_model.xlsx!" in token or "_model.xlsx!" in token:
            aliases.append("Legacy workbook oracle")
            continue
        name = re.split(r"[#!]", token, maxsplit=1)[0].replace("\\", "/").rsplit("/", 1)[-1]
        if name:
            aliases.append(name)
    unique = tuple(dict.fromkeys(aliases))
    if not unique:
        return ""
    if len(unique) == 1:
        return unique[0]
    return "Multiple source records"


def _quarter_ordinal(period: str) -> int:
    match = re.fullmatch(r"(\d{4})-Q([1-4])", str(period or ""))
    if not match:
        raise InvestmentCaseProjectionError(f"Invalid fiscal-quarter identity {period!r}.")
    return int(match.group(1)) * 4 + int(match.group(2))


def _annual_ordinal(period: str) -> int:
    match = re.fullmatch(r"(\d{4})-FY", str(period or ""))
    if not match:
        raise InvestmentCaseProjectionError(f"Invalid fiscal-year identity {period!r}.")
    return int(match.group(1))


def _exact_quarter_rows(package: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    section = package.get("quarterly_financials")
    raw_rows = section.get("rows") if isinstance(section, Mapping) else None
    if not isinstance(raw_rows, list):
        return ()
    by_period: dict[str, Mapping[str, Any]] = {}
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            continue
        period = str(raw.get("period") or "")
        _quarter_ordinal(period)
        if period in by_period:
            raise InvestmentCaseProjectionError(f"Duplicate quarterly identity {period!r}.")
        by_period[period] = raw
    return tuple(by_period[key] for key in sorted(by_period, key=_quarter_ordinal))


def _latest_ttm_window(package: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rows = _exact_quarter_rows(package)
    if len(rows) < 4:
        return ()
    window = rows[-4:]
    ordinals = [_quarter_ordinal(str(row.get("period") or "")) for row in window]
    if ordinals != list(range(ordinals[0], ordinals[0] + 4)):
        return ()
    return window


def _prior_ttm_window(package: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rows = _exact_quarter_rows(package)
    if len(rows) < 8:
        return ()
    window = rows[-8:-4]
    ordinals = [_quarter_ordinal(str(row.get("period") or "")) for row in window]
    if ordinals != list(range(ordinals[0], ordinals[0] + 4)):
        return ()
    return window


_COMPLETED_FY_REQUIRED_METRICS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "free_cash_flow",
)


def _latest_completed_fiscal_year(package: Mapping[str, Any]) -> Mapping[str, Any] | None:
    section = package.get("annual_financials")
    raw_rows = section.get("rows") if isinstance(section, Mapping) else None
    if not isinstance(raw_rows, list):
        return None
    accepted: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            continue
        period = str(raw.get("period") or "")
        _annual_ordinal(period)
        if period in seen:
            raise InvestmentCaseProjectionError(f"Duplicate annual identity {period!r}.")
        seen.add(period)
        if all(_field_number(raw.get(metric)) is not None for metric in _COMPLETED_FY_REQUIRED_METRICS):
            accepted.append(raw)
    return max(accepted, key=lambda row: _annual_ordinal(str(row.get("period") or ""))) if accepted else None


def _prior_fiscal_year(
    package: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if current is None:
        return None
    target = f"{_annual_ordinal(str(current.get('period') or '')) - 1}-FY"
    section = package.get("annual_financials")
    raw_rows = section.get("rows") if isinstance(section, Mapping) else None
    matches = [row for row in raw_rows or [] if isinstance(row, Mapping) and str(row.get("period") or "") == target]
    if len(matches) > 1:
        raise InvestmentCaseProjectionError(f"Duplicate annual identity {target!r}.")
    return matches[0] if matches else None


def _sum_metric(rows: Sequence[Mapping[str, Any]], metric: str) -> tuple[float | None, str]:
    if len(rows) != 4:
        return None, ""
    values: list[float] = []
    sources: list[str] = []
    for row in rows:
        field = row.get(metric)
        value = _field_number(field)
        if value is None:
            return None, _joined([*sources, _field_source(field)])
        values.append(value)
        sources.append(_field_source(field))
    return round(sum(values), 12), _joined(sources)


def _ratio(
    numerator: tuple[float | None, str],
    denominator: tuple[float | None, str],
) -> tuple[float | None, str]:
    num, num_source = numerator
    den, den_source = denominator
    if num is None or den is None or den == 0:
        return None, _joined((num_source, den_source))
    return num / den, _joined((num_source, den_source))


def _annual_metric(row: Mapping[str, Any] | None, metric: str) -> tuple[float | None, str]:
    if row is None:
        return None, ""
    field = row.get(metric)
    return _field_number(field), _field_source(field)


@dataclass(frozen=True)
class _MetricSpec:
    metric_id: str
    label: str
    unit: str
    annual_metric: str | None = None
    ttm_metric: str | None = None
    derived_kind: str = ""
    guidance_metric: str = ""
    selected_basis: str = "ttm_then_fy"
    basis_kind: str = "exact_four_quarter_ttm"
    note: str = ""
    valuation_input: str = ""


_METRIC_SPECS = (
    _MetricSpec("price", "Current share price ($/share)", "$/share", selected_basis="latest_snapshot", basis_kind="latest_snapshot", valuation_input="price", note="Point-in-time market input; no source fallback."),
    _MetricSpec("revenue", "Revenue ($m)", "$m", "revenue", "revenue"),
    _MetricSpec("revenue_growth", "Revenue growth (%)", "%", derived_kind="revenue_growth", guidance_metric="revenue", basis_kind="exact_four_quarter_ratio"),
    _MetricSpec("gross_margin", "Gross margin (%)", "%", derived_kind="gross_margin", basis_kind="exact_four_quarter_ratio"),
    _MetricSpec("operating_margin", "Operating margin (%)", "%", derived_kind="operating_margin", guidance_metric="operating_margin", basis_kind="exact_four_quarter_ratio"),
    _MetricSpec("base_ebitda", "Base EBITDA ($m)", "$m", "base_ebitda", "base_ebitda"),
    _MetricSpec("base_ebitda_margin", "Base EBITDA margin (%)", "%", derived_kind="base_ebitda_margin", basis_kind="exact_four_quarter_ratio"),
    _MetricSpec("adjusted_ebitda", "Adjusted EBITDA ($m)", "$m", "adjusted_ebitda", "adjusted_ebitda"),
    _MetricSpec("adjusted_ebitda_margin", "Adjusted EBITDA margin (%)", "%", derived_kind="adjusted_ebitda_margin", basis_kind="exact_four_quarter_ratio"),
    _MetricSpec("net_income", "Net income ($m)", "$m", "net_income", "net_income"),
    _MetricSpec("adjusted_eps_guidance", "Adjusted EPS guidance reference ($/share)", "$/share", guidance_metric="adjusted_eps", selected_basis="guidance_reference", basis_kind="guidance_reference", note="Guidance range is reference-only until a user selects a point."),
    _MetricSpec("free_cash_flow", "Free cash flow ($m)", "$m", "free_cash_flow", "free_cash_flow"),
    _MetricSpec("depreciation_amortization", "D&A ($m)", "$m", "depreciation_amortization", "depreciation_amortization"),
    _MetricSpec("capital_expenditures", "Capital expenditure ($m)", "$m", "capital_expenditures", "capital_expenditures", guidance_metric="capital_expenditures"),
    _MetricSpec("working_capital_investment", "Working-capital investment ($m)", "$m", selected_basis="manual_only", basis_kind="manual_only", note="Positive values are cash outflows; no source-backed normalized default exists."),
    _MetricSpec("buyback_cash", "Buyback cash ($m)", "$m", "buybacks_cash", "buybacks_cash", guidance_metric="share_repurchases", selected_basis="manual_only", basis_kind="historical_context_only", note="Historical values remain migration evidence; forecast use requires an explicit override."),
    _MetricSpec("buyback_execution_price", "Buyback execution price ($/share)", "$/share", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("diluted_shares", "Diluted shares (m)", "m shares", derived_kind="diluted_shares", guidance_metric="diluted_shares", selected_basis="latest_snapshot", basis_kind="latest_snapshot"),
    _MetricSpec("share_issuance", "Share issuance (m)", "m shares", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("net_debt", "Net cash / debt ($m)", "$m", selected_basis="latest_snapshot", basis_kind="latest_snapshot", valuation_input="net_debt", note="Positive values represent net debt; unavailable never becomes zero."),
    _MetricSpec("tax_rate", "Tax rate (%)", "%", selected_basis="manual_only", basis_kind="manual_only", note="No unsupported tax fallback."),
    _MetricSpec("target_pe", "Target P/E (x)", "x", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("target_ev_adjusted_ebitda", "Target EV / adjusted EBITDA (x)", "x", selected_basis="manual_only", basis_kind="manual_only", valuation_input="target_ev_adjusted_ebitda"),
    _MetricSpec("target_ev_revenue", "Target EV / revenue (x)", "x", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("target_fcf_yield", "Target FCF yield (%)", "%", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("dcf_revenue_growth", "DCF revenue growth (%)", "%", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("dcf_wacc", "DCF WACC (%)", "%", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("dcf_terminal_growth", "DCF terminal growth (%)", "%", selected_basis="manual_only", basis_kind="manual_only"),
    _MetricSpec("dcf_forecast_years", "DCF forecast period (years)", "years", selected_basis="manual_only", basis_kind="manual_only"),
)


@dataclass(frozen=True)
class ResolvedInvestmentCaseInput:
    metric_id: str
    metric_label: str
    unit: str
    fy_value: float | None
    ttm_value: float | None
    full_year_guidance_display: str
    full_year_guidance_low: float | None
    full_year_guidance_high: float | None
    quarter_guidance_display: str
    quarter_guidance_low: float | None
    quarter_guidance_high: float | None
    selected_value: float | None
    selected_source: str
    fy_period: str
    ttm_period: str
    full_year_guidance_period: str
    quarter_guidance_period: str
    publication_date: str
    notes: str
    source_ref: str
    status: str
    basis_kind: str
    basis_period: str
    full_year_guidance_numeric_state: str
    quarter_guidance_numeric_state: str
    source_alias: str


@dataclass(frozen=True)
class ResolvedInvestmentCaseSegment:
    priority: int
    dimension_id: str
    member: str
    display_member: str
    fy_value: float | None
    ttm_value: float | None
    selected_value: float | None
    selected_source: str
    fy_period: str
    ttm_period: str
    source_ref: str
    status: str
    basis_kind: str
    basis_period: str
    source_alias: str


@dataclass(frozen=True)
class ResolvedInvestmentCaseDebate:
    priority: int
    business_key: str
    text: str
    review_state: str
    source_ref: str
    source_alias: str


@dataclass(frozen=True)
class InvestmentCaseWorkbookProjection:
    market_inputs: tuple[ResolvedInvestmentCaseInput, ...]
    segment_inputs: tuple[ResolvedInvestmentCaseSegment, ...]
    debates: tuple[ResolvedInvestmentCaseDebate, ...]
    dimension_options: tuple[str, ...]
    workbook_rows: tuple[dict[str, Any], ...]
    fy_period: str
    ttm_period: str
    ttm_quarters: tuple[str, ...]
    full_year_guidance_period: str
    quarter_guidance_period: str
    projection_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_inputs": [dict(row.__dict__) for row in self.market_inputs],
            "segment_inputs": [dict(row.__dict__) for row in self.segment_inputs],
            "debates": [dict(row.__dict__) for row in self.debates],
            "dimension_options": list(self.dimension_options),
            "workbook_rows": [dict(row) for row in self.workbook_rows],
            "fy_period": self.fy_period,
            "ttm_period": self.ttm_period,
            "ttm_quarters": list(self.ttm_quarters),
            "full_year_guidance_period": self.full_year_guidance_period,
            "quarter_guidance_period": self.quarter_guidance_period,
            "projection_digest": self.projection_digest,
        }


def _metric_values(
    spec: _MetricSpec,
    *,
    annual: Mapping[str, Any] | None,
    prior_annual: Mapping[str, Any] | None,
    ttm: Sequence[Mapping[str, Any]],
    prior_ttm: Sequence[Mapping[str, Any]],
    valuation_inputs: Mapping[str, Any],
) -> tuple[tuple[float | None, str], tuple[float | None, str]]:
    if spec.derived_kind == "revenue_growth":
        fy_value, fy_source = _annual_metric(annual, "revenue")
        prior_value, prior_source = _annual_metric(prior_annual, "revenue")
        fy_growth = None if fy_value is None or prior_value in (None, 0) else fy_value / prior_value - 1.0
        fy = (fy_growth, _joined((fy_source, prior_source)))
        current = _sum_metric(ttm, "revenue")
        prior = _sum_metric(prior_ttm, "revenue")
        ttm_growth = None if current[0] is None or prior[0] in (None, 0) else current[0] / prior[0] - 1.0
        return fy, (ttm_growth, _joined((current[1], prior[1])))
    if spec.derived_kind in {"gross_margin", "operating_margin", "base_ebitda_margin", "adjusted_ebitda_margin"}:
        numerator_metric = {
            "gross_margin": "gross_profit",
            "operating_margin": "operating_income",
            "base_ebitda_margin": "base_ebitda",
            "adjusted_ebitda_margin": "adjusted_ebitda",
        }[spec.derived_kind]
        return (
            _ratio(_annual_metric(annual, numerator_metric), _annual_metric(annual, "revenue")),
            _ratio(_sum_metric(ttm, numerator_metric), _sum_metric(ttm, "revenue")),
        )
    if spec.derived_kind == "diluted_shares":
        fy = _annual_metric(annual, "diluted_shares")
        field = valuation_inputs.get("diluted_shares")
        return fy, (_field_number(field), _field_source(field))
    if spec.valuation_input:
        field = valuation_inputs.get(spec.valuation_input)
        value = _field_number(field)
        return (None, ""), (value, _field_source(field))
    if spec.annual_metric or spec.ttm_metric:
        return (
            _annual_metric(annual, str(spec.annual_metric or "")),
            _sum_metric(ttm, str(spec.ttm_metric or "")),
        )
    return (None, ""), (None, "")


def _guidance_bounds(row: Mapping[str, Any] | None, raw_by_evidence: Mapping[str, Mapping[str, Any]]) -> tuple[float | None, float | None]:
    if row is None:
        return None, None
    raw = raw_by_evidence.get(str(row.get("evidence_key") or ""))
    contract = raw.get("comparison_contract") if isinstance(raw, Mapping) else None
    if not isinstance(contract, Mapping):
        return None, None
    unit = str(contract.get("unit") or row.get("unit") or "")
    low = _finite_number(contract.get("low"))
    high = _finite_number(contract.get("high"))
    if low is None:
        low = _finite_number(contract.get("value"))
    if high is None:
        high = low
    if unit == "%":
        low = None if low is None else low / 100.0
        high = None if high is None else high / 100.0
    return low, high


def _guidance_numeric_state(
    row: Mapping[str, Any] | None,
    raw_by_evidence: Mapping[str, Mapping[str, Any]],
) -> str:
    if row is None or not str(row.get("value") or "").strip():
        return "unavailable"
    raw = raw_by_evidence.get(str(row.get("evidence_key") or ""))
    contract = raw.get("comparison_contract") if isinstance(raw, Mapping) else None
    if not isinstance(contract, Mapping):
        return "qualitative_only"
    low, high = _guidance_bounds(row, raw_by_evidence)
    if low is None or high is None:
        return "qualitative_only"
    qualifier = str(contract.get("qualifier") or "").casefold()
    approximate = bool(contract.get("approximate")) or qualifier in {
        "approximately",
        "around",
    }
    if low == high:
        if qualifier in {"at least", "minimum", "gte"}:
            return "typed_minimum_point"
        return "typed_approximate_point" if approximate else "typed_point"
    return "typed_range"


def format_typed_guidance_display(
    *,
    raw_display: str,
    numeric_state: str,
    low: float | None,
    high: float | None,
    unit: str,
) -> str:
    """Format accepted typed guidance without parsing investor-facing prose."""

    def trimmed(value: float, decimals: int) -> str:
        text = f"{value:,.{decimals}f}"
        return text.rstrip("0").rstrip(".") if "." in text else text

    def point(value: float, *, approximate: bool = False) -> str:
        prefix = "~" if approximate else ""
        if unit == "%":
            percent = f"{value * 100.0:.1f}" if approximate else trimmed(value * 100.0, 1)
            return f"{prefix}{percent}%"
        if unit == "$/share":
            return f"{prefix}{value:,.2f}"
        if unit == "x":
            return f"{prefix}{value:,.2f}"
        if unit in {"$m", "m shares"}:
            return f"{prefix}{trimmed(value, 1)}"
        return f"{prefix}{trimmed(value, 2)}"

    def value_range(lower: float, upper: float) -> str:
        if unit == "%":
            return f"{trimmed(lower * 100.0, 1)}–{trimmed(upper * 100.0, 1)}%"
        if unit == "$/share":
            return f"{lower:,.2f}–{upper:,.2f}"
        if unit == "x":
            return f"{lower:,.2f}–{upper:,.2f}"
        if unit in {"$m", "m shares"}:
            return f"{trimmed(lower, 1)}–{trimmed(upper, 1)}"
        return f"{trimmed(lower, 2)}–{trimmed(upper, 2)}"

    state = str(numeric_state or "")
    if state == "qualitative_only":
        display = str(raw_display or "").strip()
        approximate_match = re.fullmatch(
            r"(?i:around|approximately|approx\.)\s+\$?([0-9]+(?:\.[0-9]+)?)\s*(million|%)",
            display,
        )
        if approximate_match is None:
            minimum_match = re.fullmatch(
                r"(?i:at least)\s+\$?([0-9]+(?:\.[0-9]+)?)\s*(million|%)",
                display,
            )
            if minimum_match is None:
                return display
            numeric_text, suffix = minimum_match.groups()
            value = float(numeric_text)
            if suffix == "%" and unit == "%":
                return f"≥{trimmed(value, 1)}%"
            if suffix.casefold() == "million" and unit in {"m shares", "$m"}:
                return f"≥{trimmed(value, 1)}"
            return display
        numeric_text, suffix = approximate_match.groups()
        value = float(numeric_text)
        if suffix == "%" and unit == "%":
            return f"~{value:.1f}%"
        if suffix.casefold() == "million" and unit in {"m shares", "$m"}:
            return f"~{trimmed(value, 1)}"
        return display
    if state == "unavailable" or low is None or high is None:
        return ""
    if state == "typed_range":
        return value_range(low, high)
    if state == "typed_approximate_point":
        return point(low, approximate=True)
    if state == "typed_minimum_point":
        if unit == "%":
            return f"≥{trimmed(low * 100.0, 1)}%"
        if unit == "$/share":
            return f"≥{low:,.2f}"
        return f"≥{trimmed(low, 1)}"
    if state == "typed_point":
        return point(low)
    raise InvestmentCaseProjectionError(f"Unsupported typed guidance display state {state!r}.")


def _guidance_maps(
    package: Mapping[str, Any],
    *,
    profile_pack_ids: set[str],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], str, str, dict[str, Mapping[str, Any]]]:
    section = package.get("normalized_guidance")
    raw_rows = section.get("items") if isinstance(section, Mapping) else None
    rows = [row for row in raw_rows or [] if isinstance(row, Mapping)]
    projection = build_valuation_guidance_projection(rows, profile_pack_ids=profile_pack_ids)
    all_current = [*projection.current_primary_rows, *projection.current_secondary_rows]
    fy: dict[str, Mapping[str, Any]] = {}
    quarter: dict[str, Mapping[str, Any]] = {}
    raw_by_evidence = {str(row.get("evidence_key") or ""): row for row in rows if str(row.get("evidence_key") or "")}
    for resolved in all_current:
        row = resolved.to_dict()
        metric = str(row.get("canonical_metric") or "")
        horizon = str(row.get("horizon") or "")
        target = fy if re.fullmatch(r"FY\d{4}", horizon) else quarter if re.fullmatch(r"\d{4}-Q[1-4]", horizon) else None
        if target is None:
            continue
        if metric in target:
            raise InvestmentCaseProjectionError(f"Duplicate accepted current guidance for {metric!r} in {horizon!r}.")
        target[metric] = row
    fy_periods = {str(row.get("horizon") or "") for row in fy.values()}
    quarter_periods = {str(row.get("horizon") or "") for row in quarter.values()}
    if len(fy_periods) > 1 or len(quarter_periods) > 1:
        raise InvestmentCaseProjectionError("Current Investment Case guidance must resolve to one exact FY and one exact quarter horizon.")
    return fy, quarter, next(iter(fy_periods), ""), next(iter(quarter_periods), ""), raw_by_evidence


def _select_nonmanual_value(
    spec: _MetricSpec,
    *,
    fy_value: float | None,
    ttm_value: float | None,
    fy_period: str,
) -> tuple[float | None, str]:
    if spec.selected_basis in {"manual_only", "guidance_reference"}:
        return None, "Unavailable"
    if spec.selected_basis == "latest_snapshot":
        if ttm_value is None:
            return None, "Unavailable"
        return ttm_value, "Latest accepted snapshot"
    if ttm_value is not None:
        return ttm_value, "Model default (TTM)"
    if fy_value is not None:
        return fy_value, f"Model default ({fy_period.replace('-FY', '')})"
    return None, "Unavailable"


def _build_market_inputs(
    package: Mapping[str, Any],
    *,
    annual: Mapping[str, Any] | None,
    prior_annual: Mapping[str, Any] | None,
    ttm: Sequence[Mapping[str, Any]],
    prior_ttm: Sequence[Mapping[str, Any]],
    guidance_fy: Mapping[str, Mapping[str, Any]],
    guidance_q: Mapping[str, Mapping[str, Any]],
    raw_guidance: Mapping[str, Mapping[str, Any]],
    fy_period: str,
    ttm_period: str,
) -> tuple[ResolvedInvestmentCaseInput, ...]:
    valuation_inputs = package.get("valuation_inputs")
    valuation_inputs = valuation_inputs if isinstance(valuation_inputs, Mapping) else {}
    rows: list[ResolvedInvestmentCaseInput] = []
    for spec in _METRIC_SPECS:
        fy_pair, ttm_pair = _metric_values(
            spec,
            annual=annual,
            prior_annual=prior_annual,
            ttm=ttm,
            prior_ttm=prior_ttm,
            valuation_inputs=valuation_inputs,
        )
        fy_guide = guidance_fy.get(spec.guidance_metric) if spec.guidance_metric else None
        q_guide = guidance_q.get(spec.guidance_metric) if spec.guidance_metric else None
        fy_bounds = _guidance_bounds(fy_guide, raw_guidance)
        q_bounds = _guidance_bounds(q_guide, raw_guidance)
        fy_guidance_state = _guidance_numeric_state(fy_guide, raw_guidance)
        q_guidance_state = _guidance_numeric_state(q_guide, raw_guidance)
        fy_source_display = str((fy_guide or {}).get("value") or "")
        q_source_display = str((q_guide or {}).get("value") or "")
        fy_guidance_display = format_typed_guidance_display(
            raw_display=fy_source_display,
            numeric_state=fy_guidance_state,
            low=fy_bounds[0],
            high=fy_bounds[1],
            unit=spec.unit,
        )
        q_guidance_display = format_typed_guidance_display(
            raw_display=q_source_display,
            numeric_state=q_guidance_state,
            low=q_bounds[0],
            high=q_bounds[1],
            unit=spec.unit,
        )
        selected_value, selected_source = _select_nonmanual_value(
            spec,
            fy_value=fy_pair[0],
            ttm_value=ttm_pair[0],
            fy_period=fy_period,
        )
        row_ttm_period = ttm_period
        if spec.selected_basis == "latest_snapshot":
            valuation_field = valuation_inputs.get(spec.valuation_input or spec.metric_id)
            row_ttm_period = _field_period(valuation_field)
            if selected_value is not None:
                selected_source = "Latest snapshot"
        basis_period = (
            row_ttm_period
            if selected_value is not None and spec.selected_basis == "latest_snapshot"
            else ttm_period
            if selected_value is not None and ttm_pair[0] is not None
            else fy_period
            if selected_value is not None and fy_pair[0] is not None
            else ""
        )
        sources = [
            fy_pair[1],
            ttm_pair[1],
            str((fy_guide or {}).get("source_ref") or ""),
            str((q_guide or {}).get("source_ref") or ""),
        ]
        publication_date = _joined(
            (
                str((fy_guide or {}).get("publication_date") or ""),
                str((q_guide or {}).get("publication_date") or ""),
            )
        )
        source_ref = _joined(sources) or f"normalized-package:investment-case:{spec.metric_id}"
        status = "populated" if selected_value is not None else "manual_input_required"
        if spec.metric_id == "adjusted_eps_guidance":
            status = "reference_only"
        note_parts = [spec.note]
        if fy_source_display:
            note_parts.append(f"Full-year source guidance: {fy_source_display}")
        if q_source_display:
            note_parts.append(f"Latest-quarter source guidance: {q_source_display}")
        rows.append(
            ResolvedInvestmentCaseInput(
                metric_id=spec.metric_id,
                metric_label=spec.label,
                unit=spec.unit,
                fy_value=fy_pair[0],
                ttm_value=ttm_pair[0],
                full_year_guidance_display=fy_guidance_display,
                full_year_guidance_low=fy_bounds[0],
                full_year_guidance_high=fy_bounds[1],
                quarter_guidance_display=q_guidance_display,
                quarter_guidance_low=q_bounds[0],
                quarter_guidance_high=q_bounds[1],
                selected_value=selected_value,
                selected_source=selected_source,
                fy_period=fy_period,
                ttm_period=row_ttm_period,
                full_year_guidance_period=str((fy_guide or {}).get("horizon") or ""),
                quarter_guidance_period=str((q_guide or {}).get("horizon") or ""),
                publication_date=publication_date,
                notes=" | ".join(part for part in note_parts if part),
                source_ref=source_ref,
                status=status,
                basis_kind=spec.basis_kind,
                basis_period=basis_period,
                full_year_guidance_numeric_state=fy_guidance_state,
                quarter_guidance_numeric_state=q_guidance_state,
                source_alias=_source_alias(source_ref),
            )
        )
    return tuple(rows)


def _segment_value(row: Mapping[str, Any]) -> float | None:
    return _field_number(row.get("revenue") or row.get("metric_value"))


def _build_segment_inputs(
    package: Mapping[str, Any],
    *,
    fy_period: str,
    ttm_quarters: tuple[str, ...],
) -> tuple[ResolvedInvestmentCaseSegment, ...]:
    section = package.get("segments")
    raw_rows = section.get("items") if isinstance(section, Mapping) else None
    rows = [row for row in raw_rows or [] if isinstance(row, Mapping) and str(row.get("metric") or "") == "revenue"]
    by_identity: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    display_order: dict[tuple[str, str], int] = {}
    for row in rows:
        dimension, member = canonical_segment_dimension_member(row.get("dimension"), row.get("member"))
        period = str(row.get("period") or "")
        identity = (period, dimension, member)
        if identity in by_identity:
            raise InvestmentCaseProjectionError(f"Duplicate segment revenue identity {identity!r}.")
        by_identity[identity] = row
        display_order[(dimension, member)] = int(row.get("display_order") or 999)
    members = {
        (dimension, member)
        for period, dimension, member in by_identity
        if period == fy_period and _segment_value(by_identity[(period, dimension, member)]) is not None
    }
    dimension_rank = {"total_company": 0, "brand": 1, "geography": 2}
    ordered = sorted(members, key=lambda item: (dimension_rank.get(item[0], 99), display_order.get(item, 999), item))
    result: list[ResolvedInvestmentCaseSegment] = []
    for priority, (dimension, member) in enumerate(ordered, start=1):
        fy_row = by_identity[(fy_period, dimension, member)]
        fy_value = _segment_value(fy_row)
        quarter_rows = [by_identity.get((period, dimension, member)) for period in ttm_quarters]
        q_values = [_segment_value(row) if row is not None else None for row in quarter_rows]
        ttm_value = round(sum(value for value in q_values if value is not None), 12) if len(q_values) == 4 and all(value is not None for value in q_values) else None
        selected_value = ttm_value if ttm_value is not None else fy_value
        selected_source = "Model default (TTM)" if ttm_value is not None else f"Model default ({fy_period.replace('-FY', '')})"
        sources = [_field_source(fy_row.get("revenue") or fy_row.get("metric_value"))]
        sources.extend(_field_source(row.get("revenue") or row.get("metric_value")) for row in quarter_rows if row is not None)
        result.append(
            ResolvedInvestmentCaseSegment(
                priority=priority,
                dimension_id=dimension,
                member=member,
                display_member=canonical_segment_display_member(dimension, _field_value(fy_row.get("segment")) or member),
                fy_value=fy_value,
                ttm_value=ttm_value,
                selected_value=selected_value,
                selected_source=selected_source,
                fy_period=fy_period,
                ttm_period=f"TTM through {ttm_quarters[-1]}" if ttm_quarters else "",
                source_ref=_joined(sources) or f"normalized-package:segments:{dimension}:{member}",
                status="populated" if selected_value is not None else "missing_source",
                basis_kind=(
                    "exact_four_quarter_ttm"
                    if ttm_value is not None
                    else "latest_completed_fy"
                ),
                basis_period=(
                    f"TTM through {ttm_quarters[-1]}"
                    if ttm_value is not None and ttm_quarters
                    else fy_period
                ),
                source_alias=_source_alias(_joined(sources)),
            )
        )
    return tuple(result)


def _build_debates(package: Mapping[str, Any]) -> tuple[ResolvedInvestmentCaseDebate, ...]:
    investment_case = package.get("investment_case")
    invalidators = investment_case.get("invalidators") if isinstance(investment_case, Mapping) else None
    result: list[ResolvedInvestmentCaseDebate] = []
    for raw in invalidators or []:
        if not isinstance(raw, Mapping):
            continue
        text_field = raw.get("text")
        text = str(_field_value(text_field) or "").strip()
        business_key = str(raw.get("business_key") or "").strip()
        if not text or not business_key:
            continue
        result.append(
            ResolvedInvestmentCaseDebate(
                priority=int(raw.get("display_order") or 999),
                business_key=business_key,
                text=text,
                review_state=str((text_field or {}).get("review_state") or "manual_review_required") if isinstance(text_field, Mapping) else "manual_review_required",
                source_ref=_field_source(text_field) or f"normalized-package:investment-case:invalidators:{business_key}",
                source_alias=_source_alias(_field_source(text_field)),
            )
        )
    result.sort(key=lambda row: (row.priority, row.business_key))
    return tuple(result[:10])


def _support_row(
    *,
    row_type: str,
    row_key: str,
    priority: int,
    slot_key: str,
    metric_id: str = "",
    metric_label: str = "",
    unit: str = "",
    fy_value: float | None = None,
    ttm_value: float | None = None,
    full_year_guidance_display: str = "",
    full_year_guidance_low: float | None = None,
    full_year_guidance_high: float | None = None,
    quarter_guidance_display: str = "",
    quarter_guidance_low: float | None = None,
    quarter_guidance_high: float | None = None,
    selected_value: float | None = None,
    selected_source: str = "",
    fy_period: str = "",
    ttm_period: str = "",
    full_year_guidance_period: str = "",
    quarter_guidance_period: str = "",
    notes: str = "",
    source_ref: str = "",
    status: str = "",
    dimension_id: str = "",
    member: str = "",
    baseline_value: float | None = None,
    publication_date: str = "",
    dimension_option: str = "",
    basis_kind: str = "",
    basis_period: str = "",
    full_year_guidance_numeric_state: str = "",
    quarter_guidance_numeric_state: str = "",
    source_alias: str = "",
) -> dict[str, Any]:
    return {
        "row_type": row_type,
        "row_key": row_key,
        "priority": priority,
        "slot_key": slot_key,
        "metric_id": metric_id,
        "metric_label": metric_label,
        "unit": unit,
        "fy_value": fy_value,
        "ttm_value": ttm_value,
        "full_year_guidance_display": full_year_guidance_display,
        "full_year_guidance_low": full_year_guidance_low,
        "full_year_guidance_high": full_year_guidance_high,
        "quarter_guidance_display": quarter_guidance_display,
        "quarter_guidance_low": quarter_guidance_low,
        "quarter_guidance_high": quarter_guidance_high,
        "selected_value": selected_value,
        "selected_source": selected_source,
        "fy_period": fy_period,
        "ttm_period": ttm_period,
        "full_year_guidance_period": full_year_guidance_period,
        "quarter_guidance_period": quarter_guidance_period,
        "notes": notes,
        "source_ref": source_ref,
        "status": status,
        "dimension_id": dimension_id,
        "member": member,
        "baseline_value": baseline_value,
        "publication_date": publication_date,
        "dimension_option": dimension_option,
        "basis_kind": basis_kind,
        "basis_period": basis_period,
        "full_year_guidance_numeric_state": full_year_guidance_numeric_state,
        "quarter_guidance_numeric_state": quarter_guidance_numeric_state,
        "source_alias": source_alias,
    }


def build_investment_case_workbook_projection(
    package: Mapping[str, Any],
    *,
    profile_pack_ids: set[str] | None = None,
) -> InvestmentCaseWorkbookProjection:
    """Resolve one fresh package into deterministic Investment Case support rows."""

    annual = _latest_completed_fiscal_year(package)
    prior_annual = _prior_fiscal_year(package, annual)
    ttm = _latest_ttm_window(package)
    prior_ttm = _prior_ttm_window(package)
    fy_period = str((annual or {}).get("period") or "")
    ttm_quarters = tuple(str(row.get("period") or "") for row in ttm)
    ttm_period = f"TTM through {ttm_quarters[-1]}" if ttm_quarters else ""
    guidance_fy, guidance_q, guidance_fy_period, guidance_q_period, raw_guidance = _guidance_maps(
        package,
        profile_pack_ids=set(profile_pack_ids or ()),
    )
    market_inputs = _build_market_inputs(
        package,
        annual=annual,
        prior_annual=prior_annual,
        ttm=ttm,
        prior_ttm=prior_ttm,
        guidance_fy=guidance_fy,
        guidance_q=guidance_q,
        raw_guidance=raw_guidance,
        fy_period=fy_period,
        ttm_period=ttm_period,
    )
    segment_inputs = _build_segment_inputs(package, fy_period=fy_period, ttm_quarters=ttm_quarters)
    debates = _build_debates(package)
    dimension_options = tuple(
        dict.fromkeys(
            row.dimension_id
            for row in sorted(segment_inputs, key=lambda item: (item.priority, item.dimension_id))
        )
    )
    workbook_rows: list[dict[str, Any]] = []
    for priority, dimension in enumerate(dimension_options, start=1):
        workbook_rows.append(
            _support_row(
                row_type="dimension_option",
                row_key=f"dimension_option|{dimension}",
                priority=priority,
                slot_key=f"dimension_option|{dimension}",
                metric_id="segment_dimension",
                metric_label=dimension.replace("_", " ").title(),
                unit="choice",
                source_ref=f"normalized-package:segments:dimension:{dimension}",
                status="populated",
                dimension_id=dimension,
                dimension_option=dimension.replace("_", " ").title(),
                basis_kind="choice",
                basis_period="",
                source_alias="Normalized package",
            )
        )
    for priority, row in enumerate(market_inputs, start=1):
        workbook_rows.append(
            _support_row(
                row_type="market_input",
                row_key=f"market_input|{row.metric_id}",
                priority=100 + priority,
                slot_key=f"market_input|{row.metric_id}",
                **dict(row.__dict__),
            )
        )
    for row in segment_inputs:
        workbook_rows.append(
            _support_row(
                row_type="segment_input",
                row_key=f"segment_input|{row.dimension_id}|{row.member}",
                priority=200 + row.priority,
                slot_key=f"segment_input|{row.priority:03d}",
                metric_id="segment_revenue",
                metric_label=f"{row.display_member} revenue ($m)",
                unit="$m",
                fy_value=row.fy_value,
                ttm_value=row.ttm_value,
                selected_value=row.selected_value,
                selected_source=row.selected_source,
                fy_period=row.fy_period,
                ttm_period=row.ttm_period,
                notes="Total Company is a tie-out; brand and geography are separate analytical views.",
                source_ref=row.source_ref,
                status=row.status,
                dimension_id=row.dimension_id,
                member=row.member,
                baseline_value=row.selected_value,
                dimension_option=row.dimension_id.title() if row.dimension_id != "total_company" else "Total Company",
                basis_kind=row.basis_kind,
                basis_period=row.basis_period,
                source_alias=row.source_alias,
            )
        )
    for slot_index, row in enumerate(debates, start=1):
        workbook_rows.append(
            _support_row(
                row_type="debate",
                row_key=f"debate|{row.business_key}",
                priority=300 + row.priority,
                slot_key=f"debate|{slot_index:03d}",
                metric_id=row.business_key,
                metric_label=row.business_key.replace("-", " ").title(),
                unit="text",
                notes=row.text,
                source_ref=row.source_ref,
                status=row.review_state,
                basis_kind="typed_evidence",
                basis_period="",
                source_alias=row.source_alias,
            )
        )
    workbook_rows.sort(key=lambda row: (int(row["priority"]), str(row["row_key"])))
    digest_payload = {
        "market_inputs": [dict(row.__dict__) for row in market_inputs],
        "segment_inputs": [dict(row.__dict__) for row in segment_inputs],
        "debates": [dict(row.__dict__) for row in debates],
        "dimension_options": list(dimension_options),
        "workbook_rows": workbook_rows,
        "fy_period": fy_period,
        "ttm_period": ttm_period,
        "ttm_quarters": list(ttm_quarters),
        "full_year_guidance_period": guidance_fy_period,
        "quarter_guidance_period": guidance_q_period,
    }
    return InvestmentCaseWorkbookProjection(
        market_inputs=market_inputs,
        segment_inputs=segment_inputs,
        debates=debates,
        dimension_options=dimension_options,
        workbook_rows=tuple(workbook_rows),
        fy_period=fy_period,
        ttm_period=ttm_period,
        ttm_quarters=ttm_quarters,
        full_year_guidance_period=guidance_fy_period,
        quarter_guidance_period=guidance_q_period,
        projection_digest=_canonical_digest(digest_payload),
    )
