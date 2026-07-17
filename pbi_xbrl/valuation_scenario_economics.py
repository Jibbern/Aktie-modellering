"""Typed, ticker-neutral valuation scenario economics.

The workbook formulas mirror these rules, while this module provides an
independent Python oracle for package validation and regression tests.  Missing
inputs never become zero and source-backed assumptions retain exact lineage.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date
import math
import re
from typing import Any, Mapping, Sequence


SCENARIO_CONTRACT_VERSION = "1.2.0"
ROUTE_TOKEN_CONTRACT_VERSION = "1.0.0"
METRIC_HORIZON_CONTRACT_VERSION = "1.0.0"
SCENARIO_IDS = {"common", "bear", "base", "bull", "custom"}
VALUE_KINDS = {"point", "range", "minimum", "maximum", "route", "unavailable"}
SOURCE_CLASSIFICATIONS = {
    "source_actual",
    "source_guidance",
    "user_input",
    "derived_assumption",
    "unavailable",
}
PROPAGATION_RULES = {
    "shared_actual",
    "scenario_specific",
    "selected_growth_route",
    "profile_driver_bridge",
    "no_propagation",
}
TAX_TREATMENTS = {
    "taxable",
    "non_taxable",
    "non_taxable_credit",
    "cash_only",
    "no_eps_impact",
    "direct_eps",
    "manual_review_required",
}
CASH_CLASSIFICATIONS = {
    "operating",
    "investing",
    "financing",
    "non_cash",
    "manual_review_required",
}
DRIVER_TYPES = {
    "revenue_volume",
    "margin_ebitda",
    "cash_flow_capex",
    "capital_structure_interest",
    "share_count_buyback",
    "tax_credit_subsidy",
    "manual_incremental",
}
FIELD_STATUSES = {
    "populated",
    "missing_source",
    "missing_mapping",
    "not_applicable",
    "manual_review_required",
    "parser_conflict",
}

CANONICAL_REVENUE_OUTPUT_METRIC = "revenue_growth"
CANONICAL_TOTAL_COMPANY_TOKEN = "total_company"
CANONICAL_DIRECT_REVENUE_PROPAGATION = "selected_growth_route"
CANONICAL_PROFILE_REVENUE_PROPAGATION = "profile_driver_bridge"
CANONICAL_REVENUE_UNIT = "%"
CANONICAL_POPULATED_STATUS = "populated"
CANONICAL_DIRECT_ROUTE_VALUE_KIND = "point"
CANONICAL_PROFILE_ROUTE_VALUE_KIND = "route"
CANONICAL_IMPACT_METRICS = {
    "adjusted_eps",
    "adjusted_ebitda",
    "base_ebitda",
    "capital_expenditures",
    "cash_interest",
    "eps",
    "fcf",
    "net_debt",
    "operating_margin",
    "revenue_growth",
    "shares",
    "tax",
}
_IMPACT_METRIC_ALIASES = {
    "revenue": CANONICAL_REVENUE_OUTPUT_METRIC,
    "revenue_growth": CANONICAL_REVENUE_OUTPUT_METRIC,
}
_TOTAL_COMPANY_ALIASES = {
    "": CANONICAL_TOTAL_COMPANY_TOKEN,
    "company": CANONICAL_TOTAL_COMPANY_TOKEN,
    "consolidated": CANONICAL_TOTAL_COMPANY_TOKEN,
    "default": CANONICAL_TOTAL_COMPANY_TOKEN,
    "total": CANONICAL_TOTAL_COMPANY_TOKEN,
    "total_company": CANONICAL_TOTAL_COMPANY_TOKEN,
    "totalcompany": CANONICAL_TOTAL_COMPANY_TOKEN,
}
_UNIT_ALIASES = {
    "%": "%",
    "$m": "$m",
    "$ m": "$m",
    "$ per share": "$/share",
    "$/share": "$/share",
    "classification": "classification",
    "m shares": "m shares",
    "m-shares": "m shares",
    "m_shares": "m shares",
    "million shares": "m shares",
    "multiple": "x",
    "percent": "%",
    "percentage": "%",
    "period": "period",
    "usd m": "$m",
    "usd-m": "$m",
    "usd_m": "$m",
    "usd per share": "$/share",
    "usd/share": "$/share",
    "x": "x",
}

EXPECTED_UNITS = {
    "price": "$/share",
    "shares_outstanding": "m shares",
    "diluted_shares": "m shares",
    "per_share_mode": "classification",
    "net_debt": "$m",
    "revenue_ttm": "$m",
    "base_ebitda_ttm": "$m",
    "adjusted_ebitda_ttm": "$m",
    "fcf_ttm": "$m",
    "net_income_ttm": "$m",
    "operating_cash_flow_ttm": "$m",
    "capital_expenditures_ttm": "$m",
    "interest_paid_ttm": "$m",
    "eps_ttm": "$/share",
    "adjusted_eps_ttm": "$/share",
    "cash": "$m",
    "total_debt": "$m",
    "revenue_growth": "%",
    "base_ebitda_margin": "%",
    "adjusted_ebitda_margin": "%",
    "pre_tax_earnings_bridge": "$m",
    "tax_rate": "%",
    "cash_interest_change": "$m",
    "capital_expenditures_change": "$m",
    "working_capital_adjustment": "$m",
    "buyback_cash": "$m",
    "buyback_execution_price": "$/share",
    "share_issuance": "m shares",
    "debt_paydown": "$m",
    "target_ev_adjusted_ebitda": "x",
    "target_ev_base_ebitda": "x",
    "target_ev_revenue": "x",
    "target_pe": "x",
    "target_fcf_yield": "%",
    "dcf_fcff": "$m",
    "dcf_growth": "%",
    "dcf_terminal_growth": "%",
    "dcf_wacc": "%",
    "scenario_horizon": "period",
    "tax_treatment": "classification",
    "interest_tax_treatment": "classification",
    "operating_margin_guidance": "%",
    "adjusted_eps_guidance": "$/share",
    "capital_expenditures_guidance": "$m",
    "share_repurchases_guidance": "$m",
    "diluted_shares_guidance": "m shares",
}

_HORIZON_RE = re.compile(r"^(?:FY\d{4}|\d{4}-Q[1-4]|TTM|as_of)$")
_FISCAL_HORIZON_RE = re.compile(r"^(?:FY\d{4}|\d{4}-Q[1-4])$")
_QUARTER_HORIZON_RE = re.compile(r"^\d{4}-Q[1-4]$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

TRAILING_PERIOD = "trailing_period"
POINT_IN_TIME = "point_in_time"
REPORTED_PERIOD = "reported_period"
SCENARIO_HORIZON = "scenario_horizon"

# This is the single authoritative metric-to-horizon contract. Validation,
# route resolution, and the independent economic evaluator all consume it.
METRIC_HORIZON_CLASSES = {
    "revenue_ttm": TRAILING_PERIOD,
    "base_ebitda_ttm": TRAILING_PERIOD,
    "adjusted_ebitda_ttm": TRAILING_PERIOD,
    "operating_cash_flow_ttm": TRAILING_PERIOD,
    "capital_expenditures_ttm": TRAILING_PERIOD,
    "fcf_ttm": TRAILING_PERIOD,
    "interest_paid_ttm": TRAILING_PERIOD,
    "net_income_ttm": TRAILING_PERIOD,
    "eps_ttm": TRAILING_PERIOD,
    "adjusted_eps_ttm": TRAILING_PERIOD,
    "price": POINT_IN_TIME,
    "cash": POINT_IN_TIME,
    "total_debt": POINT_IN_TIME,
    "net_debt": POINT_IN_TIME,
    "shares_outstanding": POINT_IN_TIME,
    # Weighted-average diluted shares are a reported-period measure, not a
    # point-in-time balance-sheet fact.
    "diluted_shares": REPORTED_PERIOD,
    "per_share_mode": SCENARIO_HORIZON,
    "revenue_growth": SCENARIO_HORIZON,
    "base_ebitda_margin": SCENARIO_HORIZON,
    "adjusted_ebitda_margin": SCENARIO_HORIZON,
    "pre_tax_earnings_bridge": SCENARIO_HORIZON,
    "tax_rate": SCENARIO_HORIZON,
    "tax_treatment": SCENARIO_HORIZON,
    "cash_interest_change": SCENARIO_HORIZON,
    "interest_tax_treatment": SCENARIO_HORIZON,
    "capital_expenditures_change": SCENARIO_HORIZON,
    "working_capital_adjustment": SCENARIO_HORIZON,
    "buyback_cash": SCENARIO_HORIZON,
    "buyback_execution_price": SCENARIO_HORIZON,
    "share_issuance": SCENARIO_HORIZON,
    "debt_paydown": SCENARIO_HORIZON,
    "target_ev_adjusted_ebitda": SCENARIO_HORIZON,
    "target_ev_base_ebitda": SCENARIO_HORIZON,
    "target_ev_revenue": SCENARIO_HORIZON,
    "target_pe": SCENARIO_HORIZON,
    "target_fcf_yield": SCENARIO_HORIZON,
    "dcf_fcff": SCENARIO_HORIZON,
    "dcf_growth": SCENARIO_HORIZON,
    "dcf_terminal_growth": SCENARIO_HORIZON,
    "dcf_wacc": SCENARIO_HORIZON,
    "scenario_horizon": SCENARIO_HORIZON,
    "operating_margin_guidance": SCENARIO_HORIZON,
    "adjusted_eps_guidance": SCENARIO_HORIZON,
    "capital_expenditures_guidance": SCENARIO_HORIZON,
    "share_repurchases_guidance": SCENARIO_HORIZON,
    "diluted_shares_guidance": SCENARIO_HORIZON,
}


@dataclass(frozen=True)
class ScenarioContractIssue:
    field: str
    rule_id: str
    message: str


@dataclass(frozen=True)
class RevenueRouteResolution:
    """Deterministic result of resolving one scenario Revenue route."""

    value: float | None
    route_mode: str | None
    rule_id: str | None
    message: str
    business_keys: tuple[str, ...] = ()
    source_refs: tuple[str, ...] = ()


def metric_horizon_class(assumption_id: str) -> str | None:
    """Return the authoritative horizon class for one typed metric."""

    return METRIC_HORIZON_CLASSES.get(str(assumption_id or ""))


def metric_horizon_is_compatible(assumption_id: str, horizon: str) -> bool:
    """Validate a metric horizon without inferring semantics from its name."""

    horizon_class = metric_horizon_class(assumption_id)
    if horizon_class == TRAILING_PERIOD:
        return horizon == "TTM"
    if horizon_class == POINT_IN_TIME:
        return horizon == "as_of"
    if horizon_class == REPORTED_PERIOD:
        return bool(_QUARTER_HORIZON_RE.fullmatch(horizon) or horizon == "TTM")
    if horizon_class == SCENARIO_HORIZON:
        return bool(_FISCAL_HORIZON_RE.fullmatch(horizon))
    return False


_INVALID_TOKEN = object()
_CANONICAL_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _alias_key(value: Any) -> str:
    """Normalize spelling only for bounded vocabulary lookup at the boundary."""

    return re.sub(r"_+", "_", re.sub(r"[\s-]+", "_", str(value or "").strip().lower())).strip("_")


def _canonical_vocabulary_token(
    value: Any,
    vocabulary: set[str],
    *,
    aliases: Mapping[str, str] | None = None,
) -> str | object:
    key = _alias_key(value)
    alias_map = {_alias_key(item): item for item in vocabulary}
    alias_map.update({_alias_key(raw): canonical for raw, canonical in (aliases or {}).items()})
    return alias_map.get(key, _INVALID_TOKEN)


def _canonical_horizon_token(value: Any) -> str | object:
    raw = str(value or "").strip()
    upper = raw.upper()
    fiscal = re.fullmatch(r"FY[\s_-]*(\d{4})", upper)
    if fiscal:
        return f"FY{fiscal.group(1)}"
    quarter = re.fullmatch(r"(\d{4})[\s_-]*Q([1-4])", upper)
    if quarter:
        return f"{quarter.group(1)}-Q{quarter.group(2)}"
    special = _alias_key(raw)
    if special == "ttm":
        return "TTM"
    if special == "as_of":
        return "as_of"
    return _INVALID_TOKEN


def _canonical_unit_token(value: Any) -> str | object:
    raw = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return _UNIT_ALIASES.get(raw, _INVALID_TOKEN)


def _canonical_dimension_token(value: Any, allowed_dimension_ids: set[str] | None) -> str | object:
    key = _alias_key(value)
    if key in _TOTAL_COMPANY_ALIASES:
        return CANONICAL_TOTAL_COMPANY_TOKEN
    if allowed_dimension_ids is not None:
        return _canonical_vocabulary_token(value, allowed_dimension_ids)
    raw = str(value or "").strip()
    return raw if raw == key and _CANONICAL_ID_RE.fullmatch(raw) else _INVALID_TOKEN


def _canonical_member_token(value: Any, dimension_id: str | object) -> str | object:
    if dimension_id == CANONICAL_TOTAL_COMPANY_TOKEN:
        return _TOTAL_COMPANY_ALIASES.get(_alias_key(value), _INVALID_TOKEN)
    raw = str(value or "").strip()
    key = _alias_key(raw)
    return raw if raw == key and bool(_CANONICAL_ID_RE.fullmatch(raw)) else _INVALID_TOKEN


def _canonical_declared_id(value: Any, allowed_ids: set[str] | None) -> str | object:
    if allowed_ids is not None:
        return _canonical_vocabulary_token(value, allowed_ids)
    raw = str(value or "").strip()
    key = _alias_key(raw)
    return raw if raw == key and bool(_CANONICAL_ID_RE.fullmatch(raw)) else _INVALID_TOKEN


def _canonical_optional_declared_id(value: Any, allowed_ids: set[str] | None) -> str | None | object:
    if value in (None, ""):
        return None
    return _canonical_declared_id(value, allowed_ids)


def _token_issue(
    *,
    kind: str,
    index: int,
    row: Mapping[str, Any],
    field: str,
    raw_value: Any,
    vocabulary: Sequence[str],
) -> ScenarioContractIssue:
    identity = str(row.get("assumption_id") or row.get("driver_id") or "")
    output = CANONICAL_REVENUE_OUTPUT_METRIC if _alias_key(identity) == "revenue_growth" else identity
    source_ref = str(row.get("source_ref") or "")
    return ScenarioContractIssue(
        f"investment_case.{kind}.{index}.{field}",
        "scenario_route_token_unknown",
        f"Raw value {raw_value!r} cannot populate canonical field {field!r}; "
        f"business_key={kind}[{index}]::{row.get('scenario_id') or ''}:{identity}, "
        f"source_ref={source_ref!r}, affected_output={output!r}, "
        f"accepted_canonical_vocabulary={sorted(set(vocabulary))!r}. "
        "Only bounded case, spacing, underscore, and hyphen aliases of that vocabulary are accepted.",
    )


def canonicalize_scenario_contract(
    investment_case: Mapping[str, Any],
    *,
    allowed_profile_pack_ids: set[str] | None = None,
    allowed_scenario_driver_ids: set[str] | None = None,
    allowed_dimension_ids: set[str] | None = None,
) -> tuple[dict[str, Any], list[ScenarioContractIssue]]:
    """Canonicalize route tokens once before schema validation and planning."""

    normalized = deepcopy(dict(investment_case))
    issues: list[ScenarioContractIssue] = []
    collection_specs = (
        ("scenario_items", "scenario_items"),
        ("scenario_driver_bridge", "scenario_driver_bridge"),
    )
    for collection_name, kind in collection_specs:
        raw_rows = investment_case.get(collection_name)
        if not isinstance(raw_rows, list):
            continue
        canonical_rows: list[Any] = []
        for index, raw_row in enumerate(raw_rows):
            if not isinstance(raw_row, Mapping):
                canonical_rows.append(raw_row)
                continue
            row = deepcopy(dict(raw_row))

            def assign(field: str, candidate: str | None | object, vocabulary: Sequence[str]) -> None:
                if candidate is _INVALID_TOKEN:
                    issues.append(
                        _token_issue(
                            kind=kind,
                            index=index,
                            row=raw_row,
                            field=field,
                            raw_value=raw_row.get(field),
                            vocabulary=vocabulary,
                        )
                    )
                    return
                row[field] = candidate

            assign("scenario_id", _canonical_vocabulary_token(raw_row.get("scenario_id"), SCENARIO_IDS), SCENARIO_IDS)
            assign(
                "propagation_rule",
                _canonical_vocabulary_token(raw_row.get("propagation_rule"), PROPAGATION_RULES),
                PROPAGATION_RULES,
            )
            assign("horizon", _canonical_horizon_token(raw_row.get("horizon")), ("FYyyyy", "yyyy-Qn", "TTM", "as_of"))
            assign("unit", _canonical_unit_token(raw_row.get("unit")), sorted(set(_UNIT_ALIASES.values())))
            dimension = _canonical_dimension_token(raw_row.get("dimension_id"), allowed_dimension_ids)
            assign(
                "dimension_id",
                dimension,
                sorted(allowed_dimension_ids or {CANONICAL_TOTAL_COMPANY_TOKEN, "canonical_profile_dimension_id"}),
            )
            member = _canonical_member_token(raw_row.get("member"), dimension)
            assign("member", member, (CANONICAL_TOTAL_COMPANY_TOKEN, "canonical_profile_member_id"))
            assign(
                "profile_pack_id",
                _canonical_optional_declared_id(raw_row.get("profile_pack_id"), allowed_profile_pack_ids),
                sorted(allowed_profile_pack_ids or {"canonical_profile_pack_id"}),
            )

            if collection_name == "scenario_items":
                assign(
                    "assumption_id",
                    _canonical_vocabulary_token(raw_row.get("assumption_id"), set(EXPECTED_UNITS)),
                    sorted(EXPECTED_UNITS),
                )
            else:
                assign(
                    "driver_id",
                    _canonical_declared_id(raw_row.get("driver_id"), allowed_scenario_driver_ids),
                    sorted(allowed_scenario_driver_ids or {"canonical_profile_driver_id"}),
                )
                assign(
                    "impact_metric",
                    _canonical_vocabulary_token(
                        raw_row.get("impact_metric"),
                        CANONICAL_IMPACT_METRICS,
                        aliases=_IMPACT_METRIC_ALIASES,
                    ),
                    sorted(CANONICAL_IMPACT_METRICS),
                )
            canonical_rows.append(row)
        normalized[collection_name] = canonical_rows
    return normalized, issues


def _exact_dimension_member(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row.get("dimension_id") or ""), str(row.get("member") or "")


def _revenue_route_context(row: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    dimension_id, member = _exact_dimension_member(row)
    return (
        str(row.get("scenario_id") or ""),
        CANONICAL_REVENUE_OUTPUT_METRIC,
        str(row.get("horizon") or ""),
        dimension_id,
        member,
    )


def _route_business_key(kind: str, index: int, row: Mapping[str, Any]) -> str:
    dimension_id, member = _exact_dimension_member(row)
    identity = str(row.get("assumption_id") or row.get("driver_id") or "")
    propagation = str(row.get("propagation_rule") or "")
    pack_id = str(row.get("profile_pack_id") or "")
    source_ref = str(row.get("source_ref") or "")
    return (
        f"{kind}[{index}]::{row.get('scenario_id') or ''}:{identity}:"
        f"{row.get('horizon') or ''}:{dimension_id}:{member}:{propagation}:{pack_id}"
        f" source_ref={source_ref!r}"
    )
def _valid_iso_date(value: Any) -> bool:
    candidate = str(value or "")
    if not _ISO_DATE_RE.fullmatch(candidate):
        return False
    try:
        date.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def _post_normalization_token_issues(
    original: Mapping[str, Any],
    canonical: Mapping[str, Any],
) -> list[ScenarioContractIssue]:
    issues: list[ScenarioContractIssue] = []
    fields_by_collection = {
        "scenario_items": (
            "scenario_id",
            "assumption_id",
            "horizon",
            "dimension_id",
            "member",
            "profile_pack_id",
            "propagation_rule",
            "unit",
        ),
        "scenario_driver_bridge": (
            "scenario_id",
            "driver_id",
            "impact_metric",
            "horizon",
            "dimension_id",
            "member",
            "profile_pack_id",
            "propagation_rule",
            "unit",
        ),
    }
    for collection, fields in fields_by_collection.items():
        raw_rows = original.get(collection)
        canonical_rows = canonical.get(collection)
        if not isinstance(raw_rows, list) or not isinstance(canonical_rows, list):
            continue
        for index, (raw_row, canonical_row) in enumerate(zip(raw_rows, canonical_rows, strict=False)):
            if not isinstance(raw_row, Mapping) or not isinstance(canonical_row, Mapping):
                continue
            for field in fields:
                if raw_row.get(field) == canonical_row.get(field):
                    continue
                issues.append(
                    ScenarioContractIssue(
                        f"investment_case.{collection}.{index}.{field}",
                        "scenario_route_token_not_canonical",
                        f"Downstream scenario contracts require canonical {field!r}; "
                        f"received {raw_row.get(field)!r}, expected {canonical_row.get(field)!r}, "
                        f"source_ref={raw_row.get('source_ref')!r}.",
                    )
                )
    return issues


def validate_scenario_contract(
    investment_case: Mapping[str, Any],
    *,
    allowed_profile_pack_ids: set[str] | None = None,
    allowed_scenario_driver_ids: set[str] | None = None,
    allowed_scenario_driver_map: Mapping[str, set[str]] | None = None,
    allowed_dimension_ids: set[str] | None = None,
    authoritative_as_of_date: str | None = None,
    tokens_are_canonical: bool = False,
) -> list[ScenarioContractIssue]:
    """Validate post-normalization scenario assumptions and bridge rows."""

    issues: list[ScenarioContractIssue] = []
    if not tokens_are_canonical:
        canonical_view, token_issues = canonicalize_scenario_contract(
            investment_case,
            allowed_profile_pack_ids=allowed_profile_pack_ids,
            allowed_scenario_driver_ids=allowed_scenario_driver_ids,
            allowed_dimension_ids=allowed_dimension_ids,
        )
        issues.extend(token_issues)
        issues.extend(_post_normalization_token_issues(investment_case, canonical_view))
    items = investment_case.get("scenario_items")
    bridges = investment_case.get("scenario_driver_bridge")
    if not isinstance(items, list):
        return [ScenarioContractIssue("investment_case.scenario_items", "scenario_items_missing", "scenario_items must be an array.")]
    if not isinstance(bridges, list):
        return [ScenarioContractIssue("investment_case.scenario_driver_bridge", "scenario_bridge_missing", "scenario_driver_bridge must be an array.")]

    seen_items: set[tuple[str, str, str, str, str, str, str]] = set()
    active_contexts: set[tuple[str, str, str, str, str]] = set()
    revenue_routes: dict[tuple[str, str, str, str, str], list[tuple[int, Mapping[str, Any]]]] = {}
    for index, row in enumerate(items):
        path = f"investment_case.scenario_items.{index}"
        if not isinstance(row, Mapping):
            issues.append(ScenarioContractIssue(path, "scenario_item_invalid", "Scenario item must be an object."))
            continue
        scenario_id = str(row.get("scenario_id") or "")
        assumption_id = str(row.get("assumption_id") or "")
        dimension_id, member = _exact_dimension_member(row)
        propagation = str(row.get("propagation_rule") or "")
        profile_pack_id = str(row.get("profile_pack_id") or "")
        key = (
            scenario_id,
            assumption_id,
            str(row.get("horizon") or ""),
            dimension_id,
            member,
            propagation,
            profile_pack_id,
        )
        if key in seen_items:
            issues.append(ScenarioContractIssue(path, "duplicate_scenario_item", f"Duplicate scenario item {key!r}."))
        seen_items.add(key)
        if scenario_id not in SCENARIO_IDS:
            issues.append(ScenarioContractIssue(path + ".scenario_id", "scenario_id_invalid", f"Unsupported scenario_id {scenario_id!r}."))
        if not assumption_id:
            issues.append(ScenarioContractIssue(path + ".assumption_id", "scenario_assumption_id_missing", "assumption_id is required."))
        unit = str(row.get("unit") or "")
        expected_unit = EXPECTED_UNITS.get(assumption_id)
        if expected_unit and unit != expected_unit:
            issues.append(ScenarioContractIssue(path + ".unit", "scenario_unit_mismatch", f"{assumption_id!r} requires {expected_unit!r}, got {unit!r}."))
        horizon = str(row.get("horizon") or "")
        if not _HORIZON_RE.fullmatch(horizon):
            issues.append(ScenarioContractIssue(path + ".horizon", "scenario_horizon_invalid", f"Unsupported horizon {horizon!r}."))
        horizon_class = metric_horizon_class(assumption_id)
        if horizon_class is None:
            issues.append(
                ScenarioContractIssue(
                    path + ".horizon",
                    "scenario_metric_horizon_contract_missing",
                    f"Scenario/business key {_route_business_key('item', index, row)} has no metric-horizon contract for {assumption_id!r}.",
                )
            )
        elif str(row.get("status") or "") == "populated" and not metric_horizon_is_compatible(assumption_id, horizon):
            issues.append(
                ScenarioContractIssue(
                    path + ".horizon",
                    "scenario_metric_horizon_mismatch",
                    f"Scenario/business key {_route_business_key('item', index, row)} received horizon {horizon!r}; "
                    f"metric {assumption_id!r} requires class {horizon_class!r} for output {assumption_id!r}.",
                )
            )
        classification = str(row.get("source_classification") or "")
        if classification not in SOURCE_CLASSIFICATIONS:
            issues.append(ScenarioContractIssue(path + ".source_classification", "scenario_source_classification_invalid", f"Unsupported source classification {classification!r}."))
        if propagation not in PROPAGATION_RULES:
            issues.append(ScenarioContractIssue(path + ".propagation_rule", "scenario_propagation_rule_invalid", f"Unsupported propagation rule {propagation!r}."))
        value_kind = str(row.get("value_kind") or "")
        if value_kind not in VALUE_KINDS:
            issues.append(ScenarioContractIssue(path + ".value_kind", "scenario_value_kind_invalid", f"Unsupported value_kind {value_kind!r}."))
        status = str(row.get("status") or "")
        value = row.get("value")
        if status not in FIELD_STATUSES:
            issues.append(ScenarioContractIssue(path + ".status", "scenario_status_invalid", f"Unsupported status {status!r}."))
        low_value = row.get("low_value")
        high_value = row.get("high_value")
        is_profile_selector = (
            assumption_id == CANONICAL_REVENUE_OUTPUT_METRIC
            and propagation == CANONICAL_PROFILE_REVENUE_PROPAGATION
        )
        if status == "populated":
            if value_kind == "range":
                if not (_finite_number(low_value) and _finite_number(high_value)):
                    issues.append(ScenarioContractIssue(path, "scenario_range_values_missing", "A populated range requires numeric low_value and high_value."))
                elif float(low_value) > float(high_value):
                    issues.append(ScenarioContractIssue(path, "scenario_range_order_invalid", "low_value cannot exceed high_value."))
                if value not in (None, ""):
                    issues.append(ScenarioContractIssue(path + ".value", "scenario_range_point_conflict", "A range cannot also carry a point value."))
            elif value_kind == "route":
                if not is_profile_selector:
                    issues.append(ScenarioContractIssue(path + ".value_kind", "scenario_route_value_kind_invalid", "Only a profile-driver Revenue selector may use value_kind route."))
                if any(raw not in (None, "") for raw in (value, low_value, high_value)):
                    issues.append(ScenarioContractIssue(path + ".value", "scenario_profile_route_value_forbidden", "A profile-driver route selector carries identity only and cannot carry a competing numeric value."))
            elif value_kind in {"point", "minimum", "maximum"} and not _finite_number(value) and assumption_id not in {"per_share_mode", "tax_treatment", "interest_tax_treatment", "scenario_horizon"}:
                issues.append(ScenarioContractIssue(path + ".value", "scenario_populated_value_missing", "A populated point or threshold requires a numeric value."))
            elif value_kind == "unavailable":
                issues.append(ScenarioContractIssue(path + ".value_kind", "scenario_populated_unavailable", "A populated item cannot use value_kind unavailable."))
        elif any(raw not in (None, "") for raw in (value, low_value, high_value)):
            issues.append(ScenarioContractIssue(path, "scenario_missing_status_has_value", "A non-populated scenario item cannot carry point or range values."))
        if value_kind == "unavailable" and status == "populated":
            issues.append(ScenarioContractIssue(path, "scenario_unavailable_populated", "Unavailable scenario items cannot be populated."))
        source_refs = [str(value) for value in row.get("source_refs") or [] if str(value)]
        if classification in {"source_actual", "source_guidance"} and status == "populated" and not source_refs:
            issues.append(ScenarioContractIssue(path + ".source_refs", "scenario_source_lineage_missing", "Source-backed assumptions require exact source_refs."))
        if (classification == "unavailable" or value_kind == "unavailable") and not str(row.get("reason") or ""):
            issues.append(ScenarioContractIssue(path + ".reason", "scenario_unavailable_reason_missing", "Unavailable assumptions require a reason."))
        if dimension_id != CANONICAL_TOTAL_COMPANY_TOKEN and not member:
            issues.append(ScenarioContractIssue(path + ".member", "scenario_dimension_member_missing", "A non-total-company dimension requires a member."))
        if allowed_dimension_ids is not None and dimension_id not in allowed_dimension_ids:
            issues.append(ScenarioContractIssue(path + ".dimension_id", "scenario_dimension_unknown", f"Dimension {dimension_id!r} is not enabled by the profile."))
        bounds = row.get("validation")
        if isinstance(bounds, Mapping) and _finite_number(value):
            minimum = bounds.get("minimum")
            maximum = bounds.get("maximum")
            if _finite_number(minimum) and float(value) < float(minimum):
                issues.append(ScenarioContractIssue(path + ".value", "scenario_value_below_minimum", f"Value {value!r} is below minimum {minimum!r}."))
            if _finite_number(maximum) and float(value) > float(maximum):
                issues.append(ScenarioContractIssue(path + ".value", "scenario_value_above_maximum", f"Value {value!r} is above maximum {maximum!r}."))

        if assumption_id == "per_share_mode" and status == "populated" and str(value or "") not in {"Outstanding", "Diluted"}:
            issues.append(ScenarioContractIssue(path + ".value", "scenario_per_share_mode_invalid", "per_share_mode must be Outstanding or Diluted."))
        if assumption_id == "tax_treatment" and status == "populated" and str(value or "") not in TAX_TREATMENTS:
            issues.append(ScenarioContractIssue(path + ".value", "scenario_tax_treatment_invalid", "tax_treatment value is unsupported."))
        if assumption_id == "interest_tax_treatment" and status == "populated" and str(value or "") not in {"taxable", "non_taxable"}:
            issues.append(ScenarioContractIssue(path + ".value", "scenario_interest_tax_treatment_invalid", "interest_tax_treatment must be taxable or non_taxable."))
        if classification == "source_actual" and status == "populated" and propagation != "shared_actual":
            issues.append(ScenarioContractIssue(path + ".propagation_rule", "scenario_actual_propagation_invalid", "Source actuals must use shared_actual propagation."))
        if classification == "source_actual" and status == "populated" and horizon_class == POINT_IN_TIME:
            as_of_date = str(row.get("as_of_date") or "")
            if not _valid_iso_date(as_of_date):
                issues.append(
                    ScenarioContractIssue(
                        path + ".as_of_date",
                        "scenario_point_in_time_date_missing",
                        f"Point-in-time metric {assumption_id!r} requires an explicit ISO as_of_date; source_ref={row.get('source_ref')!r}.",
                    )
                )
            elif authoritative_as_of_date and as_of_date != authoritative_as_of_date:
                issues.append(
                    ScenarioContractIssue(
                        path + ".as_of_date",
                        "scenario_point_in_time_date_mismatch",
                        f"Point-in-time metric {assumption_id!r} uses {as_of_date!r}, not authoritative as-of date {authoritative_as_of_date!r}; source_ref={row.get('source_ref')!r}.",
                    )
                )
        if classification == "unavailable" and propagation != "no_propagation":
            issues.append(ScenarioContractIssue(path + ".propagation_rule", "scenario_unavailable_propagation_invalid", "Unavailable inputs cannot propagate."))
        if profile_pack_id and allowed_profile_pack_ids is not None and profile_pack_id not in allowed_profile_pack_ids:
            issues.append(ScenarioContractIssue(path + ".profile_pack_id", "scenario_profile_pack_unknown", f"Profile pack {profile_pack_id!r} is not enabled."))
        if assumption_id == CANONICAL_REVENUE_OUTPUT_METRIC and status == CANONICAL_POPULATED_STATUS:
            if propagation not in {
                CANONICAL_DIRECT_REVENUE_PROPAGATION,
                CANONICAL_PROFILE_REVENUE_PROPAGATION,
                "no_propagation",
            }:
                issues.append(ScenarioContractIssue(path + ".propagation_rule", "scenario_revenue_route_invalid", "Revenue growth must use one selected growth route, one profile-driver route, or remain non-propagating."))
            if propagation == CANONICAL_DIRECT_REVENUE_PROPAGATION:
                if value_kind != CANONICAL_DIRECT_ROUTE_VALUE_KIND or not _finite_number(value):
                    issues.append(ScenarioContractIssue(path, "scenario_direct_revenue_value_invalid", "A direct-growth Revenue route requires one numeric point value."))
                if profile_pack_id:
                    issues.append(ScenarioContractIssue(path + ".profile_pack_id", "scenario_direct_revenue_pack_invalid", "A direct-growth route cannot name a profile pack."))
            elif propagation == CANONICAL_PROFILE_REVENUE_PROPAGATION:
                if value_kind != CANONICAL_PROFILE_ROUTE_VALUE_KIND:
                    issues.append(ScenarioContractIssue(path + ".value_kind", "scenario_profile_route_value_kind_invalid", "A profile-driver Revenue selector must use value_kind route."))
                if not profile_pack_id:
                    issues.append(ScenarioContractIssue(path + ".profile_pack_id", "scenario_profile_route_pack_missing", "A profile-driver Revenue selector requires profile_pack_id."))
        if (
            scenario_id != "common"
            and status == "populated"
            and propagation in {
                "scenario_specific",
                CANONICAL_DIRECT_REVENUE_PROPAGATION,
                CANONICAL_PROFILE_REVENUE_PROPAGATION,
            }
            and (dimension_id, member) == (CANONICAL_TOTAL_COMPANY_TOKEN, CANONICAL_TOTAL_COMPANY_TOKEN)
        ):
            context = _revenue_route_context(row)
            active_contexts.add(context)
            if assumption_id == CANONICAL_REVENUE_OUTPUT_METRIC and propagation in {
                CANONICAL_DIRECT_REVENUE_PROPAGATION,
                CANONICAL_PROFILE_REVENUE_PROPAGATION,
            }:
                revenue_routes.setdefault(context, []).append((index, row))

    seen_bridges: set[tuple[str, str, str, str, str, str, str, str]] = set()
    profile_revenue_bridges: list[tuple[int, Mapping[str, Any]]] = []
    for index, row in enumerate(bridges):
        path = f"investment_case.scenario_driver_bridge.{index}"
        if not isinstance(row, Mapping):
            issues.append(ScenarioContractIssue(path, "scenario_bridge_invalid", "Scenario bridge row must be an object."))
            continue
        dimension_id, member = _exact_dimension_member(row)
        key = (
            str(row.get("scenario_id") or ""),
            str(row.get("driver_id") or ""),
            str(row.get("horizon") or ""),
            dimension_id,
            member,
            str(row.get("impact_metric") or ""),
            str(row.get("profile_pack_id") or ""),
            str(row.get("propagation_rule") or ""),
        )
        if key in seen_bridges:
            issues.append(ScenarioContractIssue(path, "duplicate_scenario_bridge", f"Duplicate scenario bridge {key!r}."))
        seen_bridges.add(key)
        if key[0] not in SCENARIO_IDS:
            issues.append(ScenarioContractIssue(path + ".scenario_id", "scenario_id_invalid", f"Unsupported scenario_id {key[0]!r}."))
        if str(row.get("driver_type") or "") not in DRIVER_TYPES:
            issues.append(ScenarioContractIssue(path + ".driver_type", "scenario_driver_type_invalid", "Unsupported driver_type."))
        if str(row.get("tax_treatment") or "") not in TAX_TREATMENTS:
            issues.append(ScenarioContractIssue(path + ".tax_treatment", "scenario_tax_treatment_invalid", "Unsupported tax_treatment."))
        if str(row.get("cash_classification") or "") not in CASH_CLASSIFICATIONS:
            issues.append(ScenarioContractIssue(path + ".cash_classification", "scenario_cash_classification_invalid", "Unsupported cash_classification."))
        pack_id = str(row.get("profile_pack_id") or "")
        if allowed_profile_pack_ids is not None and pack_id and pack_id not in allowed_profile_pack_ids:
            issues.append(ScenarioContractIssue(path + ".profile_pack_id", "scenario_profile_pack_unknown", f"Profile pack {pack_id!r} is not enabled."))
        driver_id = str(row.get("driver_id") or "")
        if allowed_scenario_driver_ids is not None and driver_id not in allowed_scenario_driver_ids:
            issues.append(ScenarioContractIssue(path + ".driver_id", "scenario_driver_not_enabled", f"Scenario driver {driver_id!r} is not enabled by the selected profile packs."))
        if allowed_scenario_driver_map is not None and pack_id in allowed_scenario_driver_map and driver_id not in allowed_scenario_driver_map[pack_id]:
            issues.append(ScenarioContractIssue(path + ".driver_id", "scenario_driver_pack_mismatch", f"Scenario driver {driver_id!r} is not declared by profile pack {pack_id!r}."))
        if allowed_dimension_ids is not None and dimension_id not in allowed_dimension_ids:
            issues.append(ScenarioContractIssue(path + ".dimension_id", "scenario_dimension_unknown", f"Dimension {dimension_id!r} is not enabled by the profile."))
        classification = str(row.get("source_classification") or "")
        status = str(row.get("status") or "")
        if classification not in SOURCE_CLASSIFICATIONS:
            issues.append(ScenarioContractIssue(path + ".source_classification", "scenario_source_classification_invalid", "Unsupported source classification."))
        if status not in FIELD_STATUSES:
            issues.append(ScenarioContractIssue(path + ".status", "scenario_status_invalid", "Unsupported status."))
        if str(row.get("propagation_rule") or "") not in PROPAGATION_RULES:
            issues.append(ScenarioContractIssue(path + ".propagation_rule", "scenario_propagation_rule_invalid", "Unsupported propagation rule."))
        bridge_horizon = str(row.get("horizon") or "")
        if not _FISCAL_HORIZON_RE.fullmatch(bridge_horizon):
            issues.append(
                ScenarioContractIssue(
                    path + ".horizon",
                    "scenario_bridge_horizon_mismatch",
                    f"Scenario bridge {_route_business_key('bridge', index, row)} requires class {SCENARIO_HORIZON!r}; received {bridge_horizon!r}.",
                )
            )
        if (
            str(row.get("propagation_rule") or "") == CANONICAL_PROFILE_REVENUE_PROPAGATION
            and status == CANONICAL_POPULATED_STATUS
            and str(row.get("impact_metric") or "") == CANONICAL_REVENUE_OUTPUT_METRIC
        ):
            if str(row.get("unit") or "") != CANONICAL_REVENUE_UNIT:
                issues.append(ScenarioContractIssue(path + ".unit", "scenario_profile_revenue_unit_invalid", "A propagating profile revenue bridge must normalize to %."))
            profile_revenue_bridges.append((index, row))
        if status == "populated" and not _finite_number(row.get("value")):
            issues.append(ScenarioContractIssue(path + ".value", "scenario_bridge_value_invalid", "A populated scenario bridge requires a numeric value."))
        source_refs = [str(value) for value in row.get("source_refs") or [] if str(value)]
        if classification in {"source_actual", "source_guidance"} and status == "populated" and not source_refs:
            issues.append(ScenarioContractIssue(path + ".source_refs", "scenario_source_lineage_missing", "Source-backed bridge rows require exact source_refs."))
    matched_bridge_indices: set[int] = set()
    for context in sorted(active_contexts):
        routes = revenue_routes.get(context, [])
        if not routes:
            issues.append(
                ScenarioContractIssue(
                    "investment_case.scenario_items",
                    "scenario_revenue_route_missing",
                    f"Active scenario output {context!r} has no direct-growth or profile-driver Revenue route.",
                )
            )
            continue
        if len(routes) != 1:
            details = sorted(_route_business_key("item", index, row) for index, row in routes)
            issues.append(
                ScenarioContractIssue(
                    "investment_case.scenario_items",
                    "scenario_revenue_route_conflict",
                    f"Active scenario output {context!r} has {len(routes)} Revenue routes: {details!r}.",
                )
            )
            continue
        route_index, route = routes[0]
        if str(route.get("propagation_rule") or "") != CANONICAL_PROFILE_REVENUE_PROPAGATION:
            continue
        pack_id = str(route.get("profile_pack_id") or "")
        matching = [
            (bridge_index, bridge)
            for bridge_index, bridge in profile_revenue_bridges
            if _revenue_route_context(bridge) == context
            and str(bridge.get("profile_pack_id") or "") == pack_id
            and str(bridge.get("unit") or "") == CANONICAL_REVENUE_UNIT
        ]
        if len(matching) != 1:
            details = sorted(_route_business_key("bridge", index, row) for index, row in matching)
            issues.append(
                ScenarioContractIssue(
                    f"investment_case.scenario_items.{route_index}",
                    "scenario_profile_revenue_bridge_missing" if not matching else "scenario_profile_revenue_bridge_conflict",
                    f"Profile Revenue route {_route_business_key('item', route_index, route)} requires exactly one compatible bridge; matches={details!r}.",
                )
            )
            continue
        matched_bridge_indices.add(matching[0][0])
    for bridge_index, bridge in profile_revenue_bridges:
        if bridge_index not in matched_bridge_indices:
            issues.append(
                ScenarioContractIssue(
                    f"investment_case.scenario_driver_bridge.{bridge_index}",
                    "scenario_profile_revenue_route_missing",
                    f"Propagating bridge {_route_business_key('bridge', bridge_index, bridge)} has no single matching profile-driver Revenue route.",
                )
            )
    return issues


def resolve_revenue_growth(
    items: Sequence[Mapping[str, Any]],
    bridges: Sequence[Mapping[str, Any]],
    *,
    scenario_id: str,
    horizon: str,
) -> RevenueRouteResolution:
    """Resolve exactly one direct or profile-driver Revenue growth route."""

    candidates = [
        (index, row)
        for index, row in enumerate(items)
        if isinstance(row, Mapping)
        and str(row.get("scenario_id") or "") == scenario_id
        and str(row.get("assumption_id") or "") == CANONICAL_REVENUE_OUTPUT_METRIC
        and str(row.get("horizon") or "") == horizon
        and _exact_dimension_member(row) == (CANONICAL_TOTAL_COMPANY_TOKEN, CANONICAL_TOTAL_COMPANY_TOKEN)
        and str(row.get("status") or "") == CANONICAL_POPULATED_STATUS
        and str(row.get("propagation_rule") or "") in {
            CANONICAL_DIRECT_REVENUE_PROPAGATION,
            CANONICAL_PROFILE_REVENUE_PROPAGATION,
        }
    ]
    business_keys = tuple(sorted(_route_business_key("item", index, row) for index, row in candidates))
    if len(candidates) != 1:
        rule_id = "scenario_revenue_route_missing" if not candidates else "scenario_revenue_route_conflict"
        return RevenueRouteResolution(
            None,
            None,
            rule_id,
            f"Scenario {scenario_id!r} horizon {horizon!r} requires exactly one Revenue route; found {business_keys!r}.",
            business_keys,
        )

    route_index, route = candidates[0]
    mode = str(route.get("propagation_rule") or "")
    route_refs = tuple(sorted({str(value) for value in route.get("source_refs") or [] if str(value)}))
    if str(route.get("unit") or "") != CANONICAL_REVENUE_UNIT or not metric_horizon_is_compatible(
        CANONICAL_REVENUE_OUTPUT_METRIC,
        horizon,
    ):
        return RevenueRouteResolution(
            None,
            mode,
            "scenario_revenue_route_incompatible",
            f"Revenue route {_route_business_key('item', route_index, route)} has an incompatible unit or horizon.",
            business_keys,
            route_refs,
        )
    if mode == CANONICAL_DIRECT_REVENUE_PROPAGATION:
        value = route.get("value")
        if str(route.get("value_kind") or "") != "point" or not _finite_number(value) or str(route.get("profile_pack_id") or ""):
            return RevenueRouteResolution(
                None,
                mode,
                "scenario_direct_revenue_value_invalid",
                f"Direct Revenue route {_route_business_key('item', route_index, route)} is not one typed point assumption.",
                business_keys,
                route_refs,
            )
        return RevenueRouteResolution(float(value), "direct_growth", None, "", business_keys, route_refs)

    pack_id = str(route.get("profile_pack_id") or "")
    if (
        str(route.get("value_kind") or "") != "route"
        or route.get("value") not in (None, "")
        or not pack_id
    ):
        return RevenueRouteResolution(
            None,
            mode,
            "scenario_profile_route_selector_invalid",
            f"Profile Revenue route {_route_business_key('item', route_index, route)} must be a no-value selector with profile_pack_id.",
            business_keys,
            route_refs,
        )
    matching = [
        (index, row)
        for index, row in enumerate(bridges)
        if isinstance(row, Mapping)
        and str(row.get("scenario_id") or "") == scenario_id
        and str(row.get("profile_pack_id") or "") == pack_id
        and str(row.get("impact_metric") or "") == CANONICAL_REVENUE_OUTPUT_METRIC
        and str(row.get("unit") or "") == CANONICAL_REVENUE_UNIT
        and str(row.get("horizon") or "") == horizon
        and _exact_dimension_member(row) == (CANONICAL_TOTAL_COMPANY_TOKEN, CANONICAL_TOTAL_COMPANY_TOKEN)
        and str(row.get("propagation_rule") or "") == CANONICAL_PROFILE_REVENUE_PROPAGATION
        and str(row.get("status") or "") == CANONICAL_POPULATED_STATUS
    ]
    bridge_keys = tuple(sorted(_route_business_key("bridge", index, row) for index, row in matching))
    if len(matching) != 1:
        rule_id = "scenario_profile_revenue_bridge_missing" if not matching else "scenario_profile_revenue_bridge_conflict"
        return RevenueRouteResolution(
            None,
            "profile_driver",
            rule_id,
            f"Profile Revenue route {_route_business_key('item', route_index, route)} requires exactly one compatible bridge; found {bridge_keys!r}.",
            (*business_keys, *bridge_keys),
            route_refs,
        )
    bridge_index, bridge = matching[0]
    bridge_value = bridge.get("value")
    bridge_refs = tuple(sorted({str(value) for value in bridge.get("source_refs") or [] if str(value)}))
    if not _finite_number(bridge_value):
        return RevenueRouteResolution(
            None,
            "profile_driver",
            "scenario_profile_revenue_bridge_value_invalid",
            f"Profile bridge {_route_business_key('bridge', bridge_index, bridge)} has no numeric Revenue effect.",
            (*business_keys, *bridge_keys),
            tuple(sorted(set(route_refs + bridge_refs))),
        )
    return RevenueRouteResolution(
        float(bridge_value),
        "profile_driver",
        None,
        "",
        (*business_keys, *bridge_keys),
        tuple(sorted(set(route_refs + bridge_refs))),
    )


def evaluate_scenario_economics(
    items: Sequence[Mapping[str, Any]],
    *,
    scenario_id: str,
    horizon: str,
    bridges: Sequence[Mapping[str, Any]] = (),
    authoritative_as_of_date: str | None = None,
) -> dict[str, Any]:
    """Evaluate one scenario without implicit defaults or denominator fallbacks."""

    if scenario_id not in SCENARIO_IDS - {"common"}:
        raise ValueError(f"Unsupported scenario_id {scenario_id!r}.")
    rows = _scenario_rows(items, scenario_id)

    def value(assumption_id: str, *, actual: bool = False) -> float | None:
        row = _select_scenario_row(
            rows.get(assumption_id, ()),
            assumption_id=assumption_id,
            scenario_id=scenario_id,
            horizon=horizon,
            actual=actual,
            authoritative_as_of_date=authoritative_as_of_date,
        )
        if row is None:
            return None
        if str(row.get("unit") or "") != EXPECTED_UNITS[assumption_id]:
            return None
        raw = row.get("value")
        return float(raw) if _finite_number(raw) else None

    revenue_ttm = value("revenue_ttm", actual=True)
    base_ebitda_ttm = value("base_ebitda_ttm", actual=True)
    adjusted_ebitda_ttm = value("adjusted_ebitda_ttm", actual=True)
    fcf_ttm = value("fcf_ttm", actual=True)
    net_income_ttm = value("net_income_ttm", actual=True)
    per_share_mode_row = _select_control_row(rows.get("per_share_mode", ()), assumption_id="per_share_mode", scenario_id=scenario_id, horizon=horizon)
    per_share_mode = None
    if per_share_mode_row is not None and str(per_share_mode_row.get("status") or "") == "populated":
        candidate = str(per_share_mode_row.get("value") or "")
        if candidate in {"Outstanding", "Diluted"}:
            per_share_mode = candidate
    base_shares = value("shares_outstanding", actual=True) if per_share_mode == "Outstanding" else value("diluted_shares", actual=True) if per_share_mode == "Diluted" else None
    base_net_debt = value("net_debt", actual=True)
    price = value("price", actual=True)

    revenue_route = resolve_revenue_growth(items, bridges, scenario_id=scenario_id, horizon=horizon)
    growth = revenue_route.value
    base_margin = value("base_ebitda_margin")
    adjusted_margin = value("adjusted_ebitda_margin")
    pre_tax_bridge = value("pre_tax_earnings_bridge")
    tax_rate = value("tax_rate")
    tax_treatment_row = _select_control_row(rows.get("tax_treatment", ()), assumption_id="tax_treatment", scenario_id=scenario_id, horizon=horizon)
    tax_treatment = None
    if tax_treatment_row is not None and str(tax_treatment_row.get("status") or "") == "populated":
        candidate = str(tax_treatment_row.get("value") or "")
        if candidate in TAX_TREATMENTS:
            tax_treatment = candidate
    interest_change = value("cash_interest_change")
    interest_tax_treatment_row = _select_control_row(rows.get("interest_tax_treatment", ()), assumption_id="interest_tax_treatment", scenario_id=scenario_id, horizon=horizon)
    interest_tax_treatment = None
    if interest_tax_treatment_row is not None:
        candidate = str(interest_tax_treatment_row.get("value") or "")
        if candidate in {"taxable", "non_taxable"}:
            interest_tax_treatment = candidate
    capex_change = value("capital_expenditures_change")
    wc_adjustment = value("working_capital_adjustment")
    buyback_cash = value("buyback_cash")
    buyback_price = value("buyback_execution_price")
    share_issuance = value("share_issuance")
    debt_paydown = value("debt_paydown")
    target_adj = value("target_ev_adjusted_ebitda")

    scenario_revenue = _mul(revenue_ttm, None if growth is None else 1.0 + growth)
    scenario_base_ebitda = _mul(scenario_revenue, base_margin)
    scenario_adjusted_ebitda = _mul(scenario_revenue, adjusted_margin)

    after_tax_bridge = None
    if pre_tax_bridge is not None:
        if tax_treatment == "taxable" and tax_rate is not None and 0.0 <= tax_rate <= 1.0:
            after_tax_bridge = pre_tax_bridge * (1.0 - tax_rate)
        elif tax_treatment in {"non_taxable", "non_taxable_credit", "cash_only", "no_eps_impact"}:
            after_tax_bridge = pre_tax_bridge
    after_tax_interest = None
    if interest_change is not None:
        if interest_tax_treatment == "taxable" and tax_rate is not None and 0.0 <= tax_rate <= 1.0:
            after_tax_interest = interest_change * (1.0 - tax_rate)
        elif interest_tax_treatment == "non_taxable":
            after_tax_interest = interest_change

    scenario_fcf = None
    if None not in (fcf_ttm, after_tax_bridge, after_tax_interest, capex_change, wc_adjustment):
        scenario_fcf = float(fcf_ttm) + float(after_tax_bridge) - float(after_tax_interest) - float(capex_change) + float(wc_adjustment)

    scenario_shares = None
    if None not in (base_shares, buyback_cash, share_issuance):
        if buyback_cash == 0:
            scenario_shares = float(base_shares) + float(share_issuance)
        elif buyback_price is not None and buyback_price > 0:
            scenario_shares = float(base_shares) - float(buyback_cash) / float(buyback_price) + float(share_issuance)
        if scenario_shares is not None and scenario_shares <= 0:
            scenario_shares = None

    scenario_net_debt = None
    if None not in (base_net_debt, buyback_cash, debt_paydown):
        scenario_net_debt = float(base_net_debt) + float(buyback_cash) - float(debt_paydown)

    scenario_net_income = None
    earnings_bridge = 0.0 if tax_treatment in {"cash_only", "no_eps_impact"} else after_tax_bridge
    if None not in (net_income_ttm, earnings_bridge, after_tax_interest):
        scenario_net_income = float(net_income_ttm) + float(earnings_bridge) - float(after_tax_interest)
    scenario_eps = _div(scenario_net_income, scenario_shares)

    scenario_ev = _mul(target_adj, scenario_adjusted_ebitda)
    scenario_equity_value = None
    if scenario_ev is not None and scenario_net_debt is not None:
        scenario_equity_value = scenario_ev - scenario_net_debt
    implied_price = _div(scenario_equity_value, scenario_shares)
    upside_downside = None
    if implied_price is not None and price is not None and price > 0:
        upside_downside = implied_price / price - 1.0

    market_cap = _mul(implied_price, scenario_shares)
    pe = _div(implied_price, scenario_eps)
    ev_adjusted_ebitda = _div(scenario_ev, scenario_adjusted_ebitda)
    ev_base_ebitda = _div(scenario_ev, scenario_base_ebitda)
    ev_revenue = _div(scenario_ev, scenario_revenue)
    fcf_yield = _div(scenario_fcf, market_cap)
    return {
        "revenue_growth": growth,
        "revenue_route": revenue_route.route_mode,
        "revenue_route_issue": revenue_route.rule_id,
        "revenue": scenario_revenue,
        "base_ebitda": scenario_base_ebitda,
        "adjusted_ebitda": scenario_adjusted_ebitda,
        "fcf": scenario_fcf,
        "net_income": scenario_net_income,
        "eps": scenario_eps,
        "shares": scenario_shares,
        "net_debt": scenario_net_debt,
        "enterprise_value": scenario_ev,
        "equity_value": scenario_equity_value,
        "implied_price": implied_price,
        "upside_downside": upside_downside,
        "pe": pe,
        "ev_adjusted_ebitda": ev_adjusted_ebitda,
        "ev_base_ebitda": ev_base_ebitda,
        "ev_revenue": ev_revenue,
        "fcf_yield": fcf_yield,
    }


def evaluate_dcf(
    *,
    fcff: float | None,
    growth: float | None,
    terminal_growth: float | None,
    wacc: float | None,
    horizon_years: int | None,
    net_debt: float | None,
    shares: float | None,
) -> dict[str, float | None]:
    """Explicit-horizon growing-annuity DCF with a fail-closed equity bridge."""

    if None in (fcff, growth, terminal_growth, wacc, horizon_years) or not all(_finite_number(value) for value in (fcff, growth, terminal_growth, wacc, horizon_years)):
        return {"enterprise_value": None, "equity_value": None, "implied_price": None}
    fcff_f = float(fcff)
    growth_f = float(growth)
    terminal_f = float(terminal_growth)
    wacc_f = float(wacc)
    horizon = int(horizon_years)
    if float(horizon_years) != horizon or horizon < 1 or horizon > 20 or fcff_f <= 0 or wacc_f <= growth_f or wacc_f <= terminal_f or wacc_f <= 0:
        return {"enterprise_value": None, "equity_value": None, "implied_price": None}
    stage = fcff_f * (1 + growth_f) / (wacc_f - growth_f) * (1 - ((1 + growth_f) / (1 + wacc_f)) ** horizon)
    terminal = fcff_f * (1 + growth_f) ** horizon * (1 + terminal_f) / (wacc_f - terminal_f) / (1 + wacc_f) ** horizon
    enterprise_value = stage + terminal
    if net_debt is None or shares is None or not _finite_number(net_debt) or not _finite_number(shares) or float(shares) <= 0:
        return {"enterprise_value": enterprise_value, "equity_value": None, "implied_price": None}
    equity_value = enterprise_value - float(net_debt)
    return {"enterprise_value": enterprise_value, "equity_value": equity_value, "implied_price": equity_value / float(shares)}


def evaluate_market_implied(
    *,
    price: float | None,
    shares: float | None,
    net_debt: float | None,
    revenue_ttm: float | None,
    target_ev_revenue: float | None,
    target_ev_adjusted_ebitda: float | None,
    target_fcf_yield: float | None,
    target_pe: float | None = None,
    wacc: float | None = None,
    fcff: float | None = None,
) -> dict[str, float | None]:
    """Translate current market value into independently typed requirements."""

    market_cap = _mul(price, shares)
    enterprise_value = None if market_cap is None or net_debt is None else market_cap + float(net_debt)
    required_revenue = _div(enterprise_value, target_ev_revenue)
    required_adjusted_ebitda = _div(enterprise_value, target_ev_adjusted_ebitda)
    implied_margin = _div(required_adjusted_ebitda, required_revenue)
    implied_growth = None
    if required_revenue is not None and revenue_ttm is not None and revenue_ttm > 0:
        implied_growth = required_revenue / revenue_ttm - 1.0
    required_fcff = _mul(enterprise_value, target_fcf_yield)
    required_eps = _div(price, target_pe)
    implied_terminal_growth = None
    if None not in (enterprise_value, wacc, fcff) and float(enterprise_value) + float(fcff) != 0:
        implied_terminal_growth = (float(enterprise_value) * float(wacc) - float(fcff)) / (float(enterprise_value) + float(fcff))
    return {
        "market_cap": market_cap,
        "enterprise_value": enterprise_value,
        "required_revenue": required_revenue,
        "required_adjusted_ebitda": required_adjusted_ebitda,
        "implied_adjusted_ebitda_margin": implied_margin,
        "implied_revenue_growth": implied_growth,
        "required_fcff": required_fcff,
        "required_eps": required_eps,
        "implied_terminal_growth": implied_terminal_growth,
    }


def _scenario_rows(items: Sequence[Mapping[str, Any]], scenario_id: str) -> dict[str, tuple[Mapping[str, Any], ...]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in items:
        if not isinstance(row, Mapping) or str(row.get("scenario_id") or "") not in {"common", scenario_id}:
            continue
        if _exact_dimension_member(row) != ("total_company", "total_company"):
            continue
        grouped.setdefault(str(row.get("assumption_id") or ""), []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _select_scenario_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    assumption_id: str,
    scenario_id: str,
    horizon: str,
    actual: bool,
    authoritative_as_of_date: str | None,
) -> Mapping[str, Any] | None:
    expected_scenario = "common" if actual else scenario_id
    allowed_propagation = {"shared_actual"} if actual else {"scenario_specific", "selected_growth_route"}
    eligible = [
        row
        for row in rows
        if str(row.get("status") or "") == "populated"
        and str(row.get("scenario_id") or "") == expected_scenario
        and str(row.get("propagation_rule") or "") in allowed_propagation
        and str(row.get("source_classification") or "") != "unavailable"
        and metric_horizon_is_compatible(assumption_id, str(row.get("horizon") or ""))
        and (actual or str(row.get("horizon") or "") == horizon)
        and (
            not actual
            or metric_horizon_class(assumption_id) != POINT_IN_TIME
            or (
                _valid_iso_date(row.get("as_of_date"))
                and (
                    authoritative_as_of_date is None
                    or str(row.get("as_of_date") or "") == authoritative_as_of_date
                )
            )
        )
    ]
    return eligible[0] if len(eligible) == 1 else None


def _select_control_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    assumption_id: str,
    scenario_id: str,
    horizon: str,
) -> Mapping[str, Any] | None:
    eligible = [
        row
        for row in rows
        if str(row.get("status") or "") == "populated"
        and str(row.get("scenario_id") or "") == scenario_id
        and str(row.get("horizon") or "") == horizon
        and metric_horizon_is_compatible(assumption_id, str(row.get("horizon") or ""))
        and str(row.get("propagation_rule") or "") in {"scenario_specific", "selected_growth_route"}
        and str(row.get("source_classification") or "") != "unavailable"
    ]
    return eligible[0] if len(eligible) == 1 else None


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _mul(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left) * float(right)


def _div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return float(numerator) / float(denominator)
