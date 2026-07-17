from __future__ import annotations

from copy import deepcopy

import pytest

from pbi_xbrl.standard_template_formula_contract import _scenario_revenue_route_formula
from pbi_xbrl.valuation_scenario_economics import (
    CANONICAL_REVENUE_OUTPUT_METRIC,
    canonicalize_scenario_contract,
    evaluate_dcf,
    evaluate_market_implied,
    evaluate_scenario_economics,
    resolve_revenue_growth,
    validate_scenario_contract,
)


AS_OF_DATE = "2026-01-31"


def _item(
    assumption_id: str,
    value: float | str | None,
    unit: str,
    *,
    scenario_id: str = "base",
    horizon: str = "FY2026",
    source_classification: str = "user_input",
    propagation_rule: str = "scenario_specific",
    value_kind: str = "point",
    dimension_id: str = "total_company",
    member: str = "total_company",
    profile_pack_id: str | None = None,
    as_of_date: str | None = None,
    status: str = "populated",
) -> dict[str, object]:
    source_ref = "fixture:user" if source_classification == "user_input" else "fixture:source"
    return {
        "scenario_id": scenario_id,
        "assumption_id": assumption_id,
        "metric": assumption_id.replace("_", " "),
        "value_kind": value_kind,
        "value": value,
        "low_value": None,
        "high_value": None,
        "unit": unit,
        "horizon": horizon,
        "dimension_id": dimension_id,
        "member": member,
        "profile_pack_id": profile_pack_id,
        "as_of_date": as_of_date,
        "source_classification": source_classification,
        "validation": {"minimum": None, "maximum": None},
        "propagation_rule": propagation_rule,
        "status": status,
        "source_ref": source_ref,
        "source_refs": [source_ref],
        "reason": "" if status == "populated" else "Evidence is unavailable.",
    }


def _actual(assumption_id: str, value: float, unit: str, horizon: str) -> dict[str, object]:
    return _item(
        assumption_id,
        value,
        unit,
        scenario_id="common",
        horizon=horizon,
        source_classification="source_actual",
        propagation_rule="shared_actual",
        as_of_date=AS_OF_DATE if horizon == "as_of" else None,
    )


def _profile_route(
    *,
    scenario_id: str = "base",
    horizon: str = "FY2026",
    dimension_id: str = "total_company",
    member: str = "total_company",
    profile_pack_id: str = "retail_operating_pack",
) -> dict[str, object]:
    return _item(
        "revenue_growth",
        None,
        "%",
        scenario_id=scenario_id,
        horizon=horizon,
        propagation_rule="profile_driver_bridge",
        value_kind="route",
        dimension_id=dimension_id,
        member=member,
        profile_pack_id=profile_pack_id,
    )


def _profile_bridge(
    value: float = 0.10,
    *,
    scenario_id: str = "base",
    horizon: str = "FY2026",
    unit: str = "%",
    dimension_id: str = "total_company",
    member: str = "total_company",
    profile_pack_id: str = "retail_operating_pack",
) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "driver_id": "comparable_sales",
        "profile_pack_id": profile_pack_id,
        "driver_type": "revenue_volume",
        "metric": "Comparable sales",
        "impact_metric": "revenue_growth",
        "value": value,
        "unit": unit,
        "horizon": horizon,
        "dimension_id": dimension_id,
        "member": member,
        "source_classification": "derived_assumption",
        "tax_treatment": "manual_review_required",
        "cash_classification": "operating",
        "propagation_rule": "profile_driver_bridge",
        "status": "populated",
        "source_ref": "fixture:profile-bridge",
        "source_refs": ["fixture:profile-bridge"],
        "reason": "Explicit profile bridge.",
    }


def _complete_items(
    *,
    scenario_id: str = "base",
    growth: float = 0.10,
    adjusted_margin: float = 0.20,
) -> list[dict[str, object]]:
    return [
        _actual("price", 50.0, "$/share", "as_of"),
        _actual("shares_outstanding", 98.0, "m shares", "as_of"),
        _actual("diluted_shares", 100.0, "m shares", "2026-Q1"),
        _actual("net_debt", 200.0, "$m", "as_of"),
        _actual("revenue_ttm", 1_000.0, "$m", "TTM"),
        _actual("base_ebitda_ttm", 180.0, "$m", "TTM"),
        _actual("adjusted_ebitda_ttm", 200.0, "$m", "TTM"),
        _actual("fcf_ttm", 120.0, "$m", "TTM"),
        _actual("net_income_ttm", 100.0, "$m", "TTM"),
        _item("per_share_mode", "Diluted", "classification", scenario_id=scenario_id),
        _item("revenue_growth", growth, "%", scenario_id=scenario_id, propagation_rule="selected_growth_route"),
        _item("base_ebitda_margin", 0.18, "%", scenario_id=scenario_id),
        _item("adjusted_ebitda_margin", adjusted_margin, "%", scenario_id=scenario_id),
        _item("pre_tax_earnings_bridge", 20.0, "$m", scenario_id=scenario_id),
        _item("tax_rate", 0.25, "%", scenario_id=scenario_id),
        _item("tax_treatment", "taxable", "classification", scenario_id=scenario_id),
        _item("cash_interest_change", 4.0, "$m", scenario_id=scenario_id),
        _item("interest_tax_treatment", "taxable", "classification", scenario_id=scenario_id),
        _item("capital_expenditures_change", 10.0, "$m", scenario_id=scenario_id),
        _item("working_capital_adjustment", 5.0, "$m", scenario_id=scenario_id),
        _item("buyback_cash", 100.0, "$m", scenario_id=scenario_id),
        _item("buyback_execution_price", 50.0, "$/share", scenario_id=scenario_id),
        _item("share_issuance", 1.0, "m shares", scenario_id=scenario_id),
        _item("debt_paydown", 20.0, "$m", scenario_id=scenario_id),
        _item("target_ev_adjusted_ebitda", 10.0, "x", scenario_id=scenario_id),
    ]


def _profiled_items(*, route_value: float | None = None) -> list[dict[str, object]]:
    rows = [row for row in _complete_items(growth=0.05) if row["assumption_id"] != "revenue_growth"]
    route = _profile_route()
    route["value"] = route_value
    rows.append(route)
    return rows


def _canonical_case(
    items: list[dict[str, object]],
    bridges: list[dict[str, object]],
) -> tuple[dict[str, object], list[object]]:
    return canonicalize_scenario_contract(
        {"scenario_items": items, "scenario_driver_bridge": bridges},
        allowed_profile_pack_ids={"retail_operating_pack"},
        allowed_scenario_driver_ids={"comparable_sales", "revenue_growth"},
        allowed_dimension_ids={"total_company"},
    )


def _rule_ids(items: list[dict[str, object]], bridges: list[dict[str, object]] | None = None) -> set[str]:
    canonical, token_issues = _canonical_case(items, bridges or [])
    return {
        issue.rule_id
        for issue in [
            *token_issues,
            *validate_scenario_contract(
                canonical,
                allowed_profile_pack_ids={"retail_operating_pack"},
                allowed_scenario_driver_ids={"comparable_sales", "revenue_growth"},
                allowed_scenario_driver_map={"retail_operating_pack": {"comparable_sales"}},
                allowed_dimension_ids={"total_company"},
                authoritative_as_of_date=AS_OF_DATE,
            ),
        ]
    }


def _workbook_support_growth_oracle(
    items: list[dict[str, object]],
    bridges: list[dict[str, object]],
    *,
    scenario_id: str,
    horizon: str,
) -> float | None:
    """Independently apply the exact support-table masks used by the workbook."""

    direct = [
        row
        for row in items
        if row["scenario_id"] == scenario_id
        and row["assumption_id"] == "revenue_growth"
        and row["value_kind"] == "point"
        and row["unit"] == "%"
        and row["horizon"] == horizon
        and row["dimension_id"] == "total_company"
        and row["member"] == "total_company"
        and row["propagation_rule"] == "selected_growth_route"
        and row["status"] == "populated"
        and row["profile_pack_id"] is None
    ]
    profile = [
        row
        for row in items
        if row["scenario_id"] == scenario_id
        and row["assumption_id"] == "revenue_growth"
        and row["value_kind"] == "route"
        and row["value"] is None
        and row["unit"] == "%"
        and row["horizon"] == horizon
        and row["dimension_id"] == "total_company"
        and row["member"] == "total_company"
        and row["propagation_rule"] == "profile_driver_bridge"
        and row["status"] == "populated"
        and bool(row["profile_pack_id"])
    ]
    if len(direct) + len(profile) != 1:
        return None
    if direct:
        value = direct[0]["value"]
        return float(value) if isinstance(value, (int, float)) else None
    pack_id = profile[0]["profile_pack_id"]
    matches = [
        row
        for row in bridges
        if row["scenario_id"] == scenario_id
        and row["profile_pack_id"] == pack_id
        and row["impact_metric"] == CANONICAL_REVENUE_OUTPUT_METRIC
        and row["unit"] == "%"
        and row["horizon"] == horizon
        and row["dimension_id"] == "total_company"
        and row["member"] == "total_company"
        and row["propagation_rule"] == "profile_driver_bridge"
        and row["status"] == "populated"
    ]
    if len(matches) != 1:
        return None
    value = matches[0]["value"]
    return float(value) if isinstance(value, (int, float)) else None


def test_scenario_economics_and_buyback_bridge_match_independent_values() -> None:
    result = evaluate_scenario_economics(
        _complete_items(),
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )

    assert result["revenue"] == pytest.approx(1_100.0)
    assert result["base_ebitda"] == pytest.approx(198.0)
    assert result["adjusted_ebitda"] == pytest.approx(220.0)
    assert result["fcf"] == pytest.approx(127.0)
    assert result["shares"] == pytest.approx(99.0)
    assert result["net_debt"] == pytest.approx(280.0)
    assert result["net_income"] == pytest.approx(112.0)
    assert result["eps"] == pytest.approx(112.0 / 99.0)
    assert result["enterprise_value"] == pytest.approx(2_200.0)
    assert result["equity_value"] == pytest.approx(1_920.0)
    assert result["implied_price"] == pytest.approx(1_920.0 / 99.0)
    assert result["upside_downside"] == pytest.approx((1_920.0 / 99.0) / 50.0 - 1.0)


def test_direct_and_profile_routes_use_their_exact_economic_inputs() -> None:
    direct = evaluate_scenario_economics(
        _complete_items(growth=0.05),
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )
    profiled = evaluate_scenario_economics(
        _profiled_items(),
        bridges=[_profile_bridge(0.10)],
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )

    assert direct["revenue_route"] == "direct_growth"
    assert direct["revenue_growth"] == pytest.approx(0.05)
    assert direct["revenue"] == pytest.approx(1_050.0)
    assert profiled["revenue_route"] == "profile_driver"
    assert profiled["revenue_growth"] == pytest.approx(0.10)
    assert profiled["revenue"] == pytest.approx(1_100.0)


@pytest.mark.parametrize(
    ("assumption_alias", "impact_alias", "dimension_alias", "member_alias", "propagation_alias", "scenario_alias", "horizon_alias", "unit_alias", "pack_alias", "driver_alias"),
    (
        ("Revenue Growth", "Revenue Growth", "Total Company", "", "Profile Driver Bridge", "Base", "FY-2026", "percent", "Retail Operating Pack", "Comparable Sales"),
        ("revenue_growth", "revenue_growth", "total_company", "total_company", "profile_driver_bridge", "base", "FY2026", "%", "retail_operating_pack", "comparable_sales"),
        ("revenue-growth", "revenue-growth", "TOTAL-COMPANY", "Total-Company", "PROFILE-DRIVER-BRIDGE", "BASE", "FY 2026", "percentage", "retail-operating-pack", "comparable-sales"),
        ("REVENUE GROWTH", "REVENUE GROWTH", "", "default", "Profile Driver Bridge", "base", "FY_2026", "Percent", "retail operating pack", "comparable sales"),
        ("revenue_growth", "Revenue", "consolidated", "company", "profile driver bridge", "base", "FY2026", "%", "retail_operating_pack", "comparable_sales"),
    ),
)
def test_canonical_route_aliases_match_python_and_workbook_support_exactly(
    assumption_alias: str,
    impact_alias: str,
    dimension_alias: str,
    member_alias: str,
    propagation_alias: str,
    scenario_alias: str,
    horizon_alias: str,
    unit_alias: str,
    pack_alias: str,
    driver_alias: str,
) -> None:
    items = _profiled_items()
    route = next(row for row in items if row["assumption_id"] == "revenue_growth")
    route.update(
        assumption_id=assumption_alias,
        scenario_id=scenario_alias,
        horizon=horizon_alias,
        unit=unit_alias,
        dimension_id=dimension_alias,
        member=member_alias,
        propagation_rule=propagation_alias,
        profile_pack_id=pack_alias,
    )
    bridge = _profile_bridge(0.10)
    bridge.update(
        impact_metric=impact_alias,
        driver_id=driver_alias,
        scenario_id=scenario_alias,
        horizon=horizon_alias,
        unit=unit_alias,
        dimension_id=dimension_alias,
        member=member_alias,
        propagation_rule=propagation_alias,
        profile_pack_id=pack_alias,
    )

    raw_resolution = resolve_revenue_growth(items, [bridge], scenario_id="base", horizon="FY2026")
    raw_tokens_are_canonical = (
        assumption_alias == impact_alias == "revenue_growth"
        and dimension_alias == member_alias == "total_company"
        and propagation_alias == "profile_driver_bridge"
        and scenario_alias == "base"
        and horizon_alias == "FY2026"
        and unit_alias == "%"
        and pack_alias == "retail_operating_pack"
    )
    assert raw_resolution.value == (pytest.approx(0.10) if raw_tokens_are_canonical else None)

    canonical, token_issues = _canonical_case(items, [bridge])
    assert token_issues == []
    canonical_items = canonical["scenario_items"]
    canonical_bridges = canonical["scenario_driver_bridge"]
    canonical_route = next(row for row in canonical_items if row["propagation_rule"] == "profile_driver_bridge")
    canonical_bridge = canonical_bridges[0]
    assert (
        canonical_route["assumption_id"],
        canonical_route["scenario_id"],
        canonical_route["horizon"],
        canonical_route["unit"],
        canonical_route["dimension_id"],
        canonical_route["member"],
        canonical_route["profile_pack_id"],
    ) == ("revenue_growth", "base", "FY2026", "%", "total_company", "total_company", "retail_operating_pack")
    assert canonical_bridge["impact_metric"] == CANONICAL_REVENUE_OUTPUT_METRIC
    assert canonical_bridge["driver_id"] == "comparable_sales"

    contract_issues = validate_scenario_contract(
        canonical,
        allowed_profile_pack_ids={"retail_operating_pack"},
        allowed_scenario_driver_ids={"comparable_sales"},
        allowed_scenario_driver_map={"retail_operating_pack": {"comparable_sales"}},
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert contract_issues == []
    python_result = evaluate_scenario_economics(
        canonical_items,
        bridges=canonical_bridges,
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )
    workbook_growth = _workbook_support_growth_oracle(
        canonical_items,
        canonical_bridges,
        scenario_id="base",
        horizon="FY2026",
    )
    formula = _scenario_revenue_route_formula(
        scenario_id="base",
        user_growth="'{ticker}_Investment_Case'!$C$24",
        scenario_horizon="'{ticker}_Investment_Case'!$C$23",
    )
    assert '="revenue_growth"' in formula
    assert formula.count('="total_company"') == 4
    assert all(function not in formula for function in ("LOWER(", "TRIM(", "SUBSTITUTE("))
    assert workbook_growth == pytest.approx(0.10)
    assert python_result["revenue_growth"] == pytest.approx(workbook_growth)
    assert python_result["revenue"] == pytest.approx(1_100.0)
    reversed_case, reversed_issues = _canonical_case(list(reversed(items)), list(reversed([bridge])))
    assert reversed_issues == []
    reversed_result = evaluate_scenario_economics(
        reversed_case["scenario_items"],
        bridges=reversed_case["scenario_driver_bridge"],
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert reversed_result == python_result


@pytest.mark.parametrize(
    ("collection", "field", "bad_value"),
    (
        ("bridge", "impact_metric", "Revenue Velocity"),
        ("bridge", "dimension_id", "enterprise_scope"),
        ("bridge", "member", "all stores"),
        ("route", "assumption_id", "Revenue Momentum"),
        ("route", "profile_pack_id", "unknown retail pack"),
        ("route", "propagation_rule", "automatic"),
    ),
)
def test_unknown_route_aliases_fail_at_the_normalization_boundary(
    collection: str,
    field: str,
    bad_value: str,
) -> None:
    items = _profiled_items()
    bridges = [_profile_bridge()]
    row = bridges[0] if collection == "bridge" else next(
        item for item in items if item["assumption_id"] == "revenue_growth"
    )
    row[field] = bad_value

    _, issues = _canonical_case(items, bridges)
    matching = [issue for issue in issues if issue.field.endswith(f".{field}")]
    assert len(matching) == 1
    assert matching[0].rule_id == "scenario_route_token_unknown"
    assert repr(bad_value) in matching[0].message
    assert "source_ref=" in matching[0].message
    assert "affected_output=" in matching[0].message
    assert "accepted_canonical_vocabulary=" in matching[0].message


def test_profile_route_rejects_numeric_alias_and_missing_bridge() -> None:
    ambiguous = _profiled_items(route_value=0.05)
    assert "scenario_profile_route_value_forbidden" in _rule_ids(ambiguous, [_profile_bridge()])
    result = evaluate_scenario_economics(
        ambiguous,
        bridges=[_profile_bridge()],
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert result["revenue"] is None
    assert result["revenue_route_issue"] == "scenario_profile_route_selector_invalid"

    missing = evaluate_scenario_economics(
        _profiled_items(),
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert missing["revenue"] is None
    assert missing["revenue_route_issue"] == "scenario_profile_revenue_bridge_missing"


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("unit", "$m"),
        ("horizon", "FY2025"),
        ("dimension_id", "geography"),
        ("profile_pack_id", "commodity_operating_pack"),
    ),
)
def test_profile_bridge_requires_exact_unit_horizon_dimension_and_pack(field: str, replacement: str) -> None:
    bridge = _profile_bridge()
    bridge[field] = replacement
    resolution = resolve_revenue_growth(_profiled_items(), [bridge], scenario_id="base", horizon="FY2026")
    assert resolution.value is None
    assert resolution.rule_id == "scenario_profile_revenue_bridge_missing"


def test_direct_profile_conflicts_and_total_company_aliases_fail_deterministically() -> None:
    direct_and_profile = _complete_items(growth=0.05) + [_profile_route()]
    assert "scenario_revenue_route_conflict" in _rule_ids(direct_and_profile, [_profile_bridge()])

    aliases = _complete_items(growth=0.05)
    duplicate = deepcopy(next(row for row in aliases if row["assumption_id"] == "revenue_growth"))
    duplicate["dimension_id"] = ""
    duplicate["member"] = "Total Company"
    duplicate["source_ref"] = "fixture:alias"
    duplicate["source_refs"] = ["fixture:alias"]
    aliases.append(duplicate)
    canonical, token_issues = _canonical_case(aliases, [])
    assert token_issues == []
    issues = validate_scenario_contract(
        canonical,
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert {"duplicate_scenario_item", "scenario_revenue_route_conflict"} <= {issue.rule_id for issue in issues}
    conflict = next(issue for issue in issues if issue.rule_id == "scenario_revenue_route_conflict")
    assert "fixture:user" in conflict.message
    assert "fixture:alias" in conflict.message


def test_route_validation_and_output_are_independent_of_row_order() -> None:
    items = _profiled_items()
    bridges = [_profile_bridge(0.10)]
    forward_issues = sorted((issue.rule_id, issue.message) for issue in validate_scenario_contract(
        {"scenario_items": items, "scenario_driver_bridge": bridges},
        allowed_profile_pack_ids={"retail_operating_pack"},
        allowed_scenario_driver_ids={"comparable_sales"},
        allowed_scenario_driver_map={"retail_operating_pack": {"comparable_sales"}},
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    ))
    reverse_issues = sorted((issue.rule_id, issue.message) for issue in validate_scenario_contract(
        {"scenario_items": list(reversed(items)), "scenario_driver_bridge": list(reversed(bridges))},
        allowed_profile_pack_ids={"retail_operating_pack"},
        allowed_scenario_driver_ids={"comparable_sales"},
        allowed_scenario_driver_map={"retail_operating_pack": {"comparable_sales"}},
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    ))
    forward = evaluate_scenario_economics(items, bridges=bridges, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)
    reverse = evaluate_scenario_economics(list(reversed(items)), bridges=list(reversed(bridges)), scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)
    assert forward_issues == reverse_issues == []
    assert forward == reverse
    assert forward["revenue"] == pytest.approx(1_100.0)


def test_removing_selected_route_creates_an_explicit_gap() -> None:
    items = [row for row in _complete_items() if row["assumption_id"] != "revenue_growth"]
    assert "scenario_revenue_route_missing" in _rule_ids(items)
    result = evaluate_scenario_economics(items, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)
    assert result["revenue"] is None
    assert result["revenue_route_issue"] == "scenario_revenue_route_missing"


@pytest.mark.parametrize(
    ("assumption_id", "unit", "bad_horizon", "good_horizon"),
    (
        ("revenue_ttm", "$m", "as_of", "TTM"),
        ("base_ebitda_ttm", "$m", "as_of", "TTM"),
        ("net_debt", "$m", "TTM", "as_of"),
        ("price", "$/share", "TTM", "as_of"),
    ),
)
def test_metric_specific_actual_horizons_fail_closed(
    assumption_id: str,
    unit: str,
    bad_horizon: str,
    good_horizon: str,
) -> None:
    bad = _actual(assumption_id, 100.0, unit, bad_horizon)
    bad["as_of_date"] = AS_OF_DATE if bad_horizon == "as_of" else None
    good = _actual(assumption_id, 100.0, unit, good_horizon)
    bad_rules = _rule_ids([bad])
    good_rules = _rule_ids([good])
    assert "scenario_metric_horizon_mismatch" in bad_rules
    assert "scenario_metric_horizon_mismatch" not in good_rules


def test_point_in_time_actual_requires_the_authoritative_date() -> None:
    rows = _complete_items()
    price = next(row for row in rows if row["assumption_id"] == "price")
    price["as_of_date"] = "2026-01-30"
    assert "scenario_point_in_time_date_mismatch" in _rule_ids(rows)
    result = evaluate_scenario_economics(
        rows,
        scenario_id="base",
        horizon="FY2026",
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert result["upside_downside"] is None


def test_wrong_scenario_or_guidance_horizon_cannot_populate_revenue() -> None:
    rows = _complete_items()
    route = next(row for row in rows if row["assumption_id"] == "revenue_growth")
    route["horizon"] = "FY2025"
    route["source_classification"] = "source_guidance"
    route["source_ref"] = "fixture:guidance"
    route["source_refs"] = ["fixture:guidance"]
    rules = _rule_ids(rows)
    assert "scenario_revenue_route_missing" in rules
    result = evaluate_scenario_economics(rows, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)
    assert result["revenue"] is None
    assert result["revenue_route_issue"] == "scenario_revenue_route_missing"


def test_missing_actual_remains_missing_instead_of_becoming_zero() -> None:
    rows = _complete_items()
    revenue = next(row for row in rows if row["assumption_id"] == "revenue_ttm")
    revenue.update(
        value=None,
        value_kind="unavailable",
        status="missing_source",
        source_classification="unavailable",
        propagation_rule="no_propagation",
        reason="No trailing-period evidence.",
    )
    result = evaluate_scenario_economics(rows, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)
    assert result["revenue"] is None
    assert result["base_ebitda"] is None
    assert result["adjusted_ebitda"] is None


def test_scenario_outputs_fail_closed_for_missing_unit_horizon_and_propagation() -> None:
    missing_buyback_price = [row for row in _complete_items() if row["assumption_id"] != "buyback_execution_price"]
    assert evaluate_scenario_economics(missing_buyback_price, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)["shares"] is None

    wrong_unit = _complete_items()
    next(row for row in wrong_unit if row["assumption_id"] == "revenue_growth")["unit"] = "$m"
    assert evaluate_scenario_economics(wrong_unit, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)["revenue"] is None

    wrong_horizon = _complete_items()
    next(row for row in wrong_horizon if row["assumption_id"] == "adjusted_ebitda_margin")["horizon"] = "FY2025"
    assert evaluate_scenario_economics(wrong_horizon, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)["adjusted_ebitda"] is None

    no_propagation = _complete_items()
    growth = next(row for row in no_propagation if row["assumption_id"] == "revenue_growth")
    growth["source_classification"] = "source_guidance"
    growth["propagation_rule"] = "no_propagation"
    assert evaluate_scenario_economics(no_propagation, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)["revenue"] is None


def test_cash_only_bridge_changes_fcf_but_has_zero_eps_effect() -> None:
    rows = _complete_items()
    next(row for row in rows if row["assumption_id"] == "tax_treatment")["value"] = "cash_only"

    result = evaluate_scenario_economics(rows, scenario_id="base", horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)

    assert result["fcf"] == pytest.approx(132.0)
    assert result["net_income"] == pytest.approx(97.0)
    assert result["eps"] == pytest.approx(97.0 / 99.0)


def test_bear_base_bull_ordering_follows_declared_inputs() -> None:
    rows: list[dict[str, object]] = []
    for scenario_id, growth, margin, multiple in (
        ("bear", -0.05, 0.15, 7.0),
        ("base", 0.05, 0.19, 9.0),
        ("bull", 0.15, 0.23, 11.0),
    ):
        scenario_rows = _complete_items(scenario_id=scenario_id, growth=growth, adjusted_margin=margin)
        next(row for row in scenario_rows if row["assumption_id"] == "target_ev_adjusted_ebitda")["value"] = multiple
        if not rows:
            rows.extend(row for row in scenario_rows if row["scenario_id"] == "common")
        rows.extend(row for row in scenario_rows if row["scenario_id"] == scenario_id)

    prices = [
        evaluate_scenario_economics(rows, scenario_id=name, horizon="FY2026", authoritative_as_of_date=AS_OF_DATE)["implied_price"]
        for name in ("bear", "base", "bull")
    ]
    assert all(value is not None for value in prices)
    assert prices[0] < prices[1] < prices[2]


def test_dcf_requires_an_explicit_valid_horizon_and_matches_manual_formula() -> None:
    result = evaluate_dcf(
        fcff=100.0,
        growth=0.04,
        terminal_growth=0.02,
        wacc=0.10,
        horizon_years=7,
        net_debt=150.0,
        shares=50.0,
    )
    stage = 100.0 * 1.04 / 0.06 * (1 - (1.04 / 1.10) ** 7)
    terminal = 100.0 * 1.04**7 * 1.02 / 0.08 / 1.10**7
    assert result["enterprise_value"] == pytest.approx(stage + terminal)
    assert result["implied_price"] == pytest.approx((stage + terminal - 150.0) / 50.0)

    assert evaluate_dcf(fcff=100.0, growth=0.04, terminal_growth=0.02, wacc=0.10, horizon_years=None, net_debt=150.0, shares=50.0)["enterprise_value"] is None
    assert evaluate_dcf(fcff=100.0, growth=0.04, terminal_growth=0.02, wacc=0.10, horizon_years=0, net_debt=150.0, shares=50.0)["enterprise_value"] is None


def test_market_implied_requirements_match_independent_economics() -> None:
    result = evaluate_market_implied(
        price=40.0,
        shares=50.0,
        net_debt=300.0,
        revenue_ttm=2_000.0,
        target_ev_revenue=1.0,
        target_ev_adjusted_ebitda=10.0,
        target_fcf_yield=0.05,
        target_pe=20.0,
        wacc=0.10,
        fcff=100.0,
    )
    assert result["market_cap"] == pytest.approx(2_000.0)
    assert result["enterprise_value"] == pytest.approx(2_300.0)
    assert result["required_revenue"] == pytest.approx(2_300.0)
    assert result["required_adjusted_ebitda"] == pytest.approx(230.0)
    assert result["implied_adjusted_ebitda_margin"] == pytest.approx(0.10)
    assert result["implied_revenue_growth"] == pytest.approx(0.15)
    assert result["required_fcff"] == pytest.approx(115.0)
    assert result["required_eps"] == pytest.approx(2.0)
    assert result["implied_terminal_growth"] == pytest.approx((2_300.0 * 0.10 - 100.0) / 2_400.0)


def test_contract_rejects_mismatches_and_profile_driver_not_enabled() -> None:
    investment_case = {"scenario_items": _complete_items(), "scenario_driver_bridge": []}
    assert validate_scenario_contract(
        investment_case,
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    ) == []

    wrong = deepcopy(investment_case)
    next(row for row in wrong["scenario_items"] if row["assumption_id"] == "revenue_growth")["unit"] = "$m"
    assert "scenario_unit_mismatch" in {issue.rule_id for issue in validate_scenario_contract(wrong, authoritative_as_of_date=AS_OF_DATE)}

    bridge = _profile_bridge(0.03)
    investment_case["scenario_driver_bridge"] = [bridge]
    issues = validate_scenario_contract(
        investment_case,
        allowed_profile_pack_ids={"retail_operating_pack"},
        allowed_scenario_driver_ids={"revenue_growth"},
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert "scenario_driver_not_enabled" in {issue.rule_id for issue in issues}

    mismatch = validate_scenario_contract(
        investment_case,
        allowed_profile_pack_ids={"retail_operating_pack"},
        allowed_scenario_driver_ids={"revenue_growth", "comparable_sales"},
        allowed_scenario_driver_map={"retail_operating_pack": {"revenue_growth"}},
        allowed_dimension_ids={"total_company"},
        authoritative_as_of_date=AS_OF_DATE,
    )
    assert "scenario_driver_pack_mismatch" in {issue.rule_id for issue in mismatch}
