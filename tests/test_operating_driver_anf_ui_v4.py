from __future__ import annotations

from copy import deepcopy
import inspect
import json

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (
    APPROXIMATE_RANGE_DIRECTION_CONTRACT,
    FOOTPRINT_CONTEXT_RELATIONSHIP_CONTRACT,
    FOOTPRINT_DEFINITION_CONTRACT,
    FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT,
    INTERPRETATION_SUBSECTION,
    INVESTOR_LANGUAGE_CONTRACT,
    PLAN_ORIGIN,
    STORE_COUNT_PERIOD_COMPARISON_CONTRACT,
    VISIBLE_MAJOR_SECTIONS,
    assess_combined_store_activity_evidence,
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
    derive_company_owned_store_roll_forward,
    derive_inventory_approximate_range_comparison,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (
    build_operating_driver_anf_workbook_v4_plan,
)


@pytest.fixture(scope="module")
def completeness():
    return build_anf_operating_driver_full_completeness()


@pytest.fixture(scope="module")
def source(completeness) -> dict:
    return build_operating_driver_anf_ui_source_from_completeness(completeness)


@pytest.fixture(scope="module")
def package(source: dict, completeness):
    return build_operating_driver_anf_ui_v4(
        source,
        source_identity_receipts={
            "full_data_completeness_sha256": completeness.sha256,
            "registry_sha256": completeness.registry.sha256,
            "analytics_sha256": completeness.analytics.sha256,
            "semantics_sha256": completeness.semantics.sha256,
            "selection_sha256": completeness.selection.sha256,
        },
    )


def test_blank_surface_contract_has_exactly_three_sections(package) -> None:
    assert package.plan_origin == PLAN_ORIGIN == "BLANK_SURFACE_V4"
    assert package.major_sections == VISIBLE_MAJOR_SECTIONS == (
        "Operating Drivers Overview",
        "Core Drivers",
        "Quarterly Driver History",
    )
    assert package.latest_period_label == "2026-Q1"


def test_investor_language_contract_is_text_first_and_current(package) -> None:
    assert package.language_contract == INVESTOR_LANGUAGE_CONTRACT == "operating-drivers-investor-language@3"
    assert [item.subsection for item in package.overview] == [
        INTERPRETATION_SUBSECTION,
        INTERPRETATION_SUBSECTION,
        INTERPRETATION_SUBSECTION,
        "LATEST QUARTER",
        "LATEST QUARTER",
        "LATEST QUARTER",
        "LATEST QUARTER",
        "BROADER TREND",
        "BROADER TREND",
        "BROADER TREND",
    ]
    assert all(item.source_references for item in package.overview)
    text = " ".join(item.text for item in package.overview)
    assert "Net sales rose 2%" in text
    assert "comparable sales fell 1%" in text
    assert "Americas stayed positive" in text
    assert "APAC accelerated" in text
    assert "EMEA weakened sharply" in text
    assert "829 to 834" in text
    assert "Inventory at cost declined 2%" in text
    assert "low single digits" in text
    assert "Underlying demand has slowed sharply" in text
    assert "Regional demand remained uneven" in text


def test_overview_uses_plain_language_without_internal_or_management_ownership(package) -> None:
    text = "\n".join(item.text for item in package.overview)
    forbidden = (
        "READY_NUMERIC",
        "CONTEXT_DEPENDENT",
        "current evidence is",
        "analysis:",
        "observation:",
        "management said",
        "management expects",
        "forecast",
        "should reach",
    )
    assert not any(value.casefold() in text.casefold() for value in forbidden)
    assert all(";" not in item.text for item in package.overview)


def test_core_driver_contract_is_current_plain_and_has_no_sparklines(package) -> None:
    assert [item.label for item in package.core_drivers] == [
        "Total company",
        "Net sales growth",
        "EMEA",
        "APAC",
        "Company-owned stores",
        "Inventory at cost",
        "Inventory cost growth",
        "Inventory units",
    ]
    assert all(not item.sparkline_eligible for item in package.core_drivers)
    assert [item.latest_display for item in package.core_drivers[:4]] == ["-1%", "+2%", "-11%", "+15%"]
    assert package.core_drivers[4].latest_display == "834 stores"
    assert package.core_drivers[5].latest_display == "$532.7m"
    assert package.core_drivers[6].latest_display == "-2%"
    assert package.core_drivers[7].latest_display == "Approx. low-single-digit YoY"
    assert {item.trend_fallback_display for item in package.core_drivers} <= {
        "Improving", "Slowing", "Mixed", "Stable", "Expanding", "Contracting", "Accelerating",
        "Cost pressure easing", "Not comparable", "Needs history", "Recent growth moderating"
    }


def test_core_rate_comparisons_use_percentage_points_and_other_units_are_explicit(package) -> None:
    assert package.core_drivers[0].qoq_display == "-2 pp"
    assert package.core_drivers[0].yoy_display == "-5 pp"
    assert package.core_drivers[1].qoq_display == "-3 pp"
    assert package.core_drivers[4].qoq_display == "+5 stores"
    assert package.core_drivers[4].yoy_display == "+41 stores"
    assert package.core_drivers[4].yoy_value == "41"
    assert package.core_drivers[4].yoy_status == "AVAILABLE"
    assert package.core_drivers[4].yoy_comparison_contract == STORE_COUNT_PERIOD_COMPARISON_CONTRACT
    assert package.core_drivers[4].yoy_lineage_references
    assert package.core_drivers[5].qoq_display == "-$68.5m"
    assert package.core_drivers[6].yoy_display == "-22.7 pp"
    assert [item.latest_value for item in package.core_drivers[:4]] == ["-1", "2", "-11", "15"]
    assert package.core_drivers[0].qoq_value == "-2"
    assert package.core_drivers[5].qoq_value == "-68.527"
    assert package.core_drivers[7].latest_value is None
    assert package.core_drivers[7].qoq_value is None
    assert package.core_drivers[7].qoq_display == "Down from mid-single-digit"
    assert package.core_drivers[7].qoq_status == "AVAILABLE_ORDINAL"
    assert package.core_drivers[7].yoy_status == "UNAVAILABLE_NOT_DISCLOSED"
    assert package.core_drivers[7].trend_fallback_display == "Recent growth moderating"
    plan = build_operating_driver_anf_workbook_v4_plan(package)
    note = next(item.display_value for item in plan.bindings if item.semantic_id == "history-note")
    assert note.startswith("pp = percentage points")


def test_history_has_one_shared_12q_header_and_only_supported_groups(package) -> None:
    assert package.quarter_labels == (
        "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1",
        "2024-Q2", "2024-Q3", "2024-Q4", "2025-Q1",
        "2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1",
    )
    assert list(dict.fromkeys(item.group_label for item in package.history_rows)) == [
        "Demand / Sales", "Inventory", "Store Footprint"
    ]
    assert len(package.history_rows) == 15
    assert not any(item.group_label == "Channel / Mix" for item in package.history_rows)


def test_direct_q4_and_q1_comparables_are_visible(package) -> None:
    rows = {(item.driver_id, item.dimension_member_id): item for item in package.history_rows}
    expected = {
        "member:operating-driver:total-company@1": ("1", "-1"),
        "member:operating-driver:americas@1": ("2", "1"),
        "member:operating-driver:emea@1": ("-3", "-11"),
        "member:operating-driver:apac@1": ("0", "15"),
        "member:operating-driver:abercrombie@1": ("-1", "0"),
        "member:operating-driver:hollister@1": ("3", "-2"),
    }
    for member_id, (q4, q1) in expected.items():
        points = rows[("driver:operating:comparable-sales@1", member_id)].points
        assert points[-2].value == q4
        assert points[-1].value == q1
        assert points[-2].source_observation_id and points[-1].source_observation_id


def test_store_activity_history_uses_safe_quarter_values(package) -> None:
    rows = {item.driver_id: item for item in package.history_rows if item.group_label == "Store Footprint"}
    expected = {
        "driver:operating:new-stores@1": ("7", "19", "22", "14", "6"),
        "driver:operating:remodeled-stores@1": ("9", "7", "8", "23", "24"),
        "driver:operating:right-sized-stores@1": ("1", "4", "3", "3", "2"),
        "driver:operating:closed-stores@1": ("3", "5", "2", "12", "1"),
    }
    for driver_id, values in expected.items():
        assert tuple(point.value for point in rows[driver_id].points[-5:]) == values


def test_history_preserves_missing_as_blank_not_zero(package) -> None:
    missing = [
        point
        for item in package.history_rows
        for point in item.points
        if point.value is None and point.display_value == ""
    ]
    assert missing
    assert all(point.source_observation_id is None for point in missing)


def test_inventory_cost_history_consumes_complete_exact_12q_series(package) -> None:
    rows = {item.label: item for item in package.history_rows}
    assert all(point.value is not None for point in rows["Inventory at cost ($m)"].points)
    assert all(point.value is not None for point in rows["Inventory cost growth (YoY)"].points)
    assert rows["Inventory at cost ($m)"].points[-1].value == "532.691"
    assert rows["Inventory cost growth (YoY)"].points[-1].value == "-2"


def test_net_sales_context_and_inventory_units_history_are_fail_closed(package) -> None:
    rows = {item.label: item for item in package.history_rows}
    assert [point.value for point in rows["Net sales growth"].points][-3:] == ["7", "5", "2"]
    assert rows["Net sales growth"].display_role == "OWNER_ELSEWHERE_CONTEXT"
    unit_points = rows["Inventory units (YoY)"].points
    exact = [point for point in unit_points if point.value is not None]
    assert [(point.period_label, point.value) for point in exact] == [("2025-Q2", "7")]
    assert unit_points[-1].value is None and unit_points[-1].display_value == "Up low-single"


def test_approximate_inventory_units_and_digital_context_do_not_become_numeric_history(package, completeness) -> None:
    latest_units = next(
        item for item in completeness.observation_registry
        if item.metric_label == "Inventory unit growth" and item.period_label == "2026-Q1"
    )
    assert latest_units.value is None
    assert latest_units.precision.value == "APPROXIMATE"
    units_row = next(item for item in package.history_rows if item.driver_id == latest_units.canonical_driver_id)
    assert units_row.points[-1].value is None
    approximate = {
        point.period_label: point.display_value
        for point in units_row.points
        if point.precision == "APPROXIMATE"
    }
    assert approximate == {
        "2024-Q4": "Up mid-single",
        "2025-Q3": "Up ~1%",
        "2025-Q4": "Up mid-single",
        "2026-Q1": "Up low-single",
    }
    digital_44 = next(
        item for item in completeness.observation_registry
        if item.metric_label == "Digital sales mix" and item.value == "44"
    )
    assert digital_44.period_label == "FY2025"
    assert digital_44.period_basis.value == "FY_ACTUAL"
    assert not any(item.driver_id == digital_44.canonical_driver_id for item in package.history_rows)


def test_store_count_roll_forward_reconciles_every_direct_anchor(source: dict) -> None:
    records = derive_company_owned_store_roll_forward(source)
    by_period = {item.period_label: item for item in records}
    assert {period: by_period[period].value for period in (
        "2023-Q2", "2023-Q3", "2024-Q1", "2024-Q2", "2024-Q3",
        "2025-Q1", "2025-Q2", "2025-Q3",
    )} == {
        "2023-Q2": "759", "2023-Q3": "765",
        "2024-Q1": "753", "2024-Q2": "757", "2024-Q3": "773",
        "2025-Q1": "793", "2025-Q2": "807", "2025-Q3": "827",
    }
    assert {
        period: by_period[period].direct_anchor_match
        for period in ("2023-Q4", "2024-Q4", "2025-Q4", "2026-Q1")
    } == {period: True for period in ("2023-Q4", "2024-Q4", "2025-Q4", "2026-Q1")}
    assert all(item.lineage_references for item in records)


def test_company_owned_store_yoy_is_typed_and_not_hardcoded(source: dict, completeness) -> None:
    receipts = {
        "full_data_completeness_sha256": completeness.sha256,
        "registry_sha256": completeness.registry.sha256,
        "analytics_sha256": completeness.analytics.sha256,
        "semantics_sha256": completeness.semantics.sha256,
        "selection_sha256": completeness.selection.sha256,
    }
    original = build_operating_driver_anf_ui_v4(source, source_identity_receipts=receipts)
    original_store = next(item for item in original.core_drivers if item.core_id == "company-owned-stores")
    assert original_store.yoy_value == "41"

    mutated = deepcopy(source)
    q1_new = next(
        item for item in mutated["completeness"]["facts"]
        if item["metric_label"] == "New stores" and item["period_label"] == "2025-Q1"
    )
    q2_new = next(
        item for item in mutated["completeness"]["facts"]
        if item["metric_label"] == "New stores" and item["period_label"] == "2025-Q2"
    )
    q1_new["value"] = str(int(q1_new["value"]) + 1)
    q2_new["value"] = str(int(q2_new["value"]) - 1)
    changed = build_operating_driver_anf_ui_v4(mutated, source_identity_receipts=receipts)
    changed_store = next(item for item in changed.core_drivers if item.core_id == "company-owned-stores")
    assert changed_store.yoy_value == "40"
    assert changed_store.yoy_display == "+40 stores"


def test_store_count_roll_forward_rejects_anchor_mismatch(source: dict) -> None:
    mutated = deepcopy(source)
    fact = next(
        item for item in mutated["completeness"]["facts"]
        if item["metric_label"] == "Company-owned stores, end"
        and item["period_label"] == "2024-Q4"
    )
    fact["value"] = "790"
    with pytest.raises(ValueError, match="anchor mismatch"):
        derive_company_owned_store_roll_forward(mutated)


def test_store_count_roll_forward_rejects_population_dimension_mismatch(source: dict) -> None:
    mutated = deepcopy(source)
    fact = next(
        item for item in mutated["completeness"]["facts"]
        if item["metric_label"] == "New stores" and item["period_label"] == "2024-Q2"
    )
    fact["dimension_member_ids"] = ["member:operating-driver:franchise@1"]
    with pytest.raises(ValueError, match="Incompatible New stores fact"):
        derive_company_owned_store_roll_forward(mutated)


def test_direct_store_count_is_authoritative_and_derivation_is_reconciliation_support(package) -> None:
    row = next(item for item in package.history_rows if item.label == "Company-owned stores")
    by_period = {point.period_label: point for point in row.points}
    assert by_period["2024-Q4"].source_observation_id.startswith("observation:")
    assert by_period["2024-Q4"].derivation_id.startswith("derivation:")
    assert by_period["2024-Q1"].source_observation_id == by_period["2024-Q1"].derivation_id
    assert all(by_period[period].lineage_references for period in by_period)


def test_inventory_approximate_comparison_is_ordinal_only(source: dict, package) -> None:
    comparison = derive_inventory_approximate_range_comparison(
        source, current_period="2026-Q1", prior_period="2025-Q4"
    )
    assert comparison.contract_version == APPROXIMATE_RANGE_DIRECTION_CONTRACT
    assert comparison.current_category == "LOW_SINGLE_DIGIT"
    assert comparison.prior_category == "MID_SINGLE_DIGIT"
    assert comparison.direction == "MODERATING"
    assert comparison.display_text == "Down from mid-single-digit"
    assert comparison.source_references
    core = next(item for item in package.core_drivers if item.core_id == "inventory-unit-growth")
    assert core.qoq_value is None
    assert core.yoy_value is None


def test_combined_store_activity_is_guidance_only_and_does_not_split_or_add_history(source: dict, package) -> None:
    combined = assess_combined_store_activity_evidence(source)
    assert len(combined) == 2
    assert {item.actual_or_guidance for item in combined} == {"GUIDANCE"}
    assert {item.period_basis for item in combined} == {"GUIDANCE"}
    assert {item.precision for item in combined} == {"APPROXIMATE"}
    assert not any(item.label == "Remodeled / right-sized" for item in package.history_rows)
    rows = {item.label: item for item in package.history_rows}
    for label in ("Remodeled", "Right-sized"):
        assert [point.period_label for point in rows[label].points if point.value is None] == [
            "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2"
        ]


def test_footprint_definitions_are_traceable_and_preserve_period_end_vs_activity(package) -> None:
    assert FOOTPRINT_DEFINITION_CONTRACT == "footprint-definitions-and-economics@1"
    assert [item.term for item in package.footprint_definitions] == [
        "Company-owned stores", "New stores", "Remodeled", "Right-sized", "Closed"
    ]
    assert {item.authority for item in package.footprint_definitions} == {
        "SOURCE_DEFINED", "SOURCE_SUPPORTED_INTERPRETATION"
    }
    assert all(item.source_references for item in package.footprint_definitions)
    assert "period end" in package.footprint_definitions[0].meaning
    assert all("period" in item.meaning for item in package.footprint_definitions[1:])
    assert package.footprint_definitions[0].measurement_authorities == (
        "SOURCE_DEFINED", "SAFE_DERIVATION"
    )
    assert "prior count + openings - closures" in package.footprint_definitions[0].measurement
    assert "new stores - closed stores" in package.store_count_roll_forward_note
    assert "do not change store count" in package.store_count_roll_forward_note
    assert package.store_count_roll_forward_note_sources
    visible_definition_text = " ".join(item.meaning for item in package.footprint_definitions)
    assert "management" not in visible_definition_text.casefold()


def test_footprint_economic_roles_are_bounded_traceable_and_non_directional(package) -> None:
    by_term = {item.term: item for item in package.footprint_definitions}
    assert {term: item.economic_role_type for term, item in by_term.items()} == {
        "Company-owned stores": "PERIOD_END_CAPACITY",
        "New stores": "CAPACITY_GROWTH",
        "Remodeled": "PRODUCTIVITY_INVESTMENT",
        "Right-sized": "FOOTPRINT_EFFICIENCY",
        "Closed": "FOOTPRINT_RATIONALIZATION",
    }
    assert all(item.economic_role_authority == "SOURCE_SUPPORTED_INTERPRETATION" for item in by_term.values())
    assert all(item.economic_role for item in by_term.values())
    visible = " ".join(item.economic_role for item in by_term.values()).casefold()
    assert not any(value in visible for value in (" positive ", " negative ", " good ", " bad "))
    assert "can rationalize" in by_term["Closed"].economic_role
    assert "demand and digital penetration" in by_term["Right-sized"].economic_role


def test_historical_footprint_support_is_under_the_hood_and_not_current_ownership(package) -> None:
    assert FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT == "footprint-economic-support@1"
    assert len(package.footprint_economic_support) == 3
    assert all(not item.current_period_metric_owner for item in package.footprint_economic_support)
    assert all(item.source_url.startswith("https://www.sec.gov/") for item in package.footprint_economic_support)
    remodel = next(item for item in package.footprint_economic_support if item.support_type == "HISTORICAL_REMODEL_RETURN_EVIDENCE")
    assert "historical support, not a current assumption" in remodel.evidence_summary
    visible = " ".join(
        item.meaning + " " + item.measurement + " " + item.economic_role
        for item in package.footprint_definitions
    )
    assert "high-single-digit" not in visible
    assert "basis points" not in visible


def test_future_footprint_context_relationships_are_declarative_and_do_not_rewrite_overview(package) -> None:
    assert FOOTPRINT_CONTEXT_RELATIONSHIP_CONTRACT == "footprint-context-relationships@1"
    assert len(package.footprint_context_relationships) == 4
    assert all(item.source_references for item in package.footprint_context_relationships)
    assert {item.semantic_type for item in package.footprint_context_relationships} == {
        "CAPACITY_AND_DEMAND_INTERACTION",
        "PRODUCTIVITY_INVESTMENT",
        "FOOTPRINT_EFFICIENCY",
        "FOOTPRINT_RATIONALIZATION",
    }
    overview_text = " ".join(item.text for item in package.overview)
    assert "high-single-digit" not in overview_text
    assert "occupancy" not in overview_text.casefold()


def test_lower_layer_receipts_are_carried_without_reownership(package, completeness) -> None:
    assert package.source_identity_receipts == {
        "full_data_completeness_sha256": completeness.sha256,
        "registry_sha256": completeness.registry.sha256,
        "analytics_sha256": completeness.analytics.sha256,
        "semantics_sha256": completeness.semantics.sha256,
        "selection_sha256": completeness.selection.sha256,
    }
    assert package.source_contracts == {
        "analytics": "operating-drivers-derived-longitudinal-analytics@1",
        "selection": "operating-drivers-orthogonal-story-selection@1",
        "semantics": "operating-drivers-context-semantic-priority@1",
        "shadow": "operating-drivers-canonical-shadow-registry@1",
        "completeness": "operating-drivers-anf-full-data-completeness@1",
    }


def test_v4_contains_no_multi_ticker_ui_or_rejected_builder_branch() -> None:
    source = inspect.getsource(build_operating_driver_anf_ui_v4)
    assert '== "PBI"' not in source
    assert '== "GPRE"' not in source
    assert "ticker_profile" not in source
    assert "operating_driver_" + "investor_ui" not in source


def test_workbook_plan_has_exact_current_columns_and_no_sparkline_records(package) -> None:
    plan = build_operating_driver_anf_workbook_v4_plan(package)
    headers = [item.display_value.strip() for item in plan.bindings if item.element_type == "CORE_HEADER"]
    assert headers == [
        "Metric", "Latest (2026-Q1)", "vs prior quarter", "vs year ago", "Broader trend", "Why it matters"
    ]
    assert plan.sparkline_records == ()
    assert all(item.mode == "SET_VALUE" for item in plan.cell_mutations)
    assert "SET_FORMULA" not in json.dumps(plan.to_dict())


def test_package_and_plan_are_deterministic(source: dict, completeness) -> None:
    receipts = {
        "full_data_completeness_sha256": completeness.sha256,
        "registry_sha256": completeness.registry.sha256,
        "analytics_sha256": completeness.analytics.sha256,
        "semantics_sha256": completeness.semantics.sha256,
        "selection_sha256": completeness.selection.sha256,
    }
    first = build_operating_driver_anf_ui_v4(source, source_identity_receipts=receipts)
    second = build_operating_driver_anf_ui_v4(json.loads(json.dumps(source)), source_identity_receipts=dict(receipts))
    assert first.package_sha256 == second.package_sha256
    assert build_operating_driver_anf_workbook_v4_plan(first).plan_sha256 == build_operating_driver_anf_workbook_v4_plan(second).plan_sha256
