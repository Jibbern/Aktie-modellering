from __future__ import annotations

from collections import Counter

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_anf_source_period_repair import (
    EvidencePrecision,
    PERIOD_BASIS_CONTRACT,
    PeriodBasis,
    PresentationDisposition,
    build_anf_operating_driver_source_period_repair,
)


@pytest.fixture(scope="module")
def package():
    return build_anf_operating_driver_source_period_repair()


def _facts(package, metric: str, period: str):
    return [item for item in package.source_census if item.metric_label == metric and item.period_label == period]


def test_period_basis_contract_is_explicit_and_complete(package) -> None:
    assert package.period_basis_contract == PERIOD_BASIS_CONTRACT
    assert set(item.period_basis for item in package.source_census) == set(PeriodBasis)
    assert package.reconciliation["period_basis_values"] == [item.value for item in PeriodBasis]


def test_every_fact_has_source_period_unit_dimensions_definition_and_knowledge_date(package) -> None:
    document_ids = {item.source_document_id for item in package.source_documents}
    assert len(package.source_documents) == 12
    assert len(package.source_census) == 125
    assert len({item.fact_id for item in package.source_census}) == 125
    for item in package.source_census:
        assert item.source_document_id in document_ids
        assert item.source_location
        assert item.period_label and item.period_basis
        assert item.unit_id and item.dimension_member_ids
        assert item.definition_id and item.knowledge_date


def test_q4_comparable_sales_are_direct_quarter_actuals(package) -> None:
    expected = {
        "Total Company comparable sales": "1",
        "Americas comparable sales": "2",
        "EMEA comparable sales": "-3",
        "APAC comparable sales": "0",
        "Abercrombie comparable sales": "-1",
        "Hollister comparable sales": "3",
    }
    for metric, value in expected.items():
        facts = _facts(package, metric, "2025-Q4")
        assert len(facts) == 1
        assert facts[0].value == value
        assert facts[0].period_basis is PeriodBasis.QUARTER_ACTUAL
        assert facts[0].precision is EvidencePrecision.EXACT
        assert facts[0].source_observation_role == "DIRECT_SOURCE_FACT"


def test_latest_q1_direct_facts_are_complete(package) -> None:
    expected = {
        "Net sales growth": "2",
        "Total Company comparable sales": "-1",
        "Americas comparable sales": "1",
        "EMEA comparable sales": "-11",
        "APAC comparable sales": "15",
        "Abercrombie comparable sales": "0",
        "Hollister comparable sales": "-2",
        "Company-owned stores, end": "834",
        "New stores": "6",
        "Remodeled stores": "24",
        "Right-sized stores": "2",
        "Closed stores": "1",
        "Inventory at cost": "532.691",
    }
    for metric, value in expected.items():
        facts = _facts(package, metric, "2026-Q1")
        assert len(facts) == 1 and facts[0].value == value


def test_fy_store_actuals_are_separate_from_guidance(package) -> None:
    expected = {"New stores": "62", "Remodeled stores": "47", "Right-sized stores": "11", "Closed stores": "22"}
    for metric, value in expected.items():
        facts = _facts(package, metric, "FY2025")
        assert len(facts) == 1 and facts[0].value == value
        assert facts[0].period_basis is PeriodBasis.FY_ACTUAL
    guidance = [item for item in package.source_census if item.period_basis is PeriodBasis.GUIDANCE]
    assert {item.value for item in guidance} >= {"70", "80"}
    assert not any(item.metric_label == "Right-sized stores" and item.value == "70" for item in package.source_census)


def test_safe_quarter_store_derivations_are_exact_and_lineaged(package) -> None:
    expected = {
        ("New stores", 1): "7", ("New stores", 2): "19", ("New stores", 3): "22", ("New stores", 4): "14",
        ("Remodeled stores", 1): "9", ("Remodeled stores", 2): "7", ("Remodeled stores", 3): "8", ("Remodeled stores", 4): "23",
        ("Right-sized stores", 1): "1", ("Right-sized stores", 2): "4", ("Right-sized stores", 3): "3", ("Right-sized stores", 4): "3",
        ("Closed stores", 1): "3", ("Closed stores", 2): "5", ("Closed stores", 3): "2", ("Closed stores", 4): "12",
    }
    assert len(package.quarter_activity_derivations) == 16
    for item in package.quarter_activity_derivations:
        assert item.result_value == expected[(item.metric_label, item.fiscal_quarter)]
        assert item.definition_compatible and item.dimension_compatible and item.unit_compatible
        assert item.same_fiscal_year and item.additive_activity_metric
        fact = next(fact for fact in package.source_census if fact.fact_id == item.result_fact_id)
        assert fact.period_basis is PeriodBasis.QUARTER_ACTUAL
        assert fact.source_type == "TYPED_DERIVATION"
        assert fact.source_observation_role == "SAFE_DERIVATION"
        assert item.minuend_fact_id in fact.source_location
        if item.subtrahend_fact_id is not None:
            assert item.subtrahend_fact_id in fact.source_location


def test_inventory_precision_and_owner_boundaries_are_truthful(package) -> None:
    current = _facts(package, "Inventory at cost", "2026-Q1")[0]
    prior = _facts(package, "Inventory at cost", "2025-Q1")[0]
    assert current.value == "532.691" and prior.value == "542.059"
    units = _facts(package, "Inventory unit growth", "2026-Q1")[0]
    assert units.value is None
    assert units.precision is EvidencePrecision.APPROXIMATE
    assert units.period_basis is PeriodBasis.APPROXIMATE_RANGE
    assert units.display_value == "Up low single digits (approx.)"
    assert units.presentation_disposition is PresentationDisposition.CORE_TEXT_ONLY


def test_digital_sales_mix_and_mobile_traffic_are_not_conflated(package) -> None:
    digital = _facts(package, "Digital sales mix", "FY2025")[0]
    mobile = _facts(package, "Mobile share of digital traffic", "FY2025")[0]
    assert digital.value == "44" and digital.period_basis is PeriodBasis.FY_ACTUAL
    assert mobile.value is None and mobile.precision is EvidencePrecision.APPROXIMATE
    assert "89" in mobile.display_value
    assert digital.canonical_driver_id != mobile.canonical_driver_id


def test_acceptance_gates_are_zero(package) -> None:
    required_zero = (
        "actual_guidance_confusion_count",
        "combined_metric_split_error_count",
        "unsafe_quarter_derivation_count",
        "quarter_derivation_value_mismatch_count",
        "direct_q4_comp_omission_count",
        "latest_period_mismatch_count",
        "untraceable_digital_mix_numeric_count",
        "digital_mix_quarter_misclassification_count",
        "approximate_to_exact_fabrication_count",
        "ytd_or_fy_masquerading_as_quarter_count",
        "missing_to_zero_count",
        "gap_bridging_count",
        "duplicate_economic_owner_count",
        "management_commentary_ownership_migration_count",
        "forward_assumption_ownership_migration_count",
    )
    assert package.reconciliation["status"] == "PASS"
    assert all(package.reconciliation[key] == 0 for key in required_zero)
    assert package.reconciliation["safe_quarter_derivation_count"] == 16
    assert Counter(item.period_basis for item in package.source_census)[PeriodBasis.GUIDANCE] > 0


def test_build_is_deterministic() -> None:
    first = build_anf_operating_driver_source_period_repair()
    second = build_anf_operating_driver_source_period_repair()
    assert first.sha256 == second.sha256
    assert first.registry.sha256 == second.registry.sha256
    assert first.analytics.sha256 == second.analytics.sha256
    assert first.semantics.sha256 == second.semantics.sha256
    assert first.selection.sha256 == second.selection.sha256
