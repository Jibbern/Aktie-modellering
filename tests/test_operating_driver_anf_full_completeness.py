from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (
    CoverageState,
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_source_period_repair import (
    EvidencePrecision,
    PeriodBasis,
)


@pytest.fixture(scope="module")
def package():
    return build_anf_operating_driver_full_completeness()


def _facts(package, metric: str):
    return [item for item in package.observation_registry if item.metric_label == metric]


def _coverage(package, metric: str, period: str):
    matches = [
        item
        for item in package.coverage_matrix
        if item.metric_label == metric and item.period_label == period
    ]
    assert len(matches) == 1
    return matches[0]


def test_source_census_is_complete_and_primary_first(package) -> None:
    assert package.reconciliation["official_source_count"] == 70
    assert package.reconciliation["lower_priority_transcript_count"] == 1
    assert package.reconciliation["source_document_count"] == 71
    assert all(
        Path(item.local_path).is_file()
        for item in package.source_documents
        if item.local_path is not None
    )


def test_existing_evidence_is_retained_and_new_recoveries_are_reconciled(package) -> None:
    assert package.reconciliation["existing_fact_retained_count"] == 125
    assert package.reconciliation["new_fact_count"] == 98
    assert package.reconciliation["new_direct_fact_count"] == 77
    assert package.reconciliation["new_direct_numeric_fact_count"] == 68
    assert package.reconciliation["new_safe_derived_fact_count"] == 21
    assert package.reconciliation["unreconciled_source_evidence_disappearance_count"] == 0


def test_comparable_sales_are_complete_without_recasting_old_regions(package) -> None:
    for metric in (
        "Total Company comparable sales",
        "Abercrombie comparable sales",
        "Hollister comparable sales",
    ):
        assert len(_facts(package, metric)) == 13
        assert _coverage(package, metric, "2023-Q1").coverage_state is CoverageState.DIRECT_NUMERIC
    for metric in (
        "Americas comparable sales",
        "EMEA comparable sales",
        "APAC comparable sales",
    ):
        assert len(_facts(package, metric)) == 12
        q1 = _coverage(package, metric, "2023-Q1")
        assert q1.coverage_state is CoverageState.DEFINITION_BREAK
        assert q1.value is None


def test_inventory_cost_is_complete_and_remains_summary_bs_owned(package) -> None:
    expected = {
        "2023-Q1": "447.806",
        "2023-Q2": "493.479",
        "2023-Q3": "595.067",
        "2023-Q4": "469.466",
        "2024-Q1": "449.267",
        "2024-Q2": "539.759",
        "2024-Q3": "692.596",
        "2024-Q4": "575.005",
        "2025-Q1": "542.059",
        "2025-Q2": "592.966",
        "2025-Q3": "730.453",
        "2025-Q4": "601.218",
        "2026-Q1": "532.691",
    }
    facts = {item.period_label: item for item in _facts(package, "Inventory at cost")}
    assert {period: item.value for period, item in facts.items()} == expected
    assert {item.canonical_owner_id for item in facts.values()} == {
        "owner:summary-bs:source-native@1"
    }


def test_inventory_units_preserve_exact_approximate_and_missing_states(package) -> None:
    facts = {item.period_label: item for item in _facts(package, "Inventory unit growth")}
    assert facts["2025-Q2"].value == "7"
    assert facts["2025-Q2"].precision is EvidencePrecision.EXACT
    for period in ("2024-Q4", "2025-Q3", "2025-Q4", "2026-Q1"):
        assert facts[period].value is None
        assert facts[period].precision is EvidencePrecision.APPROXIMATE
        assert _coverage(package, "Inventory unit growth", period).qoq_ready is False
    assert _coverage(package, "Inventory unit growth", "2025-Q1").coverage_state is CoverageState.NOT_DISCLOSED


def test_store_activity_uses_only_safe_same_year_additive_derivations(package) -> None:
    expected = {
        "New stores": {
            "2023-Q1": "6", "2023-Q2": "9", "2023-Q3": "9", "2023-Q4": "11",
            "2024-Q1": "1", "2024-Q2": "17", "2024-Q3": "21", "2024-Q4": "26",
        },
        "Closed stores": {
            "2023-Q1": "10", "2023-Q2": "8", "2023-Q3": "3", "2023-Q4": "11",
            "2024-Q1": "13", "2024-Q2": "13", "2024-Q3": "5", "2024-Q4": "10",
        },
        "Remodeled stores": {"2024-Q3": "7", "2024-Q4": "18"},
        "Right-sized stores": {"2024-Q3": "1", "2024-Q4": "4"},
    }
    for metric, expected_periods in expected.items():
        actual = {
            item.period_label: item.value
            for item in _facts(package, metric)
            if item.period_basis is PeriodBasis.QUARTER_ACTUAL
            and item.fiscal_year in {2023, 2024}
        }
        assert actual == expected_periods
    assert package.reconciliation["unsafe_derivation_count"] == 0
    assert package.reconciliation["direct_source_overwritten_by_derivation_count"] == 0


def test_store_count_blanks_are_explicitly_not_disclosed(package) -> None:
    facts = {item.period_label: item.value for item in _facts(package, "Company-owned stores, end")}
    assert facts == {
        "2023-Q1": "758",
        "2023-Q4": "765",
        "2024-Q4": "789",
        "2025-Q4": "829",
        "2026-Q1": "834",
    }
    for period in ("2023-Q2", "2023-Q3", "2024-Q1", "2025-Q3"):
        row = _coverage(package, "Company-owned stores, end", period)
        assert row.coverage_state is CoverageState.NOT_DISCLOSED
        assert row.reason


def test_digital_channel_context_never_masquerades_as_quarterly_history(package) -> None:
    total_fy2025 = [
        item
        for item in _facts(package, "Digital sales mix")
        if item.period_label == "FY2025"
        and item.dimension_member_ids == ("member:operating-driver:total-company@1",)
    ]
    assert len(total_fy2025) == 1
    assert total_fy2025[0].value == "44"
    assert total_fy2025[0].source_type == "TRANSCRIPT"
    q4 = _coverage(package, "Digital sales mix", "2025-Q4")
    assert q4.coverage_state is CoverageState.PERIOD_INCOMPATIBLE
    assert q4.value is None
    assert all(
        _coverage(package, "Digital sales mix", f"{year}-Q{quarter}").value is None
        for year in range(2023, 2027)
        for quarter in range(1, 5)
        if (year, quarter) <= (2026, 1)
    )


def test_aur_traffic_conversion_and_freight_keep_their_evidence_roles(package) -> None:
    aur = _coverage(package, "Average unit retail direction", "2026-Q1")
    assert aur.coverage_state is CoverageState.DIRECT_QUALITATIVE
    assert aur.value is None
    assert _coverage(package, "Traffic", "2026-Q1").coverage_state is CoverageState.NOT_DISCLOSED
    assert _coverage(package, "Conversion", "2026-Q1").coverage_state is CoverageState.NOT_DISCLOSED
    freight = _coverage(package, "Freight and tariff cost context", "2026-Q1")
    assert freight.coverage_state is CoverageState.OWNER_ELSEWHERE
    assert freight.value is None


def test_coverage_matrix_is_metric_by_period_and_every_blank_has_a_reason(package) -> None:
    assert len(package.coverage_matrix) == 25 * 13
    counts = Counter(item.metric_label for item in package.coverage_matrix)
    assert set(counts.values()) == {13}
    assert all(item.reason for item in package.coverage_matrix)
    assert package.reconciliation["unexplained_material_history_blank_count"] == 0


def test_fail_closed_acceptance_counters_are_zero(package) -> None:
    for key in (
        "actual_guidance_collision_count",
        "ytd_as_quarter_count",
        "fy_as_q4_count",
        "missing_to_zero_count",
        "qualitative_to_numeric_count",
        "approximate_to_exact_count",
        "unsafe_derivation_count",
        "duplicate_economic_owner_count",
        "direct_source_overwritten_by_derivation_count",
        "unreconciled_source_evidence_disappearance_count",
        "unexplained_material_history_blank_count",
        "new_anf_specific_python_economic_parser_branch_count",
        "needs_review_count",
    ):
        assert package.reconciliation[key] == 0, key


def test_recovery_layers_are_shared_sector_or_declarative_profile_only(package) -> None:
    assert {item.implementation_layer for item in package.parser_recoveries} <= {
        "SHARED_ENGINE",
        "RETAIL_SECTOR_PACK",
        "ANF_TICKER_PROFILE",
    }
    generic_source = Path(
        "pbi_xbrl/longitudinal_memory/operating_driver_source_parsing.py"
    ).read_text(encoding="utf-8")
    assert "ticker ==" not in generic_source
    assert '"ANF"' not in generic_source


def test_package_is_strict_json_serializable_and_deterministic(package) -> None:
    encoded = json.dumps(
        package.to_dict(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    decoded = json.loads(encoded)
    assert decoded["sha256"] == package.sha256
    replay = build_anf_operating_driver_full_completeness()
    assert replay.sha256 == package.sha256
    assert replay.to_dict() == package.to_dict()
