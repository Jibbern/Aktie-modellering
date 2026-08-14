from __future__ import annotations

import inspect
import json

import pandas as pd
import pytest

import pbi_xbrl.debt_source_registry as registry
from pbi_xbrl.debt_source_registry import (
    DebtEvidenceRoutingError,
    canonical_debt_revolver_row_identity,
    merge_source_native_revolver_history,
)


def _row(
    *,
    quarter: str = "2026-01-31",
    facility_id: str = "primary_revolver",
    commitment: object = 500_000_000.0,
    source_type: str = "table",
    document_id: str = "doc-A",
    occurrence: str = "row-A",
    basis: str = "reported_balance",
    scope: str = "consolidated",
) -> dict[str, object]:
    return {
        "quarter": pd.Timestamp(quarter),
        "facility_id": facility_id,
        "basis": basis,
        "scope": scope,
        "revolver_commitment": commitment,
        "commitment_source_type": source_type,
        "revolver_facility_size": commitment,
        "facility_source_type": source_type,
        "revolver_drawn": None,
        "revolver_letters_of_credit": None,
        "revolver_availability": None,
        "source_type": source_type,
        "source_document_id": document_id,
        "source_ref": f"source:{document_id}",
        "source_row_ref": occurrence,
        "evidence_key": f"evidence:{document_id}:{occurrence}",
        "source_backed_lineage_disposition": "VALID",
    }


def _merge(rows: list[dict[str, object]]) -> pd.DataFrame:
    return merge_source_native_revolver_history(pd.DataFrame(rows), pd.DataFrame())


def test_reversing_candidate_order_does_not_change_selected_economic_fact() -> None:
    direct = _row(commitment=500_000_000.0, source_type="table", document_id="direct")
    derived = _row(commitment=400_000_000.0, source_type="derived", document_id="derived")

    forward = _merge([derived, direct])
    reverse = _merge([direct, derived])

    pd.testing.assert_frame_equal(forward, reverse)
    assert forward.iloc[0]["revolver_commitment"] == 500_000_000.0


def test_identical_duplicate_facts_reconcile_to_one_semantic_row() -> None:
    first = _row(document_id="doc-A", occurrence="row-1")
    second = _row(document_id="doc-B", occurrence="row-2")

    resolved = _merge([first, second])

    assert len(resolved) == 1
    assert resolved.iloc[0]["debt_row_resolution"] == "corroborated"
    assert resolved.iloc[0]["debt_evidence_count"] == 2


def test_identical_corroborating_sources_retain_valid_lineage() -> None:
    resolved = _merge(
        [
            _row(document_id="doc-A", occurrence="row-1"),
            _row(document_id="doc-B", occurrence="row-2"),
        ]
    )

    lineage = json.loads(resolved.iloc[0]["debt_evidence_lineage"])
    assert {row["source_document_id"] for row in lineage} == {"doc-A", "doc-B"}
    assert {row["source_row_ref"] for row in lineage} == {"row-1", "row-2"}
    assert resolved.iloc[0]["source_backed_lineage_disposition"] == "VALID"


def test_conflicting_same_authority_duplicates_fail_closed() -> None:
    with pytest.raises(DebtEvidenceRoutingError, match="Conflicting same-authority debt facts"):
        _merge(
            [
                _row(commitment=500_000_000.0, source_type="table", document_id="doc-A"),
                _row(commitment=400_000_000.0, source_type="table", document_id="doc-B"),
            ]
        )


def test_explicit_higher_authority_fact_wins_over_derived_candidate() -> None:
    resolved = _merge(
        [
            _row(commitment=400_000_000.0, source_type="derived", document_id="derived"),
            _row(commitment=500_000_000.0, source_type="table", document_id="direct"),
        ]
    )

    assert resolved.iloc[0]["revolver_commitment"] == 500_000_000.0
    assert resolved.iloc[0]["commitment_source_type"] == "table"
    assert resolved.iloc[0]["debt_row_resolution"] == "authority_selected"


def test_explicit_zero_survives_duplicate_resolution() -> None:
    resolved = _merge(
        [
            _row(commitment=100.0, source_type="derived", document_id="derived"),
            _row(commitment=0.0, source_type="table", document_id="reported-zero"),
        ]
    )

    assert resolved.iloc[0]["revolver_commitment"] == 0.0


def test_missing_values_do_not_become_zero() -> None:
    resolved = _merge(
        [
            _row(commitment=None, document_id="doc-A"),
            _row(commitment=pd.NA, document_id="doc-B"),
        ]
    )

    assert pd.isna(resolved.iloc[0]["revolver_commitment"])
    assert resolved.iloc[0]["revolver_commitment"] != 0


def test_current_period_does_not_borrow_prior_value() -> None:
    resolved = _merge(
        [
            _row(quarter="2025-12-31", commitment=346_700_000.0),
            _row(quarter="2026-06-30", commitment=None),
        ]
    )
    by_period = {
        pd.Timestamp(row["quarter"]).strftime("%Y-%m-%d"): row
        for row in resolved.to_dict(orient="records")
    }

    assert by_period["2025-12-31"]["revolver_commitment"] == 346_700_000.0
    assert pd.isna(by_period["2026-06-30"]["revolver_commitment"])


def test_different_instruments_remain_distinct() -> None:
    resolved = _merge(
        [
            _row(facility_id="primary_revolver"),
            _row(facility_id="secondary_revolver", document_id="doc-B"),
        ]
    )

    assert len(resolved) == 2
    assert set(resolved["facility_id"]) == {"primary_revolver", "secondary_revolver"}


def test_different_periods_remain_distinct() -> None:
    resolved = _merge(
        [
            _row(quarter="2025-12-31"),
            _row(quarter="2026-06-30", document_id="doc-B"),
        ]
    )

    assert list(resolved["quarter"].dt.strftime("%Y-%m-%d")) == ["2025-12-31", "2026-06-30"]


def test_different_basis_and_scope_remain_distinct() -> None:
    resolved = _merge(
        [
            _row(basis="reported_balance", scope="consolidated"),
            _row(basis="current_principal_overlay", scope="consolidated", document_id="doc-B"),
            _row(basis="reported_balance", scope="restricted_group", document_id="doc-C"),
        ]
    )

    assert len(resolved) == 3
    assert set(zip(resolved["debt_row_basis"], resolved["debt_row_scope"], strict=True)) == {
        ("reported_balance", "consolidated"),
        ("current_principal_overlay", "consolidated"),
        ("reported_balance", "restricted_group"),
    }


def test_source_occurrence_alone_does_not_create_second_economic_identity() -> None:
    first = _row(document_id="doc-A", occurrence="occurrence-1")
    second = _row(document_id="doc-B", occurrence="occurrence-2")

    first_identity = canonical_debt_revolver_row_identity(first)
    second_identity = canonical_debt_revolver_row_identity(second)
    resolved = _merge([first, second])

    assert first_identity == second_identity
    assert len(resolved) == 1


def test_physical_dataframe_order_is_irrelevant_for_mixed_authorities() -> None:
    rows = [
        _row(commitment=500_000_000.0, source_type="table", document_id="table-A"),
        _row(commitment=500_000_000.0, source_type="table", document_id="table-B"),
        _row(commitment=450_000_000.0, source_type="derived", document_id="derived"),
    ]

    expected = _merge(rows)
    mutated = _merge(pd.DataFrame(rows).sample(frac=1.0, random_state=42).to_dict(orient="records"))

    pd.testing.assert_frame_equal(expected, mutated)


def test_registry_merge_contains_no_keep_first_or_keep_last_owner() -> None:
    source = inspect.getsource(registry.merge_source_native_revolver_history)
    assert ".iloc[" not in source
    assert ".head(" not in source
    assert "drop_duplicates" not in source
    assert "keep=" not in source
