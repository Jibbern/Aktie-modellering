from __future__ import annotations

import copy
import hashlib
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_projection import (
    DILUTED_SHARES_ROW_LABEL,
    INVENTORY_SALES_SPREAD_ROW_LABEL,
    PERCENTAGE_POINT_DISPLAY_CONTRACT,
    PRESENTATION_MUTATION_CONTRACT,
    PROJECTION_SCHEMA,
    TARGET_WORKBOOK_LIFECYCLE,
    SummaryBSWorkbookProjectionError,
    build_summary_bs_projection_plan,
    build_summary_bs_projection_plan_from_paths,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_summary_bs_foundation import (
    build_anf_summary_bs_products,
    write_anf_summary_bs_candidate_package,
)


DATA_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
SOURCE_AUDIT_ROOT = (
    DATA_ROOT
    / "audit"
    / "anf_summary_bs_segment_exhaustive_historical_lineage_audit_2026-08-10"
)
PROJECTION_AUDIT_ROOT = (
    DATA_ROOT / "audit" / "summary_bs_source_native_projection_2026-08-14"
)
SURFACE_MAP = PROJECTION_AUDIT_ROOT / "WORKBOOK_SURFACE_MAP.json"
PROTECTED_WORKBOOK = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
PROTECTED_WORKBOOK_SHA256 = (
    "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
)


@pytest.fixture(scope="session")
def source_bundle() -> dict:
    return build_anf_summary_bs_products(DATA_ROOT, SOURCE_AUDIT_ROOT)


@pytest.fixture(scope="session")
def inputs(source_bundle: dict) -> dict[str, dict]:
    artifacts = source_bundle["artifacts"]
    return {
        "summary_product": artifacts["summary_product.json"],
        "summary_shadow": artifacts["summary_shadow.json"],
        "bs_product": artifacts["bs_segment_product.json"],
        "bs_shadow": artifacts["bs_segment_shadow.json"],
        "surface_map": load_json_strict(SURFACE_MAP),
    }


@pytest.fixture(scope="session")
def plan(inputs: dict[str, dict]) -> dict:
    return build_summary_bs_projection_plan(
        **inputs,
        protected_workbook_sha256=PROTECTED_WORKBOOK_SHA256,
    )


def _binding(plan: dict, surface: str, metric_key: str, period_fragment: str) -> dict:
    matches = [
        row
        for row in plan["bindings"]
        if row["product_surface"] == surface
        and row["metric_key"] == metric_key
        and period_fragment in row["period_id"]
    ]
    assert len(matches) == 1, matches
    return matches[0]


def test_projection_is_anchored_to_protected_surface_and_stays_unwired(plan: dict) -> None:
    assert hashlib.sha256(PROTECTED_WORKBOOK.read_bytes()).hexdigest() == PROTECTED_WORKBOOK_SHA256
    assert plan["schema"] == PROJECTION_SCHEMA
    assert plan["lifecycle"] == TARGET_WORKBOOK_LIFECYCLE == "target_not_wired"
    assert plan["protected_workbook"]["sha256"] == PROTECTED_WORKBOOK_SHA256
    assert plan["protected_workbook"]["surface_digest"]


def test_plan_closes_exact_452_field_universe_without_ambiguous_targets(plan: dict) -> None:
    assert len(plan["bindings"]) == 452
    assert Counter(row["product_surface"] for row in plan["bindings"]) == Counter(
        Summary=35, BS_Segments=417
    )
    assert Counter(row["status"] for row in plan["bindings"]) == Counter(
        available=388, needs_review=26, unavailable=38
    )
    targets = [(row["target_sheet"], row["target_cell"]) for row in plan["bindings"]]
    assert len(targets) == len(set(targets))
    assert plan["validation"]["unclassified_field_count"] == 0
    assert plan["validation"]["duplicate_target_owner_count"] == 0
    assert plan["validation"]["unbound_visible_field_count"] == 0
    assert plan["validation"]["legacy_economic_survivor_count"] == 0


def test_bindings_are_semantic_and_typed_not_row_order_dependent(inputs: dict, plan: dict) -> None:
    shuffled = copy.deepcopy(inputs)
    shuffled["summary_shadow"]["field_lineage"].reverse()
    shuffled["bs_shadow"]["field_lineage"].reverse()
    reproduced = build_summary_bs_projection_plan(
        **shuffled,
        protected_workbook_sha256=PROTECTED_WORKBOOK_SHA256,
    )
    assert reproduced["plan_digest"] == plan["plan_digest"]
    assert reproduced == plan


def test_surface_map_validates_labels_period_axes_and_contains_no_formula_owner(plan: dict) -> None:
    assert all(row["row_label"] not in {None, ""} for row in plan["bindings"])
    assert {row["period_axis_id"] for row in plan["bindings"]} == {
        "summary_semantic_field",
        "bs_quarterly_periods",
        "bs_annual_periods",
    }
    assert plan["formula_ownership"] == []
    assert plan["validation"]["formula_count"] == 0
    assert plan["validation"]["formula_economic_owner_count"] == 0
    assert plan["validation"]["formula_presentation_only_count"] == 0


def test_bounded_semantic_and_presentation_repairs_are_explicit(plan: dict) -> None:
    summary_targets = {
        row["target_cell"]: row
        for row in plan["bindings"]
        if row["product_surface"] == "Summary"
    }
    assert {
        cell: summary_targets[cell]["metric_key"]
        for cell in ("A3", "A5", "A7", "B13", "B14", "B15", "A19", "A20", "A21", "A24")
    } == {
        "A3": "business_description",
        "A5": "strategic_context",
        "A7": "key_competitive_advantage",
        "B13": "segment_operating_model",
        "B14": "segment_operating_model",
        "B15": "segment_operating_model",
        "A19": "inventory_omnichannel_dependency",
        "A20": "international_growth_dependency",
        "A21": "liquidity_buyback_dependency",
        "A24": "liquidity_refinancing_invalidator",
    }
    assert len({row["field_id"] for row in plan["bindings"]}) == 452

    presentation = {row["target_cell"]: row for row in plan["presentation_mutations"]}
    assert set(presentation) == {"A3", "A49", "A53"}
    assert all(row["contract"] == PRESENTATION_MUTATION_CONTRACT for row in presentation.values())
    assert presentation["A3"]["write_value"]["text"].endswith(
        "Quarterly Seg PASS | Annual Seg PASS"
    )
    assert presentation["A3"]["derivation"]["available_binding_count"] == 48
    assert presentation["A49"]["write_value"]["text"] == DILUTED_SHARES_ROW_LABEL
    assert presentation["A53"]["write_value"]["text"] == INVENTORY_SALES_SPREAD_ROW_LABEL

    pp = [
        row
        for row in plan["bindings"]
        if row["metric_key"] == "inventory_growth_minus_sales_growth"
    ]
    assert len(pp) == 8
    assert {row["display_transform_contract"] for row in pp} == {
        PERCENTAGE_POINT_DISPLAY_CONTRACT
    }
    assert {row["display_scale"] for row in pp} == {"100"}
    assert {row["projection_number_format_code"] for row in pp} == {"0.0"}
    assert {row["row_label"] for row in pp} == {INVENTORY_SALES_SPREAD_ROW_LABEL}
    for row in pp:
        assert Decimal(row["write_value"]["canonical_decimal"]) == (
            Decimal(row["canonical_value"]["value"]) * 100
        )

    diluted = [
        row
        for row in plan["bindings"]
        if row["metric_key"] == "diluted_weighted_average_shares"
    ]
    assert len(diluted) == 8
    assert {row["row_label"] for row in diluted} == {DILUTED_SHARES_ROW_LABEL}

    b42 = summary_targets["B42"]
    assert b42["unit_id"] == "unit:core:ratio@1"
    assert b42["display_role"] == "ratio"
    assert b42["projection_number_format_code"] == "0.000"


def test_available_values_write_and_review_or_unavailable_never_become_zero(plan: dict) -> None:
    for row in plan["bindings"]:
        if row["status"] == "available":
            assert row["write_mode"] == "SET_VALUE"
            assert row["write_value"] is not None
        else:
            assert row["write_mode"] in {"CLEAR_CONTENTS", "NO_WRITE"}
            assert row["write_value"] is None
    zeros = [
        row
        for row in plan["bindings"]
        if row["write_value"] == {"kind": "number", "canonical_decimal": "0"}
    ]
    assert Counter(row["value_state"] for row in zeros) == Counter(
        explicit_zero=25, derived_zero=7
    )
    assert all(row["status"] == "available" for row in zeros)


def test_projection_resolves_exact_92_correctable_legacy_blanks(plan: dict) -> None:
    correctable = [
        row
        for row in plan["bindings"]
        if row["status"] == "available"
        and (row["legacy_classification"] == "BLANK" or row["legacy_value"] == "N/A")
    ]
    assert len(correctable) == 92
    assert Counter(row["product_surface"] for row in correctable) == Counter(
        Summary=1, BS_Segments=91
    )
    assert all(row["write_mode"] == "SET_VALUE" for row in correctable)


def test_invalid_summary_legacy_values_are_cleared_and_high_value_corrections_project(plan: dict) -> None:
    eps = _binding(plan, "Summary", "quarter_diluted_eps", "fy2026-q1")
    eps_change = _binding(plan, "Summary", "quarter_diluted_eps_yoy_change", "fy2026-q1")
    invalid_interest = _binding(plan, "Summary", "pnl_interest_coverage", "ttm-at-fy2026-q1")
    revolver = _binding(plan, "Summary", "revolver_availability", "fy2026-q1")
    liquidity = _binding(plan, "Summary", "liquidity_cash_plus_revolver", "fy2026-q1")
    assert (eps["target_cell"], eps["write_value"]) == (
        "B32",
        {"kind": "number", "canonical_decimal": "1.47"},
    )
    assert (eps_change["target_cell"], eps_change["write_value"]) == (
        "B33",
        {"kind": "number", "canonical_decimal": "-0.12"},
    )
    assert invalid_interest["target_cell"] == "B42"
    assert invalid_interest["disposition"] == "CLEAR_STALE_LEGACY_VALUE"
    assert invalid_interest["write_mode"] == "CLEAR_CONTENTS"
    assert revolver["write_value"] == {"kind": "number", "canonical_decimal": "449.531"}
    assert liquidity["write_value"] == {"kind": "number", "canonical_decimal": "1043.611"}


def test_latest_balance_sheet_and_segment_values_are_owned_by_source_native_product(plan: dict) -> None:
    expected = {
        "restricted_cash": ("I10", "7.336"),
        "marketable_securities": ("I13", "25.144"),
        "geographic_sales_americas": ("I61", "899.944"),
        "geographic_sales_emea": ("I62", "167.373"),
        "geographic_sales_apac": ("I63", "46.504"),
        "brand_sales_hollister": ("I66", "549.102"),
        "brand_sales_abercrombie": ("I67", "564.719"),
    }
    for metric_key, (cell, value) in expected.items():
        row = _binding(plan, "BS_Segments", metric_key, "fy2026-q1")
        assert row["target_cell"] == cell
        assert row["write_value"] == {"kind": "number", "canonical_decimal": value}


def test_all_available_fields_retain_typed_lineage(plan: dict) -> None:
    available = [row for row in plan["bindings"] if row["status"] == "available"]
    assert len(available) == 388
    assert all(row["lineage_present"] for row in available)
    assert plan["validation"]["available_without_lineage_count"] == 0


def test_period_axis_drift_and_duplicate_target_owner_fail_closed(inputs: dict) -> None:
    drifted = copy.deepcopy(inputs)
    drifted["surface_map"]["sheets"]["BS_Segments"]["cells"]["I7"]["value"] = "2026-Q2"
    with pytest.raises(SummaryBSWorkbookProjectionError, match="Period-axis mismatch"):
        build_summary_bs_projection_plan(
            **drifted,
            protected_workbook_sha256=PROTECTED_WORKBOOK_SHA256,
        )

    duplicate = copy.deepcopy(inputs)
    product_by_id = {row["field_id"]: row for row in duplicate["bs_product"]["fields"]}
    same_period = [
        row
        for row in duplicate["bs_shadow"]["field_lineage"]
        if product_by_id[row["field_id"]]["period_id"] == "period:anf:fy2024-q2@1"
    ]
    assert len(same_period) > 1
    same_period[1]["legacy_locator"] = same_period[0]["legacy_locator"]
    with pytest.raises(SummaryBSWorkbookProjectionError, match="Duplicate target owners"):
        build_summary_bs_projection_plan(
            **duplicate,
            protected_workbook_sha256=PROTECTED_WORKBOOK_SHA256,
        )


def test_path_builder_reproduces_plan(plan: dict, source_bundle: dict, tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate"
    write_anf_summary_bs_candidate_package(source_bundle, candidate_root)
    reproduced = build_summary_bs_projection_plan_from_paths(
        summary_product_path=candidate_root / "summary_product.json",
        summary_shadow_path=candidate_root / "summary_shadow.json",
        bs_product_path=candidate_root / "bs_segment_product.json",
        bs_shadow_path=candidate_root / "bs_segment_shadow.json",
        surface_map_path=SURFACE_MAP,
        protected_workbook_sha256=PROTECTED_WORKBOOK_SHA256,
    )
    assert reproduced == plan


def test_materialized_preview_readback_reconciles_every_source_native_field() -> None:
    summary = load_json_strict(PROJECTION_AUDIT_ROOT / "PROJECTION_VALIDATION_SUMMARY.json")
    summary_reconciliation = load_json_strict(
        PROJECTION_AUDIT_ROOT / "SUMMARY_FIELD_RECONCILIATION.json"
    )
    bs_reconciliation = load_json_strict(
        PROJECTION_AUDIT_ROOT / "BS_FIELD_RECONCILIATION.json"
    )
    assert summary["field_counts"] == {"Summary": 35, "BS_Segments": 417, "total": 452}
    assert summary["available_value_mismatch_count"] == 0
    assert summary["missing_to_zero_count"] == 0
    assert summary["stale_legacy_value_survivor_count"] == 0
    assert summary["binding_readback_mismatch_count"] == 0
    assert summary_reconciliation["field_count"] == 35
    assert summary_reconciliation["failed_count"] == 0
    assert bs_reconciliation["field_count"] == 417
    assert bs_reconciliation["failed_count"] == 0


def test_materialized_preview_preserves_accepted_blank_defect_and_determinism_contracts() -> None:
    blanks = load_json_strict(PROJECTION_AUDIT_ROOT / "CORRECTABLE_BLANKS_RECHECK.json")
    defects = load_json_strict(PROJECTION_AUDIT_ROOT / "DEFECT_PROJECTION_RECHECK.json")
    determinism = load_json_strict(PROJECTION_AUDIT_ROOT / "PREVIEW_DETERMINISM.json")
    assert blanks["identified_count"] == blanks["filled_as_expected"] == 92
    assert blanks["other"] == 0
    assert defects["baseline_closed_defect_count"] == 122
    assert defects["reopened_defect_count"] == 0
    assert determinism["canonical_ooxml_identical"] is True
    assert determinism["semantic_identical"] is True
    assert determinism["passed"] is True


def test_unrelated_workbook_gate_fails_closed_on_bounded_artifact_export_incompatibility() -> None:
    compatibility = load_json_strict(
        PROJECTION_AUDIT_ROOT / "ARTIFACT_TOOL_EXPORT_COMPATIBILITY.json"
    )
    unrelated = load_json_strict(PROJECTION_AUDIT_ROOT / "UNRELATED_WORKBOOK_DIFF.json")
    structural = load_json_strict(PROJECTION_AUDIT_ROOT / "STRUCTURAL_DIFF.json")
    assert compatibility["semantic_ownership_conflict"] is False
    assert compatibility["classification"] == (
        "BOUNDED_ARTIFACT_TOOL_XLSX_EXPORT_STRUCTURAL_INCOMPATIBILITY"
    )
    assert compatibility["blocks_preview_acceptance"] is True
    assert compatibility["passed"] is False
    assert unrelated["outside_surface_formula_change_count"] == 0
    assert unrelated["outside_surface_literal_value_change_count"] == 0
    assert unrelated["formula_cached_value_change_count"] > 0
    assert unrelated["new_formula_cached_error_count"] > 0
    assert unrelated["outside_surface_style_change_count"] > 0
    assert unrelated["unrelated_workbook_delta_count"] > 0
    assert unrelated["passed"] is False
    assert structural["layout_changed_sheet_count"] > 0
    assert structural["dropped_defined_name_count"] > 0
    assert structural["passed"] is False
