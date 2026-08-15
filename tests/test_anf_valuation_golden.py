from __future__ import annotations

import hashlib
from pathlib import Path
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.valuation_golden import (
    GOLDEN_ACCEPTANCE_STATUS,
    GOLDEN_ID,
    GOLDEN_LIFECYCLE,
    GOLDEN_MANIFEST_PATH,
    GOLDEN_PRODUCTION_DEFAULT,
    GOLDEN_WORKBOOK_ID,
    fixture_bytes,
    fixture_sha256,
    load_json_strict,
    reproduce_registered_golden,
    verify_golden_manifest,
)
from pbi_xbrl.longitudinal_memory.valuation_source_native_projection import (
    CALCULATION_METADATA_POLICY_ID,
    load_valuation_projection_plan,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPOSITORY_ROOT / "tests" / "fixtures" / "valuation"
EXPECTED_MANIFEST_FILE_SHA256 = (
    "35aa89a08ab230e492a313fa5ee46eb35e1ac3487bf51bf06348ff915ef40770"
)
EXPECTED_PROJECTION_DIGEST = (
    "b173f2da3be473797adbdbec037b7de36d4384990220e6d65f5ee070abae7079"
)
EXPECTED_FORMULA_PLAN_DIGEST = (
    "4bd9358e3cd19743f9d7712fce1d03779f21357f36617b0a0847f7d805e3fb2c"
)
EXPECTED_DEFINED_NAME_PLAN_DIGEST = (
    "6d90fdd60380a0b9ae8ad1bf5f5e6a9920afb4456e7d0fe0d4e606a1211c570f"
)
EXPECTED_RAW_WORKBOOK_SHA256 = (
    "39fba7ae39a02fa9395cf25f103097f8c6d62ccbf3cf6a8ae25767babcb7fc1d"
)
EXPECTED_SEMANTIC_SHA256 = (
    "90c20c0aeb437af25da8686270e9f5eff7cc554e270724346f19b7947d7f05c6"
)
EXPECTED_CANONICAL_OOXML_SHA256 = (
    "dd5aabd1c50250add003b519f3d8edcc1b6d6e344841109b22a6d002e451ee5e"
)

_DATA_ROOT = resolve_effective_data_root_from_ancestors(REPOSITORY_ROOT, env={}).data_root
if _DATA_ROOT is None:
    raise RuntimeError("A registered StockModelData root is required for Valuation golden tests.")
SUMMARY_BS_GOLDEN = (
    _DATA_ROOT
    / "audit"
    / "summary_bs_golden_acceptance_2026-08-14"
    / "golden"
    / "ANF_summary_bs_source_native_golden_v1.xlsx"
)


def test_valuation_golden_manifest_is_exact_closed_and_discoverable() -> None:
    assert fixture_sha256(GOLDEN_MANIFEST_PATH) == EXPECTED_MANIFEST_FILE_SHA256
    receipt = verify_golden_manifest(GOLDEN_MANIFEST_PATH)
    manifest = receipt["manifest"]

    assert receipt["passed"] is True
    assert manifest["golden_id"] == receipt["golden_id"] == GOLDEN_ID
    assert manifest["workbook_golden"]["workbook_id"] == GOLDEN_WORKBOOK_ID
    assert manifest["acceptance_status"] == GOLDEN_ACCEPTANCE_STATUS
    assert manifest["lifecycle"] == receipt["lifecycle"] == GOLDEN_LIFECYCLE
    assert manifest["production_default"] is receipt["production_default"] is GOLDEN_PRODUCTION_DEFAULT
    assert manifest["checkpoint"]["pre_valuation_checkpoint"] == (
        "42a9796cdc227e88db4ee1986d9deb75767f37e4"
    )
    assert manifest["checkpoint"]["rollback_requires_protected_workbook_modification"] is False


def test_golden_projection_fixture_pins_the_accepted_plan_and_metadata_owner() -> None:
    plan_path = FIXTURE_ROOT / "anf_valuation_projection_plan.v1.json"
    plan = load_valuation_projection_plan(
        plan_path,
        expected_projection_digest=EXPECTED_PROJECTION_DIGEST,
        expected_formula_plan_digest=EXPECTED_FORMULA_PLAN_DIGEST,
        expected_defined_name_plan_digest=EXPECTED_DEFINED_NAME_PLAN_DIGEST,
    )
    fixture = load_json_strict(plan_path)

    assert fixture["calculation_metadata_policy_id"] == CALCULATION_METADATA_POLICY_ID
    assert len(plan.cell_mutations) == 1105
    assert len(plan.defined_name_mutations) == 90
    assert len(plan.compact_link_cells) == 20
    assert len(plan.old_formula_retirement_cells) == 74
    assert plan.ic_dependency_closure.cell_count == 1346
    assert plan.ic_dependency_closure.formula_count == 350


def test_golden_acceptance_pins_native_formula_and_economic_results() -> None:
    acceptance = load_json_strict(FIXTURE_ROOT / "anf_valuation_acceptance.v1.json")

    assert acceptance["status"] == "PASS"
    assert acceptance["economic_acceptance"]["reproduced_count"] == 930
    assert acceptance["economic_acceptance"]["literal_economic_drift_count"] == 0
    assert acceptance["formula_ownership"]["valuation_formula_count"] == 21
    assert acceptance["formula_ownership"]["retired_legacy_formula_survivor_count"] == 0
    assert acceptance["investment_case"]["canonical_name_reconciled"] == "40/40"
    assert acceptance["investment_case"]["canonical_matrix_reconciled"] == "24/24"
    assert acceptance["investment_case"]["compact_link_reconciled"] == "20/20"
    assert acceptance["native_acceptance"]["pass_count"] == 4
    assert acceptance["native_acceptance"]["repair_event_count"] == 0
    assert acceptance["native_acceptance"]["native_semantic_replay_match"] is True
    assert acceptance["preservation"]["unrelated_native_delta_count"] == 0
    assert acceptance["preservation"]["unexplained_native_delta_count"] == 0


def test_registered_valuation_golden_replays_exactly_from_committed_plan(
    tmp_path: Path,
) -> None:
    output = tmp_path / "ANF_valuation_source_native_golden_replay.xlsx"
    receipt = reproduce_registered_golden(
        base_workbook=SUMMARY_BS_GOLDEN,
        output_workbook=output,
    )

    assert output != SUMMARY_BS_GOLDEN
    assert hashlib.sha256(output.read_bytes()).hexdigest() == EXPECTED_RAW_WORKBOOK_SHA256
    assert receipt["canonical_ooxml_sha256"] == EXPECTED_CANONICAL_OOXML_SHA256
    assert receipt["semantic_sha256"] == EXPECTED_SEMANTIC_SHA256
    assert receipt["projection_digest"] == EXPECTED_PROJECTION_DIGEST
    assert receipt["calculation_metadata_policy_id"] == CALCULATION_METADATA_POLICY_ID
    assert receipt["reproduced_from_committed_fixtures"] is True
    assert receipt["production_default"] is False
    with ZipFile(output, "r") as archive:
        workbook_xml = archive.read("xl/workbook.xml")
    assert b'calcMode="auto"' in workbook_xml
    assert b'fullCalcOnLoad="1"' in workbook_xml
    assert b'forceFullCalc="0"' in workbook_xml


def test_golden_contract_has_no_absolute_local_path_or_weak_identity() -> None:
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)
    manifest_text = fixture_bytes(GOLDEN_MANIFEST_PATH).decode("utf-8")

    assert "C:\\\\Users\\\\" not in manifest_text
    assert all(
        not Path(row["repository_path"]).is_absolute()
        for row in manifest["implementation_artifacts"]
    )
    assert all(
        not Path(row["relative_path"]).is_absolute()
        for row in manifest["fixture_artifacts"]
    )
    assert all(
        value not in {"", "none", "unknown"}
        for row in manifest["implementation_artifacts"]
        for value in (row["repository_path"].casefold(), row["sha256"].casefold())
    )
    assert manifest["workbook_golden"]["data_root_relative_path"].startswith("audit/")
    assert manifest["materialization"]["source_selection_performed"] is False


def test_golden_workbook_identity_separates_deterministic_product_from_native_evidence() -> None:
    acceptance = load_json_strict(FIXTURE_ROOT / "anf_valuation_acceptance.v1.json")
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)

    assert manifest["workbook_golden"]["raw_sha256"] == EXPECTED_RAW_WORKBOOK_SHA256
    assert manifest["workbook_golden"]["semantic_sha256"] == EXPECTED_SEMANTIC_SHA256
    assert manifest["workbook_golden"]["canonical_ooxml_sha256"] == (
        EXPECTED_CANONICAL_OOXML_SHA256
    )
    assert acceptance["native_acceptance"][
        "run_outputs_are_acceptance_evidence_not_deterministic_golden"
    ] is True
