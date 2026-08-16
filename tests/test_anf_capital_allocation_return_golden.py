from __future__ import annotations

import hashlib
from pathlib import Path
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.capital_allocation_return_golden import (
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
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPOSITORY_ROOT / "tests" / "fixtures" / "capital_allocation_return"
EXPECTED_MANIFEST_FILE_SHA256 = (
    "658cbb3675625c597aaeac48664967673784b5db93493cd16758304e4154f185"
)
EXPECTED_RAW_WORKBOOK_SHA256 = (
    "d8d870803f97c09ca9a8822285da7abf3d43ad86871d1b2887ee55edf3bf2020"
)
EXPECTED_SEMANTIC_SHA256 = (
    "590164c711cae1bfd068e8c182144c573d5083f292e72f469bdc405194b65bcb"
)
EXPECTED_CANONICAL_OOXML_SHA256 = (
    "5ac8b590274d8b396fa866ddcc29443d6d81bfab6cc2e9b2fbe4afffb699d463"
)
EXPECTED_RENDER_SHA256 = (
    "d18121a7661e2489175bb4f3f1e054d29afa7a8653945ad86c3d13b28092b991"
)
EXPECTED_BINDING_PLAN_DIGEST = (
    "0ac4103b27c61651217096bc9dc9a2984d65286d4d1dcdecd1df27be74bd15b3"
)
EXPECTED_PLAN_DIGEST = (
    "7eeef13ab2397d74c42cb1c3c9d0dfe195591b78ba16607408bf15b5f928615b"
)

_DATA_ROOT = resolve_effective_data_root_from_ancestors(REPOSITORY_ROOT, env={}).data_root
if _DATA_ROOT is None:
    raise RuntimeError("A registered StockModelData root is required for golden tests.")
PREDECESSOR_GOLDEN = (
    _DATA_ROOT
    / "audit"
    / "valuation_golden_acceptance_2026-08-15"
    / "golden"
    / "ANF_valuation_source_native_golden_v1.xlsx"
)


def test_capital_golden_manifest_is_exact_closed_and_discoverable() -> None:
    assert fixture_sha256(GOLDEN_MANIFEST_PATH) == EXPECTED_MANIFEST_FILE_SHA256
    receipt = verify_golden_manifest()
    manifest = receipt["manifest"]

    assert receipt["passed"] is True
    assert manifest["golden_id"] == receipt["golden_id"] == GOLDEN_ID
    assert manifest["workbook_golden"]["workbook_id"] == GOLDEN_WORKBOOK_ID
    assert manifest["acceptance_status"] == GOLDEN_ACCEPTANCE_STATUS
    assert manifest["lifecycle"] == receipt["lifecycle"] == GOLDEN_LIFECYCLE
    assert manifest["production_default"] is receipt["production_default"] is False
    assert GOLDEN_PRODUCTION_DEFAULT is False
    assert manifest["checkpoint"]["pre_capital_product_checkpoint"] == (
        "e150630c2d761d804eb16445220a517a43f9500c"
    )
    assert manifest["checkpoint"]["rollback_requires_protected_workbook_modification"] is False


def test_committed_plan_pins_bindings_lineage_and_net_share_contract() -> None:
    receipt = verify_golden_manifest()
    projection = receipt["projection"]
    plan = projection["plan"]

    assert projection["binding_count"] == 145
    assert projection["available_binding_count"] == 114
    assert projection["unavailable_binding_count"] == 31
    assert projection["lineage_complete_count"] == 114
    assert projection["binding_plan_digest"] == EXPECTED_BINDING_PLAN_DIGEST
    assert plan["plan_digest"] == EXPECTED_PLAN_DIGEST
    assert plan["added_metric_instance_count"] == 5
    assert len(plan["net_share_percentage_records"]) == 2
    assert all(
        row["contract"] == "historical-net-share-reduction-percentage@1"
        for row in plan["net_share_percentage_records"]
    )


def test_generic_contract_pins_owner_routes_without_pbi_wiring() -> None:
    contract = load_json_strict(
        FIXTURE_ROOT / "capital_allocation_return_generic_contract.v1.json"
    )

    assert contract["missing_is_never_zero"] is True
    assert contract["ticker_specific_economic_branch_allowed"] is False
    assert contract["pbi_workbook_state"] == "binding_profile_required_not_wired"
    assert contract["workbook_bridge"] == "target_not_wired"
    assert contract["production_default"] is False
    assert contract["net_share_percentage_contract"]["weighted_average_shares_allowed"] is False
    assert set(contract["capital_return_activity_families"]) == {
        "BUYBACK",
        "DIVIDEND",
        "SHARE_ISSUANCE",
    }


def test_registered_capital_golden_replays_exactly_from_committed_delta(
    tmp_path: Path,
) -> None:
    output = tmp_path / "ANF_valuation_capital_product_golden_v2.xlsx"
    receipt = reproduce_registered_golden(
        predecessor_workbook=PREDECESSOR_GOLDEN,
        output_workbook=output,
    )

    assert output != PREDECESSOR_GOLDEN
    assert hashlib.sha256(output.read_bytes()).hexdigest() == EXPECTED_RAW_WORKBOOK_SHA256
    assert receipt["canonical_ooxml_sha256"] == EXPECTED_CANONICAL_OOXML_SHA256
    assert receipt["semantic_sha256"] == EXPECTED_SEMANTIC_SHA256
    assert receipt["binding_plan_digest"] == EXPECTED_BINDING_PLAN_DIGEST
    assert receipt["workbook_id"] == GOLDEN_WORKBOOK_ID
    assert receipt["reproduced_from_committed_fixtures"] is True
    assert receipt["production_default"] is False
    with ZipFile(output, "r") as archive:
        workbook_xml = archive.read("xl/workbook.xml")
        valuation = archive.read("xl/worksheets/sheet2.xml")
    assert b'calcMode="auto"' in workbook_xml
    assert b'fullCalcOnLoad="1"' in workbook_xml
    assert b'forceFullCalc="0"' in workbook_xml
    assert b"Capital Allocation" in valuation
    assert b"Capital Return" in valuation


def test_acceptance_pins_formula_native_visual_and_cross_ticker_results() -> None:
    acceptance = load_json_strict(
        FIXTURE_ROOT / "anf_capital_allocation_return_acceptance.v1.json"
    )

    assert acceptance["status"] == "PASS"
    assert acceptance["formula_ownership"]["valuation_formula_count"] == 7
    assert acceptance["formula_ownership"]["hidden_economic_owner_formula_count"] == 0
    assert acceptance["native_evidence"]["decision"] == "NATIVE_EVIDENCE_REUSED"
    assert acceptance["native_evidence"]["repair_event_count"] == 0
    assert acceptance["preservation"]["unrelated_workbook_delta_count"] == 0
    assert acceptance["cross_ticker_generality"]["generic_test_matrix"] == "9/9"
    assert acceptance["cross_ticker_generality"]["pbi_implementation_accepted"] is False
    assert acceptance["visual_acceptance"]["render_sha256"] == EXPECTED_RENDER_SHA256
    assert acceptance["workbook_acceptance"]["raw_sha256"] == EXPECTED_RAW_WORKBOOK_SHA256


def test_v2_contract_preserves_valuation_v1_as_immutable_predecessor() -> None:
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)

    assert manifest["predecessor"]["golden_id"] == "valuation-source-native:anf@1.0.0"
    assert manifest["predecessor"]["workbook_id"] == (
        "valuation-source-native-workbook:anf@1.0.0"
    )
    assert manifest["workbook_golden"]["workbook_id"] == (
        "valuation-source-native-workbook:anf@2.0.0"
    )
    assert manifest["economic_product"]["product_id"] == (
        "capital-allocation-return-source-native:anf@1.0.0"
    )


def test_golden_contract_has_no_absolute_path_weak_identity_or_pbi_activation() -> None:
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)
    text = fixture_bytes(GOLDEN_MANIFEST_PATH).decode("utf-8")

    assert "C:\\\\Users\\\\" not in text
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
    assert manifest["cross_ticker_generality"]["pbi_lifecycle_activated"] is False
    assert manifest["materialization"]["source_selection_performed"] is False
    assert manifest["lifecycle"] == "target_not_wired"
    assert manifest["production_default"] is False
