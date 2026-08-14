from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.summary_bs_golden import (
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
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_projection import (
    build_summary_bs_projection_plan_from_paths,
    write_summary_bs_projection_plan,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_summary_bs_foundation import (
    build_anf_summary_bs_products,
    write_anf_summary_bs_candidate_package,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPOSITORY_ROOT / "tests" / "fixtures" / "summary_bs"
EXPECTED_MANIFEST_FILE_SHA256 = (
    "a6e7931a79da8dc70cc99607f43bbf1b5d6492cda22123b46dfb3ccf0aea2a5b"
)
EXPECTED_PLAN_DIGEST = "481fd188c95090b96f810e192c6927a5f5f910672d076a9acc2ebf2591f4a215"
EXPECTED_RAW_WORKBOOK_SHA256 = (
    "f57854d278b27bf206222d1979cba218d79aa355b5a36239f84af4950d6cbda2"
)
EXPECTED_SEMANTIC_SHA256 = (
    "c370caffe141fcabc27eb12f191f2e6a78c5d40d249486cd5122bb4f3a19c2a7"
)
EXPECTED_CANONICAL_OOXML_SHA256 = (
    "cc1c0f6d4f811acb1a6214fa9f854816efcf22562975aa5ed243426cc4601c9e"
)
EXPECTED_PRODUCT_HASHES = {
    "anf_summary_product.v1.json": (
        "48a197e49921709b07f83d13aa021c290787bd39d2b5ee4ba5474dc4eb78f7f3"
    ),
    "anf_bs_segment_product.v1.json": (
        "fbc8be16938c18db8a99a1a791760c88b5d50160da4cda38b80fc4485da1c709"
    ),
    "anf_summary_shadow.v1.json": (
        "0078c29c7a1e3fad4fbc8bb6f1586a82df6b9fc096e12f3e008fb41d32bdf2f6"
    ),
    "anf_bs_segment_shadow.v1.json": (
        "108e087754d301ee03ea649b872778be9d34e9c315768466892a7cec6a9b41c2"
    ),
}

_DATA_ROOT = resolve_effective_data_root_from_ancestors(REPOSITORY_ROOT, env={}).data_root
if _DATA_ROOT is None:
    raise RuntimeError("A registered StockModelData root is required for Summary/BS golden tests.")
DATA_ROOT = _DATA_ROOT
SOURCE_AUDIT_ROOT = (
    DATA_ROOT
    / "audit"
    / "anf_summary_bs_segment_exhaustive_historical_lineage_audit_2026-08-10"
)
PROTECTED_ANF = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"


@pytest.fixture(scope="session")
def regenerated_package(tmp_path_factory: pytest.TempPathFactory) -> Path:
    bundle = build_anf_summary_bs_products(DATA_ROOT, SOURCE_AUDIT_ROOT)
    output = tmp_path_factory.mktemp("summary_bs_golden_source_native")
    write_anf_summary_bs_candidate_package(bundle, output)
    return output


def test_summary_bs_golden_manifest_is_exact_closed_and_discoverable() -> None:
    assert fixture_sha256(GOLDEN_MANIFEST_PATH) == EXPECTED_MANIFEST_FILE_SHA256
    receipt = verify_golden_manifest(GOLDEN_MANIFEST_PATH)
    manifest = receipt["manifest"]

    assert receipt["passed"] is True
    assert manifest["golden_id"] == receipt["golden_id"] == GOLDEN_ID
    assert manifest["acceptance_status"] == GOLDEN_ACCEPTANCE_STATUS
    assert manifest["lifecycle"] == receipt["lifecycle"] == GOLDEN_LIFECYCLE
    assert manifest["production_default"] is receipt["production_default"] is GOLDEN_PRODUCTION_DEFAULT
    assert manifest["workbook_golden"]["workbook_id"] == GOLDEN_WORKBOOK_ID
    assert manifest["checkpoint"]["pre_summary_bs_checkpoint"] == (
        "05b9446b272ed91a7068affd0716ed66bd9046cc"
    )
    assert manifest["checkpoint"]["rollback_requires_protected_workbook_modification"] is False


def test_golden_fixtures_pin_the_accepted_economics_lineage_and_binding() -> None:
    summary = load_json_strict(FIXTURE_ROOT / "anf_summary_product.v1.json")
    bs = load_json_strict(FIXTURE_ROOT / "anf_bs_segment_product.v1.json")
    summary_shadow = load_json_strict(FIXTURE_ROOT / "anf_summary_shadow.v1.json")
    bs_shadow = load_json_strict(FIXTURE_ROOT / "anf_bs_segment_shadow.v1.json")
    foundation = load_json_strict(FIXTURE_ROOT / "anf_summary_bs_foundation_identity.v1.json")
    plan = load_json_strict(FIXTURE_ROOT / "anf_summary_bs_binding_plan.v1.json")
    acceptance = load_json_strict(FIXTURE_ROOT / "anf_summary_bs_acceptance.v1.json")

    assert {name: fixture_sha256(FIXTURE_ROOT / name) for name in EXPECTED_PRODUCT_HASHES} == (
        EXPECTED_PRODUCT_HASHES
    )
    assert len(summary["fields"]) == 35
    assert len(bs["fields"]) == 417
    assert Counter(row["status"] for row in summary["fields"]) == Counter(
        available=27, needs_review=2, unavailable=6
    )
    assert Counter(row["status"] for row in bs["fields"]) == Counter(
        available=361, needs_review=24, unavailable=32
    )
    assert summary_shadow["broken_lineage_count"] == bs_shadow["broken_lineage_count"] == 0
    assert foundation["canonical_fact_count"] == 468
    assert foundation["derivation_count"] == 141
    assert plan["plan_digest"] == EXPECTED_PLAN_DIGEST
    assert plan["lifecycle"] == GOLDEN_LIFECYCLE
    assert len(plan["bindings"]) == 452
    assert plan["formula_ownership"] == []
    assert acceptance["summary"]["readback_passed"] == 35
    assert acceptance["bs_segments"]["readback_passed"] == 417
    assert acceptance["lineage"]["traceable_count"] == 388
    assert acceptance["blank_closure"]["passed_count"] == 92
    assert acceptance["defect_closure"]["closed_count"] == 122
    assert acceptance["defect_closure"]["reopened_count"] == 0
    assert acceptance["lossless_preservation"]["unrelated_workbook_delta_count"] == 0


def test_corrected_summary_semantic_identities_are_the_only_current_owners() -> None:
    summary = load_json_strict(FIXTURE_ROOT / "anf_summary_product.v1.json")
    current_keys = {row["metric_key"] for row in summary["fields"]}
    expected_current = {
        "business_description",
        "strategic_context",
        "key_competitive_advantage",
        "segment_operating_model",
        "inventory_omnichannel_dependency",
        "international_growth_dependency",
        "liquidity_buyback_dependency",
        "liquidity_refinancing_invalidator",
    }
    historical_incorrect = {
        "investment_thesis",
        "catalysts",
        "key_risks",
        "gross_margin_assessment",
        "operating_expense_assessment",
        "capital_intensity_assessment",
        "tariff_dependency",
        "erp_dependency",
        "real_estate_dependency",
        "thesis_invalidator_margin",
    }
    assert expected_current <= current_keys
    assert historical_incorrect.isdisjoint(current_keys)
    assert len({row["field_id"] for row in summary["fields"]}) == 35


def test_source_native_golden_regenerates_from_the_current_implementation(
    regenerated_package: Path,
) -> None:
    mapping = {
        "summary_product.json": "anf_summary_product.v1.json",
        "bs_segment_product.json": "anf_bs_segment_product.v1.json",
        "summary_shadow.json": "anf_summary_shadow.v1.json",
        "bs_segment_shadow.json": "anf_bs_segment_shadow.v1.json",
        "manifest.json": "anf_summary_bs_source_native_manifest.v1.json",
    }
    for regenerated_name, fixture_name in mapping.items():
        assert fixture_bytes(regenerated_package / regenerated_name) == fixture_bytes(
            FIXTURE_ROOT / fixture_name
        )


def test_binding_plan_regenerates_from_committed_golden_inputs(
    regenerated_package: Path,
    tmp_path: Path,
) -> None:
    plan = build_summary_bs_projection_plan_from_paths(
        summary_product_path=FIXTURE_ROOT / "anf_summary_product.v1.json",
        summary_shadow_path=FIXTURE_ROOT / "anf_summary_shadow.v1.json",
        bs_product_path=FIXTURE_ROOT / "anf_bs_segment_product.v1.json",
        bs_shadow_path=FIXTURE_ROOT / "anf_bs_segment_shadow.v1.json",
        surface_map_path=FIXTURE_ROOT / "anf_summary_bs_surface_map.v1.json",
        protected_workbook_sha256=(
            "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
        ),
    )
    output = write_summary_bs_projection_plan(plan, tmp_path / "plan.json")
    assert plan["plan_digest"] == EXPECTED_PLAN_DIGEST
    assert fixture_bytes(output) == fixture_bytes(
        FIXTURE_ROOT / "anf_summary_bs_binding_plan.v1.json"
    )


def test_registered_golden_replays_exactly_from_committed_fixtures(tmp_path: Path) -> None:
    output = tmp_path / "ANF_summary_bs_source_native_golden_replay.xlsx"
    receipt = reproduce_registered_golden(
        base_workbook=PROTECTED_ANF,
        output_workbook=output,
    )
    manifest = verify_golden_manifest()["manifest"]

    assert output != PROTECTED_ANF
    assert hashlib.sha256(output.read_bytes()).hexdigest() == EXPECTED_RAW_WORKBOOK_SHA256
    assert receipt["canonical_ooxml_sha256"] == EXPECTED_CANONICAL_OOXML_SHA256
    assert manifest["workbook_golden"]["semantic_sha256"] == EXPECTED_SEMANTIC_SHA256
    assert receipt["binding_plan_digest"] == EXPECTED_PLAN_DIGEST
    assert receipt["lifecycle"] == GOLDEN_LIFECYCLE
    assert receipt["production_default"] is False
    assert receipt["reproduced_from_committed_fixtures"] is True


def test_executable_golden_contract_has_no_local_path_or_weak_identity_dependency() -> None:
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
