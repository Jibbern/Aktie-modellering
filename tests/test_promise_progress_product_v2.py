from __future__ import annotations

import dataclasses
import copy
import hashlib
import inspect
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from pbi_xbrl.longitudinal_memory.promise_progress_product_v2 import (
    BLOCK_ORDER,
    CHANGE_TYPES,
    COVERAGE_STATES,
    CREDIBILITY_BLOCK_ID,
    NEEDS_REVIEW_REASONS,
    OPEN_BLOCK_ID,
    PRODUCT_TYPE,
    PRODUCT_VERSION,
    Q4_ADD_FY_MINUS_QUARTERS_RULE_ID,
    Q4_ADD_FY_MINUS_YTD_RULE_ID,
    PROGRESSION_BLOCK_ID,
    GUIDANCE_UPDATE_ROW_KIND,
    PERIOD_RESULT_ROW_KIND,
    HORIZON_OUTCOME_ROW_KIND,
    SUCCESSOR_PRODUCT_VERSION,
    TIMELINE_BLOCK_ID,
    VERSION_STATES,
    PromiseProgressProductV2Error,
    _event_indexes,
    build_product_v2_shadow,
    build_promise_progress_product_v2,
    classify_timeline_fact_role,
    classify_change,
    compatible_foundation_metric_ids,
    derive_q4_additive_from_fy_quarters,
    derive_q4_additive_from_fy_ytd,
    derive_q4_growth_from_amounts,
    derive_q4_margin_from_components,
    display_value,
    promise_progress_product_v2_sha256,
    serialize_product_v2_shadow,
    serialize_promise_progress_product_v2,
)
from pbi_xbrl.longitudinal_memory.promise_progress_projection import (
    build_promise_progress_product,
    serialize_promise_progress_product,
    serialize_shadow_matrix,
)
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.sector_packs.retail import (
    RETAIL_SECTOR_PACK,
    RETAIL_SECTOR_PACK_V2,
    RetailSemanticError,
    derive_store_remodels_right_sizes,
    parse_guidance_currency_millions,
    parse_guidance_percent_v2,
    parse_reported_store_remodels,
    parse_reported_store_right_sizes,
)
from pbi_xbrl.longitudinal_memory.source_adapter import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import (
    load_anf_profile,
    load_anf_profile_v2,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_evidence_foundation import (
    SOURCE_SET_ID as EVIDENCE_FOUNDATION_SOURCE_SET_ID,
    build_anf_evidence_foundation,
    candidate_artifacts as evidence_foundation_artifacts,
)
from pbi_xbrl.promise_progress_workbook_preview import (
    EXPECTED_ANF_PRODUCT_SHA256,
    EXPECTED_ANF_SHADOW_SHA256,
    EXPECTED_ANF_WORKBOOK_SHA256,
    PRODUCT_V2_PRESENTATION_CONTRACT_ID,
    SUCCESSOR_PRODUCT_V2_PRESENTATION_CONTRACT_ID,
    PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID,
    PromiseProgressWorkbookPreviewError,
    _cell_text,
    _parse_xml,
    _resolve_target_sheet,
    _serialize_xml,
    _shared_strings,
    _worksheet_cell_map,
    build_promise_progress_workbook_binding_plan_v2,
    build_workbook_trace_v2,
    canonical_workbook_content_sha256,
    materialize_promise_progress_preview_v2,
    sha256_file,
    target_sheet_semantic_sha256_v2,
    validate_preview_semantics_v2,
    validate_preview_structure_v2,
    validate_preview_visual_fit_v2,
    validate_promise_progress_workbook_binding_plan_v2,
    replay_ooxml_numeric_display,
)
from scripts.build_anf_promise_progress_product_v2 import (
    COUNT_RECONCILIATION_KIND_SCHEMA_ID,
    COUNT_RECONCILIATION_REQUIRED_KINDS,
    FINAL_CLOSURE_MANIFEST_FILENAMES,
    SOURCE_SET_ID,
    _json_bytes,
    _write_visual_markdown,
    build_actual_definition_compatibility_report,
    build_anf_product_v2_source_set,
    build_capability_completion_report,
    build_legacy_capability_completeness_report,
    build_needs_review_audit,
    build_needs_review_semantics_review,
    build_numeric_cell_text_audit,
    build_progression_q4_update_audit,
    build_q4_derivation_audit,
    build_bounded_derivation_report,
    build_foundation_projection_disposition_report,
    build_guidance_completeness_report,
    build_actual_reconciliation_report,
    build_progress_reconciliation_report,
    build_q4_reconciliation_report,
    build_derivation_lineage_report,
    build_status_report,
    build_defect_closure_report,
    build_current_defect_closure_report,
    build_current_count_reconciliation_report,
    count_reconciliation_kind_schema_state,
    current_count_reconciliation_invariant_checks,
    validate_current_count_reconciliation_report,
    build_quarter_guidance_coverage_report,
    build_result_event_semantic_report,
    build_range_parser_replay_report,
    build_timeline_actual_progress_role_report,
    build_timeline_blank_completeness_report,
    build_timeline_knowledge_date_report,
)


REPO = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
LEGACY_WORKBOOK = SOURCE_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
DESIGN_LOCK = SOURCE_ROOT / "audit" / "promise_progress_design_lock"
EVIDENCE_AUDIT_ROOT = (
    SOURCE_ROOT / "audit" / "anf_local_source_review_authority_expansion_audit_2026-08-09"
)
FINAL_EXHAUSTIVE_AUDIT_ROOT = (
    SOURCE_ROOT
    / "audit"
    / "promise_progress_product_v2_1_final_exhaustive_semantic_reconciliation_acceptance_audit"
)
V1_ORACLE = REPO / "tests" / "fixtures" / "promise_progress" / "anf_legacy_oracle.v1.json"
V2_SOURCE_SET_GOLDEN = (
    REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v2.json"
)
V2_PRODUCT_GOLDEN = REPO / "tests" / "fixtures" / "promise_progress" / "anf_product.v2.json"
V2_SHADOW_GOLDEN = REPO / "tests" / "fixtures" / "promise_progress" / "anf_shadow.v2.json"
V2_MANIFEST_GOLDEN = (
    REPO
    / "tests"
    / "fixtures"
    / "promise_progress"
    / "anf_product_v2_golden_manifest.v1.json"
)
V2_1_SOURCE_SET_GOLDEN = (
    REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v2_1.json"
)
V2_1_FOUNDATION_IDENTITY_GOLDEN = (
    REPO
    / "tests"
    / "fixtures"
    / "longitudinal_memory"
    / "anf_evidence_foundation_identity.v2_1.json"
)
V2_1_PRODUCT_GOLDEN = (
    REPO / "tests" / "fixtures" / "promise_progress" / "anf_product.v2_1.json"
)
V2_1_SHADOW_GOLDEN = (
    REPO / "tests" / "fixtures" / "promise_progress" / "anf_shadow.v2_1.json"
)
V2_1_COUNT_REPORT_GOLDEN = (
    REPO
    / "tests"
    / "fixtures"
    / "promise_progress"
    / "anf_count_reconciliation.v2_1.json"
)
V2_1_MANIFEST_GOLDEN = (
    REPO
    / "tests"
    / "fixtures"
    / "promise_progress"
    / "anf_product_v2_1_golden_manifest.v1.json"
)

EXPECTED_V2_SOURCE_SET_SHA256 = "73a385b0d9c351b5356c34b06ef8d3bb71fcc9f9b503f278bdc550621016a877"
EXPECTED_V2_PRODUCT_SHA256 = "72266543e1c122691dfcdd6d0ee0e472707e527029e719a50881c417da328a05"
EXPECTED_V2_SHADOW_SHA256 = "c32fe7e85d69b1811b92e12e2e55d36fdb63930d8704d48638855c08d62de3c6"
EXPECTED_V2_WORKBOOK_SHA256 = "9476c01ef38945a0e0641a3f1ca8d38c8bc66ccf73eed691a753928c86e24b1d"
EXPECTED_V2_CANONICAL_OOXML_SHA256 = "6d9f3653cd4bddedb5f9e41ac2c8994f05443a31ab217c321b3b3e8a0450c09c"
EXPECTED_V2_TARGET_SEMANTIC_SHA256 = "ff97dc2064c83b574c4c3f27c6a5b83a9b6e6b4ede8bd2ce0886ec9e544f4a28"
EXPECTED_V2_TRACE_SHA256 = "4e06902da38963c04e439a921248946550247c7cf26c5be3943837b561a34f7c"
EXPECTED_V2_MANIFEST_FILE_SHA256 = "d9cf40475d444043fdb3b21507b1efd2ca05115a62287ce406e77e8e6d5a7d3e"
EXPECTED_V2_MANIFEST_DIGEST = "db9c8c27ee37c4275768dcd34fc7e11b64e82e6eb21b4c1fdcca3fd4e5dfbc30"

EXPECTED_SUCCESSOR_SOURCE_SET_SHA256 = (
    "2c7c51768e2d2ec426f3155c43610fe2c5ee1a4f81b8664925bc30c9d0037217"
)
EXPECTED_SUCCESSOR_PRODUCT_SHA256 = (
    "ec2b98c41ce05566bec53133fb05b92c1a77b65ad890cd94f71dc0cc1a515584"
)
EXPECTED_SUCCESSOR_SHADOW_SHA256 = (
    "094ba58548643587b93eb07e96a42742ddf297f8b3702937c72a83f5196007bc"
)
EXPECTED_SUCCESSOR_WORKBOOK_SHA256 = (
    "48c4ea0ddef8f710c07c1a0acde03a5004a98afc15046b9f24cf817ea40178e4"
)
EXPECTED_SUCCESSOR_CANONICAL_OOXML_SHA256 = (
    "6b70ba37e71376812bb09f21b9ed8212184b5f216236a3347ac1cf0a9fba6680"
)
EXPECTED_SUCCESSOR_TARGET_SEMANTIC_SHA256 = (
    "f04f842064bf637d0bffaa217509e6d093639f1b63e6ad04ed546437e6b93c62"
)
EXPECTED_SUCCESSOR_TRACE_SHA256 = (
    "d01665d73a692f057f8b0c782ecfcde0139e567aa1c54724c1efcfac6013ef4b"
)
EXPECTED_SUCCESSOR_COUNT_REPORT_SHA256 = (
    "8b761e2b3a6e923e3d956302443cf20087839725ee09eecacab23182befed5a2"
)
EXPECTED_SUCCESSOR_FOUNDATION_IDENTITY_SHA256 = (
    "8fa26d58c3d4b59897fca3e1eb5ec92255008d861775016c268c71b9333dd82c"
)
EXPECTED_SUCCESSOR_FOUNDATION_SHA256 = (
    "8dc5b59fd1128e5837e4a2ecc0eb9ad3bb69b70c146aea7f71078d46dc6ddf5b"
)
EXPECTED_V2_1_GOLDEN_MANIFEST_SHA256 = (
    "3c4893fdcd190f5f184e53e44254f170d67da24a723399f0a94adf39501881cc"
)


def _strict_json(path: Path) -> dict:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)


def _manifest_artifacts(manifest: dict) -> dict[str, dict]:
    artifacts = {row["relative_path"]: row for row in manifest["artifacts"]}
    assert len(artifacts) == len(manifest["artifacts"])
    return artifacts


@pytest.fixture(scope="module")
def product_v1():
    oracle = _strict_json(V1_ORACLE)
    package = build_source_native_sidecar(
        REPO / oracle["source_package_fixture"],
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    ).package
    return build_promise_progress_product(package, oracle["projection_plan"])


@pytest.fixture(scope="module")
def candidate(tmp_path_factory):
    root = tmp_path_factory.mktemp("promise-progress-product-v2")
    source_set = build_anf_product_v2_source_set(
        source_root=SOURCE_ROOT, repository_root=REPO
    )
    source_set_path = root / "source_set.json"
    source_set_path.write_bytes(_json_bytes(source_set))
    package = build_source_native_sidecar(
        source_set_path,
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK_V2,
        ticker_profile_loader=load_anf_profile_v2,
    ).package
    product = build_promise_progress_product_v2(
        package,
        source_set_id=source_set["source_set_id"],
        reviewed_links=source_set["reviewed_links"],
    )
    plan = build_promise_progress_workbook_binding_plan_v2(
        product, design_lock_root=DESIGN_LOCK
    )
    first = root / "first.xlsx"
    second = root / "second.xlsx"
    first_result = materialize_promise_progress_preview_v2(
        product,
        plan,
        legacy_workbook=LEGACY_WORKBOOK,
        output_workbook=first,
        design_lock_root=DESIGN_LOCK,
    )
    second_result = materialize_promise_progress_preview_v2(
        product,
        plan,
        legacy_workbook=LEGACY_WORKBOOK,
        output_workbook=second,
        design_lock_root=DESIGN_LOCK,
    )
    return SimpleNamespace(
        root=root,
        source_set=source_set,
        package=package,
        product=product,
        plan=plan,
        first=first,
        second=second,
        first_result=first_result,
        second_result=second_result,
    )


@pytest.fixture(scope="module")
def successor_candidate(tmp_path_factory):
    root = tmp_path_factory.mktemp("promise-progress-product-v2-successor")
    adapter_source_set = build_anf_product_v2_source_set(
        source_root=SOURCE_ROOT,
        repository_root=REPO,
        successor=True,
    )
    source_set_path = root / "source_set.json"
    source_set_path.write_bytes(_json_bytes(adapter_source_set))
    package = build_source_native_sidecar(
        source_set_path,
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK_V2,
        ticker_profile_loader=load_anf_profile_v2,
    ).package
    evidence_foundation = build_anf_evidence_foundation(
        source_root=SOURCE_ROOT,
        audit_root=EVIDENCE_AUDIT_ROOT,
    )
    source_set = evidence_foundation_artifacts(evidence_foundation)[
        "expanded_source_set.json"
    ]
    product = build_promise_progress_product_v2(
        package,
        source_set_id=evidence_foundation["source_set_id"],
        reviewed_links=adapter_source_set["reviewed_links"],
        product_version=SUCCESSOR_PRODUCT_VERSION,
        evidence_foundation=evidence_foundation,
    )
    plan = build_promise_progress_workbook_binding_plan_v2(
        product, design_lock_root=DESIGN_LOCK
    )
    first = root / "first.xlsx"
    second = root / "second.xlsx"
    first_result = materialize_promise_progress_preview_v2(
        product,
        plan,
        legacy_workbook=LEGACY_WORKBOOK,
        output_workbook=first,
        design_lock_root=DESIGN_LOCK,
    )
    second_result = materialize_promise_progress_preview_v2(
        product,
        plan,
        legacy_workbook=LEGACY_WORKBOOK,
        output_workbook=second,
        design_lock_root=DESIGN_LOCK,
    )
    structural = validate_preview_structure_v2(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=first,
        plan=plan,
    )
    semantic = validate_preview_semantics_v2(product, plan, preview_workbook=first)
    visual = validate_preview_visual_fit_v2(preview_workbook=first, plan=plan)
    return SimpleNamespace(
        root=root,
        source_set=source_set,
        adapter_source_set=adapter_source_set,
        evidence_foundation=evidence_foundation,
        package=package,
        product=product,
        plan=plan,
        first=first,
        second=second,
        first_result=first_result,
        second_result=second_result,
        structural=structural,
        semantic=semantic,
        visual=visual,
    )


def _blocks(candidate) -> dict:
    return {block.block_id: block for block in candidate.product.blocks}


def _series(candidate, *, year: int, metric_id: str) -> dict:
    periods = {row["period_id"]: row for row in candidate.package["periods"]}
    matches = [
        row
        for row in candidate.package["entities"]
        if row["payload"]["kind"] == "GuidanceSeries"
        and row["payload"]["metric_id"] == metric_id
        and periods[row["payload"]["horizon_period_id"]]["fiscal_year"] == year
    ]
    assert len(matches) == 1
    return matches[0]


def _series_versions(candidate, series: dict) -> list[dict]:
    return sorted(
        [
            row
            for row in candidate.package["observations"]
            if row["payload"]["kind"] == "GuidanceVersion"
            and row["payload"]["guidance_series_id"] == series["header"]["entity_id"]
        ],
        key=lambda row: row["header"]["publication_date"],
    )


def _facts(candidate, metric_id: str) -> list[dict]:
    return [
        row
        for row in candidate.package["observations"]
        if row["payload"]["kind"] == "NumericalFact"
        and row["payload"]["metric_id"] == metric_id
    ]


def _product_row(candidate, block_id: str, metric_id: str, year: int | None = None):
    matches = [
        row
        for row in _blocks(candidate)[block_id].rows
        if row.metric_id == metric_id
        and (year is None or row.horizon_label == f"FY{year}")
    ]
    assert len(matches) == 1
    return matches[0]


def test_product_v1_hashes_remain_frozen(product_v1) -> None:
    assert hashlib.sha256(serialize_promise_progress_product(product_v1)).hexdigest() == EXPECTED_ANF_PRODUCT_SHA256
    assert hashlib.sha256(serialize_shadow_matrix(product_v1)).hexdigest() == EXPECTED_ANF_SHADOW_SHA256
    assert sha256_file(LEGACY_WORKBOOK) == EXPECTED_ANF_WORKBOOK_SHA256


def test_product_v2_has_explicit_candidate_identity(candidate) -> None:
    assert candidate.product.product_type == PRODUCT_TYPE == "PromiseProgressProduct@2"
    assert candidate.product.product_version == PRODUCT_VERSION == "2.0.0-candidate"
    assert candidate.product.source_set_id == SOURCE_SET_ID
    assert candidate.product.coverage_state in COVERAGE_STATES


def test_product_v2_serialization_is_closed_and_deterministic(candidate) -> None:
    payload = serialize_promise_progress_product_v2(candidate.product)
    assert payload == serialize_promise_progress_product_v2(candidate.product)
    parsed = json.loads(payload)
    assert list(parsed) == sorted(parsed)
    assert promise_progress_product_v2_sha256(candidate.product) == hashlib.sha256(payload).hexdigest()
    with pytest.raises(dataclasses.FrozenInstanceError):
        candidate.product.coverage_state = "complete_for_reviewed_scope"


def test_product_v2_golden_manifest_pins_the_ultra_reviewed_package() -> None:
    payload = V2_MANIFEST_GOLDEN.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == EXPECTED_V2_MANIFEST_FILE_SHA256
    manifest = _strict_json(V2_MANIFEST_GOLDEN)
    artifacts = _manifest_artifacts(manifest)

    assert manifest["manifest_type"] == "PromiseProgressProductV2CandidateManifest@1"
    assert manifest["product_id"] == "promise-progress-product:anf@2"
    assert manifest["product_version"] == PRODUCT_VERSION == "2.0.0-candidate"
    assert manifest["product_sha256"] == EXPECTED_V2_PRODUCT_SHA256
    assert manifest["artifact_count"] == len(artifacts) == 29
    assert artifacts["source_set_v2_candidate.json"]["sha256"] == EXPECTED_V2_SOURCE_SET_SHA256
    assert artifacts["product_v2_candidate.json"]["sha256"] == EXPECTED_V2_PRODUCT_SHA256
    assert artifacts["shadow_v2_candidate.json"]["sha256"] == EXPECTED_V2_SHADOW_SHA256
    assert artifacts["ANF_Promise_Progress_source_native_v2_preview.xlsx"]["sha256"] == EXPECTED_V2_WORKBOOK_SHA256
    assert artifacts["ANF_Promise_Progress_source_native_v2_preview_repeat.xlsx"]["sha256"] == EXPECTED_V2_WORKBOOK_SHA256
    assert artifacts["workbook_trace_v2.json"]["sha256"] == EXPECTED_V2_TRACE_SHA256

    regeneration = manifest["fresh_regeneration"]
    assert regeneration["raw_byte_identical"] is True
    assert regeneration["canonical_content_identical"] is True
    assert regeneration["target_semantic_identical"] is True
    assert regeneration["first_raw_sha256"] == regeneration["second_raw_sha256"] == EXPECTED_V2_WORKBOOK_SHA256
    assert regeneration["first_canonical_content_sha256"] == regeneration["second_canonical_content_sha256"] == EXPECTED_V2_CANONICAL_OOXML_SHA256
    assert regeneration["first_target_semantic_sha256"] == regeneration["second_target_semantic_sha256"] == EXPECTED_V2_TARGET_SEMANTIC_SHA256

    without_digest = dict(manifest)
    manifest_digest = without_digest.pop("manifest_digest")
    assert manifest_digest == EXPECTED_V2_MANIFEST_DIGEST
    assert hashlib.sha256(_json_bytes(without_digest)).hexdigest() == manifest_digest
    assert {row["relative_path"] for row in manifest["publication_exclusions"]} == {
        "rendered.zip"
    }
    assert "rendered.zip" not in artifacts
    assert not any("__pycache__" in path.casefold() for path in artifacts)


def test_product_v2_regeneration_matches_versioned_source_product_and_shadow_goldens(
    candidate,
) -> None:
    source_set_payload = _json_bytes(candidate.source_set)
    product_payload = serialize_promise_progress_product_v2(candidate.product)
    shadow_payload = serialize_product_v2_shadow(
        build_product_v2_shadow(candidate.product, candidate.package)
    )

    assert source_set_payload == V2_SOURCE_SET_GOLDEN.read_bytes()
    assert product_payload == V2_PRODUCT_GOLDEN.read_bytes()
    assert shadow_payload == V2_SHADOW_GOLDEN.read_bytes()
    assert hashlib.sha256(source_set_payload).hexdigest() == EXPECTED_V2_SOURCE_SET_SHA256
    assert hashlib.sha256(product_payload).hexdigest() == EXPECTED_V2_PRODUCT_SHA256
    assert hashlib.sha256(shadow_payload).hexdigest() == EXPECTED_V2_SHADOW_SHA256


def test_product_v1_and_product_v2_golden_contracts_coexist(product_v1, candidate) -> None:
    assert _strict_json(V1_ORACLE)["fixture_id"].endswith("@1")
    assert _strict_json(V2_SOURCE_SET_GOLDEN)["source_set_id"].endswith("@2")
    assert _strict_json(V2_PRODUCT_GOLDEN)["product_type"] == "PromiseProgressProduct@2"
    assert _strict_json(V2_PRODUCT_GOLDEN)["product_version"] == "2.0.0-candidate"
    assert product_v1.product_type == "PromiseProgressProduct@1"
    assert candidate.product.product_type == "PromiseProgressProduct@2"
    assert hashlib.sha256(serialize_promise_progress_product(product_v1)).hexdigest() == EXPECTED_ANF_PRODUCT_SHA256


def test_product_v2_workbook_and_trace_regeneration_match_golden_checkpoint(
    candidate,
) -> None:
    assert sha256_file(candidate.first) == sha256_file(candidate.second) == EXPECTED_V2_WORKBOOK_SHA256
    assert canonical_workbook_content_sha256(candidate.first) == EXPECTED_V2_CANONICAL_OOXML_SHA256
    assert canonical_workbook_content_sha256(candidate.second) == EXPECTED_V2_CANONICAL_OOXML_SHA256
    assert target_sheet_semantic_sha256_v2(candidate.first, candidate.plan) == EXPECTED_V2_TARGET_SEMANTIC_SHA256
    assert target_sheet_semantic_sha256_v2(candidate.second, candidate.plan) == EXPECTED_V2_TARGET_SEMANTIC_SHA256

    trace = build_workbook_trace_v2(
        candidate.product,
        candidate.plan,
        preview_workbook=candidate.first,
    )
    assert hashlib.sha256(_json_bytes(trace)).hexdigest() == EXPECTED_V2_TRACE_SHA256


@pytest.mark.parametrize("year", [2022, 2023, 2024])
def test_reviewed_historical_source_coverage_is_activated(candidate, year) -> None:
    documents = [
        row for row in candidate.source_set["documents"] if row["publication_date"].startswith(str(year))
    ]
    assert len(documents) == 4
    assert all(row["required"] and row["review_state"] == "reviewed" for row in documents)
    assert all((SOURCE_ROOT / row["relative_path"]).is_file() for row in documents)
    assert all(sha256_file(SOURCE_ROOT / row["relative_path"]) == row["expected_sha256"] for row in documents)


@pytest.mark.parametrize("year", [2022, 2023, 2024])
@pytest.mark.parametrize(
    "metric_id",
    [
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:capital-expenditures@1",
    ],
)
def test_each_historical_year_has_canonical_guidance_series(candidate, year, metric_id) -> None:
    series = _series(candidate, year=year, metric_id=metric_id)
    assert _series_versions(candidate, series)
    row = _product_row(candidate, PROGRESSION_BLOCK_ID, metric_id, year)
    assert row.progression_values


def test_signed_negative_percentage_range_is_lossless() -> None:
    assert parse_guidance_percent_v2("down in the range of 2 to 3%") == {
        "kind": "range",
        "low": "-3",
        "high": "-2",
        "low_inclusive": True,
        "high_inclusive": True,
    }


def test_flat_to_up_is_a_zero_to_positive_range() -> None:
    assert parse_guidance_percent_v2("flat to up 2%") == {
        "kind": "range",
        "low": "0",
        "high": "2",
        "low_inclusive": True,
        "high_inclusive": True,
    }


@pytest.mark.parametrize(
    ("text", "low", "high"),
    (
        ("2 to 3%", "2", "3"),
        ("2-3%", "2", "3"),
        ("2 – 3%", "2", "3"),
        ("-3 to -2%", "-3", "-2"),
        ("-3--2%", "-3", "-2"),
    ),
)
def test_single_terminal_percent_range_is_lossless(text, low, high) -> None:
    assert parse_guidance_percent_v2(text) == {
        "kind": "range",
        "low": low,
        "high": high,
        "low_inclusive": True,
        "high_inclusive": True,
    }


@pytest.mark.parametrize(
    "text",
    ("$2 to 3%", "2% to $3", "2 to 3 million", "~2 to 3%"),
)
def test_ambiguous_or_unrepresentable_percent_ranges_fail_closed(text) -> None:
    with pytest.raises(RetailSemanticError):
        parse_guidance_percent_v2(text)


@pytest.mark.parametrize(
    "text",
    (
        "$200–$225m",
        "$200-$225m",
        "$200 to $225m",
        "$200–225m",
        "$200 to 225 million",
        "$200 million to $225 million",
        "$200m to $225m",
    ),
)
def test_currency_million_ranges_are_lossless_and_share_units(text) -> None:
    assert parse_guidance_currency_millions(text) == {
        "kind": "range",
        "low": "200",
        "high": "225",
        "low_inclusive": True,
        "high_inclusive": True,
    }


@pytest.mark.parametrize(
    "text",
    (
        "$200m–225%",
        "$200 to 225 shares",
        "€200–$225m",
        "$200m to $225bn",
        "200 to $225m",
        "$200 to $225",
    ),
)
def test_currency_million_ranges_reject_mixed_or_ambiguous_units(text) -> None:
    with pytest.raises(RetailSemanticError):
        parse_guidance_currency_millions(text)


@pytest.mark.parametrize(
    "text",
    (
        "approximately $200–$225m",
        "around $200 to $225 million",
        "~$200-$225m",
    ),
)
def test_approximate_currency_ranges_fail_instead_of_collapsing_to_a_point(text) -> None:
    with pytest.raises(RetailSemanticError, match="Approximate currency-million ranges"):
        parse_guidance_currency_millions(text)


def test_currency_range_endpoint_order_is_not_silently_rewritten() -> None:
    with pytest.raises(RetailSemanticError, match="lower endpoint exceeds upper endpoint"):
        parse_guidance_currency_millions("$225-$200m")


@pytest.mark.parametrize(
    ("text", "expected"),
    (
        ("Capital expenditures of approximately $150 million", {"kind": "approximate", "value": "150", "qualifier": "tilde", "tolerance": None}),
        ("Capital expenditures of $225 million", {"kind": "exact", "value": "225"}),
    ),
)
def test_currency_million_point_forms_remain_unchanged(text, expected) -> None:
    assert parse_guidance_currency_millions(text) == expected


def test_all_seven_single_terminal_percent_assertions_replay_losslessly(candidate) -> None:
    report = build_range_parser_replay_report(
        candidate.product, candidate.package, candidate.source_set
    )
    assert report["affected_assertion_count"] == 7
    assert all(row["old_parsed_value"]["kind"] == "exact" for row in report["rows"])
    assert all(row["corrected_typed_value"]["kind"] == "range" for row in report["rows"])
    assert report["after_change_type_counts"] == {
        "Initial": 29,
        "Lower bound raised": 5,
        "Lowered": 2,
        "Qualitative → range": 1,
        "Raised": 8,
        "Range narrowed": 1,
        "Range shifted higher": 5,
        "Range shifted lower": 4,
        "Range → approximate": 5,
        "Range → minimum": 1,
        "Range → qualitative": 1,
        "Reaffirmed": 31,
        "Updated — not directly comparable": 1,
        "Upper bound raised": 1,
    }


def test_fy2022_operating_margin_range_sequence_and_outcome_are_corrected(candidate) -> None:
    row = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:operating-margin@1", 2022
    )
    assert [value.display_text for value in row.progression_values] == [
        "5%–6%",
        "1%–3%",
        "2%–3%",
    ]
    assert row.actual_display == "2.5%"
    assert row.status_at_update == "Hit"
    affected = [
        value
        for value in _blocks(candidate)[TIMELINE_BLOCK_ID].rows
        if value.metric_id == "metric:core:operating-margin@1"
        and value.horizon_label == "FY2022"
    ]
    assert [value.change_type for value in sorted(affected, key=lambda value: value.event_date)] == [
        "Initial",
        "Range shifted lower",
        "Lower bound raised",
    ]


def test_qualitative_direction_is_not_lost(candidate) -> None:
    series = _series(candidate, year=2022, metric_id="metric:core:revenue-growth@1")
    values = [row["payload"]["value"] for row in _series_versions(candidate, series)]
    qualitative = next(value for value in values if value["kind"] == "qualitative")
    assert qualitative["normalized_band"] == "negative-mid-single-digits"
    assert "down" in qualitative["text"].casefold()


def test_fy2025_eps_guidance_uses_per_share_metric_and_unit(candidate) -> None:
    series = _series(
        candidate, year=2025, metric_id="metric:core:net-income-per-diluted-share@1"
    )
    assert series["payload"]["unit_id"] == "unit:core:currency-per-share@1"
    displays = [value.display_text for value in _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:net-income-per-diluted-share@1", 2025
    ).progression_values]
    assert displays == [
        "$10.40–$11.40",
        "$9.50–$10.50",
        "$10.00–$10.50",
        "$10.20–$10.50",
        "$10.30–$10.40",
    ]


def test_reported_and_adjusted_eps_facts_remain_distinct(candidate) -> None:
    facts = _facts(candidate, "metric:core:net-income-per-diluted-share@1")
    rows = {
        (row["payload"]["definition_id"], row["payload"]["basis_id"]): row["payload"]["value"]["value"]
        for row in facts
        if row["header"]["effective_period_id"] == "period:anf:fy2025@1"
    }
    assert rows[("definition:core:company-reported@1", "basis:core:reported@1")] == "10.46"
    assert rows[("definition:core:adjusted-non-gaap@1", "basis:core:adjusted-non-gaap@1")] == "9.86"
    eps = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:net-income-per-diluted-share@1", 2025
    )
    assert eps.actual_display == "$10.46"
    assert len(eps.actual_candidate_record_ids) == 2


def test_fy2025_capex_guidance_is_approximate_currency(candidate) -> None:
    row = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:capital-expenditures@1", 2025
    )
    assert [value.display_text for value in row.progression_values] == [
        "~$200m",
        "~$200m",
        "~$225m",
        "~$225m",
        "~$245m",
    ]
    assert all(value.canonical_value["kind"] == "approximate" for value in row.progression_values)


def test_capex_actual_definition_mismatch_fails_closed(candidate) -> None:
    row = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:capital-expenditures@1", 2025
    )
    assert row.actual_value is None and row.actual_display == ""
    assert row.investor_reason_code == "definition_equivalence_unreviewed"
    purchase = _facts(candidate, "metric:core:property-equipment-purchases@1")
    assert len(purchase) == 4
    periods = {row["period_id"]: row for row in candidate.package["periods"]}
    fy2025 = next(
        value
        for value in purchase
        if periods[value["header"]["effective_period_id"]]["fiscal_year"] == 2025
    )
    assert fy2025["payload"]["value"]["value"] == "240.774"


def test_fy2025_net_sales_actual_is_source_backed_six_percent(candidate) -> None:
    row = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:revenue-growth@1", 2025
    )
    assert row.actual_value == {"kind": "exact", "value": "6"}
    assert row.actual_display == "6%"
    assert len(row.actual_candidate_record_ids) == 1


def test_comparable_sales_cannot_substitute_for_net_sales(candidate) -> None:
    row = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:revenue-growth@1", 2025
    )
    selected = next(
        value
        for value in candidate.package["observations"]
        if value["header"]["record_id"] in row.actual_candidate_record_ids
    )
    assert selected["payload"]["metric_id"] == "metric:core:revenue-growth@1"
    assert "comparable" not in selected["payload"]["definition_id"]


def test_reported_and_adjusted_margin_bases_remain_distinct(candidate) -> None:
    row = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:operating-margin@1", 2025
    )
    facts = [
        value
        for value in _facts(candidate, "metric:core:operating-margin@1")
        if value["header"]["record_id"] in row.actual_candidate_record_ids
    ]
    assert {
        (value["payload"]["definition_id"], value["payload"]["basis_id"], value["payload"]["value"]["value"])
        for value in facts
    } == {
        ("definition:core:company-reported@1", "basis:core:reported@1", "13.3"),
        (
            "definition:core:adjusted-excluding-litigation-benefit@1",
            "basis:core:adjusted-excluding-litigation-benefit@1",
            "12.5",
        ),
    }
    assert row.actual_value == {"kind": "exact", "value": "13.3"}
    assert row.actual_display == "13.3%"
    assert row.status_at_update == "Beat"
    assert row.investor_reason_code is None


def test_row_eligibility_is_closed_and_fail_closed(candidate) -> None:
    assert all(row.eligible for block in candidate.product.blocks for row in block.rows)
    row = _blocks(candidate)[OPEN_BLOCK_ID].rows[0]
    with pytest.raises(PromiseProgressProductV2Error, match="eligibility"):
        dataclasses.replace(row, current_value=None, current_display="")


def test_diagnostic_and_legacy_capacity_rows_are_excluded(candidate) -> None:
    rows = [row for block in candidate.product.blocks for row in block.rows]
    assert not {
        "diagnostic_coverage_gap",
        "parity_only",
        "reserved_capacity",
        "legacy_only",
    } & {row.row_kind for row in rows}


def test_block_eligibility_has_no_empty_historical_shell(candidate) -> None:
    progression = _blocks(candidate)[PROGRESSION_BLOCK_ID]
    years = {row.horizon_label for row in progression.rows}
    assert years == {"FY2022", "FY2023", "FY2024", "FY2025"}
    assert all(any(row.horizon_label == year for row in progression.rows) for year in years)


def test_open_guidance_contains_current_versions_only(candidate) -> None:
    rows = _blocks(candidate)[OPEN_BLOCK_ID].rows
    assert len(rows) == 10
    assert all(row.version_state == "Current" and row.horizon_label == "FY2026" for row in rows)


def test_version_state_vocabulary_and_history(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    assert {row.version_state for row in timeline} <= VERSION_STATES
    assert {row.version_state for row in timeline} >= {"Current", "Superseded", "Final"}


def test_historical_versions_are_not_presented_as_current_open(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    assert all(row.version_state != "Current" for row in timeline if row.horizon_label != "FY2026")
    visible_version_bindings = [
        binding
        for binding in candidate.plan.bindings
        if binding.block_id == TIMELINE_BLOCK_ID and binding.field_role == "version_state"
    ]
    assert visible_version_bindings == []
    status_bindings = [
        binding
        for binding in candidate.plan.bindings
        if binding.block_id == TIMELINE_BLOCK_ID and binding.field_role == "status"
    ]
    assert all(binding.presentation_text not in {"Current", "Final", "Superseded"} for binding in status_bindings)


def test_change_type_is_typed_separately_from_machine_reason(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    assert {row.change_type for row in timeline} <= CHANGE_TYPES
    assert all(row.comparison_reason_code for row in timeline)
    assert any(row.change_type == "Range → minimum" for row in timeline)
    assert any(row.change_type == "Range → approximate" for row in timeline)


def test_range_to_qualitative_change_is_typed_without_direction() -> None:
    label, reason = classify_change(
        {"kind": "qualitative", "text": "down mid-single digits", "normalized_band": "negative-mid-single-digits"},
        {"kind": "range", "low": "0", "high": "2"},
    )
    assert label == "Range → qualitative"
    assert reason == "value_form_changed"


def test_incomparable_change_remains_non_directional() -> None:
    label, reason = classify_change(
        {"kind": "range", "low": "4", "high": "6"},
        {"kind": "exact", "value": "5"},
    )
    assert label == "Updated — not directly comparable"
    assert reason == "value_form_changed"


def test_disclosure_events_and_timeline_are_newest_first(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    keys = [(row.event_date, row.event_id) for row in timeline]
    assert keys == sorted(keys, key=lambda value: (value[0], value[1]), reverse=True)
    assert all(row.event_id for row in timeline)


def test_unrelated_same_date_documents_are_not_grouped(candidate) -> None:
    package = copy.deepcopy(candidate.package)
    original = package["source_documents"][0]
    duplicate = copy.deepcopy(original)
    duplicate["document_key"] = "same-date-unrelated-test-document"
    duplicate["source_document_id"] = original["source_document_id"] + "|unrelated=test"
    package["source_documents"].append(duplicate)
    events_by_id, _ = _event_indexes(package, candidate.source_set["reviewed_links"])
    events = tuple(events_by_id.values())
    by_date: dict[str, list] = {}
    for event in events:
        by_date.setdefault(event.event_date, []).append(event)
    same_date_groups = [values for values in by_date.values() if len(values) > 1]
    assert same_date_groups
    assert all(
        len(event.source_document_ids) == 1 or event.reviewed_relation_ids
        for event in events
    )


def test_reviewed_release_and_transcript_form_one_version_per_metric(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    march_event_rows = [row for row in timeline if row.event_date == "2026-03-04"]
    assert len(march_event_rows) == 10
    assert {row.metric_id for row in march_event_rows} == {
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:capital-expenditures@1",
        "metric:core:share-repurchases@1",
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
    }
    assert all(row.version_state == "Current" for row in march_event_rows)
    assert all(row.source_summary == "Mar 4 2026 release + transcript" for row in march_event_rows)
    assert all(len(row.current_source_document_ids) == 2 for row in march_event_rows)
    assert len({row.event_id for row in march_event_rows}) == 1


def test_current_source_is_separate_from_predecessor_lineage(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    with_predecessor = [row for row in timeline if row.predecessor_source_document_ids]
    assert with_predecessor
    assert all(
        set(row.current_source_document_ids).isdisjoint(row.predecessor_source_document_ids)
        for row in with_predecessor
    )
    source_bindings = [
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field"
        and binding.block_id == TIMELINE_BLOCK_ID
        and binding.field_role == "metric"
    ]
    assert all(binding.current_source_document_ids for binding in source_bindings)
    assert any(binding.predecessor_source_document_ids for binding in source_bindings)
    assert not any(
        binding.field_role == "current_source" for binding in candidate.plan.bindings
    )


def test_no_investor_engine_jargon(candidate) -> None:
    visible = " ".join(
        binding.presentation_text
        for binding in candidate.plan.bindings
        if not binding.anchor_cell.startswith("O")
    ).casefold()
    assert not {
        "guidanceseries",
        "canonical",
        "resolver",
        "binding",
        "occurrence",
        "legacy parity only",
        "unsupported mapping",
        "unresolved comparison",
        "source-native product@2 candidate",
    } & {term for term in visible.splitlines()}
    assert all(term not in visible for term in ("guidanceseries", "legacy parity only", "unresolved comparison"))


def test_stated_in_is_visible_and_typed(candidate) -> None:
    stated = [
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field"
        and binding.block_id == TIMELINE_BLOCK_ID
        and binding.field_role == "stated_in"
    ]
    assert stated
    assert all(binding.stated_in_period_id and binding.stated_in_display for binding in stated)
    assert all(binding.presentation_text == binding.stated_in_display for binding in stated)
    headers = [binding.presentation_text for binding in candidate.plan.bindings if binding.binding_kind == "table_header"]
    assert "Stated in" in headers


def test_stable_row_identity_is_independent_of_physical_row(candidate) -> None:
    row_numbers = {
        binding.source_row_id: int(binding.anchor_cell[1:])
        for binding in candidate.plan.bindings
        if binding.binding_kind == "row_trace"
    }
    assert set(row_numbers) == {
        row.row_id for block in candidate.product.blocks for row in block.rows
    }
    assert all("row=" not in row_id for row_id in row_numbers)


def test_dynamic_vertical_layout_has_no_reserved_capacity(candidate) -> None:
    assert candidate.plan.used_range == f"A1:O{len(candidate.plan.row_plan)}"
    assert len(candidate.plan.row_plan) != 102
    assert all(row.row_kind != "reserved_capacity" for row in candidate.plan.row_plan)
    assert [row.block_id for row in candidate.plan.row_plan if row.row_kind == "block_title"] == list(BLOCK_ORDER)


def test_timeline_has_exactly_one_header(candidate) -> None:
    assert len([
        row for row in candidate.plan.row_plan
        if row.block_id == TIMELINE_BLOCK_ID and row.row_kind == "table_header"
    ]) == 1


def test_preview_has_zero_clipping(candidate) -> None:
    result = validate_preview_visual_fit_v2(preview_workbook=candidate.first, plan=candidate.plan)
    assert result["passed"] and result["clipped_visible_field_count"] == 0


def test_preview_has_zero_overflow_dependency(candidate) -> None:
    result = validate_preview_visual_fit_v2(preview_workbook=candidate.first, plan=candidate.plan)
    assert result["overflow_dependency_count"] == 0


def test_metricwide_standard_grid_and_hidden_support_contract_remain_exact(candidate) -> None:
    result = validate_preview_structure_v2(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=candidate.first,
        plan=candidate.plan,
    )
    assert result["passed"]
    assert result["column_contract"]["A"] == {"width": 31.5, "hidden": False}
    assert all(
        result["column_contract"][column] == {"width": 22.5, "hidden": False}
        for column in "BCDEFGHIJ"
    )
    assert result["column_contract"]["K"] == {"width": 4.0, "hidden": True}
    assert result["column_contract"]["L"] == {"width": 4.0, "hidden": True}
    assert result["column_contract"]["M"] == {"width": 4.0, "hidden": True}
    assert result["column_contract"]["N"] == {"width": 4.0, "hidden": True}
    assert result["column_contract"]["O"] == {"width": 13.0, "hidden": True}


def test_k_l_m_n_blank_and_o_contains_only_row_ids(candidate) -> None:
    result = validate_preview_structure_v2(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=candidate.first,
        plan=candidate.plan,
    )
    assert result["k_nonblank"] == {}
    assert result["l_nonblank"] == {} and result["m_nonblank"] == {} and result["n_nonblank"] == {}
    assert set(result["o_row_ids"].values()) == {
        row.row_id for block in candidate.product.blocks for row in block.rows
    }


def test_product_v2_materializer_has_no_legacy_value_fallback() -> None:
    source = inspect.getsource(materialize_promise_progress_preview_v2).casefold()
    assert "legacy_value" not in source and "fallback" not in source


def test_product_v2_layout_has_no_ticker_branch() -> None:
    source = inspect.getsource(build_promise_progress_workbook_binding_plan_v2)
    assert "ticker" not in source.casefold()
    assert 'company_id ==' not in source


def test_fresh_regeneration_is_deterministic(candidate) -> None:
    assert candidate.first.read_bytes() == candidate.second.read_bytes()
    assert canonical_workbook_content_sha256(candidate.first) == canonical_workbook_content_sha256(candidate.second)
    assert target_sheet_semantic_sha256_v2(candidate.first, candidate.plan) == target_sheet_semantic_sha256_v2(candidate.second, candidate.plan)


def test_binding_plan_rebuild_is_deterministic(candidate) -> None:
    rebuilt = build_promise_progress_workbook_binding_plan_v2(
        candidate.product, design_lock_root=DESIGN_LOCK
    )
    assert rebuilt.to_dict() == candidate.plan.to_dict()
    assert rebuilt.presentation_contract.contract_id == PRODUCT_V2_PRESENTATION_CONTRACT_ID


def test_dynamic_plan_fails_closed_on_duplicate_destination(candidate) -> None:
    duplicate = dataclasses.replace(
        candidate.plan,
        bindings=(candidate.plan.bindings[0], candidate.plan.bindings[0], *candidate.plan.bindings[1:]),
    )
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="identities|destinations"):
        validate_promise_progress_workbook_binding_plan_v2(
            candidate.product, duplicate, design_lock_root=DESIGN_LOCK
        )


def test_dynamic_plan_fails_closed_on_unknown_transform(candidate) -> None:
    first = dataclasses.replace(candidate.plan.bindings[0], display_transform_id="unknown@1")
    mutated = dataclasses.replace(candidate.plan, bindings=(first, *candidate.plan.bindings[1:]))
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="display transform"):
        validate_promise_progress_workbook_binding_plan_v2(
            candidate.product, mutated, design_lock_root=DESIGN_LOCK
        )


def test_workbook_trace_preserves_current_and_predecessor_roles(candidate) -> None:
    trace = build_workbook_trace_v2(candidate.product, candidate.plan, preview_workbook=candidate.first)
    assert trace["record_count"] == len(candidate.plan.bindings)
    timeline = [
        row for row in trace["records"]
        if row["binding_kind"] == "product_field"
        and row["block_id"] == TIMELINE_BLOCK_ID
        and row["field_role"] == "metric"
    ]
    assert timeline
    assert all(row["current_source_document_ids"] for row in timeline)
    assert any(row["predecessor_source_document_ids"] for row in timeline)


def test_preview_semantic_validation_passes(candidate) -> None:
    result = validate_preview_semantics_v2(candidate.product, candidate.plan, preview_workbook=candidate.first)
    assert result["passed"] and all(result["validations"].values())


def test_only_target_sheet_and_append_only_styles_change(candidate) -> None:
    result = validate_preview_structure_v2(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=candidate.first,
        plan=candidate.plan,
    )
    assert result["changed_ooxml_parts"] == ["xl/styles.xml", "xl/worksheets/sheet7.xml"]
    assert result["unexpected_ooxml_parts"] == []


def test_source_set_v1_fixture_is_not_modified(candidate) -> None:
    source_set_path = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
    assert _strict_json(source_set_path)["source_set_id"].endswith("@1")
    assert candidate.source_set["source_set_id"].endswith("@2")


def test_coverage_state_is_compact_product_metadata(candidate) -> None:
    metadata = next(binding for binding in candidate.plan.bindings if binding.binding_kind == "product_metadata")
    assert candidate.product.coverage_state == "partial_reviewed_source_coverage"
    assert metadata.presentation_text == (
        "Data through Jul 29, 2026 · Guidance history FY2022–FY2026 · "
        "Some full-year Actual comparisons pending review"
    )
    assert "Product@2" not in metadata.presentation_text
    assert "candidate" not in metadata.presentation_text.casefold()


def test_credibility_is_one_typed_unavailable_state(candidate) -> None:
    block = _blocks(candidate)[CREDIBILITY_BLOCK_ID]
    assert block.block_state == "assessment_unavailable"
    assert len(block.rows) == 1
    assert block.rows[0].row_kind == "assessment_unavailable"
    assert block.rows[0].investor_reason_display == "Management credibility assessment pending reviewed evidence."


def test_progression_headers_restore_compact_legacy_like_slots(candidate) -> None:
    headers = [
        row for row in candidate.plan.row_plan
        if row.block_id == PROGRESSION_BLOCK_ID and row.row_kind == "table_header"
    ]
    assert len(headers) == 4
    labels = [dict(row.header_labels) for row in headers]
    expected = {
        "metric": "Metric",
        "version_1": "Initial guide",
        "version_2": "Q1 update",
        "version_3": "Q2 update",
        "version_4": "Q3 update",
        "version_5": "Q4 update",
        "actual": "Actual",
        "status": "Status",
    }
    assert all(values == expected for values in labels)


def test_progression_groups_are_newest_year_first(candidate) -> None:
    labels = [
        row.display_label for row in candidate.plan.row_plan
        if row.block_id == PROGRESSION_BLOCK_ID and row.row_kind == "group_title"
    ]
    assert labels == ["FY2025", "FY2024", "FY2023", "FY2022"]


def test_target_sheet_contains_no_empty_trailing_frames(candidate) -> None:
    with ZipFile(candidate.first, "r") as archive:
        _, part = _resolve_target_sheet(archive, "Promise_Progress_UI")
        root = _parse_xml(archive.read(part))
        shared = _shared_strings(archive)
        cells = _worksheet_cell_map(root)
        max_row = max(int("".join(filter(str.isdigit, ref))) for ref in cells)
        assert max_row == len(candidate.plan.row_plan)
        assert _cell_text(cells[f"O{max_row}"], shared)


def test_open_guidance_removes_redundant_current_column_but_retains_state(candidate) -> None:
    layouts = {
        row.field_role: (row.start_column, row.end_column)
        for row in candidate.plan.presentation_contract.field_layouts
        if row.block_id == OPEN_BLOCK_ID
    }
    assert layouts == {
        "metric": ("A", "A"),
        "current_guide": ("B", "C"),
        "horizon": ("D", "E"),
        "status": ("F", "F"),
    }
    assert all(row.version_state == "Current" for row in _blocks(candidate)[OPEN_BLOCK_ID].rows)
    assert all(
        binding.version_state == "Current"
        for binding in candidate.plan.bindings
        if binding.source_row_id
        in {row.row_id for row in _blocks(candidate)[OPEN_BLOCK_ID].rows}
    )


def test_progression_removes_redundant_final_column_but_retains_state(candidate) -> None:
    layouts = {
        row.field_role: (row.start_column, row.end_column)
        for row in candidate.plan.presentation_contract.field_layouts
        if row.block_id == PROGRESSION_BLOCK_ID
    }
    assert layouts == {
        "metric": ("A", "A"),
        "version_1": ("B", "B"),
        "version_2": ("C", "C"),
        "version_3": ("D", "D"),
        "version_4": ("E", "E"),
        "version_5": ("F", "F"),
        "actual": ("G", "G"),
        "status": ("H", "H"),
    }
    progression_rows = _blocks(candidate)[PROGRESSION_BLOCK_ID].rows
    assert all(row.version_state == "Final" for row in progression_rows)
    assert all(
        binding.version_state == "Final"
        for binding in candidate.plan.bindings
        if binding.source_row_id in {row.row_id for row in progression_rows}
    )


def test_timeline_uses_one_header_event_groups_and_ten_investor_roles(candidate) -> None:
    bands = [
        row
        for row in candidate.plan.row_plan
        if row.block_id == TIMELINE_BLOCK_ID and row.row_kind == "event_group"
    ]
    assert [row.group_id for row in bands] == [
        event.event_id for event in candidate.product.disclosure_events
    ]
    assert all(row.display_label.endswith(" revisions") for row in bands)
    assert len(
        [
            row
            for row in candidate.plan.row_plan
            if row.block_id == TIMELINE_BLOCK_ID and row.row_kind == "table_header"
        ]
    ) == 1
    layouts = {
        row.field_role: (row.start_column, row.end_column)
        for row in candidate.plan.presentation_contract.field_layouts
        if row.block_id == TIMELINE_BLOCK_ID
    }
    assert layouts == {
        "metric": ("A", "A"),
        "previous_guide": ("B", "B"),
        "current_guide": ("C", "C"),
        "change_type": ("D", "D"),
        "actual": ("E", "E"),
        "progress": ("F", "F"),
        "status": ("G", "G"),
        "horizon": ("H", "H"),
        "stated_in": ("I", "I"),
        "source_date": ("J", "J"),
    }


def test_horizon_stated_in_and_lifecycle_remain_in_every_timeline_trace_record(candidate) -> None:
    trace = build_workbook_trace_v2(candidate.product, candidate.plan, preview_workbook=candidate.first)
    timeline_records = [
        row
        for row in trace["records"]
        if row["source_row_id"]
        and row["block_id"] == TIMELINE_BLOCK_ID
    ]
    assert timeline_records
    assert all(row["horizon_period_id"] and row["horizon_label"] for row in timeline_records)
    assert all(row["stated_in_period_id"] and row["stated_in_display"] for row in timeline_records)
    assert all(row["version_state"] in VERSION_STATES for row in timeline_records)
    assert trace["product_id"] == candidate.product.product_id
    assert all(row["product_version"] == "2.0.0-candidate" for row in timeline_records)


def test_disclosure_event_source_is_not_visible_but_is_complete_in_trace(candidate) -> None:
    bindings = [
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field"
        and binding.block_id == TIMELINE_BLOCK_ID
        and binding.field_role == "metric"
    ]
    by_event: dict[str, list] = {}
    for binding in bindings:
        by_event.setdefault(str(binding.event_id), []).append(binding)
    assert set(by_event) == {event.event_id for event in candidate.product.disclosure_events}
    assert all(sum(row.event_start for row in rows) == 1 for rows in by_event.values())
    assert all(row.current_source_document_ids for row in bindings)
    assert not any(
        binding.field_role in {"current_source", "notes_source", "source_note"}
        for binding in candidate.plan.bindings
    )


def test_event_start_border_is_deterministic_and_not_date_grouped(candidate) -> None:
    structural = validate_preview_structure_v2(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=candidate.first,
        plan=candidate.plan,
    )
    assert structural["validations"]["event_start_separators"]
    assert structural["event_start_borders"]
    assert {
        (row["style"], row["rgb"]) for row in structural["event_start_borders"].values()
    } == {("thin", "FF9FBAD0")}
    starts = [
        row
        for row in candidate.plan.row_plan
        if row.block_id == TIMELINE_BLOCK_ID and row.row_kind == "product_row" and row.event_start
    ]
    assert [row.group_id for row in starts] == [event.event_id for event in candidate.product.disclosure_events]


def test_lifecycle_state_is_trace_only_and_outcome_status_is_visible(candidate) -> None:
    structural = validate_preview_structure_v2(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=candidate.first,
        plan=candidate.plan,
    )
    assert structural["lifecycle_styles"] == {}
    assert structural["validations"]["lifecycle_state_not_in_investor_cells"]
    status_bindings = [
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field" and binding.field_role == "status"
    ]
    assert status_bindings
    assert all(binding.style_role == f"status:{binding.status_code}" for binding in status_bindings)
    assert all(binding.version_state in VERSION_STATES for binding in status_bindings)


def test_lifecycle_style_cannot_be_substituted_with_outcome_status(candidate) -> None:
    target = next(
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field" and binding.field_role == "status"
    )
    mutated = dataclasses.replace(
        target, style_role="lifecycle:current-information"
    )
    plan = dataclasses.replace(
        candidate.plan,
        bindings=tuple(mutated if row.binding_id == target.binding_id else row for row in candidate.plan.bindings),
    )
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="outcome Status"):
        validate_promise_progress_workbook_binding_plan_v2(
            candidate.product, plan, design_lock_root=DESIGN_LOCK
        )


@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    (
        ({"kind": "range", "low": "9.5", "high": "10.5"}, {"kind": "range", "low": "10", "high": "10.5"}, "Lower bound raised"),
        ({"kind": "range", "low": "10.2", "high": "10.5"}, {"kind": "range", "low": "10.3", "high": "10.4"}, "Range narrowed"),
        ({"kind": "range", "low": "5", "high": "7"}, {"kind": "range", "low": "4", "high": "7"}, "Lower bound lowered"),
        ({"kind": "range", "low": "5", "high": "7"}, {"kind": "range", "low": "5", "high": "8"}, "Upper bound raised"),
        ({"kind": "range", "low": "5", "high": "7"}, {"kind": "range", "low": "5", "high": "6"}, "Upper bound lowered"),
        ({"kind": "range", "low": "5", "high": "7"}, {"kind": "range", "low": "4", "high": "8"}, "Range widened"),
        ({"kind": "range", "low": "5", "high": "7"}, {"kind": "range", "low": "6", "high": "8"}, "Range shifted higher"),
        ({"kind": "range", "low": "5", "high": "7"}, {"kind": "range", "low": "4", "high": "6"}, "Range shifted lower"),
        ({"kind": "qualitative", "text": "down mid-single digits"}, {"kind": "range", "low": "-3", "high": "-2"}, "Qualitative → range"),
        ({"kind": "range", "low": "0", "high": "2"}, {"kind": "qualitative", "text": "down mid-single digits"}, "Range → qualitative"),
    ),
)
def test_typed_change_shape_precedence(previous, current, expected) -> None:
    assert classify_change(current, previous)[0] == expected


def test_change_shape_precedence_prefers_shift_over_individual_bound_labels() -> None:
    label, reason = classify_change(
        {"kind": "range", "low": "6", "high": "8"},
        {"kind": "range", "low": "5", "high": "7"},
    )
    assert (label, reason) == ("Range shifted higher", "both_bounds_raised")


def test_review_notes_are_not_visible_but_full_diagnostics_remain_product_owned(candidate) -> None:
    capex = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:capital-expenditures@1", 2025
    )
    margin = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:operating-margin@1", 2025
    )
    older_sales = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:revenue-growth@1", 2024
    )
    assert not any(
        binding.field_role in {"notes_source", "source_note", "current_source"}
        for binding in candidate.plan.bindings
    )
    assert "property/equipment purchases" in capex.investor_reason_display.casefold()
    assert margin.investor_reason_code is None
    assert margin.status_at_update == "Beat"
    assert older_sales.investor_reason_code is None
    assert older_sales.actual_display == "16%" and older_sales.status_at_update == "Beat"


def test_management_credibility_single_unavailable_row_is_unchanged(candidate) -> None:
    block = _blocks(candidate)[CREDIBILITY_BLOCK_ID]
    assert len(block.rows) == 1
    assert block.rows[0].row_kind == "assessment_unavailable"
    assert block.rows[0].version_state == "Needs Review"


def test_compact_investor_block_order_and_titles(candidate) -> None:
    assert [block.block_id for block in candidate.product.blocks] == [
        CREDIBILITY_BLOCK_ID,
        PROGRESSION_BLOCK_ID,
        OPEN_BLOCK_ID,
        TIMELINE_BLOCK_ID,
    ]
    assert [block.title for block in candidate.product.blocks] == [
        "Management Credibility Scorecard",
        "Guidance Progression",
        "2026 Open Guidance",
        "Quarterly Guidance Timeline / Revision Log",
    ]


def test_progression_and_open_show_outcome_status_not_lifecycle(candidate) -> None:
    visible = [
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field"
    ]
    assert not any(binding.field_role == "version_state" for binding in visible)
    for block_id in (PROGRESSION_BLOCK_ID, OPEN_BLOCK_ID):
        rows = _blocks(candidate)[block_id].rows
        statuses = [
            binding
            for binding in visible
            if binding.block_id == block_id and binding.field_role == "status"
        ]
        assert {binding.source_row_id for binding in statuses} == {row.row_id for row in rows}
        assert all(binding.status_code and binding.style_role == f"status:{binding.status_code}" for binding in statuses)
        assert all(binding.presentation_text not in VERSION_STATES - {"Needs Review"} for binding in statuses)


def test_timeline_context_columns_have_distinct_typed_meaning(candidate) -> None:
    timeline_rows = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    by_row = {
        row.row_id: {
            binding.field_role: binding
            for binding in candidate.plan.bindings
            if binding.binding_kind == "product_field" and binding.source_row_id == row.row_id
        }
        for row in timeline_rows
    }
    for row in timeline_rows:
        bindings = by_row[row.row_id]
        assert bindings["horizon"].presentation_text == row.horizon_label
        assert bindings["stated_in"].presentation_text == row.stated_in_display
        assert bindings["source_date"].presentation_text == row.event_date
        assert bindings["horizon"].machine_value == row.horizon_period_id
        assert bindings["stated_in"].machine_value == row.stated_in_period_id
        assert bindings["source_date"].machine_value == row.event_date


def test_pre_release_event_has_typed_reporting_context(candidate) -> None:
    rows = [
        row
        for row in _blocks(candidate)[TIMELINE_BLOCK_ID].rows
        if row.event_date == "2026-01-12"
    ]
    assert rows
    assert {row.stated_in_display for row in rows} == {"2025-Q4 pre-release"}
    assert all(row.stated_in_period_id.endswith("phase=q4-pre-release") for row in rows)


def test_timeline_reporting_update_groups_are_newest_first(candidate) -> None:
    groups = [
        row
        for row in candidate.plan.row_plan
        if row.block_id == TIMELINE_BLOCK_ID and row.row_kind == "event_group"
    ]
    assert [row.group_id for row in groups] == [
        event.event_id for event in candidate.product.disclosure_events
    ]
    assert [row.display_label for row in groups[:6]] == [
        "2026-Q1 revisions",
        "2025-Q4 pre-release revisions",
        "2025-Q4 revisions",
        "2025-Q3 revisions",
        "2025-Q2 revisions",
        "2025-Q1 revisions",
    ]


def test_timeline_visible_status_is_outcome_not_version_state(candidate) -> None:
    rows = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    statuses = {
        binding.source_row_id: binding
        for binding in candidate.plan.bindings
        if binding.binding_kind == "product_field"
        and binding.block_id == TIMELINE_BLOCK_ID
        and binding.field_role == "status"
    }
    assert set(statuses) == {row.row_id for row in rows}
    assert all(statuses[row.row_id].presentation_text == row.status_at_update for row in rows)
    assert all(statuses[row.row_id].status_code == row.status_code_at_update for row in rows)
    assert not {"Current", "Final", "Superseded"} & {
        binding.presentation_text for binding in statuses.values()
    }
    assert {binding.presentation_text for binding in statuses.values()} == {"Open"}


def test_lifecycle_and_lineage_remain_in_shadow_and_trace(candidate) -> None:
    shadow = build_product_v2_shadow(candidate.product, candidate.package)
    shadow_rows = {row["row_id"]: row for row in shadow["row_lineage"]}
    product_rows = [row for block in candidate.product.blocks for row in block.rows]
    assert set(shadow_rows) == {row.row_id for row in product_rows}
    assert all(shadow_rows[row.row_id]["version_state"] == row.version_state for row in product_rows)
    assert all(
        shadow_rows[row.row_id]["current_source_document_ids"]
        == list(row.current_source_document_ids)
        for row in product_rows
    )
    trace = build_workbook_trace_v2(
        candidate.product, candidate.plan, preview_workbook=candidate.first
    )
    assert all(
        record["version_state"] is not None
        for record in trace["records"]
        if record["source_row_id"] is not None
    )


def test_all_normal_investor_rows_use_compact_equal_height(candidate) -> None:
    heights = dict(candidate.plan.row_heights)
    product_rows = [row for row in candidate.plan.row_plan if row.row_kind == "product_row"]
    assert product_rows
    assert {heights[row.row_number] for row in product_rows} == {24}
    assert not any(
        binding.field_role in {"notes_source", "current_source", "source_note"}
        for binding in candidate.plan.bindings
    )
    timeline_block = _blocks(candidate)[TIMELINE_BLOCK_ID]
    target = timeline_block.rows[0]
    mutated_row = dataclasses.replace(
        target, source_summary="Reviewed source identity and lineage. " * 100
    )
    mutated_block = dataclasses.replace(
        timeline_block,
        rows=(mutated_row, *timeline_block.rows[1:]),
    )
    mutated_product = dataclasses.replace(
        candidate.product,
        blocks=tuple(
            mutated_block if block.block_id == TIMELINE_BLOCK_ID else block
            for block in candidate.product.blocks
        ),
    )
    mutated_plan = build_promise_progress_workbook_binding_plan_v2(
        mutated_product, design_lock_root=DESIGN_LOCK
    )
    assert dict(mutated_plan.row_heights) == heights


def test_economic_values_fit_without_provenance_or_overflow(candidate) -> None:
    visual = validate_preview_visual_fit_v2(
        preview_workbook=candidate.first, plan=candidate.plan
    )
    economic_roles = {
        "current_guide",
        "previous_guide",
        "actual",
        "progress",
        "change_type",
        "horizon",
    }
    records = [row for row in visual["records"] if row["field_role"] in economic_roles]
    assert records
    assert all(row["fit"] and not row["overflow_dependency"] for row in records)


def test_compact_progression_grid_and_left_alignment_are_exact(candidate) -> None:
    layouts = {
        row.field_role: (row.start_column, row.end_column)
        for row in candidate.plan.presentation_contract.field_layouts
        if row.block_id == PROGRESSION_BLOCK_ID
    }
    assert layouts["actual"] == ("G", "G")
    assert layouts["status"] == ("H", "H")
    assert set(column for start, end in layouts.values() for column in (start, end)) <= set("ABCDEFGH")
    progression = [
        binding
        for binding in candidate.plan.bindings
        if binding.block_id == PROGRESSION_BLOCK_ID
        and binding.binding_kind in {"product_field", "table_header"}
    ]
    assert all(
        binding.horizontal_alignment == ("center" if binding.field_role == "status" else "left")
        for binding in progression
    )


def test_compact_open_and_timeline_grids_use_at_most_a_through_j(candidate) -> None:
    assert candidate.plan.presentation_contract.visible_columns == tuple("ABCDEFGHIJ")
    open_layouts = {
        row.field_role: (row.start_column, row.end_column)
        for row in candidate.plan.presentation_contract.field_layouts
        if row.block_id == OPEN_BLOCK_ID
    }
    assert open_layouts == {
        "metric": ("A", "A"),
        "current_guide": ("B", "C"),
        "horizon": ("D", "E"),
        "status": ("F", "F"),
    }
    timeline_layouts = {
        row.field_role: (row.start_column, row.end_column)
        for row in candidate.plan.presentation_contract.field_layouts
        if row.block_id == TIMELINE_BLOCK_ID
    }
    assert timeline_layouts["metric"] == ("A", "A")
    assert timeline_layouts["change_type"] == ("D", "D")
    assert timeline_layouts["source_date"] == ("J", "J")


def test_single_column_change_type_uses_only_closed_lossless_compaction(candidate) -> None:
    current_change_bindings = [
        binding
        for binding in candidate.plan.bindings
        if binding.field_role == "change_type"
    ]
    assert current_change_bindings
    assert all(binding.display_range == binding.anchor_cell for binding in current_change_bindings)
    assert all(binding.fit_measurement["fit"] for binding in current_change_bindings)

    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID]
    synthetic_row = dataclasses.replace(
        timeline.rows[0],
        change_type="Updated — not directly comparable",
        comparison_reason_code="synthetic_incomparable_review_case",
    )
    synthetic_block = dataclasses.replace(
        timeline, rows=(synthetic_row, *timeline.rows[1:])
    )
    synthetic_product = dataclasses.replace(
        candidate.product,
        blocks=tuple(
            synthetic_block if block.block_id == TIMELINE_BLOCK_ID else block
            for block in candidate.product.blocks
        ),
    )
    synthetic_plan = build_promise_progress_workbook_binding_plan_v2(
        synthetic_product, design_lock_root=DESIGN_LOCK
    )
    compacted = [
        binding
        for binding in synthetic_plan.bindings
        if binding.source_row_id == synthetic_row.row_id
        and binding.field_role == "change_type"
    ]
    assert len(compacted) == 1
    assert {
        (binding.canonical_display_text, binding.presentation_text)
        for binding in compacted
    } == {("Updated — not directly comparable", "Not directly comparable")}
    assert all(binding.fit_measurement["fit"] for binding in compacted)


def test_every_visible_needs_review_has_one_closed_final_reason(candidate) -> None:
    audit = build_needs_review_audit(candidate.product, candidate.package)
    assert audit["visible_needs_review_count"] == 9
    assert audit["unresolved_correctable_count"] == 0
    assert {row["category"] for row in audit["rows"]} <= {"A", "B", "C"}
    assert all(row["reason_code"] in NEEDS_REVIEW_REASONS for row in audit["rows"])
    assert {
        row["reason_code"] for row in audit["rows"]
    } == {
        "assessment_unavailable",
        "approximate_target_direction_ambiguous",
        "definition_equivalence_unreviewed",
    }


def test_fy2025_reported_eps_uses_normal_outcome_rule(candidate) -> None:
    row = _product_row(
        candidate,
        PROGRESSION_BLOCK_ID,
        "metric:core:net-income-per-diluted-share@1",
        2025,
    )
    assert row.current_display == "$10.30–$10.40"
    assert row.actual_display == "$10.46"
    assert row.status_code_at_update == "beat"
    assert row.status_at_update == "Beat"
    assert row.investor_reason_code is None


@pytest.mark.parametrize(
    ("year", "metric_id", "actual", "status"),
    (
        (2022, "metric:core:revenue-growth@1", "0%", "Beat"),
        (2022, "metric:core:operating-margin@1", "2.5%", "Hit"),
        (2023, "metric:core:revenue-growth@1", "16%", "Beat"),
        (2023, "metric:core:operating-margin@1", "11.3%", "Beat"),
        (2024, "metric:core:revenue-growth@1", "16%", "Beat"),
        (2024, "metric:core:operating-margin@1", "15%", "Hit"),
    ),
)
def test_historical_source_backed_actual_coverage(candidate, year, metric_id, actual, status) -> None:
    row = _product_row(candidate, PROGRESSION_BLOCK_ID, metric_id, year)
    assert row.actual_display == actual
    assert row.status_at_update == status
    assert row.actual_period_id and row.actual_knowledge_date
    assert row.actual_source_document_ids


def test_fy2026_open_guidance_completeness_and_legacy_capability_audit(candidate) -> None:
    rows = _blocks(candidate)[OPEN_BLOCK_ID].rows
    by_metric = {row.metric_id: row for row in rows}
    assert set(by_metric) == {
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:capital-expenditures@1",
        "metric:core:share-repurchases@1",
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
    }
    assert {metric_id: row.current_display for metric_id, row in by_metric.items()} == {
        "metric:core:revenue-growth@1": "3%–5%",
        "metric:core:operating-margin@1": "12%–12.5%",
        "metric:core:net-income-per-diluted-share@1": "$10.20–$11.00",
        "metric:core:diluted-weighted-average-shares@1": "~45m shares",
        "metric:core:capital-expenditures@1": "$200m–$225m",
        "metric:core:share-repurchases@1": "~$450m",
        "metric:retail:net-store-openings@1": "~30",
        "metric:retail:store-openings@1": "~55",
        "metric:retail:store-closures-count@1": "~25",
        "metric:retail:store-remodels-right-sizes@1": "~70",
    }
    assert all(row.horizon_label == "FY2026" for row in rows)
    assert all(row.status_at_update == "Open" for row in rows)
    report = build_legacy_capability_completeness_report()
    assert report["local_sources_only"] and not report["legacy_value_fallback"]
    included = {
        row["capability"]
        for row in report["rows"]
        if row["scope"] == "FY2026 open guidance" and row["classification_id"] == 1
    }
    assert included == {
        "net income per diluted share",
        "capital expenditures",
        "share repurchases",
        "diluted shares / share count",
        "real estate / store plan",
    }
    assert not any(row["classification_id"] == 2 for row in report["rows"])


def test_fy2026_capex_source_product_workbook_and_trace_are_lossless(candidate) -> None:
    assertion = next(
        row
        for row in candidate.source_set["required_assertions"]
        if row["assertion_key"] == "guidance-fy2026-capital-expenditures-release"
    )
    assert assertion["document_key"] == "anf-release-2026-03-04"
    assert assertion["value_parser_id"] == "parser:retail:guidance-currency-millions@1"
    assert assertion["locator"]["excerpt"].endswith(
        "Capital expenditures | In The Range of $200 to $225 million"
    )

    expected = {
        "kind": "range",
        "low": "200",
        "high": "225",
        "low_inclusive": True,
        "high_inclusive": True,
    }
    series = _series(
        candidate,
        year=2026,
        metric_id="metric:core:capital-expenditures@1",
    )
    assert series["payload"]["unit_id"] == "unit:core:currency-million@1"
    assert series["payload"]["basis_id"] == "basis:core:guided@1"
    versions = _series_versions(candidate, series)
    assert len(versions) == 1
    assert versions[0]["payload"]["value"] == expected

    row = _product_row(
        candidate,
        OPEN_BLOCK_ID,
        "metric:core:capital-expenditures@1",
        2026,
    )
    assert row.current_value == expected
    assert row.current_display == "$200m–$225m"
    assert row.horizon_label == "FY2026"
    assert row.status_at_update == "Open"

    trace = build_workbook_trace_v2(
        candidate.product,
        candidate.plan,
        preview_workbook=candidate.first,
    )
    record = next(
        value
        for value in trace["records"]
        if value["source_row_id"] == row.row_id and value["field_role"] == "current_guide"
    )
    assert record["machine_value"] == expected
    assert record["canonical_display_text"] == "$200m–$225m"
    assert record["written_display_value"] == "$200m–$225m"
    assert record["display_range"] == "B41:C41"
    assert any("key=anf-release-2026-03-04" in source_id for source_id in record["current_source_document_ids"])

    with ZipFile(candidate.first, "r") as archive:
        _, part = _resolve_target_sheet(archive, "Promise_Progress_UI")
        cells = _worksheet_cell_map(_parse_xml(archive.read(part)))
        assert _cell_text(cells["B41"], _shared_strings(archive)) == "$200m–$225m"


def test_timeline_actual_and_progress_are_event_time_eligible(candidate) -> None:
    report = build_timeline_knowledge_date_report(candidate.product)
    assert report["actual_population_count"] == 27
    assert report["progress_population_count"] == 6
    assert report["future_actual_leakage_count"] == 0
    assert report["future_progress_leakage_count"] == 0
    assert all(
        row["status"] == "Open"
        for row in report["rows"]
    )


@pytest.mark.parametrize(
    ("period_type", "progress_semantic", "same_year", "eligible", "expected"),
    (
        ("quarter", None, True, True, "event_period_actual"),
        ("ytd", None, True, True, "ytd_progress"),
        ("ytd", "cumulative", True, True, "cumulative_progress"),
        ("quarter", "annualized_run_rate", True, True, "annualized_run_rate"),
        ("quarter", "delta_to_target", True, True, "delta_progress"),
        ("quarter", None, False, True, "incompatible"),
        ("quarter", None, True, False, "incompatible"),
    ),
)
def test_timeline_fact_role_assignment_is_typed(
    period_type, progress_semantic, same_year, eligible, expected
) -> None:
    assert classify_timeline_fact_role(
        period_type=period_type,
        same_target_fiscal_year=same_year,
        eligible_by_event_cutoff=eligible,
        progress_semantic=progress_semantic,
    ) == expected


def test_timeline_role_report_partitions_quarter_actuals_and_genuine_progress(candidate) -> None:
    report = build_timeline_actual_progress_role_report(candidate.product, candidate.package)
    assert report["timeline_row_count"] == 95
    assert report["role_counts"] == {
        "event_period_actual": 27,
        "ytd_progress": 6,
        "unavailable": 62,
    }
    assert report["same_fact_dual_role_count"] == 0
    assert report["future_actual_leakage_count"] == 0
    assert report["future_progress_leakage_count"] == 0
    assert report["status_replay"]["before_counts"] == {"Open": 95}
    assert report["status_replay"]["after_counts"] == {"Open": 95}
    assert report["status_replay"]["changed_rows"] == []
    actual_rows = [
        row for row in report["rows"] if row["assigned_role"] == "event_period_actual"
    ]
    assert {row["fact_period_type"] for row in actual_rows} == {"quarter"}
    assert {row["source_date"] for row in actual_rows} >= {
        "2025-05-29",
        "2025-08-28",
        "2025-11-26",
    }
    progress_rows = [
        row for row in report["rows"] if row["assigned_role"] == "ytd_progress"
    ]
    assert {row["fact_period_type"] for row in progress_rows} == {"ytd"}


def test_one_fact_cannot_populate_both_timeline_actual_and_progress(candidate) -> None:
    row = next(
        value
        for value in _blocks(candidate)[TIMELINE_BLOCK_ID].rows
        if value.actual_value is not None and value.progress_value is None
    )
    with pytest.raises(PromiseProgressProductV2Error, match="both Timeline Actual and Progress"):
        dataclasses.replace(
            row,
            progress_value=row.actual_value,
            progress_display=row.actual_display,
            progress_candidate_record_ids=row.actual_candidate_record_ids,
            progress_period_id=row.actual_period_id,
            progress_knowledge_date=row.actual_knowledge_date,
            progress_source_document_ids=row.actual_source_document_ids,
        )


def test_future_actual_and_progress_evidence_fail_closed(candidate) -> None:
    timeline_row = next(
        row
        for row in _blocks(candidate)[TIMELINE_BLOCK_ID].rows
        if row.progress_value is not None
    )
    final_eps = _product_row(
        candidate,
        PROGRESSION_BLOCK_ID,
        "metric:core:net-income-per-diluted-share@1",
        2025,
    )
    with pytest.raises(PromiseProgressProductV2Error, match="Actual leaks evidence"):
        dataclasses.replace(
            timeline_row,
            actual_value=final_eps.actual_value,
            actual_display=final_eps.actual_display,
            actual_candidate_record_ids=final_eps.actual_candidate_record_ids,
            actual_period_id=final_eps.actual_period_id,
            actual_knowledge_date="2099-01-01",
            actual_source_document_ids=final_eps.actual_source_document_ids,
        )
    with pytest.raises(PromiseProgressProductV2Error, match="Progress leaks evidence"):
        dataclasses.replace(timeline_row, progress_knowledge_date="2099-01-01")


def test_pre_release_excludes_later_actual_and_progress(candidate) -> None:
    rows = [
        row
        for row in _blocks(candidate)[TIMELINE_BLOCK_ID].rows
        if row.event_date == "2026-01-12"
    ]
    assert rows
    assert {row.horizon_label for row in rows} == {"FY2025"}
    assert {row.stated_in_display for row in rows} == {"2025-Q4 pre-release"}
    assert all(row.actual_value is None and row.progress_value is None for row in rows)
    assert all(row.status_at_update == "Open" for row in rows)


def test_trace_retains_full_actual_and_progress_lineage(candidate) -> None:
    trace = build_workbook_trace_v2(
        candidate.product, candidate.plan, preview_workbook=candidate.first
    )
    by_row = {
        row.row_id: row for block in candidate.product.blocks for row in block.rows
    }
    records = [row for row in trace["records"] if row["source_row_id"]]
    assert records
    assert all(
        row["actual_period_id"] == by_row[row["source_row_id"]].actual_period_id
        and row["actual_knowledge_date"] == by_row[row["source_row_id"]].actual_knowledge_date
        and row["progress_period_id"] == by_row[row["source_row_id"]].progress_period_id
        and row["progress_knowledge_date"] == by_row[row["source_row_id"]].progress_knowledge_date
        for row in records
    )


def test_metricwide_and_standard_widths_are_closed_generic_fit_classes(candidate) -> None:
    contract = candidate.plan.presentation_contract
    assert contract.width_classes == (("MetricWide", 31.5), ("Standard", 22.5))
    assert contract.column_width_classes == (
        ("A", "MetricWide"),
        ("B", "Standard"),
        ("C", "Standard"),
        ("D", "Standard"),
        ("E", "Standard"),
        ("F", "Standard"),
        ("G", "Standard"),
        ("H", "Standard"),
        ("I", "Standard"),
        ("J", "Standard"),
    )
    assert 31.5 / 22.5 == pytest.approx(1.4)
    visible = [
        binding
        for binding in candidate.plan.bindings
        if binding.binding_kind in {"product_field", "table_header"}
    ]
    assert visible and all(binding.fit_measurement["fit"] for binding in visible)


def test_needs_review_audit_is_exact_replayable_and_complete(candidate) -> None:
    audit = build_needs_review_audit(candidate.product, candidate.package)
    visible = {
        row.row_id
        for block in candidate.product.blocks
        for row in block.rows
        if row.status_code_at_update == "needs_review"
    }
    audited = {row["product_row_id"] for row in audit["rows"]}
    assert audited == visible
    required = {
        "product_row_id",
        "metric",
        "horizon",
        "final_guidance_or_target",
        "candidate_actual",
        "candidate_progress",
        "current_reason_code",
        "source_evidence",
        "definition_and_basis",
        "can_resolve_generically",
        "final_proposed_status",
        "remaining_blocker",
    }
    assert all(required <= set(row) for row in audit["rows"])
    assert all(
        row["final_proposed_status"] == "Needs Review"
        and row["remaining_blocker"]
        and not row["can_resolve_generically"]
        for row in audit["rows"]
    )


def test_approximate_target_exact_hit_directional_beat_and_ambiguity(candidate) -> None:
    exact_center = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:operating-margin@1", 2024
    )
    favorable_side = _product_row(
        candidate, PROGRESSION_BLOCK_ID, "metric:core:operating-margin@1", 2023
    )
    direction_unknown = _product_row(
        candidate,
        PROGRESSION_BLOCK_ID,
        "metric:core:diluted-weighted-average-shares@1",
        2025,
    )
    assert (exact_center.current_display, exact_center.actual_display, exact_center.status_at_update) == (
        "~15%",
        "15%",
        "Hit",
    )
    assert favorable_side.status_at_update == "Beat"
    assert direction_unknown.status_at_update == "Needs Review"
    assert direction_unknown.investor_reason_code == "approximate_target_direction_ambiguous"


def test_capex_requires_explicit_reviewed_definition_equivalence(candidate) -> None:
    report = build_actual_definition_compatibility_report(candidate.product, candidate.package)
    capex = {
        row["horizon"]: row
        for row in report["rows"]
        if row["metric_id"] == "metric:core:capital-expenditures@1"
    }
    assert capex["FY2022"]["definition_relation_state"] == "reviewed-explicit-definition-equivalence"
    assert capex["FY2022"]["actual_selection_state"] == "selected"
    assert capex["FY2022"]["actual"] == "$164.566m"
    for year in ("FY2023", "FY2024", "FY2025"):
        assert capex[year]["definition_relation_state"] == "definition-relation-unreviewed"
        assert capex[year]["actual_selection_state"] == "not-selected"
        assert capex[year]["needs_review_reason_code"] == "definition_equivalence_unreviewed"
    assert not any(row["legacy_value_fallback"] for row in report["rows"])


def test_three_reviewed_capability_families_are_product_owned(candidate) -> None:
    report = build_capability_completion_report()
    assert report["completed_family_count"] == 3
    assert set(report["completed_families"]) == {
        "share repurchases",
        "diluted weighted-average shares",
        "real estate / store activity",
    }
    assert report["source_boundary"] == "reviewed-local-sources-only"
    assert not report["legacy_value_fallback"]
    assert not report["tariff_standalone_promise_row"]

    progression_metrics = {
        row.metric_id for row in _blocks(candidate)[PROGRESSION_BLOCK_ID].rows
    }
    open_metrics = {row.metric_id for row in _blocks(candidate)[OPEN_BLOCK_ID].rows}
    expected = {
        "metric:core:share-repurchases@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
    }
    assert expected <= progression_metrics
    assert expected <= open_metrics
    assert not any("tariff" in metric.casefold() for metric in progression_metrics | open_metrics)


def test_capability_definitions_and_multipart_store_semantics_remain_distinct(candidate) -> None:
    diluted = _series(
        candidate,
        year=2025,
        metric_id="metric:core:diluted-weighted-average-shares@1",
    )["payload"]
    assert diluted["unit_id"] == "unit:core:shares-million@1"
    assert diluted["definition_id"] == "definition:core:company-guidance@1"

    expected_displays = {
        "metric:core:share-repurchases@1": ("~$450m", "$450m"),
        "metric:retail:net-store-openings@1": ("~40", "40"),
        "metric:retail:store-openings@1": ("~60", "62"),
        "metric:retail:store-closures-count@1": ("~20", "22"),
        "metric:retail:store-remodels-right-sizes@1": ("~40", "58"),
    }
    for metric_id, (guidance, actual) in expected_displays.items():
        row = _product_row(candidate, PROGRESSION_BLOCK_ID, metric_id, 2025)
        assert (row.current_display, row.actual_display) == (guidance, actual)


def test_remodel_and_right_size_components_are_typed_and_derived_with_full_lineage(candidate) -> None:
    source_text = (
        "On the store fleet, we delivered 120 new store experiences, including 62 new "
        "stores, 11 right sizes, and 47 remodels."
    )
    right_sizes = parse_reported_store_right_sizes(source_text)
    remodels = parse_reported_store_remodels(source_text)
    assert right_sizes == {"kind": "exact", "value": "11"}
    assert remodels == {"kind": "exact", "value": "47"}
    assert derive_store_remodels_right_sizes(right_sizes, remodels) == {
        "kind": "exact",
        "value": "58",
    }

    component_facts = {
        row["payload"]["metric_id"]: row
        for row in candidate.package["observations"]
        if row["payload"]["kind"] == "NumericalFact"
        and row["header"]["effective_period_id"] == "period:anf:fy2025@1"
        and row["payload"]["metric_id"]
        in {
            "metric:retail:store-right-sizes@1",
            "metric:retail:store-remodels@1",
        }
    }
    assert set(component_facts) == {
        "metric:retail:store-right-sizes@1",
        "metric:retail:store-remodels@1",
    }
    assert {
        metric: fact["payload"]["value"]["value"]
        for metric, fact in component_facts.items()
    } == {
        "metric:retail:store-right-sizes@1": "11",
        "metric:retail:store-remodels@1": "47",
    }
    row = _product_row(
        candidate,
        PROGRESSION_BLOCK_ID,
        "metric:retail:store-remodels-right-sizes@1",
        2025,
    )
    component_ids = {
        fact["header"]["record_id"] for fact in component_facts.values()
    }
    assert row.actual_value == {"kind": "exact", "value": "58"}
    assert row.actual_display == "58"
    assert set(row.actual_candidate_record_ids) == component_ids
    assert len(row.actual_source_document_ids) == 1
    assert "key=anf-transcript-2026-03-04" in row.actual_source_document_ids[0]
    assert row.status_at_update == "Needs Review"
    assert row.investor_reason_code == "approximate_target_direction_ambiguous"


def test_needs_review_replay_has_no_correctable_mapping_or_status_deficiency(candidate) -> None:
    audit = build_needs_review_audit(candidate.product, candidate.package)
    assert audit["visible_needs_review_count"] == 9
    assert audit["correctable_mapping_deficiency_count"] == 0
    assert audit["correctable_status_deficiency_count"] == 0
    remodel = next(
        row
        for row in audit["rows"]
        if row["metric_id"] == "metric:retail:store-remodels-right-sizes@1"
    )
    assert remodel["candidate_actual"] == "58"
    assert remodel["current_reason_code"] == "approximate_target_direction_ambiguous"
    assert remodel["current_reason_code"] != "comparable_actual_unavailable"


def test_event_time_capability_progress_uses_typed_ytd_periods(candidate) -> None:
    timeline = _blocks(candidate)[TIMELINE_BLOCK_ID].rows
    capability_metrics = {
        "metric:core:share-repurchases@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
    }
    populated = [
        row
        for row in timeline
        if row.metric_id in capability_metrics and row.progress_value is not None
    ]
    assert populated
    periods = {row["period_id"]: row for row in candidate.package["periods"]}
    assert all(periods[row.progress_period_id]["period_type"] == "ytd" for row in populated)
    assert all(row.progress_knowledge_date <= row.event_date for row in populated)


@pytest.mark.parametrize(
    ("metric_label", "horizon_label"),
    (
        (
            "Enterprise cost rationalization and branch optimization milestones",
            "FY2026",
        ),
        (
            "Commodity conversion economics",
            "Policy-contingent qualitative milestone; no exact deadline disclosed",
        ),
        (
            "Very long investor-relevant operating segment metric label",
            "FY2026",
        ),
    ),
)
def test_compact_width_classes_fit_generic_cross_ticker_cases(
    candidate, metric_label, horizon_label
) -> None:
    block = _blocks(candidate)[OPEN_BLOCK_ID]
    mutated_row = dataclasses.replace(
        block.rows[0], metric_label=metric_label, horizon_label=horizon_label
    )
    mutated_block = dataclasses.replace(block, rows=(mutated_row, *block.rows[1:]))
    product = dataclasses.replace(
        candidate.product,
        blocks=tuple(
            mutated_block if value.block_id == OPEN_BLOCK_ID else value
            for value in candidate.product.blocks
        ),
    )
    plan = build_promise_progress_workbook_binding_plan_v2(
        product, design_lock_root=DESIGN_LOCK
    )
    affected = [
        binding
        for binding in plan.bindings
        if binding.source_row_id == mutated_row.row_id
        and binding.binding_kind == "product_field"
    ]
    assert affected and all(binding.fit_measurement["fit"] for binding in affected)
    row_number = next(
        row.row_number
        for row in plan.row_plan
        if row.source_row_id == mutated_row.row_id
    )
    assert dict(plan.row_heights)[row_number] in {24, 40}


def _q4_test_periods() -> dict[str, dict]:
    return {
        "fy": {
            "period_id": "fy",
            "period_type": "annual",
            "fiscal_year": 2025,
            "calendar_id": "calendar:anf",
        },
        "ytd": {
            "period_id": "ytd",
            "period_type": "ytd",
            "fiscal_year": 2025,
            "fiscal_quarter": 3,
            "calendar_id": "calendar:anf",
        },
        "q1": {"period_id": "q1", "period_type": "quarter", "fiscal_year": 2025, "fiscal_quarter": 1, "calendar_id": "calendar:anf"},
        "q2": {"period_id": "q2", "period_type": "quarter", "fiscal_year": 2025, "fiscal_quarter": 2, "calendar_id": "calendar:anf"},
        "q3": {"period_id": "q3", "period_type": "quarter", "fiscal_year": 2025, "fiscal_quarter": 3, "calendar_id": "calendar:anf"},
        "q4": {"period_id": "q4", "period_type": "quarter", "fiscal_year": 2025, "fiscal_quarter": 4, "calendar_id": "calendar:anf"},
    }


def _q4_test_fact(
    record_id: str,
    value: str,
    period_id: str,
    *,
    metric_id: str = "metric:core:property-equipment-purchases@1",
    definition_id: str = "definition:core:company-reported@1",
    basis_id: str = "basis:core:reported@1",
    unit_id: str = "unit:core:currency-million@1",
    currency: str = "USD",
    scale: str = "million",
    knowledge_date: str = "2026-03-04",
) -> dict:
    return {
        "header": {
            "record_id": record_id,
            "effective_period_id": period_id,
            "dimension_set_id": "dimset:total-company",
            "knowledge_date": knowledge_date,
        },
        "payload": {
            "kind": "NumericalFact",
            "metric_id": metric_id,
            "definition_id": definition_id,
            "basis_id": basis_id,
            "unit_id": unit_id,
            "currency": currency,
            "scale": scale,
            "value": {"kind": "exact", "value": value},
        },
    }


def test_q4_additive_fy_minus_ytd_is_typed_and_lineaged() -> None:
    periods = _q4_test_periods()
    result = derive_q4_additive_from_fy_ytd(
        _q4_test_fact("fy-fact", "170", "fy"),
        _q4_test_fact("ytd-fact", "125", "ytd"),
        periods=periods,
        q4_period_id="q4",
        event_cutoff="2026-03-04",
    )
    assert result.value == {"kind": "exact", "value": "45"}
    assert result.derivation_rule_id == Q4_ADD_FY_MINUS_YTD_RULE_ID
    assert result.input_record_ids == ("fy-fact", "ytd-fact")
    assert result.knowledge_date == "2026-03-04"


def test_q4_additive_fy_minus_q1_q2_q3_is_equivalent() -> None:
    periods = _q4_test_periods()
    result = derive_q4_additive_from_fy_quarters(
        _q4_test_fact("fy-fact", "170", "fy"),
        (
            _q4_test_fact("q1-fact", "40", "q1"),
            _q4_test_fact("q2-fact", "45", "q2"),
            _q4_test_fact("q3-fact", "40", "q3"),
        ),
        periods=periods,
        q4_period_id="q4",
        event_cutoff="2026-03-04",
    )
    assert result.value == {"kind": "exact", "value": "45"}
    assert result.derivation_rule_id == Q4_ADD_FY_MINUS_QUARTERS_RULE_ID
    assert result.input_record_ids == ("fy-fact", "q1-fact", "q2-fact", "q3-fact")


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("basis_id", "basis:core:adjusted@1"),
        ("unit_id", "unit:core:count@1"),
    ),
)
def test_q4_additive_rejects_incompatible_semantics(field, replacement) -> None:
    periods = _q4_test_periods()
    kwargs = {field: replacement}
    with pytest.raises(PromiseProgressProductV2Error, match="incompatible"):
        derive_q4_additive_from_fy_ytd(
            _q4_test_fact("fy-fact", "170", "fy"),
            _q4_test_fact("ytd-fact", "125", "ytd", **kwargs),
            periods=periods,
            q4_period_id="q4",
            event_cutoff="2026-03-04",
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("currency", "EUR"),
        ("scale", "billion"),
    ),
)
def test_q4_additive_rejects_mixed_currency_or_scale(field, replacement) -> None:
    with pytest.raises(PromiseProgressProductV2Error, match="incompatible"):
        derive_q4_additive_from_fy_ytd(
            _q4_test_fact("fy-fact", "170", "fy"),
            _q4_test_fact("ytd-fact", "125", "ytd", **{field: replacement}),
            periods=_q4_test_periods(),
            q4_period_id="q4",
            event_cutoff="2026-03-04",
        )


def test_q4_additive_rejects_mixed_fiscal_calendar() -> None:
    periods = _q4_test_periods()
    periods["ytd"] = {**periods["ytd"], "calendar_id": "calendar:other"}
    with pytest.raises(PromiseProgressProductV2Error, match="fiscal year/calendar"):
        derive_q4_additive_from_fy_ytd(
            _q4_test_fact("fy-fact", "170", "fy"),
            _q4_test_fact("ytd-fact", "125", "ytd"),
            periods=periods,
            q4_period_id="q4",
            event_cutoff="2026-03-04",
        )


def test_q4_additive_rejects_future_input() -> None:
    with pytest.raises(PromiseProgressProductV2Error, match="after its disclosure event"):
        derive_q4_additive_from_fy_ytd(
            _q4_test_fact("fy-fact", "170", "fy"),
            _q4_test_fact(
                "ytd-fact", "125", "ytd", knowledge_date="2026-03-05"
            ),
            periods=_q4_test_periods(),
            q4_period_id="q4",
            event_cutoff="2026-03-04",
        )


@pytest.mark.parametrize(
    "metric_id",
    (
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
    ),
)
def test_q4_subtraction_forbids_rates_eps_and_weighted_averages(metric_id) -> None:
    with pytest.raises(PromiseProgressProductV2Error, match="forbidden"):
        derive_q4_additive_from_fy_ytd(
            _q4_test_fact("fy-fact", "10", "fy", metric_id=metric_id),
            _q4_test_fact("ytd-fact", "7", "ytd", metric_id=metric_id),
            periods=_q4_test_periods(),
            q4_period_id="q4",
            event_cutoff="2026-03-04",
        )


def test_q4_margin_and_growth_derive_only_from_compatible_components() -> None:
    assert derive_q4_margin_from_components(
        {"kind": "exact", "value": "14"},
        {"kind": "exact", "value": "100"},
    ) == {"kind": "exact", "value": "14"}
    assert derive_q4_growth_from_amounts(
        {"kind": "exact", "value": "105"},
        {"kind": "exact", "value": "100"},
    ) == {"kind": "exact", "value": "5"}
    with pytest.raises(PromiseProgressProductV2Error):
        derive_q4_margin_from_components(
            {"kind": "range", "low": "13", "high": "15"},
            {"kind": "exact", "value": "100"},
        )


def test_successor_q4_audit_is_closed_and_lineaged(successor_candidate) -> None:
    report = build_q4_derivation_audit(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["projected_q4_actual_count"] == 39
    assert report["projected_classification_counts"] == {
        "direct": 23,
        "derived_exact": 10,
        "derived_components": 6,
        "derived_bounded": 0,
    }
    assert report["bounded_projected_count"] == 0
    derived = next(
        row for row in report["rows"] if row["derivation_rule_id"] is not None
    )
    assert derived["derivation_input_record_ids"]
    assert derived["derivation_support_record_ids"]
    assert report["forbidden_ratio_subtraction_count"] == 0
    assert report["forbidden_eps_subtraction_count"] == 0
    assert report["forbidden_weighted_average_subtraction_count"] == 0


def test_successor_projects_all_four_direct_q4_operating_income_facts(
    successor_candidate,
) -> None:
    audit = _strict_json(FINAL_EXHAUSTIVE_AUDIT_ROOT / "quarter_actual_reconciliation.json")
    expected = {
        str(row["period_id"]): row
        for row in audit["records"]
        if row["audit_result"] == "DEFECT"
        and row["metric_id"] == "metric:anf:operating-income@1"
    }
    assert set(expected) == {
        "period:anf:fy2022-q4@1",
        "period:anf:fy2023-q4@1",
        "period:anf:fy2024-q4@1",
        "period:anf:fy2025-q4@1",
    }
    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    rows = {
        str(row.horizon_period_id): row
        for row in timeline
        if row.row_kind == PERIOD_RESULT_ROW_KIND
        and row.metric_id == "metric:anf:operating-income@1"
        and "-q4@" in str(row.horizon_period_id)
    }
    assert set(rows) == set(expected)
    trace = build_workbook_trace_v2(
        successor_candidate.product,
        successor_candidate.plan,
        preview_workbook=successor_candidate.first,
    )
    trace_ids = {str(row["binding_id"]) for row in trace["records"]}
    for period_id, audited in expected.items():
        row = rows[period_id]
        assert row.actual_value == audited["canonical_value"]
        assert row.actual_display == audited["display"]
        assert row.actual_knowledge_date == audited["knowledge_date"]
        assert row.event_date == audited["knowledge_date"]
        assert row.actual_source_document_ids == tuple(audited["source_document_ids"])
        assert row.actual_candidate_record_ids == tuple(audited["candidate_record_ids"])
        assert row.actual_derivation_rule_id is None
        assert row.actual_derivation_input_record_ids == ()
        assert row.previous_display == ""
        assert row.current_display == ""
        assert row.change_type is None
        assert row.status_at_update is None
        actual_bindings = [
            binding
            for binding in successor_candidate.plan.bindings
            if binding.source_row_id == row.row_id and binding.field_role == "actual"
        ]
        assert len(actual_bindings) == 1
        assert actual_bindings[0].binding_id in trace_ids
        assert actual_bindings[0].actual_candidate_record_ids == row.actual_candidate_record_ids


def test_operating_income_uses_the_closed_product_to_canonical_metric_relation() -> None:
    assert compatible_foundation_metric_ids("metric:anf:operating-income@1") == (
        "metric:anf:operating-income@1",
        "metric:core:operating-income@1",
    )
    assert compatible_foundation_metric_ids("metric:core:operating-margin@1") == (
        "metric:core:operating-margin@1",
    )


def test_successor_projects_all_sixty_quarter_guidance_versions(
    successor_candidate,
) -> None:
    report = build_quarter_guidance_coverage_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["canonical_quarter_guidance_count"] == 60
    assert report["product_considered_quarter_guidance_count"] == 60
    assert report["product_projected_quarter_guidance_count"] == 60
    assert report["open_quarter_guidance_count"] == 6
    assert report["annual_quarter_version_overlap_count"] == 0
    assert report["annual_progression_remains_annual_only"] is True
    assert report["false_may_capex_comparator_version_count"] == 0
    assert all(row["horizon_type"] == "quarter" for row in report["rows"])


def test_successor_tariff_guidance_retains_basis_point_unit(
    successor_candidate,
) -> None:
    open_rows = _blocks(successor_candidate)[OPEN_BLOCK_ID].rows
    tariff = next(
        row for row in open_rows if row.metric_id == "metric:anf:tariff-impact@1"
    )
    assert tariff.horizon_period_id == "period:anf:fy2026-q2@1"
    assert tariff.current_value == {
        "impact_polarity": "unfavorable",
        "kind": "approximate",
        "qualifier": "around",
        "tolerance": None,
        "unit": "basis points",
        "value": "120",
    }
    assert tariff.unit_id == "unit:core:basis-points@1"
    assert tariff.current_display == "~120 bps unfavorable"
    assert display_value(
        {"kind": "approximate", "value": "120"},
        unit_id="unit:core:basis-points@1",
    ) == "~120 bps"


def test_successor_timeline_roles_separate_guidance_results_and_outcomes(
    successor_candidate,
) -> None:
    report = build_result_event_semantic_report(successor_candidate.product)
    assert report["row_kind_counts"] == {
        GUIDANCE_UPDATE_ROW_KIND: 189,
        PERIOD_RESULT_ROW_KIND: 149,
        HORIZON_OUTCOME_ROW_KIND: 76,
    }
    assert report["period_result_fabricated_guidance_field_count"] == 0
    assert report["horizon_outcome_fabricated_guidance_field_count"] == 0
    assert report["outcome_reported_change_type_count"] == 0
    assert report["status_without_outcome_actual_lineage_count"] == 0
    assert report["period_actual_paired_with_different_horizon_status_count"] == 0


def test_successor_horizon_status_uses_the_visible_horizon_actual(
    successor_candidate,
) -> None:
    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    outcomes = [row for row in timeline if row.row_kind == HORIZON_OUTCOME_ROW_KIND]
    assert len(outcomes) == 76
    assert all(row.previous_display == "" for row in outcomes)
    assert all(row.current_display == "" for row in outcomes)
    assert all(row.change_type is None for row in outcomes)
    assert all(row.actual_value is not None for row in outcomes)
    assert all(row.actual_period_id == row.horizon_period_id for row in outcomes)
    assert all(
        row.status_actual_candidate_record_ids == row.actual_candidate_record_ids
        for row in outcomes
    )
    assert all(
        row.status_actual_source_document_ids == row.actual_source_document_ids
        for row in outcomes
    )


def test_successor_q1_same_occurrence_prefers_actual_and_q2_q3_remain_distinct(
    successor_candidate,
) -> None:
    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    metrics = {
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:share-repurchases@1",
    }
    rows = [
        row
        for row in timeline
        if row.row_kind == PERIOD_RESULT_ROW_KIND
        and row.metric_id in metrics
        and row.actual_period_id is not None
        and "fy2025-q" in row.actual_period_id
    ]
    by_quarter = {
        quarter: [row for row in rows if f"fy2025-q{quarter}@" in row.actual_period_id]
        for quarter in (1, 2, 3)
    }
    assert all(len(by_quarter[quarter]) == 2 for quarter in (1, 2, 3))
    assert all(row.actual_value is not None for row in rows)
    assert all(row.progress_value is None for row in by_quarter[1])
    assert all(row.progress_value is not None for quarter in (2, 3) for row in by_quarter[quarter])


def test_successor_bounded_q4_opportunities_do_not_become_false_exact_actuals(
    successor_candidate,
) -> None:
    report = build_bounded_derivation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["bounded_opportunity_count"] == 4
    assert report["bounded_projected_actual_count"] == 0
    assert report["arbitrary_percentage_tolerance_used"] is False
    assert all(not row["selected_rule_present"] for row in report["rows"])


def test_successor_foundation_projection_has_no_unexplained_evidence(
    successor_candidate,
) -> None:
    report = build_foundation_projection_disposition_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["unexplained_promise_evidence_count"] == 0
    assert report["disposition_counts"] == {
        "projected": 335,
        "other_product": 175,
        "corroborating_only": 1,
        "not_promise_eligible": 14,
        "deferred_missing_tuple": 15,
        "temporally_ineligible": 0,
        "definition_incompatible": 0,
    }


def test_successor_reclassifies_the_exact_nineteen_active_foundation_facts(
    successor_candidate,
) -> None:
    audit = _strict_json(
        FINAL_EXHAUSTIVE_AUDIT_ROOT / "foundation_disposition_reconciliation.json"
    )
    audited_ids = {
        str(row["evidence_id"])
        for row in audit["records"]
        if row["audit_result"] == "DEFECT"
    }
    assert len(audited_ids) == 19
    report = build_foundation_projection_disposition_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    by_id = {str(row["evidence_id"]): row for row in report["rows"]}
    assert all(by_id[evidence_id]["disposition"] == "projected" for evidence_id in audited_ids)
    assert all(
        by_id[evidence_id]["reason"]
        == "selected directly or retained as typed derivation input"
        for evidence_id in audited_ids
    )
    q4_operating_income_ids = {
        str(row["candidate_record_ids"][0])
        for row in _strict_json(
            FINAL_EXHAUSTIVE_AUDIT_ROOT / "quarter_actual_reconciliation.json"
        )["records"]
        if row.get("metric_id") == "metric:anf:operating-income@1"
        and row.get("audit_result") == "DEFECT"
    }
    assert len(q4_operating_income_ids) == 4
    assert all(
        by_id[evidence_id]["disposition"] == "projected"
        for evidence_id in q4_operating_income_ids
    )
    assert any(row["disposition"] == "other_product" for row in report["rows"])


def test_progression_q4_slot_never_receives_q4_actual(successor_candidate) -> None:
    report = build_progression_q4_update_audit(successor_candidate.product)
    assert report["row_count"] == 28
    assert report["populated_q4_guidance_update_count"] == 10
    assert report["intentional_blank_q4_guidance_update_count"] == 18
    assert report["q4_actual_as_guidance_count"] == 0


def test_successor_actual_progress_roles_and_blank_audit(successor_candidate) -> None:
    roles = build_timeline_actual_progress_role_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert roles["role_counts"] == {
        "event_period_actual": 148,
        "horizon_actual": 76,
        "ytd_progress": 68,
        "cumulative_progress": 0,
        "annualized_run_rate": 0,
        "delta_progress": 0,
        "unavailable": 189,
    }
    assert roles["timeline_row_count"] == 414
    assert roles["rows_with_actual_and_progress_count"] == 67
    assert roles["same_fact_dual_role_count"] == 0
    assert roles["same_occurrence_dual_visible_role_count"] == 0
    assert roles["future_actual_leakage_count"] == 0
    assert roles["future_progress_leakage_count"] == 0
    assert roles["status_replay"]["status_without_outcome_actual_lineage_count"] == 0
    blanks = build_timeline_blank_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert blanks["timeline_row_count"] == 414
    assert blanks["blank_field_count"] == 1484
    assert blanks["correctable_blank_count"] == 0
    assert blanks["every_blank_has_evidence_search_trace"] is True
    assert blanks["reason_counts"]["extraction_missing"] == 0
    assert blanks["reason_counts"]["semantic_mapping_missing"] == 0
    assert blanks["reason_counts"]["unexplained_review_required"] == 0


def test_successor_fy2022_q3_operating_income_has_distinct_actual_and_progress(
    successor_candidate,
) -> None:
    row = next(
        row
        for row in _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
        if row.row_kind == PERIOD_RESULT_ROW_KIND
        and row.metric_id == "metric:anf:operating-income@1"
        and row.horizon_period_id == "period:anf:fy2022-q3@1"
    )
    assert row.actual_value == {"kind": "exact", "value": "17.543"}
    assert row.actual_display == "$17.543m"
    assert row.actual_period_id == "period:anf:fy2022-q3@1"
    assert row.progress_value == {"kind": "exact", "value": "5.626"}
    assert row.progress_display == "YTD: $5.626m"
    assert row.progress_period_id == "period:anf:fy2022-ytd-q3@1"
    assert set(row.actual_candidate_record_ids).isdisjoint(row.progress_candidate_record_ids)
    assert row.actual_knowledge_date == row.progress_knowledge_date == "2022-11-23"
    blanks = build_timeline_blank_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    traces = [
        trace
        for trace in blanks["resolved_field_evidence_search_traces"]
        if trace["row_id"] == row.row_id
        and trace["field_role"] == "progress_run_rate"
    ]
    assert len(traces) == 1
    assert traces[0]["selected_candidate_evidence_ids"] == list(
        row.progress_candidate_record_ids
    )
    assert set(row.progress_candidate_record_ids).issubset(
        traces[0]["candidate_evidence_ids_considered"]
    )
    assert not any(
        blank["row_id"] == row.row_id and blank["field_role"] == "progress_run_rate"
        for blank in blanks["rows"]
    )
    role_report = build_timeline_actual_progress_role_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert role_report["same_occurrence_dual_visible_role_count"] == 0


def test_successor_needs_review_is_audited_without_arbitrary_assumptions(
    successor_candidate,
) -> None:
    report = build_needs_review_semantics_review(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["prior_golden_visible_needs_review_count"] == 9
    assert report["successor_visible_needs_review_count"] == 50
    assert report["successor_unique_issue_count"] == 35
    assert report["additional_timeline_outcome_context_count"] == 15
    assert report["unresolved_correctable_count"] == 0
    assert report["approximate_target_rule"]["arbitrary_tolerance_permitted"] is False
    assert report["approximate_target_rule"]["favorable_direction_may_be_inferred"] is False
    assert all(not row["arbitrary_tolerance_used"] for row in report["approximate_cases"])
    assert all(not row["favorable_direction_inferred"] for row in report["approximate_cases"])


def test_successor_numeric_storage_is_scoped_and_intentional(successor_candidate) -> None:
    report = build_numeric_cell_text_audit(
        successor_candidate.plan, successor_candidate.semantic
    )
    assert report["numeric_cell_count"] > 177
    assert report["numeric_format_mismatch_count"] == 0
    assert report["other_presentation_defect_count"] == 0
    assert report["global_ignored_error_suppression"] is False
    assert any(
        row["presentation_text"] == "5%" and row["number_format_code"] == "0%"
        for row in report["numeric_cells"]
    )
    assert any(
        row["presentation_text"] == "$450m"
        and row["number_format_code"] == '"$"0"m"'
        for row in report["numeric_cells"]
    )
    precise_percent_cells = [
        row
        for row in report["numeric_cells"]
        if row["destination"] in {"E78", "E79"}
    ]
    assert len(precise_percent_cells) == 2
    assert all(row["stored_numeric_value"] == "0.08" for row in precise_percent_cells)
    assert all(row["number_format_code"] == "0.0%" for row in precise_percent_cells)
    assert all(
        row["independently_replayed_display"] == "8.0%"
        for row in precise_percent_cells
    )
    assert any(
        row["presentation_text"] == "$200m–$225m" and row["classification"] == "B"
        for row in report["rows"]
    )
    assert all(row["classification"] in {"B", "C", "D"} for row in report["rows"])
    with ZipFile(successor_candidate.first) as archive:
        worksheet_xml = b"".join(
            archive.read(name)
            for name in archive.namelist()
            if name.startswith("xl/worksheets/") and name.endswith(".xml")
        )
    assert b"ignoredErrors" not in worksheet_xml


def test_successor_numeric_precision_replay_and_mutation_detection(
    successor_candidate,
) -> None:
    assert replay_ooxml_numeric_display("0.08", "0.0%") == "8.0%"
    assert replay_ooxml_numeric_display("0.08", "0%") == "8%"
    assert replay_ooxml_numeric_display("450", '"$"0"m"') == "$450m"
    assert replay_ooxml_numeric_display("10.46", '"$"0.00') == "$10.46"

    mutated = successor_candidate.root / "mutated-number-format.xlsx"
    with ZipFile(successor_candidate.first, "r") as source, ZipFile(mutated, "w") as target:
        for info in source.infolist():
            payload = source.read(info.filename)
            if info.filename == "xl/styles.xml":
                root = _parse_xml(payload)
                matches = [
                    node
                    for node in root.iter()
                    if node.tag.endswith("numFmt") and node.get("formatCode") == "0.0%"
                ]
                assert len(matches) == 1
                matches[0].set("formatCode", "0%")
                payload = _serialize_xml(root)
            target.writestr(info, payload)
    validation = validate_preview_semantics_v2(
        successor_candidate.product,
        successor_candidate.plan,
        preview_workbook=mutated,
    )
    assert validation["passed"] is False
    mismatches = [
        row
        for row in validation["results"]
        if row["expected_display_value"] == "8.0%" and not row["pass"]
    ]
    assert len(mismatches) >= 2
    assert all(row["actual_number_format_code"] == "0%" for row in mismatches)
    assert all(row["independently_replayed_display"] == "8%" for row in mismatches)


def test_successor_event_header_role_is_dark_generic_and_spans_a_to_j(
    successor_candidate,
) -> None:
    contract = successor_candidate.plan.presentation_contract.to_dict()
    assert contract["contract_id"] == SUCCESSOR_PRODUCT_V2_PRESENTATION_CONTRACT_ID
    assert contract["timeline_event_header_role"] == {
        "style_role": "TimelineEventHeader",
        "legacy_style_source": "Promise_Progress_UI!A59",
        "fill_rgb": "5B9BD5",
        "font_rgb": "FFFFFF",
        "font_bold": True,
        "span": "A:J",
        "economics_authority": "none",
    }
    event_header_anchors = {
        f"A{row.row_number}"
        for row in successor_candidate.plan.row_plan
        if row.row_kind == "event_group"
    }
    event_header_bindings = [
        binding
        for binding in successor_candidate.plan.bindings
        if binding.anchor_cell in event_header_anchors
        and binding.binding_kind == "event_group"
    ]
    assert len(event_header_bindings) == len(event_header_anchors)
    assert all(
        binding.style_role == "TimelineEventHeader"
        for binding in event_header_bindings
    )


def test_successor_workbook_is_deterministic_and_visually_valid(successor_candidate) -> None:
    assert successor_candidate.first.read_bytes() == successor_candidate.second.read_bytes()
    for key in (
        "preview_workbook_sha256",
        "canonical_workbook_content_sha256",
        "target_sheet_semantic_sha256",
        "binding_plan_sha256",
        "presentation_contract_sha256",
    ):
        assert successor_candidate.first_result[key] == successor_candidate.second_result[key]
    assert successor_candidate.structural["passed"] is True
    assert successor_candidate.semantic["passed"] is True
    assert successor_candidate.visual["passed"] is True
    assert successor_candidate.visual["clipped_visible_field_count"] == 0
    assert successor_candidate.visual["overflow_dependency_count"] == 0
    assert all(
        height == 24
        for row_number, height in successor_candidate.plan.row_heights
        if next(
            row for row in successor_candidate.plan.row_plan if row.row_number == row_number
        ).row_kind
        == "product_row"
    )


def test_visual_validation_markdown_is_output_root_independent(tmp_path: Path) -> None:
    visual = {
        "record_count": 1,
        "clipped_visible_field_count": 0,
        "overflow_dependency_count": 0,
        "passed": True,
    }
    plan = SimpleNamespace(used_range="A1:O1", row_plan=(object(),))
    payloads = []
    for name in ("candidate-a", "candidate-b"):
        root = tmp_path / name
        root.mkdir()
        report = root / "visual_validation_v2.md"
        preview = root / "ANF_Promise_Progress_source_native_v2_preview.xlsx"
        _write_visual_markdown(
            report,
            product_sha256="0" * 64,
            preview_path=preview,
            visual=visual,
            plan=plan,
        )
        payloads.append(report.read_bytes())
    assert payloads[0] == payloads[1]
    assert b"ANF_Promise_Progress_source_native_v2_preview.xlsx" in payloads[0]
    assert str(tmp_path).encode() not in payloads[0]


def test_successor_manifest_inventory_includes_final_closure_reports() -> None:
    assert FINAL_CLOSURE_MANIFEST_FILENAMES == (
        "old_defect_regression_report.json",
        "current_defect_closure_report.json",
        "current_count_reconciliation_report.json",
        "numeric_ooxml_reconciliation.json",
    )


def test_successor_version_and_old_golden_tag_are_separate(successor_candidate) -> None:
    assert successor_candidate.source_set["source_set_id"] == EVIDENCE_FOUNDATION_SOURCE_SET_ID
    assert successor_candidate.product.product_version == SUCCESSOR_PRODUCT_VERSION
    assert SUCCESSOR_PRODUCT_VERSION != PRODUCT_VERSION
    peeled = subprocess.check_output(
        ["git", "rev-list", "-n", "1", "promise-progress-product-v2-workbook-golden"],
        cwd=REPO,
        text=True,
    ).strip()
    assert peeled == "05f549cd6de288366642a41e1ba81c4b33696fc5"


def test_product_v2_1_golden_publication_fixtures_are_exact() -> None:
    expected_fixture_hashes = {
        V2_1_SOURCE_SET_GOLDEN: EXPECTED_SUCCESSOR_SOURCE_SET_SHA256,
        V2_1_FOUNDATION_IDENTITY_GOLDEN: (
            EXPECTED_SUCCESSOR_FOUNDATION_IDENTITY_SHA256
        ),
        V2_1_PRODUCT_GOLDEN: EXPECTED_SUCCESSOR_PRODUCT_SHA256,
        V2_1_SHADOW_GOLDEN: EXPECTED_SUCCESSOR_SHADOW_SHA256,
        V2_1_COUNT_REPORT_GOLDEN: EXPECTED_SUCCESSOR_COUNT_REPORT_SHA256,
        V2_1_MANIFEST_GOLDEN: EXPECTED_V2_1_GOLDEN_MANIFEST_SHA256,
    }
    assert {
        path: sha256_file(path) for path in expected_fixture_hashes
    } == expected_fixture_hashes

    manifest = _strict_json(V2_1_MANIFEST_GOLDEN)
    assert manifest["manifest_type"] == "PromiseProgressProductV2GoldenManifest@2"
    assert manifest["golden_id"] == "promise-progress-product:anf@2.1.0"
    assert manifest["product_schema_type"] == PRODUCT_TYPE
    assert manifest["product_version"] == "2.1.0"
    assert manifest["product_artifact_version"] == SUCCESSOR_PRODUCT_VERSION
    assert manifest["lifecycle"] == "target_not_wired"
    assert manifest["accepted_candidate"] == {
        "manifest_digest": (
            "d8b758889781332d9aa3a69b1e69f37344409d27ed7dfbe64223a79bc97be10f"
        ),
        "manifest_file_sha256": (
            "9cb26d0608909ccd3dfb31b7f48b03ec4df6480d6addc3b53ecbb8dcee39fc97"
        ),
        "root_name": (
            "promise_progress_product_v2_1_final_required_kind_schema_"
            "invariant_correction_candidate"
        ),
    }
    fixture_rows = manifest["fixture_artifacts"]
    assert len(fixture_rows) == 5
    assert {
        (V2_1_MANIFEST_GOLDEN.parent / row["relative_path"]).resolve(): row["sha256"]
        for row in fixture_rows
    } == {
        path.resolve(): expected_fixture_hashes[path]
        for path in expected_fixture_hashes
        if path != V2_1_MANIFEST_GOLDEN
    }

    source_set = _strict_json(V2_1_SOURCE_SET_GOLDEN)
    foundation_identity = _strict_json(V2_1_FOUNDATION_IDENTITY_GOLDEN)
    product = _strict_json(V2_1_PRODUCT_GOLDEN)
    count_report = _strict_json(V2_1_COUNT_REPORT_GOLDEN)
    assert source_set["source_set_id"] == EVIDENCE_FOUNDATION_SOURCE_SET_ID
    assert foundation_identity["foundation_sha256"] == (
        EXPECTED_SUCCESSOR_FOUNDATION_SHA256
    )
    assert product["product_type"] == PRODUCT_TYPE
    assert product["product_version"] == SUCCESSOR_PRODUCT_VERSION
    assert count_report["kind_schema_id"] == COUNT_RECONCILIATION_KIND_SCHEMA_ID
    assert count_report["report_type"] == "PromiseProgressFinalCountReconciliation@3"
    assert count_report["passed"] is True
    assert validate_current_count_reconciliation_report(count_report) is True

    # Product@2.0 remains a separate immutable predecessor checkpoint.
    assert sha256_file(V2_SOURCE_SET_GOLDEN) == EXPECTED_V2_SOURCE_SET_SHA256
    assert sha256_file(V2_PRODUCT_GOLDEN) == EXPECTED_V2_PRODUCT_SHA256
    assert sha256_file(V2_SHADOW_GOLDEN) == EXPECTED_V2_SHADOW_SHA256
    assert sha256_file(V2_MANIFEST_GOLDEN) == EXPECTED_V2_MANIFEST_FILE_SHA256


def test_product_v2_1_golden_regeneration_matches_reviewed_snapshot(
    successor_candidate,
    final_count_reconciliation_bundle,
) -> None:
    assert serialize_package(successor_candidate.source_set) == (
        V2_1_SOURCE_SET_GOLDEN.read_bytes()
    )
    assert serialize_promise_progress_product_v2(successor_candidate.product) == (
        V2_1_PRODUCT_GOLDEN.read_bytes()
    )
    rebuilt_shadow = build_product_v2_shadow(
        successor_candidate.product,
        successor_candidate.package,
        evidence_foundation=successor_candidate.evidence_foundation,
    )
    assert serialize_product_v2_shadow(rebuilt_shadow) == V2_1_SHADOW_GOLDEN.read_bytes()
    assert _json_bytes(final_count_reconciliation_bundle.report) == (
        V2_1_COUNT_REPORT_GOLDEN.read_bytes()
    )

    foundation_artifacts = evidence_foundation_artifacts(
        successor_candidate.evidence_foundation
    )
    foundation_identity = _strict_json(V2_1_FOUNDATION_IDENTITY_GOLDEN)
    assert hashlib.sha256(
        serialize_package(foundation_artifacts["evidence_foundation_candidate.json"])
    ).hexdigest() == foundation_identity["foundation_sha256"]
    assert hashlib.sha256(
        serialize_package(foundation_artifacts["canonical_fact_inventory.json"])
    ).hexdigest() == foundation_identity["fact_inventory_sha256"]
    assert hashlib.sha256(
        serialize_package(
            foundation_artifacts["canonical_quarter_guidance_inventory.json"]
        )
    ).hexdigest() == foundation_identity["quarter_guidance_inventory_sha256"]

    manifest = _strict_json(V2_1_MANIFEST_GOLDEN)
    workbook_contract = manifest["workbook_preview"]
    assert sha256_file(successor_candidate.first) == (
        workbook_contract["raw_workbook_sha256"]
    )
    assert successor_candidate.first.read_bytes() == successor_candidate.second.read_bytes()
    assert canonical_workbook_content_sha256(successor_candidate.first) == (
        workbook_contract["canonical_ooxml_sha256"]
    )
    assert target_sheet_semantic_sha256_v2(
        successor_candidate.first,
        successor_candidate.plan,
    ) == workbook_contract["target_semantic_sha256"]
    trace = build_workbook_trace_v2(
        successor_candidate.product,
        successor_candidate.plan,
        preview_workbook=successor_candidate.first,
    )
    assert hashlib.sha256(_json_bytes(trace)).hexdigest() == (
        workbook_contract["trace_sha256"]
    )


def test_product_v2_1_golden_semantic_snapshot_and_regressions(
    successor_candidate,
    final_count_reconciliation_bundle,
) -> None:
    manifest = _strict_json(V2_1_MANIFEST_GOLDEN)
    count_report = final_count_reconciliation_bundle.report
    generated_counts = {
        row["kind"]: row["generated_actual"] for row in count_report["rows"]
    }
    assert {
        kind: generated_counts[kind] for kind in manifest["semantic_snapshot"]
    } == manifest["semantic_snapshot"]
    assert count_report["economic_result_counts"] == (
        manifest["count_reconciliation"]["classification"]
    )
    assert count_report["headline_total"] == count_report["kind_row_sum"] == 8309
    assert sum(count_report["economic_result_counts"].values()) == 8309

    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    fy2022_q4_revenue_guide = next(
        row
        for row in timeline
        if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
        and row.metric_id == "metric:core:revenue-growth@1"
        and row.horizon_period_id == "period:anf:fy2022-q4@1"
    )
    fy2022_q4_revenue_outcome = next(
        row
        for row in timeline
        if row.row_kind == HORIZON_OUTCOME_ROW_KIND
        and row.metric_id == "metric:core:revenue-growth@1"
        and row.horizon_period_id == "period:anf:fy2022-q4@1"
    )
    assert fy2022_q4_revenue_guide.current_display == "Down 2%\u20134%"
    assert fy2022_q4_revenue_outcome.actual_value == {"kind": "exact", "value": "3"}
    assert fy2022_q4_revenue_outcome.status_at_update == "Beat"

    tariff = next(
        row
        for row in timeline
        if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
        and row.metric_id == "metric:anf:tariff-impact@1"
        and row.horizon_period_id == "period:anf:fy2026-q2@1"
    )
    assert tariff.current_display == "~120 bps unfavorable"
    guidance = build_guidance_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert guidance["false_may_capex_comparator_version_count"] == 0

    q4 = build_q4_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert q4["classification_counts"] == {
        "derived_bounded": 0,
        "derived_components": 6,
        "derived_exact": 10,
        "direct": 23,
        "unavailable": 9,
    }
    assert q4["forbidden_eps_subtraction_count"] == 0
    assert q4["forbidden_ratio_subtraction_count"] == 0
    assert q4["forbidden_weighted_average_subtraction_count"] == 0

    derivation = build_derivation_lineage_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    disposition = build_foundation_projection_disposition_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    blanks = final_count_reconciliation_bundle.blanks
    needs_review = build_needs_review_semantics_review(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert derivation["broken_lineage_count"] == 0
    assert derivation["non_dereferenceable_derivation_input_count"] == 0
    assert derivation["status_without_outcome_actual_lineage_count"] == 0
    assert disposition["unexplained_promise_evidence_count"] == 0
    assert blanks["correctable_blank_count"] == 0
    assert needs_review["correctable_needs_review_count"] == 0


def test_exhaustive_closure_projects_may_2026_annual_outlook(successor_candidate) -> None:
    report = build_guidance_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["passed"] is True
    assert report["annual_guidance_series_count"] == 38
    assert report["annual_guidance_version_count"] == 129
    assert report["may_2026_annual_version_count"] == 10
    assert report["may_2026_current_annual_open_count"] == 10
    assert report["false_may_capex_comparator_version_count"] == 0
    open_rows = _blocks(successor_candidate)[OPEN_BLOCK_ID].rows
    may_annual = [
        row
        for row in open_rows
        if row.horizon_period_id == "period:anf:fy2026@1"
    ]
    assert len(may_annual) == 10
    assert all(row.source_summary == "May 27 2026 release" for row in may_annual)
    capex_update = next(
        row
        for row in _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
        if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
        and row.event_date == "2026-05-27"
        and row.metric_id == "metric:core:capital-expenditures@1"
        and row.horizon_period_id == "period:anf:fy2026@1"
    )
    assert capex_update.previous_display == "$200m–$225m"
    assert capex_update.current_display == "~$225m"
    assert capex_update.change_type == "Range → approximate"


def test_exhaustive_closure_projects_historical_store_guidance(successor_candidate) -> None:
    report = build_guidance_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["historical_store_annual_version_count"] == 24
    assert report["guidance_progression_row_count"] == 28
    assert report["guidance_update_row_count"] == 189
    assert report["predecessor_transition_count"] == 96
    progression = _blocks(successor_candidate)[PROGRESSION_BLOCK_ID].rows
    historical_store = [
        row
        for row in progression
        if row.metric_id
        in {
            "metric:retail:net-store-openings@1",
            "metric:retail:store-openings@1",
            "metric:retail:store-closures-count@1",
            "metric:retail:store-remodels-right-sizes@1",
        }
        and row.horizon_label in {"FY2022", "FY2023", "FY2024"}
    ]
    assert len(historical_store) == 9


def test_exhaustive_closure_preserves_down_and_unfavorable_semantics(
    successor_candidate,
) -> None:
    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    q4_guide = next(
        row
        for row in timeline
        if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
        and row.metric_id == "metric:core:revenue-growth@1"
        and row.horizon_period_id == "period:anf:fy2022-q4@1"
    )
    q4_outcome = next(
        row
        for row in timeline
        if row.row_kind == HORIZON_OUTCOME_ROW_KIND
        and row.metric_id == "metric:core:revenue-growth@1"
        and row.horizon_period_id == "period:anf:fy2022-q4@1"
    )
    assert q4_guide.current_value["direction"] == "down"
    assert q4_guide.current_display == "Down 2%–4%"
    assert q4_outcome.actual_display == "3%"
    assert q4_outcome.status_at_update == "Beat"
    tariff = next(
        row
        for row in _blocks(successor_candidate)[OPEN_BLOCK_ID].rows
        if row.metric_id == "metric:anf:tariff-impact@1"
    )
    assert tariff.current_value["impact_polarity"] == "unfavorable"
    assert tariff.current_display == "~120 bps unfavorable"


def test_exhaustive_closure_actual_progress_and_status_counts(successor_candidate) -> None:
    actual = build_actual_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    progress = build_progress_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    status = build_status_report(successor_candidate.product)
    assert actual["passed"] is True
    assert actual["annual_actual_count"] == 28
    assert actual["quarter_actual_count"] == 148
    assert actual["period_result_row_count"] == 149
    assert actual["actual_unavailable_period_result_count"] == 1
    assert progress["passed"] is True
    assert progress["progress_value_count"] == 68
    assert progress["progress_only_period_result_count"] == 1
    assert progress["same_occurrence_dual_visible_role_count"] == 0
    assert status["passed"] is True
    assert status["status_context_count"] == 310
    assert status["status_counts"] == {
        "Open": 205,
        "Beat": 35,
        "Hit": 19,
        "Missed": 1,
        "Needs Review": 50,
    }


def test_exhaustive_closure_q4_and_derivation_lineage_are_replayable(
    successor_candidate,
) -> None:
    q4 = build_q4_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    lineage = build_derivation_lineage_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert q4["passed"] is True
    assert q4["record_count"] == 48
    assert q4["classification_counts"] == {
        "direct": 23,
        "derived_exact": 10,
        "derived_components": 6,
        "derived_bounded": 0,
        "unavailable": 9,
    }
    assert q4["forbidden_ratio_subtraction_count"] == 0
    assert q4["forbidden_eps_subtraction_count"] == 0
    assert q4["forbidden_weighted_average_subtraction_count"] == 0
    assert lineage["broken_lineage_count"] == 0
    assert lineage["non_dereferenceable_derivation_input_count"] == 0
    assert lineage["non_dereferenceable_derivation_support_count"] == 0
    assert lineage["foundation_period_input_placeholder_count"] == 0


def test_exhaustive_closure_blank_search_is_evidence_driven(successor_candidate) -> None:
    report = build_timeline_blank_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["blank_field_count"] == 1484
    assert report["correctable_blank_count"] == 0
    assert report["every_blank_has_evidence_search_trace"] is True
    assert set(report["reason_counts"]) == {
        "not_applicable",
        "no_prior_guidance",
        "not_disclosed_at_event",
        "source_evidence_unavailable",
        "derivation_not_valid",
        "incompatible_basis",
        "incompatible_period",
        "extraction_missing",
        "semantic_mapping_missing",
        "unexplained_review_required",
    }
    searched = [
        row
        for row in report["rows"]
        if row["candidate_evidence_ids_considered"]
        or row["candidate_derivation_rules_considered"]
    ]
    assert searched
    assert all(row["rejection_reasons"] for row in searched)


def test_exhaustive_closure_needs_review_blockers_match_evidence(
    successor_candidate,
) -> None:
    report = build_needs_review_semantics_review(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    assert report["successor_visible_needs_review_count"] == 50
    assert report["successor_unique_issue_count"] == 35
    assert report["correctable_needs_review_count"] == 0
    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    qualitative = [
        row
        for row in timeline
        if row.status_at_update == "Needs Review"
        and row.metric_id == "metric:core:revenue-growth@1"
        and row.actual_value is not None
        and row.investor_reason_code == "qualitative_target_non_comparable"
    ]
    assert len(qualitative) == 9
    assert all("Actual" not in row.investor_reason_display or "unavailable" not in row.investor_reason_display for row in qualitative)


def test_exhaustive_closure_uses_typed_sec_event_identity(successor_candidate) -> None:
    event = next(
        row
        for row in successor_candidate.product.disclosure_events
        if row.event_date == "2025-03-31"
    )
    assert event.display_label == "2024-Q4 SEC filing"
    affected = [
        row
        for row in _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
        if row.event_date == "2025-03-31"
    ]
    assert affected
    assert all(row.stated_in_display == "2024-Q4 SEC filing" for row in affected)
    assert all(row.event_date == "2025-03-31" for row in affected)


def test_exhaustive_defect_ids_all_close(successor_candidate) -> None:
    guidance = build_guidance_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    actual = build_actual_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    progress = build_progress_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    q4 = build_q4_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    derivation = build_derivation_lineage_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    status = build_status_report(successor_candidate.product)
    blanks = build_timeline_blank_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    needs_review = build_needs_review_semantics_review(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    disposition = build_foundation_projection_disposition_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    trace = build_workbook_trace_v2(
        successor_candidate.product,
        successor_candidate.plan,
        preview_workbook=successor_candidate.first,
    )
    closure = build_defect_closure_report(
        source_root=SOURCE_ROOT,
        product=successor_candidate.product,
        foundation=successor_candidate.evidence_foundation,
        plan=successor_candidate.plan,
        workbook_trace=trace,
        guidance_report=guidance,
        actual_report=actual,
        progress_report=progress,
        q4_report=q4,
        derivation_report=derivation,
        status_report=status,
        blank_report=blanks,
        needs_review_report=needs_review,
        disposition_report=disposition,
    )
    assert closure["source_defect_count"] == 1758
    assert closure["mapped_defect_count"] == 1758
    assert closure["unresolved_exhaustive_defect_count"] == 0
    assert closure["unresolved_exhaustive_defect_ids"] == []
    assert closure["all_workbook_bindings_have_trace"] is True
    assert closure["remaining_previous_defect_count"] == 0
    assert closure["ordinal_only_defect_closure_mapping_count"] == 0
    old_q4_ids = {
        "audit-element:q4_candidate:535024b10bd7082ad89547d8",
        "audit-element:q4_candidate:9906c28c3c34c85b56790b49",
        "audit-element:q4_candidate:f147d32746fe51dabb0b112f",
        "audit-element:q4_candidate:65ccb1c431ee533b4ec1c0d0",
    }
    mapped_q4 = {
        row["audit_element_id"]: row
        for row in closure["rows"]
        if row["audit_element_id"] in old_q4_ids
    }
    assert set(mapped_q4) == old_q4_ids
    assert all(
        row["mapping_method"] == "q4_metric_period_identity"
        for row in mapped_q4.values()
    )
    assert all(
        row["closure_reason"]
        == "stable Q4 metric-period identity resolves to the source-backed Product row"
        for row in mapped_q4.values()
    )
    assert all(
        "metric:anf:operating-income@1" in str(row["fixed_product_element_id"])
        and "-q4@" in str(row["fixed_product_element_id"])
        for row in mapped_q4.values()
    )


def test_final_current_77_defects_and_count_reconciliation_close(
    successor_candidate,
) -> None:
    guidance = build_guidance_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    actual = build_actual_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    progress = build_progress_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    q4 = build_q4_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    derivation = build_derivation_lineage_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    status = build_status_report(successor_candidate.product)
    blanks = build_timeline_blank_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    needs_review = build_needs_review_semantics_review(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    disposition = build_foundation_projection_disposition_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    trace = build_workbook_trace_v2(
        successor_candidate.product,
        successor_candidate.plan,
        preview_workbook=successor_candidate.first,
    )
    numeric = build_numeric_cell_text_audit(
        successor_candidate.plan, successor_candidate.semantic
    )
    closure = build_current_defect_closure_report(
        source_root=SOURCE_ROOT,
        product=successor_candidate.product,
        plan=successor_candidate.plan,
        workbook_trace=trace,
        q4_report=q4,
        progress_report=progress,
        blank_report=blanks,
        disposition_report=disposition,
        semantic_validation=successor_candidate.semantic,
        numeric_audit=numeric,
    )
    assert closure["source_defect_count"] == 77
    assert closure["mapped_defect_count"] == 77
    assert closure["still_defective_count"] == 0
    assert closure["still_defective_ids"] == []
    assert closure["ordinal_only_defect_closure_mapping_count"] == 0
    assert set(closure["closure_category_counts"]) == {
        "fixed",
        "duplicate_downstream_manifestation_of_fixed_root",
    }

    count_report = build_current_count_reconciliation_report(
        source_root=SOURCE_ROOT,
        product=successor_candidate.product,
        foundation=successor_candidate.evidence_foundation,
        plan=successor_candidate.plan,
        guidance_report=guidance,
        actual_report=actual,
        progress_report=progress,
        q4_report=q4,
        derivation_report=derivation,
        status_report=status,
        needs_review_report=needs_review,
        blank_report=blanks,
        disposition_report=disposition,
    )
    assert count_report["passed"] is True
    assert count_report["reconciled_layered_element_count"] == 8309
    assert sum(row["generated_actual"] for row in count_report["rows"]) == 8309
    assert count_report["unexplained_divergence_count"] == 0
    assert count_report["explained_divergence_count"] == 0
    blank_count = next(row for row in count_report["rows"] if row["kind"] == "blank_cell")
    assert blank_count["final_review_expected"] == 1484
    assert blank_count["generated_actual"] == 1484
    assert blank_count["difference"] == 0
    assert blank_count["explained_divergence"] is False


def _build_final_count_report_for_test(successor_candidate):
    guidance = build_guidance_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    actual = build_actual_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    progress = build_progress_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    q4 = build_q4_reconciliation_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    derivation = build_derivation_lineage_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    status = build_status_report(successor_candidate.product)
    blanks = build_timeline_blank_completeness_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    needs_review = build_needs_review_semantics_review(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    disposition = build_foundation_projection_disposition_report(
        successor_candidate.product, successor_candidate.evidence_foundation
    )
    report = build_current_count_reconciliation_report(
        source_root=SOURCE_ROOT,
        product=successor_candidate.product,
        foundation=successor_candidate.evidence_foundation,
        plan=successor_candidate.plan,
        guidance_report=guidance,
        actual_report=actual,
        progress_report=progress,
        q4_report=q4,
        derivation_report=derivation,
        status_report=status,
        needs_review_report=needs_review,
        blank_report=blanks,
        disposition_report=disposition,
    )
    return SimpleNamespace(report=report, blanks=blanks)


@pytest.fixture(scope="module")
def final_count_reconciliation_bundle(successor_candidate):
    return _build_final_count_report_for_test(successor_candidate)


def _refresh_count_report_derived_metadata(report: dict) -> None:
    """Refresh serialized diagnostics without changing authoritative requirements."""

    report.update(count_reconciliation_kind_schema_state(report["rows"]))
    report["kind_row_count"] = len(report["rows"])
    report["kind_row_sum"] = sum(
        row["generated_actual"] for row in report["rows"]
    )
    report["invariant_checks"] = current_count_reconciliation_invariant_checks(
        report
    )


def test_final_count_report_current_rows_own_the_8309_headline(
    final_count_reconciliation_bundle,
) -> None:
    report = final_count_reconciliation_bundle.report
    expected = {
        "metric": 59,
        "annual_guidance_series": 38,
        "quarter_guidance_series": 55,
        "annual_guidance_version": 129,
        "quarter_guidance_version": 60,
        "guidance_transition": 96,
        "annual_actual": 28,
        "quarter_actual": 148,
        "progress": 68,
        "q4_candidate": 48,
        "derived_fact": 82,
        "guidance_progression_row": 28,
        "open_guidance_row": 16,
        "guidance_update_row": 189,
        "period_result_row": 149,
        "horizon_outcome_row": 76,
        "assessment_row": 1,
        "disclosure_event": 34,
        "status": 310,
        "needs_review": 50,
        "change_type": 189,
        "blank_cell": 1484,
        "workbook_field_cell": 4429,
        "foundation_disposition": 540,
        "source_conflict": 3,
    }
    generated = {row["kind"]: row["generated_actual"] for row in report["rows"]}
    independently_summed = sum(row["generated_actual"] for row in report["rows"])

    assert report["report_type"] == "PromiseProgressFinalCountReconciliation@3"
    assert report["kind_schema_id"] == COUNT_RECONCILIATION_KIND_SCHEMA_ID
    assert tuple(report["required_kinds"]) == COUNT_RECONCILIATION_REQUIRED_KINDS
    assert tuple(report["serialized_kinds"]) == COUNT_RECONCILIATION_REQUIRED_KINDS
    assert report["required_kind_count"] == report["serialized_kind_count"] == 25
    assert report["missing_required_kinds"] == []
    assert report["unexpected_kinds"] == []
    assert report["duplicate_kinds"] == []
    assert report["required_kind_set_matches"] is True
    assert report["required_kind_order_matches"] is True
    assert generated == expected
    assert report["kind_row_count"] == len(expected) == 25
    assert independently_summed == 8309
    assert report["headline_total"] == independently_summed
    assert report["kind_row_sum"] == independently_summed
    assert report["reconciled_layered_element_count"] == independently_summed
    assert report["source_audit_closed_universe_count"] == independently_summed
    assert report["headline_count_source"] == "sum(rows[*].generated_actual)"
    assert all(row["difference"] == 0 and row["pass"] for row in report["rows"])
    assert validate_current_count_reconciliation_report(report) is True
    assert report["passed"] is True


def test_final_count_report_economic_results_reconcile_to_the_headline(
    final_count_reconciliation_bundle,
) -> None:
    report = final_count_reconciliation_bundle.report
    assert report["economic_result_counts"] == {
        "PASS": 8184,
        "LEGITIMATELY_UNAVAILABLE": 24,
        "NEEDS_REVIEW": 101,
        "DEFECT": 0,
    }
    assert sum(report["economic_result_counts"].values()) == 8309
    assert report["classification_total"] == 8309
    assert report["economic_result_count_total"] == 8309
    assert report["economic_defect_count"] == 0


def test_final_count_report_mutations_fail_closed(
    final_count_reconciliation_bundle,
) -> None:
    report = final_count_reconciliation_bundle.report

    old_mismatch = copy.deepcopy(report)
    old_mismatch["reconciled_layered_element_count"] = 8310
    old_mismatch["headline_total"] = 8310
    _refresh_count_report_derived_metadata(old_mismatch)
    old_mismatch["passed"] = True
    assert validate_current_count_reconciliation_report(old_mismatch) is False

    changed_kind = copy.deepcopy(report)
    changed_kind["rows"][0]["generated_actual"] += 1
    changed_kind["rows"][0]["difference"] += 1
    changed_kind["rows"][0]["pass"] = False
    _refresh_count_report_derived_metadata(changed_kind)
    assert validate_current_count_reconciliation_report(changed_kind) is False

    changed_headline = copy.deepcopy(report)
    changed_headline["reconciled_layered_element_count"] -= 1
    changed_headline["headline_total"] -= 1
    _refresh_count_report_derived_metadata(changed_headline)
    assert validate_current_count_reconciliation_report(changed_headline) is False

    changed_classification = copy.deepcopy(report)
    changed_classification["economic_result_counts"]["PASS"] -= 1
    changed_classification["classification_total"] -= 1
    changed_classification["economic_result_count_total"] -= 1
    _refresh_count_report_derived_metadata(changed_classification)
    assert validate_current_count_reconciliation_report(changed_classification) is False

    nonzero_defect = copy.deepcopy(report)
    nonzero_defect["economic_result_counts"]["PASS"] -= 1
    nonzero_defect["economic_result_counts"]["DEFECT"] = 1
    nonzero_defect["economic_defect_count"] = 1
    _refresh_count_report_derived_metadata(nonzero_defect)
    assert validate_current_count_reconciliation_report(nonzero_defect) is False


def test_final_count_report_rename_mutation_identifies_missing_and_unexpected_kinds(
    final_count_reconciliation_bundle,
) -> None:
    report = copy.deepcopy(final_count_reconciliation_bundle.report)
    source_conflict = next(
        row for row in report["rows"] if row["kind"] == "source_conflict"
    )
    source_conflict["kind"] = "unexpected_kind"
    _refresh_count_report_derived_metadata(report)

    assert report["missing_required_kinds"] == ["source_conflict"]
    assert report["unexpected_kinds"] == ["unexpected_kind"]
    assert report["duplicate_kinds"] == []
    assert report["required_kind_set_matches"] is False
    assert validate_current_count_reconciliation_report(report) is False


def test_final_count_report_compensated_omission_fails_schema_validation(
    final_count_reconciliation_bundle,
) -> None:
    report = copy.deepcopy(final_count_reconciliation_bundle.report)
    source_conflict = next(
        row for row in report["rows"] if row["kind"] == "source_conflict"
    )
    report["rows"].remove(source_conflict)
    metric = next(row for row in report["rows"] if row["kind"] == "metric")
    for field in (
        "generated_actual",
        "final_review_expected",
        "audit_candidate_claim",
    ):
        metric[field] += source_conflict[field]
    _refresh_count_report_derived_metadata(report)

    assert report["kind_row_sum"] == report["headline_total"] == 8309
    assert report["missing_required_kinds"] == ["source_conflict"]
    assert report["unexpected_kinds"] == []
    assert report["serialized_kind_count"] == 24
    assert validate_current_count_reconciliation_report(report) is False


@pytest.mark.parametrize("missing_kind", COUNT_RECONCILIATION_REQUIRED_KINDS)
def test_final_count_report_rejects_each_missing_required_kind(
    final_count_reconciliation_bundle,
    missing_kind: str,
) -> None:
    report = copy.deepcopy(final_count_reconciliation_bundle.report)
    report["rows"] = [
        row for row in report["rows"] if row["kind"] != missing_kind
    ]
    _refresh_count_report_derived_metadata(report)

    assert report["missing_required_kinds"] == [missing_kind]
    assert validate_current_count_reconciliation_report(report) is False


def test_final_count_report_rejects_extra_duplicate_and_reordered_kinds(
    final_count_reconciliation_bundle,
) -> None:
    valid = final_count_reconciliation_bundle.report

    extra = copy.deepcopy(valid)
    extra_row = copy.deepcopy(extra["rows"][0])
    extra_row.update(
        {
            "kind": "unexpected_kind",
            "audit_candidate_claim": 0,
            "final_review_expected": 0,
            "generated_actual": 0,
            "difference": 0,
            "pass": True,
        }
    )
    extra["rows"].append(extra_row)
    _refresh_count_report_derived_metadata(extra)
    assert extra["unexpected_kinds"] == ["unexpected_kind"]
    assert validate_current_count_reconciliation_report(extra) is False

    duplicate = copy.deepcopy(valid)
    duplicate_row = copy.deepcopy(duplicate["rows"][0])
    duplicate_row.update(
        {
            "audit_candidate_claim": 0,
            "final_review_expected": 0,
            "generated_actual": 0,
            "difference": 0,
            "pass": True,
        }
    )
    duplicate["rows"].append(duplicate_row)
    _refresh_count_report_derived_metadata(duplicate)
    assert duplicate["duplicate_kinds"] == ["metric"]
    assert validate_current_count_reconciliation_report(duplicate) is False

    reordered = copy.deepcopy(valid)
    reordered["rows"] = list(reversed(reordered["rows"]))
    _refresh_count_report_derived_metadata(reordered)
    assert reordered["required_kind_set_matches"] is True
    assert reordered["required_kind_order_matches"] is False
    assert validate_current_count_reconciliation_report(reordered) is False


def test_final_count_required_kind_schema_is_independent_of_serialized_rows(
    final_count_reconciliation_bundle,
) -> None:
    report = copy.deepcopy(final_count_reconciliation_bundle.report)
    report["rows"] = [
        row for row in report["rows"] if row["kind"] != "source_conflict"
    ]
    state = count_reconciliation_kind_schema_state(report["rows"])
    assert state["missing_required_kinds"] == ["source_conflict"]

    # Even self-declaring the truncated serialized set as required cannot weaken
    # the validator's external, versioned schema owner.
    report.update(state)
    report["required_kinds"] = list(report["serialized_kinds"])
    report["required_kind_count"] = len(report["serialized_kinds"])
    report["missing_required_kinds"] = []
    report["unexpected_kinds"] = []
    report["required_kind_set_matches"] = True
    report["required_kind_order_matches"] = True
    report["kind_row_count"] = len(report["rows"])
    report["kind_row_sum"] = sum(
        row["generated_actual"] for row in report["rows"]
    )
    report["invariant_checks"] = {
        key: True for key in report["invariant_checks"]
    }
    assert validate_current_count_reconciliation_report(report) is False


def test_final_count_report_has_no_prior_delta_headline_shortcut(
    final_count_reconciliation_bundle,
) -> None:
    report = final_count_reconciliation_bundle.report
    source = inspect.getsource(build_current_count_reconciliation_report)
    assert "new_intentional_blank_manifestation_count" not in source
    assert "corrected_expected" not in source
    assert "8309" not in source
    assert "8310" not in source
    assert "+ 20" not in source
    assert "- 1" not in source
    assert "COUNT_RECONCILIATION_REQUIRED_KINDS" in source
    assert report["headline_count_source"] == "sum(rows[*].generated_actual)"


def test_final_count_report_tracks_repaired_progress_and_four_new_rows(
    successor_candidate,
    final_count_reconciliation_bundle,
) -> None:
    timeline = _blocks(successor_candidate)[TIMELINE_BLOCK_ID].rows
    repaired = next(
        row
        for row in timeline
        if row.row_kind == PERIOD_RESULT_ROW_KIND
        and row.metric_id == "metric:anf:operating-income@1"
        and row.horizon_period_id == "period:anf:fy2022-q3@1"
    )
    assert repaired.progress_value == {"kind": "exact", "value": "5.626"}
    assert repaired.progress_display == "YTD: $5.626m"
    assert not any(
        row["row_id"] == repaired.row_id and row["field_role"] == "progress_run_rate"
        for row in final_count_reconciliation_bundle.blanks["rows"]
    )

    q4_rows = [
        row
        for row in timeline
        if row.row_kind == PERIOD_RESULT_ROW_KIND
        and row.metric_id == "metric:anf:operating-income@1"
        and "-q4@" in str(row.horizon_period_id)
    ]
    assert len(q4_rows) == 4
    expected_blank_roles = {
        "previous_guide",
        "new_current_guide",
        "change_type",
        "progress_run_rate",
        "status",
    }
    blank_rows = final_count_reconciliation_bundle.blanks["rows"]
    q4_row_ids = {row.row_id for row in q4_rows}
    for row in q4_rows:
        assert row.actual_value is not None
        assert row.previous_display == row.current_display == row.progress_display == ""
        assert row.change_type is None
        assert row.status_at_update is None
        assert {
            blank["field_role"] for blank in blank_rows if blank["row_id"] == row.row_id
        } == expected_blank_roles
    assert sum(blank["row_id"] in q4_row_ids for blank in blank_rows) == 20
    assert final_count_reconciliation_bundle.blanks["blank_field_count"] == 1484


def test_final_count_report_regeneration_is_deterministic(
    successor_candidate,
    final_count_reconciliation_bundle,
) -> None:
    rebuilt = _build_final_count_report_for_test(successor_candidate).report
    assert _json_bytes(rebuilt) == _json_bytes(final_count_reconciliation_bundle.report)


def test_final_count_report_correction_freezes_all_economic_hashes(
    successor_candidate,
) -> None:
    trace = build_workbook_trace_v2(
        successor_candidate.product,
        successor_candidate.plan,
        preview_workbook=successor_candidate.first,
    )
    shadow = build_product_v2_shadow(
        successor_candidate.product,
        successor_candidate.package,
        evidence_foundation=successor_candidate.evidence_foundation,
    )
    actual = {
        "source_set": hashlib.sha256(
            serialize_package(successor_candidate.source_set)
        ).hexdigest(),
        "product": promise_progress_product_v2_sha256(successor_candidate.product),
        "shadow": hashlib.sha256(serialize_product_v2_shadow(shadow)).hexdigest(),
        "workbook": sha256_file(successor_candidate.first),
        "canonical_ooxml": canonical_workbook_content_sha256(successor_candidate.first),
        "target_semantic": target_sheet_semantic_sha256_v2(
            successor_candidate.first, successor_candidate.plan
        ),
        "trace": hashlib.sha256(_json_bytes(trace)).hexdigest(),
    }
    assert actual == {
        "source_set": EXPECTED_SUCCESSOR_SOURCE_SET_SHA256,
        "product": EXPECTED_SUCCESSOR_PRODUCT_SHA256,
        "shadow": EXPECTED_SUCCESSOR_SHADOW_SHA256,
        "workbook": EXPECTED_SUCCESSOR_WORKBOOK_SHA256,
        "canonical_ooxml": EXPECTED_SUCCESSOR_CANONICAL_OOXML_SHA256,
        "target_semantic": EXPECTED_SUCCESSOR_TARGET_SEMANTIC_SHA256,
        "trace": EXPECTED_SUCCESSOR_TRACE_SHA256,
    }
