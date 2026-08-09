from __future__ import annotations

import dataclasses
import copy
import hashlib
import inspect
import json
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
    PROGRESSION_BLOCK_ID,
    TIMELINE_BLOCK_ID,
    VERSION_STATES,
    PromiseProgressProductV2Error,
    _event_indexes,
    build_product_v2_shadow,
    build_promise_progress_product_v2,
    classify_timeline_fact_role,
    classify_change,
    promise_progress_product_v2_sha256,
    serialize_product_v2_shadow,
    serialize_promise_progress_product_v2,
)
from pbi_xbrl.longitudinal_memory.promise_progress_projection import (
    build_promise_progress_product,
    serialize_promise_progress_product,
    serialize_shadow_matrix,
)
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
from pbi_xbrl.promise_progress_workbook_preview import (
    EXPECTED_ANF_PRODUCT_SHA256,
    EXPECTED_ANF_SHADOW_SHA256,
    EXPECTED_ANF_WORKBOOK_SHA256,
    PRODUCT_V2_PRESENTATION_CONTRACT_ID,
    PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID,
    PromiseProgressWorkbookPreviewError,
    _cell_text,
    _parse_xml,
    _resolve_target_sheet,
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
)
from scripts.build_anf_promise_progress_product_v2 import (
    SOURCE_SET_ID,
    _json_bytes,
    build_actual_definition_compatibility_report,
    build_anf_product_v2_source_set,
    build_capability_completion_report,
    build_legacy_capability_completeness_report,
    build_needs_review_audit,
    build_range_parser_replay_report,
    build_timeline_actual_progress_role_report,
    build_timeline_knowledge_date_report,
)


REPO = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
LEGACY_WORKBOOK = SOURCE_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
DESIGN_LOCK = SOURCE_ROOT / "audit" / "promise_progress_design_lock"
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

EXPECTED_V2_SOURCE_SET_SHA256 = "73a385b0d9c351b5356c34b06ef8d3bb71fcc9f9b503f278bdc550621016a877"
EXPECTED_V2_PRODUCT_SHA256 = "72266543e1c122691dfcdd6d0ee0e472707e527029e719a50881c417da328a05"
EXPECTED_V2_SHADOW_SHA256 = "c32fe7e85d69b1811b92e12e2e55d36fdb63930d8704d48638855c08d62de3c6"
EXPECTED_V2_WORKBOOK_SHA256 = "9476c01ef38945a0e0641a3f1ca8d38c8bc66ccf73eed691a753928c86e24b1d"
EXPECTED_V2_CANONICAL_OOXML_SHA256 = "6d9f3653cd4bddedb5f9e41ac2c8994f05443a31ab217c321b3b3e8a0450c09c"
EXPECTED_V2_TARGET_SEMANTIC_SHA256 = "ff97dc2064c83b574c4c3f27c6a5b83a9b6e6b4ede8bd2ce0886ec9e544f4a28"
EXPECTED_V2_TRACE_SHA256 = "4e06902da38963c04e439a921248946550247c7cf26c5be3943837b561a34f7c"
EXPECTED_V2_MANIFEST_FILE_SHA256 = "d9cf40475d444043fdb3b21507b1efd2ca05115a62287ce406e77e8e6d5a7d3e"
EXPECTED_V2_MANIFEST_DIGEST = "db9c8c27ee37c4275768dcd34fc7e11b64e82e6eb21b4c1fdcca3fd4e5dfbc30"


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
