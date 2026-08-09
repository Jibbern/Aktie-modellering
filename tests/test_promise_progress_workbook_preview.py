from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

from pbi_xbrl.longitudinal_memory.promise_progress_projection import (
    ANNUAL_BLOCK_ID,
    OPEN_BLOCK_ID,
    SCORECARD_BLOCK_ID,
    TIMELINE_BLOCK_ID,
    PromiseProgressProduct,
    build_promise_progress_product,
    serialize_promise_progress_product,
    serialize_shadow_matrix,
)
from pbi_xbrl.longitudinal_memory.sector_packs.retail import RETAIL_SECTOR_PACK
from pbi_xbrl.longitudinal_memory.source_adapter.builder import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile
from pbi_xbrl.promise_progress_workbook_preview import (
    EXPECTED_ANF_PRODUCT_SHA256,
    EXPECTED_ANF_SHADOW_SHA256,
    EXPECTED_ANF_WORKBOOK_SHA256,
    IDENTITY_TRANSFORM_ID,
    PRESENTATION_CONTRACT_ID,
    SOURCE_SUMMARY_TRANSFORM_ID,
    STORE_PROGRESS_TRANSFORM_ID,
    PromiseProgressWorkbookBindingPlan,
    PromiseProgressWorkbookPreviewError,
    WorkbookBinding,
    _cell_text,
    _expand_range,
    _parse_xml,
    _source_summary_presentation,
    _store_progress_presentation,
    _style_alignment_map,
    _style_palette,
    _workbook_sheet_snapshot,
    _write_inline_string,
    build_legacy_difference_report,
    build_promise_progress_workbook_binding_plan,
    build_workbook_trace,
    canonical_workbook_content_sha256,
    materialize_promise_progress_preview,
    measure_presentation_text,
    plan_presentation_row,
    sha256_file,
    target_sheet_semantic_sha256,
    validate_preview_semantics,
    validate_preview_structure,
    validate_preview_visual_fit,
    validate_promise_progress_workbook_binding_plan,
)


REPO = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
LEGACY_WORKBOOK = SOURCE_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
DESIGN_LOCK = SOURCE_ROOT / "audit" / "promise_progress_design_lock"
ORACLE = REPO / "tests" / "fixtures" / "promise_progress" / "anf_legacy_oracle.v1.json"
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def _strict_json(path: Path) -> dict:
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key!r}")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=object_pairs)


@pytest.fixture(scope="module")
def anf_product() -> PromiseProgressProduct:
    oracle = _strict_json(ORACLE)
    package = build_source_native_sidecar(
        REPO / oracle["source_package_fixture"],
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    ).package
    return build_promise_progress_product(package, oracle["projection_plan"])


@pytest.fixture(scope="module")
def binding_plan(anf_product: PromiseProgressProduct) -> PromiseProgressWorkbookBindingPlan:
    return build_promise_progress_workbook_binding_plan(anf_product, design_lock_root=DESIGN_LOCK)


@pytest.fixture(scope="module")
def generated_previews(tmp_path_factory, anf_product, binding_plan):
    root = tmp_path_factory.mktemp("promise-progress-preview")
    first = root / "first.xlsx"
    second = root / "second.xlsx"
    first_result = materialize_promise_progress_preview(
        anf_product,
        binding_plan,
        legacy_workbook=LEGACY_WORKBOOK,
        output_workbook=first,
        design_lock_root=DESIGN_LOCK,
    )
    second_result = materialize_promise_progress_preview(
        anf_product,
        binding_plan,
        legacy_workbook=LEGACY_WORKBOOK,
        output_workbook=second,
        design_lock_root=DESIGN_LOCK,
    )
    return first, second, first_result, second_result


def _replace_binding(plan, binding_id: str, **changes) -> PromiseProgressWorkbookBindingPlan:
    replacements = []
    found = False
    for binding in plan.bindings:
        if binding.binding_id == binding_id:
            replacements.append(dataclasses.replace(binding, **changes))
            found = True
        else:
            replacements.append(binding)
    assert found
    return dataclasses.replace(plan, bindings=tuple(replacements))


def _first(plan, kind: str) -> WorkbookBinding:
    return next(binding for binding in plan.bindings if binding.binding_kind == kind)


def _role_binding(plan, block_id: str, field_role: str, row: int | None = None) -> WorkbookBinding:
    return next(
        binding for binding in plan.bindings
        if binding.binding_kind == "product_field"
        and binding.block_id == block_id
        and binding.field_role == field_role
        and (row is None or int("".join(filter(str.isdigit, binding.anchor_cell))) == row)
    )


def test_accepted_product_and_oracle_hashes_are_unchanged(anf_product) -> None:
    assert hashlib.sha256(serialize_promise_progress_product(anf_product)).hexdigest() == EXPECTED_ANF_PRODUCT_SHA256
    assert hashlib.sha256(serialize_shadow_matrix(anf_product)).hexdigest() == EXPECTED_ANF_SHADOW_SHA256
    assert sha256_file(LEGACY_WORKBOOK) == EXPECTED_ANF_WORKBOOK_SHA256


def test_binding_plan_is_deterministic_complete_and_typed(anf_product, binding_plan) -> None:
    rebuilt = build_promise_progress_workbook_binding_plan(anf_product, design_lock_root=DESIGN_LOCK)
    assert rebuilt.to_dict() == binding_plan.to_dict()
    kinds = [binding.binding_kind for binding in binding_plan.bindings]
    assert kinds.count("product_field") == 284
    assert kinds.count("row_trace") == 31
    assert kinds.count("product_metadata") == 1
    assert kinds.count("timeline_group_header") == 4
    assert len({binding.binding_id for binding in binding_plan.bindings}) == len(binding_plan.bindings)
    assert len({binding.anchor_cell for binding in binding_plan.bindings}) == len(binding_plan.bindings)


def test_binding_plan_uses_only_reviewed_destinations_and_hidden_trace(anf_product, binding_plan) -> None:
    validate_promise_progress_workbook_binding_plan(anf_product, binding_plan, design_lock_root=DESIGN_LOCK)
    assert all(not binding.anchor_cell.startswith(("M", "N")) for binding in binding_plan.bindings)
    o_bindings = [binding for binding in binding_plan.bindings if binding.anchor_cell.startswith("O")]
    assert o_bindings
    assert all(binding.binding_kind == "row_trace" and binding.display_text.endswith("@1") for binding in o_bindings)


def test_missing_and_explicit_zero_remain_distinct_in_ooxml_cell() -> None:
    cell = ET.Element(f"{{{MAIN_NS}}}c", {"r": "A1"})
    _write_inline_string(cell, "")
    assert _cell_text(cell) == ""
    assert cell.find(f"{{{MAIN_NS}}}is") is None
    _write_inline_string(cell, "0")
    assert _cell_text(cell) == "0"
    assert cell.find(f"{{{MAIN_NS}}}is/{{{MAIN_NS}}}t").text == "0"


def test_preview_replays_every_source_native_binding_exactly(anf_product, binding_plan, generated_previews) -> None:
    first = generated_previews[0]
    result = validate_preview_semantics(anf_product, binding_plan, preview_workbook=first)
    assert result["passed"]
    assert result["product_field_count"] == 284
    assert result["product_row_count"] == 31
    assert result["missing_field_count"] > 0
    assert result["validations"]["actual_progress_distinct"]


def test_preview_places_only_row_ids_in_o_and_keeps_m_n_blank(anf_product, binding_plan, generated_previews) -> None:
    result = validate_preview_structure(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=generated_previews[0],
        plan=binding_plan,
        design_lock_root=DESIGN_LOCK,
    )
    assert result["validations"]["m_blank"]
    assert result["validations"]["n_blank"]
    assert result["validations"]["o_row_ids_exact"]
    assert len(result["o_row_ids"]) == 31


def test_status_cells_use_reviewed_palette_without_reassessment(anf_product, binding_plan, generated_previews) -> None:
    result = validate_preview_structure(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=generated_previews[0],
        plan=binding_plan,
        design_lock_root=DESIGN_LOCK,
    )
    assert result["validations"]["status_styles"]
    assert {row["status_code"] for row in result["status_style_results"]} == {"needs_review", "open"}
    assert all(row["pass"] for row in result["status_style_results"])


def test_unused_status_capacity_has_no_stale_legacy_status_fill(binding_plan, generated_previews) -> None:
    snapshot, _ = _workbook_sheet_snapshot(generated_previews[0], binding_plan.sheet_name)
    bound = {binding.anchor_cell for binding in binding_plan.bindings}
    for destination in ("I18", "I19", "I20", "F41", "F56", "G88", "G92", "G102"):
        assert destination not in bound
        assert snapshot["cells"][destination]["value"] == ""
    with ZipFile(generated_previews[0]) as archive:
        alignment = _style_alignment_map(archive)
        palette = _style_palette(archive)
    assert all(alignment[snapshot["cells"][destination]["style_id"]]["horizontal"] == "center" for destination in ("I18", "F41", "G88"))
    bound_status_fills = {
        palette[snapshot["cells"][binding.anchor_cell]["style_id"]]
        for binding in binding_plan.bindings if binding.status_code is not None
    }
    assert all(palette[snapshot["cells"][destination]["style_id"]] not in bound_status_fills for destination in ("I18", "F41", "G88"))


def test_missing_source_native_value_is_not_repaired_from_legacy(anf_product, binding_plan, generated_previews) -> None:
    report = build_legacy_difference_report(
        anf_product,
        binding_plan,
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=generated_previews[0],
    )
    by_field = {field.field_id: field for field in anf_product.fields}
    candidates = [
        row
        for row in report["differences"]
        if row["source_native_field_id"] in by_field
        and by_field[row["source_native_field_id"]].display_value.value_form == "missing"
        and row["legacy_display_value"]
    ]
    assert candidates
    assert all(row["preview_display_value"] == "" for row in candidates)


def test_only_the_target_worksheet_ooxml_part_changes(binding_plan, generated_previews) -> None:
    first = generated_previews[0]
    with ZipFile(LEGACY_WORKBOOK) as legacy, ZipFile(first) as preview:
        assert legacy.namelist() == preview.namelist()
        changed = [name for name in legacy.namelist() if legacy.read(name) != preview.read(name)]
    assert set(changed) == {"xl/styles.xml", binding_plan.sheet_part}


def test_fresh_regeneration_is_raw_canonical_and_semantically_identical(binding_plan, generated_previews) -> None:
    first, second, first_result, second_result = generated_previews
    assert first.read_bytes() == second.read_bytes()
    assert first_result["preview_workbook_sha256"] == second_result["preview_workbook_sha256"]
    assert canonical_workbook_content_sha256(first) == canonical_workbook_content_sha256(second)
    assert target_sheet_semantic_sha256(first, binding_plan) == target_sheet_semantic_sha256(second, binding_plan)


def test_workbook_trace_covers_every_field_and_written_value(anf_product, binding_plan, generated_previews) -> None:
    trace = build_workbook_trace(anf_product, binding_plan, preview_workbook=generated_previews[0])
    assert trace["record_count"] == len(binding_plan.bindings)
    assert all(row["expected_display_value"] == row["written_display_value"] for row in trace["records"])
    assert len([row for row in trace["records"] if row["binding_kind"] == "product_field"]) == 284


def test_invalid_binding_display_value_fails_before_write(anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    mutated = _replace_binding(binding_plan, field.binding_id, presentation_text=field.presentation_text + " legacy fallback")
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="identity@1|display transform"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_duplicate_destination_fails_before_write(anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    duplicate = dataclasses.replace(field, binding_id=field.binding_id + ":duplicate")
    mutated = dataclasses.replace(binding_plan, bindings=binding_plan.bindings + (duplicate,))
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="duplicate workbook destinations"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_missing_product_destination_fails_before_write(anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    mutated = dataclasses.replace(
        binding_plan,
        bindings=tuple(binding for binding in binding_plan.bindings if binding.binding_id != field.binding_id),
    )
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="cover every product field"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_unknown_row_id_fails_before_write(anf_product, binding_plan) -> None:
    row = _first(binding_plan, "row_trace")
    mutated = _replace_binding(
        binding_plan,
        row.binding_id,
        source_row_ids=("promise-progress-row:unknown@1",),
        machine_value="promise-progress-row:unknown@1",
    )
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="cover every row trace|unknown row or field|row trace differs"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_unsupported_display_type_fails_before_write(anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    mutated = _replace_binding(binding_plan, field.binding_id, display_type="formula")
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="unsupported display type"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_unreviewed_destination_fails_before_write(anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    mutated = _replace_binding(binding_plan, field.binding_id, anchor_cell="Z999", display_range="Z999")
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="unreviewed destination"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_invalid_plan_leaves_no_partial_preview(tmp_path, anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    mutated = _replace_binding(binding_plan, field.binding_id, display_type="formula")
    output = tmp_path / "must-not-exist.xlsx"
    with pytest.raises(PromiseProgressWorkbookPreviewError):
        materialize_promise_progress_preview(
            anf_product,
            mutated,
            legacy_workbook=LEGACY_WORKBOOK,
            output_workbook=output,
            design_lock_root=DESIGN_LOCK,
        )
    assert not output.exists()


def test_template_hash_mismatch_fails_before_output(tmp_path, anf_product, binding_plan) -> None:
    mutated_template = tmp_path / "mutated.xlsx"
    mutated_template.write_bytes(LEGACY_WORKBOOK.read_bytes() + b"mutation")
    output = tmp_path / "must-not-exist.xlsx"
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="legacy workbook SHA"):
        materialize_promise_progress_preview(
            anf_product,
            binding_plan,
            legacy_workbook=mutated_template,
            output_workbook=output,
            design_lock_root=DESIGN_LOCK,
        )
    assert not output.exists()


def test_existing_preview_is_never_overwritten(tmp_path, anf_product, binding_plan) -> None:
    output = tmp_path / "existing.xlsx"
    output.write_bytes(b"keep-me")
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="refusing to overwrite"):
        materialize_promise_progress_preview(
            anf_product,
            binding_plan,
            legacy_workbook=LEGACY_WORKBOOK,
            output_workbook=output,
            design_lock_root=DESIGN_LOCK,
        )
    assert output.read_bytes() == b"keep-me"


def test_structural_contract_preserves_exact_invariants_and_reviewed_layout(binding_plan, generated_previews) -> None:
    result = validate_preview_structure(
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=generated_previews[0],
        plan=binding_plan,
        design_lock_root=DESIGN_LOCK,
    )
    assert result["passed"]
    assert result["changed_ooxml_parts"] == ["xl/styles.xml", "xl/worksheets/sheet7.xml"]
    assert result["unexpected_part_difference_count"] == 0
    assert result["unexpected_changed_cells"] == []
    assert result["feature_counts"]["formulas"] == 0


def test_legacy_comparison_has_no_mapping_defect_or_unresolved_classification(
    anf_product, binding_plan, generated_previews
) -> None:
    report = build_legacy_difference_report(
        anf_product,
        binding_plan,
        legacy_workbook=LEGACY_WORKBOOK,
        preview_workbook=generated_previews[0],
    )
    assert report["mapping_defect_count"] == 0
    assert report["unresolved_count"] == 0
    assert report["classification_counts"]["accepted source-native semantic correction"] > 0
    assert report["classification_counts"]["expected legacy defect removal"] > 0


def test_materializer_contains_no_first_row_or_legacy_lookup_shortcut() -> None:
    source = (REPO / "pbi_xbrl" / "promise_progress_workbook_preview.py").read_text(encoding="utf-8")
    assert ".items[0]" not in source
    assert "fuzzy" not in source.casefold()
    assert "legacy_display_value" not in source.split("def materialize_promise_progress_preview", 1)[1].split(
        "def canonical_workbook_content_sha256", 1
    )[0]


def test_presentation_contract_v2_is_closed_and_presentation_only(binding_plan) -> None:
    contract = binding_plan.presentation_contract.to_dict()
    assert contract["contract_id"] == PRESENTATION_CONTRACT_ID
    assert contract["economics_authority"] == "none-presentation-only"
    assert contract["transform_ids"] == [IDENTITY_TRANSFORM_ID, STORE_PROGRESS_TRANSFORM_ID, SOURCE_SUMMARY_TRANSFORM_ID]


def test_zero_clipped_visible_fields(binding_plan, generated_previews) -> None:
    result = validate_preview_visual_fit(preview_workbook=generated_previews[0], plan=binding_plan)
    assert result["passed"]
    assert result["clipped_visible_field_count"] == 0


def test_zero_adjacent_cell_overflow_dependency(binding_plan, generated_previews) -> None:
    result = validate_preview_visual_fit(preview_workbook=generated_previews[0], plan=binding_plan)
    assert result["validations"]["zero_overflow_dependency"]
    assert result["overflow_dependency_count"] == 0


def test_g17_preserves_all_three_store_progress_facts(binding_plan, generated_previews) -> None:
    binding = _role_binding(binding_plan, ANNUAL_BLOCK_ID, "actual", 17)
    assert binding.anchor_cell == "G17" and binding.display_range == "G17:H17"
    assert binding.display_transform_id == STORE_PROGRESS_TRANSFORM_ID
    assert binding.presentation_text == "62 openings / 22 closures\nNet: 40"
    snapshot, _ = _workbook_sheet_snapshot(generated_previews[0], binding_plan.sheet_name)
    assert snapshot["cells"]["G17"]["value"] == binding.presentation_text


def test_visible_grid_is_exactly_u24_a_through_l(binding_plan, generated_previews) -> None:
    result = validate_preview_structure(
        legacy_workbook=LEGACY_WORKBOOK, preview_workbook=generated_previews[0],
        plan=binding_plan, design_lock_root=DESIGN_LOCK,
    )
    assert all(result["column_contract"][column] == {"width": 24.0, "hidden": False} for column in "ABCDEFGHIJKL")


def test_hidden_support_widths_and_states_are_exact(binding_plan, generated_previews) -> None:
    result = validate_preview_structure(
        legacy_workbook=LEGACY_WORKBOOK, preview_workbook=generated_previews[0],
        plan=binding_plan, design_lock_root=DESIGN_LOCK,
    )
    assert result["column_contract"]["M"] == {"width": 4.0, "hidden": True}
    assert result["column_contract"]["N"] == {"width": 4.0, "hidden": True}
    assert result["column_contract"]["O"] == {"width": 13.0, "hidden": True}


def test_block_specific_span_maps_are_exact(binding_plan) -> None:
    expected = {
        SCORECARD_BLOCK_ID: {"category": "A", "score": "B", "evidence": "C:F", "read": "G:L"},
        ANNUAL_BLOCK_ID: {"metric": "A", "initial_guide": "B", "q1_guide": "C", "q2_guide": "D", "q3_guide": "E", "q4_guide": "F", "actual": "G:H", "status": "I", "notes_source": "J:L"},
        OPEN_BLOCK_ID: {"metric": "A:B", "current_guide": "C:D", "horizon": "E", "status": "F", "notes_source": "G:L"},
        TIMELINE_BLOCK_ID: {"metric": "A", "previous_guide": "B", "current_guide": "C", "change_type": "D", "actual": "E", "progress": "F", "status": "G", "horizon": "H", "stated_in": "I", "source_date": "J", "source_note": "K:L"},
    }
    actual: dict[str, dict[str, str]] = {}
    for row in binding_plan.presentation_contract.field_layouts:
        actual.setdefault(row.block_id, {})[row.field_role] = row.start_column if row.start_column == row.end_column else f"{row.start_column}:{row.end_column}"
    assert actual == expected


def test_permitted_merges_are_exactly_materialized(binding_plan, generated_previews) -> None:
    with ZipFile(generated_previews[0]) as archive:
        _, part = __import__("pbi_xbrl.promise_progress_workbook_preview", fromlist=["_resolve_target_sheet"])._resolve_target_sheet(archive, binding_plan.sheet_name)
        root = _parse_xml(archive.read(part))
    merges = [node.get("ref") for node in root.findall(f".//{{{MAIN_NS}}}mergeCell")]
    assert merges == list(binding_plan.presentation_contract.permitted_merges)


def test_merged_non_anchor_cells_are_blank(binding_plan, generated_previews) -> None:
    snapshot, _ = _workbook_sheet_snapshot(generated_previews[0], binding_plan.sheet_name)
    assert all(
        snapshot["cells"].get(cell, {}).get("value", "") == ""
        for merged_range in binding_plan.presentation_contract.permitted_merges
        for cell in _expand_range(merged_range)[1:]
    )


def test_required_wrap_roles_are_explicit(binding_plan) -> None:
    always = {"metric", "category", "evidence", "read", "notes_source", "source_note"}
    assert all(binding.wrap_text for binding in binding_plan.bindings if binding.binding_kind == "product_field" and binding.field_role in always)


def test_compact_status_and_date_roles_do_not_wrap(binding_plan) -> None:
    compact = {"status", "source_date", "stated_in", "score"}
    assert all(not binding.wrap_text for binding in binding_plan.bindings if binding.binding_kind == "product_field" and binding.field_role in compact)


def test_row_heights_use_only_approved_tiers(binding_plan) -> None:
    assert set(dict(binding_plan.row_heights).values()).issubset({24, 40, 56, 72})
    assert 24 in dict(binding_plan.row_heights).values()
    assert 40 in dict(binding_plan.row_heights).values()


def test_timeline_height_never_exceeds_56(binding_plan) -> None:
    planned_rows = dict(binding_plan.row_heights)
    timeline_rows = {row for row in planned_rows if any(binding.block_id == TIMELINE_BLOCK_ID and int("".join(filter(str.isdigit, binding.anchor_cell))) == row for binding in binding_plan.bindings)}
    assert all(planned_rows[row] <= 56 for row in timeline_rows)


def test_non_timeline_height_never_exceeds_72(binding_plan) -> None:
    assert all(height <= 72 for _, height in binding_plan.row_heights)


def test_layout_capacity_exceeded_fails_closed(binding_plan) -> None:
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="layout_capacity_exceeded"):
        plan_presentation_row(
            binding_plan.presentation_contract,
            block_id=TIMELINE_BLOCK_ID,
            values_by_role={"source_note": "investor qualifier " * 500},
        )


def test_fit_measurement_is_deterministic_and_metric_based() -> None:
    first = measure_presentation_text("A measured investor-facing sentence", span_width=2, wrap_text=True, allocated_height_points=40)
    second = measure_presentation_text("A measured investor-facing sentence", span_width=2, wrap_text=True, allocated_height_points=40)
    assert first == second
    assert first["measurement_method"] == "pillow-freetype-calibri11@1"
    assert first["required_width_pixels"] > 0 and first["font_file_sha256"]


def test_visual_acceptance_is_not_character_count_only() -> None:
    wide = measure_presentation_text("W" * 20, span_width=1, wrap_text=False)
    narrow = measure_presentation_text("i" * 20, span_width=1, wrap_text=False)
    assert wide["required_width_pixels"] > narrow["required_width_pixels"]


def test_identity_transform_preserves_canonical_text_exactly(binding_plan) -> None:
    identities = [binding for binding in binding_plan.bindings if binding.display_transform_id == IDENTITY_TRANSFORM_ID]
    assert identities
    assert all(binding.presentation_text == binding.canonical_display_text for binding in identities)


def test_store_progress_transform_derives_from_structured_components(anf_product) -> None:
    field = next(field for field in anf_product.fields if field.anchor_cell == "G17")
    assert _store_progress_presentation(field.display_value.machine_value) == "62 openings / 22 closures\nNet: 40"
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="structured components"):
        _store_progress_presentation("62 openings / 22 closures; net 40")


def test_source_summary_transform_derives_from_typed_source_identity(anf_product) -> None:
    row = next(row for row in anf_product.ordered_rows if row.visible_sheet_row == 39)
    field = next(field for field in row.fields if field.field_role == "notes_source")
    assert _source_summary_presentation(field, row.fields) == "Mar 4 release + Mar 4 transcript · Current guidance"


def test_unknown_display_transform_is_rejected(anf_product, binding_plan) -> None:
    field = _first(binding_plan, "product_field")
    mutated = _replace_binding(binding_plan, field.binding_id, display_transform_id="unknown@1")
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="unknown display transform"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_source_summary_cannot_drop_material_review_qualifier(anf_product, binding_plan) -> None:
    binding = next(binding for binding in binding_plan.bindings if binding.display_transform_id == SOURCE_SUMMARY_TRANSFORM_ID and "tolerance not disclosed" in binding.presentation_text)
    mutated = _replace_binding(binding_plan, binding.binding_id, presentation_text=binding.presentation_text.split(" · ")[0])
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="display transform differs"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_identity_transform_cannot_drop_range_bound_or_basis_meaning(anf_product, binding_plan) -> None:
    binding = next(binding for binding in binding_plan.bindings if binding.binding_kind == "product_field" and binding.value_form in {"range", "bound"})
    mutated = _replace_binding(binding_plan, binding.binding_id, presentation_text="shortened")
    with pytest.raises(PromiseProgressWorkbookPreviewError, match="identity@1"):
        validate_promise_progress_workbook_binding_plan(anf_product, mutated, design_lock_root=DESIGN_LOCK)


def test_source_summary_trace_preserves_full_shadow_lineage(anf_product, binding_plan, generated_previews) -> None:
    trace = build_workbook_trace(anf_product, binding_plan, preview_workbook=generated_previews[0])
    rows = [row for row in trace["records"] if row["display_transform_id"] == SOURCE_SUMMARY_TRANSFORM_ID]
    assert rows and all(row["lineage_full_text_digest"] and row["source_native_lineage_digests"] for row in rows)
    assert any(len(row["canonical_display_text"]) > len(row["presentation_text"]) for row in rows)


def test_timeline_source_summary_does_not_duplicate_source_date(binding_plan) -> None:
    rows = [binding for binding in binding_plan.bindings if binding.block_id == TIMELINE_BLOCK_ID and binding.field_role == "source_note"]
    assert rows
    assert all("2025-" not in binding.presentation_text and "2026-" not in binding.presentation_text for binding in rows)


def test_materializer_has_no_legacy_value_fallback_after_v2() -> None:
    source = (REPO / "pbi_xbrl" / "promise_progress_workbook_preview.py").read_text(encoding="utf-8")
    materializer = source.split("def materialize_promise_progress_preview", 1)[1].split("def canonical_workbook_content_sha256", 1)[0]
    assert "legacy_value" not in materializer and "fallback" not in materializer.casefold()


def test_presentation_contract_has_no_ticker_specific_layout_branch(binding_plan) -> None:
    contract = binding_plan.presentation_contract.to_dict()
    assert "company_id" not in contract and "ticker" not in contract
    result = plan_presentation_row(contract=binding_plan.presentation_contract, block_id=OPEN_BLOCK_ID, values_by_role={"metric": "Reusable metric"})
    assert result["company_specific_branch"] is False


def test_synthetic_pbi_like_promise_uses_same_contract(binding_plan) -> None:
    result = plan_presentation_row(
        binding_plan.presentation_contract,
        block_id=OPEN_BLOCK_ID,
        values_by_role={"metric": "Enterprise cost rationalization and branch optimization milestones"},
    )
    assert result["height_points"] in {24, 40, 56, 72}


def test_synthetic_gpre_like_policy_text_uses_same_contract(binding_plan) -> None:
    result = plan_presentation_row(
        binding_plan.presentation_contract,
        block_id=OPEN_BLOCK_ID,
        values_by_role={"notes_source": "Policy-contingent commodity milestone; basis differs and tolerance is not disclosed"},
    )
    assert result["height_points"] <= 72


def test_long_metric_label_fits_reviewed_span(binding_plan) -> None:
    result = plan_presentation_row(
        binding_plan.presentation_contract,
        block_id=OPEN_BLOCK_ID,
        values_by_role={"metric": "Very long investor-relevant operating segment metric label"},
    )
    assert result["measurements"]["metric"]["width_fits"]


def test_long_qualitative_horizon_fits_reviewed_tiers(binding_plan) -> None:
    result = plan_presentation_row(
        binding_plan.presentation_contract,
        block_id=OPEN_BLOCK_ID,
        values_by_role={"horizon": "Policy-contingent qualitative milestone; no exact deadline disclosed"},
    )
    assert result["height_points"] <= 72


def test_long_investor_qualifier_fits_without_truncation(binding_plan) -> None:
    text = "Definition changed; approximate range and basis differ; tolerance not disclosed"
    result = plan_presentation_row(
        binding_plan.presentation_contract,
        block_id=OPEN_BLOCK_ID,
        values_by_role={"notes_source": text},
    )
    assert result["measurements"]["notes_source"]["fit"]


def test_zero_off_target_ooxml_changes_under_v2(binding_plan, generated_previews) -> None:
    result = validate_preview_structure(
        legacy_workbook=LEGACY_WORKBOOK, preview_workbook=generated_previews[0],
        plan=binding_plan, design_lock_root=DESIGN_LOCK,
    )
    assert result["unexpected_part_differences"] == []
    assert set(result["changed_ooxml_parts"]) == {binding_plan.sheet_part, "xl/styles.xml"}


def test_fresh_regeneration_visual_fit_is_equivalent(binding_plan, generated_previews) -> None:
    first = validate_preview_visual_fit(preview_workbook=generated_previews[0], plan=binding_plan)
    second = validate_preview_visual_fit(preview_workbook=generated_previews[1], plan=binding_plan)
    assert first["validation_digest"] == second["validation_digest"]
