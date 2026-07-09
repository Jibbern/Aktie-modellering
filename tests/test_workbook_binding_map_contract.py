from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

STANDARD_VISIBLE_SHEETS = {
    "SUMMARY",
    "Valuation",
    "BS_Segments",
    "Operating_Drivers",
    "{ticker}_Investment_Case",
    "Quarter_Notes_UI",
    "Promise_Progress_UI",
    "QA_Log",
    "Needs_Review",
    "QA_Checks",
}

REQUIRED_KEYS = {
    "binding_id",
    "sheet",
    "section",
    "target",
    "row_family",
    "normalized_field",
    "value_shape",
    "required",
    "source_policy",
    "missing_source_behavior",
    "promotion_requirement",
    "validation_rule",
    "writable",
}

VALUE_SHAPES = {
    "scalar",
    "quarterly_series",
    "annual_series",
    "table_rows",
    "text_block",
    "validation_rows",
}

REQUIRED_ROW_SCHEMA_COLUMNS_BY_BINDING = {
    "pp_annual_guidance_rows": {
        "metric",
        "initial_guide",
        "q1_update",
        "q2_update",
        "q3_update",
        "q4_update",
        "actual",
        "status",
        "notes_source",
    },
    "pp_open_guidance_rows": {
        "metric",
        "initial_guide",
        "q1_update",
        "q2_update",
        "q3_update",
        "q4_update",
        "actual",
        "status",
        "notes_source",
    },
    "qn_quarter_note_rows": {
        "theme",
        "quarter",
        "metric",
        "commentary",
        "model_implication",
        "source",
    },
    "od_watchlist_rows": {
        "topic",
        "current_read",
        "source",
        "why_it_matters",
    },
    "qa_log_validation_rows": {
        "severity",
        "rule_id",
        "field",
        "message",
        "source_ref",
        "suggested_action",
    },
    "needs_review_validation_rows": {
        "severity",
        "rule_id",
        "field",
        "message",
        "source_ref",
        "suggested_action",
    },
    "qa_checks_mapping_gap_rows": {
        "severity",
        "rule_id",
        "field",
        "message",
        "source_ref",
        "suggested_action",
    },
}


def _payload() -> dict:
    return json.loads((ROOT / "docs" / "workbook_binding_map.json").read_text(encoding="utf-8"))


def _manifest() -> dict:
    return json.loads((ROOT / "docs" / "standard_template_shell_manifest.json").read_text(encoding="utf-8"))


def _schema() -> dict:
    return json.loads((ROOT / "docs" / "normalized_company_data.schema.json").read_text(encoding="utf-8"))


def _canonical_field(path: str) -> str:
    return ".".join(part for part in path.split(".") if not part.isdigit())


_A1_RE = re.compile(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$")


def _col_to_int(col: str) -> int:
    value = 0
    for char in col:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value


def _parse_range(target: str) -> tuple[int, int, int, int]:
    match = _A1_RE.match(target)
    assert match, f"Invalid A1 range: {target!r}"
    c1, r1, c2, r2 = match.groups()
    left = _col_to_int(c1)
    top = int(r1)
    right = _col_to_int(c2)
    bottom = int(r2)
    assert left <= right and top <= bottom, f"Invalid reversed A1 range: {target!r}"
    return left, top, right, bottom


def _overlaps(first: tuple[int, int, int, int], second: tuple[int, int, int, int]) -> bool:
    f_left, f_top, f_right, f_bottom = first
    s_left, s_top, s_right, s_bottom = second
    return not (f_right < s_left or s_right < f_left or f_bottom < s_top or s_bottom < f_top)


def _contains(outer: tuple[int, int, int, int], inner: tuple[int, int, int, int]) -> bool:
    o_left, o_top, o_right, o_bottom = outer
    i_left, i_top, i_right, i_bottom = inner
    return o_left <= i_left and i_right <= o_right and o_top <= i_top and i_bottom <= o_bottom


def _manifest_zone_maps() -> tuple[dict[tuple[str, str], tuple[int, int, int, int]], dict[str, list[tuple[str, tuple[int, int, int, int]]]]]:
    writable: dict[tuple[str, str], tuple[int, int, int, int]] = {}
    non_writable: dict[str, list[tuple[str, tuple[int, int, int, int]]]] = {}
    for sheet in _manifest()["sheets"]:
        sheet_name = sheet["sheet"]
        for zone in sheet["writable_zones"]:
            writable[(sheet_name, zone["zone_id"])] = _parse_range(zone["target"])
        for zone in sheet["non_writable_zones"]:
            non_writable.setdefault(sheet_name, []).append((zone["zone_id"], _parse_range(zone["target"])))
    return writable, non_writable


def test_workbook_binding_map_covers_visible_sheets_and_required_metadata() -> None:
    entries = _payload()["bindings"]

    assert {entry["sheet"] for entry in entries} >= STANDARD_VISIBLE_SHEETS
    binding_ids = [entry["binding_id"] for entry in entries]
    assert len(binding_ids) == len(set(binding_ids))

    for entry in entries:
        assert REQUIRED_KEYS <= set(entry)
        assert entry["binding_id"]
        assert entry["sheet"]
        assert entry["section"]
        assert entry["target"]
        assert entry.get("anchor_label") or entry.get("named_range")
        assert entry["row_family"]
        assert entry["normalized_field"]
        assert entry["value_shape"] in VALUE_SHAPES
        assert isinstance(entry["required"], bool)
        assert entry["source_policy"] in {"source-backed", "profile-backed", "manual", "derived", "validation-output"}
        assert entry["missing_source_behavior"]
        assert entry["promotion_requirement"] in {"required", "optional", "blocked_if_missing", "manual_review"}
        assert entry["validation_rule"]
        assert isinstance(entry["writable"], bool)


def test_required_binding_fields_are_represented_in_normalized_schema() -> None:
    schema_fields = set(_schema()["x-normalized-fields"])
    missing = sorted(
        _canonical_field(entry["normalized_field"])
        for entry in _payload()["bindings"]
        if entry["required"] and _canonical_field(entry["normalized_field"]) not in schema_fields
    )

    assert missing == []


def test_writable_bindings_target_manifest_writable_zones() -> None:
    manifest = _manifest()
    writable_zone_ids = {
        zone["zone_id"]
        for sheet in manifest["sheets"]
        for zone in sheet["writable_zones"]
    }
    non_writable_zone_ids = {
        zone["zone_id"]
        for sheet in manifest["sheets"]
        for zone in sheet["non_writable_zones"]
    }

    for entry in _payload()["bindings"]:
        if not entry["writable"]:
            continue
        assert entry["shell_zone"] in writable_zone_ids
        assert entry["shell_zone"] not in non_writable_zone_ids


def test_required_shell_anchors_have_bindings() -> None:
    manifest = _manifest()
    bindings_by_zone = {entry["shell_zone"] for entry in _payload()["bindings"] if entry["required"]}

    missing = [
        anchor["zone_id"]
        for anchor in manifest["required_anchors"]
        if anchor["zone_id"] not in bindings_by_zone
    ]

    assert missing == []


def test_broad_binding_targets_have_anchor_and_row_family_rules() -> None:
    for entry in _payload()["bindings"]:
        target = entry["target"]
        if ":" not in target:
            continue
        assert entry["row_family"]
        assert entry.get("anchor_label") or entry.get("named_range")
        assert entry.get("shell_zone")


def test_writable_binding_targets_are_valid_a1_ranges_inside_declared_shell_zone() -> None:
    writable_zones, _non_writable = _manifest_zone_maps()
    failures: list[str] = []

    for entry in _payload()["bindings"]:
        if not entry["writable"]:
            continue
        target_range = _parse_range(entry["target"])
        shell_range = writable_zones[(entry["sheet"], entry["shell_zone"])]
        if not _contains(shell_range, target_range):
            failures.append(f"{entry['binding_id']} target {entry['target']} outside shell zone {entry['shell_zone']}")

    assert failures == []


def test_binding_targets_do_not_overlap_manifest_non_writable_zones() -> None:
    _writable_zones, non_writable_zones = _manifest_zone_maps()
    failures: list[str] = []

    for entry in _payload()["bindings"]:
        if not entry["writable"]:
            continue
        target_range = _parse_range(entry["target"])
        for zone_id, non_writable_range in non_writable_zones.get(entry["sheet"], []):
            if _overlaps(target_range, non_writable_range):
                failures.append(f"{entry['binding_id']} overlaps non-writable zone {zone_id}")

    assert failures == []


def test_workbook_binding_map_doc_rejects_post_render_patching_as_runtime_strategy() -> None:
    text = (ROOT / "docs" / "workbook_binding_map.md").read_text(encoding="utf-8").lower()

    assert "post-render" in text
    assert "mapping gaps" in text
    assert "manual review" in text
    assert "visible sheets" in text
    assert "writable zones" in text


def test_table_row_bindings_define_concrete_row_schema() -> None:
    entries = {entry["binding_id"]: entry for entry in _payload()["bindings"]}

    for binding_id, required_columns in REQUIRED_ROW_SCHEMA_COLUMNS_BY_BINDING.items():
        entry = entries[binding_id]
        row_schema = entry.get("row_schema")
        assert isinstance(row_schema, list), binding_id
        columns = {column["column_id"] for column in row_schema}
        assert columns >= required_columns
        for column in row_schema:
            assert column["column_id"]
            assert column["source_field"]
            assert column["target_column"]
            assert column["missing_behavior"]
