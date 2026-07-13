from __future__ import annotations

import json
import re
from pathlib import Path

from openpyxl import load_workbook


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
    "pivot_matrix",
    "text_block",
    "validation_rows",
}

PLANNER_ROW_CONTRACT_KEYS = {
    "row_selector",
    "row_key",
    "sort_order",
    "capacity",
    "overflow_behavior",
    "required_columns",
    "target_columns",
    "source_ref_required",
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
        "issue_id",
        "severity",
        "rule_id",
        "issue_type",
        "section",
        "root_cause",
        "message",
        "suggested_action",
        "occurrence_count",
        "visibility_disposition",
        "promotion_blocking",
        "detail_ref",
    },
    "needs_review_validation_rows": {
        "issue_id",
        "severity",
        "rule_id",
        "section",
        "normalized_path",
        "business_row_key",
        "message",
        "suggested_action",
        "occurrence_count",
        "promotion_blocking",
        "detail_ref",
    },
    "qa_checks_mapping_gap_rows": {
        "rule_id",
        "status",
        "unique_issue_count",
        "occurrence_count",
        "blocking_count",
        "actionable_count",
        "affected_sections",
        "interpretation",
        "detail_ref",
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


_A1_RE = re.compile(r"^([A-Z]+)(\d+)(?::([A-Z]+)(\d+))?$")


def _col_to_int(col: str) -> int:
    value = 0
    for char in col:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value


def _parse_range(target: str) -> tuple[int, int, int, int]:
    match = _A1_RE.match(target)
    assert match, f"Invalid A1 range: {target!r}"
    c1, r1, c2, r2 = match.groups()
    c2 = c2 or c1
    r2 = r2 or r1
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
        if entry["required"]
        and entry["source_policy"] != "validation-output"
        and _canonical_field(entry["normalized_field"]) not in schema_fields
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
    bindings_by_zone = {
        entry["shell_zone"]
        for entry in _payload()["bindings"]
        if entry["writable"] and entry.get("planning_state", "active") == "active"
    }

    missing = [
        anchor["zone_id"]
        for anchor in manifest["required_anchors"]
        if anchor["binding_required"] and anchor["zone_id"] not in bindings_by_zone
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
            assert column.get("target_column") or column.get("target_role") in {"lineage_metadata", "business_key_metadata"}
            assert column["missing_behavior"]


def test_active_collection_bindings_have_exact_typed_planner_contracts() -> None:
    for entry in _payload()["bindings"]:
        if entry.get("planning_state", "active") != "active":
            continue
        if entry["value_shape"] not in {"quarterly_series", "annual_series", "table_rows", "pivot_matrix", "validation_rows"}:
            continue
        assert PLANNER_ROW_CONTRACT_KEYS <= set(entry), entry["binding_id"]
        assert entry["planner_target"]
        assert _contains(_parse_range(entry["target"]), _parse_range(entry["planner_target"]))
        assert isinstance(entry["row_selector"], dict)
        assert entry["row_selector"].get("source_path")
        assert entry["row_key"]
        assert isinstance(entry["sort_order"], list)
        assert isinstance(entry["capacity"], int) and entry["capacity"] > 0
        assert entry["overflow_behavior"] in {"fail", "mapping_gap", "manual_review"}
        assert isinstance(entry["source_ref_required"], bool)

        if entry["value_shape"] not in {"table_rows", "validation_rows"}:
            if entry["value_shape"] == "pivot_matrix":
                assert entry["planning_mode"] == "pivot_rows"
                assert entry["row_blocks"]
                assert entry["period_axis_id"]
                assert entry["period_axis_role"] == "dependent"
                assert entry["period_field"]
                assert entry["value_field"] in entry["required_columns"]
                rows = [row for block_rows in entry["row_blocks"].values() for row in block_rows]
                assert len(rows) == len(set(rows)), entry["binding_id"]
            continue
        target_columns = entry["target_columns"]
        target_names = [column["target_column"] for column in target_columns]
        source_fields = {column["source_field"] for column in target_columns}
        row_key_only = set(entry.get("row_key_only_columns", []))
        assert len(target_names) == len(set(target_names)), entry["binding_id"]
        assert set(entry["required_columns"]) <= source_fields | row_key_only, entry["binding_id"]
        planner_range = _parse_range(entry["planner_target"])
        for target_column in target_names:
            column_index = _col_to_int(target_column)
            assert planner_range[0] <= column_index <= planner_range[2], entry["binding_id"]
        for column in target_columns:
            if entry["source_policy"] != "validation-output":
                assert column.get("target_type"), entry["binding_id"]


def test_active_collection_bindings_use_selectors_not_items_zero_shortcuts() -> None:
    active = [
        entry
        for entry in _payload()["bindings"]
        if entry.get("planning_state", "active") == "active" and entry["value_shape"] in {"quarterly_series", "annual_series", "table_rows", "pivot_matrix"}
    ]

    assert active
    assert all(".items.0." not in entry["normalized_field"] for entry in active)
    assert all(entry["row_selector"].get("source_path") for entry in active)
    assert {"bs_annual_period_headers", "bs_annual_revenue_series"} <= {entry["binding_id"] for entry in active}


def test_manifest_has_exact_cell_roles_and_merge_ownership_contracts() -> None:
    manifest = _manifest()
    cell_contracts = manifest["planner_cell_contracts"]
    merge_contracts = manifest["planner_merge_families"]

    assert cell_contracts and merge_contracts
    for contract in cell_contracts:
        assert contract["contract_id"]
        assert contract["sheet"]
        _parse_range(contract["target"])
        assert contract["writable"] is True
        assert contract["target_role"]
        assert contract["allowed_binding_ids"]
        assert contract["allowed_target_types"]
    for contract in merge_contracts:
        assert contract["merge_id"]
        assert contract["sheet"]
        _parse_range(contract["target"])
        assert contract["anchor_column"]
        assert contract["allowed_binding_ids"]


def test_manifest_merge_families_match_existing_frozen_shell_read_only() -> None:
    workbook_path = ROOT / "templates" / "standard_stock_model_template.xlsx"
    workbook = load_workbook(workbook_path, read_only=False, data_only=False)
    try:
        for contract in _manifest()["planner_merge_families"]:
            sheet_name = contract["sheet"]
            worksheet = workbook[sheet_name]
            left, top, right, bottom = _parse_range(contract["target"])
            actual = {str(item) for item in worksheet.merged_cells.ranges}
            for row in range(top, bottom + 1):
                expected = f"{_int_to_col(left)}{row}:{_int_to_col(right)}{row}"
                assert expected in actual, f"Manifest merge family is not in shell: {sheet_name}!{expected}"
    finally:
        workbook.close()


def _int_to_col(value: int) -> str:
    result = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        result = chr(ord("A") + remainder) + result
    return result


def test_p0_period_value_and_qa_contracts_are_explicit() -> None:
    entries = {entry["binding_id"]: entry for entry in _payload()["bindings"]}

    as_of = entries["summary_as_of_quarter"]
    revenue = entries["summary_latest_revenue"]
    net_income = entries["summary_latest_net_income"]
    valuation_headers = entries["valuation_period_headers"]
    valuation_outputs = entries["valuation_output_rows"]

    assert as_of["planner_target"] == "B26:B26"
    assert as_of["source_field"] == "period"
    assert as_of["normalized_field"].endswith(".period")
    assert revenue["planner_target"] == "B28:B28"
    assert revenue["source_field"] == "revenue"
    assert revenue["row_selector"]["pick"] == "latest"
    assert net_income["planner_target"] == "B30:B30"
    assert net_income["source_field"] == "net_income"
    assert valuation_headers["planner_target"] == "B6:M6"
    assert valuation_headers["source_field"] == "period"
    assert valuation_headers["period_axis_id"] == "valuation_quarterly_periods"
    assert valuation_headers["period_axis_role"] == "header"
    for binding_id in (
        "valuation_revenue_series",
        "valuation_ebitda_series",
        "valuation_net_income_series",
        "valuation_operating_cash_flow_series",
    ):
        assert entries[binding_id]["period_axis_id"] == "valuation_quarterly_periods"
        assert entries[binding_id]["period_axis_role"] == "dependent"
    assert valuation_outputs["normalized_field"].startswith("valuation_outputs")

    for entry in entries.values():
        if entry["normalized_field"].startswith(("mapping_gaps", "manual_review_flags")):
            assert entry["sheet"] in {"QA_Log", "Needs_Review", "QA_Checks"}
        if entry["sheet"] == "Valuation" and "output" in entry["section"].lower():
            assert not entry["normalized_field"].startswith("mapping_gaps")


def test_no_direct_range_dump_or_merged_value_concatenation_remains() -> None:
    planner = (ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py").read_text(encoding="utf-8")
    filler = (ROOT / "pbi_xbrl" / "new_ticker_value_filler.py").read_text(encoding="utf-8")

    assert "zip(" not in planner
    assert "zip(" not in filler
    assert '" | ".join' not in planner
    assert '" | ".join' not in filler


def test_generic_planner_contracts_contain_no_anf_company_labels() -> None:
    generic_contract_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            ROOT / "docs" / "workbook_binding_map.json",
            ROOT / "docs" / "standard_template_shell_manifest.json",
            ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py",
            ROOT / "pbi_xbrl" / "new_ticker_evidence.py",
        )
    ).lower()

    assert "a&f" not in generic_contract_text
    assert "abercrombie" not in generic_contract_text
    assert "hollister" not in generic_contract_text
