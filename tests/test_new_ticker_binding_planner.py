from __future__ import annotations

import ast
from copy import deepcopy
import json
from pathlib import Path

from pbi_xbrl.new_ticker_binding_planner import BindingPlan, plan_standard_template_writes
from pbi_xbrl.normalized_company_data_validation import (
    validate_normalized_company_data,
    validate_normalized_company_data_schema,
)


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_PATH = ROOT / "tests" / "fixtures" / "normalized_packages" / "TEST_minimal_valid.json"
BINDING_MAP_PATH = ROOT / "docs" / "workbook_binding_map.json"
MANIFEST_PATH = ROOT / "docs" / "standard_template_shell_manifest.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _package() -> dict:
    return _load(PACKAGE_PATH)


def _bindings() -> dict:
    return _load(BINDING_MAP_PATH)


def _manifest() -> dict:
    return _load(MANIFEST_PATH)


def _plan(package: dict | None = None, *, bindings: dict | None = None, manifest: dict | None = None) -> BindingPlan:
    return plan_standard_template_writes(
        package or _package(),
        binding_payload=bindings or _bindings(),
        manifest=manifest or _manifest(),
    )


def _writes(plan: BindingPlan) -> dict[tuple[str, str], object]:
    return {(write.target_sheet, write.target_cell): write for write in plan.planned_writes}


def _binding(payload: dict, binding_id: str) -> dict:
    return next(item for item in payload["bindings"] if item["binding_id"] == binding_id)


def _rule_ids(plan: BindingPlan) -> set[str]:
    return {issue.rule_id for issue in plan.issues}


def test_planner_is_json_only_and_does_not_import_excel_io() -> None:
    source = (ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(module)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom)
    }

    assert not any(name.startswith("openpyxl") for name in imported_modules)
    assert "load_workbook(" not in source


def test_real_contract_plans_seven_business_flows_to_exact_cells() -> None:
    plan = _plan()

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    writes = _writes(plan)

    # Quarterly financials: period headers and metrics are distinct contracts.
    assert writes[("SUMMARY", "B26")].value == "2026-Q1"
    assert writes[("SUMMARY", "B28")].value == 120.5
    assert writes[("SUMMARY", "B30")].value == 13.1
    assert writes[("Valuation", "B6")].value == "2025-Q4"
    assert writes[("Valuation", "B9")].value == 118.0
    assert writes[("Valuation", "B9")].row_key == "2025-Q4"

    # Annual financials use their own period and value rows.
    assert writes[("BS_Segments", "B70")].value == "2025-FY"
    assert writes[("BS_Segments", "B71")].value == 470.0
    assert writes[("BS_Segments", "B71")].row_key == "2025-FY"

    # Guidance uses merge anchors only and a separate status cell.
    guidance_key = "Revenue|FY2026|2026-07-07|test-guidance-2026q1"
    assert writes[("Valuation", "O9")].value == "Revenue"
    assert writes[("Valuation", "Q9")].value == "2026-Q1"
    assert writes[("Valuation", "R9")].value == "FY2026"
    assert writes[("Valuation", "S9")].value.startswith("Revenue growth")
    assert writes[("Valuation", "AA9")].value == "open"
    assert writes[("Valuation", "S9")].row_key == guidance_key
    assert ("Valuation", "T9") not in writes

    # Segment rows are a member-by-period pivot, not a sequential row dump.
    assert writes[("BS_Segments", "A66")].value == "Workflow software"
    assert writes[("BS_Segments", "I66")].value == 92.0
    assert writes[("BS_Segments", "B72")].value == 360.0
    assert writes[("BS_Segments", "I66")].row_key == "2026-Q1|business_line|Workflow software|revenue"

    # Operating driver fields stay in the declared A/B/H cells.
    driver_key = "Demand|2026-Q1|operational|Retention and implementation velocity|test-driver-2026q1"
    assert writes[("Operating_Drivers", "A6")].value == "Demand"
    assert writes[("Operating_Drivers", "B6")].value.startswith("Renewal rates")
    assert writes[("Operating_Drivers", "H6")].value.startswith("Retention and implementation")
    assert writes[("Operating_Drivers", "B6")].row_key == driver_key
    assert writes[("Operating_Drivers", "B6")].source_ref == "synthetic_fixture:operating_drivers"

    # Quarter note commentary, implication, and source remain separate.
    note_key = "2026-Q1|Demand|Revenue|test-quarter-note-2026q1"
    assert writes[("Quarter_Notes_UI", "B3")].value.startswith("Recurring software demand")
    assert writes[("Quarter_Notes_UI", "A10")].value == "Demand"
    assert writes[("Quarter_Notes_UI", "C10")].value.startswith("Recurring software demand")
    assert writes[("Quarter_Notes_UI", "H10")].value.startswith("Growth durability")
    assert writes[("Quarter_Notes_UI", "M10")].value == "synthetic_fixture:quarter_notes"
    assert writes[("Quarter_Notes_UI", "C10")].row_key == note_key
    assert not any(sheet == "Quarter_Notes_UI" and cell.endswith("9") for sheet, cell in writes)

    # Promise Progress follows the real A:K header semantics.
    assert writes[("Promise_Progress_UI", "A61")].value == "Revenue"
    assert writes[("Promise_Progress_UI", "C61")].value.startswith("Revenue growth")
    assert ("Promise_Progress_UI", "B61") not in writes
    assert writes[("Promise_Progress_UI", "G61")].value == "open"
    assert writes[("Promise_Progress_UI", "H61")].value == "FY2026"
    assert writes[("Promise_Progress_UI", "I61")].value == "2026-Q1"
    assert writes[("Promise_Progress_UI", "J61")].value == "2026-07-07"

    # Investment-case fields target the actual B5/B7 merge anchors.
    assert writes[("TEST_Investment_Case", "B5")].value.startswith("The test case depends")
    assert writes[("TEST_Investment_Case", "B7")].value.startswith("Whether retention")

    # Liquidity and valuation inputs have exact business semantics.
    assert ("SUMMARY", "B41") not in writes
    assert writes[("SUMMARY", "B45")].value == 180.0
    assert writes[("Valuation", "D195")].value == "2026-03-31"
    assert writes[("Valuation", "D196")].value == 42.0
    assert writes[("Valuation", "D197")].value == 42.3
    assert writes[("Valuation", "D198")].value == 70.0
    assert writes[("Valuation", "D200")].value == 101.4
    assert writes[("Valuation", "D203")].value == 470.8

    assert len(writes) == len(plan.planned_writes)
    assert all(write.binding_id and write.normalized_path and write.row_key for write in plan.planned_writes)
    assert all(write.target_role and write.target_type for write in plan.planned_writes)
    assert not any(issue.severity in {"P0", "P1"} for issue in plan.issues)


def test_required_or_blocked_if_missing_binding_creates_p1_and_fails_plan() -> None:
    package = _package()
    field = package["company_profile"]["business_description"]
    field.update(value=None, status="missing_source", reason="Synthetic source intentionally removed.")

    plan = _plan(package)

    assert plan.status == "FAIL"
    assert "required_binding_missing" in _rule_ids(plan)
    gap = next(item for item in plan.mapping_gaps if item["binding_id"] == "summary_company_description")
    assert gap["severity"] == "P1"
    assert gap["normalized_path"] == "company_profile.business_description"
    assert gap["row_key"] == "scalar"
    assert gap["expected_target"] == "A3"


def test_manifest_rejects_guidance_write_to_merge_non_anchor_t9() -> None:
    bindings = _bindings()
    guidance = _binding(bindings, "valuation_guidance_rows")
    value_column = next(column for column in guidance["target_columns"] if column["source_field"] == "value")
    value_column["target_column"] = "T"

    plan = _plan(bindings=bindings)

    assert plan.status == "FAIL"
    assert "manifest_merge_non_anchor_target" in _rule_ids(plan)
    assert "manifest_exact_writable_cell_missing" in _rule_ids(plan)
    assert plan.planned_writes == []


def test_required_annual_overflow_is_explicit_and_fails() -> None:
    package = _package()
    seed = package["annual_financials"]["rows"][0]
    rows = []
    for year in range(2017, 2026):
        row = deepcopy(seed)
        row["period"] = f"{year}-FY"
        row["fiscal_year"] = year
        for name, value in row.items():
            if isinstance(value, dict) and "status" in value:
                value["period"] = f"{year}-FY"
                value["source_ref"] = f"synthetic_fixture:annual_financials:{year}"
        rows.append(row)
    package["annual_financials"]["rows"] = rows

    plan = _plan(package)
    report = next(item for item in plan.binding_reports if item["binding_id"] == "bs_annual_revenue_series")

    assert plan.status == "FAIL"
    assert "binding_overflow" in _rule_ids(plan)
    assert len(report["overflow_rows"]) == 1
    overflow = report["overflow_rows"][0]
    assert {"binding_id", "row_key", "normalized_path", "source_ref", "reason", "severity"} <= set(overflow)
    assert overflow["row_key"] == "2025-FY"
    assert overflow["severity"] == "P1"
    assert any(gap["row_key"] == "2025-FY" for gap in plan.mapping_gaps)


def test_selected_row_with_missing_required_value_is_not_silently_skipped() -> None:
    package = _package()
    revenue = package["annual_financials"]["rows"][0]["revenue"]
    revenue.update(value=None, status="missing_source", reason="Revenue evidence intentionally removed.")

    plan = _plan(package)
    report = next(item for item in plan.binding_reports if item["binding_id"] == "bs_annual_revenue_series")

    assert plan.status == "FAIL"
    assert "required_row_value_missing" in _rule_ids(plan)
    skip = next(item for item in report["skipped_rows"] if item["row_key"] == "2025-FY")
    assert skip["severity"] == "P1"
    assert skip["normalized_path"].endswith(".revenue")
    assert skip["expected_target"] == "B71"
    assert any(gap["row_key"] == "2025-FY" and gap["severity"] == "P1" for gap in plan.mapping_gaps)


def test_schema_and_semantics_reject_units_dimensions_and_core_lineage() -> None:
    bad_unit = _package()
    bad_unit["quarterly_financials"]["rows"][0]["revenue"]["unit"] = "bananas"
    assert "normalized_schema_enum" in {issue.rule_id for issue in validate_normalized_company_data_schema(bad_unit)}
    assert "invalid_unit" in {issue.rule_id for issue in validate_normalized_company_data(bad_unit)}

    bad_dimension = _package()
    bad_dimension["segments"]["items"][0]["dimension"] = "planet"
    assert "normalized_schema_enum" in {issue.rule_id for issue in validate_normalized_company_data_schema(bad_dimension)}
    assert "invalid_dimension" in {issue.rule_id for issue in validate_normalized_company_data(bad_dimension)}

    bad_lineage = _package()
    field = bad_lineage["annual_financials"]["rows"][0]["revenue"]
    field["core"] = True
    field["source_ref"] = ""
    assert "missing_source_ref" in {issue.rule_id for issue in validate_normalized_company_data(bad_lineage)}


def test_target_type_mismatch_and_invalid_sort_key_fail_before_writes() -> None:
    bindings = _bindings()
    _binding(bindings, "summary_latest_revenue")["target_type"] = "text"
    type_plan = _plan(bindings=bindings)
    assert type_plan.status == "FAIL"
    assert "manifest_target_type_mismatch" in _rule_ids(type_plan)
    assert type_plan.planned_writes == []

    bindings = _bindings()
    _binding(bindings, "qn_quarter_summary_rows")["sort_order"] = ["missing_business_sort_key:asc"]
    sort_plan = _plan(bindings=bindings)
    assert sort_plan.status == "FAIL"
    assert "binding_sort_key_missing" in _rule_ids(sort_plan)


def test_schema_is_enforced_before_planning_and_required_row_columns_are_real() -> None:
    missing_section = _package()
    missing_section.pop("annual_financials")
    plan = _plan(missing_section)
    assert plan.status == "FAIL"
    assert any(issue.rule_id == "normalized_schema_required" for issue in plan.schema_issues)
    assert plan.planned_writes == []

    missing_row_column = _package()
    missing_row_column["quarter_notes"]["items"][0].pop("commentary")
    plan = _plan(missing_row_column)
    assert plan.status == "FAIL"
    assert any(issue.rule_id == "normalized_schema_required" for issue in plan.schema_issues)
    assert plan.planned_writes == []


def test_planner_source_has_no_sequential_dump_or_silent_capacity_slice() -> None:
    source = (ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py").read_text(encoding="utf-8")

    assert "zip(" not in source
    assert "rows[:capacity]" not in source
    assert '" | ".join' not in source
    assert "for source_position, row in enumerate(rows):" in source


def test_current_guidance_rowset_excludes_history_with_an_explicit_reason() -> None:
    package = _package()
    historical = deepcopy(package["normalized_guidance"]["items"][0])
    historical["source_date"] = "2025-03-01"
    historical["stated_in_period"] = "2024-Q4"
    historical["horizon"]["value"] = "FY2025"
    historical["evidence_key"] = "test-guidance-history"
    historical["display_role"] = "history"
    historical["display_priority"] = 99
    package["normalized_guidance"]["items"].append(historical)

    plan = _plan(package)

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    assert not any(write.row_key.endswith("test-guidance-history") for write in plan.planned_writes)
    reports = [report for report in plan.binding_reports if report["binding_id"] in {"valuation_guidance_rows", "pp_guidance_timeline_rows"}]
    assert reports
    assert all(any(skip["row_key"].endswith("test-guidance-history") and skip["reason"].startswith("row_selector_excluded") for skip in report["skipped_rows"]) for report in reports)


def test_future_quarter_note_is_audit_only_and_never_planned_visible() -> None:
    package = _package()
    future = deepcopy(package["quarter_notes"]["items"][0])
    future["quarter"]["value"] = "2027-Q1"
    future["quarter"]["period"] = "2027-Q1"
    future["evidence_key"] = "test-quarter-note-future"
    future["display_role"] = "audit_only"
    package["quarter_notes"]["items"].append(future)

    plan = _plan(package)

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    assert not any(write.row_key.endswith("test-quarter-note-future") for write in plan.planned_writes)
    report = next(item for item in plan.binding_reports if item["binding_id"] == "qn_quarter_note_rows")
    assert any(skip["row_key"].endswith("test-quarter-note-future") and skip["reason"].startswith("row_selector_excluded") for skip in report["skipped_rows"])
