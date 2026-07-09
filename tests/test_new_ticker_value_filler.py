from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import range_boundaries

from pbi_xbrl.new_ticker_value_filler import (
    BindingContractError,
    NormalizedDataValidationError,
    fill_standard_template_from_package,
)
from scripts.validate_standard_template_shell import validate_shell


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "templates" / "standard_stock_model_template.xlsx"
MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"
PACKAGE = ROOT / "tests" / "fixtures" / "normalized_packages" / "TEST_minimal_valid.json"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _resolved_sheet(sheet_name: str, ticker: str = "TEST") -> str:
    return sheet_name.replace("{ticker}", ticker)


def _coord_in_ranges(coord: str, ranges: list[str]) -> bool:
    row = int("".join(ch for ch in coord if ch.isdigit()) or "0")
    col_letters = "".join(ch for ch in coord if ch.isalpha())
    col = 0
    for ch in col_letters:
        col = col * 26 + (ord(ch.upper()) - ord("A") + 1)
    for range_ref in ranges:
        min_col, min_row, max_col, max_row = range_boundaries(range_ref)
        if min_col <= col <= max_col and min_row <= row <= max_row:
            return True
    return False


def _writable_ranges_by_sheet() -> dict[str, list[str]]:
    manifest = _load_json(MANIFEST)
    return {
        _resolved_sheet(sheet["sheet"]): [zone["target"] for zone in sheet["writable_zones"]]
        for sheet in manifest["sheets"]
    }


def _sheet_order_for_ticker(ticker: str = "TEST") -> list[str]:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        return [_resolved_sheet(name, ticker) for name in wb.sheetnames]
    finally:
        wb.close()


def _non_writable_value_diffs(output: Path) -> list[str]:
    writable_ranges = _writable_ranges_by_sheet()
    before = load_workbook(TEMPLATE, data_only=False, read_only=False)
    after = load_workbook(output, data_only=False, read_only=False)
    try:
        diffs: list[str] = []
        for template_ws in before.worksheets:
            sheet_name = _resolved_sheet(template_ws.title)
            output_ws = after[sheet_name]
            ranges = writable_ranges.get(sheet_name, [])
            max_row = max(template_ws.max_row, output_ws.max_row)
            max_col = max(template_ws.max_column, output_ws.max_column)
            for row_idx in range(1, max_row + 1):
                for col_idx in range(1, max_col + 1):
                    before_cell = template_ws.cell(row_idx, col_idx)
                    after_cell = output_ws.cell(row_idx, col_idx)
                    if isinstance(before_cell, MergedCell) or isinstance(after_cell, MergedCell):
                        continue
                    if _coord_in_ranges(before_cell.coordinate, ranges):
                        continue
                    if before_cell.value != after_cell.value:
                        diffs.append(
                            f"{sheet_name}!{before_cell.coordinate}: {before_cell.value!r} -> {after_cell.value!r}"
                        )
        return diffs
    finally:
        before.close()
        after.close()


def _formula_map(path: Path) -> dict[tuple[str, str], str]:
    wb = load_workbook(path, data_only=False, read_only=False)
    try:
        formulas: dict[tuple[str, str], str] = {}
        for ws in wb.worksheets:
            sheet_name = _resolved_sheet(ws.title)
            for row in ws.iter_rows():
                for cell in row:
                    if isinstance(cell, MergedCell):
                        continue
                    if isinstance(cell.value, str) and cell.value.startswith("="):
                        formulas[(sheet_name, cell.coordinate)] = cell.value
        return formulas
    finally:
        wb.close()


def _layout_signature(path: Path) -> dict[str, Any]:
    wb = load_workbook(path, data_only=False, read_only=False)
    try:
        signature: dict[str, Any] = {"sheet_order": [_resolved_sheet(name) for name in wb.sheetnames], "sheets": {}}
        for ws in wb.worksheets:
            sheet_name = _resolved_sheet(ws.title)
            signature["sheets"][sheet_name] = {
                "state": ws.sheet_state,
                "freeze_panes": ws.freeze_panes,
                "merges": sorted(str(item) for item in ws.merged_cells.ranges),
                "row_heights": {
                    str(idx): dim.height
                    for idx, dim in ws.row_dimensions.items()
                    if dim.height is not None
                },
                "column_widths": {
                    key: dim.width
                    for key, dim in ws.column_dimensions.items()
                    if dim.width is not None
                },
                "hidden_columns": {
                    key: bool(dim.hidden)
                    for key, dim in ws.column_dimensions.items()
                    if dim.hidden
                },
            }
        return signature
    finally:
        wb.close()


def test_value_filler_creates_test_workbook_from_frozen_shell_and_writes_values_only(tmp_path: Path) -> None:
    output = tmp_path / "TEST_model.xlsx"

    result = fill_standard_template_from_package(PACKAGE, output_path=output)

    assert output.exists()
    assert output.suffix == ".xlsx"
    assert result.ticker == "TEST"
    assert result.validation_issue_count == 0
    assert result.mapping_gap_count >= 1

    wb = load_workbook(output, data_only=False, read_only=False)
    try:
        assert wb.sheetnames == _sheet_order_for_ticker("TEST")
        assert "{ticker}_Investment_Case" not in wb.sheetnames
        assert "TEST_Investment_Case" in wb.sheetnames
        assert wb["SUMMARY"]["A3"].value == "Test Systems provides workflow software and recurring services for asset-heavy customers."
        assert wb["SUMMARY"]["B30"].value == 18.2
        assert wb["SUMMARY"]["B41"].value == 70.0
        assert wb["Valuation"]["B6"].value == 120.5
        assert wb["BS_Segments"]["B7"].value == 80.0
        assert "Renewal rates remain stable while implementation backlog is converting on schedule." in wb["Operating_Drivers"]["B6"].value
        assert wb["TEST_Investment_Case"]["B5"].value.startswith("The test case depends on durable recurring revenue")
    finally:
        wb.close()

    assert _non_writable_value_diffs(output) == []


def test_table_row_bindings_use_row_schema_target_columns(tmp_path: Path) -> None:
    package = deepcopy(_load_json(PACKAGE))
    source_ref = "synthetic_fixture:row_schema"
    package["normalized_guidance"]["items"][0].update(
        {
            "initial_guide": {"value": "Initial guide", "status": "populated", "source_ref": source_ref, "core": False},
            "q1_update": {"value": "Q1 update", "status": "populated", "source_ref": source_ref, "core": False},
            "actual": {"value": "Actual result", "status": "populated", "source_ref": source_ref, "core": False},
            "progress_status": "Open",
            "notes_source": "Source-backed note",
        }
    )
    package["quarter_notes"]["items"][0].update(
        {
            "theme": {"value": "Demand", "status": "populated", "source_ref": source_ref, "core": False},
            "quarter": {"value": "2026-Q1", "status": "populated", "source_ref": source_ref, "core": False},
            "metric": {"value": "Revenue", "status": "populated", "source_ref": source_ref, "core": False},
            "commentary": {"value": "Quarter commentary remained useful.", "status": "populated", "source_ref": source_ref, "core": False},
            "source": source_ref,
        }
    )
    package["operating_drivers"]["items"][0].update(
        {
            "topic": {"value": "Demand", "status": "populated", "source_ref": source_ref, "core": False},
            "source": source_ref,
            "why_it_matters": {
                "value": "Retention drives recurring revenue.",
                "status": "populated",
                "source_ref": source_ref,
                "core": False,
            },
        }
    )
    package_path = _save_json(tmp_path / "TEST_row_schema.json", package)
    output = tmp_path / "TEST_row_schema.xlsx"

    fill_standard_template_from_package(package_path, output_path=output)

    wb = load_workbook(output, data_only=False, read_only=False)
    try:
        assert "topic: Demand" in wb["Operating_Drivers"]["B6"].value
        assert "current_read: Renewal rates remain stable while implementation backlog is converting on schedule." in wb["Operating_Drivers"]["B6"].value
        assert "source: synthetic_fixture:row_schema" in wb["Operating_Drivers"]["H6"].value
        assert "why_it_matters: Retention drives recurring revenue." in wb["Operating_Drivers"]["H6"].value
        assert "quarter: 2026-Q1" in wb["Quarter_Notes_UI"]["C9"].value
        assert "metric: Revenue" in wb["Quarter_Notes_UI"]["C9"].value
        assert "commentary: Quarter commentary remained useful." in wb["Quarter_Notes_UI"]["C9"].value
        assert wb["Quarter_Notes_UI"]["H9"].value == "Growth durability remains the main valuation sensitivity."
        assert wb["Quarter_Notes_UI"]["M9"].value == source_ref
        assert wb["Promise_Progress_UI"]["B13"].value == "Revenue"
        assert wb["Promise_Progress_UI"]["C13"].value == "Initial guide"
        assert wb["Promise_Progress_UI"]["D13"].value == "Q1 update"
        assert wb["Promise_Progress_UI"]["H13"].value == "Actual result"
        assert "status: Open" in wb["Promise_Progress_UI"]["I13"].value
        assert "notes_source: Source-backed note" in wb["Promise_Progress_UI"]["I13"].value
    finally:
        wb.close()


def test_value_filler_preserves_formulas_and_layout_contract(tmp_path: Path) -> None:
    output = tmp_path / "TEST_model.xlsx"

    fill_standard_template_from_package(PACKAGE, output_path=output)

    assert _formula_map(output) == _formula_map(TEMPLATE)
    assert _layout_signature(output) == _layout_signature(TEMPLATE)


def test_value_filler_rejects_binding_that_targets_non_writable_zone(tmp_path: Path) -> None:
    bad_binding_map = deepcopy(_load_json(BINDING_MAP))
    bad_binding_map["bindings"][0]["target"] = "A1:A1"
    bad_binding_map["bindings"][0]["shell_zone"] = "summary_company_description_value"
    bad_path = _save_json(tmp_path / "bad_binding_map.json", bad_binding_map)
    output = tmp_path / "bad.xlsx"

    with pytest.raises(BindingContractError):
        fill_standard_template_from_package(PACKAGE, output_path=output, binding_map_path=bad_path)

    assert not output.exists()


def test_missing_required_data_writes_mapping_gaps_and_manual_review_rows(tmp_path: Path) -> None:
    package = deepcopy(_load_json(PACKAGE))
    package["company_profile"]["revenue_model"] = {
        "value": None,
        "status": "missing_source",
        "source_ref": "",
        "core": True,
        "reason": "Synthetic omission to exercise mapping-gap output."
    }
    package_path = _save_json(tmp_path / "TEST_missing_required.json", package)
    output = tmp_path / "TEST_missing_required.xlsx"

    result = fill_standard_template_from_package(package_path, output_path=output)

    assert result.mapping_gap_count >= 2
    wb = load_workbook(output, data_only=False, read_only=False)
    try:
        qa_values = [wb["QA_Checks"].cell(row, col).value for row in range(2, 20) for col in range(1, 12)]
        needs_values = [wb["Needs_Review"].cell(row, col).value for row in range(2, 20) for col in range(1, 12)]
        assert "company_profile.revenue_model" in qa_values
        assert "company_profile.revenue_model" in needs_values
        assert wb["SUMMARY"]["A9"].value in (None, "")
    finally:
        wb.close()


def test_p1_normalized_validation_issue_fails_before_render(tmp_path: Path) -> None:
    package = deepcopy(_load_json(PACKAGE))
    package["normalized_guidance"]["items"][0]["value"]["value"] = "Adjusted EBITDA expected to improve by $10m."
    bad_package = _save_json(tmp_path / "TEST_p1_invalid.json", package)
    output = tmp_path / "TEST_p1_invalid.xlsx"

    with pytest.raises(NormalizedDataValidationError) as excinfo:
        fill_standard_template_from_package(bad_package, output_path=output)

    assert not output.exists()
    assert "guidance_metric_misclassification" in str(excinfo.value)


def test_filled_test_workbook_shell_validation_passes_in_filled_mode(tmp_path: Path) -> None:
    output = tmp_path / "TEST_model.xlsx"

    fill_standard_template_from_package(PACKAGE, output_path=output)
    report = validate_shell(template_path=output, allow_filled_values=True)

    assert report["status"] == "PASS", report
    assert report["issue_count"] == 0


def test_value_filler_does_not_create_gtx_workbook_or_change_default_validation_tickers(tmp_path: Path) -> None:
    fill_standard_template_from_package(PACKAGE, output_path=tmp_path / "TEST_model.xlsx")

    data_root = ROOT.parent.parent / "StockModelData"
    forbidden = [
        data_root / "outputs" / "Excel stock models" / "GTX_model.xlsx",
        data_root / "outputs" / "Excel stock models" / "GTX_model.xlsm",
        data_root / "outputs" / "stress_tests" / "GTX_new_ticker_engine" / "GTX_model.xlsx",
    ]
    assert [str(path) for path in forbidden if path.exists()] == []

    validation_runner = (ROOT / "pbi_xbrl" / "workbook_validation_runner.py").read_text(encoding="utf-8")
    assert 'TICKERS: Sequence[str] = ("PBI", "GPRE", "ANF")' in validation_runner
