from __future__ import annotations

import json
from pathlib import Path

from openpyxl import load_workbook

from scripts.fill_anf_shadow_workbook import run_anf_shadow_workbook_fill
from scripts.validate_standard_template_shell import validate_shell


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = next(
    ancestor / "StockModelData"
    for ancestor in [ROOT, *ROOT.parents]
    if (ancestor / "StockModelData").exists()
)
PACKAGE = DATA_ROOT / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_normalized_data_package.json"
LEGACY_ANF = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"


def test_anf_shadow_fill_creates_useful_workbook_and_reports(tmp_path: Path) -> None:
    output_dir = tmp_path / "ANF_new_ticker_engine"

    paths = run_anf_shadow_workbook_fill(
        package_path=PACKAGE,
        output_dir=output_dir,
        legacy_workbook_path=LEGACY_ANF,
    )

    expected = {
        "workbook",
        "prefill_json",
        "prefill_txt",
        "postfill_json",
        "postfill_txt",
        "comparison_json",
        "comparison_txt",
    }
    assert expected <= set(paths)
    for key in expected:
        assert paths[key].exists(), key
    assert paths["workbook"].name == "ANF_shadow_model.xlsx"
    assert not (output_dir / "ANF_model.xlsx").exists()

    prefill = json.loads(paths["prefill_json"].read_text(encoding="utf-8"))
    postfill = json.loads(paths["postfill_json"].read_text(encoding="utf-8"))
    comparison = json.loads(paths["comparison_json"].read_text(encoding="utf-8"))
    shell_report = validate_shell(template_path=paths["workbook"], allow_filled_values=True)

    assert prefill["minimum_usefulness"]["status"] == "PASS"
    assert prefill["visible_rows_available"]["quarterly_financial_rows"] >= 8
    assert prefill["visible_rows_available"]["annual_financial_rows"] >= 3
    assert prefill["visible_rows_available"]["guidance_rows"] >= 5
    assert prefill["visible_rows_available"]["segment_rows"] >= 5
    assert prefill["visible_rows_available"]["operating_driver_visible_rows"] >= 5
    assert prefill["visible_rows_available"]["quarter_note_visible_rows"] >= 8
    assert prefill["demoted_rows"]["total_demoted"] > 0

    sheet_rows = postfill["visible_usefulness_by_sheet"]
    assert sheet_rows["SUMMARY"]["written_cell_count"] > 0
    assert sheet_rows["Valuation"]["written_cell_count"] > 0
    assert sheet_rows["BS_Segments"]["written_cell_count"] > 0
    assert sheet_rows["Operating_Drivers"]["written_row_count"] >= 5
    assert sheet_rows["Quarter_Notes_UI"]["written_row_count"] >= 8
    assert sheet_rows["Promise_Progress_UI"]["written_row_count"] >= 5
    assert sheet_rows["QA_Log"]["manual_review_rows_rendered"] >= prefill["manual_review_flags"]["total_count"]
    assert postfill["layout_signature_unchanged"] is True
    assert postfill["formulas_unchanged"] is True
    assert postfill["non_writable_cells_unchanged"] is True
    assert shell_report["status"] == "PASS", shell_report["issues"][:10]

    assert comparison["summary"]["blocks_compared"] > 0
    assert comparison["summary"]["shadow_populated_blocks"] > 0
    assert comparison["top_binding_gaps_to_fix_next"] == []

    wb = load_workbook(paths["workbook"], data_only=False, read_only=False)
    try:
        assert "ANF_Investment_Case" in wb.sheetnames
        assert wb["Operating_Drivers"]["B6"].value
        assert wb["Quarter_Notes_UI"]["C9"].value
        assert wb["Promise_Progress_UI"]["B13"].value
    finally:
        wb.close()
