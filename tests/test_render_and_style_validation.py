from __future__ import annotations

import os
from pathlib import Path

import pytest
from openpyxl import Workbook, load_workbook

from pbi_xbrl.render_validation_runner import (
    RENDER_RANGES,
    USER_FACING_STYLE_SHEETS,
    _default_workbooks,
    run_render_validation,
    validate_openpyxl_layout,
)


WORKBOOK_DIR = Path(
    os.environ.get(
        "STOCK_MODEL_WORKBOOK_DIR",
        r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models",
    )
)
TICKERS = ("PBI", "GPRE", "ANF")


def _workbook_path(ticker: str) -> Path:
    path = next(
        (WORKBOOK_DIR / f"{ticker}_model{suffix}" for suffix in (".xlsm", ".xlsx") if (WORKBOOK_DIR / f"{ticker}_model{suffix}").exists()),
        WORKBOOK_DIR / f"{ticker}_model.xlsx",
    )
    if not path.exists():
        pytest.skip(f"{path} is not available for render/style validation tests")
    return path


def test_render_ranges_cover_required_workbook_surfaces() -> None:
    expected_ranges = {
        "Valuation": "A1:AC90",
        "{ticker}_Investment_Case": "A1:J160",
        "Promise_Progress_UI": "A1:L180",
        "Quarter_Notes_UI": "A1:O220",
        "Operating_Drivers": "A1:Q140",
        "Needs_Review": "A1:J80",
    }

    assert RENDER_RANGES == expected_ranges
    assert {"Valuation", "{ticker}_Investment_Case", "Promise_Progress_UI", "Quarter_Notes_UI"}.issubset(
        set(USER_FACING_STYLE_SHEETS)
    )


def test_render_runner_prefers_generated_xlsm_workbooks(tmp_path: Path) -> None:
    (tmp_path / "PBI_model.xlsm").write_bytes(b"placeholder")
    (tmp_path / "PBI_model.xlsx").write_bytes(b"stale-placeholder")
    (tmp_path / "GPRE_model.xlsm").write_bytes(b"placeholder")

    workbooks = _default_workbooks(tmp_path)

    assert workbooks["PBI"] == tmp_path / "PBI_model.xlsm"
    assert workbooks["GPRE"] == tmp_path / "GPRE_model.xlsm"
    assert workbooks["ANF"] == tmp_path / "ANF_model.xlsx"


def test_openpyxl_style_validation_passes_current_workbooks() -> None:
    for ticker in TICKERS:
        report = validate_openpyxl_layout(_workbook_path(ticker), ticker)
        blocking = [issue for issue in report.issues if issue.severity == "error"]
        assert not blocking, f"{ticker}: openpyxl layout/style issues: {[issue.to_dict() for issue in blocking[:10]]}"
        assert report.checked_sheets >= 4
        assert report.max_row_height <= 95


def test_render_validation_skips_com_cleanly_and_still_runs_style_checks(tmp_path: Path) -> None:
    workbooks = {ticker: _workbook_path(ticker) for ticker in TICKERS}

    report = run_render_validation(
        workbooks,
        output_root=tmp_path,
        timestamp="unit",
        enable_com=False,
    )

    assert report.output_dir == tmp_path / "final_validation_unit"
    assert report.render_status == "skipped"
    assert "disabled" in report.skip_reason.lower()
    assert report.output_dir.exists()
    assert set(report.style_reports) == set(TICKERS)
    assert all(not [issue for issue in style.issues if issue.severity == "error"] for style in report.style_reports.values())
    assert report.to_summary_rows()[0]["Overall"] in {"PASS", "SKIP_RENDER"}


def test_openpyxl_layout_validator_detects_broken_user_facing_styles(tmp_path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Valuation"
    ws["A1"] = "Valuation"
    ws["A2"] = "Metric"
    ws["B2"] = "Value"
    ws["A3"] = "Unstyled body"
    for sheet_name in ["PBI_Investment_Case", "Promise_Progress_UI", "Quarter_Notes_UI", "Operating_Drivers", "Needs_Review"]:
        sheet = wb.create_sheet(sheet_name)
        sheet["A1"] = sheet_name
    path = tmp_path / "PBI_model.xlsx"
    wb.save(path)

    report = validate_openpyxl_layout(path, "PBI")

    assert any(issue.severity == "error" for issue in report.issues)
    assert any("missing title/header styling" in issue.message.lower() for issue in report.issues)
