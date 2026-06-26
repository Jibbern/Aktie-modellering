from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any

import openpyxl
import pytest


DEFAULT_GTX_STAGED_WORKBOOK = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\staging\GTX\GTX_model.xlsx"
)
GTX_SPECIAL_EVENT_MARKERS = (
    "eh260781731_ex9902",
    "May 18, 2026",
    "repayment/repricing",
    "debt repayment",
)
GPRE_ONLY_TERMS = (
    "45Z",
    "RVO",
    "E15",
    "Crush margin",
    "crush spread",
    "ethanol",
    "RIN",
)


def _gtx_workbook_path() -> Path:
    return Path(os.environ.get("GTX_STAGED_WORKBOOK", DEFAULT_GTX_STAGED_WORKBOOK))


@pytest.fixture()
def gtx_wb() -> openpyxl.Workbook:
    path = _gtx_workbook_path()
    if not path.exists():
        pytest.skip(f"GTX staged workbook not found: {path}")
    return openpyxl.load_workbook(path, data_only=False, read_only=True)


def _sheet_text(wb: openpyxl.Workbook, sheet_name: str) -> str:
    ws = wb[sheet_name]
    return "\n".join(
        str(cell.value)
        for row in ws.iter_rows()
        for cell in row
        if cell.value is not None
    )


def _sheet_visible_text(wb: openpyxl.Workbook, sheet_name: str) -> str:
    ws = wb[sheet_name]
    return "\n".join(
        str(cell.value)
        for row in ws.iter_rows()
        for cell in row
        if cell.value is not None and not str(cell.value).startswith("=")
    )


def _history_row(wb: openpyxl.Workbook, quarter: dt.date) -> tuple[list[Any], tuple[Any, ...]]:
    ws = wb["History_Q"]
    rows = list(ws.iter_rows(values_only=True))
    headers = list(rows[0])
    for row in rows[1:]:
        row_q = row[headers.index("quarter")] if "quarter" in headers else None
        if getattr(row_q, "date", lambda: row_q)() == quarter:
            return headers, row
        if str(row_q).startswith(quarter.isoformat()):
            return headers, row
    raise AssertionError(f"History_Q missing quarter {quarter.isoformat()}")


def _history_value(wb: openpyxl.Workbook, quarter: dt.date, column: str) -> Any:
    headers, row = _history_row(wb, quarter)
    assert column in headers, f"History_Q missing column {column}"
    return row[headers.index(column)]


def test_gtx_staged_workbook_includes_operating_drivers(gtx_wb: openpyxl.Workbook) -> None:
    assert "Operating_Drivers" in gtx_wb.sheetnames


def test_gtx_investment_case_has_no_gpre_only_terms(gtx_wb: openpyxl.Workbook) -> None:
    assert "GTX_Investment_Case" in gtx_wb.sheetnames
    text = _sheet_visible_text(gtx_wb, "GTX_Investment_Case").lower()

    leaked = [term for term in GPRE_ONLY_TERMS if term.lower() in text]

    assert leaked == []


def test_gtx_history_q4_2025_operating_income_and_diluted_shares(
    gtx_wb: openpyxl.Workbook,
) -> None:
    q4_2025 = dt.date(2025, 12, 31)

    assert _history_value(gtx_wb, q4_2025, "op_income") == pytest.approx(
        119_000_000.0,
        abs=1_000_000.0,
    )
    assert _history_value(gtx_wb, q4_2025, "shares_diluted") == pytest.approx(
        197_514_000.0,
        abs=1_000_000.0,
    )


def test_gtx_q1_2026_fcf_qa_excludes_may_18_special_event_source(
    gtx_wb: openpyxl.Workbook,
) -> None:
    assert _history_value(gtx_wb, dt.date(2026, 3, 31), "cfo") == pytest.approx(
        98_000_000.0,
        abs=1_000_000.0,
    )
    assert _history_value(gtx_wb, dt.date(2026, 3, 31), "capex") == pytest.approx(
        29_000_000.0,
        abs=1_000_000.0,
    )

    qa_text = _sheet_text(gtx_wb, "QA_Log")
    fcf_qa_lines = [
        line
        for line in qa_text.splitlines()
        if "FCF (Q)" in line or "free cash flow" in line.lower()
    ]
    contaminated = [
        line
        for line in fcf_qa_lines
        if any(marker.lower() in line.lower() for marker in GTX_SPECIAL_EVENT_MARKERS)
    ]

    assert contaminated == []
    assert not any("extracted quarter text=$4.0m" in line for line in fcf_qa_lines)


def test_gtx_may_18_debt_event_does_not_contaminate_reported_history_or_qa(
    gtx_wb: openpyxl.Workbook,
) -> None:
    reported_surfaces = ("History_Q", "Adjusted_Metrics", "QA_Log")
    contaminated: list[tuple[str, str]] = []
    for sheet_name in reported_surfaces:
        if sheet_name not in gtx_wb.sheetnames:
            continue
        text = _sheet_text(gtx_wb, sheet_name)
        for marker in GTX_SPECIAL_EVENT_MARKERS:
            if marker.lower() in text.lower():
                contaminated.append((sheet_name, marker))

    assert contaminated == []


def test_gtx_debt_tranches_latest_omits_spurious_tiny_other_row(gtx_wb: openpyxl.Workbook) -> None:
    assert "Debt_Tranches_Latest" in gtx_wb.sheetnames
    ws = gtx_wb["Debt_Tranches_Latest"]
    rows = list(ws.iter_rows(values_only=True))
    headers = list(rows[0])
    name_idx = headers.index("tranche_name")
    amount_idx = headers.index("amount_principal")

    tiny_other_rows = [
        row
        for row in rows[1:]
        if str(row[name_idx] or "").strip().lower() == "other"
        and float(row[amount_idx] or 0.0) < 5_000_000.0
    ]

    assert tiny_other_rows == []
