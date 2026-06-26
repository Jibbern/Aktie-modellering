from __future__ import annotations

import datetime as dt
import os
import re
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
GTX_CROSS_COMPANY_LEAKAGE_TERMS = (
    "PBI",
    "GPRE",
    "ANF",
    "45Z",
    "RVO",
    "E15",
    "RIN",
    "USPS",
    "Abercrombie",
    "Hollister",
    "Carrier, logistics and service execution reliability",
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


def _visible_non_empty_rows(wb: openpyxl.Workbook, sheet_name: str) -> list[list[Any]]:
    ws = wb[sheet_name]
    out: list[list[Any]] = []
    for row in ws.iter_rows(values_only=True):
        values = [value for value in row if value is not None and str(value).strip()]
        if values:
            out.append(values)
    return out


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


def test_gtx_operating_drivers_has_user_facing_source_backed_watchlist(
    gtx_wb: openpyxl.Workbook,
) -> None:
    assert "Operating_Drivers" in gtx_wb.sheetnames
    rows = _visible_non_empty_rows(gtx_wb, "Operating_Drivers")
    text = _sheet_visible_text(gtx_wb, "Operating_Drivers")

    assert "No operating-driver history available" not in text
    assert len(rows) >= 10
    for needle in (
        "OEM production",
        "product mix",
        "Commercial vehicle",
        "Aftermarket",
        "China",
        "Europe",
        "customer concentration",
        "Adjusted EBIT",
        "Adjusted FCF",
    ):
        assert needle.lower() in text.lower()


def test_gtx_investment_case_has_no_gpre_only_terms(gtx_wb: openpyxl.Workbook) -> None:
    assert "GTX_Investment_Case" in gtx_wb.sheetnames
    text = _sheet_visible_text(gtx_wb, "GTX_Investment_Case").lower()

    leaked: list[str] = []
    for term in GPRE_ONLY_TERMS:
        if " " in term:
            if term.lower() in text:
                leaked.append(term)
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(term)}(?![A-Za-z0-9])", text, re.I):
            leaked.append(term)

    assert leaked == []


def test_gtx_investment_case_is_gtx_specific_not_generic(
    gtx_wb: openpyxl.Workbook,
) -> None:
    text = _sheet_visible_text(gtx_wb, "GTX_Investment_Case")
    low = text.lower()

    assert "generic fallback only" not in low
    assert "pending ticker-specific configuration" not in low
    for needle in (
        "Model read",
        "Why it can work",
        "Key debate",
        "What would improve",
        "What would break",
        "Watch next",
        "Bear",
        "Base",
        "Bull",
        "Quality of Earnings",
        "Adjusted EBIT",
        "Adjusted FCF",
        "turbo",
        "customer concentration",
    ):
        assert needle.lower() in low


def test_gtx_summary_uses_gtx_specific_revenue_and_dependency_language(
    gtx_wb: openpyxl.Workbook,
) -> None:
    text = _sheet_visible_text(gtx_wb, "SUMMARY")
    low = text.lower()

    assert "carrier, logistics and service execution reliability" not in low
    assert "operates through accounting view" not in low
    assert "operates through accounting view and product-line mix" not in low
    assert "business model / revenue streams" in low
    assert "n/a\nkey dependencies" not in low
    for needle in (
        "Gas",
        "Diesel",
        "Commercial Vehicle",
        "Aftermarket",
        "Europe",
        "China",
        "Stellantis",
        "BMW",
        "Ford",
    ):
        assert needle.lower() in low


def test_gtx_investment_case_data_has_meaningful_gtx_sections(
    gtx_wb: openpyxl.Workbook,
) -> None:
    assert "GTX_Investment_Case_Data" in gtx_wb.sheetnames
    rows = _visible_non_empty_rows(gtx_wb, "GTX_Investment_Case_Data")
    text = _sheet_visible_text(gtx_wb, "GTX_Investment_Case_Data")
    low = text.lower()

    assert len(rows) >= 25
    assert "generic fallback only" not in low
    for section in (
        "Investment Snapshot",
        "Key Debates",
        "Bear / Base / Bull Scenario",
        "Quality of Earnings",
        "Operating Driver Watchlist",
        "Product / Geography / Customer Cuts",
        "Current Guide -> Implied Earnings",
    ):
        assert section.lower() in low


def test_gtx_investment_case_data_keeps_source_backed_q1_metrics_visible(
    gtx_wb: openpyxl.Workbook,
) -> None:
    text = _sheet_visible_text(gtx_wb, "GTX_Investment_Case_Data")
    low = text.lower()

    assert "adjusted ebit $151.0m" in low or "adjusted ebit $151m" in low
    assert "adjusted ebitda $183.0m" in low or "adjusted ebitda $183m" in low
    assert "adjusted fcf $49.0m" in low or "adjusted fcf $49m" in low
    assert "restricted cash" in low
    assert "$2.0m" in low or "$2m" in low


def test_gtx_visible_text_has_no_cross_company_or_sector_leakage(
    gtx_wb: openpyxl.Workbook,
) -> None:
    visible_sheets = (
        "SUMMARY",
        "Operating_Drivers",
        "GTX_Investment_Case",
        "GTX_Investment_Case_Data",
        "Promise_Progress_UI",
    )
    text = "\n".join(_sheet_visible_text(gtx_wb, sheet) for sheet in visible_sheets if sheet in gtx_wb.sheetnames)
    leaked: list[str] = []
    for term in GTX_CROSS_COMPANY_LEAKAGE_TERMS:
        if " " in term or "," in term:
            if term.lower() in text.lower():
                leaked.append(term)
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(term)}(?![A-Za-z0-9])", text, re.I):
            leaked.append(term)

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


def test_gtx_older_q4_operating_income_is_not_impossible_vs_gross_profit(
    gtx_wb: openpyxl.Workbook,
) -> None:
    for quarter in (dt.date(2022, 12, 31), dt.date(2023, 12, 31)):
        gross_profit = _history_value(gtx_wb, quarter, "gross_profit")
        operating_income = _history_value(gtx_wb, quarter, "op_income")

        assert gross_profit is not None, f"GTX {quarter} gross profit should remain source-backed"
        assert operating_income is None or operating_income <= gross_profit, (
            f"GTX {quarter} operating income should be quarterized or blank, "
            f"not an impossible FY/YTD artifact above gross profit"
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


def test_gtx_q1_2026_fcf_qa_labels_gaap_vs_adjusted_fcf_definition_difference(
    gtx_wb: openpyxl.Workbook,
) -> None:
    qa_text = _sheet_text(gtx_wb, "QA_Log")
    fcf_qa_lines = [
        line
        for line in qa_text.splitlines()
        if "FCF (Q)" in line or "free cash flow" in line.lower()
    ]

    assert any("CFO-capex" in line and "company-defined" in line for line in fcf_qa_lines)
    assert any("definition mismatch" in line.lower() for line in fcf_qa_lines)
    assert not any("likely conflicting extraction or source mismatch" in line for line in fcf_qa_lines)


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


def test_gtx_may_2026_debt_repayment_is_labeled_post_quarter_event_context(
    gtx_wb: openpyxl.Workbook,
) -> None:
    text = _sheet_visible_text(gtx_wb, "Promise_Progress_UI")
    debt_event_lines = [
        line
        for line in text.splitlines()
        if "$50.0m disclosed" in line or "$50m" in line
    ]

    assert debt_event_lines, "Expected visible GTX debt repayment/event row"
    assert all(
        any(marker in line.lower() for marker in ("post-quarter", "event-context", "pro-forma", "pro forma"))
        for line in debt_event_lines
    )


def test_gtx_may_2026_debt_event_promise_row_is_concise(
    gtx_wb: openpyxl.Workbook,
) -> None:
    ws = gtx_wb["Promise_Progress_UI"]
    debt_rows: list[tuple[Any, ...]] = []
    for row in ws.iter_rows(values_only=True):
        row_text = " | ".join(str(value or "") for value in row)
        if "Debt reduction" in row_text and ("$50.0m" in row_text or "$50m" in row_text):
            debt_rows.append(row)

    assert debt_rows, "Expected GTX post-quarter debt-event row"
    for row in debt_rows:
        row_text = " | ".join(str(value or "") for value in row)
        assert re.search(r"\b(post[- ]quarter|event[- ]context|pro[- ]forma|pro forma)\b", row_text, re.I)
        assert len(row_text) < 420
        assert "Acquisition and divestiture expenses" not in row_text
        assert "Full year 2025 outlook" not in row_text


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


def test_gtx_debt_tranches_latest_does_not_misread_term_facility_name_as_2025_maturity(
    gtx_wb: openpyxl.Workbook,
) -> None:
    assert "Debt_Tranches_Latest" in gtx_wb.sheetnames
    ws = gtx_wb["Debt_Tranches_Latest"]
    rows = list(ws.iter_rows(values_only=True))
    headers = list(rows[0])
    name_idx = headers.index("tranche_name")
    maturity_year_idx = headers.index("maturity_year")
    maturity_display_idx = headers.index("maturity_display")

    term_rows = [
        row
        for row in rows[1:]
        if str(row[name_idx] or "").strip() == "2025 Dollar Term Facility"
    ]

    assert len(term_rows) == 1
    term_row = term_rows[0]
    assert term_row[maturity_year_idx] != 2025
    assert str(term_row[maturity_display_idx] or "").strip() != "2025"
