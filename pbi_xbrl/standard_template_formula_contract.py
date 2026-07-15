"""Ticker-neutral formula and visible-label contract for the frozen shell.

Raw, source-backed facts are written by the binding plan.  This module owns only
deterministic workbook formulas and generic presentation labels.  It deliberately
contains no ticker names, source values, or valuation assumptions.
"""
from __future__ import annotations

from copy import copy
from dataclasses import dataclass
from typing import Any, Collection

from openpyxl.styles import Protection
from openpyxl.utils import get_column_letter


FORMULA_CONTRACT_VERSION = "1.2.0"
FIRST_QUARTER_COLUMN = 2
LAST_QUARTER_COLUMN = 13
FIRST_ANNUAL_COLUMN = 2
LAST_ANNUAL_COLUMN = 9
CALCULATION_HISTORY_FIRST_ROW = 2
CALCULATION_HISTORY_LAST_ROW = 1000
CALCULATION_HISTORY_COLUMNS = {
    "period": "A",
    "period_ordinal": "B",
    "metric": "C",
    "value": "D",
    "unit": "E",
    "source_ref": "F",
    "status": "G",
}


@dataclass(frozen=True)
class FormulaRow:
    formula_id: str
    sheet: str
    row: int
    number_format: str
    description: str


@dataclass(frozen=True)
class FormulaTargetContract:
    """One formula contract ID and its exact owned workbook cells."""

    formula_id: str
    sheet: str
    targets: tuple[str, ...]


VALUATION_RAW_ROWS = {
    "revenue": 9,
    "base_ebitda": 18,
    "adjusted_ebitda": 24,
    "operating_income": 32,
    "net_income": 36,
    "operating_cash_flow": 43,
    "capital_expenditures": 44,
    "interest_paid": 59,
    "buybacks_cash": 62,
    "cash": 70,
    "marketable_securities": 71,
    "debt_core": 72,
    "lease_liabilities": 79,
    "pension_obligation_net": 82,
    "revolver_availability": 95,
    "diluted_shares": 102,
    "shares_outstanding": 103,
    "eps": 107,
    "adjusted_eps": 110,
}

# Hidden rows carry source-backed inputs that have no useful standalone visible
# row.  They are binding targets, not formulas, and remain part of the signed shell.
VALUATION_HELPER_ROWS = {
    "gross_profit": 262,
    "interest_expense": 263,
    "dividends_cash": 264,
    "acquisitions_cash": 265,
    "debt_repayment": 266,
    "debt_issuance": 267,
    "total_equity": 268,
    "goodwill": 269,
    "intangibles": 270,
}

VALUATION_FORMULA_HELPER_ROWS = {
    "operating_cash_flow_ttm": 271,
}

BS_RAW_ROWS = {
    "cash": 9,
    "restricted_cash": 10,
    "marketable_securities": 13,
    "accounts_receivable": 14,
    "inventory": 15,
    "current_assets": 18,
    "property_plant_equipment_net": 19,
    "goodwill": 22,
    "intangibles": 23,
    "other_assets_noncurrent": 24,
    "total_assets": 25,
    "accounts_payable": 28,
    "accrued_liabilities": 29,
    "short_term_borrowings": 32,
    "debt_current": 33,
    "lease_liabilities_current": 34,
    "current_liabilities": 35,
    "debt_core": 40,
    "lease_liabilities_noncurrent": 42,
    "pension_obligation_net": 43,
    "other_liabilities_noncurrent": 44,
    "total_liabilities": 45,
    "total_equity": 47,
    "shares_outstanding": 48,
    "diluted_shares": 49,
}

ANNUAL_RAW_ROWS = {
    "revenue": 83,
    "gross_profit": 84,
    "operating_income": 86,
    "base_ebitda": 88,
    "adjusted_ebitda": 90,
    "net_income": 92,
    "operating_cash_flow": 94,
    "capital_expenditures": 95,
    "shares_outstanding": 98,
    "eps": 99,
    "total_equity": 100,
    "cash": 102,
    "debt_core": 103,
}

ANNUAL_FORMULA_ROWS = (
    FormulaRow("annual_gross_margin", "BS_Segments", 85, "0.0%;[Red]-0.0%", "Annual gross margin."),
    FormulaRow("annual_operating_margin", "BS_Segments", 87, "0.0%;[Red]-0.0%", "Annual operating margin."),
    FormulaRow("annual_ebitda_margin", "BS_Segments", 89, "0.0%;[Red]-0.0%", "Annual base EBITDA margin."),
    FormulaRow("annual_adjusted_ebitda_margin", "BS_Segments", 91, "0.0%;[Red]-0.0%", "Annual adjusted EBITDA margin."),
    FormulaRow("annual_net_margin", "BS_Segments", 93, "0.0%;[Red]-0.0%", "Annual net margin."),
    FormulaRow("annual_free_cash_flow", "BS_Segments", 96, "#,##0.0;[Red]-#,##0.0", "Annual CFO less capex."),
    FormulaRow("annual_free_cash_flow_margin", "BS_Segments", 97, "0.0%;[Red]-0.0%", "Annual free-cash-flow margin."),
    FormulaRow("annual_book_value_per_share", "BS_Segments", 101, "$0.00;[Red]-$0.00", "Annual total equity divided by point-in-time year-end shares outstanding."),
    FormulaRow("annual_net_debt", "BS_Segments", 104, "#,##0.0;[Red]-#,##0.0", "Annual core debt less cash."),
)

BS_QUARTERLY_FORMULA_ROWS = (
    FormulaRow("bs_cash_including_restricted", "BS_Segments", 11, "#,##0.0;[Red]-#,##0.0", "Cash plus restricted cash."),
    FormulaRow("bs_cash_qoq", "BS_Segments", 12, "#,##0.0;[Red]-#,##0.0", "Quarter-over-quarter cash change."),
    FormulaRow("bs_goodwill_assets_ratio", "BS_Segments", 26, "0.0%;[Red]-0.0%", "Goodwill divided by total assets."),
    FormulaRow("bs_working_capital", "BS_Segments", 36, "#,##0.0;[Red]-#,##0.0", "Current assets less current liabilities."),
    FormulaRow("bs_working_capital_qoq", "BS_Segments", 37, "#,##0.0;[Red]-#,##0.0", "Quarter-over-quarter working-capital change."),
    FormulaRow("bs_current_ratio", "BS_Segments", 38, "0.0%;[Red]-0.0%", "Current assets divided by current liabilities."),
    FormulaRow("bs_quick_ratio", "BS_Segments", 39, "0.0%;[Red]-0.0%", "Cash-adjusted current assets divided by current liabilities."),
    FormulaRow("bs_debt_qoq", "BS_Segments", 41, "#,##0.0;[Red]-#,##0.0", "Quarter-over-quarter core-debt change."),
    FormulaRow("bs_inventory_yoy", "BS_Segments", 51, "0.0%;[Red]-0.0%", "Inventory growth versus prior year."),
    FormulaRow("bs_revenue_yoy", "BS_Segments", 52, "0.0%;[Red]-0.0%", "Revenue growth versus prior year."),
    FormulaRow("bs_inventory_vs_revenue_growth", "BS_Segments", 53, "0.0%;[Red]-0.0%", "Inventory growth less revenue growth."),
    FormulaRow("bs_core_net_cash", "BS_Segments", 54, "#,##0.0;[Red]-#,##0.0", "Cash plus securities less core debt."),
    FormulaRow("bs_total_lease_liabilities", "BS_Segments", 55, "#,##0.0;[Red]-#,##0.0", "Current plus non-current lease liabilities."),
    FormulaRow("bs_diluted_shares_yoy", "BS_Segments", 56, "0.0%;[Red]-0.0%", "Diluted-share growth versus prior year."),
)

VALUATION_OUTPUT_FORMULA_CELLS = tuple(f"N{row}" for row in range(194, 211))
VALUATION_SIDECAR_FORMULA_CELLS = tuple(f"U{row}" for row in (64, 65, 66, 67, 68, 69, 70, 73, 74, 75))


FORMULA_ROWS = (
    FormulaRow("revenue_ttm", "Valuation", 10, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter revenue."),
    FormulaRow("revenue_yoy", "Valuation", 11, "0.0%;[Red]-0.0%", "Quarterly revenue growth versus prior year."),
    FormulaRow("gross_margin", "Valuation", 13, "0.0%;[Red]-0.0%", "Gross profit divided by revenue."),
    FormulaRow("operating_margin", "Valuation", 14, "0.0%;[Red]-0.0%", "Operating income divided by revenue."),
    FormulaRow("operating_margin_ttm", "Valuation", 15, "0.0%;[Red]-0.0%", "TTM operating income divided by TTM revenue."),
    FormulaRow("ebitda_margin", "Valuation", 19, "0.0%;[Red]-0.0%", "Base EBITDA divided by revenue."),
    FormulaRow("ebitda_yoy", "Valuation", 20, "0.0%;[Red]-0.0%", "Base EBITDA growth versus prior year."),
    FormulaRow("ebitda_ttm", "Valuation", 21, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter base EBITDA."),
    FormulaRow("ebitda_margin_ttm", "Valuation", 22, "0.0%;[Red]-0.0%", "TTM base EBITDA divided by TTM revenue."),
    FormulaRow("adjusted_ebitda_ttm", "Valuation", 25, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter adjusted EBITDA."),
    FormulaRow("adjusted_ebitda_delta", "Valuation", 26, "#,##0.0;[Red]-#,##0.0", "Adjusted EBITDA less base EBITDA."),
    FormulaRow("adjusted_ebitda_margin", "Valuation", 27, "0.0%;[Red]-0.0%", "Adjusted EBITDA divided by revenue."),
    FormulaRow("adjusted_ebitda_yoy", "Valuation", 28, "0.0%;[Red]-0.0%", "Adjusted EBITDA growth versus prior year."),
    FormulaRow("adjusted_ebitda_margin_ttm", "Valuation", 29, "0.0%;[Red]-0.0%", "TTM adjusted EBITDA divided by TTM revenue."),
    FormulaRow("operating_income_margin", "Valuation", 33, "0.0%;[Red]-0.0%", "Operating income divided by revenue."),
    FormulaRow("operating_income_ttm", "Valuation", 34, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter operating income."),
    FormulaRow("operating_income_margin_ttm", "Valuation", 35, "0.0%;[Red]-0.0%", "TTM operating income divided by TTM revenue."),
    FormulaRow("net_margin", "Valuation", 37, "0.0%;[Red]-0.0%", "Net income divided by revenue."),
    FormulaRow("net_income_yoy", "Valuation", 38, "0.0%;[Red]-0.0%", "Net income growth versus prior year."),
    FormulaRow("net_income_ttm", "Valuation", 39, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter net income."),
    FormulaRow("net_margin_ttm", "Valuation", 40, "0.0%;[Red]-0.0%", "TTM net income divided by TTM revenue."),
    FormulaRow("capex_margin", "Valuation", 45, "0.0%;[Red]-0.0%", "Capital expenditures divided by revenue."),
    FormulaRow("capex_margin_ttm", "Valuation", 46, "0.0%;[Red]-0.0%", "TTM capital expenditures divided by TTM revenue."),
    FormulaRow("free_cash_flow", "Valuation", 47, "#,##0.0;[Red]-#,##0.0", "Operating cash flow less capital expenditures."),
    FormulaRow("free_cash_flow_yoy_delta", "Valuation", 48, "#,##0.0;[Red]-#,##0.0", "Free-cash-flow change versus prior year."),
    FormulaRow("free_cash_flow_ttm", "Valuation", 49, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter free cash flow."),
    FormulaRow("free_cash_flow_ttm_yoy_delta", "Valuation", 50, "#,##0.0;[Red]-#,##0.0", "TTM free-cash-flow change versus prior year."),
    FormulaRow("free_cash_flow_margin", "Valuation", 57, "0.0%;[Red]-0.0%", "Free cash flow divided by revenue."),
    FormulaRow("free_cash_flow_margin_ttm", "Valuation", 58, "0.0%;[Red]-0.0%", "TTM free cash flow divided by TTM revenue."),
    FormulaRow("buybacks_ttm", "Valuation", 63, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter cash used for buybacks."),
    FormulaRow("dividends_ttm", "Valuation", 64, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter cash dividends."),
    FormulaRow("acquisitions_ttm", "Valuation", 65, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter acquisition cash outflow."),
    FormulaRow("debt_repayment_ttm", "Valuation", 66, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter gross debt repayment."),
    FormulaRow("debt_issuance_ttm", "Valuation", 67, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter gross debt issuance."),
    FormulaRow("net_debt", "Valuation", 73, "#,##0.0;[Red]-#,##0.0", "Core debt less cash."),
    FormulaRow("net_debt_qoq", "Valuation", 74, "#,##0.0;[Red]-#,##0.0", "Quarter-over-quarter change in net debt."),
    FormulaRow("net_debt_yoy", "Valuation", 75, "#,##0.0;[Red]-#,##0.0", "Year-over-year change in net debt."),
    FormulaRow("core_net_cash", "Valuation", 77, "#,##0.0;[Red]-#,##0.0", "Cash less core debt."),
    FormulaRow("net_cash_with_securities", "Valuation", 78, "#,##0.0;[Red]-#,##0.0", "Cash plus marketable securities less core debt."),
    FormulaRow("lease_adjusted_net_debt", "Valuation", 80, "#,##0.0;[Red]-#,##0.0", "Core debt plus lease liabilities less cash."),
    FormulaRow("lease_adjusted_net_debt_with_securities", "Valuation", 81, "#,##0.0;[Red]-#,##0.0", "Lease-adjusted net debt after marketable securities."),
    FormulaRow("ebitda_ttm_copy", "Valuation", 84, "#,##0.0;[Red]-#,##0.0", "Coverage-panel base EBITDA TTM."),
    FormulaRow("adjusted_ebitda_ttm_copy", "Valuation", 85, "#,##0.0;[Red]-#,##0.0", "Coverage-panel adjusted EBITDA TTM."),
    FormulaRow("net_leverage", "Valuation", 86, "0.00x;[Red]-0.00x", "Net debt divided by base EBITDA TTM."),
    FormulaRow("adjusted_net_leverage", "Valuation", 87, "0.00x;[Red]-0.00x", "Net debt divided by adjusted EBITDA TTM."),
    FormulaRow("interest_coverage", "Valuation", 88, "0.00x;[Red]-0.00x", "TTM operating income divided by TTM interest expense."),
    FormulaRow("cash_interest_coverage", "Valuation", 89, "0.00x;[Red]-0.00x", "TTM base EBITDA divided by TTM cash interest paid."),
    FormulaRow("fcf_conversion", "Valuation", 90, "0.0%;[Red]-0.0%", "TTM free cash flow divided by TTM base EBITDA."),
    FormulaRow("diluted_shares_qoq", "Valuation", 104, "0.000;[Red]-0.000", "Quarter-over-quarter diluted share change."),
    FormulaRow("diluted_shares_yoy", "Valuation", 105, "0.000;[Red]-0.000", "Year-over-year diluted share change."),
    FormulaRow("gaap_eps_yoy", "Valuation", 108, "0.0%;[Red]-0.0%", "GAAP diluted EPS growth versus prior year."),
    FormulaRow("gaap_eps_ttm", "Valuation", 109, "$0.00;[Red]-$0.00", "Trailing-four-quarter GAAP diluted EPS."),
    FormulaRow("adjusted_eps_ttm", "Valuation", 111, "$0.00;[Red]-$0.00", "Trailing-four-quarter adjusted diluted EPS."),
    FormulaRow("book_value_per_share", "Valuation", 113, "$0.00;[Red]-$0.00", "Total equity divided by point-in-time shares outstanding."),
    FormulaRow("tangible_book_value_per_share", "Valuation", 114, "$0.00;[Red]-$0.00", "Equity less goodwill and intangibles, divided by point-in-time shares outstanding."),
    FormulaRow("free_cash_flow_per_share", "Valuation", 115, "$0.00;[Red]-$0.00", "TTM free cash flow divided by diluted shares."),
    FormulaRow("operating_cash_flow_ttm", "Valuation", 271, "#,##0.0;[Red]-#,##0.0", "Trailing-four-quarter operating cash flow."),
)


def formula_target_contracts() -> tuple[FormulaTargetContract, ...]:
    """Return the complete executable formula-cell contract for the union shell."""

    contracts = [
        FormulaTargetContract(row.formula_id, row.sheet, (f"B{row.row}:M{row.row}",))
        for row in FORMULA_ROWS
    ]
    contracts.extend(
        FormulaTargetContract(row.formula_id, row.sheet, (f"B{row.row}:M{row.row}",))
        for row in BS_QUARTERLY_FORMULA_ROWS
    )
    contracts.extend(
        FormulaTargetContract(row.formula_id, row.sheet, (f"B{row.row}:I{row.row}",))
        for row in ANNUAL_FORMULA_ROWS
    )
    contracts.extend(
        (
            FormulaTargetContract("valuation_output_formulas", "Valuation", ("N194:N210",)),
            FormulaTargetContract("valuation_sidecar_formulas", "Valuation", ("U64:U70", "U72:U75")),
            FormulaTargetContract(
                "valuation_scenario_formulas",
                "Valuation",
                ("J218", "Q219:Q221", "J222:J223", "H227:L234", "F236", "E241:E244", "E259:E261"),
            ),
            FormulaTargetContract(
                "investment_case_sensitivity_formulas",
                "{ticker}_Investment_Case",
                ("A161:A163", "A172:A174", "A178:A180"),
            ),
            FormulaTargetContract("hidden_value_issue_anchor", "Valuation", ("AI139",)),
        )
    )
    return tuple(contracts)


def apply_standard_formula_contracts(
    workbook: Any,
    *,
    enabled_formula_ids: Collection[str] | None = None,
) -> None:
    """Apply generic labels, formulas, helper rows, and formula protection."""

    all_formula_ids = {contract.formula_id for contract in formula_target_contracts()}
    enabled = all_formula_ids if enabled_formula_ids is None else {str(value) for value in enabled_formula_ids}
    unknown = sorted(enabled - all_formula_ids)
    if unknown:
        raise ValueError(f"Unknown standard-template formula contracts: {unknown!r}.")

    valuation = workbook["Valuation"]
    bs = workbook["BS_Segments"]
    _clear_disabled_formula_targets(workbook, enabled)
    _prepare_calculation_history_sheet(workbook)
    _extend_balance_sheet_quarterly_axis(bs)
    _prepare_raw_targets(valuation, bs)
    _apply_visible_labels(valuation)
    _apply_balance_sheet_labels(bs)
    _apply_hidden_helpers(valuation)
    _apply_hidden_formula_helpers(valuation)
    _apply_valuation_quarterly_formulas(valuation, enabled)
    _apply_balance_sheet_formulas(bs, enabled)
    _apply_annual_financial_block(bs, enabled)
    _apply_valuation_input_outputs(valuation, enabled)
    _apply_valuation_sidecar_outputs(valuation, enabled)


def _clear_disabled_formula_targets(workbook: Any, enabled_formula_ids: set[str]) -> None:
    from openpyxl.utils.cell import range_boundaries

    for contract in formula_target_contracts():
        if contract.formula_id in enabled_formula_ids or contract.sheet not in workbook.sheetnames:
            continue
        ws = workbook[contract.sheet]
        for target in contract.targets:
            min_col, min_row, max_col, max_row = range_boundaries(target)
            for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
                for cell in row:
                    cell.value = None


def _prepare_raw_targets(valuation: Any, bs: Any) -> None:
    for row in sorted(set(VALUATION_RAW_ROWS.values()) | set(VALUATION_HELPER_ROWS.values())):
        for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
            cell = valuation.cell(row, column)
            cell.value = None
            protection = copy(cell.protection)
            protection.locked = False
            cell.protection = protection
    for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
        protection = copy(valuation.cell(6, column).protection)
        protection.locked = False
        valuation.cell(6, column).protection = protection
    for row in BS_RAW_ROWS.values():
        for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
            cell = bs.cell(row, column)
            cell.value = None
            protection = copy(cell.protection)
            protection.locked = False
            cell.protection = protection
    for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
        protection = copy(bs.cell(7, column).protection)
        protection.locked = False
        bs.cell(7, column).protection = protection


def _prepare_calculation_history_sheet(workbook: Any) -> None:
    ws = workbook["History_Q"] if "History_Q" in workbook.sheetnames else workbook.create_sheet("History_Q")
    headers = ["period", "period_ordinal", "metric", "value", "unit", "source_ref", "status"]
    for column, header in enumerate(headers, start=1):
        ws.cell(1, column).value = header
        ws.cell(1, column).protection = Protection(locked=True)
    for row in range(CALCULATION_HISTORY_FIRST_ROW, CALCULATION_HISTORY_LAST_ROW + 1):
        for column in range(1, len(headers) + 1):
            cell = ws.cell(row, column)
            cell.value = None
            cell.protection = Protection(locked=False)
    ws.sheet_state = "hidden"


def _apply_visible_labels(ws: Any) -> None:
    labels = {
        18: "EBITDA (base)",
        19: "EBITDA margin %",
        20: "EBITDA YoY %",
        21: "EBITDA (base, TTM)",
        22: "EBITDA margin (TTM)",
        24: "Adjusted EBITDA",
        25: "Adjusted EBITDA (TTM)",
        26: "Adjusted EBITDA - EBITDA",
        27: "Adjusted EBITDA margin %",
        28: "Adjusted EBITDA YoY %",
        29: "Adjusted EBITDA margin (TTM)",
        36: "Net income",
        37: "Net margin %",
        38: "Net income YoY %",
        39: "Net income (TTM)",
        40: "Net margin (TTM)",
        44: "Capex",
        47: "FCF (CFO - Capex)",
        102: "Diluted shares (m)",
        103: "Shares outstanding (m)",
        107: "Diluted EPS (GAAP)",
        108: "Diluted EPS YoY %",
        109: "Diluted EPS (GAAP, TTM)",
        110: "Adjusted diluted EPS",
        111: "Adjusted diluted EPS (TTM)",
        113: "Book value/share",
        114: "Tangible book value/share",
        115: "FCF/share (TTM)",
    }
    for row, label in labels.items():
        ws.cell(row, 1).value = label


def _apply_balance_sheet_labels(ws: Any) -> None:
    labels = {
        13: "Short-term investments / marketable securities",
        32: "Short-term borrowings",
        33: "Current maturities of long-term debt",
        40: "Debt (core borrowings)",
    }
    for row, label in labels.items():
        ws.cell(row, 1).value = label


def _extend_balance_sheet_quarterly_axis(ws: Any) -> None:
    """Extend the reusable quarterly BS/segment axis to twelve periods."""

    merge_updates = {
        "A4:I4": "A4:M4",
        "A5:I5": "A5:M5",
        "B6:I6": "B6:M6",
        "A59:I59": "A59:M59",
    }
    existing = {str(item) for item in ws.merged_cells.ranges}
    for old, new in merge_updates.items():
        if old in existing:
            ws.unmerge_cells(old)
        if new not in {str(item) for item in ws.merged_cells.ranges}:
            ws.merge_cells(new)

    source_width = ws.column_dimensions["I"].width
    for column in range(10, LAST_QUARTER_COLUMN + 1):
        letter = get_column_letter(column)
        ws.column_dimensions[letter].width = source_width
        for row in range(4, 69):
            source = ws.cell(row, 9)
            target = ws.cell(row, column)
            target._style = copy(source._style)
            target.number_format = source.number_format
            target.protection = copy(source.protection)


def _apply_hidden_helpers(ws: Any) -> None:
    for metric, row in VALUATION_HELPER_ROWS.items():
        ws.cell(row, 1).value = f"Formula input: {metric}"
        ws.row_dimensions[row].hidden = True
        for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
            cell = ws.cell(row, column)
            cell.value = None
            cell.number_format = "#,##0.0;[Red]-#,##0.0"
            cell.protection = Protection(locked=False)


def _apply_hidden_formula_helpers(ws: Any) -> None:
    for metric, row in VALUATION_FORMULA_HELPER_ROWS.items():
        ws.cell(row, 1).value = f"Formula output: {metric}"
        ws.row_dimensions[row].hidden = True
        for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
            cell = ws.cell(row, column)
            cell.value = None
            cell.number_format = "#,##0.0;[Red]-#,##0.0"
            cell.protection = Protection(locked=True)


def _apply_valuation_quarterly_formulas(ws: Any, enabled_formula_ids: set[str]) -> None:
    helper = VALUATION_HELPER_ROWS
    for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
        col = get_column_letter(column)
        prior = get_column_letter(column - 4) if column - 4 >= FIRST_QUARTER_COLUMN else ""
        period_cell = f"{col}$6"

        formulas: dict[int, str] = {
            10: _history_ttm_sum(period_cell, "revenue", "$m"),
            11: _history_yoy_ratio(period_cell, "revenue", "$m"),
            13: _ratio(f"{col}{helper['gross_profit']}", f"{col}9"),
            14: _ratio(f"{col}32", f"{col}9"),
            15: _history_ttm_ratio(period_cell, "operating_income", "revenue", "$m"),
            19: _ratio(f"{col}18", f"{col}9"),
            20: _history_yoy_ratio(period_cell, "base_ebitda", "$m"),
            21: _history_ttm_sum(period_cell, "base_ebitda", "$m"),
            22: _ratio(f"{col}21", f"{col}10"),
            25: _history_ttm_sum(period_cell, "adjusted_ebitda", "$m"),
            26: _difference(f"{col}24", f"{col}18"),
            27: _ratio(f"{col}24", f"{col}9"),
            28: _history_yoy_ratio(period_cell, "adjusted_ebitda", "$m"),
            29: _ratio(f"{col}25", f"{col}10"),
            33: _ratio(f"{col}32", f"{col}9"),
            34: _history_ttm_sum(period_cell, "operating_income", "$m"),
            35: _ratio(f"{col}34", f"{col}10"),
            37: _ratio(f"{col}36", f"{col}9"),
            38: _history_yoy_ratio(period_cell, "net_income", "$m"),
            39: _history_ttm_sum(period_cell, "net_income", "$m"),
            40: _ratio(f"{col}39", f"{col}10"),
            45: _ratio(f"{col}44", f"{col}9"),
            46: _history_ttm_ratio(period_cell, "capital_expenditures", "revenue", "$m"),
            47: _difference(f"{col}43", f"{col}44"),
            48: _history_yoy_difference_of_differences(period_cell, "operating_cash_flow", "capital_expenditures", "$m"),
            49: _history_ttm_difference(period_cell, "operating_cash_flow", "capital_expenditures", "$m"),
            50: _history_ttm_yoy_difference_of_differences(period_cell, "operating_cash_flow", "capital_expenditures", "$m"),
            57: _ratio(f"{col}47", f"{col}9"),
            58: _ratio(f"{col}49", f"{col}10"),
            63: _history_ttm_sum(period_cell, "buybacks_cash", "$m"),
            64: _history_ttm_sum(period_cell, "dividends_cash", "$m"),
            65: _history_ttm_sum(period_cell, "acquisitions_cash", "$m"),
            66: _history_ttm_sum(period_cell, "debt_repayment", "$m"),
            67: _history_ttm_sum(period_cell, "debt_issuance", "$m"),
            73: _difference(f"{col}72", f"{col}70"),
            74: _qoq_difference(column, 73),
            75: _yoy_difference(prior, col, 73),
            77: f'=IF({col}73="","",-{col}73)',
            78: f'=IF(OR({col}70="",{col}71="",{col}72=""),"",{col}70+{col}71-{col}72)',
            80: f'=IF(OR({col}70="",{col}72="",{col}79=""),"",{col}72+{col}79-{col}70)',
            81: f'=IF(OR({col}70="",{col}71="",{col}72="",{col}79=""),"",{col}72+{col}79-{col}70-{col}71)',
            84: f'=IF({col}21="","",{col}21)',
            85: f'=IF({col}25="","",{col}25)',
            86: _ratio(f"{col}73", f"{col}84"),
            87: _ratio(f"{col}73", f"{col}85"),
            88: _history_ttm_ratio(period_cell, "operating_income", "interest_expense", "$m"),
            89: _history_ttm_ratio(period_cell, "base_ebitda", "interest_paid", "$m"),
            90: _ratio(f"{col}49", f"{col}21"),
            104: _history_point_difference(period_cell, "diluted_shares", "m shares", offset=-1),
            105: _history_point_difference(period_cell, "diluted_shares", "m shares", offset=-4),
            108: _history_yoy_ratio(period_cell, "eps", "$/share"),
            109: _history_ttm_sum(period_cell, "eps", "$/share"),
            111: _history_ttm_sum(period_cell, "adjusted_eps", "$/share"),
            113: _ratio(f"{col}{helper['total_equity']}", f"{col}103"),
            114: f'=IF(OR({col}{helper["total_equity"]}="",{col}{helper["goodwill"]}="",{col}{helper["intangibles"]}="",{col}103="",{col}103=0),"",({col}{helper["total_equity"]}-{col}{helper["goodwill"]}-{col}{helper["intangibles"]})/{col}103)',
            115: _ratio(f"{col}49", f"{col}102"),
            271: _history_ttm_sum(period_cell, "operating_cash_flow", "$m"),
        }
        formula_ids_by_row = {contract.row: contract.formula_id for contract in FORMULA_ROWS}
        for row, formula in formulas.items():
            if formula_ids_by_row[row] in enabled_formula_ids:
                _set_formula(ws.cell(row, column), formula, _number_format_for_row(row))


def _apply_balance_sheet_formulas(ws: Any, enabled_formula_ids: set[str]) -> None:
    for column in range(FIRST_QUARTER_COLUMN, LAST_QUARTER_COLUMN + 1):
        col = get_column_letter(column)
        prior = get_column_letter(column - 1) if column > 2 else ""
        yoy = get_column_letter(column - 4) if column > 5 else ""
        formulas = {
            11: f'=IF(OR({col}9="",{col}10=""),"",{col}9+{col}10)',
            12: _difference(f"{col}9", f"{prior}9") if prior else '=""',
            26: _ratio(f"{col}22", f"{col}25"),
            36: _difference(f"{col}18", f"{col}35"),
            37: _difference(f"{col}36", f"{prior}36") if prior else '=""',
            38: _ratio(f"{col}18", f"{col}35"),
            39: f'=IF(OR({col}18="",{col}15="",{col}35="",{col}35=0),"",({col}18-{col}15)/{col}35)',
            41: _difference(f"{col}40", f"{prior}40") if prior else '=""',
            51: _yoy_ratio(yoy, col, 15),
            52: _bs_sales_yoy_formula(col),
            53: f'=IF(OR({col}51="",{col}52=""),"",{col}51-{col}52)',
            54: f'=IF(OR({col}9="",{col}13="",{col}40=""),"",{col}9+{col}13-{col}40)',
            55: f'=IF(OR({col}34="",{col}42=""),"",{col}34+{col}42)',
            56: _yoy_ratio(yoy, col, 49),
        }
        formula_ids_by_row = {contract.row: contract.formula_id for contract in BS_QUARTERLY_FORMULA_ROWS}
        for row, formula in formulas.items():
            if formula_ids_by_row[row] not in enabled_formula_ids:
                continue
            number_format = "0.0%;[Red]-0.0%" if row in {26, 38, 39, 51, 52, 53, 56} else "#,##0.0;[Red]-#,##0.0"
            _set_formula(ws.cell(row, column), formula, number_format)


def _apply_annual_financial_block(ws: Any, enabled_formula_ids: set[str]) -> None:
    """Create a ticker-neutral annual history block below annual segments."""

    if "A81:I81" not in {str(item) for item in ws.merged_cells.ranges}:
        ws.merge_cells("A81:I81")
    for column in range(1, LAST_ANNUAL_COLUMN + 1):
        ws.cell(81, column)._style = copy(ws.cell(69, min(column, 9))._style)
        ws.cell(82, column)._style = copy(ws.cell(70, min(column, 9))._style)
        for row in range(83, 105):
            ws.cell(row, column)._style = copy(ws.cell(71, min(column, 9))._style)
    ws["A81"] = "Annual financial history"
    ws["A82"] = "Fiscal year"
    labels = {
        83: "Revenue",
        84: "Gross profit",
        85: "Gross margin %",
        86: "Operating income",
        87: "Operating margin %",
        88: "EBITDA (base)",
        89: "EBITDA margin %",
        90: "Adjusted EBITDA",
        91: "Adjusted EBITDA margin %",
        92: "Net income",
        93: "Net margin %",
        94: "CFO",
        95: "Capex",
        96: "FCF (CFO - Capex)",
        97: "FCF margin %",
        98: "Shares outstanding (year-end, m)",
        99: "Diluted EPS (GAAP)",
        100: "Total equity",
        101: "Book value/share",
        102: "Cash",
        103: "Debt (core borrowings)",
        104: "Net debt",
    }
    for row, label in labels.items():
        ws.cell(row, 1).value = label

    for column in range(FIRST_ANNUAL_COLUMN, LAST_ANNUAL_COLUMN + 1):
        col = get_column_letter(column)
        header = ws.cell(82, column)
        header.value = None
        header.protection = Protection(locked=False)
        for row in ANNUAL_RAW_ROWS.values():
            cell = ws.cell(row, column)
            cell.value = None
            cell.protection = Protection(locked=False)
            cell.number_format = "$0.00;[Red]-$0.00" if row == 99 else "#,##0.0;[Red]-#,##0.0"
        formulas = {
            85: _ratio(f"{col}84", f"{col}83"),
            87: _ratio(f"{col}86", f"{col}83"),
            89: _ratio(f"{col}88", f"{col}83"),
            91: _ratio(f"{col}90", f"{col}83"),
            93: _ratio(f"{col}92", f"{col}83"),
            96: _difference(f"{col}94", f"{col}95"),
            97: _ratio(f"{col}96", f"{col}83"),
            101: _ratio(f"{col}100", f"{col}98"),
            104: _difference(f"{col}103", f"{col}102"),
        }
        formats = {contract.row: contract.number_format for contract in ANNUAL_FORMULA_ROWS}
        formula_ids_by_row = {contract.row: contract.formula_id for contract in ANNUAL_FORMULA_ROWS}
        for row, formula in formulas.items():
            if formula_ids_by_row[row] in enabled_formula_ids:
                _set_formula(ws.cell(row, column), formula, formats[row])


def _apply_valuation_input_outputs(ws: Any, enabled_formula_ids: set[str]) -> None:
    if not {"valuation_output_formulas", "valuation_scenario_formulas"} & enabled_formula_ids:
        return
    labels = {
        "B199": "EBITDA (base, TTM)",
        "B200": "Adjusted EBITDA (TTM)",
        "B202": "CFO TTM ($m)",
        "B204": "Diluted EPS (GAAP, TTM)",
        "B205": "Adjusted diluted EPS (TTM)",
        "B206": "Book value/share",
        "B207": "Tangible book value/share",
        "K206": "P/E (GAAP, TTM)",
        "K207": "P/E (adjusted, TTM)",
    }
    for coordinate, label in labels.items():
        ws[coordinate] = label

    # Source-backed actuals and user assumptions share the visible input
    # surface.  The binding plan owns only declared source-backed cells; the
    # remaining unlocked cells are intentionally available for user inputs.
    for row in range(194, 217):
        protection = copy(ws.cell(row, 4).protection)
        protection.locked = False
        ws.cell(row, 4).protection = protection

    denominator = 'IF(PerShareMode="Outstanding",Shares,IF(PerShareMode="Diluted",SharesDiluted,""))'
    output_formulas = {
        "N194": '=IF(OR(Price="",Shares=""),"",Price*Shares)',
        "N195": '=IF(OR(MarketCap="",NetDebt=""),"",MarketCap+NetDebt)',
        "N196": '=IF(OR(EV="",Adj_EBITDA="",Adj_EBITDA=0),"",EV/Adj_EBITDA)',
        "N197": '=IF(OR(EV="",Base_EBITDA="",Base_EBITDA=0),"",EV/Base_EBITDA)',
        "N198": '=IF(OR(FCF_TTM="",InterestPaid_TTM=""),"",FCF_TTM+InterestPaid_TTM)',
        "N199": '=IF(OR(FCFF_Proxy_TTM="",EV="",EV=0),"",FCFF_Proxy_TTM/EV)',
        "N200": '=IF(OR(FCF_TTM="",MarketCap="",MarketCap=0),"",FCF_TTM/MarketCap)',
        "N201": '=IF(OR(FCF_TTM="",Capex_TTM="",MaintCapexRatio="",RecurringCashCosts="",WCNormalization=""),"",FCF_TTM+(1-MaintCapexRatio)*Capex_TTM-RecurringCashCosts+WCNormalization)',
        "N202": '=IF(OR(OwnerEarnings_TTM="",EV="",EV=0),"",OwnerEarnings_TTM/EV)',
        "N203": f'=IF(OR(Target_EV_AdjEBITDA="",Adj_EBITDA="",NetDebt="",{denominator}="",{denominator}=0),"",(Target_EV_AdjEBITDA*Adj_EBITDA-NetDebt)/{denominator})',
        "N204": f'=IF(OR(Target_EV_EBITDA="",Base_EBITDA="",NetDebt="",{denominator}="",{denominator}=0),"",(Target_EV_EBITDA*Base_EBITDA-NetDebt)/{denominator})',
        "N205": f'=IF(OR(Target_EV_Yield="",Target_EV_Yield<=0,FCFF_Proxy_TTM="",NetDebt="",{denominator}="",{denominator}=0),"",(FCFF_Proxy_TTM/IF(Target_EV_Yield>1,Target_EV_Yield/100,Target_EV_Yield)-NetDebt)/{denominator})',
        "N206": '=IF(OR(Price="",EPS_TTM="",EPS_TTM=0),"",Price/EPS_TTM)',
        "N207": '=IF(OR(Price="",Adj_EPS_TTM="",Adj_EPS_TTM=0),"",Price/Adj_EPS_TTM)',
        "N208": '=IF(OR(EV="",Revenue_TTM="",Revenue_TTM=0),"",EV/Revenue_TTM)',
        "N209": '=IF(OR(Price="",BV_PerShare="",BV_PerShare=0),"",Price/BV_PerShare)',
        "N210": '=IF(OR(Price="",TBV_PerShare="",TBV_PerShare=0),"",Price/TBV_PerShare)',
    }
    if "valuation_output_formulas" in enabled_formula_ids:
        for coordinate, formula in output_formulas.items():
            number_format = "0.00x" if coordinate in {"N196", "N197", "N206", "N207", "N208", "N209", "N210"} else ("0.0%" if coordinate in {"N199", "N200", "N202"} else "#,##0.0")
            _set_formula(ws[coordinate], formula, number_format)

    # Scenario assumptions are intentionally empty user inputs.  The legacy
    # workbook's company-specific presets must never be frozen into the shell.
    for coordinate in ("E236", "E237", "E238", "E239", "E240"):
        ws[coordinate] = None
        ws[coordinate].protection = Protection(locked=False)
    if "valuation_scenario_formulas" in enabled_formula_ids:
        ws["F236"] = '=IF(E236="","",IF(OR(E236="Base",E236="Bull",E236="Bear"),"Preset selected","Custom"))'


def _apply_valuation_sidecar_outputs(ws: Any, enabled_formula_ids: set[str]) -> None:
    if "valuation_sidecar_formulas" not in enabled_formula_ids:
        return
    labels = {
        64: "Adjusted EBITDA TTM",
        65: "FCF TTM",
        66: "Diluted EPS (GAAP, TTM)",
        67: "EV @ target EV/Adjusted EBITDA",
        68: "Equity value @ target EV/Adjusted EBITDA",
        69: "EV @ target EV/EBITDA",
        70: "Equity value @ target EV/EBITDA",
        72: "Per-share valuation outputs",
        73: "Value/share @ target EV/Adjusted EBITDA",
        74: "Value/share @ target EV/EBITDA",
        75: "Value/share @ target FCFF yield",
    }
    formulas = {
        64: '=IF(Adj_EBITDA="","",Adj_EBITDA)',
        65: '=IF(FCF_TTM="","",FCF_TTM)',
        66: '=IF(EPS_TTM="","",EPS_TTM)',
        67: '=IF(OR(Adj_EBITDA="",Target_EV_AdjEBITDA=""),"",Adj_EBITDA*Target_EV_AdjEBITDA)',
        68: '=IF(OR(U67="",NetDebt=""),"",U67-NetDebt)',
        69: '=IF(OR(Base_EBITDA="",Target_EV_EBITDA=""),"",Base_EBITDA*Target_EV_EBITDA)',
        70: '=IF(OR(U69="",NetDebt=""),"",U69-NetDebt)',
        72: '=TEXT(MIN(U73,U74,U75),"$0")&"-"&TEXT(MAX(U73,U74,U75),"$0")',
        73: '=IF(EqShare_Target_Adj="","",EqShare_Target_Adj)',
        74: '=IF(EqShare_Target_EV="","",EqShare_Target_EV)',
        75: '=IF(EqShare_Target_Yield="","",EqShare_Target_Yield)',
    }
    for row, label in labels.items():
        ws.cell(row, 15).value = label
        if row in formulas:
            _set_formula(ws.cell(row, 21), formulas[row], "$#,##0.00;[Red]-$#,##0.00" if row >= 73 else "#,##0.0;[Red]-#,##0.0")


def _set_formula(cell: Any, formula: str, number_format: str) -> None:
    cell.value = formula
    cell.number_format = number_format
    protection = copy(cell.protection)
    protection.locked = True
    cell.protection = protection


def _number_format_for_row(row: int) -> str:
    for contract in FORMULA_ROWS:
        if contract.row == row:
            return contract.number_format
    return "General"


def _history_range(column: str) -> str:
    return f"History_Q!${column}${CALCULATION_HISTORY_FIRST_ROW}:${column}${CALCULATION_HISTORY_LAST_ROW}"


def _history_ordinal(period_cell: str, metric: str) -> str:
    return (
        f'SUMIFS({_history_range(CALCULATION_HISTORY_COLUMNS["period_ordinal"])},'
        f'{_history_range(CALCULATION_HISTORY_COLUMNS["period"])},{period_cell},'
        f'{_history_range(CALCULATION_HISTORY_COLUMNS["metric"])},"{metric}")'
    )


def _history_end_ordinal(period_cell: str, metric: str, offset: int = 0) -> str:
    base = _history_ordinal(period_cell, metric)
    if offset == 0:
        return f"({base})"
    sign = "+" if offset > 0 else ""
    return f"({base}{sign}{offset})"


def _history_window_terms(period_cell: str, metric: str, unit: str, *, end_offset: int = 0) -> tuple[str, str, str, str]:
    end = _history_end_ordinal(period_cell, metric, end_offset)
    start = f"({end}-3)"
    metric_range = _history_range(CALCULATION_HISTORY_COLUMNS["metric"])
    ordinal_range = _history_range(CALCULATION_HISTORY_COLUMNS["period_ordinal"])
    unit_range = _history_range(CALCULATION_HISTORY_COLUMNS["unit"])
    status_range = _history_range(CALCULATION_HISTORY_COLUMNS["status"])
    value_range = _history_range(CALCULATION_HISTORY_COLUMNS["value"])
    criteria = (
        f'{metric_range},"{metric}",{ordinal_range},">="&{start},'
        f'{ordinal_range},"<="&{end},{unit_range},"{unit}",{status_range},"populated"'
    )
    count = f"COUNTIFS({criteria})"
    total = f"SUMIFS({value_range},{criteria})"
    minimum = f"MINIFS({ordinal_range},{metric_range},\"{metric}\",{ordinal_range},\">=\"&{start},{ordinal_range},\"<=\"&{end},{unit_range},\"{unit}\",{status_range},\"populated\")"
    maximum = f"MAXIFS({ordinal_range},{metric_range},\"{metric}\",{ordinal_range},\">=\"&{start},{ordinal_range},\"<=\"&{end},{unit_range},\"{unit}\",{status_range},\"populated\")"
    return count, total, minimum, maximum


def _history_point_terms(period_cell: str, metric: str, unit: str, *, offset: int = 0) -> tuple[str, str]:
    ordinal = _history_end_ordinal(period_cell, metric, offset)
    metric_range = _history_range(CALCULATION_HISTORY_COLUMNS["metric"])
    ordinal_range = _history_range(CALCULATION_HISTORY_COLUMNS["period_ordinal"])
    unit_range = _history_range(CALCULATION_HISTORY_COLUMNS["unit"])
    status_range = _history_range(CALCULATION_HISTORY_COLUMNS["status"])
    value_range = _history_range(CALCULATION_HISTORY_COLUMNS["value"])
    criteria = f'{metric_range},"{metric}",{ordinal_range},{ordinal},{unit_range},"{unit}",{status_range},"populated"'
    return f"COUNTIFS({criteria})", f"SUMIFS({value_range},{criteria})"


def _history_ttm_sum(period_cell: str, metric: str, unit: str, *, end_offset: int = 0) -> str:
    count, total, minimum, maximum = _history_window_terms(period_cell, metric, unit, end_offset=end_offset)
    return f'=IF(OR({period_cell}="",{count}<>4,{maximum}-{minimum}<>3),"",{total})'


def _history_ttm_ratio(period_cell: str, numerator_metric: str, denominator_metric: str, unit: str) -> str:
    n_count, n_total, n_min, n_max = _history_window_terms(period_cell, numerator_metric, unit)
    d_count, d_total, d_min, d_max = _history_window_terms(period_cell, denominator_metric, unit)
    return f'=IF(OR({period_cell}="",{n_count}<>4,{d_count}<>4,{n_max}-{n_min}<>3,{d_max}-{d_min}<>3,{d_total}=0),"",{n_total}/{d_total})'


def _history_yoy_ratio(period_cell: str, metric: str, unit: str) -> str:
    current_count, current = _history_point_terms(period_cell, metric, unit)
    prior_count, prior = _history_point_terms(period_cell, metric, unit, offset=-4)
    return f'=IF(OR({period_cell}="",{current_count}<>1,{prior_count}<>1,{prior}=0),"",{current}/{prior}-1)'


def _history_point_difference(period_cell: str, metric: str, unit: str, *, offset: int) -> str:
    current_count, current = _history_point_terms(period_cell, metric, unit)
    prior_count, prior = _history_point_terms(period_cell, metric, unit, offset=offset)
    return f'=IF(OR({period_cell}="",{current_count}<>1,{prior_count}<>1),"",{current}-{prior})'


def _history_ttm_difference(period_cell: str, left_metric: str, right_metric: str, unit: str, *, end_offset: int = 0) -> str:
    l_count, left, l_min, l_max = _history_window_terms(period_cell, left_metric, unit, end_offset=end_offset)
    r_count, right, r_min, r_max = _history_window_terms(period_cell, right_metric, unit, end_offset=end_offset)
    return f'=IF(OR({period_cell}="",{l_count}<>4,{r_count}<>4,{l_max}-{l_min}<>3,{r_max}-{r_min}<>3),"",{left}-{right})'


def _history_yoy_difference_of_differences(period_cell: str, left_metric: str, right_metric: str, unit: str) -> str:
    lc_count, left_current = _history_point_terms(period_cell, left_metric, unit)
    rc_count, right_current = _history_point_terms(period_cell, right_metric, unit)
    lp_count, left_prior = _history_point_terms(period_cell, left_metric, unit, offset=-4)
    rp_count, right_prior = _history_point_terms(period_cell, right_metric, unit, offset=-4)
    return f'=IF(OR({period_cell}="",{lc_count}<>1,{rc_count}<>1,{lp_count}<>1,{rp_count}<>1),"",({left_current}-{right_current})-({left_prior}-{right_prior}))'


def _history_ttm_yoy_difference_of_differences(period_cell: str, left_metric: str, right_metric: str, unit: str) -> str:
    current_left = _history_window_terms(period_cell, left_metric, unit)
    current_right = _history_window_terms(period_cell, right_metric, unit)
    prior_left = _history_window_terms(period_cell, left_metric, unit, end_offset=-4)
    prior_right = _history_window_terms(period_cell, right_metric, unit, end_offset=-4)
    checks = []
    for count, _total, minimum, maximum in (current_left, current_right, prior_left, prior_right):
        checks.extend((f"{count}<>4", f"{maximum}-{minimum}<>3"))
    current = f"({current_left[1]}-{current_right[1]})"
    prior = f"({prior_left[1]}-{prior_right[1]})"
    return f'=IF(OR({period_cell}="",{",".join(checks)}),"",{current}-{prior})'


def _ttm_sum(start_col: str, end_col: str, row: int) -> str:
    if not start_col:
        return '=""'
    range_ref = f"{start_col}{row}:{end_col}{row}"
    return f'=IF(COUNT({range_ref})<4,"",SUM({range_ref}))'


def _ttm_ratio(start_col: str, end_col: str, numerator_row: int, denominator_row: int) -> str:
    if not start_col:
        return '=""'
    numerator = f"{start_col}{numerator_row}:{end_col}{numerator_row}"
    denominator = f"{start_col}{denominator_row}:{end_col}{denominator_row}"
    return f'=IF(OR(COUNT({numerator})<4,COUNT({denominator})<4,SUM({denominator})=0),"",SUM({numerator})/SUM({denominator}))'


def _ratio(numerator: str, denominator: str) -> str:
    return f'=IF(OR({numerator}="",{denominator}="",{denominator}=0),"",{numerator}/{denominator})'


def _difference(left: str, right: str) -> str:
    return f'=IF(OR({left}="",{right}=""),"",{left}-{right})'


def _yoy_ratio(prior_col: str, current_col: str, row: int) -> str:
    if not prior_col:
        return '=""'
    return f'=IF(OR({prior_col}{row}="",{current_col}{row}="",{prior_col}{row}=0),"",{current_col}{row}/{prior_col}{row}-1)'


def _yoy_difference(prior_col: str, current_col: str, row: int) -> str:
    if not prior_col:
        return '=""'
    return _difference(f"{current_col}{row}", f"{prior_col}{row}")


def _qoq_difference(column: int, row: int) -> str:
    if column <= FIRST_QUARTER_COLUMN:
        return '=""'
    current = get_column_letter(column)
    prior = get_column_letter(column - 1)
    return _difference(f"{current}{row}", f"{prior}{row}")


def _bs_sales_yoy_formula(column: str) -> str:
    lookup = f"MATCH({column}$7,'Valuation'!$B$6:$M$6,0)"
    current = f"INDEX('Valuation'!$B$9:$M$9,1,{lookup})"
    prior = f"INDEX('Valuation'!$B$9:$M$9,1,{lookup}-4)"
    return f'=IFERROR(IF(OR({current}="",{prior}="",{prior}=0),"",{current}/{prior}-1),"")'
