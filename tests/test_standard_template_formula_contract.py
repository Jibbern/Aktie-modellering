from __future__ import annotations

from pathlib import Path

from openpyxl import load_workbook

from pbi_xbrl.standard_template_formula_contract import (
    ANNUAL_FORMULA_ROWS,
    ANNUAL_RAW_ROWS,
    BS_RAW_ROWS,
    FORMULA_CONTRACT_VERSION,
    FORMULA_ROWS,
    VALUATION_HELPER_ROWS,
    VALUATION_RAW_ROWS,
)


ROOT = Path(__file__).resolve().parents[1]
SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"


def test_quarterly_formula_contract_has_blank_guards_and_protected_outputs() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        for contract in FORMULA_ROWS:
            for column in range(2, 14):
                cell = ws.cell(contract.row, column)
                assert isinstance(cell.value, str) and cell.value.startswith("="), contract.formula_id
                assert cell.protection.locked is True

        revenue_ttm = str(ws["E10"].value)
        assert "History_Q!" in revenue_ttm
        assert 'COUNTIFS(' in revenue_ttm
        assert 'MAXIFS(' in revenue_ttm and 'MINIFS(' in revenue_ttm
        assert '"revenue"' in revenue_ttm and '"$m"' in revenue_ttm
        revenue_yoy = str(ws["F11"].value)
        assert "History_Q!" in revenue_yoy
        assert 'COUNTIFS(' in revenue_yoy and '-4)' in revenue_yoy
        assert '"revenue"' in revenue_yoy and '"$m"' in revenue_yoy
        assert ws["M47"].value == '=IF(OR(M43="",M44=""),"",M43-M44)'
        assert ws["M86"].value == '=IF(OR(M73="",M84="",M84=0),"",M73/M84)'
        assert "History_Q!" in str(ws["M271"].value)
        assert '"operating_cash_flow"' in str(ws["M271"].value)
        assert ws.row_dimensions[271].hidden is True
        assert ws["M271"].protection.locked is True
    finally:
        wb.close()


def test_source_backed_targets_are_blank_and_unlocked() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        valuation = wb["Valuation"]
        for row in sorted(set(VALUATION_RAW_ROWS.values()) | set(VALUATION_HELPER_ROWS.values())):
            for column in range(2, 14):
                cell = valuation.cell(row, column)
                assert cell.value is None
                assert cell.protection.locked is False
        for row in range(194, 217):
            assert valuation.cell(row, 4).value is None
            assert valuation.cell(row, 4).protection.locked is False

        bs = wb["BS_Segments"]
        for row in BS_RAW_ROWS.values():
            for column in range(2, 14):
                assert bs.cell(row, column).value is None
                assert bs.cell(row, column).protection.locked is False
        for row in ANNUAL_RAW_ROWS.values():
            for column in range(2, 10):
                assert bs.cell(row, column).value is None
                assert bs.cell(row, column).protection.locked is False
    finally:
        wb.close()


def test_annual_formula_contract_is_generic_and_exact() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        ws = wb["BS_Segments"]
        assert ws["A81"].value == "Annual financial history"
        assert ws["A82"].value == "Fiscal year"
        for contract in ANNUAL_FORMULA_ROWS:
            for column in range(2, 10):
                cell = ws.cell(contract.row, column)
                assert isinstance(cell.value, str) and cell.value.startswith("="), contract.formula_id
                assert cell.protection.locked is True
        assert ws["B96"].value == '=IF(OR(B94="",B95=""),"",B94-B95)'
        assert ws["B98"].value is None
        assert ws["B98"].protection.locked is False
        assert ws["B99"].value is None
        assert ws["B99"].protection.locked is False
        assert ws["B101"].value == '=IF(OR(B100="",B98="",B98=0),"",B100/B98)'
        assert ws["B104"].value == '=IF(OR(B103="",B102=""),"",B103-B102)'
    finally:
        wb.close()


def test_formula_economics_use_exact_generic_inputs_and_fail_closed() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        valuation = wb["Valuation"]
        exact = {
            "B13": '=IF(OR(B262="",B9="",B9=0),"",B262/B9)',
            "B14": '=IF(OR(B32="",B9="",B9=0),"",B32/B9)',
            "B19": '=IF(OR(B18="",B9="",B9=0),"",B18/B9)',
            "B27": '=IF(OR(B24="",B9="",B9=0),"",B24/B9)',
            "B37": '=IF(OR(B36="",B9="",B9=0),"",B36/B9)',
            "B47": '=IF(OR(B43="",B44=""),"",B43-B44)',
            "B57": '=IF(OR(B47="",B9="",B9=0),"",B47/B9)',
            "B73": '=IF(OR(B72="",B70=""),"",B72-B70)',
            "B80": '=IF(OR(B70="",B72="",B79=""),"",B72+B79-B70)',
            "B86": '=IF(OR(B73="",B84="",B84=0),"",B73/B84)',
            "B113": '=IF(OR(B268="",B103="",B103=0),"",B268/B103)',
            "B114": '=IF(OR(B268="",B269="",B270="",B103="",B103=0),"",(B268-B269-B270)/B103)',
            "B115": '=IF(OR(B49="",B102="",B102=0),"",B49/B102)',
        }
        for coordinate, formula in exact.items():
            assert valuation[coordinate].value == formula

        for contract in FORMULA_ROWS:
            formulas = [str(valuation.cell(contract.row, column).value) for column in range(2, 14)]
            assert any(formula.startswith("=IF(") for formula in formulas), contract.formula_id
            assert all(formula.startswith("=IF(") or formula == '=""' for formula in formulas), contract.formula_id
            for formula in formulas:
                if "History_Q!" in formula:
                    assert '"populated"' in formula, contract.formula_id
                    assert "COUNTIFS(" in formula, contract.formula_id
    finally:
        wb.close()


def test_visible_metric_labels_are_concise_without_losing_definition() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        assert [ws[f"A{row}"].value for row in range(36, 41)] == [
            "Net income",
            "Net margin %",
            "Net income YoY %",
            "Net income (TTM)",
            "Net margin (TTM)",
        ]
        assert ws["A107"].value == "Diluted EPS (GAAP)"
        assert ws["A109"].value == "Diluted EPS (GAAP, TTM)"
        assert ws["A110"].value == "Adjusted diluted EPS"
        assert ws["A111"].value == "Adjusted diluted EPS (TTM)"
        assert ws["A18"].value == "EBITDA (base)"
        assert ws["A24"].value == "Adjusted EBITDA"
    finally:
        wb.close()


def test_formula_contract_contains_no_ticker_specific_content() -> None:
    source = (ROOT / "pbi_xbrl" / "standard_template_formula_contract.py").read_text(encoding="utf-8")
    assert FORMULA_CONTRACT_VERSION == "1.2.0"
    for forbidden in ("Abercrombie", "Hollister", "ANF_model", "A&F"):
        assert forbidden not in source


def test_calculation_history_is_a_hidden_source_backed_formula_input_projection() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        ws = wb["History_Q"]
        assert ws.sheet_state == "hidden"
        assert [ws.cell(1, column).value for column in range(1, 8)] == [
            "period",
            "period_ordinal",
            "metric",
            "value",
            "unit",
            "source_ref",
            "status",
        ]
        for row in (2, 500, 1000):
            for column in range(1, 8):
                assert ws.cell(row, column).value is None
                assert ws.cell(row, column).protection.locked is False
    finally:
        wb.close()
