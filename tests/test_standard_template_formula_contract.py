from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import load_workbook
from openpyxl.utils import range_boundaries

from pbi_xbrl.standard_template_formula_contract import (
    BS_RAW_ROWS,
    FORMULA_CONTRACT_VERSION,
    FORMULA_ROWS,
    USER_INPUT_CONTRACTS,
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


def test_source_backed_targets_are_blank_and_locked() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        valuation = wb["Valuation"]
        for row in sorted(set(VALUATION_RAW_ROWS.values()) | set(VALUATION_HELPER_ROWS.values())):
            for column in range(2, 14):
                cell = valuation.cell(row, column)
                assert cell.value is None
                assert cell.protection.locked is True

        bs = wb["BS_Segments"]
        for row in BS_RAW_ROWS.values():
            for column in range(2, 14):
                assert bs.cell(row, column).value is None
                assert bs.cell(row, column).protection.locked is True
    finally:
        wb.close()


def test_retired_annual_financial_surface_is_blank_locked_and_hidden() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        ws = wb["BS_Segments"]
        for row in range(79, 105):
            assert ws.row_dimensions[row].hidden is True
            for column in range(1, 14):
                cell = ws.cell(row, column)
                assert cell.value is None
                assert cell.protection.locked is True
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
    assert FORMULA_CONTRACT_VERSION == "2.3.0"
    for forbidden in ("Abercrombie", "Hollister", "ANF_model", "A&F"):
        assert forbidden not in source


def test_summary_formula_contract_is_exact_period_linked_and_fail_closed() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        summary = wb["SUMMARY"]
        expected_rows = {
            "B27": 10,
            "B29": 11,
            "B31": 38,
            "B32": 107,
            "B33": 108,
            "B36": 49,
            "B42": 88,
        }
        for coordinate, source_row in expected_rows.items():
            formula = str(summary[coordinate].value)
            assert formula.startswith("=IFERROR(IF(OR(")
            assert "$B$26" in formula
            assert "COUNTIF('Valuation'!$B$6:$M$6,$B$26)<>1" in formula
            assert f"'Valuation'!$B${source_row}:$M${source_row}" in formula
            assert "MATCH($B$26,'Valuation'!$B$6:$M$6,0)" in formula
            assert summary[coordinate].protection.locked is True

        fcf_yoy = str(summary["B37"].value)
        assert "'Valuation'!$B$48:$M$48" in fcf_yoy
        assert "'Valuation'!$B$47:$M$47" in fcf_yoy
        assert "MATCH($B$26,'Valuation'!$B$6:$M$6,0)<=4" in fcf_yoy
        assert "/ABS(" in fcf_yoy
        assert "=0" in fcf_yoy
        assert summary["A27"].value == "Revenue TTM ($m)"
        assert summary["A32"].value == "GAAP diluted EPS ($/share)"
        assert summary["A33"].value == "GAAP diluted EPS growth YoY (%)"
        assert summary["A36"].value == "FCF TTM ($m)"
        assert summary["A41"].value == "Net leverage (x)"
        assert summary["A42"].value == "Interest coverage, P&L TTM (x)"
        assert summary["A44"].value == "Revolver availability ($m)"
        assert summary["A45"].value == "Total liquidity ($m)"
        assert all(summary.cell(row, 3).value is None for row in range(27, 46))
    finally:
        wb.close()


def test_balance_sheet_ratio_formats_and_investment_case_units_are_explicit() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        bs = wb["BS_Segments"]
        assert bs["A38"].value == "Current ratio (x)"
        assert bs["A39"].value == "Quick ratio (x)"
        assert all(bs.cell(row, column).number_format == "0.00x;-0.00x" for row in (38, 39) for column in range(2, 14))

        investment_case = wb["{ticker}_Investment_Case"]
        assert investment_case["A15"].value == "Current share price ($/share)"
        assert investment_case["A16"].value == "Base shares (m shares; selected denominator)"
        assert investment_case["A17"].value == "Net debt ($m)"
        assert investment_case["A18"].value == "Revenue TTM ($m)"
        assert investment_case["A24"].value == "Revenue growth (%)"
        assert investment_case["A42"].value == "Target FCF yield (%)"
    finally:
        wb.close()


def test_summary_business_oracles_use_signed_fcf_improvement() -> None:
    current_revenue = 1_113.821
    prior_revenue = current_revenue / (1.0 + 0.015045871225204177)
    current_net_income = 67.134
    prior_net_income = current_net_income / (1.0 - 0.1651349906109708)
    current_eps = 1.4697550189373207
    prior_eps = current_eps / (1.0 - 0.07562577425325745)
    current_fcf = 44.256 - 61.341
    prior_fcf = -4.0 - 50.764

    assert (current_revenue / prior_revenue) - 1 == pytest.approx(0.015045871225204177)
    assert (current_net_income / prior_net_income) - 1 == pytest.approx(-0.1651349906109708)
    assert (current_eps / prior_eps) - 1 == pytest.approx(-0.07562577425325745)
    assert (current_fcf - prior_fcf) / abs(prior_fcf) == pytest.approx(0.688024979913812)


def test_typed_scenario_formulas_have_exact_ownership_and_no_unsafe_defaults() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        valuation = wb["Valuation"]
        investment_case = wb["{ticker}_Investment_Case"]

        for coordinate in ("D218", "D219", "D220", "J218", "J219", "J220", "J221", "E237", "J236", "D247", "D253", "D254"):
            assert valuation[coordinate].value is None
            assert valuation[coordinate].protection.locked is False
        for coordinate in ("J222", "J223", "E241", "E244", "E259", "E260", "E261", "J259", "J261", "N238", "N244", "R261"):
            assert isinstance(valuation[coordinate].value, str) and valuation[coordinate].value.startswith("=")
            assert valuation[coordinate].protection.locked is True

        assert "DCF_Horizon" in str(valuation["J222"].value)
        assert "ScenarioBuybackCash/ScenarioBuybackPrice" in str(valuation["E259"].value)
        assert "NetDebt+ScenarioBuybackCash-ScenarioDebtPaydown" in str(valuation["E260"].value)
        assert "NetIncome_TTM" in str(valuation["E261"].value)
        assert "Adj_EBITDA" not in str(valuation["E242"].value)
        assert "ScenarioAdjustedMargin" in str(valuation["E243"].value)
        assert "ResolvedRevenueGrowth_Custom" in str(valuation["E241"].value)
        assert "ScenarioGrowth" not in str(valuation["E241"].value)

        n244 = str(valuation["N244"].value)
        assert all(token in n244 for token in (
            "NOT(ISNUMBER(N237))",
            "NOT(ISNUMBER(DCF_WACC))",
            "NOT(ISNUMBER(DCF_FCFF))",
            'IF(N237+DCF_FCFF=0,""',
        ))
        n261 = str(valuation["N261"].value)
        assert all(token in n261 for token in (
            "NOT(ISNUMBER(ScenarioFCF))",
            "NOT(ISNUMBER(ScenarioImpliedPrice))",
            "NOT(ISNUMBER(ScenarioShares))",
            "ScenarioImpliedPrice<=0",
            "ScenarioShares<=0",
        ))
        assert str(investment_case["B68"].value) == '=IF(ISNUMBER(\'Valuation\'!N244),\'Valuation\'!N244,"")'

        assert "ResolvedRevenueGrowth_Bear" in str(investment_case["B85"].value)
        assert "ResolvedRevenueGrowth_Base" in str(investment_case["B86"].value)
        assert "ResolvedRevenueGrowth_Bull" in str(investment_case["B87"].value)

        source = (ROOT / "pbi_xbrl" / "standard_template_formula_contract.py").read_text(encoding="utf-8")
        assert "MAX(0.001" not in source
        assert "ScenarioBuyback_m" not in source
        assert "0.10,0.11,0.12" not in source

        for coordinate in ("B23", "D42", "B160", "A161", "B171", "A172", "B177", "A178"):
            assert investment_case[coordinate].value is None
            assert investment_case[coordinate].protection.locked is False
        for coordinate in ("B15", "D22", "B50", "D53", "B56", "D58", "B85", "J87", "B161", "D180"):
            assert isinstance(investment_case[coordinate].value, str) and investment_case[coordinate].value.startswith("=")
            assert investment_case[coordinate].protection.locked is True
    finally:
        wb.close()


def test_scenario_defined_names_validations_and_support_projections_are_complete() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        expected_names = {
            "NetIncome_TTM": "'Valuation'!$D$217",
            "DCF_Horizon": "'Valuation'!$D$220",
            "ScenarioTaxTreatment": "'Valuation'!$E$247",
            "ScenarioHorizon": "'Valuation'!$J$236",
            "ScenarioInterestTaxTreatment": "'Valuation'!$E$248",
            "ScenarioBuybackCash": "'Valuation'!$D$253",
            "ScenarioShares": "'Valuation'!$E$259",
            "ScenarioNetDebt": "'Valuation'!$E$260",
            "ScenarioImpliedPrice": "'Valuation'!$J$261",
            "ResolvedRevenueGrowth_Bear": "'Valuation_Summary'!$H$2",
            "ResolvedRevenueGrowth_Base": "'Valuation_Summary'!$I$2",
            "ResolvedRevenueGrowth_Bull": "'Valuation_Summary'!$J$2",
            "ResolvedRevenueGrowth_Custom": "'Valuation_Summary'!$K$2",
        }
        for name, target in expected_names.items():
            assert wb.defined_names[name].attr_text == target

        route_support = wb["Valuation_Summary"]
        for coordinate in ("H2", "I2", "J2", "K2"):
            formula = str(route_support[coordinate].value or "")
            assert formula.startswith("=_xlfn.LET(")
            assert "selected_growth_route" in formula
            assert "profile_driver_bridge" in formula
            assert '=\"revenue_growth\"' in formula
            assert formula.count('=\"total_company\"') == 4
            assert "LOWER(" not in formula
            assert "SUBSTITUTE(" not in formula
            assert "TRIM(" not in formula
            assert "_xlpm.directCount+_xlpm.profileCount+_xlpm.userCount<>1" in formula
            assert "_xlpm.bridgeCount=1" in formula
            assert route_support[coordinate].protection.locked is True
        assert "retail_operating_pack" not in str(route_support["I2"].value)

        valuation = wb["Valuation"]
        validation_targets = {str(validation.sqref) for validation in valuation.data_validations.dataValidation}
        assert {"D194", "D208:D209", "D210", "D213", "D214", "D215", "D216", "D218:D219", "D220", "J218", "J219:J220", "J221", "H226:L226", "G227:G234", "E236", "E237:E239", "E240", "J236", "D247", "E247", "D248", "E248", "D249:D250", "D253", "D254", "D255:D256"} == validation_targets

        summary = wb["Valuation_Summary"]
        assert summary["A2"].value == "price"
        assert summary["B20"].value == '=IF(ScenarioUpside="","",ScenarioUpside)'
        assert summary["F20"].value == '=IF(B20="","unavailable","calculated")'
        assert summary["B20"].protection.locked is True

        grid = wb["Valuation_Grid"]
        assert grid["A2"].value == '="dcf"'
        assert "'Valuation'!$H$227" in str(grid["E2"].value)
        assert grid["D42"].value == "scenario_implied_price"
        assert grid["E42"].value == '=IF(ScenarioImpliedPrice="","",ScenarioImpliedPrice)'
    finally:
        wb.close()


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
                assert ws.cell(row, column).protection.locked is True
    finally:
        wb.close()


def test_user_input_contract_is_the_exact_visible_edit_surface() -> None:
    wb = load_workbook(SHELL, data_only=False, read_only=False)
    try:
        editable = {
            (ws.title, cell.coordinate)
            for ws in wb.worksheets
            if ws.sheet_state == "visible"
            for cell in ws._cells.values()
            if cell.protection.locked is False
        }
        expected = set()
        for contract in USER_INPUT_CONTRACTS:
            min_col, min_row, max_col, max_row = range_boundaries(contract.target)
            expected.update(
                (contract.sheet, cell.coordinate)
                for row in wb[contract.sheet].iter_rows(
                    min_row=min_row,
                    max_row=max_row,
                    min_col=min_col,
                    max_col=max_col,
                )
                for cell in row
            )
        assert editable == expected
        assert sum(sheet == "Valuation" for sheet, _cell in editable) == 44
        assert sum(sheet == "{ticker}_Investment_Case" for sheet, _cell in editable) == 78
        assert len(editable) == 122
        assert all(ws.protection.sheet for ws in wb.worksheets)
    finally:
        wb.close()
