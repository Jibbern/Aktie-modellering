from __future__ import annotations

import copy
from collections import Counter
from pathlib import Path

from openpyxl import load_workbook

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from scripts.build_anf_new_ticker_parity_matrix import build_parity_matrix


ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "docs" / "anf_new_ticker_parity_matrix.json"
SCHEMA = ROOT / "docs" / "anf_new_ticker_parity_matrix.schema.json"
SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"
DATA_ROOT = ROOT.parents[2] / "StockModelData"
ANF_DIR = DATA_ROOT / "outputs" / "stress_tests" / "ANF_new_ticker_engine"
PACKAGE = ANF_DIR / "ANF_normalized_data_package.json"
PLAN = ANF_DIR / "ANF_binding_plan.json"
LEGACY = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"


def _matrix() -> dict:
    return load_json_strict(MATRIX)


def test_parity_matrix_is_schema_valid_and_has_unique_business_keys() -> None:
    matrix = _matrix()
    assert validate_json_schema(matrix, load_json_strict(SCHEMA)) == []
    parity_ids = [row["parity_id"] for row in matrix["entries"]]
    assert len(parity_ids) == len(set(parity_ids))
    assert matrix["summary"]["entry_count"] == len(parity_ids)


def test_all_available_required_items_are_reproduced() -> None:
    matrix = _matrix()
    reproduced_statuses = {"reproduced_correctly", "reproduced_with_improved_wording"}
    missing = [
        row
        for row in matrix["entries"]
        if row["parity_requirement"] == "must_reproduce"
        and row["current_status"] not in reproduced_statuses
    ]
    assert missing == []
    assert matrix["summary"]["required_missing_count"] == 0
    assert {
        row["inventory_origin"]
        for row in matrix["entries"]
        if row["parity_requirement"] == "must_reproduce"
    } <= {
        "legacy_workbook_business_key",
        "legacy_visible_display_contract",
        "source_evidence_business_key",
    }
    assert matrix["summary"]["independent_source_fact_reproduced_count"] == matrix["summary"]["independent_source_fact_count"]


def test_promise_progress_legacy_aliases_and_visible_row_routes_are_reproduced() -> None:
    rows = {
        row["parity_id"]: row
        for row in _matrix()["entries"]
        if row["domain"] == "promise_progress"
    }
    expected_fy2025 = {
        "promise-progress:revenue:FY2025",
        "promise-progress:operating_margin:FY2025",
        "promise-progress:adjusted_eps:FY2025",
        "promise-progress:capital_expenditures:FY2025",
        "promise-progress:diluted_shares:FY2025",
        "promise-progress:real_estate_activity:FY2025",
        "promise-progress:share_repurchases:FY2025",
        "promise-progress:tariffs:FY2025",
    }
    assert expected_fy2025 <= rows.keys()
    for parity_id in expected_fy2025:
        row = rows[parity_id]
        assert row["current_status"] == "reproduced_correctly", parity_id
        assert row["disposition"] == "visible", parity_id
        assert row["expected_new_workbook_destination"], parity_id
        assert all(
            destination.startswith("Promise_Progress_UI!")
            for destination in row["expected_new_workbook_destination"]
        ), parity_id

    old_revenue = rows["promise-progress:revenue:FY2020"]
    assert old_revenue["current_status"] == "explicitly_rejected_with_evidence"
    assert old_revenue["disposition"] == "rejected_with_evidence"
    assert old_revenue["dimensions"]["legacy_occurrence_count"] == 1
    assert old_revenue["dimensions"]["source_refs"]
    assert "reopened-store sales productivity" in old_revenue["rejection_reason"]
    assert old_revenue["expected_new_workbook_destination"] == []


def test_promise_progress_parity_reports_explicit_key_and_occurrence_dispositions() -> None:
    matrix = _matrix()
    rows = {
        row["parity_id"]: row
        for row in matrix["entries"]
        if row["domain"] == "promise_progress"
    }

    assert matrix["summary"]["promise_progress_key_disposition_counts"] == {
        "audit_only": 6,
        "duplicate_superseded": 1,
        "rejected_with_evidence": 5,
        "visible_reproduced": 17,
    }
    assert matrix["summary"]["promise_progress_occurrence_disposition_counts"] == {
        "audit_only_historical_evidence": 9,
        "duplicate_or_superseded_evidence": 5,
        "rejected_with_evidence": 12,
    }
    assert rows["promise-progress:capital_expenditures:FY2020"]["current_status"] == "audit_only_evidence_preserved"
    assert rows["promise-progress:operating_margin:FY2023"]["current_status"] == "duplicate_or_superseded_evidence_preserved"
    assert rows["promise-progress:tariffs:FY2019"]["current_status"] == "explicitly_rejected_with_evidence"
    assert not any(row["disposition"] == "missing" for row in rows.values())


def test_parity_inventory_is_legacy_first_and_keeps_fy2018_fy2019_and_older_history() -> None:
    matrix = _matrix()
    assert matrix["inventory_method"].startswith("legacy workbook business keys are inventoried first")
    annual_rows = [
        row
        for row in matrix["entries"]
        if row["parity_id"].startswith("legacy-annual:") and row["inventory_class"] == "source_fact"
    ]
    assert {"2018-FY", "2019-FY"} <= {row["period"] for row in annual_rows}
    assert {"2015-FY", "2016-FY", "2017-FY"} <= {row["period"] for row in annual_rows}
    assert all(row["inventory_origin"] == "legacy_workbook_business_key" for row in annual_rows)

    for period in ("2018-FY", "2019-FY"):
        revenue = next(
            row
            for row in annual_rows
            if row["period"] == period and row["metric_business_meaning"] == "revenue"
        )
        assert revenue["legacy_sheet_range"].startswith("ANF_model.xlsx!History_Q!")
        assert revenue["comparison_result"] == "value_match"

    older = [row for row in annual_rows if row["period"] in {"2015-FY", "2016-FY", "2017-FY"}]
    assert older
    assert all(row["disposition"] in {"audit_only", "formula_owned", "explicitly_excluded"} for row in older)
    assert all(not row["expected_new_workbook_destination"] for row in older)


def test_removing_an_annual_package_row_cannot_remove_the_legacy_parity_item() -> None:
    package = copy.deepcopy(load_json_strict(PACKAGE))
    package["annual_financials"]["rows"] = [
        row for row in package["annual_financials"]["rows"] if row["period"] != "2018-FY"
    ]
    matrix = build_parity_matrix(
        package=package,
        plan=load_json_strict(PLAN),
        legacy_path=LEGACY,
        shell_path=SHELL,
        binding_path=BINDING_MAP,
    )
    row = next(
        row
        for row in matrix["entries"]
        if row["parity_id"] == "legacy-annual:2018-FY:revenue"
    )
    assert row["normalized_package_path"] == "annual_financials.rows[missing:2018-FY].revenue"
    assert row["comparison_result"] == "missing_normalized_fact"
    assert row["current_status"] == "missing_or_explicitly_unavailable"


def test_quarterly_and_annual_core_financial_minimums_are_locked() -> None:
    matrix = _matrix()
    reproduced = Counter(
        (row["domain"], row["metric_business_meaning"])
        for row in matrix["entries"]
        if row["current_status"] == "reproduced_correctly"
    )
    for metric in (
        "revenue",
        "gross_profit",
        "operating_income",
        "base_ebitda",
        "adjusted_ebitda",
        "net_income",
    ):
        assert reproduced[("quarterly_financials", metric)] >= 12, metric
    for metric in ("operating_cash_flow", "capital_expenditures"):
        assert reproduced[("cash_flow", metric)] >= 12, metric
    assert reproduced[("per_share", "diluted_shares")] >= 12
    assert reproduced[("per_share", "eps")] >= 9
    assert reproduced[("per_share", "adjusted_eps")] >= 11
    for metric in ("revenue", "gross_profit", "operating_income", "net_income"):
        assert reproduced[("annual_financials", metric)] >= 8, metric
    for metric in ("base_ebitda", "operating_cash_flow", "capital_expenditures"):
        assert reproduced[("annual_financials", metric)] >= 6, metric
    assert reproduced[("annual_financials", "adjusted_ebitda")] >= 2
    assert reproduced[("annual_financials", "diluted_shares")] == 0
    assert reproduced[("annual_financials", "eps")] == 0


def test_source_backed_required_items_have_lineage_and_exact_destinations() -> None:
    for row in _matrix()["entries"]:
        if row["parity_requirement"] != "must_reproduce" or row["source_backed_vs_derived"] != "source_backed":
            continue
        assert row["source_ref"], row["parity_id"]
        assert row["normalized_package_path"], row["parity_id"]
        if not row["expected_new_workbook_destination"]:
            assert row["disposition"] in {
                "audit_only",
                "formula_owned",
                "explicitly_excluded",
                "history",
                "superseded",
            }, row["parity_id"]


def test_valuation_input_parity_covers_actual_optional_and_user_input_contracts() -> None:
    rows = {
        row["normalized_package_path"]: row
        for row in _matrix()["entries"]
        if row["domain"] == "valuation_inputs"
    }
    required_destinations = {
        "valuation_inputs.operating_cash_flow_ttm": "Valuation!D202",
        "valuation_inputs.capex_ttm": "Valuation!D211",
    }
    for path, destination in required_destinations.items():
        assert rows[path]["parity_requirement"] == "must_reproduce"
        assert rows[path]["current_status"] == "reproduced_correctly"
        assert rows[path]["expected_new_workbook_destination"] == [destination]

    for path in (
        "valuation_inputs.shares_outstanding",
        "valuation_inputs.net_debt",
        "valuation_inputs.adjusted_eps_ttm",
        "valuation_inputs.book_value_per_share",
        "valuation_inputs.tangible_book_value_per_share",
        "valuation_inputs.interest_paid_ttm",
    ):
        assert rows[path]["parity_requirement"] == "unavailable_missing_evidence"
        assert rows[path]["current_status"] == "missing_or_explicitly_unavailable"

    for path in (
        "valuation_inputs.price",
        "valuation_inputs.adjusted_fcf_ttm",
        "valuation_inputs.target_ev_adjusted_ebitda",
        "valuation_inputs.target_ev_ebitda",
        "valuation_inputs.target_ev_yield",
        "valuation_inputs.maintenance_capex_ratio",
        "valuation_inputs.recurring_cash_costs",
        "valuation_inputs.working_capital_normalization",
        "valuation_inputs.per_share_denominator",
    ):
        assert rows[path]["parity_requirement"] == "intentionally_rejected"
        assert rows[path]["current_status"] == "missing_or_explicitly_unavailable"
        assert rows[path]["rejection_reason"]


def test_formula_improvements_exist_in_protected_cells() -> None:
    wb = load_workbook(SHELL, read_only=False, data_only=False)
    try:
        for row in _matrix()["entries"]:
            if row["inventory_class"] != "formula_improvement":
                continue
            assert len(row["expected_new_workbook_destination"]) == 1
            sheet, coordinate = row["expected_new_workbook_destination"][0].split("!", 1)
            cell = wb[sheet][coordinate]
            assert isinstance(cell.value, str) and cell.value.startswith("="), row["parity_id"]
            assert cell.protection.locked is True
            assert row["formula_contract_status"] == "present_protected"
            assert row["economic_calculability"] in {
                "economically_calculable",
                "blank_due_to_missing_evidence",
            }
            assert row["calculation_reason"]
    finally:
        wb.close()

    formula_rows = [row for row in _matrix()["entries"] if row["inventory_class"] == "formula_improvement"]
    assert any(row["economic_calculability"] == "economically_calculable" for row in formula_rows)
    assert any(row["economic_calculability"] == "blank_due_to_missing_evidence" for row in formula_rows)
    assert all(
        row["current_status"]
        == (
            "reproduced_correctly"
            if row["economic_calculability"] == "economically_calculable"
            else "contract_present_blank_by_missing_evidence"
        )
        for row in formula_rows
    )


def test_legacy_cogs_tax_da_and_operating_margin_are_explicitly_classified() -> None:
    matrix = _matrix()
    for metric in ("cost_of_goods_sold", "income_taxes_paid", "depreciation_amortization"):
        rows = [
            row
            for row in matrix["entries"]
            if row["metric_business_meaning"] == metric and row["inventory_class"] == "source_fact"
        ]
        assert rows, metric
        assert all(row["inventory_origin"] == "legacy_workbook_business_key" for row in rows)
        assert all(row["disposition"] == "audit_only" for row in rows)

    margin_rows = [
        row
        for row in matrix["entries"]
        if row["parity_id"].startswith("legacy-quarter:")
        and row["metric_business_meaning"] == "operating_margin"
    ]
    assert margin_rows
    assert all(row["inventory_class"] == "source_fact" for row in margin_rows)
    assert all(row["source_backed_vs_derived"] == "derived" for row in margin_rows)
    assert all(row["disposition"] == "formula_owned" for row in margin_rows)
    assert all(row["formula_contract_status"] == "not_applicable" for row in margin_rows)

    formula_rows = [
        row
        for row in matrix["entries"]
        if row["parity_id"].startswith("formula:operating_margin:")
    ]
    assert formula_rows
    assert all(row["formula_contract_status"] == "present_protected" for row in formula_rows)


def test_missing_fail_zero_placeholders_are_unavailable_and_make_formulas_blank() -> None:
    matrix = _matrix()
    by_id = {row["parity_id"]: row for row in matrix["entries"]}
    unsupported_quarterly = {
        "total_debt": {"2024-Q2", "2024-Q3", "2024-Q4"},
        "debt_core": {"2024-Q2", "2024-Q3", "2024-Q4", "2025-Q2", "2025-Q3", "2025-Q4"},
        "interest_paid": {"2024-Q3", "2024-Q4", "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"},
    }
    for metric, periods in unsupported_quarterly.items():
        for period in periods:
            row = by_id[f"legacy-quarter:{period}:{metric}"]
            assert row["parity_requirement"] == "unavailable_missing_evidence"
            assert row["inventory_class"] == "unsupported_legacy_content"
            assert row["inventory_origin"] == "legacy_report_quality_check"
            assert row["current_status"] == "missing_or_explicitly_unavailable"
            assert row["comparison_result"] == "unsupported_zero_placeholder_left_blank"
            assert row["expected_new_workbook_destination"] == []
            assert "Source=Missing or QA=FAIL" in row["rejection_reason"]

    for period, metric in (
        ("2024-FY", "total_debt"),
        ("2024-FY", "debt_core"),
        ("2024-FY", "interest_paid"),
        ("2025-FY", "debt_core"),
        ("2025-FY", "interest_paid"),
    ):
        row = by_id[f"legacy-annual:{period}:{metric}"]
        assert row["parity_requirement"] == "unavailable_missing_evidence"
        assert row["inventory_class"] == "unsupported_legacy_content"
        assert row["comparison_result"] == "unsupported_zero_placeholder_left_blank"
        assert row["expected_new_workbook_destination"] == []

    for period in ("2024-Q2", "2024-Q3", "2024-Q4", "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"):
        if f"formula:net_debt:{period}" in by_id:
            assert by_id[f"formula:net_debt:{period}"]["economic_calculability"] == "blank_due_to_missing_evidence"
    for period in ("2024-Q3", "2024-Q4", "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"):
        assert by_id[f"formula:cash_interest_coverage:{period}"]["economic_calculability"] == "blank_due_to_missing_evidence"
    for period in ("2024-FY", "2025-FY"):
        assert by_id[f"formula:annual_net_debt:{period}"]["economic_calculability"] == "blank_due_to_missing_evidence"


def test_segment_plan_preserves_dimension_identity() -> None:
    data_root = ROOT.parents[2] / "StockModelData"
    plan_path = data_root / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_binding_plan.json"
    plan = load_json_strict(plan_path)
    labels = {
        str(write["value"])
        for write in plan["planned_writes"]
        if write["binding_id"] in {"bs_segment_quarterly_rows", "bs_segment_annual_rows"}
        and str(write["target_cell"]).startswith("A")
    }
    assert {"Geography: Americas", "Geography: EMEA", "Geography: APAC"} <= labels
    assert {"Brand: Hollister", "Brand: Abercrombie", "Total Company"} <= labels


def test_segment_parity_is_exactly_inventoried_from_legacy_visible_cells() -> None:
    legacy = ROOT.parents[2] / "StockModelData" / "outputs" / "Excel stock models" / "ANF_model.xlsx"
    wb = load_workbook(legacy, read_only=True, data_only=True)
    try:
        ws = wb["BS_Segments"]
        expected = set()
        for period_type, header_row, member_rows, columns in (
            ("quarterly", 7, (61, 62, 63, 65, 66, 67), range(2, 14)),
            ("annual", 70, (72, 73, 74), range(2, 10)),
        ):
            for row_number in member_rows:
                member = str(ws.cell(row_number, 1).value or "")
                dimension = "geography" if member in {"Americas", "EMEA", "APAC"} else "brand" if member in {"Hollister", "Abercrombie"} else "total_company"
                for column in columns:
                    period = ws.cell(header_row, column).value
                    value = ws.cell(row_number, column).value
                    if period in (None, "") or not isinstance(value, (int, float)) or isinstance(value, bool):
                        continue
                    normalized_period = f"{int(period)}-FY" if period_type == "annual" else str(period)
                    expected.add((normalized_period, dimension, member, float(value)))
    finally:
        wb.close()

    actual = {
        (
            row["period"],
            row["dimensions"]["dimension"],
            row["dimensions"]["member"],
            float(row["legacy_value"]),
        )
        for row in _matrix()["entries"]
        if row["domain"] == "segments" and row["inventory_origin"] == "legacy_workbook_business_key"
    }
    assert len(expected) == 52
    assert actual == expected


def test_annual_eps_and_share_proxies_are_explicitly_rejected() -> None:
    rows = [
        row
        for row in _matrix()["entries"]
        if row["parity_id"].startswith("legacy-annual:")
        and row["metric_business_meaning"] in {"diluted_shares", "eps"}
    ]
    annual_periods = {
        row["period"]
        for row in _matrix()["entries"]
        if row["parity_id"].startswith("legacy-annual:")
    }
    assert len(rows) == 2 * len(annual_periods)
    assert {row["metric_business_meaning"] for row in rows} == {"diluted_shares", "eps"}
    assert all(row["parity_requirement"] == "unavailable_missing_evidence" for row in rows)
    assert all("Q4" in row["rejection_reason"] for row in rows)


def test_generic_formula_and_planner_modules_contain_no_anf_business_logic() -> None:
    generic_paths = (
        ROOT / "pbi_xbrl" / "standard_template_formula_contract.py",
        ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py",
        ROOT / "pbi_xbrl" / "new_ticker_value_filler.py",
    )
    for path in generic_paths:
        source = path.read_text(encoding="utf-8")
        assert "ANF_model.xlsx" not in source
        assert "Abercrombie" not in source
        assert "Hollister" not in source
