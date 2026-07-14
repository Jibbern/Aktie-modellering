"""Build the ANF legacy-oracle parity matrix for the generic new-ticker engine.

ANF is intentionally treated as a migration fixture.  The contract records
business keys, lineage, normalized paths, bindings, and generic formula
ownership; it never copies legacy workbook values into the frozen shell.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.standard_template_formula_contract import (
    ANNUAL_FORMULA_ROWS,
    FORMULA_CONTRACT_VERSION,
    FORMULA_ROWS,
    VALUATION_OUTPUT_FORMULA_CELLS,
    VALUATION_SIDECAR_FORMULA_CELLS,
)


DEFAULT_SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
DEFAULT_BINDINGS = ROOT / "docs" / "workbook_binding_map.json"
DEFAULT_OUTPUT_JSON = ROOT / "docs" / "anf_new_ticker_parity_matrix.json"
DEFAULT_OUTPUT_MD = ROOT / "docs" / "anf_new_ticker_parity_matrix.md"

LEGACY_REPORT_PLACEHOLDER_ROWS = {
    "total_debt": ("REPORT_BS_Q", "Total debt"),
    "debt_core": ("REPORT_BS_Q", "Debt core"),
    "interest_paid": ("REPORT_CF_Q", "Cash interest"),
}

QUARTERLY_CORE = {
    "revenue",
    "gross_profit",
    "operating_income",
    "base_ebitda",
    "adjusted_ebitda",
    "net_income",
    "operating_cash_flow",
    "capital_expenditures",
    "diluted_shares",
    "eps",
    "adjusted_eps",
}
QUARTERLY_BALANCE_SHEET = {
    "cash",
    "marketable_securities",
    "accounts_receivable",
    "inventory",
    "current_assets",
    "property_plant_equipment_net",
    "goodwill",
    "intangibles",
    "total_assets",
    "accounts_payable",
    "accrued_liabilities",
    "debt_current",
    "lease_liabilities_current",
    "current_liabilities",
    "debt_core",
    "lease_liabilities_noncurrent",
    "pension_obligation_net",
    "other_liabilities_noncurrent",
    "total_liabilities",
    "total_equity",
    "shares_outstanding",
}
ANNUAL_CORE = {
    "revenue",
    "gross_profit",
    "operating_income",
    "base_ebitda",
    "adjusted_ebitda",
    "net_income",
    "operating_cash_flow",
    "capital_expenditures",
    "diluted_shares",
    "total_equity",
    "cash",
    "debt_core",
}

# This map is the independent legacy inventory contract.  The keys are actual
# History_Q headers, not normalized-package field names.
LEGACY_HISTORY_METRICS: dict[str, tuple[str, str, str]] = {
    "revenue": ("revenue", "$m", "quarterly_financials"),
    "cogs": ("cost_of_goods_sold", "$m", "quarterly_financials"),
    "gross_profit": ("gross_profit", "$m", "quarterly_financials"),
    "op_income": ("operating_income", "$m", "quarterly_financials"),
    "net_income": ("net_income", "$m", "quarterly_financials"),
    "cfo": ("operating_cash_flow", "$m", "cash_flow"),
    "capex": ("capital_expenditures", "$m", "cash_flow"),
    "tax_paid": ("income_taxes_paid", "$m", "cash_flow"),
    "da": ("depreciation_amortization", "$m", "cash_flow"),
    "interest_paid": ("interest_paid", "$m", "cash_flow"),
    "interest_expense_net": ("interest_expense", "$m", "quarterly_financials"),
    "buybacks_cash": ("buybacks_cash", "$m", "cash_flow"),
    "dividends_cash": ("dividends_cash", "$m", "cash_flow"),
    "acquisitions_cash": ("acquisitions_cash", "$m", "cash_flow"),
    "debt_repayment": ("debt_repayment", "$m", "cash_flow"),
    "debt_issuance": ("debt_issuance", "$m", "cash_flow"),
    "cash": ("cash", "$m", "balance_sheet"),
    "short_term_investments": ("short_term_investments", "$m", "balance_sheet"),
    "assets": ("total_assets", "$m", "balance_sheet"),
    "liabilities": ("total_liabilities", "$m", "balance_sheet"),
    "assets_current": ("current_assets", "$m", "balance_sheet"),
    "liabilities_current": ("current_liabilities", "$m", "balance_sheet"),
    "accounts_receivable": ("accounts_receivable", "$m", "balance_sheet"),
    "inventory": ("inventory", "$m", "balance_sheet"),
    "accounts_payable_current": ("accounts_payable", "$m", "balance_sheet"),
    "accrued_liabilities_current": ("accrued_liabilities", "$m", "balance_sheet"),
    "debt_current": ("debt_current", "$m", "balance_sheet"),
    "property_plant_equipment_net": ("property_plant_equipment_net", "$m", "balance_sheet"),
    "other_assets_noncurrent": ("other_assets_noncurrent", "$m", "balance_sheet"),
    "other_liabilities_noncurrent": ("other_liabilities_noncurrent", "$m", "balance_sheet"),
    "total_equity": ("total_equity", "$m", "balance_sheet"),
    "goodwill": ("goodwill", "$m", "balance_sheet"),
    "intangibles": ("intangibles", "$m", "balance_sheet"),
    "shares_outstanding": ("shares_outstanding", "m shares", "per_share"),
    "pension_obligation_net": ("pension_obligation_net", "$m", "balance_sheet"),
    "total_debt": ("total_debt", "$m", "balance_sheet"),
    "debt_core": ("debt_core", "$m", "balance_sheet"),
    "lease_liabilities": ("lease_liabilities", "$m", "balance_sheet"),
    "shares_diluted": ("diluted_shares", "m shares", "per_share"),
    "ebitda": ("base_ebitda", "$m", "quarterly_financials"),
    "eps_diluted": ("eps", "$/share", "per_share"),
    "lease_liabilities_current": ("lease_liabilities_current", "$m", "balance_sheet"),
    "lease_liabilities_noncurrent": ("lease_liabilities_noncurrent", "$m", "balance_sheet"),
    "marketable_securities": ("marketable_securities", "$m", "balance_sheet"),
    "operating_margin": ("operating_margin", "%", "formula_derived_metrics"),
}

LEGACY_DERIVED_HISTORY_METRICS = {"operating_margin"}

ANNUAL_FLOW_FIELDS = {
    "revenue", "cost_of_goods_sold", "gross_profit", "operating_income", "base_ebitda", "net_income",
    "operating_cash_flow", "capital_expenditures", "interest_paid", "interest_expense",
    "income_taxes_paid", "depreciation_amortization", "buybacks_cash", "dividends_cash",
    "acquisitions_cash", "debt_repayment", "debt_issuance",
}
ANNUAL_POINT_IN_TIME_FIELDS = {
    "cash", "short_term_investments", "total_assets", "total_liabilities", "current_assets",
    "current_liabilities", "accounts_receivable", "inventory", "accounts_payable",
    "accrued_liabilities", "debt_current", "property_plant_equipment_net", "other_assets_noncurrent",
    "other_liabilities_noncurrent", "total_equity", "goodwill", "intangibles",
    "shares_outstanding", "pension_obligation_net", "total_debt", "debt_core",
    "lease_liabilities", "lease_liabilities_current", "lease_liabilities_noncurrent",
    "marketable_securities",
}

SCALAR_REQUIREMENTS = (
    ("summary", "ticker_metadata.company_name", "may_improve_semantically"),
    ("summary", "company_profile.description", "may_improve_semantically"),
    ("summary", "company_profile.strategic_context", "may_improve_semantically"),
    ("summary", "company_profile.revenue_mix_label", "may_improve_semantically"),
    ("summary", "debt_liquidity.summary_liquidity_display", "may_improve_semantically"),
    ("summary", "debt_liquidity.summary_liquidity_as_of_display", "may_improve_semantically"),
    ("investment_case", "ticker_metadata.investment_case_title", "may_improve_semantically"),
    ("investment_case", "investment_case.summary", "may_improve_semantically"),
    ("investment_case", "investment_case.key_debate", "may_improve_semantically"),
    ("investment_case", "investment_case.upside", "may_improve_semantically"),
    ("investment_case", "investment_case.downside", "may_improve_semantically"),
    ("valuation_inputs", "valuation_inputs.price", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.as_of_date", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.shares_outstanding", "unavailable_missing_evidence"),
    ("valuation_inputs", "valuation_inputs.diluted_shares", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.net_debt", "unavailable_missing_evidence"),
    ("valuation_inputs", "valuation_inputs.base_ebitda_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.adjusted_ebitda_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.revenue_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.operating_cash_flow_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.free_cash_flow_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.capex_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.eps_ttm", "must_reproduce"),
    ("valuation_inputs", "valuation_inputs.adjusted_eps_ttm", "unavailable_missing_evidence"),
    ("valuation_inputs", "valuation_inputs.book_value_per_share", "unavailable_missing_evidence"),
    ("valuation_inputs", "valuation_inputs.tangible_book_value_per_share", "unavailable_missing_evidence"),
    ("valuation_inputs", "valuation_inputs.interest_paid_ttm", "unavailable_missing_evidence"),
    ("valuation_inputs", "valuation_inputs.adjusted_fcf_ttm", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.target_ev_adjusted_ebitda", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.target_ev_ebitda", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.target_ev_yield", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.maintenance_capex_ratio", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.recurring_cash_costs", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.working_capital_normalization", "intentionally_rejected"),
    ("valuation_inputs", "valuation_inputs.per_share_denominator", "intentionally_rejected"),
)


def _default_data_root() -> Path:
    for parent in (ROOT, *ROOT.parents):
        candidate = parent / "StockModelData"
        if candidate.exists():
            return candidate
    return ROOT.parent / "StockModelData"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _path_get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _field_state(field: Any) -> tuple[Any, str, str]:
    if not isinstance(field, Mapping):
        return None, "missing_source", ""
    return field.get("value"), str(field.get("status") or "missing_source"), str(field.get("source_ref") or "")


def _scalar(value: Any) -> Any:
    return value.get("value") if isinstance(value, Mapping) else value


def _write_index(plan: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    result: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for write in plan.get("planned_writes") or []:
        if isinstance(write, Mapping):
            result[str(write.get("normalized_path") or "")].append(write)
    return result


def _destinations(writes: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({f"{row.get('target_sheet')}!{row.get('target_cell')}" for row in writes})


def _binding_ids(writes: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({str(row.get("binding_id") or "") for row in writes if row.get("binding_id")})


def _normalized_legacy_value(value: Any, unit: str) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    result = float(value)
    if unit in {"$m", "m shares"}:
        result /= 1_000_000
    return round(result, 6)


def _values_match(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float)) and not isinstance(left, bool) and isinstance(right, (int, float)) and not isinstance(right, bool):
        return abs(float(left) - float(right)) <= 1e-5
    return left == right


def _financial_dispositions(binding_document: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {
        (str(row.get("section") or ""), str(row.get("field") or "")): row
        for row in binding_document.get("financial_field_dispositions") or []
        if isinstance(row, Mapping)
    }


def _effective_disposition(
    *,
    static_disposition: Mapping[str, Any] | None,
    selector_exclusion: Mapping[str, Any] | None,
    writes: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any] | None, str]:
    if static_disposition is not None:
        return static_disposition, str(static_disposition.get("disposition") or "")
    if selector_exclusion is not None:
        return selector_exclusion, str(selector_exclusion.get("disposition") or "explicitly_excluded")
    if writes:
        return None, "planned_binding"
    return None, ""


def _legacy_history_rows(legacy_path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    wb = load_workbook(legacy_path, read_only=True, data_only=True)
    try:
        ws = wb["History_Q"]
        headers = {str(ws.cell(1, column).value or ""): column for column in range(1, ws.max_column + 1)}
        rows: list[dict[str, Any]] = []
        for row_number in range(2, ws.max_row + 1):
            period = str(ws.cell(row_number, headers.get("fiscal_label", 0)).value or "") if headers.get("fiscal_label") else ""
            fiscal_year = ws.cell(row_number, headers.get("fiscal_year", 0)).value if headers.get("fiscal_year") else None
            fiscal_quarter = ws.cell(row_number, headers.get("fiscal_quarter", 0)).value if headers.get("fiscal_quarter") else None
            if not period or not isinstance(fiscal_year, (int, float)) or not isinstance(fiscal_quarter, (int, float)):
                continue
            rows.append(
                {
                    "row_number": row_number,
                    "period": period,
                    "fiscal_year": int(fiscal_year),
                    "fiscal_quarter": int(fiscal_quarter),
                    "values": {header: ws.cell(row_number, column).value for header, column in headers.items()},
                }
            )
        rows.sort(key=lambda row: (int(row["fiscal_year"]), int(row["fiscal_quarter"])))
        return rows, headers
    finally:
        wb.close()


def _legacy_date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")[:10]


def _legacy_unsupported_zero_placeholders(
    legacy_path: Path,
    history_rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    period_by_end = {
        _legacy_date_key(row.get("values", {}).get("quarter")): str(row.get("period") or "")
        for row in history_rows
        if isinstance(row.get("values"), Mapping)
    }
    wb = load_workbook(legacy_path, read_only=True, data_only=True)
    try:
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for metric, (sheet_name, line_item) in LEGACY_REPORT_PLACEHOLDER_ROWS.items():
            if sheet_name not in wb.sheetnames:
                continue
            ws = wb[sheet_name]
            row_number = next(
                (
                    row
                    for row in range(4, ws.max_row + 1)
                    if str(ws.cell(row, 2).value or "").strip().casefold() == line_item.casefold()
                ),
                None,
            )
            if row_number is None:
                continue
            source_status = str(ws.cell(row_number, 3).value or "").strip()
            qa_status = str(ws.cell(row_number, 4).value or "").strip()
            if source_status.casefold() != "missing" and qa_status.casefold() != "fail":
                continue
            for column in range(7, ws.max_column + 1):
                value = ws.cell(row_number, column).value
                if (
                    not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or float(value) != 0.0
                ):
                    continue
                period_end = _legacy_date_key(ws.cell(3, column).value)
                period = period_by_end.get(period_end, "")
                if not period:
                    continue
                result[(metric, period)] = {
                    "metric": metric,
                    "period": period,
                    "period_end": period_end,
                    "line_item": line_item,
                    "source_status": source_status,
                    "qa_status": qa_status,
                    "value_source_ref": (
                        f"{legacy_path.name}!{sheet_name}!{get_column_letter(column)}{row_number}"
                    ),
                    "metadata_source_ref": f"{legacy_path.name}!{sheet_name}!C{row_number}:D{row_number}",
                }
        return result
    finally:
        wb.close()


def _package_row_index(package: Mapping[str, Any], section: str) -> dict[str, tuple[int, Mapping[str, Any]]]:
    return {
        str(row.get("period") or ""): (index, row)
        for index, row in enumerate(_path_get(package, f"{section}.rows") or [])
        if isinstance(row, Mapping)
    }


def _legacy_valuation_series(legacy_path: Path, row_number: int) -> dict[str, tuple[float, str]]:
    wb = load_workbook(legacy_path, read_only=True, data_only=True)
    try:
        ws = wb["Valuation"]
        result: dict[str, tuple[float, str]] = {}
        for column in range(2, 14):
            period = str(ws.cell(6, column).value or "")
            value = ws.cell(row_number, column).value
            if period and isinstance(value, (int, float)) and not isinstance(value, bool):
                result[period] = (float(value), f"{legacy_path.name}!Valuation!{get_column_letter(column)}{row_number}")
        return result
    finally:
        wb.close()


def _selector_exclusion_index(plan: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for binding in plan.get("bindings") or []:
        if not isinstance(binding, Mapping):
            continue
        for skipped in binding.get("skipped_rows") or []:
            if not isinstance(skipped, Mapping):
                continue
            reason = str(skipped.get("reason") or "")
            path = str(skipped.get("normalized_path") or "")
            if path and (
                reason.startswith("row_selector_")
                or reason == "period_axis_outside_visible_window"
                or reason.startswith("selector_exclusion_audit_only:")
            ):
                result.setdefault(path, skipped)
    return result


def _history_evidence_index(package: Mapping[str, Any]) -> tuple[dict[str, int], dict[tuple[str, str, int], float]]:
    periods: dict[str, int] = {}
    values: dict[tuple[str, str, int], float] = {}
    for item in _path_get(package, "calculation_history.quarterly_items") or []:
        if not isinstance(item, Mapping) or str(item.get("status") or "") != "populated":
            continue
        period = str(item.get("period") or "")
        metric = str(item.get("metric") or "")
        unit = str(item.get("unit") or "")
        ordinal = item.get("period_ordinal")
        value = item.get("value")
        source_ref = str(item.get("source_ref") or "")
        if not period or not metric or not unit or not source_ref:
            continue
        if not isinstance(ordinal, int) or not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        periods[period] = ordinal
        values[(metric, unit, ordinal)] = float(value)
    return periods, values


def _quarter_formula_calculability(
    formula_id: str,
    period: str,
    *,
    period_ordinals: Mapping[str, int],
    history_values: Mapping[tuple[str, str, int], float],
) -> tuple[bool, str]:
    ordinal = period_ordinals.get(period)
    if ordinal is None:
        return False, f"No source-backed calculation-history ordinal exists for {period}."

    def point(metric: str, unit: str, offset: int = 0, *, nonzero: bool = False) -> bool:
        value = history_values.get((metric, unit, ordinal + offset))
        return value is not None and (not nonzero or value != 0)

    def window(metric: str, unit: str, offset: int = 0, *, nonzero_total: bool = False) -> bool:
        values = [history_values.get((metric, unit, ordinal + offset - step)) for step in range(4)]
        return all(value is not None for value in values) and (not nonzero_total or sum(float(value) for value in values if value is not None) != 0)

    single_ttm = {
        "revenue_ttm": ("revenue", "$m"),
        "ebitda_ttm": ("base_ebitda", "$m"),
        "adjusted_ebitda_ttm": ("adjusted_ebitda", "$m"),
        "operating_income_ttm": ("operating_income", "$m"),
        "net_income_ttm": ("net_income", "$m"),
        "buybacks_ttm": ("buybacks_cash", "$m"),
        "dividends_ttm": ("dividends_cash", "$m"),
        "acquisitions_ttm": ("acquisitions_cash", "$m"),
        "debt_repayment_ttm": ("debt_repayment", "$m"),
        "debt_issuance_ttm": ("debt_issuance", "$m"),
        "gaap_eps_ttm": ("eps", "$/share"),
        "adjusted_eps_ttm": ("adjusted_eps", "$/share"),
        "ebitda_ttm_copy": ("base_ebitda", "$m"),
        "adjusted_ebitda_ttm_copy": ("adjusted_ebitda", "$m"),
        "operating_cash_flow_ttm": ("operating_cash_flow", "$m"),
    }
    if formula_id in single_ttm:
        metric, unit = single_ttm[formula_id]
        ok = window(metric, unit)
        return ok, "Four consecutive compatible source-backed quarters are available." if ok else f"Four consecutive {metric} quarters are unavailable."

    single_yoy = {
        "revenue_yoy": ("revenue", "$m"),
        "ebitda_yoy": ("base_ebitda", "$m"),
        "adjusted_ebitda_yoy": ("adjusted_ebitda", "$m"),
        "net_income_yoy": ("net_income", "$m"),
        "gaap_eps_yoy": ("eps", "$/share"),
    }
    if formula_id in single_yoy:
        metric, unit = single_yoy[formula_id]
        ok = point(metric, unit) and point(metric, unit, -4, nonzero=True)
        return ok, "Current and prior-year source-backed points are available." if ok else f"Current or prior-year {metric} is unavailable or has an invalid denominator."

    point_ratios = {
        "gross_margin": ("gross_profit", "revenue"),
        "operating_margin": ("operating_income", "revenue"),
        "ebitda_margin": ("base_ebitda", "revenue"),
        "adjusted_ebitda_margin": ("adjusted_ebitda", "revenue"),
        "operating_income_margin": ("operating_income", "revenue"),
        "net_margin": ("net_income", "revenue"),
        "capex_margin": ("capital_expenditures", "revenue"),
    }
    if formula_id in point_ratios:
        numerator, denominator = point_ratios[formula_id]
        ok = point(numerator, "$m") and point(denominator, "$m", nonzero=True)
        return ok, "Compatible current-period numerator and denominator are available." if ok else f"Current {numerator} or non-zero {denominator} is unavailable."

    ttm_ratios = {
        "operating_margin_ttm": ("operating_income", "revenue"),
        "ebitda_margin_ttm": ("base_ebitda", "revenue"),
        "adjusted_ebitda_margin_ttm": ("adjusted_ebitda", "revenue"),
        "operating_income_margin_ttm": ("operating_income", "revenue"),
        "net_margin_ttm": ("net_income", "revenue"),
        "capex_margin_ttm": ("capital_expenditures", "revenue"),
        "interest_coverage": ("operating_income", "interest_expense"),
        "cash_interest_coverage": ("base_ebitda", "interest_paid"),
    }
    if formula_id in ttm_ratios:
        numerator, denominator = ttm_ratios[formula_id]
        ok = window(numerator, "$m") and window(denominator, "$m", nonzero_total=True)
        return ok, "Both trailing-four-quarter series are complete and the denominator is non-zero." if ok else f"Complete compatible TTM {numerator}/{denominator} evidence is unavailable."

    if formula_id == "adjusted_ebitda_delta":
        ok = point("adjusted_ebitda", "$m") and point("base_ebitda", "$m")
    elif formula_id == "free_cash_flow":
        ok = point("operating_cash_flow", "$m") and point("capital_expenditures", "$m")
    elif formula_id == "free_cash_flow_yoy_delta":
        ok = all(point(metric, "$m", offset) for metric in ("operating_cash_flow", "capital_expenditures") for offset in (0, -4))
    elif formula_id == "free_cash_flow_ttm":
        ok = window("operating_cash_flow", "$m") and window("capital_expenditures", "$m")
    elif formula_id == "free_cash_flow_ttm_yoy_delta":
        ok = all(window(metric, "$m", offset) for metric in ("operating_cash_flow", "capital_expenditures") for offset in (0, -4))
    elif formula_id == "free_cash_flow_margin":
        ok = point("operating_cash_flow", "$m") and point("capital_expenditures", "$m") and point("revenue", "$m", nonzero=True)
    elif formula_id == "free_cash_flow_margin_ttm":
        ok = window("operating_cash_flow", "$m") and window("capital_expenditures", "$m") and window("revenue", "$m", nonzero_total=True)
    elif formula_id in {"net_debt", "core_net_cash"}:
        ok = point("debt_core", "$m") and point("cash", "$m")
    elif formula_id in {"net_debt_qoq", "diluted_shares_qoq"}:
        metric_set = (("debt_core", "$m"), ("cash", "$m")) if formula_id == "net_debt_qoq" else (("diluted_shares", "m shares"),)
        ok = all(point(metric, unit, offset) for metric, unit in metric_set for offset in (0, -1))
    elif formula_id in {"net_debt_yoy", "diluted_shares_yoy"}:
        metric_set = (("debt_core", "$m"), ("cash", "$m")) if formula_id == "net_debt_yoy" else (("diluted_shares", "m shares"),)
        ok = all(point(metric, unit, offset) for metric, unit in metric_set for offset in (0, -4))
    elif formula_id == "net_cash_with_securities":
        ok = all(point(metric, "$m") for metric in ("cash", "marketable_securities", "debt_core"))
    elif formula_id == "lease_adjusted_net_debt":
        ok = all(point(metric, "$m") for metric in ("cash", "debt_core", "lease_liabilities"))
    elif formula_id == "lease_adjusted_net_debt_with_securities":
        ok = all(point(metric, "$m") for metric in ("cash", "marketable_securities", "debt_core", "lease_liabilities"))
    elif formula_id == "net_leverage":
        ok = point("debt_core", "$m") and point("cash", "$m") and window("base_ebitda", "$m", nonzero_total=True)
    elif formula_id == "adjusted_net_leverage":
        ok = point("debt_core", "$m") and point("cash", "$m") and window("adjusted_ebitda", "$m", nonzero_total=True)
    elif formula_id == "fcf_conversion":
        ok = window("operating_cash_flow", "$m") and window("capital_expenditures", "$m") and window("base_ebitda", "$m", nonzero_total=True)
    elif formula_id == "book_value_per_share":
        ok = point("total_equity", "$m") and point("shares_outstanding", "m shares", nonzero=True)
    elif formula_id == "tangible_book_value_per_share":
        ok = all(point(metric, "$m") for metric in ("total_equity", "goodwill", "intangibles")) and point("shares_outstanding", "m shares", nonzero=True)
    elif formula_id == "free_cash_flow_per_share":
        ok = window("operating_cash_flow", "$m") and window("capital_expenditures", "$m") and point("diluted_shares", "m shares", nonzero=True)
    else:
        return False, f"No economic dependency contract is defined for formula {formula_id}."
    return ok, "All source-backed formula dependencies are available." if ok else "One or more source-backed formula dependencies are unavailable."


def _annual_formula_calculability(formula_id: str, period: str, annual_rows: Mapping[str, tuple[int, Mapping[str, Any]]]) -> tuple[bool, str]:
    row_info = annual_rows.get(period)
    if row_info is None:
        return False, f"No normalized annual row exists for {period}."
    row = row_info[1]

    def field(name: str, *, nonzero: bool = False) -> bool:
        value, status, source_ref = _field_state(row.get(name))
        return status == "populated" and bool(source_ref) and isinstance(value, (int, float)) and not isinstance(value, bool) and (not nonzero or value != 0)

    requirements = {
        "annual_gross_margin": (("gross_profit", False), ("revenue", True)),
        "annual_operating_margin": (("operating_income", False), ("revenue", True)),
        "annual_ebitda_margin": (("base_ebitda", False), ("revenue", True)),
        "annual_adjusted_ebitda_margin": (("adjusted_ebitda", False), ("revenue", True)),
        "annual_net_margin": (("net_income", False), ("revenue", True)),
        "annual_free_cash_flow": (("operating_cash_flow", False), ("capital_expenditures", False)),
        "annual_free_cash_flow_margin": (("operating_cash_flow", False), ("capital_expenditures", False), ("revenue", True)),
        "annual_book_value_per_share": (("total_equity", False), ("shares_outstanding", True)),
        "annual_net_debt": (("debt_core", False), ("cash", False)),
    }
    required = requirements.get(formula_id)
    if required is None:
        return False, f"No annual dependency contract is defined for formula {formula_id}."
    ok = all(field(name, nonzero=nonzero) for name, nonzero in required)
    return ok, "All source-backed annual formula dependencies are available." if ok else "One or more annual formula dependencies are unavailable."


VALUATION_OUTPUT_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "N194": ("price", "shares_outstanding"),
    "N195": ("price", "shares_outstanding", "net_debt"),
    "N196": ("price", "shares_outstanding", "net_debt", "adjusted_ebitda_ttm"),
    "N197": ("price", "shares_outstanding", "net_debt", "base_ebitda_ttm"),
    "N198": ("free_cash_flow_ttm", "interest_paid_ttm"),
    "N199": ("free_cash_flow_ttm", "interest_paid_ttm", "price", "shares_outstanding", "net_debt"),
    "N200": ("free_cash_flow_ttm", "price", "shares_outstanding"),
    "N201": ("free_cash_flow_ttm", "capex_ttm", "maintenance_capex_ratio", "recurring_cash_costs", "working_capital_normalization"),
    "N202": ("free_cash_flow_ttm", "capex_ttm", "maintenance_capex_ratio", "recurring_cash_costs", "working_capital_normalization", "price", "shares_outstanding", "net_debt"),
    "N203": ("target_ev_adjusted_ebitda", "adjusted_ebitda_ttm", "net_debt", "per_share_denominator"),
    "N204": ("target_ev_ebitda", "base_ebitda_ttm", "net_debt", "per_share_denominator"),
    "N205": ("target_ev_yield", "free_cash_flow_ttm", "interest_paid_ttm", "net_debt", "per_share_denominator"),
    "N206": ("price", "eps_ttm"),
    "N207": ("price", "adjusted_eps_ttm"),
    "N208": ("price", "shares_outstanding", "net_debt", "revenue_ttm"),
    "N209": ("price", "book_value_per_share"),
    "N210": ("price", "tangible_book_value_per_share"),
    "U64": ("adjusted_ebitda_ttm",),
    "U65": ("free_cash_flow_ttm",),
    "U66": ("eps_ttm",),
    "U67": ("adjusted_ebitda_ttm", "target_ev_adjusted_ebitda"),
    "U68": ("adjusted_ebitda_ttm", "target_ev_adjusted_ebitda", "net_debt"),
    "U69": ("base_ebitda_ttm", "target_ev_ebitda"),
    "U70": ("base_ebitda_ttm", "target_ev_ebitda", "net_debt"),
    "U73": ("target_ev_adjusted_ebitda", "adjusted_ebitda_ttm", "net_debt", "per_share_denominator"),
    "U74": ("target_ev_ebitda", "base_ebitda_ttm", "net_debt", "per_share_denominator"),
    "U75": ("target_ev_yield", "free_cash_flow_ttm", "interest_paid_ttm", "net_debt", "per_share_denominator"),
}


def _valuation_output_calculability(coordinate: str, package: Mapping[str, Any]) -> tuple[bool, str]:
    required = VALUATION_OUTPUT_DEPENDENCIES.get(coordinate)
    if required is None:
        return False, f"No valuation-output dependency contract is defined for {coordinate}."
    missing: list[str] = []
    for field_name in required:
        value, status, source_ref = _field_state(_path_get(package, f"valuation_inputs.{field_name}"))
        if status != "populated" or value in (None, "") or not source_ref:
            missing.append(field_name)
    if missing:
        return False, "Missing normalized valuation inputs: " + ", ".join(missing)
    return True, "All normalized valuation-output dependencies are available."


def _source_fact_status(
    *,
    legacy_value: Any,
    field: Any,
    writes: Sequence[Mapping[str, Any]],
    disposition: Mapping[str, Any] | None,
) -> tuple[str, str, Any]:
    normalized_value, status, _ = _field_state(field)
    comparison = "missing_normalized_fact"
    if status == "populated" and _values_match(legacy_value, normalized_value):
        comparison = "value_match"
    elif status == "populated":
        comparison = "value_mismatch"
    accounted = bool(writes) or disposition is not None
    reproduced = comparison == "value_match" and accounted
    return ("reproduced_correctly" if reproduced else "missing_or_explicitly_unavailable", comparison, normalized_value)


def _entry(
    *,
    parity_id: str,
    domain: str,
    metric: str,
    period: str,
    dimensions: Mapping[str, Any] | None,
    legacy_range: str,
    source_kind: str,
    normalized_path: str,
    requirement: str,
    minimum: int,
    writes: Sequence[Mapping[str, Any]],
    source_ref: str = "",
    formula_cell: str = "",
    formula_present: bool | None = None,
    formula_protected: bool | None = None,
    economically_calculable: bool | None = None,
    calculation_reason: str = "",
    inventory_class: str = "source_fact",
    inventory_origin: str = "legacy_workbook_business_key",
    legacy_value: Any = None,
    normalized_value: Any = None,
    unit: str = "",
    comparison_result: str = "",
    disposition: str = "",
    current_status: str | None = None,
    rejection_reason: str = "",
) -> dict[str, Any]:
    destinations = _destinations(writes)
    if inventory_class == "formula_improvement":
        formula_contract_status = (
            "present_protected"
            if formula_present and formula_protected
            else "present_unprotected"
            if formula_present
            else "missing"
        )
        economic_status = "economically_calculable" if economically_calculable else "blank_due_to_missing_evidence"
        if formula_contract_status != "present_protected":
            formula_status = "missing_or_explicitly_unavailable"
        elif economically_calculable:
            formula_status = "reproduced_correctly"
        else:
            formula_status = "contract_present_blank_by_missing_evidence"
        reproduced = formula_status == "reproduced_correctly"
    else:
        formula_contract_status = "not_applicable"
        economic_status = "not_applicable"
        formula_status = "reproduced_correctly" if destinations else "missing_or_explicitly_unavailable"
        reproduced = bool(destinations)
    resolved_status = current_status or formula_status
    return {
        "parity_id": parity_id,
        "domain": domain,
        "metric_business_meaning": metric,
        "period": period,
        "dimensions": dict(dimensions or {}),
        "legacy_sheet_range": legacy_range,
        "source_backed_vs_derived": source_kind,
        "inventory_class": inventory_class,
        "inventory_origin": inventory_origin,
        "legacy_value": legacy_value,
        "normalized_value": normalized_value,
        "unit": unit,
        "comparison_result": comparison_result,
        "formula_contract_status": formula_contract_status,
        "economic_calculability": economic_status,
        "calculation_reason": calculation_reason,
        "disposition": disposition,
        "source_ref": source_ref,
        "normalized_package_path": normalized_path,
        "binding_ids": _binding_ids(writes),
        "formula_ownership": formula_cell or ("formula_owned" if disposition == "formula_owned" else "value_only_binding"),
        "expected_new_workbook_destination": destinations or ([formula_cell] if formula_cell else []),
        "minimum_coverage_requirement": minimum,
        "parity_requirement": requirement,
        "rejection_reason": rejection_reason or (
            "User input or unsupported legacy assumption lacks reproducible source lineage."
            if requirement == "intentionally_rejected"
            else ""
        ),
        "current_status": resolved_status,
    }


def build_parity_matrix(
    *,
    package: Mapping[str, Any],
    plan: Mapping[str, Any],
    legacy_path: Path,
    shell_path: Path,
    binding_path: Path,
) -> dict[str, Any]:
    if str(plan.get("status") or "") != "PASS":
        raise ValueError("Parity matrix requires the current blocker-free PASS plan.")
    writes_by_path = _write_index(plan)
    entries: list[dict[str, Any]] = []

    binding_document = load_json_strict(binding_path)
    dispositions = _financial_dispositions(binding_document)
    selector_exclusions = _selector_exclusion_index(plan)
    history_rows, history_headers = _legacy_history_rows(legacy_path)
    unsupported_zero_placeholders = _legacy_unsupported_zero_placeholders(legacy_path, history_rows)
    quarterly_package_rows = _package_row_index(package, "quarterly_financials")
    annual_package_rows = _package_row_index(package, "annual_financials")
    history_period_ordinals, history_values = _history_evidence_index(package)

    # Inventory starts with the latest twelve legacy business periods, not the
    # package.  A missing package row therefore remains visible as a parity gap.
    for legacy_row in history_rows[-12:]:
        period = str(legacy_row["period"])
        package_row_info = quarterly_package_rows.get(period)
        for legacy_header, (metric, unit, domain) in LEGACY_HISTORY_METRICS.items():
            legacy_value = _normalized_legacy_value(legacy_row["values"].get(legacy_header), unit)
            if legacy_value is None:
                continue
            if package_row_info is None:
                index, package_row = -1, {}
                path = f"quarterly_financials.rows[missing:{period}].{metric}"
            else:
                index, package_row = package_row_info
                path = f"quarterly_financials.rows.{index}.{metric}"
            writes = writes_by_path.get(path, [])
            disposition, disposition_name = _effective_disposition(
                static_disposition=dispositions.get(("quarterly_financials", metric)),
                selector_exclusion=selector_exclusions.get(path),
                writes=writes,
            )
            placeholder = unsupported_zero_placeholders.get((metric, period))
            if placeholder is not None:
                normalized_value, normalized_status, _ = _field_state(package_row.get(metric))
                report_refs = " + ".join(
                    filter(
                        None,
                        (
                            str(placeholder.get("value_source_ref") or ""),
                            str(placeholder.get("metadata_source_ref") or ""),
                        ),
                    )
                )
                column = history_headers[legacy_header]
                history_ref = f"{legacy_path.name}!History_Q!{get_column_letter(column)}{legacy_row['row_number']}"
                entries.append(
                    _entry(
                        parity_id=f"legacy-quarter:{period}:{metric}",
                        domain=domain,
                        metric=metric,
                        period=period,
                        dimensions={},
                        legacy_range=f"{history_ref} + {report_refs}",
                        source_kind="unavailable",
                        normalized_path=path,
                        requirement="unavailable_missing_evidence",
                        minimum=1,
                        writes=writes,
                        source_ref=report_refs,
                        inventory_class="unsupported_legacy_content",
                        inventory_origin="legacy_report_quality_check",
                        legacy_value=None,
                        normalized_value=normalized_value,
                        unit=unit,
                        comparison_result=(
                            "unsupported_zero_placeholder_left_blank"
                            if normalized_status != "populated"
                            else "unsupported_zero_placeholder_incorrectly_populated"
                        ),
                        disposition="leave_blank",
                        current_status="missing_or_explicitly_unavailable",
                        rejection_reason=(
                            "The legacy report marks this zero candidate Source=Missing or QA=FAIL; "
                            "it is a placeholder, not source-backed financial evidence."
                        ),
                    )
                )
                continue
            status, comparison, normalized_value = _source_fact_status(
                legacy_value=legacy_value,
                field=package_row.get(metric),
                writes=writes,
                disposition=disposition,
            )
            column = history_headers[legacy_header]
            source_ref = f"{legacy_path.name}!History_Q!{get_column_letter(column)}{legacy_row['row_number']}"
            derived_metric = legacy_header in LEGACY_DERIVED_HISTORY_METRICS
            entries.append(
                _entry(
                    parity_id=f"legacy-quarter:{period}:{metric}",
                    domain=domain,
                    metric=metric,
                    period=period,
                    dimensions={},
                    legacy_range=source_ref,
                    source_kind="derived" if derived_metric else "source_backed",
                    normalized_path=path,
                    requirement="must_reproduce",
                    minimum=1,
                    writes=writes,
                    source_ref=source_ref,
                    inventory_class="source_fact",
                    legacy_value=legacy_value,
                    normalized_value=normalized_value,
                    unit=unit,
                    comparison_result=comparison,
                    disposition=disposition_name,
                    current_status=status,
                )
            )

    # Adjusted EBITDA and adjusted EPS are visible legacy definitions outside
    # History_Q.  They are independently inventoried from the Valuation matrix.
    for legacy_row_number, metric, unit in ((24, "adjusted_ebitda", "$m"), (110, "adjusted_eps", "$/share")):
        for period, (legacy_value, source_ref) in _legacy_valuation_series(legacy_path, legacy_row_number).items():
            package_row_info = quarterly_package_rows.get(period)
            if package_row_info is None:
                path, package_row = f"quarterly_financials.rows[missing:{period}].{metric}", {}
            else:
                index, package_row = package_row_info
                path = f"quarterly_financials.rows.{index}.{metric}"
            writes = writes_by_path.get(path, [])
            status, comparison, normalized_value = _source_fact_status(
                legacy_value=round(legacy_value, 6),
                field=package_row.get(metric),
                writes=writes,
                disposition=dispositions.get(("quarterly_financials", metric)),
            )
            entries.append(
                _entry(
                    parity_id=f"legacy-quarter:{period}:{metric}", domain="per_share" if metric.endswith("eps") else "quarterly_financials",
                    metric=metric, period=period, dimensions={}, legacy_range=source_ref,
                    source_kind="source_backed", normalized_path=path, requirement="must_reproduce",
                    minimum=1, writes=writes, source_ref=source_ref,
                    legacy_value=round(legacy_value, 6), normalized_value=normalized_value, unit=unit,
                    comparison_result=comparison, disposition="planned_binding" if writes else "", current_status=status,
                )
            )

    # Annual source facts are independently reconstructed only from complete
    # fiscal Q1-Q4 sets.  Annual EPS and weighted-average shares are deliberately
    # absent unless an annual source supplies them.
    by_year: dict[int, dict[int, Mapping[str, Any]]] = defaultdict(dict)
    for row in history_rows:
        by_year[int(row["fiscal_year"])][int(row["fiscal_quarter"])] = row
    adjusted_by_period = _legacy_valuation_series(legacy_path, 24)
    for year in sorted(by_year):
        quarters = by_year[year]
        period = f"{year}-FY"
        if set(quarters) != {1, 2, 3, 4}:
            continue
        package_row_info = annual_package_rows.get(period)
        if package_row_info is None:
            index, package_row = -1, {}
        else:
            index, package_row = package_row_info
        header_by_metric = {metric: header for header, (metric, _unit, _domain) in LEGACY_HISTORY_METRICS.items()}
        annual_fields = sorted(ANNUAL_FLOW_FIELDS | ANNUAL_POINT_IN_TIME_FIELDS)
        for metric in annual_fields:
            legacy_header = header_by_metric.get(metric)
            if not legacy_header:
                continue
            unit = LEGACY_HISTORY_METRICS[legacy_header][1]
            component_rows = [quarters[quarter] for quarter in (1, 2, 3, 4)]
            placeholder_rows = component_rows if metric in ANNUAL_FLOW_FIELDS else [component_rows[-1]]
            placeholder_details = [
                unsupported_zero_placeholders[(metric, str(row["period"]))]
                for row in placeholder_rows
                if (metric, str(row["period"])) in unsupported_zero_placeholders
            ]
            if placeholder_details:
                path = (
                    f"annual_financials.rows.{index}.{metric}"
                    if package_row_info is not None
                    else f"annual_financials.rows[missing:{period}].{metric}"
                )
                writes = writes_by_path.get(path, [])
                normalized_value, normalized_status, _ = _field_state(package_row.get(metric))
                report_refs = " + ".join(
                    sorted(
                        {
                            str(detail.get(ref_name) or "")
                            for detail in placeholder_details
                            for ref_name in ("value_source_ref", "metadata_source_ref")
                            if detail.get(ref_name)
                        }
                    )
                )
                entries.append(
                    _entry(
                        parity_id=f"legacy-annual:{period}:{metric}",
                        domain=(
                            "annual_financials"
                            if metric not in ANNUAL_POINT_IN_TIME_FIELDS
                            else "balance_sheet"
                        ),
                        metric=metric,
                        period=period,
                        dimensions={},
                        legacy_range=report_refs,
                        source_kind="unavailable",
                        normalized_path=path,
                        requirement="unavailable_missing_evidence",
                        minimum=1,
                        writes=writes,
                        source_ref=report_refs,
                        inventory_class="unsupported_legacy_content",
                        inventory_origin="legacy_report_quality_check",
                        legacy_value=None,
                        normalized_value=normalized_value,
                        unit=unit,
                        comparison_result=(
                            "unsupported_zero_placeholder_left_blank"
                            if normalized_status != "populated"
                            else "unsupported_zero_placeholder_incorrectly_populated"
                        ),
                        disposition="leave_blank",
                        current_status="missing_or_explicitly_unavailable",
                        rejection_reason=(
                            "One or more annual components are legacy zero placeholders marked "
                            "Source=Missing or QA=FAIL; no annual value may be derived."
                        ),
                    )
                )
                continue
            component_values = [_normalized_legacy_value(row["values"].get(legacy_header), unit) for row in component_rows]
            if metric in ANNUAL_FLOW_FIELDS:
                if any(value is None for value in component_values):
                    continue
                legacy_value = round(sum(float(value) for value in component_values), 6)
                source_rows = component_rows
            else:
                legacy_value = component_values[-1]
                source_rows = [component_rows[-1]]
                if legacy_value is None:
                    continue
            path = (
                f"annual_financials.rows.{index}.{metric}"
                if package_row_info is not None
                else f"annual_financials.rows[missing:{period}].{metric}"
            )
            writes = writes_by_path.get(path, [])
            disposition, disposition_name = _effective_disposition(
                static_disposition=dispositions.get(("annual_financials", metric)),
                selector_exclusion=selector_exclusions.get(path),
                writes=writes,
            )
            status, comparison, normalized_value = _source_fact_status(
                legacy_value=legacy_value, field=package_row.get(metric), writes=writes, disposition=disposition,
            )
            source_refs = [f"{legacy_path.name}!History_Q!{get_column_letter(history_headers[legacy_header])}{row['row_number']}" for row in source_rows]
            entries.append(
                _entry(
                    parity_id=f"legacy-annual:{period}:{metric}", domain="annual_financials" if metric not in ANNUAL_POINT_IN_TIME_FIELDS else "balance_sheet",
                    metric=metric, period=period, dimensions={}, legacy_range=" + ".join(source_refs),
                    source_kind="source_backed", normalized_path=path, requirement="must_reproduce", minimum=1,
                    writes=writes, source_ref=" + ".join(source_refs), legacy_value=legacy_value,
                    normalized_value=normalized_value, unit=unit, comparison_result=comparison,
                    disposition=disposition_name, current_status=status,
                )
            )
        adjusted_components = [adjusted_by_period.get(str(quarters[q]["period"])) for q in (1, 2, 3, 4)]
        if all(component is not None for component in adjusted_components):
            legacy_value = round(sum(float(component[0]) for component in adjusted_components if component), 6)
            metric = "adjusted_ebitda"
            path = (
                f"annual_financials.rows.{index}.{metric}"
                if package_row_info is not None
                else f"annual_financials.rows[missing:{period}].{metric}"
            )
            writes = writes_by_path.get(path, [])
            disposition, disposition_name = _effective_disposition(
                static_disposition=dispositions.get(("annual_financials", metric)),
                selector_exclusion=selector_exclusions.get(path),
                writes=writes,
            )
            status, comparison, normalized_value = _source_fact_status(
                legacy_value=legacy_value, field=package_row.get(metric), writes=writes,
                disposition=disposition,
            )
            source_refs = [str(component[1]) for component in adjusted_components if component]
            entries.append(_entry(
                parity_id=f"legacy-annual:{period}:{metric}", domain="annual_financials", metric=metric,
                period=period, dimensions={}, legacy_range=" + ".join(source_refs), source_kind="source_backed",
                normalized_path=path, requirement="must_reproduce", minimum=1, writes=writes,
                source_ref=" + ".join(source_refs), legacy_value=legacy_value, normalized_value=normalized_value,
                unit="$m", comparison_result=comparison, disposition=disposition_name, current_status=status,
            ))
        for unavailable_metric in ("diluted_shares", "eps"):
            path = (
                f"annual_financials.rows.{index}.{unavailable_metric}"
                if package_row_info is not None
                else f"annual_financials.rows[missing:{period}].{unavailable_metric}"
            )
            entries.append(_entry(
                parity_id=f"legacy-annual:{period}:{unavailable_metric}:unsupported-proxy", domain="per_share",
                metric=unavailable_metric, period=period, dimensions={}, legacy_range="",
                source_kind="unavailable", normalized_path=path, requirement="unavailable_missing_evidence",
                minimum=1, writes=writes_by_path.get(path, []), inventory_class="unsupported_legacy_content",
                inventory_origin="independent_legacy_absence_check", comparison_result="no_source_backed_annual_denominator_or_eps",
                disposition="leave_blank", current_status="missing_or_explicitly_unavailable",
                rejection_reason="The legacy fixture has no source-backed annual diluted EPS or annual weighted-average diluted-share denominator; fiscal-Q4 shares are not a valid proxy.",
            ))

    # Segment inventory is read directly from the visible legacy matrix.  The
    # normalized package and plan are only queried after each exact cell key is known.
    segment_package_rows = _path_get(package, "segments.items") or []
    segment_index: dict[tuple[str, str, str, str], tuple[int, Mapping[str, Any]]] = {}
    for index, row in enumerate(segment_package_rows):
        if isinstance(row, Mapping):
            field_name = "annual_revenue" if str(row.get("period_type") or "") == "annual" else "revenue"
            segment_index.setdefault((str(row.get("dimension") or ""), str(row.get("member") or ""), str(row.get("period") or ""), field_name), (index, row))
    legacy_wb = load_workbook(legacy_path, read_only=True, data_only=True)
    try:
        ws = legacy_wb["BS_Segments"]
        for period_type, header_row, member_rows, columns in (
            ("quarterly", 7, (61, 62, 63, 65, 66, 67), range(2, 14)),
            ("annual", 70, (72, 73, 74), range(2, 10)),
        ):
            for row_number in member_rows:
                member = str(ws.cell(row_number, 1).value or "").strip()
                dimension = "geography" if member in {"Americas", "EMEA", "APAC"} else "brand" if member in {"Hollister", "Abercrombie"} else "total_company"
                for column in columns:
                    raw_period, raw_value = ws.cell(header_row, column).value, ws.cell(row_number, column).value
                    if raw_period in (None, "") or not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
                        continue
                    period = f"{int(raw_period)}-FY" if period_type == "annual" else str(raw_period)
                    field_name = "annual_revenue" if period_type == "annual" else "revenue"
                    package_info = segment_index.get((dimension, member, period, field_name))
                    if package_info is None:
                        path, package_row = f"segments.items[missing:{dimension}:{member}:{period}].{field_name}", {}
                    else:
                        index, package_row = package_info
                        path = f"segments.items.{index}.{field_name}"
                    legacy_value = float(raw_value)
                    writes = writes_by_path.get(path, [])
                    status, comparison, normalized_value = _source_fact_status(
                        legacy_value=legacy_value, field=package_row.get(field_name), writes=writes, disposition=None,
                    )
                    source_ref = f"{legacy_path.name}!BS_Segments!{get_column_letter(column)}{row_number}"
                    entries.append(_entry(
                        parity_id=f"legacy-segment:{period}:{dimension}:{member}:revenue", domain="segments",
                        metric="segment revenue", period=period, dimensions={"dimension": dimension, "member": member},
                        legacy_range=source_ref, source_kind="source_backed", normalized_path=path,
                        requirement="must_reproduce", minimum=1, writes=writes, source_ref=source_ref,
                        legacy_value=legacy_value, normalized_value=normalized_value, unit="$m",
                        comparison_result=comparison, disposition="planned_binding" if writes else "", current_status=status,
                    ))
    finally:
        legacy_wb.close()

    for index, row in enumerate(_path_get(package, "normalized_guidance.items") or []):
        if not isinstance(row, Mapping):
            continue
        path = f"normalized_guidance.items.{index}.value"
        _, status, source_ref = _field_state(row.get("value"))
        requirement = "may_improve_semantically"
        metric = str(_scalar(row.get("metric")) or "guidance")
        horizon = str(_scalar(row.get("horizon")) or _scalar(row.get("period")) or "")
        publication_date = str(_scalar(row.get("publication_date")) or "")
        evidence_key = str(_scalar(row.get("evidence_key")) or f"row-{index}")
        entries.append(
            _entry(
                parity_id=f"guidance:{metric}:{horizon}:{publication_date}:{evidence_key}",
                domain="guidance_promise_progress",
                metric=metric,
                period=horizon,
                dimensions={"visibility": row.get("visibility"), "update_stage": _scalar(row.get("update_stage"))},
                legacy_range=source_ref,
                source_kind="source_backed",
                normalized_path=path,
                requirement=requirement,
                minimum=5,
                writes=writes_by_path.get(path, []),
                source_ref=source_ref,
                inventory_origin="deferred_pass_2_package_projection",
            )
        )

    for domain, path, requirement in SCALAR_REQUIREMENTS:
        value, status, source_ref = _field_state(_path_get(package, path))
        effective = requirement
        inventory_class = "duplicate_display_use" if domain == "valuation_inputs" else "source_fact"
        inventory_origin = (
            "legacy_visible_display_contract"
            if domain == "valuation_inputs"
            else "deferred_pass_2_package_projection"
        )
        entries.append(
            _entry(
                parity_id=f"scalar:{path}",
                domain=domain,
                metric=path.rsplit(".", 1)[-1],
                period=str((_path_get(package, path) or {}).get("period") or "latest") if isinstance(_path_get(package, path), Mapping) else "latest",
                dimensions={},
                legacy_range=source_ref,
                source_kind="source_backed" if status == "populated" else "unavailable",
                normalized_path=path,
                requirement=effective,
                minimum=1,
                writes=writes_by_path.get(path, []),
                source_ref=source_ref,
                inventory_class=inventory_class,
                inventory_origin=inventory_origin,
                disposition="duplicate_display_binding" if domain == "valuation_inputs" and status == "populated" else "",
            )
        )

    wb = load_workbook(shell_path, read_only=False, data_only=False)
    legacy = load_workbook(legacy_path, read_only=False, data_only=False)
    try:
        axes = plan.get("period_axes") or {}
        for contracts, axis_id in ((FORMULA_ROWS, "valuation_quarterly_periods"), (ANNUAL_FORMULA_ROWS, "bs_annual_financial_periods")):
            axis = axes.get(axis_id) if isinstance(axes, Mapping) else {}
            period_to_column = axis.get("period_to_column") if isinstance(axis, Mapping) else {}
            for contract in contracts:
                for period, column in sorted((period_to_column or {}).items()):
                    coordinate = f"{column}{contract.row}"
                    cell = wb[contract.sheet][coordinate]
                    formula = cell.value
                    legacy_range = f"{contract.sheet}!{column}{contract.row}"
                    formula_present = isinstance(formula, str) and formula.startswith("=")
                    formula_protected = bool(cell.protection.locked)
                    if axis_id == "valuation_quarterly_periods":
                        calculable, calculation_reason = _quarter_formula_calculability(
                            contract.formula_id,
                            str(period),
                            period_ordinals=history_period_ordinals,
                            history_values=history_values,
                        )
                    else:
                        calculable, calculation_reason = _annual_formula_calculability(
                            contract.formula_id,
                            str(period),
                            annual_package_rows,
                        )
                    entries.append(
                        _entry(
                            parity_id=f"formula:{contract.formula_id}:{period}",
                            domain="formula_derived_metrics",
                            metric=contract.description,
                            period=str(period),
                            dimensions={},
                            legacy_range=legacy_range if contract.sheet in legacy.sheetnames else "",
                            source_kind="derived",
                            normalized_path="",
                            requirement="may_improve_semantically",
                            minimum=1,
                            writes=[],
                            formula_cell=f"{contract.sheet}!{coordinate}",
                            formula_present=formula_present,
                            formula_protected=formula_protected,
                            economically_calculable=calculable,
                            calculation_reason=calculation_reason,
                            inventory_class="formula_improvement",
                            inventory_origin="generic_formula_contract",
                            comparison_result=(
                                "formula_present_protected"
                                if formula_present and formula_protected
                                else "formula_present_unprotected"
                                if formula_present
                                else "formula_missing"
                            ),
                        )
                    )
        for coordinate in (*VALUATION_OUTPUT_FORMULA_CELLS, *VALUATION_SIDECAR_FORMULA_CELLS):
            cell = wb["Valuation"][coordinate]
            formula = cell.value
            formula_present = isinstance(formula, str) and formula.startswith("=")
            formula_protected = bool(cell.protection.locked)
            calculable, calculation_reason = _valuation_output_calculability(coordinate, package)
            entries.append(
                _entry(
                    parity_id=f"valuation_output:{coordinate}",
                    domain="valuation_outputs",
                    metric=str(wb["Valuation"].cell(wb["Valuation"][coordinate].row, 11 if coordinate.startswith("N") else 15).value or coordinate),
                    period="latest",
                    dimensions={},
                    legacy_range=f"Valuation!{coordinate}",
                    source_kind="derived",
                    normalized_path="",
                    requirement="may_improve_semantically",
                    minimum=1,
                    writes=[],
                    formula_cell=f"Valuation!{coordinate}",
                    formula_present=formula_present,
                    formula_protected=formula_protected,
                    economically_calculable=calculable,
                    calculation_reason=calculation_reason,
                    inventory_class="formula_improvement",
                    inventory_origin="generic_formula_contract",
                    comparison_result=(
                        "formula_present_protected"
                        if formula_present and formula_protected
                        else "formula_present_unprotected"
                        if formula_present
                        else "formula_missing"
                    ),
                )
            )
    finally:
        wb.close()
        legacy.close()

    domain_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in entries:
        domain_counts[str(row["domain"])][str(row["current_status"])] += 1
    required = [row for row in entries if row["parity_requirement"] == "must_reproduce"]
    missing_required = [row for row in required if row["current_status"] != "reproduced_correctly"]
    independent_source_facts = [
        row for row in entries
        if row["inventory_class"] == "source_fact" and row["inventory_origin"] == "legacy_workbook_business_key"
    ]
    inventory_class_counts = Counter(str(row["inventory_class"]) for row in entries)
    formula_entries = [row for row in entries if row["inventory_class"] == "formula_improvement"]
    formula_contract_counts = Counter(str(row["formula_contract_status"]) for row in formula_entries)
    formula_calculability_counts = Counter(str(row["economic_calculability"]) for row in formula_entries)
    return {
        "$schema": "./anf_new_ticker_parity_matrix.schema.json",
        "version": "1.2.0",
        "contract_name": "ANF legacy-oracle parity matrix",
        "architectural_scope": "legacy_adapter_fixture_only; generic engine remains ticker-neutral",
        "inventory_method": "legacy workbook business keys are inventoried first; normalized package and plan are comparison targets only",
        "formula_contract_version": FORMULA_CONTRACT_VERSION,
        "source_digests": {
            "legacy_workbook_sha256": _sha(legacy_path),
            "normalized_package_sha256": hashlib.sha256(json.dumps(package, sort_keys=True, default=str).encode()).hexdigest(),
            "binding_plan_sha256": hashlib.sha256(json.dumps(plan, sort_keys=True, default=str).encode()).hexdigest(),
            "shell_sha256": _sha(shell_path),
            "binding_map_sha256": _sha(binding_path),
        },
        "summary": {
            "entry_count": len(entries),
            "required_count": len(required),
            "required_reproduced_count": len(required) - len(missing_required),
            "required_missing_count": len(missing_required),
            "independent_source_fact_count": len(independent_source_facts),
            "independent_source_fact_reproduced_count": sum(row["current_status"] == "reproduced_correctly" for row in independent_source_facts),
            "inventory_class_counts": dict(sorted(inventory_class_counts.items())),
            "formula_contract_counts": dict(sorted(formula_contract_counts.items())),
            "formula_calculability_counts": dict(sorted(formula_calculability_counts.items())),
            "domain_status_counts": {key: dict(value) for key, value in sorted(domain_counts.items())},
        },
        "entries": entries,
    }


def _markdown(matrix: Mapping[str, Any]) -> str:
    summary = matrix["summary"]
    lines = [
        "# ANF New-Ticker Parity Matrix",
        "",
        "ANF is a read-only migration oracle. This matrix locks business-key coverage without moving ANF logic into the generic engine.",
        "",
        f"- Entries: {summary['entry_count']}",
        f"- Required reproduced: {summary['required_reproduced_count']} / {summary['required_count']}",
        f"- Required missing: {summary['required_missing_count']}",
        f"- Independently inventoried legacy source facts reproduced: {summary['independent_source_fact_reproduced_count']} / {summary['independent_source_fact_count']}",
        f"- Inventory classes: {json.dumps(summary['inventory_class_counts'], sort_keys=True)}",
        f"- Formula contracts: {json.dumps(summary['formula_contract_counts'], sort_keys=True)}",
        f"- Formula calculability: {json.dumps(summary['formula_calculability_counts'], sort_keys=True)}",
        "",
        "| Domain | Economically reproduced | Contract present, blank by missing evidence | Missing / unavailable |",
        "|---|---:|---:|---:|",
    ]
    for domain, counts in summary["domain_status_counts"].items():
        lines.append(
            f"| {domain} | {counts.get('reproduced_correctly', 0)} | "
            f"{counts.get('contract_present_blank_by_missing_evidence', 0)} | "
            f"{counts.get('missing_or_explicitly_unavailable', 0)} |"
        )
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    data_root = _default_data_root()
    default_dir = data_root / "outputs" / "stress_tests" / "ANF_new_ticker_engine"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, default=default_dir / "ANF_normalized_data_package.json")
    parser.add_argument("--plan", type=Path, default=default_dir / "ANF_binding_plan.json")
    parser.add_argument("--legacy", type=Path, default=data_root / "outputs" / "Excel stock models" / "ANF_model.xlsx")
    parser.add_argument("--shell", type=Path, default=DEFAULT_SHELL)
    parser.add_argument("--binding-map", type=Path, default=DEFAULT_BINDINGS)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    args = parser.parse_args(argv)
    matrix = build_parity_matrix(
        package=load_json_strict(args.package),
        plan=load_json_strict(args.plan),
        legacy_path=args.legacy,
        shell_path=args.shell,
        binding_path=args.binding_map,
    )
    args.output_json.write_text(json.dumps(matrix, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.output_md.write_text(_markdown(matrix), encoding="utf-8")
    print(f"parity matrix: {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
