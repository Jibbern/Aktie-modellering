"""Investor-facing Capital Allocation and Capital Return workbook expansion.

This module is deliberately a presentation consumer.  It binds already accepted
owners from normalized financials, Capital Return, Debt/Liquidity, and the
Summary/BS product; it does not create a second economic engine.  The workbook
plan uses only value, clear, style, merge, and row-visibility mutations on the
accepted Capital Return/Debt preview.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from urllib.parse import unquote
from zipfile import ZipFile

from pbi_xbrl.new_ticker_capital_return import (
    CAPITAL_RETURN_PRODUCT_ROWS,
    build_capital_return_workbook_projection,
    validate_capital_return_records,
)
from pbi_xbrl.longitudinal_memory.capital_return_debt_workbook_materialization import (
    FormulaAwareCellMutation,
    FormulaAwareMaterializationResult,
    WorksheetRowMutation,
    materialize_capital_return_debt_mutations,
)
from pbi_xbrl.longitudinal_memory.formula_aware_workbook_materialization import (
    WorksheetMergeMutation,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    _cell_elements,
    _sheet_part_map,
    sha256_file,
)


INVESTOR_PRODUCT_CONTRACT = "capital-allocation-return-investor-product@1"
WORKBOOK_PROJECTION_CONTRACT = "capital-allocation-return-valuation-presentation@1"
EXPECTED_ACCEPTED_PREVIEW_SHA256 = (
    "8cb7ed3c9b080c8dbd1518cfdbbb80d8a70a4cea530c4a734f9e8fbfd6f982bf"
)
VISIBLE_PRODUCT_RANGE = "N79:AA122"
LINEAGE_SUPPORT_RANGE = "A153:E158"
RETIRED_CURRENT_PRODUCT_RANGE = "A152:M168"
RETIRED_CURRENT_SUPPORT_RANGE = "AD172:AO186"
QUARTERLY_HISTORY_LENGTH = 12
ANNUAL_ALLOCATION_HISTORY_LENGTH = 5

_AMOUNT_FORMAT = "#,##0.0"
_SHARES_FORMAT = "#,##0.000"
_PRICE_FORMAT = "$0.00"
_PERCENT_FORMAT = "0.0%"

_SUMMARY_PERIOD_KEYS = ("latest_quarter", "ttm", "latest_completed_year")
_CAPITAL_ALLOCATION_ROWS = (
    ("free_cash_flow", "FCF ($m)", _AMOUNT_FORMAT),
    ("capital_expenditures", "Capex / Reinvestment ($m)", _AMOUNT_FORMAT),
    ("repurchase_cash_program", "Buybacks ($m)", _AMOUNT_FORMAT),
    ("ending_net_cash", "Ending net cash / (debt) ($m)", _AMOUNT_FORMAT),
)
_CAPITAL_RETURN_SUMMARY_ROWS = (
    ("repurchase_cash_program", "Buybacks ($m)", _AMOUNT_FORMAT),
    (
        "accounting_program_shares_repurchased",
        "Shares repurchased (m)",
        _SHARES_FORMAT,
    ),
    ("cash_per_program_share", "Avg. repurchase price ($/share)", _PRICE_FORMAT),
    ("share_issuance_sbc", "Shares issued (m)", _SHARES_FORMAT),
    (
        "net_share_reduction",
        "Net shares retired / (issued) (m)",
        _SHARES_FORMAT,
    ),
    ("buybacks_to_fcf", "Buybacks / FCF (%)", _PERCENT_FORMAT),
    ("dividends_paid", "Dividends ($m)", _AMOUNT_FORMAT),
    ("authorization_remaining", "Authorization remaining ($m)", _AMOUNT_FORMAT),
)
_CAPITAL_RETURN_HISTORY_ROWS = _CAPITAL_RETURN_SUMMARY_ROWS[:7]

# Presentation composes canonical owners; Capital Return is only one of them.
# These contracts are deliberately declarative so a ticker profile can expose
# each supported owner independently without acquiring ticker-specific logic.
CAPITAL_ALLOCATION_OWNER_ROUTES = (
    ("free_cash_flow", "normalized_company_data.free_cash_flow", "normalized_financials"),
    (
        "capital_expenditures",
        "normalized_company_data.capital_expenditures",
        "normalized_financials",
    ),
    (
        "acquisitions_investments",
        "normalized_company_data.acquisitions_cash",
        "normalized_financials",
    ),
    ("debt_repayment", "debt_liquidity.actual_debt_repayment", "debt_liquidity"),
    (
        "debt_issuance_financing",
        "debt_liquidity.actual_debt_issuance",
        "debt_liquidity",
    ),
    ("repurchase_cash_program", "capital_return.repurchase_cash_program", "buyback"),
    ("dividends_paid", "capital_return.dividends_paid", "dividend"),
    ("ending_net_cash", "summary_bs.net_cash", "debt_liquidity"),
)

CAPITAL_RETURN_ACTIVITY_FAMILIES = {
    "BUYBACK": frozenset(
        {
            "repurchase_cash_program",
            "accounting_program_shares_repurchased",
            "cash_per_program_share",
            "buybacks_to_fcf",
            "authorization_remaining",
        }
    ),
    "DIVIDEND": frozenset({"dividends_paid"}),
    "SHARE_ISSUANCE": frozenset({"share_issuance_sbc", "net_share_reduction"}),
}

_BLOCK_LAYOUT = {
    "capital_allocation_summary": {
        "title": "A. Capital Allocation Summary",
        "title_row": 81,
        "header_row": 82,
        "first_data_row": 83,
        "period_columns": ("P", "Q", "R"),
        "context_range": "S:AA",
        "history": False,
    },
    "annual_capital_allocation_history": {
        "title": "B. Annual Capital Allocation History",
        "title_row": 88,
        "header_row": 89,
        "first_data_row": 90,
        "period_columns": ("P", "Q", "R", "S", "T"),
        "context_range": "U:AA",
        "history": True,
    },
    "capital_return_summary": {
        "title": "C. Capital Return Summary",
        "title_row": 95,
        "header_row": 96,
        "first_data_row": 97,
        "period_columns": ("P", "Q", "R"),
        "context_range": "S:AA",
        "history": False,
    },
    "quarterly_capital_return_history": {
        "title": "D. Quarterly Capital Return History",
        "title_row": 106,
        "header_row": 107,
        "first_data_row": 108,
        "period_columns": ("P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "AA"),
        "context_range": None,
        "history": True,
    },
    "annual_capital_return_history": {
        "title": "E. Annual Capital Return History",
        "title_row": 115,
        "header_row": 116,
        "first_data_row": 117,
        "period_columns": ("P", "Q"),
        "context_range": "R:AA",
        "history": True,
    },
}


class CapitalAllocationReturnExpansionError(ValueError):
    """Fail-closed product or workbook projection contract violation."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _number_text(value: float | int | Decimal) -> str:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise CapitalAllocationReturnExpansionError("Workbook numeric writes must be finite.")
    return format(parsed, "f")


def _display_period(period: str) -> str:
    quarter = re.fullmatch(r"([0-9]{4})-Q([1-4])", period)
    annual = re.fullmatch(r"([0-9]{4})-FY", period)
    ttm = re.fullmatch(r"TTM through ([0-9]{4})-Q([1-4])", period)
    if quarter:
        return f"Q{quarter.group(2)}'{quarter.group(1)[2:]}"
    if annual:
        return f"FY{annual.group(1)[2:]}"
    if ttm:
        return f"TTM Q{ttm.group(2)}'{ttm.group(1)[2:]}"
    raise CapitalAllocationReturnExpansionError(f"Unsupported fiscal period {period!r}.")


def _entry(
    *,
    period: str,
    value: float | int | None,
    status: str,
    owner: str,
    source_identity: str,
    source_ref: str,
    definition: str,
    unit: str,
    aggregation_role: str,
    source_classification: str,
    source_period: str | None = None,
    reason: str = "",
) -> dict[str, Any]:
    if value is not None and isinstance(value, bool):
        raise CapitalAllocationReturnExpansionError("Boolean economic values are invalid.")
    available = value is not None and status not in {"missing_source", "unavailable"}
    return {
        "aggregation_role": aggregation_role,
        "definition": definition,
        "display_period": _display_period(period),
        "owner": owner,
        "period": period,
        "reason": reason,
        "source_classification": source_classification,
        "source_identity": source_identity,
        "source_period": source_period or period,
        "source_ref": source_ref,
        "status": "available" if available else "unavailable",
        "unit": unit,
        "value": None if value is None else float(value),
    }


def _unavailable_entry(
    *,
    period: str,
    owner: str,
    unit: str,
    aggregation_role: str,
    reason: str,
    source_identity: str = "",
    source_ref: str = "",
) -> dict[str, Any]:
    return _entry(
        period=period,
        value=None,
        status="unavailable",
        owner=owner,
        source_identity=source_identity,
        source_ref=source_ref,
        definition=reason,
        unit=unit,
        aggregation_role=aggregation_role,
        source_classification="unavailable",
        reason=reason,
    )


def _capital_return_index(package: Mapping[str, Any]) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    section = package.get("capital_returns")
    raw = section.get("records") if isinstance(section, Mapping) else None
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise CapitalAllocationReturnExpansionError("Capital Return records are malformed.")
    records = validate_capital_return_records(raw)
    return {
        (str(row["metric_id"]), str(row["fiscal_period"]), str(row["period_type"])): row
        for row in records
    }


def _capital_return_entry(
    index: Mapping[tuple[str, str, str], Mapping[str, Any]],
    *,
    metric_id: str,
    period: str,
    period_type: str,
) -> dict[str, Any]:
    record = index.get((metric_id, period, period_type))
    owner = f"capital_return.{metric_id}"
    if record is None:
        unit = {
            "cash_per_program_share": "$/share",
            "buybacks_to_fcf": "%",
            "accounting_program_shares_repurchased": "m shares",
            "share_issuance_sbc": "m shares",
            "net_share_reduction": "m shares",
        }.get(metric_id, "$m")
        role = "point_in_time" if metric_id == "authorization_remaining" else "additive_flow"
        return _unavailable_entry(
            period=period,
            owner=owner,
            unit=unit,
            aggregation_role=role,
            reason="No compatible accepted source-native Capital Return record.",
        )
    value = record.get("value")
    if value is not None and not isinstance(value, (int, float)):
        raise CapitalAllocationReturnExpansionError(
            f"Capital Return record {record.get('record_id')!r} has a non-numeric value."
        )
    return _entry(
        period=period,
        value=value,
        status=str(record.get("status") or ""),
        owner=owner,
        source_identity=str(record.get("record_id") or ""),
        source_ref=str(record.get("evidence_ref") or ""),
        definition=str(record.get("derivation_identity") or record.get("semantic_role") or ""),
        unit=str(record.get("unit") or ""),
        aggregation_role=str(record.get("aggregation_role") or ""),
        source_classification=str(record.get("source_classification") or ""),
        reason=str(record.get("reason") or ""),
    )


def _normalized_annual_entry(
    row: Mapping[str, Any],
    *,
    metric_id: str,
    period: str,
) -> dict[str, Any]:
    cell = row.get(metric_id)
    owner = f"normalized_company_data.annual_financials.{metric_id}"
    if not isinstance(cell, Mapping):
        return _unavailable_entry(
            period=period,
            owner=owner,
            unit="$m",
            aggregation_role="additive_flow",
            reason="Annual normalized field is unavailable.",
        )
    value = cell.get("value")
    if value is not None and not isinstance(value, (int, float)):
        raise CapitalAllocationReturnExpansionError(
            f"Annual normalized field {metric_id!r} is non-numeric."
        )
    return _entry(
        period=period,
        value=value,
        status=str(cell.get("status") or ""),
        owner=owner,
        source_identity=f"normalized-company-data:annual:{period}:{metric_id}",
        source_ref=str(cell.get("source_ref") or ""),
        definition=str(cell.get("definition") or ""),
        unit=str(cell.get("unit") or "$m"),
        aggregation_role="additive_flow",
        source_classification=str(cell.get("confidence") or "normalized_company_data"),
    )


def _quarter_ordinal(period: str) -> int:
    match = re.fullmatch(r"([0-9]{4})-Q([1-4])", period)
    if match is None:
        raise CapitalAllocationReturnExpansionError(
            f"Normalized quarter has an invalid semantic period: {period!r}."
        )
    return int(match.group(1)) * 4 + int(match.group(2)) - 1


def _annual_ordinal(period: str) -> int:
    match = re.fullmatch(r"([0-9]{4})-FY", period)
    if match is None:
        raise CapitalAllocationReturnExpansionError(
            f"Normalized annual row has an invalid semantic period: {period!r}."
        )
    return int(match.group(1))


def _ordered_normalized_rows(
    rows: Sequence[Mapping[str, Any]], *, period_type: str
) -> tuple[Mapping[str, Any], ...]:
    if period_type == "quarter":
        key = lambda row: _quarter_ordinal(str(row.get("period") or ""))
    elif period_type == "annual":
        key = lambda row: _annual_ordinal(str(row.get("period") or ""))
    else:
        raise CapitalAllocationReturnExpansionError(
            f"Unsupported normalized period type {period_type!r}."
        )
    ordered = tuple(sorted((row for row in rows if isinstance(row, Mapping)), key=key))
    periods = [str(row.get("period") or "") for row in ordered]
    if len(periods) != len(set(periods)):
        raise CapitalAllocationReturnExpansionError(
            f"Normalized {period_type} history contains duplicate semantic periods."
        )
    return ordered


def _normalized_summary_entry(
    *,
    metric_id: str,
    period: str,
    period_type: str,
    quarter_rows: Sequence[Mapping[str, Any]],
    annual_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    owner = f"normalized_company_data.{metric_id}"
    if period_type == "annual":
        row = next(
            (row for row in annual_rows if str(row.get("period") or "") == period),
            None,
        )
        if row is None:
            return _unavailable_entry(
                period=period,
                owner=owner,
                unit="$m",
                aggregation_role="additive_flow",
                reason="No compatible normalized annual field.",
            )
        entry = _normalized_annual_entry(row, metric_id=metric_id, period=period)
        entry["owner"] = owner
        return entry

    ending_period = period.removeprefix("TTM through ") if period_type == "ttm" else period
    ending_ordinal = _quarter_ordinal(ending_period)
    periods = (
        {ending_ordinal}
        if period_type == "quarter"
        else set(range(ending_ordinal - 3, ending_ordinal + 1))
    )
    selected = [
        row
        for row in quarter_rows
        if _quarter_ordinal(str(row.get("period") or "")) in periods
    ]
    if len(selected) != len(periods):
        return _unavailable_entry(
            period=period,
            owner=owner,
            unit="$m",
            aggregation_role="additive_flow",
            reason="Compatible normalized quarterly history is incomplete.",
        )
    cells = [row.get(metric_id) for row in selected]
    if any(not isinstance(cell, Mapping) for cell in cells):
        return _unavailable_entry(
            period=period,
            owner=owner,
            unit="$m",
            aggregation_role="additive_flow",
            reason="Compatible normalized field is unavailable.",
        )
    values = [cell.get("value") for cell in cells if isinstance(cell, Mapping)]
    if any(value is None for value in values):
        return _unavailable_entry(
            period=period,
            owner=owner,
            unit="$m",
            aggregation_role="additive_flow",
            reason="Compatible normalized field is unavailable.",
        )
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in values):
        raise CapitalAllocationReturnExpansionError(
            f"Normalized field {metric_id!r} is non-numeric."
        )
    source_refs = [
        str(cell.get("source_ref") or "") for cell in cells if isinstance(cell, Mapping)
    ]
    first_cell = cells[0]
    assert isinstance(first_cell, Mapping)
    value = sum(float(item) for item in values)
    return _entry(
        period=period,
        value=value,
        status="populated",
        owner=owner,
        source_identity=f"normalized-company-data:{period_type}:{period}:{metric_id}",
        source_ref=" + ".join(source_ref for source_ref in source_refs if source_ref),
        definition=str(first_cell.get("definition") or ""),
        unit=str(first_cell.get("unit") or "$m"),
        aggregation_role="additive_flow",
        source_classification="normalized_company_data",
    )


def _routed_normalized_summary_entry(
    *,
    capital_return_index: Mapping[tuple[str, str, str], Mapping[str, Any]],
    metric_id: str,
    period: str,
    period_type: str,
    quarter_rows: Sequence[Mapping[str, Any]],
    annual_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Select the canonical normalized owner without changing accepted ANF lineage."""

    existing = _capital_return_entry(
        capital_return_index,
        metric_id=metric_id,
        period=period,
        period_type=period_type,
    )
    normalized = _normalized_summary_entry(
        metric_id=metric_id,
        period=period,
        period_type=period_type,
        quarter_rows=quarter_rows,
        annual_rows=annual_rows,
    )
    if existing["status"] == "available":
        if normalized["status"] == "available" and abs(
            float(existing["value"]) - float(normalized["value"])
        ) > 1e-9:
            raise CapitalAllocationReturnExpansionError(
                "Genuine economic ownership conflict: Capital Return compatibility data "
                f"and normalized {metric_id} disagree for {period}."
            )
        result = existing
    else:
        result = normalized
    result["owner"] = f"normalized_company_data.{metric_id}"
    return result


def _available_capital_return_periods(
    index: Mapping[tuple[str, str, str], Mapping[str, Any]],
    *,
    period_type: str,
) -> tuple[str, ...]:
    relevant_metrics = {metric_id for metric_id, _, _ in _CAPITAL_RETURN_HISTORY_ROWS}
    periods = {
        period
        for (metric_id, period, record_period_type), record in index.items()
        if metric_id in relevant_metrics
        and record_period_type == period_type
        and record.get("value") is not None
        and str(record.get("status") or "") not in {"missing_source", "unavailable"}
    }
    if period_type == "annual":
        return tuple(sorted(periods, key=_annual_ordinal))
    if period_type == "quarter":
        return tuple(sorted(periods, key=_quarter_ordinal))
    raise CapitalAllocationReturnExpansionError(
        f"Unsupported Capital Return period type {period_type!r}."
    )


def capital_return_activity_family_contract(
    package: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Report independent activity-family relevance without ticker branching."""

    index = _capital_return_index(package)
    records_by_metric: dict[str, list[Mapping[str, Any]]] = {}
    for (metric_id, _period, _period_type), record in index.items():
        records_by_metric.setdefault(metric_id, []).append(record)
    result: list[dict[str, Any]] = []
    for family, metrics in CAPITAL_RETURN_ACTIVITY_FAMILIES.items():
        declared = sorted(metric for metric in metrics if metric in records_by_metric)
        available = sorted(
            metric
            for metric in declared
            if any(
                row.get("value") is not None
                and str(row.get("status") or "") not in {"missing_source", "unavailable"}
                for row in records_by_metric[metric]
            )
        )
        result.append(
            {
                "activity_family": family,
                "available_metrics": available,
                "declared_metrics": declared,
                "is_available": bool(available),
                "is_relevant": bool(declared),
            }
        )
    return tuple(result)


def capital_allocation_owner_routing_review(
    package: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Inspect canonical-owner support without wiring a ticker workbook surface."""

    annual = package.get("annual_financials", {}).get("rows")
    quarters = package.get("quarterly_financials", {}).get("rows")
    normalized_rows = [
        row
        for rows in (annual, quarters)
        if isinstance(rows, list)
        for row in rows
        if isinstance(row, Mapping)
    ]
    normalized_keys = {
        "free_cash_flow": "free_cash_flow",
        "capital_expenditures": "capital_expenditures",
        "acquisitions_investments": "acquisitions_cash",
        "debt_repayment": "debt_repayment",
        "debt_issuance_financing": "debt_issuance",
        "dividends_paid": "dividends_cash",
    }
    capital_index = _capital_return_index(package)
    capital_available = {
        metric_id
        for (metric_id, _period, _period_type), row in capital_index.items()
        if row.get("value") is not None
        and str(row.get("status") or "") not in {"missing_source", "unavailable"}
    }
    result: list[dict[str, Any]] = []
    for metric_id, owner, owner_family in CAPITAL_ALLOCATION_OWNER_ROUTES:
        normalized_key = normalized_keys.get(metric_id)
        normalized_available = bool(
            normalized_key
            and any(
                isinstance(row.get(normalized_key), Mapping)
                and row[normalized_key].get("value") is not None
                for row in normalized_rows
            )
        )
        if metric_id == "ending_net_cash":
            owner_exists = bool(package.get("debt_liquidity"))
        elif metric_id in {"repurchase_cash_program", "dividends_paid"}:
            owner_exists = metric_id in capital_available or normalized_available
        else:
            owner_exists = normalized_available
        result.append(
            {
                "classification": (
                    "CANONICAL_OWNER_AVAILABLE" if owner_exists else "UNAVAILABLE"
                ),
                "metric_id": metric_id,
                "owner": owner,
                "owner_family": owner_family,
                "workbook_wired": False,
            }
        )
    return tuple(result)


def _period_from_canonical_fact(canonical_fact_id: str) -> str:
    decoded = unquote(canonical_fact_id)
    match = re.search(r"period=period:[^|]*:fy([0-9]{4})-q([1-4])@1", decoded)
    if match is None:
        raise CapitalAllocationReturnExpansionError(
            f"Cannot resolve Summary/BS fiscal period from {canonical_fact_id!r}."
        )
    return f"{match.group(1)}-Q{match.group(2)}"


def _net_cash_index(
    balance_sheet_product: Mapping[str, Any],
    balance_sheet_shadow: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    fields = balance_sheet_product.get("fields")
    lineage = balance_sheet_shadow.get("field_lineage")
    if not isinstance(fields, list) or not isinstance(lineage, list):
        raise CapitalAllocationReturnExpansionError("Summary/BS product or shadow is malformed.")
    lineage_by_field = {
        str(row.get("field_id") or ""): row
        for row in lineage
        if isinstance(row, Mapping)
    }
    result: dict[str, dict[str, Any]] = {}
    for field in fields:
        if not isinstance(field, Mapping) or field.get("metric_id") != "metric:derived:net-cash@1":
            continue
        field_id = str(field.get("field_id") or "")
        canonical_fact_id = str(field.get("canonical_fact_id") or "")
        period = _period_from_canonical_fact(canonical_fact_id)
        raw_value = field.get("value")
        value = raw_value.get("value") if isinstance(raw_value, Mapping) else None
        parsed = float(value) if value is not None else None
        shadow = lineage_by_field.get(field_id)
        if shadow is None:
            raise CapitalAllocationReturnExpansionError(
                f"Summary/BS net-cash field lacks shadow lineage: {field_id!r}."
            )
        result[period] = _entry(
            period=period,
            value=parsed,
            status=str(field.get("status") or ""),
            owner="summary_bs.net_cash",
            source_identity=field_id,
            source_ref=str(shadow.get("legacy_locator") or canonical_fact_id),
            definition=canonical_fact_id,
            unit="$m",
            aggregation_role="point_in_time",
            source_classification="summary_bs_source_native",
            reason=str(shadow.get("value_state") or ""),
        ) | {
            "canonical_fact_id": canonical_fact_id,
            "derivation_id": str(field.get("derivation_id") or ""),
            "shadow_lineage_id": str(shadow.get("audit_field_id") or ""),
        }
    if not result:
        raise CapitalAllocationReturnExpansionError("Summary/BS net-cash universe is empty.")
    return result


def _terminal_entry(entry: Mapping[str, Any], *, target_period: str) -> dict[str, Any]:
    result = dict(entry)
    result["display_period"] = _display_period(target_period)
    result["period"] = target_period
    result["source_period"] = str(entry["period"])
    result["period_behavior"] = "terminal_point_in_time"
    return result


def _row(
    row_key: str,
    label: str,
    number_format: str,
    values: Sequence[Mapping[str, Any]],
    *,
    state_context: str,
) -> dict[str, Any]:
    values_list = [dict(value) for value in values]
    return {
        "available_count": sum(value["status"] == "available" for value in values_list),
        "label": label,
        "number_format": number_format,
        "row_key": row_key,
        "state_context": state_context,
        "values": values_list,
    }


def _has_available(row: Mapping[str, Any]) -> bool:
    return any(value.get("status") == "available" for value in row["values"])


def _slot_dispositions(projection: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    moved = {row[0] for row in _CAPITAL_RETURN_SUMMARY_ROWS}
    hidden = {"ending_period_end_shares"}
    not_displayed = {"diluted_weighted_average_shares", "reported_average_all_purchases"}
    unavailable = {
        "ordinary_dividend_per_share",
        "total_capital_return",
        "dividends_to_fcf",
        "total_capital_return_to_fcf",
    }
    result: list[dict[str, Any]] = []
    for row in projection["product_rows"]:
        key = str(row["row_key"])
        if key in moved:
            disposition = "MOVED_TO_SUMMARY"
        elif key in hidden:
            disposition = "HIDDEN_SUPPORT_ONLY"
        elif key in not_displayed:
            disposition = "INTENTIONALLY_NOT_DISPLAYED"
        elif key in unavailable:
            disposition = "UNAVAILABLE"
        else:
            raise CapitalAllocationReturnExpansionError(
                f"Current Capital Return row lacks an explicit disposition: {key!r}."
            )
        for slot in _SUMMARY_PERIOD_KEYS:
            result.append(
                {
                    "current_value": row[slot],
                    "disposition": disposition,
                    "metric_id": key,
                    "slot": slot,
                }
            )
    if len(result) != 45:
        raise CapitalAllocationReturnExpansionError("Current 45-slot disposition changed.")
    return tuple(result)


@dataclass(frozen=True)
class CapitalAllocationReturnInvestorProduct:
    contract: str
    summary_periods: tuple[str, ...]
    annual_allocation_periods: tuple[str, ...]
    quarterly_return_periods: tuple[str, ...]
    annual_return_periods: tuple[str, ...]
    capital_allocation_summary: tuple[dict[str, Any], ...]
    annual_capital_allocation_history: tuple[dict[str, Any], ...]
    capital_return_summary: tuple[dict[str, Any], ...]
    quarterly_capital_return_history: tuple[dict[str, Any], ...]
    annual_capital_return_history: tuple[dict[str, Any], ...]
    capital_allocation_owner_map: tuple[dict[str, Any], ...]
    current_45_slot_disposition: tuple[dict[str, Any], ...]
    derivation_review: Mapping[str, Any]
    row_relevance_contract: Mapping[str, Any]
    product_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "annual_allocation_periods": list(self.annual_allocation_periods),
            "annual_capital_allocation_history": [dict(row) for row in self.annual_capital_allocation_history],
            "annual_capital_return_history": [dict(row) for row in self.annual_capital_return_history],
            "annual_return_periods": list(self.annual_return_periods),
            "capital_allocation_owner_map": [dict(row) for row in self.capital_allocation_owner_map],
            "capital_allocation_summary": [dict(row) for row in self.capital_allocation_summary],
            "capital_return_summary": [dict(row) for row in self.capital_return_summary],
            "contract": self.contract,
            "current_45_slot_disposition": [dict(row) for row in self.current_45_slot_disposition],
            "derivation_review": dict(self.derivation_review),
            "product_digest": self.product_digest,
            "quarterly_capital_return_history": [dict(row) for row in self.quarterly_capital_return_history],
            "quarterly_return_periods": list(self.quarterly_return_periods),
            "row_relevance_contract": dict(self.row_relevance_contract),
            "summary_periods": list(self.summary_periods),
        }


def build_capital_allocation_return_investor_product(
    *,
    package: Mapping[str, Any],
    balance_sheet_product: Mapping[str, Any],
    balance_sheet_shadow: Mapping[str, Any],
) -> CapitalAllocationReturnInvestorProduct:
    cr_projection = build_capital_return_workbook_projection(package).to_dict()
    cr_index = _capital_return_index(package)
    raw_annual_rows = package.get("annual_financials", {}).get("rows")
    raw_quarter_rows = package.get("quarterly_financials", {}).get("rows")
    if not isinstance(raw_annual_rows, list) or not isinstance(raw_quarter_rows, list):
        raise CapitalAllocationReturnExpansionError("Normalized financial history is malformed.")
    annual_rows = _ordered_normalized_rows(raw_annual_rows, period_type="annual")
    quarter_rows = _ordered_normalized_rows(raw_quarter_rows, period_type="quarter")
    if not cr_index:
        if not quarter_rows:
            raise CapitalAllocationReturnExpansionError("No accepted fiscal periods are available.")
        latest_quarter = str(quarter_rows[-1]["period"])
        if not annual_rows:
            raise CapitalAllocationReturnExpansionError("No accepted annual periods are available.")
        latest_annual = str(annual_rows[-1]["period"])
        summary_periods = (latest_quarter, f"TTM through {latest_quarter}", latest_annual)
    else:
        summary_periods = (
            str(cr_projection["latest_quarter_label"]),
            str(cr_projection["ttm_label"]),
            str(cr_projection["annual_label"]),
        )
    latest_quarter, latest_ttm, latest_annual = summary_periods
    summary_period_types = ("quarter", "ttm", "annual")

    annual_rows_by_period = {
        str(row.get("period") or ""): row for row in annual_rows if isinstance(row, Mapping)
    }
    annual_allocation_periods = tuple(
        str(row.get("period") or "") for row in annual_rows[-ANNUAL_ALLOCATION_HISTORY_LENGTH:]
    )
    quarterly_return_periods = tuple(
        str(row.get("period") or "") for row in quarter_rows[-QUARTERLY_HISTORY_LENGTH:]
    )
    if len(annual_allocation_periods) != 5 or len(quarterly_return_periods) != 12:
        raise CapitalAllocationReturnExpansionError(
            "Accepted normalized history no longer supports the 5Y/12Q product horizon."
        )
    annual_return_periods = _available_capital_return_periods(
        cr_index, period_type="annual"
    )[-5:]
    net_cash = _net_cash_index(balance_sheet_product, balance_sheet_shadow)

    allocation_summary: list[dict[str, Any]] = []
    for metric_id, label, number_format in _CAPITAL_ALLOCATION_ROWS:
        values: list[dict[str, Any]] = []
        if metric_id == "ending_net_cash":
            quarter_entry = net_cash.get(latest_quarter)
            annual_entry = net_cash.get(latest_annual.replace("-FY", "-Q4"))
            if quarter_entry is None or annual_entry is None:
                raise CapitalAllocationReturnExpansionError(
                    "Required current Summary/BS net-cash periods are unavailable."
                )
            values = [
                dict(quarter_entry),
                _terminal_entry(quarter_entry, target_period=latest_ttm),
                dict(annual_entry) | {"period": latest_annual, "display_period": _display_period(latest_annual)},
            ]
            context = "Summary/BS net cash; TTM column uses the terminal quarter balance."
        elif metric_id in {"free_cash_flow", "capital_expenditures"}:
            values = [
                _routed_normalized_summary_entry(
                    capital_return_index=cr_index,
                    metric_id=metric_id,
                    period=period,
                    period_type=period_type,
                    quarter_rows=quarter_rows,
                    annual_rows=annual_rows,
                )
                for period, period_type in zip(summary_periods, summary_period_types)
            ]
            if metric_id == "free_cash_flow":
                context = "Accepted FCF owner; exact compatible Q / TTM / FY facts."
            else:
                context = "Accepted reinvestment owner; capex shown as a positive cash outflow."
        else:
            values = [
                _capital_return_entry(
                    cr_index,
                    metric_id=metric_id,
                    period=period,
                    period_type=period_type,
                )
                for period, period_type in zip(summary_periods, summary_period_types)
            ]
            context = "Capital Return canonical historical buyback cash."
        allocation_row = _row(metric_id, label, number_format, values, state_context=context)
        if _has_available(allocation_row):
            allocation_summary.append(allocation_row)

    annual_allocation: list[dict[str, Any]] = []
    for metric_id, label, number_format in _CAPITAL_ALLOCATION_ROWS:
        values = []
        for period in annual_allocation_periods:
            if metric_id in {"free_cash_flow", "capital_expenditures"}:
                values.append(
                    _normalized_annual_entry(
                        annual_rows_by_period[period], metric_id=metric_id, period=period
                    )
                )
            elif metric_id == "repurchase_cash_program":
                values.append(
                    _capital_return_entry(
                        cr_index,
                        metric_id=metric_id,
                        period=period,
                        period_type="annual",
                    )
                )
            else:
                quarter_period = period.replace("-FY", "-Q4")
                item = net_cash.get(quarter_period)
                if item is None:
                    values.append(
                        _unavailable_entry(
                            period=period,
                            owner="summary_bs.net_cash",
                            unit="$m",
                            aggregation_role="point_in_time",
                            reason="No compatible accepted Summary/BS year-end net-cash field.",
                        )
                    )
                else:
                    values.append(
                        dict(item)
                        | {
                            "display_period": _display_period(period),
                            "period": period,
                            "source_period": quarter_period,
                        }
                    )
        context = {
            "free_cash_flow": "Accepted annual FCF owner.",
            "capital_expenditures": "Accepted annual capex owner; positive cash outflow.",
            "repurchase_cash_program": "Capital Return canonical owner; older unsupported years remain blank.",
            "ending_net_cash": "Summary/BS year-end point-in-time balance; unsupported years remain blank.",
        }[metric_id]
        allocation_row = _row(metric_id, label, number_format, values, state_context=context)
        if _has_available(allocation_row):
            annual_allocation.append(allocation_row)

    capital_return_summary: list[dict[str, Any]] = []
    typed_collection = bool(cr_index)
    declared_metrics = {metric_id for metric_id, _, _ in cr_index}
    for metric_id, label, number_format in _CAPITAL_RETURN_SUMMARY_ROWS:
        values = [
            _capital_return_entry(
                cr_index,
                metric_id=metric_id,
                period=period,
                period_type=period_type,
            )
            for period, period_type in zip(summary_periods, summary_period_types)
        ]
        context = {
            "repurchase_cash_program": "Historical program repurchase cash.",
            "accounting_program_shares_repurchased": "Accounting program shares; definition is consistent across periods.",
            "cash_per_program_share": "Repurchase cash / accounting program shares; not reported all-purchases price.",
            "share_issuance_sbc": "Source-backed issuance/SBC; missing never defaults to zero.",
            "net_share_reduction": "Positive = net retired; negative = net issued / dilution.",
            "buybacks_to_fcf": "Compatible-period buybacks / FCF; blank when the ratio is not meaningful.",
            "dividends_paid": "Unavailable remains blank; the workbook does not imply zero.",
            "authorization_remaining": "Point-in-time; TTM uses the terminal-quarter balance.",
        }[metric_id]
        row = _row(metric_id, label, number_format, values, state_context=context)
        if _has_available(row) or (
            typed_collection
            and metric_id == "dividends_paid"
            and metric_id in declared_metrics
        ):
            capital_return_summary.append(row)

    quarterly_history: list[dict[str, Any]] = []
    for metric_id, label, number_format in _CAPITAL_RETURN_HISTORY_ROWS:
        values = [
            _capital_return_entry(
                cr_index,
                metric_id=metric_id,
                period=period,
                period_type="quarter",
            )
            for period in quarterly_return_periods
        ]
        row = _row(
            metric_id,
            label,
            number_format,
            values,
            state_context="Twelve accepted fiscal quarters; unavailable history stays blank.",
        )
        if _has_available(row):
            quarterly_history.append(row)

    annual_history: list[dict[str, Any]] = []
    for metric_id, label, number_format in _CAPITAL_RETURN_HISTORY_ROWS:
        values = [
            _capital_return_entry(
                cr_index,
                metric_id=metric_id,
                period=period,
                period_type="annual",
            )
            for period in annual_return_periods
        ]
        row = _row(
            metric_id,
            label,
            number_format,
            values,
            state_context="Accepted annual Capital Return facts / exact derivations.",
        )
        if _has_available(row):
            annual_history.append(row)

    displayed_allocation = {row["row_key"] for row in allocation_summary}
    owner_map = (
        {"classification": "CANONICAL_OWNER_AVAILABLE", "displayed": "free_cash_flow" in displayed_allocation, "metric_id": "free_cash_flow", "owner": "normalized_company_data.free_cash_flow"},
        {"classification": "CANONICAL_OWNER_AVAILABLE", "displayed": "capital_expenditures" in displayed_allocation, "metric_id": "capital_expenditures", "owner": "normalized_company_data.capital_expenditures"},
        {"classification": "UNSUPPORTED_CURRENTLY", "displayed": False, "metric_id": "acquisitions_investments", "owner": "normalized_company_data.acquisitions_cash"},
        {"classification": "UNSUPPORTED_CURRENTLY", "displayed": False, "metric_id": "debt_repayment", "owner": "debt_liquidity.actual_debt_repayment"},
        {"classification": "UNSUPPORTED_CURRENTLY", "displayed": False, "metric_id": "debt_issuance_financing", "owner": "debt_liquidity.actual_debt_issuance"},
        {"classification": "CANONICAL_OWNER_AVAILABLE" if "repurchase_cash_program" in displayed_allocation else "UNAVAILABLE", "displayed": "repurchase_cash_program" in displayed_allocation, "metric_id": "repurchase_cash_program", "owner": "capital_return.repurchase_cash_program"},
        {"classification": "UNAVAILABLE", "displayed": False, "metric_id": "dividends_paid", "owner": "capital_return.dividends_paid"},
        {"classification": "CANONICAL_OWNER_AVAILABLE", "displayed": "ending_net_cash" in displayed_allocation, "metric_id": "ending_net_cash", "owner": "summary_bs.net_cash"},
    )

    annual_price_checks: list[dict[str, Any]] = []
    for period in annual_return_periods:
        price = _capital_return_entry(cr_index, metric_id="cash_per_program_share", period=period, period_type="annual")
        if price["status"] != "available":
            continue
        cash = _capital_return_entry(cr_index, metric_id="repurchase_cash_program", period=period, period_type="annual")
        shares = _capital_return_entry(cr_index, metric_id="accounting_program_shares_repurchased", period=period, period_type="annual")
        compatible = None if cash["value"] is None or shares["value"] in {None, 0.0} else cash["value"] / shares["value"]
        quarters = sorted(
            (
                (fiscal_period, row)
                for (metric, fiscal_period, period_type), row in cr_index.items()
                if metric == "cash_per_program_share"
                and period_type == "quarter"
                and fiscal_period.startswith(period[:4] + "-")
                and row.get("value") is not None
            ),
            key=lambda item: item[0],
        )
        simple_average = (
            sum(float(row["value"]) for _, row in quarters) / len(quarters) if quarters else None
        )
        annual_price_checks.append(
            {
                "annual_value": price["value"],
                "cash_divided_by_program_shares": compatible,
                "matches_accepted_derivation": compatible is not None and abs(float(price["value"]) - compatible) < 1e-6,
                "period": period,
                "quarterly_simple_average": simple_average,
                "simple_average_differs": simple_average is not None and abs(float(price["value"]) - simple_average) > 1e-6,
            }
        )
    if annual_price_checks and any(
        not row["matches_accepted_derivation"] for row in annual_price_checks
    ):
        raise CapitalAllocationReturnExpansionError(
            "Annual repurchase-price derivation no longer satisfies the accepted definition."
        )

    authorization_q = _capital_return_entry(
        cr_index, metric_id="authorization_remaining", period=latest_quarter, period_type="quarter"
    )
    authorization_ttm = _capital_return_entry(
        cr_index, metric_id="authorization_remaining", period=latest_ttm, period_type="ttm"
    )
    authorization_is_relevant = any(
        value["status"] == "available" for value in (authorization_q, authorization_ttm)
    )
    if authorization_is_relevant and authorization_q["value"] != authorization_ttm["value"]:
        raise CapitalAllocationReturnExpansionError(
            "TTM authorization is no longer the terminal point-in-time balance."
        )

    payload = {
        "annual_allocation_periods": annual_allocation_periods,
        "annual_capital_allocation_history": annual_allocation,
        "annual_capital_return_history": annual_history,
        "annual_return_periods": annual_return_periods,
        "capital_allocation_owner_map": owner_map,
        "capital_allocation_summary": allocation_summary,
        "capital_return_summary": capital_return_summary,
        "contract": INVESTOR_PRODUCT_CONTRACT,
        "current_45_slot_disposition": _slot_dispositions(cr_projection),
        "derivation_review": {
            "annual_average_price": annual_price_checks,
            "authorization_ttm_equals_terminal_quarter": (
                authorization_q["value"] == authorization_ttm["value"]
            ),
            "simple_average_of_quarterly_ratio_count": 0,
        },
        "quarterly_capital_return_history": quarterly_history,
        "quarterly_return_periods": quarterly_return_periods,
        "row_relevance_contract": {
            "blank_but_relevant": "typed collection plus explicit current-summary disclosure row",
            "contract": "conditional-investor-row-relevance@1",
            "omitted_not_relevant": "no available value and no explicit disclosure role",
            "visible": "at least one compatible accepted available value",
        },
        "summary_periods": summary_periods,
    }
    return CapitalAllocationReturnInvestorProduct(
        contract=INVESTOR_PRODUCT_CONTRACT,
        summary_periods=tuple(summary_periods),
        annual_allocation_periods=annual_allocation_periods,
        quarterly_return_periods=quarterly_return_periods,
        annual_return_periods=annual_return_periods,
        capital_allocation_summary=tuple(allocation_summary),
        annual_capital_allocation_history=tuple(annual_allocation),
        capital_return_summary=tuple(capital_return_summary),
        quarterly_capital_return_history=tuple(quarterly_history),
        annual_capital_return_history=tuple(annual_history),
        capital_allocation_owner_map=owner_map,
        current_45_slot_disposition=_slot_dispositions(cr_projection),
        derivation_review=payload["derivation_review"],
        row_relevance_contract=payload["row_relevance_contract"],
        product_digest=_digest(payload),
    )


def _mutation_dict(value: Any) -> dict[str, Any]:
    return asdict(value)


def _coordinate_bounds(reference: str) -> tuple[int, int, int, int]:
    def coordinate(value: str) -> tuple[int, int]:
        match = re.fullmatch(r"([A-Z]+)([0-9]+)", value)
        if match is None:
            raise CapitalAllocationReturnExpansionError(f"Invalid coordinate {value!r}.")
        column = 0
        for character in match.group(1):
            column = column * 26 + ord(character) - 64
        return column, int(match.group(2))

    left, _, right = reference.partition(":")
    if not right:
        right = left
    minimum_column, minimum_row = coordinate(left)
    maximum_column, maximum_row = coordinate(right)
    return minimum_column, minimum_row, maximum_column, maximum_row


def _intersects(left: str, right: str) -> bool:
    l_min_c, l_min_r, l_max_c, l_max_r = _coordinate_bounds(left)
    r_min_c, r_min_r, r_max_c, r_max_r = _coordinate_bounds(right)
    return not (
        l_max_c < r_min_c
        or r_max_c < l_min_c
        or l_max_r < r_min_r
        or r_max_r < l_min_r
    )


def _blank_legacy_merges(base_workbook: Path) -> tuple[str, ...]:
    merge_re = re.compile(rb'<mergeCell\b[^>]*\bref="([^"]+)"[^>]*/>')
    with ZipFile(base_workbook, "r") as archive:
        part = _sheet_part_map(archive).get("Valuation")
        if part is None:
            raise CapitalAllocationReturnExpansionError("Valuation sheet is missing.")
        data = archive.read(part)
        cells = _cell_elements(data)
    ranges = tuple(
        match.group(1).decode("ascii")
        for match in merge_re.finditer(data)
        if _intersects(match.group(1).decode("ascii"), VISIBLE_PRODUCT_RANGE)
    )
    for range_ref in ranges:
        minimum_column, minimum_row, maximum_column, maximum_row = _coordinate_bounds(range_ref)
        for column in range(minimum_column, maximum_column + 1):
            name = ""
            value = column
            while value:
                value, remainder = divmod(value - 1, 26)
                name = chr(65 + remainder) + name
            for row in range(minimum_row, maximum_row + 1):
                raw = cells.get(f"{name}{row}", b"")
                if any(token in raw for token in (b"<f", b"<v", b"<is")):
                    raise CapitalAllocationReturnExpansionError(
                        f"Refusing to retire a non-empty legacy merge: {range_ref}."
                    )
    return tuple(sorted(ranges))


def _put_text(
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    coordinate: str,
    value: str,
    *,
    owner: str,
    style_source: str | None = None,
) -> None:
    mutations[("Valuation", coordinate)] = FormulaAwareCellMutation(
        "Valuation",
        coordinate,
        "SET_VALUE" if value else "CLEAR_CONTENTS",
        value=value or None,
        value_kind="text" if value else None,
        style_source_cell=style_source,
        semantic_owner=owner,
    )


def _put_value(
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    coordinate: str,
    value: float | None,
    *,
    number_format: str,
    owner: str,
    style_source: str,
) -> None:
    mutations[("Valuation", coordinate)] = FormulaAwareCellMutation(
        "Valuation",
        coordinate,
        "SET_VALUE" if value is not None else "CLEAR_CONTENTS",
        value=_number_text(value) if value is not None else None,
        value_kind="number" if value is not None else None,
        number_format_code=number_format,
        style_source_cell=style_source,
        semantic_owner=owner,
    )


def _column_number(column: str) -> int:
    result = 0
    for character in column:
        result = result * 26 + ord(character) - 64
    return result


def _column_name(number: int) -> str:
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _range_columns(range_ref: str) -> tuple[str, ...]:
    left, right = range_ref.split(":")
    return tuple(
        _column_name(number)
        for number in range(_column_number(left), _column_number(right) + 1)
    )


def _block_mutations(
    *,
    section_key: str,
    rows: Sequence[Mapping[str, Any]],
    periods: Sequence[str],
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    merges: list[WorksheetMergeMutation],
    bindings: list[dict[str, Any]],
    support_records: list[dict[str, Any]],
) -> None:
    layout = _BLOCK_LAYOUT[section_key]
    title_row = int(layout["title_row"])
    header_row = int(layout["header_row"])
    first_data_row = int(layout["first_data_row"])
    period_columns = tuple(layout["period_columns"])
    if len(period_columns) != len(periods):
        raise CapitalAllocationReturnExpansionError(
            f"{section_key} period/column cardinality changed."
        )
    merges.append(WorksheetMergeMutation("Valuation", f"N{title_row}:AA{title_row}", "ADD"))
    for column in _range_columns("N:AA"):
        _put_text(
            mutations,
            f"{column}{title_row}",
            str(layout["title"]) if column == "N" else "",
            owner=f"capital_allocation_return.{section_key}.title",
            style_source="A151",
        )
    merges.append(WorksheetMergeMutation("Valuation", f"N{header_row}:O{header_row}", "ADD"))
    header_values = ("Metric", *(_display_period(period) for period in periods))
    for coordinate, value in zip(
        (f"N{header_row}", *(f"{column}{header_row}" for column in period_columns)),
        header_values,
    ):
        _put_text(
            mutations,
            coordinate,
            value,
            owner=f"capital_allocation_return.{section_key}.header",
            style_source="A153",
        )
    context_range = layout["context_range"]
    if context_range:
        start, end = str(context_range).split(":")
        merges.append(WorksheetMergeMutation("Valuation", f"{start}{header_row}:{end}{header_row}", "ADD"))
        _put_text(
            mutations,
            f"{start}{header_row}",
            "State / definition",
            owner=f"capital_allocation_return.{section_key}.header",
            style_source="A153",
        )
    for offset, row in enumerate(rows):
        target_row = first_data_row + offset
        merges.append(WorksheetMergeMutation("Valuation", f"N{target_row}:O{target_row}", "ADD"))
        _put_text(
            mutations,
            f"N{target_row}",
            str(row["label"]),
            owner=f"capital_allocation_return.{section_key}.{row['row_key']}.label",
            style_source="A154",
        )
        row_bindings: list[dict[str, Any]] = []
        for column, value in zip(period_columns, row["values"]):
            target_cell = f"{column}{target_row}"
            owner = str(value["owner"])
            _put_value(
                mutations,
                target_cell,
                value["value"],
                number_format=str(row["number_format"]),
                owner=f"{owner}.presentation_binding",
                style_source="B70" if layout["history"] else "B154",
            )
            binding = {
                "aggregation_role": value["aggregation_role"],
                "definition": value["definition"],
                "metric_id": row["row_key"],
                "owner": owner,
                "period": value["period"],
                "section": section_key,
                "source_classification": value["source_classification"],
                "source_identity": value["source_identity"],
                "source_period": value["source_period"],
                "source_ref": value["source_ref"],
                "status": value["status"],
                "target_cell": f"Valuation!{target_cell}",
                "unit": value["unit"],
                "value": value["value"],
            }
            row_bindings.append(binding)
            bindings.append(binding)
        if context_range:
            start, end = str(context_range).split(":")
            merges.append(WorksheetMergeMutation("Valuation", f"{start}{target_row}:{end}{target_row}", "ADD"))
            _put_text(
                mutations,
                f"{start}{target_row}",
                str(row["state_context"]),
                owner=f"capital_allocation_return.{section_key}.{row['row_key']}.context",
                style_source="E154",
            )
        support = {
            "bindings": row_bindings,
            "metric_id": row["row_key"],
            "section": section_key,
        }
        support_records.append(support | {"support_digest": _digest(support)})


@dataclass(frozen=True)
class CapitalAllocationReturnWorkbookProjectionPlan:
    contract: str
    base_workbook_sha256: str
    source_package_sha256: str
    balance_sheet_product_sha256: str
    balance_sheet_shadow_sha256: str
    investor_product: Mapping[str, Any]
    cell_mutations: tuple[FormulaAwareCellMutation, ...]
    merge_mutations: tuple[WorksheetMergeMutation, ...]
    row_mutations: tuple[WorksheetRowMutation, ...]
    bindings: tuple[dict[str, Any], ...]
    binding_plan_digest: str
    layout_plan_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "balance_sheet_product_sha256": self.balance_sheet_product_sha256,
            "balance_sheet_shadow_sha256": self.balance_sheet_shadow_sha256,
            "base_workbook_sha256": self.base_workbook_sha256,
            "binding_plan_digest": self.binding_plan_digest,
            "bindings": [dict(row) for row in self.bindings],
            "cell_mutations": [_mutation_dict(row) for row in self.cell_mutations],
            "contract": self.contract,
            "investor_product": dict(self.investor_product),
            "layout_plan_digest": self.layout_plan_digest,
            "merge_mutations": [_mutation_dict(row) for row in self.merge_mutations],
            "row_mutations": [_mutation_dict(row) for row in self.row_mutations],
            "source_package_sha256": self.source_package_sha256,
        }


def build_capital_allocation_return_workbook_projection_plan(
    *,
    package: Mapping[str, Any],
    source_package_path: Path | str,
    balance_sheet_product: Mapping[str, Any],
    balance_sheet_product_path: Path | str,
    balance_sheet_shadow: Mapping[str, Any],
    balance_sheet_shadow_path: Path | str,
    base_workbook: Path | str,
) -> CapitalAllocationReturnWorkbookProjectionPlan:
    base = Path(base_workbook)
    base_sha = sha256_file(base)
    if base_sha != EXPECTED_ACCEPTED_PREVIEW_SHA256:
        raise CapitalAllocationReturnExpansionError(
            f"Accepted Capital Return/Debt preview identity changed: {base_sha}."
        )
    investor_product = build_capital_allocation_return_investor_product(
        package=package,
        balance_sheet_product=balance_sheet_product,
        balance_sheet_shadow=balance_sheet_shadow,
    )
    expected_counts = {
        "capital_allocation_summary": 4,
        "annual_capital_allocation_history": 4,
        "capital_return_summary": 8,
        "quarterly_capital_return_history": 6,
        "annual_capital_return_history": 6,
    }
    for key, expected in expected_counts.items():
        if len(getattr(investor_product, key)) != expected:
            raise CapitalAllocationReturnExpansionError(
                f"ANF {key} row relevance changed: {len(getattr(investor_product, key))} != {expected}."
            )

    mutations: dict[tuple[str, str], FormulaAwareCellMutation] = {}
    merges = [
        WorksheetMergeMutation("Valuation", range_ref, "DELETE")
        for range_ref in _blank_legacy_merges(base)
    ]
    bindings: list[dict[str, Any]] = []
    support_records: list[dict[str, Any]] = []

    for row in range(79, 123):
        for column in range(14, 28):
            coordinate = f"{_column_name(column)}{row}"
            _put_text(
                mutations,
                coordinate,
                "",
                owner="capital_allocation_return.presentation_surface_reset",
            )

    merges.extend(
        (
            WorksheetMergeMutation("Valuation", "N79:AA79", "ADD"),
            WorksheetMergeMutation("Valuation", "N80:AA80", "ADD"),
        )
    )
    for column in _range_columns("N:AA"):
        _put_text(
            mutations,
            f"{column}79",
            "Capital Allocation & Capital Return" if column == "N" else "",
            owner="capital_allocation_return.presentation_title",
            style_source="A152",
        )
        _put_text(
            mutations,
            f"{column}80",
            "Historical, source-native presentation; no forward assumptions."
            if column == "N"
            else "",
            owner="capital_allocation_return.presentation_scope",
            style_source="O49",
        )

    section_payloads = (
        ("capital_allocation_summary", investor_product.capital_allocation_summary, investor_product.summary_periods),
        ("annual_capital_allocation_history", investor_product.annual_capital_allocation_history, investor_product.annual_allocation_periods),
        ("capital_return_summary", investor_product.capital_return_summary, investor_product.summary_periods),
        ("quarterly_capital_return_history", investor_product.quarterly_capital_return_history, investor_product.quarterly_return_periods),
        ("annual_capital_return_history", investor_product.annual_capital_return_history, investor_product.annual_return_periods),
    )
    for section_key, rows, periods in section_payloads:
        _block_mutations(
            section_key=section_key,
            rows=rows,
            periods=periods,
            mutations=mutations,
            merges=merges,
            bindings=bindings,
            support_records=support_records,
        )

    _put_text(
        mutations,
        "A151",
        "Capital allocation & return",
        owner="capital_allocation_return.navigation_section",
        style_source="A151",
    )
    _put_text(
        mutations,
        "A152",
        "See Capital Allocation & Capital Return beside the historical Valuation grid.",
        owner="capital_allocation_return.navigation_note",
        style_source="A152",
    )
    for row in range(153, 169):
        for column in range(1, 14):
            _put_text(
                mutations,
                f"{_column_name(column)}{row}",
                "",
                owner="capital_allocation_return.retired_current_surface",
            )
    for row in range(172, 187):
        for column in range(30, 42):
            _put_text(
                mutations,
                f"{_column_name(column)}{row}",
                "",
                owner="capital_allocation_return.retired_current_lineage_surface",
            )
    support_coordinates = tuple(
        f"{_column_name(column)}{row}"
        for row in range(153, 159)
        for column in range(1, 6)
    )[:28]
    if len(support_records) != 28:
        raise CapitalAllocationReturnExpansionError(
            f"Lineage support row universe changed: {len(support_records)} != 28."
        )
    for coordinate, support in zip(support_coordinates, support_records):
        _put_text(
            mutations,
            coordinate,
            _canonical_bytes(support).decode("utf-8"),
            owner=(
                f"capital_allocation_return.lineage.{support['section']}."
                f"{support['metric_id']}"
            ),
        )

    row_mutations = tuple(
        [
            WorksheetRowMutation("Valuation", 79, height=25.0),
            WorksheetRowMutation("Valuation", 80, height=19.5),
        ]
        + [
            WorksheetRowMutation(
                "Valuation",
                row,
                height=8.1 if row in {87, 94, 105, 114} else 21.0 if row in {81, 88, 95, 106, 115} else 19.5,
            )
            for row in range(81, 123)
        ]
        + [WorksheetRowMutation("Valuation", row, hidden=True) for row in range(153, 169)]
    )
    cell_mutations = tuple(
        sorted(mutations.values(), key=lambda row: (row.target_sheet, row.target_cell))
    )
    merge_mutations = tuple(
        sorted(merges, key=lambda row: (row.mode, row.range_ref))
    )
    binding_payload = {
        "bindings": bindings,
        "contract": WORKBOOK_PROJECTION_CONTRACT,
        "product_digest": investor_product.product_digest,
    }
    layout_payload = {
        "block_layout": _BLOCK_LAYOUT,
        "merge_mutations": [_mutation_dict(row) for row in merge_mutations],
        "row_mutations": [_mutation_dict(row) for row in row_mutations],
        "visible_product_range": VISIBLE_PRODUCT_RANGE,
    }
    payload = {
        "balance_sheet_product_sha256": sha256_file(Path(balance_sheet_product_path)),
        "balance_sheet_shadow_sha256": sha256_file(Path(balance_sheet_shadow_path)),
        "base_workbook_sha256": base_sha,
        "binding_plan_digest": _digest(binding_payload),
        "cell_mutations": [_mutation_dict(row) for row in cell_mutations],
        "contract": WORKBOOK_PROJECTION_CONTRACT,
        "investor_product_digest": investor_product.product_digest,
        "layout_plan_digest": _digest(layout_payload),
        "merge_mutations": [_mutation_dict(row) for row in merge_mutations],
        "row_mutations": [_mutation_dict(row) for row in row_mutations],
        "source_package_sha256": sha256_file(Path(source_package_path)),
    }
    if len(bindings) != 140:
        raise CapitalAllocationReturnExpansionError(
            f"Displayed binding universe changed: {len(bindings)} != 140."
        )
    if sum(row["status"] == "available" for row in bindings) != 110:
        raise CapitalAllocationReturnExpansionError(
            "Displayed available binding universe changed from 110."
        )
    return CapitalAllocationReturnWorkbookProjectionPlan(
        contract=WORKBOOK_PROJECTION_CONTRACT,
        base_workbook_sha256=base_sha,
        source_package_sha256=payload["source_package_sha256"],
        balance_sheet_product_sha256=payload["balance_sheet_product_sha256"],
        balance_sheet_shadow_sha256=payload["balance_sheet_shadow_sha256"],
        investor_product=investor_product.to_dict(),
        cell_mutations=cell_mutations,
        merge_mutations=merge_mutations,
        row_mutations=row_mutations,
        bindings=tuple(bindings),
        binding_plan_digest=payload["binding_plan_digest"],
        layout_plan_digest=payload["layout_plan_digest"],
    )


def materialize_capital_allocation_return_workbook_projection(
    *,
    plan: CapitalAllocationReturnWorkbookProjectionPlan,
    base_workbook: Path | str,
    output_workbook: Path | str,
) -> FormulaAwareMaterializationResult:
    if plan.contract != WORKBOOK_PROJECTION_CONTRACT:
        raise CapitalAllocationReturnExpansionError("Workbook projection contract changed.")
    return materialize_capital_return_debt_mutations(
        base_workbook=base_workbook,
        output_workbook=output_workbook,
        cell_mutations=plan.cell_mutations,
        merge_mutations=plan.merge_mutations,
        row_mutations=plan.row_mutations,
        expected_base_sha256=plan.base_workbook_sha256,
    )


__all__ = [
    "ANNUAL_ALLOCATION_HISTORY_LENGTH",
    "CAPITAL_ALLOCATION_OWNER_ROUTES",
    "CAPITAL_RETURN_ACTIVITY_FAMILIES",
    "CapitalAllocationReturnExpansionError",
    "CapitalAllocationReturnInvestorProduct",
    "CapitalAllocationReturnWorkbookProjectionPlan",
    "EXPECTED_ACCEPTED_PREVIEW_SHA256",
    "INVESTOR_PRODUCT_CONTRACT",
    "LINEAGE_SUPPORT_RANGE",
    "QUARTERLY_HISTORY_LENGTH",
    "VISIBLE_PRODUCT_RANGE",
    "WORKBOOK_PROJECTION_CONTRACT",
    "capital_allocation_owner_routing_review",
    "capital_return_activity_family_contract",
    "build_capital_allocation_return_investor_product",
    "build_capital_allocation_return_workbook_projection_plan",
    "materialize_capital_allocation_return_workbook_projection",
]
