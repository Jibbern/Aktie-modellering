"""Bounded Capital Return and Debt/Liquidity projection onto the Valuation golden.

The module owns exact workbook presentation bindings only.  Economic selection
remains in ``new_ticker_capital_return`` and ``new_ticker_debt_projection``;
the lossless materializer remains a package-level mutation primitive.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from zipfile import ZipFile

from pbi_xbrl.new_ticker_capital_return import (
    CapitalReturnWorkbookProjection,
    build_capital_return_workbook_projection,
)
from pbi_xbrl.new_ticker_debt_projection import (
    DebtWorkbookProjection,
    build_debt_workbook_projection,
)
from pbi_xbrl.longitudinal_memory.capital_return_debt_workbook_materialization import (
    FormulaAwareCellMutation,
    FormulaAwareMaterializationResult,
    WorkbookSheetStateMutation,
    WorksheetColumnMutation,
    WorksheetDimensionMutation,
    WorksheetRowMutation,
    WorksheetTableMutation,
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


CAPITAL_RETURN_DEBT_PROJECTION_CONTRACT = (
    "capital-return-debt-lossless-workbook-projection@1"
)
EXPECTED_VALUATION_GOLDEN_SHA256 = (
    "39fba7ae39a02fa9395cf25f103097f8c6d62ccbf3cf6a8ae25767babcb7fc1d"
)
EXPECTED_CAPITAL_RETURN_PROJECTION_DIGEST = (
    "1bc76fe1a71b4c2732472ad0c1d77bcff7ea7192cd6ce057566d984db8f00215"
)
EXPECTED_DEBT_PROJECTION_DIGEST = (
    "2f3da3650c0a09da51cef9bd00e240faf62a0dda04483781f72b2a3794fc7868"
)

CAPITAL_RETURN_VISIBLE_RANGE = "A152:M168"
CAPITAL_RETURN_PRODUCT_RANGE = "A154:E168"
CAPITAL_RETURN_SUPPORT_RANGE = "AD172:AO186"
CAPITAL_RETURN_SLOT_COUNT = 45
CURRENT_BUYBACK_TARGET = "M63"

_DEBT_PRODUCT_SHEETS = (
    "Debt_Profile",
    "Revolver_History",
    "Leverage_Liquidity",
    "Debt_Credit_Notes",
    "Debt_Maturity_Ladder",
)

_DEBT_TITLES = {
    "Debt_Profile": "Debt and liquidity profile",
    "Revolver_History": "Revolver history",
    "Leverage_Liquidity": "Leverage and liquidity",
    "Debt_Credit_Notes": "Debt and credit notes",
    "Debt_Maturity_Ladder": "Debt maturity ladder",
}

_DEBT_HEADERS = {
    "Debt_Profile": (
        "Category",
        "Item",
        "Facility / instrument",
        "Value",
        "Unit",
        "As of",
        "Expiry / maturity",
        "State",
        "Evidence key",
        "Definition / source",
    ),
    "Revolver_History": (
        "As of",
        "Published",
        "Facility",
        "Commitment",
        "Loan cap",
        "Drawn",
        "LOC",
        "Gross capacity",
        "Minimum excess",
        "Net availability",
        "Utilization",
        "Rate basis",
        "Expiry",
        "Covenant state",
        "Source state",
        "Evidence source",
    ),
    "Leverage_Liquidity": (
        "Period",
        "Cash",
        "Restricted cash",
        "Core debt",
        "Operating leases",
        "Net debt",
        "Revolver availability",
        "Liquidity",
        "Gross leverage",
        "Net leverage",
        "Interest coverage (unavailable)",
        "State",
        "Evidence key",
        "Period / definition",
    ),
    "Debt_Credit_Notes": (
        "Topic",
        "Facility / instrument",
        "As of",
        "Published",
        "Exact bounded note",
        "State",
        "Evidence key",
        "Source",
    ),
    "Debt_Maturity_Ladder": (
        "Instrument",
        "Maturity bucket",
        "Amount",
        "Unit",
        "Due date",
        "As of",
        "State",
        "Evidence source",
    ),
}

_DEBT_WIDTHS = {
    "Debt_Profile": (16.0, 34.0, 24.0, 12.0, 9.0, 13.0, 16.0, 22.0, 30.0, 44.0),
    "Revolver_History": (
        12.0,
        13.0,
        18.0,
        12.0,
        12.0,
        10.0,
        13.0,
        13.0,
        16.0,
        16.0,
        11.0,
        16.0,
        12.0,
        28.0,
        36.0,
        44.0,
    ),
    "Leverage_Liquidity": (
        11.0,
        13.0,
        13.0,
        11.0,
        14.0,
        11.0,
        20.0,
        13.0,
        12.0,
        12.0,
        26.0,
        30.0,
        38.0,
        52.0,
    ),
    "Debt_Credit_Notes": (34.0, 22.0, 12.0, 13.0, 64.0, 14.0, 38.0, 20.0),
    "Debt_Maturity_Ladder": (26.0, 16.0, 14.0, 10.0, 14.0, 13.0, 20.0, 40.0),
}

_DEBT_DIMENSIONS = {
    "Debt_Profile": "A1:J14",
    "Revolver_History": "A1:P15",
    "Leverage_Liquidity": "A1:N15",
    "Debt_Credit_Notes": "A1:H9",
    "Debt_Maturity_Ladder": "A1:H3",
}

_DEBT_TABLE_RANGES = {
    "Debt_Profile": "A3:J14",
    "Revolver_History": "A3:P15",
    "Leverage_Liquidity": "A3:N15",
    "Debt_Credit_Notes": "A3:H9",
}

_CAPITAL_RETURN_SUPPORT_FIELDS = (
    "row_key",
    "metric_id",
    "semantic_role",
    "latest_record_id",
    "ttm_record_id",
    "annual_record_id",
    "latest_evidence_ref",
    "ttm_evidence_ref",
    "annual_evidence_ref",
    "latest_classification",
    "ttm_classification",
    "annual_classification",
)


class CapitalReturnDebtWorkbookProjectionError(ValueError):
    """Fail-closed plan or materialization contract violation."""


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
        raise CapitalReturnDebtWorkbookProjectionError("Workbook numeric writes must be finite.")
    return format(parsed, "f")


def _column_name(number: int) -> str:
    if number < 1:
        raise CapitalReturnDebtWorkbookProjectionError("Column numbers must be positive.")
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _mutation_dict(value: Any) -> dict[str, Any]:
    return asdict(value)


def _existing_content_cells(base_workbook: Path, sheets: Sequence[str]) -> dict[str, tuple[str, ...]]:
    with ZipFile(base_workbook, "r") as archive:
        part_by_sheet = _sheet_part_map(archive)
        result: dict[str, tuple[str, ...]] = {}
        for sheet in sheets:
            if sheet not in part_by_sheet:
                raise CapitalReturnDebtWorkbookProjectionError(
                    f"Required workbook sheet is missing: {sheet!r}."
                )
            cells = _cell_elements(archive.read(part_by_sheet[sheet]))
            result[sheet] = tuple(
                sorted(
                    cells,
                    key=lambda coordinate: (
                        int("".join(character for character in coordinate if character.isdigit())),
                        coordinate,
                    ),
                )
            )
    return result


def _registered_capital_return_number_format(source_format: str) -> str:
    if source_format.startswith("#,##0.000"):
        return "#,##0.000"
    if source_format.startswith("#,##0.0"):
        return "#,##0.0"
    if source_format.startswith("0.00"):
        return "$0.00"
    if source_format.startswith("0.0%"):
        return "0.0%"
    raise CapitalReturnDebtWorkbookProjectionError(
        f"Unsupported Capital Return number-format role: {source_format!r}."
    )


def _capital_return_display_periods(
    latest_quarter: str,
    ttm_label: str,
    annual_label: str,
) -> tuple[str, str, str]:
    quarter = re.fullmatch(r"([0-9]{4})-Q([1-4])", latest_quarter)
    ttm = re.fullmatch(r"TTM through ([0-9]{4})-Q([1-4])", ttm_label)
    annual = re.fullmatch(r"([0-9]{4})-FY", annual_label)
    if quarter is None or ttm is None or annual is None:
        raise CapitalReturnDebtWorkbookProjectionError(
            "Capital Return period labels no longer satisfy the accepted display contract."
        )
    if quarter.groups() != ttm.groups():
        raise CapitalReturnDebtWorkbookProjectionError(
            "Capital Return latest-quarter and TTM terminal periods diverged."
        )
    return (
        f"Q{quarter.group(2)}'{quarter.group(1)[2:]}",
        f"TTM Q{ttm.group(2)}'{ttm.group(1)[2:]}",
        f"FY{annual.group(1)[2:]}",
    )


@dataclass(frozen=True)
class CapitalReturnDebtWorkbookProjectionPlan:
    contract: str
    base_workbook_sha256: str
    source_package_sha256: str
    capital_return_projection: Mapping[str, Any]
    debt_projection: Mapping[str, Any]
    cell_mutations: tuple[FormulaAwareCellMutation, ...]
    merge_mutations: tuple[WorksheetMergeMutation, ...]
    row_mutations: tuple[WorksheetRowMutation, ...]
    column_mutations: tuple[WorksheetColumnMutation, ...]
    dimension_mutations: tuple[WorksheetDimensionMutation, ...]
    sheet_state_mutations: tuple[WorkbookSheetStateMutation, ...]
    table_mutations: tuple[WorksheetTableMutation, ...]
    binding_plan_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_workbook_sha256": self.base_workbook_sha256,
            "binding_plan_digest": self.binding_plan_digest,
            "capital_return_projection": dict(self.capital_return_projection),
            "cell_mutations": [_mutation_dict(row) for row in self.cell_mutations],
            "column_mutations": [_mutation_dict(row) for row in self.column_mutations],
            "contract": self.contract,
            "debt_projection": dict(self.debt_projection),
            "dimension_mutations": [_mutation_dict(row) for row in self.dimension_mutations],
            "merge_mutations": [_mutation_dict(row) for row in self.merge_mutations],
            "row_mutations": [_mutation_dict(row) for row in self.row_mutations],
            "sheet_state_mutations": [
                _mutation_dict(row) for row in self.sheet_state_mutations
            ],
            "source_package_sha256": self.source_package_sha256,
            "table_mutations": [_mutation_dict(row) for row in self.table_mutations],
        }


def _capital_return_mutations(
    projection: CapitalReturnWorkbookProjection,
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
) -> tuple[WorksheetMergeMutation, ...]:
    def put(item: FormulaAwareCellMutation) -> None:
        mutations[(item.target_sheet, item.target_cell)] = item

    for row in range(152, 169):
        for column in range(1, 14):
            coordinate = f"{_column_name(column)}{row}"
            if row == 152:
                source = "A192"
            elif row == 153:
                source = "A193" if column <= 4 else "F193"
            else:
                source = "A194" if column == 1 else "B194" if column <= 4 else "F194"
            put(
                FormulaAwareCellMutation(
                    "Valuation",
                    coordinate,
                    "CLEAR_CONTENTS",
                    style_source_cell=source,
                    semantic_owner="capital_return.presentation",
                )
            )

    put(
        FormulaAwareCellMutation(
            "Valuation",
            "A152",
            "SET_VALUE",
            value="Capital Return",
            value_kind="text",
            style_source_cell="A192",
            semantic_owner="capital_return.presentation_title",
        )
    )
    display_periods = _capital_return_display_periods(
        projection.latest_quarter_label,
        projection.ttm_label,
        projection.annual_label,
    )
    headers = (
        "Metric",
        *display_periods,
        "State / definition",
    )
    for column, value in enumerate(headers, start=1):
        put(
            FormulaAwareCellMutation(
                "Valuation",
                f"{_column_name(column)}153",
                "SET_VALUE",
                value=value,
                value_kind="text",
                style_source_cell="A193" if column <= 4 else "F193",
                semantic_owner="capital_return.presentation_header",
            )
        )

    if len(projection.product_rows) != 15:
        raise CapitalReturnDebtWorkbookProjectionError("Capital Return must contain 15 product rows.")
    slot_count = 0
    for offset, row in enumerate(projection.product_rows, start=154):
        put(
            FormulaAwareCellMutation(
                "Valuation",
                f"A{offset}",
                "SET_VALUE",
                value=str(row["metric"]),
                value_kind="text",
                style_source_cell="A194",
                semantic_owner=f"capital_return.{row['row_key']}",
            )
        )
        number_format = _registered_capital_return_number_format(str(row["number_format"]))
        for column, field in zip(("B", "C", "D"), ("latest_quarter", "ttm", "latest_completed_year")):
            value = row[field]
            slot_count += 1
            if value is None:
                put(
                    FormulaAwareCellMutation(
                        "Valuation",
                        f"{column}{offset}",
                        "CLEAR_CONTENTS",
                        style_source_cell="B194",
                        number_format_code=number_format,
                        semantic_owner=f"capital_return.{row['row_key']}.{field}",
                    )
                )
            else:
                put(
                    FormulaAwareCellMutation(
                        "Valuation",
                        f"{column}{offset}",
                        "SET_VALUE",
                        value=_number_text(value),
                        value_kind="number",
                        style_source_cell="B194",
                        number_format_code=number_format,
                        semantic_owner=f"capital_return.{row['row_key']}.{field}",
                    )
                )
        put(
            FormulaAwareCellMutation(
                "Valuation",
                f"E{offset}",
                "SET_VALUE",
                value=str(row["state_context"]),
                value_kind="text",
                style_source_cell="F194",
                semantic_owner=f"capital_return.{row['row_key']}.state_context",
            )
        )
    if slot_count != CAPITAL_RETURN_SLOT_COUNT:
        raise CapitalReturnDebtWorkbookProjectionError("Capital Return slot count changed.")

    if len(projection.support_rows) != 15:
        raise CapitalReturnDebtWorkbookProjectionError("Capital Return support must contain 15 rows.")
    for row_number, support in enumerate(projection.support_rows, start=172):
        for column_number, field in enumerate(_CAPITAL_RETURN_SUPPORT_FIELDS, start=30):
            coordinate = f"{_column_name(column_number)}{row_number}"
            value = str(support[field] or "")
            put(
                FormulaAwareCellMutation(
                    "Valuation",
                    coordinate,
                    "SET_VALUE" if value else "CLEAR_CONTENTS",
                    value=value or None,
                    value_kind="text" if value else None,
                    semantic_owner=f"capital_return.lineage.{support['row_key']}.{field}",
                )
            )

    buyback = next(
        row for row in projection.product_rows if row["row_key"] == "repurchase_cash_program"
    )
    if buyback["ttm"] is None:
        raise CapitalReturnDebtWorkbookProjectionError("Current TTM buyback value is unavailable.")
    put(
        FormulaAwareCellMutation(
            "Valuation",
            CURRENT_BUYBACK_TARGET,
            "SET_VALUE",
            value=_number_text(buyback["ttm"]),
            value_kind="number",
            semantic_owner="capital_return.repurchase_cash_program.ttm.current_consumer",
        )
    )

    merge_mutations = [
        WorksheetMergeMutation("Valuation", range_ref, "DELETE")
        for range_ref in ("B152:K152", "B153:K153", "B155:K155", "B156:K156", "A168:I168")
    ]
    merge_mutations.extend(
        [WorksheetMergeMutation("Valuation", "A152:M152", "ADD")]
        + [WorksheetMergeMutation("Valuation", "E153:M153", "ADD")]
        + [
            WorksheetMergeMutation("Valuation", f"E{row}:M{row}", "ADD")
            for row in range(154, 169)
        ]
    )
    return tuple(merge_mutations)


def _put_text(
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    sheet: str,
    coordinate: str,
    value: str,
    *,
    owner: str,
    style_source: str,
    style_source_sheet: str | None = None,
) -> None:
    mutations[(sheet, coordinate)] = FormulaAwareCellMutation(
        sheet,
        coordinate,
        "SET_VALUE" if value else "CLEAR_CONTENTS",
        value=value or None,
        value_kind="text" if value else None,
        style_source_cell=style_source,
        style_source_sheet=style_source_sheet,
        semantic_owner=owner,
    )


def _put_number(
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    sheet: str,
    coordinate: str,
    value: float | None,
    *,
    owner: str,
    number_format: str = "#,##0.000",
) -> None:
    mutations[(sheet, coordinate)] = FormulaAwareCellMutation(
        sheet,
        coordinate,
        "SET_VALUE" if value is not None else "CLEAR_CONTENTS",
        value=_number_text(value) if value is not None else None,
        value_kind="number" if value is not None else None,
        style_source_cell="A7",
        style_source_sheet="ANF_Investment_Case",
        number_format_code=number_format,
        semantic_owner=owner,
    )


def _put_formula(
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    sheet: str,
    coordinate: str,
    formula: str,
    *,
    owner: str,
    number_format: str,
) -> None:
    mutations[(sheet, coordinate)] = FormulaAwareCellMutation(
        sheet,
        coordinate,
        "SET_FORMULA",
        value=formula,
        style_source_cell="B77",
        style_source_sheet="ANF_Investment_Case",
        number_format_code=number_format,
        semantic_owner=owner,
    )


def _debt_mutations(
    projection: DebtWorkbookProjection,
    existing: Mapping[str, tuple[str, ...]],
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
) -> tuple[
    tuple[WorksheetRowMutation, ...],
    tuple[WorksheetColumnMutation, ...],
    tuple[WorksheetDimensionMutation, ...],
    tuple[WorkbookSheetStateMutation, ...],
    tuple[WorksheetTableMutation, ...],
]:
    if projection.blocking_issues:
        raise CapitalReturnDebtWorkbookProjectionError(
            f"Debt projection has blocking issues: {projection.blocking_issues!r}."
        )

    for sheet, coordinates in existing.items():
        for coordinate in coordinates:
            mutations[(sheet, coordinate)] = FormulaAwareCellMutation(
                sheet,
                coordinate,
                "REMOVE_CELL",
                semantic_owner="debt_liquidity.retired_legacy_surface",
            )

    capacities = {
        "Debt_Profile": 11,
        "Revolver_History": 12,
        "Leverage_Liquidity": 12,
        "Debt_Credit_Notes": 6,
        "Debt_Maturity_Ladder": 0,
    }
    for sheet, headers in _DEBT_HEADERS.items():
        last_column = len(headers)
        for column in range(1, last_column + 1):
            coordinate = f"{_column_name(column)}1"
            _put_text(
                mutations,
                sheet,
                coordinate,
                _DEBT_TITLES[sheet] if column == 1 else "",
                owner="debt_liquidity.presentation_title",
                style_source="A1",
                style_source_sheet="ANF_Investment_Case",
            )
            _put_text(
                mutations,
                sheet,
                f"{_column_name(column)}2",
                "",
                owner="debt_liquidity.presentation_spacer",
                style_source="B7",
                style_source_sheet="ANF_Investment_Case",
            )
            _put_text(
                mutations,
                sheet,
                f"{_column_name(column)}3",
                headers[column - 1],
                owner="debt_liquidity.presentation_header",
                style_source="A4",
                style_source_sheet="ANF_Investment_Case",
            )
        for row in range(4, 4 + capacities[sheet]):
            for column in range(1, last_column + 1):
                _put_text(
                    mutations,
                    sheet,
                    f"{_column_name(column)}{row}",
                    "",
                    owner="debt_liquidity.presentation_capacity",
                    style_source="B7",
                    style_source_sheet="ANF_Investment_Case",
                )

    for row_number, row in enumerate(projection.debt_profile_rows, start=4):
        values = (
            row.category,
            row.item,
            row.facility_or_instrument,
            row.value,
            row.unit,
            row.as_of_date,
            row.expiry_or_maturity,
            row.state,
            row.evidence_key,
            row.definition_or_source,
        )
        for column, value in enumerate(values, start=1):
            coordinate = f"{_column_name(column)}{row_number}"
            owner = f"debt_liquidity.debt_profile.{row.row_key}"
            if column == 4:
                _put_number(mutations, "Debt_Profile", coordinate, value, owner=owner)
            else:
                _put_text(
                    mutations,
                    "Debt_Profile",
                    coordinate,
                    str(value or ""),
                    owner=owner,
                    style_source="A7" if column not in {9, 10} else "B7",
                    style_source_sheet="ANF_Investment_Case",
                )

    for row_number, row in enumerate(projection.revolver_history_rows, start=4):
        direct = (
            row.as_of_date,
            row.publication_date,
            row.facility,
            row.commitment,
            row.loan_cap,
            row.drawn,
            row.letters_of_credit,
            row.gross_capacity,
            row.minimum_excess,
            row.net_availability,
        )
        for column, value in enumerate(direct, start=1):
            coordinate = f"{_column_name(column)}{row_number}"
            owner = f"debt_liquidity.revolver_history.{row.row_key}"
            if column >= 4:
                _put_number(mutations, "Revolver_History", coordinate, value, owner=owner)
            else:
                _put_text(
                    mutations,
                    "Revolver_History",
                    coordinate,
                    str(value or ""),
                    owner=owner,
                    style_source="A7",
                    style_source_sheet="ANF_Investment_Case",
                )
        _put_formula(
            mutations,
            "Revolver_History",
            f"K{row_number}",
            f'IFERROR(IF(OR(NOT(ISNUMBER(F{row_number})),NOT(ISNUMBER(D{row_number})),D{row_number}=0),"",F{row_number}/D{row_number}),"")',
            owner="debt_liquidity.presentation.revolver_utilization",
            number_format="0.0%",
        )
        for column, value in zip(
            ("L", "M", "N", "O", "P"),
            (row.rate_basis, row.expiry, row.covenant_state, row.source_state, row.evidence_source),
        ):
            _put_text(
                mutations,
                "Revolver_History",
                f"{column}{row_number}",
                str(value or ""),
                owner=f"debt_liquidity.revolver_history.{row.row_key}",
                style_source="B7" if column == "P" else "A7",
                style_source_sheet="ANF_Investment_Case",
            )

    for row_number, row in enumerate(projection.leverage_liquidity_rows, start=4):
        _put_text(
            mutations,
            "Leverage_Liquidity",
            f"A{row_number}",
            row.period,
            owner=f"debt_liquidity.leverage.{row.row_key}",
            style_source="A7",
            style_source_sheet="ANF_Investment_Case",
        )
        for column, value in zip(
            ("B", "C", "D", "E", "G"),
            (row.cash, row.restricted_cash, row.core_debt, row.operating_leases, row.revolver_availability),
        ):
            _put_number(
                mutations,
                "Leverage_Liquidity",
                f"{column}{row_number}",
                value,
                owner=f"debt_liquidity.leverage.{row.row_key}",
            )
        _put_formula(
            mutations,
            "Leverage_Liquidity",
            f"F{row_number}",
            f'IFERROR(IF(OR(NOT(ISNUMBER(D{row_number})),NOT(ISNUMBER(B{row_number}))),"",D{row_number}-B{row_number}),"")',
            owner="debt_liquidity.presentation.core_net_debt",
            number_format="#,##0.000",
        )
        _put_formula(
            mutations,
            "Leverage_Liquidity",
            f"H{row_number}",
            f'IFERROR(IF(OR(NOT(ISNUMBER(B{row_number})),NOT(ISNUMBER(G{row_number}))),"",B{row_number}+G{row_number}),"")',
            owner="debt_liquidity.presentation.same_date_liquidity",
            number_format="#,##0.000",
        )
        for column, numerator in (("I", f"D{row_number}"), ("J", f"F{row_number}")):
            _put_formula(
                mutations,
                "Leverage_Liquidity",
                f"{column}{row_number}",
                f'IFERROR(IF(OR($A{row_number}="",COUNTIF(Valuation!$B$6:$M$6,$A{row_number})<>1,NOT(ISNUMBER({numerator})),NOT(ISNUMBER(INDEX(Valuation!$B$21:$M$21,1,MATCH($A{row_number},Valuation!$B$6:$M$6,0)))),INDEX(Valuation!$B$21:$M$21,1,MATCH($A{row_number},Valuation!$B$6:$M$6,0))=0),"",{numerator}/INDEX(Valuation!$B$21:$M$21,1,MATCH($A{row_number},Valuation!$B$6:$M$6,0))),"")',
                owner=(
                    "debt_liquidity.presentation.gross_leverage"
                    if column == "I"
                    else "debt_liquidity.presentation.net_leverage"
                ),
                number_format="0.0x",
            )
        _put_text(
            mutations,
            "Leverage_Liquidity",
            f"K{row_number}",
            "",
            owner="debt_liquidity.retired_invalid_interest_coverage",
            style_source="A7",
            style_source_sheet="ANF_Investment_Case",
        )
        for column, value in zip(
            ("L", "M", "N"),
            (row.disposition_state, row.evidence_key, row.component_period_explanation),
        ):
            _put_text(
                mutations,
                "Leverage_Liquidity",
                f"{column}{row_number}",
                str(value or ""),
                owner=f"debt_liquidity.leverage.{row.row_key}",
                style_source="B7" if column in {"M", "N"} else "A7",
                style_source_sheet="ANF_Investment_Case",
            )

    for row_number, row in enumerate(projection.debt_credit_note_rows, start=4):
        values = (
            row.topic,
            row.facility_or_instrument,
            row.as_of_date,
            row.publication_date,
            row.exact_bounded_note,
            row.state,
            row.evidence_key,
            row.source,
        )
        for column, value in enumerate(values, start=1):
            _put_text(
                mutations,
                "Debt_Credit_Notes",
                f"{_column_name(column)}{row_number}",
                str(value or ""),
                owner=f"debt_liquidity.credit_note.{row.row_key}",
                style_source="B7" if column in {1, 5, 7} else "A7",
                style_source_sheet="ANF_Investment_Case",
            )

    row_mutations: list[WorksheetRowMutation] = []
    for sheet in _DEBT_PRODUCT_SHEETS:
        row_mutations.extend(
            (
                WorksheetRowMutation(sheet, 1, height=24.0),
                WorksheetRowMutation(sheet, 2, height=8.1),
                WorksheetRowMutation(
                    sheet,
                    3,
                    height=36.0 if sheet in {"Revolver_History", "Leverage_Liquidity"} else 33.95,
                ),
            )
        )
        if sheet == "Debt_Profile":
            row_mutations.extend(
                WorksheetRowMutation(sheet, row, height=32.1) for row in range(4, 15)
            )
        elif sheet == "Revolver_History":
            row_mutations.extend(
                WorksheetRowMutation(sheet, row, height=32.1) for row in range(4, 16)
            )
        elif sheet == "Leverage_Liquidity":
            row_mutations.extend(
                WorksheetRowMutation(sheet, row, height=48.0) for row in range(4, 16)
            )
        elif sheet == "Debt_Credit_Notes":
            row_mutations.extend(
                WorksheetRowMutation(sheet, row, height=48.0) for row in range(4, 10)
            )

    column_mutations = tuple(
        WorksheetColumnMutation(sheet, column, width)
        for sheet, widths in _DEBT_WIDTHS.items()
        for column, width in enumerate(widths, start=1)
    )
    dimension_mutations = tuple(
        WorksheetDimensionMutation(sheet, target, trim_empty_tail=True)
        for sheet, target in _DEBT_DIMENSIONS.items()
    )
    sheet_state_mutations = tuple(
        WorkbookSheetStateMutation(sheet, state)
        for sheet, state in projection.sheet_states
    )
    table_mutations = tuple(
        WorksheetTableMutation(
            sheet,
            _DEBT_TABLE_RANGES[sheet],
            tuple(_DEBT_HEADERS[sheet]),
            show_row_stripes=False,
        )
        for sheet in _DEBT_TABLE_RANGES
    )
    return (
        tuple(row_mutations),
        column_mutations,
        dimension_mutations,
        sheet_state_mutations,
        table_mutations,
    )


def build_capital_return_debt_workbook_projection_plan(
    *,
    package: Mapping[str, Any],
    source_package_path: Path | str,
    base_workbook: Path | str,
) -> CapitalReturnDebtWorkbookProjectionPlan:
    base = Path(base_workbook)
    package_path = Path(source_package_path)
    base_sha = sha256_file(base)
    if base_sha != EXPECTED_VALUATION_GOLDEN_SHA256:
        raise CapitalReturnDebtWorkbookProjectionError(
            f"Valuation golden identity changed: {base_sha}."
        )
    package_sha = sha256_file(package_path)
    capital_return = build_capital_return_workbook_projection(package)
    debt = build_debt_workbook_projection(package)
    if capital_return.projection_digest != EXPECTED_CAPITAL_RETURN_PROJECTION_DIGEST:
        raise CapitalReturnDebtWorkbookProjectionError(
            f"Capital Return projection digest changed: {capital_return.projection_digest}."
        )
    if debt.projection_digest != EXPECTED_DEBT_PROJECTION_DIGEST:
        raise CapitalReturnDebtWorkbookProjectionError(
            f"Debt projection digest changed: {debt.projection_digest}."
        )

    mutations: dict[tuple[str, str], FormulaAwareCellMutation] = {}
    merge_mutations = _capital_return_mutations(capital_return, mutations)
    existing = _existing_content_cells(base, _DEBT_PRODUCT_SHEETS)
    (
        row_mutations,
        column_mutations,
        dimension_mutations,
        sheet_state_mutations,
        table_mutations,
    ) = _debt_mutations(debt, existing, mutations)

    cell_mutations = tuple(
        sorted(
            mutations.values(),
            key=lambda row: (row.target_sheet, row.target_cell),
        )
    )
    payload = {
        "base_workbook_sha256": base_sha,
        "capital_return_projection_digest": capital_return.projection_digest,
        "cell_mutations": [_mutation_dict(row) for row in cell_mutations],
        "column_mutations": [_mutation_dict(row) for row in column_mutations],
        "contract": CAPITAL_RETURN_DEBT_PROJECTION_CONTRACT,
        "debt_projection_digest": debt.projection_digest,
        "dimension_mutations": [_mutation_dict(row) for row in dimension_mutations],
        "merge_mutations": [_mutation_dict(row) for row in merge_mutations],
        "row_mutations": [_mutation_dict(row) for row in row_mutations],
        "sheet_state_mutations": [_mutation_dict(row) for row in sheet_state_mutations],
        "source_package_sha256": package_sha,
        "table_mutations": [_mutation_dict(row) for row in table_mutations],
    }
    return CapitalReturnDebtWorkbookProjectionPlan(
        contract=CAPITAL_RETURN_DEBT_PROJECTION_CONTRACT,
        base_workbook_sha256=base_sha,
        source_package_sha256=package_sha,
        capital_return_projection=capital_return.to_dict(),
        debt_projection=debt.to_dict(),
        cell_mutations=cell_mutations,
        merge_mutations=merge_mutations,
        row_mutations=row_mutations,
        column_mutations=column_mutations,
        dimension_mutations=dimension_mutations,
        sheet_state_mutations=sheet_state_mutations,
        table_mutations=table_mutations,
        binding_plan_digest=_digest(payload),
    )


def materialize_capital_return_debt_workbook_projection(
    *,
    plan: CapitalReturnDebtWorkbookProjectionPlan,
    base_workbook: Path | str,
    output_workbook: Path | str,
) -> FormulaAwareMaterializationResult:
    if plan.contract != CAPITAL_RETURN_DEBT_PROJECTION_CONTRACT:
        raise CapitalReturnDebtWorkbookProjectionError("Projection contract changed.")
    return materialize_capital_return_debt_mutations(
        base_workbook=base_workbook,
        output_workbook=output_workbook,
        cell_mutations=plan.cell_mutations,
        merge_mutations=plan.merge_mutations,
        row_mutations=plan.row_mutations,
        column_mutations=plan.column_mutations,
        dimension_mutations=plan.dimension_mutations,
        sheet_state_mutations=plan.sheet_state_mutations,
        table_mutations=plan.table_mutations,
        expected_base_sha256=plan.base_workbook_sha256,
    )


__all__ = [
    "CAPITAL_RETURN_DEBT_PROJECTION_CONTRACT",
    "CAPITAL_RETURN_PRODUCT_RANGE",
    "CAPITAL_RETURN_SLOT_COUNT",
    "CAPITAL_RETURN_SUPPORT_RANGE",
    "CAPITAL_RETURN_VISIBLE_RANGE",
    "CURRENT_BUYBACK_TARGET",
    "CapitalReturnDebtWorkbookProjectionError",
    "CapitalReturnDebtWorkbookProjectionPlan",
    "EXPECTED_CAPITAL_RETURN_PROJECTION_DIGEST",
    "EXPECTED_DEBT_PROJECTION_DIGEST",
    "EXPECTED_VALUATION_GOLDEN_SHA256",
    "build_capital_return_debt_workbook_projection_plan",
    "materialize_capital_return_debt_workbook_projection",
]
