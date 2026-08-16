"""Bounded Valuation product cleanup and Capital Allocation/Return relayout.

The module is a presentation-only consumer over the already accepted Capital
Allocation / Capital Return investor product.  It deliberately retires obsolete
Valuation surfaces, relocates the accepted historical product into ``A:M``, and
stores row-level lineage in hidden support rows.  It performs no source
selection and creates no economic formulas or owners.
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

from pbi_xbrl.longitudinal_memory.capital_allocation_return_product_expansion import (
    CapitalAllocationReturnInvestorProduct,
    build_capital_allocation_return_investor_product,
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


CLEANUP_CONTRACT = "valuation-capital-product-cleanup@1"
EXPECTED_EXPANDED_PREVIEW_SHA256 = (
    "90bdaf302fa522686fd1b83c039e5fd13437d336a96779e1c5b3738e65d1a085"
)
EXPECTED_INVESTOR_PRODUCT_DIGEST = (
    "09160adb781a2efa44a91f77ee988f1f9ffce0afa4d1caa465a91f4f44bbcfbd"
)
VISIBLE_CAPITAL_RANGE = "A126:M166"
HIDDEN_LINEAGE_RANGE = "A270:A297"

_AMOUNT_FORMAT = "#,##0.0"
_SHARES_FORMAT = "#,##0.000"
_PRICE_FORMAT = "$0.00"
_PERCENT_FORMAT = "0.0%"

_STYLE_SOURCE_BY_FORMAT = {
    _AMOUNT_FORMAT: "T108",
    _SHARES_FORMAT: "B70",
    _PRICE_FORMAT: "T110",
    _PERCENT_FORMAT: "T113",
}

_RETIREMENT_SURFACES = (
    "O48:AC49",
    "N79:AA122",
    "A126:M168",
    "N137:R143",
    "A169:M188",
    "A192:AO200",
    "AI139:AI139",
)

_LAYOUT = {
    "capital_allocation": {"title_row": 126, "title": "Capital Allocation"},
    "capital_allocation_summary": {
        "title_row": 127,
        "title": "Summary",
        "header_row": 128,
        "first_data_row": 129,
        "period_columns": ("B", "C", "D"),
    },
    "annual_capital_allocation_history": {
        "title_row": 133,
        "title": "Annual History",
        "header_row": 134,
        "first_data_row": 135,
        "period_columns": ("B", "C", "D", "E", "F"),
    },
    "allocation_return_spacer": {"row": 139},
    "capital_return": {"title_row": 140, "title": "Capital Return"},
    "capital_return_summary": {
        "title_row": 141,
        "title": "Summary",
        "header_row": 142,
        "first_data_row": 143,
        "period_columns": ("B", "C", "D"),
    },
    "quarterly_capital_return_history": {
        "title_row": 151,
        "title": "Quarterly History",
        "header_row": 152,
        "first_data_row": 153,
        "period_columns": tuple(chr(ord("B") + offset) for offset in range(12)),
    },
    "annual_capital_return_history": {
        "title_row": 159,
        "title": "Annual History",
        "header_row": 160,
        "first_data_row": 161,
        "period_columns": ("B", "C"),
    },
}


class ValuationCapitalProductCleanupError(ValueError):
    """Fail-closed cleanup contract violation."""


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
    decimal = Decimal(str(value))
    text = format(decimal, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


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


def _coordinate_bounds(reference: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)(?::([A-Z]+)([1-9][0-9]*))?", reference)
    if match is None:
        raise ValuationCapitalProductCleanupError(f"Invalid range {reference!r}.")
    minimum_column = _column_number(match.group(1))
    minimum_row = int(match.group(2))
    maximum_column = _column_number(match.group(3) or match.group(1))
    maximum_row = int(match.group(4) or match.group(2))
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


def _coordinate_in_range(coordinate: str, range_ref: str) -> bool:
    column, row, _, _ = _coordinate_bounds(coordinate)
    minimum_column, minimum_row, maximum_column, maximum_row = _coordinate_bounds(range_ref)
    return minimum_column <= column <= maximum_column and minimum_row <= row <= maximum_row


def _display_period(period: str) -> str:
    quarter = re.fullmatch(r"([0-9]{4})-Q([1-4])", period)
    if quarter:
        return f"Q{quarter.group(2)}'{quarter.group(1)[2:]}"
    annual = re.fullmatch(r"([0-9]{4})-FY", period)
    if annual:
        return f"FY{annual.group(1)[2:]}"
    ttm = re.fullmatch(r"TTM through ([0-9]{4})-Q([1-4])", period)
    if ttm:
        return f"TTM Q{ttm.group(2)}'{ttm.group(1)[2:]}"
    raise ValuationCapitalProductCleanupError(f"Unsupported display period {period!r}.")


def _mutation_dict(value: Any) -> dict[str, Any]:
    return asdict(value)


def _base_valuation_state(base_workbook: Path) -> tuple[set[str], tuple[str, ...]]:
    merge_re = re.compile(rb'<mergeCell\b[^>]*\bref="([^"]+)"[^>]*/>')
    with ZipFile(base_workbook, "r") as archive:
        part = _sheet_part_map(archive).get("Valuation")
        if part is None:
            raise ValuationCapitalProductCleanupError("Valuation sheet is missing.")
        worksheet = archive.read(part)
    cells = set(_cell_elements(worksheet))
    merges = tuple(match.group(1).decode("ascii") for match in merge_re.finditer(worksheet))
    return cells, merges


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
) -> None:
    style_source = _STYLE_SOURCE_BY_FORMAT.get(number_format)
    if style_source is None:
        raise ValuationCapitalProductCleanupError(
            f"No read-only Valuation style is registered for {number_format!r}."
        )
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


def _fill_title_row(
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    merges: list[WorksheetMergeMutation],
    *,
    row: int,
    title: str,
    major: bool,
) -> None:
    style_source = "A122" if major else "A69"
    for column in range(1, 14):
        coordinate = f"{_column_name(column)}{row}"
        _put_text(
            mutations,
            coordinate,
            title if column == 1 else "",
            owner=f"valuation_capital_cleanup.heading.{title.lower().replace(' ', '_')}",
            style_source=style_source,
        )
    merges.append(WorksheetMergeMutation("Valuation", f"A{row}:M{row}", "ADD"))


def _fill_table(
    *,
    section_key: str,
    rows: Sequence[Mapping[str, Any]],
    periods: Sequence[str],
    mutations: dict[tuple[str, str], FormulaAwareCellMutation],
    merges: list[WorksheetMergeMutation],
    bindings: list[dict[str, Any]],
    support_records: list[dict[str, Any]],
) -> None:
    layout = _LAYOUT[section_key]
    title_row = int(layout["title_row"])
    header_row = int(layout["header_row"])
    first_data_row = int(layout["first_data_row"])
    period_columns = tuple(layout["period_columns"])
    if len(period_columns) != len(periods):
        raise ValuationCapitalProductCleanupError(
            f"{section_key} period/column cardinality changed."
        )
    _fill_title_row(
        mutations,
        merges,
        row=title_row,
        title=str(layout["title"]),
        major=False,
    )
    headers = {"A": "Metric"}
    headers.update(
        {column: _display_period(period) for column, period in zip(period_columns, periods)}
    )
    for column_number in range(1, 14):
        column = _column_name(column_number)
        _put_text(
            mutations,
            f"{column}{header_row}",
            headers.get(column, ""),
            owner=f"valuation_capital_cleanup.{section_key}.header",
            style_source="A123",
        )
    for offset, row in enumerate(rows):
        target_row = first_data_row + offset
        label = str(row["label"])
        _put_text(
            mutations,
            f"A{target_row}",
            label,
            owner=f"valuation_capital_cleanup.{section_key}.{row['row_key']}.label",
            style_source="A70",
        )
        row_bindings: list[dict[str, Any]] = []
        values_by_column = dict(zip(period_columns, row["values"]))
        for column_number in range(2, 14):
            column = _column_name(column_number)
            value = values_by_column.get(column)
            if value is None:
                _put_value(
                    mutations,
                    f"{column}{target_row}",
                    None,
                    number_format=str(row["number_format"]),
                    owner=f"valuation_capital_cleanup.{section_key}.presentation_padding",
                )
                continue
            owner = str(value["owner"])
            target_cell = f"{column}{target_row}"
            _put_value(
                mutations,
                target_cell,
                value["value"],
                number_format=str(row["number_format"]),
                owner=f"{owner}.presentation_binding",
            )
            binding = {
                "aggregation_role": value["aggregation_role"],
                "definition": value["definition"],
                "display_period": value["display_period"],
                "label": label,
                "metric_id": row["row_key"],
                "number_format": row["number_format"],
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
        support = {
            "bindings": row_bindings,
            "metric_id": row["row_key"],
            "section": section_key,
        }
        support_records.append(support | {"support_digest": _digest(support)})


@dataclass(frozen=True)
class ValuationCapitalProductCleanupPlan:
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
    formula_retirement_plan: tuple[dict[str, Any], ...]
    lineage_support_range: str
    retired_surface_ranges: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "balance_sheet_product_sha256": self.balance_sheet_product_sha256,
            "balance_sheet_shadow_sha256": self.balance_sheet_shadow_sha256,
            "base_workbook_sha256": self.base_workbook_sha256,
            "binding_plan_digest": self.binding_plan_digest,
            "bindings": [dict(row) for row in self.bindings],
            "cell_mutations": [_mutation_dict(row) for row in self.cell_mutations],
            "contract": self.contract,
            "formula_retirement_plan": [dict(row) for row in self.formula_retirement_plan],
            "investor_product": dict(self.investor_product),
            "layout_plan_digest": self.layout_plan_digest,
            "lineage_support_range": self.lineage_support_range,
            "merge_mutations": [_mutation_dict(row) for row in self.merge_mutations],
            "retired_surface_ranges": list(self.retired_surface_ranges),
            "row_mutations": [_mutation_dict(row) for row in self.row_mutations],
            "source_package_sha256": self.source_package_sha256,
            "visible_capital_range": VISIBLE_CAPITAL_RANGE,
        }


def _formula_retirement_plan() -> tuple[dict[str, Any], ...]:
    names = (
        (194, "GAAP_EPS"),
        (195, "Adjusted_EBITDA"),
        (196, "FCF_Per_Share"),
        (197, "Blended_Value_Per_Share"),
        (198, "Upside_Downside"),
    )
    scenarios = (("B", "Current"), ("C", "Bear"), ("D", "Base"), ("E", "Bull"))
    result = []
    for row, metric in names:
        for column, scenario in scenarios:
            result.append(
                {
                    "coordinate": f"Valuation!{column}{row}",
                    "disposition": "RETIRED_NO_CONSUMER",
                    "formula": f"=IC_{scenario}_{metric}",
                    "reason": "The Valuation forward-summary consumer is intentionally retired.",
                }
            )
    result.append(
        {
            "coordinate": "Valuation!AI139",
            "disposition": "RETIRED_NO_CONSUMER",
            "formula": '=IFERROR(MATCH(1,\'Hidden_Value_Flags\'!$L$2:$L$100,0)+1,"")',
            "reason": "The Valuation Hidden Value panel consumer is intentionally retired.",
        }
    )
    return tuple(result)


def build_valuation_capital_product_cleanup_plan(
    *,
    package: Mapping[str, Any],
    source_package_path: Path | str,
    balance_sheet_product: Mapping[str, Any],
    balance_sheet_product_path: Path | str,
    balance_sheet_shadow: Mapping[str, Any],
    balance_sheet_shadow_path: Path | str,
    base_workbook: Path | str,
) -> ValuationCapitalProductCleanupPlan:
    base = Path(base_workbook)
    base_sha = sha256_file(base)
    if base_sha != EXPECTED_EXPANDED_PREVIEW_SHA256:
        raise ValuationCapitalProductCleanupError(
            f"Accepted expanded preview identity changed: {base_sha}."
        )
    investor_product: CapitalAllocationReturnInvestorProduct = (
        build_capital_allocation_return_investor_product(
            package=package,
            balance_sheet_product=balance_sheet_product,
            balance_sheet_shadow=balance_sheet_shadow,
        )
    )
    if investor_product.product_digest != EXPECTED_INVESTOR_PRODUCT_DIGEST:
        raise ValuationCapitalProductCleanupError(
            "Accepted Capital Allocation/Return economics changed during relayout."
        )
    expected_rows = {
        "capital_allocation_summary": 4,
        "annual_capital_allocation_history": 4,
        "capital_return_summary": 8,
        "quarterly_capital_return_history": 6,
        "annual_capital_return_history": 6,
    }
    for key, expected in expected_rows.items():
        if len(getattr(investor_product, key)) != expected:
            raise ValuationCapitalProductCleanupError(
                f"Accepted {key} row universe changed."
            )

    base_cells, base_merges = _base_valuation_state(base)
    mutations: dict[tuple[str, str], FormulaAwareCellMutation] = {}
    merges = [
        WorksheetMergeMutation("Valuation", range_ref, "DELETE")
        for range_ref in base_merges
        if any(_intersects(range_ref, surface) for surface in _RETIREMENT_SURFACES)
    ]
    bindings: list[dict[str, Any]] = []
    support_records: list[dict[str, Any]] = []

    _fill_title_row(
        mutations,
        merges,
        row=int(_LAYOUT["capital_allocation"]["title_row"]),
        title=str(_LAYOUT["capital_allocation"]["title"]),
        major=True,
    )
    _fill_table(
        section_key="capital_allocation_summary",
        rows=investor_product.capital_allocation_summary,
        periods=investor_product.summary_periods,
        mutations=mutations,
        merges=merges,
        bindings=bindings,
        support_records=support_records,
    )
    _fill_table(
        section_key="annual_capital_allocation_history",
        rows=investor_product.annual_capital_allocation_history,
        periods=investor_product.annual_allocation_periods,
        mutations=mutations,
        merges=merges,
        bindings=bindings,
        support_records=support_records,
    )
    _fill_title_row(
        mutations,
        merges,
        row=int(_LAYOUT["capital_return"]["title_row"]),
        title=str(_LAYOUT["capital_return"]["title"]),
        major=True,
    )
    _fill_table(
        section_key="capital_return_summary",
        rows=investor_product.capital_return_summary,
        periods=investor_product.summary_periods,
        mutations=mutations,
        merges=merges,
        bindings=bindings,
        support_records=support_records,
    )
    _fill_table(
        section_key="quarterly_capital_return_history",
        rows=investor_product.quarterly_capital_return_history,
        periods=investor_product.quarterly_return_periods,
        mutations=mutations,
        merges=merges,
        bindings=bindings,
        support_records=support_records,
    )
    _fill_table(
        section_key="annual_capital_return_history",
        rows=investor_product.annual_capital_return_history,
        periods=investor_product.annual_return_periods,
        mutations=mutations,
        merges=merges,
        bindings=bindings,
        support_records=support_records,
    )

    if len(bindings) != 140 or sum(row["status"] == "available" for row in bindings) != 110:
        raise ValuationCapitalProductCleanupError(
            "Accepted 140/110 binding universe changed during relayout."
        )
    if len(support_records) != 28:
        raise ValuationCapitalProductCleanupError("Row-level lineage universe changed from 28.")
    for row, support in zip(range(270, 298), support_records):
        payload = _canonical_bytes(support).decode("utf-8")
        if len(payload) > 32767:
            raise ValuationCapitalProductCleanupError(
                f"Lineage support record at row {row} exceeds the XLSX text limit."
            )
        _put_text(
            mutations,
            f"A{row}",
            payload,
            owner=f"valuation_capital_cleanup.lineage.{support['section']}.{support['metric_id']}",
        )

    final_coordinates = {coordinate for _, coordinate in mutations}
    for coordinate in sorted(base_cells):
        if coordinate in final_coordinates:
            continue
        if any(_coordinate_in_range(coordinate, surface) for surface in _RETIREMENT_SURFACES):
            mutations[("Valuation", coordinate)] = FormulaAwareCellMutation(
                "Valuation",
                coordinate,
                "REMOVE_CELL",
                semantic_owner="valuation_capital_cleanup.retired_surface",
            )

    row_mutations: list[WorksheetRowMutation] = []
    for row in range(79, 123):
        row_mutations.append(WorksheetRowMutation("Valuation", row, hidden=False, height=19.5))
    for row in range(126, 201):
        height = 8.1 if row == 139 else 21.0 if row in {126, 140} else 19.5
        row_mutations.append(WorksheetRowMutation("Valuation", row, hidden=False, height=height))
    row_mutations.extend(
        WorksheetRowMutation("Valuation", row, hidden=True, height=19.5)
        for row in range(270, 298)
    )

    cell_mutations = tuple(
        sorted(mutations.values(), key=lambda item: (item.target_sheet, item.target_cell))
    )
    merge_mutations = tuple(
        sorted(merges, key=lambda item: (item.mode, item.range_ref))
    )
    binding_payload = {
        "bindings": bindings,
        "contract": CLEANUP_CONTRACT,
        "product_digest": investor_product.product_digest,
    }
    formula_plan = _formula_retirement_plan()
    layout_payload = {
        "formula_retirement_plan": formula_plan,
        "hidden_lineage_range": HIDDEN_LINEAGE_RANGE,
        "layout": _LAYOUT,
        "merge_mutations": [_mutation_dict(item) for item in merge_mutations],
        "retired_surface_ranges": _RETIREMENT_SURFACES,
        "row_mutations": [_mutation_dict(item) for item in row_mutations],
        "visible_capital_range": VISIBLE_CAPITAL_RANGE,
    }
    return ValuationCapitalProductCleanupPlan(
        contract=CLEANUP_CONTRACT,
        base_workbook_sha256=base_sha,
        source_package_sha256=sha256_file(Path(source_package_path)),
        balance_sheet_product_sha256=sha256_file(Path(balance_sheet_product_path)),
        balance_sheet_shadow_sha256=sha256_file(Path(balance_sheet_shadow_path)),
        investor_product=investor_product.to_dict(),
        cell_mutations=cell_mutations,
        merge_mutations=merge_mutations,
        row_mutations=tuple(row_mutations),
        bindings=tuple(bindings),
        binding_plan_digest=_digest(binding_payload),
        layout_plan_digest=_digest(layout_payload),
        formula_retirement_plan=formula_plan,
        lineage_support_range=HIDDEN_LINEAGE_RANGE,
        retired_surface_ranges=_RETIREMENT_SURFACES,
    )


def materialize_valuation_capital_product_cleanup(
    *,
    plan: ValuationCapitalProductCleanupPlan,
    base_workbook: Path | str,
    output_workbook: Path | str,
) -> FormulaAwareMaterializationResult:
    if plan.contract != CLEANUP_CONTRACT:
        raise ValuationCapitalProductCleanupError("Cleanup contract changed.")
    return materialize_capital_return_debt_mutations(
        base_workbook=base_workbook,
        output_workbook=output_workbook,
        cell_mutations=plan.cell_mutations,
        merge_mutations=plan.merge_mutations,
        row_mutations=plan.row_mutations,
        expected_base_sha256=plan.base_workbook_sha256,
    )


__all__ = [
    "CLEANUP_CONTRACT",
    "EXPECTED_EXPANDED_PREVIEW_SHA256",
    "EXPECTED_INVESTOR_PRODUCT_DIGEST",
    "HIDDEN_LINEAGE_RANGE",
    "VISIBLE_CAPITAL_RANGE",
    "ValuationCapitalProductCleanupError",
    "ValuationCapitalProductCleanupPlan",
    "build_valuation_capital_product_cleanup_plan",
    "materialize_valuation_capital_product_cleanup",
]
