"""Bounded OOXML extensions for the Capital Return / Debt workbook bridge.

The accepted formula-aware materializer is intentionally left byte-identical
because its implementation identity is part of the Valuation golden.  This
module composes that accepted primitive with the small structural operations
needed by the Capital Return and Debt presentation surfaces.  It owns no
economic selection.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from html import escape
from pathlib import Path
from posixpath import dirname as posix_dirname, join as posix_join, normpath as posix_normpath
import re
import shutil
from tempfile import TemporaryDirectory
from typing import Any, Literal, Mapping, Sequence
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.formula_aware_workbook_materialization import (
    DefinedNameMutation,
    FormulaAwareCellMutation as AcceptedCellMutation,
    FormulaAwareMaterializationError,
    WorkbookCalculationMetadataPolicy,
    WorksheetMergeMutation,
    WorksheetRowMutation as AcceptedRowMutation,
    _ATTRIBUTE_RE,
    _DIMENSION_RE,
    _ROW_RE,
    _SHEET_DATA_RE,
    _patch_worksheet,
    _range_bounds,
    materialize_formula_aware_mutations,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _cell_elements,
    _cell_style_id,
    _set_attribute,
    _sheet_part_map,
    _write_package,
    canonical_ooxml_sha256,
    sha256_file,
)


MATERIALIZER_CONTRACT = "capital-return-debt-bounded-ooxml-extension@1"
_COLS_RE = re.compile(rb"<cols\b[^>]*>(?P<body>.*?)</cols>", re.DOTALL)
_COL_RE = re.compile(rb"<col\b[^>]*/>")
_SHEET_RE = re.compile(rb"<sheet\b[^>]*/>")
_RELATIONSHIP_RE = re.compile(rb"<Relationship\b[^>]*/>")
_TABLE_RE = re.compile(rb"<table\b[^>]*>")
_AUTO_FILTER_RE = re.compile(rb"<autoFilter\b[^>]*/>")
_TABLE_COLUMNS_RE = re.compile(rb"<tableColumns\b[^>]*>.*?</tableColumns>", re.DOTALL)
_TABLE_STYLE_INFO_RE = re.compile(rb"<tableStyleInfo\b[^>]*/>")


@dataclass(frozen=True)
class FormulaAwareCellMutation:
    target_sheet: str
    target_cell: str
    mode: Literal["CLEAR_CONTENTS", "REMOVE_CELL", "SET_VALUE", "SET_FORMULA"]
    value: str | None = None
    value_kind: Literal["number", "text", "boolean"] | None = None
    number_format_code: str | None = None
    style_source_cell: str | None = None
    semantic_owner: str = "presentation"
    style_source_sheet: str | None = None


@dataclass(frozen=True)
class WorksheetRowMutation:
    target_sheet: str
    row: int
    hidden: bool | None = None
    height: float | None = None


@dataclass(frozen=True)
class WorksheetColumnMutation:
    target_sheet: str
    column: int
    width: float
    hidden: bool | None = None


@dataclass(frozen=True)
class WorksheetDimensionMutation:
    target_sheet: str
    range_ref: str
    trim_empty_tail: bool = False


@dataclass(frozen=True)
class WorkbookSheetStateMutation:
    target_sheet: str
    state: Literal["visible", "hidden", "veryHidden"]


@dataclass(frozen=True)
class WorksheetTableMutation:
    target_sheet: str
    range_ref: str
    column_names: tuple[str, ...]
    show_row_stripes: bool = False


@dataclass(frozen=True)
class FormulaAwareMaterializationResult:
    base_workbook_sha256: str
    output_workbook_sha256: str
    canonical_ooxml_sha256: str
    changed_ooxml_parts: tuple[str, ...]
    unchanged_ooxml_part_count: int
    cell_mutation_count: int
    defined_name_upsert_count: int
    defined_name_delete_count: int
    merge_add_count: int
    merge_delete_count: int
    row_mutation_count: int
    column_mutation_count: int
    dimension_mutation_count: int
    sheet_state_mutation_count: int
    table_mutation_count: int
    style_variant_count: int
    write_type_counts: Mapping[str, int]
    calculation_metadata_change_count: int = 0
    calculation_metadata_policy_id: str | None = None
    calculation_metadata_before: Mapping[str, str] | None = None
    calculation_metadata_after: Mapping[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_workbook_sha256": self.base_workbook_sha256,
            "calculation_metadata_after": None if self.calculation_metadata_after is None else dict(self.calculation_metadata_after),
            "calculation_metadata_before": None if self.calculation_metadata_before is None else dict(self.calculation_metadata_before),
            "calculation_metadata_change_count": self.calculation_metadata_change_count,
            "calculation_metadata_policy_id": self.calculation_metadata_policy_id,
            "canonical_ooxml_contract": CANONICAL_OOXML_HASH_CONTRACT,
            "canonical_ooxml_sha256": self.canonical_ooxml_sha256,
            "cell_mutation_count": self.cell_mutation_count,
            "changed_ooxml_parts": list(self.changed_ooxml_parts),
            "column_mutation_count": self.column_mutation_count,
            "defined_name_delete_count": self.defined_name_delete_count,
            "defined_name_upsert_count": self.defined_name_upsert_count,
            "dimension_mutation_count": self.dimension_mutation_count,
            "materializer_contract": MATERIALIZER_CONTRACT,
            "merge_add_count": self.merge_add_count,
            "merge_delete_count": self.merge_delete_count,
            "output_workbook_sha256": self.output_workbook_sha256,
            "row_mutation_count": self.row_mutation_count,
            "sheet_state_mutation_count": self.sheet_state_mutation_count,
            "style_variant_count": self.style_variant_count,
            "table_mutation_count": self.table_mutation_count,
            "unchanged_ooxml_part_count": self.unchanged_ooxml_part_count,
            "write_type_counts": dict(self.write_type_counts),
        }

    def as_dict(self) -> dict[str, Any]:
        """Match the accepted materializer receipt API."""

        return self.to_dict()


def _attributes(element: bytes) -> dict[str, str]:
    start = element[: element.find(b">") + 1]
    return {
        match.group("name").decode("utf-8"): match.group("value").decode("utf-8")
        for match in _ATTRIBUTE_RE.finditer(start)
    }


def _patch_rows(data: bytes, mutations: Sequence[WorksheetRowMutation]) -> bytes:
    output = data
    for item in sorted(mutations, key=lambda value: value.row):
        matches = [match for match in _ROW_RE.finditer(output) if int(match.group("row")) == item.row]
        if len(matches) != 1:
            raise FormulaAwareMaterializationError(
                f"Expected exactly one row {item.target_sheet}!{item.row}, found {len(matches)}."
            )
        match = matches[0]
        raw = match.group(0)
        start_end = raw.find(b">")
        if start_end < 0:
            raise FormulaAwareMaterializationError("Malformed row element.")
        start = raw[: start_end + 1]
        remainder = raw[start_end + 1 :]
        if item.hidden is not None:
            start = _set_attribute(start, "hidden", "1" if item.hidden else None)
        if item.height is not None:
            start = _set_attribute(start, "ht", format(item.height, ".15g"))
            start = _set_attribute(start, "customHeight", "1")
        output = output[: match.start()] + start + remainder + output[match.end() :]
    return output


def _patch_columns(data: bytes, mutations: Sequence[WorksheetColumnMutation]) -> bytes:
    if not mutations:
        return data
    section = _COLS_RE.search(data)
    if section is None:
        sheet_data = _SHEET_DATA_RE.search(data)
        if sheet_data is None:
            raise FormulaAwareMaterializationError("Worksheet lacks sheetData.")
        raw_by_column: dict[int, bytes] = {}
    else:
        raw_by_column = {}
        for match in _COL_RE.finditer(section.group("body")):
            attrs = _attributes(match.group(0))
            minimum = int(attrs.get("min", "0"))
            maximum = int(attrs.get("max", "0"))
            if minimum < 1 or minimum != maximum or minimum in raw_by_column:
                raise FormulaAwareMaterializationError(
                    "Bounded column mutations require unique single-column definitions."
                )
            raw_by_column[minimum] = match.group(0)
    for item in sorted(mutations, key=lambda value: value.column):
        if item.column < 1 or item.width <= 0:
            raise FormulaAwareMaterializationError("Worksheet columns and widths must be positive.")
        raw = raw_by_column.get(item.column, f'<col min="{item.column}" max="{item.column}"/>'.encode("ascii"))
        raw = _set_attribute(raw, "width", format(item.width, ".15g"))
        raw = _set_attribute(raw, "customWidth", "1")
        if item.hidden is not None:
            raw = _set_attribute(raw, "hidden", "1" if item.hidden else None)
        raw_by_column[item.column] = raw
    replacement = b"<cols>" + b"".join(raw_by_column[index] for index in sorted(raw_by_column)) + b"</cols>"
    if section is not None:
        return data[: section.start()] + replacement + data[section.end() :]
    sheet_data = _SHEET_DATA_RE.search(data)
    assert sheet_data is not None
    return data[: sheet_data.start()] + replacement + data[sheet_data.start() :]


def _patch_dimension(data: bytes, mutation: WorksheetDimensionMutation | None) -> bytes:
    if mutation is None:
        return data
    _, _, maximum_column, maximum_row = _range_bounds(mutation.range_ref)
    if mutation.trim_empty_tail:
        for row in reversed(list(_ROW_RE.finditer(data))):
            if int(row.group("row")) <= maximum_row:
                continue
            if b"<c" in row.group(0):
                raise FormulaAwareMaterializationError(
                    f"Refusing to trim non-empty row {mutation.target_sheet}!{int(row.group('row'))}."
                )
            data = data[: row.start()] + data[row.end() :]
        columns = _COLS_RE.search(data)
        if columns is not None:
            retained: list[bytes] = []
            for column in _COL_RE.finditer(columns.group("body")):
                raw = column.group(0)
                attrs = _attributes(raw)
                minimum = int(attrs.get("min", "0"))
                maximum = int(attrs.get("max", "0"))
                if minimum < 1 or maximum < minimum:
                    raise FormulaAwareMaterializationError("Malformed worksheet column definition.")
                if minimum <= maximum_column:
                    if maximum > maximum_column:
                        raw = _set_attribute(raw, "max", str(maximum_column))
                    retained.append(raw)
            replacement = b"<cols>" + b"".join(retained) + b"</cols>"
            data = data[: columns.start()] + replacement + data[columns.end() :]
    match = _DIMENSION_RE.search(data)
    if match is None:
        raise FormulaAwareMaterializationError("Worksheet lacks a dimension element.")
    updated = _set_attribute(match.group(0), "ref", mutation.range_ref)
    return data[: match.start()] + updated + data[match.end() :]


def _patch_sheet_states(data: bytes, mutations: Sequence[WorkbookSheetStateMutation]) -> bytes:
    targets = {item.target_sheet: item.state for item in mutations}
    if len(targets) != len(mutations):
        raise FormulaAwareMaterializationError("Sheet-state mutations contain duplicate targets.")
    found: set[str] = set()
    pieces: list[bytes] = []
    cursor = 0
    visible_count = 0
    for match in _SHEET_RE.finditer(data):
        raw = match.group(0)
        name = _attributes(raw).get("name", "")
        if name in targets:
            state = targets[name]
            raw = _set_attribute(raw, "state", None if state == "visible" else state)
            found.add(name)
        if _attributes(raw).get("state", "visible") == "visible":
            visible_count += 1
        pieces.extend((data[cursor : match.start()], raw))
        cursor = match.end()
    pieces.append(data[cursor:])
    if found != set(targets):
        raise FormulaAwareMaterializationError(f"Missing sheet-state targets: {sorted(set(targets) - found)!r}.")
    if visible_count == 0:
        raise FormulaAwareMaterializationError("Workbook must retain at least one visible sheet.")
    return b"".join(pieces)


def _table_part_for_sheet(members: Mapping[str, bytes], sheet_part: str) -> str:
    path = Path(sheet_part)
    relationship_part = (path.parent / "_rels" / f"{path.name}.rels").as_posix()
    data = members.get(relationship_part)
    if data is None:
        raise FormulaAwareMaterializationError(f"Worksheet {sheet_part!r} lacks relationships.")
    targets: list[str] = []
    for match in _RELATIONSHIP_RE.finditer(data):
        attrs = _attributes(match.group(0))
        if attrs.get("Type", "").endswith("/table"):
            target = attrs.get("Target", "")
            resolved = target.lstrip("/") if target.startswith("/") else posix_normpath(posix_join(posix_dirname(sheet_part), target))
            targets.append(resolved)
    if len(targets) != 1 or targets[0] not in members:
        raise FormulaAwareMaterializationError(
            f"Expected exactly one existing table for worksheet {sheet_part!r}."
        )
    return targets[0]


def _patch_table(data: bytes, mutation: WorksheetTableMutation) -> bytes:
    minimum_column, _, maximum_column, _ = _range_bounds(mutation.range_ref)
    if len(mutation.column_names) != maximum_column - minimum_column + 1:
        raise FormulaAwareMaterializationError("Table column count does not match its range.")
    if any(not name.strip() for name in mutation.column_names) or len(set(mutation.column_names)) != len(mutation.column_names):
        raise FormulaAwareMaterializationError("Table column names must be non-empty and unique.")
    root = _TABLE_RE.search(data)
    auto_filter = _AUTO_FILTER_RE.search(data)
    columns = _TABLE_COLUMNS_RE.search(data)
    style = _TABLE_STYLE_INFO_RE.search(data)
    if None in (root, auto_filter, columns, style):
        raise FormulaAwareMaterializationError("Existing table part has an unsupported shape.")
    assert root is not None and auto_filter is not None and columns is not None and style is not None
    data = data[: root.start()] + _set_attribute(root.group(0), "ref", mutation.range_ref) + data[root.end() :]
    auto_filter = _AUTO_FILTER_RE.search(data)
    assert auto_filter is not None
    data = data[: auto_filter.start()] + _set_attribute(auto_filter.group(0), "ref", mutation.range_ref) + data[auto_filter.end() :]
    columns = _TABLE_COLUMNS_RE.search(data)
    assert columns is not None
    body = b"".join(
        f'<tableColumn id="{index}" name="{escape(name, quote=True)}"/>'.encode("utf-8")
        for index, name in enumerate(mutation.column_names, start=1)
    )
    replacement = f'<tableColumns count="{len(mutation.column_names)}">'.encode("ascii") + body + b"</tableColumns>"
    data = data[: columns.start()] + replacement + data[columns.end() :]
    style = _TABLE_STYLE_INFO_RE.search(data)
    assert style is not None
    updated_style = _set_attribute(style.group(0), "showRowStripes", "1" if mutation.show_row_stripes else "0")
    return data[: style.start()] + updated_style + data[style.end() :]


def _remove_cells(data: bytes, coordinates: Sequence[str], *, sheet: str) -> bytes:
    output = data
    for coordinate in sorted(coordinates):
        located = _cell_elements(output).get(coordinate)
        if located is None:
            raise FormulaAwareMaterializationError(f"REMOVE_CELL target is absent: {sheet}!{coordinate}.")
        output = output[: located[0]] + output[located[1] :]
    return output


def _validate_unique(items: Sequence[Any], key: Any, label: str) -> None:
    values = [key(item) for item in items]
    if len(values) != len(set(values)):
        raise FormulaAwareMaterializationError(f"{label} contain duplicate targets.")


def materialize_capital_return_debt_mutations(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    cell_mutations: Sequence[FormulaAwareCellMutation],
    defined_name_mutations: Sequence[DefinedNameMutation] = (),
    merge_mutations: Sequence[WorksheetMergeMutation] = (),
    row_mutations: Sequence[WorksheetRowMutation] = (),
    column_mutations: Sequence[WorksheetColumnMutation] = (),
    dimension_mutations: Sequence[WorksheetDimensionMutation] = (),
    sheet_state_mutations: Sequence[WorkbookSheetStateMutation] = (),
    table_mutations: Sequence[WorksheetTableMutation] = (),
    calculation_metadata_policy: WorkbookCalculationMetadataPolicy | None = None,
    expected_base_sha256: str | None = None,
) -> FormulaAwareMaterializationResult:
    """Compose the accepted materializer with bounded target-surface structure edits."""

    base = Path(base_workbook)
    output = Path(output_workbook)
    if base.resolve() == output.resolve():
        raise FormulaAwareMaterializationError("Protected/base workbook cannot be an output target.")
    if output.exists():
        raise FormulaAwareMaterializationError(f"Refusing to overwrite existing output: {output}.")
    base_sha = sha256_file(base)
    if expected_base_sha256 is not None and base_sha != expected_base_sha256.lower():
        raise FormulaAwareMaterializationError(f"Base workbook hash changed: {base_sha}.")
    _validate_unique(cell_mutations, lambda item: (item.target_sheet, item.target_cell), "Cell mutations")
    _validate_unique(row_mutations, lambda item: (item.target_sheet, item.row), "Row mutations")
    _validate_unique(column_mutations, lambda item: (item.target_sheet, item.column), "Column mutations")
    _validate_unique(dimension_mutations, lambda item: item.target_sheet, "Dimension mutations")
    _validate_unique(table_mutations, lambda item: item.target_sheet, "Table mutations")
    for item in cell_mutations:
        if item.mode in {"CLEAR_CONTENTS", "REMOVE_CELL"} and (item.value is not None or item.value_kind is not None):
            raise FormulaAwareMaterializationError(f"{item.mode} cannot carry a value.")
        if item.mode == "REMOVE_CELL" and (item.number_format_code or item.style_source_cell or item.style_source_sheet):
            raise FormulaAwareMaterializationError("REMOVE_CELL cannot carry presentation styling.")
    output.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix="capital_return_debt_", dir=output.parent) as temporary:
        temporary_root = Path(temporary)
        styled_base = temporary_root / "styled_base.xlsx"
        intermediate = temporary_root / "accepted_materializer_output.xlsx"
        with ZipFile(base, "r") as archive:
            members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
            sheet_parts = _sheet_part_map(archive)
        cross_by_sheet: dict[str, list[tuple[FormulaAwareCellMutation, int]]] = {}
        for item in cell_mutations:
            if item.style_source_sheet is None:
                continue
            if item.style_source_sheet not in sheet_parts or item.target_sheet not in sheet_parts:
                raise FormulaAwareMaterializationError("Cross-sheet style source or target is missing.")
            source_coordinate = item.style_source_cell or item.target_cell
            located = _cell_elements(members[sheet_parts[item.style_source_sheet]]).get(source_coordinate)
            if located is None:
                raise FormulaAwareMaterializationError(
                    f"Style source is absent: {item.style_source_sheet}!{source_coordinate}."
                )
            cross_by_sheet.setdefault(item.target_sheet, []).append((item, _cell_style_id(located[2])))
        for sheet, items in cross_by_sheet.items():
            prelim: list[AcceptedCellMutation] = []
            style_ids: dict[tuple[str, str], int] = {}
            for item, style_id in items:
                if item.mode == "REMOVE_CELL":
                    continue
                prelim.append(
                    AcceptedCellMutation(
                        item.target_sheet,
                        item.target_cell,
                        item.mode,
                        item.value,
                        item.value_kind,
                        None,
                        None,
                        item.semantic_owner,
                    )
                )
                style_ids[(item.target_sheet, item.target_cell)] = style_id
            members[sheet_parts[sheet]] = _patch_worksheet(members[sheet_parts[sheet]], prelim, style_ids)
        if cross_by_sheet:
            _write_package(base_workbook=base, output_workbook=styled_base, members=members)
        else:
            shutil.copyfile(base, styled_base)

        accepted_cells: list[AcceptedCellMutation] = []
        for item in cell_mutations:
            mode = "CLEAR_CONTENTS" if item.mode == "REMOVE_CELL" else item.mode
            accepted_cells.append(
                AcceptedCellMutation(
                    item.target_sheet,
                    item.target_cell,
                    mode,
                    item.value,
                    item.value_kind,
                    item.number_format_code,
                    item.target_cell if item.style_source_sheet is not None else item.style_source_cell,
                    item.semantic_owner,
                )
            )
        accepted_result = materialize_formula_aware_mutations(
            base_workbook=styled_base,
            output_workbook=intermediate,
            cell_mutations=accepted_cells,
            defined_name_mutations=defined_name_mutations,
            merge_mutations=merge_mutations,
            row_mutations=(),
            calculation_metadata_policy=calculation_metadata_policy,
            expected_base_sha256=sha256_file(styled_base),
        )

        with ZipFile(intermediate, "r") as archive:
            final_members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
            final_sheet_parts = _sheet_part_map(archive)
        removes: dict[str, list[str]] = {}
        rows: dict[str, list[WorksheetRowMutation]] = {}
        columns: dict[str, list[WorksheetColumnMutation]] = {}
        dimensions = {item.target_sheet: item for item in dimension_mutations}
        for item in cell_mutations:
            if item.mode == "REMOVE_CELL":
                removes.setdefault(item.target_sheet, []).append(item.target_cell)
        for item in row_mutations:
            rows.setdefault(item.target_sheet, []).append(item)
        for item in column_mutations:
            columns.setdefault(item.target_sheet, []).append(item)
        for sheet in sorted(set(removes) | set(rows) | set(columns) | set(dimensions)):
            if sheet not in final_sheet_parts:
                raise FormulaAwareMaterializationError(f"Missing structural target sheet {sheet!r}.")
            part = final_sheet_parts[sheet]
            updated = _remove_cells(final_members[part], removes.get(sheet, ()), sheet=sheet)
            updated = _patch_rows(updated, rows.get(sheet, ()))
            updated = _patch_columns(updated, columns.get(sheet, ()))
            updated = _patch_dimension(updated, dimensions.get(sheet))
            final_members[part] = updated
        if sheet_state_mutations:
            final_members["xl/workbook.xml"] = _patch_sheet_states(
                final_members["xl/workbook.xml"], sheet_state_mutations
            )
        for item in sorted(table_mutations, key=lambda value: value.target_sheet):
            table_part = _table_part_for_sheet(final_members, final_sheet_parts[item.target_sheet])
            final_members[table_part] = _patch_table(final_members[table_part], item)
        _write_package(base_workbook=intermediate, output_workbook=output, members=final_members)

    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if before.namelist() != after.namelist():
            raise FormulaAwareMaterializationError("OOXML member inventory changed.")
        changed = tuple(sorted(name for name in before.namelist() if before.read(name) != after.read(name)))
        member_count = len(before.namelist())
    write_types = Counter(
        "clear" if item.mode == "CLEAR_CONTENTS" else "remove" if item.mode == "REMOVE_CELL" else "formula" if item.mode == "SET_FORMULA" else str(item.value_kind)
        for item in cell_mutations
    )
    return FormulaAwareMaterializationResult(
        base_workbook_sha256=base_sha,
        output_workbook_sha256=sha256_file(output),
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        changed_ooxml_parts=changed,
        unchanged_ooxml_part_count=member_count - len(changed),
        cell_mutation_count=len(cell_mutations),
        defined_name_upsert_count=accepted_result.defined_name_upsert_count,
        defined_name_delete_count=accepted_result.defined_name_delete_count,
        merge_add_count=accepted_result.merge_add_count,
        merge_delete_count=accepted_result.merge_delete_count,
        row_mutation_count=len(row_mutations),
        column_mutation_count=len(column_mutations),
        dimension_mutation_count=len(dimension_mutations),
        sheet_state_mutation_count=len(sheet_state_mutations),
        table_mutation_count=len(table_mutations),
        style_variant_count=accepted_result.style_variant_count,
        write_type_counts=dict(sorted(write_types.items())),
        calculation_metadata_change_count=accepted_result.calculation_metadata_change_count,
        calculation_metadata_policy_id=accepted_result.calculation_metadata_policy_id,
        calculation_metadata_before=accepted_result.calculation_metadata_before,
        calculation_metadata_after=accepted_result.calculation_metadata_after,
    )


__all__ = [
    "FormulaAwareCellMutation",
    "FormulaAwareMaterializationResult",
    "MATERIALIZER_CONTRACT",
    "WorkbookSheetStateMutation",
    "WorksheetColumnMutation",
    "WorksheetDimensionMutation",
    "WorksheetRowMutation",
    "WorksheetTableMutation",
    "materialize_capital_return_debt_mutations",
]
