"""Bounded, lossless OOXML mutations for formulas, values, and defined names.

This is a package-level primitive.  It deliberately owns no economic selection,
formula design, or workbook layout.  Callers supply an immutable exact-cell plan.
Untouched ZIP members and untouched cell elements remain byte-identical.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from html import escape
from pathlib import Path
import re
import shutil
from typing import Any, Literal, Mapping, Sequence
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _cell_elements,
    _cell_style_id,
    _column_number,
    _resolve_style_variants,
    _set_attribute,
    _sheet_part_map,
    _write_package,
    canonical_ooxml_sha256,
    sha256_file,
)


MATERIALIZER_CONTRACT = "formula-aware-lossless-workbook-materializer@2"
FORMULA_CACHE_POLICY = "target-formula-cache-absent-pending-native-recalc@1"
CALCULATION_METADATA_CONTRACT = "workbook-calculation-metadata-finalization@1"

_CELL_RE = re.compile(r"([A-Z]+)([1-9][0-9]*)\Z")
_ROW_RE = re.compile(
    rb"<row\b[^>]*\br=([\"'])(?P<row>[1-9][0-9]*)\1[^>]*(?:/>|>.*?</row>)",
    re.DOTALL,
)
_SHEET_DATA_RE = re.compile(rb"<sheetData\b[^>]*>(?P<body>.*?)</sheetData>", re.DOTALL)
_DIMENSION_RE = re.compile(rb"<dimension\b[^>]*/>")
_MERGE_CELLS_RE = re.compile(rb"<mergeCells\b[^>]*>(?P<body>.*?)</mergeCells>", re.DOTALL)
_MERGE_CELL_RE = re.compile(rb"<mergeCell\b[^>]*/>")
_DEFINED_NAMES_RE = re.compile(rb"<definedNames>(?P<body>.*?)</definedNames>", re.DOTALL)
_DEFINED_NAME_RE = re.compile(rb"<definedName\b[^>]*>.*?</definedName>", re.DOTALL)
_CALCULATION_PROPERTIES_RE = re.compile(
    rb"<calcPr\b[^>]*(?:/>|>.*?</calcPr>)",
    re.DOTALL,
)
_ATTRIBUTE_RE = re.compile(rb"\s(?P<name>[A-Za-z_:][A-Za-z0-9_.:-]*)=(?P<quote>[\"'])(?P<value>.*?)\2")


class FormulaAwareMaterializationError(ValueError):
    """Fail-closed validation error for a bounded OOXML mutation."""


@dataclass(frozen=True)
class FormulaAwareCellMutation:
    target_sheet: str
    target_cell: str
    mode: Literal["CLEAR_CONTENTS", "SET_VALUE", "SET_FORMULA"]
    value: str | None = None
    value_kind: Literal["number", "text", "boolean"] | None = None
    number_format_code: str | None = None
    style_source_cell: str | None = None
    semantic_owner: str = "presentation"


@dataclass(frozen=True)
class WorkbookCalculationMetadataPolicy:
    """Caller-owned policy for one bounded workbook calculation-metadata mutation."""

    policy_id: str
    expected_calc_mode: Literal["auto"] = "auto"
    expected_full_calc_on_load: bool = True
    expected_force_full_calc: bool = True
    force_full_calc: bool = False


@dataclass(frozen=True)
class DefinedNameMutation:
    name: str
    mode: Literal["UPSERT", "DELETE"]
    attr_text: str | None = None


@dataclass(frozen=True)
class WorksheetMergeMutation:
    target_sheet: str
    range_ref: str
    mode: Literal["ADD", "DELETE"]


@dataclass(frozen=True)
class WorksheetRowMutation:
    target_sheet: str
    row: int
    hidden: bool


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
    style_variant_count: int
    write_type_counts: Mapping[str, int]
    calculation_metadata_change_count: int = 0
    calculation_metadata_policy_id: str | None = None
    calculation_metadata_before: Mapping[str, str] | None = None
    calculation_metadata_after: Mapping[str, str] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "base_workbook_sha256": self.base_workbook_sha256,
            "canonical_ooxml_hash_contract": CANONICAL_OOXML_HASH_CONTRACT,
            "canonical_ooxml_sha256": self.canonical_ooxml_sha256,
            "calculation_metadata_after": None
            if self.calculation_metadata_after is None
            else dict(self.calculation_metadata_after),
            "calculation_metadata_before": None
            if self.calculation_metadata_before is None
            else dict(self.calculation_metadata_before),
            "calculation_metadata_change_count": self.calculation_metadata_change_count,
            "calculation_metadata_contract": CALCULATION_METADATA_CONTRACT,
            "calculation_metadata_policy_id": self.calculation_metadata_policy_id,
            "cell_mutation_count": self.cell_mutation_count,
            "changed_ooxml_parts": list(self.changed_ooxml_parts),
            "defined_name_delete_count": self.defined_name_delete_count,
            "defined_name_upsert_count": self.defined_name_upsert_count,
            "formula_cache_policy": FORMULA_CACHE_POLICY,
            "materializer_contract": MATERIALIZER_CONTRACT,
            "merge_add_count": self.merge_add_count,
            "merge_delete_count": self.merge_delete_count,
            "output_workbook_sha256": self.output_workbook_sha256,
            "row_mutation_count": self.row_mutation_count,
            "style_variant_count": self.style_variant_count,
            "unchanged_ooxml_part_count": self.unchanged_ooxml_part_count,
            "write_type_counts": dict(self.write_type_counts),
        }


def _mutation_sort_key(item: FormulaAwareCellMutation) -> tuple[str, int, int]:
    match = _CELL_RE.fullmatch(item.target_cell)
    if match is None:
        raise FormulaAwareMaterializationError(f"Invalid target cell {item.target_cell!r}.")
    return item.target_sheet, int(match.group(2)), _column_number(match.group(1))


def _validate_mutations(mutations: Sequence[FormulaAwareCellMutation]) -> tuple[FormulaAwareCellMutation, ...]:
    ordered = tuple(sorted(mutations, key=_mutation_sort_key))
    targets = [(item.target_sheet, item.target_cell) for item in ordered]
    if len(targets) != len(set(targets)):
        duplicates = sorted({target for target in targets if targets.count(target) > 1})
        raise FormulaAwareMaterializationError(f"Duplicate cell mutation targets: {duplicates!r}.")
    for item in ordered:
        if item.mode == "CLEAR_CONTENTS":
            if item.value is not None or item.value_kind is not None:
                raise FormulaAwareMaterializationError("CLEAR_CONTENTS cannot carry a value.")
        elif item.mode == "SET_FORMULA":
            if not item.value or item.value_kind is not None:
                raise FormulaAwareMaterializationError("SET_FORMULA requires formula text only.")
            if item.value.startswith("="):
                raise FormulaAwareMaterializationError("Formula text must omit the leading equals sign.")
            if "[" in item.value or "]" in item.value:
                raise FormulaAwareMaterializationError(
                    f"External-reference syntax is forbidden in {item.target_sheet}!{item.target_cell}."
                )
        elif item.mode == "SET_VALUE":
            if item.value is None or item.value_kind is None:
                raise FormulaAwareMaterializationError("SET_VALUE requires typed content.")
            if item.value_kind == "number":
                try:
                    parsed = Decimal(item.value)
                except InvalidOperation as exc:
                    raise FormulaAwareMaterializationError(f"Invalid numeric value {item.value!r}.") from exc
                if not parsed.is_finite():
                    raise FormulaAwareMaterializationError("Numeric writes must be finite.")
            elif item.value_kind == "boolean" and item.value not in {"0", "1"}:
                raise FormulaAwareMaterializationError("Boolean values must be 0 or 1.")
        else:  # pragma: no cover - Literal typing plus construction sites close this branch
            raise FormulaAwareMaterializationError(f"Unsupported mutation mode {item.mode!r}.")
    return ordered


def _cell_element(
    mutation: FormulaAwareCellMutation,
    *,
    style_id: int,
    existing: bytes | None,
) -> bytes:
    coordinate = mutation.target_cell
    if existing is None:
        start_tag = f'<c r="{coordinate}">'.encode("ascii")
    else:
        start_end = existing.find(b">")
        if start_end < 0:
            raise FormulaAwareMaterializationError("Malformed cell element.")
        start_tag = existing[: start_end + 1]
        if start_tag.endswith(b"/>"):
            start_tag = start_tag[:-2] + b">"
    start_tag = _set_attribute(start_tag, "r", coordinate)
    start_tag = _set_attribute(start_tag, "s", None if style_id == 0 else str(style_id))

    if mutation.mode == "CLEAR_CONTENTS":
        start_tag = _set_attribute(start_tag, "t", None)
        return start_tag[:-1] + b"/>"
    if mutation.mode == "SET_FORMULA":
        start_tag = _set_attribute(start_tag, "t", None)
        return start_tag + f"<f>{escape(str(mutation.value))}</f></c>".encode("utf-8")

    assert mutation.value is not None and mutation.value_kind is not None
    if mutation.value_kind == "number":
        start_tag = _set_attribute(start_tag, "t", "n")
        body = f"<v>{mutation.value}</v>".encode("ascii")
    elif mutation.value_kind == "boolean":
        start_tag = _set_attribute(start_tag, "t", "b")
        body = f"<v>{mutation.value}</v>".encode("ascii")
    else:
        start_tag = _set_attribute(start_tag, "t", "inlineStr")
        preserve = mutation.value != mutation.value.strip() or "  " in mutation.value or "\n" in mutation.value
        spacing = ' xml:space="preserve"' if preserve else ""
        body = f"<is><t{spacing}>{escape(mutation.value)}</t></is>".encode("utf-8")
    return start_tag + body + b"</c>"


def _column_from_coordinate(coordinate: str) -> int:
    match = _CELL_RE.fullmatch(coordinate)
    if match is None:  # pragma: no cover - validated before this helper
        raise FormulaAwareMaterializationError(f"Invalid coordinate {coordinate!r}.")
    return _column_number(match.group(1))


def _insert_cell_into_row(row: bytes, coordinate: str, cell: bytes) -> bytes:
    if row.endswith(b"/>"):
        return row[:-2] + b">" + cell + b"</row>"
    closing = row.rfind(b"</row>")
    if closing < 0:
        raise FormulaAwareMaterializationError("Malformed row element.")
    target_column = _column_from_coordinate(coordinate)
    body_start = row.find(b">") + 1
    for _coordinate, (start, _end, element) in _cell_elements(row).items():
        element_coordinate_match = re.search(rb'\br="([A-Z]+)[1-9][0-9]*"', element)
        if element_coordinate_match is None:
            raise FormulaAwareMaterializationError("Malformed cell coordinate inside row.")
        if _column_number(element_coordinate_match.group(1).decode("ascii")) > target_column:
            return row[:start] + cell + row[start:]
    return row[:closing] + cell + row[closing:]


def _patch_dimension(data: bytes, coordinates: Sequence[str]) -> bytes:
    match = _DIMENSION_RE.search(data)
    if match is None or not coordinates:
        return data
    raw = match.group(0)
    ref_match = re.search(rb'\bref="([^"]+)"', raw)
    if ref_match is None:
        return data
    ref = ref_match.group(1).decode("ascii")
    parts = ref.split(":", 1)
    existing = parts if len(parts) == 2 else [parts[0], parts[0]]
    all_coordinates = [item.replace("$", "") for item in (*existing, *coordinates)]
    parsed = []
    for item in all_coordinates:
        cell_match = _CELL_RE.fullmatch(item)
        if cell_match is None:
            return data
        parsed.append((_column_number(cell_match.group(1)), int(cell_match.group(2))))
    minimum_column = min(item[0] for item in parsed)
    minimum_row = min(item[1] for item in parsed)
    maximum_column = max(item[0] for item in parsed)
    maximum_row = max(item[1] for item in parsed)

    def column_name(number: int) -> str:
        result = ""
        while number:
            number, remainder = divmod(number - 1, 26)
            result = chr(65 + remainder) + result
        return result

    updated_ref = f"{column_name(minimum_column)}{minimum_row}:{column_name(maximum_column)}{maximum_row}"
    updated = _set_attribute(raw, "ref", updated_ref)
    return data[: match.start()] + updated + data[match.end() :]


def _patch_worksheet(
    data: bytes,
    mutations: Sequence[FormulaAwareCellMutation],
    style_ids: Mapping[tuple[str, str], int],
) -> bytes:
    output = data
    for mutation in sorted(mutations, key=_mutation_sort_key):
        cells = _cell_elements(output)
        located = cells.get(mutation.target_cell)
        replacement = _cell_element(
            mutation,
            style_id=style_ids[(mutation.target_sheet, mutation.target_cell)],
            existing=None if located is None else located[2],
        )
        if located is not None:
            output = output[: located[0]] + replacement + output[located[1] :]
            continue

        row_number = int(_CELL_RE.fullmatch(mutation.target_cell).group(2))  # type: ignore[union-attr]
        row_matches = list(_ROW_RE.finditer(output))
        row_match = next((item for item in row_matches if int(item.group("row")) == row_number), None)
        if row_match is not None:
            row = row_match.group(0)
            updated_row = _insert_cell_into_row(row, mutation.target_cell, replacement)
            output = output[: row_match.start()] + updated_row + output[row_match.end() :]
            continue

        sheet_data = _SHEET_DATA_RE.search(output)
        if sheet_data is None:
            raise FormulaAwareMaterializationError("Worksheet lacks sheetData.")
        insertion = sheet_data.end("body")
        next_row = next((item for item in row_matches if int(item.group("row")) > row_number), None)
        if next_row is not None:
            insertion = next_row.start()
        row = f'<row r="{row_number}">'.encode("ascii") + replacement + b"</row>"
        output = output[:insertion] + row + output[insertion:]
    return _patch_dimension(output, [item.target_cell for item in mutations])


def _range_bounds(range_ref: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*):([A-Z]+)([1-9][0-9]*)", range_ref)
    if match is None:
        raise FormulaAwareMaterializationError(f"Invalid merge range {range_ref!r}.")
    minimum_column = _column_number(match.group(1))
    minimum_row = int(match.group(2))
    maximum_column = _column_number(match.group(3))
    maximum_row = int(match.group(4))
    if minimum_column > maximum_column or minimum_row > maximum_row:
        raise FormulaAwareMaterializationError(f"Reversed merge range {range_ref!r}.")
    return minimum_column, minimum_row, maximum_column, maximum_row


def _patch_merges(data: bytes, mutations: Sequence[WorksheetMergeMutation]) -> bytes:
    if not mutations:
        return data
    section = _MERGE_CELLS_RE.search(data)
    existing: list[str] = []
    if section is not None:
        for item in _MERGE_CELL_RE.finditer(section.group("body")):
            ref = _attributes(item.group(0)).get("ref")
            if ref is None:
                raise FormulaAwareMaterializationError("Merge cell lacks ref.")
            existing.append(ref)
    result = list(existing)
    for item in sorted(mutations, key=lambda value: (0 if value.mode == "DELETE" else 1, value.range_ref)):
        _range_bounds(item.range_ref)
        if item.mode == "DELETE":
            if item.range_ref not in result:
                raise FormulaAwareMaterializationError(
                    f"Expected merge is absent: {item.target_sheet}!{item.range_ref}."
                )
            result.remove(item.range_ref)
        else:
            if item.range_ref in result:
                raise FormulaAwareMaterializationError(
                    f"Merge already exists: {item.target_sheet}!{item.range_ref}."
                )
            new_bounds = _range_bounds(item.range_ref)
            for candidate in result:
                bounds = _range_bounds(candidate)
                overlaps = not (
                    new_bounds[2] < bounds[0]
                    or bounds[2] < new_bounds[0]
                    or new_bounds[3] < bounds[1]
                    or bounds[3] < new_bounds[1]
                )
                if overlaps:
                    raise FormulaAwareMaterializationError(
                        f"Merge {item.range_ref!r} overlaps retained merge {candidate!r}."
                    )
            result.append(item.range_ref)
    body = b"".join(f'<mergeCell ref="{value}"/>'.encode("ascii") for value in result)
    opening = f'<mergeCells count="{len(result)}">'.encode("ascii")
    replacement = opening + body + b"</mergeCells>"
    if section is not None:
        return data[: section.start()] + replacement + data[section.end() :]
    if not result:
        return data
    insertion = data.find(b"</worksheet>")
    if insertion < 0:
        raise FormulaAwareMaterializationError("Worksheet lacks closing element.")
    return data[:insertion] + replacement + data[insertion:]


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
        start = _set_attribute(start, "hidden", "1" if item.hidden else None)
        updated = start + remainder
        output = output[: match.start()] + updated + output[match.end() :]
    return output


def _attributes(element: bytes) -> dict[str, str]:
    start = element[: element.find(b">") + 1]
    return {
        match.group("name").decode("utf-8"): match.group("value").decode("utf-8")
        for match in _ATTRIBUTE_RE.finditer(start)
    }


def _patch_defined_names(data: bytes, mutations: Sequence[DefinedNameMutation]) -> bytes:
    ordered = tuple(sorted(mutations, key=lambda item: item.name.casefold()))
    names = [item.name for item in ordered]
    if len(names) != len(set(names)):
        raise FormulaAwareMaterializationError("Defined-name mutations contain duplicate names.")
    section = _DEFINED_NAMES_RE.search(data)
    body = b"" if section is None else section.group("body")
    existing = list(_DEFINED_NAME_RE.finditer(body))
    removed: set[str] = set()
    pieces: list[bytes] = []
    cursor = 0
    mutation_names = set(names)
    for match in existing:
        pieces.append(body[cursor : match.start()])
        attrs = _attributes(match.group(0))
        name = attrs.get("name")
        if name in mutation_names:
            if "localSheetId" in attrs:
                raise FormulaAwareMaterializationError(
                    f"Refusing to shadow sheet-local defined name {name!r}."
                )
            removed.add(str(name))
        else:
            pieces.append(match.group(0))
        cursor = match.end()
    pieces.append(body[cursor:])
    retained = b"".join(pieces)
    additions: list[bytes] = []
    for item in ordered:
        if item.mode == "DELETE":
            continue
        if item.attr_text is None:
            raise FormulaAwareMaterializationError(f"Defined name {item.name!r} lacks attr_text.")
        if not re.fullmatch(r"[A-Za-z_\\][A-Za-z0-9_.\\]*", item.name):
            raise FormulaAwareMaterializationError(f"Invalid defined name {item.name!r}.")
        additions.append(
            f'<definedName name="{escape(item.name, quote=True)}">{escape(item.attr_text)}</definedName>'.encode(
                "utf-8"
            )
        )
    updated_body = retained + b"".join(additions)
    if section is not None:
        replacement = b"<definedNames>" + updated_body + b"</definedNames>"
        return data[: section.start()] + replacement + data[section.end() :]
    if not updated_body:
        return data
    insertion = data.rfind(b"</workbook>")
    if insertion < 0:
        raise FormulaAwareMaterializationError("Workbook XML lacks a closing workbook element.")
    return data[:insertion] + b"<definedNames>" + updated_body + b"</definedNames>" + data[insertion:]


def _boolean_attribute(value: bool) -> str:
    return "1" if value else "0"


def _patch_calculation_metadata(
    data: bytes,
    policy: WorkbookCalculationMetadataPolicy,
) -> tuple[bytes, dict[str, str], dict[str, str]]:
    """Set only ``calcPr@forceFullCalc`` after validating companion properties."""

    if not policy.policy_id.strip():
        raise FormulaAwareMaterializationError("Calculation metadata policy ID is empty.")
    matches = list(_CALCULATION_PROPERTIES_RE.finditer(data))
    if len(matches) != 1:
        raise FormulaAwareMaterializationError(
            f"Expected exactly one workbook calcPr element, found {len(matches)}."
        )
    match = matches[0]
    raw = match.group(0)
    before = _attributes(raw)
    expected = {
        "calcMode": policy.expected_calc_mode,
        "fullCalcOnLoad": _boolean_attribute(policy.expected_full_calc_on_load),
        "forceFullCalc": _boolean_attribute(policy.expected_force_full_calc),
    }
    observed = {key: before.get(key) for key in expected}
    if observed != expected:
        raise FormulaAwareMaterializationError(
            f"Calculation metadata precondition mismatch: expected {expected!r}, observed {observed!r}."
        )

    start_end = raw.find(b">")
    if start_end < 0:
        raise FormulaAwareMaterializationError("Malformed calcPr element.")
    start = raw[: start_end + 1]
    remainder = raw[start_end + 1 :]
    updated_start = _set_attribute(
        start,
        "forceFullCalc",
        _boolean_attribute(policy.force_full_calc),
    )
    updated = updated_start + remainder
    after = _attributes(updated)
    expected_after = dict(before)
    expected_after["forceFullCalc"] = _boolean_attribute(policy.force_full_calc)
    if after != expected_after:
        raise FormulaAwareMaterializationError(
            f"Calculation metadata mutation changed unexpected attributes: {before!r} -> {after!r}."
        )
    if updated == raw:
        raise FormulaAwareMaterializationError("Calculation metadata policy produced no mutation.")
    return data[: match.start()] + updated + data[match.end() :], before, after


def materialize_formula_aware_mutations(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    cell_mutations: Sequence[FormulaAwareCellMutation],
    defined_name_mutations: Sequence[DefinedNameMutation] = (),
    merge_mutations: Sequence[WorksheetMergeMutation] = (),
    row_mutations: Sequence[WorksheetRowMutation] = (),
    calculation_metadata_policy: WorkbookCalculationMetadataPolicy | None = None,
    expected_base_sha256: str | None = None,
) -> FormulaAwareMaterializationResult:
    """Apply an exact formula/value/name plan without rebuilding workbook parts."""

    base = Path(base_workbook)
    output = Path(output_workbook)
    if base.resolve() == output.resolve():
        raise FormulaAwareMaterializationError("Protected/base workbook cannot be an output target.")
    if output.exists():
        raise FormulaAwareMaterializationError(f"Refusing to overwrite existing output: {output}.")
    if base.suffix.lower() != ".xlsx" or output.suffix.lower() != ".xlsx":
        raise FormulaAwareMaterializationError("Formula-aware materialization requires .xlsx paths.")
    base_sha = sha256_file(base)
    if expected_base_sha256 is not None and base_sha != expected_base_sha256.lower():
        raise FormulaAwareMaterializationError(f"Base workbook hash changed: {base_sha}.")
    ordered = _validate_mutations(cell_mutations)
    merge_targets = [(item.target_sheet, item.range_ref) for item in merge_mutations]
    if len(merge_targets) != len(set(merge_targets)):
        raise FormulaAwareMaterializationError("Merge mutations contain duplicate targets.")
    for item in merge_mutations:
        _range_bounds(item.range_ref)
    row_targets = [(item.target_sheet, item.row) for item in row_mutations]
    if len(row_targets) != len(set(row_targets)):
        raise FormulaAwareMaterializationError("Row mutations contain duplicate targets.")
    if any(item.row < 1 for item in row_mutations):
        raise FormulaAwareMaterializationError("Worksheet row numbers must be positive.")
    if (
        not ordered
        and not defined_name_mutations
        and not merge_mutations
        and not row_mutations
        and calculation_metadata_policy is None
    ):
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(base, output)
        with ZipFile(base, "r") as archive:
            unchanged = len(archive.namelist())
        return FormulaAwareMaterializationResult(
            base_workbook_sha256=base_sha,
            output_workbook_sha256=sha256_file(output),
            canonical_ooxml_sha256=canonical_ooxml_sha256(output),
            changed_ooxml_parts=(),
            unchanged_ooxml_part_count=unchanged,
            cell_mutation_count=0,
            defined_name_upsert_count=0,
            defined_name_delete_count=0,
            merge_add_count=0,
            merge_delete_count=0,
            row_mutation_count=0,
            style_variant_count=0,
            write_type_counts={},
        )

    with ZipFile(base, "r") as source:
        members = {info.filename: source.read(info.filename) for info in source.infolist()}
        sheet_parts = _sheet_part_map(source)
        member_count = len(source.infolist())
    style_part = "xl/styles.xml"
    workbook_part = "xl/workbook.xml"
    if style_part not in members or workbook_part not in members:
        raise FormulaAwareMaterializationError("Workbook package lacks required core parts.")

    by_sheet: dict[str, list[FormulaAwareCellMutation]] = {}
    for item in ordered:
        if item.target_sheet not in sheet_parts:
            raise FormulaAwareMaterializationError(f"Missing target sheet {item.target_sheet!r}.")
        by_sheet.setdefault(item.target_sheet, []).append(item)
    merges_by_sheet: dict[str, list[WorksheetMergeMutation]] = {}
    for item in merge_mutations:
        if item.target_sheet not in sheet_parts:
            raise FormulaAwareMaterializationError(f"Missing merge target sheet {item.target_sheet!r}.")
        merges_by_sheet.setdefault(item.target_sheet, []).append(item)
    rows_by_sheet: dict[str, list[WorksheetRowMutation]] = {}
    for item in row_mutations:
        if item.target_sheet not in sheet_parts:
            raise FormulaAwareMaterializationError(f"Missing row target sheet {item.target_sheet!r}.")
        rows_by_sheet.setdefault(item.target_sheet, []).append(item)

    style_requests: list[tuple[int, str]] = []
    base_style_ids: dict[tuple[str, str], int] = {}
    for sheet_name, items in by_sheet.items():
        cells = _cell_elements(members[sheet_parts[sheet_name]])
        for item in items:
            source_coordinate = item.style_source_cell or item.target_cell
            located = cells.get(source_coordinate)
            style_id = 0 if located is None else _cell_style_id(located[2])
            base_style_ids[(sheet_name, item.target_cell)] = style_id
            if item.number_format_code is not None:
                style_requests.append((style_id, item.number_format_code))

    style_variants, updated_styles, style_variant_count = _resolve_style_variants(
        members[style_part], style_requests
    )
    style_ids: dict[tuple[str, str], int] = {}
    for item in ordered:
        key = (item.target_sheet, item.target_cell)
        base_style_id = base_style_ids[key]
        style_ids[key] = (
            style_variants[(base_style_id, item.number_format_code)]
            if item.number_format_code is not None
            else base_style_id
        )

    changed_parts: set[str] = set()
    for sheet_name in sorted(set(by_sheet) | set(merges_by_sheet) | set(rows_by_sheet)):
        part = sheet_parts[sheet_name]
        updated = _patch_worksheet(members[part], by_sheet.get(sheet_name, ()), style_ids)
        updated = _patch_merges(updated, merges_by_sheet.get(sheet_name, ()))
        updated = _patch_rows(updated, rows_by_sheet.get(sheet_name, ()))
        if updated != members[part]:
            members[part] = updated
            changed_parts.add(part)
    if updated_styles != members[style_part]:
        members[style_part] = updated_styles
        changed_parts.add(style_part)
    if defined_name_mutations:
        updated_workbook = _patch_defined_names(members[workbook_part], defined_name_mutations)
        if updated_workbook != members[workbook_part]:
            members[workbook_part] = updated_workbook
            changed_parts.add(workbook_part)
    calculation_metadata_before: dict[str, str] | None = None
    calculation_metadata_after: dict[str, str] | None = None
    if calculation_metadata_policy is not None:
        (
            updated_workbook,
            calculation_metadata_before,
            calculation_metadata_after,
        ) = _patch_calculation_metadata(members[workbook_part], calculation_metadata_policy)
        if updated_workbook != members[workbook_part]:
            members[workbook_part] = updated_workbook
            changed_parts.add(workbook_part)

    _write_package(base_workbook=base, output_workbook=output, members=members)
    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if before.namelist() != after.namelist():
            raise FormulaAwareMaterializationError("OOXML member inventory changed.")
        observed = {name for name in before.namelist() if before.read(name) != after.read(name)}
    if observed != changed_parts:
        raise FormulaAwareMaterializationError(
            f"Unexpected OOXML part changes: {sorted(observed ^ changed_parts)!r}."
        )

    write_types = Counter(
        "clear" if item.mode == "CLEAR_CONTENTS" else "formula" if item.mode == "SET_FORMULA" else str(item.value_kind)
        for item in ordered
    )
    return FormulaAwareMaterializationResult(
        base_workbook_sha256=base_sha,
        output_workbook_sha256=sha256_file(output),
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        changed_ooxml_parts=tuple(sorted(changed_parts)),
        unchanged_ooxml_part_count=member_count - len(changed_parts),
        cell_mutation_count=len(ordered),
        defined_name_upsert_count=sum(item.mode == "UPSERT" for item in defined_name_mutations),
        defined_name_delete_count=sum(item.mode == "DELETE" for item in defined_name_mutations),
        merge_add_count=sum(item.mode == "ADD" for item in merge_mutations),
        merge_delete_count=sum(item.mode == "DELETE" for item in merge_mutations),
        row_mutation_count=len(row_mutations),
        style_variant_count=style_variant_count,
        write_type_counts=dict(sorted(write_types.items())),
        calculation_metadata_change_count=0
        if calculation_metadata_policy is None
        else 1,
        calculation_metadata_policy_id=None
        if calculation_metadata_policy is None
        else calculation_metadata_policy.policy_id,
        calculation_metadata_before=calculation_metadata_before,
        calculation_metadata_after=calculation_metadata_after,
    )


__all__ = [
    "CANONICAL_OOXML_HASH_CONTRACT",
    "CALCULATION_METADATA_CONTRACT",
    "DefinedNameMutation",
    "FORMULA_CACHE_POLICY",
    "FormulaAwareCellMutation",
    "FormulaAwareMaterializationError",
    "FormulaAwareMaterializationResult",
    "MATERIALIZER_CONTRACT",
    "WorkbookCalculationMetadataPolicy",
    "WorksheetMergeMutation",
    "WorksheetRowMutation",
    "materialize_formula_aware_mutations",
]
