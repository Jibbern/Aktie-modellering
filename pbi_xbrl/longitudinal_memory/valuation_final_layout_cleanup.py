"""Lossless final Valuation layout cleanup over accepted capital economics.

This module owns presentation structure only.  It consumes the accepted
Capital Allocation / Capital Return workbook and binding receipt, applies a
small targeted OOXML patch, and moves the existing deterministic lineage JSON
records to a dedicated hidden support sheet.  It performs no source selection,
economic calculation, or formula authoring.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from html import escape, unescape
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping, Sequence
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _cell_elements,
    _set_attribute,
    _sheet_part_map,
    canonical_ooxml_sha256,
    sha256_file,
)


LAYOUT_CLEANUP_CONTRACT = "valuation-final-investor-layout-cleanup@1"
SEMANTIC_SNAPSHOT_CONTRACT = "valuation-final-layout-semantic-snapshot-sha256@1"
EXPECTED_BASE_WORKBOOK_SHA256 = (
    "d904d65f2bf19637c7f7ccabd8004b1af553e5191ec7804192c7cef6ccbe3912"
)
EXPECTED_PRIOR_BINDING_PLAN_DIGEST = (
    "9e20710ece030f29c34dd236fb01bf1e27c4da91583c420a697d26f0a55a02f6"
)
EXPECTED_PRODUCT_DIGEST = (
    "09160adb781a2efa44a91f77ee988f1f9ffce0afa4d1caa465a91f4f44bbcfbd"
)

VALUATION_SHEET = "Valuation"
VALUATION_PART = "xl/worksheets/sheet2.xml"
RIGHT_SIDE_LEGACY_RANGE = "O50:AC75"
OLD_LINEAGE_RANGE = "Valuation!A270:A297"
LINEAGE_SUPPORT_SHEET = "Capital_Product_Lineage"
LINEAGE_SUPPORT_RANGE = f"{LINEAGE_SUPPORT_SHEET}!A1:A28"
NORMAL_VALUATION_ROW_HEIGHT = 19.5
FINAL_VISIBLE_PRODUCT_ROW = 166

_SUBSECTION_ROWS = (127, 133, 141, 151, 159)
_SECTION_HEADERS = {
    "capital_allocation_summary": (128, ("B", "C", "D")),
    "annual_capital_allocation_history": (134, ("B", "C", "D", "E", "F")),
    "capital_return_summary": (142, ("B", "C", "D")),
    "quarterly_capital_return_history": (
        152,
        ("B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"),
    ),
    "annual_capital_return_history": (160, ("B", "C")),
}
_MAJOR_HEADER_STYLE_ID = 90
_SUBSECTION_ANCHOR_STYLE_ID = 38
_SUBSECTION_FILLER_STYLE_ID = 39
_TABLE_HEADER_STYLE_ID = 91
_EXPECTED_STYLE_RGB = {
    "major_section_header": "6FA8DC",
    "capital_subsection_header": "D9E7F3",
    "table_period_header": "EAF3FB",
}

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_DOCUMENT_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_WORKSHEET_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
)
_WORKSHEET_REL_TYPE = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
)

_CELL_COORDINATE_RE = re.compile(r"([A-Z]+)([1-9][0-9]*)\Z")
_ROW_RE = re.compile(
    rb"<row\b[^>]*\br=([\"'])(?P<row>[1-9][0-9]*)\1[^>]*(?:/>|>.*?</row>)",
    re.DOTALL,
)
_MERGE_CONTAINER_RE = re.compile(
    rb"<mergeCells\b[^>]*>(?P<body>.*?)</mergeCells>", re.DOTALL
)
_MERGE_RE = re.compile(rb"<mergeCell\b[^>]*/>")
_CONDITIONAL_FORMATTING_RE = re.compile(
    rb"<conditionalFormatting\b[^>]*>.*?</conditionalFormatting>", re.DOTALL
)
_COMMENT_RE = re.compile(rb"<comment\b[^>]*>.*?</comment>", re.DOTALL)
_VML_SHAPE_RE = re.compile(
    rb"<(?P<prefix>[A-Za-z_][A-Za-z0-9_.-]*):shape\b.*?</(?P=prefix):shape>",
    re.DOTALL,
)
_RELATIONSHIP_RE = re.compile(rb"<Relationship\b[^>]*/>")
_SHEET_RE = re.compile(rb"<sheet\b[^>]*/>")
_OVERRIDE_RE = re.compile(rb"<Override\b[^>]*/>")
_ATTRIBUTE_RE = re.compile(
    rb"\s(?P<name>[A-Za-z_:][A-Za-z0-9_.:-]*)=(?P<quote>[\"'])(?P<value>.*?)\2"
)
_VALUATION_REFERENCE_RE = re.compile(
    r"(?i)(?:'Valuation'|Valuation)!\$?[A-Z]{1,3}\$?([0-9]+)"
)


class ValuationFinalLayoutCleanupError(ValueError):
    """Fail-closed final-layout contract violation."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValuationFinalLayoutCleanupError(f"Duplicate JSON key {key!r}.")
        result[key] = value
    return result


def load_json_strict(path: Path | str) -> Any:
    return json.loads(
        Path(path).read_text(encoding="utf-8"),
        object_pairs_hook=_reject_duplicates,
    )


def _attributes(raw: bytes) -> dict[str, str]:
    start = raw[: raw.find(b">") + 1]
    return {
        match.group("name").decode("utf-8"): match.group("value").decode("utf-8")
        for match in _ATTRIBUTE_RE.finditer(start)
    }


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


def _coordinate(coordinate: str) -> tuple[int, int]:
    match = _CELL_COORDINATE_RE.fullmatch(coordinate.replace("$", ""))
    if match is None:
        raise ValuationFinalLayoutCleanupError(f"Invalid coordinate {coordinate!r}.")
    return _column_number(match.group(1)), int(match.group(2))


def _range_bounds(reference: str) -> tuple[int, int, int, int]:
    left, separator, right = reference.replace("$", "").partition(":")
    if not separator:
        right = left
    minimum_column, minimum_row = _coordinate(left)
    maximum_column, maximum_row = _coordinate(right)
    return minimum_column, minimum_row, maximum_column, maximum_row


def _intersects(reference: str, target: str) -> bool:
    left_min_c, left_min_r, left_max_c, left_max_r = _range_bounds(reference)
    right_min_c, right_min_r, right_max_c, right_max_r = _range_bounds(target)
    return not (
        left_max_c < right_min_c
        or right_max_c < left_min_c
        or left_max_r < right_min_r
        or right_max_r < left_min_r
    )


def _in_range(coordinate: str, target: str) -> bool:
    column, row = _coordinate(coordinate)
    minimum_column, minimum_row, maximum_column, maximum_row = _range_bounds(target)
    return minimum_column <= column <= maximum_column and minimum_row <= row <= maximum_row


def render_period_label(period: str) -> str:
    """Render generic source-native periods as investor-facing model labels."""

    quarter = re.fullmatch(r"([0-9]{4})-Q([1-4])", period)
    if quarter:
        return f"{quarter.group(1)}-Q{quarter.group(2)}"
    annual = re.fullmatch(r"([0-9]{4})-FY", period)
    if annual:
        return annual.group(1)
    ttm = re.fullmatch(r"TTM through ([0-9]{4})-Q([1-4])", period)
    if ttm:
        return f"TTM {ttm.group(1)}-Q{ttm.group(2)}"
    raise ValuationFinalLayoutCleanupError(f"Unsupported semantic period {period!r}.")


def _inline_text(raw_cell: bytes) -> str:
    match = re.search(rb"<t(?:\s[^>]*)?>(.*?)</t>", raw_cell, re.DOTALL)
    if match is None:
        raise ValuationFinalLayoutCleanupError("Expected an inline-string header cell.")
    return unescape(match.group(1).decode("utf-8"))


def _patch_inline_text(raw_cell: bytes, *, expected: str, replacement: str) -> bytes:
    current = _inline_text(raw_cell)
    if current != expected:
        raise ValuationFinalLayoutCleanupError(
            f"Period header changed: expected {expected!r}, found {current!r}."
        )
    encoded = escape(replacement, quote=False).encode("utf-8")
    return re.sub(
        rb"(?P<open><t(?:\s[^>]*)?>).*?(?P<close></t>)",
        lambda match: match.group("open") + encoded + match.group("close"),
        raw_cell,
        count=1,
        flags=re.DOTALL,
    )


def _patch_cell_style(raw_cell: bytes, style_id: int) -> bytes:
    end = raw_cell.find(b">")
    if end < 0:
        raise ValuationFinalLayoutCleanupError("Malformed worksheet cell.")
    start = _set_attribute(raw_cell[: end + 1], "s", str(style_id))
    return start + raw_cell[end + 1 :]


def _replace_cells(data: bytes, replacements: Mapping[str, bytes]) -> bytes:
    located = _cell_elements(data)
    missing = sorted(set(replacements) - set(located))
    if missing:
        raise ValuationFinalLayoutCleanupError(f"Missing cell targets: {missing!r}.")
    output = data
    changes = [
        (located[coordinate][0], located[coordinate][1], raw)
        for coordinate, raw in replacements.items()
    ]
    for start, end, replacement in sorted(changes, reverse=True):
        output = output[:start] + replacement + output[end:]
    return output


def _remove_cells(data: bytes, coordinates: Sequence[str]) -> bytes:
    located = _cell_elements(data)
    missing = sorted(set(coordinates) - set(located))
    if missing:
        raise ValuationFinalLayoutCleanupError(
            f"Expected removable cells are absent: {missing[:10]!r}."
        )
    output = data
    for start, end in sorted(
        ((located[coordinate][0], located[coordinate][1]) for coordinate in coordinates),
        reverse=True,
    ):
        output = output[:start] + output[end:]
    return output


def _remove_rows(data: bytes, rows: Sequence[int]) -> bytes:
    targets = set(rows)
    matches = {
        int(match.group("row")): (match.start(), match.end())
        for match in _ROW_RE.finditer(data)
        if int(match.group("row")) in targets
    }
    if set(matches) != targets:
        raise ValuationFinalLayoutCleanupError(
            f"Row-removal targets changed: missing {sorted(targets - set(matches))!r}."
        )
    output = data
    for start, end in sorted(matches.values(), reverse=True):
        output = output[:start] + output[end:]
    return output


def _patch_row_height(data: bytes, *, row: int, height: float) -> bytes:
    matches = [match for match in _ROW_RE.finditer(data) if int(match.group("row")) == row]
    if len(matches) != 1:
        raise ValuationFinalLayoutCleanupError(
            f"Expected exactly one row element for {row}, found {len(matches)}."
        )
    match = matches[0]
    raw = match.group(0)
    end = raw.find(b">")
    start = raw[: end + 1]
    start = _set_attribute(start, "ht", format(height, ".15g"))
    start = _set_attribute(start, "customHeight", "1")
    start = _set_attribute(start, "hidden", None)
    return data[: match.start()] + start + raw[end + 1 :] + data[match.end() :]


def _patch_merges(data: bytes, removed: Sequence[str]) -> bytes:
    container = _MERGE_CONTAINER_RE.search(data)
    if container is None:
        raise ValuationFinalLayoutCleanupError("Valuation lacks mergeCells.")
    targets = set(removed)
    retained: list[bytes] = []
    found: set[str] = set()
    for match in _MERGE_RE.finditer(container.group("body")):
        raw = match.group(0)
        reference = _attributes(raw).get("ref", "")
        if reference in targets:
            found.add(reference)
        else:
            retained.append(raw)
    if found != targets:
        raise ValuationFinalLayoutCleanupError(
            f"Merge-removal plan drifted: {sorted(targets - found)!r}."
        )
    start_tag_end = container.group(0).find(b">")
    start_tag = container.group(0)[: start_tag_end + 1]
    start_tag = _set_attribute(start_tag, "count", str(len(retained)))
    replacement = start_tag + b"".join(retained) + b"</mergeCells>"
    return data[: container.start()] + replacement + data[container.end() :]


def _patch_conditional_formatting(data: bytes, removed_ranges: Sequence[str]) -> bytes:
    targets = set(removed_ranges)
    found: set[str] = set()
    output: list[bytes] = []
    cursor = 0
    for match in _CONDITIONAL_FORMATTING_RE.finditer(data):
        raw = match.group(0)
        sqref = _attributes(raw).get("sqref", "")
        if sqref in targets:
            output.append(data[cursor : match.start()])
            cursor = match.end()
            found.add(sqref)
    output.append(data[cursor:])
    if found != targets:
        raise ValuationFinalLayoutCleanupError(
            f"Conditional-format retirement changed: {sorted(targets - found)!r}."
        )
    return b"".join(output)


def _patch_dimension(data: bytes) -> tuple[bytes, str]:
    cells = _cell_elements(data)
    if not cells:
        raise ValuationFinalLayoutCleanupError("Valuation cannot become empty.")
    coordinates = [_coordinate(coordinate) for coordinate in cells]
    maximum_column = max(column for column, _ in coordinates)
    maximum_row = max(row for _, row in coordinates)
    dimension = f"A1:{_column_name(maximum_column)}{maximum_row}"
    match = re.search(rb"<dimension\b[^>]*/>", data)
    if match is None:
        raise ValuationFinalLayoutCleanupError("Valuation lacks dimension metadata.")
    replacement = _set_attribute(match.group(0), "ref", dimension)
    return data[: match.start()] + replacement + data[match.end() :], dimension


def _comment_reference(raw: bytes) -> str:
    reference = _attributes(raw).get("ref")
    if not reference:
        raise ValuationFinalLayoutCleanupError("Comment lacks a cell reference.")
    return reference


def _patch_comments(data: bytes, removed: Sequence[str]) -> bytes:
    targets = set(removed)
    found: set[str] = set()
    output: list[bytes] = []
    cursor = 0
    for match in _COMMENT_RE.finditer(data):
        raw = match.group(0)
        reference = _comment_reference(raw)
        if reference in targets:
            output.append(data[cursor : match.start()])
            cursor = match.end()
            found.add(reference)
    output.append(data[cursor:])
    if found != targets:
        raise ValuationFinalLayoutCleanupError(
            f"Comment-removal plan drifted: {sorted(targets - found)!r}."
        )
    return b"".join(output)


def _vml_coordinate(raw: bytes) -> str:
    row = re.search(rb"<[A-Za-z_][A-Za-z0-9_.-]*:Row>([0-9]+)</", raw)
    column = re.search(rb"<[A-Za-z_][A-Za-z0-9_.-]*:Column>([0-9]+)</", raw)
    if row is None or column is None:
        raise ValuationFinalLayoutCleanupError("VML note shape lacks row/column ownership.")
    return f"{_column_name(int(column.group(1)) + 1)}{int(row.group(1)) + 1}"


def _patch_vml(data: bytes, removed: Sequence[str]) -> bytes:
    targets = set(removed)
    found: set[str] = set()
    output: list[bytes] = []
    cursor = 0
    for match in _VML_SHAPE_RE.finditer(data):
        raw = match.group(0)
        coordinate = _vml_coordinate(raw)
        if coordinate in targets:
            output.append(data[cursor : match.start()])
            cursor = match.end()
            found.add(coordinate)
    output.append(data[cursor:])
    if found != targets:
        raise ValuationFinalLayoutCleanupError(
            f"VML-removal plan drifted: {sorted(targets - found)!r}."
        )
    return b"".join(output)


def _support_sheet_xml(records: Sequence[str]) -> bytes:
    if len(records) != 28:
        raise ValuationFinalLayoutCleanupError("Lineage record count changed from 28.")
    rows = []
    for row, record in enumerate(records, start=1):
        payload = escape(record, quote=False)
        rows.append(
            f'<row r="{row}"><c r="A{row}" t="inlineStr"><is><t>{payload}</t></is></c></row>'
        )
    body = "".join(rows)
    return (
        f'<worksheet xmlns="{_MAIN_NS}"><sheetPr><outlinePr summaryBelow="1" '
        f'summaryRight="1"/></sheetPr><dimension ref="A1:A28"/><sheetViews><sheetView '
        f'workbookViewId="0"/></sheetViews><sheetFormatPr baseColWidth="8" '
        f'defaultRowHeight="15"/><sheetData>{body}</sheetData></worksheet>'
    ).encode("utf-8")


def _next_sheet_identity(members: Mapping[str, bytes]) -> tuple[str, int, str]:
    workbook = members["xl/workbook.xml"]
    relationships = members["xl/_rels/workbook.xml.rels"]
    sheet_ids = [int(_attributes(match.group(0))["sheetId"]) for match in _SHEET_RE.finditer(workbook)]
    relationship_ids = {
        _attributes(match.group(0)).get("Id", "")
        for match in _RELATIONSHIP_RE.finditer(relationships)
    }
    sheet_numbers = [
        int(match.group(1))
        for name in members
        if (match := re.fullmatch(r"xl/worksheets/sheet([0-9]+)\.xml", name))
    ]
    sheet_number = max(sheet_numbers) + 1
    sheet_id = max(sheet_ids) + 1
    relationship_number = 1
    while f"rId{relationship_number}" in relationship_ids:
        relationship_number += 1
    return f"xl/worksheets/sheet{sheet_number}.xml", sheet_id, f"rId{relationship_number}"


def _append_support_sheet(
    members: dict[str, bytes], records: Sequence[str]
) -> tuple[str, int, str]:
    workbook = members["xl/workbook.xml"]
    if LINEAGE_SUPPORT_SHEET.encode("utf-8") in workbook:
        raise ValuationFinalLayoutCleanupError("Lineage support sheet already exists.")
    part, sheet_id, relationship_id = _next_sheet_identity(members)
    sheet_xml = _support_sheet_xml(records)

    sheet_tag = (
        f'<sheet xmlns:r="{_DOCUMENT_REL_NS}" name="{LINEAGE_SUPPORT_SHEET}" '
        f'sheetId="{sheet_id}" state="hidden" r:id="{relationship_id}"/>'
    ).encode("utf-8")
    marker = b"</sheets>"
    if workbook.count(marker) != 1:
        raise ValuationFinalLayoutCleanupError("Workbook sheets container changed.")
    members["xl/workbook.xml"] = workbook.replace(marker, sheet_tag + marker, 1)

    relationships = members["xl/_rels/workbook.xml.rels"]
    relationship_tag = (
        f'<Relationship Type="{_WORKSHEET_REL_TYPE}" Target="/{part}" '
        f'Id="{relationship_id}"/>'
    ).encode("utf-8")
    marker = b"</Relationships>"
    if relationships.count(marker) != 1:
        raise ValuationFinalLayoutCleanupError("Workbook relationships container changed.")
    members["xl/_rels/workbook.xml.rels"] = relationships.replace(
        marker, relationship_tag + marker, 1
    )

    content_types = members["[Content_Types].xml"]
    override = (
        f'<Override PartName="/{part}" ContentType="{_WORKSHEET_CONTENT_TYPE}"/>'
    ).encode("utf-8")
    marker = b"</Types>"
    if content_types.count(marker) != 1:
        raise ValuationFinalLayoutCleanupError("Content-types container changed.")
    members["[Content_Types].xml"] = content_types.replace(marker, override + marker, 1)
    members[part] = sheet_xml
    return part, sheet_id, relationship_id


def _write_package_with_addition(
    *, base_workbook: Path, output_workbook: Path, members: Mapping[str, bytes]
) -> None:
    output_workbook.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{output_workbook.stem}.",
        suffix=output_workbook.suffix,
        dir=output_workbook.parent,
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with ZipFile(base_workbook, "r") as source, ZipFile(temporary, "w") as output:
            output.comment = source.comment
            existing = []
            for info in source.infolist():
                existing.append(info.filename)
                output.writestr(info, members[info.filename])
            additions = sorted(set(members) - set(existing))
            for name in additions:
                info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = ZIP_DEFLATED
                info.create_system = 0
                info.external_attr = 0
                output.writestr(info, members[name])
        os.replace(temporary, output_workbook)
    finally:
        if temporary.exists():
            temporary.unlink()


@dataclass(frozen=True)
class PeriodLabelMutation:
    section: str
    target_cell: str
    semantic_period: str
    source_periods: tuple[str, ...]
    old_label: str
    new_label: str
    binding_count: int
    binding_identity_digest: str
    value_status_digest: str


@dataclass(frozen=True)
class ValuationFinalLayoutCleanupPlan:
    contract: str
    base_workbook_sha256: str
    prior_plan_sha256: str
    prior_binding_plan_digest: str
    product_digest: str
    period_label_mutations: tuple[PeriodLabelMutation, ...]
    period_label_plan_digest: str
    right_side_cells: tuple[str, ...]
    right_side_style_counts: Mapping[str, int]
    right_side_merges: tuple[str, ...]
    post_product_tail_cells: tuple[str, ...]
    retired_row_merges: tuple[str, ...]
    retired_rows: tuple[int, ...]
    retired_surface_rows: tuple[int, ...]
    old_lineage_rows: tuple[int, ...]
    removed_comment_refs: tuple[str, ...]
    preserved_comment_refs: tuple[str, ...]
    removed_conditional_format_ranges: tuple[str, ...]
    lineage_records: tuple[str, ...]
    lineage_record_sha256: tuple[str, ...]
    style_contract: Mapping[str, Any]
    reference_preflight: Mapping[str, Any]
    plan_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_workbook_sha256": self.base_workbook_sha256,
            "contract": self.contract,
            "lineage_record_sha256": list(self.lineage_record_sha256),
            "lineage_records": list(self.lineage_records),
            "new_lineage_support_range": LINEAGE_SUPPORT_RANGE,
            "old_lineage_range": OLD_LINEAGE_RANGE,
            "old_lineage_rows": list(self.old_lineage_rows),
            "period_label_mutations": [asdict(item) for item in self.period_label_mutations],
            "period_label_plan_digest": self.period_label_plan_digest,
            "plan_digest": self.plan_digest,
            "post_product_tail_cells": list(self.post_product_tail_cells),
            "preserved_comment_refs": list(self.preserved_comment_refs),
            "prior_binding_plan_digest": self.prior_binding_plan_digest,
            "prior_plan_sha256": self.prior_plan_sha256,
            "product_digest": self.product_digest,
            "reference_preflight": dict(self.reference_preflight),
            "removed_comment_refs": list(self.removed_comment_refs),
            "removed_conditional_format_ranges": list(
                self.removed_conditional_format_ranges
            ),
            "retired_row_merges": list(self.retired_row_merges),
            "retired_rows": list(self.retired_rows),
            "retired_surface_rows": list(self.retired_surface_rows),
            "right_side_cells": list(self.right_side_cells),
            "right_side_merges": list(self.right_side_merges),
            "right_side_style_counts": dict(self.right_side_style_counts),
            "style_contract": dict(self.style_contract),
        }


@dataclass(frozen=True)
class ValuationFinalLayoutMaterializationResult:
    contract: str
    plan_digest: str
    base_workbook_sha256: str
    output_workbook_sha256: str
    canonical_ooxml_contract: str
    canonical_ooxml_sha256: str
    changed_ooxml_parts: tuple[str, ...]
    added_ooxml_parts: tuple[str, ...]
    unchanged_ooxml_part_count: int
    valuation_dimension: str
    lineage_support_part: str
    lineage_support_sheet_id: int
    lineage_support_relationship_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _cell_content_present(raw: bytes) -> bool:
    return any(token in raw for token in (b"<v", b"<f", b"<is"))


def _comment_refs(data: bytes) -> tuple[str, ...]:
    return tuple(_comment_reference(match.group(0)) for match in _COMMENT_RE.finditer(data))


def _merge_refs(data: bytes) -> tuple[str, ...]:
    container = _MERGE_CONTAINER_RE.search(data)
    if container is None:
        return ()
    return tuple(
        _attributes(match.group(0)).get("ref", "")
        for match in _MERGE_RE.finditer(container.group("body"))
    )


def _lineage_records(valuation_data: bytes) -> tuple[str, ...]:
    cells = _cell_elements(valuation_data)
    records: list[str] = []
    for row in range(270, 298):
        raw = cells.get(f"A{row}")
        if raw is None:
            raise ValuationFinalLayoutCleanupError(f"Missing lineage record A{row}.")
        value = _inline_text(raw[2])
        load = json.loads(value, object_pairs_hook=_reject_duplicates)
        if not isinstance(load, Mapping) or "bindings" not in load:
            raise ValuationFinalLayoutCleanupError(f"Malformed lineage record A{row}.")
        records.append(value)
    return tuple(records)


def _reference_preflight(members: Mapping[str, bytes]) -> dict[str, Any]:
    hits: list[dict[str, Any]] = []
    ref_errors: list[str] = []
    for part, payload in members.items():
        if not part.endswith((".xml", ".rels", ".vml")):
            continue
        if b"#REF!" in payload:
            ref_errors.append(part)
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for match in _VALUATION_REFERENCE_RE.finditer(text):
            row = int(match.group(1))
            if 167 <= row <= 297:
                hits.append({"part": part, "reference": match.group(0), "row": row})
    return {
        "deleted_or_cleaned_row_reference_count": len(hits),
        "deleted_or_cleaned_row_references": hits,
        "ref_error_part_count": len(ref_errors),
        "ref_error_parts": sorted(ref_errors),
    }


def _period_label_plan(
    prior_plan: Mapping[str, Any], valuation_data: bytes
) -> tuple[PeriodLabelMutation, ...]:
    bindings = prior_plan.get("bindings")
    if not isinstance(bindings, list) or len(bindings) != 140:
        raise ValuationFinalLayoutCleanupError("Accepted binding universe changed from 140.")
    by_section_column: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for binding in bindings:
        target = str(binding.get("target_cell", ""))
        if not target.startswith("Valuation!"):
            raise ValuationFinalLayoutCleanupError("Capital binding left Valuation.")
        coordinate = target.split("!", 1)[1]
        match = _CELL_COORDINATE_RE.fullmatch(coordinate)
        if match is None:
            raise ValuationFinalLayoutCleanupError(f"Invalid binding target {target!r}.")
        by_section_column[(str(binding["section"]), match.group(1))].append(binding)

    cells = _cell_elements(valuation_data)
    result: list[PeriodLabelMutation] = []
    for section, (header_row, columns) in _SECTION_HEADERS.items():
        for column in columns:
            section_bindings = by_section_column.get((section, column), [])
            if not section_bindings:
                raise ValuationFinalLayoutCleanupError(
                    f"No accepted bindings own {section} column {column}."
                )
            periods = {str(item["period"]) for item in section_bindings}
            if len(periods) != 1:
                raise ValuationFinalLayoutCleanupError(
                    f"Ambiguous semantic period for {section} column {column}: {periods!r}."
                )
            semantic_period = periods.pop()
            old_labels = {str(item["display_period"]) for item in section_bindings}
            if len(old_labels) != 1:
                raise ValuationFinalLayoutCleanupError(
                    f"Ambiguous legacy label for {section} column {column}."
                )
            old_label = old_labels.pop()
            target_cell = f"{column}{header_row}"
            located = cells.get(target_cell)
            if located is None or _inline_text(located[2]) != old_label:
                raise ValuationFinalLayoutCleanupError(
                    f"Visible header and binding metadata disagree at {target_cell}."
                )
            binding_identity = [
                {
                    key: value
                    for key, value in item.items()
                    if key not in {"display_period", "value", "status"}
                }
                for item in section_bindings
            ]
            value_status = [
                {
                    "status": item.get("status"),
                    "target_cell": item.get("target_cell"),
                    "value": item.get("value"),
                }
                for item in section_bindings
            ]
            result.append(
                PeriodLabelMutation(
                    section=section,
                    target_cell=target_cell,
                    semantic_period=semantic_period,
                    source_periods=tuple(
                        sorted({str(item["source_period"]) for item in section_bindings})
                    ),
                    old_label=old_label,
                    new_label=render_period_label(semantic_period),
                    binding_count=len(section_bindings),
                    binding_identity_digest=_digest(binding_identity),
                    value_status_digest=_digest(value_status),
                )
            )
    return tuple(result)


def build_valuation_final_layout_cleanup_plan(
    *, base_workbook: Path | str, prior_plan_path: Path | str
) -> ValuationFinalLayoutCleanupPlan:
    base = Path(base_workbook)
    prior_path = Path(prior_plan_path)
    base_sha = sha256_file(base)
    if base_sha != EXPECTED_BASE_WORKBOOK_SHA256:
        raise ValuationFinalLayoutCleanupError(
            f"Accepted final-layout input changed: {base_sha}."
        )
    prior_plan = load_json_strict(prior_path)
    if prior_plan.get("binding_plan_digest") != EXPECTED_PRIOR_BINDING_PLAN_DIGEST:
        raise ValuationFinalLayoutCleanupError("Accepted binding-plan digest changed.")
    product_digest = str(prior_plan.get("investor_product", {}).get("product_digest", ""))
    if product_digest != EXPECTED_PRODUCT_DIGEST:
        raise ValuationFinalLayoutCleanupError("Accepted investor-product digest changed.")

    with ZipFile(base, "r") as archive:
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
        sheet_parts = _sheet_part_map(archive)
    if sheet_parts.get(VALUATION_SHEET) != VALUATION_PART:
        raise ValuationFinalLayoutCleanupError("Valuation worksheet ownership changed.")
    valuation_data = members[VALUATION_PART]
    cells = _cell_elements(valuation_data)
    right_side_cells = tuple(
        sorted(
            (coordinate for coordinate in cells if _in_range(coordinate, RIGHT_SIDE_LEGACY_RANGE)),
            key=lambda value: (_coordinate(value)[1], _coordinate(value)[0]),
        )
    )
    if len(right_side_cells) != 390:
        raise ValuationFinalLayoutCleanupError("Right-side legacy cell universe changed from 390.")
    if any(_cell_content_present(cells[coordinate][2]) for coordinate in right_side_cells):
        raise ValuationFinalLayoutCleanupError("Right-side legacy block gained economic content.")
    right_style_counts = Counter(
        str(int(_attributes(cells[coordinate][2]).get("s", "0")))
        for coordinate in right_side_cells
    )

    post_product_tail_cells = tuple(
        sorted(
            (
                coordinate
                for coordinate in cells
                if 167 <= _coordinate(coordinate)[1] <= 191
            ),
            key=lambda value: (_coordinate(value)[1], _coordinate(value)[0]),
        )
    )
    if len(post_product_tail_cells) != 679:
        raise ValuationFinalLayoutCleanupError(
            "Neutral post-product ghost-cell universe changed from 679."
        )
    if any(_cell_content_present(cells[coordinate][2]) for coordinate in post_product_tail_cells):
        raise ValuationFinalLayoutCleanupError("Post-product neutral tail gained content.")

    merge_refs = _merge_refs(valuation_data)
    right_merges = tuple(
        sorted(reference for reference in merge_refs if _intersects(reference, RIGHT_SIDE_LEGACY_RANGE))
    )
    retired_merges = tuple(
        sorted(reference for reference in merge_refs if _intersects(reference, "A201:AO261"))
    )
    if len(right_merges) != 75 or len(retired_merges) != 132:
        raise ValuationFinalLayoutCleanupError(
            "Accepted right-side/retired merge universes changed."
        )

    comment_data = members["xl/comments/comment2.xml"]
    comments = _comment_refs(comment_data)
    removed_comments = tuple(
        reference
        for reference in comments
        if _in_range(reference, RIGHT_SIDE_LEGACY_RANGE)
        or _in_range(reference, "A192:AO261")
    )
    preserved_comments = tuple(reference for reference in comments if reference not in removed_comments)
    if len(removed_comments) != 109 or len(preserved_comments) != 40:
        raise ValuationFinalLayoutCleanupError("Accepted Valuation comment universe changed.")

    conditional_ranges = tuple(
        _attributes(match.group(0)).get("sqref", "")
        for match in _CONDITIONAL_FORMATTING_RE.finditer(valuation_data)
        if _attributes(match.group(0)).get("sqref", "") == "Q223:Q230"
    )
    if conditional_ranges != ("Q223:Q230",):
        raise ValuationFinalLayoutCleanupError("Retired conditional formatting changed.")

    records = _lineage_records(valuation_data)
    flattened = [
        binding
        for record in records
        for binding in json.loads(record, object_pairs_hook=_reject_duplicates)["bindings"]
    ]
    if flattened != prior_plan["bindings"]:
        raise ValuationFinalLayoutCleanupError(
            "Embedded lineage no longer reconstructs the accepted binding plan."
        )
    period_plan = _period_label_plan(prior_plan, valuation_data)
    if len(period_plan) != 25:
        raise ValuationFinalLayoutCleanupError("Visible capital header universe changed from 25.")

    reference_preflight = _reference_preflight(members)
    if (
        reference_preflight["deleted_or_cleaned_row_reference_count"] != 0
        or reference_preflight["ref_error_part_count"] != 0
    ):
        raise ValuationFinalLayoutCleanupError(
            "A surviving workbook dependency references the retired Valuation tail."
        )

    style_contract = {
        "blank_spacer": {
            "row": 139,
            "height": NORMAL_VALUATION_ROW_HEIGHT,
            "source": "surrounding Valuation body rows 127:138 and 140:166",
        },
        "capital_subsection_header": {
            "anchor_style_id": _SUBSECTION_ANCHOR_STYLE_ID,
            "fill_rgb": _EXPECTED_STYLE_RGB["capital_subsection_header"],
            "filler_style_id": _SUBSECTION_FILLER_STYLE_ID,
            "rows": list(_SUBSECTION_ROWS),
            "source_range": "Valuation!A68:M68",
        },
        "historical_value": {"preserved_from_binding": True},
        "major_section_header": {
            "fill_rgb": _EXPECTED_STYLE_RGB["major_section_header"],
            "style_id": _MAJOR_HEADER_STYLE_ID,
        },
        "metric_label": {"preserved_from_binding": True},
        "table_period_header": {
            "fill_rgb": _EXPECTED_STYLE_RGB["table_period_header"],
            "style_id": _TABLE_HEADER_STYLE_ID,
        },
    }
    payload = {
        "base_workbook_sha256": base_sha,
        "conditional_formats": list(conditional_ranges),
        "contract": LAYOUT_CLEANUP_CONTRACT,
        "lineage_record_sha256": [hashlib.sha256(item.encode("utf-8")).hexdigest() for item in records],
        "new_lineage_support_range": LINEAGE_SUPPORT_RANGE,
        "period_label_mutations": [asdict(item) for item in period_plan],
        "post_product_tail_cells": list(post_product_tail_cells),
        "removed_comments": list(removed_comments),
        "retired_merges": list(retired_merges),
        "retired_rows": list(range(201, 262)),
        "retired_surface_rows": list(range(192, 201)),
        "right_merges": list(right_merges),
        "right_side_cells": list(right_side_cells),
        "style_contract": style_contract,
    }
    plan_digest = _digest(payload)
    return ValuationFinalLayoutCleanupPlan(
        contract=LAYOUT_CLEANUP_CONTRACT,
        base_workbook_sha256=base_sha,
        prior_plan_sha256=sha256_file(prior_path),
        prior_binding_plan_digest=EXPECTED_PRIOR_BINDING_PLAN_DIGEST,
        product_digest=product_digest,
        period_label_mutations=period_plan,
        period_label_plan_digest=_digest([asdict(item) for item in period_plan]),
        right_side_cells=right_side_cells,
        right_side_style_counts=dict(sorted(right_style_counts.items(), key=lambda item: int(item[0]))),
        right_side_merges=right_merges,
        post_product_tail_cells=post_product_tail_cells,
        retired_row_merges=retired_merges,
        retired_rows=tuple(range(201, 262)),
        retired_surface_rows=tuple(range(192, 201)),
        old_lineage_rows=tuple(range(270, 298)),
        removed_comment_refs=removed_comments,
        preserved_comment_refs=preserved_comments,
        removed_conditional_format_ranges=conditional_ranges,
        lineage_records=records,
        lineage_record_sha256=tuple(
            hashlib.sha256(item.encode("utf-8")).hexdigest() for item in records
        ),
        style_contract=style_contract,
        reference_preflight=reference_preflight,
        plan_digest=plan_digest,
    )


def materialize_valuation_final_layout_cleanup(
    *,
    plan: ValuationFinalLayoutCleanupPlan,
    base_workbook: Path | str,
    output_workbook: Path | str,
) -> ValuationFinalLayoutMaterializationResult:
    if plan.contract != LAYOUT_CLEANUP_CONTRACT:
        raise ValuationFinalLayoutCleanupError("Final-layout contract changed.")
    base = Path(base_workbook)
    output = Path(output_workbook)
    if base.resolve() == output.resolve():
        raise ValuationFinalLayoutCleanupError("Accepted input cannot be an output target.")
    if output.exists():
        raise ValuationFinalLayoutCleanupError(f"Refusing to overwrite {output}.")
    if sha256_file(base) != plan.base_workbook_sha256:
        raise ValuationFinalLayoutCleanupError("Final-layout input changed after planning.")

    with ZipFile(base, "r") as archive:
        original_names = tuple(info.filename for info in archive.infolist())
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}

    valuation = members[VALUATION_PART]
    cells = _cell_elements(valuation)
    replacements: dict[str, bytes] = {}
    for mutation in plan.period_label_mutations:
        replacements[mutation.target_cell] = _patch_inline_text(
            cells[mutation.target_cell][2],
            expected=mutation.old_label,
            replacement=mutation.new_label,
        )
    for row in _SUBSECTION_ROWS:
        for column in range(1, 14):
            coordinate = f"{_column_name(column)}{row}"
            replacements[coordinate] = _patch_cell_style(
                replacements.get(coordinate, cells[coordinate][2]),
                _SUBSECTION_ANCHOR_STYLE_ID if column == 1 else _SUBSECTION_FILLER_STYLE_ID,
            )
    valuation = _replace_cells(valuation, replacements)
    valuation = _patch_row_height(
        valuation, row=139, height=NORMAL_VALUATION_ROW_HEIGHT
    )
    valuation = _remove_cells(
        valuation, plan.right_side_cells + plan.post_product_tail_cells
    )
    valuation = _remove_rows(
        valuation,
        plan.retired_surface_rows + plan.retired_rows + plan.old_lineage_rows,
    )
    valuation = _patch_merges(
        valuation, plan.right_side_merges + plan.retired_row_merges
    )
    valuation = _patch_conditional_formatting(
        valuation, plan.removed_conditional_format_ranges
    )
    valuation, dimension = _patch_dimension(valuation)
    if dimension != "A1:AI166":
        raise ValuationFinalLayoutCleanupError(
            f"Final Valuation used range changed unexpectedly: {dimension}."
        )
    members[VALUATION_PART] = valuation

    members["xl/comments/comment2.xml"] = _patch_comments(
        members["xl/comments/comment2.xml"], plan.removed_comment_refs
    )
    members["xl/drawings/commentsDrawing2.vml"] = _patch_vml(
        members["xl/drawings/commentsDrawing2.vml"], plan.removed_comment_refs
    )
    support_part, support_sheet_id, support_relationship_id = _append_support_sheet(
        members, plan.lineage_records
    )
    _write_package_with_addition(
        base_workbook=base, output_workbook=output, members=members
    )

    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        before_names = set(before.namelist())
        after_names = set(after.namelist())
        removed_parts = before_names - after_names
        added_parts = tuple(sorted(after_names - before_names))
        if removed_parts or added_parts != (support_part,):
            raise ValuationFinalLayoutCleanupError(
                f"Unexpected OOXML inventory delta: added={added_parts}, removed={sorted(removed_parts)}."
            )
        changed = tuple(
            sorted(
                name
                for name in before_names | after_names
                if name not in before_names
                or name not in after_names
                or before.read(name) != after.read(name)
            )
        )
    expected_changed = {
        "[Content_Types].xml",
        "xl/_rels/workbook.xml.rels",
        "xl/comments/comment2.xml",
        "xl/drawings/commentsDrawing2.vml",
        "xl/workbook.xml",
        VALUATION_PART,
        support_part,
    }
    if set(changed) != expected_changed:
        raise ValuationFinalLayoutCleanupError(
            f"Unexpected changed OOXML parts: {sorted(set(changed) ^ expected_changed)!r}."
        )
    return ValuationFinalLayoutMaterializationResult(
        contract=LAYOUT_CLEANUP_CONTRACT,
        plan_digest=plan.plan_digest,
        base_workbook_sha256=plan.base_workbook_sha256,
        output_workbook_sha256=sha256_file(output),
        canonical_ooxml_contract=CANONICAL_OOXML_HASH_CONTRACT,
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        changed_ooxml_parts=changed,
        added_ooxml_parts=added_parts,
        unchanged_ooxml_part_count=len(original_names) - (len(changed) - len(added_parts)),
        valuation_dimension=dimension,
        lineage_support_part=support_part,
        lineage_support_sheet_id=support_sheet_id,
        lineage_support_relationship_id=support_relationship_id,
    )


__all__ = [
    "EXPECTED_BASE_WORKBOOK_SHA256",
    "EXPECTED_PRIOR_BINDING_PLAN_DIGEST",
    "FINAL_VISIBLE_PRODUCT_ROW",
    "LAYOUT_CLEANUP_CONTRACT",
    "LINEAGE_SUPPORT_RANGE",
    "LINEAGE_SUPPORT_SHEET",
    "NORMAL_VALUATION_ROW_HEIGHT",
    "OLD_LINEAGE_RANGE",
    "RIGHT_SIDE_LEGACY_RANGE",
    "SEMANTIC_SNAPSHOT_CONTRACT",
    "ValuationFinalLayoutCleanupError",
    "ValuationFinalLayoutCleanupPlan",
    "ValuationFinalLayoutMaterializationResult",
    "build_valuation_final_layout_cleanup_plan",
    "load_json_strict",
    "materialize_valuation_final_layout_cleanup",
    "render_period_label",
]
