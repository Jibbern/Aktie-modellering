"""Lossless shared workbook projection for cross-ticker Operating Drivers.

The workbook is a presentation consumer only.  It blanks the existing
Operating_Drivers canvas in an isolated copy, applies the frozen investor
contract with targeted OOXML mutations, and preserves every unrelated package
member (including VBA for XLSM inputs).
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from typing import Any, Mapping, Sequence
import xml.etree.ElementTree as ET
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.capital_return_debt_workbook_materialization import (
    FormulaAwareCellMutation,
    WorksheetColumnMutation,
    WorksheetDimensionMutation,
    WorksheetRowMutation,
    materialize_capital_return_debt_mutations,
)
from pbi_xbrl.longitudinal_memory.formula_aware_workbook_materialization import (
    WorksheetMergeMutation,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (
    SHEET_NAME,
    _prepare_blank_surface,
    _patch_smart_precision,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_product import (
    CrossTickerOperatingDriverPackage,
    DriverObservation,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _sheet_part_map,
    _write_package,
    canonical_ooxml_sha256,
    sha256_file,
)


WORKBOOK_CONTRACT = "operating-drivers-cross-ticker-lossless-workbook@1"
SEMANTIC_HASH_CONTRACT = "operating-drivers-cross-ticker-visible-snapshot-sha256@1"
ZOOM_SCALE = 110

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_CELL_RE = re.compile(r"([A-Z]+)([1-9][0-9]*)")


class OperatingDriverCrossTickerWorkbookError(ValueError):
    """Raised when the shared workbook projection cannot be proven lossless."""


def _digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CrossTickerWorkbookBinding:
    semantic_id: str
    element_type: str
    target_range: str
    value_kind: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class CrossTickerWorkbookPlan:
    contract_version: str
    ticker: str
    sheet_name: str
    used_range: str
    zoom_scale: int
    cell_mutations: tuple[FormulaAwareCellMutation, ...]
    merge_mutations: tuple[WorksheetMergeMutation, ...]
    row_mutations: tuple[WorksheetRowMutation, ...]
    column_mutations: tuple[WorksheetColumnMutation, ...]
    dimension_mutation: WorksheetDimensionMutation
    bindings: tuple[CrossTickerWorkbookBinding, ...]
    display_number_formats: Mapping[str, str]
    exact_numeric_coordinates: tuple[str, ...]
    missing_coordinates: tuple[str, ...]
    major_section_rows: Mapping[str, int]
    core_group_rows: Mapping[str, int]
    core_metric_rows: Mapping[str, int]
    history_group_rows: Mapping[str, int]
    history_metric_rows: Mapping[str, int]
    guide_rows: Mapping[str, int]
    plan_sha256: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["cell_mutations"] = [asdict(item) for item in self.cell_mutations]
        value["merge_mutations"] = [asdict(item) for item in self.merge_mutations]
        value["row_mutations"] = [asdict(item) for item in self.row_mutations]
        value["column_mutations"] = [asdict(item) for item in self.column_mutations]
        value["dimension_mutation"] = asdict(self.dimension_mutation)
        return value


@dataclass(frozen=True)
class CrossTickerWorkbookResult:
    ticker: str
    base_workbook_sha256: str
    output_workbook_sha256: str
    semantic_workbook_sha256: str
    canonical_ooxml_sha256: str
    canonical_ooxml_contract: str
    changed_ooxml_parts: tuple[str, ...]
    allowed_changed_ooxml_parts: tuple[str, ...]
    unrelated_workbook_delta_count: int
    unchanged_ooxml_part_count: int
    target_formula_count: int
    exact_numeric_cell_count: int
    exact_numeric_stored_as_text_count: int
    missing_to_zero_count: int
    comments_removed_count: int
    vba_sha256_before: str | None
    vba_sha256_after: str | None
    vba_delta_count: int
    plan_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _column_index(column: str) -> int:
    result = 0
    for char in column:
        result = result * 26 + ord(char) - 64
    return result


def _column_name(index: int) -> str:
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _range_coordinates(range_ref: str) -> tuple[str, ...]:
    if ":" not in range_ref:
        return (range_ref,)
    start, end = range_ref.split(":", 1)
    start_match = _CELL_RE.fullmatch(start)
    end_match = _CELL_RE.fullmatch(end)
    if start_match is None or end_match is None:
        raise OperatingDriverCrossTickerWorkbookError(f"Invalid A1 range {range_ref!r}.")
    start_column, start_row = start_match.groups()
    end_column, end_row = end_match.groups()
    return tuple(
        f"{_column_name(column)}{row}"
        for row in range(int(start_row), int(end_row) + 1)
        for column in range(_column_index(start_column), _column_index(end_column) + 1)
    )


def _style(role: str, investment_case_sheet: str) -> tuple[str, str]:
    styles = {
        # All visible major product sections use Valuation's accepted dark-blue
        # investor hierarchy, including the sheet title and Driver Guide.
        "title": ("Valuation", "A137"),
        "subtitle": ("Promise_Progress_UI", "A2"),
        # Core Drivers, Quarterly Driver History, and the three investor-facing
        # Overview headings all use the model's true major-section band.
        "section": ("Valuation", "A122"),
        "major_section": ("Valuation", "A137"),
        "overview": ("Valuation", "S138"),
        "subsection": ("Valuation", "A7"),
        "header_left": ("Valuation", "O8"),
        "header_right": ("Valuation", "F138"),
        "body_left": (investment_case_sheet, "A7"),
        "body_left_wrap": (investment_case_sheet, "B7"),
        # Valuation!F139 is the accepted neutral, right-aligned investor-table
        # body style in both protected workbooks.  BS_Segments!D13 is not a
        # portable oracle: in PBI it carries an orange input-like fill.
        "body_right": ("Valuation", "F139"),
        "body_right_latest": ("Valuation", "F139"),
        # The guide is an investor-reference surface, not another data table.
        # These portable Valuation oracles retain the model font and light
        # structural border while keeping the body white.
        "guide_body_left": ("Valuation", "B139"),
        "guide_body_wrap": ("Valuation", "H139"),
        # Core category bands reuse the model's accepted light-blue subsection
        # role, while Core labels and explanations use the neutral white
        # investor-reference body styles.
        "core_group": ("Valuation", "A7"),
        "core_body_left": ("Valuation", "B139"),
        "core_body_wrap": ("Valuation", "H139"),
        # History groups use the same portable Valuation subsection role.
        # History body values start from the existing right-aligned style and
        # are made borderless in the bounded target-only patch below so smart
        # number formats and numeric cell typing remain intact.
        "group": ("Valuation", "A7"),
        "history_body_left": ("Valuation", "A9"),
        "history_body_right": ("Valuation", "F139"),
        "note": ("Promise_Progress_UI", "A2"),
        "note_wrap": ("Promise_Progress_UI", "K71"),
    }
    return styles[role]


def _smart_format(value: Decimal, whole: str, decimal: str) -> str:
    return whole if value == value.to_integral_value() else decimal


def _numeric_value(
    value: Decimal,
    unit: str,
    *,
    comparison: bool,
) -> tuple[str, str]:
    if unit == "percent":
        if comparison:
            return format(value, "f"), _smart_format(
                value, '+0" pp";-0" pp";0" pp"', '+0.0" pp";-0.0" pp";0" pp"'
            )
        return format(value / Decimal("100"), "f"), _smart_format(
            value, "+0%;-0%;0%", "+0.0%;-0.0%;0%"
        )
    formats = {
        "billion_pieces": ('0.0"bn pieces"', '+0.0"bn";-0.0"bn";0.0"bn"'),
        "million_gallons": ('#,##0.0"m gal"', '+#,##0.0"m gal";-#,##0.0"m gal";0.0"m gal"'),
        "million_bushels": ('#,##0.0"m bu"', '+#,##0.0"m bu";-#,##0.0"m bu";0.0"m bu"'),
        "million_pounds": ('#,##0.0"m lbs"', '+#,##0.0"m lbs";-#,##0.0"m lbs";0.0"m lbs"'),
        "thousand_tons": ('#,##0"k tons"', '+#,##0"k tons";-#,##0"k tons";0"k tons"'),
        "usd_million": ('"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"', '+"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"'),
    }
    level, delta = formats.get(unit, ("#,##0.0", "+#,##0.0;-#,##0.0;0.0"))
    return format(value, "f"), delta if comparison else level


def _history_numeric_value(value: Decimal, unit: str) -> tuple[str, str]:
    if unit == "percent":
        return format(value / Decimal("100"), "f"), _smart_format(value, "0%", "0.0%")
    formats = {
        "billion_pieces": "0.0",
        "million_gallons": "#,##0.0",
        "million_bushels": "#,##0.0",
        "million_pounds": "#,##0.0",
        "thousand_tons": "#,##0",
        "usd_million": '"$"#,##0.0;-"$"#,##0.0;"$"0.0',
    }
    return format(value, "f"), formats.get(unit, "#,##0.0")


def _adaptive_guide_row_height(*, term: str, meaning: str, economic_role: str) -> float:
    """Return a bounded investor-reference height from visible text density.

    The thresholds correspond to the fixed A:C, D:H, and I:P guide spans.
    They keep concise rows compact while allowing one extra wrapped line for
    longer definitions without changing the accepted column widths.
    """

    def estimated_lines(text: str, capacity: int) -> int:
        return max(1, (len(text.strip()) + capacity - 1) // capacity)

    lines = max(
        estimated_lines(term, 34),
        estimated_lines(meaning, 62),
        estimated_lines(economic_role, 92),
    )
    if lines <= 1:
        return 30.0
    if lines == 2:
        return 36.0
    return 48.0


def build_cross_ticker_workbook_plan(
    package: CrossTickerOperatingDriverPackage,
    *,
    investment_case_sheet: str,
) -> CrossTickerWorkbookPlan:
    cells: list[FormulaAwareCellMutation] = []
    merges: list[WorksheetMergeMutation] = []
    bindings: list[CrossTickerWorkbookBinding] = []
    exact_numeric: list[str] = []
    missing: list[str] = []
    display_number_formats: dict[str, str] = {}

    def bind(
        semantic_id: str,
        element_type: str,
        range_ref: str,
        value: str,
        role: str,
        *,
        value_kind: str = "text",
        number_format_code: str | None = None,
        sources: Sequence[str] = (),
        merge: bool = True,
    ) -> None:
        coordinates = _range_coordinates(range_ref)
        source_sheet, source_cell = _style(role, investment_case_sheet)
        for index, coordinate in enumerate(coordinates):
            cells.append(
                FormulaAwareCellMutation(
                    target_sheet=SHEET_NAME,
                    target_cell=coordinate,
                    mode="SET_VALUE",
                    value=value if index == 0 else "",
                    value_kind=value_kind if index == 0 else "text",
                    # The accepted materializer only accepts registered oracle
                    # formats. Stage numeric cells with a registered neutral
                    # format, then apply the authorized investor format in the
                    # bounded target-only patch below.
                    number_format_code="0.000" if index == 0 and value_kind == "number" else None,
                    style_source_cell=source_cell,
                    style_source_sheet=source_sheet,
                    semantic_owner="operating_drivers_presentation",
                )
            )
        if value_kind == "number":
            exact_numeric.append(coordinates[0])
            if number_format_code is None:
                raise OperatingDriverCrossTickerWorkbookError("Numeric bindings require a display format.")
            display_number_formats[coordinates[0]] = number_format_code
        if element_type.endswith("MISSING"):
            missing.append(coordinates[0])
        if merge and ":" in range_ref:
            merges.append(WorksheetMergeMutation(SHEET_NAME, range_ref, "ADD"))
        bindings.append(
            CrossTickerWorkbookBinding(
                semantic_id=semantic_id,
                element_type=element_type,
                target_range=range_ref,
                value_kind=value_kind,
                source_references=tuple(sources),
            )
        )

    bind("title", "TITLE", "A1:P1", "Operating Drivers", "title")
    bind(
        "subtitle", "SUBTITLE", "A2:P2",
        f"The operating metrics beneath {package.company_name}'s reported financial results", "subtitle",
    )
    bind("section-overview", "MAJOR_SECTION", "A3:P3", "Operating Drivers Overview", "section")

    row = 4
    overview_rows: list[int] = []
    for subsection in ("OPERATING INTERPRETATION", "LATEST QUARTER", "BROADER TREND"):
        label = subsection if subsection != "LATEST QUARTER" else f"LATEST QUARTER — {package.latest_period_label}"
        bind(f"overview:{subsection}:header", "OVERVIEW_SUBSECTION", f"A{row}:P{row}", label, "major_section")
        row += 1
        statements = [item for item in package.overview if item.subsection == subsection]
        for statement in statements:
            bind(
                statement.statement_id, "OVERVIEW_STATEMENT", f"A{row}:P{row}",
                "• " + statement.text, "overview", sources=statement.source_references,
            )
            overview_rows.append(row)
            row += 1
        row += 1

    # Reuse the existing blank row immediately above Core Drivers for the
    # explanatory note. This moves the note without shifting the accepted
    # Overview/Core/History coordinates.
    footnote_row = row - 1
    bind(
        "history-note", "FOOTNOTE", f"A{footnote_row}:P{footnote_row}",
        "pp = percentage points. Text entries preserve qualitative source precision; blank cells mean compatible evidence is unavailable.",
        "note",
    )

    core_section_row = row
    bind("section-core", "MAJOR_SECTION", f"A{row}:P{row}", "Core Drivers", "major_section")
    row += 1
    headers = (
        ("A:D", "Metric", "header_left"),
        ("E:F", f"Latest ({package.latest_period_label})", "header_left"),
        ("G:H", "vs prior quarter", "header_left"),
        ("I:J", "vs year ago", "header_left"),
        ("K:L", "Broader trend", "header_left"),
        ("M:P", "Why it matters", "header_left"),
    )
    core_header_row = row
    for index, (span, label, role) in enumerate(headers):
        start, end = span.split(":")
        bind(f"core-header-{index}", "CORE_HEADER", f"{start}{row}:{end}{row}", label, role)
    row += 1

    core_group_rows: dict[str, int] = {}
    core_metric_rows: dict[str, int] = {}
    previous_group: str | None = None
    for item in package.core_drivers:
        if item.group_label != previous_group:
            core_group_rows[item.group_label] = row
            bind(f"core-group:{item.group_label}", "CORE_GROUP", f"A{row}:D{row}", item.group_label, "core_group")
            bind(f"core-group-band:{item.group_label}", "CORE_GROUP_BAND", f"E{row}:P{row}", "", "core_group", merge=False)
            row += 1
            previous_group = item.group_label
        core_metric_rows[item.core_id] = row
        bind(f"core:{item.core_id}:label", "CORE_LABEL", f"A{row}:D{row}", item.label, "core_body_left", sources=item.source_references)
        for suffix, target, raw, display, comparison, status in (
            ("latest", f"E{row}:F{row}", item.latest_value, item.latest_display, False, "AVAILABLE"),
            ("qoq", f"G{row}:H{row}", item.qoq_value, item.qoq_display, True, item.qoq_status),
            ("yoy", f"I{row}:J{row}", item.yoy_value, item.yoy_display, True, item.yoy_status),
        ):
            if raw is None:
                bind(
                    f"core:{item.core_id}:{suffix}",
                    "CORE_MISSING" if not display else "CORE_TEXT",
                    target, display, "core_body_left", sources=item.source_references,
                )
            else:
                numeric, number_format = _numeric_value(raw, item.unit, comparison=comparison)
                bind(
                    f"core:{item.core_id}:{suffix}", "CORE_NUMERIC", target, numeric, "core_body_left",
                    value_kind="number", number_format_code=number_format, sources=item.source_references,
                )
        bind(f"core:{item.core_id}:trend", "CORE_TREND", f"K{row}:L{row}", item.broader_trend, "core_body_left")
        bind(f"core:{item.core_id}:why", "CORE_WHY", f"M{row}:P{row}", item.why_it_matters, "core_body_wrap")
        row += 1

    row += 1
    history_section_row = row
    bind("section-history", "MAJOR_SECTION", f"A{row}:P{row}", "Quarterly Driver History", "major_section")
    row += 1
    history_header_row = row
    bind("history-header-metric", "HISTORY_HEADER", f"A{row}:D{row}", "Metric", "header_left")
    for column, period_label in zip("EFGHIJKLMNOP", package.quarter_labels, strict=True):
        bind(f"history-header:{period_label}", "HISTORY_HEADER", f"{column}{row}", period_label, "header_right", merge=False)
    row += 1

    history_group_rows: dict[str, int] = {}
    history_metric_rows: dict[str, int] = {}
    previous_group = None
    for item in package.history_rows:
        if item.group_label != previous_group:
            history_group_rows[item.group_label] = row
            bind(f"history-group:{item.group_label}", "HISTORY_GROUP", f"A{row}:D{row}", item.group_label, "group")
            bind(f"history-group-band:{item.group_label}", "HISTORY_GROUP_BAND", f"E{row}:P{row}", "", "group", merge=False)
            row += 1
            previous_group = item.group_label
        history_metric_rows[item.driver_id] = row
        bind(f"history:{item.driver_id}:label", "HISTORY_LABEL", f"A{row}:D{row}", item.label, "history_body_left")
        for column, point in zip("EFGHIJKLMNOP", item.points, strict=True):
            target = f"{column}{row}"
            role = "history_body_right"
            if point.value is not None and point.status == "AVAILABLE" and point.precision == "EXACT":
                numeric, number_format = _history_numeric_value(point.value, item.unit)
                bind(
                    f"history:{item.driver_id}:{point.period_label}", "HISTORY_NUMERIC", target, numeric, role,
                    value_kind="number", number_format_code=number_format, sources=point.source_ids, merge=False,
                )
            elif point.display_value:
                bind(
                    f"history:{item.driver_id}:{point.period_label}", "HISTORY_TEXT", target,
                    point.display_value, role, sources=point.source_ids, merge=False,
                )
            else:
                bind(
                    f"history:{item.driver_id}:{point.period_label}", "HISTORY_MISSING", target,
                    "", role, merge=False,
                )
        row += 1

    guide_rows: dict[str, int] = {}
    if package.guide_terms:
        # Retain one normal spacer row between History and Driver Guide after
        # relocating the explanatory note above Core Drivers.
        row += 1
        bind("guide-section", "GUIDE_SECTION", f"A{row}:P{row}", "Driver Guide", "major_section")
        guide_section_row = row
        row += 1
        bind("guide-header-term", "GUIDE_HEADER", f"A{row}:C{row}", "Term", "header_left")
        bind("guide-header-meaning", "GUIDE_HEADER", f"D{row}:H{row}", "What it means", "header_left")
        bind("guide-header-role", "GUIDE_HEADER", f"I{row}:P{row}", "Economic role", "header_left")
        guide_header_row = row
        row += 1
        for item in package.guide_terms:
            guide_rows[item.term] = row
            bind(f"guide:{item.term}:term", "GUIDE_TERM", f"A{row}:C{row}", item.term, "guide_body_left", sources=item.source_references)
            bind(f"guide:{item.term}:meaning", "GUIDE_MEANING", f"D{row}:H{row}", item.meaning, "guide_body_wrap", sources=item.source_references)
            bind(f"guide:{item.term}:role", "GUIDE_ROLE", f"I{row}:P{row}", item.economic_role, "guide_body_wrap", sources=item.source_references)
            row += 1
    else:
        guide_section_row = guide_header_row = None
    final_row = row - 1
    used_range = f"A1:P{final_row}"

    row_heights: dict[int, float] = {
        1: 26.0,
        2: 22.0,
        3: 22.0,
        core_section_row: 26.0,
        core_header_row: 22.0,
        history_section_row: 26.0,
        history_header_row: 22.0,
        footnote_row: 19.5,
    }
    for item in overview_rows:
        row_heights[item] = 36.0
    for binding in bindings:
        if binding.element_type == "OVERVIEW_SUBSECTION":
            row_heights[int(re.search(r"\d+", binding.target_range).group(0))] = 26.0  # type: ignore[union-attr]
    for item in core_group_rows.values():
        row_heights[item] = 22.0
    for item in core_metric_rows.values():
        row_heights[item] = 19.5
    for item in history_group_rows.values():
        row_heights[item] = 22.0
    for item in history_metric_rows.values():
        row_heights[item] = 19.5
    if guide_section_row is not None and guide_header_row is not None:
        row_heights[guide_section_row] = 26.0
        row_heights[guide_header_row] = 22.0
    for item in package.guide_terms:
        row_heights[guide_rows[item.term]] = _adaptive_guide_row_height(
            term=item.term,
            meaning=item.meaning,
            economic_role=item.economic_role,
        )
    row_mutations = tuple(
        WorksheetRowMutation(SHEET_NAME, row_number, height=height)
        for row_number, height in sorted(row_heights.items())
    )
    widths = {1: 25.0, 2: 8.0, 3: 8.0, 4: 8.0, **{index: 15.4 for index in range(5, 17)}}
    column_mutations = tuple(
        WorksheetColumnMutation(SHEET_NAME, column, width)
        for column, width in sorted(widths.items())
    )
    payload = {
        "contract_version": WORKBOOK_CONTRACT,
        "ticker": package.ticker,
        "sheet_name": SHEET_NAME,
        "used_range": used_range,
        "zoom_scale": ZOOM_SCALE,
        "cells": [asdict(item) for item in cells],
        "merges": [asdict(item) for item in merges],
        "rows": [asdict(item) for item in row_mutations],
        "columns": [asdict(item) for item in column_mutations],
        "bindings": [asdict(item) for item in bindings],
        "display_number_formats": dict(sorted(display_number_formats.items())),
    }
    return CrossTickerWorkbookPlan(
        contract_version=WORKBOOK_CONTRACT,
        ticker=package.ticker,
        sheet_name=SHEET_NAME,
        used_range=used_range,
        zoom_scale=ZOOM_SCALE,
        cell_mutations=tuple(cells),
        merge_mutations=tuple(merges),
        row_mutations=row_mutations,
        column_mutations=column_mutations,
        dimension_mutation=WorksheetDimensionMutation(SHEET_NAME, used_range),
        bindings=tuple(bindings),
        display_number_formats=dict(sorted(display_number_formats.items())),
        exact_numeric_coordinates=tuple(sorted(exact_numeric)),
        missing_coordinates=tuple(sorted(missing)),
        major_section_rows={
            "Operating Drivers Overview": 3,
            "Core Drivers": core_section_row,
            "Quarterly Driver History": history_section_row,
        },
        core_group_rows=core_group_rows,
        core_metric_rows=core_metric_rows,
        history_group_rows=history_group_rows,
        history_metric_rows=history_metric_rows,
        guide_rows=guide_rows,
        plan_sha256=_digest(payload),
    )


def _visible_snapshot(workbook: Path) -> dict[str, Any]:
    ns = {"m": _MAIN_NS}
    with ZipFile(workbook, "r") as archive:
        part = _sheet_part_map(archive)[SHEET_NAME]
        root = ET.fromstring(archive.read(part))
    cells = []
    for cell in root.findall(".//m:sheetData/m:row/m:c", ns):
        inline = cell.find("m:is", ns)
        value = cell.find("m:v", ns)
        formula = cell.find("m:f", ns)
        text = None if inline is None else "".join(node.text or "" for node in inline.findall(".//m:t", ns))
        cells.append(
            {
                "coordinate": cell.attrib["r"],
                "formula": None if formula is None else formula.text,
                "style_id": int(cell.attrib.get("s", "0")),
                "type": cell.attrib.get("t"),
                "value": text if text is not None else (None if value is None else value.text),
            }
        )
    return {
        "semantic_hash_contract": SEMANTIC_HASH_CONTRACT,
        "cells": sorted(cells, key=lambda item: item["coordinate"]),
        "merges": sorted(item.attrib["ref"] for item in root.findall("m:mergeCells/m:mergeCell", ns)),
        "dimension": root.find("m:dimension", ns).attrib["ref"],  # type: ignore[union-attr]
        "rows": [dict(item.attrib) for item in root.findall("m:sheetData/m:row", ns)],
        "columns": [dict(item.attrib) for item in root.findall("m:cols/m:col", ns)],
    }


def cross_ticker_workbook_semantic_sha256(workbook: Path | str) -> str:
    return _digest(_visible_snapshot(Path(workbook)))


def _patch_history_body_neutral(
    members: dict[str, bytes],
    *,
    sheet_parts: Mapping[str, str],
    target_part: str,
    plan: CrossTickerWorkbookPlan,
) -> int:
    """Use a white history body with one subtle full-width row separator.

    The presentation contract uses full-width light-blue category bands and a
    white evidence body.  A single thin bottom border in an existing workbook
    palette color improves sparse-history scanability without bringing back an
    every-cell grid.  Cloning each final target style and replacing only
    ``fillId`` and ``borderId`` preserves right alignment, qualitative text
    handling, and the smart number formats already applied.
    """

    valuation_part = sheet_parts.get("Valuation")
    if valuation_part is None:
        raise OperatingDriverCrossTickerWorkbookError("Valuation sheet is missing.")
    styles_root = ET.fromstring(members["xl/styles.xml"])
    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    borders = styles_root.find(f"{{{_MAIN_NS}}}borders")
    if cell_xfs is None or borders is None:
        raise OperatingDriverCrossTickerWorkbookError(
            "Workbook styles are missing cellXfs or borders."
        )
    xfs = list(cell_xfs)
    valuation_root = ET.fromstring(members[valuation_part])
    source = valuation_root.find(f".//{{{_MAIN_NS}}}c[@r='A9']")
    if source is None:
        raise OperatingDriverCrossTickerWorkbookError("Neutral history-body oracle Valuation!A9 is absent.")
    source_style = int(source.attrib.get("s", "0"))
    neutral_fill = int(xfs[source_style].attrib.get("fillId", "0"))

    # D9E2EF is already present in the protected workbook palette.  The
    # separator deliberately has no left, right, or top edge, so the History
    # remains a research table rather than a spreadsheet grid.
    separator_border = ET.Element(f"{{{_MAIN_NS}}}border")
    ET.SubElement(separator_border, f"{{{_MAIN_NS}}}left")
    ET.SubElement(separator_border, f"{{{_MAIN_NS}}}right")
    ET.SubElement(separator_border, f"{{{_MAIN_NS}}}top")
    bottom = ET.SubElement(
        separator_border,
        f"{{{_MAIN_NS}}}bottom",
        {"style": "thin"},
    )
    ET.SubElement(bottom, f"{{{_MAIN_NS}}}color", {"rgb": "FFD9E2EF"})
    ET.SubElement(separator_border, f"{{{_MAIN_NS}}}diagonal")
    neutral_border = len(list(borders))
    borders.append(separator_border)
    borders.attrib["count"] = str(len(list(borders)))

    target_root = ET.fromstring(members[target_part])
    style_cache: dict[tuple[int, int, int], int] = {}
    patched = 0
    for row in sorted(plan.history_metric_rows.values()):
        for column in "ABCDEFGHIJKLMNOP":
            coordinate = f"{column}{row}"
            cell = target_root.find(f".//{{{_MAIN_NS}}}c[@r='{coordinate}']")
            if cell is None:
                raise OperatingDriverCrossTickerWorkbookError(
                    f"Neutral history-body target is absent: {coordinate}."
                )
            base_style = int(cell.attrib.get("s", "0"))
            key = (base_style, neutral_fill, neutral_border)
            style_id = style_cache.get(key)
            if style_id is None:
                clone = deepcopy(xfs[base_style])
                clone.attrib["fillId"] = str(neutral_fill)
                clone.attrib["borderId"] = str(neutral_border)
                clone.attrib["applyFill"] = "1"
                clone.attrib["applyBorder"] = "1"
                cell_xfs.append(clone)
                style_id = len(xfs) + len(style_cache)
                style_cache[key] = style_id
            cell.attrib["s"] = str(style_id)
            patched += 1

    cell_xfs.attrib["count"] = str(len(list(cell_xfs)))
    ET.register_namespace("", _MAIN_NS)
    members["xl/styles.xml"] = ET.tostring(
        styles_root, encoding="utf-8", xml_declaration=True
    )
    members[target_part] = ET.tostring(
        target_root, encoding="utf-8", xml_declaration=True
    )
    return patched


def materialize_cross_ticker_operating_driver_workbook(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    plan: CrossTickerWorkbookPlan,
    expected_base_sha256: str,
) -> CrossTickerWorkbookResult:
    base = Path(base_workbook)
    output = Path(output_workbook)
    if output.exists():
        raise OperatingDriverCrossTickerWorkbookError(f"Refusing to overwrite {output}.")
    if sha256_file(base) != expected_base_sha256.lower():
        raise OperatingDriverCrossTickerWorkbookError("Protected workbook identity changed.")
    output.parent.mkdir(parents=True, exist_ok=True)
    max_row = int(plan.used_range.rsplit("P", 1)[-1])
    with TemporaryDirectory(prefix="operating_drivers_cross_ticker_", dir=output.parent) as temporary:
        blank = Path(temporary) / f"blank{base.suffix}"
        intermediate = Path(temporary) / f"materialized{base.suffix}"
        sheet_part, comments_removed, support_parts = _prepare_blank_surface(
            base, blank, max_row=max_row, zoom_scale=plan.zoom_scale
        )
        materialize_capital_return_debt_mutations(
            base_workbook=blank,
            output_workbook=intermediate,
            cell_mutations=plan.cell_mutations,
            merge_mutations=plan.merge_mutations,
            row_mutations=plan.row_mutations,
            column_mutations=plan.column_mutations,
            dimension_mutations=(plan.dimension_mutation,),
            expected_base_sha256=sha256_file(blank),
        )
        with ZipFile(intermediate, "r") as archive:
            members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
            sheet_parts = _sheet_part_map(archive)
            target_part = sheet_parts[SHEET_NAME]
        if target_part != sheet_part:
            raise OperatingDriverCrossTickerWorkbookError("Operating_Drivers sheet identity changed.")
        _patch_smart_precision(members, target_part, plan)  # type: ignore[arg-type]
        _patch_history_body_neutral(
            members,
            sheet_parts=sheet_parts,
            target_part=target_part,
            plan=plan,
        )
        _write_package(base_workbook=intermediate, output_workbook=output, members=members)

    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if before.namelist() != after.namelist():
            raise OperatingDriverCrossTickerWorkbookError("Workbook member inventory changed.")
        before_members = {name: before.read(name) for name in before.namelist()}
        after_members = {name: after.read(name) for name in after.namelist()}
    changed = tuple(sorted(name for name in before_members if before_members[name] != after_members[name]))
    allowed = tuple(sorted({sheet_part, "xl/styles.xml", *support_parts}))
    unexpected = tuple(sorted(set(changed) - set(allowed)))
    snapshot = _visible_snapshot(output)
    by_coordinate = {item["coordinate"]: item for item in snapshot["cells"]}
    formula_count = sum(item["formula"] is not None for item in snapshot["cells"])
    numeric_as_text = sum(
        coordinate not in by_coordinate or by_coordinate[coordinate]["type"] != "n"
        for coordinate in plan.exact_numeric_coordinates
    )
    missing_to_zero = sum(
        coordinate in by_coordinate
        and by_coordinate[coordinate]["type"] == "n"
        and Decimal(str(by_coordinate[coordinate]["value"] or "0")) == 0
        for coordinate in plan.missing_coordinates
    )
    before_vba = before_members.get("xl/vbaProject.bin")
    after_vba = after_members.get("xl/vbaProject.bin")
    before_vba_hash = None if before_vba is None else hashlib.sha256(before_vba).hexdigest()
    after_vba_hash = None if after_vba is None else hashlib.sha256(after_vba).hexdigest()
    return CrossTickerWorkbookResult(
        ticker=plan.ticker,
        base_workbook_sha256=sha256_file(base),
        output_workbook_sha256=sha256_file(output),
        semantic_workbook_sha256=cross_ticker_workbook_semantic_sha256(output),
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        canonical_ooxml_contract=CANONICAL_OOXML_HASH_CONTRACT,
        changed_ooxml_parts=changed,
        allowed_changed_ooxml_parts=allowed,
        unrelated_workbook_delta_count=len(unexpected),
        unchanged_ooxml_part_count=len(before_members) - len(changed),
        target_formula_count=formula_count,
        exact_numeric_cell_count=len(plan.exact_numeric_coordinates),
        exact_numeric_stored_as_text_count=numeric_as_text,
        missing_to_zero_count=missing_to_zero,
        comments_removed_count=comments_removed,
        vba_sha256_before=before_vba_hash,
        vba_sha256_after=after_vba_hash,
        vba_delta_count=int(before_vba_hash != after_vba_hash),
        plan_sha256=plan.plan_sha256,
    )


__all__ = [
    "CrossTickerWorkbookPlan",
    "CrossTickerWorkbookResult",
    "OperatingDriverCrossTickerWorkbookError",
    "SEMANTIC_HASH_CONTRACT",
    "WORKBOOK_CONTRACT",
    "ZOOM_SCALE",
    "build_cross_ticker_workbook_plan",
    "cross_ticker_workbook_semantic_sha256",
    "materialize_cross_ticker_operating_driver_workbook",
]
