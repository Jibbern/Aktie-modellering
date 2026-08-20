"""Lossless blank-surface workbook adapter for the ANF Operating Drivers V4 UI.

The adapter owns coordinates and presentation only.  It replaces the rejected
visible Operating_Drivers canvas in an isolated workbook package, then applies
the new plan through the accepted targeted OOXML materializer.  No workbook
formula creates or owns Operating Drivers economics.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from copy import deepcopy
from decimal import Decimal
from html import escape
import hashlib
import json
from pathlib import Path, PurePosixPath
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
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (
    OperatingDriverAnfUIV4Package,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _sheet_part_map,
    _write_package,
    canonical_ooxml_sha256,
    sha256_file,
)


WORKBOOK_CONTRACT = "operating-drivers-anf-blank-surface-workbook-v4@7"
WORKBOOK_SEMANTIC_HASH_CONTRACT = "operating-drivers-anf-visible-snapshot-sha256@10"
SHEET_NAME = "Operating_Drivers"
USED_RANGE = "A1:P61"
ZOOM_SCALE = 110

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_X14_NS = "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main"
_XM_NS = "http://schemas.microsoft.com/office/excel/2006/main"
_SPARKLINE_EXTENSION_URI = "{05C60535-1F16-4fd2-B633-F4F36F0B64E0}"
_COMMENT_LIST_RE = re.compile(rb"<commentList>.*?</commentList>", re.DOTALL)
_NOTE_SHAPE_RE = re.compile(rb"<v:shape\b.*?</v:shape>", re.DOTALL)
_CELL_RE = re.compile(r"([A-Z]+)([1-9][0-9]*)")


class OperatingDriverAnfWorkbookV4Error(ValueError):
    """Raised when the blank-surface workbook contract cannot be proven."""


def _digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class WorkbookBinding:
    semantic_id: str
    element_type: str
    target_range: str
    display_value: str
    source_references: tuple[str, ...] = ()


@dataclass(frozen=True)
class SparklineRecord:
    semantic_id: str
    target_cell: str
    source_range: str
    history_point_count: int
    display_empty_cells_as: str = "gap"
    line_color: str = "FF4472C4"


@dataclass(frozen=True)
class OperatingDriverAnfWorkbookV4Plan:
    contract_version: str
    plan_origin: str
    sheet_name: str
    used_range: str
    zoom_scale: int
    cell_mutations: tuple[FormulaAwareCellMutation, ...]
    merge_mutations: tuple[WorksheetMergeMutation, ...]
    row_mutations: tuple[WorksheetRowMutation, ...]
    column_mutations: tuple[WorksheetColumnMutation, ...]
    dimension_mutation: WorksheetDimensionMutation
    display_number_formats: Mapping[str, str]
    bindings: tuple[WorkbookBinding, ...]
    sparkline_records: tuple[SparklineRecord, ...]
    major_section_rows: Mapping[str, int]
    history_group_rows: Mapping[str, int]
    history_metric_rows: Mapping[str, int]
    core_group_rows: Mapping[str, int]
    core_metric_rows: Mapping[str, int]
    footprint_definition_rows: Mapping[str, int]
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
class OperatingDriverAnfWorkbookV4Result:
    base_workbook_sha256: str
    output_workbook_sha256: str
    semantic_workbook_sha256: str
    canonical_ooxml_sha256: str
    canonical_ooxml_contract: str
    changed_ooxml_parts: tuple[str, ...]
    allowed_changed_ooxml_parts: tuple[str, ...]
    unrelated_workbook_delta_count: int
    unchanged_ooxml_part_count: int
    comments_removed_count: int
    target_formula_count: int
    missing_to_zero_count: int
    sparkline_count: int
    sparkline_readback_mismatch_count: int
    full_range_style_mismatch_count: int
    latest_quarter_emphasis_cell_count: int
    smart_precision_cell_count: int
    vba_sha256_before: str | None
    vba_sha256_after: str | None
    plan_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _style(role: str) -> tuple[str, str]:
    styles = {
        "title": ("Promise_Progress_UI", "A1"),
        "subtitle": ("Promise_Progress_UI", "A2"),
        "section": ("Valuation", "A122"),
        "overview": ("Valuation", "S138"),
        "subsection": ("Valuation", "A7"),
        "header_left": ("Valuation", "O8"),
        "header_right": ("Valuation", "F138"),
        "body_left": ("ANF_Investment_Case", "A7"),
        "body_left_wrap": ("ANF_Investment_Case", "B7"),
        "body_right": ("BS_Segments", "D13"),
        "group": ("ANF_Investment_Case", "A39"),
        "note": ("Promise_Progress_UI", "A2"),
        "note_wrap": ("Promise_Progress_UI", "K71"),
    }
    return styles[role]


def _cell(
    coordinate: str,
    value: str,
    role: str,
    *,
    number_format_code: str | None = None,
    value_kind: str = "text",
    semantic_owner: str = "operating_drivers_v4_presentation",
) -> FormulaAwareCellMutation:
    source_sheet, source_cell = _style(role)
    return FormulaAwareCellMutation(
        target_sheet=SHEET_NAME,
        target_cell=coordinate,
        mode="SET_VALUE",
        value=value,
        value_kind=value_kind,  # type: ignore[arg-type]
        number_format_code=number_format_code,
        style_source_cell=source_cell,
        style_source_sheet=source_sheet,
        semantic_owner=semantic_owner,
    )


def _merge(range_ref: str) -> WorksheetMergeMutation:
    return WorksheetMergeMutation(SHEET_NAME, range_ref, "ADD")


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
        raise OperatingDriverAnfWorkbookV4Error(f"Invalid A1 range: {range_ref}.")
    start_column, start_row = start_match.groups()
    end_column, end_row = end_match.groups()
    return tuple(
        f"{_column_name(column)}{row}"
        for row in range(int(start_row), int(end_row) + 1)
        for column in range(_column_index(start_column), _column_index(end_column) + 1)
    )


def _numeric_history_value(raw: str, unit_id: str) -> tuple[str, str | None]:
    value = Decimal(raw)
    if unit_id == "unit:core:percent@1":
        number_format = "0%" if value == value.to_integral_value() else "0.0%"
        return format(value / Decimal("100"), "f"), number_format
    if unit_id == "unit:operating-driver:stores@1":
        return format(value, "f"), "#,##0"
    if unit_id == "unit:core:usd-million@1":
        return format(value, "f"), "#,##0.0"
    return format(value, "f"), "#,##0.#"


def _smart_format(value: Decimal, whole: str, decimal: str) -> str:
    return whole if value == value.to_integral_value() else decimal


def _numeric_core_value(
    raw: str,
    unit_id: str,
    *,
    comparison: bool,
) -> tuple[str, str]:
    """Serialize exact Core values numerically while preserving investor display."""

    value = Decimal(raw)
    if unit_id == "unit:core:percent@1":
        if comparison:
            return format(value, "f"), _smart_format(
                value,
                '+0" pp";-0" pp";0" pp"',
                '+0.0" pp";-0.0" pp";0" pp"',
            )
        return format(value / Decimal("100"), "f"), _smart_format(
            value,
            "+0%;-0%;0%",
            "+0.0%;-0.0%;0%",
        )
    if unit_id == "unit:operating-driver:stores@1":
        return format(value, "f"), (
            '+#,##0" stores";-#,##0" stores";0" stores"'
            if comparison
            else '#,##0" stores"'
        )
    if unit_id == "unit:core:usd-million@1":
        return format(value, "f"), (
            '+"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"'
            if comparison
            else '"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"'
        )
    return format(value, "f"), (
        "+#,##0.0;-#,##0.0;0.0" if comparison else "#,##0.0"
    )


def build_operating_driver_anf_workbook_v4_plan(
    package: OperatingDriverAnfUIV4Package,
) -> OperatingDriverAnfWorkbookV4Plan:
    if package.ticker != "ANF" or package.plan_origin != "BLANK_SURFACE_V4":
        raise OperatingDriverAnfWorkbookV4Error("The V4 workbook path accepts only the ANF blank-surface package.")

    cells: list[FormulaAwareCellMutation] = []
    merges: list[WorksheetMergeMutation] = []
    display_number_formats: dict[str, str] = {}
    bindings: list[WorkbookBinding] = []
    sparklines: list[SparklineRecord] = []

    def bind(
        semantic_id: str,
        element_type: str,
        range_ref: str,
        value: str,
        role: str,
        *,
        sources: Sequence[str] = (),
        number_format_code: str | None = None,
        value_kind: str = "text",
        merge: bool = True,
    ) -> None:
        coordinates = _range_coordinates(range_ref)
        if number_format_code is not None and value_kind == "number":
            display_number_formats[coordinates[0]] = number_format_code
        for index, coordinate in enumerate(coordinates):
            cells.append(
                _cell(
                    coordinate,
                    value if index == 0 else "",
                    role,
                    # Use a unique, registered staging format.  The protected
                    # workbook contains duplicated presentation-style variants,
                    # so final investor formats are applied deterministically to
                    # the authorized target cells after generic materialization.
                    number_format_code=(
                        "0.000"
                        if index == 0
                        and number_format_code is not None
                        and value_kind == "number"
                        else None
                    ),
                    value_kind=value_kind if index == 0 else "text",
                )
            )
        if ":" in range_ref and merge:
            merges.append(_merge(range_ref))
        bindings.append(
            WorkbookBinding(
                semantic_id=semantic_id,
                element_type=element_type,
                target_range=range_ref,
                display_value=value,
                source_references=tuple(sources),
            )
        )

    bind("title", "TITLE", "A1:P1", "Operating Drivers", "title")
    bind(
        "subtitle",
        "SUBTITLE",
        "A2:P2",
        "The operating metrics beneath ANF’s reported financial results",
        "subtitle",
    )
    bind("section-overview", "MAJOR_SECTION", "A3:P3", "Operating Drivers Overview", "section")
    interpretation = [item for item in package.overview if item.subsection == "OPERATING INTERPRETATION"]
    latest = [item for item in package.overview if item.subsection == "LATEST QUARTER"]
    broader = [item for item in package.overview if item.subsection == "BROADER TREND"]
    if len(interpretation) != 3 or len(latest) != 4 or len(broader) != 3:
        raise OperatingDriverAnfWorkbookV4Error(
            "V4 overview requires three interpretation, four latest, and three broader statements."
        )
    bind(
        "overview-subsection-interpretation",
        "OVERVIEW_SUBSECTION",
        "A4:P4",
        "OPERATING INTERPRETATION",
        "subsection",
    )
    for row, statement in enumerate(interpretation, start=5):
        bind(
            statement.statement_id,
            "OVERVIEW_STATEMENT",
            f"A{row}:P{row}",
            statement.text,
            "overview",
            sources=statement.source_references,
        )
    bind(
        "overview-subsection-latest",
        "OVERVIEW_SUBSECTION",
        "A8:P8",
        f"LATEST QUARTER — {package.latest_period_label}",
        "subsection",
    )
    for row, statement in enumerate(latest, start=9):
        bind(
            statement.statement_id,
            "OVERVIEW_STATEMENT",
            f"A{row}:P{row}",
            f"• {statement.text}",
            "overview",
            sources=statement.source_references,
        )
    bind(
        "overview-subsection-broader",
        "OVERVIEW_SUBSECTION",
        "A13:P13",
        "BROADER TREND",
        "subsection",
    )
    for row, statement in enumerate(broader, start=14):
        bind(
            statement.statement_id,
            "OVERVIEW_STATEMENT",
            f"A{row}:P{row}",
            f"• {statement.text}",
            "overview",
            sources=statement.source_references,
        )

    bind("section-core", "MAJOR_SECTION", "A18:P18", "Core Drivers", "section")
    headers = (
        ("A19:D19", "Metric", "header_left"),
        ("E19:F19", f"Latest ({package.latest_period_label})", "header_right"),
        ("G19:H19", "vs prior quarter", "header_right"),
        ("I19:J19", "vs year ago", "header_right"),
        ("K19:L19", "Broader trend", "header_right"),
        ("M19:P19", " Why it matters", "header_left"),
    )
    for index, (target, label, role) in enumerate(headers):
        bind(f"core-header-{index}", "CORE_HEADER", target, label, role)

    core_rows: dict[str, int] = {}
    core_group_rows: dict[str, int] = {}
    current_core_row = 20
    previous_core_group: str | None = None
    for item in package.core_drivers:
        if item.group_label != previous_core_group:
            core_group_rows[item.group_label] = current_core_row
            bind(
                f"core-group:{item.group_label}",
                "CORE_GROUP",
                f"A{current_core_row}:P{current_core_row}",
                item.group_label,
                "group",
                merge=False,
            )
            current_core_row += 1
            previous_core_group = item.group_label
        row = current_core_row
        core_rows[item.core_id] = row
        bind(f"core:{item.core_id}:label", "CORE_METRIC_LABEL", f"A{row}:D{row}", item.label, "body_left", sources=item.source_references)
        for semantic_suffix, element_type, target, raw_value, display_value, comparison in (
            ("latest", "CORE_LATEST", f"E{row}:F{row}", item.latest_value, item.latest_display, False),
            ("qoq", "CORE_QOQ", f"G{row}:H{row}", item.qoq_value, item.qoq_display, True),
            ("yoy", "CORE_YOY", f"I{row}:J{row}", item.yoy_value, item.yoy_display, True),
        ):
            if raw_value is None:
                bind(
                    f"core:{item.core_id}:{semantic_suffix}",
                    element_type,
                    target,
                    display_value,
                    "body_right",
                    sources=item.source_references,
                )
            else:
                numeric, number_format = _numeric_core_value(
                    raw_value,
                    item.unit_id,
                    comparison=comparison,
                )
                bind(
                    f"core:{item.core_id}:{semantic_suffix}",
                    element_type,
                    target,
                    numeric,
                    "body_right",
                    sources=item.source_references,
                    number_format_code=number_format,
                    value_kind="number",
                )
        bind(f"core:{item.core_id}:trend", "CORE_TREND", f"K{row}:L{row}", item.trend_fallback_display, "body_right", sources=item.source_references)
        bind(f"core:{item.core_id}:why", "CORE_WHY", f"M{row}:P{row}", f" {item.why_it_matters}", "body_left", sources=item.source_references)
        current_core_row += 1
    if current_core_row != 31:
        raise OperatingDriverAnfWorkbookV4Error(f"Unexpected V4 core row shape: {current_core_row}.")

    bind("section-history", "MAJOR_SECTION", "A32:P32", "Quarterly Driver History", "section")
    bind("history-header-metric", "HISTORY_HEADER", "A33:D33", "Metric", "header_left")
    for column, period_label in zip("EFGHIJKLMNOP", package.quarter_labels, strict=True):
        bind(f"history-header:{period_label}", "HISTORY_QUARTER_HEADER", f"{column}33", period_label, "header_right")

    history_group_rows: dict[str, int] = {}
    history_metric_rows: dict[str, int] = {}
    current_row = 34
    previous_group: str | None = None
    for item in package.history_rows:
        if item.group_label != previous_group:
            history_group_rows[item.group_label] = current_row
            bind(
                f"history-group:{item.group_label}",
                "HISTORY_GROUP",
                f"A{current_row}:P{current_row}",
                item.group_label,
                "group",
                merge=False,
            )
            current_row += 1
            previous_group = item.group_label
        key = f"{item.driver_id}|{item.dimension_member_id}"
        history_metric_rows[key] = current_row
        bind(
            f"history:{key}:label",
            "HISTORY_LABEL",
            f"A{current_row}:D{current_row}",
            item.label,
            "body_left",
        )
        for column, point in zip("EFGHIJKLMNOP", item.points, strict=True):
            coordinate = f"{column}{current_row}"
            if point.value is None:
                if point.display_value:
                    bind(
                        f"history:{key}:{point.period_label}",
                        "HISTORY_APPROXIMATE_TEXT",
                        coordinate,
                        point.display_value,
                        "body_right",
                        sources=tuple(
                            value
                            for value in (
                                point.source_observation_id,
                                point.source_evidence_id,
                                *point.lineage_references,
                            )
                            if value
                        ),
                    )
                    continue
                bind(
                    f"history:{key}:{point.period_label}",
                    "HISTORY_MISSING",
                    coordinate,
                    "",
                    "body_right",
                )
            else:
                numeric, number_format = _numeric_history_value(point.value, item.unit_id)
                bind(
                    f"history:{key}:{point.period_label}",
                    "HISTORY_VALUE",
                    coordinate,
                    numeric,
                    "body_right",
                    sources=tuple(
                        value
                        for value in (
                            point.source_observation_id,
                            point.source_evidence_id,
                            *point.lineage_references,
                        )
                        if value
                    ),
                    number_format_code=number_format,
                    value_kind="number",
                )
        current_row += 1

    if current_row != 52:
        raise OperatingDriverAnfWorkbookV4Error(f"Unexpected V4 history row shape; footer would start at {current_row}.")
    bind(
        "history-note",
        "FOOTNOTE",
        "A52:P52",
        "pp = percentage points. Approximate inventory-unit entries are text; blank cells mean accepted comparable evidence is unavailable.",
        "note",
    )
    bind(
        "footprint-definitions-section",
        "FOOTPRINT_DEFINITION_SECTION",
        "A54:P54",
        "Store Footprint Guide",
        "subsection",
    )
    bind(
        "footprint-definitions-term-header",
        "FOOTPRINT_DEFINITION_HEADER",
        "A55:C55",
        "Term",
        "header_left",
    )
    bind(
        "footprint-definitions-meaning-header",
        "FOOTPRINT_DEFINITION_HEADER",
        "D55:H55",
        "What it means",
        "header_left",
    )
    bind(
        "footprint-definitions-economic-role-header",
        "FOOTPRINT_DEFINITION_HEADER",
        "I55:P55",
        "Economic role",
        "header_left",
    )
    footprint_definition_rows: dict[str, int] = {}
    for row, definition in enumerate(package.footprint_definitions, start=56):
        footprint_definition_rows[definition.term] = row
        bind(
            f"footprint-definition:{definition.driver_id}:term",
            "FOOTPRINT_DEFINITION_TERM",
            f"A{row}:C{row}",
            definition.term,
            "body_left",
            sources=definition.source_references,
        )
        bind(
            f"footprint-definition:{definition.driver_id}:meaning",
            "FOOTPRINT_DEFINITION_MEANING",
            f"D{row}:H{row}",
            definition.meaning,
            "body_left_wrap",
            sources=definition.source_references,
        )
        bind(
            f"footprint-definition:{definition.driver_id}:economic-role",
            "FOOTPRINT_DEFINITION_ECONOMIC_ROLE",
            f"I{row}:P{row}",
            definition.economic_role,
            "body_left_wrap",
            sources=definition.source_references,
        )
    if len(footprint_definition_rows) != 5:
        raise OperatingDriverAnfWorkbookV4Error(
            "Footprint definition support requires exactly five visible terms."
        )
    bind(
        "footprint-store-count-bridge-note",
        "FOOTPRINT_DEFINITION_NOTE",
        "A61:P61",
        package.store_count_roll_forward_note,
        "note_wrap",
        sources=package.store_count_roll_forward_note_sources,
    )

    row_heights = {
        1: 28.0,
        2: 22.0,
        3: 22.0,
        4: 21.0,
        5: 38.0,
        6: 36.0,
        7: 36.0,
        8: 21.0,
        9: 34.0,
        10: 34.0,
        11: 34.0,
        12: 38.0,
        13: 21.0,
        14: 34.0,
        15: 34.0,
        16: 38.0,
        17: 9.75,
        18: 22.0,
        19: 22.0,
        31: 9.75,
        32: 22.0,
        33: 22.0,
        52: 18.0,
        53: 19.5,
        54: 21.0,
        55: 22.0,
        61: 32.0,
    }
    for row in core_group_rows.values():
        row_heights[row] = 21.0
    for row in core_rows.values():
        row_heights[row] = 19.5
    for row in history_group_rows.values():
        row_heights[row] = 21.0
    for row in history_metric_rows.values():
        row_heights[row] = 19.5
    for row in footprint_definition_rows.values():
        row_heights[row] = 38.0
    row_mutations = tuple(
        WorksheetRowMutation(SHEET_NAME, row, height=height)
        for row, height in sorted(row_heights.items())
    )
    widths = {
        1: 25.0,
        2: 8.0,
        3: 8.0,
        4: 8.0,
        5: 15.4,
        6: 15.4,
        7: 15.4,
        8: 15.4,
        9: 15.4,
        10: 15.4,
        11: 15.4,
        12: 15.4,
        13: 15.4,
        14: 15.4,
        15: 15.4,
        16: 15.4,
    }
    column_mutations = tuple(
        WorksheetColumnMutation(SHEET_NAME, column, width)
        for column, width in sorted(widths.items())
    )
    payload = {
        "contract_version": WORKBOOK_CONTRACT,
        "plan_origin": "BLANK_SURFACE_V4",
        "sheet_name": SHEET_NAME,
        "used_range": USED_RANGE,
        "zoom_scale": ZOOM_SCALE,
        "cells": [asdict(item) for item in cells],
        "merges": [asdict(item) for item in merges],
        "rows": [asdict(item) for item in row_mutations],
        "columns": [asdict(item) for item in column_mutations],
        "display_number_formats": dict(sorted(display_number_formats.items())),
        "bindings": [asdict(item) for item in bindings],
        "sparklines": [asdict(item) for item in sparklines],
        "major_section_rows": {
            "Operating Drivers Overview": 3,
            "Core Drivers": 18,
            "Quarterly Driver History": 32,
        },
        "core_group_rows": core_group_rows,
        "history_group_rows": history_group_rows,
        "history_metric_rows": history_metric_rows,
        "core_metric_rows": core_rows,
        "footprint_definition_rows": footprint_definition_rows,
    }
    return OperatingDriverAnfWorkbookV4Plan(
        contract_version=WORKBOOK_CONTRACT,
        plan_origin="BLANK_SURFACE_V4",
        sheet_name=SHEET_NAME,
        used_range=USED_RANGE,
        zoom_scale=ZOOM_SCALE,
        cell_mutations=tuple(cells),
        merge_mutations=tuple(merges),
        row_mutations=row_mutations,
        column_mutations=column_mutations,
        dimension_mutation=WorksheetDimensionMutation(SHEET_NAME, USED_RANGE),
        display_number_formats=dict(sorted(display_number_formats.items())),
        bindings=tuple(bindings),
        sparkline_records=tuple(sparklines),
        major_section_rows=payload["major_section_rows"],
        history_group_rows=history_group_rows,
        history_metric_rows=history_metric_rows,
        core_group_rows=core_group_rows,
        core_metric_rows=core_rows,
        footprint_definition_rows=footprint_definition_rows,
        plan_sha256=_digest(payload),
    )


def _relationship_part(sheet_part: str) -> str:
    path = PurePosixPath(sheet_part)
    return str(path.parent / "_rels" / f"{path.name}.rels")


def _related_target(sheet_part: str, target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    return str(PurePosixPath(sheet_part).parent / target)


def _blank_worksheet_xml(
    legacy_drawing_id: str | None, *, max_row: int, zoom_scale: int
) -> bytes:
    legacy = (
        ""
        if legacy_drawing_id is None
        else f'<legacyDrawing xmlns:r="{_REL_NS}" r:id="{escape(legacy_drawing_id, quote=True)}"/>'
    )
    # Seed the bounded blank canvas with empty row elements so the accepted
    # row-property materializer can apply heights to visual spacer rows as
    # well as value-bearing rows.  These nodes carry presentation geometry
    # only; no rejected V1/V2/V3 cells or row semantics are retained.
    blank_rows = "".join(f'<row r="{row}"></row>' for row in range(1, max_row + 1))
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<worksheet xmlns="{_MAIN_NS}" xmlns:r="{_REL_NS}">'
        '<dimension ref="A1"/>'
        f'<sheetViews><sheetView showGridLines="0" zoomScale="{zoom_scale}" zoomScaleNormal="{zoom_scale}" workbookViewId="0"><selection activeCell="A1" sqref="A1"/></sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="19.5"/>'
        f'<sheetData>{blank_rows}</sheetData>'
        '<pageMargins left="0.5" right="0.5" top="0.5" bottom="0.5" header="0.3" footer="0.3"/>'
        f'{legacy}'
        '</worksheet>'
    ).encode("utf-8")


def _prepare_blank_surface(
    base_workbook: Path,
    output_workbook: Path,
    *,
    max_row: int,
    zoom_scale: int,
) -> tuple[str, int, tuple[str, ...]]:
    with ZipFile(base_workbook, "r") as archive:
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
        sheet_part = _sheet_part_map(archive)[SHEET_NAME]
    rel_part = _relationship_part(sheet_part)
    legacy_drawing_id: str | None = None
    comment_part: str | None = None
    vml_part: str | None = None
    if rel_part in members:
        rel_root = ET.fromstring(members[rel_part])
        for item in rel_root:
            relation_type = item.attrib.get("Type", "")
            if relation_type.endswith("/comments"):
                comment_part = _related_target(sheet_part, item.attrib["Target"])
            elif relation_type.endswith("/vmlDrawing"):
                vml_part = _related_target(sheet_part, item.attrib["Target"])
                legacy_drawing_id = item.attrib.get("Id")
    comments_removed = 0
    changed_support: list[str] = []
    if comment_part is not None and comment_part in members:
        root = ET.fromstring(members[comment_part])
        comments_removed = sum(1 for item in root.iter() if item.tag.endswith("comment"))
        updated = _COMMENT_LIST_RE.sub(b"<commentList/>", members[comment_part], count=1)
        if updated != members[comment_part]:
            members[comment_part] = updated
            changed_support.append(comment_part)
    if vml_part is not None and vml_part in members:
        updated = _NOTE_SHAPE_RE.sub(
            lambda match: b"" if b'ObjectType="Note"' in match.group(0) else match.group(0),
            members[vml_part],
        )
        if updated != members[vml_part]:
            members[vml_part] = updated
            changed_support.append(vml_part)
    members[sheet_part] = _blank_worksheet_xml(
        legacy_drawing_id, max_row=max_row, zoom_scale=zoom_scale
    )
    _write_package(base_workbook=base_workbook, output_workbook=output_workbook, members=members)
    return sheet_part, comments_removed, tuple(sorted(changed_support))


def _cell_style_id(root: ET.Element, coordinate: str) -> int:
    cell = root.find(f".//{{{_MAIN_NS}}}c[@r='{coordinate}']")
    if cell is None:
        raise OperatingDriverAnfWorkbookV4Error(f"Style source or target cell is absent: {coordinate}.")
    return int(cell.attrib.get("s", "0"))


def _patch_smart_precision(
    members: dict[str, bytes],
    target_part: str,
    plan: OperatingDriverAnfWorkbookV4Plan,
) -> int:
    """Apply target-only smart percentage and dollar-million display roles."""

    target_formats = dict(plan.display_number_formats)
    targets = tuple(
        sorted(
            target_formats
        )
    )
    if not targets:
        return 0
    styles_root = ET.fromstring(members["xl/styles.xml"])
    num_fmts = styles_root.find(f"{{{_MAIN_NS}}}numFmts")
    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    if num_fmts is None or cell_xfs is None:
        raise OperatingDriverAnfWorkbookV4Error(
            "Workbook styles are missing numFmts or cellXfs."
        )
    existing_formats = {
        item.attrib.get("formatCode"): int(item.attrib["numFmtId"])
        for item in num_fmts
    }
    existing_formats.setdefault("#,##0", 3)
    existing_formats.setdefault("0%", 9)
    next_format_id = max(
        (int(item.attrib["numFmtId"]) for item in num_fmts),
        default=163,
    ) + 1
    format_ids: dict[str, int] = {}
    for format_code in sorted(set(target_formats.values())):
        number_format_id = existing_formats.get(format_code)
        if number_format_id is None:
            number_format_id = next_format_id
            next_format_id += 1
            num_fmts.append(
                ET.Element(
                    f"{{{_MAIN_NS}}}numFmt",
                    {"numFmtId": str(number_format_id), "formatCode": format_code},
                )
            )
        format_ids[format_code] = number_format_id
    num_fmts.attrib["count"] = str(len(list(num_fmts)))

    target_root = ET.fromstring(members[target_part])
    xfs = list(cell_xfs)
    style_cache: dict[tuple[int, str], int] = {}
    for coordinate in targets:
        cell = target_root.find(f".//{{{_MAIN_NS}}}c[@r='{coordinate}']")
        if cell is None:
            raise OperatingDriverAnfWorkbookV4Error(
                f"Smart-precision target is absent: {coordinate}."
            )
        base_style = int(cell.attrib.get("s", "0"))
        format_code = target_formats[coordinate]
        style_key = (base_style, format_code)
        style_id = style_cache.get(style_key)
        if style_id is None:
            clone = deepcopy(xfs[base_style])
            clone.attrib["numFmtId"] = str(format_ids[format_code])
            clone.attrib["applyNumberFormat"] = "1"
            cell_xfs.append(clone)
            style_id = len(xfs) + len(style_cache)
            style_cache[style_key] = style_id
        cell.attrib["s"] = str(style_id)
    cell_xfs.attrib["count"] = str(len(list(cell_xfs)))
    ET.register_namespace("", _MAIN_NS)
    ET.register_namespace("r", _REL_NS)
    members["xl/styles.xml"] = ET.tostring(
        styles_root, encoding="utf-8", xml_declaration=True
    )
    members[target_part] = ET.tostring(
        target_root, encoding="utf-8", xml_declaration=True
    )
    return len(targets)


def _patch_latest_quarter_emphasis(
    members: dict[str, bytes],
    sheet_parts: Mapping[str, str],
    target_part: str,
    plan: OperatingDriverAnfWorkbookV4Plan,
) -> int:
    """Apply a restrained, right-aligned latest-quarter band with cloned styles."""

    styles_root = ET.fromstring(members["xl/styles.xml"])
    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    if cell_xfs is None:
        raise OperatingDriverAnfWorkbookV4Error("Workbook styles are missing cellXfs.")
    xfs = list(cell_xfs)

    source_roots = {
        sheet: ET.fromstring(members[part])
        for sheet, part in sheet_parts.items()
        if sheet in {"Valuation", "ANF_Investment_Case"}
    }

    def fill_id(sheet: str, coordinate: str) -> int:
        source_style = _cell_style_id(source_roots[sheet], coordinate)
        return int(xfs[source_style].attrib.get("fillId", "0"))

    light_fill = fill_id("Valuation", "O9")
    header_fill = fill_id("ANF_Investment_Case", "A39")
    group_fill = fill_id("Valuation", "A7")
    target_root = ET.fromstring(members[target_part])
    history_header_row = int(plan.major_section_rows["Quarterly Driver History"]) + 1
    assignments = {
        history_header_row: header_fill,
        **{row: group_fill for row in plan.history_group_rows.values()},
        **{row: light_fill for row in plan.history_metric_rows.values()},
    }
    style_cache: dict[tuple[int, int], int] = {}
    for row, requested_fill in sorted(assignments.items()):
        coordinate = f"P{row}"
        cell = target_root.find(f".//{{{_MAIN_NS}}}c[@r='{coordinate}']")
        if cell is None:
            raise OperatingDriverAnfWorkbookV4Error(f"Latest-quarter emphasis target is absent: {coordinate}.")
        base_style = int(cell.attrib.get("s", "0"))
        cache_key = (base_style, requested_fill)
        style_id = style_cache.get(cache_key)
        if style_id is None:
            clone = deepcopy(xfs[base_style])
            clone.attrib["fillId"] = str(requested_fill)
            clone.attrib["applyFill"] = "1"
            cell_xfs.append(clone)
            style_id = len(xfs) + len(style_cache)
            style_cache[cache_key] = style_id
        cell.attrib["s"] = str(style_id)

    cell_xfs.attrib["count"] = str(len(list(cell_xfs)))
    ET.register_namespace("", _MAIN_NS)
    ET.register_namespace("r", _REL_NS)
    members["xl/styles.xml"] = ET.tostring(styles_root, encoding="utf-8", xml_declaration=True)
    members[target_part] = ET.tostring(target_root, encoding="utf-8", xml_declaration=True)
    return len(assignments)


def _patch_sparklines(data: bytes, records: Sequence[SparklineRecord]) -> bytes:
    if not records:
        return data
    if b"sparklineGroups" in data:
        raise OperatingDriverAnfWorkbookV4Error("Blank V4 surface unexpectedly contains existing sparklines.")
    items = "".join(
        f"<x14:sparkline><xm:f>'{SHEET_NAME}'!{item.source_range}</xm:f><xm:sqref>{item.target_cell}</xm:sqref></x14:sparkline>"
        for item in records
    )
    extension = (
        f'<ext uri="{_SPARKLINE_EXTENSION_URI}" xmlns:x14="{_X14_NS}">'
        f'<x14:sparklineGroups xmlns:xm="{_XM_NS}">'
        '<x14:sparklineGroup displayEmptyCellsAs="gap" lineWeight="1">'
        '<x14:colorSeries rgb="FF4472C4"/>'
        '<x14:colorNegative rgb="FF4472C4"/>'
        '<x14:colorAxis rgb="FF4472C4"/>'
        '<x14:colorMarkers rgb="FF4472C4"/>'
        '<x14:colorFirst rgb="FF4472C4"/>'
        '<x14:colorLast rgb="FF4472C4"/>'
        '<x14:colorHigh rgb="FF4472C4"/>'
        '<x14:colorLow rgb="FF4472C4"/>'
        f'<x14:sparklines>{items}</x14:sparklines>'
        '</x14:sparklineGroup></x14:sparklineGroups></ext>'
    ).encode("utf-8")
    closing = data.rfind(b"</worksheet>")
    if closing < 0:
        raise OperatingDriverAnfWorkbookV4Error("V4 target worksheet is not well formed.")
    return data[:closing] + b"<extLst>" + extension + b"</extLst>" + data[closing:]


def _sparkline_semantics(root: ET.Element) -> tuple[tuple[str, str], ...]:
    ns = {"x14": _X14_NS, "xm": _XM_NS}
    result = []
    for item in root.findall(".//x14:sparkline", ns):
        formula = item.find("xm:f", ns)
        target = item.find("xm:sqref", ns)
        result.append(("" if target is None else target.text or "", "" if formula is None else formula.text or ""))
    return tuple(sorted(result))


def _visible_snapshot(workbook: Path) -> dict[str, Any]:
    ns = {"m": _MAIN_NS}
    with ZipFile(workbook, "r") as archive:
        part = _sheet_part_map(archive)[SHEET_NAME]
        root = ET.fromstring(archive.read(part))
    cells: list[dict[str, Any]] = []
    for cell in root.findall(".//m:sheetData/m:row/m:c", ns):
        inline = cell.find("m:is", ns)
        value = cell.find("m:v", ns)
        formula = cell.find("m:f", ns)
        text = None if inline is None else "".join(item.text or "" for item in inline.findall(".//m:t", ns))
        cells.append(
            {
                "coordinate": cell.attrib["r"],
                "formula": None if formula is None else formula.text,
                "style_id": int(cell.attrib.get("s", "0")),
                "type": cell.attrib.get("t"),
                "value": text if text is not None else (None if value is None else value.text),
            }
        )
    merges = sorted(item.attrib["ref"] for item in root.findall("m:mergeCells/m:mergeCell", ns))
    dimension = root.find("m:dimension", ns)
    columns = [dict(item.attrib) for item in root.findall("m:cols/m:col", ns)]
    rows = [dict(item.attrib) for item in root.findall("m:sheetData/m:row", ns)]
    sheet_view = root.find("m:sheetViews/m:sheetView", ns)
    return {
        "cells": sorted(cells, key=lambda item: item["coordinate"]),
        "columns": columns,
        "dimension": None if dimension is None else dimension.attrib.get("ref"),
        "merge_ranges": merges,
        "rows": rows,
        "sheet_view": {} if sheet_view is None else dict(sorted(sheet_view.attrib.items())),
        "semantic_hash_contract": WORKBOOK_SEMANTIC_HASH_CONTRACT,
        "sparklines": _sparkline_semantics(root),
    }


def operating_driver_anf_v4_semantic_sha256(workbook: Path | str) -> str:
    return _digest(_visible_snapshot(Path(workbook)))


def _comment_count(workbook: Path, sheet_part: str) -> int:
    with ZipFile(workbook, "r") as archive:
        rel_part = _relationship_part(sheet_part)
        if rel_part not in archive.namelist():
            return 0
        root = ET.fromstring(archive.read(rel_part))
        for item in root:
            if item.attrib.get("Type", "").endswith("/comments"):
                comment_part = _related_target(sheet_part, item.attrib["Target"])
                comment_root = ET.fromstring(archive.read(comment_part))
                return sum(1 for node in comment_root.iter() if node.tag.endswith("comment"))
    return 0


def materialize_operating_driver_anf_workbook_v4(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    plan: OperatingDriverAnfWorkbookV4Plan,
    expected_base_sha256: str,
) -> OperatingDriverAnfWorkbookV4Result:
    base = Path(base_workbook)
    output = Path(output_workbook)
    if output.exists():
        raise OperatingDriverAnfWorkbookV4Error(f"Refusing to overwrite existing output: {output}.")
    if sha256_file(base) != expected_base_sha256.lower():
        raise OperatingDriverAnfWorkbookV4Error("Protected ANF workbook identity changed.")
    output.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix="anf_operating_drivers_v4_", dir=output.parent) as temp:
        temporary = Path(temp)
        blank = temporary / "blank_surface.xlsx"
        intermediate = temporary / "targeted_materialization.xlsx"
        max_row = int(USED_RANGE.rsplit("P", 1)[-1])
        sheet_part, comments_removed, comment_support_parts = _prepare_blank_surface(
            base,
            blank,
            max_row=max_row,
            zoom_scale=plan.zoom_scale,
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
            raise OperatingDriverAnfWorkbookV4Error("Operating_Drivers sheet identity changed during materialization.")
        smart_precision_count = _patch_smart_precision(members, target_part, plan)
        latest_emphasis_count = _patch_latest_quarter_emphasis(
            members, sheet_parts, target_part, plan
        )
        members[target_part] = _patch_sparklines(members[target_part], plan.sparkline_records)
        _write_package(base_workbook=intermediate, output_workbook=output, members=members)

    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if before.namelist() != after.namelist():
            raise OperatingDriverAnfWorkbookV4Error("Workbook member inventory changed.")
        before_members = {name: before.read(name) for name in before.namelist()}
        after_members = {name: after.read(name) for name in after.namelist()}
    changed = tuple(sorted(name for name in before_members if before_members[name] != after_members[name]))
    allowed = tuple(sorted({sheet_part, "xl/styles.xml", *comment_support_parts}))
    unexpected = tuple(sorted(set(changed) - set(allowed)))
    snapshot = _visible_snapshot(output)
    formula_count = sum(item["formula"] is not None for item in snapshot["cells"])
    expected_sparklines = tuple(
        sorted((item.target_cell, f"'{SHEET_NAME}'!{item.source_range}") for item in plan.sparkline_records)
    )
    actual_sparklines = tuple(snapshot["sparklines"])

    missing_coordinates = {
        item.target_range
        for item in plan.bindings
        if item.element_type == "HISTORY_MISSING"
    }
    by_coordinate = {item["coordinate"]: item for item in snapshot["cells"]}
    full_range_style_mismatch = sum(
        coordinate not in by_coordinate or int(by_coordinate[coordinate]["style_id"]) == 0
        for binding in plan.bindings
        for coordinate in _range_coordinates(binding.target_range)
    )
    missing_to_zero = sum(
        coordinate in by_coordinate
        and by_coordinate[coordinate]["type"] == "n"
        and Decimal(str(by_coordinate[coordinate]["value"] or "0")) == 0
        for coordinate in missing_coordinates
    )
    before_vba = before_members.get("xl/vbaProject.bin")
    after_vba = after_members.get("xl/vbaProject.bin")
    return OperatingDriverAnfWorkbookV4Result(
        base_workbook_sha256=sha256_file(base),
        output_workbook_sha256=sha256_file(output),
        semantic_workbook_sha256=operating_driver_anf_v4_semantic_sha256(output),
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        canonical_ooxml_contract=CANONICAL_OOXML_HASH_CONTRACT,
        changed_ooxml_parts=changed,
        allowed_changed_ooxml_parts=allowed,
        unrelated_workbook_delta_count=len(unexpected),
        unchanged_ooxml_part_count=len(before_members) - len(changed),
        comments_removed_count=comments_removed,
        target_formula_count=formula_count,
        missing_to_zero_count=missing_to_zero,
        sparkline_count=len(actual_sparklines),
        sparkline_readback_mismatch_count=int(actual_sparklines != expected_sparklines),
        full_range_style_mismatch_count=full_range_style_mismatch,
        latest_quarter_emphasis_cell_count=latest_emphasis_count,
        smart_precision_cell_count=smart_precision_count,
        vba_sha256_before=None if before_vba is None else hashlib.sha256(before_vba).hexdigest(),
        vba_sha256_after=None if after_vba is None else hashlib.sha256(after_vba).hexdigest(),
        plan_sha256=plan.plan_sha256,
    )


__all__ = [
    "OperatingDriverAnfWorkbookV4Error",
    "OperatingDriverAnfWorkbookV4Plan",
    "OperatingDriverAnfWorkbookV4Result",
    "SparklineRecord",
    "USED_RANGE",
    "WORKBOOK_CONTRACT",
    "WORKBOOK_SEMANTIC_HASH_CONTRACT",
    "ZOOM_SCALE",
    "WorkbookBinding",
    "build_operating_driver_anf_workbook_v4_plan",
    "materialize_operating_driver_anf_workbook_v4",
    "operating_driver_anf_v4_semantic_sha256",
]
