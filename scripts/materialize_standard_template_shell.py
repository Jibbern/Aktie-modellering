"""Materialize the frozen standard stock-model workbook shell.

This is a one-time/repeatable shell authoring helper, not the new-ticker value
filler runtime. It creates an empty visible shell from the manifest and binding
map, using the existing PBI/GPRE/ANF workbooks only as layout contract sources
for broad dimensions/freeze-pane conventions.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from openpyxl import Workbook, load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import absolute_coordinate, get_column_letter, quote_sheetname, range_boundaries
from openpyxl.workbook.defined_name import DefinedName


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "templates" / "standard_stock_model_template.xlsx"
DEFAULT_LAB_SOURCE = ROOT / "templates" / "lab" / "ANF_template_lab.xlsx"

SOURCE_TICKERS = ("PBI", "GPRE", "ANF")
SOURCE_SPECIFIC_TERMS = (
    "ANF",
    "Pitney Bowes",
    "Green Plains",
    "Abercrombie",
    "Hollister",
    "Presort",
    "SendTech",
    "45Z",
    "RIN",
    "crush margin",
)
FIXED_DIMENSION_REPLACEMENTS = (
    (re.compile(r"\bAmericas\b", re.I), "[Dimension member 1]"),
    (re.compile(r"\bEMEA\b", re.I), "[Dimension member 2]"),
    (re.compile(r"\bAPAC\b", re.I), "[Dimension member 3]"),
)
FIXED_SECTOR_LABEL_PATTERNS = tuple(
    re.compile(pattern, re.I)
    for pattern in (
        r"\bclosures?\b",
        r"\bopenings?\b",
        r"\bremodels?\b",
        r"\bstores?\s*/\s*buybacks?\b",
        r"\bstores?\s*/\s*real estate\b",
        r"\bfranchise stores?\b",
        r"\bowned stores?\b",
        r"\btariffs?\b",
        r"\bERP\b",
        r"\bfreight tailwind\b",
        r"\bmarketing headwind\b",
        r"\bnet sales growth\b",
        r"\badjusted EPS\b",
        r"\bshare repurchases?\b",
        r"\breal estate activity\b",
    )
)
VALUATION_LABEL_REPLACEMENTS = (
    (re.compile(r"\bAdj\.?\s*EPS\b", re.I), "per-share earnings"),
    (re.compile(r"\badjusted EPS\b", re.I), "per-share earnings"),
    (re.compile(r"\bEPS\b", re.I), "per-share earnings"),
)
SIGNAL_FILL_COLORS = {
    "002F80ED",
    "006FA8DC",
    "009BD3F5",
    "00A63A00",
    "00D55E00",
    "00E69F00",
    "00009E73",
    "0066C2A5",
    "0056B4E9",
    "00CC79A7",
}
GRAY_BLANK_FILLS = {"00DDDDDD", "00D9D9D9", "FFD9D9D9", "FFDDDDDD"}
STATUS_OUTPUT_FILL_COLORS = {"00D9EAF7", "00F2F2F2", "00F4CCCC", "00FFF2CC"}
NEUTRAL_HEADER_FILL_COLORS = SIGNAL_FILL_COLORS
VALUATION_RUNTIME_VALUE_CONSTANT_RANGES = (
    "D194:D216",
    "E236:E240",
    "D247:D250",
    "E253:E256",
    "L248:S250",
)
QA_HEADERS = [
    "severity",
    "rule_id",
    "field",
    "message",
    "source_ref",
    "suggested_action",
    "sheet",
    "section",
    "binding_id",
    "target",
    "status",
]
SUPPORT_SHEET_HEADERS = {
    "Hidden_Value_Flags": [
        "field",
        "display_name",
        "metric_type",
        "source_policy",
        "status",
        "reason",
        "sheet",
        "target",
        "binding_id",
        "review_status",
        "reserved",
        "has_hidden_value_issue",
    ],
    "Revolver_History": ["period", "capacity", "drawn", "availability", "covenant_status", "source_ref", "status"],
    "Debt_Tranches_Latest": ["instrument", "principal", "coupon", "maturity", "secured", "source_ref", "status"],
    "Debt_Profile": ["metric", "value", "period", "unit", "source_ref", "status"],
    "Guidance_Normalized": ["metric", "horizon", "period", "value", "unit", "status", "source_ref", "notes"],
    "Quarter_Notes": ["period", "theme", "metric", "note", "source_ref", "status"],
    "Promise_Progress": ["period", "metric", "previous_guide", "current_guide", "actual", "status", "source_ref"],
    "History_Q": ["period", "metric", "value", "unit", "source_ref", "status"],
}
VALUATION_GUIDANCE_SIDECAR_HEADERS = {
    "O7": "Guidance",
    "O28": "Metric",
    "Q28": "Stated in",
    "R28": "Applies to",
    "S28": "Guidance",
    "AA28": "Trend / realized",
    "O37": "Operating Drivers",
    "O38": "Driver group",
    "R38": "Driver",
    "U38": "Why it matters",
    "AA38": "Source/type",
    "O48": "Thesis Bridge",
    "O49": "Quick valuation bridge; no market price required.",
    "O50": "Bridge item",
    "U50": "Value",
    "X50": "Notes",
    "O63": "Output",
    "U63": "Value",
    "X63": "Interpretation",
}
VALUATION_STRUCTURAL_HEADERS = {
    "B123": "Principal due ($m)",
    "C123": "Rate type",
    "D123": "Coupon/Spread %",
    "F123": "Maturity",
    "G123": "Conversion price",
    "I123": "Added shares on full conversion (m)",
    "L123": "Concurrent repurchased shares (m)",
    "B138": "Summary",
    "F138": "Score",
    "G138": "Severity",
    "H138": "Result / support",
    "B159": "Δ",
    "C159": "Direction",
    "D159": "As-of",
    "B169": "Status",
    "C169": "Evidence",
    "I169": "As-of",
}
VALUATION_BLUE_SECTION_HEADERS = {
    "O7": "Guidance",
    "O37": "Operating Drivers",
    "O48": "Thesis Bridge",
    "A122": "Debt Detail (latest)",
    "A137": "Hidden value flags",
    "N137": "Hidden Value Panel",
    "A145": "Operating signals",
    "A151": "Capital return",
    "A158": "Trend/Δ (last 4Q)",
    "A168": "Red/Green Flags",
    "B192": "Valuation",
}
STANDARD_RED_GREEN_FLAG_LABELS = {
    170: "Red: Revenue up but CFO down (YoY)",
    171: "Red: Earnings quality CFO/NI (TTM)",
    172: "Red: AR growing faster than revenue (YoY)",
    173: "Red: Inventory build without revenue growth",
    174: "Red: Debt growing faster than revenue (YoY)",
    175: "Red: Leverage rising (YoY Δ)",
    176: "Red: Interest coverage low (cash)",
    177: "Red: FCF negative while EBITDA positive (TTM)",
    178: "Watch: Buybacks exceeded FCF",
    179: "Red: Goodwill heavy",
    180: "Red: Share dilution (YoY)",
    181: "Red: Pension obligations pressure",
    183: "Green: Operating margin trend QoQ",
    184: "Green: FCF TTM growth (YoY)",
    185: "Green: Net debt decreasing (YoY)",
    186: "Green: Interest coverage improving (YoY)",
    187: "Green: Shares outstanding decreasing (YoY)",
    188: "Green: Liquidity improving (YoY)",
}
OPERATING_DRIVER_SHEET_HEADERS = {
    "A12": "Topic",
    "B12": "Current read",
    "H12": "Source / use",
    "A19": "Horizon",
    "B19": "Stated in",
    "C19": "Commentary",
}
ALLOWED_HIDDEN_SHELL_SHEETS = set(SUPPORT_SHEET_HEADERS)
SHEET_REF_RE = re.compile(r"'([^']+)'!|(?<![A-Za-z0-9_])([A-Za-z_][A-Za-z0-9_ ]{0,60})!")


@dataclass(frozen=True)
class SourceSheetContract:
    freeze_panes: str | None
    column_widths: dict[str, float]
    row_heights: dict[int, float]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _default_data_root() -> Path:
    for parent in (ROOT, *ROOT.parents):
        candidate = parent / "StockModelData"
        if candidate.exists():
            return candidate
    return ROOT.parent / "StockModelData"


DEFAULT_DATA_ROOT = _default_data_root()


def _source_workbook_paths(data_root: Path) -> dict[str, Path]:
    output_dir = data_root / "outputs" / "Excel stock models"
    return {ticker: output_dir / f"{ticker}_model.xlsx" for ticker in SOURCE_TICKERS}


def _source_sheet_name(sheet_name: str, ticker: str) -> str:
    if sheet_name == "{ticker}_Investment_Case":
        return f"{ticker}_Investment_Case"
    return sheet_name


def _load_source_contracts(data_root: Path, manifest: dict[str, Any]) -> dict[str, SourceSheetContract]:
    paths = _source_workbook_paths(data_root)
    workbooks: dict[str, Any] = {}
    for ticker, path in paths.items():
        if path.exists():
            workbooks[ticker] = load_workbook(path, read_only=False, data_only=False)

    contracts: dict[str, SourceSheetContract] = {}
    for sheet_def in manifest["sheets"]:
        sheet_name = str(sheet_def["sheet"])
        chosen = None
        for ticker in SOURCE_TICKERS:
            wb = workbooks.get(ticker)
            candidate = _source_sheet_name(sheet_name, ticker)
            if wb is not None and candidate in wb.sheetnames:
                chosen = wb[candidate]
                break
        if chosen is None:
            contracts[sheet_name] = SourceSheetContract(
                freeze_panes=None,
                column_widths={},
                row_heights={},
            )
            continue
        contracts[sheet_name] = SourceSheetContract(
            freeze_panes=str(chosen.freeze_panes) if chosen.freeze_panes else None,
            column_widths={
                col: float(dim.width)
                for col, dim in chosen.column_dimensions.items()
                if dim.width is not None and 1.0 <= float(dim.width) <= 80.0
            },
            row_heights={
                int(row): float(dim.height)
                for row, dim in chosen.row_dimensions.items()
                if dim.height is not None and 1 <= int(row) <= 1000
            },
        )
    for wb in workbooks.values():
        wb.close()
    return contracts


def _max_manifest_bounds(sheet_def: dict[str, Any]) -> tuple[int, int]:
    max_col = 1
    max_row = 1
    for zone_type in ("writable_zones", "non_writable_zones"):
        for zone in sheet_def[zone_type]:
            min_col, min_row, max_col_in, max_row_in = range_boundaries(zone["target"])
            max_col = max(max_col, max_col_in, min_col)
            max_row = max(max_row, max_row_in, min_row)
    return max_col, max_row


def _ranges_for(sheet_def: dict[str, Any], zone_type: str) -> list[str]:
    return [str(zone["target"]) for zone in sheet_def.get(zone_type, [])]


def _sheet_title(sheet_name: str) -> str:
    return {
        "SUMMARY": "SUMMARY",
        "Valuation": "Valuation",
        "BS_Segments": "Balance Sheet & Segments",
        "Operating_Drivers": "Operating Drivers",
        "{ticker}_Investment_Case": "{ticker} Investment Case",
        "Quarter_Notes_UI": "Quarter Notes",
        "Promise_Progress_UI": "Promise Progress",
        "QA_Log": "QA Log",
        "Needs_Review": "Needs Review",
        "QA_Checks": "QA Checks",
    }.get(sheet_name, sheet_name)


def _title_cell(sheet_name: str) -> str:
    return {
        "Valuation": "A3",
        "Operating_Drivers": "A2",
    }.get(sheet_name, "A1")


def _fallback_freeze(sheet_name: str) -> str:
    return {
        "SUMMARY": "A4",
        "Valuation": "B20",
        "BS_Segments": "B8",
        "Operating_Drivers": "B7",
        "{ticker}_Investment_Case": "B5",
        "Quarter_Notes_UI": "B8",
        "Promise_Progress_UI": "B8",
        "QA_Log": "A2",
        "Needs_Review": "A2",
        "QA_Checks": "A2",
    }[sheet_name]


def _anchor_row_from_zone(sheet_def: dict[str, Any], zone_id: str) -> int:
    for zone in sheet_def["writable_zones"]:
        if zone["zone_id"] == zone_id:
            _min_col, min_row, _max_col, _max_row = range_boundaries(zone["target"])
            return int(min_row)
    return 1


def _binding_anchor_rows(bindings: Iterable[dict[str, Any]]) -> dict[tuple[str, int], str]:
    labels: dict[tuple[str, int], str] = {}
    for entry in bindings:
        sheet = str(entry["sheet"])
        target = str(entry["target"])
        _min_col, min_row, _max_col, _max_row = range_boundaries(target)
        if str(entry.get("source_policy")) == "validation-output":
            continue
        label = str(entry.get("anchor_label") or entry.get("section") or "").strip()
        if label:
            labels[(sheet, int(min_row))] = label
    return labels


def _apply_zone_style(ws: Any, ranges: Iterable[str], fill: PatternFill, border: Border) -> None:
    for range_ref in ranges:
        for row in ws[range_ref]:
            for cell in row:
                cell.fill = fill
                cell.border = border


def _merge_title(ws: Any, sheet_name: str, max_col: int) -> None:
    if sheet_name in {"QA_Log", "Needs_Review", "QA_Checks"}:
        return
    title_ref = _title_cell(sheet_name)
    row = int("".join(ch for ch in title_ref if ch.isdigit()))
    end_col = min(max_col, 15)
    if end_col > 1:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=end_col)


def _write_formula_support(ws: Any, sheet_name: str) -> None:
    if sheet_name == "Valuation":
        ws["N20"] = '=IFERROR(B20,"")'
        ws["N21"] = '=IFERROR(B21,"")'
        ws["N120"] = '=IFERROR(B120,"")'
        ws["N151"] = '=IFERROR(B151,"")'
    elif sheet_name == "{ticker}_Investment_Case":
        ws["B191"] = '=IFERROR(B81,"")'
        ws["B192"] = '=IFERROR(B131,"")'


def _apply_dimensions(ws: Any, sheet_name: str, contract: SourceSheetContract, max_col: int, max_row: int) -> None:
    for col_idx in range(1, max_col + 1):
        letter = get_column_letter(col_idx)
        width = contract.column_widths.get(letter)
        if width is None:
            if col_idx == 1:
                width = 28.0
            elif sheet_name in {"Quarter_Notes_UI", "Promise_Progress_UI"}:
                width = 18.0
            elif sheet_name in {"QA_Log", "Needs_Review", "QA_Checks"}:
                width = 20.0
            else:
                width = 14.0
        ws.column_dimensions[letter].width = width
    for row_idx in range(1, min(max_row, 260) + 1):
        height = contract.row_heights.get(row_idx)
        if height is None:
            height = 24.0 if row_idx in {1, 2, 3} else 18.0
        ws.row_dimensions[row_idx].height = height
    ws.freeze_panes = contract.freeze_panes or _fallback_freeze(sheet_name)


def _write_static_structure(
    wb: Workbook,
    ws: Any,
    sheet_def: dict[str, Any],
    bindings: list[dict[str, Any]],
    contract: SourceSheetContract,
) -> None:
    sheet_name = str(sheet_def["sheet"])
    max_col, max_row = _max_manifest_bounds(sheet_def)

    title_fill = PatternFill("solid", fgColor="4472C4")
    section_fill = PatternFill("solid", fgColor="DDEBF7")
    label_fill = PatternFill("solid", fgColor="F2F6FA")
    writable_fill = PatternFill("solid", fgColor="FFFFFF")
    border = Border(
        left=Side(style="thin", color="D9E2EA"),
        right=Side(style="thin", color="D9E2EA"),
        top=Side(style="thin", color="D9E2EA"),
        bottom=Side(style="thin", color="D9E2EA"),
    )

    ws.sheet_view.showGridLines = False
    _apply_dimensions(ws, sheet_name, contract, max_col, max_row)
    _apply_zone_style(ws, _ranges_for(sheet_def, "writable_zones"), writable_fill, border)
    _apply_zone_style(ws, _ranges_for(sheet_def, "non_writable_zones"), label_fill, border)

    _merge_title(ws, sheet_name, max_col)
    title_cell = ws[_title_cell(sheet_name)]
    title_cell.value = _sheet_title(sheet_name)
    title_cell.fill = title_fill
    title_cell.font = Font(bold=True, color="FFFFFF", size=15)
    title_cell.alignment = Alignment(horizontal="center", vertical="center")

    binding_rows = _binding_anchor_rows(bindings)
    for (binding_sheet, row_idx), label in binding_rows.items():
        if binding_sheet != sheet_name:
            continue
        cell = ws.cell(row_idx, 1)
        if not cell.value:
            cell.value = label
        cell.fill = section_fill
        cell.font = Font(bold=True, color="1F2933")
        cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

    for label in sheet_def.get("formulas_static_labels", []):
        label_text = str(label or "").strip()
        if not label_text:
            continue
        if any(str(cell.value or "").strip() == label_text for row in ws.iter_rows() for cell in row):
            continue
        # Place extra static labels in the first label column row that is still blank.
        for row_idx in range(1, max_row + 1):
            cell = ws.cell(row_idx, 1)
            if not cell.value:
                cell.value = label_text
                cell.fill = section_fill
                cell.font = Font(bold=True, color="1F2933")
                break

    if sheet_name in {"QA_Log", "Needs_Review", "QA_Checks"}:
        ws["A1"] = _sheet_title(sheet_name)
        for col_idx, header in enumerate(QA_HEADERS, start=2):
            ws.cell(1, col_idx, header)
        for cell in ws[1]:
            cell.fill = title_fill if cell.column == 1 else section_fill
            cell.font = Font(bold=True, color="FFFFFF" if cell.column == 1 else "1F2933")
            cell.alignment = Alignment(horizontal="left", vertical="center")

    if sheet_name == "Valuation":
        ws["A1"] = "Scale"
        ws["B1"] = "$m"
        ws["A2"] = "Values scaled to $m unless %"

    _write_formula_support(ws, sheet_name)

    for anchor in wb._standard_template_required_anchors:  # type: ignore[attr-defined]
        if anchor["sheet"] != sheet_name:
            continue
        row_idx = 1 if sheet_name in {"QA_Log", "Needs_Review", "QA_Checks"} else _anchor_row_from_zone(sheet_def, anchor["zone_id"])
        cell = ws.cell(row_idx, 1)
        if not cell.value:
            cell.value = anchor.get("anchor_label") or anchor["anchor_id"]
        coord = absolute_coordinate(cell.coordinate)
        attr_text = f"{quote_sheetname(sheet_name)}!{coord}"
        wb.defined_names.add(DefinedName(str(anchor["anchor_id"]), attr_text=attr_text))


def _rich_template_source(data_root: Path) -> Path | None:
    if DEFAULT_LAB_SOURCE.exists():
        return DEFAULT_LAB_SOURCE
    source = data_root / "outputs" / "Excel stock models" / "ANF_model.xlsx"
    return source if source.exists() else None


def _rename_investment_case_sheet(wb: Workbook) -> None:
    if "ANF_Investment_Case" in wb.sheetnames and "{ticker}_Investment_Case" not in wb.sheetnames:
        wb["ANF_Investment_Case"].title = "{ticker}_Investment_Case"


def _hide_nonstandard_sheets(wb: Workbook, manifest: dict[str, Any]) -> None:
    standard_visible = {str(sheet_name) for sheet_name in manifest["visible_sheet_order"]}
    for ws in wb.worksheets:
        ws.sheet_state = "visible" if ws.title in standard_visible else "hidden"


def _clear_range_values(ws: Any, range_ref: str) -> None:
    min_col, min_row, max_col, max_row = range_boundaries(range_ref)
    for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            cell.value = None


def _clear_writable_zones(wb: Workbook, manifest: dict[str, Any]) -> None:
    for sheet_def in manifest["sheets"]:
        sheet_name = str(sheet_def["sheet"])
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for zone in sheet_def["writable_zones"]:
            _clear_range_values(ws, str(zone["target"]))


def _clear_source_specific_visible_text(wb: Workbook, manifest: dict[str, Any]) -> None:
    term_re = re.compile("|".join(r"\b" + re.escape(term) + r"\b" for term in SOURCE_SPECIFIC_TERMS), re.I)
    for sheet_name in manifest["visible_sheet_order"]:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for row in ws.iter_rows():
            for cell in row:
                if isinstance(cell, MergedCell) or not isinstance(cell.value, str):
                    continue
                if cell.value.startswith("="):
                    continue
                if term_re.search(cell.value):
                    cell.value = None


def _cell_row(coord: str) -> int:
    digits = "".join(ch for ch in coord if ch.isdigit())
    return int(digits or "0")


def _generic_slot_label(sheet_name: str, coord: str, original: str) -> str:
    row = _cell_row(coord)
    if sheet_name == "Valuation":
        text = original
        for pattern, replacement in VALUATION_LABEL_REPLACEMENTS:
            text = pattern.sub(replacement, text)
        return text
    if sheet_name == "BS_Segments":
        return f"[Dimension member slot {row}]"
    if sheet_name == "Operating_Drivers":
        return f"[Operating driver slot {row}]"
    if sheet_name == "{ticker}_Investment_Case":
        return f"[Scenario driver slot {row}]"
    if sheet_name == "Quarter_Notes_UI":
        return f"[Quarter note theme slot {row}]"
    if sheet_name == "Promise_Progress_UI":
        return f"[Guidance metric slot {row}]"
    return f"[Template slot {row}]"


def _genericize_visible_text_value(sheet_name: str, coord: str, value: str) -> str:
    text = value
    if sheet_name == "Valuation":
        for pattern, replacement in VALUATION_LABEL_REPLACEMENTS:
            text = pattern.sub(replacement, text)
    had_dimension_member = False
    for pattern, replacement in FIXED_DIMENSION_REPLACEMENTS:
        if pattern.search(text):
            had_dimension_member = True
        text = pattern.sub(replacement, text)

    had_sector_label = any(pattern.search(text) for pattern in FIXED_SECTOR_LABEL_PATTERNS) or any(
        pattern.search(value) for pattern in FIXED_SECTOR_LABEL_PATTERNS
    )
    if had_sector_label:
        return _generic_slot_label(sheet_name, coord, text)
    if had_dimension_member:
        text = re.sub(r"\s*\(geography\s*/\s*stores\)", "", text, flags=re.I)
        text = re.sub(r"\bstores?\b", "metric", text, flags=re.I)
        return text
    return text


def _genericize_sector_specific_visible_text(wb: Workbook, manifest: dict[str, Any]) -> None:
    for sheet_name in manifest["visible_sheet_order"]:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for row in ws.iter_rows():
            for cell in row:
                if isinstance(cell, MergedCell) or not isinstance(cell.value, str):
                    continue
                if cell.value.startswith("="):
                    continue
                replacement = _genericize_visible_text_value(sheet_name, cell.coordinate, cell.value)
                if replacement != cell.value:
                    cell.value = replacement


def _clear_valuation_numeric_constants(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            if isinstance(cell.value, (int, float, date, datetime)):
                cell.value = None


def _clear_valuation_runtime_value_constants(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    for range_ref in VALUATION_RUNTIME_VALUE_CONSTANT_RANGES:
        min_col, min_row, max_col, max_row = range_boundaries(range_ref)
        for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
            for cell in row:
                if isinstance(cell, MergedCell):
                    continue
                if cell.value in (None, ""):
                    continue
                if isinstance(cell.value, str) and cell.value.startswith("="):
                    continue
                cell.value = None


def _guard_valuation_scenario_formulas_until_input(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    formulas = {
        "E237": '=IF(E236="","",IF(E236="Bull",0.141711,IF(E236="Bear",0.054169,IF(E236="Base",0.071283,0.071283))))',
        "E238": '=IF(E236="","",IF(E236="Bull",0.184811,IF(E236="Bear",0.128800,IF(E236="Base",0.167782,0.167782))))',
        "E239": '=IF(E236="","",IF(E236="Bull",15,IF(E236="Bear",-10,0)))',
        "E240": '=IF(E236="","",IF(E236="Bull",2,IF(E236="Bear",-1,0)))',
    }
    for coord, formula in formulas.items():
        ws[coord] = formula


def _neutralize_writable_data_like_fills(wb: Workbook, manifest: dict[str, Any]) -> None:
    neutral_fill = PatternFill(fill_type=None)
    for sheet_def in manifest["sheets"]:
        sheet_name = str(sheet_def["sheet"])
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for zone in sheet_def["writable_zones"]:
            min_col, min_row, max_col, max_row = range_boundaries(str(zone["target"]))
            for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
                for cell in row:
                    if isinstance(cell, MergedCell) or cell.value not in (None, ""):
                        continue
                    fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                    if fill in SIGNAL_FILL_COLORS or fill in GRAY_BLANK_FILLS:
                        cell.fill = neutral_fill


def _neutralize_visible_blank_gray_fills(wb: Workbook) -> None:
    neutral_fill = PatternFill(fill_type=None)
    for ws in wb.worksheets:
        if ws.sheet_state != "visible":
            continue
        for row in ws.iter_rows():
            for cell in row:
                if isinstance(cell, MergedCell) or cell.value not in (None, ""):
                    continue
                fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                if fill in GRAY_BLANK_FILLS:
                    cell.fill = neutral_fill


def _neutralize_valuation_signal_fills(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    neutral_fill = PatternFill(fill_type=None)
    for row in ws.iter_rows(min_row=6, max_row=ws.max_row):
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            if cell.coordinate in VALUATION_BLUE_SECTION_HEADERS:
                continue
            fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
            if fill in SIGNAL_FILL_COLORS:
                cell.fill = neutral_fill


def _neutralize_red_green_flags(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    neutral_fill = PatternFill(fill_type=None)
    ws["A168"] = VALUATION_BLUE_SECTION_HEADERS["A168"]
    ws["A169"] = "Flag"
    ws["B169"] = "Status"
    ws["C169"] = "Evidence"
    ws["I169"] = "As-of"
    for row_idx in range(170, 189):
        label_cell = ws.cell(row_idx, 1)
        if not isinstance(label_cell, MergedCell):
            label_cell.value = STANDARD_RED_GREEN_FLAG_LABELS.get(row_idx)
        for col_idx in range(2, 10):
            cell = ws.cell(row_idx, col_idx)
            if isinstance(cell, MergedCell):
                continue
            cell.value = None
            fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
            if fill in SIGNAL_FILL_COLORS or fill in STATUS_OUTPUT_FILL_COLORS or fill in GRAY_BLANK_FILLS:
                cell.fill = neutral_fill


def _neutralize_blank_valuation_value_fills(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    neutral_fill = PatternFill(fill_type=None)
    for range_ref in ("U51:U62",):
        min_col, min_row, max_col, max_row = range_boundaries(range_ref)
        for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
            for cell in row:
                if isinstance(cell, MergedCell) or cell.value not in (None, ""):
                    continue
                fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                if fill not in ("", "00000000", "00FFFFFF", "FFFFFFFF"):
                    cell.fill = neutral_fill


def _ensure_promise_progress_structure(wb: Workbook) -> None:
    if "Promise_Progress_UI" not in wb.sheetnames:
        return
    ws = wb["Promise_Progress_UI"]
    annual_headers = [
        "Metric",
        "Initial guide",
        "Q1 update",
        "Q2 update",
        "Q3 update",
        "Q4 update",
        "Actual",
        "Status",
        "Notes/source",
    ]
    open_guidance_headers = ["Metric", "Current guide", "Horizon", "Status", "Notes/source"]
    revision_headers = [
        "Metric",
        "Previous guide",
        "New/current guide",
        "Change type",
        "Actual",
        "Progress / run-rate",
        "Status",
        "Horizon",
        "Stated in",
        "Source date",
        "Source / note",
    ]

    for row_idx in (12, 23, 29, 34):
        for col_idx, header in enumerate(annual_headers, start=1):
            ws.cell(row_idx, col_idx, header)
    for col_idx, header in enumerate(open_guidance_headers, start=1):
        ws.cell(38, col_idx, header)
    for row_idx in (60, 70, 77, 85, 91, 98):
        for col_idx, header in enumerate(revision_headers, start=1):
            ws.cell(row_idx, col_idx, header)

    for slot_idx, row_idx in enumerate((5, 6, 7, 8, 9), start=1):
        ws.cell(row_idx, 1, f"[Credibility category slot {slot_idx}]")
    guidance_rows = [
        14,
        15,
        16,
        17,
        18,
        24,
        25,
        26,
        27,
        30,
        31,
        32,
        35,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
        48,
        49,
        50,
        51,
        52,
        53,
        54,
        55,
        56,
        57,
        58,
        59,
        61,
        62,
        63,
        64,
        65,
        66,
        67,
        68,
        69,
        71,
        72,
        73,
        74,
        75,
        76,
        78,
        79,
        80,
        81,
        82,
        83,
        84,
        86,
        87,
        88,
        89,
        90,
        92,
        93,
        94,
        95,
        96,
        97,
        99,
        100,
        101,
        102,
        103,
        104,
        105,
        106,
        107,
        108,
        109,
        110,
        111,
        112,
        113,
        114,
        115,
    ]
    for row_idx in guidance_rows:
        cell = ws.cell(row_idx, 1)
        if isinstance(cell, MergedCell):
            continue
        if cell.value in (None, ""):
            continue
        text = str(cell.value)
        if text == "Metric" or text.startswith("["):
            continue
        cell.value = f"[Guidance metric slot {row_idx}]"


def _ensure_merged_range(ws: Any, range_ref: str) -> None:
    if range_ref not in {str(merged_range) for merged_range in ws.merged_cells.ranges}:
        ws.merge_cells(range_ref)


def _style_cells(
    ws: Any,
    range_ref: str,
    *,
    fill: PatternFill | None = None,
    font: Font | None = None,
    alignment: Alignment | None = None,
) -> None:
    min_col, min_row, max_col, max_row = range_boundaries(range_ref)
    for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            if fill is not None:
                cell.fill = copy(fill)
            if font is not None:
                cell.font = copy(font)
            if alignment is not None:
                cell.alignment = copy(alignment)


def _ensure_valuation_guidance_sidecar_headers(wb: Workbook) -> None:
    if "Valuation" not in wb.sheetnames:
        return
    ws = wb["Valuation"]
    section_fill = PatternFill("solid", fgColor="6FA8DC")
    column_header_fill = PatternFill("solid", fgColor="EAF3FB")
    section_font = Font(bold=True, color="FFFFFF", size=12)
    header_font = Font(bold=True, color="000000", size=12)
    title_font = Font(bold=True, color="FFFFFF", size=18)
    left_center = Alignment(horizontal="left", vertical="center", wrap_text=True)
    center_center = Alignment(horizontal="center", vertical="center")

    for range_ref in ("D123:E123", "H138:I138"):
        _ensure_merged_range(ws, range_ref)

    for coord, value in {**VALUATION_GUIDANCE_SIDECAR_HEADERS, **VALUATION_STRUCTURAL_HEADERS}.items():
        cell = ws[coord]
        if not isinstance(cell, MergedCell):
            cell.value = value
            cell.fill = column_header_fill
            cell.font = header_font
            cell.alignment = left_center

    for coord, value in VALUATION_BLUE_SECTION_HEADERS.items():
        cell = ws[coord]
        if not isinstance(cell, MergedCell):
            cell.value = value
            cell.fill = section_fill
            cell.font = section_font
            cell.alignment = left_center

    _style_cells(ws, "A122:N122", fill=section_fill)
    _style_cells(ws, "B123:N123", fill=column_header_fill, font=header_font, alignment=left_center)
    _style_cells(ws, "B138:I138", fill=column_header_fill, font=header_font, alignment=left_center)
    _style_cells(ws, "A145:M145", fill=section_fill)
    _style_cells(ws, "A151:M151", fill=section_fill)
    _style_cells(ws, "A158:D158", fill=section_fill)
    _style_cells(ws, "A169:I169", fill=column_header_fill, font=header_font, alignment=center_center)
    _style_cells(ws, "B192:S192", fill=section_fill, font=title_font)

    for coord in ("A122", "A145", "A151", "A158", "A168"):
        ws[coord].font = section_font
    ws["B192"].value = "Valuation"
    ws["B192"].font = title_font


def _ensure_operating_driver_sheet_headers(wb: Workbook) -> None:
    if "Operating_Drivers" not in wb.sheetnames:
        return
    ws = wb["Operating_Drivers"]
    for coord, value in OPERATING_DRIVER_SHEET_HEADERS.items():
        cell = ws[coord]
        if not isinstance(cell, MergedCell):
            cell.value = value


def _move_operating_drivers_title_to_row1(wb: Workbook) -> None:
    if "Operating_Drivers" not in wb.sheetnames:
        return
    ws = wb["Operating_Drivers"]
    title_fill = PatternFill("solid", fgColor="6FA8DC")
    title_font = Font(bold=True, color="FFFFFF", size=15)
    title_alignment = Alignment(horizontal="center", vertical="center")

    if "A2:N2" in {str(merged_range) for merged_range in ws.merged_cells.ranges}:
        ws.unmerge_cells("A2:N2")
    _ensure_merged_range(ws, "A1:N1")

    ws["A1"] = "Operating Drivers"
    ws["A1"].fill = title_fill
    ws["A1"].font = title_font
    ws["A1"].alignment = title_alignment
    ws.row_dimensions[1].height = ws.row_dimensions[2].height or 24

    for row in ws.iter_rows(min_row=2, max_row=2, min_col=1, max_col=14):
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            cell.value = None
            cell.fill = PatternFill(fill_type=None)
    ws.freeze_panes = "A2"


def _neutralize_remaining_visible_row_labels(wb: Workbook) -> None:
    replacements: dict[str, list[int]] = {
        "Operating_Drivers": [6, 31, 56, 87, 88, 89, 102],
        "Quarter_Notes_UI": [109, 140, 228],
    }
    for sheet_name, rows in replacements.items():
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for row_idx in rows:
            cell = ws.cell(row_idx, 1)
            if isinstance(cell, MergedCell) or cell.value in (None, ""):
                continue
            cell.value = _generic_slot_label(sheet_name, cell.coordinate, str(cell.value))


def _ensure_sheet_titles(wb: Workbook, manifest: dict[str, Any]) -> None:
    for sheet_name in manifest["visible_sheet_order"]:
        if sheet_name not in wb.sheetnames:
            continue
        cell = wb[sheet_name][_title_cell(str(sheet_name))]
        if not isinstance(cell, MergedCell):
            cell.value = _sheet_title(str(sheet_name))


def _ensure_binding_anchor_labels(wb: Workbook, manifest: dict[str, Any], bindings: list[dict[str, Any]]) -> None:
    for entry in bindings:
        sheet_name = str(entry["sheet"])
        if sheet_name not in wb.sheetnames:
            continue
        target = str(entry["target"])
        min_col, min_row, _max_col, _max_row = range_boundaries(target)
        coord = absolute_coordinate(wb[sheet_name].cell(min_row, min_col).coordinate)
        wb.defined_names.add(DefinedName(str(entry["binding_id"]), attr_text=f"{quote_sheetname(sheet_name)}!{coord}"))

    for anchor in manifest.get("required_anchors", []):
        sheet_name = str(anchor["sheet"])
        if sheet_name not in wb.sheetnames:
            continue
        sheet_def = next(sheet for sheet in manifest["sheets"] if sheet["sheet"] == sheet_name)
        row_idx = 1 if sheet_name in {"QA_Log", "Needs_Review", "QA_Checks"} else _anchor_row_from_zone(sheet_def, anchor["zone_id"])
        cell = wb[sheet_name].cell(row_idx, 1)
        coord = absolute_coordinate(cell.coordinate)
        wb.defined_names.add(DefinedName(str(anchor["anchor_id"]), attr_text=f"{quote_sheetname(sheet_name)}!{coord}"))


def _neutralize_dynamic_headers(wb: Workbook) -> None:
    if "Valuation" in wb.sheetnames:
        ws = wb["Valuation"]
        if isinstance(ws["O7"].value, str) and ws["O7"].value.startswith("Guidance"):
            ws["O7"] = "Guidance"


def _ensure_qa_headers(wb: Workbook) -> None:
    for sheet_name in ("QA_Log", "Needs_Review", "QA_Checks"):
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        ws["A1"] = _sheet_title(sheet_name)
        for col_idx, header in enumerate(QA_HEADERS, start=2):
            ws.cell(1, col_idx, header)


def _remove_qa_excel_tables(wb: Workbook) -> None:
    for sheet_name in ("QA_Log", "Needs_Review", "QA_Checks"):
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for table_name in list(ws.tables.keys()):
            del ws.tables[table_name]


def _clear_workbook_comments(wb: Workbook) -> None:
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if isinstance(cell, MergedCell):
                    continue
                if cell.comment is not None:
                    cell.comment = None


def _sheet_refs(text: str) -> set[str]:
    refs: set[str] = set()
    for match in SHEET_REF_RE.finditer(text):
        ref = (match.group(1) or match.group(2) or "").strip()
        if ref:
            refs.add(ref)
    return refs


def _remove_defined_names_pointing_to_missing_sheets(wb: Workbook) -> None:
    sheet_names = set(wb.sheetnames)
    for name in list(wb.defined_names):
        defined_name = wb.defined_names[name]
        text = getattr(defined_name, "attr_text", "") or str(defined_name)
        if any(ref not in sheet_names for ref in _sheet_refs(text)):
            del wb.defined_names[name]


def _neutralize_support_sheet(ws: Any, headers: list[str]) -> None:
    for table_name in list(ws.tables.keys()):
        del ws.tables[table_name]
    for merged_range in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merged_range))
    if getattr(ws, "data_validations", None) is not None:
        ws.data_validations.dataValidation = []
    if getattr(ws, "conditional_formatting", None) is not None:
        ws.conditional_formatting._cf_rules.clear()
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            cell.value = None
            cell.comment = None
    for col_idx, header in enumerate(headers, start=1):
        ws.cell(1, col_idx, header)
    ws.sheet_state = "hidden"


def _neutralize_hidden_support_sheets(wb: Workbook, manifest: dict[str, Any]) -> None:
    standard_visible = {str(sheet_name) for sheet_name in manifest["visible_sheet_order"]}
    for sheet_name in list(wb.sheetnames):
        if sheet_name in standard_visible:
            continue
        if sheet_name not in ALLOWED_HIDDEN_SHELL_SHEETS:
            del wb[sheet_name]
            continue
        _neutralize_support_sheet(wb[sheet_name], SUPPORT_SHEET_HEADERS[sheet_name])

    for sheet_name in SUPPORT_SHEET_HEADERS:
        if sheet_name in wb.sheetnames:
            wb[sheet_name].sheet_state = "hidden"
            continue
        ws = wb.create_sheet(sheet_name)
        _neutralize_support_sheet(ws, SUPPORT_SHEET_HEADERS[sheet_name])

    _remove_defined_names_pointing_to_missing_sheets(wb)


def _ensure_freeze_panes(wb: Workbook, manifest: dict[str, Any]) -> None:
    for sheet_name in manifest["visible_sheet_order"]:
        if sheet_name in wb.sheetnames and not wb[sheet_name].freeze_panes:
            wb[sheet_name].freeze_panes = _fallback_freeze(str(sheet_name))


def _configure_calculation(wb: Workbook) -> None:
    try:
        wb.calculation.calcMode = "auto"
        wb.calculation.fullCalcOnLoad = True
        wb.calculation.forceFullCalc = True
    except Exception:
        pass


def _materialize_rich_shell(
    *,
    source_path: Path,
    output_path: Path,
    manifest: dict[str, Any],
    bindings: list[dict[str, Any]],
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, output_path)
    wb = load_workbook(output_path, data_only=False, read_only=False)
    _rename_investment_case_sheet(wb)
    _hide_nonstandard_sheets(wb, manifest)
    _clear_source_specific_visible_text(wb, manifest)
    _genericize_sector_specific_visible_text(wb, manifest)
    _clear_writable_zones(wb, manifest)
    _clear_valuation_numeric_constants(wb)
    _clear_valuation_runtime_value_constants(wb)
    _guard_valuation_scenario_formulas_until_input(wb)
    _neutralize_writable_data_like_fills(wb, manifest)
    _neutralize_visible_blank_gray_fills(wb)
    _neutralize_valuation_signal_fills(wb)
    _neutralize_red_green_flags(wb)
    _neutralize_blank_valuation_value_fills(wb)
    _ensure_promise_progress_structure(wb)
    _ensure_valuation_guidance_sidecar_headers(wb)
    _ensure_operating_driver_sheet_headers(wb)
    _neutralize_remaining_visible_row_labels(wb)
    _ensure_sheet_titles(wb, manifest)
    _move_operating_drivers_title_to_row1(wb)
    _neutralize_dynamic_headers(wb)
    _ensure_binding_anchor_labels(wb, manifest, bindings)
    _ensure_qa_headers(wb)
    _remove_qa_excel_tables(wb)
    _clear_workbook_comments(wb)
    _neutralize_hidden_support_sheets(wb, manifest)
    _ensure_freeze_panes(wb, manifest)
    _configure_calculation(wb)
    wb.save(output_path)
    wb.close()
    return output_path


def materialize_shell(
    *,
    data_root: Path,
    output_path: Path,
    manifest_path: Path,
    binding_map_path: Path,
) -> Path:
    manifest = _load_json(manifest_path)
    binding_payload = _load_json(binding_map_path)
    bindings = list(binding_payload.get("bindings") or [])

    rich_source = _rich_template_source(data_root)
    if rich_source is not None:
        return _materialize_rich_shell(
            source_path=rich_source,
            output_path=output_path,
            manifest=manifest,
            bindings=bindings,
        )

    contracts = _load_source_contracts(data_root, manifest)

    wb = Workbook()
    default = wb.active
    wb.remove(default)
    wb._standard_template_required_anchors = list(manifest.get("required_anchors") or [])  # type: ignore[attr-defined]

    for sheet_def in manifest["sheets"]:
        sheet_name = str(sheet_def["sheet"])
        ws = wb.create_sheet(sheet_name)
        sheet_bindings = [entry for entry in bindings if entry["sheet"] == sheet_name]
        _write_static_structure(wb, ws, sheet_def, sheet_bindings, contracts.get(sheet_name, SourceSheetContract(None, {}, {})))

    _configure_calculation(wb)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--manifest", type=Path, default=ROOT / "docs" / "standard_template_shell_manifest.json")
    parser.add_argument("--binding-map", type=Path, default=ROOT / "docs" / "workbook_binding_map.json")
    args = parser.parse_args()

    path = materialize_shell(
        data_root=args.data_root.expanduser().resolve(),
        output_path=args.output.expanduser().resolve(),
        manifest_path=args.manifest.expanduser().resolve(),
        binding_map_path=args.binding_map.expanduser().resolve(),
    )
    print(f"standard template shell: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
