"""Targeted final investor polish for the accepted ANF Valuation preview.

This bridge owns presentation only.  It reuses the accepted source-native
Capital Allocation / Capital Return values, remaps their workbook coordinates
without changing economics, and activates a current-market presentation block
from the already accepted manual current-price input.  The implementation
patches only explicit OOXML members and never routes XLSX authoring through the
artifact inspection/rendering tool.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from html import escape, unescape
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _cell_elements,
    _set_attribute,
    _sheet_part_map,
    canonical_ooxml_sha256,
    sha256_file,
)
from pbi_xbrl.longitudinal_memory.valuation_final_layout_cleanup import (
    LINEAGE_SUPPORT_SHEET,
    NORMAL_VALUATION_ROW_HEIGHT,
    _attributes,
    _column_name,
    _comment_refs,
    _inline_text,
    _patch_comments,
    _patch_inline_text,
    _patch_vml,
    _replace_cells,
    _write_package_with_addition,
)


POLISH_CONTRACT = "valuation-final-investor-polish@1"
SEMANTIC_SNAPSHOT_CONTRACT = "valuation-final-investor-polish-semantic-snapshot@1"
EXPECTED_BASE_WORKBOOK_SHA256 = (
    "db99549c6504a185b900daaa14d06397ff8406727b00a698cac64c8bd754d147"
)
VALUATION_SHEET = "Valuation"
VALUATION_PART = "xl/worksheets/sheet2.xml"
STYLES_PART = "xl/styles.xml"
COMMENTS_PART = "xl/comments/comment2.xml"
COMMENTS_VML_PART = "xl/drawings/commentsDrawing2.vml"
FINAL_VALUATION_DIMENSION = "A1:AI175"
FINAL_VISIBLE_PRODUCT_ROW = 175
ORIGINAL_EMPTY_TAIL_START_ROW = 167
DEBT_HEADER_ROW_HEIGHT = NORMAL_VALUATION_ROW_HEIGHT
INVESTOR_SECTION_SPACER_ROLE = "investor_section_spacer"
SUBSECTION_FILL_RGB = "C6DBEE"
VALUATION_COLUMN_WIDTH = 13.8571428571
VALUATION_COLUMN_WIDTH_PIXELS = 102
REMOVED_COMMENT_REFS = ("O48", "X49")

MARKET_PRICE_OWNER = {
    "classification": "DECLARATIVE_CURRENT_PRICE_INPUT_EXISTS",
    "input_cell": "ANF_Investment_Case!F15",
    "resolved_cell": "ANF_Investment_Case!G15",
    "input_label_cell": "ANF_Investment_Case!A15",
    "ownership": "existing explicit manual current-market-price input",
}

MARKET_FORMULAS = {
    "B117": "=IF(ISNUMBER('ANF_Investment_Case'!$G$15),'ANF_Investment_Case'!$G$15,\"\")",
    "B118": "=IF(AND(ISNUMBER(B117),ISNUMBER($M$102)),B117*$M$102,\"\")",
    "B119": "=IF(AND(ISNUMBER(B118),ISNUMBER($M$78)),B118-$M$78,\"\")",
    "B120": "=IFERROR(IF(OR(B119=\"\",NOT(ISNUMBER($M$84)),$M$84=0),\"\",B119/$M$84),\"\")",
    "B121": "=IFERROR(IF(OR(B119=\"\",NOT(ISNUMBER($M$85)),$M$85=0),\"\",B119/$M$85),\"\")",
    "B122": "=IFERROR(IF(OR(B118=\"\",NOT(ISNUMBER($M$49)),B118=0),\"\",$M$49/B118),\"\")",
    "B123": "=IFERROR(IF(OR(B119=\"\",NOT(ISNUMBER($M$49)),B119=0),\"\",$M$49/B119),\"\")",
}

MARKET_LABELS = {
    117: "Current share price ($)",
    118: "Market capitalization ($m)",
    119: "Enterprise value ($m)",
    120: "EV / EBITDA (TTM)",
    121: "EV / Adj. EBITDA (TTM)",
    122: "FCF yield (TTM, equity)",
    123: "FCF yield (TTM, EV)",
}

DEBT_HEADER_LABELS = {
    "A": "Year / Label",
    "B": "Principal due ($m)",
    "D": "Rate type",
    "E": "Coupon / Spread %",
    "G": "Maturity",
    "H": "Conversion price",
    "J": "Added shares on full conversion (m)",
    "M": "Concurrent repurchased shares (m)",
}
DEBT_HEADER_MERGES = ("B126:C126", "E126:F126", "H126:I126", "J126:L126", "M126:O126")

# Old accepted rows are copied to these final presentation rows.  The omitted
# rows are the deliberate normal-height spacers described by the contract.
CAPITAL_ROW_MAP = {
    126: 130,
    127: 131,
    128: 132,
    129: 133,
    130: 134,
    131: 135,
    132: 137,
    133: 138,
    134: 139,
    135: 140,
    136: 141,
    137: 142,
    138: 143,
    140: 145,
    141: 146,
    142: 147,
    143: 148,
    144: 149,
    145: 150,
    149: 151,
    146: 153,
    147: 154,
    148: 156,
    150: 157,
    151: 158,
    152: 159,
    153: 160,
    154: 161,
    155: 162,
    156: 164,
    157: 165,
    158: 166,
    159: 167,
    160: 168,
    161: 169,
    162: 170,
    163: 171,
    164: 173,
    165: 174,
    166: 175,
}
ROW_REMAP = dict(CAPITAL_ROW_MAP)
SPACER_ROWS = (124, 129, 136, 144, 152, 155, 163, 172)
SUBSECTION_ROWS = (131, 138, 146, 158, 167)
PERIOD_HEADER_ROWS = {
    132: ("B", "C", "D"),
    139: ("B", "C", "D", "E", "F"),
    147: ("B", "C", "D"),
    159: ("B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"),
    168: ("B", "C"),
}
ANNUAL_HEADER_ROWS = (139, 168)
ADDITIONAL_NUMERIC_YEAR_HEADER_CELLS = ("D132", "D147")

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_ROW_RE = re.compile(
    rb"<row\b[^>]*\br=([\"'])(?P<row>[1-9][0-9]*)\1[^>]*(?:/>|>.*?</row>)",
    re.DOTALL,
)
_MERGE_CONTAINER_RE = re.compile(rb"<mergeCells\b[^>]*>(?P<body>.*?)</mergeCells>", re.DOTALL)
_MERGE_RE = re.compile(rb"<mergeCell\b[^>]*/>")
_COLS_RE = re.compile(rb"<cols>.*?</cols>", re.DOTALL)
_COL_RE = re.compile(rb"<col\b[^>]*/>")
_FILLS_RE = re.compile(rb"<fills\b[^>]*>(?P<body>.*?)</fills>", re.DOTALL)
_CELL_XFS_RE = re.compile(rb"<cellXfs\b[^>]*>(?P<body>.*?)</cellXfs>", re.DOTALL)
_FORMULA_RE = re.compile(rb"<f(?:\s[^>]*)?>(.*?)</f>", re.DOTALL)
_TARGET_RE = re.compile(r"Valuation!([A-Z]+)([1-9][0-9]*)")


class ValuationFinalInvestorPolishError(ValueError):
    """Fail-closed bounded-polish contract violation."""


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


def _row_elements(data: bytes) -> dict[int, tuple[int, int, bytes]]:
    return {
        int(match.group("row")): (match.start(), match.end(), match.group(0))
        for match in _ROW_RE.finditer(data)
    }


def _formula_text(raw_cell: bytes) -> str | None:
    match = _FORMULA_RE.search(raw_cell)
    return None if match is None else unescape(match.group(1).decode("utf-8"))


def _xf_elements(body: bytes) -> list[bytes]:
    """Return direct cellXfs children without joining adjacent self-closing XFs."""

    result: list[bytes] = []
    cursor = 0
    while True:
        start = body.find(b"<xf", cursor)
        if start < 0:
            break
        tag_end = body.find(b">", start)
        if tag_end < 0:
            raise ValuationFinalInvestorPolishError("Malformed cellXfs entry.")
        if body[tag_end - 1 : tag_end] == b"/":
            end = tag_end + 1
        else:
            close = body.find(b"</xf>", tag_end + 1)
            if close < 0:
                raise ValuationFinalInvestorPolishError("Unclosed cellXfs entry.")
            end = close + len(b"</xf>")
        result.append(body[start:end])
        cursor = end
    return result


def _patch_formula_cell(raw_cell: bytes, *, formula: str, style_id: int) -> bytes:
    if not formula.startswith("="):
        raise ValuationFinalInvestorPolishError("Formula must use workbook notation.")
    end = raw_cell.find(b">")
    if end < 0:
        raise ValuationFinalInvestorPolishError("Malformed target cell.")
    start = raw_cell[: end + 1]
    if start.endswith(b"/>"):
        start = start[:-2] + b">"
    start = _set_attribute(start, "s", str(style_id))
    start = _set_attribute(start, "t", None)
    payload = escape(formula[1:], quote=False).encode("utf-8")
    return start + b"<f>" + payload + b"</f></c>"


def _inline_cell(coordinate: str, value: str, style_id: int) -> bytes:
    payload = escape(value, quote=False).encode("utf-8")
    return (
        f'<c r="{coordinate}" s="{style_id}" t="inlineStr"><is><t>'.encode("utf-8")
        + payload
        + b"</t></is></c>"
    )


def _blank_cell(coordinate: str, style_id: int) -> bytes:
    return f'<c r="{coordinate}" s="{style_id}" t="n"></c>'.encode("utf-8")


def _numeric_cell(coordinate: str, value: int, style_id: int) -> bytes:
    return f'<c r="{coordinate}" s="{style_id}" t="n"><v>{value}</v></c>'.encode("utf-8")


def _row_xml(row: int, cells: list[bytes], *, height: float = NORMAL_VALUATION_ROW_HEIGHT) -> bytes:
    return (
        f'<row r="{row}" ht="{height:.15g}" customHeight="1">'.encode("utf-8")
        + b"".join(cells)
        + b"</row>"
    )


def _spacer_row(row: int) -> bytes:
    return _row_xml(row, [], height=NORMAL_VALUATION_ROW_HEIGHT)


def _readdress_row(raw: bytes, *, old_row: int, new_row: int) -> bytes:
    end = raw.find(b">")
    if end < 0:
        raise ValuationFinalInvestorPolishError("Malformed worksheet row.")
    start = _set_attribute(raw[: end + 1], "r", str(new_row))
    body = raw[end + 1 :]
    pattern = re.compile(rb'(?P<prefix>\br=["\'])(?P<column>[A-Z]+)' + str(old_row).encode() + rb'(?P<quote>["\'])')
    body, count = pattern.subn(
        lambda match: match.group("prefix")
        + match.group("column")
        + str(new_row).encode()
        + match.group("quote"),
        body,
    )
    if count == 0:
        raise ValuationFinalInvestorPolishError(f"Row {old_row} has no owned cells.")
    return start + body


def _patch_row_cell_styles(raw_row: bytes, style_by_coordinate: Mapping[str, int]) -> bytes:
    cells = _cell_elements(raw_row)
    replacements: dict[str, bytes] = {}
    for coordinate, style_id in style_by_coordinate.items():
        located = cells.get(coordinate)
        if located is None:
            raise ValuationFinalInvestorPolishError(f"Missing styled cell {coordinate}.")
        raw = located[2]
        end = raw.find(b">")
        start = _set_attribute(raw[: end + 1], "s", str(style_id))
        replacements[coordinate] = start + raw[end + 1 :]
    return _replace_cells(raw_row, replacements)


def _xf_variant(
    raw: bytes,
    *,
    fill_id: int | None = None,
    num_fmt_id: int | None = None,
    alignment: Mapping[str, str] | None = None,
) -> bytes:
    if not raw.rstrip().endswith(b"/>"):
        raise ValuationFinalInvestorPolishError("Expected a self-closing base cell style.")
    result = raw
    if fill_id is not None:
        result = _set_attribute(result, "fillId", str(fill_id))
        result = _set_attribute(result, "applyFill", "1")
    if num_fmt_id is not None:
        result = _set_attribute(result, "numFmtId", str(num_fmt_id))
        result = _set_attribute(result, "applyNumberFormat", "1")
    if alignment:
        result = _set_attribute(result, "applyAlignment", "1")
        result = result.rstrip()[:-2] + b">"
        attributes = " ".join(f'{key}="{value}"' for key, value in alignment.items())
        result += f"<alignment {attributes}/></xf>".encode("utf-8")
    return result


@dataclass(frozen=True)
class StylePlan:
    subsection_anchor: int
    subsection_filler: int
    table_left: int
    table_right: int
    table_right_integer: int
    debt_header: int
    market_price: int
    new_fill_id: int
    original_style_count: int


def _style_plan(styles: bytes) -> StylePlan:
    fills = _FILLS_RE.search(styles)
    cell_xfs = _CELL_XFS_RE.search(styles)
    if fills is None or cell_xfs is None:
        raise ValuationFinalInvestorPolishError("Workbook style containers are missing.")
    fill_id = int(_attributes(fills.group(0)).get("count", "0"))
    xfs = _xf_elements(cell_xfs.group("body"))
    if len(xfs) < 360:
        raise ValuationFinalInvestorPolishError("Workbook style universe changed.")
    base = len(xfs)
    return StylePlan(
        subsection_anchor=base,
        subsection_filler=base + 1,
        table_left=base + 2,
        table_right=base + 3,
        table_right_integer=base + 4,
        debt_header=base + 5,
        market_price=base + 6,
        new_fill_id=fill_id,
        original_style_count=base,
    )


def _patch_styles(styles: bytes, plan: StylePlan) -> bytes:
    fills = _FILLS_RE.search(styles)
    cell_xfs = _CELL_XFS_RE.search(styles)
    if fills is None or cell_xfs is None:
        raise ValuationFinalInvestorPolishError("Workbook style containers are missing.")
    xfs = _xf_elements(cell_xfs.group("body"))
    if len(xfs) != plan.original_style_count:
        raise ValuationFinalInvestorPolishError("Style count changed after planning.")
    fill = (
        f'<fill><patternFill patternType="solid"><fgColor rgb="00{SUBSECTION_FILL_RGB}"/>'
        f"</patternFill></fill>"
    ).encode("utf-8")
    fill_start_end = fills.group(0).find(b">")
    fill_start = _set_attribute(
        fills.group(0)[: fill_start_end + 1], "count", str(plan.new_fill_id + 1)
    )
    fill_replacement = fill_start + fills.group("body") + fill + b"</fills>"
    styles = styles[: fills.start()] + fill_replacement + styles[fills.end() :]

    cell_xfs = _CELL_XFS_RE.search(styles)
    assert cell_xfs is not None
    additions = (
        _xf_variant(xfs[38], fill_id=plan.new_fill_id),
        _xf_variant(xfs[39], fill_id=plan.new_fill_id),
        _xf_variant(xfs[91], alignment={"horizontal": "left", "vertical": "center"}),
        _xf_variant(xfs[91], alignment={"horizontal": "right", "vertical": "center"}),
        _xf_variant(
            xfs[91],
            num_fmt_id=1,
            alignment={"horizontal": "right", "vertical": "center"},
        ),
        _xf_variant(
            xfs[91],
            alignment={"horizontal": "center", "vertical": "center", "wrapText": "1"},
        ),
        _xf_variant(xfs[62], num_fmt_id=170),
    )
    start_end = cell_xfs.group(0).find(b">")
    start = _set_attribute(
        cell_xfs.group(0)[: start_end + 1],
        "count",
        str(plan.original_style_count + len(additions)),
    )
    replacement = start + cell_xfs.group("body") + b"".join(additions) + b"</cellXfs>"
    return styles[: cell_xfs.start()] + replacement + styles[cell_xfs.end() :]


def _patch_columns(data: bytes) -> bytes:
    container = _COLS_RE.search(data)
    if container is None:
        raise ValuationFinalInvestorPolishError("Valuation lacks column metadata.")
    parts: list[bytes] = []
    cursor = 0
    changed: set[int] = set()
    body = container.group(0)
    for match in _COL_RE.finditer(body):
        raw = match.group(0)
        attrs = _attributes(raw)
        minimum = int(attrs["min"])
        maximum = int(attrs["max"])
        parts.append(body[cursor : match.start()])
        if 2 <= minimum <= maximum <= 13:
            raw = _set_attribute(raw, "width", f"{VALUATION_COLUMN_WIDTH:.10f}")
            raw = _set_attribute(raw, "customWidth", "1")
            changed.update(range(minimum, maximum + 1))
        parts.append(raw)
        cursor = match.end()
    parts.append(body[cursor:])
    if changed != set(range(2, 14)):
        raise ValuationFinalInvestorPolishError("B:M column ownership changed.")
    replacement = b"".join(parts)
    return data[: container.start()] + replacement + data[container.end() :]


def _patch_dimension(data: bytes) -> bytes:
    match = re.search(rb"<dimension\b[^>]*/>", data)
    if match is None:
        raise ValuationFinalInvestorPolishError("Valuation lacks dimension metadata.")
    current = _attributes(match.group(0)).get("ref")
    if current != "A1:AI166":
        raise ValuationFinalInvestorPolishError(f"Valuation dimension changed: {current}.")
    replacement = _set_attribute(match.group(0), "ref", FINAL_VALUATION_DIMENSION)
    return data[: match.start()] + replacement + data[match.end() :]


def _patch_merges(data: bytes) -> bytes:
    container = _MERGE_CONTAINER_RE.search(data)
    if container is None:
        raise ValuationFinalInvestorPolishError("Valuation lacks merge metadata.")
    remap = {
        "A126:M126": "A130:M130",
        "A127:M127": "A131:M131",
        "A133:M133": "A138:M138",
        "A140:M140": "A145:M145",
        "A141:M141": "A146:M146",
        "A151:M151": "A158:M158",
        "A159:M159": "A167:M167",
    }
    remove = {"G123:H123", "I123:K123", "L123:N123"}
    retained: list[bytes] = []
    found_remap: set[str] = set()
    found_remove: set[str] = set()
    for match in _MERGE_RE.finditer(container.group("body")):
        raw = match.group(0)
        reference = _attributes(raw).get("ref", "")
        if reference in remove:
            found_remove.add(reference)
            continue
        if reference in remap:
            raw = _set_attribute(raw, "ref", remap[reference])
            found_remap.add(reference)
        retained.append(raw)
    if found_remap != set(remap) or found_remove != remove:
        raise ValuationFinalInvestorPolishError("Accepted merge plan drifted.")
    retained.extend(
        f'<mergeCell ref="{reference}"/>'.encode("utf-8")
        for reference in DEBT_HEADER_MERGES
    )
    start_end = container.group(0).find(b">")
    start = _set_attribute(container.group(0)[: start_end + 1], "count", str(len(retained)))
    replacement = start + b"".join(retained) + b"</mergeCells>"
    return data[: container.start()] + replacement + data[container.end() :]


def _market_rows(style: StylePlan) -> dict[int, bytes]:
    header_cells = [_inline_cell("A116", "Market Valuation", 96)]
    header_cells.extend(_blank_cell(f"{_column_name(column)}116", 104) for column in range(2, 13))
    header_cells.append(_blank_cell("M116", 105))
    rows = {116: _row_xml(116, header_cells, height=21.0)}
    formula_styles = {
        117: style.market_price,
        118: 62,
        119: 62,
        120: 359,
        121: 359,
        122: 59,
        123: 59,
    }
    for row, label in MARKET_LABELS.items():
        formula = MARKET_FORMULAS[f"B{row}"]
        template = _blank_cell(f"B{row}", formula_styles[row])
        value_cell = _patch_formula_cell(template, formula=formula, style_id=formula_styles[row])
        rows[row] = _row_xml(row, [_inline_cell(f"A{row}", label, 48), value_cell])
    return rows


def _debt_header_row(style: StylePlan) -> bytes:
    cells = []
    for column in range(1, 16):
        letter = _column_name(column)
        coordinate = f"{letter}126"
        label = DEBT_HEADER_LABELS.get(letter)
        cells.append(
            _inline_cell(coordinate, label, style.debt_header)
            if label is not None
            else _blank_cell(coordinate, style.debt_header)
        )
    return _row_xml(126, cells, height=DEBT_HEADER_ROW_HEIGHT)


def _debt_section_row(raw: bytes) -> bytes:
    result = _readdress_row(raw, old_row=122, new_row=125)
    cells = _cell_elements(result)
    if "M125" not in cells or any(coordinate in cells for coordinate in ("N125", "O125")):
        raise ValuationFinalInvestorPolishError("Debt section width baseline changed.")
    style_id = int(_attributes(cells["M125"][2]).get("s", "0"))
    close = result.rfind(b"</row>")
    if close < 0:
        raise ValuationFinalInvestorPolishError("Debt section row is malformed.")
    later_cell_starts = []
    for coordinate, (start, _, _) in cells.items():
        letters = re.match(r"[A-Z]+", coordinate)
        if letters is None:
            raise ValuationFinalInvestorPolishError("Debt section cell is malformed.")
        column_index = 0
        for letter in letters.group(0):
            column_index = column_index * 26 + ord(letter) - ord("A") + 1
        if column_index > 15:
            later_cell_starts.append(start)
    insert_at = min(later_cell_starts, default=close)
    extension = _blank_cell("N125", style_id) + _blank_cell("O125", style_id)
    return result[:insert_at] + extension + result[insert_at:]


def _patch_capital_row(raw: bytes, *, old_row: int, new_row: int, style: StylePlan) -> bytes:
    result = _readdress_row(raw, old_row=old_row, new_row=new_row)
    if new_row in SUBSECTION_ROWS:
        result = _patch_row_cell_styles(
            result,
            {
                f"{_column_name(column)}{new_row}": (
                    style.subsection_anchor if column == 1 else style.subsection_filler
                )
                for column in range(1, 14)
            },
        )
    if new_row in PERIOD_HEADER_ROWS:
        cells = _cell_elements(result)
        replacements: dict[str, bytes] = {}
        metric = f"A{new_row}"
        raw_metric = cells[metric][2]
        end = raw_metric.find(b">")
        replacements[metric] = (
            _set_attribute(raw_metric[: end + 1], "s", str(style.table_left))
            + raw_metric[end + 1 :]
        )
        for column in PERIOD_HEADER_ROWS[new_row]:
            coordinate = f"{column}{new_row}"
            raw_cell = cells[coordinate][2]
            if new_row in ANNUAL_HEADER_ROWS or coordinate in ADDITIONAL_NUMERIC_YEAR_HEADER_CELLS:
                label = _inline_text(raw_cell)
                if not re.fullmatch(r"[0-9]{4}", label):
                    raise ValuationFinalInvestorPolishError(
                        f"Numeric year header {coordinate} is not a four-digit year."
                    )
                replacements[coordinate] = _numeric_cell(
                    coordinate, int(label), style.table_right_integer
                )
            else:
                end = raw_cell.find(b">")
                replacements[coordinate] = (
                    _set_attribute(raw_cell[: end + 1], "s", str(style.table_right))
                    + raw_cell[end + 1 :]
                )
        result = _replace_cells(result, replacements)
    return result


def _patch_valuation(data: bytes, style: StylePlan) -> bytes:
    rows = _row_elements(data)
    if not all(row in rows for row in range(116, FINAL_VISIBLE_PRODUCT_ROW + 1)):
        raise ValuationFinalInvestorPolishError("Accepted Valuation block is incomplete.")
    # The accepted shell carries normal-height, cell-free placeholder rows after
    # its former visible product edge (167:200).  The final product expands into
    # 167:175, so those nine placeholder row records must be consumed together
    # with the old product block.  Leaving them behind creates duplicate,
    # non-monotonic row records that Excel repairs as invalid cell information.
    for row in range(ORIGINAL_EMPTY_TAIL_START_ROW, FINAL_VISIBLE_PRODUCT_ROW + 1):
        raw = rows[row][2]
        row_start_end = raw.find(b">")
        attributes = _attributes(raw[: row_start_end + 1])
        if (
            _cell_elements(raw)
            or float(attributes.get("ht", "nan")) != NORMAL_VALUATION_ROW_HEIGHT
            or attributes.get("hidden") in {"1", "true"}
        ):
            raise ValuationFinalInvestorPolishError(
                f"Valuation placeholder row {row} is no longer safely replaceable."
            )
    output_rows: dict[int, bytes] = _market_rows(style)
    output_rows[124] = _spacer_row(124)
    output_rows[125] = _debt_section_row(rows[122][2])
    output_rows[126] = _debt_header_row(style)
    output_rows[127] = _readdress_row(rows[124][2], old_row=124, new_row=127)
    output_rows[128] = _readdress_row(rows[125][2], old_row=125, new_row=128)
    output_rows[129] = _spacer_row(129)
    for old_row, new_row in CAPITAL_ROW_MAP.items():
        output_rows[new_row] = _patch_capital_row(
            rows[old_row][2], old_row=old_row, new_row=new_row, style=style
        )
    for row in SPACER_ROWS:
        output_rows[row] = _spacer_row(row)
    if set(output_rows) != set(range(116, FINAL_VISIBLE_PRODUCT_ROW + 1)):
        missing = sorted(set(range(116, FINAL_VISIBLE_PRODUCT_ROW + 1)) - set(output_rows))
        raise ValuationFinalInvestorPolishError(f"Final row plan is incomplete: {missing}.")
    replacement = b"".join(output_rows[row] for row in sorted(output_rows))
    start = rows[116][0]
    end = rows[FINAL_VISIBLE_PRODUCT_ROW][1]
    data = data[:start] + replacement + data[end:]
    data = _patch_columns(data)
    data = _patch_merges(data)
    return _patch_dimension(data)


def _remap_lineage(data: bytes) -> tuple[bytes, tuple[str, ...], int]:
    cells = _cell_elements(data)
    replacements: dict[str, bytes] = {}
    records: list[str] = []
    remapped = 0
    for row in range(1, 29):
        coordinate = f"A{row}"
        raw = cells.get(coordinate)
        if raw is None:
            raise ValuationFinalInvestorPolishError(f"Missing lineage record {coordinate}.")
        current = _inline_text(raw[2])

        def replace(match: re.Match[str]) -> str:
            nonlocal remapped
            old_row = int(match.group(2))
            if old_row not in ROW_REMAP:
                return match.group(0)
            remapped += 1
            return f"Valuation!{match.group(1)}{ROW_REMAP[old_row]}"

        updated = _TARGET_RE.sub(replace, current)
        replacements[coordinate] = _patch_inline_text(
            raw[2], expected=current, replacement=updated
        )
        records.append(updated)
    if remapped != 140:
        raise ValuationFinalInvestorPolishError(
            f"Expected 140 remapped capital bindings, found {remapped}."
        )
    result = _replace_cells(data, replacements)
    return result, tuple(records), remapped


def _binding_inventory(records: tuple[str, ...]) -> tuple[list[dict[str, Any]], str]:
    bindings: list[dict[str, Any]] = []
    for record in records:
        payload = json.loads(record)
        values = payload.get("bindings")
        if not isinstance(values, list):
            raise ValuationFinalInvestorPolishError("Lineage record lost bindings.")
        bindings.extend(values)
    if len(bindings) != 140:
        raise ValuationFinalInvestorPolishError("Capital binding universe changed from 140.")
    if sum(item.get("status") == "available" for item in bindings) != 110:
        raise ValuationFinalInvestorPolishError("Available capital binding count changed from 110.")
    return bindings, _digest(bindings)


@dataclass(frozen=True)
class ValuationFinalInvestorPolishPlan:
    contract: str
    base_workbook_sha256: str
    comment_removals: tuple[str, ...]
    current_price_owner: Mapping[str, Any]
    market_formula_map: Mapping[str, str]
    market_formula_digest: str
    market_disposition: str
    capital_row_map: Mapping[int, int]
    spacer_rows: tuple[int, ...]
    investor_section_spacer_role: str
    column_width: float
    column_width_pixels: int
    subsection_fill_rgb: str
    debt_header_merges: tuple[str, ...]
    style_plan: StylePlan
    prior_binding_plan_digest: str
    remapped_binding_plan_digest: str
    plan_digest: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["capital_row_map"] = {
            str(key): value for key, value in sorted(self.capital_row_map.items())
        }
        return result


@dataclass(frozen=True)
class ValuationFinalInvestorPolishResult:
    contract: str
    plan_digest: str
    base_workbook_sha256: str
    output_workbook_sha256: str
    canonical_ooxml_contract: str
    canonical_ooxml_sha256: str
    changed_ooxml_parts: tuple[str, ...]
    unchanged_ooxml_part_count: int
    valuation_dimension: str
    remapped_binding_count: int
    remapped_binding_plan_digest: str
    valuation_formula_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_valuation_final_investor_polish_plan(
    *, base_workbook: Path | str
) -> ValuationFinalInvestorPolishPlan:
    base = Path(base_workbook)
    base_hash = sha256_file(base)
    if base_hash != EXPECTED_BASE_WORKBOOK_SHA256:
        raise ValuationFinalInvestorPolishError(
            f"Accepted final-layout preview changed: {base_hash}."
        )
    with ZipFile(base, "r") as archive:
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
        sheet_parts = _sheet_part_map(archive)
    if sheet_parts.get(VALUATION_SHEET) != VALUATION_PART:
        raise ValuationFinalInvestorPolishError("Valuation sheet ownership changed.")
    support_part = sheet_parts.get(LINEAGE_SUPPORT_SHEET)
    if support_part is None:
        raise ValuationFinalInvestorPolishError("Capital lineage support sheet is missing.")
    valuation = members[VALUATION_PART]
    cells = _cell_elements(valuation)
    if _inline_text(cells["A116"][2]) != "Market-linked — unavailable (current price not populated)":
        raise ValuationFinalInvestorPolishError("Market-linked baseline changed.")
    if sum(1 for match in _FORMULA_RE.finditer(valuation)) != 0:
        raise ValuationFinalInvestorPolishError("Valuation unexpectedly gained formulas.")
    comments = _comment_refs(members[COMMENTS_PART])
    if not all(reference in comments for reference in REMOVED_COMMENT_REFS):
        raise ValuationFinalInvestorPolishError("Expected legacy comments are missing.")
    nearby = tuple(
        reference
        for reference in comments
        if reference in REMOVED_COMMENT_REFS
        or (reference.startswith("O") and 48 <= int(reference[1:]) <= 49)
        or (reference.startswith("X") and 48 <= int(reference[1:]) <= 49)
    )
    if set(nearby) != set(REMOVED_COMMENT_REFS):
        raise ValuationFinalInvestorPolishError("Nearby retired-comment surface changed.")

    ic_part = sheet_parts.get("ANF_Investment_Case")
    if ic_part is None:
        raise ValuationFinalInvestorPolishError("Investment Case sheet is missing.")
    ic_cells = _cell_elements(members[ic_part])
    if _inline_text(ic_cells["A15"][2]) != "Current share price":
        raise ValuationFinalInvestorPolishError("Current-price input label changed.")
    if _formula_text(ic_cells["G15"][2]) != 'IF(F15<>"",F15,"")':
        raise ValuationFinalInvestorPolishError("Resolved current-price contract changed.")
    if any(token in ic_cells["F15"][2] for token in (b"<f", b"<v", b"<is")):
        raise ValuationFinalInvestorPolishError("Manual current-price input is no longer blank.")

    _, prior_records, _ = _remap_lineage(members[support_part])
    remapped_bindings, remapped_digest = _binding_inventory(prior_records)
    # Reverse the row mapping to recover the accepted pre-polish binding identity.
    reverse = {new: old for old, new in ROW_REMAP.items()}
    prior_bindings = json.loads(json.dumps(remapped_bindings))
    for binding in prior_bindings:
        target = str(binding["target_cell"])
        match = _TARGET_RE.fullmatch(target)
        if match is None or int(match.group(2)) not in reverse:
            raise ValuationFinalInvestorPolishError("Remapped binding target is invalid.")
        binding["target_cell"] = f"Valuation!{match.group(1)}{reverse[int(match.group(2))]}"
    prior_digest = _digest(prior_bindings)

    style = _style_plan(members[STYLES_PART])
    payload = {
        "base_workbook_sha256": base_hash,
        "capital_row_map": {str(key): value for key, value in sorted(ROW_REMAP.items())},
        "comment_removals": list(REMOVED_COMMENT_REFS),
        "contract": POLISH_CONTRACT,
        "current_price_owner": MARKET_PRICE_OWNER,
        "debt_header_merges": list(DEBT_HEADER_MERGES),
        "numeric_year_header_cells": list(ADDITIONAL_NUMERIC_YEAR_HEADER_CELLS),
        "market_formula_map": MARKET_FORMULAS,
        "remapped_binding_plan_digest": remapped_digest,
        "spacer_rows": list(SPACER_ROWS),
        "style_plan": asdict(style),
        "subsection_fill_rgb": SUBSECTION_FILL_RGB,
        "valuation_column_width": VALUATION_COLUMN_WIDTH,
    }
    return ValuationFinalInvestorPolishPlan(
        contract=POLISH_CONTRACT,
        base_workbook_sha256=base_hash,
        comment_removals=REMOVED_COMMENT_REFS,
        current_price_owner=MARKET_PRICE_OWNER,
        market_formula_map=MARKET_FORMULAS,
        market_formula_digest=_digest(MARKET_FORMULAS),
        market_disposition="ACTIVE_CURRENT_MARKET_PRESENTATION_USING_EXISTING_MANUAL_PRICE_INPUT",
        capital_row_map=ROW_REMAP,
        spacer_rows=SPACER_ROWS,
        investor_section_spacer_role=INVESTOR_SECTION_SPACER_ROLE,
        column_width=VALUATION_COLUMN_WIDTH,
        column_width_pixels=VALUATION_COLUMN_WIDTH_PIXELS,
        subsection_fill_rgb=SUBSECTION_FILL_RGB,
        debt_header_merges=DEBT_HEADER_MERGES,
        style_plan=style,
        prior_binding_plan_digest=prior_digest,
        remapped_binding_plan_digest=remapped_digest,
        plan_digest=_digest(payload),
    )


def materialize_valuation_final_investor_polish(
    *,
    plan: ValuationFinalInvestorPolishPlan,
    base_workbook: Path | str,
    output_workbook: Path | str,
) -> ValuationFinalInvestorPolishResult:
    if plan.contract != POLISH_CONTRACT:
        raise ValuationFinalInvestorPolishError("Polish contract changed.")
    base = Path(base_workbook)
    output = Path(output_workbook)
    if base.resolve() == output.resolve():
        raise ValuationFinalInvestorPolishError("Accepted preview cannot be overwritten.")
    if output.exists():
        raise ValuationFinalInvestorPolishError(f"Refusing to overwrite {output}.")
    if sha256_file(base) != plan.base_workbook_sha256:
        raise ValuationFinalInvestorPolishError("Base workbook changed after planning.")
    with ZipFile(base, "r") as archive:
        original_names = tuple(info.filename for info in archive.infolist())
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
        sheet_parts = _sheet_part_map(archive)
    support_part = sheet_parts[LINEAGE_SUPPORT_SHEET]
    members[STYLES_PART] = _patch_styles(members[STYLES_PART], plan.style_plan)
    members[VALUATION_PART] = _patch_valuation(members[VALUATION_PART], plan.style_plan)
    members[COMMENTS_PART] = _patch_comments(members[COMMENTS_PART], plan.comment_removals)
    members[COMMENTS_VML_PART] = _patch_vml(members[COMMENTS_VML_PART], plan.comment_removals)
    support, records, remapped_count = _remap_lineage(members[support_part])
    members[support_part] = support
    _, binding_digest = _binding_inventory(records)
    if binding_digest != plan.remapped_binding_plan_digest:
        raise ValuationFinalInvestorPolishError("Remapped binding identity changed.")
    _write_package_with_addition(
        base_workbook=base,
        output_workbook=output,
        members=members,
    )
    expected_changed = {
        COMMENTS_PART,
        COMMENTS_VML_PART,
        STYLES_PART,
        VALUATION_PART,
        support_part,
    }
    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if set(before.namelist()) != set(after.namelist()):
            raise ValuationFinalInvestorPolishError("OOXML member inventory changed.")
        changed = tuple(
            sorted(name for name in before.namelist() if before.read(name) != after.read(name))
        )
    if set(changed) != expected_changed:
        raise ValuationFinalInvestorPolishError(
            f"Unexpected changed OOXML parts: {sorted(set(changed) ^ expected_changed)}."
        )
    valuation_formula_count = len(_FORMULA_RE.findall(members[VALUATION_PART]))
    if valuation_formula_count != len(MARKET_FORMULAS):
        raise ValuationFinalInvestorPolishError("Valuation formula inventory changed.")
    return ValuationFinalInvestorPolishResult(
        contract=POLISH_CONTRACT,
        plan_digest=plan.plan_digest,
        base_workbook_sha256=plan.base_workbook_sha256,
        output_workbook_sha256=sha256_file(output),
        canonical_ooxml_contract=CANONICAL_OOXML_HASH_CONTRACT,
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        changed_ooxml_parts=changed,
        unchanged_ooxml_part_count=len(original_names) - len(changed),
        valuation_dimension=FINAL_VALUATION_DIMENSION,
        remapped_binding_count=remapped_count,
        remapped_binding_plan_digest=binding_digest,
        valuation_formula_count=valuation_formula_count,
    )


__all__ = [
    "ADDITIONAL_NUMERIC_YEAR_HEADER_CELLS",
    "ANNUAL_HEADER_ROWS",
    "CAPITAL_ROW_MAP",
    "DEBT_HEADER_LABELS",
    "DEBT_HEADER_MERGES",
    "EXPECTED_BASE_WORKBOOK_SHA256",
    "FINAL_VALUATION_DIMENSION",
    "FINAL_VISIBLE_PRODUCT_ROW",
    "INVESTOR_SECTION_SPACER_ROLE",
    "MARKET_FORMULAS",
    "MARKET_LABELS",
    "MARKET_PRICE_OWNER",
    "NORMAL_VALUATION_ROW_HEIGHT",
    "PERIOD_HEADER_ROWS",
    "POLISH_CONTRACT",
    "REMOVED_COMMENT_REFS",
    "ROW_REMAP",
    "SEMANTIC_SNAPSHOT_CONTRACT",
    "SPACER_ROWS",
    "SUBSECTION_FILL_RGB",
    "SUBSECTION_ROWS",
    "VALUATION_COLUMN_WIDTH",
    "VALUATION_COLUMN_WIDTH_PIXELS",
    "ValuationFinalInvestorPolishError",
    "ValuationFinalInvestorPolishPlan",
    "ValuationFinalInvestorPolishResult",
    "build_valuation_final_investor_polish_plan",
    "materialize_valuation_final_investor_polish",
]
