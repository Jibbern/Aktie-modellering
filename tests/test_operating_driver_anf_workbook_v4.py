from __future__ import annotations

from decimal import Decimal
import hashlib
from pathlib import Path, PurePosixPath
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import pytest

from pbi_xbrl.longitudinal_memory.formula_aware_workbook_materialization import (
    WorksheetMergeMutation,
    _patch_merges,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (
    USED_RANGE,
    ZOOM_SCALE,
    build_operating_driver_anf_workbook_v4_plan,
    materialize_operating_driver_anf_workbook_v4,
)


PROTECTED_ANF = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\ANF_model.xlsx")
PROTECTED_ANF_SHA256 = "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
WORKSHEET_ORDER = (
    "sheetPr", "dimension", "sheetViews", "sheetFormatPr", "cols", "sheetData",
    "sheetCalcPr", "sheetProtection", "protectedRanges", "scenarios", "autoFilter",
    "sortState", "dataConsolidate", "customSheetViews", "mergeCells", "phoneticPr",
    "conditionalFormatting", "dataValidations", "hyperlinks", "printOptions",
    "pageMargins", "pageSetup", "headerFooter", "rowBreaks", "colBreaks",
    "customProperties", "cellWatches", "ignoredErrors", "smartTags", "drawing",
    "legacyDrawing", "legacyDrawingHF", "picture", "oleObjects", "controls",
    "webPublishItems", "tableParts", "extLst",
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _sheet_part(archive: ZipFile) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relations = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relation_map = {item.attrib["Id"]: item.attrib["Target"] for item in relations}
    sheet = next(item for item in workbook.findall("m:sheets/m:sheet", NS) if item.attrib["name"] == "Operating_Drivers")
    target = relation_map[sheet.attrib[f"{{{NS['r']}}}id"]]
    return target.lstrip("/") if target.startswith("/") else f"xl/{target}"


def _target_root(path: Path) -> tuple[str, ET.Element]:
    with ZipFile(path, "r") as archive:
        part = _sheet_part(archive)
        return part, ET.fromstring(archive.read(part))


def _display_text(root: ET.Element) -> dict[str, str]:
    result = {}
    for cell in root.findall(".//m:sheetData/m:row/m:c", NS):
        inline = cell.find("m:is", NS)
        if inline is not None:
            result[cell.attrib["r"]] = "".join(item.text or "" for item in inline.findall(".//m:t", NS))
    return result


@pytest.fixture(scope="module")
def completeness():
    return build_anf_operating_driver_full_completeness()


@pytest.fixture(scope="module")
def package(completeness):
    return build_operating_driver_anf_ui_v4(
        build_operating_driver_anf_ui_source_from_completeness(completeness),
        source_identity_receipts={
            "full_data_completeness_sha256": completeness.sha256,
            "registry_sha256": completeness.registry.sha256,
            "analytics_sha256": completeness.analytics.sha256,
            "semantics_sha256": completeness.semantics.sha256,
            "selection_sha256": completeness.selection.sha256,
        },
    )


@pytest.fixture(scope="module")
def plan(package):
    return build_operating_driver_anf_workbook_v4_plan(package)


@pytest.fixture(scope="module")
def outputs(tmp_path_factory, plan):
    root = tmp_path_factory.mktemp("operating_driver_anf_v4_ui_refinement")
    output_a = root / "a.xlsx"
    output_b = root / "b.xlsx"
    result_a = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED_ANF, output_workbook=output_a, plan=plan,
        expected_base_sha256=PROTECTED_ANF_SHA256,
    )
    result_b = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED_ANF, output_workbook=output_b, plan=plan,
        expected_base_sha256=PROTECTED_ANF_SHA256,
    )
    return output_a, output_b, result_a, result_b


def test_plan_matches_repaired_three_section_surface(plan) -> None:
    assert plan.plan_origin == "BLANK_SURFACE_V4"
    assert plan.used_range == USED_RANGE == "A1:P61"
    assert plan.zoom_scale == ZOOM_SCALE == 110
    assert plan.major_section_rows == {
        "Operating Drivers Overview": 3,
        "Core Drivers": 18,
        "Quarterly Driver History": 32,
    }
    assert plan.core_group_rows == {"Demand / Sales": 20, "Store Footprint": 25, "Inventory": 27}
    assert list(plan.history_group_rows) == ["Demand / Sales", "Inventory", "Store Footprint"]
    assert len(plan.history_metric_rows) == 15
    assert plan.footprint_definition_rows == {
        "Company-owned stores": 56,
        "New stores": 57,
        "Remodeled": 58,
        "Right-sized": 59,
        "Closed": 60,
    }


def test_plan_has_one_continuous_current_12q_header(plan) -> None:
    headers = [item for item in plan.bindings if item.element_type == "HISTORY_QUARTER_HEADER"]
    assert len(headers) == 12
    assert {item.target_range[-2:] for item in headers} == {"33"}
    assert [item.display_value for item in headers][-2:] == ["2025-Q4", "2026-Q1"]


def test_generic_merge_patcher_inserts_merge_cells_in_schema_order() -> None:
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<dimension ref="A1"/><sheetViews/><sheetFormatPr/><sheetData/>'
        '<pageMargins left="0.5" right="0.5" top="0.5" bottom="0.5" header="0.3" footer="0.3"/>'
        '<legacyDrawing xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" r:id="rId1"/>'
        '<extLst/></worksheet>'
    ).encode("utf-8")
    updated = _patch_merges(xml, (WorksheetMergeMutation("Operating_Drivers", "A1:P1", "ADD"),))
    root = ET.fromstring(updated)
    children = [_local(item.tag) for item in root]
    assert children == [
        "dimension", "sheetViews", "sheetFormatPr", "sheetData", "mergeCells",
        "pageMargins", "legacyDrawing", "extLst",
    ]


def test_materialized_sheet_children_are_validly_ordered(outputs) -> None:
    output_a, _, _, _ = outputs
    _, root = _target_root(output_a)
    children = [_local(item.tag) for item in root]
    positions = [WORKSHEET_ORDER.index(item) for item in children]
    assert positions == sorted(positions)
    assert children.index("mergeCells") < children.index("pageMargins")
    assert children.index("mergeCells") < children.index("legacyDrawing")


def test_materialization_is_raw_semantic_and_canonical_deterministic(outputs) -> None:
    output_a, output_b, result_a, result_b = outputs
    assert _sha(output_a) == _sha(output_b)
    assert result_a.output_workbook_sha256 == result_b.output_workbook_sha256
    assert result_a.semantic_workbook_sha256 == result_b.semantic_workbook_sha256
    assert result_a.canonical_ooxml_sha256 == result_b.canonical_ooxml_sha256


def test_only_authorized_ooxml_parts_change(outputs) -> None:
    _, _, result_a, _ = outputs
    assert result_a.unrelated_workbook_delta_count == 0
    assert set(result_a.changed_ooxml_parts) <= set(result_a.allowed_changed_ooxml_parts)
    assert "xl/worksheets/sheet4.xml" in result_a.changed_ooxml_parts


def test_visible_surface_has_current_sections_and_no_old_or_sparkline_content(outputs) -> None:
    output_a, _, result_a, _ = outputs
    _, root = _target_root(output_a)
    values = list(_display_text(root).values())
    text = "\n".join(values)
    assert values.count("Operating Drivers Overview") == 1
    assert values.count("Core Drivers") == 1
    assert values.count("Quarterly Driver History") == 1
    assert "OPERATING INTERPRETATION" in text
    assert "LATEST QUARTER — 2026-Q1" in text
    assert "BROADER TREND" in text
    assert "Latest (2026-Q1)" in text
    assert "pp = percentage points" in text
    assert "Store Footprint Guide" in text
    assert "Store-count bridge" in text
    assert "Current Read" not in text and "Watchlist" not in text
    assert result_a.sparkline_count == 0


def test_used_range_rows_columns_and_merges_match_plan(outputs, plan) -> None:
    output_a, _, _, _ = outputs
    _, root = _target_root(output_a)
    dimension = root.find("m:dimension", NS)
    assert dimension is not None and dimension.attrib["ref"] == "A1:P61"
    merges = {item.attrib["ref"] for item in root.findall("m:mergeCells/m:mergeCell", NS)}
    assert merges == {item.range_ref for item in plan.merge_mutations}
    assert [int(item.attrib["min"]) for item in root.findall("m:cols/m:col", NS)] == list(range(1, 17))


def test_zoom_is_110_only_on_operating_drivers_and_column_geometry_is_bounded(outputs) -> None:
    output_a, _, _, _ = outputs
    with ZipFile(PROTECTED_ANF, "r") as before, ZipFile(output_a, "r") as after:
        part = _sheet_part(after)
        root = ET.fromstring(after.read(part))
        view = root.find("m:sheetViews/m:sheetView", NS)
        assert view is not None
        assert view.attrib["zoomScale"] == view.attrib["zoomScaleNormal"] == "110"
        for name in before.namelist():
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml") and name != part:
                assert before.read(name) == after.read(name)
    widths = {
        int(item.attrib["min"]): float(item.attrib["width"])
        for item in root.findall("m:cols/m:col", NS)
    }
    assert widths[1] == 25.0
    assert [widths[column] for column in range(2, 5)] == [8.0, 8.0, 8.0]
    assert all(widths[column] == 15.4 for column in range(5, 17))


def test_model_native_row_heights_and_full_range_cell_styling(outputs, plan) -> None:
    output_a, _, result_a, _ = outputs
    _, root = _target_root(output_a)
    rows = {int(item.attrib["r"]): item for item in root.findall("m:sheetData/m:row", NS)}
    assert rows[1].attrib["ht"] == "28"
    assert rows[3].attrib["ht"] == "22"
    assert rows[4].attrib["ht"] == "21"
    assert rows[5].attrib["ht"] == "38"
    assert rows[18].attrib["ht"] == "22"
    assert rows[21].attrib["ht"] == "19.5"
    assert rows[33].attrib["ht"] == "22"
    assert rows[35].attrib["ht"] == "19.5"
    assert rows[53].attrib["ht"] == "19.5"
    assert rows[54].attrib["ht"] == "21"
    assert rows[55].attrib["ht"] == "22"
    assert all(rows[row].attrib["ht"] == "38" for row in range(56, 61))
    assert rows[61].attrib["ht"] == "32"
    by_cell = {cell.attrib["r"]: cell for cell in root.findall(".//m:sheetData/m:row/m:c", NS)}
    for binding in plan.bindings:
        start, *end = binding.target_range.split(":")
        if not end:
            assert start in by_cell
            continue
        start_column = ord(''.join(filter(str.isalpha, start))) - 64
        end_column = ord(''.join(filter(str.isalpha, end[0]))) - 64
        row = int(''.join(filter(str.isdigit, start)))
        for column in range(start_column, end_column + 1):
            coordinate = f"{chr(64 + column)}{row}"
            assert coordinate in by_cell
            assert int(by_cell[coordinate].attrib.get("s", "0")) > 0
    assert result_a.full_range_style_mismatch_count == 0


def test_group_fills_borders_and_latest_quarter_band_are_full_range(outputs, plan) -> None:
    output_a, _, result_a, _ = outputs
    with ZipFile(output_a, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    xfs = list(styles.find("m:cellXfs", NS))
    by_cell = {cell.attrib["r"]: cell for cell in root.findall(".//m:sheetData/m:row/m:c", NS)}

    def xf(coordinate: str):
        return xfs[int(by_cell[coordinate].attrib.get("s", "0"))]

    for row in (*plan.core_group_rows.values(), *plan.history_group_rows.values()):
        border_ids = {xf(f"{chr(64 + column)}{row}").attrib.get("borderId") for column in range(1, 17)}
        fill_ids = {xf(f"{chr(64 + column)}{row}").attrib.get("fillId") for column in range(1, 17)}
        assert len(border_ids) == 1
        assert len(fill_ids) in {1, 2}
    emphasized_rows = {33, *plan.history_group_rows.values(), *plan.history_metric_rows.values()}
    assert result_a.latest_quarter_emphasis_cell_count == len(emphasized_rows) == 19
    for row in plan.history_metric_rows.values():
        assert xf(f"P{row}").attrib.get("fillId") != xf(f"O{row}").attrib.get("fillId")
        assert xf(f"P{row}").attrib.get("borderId") == xf(f"O{row}").attrib.get("borderId")


def test_history_uses_smart_investor_number_formats(outputs, plan) -> None:
    output_a, _, result_a, _ = outputs
    with ZipFile(output_a, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    xfs = list(styles.find("m:cellXfs", NS))
    num_fmts = styles.find("m:numFmts", NS)
    assert num_fmts is not None
    format_codes = {
        int(item.attrib["numFmtId"]): item.attrib["formatCode"]
        for item in num_fmts
    }
    format_codes[3] = "#,##0"
    format_codes[9] = "0%"
    by_cell = {
        cell.attrib["r"]: cell
        for cell in root.findall(".//m:sheetData/m:row/m:c", NS)
    }
    assert set(plan.display_number_formats.values()) == {
        "0%", "0.0%", "#,##0", "#,##0.0",
        "+0%;-0%;0%",
        '+0" pp";-0" pp";0" pp"',
        '+0.0" pp";-0.0" pp";0" pp"',
        '#,##0" stores"',
        '+#,##0" stores";-#,##0" stores";0" stores"',
        '"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"',
        '+"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"',
    }
    for coordinate, expected in plan.display_number_formats.items():
        style_id = int(by_cell[coordinate].attrib.get("s", "0"))
        format_id = int(xfs[style_id].attrib.get("numFmtId", "0"))
        assert format_codes[format_id] == expected
    assert result_a.smart_precision_cell_count == len(plan.display_number_formats)


def test_all_exact_core_values_are_numeric_cells_with_formats(outputs, plan) -> None:
    output_a, _, _, _ = outputs
    with ZipFile(output_a, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
    by_cell = {
        cell.attrib["r"]: cell
        for cell in root.findall(".//m:sheetData/m:row/m:c", NS)
    }
    exact_core = {
        "E21", "G21", "I21", "E22", "G22", "I22",
        "E23", "G23", "I23", "E24", "G24", "I24",
        "E26", "G26", "I26", "E28", "G28", "I28", "E29", "G29", "I29",
    }
    assert len(exact_core) == 21
    for coordinate in exact_core:
        cell = by_cell[coordinate]
        assert cell.attrib.get("t") == "n"
        assert cell.find("m:v", NS) is not None
        assert coordinate in plan.display_number_formats
    for coordinate in ("E30", "G30", "I30"):
        assert by_cell[coordinate].attrib.get("t") == "inlineStr"


def test_store_comparison_and_approximate_inventory_cells_have_correct_types(outputs) -> None:
    output_a, _, _, _ = outputs
    _, root = _target_root(output_a)
    by_cell = {
        cell.attrib["r"]: cell
        for cell in root.findall(".//m:sheetData/m:row/m:c", NS)
    }
    assert by_cell["I26"].attrib.get("t") == "n"
    assert by_cell["I26"].find("m:v", NS).text == "41"
    assert by_cell["G30"].attrib.get("t") == "inlineStr"
    assert "Down from mid-single-digit" in "".join(
        item.text or "" for item in by_cell["G30"].findall(".//m:t", NS)
    )
    assert by_cell["I30"].attrib.get("t") == "inlineStr"
    assert by_cell["I30"].find("m:v", NS) is None


def test_footprint_definition_support_is_full_range_and_source_backed(outputs, plan, package) -> None:
    output_a, _, _, _ = outputs
    _, root = _target_root(output_a)
    text = _display_text(root)
    assert text["A54"] == "Store Footprint Guide"
    assert text["A55"] == "Term"
    assert text["D55"] == "What it means"
    assert text["I55"] == "Economic role"
    assert [text[f"A{row}"] for row in range(56, 61)] == [
        "Company-owned stores", "New stores", "Remodeled", "Right-sized", "Closed"
    ]
    assert "period end" in text["D56"]
    assert "fiscal period" in text["D57"]
    assert "physical selling footprint" in text["I56"]
    assert "store productivity" in text["I58"]
    assert "digital penetration" in text["I59"]
    assert "Store-count bridge" in text["A61"]
    assert all(
        binding.source_references
        for binding in plan.bindings
        if binding.element_type in {
            "FOOTPRINT_DEFINITION_TERM",
            "FOOTPRINT_DEFINITION_MEANING",
            "FOOTPRINT_DEFINITION_ECONOMIC_ROLE",
            "FOOTPRINT_DEFINITION_NOTE",
        }
    )
    assert all(definition.measurement for definition in package.footprint_definitions)
    assert not any(
        binding.element_type == "FOOTPRINT_DEFINITION_MEASUREMENT"
        for binding in plan.bindings
    )


def test_footprint_guide_uses_three_compact_full_width_columns(plan) -> None:
    expected = {
        ("FOOTPRINT_DEFINITION_HEADER", "A55:C55", "Term"),
        ("FOOTPRINT_DEFINITION_HEADER", "D55:H55", "What it means"),
        ("FOOTPRINT_DEFINITION_HEADER", "I55:P55", "Economic role"),
    }
    actual = {
        (item.element_type, item.target_range, item.display_value)
        for item in plan.bindings
        if item.element_type == "FOOTPRINT_DEFINITION_HEADER"
    }
    assert actual == expected
    for row in range(56, 61):
        ranges = {
            item.target_range
            for item in plan.bindings
            if item.target_range.endswith(str(row)) and item.element_type.startswith("FOOTPRINT_DEFINITION_")
        }
        assert ranges == {f"A{row}:C{row}", f"D{row}:H{row}", f"I{row}:P{row}"}


def test_footprint_guide_long_text_uses_wrapped_model_styles(plan) -> None:
    mutations = {item.target_cell: item for item in plan.cell_mutations}
    for row in range(56, 61):
        for coordinate in (f"D{row}", f"I{row}"):
            mutation = mutations[coordinate]
            assert (mutation.style_source_sheet, mutation.style_source_cell) == (
                "ANF_Investment_Case",
                "B7",
            )
    bridge = mutations["A61"]
    assert (bridge.style_source_sheet, bridge.style_source_cell) == (
        "Promise_Progress_UI",
        "K71",
    )


def test_approximate_inventory_history_remains_text_and_store_rollforward_is_numeric(outputs) -> None:
    output_a, _, _, _ = outputs
    _, root = _target_root(output_a)
    by_cell = {
        cell.attrib["r"]: cell
        for cell in root.findall(".//m:sheetData/m:row/m:c", NS)
    }
    approximate = {
        "K45": "Up mid-single",
        "N45": "Up ~1%",
        "O45": "Up mid-single",
        "P45": "Up low-single",
    }
    for coordinate, expected in approximate.items():
        cell = by_cell[coordinate]
        assert cell.attrib.get("t") == "inlineStr"
        assert "".join(item.text or "" for item in cell.findall(".//m:t", NS)) == expected
        assert cell.find("m:v", NS) is None
    for coordinate, expected in {
        "E47": "759", "F47": "765", "H47": "753", "I47": "757",
        "J47": "773", "L47": "793", "M47": "807", "N47": "827",
    }.items():
        cell = by_cell[coordinate]
        assert cell.attrib.get("t") == "n"
        assert cell.find("m:v", NS).text == expected


def test_target_has_no_formula_and_missing_never_becomes_zero(outputs, plan) -> None:
    output_a, _, result_a, _ = outputs
    _, root = _target_root(output_a)
    assert result_a.target_formula_count == 0
    assert not root.findall(".//m:f", NS)
    assert result_a.missing_to_zero_count == 0
    by_cell = {cell.attrib["r"]: cell for cell in root.findall(".//m:sheetData/m:row/m:c", NS)}
    missing = [item.target_range for item in plan.bindings if item.element_type == "HISTORY_MISSING"]
    assert missing
    for coordinate in missing:
        cell = by_cell[coordinate]
        assert cell.attrib.get("t") == "inlineStr"
        assert cell.find("m:v", NS) is None


def test_negative_numeric_history_does_not_use_red_font(outputs) -> None:
    output_a, _, _, _ = outputs
    with ZipFile(output_a, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    fonts = styles.find("m:fonts", NS)
    xfs = styles.find("m:cellXfs", NS)
    assert fonts is not None and xfs is not None
    negative_style_ids = set()
    for cell in root.findall(".//m:sheetData/m:row/m:c", NS):
        value = cell.find("m:v", NS)
        if cell.attrib.get("t") == "n" and value is not None and Decimal(value.text or "0") < 0:
            negative_style_ids.add(int(cell.attrib.get("s", "0")))
    assert negative_style_ids
    for style_id in negative_style_ids:
        xf = list(xfs)[style_id]
        font = list(fonts)[int(xf.attrib.get("fontId", "0"))]
        color = font.find("m:color", NS)
        assert color is None or color.attrib.get("rgb", "").upper() not in {"FFD55E00", "FFFF0000"}


def test_relationship_inventory_and_protected_workbook_are_preserved(outputs) -> None:
    output_a, _, result_a, _ = outputs
    with ZipFile(PROTECTED_ANF, "r") as before, ZipFile(output_a, "r") as after:
        assert before.namelist() == after.namelist()
        part = _sheet_part(after)
        rel_part = str(PurePosixPath(part).parent / "_rels" / f"{PurePosixPath(part).name}.rels")
        assert before.read(rel_part) == after.read(rel_part)
    assert result_a.comments_removed_count == 23
    assert _sha(PROTECTED_ANF) == PROTECTED_ANF_SHA256
