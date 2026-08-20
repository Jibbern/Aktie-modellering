from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_product import (
    build_cross_ticker_operating_driver_package,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_profiles import PROFILES
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_workbook import (
    build_cross_ticker_workbook_plan,
    materialize_cross_ticker_operating_driver_workbook,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import _sheet_part_map


ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models")
CONFIG = {
    "PBI": (ROOT / "PBI_model.xlsx", "6482617ad4f412dc5a1e130dc56c72bba5113ddacea5f7d1ab166fad8ddf5689", "PBI_Investment_Case", ".xlsx"),
    "GPRE": (ROOT / "GPRE_model.xlsm", "f7c23c9c0d9e3a52c708553e5fc6f964aa3fc58c8b4805d235f7ccfce5bde41b", "GPRE_Investment_Case", ".xlsm"),
}
ACCEPTED_UI_CONSISTENCY_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_final_ui_consistency_followup_2026-08-20"
)
ACCEPTED_UI_CONSISTENCY_PREVIEWS = {
    "PBI": ACCEPTED_UI_CONSISTENCY_ROOT / "PBI_operating_drivers_final_ui_consistency_followup_preview.xlsx",
    "GPRE": ACCEPTED_UI_CONSISTENCY_ROOT / "GPRE_operating_drivers_final_ui_consistency_followup_preview.xlsm",
}
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


@pytest.fixture(scope="module")
def packages():
    return {key: build_cross_ticker_operating_driver_package(value) for key, value in PROFILES.items()}


@pytest.fixture(scope="module")
def materialized(tmp_path_factory, packages):
    root = tmp_path_factory.mktemp("cross_ticker_workbooks")
    outputs = {}
    for ticker, package in packages.items():
        base, expected, investment_case, suffix = CONFIG[ticker]
        plan = build_cross_ticker_workbook_plan(package, investment_case_sheet=investment_case)
        output = root / f"{ticker}{suffix}"
        result = materialize_cross_ticker_operating_driver_workbook(
            base_workbook=base,
            output_workbook=output,
            plan=plan,
            expected_base_sha256=expected,
        )
        outputs[ticker] = (output, plan, result)
    return outputs


def _sheet_root(path: Path):
    with ZipFile(path) as archive:
        part = _sheet_part_map(archive)["Operating_Drivers"]
        return ET.fromstring(archive.read(part))


def _style_details(path: Path, coordinate: str):
    with ZipFile(path) as archive:
        part = _sheet_part_map(archive)["Operating_Drivers"]
        sheet = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    cell = sheet.find(f".//m:sheetData/m:row/m:c[@r='{coordinate}']", NS)
    assert cell is not None
    cell_xfs = styles.find("m:cellXfs", NS)
    fills = styles.find("m:fills", NS)
    assert cell_xfs is not None and fills is not None
    xf = list(cell_xfs)[int(cell.attrib.get("s", "0"))]
    alignment = xf.find("m:alignment", NS)
    fill = list(fills)[int(xf.attrib.get("fillId", "0"))]
    color = fill.find("m:patternFill/m:fgColor", NS)
    return (
        None if alignment is None else alignment.attrib.get("horizontal"),
        None if color is None else color.attrib.get("rgb"),
    )


def _style_contract(path: Path, coordinate: str):
    with ZipFile(path) as archive:
        part = _sheet_part_map(archive)["Operating_Drivers"]
        sheet = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    cell = sheet.find(f".//m:sheetData/m:row/m:c[@r='{coordinate}']", NS)
    assert cell is not None
    cell_xfs = styles.find("m:cellXfs", NS)
    assert cell_xfs is not None
    xf = list(cell_xfs)[int(cell.attrib.get("s", "0"))]
    alignment = xf.find("m:alignment", NS)
    return {
        "fill_id": int(xf.attrib.get("fillId", "0")),
        "border_id": int(xf.attrib.get("borderId", "0")),
        "horizontal": None if alignment is None else alignment.attrib.get("horizontal"),
        "wrap_text": None if alignment is None else alignment.attrib.get("wrapText"),
    }


def _visual_style_contract(path: Path, coordinate: str):
    with ZipFile(path) as archive:
        part = _sheet_part_map(archive)["Operating_Drivers"]
        sheet = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    cell = sheet.find(f".//m:sheetData/m:row/m:c[@r='{coordinate}']", NS)
    assert cell is not None
    cell_xfs = styles.find("m:cellXfs", NS)
    fills = styles.find("m:fills", NS)
    fonts = styles.find("m:fonts", NS)
    assert cell_xfs is not None and fills is not None and fonts is not None
    xf = list(cell_xfs)[int(cell.attrib.get("s", "0"))]
    alignment = xf.find("m:alignment", NS)
    fill = list(fills)[int(xf.attrib.get("fillId", "0"))]
    fill_color = fill.find("m:patternFill/m:fgColor", NS)
    font = list(fonts)[int(xf.attrib.get("fontId", "0"))]
    font_color = font.find("m:color", NS)
    font_name = font.find("m:name", NS)
    font_size = font.find("m:sz", NS)
    return {
        "fill_rgb": None if fill_color is None else fill_color.attrib.get("rgb"),
        "font_rgb": None if font_color is None else font_color.attrib.get("rgb"),
        "font_name": None if font_name is None else font_name.attrib.get("val"),
        "font_size": None if font_size is None else float(font_size.attrib["val"]),
        "bold": font.find("m:b", NS) is not None,
        "horizontal": None if alignment is None else alignment.attrib.get("horizontal"),
        "vertical": None if alignment is None else alignment.attrib.get("vertical"),
    }


def _border_contract(path: Path, coordinate: str):
    with ZipFile(path) as archive:
        part = _sheet_part_map(archive)["Operating_Drivers"]
        sheet = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    cell = sheet.find(f".//m:sheetData/m:row/m:c[@r='{coordinate}']", NS)
    assert cell is not None
    cell_xfs = styles.find("m:cellXfs", NS)
    borders = styles.find("m:borders", NS)
    assert cell_xfs is not None and borders is not None
    xf = list(cell_xfs)[int(cell.attrib.get("s", "0"))]
    border = list(borders)[int(xf.attrib.get("borderId", "0"))]
    result = {}
    for edge in ("left", "right", "top", "bottom"):
        node = border.find(f"m:{edge}", NS)
        color = None if node is None else node.find("m:color", NS)
        result[edge] = {
            "style": None if node is None else node.attrib.get("style"),
            "rgb": None if color is None else color.attrib.get("rgb"),
        }
    return result


def _visible_value_snapshot(path: Path):
    with ZipFile(path) as archive:
        part = _sheet_part_map(archive)["Operating_Drivers"]
        sheet = ET.fromstring(archive.read(part))
    rows = []
    for cell in sheet.findall(".//m:sheetData/m:row/m:c", NS):
        inline = cell.find("m:is", NS)
        value = cell.find("m:v", NS)
        formula = cell.find("m:f", NS)
        rows.append(
            {
                "coordinate": cell.attrib["r"],
                "type": cell.attrib.get("t"),
                "value": (
                    "".join(node.text or "" for node in inline.findall(".//m:t", NS))
                    if inline is not None
                    else None if value is None else value.text
                ),
                "formula": None if formula is None else formula.text,
            }
        )
    return sorted(rows, key=lambda item: item["coordinate"])


def _visible_value_multiset(path: Path):
    return Counter(
        (item["type"], item["value"], item["formula"])
        for item in _visible_value_snapshot(path)
        if item["value"] not in {None, ""} or item["formula"] is not None
    )


def test_frozen_three_section_contract_and_dynamic_latest(packages):
    for ticker, package in packages.items():
        plan = build_cross_ticker_workbook_plan(package, investment_case_sheet=CONFIG[ticker][2])
        assert list(plan.major_section_rows) == ["Operating Drivers Overview", "Core Drivers", "Quarterly Driver History"]
        header = next(item for item in plan.bindings if item.semantic_id == "core-header-1")
        mutation = next(item for item in plan.cell_mutations if item.target_cell == header.target_range.split(":")[0])
        assert package.latest_period_label in mutation.value


def test_driver_guide_has_three_visible_columns_and_no_measurement(packages):
    for ticker, package in packages.items():
        plan = build_cross_ticker_workbook_plan(package, investment_case_sheet=CONFIG[ticker][2])
        visible = [item.semantic_id for item in plan.bindings if item.element_type == "GUIDE_HEADER"]
        assert visible == ["guide-header-term", "guide-header-meaning", "guide-header-role"]
        assert not any("measurement" in item.lower() for item in visible)


def test_core_category_rows_match_investor_economic_map(packages):
    pbi = build_cross_ticker_workbook_plan(packages["PBI"], investment_case_sheet=CONFIG["PBI"][2])
    gpre = build_cross_ticker_workbook_plan(packages["GPRE"], investment_case_sheet=CONFIG["GPRE"][2])
    assert list(pbi.core_group_rows) == ["Presort Economics", "SendTech Leading Indicators"]
    assert list(gpre.core_group_rows) == [
        "Production & Asset Utilization",
        "Commodity Unit Economics",
        "Policy & Low-Carbon Economics",
    ]


def test_history_group_labels_match_investor_language(packages):
    pbi = build_cross_ticker_workbook_plan(packages["PBI"], investment_case_sheet=CONFIG["PBI"][2])
    gpre = build_cross_ticker_workbook_plan(packages["GPRE"], investment_case_sheet=CONFIG["GPRE"][2])
    assert list(pbi.history_group_rows) == ["Presort", "SendTech"]
    assert list(gpre.history_group_rows) == [
        "Production / Throughput",
        "Commodity Unit Economics",
        "Policy / Carbon",
        "Coproducts",
    ]


def test_pbi_accepted_observations_are_either_bound_or_intentionally_non_history(packages):
    package = packages["PBI"]
    plan = build_cross_ticker_workbook_plan(package, investment_case_sheet=CONFIG["PBI"][2])
    history_ids = {item.driver_id for item in package.history_rows}
    binding_ids = {item.semantic_id for item in plan.bindings}
    assert len(package.source_documents) == 29
    assert len(package.observations) == 30
    assert not [
        (item.driver_id, item.period_label)
        for item in package.observations
        if item.driver_id in history_ids
        and item.period_label in package.quarter_labels
        and f"history:{item.driver_id}:{item.period_label}" not in binding_ids
    ]
    assert {item.driver_id for item in package.observations if item.driver_id not in history_ids} == {
        "pbi.sendtech.backlog_state",
        "pbi.sendtech.subscription_revenue_direction",
    }


def test_source_native_packages_are_identical_to_accepted_ui_consistency_state(packages):
    for ticker, package in packages.items():
        accepted = json.loads(
            (ACCEPTED_UI_CONSISTENCY_ROOT / "work" / f"{ticker}_PACKAGE.json").read_text(encoding="utf-8")
        )
        assert package.to_dict() == accepted


def test_exact_values_are_real_numeric_cells(materialized):
    for output, plan, result in materialized.values():
        root = _sheet_root(output)
        by_coordinate = {item.attrib["r"]: item for item in root.findall(".//m:sheetData/m:row/m:c", NS)}
        assert plan.exact_numeric_coordinates
        assert all(by_coordinate[item].attrib.get("t") == "n" for item in plan.exact_numeric_coordinates)
        assert result.exact_numeric_stored_as_text_count == 0


def test_qualitative_values_remain_inline_text(materialized):
    output, plan, _ = materialized["PBI"]
    root = _sheet_root(output)
    text_cells = [item for item in root.findall(".//m:sheetData/m:row/m:c", NS) if item.attrib.get("t") == "inlineStr"]
    values = ["".join(node.text or "" for node in item.findall(".//m:t", NS)) for item in text_cells]
    assert "Highest since 2024 migration" in values
    assert "Declining" in values


def test_missing_cells_are_not_numeric_zero(materialized):
    for output, plan, result in materialized.values():
        assert plan.missing_coordinates
        assert result.missing_to_zero_count == 0


def test_target_contains_no_formulas(materialized):
    assert all(result.target_formula_count == 0 for _, _, result in materialized.values())


def test_unrelated_package_parts_are_lossless(materialized):
    assert all(result.unrelated_workbook_delta_count == 0 for _, _, result in materialized.values())
    assert all(set(result.changed_ooxml_parts) <= set(result.allowed_changed_ooxml_parts) for _, _, result in materialized.values())


def test_gpre_vba_is_byte_identical(materialized):
    output, _, result = materialized["GPRE"]
    assert output.suffix == ".xlsm"
    assert result.vba_sha256_before == result.vba_sha256_after
    assert result.vba_delta_count == 0
    with ZipFile(output) as archive:
        assert "xl/vbaProject.bin" in archive.namelist()


def test_workbook_replay_is_raw_deterministic(tmp_path, packages, materialized):
    for ticker, package in packages.items():
        base, expected, investment_case, suffix = CONFIG[ticker]
        original, plan, result = materialized[ticker]
        replay = tmp_path / f"{ticker}_replay{suffix}"
        replay_result = materialize_cross_ticker_operating_driver_workbook(
            base_workbook=base,
            output_workbook=replay,
            plan=plan,
            expected_base_sha256=expected,
        )
        assert result.output_workbook_sha256 == replay_result.output_workbook_sha256
        assert hashlib.sha256(original.read_bytes()).hexdigest() == hashlib.sha256(replay.read_bytes()).hexdigest()


def test_zoom_and_width_contract(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        view = root.find("m:sheetViews/m:sheetView", NS)
        assert view is not None and view.attrib["zoomScale"] == "110"
        widths = {int(item.attrib["min"]): float(item.attrib["width"]) for item in root.findall("m:cols/m:col", NS)}
        assert all(widths[index] == pytest.approx(15.4) for index in range(5, 17))


def test_full_range_group_styles_are_materialized(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        by_coordinate = {item.attrib["r"]: item for item in root.findall(".//m:sheetData/m:row/m:c", NS)}
        for row in [*plan.core_group_rows.values(), *plan.history_group_rows.values()]:
            assert all(f"{column}{row}" in by_coordinate for column in "ABCDEFGHIJKLMNOP")


def test_group_labels_use_a_to_d_and_full_width_subsection_bands(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        merges = {item.attrib["ref"] for item in root.findall("m:mergeCells/m:mergeCell", NS)}
        for row in [*plan.core_group_rows.values(), *plan.history_group_rows.values()]:
            assert f"A{row}:D{row}" in merges
            first = _visual_style_contract(output, f"A{row}")
            assert first["fill_rgb"] in {"00D9E7F3", "FFD9E7F3"}
            assert first["bold"] is True
            for column in "BCDEFGHIJKLMNOP":
                assert _visual_style_contract(output, f"{column}{row}")["fill_rgb"] == first["fill_rgb"]


def test_core_rows_use_compact_professional_research_heights(packages):
    for ticker, package in packages.items():
        plan = build_cross_ticker_workbook_plan(
            package,
            investment_case_sheet=CONFIG[ticker][2],
        )
        heights = {item.row: item.height for item in plan.row_mutations}
        assert heights[plan.major_section_rows["Core Drivers"]] == 26.0
        assert heights[plan.major_section_rows["Quarterly Driver History"]] == 26.0
        assert heights[1] == 26.0
        assert all(heights[row] == 22.0 for row in plan.core_group_rows.values())
        assert all(heights[row] == 19.5 for row in plan.core_metric_rows.values())
        assert all(heights[row] == 22.0 for row in plan.history_group_rows.values())
        overview_rows = {
            int(item.target_range.split(":", 1)[0][1:])
            for item in plan.bindings
            if item.element_type == "OVERVIEW_SUBSECTION"
        }
        assert all(heights[row] == 26.0 for row in overview_rows)
        narrative_rows = {
            int(item.target_range.split(":", 1)[0][1:])
            for item in plan.bindings
            if item.element_type == "OVERVIEW_STATEMENT"
        }
        assert all(heights[row] == 36.0 for row in narrative_rows)
        guide_section_row = min(plan.guide_rows.values()) - 2
        assert heights[guide_section_row] == 26.0
        assert all(heights[row] in {30.0, 36.0, 48.0} for row in plan.guide_rows.values())
        assert max(heights[row] for row in plan.guide_rows.values()) <= 48.0
        assert all(heights[row] != 42.0 for row in plan.guide_rows.values())


def test_all_major_sections_use_full_width_valuation_title_band(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        merges = {item.attrib["ref"] for item in root.findall("m:mergeCells/m:mergeCell", NS)}
        rows = {int(item.attrib["r"]): item for item in root.findall("m:sheetData/m:row", NS)}
        guide_section_row = min(plan.guide_rows.values()) - 2
        rows_by_name = {
            "Operating Drivers": 1,
            "Core Drivers": plan.major_section_rows["Core Drivers"],
            "Quarterly Driver History": plan.major_section_rows["Quarterly Driver History"],
            "Driver Guide": guide_section_row,
        }
        for name, row in rows_by_name.items():
            assert f"A{row}:P{row}" in merges
            assert float(rows[row].attrib["ht"]) == pytest.approx(26.0)
            first = _visual_style_contract(output, f"A{row}")
            assert first["fill_rgb"][-6:] == "6FA8DC"
            assert first["font_rgb"][-6:] == "FFFFFF"
            assert first["bold"] is True
            # OOXML General alignment renders text left in native Excel; an
            # explicit left alignment is equivalent for this text-only band.
            assert first["horizontal"] in {None, "left"}
            assert first["vertical"] == "center"
            for column in "BCDEFGHIJKLMNOP":
                current = _visual_style_contract(output, f"{column}{row}")
                assert current["fill_rgb"] == first["fill_rgb"]
                assert current["font_rgb"] == first["font_rgb"]


def test_overview_headings_use_the_same_major_title_hierarchy_as_core(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        rows = {int(item.attrib["r"]): item for item in root.findall("m:sheetData/m:row", NS)}
        overview_rows = {
            int(item.target_range.split(":", 1)[0][1:])
            for item in plan.bindings
            if item.element_type == "OVERVIEW_SUBSECTION"
        }
        for row in overview_rows:
            assert float(rows[row].attrib["ht"]) == pytest.approx(26.0)
            first = _visual_style_contract(output, f"A{row}")
            assert first["fill_rgb"][-6:] == "6FA8DC"
            assert first["font_rgb"][-6:] == "FFFFFF"
            assert first["bold"] is True
            assert first["horizontal"] in {None, "left"}
            for column in "BCDEFGHIJKLMNOP":
                current = _visual_style_contract(output, f"{column}{row}")
                assert current["fill_rgb"] == first["fill_rgb"]
                assert current["font_rgb"] == first["font_rgb"]


def test_overview_narrative_is_bulleted_and_model_native(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        rows = {int(item.attrib["r"]): item for item in root.findall("m:sheetData/m:row", NS)}
        for binding in (item for item in plan.bindings if item.element_type == "OVERVIEW_STATEMENT"):
            coordinate = binding.target_range.split(":", 1)[0]
            row = int(coordinate[1:])
            cell = root.find(f".//m:sheetData/m:row/m:c[@r='{coordinate}']", NS)
            assert cell is not None
            text = "".join(node.text or "" for node in cell.findall(".//m:t", NS))
            assert text.startswith("• ")
            assert float(rows[row].attrib["ht"]) == pytest.approx(36.0)
            style = _visual_style_contract(output, coordinate)
            assert style["font_name"] == "Calibri"
            assert style["font_size"] == pytest.approx(12.0)
            assert style["horizontal"] == "left"
            assert style["vertical"] == "center"


def test_core_and_history_categories_retain_light_blue_hierarchy(materialized):
    for output, plan, _ in materialized.values():
        root = _sheet_root(output)
        rows = {int(item.attrib["r"]): item for item in root.findall("m:sheetData/m:row", NS)}
        subsection_rows = set(plan.core_group_rows.values()) | set(plan.history_group_rows.values())
        for row in subsection_rows:
            assert float(rows[row].attrib["ht"]) == pytest.approx(22.0)
            first = _visual_style_contract(output, f"A{row}")
            assert first["fill_rgb"][-6:] == "D9E7F3"
            assert first["bold"] is True
            for column in "BCDEFGHIJKLMNOP":
                assert _visual_style_contract(output, f"{column}{row}")["fill_rgb"] == first["fill_rgb"]


def test_broader_trend_header_and_values_are_left_centered(materialized):
    for output, plan, _ in materialized.values():
        header_row = plan.major_section_rows["Core Drivers"] + 1
        for coordinate in (f"K{header_row}", f"L{header_row}"):
            style = _visual_style_contract(output, coordinate)
            assert style["horizontal"] == "left"
            assert style["vertical"] == "center"
        for row in plan.core_metric_rows.values():
            for coordinate in (f"K{row}", f"L{row}"):
                style = _visual_style_contract(output, coordinate)
                assert style["horizontal"] == "left"
                assert style["vertical"] == "center"


def test_core_driver_column_alignment_contract(materialized):
    for output, plan, _ in materialized.values():
        header_row = plan.major_section_rows["Core Drivers"] + 1
        for column in "ABCDEFGHIJKLMNOP":
            assert _visual_style_contract(output, f"{column}{header_row}")["horizontal"] == "left"
        for row in plan.core_metric_rows.values():
            for column in "ABCDEFGHIJKLMNOP":
                assert _visual_style_contract(output, f"{column}{row}")["horizontal"] == "left"


def test_precision_note_is_immediately_above_core_and_absent_below_history(materialized):
    for output, plan, _ in materialized.values():
        note = [item for item in plan.bindings if item.semantic_id == "history-note"]
        assert len(note) == 1
        note_row = int(note[0].target_range.split(":", 1)[0][1:])
        assert note_row == plan.major_section_rows["Core Drivers"] - 1
        assert note_row < plan.major_section_rows["Quarterly Driver History"]
        assert "pp = percentage points" in next(
            item.value for item in plan.cell_mutations if item.target_cell == f"A{note_row}"
        )


def test_core_category_bands_and_body_rows_have_distinct_hierarchy(materialized):
    for output, plan, _ in materialized.values():
        for row in plan.core_group_rows.values():
            first = _style_contract(output, f"A{row}")
            last = _style_contract(output, f"P{row}")
            assert first == last
            assert first["fill_id"] != 0
        for row in plan.core_metric_rows.values():
            assert _style_contract(output, f"A{row}")["fill_id"] == 0
            assert _style_contract(output, f"M{row}")["fill_id"] == 0


def test_driver_guide_uses_white_wrapped_reference_body(materialized):
    for output, plan, _ in materialized.values():
        assert plan.guide_rows
        for row in plan.guide_rows.values():
            term = _style_contract(output, f"A{row}")
            meaning = _style_contract(output, f"D{row}")
            role = _style_contract(output, f"I{row}")
            assert term == {
                "fill_id": 0,
                "border_id": 2,
                "horizontal": "left",
                "wrap_text": None,
            }
            assert meaning["fill_id"] == role["fill_id"] == 0
            assert meaning["border_id"] == role["border_id"] == 2
            assert meaning["horizontal"] == role["horizontal"] == "left"
            assert meaning["wrap_text"] == role["wrap_text"] == "1"


def test_visible_values_and_formulas_change_only_by_required_bullet_prefix(materialized):
    for ticker, (output, plan, _) in materialized.items():
        before = {item["coordinate"]: item for item in _visible_value_snapshot(ACCEPTED_UI_CONSISTENCY_PREVIEWS[ticker])}
        after = {item["coordinate"]: item for item in _visible_value_snapshot(output)}
        assert set(before) == set(after)
        overview_coordinates = {
            item.target_range.split(":", 1)[0]
            for item in plan.bindings
            if item.element_type == "OVERVIEW_STATEMENT"
        }
        for coordinate in sorted(before):
            if coordinate not in overview_coordinates:
                assert after[coordinate] == before[coordinate]
                continue
            assert after[coordinate]["type"] == before[coordinate]["type"]
            assert after[coordinate]["formula"] == before[coordinate]["formula"]
            expected = before[coordinate]["value"]
            if expected and not expected.startswith("• "):
                expected = "• " + expected
            assert after[coordinate]["value"] == expected


def test_gpre_underlying_crush_is_visible_fail_closed_text(materialized):
    output, plan, _ = materialized["GPRE"]
    row = plan.core_metric_rows["gpre-core-underlying-crush"]
    root = _sheet_root(output)
    cell = root.find(f".//m:sheetData/m:row/m:c[@r='E{row}']", NS)
    assert cell is not None and cell.attrib.get("t") == "inlineStr"
    assert "".join(node.text or "" for node in cell.findall(".//m:t", NS)) == "Not disclosed"


def test_historical_outputs_use_white_rows_with_subtle_horizontal_separators(materialized):
    for output, plan, _ in materialized.values():
        for row in plan.history_metric_rows.values():
            for column in "ABCD":
                style = _style_contract(output, f"{column}{row}")
                assert style["fill_id"] == 0
            for column in "EFGHIJKLMNOP":
                style = _style_contract(output, f"{column}{row}")
                assert style["fill_id"] == 0
                assert style["horizontal"] == "right"
            for column in "ABCDEFGHIJKLMNOP":
                border = _border_contract(output, f"{column}{row}")
                assert border["left"]["style"] is None
                assert border["right"]["style"] is None
                assert border["top"]["style"] is None
                assert border["bottom"]["style"] == "thin"
                assert border["bottom"]["rgb"][-6:] == "D9E2EF"


def test_gpre_supporting_throughput_stays_out_of_core_and_in_history(packages):
    package = packages["GPRE"]
    assert "gpre.ethanol.produced_mgal" not in {item.driver_id for item in package.core_drivers}
    assert "gpre.ethanol.produced_mgal" in {item.driver_id for item in package.history_rows}


def test_protected_inputs_remain_unchanged():
    for path, expected, _, _ in CONFIG.values():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected
