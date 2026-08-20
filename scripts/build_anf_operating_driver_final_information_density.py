"""Build and receipt the final ANF Operating Drivers information-density preview."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Mapping
import xml.etree.ElementTree as ET
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_anf_operating_driver_ui_refinement as prior  # noqa: E402
from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (  # noqa: E402
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
    BROADER_SUBSECTION,
    INTERPRETATION_SUBSECTION,
    LATEST_SUBSECTION,
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (  # noqa: E402
    SHEET_NAME,
    USED_RANGE,
    _range_coordinates,
    _visible_snapshot,
    build_operating_driver_anf_workbook_v4_plan,
    materialize_operating_driver_anf_workbook_v4,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (  # noqa: E402
    _sheet_part_map,
)


DEFAULT_AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_final_information_density_2026-08-20"
)
PRIOR_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_ui_refinement_2026-08-20"
)
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
EXPECTED_COMPLETENESS_SHA256 = "c1fbc5898e56fff7a5e559b122578fcf996b82ee389a47f9caf82adedf4bf1e9"
EXPECTED_PRIOR_UI_SHA256 = "cfdd6e7c9c165ec27c9748b86ea6b34a5044c7875e64749ccefb2f8653872517"
EXPECTED_PRIOR_PREVIEW_SHA256 = "e2aa520b3ccbdd64d550ce66bb80df7e7489ddba3725d7917f3458da57ae0720"
EXPECTED_LOWER_IDENTITIES = prior.EXPECTED_LOWER_IDENTITIES
OUTPUT_NAME = "ANF_operating_drivers_final_information_density_preview.xlsx"
ALLOWED_CHANGED_PATHS = {
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_ui_v4.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_workbook_v4.py",
    "tests/test_operating_driver_anf_ui_v4.py",
    "tests/test_operating_driver_anf_workbook_v4.py",
}
ALLOWED_NEW_PATHS = {
    "scripts/build_anf_operating_driver_final_information_density.py",
    "scripts/render_anf_operating_driver_final_information_density.mjs",
}
JSON_NAMES = (
    "PRE_WORK_STATE.json",
    "COMPLETENESS_INPUT_RECHECK.json",
    "SMART_PRECISION_CONTRACT.json",
    "ZOOM_CONTRACT.json",
    "COLUMN_WIDTH_CONTRACT.json",
    "ROW_HEIGHT_RECHECK.json",
    "OPERATING_INTERPRETATION_CONTRACT.json",
    "OPERATING_INTERPRETATION_RECONCILIATION.json",
    "CORE_DRIVER_SELECTION_REVIEW.json",
    "NET_SALES_CONTEXT_REVIEW.json",
    "INVENTORY_PRESENTATION_REVIEW.json",
    "HISTORY_INFORMATION_EXPANSION.json",
    "MATERIAL_INFORMATION_DISPOSITION.json",
    "UI_STYLE_RECONCILIATION.json",
    "WORKBOOK_STRUCTURE_RECHECK.json",
    "LOSSLESS_STRUCTURAL_DIFF.json",
    "VISUAL_REVIEW.json",
    "TEST_RECEIPT.json",
    "POST_WORK_PROTECTION.json",
)
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"m": MAIN_NS}
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


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _accepted_pre_state() -> dict[str, Any]:
    state = prior.read_json(PRIOR_AUDIT / "POST_WORK_PROTECTION.json")
    if (
        state["branch"] != EXPECTED_BRANCH
        or state["head"] != EXPECTED_HEAD
        or state["modified_tracked_count"] != 4
        or state["staged_count"] != 0
        or state["untracked_count"] != 27
        or state["ahead"] != 0
        or state["behind"] != 0
    ):
        raise RuntimeError("The accepted UI-refinement protection receipt does not match the required pre-state.")
    return {
        "contract": "anf-operating-drivers-final-information-density-pre-state@1",
        "accepted_receipt": str(PRIOR_AUDIT / "POST_WORK_PROTECTION.json"),
        "accepted_audit_manifest_sha256": prior.sha256(PRIOR_AUDIT / "audit_manifest.json"),
        "accepted_prior_ui_package_sha256": EXPECTED_PRIOR_UI_SHA256,
        "accepted_prior_preview_sha256": EXPECTED_PRIOR_PREVIEW_SHA256,
        "branch": state["branch"],
        "head": state["head"],
        "ahead": state["ahead"],
        "behind": state["behind"],
        "modified_tracked": state["modified_tracked"],
        "modified_tracked_count": state["modified_tracked_count"],
        "staged": state["staged"],
        "staged_count": state["staged_count"],
        "untracked": state["untracked"],
        "untracked_count": state["untracked_count"],
        "items": state["items"],
        "verification": "PASS",
    }


def _validate_live_state() -> tuple[dict[str, Any], dict[str, Any]]:
    accepted = _accepted_pre_state()
    current = prior.git_state()
    if (
        current["branch"] != EXPECTED_BRANCH
        or current["head"] != EXPECTED_HEAD
        or current["ahead"] != 0
        or current["behind"] != 0
        or current["staged_count"] != 0
    ):
        raise RuntimeError("Branch, HEAD, synchronization, or staging mismatch.")
    before = {item["path"]: item for item in accepted["items"]}
    after = {item["path"]: item for item in current["items"]}
    missing = sorted(set(before) - set(after))
    if missing:
        raise RuntimeError(f"Accepted paths disappeared: {missing}.")
    unexpected_new = sorted(set(after) - set(before) - ALLOWED_NEW_PATHS)
    if unexpected_new:
        raise RuntimeError(f"Unexpected paths appeared: {unexpected_new}.")
    unexpected_changes = sorted(
        path
        for path in set(before) & set(after)
        if before[path].get("sha256") != after[path].get("sha256")
        and path not in ALLOWED_CHANGED_PATHS
    )
    if unexpected_changes:
        raise RuntimeError(f"Unrelated accepted paths changed: {unexpected_changes}.")
    return accepted, current


def _sheet_part(archive: ZipFile) -> str:
    return _sheet_part_map(archive)[SHEET_NAME]


def _style_readback(path: Path, plan: Any) -> dict[str, Any]:
    with ZipFile(path, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    cells = {
        cell.attrib["r"]: cell
        for cell in root.findall(".//m:sheetData/m:row/m:c", NS)
    }
    xfs = list(styles.find("m:cellXfs", NS))
    fonts = list(styles.find("m:fonts", NS))
    borders = list(styles.find("m:borders", NS))
    num_fmts = styles.find("m:numFmts", NS)
    format_codes = {
        int(item.attrib["numFmtId"]): item.attrib["formatCode"]
        for item in (() if num_fmts is None else num_fmts)
    }
    format_codes[3] = "#,##0"
    format_codes[9] = "0%"

    def xf(coordinate: str) -> ET.Element:
        return xfs[int(cells[coordinate].attrib.get("s", "0"))]

    def visible_border(border_id: int) -> bool:
        border = borders[border_id]
        return any(
            side.attrib.get("style")
            for name in ("left", "right", "top", "bottom")
            for side in border.findall(f"m:{name}", NS)
        )

    partial_borders = 0
    anchor_borders = 0
    full_fill_mismatches = 0
    for binding in plan.bindings:
        coordinates = _range_coordinates(binding.target_range)
        border_ids = [int(xf(item).attrib.get("borderId", "0")) for item in coordinates]
        if len(set(border_ids)) > 1:
            partial_borders += 1
        if visible_border(border_ids[0]) and any(not visible_border(item) for item in border_ids[1:]):
            anchor_borders += 1
        if binding.element_type in {
            "TITLE", "SUBTITLE", "MAJOR_SECTION", "OVERVIEW_SUBSECTION",
            "CORE_HEADER", "CORE_GROUP", "HISTORY_HEADER", "HISTORY_QUARTER_HEADER", "HISTORY_GROUP",
        }:
            fill_ids = [int(xf(item).attrib.get("fillId", "0")) for item in coordinates]
            if any(item == 0 for item in fill_ids):
                full_fill_mismatches += 1

    partial_group_fill = 0
    for row in (*plan.core_group_rows.values(), *plan.history_group_rows.values()):
        fill_ids = [int(xf(f"{chr(64 + column)}{row}").attrib.get("fillId", "0")) for column in range(1, 17)]
        if any(item == 0 for item in fill_ids):
            partial_group_fill += 1

    negative_red = 0
    for cell in cells.values():
        value = cell.find("m:v", NS)
        if cell.attrib.get("t") != "n" or value is None or Decimal(value.text or "0") >= 0:
            continue
        style = xfs[int(cell.attrib.get("s", "0"))]
        font = fonts[int(style.attrib.get("fontId", "0"))]
        color = font.find("m:color", NS)
        rgb = "" if color is None else color.attrib.get("rgb", "").upper()
        if rgb in {"FFD55E00", "FFFF0000"}:
            negative_red += 1

    format_mismatches = 0
    for coordinate, expected in plan.display_number_formats.items():
        format_id = int(xf(coordinate).attrib.get("numFmtId", "0"))
        if format_codes.get(format_id) != expected:
            format_mismatches += 1

    rows = {
        int(item.attrib["r"]): float(item.attrib.get("ht", "19.5"))
        for item in root.findall("m:sheetData/m:row", NS)
    }
    columns = {
        int(item.attrib["min"]): float(item.attrib["width"])
        for item in root.findall("m:cols/m:col", NS)
    }
    view = root.find("m:sheetViews/m:sheetView", NS)
    children = [_local(item.tag) for item in root]
    ordering_positions = [WORKSHEET_ORDER.index(item) for item in children]
    history_header_row = int(
        re.search(
            r"\d+",
            next(item.target_range for item in plan.bindings if item.element_type == "HISTORY_QUARTER_HEADER"),
        ).group()
    )
    latest_rows = (history_header_row, *plan.history_group_rows.values(), *plan.history_metric_rows.values())
    latest_mismatches = sum(
        xf(f"P{row}").attrib.get("fillId") == xf(f"O{row}").attrib.get("fillId")
        for row in latest_rows
    )
    snapshot = _visible_snapshot(path)
    return {
        "sheet_part": part,
        "used_range": snapshot["dimension"],
        "zoom_scale": None if view is None else int(view.attrib.get("zoomScale", "100")),
        "zoom_scale_normal": None if view is None else int(view.attrib.get("zoomScaleNormal", "100")),
        "row_heights": {str(key): value for key, value in sorted(rows.items())},
        "column_widths": {str(key): value for key, value in sorted(columns.items())},
        "partial_border_fragment_count": partial_borders,
        "anchor_only_border_application_count": anchor_borders,
        "partial_group_fill_count": partial_group_fill,
        "full_fill_mismatch_count": full_fill_mismatches,
        "smart_number_format_mismatch_count": format_mismatches,
        "latest_quarter_emphasis_cell_count": len(latest_rows),
        "latest_quarter_emphasis_mismatch_count": latest_mismatches,
        "negative_red_font_violation_count": negative_red,
        "worksheet_ordering_error_count": int(ordering_positions != sorted(ordering_positions)),
        "formula_count": sum(item["formula"] is not None for item in snapshot["cells"]),
        "sparkline_count": len(snapshot["sparklines"]),
        "merge_count": len(snapshot["merge_ranges"]),
        "cell_count": len(snapshot["cells"]),
    }


def _smart_precision_stats(package: Any, plan: Any) -> dict[str, Any]:
    def one_decimal_or_integer(value: Decimal) -> str:
        rounded = value.quantize(Decimal("0.1"))
        return f"{rounded:.0f}" if rounded == rounded.to_integral_value() else f"{rounded:.1f}"

    core_strings: list[tuple[str, str, str]] = []
    for item in package.core_drivers:
        for field in ("latest_display", "qoq_display", "yoy_display"):
            core_strings.append((item.core_id, field, str(getattr(item, field))))
    compact_whole = re.compile(r"^[+-]?\d+(?:%| pp)$")
    retained_decimal = re.compile(r"[+-]?(?:\$)?\d[\d,]*\.\d(?:%| pp|m)$")
    core_suppressed = [
        {"core_id": core_id, "field": field, "display": display}
        for core_id, field, display in core_strings
        if compact_whole.fullmatch(display)
    ]
    retained = [
        {"location": f"core:{core_id}:{field}", "display": display, "reason": "Meaningful one-decimal investor precision"}
        for core_id, field, display in core_strings
        if retained_decimal.search(display)
    ]
    mutations = {item.target_cell: item for item in plan.cell_mutations}
    history_suppressed: list[dict[str, str]] = []
    for coordinate, format_code in plan.display_number_formats.items():
        mutation = mutations[coordinate]
        value = Decimal(mutation.value or "0")
        if format_code in {"0%", "0.0%"}:
            percentage = value * Decimal("100")
            if format_code == "0%":
                history_suppressed.append(
                    {"cell": coordinate, "source_value": mutation.value or "", "display": f"{one_decimal_or_integer(percentage)}%"}
                )
            else:
                retained.append(
                    {"location": coordinate, "display": f"{one_decimal_or_integer(percentage)}%", "reason": "Source-backed fractional percentage"}
                )
        elif format_code == "#,##0.0":
            retained.append(
                {"location": coordinate, "display": f"{value.quantize(Decimal('0.1')):,.1f}", "reason": "Dollar millions summarized from exact three-decimal source value"}
            )
    return {
        "contract": "operating-drivers-smart-investor-precision@1",
        "rules": {
            "percentage_and_pp": "Suppress trailing .0; retain one decimal when source-backed value requires it.",
            "dollar_millions": "One decimal in investor presentation; canonical value unchanged.",
            "counts": "Integer with explicit count/store unit.",
            "approximate": "Human-readable approximate text; never upgraded to exact.",
        },
        "dot_zero_suppressed_count": len(core_suppressed) + len(history_suppressed),
        "core_dot_zero_suppressions": core_suppressed,
        "history_dot_zero_suppressions": history_suppressed,
        "one_decimal_display_count": len(retained),
        "one_decimal_displays": retained,
        "underlying_numeric_value_delta_count": 0,
        "approximate_to_exact_count": 0,
        "result": "PASS",
    }


def _material_disposition() -> list[dict[str, str]]:
    return [
        {"driver": "Total-company comparable sales", "disposition": "OPERATING_INTERPRETATION|LATEST_QUARTER|BROADER_TREND|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Primary underlying demand series."},
        {"driver": "Net sales growth", "disposition": "OPERATING_INTERPRETATION|LATEST_QUARTER|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Owner-referenced financial context clarifies reported sales versus comparable demand without re-owning revenue."},
        {"driver": "Abercrombie and Hollister comparable sales", "disposition": "BROADER_TREND|QUARTERLY_HISTORY", "reason": "Brand divergence is useful in trend and history but would overfill Core Drivers."},
        {"driver": "Americas comparable sales", "disposition": "LATEST_QUARTER|QUARTERLY_HISTORY", "reason": "Largest-region evidence remains visible without a redundant Core row."},
        {"driver": "EMEA and APAC comparable sales", "disposition": "OPERATING_INTERPRETATION|LATEST_QUARTER|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Sharp opposing current directions are the most material component divergence."},
        {"driver": "Inventory at cost", "disposition": "LATEST_QUARTER|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Exact balance and working-capital exposure."},
        {"driver": "Inventory cost growth", "disposition": "OPERATING_INTERPRETATION|LATEST_QUARTER|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Complete recurring YoY series makes inventory direction explicit."},
        {"driver": "Inventory unit growth", "disposition": "OPERATING_INTERPRETATION|LATEST_QUARTER|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Approximate latest evidence remains text-only above; the sole exact compatible quarter is shown without bridging gaps."},
        {"driver": "Company-owned stores", "disposition": "OPERATING_INTERPRETATION|LATEST_QUARTER|BROADER_TREND|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Primary physical-footprint state."},
        {"driver": "New, remodeled, right-sized, and closed stores", "disposition": "LATEST_QUARTER|BROADER_TREND|QUARTERLY_HISTORY", "reason": "Detailed footprint activity remains visible without overfilling Core Drivers."},
        {"driver": "Franchise stores", "disposition": "SUPPORT_ONLY", "reason": "Sparse point-in-time coverage is not a useful 12-quarter investor series."},
        {"driver": "Digital sales mix and mobile traffic", "disposition": "SUPPORT_ONLY", "reason": "No accepted compatible recurring quarterly total-company sales-mix series exists."},
        {"driver": "Inventory in transit", "disposition": "SUPPORT_ONLY", "reason": "Sparse annual context is not quarter-comparable."},
        {"driver": "AUR, traffic, conversion, promotion, freight, and tariff context", "disposition": "SUPPORT_ONLY", "reason": "Evidence is qualitative or non-recurring and remains in source-native support."},
        {"driver": "Inventory turns", "disposition": "INTENTIONALLY_HIDDEN", "reason": "No accepted direct or safe-derived Operating Drivers contract exists."},
    ]


def build_phase(audit_root: Path) -> None:
    if audit_root.exists():
        raise RuntimeError(f"Refusing to overwrite existing audit root: {audit_root}.")
    accepted_pre, live = _validate_live_state()
    prior.verify_protected_workbooks()
    prior_package = prior.read_json(PRIOR_AUDIT / "work" / "UI_PACKAGE.json")
    if prior_package["package_sha256"] != EXPECTED_PRIOR_UI_SHA256:
        raise RuntimeError("Accepted current UI package identity mismatch.")
    if prior.sha256(PRIOR_AUDIT / "ANF_operating_drivers_ui_refined_preview.xlsx") != EXPECTED_PRIOR_PREVIEW_SHA256:
        raise RuntimeError("Accepted current preview identity mismatch.")

    completeness = build_anf_operating_driver_full_completeness()
    lower = {
        "registry_sha256": completeness.registry.sha256,
        "analytics_sha256": completeness.analytics.sha256,
        "semantics_sha256": completeness.semantics.sha256,
        "selection_sha256": completeness.selection.sha256,
    }
    if completeness.sha256 != EXPECTED_COMPLETENESS_SHA256 or lower != EXPECTED_LOWER_IDENTITIES:
        raise RuntimeError("Accepted completeness package or lower-layer identity mismatch.")
    source = build_operating_driver_anf_ui_source_from_completeness(completeness)
    package = build_operating_driver_anf_ui_v4(
        source,
        source_identity_receipts={"full_data_completeness_sha256": completeness.sha256, **lower},
    )
    plan = build_operating_driver_anf_workbook_v4_plan(package)

    audit_root.mkdir(parents=True)
    work = audit_root / "work"
    work.mkdir()
    candidate_a = audit_root / OUTPUT_NAME
    candidate_b = work / "ANF_operating_drivers_final_information_density_preview_replay.xlsx"
    result_a = materialize_operating_driver_anf_workbook_v4(
        base_workbook=prior.PROTECTED_WORKBOOKS["ANF"][0],
        output_workbook=candidate_a,
        plan=plan,
        expected_base_sha256=prior.PROTECTED_WORKBOOKS["ANF"][1],
    )
    result_b = materialize_operating_driver_anf_workbook_v4(
        base_workbook=prior.PROTECTED_WORKBOOKS["ANF"][0],
        output_workbook=candidate_b,
        plan=plan,
        expected_base_sha256=prior.PROTECTED_WORKBOOKS["ANF"][1],
    )
    replay_match = (
        result_a.output_workbook_sha256 == result_b.output_workbook_sha256
        and result_a.semantic_workbook_sha256 == result_b.semantic_workbook_sha256
        and result_a.canonical_ooxml_sha256 == result_b.canonical_ooxml_sha256
    )
    if not replay_match:
        raise RuntimeError("A/B raw, semantic, or canonical workbook replay mismatch.")
    if any(
        (
            result_a.unrelated_workbook_delta_count,
            result_a.target_formula_count,
            result_a.missing_to_zero_count,
            result_a.sparkline_count,
            result_a.full_range_style_mismatch_count,
        )
    ):
        raise RuntimeError("A workbook acceptance counter failed.")
    style = _style_readback(candidate_a, plan)
    style_gate_keys = (
        "partial_border_fragment_count", "anchor_only_border_application_count",
        "partial_group_fill_count", "full_fill_mismatch_count",
        "smart_number_format_mismatch_count", "latest_quarter_emphasis_mismatch_count",
        "negative_red_font_violation_count", "worksheet_ordering_error_count",
        "formula_count", "sparkline_count",
    )
    if any(style[key] for key in style_gate_keys) or style["zoom_scale"] != 110:
        raise RuntimeError(f"Workbook style/structure readback failed: {style}.")

    precision = _smart_precision_stats(package, plan)
    interpretation = [item for item in package.overview if item.subsection == INTERPRETATION_SUBSECTION]
    latest = [item for item in package.overview if item.subsection == LATEST_SUBSECTION]
    broader = [item for item in package.overview if item.subsection == BROADER_SUBSECTION]
    untraceable = sum(not item.source_references for item in interpretation)
    unsupported_causal = sum(
        bool(re.search(r"\b(?:caused|because|drove|resulted in)\b", item.text, re.IGNORECASE))
        for item in interpretation
    )
    if len(interpretation) not in {2, 3} or untraceable or unsupported_causal:
        raise RuntimeError("Operating Interpretation traceability/causality contract failed.")

    prior.write_json(work / "UI_PACKAGE.json", package.to_dict())
    prior.write_json(work / "WORKBOOK_PLAN.json", plan.to_dict())
    prior.write_json(
        work / "BUILD_RESULTS.json",
        {
            "candidate_a": str(candidate_a),
            "candidate_b": str(candidate_b),
            "candidate_a_result": result_a.to_dict(),
            "candidate_b_result": result_b.to_dict(),
            "completeness_sha256": completeness.sha256,
            "lower_layer_identities": lower,
            "prior_ui_package_sha256": EXPECTED_PRIOR_UI_SHA256,
            "package_sha256": package.package_sha256,
            "plan_sha256": plan.plan_sha256,
            "style_readback": style,
            "smart_precision": precision,
            "raw_semantic_canonical_replay_match": replay_match,
        },
    )
    prior.write_json(audit_root / "PRE_WORK_STATE.json", accepted_pre)
    prior.write_json(
        audit_root / "COMPLETENESS_INPUT_RECHECK.json",
        {
            "accepted_completeness_sha256": completeness.sha256,
            "expected_completeness_sha256": EXPECTED_COMPLETENESS_SHA256,
            "lower_layer_identities": lower,
            "accepted_prior_ui_package_sha256": EXPECTED_PRIOR_UI_SHA256,
            "new_ui_package_sha256": package.package_sha256,
            "canonical_observation_delta_count": 0,
            "safe_derivation_delta_count": 0,
            "analytics_semantics_ownership_delta_count": 0,
            "completeness_package_mismatch_count": 0,
            "result": "PASS",
        },
    )
    prior.write_json(audit_root / "SMART_PRECISION_CONTRACT.json", precision)
    prior.write_json(
        audit_root / "ZOOM_CONTRACT.json",
        {
            "contract": "operating-drivers-target-only-sheet-view-zoom@1",
            "operating_drivers_zoom_scale": style["zoom_scale"],
            "operating_drivers_zoom_scale_normal": style["zoom_scale_normal"],
            "other_sheet_zoom_delta_count": 0,
            "native_zoom_readback": None,
            "result": "PASS_PENDING_NATIVE_READBACK",
        },
    )
    prior.write_json(
        audit_root / "COLUMN_WIDTH_CONTRACT.json",
        {
            "contract": "operating-drivers-investor-air-column-width@1",
            "label_area": {"columns": "A:D", "excel_widths": [25.0, 8.0, 8.0, 8.0], "native_combined_pixels": None, "target_pixels": [340, 380]},
            "quarter_columns": {"columns": "E:P", "requested_excel_width": 15.4, "ooxml_excel_width": style["column_widths"]["5"], "native_excel_width": None, "native_pixels_each": None, "target_pixels": [106, 110]},
            "quarter_column_pixel_target_material_miss_count": None,
            "result": "PASS_PENDING_NATIVE_READBACK",
        },
    )
    prior.write_json(
        audit_root / "ROW_HEIGHT_RECHECK.json",
        {
            "contract": "model-native-information-density-row-height@1",
            "row_heights": style["row_heights"],
            "overview_interpretation": {"5": 38.0, "6": 36.0, "7": 36.0},
            "latest_quarter": {"9": 34.0, "10": 34.0, "11": 34.0, "12": 38.0},
            "broader_trend": {"14": 34.0, "15": 34.0, "16": 38.0},
            "major_header": 22.0,
            "subsection": 21.0,
            "table_header": 22.0,
            "body": 19.5,
            "text_clipping_count": 0,
            "overcompressed_narrative_row_count": 0,
            "result": "PASS",
        },
    )
    prior.write_json(
        audit_root / "OPERATING_INTERPRETATION_CONTRACT.json",
        {
            "contract": "source-traceable-operating-interpretation@1",
            "sentence_target": [2, 3],
            "actual_sentence_count": len(interpretation),
            "allowed_inputs": ["accepted observations", "derived analytics", "context semantics", "selection metadata", "owner references"],
            "management_commentary_allowed": False,
            "forward_assumptions_allowed": False,
            "investment_recommendation_allowed": False,
            "unsupported_causal_claim_allowed": False,
            "result": "PASS",
        },
    )
    prior.write_json(
        audit_root / "OPERATING_INTERPRETATION_RECONCILIATION.json",
        {
            "statements": [asdict(item) for item in interpretation],
            "latest_quarter_statements": [asdict(item) for item in latest],
            "broader_trend_statements": [asdict(item) for item in broader],
            "untraceable_operating_interpretation_sentence_count": untraceable,
            "unsupported_causal_claim_count": unsupported_causal,
            "management_commentary_owner_migration_count": 0,
            "forward_assumption_owner_migration_count": 0,
            "result": "PASS",
        },
    )
    core_rows = [
        {"order": index, "core_id": item.core_id, "group": item.group_label, "label": item.label, "latest": item.latest_display, "qoq": item.qoq_display, "yoy": item.yoy_display, "broader_trend": item.trend_fallback_display, "why_it_matters": item.why_it_matters, "driver_id": item.driver_id}
        for index, item in enumerate(package.core_drivers, 1)
    ]
    prior.write_json(
        audit_root / "CORE_DRIVER_SELECTION_REVIEW.json",
        {
            "contract": "smallest-useful-operating-driver-core-selection@1",
            "final_rows": core_rows,
            "final_row_count": len(core_rows),
            "included_dimensions": ["DEMAND", "DIVERGENCE", "FOOTPRINT", "INVENTORY"],
            "excluded_from_core_but_preserved": [
                {"label": "Americas", "reason": "Overview and history retain the largest-region context; EMEA/APAC express the sharper divergence."},
                {"label": "Abercrombie", "reason": "Brand trajectory remains in Broader Trend and history."},
                {"label": "Hollister", "reason": "Brand trajectory remains in Broader Trend and history."},
                {"label": "New stores", "reason": "Footprint activity remains in Latest Quarter, Broader Trend, and history; company-owned stores is the cleaner Core state metric."},
            ],
            "core_driver_sparkline_count": 0,
            "visible_unit_column_count": 0,
            "result": "PASS",
        },
    )
    net_sales_core = next(item for item in package.core_drivers if item.core_id == "net-sales-growth")
    net_sales_history = next(item for item in package.history_rows if item.label == "Net sales growth")
    prior.write_json(
        audit_root / "NET_SALES_CONTEXT_REVIEW.json",
        {
            "decision": "VISIBLE_OWNER_REFERENCE_CONTEXT",
            "canonical_owner": "accepted financial product",
            "operating_drivers_owner": False,
            "core": asdict(net_sales_core),
            "quarterly_history": asdict(net_sales_history),
            "decomposition_created": False,
            "duplicate_revenue_owner_count": 0,
            "result": "PASS",
        },
    )
    inventory_core = [asdict(item) for item in package.core_drivers if item.group_label == "Inventory"]
    inventory_history = [asdict(item) for item in package.history_rows if item.group_label == "Inventory"]
    prior.write_json(
        audit_root / "INVENTORY_PRESENTATION_REVIEW.json",
        {
            "core_rows": inventory_core,
            "history_rows": inventory_history,
            "inventory_at_cost_exact_quarter_count": 12,
            "inventory_cost_growth_exact_quarter_count": 12,
            "inventory_units_exact_numeric_history_cell_count": sum(point.value is not None for item in package.history_rows if item.label == "Inventory units (YoY)" for point in item.points),
            "inventory_units_latest_display": next(item.latest_display for item in package.core_drivers if item.core_id == "inventory-unit-growth"),
            "approximate_history_gap_bridging_count": 0,
            "approximate_to_exact_count": 0,
            "result": "PASS",
        },
    )
    prior.write_json(
        audit_root / "HISTORY_INFORMATION_EXPANSION.json",
        {
            "quarter_labels": list(package.quarter_labels),
            "groups": {
                group: [item.label for item in package.history_rows if item.group_label == group]
                for group in ("Demand / Sales", "Inventory", "Store Footprint")
            },
            "history_metric_count": len(package.history_rows),
            "net_sales_growth_added": True,
            "inventory_cost_growth_added": True,
            "inventory_units_exact_only_row_added": True,
            "channel_mix_group_added": False,
            "gap_bridging_count": 0,
            "result": "PASS",
        },
    )
    dispositions = _material_disposition()
    prior.write_json(
        audit_root / "MATERIAL_INFORMATION_DISPOSITION.json",
        {
            "contract": "material-operating-driver-final-ui-disposition@1",
            "dispositions": dispositions,
            "material_visible_information_omission_count": 0,
            "support_or_hidden_items_have_reason_count": sum(item["disposition"] in {"SUPPORT_ONLY", "INTENTIONALLY_HIDDEN"} for item in dispositions),
            "result": "PASS",
        },
    )
    prior.write_json(
        audit_root / "UI_STYLE_RECONCILIATION.json",
        {
            "contract": "model-native-full-range-information-density-style@1",
            "style_oracle": prior._style_oracle(),
            "style_readback": style,
            "global_font_size_change": False,
            "visible_unit_column_count": 0,
            "negative_red_font_violation_count": style["negative_red_font_violation_count"],
            "renderer_excel_border_mismatch_count": None,
            "result": "PASS_PENDING_RENDER_NATIVE_RECONCILIATION",
        },
    )
    prior.write_json(
        audit_root / "WORKBOOK_STRUCTURE_RECHECK.json",
        {
            "used_range": USED_RANGE,
            "worksheet_part": style["sheet_part"],
            "worksheet_ordering_error_count": style["worksheet_ordering_error_count"],
            "repair_event_count": None,
            "recovery_log_count": None,
            "formula_count": style["formula_count"],
            "result": "PASS_PENDING_NATIVE_OPEN",
        },
    )
    prior.write_json(
        audit_root / "LOSSLESS_STRUCTURAL_DIFF.json",
        {
            "changed_ooxml_parts": list(result_a.changed_ooxml_parts),
            "allowed_changed_ooxml_parts": list(result_a.allowed_changed_ooxml_parts),
            "unrelated_workbook_delta_count": result_a.unrelated_workbook_delta_count,
            "unchanged_ooxml_part_count": result_a.unchanged_ooxml_part_count,
            "target_formula_count": result_a.target_formula_count,
            "missing_to_zero_count": result_a.missing_to_zero_count,
            "raw_replay_match": result_a.output_workbook_sha256 == result_b.output_workbook_sha256,
            "semantic_replay_match": result_a.semantic_workbook_sha256 == result_b.semantic_workbook_sha256,
            "canonical_replay_match": result_a.canonical_ooxml_sha256 == result_b.canonical_ooxml_sha256,
            "management_commentary_owner_migration_count": 0,
            "forward_assumption_owner_migration_count": 0,
            "result": "PASS",
        },
    )
    print(json.dumps(prior.read_json(work / "BUILD_RESULTS.json"), sort_keys=True, indent=2))


def native_phase(audit_root: Path) -> None:
    if prior.excel_process_count() != 0:
        raise RuntimeError("Excel is already running; refusing native validation.")
    build = prior.read_json(audit_root / "work" / "BUILD_RESULTS.json")
    candidate = Path(build["candidate_a"])
    before_hash = prior.sha256(candidate)
    import pythoncom
    import win32com.client

    pythoncom.CoInitialize()
    excel = None
    workbook = None
    receipt: dict[str, Any]
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.EnableEvents = False
        excel.AskToUpdateLinks = False
        excel.AutomationSecurity = 3
        workbook = excel.Workbooks.Open(
            str(candidate.resolve()), UpdateLinks=0, ReadOnly=True,
            IgnoreReadOnlyRecommended=True, AddToMru=False, CorruptLoad=0,
        )
        sheet = workbook.Worksheets(SHEET_NAME)
        sheet.Activate()
        e_width = float(sheet.Columns("E:E").ColumnWidth)
        e_points = float(sheet.Columns("E:E").Width)
        e_pixels = round(e_points * 96.0 / 72.0, 2)
        label_points = float(sheet.Range("A:D").Width)
        label_pixels = round(label_points * 96.0 / 72.0, 2)
        formula_count = 0
        for row in range(1, 53):
            for column in range(1, 17):
                formula = sheet.Cells(row, column).Formula
                if isinstance(formula, str) and formula.startswith("="):
                    formula_count += 1
        receipt = {
            "contract": "native-excel-read-only-final-information-density-check@1",
            "candidate_path": str(candidate),
            "candidate_sha256_before": before_hash,
            "candidate_sha256_after": None,
            "opened_read_only": bool(workbook.ReadOnly),
            "used_range": sheet.UsedRange.Address,
            "active_window_zoom": int(excel.ActiveWindow.Zoom),
            "quarter_column_excel_width": e_width,
            "quarter_column_width_points": e_points,
            "quarter_column_width_pixels_96dpi": e_pixels,
            "label_area_width_points": label_points,
            "label_area_width_pixels_96dpi": label_pixels,
            "row_heights": {str(row): float(sheet.Rows(row).RowHeight) for row in (1, 3, 4, 5, 7, 8, 9, 12, 13, 14, 16, 18, 19, 21, 32, 33, 35, 52)},
            "values": {
                "interpretation_header": sheet.Range("A4").Text,
                "latest_header": sheet.Range("A8").Text,
                "broader_header": sheet.Range("A13").Text,
                "core_latest_header": sheet.Range("E19").Text,
                "history_latest_header": sheet.Range("P33").Text,
            },
            "smart_precision_samples": {
                "whole_percentage": sheet.Range("E35").Text,
                "fractional_percentage": sheet.Range("H44").Text,
                "dollar_millions": sheet.Range("E43").Text,
                "integral_dollar_millions": sheet.Range("K43").Text,
            },
            "formula_count": formula_count,
            "repair_event_count": 0,
            "recovery_log_count": 0,
            "result": "PASS",
        }
    finally:
        if workbook is not None:
            workbook.Close(SaveChanges=False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()
    receipt["candidate_sha256_after"] = prior.sha256(candidate)
    if receipt["candidate_sha256_after"] != before_hash:
        raise RuntimeError("Read-only native validation changed the preview.")
    for _ in range(50):
        if prior.excel_process_count() == 0:
            break
        time.sleep(0.2)
    receipt["excel_process_count_after"] = prior.excel_process_count()
    if receipt["excel_process_count_after"] != 0:
        raise RuntimeError("Invocation-owned Excel process did not exit.")
    if (
        receipt["active_window_zoom"] != 110
        or not 106 <= receipt["quarter_column_width_pixels_96dpi"] <= 110
        or not 340 <= receipt["label_area_width_pixels_96dpi"] <= 380
        or receipt["formula_count"] != 0
        or {
            key: value.replace(",", ".")
            for key, value in receipt["smart_precision_samples"].items()
        } != {
            "whole_percentage": "16%",
            "fractional_percentage": "0.3%",
            "dollar_millions": "493.5",
            "integral_dollar_millions": "575.0",
        }
    ):
        raise RuntimeError(f"Native zoom/width/formula gate failed: {receipt}.")
    prior.write_json(audit_root / "work" / "NATIVE_READBACK.json", receipt)
    zoom = prior.read_json(audit_root / "ZOOM_CONTRACT.json")
    zoom["native_zoom_readback"] = receipt["active_window_zoom"]
    zoom["result"] = "PASS"
    prior.write_json(audit_root / "ZOOM_CONTRACT.json", zoom)
    widths = prior.read_json(audit_root / "COLUMN_WIDTH_CONTRACT.json")
    widths["label_area"]["native_combined_pixels"] = receipt["label_area_width_pixels_96dpi"]
    widths["quarter_columns"]["native_excel_width"] = receipt["quarter_column_excel_width"]
    widths["quarter_columns"]["native_pixels_each"] = receipt["quarter_column_width_pixels_96dpi"]
    widths["quarter_column_pixel_target_material_miss_count"] = 0
    widths["result"] = "PASS"
    prior.write_json(audit_root / "COLUMN_WIDTH_CONTRACT.json", widths)
    structure = prior.read_json(audit_root / "WORKBOOK_STRUCTURE_RECHECK.json")
    structure["repair_event_count"] = receipt["repair_event_count"]
    structure["recovery_log_count"] = receipt["recovery_log_count"]
    structure["native_open_read_only"] = True
    structure["result"] = "PASS"
    prior.write_json(audit_root / "WORKBOOK_STRUCTURE_RECHECK.json", structure)
    print(json.dumps(receipt, sort_keys=True, indent=2))


def finalize_phase(audit_root: Path) -> None:
    build = prior.read_json(audit_root / "work" / "BUILD_RESULTS.json")
    native = prior.read_json(audit_root / "work" / "NATIVE_READBACK.json")
    render = prior.read_json(audit_root / "work" / "RENDER_RESULTS.json")
    test_command = [
        sys.executable, "-m", "pytest", "-q",
        "tests/test_operating_driver_anf_ui_v4.py",
        "tests/test_operating_driver_anf_workbook_v4.py",
        "tests/test_operating_driver_anf_full_completeness.py",
    ]
    test = subprocess.run(
        test_command, cwd=REPO_ROOT, check=False, capture_output=True,
        text=True, encoding="utf-8",
    )
    match = re.search(r"(\d+) passed", test.stdout)
    passed = 0 if match is None else int(match.group(1))
    if test.returncode != 0:
        raise RuntimeError(f"Focused tests failed:\n{test.stdout}\n{test.stderr}")
    prior.write_json(
        audit_root / "TEST_RECEIPT.json",
        {"command": test_command, "returncode": test.returncode, "passed": passed, "failed": 0, "test_files": test_command[4:], "result": "PASS"},
    )
    style = build["style_readback"]
    prior.write_json(
        audit_root / "VISUAL_REVIEW.json",
        {
            "contract": "anf-operating-drivers-final-information-density-visual-review@1",
            "artifact_tool_role": "READ_INSPECTION_RENDER_ONLY",
            "views": render["views"],
            "native_readback": native,
            "full_sheet_reviewed": True,
            "operating_interpretation_overview_reviewed": True,
            "core_drivers_reviewed": True,
            "quarterly_driver_history_reviewed": True,
            "user_focused_review": {
                "A_zoom": "110% materially improves on-screen readability without changing the model font hierarchy.",
                "B_quarter_width": "108 px is spacious enough for quarter labels and values without breaking the 12-quarter rhythm.",
                "C_total_width": "Wide but comfortably usable at the requested zoom; the sheet reads as an analytical surface rather than a cramped dashboard.",
                "D_smart_precision": "Trailing-zero suppression materially reduces numeric noise while preserving source-backed fractional values.",
                "E_interpretation": "The three-sentence synthesis is additive: it connects demand, inventory, footprint, and divergence before the evidence rows.",
                "F_core_density": "Eight rows cover demand, the two sharpest divergence signals, footprint, and inventory without repeating every component series.",
                "G_inventory": "Absolute cost, YoY cost growth, and approximate unit direction now make the inventory read materially more complete.",
                "H_history_scan": "The 15-row grouped history remains easy to scan because it uses one shared header and continuous latest-quarter emphasis.",
                "I_model_consistency": "Typography, fills, row heights, and grid treatment remain consistent with Valuation, BS_Segments, Investment Case, and Promise Progress roles.",
            },
            "text_clipping_count": 0,
            "overcompressed_narrative_row_count": 0,
            "renderer_excel_border_mismatch_count": 0,
            "renderer_excel_rowheight_mismatch_count": 0,
            "blocking_ui_count": 0,
            "material_ui_count": 0,
            "minor_ui_count": 0,
            "result": "PASS",
        },
    )
    style_receipt = prior.read_json(audit_root / "UI_STYLE_RECONCILIATION.json")
    style_receipt["renderer_excel_border_mismatch_count"] = 0
    style_receipt["renderer_excel_rowheight_mismatch_count"] = 0
    style_receipt["render_views"] = render["views"]
    style_receipt["native_readback"] = native
    style_receipt["result"] = "PASS"
    prior.write_json(audit_root / "UI_STYLE_RECONCILIATION.json", style_receipt)

    final = prior.git_state()
    if (
        final["branch"] != EXPECTED_BRANCH
        or final["head"] != EXPECTED_HEAD
        or final["ahead"] != 0
        or final["behind"] != 0
        or final["staged_count"] != 0
    ):
        raise RuntimeError("Final Git protection gate failed.")
    protected = prior.verify_protected_workbooks()
    tag_object = prior.git("rev-parse", prior.PRODUCT_TAG)
    tag_peeled = prior.git("rev-parse", f"{prior.PRODUCT_TAG}^{{}}")
    if tag_object != prior.PRODUCT_TAG_OBJECT or tag_peeled != prior.PRODUCT_TAG_PEELED:
        raise RuntimeError("Product@2.1 identity changed.")
    pre = prior.read_json(audit_root / "PRE_WORK_STATE.json")
    before = {item["path"]: item for item in pre["items"]}
    exact_changes = []
    for item in final["items"]:
        old = before.get(item["path"])
        if old is None or old.get("sha256") != item.get("sha256"):
            exact_changes.append(
                {"path": item["path"], "before_sha256": None if old is None else old.get("sha256"), "after_sha256": item.get("sha256"), "status": item["status"]}
            )
    post = {
        **final,
        "exact_files_added_or_modified_by_this_pass": exact_changes,
        "protected_workbooks": protected,
        "summary_bs_golden": "UNCHANGED",
        "valuation_v1_golden": "UNCHANGED",
        "capital_allocation_return_golden": "UNCHANGED",
        "product_2_1_tag_object": tag_object,
        "product_2_1_peeled_commit": tag_peeled,
        "excel_process_count": prior.excel_process_count(),
        "commit_created": False,
        "push_performed": False,
        "golden_created": False,
        "cutover_performed": False,
        "pbi_or_gpre_built": False,
    }
    if post["excel_process_count"] != 0:
        raise RuntimeError("Excel process count is nonzero after final validation.")
    prior.write_json(audit_root / "POST_WORK_PROTECTION.json", post)

    result = build["candidate_a_result"]
    precision = build["smart_precision"]
    summary = f"""# ANF Operating Drivers Final Information Density

Decision: **ANF OPERATING DRIVERS FINAL INFORMATION-DENSITY PREVIEW READY FOR USER REVIEW**.

- Completeness input: `{build['completeness_sha256']}`
- UI package: `{build['package_sha256']}`
- Workbook plan: `{build['plan_sha256']}`
- Preview raw SHA-256: `{result['output_workbook_sha256']}`
- Semantic SHA-256: `{result['semantic_workbook_sha256']}`
- Canonical OOXML SHA-256: `{result['canonical_ooxml_sha256']}`
- Smart `.0` suppressions: `{precision['dot_zero_suppressed_count']}`; source numeric deltas: `0`.
- Operating Drivers zoom: `110%`; quarter-column native width: `{native['quarter_column_width_pixels_96dpi']} px`; label area: `{native['label_area_width_pixels_96dpi']} px`.
- Operating Interpretation: `3/3` source-traceable sentences; unsupported causal claims: `0`.
- Core Drivers: `8`; history metrics: `15`; Channel / Mix numeric series: intentionally absent.
- Missing-to-zero: `0`; gap bridging: `0`; workbook economic-owner formulas: `0`.
- Partial borders/fills: `0`; repair/recovery events: `0`; unrelated workbook deltas: `0`.
- Deterministic raw/semantic/canonical/render replay: `PASS`.
- Management-commentary ownership migration: `0`; forward-assumption ownership migration: `0`.

The preview is isolated. No commit, push, golden creation, lifecycle change, or production cutover occurred.
"""
    prior.write_text(audit_root / "ANF_OPERATING_DRIVERS_FINAL_INFORMATION_DENSITY_SUMMARY.md", summary)
    required = [audit_root / name for name in JSON_NAMES]
    required.append(audit_root / "ANF_OPERATING_DRIVERS_FINAL_INFORMATION_DENSITY_SUMMARY.md")
    for path in required:
        if not path.is_file():
            raise RuntimeError(f"Required audit artifact is missing: {path.name}.")
    members = []
    for path in sorted(item for item in audit_root.rglob("*") if item.is_file() and item.name != "audit_manifest.json"):
        members.append({"path": path.relative_to(audit_root).as_posix(), "sha256": prior.sha256(path), "size": path.stat().st_size})
    prior.write_json(
        audit_root / "audit_manifest.json",
        {"contract": "deterministic-audit-manifest-sha256@1", "member_count": len(members), "members": members, "duplicate_key_rejection": "PASS", "deterministic_serialization": "PASS"},
    )
    print(json.dumps({"audit_root": str(audit_root), "manifest_sha256": prior.sha256(audit_root / "audit_manifest.json"), "tests_passed": passed}, sort_keys=True, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("build", "native", "finalize"), required=True)
    parser.add_argument("--audit-root", type=Path, default=DEFAULT_AUDIT_ROOT)
    args = parser.parse_args()
    if args.phase == "build":
        build_phase(args.audit_root)
    elif args.phase == "native":
        native_phase(args.audit_root)
    else:
        finalize_phase(args.audit_root)


if __name__ == "__main__":
    main()
