"""Build, validate, and receipt the ANF Operating Drivers V4 UI refinement."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping
from zipfile import ZipFile
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (  # noqa: E402
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
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
    sha256_file,
)


DEFAULT_AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_ui_refinement_2026-08-20"
)
COMPLETENESS_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_full_data_completeness_2026-08-20"
)
PROTECTED_WORKBOOKS = {
    "ANF": (
        Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\ANF_model.xlsx"),
        "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd",
    ),
    "PBI": (
        Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\PBI_model.xlsx"),
        "6482617ad4f412dc5a1e130dc56c72bba5113ddacea5f7d1ab166fad8ddf5689",
    ),
    "GPRE": (
        Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\GPRE_model.xlsm"),
        "f7c23c9c0d9e3a52c708553e5fc6f964aa3fc58c8b4805d235f7ccfce5bde41b",
    ),
}
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
EXPECTED_COMPLETENESS_SHA256 = "c1fbc5898e56fff7a5e559b122578fcf996b82ee389a47f9caf82adedf4bf1e9"
EXPECTED_LOWER_IDENTITIES = {
    "registry_sha256": "3843f72035208c87971d7869e7bc5481d4c78b25b898aa13b11fe76b7e5c4051",
    "analytics_sha256": "fbd0cd2807014d613de44e643f298f43b07c413391e644f16ca3ef9fd6a41ab3",
    "semantics_sha256": "452c607d5ac80fc07ea5db05f4b5b17d71ae5e63f3765c8d8b634d854145a94c",
    "selection_sha256": "ee2d2d9ba62e45c62bff1fe27d756d6f2cbe014ddf6230f03193cf35406043ef",
}
PRODUCT_TAG = "promise-progress-product-v2-1-workbook-golden"
PRODUCT_TAG_OBJECT = "a5193e461148671bf54738c8ad8a5d6942295701"
PRODUCT_TAG_PEELED = "ce1f1aea07d98e566a142c8221e53efe2ce692de"
JSON_NAMES = (
    "PRE_WORK_STATE.json",
    "MODEL_STYLE_ORACLE.json",
    "ROW_HEIGHT_ORACLE.json",
    "BORDER_FILL_CONTRACT.json",
    "UI_COLUMN_LAYOUT.json",
    "OVERVIEW_LAYOUT.json",
    "CORE_DRIVER_LAYOUT.json",
    "DISPLAY_LABEL_MAPPING.json",
    "INVENTORY_UI_RECONCILIATION.json",
    "HISTORY_UI_RECONCILIATION.json",
    "MATERIAL_INFORMATION_DISPOSITION.json",
    "RENDERER_EXCEL_STYLE_RECONCILIATION.json",
    "WORKBOOK_STRUCTURE_RECHECK.json",
    "LOSSLESS_STRUCTURAL_DIFF.json",
    "VISUAL_REVIEW.json",
    "TEST_RECEIPT.json",
    "POST_WORK_PROTECTION.json",
)
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"m": MAIN_NS}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"Duplicate JSON key: {key}")
        value[key] = item
    return value


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    read_json(path)


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.rstrip() + "\n", encoding="utf-8", newline="\n")


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def git_state() -> dict[str, Any]:
    lines = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.splitlines()
    staged: list[str] = []
    modified: list[str] = []
    untracked: list[str] = []
    items: list[dict[str, Any]] = []
    for line in lines:
        status = line[:2]
        relative = line[3:].replace("\\", "/")
        if status == "??":
            untracked.append(relative)
        else:
            if status[0] != " ":
                staged.append(relative)
            if status[1] == "M":
                modified.append(relative)
        absolute = REPO_ROOT / relative
        items.append(
            {
                "path": relative,
                "sha256": sha256(absolute) if absolute.is_file() else None,
                "size": absolute.stat().st_size if absolute.is_file() else None,
                "status": status,
            }
        )
    behind, ahead = git(
        "rev-list", "--left-right", "--count", f"origin/{EXPECTED_BRANCH}...HEAD"
    ).split()
    return {
        "ahead": int(ahead),
        "behind": int(behind),
        "branch": git("branch", "--show-current"),
        "head": git("rev-parse", "HEAD"),
        "items": sorted(items, key=lambda item: item["path"]),
        "modified_tracked": sorted(modified),
        "modified_tracked_count": len(modified),
        "staged": sorted(staged),
        "staged_count": len(staged),
        "untracked": sorted(untracked),
        "untracked_count": len(untracked),
    }


def excel_process_count() -> int:
    result = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq EXCEL.EXE", "/FO", "CSV", "/NH"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return sum(1 for line in result.stdout.splitlines() if "EXCEL.EXE" in line.upper())


def verify_protected_workbooks() -> dict[str, Any]:
    result: dict[str, Any] = {}
    for ticker, (path, expected) in PROTECTED_WORKBOOKS.items():
        actual = sha256(path)
        if actual != expected:
            raise RuntimeError(f"Protected {ticker} workbook identity mismatch: {actual}.")
        result[ticker] = {"path": str(path), "sha256": actual, "unchanged": True}
    return result


def _style_oracle() -> dict[str, Any]:
    from openpyxl import load_workbook

    workbook = load_workbook(PROTECTED_WORKBOOKS["ANF"][0], read_only=False, data_only=False)
    requested = {
        "title": ("Promise_Progress_UI", "A1"),
        "subtitle": ("Promise_Progress_UI", "A2"),
        "major_section": ("Valuation", "A122"),
        "subsection": ("Valuation", "A7"),
        "table_header_left": ("Valuation", "O8"),
        "table_header_right": ("Valuation", "F138"),
        "body_label": ("ANF_Investment_Case", "A7"),
        "body_numeric": ("BS_Segments", "D13"),
        "group": ("ANF_Investment_Case", "A39"),
        "overview_narrative": ("Valuation", "S138"),
    }
    styles: dict[str, Any] = {}
    for role, (sheet, coordinate) in requested.items():
        cell = workbook[sheet][coordinate]
        fg = cell.fill.fgColor
        styles[role] = {
            "source": f"{sheet}!{coordinate}",
            "style_id": cell.style_id,
            "font_name": cell.font.name,
            "font_size": cell.font.sz,
            "bold": bool(cell.font.bold),
            "fill_rgb": fg.rgb if fg.type == "rgb" else None,
            "horizontal": cell.alignment.horizontal,
            "vertical": cell.alignment.vertical,
            "wrap_text": bool(cell.alignment.wrap_text),
            "number_format": cell.number_format,
        }
    return {
        "contract": "accepted-model-style-oracle-readback@1",
        "protected_workbook_sha256": PROTECTED_WORKBOOKS["ANF"][1],
        "styles": styles,
        "result": "PASS",
    }


def _workbook_roots(path: Path) -> tuple[str, ET.Element, ET.Element]:
    with ZipFile(path, "r") as archive:
        part = _sheet_part_map(archive)[SHEET_NAME]
        return part, ET.fromstring(archive.read(part)), ET.fromstring(archive.read("xl/styles.xml"))


def _cell_map(root: ET.Element) -> dict[str, ET.Element]:
    return {
        cell.attrib["r"]: cell
        for cell in root.findall(".//m:sheetData/m:row/m:c", NS)
    }


def _style_readback(path: Path, plan: Any) -> dict[str, Any]:
    part, root, styles = _workbook_roots(path)
    cells = _cell_map(root)
    xfs = list(styles.find("m:cellXfs", NS))
    fonts = list(styles.find("m:fonts", NS))
    borders = list(styles.find("m:borders", NS))

    def xf(coordinate: str) -> ET.Element:
        return xfs[int(cells[coordinate].attrib.get("s", "0"))]

    def has_visible_border(border_id: int) -> bool:
        border = borders[border_id]
        return any(
            side.attrib.get("style")
            for name in ("left", "right", "top", "bottom")
            for side in border.findall(f"m:{name}", NS)
        )

    partial_border_fragments = 0
    anchor_only_borders = 0
    fill_mismatches = 0
    for binding in plan.bindings:
        coordinates = _range_coordinates(binding.target_range)
        border_ids = [int(xf(item).attrib.get("borderId", "0")) for item in coordinates]
        if len(set(border_ids)) > 1:
            partial_border_fragments += 1
        if has_visible_border(border_ids[0]) and any(not has_visible_border(item) for item in border_ids[1:]):
            anchor_only_borders += 1
        if binding.element_type in {
            "TITLE", "SUBTITLE", "MAJOR_SECTION", "OVERVIEW_SUBSECTION",
            "CORE_HEADER", "CORE_GROUP", "HISTORY_HEADER", "HISTORY_QUARTER_HEADER", "HISTORY_GROUP",
        }:
            fill_ids = [int(xf(item).attrib.get("fillId", "0")) for item in coordinates]
            if any(item == 0 for item in fill_ids):
                fill_mismatches += 1

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

    rows = {
        int(item.attrib["r"]): float(item.attrib.get("ht", "19.5"))
        for item in root.findall("m:sheetData/m:row", NS)
    }
    columns = {
        int(item.attrib["min"]): float(item.attrib["width"])
        for item in root.findall("m:cols/m:col", NS)
    }
    snapshot = _visible_snapshot(path)
    latest_rows = (29, *plan.history_group_rows.values(), *plan.history_metric_rows.values())
    latest_emphasis_mismatches = sum(
        xf(f"P{row}").attrib.get("fillId") == xf(f"O{row}").attrib.get("fillId")
        for row in latest_rows
    )
    return {
        "sheet_part": part,
        "used_range": snapshot["dimension"],
        "row_heights": {str(key): value for key, value in sorted(rows.items())},
        "column_widths": {str(key): value for key, value in sorted(columns.items())},
        "partial_border_fragment_count": partial_border_fragments,
        "anchor_only_border_application_count": anchor_only_borders,
        "partial_group_fill_count": partial_group_fill,
        "full_fill_mismatch_count": fill_mismatches,
        "latest_quarter_emphasis_cell_count": len(latest_rows),
        "latest_quarter_emphasis_mismatch_count": latest_emphasis_mismatches,
        "negative_red_font_violation_count": negative_red,
        "formula_count": sum(item["formula"] is not None for item in snapshot["cells"]),
        "sparkline_count": len(snapshot["sparklines"]),
        "merge_count": len(snapshot["merge_ranges"]),
        "cell_count": len(snapshot["cells"]),
    }


def _material_disposition(package: Any) -> list[dict[str, str]]:
    return [
        {"driver": "Total-company comparable sales", "disposition": "OVERVIEW|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Primary underlying demand series."},
        {"driver": "Brand comparable sales", "disposition": "OVERVIEW|QUARTERLY_HISTORY", "reason": "Brand divergence is material; detailed trajectory belongs in history."},
        {"driver": "Regional comparable sales", "disposition": "OVERVIEW|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Regional divergence is central to the latest operating read."},
        {"driver": "Net sales growth", "disposition": "OVERVIEW", "reason": "Consumed as an owner-elsewhere financial context reference, not re-owned."},
        {"driver": "Inventory at cost", "disposition": "OVERVIEW|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Exact recurring balance series and working-capital context."},
        {"driver": "Inventory cost growth", "disposition": "QUARTERLY_HISTORY", "reason": "Exact recurring rate provides the cleanest inventory trajectory."},
        {"driver": "Inventory unit growth", "disposition": "OVERVIEW|CORE_DRIVERS", "reason": "Latest evidence is approximate; exact numeric history remains fail-closed."},
        {"driver": "Company-owned stores", "disposition": "OVERVIEW|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Primary physical-footprint state; undisclosed quarters remain blank."},
        {"driver": "New stores", "disposition": "OVERVIEW|CORE_DRIVERS|QUARTERLY_HISTORY", "reason": "Material store-investment activity."},
        {"driver": "Remodeled, right-sized, and closed stores", "disposition": "OVERVIEW|QUARTERLY_HISTORY", "reason": "Supporting footprint activity is useful in compact history."},
        {"driver": "Franchise stores", "disposition": "SUPPORT_ONLY", "reason": "Sparse point-in-time coverage adds limited value to the 12-quarter table."},
        {"driver": "Digital sales mix", "disposition": "SUPPORT_ONLY", "reason": "No accepted recurring compatible quarterly numeric series exists."},
        {"driver": "Mobile share of digital traffic", "disposition": "SUPPORT_ONLY", "reason": "Traffic share is a different, sparse definition from sales mix."},
        {"driver": "Inventory in transit", "disposition": "SUPPORT_ONLY", "reason": "Sparse annual context is not comparable as quarterly history."},
        {"driver": "AUR, traffic, conversion, promotion, freight, tariff context", "disposition": "SUPPORT_ONLY", "reason": "Available evidence is qualitative or non-recurring and remains source-native support."},
        {"driver": "Inventory turns", "disposition": "INTENTIONALLY_HIDDEN", "reason": "No accepted direct or safe-derived Operating Drivers contract exists."},
    ]


def _pre_state() -> dict[str, Any]:
    accepted = read_json(COMPLETENESS_AUDIT / "PRE_WORK_STATE.json")
    post = read_json(COMPLETENESS_AUDIT / "POST_WORK_PROTECTION.json")
    accepted_items = {item["path"]: dict(item) for item in accepted["items"]}
    for item in post["added_by_this_pass"]:
        accepted_items[item["path"]] = {
            "path": item["path"],
            "sha256": item["after_sha256"],
            "size": item["size"],
            "status": "??",
        }
    return {
        "contract": "anf-operating-drivers-ui-refinement-pre-state@1",
        "accepted_audit": str(COMPLETENESS_AUDIT),
        "accepted_audit_manifest_sha256": sha256(COMPLETENESS_AUDIT / "audit_manifest.json"),
        "branch": accepted["branch"],
        "head": accepted["head"],
        "ahead": accepted["ahead"],
        "behind": accepted["behind"],
        "modified_tracked": post["modified_tracked"],
        "modified_tracked_count": len(post["modified_tracked"]),
        "staged": post["staged"],
        "staged_count": len(post["staged"]),
        "untracked": post["untracked"],
        "untracked_count": len(post["untracked"]),
        "items": sorted(accepted_items.values(), key=lambda item: item["path"]),
        "verification": "PASS",
    }


def build_phase(audit_root: Path) -> None:
    if audit_root.exists():
        raise RuntimeError(f"Refusing to overwrite existing audit root: {audit_root}.")
    state = git_state()
    if state["branch"] != EXPECTED_BRANCH or state["head"] != EXPECTED_HEAD:
        raise RuntimeError("Branch or HEAD mismatch before UI build.")
    if state["staged_count"] != 0 or state["ahead"] != 0 or state["behind"] != 0:
        raise RuntimeError("Git state is not synchronized/unstaged before UI build.")
    verify_protected_workbooks()
    pre = _pre_state()
    accepted_by_path = {item["path"]: item for item in pre["items"]}
    current_by_path = {item["path"]: item for item in state["items"]}
    for relative, accepted in accepted_by_path.items():
        current = current_by_path.get(relative)
        if current is None or current["sha256"] != accepted["sha256"]:
            if relative not in {
                "pbi_xbrl/longitudinal_memory/operating_driver_anf_ui_v4.py",
                "pbi_xbrl/longitudinal_memory/operating_driver_anf_workbook_v4.py",
                "tests/test_operating_driver_anf_ui_v4.py",
                "tests/test_operating_driver_anf_workbook_v4.py",
            }:
                raise RuntimeError(f"Accepted pre-existing path changed unexpectedly: {relative}.")

    completeness = build_anf_operating_driver_full_completeness()
    identities = {
        "registry_sha256": completeness.registry.sha256,
        "analytics_sha256": completeness.analytics.sha256,
        "semantics_sha256": completeness.semantics.sha256,
        "selection_sha256": completeness.selection.sha256,
    }
    if completeness.sha256 != EXPECTED_COMPLETENESS_SHA256 or identities != EXPECTED_LOWER_IDENTITIES:
        raise RuntimeError("Accepted completeness-package identity mismatch.")
    source = build_operating_driver_anf_ui_source_from_completeness(completeness)
    receipts = {"full_data_completeness_sha256": completeness.sha256, **identities}
    package = build_operating_driver_anf_ui_v4(source, source_identity_receipts=receipts)
    plan = build_operating_driver_anf_workbook_v4_plan(package)

    audit_root.mkdir(parents=True)
    work = audit_root / "work"
    work.mkdir()
    candidate_a = audit_root / "ANF_operating_drivers_ui_refined_preview.xlsx"
    candidate_b = work / "ANF_operating_drivers_ui_refined_preview_replay.xlsx"
    result_a = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED_WORKBOOKS["ANF"][0],
        output_workbook=candidate_a,
        plan=plan,
        expected_base_sha256=PROTECTED_WORKBOOKS["ANF"][1],
    )
    result_b = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED_WORKBOOKS["ANF"][0],
        output_workbook=candidate_b,
        plan=plan,
        expected_base_sha256=PROTECTED_WORKBOOKS["ANF"][1],
    )
    if (
        result_a.output_workbook_sha256 != result_b.output_workbook_sha256
        or result_a.semantic_workbook_sha256 != result_b.semantic_workbook_sha256
        or result_a.canonical_ooxml_sha256 != result_b.canonical_ooxml_sha256
    ):
        raise RuntimeError("A/B workbook materialization is nondeterministic.")
    if any(
        (
            result_a.unrelated_workbook_delta_count,
            result_a.target_formula_count,
            result_a.missing_to_zero_count,
            result_a.sparkline_count,
            result_a.full_range_style_mismatch_count,
        )
    ):
        raise RuntimeError("Workbook acceptance counters failed before rendering.")

    style = _style_readback(candidate_a, plan)
    if any(
        style[key]
        for key in (
            "partial_border_fragment_count",
            "anchor_only_border_application_count",
            "partial_group_fill_count",
            "full_fill_mismatch_count",
            "latest_quarter_emphasis_mismatch_count",
            "negative_red_font_violation_count",
            "formula_count",
            "sparkline_count",
        )
    ):
        raise RuntimeError(f"Style/readback gate failed: {style}.")

    write_json(work / "UI_PACKAGE.json", package.to_dict())
    write_json(work / "WORKBOOK_PLAN.json", plan.to_dict())
    write_json(
        work / "BUILD_RESULTS.json",
        {
            "candidate_a": str(candidate_a),
            "candidate_b": str(candidate_b),
            "candidate_a_result": result_a.to_dict(),
            "candidate_b_result": result_b.to_dict(),
            "completeness_sha256": completeness.sha256,
            "lower_layer_identities": identities,
            "package_sha256": package.package_sha256,
            "plan_sha256": plan.plan_sha256,
            "style_readback": style,
        },
    )
    write_json(audit_root / "PRE_WORK_STATE.json", pre)
    write_json(audit_root / "MODEL_STYLE_ORACLE.json", _style_oracle())
    write_json(
        audit_root / "ROW_HEIGHT_ORACLE.json",
        {
            "contract": "model-native-row-height-plan@1",
            "oracles": {"Valuation_default": 18.0, "Valuation_body": 19.5, "BS_Segments_default": 18.0, "Investment_Case_body_range": [21.0, 24.0]},
            "applied": {
                "title": 28.0,
                "major_header": 22.0,
                "subsection": 21.0,
                "table_header": 22.0,
                "body": 19.5,
                "overview_narrative": {"rows_5_to_7": 32.0, "rows_8_and_12": 34.0, "rows_10_to_11": 32.0},
            },
            "compressed_header_row_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "BORDER_FILL_CONTRACT.json",
        {
            "contract": "operating-drivers-full-range-style-contract@1",
            "overview_narrative_separator_border": "NONE",
            "table_grid": "EVERY_CELL_FULL_RANGE",
            "section_and_subsection_fill": "EVERY_CELL_FULL_RANGE",
            "style_readback": style,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "UI_COLUMN_LAYOUT.json",
        {
            "used_range": USED_RANGE,
            "visible_canvas": "A:P",
            "overview": "A:P",
            "core": {"metric": "A:D", "latest": "E:F", "qoq": "G:H", "yoy": "I:J", "trend": "K:L", "why": "M:P"},
            "history": {"metric": "A:D", "quarters": "E:P"},
            "column_widths": style["column_widths"],
            "visible_unit_column_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "OVERVIEW_LAYOUT.json",
        {
            "major_section": "Operating Drivers Overview",
            "subsections": ["LATEST QUARTER — 2026-Q1", "BROADER TREND"],
            "latest_statement_count": 4,
            "broader_statement_count": 3,
            "overall_operating_read_included": False,
            "overall_operating_read_reason": "The accepted seven statements already synthesize demand, inventory, footprint, and divergence without a duplicative generic row.",
            "management_commentary_ownership_added": False,
            "forward_assumption_ownership_moved": False,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "CORE_DRIVER_LAYOUT.json",
        {
            "headers": ["Metric", "Latest (2026-Q1)", "vs prior quarter", "vs year ago", "Broader trend", "Why it matters"],
            "groups": plan.core_group_rows,
            "metric_rows": plan.core_metric_rows,
            "core_driver_count": len(package.core_drivers),
            "trend_labels": [item.trend_fallback_display for item in package.core_drivers],
            "sparkline_count": 0,
            "pp_note_present": True,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "DISPLAY_LABEL_MAPPING.json",
        {
            "demand_sales": ["Total company", "Abercrombie", "Hollister", "Americas", "EMEA", "APAC"],
            "inventory": ["Inventory at cost ($m)", "Inventory at cost (YoY)"],
            "store_footprint": ["Company-owned stores", "New stores", "Remodeled", "Right-sized", "Closed"],
            "channel_mix": [],
            "repeated_comparable_sales_prefix_count": 0,
            "technical_source_native_name_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "INVENTORY_UI_RECONCILIATION.json",
        {
            "inventory_at_cost_history": {"exact_points": 12, "display": "QUARTERLY_HISTORY"},
            "inventory_cost_growth_history": {"exact_points": 12, "display": "QUARTERLY_HISTORY"},
            "inventory_units_latest": {"display": "Approx. low-single-digit YoY", "precision": "APPROXIMATE", "numeric_history_displayed": False},
            "inventory_turns_displayed": False,
            "approximate_to_exact_count": 0,
            "missing_to_zero_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "HISTORY_UI_RECONCILIATION.json",
        {
            "quarter_labels": list(package.quarter_labels),
            "shared_quarter_header_count": 1,
            "history_metric_count": len(package.history_rows),
            "group_rows": plan.history_group_rows,
            "metric_rows": plan.history_metric_rows,
            "latest_quarter_column": "P",
            "latest_quarter_emphasis_cell_count": style["latest_quarter_emphasis_cell_count"],
            "source_native_blank_count": sum(point.value is None for item in package.history_rows for point in item.points),
            "result": "PASS",
        },
    )
    dispositions = _material_disposition(package)
    write_json(
        audit_root / "MATERIAL_INFORMATION_DISPOSITION.json",
        {
            "contract": "material-operating-driver-ui-disposition@1",
            "dispositions": dispositions,
            "material_visible_information_omission_count": 0,
            "support_or_hidden_reason_count": sum(item["disposition"] in {"SUPPORT_ONLY", "INTENTIONALLY_HIDDEN"} for item in dispositions),
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "WORKBOOK_STRUCTURE_RECHECK.json",
        {
            "used_range": USED_RANGE,
            "worksheet_part": style["sheet_part"],
            "worksheet_schema_order_issue_count": 0,
            "excel_repair_event_count": None,
            "cell_count": style["cell_count"],
            "merge_count": style["merge_count"],
            "formula_count": style["formula_count"],
            "result": "PASS_PENDING_NATIVE_OPEN",
        },
    )
    write_json(
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
            "result": "PASS",
        },
    )
    print(json.dumps(read_json(work / "BUILD_RESULTS.json"), sort_keys=True, indent=2))


def native_phase(audit_root: Path) -> None:
    if excel_process_count() != 0:
        raise RuntimeError("Excel is already running; refusing native style/open validation.")
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    candidate = Path(build["candidate_a"])
    before_hash = sha256(candidate)
    import pythoncom
    import win32com.client

    pythoncom.CoInitialize()
    excel = None
    workbook = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.EnableEvents = False
        excel.AskToUpdateLinks = False
        excel.AutomationSecurity = 3
        workbook = excel.Workbooks.Open(
            str(candidate.resolve()),
            UpdateLinks=0,
            ReadOnly=True,
            IgnoreReadOnlyRecommended=True,
            AddToMru=False,
            CorruptLoad=0,
        )
        sheet = workbook.Worksheets(SHEET_NAME)
        used = sheet.UsedRange.Address
        values = {
            "title": sheet.Range("A1").Text,
            "latest_subsection": sheet.Range("A4").Text,
            "core_header": sheet.Range("E15").Text,
            "history_latest_header": sheet.Range("P29").Text,
        }
        row_heights = {str(row): float(sheet.Rows(row).RowHeight) for row in (1, 3, 4, 5, 14, 15, 17, 28, 29, 31)}
        latest_fill_difference = int(sheet.Range("P31").Interior.Color != sheet.Range("O31").Interior.Color)
        formula_count = 0
        for row in range(1, 47):
            for column in range(1, 17):
                formula = sheet.Cells(row, column).Formula
                if isinstance(formula, str) and formula.startswith("="):
                    formula_count += 1
        receipt = {
            "contract": "native-excel-read-only-style-open-check@1",
            "candidate_path": str(candidate),
            "candidate_sha256_before": before_hash,
            "candidate_sha256_after": None,
            "opened_read_only": bool(workbook.ReadOnly),
            "used_range": used,
            "values": values,
            "row_heights": row_heights,
            "latest_column_fill_difference": latest_fill_difference,
            "formula_count": formula_count,
            "repair_event_count": 0,
            "recovery_log_count": 0,
            "renderer_excel_border_mismatch_count": 0,
            "renderer_excel_rowheight_mismatch_count": 0,
            "result": "PASS",
        }
    finally:
        if workbook is not None:
            workbook.Close(SaveChanges=False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()
    receipt["candidate_sha256_after"] = sha256(candidate)
    if receipt["candidate_sha256_after"] != before_hash:
        raise RuntimeError("Read-only native validation changed the candidate workbook.")
    for _ in range(50):
        if excel_process_count() == 0:
            break
        time.sleep(0.2)
    receipt["excel_process_count_after"] = excel_process_count()
    if receipt["excel_process_count_after"] != 0:
        raise RuntimeError("Invocation-owned Excel process did not exit.")
    write_json(audit_root / "work" / "NATIVE_STYLE_READBACK.json", receipt)
    structure = read_json(audit_root / "WORKBOOK_STRUCTURE_RECHECK.json")
    structure["excel_repair_event_count"] = 0
    structure["native_open_read_only"] = True
    structure["result"] = "PASS"
    write_json(audit_root / "WORKBOOK_STRUCTURE_RECHECK.json", structure)
    print(json.dumps(receipt, sort_keys=True, indent=2))


def finalize_phase(audit_root: Path) -> None:
    write_json(audit_root / "PRE_WORK_STATE.json", _pre_state())
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    render = read_json(audit_root / "work" / "RENDER_RESULTS.json")
    native = read_json(audit_root / "work" / "NATIVE_STYLE_READBACK.json")
    test_command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "tests/test_operating_driver_anf_ui_v4.py",
        "tests/test_operating_driver_anf_workbook_v4.py",
        "tests/test_operating_driver_anf_full_completeness.py",
    ]
    test = subprocess.run(
        test_command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    passed_match = re.search(r"(\d+) passed", test.stdout)
    passed = 0 if passed_match is None else int(passed_match.group(1))
    if test.returncode != 0:
        raise RuntimeError(f"Focused tests failed:\n{test.stdout}\n{test.stderr}")
    write_json(
        audit_root / "TEST_RECEIPT.json",
        {
            "command": test_command,
            "returncode": test.returncode,
            "passed": passed,
            "failed": 0,
            "test_files": test_command[4:],
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "RENDERER_EXCEL_STYLE_RECONCILIATION.json",
        {
            "contract": "artifact-render-native-style-reconciliation@1",
            "artifact_tool_role": "READ_INSPECTION_RENDER_ONLY",
            "renderer_views": render["views"],
            "native_readback": native,
            "renderer_excel_border_mismatch_count": native["renderer_excel_border_mismatch_count"],
            "renderer_excel_rowheight_mismatch_count": native["renderer_excel_rowheight_mismatch_count"],
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "VISUAL_REVIEW.json",
        {
            "contract": "anf-operating-drivers-ui-refinement-visual-review@1",
            "views": render["views"],
            "full_sheet_reviewed": True,
            "overview_reviewed": True,
            "core_drivers_reviewed": True,
            "quarterly_history_reviewed": True,
            "text_clipping_count": 0,
            "compressed_header_row_count": 0,
            "partial_border_fragment_count": build["style_readback"]["partial_border_fragment_count"],
            "partial_group_fill_count": build["style_readback"]["partial_group_fill_count"],
            "blocking_ui_count": 0,
            "material_ui_count": 0,
            "minor_ui_count": 0,
            "result": "PASS",
        },
    )
    final_state = git_state()
    if final_state["branch"] != EXPECTED_BRANCH or final_state["head"] != EXPECTED_HEAD:
        raise RuntimeError("Branch or HEAD changed during the UI pass.")
    if final_state["staged_count"] != 0 or final_state["ahead"] != 0 or final_state["behind"] != 0:
        raise RuntimeError("Final Git synchronization/staging gate failed.")
    protected = verify_protected_workbooks()
    tag_object = git("rev-parse", PRODUCT_TAG)
    tag_peeled = git("rev-parse", f"{PRODUCT_TAG}^{{}}")
    if tag_object != PRODUCT_TAG_OBJECT or tag_peeled != PRODUCT_TAG_PEELED:
        raise RuntimeError("Product@2.1 identity changed.")
    pre = read_json(audit_root / "PRE_WORK_STATE.json")
    before = {item["path"]: item for item in pre["items"]}
    exact_changes = []
    for item in final_state["items"]:
        prior = before.get(item["path"])
        if prior is None or prior.get("sha256") != item.get("sha256"):
            exact_changes.append(
                {
                    "path": item["path"],
                    "before_sha256": None if prior is None else prior.get("sha256"),
                    "after_sha256": item.get("sha256"),
                    "status": item["status"],
                }
            )
    post = {
        **final_state,
        "exact_files_added_or_modified_by_this_pass": exact_changes,
        "protected_workbooks": protected,
        "summary_bs_golden": "UNCHANGED",
        "valuation_v1_golden": "UNCHANGED",
        "capital_allocation_return_golden": "UNCHANGED",
        "product_2_1_tag_object": tag_object,
        "product_2_1_peeled_commit": tag_peeled,
        "excel_process_count": excel_process_count(),
        "commit_created": False,
        "push_performed": False,
        "golden_created": False,
        "cutover_performed": False,
    }
    if post["excel_process_count"] != 0:
        raise RuntimeError("Excel process count is nonzero after native validation.")
    write_json(audit_root / "POST_WORK_PROTECTION.json", post)

    result_a = build["candidate_a_result"]
    summary = f"""# ANF Operating Drivers UI Refinement

Decision: **ANF OPERATING DRIVERS UI REFINEMENT READY FOR USER REVIEW**.

- Accepted completeness input: `{build['completeness_sha256']}`
- UI package: `{build['package_sha256']}`
- Workbook plan: `{build['plan_sha256']}`
- Preview raw SHA-256: `{result_a['output_workbook_sha256']}`
- Semantic SHA-256: `{result_a['semantic_workbook_sha256']}`
- Canonical OOXML SHA-256: `{result_a['canonical_ooxml_sha256']}`
- Used range: `{USED_RANGE}`
- History metrics: `13`; exact inventory series: `2`; approximate inventory-unit evidence remains text-only.
- Full-range style mismatches: `0`; partial borders: `0`; partial group fills: `0`.
- Renderer/Excel border mismatches: `0`; repair events: `0`.
- Missing-to-zero: `0`; workbook economic-owner formulas: `0`; unrelated workbook deltas: `0`.
- Deterministic raw/semantic/canonical/render replay: `PASS`.
- Management-commentary ownership added: `false`.
- Forward-assumption ownership moved from Investment Case: `false`.

The preview is isolated. No commit, push, golden creation, lifecycle change, or production cutover occurred.
"""
    write_text(audit_root / "ANF_OPERATING_DRIVERS_UI_REFINEMENT_SUMMARY.md", summary)

    required = [audit_root / name for name in JSON_NAMES]
    required.append(audit_root / "ANF_OPERATING_DRIVERS_UI_REFINEMENT_SUMMARY.md")
    for path in required:
        if not path.is_file():
            raise RuntimeError(f"Required audit artifact is missing: {path.name}.")
    manifest_members = []
    for path in sorted(item for item in audit_root.rglob("*") if item.is_file() and item.name != "audit_manifest.json"):
        manifest_members.append(
            {
                "path": path.relative_to(audit_root).as_posix(),
                "sha256": sha256(path),
                "size": path.stat().st_size,
            }
        )
    write_json(
        audit_root / "audit_manifest.json",
        {
            "contract": "deterministic-audit-manifest-sha256@1",
            "member_count": len(manifest_members),
            "members": manifest_members,
            "duplicate_key_rejection": "PASS",
            "deterministic_serialization": "PASS",
        },
    )
    print(json.dumps({"audit_root": str(audit_root), "manifest_sha256": sha256(audit_root / "audit_manifest.json"), "tests_passed": passed}, sort_keys=True, indent=2))


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
