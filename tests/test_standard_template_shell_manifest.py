from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

VISIBLE_SHEET_ORDER = [
    "SUMMARY",
    "Valuation",
    "BS_Segments",
    "Operating_Drivers",
    "{ticker}_Investment_Case",
    "Quarter_Notes_UI",
    "Promise_Progress_UI",
    "QA_Log",
    "Needs_Review",
    "QA_Checks",
]


def _manifest() -> dict:
    return json.loads((ROOT / "docs" / "standard_template_shell_manifest.json").read_text(encoding="utf-8"))


_A1_RE = re.compile(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$")


def _col_to_int(col: str) -> int:
    value = 0
    for char in col:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value


def _parse_range(target: str) -> tuple[int, int, int, int]:
    match = _A1_RE.match(target)
    assert match, f"Invalid A1 range: {target!r}"
    c1, r1, c2, r2 = match.groups()
    left = _col_to_int(c1)
    top = int(r1)
    right = _col_to_int(c2)
    bottom = int(r2)
    assert left <= right and top <= bottom, f"Invalid reversed A1 range: {target!r}"
    return left, top, right, bottom


def _overlaps(first: tuple[int, int, int, int], second: tuple[int, int, int, int]) -> bool:
    f_left, f_top, f_right, f_bottom = first
    s_left, s_top, s_right, s_bottom = second
    return not (f_right < s_left or s_right < f_left or f_bottom < s_top or s_bottom < f_top)


def test_standard_template_shell_manifest_defines_visible_shell_contract() -> None:
    manifest = _manifest()

    assert manifest["file_type"] == ".xlsx"
    assert manifest["visible_sheet_order"] == VISIBLE_SHEET_ORDER
    assert manifest["ticker_sheet_token_rule"]["template"] == "{ticker}_Investment_Case"
    assert manifest["ticker_sheet_token_rule"]["example"] == "PBI_Investment_Case"
    assert "macros" not in json.dumps(manifest).lower()

    sheets_by_name = {sheet["sheet"]: sheet for sheet in manifest["sheets"]}
    assert set(sheets_by_name) >= set(VISIBLE_SHEET_ORDER)
    for sheet_name in VISIBLE_SHEET_ORDER:
        sheet = sheets_by_name[sheet_name]
        assert sheet["static_layout_owner"] == "frozen_template_shell"
        assert sheet["writable_zones"]
        assert sheet["non_writable_zones"]


def test_standard_template_shell_manifest_required_anchors_are_bindable() -> None:
    manifest = _manifest()
    writable_zone_ids = {
        zone["zone_id"]
        for sheet in manifest["sheets"]
        for zone in sheet["writable_zones"]
    }

    required_anchors = manifest["required_anchors"]
    assert required_anchors
    for anchor in required_anchors:
        assert anchor["anchor_id"]
        assert anchor["sheet"]
        assert anchor["zone_id"] in writable_zone_ids
        assert anchor.get("anchor_label") or anchor.get("named_range")
        assert anchor["binding_required"] is True


def test_standard_template_shell_manifest_ranges_are_valid_a1_ranges() -> None:
    for sheet in _manifest()["sheets"]:
        for zone_type in ("writable_zones", "non_writable_zones"):
            for zone in sheet[zone_type]:
                _parse_range(zone["target"])


def test_standard_template_shell_manifest_writable_and_non_writable_zones_do_not_overlap() -> None:
    failures: list[str] = []
    for sheet in _manifest()["sheets"]:
        writable = [(zone["zone_id"], _parse_range(zone["target"])) for zone in sheet["writable_zones"]]
        non_writable = [(zone["zone_id"], _parse_range(zone["target"])) for zone in sheet["non_writable_zones"]]
        for writable_id, writable_range in writable:
            for non_writable_id, non_writable_range in non_writable:
                if _overlaps(writable_range, non_writable_range):
                    failures.append(f"{sheet['sheet']}: {writable_id} overlaps {non_writable_id}")

    assert failures == []


def test_standard_template_shell_manifest_docs_state_materialized_shell_artifact() -> None:
    doc = (ROOT / "docs" / "standard_template_shell_manifest.md").read_text(encoding="utf-8").lower()

    assert "templates/standard_stock_model_template.xlsx" in doc
    assert ".xlsx" in doc
    assert "future filler writes values only" in doc
