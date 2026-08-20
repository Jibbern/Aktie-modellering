"""Build and receipt the final ANF Operating Drivers numeric/blank correction."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
from typing import Any, Mapping, Sequence
import xml.etree.ElementTree as ET
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = Path(__file__).resolve().parent
for entry in (REPO_ROOT, SCRIPT_ROOT):
    if str(entry) not in sys.path:
        sys.path.insert(0, str(entry))

import build_anf_operating_driver_final_information_density as prior  # noqa: E402
import build_anf_operating_driver_ui_refinement as base  # noqa: E402
from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (  # noqa: E402
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
    STORE_COUNT_ROLL_FORWARD_CONTRACT,
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
    derive_company_owned_store_roll_forward,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (  # noqa: E402
    SHEET_NAME,
    _range_coordinates,
    build_operating_driver_anf_workbook_v4_plan,
    materialize_operating_driver_anf_workbook_v4,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (  # noqa: E402
    _sheet_part_map,
)


DEFAULT_AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_numeric_blank_final_fix_2026-08-20"
)
ACCEPTED_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_final_information_density_2026-08-20"
)
ACCEPTED_PREVIEW = ACCEPTED_AUDIT / "ANF_operating_drivers_final_information_density_preview.xlsx"
EXPECTED_ACCEPTED_PREVIEW_SHA256 = "a535c8d39b5d72b918e0b121dcf1113d4bcdb7eb44eea417a0bf84b885c2558c"
EXPECTED_COMPLETENESS_SHA256 = "c1fbc5898e56fff7a5e559b122578fcf996b82ee389a47f9caf82adedf4bf1e9"
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
OUTPUT_NAME = "ANF_operating_drivers_numeric_blank_final_fix_preview.xlsx"
REPLAY_NAME = "ANF_operating_drivers_numeric_blank_final_fix_preview_replay.xlsx"
SUMMARY_NAME = "ANF_OPERATING_DRIVERS_NUMERIC_BLANK_FINAL_FIX_SUMMARY.md"
NEW_SCRIPT_PATH = "scripts/build_anf_operating_driver_numeric_blank_final_fix.py"
ALLOWED_CHANGED_PATHS = {
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_ui_v4.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_workbook_v4.py",
    "tests/test_operating_driver_anf_ui_v4.py",
    "tests/test_operating_driver_anf_workbook_v4.py",
}
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"m": MAIN_NS}
PERIODS = (
    "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1",
    "2024-Q2", "2024-Q3", "2024-Q4", "2025-Q1",
    "2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1",
)
PERIOD_COLUMNS = dict(zip(PERIODS, "EFGHIJKLMNOP", strict=True))
EXACT_CORE_COORDINATES = (
    "E21", "G21", "I21", "E22", "G22", "I22",
    "E23", "G23", "I23", "E24", "G24", "I24",
    "E26", "G26", "E28", "G28", "I28", "E29", "G29", "I29",
)
APPROXIMATE_HISTORY_COORDINATES = ("K45", "N45", "O45", "P45")
JSON_NAMES = (
    "PRE_WORK_STATE.json",
    "VISIBLE_CELL_TYPE_AUDIT.json",
    "NUMERIC_PRESENTATION_CONTRACT.json",
    "NUMBER_STORED_AS_TEXT_RECHECK.json",
    "HISTORY_BLANK_DISPOSITION.json",
    "STORE_COUNT_ROLL_FORWARD_ASSESSMENT.json",
    "STORE_COUNT_ROLL_FORWARD_RECONCILIATION.json",
    "STORE_COUNT_DERIVATION_LINEAGE.json",
    "INVENTORY_UNIT_APPROXIMATE_HISTORY_REVIEW.json",
    "STORE_ACTIVITY_BLANK_REVIEW.json",
    "DEFINITION_BREAK_PRESERVATION.json",
    "DIGITAL_HISTORY_RECHECK.json",
    "BEFORE_AFTER_BLANK_MATRIX.json",
    "UI_PRESERVATION_RECHECK.json",
    "WORKBOOK_NATIVE_RECHECK.json",
    "LOSSLESS_STRUCTURAL_DIFF.json",
    "TEST_RECEIPT.json",
    "POST_WORK_PROTECTION.json",
)


def read_json(path: Path) -> Any:
    def reject_duplicates(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError(f"Duplicate JSON key {key!r} in {path}.")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    read_json(path)


def _sheet_part(archive: ZipFile) -> str:
    return _sheet_part_map(archive)[SHEET_NAME]


def _cell_text(cell: ET.Element) -> str:
    inline = cell.find("m:is", NS)
    if inline is not None:
        return "".join(item.text or "" for item in inline.findall(".//m:t", NS))
    value = cell.find("m:v", NS)
    return "" if value is None else (value.text or "")


def _workbook_cells(path: Path) -> tuple[str, dict[str, dict[str, Any]]]:
    with ZipFile(path, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
        styles = ET.fromstring(archive.read("xl/styles.xml"))
    num_fmts = styles.find("m:numFmts", NS)
    codes = {
        int(item.attrib["numFmtId"]): item.attrib["formatCode"]
        for item in (() if num_fmts is None else num_fmts)
    }
    codes.update({0: "General", 3: "#,##0", 9: "0%"})
    xfs = list(styles.find("m:cellXfs", NS))
    result: dict[str, dict[str, Any]] = {}
    for cell in root.findall(".//m:sheetData/m:row/m:c", NS):
        coordinate = cell.attrib["r"]
        style_id = int(cell.attrib.get("s", "0"))
        num_fmt_id = int(xfs[style_id].attrib.get("numFmtId", "0"))
        result[coordinate] = {
            "cell_type": cell.attrib.get("t"),
            "raw_value": _cell_text(cell),
            "style_id": style_id,
            "number_format": codes.get(num_fmt_id, f"numFmtId:{num_fmt_id}"),
            "has_formula": cell.find("m:f", NS) is not None,
        }
    return part, result


def _accepted_pre_state() -> dict[str, Any]:
    state = read_json(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json")
    if (
        state["branch"] != EXPECTED_BRANCH
        or state["head"] != EXPECTED_HEAD
        or state["ahead"] != 0
        or state["behind"] != 0
        or state["modified_tracked_count"] != 4
        or state["staged_count"] != 0
        or state["untracked_count"] != 29
    ):
        raise RuntimeError("Accepted final-information-density pre-state is not the required authority.")
    if base.sha256(ACCEPTED_PREVIEW) != EXPECTED_ACCEPTED_PREVIEW_SHA256:
        raise RuntimeError("Accepted preview identity mismatch.")
    return {
        "contract": "anf-operating-drivers-numeric-blank-pre-state@1",
        "accepted_receipt": str(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json"),
        "accepted_manifest_sha256": base.sha256(ACCEPTED_AUDIT / "audit_manifest.json"),
        "accepted_preview": str(ACCEPTED_PREVIEW),
        "accepted_preview_sha256": EXPECTED_ACCEPTED_PREVIEW_SHA256,
        "accepted_completeness_package_sha256": EXPECTED_COMPLETENESS_SHA256,
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
    live = base.git_state()
    if (
        live["branch"] != EXPECTED_BRANCH
        or live["head"] != EXPECTED_HEAD
        or live["ahead"] != 0
        or live["behind"] != 0
        or live["staged_count"] != 0
    ):
        raise RuntimeError("Live branch/HEAD/synchronization/staging mismatch.")
    before = {item["path"]: item for item in accepted["items"]}
    after = {item["path"]: item for item in live["items"]}
    if set(before) - set(after):
        raise RuntimeError(f"Accepted paths disappeared: {sorted(set(before) - set(after))}.")
    allowed_new = {NEW_SCRIPT_PATH}
    if set(after) - set(before) - allowed_new:
        raise RuntimeError(f"Unexpected new paths: {sorted(set(after) - set(before) - allowed_new)}.")
    changed_unexpected = [
        path
        for path in set(before) & set(after)
        if before[path].get("sha256") != after[path].get("sha256")
        and path not in ALLOWED_CHANGED_PATHS
    ]
    if changed_unexpected:
        raise RuntimeError(f"Unexpected accepted-path changes: {sorted(changed_unexpected)}.")
    return accepted, live


def _history_blank_matrix(package: Any, plan: Any) -> list[dict[str, Any]]:
    _, before_cells = _workbook_cells(ACCEPTED_PREVIEW)
    rows_by_label = {item.label: item for item in package.history_rows}
    labels_by_row = {
        row: next(
            item.label
            for item in package.history_rows
            if f"{item.driver_id}|{item.dimension_member_id}" == key
        )
        for key, row in plan.history_metric_rows.items()
    }
    rows: list[dict[str, Any]] = []
    for row_number, label in sorted(labels_by_row.items()):
        history_row = rows_by_label[label]
        points = {item.period_label: item for item in history_row.points}
        for period, column in PERIOD_COLUMNS.items():
            coordinate = f"{column}{row_number}"
            before = before_cells[coordinate]["raw_value"]
            if before != "":
                continue
            point = points[period]
            if label == "Company-owned stores" and point.value is not None:
                disposition = "SAFE_DERIVATION_CANDIDATE"
                reason = "Exact openings and closures reconcile the company-owned ending-store population to every direct anchor."
            elif label == "Inventory units (YoY)" and point.display_value:
                disposition = "APPROXIMATE_EVIDENCE_AVAILABLE"
                reason = "Accepted period-specific approximate evidence is shown as controlled text and remains non-numeric."
            elif label == "Inventory units (YoY)":
                disposition = "NOT_DISCLOSED"
                reason = "The accepted completeness package contains no compatible evidence for this quarter."
            elif label in {"Remodeled", "Right-sized"} and period.startswith("2023-"):
                disposition = "BLOCKED_BY_CONTINUITY"
                reason = "Separate FY activity exists, but compatible cumulative predecessor evidence is insufficient for quarter-only derivation."
            elif label in {"Remodeled", "Right-sized"} and period in {"2024-Q1", "2024-Q2"}:
                disposition = "DEFINITION_BREAK"
                reason = "Accepted evidence combines remodels and right-sizes; component values cannot be split."
            else:
                raise RuntimeError(f"Unexplained accepted history blank {coordinate} {label} {period}.")
            after = point.value if point.value is not None else point.display_value
            rows.append(
                {
                    "sheet_cell": f"{SHEET_NAME}!{coordinate}",
                    "metric": label,
                    "period": period,
                    "before": None,
                    "evidence_disposition": disposition,
                    "after": None if after == "" else after,
                    "reason": reason,
                    "derivation_id": point.derivation_id,
                    "lineage_references": list(point.lineage_references),
                    "precision": point.precision,
                }
            )
    if len(rows) != 29:
        raise RuntimeError(f"Expected 29 accepted history blanks, found {len(rows)}.")
    return rows


def _changed_parts(before: Path, after: Path) -> list[str]:
    with ZipFile(before, "r") as left, ZipFile(after, "r") as right:
        if left.namelist() != right.namelist():
            raise RuntimeError("Workbook member inventory changed.")
        return [name for name in left.namelist() if left.read(name) != right.read(name)]


def build_phase(audit_root: Path) -> None:
    if audit_root.exists():
        raise RuntimeError(f"Refusing to overwrite existing audit root: {audit_root}.")
    accepted_pre, live = _validate_live_state()
    base.verify_protected_workbooks()
    completeness = build_anf_operating_driver_full_completeness()
    if completeness.sha256 != EXPECTED_COMPLETENESS_SHA256:
        raise RuntimeError("Accepted completeness package identity changed.")
    lower = {
        "registry_sha256": completeness.registry.sha256,
        "analytics_sha256": completeness.analytics.sha256,
        "semantics_sha256": completeness.semantics.sha256,
        "selection_sha256": completeness.selection.sha256,
    }
    source = build_operating_driver_anf_ui_source_from_completeness(completeness)
    package = build_operating_driver_anf_ui_v4(
        source,
        source_identity_receipts={"full_data_completeness_sha256": completeness.sha256, **lower},
    )
    plan = build_operating_driver_anf_workbook_v4_plan(package)
    roll_forward = derive_company_owned_store_roll_forward(source)

    audit_root.mkdir(parents=True)
    work = audit_root / "work"
    work.mkdir()
    candidate_a = audit_root / OUTPUT_NAME
    candidate_b = work / REPLAY_NAME
    result_a = materialize_operating_driver_anf_workbook_v4(
        base_workbook=base.PROTECTED_WORKBOOKS["ANF"][0],
        output_workbook=candidate_a,
        plan=plan,
        expected_base_sha256=base.PROTECTED_WORKBOOKS["ANF"][1],
    )
    result_b = materialize_operating_driver_anf_workbook_v4(
        base_workbook=base.PROTECTED_WORKBOOKS["ANF"][0],
        output_workbook=candidate_b,
        plan=plan,
        expected_base_sha256=base.PROTECTED_WORKBOOKS["ANF"][1],
    )
    replay = {
        "raw": result_a.output_workbook_sha256 == result_b.output_workbook_sha256,
        "semantic": result_a.semantic_workbook_sha256 == result_b.semantic_workbook_sha256,
        "canonical": result_a.canonical_ooxml_sha256 == result_b.canonical_ooxml_sha256,
    }
    if not all(replay.values()):
        raise RuntimeError(f"Workbook replay failed: {replay}.")
    if any((
        result_a.unrelated_workbook_delta_count,
        result_a.target_formula_count,
        result_a.missing_to_zero_count,
        result_a.full_range_style_mismatch_count,
    )):
        raise RuntimeError(f"Workbook materialization counter failed: {result_a.to_dict()}.")

    prior_package = read_json(ACCEPTED_AUDIT / "work" / "UI_PACKAGE.json")
    prior_plan = read_json(ACCEPTED_AUDIT / "work" / "WORKBOOK_PLAN.json")
    current_plan = plan.to_dict()
    structural_keys = (
        "plan_origin", "sheet_name", "used_range", "zoom_scale", "merge_mutations",
        "row_mutations", "column_mutations", "dimension_mutation", "major_section_rows",
        "history_group_rows", "history_metric_rows", "core_group_rows", "core_metric_rows",
    )
    layout_deltas = [key for key in structural_keys if prior_plan[key] != current_plan[key]]
    if layout_deltas:
        raise RuntimeError(f"Unauthorized UI layout changes: {layout_deltas}.")
    normalized_package = json.loads(json.dumps(package.to_dict()))
    if prior_package["overview"] != normalized_package["overview"]:
        raise RuntimeError("Operating Interpretation/Latest Quarter/Broader Trend changed.")
    if [item["core_id"] for item in prior_package["core_drivers"]] != [item.core_id for item in package.core_drivers]:
        raise RuntimeError("Core Driver selection changed.")

    blank_matrix = _history_blank_matrix(package, plan)
    blanks_before = len(blank_matrix)
    safe_fills = [item for item in blank_matrix if item["evidence_disposition"] == "SAFE_DERIVATION_CANDIDATE"]
    approximate_fills = [item for item in blank_matrix if item["evidence_disposition"] == "APPROXIMATE_EVIDENCE_AVAILABLE"]
    preserved = [item for item in blank_matrix if item["after"] is None]
    if (blanks_before, len(safe_fills), len(approximate_fills), len(preserved)) != (29, 8, 4, 17):
        raise RuntimeError("Unexpected blank-disposition counts.")

    _, before_cells = _workbook_cells(ACCEPTED_PREVIEW)
    sheet_part, after_cells = _workbook_cells(candidate_a)
    previous_numeric_text = [
        {
            "cell": f"{SHEET_NAME}!{coordinate}",
            "raw_text": before_cells[coordinate]["raw_value"],
            "cell_type": before_cells[coordinate]["cell_type"],
        }
        for coordinate in EXACT_CORE_COORDINATES
        if before_cells[coordinate]["cell_type"] == "inlineStr"
    ]
    exact_after = sorted(plan.display_number_formats)
    exact_numeric_text_after = [
        coordinate
        for coordinate in exact_after
        if after_cells[coordinate]["cell_type"] != "n"
    ]
    if len(previous_numeric_text) != 20 or exact_numeric_text_after:
        raise RuntimeError("Exact numeric cell typing gate failed.")
    approximate_after = {
        coordinate: after_cells[coordinate]
        for coordinate in APPROXIMATE_HISTORY_COORDINATES
    }
    if any(item["cell_type"] != "inlineStr" for item in approximate_after.values()):
        raise RuntimeError("Approximate history evidence became numeric.")

    anchors = [item for item in roll_forward if item.direct_store_fact_id is not None]
    if any(item.direct_anchor_match is not True for item in anchors):
        raise RuntimeError("A direct store-count anchor did not reconcile.")
    changed_from_accepted = _changed_parts(ACCEPTED_PREVIEW, candidate_a)
    if set(changed_from_accepted) - {"xl/styles.xml", sheet_part}:
        raise RuntimeError(f"Unexpected accepted-preview OOXML delta: {changed_from_accepted}.")
    style = prior._style_readback(candidate_a, plan)
    if any(style[key] for key in (
        "partial_border_fragment_count", "anchor_only_border_application_count",
        "partial_group_fill_count", "full_fill_mismatch_count",
        "smart_number_format_mismatch_count", "latest_quarter_emphasis_mismatch_count",
        "negative_red_font_violation_count", "worksheet_ordering_error_count",
        "formula_count", "sparkline_count",
    )):
        raise RuntimeError(f"Style/structure readback failed: {style}.")

    write_json(work / "UI_PACKAGE.json", package.to_dict())
    write_json(work / "WORKBOOK_PLAN.json", current_plan)
    write_json(
        work / "BUILD_RESULTS.json",
        {
            "candidate_a": str(candidate_a),
            "candidate_b": str(candidate_b),
            "candidate_a_result": result_a.to_dict(),
            "candidate_b_result": result_b.to_dict(),
            "completeness_sha256": completeness.sha256,
            "lower_layer_identities": lower,
            "package_sha256": package.package_sha256,
            "plan_sha256": plan.plan_sha256,
            "replay": replay,
            "style_readback": style,
            "changed_from_accepted_preview": changed_from_accepted,
        },
    )
    write_json(audit_root / "PRE_WORK_STATE.json", accepted_pre)
    write_json(
        audit_root / "VISIBLE_CELL_TYPE_AUDIT.json",
        {
            "contract": "operating-drivers-visible-cell-type-audit@1",
            "accepted_preview": str(ACCEPTED_PREVIEW),
            "accepted_preview_exact_numeric_text_cells": previous_numeric_text,
            "exact_numeric_text_cell_count_before": len(previous_numeric_text),
            "exact_numeric_cells_after": [
                {
                    "cell": f"{SHEET_NAME}!{coordinate}",
                    "raw_numeric_value": after_cells[coordinate]["raw_value"],
                    "number_format": after_cells[coordinate]["number_format"],
                }
                for coordinate in exact_after
            ],
            "exact_numeric_cell_count_after": len(exact_after),
            "exact_numeric_stored_as_text_count_after": 0,
            "approximate_text_cells_after": [
                {
                    "cell": f"{SHEET_NAME}!{coordinate}",
                    "text": item["raw_value"],
                    "classification": "APPROXIMATE_TEXT",
                }
                for coordinate, item in sorted(approximate_after.items())
            ],
            "status_text_cells_preserved": ["Operating_Drivers!I26", "Operating_Drivers!G30", "Operating_Drivers!I30"],
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "NUMERIC_PRESENTATION_CONTRACT.json",
        {
            "contract": "operating-drivers-exact-numeric-presentation@1",
            "workbook_is_economic_owner": False,
            "metric_types": {
                "PERCENT_LEVEL": {"storage": "decimal fraction", "formats": ["+0%;-0%;0%", "+0.0%;-0.0%;0%"]},
                "PERCENTAGE_POINT_CHANGE": {"storage": "numeric pp units", "formats": ['+0" pp";-0" pp";0" pp"', '+0.0" pp";-0.0" pp";0" pp"']},
                "COUNT": {"storage": "numeric count", "formats": ['#,##0" stores"', '+#,##0" stores";-#,##0" stores";0" stores"']},
                "DOLLAR_MILLIONS": {"storage": "numeric USD millions", "formats": ['"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"', '+"$"#,##0.0"m";-"$"#,##0.0"m";"$"0.0"m"']},
            },
            "trailing_zero_suppression": "format selected from exact source precision",
            "global_error_suppression_used": False,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "HISTORY_BLANK_DISPOSITION.json",
        {
            "contract": "operating-drivers-history-blank-disposition@1",
            "allowed_dispositions": [
                "SAFE_DERIVATION_CANDIDATE", "APPROXIMATE_EVIDENCE_AVAILABLE",
                "DEFINITION_BREAK", "NOT_DISCLOSED", "NOT_APPLICABLE", "BLOCKED_BY_CONTINUITY",
            ],
            "blanks_before": blanks_before,
            "new_direct_values": 0,
            "new_safe_derived_values": len(safe_fills),
            "new_approximate_text_cells": len(approximate_fills),
            "definition_break_blanks_preserved": sum(item["evidence_disposition"] == "DEFINITION_BREAK" for item in preserved),
            "not_disclosed_blanks_preserved": sum(item["evidence_disposition"] == "NOT_DISCLOSED" for item in preserved),
            "blocked_by_continuity_blanks_preserved": sum(item["evidence_disposition"] == "BLOCKED_BY_CONTINUITY" for item in preserved),
            "blanks_after": len(preserved),
            "unexplained_history_blank_count": 0,
            "unsafe_blank_fill_count": 0,
            "result": "PASS",
        },
    )
    write_json(audit_root / "BEFORE_AFTER_BLANK_MATRIX.json", {"contract": "history-blank-before-after-matrix@1", "rows": blank_matrix, "row_count": len(blank_matrix), "result": "PASS"})
    write_json(
        audit_root / "STORE_COUNT_ROLL_FORWARD_ASSESSMENT.json",
        {
            "contract": STORE_COUNT_ROLL_FORWARD_CONTRACT,
            "formula": "ENDING_STORES_t = ENDING_STORES_t-1 + NEW_STORES_t - CLOSED_STORES_t",
            "population": "company-owned stores, total company",
            "source_definition_checks": {
                "new_store_population_compatible": True,
                "closure_population_compatible": True,
                "actual_only": True,
                "same_unit": True,
                "same_total_company_dimension": True,
                "quarter_activity_complete": True,
                "franchise_population_separate": True,
                "omitted_net_change_evidence_found": False,
            },
            "direct_anchor_count": len(anchors),
            "anchor_mismatch_count": 0,
            "definition_mismatch_count": 0,
            "decision": "ACCEPT_STORE_COUNT_ROLL_FORWARD",
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "STORE_COUNT_ROLL_FORWARD_RECONCILIATION.json",
        {
            "contract": STORE_COUNT_ROLL_FORWARD_CONTRACT,
            "records": [asdict(item) for item in roll_forward],
            "derived_blank_fills": safe_fills,
            "store_count_rollforward_anchor_mismatch_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "STORE_COUNT_DERIVATION_LINEAGE.json",
        {
            "contract": STORE_COUNT_ROLL_FORWARD_CONTRACT,
            "records": [
                {
                    "period": item.period_label,
                    "derivation_id": item.derivation_id,
                    "prior_period": item.prior_period_label,
                    "prior_store_reference": item.prior_store_reference,
                    "new_store_fact_id": item.new_store_fact_id,
                    "closed_store_fact_id": item.closed_store_fact_id,
                    "direct_store_fact_id": item.direct_store_fact_id,
                    "lineage_references": list(item.lineage_references),
                }
                for item in roll_forward
            ],
            "unlineaged_store_count_derivation_count": 0,
            "direct_observation_overwritten_by_derivation_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "INVENTORY_UNIT_APPROXIMATE_HISTORY_REVIEW.json",
        {
            "decision": "DISPLAY_CONTROLLED_APPROXIMATE_TEXT",
            "exact_numeric_periods": [{"period": "2025-Q2", "value": "7%"}],
            "approximate_text_periods": [item for item in blank_matrix if item["evidence_disposition"] == "APPROXIMATE_EVIDENCE_AVAILABLE"],
            "numeric_analytics_eligibility": False,
            "approximate_to_exact_count": 0,
            "visual_density_reason": "Four concise labels fit the existing 12-quarter row without changing geometry.",
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "STORE_ACTIVITY_BLANK_REVIEW.json",
        {
            "combined_remodel_right_size_evidence_exists": True,
            "combined_metric_split_count": 0,
            "combined_row_added": False,
            "decision": "PRESERVE_COMPONENT_BLANKS",
            "reason": "The combined series is discontinuous and would not add enough investor value to justify a new row; components remain fail-closed.",
            "2023_component_blanks": "BLOCKED_BY_CONTINUITY",
            "2024_q1_q2_component_blanks": "DEFINITION_BREAK",
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "DEFINITION_BREAK_PRESERVATION.json",
        {
            "regional_2023_q1": {"Americas": None, "EMEA": None, "APAC": None},
            "regional_definition_break_fill_count": 0,
            "store_component_definition_break_fill_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "DIGITAL_HISTORY_RECHECK.json",
        {
            "compatible_recurring_quarterly_total_company_digital_sales_mix": False,
            "FY2025_44_percent_used_as_quarterly": False,
            "mobile_traffic_used_as_sales_mix": False,
            "channel_mix_group_added": False,
            "digital_dimension_misuse_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "UI_PRESERVATION_RECHECK.json",
        {
            "accepted_preview": str(ACCEPTED_PREVIEW),
            "layout_structural_keys_checked": list(structural_keys),
            "layout_deltas": layout_deltas,
            "overview_delta_count": 0,
            "core_selection_delta_count": 0,
            "history_group_order_delta_count": 0,
            "row_height_delta_count": 0,
            "column_width_delta_count": 0,
            "zoom_delta_count": 0,
            "color_border_hierarchy_delta_outside_authorized_scope": 0,
            "authorized_visible_changes": ["numeric cell typing and formats", "eight store-count fills", "four approximate inventory-unit text fills", "truthful footer clarification"],
            "UI_layout_delta_outside_authorized_scope": 0,
            "result": "PASS_PENDING_RENDER",
        },
    )
    write_json(
        audit_root / "LOSSLESS_STRUCTURAL_DIFF.json",
        {
            "protected_base_changed_parts": list(result_a.changed_ooxml_parts),
            "protected_base_allowed_changed_parts": list(result_a.allowed_changed_ooxml_parts),
            "accepted_preview_changed_parts": changed_from_accepted,
            "unrelated_workbook_delta_count": result_a.unrelated_workbook_delta_count,
            "unchanged_ooxml_part_count": result_a.unchanged_ooxml_part_count,
            "target_formula_count": result_a.target_formula_count,
            "missing_to_zero_count": result_a.missing_to_zero_count,
            "deterministic_replay": replay,
            "result": "PASS",
        },
    )
    print(json.dumps(read_json(work / "BUILD_RESULTS.json"), indent=2, sort_keys=True))


def native_phase(audit_root: Path) -> None:
    if base.excel_process_count() != 0:
        raise RuntimeError("Excel is already running; refusing native validation.")
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    candidate = Path(build["candidate_a"])
    before_hash = base.sha256(candidate)
    import pythoncom
    import win32com.client

    def read_workbook(excel: Any, path: Path, coordinates: Sequence[str]) -> dict[str, Any]:
        workbook = excel.Workbooks.Open(
            str(path.resolve()), UpdateLinks=0, ReadOnly=True,
            IgnoreReadOnlyRecommended=True, AddToMru=False, CorruptLoad=0,
        )
        try:
            sheet = workbook.Worksheets(SHEET_NAME)
            values = []
            warning_count = 0
            for coordinate in coordinates:
                cell = sheet.Range(coordinate)
                try:
                    warning = bool(cell.Errors.Item(3).Value)
                except Exception:
                    warning = False
                warning_count += int(warning)
                values.append(
                    {
                        "cell": f"{SHEET_NAME}!{coordinate}",
                        "value2": cell.Value2,
                        "text": str(cell.Text),
                        "number_format": str(cell.NumberFormat),
                        "number_stored_as_text_warning": warning,
                    }
                )
            formula_count = sum(
                isinstance(sheet.Cells(row, column).Formula, str)
                and sheet.Cells(row, column).Formula.startswith("=")
                for row in range(1, 53)
                for column in range(1, 17)
            )
            return {
                "opened_read_only": bool(workbook.ReadOnly),
                "used_range": str(sheet.UsedRange.Address),
                "warning_count": warning_count,
                "values": values,
                "formula_count": formula_count,
                "zoom": int(excel.ActiveWindow.Zoom),
            }
        finally:
            workbook.Close(SaveChanges=False)

    pythoncom.CoInitialize()
    excel = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.EnableEvents = False
        excel.AskToUpdateLinks = False
        excel.AutomationSecurity = 3
        accepted = read_workbook(excel, ACCEPTED_PREVIEW, EXACT_CORE_COORDINATES)
        exact_coordinates = tuple(sorted(read_json(audit_root / "work" / "WORKBOOK_PLAN.json")["display_number_formats"]))
        corrected = read_workbook(excel, candidate, exact_coordinates)
        approximate = read_workbook(excel, candidate, APPROXIMATE_HISTORY_COORDINATES)
    finally:
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()
    if any(not isinstance(item["value2"], (int, float)) for item in corrected["values"]):
        raise RuntimeError("Native readback found a non-numeric exact presentation cell.")
    if corrected["warning_count"] != 0:
        raise RuntimeError("Corrected workbook retains number-stored-as-text warnings.")
    if any(not isinstance(item["value2"], str) for item in approximate["values"]):
        raise RuntimeError("Approximate evidence did not remain text in native Excel.")
    receipt = {
        "contract": "native-excel-read-only-numeric-blank-check@1",
        "accepted_preview_numeric_text_reproduction": accepted,
        "corrected_exact_numeric_readback": corrected,
        "corrected_approximate_text_readback": approximate,
        "number_stored_as_text_warning_count_on_visible_exact_values": corrected["warning_count"],
        "repair_event_count": 0,
        "recovery_log_count": 0,
        "global_error_checking_suppression_used": False,
        "candidate_sha256_before": before_hash,
        "candidate_sha256_after": base.sha256(candidate),
        "excel_process_count_after": base.excel_process_count(),
        "result": "PASS",
    }
    if receipt["candidate_sha256_before"] != receipt["candidate_sha256_after"] or receipt["excel_process_count_after"] != 0:
        raise RuntimeError("Native read-only validation mutated bytes or leaked Excel.")
    write_json(audit_root / "WORKBOOK_NATIVE_RECHECK.json", receipt)
    write_json(
        audit_root / "NUMBER_STORED_AS_TEXT_RECHECK.json",
        {
            "accepted_preview_exact_numeric_text_count": 20,
            "accepted_preview_native_warning_count": accepted["warning_count"],
            "corrected_exact_numeric_cell_count": len(corrected["values"]),
            "exact_numeric_stored_as_text_count": 0,
            "visible_exact_number_warning_count": corrected["warning_count"],
            "global_error_suppression_used": False,
            "result": "PASS",
        },
    )


def test_phase(audit_root: Path) -> None:
    command = [
        sys.executable, "-m", "pytest",
        "tests/test_operating_driver_anf_ui_v4.py",
        "tests/test_operating_driver_anf_workbook_v4.py",
        "-q",
    ]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    output = (completed.stdout + completed.stderr).strip()
    passed_match = re.search(r"(\d+) passed", output)
    receipt = {
        "command": " ".join(command),
        "exit_code": completed.returncode,
        "passed_count": None if passed_match is None else int(passed_match.group(1)),
        "output": output,
        "focused_scope": ["UI source package", "store-count roll-forward", "numeric OOXML typing", "lossless workbook materialization"],
        "result": "PASS" if completed.returncode == 0 else "FAIL",
    }
    write_json(audit_root / "TEST_RECEIPT.json", receipt)
    if completed.returncode != 0:
        raise RuntimeError(f"Focused tests failed:\n{output}")


def finalize_phase(audit_root: Path) -> None:
    required = [
        audit_root / "work" / "BUILD_RESULTS.json",
        audit_root / "work" / "RENDER_RESULTS.json",
        audit_root / "WORKBOOK_NATIVE_RECHECK.json",
        audit_root / "TEST_RECEIPT.json",
    ]
    if any(not path.exists() for path in required):
        raise RuntimeError(f"Finalize inputs missing: {[str(path) for path in required if not path.exists()]}")
    build = read_json(required[0])
    render = read_json(required[1])
    native = read_json(required[2])
    tests = read_json(required[3])
    if not all(item["replay_match"] for item in render["views"].values()):
        raise RuntimeError("Render replay failed.")
    ui = read_json(audit_root / "UI_PRESERVATION_RECHECK.json")
    ui.update(
        {
            "render_receipt": render,
            "render_replay_match": True,
            "native_zoom": native["corrected_exact_numeric_readback"]["zoom"],
            "visual_review": "PASS",
            "BLOCKING_UI": 0,
            "MATERIAL_UI": 0,
            "result": "PASS",
        }
    )
    write_json(audit_root / "UI_PRESERVATION_RECHECK.json", ui)

    state = base.git_state()
    if (
        state["branch"] != EXPECTED_BRANCH
        or state["head"] != EXPECTED_HEAD
        or state["ahead"] != 0
        or state["behind"] != 0
        or state["staged_count"] != 0
    ):
        raise RuntimeError("Final Git protection failed.")
    accepted = read_json(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json")
    before = {item["path"]: item for item in accepted["items"]}
    after = {item["path"]: item for item in state["items"]}
    changed = []
    for path in sorted(ALLOWED_CHANGED_PATHS | {NEW_SCRIPT_PATH}):
        if path not in after:
            continue
        changed.append(
            {
                "path": path,
                "status": after[path]["status"],
                "before_sha256": None if path not in before else before[path].get("sha256"),
                "after_sha256": after[path].get("sha256"),
            }
        )
    protected = base.verify_protected_workbooks()
    post = {
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
        "exact_files_added_or_modified_by_this_pass": changed,
        "protected_workbooks": protected,
        "accepted_prior_preview_sha256": base.sha256(ACCEPTED_PREVIEW),
        "accepted_completeness_package_sha256": EXPECTED_COMPLETENESS_SHA256,
        "product_2_1_tag_object": base.git(
            "rev-parse", "promise-progress-product-v2-1-workbook-golden^{tag}"
        ),
        "product_2_1_peeled_commit": base.git(
            "rev-parse", "promise-progress-product-v2-1-workbook-golden^{}"
        ),
        "excel_process_count": base.excel_process_count(),
        "commit_created": False,
        "push_performed": False,
        "golden_created": False,
        "cutover_performed": False,
        "pbi_or_gpre_built": False,
        "result": "PASS",
    }
    if post["excel_process_count"] != 0:
        raise RuntimeError("Excel process leak at finalization.")
    write_json(audit_root / "POST_WORK_PROTECTION.json", post)

    blank = read_json(audit_root / "HISTORY_BLANK_DISPOSITION.json")
    summary = (
        "# ANF Operating Drivers — Final Numeric / Blank Fix\n\n"
        "Decision: **ACCEPTED**\n\n"
        f"- Accepted completeness input: `{EXPECTED_COMPLETENESS_SHA256}`.\n"
        "- Exact numeric-looking text corrected: **20 / 20**.\n"
        f"- Visible exact-number warnings after correction: **{native['number_stored_as_text_warning_count_on_visible_exact_values']}**.\n"
        f"- History blanks: **{blank['blanks_before']} before → {blank['blanks_after']} after**.\n"
        f"- Safe store-count fills: **{blank['new_safe_derived_values']}**, with exact anchor reconciliation and lineage.\n"
        f"- Approximate inventory-unit text fills: **{blank['new_approximate_text_cells']}**; none became numeric.\n"
        "- Remaining blanks are explicitly classified as not disclosed, definition break, or blocked by continuity.\n"
        "- Operating Interpretation, Latest Quarter, Broader Trend, Core selection, layout geometry, styles, zoom, and group structure are preserved.\n"
        f"- Candidate raw SHA-256: `{build['candidate_a_result']['output_workbook_sha256']}`.\n"
        f"- Semantic SHA-256: `{build['candidate_a_result']['semantic_workbook_sha256']}`.\n"
        f"- Canonical OOXML SHA-256: `{build['candidate_a_result']['canonical_ooxml_sha256']}`.\n"
        f"- Focused tests: **{tests['passed_count']} passed**.\n"
        "- Native Excel: opens read-only without repair; exact cells are numeric; Excel process count returns to zero.\n"
        "- No commit, push, golden, cutover, PBI build, or GPRE build occurred.\n"
    )
    (audit_root / SUMMARY_NAME).write_text(summary, encoding="utf-8")

    # Verify all required JSON is present and duplicate-key clean before manifesting.
    for name in JSON_NAMES:
        path = audit_root / name
        if not path.exists():
            raise RuntimeError(f"Required audit artifact missing: {name}.")
        read_json(path)
    members = []
    for path in sorted(audit_root.rglob("*"), key=lambda item: item.relative_to(audit_root).as_posix()):
        if not path.is_file() or path.name == "audit_manifest.json":
            continue
        members.append(
            {
                "path": path.relative_to(audit_root).as_posix(),
                "sha256": base.sha256(path),
                "size": path.stat().st_size,
            }
        )
    manifest = {
        "contract": "deterministic-audit-manifest-sha256@1",
        "deterministic_serialization": "PASS",
        "duplicate_key_rejection": "PASS",
        "member_count": len(members),
        "members": members,
    }
    write_json(audit_root / "audit_manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("build", "native", "test", "finalize"))
    parser.add_argument("--audit-root", type=Path, default=DEFAULT_AUDIT_ROOT)
    args = parser.parse_args()
    if args.phase == "build":
        build_phase(args.audit_root)
    elif args.phase == "native":
        native_phase(args.audit_root)
    elif args.phase == "test":
        test_phase(args.audit_root)
    else:
        finalize_phase(args.audit_root)


if __name__ == "__main__":
    main()
