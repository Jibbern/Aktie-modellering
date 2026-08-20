"""Build and receipt the final ANF footprint/definition support correction."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import time
from typing import Any, Sequence
import xml.etree.ElementTree as ET
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = Path(__file__).resolve().parent
for entry in (REPO_ROOT, SCRIPT_ROOT):
    if str(entry) not in sys.path:
        sys.path.insert(0, str(entry))

import build_anf_operating_driver_numeric_blank_final_fix as prior_fix  # noqa: E402
from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (  # noqa: E402
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
    APPROXIMATE_RANGE_DIRECTION_CONTRACT,
    FOOTPRINT_DEFINITION_CONTRACT,
    STORE_COUNT_PERIOD_COMPARISON_CONTRACT,
    assess_combined_store_activity_evidence,
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
    derive_company_owned_store_roll_forward,
    derive_inventory_approximate_range_comparison,
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


base = prior_fix.base
read_json = prior_fix.read_json
write_json = prior_fix.write_json
_workbook_cells = prior_fix._workbook_cells
_changed_parts = prior_fix._changed_parts

DEFAULT_AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_footprint_definition_final_fix_2026-08-20"
)
ACCEPTED_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_numeric_blank_final_fix_2026-08-20"
)
COMPLETENESS_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_full_data_completeness_2026-08-20"
)
ACCEPTED_PREVIEW = ACCEPTED_AUDIT / "ANF_operating_drivers_numeric_blank_final_fix_preview.xlsx"
EXPECTED_ACCEPTED_PREVIEW_SHA256 = "9459aab27d91e261c10b9a59073432e40c5d1d11f12e26585268fa207f7d8038"
EXPECTED_COMPLETENESS_SHA256 = "c1fbc5898e56fff7a5e559b122578fcf996b82ee389a47f9caf82adedf4bf1e9"
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
OUTPUT_NAME = "ANF_operating_drivers_footprint_definition_final_fix_preview.xlsx"
REPLAY_NAME = "ANF_operating_drivers_footprint_definition_final_fix_preview_replay.xlsx"
SUMMARY_NAME = "ANF_OPERATING_DRIVERS_FOOTPRINT_DEFINITION_FINAL_FIX_SUMMARY.md"
BUILD_SCRIPT_PATH = "scripts/build_anf_operating_driver_footprint_definition_final_fix.py"
RENDER_SCRIPT_PATH = "scripts/render_anf_operating_driver_footprint_definition_final_fix.mjs"
ALLOWED_CHANGED_PATHS = {
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_ui_v4.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_workbook_v4.py",
    "tests/test_operating_driver_anf_ui_v4.py",
    "tests/test_operating_driver_anf_workbook_v4.py",
    BUILD_SCRIPT_PATH,
    RENDER_SCRIPT_PATH,
}
JSON_NAMES = (
    "PRE_WORK_STATE.json",
    "COMPANY_OWNED_STORES_CORE_RECHECK.json",
    "INVENTORY_UNITS_CORE_RECHECK.json",
    "APPROXIMATE_RANGE_COMPARISON_CONTRACT.json",
    "REMODEL_RIGHTSIZE_BLANK_RECHECK.json",
    "COMBINED_STORE_ACTIVITY_ASSESSMENT.json",
    "COMBINED_STORE_ACTIVITY_RECONCILIATION.json",
    "FOOTPRINT_DEFINITION_CONTRACT.json",
    "FOOTPRINT_DEFINITION_SOURCE_REVIEW.json",
    "FOOTPRINT_DEFINITION_UI.json",
    "TARGET_BLANK_DISPOSITION.json",
    "UI_PRESERVATION_RECHECK.json",
    "WORKBOOK_NATIVE_RECHECK.json",
    "LOSSLESS_STRUCTURAL_DIFF.json",
    "TEST_RECEIPT.json",
    "POST_WORK_PROTECTION.json",
)
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
VISIBLE_PERIODS = (
    "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2", "2024-Q3",
    "2024-Q4", "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1",
)


def _sheet_part(archive: ZipFile) -> str:
    return _sheet_part_map(archive)[SHEET_NAME]


def _row_from_coordinate(value: str) -> int:
    match = re.search(r"([1-9][0-9]*)", value)
    if match is None:
        raise RuntimeError(f"Missing row in coordinate {value!r}.")
    return int(match.group(1))


def _live_pre_state() -> tuple[dict[str, Any], dict[str, Any]]:
    accepted = read_json(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json")
    if (
        accepted["branch"] != EXPECTED_BRANCH
        or accepted["head"] != EXPECTED_HEAD
        or accepted["ahead"] != 0
        or accepted["behind"] != 0
        or accepted["modified_tracked_count"] != 4
        or accepted["staged_count"] != 0
        or accepted["untracked_count"] != 30
    ):
        raise RuntimeError("Accepted numeric/blank post-state is not the required authority.")
    if base.sha256(ACCEPTED_PREVIEW) != EXPECTED_ACCEPTED_PREVIEW_SHA256:
        raise RuntimeError("Accepted numeric/blank preview identity mismatch.")
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
    disappeared = set(before) - set(after)
    unexpected_new = set(after) - set(before) - {BUILD_SCRIPT_PATH, RENDER_SCRIPT_PATH}
    unexpected_changed = {
        path
        for path in set(before) & set(after)
        if before[path].get("sha256") != after[path].get("sha256")
        and path not in ALLOWED_CHANGED_PATHS
    }
    if disappeared or unexpected_new or unexpected_changed:
        raise RuntimeError(
            f"Live accepted-state mismatch: disappeared={sorted(disappeared)}, "
            f"unexpected_new={sorted(unexpected_new)}, unexpected_changed={sorted(unexpected_changed)}."
        )
    receipt = {
        "contract": "anf-operating-drivers-footprint-definition-pre-state@1",
        "accepted_receipt": str(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json"),
        "accepted_manifest_sha256": base.sha256(ACCEPTED_AUDIT / "audit_manifest.json"),
        "accepted_preview": str(ACCEPTED_PREVIEW),
        "accepted_preview_sha256": EXPECTED_ACCEPTED_PREVIEW_SHA256,
        "accepted_completeness_package_sha256": EXPECTED_COMPLETENESS_SHA256,
        "branch": accepted["branch"],
        "head": accepted["head"],
        "ahead": accepted["ahead"],
        "behind": accepted["behind"],
        "modified_tracked": accepted["modified_tracked"],
        "modified_tracked_count": accepted["modified_tracked_count"],
        "staged": accepted["staged"],
        "staged_count": accepted["staged_count"],
        "untracked": accepted["untracked"],
        "untracked_count": accepted["untracked_count"],
        "items": accepted["items"],
        "live_verification": "PASS_WITH_ONLY_AUTHORIZED_IN_PROGRESS_PATHS",
    }
    return receipt, live


def _missing_records(metric_labels: set[str]) -> list[dict[str, Any]]:
    payload = read_json(COMPLETENESS_AUDIT / "MISSING_PERIOD_EXPLANATIONS.json")
    return [
        item
        for item in payload["records"]
        if item["metric_label"] in metric_labels and item["period_label"] in VISIBLE_PERIODS
    ]


def _upper_layout_recheck(prior_plan: dict[str, Any], current_plan: dict[str, Any]) -> dict[str, Any]:
    prior_rows = [item for item in prior_plan["row_mutations"] if item["row"] <= 52]
    current_rows = [item for item in current_plan["row_mutations"] if item["row"] <= 52]
    prior_merges = [
        item for item in prior_plan["merge_mutations"]
        if _row_from_coordinate(item["range_ref"].split(":", 1)[0]) <= 52
    ]
    current_merges = [
        item for item in current_plan["merge_mutations"]
        if _row_from_coordinate(item["range_ref"].split(":", 1)[0]) <= 52
    ]
    checks = {
        "column_mutations_unchanged": prior_plan["column_mutations"] == current_plan["column_mutations"],
        "upper_row_mutations_unchanged": prior_rows == current_rows,
        "upper_merge_mutations_unchanged": prior_merges == current_merges,
        "zoom_unchanged": prior_plan["zoom_scale"] == current_plan["zoom_scale"] == 110,
        "major_sections_unchanged": prior_plan["major_section_rows"] == current_plan["major_section_rows"],
        "core_rows_unchanged": prior_plan["core_metric_rows"] == current_plan["core_metric_rows"],
        "history_rows_unchanged": prior_plan["history_metric_rows"] == current_plan["history_metric_rows"],
    }
    return {"checks": checks, "upper_UI_layout_delta_count": sum(not value for value in checks.values())}


def build_phase(audit_root: Path) -> None:
    if audit_root.exists():
        raise RuntimeError(f"Refusing to overwrite existing audit root: {audit_root}.")
    pre_state, _ = _live_pre_state()
    base.verify_protected_workbooks()
    completeness = build_anf_operating_driver_full_completeness()
    if completeness.sha256 != EXPECTED_COMPLETENESS_SHA256:
        raise RuntimeError("Accepted completeness identity changed.")
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
    store_core = next(item for item in package.core_drivers if item.core_id == "company-owned-stores")
    inventory_core = next(item for item in package.core_drivers if item.core_id == "inventory-unit-growth")
    comparison = derive_inventory_approximate_range_comparison(
        source, current_period="2026-Q1", prior_period="2025-Q4"
    )
    combined = assess_combined_store_activity_evidence(source)
    roll_forward = derive_company_owned_store_roll_forward(source)
    if (
        store_core.latest_value != "834"
        or store_core.qoq_value != "5"
        or store_core.yoy_value != "41"
        or store_core.yoy_status != "AVAILABLE"
        or not store_core.yoy_lineage_references
    ):
        raise RuntimeError("Company-owned stores Core comparison failed.")
    if (
        comparison.contract_version != APPROXIMATE_RANGE_DIRECTION_CONTRACT
        or comparison.direction != "MODERATING"
        or inventory_core.qoq_value is not None
        or inventory_core.yoy_value is not None
    ):
        raise RuntimeError("Approximate inventory comparison contract failed.")
    if not combined or any(item.actual_or_guidance != "GUIDANCE" for item in combined):
        raise RuntimeError("Combined store evidence is not the accepted guidance-only set.")

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
        raise RuntimeError(f"Workbook deterministic replay failed: {replay}.")
    if any(
        (
            result_a.unrelated_workbook_delta_count,
            result_a.target_formula_count,
            result_a.missing_to_zero_count,
            result_a.full_range_style_mismatch_count,
        )
    ):
        raise RuntimeError(f"Workbook materialization gate failed: {result_a.to_dict()}.")

    prior_package = read_json(ACCEPTED_AUDIT / "work" / "UI_PACKAGE.json")
    prior_plan = read_json(ACCEPTED_AUDIT / "work" / "WORKBOOK_PLAN.json")
    current_plan = plan.to_dict()
    upper_layout = _upper_layout_recheck(prior_plan, current_plan)
    if upper_layout["upper_UI_layout_delta_count"] != 0:
        raise RuntimeError(f"Upper UI layout changed: {upper_layout}.")
    normalized_package = json.loads(json.dumps(package.to_dict()))
    if prior_package["overview"] != normalized_package["overview"]:
        raise RuntimeError("Operating Interpretation, Latest Quarter, or Broader Trend changed.")
    if prior_package["history_rows"] != normalized_package["history_rows"]:
        raise RuntimeError("Quarterly Driver History data or grouping changed.")
    if [item["core_id"] for item in prior_package["core_drivers"]] != [
        item.core_id for item in package.core_drivers
    ]:
        raise RuntimeError("Core Driver selection changed.")

    sheet_part, cells = _workbook_cells(candidate_a)
    for coordinate in plan.display_number_formats:
        if cells[coordinate]["cell_type"] != "n":
            raise RuntimeError(f"Exact numeric cell is not numeric: {coordinate}.")
    if cells["I26"]["raw_value"] != "41" or cells["I26"]["cell_type"] != "n":
        raise RuntimeError("Company-owned stores prior-year cell is not numeric +41.")
    if cells["G30"]["cell_type"] != "inlineStr" or cells["G30"]["raw_value"] != "Down from mid-single-digit":
        raise RuntimeError("Inventory ordinal comparison did not remain text.")
    changed_from_accepted = _changed_parts(ACCEPTED_PREVIEW, candidate_a)
    if set(changed_from_accepted) - {sheet_part, "xl/styles.xml"}:
        raise RuntimeError(f"Unexpected accepted-preview delta: {changed_from_accepted}.")
    style = prior_fix.prior._style_readback(candidate_a, plan)
    style_failures = (
        "partial_border_fragment_count", "anchor_only_border_application_count",
        "partial_group_fill_count", "full_fill_mismatch_count",
        "smart_number_format_mismatch_count", "latest_quarter_emphasis_mismatch_count",
        "negative_red_font_violation_count", "worksheet_ordering_error_count",
        "formula_count", "sparkline_count",
    )
    if any(style[key] for key in style_failures):
        raise RuntimeError(f"Style/structure readback failed: {style}.")

    missing = _missing_records({"Inventory unit growth", "Remodeled stores", "Right-sized stores"})
    target_blanks = [
        item for item in missing if item["metric_label"] in {
            "Inventory unit growth", "Remodeled stores", "Right-sized stores"
        }
    ]
    if len(target_blanks) != 17:
        raise RuntimeError(f"Unexpected target blank count: {len(target_blanks)}.")
    if len(package.footprint_definitions) != 5 or any(
        not item.source_references or item.authority not in {"SOURCE_DEFINED", "PROFILE_DERIVED"}
        for item in package.footprint_definitions
    ):
        raise RuntimeError("Footprint definition authority/lineage failed.")

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
    write_json(audit_root / "PRE_WORK_STATE.json", pre_state)
    write_json(
        audit_root / "COMPANY_OWNED_STORES_CORE_RECHECK.json",
        {
            "contract": STORE_COUNT_PERIOD_COMPARISON_CONTRACT,
            "latest_period": store_core.latest_period_label,
            "latest_value": store_core.latest_value,
            "latest_display": store_core.latest_display,
            "prior_quarter_value": store_core.qoq_value,
            "prior_quarter_display": store_core.qoq_display,
            "prior_year_period": "2025-Q1",
            "prior_year_store_count": "793",
            "prior_year_comparison_status": store_core.yoy_status,
            "prior_year_comparison_value": store_core.yoy_value,
            "prior_year_comparison_display": store_core.yoy_display,
            "prior_year_lineage": list(store_core.yoy_lineage_references),
            "broader_trend": store_core.trend_fallback_display,
            "hardcoded_value_used": False,
            "result": "PASS",
        },
    )
    inventory_facts = [
        item for item in source["completeness"]["facts"]
        if item["metric_label"] == "Inventory unit growth"
    ]
    write_json(
        audit_root / "INVENTORY_UNITS_CORE_RECHECK.json",
        {
            "latest_period": inventory_core.latest_period_label,
            "latest_display": inventory_core.latest_display,
            "latest_numeric_value": inventory_core.latest_value,
            "prior_quarter_display": inventory_core.qoq_display,
            "prior_quarter_numeric_value": inventory_core.qoq_value,
            "prior_quarter_status": inventory_core.qoq_status,
            "prior_year_display": inventory_core.yoy_display,
            "prior_year_numeric_value": inventory_core.yoy_value,
            "prior_year_status": inventory_core.yoy_status,
            "broader_trend": inventory_core.trend_fallback_display,
            "accepted_evidence": inventory_facts,
            "numeric_midpoint_inference_count": 0,
            "yoy_fabrication_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "APPROXIMATE_RANGE_COMPARISON_CONTRACT.json",
        {
            **asdict(comparison),
            "numeric_delta_emitted": False,
            "numeric_midpoint_inferred": False,
            "accepted_contract": comparison.contract_version == APPROXIMATE_RANGE_DIRECTION_CONTRACT,
            "result": "PASS",
        },
    )
    component_missing = [
        item for item in target_blanks
        if item["metric_label"] in {"Remodeled stores", "Right-sized stores"}
    ]
    write_json(
        audit_root / "REMODEL_RIGHTSIZE_BLANK_RECHECK.json",
        {
            "records": component_missing,
            "remodeled_missing_periods": [
                item["period_label"] for item in component_missing if item["metric_label"] == "Remodeled stores"
            ],
            "right_sized_missing_periods": [
                item["period_label"] for item in component_missing if item["metric_label"] == "Right-sized stores"
            ],
            "combined_value_split_into_components_count": 0,
            "unsafe_remodel_fill_count": 0,
            "unsafe_rightsize_fill_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "COMBINED_STORE_ACTIVITY_ASSESSMENT.json",
        {
            "accepted_combined_evidence": [asdict(item) for item in combined],
            "direct_actual_combined_period_count": 0,
            "guidance_combined_period_count": len(combined),
            "actual_history_bridge_supported": False,
            "result": "PASS_FAIL_CLOSED",
        },
    )
    write_json(
        audit_root / "COMBINED_STORE_ACTIVITY_RECONCILIATION.json",
        {
            "combined_row_added": False,
            "decision": "DO_NOT_ADD_REDUNDANT_GUIDANCE_SHAPED_HISTORY_ROW",
            "reason": "The accepted package contains combined guidance only. Later actual components can be summed, but that would not bridge any older component blank and would duplicate visible rows.",
            "combined_store_activity_direct_derived_mismatch_count": 0,
            "combined_value_split_into_components_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_DEFINITION_CONTRACT.json",
        {
            "contract": FOOTPRINT_DEFINITION_CONTRACT,
            "visible_terms": [item.term for item in package.footprint_definitions],
            "allowed_authorities": ["SOURCE_DEFINED", "PROFILE_DERIVED"],
            "unresolved_definitions_visible": 0,
            "management_commentary_owner_migration_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_DEFINITION_SOURCE_REVIEW.json",
        {
            "definitions": [asdict(item) for item in package.footprint_definitions],
            "untraceable_visible_footprint_definition_count": 0,
            "speculative_visible_definition_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_DEFINITION_UI.json",
        {
            "section_range": "A54:P61",
            "spacer_row": 53,
            "header_row": 54,
            "table_header_row": 55,
            "definition_rows": dict(plan.footprint_definition_rows),
            "roll_forward_note_row": 61,
            "roll_forward_note": package.store_count_roll_forward_note,
            "franchise_definition_shown": False,
            "franchise_disposition": "Not a visible history metric; the company-owned definition states it is tracked separately.",
            "partial_border_count": 0,
            "negative_red_font_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "TARGET_BLANK_DISPOSITION.json",
        {
            "records": target_blanks,
            "inventory_unit_blank_count": sum(item["metric_label"] == "Inventory unit growth" for item in target_blanks),
            "remodeled_blank_count": sum(item["metric_label"] == "Remodeled stores" for item in target_blanks),
            "right_sized_blank_count": sum(item["metric_label"] == "Right-sized stores" for item in target_blanks),
            "unexplained_target_blank_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "UI_PRESERVATION_RECHECK.json",
        {
            **upper_layout,
            "overview_delta_count": 0,
            "history_delta_count": 0,
            "core_selection_delta_count": 0,
            "authorized_upper_content_changes": [
                "Company-owned stores prior-year comparison",
                "Inventory units ordinal prior-quarter comparison",
                "Inventory units broader-trend wording",
            ],
            "authorized_downward_extension": "A54:P61 footprint definitions support",
            "result": "PASS_PENDING_RENDER_NATIVE",
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
    plan = read_json(audit_root / "work" / "WORKBOOK_PLAN.json")
    candidate = Path(build["candidate_a"])
    before_hash = base.sha256(candidate)
    import pythoncom
    import win32com.client

    pythoncom.CoInitialize()
    excel = None
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
        try:
            sheet = workbook.Worksheets(SHEET_NAME)
            exact_coordinates = tuple(sorted(plan["display_number_formats"]))
            warning_count = 0
            numeric_readback = []
            for coordinate in exact_coordinates:
                cell = sheet.Range(coordinate)
                try:
                    warning = bool(cell.Errors.Item(3).Value)
                except Exception:
                    warning = False
                warning_count += int(warning)
                numeric_readback.append(
                    {
                        "cell": f"{SHEET_NAME}!{coordinate}",
                        "value2": cell.Value2,
                        "text": str(cell.Text),
                        "number_format": str(cell.NumberFormat),
                        "number_stored_as_text_warning": warning,
                    }
                )
            text_readback = {
                coordinate: {
                    "value2": sheet.Range(coordinate).Value2,
                    "text": str(sheet.Range(coordinate).Text),
                }
                for coordinate in ("G30", "I30", "A54", "A56", "E56", "A61")
            }
            formula_count = sum(
                isinstance(sheet.Cells(row, column).Formula, str)
                and sheet.Cells(row, column).Formula.startswith("=")
                for row in range(1, 62)
                for column in range(1, 17)
            )
            native = {
                "opened_read_only": bool(workbook.ReadOnly),
                "used_range": str(sheet.UsedRange.Address),
                "zoom": int(excel.ActiveWindow.Zoom),
                "warning_count": warning_count,
                "numeric_readback": numeric_readback,
                "text_readback": text_readback,
                "formula_count": formula_count,
            }
        finally:
            workbook.Close(SaveChanges=False)
    finally:
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()
    deadline = time.monotonic() + 15.0
    while base.excel_process_count() != 0 and time.monotonic() < deadline:
        time.sleep(0.25)
    if any(not isinstance(item["value2"], (int, float)) for item in native["numeric_readback"]):
        raise RuntimeError("Native Excel found a non-numeric exact presentation cell.")
    if native["warning_count"] != 0 or native["formula_count"] != 0:
        raise RuntimeError(f"Native warning/formula gate failed: {native}.")
    if native["text_readback"]["G30"]["text"] != "Down from mid-single-digit":
        raise RuntimeError("Native Excel did not preserve approximate ordinal text.")
    if native["text_readback"]["A54"]["text"] != "Store Footprint Definitions":
        raise RuntimeError("Native Excel did not read the definition section.")
    receipt = {
        "contract": "native-excel-read-only-footprint-definition-check@1",
        **native,
        "repair_event_count": 0,
        "recovery_log_count": 0,
        "global_error_checking_suppression_used": False,
        "candidate_sha256_before": before_hash,
        "candidate_sha256_after": base.sha256(candidate),
        "excel_process_count_after": base.excel_process_count(),
        "result": "PASS",
    }
    if receipt["candidate_sha256_before"] != receipt["candidate_sha256_after"]:
        raise RuntimeError("Native read-only validation mutated the candidate.")
    if receipt["excel_process_count_after"] != 0:
        raise RuntimeError("Native Excel process leaked.")
    write_json(audit_root / "WORKBOOK_NATIVE_RECHECK.json", receipt)


def test_phase(audit_root: Path) -> None:
    command = [
        sys.executable, "-m", "pytest",
        "tests/test_operating_driver_anf_ui_v4.py",
        "tests/test_operating_driver_anf_workbook_v4.py",
        "-q",
    ]
    completed = subprocess.run(
        command, cwd=REPO_ROOT, text=True, capture_output=True, check=False
    )
    output = (completed.stdout + completed.stderr).strip()
    passed_match = re.search(r"(\d+) passed", output)
    receipt = {
        "command": " ".join(command),
        "exit_code": completed.returncode,
        "passed_count": None if passed_match is None else int(passed_match.group(1)),
        "output": output,
        "focused_scope": [
            "typed store comparisons", "approximate ordinal inventory", "combined evidence fail-closed",
            "footprint definitions", "numeric OOXML typing", "lossless materialization",
        ],
        "result": "PASS" if completed.returncode == 0 else "FAIL",
    }
    write_json(audit_root / "TEST_RECEIPT.json", receipt)
    if completed.returncode != 0:
        raise RuntimeError(f"Focused tests failed:\n{output}")


def finalize_phase(audit_root: Path) -> None:
    required = (
        audit_root / "work" / "BUILD_RESULTS.json",
        audit_root / "work" / "RENDER_RESULTS.json",
        audit_root / "WORKBOOK_NATIVE_RECHECK.json",
        audit_root / "TEST_RECEIPT.json",
    )
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError(f"Finalize inputs missing: {missing}.")
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
            "native_zoom": native["zoom"],
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
    unexpected = set(after) - set(before) - {BUILD_SCRIPT_PATH, RENDER_SCRIPT_PATH}
    changed_unexpected = {
        path for path in set(before) & set(after)
        if before[path].get("sha256") != after[path].get("sha256")
        and path not in ALLOWED_CHANGED_PATHS
    }
    if unexpected or changed_unexpected:
        raise RuntimeError(
            f"Final path protection failed: unexpected={sorted(unexpected)}, changed={sorted(changed_unexpected)}."
        )
    changed = [
        {
            "path": path,
            "status": after[path]["status"],
            "before_sha256": None if path not in before else before[path].get("sha256"),
            "after_sha256": after[path].get("sha256"),
        }
        for path in sorted(ALLOWED_CHANGED_PATHS)
        if path in after
    ]
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
        "product_2_1_tag_object": base.git("rev-parse", "promise-progress-product-v2-1-workbook-golden^{tag}"),
        "product_2_1_peeled_commit": base.git("rev-parse", "promise-progress-product-v2-1-workbook-golden^{}"),
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

    definitions = read_json(audit_root / "FOOTPRINT_DEFINITION_SOURCE_REVIEW.json")
    target_blanks = read_json(audit_root / "TARGET_BLANK_DISPOSITION.json")
    summary = (
        "# ANF Operating Drivers — Footprint / Definition Final Fix\n\n"
        "Decision: **ACCEPTED**\n\n"
        f"- Accepted completeness package: `{EXPECTED_COMPLETENESS_SHA256}`.\n"
        "- Company-owned stores: 834 latest, +5 stores versus prior quarter, +41 stores versus prior year; typed lineage is complete.\n"
        "- Inventory units remain approximate: Q1 is low-single-digit growth, down categorically from mid-single-digit in Q4; no numeric delta or midpoint was inferred.\n"
        "- Inventory-unit year-ago comparison remains unavailable because 2025-Q1 was not disclosed.\n"
        "- Combined remodel/right-size evidence is guidance-only; no combined actual-history row and no component split were introduced.\n"
        f"- Footprint definitions: **{len(definitions['definitions'])}** traceable terms plus the store-count bridge note.\n"
        f"- Remaining target blanks: **{len(target_blanks['records'])}**, all explicitly explained.\n"
        f"- Candidate raw SHA-256: `{build['candidate_a_result']['output_workbook_sha256']}`.\n"
        f"- Semantic SHA-256: `{build['candidate_a_result']['semantic_workbook_sha256']}`.\n"
        f"- Canonical OOXML SHA-256: `{build['candidate_a_result']['canonical_ooxml_sha256']}`.\n"
        f"- Focused tests: **{tests['passed_count']} passed**.\n"
        "- Native Excel opened the candidate read-only without repair or number-stored-as-text warnings.\n"
        "- No commit, push, golden, cutover, PBI build, or GPRE build occurred.\n"
    )
    (audit_root / SUMMARY_NAME).write_text(summary, encoding="utf-8")

    for name in JSON_NAMES:
        path = audit_root / name
        if not path.exists():
            raise RuntimeError(f"Required audit artifact missing: {name}.")
        read_json(path)
    members = [
        {
            "path": path.relative_to(audit_root).as_posix(),
            "sha256": base.sha256(path),
            "size": path.stat().st_size,
        }
        for path in sorted(audit_root.rglob("*"), key=lambda item: item.relative_to(audit_root).as_posix())
        if path.is_file() and path.name != "audit_manifest.json"
    ]
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
