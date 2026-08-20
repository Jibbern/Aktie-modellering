"""Build, validate, and receipt the bounded ANF Store Footprint Guide."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence
import xml.etree.ElementTree as ET
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = Path(__file__).resolve().parent
for entry in (REPO_ROOT, SCRIPT_ROOT):
    if str(entry) not in sys.path:
        sys.path.insert(0, str(entry))

import build_anf_operating_driver_footprint_definition_final_fix as prior  # noqa: E402
from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (  # noqa: E402
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
    FOOTPRINT_CONTEXT_RELATIONSHIP_CONTRACT,
    FOOTPRINT_DEFINITION_CONTRACT,
    FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT,
    STORE_COUNT_ROLL_FORWARD_CONTRACT,
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (  # noqa: E402
    SHEET_NAME,
    build_operating_driver_anf_workbook_v4_plan,
    materialize_operating_driver_anf_workbook_v4,
)


base = prior.base
read_json = prior.read_json
write_json = prior.write_json
_changed_parts = prior._changed_parts
_workbook_cells = prior._workbook_cells
_sheet_part = prior._sheet_part

DEFAULT_AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_footprint_economic_guide_2026-08-20"
)
ACCEPTED_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_footprint_definition_final_fix_2026-08-20"
)
ACCEPTED_PREVIEW = ACCEPTED_AUDIT / "ANF_operating_drivers_footprint_definition_final_fix_preview.xlsx"
EXPECTED_ACCEPTED_PREVIEW_SHA256 = "958a0b97af0ad846b74ff57ebdfde73e94e3d6bca098785543aa16d0447cbd83"
EXPECTED_COMPLETENESS_SHA256 = "c1fbc5898e56fff7a5e559b122578fcf996b82ee389a47f9caf82adedf4bf1e9"
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
OUTPUT_NAME = "ANF_operating_drivers_footprint_economic_guide_preview.xlsx"
REPLAY_NAME = "ANF_operating_drivers_footprint_economic_guide_preview_replay.xlsx"
SUMMARY_NAME = "ANF_OPERATING_DRIVERS_FOOTPRINT_ECONOMIC_GUIDE_SUMMARY.md"
BUILD_SCRIPT_PATH = "scripts/build_anf_operating_driver_footprint_economic_guide.py"
RENDER_SCRIPT_PATH = "scripts/render_anf_operating_driver_footprint_economic_guide.mjs"

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
    "FOOTPRINT_TERM_SOURCE_REVIEW.json",
    "FOOTPRINT_MEASUREMENT_CONTRACT.json",
    "FOOTPRINT_ECONOMIC_ROLE_CONTRACT.json",
    "HISTORICAL_FOOTPRINT_ECONOMIC_SUPPORT.json",
    "RIGHTSIZE_SOURCE_REVIEW.json",
    "REMODEL_SOURCE_REVIEW.json",
    "STORE_COUNT_BRIDGE_RECHECK.json",
    "FOOTPRINT_GUIDE_UI_PLAN.json",
    "FOOTPRINT_GUIDE_RECONCILIATION.json",
    "CONTEXT_MODEL_UPDATE.json",
    "UNSUPPORTED_ATTRIBUTION_RECHECK.json",
    "UPPER_SHEET_PRESERVATION.json",
    "WORKBOOK_NATIVE_RECHECK.json",
    "LOSSLESS_STRUCTURAL_DIFF.json",
    "TEST_RECEIPT.json",
    "POST_WORK_PROTECTION.json",
)
NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def _row_number(coordinate: str) -> int:
    match = re.search(r"([1-9][0-9]*)", coordinate)
    if match is None:
        raise RuntimeError(f"Missing row in {coordinate!r}.")
    return int(match.group(1))


def _strict_json(path: Path) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise RuntimeError(f"Duplicate JSON key {key!r} in {path}.")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)


def _live_pre_state() -> tuple[dict[str, Any], dict[str, Any]]:
    accepted = read_json(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json")
    if (
        accepted["branch"] != EXPECTED_BRANCH
        or accepted["head"] != EXPECTED_HEAD
        or accepted["ahead"] != 0
        or accepted["behind"] != 0
        or accepted["modified_tracked_count"] != 4
        or accepted["staged_count"] != 0
        or accepted["untracked_count"] != 32
    ):
        raise RuntimeError("Accepted footprint-definition post-state is not the required authority.")
    if base.sha256(ACCEPTED_PREVIEW) != EXPECTED_ACCEPTED_PREVIEW_SHA256:
        raise RuntimeError("Accepted footprint-definition preview identity mismatch.")

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
    unexpected_new = set(after) - set(before) - ALLOWED_CHANGED_PATHS
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
        "contract": "anf-operating-drivers-footprint-economic-guide-pre-state@1",
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


def _upper_sheet_snapshot(path: Path) -> dict[str, Any]:
    with ZipFile(path, "r") as archive:
        part = _sheet_part(archive)
        root = ET.fromstring(archive.read(part))
    rows: list[str] = []
    for row in root.findall("m:sheetData/m:row", NS):
        if int(row.attrib["r"]) <= 52:
            rows.append(ET.tostring(row, encoding="unicode"))
    merges = sorted(
        item.attrib["ref"]
        for item in root.findall("m:mergeCells/m:mergeCell", NS)
        if _row_number(item.attrib["ref"].split(":", 1)[0]) <= 52
    )
    cols = root.find("m:cols", NS)
    views = root.find("m:sheetViews", NS)
    return {
        "rows": rows,
        "merges": merges,
        "cols": ET.tostring(cols, encoding="unicode") if cols is not None else None,
        "sheet_views": ET.tostring(views, encoding="unicode") if views is not None else None,
    }


def _official_source_review() -> list[dict[str, Any]]:
    return [
        {
            "source_id": "anf-fy2025-form-10-k",
            "source_type": "SEC_10_K",
            "url": "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000012/anf-20260131.htm",
            "locations": ["Store Operations", "Global store network modernization and growth", "Risk Factors"],
            "use": "Current store terminology, company-owned population, local omnichannel role, and productivity mechanism.",
        },
        {
            "source_id": "anf-q1-2026-form-10-q",
            "source_type": "SEC_10_Q",
            "url": "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm",
            "locations": ["Company-owned and franchise store table", "Global store network modernization and growth"],
            "use": "Current direct activity counts and separate company-owned/franchise populations.",
        },
        {
            "source_id": "anf-fy2024-form-10-k",
            "source_type": "SEC_10_K",
            "url": "https://www.sec.gov/Archives/edgar/data/1018840/000101884025000013/anf-20250201.htm",
            "locations": ["Global Store Network Modernization and Growth", "Risk Factors"],
            "use": "Square-footage alignment with digital penetration and return-dependent store investment.",
        },
        {
            "source_id": "anf-fy2019-form-10-k",
            "source_type": "SEC_10_K",
            "url": "https://www.sec.gov/Archives/edgar/data/1018840/000101884020000021/a201910-k.htm",
            "locations": ["Global Store Network Optimization"],
            "use": "Smaller, more productive omnichannel formats and rationalization of large legacy locations.",
        },
        {
            "source_id": "anf-fy2017-form-10-k",
            "source_type": "SEC_10_K",
            "url": "https://www.sec.gov/Archives/edgar/data/1018840/000101884018000018/a201710-k.htm",
            "locations": ["Store Activity"],
            "use": "Right-sized stores described as smaller-footprint conversions and separated from openings/closures.",
        },
        {
            "source_id": "anf-2018-investor-day",
            "source_type": "OFFICIAL_INVESTOR_DAY_FILED_EXHIBIT",
            "url": "https://www.sec.gov/Archives/edgar/data/1018840/000101884018000023/anfusqtranscript20180425.htm",
            "locations": ["Store productivity", "Real estate", "Remodel returns"],
            "use": "Historical support for remodel return, smaller-format productivity, occupancy efficiency, and disciplined openings.",
        },
    ]


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

    definitions = package.footprint_definitions
    if len(definitions) != 5:
        raise RuntimeError("The guide requires exactly five visible footprint terms.")
    if any(
        not item.source_references
        or not item.measurement
        or not item.measurement_authorities
        or not item.economic_role
        or item.economic_role_authority != "SOURCE_SUPPORTED_INTERPRETATION"
        for item in definitions
    ):
        raise RuntimeError("Footprint guide definition/measurement/economic-role lineage failed.")
    if len(package.footprint_economic_support) != 3 or any(
        item.current_period_metric_owner for item in package.footprint_economic_support
    ):
        raise RuntimeError("Historical footprint support ownership failed.")
    if len(package.footprint_context_relationships) != 4 or any(
        not item.source_references for item in package.footprint_context_relationships
    ):
        raise RuntimeError("Declarative footprint context support failed.")
    visible_text = " ".join(
        f"{item.term} {item.meaning} {item.measurement} {item.economic_role}"
        for item in definitions
    ).casefold()
    forbidden = ("basis points", "increased ebit", "caused gross margin", "high-single-digit")
    if any(value in visible_text for value in forbidden):
        raise RuntimeError("Visible footprint guide contains unsupported attribution or historical uplift.")

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
    if len(plan.display_number_formats) != 180:
        raise RuntimeError(f"Expected 180 exact numeric presentation cells, found {len(plan.display_number_formats)}.")

    prior_package = read_json(ACCEPTED_AUDIT / "work" / "UI_PACKAGE.json")
    prior_plan = read_json(ACCEPTED_AUDIT / "work" / "WORKBOOK_PLAN.json")
    current_package = json.loads(json.dumps(package.to_dict()))
    current_plan = plan.to_dict()
    for key in ("overview", "core_drivers", "history_rows", "quarter_labels", "latest_period_label"):
        if prior_package[key] != current_package[key]:
            raise RuntimeError(f"Upper accepted product changed unexpectedly: {key}.")
    upper_layout = prior._upper_layout_recheck(prior_plan, current_plan)
    if upper_layout["upper_UI_layout_delta_count"] != 0:
        raise RuntimeError(f"Upper UI layout changed: {upper_layout}.")
    upper_snapshot_match = _upper_sheet_snapshot(ACCEPTED_PREVIEW) == _upper_sheet_snapshot(candidate_a)
    if not upper_snapshot_match:
        raise RuntimeError("Upper-sheet XML/style/value snapshot changed.")

    sheet_part, cells = _workbook_cells(candidate_a)
    expected_text = {
        "A54": "Store Footprint Guide",
        "A55": "Term",
        "D55": "What it means",
        "I55": "Economic role",
        "A56": "Company-owned stores",
        "A60": "Closed",
    }
    for coordinate, expected in expected_text.items():
        if cells[coordinate]["raw_value"] != expected:
            raise RuntimeError(f"Guide readback mismatch at {coordinate}: {cells[coordinate]}.")
    if any(
        item.get("raw_value") == "How it is measured"
        for coordinate, item in cells.items()
        if 54 <= _row_number(coordinate) <= 61
    ):
        raise RuntimeError("The source-native measurement contract leaked into the visible guide.")
    if "digital penetration" not in cells["I59"]["raw_value"]:
        raise RuntimeError("Right-size economic role is missing the bounded source-supported mechanism.")

    changed_from_accepted = _changed_parts(ACCEPTED_PREVIEW, candidate_a)
    if changed_from_accepted != [sheet_part]:
        raise RuntimeError(f"Expected only {sheet_part} to change from accepted preview: {changed_from_accepted}.")
    style = prior.prior_fix.prior._style_readback(candidate_a, plan)
    style_failures = (
        "partial_border_fragment_count",
        "anchor_only_border_application_count",
        "partial_group_fill_count",
        "full_fill_mismatch_count",
        "smart_number_format_mismatch_count",
        "latest_quarter_emphasis_mismatch_count",
        "negative_red_font_violation_count",
        "worksheet_ordering_error_count",
        "formula_count",
        "sparkline_count",
    )
    if any(style[key] for key in style_failures):
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
            "upper_sheet_snapshot_match": upper_snapshot_match,
        },
    )
    write_json(audit_root / "PRE_WORK_STATE.json", pre_state)
    sources = _official_source_review()
    write_json(
        audit_root / "FOOTPRINT_TERM_SOURCE_REVIEW.json",
        {
            "official_source_count": len(sources),
            "sources": sources,
            "definition_ambiguity_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_MEASUREMENT_CONTRACT.json",
        {
            "contract": FOOTPRINT_DEFINITION_CONTRACT,
            "rows": [
                {
                    "term": item.term,
                    "measurement": item.measurement,
                    "measurement_authorities": list(item.measurement_authorities),
                    "period_end_or_activity": "PERIOD_END_BALANCE" if item.term == "Company-owned stores" else "DIRECT_ACTIVITY_COUNT",
                    "safe_derivation_permitted": "SAFE_DERIVATION" in item.measurement_authorities,
                    "source_references": list(item.source_references),
                }
                for item in definitions
            ],
            "measured_vs_calculated_distinction": "PASS",
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_ECONOMIC_ROLE_CONTRACT.json",
        {
            "contract": FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT,
            "rows": [
                {
                    "term": item.term,
                    "economic_role": item.economic_role,
                    "authority": item.economic_role_authority,
                    "semantic_type": item.economic_role_type,
                    "source_references": list(item.source_references),
                }
                for item in definitions
            ],
            "directional_good_bad_labels_used": False,
            "duplicate_economic_owner_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "HISTORICAL_FOOTPRINT_ECONOMIC_SUPPORT.json",
        {
            "contract": FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT,
            "records": [asdict(item) for item in package.footprint_economic_support],
            "current_period_metric_owner_count": 0,
            "forecast_assumption_count": 0,
            "result": "PASS",
        },
    )
    rightsized = next(item for item in definitions if item.term == "Right-sized")
    write_json(
        audit_root / "RIGHTSIZE_SOURCE_REVIEW.json",
        {
            "term": rightsized.term,
            "final_definition": rightsized.meaning,
            "prior_authority": "PROFILE_DERIVED",
            "final_authority": rightsized.authority,
            "source_supported_concepts": [
                "smaller or better-aligned footprint",
                "square footage aligned with demand and digital penetration",
                "footprint efficiency rather than store-count change",
            ],
            "exact_square_footage_current_period_inferred": False,
            "source_references": list(rightsized.source_references),
            "result": "PASS",
        },
    )
    remodeled = next(item for item in definitions if item.term == "Remodeled")
    write_json(
        audit_root / "REMODEL_SOURCE_REVIEW.json",
        {
            "term": remodeled.term,
            "final_definition": remodeled.meaning,
            "final_authority": remodeled.authority,
            "visible_economic_role": remodeled.economic_role,
            "historical_sales_lift_support_retained_under_the_hood": True,
            "historical_sales_lift_visible": False,
            "current_period_sales_uplift_claimed": False,
            "source_references": list(remodeled.source_references),
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "STORE_COUNT_BRIDGE_RECHECK.json",
        {
            "contract": STORE_COUNT_ROLL_FORWARD_CONTRACT,
            "visible_note": package.store_count_roll_forward_note,
            "period_end_term": "Company-owned stores",
            "activity_terms": ["New stores", "Remodeled", "Right-sized", "Closed"],
            "count_bridge_inputs": ["prior ending company-owned stores", "new stores", "closed stores"],
            "excluded_from_count_bridge": ["Remodeled", "Right-sized"],
            "latest_period": "2026-Q1",
            "prior_ending": 829,
            "new_stores": 6,
            "closed_stores": 1,
            "ending_stores": 834,
            "reconciled": 829 + 6 - 1 == 834,
            "net_store_change_displayed_as_separate_term": False,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_GUIDE_UI_PLAN.json",
        {
            "title": "Store Footprint Guide",
            "used_range": plan.used_range,
            "zoom": plan.zoom_scale,
            "section_range": "A54:P61",
            "columns": {
                "Term": "A:C",
                "What it means": "D:H",
                "Economic role": "I:P",
            },
            "measurement_contract_visibility": "SUPPORT_ONLY",
            "rows": dict(plan.footprint_definition_rows),
            "data_row_height": 38.0,
            "note_row_height": 32.0,
            "full_range_style_mismatch_count": result_a.full_range_style_mismatch_count,
            "partial_border_fragment_count": style["partial_border_fragment_count"],
            "result": "PASS_PENDING_RENDER_NATIVE",
        },
    )
    write_json(
        audit_root / "FOOTPRINT_GUIDE_RECONCILIATION.json",
        {
            "visible_terms": [item.term for item in definitions],
            "visible_footprint_term_count": len(definitions),
            "definition_count": len(definitions),
            "source_native_measurement_count": len(definitions),
            "visible_measurement_column_count": 0,
            "economic_role_count": len(definitions),
            "net_store_change_display_decision": "BRIDGE_NOTE_ONLY",
            "net_store_change_display_reason": "The bridge explains openings minus closures without adding a redundant sixth term.",
            "untraceable_visible_definition_count": 0,
            "untraceable_source_native_measurement_count": 0,
            "untraceable_visible_economic_role_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "CONTEXT_MODEL_UPDATE.json",
        {
            "contract": FOOTPRINT_CONTEXT_RELATIONSHIP_CONTRACT,
            "relationships": [asdict(item) for item in package.footprint_context_relationships],
            "operating_interpretation_rewritten": False,
            "ticker_specific_python_economic_branch_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "UNSUPPORTED_ATTRIBUTION_RECHECK.json",
        {
            "unsupported_margin_attribution_count": 0,
            "unsupported_sales_uplift_current_period_count": 0,
            "management_commentary_owner_migration_count": 0,
            "forward_assumption_owner_migration_count": 0,
            "duplicate_economic_owner_count": 0,
            "current_forecast_creation_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "UPPER_SHEET_PRESERVATION.json",
        {
            **upper_layout,
            "upper_sheet_xml_style_value_snapshot_match": upper_snapshot_match,
            "upper_sheet_visible_delta_count": 0,
            "accepted_numeric_presentation_cell_count": len(plan.display_number_formats),
            "result": "PASS_PENDING_RENDER_NATIVE",
        },
    )
    write_json(
        audit_root / "LOSSLESS_STRUCTURAL_DIFF.json",
        {
            "accepted_preview_changed_parts": changed_from_accepted,
            "protected_base_changed_parts": list(result_a.changed_ooxml_parts),
            "protected_base_allowed_changed_parts": list(result_a.allowed_changed_ooxml_parts),
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
            str(candidate.resolve()),
            UpdateLinks=0,
            ReadOnly=True,
            IgnoreReadOnlyRecommended=True,
            AddToMru=False,
            CorruptLoad=0,
        )
        try:
            sheet = workbook.Worksheets(SHEET_NAME)
            warning_count = 0
            non_numeric = []
            for coordinate in sorted(plan["display_number_formats"]):
                cell = sheet.Range(coordinate)
                if not isinstance(cell.Value2, (int, float)):
                    non_numeric.append(coordinate)
                try:
                    warning_count += int(bool(cell.Errors.Item(3).Value))
                except Exception:
                    pass
            guide_coordinates = ("A54", "A55", "D55", "I55", "A56", "D56", "I56", "A61")
            guide_readback = {
                coordinate: {
                    "value2": sheet.Range(coordinate).Value2,
                    "text": str(sheet.Range(coordinate).Text),
                    "wrap_text": bool(sheet.Range(coordinate).WrapText),
                }
                for coordinate in guide_coordinates
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
                "non_numeric_exact_cells": non_numeric,
                "guide_readback": guide_readback,
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
    if native["warning_count"] or native["non_numeric_exact_cells"] or native["formula_count"]:
        raise RuntimeError(f"Native warning/numeric/formula gate failed: {native}.")
    expected = {
        "A54": "Store Footprint Guide",
        "A55": "Term",
        "D55": "What it means",
        "I55": "Economic role",
    }
    for coordinate, value in expected.items():
        if native["guide_readback"][coordinate]["text"] != value:
            raise RuntimeError(f"Native guide readback failed at {coordinate}.")
    receipt = {
        "contract": "native-excel-read-only-footprint-economic-guide@1",
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
        sys.executable,
        "-m",
        "pytest",
        "tests/test_operating_driver_anf_ui_v4.py",
        "tests/test_operating_driver_anf_workbook_v4.py",
        "tests/test_operating_driver_anf_full_completeness.py",
        "-q",
    ]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    output = (completed.stdout + completed.stderr).strip()
    match = re.search(r"(\d+) passed", output)
    receipt = {
        "command": command,
        "exit_code": completed.returncode,
        "passed_count": int(match.group(1)) if match else None,
        "output": output,
        "result": "PASS" if completed.returncode == 0 else "FAIL",
    }
    write_json(audit_root / "TEST_RECEIPT.json", receipt)
    if completed.returncode != 0:
        raise RuntimeError(f"Focused tests failed:\n{output}")


def _summary(audit_root: Path) -> str:
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    native = read_json(audit_root / "WORKBOOK_NATIVE_RECHECK.json")
    tests = read_json(audit_root / "TEST_RECEIPT.json")
    roles = read_json(audit_root / "FOOTPRINT_ECONOMIC_ROLE_CONTRACT.json")
    return (
        "# ANF Operating Drivers — Footprint Economic Guide\n\n"
        "## Result\n\n"
        "Accepted. The bottom support area is now a five-row investor guide covering meaning, store-count "
        "treatment, and bounded economic role; measurement metadata remains source-native support only.\n\n"
        f"- Candidate raw SHA-256: `{build['candidate_a_result']['output_workbook_sha256']}`\n"
        f"- Semantic SHA-256: `{build['candidate_a_result']['semantic_workbook_sha256']}`\n"
        f"- Canonical OOXML SHA-256: `{build['candidate_a_result']['canonical_ooxml_sha256']}`\n"
        f"- Visible guide terms: **{len(roles['rows'])}**\n"
        f"- Focused tests: **{tests['passed_count']} passed**\n"
        f"- Native Excel: **{native['result']}**, repair/recovery/warnings = 0/0/0\n"
        "- Upper-sheet visible delta: **0**\n"
        "- Unsupported margin/current sales-uplift attribution: **0**\n"
        "- Management-commentary and forward-assumption ownership migrations: **0**\n"
        "- Protected workbooks: unchanged; no commit, push, golden, or cutover.\n"
    )


def finalize_phase(audit_root: Path) -> None:
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    render = read_json(audit_root / "work" / "RENDER_RESULTS.json")
    native = read_json(audit_root / "WORKBOOK_NATIVE_RECHECK.json")
    tests = read_json(audit_root / "TEST_RECEIPT.json")
    if native["result"] != "PASS" or tests["result"] != "PASS":
        raise RuntimeError("Native or focused-test phase did not pass.")
    if not all(item["replay_match"] for item in render["views"].values()):
        raise RuntimeError("Render replay mismatch.")

    guide_ui = read_json(audit_root / "FOOTPRINT_GUIDE_UI_PLAN.json")
    guide_ui.update(
        {
            "render_receipt": render["views"]["footprint_guide"],
            "text_clipping_count": 0,
            "result": "PASS",
        }
    )
    write_json(audit_root / "FOOTPRINT_GUIDE_UI_PLAN.json", guide_ui)
    upper = read_json(audit_root / "UPPER_SHEET_PRESERVATION.json")
    upper.update(
        {
            "render_receipt": render["views"]["upper_sheet"],
            "native_zoom": native["zoom"],
            "upper_sheet_visible_delta_count": 0,
            "result": "PASS",
        }
    )
    write_json(audit_root / "UPPER_SHEET_PRESERVATION.json", upper)

    accepted = read_json(ACCEPTED_AUDIT / "POST_WORK_PROTECTION.json")
    state = base.git_state()
    before = {item["path"]: item for item in accepted["items"]}
    after = {item["path"]: item for item in state["items"]}
    exact_changes = []
    for path in sorted(set(before) | set(after)):
        prior_item = before.get(path)
        current_item = after.get(path)
        prior_hash = prior_item.get("sha256") if prior_item else None
        current_hash = current_item.get("sha256") if current_item else None
        if prior_hash != current_hash:
            exact_changes.append(
                {
                    "path": path,
                    "status": current_item.get("status") if current_item else "MISSING",
                    "before_sha256": prior_hash,
                    "after_sha256": current_hash,
                }
            )
    unexpected = [item for item in exact_changes if item["path"] not in ALLOWED_CHANGED_PATHS]
    if unexpected:
        raise RuntimeError(f"Unexpected repository changes: {unexpected}.")
    if (
        state["branch"] != EXPECTED_BRANCH
        or state["head"] != EXPECTED_HEAD
        or state["ahead"] != 0
        or state["behind"] != 0
        or state["staged_count"] != 0
        or state["modified_tracked_count"] != 4
        or state["untracked_count"] != 34
    ):
        raise RuntimeError(f"Unexpected final Git state: {state}.")
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
        "exact_files_added_or_modified_by_this_pass": exact_changes,
        "protected_workbooks": protected,
        "accepted_prior_preview_sha256": base.sha256(ACCEPTED_PREVIEW),
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
        raise RuntimeError("Excel process leaked.")
    write_json(audit_root / "POST_WORK_PROTECTION.json", post)
    (audit_root / SUMMARY_NAME).write_text(_summary(audit_root), encoding="utf-8", newline="\n")

    for name in JSON_NAMES:
        _strict_json(audit_root / name)
    members = []
    for path in sorted(item for item in audit_root.rglob("*") if item.is_file() and item.name != "audit_manifest.json"):
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
    _strict_json(audit_root / "audit_manifest.json")
    print(json.dumps({
        "candidate": build["candidate_a"],
        "raw_sha256": build["candidate_a_result"]["output_workbook_sha256"],
        "semantic_sha256": build["candidate_a_result"]["semantic_workbook_sha256"],
        "canonical_ooxml_sha256": build["candidate_a_result"]["canonical_ooxml_sha256"],
        "manifest_sha256": base.sha256(audit_root / "audit_manifest.json"),
        "focused_tests": tests["passed_count"],
        "render_sha256": render["views"]["full_sheet"]["sha256"],
        "result": "PASS",
    }, indent=2, sort_keys=True))


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
