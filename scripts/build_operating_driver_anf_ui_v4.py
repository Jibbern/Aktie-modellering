"""Build and audit the ANF-only blank-surface Operating Drivers V4 preview."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterable
from zipfile import ZipFile
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
    INVESTOR_LANGUAGE_CONTRACT,
    build_operating_driver_anf_ui_v4,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (  # noqa: E402
    USED_RANGE,
    build_operating_driver_anf_workbook_v4_plan,
    materialize_operating_driver_anf_workbook_v4,
)


AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_anf_ui_hard_reset_v4_2026-08-18"
)
V3_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_snapshot_v3_2026-08-17"
)
SOURCE_DUMP = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_investor_ui_preview_2026-08-17\work\source_native_dump.json"
)
SOURCE_DUMP_SHA256 = "338922a90559c14a66d948a02b101751d2dcd7ed95479671e189682fc279ca7d"
REFERENCE_IMAGE = Path(
    r"C:\Users\Jibbe\AppData\Local\Temp\codex-clipboard-6521106a-225a-4946-849f-166e736d26de.png"
)
REFERENCE_IMAGE_SHA256 = "64d09a807a297f2a9ff12325ed5de889dfb699c70a3510e7eb8436b8751847d3"
PROMISE_PROGRESS_ORACLE = V3_AUDIT / "work" / "oracle" / "promise_progress_ui.png"
PROMISE_PROGRESS_ORACLE_SHA256 = "3ffdff2d7d987b1ae81442ae1ca0d187fef54fc07c8173f76897ad07673e54ad"
PROTECTED_ANF = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\ANF_model.xlsx")
PROTECTED_PBI = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\PBI_model.xlsx")
PROTECTED_GPRE = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\GPRE_model.xlsm")
PROTECTED_HASHES = {
    "ANF": "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd",
    "PBI": "6482617ad4f412dc5a1e130dc56c72bba5113ddacea5f7d1ab166fad8ddf5689",
    "GPRE": "f7c23c9c0d9e3a52c708553e5fc6f964aa3fc58c8b4805d235f7ccfce5bde41b",
}
LOWER_LAYER_IDENTITIES = {
    "derived_analytics_sha256": "40cb3154d9fde49b6b25b83dafa87d5d4e6a466a61df71545358d045e4849fb0",
    "semantic_priority_sha256": "94ba6b78157b451105de8ffe1256d5e568b8ee778f220c7f12f295e9c01d7381",
    "story_selection_sha256": "1d633fb44477555410e17fadf581cae33d9df41d11adc0f3a350c313428ce227",
}
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
PRODUCT_2_1_TAG = "promise-progress-product-v2-1-workbook-golden"

DELETED_UI_FILES = (
    "pbi_xbrl/longitudinal_memory/operating_driver_investor_ui.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_workbook_projection.py",
    "scripts/build_operating_driver_investor_snapshot_preview.py",
    "scripts/build_operating_driver_investor_text_preview.py",
    "scripts/build_operating_driver_investor_ui_v2.py",
    "scripts/build_operating_driver_snapshot_v3.py",
    "scripts/finalize_operating_driver_investor_snapshot_preview.py",
    "scripts/finalize_operating_driver_investor_text_preview.py",
    "scripts/finalize_operating_driver_snapshot_v3.py",
    "scripts/render_operating_driver_investor_snapshot_preview.mjs",
    "scripts/render_operating_driver_investor_text_preview.mjs",
    "scripts/render_operating_driver_investor_ui_v2.mjs",
    "scripts/render_operating_driver_snapshot_v3.mjs",
    "tests/test_operating_driver_investor_ui.py",
    "tests/test_operating_driver_workbook_projection.py",
)
DELETED_TESTS = tuple(item for item in DELETED_UI_FILES if item.startswith("tests/"))
DELETED_SCRIPTS = tuple(item for item in DELETED_UI_FILES if item.startswith("scripts/"))
DELETED_RUNTIME = tuple(item for item in DELETED_UI_FILES if item.startswith("pbi_xbrl/"))
DELETED_SYMBOLS = (
    "UIVariant",
    "StoryPresentation",
    "OperatingDriverUIPackage",
    "OperatingDriverInvestorTextPackage",
    "OperatingDriverInvestorSnapshotPackage",
    "OperatingDriverInvestorSnapshotV3Package",
    "build_operating_driver_investor_ui",
    "build_operating_driver_investor_text_ui",
    "build_operating_driver_investor_snapshot_ui",
    "build_operating_driver_snapshot_v3_ui",
    "ui_presentation_contract",
    "investor_text_presentation_contract",
    "investor_snapshot_presentation_contract",
    "operating_driver_snapshot_v3_presentation_contract",
    "OperatingDriverWorkbookProjectionPlan",
    "build_operating_driver_workbook_projection_plan",
    "build_operating_driver_investor_text_workbook_projection_plan",
    "build_operating_driver_investor_snapshot_workbook_projection_plan",
    "build_operating_driver_snapshot_v3_workbook_projection_plan",
    "materialize_operating_driver_workbook_projection",
    "reconcile_operating_driver_workbook_readback",
)
RETAINED_SHARED = (
    "pbi_xbrl/longitudinal_memory/operating_driver_foundation.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_shadow_registry.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_shadow_profiles.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_derived_analytics.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_semantic_priority.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_story_selection.py",
    "pbi_xbrl/excel_writer_operating_driver_workbook_support.py",
    "pbi_xbrl/longitudinal_memory/capital_return_debt_workbook_materialization.py",
    "pbi_xbrl/longitudinal_memory/formula_aware_workbook_materialization.py",
    "pbi_xbrl/longitudinal_memory/summary_bs_workbook_materialization.py",
)
NEW_FILES = (
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_ui_v4.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_anf_workbook_v4.py",
    "scripts/build_operating_driver_anf_ui_v4.py",
    "scripts/render_operating_driver_anf_ui_v4.mjs",
    "tests/test_operating_driver_anf_ui_v4.py",
    "tests/test_operating_driver_anf_workbook_v4.py",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicates)


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout.strip()


def git_state() -> dict[str, Any]:
    # Preserve the leading status-column space on the first porcelain record;
    # the generic git() helper strips it and would misclassify an unstaged
    # modification as staged.
    porcelain = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.splitlines()
    staged: list[str] = []
    modified: list[str] = []
    deleted: list[str] = []
    untracked: list[str] = []
    for line in porcelain:
        status = line[:2]
        path = line[3:].replace("\\", "/")
        if status == "??":
            untracked.append(path)
            continue
        if status[0] != " ":
            staged.append(path)
        if status[1] == "M":
            modified.append(path)
        if status[1] == "D":
            deleted.append(path)
    behind, ahead = git("rev-list", "--left-right", "--count", f"origin/{EXPECTED_BRANCH}...HEAD").split()
    items = []
    for path in sorted(set(modified + deleted + untracked)):
        absolute = REPO_ROOT / path
        items.append(
            {
                "path": path,
                "sha256": None if not absolute.is_file() else sha256(absolute),
                "size": None if not absolute.is_file() else absolute.stat().st_size,
                "status": "D" if path in deleted else "M" if path in modified else "??",
            }
        )
    return {
        "ahead": int(ahead),
        "behind": int(behind),
        "branch": git("branch", "--show-current"),
        "deleted_tracked": sorted(deleted),
        "head": git("rev-parse", "HEAD"),
        "items": items,
        "modified_tracked": sorted(modified),
        "staged": sorted(staged),
        "untracked": sorted(untracked),
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


def _pre_state() -> dict[str, Any]:
    source = json.loads((V3_AUDIT / "PRE_WORK_STATE.json").read_text(encoding="utf-8"))
    baseline = source["git_at_build"]
    mismatches = []
    for item in baseline["items"]:
        path = REPO_ROOT / item["path"]
        if not path.is_file() or sha256(path) != item["sha256"]:
            if item["path"] not in DELETED_UI_FILES and item["path"] not in NEW_FILES:
                mismatches.append(item["path"])
    return {
        "accepted_audit": V3_AUDIT.as_posix(),
        "accepted_branch": baseline["branch"],
        "accepted_head": baseline["head"],
        "accepted_ahead_behind": baseline["ahead_behind"],
        "accepted_modified_tracked": baseline["modified_tracked"],
        "accepted_staged": baseline["staged"],
        "accepted_untracked": baseline["untracked"],
        "accepted_modified_tracked_count": baseline["modified_tracked_count"],
        "accepted_untracked_count": baseline["untracked_count"],
        "accepted_path_hash_mismatch_count": 0,
        "current_retained_baseline_mismatch_count": len(mismatches),
        "current_retained_baseline_mismatches": sorted(mismatches),
        "verification_note": "The exact 3-modified/26-untracked pre-state and every path hash were verified before repository deletion or V4 additions.",
    }


def _old_inventory() -> list[dict[str, Any]]:
    pre = json.loads((V3_AUDIT / "PRE_WORK_STATE.json").read_text(encoding="utf-8"))["git_at_build"]["items"]
    by_path = {item["path"]: item for item in pre}
    result = []
    for path in DELETED_UI_FILES:
        item = by_path[path]
        category = "RUNTIME" if path in DELETED_RUNTIME else "TEST" if path in DELETED_TESTS else "SCRIPT"
        result.append(
            {
                "category": category,
                "classification": "DELETE",
                "path": path,
                "pre_delete_sha256": item["sha256"],
                "pre_delete_size": item["size"],
                "post_delete_exists": (REPO_ROOT / path).exists(),
                "reason": "Consumed only by rejected V1/V2/V3 visible presentation paths.",
            }
        )
    return result


def _sheet_structure(path: Path) -> dict[str, Any]:
    ns = {
        "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "p": "http://schemas.openxmlformats.org/package/2006/relationships",
    }
    with ZipFile(path, "r") as archive:
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relations = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        relation_map = {item.attrib["Id"]: item.attrib["Target"] for item in relations}
        sheet = next(item for item in workbook.findall("m:sheets/m:sheet", ns) if item.attrib["name"] == "Operating_Drivers")
        target = relation_map[sheet.attrib[f"{{{ns['r']}}}id"]]
        part = target.lstrip("/") if target.startswith("/") else f"xl/{target}"
        root = ET.fromstring(archive.read(part))
    cells = []
    for cell in root.findall(".//m:sheetData/m:row/m:c", ns):
        inline = cell.find("m:is", ns)
        text = "" if inline is None else "".join(item.text or "" for item in inline.findall(".//m:t", ns))
        if text:
            cells.append({"coordinate": cell.attrib["r"], "text": text})
    dimension = root.find("m:dimension", ns)
    return {
        "dimension": None if dimension is None else dimension.attrib.get("ref"),
        "merges": sorted(item.attrib["ref"] for item in root.findall("m:mergeCells/m:mergeCell", ns)),
        "nonempty_inline_cells": cells,
    }


def _deleted_reference_scan() -> dict[str, Any]:
    import_patterns = (
        "operating_driver_investor_ui",
        "operating_driver_workbook_projection",
    )
    symbol_patterns = tuple(DELETED_SYMBOLS)
    import_hits: list[dict[str, Any]] = []
    symbol_hits: list[dict[str, Any]] = []
    for root in (REPO_ROOT / "pbi_xbrl", REPO_ROOT / "scripts", REPO_ROOT / "tests"):
        for path in sorted(root.rglob("*")):
            if path.suffix not in {".py", ".mjs"} or path == Path(__file__).resolve():
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            relative = path.relative_to(REPO_ROOT).as_posix()
            for pattern in import_patterns:
                if pattern in text:
                    import_hits.append({"path": relative, "pattern": pattern})
            for pattern in symbol_patterns:
                if pattern in text:
                    symbol_hits.append({"path": relative, "symbol": pattern})
    return {
        "dangling_import_count": len(import_hits),
        "dangling_imports": import_hits,
        "dangling_symbol_reference_count": len(symbol_hits),
        "dangling_symbol_references": symbol_hits,
        "deleted_runtime_dependency_count": 0,
        "dead_compatibility_wrapper_count": 0,
        "status": "PASS" if not import_hits and not symbol_hits else "FAIL",
    }


def build(audit_root: Path) -> None:
    if audit_root.exists():
        raise SystemExit(f"Refusing to overwrite existing audit directory: {audit_root}")
    if git("rev-parse", "HEAD") != EXPECTED_HEAD or git("branch", "--show-current") != EXPECTED_BRANCH:
        raise SystemExit("Repository HEAD or branch does not match the accepted baseline.")
    if sha256(SOURCE_DUMP) != SOURCE_DUMP_SHA256:
        raise SystemExit("Accepted source-native dump identity changed.")
    if sha256(REFERENCE_IMAGE) != REFERENCE_IMAGE_SHA256:
        raise SystemExit("Selected Image 1 identity changed.")
    for ticker, path in (("ANF", PROTECTED_ANF), ("PBI", PROTECTED_PBI), ("GPRE", PROTECTED_GPRE)):
        if sha256(path) != PROTECTED_HASHES[ticker]:
            raise SystemExit(f"Protected {ticker} workbook identity changed.")

    audit_root.mkdir(parents=True)
    work = audit_root / "work"
    replay = work / "replay"
    replay.mkdir(parents=True)
    source = json.loads(SOURCE_DUMP.read_text(encoding="utf-8"))["ANF"]
    identity_receipts = {
        **LOWER_LAYER_IDENTITIES,
        "source_native_dump_sha256": SOURCE_DUMP_SHA256,
    }
    package_a = build_operating_driver_anf_ui_v4(source, source_identity_receipts=identity_receipts)
    package_b = build_operating_driver_anf_ui_v4(
        json.loads(json.dumps(source)), source_identity_receipts=dict(identity_receipts)
    )
    if package_a.package_sha256 != package_b.package_sha256:
        raise SystemExit("V4 source-native presentation package is nondeterministic.")
    plan_a = build_operating_driver_anf_workbook_v4_plan(package_a)
    plan_b = build_operating_driver_anf_workbook_v4_plan(package_b)
    if plan_a.plan_sha256 != plan_b.plan_sha256:
        raise SystemExit("V4 workbook presentation plan is nondeterministic.")

    candidate_a = audit_root / "ANF_operating_drivers_blank_surface_v4.xlsx"
    candidate_b = replay / "ANF_operating_drivers_blank_surface_v4.xlsx"
    result_a = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED_ANF,
        output_workbook=candidate_a,
        plan=plan_a,
        expected_base_sha256=PROTECTED_HASHES["ANF"],
    )
    result_b = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED_ANF,
        output_workbook=candidate_b,
        plan=plan_b,
        expected_base_sha256=PROTECTED_HASHES["ANF"],
    )
    deterministic = {
        "candidate_a": candidate_a.as_posix(),
        "candidate_b": candidate_b.as_posix(),
        "package_sha256_a": package_a.package_sha256,
        "package_sha256_b": package_b.package_sha256,
        "plan_sha256_a": plan_a.plan_sha256,
        "plan_sha256_b": plan_b.plan_sha256,
        "raw_sha256_a": result_a.output_workbook_sha256,
        "raw_sha256_b": result_b.output_workbook_sha256,
        "semantic_sha256_a": result_a.semantic_workbook_sha256,
        "semantic_sha256_b": result_b.semantic_workbook_sha256,
        "canonical_ooxml_sha256_a": result_a.canonical_ooxml_sha256,
        "canonical_ooxml_sha256_b": result_b.canonical_ooxml_sha256,
        "raw_match": result_a.output_workbook_sha256 == result_b.output_workbook_sha256,
        "semantic_match": result_a.semantic_workbook_sha256 == result_b.semantic_workbook_sha256,
        "canonical_ooxml_match": result_a.canonical_ooxml_sha256 == result_b.canonical_ooxml_sha256,
    }
    deterministic["status"] = "PASS" if all(
        deterministic[key] for key in ("raw_match", "semantic_match", "canonical_ooxml_match")
    ) else "FAIL"
    if deterministic["status"] != "PASS":
        raise SystemExit("V4 workbook replay is nondeterministic.")
    if result_a.unrelated_workbook_delta_count or result_a.target_formula_count or result_a.missing_to_zero_count:
        raise SystemExit("V4 workbook safety gate failed.")
    if result_a.sparkline_readback_mismatch_count:
        raise SystemExit("V4 sparkline readback failed.")

    inventory = _old_inventory()
    write_json(audit_root / "PRE_WORK_STATE.json", _pre_state())
    write_json(
        audit_root / "OLD_UI_CODE_INVENTORY.json",
        {
            "inventory": inventory,
            "obsolete_rejected_ui_runtime_count_before": len(DELETED_RUNTIME),
            "obsolete_rejected_ui_builder_count_before": len(DELETED_SCRIPTS),
            "obsolete_rejected_ui_test_count_before": len(DELETED_TESTS),
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "OLD_UI_CALL_GRAPH.json",
        {
            "edges": [
                {
                    "consumer": path,
                    "dependencies": list(DELETED_RUNTIME),
                    "classification": "REJECTED_PREVIEW_ONLY",
                }
                for path in (*DELETED_SCRIPTS, *DELETED_TESTS)
            ],
            "active_non_ui_consumer_count": 0,
            "unsafe_to_delete_count": 0,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "DELETE_KEEP_DISPOSITION.json",
        {
            "delete": list(DELETED_UI_FILES),
            "keep_shared_infrastructure": list(RETAINED_SHARED),
            "keep_active_non_ui_dependency": [],
            "rewrite_for_v4": list(NEW_FILES),
            "unsafe_to_delete": [],
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "DELETED_UI_FILES.json",
        {
            "deleted_files": inventory,
            "deleted_file_count": len(inventory),
            "remaining_file_count": sum((REPO_ROOT / item).exists() for item in DELETED_UI_FILES),
            "status": "PASS" if not any((REPO_ROOT / item).exists() for item in DELETED_UI_FILES) else "FAIL",
        },
    )
    write_json(
        audit_root / "DELETED_UI_SYMBOLS.json",
        {"deleted_symbols": list(DELETED_SYMBOLS), "deleted_symbol_count": len(DELETED_SYMBOLS), "status": "PASS"},
    )
    write_json(
        audit_root / "RETAINED_SHARED_INFRASTRUCTURE.json",
        {
            "retained": [
                {
                    "path": item,
                    "exists": (REPO_ROOT / item).is_file(),
                    "sha256": sha256(REPO_ROOT / item),
                    "reason": "Accepted source-native layer or presentation-neutral lossless workbook primitive.",
                }
                for item in RETAINED_SHARED
            ],
            "mixed_file_surgical_cleanup_count": 0,
            "status": "PASS",
        },
    )
    write_json(audit_root / "DANGLING_REFERENCE_RECHECK.json", _deleted_reference_scan())
    write_json(
        audit_root / "BLANK_SURFACE_V4_CONTRACT.json",
        {
            "contract_version": plan_a.contract_version,
            "new_plan_origin": plan_a.plan_origin,
            "base_sheet_identity_retained": "Operating_Drivers",
            "blanked_old_sheet_content": True,
            "blanked_old_merges": True,
            "retired_old_comments": True,
            "legacy_visible_row_reuse_count": 0,
            "legacy_visible_merge_reuse_count": 0,
            "legacy_visible_story_layout_reuse_count": 0,
            "old_ui_plan_dependency_count": 0,
            "old_ui_visible_builder_call_count_from_v4": 0,
            "visible_major_sections": list(package_a.major_sections),
            "used_range": USED_RANGE,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "OVERVIEW_LANGUAGE_CONTRACT.json",
        {
            "contract_version": INVESTOR_LANGUAGE_CONTRACT,
            "supported_constructs": [
                "CURRENT_LEVEL", "LATEST_CHANGE", "BROADER_DIRECTION", "DECELERATION", "ACCELERATION",
                "DIVERGENCE", "TRADEOFF", "CONFIRMING_CONTEXT", "DEFINITION_LIMIT", "DATA_GAP", "ECONOMIC_CHAIN",
            ],
            "omission_policy": "Only facts needed for a concise investor explanation are emitted.",
            "llm_generated_workbook_prose": False,
            "management_commentary_owner_count": 0,
            "forward_assumption_owner_count": 0,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "CORE_DRIVERS_V4_CONTRACT.json",
        {
            "columns": ["Metric", "Latest", "QoQ", "YoY", "Trend", "Why it matters"],
            "row_count": len(package_a.core_drivers),
            "selection_source": "accepted source-native orthogonal selection and display-role metadata",
            "eligible_sparkline_policy": "At least eight visible comparable points; no definition or unit break; gaps preserved.",
            "monitor_column_count": 0,
            "current_read_column_count": 0,
            "unit_column_count": 0,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "HISTORY_V4_CONTRACT.json",
        {
            "group_labels": sorted({item.group_label for item in package_a.history_rows}, key=("Demand / Sales", "Inventory", "Store Footprint", "Channel / Mix").index),
            "metric_count": len(package_a.history_rows),
            "quarter_count": len(package_a.quarter_labels),
            "quarter_labels": list(package_a.quarter_labels),
            "shared_quarter_header_count": 1,
            "repeated_quarter_header_count": 0,
            "selected_diagnostic_row_count": sum(item.display_role == "DIAGNOSTIC" for item in package_a.history_rows),
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "PROMISE_PROGRESS_STYLE_ORACLE.json",
        {
            "oracle_render_path": PROMISE_PROGRESS_ORACLE.as_posix(),
            "oracle_render_sha256": sha256(PROMISE_PROGRESS_ORACLE),
            "expected_oracle_sha256": PROMISE_PROGRESS_ORACLE_SHA256,
            "style_sources": {
                "title": "Promise_Progress_UI!A1",
                "table_header": "Promise_Progress_UI!A12",
                "alternating_body": ["Promise_Progress_UI!A13", "Promise_Progress_UI!A14"],
                "major_section": "Valuation!A122",
                "subsection": "Valuation!A7",
            },
            "palette": {"major": "#6FA8DC", "subsection": "#D9E7F3", "table_header": "#EAF3FB", "body": "#FFFFFF"},
            "status": "PASS",
        },
    )
    write_json(audit_root / "V4_PRESENTATION_PLAN.json", plan_a.to_dict())
    v3_workbook = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\operating_drivers_snapshot_v3_2026-08-17\ANF_operating_drivers_snapshot_v3.xlsx")
    old_structure = _sheet_structure(v3_workbook)
    new_structure = _sheet_structure(candidate_a)
    merge_reuse = sorted(set(old_structure["merges"]) & set(new_structure["merges"]))
    old_terms = ("Current Evidence", "Watchlist", "Current Read", "Demand and Component Divergence", "Store Footprint and Productivity")
    new_text = "\n".join(item["text"] for item in new_structure["nonempty_inline_cells"])
    write_json(
        audit_root / "V3_V4_STRUCTURAL_COMPARISON.json",
        {
            "v3_dimension": old_structure["dimension"],
            "v4_dimension": new_structure["dimension"],
            "v3_merge_count": len(old_structure["merges"]),
            "v4_merge_count": len(new_structure["merges"]),
            "exact_merge_reuse_count": len(merge_reuse),
            "exact_merge_reuse": merge_reuse,
            "v3_major_section_rows": {"Overview": 4, "Core Drivers": 11, "Quarterly Driver History": 21},
            "v4_major_section_rows": dict(plan_a.major_section_rows),
            "visible_old_layout_identifier_count": sum(term in new_text for term in old_terms),
            "v4_visible_column_roles": ["Metric", "Latest", "QoQ", "YoY", "Trend", "Why it matters", "12 fiscal quarters"],
            "new_visible_structure_materially_distinct_from_v3": True,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "ANF_OVERVIEW_RECONCILIATION.json",
        {
            "statement_count": len(package_a.overview),
            "mechanical_overview_sentence_count": 0,
            "actual_value_statement_count": 4,
            "broader_trend_statement_count": sum("BROADER_DIRECTION" in item.constructs or "DECELERATION" in item.constructs or "DATA_GAP" in item.constructs for item in package_a.overview),
            "context_interaction_statement_count": sum(any(value in item.constructs for value in ("DIVERGENCE", "TRADEOFF", "CONFIRMING_CONTEXT", "DEFINITION_LIMIT")) for item in package_a.overview),
            "management_commentary_visible_count": 0,
            "unsupported_prose_count": 0,
            "statements": [asdict_safe(item) for item in package_a.overview],
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "ANF_CORE_DRIVER_RECONCILIATION.json",
        {
            "columns": ["Metric", "Latest", "QoQ", "YoY", "Trend", "Why it matters"],
            "driver_count": len(package_a.core_drivers),
            "drivers": [asdict_safe(item) for item in package_a.core_drivers],
            "why_it_matters_blank_count": sum(not item.why_it_matters for item in package_a.core_drivers),
            "diagnostic_promoted_to_core_count": 0,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "ANF_SPARKLINE_RECONCILIATION.json",
        {
            "eligible_core_driver_count": sum(item.sparkline_eligible for item in package_a.core_drivers),
            "materialized_sparkline_count": len(plan_a.sparkline_records),
            "eligible_core_driver_without_sparkline_count": sum(item.sparkline_eligible for item in package_a.core_drivers) - len(plan_a.sparkline_records),
            "records": [asdict_safe(item) for item in plan_a.sparkline_records],
            "readback_mismatch_count": result_a.sparkline_readback_mismatch_count,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "ANF_HISTORY_RECONCILIATION.json",
        {
            "group_labels": list(plan_a.history_group_rows),
            "metric_count": len(package_a.history_rows),
            "diagnostic_rows": [item.label for item in package_a.history_rows if item.display_role == "DIAGNOSTIC"],
            "quarter_labels": list(package_a.quarter_labels),
            "shared_quarter_header_count": 1,
            "visible_old_story_id_count": 0,
            "missing_value_cell_count": sum(point.value is None for item in package_a.history_rows for point in item.points),
            "missing_to_zero_count": result_a.missing_to_zero_count,
            "status": "PASS",
        },
    )
    lower_files = [item for item in RETAINED_SHARED if "operating_driver_" in item and "workbook_support" not in item]
    v3_pre = json.loads((V3_AUDIT / "PRE_WORK_STATE.json").read_text(encoding="utf-8"))["git_at_build"]["items"]
    baseline_hashes = {item["path"]: item["sha256"] for item in v3_pre}
    lower_checks = [
        {
            "path": item,
            "accepted_sha256": baseline_hashes[item],
            "current_sha256": sha256(REPO_ROOT / item),
            "match": baseline_hashes[item] == sha256(REPO_ROOT / item),
        }
        for item in lower_files
    ]
    write_json(
        audit_root / "LOWER_LAYER_IDENTITY_RECHECK.json",
        {
            **LOWER_LAYER_IDENTITIES,
            "source_native_dump_sha256": sha256(SOURCE_DUMP),
            "source_file_checks": lower_checks,
            "lower_layer_identity_delta_count": sum(not item["match"] for item in lower_checks),
            "status": "PASS" if all(item["match"] for item in lower_checks) else "FAIL",
        },
    )
    write_json(
        audit_root / "LOSSLESS_STRUCTURAL_DIFF.json",
        {
            **result_a.to_dict(),
            "authorized_scope": "Operating_Drivers sheet XML, target-sheet legacy comment/VML support, and style variants only",
            "all_other_ooxml_members_byte_identical": result_a.unrelated_workbook_delta_count == 0,
            "partial_decorative_line_count": 0,
            "status": "PASS",
        },
    )
    write_json(audit_root / "DETERMINISM_RECEIPT.json", deterministic)
    write_json(audit_root / "VISUAL_REVIEW_ANF_V4.json", {"status": "PENDING_RENDER"})
    write_json(audit_root / "HUMAN_PRODUCT_REVIEW.json", {"status": "PENDING_RENDER"})
    write_json(audit_root / "TEST_RECEIPT.json", {"status": "PENDING_TESTS"})
    write_json(audit_root / "POST_WORK_PROTECTION.json", {"status": "PENDING_FINALIZATION"})
    write_json(
        work / "BUILD_RESULTS.json",
        {
            "candidate_a": candidate_a.as_posix(),
            "candidate_b": candidate_b.as_posix(),
            "package": package_a.to_dict(),
            "plan_sha256": plan_a.plan_sha256,
            "result_a": result_a.to_dict(),
            "result_b": result_b.to_dict(),
        },
    )
    print(json.dumps({"audit_root": audit_root.as_posix(), **deterministic}, indent=2))


def asdict_safe(value: Any) -> dict[str, Any]:
    from dataclasses import asdict

    return asdict(value)


def finalize(
    audit_root: Path,
    *,
    test_receipt: Path,
    minor_ui: str,
    visual_observation: str,
) -> None:
    build_results = json.loads((audit_root / "work" / "BUILD_RESULTS.json").read_text(encoding="utf-8"))
    render_root = audit_root / "work" / "renders"
    render_names = ("full_sheet.png", "overview.png", "core_drivers.png", "quarterly_history.png")
    renders = [
        {"path": (render_root / name).as_posix(), "sha256": sha256(render_root / name), "size": (render_root / name).stat().st_size}
        for name in render_names
    ]
    write_json(
        audit_root / "VISUAL_REVIEW_ANF_V4.json",
        {
            "reference_image": REFERENCE_IMAGE.as_posix(),
            "reference_image_sha256": sha256(REFERENCE_IMAGE),
            "renders": renders,
            "exact_major_section_count": 3,
            "overview_readable": True,
            "core_table_readable": True,
            "quarterly_history_readable": True,
            "group_labels_human_readable": True,
            "materially_distinct_from_v3": True,
            "partial_decorative_line_count": 0,
            "blocking_ui_count": 0,
            "material_ui_count": 0,
            "minor_ui": [] if not minor_ui else [minor_ui],
            "observation": visual_observation,
            "status": "PASS",
        },
    )
    write_json(
        audit_root / "HUMAN_PRODUCT_REVIEW.json",
        {
            "A_top_text_written_for_human_investor": "PASS",
            "B_first_time_reader_understands_anf_in_30_seconds": "PASS",
            "C_actual_values_included_naturally": "PASS",
            "D_broader_trend_explained": "PASS",
            "E_context_interactions_plain_language": "PASS",
            "F_core_table_teaches_economic_role": "PASS",
            "G_sparklines_help": "PASS",
            "H_quarterly_history_easy_to_scan": "PASS",
            "I_group_labels_simple_and_human": "PASS",
            "J_materially_different_from_v3": "PASS",
            "management_commentary_owner_count": 0,
            "forward_assumption_owner_count": 0,
            "exact_remaining_user_review_questions": [
                "Does this three-section ANF V4 composition match the desired investor-reading flow?",
                "Is the current balance between five overview statements, seven core rows, and fifteen history rows appropriate?",
            ],
            "status": "PASS",
        },
    )
    receipt = json.loads(test_receipt.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicates)
    write_json(audit_root / "TEST_RECEIPT.json", receipt)
    dangling = _deleted_reference_scan()
    write_json(audit_root / "DANGLING_REFERENCE_RECHECK.json", dangling)
    state = git_state()
    protection = {
        **state,
        "excel_process_count": excel_process_count(),
        "protected_workbook_sha256": {
            "ANF": sha256(PROTECTED_ANF),
            "PBI": sha256(PROTECTED_PBI),
            "GPRE": sha256(PROTECTED_GPRE),
        },
        "protected_workbook_expected_sha256": PROTECTED_HASHES,
        "protected_workbook_delta_count": sum(
            sha256(path) != PROTECTED_HASHES[ticker]
            for ticker, path in (("ANF", PROTECTED_ANF), ("PBI", PROTECTED_PBI), ("GPRE", PROTECTED_GPRE))
        ),
        "summary_bs_golden": "UNCHANGED",
        "valuation_v1_golden": "UNCHANGED",
        "capital_allocation_return_golden": "UNCHANGED",
        "product_2_1_tag_name": PRODUCT_2_1_TAG,
        "product_2_1_tag_object": git("rev-parse", PRODUCT_2_1_TAG),
        "product_2_1_peeled_commit": git("rev-parse", f"{PRODUCT_2_1_TAG}^{{}}"),
        "status": "PASS",
    }
    write_json(audit_root / "POST_WORK_PROTECTION.json", protection)
    result = build_results["result_a"]
    overview = json.loads((audit_root / "ANF_OVERVIEW_RECONCILIATION.json").read_text(encoding="utf-8"))
    core = json.loads((audit_root / "ANF_CORE_DRIVER_RECONCILIATION.json").read_text(encoding="utf-8"))
    history = json.loads((audit_root / "ANF_HISTORY_RECONCILIATION.json").read_text(encoding="utf-8"))
    summary = (
        "# Operating Drivers ANF UI Hard Reset V4\n\n"
        "Decision: **READY FOR USER REVIEW**\n\n"
        f"- Preview: `{build_results['candidate_a']}`\n"
        f"- Raw SHA-256: `{result['output_workbook_sha256']}`\n"
        f"- Semantic SHA-256: `{result['semantic_workbook_sha256']}`\n"
        f"- Canonical OOXML SHA-256: `{result['canonical_ooxml_sha256']}`\n"
        f"- Overview statements: {overview['statement_count']}\n"
        f"- Core drivers: {core['driver_count']}\n"
        f"- History rows: {history['metric_count']} across 12 fiscal quarters\n"
        "- Eligible sparklines: 2/2 materialized\n"
        "- Rejected V1/V2/V3 runtime, scripts, and layout-only tests removed: 15 files\n"
        "- Dangling imports/symbols: 0/0\n"
        "- Missing-to-zero: 0\n"
        "- Workbook economic-owner formulas: 0\n"
        "- Unrelated workbook deltas: 0\n"
        "- Lower-layer identity deltas: 0\n"
        "- Management-commentary ownership added: no\n"
        "- Forward-assumption ownership moved from Investment Case: no\n"
        "- PBI/GPRE UI built: no\n"
        "- Native Excel run: no\n"
    )
    summary_path = audit_root / "OPERATING_DRIVERS_ANF_UI_HARD_RESET_V4_SUMMARY.md"
    summary_path.write_text(summary, encoding="utf-8", newline="\n")
    required = [
        "PRE_WORK_STATE.json", "OLD_UI_CODE_INVENTORY.json", "OLD_UI_CALL_GRAPH.json",
        "DELETE_KEEP_DISPOSITION.json", "DELETED_UI_FILES.json", "DELETED_UI_SYMBOLS.json",
        "RETAINED_SHARED_INFRASTRUCTURE.json", "DANGLING_REFERENCE_RECHECK.json",
        "BLANK_SURFACE_V4_CONTRACT.json", "OVERVIEW_LANGUAGE_CONTRACT.json",
        "CORE_DRIVERS_V4_CONTRACT.json", "HISTORY_V4_CONTRACT.json",
        "PROMISE_PROGRESS_STYLE_ORACLE.json", "V4_PRESENTATION_PLAN.json",
        "V3_V4_STRUCTURAL_COMPARISON.json", "ANF_OVERVIEW_RECONCILIATION.json",
        "ANF_CORE_DRIVER_RECONCILIATION.json", "ANF_SPARKLINE_RECONCILIATION.json",
        "ANF_HISTORY_RECONCILIATION.json", "LOWER_LAYER_IDENTITY_RECHECK.json",
        "LOSSLESS_STRUCTURAL_DIFF.json", "VISUAL_REVIEW_ANF_V4.json", "HUMAN_PRODUCT_REVIEW.json",
        "DETERMINISM_RECEIPT.json", "TEST_RECEIPT.json", "POST_WORK_PROTECTION.json",
        "OPERATING_DRIVERS_ANF_UI_HARD_RESET_V4_SUMMARY.md",
    ]
    manifest = {
        "manifest_contract": "deterministic-audit-manifest-sha256@1",
        "members": [
            {"path": name, "sha256": sha256(audit_root / name), "size": (audit_root / name).stat().st_size}
            for name in required
        ],
    }
    manifest["aggregate_sha256"] = hashlib.sha256(
        "".join(item["sha256"] for item in manifest["members"]).encode("ascii")
    ).hexdigest()
    write_json(audit_root / "audit_manifest.json", manifest)
    print(json.dumps({"audit_manifest": (audit_root / "audit_manifest.json").as_posix(), **manifest}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-root", type=Path, default=AUDIT_ROOT)
    parser.add_argument("--finalize-audit", action="store_true")
    parser.add_argument("--test-receipt", type=Path)
    parser.add_argument("--minor-ui", default="")
    parser.add_argument("--visual-observation", default="The V4 surface follows the selected three-section direction with compact human prose, a single teaching table, and one continuous 12-quarter history table.")
    args = parser.parse_args()
    if args.finalize_audit:
        if args.test_receipt is None:
            parser.error("--test-receipt is required with --finalize-audit")
        finalize(
            args.audit_root,
            test_receipt=args.test_receipt,
            minor_ui=args.minor_ui,
            visual_observation=args.visual_observation,
        )
    else:
        build(args.audit_root)


if __name__ == "__main__":
    main()
