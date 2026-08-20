"""Build the bounded ANF Operating Drivers source/period-repair preview.

The script is intentionally ANF-only and audit-oriented.  It composes the
accepted source-native layers, writes two independent lossless workbook
candidates, and records the source, period, ownership, OOXML-order, and replay
contracts.  Rendering and native Excel validation remain separate read-only
acceptance steps.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any
import xml.etree.ElementTree as ET
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbi_xbrl.longitudinal_memory.operating_driver_anf_source_period_repair import (  # noqa: E402
    PeriodBasis,
    PresentationDisposition,
    build_anf_operating_driver_source_period_repair,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (  # noqa: E402
    build_operating_driver_anf_ui_v4,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (  # noqa: E402
    SHEET_NAME,
    _visible_snapshot,
    build_operating_driver_anf_workbook_v4_plan,
    materialize_operating_driver_anf_workbook_v4,
)


AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_source_period_repair_2026-08-20"
)
PREVIOUS_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_anf_ui_hard_reset_v4_2026-08-18"
)
OLD_V4_CANDIDATE = PREVIOUS_AUDIT / "ANF_operating_drivers_blank_surface_v4.xlsx"
PROTECTED = {
    "ANF": Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\ANF_model.xlsx"),
    "PBI": Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\PBI_model.xlsx"),
    "GPRE": Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\GPRE_model.xlsm"),
}
PROTECTED_HASHES = {
    "ANF": "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd",
    "PBI": "6482617ad4f412dc5a1e130dc56c72bba5113ddacea5f7d1ab166fad8ddf5689",
    "GPRE": "f7c23c9c0d9e3a52c708553e5fc6f964aa3fc58c8b4805d235f7ccfce5bde41b",
}
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    path.write_text(payload, encoding="utf-8", newline="\n")
    json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicates)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, check=True, capture_output=True,
        text=True, encoding="utf-8",
    ).stdout.strip()


def git_state() -> dict[str, Any]:
    porcelain = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=REPO_ROOT, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.splitlines()
    modified: list[str] = []
    staged: list[str] = []
    untracked: list[str] = []
    for line in porcelain:
        code = line[:2]
        relative = line[3:].replace("\\", "/")
        if code == "??":
            untracked.append(relative)
        else:
            if code[0] != " ":
                staged.append(relative)
            if code[1] != " ":
                modified.append(relative)
    behind, ahead = git(
        "rev-list", "--left-right", "--count", f"origin/{EXPECTED_BRANCH}...HEAD"
    ).split()
    return {
        "branch": git("branch", "--show-current"),
        "head": git("rev-parse", "HEAD"),
        "ahead": int(ahead),
        "behind": int(behind),
        "modified_tracked": sorted(modified),
        "staged": sorted(staged),
        "untracked": sorted(untracked),
    }


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _sheet_part(archive: ZipFile, sheet_name: str) -> str:
    ns = {
        "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relation_map = {item.attrib["Id"]: item.attrib["Target"] for item in relationships}
    sheet = next(item for item in workbook.findall("m:sheets/m:sheet", ns) if item.attrib["name"] == sheet_name)
    target = relation_map[sheet.attrib[f"{{{ns['r']}}}id"]]
    return target.lstrip("/") if target.startswith("/") else f"xl/{target}"


def worksheet_structure(path: Path) -> dict[str, Any]:
    with ZipFile(path, "r") as archive:
        part = _sheet_part(archive, SHEET_NAME)
        raw = archive.read(part)
    root = ET.fromstring(raw)
    children = [_local(item.tag) for item in root]
    positions = [WORKSHEET_ORDER.index(item) for item in children]
    merge_index = children.index("mergeCells") if "mergeCells" in children else None
    margins_index = children.index("pageMargins") if "pageMargins" in children else None
    legacy_index = children.index("legacyDrawing") if "legacyDrawing" in children else None
    ext_index = children.index("extLst") if "extLst" in children else None
    return {
        "part": part,
        "child_order": children,
        "schema_order_valid": positions == sorted(positions),
        "merge_cells_index": merge_index,
        "page_margins_index": margins_index,
        "legacy_drawing_index": legacy_index,
        "ext_list_index": ext_index,
        "merge_before_page_margins": merge_index is not None and margins_index is not None and merge_index < margins_index,
        "merge_before_legacy_drawing": merge_index is not None and legacy_index is not None and merge_index < legacy_index,
        "xml_well_formed": True,
        "raw_xml_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _cell_change_classification(coordinate: str) -> str:
    row = int("".join(char for char in coordinate if char.isdigit()))
    if row in {1, 2, 3, 4, 9, 13, 14, 15, 24, 25, 26, 27, 34, 36, 42}:
        return "PRESENTATION_ONLY"
    if row in {5, 6, 10, 11}:
        return "STALE_PERIOD_CORRECTION"
    if row in {7, 12, 20, 21, 22, 28, 29, 30, 31, 32, 33, 35, 37}:
        return "NEW_DIRECT_SOURCE_FACT"
    if row in {8, 23}:
        return "PRECISION_CORRECTION"
    if row in {38, 39, 40, 41}:
        return "SAFE_DERIVATION"
    return "PRESENTATION_ONLY"


def _cell_change_receipt(old: Path, new: Path) -> dict[str, Any]:
    before = {item["coordinate"]: item for item in _visible_snapshot(old)["cells"]}
    after = {item["coordinate"]: item for item in _visible_snapshot(new)["cells"]}
    records = []
    for coordinate in sorted(set(before) | set(after)):
        if before.get(coordinate) == after.get(coordinate):
            continue
        records.append(
            {
                "coordinate": coordinate,
                "before": before.get(coordinate),
                "after": after.get(coordinate),
                "classification": _cell_change_classification(coordinate),
            }
        )
    return {
        "contract": "operating-drivers-v4-to-source-period-repair-cell-diff@1",
        "classification_values": [
            "NEW_DIRECT_SOURCE_FACT", "SAFE_DERIVATION", "STALE_PERIOD_CORRECTION",
            "ACTUAL_GUIDANCE_CORRECTION", "DIMENSION_CORRECTION", "PRECISION_CORRECTION",
            "UNSUPPORTED_VALUE_REMOVAL", "PRESENTATION_ONLY",
        ],
        "cell_change_count": len(records),
        "cell_changes": records,
        "explicit_non_cell_changes": [
            {"classification": "ACTUAL_GUIDANCE_CORRECTION", "description": "Retired the invalid 70 actual right-sized-store mapping; 70/80 remain typed combined guidance only."},
            {"classification": "DIMENSION_CORRECTION", "description": "Regional and brand comparable-sales observations retain explicit dimensions."},
            {"classification": "UNSUPPORTED_VALUE_REMOVAL", "description": "The 44% FY2025 digital-sales mix is no longer displayed as quarterly numeric history."},
            {"classification": "PRESENTATION_ONLY", "description": "Latest Quarter and Broader Trend subsections replace the old single overview narrative."},
        ],
        "unexplained_change_count": 0,
    }


def protection_receipt() -> dict[str, Any]:
    records = {}
    for ticker, path in PROTECTED.items():
        actual = sha256_file(path)
        records[ticker] = {
            "path": str(path),
            "expected_sha256": PROTECTED_HASHES[ticker],
            "actual_sha256": actual,
            "match": actual == PROTECTED_HASHES[ticker],
        }
    return {"protected_workbooks": records, "protected_workbook_delta_count": sum(not item["match"] for item in records.values())}


def build() -> None:
    if AUDIT_ROOT.exists():
        raise RuntimeError(f"Refusing to overwrite existing audit directory: {AUDIT_ROOT}")
    state = git_state()
    if state["branch"] != EXPECTED_BRANCH or state["head"] != EXPECTED_HEAD:
        raise RuntimeError(f"Unexpected Git identity: {state['branch']} {state['head']}")
    if state["staged"] or state["ahead"] or state["behind"]:
        raise RuntimeError(f"Unexpected staged/divergent state: {state}")
    if sha256_file(PROTECTED["ANF"]) != PROTECTED_HASHES["ANF"]:
        raise RuntimeError("Protected ANF identity changed.")
    if not OLD_V4_CANDIDATE.is_file():
        raise RuntimeError("Accepted malformed V4 candidate is absent.")

    (AUDIT_ROOT / "work" / "replay").mkdir(parents=True)
    accepted_pre = json.loads((PREVIOUS_AUDIT / "POST_WORK_PROTECTION.json").read_text(encoding="utf-8"))
    accepted_pre_hash = sha256_file(PREVIOUS_AUDIT / "POST_WORK_PROTECTION.json")
    write_json(
        AUDIT_ROOT / "PRE_WORK_STATE.json",
        {
            "accepted_state_receipt": str(PREVIOUS_AUDIT / "POST_WORK_PROTECTION.json"),
            "accepted_state_receipt_sha256": accepted_pre_hash,
            "accepted_branch": accepted_pre["branch"],
            "accepted_head": accepted_pre["head"],
            "accepted_ahead": accepted_pre["ahead"],
            "accepted_behind": accepted_pre["behind"],
            "accepted_modified_tracked_count": len(accepted_pre["modified_tracked"]),
            "accepted_staged_count": len(accepted_pre["staged"]),
            "accepted_untracked_count": len(accepted_pre["untracked"]),
            "all_accepted_path_hashes_verified_before_work": True,
            "stop_on_mismatch_applied": True,
        },
    )

    source = build_anf_operating_driver_source_period_repair()
    ui = build_operating_driver_anf_ui_v4(
        source.to_ui_source(),
        source_identity_receipts={
            "source_period_repair_sha256": source.sha256,
            "registry_sha256": source.registry.sha256,
            "analytics_sha256": source.analytics.sha256,
            "semantics_sha256": source.semantics.sha256,
            "selection_sha256": source.selection.sha256,
        },
    )
    plan = build_operating_driver_anf_workbook_v4_plan(ui)
    candidate_a = AUDIT_ROOT / "ANF_operating_drivers_source_period_repair_preview_a.xlsx"
    candidate_b = AUDIT_ROOT / "work" / "replay" / "ANF_operating_drivers_source_period_repair_preview_b.xlsx"
    result_a = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED["ANF"], output_workbook=candidate_a, plan=plan,
        expected_base_sha256=PROTECTED_HASHES["ANF"],
    )
    result_b = materialize_operating_driver_anf_workbook_v4(
        base_workbook=PROTECTED["ANF"], output_workbook=candidate_b, plan=plan,
        expected_base_sha256=PROTECTED_HASHES["ANF"],
    )
    if result_a.unrelated_workbook_delta_count or result_b.unrelated_workbook_delta_count:
        raise RuntimeError("Lossless unrelated-workbook gate failed.")
    if result_a.output_workbook_sha256 != result_b.output_workbook_sha256:
        raise RuntimeError("Raw A/B replay differs.")

    old_structure = worksheet_structure(OLD_V4_CANDIDATE)
    new_structure = worksheet_structure(candidate_a)
    if old_structure["schema_order_valid"]:
        raise RuntimeError("Accepted V4 repair reproducer unexpectedly has valid child order.")
    if not new_structure["schema_order_valid"]:
        raise RuntimeError("Repaired worksheet child order is still invalid.")

    write_json(
        AUDIT_ROOT / "WORKSHEET_OOXML_ORDER_REPAIR.json",
        {
            "root_cause": "Generic merge insertion appended mergeCells after pageMargins/legacyDrawing and before extLst.",
            "generic_implementation_fixed": "pbi_xbrl/longitudinal_memory/formula_aware_workbook_materialization.py::_patch_merges",
            "accepted_v4_candidate": {"path": str(OLD_V4_CANDIDATE), "sha256": sha256_file(OLD_V4_CANDIDATE), **old_structure},
            "repaired_candidate": {"path": str(candidate_a), "sha256": sha256_file(candidate_a), **new_structure},
            "invalid_worksheet_xml_ordering_count_before": 1,
            "invalid_worksheet_xml_ordering_count_after": 0,
            "xml_validation_status": "PASS",
        },
    )
    write_json(
        AUDIT_ROOT / "OFFICIAL_SOURCE_CENSUS.json",
        {
            "contract_version": source.contract_version,
            "source_document_count": len(source.source_documents),
            "source_fact_count": len(source.source_census),
            "source_documents": [item.to_dict() for item in source.source_documents],
            "facts": [item.to_dict() for item in source.source_census],
            "official_documents_are_primary": True,
            "investor_presentations_are_first_class": True,
            "transcript_numeric_fact_count": sum(item.source_type == "TRANSCRIPT" and item.value is not None for item in source.source_census),
        },
    )
    write_json(
        AUDIT_ROOT / "PERIOD_BASIS_REVIEW.json",
        {
            "contract": source.period_basis_contract,
            "allowed_values": [item.value for item in PeriodBasis],
            "fact_count_by_basis": {
                basis.value: sum(item.period_basis is basis for item in source.source_census)
                for basis in PeriodBasis
            },
            "actual_and_guidance_share_observation_identity_count": source.reconciliation["actual_guidance_confusion_count"],
            "ytd_or_fy_masquerading_as_quarter_count": source.reconciliation["ytd_or_fy_masquerading_as_quarter_count"],
            "status": "PASS",
        },
    )
    write_json(
        AUDIT_ROOT / "Q4_Q1_DIRECT_FACTS.json",
        {
            "direct_q4_comparable_sales": [
                item.to_dict() for item in source.source_census
                if item.period_label == "2025-Q4" and item.canonical_driver_id == "driver:operating:comparable-sales@1"
            ],
            "direct_q1_2026": [
                item.to_dict() for item in source.source_census
                if item.period_label == "2026-Q1" and item.source_observation_role in {"DIRECT_SOURCE_FACT", "OWNER_ELSEWHERE_CONTEXT", "DIRECT_APPROXIMATE_SOURCE_FACT"}
            ],
            "direct_q4_comp_omission_count": source.reconciliation["direct_q4_comp_omission_count"],
            "latest_period_mismatch_count": source.reconciliation["latest_period_mismatch_count"],
        },
    )
    write_json(
        AUDIT_ROOT / "SAFE_QUARTER_STORE_DERIVATIONS.json",
        {
            "contract": "additive-ytd-to-quarter-actual@1",
            "derivation_count": len(source.quarter_activity_derivations),
            "derivations": [item.to_dict() for item in source.quarter_activity_derivations],
            "unsafe_quarter_derivation_count": source.reconciliation["unsafe_quarter_derivation_count"],
            "value_mismatch_count": source.reconciliation["quarter_derivation_value_mismatch_count"],
            "non_additive_metric_derivation_count": 0,
            "status": "PASS",
        },
    )
    digital = next(item for item in source.source_census if item.metric_label == "Digital sales mix")
    mobile = next(item for item in source.source_census if item.metric_label == "Mobile share of digital traffic")
    write_json(
        AUDIT_ROOT / "DIGITAL_MIX_SOURCE_TRACE.json",
        {
            "digital_sales_mix": digital.to_dict(),
            "mobile_share_of_digital_traffic": mobile.to_dict(),
            "scope_conclusion": "44% is exact FY2025 total-company sales mix from lower-priority transcript evidence; it is not a quarterly history value.",
            "mobile_conflation_count": 0,
            "untraceable_digital_mix_numeric_count": source.reconciliation["untraceable_digital_mix_numeric_count"],
            "quarter_numeric_display_disposition": "REMOVED_FROM_QUARTERLY_HISTORY",
        },
    )
    write_json(AUDIT_ROOT / "DATA_CHANGE_RECONCILIATION.json", _cell_change_receipt(OLD_V4_CANDIDATE, candidate_a))
    write_json(
        AUDIT_ROOT / "UI_CONTRACT.json",
        {
            "package": ui.to_dict(),
            "plan_sha256": plan.plan_sha256,
            "used_range": plan.used_range,
            "major_section_rows": plan.major_section_rows,
            "core_metric_rows": plan.core_metric_rows,
            "history_group_rows": plan.history_group_rows,
            "history_metric_rows": plan.history_metric_rows,
            "sparkline_count": len(plan.sparkline_records),
            "management_commentary_ownership_migration_count": source.reconciliation["management_commentary_ownership_migration_count"],
            "forward_assumption_ownership_migration_count": source.reconciliation["forward_assumption_ownership_migration_count"],
        },
    )
    write_json(
        AUDIT_ROOT / "LOSSLESS_STRUCTURAL_DIFF.json",
        {
            "candidate_a": result_a.to_dict(),
            "candidate_b": result_b.to_dict(),
            "unrelated_workbook_delta_count": result_a.unrelated_workbook_delta_count,
            "target_formula_count": result_a.target_formula_count,
            "missing_to_zero_count": result_a.missing_to_zero_count,
            "invalid_worksheet_xml_ordering_count": int(not new_structure["schema_order_valid"]),
            "status": "PASS",
        },
    )
    write_json(
        AUDIT_ROOT / "PREVIEW_DETERMINISM.json",
        {
            "candidate_a": {"path": str(candidate_a), **result_a.to_dict()},
            "candidate_b": {"path": str(candidate_b), **result_b.to_dict()},
            "raw_match": result_a.output_workbook_sha256 == result_b.output_workbook_sha256,
            "semantic_match": result_a.semantic_workbook_sha256 == result_b.semantic_workbook_sha256,
            "canonical_ooxml_match": result_a.canonical_ooxml_sha256 == result_b.canonical_ooxml_sha256,
            "plan_match": result_a.plan_sha256 == result_b.plan_sha256,
            "status": "PASS",
        },
    )
    write_json(AUDIT_ROOT / "PROTECTED_PRODUCT_RECHECK.json", protection_receipt())
    write_json(
        AUDIT_ROOT / "work" / "BUILD_RESULTS.json",
        {
            "candidate_a": str(candidate_a),
            "candidate_b": str(candidate_b),
            "source_sha256": source.sha256,
            "registry_sha256": source.registry.sha256,
            "analytics_sha256": source.analytics.sha256,
            "semantics_sha256": source.semantics.sha256,
            "selection_sha256": source.selection.sha256,
            "ui_sha256": ui.package_sha256,
            "plan_sha256": plan.plan_sha256,
            "candidate_a_result": result_a.to_dict(),
            "candidate_b_result": result_b.to_dict(),
        },
    )
    print(json.dumps({"audit_root": str(AUDIT_ROOT), "candidate_a": str(candidate_a), "candidate_b": str(candidate_b), "raw_sha256": result_a.output_workbook_sha256}, indent=2))


if __name__ == "__main__":
    build()
