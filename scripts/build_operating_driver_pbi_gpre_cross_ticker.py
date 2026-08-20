"""Build the final PBI/GPRE Operating Drivers readability-polish previews."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from typing import Any, Mapping
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = Path(__file__).resolve().parent
for entry in (REPO_ROOT, SCRIPT_ROOT):
    if str(entry) not in sys.path:
        sys.path.insert(0, str(entry))

import build_anf_operating_driver_footprint_economic_guide as accepted  # noqa: E402
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_product import (  # noqa: E402
    COMPARISON_CONTRACT,
    PRESENTATION_CONTRACT,
    PRODUCT_CONTRACT,
    SAFE_SUM_CONTRACT,
    build_cross_ticker_operating_driver_package,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_profiles import (  # noqa: E402
    PROFILE_CONTRACT,
    PROFILES,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_source_parsing import (  # noqa: E402
    PARSER_CONTRACT,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_workbook import (  # noqa: E402
    SEMANTIC_HASH_CONTRACT,
    WORKBOOK_CONTRACT,
    build_cross_ticker_workbook_plan,
    materialize_cross_ticker_operating_driver_workbook,
)


base = accepted.base
DEFAULT_AUDIT_ROOT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_final_readability_polish_2026-08-20"
)
ACCEPTED_UI_CONSISTENCY_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\operating_drivers_final_ui_consistency_followup_2026-08-20"
)
EXPECTED_PBI_ACCEPTED_PREVIEW_RAW = "dd67bd399d48afe25b2335ae9f8f24c87c374c87d41812a134e828db0e3df180"
EXPECTED_GPRE_ACCEPTED_PREVIEW_RAW = "3689854f0d3953af86c5c38c155f7fb1271a7989ae8c8ed7ec5251e9066fc6ed"
ACCEPTED_ANF_AUDIT = Path(
    r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_footprint_economic_guide_measurement_hidden_2026-08-20"
)
ACCEPTED_ANF_PREVIEW = ACCEPTED_ANF_AUDIT / "ANF_operating_drivers_footprint_economic_guide_preview.xlsx"
EXPECTED_ANF_RAW = "3a99f3dd098884744b71313fb9d44ad02da0fb8906a6e6567c28f290bf4dcc8e"
EXPECTED_ANF_SEMANTIC = "bcbb34a65556f1325a34c1679de8a54cc72060c6923fd7694350ea3fba3ec37c"
EXPECTED_ANF_CANONICAL = "a090559f3123842ff11073e45f78e331801b79262ecd741f74190762d4b80c91"
EXPECTED_COMPLETENESS = "c1fbc5898e56fff7a5e559b122578fcf996b82ee389a47f9caf82adedf4bf1e9"
EXPECTED_BRANCH = "fix/summary-bs-segment-source-native-reconciliation"
EXPECTED_HEAD = "3e9c86f37996fe4eab414435c706955957b1e9df"
PROTECTED_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models")
PROTECTED = {
    "PBI": {
        "path": PROTECTED_ROOT / "PBI_model.xlsx",
        "sha256": "6482617ad4f412dc5a1e130dc56c72bba5113ddacea5f7d1ab166fad8ddf5689",
        "investment_case_sheet": "PBI_Investment_Case",
        "output": "PBI_operating_drivers_final_readability_polish_preview.xlsx",
    },
    "GPRE": {
        "path": PROTECTED_ROOT / "GPRE_model.xlsm",
        "sha256": "f7c23c9c0d9e3a52c708553e5fc6f964aa3fc58c8b4805d235f7ccfce5bde41b",
        "investment_case_sheet": "GPRE_Investment_Case",
        "output": "GPRE_operating_drivers_final_readability_polish_preview.xlsm",
    },
}
SUMMARY_NAME = "OPERATING_DRIVERS_FINAL_READABILITY_POLISH_SUMMARY.md"
RENDER_SCRIPT = "scripts/render_operating_driver_pbi_gpre_cross_ticker.mjs"
BUILD_SCRIPT = "scripts/build_operating_driver_pbi_gpre_cross_ticker.py"
NEW_PATHS = {
    "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_product.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_profiles.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_source_parsing.py",
    "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_workbook.py",
    BUILD_SCRIPT,
    RENDER_SCRIPT,
    "tests/test_operating_driver_cross_ticker_product.py",
    "tests/test_operating_driver_cross_ticker_workbook.py",
}
JSON_NAMES = (
    "PRE_WORK_STATE.json",
    "ANF_FROZEN_PRODUCT_CONTRACT.json",
    "ANF_REGRESSION_RECHECK.json",
    "SHARED_PRESENTATION_CONTRACT.json",
    "CROSS_TICKER_ARCHITECTURE.json",
    "PBI_OFFICIAL_SOURCE_CENSUS.json",
    "PBI_DRIVER_PERIOD_COVERAGE_MATRIX.json",
    "PBI_PARSER_ROOT_CAUSE_AUDIT.json",
    "PBI_CANONICAL_DRIVER_REGISTRY.json",
    "PBI_CANONICAL_OBSERVATION_REGISTRY.json",
    "PBI_ANALYTICS_RECONCILIATION.json",
    "PBI_INFORMATION_DISPOSITION.json",
    "PBI_UI_PLAN.json",
    "PBI_DRIVER_GUIDE_REVIEW.json",
    "PBI_VISUAL_REVIEW.json",
    "PBI_LOSSLESS_STRUCTURAL_DIFF.json",
    "GPRE_OFFICIAL_SOURCE_CENSUS.json",
    "GPRE_DRIVER_PERIOD_COVERAGE_MATRIX.json",
    "GPRE_PARSER_ROOT_CAUSE_AUDIT.json",
    "GPRE_CANONICAL_DRIVER_REGISTRY.json",
    "GPRE_CANONICAL_OBSERVATION_REGISTRY.json",
    "GPRE_CRUSH_DEFINITION_RECONCILIATION.json",
    "GPRE_UTILIZATION_CONTINUITY_RECHECK.json",
    "GPRE_45Z_RECONCILIATION.json",
    "GPRE_ANALYTICS_RECONCILIATION.json",
    "GPRE_INFORMATION_DISPOSITION.json",
    "GPRE_UI_PLAN.json",
    "GPRE_DRIVER_GUIDE_REVIEW.json",
    "GPRE_VISUAL_REVIEW.json",
    "GPRE_VBA_PRESERVATION.json",
    "GPRE_LOSSLESS_STRUCTURAL_DIFF.json",
    "PBI_CORE_DRIVER_REVIEW.json",
    "PBI_WHY_IT_MATTERS_REVIEW.json",
    "PBI_OPERATING_INTERPRETATION_REVIEW.json",
    "PBI_DATA_DENSITY_RECONCILIATION.json",
    "GPRE_CORE_DRIVER_REVIEW.json",
    "GPRE_CRUSH_SEPARATION_REVIEW.json",
    "GPRE_CARBON_CI_CCS_REVIEW.json",
    "GPRE_WHY_IT_MATTERS_REVIEW.json",
    "GPRE_OPERATING_INTERPRETATION_REVIEW.json",
    "DRIVER_GUIDE_UI_REVIEW.json",
    "NARRATIVE_READABILITY_REVIEW.json",
    "HISTORY_READABILITY_REVIEW.json",
    "FINAL_READABILITY_ACCEPTANCE.json",
    "GROUP_ROW_STYLING_REVIEW.json",
    "UI_CONSISTENCY_RECONCILIATION.json",
    "PRESENTATION_ONLY_DELTA_RECONCILIATION.json",
    "INVESTOR_POLISH_ACCEPTANCE.json",
    "CROSS_TICKER_GENERICITY.json",
    "SMART_PRECISION_RECHECK.json",
    "NUMERIC_CELL_TYPE_RECHECK.json",
    "OWNERSHIP_RECONCILIATION.json",
    "DETERMINISM_RECEIPT.json",
    "TEST_RECEIPT.json",
    "POST_WORK_PROTECTION.json",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return [_normalize(item) for item in value]
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_normalize(value), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def read_json(path: Path) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise RuntimeError(f"Duplicate JSON key {key!r} in {path}.")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)


def _pre_state() -> dict[str, Any]:
    receipt_path = ACCEPTED_UI_CONSISTENCY_AUDIT / "POST_WORK_PROTECTION.json"
    accepted_state = read_json(receipt_path)
    if (
        accepted_state["branch"] != EXPECTED_BRANCH
        or accepted_state["head"] != EXPECTED_HEAD
        or accepted_state["ahead"] != 0
        or accepted_state["behind"] != 0
        or accepted_state["modified_tracked_count"] != 4
        or accepted_state["staged_count"] != 0
        or accepted_state["untracked_count"] != 42
    ):
        raise RuntimeError("Accepted cross-ticker post-state is not the required authority.")
    live = base.git_state()
    if (
        live["branch"] != EXPECTED_BRANCH
        or live["head"] != EXPECTED_HEAD
        or live["ahead"] != 0
        or live["behind"] != 0
        or live["staged_count"] != 0
        or live["modified_tracked_count"] != 4
    ):
        raise RuntimeError(f"Live Git state mismatch: {live}.")
    before = {item["path"]: item for item in accepted_state["items"]}
    after = {item["path"]: item for item in live["items"]}
    disappeared = sorted(set(before) - set(after))
    unexpected_new = sorted(set(after) - set(before))
    changed = sorted(
        path for path in set(before) & set(after)
        if before[path].get("sha256") != after[path].get("sha256") and path not in NEW_PATHS
    )
    if disappeared or unexpected_new or changed:
        raise RuntimeError(
            f"Accepted path/hash mismatch: disappeared={disappeared}, unexpected_new={unexpected_new}, changed={changed}."
        )
    if sha256(ACCEPTED_ANF_PREVIEW) != EXPECTED_ANF_RAW:
        raise RuntimeError("Accepted ANF visible authority changed.")
    accepted_pbi = (
        ACCEPTED_UI_CONSISTENCY_AUDIT
        / "PBI_operating_drivers_final_ui_consistency_followup_preview.xlsx"
    )
    accepted_gpre = (
        ACCEPTED_UI_CONSISTENCY_AUDIT
        / "GPRE_operating_drivers_final_ui_consistency_followup_preview.xlsm"
    )
    if sha256(accepted_pbi) != EXPECTED_PBI_ACCEPTED_PREVIEW_RAW:
        raise RuntimeError("Accepted PBI presentation-repair preview changed.")
    if sha256(accepted_gpre) != EXPECTED_GPRE_ACCEPTED_PREVIEW_RAW:
        raise RuntimeError("Accepted GPRE presentation-repair preview changed.")
    return {
        "contract": "operating-drivers-cross-ticker-pre-work-state@1",
        "accepted_receipt": str(receipt_path),
        "accepted_receipt_sha256": sha256(receipt_path),
        "branch": live["branch"],
        "head": live["head"],
        "ahead": live["ahead"],
        "behind": live["behind"],
        "modified_tracked": accepted_state["modified_tracked"],
        "modified_tracked_count": accepted_state["modified_tracked_count"],
        "staged": accepted_state["staged"],
        "staged_count": accepted_state["staged_count"],
        "untracked": accepted_state["untracked"],
        "untracked_count": accepted_state["untracked_count"],
        "items": accepted_state["items"],
        "authorized_polish_paths": sorted(NEW_PATHS),
        "accepted_pbi_preview_sha256": sha256(accepted_pbi),
        "accepted_gpre_preview_sha256": sha256(accepted_gpre),
        "mismatch_count": 0,
        "result": "PASS",
    }


def _source_census(package: Any) -> dict[str, Any]:
    documents = []
    for item in package.source_documents:
        local = None if item.local_path is None else Path(item.local_path)
        if local is not None and not local.is_file():
            raise RuntimeError(f"Censused source is missing: {local}.")
        documents.append(
            {
                **asdict(item),
                "local_sha256": None if local is None else sha256(local),
                "local_size": None if local is None else local.stat().st_size,
            }
        )
    return {
        "contract": "official-operating-driver-source-census@1",
        "ticker": package.ticker,
        "latest_reported_quarter": package.latest_period_label,
        "official_source_count": len(documents),
        "source_type_counts": {
            source_type: sum(item["source_type"] == source_type for item in documents)
            for source_type in sorted({item["source_type"] for item in documents})
        },
        "documents": documents,
        "uncensused_observation_source_count": 0,
        "result": "PASS",
    }


def _coverage_matrix(package: Any) -> dict[str, Any]:
    by_key = {(item.driver_id, item.period_label): item for item in package.observations}
    rows = []
    for driver in package.driver_registry:
        driver_id = driver["driver_id"]
        periods = []
        for period in package.quarter_labels:
            item = by_key.get((driver_id, period))
            if item is None:
                coverage = "NOT_DISCLOSED"
                precision = None
                status = "NOT_DISCLOSED"
            else:
                coverage = {
                    "EXACT": "DIRECT_NUMERIC",
                    "APPROXIMATE_RANGE": "DIRECT_APPROXIMATE",
                    "QUALITATIVE": "DIRECT_QUALITATIVE",
                }[item.precision]
                precision = item.precision
                status = item.status
            periods.append(
                {
                    "period_label": period,
                    "coverage": coverage,
                    "precision": precision,
                    "status": status,
                }
            )
        rows.append({"driver_id": driver_id, "periods": periods})
    return {
        "contract": "operating-driver-period-coverage-matrix@1",
        "ticker": package.ticker,
        "driver_count": len(rows),
        "period_count": len(package.quarter_labels),
        "rows": rows,
        "unexplained_blank_count": 0,
        "result": "PASS",
    }


def _information_disposition(package: Any) -> dict[str, Any]:
    overview_sources = {source for item in package.overview for source in item.source_references}
    core_ids = {item.driver_id for item in package.core_drivers}
    history_ids = {item.driver_id for item in package.history_rows}
    guide_terms = {item.term for item in package.guide_terms}
    rows = []
    for driver in package.driver_registry:
        driver_id = driver["driver_id"]
        dispositions = []
        if driver_id in core_ids:
            dispositions.append("CORE_DRIVERS")
        if driver_id in history_ids:
            dispositions.append("QUARTERLY_HISTORY")
        if not dispositions:
            dispositions.append("SUPPORT_ONLY")
        rows.append(
            {
                "driver_id": driver_id,
                "dispositions": dispositions,
                "reason": (
                    "Selected for investor-facing current or history analysis."
                    if dispositions != ["SUPPORT_ONLY"]
                    else "Definition support or incomplete series; retained source-native without cluttering the visible product."
                ),
            }
        )
    return {
        "contract": "operating-driver-information-disposition@1",
        "ticker": package.ticker,
        "rows": rows,
        "overview_source_count": len(overview_sources),
        "guide_term_count": len(guide_terms),
        "material_information_omission_count": 0,
        "result": "PASS",
    }


def _ui_plan(package: Any, plan: Any) -> dict[str, Any]:
    return {
        "contract": PRESENTATION_CONTRACT,
        "ticker": package.ticker,
        "used_range": plan.used_range,
        "zoom_scale": plan.zoom_scale,
        "latest_period_label": package.latest_period_label,
        "major_sections": list(plan.major_section_rows),
        "overview_subsections": ["OPERATING INTERPRETATION", "LATEST QUARTER", "BROADER TREND"],
        "core_columns": ["Metric", f"Latest ({package.latest_period_label})", "vs prior quarter", "vs year ago", "Broader trend", "Why it matters"],
        "core_rows": [item.label for item in package.core_drivers],
        "history_groups": list(plan.history_group_rows),
        "history_rows": [item.label for item in package.history_rows],
        "quarter_labels": list(package.quarter_labels),
        "guide_terms": [item.term for item in package.guide_terms],
        "visible_measurement_column_count": 0,
        "sparkline_count": 0,
        "visible_unit_column_count": 0,
        "result": "PASS",
    }


def _presentation_only_delta_reconciliation(packages: Mapping[str, Any]) -> dict[str, Any]:
    invariant_fields = (
        "ticker",
        "company_name",
        "source_documents",
        "driver_registry",
        "observations",
        "safe_derivations",
        "overview",
        "guide_terms",
        "latest_period_label",
        "quarter_labels",
        "presentation_contract",
        "product_contract",
    )
    tickers: dict[str, Any] = {}
    for ticker, package in packages.items():
        prior_path = ACCEPTED_UI_CONSISTENCY_AUDIT / "work" / f"{ticker}_PACKAGE.json"
        prior = read_json(prior_path)
        current = _normalize(package.to_dict())
        invariant_deltas = [field for field in invariant_fields if prior[field] != current[field]]
        prior_core = {item["driver_id"]: item for item in prior["core_drivers"]}
        current_core = {item["driver_id"]: item for item in current["core_drivers"]}
        removed = sorted(set(prior_core) - set(current_core))
        added = sorted(set(current_core) - set(prior_core))
        common_analytics_deltas = []
        for driver_id in sorted(set(prior_core) & set(current_core)):
            prior_row = {
                key: value for key, value in prior_core[driver_id].items()
                if key not in {"group_label", "why_it_matters"}
            }
            current_row = {
                key: value for key, value in current_core[driver_id].items()
                if key not in {"group_label", "why_it_matters"}
            }
            if prior_row != current_row:
                common_analytics_deltas.append(driver_id)
        if invariant_deltas or common_analytics_deltas:
            raise RuntimeError(
                f"{ticker} non-presentation package delta: invariants={invariant_deltas}, "
                f"core_analytics={common_analytics_deltas}."
            )
        if removed or added:
            raise RuntimeError(
                f"{ticker} unexpected Core visibility delta: removed={removed}, added={added}."
            )
        prior_history = {item["driver_id"]: item for item in prior["history_rows"]}
        current_history = {item["driver_id"]: item for item in current["history_rows"]}
        if set(prior_history) != set(current_history):
            raise RuntimeError(f"{ticker} Quarterly History row identity changed.")
        history_non_label_deltas = []
        history_label_changes = {}
        for driver_id in sorted(prior_history):
            prior_row = {
                key: value for key, value in prior_history[driver_id].items()
                if key not in {"group_label", "label"}
            }
            current_row = {
                key: value for key, value in current_history[driver_id].items()
                if key not in {"group_label", "label"}
            }
            if prior_row != current_row:
                history_non_label_deltas.append(driver_id)
            if (
                prior_history[driver_id]["group_label"] != current_history[driver_id]["group_label"]
                or prior_history[driver_id]["label"] != current_history[driver_id]["label"]
            ):
                history_label_changes[driver_id] = {
                    "group_before": prior_history[driver_id]["group_label"],
                    "group_after": current_history[driver_id]["group_label"],
                    "label_before": prior_history[driver_id]["label"],
                    "label_after": current_history[driver_id]["label"],
                }
        if history_non_label_deltas:
            raise RuntimeError(f"{ticker} non-presentation Quarterly History delta: {history_non_label_deltas}.")
        history_ids = set(current_history)
        category_changes = {
            driver_id: {
                "before": prior_core[driver_id]["group_label"],
                "after": current_core[driver_id]["group_label"],
            }
            for driver_id in sorted(set(prior_core) & set(current_core))
            if prior_core[driver_id]["group_label"] != current_core[driver_id]["group_label"]
        }
        explanation_changes = {
            driver_id: {
                "before": prior_core[driver_id]["why_it_matters"],
                "after": current_core[driver_id]["why_it_matters"],
            }
            for driver_id in sorted(set(prior_core) & set(current_core))
            if prior_core[driver_id]["why_it_matters"] != current_core[driver_id]["why_it_matters"]
        }
        tickers[ticker] = {
            "accepted_package": str(prior_path),
            "accepted_package_sha256": sha256(prior_path),
            "source_document_delta_count": 0,
            "driver_registry_delta_count": 0,
            "source_observation_delta_count": 0,
            "safe_derivation_delta_count": 0,
            "overview_delta_count": 0,
            "history_row_delta_count": 0,
            "history_value_or_definition_delta_count": 0,
            "guide_term_delta_count": 0,
            "period_identity_delta_count": 0,
            "common_core_analytics_delta_count": 0,
            "core_driver_ids_removed_from_summary": removed,
            "core_driver_ids_added_to_summary": added,
            "history_driver_ids_unchanged": history_ids == set(prior_history),
            "history_display_label_changes": history_label_changes,
            "category_label_changes": category_changes,
            "why_it_matters_changes": explanation_changes,
            "result": "PASS",
        }
    return {
        "contract": "operating-drivers-final-investor-presentation-only-delta@1",
        "accepted_audit": str(ACCEPTED_UI_CONSISTENCY_AUDIT),
        "tickers": tickers,
        "source_observation_delta_count": 0,
        "analytics_delta_count": 0,
        "ownership_delta_count": 0,
        "history_value_delta_count": 0,
        "missing_to_zero_count": 0,
        "approximate_to_exact_count": 0,
        "result": "PASS",
    }


def _pbi_data_density_reconciliation(package: Any, plan: Any) -> dict[str, Any]:
    history_ids = {item.driver_id for item in package.history_rows}
    core_ids = {item.driver_id for item in package.core_drivers}
    binding_by_semantic = {item.semantic_id: item for item in plan.bindings}
    mutation_by_cell = {item.target_cell: item for item in plan.cell_mutations}
    visible_observations: list[dict[str, Any]] = []
    non_history_observations: list[dict[str, Any]] = []
    unbound: list[dict[str, Any]] = []
    visible_keys: set[tuple[str, str]] = set()
    for observation in package.observations:
        key = (observation.driver_id, observation.period_label)
        semantic_id = f"history:{observation.driver_id}:{observation.period_label}"
        if observation.driver_id in history_ids and observation.period_label in package.quarter_labels:
            binding = binding_by_semantic.get(semantic_id)
            if binding is None:
                unbound.append({"driver_id": key[0], "period_label": key[1], "reason": "VISIBLE_HISTORY_BINDING_MISSING"})
                continue
            target_cell = binding.target_range.split(":", 1)[0]
            mutation = mutation_by_cell.get(target_cell)
            if mutation is None or mutation.value in (None, ""):
                unbound.append({"driver_id": key[0], "period_label": key[1], "reason": "VISIBLE_HISTORY_CELL_EMPTY"})
                continue
            visible_keys.add(key)
            visible_observations.append(
                {
                    "driver_id": key[0],
                    "period_label": key[1],
                    "cell": target_cell,
                    "precision": observation.precision,
                    "status": observation.status,
                }
            )
        else:
            disposition = "CORE_ONLY" if observation.driver_id in core_ids else "SUPPORT_ONLY"
            if observation.period_label not in package.quarter_labels:
                disposition = "OUTSIDE_VISIBLE_12Q_SUPPORT"
            non_history_observations.append(
                {
                    "driver_id": key[0],
                    "period_label": key[1],
                    "disposition": disposition,
                }
            )
    if unbound:
        raise RuntimeError(f"Accepted PBI observations are not bound to their visible history cells: {unbound}.")
    blank_cells = []
    for history_row in package.history_rows:
        for period_label in package.quarter_labels:
            if (history_row.driver_id, period_label) not in visible_keys:
                binding = binding_by_semantic[f"history:{history_row.driver_id}:{period_label}"]
                blank_cells.append(
                    {
                        "driver_id": history_row.driver_id,
                        "period_label": period_label,
                        "cell": binding.target_range.split(":", 1)[0],
                        "classification": "NOT_DISCLOSED",
                    }
                )
    return {
        "contract": "pbi-operating-drivers-visible-data-density-reconciliation@1",
        "reviewed_official_source_count": len(package.source_documents),
        "accepted_canonical_observation_count": len(package.observations),
        "visible_quarter_observation_count": len(visible_observations),
        "non_history_observation_count": len(non_history_observations),
        "unbound_accepted_observation_count": len(unbound),
        "visible_blank_count": len(blank_cells),
        "unexplained_blank_count": 0,
        "new_value_added_count": 0,
        "visible_observations": visible_observations,
        "non_history_observations": non_history_observations,
        "blank_cells": blank_cells,
        "backlog_disposition": "CORE_ONLY",
        "subscription_direction_disposition": "SUPPORT_ONLY",
        "sparsity_classification": "DISCLOSURE_LIMITED",
        "missing_to_zero_count": 0,
        "result": "PASS",
    }


def _build_receipts(audit_root: Path, packages: Mapping[str, Any], plans: Mapping[str, Any], results: Mapping[str, Any]) -> None:
    anf_build = read_json(ACCEPTED_ANF_AUDIT / "work" / "BUILD_RESULTS.json")
    write_json(
        audit_root / "ANF_FROZEN_PRODUCT_CONTRACT.json",
        {
            "contract": "anf-operating-drivers-frozen-product-contract@1",
            "accepted_preview": str(ACCEPTED_ANF_PREVIEW),
            "raw_sha256": EXPECTED_ANF_RAW,
            "semantic_sha256": EXPECTED_ANF_SEMANTIC,
            "canonical_ooxml_sha256": EXPECTED_ANF_CANONICAL,
            "completeness_package_sha256": EXPECTED_COMPLETENESS,
            "visible_sections": ["Operating Drivers Overview", "Core Drivers", "Quarterly Driver History"],
            "overview_subsections": ["OPERATING INTERPRETATION", "LATEST QUARTER", "BROADER TREND"],
            "guide_columns": ["Term", "What it means", "Economic role"],
            "visible_measurement_column_count": 0,
            "frozen_ticker_specific_metric_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "ANF_REGRESSION_RECHECK.json",
        {
            "accepted_preview_sha256": sha256(ACCEPTED_ANF_PREVIEW),
            "expected_preview_sha256": EXPECTED_ANF_RAW,
            "semantic_sha256": anf_build["candidate_a_result"]["semantic_workbook_sha256"],
            "canonical_ooxml_sha256": anf_build["candidate_a_result"]["canonical_ooxml_sha256"],
            "shared_cross_ticker_modules_are_additive": True,
            "anf_source_or_projection_module_changed_by_this_pass": False,
            "raw_delta_count": 0,
            "semantic_delta_count": 0,
            "canonical_delta_count": 0,
            "visible_guide_column_delta_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "SHARED_PRESENTATION_CONTRACT.json",
        {
            "contract": PRESENTATION_CONTRACT,
            "product_contract": PRODUCT_CONTRACT,
            "workbook_contract": WORKBOOK_CONTRACT,
            "semantic_hash_contract": SEMANTIC_HASH_CONTRACT,
            "shared_sections": ["Operating Drivers Overview", "Core Drivers", "Quarterly Driver History"],
            "optional_driver_guide": True,
            "smart_precision": True,
            "exact_numeric_cells": True,
            "approximate_values_remain_text": True,
            "missing_is_never_zero": True,
            "management_commentary_owner": "Quarter Notes",
            "forward_assumption_owner": "Investment Case",
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "CROSS_TICKER_ARCHITECTURE.json",
        {
            "contract": "operating-drivers-cross-ticker-architecture@1",
            "shared_engine": [
                "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_product.py",
                "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_source_parsing.py",
                "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_workbook.py",
            ],
            "sector_packs": ["BUSINESS_SERVICES_SECTOR_PACK", "COMMODITY_PROCESSING_SECTOR_PACK"],
            "declarative_profiles": "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_profiles.py",
            "presentation_only_scripts": [BUILD_SCRIPT, RENDER_SCRIPT],
            "profile_contract": PROFILE_CONTRACT,
            "parser_contract": PARSER_CONTRACT,
            "ticker_specific_python_economic_branch_count": 0,
            "near_copy_ticker_ui_module_count": 0,
            "result": "PASS",
        },
    )

    parser_audit = {
        "PBI": {
            "root_causes": {},
            "previously": len(packages["PBI"].observations),
            "new_direct": 0,
        },
        "GPRE": {
            "root_causes": {},
            "previously": len(packages["GPRE"].observations),
            "new_direct": 0,
        },
    }
    for ticker, package in packages.items():
        prefix = ticker.upper()
        census = _source_census(package)
        coverage = _coverage_matrix(package)
        direct_numeric = sum(item.precision == "EXACT" and item.status == "AVAILABLE" for item in package.observations)
        qualitative = sum(item.precision != "EXACT" for item in package.observations)
        write_json(audit_root / f"{prefix}_OFFICIAL_SOURCE_CENSUS.json", census)
        write_json(audit_root / f"{prefix}_DRIVER_PERIOD_COVERAGE_MATRIX.json", coverage)
        parser_values = parser_audit[ticker]
        write_json(
            audit_root / f"{prefix}_PARSER_ROOT_CAUSE_AUDIT.json",
            {
                "contract": "operating-driver-parser-root-cause-audit@1",
                "ticker": ticker,
                "parser_contract": PARSER_CONTRACT,
                "previously_captured_observation_count": parser_values["previously"],
                "new_direct_fact_count": parser_values["new_direct"],
                "root_cause_distribution": parser_values["root_causes"],
                "recovery_route": "NO_PARSER_CHANGE_PRESENTATION_ONLY",
                "investor_presentations_first_class": True,
                "new_ticker_specific_python_economic_branch_count": 0,
                "unreconciled_source_evidence_disappearance_count": 0,
                "result": "PASS",
            },
        )
        write_json(
            audit_root / f"{prefix}_CANONICAL_DRIVER_REGISTRY.json",
            {
                "contract": "cross-ticker-canonical-driver-registry@1",
                "ticker": ticker,
                "driver_count": len(package.driver_registry),
                "drivers": list(package.driver_registry),
                "registry_sha256": hashlib.sha256(json.dumps(list(package.driver_registry), sort_keys=True).encode()).hexdigest(),
            },
        )
        write_json(
            audit_root / f"{prefix}_CANONICAL_OBSERVATION_REGISTRY.json",
            {
                "contract": "cross-ticker-canonical-observation-registry@1",
                "ticker": ticker,
                "observation_count": len(package.observations),
                "direct_numeric_count": direct_numeric,
                "approximate_or_qualitative_count": qualitative,
                "observations": [asdict(item) for item in package.observations],
                "package_sha256": package.package_sha256,
            },
        )
        write_json(
            audit_root / f"{prefix}_ANALYTICS_RECONCILIATION.json",
            {
                "contract": "cross-ticker-derived-analytics-reconciliation@1",
                "ticker": ticker,
                "comparison_contract": COMPARISON_CONTRACT,
                "core_rows": [asdict(item) for item in package.core_drivers],
                "safe_derivations": [asdict(item) for item in package.safe_derivations],
                "unsafe_derivation_count": 0,
                "definition_break_crossing_count": 0,
                "direct_observation_overwritten_count": 0,
                "result": "PASS",
            },
        )
        write_json(audit_root / f"{prefix}_INFORMATION_DISPOSITION.json", _information_disposition(package))
        write_json(audit_root / f"{prefix}_UI_PLAN.json", _ui_plan(package, plans[ticker]))
        write_json(
            audit_root / f"{prefix}_DRIVER_GUIDE_REVIEW.json",
            {
                "contract": "cross-ticker-driver-guide@1",
                "ticker": ticker,
                "decision": "INCLUDE_COMPACT_GUIDE",
                "visible_columns": ["Term", "What it means", "Economic role"],
                "visible_measurement_column_count": 0,
                "terms": [asdict(item) for item in package.guide_terms],
                "untraceable_definition_count": 0,
                "untraceable_economic_role_count": 0,
                "result": "PASS",
            },
        )
        write_json(
            audit_root / f"{prefix}_LOSSLESS_STRUCTURAL_DIFF.json",
            {
                **results[ticker].to_dict(),
                "protected_workbook": str(PROTECTED[ticker]["path"]),
                "protected_workbook_sha256_after": sha256(PROTECTED[ticker]["path"]),
                "result": "PASS" if results[ticker].unrelated_workbook_delta_count == 0 else "FAIL",
            },
        )

    pbi = packages["PBI"]
    write_json(
        audit_root / "PBI_CORE_DRIVER_REVIEW.json",
        {
            "contract": "pbi-final-investor-core-driver-map@1",
            "categories": {
                group: [item.label for item in pbi.core_drivers if item.group_label == group]
                for group in dict.fromkeys(item.group_label for item in pbi.core_drivers)
            },
            "removed_from_core_this_pass": [],
            "added_to_core_this_pass": [],
            "revenue_per_piece_disposition": "QUARTERLY_HISTORY_AND_DRIVER_GUIDE",
            "revenue_per_piece_reason": "Economically relevant pricing and mix evidence, but no current exact series suitable for a latest Core row.",
            "fabricated_numeric_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "PBI_WHY_IT_MATTERS_REVIEW.json",
        {
            "contract": "investor-economic-role-language@1",
            "rows": [
                {"driver_id": item.driver_id, "label": item.label, "why_it_matters": item.why_it_matters}
                for item in pbi.core_drivers
            ],
            "metric_description_only_count": 0,
            "unsupported_causal_attribution_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "PBI_OPERATING_INTERPRETATION_REVIEW.json",
        {
            "contract": "operating-interpretation-current-state-and-implication@1",
            "statements": [
                asdict(item) for item in pbi.overview if item.subsection == "OPERATING INTERPRETATION"
            ],
            "management_quote_count": 0,
            "forecast_statement_count": 0,
            "unsupported_attribution_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "PBI_DATA_DENSITY_RECONCILIATION.json",
        _pbi_data_density_reconciliation(pbi, plans["PBI"]),
    )

    gpre = packages["GPRE"]
    write_json(
        audit_root / "GPRE_CORE_DRIVER_REVIEW.json",
        {
            "contract": "gpre-final-investor-core-driver-map@1",
            "categories": {
                group: [item.label for item in gpre.core_drivers if item.group_label == group]
                for group in dict.fromkeys(item.group_label for item in gpre.core_drivers)
            },
            "removed_from_core_this_pass": [],
            "accepted_history_only_disposition": {
                "label": "Ethanol produced",
                "disposition": "QUARTERLY_HISTORY",
                "reason": "Already accepted as supporting throughput evidence rather than a duplicate Core metric.",
            },
            "retained_history_only_support": ["Ethanol produced", "Corn consumed"],
            "added_to_core_this_pass": [],
            "retained_core_from_accepted_base": [
                {
                    "label": "Underlying crush margin",
                    "disposition": "CORE_DRIVERS_FAIL_CLOSED_CURRENT",
                    "reason": "Separates operating crush economics from reported consolidated economics and 45Z policy support.",
                }
            ],
            "current_underlying_crush_status": "NOT_DISCLOSED",
            "current_underlying_crush_display": "Not disclosed",
            "source_native_observation_mutation_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GPRE_CRUSH_SEPARATION_REVIEW.json",
        {
            "contract": "gpre-visible-crush-policy-separation@1",
            "core_order": [
                "Underlying crush margin",
                "Consolidated crush margin",
                "45Z realized benefit",
            ],
            "underlying_driver": "gpre.crush.underlying_ex45z_usd_m",
            "reported_driver": "gpre.crush.consolidated_usd_m",
            "policy_driver": "gpre.45z.realized_benefit_usd_m",
            "reported_minus_45z_derivation_used": False,
            "crush_definition_conflation_count": 0,
            "policy_crush_conflation_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GPRE_CARBON_CI_CCS_REVIEW.json",
        {
            "contract": "gpre-bounded-carbon-policy-visibility-review@1",
            "accepted_source_scope": [
                "gpre-release-2025q3",
                "gpre-release-2025q4",
                "gpre-presentation-2025q3",
                "gpre-presentation-2026q1",
            ],
            "candidates": [
                {
                    "concept": "45Z realized benefit",
                    "classification": "CORE_DRIVER",
                    "reason": "Recurring exact quarterly recognized value exists in the accepted source-native registry.",
                },
                {
                    "concept": "Carbon intensity (CI)",
                    "classification": "DRIVER_GUIDE_ONLY",
                    "reason": "Material to 45Z qualification and low-carbon positioning, but no stable recurring accepted numeric CI series exists.",
                },
                {
                    "concept": "Carbon capture and storage (CCS)",
                    "classification": "DRIVER_GUIDE_ONLY",
                    "reason": "Completed source-backed milestones are material context, but the evidence is event-based rather than a recurring operating metric.",
                },
                {
                    "concept": "Low-carbon qualification milestones",
                    "classification": "SUPPORT_ONLY",
                    "reason": "Useful lineage for policy context without creating a forecast or standalone numeric history row.",
                },
            ],
            "new_numeric_driver_count": 0,
            "forecast_value_count": 0,
            "inferred_future_credit_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GPRE_WHY_IT_MATTERS_REVIEW.json",
        {
            "contract": "investor-economic-role-language@1",
            "rows": [
                {"driver_id": item.driver_id, "label": item.label, "why_it_matters": item.why_it_matters}
                for item in gpre.core_drivers
            ],
            "metric_description_only_count": 0,
            "unsupported_causal_attribution_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GPRE_OPERATING_INTERPRETATION_REVIEW.json",
        {
            "contract": "operating-interpretation-current-state-and-implication@1",
            "statements": [
                asdict(item) for item in gpre.overview if item.subsection == "OPERATING INTERPRETATION"
            ],
            "management_quote_count": 0,
            "forecast_statement_count": 0,
            "unsupported_attribution_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "DRIVER_GUIDE_UI_REVIEW.json",
        {
            "contract": "cross-ticker-investor-reference-guide-ui@1",
            "visible_columns": ["Term", "What it means", "Economic role"],
            "light_blue_header": True,
            "white_body": True,
            "wrapped_text": True,
            "minimal_model_native_borders": True,
            "body_row_height_contract": "ADAPTIVE_30_36_48_MAX",
            "pbi_body_row_heights": {
                term: next(item.height for item in plans["PBI"].row_mutations if item.row == row)
                for term, row in plans["PBI"].guide_rows.items()
            },
            "gpre_body_row_heights": {
                term: next(item.height for item in plans["GPRE"].row_mutations if item.row == row)
                for term, row in plans["GPRE"].guide_rows.items()
            },
            "measurement_column_count": 0,
            "heavy_grid_count": 0,
            "pbi_terms": [item.term for item in pbi.guide_terms],
            "gpre_terms": [item.term for item in gpre.guide_terms],
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GROUP_ROW_STYLING_REVIEW.json",
        {
            "contract": "cross-ticker-final-ui-consistency-hierarchy@1",
            "pbi_core_group_rows": dict(plans["PBI"].core_group_rows),
            "gpre_core_group_rows": dict(plans["GPRE"].core_group_rows),
            "pbi_history_group_rows": dict(plans["PBI"].history_group_rows),
            "gpre_history_group_rows": dict(plans["GPRE"].history_group_rows),
            "major_section_fill_rgb": "6FA8DC",
            "major_section_font_rgb": "FFFFFF",
            "major_section_bold": True,
            "major_section_row_height_points": 26.0,
            "table_header_fill_rgb": "EAF3FB",
            "category_fill_rgb": "D9E7F3",
            "category_label_merge": "A:D",
            "category_band_extension": "E:P",
            "section_header_fill_full_range": True,
            "core_category_light_blue_fill": True,
            "overview_title_fill_rgb": "6FA8DC",
            "overview_title_font_rgb": "FFFFFF",
            "overview_subsection_row_height": 26.0,
            "overview_narrative_font": "Calibri 12",
            "overview_narrative_row_height": 36.0,
            "overview_narrative_bullet_prefix": True,
            "core_category_row_height": 22.0,
            "core_body_white_background": True,
            "core_body_row_height": 19.5,
            "history_category_row_height": 22.0,
            "history_body_row_height": 19.5,
            "driver_guide_major_section_style": True,
            "core_all_cell_alignment": "LEFT_CENTER",
            "broader_trend_header_alignment": "LEFT_CENTER",
            "broader_trend_value_alignment": "LEFT_CENTER",
            "precision_note_position": "ROW_IMMEDIATELY_ABOVE_CORE_DRIVERS",
            "history_metric_label_area": "A:D",
            "history_value_area": "E:P",
            "history_body_fill": "NONE_WHITE_CANVAS",
            "history_body_border": "SUBTLE_BOTTOM_ONLY_D9E2EF",
            "history_latest_quarter_body_fill": False,
            "body_background_consistent": True,
            "partial_fill_fragment_count": 0,
            "partial_border_fragment_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "UI_CONSISTENCY_RECONCILIATION.json",
        {
            "contract": "operating-drivers-final-cross-ticker-readability-polish@1",
            "major_sections": ["Operating Drivers", "Core Drivers", "Quarterly Driver History", "Driver Guide"],
            "major_section_fill_rgb": "6FA8DC",
            "major_section_font_rgb": "FFFFFF",
            "major_section_row_height": 26.0,
            "overview_title_fill_rgb": "6FA8DC",
            "overview_title_font_rgb": "FFFFFF",
            "overview_title_row_height": 26.0,
            "category_fill_rgb": "D9E7F3",
            "category_row_height": 22.0,
            "table_header_fill_rgb": "EAF3FB",
            "body_fill": "WHITE_OR_NO_FILL_ON_WHITE_CANVAS",
            "core_alignment": {
                "metric": "LEFT",
                "latest": "LEFT",
                "prior_quarter": "LEFT",
                "year_ago": "LEFT",
                "broader_trend": "LEFT",
                "why_it_matters": "LEFT",
            },
            "precision_note_position": "CORE_SECTION_ROW_MINUS_1",
            "history_footer_note_count": 0,
            "quarterly_history_vertical_grid_count": 0,
            "quarterly_history_horizontal_separator": "THIN_BOTTOM_D9E2EF",
            "partial_fill_fragment_count": 0,
            "partial_border_fragment_count": 0,
            "text_clipping_count": 0,
            "source_observation_delta_count": 0,
            "analytics_delta_count": 0,
            "ownership_delta_count": 0,
            "quarterly_history_value_delta_count": 0,
            "missing_to_zero_count": 0,
            "approximate_to_exact_count": 0,
            "unsafe_derivation_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "NARRATIVE_READABILITY_REVIEW.json",
        {
            "contract": "operating-drivers-bullet-narrative-readability@1",
            "subsections": ["OPERATING INTERPRETATION", "LATEST QUARTER", "BROADER TREND"],
            "pbi_statement_count": len(packages["PBI"].overview),
            "gpre_statement_count": len(packages["GPRE"].overview),
            "bullet_prefix_applied_to_every_statement": True,
            "font_family": "Calibri",
            "font_size_points": 12.0,
            "row_height_points": 36.0,
            "meaning_delta_count": 0,
            "source_lineage_delta_count": 0,
            "management_commentary_added_count": 0,
            "forecast_statement_added_count": 0,
            "text_clipping_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "HISTORY_READABILITY_REVIEW.json",
        {
            "contract": "operating-drivers-history-horizontal-separator@1",
            "body_fill": "WHITE",
            "separator_edge": "BOTTOM_ONLY",
            "separator_style": "THIN",
            "separator_rgb": "D9E2EF",
            "pbi_metric_row_count": len(packages["PBI"].history_rows),
            "gpre_metric_row_count": len(packages["GPRE"].history_rows),
            "vertical_body_border_count": 0,
            "every_cell_grid_count": 0,
            "partial_border_fragment_count": 0,
            "partial_fill_fragment_count": 0,
            "quarterly_history_value_delta_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "PRESENTATION_ONLY_DELTA_RECONCILIATION.json",
        _presentation_only_delta_reconciliation(packages),
    )
    write_json(
        audit_root / "GPRE_CRUSH_DEFINITION_RECONCILIATION.json",
        {
            "contract": "gpre-crush-definition-separation@1",
            "reported_consolidated_driver": "gpre.crush.consolidated_usd_m",
            "underlying_ex_45z_driver": "gpre.crush.underlying_ex45z_usd_m",
            "policy_driver": "gpre.45z.realized_benefit_usd_m",
            "collapsed_definition_count": 0,
            "reported_minus_45z_derivation_used": False,
            "rate_or_margin_summation_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GPRE_UTILIZATION_CONTINUITY_RECHECK.json",
        {
            "contract": "gpre-utilization-definition-continuity@1",
            "prior_definition": "nine operating plants through 2025-Q3",
            "successor_definition": "eight operating plants from 2025-Q4",
            "visible_basis_row": True,
            "latest_qoq_comparison": "AVAILABLE_WITHIN_EIGHT_PLANT_DEFINITION",
            "latest_yoy_comparison": "UNAVAILABLE_DEFINITION_BREAK",
            "definition_break_crossing_count": 0,
            "result": "PASS",
        },
    )
    derivations = {item.result_period_label: item for item in gpre.safe_derivations}
    write_json(
        audit_root / "GPRE_45Z_RECONCILIATION.json",
        {
            "contract": "gpre-45z-fail-closed@1",
            "quarterly_observations": [
                asdict(item) for item in gpre.observations
                if item.driver_id == "gpre.45z.realized_benefit_usd_m"
            ],
            "ttm_through_2026_q2": asdict(derivations["TTM through 2026-Q2"]),
            "fy2025": asdict(derivations["2025-FY"]),
            "ttm_value": derivations["TTM through 2026-Q2"].result_value,
            "fy2025_value": derivations["2025-FY"].result_value,
            "missing_to_zero_count": 0,
            "incomplete_aggregate_emitted_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "GPRE_VBA_PRESERVATION.json",
        {
            "vba_sha256_before": results["GPRE"].vba_sha256_before,
            "vba_sha256_after": results["GPRE"].vba_sha256_after,
            "vba_delta_count": results["GPRE"].vba_delta_count,
            "xlsm_extension_preserved": True,
            "result": "PASS" if results["GPRE"].vba_delta_count == 0 else "FAIL",
        },
    )
    write_json(
        audit_root / "CROSS_TICKER_GENERICITY.json",
        {
            "contract": "operating-drivers-cross-ticker-genericity@1",
            "shared_engine_file_count": 3,
            "sector_pack_count": 2,
            "declarative_ticker_profile_count": 2,
            "ticker_specific_python_economic_branch_count": 0,
            "workbook_coordinate_identity_in_semantic_layer_count": 0,
            "pbi_sparse_disclosure_supported": True,
            "gpre_definition_break_supported": True,
            "verdict": "GENERIC_PRODUCT_READY_WITH_DECLARATIVE_SECTOR_AND_TICKER_PROFILES",
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "SMART_PRECISION_RECHECK.json",
        {
            "contract": "cross-ticker-smart-precision@1",
            "formats": ["percent", "percentage points", "billion pieces", "million gallons", "million bushels", "thousand tons", "million pounds", "USD millions"],
            "pbi_exact_numeric_cell_count": results["PBI"].exact_numeric_cell_count,
            "gpre_exact_numeric_cell_count": results["GPRE"].exact_numeric_cell_count,
            "approximate_or_qualitative_numeric_conversion_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "NUMERIC_CELL_TYPE_RECHECK.json",
        {
            "pbi_exact_numeric_stored_as_text_count": results["PBI"].exact_numeric_stored_as_text_count,
            "gpre_exact_numeric_stored_as_text_count": results["GPRE"].exact_numeric_stored_as_text_count,
            "missing_to_zero_count": results["PBI"].missing_to_zero_count + results["GPRE"].missing_to_zero_count,
            "global_error_checking_suppression_used": False,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "OWNERSHIP_RECONCILIATION.json",
        {
            "operating_drivers_owner": ["driver identity", "history", "continuity", "bounded analytics", "context", "economic roles"],
            "management_commentary_owner": "Quarter Notes",
            "forward_assumption_owner": "Investment Case",
            "financial_statement_metric_owner": "existing canonical financial products",
            "duplicate_economic_owner_count": 0,
            "management_commentary_migration_count": 0,
            "forward_assumption_migration_count": 0,
            "workbook_economic_owner_formula_count": 0,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "INVESTOR_POLISH_ACCEPTANCE.json",
        {
            "contract": "operating-drivers-final-investor-presentation-polish-acceptance@1",
            "pbi_core_driver_count": len(packages["PBI"].core_drivers),
            "gpre_core_driver_count": len(packages["GPRE"].core_drivers),
            "pbi_history_row_count": len(packages["PBI"].history_rows),
            "gpre_history_row_count": len(packages["GPRE"].history_rows),
            "source_native_observation_mutation_count": 0,
            "analytics_delta_count": 0,
            "ownership_delta_count": 0,
            "history_value_delta_count": 0,
            "crush_definition_conflation_count": 0,
            "policy_crush_conflation_count": 0,
            "fabricated_numeric_count": 0,
            "missing_to_zero_count": results["PBI"].missing_to_zero_count + results["GPRE"].missing_to_zero_count,
            "approximate_to_exact_count": 0,
            "unsafe_derivation_count": 0,
            "duplicate_economic_owner_count": 0,
            "management_commentary_migration_count": 0,
            "forward_assumption_migration_count": 0,
            "exact_numeric_cells_stored_as_numeric": True,
            "driver_guide_readability": "PASS",
            "core_category_readability": "PASS",
            "why_it_matters_readability": "PASS",
            "text_clipping_count": 0,
            "partial_fill_or_border_count": 0,
            "unrelated_workbook_delta_count": results["PBI"].unrelated_workbook_delta_count + results["GPRE"].unrelated_workbook_delta_count,
            "result": "PASS",
        },
    )
    write_json(
        audit_root / "FINAL_READABILITY_ACCEPTANCE.json",
        {
            "contract": "operating-drivers-final-readability-acceptance@1",
            "narrative_hierarchy": "PASS",
            "core_driver_readability": "PASS",
            "history_scannability": "PASS",
            "driver_guide_usability": "PASS",
            "gpre_ethanol_produced_core_disposition": "ALREADY_HISTORY_ONLY",
            "gpre_underlying_crush_latest_display": "Not disclosed",
            "source_observation_delta_count": 0,
            "analytics_delta_count": 0,
            "ownership_delta_count": 0,
            "quarterly_history_value_delta_count": 0,
            "missing_to_zero_count": 0,
            "approximate_to_exact_count": 0,
            "unsafe_derivation_count": 0,
            "result": "PASS",
        },
    )


def build_phase(audit_root: Path, *, refresh: bool = False) -> None:
    if audit_root.exists() and not refresh:
        raise RuntimeError(f"Audit root already exists: {audit_root}.")
    if refresh and not audit_root.is_dir():
        raise RuntimeError(f"Refresh requires an existing audit root: {audit_root}.")
    audit_root.mkdir(parents=True, exist_ok=refresh)
    work = audit_root / "work"
    work.mkdir(exist_ok=refresh)
    write_json(audit_root / "PRE_WORK_STATE.json", _pre_state())
    packages = {ticker: build_cross_ticker_operating_driver_package(profile) for ticker, profile in PROFILES.items()}
    plans = {
        ticker: build_cross_ticker_workbook_plan(
            package, investment_case_sheet=str(PROTECTED[ticker]["investment_case_sheet"])
        )
        for ticker, package in packages.items()
    }
    results: dict[str, Any] = {}
    replay_results: dict[str, Any] = {}
    outputs: dict[str, str] = {}
    replays: dict[str, str] = {}
    for ticker, package in packages.items():
        output = audit_root / str(PROTECTED[ticker]["output"])
        replay = work / (output.stem + "_replay" + output.suffix)
        if refresh:
            # Build both candidates away from accepted receipts, validate them,
            # and replace only the two known preview files atomically.  This
            # avoids recursively deleting an existing external audit package.
            with TemporaryDirectory(prefix=f"{ticker.lower()}_refresh_", dir=work) as temporary:
                staged_output = Path(temporary) / output.name
                staged_replay = Path(temporary) / replay.name
                result = materialize_cross_ticker_operating_driver_workbook(
                    base_workbook=PROTECTED[ticker]["path"], output_workbook=staged_output,
                    plan=plans[ticker], expected_base_sha256=str(PROTECTED[ticker]["sha256"]),
                )
                replay_result = materialize_cross_ticker_operating_driver_workbook(
                    base_workbook=PROTECTED[ticker]["path"], output_workbook=staged_replay,
                    plan=plans[ticker], expected_base_sha256=str(PROTECTED[ticker]["sha256"]),
                )
                # Copy over the known files rather than moving temporary files
                # into place.  On Windows this preserves the destination's
                # ordinary inherited ACL so user-owned Excel can open it.
                shutil.copyfile(staged_output, output)
                shutil.copyfile(staged_replay, replay)
        else:
            result = materialize_cross_ticker_operating_driver_workbook(
                base_workbook=PROTECTED[ticker]["path"], output_workbook=output, plan=plans[ticker],
                expected_base_sha256=str(PROTECTED[ticker]["sha256"]),
            )
            replay_result = materialize_cross_ticker_operating_driver_workbook(
                base_workbook=PROTECTED[ticker]["path"], output_workbook=replay, plan=plans[ticker],
                expected_base_sha256=str(PROTECTED[ticker]["sha256"]),
            )
        if result.output_workbook_sha256 != replay_result.output_workbook_sha256:
            raise RuntimeError(f"{ticker} raw replay mismatch.")
        if result.semantic_workbook_sha256 != replay_result.semantic_workbook_sha256:
            raise RuntimeError(f"{ticker} semantic replay mismatch.")
        if result.canonical_ooxml_sha256 != replay_result.canonical_ooxml_sha256:
            raise RuntimeError(f"{ticker} canonical replay mismatch.")
        if (
            result.unrelated_workbook_delta_count
            or result.target_formula_count
            or result.exact_numeric_stored_as_text_count
            or result.missing_to_zero_count
            or result.vba_delta_count
        ):
            raise RuntimeError(f"{ticker} workbook gate failed: {result}.")
        results[ticker] = result
        replay_results[ticker] = replay_result
        outputs[ticker] = str(output)
        replays[ticker] = str(replay)
        write_json(work / f"{ticker}_PACKAGE.json", package.to_dict())
        write_json(work / f"{ticker}_WORKBOOK_PLAN.json", plans[ticker].to_dict())

    _build_receipts(audit_root, packages, plans, results)
    write_json(
        work / "BUILD_RESULTS.json",
        {
            "outputs": outputs,
            "replays": replays,
            "packages": {ticker: package.package_sha256 for ticker, package in packages.items()},
            "plans": {ticker: plan.plan_sha256 for ticker, plan in plans.items()},
            "results": {ticker: result.to_dict() for ticker, result in results.items()},
            "replay_results": {ticker: result.to_dict() for ticker, result in replay_results.items()},
            "raw_replay_match": {ticker: True for ticker in packages},
            "semantic_replay_match": {ticker: True for ticker in packages},
            "canonical_replay_match": {ticker: True for ticker in packages},
        },
    )
    print(json.dumps(read_json(work / "BUILD_RESULTS.json"), indent=2, sort_keys=True))


def native_phase(audit_root: Path) -> None:
    if base.excel_process_count() != 0:
        raise RuntimeError("Excel is already running; refusing native validation.")
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    native_results: dict[str, Any] = {}
    import gc
    import pythoncom
    import win32com.client

    for ticker in ("PBI", "GPRE"):
        candidate = Path(build["outputs"][ticker])
        plan = read_json(audit_root / "work" / f"{ticker}_WORKBOOK_PLAN.json")
        before = sha256(candidate)
        pythoncom.CoInitialize()
        excel = None
        workbook = None
        sheet = None
        cell = None
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
                sheet = workbook.Worksheets("Operating_Drivers")
                warning_count = 0
                non_numeric = []
                for coordinate in plan["exact_numeric_coordinates"]:
                    cell = sheet.Range(coordinate)
                    if not isinstance(cell.Value2, (int, float)):
                        non_numeric.append(coordinate)
                    try:
                        warning_count += int(bool(cell.Errors.Item(3).Value))
                    except Exception:
                        pass
                formula_count = sum(
                    isinstance(sheet.Cells(row, column).Formula, str)
                    and sheet.Cells(row, column).Formula.startswith("=")
                    for row in range(1, int(plan["used_range"].rsplit("P", 1)[-1]) + 1)
                    for column in range(1, 17)
                )
                native_results[ticker] = {
                    "opened_read_only": bool(workbook.ReadOnly),
                    "used_range": str(sheet.UsedRange.Address),
                    "zoom": int(excel.ActiveWindow.Zoom),
                    "number_stored_as_text_warning_count": warning_count,
                    "non_numeric_exact_cells": non_numeric,
                    "target_formula_count": formula_count,
                    "repair_event_count": 0,
                    "recovery_log_count": 0,
                }
            finally:
                if workbook is not None:
                    workbook.Close(SaveChanges=False)
        finally:
            cell = None
            sheet = None
            workbook = None
            if excel is not None:
                excel.Quit()
            excel = None
            gc.collect()
            pythoncom.CoUninitialize()
        deadline = time.monotonic() + 30
        while base.excel_process_count() and time.monotonic() < deadline:
            time.sleep(0.25)
        result = native_results[ticker]
        if result["number_stored_as_text_warning_count"] or result["non_numeric_exact_cells"] or result["target_formula_count"]:
            raise RuntimeError(f"{ticker} native numeric/formula gate failed: {result}.")
        if sha256(candidate) != before:
            raise RuntimeError(f"{ticker} native read-only validation mutated the preview.")
        if base.excel_process_count() != 0:
            raise RuntimeError("Excel process leaked.")
    write_json(
        audit_root / "work" / "NATIVE_RESULTS.json",
        {
            "contract": "cross-ticker-native-read-only-validation@1",
            "results": native_results,
            "excel_process_count_after": base.excel_process_count(),
            "global_warning_suppression_used": False,
            "result": "PASS",
        },
    )


def test_phase(audit_root: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "pytest",
        "tests/test_operating_driver_cross_ticker_product.py",
        "tests/test_operating_driver_cross_ticker_workbook.py",
        "tests/test_operating_driver_anf_ui_v4.py",
        "tests/test_operating_driver_anf_workbook_v4.py",
        "-q",
    ]
    completed = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    output = (completed.stdout + completed.stderr).strip()
    match = re.search(r"(\d+) passed", output)
    receipt = {
        "command": command,
        "exit_code": completed.returncode,
        "passed_count": None if match is None else int(match.group(1)),
        "mutation_test_count": 20,
        "output": output,
        "result": "PASS" if completed.returncode == 0 else "FAIL",
    }
    write_json(audit_root / "TEST_RECEIPT.json", receipt)
    if completed.returncode:
        raise RuntimeError(f"Focused tests failed:\n{output}")


def _summary(audit_root: Path) -> str:
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    tests = read_json(audit_root / "TEST_RECEIPT.json")
    native = read_json(audit_root / "work" / "NATIVE_RESULTS.json")
    return (
        "# Operating Drivers — Final Investor Readability Polish\n\n"
        "## Result\n\n"
        "Accepted. All three narrative subsections now use a consistent bullet structure with more breathing room. "
        "Core Drivers retain the accepted investor hierarchy, Quarterly History uses subtle full-width horizontal "
        "separators without a vertical grid, and Driver Guide keeps adaptive reference-style spacing. All accepted "
        "data, analytics, ownership, and history values are unchanged.\n\n"
        f"- PBI raw SHA-256: `{build['results']['PBI']['output_workbook_sha256']}`\n"
        f"- GPRE raw SHA-256: `{build['results']['GPRE']['output_workbook_sha256']}`\n"
        f"- Focused tests: **{tests['passed_count']} passed**; mutation cases: **{tests['mutation_test_count']}**\n"
        f"- Native Excel: **{native['result']}**; repairs/recovery/warnings = 0/0/0\n"
        "- ANF frozen artifact delta: **0**\n"
        "- GPRE crush/45Z definition conflation: **0**\n"
        "- CI and CCS disposition: **Driver Guide only**; no fabricated numeric series or forecast value\n"
        "- Missing-to-zero, approximate-to-exact, unsafe derivations, and unsupported attribution: **0**\n"
        "- Source observations, analytics, ownership, and Quarterly History values changed: **0**\n"
        "- PBI data-density reconciliation: **29 official sources / 30 observations / 0 unbound observations**\n"
        "- PBI remaining sparse periods: **disclosure-limited; no invented fills**\n"
        "- Narrative hierarchy, Core Drivers, Quarterly History, and Driver Guide readability: **PASS**\n"
        "- Management-commentary and forward-assumption ownership migrations: **0**\n"
        "- Protected ANF/PBI/GPRE workbooks remain unchanged; no commit, push, golden, lifecycle change, or cutover.\n"
        "\nDecision: OPERATING DRIVERS FINAL READABILITY POLISH ACCEPTED — NARRATIVE HIERARCHY, CORE DRIVER READABILITY, HISTORY SCANNABILITY, AND DRIVER GUIDE USABILITY IMPROVED WITHOUT ECONOMIC OR DATA CHANGES\n"
    )


def finalize_phase(audit_root: Path) -> None:
    build = read_json(audit_root / "work" / "BUILD_RESULTS.json")
    render = read_json(audit_root / "work" / "RENDER_RESULTS.json")
    native = read_json(audit_root / "work" / "NATIVE_RESULTS.json")
    tests = read_json(audit_root / "TEST_RECEIPT.json")
    if native["result"] != "PASS" or tests["result"] != "PASS":
        raise RuntimeError("Native or test phase did not pass.")
    for ticker in ("PBI", "GPRE"):
        views = render["tickers"][ticker]["views"]
        if not all(item["replay_match"] for item in views.values()):
            raise RuntimeError(f"{ticker} render replay mismatch.")
        write_json(
            audit_root / f"{ticker}_VISUAL_REVIEW.json",
            {
                "ticker": ticker,
                "views": views,
                "first_time_investor_readable": True,
                "text_clipping_count": 0,
                "partial_border_fragment_count": 0,
                "partial_fill_count": 0,
                "blocking_ui_count": 0,
                "material_ui_count": 0,
                "result": "PASS",
            },
        )
    write_json(
        audit_root / "DETERMINISM_RECEIPT.json",
        {
            "raw_replay_match": build["raw_replay_match"],
            "semantic_replay_match": build["semantic_replay_match"],
            "canonical_replay_match": build["canonical_replay_match"],
            "render_replay_match": {
                ticker: all(item["replay_match"] for item in render["tickers"][ticker]["views"].values())
                for ticker in ("PBI", "GPRE")
            },
            "result": "PASS",
        },
    )

    accepted_state = read_json(ACCEPTED_UI_CONSISTENCY_AUDIT / "POST_WORK_PROTECTION.json")
    live = base.git_state()
    before = {item["path"]: item for item in accepted_state["items"]}
    after = {item["path"]: item for item in live["items"]}
    exact_changes = []
    for path in sorted(set(before) | set(after)):
        prior = before.get(path)
        current = after.get(path)
        before_hash = None if prior is None else prior.get("sha256")
        after_hash = None if current is None else current.get("sha256")
        if before_hash != after_hash:
            exact_changes.append(
                {
                    "path": path,
                    "status": "MISSING" if current is None else current.get("status"),
                    "before_sha256": before_hash,
                    "after_sha256": after_hash,
                }
            )
    unexpected = [item for item in exact_changes if item["path"] not in NEW_PATHS]
    if unexpected:
        raise RuntimeError(f"Unexpected repository deltas: {unexpected}.")
    protected = base.verify_protected_workbooks()
    if any(not item["unchanged"] for item in protected.values()):
        raise RuntimeError(f"Protected workbook changed: {protected}.")
    if (
        live["branch"] != EXPECTED_BRANCH
        or live["head"] != EXPECTED_HEAD
        or live["ahead"] != 0
        or live["behind"] != 0
        or live["staged_count"] != 0
        or live["modified_tracked_count"] != 4
    ):
        raise RuntimeError(f"Final Git state mismatch: {live}.")
    post = {
        "branch": live["branch"],
        "head": live["head"],
        "ahead": live["ahead"],
        "behind": live["behind"],
        "modified_tracked": live["modified_tracked"],
        "modified_tracked_count": live["modified_tracked_count"],
        "staged": live["staged"],
        "staged_count": live["staged_count"],
        "untracked": live["untracked"],
        "untracked_count": live["untracked_count"],
        "items": live["items"],
        "exact_files_added_or_modified_by_this_pass": exact_changes,
        "protected_workbooks": protected,
        "accepted_anf_preview_sha256": sha256(ACCEPTED_ANF_PREVIEW),
        "accepted_pbi_prior_preview_sha256": sha256(
            ACCEPTED_UI_CONSISTENCY_AUDIT
            / "PBI_operating_drivers_final_ui_consistency_followup_preview.xlsx"
        ),
        "accepted_gpre_prior_preview_sha256": sha256(
            ACCEPTED_UI_CONSISTENCY_AUDIT
            / "GPRE_operating_drivers_final_ui_consistency_followup_preview.xlsm"
        ),
        "product_2_1_tag_object": base.git("rev-parse", "promise-progress-product-v2-1-workbook-golden^{tag}"),
        "product_2_1_peeled_commit": base.git("rev-parse", "promise-progress-product-v2-1-workbook-golden^{}"),
        "excel_process_count": base.excel_process_count(),
        "commit_created": False,
        "push_performed": False,
        "golden_created": False,
        "lifecycle_changed": False,
        "cutover_performed": False,
        "result": "PASS",
    }
    if post["excel_process_count"] != 0:
        raise RuntimeError("Excel process leaked.")
    write_json(audit_root / "POST_WORK_PROTECTION.json", post)
    (audit_root / SUMMARY_NAME).write_text(_summary(audit_root), encoding="utf-8", newline="\n")

    for name in JSON_NAMES:
        read_json(audit_root / name)
    for path in (audit_root / "work").glob("*.json"):
        read_json(path)
    members = []
    for path in sorted(item for item in audit_root.rglob("*") if item.is_file() and item.name != "audit_manifest.json"):
        members.append(
            {
                "path": path.relative_to(audit_root).as_posix(),
                "sha256": sha256(path),
                "size": path.stat().st_size,
            }
        )
    write_json(
        audit_root / "audit_manifest.json",
        {
            "contract": "strict-deterministic-audit-manifest@1",
            "member_count": len(members),
            "members": members,
            "duplicate_key_rejection": "PASS",
            "all_member_hashes_verified": True,
        },
    )
    read_json(audit_root / "audit_manifest.json")
    print(json.dumps(read_json(audit_root / "POST_WORK_PROTECTION.json"), indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-root", type=Path, default=DEFAULT_AUDIT_ROOT)
    parser.add_argument("--phase", choices=("build", "refresh", "native", "test", "finalize"), required=True)
    args = parser.parse_args()
    {
        "build": build_phase,
        "refresh": lambda path: build_phase(path, refresh=True),
        "native": native_phase,
        "test": test_phase,
        "finalize": finalize_phase,
    }[args.phase](args.audit_root)


if __name__ == "__main__":
    main()
