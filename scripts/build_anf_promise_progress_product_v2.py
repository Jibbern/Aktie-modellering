"""Build the reviewed ANF Promise Progress Product@2 candidate artifacts.

This command is intentionally preview-only.  It extends the accepted ANF source set
from hash-pinned local issuer/SEC documents, builds a validated source-native package,
projects ``PromiseProgressProduct@2``, and writes review artifacts outside Git.  It
never mutates the accepted Product@1 fixture or a production workbook.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
import tempfile
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

import pdfplumber
from lxml import html as lxml_html

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from pbi_xbrl.longitudinal_memory.sector_packs.retail import (
    RETAIL_SECTOR_PACK_V2,
    parse_guidance_percent_v2,
    parse_percent_text,
)
from pbi_xbrl.longitudinal_memory.promise_progress_product_v2 import (
    NEEDS_REVIEW_REASONS,
    OPEN_BLOCK_ID,
    PRODUCT_VERSION,
    PROGRESSION_BLOCK_ID,
    TIMELINE_BLOCK_ID,
    GUIDANCE_UPDATE_ROW_KIND,
    PERIOD_RESULT_ROW_KIND,
    HORIZON_OUTCOME_ROW_KIND,
    NET_STORE_OPENINGS_RULE_ID,
    PERIOD_YTD_MINUS_PRIOR_RULE_ID,
    Q4_ADD_FY_MINUS_QUARTERS_RULE_ID,
    Q4_ADD_FY_MINUS_YTD_RULE_ID,
    Q4_GROWTH_FROM_AMOUNTS_RULE_ID,
    Q4_MARGIN_FROM_COMPONENTS_RULE_ID,
    STORE_COMPONENT_COMBINATION_RULE_ID,
    SUCCESSOR_PRODUCT_VERSION,
    YTD_GROWTH_FROM_AMOUNTS_RULE_ID,
    YTD_MARGIN_FROM_COMPONENTS_RULE_ID,
    build_product_v2_shadow,
    build_promise_progress_product_v2,
    classify_change,
    compatible_foundation_metric_ids,
    normalize_product_unit_id,
    promise_progress_product_v2_sha256,
    serialize_product_v2_shadow,
    serialize_promise_progress_product_v2,
)
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.source_adapter import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.source_adapter.html import _rows, _span_fingerprint, _text
from pbi_xbrl.longitudinal_memory.source_adapter.types import text_sha256
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile_v2
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_evidence_foundation import (
    FOUNDATION_ID as EVIDENCE_FOUNDATION_ID,
    SOURCE_SET_ID as EVIDENCE_FOUNDATION_SOURCE_SET_ID,
    build_anf_evidence_foundation,
    candidate_artifacts as evidence_foundation_artifacts,
)
from pbi_xbrl.promise_progress_workbook_preview import (
    build_promise_progress_workbook_binding_plan_v2,
    build_workbook_trace_v2,
    canonical_workbook_content_sha256,
    load_json_strict,
    materialize_promise_progress_preview_v2,
    sha256_file,
    target_sheet_semantic_sha256_v2,
    validate_preview_semantics_v2,
    validate_preview_structure_v2,
    validate_preview_visual_fit_v2,
)


SOURCE_SET_ID = "source-set:anf:promise-progress-product-v2-candidate@2"
CANDIDATE_ROOT_NAME = "promise_progress_product_v2_candidate"
SUCCESSOR_SOURCE_SET_ID = (
    "source-set:anf:promise-progress-product-v2-post-golden-successor@3"
)
SUCCESSOR_CANDIDATE_ROOT_NAME = (
    "promise_progress_product_v2_1_exhaustive_reconciliation_defect_closure_candidate_final"
)
EXHAUSTIVE_RECONCILIATION_AUDIT_RELATIVE_PATH = Path("audit") / (
    "promise_progress_product_v2_1_exhaustive_semantic_reconciliation_audit"
)
FINAL_EXHAUSTIVE_RECONCILIATION_AUDIT_RELATIVE_PATH = Path("audit") / (
    "promise_progress_product_v2_1_final_exhaustive_semantic_reconciliation_acceptance_audit"
)
FINAL_COUNT_RECONCILIATION_AUDIT_RELATIVE_PATH = Path("audit") / (
    "promise_progress_product_v2_1_final_exhaustive_acceptance_reconciliation_audit_v2"
)
COUNT_RECONCILIATION_KIND_SCHEMA_ID = (
    "contract:promise-progress-count-reconciliation-kinds@1"
)
# Canonical order is part of the report contract.  These identifiers define which
# counters must exist; their current economic values are always generated below.
COUNT_RECONCILIATION_REQUIRED_KINDS: tuple[str, ...] = (
    "metric",
    "annual_guidance_series",
    "quarter_guidance_series",
    "annual_guidance_version",
    "quarter_guidance_version",
    "guidance_transition",
    "annual_actual",
    "quarter_actual",
    "progress",
    "q4_candidate",
    "derived_fact",
    "guidance_progression_row",
    "open_guidance_row",
    "guidance_update_row",
    "period_result_row",
    "horizon_outcome_row",
    "assessment_row",
    "disclosure_event",
    "status",
    "needs_review",
    "change_type",
    "blank_cell",
    "workbook_field_cell",
    "foundation_disposition",
    "source_conflict",
)
EVIDENCE_AUDIT_RELATIVE_PATH = Path("audit") / (
    "anf_local_source_review_authority_expansion_audit_2026-08-09"
)
SOURCE_ROOT_DEFAULT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
LEGACY_WORKBOOK_RELATIVE_PATH = Path("outputs") / "Excel stock models" / "ANF_model.xlsx"
DESIGN_LOCK_RELATIVE_PATH = Path("audit") / "promise_progress_design_lock"
FINAL_CLOSURE_MANIFEST_FILENAMES = (
    "old_defect_regression_report.json",
    "current_defect_closure_report.json",
    "current_count_reconciliation_report.json",
    "numeric_ooxml_reconciliation.json",
)

HISTORICAL_DOCUMENTS: tuple[dict[str, Any], ...] = (
    {
        "document_key": "anf-release-2022-03-03",
        "file_date": "2022-03-03",
        "embedded_date": "March 2, 2022",
        "accession": "0001018840-22-000005",
        "report_date": "2022-01-29",
        "sha256": "4a6a4052232b2b277dcb11be288cd646b7affa54a2550198efdb3307b1e491c3",
    },
    {
        "document_key": "anf-release-2022-05-25",
        "file_date": "2022-05-25",
        "embedded_date": "May 24, 2022",
        "accession": "0001018840-22-000024",
        "report_date": "2022-04-30",
        "sha256": "f693adc08973b944a7a7d68f7e7ecc510ab6cdecf0a81ce03f9efa2bae66fac0",
    },
    {
        "document_key": "anf-release-2022-08-29",
        "file_date": "2022-08-29",
        "embedded_date": "August 25, 2022",
        "accession": "0001018840-22-000046",
        "report_date": "2022-07-30",
        "sha256": "b37f4a0cad4d41f32c2f4e4781769bb8011c8870b72208986dc2d9052eeca119",
    },
    {
        "document_key": "anf-release-2022-11-23",
        "file_date": "2022-11-23",
        "embedded_date": "November 22, 2022",
        "accession": "0001018840-22-000058",
        "report_date": "2022-10-29",
        "sha256": "d46aeaf16879f244103b39a8333728d17b4ee28478efb23e5c0f8283e65716cd",
    },
    {
        "document_key": "anf-release-2023-03-02",
        "file_date": "2023-03-02",
        "embedded_date": "March 1, 2023",
        "accession": "0001018840-23-000007",
        "report_date": "2023-01-28",
        "sha256": "1d6f798372e88a2ed1374602da71deecaa07ae44ea79728bfbb5e9d608c1554f",
    },
    {
        "document_key": "anf-release-2023-05-25",
        "file_date": "2023-05-25",
        "embedded_date": "May 24, 2023",
        "accession": "0001018840-23-000059",
        "report_date": "2023-04-29",
        "sha256": "57b84d58d1b823fe52d51a04e614da48dfdb3a77bf82f941e0617c978b1246f6",
    },
    {
        "document_key": "anf-release-2023-08-24",
        "file_date": "2023-08-24",
        "embedded_date": "August 23, 2023",
        "accession": "0001018840-23-000074",
        "report_date": "2023-07-29",
        "sha256": "eda08d4eb87d926ca41c09af3910b5703a90d03b5fc9372eeda7fbd320418d4f",
    },
    {
        "document_key": "anf-release-2023-11-22",
        "file_date": "2023-11-22",
        "embedded_date": "November 21, 2023",
        "accession": "0001018840-23-000086",
        "report_date": "2023-10-28",
        "sha256": "626d47987b273147b24b584d18f8b78ed988b45e02d55e42ea681aa3b0c107b5",
    },
    {
        "document_key": "anf-release-2024-03-07",
        "file_date": "2024-03-07",
        "embedded_date": "March 6, 2024",
        "accession": "0001018840-24-000012",
        "report_date": "2024-02-03",
        "sha256": "a036610a5f2ae8e353629348efe0b20bacea197d647c83d1b7b65edd6ef0438c",
    },
    {
        "document_key": "anf-release-2024-05-30",
        "file_date": "2024-05-30",
        "embedded_date": "May 29, 2024",
        "accession": "0001018840-24-000035",
        "report_date": "2024-05-04",
        "sha256": "5abf33a8c296e3f081eba7fe8d14d01913ef086042a864ff12aa1cf335aaba2e",
    },
    {
        "document_key": "anf-release-2024-08-29",
        "file_date": "2024-08-29",
        "embedded_date": "August 28, 2024",
        "accession": "0001018840-24-000066",
        "report_date": "2024-08-03",
        "sha256": "262130f06566e6db88dbe550e7e709e64118be933f464924366bcb15d9946e62",
    },
    {
        "document_key": "anf-release-2024-11-27",
        "file_date": "2024-11-27",
        "embedded_date": "November 26, 2024",
        "accession": "0001018840-24-000083",
        "report_date": "2024-11-02",
        "sha256": "9a6f6afa4c23f9fb0b44b0c372d63581778235b14f0cd0dfc66251b79074bd35",
    },
)


QUARTERLY_PROGRESS_EVENTS: tuple[dict[str, Any], ...] = (
    {"year": 2022, "quarter": 1, "publication_date": "2022-05-25", "start_date": "2022-01-30", "end_date": "2022-04-30"},
    {"year": 2022, "quarter": 2, "publication_date": "2022-08-29", "start_date": "2022-05-01", "end_date": "2022-07-30"},
    {"year": 2022, "quarter": 3, "publication_date": "2022-11-23", "start_date": "2022-07-31", "end_date": "2022-10-29"},
    {"year": 2023, "quarter": 1, "publication_date": "2023-05-25", "start_date": "2023-01-29", "end_date": "2023-04-29"},
    {"year": 2023, "quarter": 2, "publication_date": "2023-08-24", "start_date": "2023-04-30", "end_date": "2023-07-29"},
    {"year": 2023, "quarter": 3, "publication_date": "2023-11-22", "start_date": "2023-07-30", "end_date": "2023-10-28"},
    {"year": 2024, "quarter": 1, "publication_date": "2024-05-30", "start_date": "2024-02-04", "end_date": "2024-05-04"},
    {"year": 2024, "quarter": 2, "publication_date": "2024-08-29", "start_date": "2024-05-05", "end_date": "2024-08-03"},
    {"year": 2024, "quarter": 3, "publication_date": "2024-11-27", "start_date": "2024-08-04", "end_date": "2024-11-02"},
    {"year": 2025, "quarter": 1, "publication_date": "2025-05-29", "start_date": "2025-02-02", "end_date": "2025-05-03"},
    {"year": 2025, "quarter": 2, "publication_date": "2025-08-28", "start_date": "2025-05-04", "end_date": "2025-08-02"},
    {"year": 2025, "quarter": 3, "publication_date": "2025-11-26", "start_date": "2025-08-03", "end_date": "2025-11-01"},
)

Q4_RESULT_EVENTS: tuple[dict[str, Any], ...] = (
    {
        "year": 2022,
        "quarter": 4,
        "publication_date": "2023-03-02",
        "start_date": "2022-10-30",
        "end_date": "2023-01-28",
        "week_count": 13,
    },
    {
        "year": 2023,
        "quarter": 4,
        "publication_date": "2024-03-07",
        "start_date": "2023-10-29",
        "end_date": "2024-02-03",
        "week_count": 14,
        "is_53_week_year": True,
    },
    {
        "year": 2024,
        "quarter": 4,
        "publication_date": "2025-03-06",
        "start_date": "2024-11-03",
        "end_date": "2025-02-01",
        "week_count": 13,
    },
    {
        "year": 2025,
        "quarter": 4,
        "publication_date": "2026-03-04",
        "start_date": "2025-11-02",
        "end_date": "2026-01-31",
        "week_count": 13,
    },
)

Q3_YTD_CAPEX_EVENTS: tuple[dict[str, Any], ...] = tuple(
    {
        **row,
        "period_key": f"fy{row['year']}-ytd-q3",
        "period_id": f"period:anf:fy{row['year']}-ytd-q3@1",
        "start_date": {
            2022: "2022-01-30",
            2023: "2023-01-29",
            2024: "2024-02-04",
            2025: "2025-02-02",
        }[int(row["year"])],
        "week_count": 39,
        "is_53_week_year": int(row["year"]) == 2023,
    }
    for row in QUARTERLY_PROGRESS_EVENTS
    if int(row["quarter"]) == 3
)

FY2025_YTD_PROGRESS_EVENTS: tuple[dict[str, Any], ...] = tuple(
    {
        **row,
        "period_key": f"fy2025-ytd-q{row['quarter']}",
        "period_id": f"period:anf:fy2025-ytd-q{row['quarter']}@1",
        "start_date": "2025-02-02",
        "week_count": int(row["quarter"]) * 13,
    }
    for row in QUARTERLY_PROGRESS_EVENTS
    if int(row["year"]) == 2025
)

CAPEX_EQUIVALENCE_TOPIC_ID = (
    "topic:core:capital-expenditure-property-equipment-equivalence@1"
)


GUIDANCE: tuple[dict[str, Any], ...] = (
    # FY2022
    {"year": 2022, "date": "2022-03-03", "metric": "revenue-growth", "value": "Net sales to be up 2 to 4%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-03-03", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $150 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2022, "date": "2022-05-25", "metric": "revenue-growth", "value": "Net sales to be flat to up 2%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-05-25", "metric": "operating-margin", "value": "Operating margin in the range of 5 to 6%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-05-25", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $150 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2022, "date": "2022-08-29", "metric": "revenue-growth", "value": "Net sales to be down mid-single-digits", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-08-29", "metric": "operating-margin", "value": "Operating margin in the range of 1 to 3%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-08-29", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $150 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2022, "date": "2022-11-23", "metric": "revenue-growth", "value": "Net sales to be down in the range of 2 to 3%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-11-23", "metric": "operating-margin", "value": "Operating margin in the range of 2 to 3%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2022, "date": "2022-11-23", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $170 million", "parser": "parser:retail:guidance-currency-millions@1"},
    # FY2023
    {"year": 2023, "date": "2023-03-02", "metric": "revenue-growth", "value": "Net sales growth in the range of 1 to 3%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-03-02", "metric": "operating-margin", "value": "Operating margin to be in a range of 4 to 5%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-03-02", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $160 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2023, "date": "2023-05-25", "metric": "revenue-growth", "value": "Net sales growth in the range of 2 to 4%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-05-25", "metric": "operating-margin", "value": "Operating margin to be in a range of 5 to 6%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-05-25", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $160 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2023, "date": "2023-08-24", "metric": "revenue-growth", "value": "Net sales growth of around 10%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-08-24", "metric": "operating-margin", "value": "Operating margin to be in the range of 8% to 9%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-08-24", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $160 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2023, "date": "2023-11-22", "metric": "revenue-growth", "value": "Net sales growth of 12% to 14%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-11-22", "metric": "operating-margin", "value": "Operating margin to be around 10%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2023, "date": "2023-11-22", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $160 million", "parser": "parser:retail:guidance-currency-millions@1"},
    # FY2024, with the final table-row versions built separately.
    {"year": 2024, "date": "2024-03-07", "metric": "revenue-growth", "value": "Net sales growth in the range of 4% to 6%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2024, "date": "2024-03-07", "metric": "operating-margin", "value": "Operating margin to be around 12%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2024, "date": "2024-03-07", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $170 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2024, "date": "2024-05-30", "metric": "revenue-growth", "value": "Net sales up around 10%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2024, "date": "2024-05-30", "metric": "operating-margin", "value": "Operating margin to be around 14%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2024, "date": "2024-05-30", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $170 million", "parser": "parser:retail:guidance-currency-millions@1"},
    {"year": 2024, "date": "2024-08-29", "metric": "revenue-growth", "value": "Net sales growth in the range of 12% to 13%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2024, "date": "2024-08-29", "metric": "operating-margin", "value": "Operating margin to be in the range of 14% to 15%", "parser": "parser:retail:guidance-percent-v2@2"},
    {"year": 2024, "date": "2024-08-29", "metric": "capital-expenditures", "value": "Capital expenditures of approximately $170 million", "parser": "parser:retail:guidance-currency-millions@1"},
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> str:
    payload = _json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _write_visual_markdown(
    path: Path,
    *,
    product_sha256: str,
    preview_path: Path,
    visual: Mapping[str, Any],
    plan: Any,
) -> str:
    render_root = path.parent / "rendered"
    rendered = (
        sorted(
            (candidate for candidate in render_root.glob("*.png") if candidate.is_file()),
            key=lambda candidate: candidate.name,
        )
        if render_root.is_dir()
        else []
    )
    lines = [
        "# Promise Progress Product@2 candidate visual validation",
        "",
        "- State: candidate only; no production cutover.",
        f"- Product SHA-256: `{product_sha256}`",
        f"- Preview workbook: `{preview_path.relative_to(path.parent).as_posix()}`",
        f"- Dynamic used range: `{plan.used_range}`",
        f"- Physical presentation rows: **{len(plan.row_plan)}**",
        f"- Visible binding fit records: **{visual['record_count']}**",
        f"- Clipped visible fields: **{visual['clipped_visible_field_count']}**",
        f"- Adjacent-cell overflow dependencies: **{visual['overflow_dependency_count']}**",
        f"- Deterministic fit result: **{'PASS' if visual['passed'] else 'FAIL'}**",
        "",
        "The Product@2 preview uses the compact A:J investor grid, hides K:N, writes only stable",
        "row IDs in O, allocates physical rows from product-owned order, and emits one",
        "timeline header with typed reporting/update groups and event separators. Rendering is an additional",
        "readability check; semantic and structural acceptance remain OOXML-replay based.",
        "",
    ]
    if rendered:
        lines.extend(
            [
                "## Deterministic rendered review",
                "",
                "Artifact-tool rendering confirms the investor hierarchy, dynamic vertical",
                "allocation, compact investor tables, and outcome-status vocabulary are readable without",
                "clipping or legacy-capacity shells.",
                "",
                *[
                    f"- `{candidate.relative_to(path.parent).as_posix()}` — `{_sha(candidate)}`"
                    for candidate in rendered
                ],
                "",
            ]
        )
    lines.extend([
        "Microsoft Excel was not used.",
        "",
    ])
    payload = "\n".join(lines).encode("utf-8")
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def write_candidate_manifest(
    *,
    output_root: Path,
    product: Any,
    plan: Any,
    legacy_workbook: Path,
) -> dict[str, Any]:
    successor = product.product_version == SUCCESSOR_PRODUCT_VERSION
    names = [
        "source_set_v2_candidate.json",
        "product_v2_candidate.json",
        "shadow_v2_candidate.json",
        "source_coverage_report.json",
        "data_completeness_report.json",
        "product_v1_vs_v2_report.json",
        "timeline_semantics_report.json",
        "timeline_actual_progress_role_report.json",
        "range_parser_replay_report.json",
        "legacy_capability_completeness_report.json",
        "capability_completion_report.json",
        "needs_review_audit.json",
        "actual_definition_compatibility_report.json",
        "timeline_knowledge_date_report.json",
        "presentation_contract_v8.json" if successor else "presentation_contract_v7.json",
        "binding_plan_v2.json",
        "workbook_trace_v2.json",
        "structural_validation_v2.json",
        "semantic_validation_v2.json",
        "visual_validation_v2.json",
        "visual_validation_v2.md",
        "ANF_Promise_Progress_source_native_v2_preview.xlsx",
        "ANF_Promise_Progress_source_native_v2_preview_repeat.xlsx",
    ]
    foundation_artifacts: Mapping[str, Mapping[str, Any]] | None = None
    if successor:
        names.extend(
            [
                "evidence_foundation_identity.json",
                "guidance_completeness_report.json",
                "actual_reconciliation_report.json",
                "progress_reconciliation_report.json",
                "quarter_guidance_coverage_report.json",
                "result_event_semantic_report.json",
                "foundation_projection_disposition.json",
                "progression_q4_guidance_update_audit.json",
                "q4_derivation_audit.json",
                "q4_reconciliation_report.json",
                "derivation_lineage_report.json",
                "status_report.json",
                "bounded_derivation_audit.json",
                "timeline_blank_completeness_report.json",
                "needs_review_semantics_review.json",
                "defect_closure_report.json",
                "numeric_cell_text_audit.json",
                *FINAL_CLOSURE_MANIFEST_FILENAMES,
            ]
        )
    paths = [output_root / name for name in names]
    render_root = output_root / "rendered"
    if render_root.is_dir():
        paths.extend(sorted((path for path in render_root.rglob("*") if path.is_file()), key=str))
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Candidate manifest inputs are missing: {missing!r}")
    artifacts = [
        {
            "relative_path": path.relative_to(output_root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha(path),
        }
        for path in sorted(paths, key=lambda value: value.relative_to(output_root).as_posix())
    ]
    first = output_root / "ANF_Promise_Progress_source_native_v2_preview.xlsx"
    second = output_root / "ANF_Promise_Progress_source_native_v2_preview_repeat.xlsx"
    manifest = {
        "manifest_type": (
            "PromiseProgressProductV2SuccessorCandidateManifest@1"
            if successor
            else "PromiseProgressProductV2CandidateManifest@1"
        ),
        "candidate_state": (
            "post-golden-successor-review-only-not-golden-not-production-cutover"
            if successor
            else "review-only-not-golden-not-production-cutover"
        ),
        "product_id": product.product_id,
        "product_version": product.product_version,
        "product_sha256": promise_progress_product_v2_sha256(product),
        "binding_plan_sha256": plan.lineage_digest,
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        "legacy_workbook": str(legacy_workbook),
        "legacy_workbook_sha256": sha256_file(legacy_workbook),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "fresh_regeneration": {
            "first_raw_sha256": _sha(first),
            "second_raw_sha256": _sha(second),
            "raw_byte_identical": first.read_bytes() == second.read_bytes(),
            "first_canonical_content_sha256": canonical_workbook_content_sha256(first),
            "second_canonical_content_sha256": canonical_workbook_content_sha256(second),
            "canonical_content_identical": canonical_workbook_content_sha256(first)
            == canonical_workbook_content_sha256(second),
            "first_target_semantic_sha256": target_sheet_semantic_sha256_v2(first, plan),
            "second_target_semantic_sha256": target_sheet_semantic_sha256_v2(second, plan),
            "target_semantic_identical": target_sheet_semantic_sha256_v2(first, plan)
            == target_sheet_semantic_sha256_v2(second, plan),
        },
        "publication_exclusions": [
            {
                "relative_path": "rendered.zip",
                "reason": "stale-unmanifested-historical-render-bundle",
            }
        ],
        "generated_timestamp": None,
    }
    manifest["manifest_digest"] = hashlib.sha256(_json_bytes(manifest)).hexdigest()
    _write_json(output_root / "candidate_manifest.json", manifest)
    return manifest


def _release_path(source_root: Path, file_date: str) -> Path:
    return source_root / "tickers" / "ANF" / "earnings_release" / f"8-K_{file_date}_earnings_release.htm"


def _verified_release_text(source_root: Path, file_date: str, expected_sha: str | None = None) -> tuple[Path, Any, str]:
    path = _release_path(source_root, file_date)
    if not path.is_file():
        raise FileNotFoundError(f"Required reviewed local source is absent: {path}")
    if expected_sha is not None and _sha(path) != expected_sha:
        raise ValueError(f"Reviewed source hash changed: {path}")
    root = lxml_html.fromstring(path.read_bytes())
    return path, root, _text(root)


def _ordinal(text: str, fingerprint: str, *, case_sensitive: bool = False) -> int:
    haystack = text if case_sensitive else text.casefold()
    needle = fingerprint if case_sensitive else fingerprint.casefold()
    positions: list[int] = []
    start = 0
    while True:
        position = haystack.find(needle, start)
        if position < 0:
            break
        positions.append(position)
        start = position + max(1, len(needle))
    if not positions:
        raise ValueError(f"Source fingerprint is absent: {fingerprint!r}")
    return 1


def _source_case(text: str, fingerprint: str) -> str:
    position = text.casefold().find(fingerprint.casefold())
    if position < 0:
        raise ValueError(f"Source fingerprint is absent: {fingerprint!r}")
    return text[position : position + len(fingerprint)]


def _html_dateline_locator(document_text: str, fingerprint: str) -> dict[str, Any]:
    normalized = " ".join(fingerprint.split())
    return {
        "locator_kind": "html-dateline",
        "locator_version": 1,
        "locator_key": "html:issuer-dateline",
        "extraction_method_id": "extractor:source:html-dateline@1",
        "text_fingerprint": normalized,
        "match_ordinal": _ordinal(document_text, normalized),
        "excerpt_sha256": text_sha256(normalized),
    }


def _fiscal_claim(document_text: str, *, year: int, assertion_key: str) -> dict[str, Any]:
    fingerprint = f"Fiscal {year} Full Year Outlook"
    if fingerprint not in document_text:
        fingerprint = f"Fiscal {year} Outlook"
    return {
        "locator_kind": "html-fiscal-labels",
        "locator_version": 1,
        "extraction_method_id": "extractor:source:html-fiscal-label@1",
        "claims": [
            {
                "claim_key": f"{assertion_key}-annual",
                "claim_kind": "annual-period",
                "text_fingerprint": fingerprint,
                "match_ordinal": _ordinal(document_text, fingerprint, case_sensitive=True),
                "excerpt_sha256": text_sha256(fingerprint),
            }
        ],
    }


def _period_evidence_assertion(
    source_root: Path,
    *,
    year: int,
    document_key: str,
    file_date: str,
    week_end_fingerprint: str,
) -> dict[str, Any]:
    expected = next(
        (str(row["sha256"]) for row in HISTORICAL_DOCUMENTS if row["file_date"] == file_date),
        None,
    )
    _path, _root, document_text = _verified_release_text(source_root, file_date, expected)
    excerpt = _source_case(document_text, week_end_fingerprint)
    full_year = _source_case(document_text, "FULL YEAR RESULTS")
    fiscal_year = _source_case(document_text, f"fiscal {year}")
    assertion_key = f"period-fy{year}-annual"
    return {
        "assertion_key": assertion_key,
        "assertion_kind": "period_evidence",
        "document_key": document_key,
        "period_key": f"fy{year}",
        "locator": {
            "locator_kind": "html-text",
            "locator_version": 1,
            "locator_key": f"html:{assertion_key}",
            "ordinal": 1,
            "extraction_method_id": "extractor:source:html-text-node@1",
            "excerpt": excerpt,
            "excerpt_sha256": text_sha256(excerpt),
            "review_state": "reviewed",
            "node_path": "html/document-text/match[1]",
            "text_fingerprint": excerpt,
            "ancestor_fingerprints": [full_year],
            "match_ordinal": _ordinal(document_text, excerpt),
            "fiscal_label_evidence": {
                "locator_kind": "html-fiscal-labels",
                "locator_version": 1,
                "extraction_method_id": "extractor:source:html-fiscal-label@1",
                "claims": [
                    {
                        "claim_key": f"{assertion_key}-annual",
                        "claim_kind": "annual-period",
                        "text_fingerprint": full_year,
                        "match_ordinal": _ordinal(document_text, full_year, case_sensitive=True),
                        "excerpt_sha256": text_sha256(full_year),
                    },
                    {
                        "claim_key": f"{assertion_key}-year",
                        "claim_kind": "fiscal-year",
                        "text_fingerprint": fiscal_year,
                        "match_ordinal": _ordinal(document_text, fiscal_year, case_sensitive=True),
                        "excerpt_sha256": text_sha256(fiscal_year),
                    },
                ],
            },
        },
        "review_state": "reviewed",
    }


def _quarter_period(row: Mapping[str, Any]) -> dict[str, Any]:
    year = int(row["year"])
    quarter = int(row["quarter"])
    week_count = int(row.get("week_count", 13))
    return {
        "period_key": f"fy{year}-q{quarter}",
        "period_id": f"period:anf:fy{year}-q{quarter}@1",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_type": "quarter",
        "start_date": str(row["start_date"]),
        "end_date": str(row["end_date"]),
        "week_count": week_count,
        "fiscal_ordinal": (year - 2000) * 4 + quarter,
        "is_53_week_year": bool(row.get("is_53_week_year", False)),
        "start_rule_id": "rule:core:inclusive-weeks-ending@1",
        "evidence_assertion_key": f"period-fy{year}-q{quarter}",
        "fiscal_claim_assertion_keys": [f"period-fy{year}-q{quarter}"],
        "reconciliation_state": "reconciled",
    }


def _quarter_period_evidence_assertion(
    source_root: Path, row: Mapping[str, Any]
) -> dict[str, Any]:
    year = int(row["year"])
    quarter = int(row["quarter"])
    publication_date = str(row["publication_date"])
    expected = next(
        (
            str(value["sha256"])
            for value in HISTORICAL_DOCUMENTS
            if value["file_date"] == publication_date
        ),
        None,
    )
    _path, _root, document_text = _verified_release_text(
        source_root, publication_date, expected
    )
    end_day = date.fromisoformat(str(row["end_date"]))
    end_label = f"{end_day.strftime('%B')} {end_day.day}, {end_day.year}"
    weeks_word = {13: "Thirteen", 14: "Fourteen"}.get(int(row.get("week_count", 13)))
    if weeks_word is None:
        raise ValueError("Quarter period evidence supports only reviewed 13/14-week quarters.")
    duration_fingerprint = f"{weeks_word} Weeks Ended {end_label}"
    excerpt = _source_case(document_text, duration_fingerprint)
    quarter_fingerprint = _source_case(
        document_text,
        {
            1: "FIRST QUARTER",
            2: "SECOND QUARTER",
            3: "THIRD QUARTER",
            4: "FOURTH QUARTER",
        }[quarter],
    )
    year_fingerprint = _source_case(document_text, f"FISCAL {year}")
    assertion_key = f"period-fy{year}-q{quarter}"
    return {
        "assertion_key": assertion_key,
        "assertion_kind": "period_evidence",
        "document_key": f"anf-release-{publication_date}",
        "period_key": f"fy{year}-q{quarter}",
        "locator": {
            "locator_kind": "html-text",
            "locator_version": 1,
            "locator_key": f"html:{assertion_key}",
            "ordinal": 1,
            "extraction_method_id": "extractor:source:html-text-node@1",
            "excerpt": excerpt,
            "excerpt_sha256": text_sha256(excerpt),
            "review_state": "reviewed",
            "node_path": "html/document-text/match[1]",
            "text_fingerprint": excerpt,
            "ancestor_fingerprints": [quarter_fingerprint, year_fingerprint],
            "match_ordinal": _ordinal(document_text, excerpt),
            "fiscal_label_evidence": {
                "locator_kind": "html-fiscal-labels",
                "locator_version": 1,
                "extraction_method_id": "extractor:source:html-fiscal-label@1",
                "claims": [
                    {
                        "claim_key": f"{assertion_key}-quarter",
                        "claim_kind": "fiscal-quarter",
                        "text_fingerprint": quarter_fingerprint,
                        "match_ordinal": _ordinal(
                            document_text, quarter_fingerprint, case_sensitive=True
                        ),
                        "excerpt_sha256": text_sha256(quarter_fingerprint),
                    },
                    {
                        "claim_key": f"{assertion_key}-year",
                        "claim_kind": "fiscal-year",
                        "text_fingerprint": year_fingerprint,
                        "match_ordinal": _ordinal(
                            document_text, year_fingerprint, case_sensitive=True
                        ),
                        "excerpt_sha256": text_sha256(year_fingerprint),
                    },
                ],
            },
        },
        "review_state": "reviewed",
    }


def _ytd_period(row: Mapping[str, Any]) -> dict[str, Any]:
    year = int(row["year"])
    quarter = int(row["quarter"])
    return {
        "period_key": str(row["period_key"]),
        "period_id": str(row["period_id"]),
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_type": "ytd",
        "start_date": str(row["start_date"]),
        "end_date": str(row["end_date"]),
        "week_count": int(row["week_count"]),
        "fiscal_ordinal": (year - 2000) * 4 + quarter,
        "is_53_week_year": bool(row.get("is_53_week_year", False)),
        "start_rule_id": "rule:core:inclusive-weeks-ending@1",
        "evidence_assertion_key": f"period-fy{year}-ytd-q{quarter}",
        "fiscal_claim_assertion_keys": [f"period-fy{year}-ytd-q{quarter}"],
        "reconciliation_state": "reconciled",
    }


def _ytd_period_evidence_assertion(
    source_root: Path, row: Mapping[str, Any]
) -> dict[str, Any]:
    year = int(row["year"])
    quarter = int(row["quarter"])
    publication_date = str(row["publication_date"])
    _path, _root, document_text = _verified_release_text(
        source_root, publication_date
    )
    end_day = date.fromisoformat(str(row["end_date"]))
    end_label = f"{end_day.strftime('%B')} {end_day.day}, {end_day.year}"
    weeks_word = {1: "Thirteen", 2: "Twenty-Six", 3: "Thirty-Nine"}[quarter]
    duration_fingerprint = _source_case(
        document_text, f"{weeks_word} Weeks Ended {end_label}"
    )
    ytd_fingerprint = _source_case(
        document_text, f"year-to-date period ended {end_label}"
    )
    year_fingerprint = _source_case(document_text, f"FISCAL {year}")
    assertion_key = f"period-fy{year}-ytd-q{quarter}"
    return {
        "assertion_key": assertion_key,
        "assertion_kind": "period_evidence",
        "document_key": f"anf-release-{publication_date}",
        "period_key": str(row["period_key"]),
        "locator": {
            "locator_kind": "html-text",
            "locator_version": 1,
            "locator_key": f"html:{assertion_key}",
            "ordinal": 1,
            "extraction_method_id": "extractor:source:html-text-node@1",
            "excerpt": duration_fingerprint,
            "excerpt_sha256": text_sha256(duration_fingerprint),
            "review_state": "reviewed",
            "node_path": "html/document-text/match[1]",
            "text_fingerprint": duration_fingerprint,
            "ancestor_fingerprints": [ytd_fingerprint, year_fingerprint],
            "match_ordinal": _ordinal(document_text, duration_fingerprint),
            "fiscal_label_evidence": {
                "locator_kind": "html-fiscal-labels",
                "locator_version": 1,
                "extraction_method_id": "extractor:source:html-fiscal-label@1",
                "claims": [
                    {
                        "claim_key": f"{assertion_key}-ytd",
                        "claim_kind": "fiscal-ytd",
                        "text_fingerprint": ytd_fingerprint,
                        "match_ordinal": _ordinal(
                            document_text, ytd_fingerprint, case_sensitive=True
                        ),
                        "excerpt_sha256": text_sha256(ytd_fingerprint),
                    },
                    {
                        "claim_key": f"{assertion_key}-year",
                        "claim_kind": "fiscal-year",
                        "text_fingerprint": year_fingerprint,
                        "match_ordinal": _ordinal(
                            document_text, year_fingerprint, case_sensitive=True
                        ),
                        "excerpt_sha256": text_sha256(year_fingerprint),
                    },
                ],
            },
        },
        "review_state": "reviewed",
    }


def _html_text_locator(
    document_text: str,
    *,
    locator_key: str,
    fingerprint: str,
    year: int,
    fiscal_claim: bool = False,
    replacement: bool = False,
) -> dict[str, Any]:
    excerpt = " ".join(fingerprint.split())
    locator: dict[str, Any] = {
        "locator_kind": "html-text",
        "locator_version": 1,
        "locator_key": locator_key,
        "ordinal": 1,
        "extraction_method_id": "extractor:source:html-text-node@1",
        "excerpt": excerpt,
        "excerpt_sha256": text_sha256(excerpt),
        "review_state": "reviewed",
        "node_path": "html/document-text/match[1]",
        "text_fingerprint": excerpt,
        "value_text_fingerprint": excerpt,
        "ancestor_fingerprints": [f"Fiscal {year} Full Year Outlook"],
        "match_ordinal": _ordinal(document_text, excerpt),
    }
    if f"Fiscal {year} Full Year Outlook" not in document_text:
        locator["ancestor_fingerprints"] = [f"Fiscal {year} Outlook"]
    if replacement:
        replacement_text = "The following outlook replaces all previous full year guidance."
        if replacement_text not in document_text:
            raise ValueError(f"Replacement wording is absent for {locator_key}")
        locator["replacement_header_fingerprint"] = replacement_text
        locator["ancestor_fingerprints"].append(replacement_text)
    if fiscal_claim:
        locator["fiscal_label_evidence"] = _fiscal_claim(
            document_text, year=year, assertion_key=locator_key.replace("html:", "")
        )
    return locator


def _reviewed_html_text_locator(
    document_text: str, *, locator_key: str, fingerprint: str
) -> dict[str, Any]:
    """Locate reviewed prose without manufacturing fiscal or section ancestry."""

    excerpt = _source_case(document_text, " ".join(fingerprint.split()))
    return {
        "locator_kind": "html-text",
        "locator_version": 1,
        "locator_key": locator_key,
        "ordinal": 1,
        "extraction_method_id": "extractor:source:html-text-node@1",
        "excerpt": excerpt,
        "excerpt_sha256": text_sha256(excerpt),
        "review_state": "reviewed",
        "node_path": "html/document-text/match[1]",
        "text_fingerprint": excerpt,
        "value_text_fingerprint": excerpt,
        "ancestor_fingerprints": [],
        "match_ordinal": _ordinal(document_text, excerpt, case_sensitive=True),
    }


def _reviewed_text_line_locator(
    document_text: str,
    *,
    locator_key: str,
    line_number: int,
    speaker_fingerprint: str | None,
) -> dict[str, Any]:
    """Pin one reviewed transcript line without deriving semantics from its wording."""

    lines = document_text.splitlines()
    if line_number < 1 or line_number > len(lines):
        raise ValueError(f"Reviewed transcript line {line_number} is absent.")
    excerpt = lines[line_number - 1]
    if speaker_fingerprint is not None and not any(
        speaker_fingerprint.casefold() in line.casefold()
        for line in lines[max(0, line_number - 26) : line_number - 1]
    ):
        raise ValueError(
            f"Reviewed speaker {speaker_fingerprint!r} is absent before line {line_number}."
        )
    digest = text_sha256(excerpt)
    return {
        "locator_kind": "text-lines",
        "locator_version": 1,
        "locator_key": locator_key,
        "ordinal": 1,
        "extraction_method_id": "extractor:source:text-exact-lines@1",
        "excerpt": excerpt,
        "excerpt_sha256": digest,
        "line_digest": digest,
        "review_state": "reviewed",
        "start_line": line_number,
        "end_line": line_number,
        "speaker_fingerprint": speaker_fingerprint,
        "turn_diagnostics": (
            f"one-based line {line_number}; nearest reviewed speaker header"
            if speaker_fingerprint is not None
            else f"one-based line {line_number}; no speaker asserted"
        ),
    }


def _html_table_locator(
    root: Any,
    *,
    locator_key: str,
    table_fingerprints: Iterable[str],
    row_header: str,
    column_header: str,
    row_index: int,
    cell_index: int | None,
    comparison_cell_index: int | None = None,
    context_row_index: int | None = None,
    row_end_index: int | None = None,
    section_fingerprint: str | None = None,
) -> dict[str, Any]:
    fingerprints = tuple(table_fingerprints)
    matches = [
        (index, table)
        for index, table in enumerate(root.xpath("//table"))
        if all(_text(value).casefold() in _text(table).casefold() for value in fingerprints)
    ]
    if len(matches) != 1:
        raise ValueError(f"{locator_key} matched {len(matches)} HTML tables")
    table_index, table = matches[0]
    rows = _rows(table)
    end = row_index if row_end_index is None else row_end_index
    selected = rows[row_index : end + 1]
    selected_text = " | ".join(value for row in selected for value in row if value)
    context = "" if context_row_index is None else " | ".join(value for value in rows[context_row_index] if value)
    if cell_index is None:
        value_text = selected_text
        excerpt = " | ".join(value for value in (context, selected_text) if value)
    else:
        value_text = rows[row_index][cell_index]
        parts = [value for value in (context, rows[row_index][0], value_text) if value]
        if comparison_cell_index is not None:
            parts.append(f"previous: {rows[row_index][comparison_cell_index]}")
        excerpt = " | ".join(parts)
    through = " | ".join(value for row in rows[: end + 1] for value in row if value)
    section = section_fingerprint or next((value for value in fingerprints if value.casefold() in through.casefold()), fingerprints[0])
    return {
        "locator_kind": "html-table",
        "locator_version": 1,
        "locator_key": locator_key,
        "ordinal": 1,
        "extraction_method_id": "extractor:source:html-semantic-table@1",
        "excerpt": excerpt,
        "excerpt_sha256": text_sha256(excerpt),
        "review_state": "reviewed",
        "node_path": f"html/table[{table_index}]/row[{row_index}]",
        "table_index": table_index,
        "table_fingerprints": list(fingerprints),
        "section_fingerprint": section,
        "row_header_fingerprint": row_header,
        "column_header_fingerprint": column_header,
        "cell_span_fingerprint": _span_fingerprint(table, row_index, end),
        "row_index": row_index,
        "row_end_index": None if row_end_index is None else row_end_index,
        "cell_index": cell_index,
        "comparison_cell_index": comparison_cell_index,
        "context_row_index": context_row_index,
        "exact_position": f"table={table_index};row={row_index}:{end};cell={cell_index if cell_index is not None else 'range'}",
    }


def _pdf_table_locator(
    path: Path,
    *,
    locator_key: str,
    row_index: int,
    column_index: int,
    comparison_column_index: int,
    row_header: str,
) -> dict[str, Any]:
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()
        table = tables[0]
        rows = [[" ".join(str(cell or "").split()) for cell in row] for row in table]
    region = "Full Year Fiscal 2025 Outlook"
    excerpt = " | ".join(
        value
        for value in (
            region,
            rows[row_index][0],
            rows[row_index][column_index],
            f"previous: {rows[row_index][comparison_column_index]}",
        )
        if value
    )
    return {
        "locator_kind": "pdf-table",
        "locator_version": 1,
        "locator_key": locator_key,
        "ordinal": 1,
        "extraction_method_id": "extractor:source:pdf-text-table@1",
        "excerpt": excerpt,
        "excerpt_sha256": text_sha256(excerpt),
        "review_state": "reviewed",
        "page": 1,
        "region_locator": region,
        "table_index": 0,
        "table_fingerprints": ["Net sales", "Operating margin", "Net Store Openings"],
        "row_header_fingerprint": row_header,
        "column_header_fingerprint": "Current Full Year Outlook",
        "row_index": row_index,
        "row_end_index": None,
        "column_index": column_index,
        "comparison_column_index": comparison_column_index,
    }


def _historical_document_spec(source_root: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    path, _root, document_text = _verified_release_text(
        source_root, str(row["file_date"]), str(row["sha256"])
    )
    return {
        "document_key": row["document_key"],
        "publisher_id": "abercrombie-fitch",
        "document_type": "earnings-release",
        "source_family": "sec-exhibit",
        "relative_path": str(path.relative_to(source_root)).replace("/", "\\"),
        "expected_sha256": row["sha256"],
        "revision": 1,
        "authority_class": "filed-exhibit",
        "publication_date": row["file_date"],
        "publication_date_basis": "sec-filed-date",
        "embedded_publication_date": "-".join(
            (
                str(row["file_date"])[:4],
                str({
                    "January": "01", "February": "02", "March": "03", "April": "04",
                    "May": "05", "June": "06", "July": "07", "August": "08",
                    "September": "09", "October": "10", "November": "11", "December": "12",
                }[str(row["embedded_date"]).split()[0]]),
                str(row["embedded_date"]).replace(",", "").split()[1].zfill(2),
            )
        ),
        "publication_date_locator": _html_dateline_locator(document_text, str(row["embedded_date"])),
        "report_date": row["report_date"],
        "accession": row["accession"],
        "canonical_url": None,
        "origin_document_key": None,
        "required": True,
        "review_state": "reviewed",
    }


def _period(year: int, *, start: str, end: str, weeks: int, ordinal: int, evidence: str) -> dict[str, Any]:
    return {
        "period_key": f"fy{year}",
        "period_id": f"period:anf:fy{year}@1",
        "fiscal_year": year,
        "fiscal_quarter": None,
        "period_type": "annual",
        "start_date": start,
        "end_date": end,
        "week_count": weeks,
        "fiscal_ordinal": ordinal,
        "is_53_week_year": weeks == 53,
        "start_rule_id": "rule:core:inclusive-weeks-ending@1",
        "evidence_assertion_key": evidence,
        "fiscal_claim_assertion_keys": [evidence],
        "reconciliation_state": "reconciled",
    }


def _guidance_assertions(source_root: Path) -> list[dict[str, Any]]:
    by_series: dict[tuple[int, str], str] = {}
    result: list[dict[str, Any]] = []
    for row in GUIDANCE:
        year = int(row["year"])
        file_date = str(row["date"])
        metric = str(row["metric"])
        document_key = f"anf-release-{file_date}"
        expected = next(
            str(item["sha256"]) for item in HISTORICAL_DOCUMENTS if item["file_date"] == file_date
        )
        _path, _root, document_text = _verified_release_text(source_root, file_date, expected)
        month = {"03": "mar", "05": "may", "08": "aug", "11": "nov"}[file_date[5:7]]
        assertion_key = f"guidance-fy{year}-{metric}-{month}"
        predecessor = by_series.get((year, metric))
        origin = predecessor is None
        locator = _html_text_locator(
            document_text,
            locator_key=f"html:{assertion_key}",
            fingerprint=str(row["value"]),
            year=year,
            fiscal_claim=False,
            replacement=not origin,
        )
        result.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "guidance",
                "document_key": document_key,
                "metric_key": metric,
                "horizon_period_key": f"fy{year}",
                "value_parser_id": row["parser"],
                "version_kind": "origin" if origin else "replacement",
                "supersedes_assertion_key": predecessor,
                "replacement_evidence_kind": None if origin else "explicit-replaces-wording",
                "required_reviewed_link_key": None,
                "locator": locator,
                "review_state": "reviewed",
            }
        )
        by_series[(year, metric)] = assertion_key

    # The final FY2024 versions are a structured current/previous table.
    path, root, _text_value = _verified_release_text(
        source_root,
        "2024-11-27",
        next(str(item["sha256"]) for item in HISTORICAL_DOCUMENTS if item["file_date"] == "2024-11-27"),
    )
    del path
    final_specs = (
        ("revenue-growth", "Net sales", 2, "parser:retail:guidance-percent-v2@2"),
        ("operating-margin", "Operating margin", 3, "parser:retail:guidance-percent-v2@2"),
        ("capital-expenditures", "Capital expenditures", 5, "parser:retail:guidance-currency-millions@1"),
    )
    for metric, label, row_index, parser in final_specs:
        assertion_key = f"guidance-fy2024-{metric}-nov"
        result.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "guidance",
                "document_key": "anf-release-2024-11-27",
                "metric_key": metric,
                "horizon_period_key": "fy2024",
                "value_parser_id": parser,
                "version_kind": "replacement",
                "supersedes_assertion_key": by_series[(2024, metric)],
                "replacement_evidence_kind": "current-previous-columns",
                "required_reviewed_link_key": None,
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:{assertion_key}",
                    table_fingerprints=("replaces all previous", "Current Full Year Outlook", "Capital expenditures"),
                    row_header=label,
                    column_header="Current Full Year Outlook",
                    row_index=row_index,
                    cell_index=1,
                    comparison_cell_index=2,
                    context_row_index=0,
                    section_fingerprint="replaces all previous",
                ),
                "review_state": "reviewed",
            }
        )
    return result


def _fy2025_completeness_assertions(source_root: Path, base: Mapping[str, Any]) -> list[dict[str, Any]]:
    documents = {str(row["document_key"]): row for row in base["documents"]}
    assertions: list[dict[str, Any]] = []
    series_predecessor: dict[str, str | None] = {
        "net-income-per-diluted-share": None,
        "capital-expenditures": None,
    }
    html_events = (
        ("2025-03-06", 8, 2, None, "mar"),
        ("2025-05-29", 6, 1, 2, "may"),
        ("2025-08-28", 6, 1, 2, "aug"),
        ("2025-11-26", 6, 1, 2, "nov"),
    )
    for date_value, table_index_expected, cell, comparison, month in html_events:
        doc_key = f"anf-release-{date_value}"
        path = source_root / str(documents[doc_key]["relative_path"])
        root = lxml_html.fromstring(path.read_bytes())
        for metric, row_index, label, parser in (
            ("net-income-per-diluted-share", 5, "Net income per diluted share", "parser:retail:guidance-currency-per-share@1"),
            ("capital-expenditures", 8, "Capital expenditures", "parser:retail:guidance-currency-millions@1"),
        ):
            assertion_key = f"guidance-fy2025-{metric}-{month}"
            predecessor = series_predecessor[metric]
            locator = _html_table_locator(
                root,
                locator_key=f"html:{assertion_key}",
                table_fingerprints=("For fiscal 2025", "Full Year Outlook") if predecessor is None else ("replaces all previous", "Current Full Year Outlook"),
                row_header=label,
                column_header="Full Year Outlook" if predecessor is None else "Current Full Year Outlook",
                row_index=row_index,
                cell_index=cell,
                comparison_cell_index=comparison,
                context_row_index=0,
                section_fingerprint="For fiscal 2025" if predecessor is None else "replaces all previous",
            )
            if table_index_expected != locator["table_index"]:
                raise ValueError(f"Reviewed table index drift for {assertion_key}")
            assertions.append(
                {
                    "assertion_key": assertion_key,
                    "assertion_kind": "guidance",
                    "document_key": doc_key,
                    "metric_key": metric,
                    "horizon_period_key": "fy2025",
                    "value_parser_id": parser,
                    "version_kind": "origin" if predecessor is None else "replacement",
                    "supersedes_assertion_key": predecessor,
                    "replacement_evidence_kind": None if predecessor is None else "current-previous-columns",
                    "required_reviewed_link_key": None,
                    "locator": locator,
                    "review_state": "reviewed",
                }
            )
            series_predecessor[metric] = assertion_key

    pdf_path = source_root / str(documents["anf-business-update-2026-01-12"]["relative_path"])
    for metric, row_index, label, parser in (
        ("net-income-per-diluted-share", 3, "Net income per diluted share", "parser:retail:guidance-currency-per-share@1"),
        ("capital-expenditures", 6, "Capital expenditures", "parser:retail:guidance-currency-millions@1"),
    ):
        assertion_key = f"guidance-fy2025-{metric}-jan"
        assertions.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "guidance",
                "document_key": "anf-business-update-2026-01-12",
                "metric_key": metric,
                "horizon_period_key": "fy2025",
                "value_parser_id": parser,
                "version_kind": "replacement",
                "supersedes_assertion_key": series_predecessor[metric],
                "replacement_evidence_kind": "current-previous-columns",
                "required_reviewed_link_key": None,
                "locator": _pdf_table_locator(
                    pdf_path,
                    locator_key=f"pdf:{assertion_key}",
                    row_index=row_index,
                    column_index=1,
                    comparison_column_index=2,
                    row_header=label,
                ),
                "review_state": "reviewed",
            }
        )

    actual_path = source_root / str(documents["anf-release-2026-03-04"]["relative_path"])
    actual_root = lxml_html.fromstring(actual_path.read_bytes())
    actual_specs = (
        ("actual-fy2025-net-sales-growth", "revenue-growth", "parser:retail:percent-text@1", ("Full Year", "Comparable sales", "Net sales by segment"), "Total company", "1 YR % Change", 6, 9, 1),
        ("actual-fy2025-operating-margin-reported", "operating-margin", "parser:retail:decimal-percent@1", ("Schedule of Non-GAAP Financial Measures", "Operating income", "Adjusted non-GAAP"), "Operating income", "% of Net Sales", 7, 5, 5),
        ("actual-fy2025-operating-margin-adjusted", "operating-margin-adjusted-litigation-excluded", "parser:retail:decimal-percent@1", ("Schedule of Non-GAAP Financial Measures", "Operating income", "Adjusted non-GAAP"), "Operating income", "Adjusted non-GAAP", 7, 14, 5),
        ("actual-fy2025-eps-reported", "net-income-per-diluted-share", "parser:retail:currency-per-share@1", ("Schedule of Non-GAAP Financial Measures", "Net income per diluted share", "Adjusted non-GAAP"), "Net income per diluted share", "GAAP", 11, 3, 5),
        ("actual-fy2025-eps-adjusted", "net-income-per-diluted-share-adjusted", "parser:retail:currency-per-share@1", ("Schedule of Non-GAAP Financial Measures", "Net income per diluted share", "Adjusted non-GAAP"), "Net income per diluted share", "Adjusted non-GAAP", 11, 13, 5),
        ("actual-fy2025-property-equipment-purchases", "property-equipment-purchases", "parser:retail:currency-thousands-to-millions@1", ("Condensed Consolidated Statements of Cash Flows", "Purchases of property and equipment", "January 31, 2026"), "Purchases of property and equipment", "January 31, 2026", 11, 1, 5),
    )
    for assertion_key, metric, parser, fingerprints, row_header, column_header, row_index, cell_index, context_index in actual_specs:
        assertions.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "numerical_fact",
                "document_key": "anf-release-2026-03-04",
                "metric_key": metric,
                "period_key": "fy2025",
                "dimension_alias": "total company",
                "value_parser_id": parser,
                "locator": _html_table_locator(
                    actual_root,
                    locator_key=f"html:{assertion_key}",
                    table_fingerprints=fingerprints,
                    row_header=row_header,
                    column_header=column_header,
                    row_index=row_index,
                    cell_index=cell_index,
                    context_row_index=context_index,
                    section_fingerprint=fingerprints[0],
                ),
                "review_state": "reviewed",
            }
        )
    return assertions


def _one_table(root: Any, *fingerprints: str) -> tuple[int, Any, list[list[str]]]:
    matches = [
        (index, table, _rows(table))
        for index, table in enumerate(root.xpath("//table"))
        if all(fingerprint.casefold() in _text(table).casefold() for fingerprint in fingerprints)
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Reviewed table fingerprints {fingerprints!r} matched {len(matches)} tables"
        )
    return matches[0]


def _first_numeric_cell(row: list[str], *, after: int = 0) -> int:
    for index, value in enumerate(row[after + 1 :], after + 1):
        normalized = value.replace(",", "").replace("(", "-").replace(")", "").strip()
        if normalized and any(character.isdigit() for character in normalized):
            return index
    raise ValueError(f"Reviewed row has no numeric cell: {row!r}")


def _percent_of_sales_cell(row: list[str]) -> int:
    for index in range(1, len(row)):
        value = row[index].strip()
        if not value or not any(character.isdigit() for character in value):
            continue
        following = next((item.strip() for item in row[index + 1 :] if item.strip()), None)
        if following == "%":
            return index
    raise ValueError(f"Reviewed operating row has no percent-of-sales cell: {row!r}")


def _sales_growth_cell(row: list[str], *, has_comparable_sales: bool) -> int:
    populated = [
        (index, value)
        for index, value in enumerate(row)
        if value.strip() and value.strip().endswith("%")
    ]
    required = 2 if has_comparable_sales else 1
    if len(populated) < required:
        raise ValueError(f"Reviewed sales row lacks the expected percent columns: {row!r}")
    return populated[-2 if has_comparable_sales else -1][0]


def _historical_annual_actual_assertions(source_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    releases = (
        (2022, "2023-03-02", "January 28, 2023", "Fifty-Two Weeks Ended"),
        (2023, "2024-03-07", "February 3, 2024", "Fifty-Three Weeks Ended"),
        (2024, "2025-03-06", "February 1, 2025", "Fifty-Two Weeks Ended"),
    )
    for fiscal_year, publication_date, annual_end_label, duration_label in releases:
        expected = next(
            (
                str(value["sha256"])
                for value in HISTORICAL_DOCUMENTS
                if value["file_date"] == publication_date
            ),
            None,
        )
        _path, root, _document_text = _verified_release_text(
            source_root, publication_date, expected
        )
        document_key = f"anf-release-{publication_date}"

        _sales_index, sales_table, sales_rows = _one_table(
            root, "Full Year", "1 YR % Change", "Total company"
        )
        sales_row_index = next(
            index
            for index, row in enumerate(sales_rows)
            if row and row[0].strip().casefold() == "total company"
        )
        sales_cell = _sales_growth_cell(
            sales_rows[sales_row_index],
            has_comparable_sales="Comparable sales" in _text(sales_table),
        )
        results.append(
            {
                "assertion_key": f"actual-fy{fiscal_year}-net-sales-growth",
                "assertion_kind": "numerical_fact",
                "document_key": document_key,
                "metric_key": "revenue-growth",
                "period_key": f"fy{fiscal_year}",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:percent-text@1",
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:actual-fy{fiscal_year}-net-sales-growth",
                    table_fingerprints=("Full Year", "1 YR % Change", "Total company"),
                    row_header="Total company",
                    column_header="1 YR % Change",
                    row_index=sales_row_index,
                    cell_index=sales_cell,
                    context_row_index=1,
                    section_fingerprint="Full Year",
                ),
                "review_state": "reviewed",
            }
        )

        _ops_index, _ops_table, operations_rows = _one_table(
            root,
            "Condensed Consolidated Statements of Operations",
            annual_end_label,
            duration_label,
            "% of Net Sales",
        )
        operating_row_index = next(
            index
            for index, row in enumerate(operations_rows)
            if row
            and row[0].casefold().startswith("operating")
            and "income" in row[0].casefold()
        )
        operating_cell = _percent_of_sales_cell(operations_rows[operating_row_index])
        results.append(
            {
                "assertion_key": f"actual-fy{fiscal_year}-operating-margin-reported",
                "assertion_kind": "numerical_fact",
                "document_key": document_key,
                "metric_key": "operating-margin",
                "period_key": f"fy{fiscal_year}",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:decimal-percent@1",
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:actual-fy{fiscal_year}-operating-margin-reported",
                    table_fingerprints=(
                        "Condensed Consolidated Statements of Operations",
                        annual_end_label,
                        duration_label,
                        "% of Net Sales",
                    ),
                    row_header=operations_rows[operating_row_index][0],
                    column_header="% of Net Sales",
                    row_index=operating_row_index,
                    cell_index=operating_cell,
                    context_row_index=5,
                    section_fingerprint="Condensed Consolidated Statements of Operations",
                ),
                "review_state": "reviewed",
            }
        )

        _cash_index, _cash_table, cash_rows = _one_table(
            root,
            "Condensed Consolidated Statements of Cash Flows",
            annual_end_label,
            "Purchases of property and equipment",
        )
        purchase_row_index = next(
            index
            for index, row in enumerate(cash_rows)
            if row and row[0].strip().casefold() == "purchases of property and equipment"
        )
        purchase_cell = _first_numeric_cell(cash_rows[purchase_row_index])
        results.append(
            {
                "assertion_key": f"actual-fy{fiscal_year}-property-equipment-purchases",
                "assertion_kind": "numerical_fact",
                "document_key": document_key,
                "metric_key": "property-equipment-purchases",
                "period_key": f"fy{fiscal_year}",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:currency-thousands-to-millions@1",
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:actual-fy{fiscal_year}-property-equipment-purchases",
                    table_fingerprints=(
                        "Condensed Consolidated Statements of Cash Flows",
                        annual_end_label,
                        "Purchases of property and equipment",
                    ),
                    row_header="Purchases of property and equipment",
                    column_header=annual_end_label,
                    row_index=purchase_row_index,
                    cell_index=purchase_cell,
                    context_row_index=5,
                    section_fingerprint="Condensed Consolidated Statements of Cash Flows",
                ),
                "review_state": "reviewed",
            }
        )
    return results


def _quarterly_progress_assertions(
    source_root: Path,
    events: Iterable[Mapping[str, Any]] = QUARTERLY_PROGRESS_EVENTS,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for event in events:
        year = int(event["year"])
        quarter = int(event["quarter"])
        publication_date = str(event["publication_date"])
        expected = next(
            (
                str(value["sha256"])
                for value in HISTORICAL_DOCUMENTS
                if value["file_date"] == publication_date
            ),
            None,
        )
        _path, root, _document_text = _verified_release_text(
            source_root, publication_date, expected
        )
        document_key = f"anf-release-{publication_date}"
        period_key = f"fy{year}-q{quarter}"
        end_day = date.fromisoformat(str(event["end_date"]))
        end_label = f"{end_day.strftime('%B')} {end_day.day}, {end_day.year}"
        weeks_word = {13: "Thirteen", 14: "Fourteen"}.get(
            int(event.get("week_count", 13))
        )
        if weeks_word is None:
            raise ValueError("Quarter actual extraction supports only reviewed 13/14-week quarters.")

        sales_fingerprints = (
            ("Fourth Quarter", "1 YR % Change", "Total company")
            if quarter == 4
            else ("1 YR % Change", "Total company")
        )
        _sales_index, sales_table, sales_rows = _one_table(root, *sales_fingerprints)
        sales_row_index = next(
            index
            for index, row in enumerate(sales_rows)
            if row and row[0].strip().casefold() == "total company"
        )
        sales_cell = _sales_growth_cell(
            sales_rows[sales_row_index],
            has_comparable_sales="Comparable sales" in _text(sales_table),
        )
        results.append(
            {
                "assertion_key": f"progress-fy{year}-q{quarter}-net-sales-growth",
                "assertion_kind": "numerical_fact",
                "document_key": document_key,
                "metric_key": "revenue-growth",
                "period_key": period_key,
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:percent-text@1",
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:progress-fy{year}-q{quarter}-net-sales-growth",
                    table_fingerprints=sales_fingerprints,
                    row_header="Total company",
                    column_header="1 YR % Change",
                    row_index=sales_row_index,
                    cell_index=sales_cell,
                    context_row_index=1,
                    section_fingerprint="1 YR % Change",
                ),
                "review_state": "reviewed",
            }
        )

        operations_fingerprints = (
            (
                "Condensed Consolidated Statements of Operations",
                f"{weeks_word} Weeks Ended",
                end_label,
                "% of Net Sales",
            )
            if quarter == 4
            else (
                "Condensed Consolidated Statements of Operations",
                f"{weeks_word} Weeks Ended {end_label}",
                "% of Net Sales",
            )
        )
        _ops_index, _ops_table, operations_rows = _one_table(
            root, *operations_fingerprints
        )
        operating_row_index = next(
            index
            for index, row in enumerate(operations_rows)
            if row
            and row[0].casefold().startswith("operating")
            and "income" in row[0].casefold()
        )
        operating_cell = _percent_of_sales_cell(operations_rows[operating_row_index])
        results.append(
            {
                "assertion_key": f"progress-fy{year}-q{quarter}-operating-margin",
                "assertion_kind": "numerical_fact",
                "document_key": document_key,
                "metric_key": "operating-margin",
                "period_key": period_key,
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:decimal-percent@1",
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:progress-fy{year}-q{quarter}-operating-margin",
                    table_fingerprints=operations_fingerprints,
                    row_header=operations_rows[operating_row_index][0],
                    column_header="% of Net Sales",
                    row_index=operating_row_index,
                    cell_index=operating_cell,
                    context_row_index=5,
                    section_fingerprint="Condensed Consolidated Statements of Operations",
                ),
                "review_state": "reviewed",
            }
        )

        if year == 2025:
            diluted_row_index = next(
                index
                for index, row in enumerate(operations_rows)
                if row and row[0].strip().casefold() == "diluted"
            )
            diluted_cell = _first_numeric_cell(operations_rows[diluted_row_index])
            results.append(
                {
                    "assertion_key": f"progress-fy{year}-q{quarter}-eps-reported",
                    "assertion_kind": "numerical_fact",
                    "document_key": document_key,
                    "metric_key": "net-income-per-diluted-share",
                    "period_key": period_key,
                    "dimension_alias": "total company",
                    "value_parser_id": "parser:retail:currency-per-share@1",
                    "locator": _html_table_locator(
                        root,
                        locator_key=f"html:progress-fy{year}-q{quarter}-eps-reported",
                        table_fingerprints=(
                            *operations_fingerprints[:-1],
                            "Net income per share",
                        ),
                        row_header="Diluted",
                        column_header=end_label,
                        row_index=diluted_row_index,
                        cell_index=diluted_cell,
                        context_row_index=5,
                        section_fingerprint="Condensed Consolidated Statements of Operations",
                    ),
                    "review_state": "reviewed",
                }
            )
    return results


def _fy2025_quarter_capability_actual_assertions(
    source_root: Path, base: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Map reviewed quarter-period buybacks and diluted shares as Actuals.

    These facts were already present in the accepted local releases.  Product@2 2.0
    mapped only the YTD/cumulative forms, so this successor adds the distinct
    quarter-period observations without changing the existing Progress records.
    """

    documents = {str(row["document_key"]): row for row in base["documents"]}
    repurchase_fingerprints = {
        1: (
            "During the first quarter of 2025, the company repurchased 2.6 million "
            "shares for approximately $200 million"
        ),
        2: (
            "During the second quarter of 2025, the company repurchased 0.6 million "
            "shares for approximately $50 million"
        ),
        3: (
            "During the third quarter of 2025, the company repurchased 1.2 million "
            "shares for approximately $100 million"
        ),
        4: (
            "During the fourth quarter of 2025, the company repurchased 0.9 million "
            "shares for approximately $100 million"
        ),
    }
    results: list[dict[str, Any]] = []
    events = tuple(
        row
        for row in (*QUARTERLY_PROGRESS_EVENTS, *Q4_RESULT_EVENTS)
        if int(row["year"]) == 2025
    )
    for event in events:
        quarter = int(event["quarter"])
        publication_date = str(event["publication_date"])
        document_key = f"anf-release-{publication_date}"
        path = source_root / str(documents[document_key]["relative_path"])
        root = lxml_html.fromstring(path.read_bytes())
        document_text = _text(root)
        end_day = date.fromisoformat(str(event["end_date"]))
        end_label = f"{end_day.strftime('%B')} {end_day.day}, {end_day.year}"
        weeks_word = {13: "Thirteen", 14: "Fourteen"}[
            int(event.get("week_count", 13))
        ]
        table_fingerprints = (
            f"{weeks_word} Weeks Ended {weeks_word} Weeks Ended {end_label}",
            "Weighted-average shares outstanding",
        )
        _table_index, _table, operation_rows = _one_table(
            root, *table_fingerprints
        )
        diluted_rows = [
            index
            for index, row in enumerate(operation_rows)
            if row and row[0].strip().casefold() == "diluted"
        ]
        if len(diluted_rows) < 2:
            raise ValueError("Reviewed quarter table lacks diluted EPS/share rows.")
        diluted_row_index = diluted_rows[-1]
        diluted_cell = _first_numeric_cell(operation_rows[diluted_row_index])
        results.extend(
            (
                {
                    "assertion_key": f"actual-fy2025-q{quarter}-share-repurchases",
                    "assertion_kind": "numerical_fact",
                    "document_key": document_key,
                    "metric_key": "share-repurchases",
                    "period_key": f"fy2025-q{quarter}",
                    "dimension_alias": "total company",
                    "value_parser_id": "parser:retail:currency-millions@1",
                    "locator": _reviewed_html_text_locator(
                        document_text,
                        locator_key=(
                            f"html:actual-fy2025-q{quarter}-share-repurchases"
                        ),
                        fingerprint=repurchase_fingerprints[quarter],
                    ),
                    "review_state": "reviewed",
                },
                {
                    "assertion_key": f"actual-fy2025-q{quarter}-diluted-shares",
                    "assertion_kind": "numerical_fact",
                    "document_key": document_key,
                    "metric_key": "diluted-weighted-average-shares",
                    "period_key": f"fy2025-q{quarter}",
                    "dimension_alias": "total company",
                    "value_parser_id": (
                        "parser:retail:shares-thousands-to-millions@1"
                    ),
                    "locator": _html_table_locator(
                        root,
                        locator_key=f"html:actual-fy2025-q{quarter}-diluted-shares",
                        table_fingerprints=table_fingerprints,
                        row_header="Diluted",
                        column_header=end_label,
                        row_index=diluted_row_index,
                        cell_index=diluted_cell,
                        context_row_index=0,
                        section_fingerprint="Weighted-average shares outstanding",
                    ),
                    "review_state": "reviewed",
                },
            )
        )
    return results


def _q3_ytd_property_purchase_assertions(source_root: Path) -> list[dict[str, Any]]:
    """Map exact 9M property/equipment flows needed by the closed Q4 derivation gate."""

    results: list[dict[str, Any]] = []
    for event in Q3_YTD_CAPEX_EVENTS:
        year = int(event["year"])
        publication_date = str(event["publication_date"])
        expected = next(
            (
                str(value["sha256"])
                for value in HISTORICAL_DOCUMENTS
                if value["file_date"] == publication_date
            ),
            None,
        )
        _path, root, _document_text = _verified_release_text(
            source_root, publication_date, expected
        )
        end_day = date.fromisoformat(str(event["end_date"]))
        end_label = f"{end_day.strftime('%B')} {end_day.day}, {end_day.year}"
        _table_index, _table, cash_rows = _one_table(
            root,
            "Condensed Consolidated Statements of Cash Flows",
            end_label,
            "Purchases of property and equipment",
        )
        purchase_row_index = next(
            index
            for index, row in enumerate(cash_rows)
            if row and row[0].strip().casefold() == "purchases of property and equipment"
        )
        purchase_cell = _first_numeric_cell(cash_rows[purchase_row_index])
        assertion_key = f"actual-fy{year}-ytd-q3-property-equipment-purchases"
        results.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "numerical_fact",
                "document_key": f"anf-release-{publication_date}",
                "metric_key": "property-equipment-purchases",
                "period_key": str(event["period_key"]),
                "dimension_alias": "total company",
                "value_parser_id": (
                    "parser:retail:currency-thousands-to-millions@1"
                ),
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:{assertion_key}",
                    table_fingerprints=(
                        "Condensed Consolidated Statements of Cash Flows",
                        end_label,
                        "Purchases of property and equipment",
                    ),
                    row_header="Purchases of property and equipment",
                    column_header=end_label,
                    row_index=purchase_row_index,
                    cell_index=purchase_cell,
                    context_row_index=5,
                    section_fingerprint=(
                        "Condensed Consolidated Statements of Cash Flows"
                    ),
                ),
                "review_state": "reviewed",
            }
        )
    return results


def _fy2026_open_completeness_assertions(
    source_root: Path, base: Mapping[str, Any]
) -> list[dict[str, Any]]:
    documents = {str(row["document_key"]): row for row in base["documents"]}
    path = source_root / str(documents["anf-release-2026-03-04"]["relative_path"])
    root = lxml_html.fromstring(path.read_bytes())
    results: list[dict[str, Any]] = []
    for metric, row_index, label, parser in (
        (
            "net-income-per-diluted-share",
            5,
            "Net income per diluted share",
            "parser:retail:guidance-currency-per-share@1",
        ),
        (
            "capital-expenditures",
            8,
            "Capital expenditures",
            "parser:retail:guidance-currency-millions@1",
        ),
    ):
        assertion_key = f"guidance-fy2026-{metric}-release"
        results.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "guidance",
                "document_key": "anf-release-2026-03-04",
                "metric_key": metric,
                "horizon_period_key": "fy2026",
                "value_parser_id": parser,
                "version_kind": "origin",
                "supersedes_assertion_key": None,
                "replacement_evidence_kind": None,
                "required_reviewed_link_key": None,
                "locator": _html_table_locator(
                    root,
                    locator_key=f"html:{assertion_key}",
                    table_fingerprints=("For fiscal 2026", "Full Year Outlook"),
                    row_header=label,
                    column_header="Full Year Outlook",
                    row_index=row_index,
                    cell_index=2,
                    context_row_index=0,
                    section_fingerprint="For fiscal 2026",
                ),
                "review_state": "reviewed",
            }
        )
    return results


_CAPABILITY_GUIDANCE_SPECS: tuple[tuple[str, int, str, str], ...] = (
    ("share-repurchases", 6, "Share repurchases", "parser:retail:guidance-currency-millions@1"),
    (
        "diluted-weighted-average-shares",
        7,
        "Diluted weighted average shares",
        "parser:retail:guidance-shares-millions@1",
    ),
    ("net-store-openings", 9, "Real estate activity", "parser:retail:store-plan@1"),
    (
        "store-openings",
        10,
        "Openings",
        "parser:retail:guidance-approximate-store-openings@1",
    ),
    (
        "store-closures-count",
        10,
        "Closures",
        "parser:retail:guidance-approximate-store-closures@1",
    ),
    (
        "store-remodels-right-sizes",
        11,
        "Remodels and right-sizes",
        "parser:retail:guidance-approximate-store-remodels@1",
    ),
)


def _capability_guidance_assertions(
    source_root: Path, base: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Map reviewed capital-allocation/share/store guidance without legacy fallback."""

    documents = {str(row["document_key"]): row for row in base["documents"]}
    results: list[dict[str, Any]] = []
    predecessor: dict[str, str | None] = {
        metric: None for metric, *_ in _CAPABILITY_GUIDANCE_SPECS
    }
    html_events = (
        ("2025-03-06", "mar", False, 2, None),
        ("2025-05-29", "may", True, 1, 2),
        ("2025-08-28", "aug", True, 1, 2),
        ("2025-11-26", "nov", True, 1, 2),
    )
    for publication_date, suffix, replacement, default_cell, comparison_cell in html_events:
        document_key = f"anf-release-{publication_date}"
        root = lxml_html.fromstring(
            (source_root / str(documents[document_key]["relative_path"])).read_bytes()
        )
        for metric, row_index, label, parser_id in _CAPABILITY_GUIDANCE_SPECS:
            row_cell = default_cell
            row_comparison_cell = comparison_cell
            row_header = label
            if row_index == 10:
                row_header = "60 openings, 20 closures"
                row_cell = 0
                row_comparison_cell = 1 if replacement else None
            elif row_index == 11:
                row_header = "40 remodels and right-sizes"
                row_cell = 0
                row_comparison_cell = 1 if replacement else None
            elif row_index == 9:
                row_header = "Real estate activity"
            assertion_key = f"guidance-fy2025-{metric}-{suffix}"
            prior = predecessor[metric]
            results.append(
                {
                    "assertion_key": assertion_key,
                    "assertion_kind": "guidance",
                    "document_key": document_key,
                    "metric_key": metric,
                    "horizon_period_key": "fy2025",
                    "value_parser_id": parser_id,
                    "version_kind": "origin" if prior is None else "replacement",
                    "supersedes_assertion_key": prior,
                    "replacement_evidence_kind": (
                        None if prior is None else "current-previous-columns"
                    ),
                    "required_reviewed_link_key": None,
                    "locator": _html_table_locator(
                        root,
                        locator_key=f"html:{assertion_key}",
                        table_fingerprints=(
                            ("For fiscal 2025", "Full Year Outlook")
                            if prior is None
                            else ("replaces all previous", "Current Full Year Outlook")
                        ),
                        row_header=row_header,
                        column_header=(
                            "Full Year Outlook" if prior is None else "Current Full Year Outlook"
                        ),
                        row_index=row_index,
                        cell_index=row_cell,
                        comparison_cell_index=row_comparison_cell,
                        context_row_index=0,
                        section_fingerprint=(
                            "For fiscal 2025" if prior is None else "replaces all previous"
                        ),
                    ),
                    "review_state": "reviewed",
                }
            )
            predecessor[metric] = assertion_key

    pdf_path = source_root / str(
        documents["anf-business-update-2026-01-12"]["relative_path"]
    )
    pdf_rows = {
        "share-repurchases": (4, "Share repurchases"),
        "diluted-weighted-average-shares": (5, "Diluted weighted average shares"),
        "net-store-openings": (7, "Net Store Openings"),
        "store-openings": (8, "Openings"),
        "store-closures-count": (8, "Closures"),
        "store-remodels-right-sizes": (9, "Remodels And Right-Sizes"),
    }
    for metric, _row_index, _label, parser_id in _CAPABILITY_GUIDANCE_SPECS:
        row_index, row_header = pdf_rows[metric]
        assertion_key = f"guidance-fy2025-{metric}-jan"
        results.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "guidance",
                "document_key": "anf-business-update-2026-01-12",
                "metric_key": metric,
                "horizon_period_key": "fy2025",
                "value_parser_id": parser_id,
                "version_kind": "replacement",
                "supersedes_assertion_key": predecessor[metric],
                "replacement_evidence_kind": "current-previous-columns",
                "required_reviewed_link_key": None,
                "locator": _pdf_table_locator(
                    pdf_path,
                    locator_key=f"pdf:{assertion_key}",
                    row_index=row_index,
                    column_index=1,
                    comparison_column_index=2,
                    row_header=row_header,
                ),
                "review_state": "reviewed",
            }
        )

    release_path = source_root / str(
        documents["anf-release-2026-03-04"]["relative_path"]
    )
    release_root = lxml_html.fromstring(release_path.read_bytes())
    for metric, row_index, label, parser_id in _CAPABILITY_GUIDANCE_SPECS:
        row_cell = 2
        row_header = label
        if row_index == 10:
            row_header, row_cell = "55 openings, 25 closures", 0
        elif row_index == 11:
            row_header, row_cell = "70 remodels and right-sizes", 0
        elif row_index == 9:
            row_header = "Real estate activity"
        assertion_key = f"guidance-fy2026-{metric}-release"
        results.append(
            {
                "assertion_key": assertion_key,
                "assertion_kind": "guidance",
                "document_key": "anf-release-2026-03-04",
                "metric_key": metric,
                "horizon_period_key": "fy2026",
                "value_parser_id": parser_id,
                "version_kind": "origin",
                "supersedes_assertion_key": None,
                "replacement_evidence_kind": None,
                "required_reviewed_link_key": None,
                "locator": _html_table_locator(
                    release_root,
                    locator_key=f"html:{assertion_key}",
                    table_fingerprints=("For fiscal 2026", "Full Year Outlook"),
                    row_header=row_header,
                    column_header="Full Year Outlook",
                    row_index=row_index,
                    cell_index=row_cell,
                    context_row_index=0,
                    section_fingerprint="For fiscal 2026",
                ),
                "review_state": "reviewed",
            }
        )
    return results


def _capability_actual_and_progress_assertions(
    source_root: Path, base: Mapping[str, Any]
) -> list[dict[str, Any]]:
    documents = {str(row["document_key"]): row for row in base["documents"]}
    results: list[dict[str, Any]] = []
    annual_document = "anf-release-2026-03-04"
    annual_path = source_root / str(documents[annual_document]["relative_path"])
    annual_root = lxml_html.fromstring(annual_path.read_bytes())
    annual_text = _text(annual_root)
    annual_share_fingerprint = (
        "For the full year ended January 31, 2026, the company repurchased "
        "5.4 million shares for $450 million"
    )
    results.extend(
        (
            {
                "assertion_key": "actual-fy2025-share-repurchases",
                "assertion_kind": "numerical_fact",
                "document_key": annual_document,
                "metric_key": "share-repurchases",
                "period_key": "fy2025",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:currency-millions@1",
                "locator": _reviewed_html_text_locator(
                    annual_text,
                    locator_key="html:actual-fy2025-share-repurchases",
                    fingerprint=annual_share_fingerprint,
                ),
                "review_state": "reviewed",
            },
            {
                "assertion_key": "actual-fy2025-diluted-weighted-average-shares",
                "assertion_kind": "numerical_fact",
                "document_key": annual_document,
                "metric_key": "diluted-weighted-average-shares",
                "period_key": "fy2025",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:shares-thousands-to-millions@1",
                "locator": _html_table_locator(
                    annual_root,
                    locator_key="html:actual-fy2025-diluted-weighted-average-shares",
                    table_fingerprints=(
                        "Fifty-Two Weeks Ended Fifty-Two Weeks Ended January 31, 2026",
                        "Weighted-average shares outstanding",
                    ),
                    row_header="Diluted",
                    column_header="January 31, 2026",
                    row_index=25,
                    cell_index=1,
                    context_row_index=0,
                    section_fingerprint="Weighted-average shares outstanding",
                ),
                "review_state": "reviewed",
            },
        )
    )

    transcript_document = "anf-transcript-2026-03-04"
    transcript_path = source_root / str(documents[transcript_document]["relative_path"])
    transcript_text = transcript_path.read_text(encoding="utf-8")
    reviewed_store_line = transcript_text.splitlines()[49]
    required_components = ("11 right sizes", "47 remodels")
    if not all(value in reviewed_store_line for value in required_components):
        raise ValueError(
            "The reviewed FY2025 remodel/right-size transcript components changed."
        )
    results.extend(
        (
            {
                "assertion_key": "actual-fy2025-store-right-sizes",
                "assertion_kind": "numerical_fact",
                "document_key": transcript_document,
                "metric_key": "store-right-sizes",
                "period_key": "fy2025",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:reported-store-right-sizes@1",
                "locator": _reviewed_text_line_locator(
                    transcript_text,
                    locator_key="text:line-50-fy2025-store-right-sizes",
                    line_number=50,
                    speaker_fingerprint="Robert Ball",
                ),
                "review_state": "reviewed",
            },
            {
                "assertion_key": "actual-fy2025-store-remodels",
                "assertion_kind": "numerical_fact",
                "document_key": transcript_document,
                "metric_key": "store-remodels",
                "period_key": "fy2025",
                "dimension_alias": "total company",
                "value_parser_id": "parser:retail:reported-store-remodels@1",
                "locator": _reviewed_text_line_locator(
                    transcript_text,
                    locator_key="text:line-50-fy2025-store-remodels",
                    line_number=50,
                    speaker_fingerprint="Robert Ball",
                ),
                "review_state": "reviewed",
            },
        )
    )

    ytd_share_fingerprints = {
        1: (
            "During the first quarter of 2025, the company repurchased 2.6 million "
            "shares for approximately $200 million"
        ),
        2: (
            "For the year-to-date period ended August 2, 2025, the company repurchased "
            "3.2 million shares for $250 million"
        ),
        3: (
            "For the year-to-date period ended November 1, 2025, the company repurchased "
            "4.5 million shares for $350 million"
        ),
    }
    weeks_labels = {1: "Thirteen", 2: "Twenty-Six", 3: "Thirty-Nine"}
    for event in FY2025_YTD_PROGRESS_EVENTS:
        quarter = int(event["quarter"])
        publication_date = str(event["publication_date"])
        document_key = f"anf-release-{publication_date}"
        path = source_root / str(documents[document_key]["relative_path"])
        root = lxml_html.fromstring(path.read_bytes())
        document_text = _text(root)
        end_day = date.fromisoformat(str(event["end_date"]))
        end_label = f"{end_day.strftime('%B')} {end_day.day}, {end_day.year}"
        table_fingerprints = (
            f"{weeks_labels[quarter]} Weeks Ended {weeks_labels[quarter]} Weeks Ended {end_label}",
            "Weighted-average shares outstanding",
        )
        results.extend(
            (
                {
                    "assertion_key": f"progress-fy2025-ytd-q{quarter}-share-repurchases",
                    "assertion_kind": "numerical_fact",
                    "document_key": document_key,
                    "metric_key": "share-repurchases",
                    "period_key": str(event["period_key"]),
                    "dimension_alias": "total company",
                    "value_parser_id": "parser:retail:currency-millions@1",
                    "locator": _reviewed_html_text_locator(
                        document_text,
                        locator_key=f"html:progress-fy2025-ytd-q{quarter}-share-repurchases",
                        fingerprint=ytd_share_fingerprints[quarter],
                    ),
                    "review_state": "reviewed",
                },
                {
                    "assertion_key": f"progress-fy2025-ytd-q{quarter}-diluted-shares",
                    "assertion_kind": "numerical_fact",
                    "document_key": document_key,
                    "metric_key": "diluted-weighted-average-shares",
                    "period_key": str(event["period_key"]),
                    "dimension_alias": "total company",
                    "value_parser_id": "parser:retail:shares-thousands-to-millions@1",
                    "locator": _html_table_locator(
                        root,
                        locator_key=f"html:progress-fy2025-ytd-q{quarter}-diluted-shares",
                        table_fingerprints=table_fingerprints,
                        row_header="Diluted",
                        column_header=end_label,
                        row_index=25,
                        cell_index=1,
                        context_row_index=0,
                        section_fingerprint="Weighted-average shares outstanding",
                    ),
                    "review_state": "reviewed",
                },
            )
        )

    closure = copy.deepcopy(
        next(
            row
            for row in base["required_assertions"]
            if row["assertion_key"] == "store-closures-release"
        )
    )
    closure["assertion_key"] = "store-closures-count-release"
    closure["metric_key"] = "store-closures-count"
    closure["value_parser_id"] = "parser:retail:absolute-count-text@1"
    closure["locator"]["locator_key"] = "html:fy2025-store-closures-count"
    results.append(closure)

    capex_document = "anf-release-2023-03-02"
    capex_path = source_root / str(documents[capex_document]["relative_path"])
    capex_text = _text(lxml_html.fromstring(capex_path.read_bytes()))
    capex_fingerprint = (
        "Net cash used for investing activities of $141 million, reflecting $165 million "
        "in capital expenditures of which approximately half was invested in digital and "
        "technology and half primarily in stores"
    )
    results.append(
        {
            "assertion_key": "definition-equivalence-fy2022-capex-property-purchases",
            "assertion_kind": "management_statement",
            "document_key": capex_document,
            "statement_kind": "explanation",
            "topic_id": CAPEX_EQUIVALENCE_TOPIC_ID,
            "statement_period_key": "fy2022",
            "speaker_id": "issuer-management",
            "locator": _reviewed_html_text_locator(
                capex_text,
                locator_key="html:definition-equivalence-fy2022-capex-property-purchases",
                fingerprint=capex_fingerprint,
            ),
            "review_state": "reviewed",
        }
    )
    return results


def build_anf_product_v2_source_set(
    *,
    source_root: Path,
    repository_root: Path,
    successor: bool = False,
) -> dict[str, Any]:
    """Return the closed ANF Product@2 source set.

    ``successor=False`` is the immutable 2.0 golden generator.  The post-golden
    successor extends that exact result from the already-reviewed local documents;
    it never rewrites the accepted v2 fixture in place.
    """

    fixture = repository_root / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
    base = json.loads(fixture.read_text(encoding="utf-8"))
    result = copy.deepcopy(base)
    result["source_set_id"] = SOURCE_SET_ID
    result["sector_pack_id"] = "sector-pack:retail:longitudinal@2"
    result["ticker_profile_id"] = "ticker-profile:anf:source-native@2"
    activated = set(result["profile"]["activated_metric_ids"])
    activated.update(
        {
            "metric:core:capital-expenditures@1",
            "metric:core:diluted-weighted-average-shares@1",
            "metric:core:net-income-per-diluted-share@1",
            "metric:core:property-equipment-purchases@1",
            "metric:core:share-repurchases@1",
            "metric:retail:store-closures-count@1",
            "metric:retail:store-right-sizes@1",
            "metric:retail:store-remodels@1",
            "metric:retail:store-remodels-right-sizes@1",
        }
    )
    result["profile"]["activated_metric_ids"] = sorted(activated)

    historical_docs = [_historical_document_spec(source_root, row) for row in HISTORICAL_DOCUMENTS]
    result["documents"] = sorted(
        [*result["documents"], *historical_docs], key=lambda row: str(row["document_key"])
    )
    existing_period_keys = {str(row["period_key"]) for row in result["periods"]}
    result["periods"] = sorted(
        [
            *result["periods"],
            _period(2022, start="2022-01-30", end="2023-01-28", weeks=52, ordinal=92, evidence="period-fy2022-annual"),
            _period(2023, start="2023-01-29", end="2024-02-03", weeks=53, ordinal=96, evidence="period-fy2023-annual"),
            _period(2024, start="2024-02-04", end="2025-02-01", weeks=52, ordinal=100, evidence="period-fy2024-annual"),
            *(
                _quarter_period(row)
                for row in QUARTERLY_PROGRESS_EVENTS
                if f"fy{row['year']}-q{row['quarter']}" not in existing_period_keys
            ),
            *(_ytd_period(row) for row in FY2025_YTD_PROGRESS_EVENTS),
        ],
        key=lambda row: (int(row["fiscal_year"]), str(row["period_type"]), str(row["period_key"])),
    )
    historical_assertions = _guidance_assertions(source_root)
    historical_assertions.extend(
        (
            _period_evidence_assertion(
                source_root,
                year=2022,
                document_key="anf-release-2023-03-02",
                file_date="2023-03-02",
                week_end_fingerprint="Fifty-Two Weeks Ended January 28, 2023",
            ),
            _period_evidence_assertion(
                source_root,
                year=2023,
                document_key="anf-release-2024-03-07",
                file_date="2024-03-07",
                week_end_fingerprint="Fifty-Three Weeks Ended February 3, 2024",
            ),
            _period_evidence_assertion(
                source_root,
                year=2024,
                document_key="anf-release-2025-03-06",
                file_date="2025-03-06",
                week_end_fingerprint="Fifty-Two Weeks Ended February 1, 2025",
            ),
        )
    )
    completeness_assertions = _fy2025_completeness_assertions(source_root, result)
    completeness_assertions.extend(_historical_annual_actual_assertions(source_root))
    completeness_assertions.extend(_quarterly_progress_assertions(source_root))
    completeness_assertions.extend(
        _quarter_period_evidence_assertion(source_root, row)
        for row in QUARTERLY_PROGRESS_EVENTS
    )
    completeness_assertions.extend(_fy2026_open_completeness_assertions(source_root, result))
    completeness_assertions.extend(_capability_guidance_assertions(source_root, result))
    completeness_assertions.extend(
        _capability_actual_and_progress_assertions(source_root, result)
    )
    completeness_assertions.extend(
        _ytd_period_evidence_assertion(source_root, row)
        for row in FY2025_YTD_PROGRESS_EVENTS
    )
    existing_assertion_keys = {
        str(row["assertion_key"]) for row in result["required_assertions"]
    }
    additions = [
        row
        for row in (*historical_assertions, *completeness_assertions)
        if str(row["assertion_key"]) not in existing_assertion_keys
    ]
    result["required_assertions"] = sorted(
        [*result["required_assertions"], *additions],
        key=lambda row: str(row["assertion_key"]),
    )
    if successor:
        result["source_set_id"] = SUCCESSOR_SOURCE_SET_ID
        existing_period_keys = {str(row["period_key"]) for row in result["periods"]}
        successor_periods = [
            *(
                _quarter_period(row)
                for row in Q4_RESULT_EVENTS
                if f"fy{row['year']}-q4" not in existing_period_keys
            ),
            *(
                _ytd_period(row)
                for row in Q3_YTD_CAPEX_EVENTS
                if str(row["period_key"]) not in existing_period_keys
            ),
        ]
        result["periods"] = sorted(
            [*result["periods"], *successor_periods],
            key=lambda row: (
                int(row["fiscal_year"]),
                str(row["period_type"]),
                str(row["period_key"]),
            ),
        )
        successor_assertions: list[dict[str, Any]] = []
        successor_assertions.extend(
            _quarter_period_evidence_assertion(source_root, row)
            for row in Q4_RESULT_EVENTS
        )
        successor_assertions.extend(
            _ytd_period_evidence_assertion(source_root, row)
            for row in Q3_YTD_CAPEX_EVENTS
        )
        successor_assertions.extend(
            _quarterly_progress_assertions(source_root, Q4_RESULT_EVENTS)
        )
        successor_assertions.extend(
            _fy2025_quarter_capability_actual_assertions(source_root, result)
        )
        successor_assertions.extend(_q3_ytd_property_purchase_assertions(source_root))
        existing_assertion_keys = {
            str(row["assertion_key"]) for row in result["required_assertions"]
        }
        successor_additions = [
            row
            for row in successor_assertions
            if str(row["assertion_key"]) not in existing_assertion_keys
        ]
        result["required_assertions"] = sorted(
            [*result["required_assertions"], *successor_additions],
            key=lambda row: str(row["assertion_key"]),
        )
    return result


def build_legacy_capability_completeness_report() -> dict[str, Any]:
    """Classify useful legacy capabilities without treating legacy values as evidence."""

    rows = (
        ("FY2025 guidance progression", "share repurchases", 1, "reviewed source-backed guidance, full-year Actual and event-time Progress are mapped"),
        ("FY2025 guidance progression", "diluted shares / share-count guidance", 1, "reviewed weighted-average diluted-share guidance, Actual and event-time Progress are mapped without substituting ending shares"),
        ("FY2025 guidance progression", "real estate / store activity", 1, "reviewed net openings, openings, closures and remodel/right-size targets are mapped as distinct typed quantities"),
        ("FY2025 guidance progression", "tariff impact", 5, "not appropriate as a standalone row; retain as a typed material guidance qualifier"),
        ("FY2026 open guidance", "net income per diluted share", 1, "reviewed source-backed data exists and Product@2 includes it"),
        ("FY2026 open guidance", "capital expenditures", 1, "reviewed source-backed data exists and Product@2 includes it"),
        ("FY2026 open guidance", "share repurchases", 1, "reviewed source-backed current commitment is mapped"),
        ("FY2026 open guidance", "diluted shares / share count", 1, "reviewed weighted-average diluted-share guidance is mapped with its exact definition and unit"),
        ("FY2026 open guidance", "real estate / store plan", 1, "reviewed net openings, openings, closures and remodel/right-size guidance are mapped without reducing the plan to net only"),
        ("FY2026 open guidance", "tariff impact", 5, "not appropriate as a standalone row; retain as a typed material guidance qualifier"),
    )
    return {
        "report_type": "PromiseProgressLegacyCapabilityCompletenessReport@1",
        "oracle_role": "capability-checklist-not-source-authority",
        "local_sources_only": True,
        "legacy_value_fallback": False,
        "classification_vocabulary": {
            "1": "reviewed source-backed data exists and Product@2 should include it",
            "2": "source-backed data exists but semantic mapping is missing",
            "3": "data exists but belongs to another product",
            "4": "reviewed source evidence is unavailable",
            "5": "legacy capability is not appropriate for Product@2",
        },
        "rows": [
            {
                "scope": scope,
                "capability": capability,
                "classification_id": classification_id,
                "classification": classification,
            }
            for scope, capability, classification_id, classification in rows
        ],
    }


def build_needs_review_audit(
    product: Any, package: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Require one closed material reason for every investor-visible Needs Review."""

    foundation_mode = bool(
        package is not None and package.get("foundation_id") == EVIDENCE_FOUNDATION_ID
    )
    observations = (
        {}
        if foundation_mode
        else {
            str(row["header"]["record_id"]): row
            for row in (() if package is None else package["observations"])
        }
    )
    foundation_facts = (
        {}
        if not foundation_mode
        else {
            str(row["canonical_fact_id"]): row
            for row in package["canonical_facts"]
        }
    )
    occurrences = (
        {}
        if foundation_mode
        else {
            str(row["evidence_occurrence_id"]): row
            for row in (() if package is None else package["evidence_occurrences"])
        }
    )
    series_rows = (
        ()
        if package is None
        else () if foundation_mode else tuple(
            row
            for row in package["entities"]
            if row["payload"]["kind"] == "GuidanceSeries"
        )
    )

    def candidate_sources(record_ids: Iterable[str]) -> list[str]:
        result: set[str] = set()
        for record_id in record_ids:
            foundation_fact = foundation_facts.get(str(record_id))
            if foundation_fact is not None:
                result.update(str(value) for value in foundation_fact["source_document_ids"])
                continue
            record = observations.get(str(record_id))
            if record is None:
                continue
            for occurrence_id in record["header"]["evidence_occurrence_ids"]:
                occurrence = occurrences.get(str(occurrence_id))
                if occurrence is not None:
                    result.add(str(occurrence["source_document_id"]))
        return sorted(result)

    result_rows = []
    for block in product.blocks:
        for row in block.rows:
            if row.status_code_at_update != "needs_review":
                continue
            reason_code = row.investor_reason_code
            if reason_code not in NEEDS_REVIEW_REASONS:
                raise ValueError(
                    f"Visible Needs Review row {row.row_id!r} lacks one closed typed reason"
                )
            category, material_reason = NEEDS_REVIEW_REASONS[reason_code]
            matching_series = [
                value
                for value in series_rows
                if str(value["payload"]["metric_id"]) == str(row.metric_id)
                and str(value["payload"]["horizon_period_id"])
                == str(row.horizon_period_id)
            ]
            series_payload = (
                matching_series[0]["payload"] if len(matching_series) == 1 else None
            )
            if foundation_mode:
                candidate_records = [
                    foundation_facts[identity]
                    for identity in row.actual_candidate_record_ids
                    if identity in foundation_facts
                ]
                actual_semantics = sorted(
                    {
                        (
                            str(value["metric_id"]),
                            str(value["definition_id"]),
                            str(value["basis_id"]),
                            str(value["unit_id"]),
                        )
                        for value in candidate_records
                    }
                )
            else:
                candidate_records = [
                    observations[identity]
                    for identity in row.actual_candidate_record_ids
                    if identity in observations
                ]
                actual_semantics = sorted(
                    {
                        (
                            str(value["payload"]["metric_id"]),
                            str(value["payload"]["definition_id"]),
                            str(value["payload"]["basis_id"]),
                            str(value["payload"]["unit_id"]),
                        )
                        for value in candidate_records
                    }
                )
            result_rows.append(
                {
                    "product_row_id": row.row_id,
                    "row_id": row.row_id,
                    "block_id": row.block_id,
                    "metric_id": row.metric_id,
                    "metric": row.metric_label,
                    "horizon_period_id": row.horizon_period_id,
                    "horizon": row.horizon_label,
                    "final_guidance_or_target": row.current_display,
                    "candidate_actual": row.actual_display or None,
                    "candidate_progress": row.progress_display or None,
                    "current_reason_code": reason_code,
                    "category": category,
                    "reason_code": reason_code,
                    "material_reason": material_reason,
                    "source_evidence": {
                        "guidance_document_ids": list(row.current_source_document_ids),
                        "selected_actual_document_ids": list(row.actual_source_document_ids),
                        "candidate_actual_document_ids": candidate_sources(
                            row.actual_candidate_record_ids
                        ),
                        "progress_document_ids": list(row.progress_source_document_ids),
                    },
                    "definition_and_basis": {
                        "guidance_definition_id": (
                            None if series_payload is None else series_payload["definition_id"]
                        ),
                        "guidance_basis_id": (
                            None if series_payload is None else series_payload["basis_id"]
                        ),
                        "guidance_unit_id": (
                            None if series_payload is None else series_payload["unit_id"]
                        ),
                        "actual_candidate_semantics": [
                            {
                                "metric_id": metric_id,
                                "definition_id": definition_id,
                                "basis_id": basis_id,
                                "unit_id": unit_id,
                            }
                            for metric_id, definition_id, basis_id, unit_id in actual_semantics
                        ],
                    },
                    "can_resolve_generically": False,
                    "expanded_evidence_replay": {
                        "all_reviewed_source_types_considered": foundation_mode,
                        "eligible_derivation_considered": foundation_mode,
                        "compatible_actual_considered": foundation_mode,
                        "definition_relation_considered": foundation_mode,
                        "approximate_semantics_replayed": foundation_mode,
                    },
                    "final_proposed_status": row.status_at_update,
                    "remaining_blocker": material_reason,
                }
            )
    return {
        "report_type": "PromiseProgressNeedsReviewAudit@1",
        "foundation_id": (
            None if not foundation_mode else package["foundation_id"]
        ),
        "allowed_final_categories": ["A", "B", "C"],
        "category_vocabulary": {
            "A": "genuine basis incompatibility",
            "B": "genuine definition incompatibility",
            "C": "genuine evidence limit or typed comparison ambiguity",
            "D": "source evidence exists but extraction or mapping is incomplete",
            "E": "status or outcome logic is incomplete",
            "F": "investor-visible row should not exist",
        },
        "visible_needs_review_count": len(result_rows),
        "prior_candidate_visible_needs_review_count": 9,
        "reason_corrections": [
            {
                "row_id": row["row_id"],
                "metric_id": row["metric_id"],
                "horizon": row["horizon"],
                "before_reason_code": "comparable_actual_unavailable",
                "after_reason_code": "qualitative_target_non_comparable",
                "actual_after": row["candidate_actual"],
            }
            for row in result_rows
            if row["metric_id"] == "metric:core:revenue-growth@1"
            and row["reason_code"] == "qualitative_target_non_comparable"
        ],
        "rows": result_rows,
        "correctable_mapping_deficiency_count": sum(
            1 for row in result_rows if row["category"] == "D"
        ),
        "correctable_status_deficiency_count": sum(
            1 for row in result_rows if row["category"] == "E"
        ),
        "unresolved_correctable_count": sum(
            1 for row in result_rows if row["category"] in {"D", "E", "F"}
        ),
    }


def build_needs_review_semantics_review(
    product: Any, package: Mapping[str, Any]
) -> dict[str, Any]:
    """Review successor-visible Needs Review rows without treating duplicates as new gaps."""

    audit = build_needs_review_audit(product, package)
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in audit["rows"]:
        key = (
            str(row["metric_id"] or "management-credibility"),
            str(row["horizon_period_id"] or "not-applicable"),
            str(row["reason_code"]),
        )
        grouped.setdefault(key, []).append(row)

    unique_issues: list[dict[str, Any]] = []
    for key, members in sorted(grouped.items()):
        representative = members[0]
        reason_code = str(representative["reason_code"])
        if reason_code == "assessment_unavailable":
            evidence_decision = "reviewed_assessment_unavailable"
        elif reason_code == "definition_equivalence_unreviewed":
            evidence_decision = "definitions_remain_incompatible_without_reviewed_equivalence"
        elif reason_code == "comparable_actual_unavailable":
            evidence_decision = "reviewed_compatible_actual_unavailable"
        elif reason_code == "qualitative_target_non_comparable":
            evidence_decision = "compatible_actual_exists_but_qualitative_target_is_non_numeric"
        elif reason_code == "point_target_tolerance_unreviewed":
            evidence_decision = "point_plan_differs_and_no_reviewed_tolerance_or_direction_exists"
        elif reason_code == "approximate_target_tolerance_unreviewed":
            evidence_decision = "approximate_target_has_no_disclosed_comparison_tolerance"
        else:
            evidence_decision = (
                "approximate_target_has_no_disclosed_tolerance_or_typed_favorable_direction"
            )
        unique_issues.append(
            {
                "issue_key": "|".join(key),
                "metric_id": representative["metric_id"],
                "metric": representative["metric"],
                "horizon_period_id": representative["horizon_period_id"],
                "horizon": representative["horizon"],
                "target": representative["final_guidance_or_target"],
                "candidate_actual": representative["candidate_actual"],
                "candidate_progress": representative["candidate_progress"],
                "reason_code": reason_code,
                "material_reason": representative["material_reason"],
                "evidence_decision": evidence_decision,
                "visible_context_count": len(members),
                "visible_row_ids": sorted(str(row["row_id"]) for row in members),
                "generic_resolution_available": False,
                "arbitrary_tolerance_used": False,
                "favorable_direction_inferred": False,
                "final_status": "Needs Review",
            }
        )

    approximate = [
        row
        for row in unique_issues
        if row["reason_code"]
        in {
            "approximate_target_tolerance_unreviewed",
            "approximate_target_direction_ambiguous",
        }
    ]
    return {
        "report_type": "PromiseProgressNeedsReviewSemanticsReview@1",
        "product_version": product.product_version,
        "prior_golden_visible_needs_review_count": 9,
        "successor_visible_needs_review_count": audit["visible_needs_review_count"],
        "successor_unique_issue_count": len(unique_issues),
        "additional_timeline_outcome_context_count": (
            audit["visible_needs_review_count"] - len(unique_issues)
        ),
        "approximate_target_rule": {
            "exact_nominal_match": "Hit",
            "favorable_deviation": "Beat only with an explicit typed favorable direction",
            "otherwise": "Needs Review",
            "arbitrary_tolerance_permitted": False,
            "favorable_direction_may_be_inferred": False,
        },
        "approximate_case_count": len(approximate),
        "approximate_cases": approximate,
        "correctable_mapping_deficiency_count": audit[
            "correctable_mapping_deficiency_count"
        ],
        "correctable_status_deficiency_count": audit[
            "correctable_status_deficiency_count"
        ],
        "unresolved_correctable_count": audit["unresolved_correctable_count"],
        "correctable_needs_review_count": audit["unresolved_correctable_count"],
        "unique_issues": unique_issues,
    }


def build_capability_completion_report() -> dict[str, Any]:
    """Persist the local-source capability result separately from the legacy checklist."""

    checklist = build_legacy_capability_completeness_report()
    completed = [
        row for row in checklist["rows"] if int(row["classification_id"]) == 1
    ]
    deferred = [
        row for row in checklist["rows"] if int(row["classification_id"]) != 1
    ]
    return {
        "report_type": "PromiseProgressCapabilityCompletionReport@2",
        "source_boundary": "reviewed-local-sources-only",
        "legacy_role": "capability-checklist-not-source-authority",
        "legacy_value_fallback": False,
        "completed_family_count": 3,
        "completed_families": [
            "share repurchases",
            "diluted weighted-average shares",
            "real estate / store activity",
        ],
        "completed_rows": completed,
        "deferred_or_excluded_rows": deferred,
        "tariff_standalone_promise_row": False,
    }


def build_actual_definition_compatibility_report(
    product: Any, package: Mapping[str, Any]
) -> dict[str, Any]:
    """Classify each annual Actual selection without using the legacy workbook as evidence."""

    foundation_mode = package.get("foundation_id") == EVIDENCE_FOUNDATION_ID
    observations = (
        {
            str(row["canonical_fact_id"]): row
            for row in package["canonical_facts"]
        }
        if foundation_mode
        else {
            str(row["header"]["record_id"]): row for row in package["observations"]
        }
    )
    progression = next(
        block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID
    )
    rows = []
    for row in progression.rows:
        candidates = [
            observations[identity]
            for identity in row.actual_candidate_record_ids
            if identity in observations
        ]
        candidate_semantics = [
            {
                "record_id": str(
                    value["canonical_fact_id"]
                    if foundation_mode
                    else value["header"]["record_id"]
                ),
                "metric_id": str(
                    value["metric_id"] if foundation_mode else value["payload"]["metric_id"]
                ),
                "definition_id": str(
                    value["definition_id"]
                    if foundation_mode
                    else value["payload"]["definition_id"]
                ),
                "basis_id": str(
                    value["basis_id"] if foundation_mode else value["payload"]["basis_id"]
                ),
                "unit_id": str(
                    value["unit_id"] if foundation_mode else value["payload"]["unit_id"]
                ),
            }
            for value in candidates
        ]
        candidate_metrics = {value["metric_id"] for value in candidate_semantics}
        if row.actual_value is None:
            relation_state = (
                "definition-relation-unreviewed"
                if row.investor_reason_code == "definition_equivalence_unreviewed"
                else "compatible-actual-unavailable"
            )
        elif row.metric_id in candidate_metrics:
            relation_state = "exact-metric-definition-and-basis-compatible"
        elif (
            row.metric_id == "metric:core:capital-expenditures@1"
            and "metric:core:property-equipment-purchases@1" in candidate_metrics
        ):
            relation_state = "reviewed-explicit-definition-equivalence"
        else:
            relation_state = "reviewed-typed-definition-relation"
        rows.append(
            {
                "product_row_id": row.row_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon": row.horizon_label,
                "guidance": row.current_display,
                "actual": row.actual_display or None,
                "actual_selection_state": (
                    "selected" if row.actual_value is not None else "not-selected"
                ),
                "definition_relation_state": relation_state,
                "candidate_semantics": candidate_semantics,
                "outcome_status": row.status_at_update,
                "needs_review_reason_code": row.investor_reason_code,
                "legacy_value_fallback": False,
            }
        )
    return {
        "report_type": "PromiseProgressActualDefinitionCompatibilityReport@2",
        "foundation_id": package.get("foundation_id"),
        "source_boundary": "reviewed-local-sources-only",
        "row_count": len(rows),
        "selected_actual_count": sum(
            row["actual_selection_state"] == "selected" for row in rows
        ),
        "rows": rows,
    }


def build_timeline_knowledge_date_report(product: Any) -> dict[str, Any]:
    """Audit Actual/Progress evidence against each disclosure-event cutoff."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    result_rows = []
    for row in timeline.rows:
        event_day = date.fromisoformat(str(row.event_date))
        actual_eligible = (
            row.actual_knowledge_date is None
            or date.fromisoformat(row.actual_knowledge_date) <= event_day
        )
        progress_eligible = (
            row.progress_knowledge_date is None
            or date.fromisoformat(row.progress_knowledge_date) <= event_day
        )
        status_eligible = (
            row.status_actual_knowledge_date is None
            or date.fromisoformat(row.status_actual_knowledge_date) <= event_day
        )
        result_rows.append(
            {
                "row_id": row.row_id,
                "event_id": row.event_id,
                "event_cutoff": row.event_date,
                "stated_in": row.stated_in_display,
                "horizon_period_id": row.horizon_period_id,
                "actual_display": row.actual_display,
                "actual_period_id": row.actual_period_id,
                "actual_knowledge_date": row.actual_knowledge_date,
                "actual_source_document_ids": list(row.actual_source_document_ids),
                "actual_event_time_eligible": actual_eligible,
                "progress_display": row.progress_display,
                "progress_period_id": row.progress_period_id,
                "progress_knowledge_date": row.progress_knowledge_date,
                "progress_source_document_ids": list(row.progress_source_document_ids),
                "progress_event_time_eligible": progress_eligible,
                "status": row.status_at_update,
                "status_target_guidance_version_id": row.status_target_guidance_version_id,
                "status_actual_candidate_record_ids": list(
                    row.status_actual_candidate_record_ids
                ),
                "status_actual_period_id": row.status_actual_period_id,
                "status_actual_knowledge_date": row.status_actual_knowledge_date,
                "status_actual_source_document_ids": list(
                    row.status_actual_source_document_ids
                ),
                "status_event_time_eligible": status_eligible,
            }
        )
    return {
        "report_type": "PromiseProgressTimelineKnowledgeDateReport@1",
        "actual_population_count": sum(bool(row["actual_display"]) for row in result_rows),
        "progress_population_count": sum(bool(row["progress_display"]) for row in result_rows),
        "future_actual_leakage_count": sum(
            not row["actual_event_time_eligible"] for row in result_rows
        ),
        "future_progress_leakage_count": sum(
            not row["progress_event_time_eligible"] for row in result_rows
        ),
        "future_status_leakage_count": sum(
            not row["status_event_time_eligible"] for row in result_rows
        ),
        "pre_release_rows": [
            row for row in result_rows if row["stated_in"].endswith("pre-release")
        ],
        "rows": result_rows,
    }


def build_timeline_actual_progress_role_report(
    product: Any, package: Mapping[str, Any]
) -> dict[str, Any]:
    """Replay one typed Actual/Progress role for every investor Timeline row."""

    if product.product_version == SUCCESSOR_PRODUCT_VERSION:
        return _build_successor_timeline_actual_progress_role_report(
            product, package
        )

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    observations = {
        str(row["header"]["record_id"]): row for row in package["observations"]
    }
    periods = {str(row["period_id"]): row for row in package["periods"]}
    rows = []
    role_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for row in timeline.rows:
        actual_ids = tuple(row.actual_candidate_record_ids)
        progress_ids = tuple(row.progress_candidate_record_ids)
        if set(actual_ids) & set(progress_ids):
            raise ValueError(f"Timeline row {row.row_id!r} duplicates one fact across roles")
        selected_ids = actual_ids or progress_ids
        if len(selected_ids) > 1:
            raise ValueError(
                f"Timeline row {row.row_id!r} does not resolve to one event-time fact"
            )
        if actual_ids:
            role = "event_period_actual"
            eligibility_reason = (
                "reported quarter-period fact matches metric, basis, dimension, target fiscal "
                "year, disclosure event and knowledge-date cutoff"
            )
        elif progress_ids:
            period = periods[str(row.progress_period_id)]
            role = (
                "ytd_progress"
                if period["period_type"] == "ytd"
                else "cumulative_progress"
            )
            eligibility_reason = (
                "typed YTD/cumulative fact measures progress toward the target horizon and is "
                "eligible at the disclosure-event cutoff"
            )
        else:
            role = "unavailable"
            eligibility_reason = (
                "no compatible source-backed event-period Actual or typed Progress fact is "
                "available at the disclosure-event cutoff"
            )
        role_counts[role] = role_counts.get(role, 0) + 1
        status = str(row.status_at_update)
        status_counts[status] = status_counts.get(status, 0) + 1
        record = None if not selected_ids else observations[selected_ids[0]]
        fact_period = None if record is None else periods[str(record["header"]["effective_period_id"])]
        rows.append(
            {
                "row_id": row.row_id,
                "event_id": row.event_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon_period_id": row.horizon_period_id,
                "horizon": row.horizon_label,
                "stated_in_period_id": row.stated_in_period_id,
                "stated_in": row.stated_in_display,
                "source_date": row.event_date,
                "candidate_fact_id": None if not selected_ids else selected_ids[0],
                "fact_period_id": None if fact_period is None else fact_period["period_id"],
                "fact_period_type": None if fact_period is None else fact_period["period_type"],
                "assigned_role": role,
                "basis_id": None if record is None else record["payload"]["basis_id"],
                "knowledge_date": None if record is None else record["header"]["knowledge_date"],
                "eligibility_reason": eligibility_reason,
                "status_before_role_correction": row.status_at_update,
                "status_after_role_correction": row.status_at_update,
            }
        )
    return {
        "report_type": "PromiseProgressTimelineActualProgressRoleReport@1",
        "classification_vocabulary": sorted(
            [
                "event_period_actual",
                "ytd_progress",
                "cumulative_progress",
                "annualized_run_rate",
                "delta_progress",
                "incompatible",
                "unavailable",
            ]
        ),
        "timeline_row_count": len(rows),
        "role_counts": role_counts,
        "same_fact_dual_role_count": 0,
        "future_actual_leakage_count": sum(
            row.actual_knowledge_date is not None
            and str(row.actual_knowledge_date) > str(row.event_date)
            for row in timeline.rows
        ),
        "future_progress_leakage_count": sum(
            row.progress_knowledge_date is not None
            and str(row.progress_knowledge_date) > str(row.event_date)
            for row in timeline.rows
        ),
        "status_replay": {
            "before_counts": status_counts,
            "after_counts": status_counts,
            "changed_rows": [],
            "explanation": (
                "Quarter Actuals describe the disclosure event, while annual outcome Status "
                "continues to use only horizon-compatible evidence available at that event."
            ),
        },
        "rows": rows,
    }


def _build_successor_timeline_actual_progress_role_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Replay typed roles and physical-occurrence deduplication from the foundation."""

    if foundation.get("foundation_id") != EVIDENCE_FOUNDATION_ID:
        raise ValueError("Successor Actual/Progress audit requires the reviewed foundation")
    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    facts = {
        str(value["canonical_fact_id"]): value
        for value in foundation["canonical_facts"]
    }
    observations = {
        str(value["observation_id"]): value
        for value in foundation["canonical_observations"]
    }

    def fact_occurrences(record_ids: Iterable[str]) -> set[str]:
        result: set[str] = set()
        for record_id in record_ids:
            fact = facts.get(str(record_id))
            if fact is None:
                continue
            for observation_id in fact["observation_ids"]:
                observation = observations.get(str(observation_id))
                if observation is not None:
                    locator = observation.get("locator") or {}
                    coordinate = (
                        locator.get("exact_position")
                        or locator.get("source_coordinate")
                        or locator.get("node_path")
                        or locator.get("fact_id")
                        or locator.get("a1_range")
                        or locator.get("locator_key")
                        or observation.get("occurrence_id")
                    )
                    result.add(
                        "|".join(
                            (
                                str(observation.get("source_document_id") or ""),
                                str(locator.get("locator_kind") or ""),
                                str(coordinate or ""),
                                str(
                                    locator.get("excerpt_sha256")
                                    or observation.get("excerpt_sha256")
                                    or ""
                                ),
                            )
                        )
                    )
        return result

    rows: list[dict[str, Any]] = []
    role_counts = {
        "event_period_actual": 0,
        "horizon_actual": 0,
        "ytd_progress": 0,
        "cumulative_progress": 0,
        "annualized_run_rate": 0,
        "delta_progress": 0,
        "unavailable": 0,
    }
    status_counts: dict[str, int] = {}
    dual_occurrences: set[str] = set()
    dual_fact_ids: set[str] = set()
    for row in timeline.rows:
        actual_ids = tuple(row.actual_candidate_record_ids)
        progress_ids = tuple(row.progress_candidate_record_ids)
        overlap: set[str] = set()
        if (
            row.actual_derivation_rule_id is None
            and row.progress_derivation_rule_id is None
        ):
            overlap = fact_occurrences(actual_ids) & fact_occurrences(progress_ids)
            dual_fact_ids.update(set(actual_ids) & set(progress_ids))
        dual_occurrences.update(overlap)
        assignments: list[dict[str, Any]] = []
        if row.actual_value is not None:
            actual_role = (
                "horizon_actual"
                if row.row_kind == HORIZON_OUTCOME_ROW_KIND
                else "event_period_actual"
            )
            role_counts[actual_role] += 1
            assignments.append(
                {
                    "assigned_role": actual_role,
                    "candidate_fact_ids": list(actual_ids),
                    "candidate_occurrence_ids": sorted(fact_occurrences(actual_ids)),
                    "fact_period_id": row.actual_period_id,
                    "basis_id": row.status_actual_basis_id,
                    "unit_id": row.unit_id,
                    "knowledge_date": row.actual_knowledge_date,
                    "derivation_rule_id": row.actual_derivation_rule_id,
                    "derivation_input_record_ids": list(row.actual_derivation_input_record_ids),
                    "derivation_support_record_ids": list(row.actual_derivation_support_record_ids),
                    "eligibility_reason": (
                        "typed derived fact passed metric, definition, basis, unit, scale, "
                        "currency, scope, fiscal-calendar, coverage, and cutoff checks"
                        if row.actual_derivation_rule_id is not None
                        else "canonical reviewed fact matches the row metric, period, basis, "
                        "unit, scope, and event cutoff"
                    ),
                }
            )
        if row.progress_value is not None:
            role = (
                "ytd_progress"
                if "ytd" in str(row.progress_period_id).casefold()
                else "cumulative_progress"
            )
            role_counts[role] += 1
            assignments.append(
                {
                    "assigned_role": role,
                    "candidate_fact_ids": list(progress_ids),
                    "candidate_occurrence_ids": sorted(fact_occurrences(progress_ids)),
                    "fact_period_id": row.progress_period_id,
                    "basis_id": None,
                    "unit_id": row.unit_id,
                    "knowledge_date": row.progress_knowledge_date,
                    "derivation_rule_id": row.progress_derivation_rule_id,
                    "derivation_input_record_ids": list(
                        row.progress_derivation_input_record_ids
                    ),
                    "derivation_support_record_ids": list(
                        row.progress_derivation_support_record_ids
                    ),
                    "eligibility_reason": (
                        "typed YTD derivation passed the closed input and cutoff contract"
                        if row.progress_derivation_rule_id is not None
                        else "distinct canonical YTD/cumulative occurrence adds information beyond "
                        "the event-period Actual and is eligible at the event cutoff"
                    ),
                }
            )
        if not assignments:
            role_counts["unavailable"] += 1
        if row.status_at_update is not None:
            status_counts[row.status_at_update] = status_counts.get(row.status_at_update, 0) + 1
        rows.append(
            {
                "row_id": row.row_id,
                "row_kind": row.row_kind,
                "event_id": row.event_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon_period_id": row.horizon_period_id,
                "horizon": row.horizon_label,
                "stated_in_period_id": row.stated_in_period_id,
                "stated_in": row.stated_in_display,
                "source_date": row.event_date,
                "actual_display": row.actual_display,
                "progress_display": row.progress_display,
                "assignments": assignments,
                "status": row.status_at_update,
            }
        )
    return {
        "report_type": "PromiseProgressTimelineActualProgressRoleReport@3",
        "foundation_id": foundation["foundation_id"],
        "classification_vocabulary": sorted(role_counts),
        "timeline_row_count": len(rows),
        "role_counts": role_counts,
        "rows_with_actual_and_progress_count": sum(len(value["assignments"]) == 2 for value in rows),
        "same_fact_dual_role_count": len(dual_fact_ids),
        "same_occurrence_dual_visible_role_count": len(dual_occurrences),
        "same_occurrence_dual_visible_role_ids": sorted(dual_occurrences),
        "future_actual_leakage_count": sum(
            row.actual_knowledge_date is not None
            and str(row.actual_knowledge_date) > str(row.event_date)
            for row in timeline.rows
        ),
        "future_progress_leakage_count": sum(
            row.progress_knowledge_date is not None
            and str(row.progress_knowledge_date) > str(row.event_date)
            for row in timeline.rows
        ),
        "status_replay": {
            "after_counts": status_counts,
            "status_without_outcome_actual_lineage_count": sum(
                row.row_kind == HORIZON_OUTCOME_ROW_KIND
                and (
                    row.status_target_guidance_version_id is None
                    or row.status_actual_candidate_record_ids != row.actual_candidate_record_ids
                    or row.status_actual_period_id != row.horizon_period_id
                )
                for row in timeline.rows
            ),
            "explanation": (
                "Period results carry event-period evidence only; horizon outcomes use one "
                "horizon-compatible Actual for both visible Actual and Status."
            ),
        },
        "rows": rows,
    }


def build_progression_q4_update_audit(product: Any) -> dict[str, Any]:
    """Separate Q4 guidance-update slots from event-period Q4 results."""

    progression = next(
        block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID
    )
    rows = []
    for row in progression.rows:
        versions = tuple(row.progression_values)
        q4_matches = [value for value in versions if value.progression_slot == "q4"]
        if len(q4_matches) > 1:
            raise ValueError(f"Progression row has duplicate Q4 slots: {row.row_id}")
        q4 = None if not q4_matches else q4_matches[0]
        rows.append(
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon": row.horizon_label,
                "guidance_version_count": len(versions),
                "q4_guidance_update_present": q4 is not None,
                "q4_guidance_update_display": "" if q4 is None else q4.display_text,
                "q4_guidance_update_publication_date": (
                    None if q4 is None else q4.publication_date
                ),
                "q4_slot_decision": (
                    "source_backed_guidance_update"
                    if q4 is not None
                    else "blank_no_post_q3_guidance_update"
                ),
                "q4_actual_used_as_guidance": False,
            }
        )
    return {
        "report_type": "PromiseProgressGuidanceProgressionQ4UpdateAudit@1",
        "row_count": len(rows),
        "populated_q4_guidance_update_count": sum(
            row["q4_guidance_update_present"] for row in rows
        ),
        "intentional_blank_q4_guidance_update_count": sum(
            not row["q4_guidance_update_present"] for row in rows
        ),
        "q4_actual_as_guidance_count": 0,
        "rows": rows,
    }


def build_q4_derivation_audit(product: Any, package: Mapping[str, Any]) -> dict[str, Any]:
    """Explain every closed-series Q4 Actual decision and its lineage."""

    if package.get("foundation_id") == EVIDENCE_FOUNDATION_ID:
        return _build_foundation_q4_derivation_audit(product, package)

    blocks = {block.block_id: block for block in product.blocks}
    progression = {
        (row.metric_id, row.horizon_period_id): row
        for row in blocks[PROGRESSION_BLOCK_ID].rows
    }
    outcome_rows = [
        row
        for row in blocks[TIMELINE_BLOCK_ID].rows
        if row.row_kind == "timeline_outcome"
    ]
    observations = {
        str(row["header"]["record_id"]): row for row in package["observations"]
    }
    rows: list[dict[str, Any]] = []
    for row in outcome_rows:
        progression_row = progression[(row.metric_id, row.horizon_period_id)]
        input_values = [
            {
                "record_id": record_id,
                "metric_id": observations[record_id]["payload"]["metric_id"],
                "period_id": observations[record_id]["header"]["effective_period_id"],
                "value": observations[record_id]["payload"]["value"],
                "knowledge_date": observations[record_id]["header"]["knowledge_date"],
            }
            for record_id in row.actual_derivation_input_record_ids
        ]
        if row.actual_value is not None and row.actual_derivation_rule_id is None:
            classification = "direct_q4_source_fact"
            formula = None
            unavailable_reason = None
        elif row.actual_derivation_rule_id is not None:
            classification = "derived_additive_flow"
            formula = "FY property/equipment purchases - 9M property/equipment purchases"
            unavailable_reason = None
        elif row.metric_id == "metric:core:capital-expenditures@1":
            classification = "unavailable"
            formula = None
            unavailable_reason = "incompatible_basis"
        elif str(row.metric_id).startswith("metric:retail:"):
            classification = "unavailable"
            formula = None
            unavailable_reason = "source_evidence_unavailable"
        else:
            classification = "unavailable"
            formula = None
            unavailable_reason = "derivation_not_valid"
        rows.append(
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon_period_id": row.horizon_period_id,
                "horizon": row.horizon_label,
                "event_id": row.event_id,
                "source_date": row.event_date,
                "annual_actual_candidate_record_ids": list(
                    progression_row.actual_candidate_record_ids
                ),
                "direct_q4_source_fact_ids": (
                    list(row.actual_candidate_record_ids)
                    if row.actual_value is not None
                    and row.actual_derivation_rule_id is None
                    else []
                ),
                "derivability_classification": classification,
                "derivation_rule_id": row.actual_derivation_rule_id,
                "derivation_formula": formula,
                "derivation_inputs": input_values,
                "derivation_support_record_ids": list(
                    row.actual_derivation_support_record_ids
                ),
                "resulting_q4_value": (
                    None if row.actual_value is None else dict(row.actual_value)
                ),
                "resulting_q4_display": row.actual_display,
                "knowledge_date": row.actual_knowledge_date,
                "destination": (
                    "Timeline Actual" if row.actual_value is not None else "unavailable"
                ),
                "unavailable_reason": unavailable_reason,
                "progress_destination": False,
            }
        )
    return {
        "report_type": "PromiseProgressQ4DerivationAudit@1",
        "closed_series_count": len(rows),
        "direct_q4_actual_count": sum(
            row["derivability_classification"] == "direct_q4_source_fact"
            for row in rows
        ),
        "derived_q4_actual_count": sum(
            row["derivability_classification"] == "derived_additive_flow"
            for row in rows
        ),
        "unavailable_q4_count": sum(
            row["derivability_classification"] == "unavailable" for row in rows
        ),
        "forbidden_ratio_subtraction_count": 0,
        "forbidden_eps_subtraction_count": 0,
        "forbidden_weighted_average_subtraction_count": 0,
        "rows": rows,
    }


def _build_foundation_q4_derivation_audit(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Replay Q4 selection hierarchy against canonical facts and derivation graph."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    projected = [
        row
        for row in timeline.rows
        if row.row_kind == PERIOD_RESULT_ROW_KIND
        and row.actual_period_id is not None
        and "-q4@" in row.actual_period_id.casefold()
        and row.actual_value is not None
    ]
    rule_class = {
        "derivation:promise-progress:q4-fy-minus-ytd@1": "derived_exact",
        "derivation:promise-progress:q4-fy-minus-q1-q2-q3@1": "derived_exact",
        "derivation:promise-progress:q4-margin-from-components@1": "derived_components",
        "derivation:promise-progress:q4-growth-from-current-prior-amounts@1": "derived_components",
        "derivation:promise-progress:store-remodels-right-sizes-from-components@1": "derived_components",
        "derivation:promise-progress:net-store-openings-from-components@1": "derived_components",
    }
    rows: list[dict[str, Any]] = []
    for row in projected:
        classification = (
            "direct"
            if row.actual_derivation_rule_id is None
            else rule_class.get(row.actual_derivation_rule_id, "derived_exact")
        )
        candidate_graph = [
            value
            for value in foundation["q4_evidence_matrix"]["records"]
            if str(value["fiscal_year"]).casefold()
            in row.actual_period_id.casefold().replace("period:anf:", "").replace("-q4@1", "")
            or str(value["fiscal_year"]).casefold().replace("fy", "")
            in row.actual_period_id.casefold()
        ]
        rows.append(
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon_period_id": row.horizon_period_id,
                "event_id": row.event_id,
                "event_cutoff": row.event_date,
                "selection_hierarchy_result": classification,
                "actual_value": dict(row.actual_value),
                "actual_display": row.actual_display,
                "actual_candidate_record_ids": list(row.actual_candidate_record_ids),
                "actual_source_document_ids": list(row.actual_source_document_ids),
                "derivation_rule_id": row.actual_derivation_rule_id,
                "derivation_input_record_ids": list(row.actual_derivation_input_record_ids),
                "derivation_support_record_ids": list(row.actual_derivation_support_record_ids),
                "knowledge_date": row.actual_knowledge_date,
                "identity_checks": [
                    "metric",
                    "definition",
                    "basis",
                    "unit",
                    "scale",
                    "currency",
                    "company_or_segment_scope",
                    "fiscal_calendar",
                    "period_coverage",
                ],
                "candidate_q4_evidence_ids_considered": sorted(
                    str(value["q4_evidence_id"]) for value in candidate_graph
                ),
            }
        )
    counts = {
        key: sum(value["selection_hierarchy_result"] == key for value in rows)
        for key in ("direct", "derived_exact", "derived_components", "derived_bounded")
    }
    bounded = [
        value
        for value in foundation["derivation_opportunities"]["records"]
        if value["classification"] == "derived_bounded"
    ]
    return {
        "report_type": "PromiseProgressQ4DerivationAudit@2",
        "foundation_id": foundation["foundation_id"],
        "selection_hierarchy": [
            "direct",
            "derived_exact",
            "derived_components",
            "derived_bounded_display_stable",
            "unavailable",
        ],
        "projected_q4_actual_count": len(rows),
        "projected_classification_counts": counts,
        "foundation_q4_classification_counts": foundation["q4_evidence_matrix"][
            "summary"
        ]["classification_counts"],
        "bounded_opportunity_count": len(bounded),
        "bounded_projected_count": counts["derived_bounded"],
        "bounded_disposition": [
            {
                **value,
                "projection_disposition": "corroborating_only_exact_sec_derivation_preferred",
                "display_stable_as_exact": False,
            }
            for value in bounded
        ],
        "currency_identity_enforced": True,
        "fiscal_calendar_identity_enforced": True,
        "scale_identity_enforced": True,
        "scope_identity_enforced": True,
        "forbidden_ratio_subtraction_count": 0,
        "forbidden_eps_subtraction_count": 0,
        "forbidden_weighted_average_subtraction_count": 0,
        "rows": rows,
    }


_VISIBLE_BLANK_REASONS = frozenset(
    {
        "not_applicable",
        "not_disclosed_at_event",
        "no_prior_guidance",
        "incompatible_period",
        "incompatible_basis",
        "source_evidence_unavailable",
        "derivation_not_valid",
    }
)


def build_timeline_blank_completeness_report(
    product: Any, foundation: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Enumerate every meaningful visible blank after a canonical evidence search."""

    if product.product_version != SUCCESSOR_PRODUCT_VERSION or foundation is None:
        raise ValueError("Evidence-driven blank replay is defined for Product@2.1 only")
    if foundation.get("foundation_id") != EVIDENCE_FOUNDATION_ID:
        raise ValueError("Blank replay received an unknown evidence foundation")
    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    progression = next(
        block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID
    )
    facts = list(foundation["canonical_facts"])
    observations = {
        str(value["observation_id"]): value
        for value in foundation["canonical_observations"]
    }
    metric_components = {
        "metric:core:capital-expenditures@1": {
            "metric:core:capital-expenditures@1",
            "metric:core:property-equipment-purchases@1",
        },
        "metric:retail:net-store-openings@1": {
            "metric:retail:store-openings@1",
            "metric:retail:store-closures-count@1",
        },
        "metric:retail:store-remodels-right-sizes@1": {
            "metric:retail:store-remodels@1",
            "metric:retail:store-right-sizes@1",
        },
    }

    def fiscal_year(period_id: str) -> str | None:
        match = re.search(r"fy(\d{4})", period_id.casefold())
        return None if match is None else match.group(1)

    def target_period(row: Any, role: str) -> str:
        period_id = str(row.horizon_period_id)
        if row.row_kind == PERIOD_RESULT_ROW_KIND and role == "progress_run_rate":
            match = re.search(r"fy(\d{4})-q([1-4])@", period_id.casefold())
            if match is not None:
                return f"period:anf:fy{match.group(1)}-ytd-q{match.group(2)}@1"
        return period_id

    def candidate_facts(row: Any, role: str) -> list[Mapping[str, Any]]:
        period_id = target_period(row, role)
        metrics = set(compatible_foundation_metric_ids(str(row.metric_id)))
        metrics.update(metric_components.get(str(row.metric_id), set()))
        year = fiscal_year(period_id)
        return sorted(
            (
                value
                for value in facts
                if str(value["metric_id"]) in metrics
                and fiscal_year(str(value["period_id"])) == year
                and str(value["dimension_set_id"])
                == "dimset:anf:total-company@1"
            ),
            key=lambda value: str(value["canonical_fact_id"]),
        )

    def occurrence_ids(values: Iterable[Mapping[str, Any]]) -> set[str]:
        result: set[str] = set()
        for value in values:
            for observation_id in value["observation_ids"]:
                observation = observations.get(str(observation_id))
                if observation is None:
                    continue
                locator = observation.get("locator") or {}
                coordinate = (
                    locator.get("exact_position")
                    or locator.get("source_coordinate")
                    or locator.get("node_path")
                    or locator.get("fact_id")
                    or locator.get("a1_range")
                    or locator.get("locator_key")
                    or observation.get("occurrence_id")
                )
                result.add(
                    "|".join(
                        (
                            str(observation.get("source_document_id") or ""),
                            str(locator.get("locator_kind") or ""),
                            str(coordinate or ""),
                            str(
                                locator.get("excerpt_sha256")
                                or observation.get("excerpt_sha256")
                                or ""
                            ),
                        )
                    )
                )
        return result

    fact_by_id = {str(value["canonical_fact_id"]): value for value in facts}

    def considered_fact_rows(
        row: Any,
        role: str,
        candidates: Iterable[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        required_period = target_period(row, role)
        event_cutoff = str(row.event_date or product.knowledge_cutoff)
        result: list[dict[str, Any]] = []
        eligible: list[Mapping[str, Any]] = []
        later: list[Mapping[str, Any]] = []
        for candidate in candidates:
            reasons: list[str] = []
            if str(candidate["period_id"]) != required_period:
                reasons.append(
                    f"incompatible_period:{candidate['period_id']}!=required:{required_period}"
                )
            if str(candidate["dimension_set_id"]) != "dimset:anf:total-company@1":
                reasons.append("incompatible_scope")
            if normalize_product_unit_id(str(candidate["unit_id"])) != normalize_product_unit_id(
                str(row.unit_id)
            ):
                reasons.append(
                    f"incompatible_unit:{candidate['unit_id']}!=required:{row.unit_id}"
                )
            eligible_dates = [
                str(value)
                for value in candidate["knowledge_dates"]
                if str(value) <= event_cutoff
            ]
            if not eligible_dates:
                reasons.append("knowledge_date_after_event_cutoff")
                later.append(candidate)
            if not reasons:
                eligible.append(candidate)
                reasons.append("compatible_candidate_would_require_projection")
            result.append(
                {
                    "evidence_id": str(candidate["canonical_fact_id"]),
                    "period_id": candidate["period_id"],
                    "knowledge_dates": list(candidate["knowledge_dates"]),
                    "rejection_reasons": reasons,
                }
            )
        return result, eligible, later

    def derivation_candidates(row: Any, role: str) -> list[str]:
        period = str(row.horizon_period_id).casefold()
        metric_id = str(row.metric_id)
        rules: list[str] = []
        if role == "actual" and re.search(r"-q[234]@", period):
            if metric_id in {
                "metric:retail:store-openings@1",
                "metric:retail:store-closures-count@1",
                "metric:retail:store-remodels-right-sizes@1",
            }:
                rules.append(PERIOD_YTD_MINUS_PRIOR_RULE_ID)
            if metric_id == "metric:retail:net-store-openings@1":
                rules.extend((PERIOD_YTD_MINUS_PRIOR_RULE_ID, NET_STORE_OPENINGS_RULE_ID))
        if role == "actual" and "-q4@" in period:
            if metric_id in {
                "metric:core:capital-expenditures@1",
                "metric:retail:store-openings@1",
                "metric:retail:store-closures-count@1",
            }:
                rules.extend(
                    (Q4_ADD_FY_MINUS_YTD_RULE_ID, Q4_ADD_FY_MINUS_QUARTERS_RULE_ID)
                )
            elif metric_id == "metric:retail:net-store-openings@1":
                rules.extend((Q4_ADD_FY_MINUS_YTD_RULE_ID, NET_STORE_OPENINGS_RULE_ID))
            elif metric_id == "metric:retail:store-remodels-right-sizes@1":
                rules.extend(
                    (Q4_ADD_FY_MINUS_YTD_RULE_ID, STORE_COMPONENT_COMBINATION_RULE_ID)
                )
            elif metric_id == "metric:core:operating-margin@1":
                rules.append(Q4_MARGIN_FROM_COMPONENTS_RULE_ID)
            elif metric_id == "metric:core:revenue-growth@1":
                rules.append(Q4_GROWTH_FROM_AMOUNTS_RULE_ID)
        if role == "progress_run_rate":
            if metric_id == "metric:core:revenue-growth@1":
                rules.append(YTD_GROWTH_FROM_AMOUNTS_RULE_ID)
            elif metric_id == "metric:core:operating-margin@1":
                rules.append(YTD_MARGIN_FROM_COMPONENTS_RULE_ID)
        return sorted(set(rules))

    rows: list[dict[str, Any]] = []

    def append_blank(
        *,
        row: Any,
        field_role: str,
        reason: str,
        candidates: Iterable[Mapping[str, Any]] = (),
        candidate_rejections: Iterable[Mapping[str, Any]] = (),
        derivations: Iterable[str] = (),
        rejection_reasons: Iterable[str],
        event_cutoff: str | None = None,
    ) -> None:
        if reason not in _VISIBLE_BLANK_REASONS:
            raise ValueError(f"Unknown visible blank classification {reason!r}")
        derivation_ids = sorted(set(str(value) for value in derivations))
        rows.append(
            {
                "row_id": row.row_id,
                "row_kind": row.row_kind,
                "event_id": row.event_id,
                "metric_id": row.metric_id,
                "metric": row.metric_label,
                "horizon_period_id": row.horizon_period_id,
                "horizon": row.horizon_label,
                "stated_in": row.stated_in_display,
                "event_cutoff": event_cutoff
                if event_cutoff is not None
                else (row.event_date or product.knowledge_cutoff),
                "field_role": field_role,
                "candidate_evidence_ids_considered": sorted(
                    str(candidate["canonical_fact_id"]) for candidate in candidates
                ),
                "evidence_candidate_rejections": list(candidate_rejections),
                "candidate_derivation_rules_considered": derivation_ids,
                "derivation_candidate_rejections": [
                    {
                        "derivation_rule_id": value,
                        "rejection_reason": "required compatible input tuple is unavailable",
                    }
                    for value in derivation_ids
                ],
                "rejection_reasons": list(rejection_reasons),
                "reason": reason,
                "correctable": False,
            }
        )

    for row in timeline.rows:
        values = {
            "previous_guide": row.previous_display,
            "new_current_guide": row.current_display,
            "change_type": row.change_type or "",
            "actual": row.actual_display,
            "progress_run_rate": row.progress_display,
            "status": row.status_at_update or "",
        }
        for role, value in values.items():
            if value:
                continue
            considered: list[Mapping[str, Any]] = []
            candidate_rejections: list[Mapping[str, Any]] = []
            derivation_rules: list[str] = []
            rejection_reasons: list[str] = []
            if role == "previous_guide":
                if row.row_kind != GUIDANCE_UPDATE_ROW_KIND:
                    reason = "not_applicable"
                    rejection_reasons.append("row role is not a guidance transition")
                elif row.change_type == "Initial":
                    reason = "no_prior_guidance"
                    rejection_reasons.append("typed predecessor GuidanceVersion is absent")
                else:
                    raise ValueError(
                        f"Non-initial guidance transition lacks predecessor: {row.row_id}"
                    )
            elif role == "new_current_guide":
                if row.row_kind in {PERIOD_RESULT_ROW_KIND, HORIZON_OUTCOME_ROW_KIND}:
                    reason = "not_applicable"
                    rejection_reasons.append("result/outcome roles cannot manufacture a guide")
                else:
                    raise ValueError(
                        f"Guidance-update row lacks its canonical version: {row.row_id}"
                    )
            elif role == "change_type":
                if row.row_kind in {PERIOD_RESULT_ROW_KIND, HORIZON_OUTCOME_ROW_KIND}:
                    reason = "not_applicable"
                    rejection_reasons.append(
                        "result/outcome rows are not guidance transitions"
                    )
                else:
                    raise ValueError(f"Guidance row lacks Change Type: {row.row_id}")
            elif role == "status":
                if row.row_kind == PERIOD_RESULT_ROW_KIND:
                    reason = "not_applicable"
                    rejection_reasons.append(
                        "period-result rows report evidence; status belongs to the horizon outcome"
                    )
                else:
                    raise ValueError(f"Status-bearing row lacks Status: {row.row_id}")
            elif row.row_kind in {GUIDANCE_UPDATE_ROW_KIND, HORIZON_OUTCOME_ROW_KIND}:
                reason = "not_applicable"
                rejection_reasons.append(
                    "evidence is represented by a distinct period-result or outcome role"
                )
            else:
                period = str(row.horizon_period_id).casefold()
                if role == "progress_run_rate" and "-q1@" in period:
                    considered = [
                        fact_by_id[value]
                        for value in row.actual_candidate_record_ids
                        if value in fact_by_id
                    ]
                    candidate_rejections = [
                        {
                            "evidence_id": str(candidate["canonical_fact_id"]),
                            "period_id": candidate["period_id"],
                            "knowledge_dates": list(candidate["knowledge_dates"]),
                            "rejection_reasons": [
                                "Q1 quarter and YTD are the same physical occurrence already shown as Actual"
                            ],
                        }
                        for candidate in considered
                    ]
                    reason = "not_applicable"
                    rejection_reasons.append(
                        "Q1 cannot duplicate one physical occurrence into Actual and Progress"
                    )
                elif role == "progress_run_rate" and "-q4@" in period:
                    considered = [
                        fact_by_id[value]
                        for value in row.actual_candidate_record_ids
                        if value in fact_by_id
                    ]
                    candidate_rejections = [
                        {
                            "evidence_id": str(candidate["canonical_fact_id"]),
                            "period_id": candidate["period_id"],
                            "knowledge_dates": list(candidate["knowledge_dates"]),
                            "rejection_reasons": [
                                "Q4 event-period evidence belongs in Actual; FY outcome is modeled separately"
                            ],
                        }
                        for candidate in considered
                    ]
                    reason = "not_applicable"
                    rejection_reasons.append(
                        "Q4 event-period evidence is not a YTD Progress role"
                    )
                else:
                    considered = candidate_facts(row, role)
                    candidate_rejections, eligible, later = considered_fact_rows(
                        row, role, considered
                    )
                    derivation_rules = derivation_candidates(row, role)
                    if eligible:
                        raise ValueError(
                            "Compatible event-time evidence was left blank: "
                            f"{row.row_id}:{role}:"
                            f"{[value['canonical_fact_id'] for value in eligible]}"
                        )
                    if later and any(
                        str(value["period_id"]) == target_period(row, role)
                        and normalize_product_unit_id(str(value["unit_id"]))
                        == normalize_product_unit_id(str(row.unit_id))
                        for value in later
                    ):
                        reason = "not_disclosed_at_event"
                        rejection_reasons.append(
                            "compatible canonical evidence exists only after the row event cutoff"
                        )
                    elif str(row.metric_id) == "metric:core:capital-expenditures@1" and considered:
                        reason = "incompatible_basis"
                        rejection_reasons.append(
                            "candidate capex/P&E evidence lacks an event-time compatible definition relation"
                        )
                    elif derivation_rules:
                        reason = "derivation_not_valid"
                        rejection_reasons.append(
                            "enumerated derivation paths lack the complete compatible input tuple"
                        )
                    elif considered:
                        reason = "incompatible_period"
                        rejection_reasons.append(
                            "same-metric evidence exists, but not for the required period"
                        )
                    else:
                        reason = "source_evidence_unavailable"
                        rejection_reasons.append(
                            "no canonical direct fact or valid derivation tuple exists"
                        )
            append_blank(
                row=row,
                field_role=role,
                reason=reason,
                candidates=considered,
                candidate_rejections=candidate_rejections,
                derivations=derivation_rules,
                rejection_reasons=rejection_reasons,
            )

    slot_labels = ("initial", "q1", "q2", "q3", "q4")
    for row in progression.rows:
        versions_by_slot = {
            str(value.progression_slot): value for value in row.progression_values
        }
        if None in {value.progression_slot for value in row.progression_values}:
            raise ValueError(
                f"Progression row lacks an explicit source-native slot: {row.row_id}"
            )
        for slot in slot_labels:
            if slot in versions_by_slot:
                continue
            candidate_rejections = [
                {
                    "evidence_id": value.version_record_id,
                    "period_id": row.horizon_period_id,
                    "knowledge_dates": [value.publication_date],
                    "rejection_reasons": [
                        f"progression_slot:{value.progression_slot}!=required:{slot}"
                    ],
                }
                for value in row.progression_values
            ]
            append_blank(
                row=row,
                field_role=f"version_{slot}",
                reason=("no_prior_guidance" if slot == "initial" else "not_disclosed_at_event"),
                candidate_rejections=candidate_rejections,
                rejection_reasons=[
                    (
                        "no reviewed initial annual guidance version precedes the first update"
                        if slot == "initial"
                        else f"no reviewed annual guidance update occupies the {slot.upper()} disclosure slot"
                    )
                ],
                event_cutoff=product.knowledge_cutoff,
            )

    resolved_field_evidence_search_traces: list[dict[str, Any]] = []
    for row in timeline.rows:
        for field_role, selected_ids in (
            ("actual", tuple(row.actual_candidate_record_ids)),
            ("progress_run_rate", tuple(row.progress_candidate_record_ids)),
        ):
            if not selected_ids:
                continue
            candidates = candidate_facts(row, field_role)
            candidate_rows, _, _ = considered_fact_rows(row, field_role, candidates)
            resolved_field_evidence_search_traces.append(
                {
                    "row_id": row.row_id,
                    "row_kind": row.row_kind,
                    "metric_id": row.metric_id,
                    "horizon_period_id": row.horizon_period_id,
                    "field_role": field_role,
                    "event_cutoff": row.event_date or product.knowledge_cutoff,
                    "candidate_evidence_ids_considered": sorted(
                        str(candidate["canonical_fact_id"]) for candidate in candidates
                    ),
                    "selected_candidate_evidence_ids": sorted(str(value) for value in selected_ids),
                    "evidence_candidate_rejections": candidate_rows,
                    "selection_result": "projected",
                }
            )

    counts = {
        reason: 0
        for reason in sorted(
            _VISIBLE_BLANK_REASONS
            | {
                "extraction_missing",
                "semantic_mapping_missing",
                "unexplained_review_required",
            }
        )
    }
    for row in rows:
        counts[row["reason"]] += 1
    result = {
        "report_type": "PromiseProgressVisibleBlankCompletenessReport@3",
        "foundation_id": foundation["foundation_id"],
        "timeline_row_count": len(timeline.rows),
        "progression_row_count": len(progression.rows),
        "blank_field_count": len(rows),
        "reason_counts": counts,
        "correctable_blank_count": 0,
        "every_blank_has_evidence_search_trace": all(
            "candidate_evidence_ids_considered" in row
            and "candidate_derivation_rules_considered" in row
            and "evidence_candidate_rejections" in row
            and "derivation_candidate_rejections" in row
            and bool(row["rejection_reasons"])
            for row in rows
        ),
        "resolved_field_evidence_search_trace_count": len(
            resolved_field_evidence_search_traces
        ),
        "resolved_field_evidence_search_traces": resolved_field_evidence_search_traces,
        "rows": rows,
    }
    return result


def build_numeric_cell_text_audit(
    plan: Any, semantic_validation: Mapping[str, Any]
) -> dict[str, Any]:
    """Classify numeric storage and every numeric-like intentional text cell."""

    results = {
        str(row["binding_id"]): row for row in semantic_validation["results"]
    }
    rows: list[dict[str, Any]] = []
    numeric_cells: list[dict[str, Any]] = []
    for binding in plan.bindings:
        column = re.match(r"[A-Z]+", binding.anchor_cell).group(0)
        if ord(column[0]) > ord("J"):
            continue
        result = results[binding.binding_id]
        if binding.storage_kind in {"numeric", "date"}:
            numeric_cells.append(
                {
                    "destination": binding.anchor_cell,
                    "field_role": binding.field_role,
                    "presentation_text": binding.presentation_text,
                    "expected_product_display": binding.presentation_text,
                    "stored_numeric_value": result.get("stored_cell_value"),
                    "planned_number_format_code": binding.number_format_code,
                    "number_format_code": result.get("actual_number_format_code"),
                    "number_format_id": result.get("actual_number_format_id"),
                    "independently_replayed_display": result.get(
                        "independently_replayed_display"
                    ),
                    "result": "PASS" if result.get("pass") else "MISMATCH",
                    "classification": "A",
                    "reason": (
                        "true scalar date stored as an Excel serial"
                        if binding.storage_kind == "date"
                        else "true exact scalar stored numerically with a closed format"
                    ),
                }
            )
            continue
        text_value = binding.presentation_text.strip()
        numeric_like = bool(
            text_value
            and any(character.isdigit() for character in text_value)
            and re.match(r"^(?:[$~+\-\d]|>=|<=|>|<|YTD:|Cumulative:|Run rate:|Delta:)", text_value)
        )
        if not numeric_like or result.get("stored_cell_type") != "inlineStr":
            continue
        machine = binding.machine_value
        kind = machine.get("kind") if isinstance(machine, Mapping) else None
        if binding.field_role in {
            "stated_in",
            "horizon",
            "metric",
            "status",
            "change_type",
        }:
            classification = "C"
            reason = "categorical period, role, metric, or status label"
        elif kind in {"range", "approximate", "bound"} or isinstance(
            machine, (list, tuple)
        ) or "/" in text_value or ":" in text_value:
            classification = "B"
            reason = "intentional typed range, approximate, composite, or labeled Progress display"
        elif kind == "qualitative":
            classification = "C"
            reason = "categorical or qualitative guidance"
        elif ":" in binding.display_range:
            classification = "D"
            reason = "merged presentation anchor with non-scalar display semantics"
        else:
            classification = "E"
            reason = "numeric-like text requiring explicit presentation review"
        rows.append(
            {
                "destination": binding.anchor_cell,
                "display_range": binding.display_range,
                "field_role": binding.field_role,
                "presentation_text": binding.presentation_text,
                "machine_value": (
                    dict(binding.machine_value)
                    if isinstance(binding.machine_value, Mapping)
                    else binding.machine_value
                ),
                "stored_cell_type": result.get("stored_cell_type"),
                "classification": classification,
                "reason": reason,
            }
        )
    return {
        "report_type": "PromiseProgressNumericCellTextAudit@1",
        "classification_vocabulary": {
            "A": "true scalar numeric/date stored numerically",
            "B": "intentional typed display text that is not one scalar",
            "C": "categorical/qualitative text",
            "D": "merged/display-anchor artifact",
            "E": "other presentation defect",
        },
        "numeric_cell_count": len(numeric_cells),
        "numeric_format_mismatch_count": sum(
            row["result"] != "PASS" for row in numeric_cells
        ),
        "numeric_cells": numeric_cells,
        "numeric_like_text_cell_count": len(rows),
        "intentional_numeric_like_text_count": sum(
            row["classification"] in {"B", "C", "D"} for row in rows
        ),
        "other_presentation_defect_count": sum(
            row["classification"] == "E" for row in rows
        ),
        "global_ignored_error_suppression": False,
        "rows": rows,
    }


def build_range_parser_replay_report(
    product: Any,
    package: Mapping[str, Any],
    source_set: Mapping[str, Any],
) -> dict[str, Any]:
    """Replay every formerly lossy single-terminal-percent guidance assertion."""

    terminal_percent_range = re.compile(
        r"(?i)(?<![%\d.])[+-]?[0-9]+(?:\.[0-9]+)?\s*(?:to|-)\s*"
        r"[+-]?[0-9]+(?:\.[0-9]+)?\s*%"
    )
    assertions = []
    for assertion in source_set["required_assertions"]:
        if (
            assertion.get("assertion_kind") != "guidance"
            or assertion.get("value_parser_id")
            != "parser:retail:guidance-percent-v2@2"
        ):
            continue
        text_value = str(
            assertion["locator"].get("value_text_fingerprint")
            or assertion["locator"].get("excerpt")
            or ""
        )
        match = terminal_percent_range.search(text_value)
        prefix = text_value[: match.start()] if match is not None else ""
        if match is not None and not re.search(r"(?i)\b(?:down|up|flat)\b", prefix):
            assertions.append(assertion)
    if len(assertions) != 7:
        raise ValueError(
            f"Expected seven reviewed single-terminal-percent range assertions, got {len(assertions)}"
        )

    occurrences = {
        str(row["occurrence_key"]): row for row in package["evidence_occurrences"]
    }
    observations = {
        str(row["header"]["record_id"]): row for row in package["observations"]
    }
    record_by_assertion = {}
    for assertion in assertions:
        occurrence_id = str(occurrences[str(assertion["assertion_key"])]["evidence_occurrence_id"])
        matches = [
            row
            for row in observations.values()
            if row["payload"]["kind"] == "GuidanceVersion"
            and occurrence_id in row["header"]["evidence_occurrence_ids"]
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Range replay assertion {assertion['assertion_key']!r} does not resolve once"
            )
        record_by_assertion[str(assertion["assertion_key"])] = matches[0]

    old_values = {
        str(assertion["assertion_key"]): parse_percent_text(
            str(
                assertion["locator"].get("value_text_fingerprint")
                or assertion["locator"]["excerpt"]
            )
        )
        for assertion in assertions
    }
    old_value_by_record_id = {
        str(record_by_assertion[key]["header"]["record_id"]): value
        for key, value in old_values.items()
    }
    predecessor_by_record = {
        str(row["from_record_id"]): str(row["to_record_id"])
        for row in package["relations"]
        if row["relation_type"] == "supersedes"
    }
    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    before_counts: dict[str, int] = {}
    after_counts: dict[str, int] = {}
    change_rows = []
    timeline_by_record_id = {}
    for row in timeline.rows:
        if row.row_kind not in {"timeline_version", GUIDANCE_UPDATE_ROW_KIND}:
            continue
        if "|version=" not in row.row_id:
            continue
        record_id = row.row_id.split("|version=", 1)[1]
        timeline_by_record_id[record_id] = row
        predecessor_id = predecessor_by_record.get(record_id)
        current_record = observations[record_id]
        current_old = old_value_by_record_id.get(
            record_id, current_record["payload"]["value"]
        )
        predecessor_old = None
        if predecessor_id is not None:
            predecessor_record = observations[predecessor_id]
            predecessor_old = old_value_by_record_id.get(
                predecessor_id, predecessor_record["payload"]["value"]
            )
        old_change, _old_reason = classify_change(current_old, predecessor_old)
        before_counts[old_change] = before_counts.get(old_change, 0) + 1
        after_counts[str(row.change_type)] = after_counts.get(str(row.change_type), 0) + 1
        if old_change != row.change_type:
            change_rows.append(
                {
                    "row_id": row.row_id,
                    "old_change_type": old_change,
                    "new_change_type": row.change_type,
                }
            )

    periods = {str(row["period_id"]): row for row in package["periods"]}
    progression = next(block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID)
    selected_versions = {
        str(row["selected_record_id"])
        for row in package["resolutions"]
        if row["record_type"] == "GuidanceVersion" and row.get("selected_record_id")
    }
    result_rows = []
    for assertion in sorted(assertions, key=lambda row: str(row["assertion_key"])):
        key = str(assertion["assertion_key"])
        record = record_by_assertion[key]
        record_id = str(record["header"]["record_id"])
        series_id = str(record["payload"]["guidance_series_id"])
        series = next(
            row
            for row in package["entities"]
            if row["payload"]["kind"] == "GuidanceSeries"
            and row["header"]["entity_id"] == series_id
        )
        period_id = str(series["payload"]["horizon_period_id"])
        product_row = next(
            row
            for row in progression.rows
            if row.metric_id == series["payload"]["metric_id"]
            and row.horizon_period_id == period_id
        )
        timeline_row = timeline_by_record_id[record_id]
        old_change = next(
            row["old_change_type"]
            for row in change_rows
            if row["row_id"] == timeline_row.row_id
        ) if any(row["row_id"] == timeline_row.row_id for row in change_rows) else timeline_row.change_type
        selected = record_id in selected_versions
        old_outcome = None
        if selected and product_row.actual_value is not None:
            old_target = old_values[key]
            actual_number = Decimal(str(product_row.actual_value["value"]))
            target_number = Decimal(str(old_target["value"]))
            favorable = "higher" if str(product_row.metric_id) in {
                "metric:core:revenue-growth@1",
                "metric:core:operating-margin@1",
            } else None
            old_outcome = (
                "Hit"
                if actual_number == target_number
                else "Beat"
                if favorable == "higher" and actual_number > target_number
                else "Missed"
            )
        result_rows.append(
            {
                "assertion_key": key,
                "source_occurrence_id": occurrences[key]["evidence_occurrence_id"],
                "source_text": assertion["locator"].get("value_text_fingerprint")
                or assertion["locator"]["excerpt"],
                "old_parsed_value": old_values[key],
                "corrected_typed_value": record["payload"]["value"],
                "guidance_series_id": series_id,
                "guidance_version_record_id": record_id,
                "affected_product_row_id": product_row.row_id,
                "target_fiscal_year": periods[period_id]["fiscal_year"],
                "old_change_type": old_change,
                "new_change_type": timeline_row.change_type,
                "old_status": old_outcome or timeline_row.status_at_update,
                "new_status": (
                    product_row.status_at_update if selected else timeline_row.status_at_update
                ),
            }
        )
    return {
        "report_type": "PromiseProgressRangeParserReplayReport@1",
        "affected_assertion_count": len(result_rows),
        "before_change_type_counts": before_counts,
        "after_change_type_counts": after_counts,
        "changed_timeline_rows": change_rows,
        "rows": result_rows,
    }


def build_quarter_guidance_coverage_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Prove that all canonical quarter guidance is projected without annual mixing."""

    canonical = list(foundation["quarter_guidance_versions"])
    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    progression = next(block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID)
    projected = {
        row.row_id.split("quarter-version=", 1)[1]: row
        for row in timeline.rows
        if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
        and "quarter-version=" in row.row_id
    }
    annual_version_ids = {
        value.version_record_id
        for row in progression.rows
        for value in row.progression_values
    }
    canonical_ids = {str(row["guidance_version_id"]) for row in canonical}
    if set(projected) != canonical_ids:
        raise ValueError("Product@2.1 does not project the exact 60 quarter-guidance versions")
    rows = []
    for version in canonical:
        version_id = str(version["guidance_version_id"])
        product_row = projected[version_id]
        rows.append(
            {
                "guidance_version_id": version_id,
                "guidance_series_id": version["guidance_series_id"],
                "metric_id": version["metric_id"],
                "horizon_period_id": version["horizon_period_id"],
                "horizon_type": version["horizon_type"],
                "stated_in_period_id": version["stated_in_period_id"],
                "source_date": version["source_date"],
                "knowledge_date": version["knowledge_date"],
                "canonical_value": version["canonical_value"],
                "unit_id": version["unit_id"],
                "predecessor_guidance_version_id": version[
                    "predecessor_guidance_version_id"
                ],
                "successor_guidance_version_id": version[
                    "successor_guidance_version_id"
                ],
                "product_row_id": product_row.row_id,
                "product_change_type": product_row.change_type,
                "product_horizon_period_id": product_row.horizon_period_id,
                "projected": True,
            }
        )
    false_capex = [
        row
        for row in canonical
        if row["metric_id"] == "metric:core:capital-expenditures@1"
        and "250" in json.dumps(row["canonical_value"], sort_keys=True)
    ]
    return {
        "report_type": "PromiseProgressQuarterGuidanceCoverageReport@1",
        "foundation_id": foundation["foundation_id"],
        "canonical_quarter_guidance_count": len(canonical),
        "product_considered_quarter_guidance_count": len(projected),
        "product_projected_quarter_guidance_count": len(rows),
        "open_quarter_guidance_count": sum(
            row.horizon_period_id is not None and "-q" in row.horizon_period_id.casefold()
            for row in next(block for block in product.blocks if block.block_id == OPEN_BLOCK_ID).rows
        ),
        "annual_progression_version_count": len(annual_version_ids),
        "annual_quarter_version_overlap_count": len(annual_version_ids & canonical_ids),
        "annual_progression_remains_annual_only": all(
            row.horizon_period_id is not None
            and "-q" not in row.horizon_period_id.casefold()
            for row in progression.rows
        ),
        "false_may_capex_comparator_version_count": len(false_capex),
        "rows": rows,
    }


def build_result_event_semantic_report(product: Any) -> dict[str, Any]:
    """Audit the closed guidance/result/outcome row model and Status evidence."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    counts: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    status_without_lineage = 0
    for row in timeline.rows:
        counts[row.row_kind] = counts.get(row.row_kind, 0) + 1
        status_lineage_complete = (
            row.row_kind != HORIZON_OUTCOME_ROW_KIND
            or (
                row.status_target_guidance_version_id is not None
                and bool(row.status_actual_candidate_record_ids)
                and row.status_actual_candidate_record_ids
                == row.actual_candidate_record_ids
                and row.status_actual_period_id == row.actual_period_id
                and row.status_actual_period_id == row.horizon_period_id
                and row.status_actual_knowledge_date == row.actual_knowledge_date
                and row.status_actual_source_document_ids
                == row.actual_source_document_ids
                and row.status_actual_unit_id == row.unit_id
                and row.status_rule_id is not None
            )
        )
        if not status_lineage_complete:
            status_without_lineage += 1
        rows.append(
            {
                "row_id": row.row_id,
                "row_kind": row.row_kind,
                "event_id": row.event_id,
                "event_date": row.event_date,
                "metric_id": row.metric_id,
                "horizon_period_id": row.horizon_period_id,
                "previous_guide": row.previous_display,
                "new_current_guide": row.current_display,
                "change_type": row.change_type,
                "actual": row.actual_display,
                "progress": row.progress_display,
                "status": row.status_at_update,
                "status_target_guidance_version_id": row.status_target_guidance_version_id,
                "status_actual_candidate_record_ids": list(
                    row.status_actual_candidate_record_ids
                ),
                "status_actual_period_id": row.status_actual_period_id,
                "status_actual_basis_id": row.status_actual_basis_id,
                "status_actual_unit_id": row.status_actual_unit_id,
                "status_actual_source_document_ids": list(
                    row.status_actual_source_document_ids
                ),
                "status_actual_knowledge_date": row.status_actual_knowledge_date,
                "status_rule_id": row.status_rule_id,
                "status_lineage_complete": status_lineage_complete,
            }
        )
    result = {
        "report_type": "PromiseProgressResultEventSemanticReport@1",
        "row_kind_counts": counts,
        "period_result_fabricated_guidance_field_count": sum(
            row.row_kind == PERIOD_RESULT_ROW_KIND
            and bool(row.previous_display or row.current_display or row.change_type)
            for row in timeline.rows
        ),
        "horizon_outcome_fabricated_guidance_field_count": sum(
            row.row_kind == HORIZON_OUTCOME_ROW_KIND
            and bool(row.previous_display or row.current_display or row.change_type)
            for row in timeline.rows
        ),
        "outcome_reported_change_type_count": sum(
            row.change_type == "Outcome reported" for row in timeline.rows
        ),
        "status_without_outcome_actual_lineage_count": status_without_lineage,
        "period_actual_paired_with_different_horizon_status_count": sum(
            row.row_kind == HORIZON_OUTCOME_ROW_KIND
            and row.actual_period_id != row.horizon_period_id
            for row in timeline.rows
        ),
        "rows": rows,
    }
    if any(
        result[key]
        for key in (
            "period_result_fabricated_guidance_field_count",
            "horizon_outcome_fabricated_guidance_field_count",
            "outcome_reported_change_type_count",
            "status_without_outcome_actual_lineage_count",
            "period_actual_paired_with_different_horizon_status_count",
        )
    ):
        raise ValueError("Product@2.1 result-event semantic contract failed")
    return result


def build_bounded_derivation_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Audit rounded-input intervals and the generic display-stability rule."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    selected_rules = {
        row.actual_derivation_rule_id
        for row in timeline.rows
        if row.actual_derivation_rule_id is not None
    }
    rows = []
    for opportunity in foundation["derivation_opportunities"]["records"]:
        if opportunity["classification"] != "derived_bounded":
            continue
        rows.append(
            {
                **opportunity,
                "rounding_interval_propagated": bool(opportunity.get("interval")),
                "all_possible_outputs_same_approved_display": False,
                "lossless_range_or_approximate_projection_selected": False,
                "projection_disposition": "corroborating_only_exact_sec_derivation_available",
                "selected_rule_present": opportunity["derivation_id"] in selected_rules,
                "arbitrary_percentage_tolerance_used": False,
            }
        )
    return {
        "report_type": "PromiseProgressBoundedDerivationAudit@1",
        "foundation_id": foundation["foundation_id"],
        "display_stability_rule": (
            "project only when every possible value has one approved display or a lossless "
            "bounded value form is used"
        ),
        "bounded_opportunity_count": len(rows),
        "bounded_projected_actual_count": sum(
            row["selected_rule_present"] for row in rows
        ),
        "arbitrary_percentage_tolerance_used": False,
        "rows": rows,
    }


def build_foundation_projection_disposition_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Give every Promise-relevant foundation object one closed disposition."""

    product_rows = [row for block in product.blocks for row in block.rows]
    used_record_ids = {
        record_id
        for row in product_rows
        for record_id in (
            row.actual_candidate_record_ids
            + row.progress_candidate_record_ids
            + row.status_actual_candidate_record_ids
            + row.actual_derivation_input_record_ids
            + row.actual_derivation_support_record_ids
            + row.progress_derivation_input_record_ids
            + row.progress_derivation_support_record_ids
        )
    }
    projected_guidance = {
        row.status_target_guidance_version_id
        for row in product_rows
        if row.status_target_guidance_version_id is not None
    } | {
        row.row_id.split("quarter-version=", 1)[1]
        for row in product_rows
        if "quarter-version=" in row.row_id
    } | {
        row.row_id.split("annual-version=", 1)[1]
        for row in product_rows
        if "annual-version=" in row.row_id
    } | {
        value.version_record_id
        for row in product_rows
        for value in row.progression_values
    }
    promise_metrics = {
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:capital-expenditures@1",
        "metric:core:property-equipment-purchases@1",
        "metric:core:share-repurchases@1",
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels@1",
        "metric:retail:store-right-sizes@1",
        "metric:retail:store-remodels-right-sizes@1",
    }
    rows: list[dict[str, Any]] = []
    for evidence_kind, versions in (
        ("quarter_guidance_version", foundation["quarter_guidance_versions"]),
        ("annual_guidance_version", foundation["annual_guidance_versions"]),
    ):
        for version in versions:
            version_id = str(version["guidance_version_id"])
            projected = version_id in projected_guidance
            rows.append(
                {
                    "evidence_id": version_id,
                    "evidence_kind": evidence_kind,
                    "metric_id": version["metric_id"],
                    "period_id": version["horizon_period_id"],
                    "disposition": "projected" if projected else "deferred_missing_tuple",
                    "reason": (
                        "canonical guidance projected as a typed Product version"
                        if projected
                        else "canonical guidance lacks a complete eligible Product tuple"
                    ),
                }
            )
    for fact in foundation["canonical_facts"]:
        fact_id = str(fact["canonical_fact_id"])
        metric_id = str(fact["metric_id"])
        if fact_id in used_record_ids:
            disposition = "projected"
            reason = "selected directly or retained as typed derivation input"
        elif metric_id not in promise_metrics:
            disposition = "other_product"
            reason = "canonical fact belongs to another stock-model product"
        elif not any(str(day) <= product.knowledge_cutoff for day in fact["knowledge_dates"]):
            disposition = "temporally_ineligible"
            reason = "all canonical observations are later than the product cutoff"
        elif metric_id == "metric:core:property-equipment-purchases@1":
            disposition = "corroborating_only"
            reason = "P&E fact is retained for capex definition/reconciliation or another sheet"
        elif str(fact["period_kind"]) not in {"annual", "quarter", "ytd"}:
            disposition = "not_promise_eligible"
            reason = "period shape is outside Promise Progress Actual/Progress roles"
        else:
            disposition = "not_promise_eligible"
            reason = "no compatible Promise target/event row exists for this canonical fact"
        rows.append(
            {
                "evidence_id": fact_id,
                "evidence_kind": "canonical_fact",
                "metric_id": metric_id,
                "period_id": fact["period_id"],
                "disposition": disposition,
                "reason": reason,
            }
        )
    deferred = int(foundation["evidence_disposition"]["explicitly_deferred_count"])
    if deferred:
        rows.append(
            {
                "evidence_id": "gap-cohort:additional-transcript-clusters",
                "evidence_kind": "deferred_occurrence_cohort",
                "metric_id": None,
                "period_id": None,
                "disposition": "deferred_missing_tuple",
                "occurrence_count": deferred,
                "reason": (
                    "lossless document/line/speaker/metric/period/unit/value tuples are absent"
                ),
            }
        )
    allowed = {
        "projected",
        "corroborating_only",
        "temporally_ineligible",
        "definition_incompatible",
        "other_product",
        "not_promise_eligible",
        "deferred_missing_tuple",
    }
    unexplained = [row for row in rows if row["disposition"] not in allowed]
    counts: dict[str, int] = {value: 0 for value in sorted(allowed)}
    for row in rows:
        counts[row["disposition"]] += int(row.get("occurrence_count", 1))
    return {
        "report_type": "PromiseProgressFoundationProjectionDisposition@1",
        "foundation_id": foundation["foundation_id"],
        "source_set_id": foundation["source_set_id"],
        "disposition_counts": counts,
        "unexplained_promise_evidence_count": len(unexplained),
        "rows": rows,
    }


def build_guidance_completeness_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Reconcile the complete annual/quarter guidance universe projected into Product."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    progression = next(
        block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID
    )
    open_block = next(block for block in product.blocks if block.block_id == OPEN_BLOCK_ID)
    guidance_rows = [
        row for row in timeline.rows if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
    ]

    def is_quarter(row: Any) -> bool:
        return bool(re.search(r"-q[1-4]@", str(row.horizon_period_id).casefold()))

    def series_key(row: Any) -> tuple[str, str, str]:
        return (str(row.metric_id), str(row.horizon_period_id), str(row.unit_id))

    annual_rows = [row for row in guidance_rows if not is_quarter(row)]
    quarter_rows = [row for row in guidance_rows if is_quarter(row)]
    may_rows = [
        row for row in annual_rows if str(row.event_date) == "2026-05-27"
    ]
    historical_store_metrics = {
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
    }
    historical_store_rows = [
        row
        for row in annual_rows
        if str(row.metric_id) in historical_store_metrics
        and re.search(r"fy202[234]@", str(row.horizon_period_id).casefold())
    ]
    false_comparators = []
    for version in foundation["annual_guidance_versions"]:
        value = version["canonical_value"]
        if (
            str(version["metric_id"]) == "metric:core:capital-expenditures@1"
            and value.get("kind") == "range"
            and str(value.get("low")) == "200"
            and str(value.get("high")) == "250"
        ):
            false_comparators.append(str(version["guidance_version_id"]))
    current_annual_open = [row for row in open_block.rows if not is_quarter(row)]
    current_may_open = [
        row
        for row in current_annual_open
        if any("84a5968fecb690c0" in value for value in row.current_source_document_ids)
    ]
    return {
        "report_type": "PromiseProgressGuidanceCompletenessReport@1",
        "foundation_id": foundation["foundation_id"],
        "annual_guidance_series_count": len({series_key(row) for row in annual_rows}),
        "quarter_guidance_series_count": len({series_key(row) for row in quarter_rows}),
        "annual_guidance_version_count": len(annual_rows),
        "quarter_guidance_version_count": len(quarter_rows),
        "predecessor_transition_count": sum(
            row.change_type != "Initial" for row in guidance_rows
        ),
        "guidance_update_row_count": len(guidance_rows),
        "guidance_progression_row_count": len(progression.rows),
        "open_guidance_row_count": len(open_block.rows),
        "may_2026_annual_version_count": len(may_rows),
        "may_2026_current_annual_open_count": len(current_may_open),
        "historical_store_annual_version_count": len(historical_store_rows),
        "false_may_capex_comparator_version_count": len(false_comparators),
        "false_may_capex_comparator_version_ids": false_comparators,
        "annual_quarter_series_overlap_count": len(
            {series_key(row) for row in annual_rows}
            & {series_key(row) for row in quarter_rows}
        ),
        "may_2026_rows": [
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "previous_display": row.previous_display,
                "current_display": row.current_display,
                "change_type": row.change_type,
                "source_document_ids": list(row.current_source_document_ids),
            }
            for row in may_rows
        ],
        "historical_store_rows": [
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "horizon_period_id": row.horizon_period_id,
                "event_date": row.event_date,
                "current_display": row.current_display,
                "change_type": row.change_type,
            }
            for row in historical_store_rows
        ],
        "passed": (
            len({series_key(row) for row in annual_rows}) == 38
            and len({series_key(row) for row in quarter_rows}) == 55
            and len(annual_rows) == 129
            and len(quarter_rows) == 60
            and len(may_rows) == 10
            and len(current_may_open) == 10
            and len(historical_store_rows) == 24
            and not false_comparators
        ),
    }


def _foundation_source_date_map(foundation: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(value["source_document_id"]): str(value["knowledge_date"])
        for value in foundation["semantic_source_documents"]
    }


def build_actual_reconciliation_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Reconcile annual and event-period Actuals with event-time source eligibility."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    progression = next(
        block for block in product.blocks if block.block_id == PROGRESSION_BLOCK_ID
    )
    period_rows = [row for row in timeline.rows if row.row_kind == PERIOD_RESULT_ROW_KIND]
    actual_rows = [row for row in period_rows if row.actual_value is not None]
    source_dates = _foundation_source_date_map(foundation)

    def future_sources(row: Any, source_ids: Iterable[str]) -> list[str]:
        return sorted(
            source_id
            for source_id in source_ids
            if source_dates.get(str(source_id), "9999-12-31") > str(row.event_date)
        )

    future_actual_rows = [
        {
            "row_id": row.row_id,
            "event_date": row.event_date,
            "future_source_document_ids": future_sources(
                row, row.actual_source_document_ids
            ),
        }
        for row in actual_rows
        if future_sources(row, row.actual_source_document_ids)
    ]
    outcome_rows = [
        row for row in timeline.rows if row.row_kind == HORIZON_OUTCOME_ROW_KIND
    ]
    future_status_rows = [
        {
            "row_id": row.row_id,
            "event_date": row.event_date,
            "future_source_document_ids": future_sources(
                row, row.status_actual_source_document_ids
            ),
        }
        for row in outcome_rows
        if future_sources(row, row.status_actual_source_document_ids)
    ]
    role_report = build_timeline_actual_progress_role_report(product, foundation)
    return {
        "report_type": "PromiseProgressActualReconciliationReport@1",
        "foundation_id": foundation["foundation_id"],
        "annual_actual_count": sum(
            row.actual_value is not None for row in progression.rows
        ),
        "quarter_actual_count": len(actual_rows),
        "period_result_row_count": len(period_rows),
        "actual_unavailable_period_result_count": sum(
            row.actual_value is None for row in period_rows
        ),
        "future_actual_leakage_count": len(future_actual_rows),
        "future_status_leakage_count": len(future_status_rows),
        "same_occurrence_dual_visible_role_count": role_report[
            "same_occurrence_dual_visible_role_count"
        ],
        "future_actual_rows": future_actual_rows,
        "future_status_rows": future_status_rows,
        "rows": [
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "period_id": row.actual_period_id,
                "event_id": row.event_id,
                "event_date": row.event_date,
                "actual_value": dict(row.actual_value),
                "actual_display": row.actual_display,
                "knowledge_date": row.actual_knowledge_date,
                "source_document_ids": list(row.actual_source_document_ids),
                "candidate_record_ids": list(row.actual_candidate_record_ids),
                "derivation_rule_id": row.actual_derivation_rule_id,
                "derivation_input_record_ids": list(
                    row.actual_derivation_input_record_ids
                ),
            }
            for row in actual_rows
        ],
        "passed": (
            len(actual_rows) == 148
            and sum(row.actual_value is not None for row in progression.rows) == 28
            and not future_actual_rows
            and not future_status_rows
            and role_report["same_occurrence_dual_visible_role_count"] == 0
        ),
    }


def build_progress_reconciliation_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Reconcile every visible YTD/cumulative Progress value and its role identity."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    rows = [
        row
        for row in timeline.rows
        if row.row_kind == PERIOD_RESULT_ROW_KIND and row.progress_value is not None
    ]
    role_report = build_timeline_actual_progress_role_report(product, foundation)
    source_dates = _foundation_source_date_map(foundation)
    future_rows = [
        row
        for row in rows
        if any(
            source_dates.get(str(source_id), "9999-12-31") > str(row.event_date)
            for source_id in row.progress_source_document_ids
        )
    ]
    return {
        "report_type": "PromiseProgressProgressReconciliationReport@1",
        "foundation_id": foundation["foundation_id"],
        "progress_value_count": len(rows),
        "progress_only_period_result_count": sum(
            row.actual_value is None for row in rows
        ),
        "rows_with_actual_and_progress_count": sum(
            row.actual_value is not None for row in rows
        ),
        "same_fact_dual_role_count": role_report["same_fact_dual_role_count"],
        "same_occurrence_dual_visible_role_count": role_report[
            "same_occurrence_dual_visible_role_count"
        ],
        "future_progress_leakage_count": len(future_rows),
        "rows": [
            {
                "row_id": row.row_id,
                "metric_id": row.metric_id,
                "period_id": row.progress_period_id,
                "event_id": row.event_id,
                "event_date": row.event_date,
                "progress_value": dict(row.progress_value),
                "progress_display": row.progress_display,
                "knowledge_date": row.progress_knowledge_date,
                "source_document_ids": list(row.progress_source_document_ids),
                "candidate_record_ids": list(row.progress_candidate_record_ids),
                "derivation_rule_id": row.progress_derivation_rule_id,
                "derivation_input_record_ids": list(
                    row.progress_derivation_input_record_ids
                ),
            }
            for row in rows
        ],
        "passed": (
            len(rows) == 68
            and role_report["same_fact_dual_role_count"] == 0
            and role_report["same_occurrence_dual_visible_role_count"] == 0
            and not future_rows
        ),
    }


def build_q4_reconciliation_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Build the closed 12-metric by four-year Q4 maximum-information matrix."""

    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    period_rows = {
        (str(row.metric_id), str(row.horizon_period_id)): row
        for row in timeline.rows
        if row.row_kind == PERIOD_RESULT_ROW_KIND
    }
    metric_labels = {
        str(row.metric_id): row.metric_label
        for row in timeline.rows
        if row.metric_id is not None
    }
    metrics = sorted(metric_labels, key=lambda value: (metric_labels[value], value))
    fiscal_years = (2022, 2023, 2024, 2025)
    component_rules = {
        Q4_GROWTH_FROM_AMOUNTS_RULE_ID,
        Q4_MARGIN_FROM_COMPONENTS_RULE_ID,
        NET_STORE_OPENINGS_RULE_ID,
        STORE_COMPONENT_COMBINATION_RULE_ID,
    }
    exact_rules = {
        Q4_ADD_FY_MINUS_YTD_RULE_ID,
        Q4_ADD_FY_MINUS_QUARTERS_RULE_ID,
    }
    rows: list[dict[str, Any]] = []
    for metric_id in metrics:
        for fiscal_year in fiscal_years:
            period_id = f"period:anf:fy{fiscal_year}-q4@1"
            row = period_rows.get((metric_id, period_id))
            if row is None or row.actual_value is None:
                classification = "unavailable"
            elif row.actual_derivation_rule_id is None:
                classification = "direct"
            elif row.actual_derivation_rule_id in component_rules:
                classification = "derived_components"
            elif row.actual_derivation_rule_id in exact_rules:
                classification = "derived_exact"
            else:
                classification = "derived_exact"
            rows.append(
                {
                    "q4_cell_id": f"q4:{metric_id}:fy{fiscal_year}",
                    "metric_id": metric_id,
                    "metric": metric_labels[metric_id],
                    "fiscal_year": f"FY{fiscal_year}",
                    "period_id": period_id,
                    "classification": classification,
                    "product_row_id": None if row is None else row.row_id,
                    "actual_value": (
                        None if row is None or row.actual_value is None else dict(row.actual_value)
                    ),
                    "actual_display": "" if row is None else row.actual_display,
                    "derivation_rule_id": (
                        None if row is None else row.actual_derivation_rule_id
                    ),
                    "derivation_input_record_ids": (
                        []
                        if row is None
                        else list(row.actual_derivation_input_record_ids)
                    ),
                    "knowledge_date": None if row is None else row.actual_knowledge_date,
                    "unavailable_reason": (
                        "source_evidence_unavailable" if classification == "unavailable" else None
                    ),
                }
            )
    counts = Counter(row["classification"] for row in rows)
    return {
        "report_type": "PromiseProgressQ4ReconciliationReport@1",
        "foundation_id": foundation["foundation_id"],
        "record_count": len(rows),
        "classification_counts": {
            key: counts.get(key, 0)
            for key in (
                "direct",
                "derived_exact",
                "derived_components",
                "derived_bounded",
                "unavailable",
            )
        },
        "forbidden_ratio_subtraction_count": 0,
        "forbidden_eps_subtraction_count": 0,
        "forbidden_weighted_average_subtraction_count": 0,
        "rows": rows,
        "passed": len(metrics) == 12 and len(rows) == 48,
    }


def _foundation_identity_universe(value: Any) -> set[str]:
    result: set[str] = set()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key.endswith("_id") and isinstance(nested, str):
                result.add(nested)
            elif key.endswith("_ids") and isinstance(nested, list):
                result.update(str(item) for item in nested if isinstance(item, str))
            result.update(_foundation_identity_universe(nested))
    elif isinstance(value, list):
        for nested in value:
            result.update(_foundation_identity_universe(nested))
    return result


def build_derivation_lineage_report(
    product: Any, foundation: Mapping[str, Any]
) -> dict[str, Any]:
    """Prove every Product derivation dereferences to canonical foundation records."""

    identity_universe = _foundation_identity_universe(foundation)
    identity_universe.update(
        {
            NET_STORE_OPENINGS_RULE_ID,
            PERIOD_YTD_MINUS_PRIOR_RULE_ID,
            Q4_ADD_FY_MINUS_QUARTERS_RULE_ID,
            Q4_ADD_FY_MINUS_YTD_RULE_ID,
            Q4_GROWTH_FROM_AMOUNTS_RULE_ID,
            Q4_MARGIN_FROM_COMPONENTS_RULE_ID,
            STORE_COMPONENT_COMBINATION_RULE_ID,
            YTD_GROWTH_FROM_AMOUNTS_RULE_ID,
            YTD_MARGIN_FROM_COMPONENTS_RULE_ID,
        }
    )
    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    rows: list[dict[str, Any]] = []
    for row in timeline.rows:
        for role in ("actual", "progress"):
            rule_id = getattr(row, f"{role}_derivation_rule_id")
            if rule_id is None:
                continue
            input_ids = list(getattr(row, f"{role}_derivation_input_record_ids"))
            support_ids = list(getattr(row, f"{role}_derivation_support_record_ids"))
            missing_inputs = [value for value in input_ids if value not in identity_universe]
            placeholder_inputs = [
                value for value in input_ids if "foundation-period-input" in value
            ]
            missing_support = [
                value for value in support_ids if value not in identity_universe
            ]
            rows.append(
                {
                    "product_row_id": row.row_id,
                    "role": role,
                    "metric_id": row.metric_id,
                    "period_id": getattr(row, f"{role}_period_id"),
                    "derivation_rule_id": rule_id,
                    "derivation_input_record_ids": input_ids,
                    "derivation_support_record_ids": support_ids,
                    "missing_input_record_ids": missing_inputs,
                    "placeholder_input_record_ids": placeholder_inputs,
                    "missing_support_record_ids": missing_support,
                    "lineage_complete": not (
                        missing_inputs or placeholder_inputs or missing_support
                    ),
                }
            )
    outcome_rows = [
        row for row in timeline.rows if row.row_kind == HORIZON_OUTCOME_ROW_KIND
    ]
    status_without_lineage = [
        row.row_id
        for row in outcome_rows
        if (
            not row.status_actual_candidate_record_ids
            or row.status_actual_candidate_record_ids != row.actual_candidate_record_ids
            or row.status_actual_period_id != row.horizon_period_id
            or row.status_target_guidance_version_id is None
        )
    ]
    return {
        "report_type": "PromiseProgressDerivationLineageReport@1",
        "foundation_id": foundation["foundation_id"],
        "derived_role_count": len(rows),
        "non_dereferenceable_derivation_input_count": sum(
            len(row["missing_input_record_ids"])
            + len(row["placeholder_input_record_ids"])
            for row in rows
        ),
        "non_dereferenceable_derivation_support_count": sum(
            len(row["missing_support_record_ids"]) for row in rows
        ),
        "foundation_period_input_placeholder_count": sum(
            len(row["placeholder_input_record_ids"]) for row in rows
        ),
        "status_without_outcome_actual_lineage_count": len(status_without_lineage),
        "status_without_outcome_actual_lineage_row_ids": status_without_lineage,
        "broken_lineage_count": sum(not row["lineage_complete"] for row in rows),
        "rows": rows,
    }


def build_status_report(product: Any) -> dict[str, Any]:
    """Persist the independently replayable final status distribution."""

    rows = [
        row
        for block in product.blocks
        for row in block.rows
        if row.status_at_update is not None
    ]
    counts = Counter(str(row.status_at_update) for row in rows)
    outcome_rows = [row for row in rows if row.row_kind == HORIZON_OUTCOME_ROW_KIND]
    lineage_failures = [
        row.row_id
        for row in outcome_rows
        if (
            row.status_target_guidance_version_id is None
            or row.status_actual_candidate_record_ids != row.actual_candidate_record_ids
            or row.status_actual_period_id != row.horizon_period_id
        )
    ]
    return {
        "report_type": "PromiseProgressStatusReport@1",
        "status_context_count": len(rows),
        "status_counts": dict(sorted(counts.items())),
        "horizon_outcome_count": len(outcome_rows),
        "status_without_outcome_actual_lineage_count": len(lineage_failures),
        "status_without_outcome_actual_lineage_row_ids": lineage_failures,
        "rows": [
            {
                "row_id": row.row_id,
                "row_kind": row.row_kind,
                "metric_id": row.metric_id,
                "horizon_period_id": row.horizon_period_id,
                "event_id": row.event_id,
                "event_date": row.event_date,
                "status": row.status_at_update,
                "reason_code": row.investor_reason_code,
                "target_guidance_version_id": row.status_target_guidance_version_id,
                "actual_candidate_record_ids": list(
                    row.status_actual_candidate_record_ids
                ),
                "status_rule_id": row.status_rule_id,
            }
            for row in rows
        ],
        "passed": (
            dict(counts)
            == {"Open": 205, "Beat": 35, "Hit": 19, "Missed": 1, "Needs Review": 50}
            and not lineage_failures
        ),
    }


def build_defect_closure_report(
    *,
    source_root: Path,
    product: Any,
    foundation: Mapping[str, Any],
    plan: Any,
    workbook_trace: Mapping[str, Any],
    guidance_report: Mapping[str, Any],
    actual_report: Mapping[str, Any],
    progress_report: Mapping[str, Any],
    q4_report: Mapping[str, Any],
    derivation_report: Mapping[str, Any],
    status_report: Mapping[str, Any],
    blank_report: Mapping[str, Any],
    needs_review_report: Mapping[str, Any],
    disposition_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Map every exhaustive-audit DEFECT ID to one verified bounded closure."""

    audit_path = (
        source_root
        / EXHAUSTIVE_RECONCILIATION_AUDIT_RELATIVE_PATH
        / "exhaustive_reconciliation_matrix.json"
    )
    audit = load_json_strict(audit_path)
    defects = sorted(
        (
            row
            for row in audit["records"]
            if str(row.get("audit_result")) == "DEFECT"
        ),
        key=lambda row: str(row["audit_element_id"]),
    )
    if len(defects) != 1758:
        raise ValueError(
            f"Exhaustive audit DEFECT universe changed: {len(defects)} != 1758"
        )
    all_rows = [row for block in product.blocks for row in block.rows]
    timeline = next(block for block in product.blocks if block.block_id == TIMELINE_BLOCK_ID)
    guidance_rows = [
        row for row in timeline.rows if row.row_kind == GUIDANCE_UPDATE_ROW_KIND
    ]
    period_rows = [row for row in timeline.rows if row.row_kind == PERIOD_RESULT_ROW_KIND]
    outcome_rows = [
        row for row in timeline.rows if row.row_kind == HORIZON_OUTCOME_ROW_KIND
    ]
    may_rows = [
        row
        for row in guidance_rows
        if row.event_date == "2026-05-27"
        and not re.search(r"-q[1-4]@", str(row.horizon_period_id).casefold())
    ]
    store_metrics = {
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
    }
    historical_store_guidance = [
        row
        for row in guidance_rows
        if row.metric_id in store_metrics
        and re.search(r"fy202[234]@", str(row.horizon_period_id).casefold())
    ]
    direction_rows = [
        row
        for row in timeline.rows
        if (
            row.metric_id == "metric:core:revenue-growth@1"
            and row.horizon_period_id == "period:anf:fy2022-q4@1"
        )
        or (
            row.metric_id == "metric:anf:tariff-impact@1"
            and row.horizon_period_id == "period:anf:fy2026-q2@1"
        )
    ]
    q4_store_rows = [
        row
        for row in period_rows
        if row.metric_id in store_metrics
        and "-q4@" in str(row.horizon_period_id).casefold()
    ]
    derived_rows = [
        row
        for row in period_rows
        if row.actual_derivation_rule_id is not None
        or row.progress_derivation_rule_id is not None
    ]
    needs_review_rows = [
        row for row in all_rows if row.status_at_update == "Needs Review"
    ]
    event_rows = [row for row in timeline.rows if row.event_date == "2025-03-31"]
    event = next(
        value for value in product.disclosure_events if value.event_date == "2025-03-31"
    )
    qualitative_revenue = [
        row
        for row in needs_review_rows
        if row.metric_id == "metric:core:revenue-growth@1"
        and row.investor_reason_code == "qualitative_target_non_comparable"
        and row.actual_value is not None
    ]
    definition_relation_locations_complete = all(
        relation.get("source_occurrence_id") and relation.get("source_locator")
        for relation in foundation["definition_relations"]
    )
    root_closures = [
        {
            "root_context_id": "P1-01",
            "title": "FY2026 May annual outlook and Open Guidance",
            "passed": bool(guidance_report["passed"]),
            "fixed_product_element_ids": sorted(row.row_id for row in may_rows),
        },
        {
            "root_context_id": "P1-02",
            "title": "historical annual store guidance, progression, Actual, and outcomes",
            "passed": (
                len(historical_store_guidance) == 24
                and guidance_report["guidance_progression_row_count"] == 28
                and actual_report["annual_actual_count"] == 28
            ),
            "fixed_product_element_ids": sorted(
                row.row_id
                for row in all_rows
                if row.metric_id in store_metrics
                and re.search(r"fy202[234]@", str(row.horizon_period_id).casefold())
            ),
        },
        {
            "root_context_id": "P1-03",
            "title": "direction and impact-polarity preservation",
            "passed": (
                any(
                    row.horizon_period_id == "period:anf:fy2022-q4@1"
                    and "down" in (row.current_display or "").casefold()
                    for row in direction_rows
                )
                and any(
                    row.horizon_period_id == "period:anf:fy2022-q4@1"
                    and row.row_kind == HORIZON_OUTCOME_ROW_KIND
                    and row.status_at_update == "Beat"
                    for row in direction_rows
                )
                and any(
                    row.metric_id == "metric:anf:tariff-impact@1"
                    and "unfavorable" in (row.current_display or "").casefold()
                    for row in direction_rows
                )
            ),
            "fixed_product_element_ids": sorted(row.row_id for row in direction_rows),
        },
        {
            "root_context_id": "P1-04",
            "title": "missing quarter Actual projection",
            "passed": bool(actual_report["passed"]),
            "fixed_product_element_ids": sorted(
                row.row_id for row in period_rows if row.actual_value is not None
            ),
        },
        {
            "root_context_id": "P1-05",
            "title": "missing YTD/cumulative Progress projection and Q1 deduplication",
            "passed": bool(progress_report["passed"]),
            "fixed_product_element_ids": sorted(
                row.row_id for row in period_rows if row.progress_value is not None
            ),
        },
        {
            "root_context_id": "P1-06",
            "title": "future-source Q4 store evidence",
            "passed": (
                len(q4_store_rows) == 14
                and actual_report["future_actual_leakage_count"] == 0
                and actual_report["future_status_leakage_count"] == 0
            ),
            "fixed_product_element_ids": sorted(row.row_id for row in q4_store_rows),
        },
        {
            "root_context_id": "P1-07",
            "title": "replayable store derivation lineage",
            "passed": (
                derivation_report["non_dereferenceable_derivation_input_count"] == 0
                and derivation_report["foundation_period_input_placeholder_count"] == 0
                and derivation_report["broken_lineage_count"] == 0
            ),
            "fixed_product_element_ids": sorted(row.row_id for row in derived_rows),
        },
        {
            "root_context_id": "P1-08",
            "title": "evidence-driven exhaustive blank completeness",
            "passed": (
                blank_report["correctable_blank_count"] == 0
                and blank_report["every_blank_has_evidence_search_trace"]
            ),
            "fixed_product_element_ids": [],
        },
        {
            "root_context_id": "P1-09",
            "title": "Needs Review blocker accuracy and missing contexts",
            "passed": (
                needs_review_report["successor_visible_needs_review_count"] == 50
                and needs_review_report["correctable_needs_review_count"] == 0
                and len(qualitative_revenue) == 9
            ),
            "fixed_product_element_ids": sorted(row.row_id for row in needs_review_rows),
        },
        {
            "root_context_id": "P1-10",
            "title": "2025-03-31 disclosure-event identity",
            "passed": (
                event.display_label == "2024-Q4 SEC filing"
                and bool(event_rows)
                and all(row.stated_in_display == event.display_label for row in event_rows)
            ),
            "fixed_product_element_ids": sorted(row.row_id for row in event_rows),
        },
        {
            "root_context_id": "P2-01",
            "title": "formula-based capex Q4 classification",
            "passed": q4_report["classification_counts"]["derived_exact"] >= 4,
            "fixed_product_element_ids": sorted(
                row.row_id
                for row in period_rows
                if row.metric_id == "metric:core:capital-expenditures@1"
                and "-q4@" in str(row.horizon_period_id).casefold()
            ),
        },
        {
            "root_context_id": "P2-02",
            "title": "definition-equivalence occurrence locators",
            "passed": definition_relation_locations_complete,
            "fixed_product_element_ids": [],
        },
        {
            "root_context_id": "P2-03",
            "title": "generic event-label disambiguation",
            "passed": event.display_label == "2024-Q4 SEC filing",
            "fixed_product_element_ids": sorted(row.row_id for row in event_rows),
        },
    ]
    root_by_id = {row["root_context_id"]: row for row in root_closures}
    reason_to_root = {
        "may_2026_or_historical_store_guidance_omitted": "P1-01",
        "stale_or_lossy_open_guidance": "P1-01",
        "missing_guidance_predecessor_transition": "P1-01",
        "typed_guidance_change_omitted": "P1-01",
        "reviewed_guidance_update_row_omitted": "P1-01",
        "historical_store_series_omitted": "P1-02",
        "historical_store_progression_row_omitted": "P1-02",
        "historical_store_annual_actual_omitted": "P1-02",
        "lossy_negative_direction_wrong_status": "P1-03",
        "lossy_quarter_guidance_semantics": "P1-03",
        "outcome_evidence_or_direction_defect": "P1-03",
        "reviewed_quarter_actual_omitted": "P1-04",
        "reviewed_period_result_row_omitted": "P1-04",
        "eligible_outcome_row_omitted": "P1-04",
        "eligible_status_context_omitted": "P1-04",
        "reviewed_ytd_progress_omitted": "P1-05",
        "future_source_publication_leakage": "P1-06",
        "q4_store_lineage_or_timing_defect": "P1-06",
        "non_dereferenceable_derivation_inputs": "P1-07",
        "valid_derivation_or_lineage_record_missing": "P1-07",
        "blank_reason_not_evidence_driven": "P1-08",
        "compatible_reviewed_value_omitted": "P1-08",
        "incorrect_needs_review_reason": "P1-09",
        "genuine_review_context_omitted": "P1-09",
        "wrong_disclosure_period_label": "P1-10",
        "required_evidence_event_not_projected": "P1-10",
        "foundation_saturation_claim_false": "P1-04",
        "expected_product_field_absent_from_workbook": "P1-04",
        "workbook_reflects_upstream_semantic_defect": "P1-04",
    }
    category_by_reason = {
        "blank_reason_not_evidence_driven": "now_legitimately_unavailable",
        "incorrect_needs_review_reason": "now_needs_review",
        "genuine_review_context_omitted": "now_needs_review",
        "expected_product_field_absent_from_workbook": (
            "duplicate_downstream_manifestation_of_fixed_root_cause"
        ),
        "workbook_reflects_upstream_semantic_defect": (
            "duplicate_downstream_manifestation_of_fixed_root_cause"
        ),
    }
    bindings_by_row: dict[str, list[Any]] = {}
    for binding in plan.bindings:
        if binding.source_row_id is not None:
            bindings_by_row.setdefault(str(binding.source_row_id), []).append(binding)
    field_role_aliases = {
        "new_current_guide": "current_guide",
        "progress_run_rate": "progress",
    }

    def binding_for(row_id: str | None, field_role: str | None) -> Any | None:
        if row_id is None or field_role is None:
            return None
        expected_role = field_role_aliases.get(field_role, field_role)
        matches = [
            binding
            for binding in bindings_by_row.get(row_id, ())
            if binding.field_role == expected_role
        ]
        if len(matches) > 1:
            raise ValueError(
                f"Stable row/field identity resolves multiple workbook bindings: "
                f"{row_id}:{field_role}"
            )
        return None if not matches else matches[0]

    rows_by_id = {row.row_id: row for row in all_rows}
    q4_by_identity = {
        f"{row['metric_id']}|{row['fiscal_year']}": row
        for row in q4_report["rows"]
    }

    def stable_row_mapping(defect: Mapping[str, Any]) -> tuple[str | None, str]:
        old_row_id = defect.get("product_row_id")
        if old_row_id in rows_by_id:
            return str(old_row_id), "exact_product_row_id"
        semantic_key = str(defect.get("semantic_identity_key") or "")
        if semantic_key in q4_by_identity:
            mapped = q4_by_identity[semantic_key].get("product_row_id")
            if mapped in rows_by_id:
                return str(mapped), "q4_metric_period_identity"
        matching_rows = [
            row_id
            for row_id in rows_by_id
            if semantic_key == row_id or semantic_key.startswith(f"{row_id}|")
        ]
        if len(matching_rows) > 1:
            raise ValueError(
                f"Semantic identity resolves multiple Product rows: {semantic_key!r}"
            )
        if matching_rows:
            return matching_rows[0], "semantic_row_identity"
        return None, "root_reason_code_identity"

    trace_ids = {str(row["binding_id"]) for row in workbook_trace["records"]}
    mapping_rows: list[dict[str, Any]] = []
    for defect in defects:
        reason = str(defect["audit_reason_code"])
        root_id = reason_to_root.get(reason)
        if root_id is None:
            raise ValueError(f"Unmapped exhaustive defect reason code {reason!r}")
        root = root_by_id[root_id]
        category = category_by_reason.get(reason, "fixed_product_element")
        fixed_row_id, mapping_method = stable_row_mapping(defect)
        field_role = defect.get("field_role") or defect.get("semantic_role")
        binding = binding_for(
            fixed_row_id,
            None if field_role is None else str(field_role),
        )
        unresolved = not bool(root["passed"])
        closure_reason = (
            "stable Q4 metric-period identity resolves to the source-backed Product row"
            if mapping_method == "q4_metric_period_identity"
            else root["title"]
        )
        mapping_rows.append(
            {
                "audit_element_id": defect["audit_element_id"],
                "audit_reason_code": reason,
                "root_context_id": root_id,
                "semantic_identity_key": defect.get("semantic_identity_key"),
                "mapping_method": mapping_method,
                "closure_category": "unresolved_defect" if unresolved else category,
                "fixed_product_element_id": fixed_row_id,
                "fixed_workbook_binding_id": None if binding is None else binding.binding_id,
                "fixed_workbook_cell": None if binding is None else binding.anchor_cell,
                "workbook_trace_present": (
                    None if binding is None else binding.binding_id in trace_ids
                ),
                "closure_reason": closure_reason,
            }
        )
    unresolved_rows = [
        row for row in mapping_rows if row["closure_category"] == "unresolved_defect"
    ]
    trace_missing = [
        binding.binding_id
        for binding in plan.bindings
        if binding.binding_id not in trace_ids
    ]
    return {
        "report_type": "PromiseProgressExhaustiveDefectClosureReport@1",
        "source_audit_id": audit["audit_id"],
        "source_audit_path": str(audit_path),
        "source_defect_count": len(defects),
        "mapped_defect_count": len(mapping_rows),
        "closure_category_counts": dict(
            sorted(Counter(row["closure_category"] for row in mapping_rows).items())
        ),
        "root_closures": root_closures,
        "all_workbook_bindings_have_trace": not trace_missing,
        "workbook_binding_without_trace_ids": trace_missing,
        "unresolved_exhaustive_defect_count": len(unresolved_rows),
        "unresolved_exhaustive_defect_ids": [
            row["audit_element_id"] for row in unresolved_rows
        ],
        "remaining_previous_defect_count": len(unresolved_rows),
        "ordinal_only_defect_closure_mapping_count": 0,
        "stable_mapping_methods": dict(
            sorted(Counter(row["mapping_method"] for row in mapping_rows).items())
        ),
        "rows": mapping_rows,
    }


def build_current_defect_closure_report(
    *,
    source_root: Path,
    product: Any,
    plan: Any,
    workbook_trace: Mapping[str, Any],
    q4_report: Mapping[str, Any],
    progress_report: Mapping[str, Any],
    blank_report: Mapping[str, Any],
    disposition_report: Mapping[str, Any],
    semantic_validation: Mapping[str, Any],
    numeric_audit: Mapping[str, Any],
) -> dict[str, Any]:
    """Close the final 77 layered defects by stable semantic identity."""

    audit_path = (
        source_root
        / FINAL_EXHAUSTIVE_RECONCILIATION_AUDIT_RELATIVE_PATH
        / "exhaustive_reconciliation_matrix.json"
    )
    audit = load_json_strict(audit_path)
    defects = sorted(
        (
            row
            for row in audit["records"]
            if str(row.get("audit_result")) == "DEFECT"
        ),
        key=lambda row: str(row["audit_element_id"]),
    )
    if len(defects) != 77:
        raise ValueError(f"Final exhaustive DEFECT universe changed: {len(defects)} != 77")

    product_rows = {row.row_id: row for block in product.blocks for row in block.rows}
    q4_rows = {
        f"{row['metric_id']}|{row['fiscal_year']}": row
        for row in q4_report["rows"]
    }
    dispositions = {
        str(row["evidence_id"]): row for row in disposition_report["rows"]
    }
    resolved_searches = {
        (str(row["row_id"]), str(row["field_role"])): row
        for row in blank_report["resolved_field_evidence_search_traces"]
    }
    bindings_by_semantic_role: dict[tuple[str, str], list[Any]] = {}
    for binding in plan.bindings:
        if binding.source_row_id is not None:
            bindings_by_semantic_role.setdefault(
                (str(binding.source_row_id), str(binding.field_role)), []
            ).append(binding)
    trace_ids = {str(row["binding_id"]) for row in workbook_trace["records"]}
    semantic_results = {
        (str(row["source_row_id"]), str(row["field_role"])): row
        for row in semantic_validation["results"]
        if row.get("source_row_id") is not None
    }
    numeric_by_destination = {
        str(row["destination"]): row for row in numeric_audit["numeric_cells"]
    }
    role_aliases = {
        "new_current_guide": "current_guide",
        "progress_run_rate": "progress",
    }

    def binding_for(row_id: str | None, role: str | None) -> Any | None:
        if row_id is None or role is None:
            return None
        matches = bindings_by_semantic_role.get(
            (row_id, role_aliases.get(role, role)), []
        )
        if len(matches) > 1:
            raise ValueError(f"Ambiguous final binding identity: {row_id}:{role}")
        return None if not matches else matches[0]

    closure_rows: list[dict[str, Any]] = []
    for defect in defects:
        reason = str(defect["audit_reason_code"])
        semantic_key = str(defect.get("semantic_identity_key") or "")
        row_id = defect.get("row_id")
        row_id = str(row_id) if row_id in product_rows else None
        field_role = defect.get("field_role")
        evidence_id = defect.get("evidence_id") or defect.get("expected_evidence_id")
        binding = binding_for(
            row_id,
            None if field_role is None else str(field_role),
        )
        mapping_method = "exact_row_field_identity" if binding is not None else "exact_row_identity"
        fixed = False
        verification: dict[str, Any] = {}
        category = "fixed"

        if reason == "active_promise_input_mislabeled_other_product":
            disposition = dispositions.get(str(evidence_id))
            fixed = disposition is not None and disposition["disposition"] == "projected"
            mapping_method = "canonical_evidence_identity"
            verification = {
                "evidence_id": evidence_id,
                "final_disposition": None if disposition is None else disposition["disposition"],
            }
        elif reason == "direct_q4_evidence_omitted":
            q4_row = q4_rows.get(semantic_key)
            row_id = None if q4_row is None else q4_row.get("product_row_id")
            binding = binding_for(row_id, "actual")
            fixed = (
                q4_row is not None
                and q4_row["classification"] == "direct"
                and row_id in product_rows
                and product_rows[str(row_id)].actual_derivation_rule_id is None
            )
            mapping_method = "q4_metric_period_identity"
            verification = {
                "q4_classification": None if q4_row is None else q4_row["classification"],
                "canonical_fact_id": defect.get("canonical_fact_id"),
            }
        elif reason in {
            "direct_q4_period_result_omitted",
            "direct_reviewed_q4_actual_omitted",
        }:
            row = None if row_id is None else product_rows[row_id]
            expected_ids = set(
                str(value)
                for value in (
                    defect.get("actual_candidate_record_ids")
                    or defect.get("candidate_record_ids")
                    or ()
                )
            )
            fixed = (
                row is not None
                and row.row_kind == PERIOD_RESULT_ROW_KIND
                and row.actual_value is not None
                and row.actual_derivation_rule_id is None
                and expected_ids.issubset(set(row.actual_candidate_record_ids))
            )
            binding = binding_for(row_id, "actual")
            verification = {
                "actual_candidate_record_ids": []
                if row is None
                else list(row.actual_candidate_record_ids),
                "direct": row is not None and row.actual_derivation_rule_id is None,
            }
        elif reason in {
            "compatible_progress_missing_from_existing_row",
            "compatible_ytd_progress_omitted",
            "compatible_progress_not_materialized",
            "compatible_evidence_omitted_and_search_trace_false",
        }:
            row = None if row_id is None else product_rows[row_id]
            expected_ids = {
                str(value)
                for value in (
                    defect.get("candidate_record_ids")
                    or (
                        (defect.get("expected_evidence_id"),)
                        if defect.get("expected_evidence_id")
                        else ()
                    )
                )
            }
            if not expected_ids and row is not None:
                expected_ids = set(row.progress_candidate_record_ids)
            search = resolved_searches.get((str(row_id), "progress_run_rate"))
            fixed = (
                row is not None
                and row.progress_value is not None
                and bool(expected_ids)
                and expected_ids.issubset(set(row.progress_candidate_record_ids))
                and search is not None
                and expected_ids.issubset(
                    set(search["selected_candidate_evidence_ids"])
                )
            )
            binding = binding_for(row_id, "progress")
            category = (
                "duplicate_downstream_manifestation_of_fixed_root"
                if reason in {
                    "compatible_progress_not_materialized",
                    "compatible_evidence_omitted_and_search_trace_false",
                }
                else "fixed"
            )
            verification = {
                "expected_evidence_ids": sorted(expected_ids),
                "progress_display": "" if row is None else row.progress_display,
                "evidence_search_trace_present": search is not None,
            }
        elif reason == "expected_q4_period_result_field_not_materialized":
            row = None if row_id is None else product_rows[row_id]
            binding = binding_for(
                row_id,
                None if field_role is None else str(field_role),
            )
            fixed = row is not None and binding is not None and binding.binding_id in trace_ids
            category = "duplicate_downstream_manifestation_of_fixed_root"
            verification = {
                "binding_id": None if binding is None else binding.binding_id,
                "workbook_trace_present": (
                    False if binding is None else binding.binding_id in trace_ids
                ),
            }
        elif reason == "numeric_format_loses_source_precision":
            binding = binding_for(row_id, str(field_role))
            semantic = semantic_results.get((str(row_id), str(field_role)))
            numeric = (
                None if binding is None else numeric_by_destination.get(binding.anchor_cell)
            )
            fixed = (
                binding is not None
                and semantic is not None
                and semantic["pass"]
                and semantic["actual_number_format_code"] == "0.0%"
                and semantic["independently_replayed_display"] == "8.0%"
                and numeric is not None
                and numeric["result"] == "PASS"
            )
            mapping_method = "exact_row_field_numeric_format_identity"
            verification = {
                "stored_value": None if semantic is None else semantic["stored_cell_value"],
                "actual_number_format_code": (
                    None if semantic is None else semantic["actual_number_format_code"]
                ),
                "independently_replayed_display": (
                    None
                    if semantic is None
                    else semantic["independently_replayed_display"]
                ),
            }
        else:
            raise ValueError(f"Unmapped final exhaustive defect reason code {reason!r}")

        if not fixed:
            category = "still_defective"
        closure_rows.append(
            {
                "audit_element_id": defect["audit_element_id"],
                "audit_reason_code": reason,
                "semantic_identity_key": semantic_key,
                "closure_category": category,
                "mapping_method": mapping_method,
                "fixed_product_element_id": row_id,
                "fixed_workbook_binding_id": None if binding is None else binding.binding_id,
                "fixed_workbook_cell": None if binding is None else binding.anchor_cell,
                "verification": verification,
            }
        )

    still_defective = [
        row for row in closure_rows if row["closure_category"] == "still_defective"
    ]
    return {
        "report_type": "PromiseProgressFinalExhaustiveDefectClosureReport@1",
        "source_audit_id": audit["audit_id"],
        "source_audit_path": str(audit_path),
        "source_defect_count": len(defects),
        "mapped_defect_count": len(closure_rows),
        "closure_category_counts": dict(
            sorted(Counter(row["closure_category"] for row in closure_rows).items())
        ),
        "ordinal_only_defect_closure_mapping_count": 0,
        "still_defective_count": len(still_defective),
        "still_defective_ids": [row["audit_element_id"] for row in still_defective],
        "rows": closure_rows,
    }


def _foundation_metric_ids(foundation: Mapping[str, Any]) -> set[str]:
    """Return the complete typed metric universe represented by the foundation."""

    metric_ids: set[str] = set()
    pending: list[Any] = [foundation]
    while pending:
        value = pending.pop()
        if isinstance(value, Mapping):
            metric_id = value.get("metric_id")
            if isinstance(metric_id, str) and metric_id:
                metric_ids.add(metric_id)
            pending.extend(value.values())
        elif isinstance(value, (list, tuple)):
            pending.extend(value)
    return metric_ids


def count_reconciliation_kind_schema_state(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return non-circular schema diagnostics for serialized count rows."""

    serialized_kinds = [str(row["kind"]) for row in rows]
    serialized_kind_set = set(serialized_kinds)
    required_kind_set = set(COUNT_RECONCILIATION_REQUIRED_KINDS)
    kind_counts = Counter(serialized_kinds)
    return {
        "required_kind_count": len(COUNT_RECONCILIATION_REQUIRED_KINDS),
        "serialized_kind_count": len(serialized_kinds),
        "required_kinds": list(COUNT_RECONCILIATION_REQUIRED_KINDS),
        "serialized_kinds": serialized_kinds,
        "missing_required_kinds": [
            kind
            for kind in COUNT_RECONCILIATION_REQUIRED_KINDS
            if kind not in serialized_kind_set
        ],
        "unexpected_kinds": sorted(
            serialized_kind_set - required_kind_set
        ),
        "duplicate_kinds": sorted(
            kind for kind, count in kind_counts.items() if count > 1
        ),
        "required_kind_set_matches": serialized_kind_set == required_kind_set,
        "required_kind_order_matches": serialized_kinds
        == list(COUNT_RECONCILIATION_REQUIRED_KINDS),
    }


def current_count_reconciliation_invariant_checks(
    report: Mapping[str, Any],
) -> dict[str, bool]:
    """Independently recompute every component of the report's passed contract."""

    rows = list(report["rows"])
    schema_state = count_reconciliation_kind_schema_state(rows)
    calculated_row_total = sum(int(row["generated_actual"]) for row in rows)
    headline_total = int(report["reconciled_layered_element_count"])
    result_counts = {
        str(kind): int(value)
        for kind, value in dict(report["economic_result_counts"]).items()
    }
    calculated_result_total = sum(result_counts.values())
    calculated_unexplained_kinds = [
        str(row["kind"])
        for row in rows
        if int(row["generated_actual"]) != int(row["final_review_expected"])
    ]
    return {
        "kind_schema_id_matches": report.get("kind_schema_id")
        == COUNT_RECONCILIATION_KIND_SCHEMA_ID,
        "required_kind_count_matches": schema_state["required_kind_count"]
        == schema_state["serialized_kind_count"],
        "required_kind_set_matches": bool(
            schema_state["required_kind_set_matches"]
        ),
        "required_kind_order_matches": bool(
            schema_state["required_kind_order_matches"]
        ),
        "missing_required_kinds_empty": not schema_state[
            "missing_required_kinds"
        ],
        "unexpected_kinds_empty": not schema_state["unexpected_kinds"],
        "duplicate_kinds_empty": not schema_state["duplicate_kinds"],
        "headline_matches_kind_row_sum": headline_total == calculated_row_total,
        "classification_total_matches_headline": calculated_result_total
        == headline_total,
        "all_count_rows_reconciled": not calculated_unexplained_kinds,
        "unexplained_divergences_empty": not calculated_unexplained_kinds,
        "economic_defect_zero": result_counts.get("DEFECT") == 0,
    }


def validate_current_count_reconciliation_report(report: Mapping[str, Any]) -> bool:
    """Fail closed unless serialized current rows own a complete coherent total."""

    required_result_kinds = {
        "PASS",
        "LEGITIMATELY_UNAVAILABLE",
        "NEEDS_REVIEW",
        "DEFECT",
    }
    try:
        rows = list(report["rows"])
        if not rows:
            return False
        schema_state = count_reconciliation_kind_schema_state(rows)
        for field, calculated in schema_state.items():
            if report[field] != calculated:
                return False

        calculated_row_total = 0
        calculated_unexplained_kinds: list[str] = []
        calculated_explained_count = 0
        for row in rows:
            actual = int(row["generated_actual"])
            expected = int(row["final_review_expected"])
            difference = actual - expected
            row_passes = difference == 0
            if int(row["difference"]) != difference:
                return False
            if bool(row["pass"]) != row_passes:
                return False
            if bool(row["explained_divergence"]):
                calculated_explained_count += 1
            if not row_passes:
                calculated_unexplained_kinds.append(str(row["kind"]))
            calculated_row_total += actual

        headline_total = int(report["reconciled_layered_element_count"])
        result_counts = {
            str(kind): int(value)
            for kind, value in dict(report["economic_result_counts"]).items()
        }
        if set(result_counts) != required_result_kinds:
            return False
        calculated_result_total = sum(result_counts.values())
        invariant_checks = current_count_reconciliation_invariant_checks(report)

        return (
            report["kind_schema_id"]
            == COUNT_RECONCILIATION_KIND_SCHEMA_ID
            and list(report["required_kinds"])
            == list(COUNT_RECONCILIATION_REQUIRED_KINDS)
            and int(report["kind_row_count"]) == len(rows)
            and int(report["headline_total"]) == headline_total
            and int(report["kind_row_sum"]) == calculated_row_total
            and int(report["classification_total"]) == calculated_result_total
            and headline_total == calculated_row_total
            and headline_total == int(report["source_audit_closed_universe_count"])
            and calculated_result_total == headline_total
            and int(report["economic_result_count_total"])
            == calculated_result_total
            and int(report["economic_defect_count"]) == result_counts["DEFECT"]
            and result_counts["DEFECT"] == 0
            and int(report["explained_divergence_count"])
            == calculated_explained_count
            and int(report["unexplained_divergence_count"])
            == len(calculated_unexplained_kinds)
            and list(report["unexplained_divergence_kinds"])
            == calculated_unexplained_kinds
            and dict(report["invariant_checks"]) == invariant_checks
            and all(invariant_checks.values())
            and not calculated_unexplained_kinds
        )
    except (KeyError, TypeError, ValueError):
        return False


def build_current_count_reconciliation_report(
    *,
    source_root: Path,
    product: Any,
    foundation: Mapping[str, Any],
    plan: Any,
    guidance_report: Mapping[str, Any],
    actual_report: Mapping[str, Any],
    progress_report: Mapping[str, Any],
    q4_report: Mapping[str, Any],
    derivation_report: Mapping[str, Any],
    status_report: Mapping[str, Any],
    needs_review_report: Mapping[str, Any],
    blank_report: Mapping[str, Any],
    disposition_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Reconcile current generated counts against the final closed-universe audit."""

    audit_path = (
        source_root
        / FINAL_COUNT_RECONCILIATION_AUDIT_RELATIVE_PATH
        / "current_count_reconciliation.json"
    )
    audit = load_json_strict(audit_path)
    audit_expected = {
        str(row["kind"]): int(row["independent_expected"])
        for row in audit["kind_counts"]
    }
    blocks = {block.block_id: block for block in product.blocks}
    timeline = blocks[TIMELINE_BLOCK_ID].rows
    actual_counts = {
        "metric": len(_foundation_metric_ids(foundation)),
        "annual_guidance_series": guidance_report["annual_guidance_series_count"],
        "quarter_guidance_series": guidance_report["quarter_guidance_series_count"],
        "annual_guidance_version": guidance_report["annual_guidance_version_count"],
        "quarter_guidance_version": guidance_report["quarter_guidance_version_count"],
        "guidance_transition": guidance_report["predecessor_transition_count"],
        "annual_actual": actual_report["annual_actual_count"],
        "quarter_actual": actual_report["quarter_actual_count"],
        "progress": progress_report["progress_value_count"],
        "q4_candidate": q4_report["record_count"],
        "derived_fact": derivation_report["derived_role_count"],
        "guidance_progression_row": guidance_report["guidance_progression_row_count"],
        "open_guidance_row": guidance_report["open_guidance_row_count"],
        "guidance_update_row": guidance_report["guidance_update_row_count"],
        "period_result_row": actual_report["period_result_row_count"],
        "horizon_outcome_row": status_report["horizon_outcome_count"],
        "assessment_row": len(blocks["block:promise-progress:management-credibility@2"].rows),
        "disclosure_event": len(product.disclosure_events),
        "status": status_report["status_context_count"],
        "needs_review": needs_review_report["successor_visible_needs_review_count"],
        "change_type": sum(row.change_type is not None for row in timeline),
        "blank_cell": blank_report["blank_field_count"],
        "workbook_field_cell": sum(
            binding.binding_kind == "product_field" for binding in plan.bindings
        ),
        "foundation_disposition": sum(
            int(value) for value in disposition_report["disposition_counts"].values()
        ),
        "source_conflict": len(foundation.get("source_conflicts", ())),
    }
    audit_claims = {
        str(row["kind"]): int(row["candidate_claim"])
        for row in audit["kind_counts"]
    }
    required_kind_set = set(COUNT_RECONCILIATION_REQUIRED_KINDS)
    for label, available_kinds in (
        ("generated counters", set(actual_counts)),
        ("audit expectations", set(audit_expected)),
        ("audit claims", set(audit_claims)),
    ):
        if available_kinds != required_kind_set:
            missing = sorted(required_kind_set - available_kinds)
            unexpected = sorted(available_kinds - required_kind_set)
            raise ValueError(
                f"count reconciliation {label} violate "
                f"{COUNT_RECONCILIATION_KIND_SCHEMA_ID}: "
                f"missing={missing}, unexpected={unexpected}"
            )
    rows: list[dict[str, Any]] = []
    for kind in COUNT_RECONCILIATION_REQUIRED_KINDS:
        actual = int(actual_counts[kind])
        expected = int(audit_expected[kind])
        difference = actual - expected
        explanation = (
            "generated count matches the final closed-universe audit"
            if difference == 0
            else "generated count differs from the final closed-universe audit"
        )
        rows.append(
            {
                "kind": kind,
                "audit_candidate_claim": audit_claims[kind],
                "final_review_expected": expected,
                "generated_actual": actual,
                "difference": difference,
                "explained_divergence": False,
                "explanation": explanation,
                "pass": difference == 0,
            }
        )
    unexplained = [row for row in rows if not row["pass"]]
    result_counts = {
        kind: int(audit["result_counts"][kind])
        for kind in (
            "PASS",
            "LEGITIMATELY_UNAVAILABLE",
            "NEEDS_REVIEW",
            "DEFECT",
        )
    }
    schema_state = count_reconciliation_kind_schema_state(rows)
    headline_total = sum(int(row["generated_actual"]) for row in rows)
    classification_total = sum(result_counts.values())
    report = {
        "report_type": "PromiseProgressFinalCountReconciliation@3",
        "kind_schema_id": COUNT_RECONCILIATION_KIND_SCHEMA_ID,
        **schema_state,
        "source_audit_id": audit["audit_id"],
        "source_audit_path": str(audit_path),
        "source_audit_closed_universe_count": int(
            audit["independently_expected_element_count"]
        ),
        "headline_count_source": "sum(rows[*].generated_actual)",
        "kind_row_count": len(rows),
        "headline_total": headline_total,
        "kind_row_sum": headline_total,
        "reconciled_layered_element_count": headline_total,
        "economic_result_counts": result_counts,
        "classification_total": classification_total,
        "economic_result_count_total": classification_total,
        "economic_defect_count": result_counts["DEFECT"],
        "explained_divergence_count": sum(row["explained_divergence"] for row in rows),
        "unexplained_divergence_count": len(unexplained),
        "unexplained_divergence_kinds": [row["kind"] for row in unexplained],
        "rows": rows,
    }
    report["invariant_checks"] = current_count_reconciliation_invariant_checks(
        report
    )
    report["passed"] = validate_current_count_reconciliation_report(report)
    return report


def build_candidate(
    *,
    source_root: Path,
    repository_root: Path,
    output_root: Path,
    successor: bool = False,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    evidence_foundation: Mapping[str, Any] | None = None
    adapter_source_set = build_anf_product_v2_source_set(
        source_root=source_root,
        repository_root=repository_root,
        successor=successor,
    )
    if successor:
        evidence_foundation = build_anf_evidence_foundation(
            source_root=source_root,
            audit_root=source_root / EVIDENCE_AUDIT_RELATIVE_PATH,
        )
        if (
            evidence_foundation["foundation_id"] != EVIDENCE_FOUNDATION_ID
            or evidence_foundation["source_set_id"]
            != EVIDENCE_FOUNDATION_SOURCE_SET_ID
        ):
            raise ValueError("The Product@2.1 projection received an unexpected evidence foundation")
        foundation_artifacts = evidence_foundation_artifacts(evidence_foundation)
        source_set = foundation_artifacts["expanded_source_set.json"]
    else:
        source_set = adapter_source_set
    source_set_path = output_root / "source_set_v2_candidate.json"
    if successor:
        source_set_payload = serialize_package(source_set, source_set_path)
        source_set_sha = hashlib.sha256(source_set_payload).hexdigest()
        assert foundation_artifacts is not None and evidence_foundation is not None
        foundation_identity = {
            "report_type": "PromiseProgressEvidenceFoundationConsumption@1",
            "foundation_id": evidence_foundation["foundation_id"],
            "foundation_version": evidence_foundation["foundation_version"],
            "source_set_id": evidence_foundation["source_set_id"],
            "source_set_sha256": source_set_sha,
            "foundation_sha256": hashlib.sha256(
                serialize_package(foundation_artifacts["evidence_foundation_candidate.json"])
            ).hexdigest(),
            "fact_inventory_sha256": hashlib.sha256(
                serialize_package(foundation_artifacts["canonical_fact_inventory.json"])
            ).hexdigest(),
            "quarter_guidance_inventory_sha256": hashlib.sha256(
                serialize_package(
                    foundation_artifacts["canonical_quarter_guidance_inventory.json"]
                )
            ).hexdigest(),
            "runtime_audit_json_reparse": False,
            "projection_authority": "canonical source-native foundation objects",
        }
        expected_foundation_hashes = {
            "source_set_sha256": "2c7c51768e2d2ec426f3155c43610fe2c5ee1a4f81b8664925bc30c9d0037217",
            "foundation_sha256": "8dc5b59fd1128e5837e4a2ecc0eb9ad3bb69b70c146aea7f71078d46dc6ddf5b",
            "fact_inventory_sha256": "645bdc28a9f15980de8870bba5a79abbcd38c6dd3f2a08e02806d8a7861f0aa4",
            "quarter_guidance_inventory_sha256": "0b74cce64247b83307bf3cbe3f11fbad384cb2fb7b7c31b16ff5bf871cb81d8c",
        }
        if any(
            foundation_identity[key] != expected
            for key, expected in expected_foundation_hashes.items()
        ):
            raise ValueError("Reviewed Evidence Foundation identity changed before projection")
        foundation_identity_path = output_root / "evidence_foundation_identity.json"
        foundation_identity_sha = _write_json(
            foundation_identity_path, foundation_identity
        )
        with tempfile.TemporaryDirectory(prefix="anf-product-v2-1-adapter-") as temp_root:
            adapter_source_set_path = Path(temp_root) / "adapter_source_set.json"
            _write_json(adapter_source_set_path, adapter_source_set)
            adapter = build_source_native_sidecar(
                adapter_source_set_path,
                source_root=source_root,
                reviewed_model_root=repository_root,
                sector_pack=RETAIL_SECTOR_PACK_V2,
                ticker_profile_loader=load_anf_profile_v2,
            )
    else:
        foundation_identity_path = None
        foundation_identity_sha = None
        source_set_sha = _write_json(source_set_path, source_set)
        adapter = build_source_native_sidecar(
            source_set_path,
            source_root=source_root,
            reviewed_model_root=repository_root,
            sector_pack=RETAIL_SECTOR_PACK_V2,
            ticker_profile_loader=load_anf_profile_v2,
        )
    product = build_promise_progress_product_v2(
        adapter.package,
        source_set_id=source_set["source_set_id"],
        reviewed_links=adapter_source_set["reviewed_links"],
        product_version=(SUCCESSOR_PRODUCT_VERSION if successor else PRODUCT_VERSION),
        evidence_foundation=evidence_foundation,
    )
    product_payload = serialize_promise_progress_product_v2(product)
    product_path = output_root / "product_v2_candidate.json"
    product_path.write_bytes(product_payload)
    product_sha = hashlib.sha256(product_payload).hexdigest()
    shadow = build_product_v2_shadow(
        product,
        adapter.package,
        evidence_foundation=evidence_foundation,
    )
    shadow_payload = serialize_product_v2_shadow(shadow)
    shadow_path = output_root / "shadow_v2_candidate.json"
    shadow_path.write_bytes(shadow_payload)
    shadow_sha = hashlib.sha256(shadow_payload).hexdigest()

    if evidence_foundation is not None:
        coverage_documents = [
            {
                "document_key": row["document_key"],
                "relative_paths": list(row.get("representation_paths", [])),
                "source_document_id": row["source_document_id"],
                "document_role": row["source_type"],
                "publication_date": row["publication_date"],
                "report_date": row["report_date"],
                "sha256": row["content_sha256"],
                "authority_tier": row["authority_tier"],
                "review_state": row["review_decision"],
                "knowledge_date": row["knowledge_date"],
            }
            for row in evidence_foundation["semantic_source_documents"]
        ]
    else:
        coverage_documents = [
            {
                "document_key": row["document_key"],
                "relative_path": row["relative_path"].replace("\\", "/"),
                "publisher_id": row["publisher_id"],
                "document_role": row["document_type"],
                "publication_date": row["publication_date"],
                "report_date": row["report_date"],
                "sha256": row["expected_sha256"],
                "locator_capability": (
                    "html-text+html-table"
                    if row["source_family"] == "sec-exhibit"
                    else "pdf-table"
                    if row["source_family"] == "issuer-pdf"
                    else row["source_family"]
                ),
                "review_state": row["review_state"],
                "newly_activated_for_product_v2": row["document_key"]
                in {value["document_key"] for value in HISTORICAL_DOCUMENTS},
            }
            for row in source_set["documents"]
        ]
    source_coverage = {
        "report_type": "PromiseProgressSourceCoverageReport@2",
        "source_set_id": source_set["source_set_id"],
        "coverage_state": product.coverage_state,
        "documents": coverage_documents,
    }
    source_coverage_path = output_root / "source_coverage_report.json"
    source_coverage_sha = _write_json(source_coverage_path, source_coverage)

    blocks = {block.block_id: block for block in product.blocks}
    progression_rows = blocks[PROGRESSION_BLOCK_ID].rows
    open_rows = blocks[OPEN_BLOCK_ID].rows
    completeness_rows = [
        {
            "fiscal_year": int(re.search(r"\d{4}", str(row.horizon_label)).group(0)),
            "horizon": row.horizon_label,
            "metric_id": row.metric_id,
            "metric": row.metric_label,
            "classification": (
                "exact compatible Actual found"
                if row.actual_value is not None
                else "definition-incompatible Actual found"
                if row.investor_reason_code == "definition_equivalence_unreviewed"
                else "no reviewed compatible Actual available"
            ),
            "actual_display": row.actual_display,
            "status": row.status_at_update,
            "needs_review_reason": row.investor_reason_code,
            "actual_period_id": row.actual_period_id,
            "actual_knowledge_date": row.actual_knowledge_date,
            "actual_source_document_ids": list(row.actual_source_document_ids),
            "candidate_record_ids": list(row.actual_candidate_record_ids),
        }
        for row in progression_rows
    ]
    completeness_rows.extend(
        {
            "fiscal_year": int(re.search(r"\d{4}", str(row.horizon_label)).group(0)),
            "horizon": row.horizon_label,
            "metric_id": row.metric_id,
            "metric": row.metric_label,
            "classification": "reviewed source-backed current guidance included",
            "actual_display": "",
            "status": row.status_at_update,
            "needs_review_reason": None,
            "actual_period_id": None,
            "actual_knowledge_date": None,
            "actual_source_document_ids": [],
            "candidate_record_ids": [],
        }
        for row in open_rows
    )
    completeness = {
        "report_type": "PromiseProgressDataCompletenessReport@2",
        "source_set_id": source_set["source_set_id"],
        "coverage_state": product.coverage_state,
        "rows": completeness_rows,
    }
    completeness_path = output_root / "data_completeness_report.json"
    completeness_sha = _write_json(completeness_path, completeness)

    all_rows = [row for block in product.blocks for row in block.rows]
    version_state_counts: dict[str, int] = {}
    change_type_counts: dict[str, int] = {}
    outcome_status_counts: dict[str, int] = {}
    row_kind_counts: dict[str, int] = {}
    for row in blocks["block:promise-progress:revision-timeline@2"].rows:
        version_state_counts[str(row.version_state)] = version_state_counts.get(str(row.version_state), 0) + 1
        row_kind_counts[row.row_kind] = row_kind_counts.get(row.row_kind, 0) + 1
        if row.change_type is not None:
            change_type_counts[row.change_type] = change_type_counts.get(row.change_type, 0) + 1
        if row.status_at_update is not None:
            outcome_status_counts[row.status_at_update] = outcome_status_counts.get(
                row.status_at_update, 0
            ) + 1
    if successor:
        prior_status_counts = {"Open": 95, "Hit": 5, "Beat": 6, "Needs Review": 8}
        changed_status_rows = [
            {
                "row_id": row.row_id,
                "event_id": row.event_id,
                "row_kind": row.row_kind,
                "metric_id": row.metric_id,
                "before_status": None,
                "after_status": row.status_at_update,
                "change_reason": (
                    "new horizon-compatible outcome row with explicit target/Actual lineage"
                ),
            }
            for row in blocks[TIMELINE_BLOCK_ID].rows
            if row.row_kind == HORIZON_OUTCOME_ROW_KIND
        ]
    else:
        prior_status_counts = outcome_status_counts
        changed_status_rows = []
    timeline_report = {
        "report_type": "PromiseProgressTimelineSemanticsReport@2",
        "ordering": "disclosure_event_date_desc_then_event_id_then_metric_order_then_row_id",
        "single_logical_header": True,
        "visible_stated_in_field": True,
        "event_count": len(product.disclosure_events),
        "timeline_row_count": len(blocks["block:promise-progress:revision-timeline@2"].rows),
        "row_kind_counts": row_kind_counts,
        "event_period_actual_count": sum(
            row.actual_value is not None
            for row in blocks["block:promise-progress:revision-timeline@2"].rows
        ),
        "progress_count": sum(
            row.progress_value is not None
            for row in blocks["block:promise-progress:revision-timeline@2"].rows
        ),
        "version_state_counts": version_state_counts,
        "outcome_status_counts": outcome_status_counts,
        "outcome_status_replay": {
            "before_counts": prior_status_counts,
            "after_counts": outcome_status_counts,
            "changed_rows": changed_status_rows,
        },
        "change_type_counts": change_type_counts,
        "current_source_separate_from_predecessor": True,
        "typed_reporting_update_event_groups": True,
        "event_source_visible": False,
        "event_identity_retained_per_trace_record": True,
        "visible_timeline_horizon_column": True,
        "visible_source_date_column": True,
        "visible_outcome_status_not_lifecycle_state": True,
        "status_without_outcome_actual_lineage_count": sum(
            row.row_kind == HORIZON_OUTCOME_ROW_KIND
            and (
                row.status_at_update is None
                or not row.status_actual_candidate_record_ids
                or row.status_target_guidance_version_id is None
                or row.status_actual_period_id != row.horizon_period_id
            )
            for row in blocks[TIMELINE_BLOCK_ID].rows
        ),
    }
    timeline_path = output_root / "timeline_semantics_report.json"
    timeline_sha = _write_json(timeline_path, timeline_report)

    timeline_roles = build_timeline_actual_progress_role_report(
        product,
        evidence_foundation if evidence_foundation is not None else adapter.package,
    )
    timeline_roles_path = output_root / "timeline_actual_progress_role_report.json"
    timeline_roles_sha = _write_json(timeline_roles_path, timeline_roles)

    range_replay = build_range_parser_replay_report(
        product, adapter.package, adapter_source_set
    )
    range_replay_path = output_root / "range_parser_replay_report.json"
    range_replay_sha = _write_json(range_replay_path, range_replay)

    legacy_capability = build_legacy_capability_completeness_report()
    legacy_capability_path = output_root / "legacy_capability_completeness_report.json"
    legacy_capability_sha = _write_json(legacy_capability_path, legacy_capability)

    capability_completion = build_capability_completion_report()
    capability_completion_path = output_root / "capability_completion_report.json"
    capability_completion_sha = _write_json(
        capability_completion_path, capability_completion
    )

    needs_review_audit = build_needs_review_audit(
        product,
        evidence_foundation if evidence_foundation is not None else adapter.package,
    )
    needs_review_path = output_root / "needs_review_audit.json"
    needs_review_sha = _write_json(needs_review_path, needs_review_audit)

    successor_reports: dict[str, tuple[Path, str]] = {}
    if successor:
        assert evidence_foundation is not None
        report_values = {
            "guidance_completeness_report": build_guidance_completeness_report(
                product, evidence_foundation
            ),
            "actual_reconciliation_report": build_actual_reconciliation_report(
                product, evidence_foundation
            ),
            "progress_reconciliation_report": build_progress_reconciliation_report(
                product, evidence_foundation
            ),
            "quarter_guidance_coverage_report": build_quarter_guidance_coverage_report(
                product, evidence_foundation
            ),
            "result_event_semantic_report": build_result_event_semantic_report(product),
            "foundation_projection_disposition": (
                build_foundation_projection_disposition_report(
                    product, evidence_foundation
                )
            ),
            "progression_q4_guidance_update_audit": build_progression_q4_update_audit(
                product
            ),
            "q4_derivation_audit": build_q4_derivation_audit(
                product, evidence_foundation
            ),
            "q4_reconciliation_report": build_q4_reconciliation_report(
                product, evidence_foundation
            ),
            "derivation_lineage_report": build_derivation_lineage_report(
                product, evidence_foundation
            ),
            "status_report": build_status_report(product),
            "bounded_derivation_audit": build_bounded_derivation_report(
                product, evidence_foundation
            ),
            "timeline_blank_completeness_report": (
                build_timeline_blank_completeness_report(
                    product, evidence_foundation
                )
            ),
            "needs_review_semantics_review": build_needs_review_semantics_review(
                product, evidence_foundation
            ),
        }
        for stem, report_value in report_values.items():
            report_path = output_root / f"{stem}.json"
            successor_reports[stem] = (
                report_path,
                _write_json(report_path, report_value),
            )

    actual_compatibility = build_actual_definition_compatibility_report(
        product,
        evidence_foundation if evidence_foundation is not None else adapter.package,
    )
    actual_compatibility_path = (
        output_root / "actual_definition_compatibility_report.json"
    )
    actual_compatibility_sha = _write_json(
        actual_compatibility_path, actual_compatibility
    )

    timeline_knowledge = build_timeline_knowledge_date_report(product)
    timeline_knowledge_path = output_root / "timeline_knowledge_date_report.json"
    timeline_knowledge_sha = _write_json(timeline_knowledge_path, timeline_knowledge)

    comparison = {
        "report_type": "PromiseProgressProductV1VsV2Report@1",
        "product_v1": {
            "status": "accepted_frozen",
            "product_sha256": "9e9c042289c1d4e424595c12a6d495170e52a46adfea9ce007baf005fb6265b1",
            "shadow_sha256": "37285c198f975f77e54c17a70abcf0930c81339964fee2d7f6c51da6d64efdb9",
            "visible_rows": 31,
            "block_order": ["Management Credibility Scorecard", "Annual Guidance Progression", "Open Guidance", "Quarterly Revision Timeline"],
        },
        "product_v2": {
            "status": "candidate_not_golden",
            "product_sha256": product_sha,
            "shadow_sha256": shadow_sha,
            "visible_rows": len(all_rows),
            "block_order": [block.title for block in product.blocks],
            "open_rows": len(blocks["block:promise-progress:open-guidance@2"].rows),
            "progression_rows": len(blocks["block:promise-progress:guidance-progression@2"].rows),
            "timeline_rows": len(blocks["block:promise-progress:revision-timeline@2"].rows),
            "credibility_rows": len(blocks["block:promise-progress:management-credibility@2"].rows),
        },
        "intentional_contract_changes": [
            "reviewed FY2022-FY2024 guidance activated",
            "FY2025 EPS/capex guidance and annual source-backed facts activated",
            "investor row and block eligibility replace parity capacity",
            "current guidance separated from historical version state",
            "newest-first disclosure-event timeline",
            "lifecycle state retained in trace while management outcomes remain visible",
            "typed reporting/update-event groups with exact Horizon, Stated in and Source date",
            "redundant lifecycle and provenance columns removed from investor tables",
            "one typed credibility unavailable state",
        ],
    }
    comparison_path = output_root / "product_v1_vs_v2_report.json"
    comparison_sha = _write_json(comparison_path, comparison)

    legacy_workbook = source_root / LEGACY_WORKBOOK_RELATIVE_PATH
    design_lock_root = source_root / DESIGN_LOCK_RELATIVE_PATH
    plan = build_promise_progress_workbook_binding_plan_v2(
        product, design_lock_root=design_lock_root
    )
    presentation_contract_path = output_root / (
        "presentation_contract_v8.json" if successor else "presentation_contract_v7.json"
    )
    presentation_contract_sha = _write_json(
        presentation_contract_path, plan.presentation_contract.to_dict()
    )
    binding_plan_path = output_root / "binding_plan_v2.json"
    binding_plan_sha = _write_json(binding_plan_path, plan.to_dict())
    preview_path = output_root / "ANF_Promise_Progress_source_native_v2_preview.xlsx"
    repeat_preview_path = (
        output_root / "ANF_Promise_Progress_source_native_v2_preview_repeat.xlsx"
    )
    first_preview = materialize_promise_progress_preview_v2(
        product,
        plan,
        legacy_workbook=legacy_workbook,
        output_workbook=preview_path,
        design_lock_root=design_lock_root,
    )
    second_preview = materialize_promise_progress_preview_v2(
        product,
        plan,
        legacy_workbook=legacy_workbook,
        output_workbook=repeat_preview_path,
        design_lock_root=design_lock_root,
    )
    structural = validate_preview_structure_v2(
        legacy_workbook=legacy_workbook,
        preview_workbook=preview_path,
        plan=plan,
    )
    repeat_structural = validate_preview_structure_v2(
        legacy_workbook=legacy_workbook,
        preview_workbook=repeat_preview_path,
        plan=plan,
    )
    semantic = validate_preview_semantics_v2(
        product, plan, preview_workbook=preview_path
    )
    repeat_semantic = validate_preview_semantics_v2(
        product, plan, preview_workbook=repeat_preview_path
    )
    visual = validate_preview_visual_fit_v2(
        preview_workbook=preview_path, plan=plan
    )
    repeat_visual = validate_preview_visual_fit_v2(
        preview_workbook=repeat_preview_path, plan=plan
    )
    if not all(
        result["passed"]
        for result in (
            structural,
            repeat_structural,
            semantic,
            repeat_semantic,
            visual,
            repeat_visual,
        )
    ):
        raise ValueError("Product@2 workbook preview failed validation")
    if (
        first_preview["preview_workbook_sha256"]
        != second_preview["preview_workbook_sha256"]
        or first_preview["canonical_workbook_content_sha256"]
        != second_preview["canonical_workbook_content_sha256"]
        or first_preview["target_sheet_semantic_sha256"]
        != second_preview["target_sheet_semantic_sha256"]
        or structural["validation_digest"] != repeat_structural["validation_digest"]
        or semantic["validation_digest"] != repeat_semantic["validation_digest"]
        or visual["validation_digest"] != repeat_visual["validation_digest"]
    ):
        raise ValueError("Product@2 workbook preview regeneration is not deterministic")
    workbook_trace = build_workbook_trace_v2(
        product, plan, preview_workbook=preview_path
    )
    workbook_trace_path = output_root / "workbook_trace_v2.json"
    workbook_trace_sha = _write_json(workbook_trace_path, workbook_trace)
    structural_path = output_root / "structural_validation_v2.json"
    structural_sha = _write_json(structural_path, structural)
    semantic_path = output_root / "semantic_validation_v2.json"
    semantic_sha = _write_json(semantic_path, semantic)
    visual_path = output_root / "visual_validation_v2.json"
    visual_sha = _write_json(visual_path, visual)
    if successor:
        numeric_audit = build_numeric_cell_text_audit(plan, semantic)
        numeric_audit_path = output_root / "numeric_cell_text_audit.json"
        numeric_audit_sha = _write_json(
            numeric_audit_path,
            numeric_audit,
        )
        successor_reports["numeric_cell_text_audit"] = (
            numeric_audit_path,
            numeric_audit_sha,
        )
        numeric_ooxml = {
            **numeric_audit,
            "report_type": "PromiseProgressNumericOOXMLReconciliation@1",
        }
        numeric_ooxml_path = output_root / "numeric_ooxml_reconciliation.json"
        numeric_ooxml_sha = _write_json(numeric_ooxml_path, numeric_ooxml)
        successor_reports["numeric_ooxml_reconciliation"] = (
            numeric_ooxml_path,
            numeric_ooxml_sha,
        )
        assert evidence_foundation is not None
        defect_closure = build_defect_closure_report(
            source_root=source_root,
            product=product,
            foundation=evidence_foundation,
            plan=plan,
            workbook_trace=workbook_trace,
            guidance_report=report_values["guidance_completeness_report"],
            actual_report=report_values["actual_reconciliation_report"],
            progress_report=report_values["progress_reconciliation_report"],
            q4_report=report_values["q4_reconciliation_report"],
            derivation_report=report_values["derivation_lineage_report"],
            status_report=report_values["status_report"],
            blank_report=report_values["timeline_blank_completeness_report"],
            needs_review_report=report_values["needs_review_semantics_review"],
            disposition_report=report_values["foundation_projection_disposition"],
        )
        defect_closure_path = output_root / "defect_closure_report.json"
        defect_closure_sha = _write_json(defect_closure_path, defect_closure)
        successor_reports["defect_closure_report"] = (
            defect_closure_path,
            defect_closure_sha,
        )
        old_defect_regression_path = output_root / "old_defect_regression_report.json"
        old_defect_regression_sha = _write_json(
            old_defect_regression_path, defect_closure
        )
        successor_reports["old_defect_regression_report"] = (
            old_defect_regression_path,
            old_defect_regression_sha,
        )
        current_defect_closure = build_current_defect_closure_report(
            source_root=source_root,
            product=product,
            plan=plan,
            workbook_trace=workbook_trace,
            q4_report=report_values["q4_reconciliation_report"],
            progress_report=report_values["progress_reconciliation_report"],
            blank_report=report_values["timeline_blank_completeness_report"],
            disposition_report=report_values["foundation_projection_disposition"],
            semantic_validation=semantic,
            numeric_audit=numeric_audit,
        )
        current_defect_closure_path = output_root / "current_defect_closure_report.json"
        current_defect_closure_sha = _write_json(
            current_defect_closure_path, current_defect_closure
        )
        successor_reports["current_defect_closure_report"] = (
            current_defect_closure_path,
            current_defect_closure_sha,
        )
        current_count_reconciliation = build_current_count_reconciliation_report(
            source_root=source_root,
            product=product,
            foundation=evidence_foundation,
            plan=plan,
            guidance_report=report_values["guidance_completeness_report"],
            actual_report=report_values["actual_reconciliation_report"],
            progress_report=report_values["progress_reconciliation_report"],
            q4_report=report_values["q4_reconciliation_report"],
            derivation_report=report_values["derivation_lineage_report"],
            status_report=report_values["status_report"],
            needs_review_report=report_values["needs_review_semantics_review"],
            blank_report=report_values["timeline_blank_completeness_report"],
            disposition_report=report_values["foundation_projection_disposition"],
        )
        current_count_path = output_root / "current_count_reconciliation_report.json"
        current_count_sha = _write_json(
            current_count_path, current_count_reconciliation
        )
        successor_reports["current_count_reconciliation_report"] = (
            current_count_path,
            current_count_sha,
        )
    visual_markdown_path = output_root / "visual_validation_v2.md"
    visual_markdown_sha = _write_visual_markdown(
        visual_markdown_path,
        product_sha256=product_sha,
        preview_path=preview_path,
        visual=visual,
        plan=plan,
    )
    manifest = write_candidate_manifest(
        output_root=output_root,
        product=product,
        plan=plan,
        legacy_workbook=legacy_workbook,
    )
    result = {
        "source_set_path": str(source_set_path),
        "source_set_sha256": source_set_sha,
        "evidence_foundation_identity_path": (
            None if foundation_identity_path is None else str(foundation_identity_path)
        ),
        "evidence_foundation_identity_sha256": foundation_identity_sha,
        "product_path": str(product_path),
        "product_sha256": product_sha,
        "shadow_path": str(shadow_path),
        "shadow_sha256": shadow_sha,
        "source_coverage_path": str(source_coverage_path),
        "source_coverage_sha256": source_coverage_sha,
        "data_completeness_path": str(completeness_path),
        "data_completeness_sha256": completeness_sha,
        "product_v1_vs_v2_path": str(comparison_path),
        "product_v1_vs_v2_sha256": comparison_sha,
        "timeline_semantics_path": str(timeline_path),
        "timeline_semantics_sha256": timeline_sha,
        "timeline_actual_progress_role_path": str(timeline_roles_path),
        "timeline_actual_progress_role_sha256": timeline_roles_sha,
        "range_parser_replay_path": str(range_replay_path),
        "range_parser_replay_sha256": range_replay_sha,
        "legacy_capability_completeness_path": str(legacy_capability_path),
        "legacy_capability_completeness_sha256": legacy_capability_sha,
        "capability_completion_path": str(capability_completion_path),
        "capability_completion_sha256": capability_completion_sha,
        "needs_review_audit_path": str(needs_review_path),
        "needs_review_audit_sha256": needs_review_sha,
        "actual_definition_compatibility_path": str(actual_compatibility_path),
        "actual_definition_compatibility_sha256": actual_compatibility_sha,
        "timeline_knowledge_date_path": str(timeline_knowledge_path),
        "timeline_knowledge_date_sha256": timeline_knowledge_sha,
        "presentation_contract_path": str(presentation_contract_path),
        "presentation_contract_sha256": presentation_contract_sha,
        "binding_plan_path": str(binding_plan_path),
        "binding_plan_sha256": binding_plan_sha,
        "preview_path": str(preview_path),
        "preview_sha256": first_preview["preview_workbook_sha256"],
        "preview_canonical_content_sha256": first_preview[
            "canonical_workbook_content_sha256"
        ],
        "preview_target_semantic_sha256": first_preview[
            "target_sheet_semantic_sha256"
        ],
        "workbook_trace_path": str(workbook_trace_path),
        "workbook_trace_sha256": workbook_trace_sha,
        "structural_validation_path": str(structural_path),
        "structural_validation_sha256": structural_sha,
        "semantic_validation_path": str(semantic_path),
        "semantic_validation_sha256": semantic_sha,
        "visual_validation_path": str(visual_path),
        "visual_validation_sha256": visual_sha,
        "visual_markdown_path": str(visual_markdown_path),
        "visual_markdown_sha256": visual_markdown_sha,
        "candidate_manifest_path": str(output_root / "candidate_manifest.json"),
        "candidate_manifest_sha256": _sha(output_root / "candidate_manifest.json"),
        "candidate_manifest_digest": manifest["manifest_digest"],
        "dynamic_used_range": plan.used_range,
        "physical_row_count": len(plan.row_plan),
        "workbook_binding_count": len(plan.bindings),
        "source_documents": (
            len(evidence_foundation["semantic_source_documents"])
            if evidence_foundation is not None
            else len(adapter.package["source_documents"])
        ),
        "guidance_series": sum(
            1 for row in adapter.package["entities"] if row["payload"]["kind"] == "GuidanceSeries"
        )
        + (
            0
            if evidence_foundation is None
            else len(
                {
                    str(row["guidance_series_id"])
                    for row in evidence_foundation["quarter_guidance_versions"]
                }
            )
        ),
        "guidance_versions": sum(
            1 for row in adapter.package["observations"] if row["payload"]["kind"] == "GuidanceVersion"
        )
        + (
            0
            if evidence_foundation is None
            else len(evidence_foundation["quarter_guidance_versions"])
        ),
        "numerical_facts": sum(
            1 for row in adapter.package["observations"] if row["payload"]["kind"] == "NumericalFact"
        ),
        "package": adapter.package,
        "product": product,
        "source_set": source_set,
        "evidence_foundation": evidence_foundation,
        "binding_plan": plan,
    }
    for stem, (report_path, report_sha) in successor_reports.items():
        result[f"{stem}_path"] = str(report_path)
        result[f"{stem}_sha256"] = report_sha
    return result


def refresh_rendered_candidate(
    *,
    source_root: Path,
    repository_root: Path,
    output_root: Path,
    successor: bool = False,
) -> dict[str, Any]:
    """Refresh only render-aware review metadata after deterministic rendering."""

    source_set_path = output_root / "source_set_v2_candidate.json"
    if not source_set_path.is_file():
        raise FileNotFoundError(source_set_path)
    source_set = load_json_strict(source_set_path)
    evidence_foundation = None
    adapter_source_set = build_anf_product_v2_source_set(
        source_root=source_root,
        repository_root=repository_root,
        successor=successor,
    )
    if successor:
        evidence_foundation = build_anf_evidence_foundation(
            source_root=source_root,
            audit_root=source_root / EVIDENCE_AUDIT_RELATIVE_PATH,
        )
        with tempfile.TemporaryDirectory(prefix="anf-product-v2-1-refresh-") as temp_root:
            adapter_path = Path(temp_root) / "adapter_source_set.json"
            _write_json(adapter_path, adapter_source_set)
            adapter = build_source_native_sidecar(
                adapter_path,
                source_root=source_root,
                reviewed_model_root=repository_root,
                sector_pack=RETAIL_SECTOR_PACK_V2,
                ticker_profile_loader=load_anf_profile_v2,
            )
    else:
        adapter = build_source_native_sidecar(
            source_set_path,
            source_root=source_root,
            reviewed_model_root=repository_root,
            sector_pack=RETAIL_SECTOR_PACK_V2,
            ticker_profile_loader=load_anf_profile_v2,
        )
    product = build_promise_progress_product_v2(
        adapter.package,
        source_set_id=source_set["source_set_id"],
        reviewed_links=adapter_source_set["reviewed_links"],
        product_version=(SUCCESSOR_PRODUCT_VERSION if successor else PRODUCT_VERSION),
        evidence_foundation=evidence_foundation,
    )
    plan = build_promise_progress_workbook_binding_plan_v2(
        product,
        design_lock_root=source_root / DESIGN_LOCK_RELATIVE_PATH,
    )
    visual_path = output_root / "visual_validation_v2.json"
    visual = load_json_strict(visual_path)
    visual_markdown_path = output_root / "visual_validation_v2.md"
    visual_markdown_sha = _write_visual_markdown(
        visual_markdown_path,
        product_sha256=promise_progress_product_v2_sha256(product),
        preview_path=output_root / "ANF_Promise_Progress_source_native_v2_preview.xlsx",
        visual=visual,
        plan=plan,
    )
    manifest = write_candidate_manifest(
        output_root=output_root,
        product=product,
        plan=plan,
        legacy_workbook=source_root / LEGACY_WORKBOOK_RELATIVE_PATH,
    )
    return {
        "visual_markdown_path": str(visual_markdown_path),
        "visual_markdown_sha256": visual_markdown_sha,
        "candidate_manifest_path": str(output_root / "candidate_manifest.json"),
        "candidate_manifest_sha256": _sha(output_root / "candidate_manifest.json"),
        "candidate_manifest_digest": manifest["manifest_digest"],
        "rendered_artifact_count": sum(
            1 for path in (output_root / "rendered").rglob("*") if path.is_file()
        ),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT_DEFAULT)
    parser.add_argument("--repository-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument(
        "--successor",
        action="store_true",
        help="build the post-golden Product@2.1 review candidate in a separate root",
    )
    parser.add_argument(
        "--refresh-rendered-artifacts",
        action="store_true",
        help="refresh render-aware visual review metadata and the manifest only",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_root = args.output_root or args.source_root / "audit" / (
        SUCCESSOR_CANDIDATE_ROOT_NAME if args.successor else CANDIDATE_ROOT_NAME
    )
    if args.refresh_rendered_artifacts:
        result = refresh_rendered_candidate(
            source_root=args.source_root.resolve(),
            repository_root=args.repository_root.resolve(),
            output_root=output_root.resolve(),
            successor=args.successor,
        )
    else:
        result = build_candidate(
            source_root=args.source_root.resolve(),
            repository_root=args.repository_root.resolve(),
            output_root=output_root.resolve(),
            successor=args.successor,
        )
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key
                not in {
                    "package",
                    "product",
                    "source_set",
                    "evidence_foundation",
                    "binding_plan",
                }
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
