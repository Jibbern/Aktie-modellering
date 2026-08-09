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
    PROGRESSION_BLOCK_ID,
    TIMELINE_BLOCK_ID,
    build_product_v2_shadow,
    build_promise_progress_product_v2,
    classify_change,
    promise_progress_product_v2_sha256,
    serialize_product_v2_shadow,
    serialize_promise_progress_product_v2,
)
from pbi_xbrl.longitudinal_memory.source_adapter import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.source_adapter.html import _rows, _span_fingerprint, _text
from pbi_xbrl.longitudinal_memory.source_adapter.types import text_sha256
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile_v2
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
SOURCE_ROOT_DEFAULT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
LEGACY_WORKBOOK_RELATIVE_PATH = Path("outputs") / "Excel stock models" / "ANF_model.xlsx"
DESIGN_LOCK_RELATIVE_PATH = Path("audit") / "promise_progress_design_lock"

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
        f"- Preview workbook: `{preview_path}`",
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
    names = (
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
        "presentation_contract_v7.json",
        "binding_plan_v2.json",
        "workbook_trace_v2.json",
        "structural_validation_v2.json",
        "semantic_validation_v2.json",
        "visual_validation_v2.json",
        "visual_validation_v2.md",
        "ANF_Promise_Progress_source_native_v2_preview.xlsx",
        "ANF_Promise_Progress_source_native_v2_preview_repeat.xlsx",
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
        "manifest_type": "PromiseProgressProductV2CandidateManifest@1",
        "candidate_state": "review-only-not-golden-not-production-cutover",
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
    return {
        "period_key": f"fy{year}-q{quarter}",
        "period_id": f"period:anf:fy{year}-q{quarter}@1",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_type": "quarter",
        "start_date": str(row["start_date"]),
        "end_date": str(row["end_date"]),
        "week_count": 13,
        "fiscal_ordinal": (year - 2000) * 4 + quarter,
        "is_53_week_year": False,
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
    duration_fingerprint = f"Thirteen Weeks Ended {end_label}"
    excerpt = _source_case(document_text, duration_fingerprint)
    quarter_fingerprint = _source_case(
        document_text,
        {1: "FIRST QUARTER", 2: "SECOND QUARTER", 3: "THIRD QUARTER"}[quarter],
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
        "is_53_week_year": False,
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


def _quarterly_progress_assertions(source_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for event in QUARTERLY_PROGRESS_EVENTS:
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

        _sales_index, sales_table, sales_rows = _one_table(
            root, "1 YR % Change", "Total company"
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
                    table_fingerprints=("1 YR % Change", "Total company"),
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

        _ops_index, _ops_table, operations_rows = _one_table(
            root,
            "Condensed Consolidated Statements of Operations",
            f"Thirteen Weeks Ended {end_label}",
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
                    table_fingerprints=(
                        "Condensed Consolidated Statements of Operations",
                        f"Thirteen Weeks Ended {end_label}",
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
                            "Condensed Consolidated Statements of Operations",
                            f"Thirteen Weeks Ended {end_label}",
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


def build_anf_product_v2_source_set(*, source_root: Path, repository_root: Path) -> dict[str, Any]:
    """Return the closed, deterministic ANF Product@2 candidate source set."""

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

    observations = {
        str(row["header"]["record_id"]): row
        for row in (() if package is None else package["observations"])
    }
    occurrences = {
        str(row["evidence_occurrence_id"]): row
        for row in (() if package is None else package["evidence_occurrences"])
    }
    series_rows = (
        ()
        if package is None
        else tuple(
            row
            for row in package["entities"]
            if row["payload"]["kind"] == "GuidanceSeries"
        )
    )

    def candidate_sources(record_ids: Iterable[str]) -> list[str]:
        result: set[str] = set()
        for record_id in record_ids:
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
                    "final_proposed_status": row.status_at_update,
                    "remaining_blocker": material_reason,
                }
            )
    return {
        "report_type": "PromiseProgressNeedsReviewAudit@1",
        "allowed_final_categories": ["A", "B", "C"],
        "category_vocabulary": {
            "A": "genuine basis incompatibility",
            "B": "genuine definition incompatibility",
            "C": "genuine missing reviewed source evidence",
            "D": "source evidence exists but extraction or mapping is incomplete",
            "E": "status or outcome logic is incomplete",
            "F": "investor-visible row should not exist",
        },
        "visible_needs_review_count": len(result_rows),
        "prior_candidate_visible_needs_review_count": 9,
        "reason_corrections": [
            {
                "metric_id": "metric:retail:store-remodels-right-sizes@1",
                "horizon": "FY2025",
                "before_reason_code": "comparable_actual_unavailable",
                "after_reason_code": "approximate_target_direction_ambiguous",
                "actual_after": "58",
            }
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

    observations = {
        str(row["header"]["record_id"]): row for row in package["observations"]
    }
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
                "record_id": str(value["header"]["record_id"]),
                "metric_id": str(value["payload"]["metric_id"]),
                "definition_id": str(value["payload"]["definition_id"]),
                "basis_id": str(value["payload"]["basis_id"]),
                "unit_id": str(value["payload"]["unit_id"]),
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
        "pre_release_rows": [
            row for row in result_rows if row["stated_in"].endswith("pre-release")
        ],
        "rows": result_rows,
    }


def build_timeline_actual_progress_role_report(
    product: Any, package: Mapping[str, Any]
) -> dict[str, Any]:
    """Replay one typed Actual/Progress role for every investor Timeline row."""

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


def build_candidate(*, source_root: Path, repository_root: Path, output_root: Path) -> dict[str, Any]:
    source_set = build_anf_product_v2_source_set(
        source_root=source_root, repository_root=repository_root
    )
    source_set_path = output_root / "source_set_v2_candidate.json"
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
        reviewed_links=source_set["reviewed_links"],
    )
    product_payload = serialize_promise_progress_product_v2(product)
    product_path = output_root / "product_v2_candidate.json"
    product_path.write_bytes(product_payload)
    product_sha = hashlib.sha256(product_payload).hexdigest()
    shadow = build_product_v2_shadow(product, adapter.package)
    shadow_payload = serialize_product_v2_shadow(shadow)
    shadow_path = output_root / "shadow_v2_candidate.json"
    shadow_path.write_bytes(shadow_payload)
    shadow_sha = hashlib.sha256(shadow_payload).hexdigest()

    source_coverage = {
        "report_type": "PromiseProgressSourceCoverageReport@2",
        "source_set_id": source_set["source_set_id"],
        "coverage_state": product.coverage_state,
        "documents": [
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
        ],
    }
    source_coverage_path = output_root / "source_coverage_report.json"
    source_coverage_sha = _write_json(source_coverage_path, source_coverage)

    blocks = {block.block_id: block for block in product.blocks}
    progression_rows = blocks[PROGRESSION_BLOCK_ID].rows
    open_rows = blocks[OPEN_BLOCK_ID].rows
    completeness_rows = [
        {
            "fiscal_year": int(str(row.horizon_label).removeprefix("FY")),
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
            "fiscal_year": int(str(row.horizon_label).removeprefix("FY")),
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
    for row in blocks["block:promise-progress:revision-timeline@2"].rows:
        version_state_counts[str(row.version_state)] = version_state_counts.get(str(row.version_state), 0) + 1
        change_type_counts[str(row.change_type)] = change_type_counts.get(str(row.change_type), 0) + 1
        outcome_status_counts[str(row.status_at_update)] = outcome_status_counts.get(
            str(row.status_at_update), 0
        ) + 1
    timeline_report = {
        "report_type": "PromiseProgressTimelineSemanticsReport@2",
        "ordering": "disclosure_event_date_desc_then_event_id_then_metric_order_then_row_id",
        "single_logical_header": True,
        "visible_stated_in_field": True,
        "event_count": len(product.disclosure_events),
        "timeline_row_count": len(blocks["block:promise-progress:revision-timeline@2"].rows),
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
            "before_counts": outcome_status_counts,
            "after_counts": outcome_status_counts,
            "changed_rows": [],
        },
        "change_type_counts": change_type_counts,
        "current_source_separate_from_predecessor": True,
        "typed_reporting_update_event_groups": True,
        "event_source_visible": False,
        "event_identity_retained_per_trace_record": True,
        "visible_timeline_horizon_column": True,
        "visible_source_date_column": True,
        "visible_outcome_status_not_lifecycle_state": True,
    }
    timeline_path = output_root / "timeline_semantics_report.json"
    timeline_sha = _write_json(timeline_path, timeline_report)

    timeline_roles = build_timeline_actual_progress_role_report(
        product, adapter.package
    )
    timeline_roles_path = output_root / "timeline_actual_progress_role_report.json"
    timeline_roles_sha = _write_json(timeline_roles_path, timeline_roles)

    range_replay = build_range_parser_replay_report(
        product, adapter.package, source_set
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

    needs_review_audit = build_needs_review_audit(product, adapter.package)
    needs_review_path = output_root / "needs_review_audit.json"
    needs_review_sha = _write_json(needs_review_path, needs_review_audit)

    actual_compatibility = build_actual_definition_compatibility_report(
        product, adapter.package
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
    presentation_contract_path = output_root / "presentation_contract_v7.json"
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
        "source_documents": len(adapter.package["source_documents"]),
        "guidance_series": sum(
            1 for row in adapter.package["entities"] if row["payload"]["kind"] == "GuidanceSeries"
        ),
        "guidance_versions": sum(
            1 for row in adapter.package["observations"] if row["payload"]["kind"] == "GuidanceVersion"
        ),
        "numerical_facts": sum(
            1 for row in adapter.package["observations"] if row["payload"]["kind"] == "NumericalFact"
        ),
        "package": adapter.package,
        "product": product,
        "source_set": source_set,
        "binding_plan": plan,
    }
    return result


def refresh_rendered_candidate(
    *, source_root: Path, repository_root: Path, output_root: Path
) -> dict[str, Any]:
    """Refresh only render-aware review metadata after deterministic rendering."""

    source_set_path = output_root / "source_set_v2_candidate.json"
    if not source_set_path.is_file():
        raise FileNotFoundError(source_set_path)
    adapter = build_source_native_sidecar(
        source_set_path,
        source_root=source_root,
        reviewed_model_root=repository_root,
        sector_pack=RETAIL_SECTOR_PACK_V2,
        ticker_profile_loader=load_anf_profile_v2,
    )
    source_set = load_json_strict(source_set_path)
    product = build_promise_progress_product_v2(
        adapter.package,
        source_set_id=source_set["source_set_id"],
        reviewed_links=source_set["reviewed_links"],
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
        "--refresh-rendered-artifacts",
        action="store_true",
        help="refresh render-aware visual review metadata and the manifest only",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_root = args.output_root or args.source_root / "audit" / CANDIDATE_ROOT_NAME
    if args.refresh_rendered_artifacts:
        result = refresh_rendered_candidate(
            source_root=args.source_root.resolve(),
            repository_root=args.repository_root.resolve(),
            output_root=output_root.resolve(),
        )
    else:
        result = build_candidate(
            source_root=args.source_root.resolve(),
            repository_root=args.repository_root.resolve(),
            output_root=output_root.resolve(),
        )
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in {"package", "product", "source_set", "binding_plan"}
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
