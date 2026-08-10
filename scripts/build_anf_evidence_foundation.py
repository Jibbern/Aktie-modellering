"""Build the deterministic ANF reviewed-evidence foundation candidate.

This command is source-layer only.  It does not project Promise Progress rows,
materialize a workbook, update fixtures, or pin a golden.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_evidence_foundation import (
    build_anf_evidence_foundation,
    candidate_artifacts,
    write_evidence_foundation_candidate,
)
from pbi_xbrl.longitudinal_memory.serialization import serialize_package


SOURCE_ROOT_DEFAULT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
AUDIT_ROOT_DEFAULT = (
    SOURCE_ROOT_DEFAULT
    / "audit"
    / "anf_local_source_review_authority_expansion_audit_2026-08-09"
)
OUTPUT_ROOT_DEFAULT = (
    SOURCE_ROOT_DEFAULT
    / "audit"
    / "anf_product_v2_1_reviewed_evidence_foundation_candidate"
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT_DEFAULT)
    parser.add_argument("--audit-root", type=Path, default=AUDIT_ROOT_DEFAULT)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT_DEFAULT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    first = build_anf_evidence_foundation(
        source_root=args.source_root,
        audit_root=args.audit_root,
    )
    second = build_anf_evidence_foundation(
        source_root=args.source_root,
        audit_root=args.audit_root,
    )
    first_payload = serialize_package(first)
    second_payload = serialize_package(second)
    if first_payload != second_payload:
        raise RuntimeError("ANF evidence-foundation regeneration is not deterministic.")

    allowed_names = {*candidate_artifacts(first), "manifest.json"}
    if args.output_root.exists():
        unexpected = sorted(
            str(path.relative_to(args.output_root))
            for path in args.output_root.rglob("*")
            if path.is_file() and str(path.relative_to(args.output_root)) not in allowed_names
        )
        if unexpected:
            raise RuntimeError(
                f"Candidate root contains unexpected existing files: {unexpected}"
            )

    manifest = write_evidence_foundation_candidate(first, args.output_root)
    summary = {
        "output_root": str(args.output_root),
        "foundation_id": first["foundation_id"],
        "foundation_version": first["foundation_version"],
        "source_set_id": first["source_set_id"],
        "foundation_sha256": _sha256(first_payload),
        "semantic_source_document_count": len(first["semantic_source_documents"]),
        "canonical_fact_count": len(first["canonical_facts"]),
        "quarter_guidance_source_assertion_count": len(
            first["quarter_guidance_source_assertions"]
        ),
        "quarter_guidance_version_count": len(first["quarter_guidance_versions"]),
        "sec_release_reconciliation_count": len(
            first["sec_release_reconciliation_relations"]
        ),
        "evidence_disposition": first["evidence_disposition"],
        "manifest": manifest,
        "projection_or_workbook_correction_performed": first[
            "projection_or_workbook_correction_performed"
        ],
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
