"""Build deterministic source-native ANF Summary and BS/segment candidates.

This command never edits or opens the production workbook.  It materializes two
external read-only review packages whose economics are owned by canonical facts and
explicit derivations rather than workbook cells.
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

from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_summary_bs_foundation import (
    build_anf_summary_bs_products,
    write_anf_summary_bs_candidate_package,
)


SOURCE_ROOT_DEFAULT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
AUDIT_ROOT_DEFAULT = (
    SOURCE_ROOT_DEFAULT
    / "audit"
    / "anf_summary_bs_segment_exhaustive_historical_lineage_audit_2026-08-10"
)
CANDIDATE_ROOT_DEFAULT = (
    SOURCE_ROOT_DEFAULT
    / "audit"
    / "anf_summary_bs_segment_source_native_contract_candidate"
)
REPEAT_ROOT_DEFAULT = (
    SOURCE_ROOT_DEFAULT
    / "audit"
    / "anf_summary_bs_segment_source_native_contract_repeat"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT_DEFAULT)
    parser.add_argument("--audit-root", type=Path, default=AUDIT_ROOT_DEFAULT)
    parser.add_argument("--candidate-root", type=Path, default=CANDIDATE_ROOT_DEFAULT)
    parser.add_argument("--repeat-root", type=Path, default=REPEAT_ROOT_DEFAULT)
    return parser.parse_args()


def _package_hashes(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)).replace("\\", "/"): _sha256(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def main() -> int:
    args = parse_args()
    first = build_anf_summary_bs_products(args.source_root, args.audit_root)
    second = build_anf_summary_bs_products(args.source_root, args.audit_root)
    first_result = write_anf_summary_bs_candidate_package(first, args.candidate_root)
    second_result = write_anf_summary_bs_candidate_package(second, args.repeat_root)
    candidate_hashes = _package_hashes(args.candidate_root)
    repeat_hashes = _package_hashes(args.repeat_root)
    if candidate_hashes != repeat_hashes:
        raise RuntimeError("Candidate and repeat packages are not byte-identical.")
    summary = {
        "artifact_file_count": len(candidate_hashes),
        "candidate_manifest_sha256": first_result["manifest_sha256"],
        "candidate_root": str(args.candidate_root.resolve()),
        "deterministic": True,
        "economic_defect_count": first["artifacts"]["product_count_reconciliation.json"][
            "economic_defect_count"
        ],
        "field_counts": first["artifacts"]["product_count_reconciliation.json"][
            "field_counts"
        ],
        "production_workbook_modified": False,
        "repeat_manifest_sha256": second_result["manifest_sha256"],
        "repeat_root": str(args.repeat_root.resolve()),
        "status_counts": first["artifacts"]["product_count_reconciliation.json"][
            "status_counts"
        ],
        "workbook_binding_status": "not_wired",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
