"""Materialize the frozen Summary/BS binding plan into one scratch workbook."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    load_materialization_plan,
    materialize_summary_bs_preview,
)


EXPECTED_PLAN_DIGEST = "481fd188c95090b96f810e192c6927a5f5f910672d076a9acc2ebf2591f4a215"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply the immutable Summary/BS plan through the lossless OOXML materializer."
    )
    parser.add_argument("--base-workbook", type=Path, required=True)
    parser.add_argument("--binding-plan", type=Path, required=True)
    parser.add_argument("--output-workbook", type=Path, required=True)
    parser.add_argument("--expected-plan-digest", default=EXPECTED_PLAN_DIGEST)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = load_materialization_plan(
        args.binding_plan,
        expected_plan_digest=args.expected_plan_digest,
    )
    receipt = materialize_summary_bs_preview(
        base_workbook=args.base_workbook,
        output_workbook=args.output_workbook,
        plan=plan,
        expected_plan_digest=args.expected_plan_digest,
    )
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
