from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from pbi_xbrl.longitudinal_memory.summary_bs_workbook_projection import (
    build_summary_bs_projection_plan_from_paths,
    write_summary_bs_projection_plan,
)


PROTECTED_ANF_WORKBOOK_SHA256 = (
    "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the shadow-first ANF Summary/BS_Segments workbook projection plan."
    )
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument("--surface-map", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_summary_bs_projection_plan_from_paths(
        summary_product_path=args.candidate_root / "summary_product.json",
        summary_shadow_path=args.candidate_root / "summary_shadow.json",
        bs_product_path=args.candidate_root / "bs_segment_product.json",
        bs_shadow_path=args.candidate_root / "bs_segment_shadow.json",
        surface_map_path=args.surface_map,
        protected_workbook_sha256=PROTECTED_ANF_WORKBOOK_SHA256,
    )
    output = write_summary_bs_projection_plan(plan, args.output)
    print(
        json.dumps(
            {
                "binding_count": plan["validation"]["binding_count"],
                "lifecycle": plan["lifecycle"],
                "output": str(output.resolve()),
                "passed": plan["validation"]["passed"],
                "plan_digest": plan["plan_digest"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
