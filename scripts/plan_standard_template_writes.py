"""Create a JSON binding plan without opening or creating an Excel workbook."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbi_xbrl.new_ticker_binding_planner import (
    DEFAULT_BINDING_MAP,
    DEFAULT_MANIFEST,
    DEFAULT_SHELL,
    plan_standard_template_writes_from_paths,
    write_binding_plan_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--binding-map", type=Path, default=DEFAULT_BINDING_MAP)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--shell", type=Path, default=DEFAULT_SHELL)
    parser.add_argument("--ticker")
    parser.add_argument("--promotion-requested", action="store_true")
    args = parser.parse_args(argv)

    plan = plan_standard_template_writes_from_paths(
        args.package,
        binding_map_path=args.binding_map,
        manifest_path=args.manifest,
        shell_path=args.shell,
        ticker_override=args.ticker,
        promotion_requested=args.promotion_requested,
    )
    write_binding_plan_report(plan, args.output)
    print(f"{plan.status}: {len(plan.planned_writes)} planned writes -> {args.output}")
    return 0 if plan.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
