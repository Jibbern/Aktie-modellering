"""Verify and reproduce the registered ANF Summary/BS source-native golden."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from pbi_xbrl.longitudinal_memory.summary_bs_golden import (
    GOLDEN_MANIFEST_PATH,
    reproduce_registered_golden,
    verify_golden_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=GOLDEN_MANIFEST_PATH)
    parser.add_argument("--base-workbook", type=Path)
    parser.add_argument("--output-workbook", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if (args.base_workbook is None) != (args.output_workbook is None):
        raise SystemExit("--base-workbook and --output-workbook must be supplied together")
    result = verify_golden_manifest(args.manifest)
    if args.base_workbook is not None and args.output_workbook is not None:
        result = reproduce_registered_golden(
            base_workbook=args.base_workbook,
            output_workbook=args.output_workbook,
            manifest_path=args.manifest,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
