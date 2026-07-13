"""Run or replay one declared standard-template audit through the trusted runner."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.standard_template_audit_freshness import DEFAULT_AUDIT_CONTRACTS  # noqa: E402
from pbi_xbrl.standard_template_audit_runner import (  # noqa: E402
    run_audit_generator,
    verify_deterministic_audit_replay,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--generator",
        choices=[contract.generator for contract in DEFAULT_AUDIT_CONTRACTS],
        help="Exactly one declared audit generator to execute.",
    )
    selection.add_argument(
        "--all",
        action="store_true",
        help="Replay every declared audit in isolated temporary storage.",
    )
    parser.add_argument(
        "--replay-only",
        action="store_true",
        help="Rerun in temporary storage and compare canonical content without promoting outputs.",
    )
    args = parser.parse_args(argv)

    if args.all:
        if not args.replay_only:
            parser.error("--all is read-only and requires --replay-only")
        results = [
            verify_deterministic_audit_replay(contract.generator, root=ROOT)
            for contract in DEFAULT_AUDIT_CONTRACTS
        ]
        payload = {
            "status": "PASS" if all(row["status"] in {"PASS", "SKIPPED"} for row in results) else "FAIL",
            "replays": results,
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0 if payload["status"] == "PASS" else 1

    generator = ROOT / args.generator
    if args.replay_only:
        result = verify_deterministic_audit_replay(generator, root=ROOT)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["status"] == "PASS" else 1

    result = run_audit_generator(generator, root=ROOT)
    print(
        json.dumps(
            {
                "status": "PASS",
                "generator": args.generator,
                "run_generation_id": result.generation_run.payload["run_generation_id"],
                "artifacts": list(result.comparison_rows),
                "receipts": [str(path) for path in result.receipt_paths],
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
