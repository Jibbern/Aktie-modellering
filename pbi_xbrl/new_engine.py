"""Supported command-line entrypoint for the deterministic new-engine workflow."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Sequence

from pbi_xbrl.new_engine_excel import ExcelNativeValidationError
from pbi_xbrl.new_engine_orchestration import (
    NewEngineOrchestrationError,
    render_shadow,
    run_plan,
    validate_workbook_immutable,
)
from pbi_xbrl.new_engine_transaction import NewEngineTransactionError
from pbi_xbrl.new_ticker_style_planner import DEFAULT_MODULE_MANIFEST, DEFAULT_STYLE_POLICY
from pbi_xbrl.new_ticker_value_filler import DEFAULT_BINDING_MAP, DEFAULT_MANIFEST, DEFAULT_TEMPLATE


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--package", required=True, type=Path, dest="package_path")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE, dest="template_path")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, dest="manifest_path")
    parser.add_argument("--binding-map", type=Path, default=DEFAULT_BINDING_MAP, dest="binding_map_path")
    parser.add_argument(
        "--module-manifest", type=Path, default=DEFAULT_MODULE_MANIFEST, dest="module_manifest_path"
    )
    parser.add_argument("--style-policy", type=Path, default=DEFAULT_STYLE_POLICY, dest="style_policy_path")
    parser.add_argument("--expected-contract-digest")
    parser.add_argument(
        "--expected-value-plan-digest",
        "--expected-binding-plan-digest",
        dest="expected_binding_plan_digest",
    )
    parser.add_argument("--expected-style-plan-digest")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pbi_xbrl.new_engine",
        description="Plan, render and validate deterministic versioned new-engine shadow workbooks.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Reproduce plans without touching a workbook.")
    _add_common_arguments(plan)
    plan.add_argument("--run-dir", required=True, type=Path)

    render = subparsers.add_parser("render-shadow", help="Transactionally render a versioned shadow workbook.")
    _add_common_arguments(render)
    render.add_argument("--run-dir", required=True, type=Path)
    render.add_argument("--plan-receipt", required=True, type=Path, dest="plan_receipt_path")
    render.add_argument("--output-root", required=True, type=Path)
    render.add_argument("--version", required=True)
    render.add_argument("--excel-native", choices=("off", "required"), default="off")
    render.add_argument("--excel-locale-id", type=int, dest="required_locale_id")

    validate = subparsers.add_parser("validate", help="Validate a workbook without modifying the supplied file.")
    _add_common_arguments(validate)
    validate.add_argument("--run-dir", required=True, type=Path)
    validate.add_argument("--plan-receipt", required=True, type=Path, dest="plan_receipt_path")
    validate.add_argument("--workbook", required=True, type=Path, dest="workbook_path")
    validate.add_argument("--excel-native", choices=("off", "required"), default="off")
    validate.add_argument("--excel-locale-id", type=int, dest="required_locale_id")
    return parser


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = vars(parser.parse_args(argv))
    command = str(args.pop("command"))
    log_level = str(args.pop("log_level"))
    logging.basicConfig(level=getattr(logging, log_level), format="%(levelname)s %(message)s")
    try:
        if command == "plan":
            result = run_plan(**args)
        elif command == "render-shadow":
            result = render_shadow(**args)
        else:
            result = validate_workbook_immutable(**args)
    except (NewEngineOrchestrationError, NewEngineTransactionError, ExcelNativeValidationError, OSError) as exc:
        print(
            json.dumps(
                {"status": "FAIL", "reason": type(exc).__name__, "message": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(_json_safe(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
