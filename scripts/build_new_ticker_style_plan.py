"""Reproduce and serialize an exact-cell style plan without opening Excel."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.json_schema_validation import load_json_strict  # noqa: E402
from pbi_xbrl.new_ticker_style_planner import (  # noqa: E402
    DEFAULT_BINDING_MAP,
    DEFAULT_MANIFEST,
    DEFAULT_MODULE_MANIFEST,
    DEFAULT_SHELL,
    DEFAULT_STYLE_POLICY,
    reproduce_style_plan,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binding-map", type=Path, default=DEFAULT_BINDING_MAP)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--module-manifest", type=Path, default=DEFAULT_MODULE_MANIFEST)
    parser.add_argument("--style-policy", type=Path, default=DEFAULT_STYLE_POLICY)
    parser.add_argument("--shell", type=Path, default=DEFAULT_SHELL)
    parser.add_argument("--expected-binding-plan", type=Path)
    args = parser.parse_args(argv)

    package = load_json_strict(args.package)
    binding_payload = load_json_strict(args.binding_map)
    manifest = load_json_strict(args.manifest)
    modules = load_json_strict(args.module_manifest)
    style_contract = load_json_strict(args.style_policy)
    expected_plan = load_json_strict(args.expected_binding_plan) if args.expected_binding_plan else None
    _value_plan, style_plan = reproduce_style_plan(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        shell_path=args.shell,
        module_payload=modules,
        style_contract=style_contract,
        expected_binding_plan=expected_plan,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(style_plan.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"PASS: {len(style_plan.actions)} exact style actions -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
