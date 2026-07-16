"""Build a deterministic projection audit of the authoritative style contract."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.json_schema_validation import load_json_strict  # noqa: E402
from pbi_xbrl.new_ticker_style_planner import (  # noqa: E402
    DEFAULT_BINDING_MAP,
    DEFAULT_MODULE_MANIFEST,
    DEFAULT_STYLE_POLICY,
    load_style_policy_contract,
    style_policy_ids_for_profile,
)
from pbi_xbrl.workbook_modules import canonical_json_sha256, style_range_contracts  # noqa: E402


DEFAULT_JSON_OUTPUT = ROOT / "docs" / "standard_template_style_policy_audit.json"
DEFAULT_MARKDOWN_OUTPUT = ROOT / "docs" / "standard_template_style_policy_audit.md"
AUDIT_VERSION = "1.0.0"


def build_audit(
    *,
    style_path: Path = DEFAULT_STYLE_POLICY,
    module_path: Path = DEFAULT_MODULE_MANIFEST,
    binding_path: Path = DEFAULT_BINDING_MAP,
) -> dict[str, Any]:
    modules = load_json_strict(module_path)
    bindings = load_json_strict(binding_path)
    contract = load_style_policy_contract(style_path, module_payload=modules, binding_payload=bindings)
    policies = list(contract["policies"])
    disabled_targets = list(contract["style_disabled"])
    ranges = {row.contract_id: row for row in style_range_contracts(modules)}
    sheets_by_policy = {
        str(policy["policy_id"]): sorted({ranges[str(style_id)].sheet for style_id in policy["owned_style_ids"]})
        for policy in policies
    }
    profiles = {
        profile_id: {
            "policy_count": len(style_policy_ids_for_profile(contract, modules, profile_id)),
            "policy_ids": list(style_policy_ids_for_profile(contract, modules, profile_id)),
        }
        for profile_id in ("full_union", "anf", "pbi", "gpre", "core_only")
    }
    return {
        "audit_version": AUDIT_VERSION,
        "status": "PASS",
        "authoritative_contract": str(style_path.relative_to(ROOT)).replace("\\", "/"),
        "authoritative_contract_digest": canonical_json_sha256(contract),
        "module_manifest_digest": canonical_json_sha256(modules),
        "binding_contract_digest": canonical_json_sha256(bindings),
        "policy_count": len(policies),
        "selector_count": sum(len(policy["target_selectors"]) for policy in policies),
        "style_disabled_count": len(disabled_targets),
        "palette_tokens": contract["palette_tokens"],
        "threshold_sets": contract["threshold_sets"],
        "counts_by_module": dict(sorted(Counter(str(row["owner_module_id"]) for row in policies).items())),
        "counts_by_period_type": dict(sorted(Counter(str(row["period_type"]) for row in policies).items())),
        "counts_by_comparison_basis": dict(sorted(Counter(str(row["comparison_basis"]) for row in policies).items())),
        "counts_by_polarity": dict(sorted(Counter(str(row["polarity"]) for row in policies).items())),
        "style_disabled_by_module": dict(
            sorted(Counter(str(row["owner_module_id"]) for row in disabled_targets).items())
        ),
        "selector_completeness_contract": {
            "status": "PASS",
            "rule": "Every active formula target overlapping an active style-owned range has exactly one selector or exact style_disabled disposition.",
        },
        "sheets_by_policy": sheets_by_policy,
        "profiles": profiles,
        "intentional_corrections": [
            {
                "correction_id": "annual_segment_prior_year",
                "policy_id": "segment_annual_revenue",
                "contract": "fiscal_year lag 1",
                "reason": "Annual YoY compares the immediately preceding fiscal year, never a four-column quarterly lag.",
            },
            {
                "correction_id": "exact_positive_boundaries",
                "policy_id": "legacy_five_band_change",
                "contract": "+5% enters positive and +15% enters strong_positive",
                "reason": "Boundary inclusivity is explicit and no longer inherited from procedural branch order.",
            },
            {
                "correction_id": "direct_formula_deltas",
                "policy_id": "valuation_cash_flow_delta_direct,valuation_share_delta_direct_lower",
                "contract": "already-calculated FCF and diluted-share deltas are classified directly",
                "reason": "A calculated delta is a signal, not a value to compare against another period.",
            },
            {
                "correction_id": "fcf_conversion_base_ebitda",
                "policy_id": "valuation_fcf_conversion_ttm",
                "contract": "FCF conversion uses the accepted generic base-EBITDA formula and prior-TTM comparison",
                "reason": "Legacy preferred adjusted EBITDA when available; style planning must preserve the accepted generic formula definition.",
            },
        ],
    }


def render_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Standard Template Style Policy Audit",
        "",
        f"Status: **{audit['status']}**",
        "",
        f"- Authoritative contract: `{audit['authoritative_contract']}`",
        f"- Contract digest: `{audit['authoritative_contract_digest']}`",
        f"- Policies: {audit['policy_count']}",
        f"- Exact target selectors: {audit['selector_count']}",
        f"- Explicit no-style formula targets: {audit['style_disabled_count']}",
        "",
        "## Profiles",
        "",
        "| Profile | Active policies |",
        "|---|---:|",
    ]
    lines.extend(f"| {profile_id} | {row['policy_count']} |" for profile_id, row in audit["profiles"].items())
    lines.extend(["", "## Intentional Corrections", ""])
    lines.extend(
        f"- `{row['correction_id']}`: {row['contract']}. {row['reason']}"
        for row in audit["intentional_corrections"]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--style-policy", type=Path, default=DEFAULT_STYLE_POLICY)
    parser.add_argument("--module-manifest", type=Path, default=DEFAULT_MODULE_MANIFEST)
    parser.add_argument("--binding-map", type=Path, default=DEFAULT_BINDING_MAP)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_MARKDOWN_OUTPUT)
    args = parser.parse_args(argv)

    audit = build_audit(style_path=args.style_policy, module_path=args.module_manifest, binding_path=args.binding_map)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(audit, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.markdown_output.write_text(render_markdown(audit), encoding="utf-8")
    print(f"PASS: {audit['policy_count']} style policies")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
