"""Write the digest-backed freshness status for standard-template audits."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.json_schema_validation import load_json_strict  # noqa: E402
from pbi_xbrl.standard_template_audit_freshness import (  # noqa: E402
    DEFAULT_FRESHNESS_PATH,
    write_audit_freshness,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell", type=Path, default=ROOT / "templates" / "standard_stock_model_template.xlsx")
    parser.add_argument("--manifest", type=Path, default=ROOT / "docs" / "standard_template_shell_manifest.json")
    parser.add_argument("--binding-map", type=Path, default=ROOT / "docs" / "workbook_binding_map.json")
    parser.add_argument("--output", type=Path, default=DEFAULT_FRESHNESS_PATH)
    args = parser.parse_args(argv)

    output = write_audit_freshness(
        output_path=args.output.expanduser().resolve(),
        shell_path=args.shell.expanduser().resolve(),
        manifest=load_json_strict(args.manifest.expanduser().resolve()),
        binding_payload=load_json_strict(args.binding_map.expanduser().resolve()),
        root=ROOT,
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    print(f"standard-template audit freshness: {payload['status']} ({output})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
