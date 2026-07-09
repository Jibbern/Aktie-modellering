"""Fill the frozen standard workbook shell from a normalized data package.

This CLI is the first value-only runtime surface. It does not parse sources,
call production workbook writers, create macros, or onboard real tickers by
itself.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.new_ticker_value_filler import (  # noqa: E402
    BindingContractError,
    NewTickerValueFillerError,
    NormalizedDataValidationError,
    fill_standard_template_from_package,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path, help="Normalized company data package JSON.")
    parser.add_argument("--output", required=True, type=Path, help="Output .xlsx path.")
    parser.add_argument("--ticker", default=None, help="Optional uppercase ticker override.")
    parser.add_argument("--template", type=Path, default=ROOT / "templates" / "standard_stock_model_template.xlsx")
    parser.add_argument("--manifest", type=Path, default=ROOT / "docs" / "standard_template_shell_manifest.json")
    parser.add_argument("--binding-map", type=Path, default=ROOT / "docs" / "workbook_binding_map.json")
    parser.add_argument("--promotion-requested", action="store_true", help="Apply promotion-only normalized-data checks.")
    args = parser.parse_args(argv)

    try:
        result = fill_standard_template_from_package(
            args.package,
            output_path=args.output,
            ticker_override=args.ticker,
            template_path=args.template,
            manifest_path=args.manifest,
            binding_map_path=args.binding_map,
            promotion_requested=args.promotion_requested,
        )
    except NormalizedDataValidationError as exc:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "reason": "normalized_data_validation",
                    "issues": [issue.to_dict() for issue in exc.issues],
                },
                indent=2,
            )
        )
        return 2
    except (BindingContractError, NewTickerValueFillerError) as exc:
        print(json.dumps({"status": "FAIL", "reason": type(exc).__name__, "message": str(exc)}, indent=2))
        return 1

    print(
        json.dumps(
            {
                "status": "PASS",
                "ticker": result.ticker,
                "output_path": str(result.output_path),
                "written_cell_count": result.written_cell_count,
                "validation_issue_count": result.validation_issue_count,
                "mapping_gap_count": result.mapping_gap_count,
                "manual_review_count": result.manual_review_count,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
