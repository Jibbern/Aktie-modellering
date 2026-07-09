"""Generate normalized-company-data stress reports for a ticker.

This command does not build or inspect a ticker workbook.  It creates a sparse
normalized data package, reports mapping gaps, and runs pre-render validation.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbi_xbrl.normalized_company_data_validation import (
    build_mapping_gap_report,
    validate_normalized_company_data,
)


REQUIRED_SECTIONS = [
    "ticker_metadata",
    "company_profile",
    "quarterly_financials",
    "annual_financials",
    "debt_liquidity",
    "capital_returns",
    "normalized_guidance",
    "segments",
    "operating_drivers",
    "quarter_notes",
    "investment_case",
    "source_coverage",
    "mapping_gaps",
    "manual_review_flags",
]


def _repo_root() -> Path:
    return REPO_ROOT


def _default_data_root() -> Path:
    return _repo_root().parent / "StockModelData"


def _default_output_dir(data_root: Path, ticker: str) -> Path:
    return data_root / "outputs" / "stress_tests" / f"{ticker}_new_ticker_engine"


def _field(
    value: Any,
    *,
    status: str,
    source_ref: str = "",
    core: bool = False,
    reason: str = "",
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "value": value,
        "status": status,
        "source_ref": source_ref,
        "core": bool(core),
    }
    if reason:
        out["reason"] = reason
    return out


def _read_sample(path: Path, *, limit: int = 2400) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")[:limit]
    except (OSError, UnicodeError):
        return ""


def _source_candidates(data_root: Path, ticker: str) -> List[Path]:
    ticker_u = ticker.upper()
    roots = [
        data_root / "tickers" / ticker_u,
        data_root / "sec_cache" / ticker_u,
    ]
    paths: List[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in {".txt", ".htm", ".html", ".json", ".csv", ".md"}:
                paths.append(path)
    return sorted(paths, key=lambda item: str(item).lower())[:80]


def _source_coverage(data_root: Path, ticker: str) -> Dict[str, Any]:
    files = _source_candidates(data_root, ticker)
    return {
        "sources": [
            {
                "path": str(path),
                "kind": path.suffix.lower().lstrip(".") or "file",
                "status": "available",
            }
            for path in files
        ],
        "source_roots": {
            "ticker_root": str(data_root / "tickers" / ticker.upper()),
            "sec_cache": str(data_root / "sec_cache" / ticker.upper()),
        },
    }


def build_sparse_normalized_package(
    *,
    ticker: str,
    data_root: Path,
    stress_test: bool,
) -> Dict[str, Any]:
    ticker_u = ticker.upper()
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    sources = _source_candidates(data_root, ticker_u)
    first_source = str(sources[0]) if sources else ""
    sample_text = _read_sample(sources[0]) if sources else ""

    package: Dict[str, Any] = {
        "package_version": "0.1.0",
        "generated_at_utc": generated_at,
        "stress_test": bool(stress_test),
        "ticker_metadata": {
            "ticker": _field(ticker_u, status="populated", core=True),
            "exchange": _field("", status="missing_source", core=True, reason="No normalized exchange source mapped yet."),
            "cik": _field("", status="missing_source", core=True, reason="SEC identifier intake is not normalized in this pass."),
        },
        "company_profile": {
            "company_name": _field("", status="missing_source", core=True, reason="Profile source exists but no normalized field extractor is active."),
            "sector": _field("", status="manual_review_required", core=True, reason="Sector must be source/profile-backed before render."),
            "business_description": _field("", status="missing_source", core=True, reason="Do not copy visible summary fallback text from GTX rescue code."),
            "allowed_sector_terms": [],
        },
        "quarterly_financials": {
            "rows": [
                {
                    "period": "latest_unmapped",
                    "revenue": _field("", status="missing_source", core=True, reason="Quarterly financial parser output is not normalized yet."),
                    "operating_income": _field("", status="missing_source", core=True, reason="Quarterly financial parser output is not normalized yet."),
                    "diluted_shares": _field("", status="missing_source", core=True, reason="Share count must be unit-checked before valuation."),
                }
            ]
        },
        "annual_financials": {
            "rows": [
                {
                    "period": "latest_unmapped",
                    "revenue": _field("", status="missing_source", core=True, reason="Annual financial parser output is not normalized yet."),
                    "operating_income": _field("", status="missing_source", core=True, reason="Annual financial parser output is not normalized yet."),
                }
            ]
        },
        "debt_liquidity": {
            "cash": _field("", status="missing_source", core=True, reason="Debt/liquidity package not normalized yet."),
            "total_debt": _field("", status="missing_source", core=True, reason="Debt/liquidity package not normalized yet."),
            "net_debt": _field("", status="missing_source", core=True, reason="Debt/liquidity package not normalized yet."),
        },
        "capital_returns": {
            "buybacks": _field("", status="missing_source", core=True, reason="Capital return rows are not normalized yet."),
            "dividends": _field("", status="not_applicable", core=False, reason="Not assumed applicable until source-backed."),
        },
        "normalized_guidance": {
            "items": [
                {
                    "metric": _field("Unclassified guidance candidate", status="parser_conflict", core=True, source_ref=first_source, reason="Stress package keeps raw candidate unpromoted."),
                    "value": _field("", status="parser_conflict", core=True, source_ref=first_source, reason="No clean metric/value mapping yet."),
                    "source_excerpt": sample_text,
                }
            ]
            if sample_text
            else []
        },
        "segments": {
            "items": [
                {
                    "segment": _field("", status="missing_source", core=True, reason="Segment taxonomy is not normalized yet."),
                    "revenue": _field("", status="missing_source", core=True, reason="Segment values are not normalized yet."),
                }
            ]
        },
        "operating_drivers": {
            "items": [
                {
                    "driver": _field("", status="missing_source", core=True, reason="Driver extraction must be source-backed."),
                    "current_read": _field("", status="missing_source", core=True, reason="No normalized operating-driver read yet."),
                }
            ]
        },
        "quarter_notes": {
            "items": [
                {
                    "period": "latest_unmapped",
                    "note": _field(sample_text, status="parser_conflict", source_ref=first_source, core=True, reason="Raw source text is stress evidence only."),
                }
            ]
            if sample_text
            else []
        },
        "investment_case": {
            "summary": _field("", status="manual_review_required", core=True, reason="Investment case must not use GTX rescue placeholders."),
            "key_debate": _field("", status="manual_review_required", core=True, reason="Promotion requires source-backed debate framing."),
        },
        "source_coverage": _source_coverage(data_root, ticker_u),
        "mapping_gaps": [],
        "manual_review_flags": [],
    }
    return package


def _load_binding_map(path: Optional[Path]) -> List[Mapping[str, Any]]:
    binding_path = path or (_repo_root() / "docs" / "workbook_binding_map.json")
    payload = json.loads(binding_path.read_text(encoding="utf-8"))
    return list(payload.get("bindings") or [])


def write_reports(
    *,
    package: Mapping[str, Any],
    binding_map: Sequence[Mapping[str, Any]],
    output_dir: Path,
    promotion_requested: bool,
) -> Dict[str, Path]:
    ticker = str(package["ticker_metadata"]["ticker"]["value"]).upper()
    output_dir.mkdir(parents=True, exist_ok=True)
    mapping_report = build_mapping_gap_report(package, binding_map, ticker=ticker)
    validation_issues = validate_normalized_company_data(
        package,
        binding_map=binding_map,
        promotion_requested=promotion_requested,
    )
    validation_report = {
        "ticker": ticker,
        "promotion_requested": bool(promotion_requested),
        "issue_count": len(validation_issues),
        "issues": [issue.to_dict() for issue in validation_issues],
    }

    package_out = dict(package)
    package_out["mapping_gaps"] = mapping_report["gaps"]
    package_out["manual_review_flags"] = validation_report["issues"]

    paths = {
        "package": output_dir / f"{ticker}_normalized_data_package.json",
        "mapping_gaps": output_dir / f"{ticker}_mapping_gaps_report.json",
        "validation": output_dir / f"{ticker}_content_validation_report.json",
    }
    paths["package"].write_text(json.dumps(package_out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    paths["mapping_gaps"].write_text(json.dumps(mapping_report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    paths["validation"].write_text(json.dumps(validation_report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return paths


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--data-root", type=Path, default=_default_data_root())
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--binding-map", type=Path, default=None)
    parser.add_argument("--stress-test", action="store_true")
    parser.add_argument("--promotion-requested", action="store_true")
    args = parser.parse_args(argv)

    ticker = str(args.ticker).strip().upper()
    data_root = args.data_root.expanduser().resolve()
    output_dir = args.output_dir or _default_output_dir(data_root, ticker)
    binding_map = _load_binding_map(args.binding_map)
    package = build_sparse_normalized_package(
        ticker=ticker,
        data_root=data_root,
        stress_test=bool(args.stress_test),
    )
    paths = write_reports(
        package=package,
        binding_map=binding_map,
        output_dir=output_dir,
        promotion_requested=bool(args.promotion_requested),
    )
    for label, path in paths.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
