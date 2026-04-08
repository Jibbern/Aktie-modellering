#!/usr/bin/env python3
"""Small CLI helper for USDA NWER / AMS archive backfill."""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from pbi_xbrl.market_data.usda_backfill import run_usda_archive_backfill


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(str(value or "").strip())
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill USDA NWER / AMS archive PDFs into local USDA folders and sec_cache.")
    ap.add_argument("--ticker", required=True, help="Ticker root to populate, for example GPRE")
    ap.add_argument("--start", required=True, type=_parse_date, help="Start date in YYYY-MM-DD")
    ap.add_argument("--end", required=True, type=_parse_date, help="End date in YYYY-MM-DD")
    ap.add_argument(
        "--sources",
        default="nwer,ams_3617",
        help="Comma-separated subset of USDA sources to backfill. Default: nwer,ams_3617",
    )
    ap.add_argument("--cache-dir", default="", help="Optional explicit ticker cache dir")
    ap.add_argument("--skip-sync", action="store_true", help="Download into local USDA folders only, without sec_cache sync/export rebuild")
    args = ap.parse_args()

    source_list = tuple(str(x or "").strip() for x in str(args.sources or "").split(",") if str(x or "").strip())
    summary = run_usda_archive_backfill(
        repo_root=_project_root(),
        ticker=str(args.ticker or "").strip().upper(),
        start_date=args.start,
        end_date=args.end,
        sources=source_list,
        cache_dir=Path(args.cache_dir).expanduser().resolve() if str(args.cache_dir or "").strip() else None,
        sync_cache=not bool(args.skip_sync),
    )

    print(f"[usda_backfill] ticker={summary.ticker} start={summary.start_date.isoformat()} end={summary.end_date.isoformat()}")
    for item in summary.provider_summaries:
        print(
            "[usda_backfill] "
            f"source={item.source} "
            f"status={'error' if str(item.error_text or '').strip() else 'ok'} "
            f"dir={item.local_dir} "
            f"discovered={item.discovered_assets} "
            f"downloaded={item.downloaded_files} "
            f"skipped={item.skipped_existing}"
        )
        if str(item.error_text or "").strip():
            print(f"[usda_backfill] source={item.source} error={item.error_text}")
    if summary.market_sync_summary is not None:
        market = summary.market_sync_summary
        print(
            "[market_data] "
            f"sources={','.join(market.sources_enabled) or 'none'} "
            f"raw_added={market.raw_added} "
            f"raw_refreshed={market.raw_refreshed} "
            f"raw_skipped={market.raw_skipped} "
            f"parsed={','.join(market.parsed_sources) or 'none'} "
            f"export_rows={market.export_rows} "
            f"export_path={market.export_path}"
        )


if __name__ == "__main__":
    main()
