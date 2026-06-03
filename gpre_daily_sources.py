#!/usr/bin/env python3
"""Fast daily GPRE source-file refresh.

Use this for the common operator task "download today's GPRE corn bids and USDA
files". It intentionally stops before the full market-cache parse/export step in
``stock_models.py --refresh-market-data``; that full path is useful for rebuilding
workbook-facing parquet exports, but it can run long enough to hit interactive
timeouts when the user only needs fresh source files on disk.
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import queue
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from pbi_xbrl.market_data.providers import PROVIDERS
from pbi_xbrl.market_data.service import download_gpre_corn_bids_snapshot
from pbi_xbrl.path_config import resolve_effective_data_root, resolve_stock_model_paths


_DEFAULT_USDA_SOURCES = ("nwer", "ams_3617", "ams_3618")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(str(value or "").strip())
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def _source_list(value: str) -> tuple[str, ...]:
    requested = tuple(str(item or "").strip() for item in str(value or "").split(",") if str(item or "").strip())
    return requested or _DEFAULT_USDA_SOURCES


def _latest_entries(entries: Iterable[Dict[str, Any]], *, limit: int = 4) -> List[Dict[str, Any]]:
    rows = [dict(item) for item in entries if isinstance(item, dict)]
    rows.sort(key=lambda item: (str(item.get("report_date") or ""), str(item.get("path") or "")), reverse=True)
    return rows[: max(0, int(limit))]


def _usda_source_worker(
    source_key: str,
    ticker_root_text: str,
    cache_root_text: str,
    usda_timeout_seconds: float,
    usda_retry_attempts: int,
    out_queue: Any,
) -> None:
    provider = PROVIDERS.get(source_key)
    if provider is None:
        out_queue.put({"source": source_key, "status": "error", "error": "unsupported source"})
        return
    setattr(provider, "remote_timeout_seconds", float(usda_timeout_seconds))
    setattr(provider, "remote_retry_attempts", max(1, int(usda_retry_attempts)))
    try:
        entries = provider.discover_available(Path(ticker_root_text), refresh=True, cache_root=Path(cache_root_text))
        out_queue.put(
            {
                "source": source_key,
                "status": "ok",
                "entry_count": len(entries),
                "latest_entries": _latest_entries(entries),
            }
        )
    except Exception as exc:
        out_queue.put({"source": source_key, "status": "error", "error": f"{type(exc).__name__}: {exc}"})


def _run_usda_source_with_timeout(
    *,
    source_key: str,
    ticker_root: Path,
    cache_root: Path,
    usda_timeout_seconds: float,
    usda_retry_attempts: int,
    source_timeout_seconds: float,
) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    out_queue = ctx.Queue()
    proc = ctx.Process(
        target=_usda_source_worker,
        args=(
            source_key,
            str(ticker_root),
            str(cache_root),
            float(usda_timeout_seconds),
            int(usda_retry_attempts),
            out_queue,
        ),
    )
    proc.start()
    proc.join(max(1.0, float(source_timeout_seconds)))
    if proc.is_alive():
        proc.terminate()
        proc.join(5.0)
        return {
            "source": source_key,
            "status": "timeout",
            "error": f"USDA source refresh exceeded {float(source_timeout_seconds):.0f}s",
        }
    try:
        return dict(out_queue.get_nowait())
    except queue.Empty:
        return {
            "source": source_key,
            "status": "error",
            "error": f"worker exited without summary (exitcode={proc.exitcode})",
        }


def refresh_gpre_daily_sources(
    *,
    repo_root: Path,
    as_of_date: date,
    usda_sources: Sequence[str] = _DEFAULT_USDA_SOURCES,
    corn_timeout_seconds: float = 20.0,
    usda_timeout_seconds: float = 12.0,
    usda_retry_attempts: int = 1,
    usda_source_timeout_seconds: float = 45.0,
) -> Dict[str, Any]:
    """Download GPRE corn-bids and latest USDA source files without export rebuilds."""

    repo = Path(repo_root).expanduser().resolve()
    effective = resolve_effective_data_root(repo)
    paths = resolve_stock_model_paths(repo, effective.data_root)
    paths.ensure_runtime_dirs("GPRE")
    ticker_root = paths.ticker_dir("GPRE")
    cache_root = paths.market_cache_dir

    corn_summary = download_gpre_corn_bids_snapshot(
        ticker_root,
        as_of_date=as_of_date,
        timeout_seconds=float(corn_timeout_seconds),
    )

    usda_summaries: List[Dict[str, Any]] = []
    for source in usda_sources:
        source_key = str(source or "").strip()
        if not source_key:
            continue
        usda_summaries.append(
            _run_usda_source_with_timeout(
                source_key=source_key,
                ticker_root=ticker_root,
                cache_root=cache_root,
                usda_timeout_seconds=float(usda_timeout_seconds),
                usda_retry_attempts=int(usda_retry_attempts),
                source_timeout_seconds=float(usda_source_timeout_seconds),
            )
        )

    return {
        "ticker": "GPRE",
        "as_of_date": as_of_date,
        "data_root": effective.data_root,
        "ticker_root": ticker_root,
        "market_cache_dir": cache_root,
        "corn_bids": corn_summary,
        "usda": usda_summaries,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Download GPRE corn-bids plus latest USDA source files without full market export rebuild.")
    ap.add_argument("--date", type=_parse_date, default=date.today(), help="Snapshot date in YYYY-MM-DD. Default: today.")
    ap.add_argument("--sources", default=",".join(_DEFAULT_USDA_SOURCES), help="Comma-separated USDA sources. Default: nwer,ams_3617,ams_3618.")
    ap.add_argument("--corn-timeout", type=float, default=20.0, help="Seconds for the GPRE corn-bids web fetch. Default: 20.")
    ap.add_argument("--usda-timeout", type=float, default=12.0, help="Seconds per USDA remote request. Default: 12.")
    ap.add_argument("--usda-retries", type=int, default=1, help="USDA remote attempts per request. Default: 1.")
    ap.add_argument("--usda-source-timeout", type=float, default=45.0, help="Hard seconds per USDA source process. Default: 45.")
    args = ap.parse_args()

    summary = refresh_gpre_daily_sources(
        repo_root=_project_root(),
        as_of_date=args.date,
        usda_sources=_source_list(args.sources),
        corn_timeout_seconds=float(args.corn_timeout),
        usda_timeout_seconds=float(args.usda_timeout),
        usda_retry_attempts=int(args.usda_retries),
        usda_source_timeout_seconds=float(args.usda_source_timeout),
    )

    corn = dict(summary.get("corn_bids") or {})
    print(
        "[gpre_daily] "
        f"ticker=GPRE date={summary['as_of_date'].isoformat()} "
        f"data_root={summary.get('data_root')} "
        f"corn_status={corn.get('status')} "
        f"corn_rows={corn.get('row_count')} "
        f"corn_locations={len(corn.get('locations_included') or [])} "
        f"corn_source={corn.get('source_url')} "
        f"raw={corn.get('archive_raw_path')} "
        f"parsed={corn.get('archive_parsed_path')}",
        flush=True,
    )
    for item in list(summary.get("usda") or []):
        latest = list(item.get("latest_entries") or [])
        latest_text = "; ".join(
            f"{entry.get('report_date')}:{entry.get('path')}"
            for entry in latest[:2]
        )
        print(
            "[gpre_daily] "
            f"usda_source={item.get('source')} "
            f"status={item.get('status')} "
            f"entries={item.get('entry_count', 0)} "
            f"latest={latest_text or item.get('error', '')}",
            flush=True,
        )


if __name__ == "__main__":
    main()
