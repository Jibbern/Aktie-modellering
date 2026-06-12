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
import re
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


def _coerce_provider_report_date(provider: Any, value: Any) -> Any:
    parser = getattr(provider, "_date_from_value", None)
    if callable(parser):
        return parser(value)
    return value


def _report_date_iso(value: Any) -> str:
    if value is None:
        return ""
    try:
        if hasattr(value, "date"):
            return value.date().isoformat()
    except Exception:
        pass
    text = str(value or "").strip()
    return text[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", text) else ""


def _provider_local_dir(provider: Any, ticker_root: Path) -> Path:
    local_dir = getattr(provider, "_local_dir", None)
    if callable(local_dir):
        return Path(local_dir(ticker_root))
    local_name = str(getattr(provider, "local_dir_name", "") or f"{getattr(provider, 'source', 'usda')}_pdfs").strip()
    out = Path(ticker_root) / local_name
    out.mkdir(parents=True, exist_ok=True)
    return out


def _asset_type_for(provider: Any, *, url: str, candidate: Dict[str, Any], path: Path | None = None) -> str:
    explicit = str(candidate.get("asset_type") or "").strip().lower()
    if explicit:
        return explicit
    typer = getattr(provider, "_asset_type_for_name", None)
    name = path.name if path is not None else url
    if callable(typer):
        return str(typer(name) or "data").strip().lower()
    suffix = Path(str(name or "")).suffix.lower()
    if suffix == ".pdf":
        return "pdf"
    if suffix == ".json":
        return "json"
    return "data"


def _stable_local_name(provider: Any, report_date_value: Any, asset_type: str, url: str) -> str:
    namer = getattr(provider, "_stable_local_name", None)
    if callable(namer):
        return str(namer(report_date_value, asset_type, url))
    report_date = _report_date_iso(report_date_value)
    prefix = str(getattr(provider, "stable_name_prefix", "") or getattr(provider, "source", "usda")).strip()
    suffix = ".json" if asset_type == "json" else ".pdf" if asset_type == "pdf" else Path(url).suffix or ".dat"
    stem = f"{prefix}_{report_date}"
    if asset_type == "json":
        stem += "_data"
    return f"{stem}{suffix}"


def _latest_local_usda_entries_fast(provider: Any, ticker_root: Path) -> List[Dict[str, Any]]:
    local_dir = _provider_local_dir(provider, ticker_root)
    source = str(getattr(provider, "source", "") or "").strip()
    prefix = str(getattr(provider, "stable_name_prefix", "") or source).strip()
    prefix_re = re.compile(rf"^{re.escape(prefix)}_(?P<date>\d{{4}}-\d{{2}}-\d{{2}})(?:_data)?\.[^.]+$", re.I)
    owns_local_asset = getattr(provider, "owns_local_asset", None)
    entries: List[Dict[str, Any]] = []
    if not local_dir.exists():
        return entries
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        if callable(owns_local_asset):
            try:
                if not bool(owns_local_asset(path)):
                    continue
            except Exception:
                continue
        match = prefix_re.match(path.name)
        if not match:
            continue
        asset_type = _asset_type_for(provider, url=str(path), candidate={}, path=path)
        entries.append(
            {
                "source": source,
                "source_id": path.stem,
                "report_date": match.group("date"),
                "publication_date": match.group("date"),
                "path": str(path.resolve()),
                "asset_type": asset_type,
                "asset_role": "primary_parse" if asset_type == "json" else "audit_provenance",
                "source_role": "primary_structured_json" if asset_type == "json" else "audit_pdf",
            }
        )
    entries.sort(key=lambda item: (str(item.get("report_date") or ""), str(item.get("path") or "")))
    return entries


def _refresh_usda_source_files_fast(
    provider: Any,
    *,
    ticker_root: Path,
    cache_root: Path,
    as_of_date: date,
) -> Dict[str, Any]:
    """Refresh latest USDA assets without the slow full local PDF normalization scan."""

    source = str(getattr(provider, "source", "") or "").strip()
    local_dir = _provider_local_dir(provider, ticker_root)
    attempts: List[Dict[str, Any]] = []
    try:
        remote_candidates = list(provider.discover_remote_assets(as_of=as_of_date, cache_root=cache_root))
    except Exception as exc:
        entries = _latest_local_usda_entries_fast(provider, ticker_root)
        return {
            "source": source,
            "status": "error" if not entries else "ok",
            "entry_count": len(entries),
            "latest_entries": _latest_entries(entries),
            "error": f"{type(exc).__name__}: {exc}",
        }

    for cand in remote_candidates:
        candidate = dict(cand or {})
        url = str(candidate.get("url") or "").strip()
        report_date_value = _coerce_provider_report_date(provider, candidate.get("report_date"))
        report_date_text = _report_date_iso(report_date_value)
        asset_type = _asset_type_for(provider, url=url, candidate=candidate)
        if not url or not report_date_text:
            continue
        local_path = local_dir / _stable_local_name(provider, report_date_value, asset_type, url)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        attempt: Dict[str, Any] = {
            "url": url,
            "asset_type": asset_type,
            "report_date": report_date_text,
            "saved_local_path": str(local_path),
        }
        if local_path.exists():
            attempt["status"] = "skipped"
            attempts.append(attempt)
            print(
                f"[market_data:{source}] asset={asset_type} date={report_date_text} status=skipped path={local_path}",
                flush=True,
            )
            continue
        try:
            payload = candidate.get("prefetched_payload")
            fetch_attempts: List[Dict[str, Any]] = []
            if not isinstance(payload, (bytes, bytearray)):
                payload, fetch_attempts = provider._fetch_bytes_diagnostic(url)
            local_path.write_bytes(bytes(payload))
            attempt["status"] = "updated"
            attempt["bytes"] = len(payload)
            attempt["fetch_attempts"] = list(fetch_attempts or [])
            print(
                f"[market_data:{source}] asset={asset_type} date={report_date_text} status=updated path={local_path}",
                flush=True,
            )
        except Exception as exc:
            attempt["status"] = "error"
            attempt["error"] = f"{type(exc).__name__}: {exc}"
            print(
                f"[market_data:{source}] asset={asset_type} date={report_date_text} status=failed path={local_path} error={type(exc).__name__}: {exc}",
                flush=True,
            )
        attempts.append(attempt)

    entries = _latest_local_usda_entries_fast(provider, ticker_root)
    successful_attempts = [item for item in attempts if str(item.get("status") or "") in {"updated", "skipped"}]
    failed_attempts = [item for item in attempts if str(item.get("status") or "") == "error"]
    status = "ok" if entries or successful_attempts else "error" if failed_attempts else "no_candidates"
    return {
        "source": source,
        "status": status,
        "entry_count": len(entries),
        "latest_entries": _latest_entries(entries),
        "download_attempts": attempts,
    }


def _usda_source_worker(
    source_key: str,
    ticker_root_text: str,
    cache_root_text: str,
    as_of_date_text: str,
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
        summary = _refresh_usda_source_files_fast(
            provider,
            ticker_root=Path(ticker_root_text),
            cache_root=Path(cache_root_text),
            as_of_date=date.fromisoformat(str(as_of_date_text)),
        )
        out_queue.put(summary)
    except Exception as exc:
        out_queue.put({"source": source_key, "status": "error", "error": f"{type(exc).__name__}: {exc}"})


def _run_usda_source_with_timeout(
    *,
    source_key: str,
    ticker_root: Path,
    cache_root: Path,
    as_of_date: date,
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
            as_of_date.isoformat(),
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
    usda_source_timeout_seconds: float = 90.0,
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
                as_of_date=as_of_date,
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
    ap.add_argument("--usda-source-timeout", type=float, default=90.0, help="Hard seconds per USDA source process. Default: 90.")
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
