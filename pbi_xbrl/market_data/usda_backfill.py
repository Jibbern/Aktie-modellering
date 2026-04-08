"""Helpers for month-by-month USDA archive backfill.

Normal `--refresh-market-data` handles the freshest NWER / AMS releases. This
module handles the slower historical archive path exposed by USDA's
`get_previous_release?...type=month` endpoint and then hands the downloaded
files back into the normal market-data sync/export flow.
"""
from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from ..cache_layout import bootstrap_canonical_ticker_cache, canonical_ticker_cache_root
from ..company_profiles import get_company_profile
from .providers import PROVIDERS
from .service import sync_market_cache


_USDA_ARCHIVE_SOURCES: Tuple[str, ...] = ("nwer", "ams_3617")
@dataclass(frozen=True)
class USDAProviderBackfillSummary:
    source: str
    local_dir: Path
    start_date: date
    end_date: date
    discovered_assets: int
    downloaded_files: int
    skipped_existing: int
    error_text: str = ""


@dataclass(frozen=True)
class USDABackfillSummary:
    ticker: str
    start_date: date
    end_date: date
    provider_summaries: Tuple[USDAProviderBackfillSummary, ...]
    market_sync_summary: Any | None = None


def _fetch_text_retry(provider: Any, url: str, *, extra_headers: Optional[Dict[str, str]] = None) -> str:
    return provider._fetch_text(url, extra_headers=extra_headers)


def _fetch_bytes_retry(provider: Any, url: str, *, extra_headers: Optional[Dict[str, str]] = None) -> bytes:
    return provider._fetch_bytes(url, extra_headers=extra_headers)


def _iter_year_months(start_date: date, end_date: date) -> Iterable[tuple[int, int]]:
    year = int(start_date.year)
    month = int(start_date.month)
    while (year, month) <= (int(end_date.year), int(end_date.month)):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def _previous_release_fragment_url(provider: Any) -> Optional[str]:
    landing_html = _fetch_text_retry(provider, provider.landing_page_url)
    fragment_urls = provider._release_fragment_urls(landing_html, provider.landing_page_url)
    for url in fragment_urls:
        if "get_previous_release" in str(url or ""):
            return str(url)
    return None


def _normalize_archive_doc(provider: Any, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(doc.get("file_extension") or "").strip().lower() != "pdf":
        return None
    doc_url = urllib.parse.urljoin(provider.landing_page_url, str(doc.get("document_url") or ""))
    if not doc_url:
        return None
    report_ts = provider._remote_date_from_text(
        doc_url,
        doc.get("report_date"),
        doc.get("document_date"),
        doc.get("report_end_date"),
    )
    if report_ts is None:
        return None
    return {
        "url": doc_url,
        "label": str(doc.get("title") or "Archived Report"),
        "asset_type": "pdf",
        "report_date": report_ts,
    }


def collect_archive_assets(
    provider: Any,
    start_date: date,
    end_date: date,
    *,
    cache_root: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Collect latest and month-archive USDA assets for one provider/date range."""
    assets: Dict[str, Dict[str, Any]] = {}
    archive_debug: Dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "latest_refresh_candidates": [],
        "previous_release_url": "",
        "month_fetches": [],
        "discovered_assets": [],
    }

    for item in list(provider.discover_remote_assets(as_of=end_date, cache_root=cache_root) or []):
        report_ts = provider._date_from_value(item.get("report_date"))
        if report_ts is None:
            continue
        if start_date <= report_ts.date() <= end_date:
            assets[str(item.get("url") or "")] = dict(item)
    archive_debug["latest_refresh_candidates"] = [provider._sanitize_candidate_debug(item) for item in assets.values()]

    previous_release_url = _previous_release_fragment_url(provider)
    archive_debug["previous_release_url"] = str(previous_release_url or "")
    if not previous_release_url:
        archive_debug["discovered_assets"] = [provider._sanitize_candidate_debug(item) for item in assets.values()]
        provider._write_remote_debug(cache_root, {"archive_backfill": archive_debug}, merge=True)
        return sorted(assets.values(), key=lambda item: str(item.get("report_date") or ""))

    headers = {
        "Accept": "application/json,text/html,*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": provider.landing_page_url,
    }
    for year, month in _iter_year_months(start_date, end_date):
        archive_url = f"{previous_release_url}?type=month&month={month}&year={year}"
        month_diag: Dict[str, Any] = {
            "url": archive_url,
            "year": year,
            "month": month,
            "status": "pending",
            "document_count": 0,
        }
        try:
            payload = _fetch_text_retry(provider, archive_url, extra_headers=headers)
            parsed = json.loads(payload)
            docs = list(parsed.get("data") or [])
            month_diag["status"] = "ok"
            month_diag["document_count"] = len(docs)
            for doc in docs:
                normalized = _normalize_archive_doc(provider, doc)
                if normalized is None:
                    continue
                report_ts = provider._date_from_value(normalized.get("report_date"))
                if report_ts is None:
                    continue
                if not (start_date <= report_ts.date() <= end_date):
                    continue
                assets[str(normalized.get("url") or "")] = normalized
        except Exception as exc:
            month_diag["status"] = "error"
            month_diag["error"] = f"{type(exc).__name__}: {exc}"
        archive_debug["month_fetches"].append(month_diag)
    selected_assets = sorted(
        assets.values(),
        key=lambda item: (
            str(item.get("report_date") or ""),
            str(item.get("url") or ""),
        ),
    )
    archive_debug["discovered_assets"] = [provider._sanitize_candidate_debug(item) for item in selected_assets]
    provider._write_remote_debug(cache_root, {"archive_backfill": archive_debug}, merge=True)
    return selected_assets


def download_archive_assets(
    provider: Any,
    ticker_root: Path,
    start_date: date,
    end_date: date,
    *,
    cache_root: Optional[Path] = None,
) -> USDAProviderBackfillSummary:
    """Download missing archive assets into the provider's ticker-local USDA folder."""
    local_dir = provider._local_dir(Path(ticker_root))
    discovered_assets = collect_archive_assets(provider, start_date, end_date, cache_root=cache_root)
    downloaded = 0
    skipped = 0
    download_attempts: List[Dict[str, Any]] = []
    for asset in discovered_assets:
        url = str(asset.get("url") or "").strip()
        asset_type = str(asset.get("asset_type") or "pdf").strip() or "pdf"
        report_ts = provider._date_from_value(asset.get("report_date"))
        if not url or report_ts is None:
            continue
        local_name = provider._stable_local_name(report_ts, asset_type, url)
        local_path = local_dir / local_name
        if local_path.exists():
            skipped += 1
            download_attempts.append(
                {
                    "url": url,
                    "status": "skipped",
                    "saved_local_path": str(local_path),
                    "report_date": report_ts,
                }
            )
            continue
        try:
            payload = _fetch_bytes_retry(provider, url)
            local_path.write_bytes(payload)
            downloaded += 1
            download_attempts.append(
                {
                    "url": url,
                    "status": "downloaded",
                    "saved_local_path": str(local_path),
                    "report_date": report_ts,
                    "bytes": len(payload),
                }
            )
        except Exception as exc:
            download_attempts.append(
                {
                    "url": url,
                    "status": "error",
                    "saved_local_path": str(local_path),
                    "report_date": report_ts,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            raise
    provider._write_remote_debug(
        cache_root,
        {
            "archive_backfill_downloads": {
                "start_date": start_date,
                "end_date": end_date,
                "download_attempts": download_attempts,
                "downloaded_files": downloaded,
                "skipped_existing": skipped,
            }
        },
        merge=True,
    )
    return USDAProviderBackfillSummary(
        source=str(provider.source),
        local_dir=local_dir,
        start_date=start_date,
        end_date=end_date,
        discovered_assets=len(discovered_assets),
        downloaded_files=downloaded,
        skipped_existing=skipped,
    )


def resolve_usda_sources(sources: Optional[Sequence[str]] = None) -> Tuple[str, ...]:
    requested = tuple(str(src or "").strip() for src in (sources or ()) if str(src or "").strip())
    if not requested:
        requested = _USDA_ARCHIVE_SOURCES
    valid = tuple(src for src in requested if src in PROVIDERS and src in _USDA_ARCHIVE_SOURCES)
    if not valid:
        raise ValueError("no supported USDA archive sources selected")
    return valid


def run_usda_archive_backfill(
    *,
    repo_root: Path,
    ticker: str,
    start_date: date,
    end_date: date,
    sources: Optional[Sequence[str]] = None,
    cache_dir: Optional[Path] = None,
    sync_cache: bool = True,
) -> USDABackfillSummary:
    """Backfill USDA archive PDFs into local folders and optionally sync/export them."""
    ticker_u = str(ticker or "").strip().upper()
    if not ticker_u:
        raise ValueError("ticker is required")
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    repo_root = Path(repo_root).expanduser().resolve()
    ticker_root = repo_root / ticker_u
    if not ticker_root.exists():
        ticker_root.mkdir(parents=True, exist_ok=True)
    resolved_cache_dir = Path(cache_dir).expanduser().resolve() if cache_dir is not None else canonical_ticker_cache_root(repo_root, ticker_u).resolve()

    selected_sources = resolve_usda_sources(sources)
    provider_summaries: List[USDAProviderBackfillSummary] = []
    for source in selected_sources:
        provider = PROVIDERS[source]
        try:
            provider_summaries.append(download_archive_assets(provider, ticker_root, start_date, end_date, cache_root=resolved_cache_dir))
        except Exception as exc:
            provider._write_remote_debug(
                resolved_cache_dir,
                {
                    "archive_backfill_error": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                },
                merge=True,
            )
            provider_summaries.append(
                USDAProviderBackfillSummary(
                    source=str(provider.source),
                    local_dir=provider._local_dir(Path(ticker_root)),
                    start_date=start_date,
                    end_date=end_date,
                    discovered_assets=0,
                    downloaded_files=0,
                    skipped_existing=0,
                    error_text=f"{type(exc).__name__}: {exc}",
                )
            )

    market_sync_summary = None
    if sync_cache:
        bootstrap_canonical_ticker_cache(repo_root, ticker_u)
        market_sync_summary = sync_market_cache(
            cache_dir=resolved_cache_dir,
            ticker=ticker_u,
            profile=get_company_profile(ticker_u),
            sync_raw=True,
            refresh=False,
            reparse=False,
        )

    return USDABackfillSummary(
        ticker=ticker_u,
        start_date=start_date,
        end_date=end_date,
        provider_summaries=tuple(provider_summaries),
        market_sync_summary=market_sync_summary,
    )
