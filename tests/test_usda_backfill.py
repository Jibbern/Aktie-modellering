from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

from pbi_xbrl.market_data.providers.nwer import NWERProvider
from pbi_xbrl.market_data.usda_backfill import (
    collect_archive_assets,
    download_archive_assets,
    resolve_usda_sources,
    run_usda_archive_backfill,
)


def test_collect_archive_assets_merges_latest_and_month_archive(monkeypatch) -> None:
    provider = NWERProvider()
    landing_html = """
    <html><body>
      <script>
        const slugId = 3616;
      </script>
    </body></html>
    """
    month_payload = json.dumps(
        {
            "data": [
                {
                    "title": "National Weekly Ethanol Report",
                    "document_date": "02/20/2026 13:53:01",
                    "file_extension": "PDF",
                    "document_url": "/filerepo/sites/default/files/3616/2026-02-16/1305786/ams_3616_00179.pdf",
                    "report_date": "02/16/2026 - Fri, 02/20/2026",
                    "slug_id": "3616",
                    "report_end_date": "Fri, 02/20/2026 - 00:00",
                },
                {
                    "title": "National Weekly Ethanol Report",
                    "document_date": "02/27/2026 12:38:07",
                    "file_extension": "PDF",
                    "document_url": "/filerepo/sites/default/files/3616/2026-02-23/1307312/ams_3616_00180.pdf",
                    "report_date": "02/23/2026 - Fri, 02/27/2026",
                    "slug_id": "3616",
                    "report_end_date": "Fri, 02/27/2026 - 00:00",
                },
            ]
        }
    )

    def _fake_fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del extra_headers
        if "type=month" in url:
            return month_payload
        return landing_html

    monkeypatch.setattr(provider, "_fetch_text", _fake_fetch_text)
    monkeypatch.setattr(
        provider,
        "discover_remote_assets",
        lambda as_of=None: [
            {
                "url": "https://mymarketnews.ams.usda.gov/filerepo/sites/default/files/3616/2026-03-23/1313671/ams_3616_00184.pdf",
                "label": "Latest Report",
                "asset_type": "pdf",
                "report_date": "2026-03-23",
            }
        ],
    )

    assets = collect_archive_assets(provider, date(2026, 2, 1), date(2026, 3, 23))

    assert [str(item.get("report_date"))[:10] for item in assets] == [
        "2026-02-16",
        "2026-02-23",
        "2026-03-23",
    ]


def test_download_archive_assets_writes_stable_files_and_skips_existing(monkeypatch) -> None:
    provider = NWERProvider()
    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_usda_backfill_download"
    shutil.rmtree(tmp_path, ignore_errors=True)
    ticker_root = tmp_path / "GPRE"
    local_dir = ticker_root / "USDA_weekly_data"
    local_dir.mkdir(parents=True, exist_ok=True)
    (local_dir / "nwer_2026-02-16.pdf").write_bytes(b"existing")

    monkeypatch.setattr(
        "pbi_xbrl.market_data.usda_backfill.collect_archive_assets",
        lambda provider_in, start_date, end_date: [
            {
                "url": "https://mymarketnews.ams.usda.gov/filerepo/sites/default/files/3616/2026-02-16/1305786/ams_3616_00179.pdf",
                "label": "Archived Report",
                "asset_type": "pdf",
                "report_date": "2026-02-16",
            },
            {
                "url": "https://mymarketnews.ams.usda.gov/filerepo/sites/default/files/3616/2026-02-23/1307312/ams_3616_00180.pdf",
                "label": "Archived Report",
                "asset_type": "pdf",
                "report_date": "2026-02-23",
            },
        ],
    )
    monkeypatch.setattr("pbi_xbrl.market_data.usda_backfill._fetch_bytes_retry", lambda provider_in, url, extra_headers=None: b"demo-pdf")

    try:
        summary = download_archive_assets(provider, ticker_root, date(2026, 2, 16), date(2026, 2, 23))
        assert summary.discovered_assets == 2
        assert summary.downloaded_files == 1
        assert summary.skipped_existing == 1
        assert (local_dir / "nwer_2026-02-23.pdf").exists()
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_run_usda_archive_backfill_can_skip_market_sync(monkeypatch) -> None:
    tmp_repo = Path(__file__).resolve().parents[1] / "tests" / "_tmp_usda_backfill_run"
    shutil.rmtree(tmp_repo, ignore_errors=True)
    (tmp_repo / "GPRE").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "pbi_xbrl.market_data.usda_backfill.download_archive_assets",
        lambda provider, ticker_root, start_date, end_date: type("Summary", (), {
            "source": provider.source,
            "local_dir": ticker_root / ("USDA_weekly_data" if provider.source == "nwer" else "USDA_daily_data"),
            "start_date": start_date,
            "end_date": end_date,
            "discovered_assets": 1,
            "downloaded_files": 1,
            "skipped_existing": 0,
        })(),
    )

    try:
        summary = run_usda_archive_backfill(
            repo_root=tmp_repo,
            ticker="GPRE",
            start_date=date(2026, 1, 23),
            end_date=date(2026, 3, 31),
            sources=("nwer", "ams_3617"),
            sync_cache=False,
        )
        assert summary.ticker == "GPRE"
        assert summary.market_sync_summary is None
        assert tuple(item.source for item in summary.provider_summaries) == ("nwer", "ams_3617")
    finally:
        shutil.rmtree(tmp_repo, ignore_errors=True)


def test_resolve_usda_sources_rejects_invalid_only_selection() -> None:
    try:
        resolve_usda_sources(("unknown_source",))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unsupported USDA sources")
