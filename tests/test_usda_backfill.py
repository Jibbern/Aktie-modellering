from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from pbi_xbrl.market_data.providers.base import BaseMarketProvider, report_period_end_date_from_text
from pbi_xbrl.market_data.providers.ams_3617 import AMS3617Provider
from pbi_xbrl.market_data.providers.nwer import NWERProvider
from pbi_xbrl.market_data.providers.ams_3618 import AMS3618Provider, _ams3618_report_date
from pbi_xbrl.market_data.usda_backfill import (
    collect_archive_assets,
    download_archive_assets,
    resolve_usda_sources,
    run_usda_archive_backfill,
)


def test_usda_report_period_end_date_wins_over_publication_header() -> None:
    text = (
        "National Weekly Grain Co-Products Report\n"
        "Livestock, Poultry and Grain Market News May 4, 2026\n"
        "Report for 4/27/2026 - 5/1/2026\n"
    )

    assert report_period_end_date_from_text(text) == date(2026, 5, 1)
    assert _ams3618_report_date(text, fallback=None) == date(2026, 5, 1)


def test_usda_normalize_removes_begin_date_duplicate_when_end_date_copy_exists() -> None:
    class DemoUsdaProvider(BaseMarketProvider):
        source = "demo_usda"
        stable_name_prefix = "demo_usda"
        local_dir_name = "USDA_demo"
        local_patterns = ("USDA_demo/*",)

        def owns_local_asset(self, path: Path) -> bool:
            return path.name.startswith("demo_usda_")

        def infer_pdf_report_date_from_content(self, path: Path) -> pd.Timestamp | None:
            if path.name in {"demo_usda_2026-04-27.pdf", "demo_usda_2026-05-01.pdf"}:
                return pd.Timestamp("2026-05-01")
            return None

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_usda_normalize"
    shutil.rmtree(tmp_path, ignore_errors=True)
    local_dir = tmp_path / "GPRE" / "USDA_demo"
    local_dir.mkdir(parents=True, exist_ok=True)
    begin_named = local_dir / "demo_usda_2026-04-27.pdf"
    end_named = local_dir / "demo_usda_2026-05-01.pdf"
    begin_named.write_bytes(b"same-pdf")
    end_named.write_bytes(b"same-pdf")

    try:
        actions = DemoUsdaProvider().normalize_local_filenames(tmp_path / "GPRE")

        assert not begin_named.exists()
        assert end_named.exists()
        assert any(item.get("status") == "removed_duplicate" for item in actions)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_usda_public_data_latest_refresh_can_skip_legacy_view_report(monkeypatch, tmp_path) -> None:
    class DemoDirectFirstProvider(BaseMarketProvider):
        source = "demo_public_data"
        landing_page_url = "https://example.test/viewReport/999"
        public_data_url = "https://example.test/public_data?slug_id=999"
        public_data_slug_id = "999"
        public_data_latest_refresh_sufficient = True

    provider = DemoDirectFirstProvider()
    calls: list[str] = []

    def _fake_fetch_text_diagnostic(url: str, *, extra_headers: dict[str, str] | None = None):
        del extra_headers
        calls.append(url)
        if "viewReport" in url:
            raise AssertionError("legacy viewReport discovery should not run after public_data succeeds")
        payload = {
            "reportBeginDates": ["05/12/2026"],
            "reportEndDates": ["05/12/2026"],
        }
        return json.dumps(payload), [{"status": "ok"}]

    def _fake_fetch_bytes_diagnostic(url: str, *, extra_headers: dict[str, str] | None = None):
        del extra_headers
        calls.append(url)
        payload = {"results": [{"report_date": "05/12/2026", "value": 123}]}
        return json.dumps(payload).encode("utf-8"), [{"status": "ok"}]

    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _fake_fetch_text_diagnostic)
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _fake_fetch_bytes_diagnostic)

    candidates = provider.discover_remote_assets(as_of=date(2026, 5, 12), cache_root=tmp_path)

    assert len(candidates) == 1
    assert candidates[0]["asset_type"] == "json"
    assert "viewReport" not in " ".join(calls)


def test_usda_latest_refresh_can_pair_public_data_json_with_pdf_audit_companion(monkeypatch, tmp_path) -> None:
    class DemoUsdaProvider(BaseMarketProvider):
        source = "demo_usda"
        stable_name_prefix = "demo_usda"
        local_dir_name = "USDA_demo"
        local_patterns = ("USDA_demo/*",)
        landing_page_url = "https://example.test/viewReport/999"
        public_data_url = "https://example.test/public_data?slug_id=999"
        public_data_slug_id = "999"
        report_token = "/999/"
        public_data_latest_refresh_sufficient = True
        download_audit_pdf_companion = True

    provider = DemoUsdaProvider()
    calls: list[str] = []

    def _fake_fetch_text_diagnostic(url: str, *, extra_headers: dict[str, str] | None = None):
        del extra_headers
        calls.append(url)
        if "ajax-get-conditions" in url:
            payload = {
                "reportBeginDates": ["05/04/2026"],
                "reportEndDates": ["05/08/2026"],
            }
            return json.dumps(payload), [{"status": "ok"}]
        if "viewReport" in url:
            return (
                '<html><body><a href="/filerepo/sites/default/files/999/2026-05-04/123/demo.pdf">'
                "Demo USDA PDF</a></body></html>"
            ), [{"status": "ok"}]
        raise AssertionError(f"unexpected text fetch: {url}")

    def _fake_fetch_bytes_diagnostic(url: str, *, extra_headers: dict[str, str] | None = None):
        del extra_headers
        calls.append(url)
        assert "ajax-search-data" in url
        payload = {"results": [{"report_begin_date": "05/04/2026", "report_end_date": "05/08/2026", "value": 123}]}
        return json.dumps(payload).encode("utf-8"), [{"status": "ok"}]

    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _fake_fetch_text_diagnostic)
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _fake_fetch_bytes_diagnostic)

    candidates = provider.discover_remote_assets(as_of=date(2026, 5, 8), cache_root=tmp_path)
    by_type = {str(item.get("asset_type")): item for item in candidates}

    assert set(by_type) == {"json", "pdf"}
    assert by_type["json"]["source_role"] == "primary_structured_json"
    assert by_type["json"]["asset_role"] == "primary_parse"
    assert by_type["pdf"]["source_role"] == "audit_pdf"
    assert by_type["pdf"]["asset_role"] == "audit_provenance"
    assert str(by_type["pdf"]["report_date"])[:10] == "2026-05-08"
    assert by_type["pdf"]["date_alignment_note"] == "pdf_url_begin_date_aligned_to_public_data_report_end"
    assert any("viewReport" in url for url in calls)


def test_usda_refresh_downloads_public_data_json_primary_and_pdf_audit_files(monkeypatch, tmp_path) -> None:
    class DemoUsdaProvider(BaseMarketProvider):
        source = "demo_usda"
        stable_name_prefix = "demo_usda"
        local_dir_name = "USDA_demo"
        local_patterns = ("USDA_demo/*",)

    provider = DemoUsdaProvider()
    ticker_root = tmp_path / "GPRE"
    ticker_root.mkdir(parents=True, exist_ok=True)
    candidates = [
        {
            "url": "https://example.test/public_data/demo",
            "label": "USDA public_data Report Detail 2026-05-04 to 2026-05-08",
            "asset_type": "json",
            "report_date": pd.Timestamp("2026-05-08"),
            "source_role": "primary_structured_json",
            "asset_role": "primary_parse",
            "prefetched_payload": b'{"results":[{"report_end_date":"05/08/2026"}]}',
        },
        {
            "url": "https://example.test/filerepo/sites/default/files/999/2026-05-04/demo.pdf",
            "label": "Demo USDA PDF",
            "asset_type": "pdf",
            "report_date": pd.Timestamp("2026-05-08"),
            "source_role": "audit_pdf",
            "asset_role": "audit_provenance",
            "date_alignment_note": "pdf_url_begin_date_aligned_to_public_data_report_end",
        },
    ]

    monkeypatch.setattr(provider, "discover_remote_assets", lambda as_of=None, cache_root=None: candidates)
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", lambda url, extra_headers=None: (b"%PDF-1.4 demo", [{"status": "ok"}]))

    discovered = provider.discover_available(ticker_root, refresh=True, cache_root=tmp_path)
    by_name = {Path(item["path"]).name: item for item in discovered}

    assert (ticker_root / "USDA_demo" / "demo_usda_2026-05-08_data.json").exists()
    assert (ticker_root / "USDA_demo" / "demo_usda_2026-05-08.pdf").exists()
    assert by_name["demo_usda_2026-05-08_data.json"]["asset_role"] == "primary_parse"
    assert by_name["demo_usda_2026-05-08_data.json"]["source_role"] == "primary_structured_json"
    assert by_name["demo_usda_2026-05-08.pdf"]["asset_role"] == "audit_provenance"
    assert by_name["demo_usda_2026-05-08.pdf"]["source_role"] == "audit_pdf"


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
        lambda as_of=None, cache_root=None: [
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
        "2026-02-20",
        "2026-02-27",
        "2026-03-23",
    ]


def test_collect_archive_assets_prefers_report_end_date_over_url_folder_date(monkeypatch) -> None:
    provider = NWERProvider()
    landing_html = """
    <html><body>
      <script>const slugId = 3616;</script>
    </body></html>
    """
    month_payload = json.dumps(
        {
            "data": [
                {
                    "title": "National Weekly Ethanol Report",
                    "document_date": "04/24/2026 13:53:01",
                    "file_extension": "PDF",
                    "document_url": "/filerepo/sites/default/files/3616/2026-04-20/1319930/ams_3616_00188_01.pdf",
                    "report_date": "04/20/2026 - Fri, 04/24/2026",
                    "slug_id": "3616",
                    "report_end_date": "Fri, 04/24/2026 - 00:00",
                }
            ]
        }
    )

    def _fake_fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del extra_headers
        if "type=month" in url:
            return month_payload
        return landing_html

    monkeypatch.setattr(provider, "_fetch_text", _fake_fetch_text)
    monkeypatch.setattr(provider, "discover_remote_assets", lambda as_of=None, cache_root=None: [])

    assets = collect_archive_assets(provider, date(2026, 4, 24), date(2026, 4, 24))

    assert len(assets) == 1
    assert str(assets[0].get("report_date"))[:10] == "2026-04-24"


def test_collect_archive_assets_includes_public_data_json_and_pdf_when_available(monkeypatch) -> None:
    class DemoUsdaProvider(BaseMarketProvider):
        source = "demo_usda"
        stable_name_prefix = "demo_usda"
        local_dir_name = "USDA_demo"
        landing_page_url = "https://example.test/viewReport/999"
        public_data_url = "https://example.test/public_data?slug_id=999"
        public_data_slug_id = "999"
        report_token = "/999/"
        public_data_sections = ("Report Detail",)

    provider = DemoUsdaProvider()
    month_payload = json.dumps(
        {
            "data": [
                {
                    "title": "Demo USDA Report",
                    "document_date": "05/08/2026 13:53:01",
                    "file_extension": "PDF",
                    "document_url": "/filerepo/sites/default/files/999/2026-05-04/123/demo.pdf",
                    "report_date": "05/04/2026 - Fri, 05/08/2026",
                    "slug_id": "999",
                    "report_end_date": "Fri, 05/08/2026 - 00:00",
                }
            ]
        }
    )

    monkeypatch.setattr("pbi_xbrl.market_data.usda_backfill._previous_release_fragment_url", lambda provider_in: "https://example.test/get_previous_release/999")

    def _fake_fetch_text(provider_in, url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del provider_in, extra_headers
        if "ajax-get-conditions" in url:
            return json.dumps({"reportBeginDates": ["05/04/2026"], "reportEndDates": ["05/08/2026"]})
        if "type=month" in url:
            return month_payload
        raise AssertionError(f"unexpected fetch: {url}")

    monkeypatch.setattr("pbi_xbrl.market_data.usda_backfill._fetch_text_retry", _fake_fetch_text)
    monkeypatch.setattr(provider, "discover_remote_assets", lambda as_of=None, cache_root=None: [])

    assets = collect_archive_assets(provider, date(2026, 5, 8), date(2026, 5, 8))
    by_type = {str(item.get("asset_type")): item for item in assets}

    assert set(by_type) == {"json", "pdf"}
    assert by_type["json"]["source_role"] == "primary_structured_json"
    assert by_type["json"]["asset_role"] == "primary_parse"
    assert by_type["pdf"]["source_role"] == "audit_pdf"
    assert by_type["pdf"]["asset_role"] == "audit_provenance"
    assert str(by_type["json"]["report_date"])[:10] == "2026-05-08"
    assert str(by_type["pdf"]["report_date"])[:10] == "2026-05-08"


def test_download_archive_assets_writes_stable_files_and_skips_existing(monkeypatch) -> None:
    provider = NWERProvider()
    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_usda_backfill_download"
    shutil.rmtree(tmp_path, ignore_errors=True)
    ticker_root = tmp_path / "GPRE"
    local_dir = ticker_root / provider.local_dir_name
    local_dir.mkdir(parents=True, exist_ok=True)
    (local_dir / "nwer_2026-02-16.pdf").write_bytes(b"existing")

    monkeypatch.setattr(
        "pbi_xbrl.market_data.usda_backfill.collect_archive_assets",
        lambda provider_in, start_date, end_date, cache_root=None: [
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
    providers = {
        "nwer": NWERProvider(),
        "ams_3617": AMS3617Provider(),
        "ams_3618": AMS3618Provider(),
    }

    monkeypatch.setattr(
        "pbi_xbrl.market_data.usda_backfill.download_archive_assets",
        lambda provider, ticker_root, start_date, end_date, cache_root=None: type("Summary", (), {
            "source": provider.source,
            "local_dir": ticker_root / str(getattr(provider, "local_dir_name", f"{provider.source}_pdfs")),
            "start_date": start_date,
            "end_date": end_date,
            "discovered_assets": 1,
            "downloaded_files": 1,
            "skipped_existing": 0,
        })(),
    )
    monkeypatch.setattr("pbi_xbrl.market_data.usda_backfill.PROVIDERS", providers)

    try:
        summary = run_usda_archive_backfill(
            repo_root=tmp_repo,
            ticker="GPRE",
            start_date=date(2026, 1, 23),
            end_date=date(2026, 3, 31),
            sources=("nwer", "ams_3617", "ams_3618"),
            sync_cache=False,
        )
        assert summary.ticker == "GPRE"
        assert summary.market_sync_summary is None
        assert tuple(item.source for item in summary.provider_summaries) == ("nwer", "ams_3617", "ams_3618")
    finally:
        shutil.rmtree(tmp_repo, ignore_errors=True)


def test_resolve_usda_sources_rejects_invalid_only_selection() -> None:
    try:
        resolve_usda_sources(("unknown_source",))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unsupported USDA sources")
