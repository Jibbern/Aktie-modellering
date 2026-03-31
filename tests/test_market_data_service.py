from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil

import pandas as pd
import pytest

from pbi_xbrl.market_data.cache import ensure_market_cache_dirs
from pbi_xbrl.market_data.providers.ams_3617 import AMS3617Provider, parse_ams_3617_pdf_text
from pbi_xbrl.market_data.providers.nwer import NWERProvider, parse_nwer_pdf_text
import pbi_xbrl.market_data.service as market_service
from pbi_xbrl.market_data.service import (
    PARSED_SCHEMA_COLUMNS,
    _build_export_rows,
    _build_quarterly_rows,
    _dedupe_parsed_df,
    _standardize_parsed_df,
    build_current_qtd_simple_crush_snapshot,
    build_next_quarter_thesis_snapshot,
    sync_market_cache,
)


def _parsed_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "observation_date": "2025-06-15",
        "quarter": "2025-06-30",
        "aggregation_level": "observation",
        "publication_date": "2025-06-16",
        "source": "demo",
        "report_type": "provider_demo",
        "source_type": "provider_demo",
        "market_family": "corn_price",
        "series_key": "corn_cash_demo",
        "instrument": "Corn",
        "location": "Iowa",
        "region": "midwest",
        "tenor": "",
        "price_value": "4.10",
        "unit": "$/bushel",
        "quality": "high",
        "source_file": "demo.csv",
        "parsed_note": "demo",
        "origin": "provider_raw",
        "_priority": 40,
        "_obs_count": 1,
    }
    base.update(overrides)
    return base


def test_standardize_parsed_df_normalizes_schema_and_obs_count_alias() -> None:
    raw = pd.DataFrame(
        [
            {
                **_parsed_row(),
                "obs_count": "3",
                "_obs_count": None,
                "_priority": "50",
            }
        ]
    )

    out = _standardize_parsed_df(raw)

    assert list(out.columns) == PARSED_SCHEMA_COLUMNS
    assert out.loc[0, "_obs_count"] == 3
    assert out.loc[0, "_priority"] == 50
    assert str(out.loc[0, "quarter"].date()) == "2025-06-30"


def test_dedupe_parsed_df_keeps_highest_priority_and_latest_record() -> None:
    df = pd.DataFrame(
        [
            _parsed_row(_priority=20, publication_date="2025-06-10", price_value=4.00),
            _parsed_row(_priority=50, publication_date="2025-06-20", price_value=4.25),
        ]
    )

    out = _dedupe_parsed_df(_standardize_parsed_df(df))

    assert len(out) == 1
    assert out.loc[0, "price_value"] == 4.25
    assert out.loc[0, "_priority"] == 50


def test_build_quarterly_rows_preserves_dedupe_precedence_and_order() -> None:
    obs_df = pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    bootstrap_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(
                    aggregation_level="quarter_avg",
                    source="shared_source",
                    source_type="bootstrap_demo",
                    report_type="bootstrap_demo",
                    price_value=4.00,
                    _priority=10,
                )
            ]
        )
    )
    provider_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(
                    aggregation_level="quarter_avg",
                    source="shared_source",
                    source_type="provider_demo",
                    report_type="provider_demo",
                    price_value=4.35,
                    _priority=50,
                ),
                _parsed_row(
                    aggregation_level="quarter_end",
                    series_key="ethanol_demo",
                    market_family="ethanol_price",
                    instrument="Ethanol",
                    price_value=2.15,
                    _priority=45,
                ),
            ]
        )
    )

    out = _build_quarterly_rows(obs_df, bootstrap_df, provider_df)

    assert list(out["series_key"]) == ["corn_cash_demo", "ethanol_demo"]
    assert list(out["aggregation_level"]) == ["quarter_avg", "quarter_end"]
    assert out.loc[out["series_key"] == "corn_cash_demo", "price_value"].iloc[0] == 4.35


def test_build_export_rows_preserves_deterministic_sorting_and_fallback_source_type() -> None:
    quarterly_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(
                    aggregation_level="quarter_avg",
                    observation_date="2025-06-30",
                    publication_date="2025-07-01",
                    source_type=None,
                    report_type="provider_quarterly",
                )
            ]
        )
    )
    observations_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(observation_date="2025-06-10", publication_date="2025-06-11", series_key="ethanol_demo"),
                _parsed_row(observation_date="2025-06-05", publication_date="2025-06-06", series_key="corn_cash_demo"),
            ]
        )
    )

    out = _build_export_rows(quarterly_df, observations_df)

    assert list(out["series_key"]) == ["corn_cash_demo", "corn_cash_demo", "ethanol_demo"]
    assert list(out["aggregation_level"]) == ["observation", "quarter_avg", "observation"]
    assert out.loc[out["aggregation_level"] == "quarter_avg", "source_type"].iloc[0] == "provider_quarterly"


def test_parse_nwer_pdf_text_extracts_nebraska_ethanol_and_futures() -> None:
    text = """
National Weekly Ethanol Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News January 23, 2026
Future Settlements
CBOT Corn (¢/bu) 424.00 (Mar 26) 432.25 (May 26) 438.50 (Jul 26)
NYMEX Natural Gas ($/MMBtu) 5.0450 (Feb 26) 3.5780 (Mar 26) 3.5000 (May 26)
Ethanol Plant
State/Province/Region Sale Type Price ($ Per Gallon) Price Change Average Year Ago Freight Delivery
Illinois Trade 1.5800 UP 0.0500 1.5800 1.6450 FOB - R Current
Nebraska Trade 1.6000-1.6100 UNCH 1.6050 1.4700 FOB - T Current
"""

    rows = pd.DataFrame(parse_nwer_pdf_text(text, fallback_date=pd.Timestamp("2026-01-23"), source_file="nwer_2026-01-23.pdf"))

    assert not rows.empty
    assert float(rows.loc[rows["series_key"] == "ethanol_nebraska", "price_value"].iloc[0]) == 1.605
    assert float(rows.loc[rows["series_key"] == "cbot_corn_usd_per_bu", "price_value"].iloc[0]) == 4.24
    assert float(rows.loc[rows["series_key"] == "cbot_corn_may26_usd", "price_value"].iloc[0]) == 4.3225
    assert float(rows.loc[rows["series_key"] == "nymex_gas", "price_value"].iloc[0]) == 5.045
    assert float(rows.loc[rows["series_key"] == "nymex_gas_may26_usd", "price_value"].iloc[0]) == 3.5


def test_parse_ams_3617_pdf_text_extracts_daily_nebraska_corn_average() -> None:
    text = """
National Daily Ethanol Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News September 30, 2025
US #2 Yellow Corn -Bulk
Ethanol Plant
State/Province/Region Sale Type Basis (¢/bu) Basis Change Price ($/Bu) Price Change Average Year Ago Freight Delivery
Iowa East Bid -37.00Z to -20.00Z UP 3-UP 5 3.8450-4.0150 UP 0.0250-UP 0.0450 3.9035 4.1358 DLVD - T Current
Nebraska Bid -43.00Z to -10.00Z UNCH 3.7850-4.1150 DN 0.0050 3.9338 4.1646 DLVD - T Current
"""

    rows = pd.DataFrame(parse_ams_3617_pdf_text(text, fallback_date=pd.Timestamp("2025-09-30"), source_file="ams_3617_2025-09-30.pdf"))

    assert not rows.empty
    assert float(rows.loc[rows["series_key"] == "corn_nebraska", "price_value"].iloc[0]) == 3.9338
    assert float(rows.loc[rows["series_key"] == "corn_iowa_east", "price_value"].iloc[0]) == 3.9035


def test_nwer_sync_raw_discovers_current_quarter_pdf_and_data_assets(monkeypatch) -> None:
    provider = NWERProvider()
    landing_html = """
    <html><body>
      <section id="block-reportdocuments" class="documents clearfix"></section>
      <script>
        $(document).ready(function () {
            const slugId = 3616;
            $.ajax({ url: window.location.origin + '/get_latest_release/' + slugId });
            $.ajax({ url: window.location.origin + '/get_previous_release/' + slugId });
        })
      </script>
    </body></html>
    """
    latest_html = """
    {"html":"<a href=\"/filerepo/sites/default/files/3616/2026-01-23/1299792/ams_3616_00175.pdf\">Jan 23 PDF</a><a href=\"/filerepo/sites/default/files/3616/2026-01-23/1299792/ams_3616_00175.csv\">Jan 23 DATA</a>"}
    """
    previous_html = """
    {"html":"<a href=\"/filerepo/sites/default/files/3616/2026-01-09/1299701/ams_3616_00160.pdf\">Jan 9 PDF</a><a href=\"/filerepo/sites/default/files/3616/2026-01-09/1299701/ams_3616_00160.csv\">Jan 9 DATA</a><a href=\"/filerepo/sites/default/files/3616/2026-01-16/1299754/ams_3616_00168.pdf\">Jan 16 PDF</a><a href=\"/filerepo/sites/default/files/3616/2026-01-16/1299754/ams_3616_00168.csv\">Jan 16 DATA</a>"}
    """

    def _fake_fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del extra_headers
        if "get_latest_release" in url:
            return latest_html
        if "get_previous_release" in url:
            return previous_html
        return landing_html

    monkeypatch.setattr(provider, "_today", lambda: date(2026, 3, 28))
    monkeypatch.setattr(provider, "_fetch_text", _fake_fetch_text)
    monkeypatch.setattr(provider, "_fetch_bytes", lambda url: b"demo")

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_service"
    shutil.rmtree(tmp_path, ignore_errors=True)
    tmp_path.mkdir(parents=True, exist_ok=True)
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    ticker_root = tmp_path / "GPRE"

    try:
        result = provider.sync_raw(cache_root, ticker_root, refresh=True)

        assert result["raw_added"] >= 6
        assert (ticker_root / "USDA_weekly_data" / "nwer_2026-01-23.pdf").exists()
        assert (ticker_root / "USDA_weekly_data" / "nwer_2026-01-23_data.csv").exists()
        assert any(str(entry.get("asset_type") or "") == "pdf" for entry in result["entries"])
        assert any(str(entry.get("asset_type") or "") == "data" for entry in result["entries"])
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_nwer_sync_raw_discovers_current_quarter_pdf_and_data_assets_from_legacy_documents_page(monkeypatch) -> None:
    provider = NWERProvider()
    landing_html = """
    <html><body>
      <a href="/filerepo/reports/AMS_3616_documents">Report Documents</a>
    </body></html>
    """
    docs_html = """
    <html><body>
      <a href="/filerepo/sites/default/files/3616/2026-01-09/1299701/ams_3616_00160.pdf">Jan 9 PDF</a>
      <a href="/filerepo/sites/default/files/3616/2026-01-09/1299701/ams_3616_00160.csv">Jan 9 DATA</a>
      <a href="/filerepo/sites/default/files/3616/2026-01-16/1299754/ams_3616_00168.pdf">Jan 16 PDF</a>
      <a href="/filerepo/sites/default/files/3616/2026-01-16/1299754/ams_3616_00168.csv">Jan 16 DATA</a>
      <a href="/filerepo/sites/default/files/3616/2026-01-23/1299792/ams_3616_00175.pdf">Jan 23 PDF</a>
      <a href="/filerepo/sites/default/files/3616/2026-01-23/1299792/ams_3616_00175.csv">Jan 23 DATA</a>
    </body></html>
    """

    def _fake_fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del extra_headers
        return docs_html if "documents" in url else landing_html

    monkeypatch.setattr(provider, "_today", lambda: date(2026, 3, 28))
    monkeypatch.setattr(provider, "_fetch_text", _fake_fetch_text)
    monkeypatch.setattr(provider, "_fetch_bytes", lambda url: b"demo")

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_service"
    shutil.rmtree(tmp_path, ignore_errors=True)
    tmp_path.mkdir(parents=True, exist_ok=True)
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    ticker_root = tmp_path / "GPRE"

    try:
        result = provider.sync_raw(cache_root, ticker_root, refresh=True)

        assert result["raw_added"] >= 6
        assert (ticker_root / "USDA_weekly_data" / "nwer_2026-01-23.pdf").exists()
        assert (ticker_root / "USDA_weekly_data" / "nwer_2026-01-23_data.csv").exists()
        assert any(str(entry.get("asset_type") or "") == "pdf" for entry in result["entries"])
        assert any(str(entry.get("asset_type") or "") == "data" for entry in result["entries"])
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_bootstrap_specs_can_read_usda_weekly_and_daily_folders() -> None:
    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_bootstrap"
    shutil.rmtree(tmp_path, ignore_errors=True)
    ticker_root = tmp_path / "GPRE"
    weekly_dir = ticker_root / "USDA_weekly_data"
    daily_dir = ticker_root / "USDA_daily_data"
    weekly_dir.mkdir(parents=True, exist_ok=True)
    daily_dir.mkdir(parents=True, exist_ok=True)
    (weekly_dir / "nwer_weekly.csv").write_text(
        "week_end,ethanol_nebraska,source_pdf\n2026-01-23,1.61,nwer_2026-01-23.pdf\n",
        encoding="utf-8",
    )
    (daily_dir / "ams_3617_daily_corn.csv").write_text(
        "report_date,corn_nebraska,source_pdf\n2026-01-23,4.12,ams_3617_2026-01-23.pdf\n",
        encoding="utf-8",
    )

    try:
        nwer_df, nwer_fp = market_service._bootstrap_rows_for_source("nwer", ticker_root)
        ams_df, ams_fp = market_service._bootstrap_rows_for_source("ams_3617", ticker_root)

        assert nwer_fp != "none"
        assert ams_fp != "none"
        assert float(nwer_df.loc[nwer_df["series_key"] == "ethanol_nebraska", "price_value"].iloc[0]) == 1.61
        assert float(ams_df.loc[ams_df["series_key"] == "corn_nebraska", "price_value"].iloc[0]) == 4.12
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_sync_market_cache_rebuilds_raw_manifest_from_existing_raw_files(monkeypatch, tmp_path: Path) -> None:
    provider = NWERProvider()
    cache_dir = tmp_path / "sec_cache" / "GPRE"
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    raw_dir = cache_root / "raw" / "nwer" / "2026"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / "nwer_2026-01-23.pdf"
    raw_file.write_bytes(b"%PDF-1.4 demo raw file")
    (cache_root / "index" / "raw_manifest.json").write_text('{"nwer":[]}', encoding="utf-8")
    (cache_root / "index" / "parsed_manifest.json").write_text("{}", encoding="utf-8")

    def _fake_parse_raw_to_rows(cache_root_in: Path, ticker_root_in: Path, raw_entries: list[dict[str, object]]) -> pd.DataFrame:
        del cache_root_in, ticker_root_in
        assert len(raw_entries) == 1
        assert str(raw_entries[0].get("local_path") or "").endswith("nwer_2026-01-23.pdf")
        return pd.DataFrame(
            [
                _parsed_row(
                    source="nwer",
                    report_type="nwer_pdf",
                    source_type="nwer_pdf",
                    market_family="ethanol_price",
                    series_key="ethanol_nebraska",
                    instrument="Ethanol",
                    location="Nebraska",
                    region="nebraska",
                    source_file="nwer_2026-01-23.pdf",
                    observation_date="2026-01-23",
                    publication_date="2026-01-23",
                    quarter="2026-03-31",
                    price_value=1.61,
                )
            ]
        )

    monkeypatch.setitem(market_service.PROVIDERS, "nwer", provider)
    monkeypatch.setattr(provider, "parse_raw_to_rows", _fake_parse_raw_to_rows)

    class _Profile:
        enabled_market_sources = ("nwer",)

    summary = sync_market_cache(cache_dir, "GPRE", profile=_Profile(), reparse=True)

    assert summary.export_rows == 3
    rebuilt_raw_manifest = (cache_root / "index" / "raw_manifest.json").read_text(encoding="utf-8")
    assert "nwer_2026-01-23.pdf" in rebuilt_raw_manifest
    export_df = pd.read_parquet(cache_root / "parsed" / "exports" / "GPRE.parquet")
    assert not export_df.empty
    assert set(export_df["series_key"]) == {"ethanol_nebraska"}


def test_qtd_simple_crush_snapshot_uses_weekly_components_then_averages() -> None:
    rows = [
        {"observation_date": date(2026, 1, 5), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.00, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 6), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.10, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 7), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.20, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 8), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.10, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 9), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.00, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 12), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.20, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 13), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.30, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 14), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.40, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 15), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.30, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 16), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.20, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 9), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.60, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 16), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.65, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 9), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.40, "contract_tenor": "front", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 16), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.10, "contract_tenor": "front", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 23), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "cbot_corn_may26_usd", "price_value": 4.32, "contract_tenor": "may26", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 23), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "cbot_corn_jul26_usd", "price_value": 4.41, "contract_tenor": "jul26", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 23), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas_may26_usd", "price_value": 3.50, "contract_tenor": "may26", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 23), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas_jun26_usd", "price_value": 3.62, "contract_tenor": "jun26", "source_type": "nwer_pdf"},
    ]

    current = build_current_qtd_simple_crush_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=27000.0,
        as_of_date=date(2026, 3, 28),
    )
    thesis = build_next_quarter_thesis_snapshot(rows, as_of_date=date(2026, 3, 28))

    assert current["weeks_included"] == 2
    assert current["as_of"] == date(2026, 1, 16)
    assert current["current_market"]["corn_price"] == pytest.approx(4.18, abs=0.0001)
    assert current["current_market"]["ethanol_price"] == pytest.approx(1.625, abs=0.0001)
    assert current["current_market"]["natural_gas_price"] == pytest.approx(3.25, abs=0.0001)
    assert current["current_process"]["simple_crush"] == pytest.approx(
        ((2.9 * 1.60) - 4.08 - ((27000.0 / 1_000_000.0) * 2.9 * 3.40)
         + (2.9 * 1.65) - 4.28 - ((27000.0 / 1_000_000.0) * 2.9 * 3.10)) / 2.0,
        abs=0.0001,
    )
    assert thesis["corn"]["contract_tenor"] == "may26"
    assert thesis["natural_gas"]["contract_tenor"] == "may26"
