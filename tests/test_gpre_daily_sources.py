from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd

import gpre_daily_sources as daily


class _FakeDailyUSDAProvider:
    source = "ams_3617"
    stable_name_prefix = "ams_3617"
    local_dir_name = "USDA_daily_data"

    def __init__(self) -> None:
        self.fetch_calls: list[str] = []

    def _local_dir(self, ticker_root: Path) -> Path:
        out = ticker_root / self.local_dir_name
        out.mkdir(parents=True, exist_ok=True)
        return out

    def discover_remote_assets(self, *, as_of: date, cache_root: Path) -> list[dict[str, object]]:
        assert as_of == date(2026, 6, 12)
        assert cache_root.name == "market_data"
        return [
            {
                "url": "https://example.test/ams-3617.json",
                "asset_type": "json",
                "report_date": pd.Timestamp("2026-06-12"),
                "prefetched_payload": b'{"results": []}',
            },
            {
                "url": "https://example.test/ams-3617.pdf",
                "asset_type": "pdf",
                "report_date": pd.Timestamp("2026-06-12"),
            },
        ]

    def _date_from_value(self, value: object) -> pd.Timestamp | None:
        ts = pd.to_datetime(value, errors="coerce")
        return None if pd.isna(ts) else pd.Timestamp(ts)

    def _asset_type_for_name(self, name: str) -> str:
        return Path(name).suffix.lower().lstrip(".") or "data"

    def _stable_local_name(self, report_date: pd.Timestamp, asset_type: str, url: str) -> str:
        del url
        suffix = ".json" if asset_type == "json" else ".pdf"
        stem = f"{self.stable_name_prefix}_{report_date.date().isoformat()}"
        if asset_type == "json":
            stem += "_data"
        return f"{stem}{suffix}"

    def _fetch_bytes_diagnostic(self, url: str) -> tuple[bytes, list[dict[str, object]]]:
        self.fetch_calls.append(url)
        return b"%PDF fixture", [{"status": "ok", "url": url, "bytes": 12}]

    def owns_local_asset(self, path: Path) -> bool:
        return path.name.startswith(f"{self.stable_name_prefix}_")

    def infer_local_report_date(self, path: Path) -> pd.Timestamp | None:
        parts = path.stem.split("_")
        date_text = "_".join(parts[2:5]) if path.stem.endswith("_data") else "_".join(parts[2:5])
        return pd.Timestamp(date.fromisoformat(date_text))


def test_fast_usda_refresh_downloads_stable_files_without_full_local_discovery(tmp_path: Path) -> None:
    provider = _FakeDailyUSDAProvider()
    ticker_root = tmp_path / "tickers" / "GPRE"
    cache_root = tmp_path / "sec_cache" / "market_data"

    summary = daily._refresh_usda_source_files_fast(
        provider,
        ticker_root=ticker_root,
        cache_root=cache_root,
        as_of_date=date(2026, 6, 12),
    )

    assert summary["source"] == "ams_3617"
    assert summary["status"] == "ok"
    assert summary["entry_count"] == 2
    assert {entry["asset_type"] for entry in summary["latest_entries"]} == {"pdf", "json"}
    assert provider.fetch_calls == ["https://example.test/ams-3617.pdf"]
    assert (ticker_root / "USDA_daily_data" / "ams_3617_2026-06-12_data.json").read_bytes() == b'{"results": []}'
    assert (ticker_root / "USDA_daily_data" / "ams_3617_2026-06-12.pdf").read_bytes() == b"%PDF fixture"


def test_daily_refresh_uses_configured_stock_model_data_root_without_legacy_gpre_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "StockModelData"
    (data_root / "tickers").mkdir(parents=True)
    repo_root.mkdir()
    (repo_root / "stock_model_config.json").write_text(
        json.dumps({"data_root": str(data_root), "allow_onedrive_data_root": False}),
        encoding="utf-8",
    )

    def _fake_download(ticker_root: Path, **kwargs) -> dict[str, object]:
        assert ticker_root == data_root / "tickers" / "GPRE"
        assert kwargs["as_of_date"] == date(2026, 6, 12)
        (ticker_root / "corn_bids").mkdir(parents=True)
        return {
            "status": "ok",
            "row_count": 1,
            "locations_included": ["Central City"],
            "archive_raw_path": ticker_root / "corn_bids" / "raw.html",
            "archive_parsed_path": ticker_root / "corn_bids" / "parsed.csv",
        }

    monkeypatch.setattr(daily, "download_gpre_corn_bids_snapshot", _fake_download)

    summary = daily.refresh_gpre_daily_sources(
        repo_root=repo_root,
        as_of_date=date(2026, 6, 12),
        usda_sources=(),
    )

    assert summary["ticker_root"] == data_root / "tickers" / "GPRE"
    assert (data_root / "tickers" / "GPRE" / "corn_bids").exists()
    assert not (repo_root / "GPRE").exists()
