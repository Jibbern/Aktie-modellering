from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path
import shutil
import urllib.request
import uuid

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from pbi_xbrl.company_profiles import get_company_profile
from pbi_xbrl.market_data.cache import ensure_market_cache_dirs, remote_debug_path
from pbi_xbrl.market_data.providers.ams_3617 import AMS3617Provider, parse_ams_3617_pdf_text, parse_ams_3617_public_data_payload
from pbi_xbrl.market_data.providers.ams_3618 import AMS3618Provider, parse_ams_3618_pdf_text, parse_ams_3618_public_data_payload
from pbi_xbrl.market_data.providers.base import BaseMarketProvider
from pbi_xbrl.market_data.providers.cme_ethanol_platts import (
    CMEChicagoEthanolPlattsProvider,
    find_local_manual_ethanol_quarter_open_files,
    load_local_manual_ethanol_quarter_open_snapshot_rows,
    parse_manual_ethanol_quarter_open_snapshot_table,
    parse_cme_ethanol_settlement_table,
)
from pbi_xbrl.market_data.providers.nwer import NWERProvider, parse_nwer_pdf_text, parse_nwer_public_data_payload
import pbi_xbrl.market_data.providers.ams_3617 as ams_module
import pbi_xbrl.market_data.providers.ams_3618 as ams_3618_module
import pbi_xbrl.market_data.providers.nwer as nwer_module
import pbi_xbrl.market_data.service as market_service
import pbi_xbrl.market_data.usda_backfill as usda_backfill_module
from pbi_xbrl.market_data.service import (
    PARSED_SCHEMA_COLUMNS,
    _build_export_rows,
    _build_quarterly_rows,
    _dedupe_parsed_df,
    _standardize_parsed_df,
    build_current_qtd_simple_crush_snapshot,
    build_gpre_basis_proxy_model,
    build_gpre_official_proxy_history_series,
    build_gpre_official_proxy_snapshot,
    build_gpre_next_quarter_preview_snapshot,
    build_simple_crush_history_series,
    build_prior_quarter_simple_crush_snapshot,
    build_next_quarter_thesis_snapshot,
    download_gpre_corn_bids_snapshot,
    fetch_gpre_corn_bids_snapshot,
    load_market_export_rows,
    parse_gpre_corn_bids_html,
    parse_gpre_corn_bids_text,
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


def _local_test_dir(prefix: str) -> Path:
    root = Path.cwd()
    path = root / f"{str(prefix or 'pytest_tmp_').lstrip('.')}{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def _write_manual_quarter_open_snapshot(
    path: Path,
    rows: list[tuple[str, str, str, str, str]] | None = None,
) -> Path:
    payload_rows = rows or [
        ("2026-03-31", "2026-Q2", "2026-04", "2.0025", "barchart_manual"),
        ("2026-03-31", "2026-Q2", "2026-05", "2.0050", "barchart_manual"),
        ("2026-03-31", "2026-Q2", "2026-06", "1.9825", "barchart_manual"),
    ]
    lines = ["snapshot_date,target_quarter,contract_month,settle_usd_per_gal,source"]
    lines.extend(",".join(row) for row in payload_rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_gpre_corn_bids_archive_snapshot(
    ticker_root: Path,
    *,
    snapshot_date: date,
    rows: list[dict[str, object]],
    html_text: str = "<html><body>Last Updated 03/20/2026</body></html>",
    source_url: str = "fixture://gpre-bids",
) -> Path:
    storage_root = ticker_root / "corn_bids"
    raw_path = storage_root / "raw_snapshots" / snapshot_date.isoformat() / "grain_gpre_home.html"
    parsed_path = storage_root / "parsed_snapshots" / snapshot_date.isoformat() / "gpre_corn_bids_snapshot.csv"
    manifest_path = storage_root / "manifest.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    parsed_path.parent.mkdir(parents=True, exist_ok=True)
    storage_root.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(str(html_text or ""), encoding="utf-8")
    pd.DataFrame(list(rows or [])).to_csv(parsed_path, index=False)
    delivery_dates = [
        dt.isoformat()
        for dt in (
            market_service._gpre_parse_snapshot_date_like(rec.get("delivery_end"))
            for rec in rows
            if isinstance(rec, dict)
        )
        if isinstance(dt, date)
    ]
    payload = {"snapshots": []}
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {"snapshots": []}
    snapshots = [rec for rec in list(payload.get("snapshots") or []) if str(rec.get("snapshot_date") or "") != snapshot_date.isoformat()]
    snapshots.append(
        {
            "snapshot_date": snapshot_date.isoformat(),
            "raw_relpath": str(raw_path.relative_to(storage_root)),
            "parsed_relpath": str(parsed_path.relative_to(storage_root)),
            "source_url": source_url,
            "entry_url": "https://gpreinc.com/corn-bids/",
            "source_kind": "fixture_archive",
            "row_count": len(rows),
            "delivery_end_min": min(delivery_dates) if delivery_dates else "",
            "delivery_end_max": max(delivery_dates) if delivery_dates else "",
            "page_last_updated_text": "",
            "raw_fingerprint": "",
            "parsed_fingerprint": "",
        }
    )
    payload["snapshots"] = sorted(snapshots, key=lambda rec: str(rec.get("snapshot_date") or ""))
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return manifest_path


def _gpre_qtd_snapshot_fixture(
    *,
    as_of_date: date,
    quarter_end: date,
    ethanol_price: float,
    cbot_corn_price: float,
    corn_basis_usd_per_bu: float,
    natural_gas_price: float,
    coproduct_credit_usd_per_gal: float,
    ethanol_yield: float = 2.80,
    natural_gas_usage_btu_per_gal: float = 30_000.0,
    basis_source_kind: str = "actual_gpre_bids_with_ams_fallback",
    basis_source_label: str = "Retained GPRE plant bids",
) -> tuple[dict[str, object], dict[str, object]]:
    quarter_start, _ = market_service.calendar_quarter_bounds(as_of_date=quarter_end)
    simple_crush_per_gal = (
        float(ethanol_price)
        - ((float(cbot_corn_price) + float(corn_basis_usd_per_bu)) / float(ethanol_yield))
        - ((float(natural_gas_usage_btu_per_gal) / 1_000_000.0) * float(natural_gas_price))
    )
    snapshot = {
        "quarter_start": quarter_start,
        "quarter_end": quarter_end,
        "display_quarter": quarter_end,
        "as_of": as_of_date,
        "process_as_of": as_of_date,
        "snapshot_as_of": as_of_date,
        "current_market": {
            "ethanol_price": float(ethanol_price),
            "cbot_corn_front_price": float(cbot_corn_price),
            "natural_gas_price": float(natural_gas_price),
        },
        "market_meta": {
            "corn_price": {
                "official_weighted_corn_basis_usd_per_bu": float(corn_basis_usd_per_bu),
                "official_corn_basis_source_kind": basis_source_kind,
                "official_corn_basis_source_label": basis_source_label,
                "official_corn_basis_snapshot_date": as_of_date,
                "official_corn_basis_selection_rule": "latest_snapshot_on_or_before_as_of",
                "official_actual_bid_plant_count": 6,
                "official_fallback_plant_count": 1,
                "cbot_corn_front_price_usd_per_bu": float(cbot_corn_price),
            }
        },
        "current_process": {
            "simple_crush_per_gal": float(simple_crush_per_gal),
        },
        "official_simple_proxy_usd_per_gal": float(simple_crush_per_gal),
    }
    coproduct_frame = {
        "approximate_coproduct_credit_per_gal": float(coproduct_credit_usd_per_gal),
        "resolved_source_mode": "nwer_primary",
        "coverage_ratio": 0.92,
    }
    return snapshot, coproduct_frame


def _build_gpre_qtd_tracking_bundle(
    ticker_root: Path,
    *,
    current_as_of: date,
    current_ethanol_price: float,
    current_cbot_corn_price: float,
    current_corn_basis_usd_per_bu: float,
    current_natural_gas_price: float,
    current_coproduct_credit_usd_per_gal: float,
    quarter_open_as_of: date,
    quarter_open_ethanol_price: float,
    quarter_open_cbot_corn_price: float,
    quarter_open_corn_basis_usd_per_bu: float,
    quarter_open_natural_gas_price: float,
    quarter_open_coproduct_credit_usd_per_gal: float,
    quarter_end: date = date(2026, 6, 30),
    ethanol_yield: float = 2.80,
    natural_gas_usage_btu_per_gal: float = 30_000.0,
    rows: list[dict[str, object]] | None = None,
    plant_capacity_history: dict[str, object] | None = None,
) -> dict[str, object]:
    current_snapshot, current_coproduct_frame = _gpre_qtd_snapshot_fixture(
        as_of_date=current_as_of,
        quarter_end=quarter_end,
        ethanol_price=current_ethanol_price,
        cbot_corn_price=current_cbot_corn_price,
        corn_basis_usd_per_bu=current_corn_basis_usd_per_bu,
        natural_gas_price=current_natural_gas_price,
        coproduct_credit_usd_per_gal=current_coproduct_credit_usd_per_gal,
        ethanol_yield=ethanol_yield,
        natural_gas_usage_btu_per_gal=natural_gas_usage_btu_per_gal,
    )
    quarter_open_snapshot, quarter_open_coproduct_frame = _gpre_qtd_snapshot_fixture(
        as_of_date=quarter_open_as_of,
        quarter_end=quarter_end,
        ethanol_price=quarter_open_ethanol_price,
        cbot_corn_price=quarter_open_cbot_corn_price,
        corn_basis_usd_per_bu=quarter_open_corn_basis_usd_per_bu,
        natural_gas_price=quarter_open_natural_gas_price,
        coproduct_credit_usd_per_gal=quarter_open_coproduct_credit_usd_per_gal,
        ethanol_yield=ethanol_yield,
        natural_gas_usage_btu_per_gal=natural_gas_usage_btu_per_gal,
    )
    return market_service.build_gpre_current_qtd_trend_tracking_bundle(
        ticker_root=ticker_root,
        current_snapshot=current_snapshot,
        quarter_open_snapshot=quarter_open_snapshot,
        current_coproduct_frame=current_coproduct_frame,
        quarter_open_coproduct_frame=quarter_open_coproduct_frame,
        ethanol_yield=ethanol_yield,
        natural_gas_usage_btu_per_gal=natural_gas_usage_btu_per_gal,
        rows=rows,
        plant_capacity_history=plant_capacity_history,
    )


def _gpre_qtd_backfill_rows_fixture() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    rows.extend(
        _gpre_overlay_fixture_rows(
            ethanol_base=1.70,
            corn_front=4.46,
            gas_front=2.92,
            qd=date(2026, 6, 30),
            obs_dt=date(2026, 4, 2),
        )
    )
    rows.extend(
        _gpre_overlay_fixture_rows(
            ethanol_base=1.76,
            corn_front=4.52,
            gas_front=2.80,
            qd=date(2026, 6, 30),
            obs_dt=date(2026, 4, 10),
        )
    )
    return rows


def _diagnostic_text_stub(fetch_fn):
    def _wrapped(url: str, *, extra_headers: dict[str, str] | None = None):
        return fetch_fn(url, extra_headers=extra_headers), [
            {
                "attempt": 1,
                "url": str(url or ""),
                "status": "ok",
                "http_status": 200,
            }
        ]

    return _wrapped


def _diagnostic_bytes_stub(fetch_fn):
    def _wrapped(url: str, *, extra_headers: dict[str, str] | None = None):
        payload = fetch_fn(url, extra_headers=extra_headers)
        return payload, [
            {
                "attempt": 1,
                "url": str(url or ""),
                "status": "ok",
                "http_status": 200,
                "bytes": len(payload),
            }
        ]

    return _wrapped


def _gpre_overlay_fixture_rows(
    *,
    ethanol_base: float = 1.60,
    corn_front: float = 4.50,
    gas_front: float = 3.00,
    qd: date = date(2026, 6, 30),
    obs_dt: date = date(2026, 4, 3),
) -> list[dict[str, object]]:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=corn_front, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=gas_front, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=ethanol_base, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=ethanol_base + 0.02, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=ethanol_base + 0.01, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=ethanol_base - 0.01, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=ethanol_base - 0.02, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    rows.extend(
        [
            _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_sep26_usd", instrument="Corn futures", price_value=corn_front + 0.15, contract_tenor="sep26", source_type="nwer_pdf"),
            _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas_aug26_usd", instrument="Natural gas futures", price_value=gas_front + 0.20, contract_tenor="aug26", source_type="nwer_pdf"),
            _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_jul26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=ethanol_base + 0.03, contract_tenor="jul26", source_type="local_chicago_ethanol_futures_csv"),
            _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_aug26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=ethanol_base + 0.04, contract_tenor="aug26", source_type="local_chicago_ethanol_futures_csv"),
            _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_sep26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=ethanol_base + 0.05, contract_tenor="sep26", source_type="local_chicago_ethanol_futures_csv"),
        ]
    )
    return rows


def _gpre_basis_model_result_fixture(model_key: str = "process_front_loaded") -> dict[str, object]:
    return {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.20,
                    "gpre_proxy_official_usd_per_gal": -0.012,
                    "approx_market_bridge_proxy_usd_per_gal": 0.004,
                    "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.012,
                    "process_proxy_front_loaded_usd_per_gal": -0.020,
                }
            ]
        ),
        "gpre_proxy_model_key": model_key,
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Front-loaded current",
    }


def _gpre_proxy_implied_overlay_bundle_fixture() -> dict[str, object]:
    return {
        "official_frames": {
            "prior_quarter": {"quarter_end": date(2026, 3, 31), "value": 0.110},
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.210},
            "current_qtd": {"quarter_end": date(2026, 6, 30), "value": 0.240},
            "next_quarter_thesis": {"quarter_end": date(2026, 9, 30), "value": 0.280},
        },
        "gpre_proxy_frames": {
            "prior_quarter": {"quarter_end": date(2026, 3, 31), "value": 0.120},
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.230},
            "current_qtd": {"quarter_end": date(2026, 6, 30), "value": 0.260},
            "next_quarter_thesis": {"quarter_end": date(2026, 9, 30), "value": 0.310},
        },
    }


def _gpre_bids_snapshot_fixture(
    delivery_end: date,
    basis_value: float = -0.20,
    cash_price: float | None = None,
) -> dict[str, object]:
    def _row(location: str, region: str) -> dict[str, object]:
        row: dict[str, object] = {
            "location": location,
            "region": region,
            "delivery_label": delivery_end.strftime("%b %Y"),
            "delivery_end": delivery_end,
            "basis_usd_per_bu": basis_value,
        }
        if cash_price is not None:
            row["cash_price"] = cash_price
        return row

    return {
        "status": "ok",
        "source_kind": "fixture",
        "source_url": "fixture://gpre-bids",
        "rows": [
            _row("Central City", "nebraska"),
            _row("Wood River", "nebraska"),
            _row("York", "nebraska"),
            _row("Madison", "illinois"),
            _row("Mount Vernon", "indiana"),
            _row("Shenandoah", "iowa_west"),
            _row("Superior", "iowa_west"),
            _row("Otter Tail", "minnesota"),
        ],
    }


def _gpre_hedge_style_quarterly_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "quarter": date(2024, 3, 31),
                "quarter_label": "2024-Q1",
                "reported_consolidated_crush_margin_usd_per_gal": 0.535,
                "official_simple_proxy_usd_per_gal": 0.300,
                "process_quarter_open_anchor_usd_per_gal": 0.560,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.300,
                "process_proxy_front_loaded_usd_per_gal": 0.510,
                "process_quarter_open_blend_usd_per_gal": 0.495,
                "process_quarter_open_blend_hedge_realization_usd_per_gal": 0.495,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.495,
                "hard_quarter_flag": False,
                "hard_quarter_reason": "",
            },
            {
                "quarter": date(2024, 6, 30),
                "quarter_label": "2024-Q2",
                "reported_consolidated_crush_margin_usd_per_gal": 0.180,
                "official_simple_proxy_usd_per_gal": 0.310,
                "process_quarter_open_anchor_usd_per_gal": 0.340,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.280,
                "process_proxy_front_loaded_usd_per_gal": 0.290,
                "process_quarter_open_blend_usd_per_gal": 0.300,
                "process_quarter_open_blend_hedge_realization_usd_per_gal": 0.260,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.190,
                "hard_quarter_flag": True,
                "hard_quarter_reason": "Ops signal: maintenance/outage",
            },
            {
                "quarter": date(2024, 9, 30),
                "quarter_label": "2024-Q3",
                "reported_consolidated_crush_margin_usd_per_gal": -0.200,
                "official_simple_proxy_usd_per_gal": 0.250,
                "process_quarter_open_anchor_usd_per_gal": 0.300,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.220,
                "process_proxy_front_loaded_usd_per_gal": 0.280,
                "process_quarter_open_blend_usd_per_gal": 0.260,
                "process_quarter_open_blend_hedge_realization_usd_per_gal": 0.210,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.200,
                "hard_quarter_flag": True,
                "hard_quarter_reason": "Large realized-vs-on-paper gap",
            },
            {
                "quarter": date(2024, 12, 31),
                "quarter_label": "2024-Q4",
                "reported_consolidated_crush_margin_usd_per_gal": 0.270,
                "official_simple_proxy_usd_per_gal": 0.265,
                "process_quarter_open_anchor_usd_per_gal": np.nan,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.270,
                "process_proxy_front_loaded_usd_per_gal": 0.240,
                "process_quarter_open_blend_usd_per_gal": 0.270,
                "process_quarter_open_blend_hedge_realization_usd_per_gal": 0.270,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.270,
                "hard_quarter_flag": False,
                "hard_quarter_reason": "",
            },
        ]
    )


class _DebugDemoProvider(BaseMarketProvider):
    source = "debug_demo"
    landing_page_url = "https://example.test/debug"


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


def test_parse_nwer_pdf_text_extracts_coproduct_rows() -> None:
    text = """
National Weekly Ethanol Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News April 2, 2026
Grain By-Products
Distillers Corn Oil Feed Grade
Ethanol Plant
State/Province/Region Sale Type Price (¢/Lb) Price Change Average Year Ago Freight Delivery
Iowa East Ask 71.00 UNCH 71.00 48.50 FOB - T Current
Nebraska Ask 65.00-77.00 UNCH-UP 2.00 71.00 47.64 FOB - T Current
Source: USDA AMS Livestock, Poultry & Grain Market News Page 1 of 4
National Weekly Ethanol Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News April 2, 2026
Michigan Ask 70.00-73.00 UNCH-UP 1.00 71.50 46.60 FOB - T Current
South Dakota Ask 68.00-75.00 UNCH-UP 3.00 71.00 47.00 FOB - T Current
Distillers Grain Dried 10%
Ethanol Plant
State/Province/Region Sale Type Price ($/Ton) Price Change Average Year Ago Freight Delivery
Iowa East Ask 160.00-168.00 UNCH 164.00 141.67 FOB - T Current
Nebraska Ask 165.00-190.00 UNCH 179.29 156.80 FOB - T Current
Source: USDA AMS Livestock, Poultry & Grain Market News Page 2 of 4
"""

    rows = pd.DataFrame(parse_nwer_pdf_text(text, fallback_date=pd.Timestamp("2026-04-02"), source_file="nwer_2026-03-30.pdf"))

    assert float(rows.loc[rows["series_key"] == "corn_oil_iowa_east", "price_value"].iloc[0]) == 71.0
    assert float(rows.loc[rows["series_key"] == "corn_oil_nebraska", "price_value"].iloc[0]) == 71.0
    assert float(rows.loc[rows["series_key"] == "ddgs_10_iowa_east", "price_value"].iloc[0]) == 164.0
    assert float(rows.loc[rows["series_key"] == "ddgs_10_nebraska", "price_value"].iloc[0]) == 179.29
    assert str(rows.loc[rows["series_key"] == "corn_oil_nebraska", "unit"].iloc[0]) == "c/lb"
    assert str(rows.loc[rows["series_key"] == "ddgs_10_nebraska", "unit"].iloc[0]) == "$/ton"


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
    assert float(rows.loc[rows["series_key"] == "corn_basis_nebraska", "price_value"].iloc[0]) == pytest.approx(-0.265, abs=0.0001)
    assert float(rows.loc[rows["series_key"] == "corn_basis_iowa_east", "price_value"].iloc[0]) == pytest.approx(-0.285, abs=0.0001)


def test_parse_ams_3618_pdf_text_extracts_corn_oil_and_ddgs_rows() -> None:
    text = """
National Weekly Grain Co-Products Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News February 23, 2026
Corn Values¹
Distillers Corn Oil
Region/Location
Price (¢/Lb)
Value ($/Bu)³
Value Change
Week Ago
Year Ago
Eastern Cornbelt
56.95
0.34
UNCH
0.34
0.28
Iowa
57.19
0.34
UNCH
0.34
0.28
Kansas
52.00
0.31
UNCH
0.31
0.29
Nebraska
57.19
0.34
UNCH
0.34
0.28
Distillers Grain Dried 10%
Region/Location
Price ($/Ton)
Value ($/Bu)³
Value Change
Week Ago
Year Ago
Iowa
145.00
1.20
0.01
1.19
1.23
Nebraska
163.50
1.35
UNCH
1.35
1.40
Distillers Grain Wet 65-70%
"""

    rows = pd.DataFrame(parse_ams_3618_pdf_text(text, fallback_date=pd.Timestamp("2026-02-23"), source_file="ams_3618_2026-02-16.pdf"))

    assert float(rows.loc[rows["series_key"] == "corn_oil_iowa_avg", "price_value"].iloc[0]) == 57.19
    assert float(rows.loc[rows["series_key"] == "corn_oil_nebraska", "price_value"].iloc[0]) == 57.19
    assert float(rows.loc[rows["series_key"] == "ddgs_10_iowa", "price_value"].iloc[0]) == 145.0
    assert float(rows.loc[rows["series_key"] == "ddgs_10_nebraska", "price_value"].iloc[0]) == 163.5
    assert str(rows.loc[rows["series_key"] == "corn_oil_nebraska", "unit"].iloc[0]) == "c/lb"
    assert str(rows.loc[rows["series_key"] == "ddgs_10_nebraska", "unit"].iloc[0]) == "$/ton"


def test_ams_3618_provider_parse_raw_to_rows_uses_report_date_from_pdf_when_filename_is_undated(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_ams3618_undated_")
    try:
        provider = AMS3618Provider()
        pdf_path = tmp_path / "ams_3618_00183.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 demo 3618")
        text = """
National Weekly Grain Co-Products Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News March 30, 2026
Corn Values
Distillers Corn Oil
Region/Location
Price (c/Lb)
Value ($/Bu)
Value Change
Week Ago
Year Ago
Iowa
57.19
0.34
UNCH
0.34
0.28
Nebraska
57.19
0.34
UNCH
0.34
0.28
Distillers Grain Dried 10%
Region/Location
Price ($/Ton)
Value ($/Bu)
Value Change
Week Ago
Year Ago
Iowa
145.00
1.20
0.01
1.19
1.23
Nebraska
163.50
1.35
UNCH
1.35
1.40
Distillers Grain Wet 65-70%
""".strip()
        monkeypatch.setattr(ams_3618_module, "_safe_pdf_text", lambda _: text)

        df = provider.parse_raw_to_rows(
            tmp_path,
            tmp_path,
            [
                {
                    "report_date": "",
                    "local_path": str(pdf_path),
                }
            ],
        )

        assert not df.empty
        assert set(df["series_key"].astype(str)) >= {"corn_oil_iowa_avg", "corn_oil_nebraska", "ddgs_10_iowa", "ddgs_10_nebraska"}
        assert set(pd.to_datetime(df["observation_date"], errors="coerce").dt.date) == {date(2026, 3, 30)}
        assert set(pd.to_datetime(df["quarter"], errors="coerce").dt.date) == {date(2026, 3, 31)}
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_nwer_provider_parse_raw_to_rows_skips_ams_3618_pdf_in_shared_bioenergy_folder(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_nwer_skip_3618_")
    try:
        provider = NWERProvider()
        pdf_path = tmp_path / "ams_3618_00183.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 demo 3618")
        text = """
National Weekly Grain Co-Products Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News March 30, 2026
Distillers Corn Oil
Nebraska 57.19 0.34
Distillers Grain Dried 10%
Nebraska 163.50 1.35
""".strip()
        monkeypatch.setattr(nwer_module, "_safe_pdf_text", lambda _: text)

        df = provider.parse_raw_to_rows(
            tmp_path,
            tmp_path,
            [
                {
                    "report_date": "2026-03-30",
                    "local_path": str(pdf_path),
                }
            ],
        )

        assert df.empty
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_ams3617_public_data_refresh_writes_latest_json_and_parses_rows(monkeypatch) -> None:
    provider = AMS3617Provider()
    conditions = {
        "reportBeginDates": ["2026-04-24", "2026-04-23", "2026-04-22"],
        "reportEndDates": ["2026-04-24", "2026-04-23", "2026-04-22"],
    }
    detail_payload = {
        "reportSection": "Report Detail",
        "results": [
            {
                "report_date": "04/23/2026",
                "report_begin_date": "04/23/2026",
                "report_end_date": "04/23/2026",
                "commodity": "Corn",
                "quote_type": "Basis",
                "state/Province": "Iowa",
                "region": "East",
                "basis Min": -12,
                "basis Max": 8,
                "avg_price": 4.455,
            }
        ],
    }
    search_urls: list[str] = []

    monkeypatch.setattr(provider, "_today", lambda: date(2026, 4, 23))
    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _diagnostic_text_stub(lambda url, *, extra_headers=None: json.dumps(conditions)))

    def _fake_bytes(url: str, *, extra_headers: dict[str, str] | None = None) -> bytes:
        del extra_headers
        search_urls.append(str(url))
        return json.dumps(detail_payload).encode("utf-8")

    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _diagnostic_bytes_stub(_fake_bytes))
    tmp_path = _local_test_dir(".pytest_tmp_market_public_data_3617_")
    try:
        cache_root = tmp_path / "sec_cache" / "market_data"
        ensure_market_cache_dirs(cache_root)
        ticker_root = tmp_path / "GPRE"

        result = provider.sync_raw(cache_root, ticker_root, refresh=True)
        local_json = ticker_root / "USDA_daily_data" / "ams_3617_2026-04-23_data.json"

        assert result["raw_added"] == 1
        assert local_json.exists()
        assert search_urls and "04/23/2026:04/23/2026" in search_urls[0]
        assert "04/24/2026" not in search_urls[0]
        assert str(result["entries"][0].get("asset_type") or "") == "json"

        df = provider.parse_raw_to_rows(cache_root, ticker_root, result["entries"])
        assert set(df["series_key"].astype(str)) == {"corn_iowa_east", "corn_basis_iowa_east"}
        basis = float(df.loc[df["series_key"] == "corn_basis_iowa_east", "price_value"].iloc[0])
        assert basis == pytest.approx(-0.02)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_public_data_payload_parsers_map_coproduct_rows_to_existing_series() -> None:
    nwer_rows = parse_nwer_public_data_payload(
        {
            "results": [
                {
                    "report_end_date": "04/17/2026",
                    "commodity": "Distillers Corn Oil",
                    "application": "Feed Grade",
                    "state/Province": "Nebraska",
                    "avg_price": 75.25,
                },
                {
                    "report_end_date": "04/17/2026",
                    "commodity": "Distillers Grain",
                    "variety": "Dried 10%",
                    "state/Province": "Iowa East",
                    "avg_price": 162.5,
                },
                {
                    "report_end_date": "04/17/2026",
                    "commodity": "Ethanol",
                    "state/Province": "Nebraska",
                    "avg_price": 1.75,
                },
            ]
        },
        source_file="nwer_2026-04-17_data.json",
    )
    ams3618_rows = parse_ams_3618_public_data_payload(
        {
            "results": [
                {
                    "report_end_date": "04/17/2026",
                    "commodity": "Distillers Corn Oil",
                    "application": "Feed Grade",
                    "trade_loc": "Eastern Cornbelt",
                    "price": 76.31,
                },
                {
                    "report_end_date": "04/17/2026",
                    "commodity": "Distillers Grain",
                    "variety": "Dried 10%",
                    "trade_loc": "Nebraska",
                    "price": 168.0,
                },
            ]
        },
        source_file="ams_3618_2026-04-17_data.json",
    )

    assert {row["series_key"] for row in nwer_rows} == {
        "corn_oil_nebraska",
        "ddgs_10_iowa_east",
        "ethanol_nebraska",
    }
    assert {row["series_key"] for row in ams3618_rows} == {
        "corn_oil_eastern_cornbelt",
        "ddgs_10_nebraska",
    }


def test_shared_bioenergy_public_data_json_is_guarded_by_slug_id() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_public_data_slug_guard_")
    try:
        cache_root = tmp_path / "sec_cache" / "market_data"
        ensure_market_cache_dirs(cache_root)
        ams3618_json = tmp_path / "ams_3618_2026-04-17_data.json"
        ams3618_json.write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "slug_id": 3618,
                            "report_end_date": "04/17/2026",
                            "commodity": "Distillers Corn Oil",
                            "application": "Feed Grade",
                            "trade_loc": "Nebraska",
                            "price": 74.71,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        nwer_json = tmp_path / "nwer_2026-04-17_data.json"
        nwer_json.write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "slug_id": 3616,
                            "report_end_date": "04/17/2026",
                            "commodity": "Distillers Corn Oil",
                            "application": "Feed Grade",
                            "state/Province": "Nebraska",
                            "avg_price": 74.71,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        nwer_df = NWERProvider().parse_raw_to_rows(
            cache_root,
            tmp_path,
            [{"report_date": "2026-04-17", "local_path": str(ams3618_json)}],
        )
        ams3618_df = AMS3618Provider().parse_raw_to_rows(
            cache_root,
            tmp_path,
            [{"report_date": "2026-04-17", "local_path": str(nwer_json)}],
        )

        assert nwer_df.empty
        assert ams3618_df.empty
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


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
    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _diagnostic_text_stub(_fake_fetch_text))
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _diagnostic_bytes_stub(lambda url, *, extra_headers=None: b"demo"))

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_service"
    shutil.rmtree(tmp_path, ignore_errors=True)
    tmp_path.mkdir(parents=True, exist_ok=True)
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    ticker_root = tmp_path / "GPRE"

    try:
        result = provider.sync_raw(cache_root, ticker_root, refresh=True)

        assert result["raw_added"] >= 6
        assert (ticker_root / "USDA_bioenergy_reports" / "nwer_2026-01-23.pdf").exists()
        assert (ticker_root / "USDA_bioenergy_reports" / "nwer_2026-01-23_data.csv").exists()
        assert any(str(entry.get("asset_type") or "") == "pdf" for entry in result["entries"])
        assert any(str(entry.get("asset_type") or "") == "data" for entry in result["entries"])
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_usda_release_fragment_urls_accept_visible_slug_text() -> None:
    provider = NWERProvider()
    landing_html = """
    <html><body>
      <div>Slug Id: 3616</div>
      <p>Latest release content is loaded dynamically.</p>
    </body></html>
    """

    urls = provider._release_fragment_urls(landing_html, provider.landing_page_url)

    assert urls == [
        "https://mymarketnews.ams.usda.gov/get_latest_release/3616",
        "https://mymarketnews.ams.usda.gov/get_previous_release/3616",
    ]


def test_base_provider_fetch_text_diagnostic_retries_after_transient_timeout(monkeypatch) -> None:
    provider = _DebugDemoProvider()
    provider.remote_retry_attempts = 3
    provider.remote_backoff_seconds = (0.0, 0.0, 0.0)
    call_count = {"count": 0}

    class _Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b"ok"

    def _fake_urlopen(req, timeout=0):
        del req, timeout
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise TimeoutError("timed out")
        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    text, attempts = provider._fetch_text_diagnostic("https://example.test/debug")

    assert text == "ok"
    assert len(attempts) == 2
    assert str(attempts[0].get("classification") or "") == "network_timeout"
    assert str(attempts[1].get("status") or "") == "ok"


def test_nwer_remote_discovery_classifies_slug_not_found(monkeypatch) -> None:
    provider = NWERProvider()
    landing_html = "<html><body><div>No slug marker here.</div></body></html>"

    monkeypatch.setattr(provider, "_today", lambda: date(2026, 3, 28))
    monkeypatch.setattr(
        provider,
        "_fetch_text_diagnostic",
        _diagnostic_text_stub(lambda url, *, extra_headers=None: landing_html),
    )

    tmp_path = _local_test_dir(".pytest_tmp_market_data_slug_missing_")
    try:
        cache_root = tmp_path / "sec_cache" / "market_data"
        ensure_market_cache_dirs(cache_root)

        discovered = provider.discover_remote_assets(as_of=date(2026, 3, 28), cache_root=cache_root)
        debug_payload = json.loads(remote_debug_path(cache_root, "nwer").read_text(encoding="utf-8"))
        latest_refresh = debug_payload.get("latest_refresh") or {}

        assert discovered == []
        assert str(latest_refresh.get("slug_id") or "") == ""
        assert str(latest_refresh.get("final_classification") or "") == "slug_not_found"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_nwer_sync_raw_writes_remote_debug_artifact_for_visible_slug_text(monkeypatch) -> None:
    provider = NWERProvider()
    landing_html = """
    <html><body>
      <div>Slug Id: 3616</div>
    </body></html>
    """
    latest_html = """
    {"html":"<a href=\"/filerepo/sites/default/files/3616/2026-01-23/1299792/ams_3616_00175.pdf\">Jan 23 PDF</a><a href=\"/filerepo/sites/default/files/3616/2026-01-23/1299792/ams_3616_00175.csv\">Jan 23 DATA</a>"}
    """
    previous_html = """
    {"html":"<a href=\"/filerepo/sites/default/files/3616/2026-01-16/1299754/ams_3616_00168.pdf\">Jan 16 PDF</a>"}
    """

    def _fake_fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del extra_headers
        if "get_latest_release" in url:
            return latest_html
        if "get_previous_release" in url:
            return previous_html
        return landing_html

    monkeypatch.setattr(provider, "_today", lambda: date(2026, 3, 28))
    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _diagnostic_text_stub(_fake_fetch_text))
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _diagnostic_bytes_stub(lambda url, *, extra_headers=None: b"demo"))

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_service_debug"
    shutil.rmtree(tmp_path, ignore_errors=True)
    tmp_path.mkdir(parents=True, exist_ok=True)
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    ticker_root = tmp_path / "GPRE"

    try:
        provider.sync_raw(cache_root, ticker_root, refresh=True)
        debug_payload = json.loads(remote_debug_path(cache_root, "nwer").read_text(encoding="utf-8"))

        assert str(((debug_payload.get("latest_refresh") or {}).get("slug_id") or "")).strip() == "3616"
        fragment_urls = list((debug_payload.get("latest_refresh") or {}).get("fragment_urls") or [])
        assert "https://mymarketnews.ams.usda.gov/get_latest_release/3616" in fragment_urls
        discovered = list((debug_payload.get("latest_refresh") or {}).get("direct_asset_urls_discovered") or [])
        assert any(str(item.get("url") or "").endswith("ams_3616_00175.pdf") for item in discovered)
        downloads = list((debug_payload.get("latest_refresh") or {}).get("download_attempts") or [])
        assert any(str(item.get("status") or "") in {"updated", "skipped"} for item in downloads)
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
    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _diagnostic_text_stub(_fake_fetch_text))
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _diagnostic_bytes_stub(lambda url, *, extra_headers=None: b"demo"))

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_service"
    shutil.rmtree(tmp_path, ignore_errors=True)
    tmp_path.mkdir(parents=True, exist_ok=True)
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    ticker_root = tmp_path / "GPRE"

    try:
        result = provider.sync_raw(cache_root, ticker_root, refresh=True)

        assert result["raw_added"] >= 6
        assert (ticker_root / "USDA_bioenergy_reports" / "nwer_2026-01-23.pdf").exists()
        assert (ticker_root / "USDA_bioenergy_reports" / "nwer_2026-01-23_data.csv").exists()
        assert any(str(entry.get("asset_type") or "") == "pdf" for entry in result["entries"])
        assert any(str(entry.get("asset_type") or "") == "data" for entry in result["entries"])
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_ams_3618_sync_raw_discovers_current_quarter_pdf_asset(monkeypatch) -> None:
    provider = AMS3618Provider()
    landing_html = """
    <html><body>
      <div>Slug Id: 3618</div>
    </body></html>
    """
    latest_html = """
    {"html":"<a href=\"/filerepo/sites/default/files/3618/2026-02-16/1306158/ams_3618_00177.pdf\">Feb 16 PDF</a>"}
    """

    def _fake_fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
        del extra_headers
        if "get_latest_release" in url:
            return latest_html
        if "get_previous_release" in url:
            return "{\"html\":\"\"}"
        return landing_html

    monkeypatch.setattr(provider, "_today", lambda: date(2026, 3, 28))
    monkeypatch.setattr(provider, "_fetch_text_diagnostic", _diagnostic_text_stub(_fake_fetch_text))
    monkeypatch.setattr(provider, "_fetch_bytes_diagnostic", _diagnostic_bytes_stub(lambda url, *, extra_headers=None: b"demo"))

    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_service_3618"
    shutil.rmtree(tmp_path, ignore_errors=True)
    tmp_path.mkdir(parents=True, exist_ok=True)
    cache_root = tmp_path / "sec_cache" / "market_data"
    ensure_market_cache_dirs(cache_root)
    ticker_root = tmp_path / "GPRE"

    try:
        result = provider.sync_raw(cache_root, ticker_root, refresh=True)

        assert result["raw_added"] >= 1
        assert (ticker_root / "USDA_bioenergy_reports" / "ams_3618_2026-02-16.pdf").exists()
        assert any(str(entry.get("asset_type") or "") == "pdf" for entry in result["entries"])
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


def test_bootstrap_specs_can_read_usda_bioenergy_folder_for_nwer() -> None:
    tmp_path = Path(__file__).resolve().parents[1] / "tests" / "_tmp_market_data_bioenergy_bootstrap"
    shutil.rmtree(tmp_path, ignore_errors=True)
    ticker_root = tmp_path / "GPRE"
    bioenergy_dir = ticker_root / "USDA_bioenergy_reports"
    bioenergy_dir.mkdir(parents=True, exist_ok=True)
    (bioenergy_dir / "nwer_weekly.csv").write_text(
        "week_end,ethanol_nebraska,source_pdf\n2026-01-23,1.61,nwer_2026-01-23.pdf\n",
        encoding="utf-8",
    )

    try:
        nwer_df, nwer_fp = market_service._bootstrap_rows_for_source("nwer", ticker_root)
        assert nwer_fp != "none"
        assert float(nwer_df.loc[nwer_df["series_key"] == "ethanol_nebraska", "price_value"].iloc[0]) == 1.61
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_profile_enables_ams_3618_without_changing_non_gpre_sources() -> None:
    gpre_profile = get_company_profile("GPRE")
    pbi_profile = get_company_profile("PBI")

    assert "ams_3618" in tuple(gpre_profile.enabled_market_sources or ())
    assert "ams_3618" not in tuple(pbi_profile.enabled_market_sources or ())


def test_sync_market_cache_rebuilds_raw_manifest_from_existing_raw_files(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_sync_")
    try:
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
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_sync_market_cache_ingests_manually_added_local_usda_pdfs(monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_local_usda_sync_")
    try:
        cache_dir = tmp_path / "sec_cache" / "GPRE"
        cache_root = tmp_path / "sec_cache" / "market_data"
        ticker_root = tmp_path / "GPRE"
        weekly_dir = ticker_root / "USDA_weekly_data"
        daily_dir = ticker_root / "USDA_daily_data"
        weekly_dir.mkdir(parents=True, exist_ok=True)
        daily_dir.mkdir(parents=True, exist_ok=True)
        ensure_market_cache_dirs(cache_root)

        weekly_pdf = weekly_dir / "nwer_2026-03-30.pdf"
        daily_pdf = daily_dir / "ams_3617_2026-04-03.pdf"
        weekly_pdf.write_bytes(b"%PDF-1.4 weekly demo")
        daily_pdf.write_bytes(b"%PDF-1.4 daily demo")

        nwer_provider = NWERProvider()
        ams_provider = AMS3617Provider()

        def _fake_nwer_parse(cache_root_in: Path, ticker_root_in: Path, raw_entries: list[dict[str, object]]) -> pd.DataFrame:
            del cache_root_in, ticker_root_in
            source_files = {Path(str(entry.get("local_path") or "")).name for entry in raw_entries}
            assert "nwer_2026-03-30.pdf" in source_files
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
                        source_file="nwer_2026-03-30.pdf",
                        observation_date="2026-03-30",
                        publication_date="2026-03-30",
                        quarter="2026-03-31",
                        price_value=1.62,
                    )
                ]
            )

        def _fake_ams_parse(cache_root_in: Path, ticker_root_in: Path, raw_entries: list[dict[str, object]]) -> pd.DataFrame:
            del cache_root_in, ticker_root_in
            source_files = {Path(str(entry.get("local_path") or "")).name for entry in raw_entries}
            assert "ams_3617_2026-04-03.pdf" in source_files
            return pd.DataFrame(
                [
                    _parsed_row(
                        source="ams_3617",
                        report_type="ams_3617_pdf",
                        source_type="ams_3617_pdf",
                        market_family="corn_basis",
                        series_key="corn_basis_nebraska",
                        instrument="Corn basis",
                        location="Nebraska",
                        region="nebraska",
                        source_file="ams_3617_2026-04-03.pdf",
                        observation_date="2026-04-03",
                        publication_date="2026-04-03",
                        quarter="2026-06-30",
                        price_value=-0.28,
                    )
                ]
            )

        monkeypatch.setitem(market_service.PROVIDERS, "nwer", nwer_provider)
        monkeypatch.setitem(market_service.PROVIDERS, "ams_3617", ams_provider)
        monkeypatch.setattr(nwer_provider, "parse_raw_to_rows", _fake_nwer_parse)
        monkeypatch.setattr(ams_provider, "parse_raw_to_rows", _fake_ams_parse)

        class _Profile:
            enabled_market_sources = ("nwer", "ams_3617")

        summary = sync_market_cache(cache_dir, "GPRE", profile=_Profile(), sync_raw=True, refresh=False, reparse=True)

        assert summary.raw_added >= 2
        raw_weekly = cache_root / "raw" / "nwer" / "2026" / "nwer_2026-03-30.pdf"
        raw_daily = cache_root / "raw" / "ams_3617" / "2026" / "ams_3617_2026-04-03.pdf"
        assert raw_weekly.exists()
        assert raw_daily.exists()
        export_df = pd.read_parquet(cache_root / "parsed" / "exports" / "GPRE.parquet")
        assert {"nwer_2026-03-30.pdf", "ams_3617_2026-04-03.pdf"} <= set(export_df["source_file"].astype(str))
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_sync_market_cache_exports_manually_added_undated_ams_3618_pdf(monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_ams3618_manual_sync_")
    try:
        cache_dir = tmp_path / "sec_cache" / "GPRE"
        cache_root = tmp_path / "sec_cache" / "market_data"
        ticker_root = tmp_path / "GPRE"
        bioenergy_dir = ticker_root / "USDA_bioenergy_reports"
        bioenergy_dir.mkdir(parents=True, exist_ok=True)
        ensure_market_cache_dirs(cache_root)

        ams_pdf = bioenergy_dir / "ams_3618_00183.pdf"
        ams_pdf.write_bytes(b"%PDF-1.4 ams 3618 manual")

        text = """
National Weekly Grain Co-Products Report
Agricultural Marketing Service
Livestock, Poultry, and Grain Market News March 30, 2026
Corn Values
Distillers Corn Oil
Region/Location
Price (c/Lb)
Value ($/Bu)
Value Change
Week Ago
Year Ago
Iowa
57.19
0.34
UNCH
0.34
0.28
Nebraska
57.19
0.34
UNCH
0.34
0.28
Distillers Grain Dried 10%
Region/Location
Price ($/Ton)
Value ($/Bu)
Value Change
Week Ago
Year Ago
Iowa
145.00
1.20
0.01
1.19
1.23
Nebraska
163.50
1.35
UNCH
1.35
1.40
Distillers Grain Wet 65-70%
""".strip()

        monkeypatch.setattr(ams_3618_module, "_safe_pdf_text", lambda _: text)
        monkeypatch.setattr(market_service, "_refresh_gpre_corn_bids_download", lambda *args, **kwargs: None)

        class _Profile:
            enabled_market_sources = ("ams_3618",)

        summary = sync_market_cache(cache_dir, "GPRE", profile=_Profile(), sync_raw=True, refresh=False, reparse=True)

        assert summary.raw_added >= 1
        raw_ams = cache_root / "raw" / "ams_3618" / "2026"
        assert any(path.name == "ams_3618_00183.pdf" for path in raw_ams.glob("*.pdf"))
        export_df = pd.read_parquet(cache_root / "parsed" / "exports" / "GPRE.parquet")
        export_source_types = set(export_df["source_type"].astype(str))
        export_series = set(export_df["series_key"].astype(str))
        assert "ams_3618_pdf" in export_source_types
        assert {"corn_oil_iowa_avg", "corn_oil_nebraska", "ddgs_10_iowa", "ddgs_10_nebraska"} <= export_series
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_bioenergy_manual_folder_policy_keeps_nwer_primary_path_and_ams_3618_manual_support() -> None:
    nwer_provider = NWERProvider()
    ams_provider = AMS3618Provider()

    assert nwer_provider.local_dir_name == "USDA_bioenergy_reports"
    assert ams_provider.local_dir_name == "USDA_bioenergy_reports"
    assert "USDA_bioenergy_reports/*" in set(nwer_provider.local_patterns)
    assert "USDA_bioenergy_reports/*" in set(ams_provider.local_patterns)
    assert "USDA_weekly_data/*" in set(nwer_provider.local_patterns)
    assert "ams_3618_pdfs/*" in set(ams_provider.local_patterns)


def test_nwer_provider_parse_raw_to_rows_keeps_historical_pdf_entries(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_nwer_")
    try:
        provider = NWERProvider()
        pdf_2024 = tmp_path / "nwer_2024-01-12.pdf"
        pdf_2026 = tmp_path / "nwer_2026-01-23.pdf"
        pdf_2024.write_bytes(b"%PDF-1.4 demo 2024")
        pdf_2026.write_bytes(b"%PDF-1.4 demo 2026")

        monkeypatch.setattr(nwer_module, "_safe_pdf_text", lambda _: "demo nwer text")

        def _fake_parse(text: str, *, fallback_date: object, source_file: str) -> list[dict[str, object]]:
            del text, source_file
            report_date = pd.Timestamp(fallback_date).date()
            return [
                _parsed_row(
                    source="nwer",
                    report_type="nwer_pdf",
                    source_type="nwer_pdf",
                    market_family="ethanol_price",
                    series_key="ethanol_nebraska",
                    instrument="Ethanol",
                    location="Nebraska",
                    region="nebraska",
                    source_file=f"nwer_{report_date.isoformat()}.pdf",
                    observation_date=report_date.isoformat(),
                    publication_date=report_date.isoformat(),
                    quarter=str(pd.Timestamp(report_date) + pd.offsets.QuarterEnd(0))[:10],
                    price_value=1.61,
                )
            ]

        monkeypatch.setattr(nwer_module, "parse_nwer_pdf_text", _fake_parse)

        df = provider.parse_raw_to_rows(
            tmp_path,
            tmp_path,
            [
                {"report_date": "2024-01-12", "local_path": str(pdf_2024)},
                {"report_date": "2026-01-23", "local_path": str(pdf_2026)},
            ],
        )

        quarters = set(pd.to_datetime(df["quarter"], errors="coerce").dt.date)
        assert quarters == {date(2024, 3, 31), date(2026, 3, 31)}
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_ams_provider_parse_raw_to_rows_keeps_historical_pdf_entries(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_ams_")
    try:
        provider = AMS3617Provider()
        pdf_2023 = tmp_path / "ams_3617_2023-04-03.pdf"
        pdf_2026 = tmp_path / "ams_3617_2026-01-23.pdf"
        pdf_2023.write_bytes(b"%PDF-1.4 demo 2023")
        pdf_2026.write_bytes(b"%PDF-1.4 demo 2026")

        monkeypatch.setattr(ams_module, "_safe_pdf_text", lambda _: "demo ams text")

        def _fake_parse(text: str, *, fallback_date: object, source_file: str) -> list[dict[str, object]]:
            del text, source_file
            report_date = pd.Timestamp(fallback_date).date()
            return [
                _parsed_row(
                    source="ams_3617",
                    report_type="ams_3617_pdf",
                    source_type="ams_3617_pdf",
                    market_family="corn_price",
                    series_key="corn_nebraska",
                    instrument="Corn cash price",
                    location="Nebraska",
                    region="nebraska",
                    source_file=f"ams_3617_{report_date.isoformat()}.pdf",
                    observation_date=report_date.isoformat(),
                    publication_date=report_date.isoformat(),
                    quarter=str(pd.Timestamp(report_date) + pd.offsets.QuarterEnd(0))[:10],
                    price_value=4.12,
                )
            ]

        monkeypatch.setattr(ams_module, "parse_ams_3617_pdf_text", _fake_parse)

        df = provider.parse_raw_to_rows(
            tmp_path,
            tmp_path,
            [
                {"report_date": "2023-04-03", "local_path": str(pdf_2023)},
                {"report_date": "2026-01-23", "local_path": str(pdf_2026)},
            ],
        )

        quarters = set(pd.to_datetime(df["quarter"], errors="coerce").dt.date)
        assert quarters == {date(2023, 6, 30), date(2026, 3, 31)}
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_market_export_rows_repairs_thin_historical_export_from_local_usda_history(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_repair_")
    try:
        cache_dir = tmp_path / "sec_cache"
        cache_root = market_service.resolve_market_cache_root(cache_dir)
        ensure_market_cache_dirs(cache_root)
        ticker_root = tmp_path / "GPRE"
        (ticker_root / "USDA_weekly_data").mkdir(parents=True, exist_ok=True)
        (ticker_root / "USDA_weekly_data" / "nwer_2024-01-12.pdf").write_bytes(b"%PDF-1.4 old history")
        export_path = cache_root / "parsed" / "exports" / "GPRE.parquet"
        thin_df = pd.DataFrame(
            [
                {
                    "observation_date": pd.Timestamp("2026-01-23"),
                    "quarter": pd.Timestamp("2026-03-31"),
                    "aggregation_level": "observation",
                    "source_file": "nwer_2026-01-23.pdf",
                    "source_type": "nwer_pdf",
                    "market_family": "ethanol_price",
                    "series_key": "ethanol_nebraska",
                    "instrument": "Ethanol",
                    "region": "nebraska",
                    "contract_tenor": "",
                    "price_value": 1.61,
                    "unit": "$/gal",
                    "parsed_text": "thin export",
                    "quality": "high",
                    "_obs_count": 1,
                }
            ]
        )
        thin_df.to_parquet(export_path, index=False)

        calls: list[tuple[bool, bool, bool]] = []

        def _fake_sync(cache_dir_in: Path, ticker_in: str, profile: object = None, *, sync_raw: bool = False, refresh: bool = False, reparse: bool = False):
            del cache_dir_in, ticker_in, profile
            calls.append((sync_raw, refresh, reparse))
            repaired_df = pd.DataFrame(
                [
                    {**thin_df.iloc[0].to_dict(), "observation_date": pd.Timestamp("2024-01-12"), "quarter": pd.Timestamp("2024-03-31"), "source_file": "nwer_2024-01-12.pdf", "price_value": 1.55},
                    {**thin_df.iloc[0].to_dict(), "observation_date": pd.Timestamp("2026-01-23"), "quarter": pd.Timestamp("2026-03-31"), "source_file": "nwer_2026-01-23.pdf", "price_value": 1.61},
                ]
            )
            repaired_df.to_parquet(export_path, index=False)
            return market_service.SyncSummary(
                sources_enabled=("nwer",),
                raw_added=0,
                raw_refreshed=0,
                raw_skipped=0,
                parsed_sources=("nwer",),
                export_rows=int(len(repaired_df)),
                export_path=export_path,
            )

        monkeypatch.setattr(market_service, "sync_market_cache", _fake_sync)

        class _Profile:
            enabled_market_sources = ("nwer",)

        rows = load_market_export_rows(cache_dir, "GPRE", profile=_Profile(), ensure_cache=True)

        assert calls == [(True, False, True)]
        quarters = sorted({row["quarter"] for row in rows if isinstance(row.get("quarter"), date)})
        assert quarters == [date(2024, 3, 31), date(2026, 3, 31)]
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_market_export_rows_repairs_export_when_basis_series_are_missing(monkeypatch) -> None:
    tmp_path = _local_test_dir(".pytest_tmp_market_basis_repair_")
    try:
        cache_dir = tmp_path / "sec_cache"
        cache_root = market_service.resolve_market_cache_root(cache_dir)
        ensure_market_cache_dirs(cache_root)
        ticker_root = tmp_path / "GPRE"
        (ticker_root / "USDA_daily_data").mkdir(parents=True, exist_ok=True)
        (ticker_root / "USDA_daily_data" / "ams_3617_2025-09-30.pdf").write_bytes(b"%PDF-1.4 basis history")
        export_path = cache_root / "parsed" / "exports" / "GPRE.parquet"
        thin_df = pd.DataFrame(
            [
                {
                    "observation_date": pd.Timestamp("2025-09-30"),
                    "quarter": pd.Timestamp("2025-09-30"),
                    "aggregation_level": "quarter_avg",
                    "source_file": "ams_3617_2025-09-30.pdf",
                    "source_type": "ams_3617_pdf",
                    "market_family": "corn_price",
                    "series_key": "corn_nebraska",
                    "instrument": "Corn cash price",
                    "region": "nebraska",
                    "contract_tenor": "",
                    "price_value": 3.93,
                    "unit": "$/bushel",
                    "parsed_text": "thin export",
                    "quality": "high",
                    "_obs_count": 1,
                }
            ]
        )
        thin_df.to_parquet(export_path, index=False)

        calls: list[tuple[bool, bool, bool]] = []

        def _fake_sync(cache_dir_in: Path, ticker_in: str, profile: object = None, *, sync_raw: bool = False, refresh: bool = False, reparse: bool = False):
            del cache_dir_in, ticker_in, profile
            calls.append((sync_raw, refresh, reparse))
            repaired_df = pd.DataFrame(
                [
                    thin_df.iloc[0].to_dict(),
                    {
                        **thin_df.iloc[0].to_dict(),
                        "market_family": "corn_basis",
                        "series_key": "corn_basis_nebraska",
                        "instrument": "Corn basis",
                        "price_value": -0.265,
                    },
                ]
            )
            repaired_df.to_parquet(export_path, index=False)
            return market_service.SyncSummary(
                sources_enabled=("ams_3617",),
                raw_added=0,
                raw_refreshed=0,
                raw_skipped=0,
                parsed_sources=("ams_3617",),
                export_rows=int(len(repaired_df)),
                export_path=export_path,
            )

        monkeypatch.setattr(market_service, "sync_market_cache", _fake_sync)

        class _Profile:
            enabled_market_sources = ("ams_3617",)

        rows = load_market_export_rows(cache_dir, "GPRE", profile=_Profile(), ensure_cache=True)

        assert calls == [(True, False, True)]
        series_keys = {str(row.get("series_key") or "") for row in rows}
        assert "corn_basis_nebraska" in series_keys
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_parse_gpre_corn_bids_text_and_html_decode_basis_rows() -> None:
    text_blob = """
Madison
Commodity Delivery End Cash Price Basis Symbol Futures Price Change
Corn Apr 2026 4.84 0.28 @C6K 455'6 1'4
Corn May 2026 4.88 0.32 @C6K 455'6 1'4
Wood River
Commodity Delivery End Cash Price Basis Symbol Futures Price Change
Corn Apr 2026 4.31 -0.25 @C6K 455'6 1'4
""".strip()
    text_rows = parse_gpre_corn_bids_text(text_blob, as_of_date=date(2026, 4, 2))
    assert len(text_rows) >= 3
    madison_apr = next(rec for rec in text_rows if str(rec.get("location")) == "Madison" and str(rec.get("delivery_label")) == "Apr 2026")
    assert float(pd.to_numeric(madison_apr.get("cash_price"), errors="coerce")) == pytest.approx(4.84, abs=0.001)
    assert float(pd.to_numeric(madison_apr.get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(0.28, abs=0.001)

    html_blob = """
    <html><script>var cfg = { NoScrapeOffset: 33.4225 };</script><body>
    <table>
      <tr><td><b>Madison</b></td></tr>
      <tr>
        <td>Corn</td><td>Apr 2026</td>
        <td><script>displayNumber(38.2425,2);</script></td>
        <td>@C6K</td>
        <td title="Basis Month: @C6K"><script>displayNumber(33.7025,2);</script></td>
      </tr>
    </table>
    </body></html>
    """.strip()
    html_rows = parse_gpre_corn_bids_html(
        html_blob,
        as_of_date=date(2026, 4, 2),
        source_url="https://grain.gpreinc.com/index.cfm",
    )
    assert len(html_rows) == 1
    assert float(pd.to_numeric(html_rows[0].get("cash_price"), errors="coerce")) == pytest.approx(4.82, abs=0.01)
    assert float(pd.to_numeric(html_rows[0].get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(0.28, abs=0.01)


def test_parse_gpre_corn_bids_html_normalizes_plain_location_labels_and_includes_central_city() -> None:
    html_blob = """
    <html><body>
    <table>
      <tr><td>Central City, Nebraska</td></tr>
      <tr>
        <td>Corn</td><td>Apr 2026</td>
        <td>4.00</td><td>@C6K</td><td title="Basis Month: @C6K">-0.20</td>
      </tr>
      <tr><td>Wood River, Nebraska</td></tr>
      <tr>
        <td>Corn</td><td>May 2026</td>
        <td>4.10</td><td>@C6N</td><td title="Basis Month: @C6N">-0.18</td>
      </tr>
    </table>
    </body></html>
    """.strip()

    rows = parse_gpre_corn_bids_html(
        html_blob,
        as_of_date=date(2026, 4, 16),
        source_url="https://grain.gpreinc.com/location/central-city-ne/",
    )

    locations = {str(rec.get("location") or "").strip() for rec in rows}
    assert {"Central City", "Wood River"} <= locations
    central_city = next(rec for rec in rows if str(rec.get("location") or "").strip() == "Central City")
    assert float(pd.to_numeric(central_city.get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.20, abs=1e-9)
    assert central_city.get("region") == "nebraska"


def test_parse_gpre_corn_bids_html_reads_location_specific_layout_with_selected_location() -> None:
    html_blob = """
    <html><body>
      <select name="Location">
        <option value="3" selected="selected">Central City</option>
        <option value="17">Madison</option>
      </select>
      <table name="cashbids-data-table">
        <tr><th>Delivery</th><th>Cash Price</th><th>Basis</th><th>Futures Month</th></tr>
        <tr>
          <td>History Apr 30, 2026</td>
          <td><script>displayNumber(-201.3784,2);</script></td>
          <td><script>displayNumber(-205.8384,2);</script></td>
          <td><a class="basisMonth">@C6K</a></td>
        </tr>
      </table>
      <script>// NoScrapeOffset: -205.6284</script>
    </body></html>
    """.strip()

    rows = parse_gpre_corn_bids_html(
        html_blob,
        as_of_date=date(2026, 4, 17),
        source_url="https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["location"] == "Central City"
    assert row["region"] == "nebraska"
    assert row["delivery_end"] == date(2026, 4, 30)
    assert float(pd.to_numeric(row.get("cash_price"), errors="coerce")) == pytest.approx(4.25, abs=0.01)
    assert float(pd.to_numeric(row.get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.21, abs=0.01)
    assert row["symbol"] == "@C6K"


def test_dedupe_gpre_corn_bids_rows_prefers_location_specific_source_for_duplicate_delivery() -> None:
    deduped = market_service._dedupe_gpre_corn_bids_rows(
        [
            {
                "location": "Central City",
                "region": "nebraska",
                "delivery_label": "Apr 30, 2026",
                "delivery_end": date(2026, 4, 30),
                "cash_price": 4.01,
                "basis_usd_per_bu": -0.18,
                "basis_cents_per_bu": -18.0,
                "symbol": "@C6K",
                "source_url": "https://grain.gpreinc.com/index.cfm",
                "candidate_source_urls": ["https://grain.gpreinc.com/index.cfm"],
            },
            {
                "location": "Central City",
                "region": "nebraska",
                "delivery_label": "Apr 30, 2026",
                "delivery_end": date(2026, 4, 30),
                "cash_price": 4.26,
                "basis_usd_per_bu": -0.21,
                "basis_cents_per_bu": -21.0,
                "symbol": "@C6K",
                "source_url": "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19",
                "candidate_source_urls": ["https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19"],
            },
        ]
    )

    assert len(deduped) == 1
    row = deduped[0]
    assert float(pd.to_numeric(row.get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.21, abs=1e-9)
    assert str(row.get("source_url") or "").endswith("theLocation=3&layout=19")


def test_fetch_gpre_corn_bids_html_payload_unions_best_candidate_sources_for_split_locations(monkeypatch: pytest.MonkeyPatch) -> None:
    entry_url = "https://gpreinc.com/corn-bids/"
    grain_url = "https://grain.gpreinc.com/index.cfm"
    central_city_url = "https://gpreinc.com/location/central-city-ne/"
    entry_html = f"""
    <html><body>
      <a href="{grain_url}">Grain</a>
      <a href="{central_city_url}">Central City</a>
    </body></html>
    """.strip()
    grain_html = """
    <html><body>
      <div>Last Updated 4/16/26</div>
      <table>
        <tr><td><b>Wood River</b></td></tr>
        <tr><td>Corn</td><td>Apr 2026</td><td>4.02</td><td>@C6K</td><td title="Basis Month: @C6K">-0.19</td></tr>
        <tr><td><b>York</b></td></tr>
        <tr><td>Corn</td><td>Apr 2026</td><td>4.01</td><td>@C6K</td><td title="Basis Month: @C6K">-0.21</td></tr>
      </table>
    </body></html>
    """.strip()
    central_city_html = """
    <html><body>
      <div>Last Updated 4/16/26</div>
      <table>
        <tr><td>Central City, Nebraska</td></tr>
        <tr><td>Corn</td><td>Apr 2026</td><td>4.00</td><td>@C6K</td><td title="Basis Month: @C6K">-0.20</td></tr>
      </table>
    </body></html>
    """.strip()

    class _FakeResponse:
        def __init__(self, payload: str) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return self._payload.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    class _FakeOpener:
        def __init__(self, payloads: dict[str, str]) -> None:
            self._payloads = payloads

        def open(self, req, timeout=None):
            del timeout
            url = str(getattr(req, "full_url", req))
            if url not in self._payloads:
                raise RuntimeError(f"unexpected url {url}")
            return _FakeResponse(self._payloads[url])

    monkeypatch.setattr(
        market_service.urllib.request,
        "build_opener",
        lambda *args, **kwargs: _FakeOpener(
            {
                entry_url: entry_html,
                grain_url: grain_html,
                central_city_url: central_city_html,
            }
        ),
    )

    payload = market_service._fetch_gpre_corn_bids_html_payload(timeout_seconds=0.01)

    assert payload["status"] == "ok"
    assert bool(payload.get("selected_from_union"))
    assert set(payload.get("candidate_source_urls") or []) == {central_city_url, grain_url}
    assert {str(rec.get("location") or "").strip() for rec in list(payload.get("rows") or [])} == {
        "Central City",
        "Wood River",
        "York",
    }


def test_fetch_gpre_corn_bids_html_payload_extracts_location_specific_urls_from_entry_js(monkeypatch: pytest.MonkeyPatch) -> None:
    entry_url = "https://gpreinc.com/corn-bids/"
    child_theme_url = "https://gpreinc.com/wp-content/themes/gpre/js/child-theme.min.js?ver=2023.0.1753284682"
    grain_url = "https://grain.gpreinc.com/index.cfm"
    central_city_url = "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19"
    entry_html = f"""
    <html><body>
      <a href="{grain_url}">Cash Bids</a>
      <script src="{child_theme_url}"></script>
    </body></html>
    """.strip()
    child_theme_js = """
    document.getElementById("bidsLocationSelect").addEventListener("change",(function(){
      if("central_city"===this.value){var e="https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19";Gt.href=e}
    }));
    """.strip()
    grain_html = """
    <html><body>
      <div>Last Updated 4/16/26</div>
      <table>
        <tr><td><b>Wood River</b></td></tr>
        <tr><td>Corn</td><td>Apr 2026</td><td>4.02</td><td>@C6K</td><td title="Basis Month: @C6K">-0.19</td></tr>
      </table>
    </body></html>
    """.strip()
    central_city_html = """
    <html><body>
      <select name="Location">
        <option value="3" selected="selected">Central City</option>
        <option value="17">Madison</option>
      </select>
      <table name="cashbids-data-table">
        <tr><th>Delivery</th><th>Cash Price</th><th>Basis</th><th>Futures Month</th></tr>
        <tr>
          <td>History Apr 30, 2026</td>
          <td><script>displayNumber(-201.3784,2);</script></td>
          <td><script>displayNumber(-205.8384,2);</script></td>
          <td><a class="basisMonth">@C6K</a></td>
        </tr>
      </table>
      <script>// NoScrapeOffset: -205.6284</script>
    </body></html>
    """.strip()

    class _FakeResponse:
        def __init__(self, payload: str) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return self._payload.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    class _FakeOpener:
        def __init__(self, payloads: dict[str, str]) -> None:
            self._payloads = payloads

        def open(self, req, timeout=None):
            del timeout
            url = str(getattr(req, "full_url", req))
            if url not in self._payloads:
                raise RuntimeError(f"unexpected url {url}")
            return _FakeResponse(self._payloads[url])

    monkeypatch.setattr(
        market_service.urllib.request,
        "build_opener",
        lambda *args, **kwargs: _FakeOpener(
            {
                entry_url: entry_html,
                child_theme_url: child_theme_js,
                grain_url: grain_html,
                central_city_url: central_city_html,
            }
        ),
    )

    payload = market_service._fetch_gpre_corn_bids_html_payload(timeout_seconds=0.01)

    assert payload["status"] == "ok"
    assert central_city_url in list(payload.get("candidate_source_urls") or [])
    assert {str(rec.get("location") or "").strip() for rec in list(payload.get("rows") or [])} == {
        "Central City",
        "Wood River",
    }


def test_fetch_gpre_corn_bids_snapshot_preserves_union_rows_from_low_level_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        market_service,
        "_fetch_gpre_corn_bids_html_payload",
        lambda **kwargs: {
            "status": "ok",
            "source_url": "https://grain.gpreinc.com/index.cfm",
            "entry_url": "https://gpreinc.com/corn-bids/",
            "html_text": "<html><body><div>best candidate only</div></body></html>",
            "rows": [
                {
                    "location": "Central City",
                    "region": "nebraska",
                    "delivery_label": "Apr 30, 2026",
                    "delivery_end": date(2026, 4, 30),
                    "cash_price": 4.26,
                    "basis_usd_per_bu": -0.21,
                    "basis_cents_per_bu": -21.0,
                    "symbol": "@C6K",
                    "source_url": "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19",
                    "candidate_source_urls": [
                        "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19"
                    ],
                },
                {
                    "location": "Wood River",
                    "region": "nebraska",
                    "delivery_label": "Apr 2026",
                    "delivery_end": date(2026, 4, 30),
                    "cash_price": 4.02,
                    "basis_usd_per_bu": -0.19,
                    "basis_cents_per_bu": -19.0,
                    "symbol": "@C6K",
                    "source_url": "https://grain.gpreinc.com/index.cfm",
                    "candidate_source_urls": ["https://grain.gpreinc.com/index.cfm"],
                },
            ],
            "candidate_source_urls": [
                "https://grain.gpreinc.com/index.cfm",
                "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19",
            ],
            "selected_from_union": True,
        },
    )

    summary = market_service.fetch_gpre_corn_bids_snapshot(as_of_date=date(2026, 4, 17), timeout_seconds=0.01)

    assert summary["status"] == "ok"
    assert {str(rec.get("location") or "").strip() for rec in list(summary.get("rows") or [])} == {
        "Central City",
        "Wood River",
    }


def test_download_gpre_corn_bids_snapshot_preserves_union_rows_from_low_level_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_download_union_rows")
    try:
        monkeypatch.setattr(
            market_service,
            "_fetch_gpre_corn_bids_html_payload",
            lambda **kwargs: {
                "status": "ok",
                "source_url": "https://grain.gpreinc.com/index.cfm",
                "entry_url": "https://gpreinc.com/corn-bids/",
                "html_text": "<html><body><div>best candidate only</div></body></html>",
                "rows": [
                    {
                        "location": "Central City",
                        "region": "nebraska",
                        "delivery_label": "Apr 30, 2026",
                        "delivery_end": date(2026, 4, 30),
                        "cash_price": 4.26,
                        "basis_usd_per_bu": -0.21,
                        "basis_cents_per_bu": -21.0,
                        "symbol": "@C6K",
                        "source_url": "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19",
                        "candidate_source_urls": [
                            "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19"
                        ],
                    },
                    {
                        "location": "Wood River",
                        "region": "nebraska",
                        "delivery_label": "Apr 2026",
                        "delivery_end": date(2026, 4, 30),
                        "cash_price": 4.02,
                        "basis_usd_per_bu": -0.19,
                        "basis_cents_per_bu": -19.0,
                        "symbol": "@C6K",
                        "source_url": "https://grain.gpreinc.com/index.cfm",
                        "candidate_source_urls": ["https://grain.gpreinc.com/index.cfm"],
                    },
                ],
                "candidate_source_urls": [
                    "https://grain.gpreinc.com/index.cfm",
                    "https://grain.gpreinc.com/index.cfm?show=11&mid=3&theLocation=3&layout=19",
                ],
                "selected_from_union": True,
            },
        )

        summary = market_service.download_gpre_corn_bids_snapshot(
            ticker_root=tmp_path / "GPRE",
            as_of_date=date(2026, 4, 17),
            timeout_seconds=0.01,
        )

        assert summary["status"] == "ok"
        assert {str(rec.get("location") or "").strip() for rec in list(summary.get("rows") or [])} == {
            "Central City",
            "Wood River",
        }
        csv_path = Path(summary["csv_path"])
        csv_text = csv_path.read_text(encoding="utf-8")
        assert "Central City" in csv_text
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_fetch_gpre_corn_bids_snapshot_falls_back_gracefully(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        market_service,
        "_fetch_gpre_corn_bids_html_payload",
        lambda **kwargs: {
            "status": "unavailable",
            "source_url": "https://grain.gpreinc.com/index.cfm",
            "entry_url": "https://gpreinc.com/corn-bids/",
            "error": "network blocked",
            "html_text": "",
            "rows": [],
        },
    )
    snap = fetch_gpre_corn_bids_snapshot(as_of_date=date(2026, 4, 2), timeout_seconds=0.01)
    assert str(snap.get("status") or "") == "unavailable"
    assert "network blocked" in str(snap.get("error") or "")


def test_download_gpre_corn_bids_snapshot_writes_html_csv_and_archive_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_download")
    html_blob = """
    <html><body>
    <table>
      <tr><td><b>Madison</b></td></tr>
      <tr>
        <td>Corn</td><td>Apr 2026</td>
        <td><script>displayNumber(38.2425,2);</script></td>
        <td>@C6K</td>
        <td title="Basis Month: @C6K"><script>displayNumber(33.7025,2);</script></td>
      </tr>
      <tr><td><b>Wood River</b></td></tr>
      <tr>
        <td>Corn</td><td>May 2026</td>
        <td><script>displayNumber(37.7125,2);</script></td>
        <td>@C6N</td>
        <td title="Basis Month: @C6N"><script>displayNumber(32.9925,2);</script></td>
      </tr>
    </table>
    <script>var cfg = { NoScrapeOffset: 33.4225 };</script>
    </body></html>
    """.strip()

    monkeypatch.setattr(
        market_service,
        "_fetch_gpre_corn_bids_html_payload",
        lambda **kwargs: {
            "status": "ok",
            "source_url": "https://grain.gpreinc.com/index.cfm",
            "entry_url": "https://gpreinc.com/corn-bids/",
            "html_text": html_blob,
            "rows": [{"location": "Madison"}],
        },
    )

    try:
        summary = download_gpre_corn_bids_snapshot(tmp_path, as_of_date=date(2026, 4, 2), timeout_seconds=0.01)
        assert str(summary.get("status") or "") == "ok"
        html_path = Path(summary["html_path"])
        csv_path = Path(summary["csv_path"])
        assert html_path.exists()
        assert csv_path.exists()
        assert html_path.parent.name == "corn_bids"
        assert csv_path.parent == html_path.parent
        assert "Madison" in html_path.read_text(encoding="utf-8")
        saved_df = pd.read_csv(csv_path)
        assert {"location", "delivery_label", "basis_usd_per_bu"} <= set(saved_df.columns)
        assert "Madison" in set(saved_df["location"].astype(str))
        assert "Wood River" in set(saved_df["location"].astype(str))
        manifest_path = Path(summary["manifest_path"])
        archive_raw_path = Path(summary["archive_raw_path"])
        archive_parsed_path = Path(summary["archive_parsed_path"])
        assert manifest_path.exists()
        assert archive_raw_path.exists()
        assert archive_parsed_path.exists()
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshots = list(manifest_payload.get("snapshots") or [])
        assert len(snapshots) == 1
        assert snapshots[0]["snapshot_date"] == "2026-04-02"
        assert snapshots[0]["raw_relpath"].startswith("raw_snapshots/2026-04-02/")
        assert snapshots[0]["parsed_relpath"].startswith("parsed_snapshots/2026-04-02/")
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_corn_bids_manifest_entries_normalize_windows_relpaths() -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_manifest_normalize")
    try:
        bids_dir = tmp_path / "corn_bids"
        bids_dir.mkdir(parents=True, exist_ok=True)
        (bids_dir / "raw_snapshots" / "2026-04-09").mkdir(parents=True, exist_ok=True)
        (bids_dir / "parsed_snapshots" / "2026-04-09").mkdir(parents=True, exist_ok=True)
        manifest_path = bids_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "snapshots": [
                        {
                            "snapshot_date": "2026-04-09",
                            "raw_relpath": "raw_snapshots\\2026-04-09\\grain_gpre_home.html",
                            "parsed_relpath": "parsed_snapshots\\2026-04-09\\gpre_corn_bids_snapshot.csv",
                            "source_kind": "legacy_local_bootstrap",
                        }
                    ]
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        entries = market_service._gpre_corn_bids_manifest_entries(bids_dir)
        assert len(entries) == 1
        assert entries[0]["raw_relpath"] == "raw_snapshots/2026-04-09/grain_gpre_home.html"
        assert entries[0]["parsed_relpath"] == "parsed_snapshots/2026-04-09/gpre_corn_bids_snapshot.csv"
        rewritten = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert rewritten["snapshots"][0]["raw_relpath"] == "raw_snapshots/2026-04-09/grain_gpre_home.html"
        assert rewritten["snapshots"][0]["parsed_relpath"] == "parsed_snapshots/2026-04-09/gpre_corn_bids_snapshot.csv"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_local_gpre_corn_bids_snapshot_bootstraps_archive_from_latest() -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_local")
    html_blob = """
    <html><body>
    <table>
      <tr><td><b>Madison</b></td></tr>
      <tr>
        <td>Corn</td><td>Apr 2026</td>
        <td><script>displayNumber(38.2425,2);</script></td>
        <td>@C6K</td>
        <td title="Basis Month: @C6K"><script>displayNumber(33.7025,2);</script></td>
      </tr>
    </table>
    <script>var cfg = { NoScrapeOffset: 33.4225 };</script>
    </body></html>
    """.strip()
    try:
        corn_bids_dir = tmp_path / "corn_bids"
        corn_bids_dir.mkdir(parents=True, exist_ok=True)
        html_path = corn_bids_dir / "grain_gpre_home.html"
        html_path.write_text(html_blob, encoding="utf-8")
        snap = market_service._load_local_gpre_corn_bids_snapshot(
            ticker_root=tmp_path,
            as_of_date=date(2026, 4, 2),
        )
        assert str(snap.get("status") or "") == "ok"
        manifest_path = tmp_path / "corn_bids" / "manifest.json"
        assert manifest_path.exists()
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshots = list(manifest_payload.get("snapshots") or [])
        assert len(snapshots) == 1
        assert snapshots[0]["snapshot_date"] == "2026-04-02"
        assert str(snap.get("selection_rule") or "") in {"latest_snapshot_on_or_before_as_of", "legacy_latest_file"}
        assert str(snap.get("source_kind") or "") in {"archived_parsed_csv", "archived_raw_html", "local_html"}
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_local_gpre_corn_bids_snapshot_does_not_use_late_legacy_file_for_historical_quarter() -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_late_legacy_file")
    try:
        bids_dir = tmp_path / "corn_bids"
        bids_dir.mkdir(parents=True, exist_ok=True)
        html_path = bids_dir / "grain_gpre_home.html"
        html_path.write_text(
            """
            <html><body>
            <div>Last Updated 4/9/26</div>
            <table>
              <tr><td><b>Central City</b></td></tr>
              <tr>
                <td>Corn</td><td>Apr 2026</td>
                <td>4.00</td><td>@C6K</td><td title="Basis Month: @C6K">-0.20</td>
              </tr>
            </table>
            </body></html>
            """.strip(),
            encoding="utf-8",
        )
        snap = market_service._load_local_gpre_corn_bids_snapshot(
            ticker_root=tmp_path,
            as_of_date=date(2026, 4, 10),
            target_date=date(2026, 3, 31),
            target_quarter_end=date(2026, 3, 31),
            selection_mode="historical_quarter",
        )
        assert str(snap.get("status") or "") == "unavailable"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_sync_market_cache_refresh_triggers_gpre_corn_bids_download(monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_sync")
    calls: list[tuple[str, bool]] = []

    class _Profile:
        market_data_enabled = True

    monkeypatch.setattr(market_service, "_enabled_sources_for_profile", lambda profile: tuple())

    def _fake_refresh(ticker_root: Path, *, refresh: bool) -> dict[str, object]:
        calls.append((str(ticker_root), bool(refresh)))
        return {"status": "ok"}

    monkeypatch.setattr(market_service, "_refresh_gpre_corn_bids_download", _fake_refresh)
    try:
        summary = sync_market_cache(tmp_path, "GPRE", profile=_Profile(), sync_raw=True, refresh=True, reparse=False)
        assert calls
        assert Path(calls[0][0]).name == "GPRE"
        assert calls[0][1] is True
        assert summary.export_rows == 0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_market_input_fingerprint_changes_when_legacy_corn_bids_or_local_usda_files_change() -> None:
    tmp_path = _local_test_dir("gpre_market_input_fingerprint")
    try:
        cache_dir = tmp_path / "sec_cache" / "GPRE"
        cache_dir.mkdir(parents=True, exist_ok=True)
        ticker_root = tmp_path / "GPRE"
        ticker_root.mkdir(parents=True, exist_ok=True)

        _write_gpre_corn_bids_archive_snapshot(
            ticker_root,
            snapshot_date=date(2026, 4, 11),
            rows=[
                {
                    "location": "Central City",
                    "region": "nebraska",
                    "delivery_label": "Apr 2026",
                    "delivery_end": date(2026, 4, 30),
                    "basis_usd_per_bu": -0.21,
                }
            ],
        )

        class _Profile:
            enabled_market_sources = ("nwer", "ams_3617")

        first = market_service.market_input_fingerprint(
            cache_dir,
            "GPRE",
            profile=_Profile(),
            include_sidecars=False,
        )

        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 4, 16),
            rows=[
                {
                    "location": "Central City",
                    "region": "nebraska",
                    "delivery_label": "Apr 2026",
                    "delivery_end": date(2026, 4, 30),
                    "basis_usd_per_bu": -0.18,
                }
            ],
        )

        second = market_service.market_input_fingerprint(
            cache_dir,
            "GPRE",
            profile=_Profile(),
            include_sidecars=False,
        )

        daily_dir = ticker_root / "USDA_daily_data"
        daily_dir.mkdir(parents=True, exist_ok=True)
        (daily_dir / "ams_3617_2026-04-16.pdf").write_bytes(b"%PDF-1.4 fake daily report")

        third = market_service.market_input_fingerprint(
            cache_dir,
            "GPRE",
            profile=_Profile(),
            include_sidecars=False,
        )

        assert first["fingerprint"] != second["fingerprint"]
        assert second["fingerprint"] != third["fingerprint"]
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_market_export_rows_refreshes_when_market_input_fingerprint_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tmp_path = _local_test_dir("gpre_export_fingerprint_refresh")
    try:
        cache_dir = tmp_path / "sec_cache" / "GPRE"
        cache_dir.mkdir(parents=True, exist_ok=True)
        ticker_root = tmp_path / "GPRE"
        ticker_root.mkdir(parents=True, exist_ok=True)
        cache_root = market_service.resolve_market_cache_root(cache_dir)
        ensure_market_cache_dirs(cache_root)

        export_path = market_service.export_rows_path(cache_root, "GPRE")
        pd.DataFrame(
            [
                {
                    "observation_date": pd.Timestamp("2026-04-10"),
                    "quarter": pd.Timestamp("2026-06-30"),
                    "aggregation_level": "observation",
                    "source_file": "demo.csv",
                    "source_type": "provider_demo",
                    "market_family": "corn_basis",
                    "series_key": "corn_basis_nebraska",
                    "instrument": "Corn basis",
                    "region": "nebraska",
                    "contract_tenor": "",
                    "price_value": -0.21,
                    "unit": "$/bushel",
                    "parsed_text": "demo",
                    "quality": "high",
                    "_obs_count": 1,
                }
            ]
        ).to_parquet(export_path, index=False)

        manifest_path = market_service._market_export_inputs_manifest_path(cache_root, "GPRE")
        manifest_path.write_text(json.dumps({"input_fingerprint": "old-fingerprint"}), encoding="utf-8")

        class _Profile:
            enabled_market_sources = ("ams_3617",)

        monkeypatch.setattr(
            market_service,
            "market_input_fingerprint",
            lambda *args, **kwargs: {"fingerprint": "new-fingerprint", "tracked_paths": []},
        )

        calls: list[dict[str, object]] = []

        def _fake_sync(*args, **kwargs):
            calls.append(dict(kwargs))
            return market_service.SyncSummary(
                sources_enabled=("ams_3617",),
                export_rows=1,
                export_path=export_path,
            )

        monkeypatch.setattr(market_service, "sync_market_cache", _fake_sync)

        rows = load_market_export_rows(cache_dir, "GPRE", profile=_Profile(), ensure_cache=True)

        assert calls
        assert calls[0]["sync_raw"] is True
        assert calls[0]["refresh"] is False
        assert calls[0]["reparse"] is True
        assert len(rows) == 1
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_build_gpre_basis_proxy_model_returns_quarterly_table_weights_and_metrics() -> None:
    quarters = pd.date_range("2023-03-31", periods=12, freq="QE")
    rows: list[dict[str, object]] = []
    reported: dict[date, float] = {}
    denominator_policy: dict[date, str] = {}
    region_weights_true = {
        "nebraska": 0.40,
        "iowa_west": 0.25,
        "minnesota": 0.15,
        "indiana": 0.10,
        "illinois": 0.05,
        "iowa_east": 0.05,
    }
    region_offsets = {
        "nebraska": -0.30,
        "iowa_west": -0.22,
        "minnesota": -0.18,
        "indiana": -0.12,
        "illinois": -0.10,
        "iowa_east": -0.14,
    }
    for idx, ts in enumerate(quarters):
        qd = ts.date()
        ethanol = 1.45 + (0.02 * idx)
        futures = 4.20 + (0.03 * idx)
        gas = 3.10 + (0.04 * ((idx % 5) - 2))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=ethanol, source_type="nwer_pdf"))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=futures, source_type="nwer_pdf"))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=gas, source_type="nwer_pdf", unit="$/MMBtu"))
        weighted_basis_true = 0.0
        for region, base in region_offsets.items():
            basis_val = base + (0.01 * ((idx % 4) - 1))
            weighted_basis_true += region_weights_true[region] * basis_val
            rows.append(
                _parsed_row(
                    aggregation_level="quarter_avg",
                    observation_date=qd.isoformat(),
                    publication_date=qd.isoformat(),
                    quarter=qd.isoformat(),
                    market_family="corn_basis",
                    series_key=f"corn_basis_{region}",
                    instrument="Corn basis",
                    location=region.replace("_", " ").title(),
                    region=region,
                    price_value=basis_val,
                    unit="$/bushel",
                    source_type="ams_3617_pdf",
                )
            )
        baseline = ethanol - (futures / 2.9) - (0.028 * gas)
        reported[qd] = baseline - (weighted_basis_true / 2.9) + (0.005 if idx % 3 == 0 else -0.004)
        denominator_policy[qd] = "ethanol gallons sold"

    result = build_gpre_basis_proxy_model(
        rows,
        ticker_root=None,
        reported_margin_by_quarter=reported,
        denominator_policy_by_quarter=denominator_policy,
        as_of_date=date(2026, 4, 1),
    )

    quarterly_df = result["quarterly_df"]
    metrics_df = result["metrics_df"]
    leaderboard_df = result["leaderboard_df"]
    weights_df = result["weights_df"]

    assert not quarterly_df.empty
    assert len(quarterly_df) == 12
    assert {"train", "test"} == set(quarterly_df["train_test_flag"].astype(str))
    assert quarterly_df["reported_consolidated_crush_margin_usd_per_gal"].notna().all()
    assert quarterly_df["natural_gas_burden_usd_per_gal"].notna().all()
    assert quarterly_df["simple_market_proxy_usd_per_gal"].notna().all()
    assert quarterly_df["weighted_ethanol_benchmark_usd_per_gal"].notna().all()
    assert quarterly_df["weighted_ams_basis_usd_per_bu"].notna().all()
    assert quarterly_df["official_simple_proxy_usd_per_gal"].notna().all()
    assert quarterly_df["official_process_proxy_usd_per_gal"].notna().all()
    assert quarterly_df["gpre_proxy_official_usd_per_gal"].notna().all()
    assert {
        "process_inventory_gap_penalty_small_usd_per_gal",
        "process_inventory_gap_penalty_medium_usd_per_gal",
        "process_utilization_regime_blend_usd_per_gal",
        "process_utilization_regime_residual_usd_per_gal",
        "process_exec_inventory_combo_medium_usd_per_gal",
        "process_asymmetric_basis_passthrough_usd_per_gal",
        "process_residual_regime_locked_vs_disturbed_usd_per_gal",
        "process_gated_incumbent_vs_residual_usd_per_gal",
    } <= set(quarterly_df.columns)
    assert quarterly_df["weighted_basis_recommended_cents_per_bu"].notna().any()
    assert quarterly_df["basis_adjusted_recommended_usd_per_gal"].notna().any()
    assert quarterly_df["official_proxy_usd_per_gal"].notna().any()
    assert quarterly_df["approx_market_bridge_proxy_usd_per_gal"].notna().any()
    assert not metrics_df.empty
    assert {"full", "train", "test", "clean_reported_window", "diag_underlying"} <= set(metrics_df["split"].astype(str))
    assert {
        "mean_error",
        "sign_hit_rate",
        "q1_mae",
        "q2_mae",
        "q3_mae",
        "q4_mae",
        "q1_mean_error",
        "q2_mean_error",
        "q3_mean_error",
        "q4_mean_error",
        "top_miss_quarters",
    } <= set(metrics_df.columns)
    assert not leaderboard_df.empty
    assert {
        "model_key",
        "family",
        "clean_mae",
        "underlying_mae",
        "hybrid_score",
        "chosen",
        "test_mean_error",
        "test_corr",
        "test_sign_hit_rate",
        "q1_mae",
        "q2_mae",
        "q3_mae",
        "q4_mae",
        "q1_mean_error",
        "q2_mean_error",
        "q3_mean_error",
        "q4_mean_error",
        "material_diff_quarter_count_vs_official",
        "selection_guard_reason",
        "top_miss_quarters",
    } <= set(leaderboard_df.columns)
    assert int(leaderboard_df["chosen"].astype(bool).sum()) == 1
    assert not weights_df.empty
    opt_weights = weights_df[weights_df["model_key"].astype(str) == "optimized_weights"].copy()
    assert not opt_weights.empty
    assert float(opt_weights["weight"].sum()) == pytest.approx(1.0, abs=1e-6)
    assert float(opt_weights["weight"].min()) >= -1e-9
    assert float(opt_weights["weight"].max()) <= 0.500001
    assert str(result["recommended_model_key"] or "") == "plant_count_weighted"
    assert str(result["gpre_proxy_model_key"] or "").strip()
    assert np.allclose(
        pd.to_numeric(quarterly_df["simple_market_proxy_usd_per_gal"], errors="coerce"),
        pd.to_numeric(quarterly_df["official_simple_proxy_usd_per_gal"], errors="coerce"),
        atol=1e-9,
        rtol=0.0,
        equal_nan=False,
    )
    assert np.allclose(
        pd.to_numeric(quarterly_df["official_process_proxy_usd_per_gal"], errors="coerce"),
        pd.to_numeric(quarterly_df["official_simple_proxy_usd_per_gal"], errors="coerce"),
        atol=1e-9,
        rtol=0.0,
        equal_nan=False,
    )
    assert (
        pd.to_numeric(quarterly_df["gpre_proxy_official_usd_per_gal"], errors="coerce")
        .sub(pd.to_numeric(quarterly_df["official_simple_proxy_usd_per_gal"], errors="coerce"))
        .abs()
        .gt(1e-9)
        .any()
    )
    chosen_row = leaderboard_df[leaderboard_df["chosen"].astype(bool)].iloc[0]
    assert (
        pd.to_numeric(leaderboard_df["material_diff_quarter_count_vs_official"], errors="coerce")
        .fillna(0)
        .ge(3)
        .any()
    )
    assert str(chosen_row.get("selection_guard_reason") or "").strip()
    assert str(result.get("production_decision_story") or "").strip()
    assert str(result.get("selection_vs_promotion_explanation") or "").strip()
    assert str(result["recommended_formula"] or "").startswith(
        "proxy = weighted_ethanol_benchmark - (cbot_corn_front + weighted_ams_basis) / 2.9 - (0.028 * nymex_gas)"
    )
    assert "Active-capacity weighted" in str(result["official_weighting_method"] or "")
    assert "Weighted ethanol benchmark" in str(result["official_ethanol_method"] or "")
    assert "dated GPRE plant bids" in str(result["official_basis_method"] or "")
    assert "0.028" in str(result["official_gas_method"] or "")
    assert "fallback" in str(result["official_fallback_policy"] or "").lower()
    assert np.allclose(
        pd.to_numeric(quarterly_df["process_capacity_weighted_basis_strict_usd_per_gal"], errors="coerce"),
        pd.to_numeric(quarterly_df["basis_adjusted_capacity_usd_per_gal"], errors="coerce"),
        atol=1e-9,
        rtol=0.0,
        equal_nan=True,
    )
    assert str(result.get("official_market_summary") or "") == "Official market model | Representative quarter: 2025-Q4."
    summary_lower = str(result["summary_markdown"] or "").lower()
    assert "selection vs promotion" in summary_lower
    assert "production decision story" in summary_lower
    assert "top miss quarters" in str(result["summary_markdown"] or "").lower()
    assert result["official_market_rows"]
    official_weight_rows = weights_df[weights_df["model_key"].astype(str) == "official_market_model"].copy()
    assert not official_weight_rows.empty
    official_weight_sums = (
        official_weight_rows.assign(weight_num=pd.to_numeric(official_weight_rows["weight"], errors="coerce"))
        .groupby("quarter", dropna=False)["weight_num"]
        .sum()
    )
    assert official_weight_sums.notna().all()
    assert all(float(val) == pytest.approx(1.0, abs=1e-6) for val in official_weight_sums.tolist())
    official_market_rows_df = pd.DataFrame(result["official_market_rows"])
    assert not official_market_rows_df.empty
    assert pd.to_numeric(official_market_rows_df["active_capacity_mmgy"], errors="coerce").notna().any()
    assert pd.to_numeric(official_market_rows_df["ethanol_value_usd_per_gal"], errors="coerce").notna().any()
    assert pd.to_numeric(official_market_rows_df["basis_value_cents_per_bu"], errors="coerce").notna().any()
    assert official_market_rows_df["basis_series_label"].astype(str).str.strip().ne("").any()
    latest_official_quarter = official_weight_rows["quarter"].astype(str).max()
    latest_official_weights = official_weight_rows[official_weight_rows["quarter"].astype(str) == latest_official_quarter].copy()
    latest_official_weights["weight_num"] = pd.to_numeric(latest_official_weights["weight"], errors="coerce")
    latest_weight_by_component = {
        str(component or "").strip().lower(): float(weight_num)
        for component, weight_num in zip(latest_official_weights["region"], latest_official_weights["weight_num"])
        if pd.notna(weight_num)
    }
    assert latest_weight_by_component.get("nebraska", 0.0) > (3.0 / 8.0)
    assert latest_weight_by_component.get("minnesota", 1.0) < (1.0 / 8.0)
    hedge_study = result.get("hedge_style_study")
    assert isinstance(hedge_study, dict)
    proxy_implied_results = result.get("proxy_implied_results")
    assert isinstance(proxy_implied_results, dict)
    assert list(proxy_implied_results.get("frame_order") or []) == [
        "prior_quarter",
        "quarter_open",
        "current_qtd",
        "next_quarter_thesis",
    ]
    experimental_signal_audit = result.get("experimental_signal_audit")
    experimental_candidate_comparison_df = result.get("experimental_candidate_comparison_df")
    assert isinstance(experimental_signal_audit, dict)
    assert isinstance(experimental_candidate_comparison_df, pd.DataFrame)
    assert len(list(experimental_signal_audit.get("signal_rows") or [])) == 10
    assert {
        "model_key",
        "candidate_method_family",
        "signal_dependency_note",
        "concentration_note",
        "clean_window_mae",
        "underlying_window_mae",
        "hybrid_score",
        "hard_quarter_mae",
        "preview_mae",
        "preview_max_error",
        "preview_quality_class",
        "walk_forward_tail_mae",
        "signal_coverage_quarters",
        "signal_coverage_ratio",
        "forward_usability_rating",
        "complexity_rating",
        "top_miss_quarters",
        "top_improved_quarters_vs_incumbent",
        "top_worsened_quarters_vs_incumbent",
        "promotion_status",
        "promotion_reason_human",
    } <= set(experimental_candidate_comparison_df.columns)
    assert {
        str(result.get("incumbent_baseline_model_key") or ""),
        "process_inventory_gap_penalty_small",
        "process_inventory_gap_penalty_medium",
        "process_utilization_regime_blend",
        "process_utilization_regime_residual",
        "process_exec_inventory_combo_medium",
        "process_asymmetric_basis_passthrough",
        "process_market_process_ensemble_35_65",
        "process_locked_share_asymmetric_passthrough",
        "process_prior_gap_carryover_small",
        "process_prior_disturbance_carryover",
        "process_residual_regime_locked_vs_disturbed",
        "process_gated_incumbent_vs_residual",
    } <= set(experimental_candidate_comparison_df["model_key"].astype(str))
    assert experimental_candidate_comparison_df["preview_quality_class"].astype(str).str.strip().ne("").all()
    assert experimental_candidate_comparison_df["promotion_reason_human"].astype(str).str.strip().ne("").all()
    assert experimental_candidate_comparison_df["candidate_method_family"].astype(str).str.strip().ne("").all()
    assert experimental_candidate_comparison_df["signal_dependency_note"].astype(str).str.strip().ne("").all()
    assert experimental_candidate_comparison_df["concentration_note"].astype(str).isin({"broad", "mixed", "mostly_1_2_quarters"}).all()
    assert experimental_candidate_comparison_df["forward_usability_rating"].astype(str).isin({"high", "medium", "low"}).all()
    assert experimental_candidate_comparison_df["complexity_rating"].astype(str).isin({"low", "moderate", "high"}).all()
    coproduct_experimental_candidate_comparison_df = result.get("coproduct_experimental_candidate_comparison_df")
    assert isinstance(coproduct_experimental_candidate_comparison_df, pd.DataFrame)
    assert not coproduct_experimental_candidate_comparison_df.empty
    assert {
        "model_key",
        "method_label",
        "rule",
        "clean_window_mae",
        "underlying_window_mae",
        "hybrid_score",
        "hard_quarter_mae",
        "sign_accuracy",
        "avg_abs_diff_vs_official",
        "walk_forward_tail_mae",
        "forward_usability_rating",
        "complexity_rating",
        "low_coverage_mae",
        "coverage_sensitivity_delta",
        "comparison_only",
        "eligible_official",
        "status",
    } <= set(coproduct_experimental_candidate_comparison_df.columns)
    assert set(coproduct_experimental_candidate_comparison_df["model_key"].astype(str)) == {
        "simple_plus_10pct_credit",
        "simple_plus_15pct_credit",
        "simple_plus_20pct_credit",
        "simple_plus_25pct_credit",
        "simple_plus_30pct_credit",
        "simple_plus_10pct_coverage_credit",
        "simple_plus_20pct_coverage_credit",
        "simple_plus_30pct_coverage_credit",
        "simple_plus_25pct_credit_less_2c",
        "simple_plus_30pct_coverage_credit_less_2c",
    }
    assert len(coproduct_experimental_candidate_comparison_df) == 10
    assert coproduct_experimental_candidate_comparison_df["comparison_only"].eq(True).all()
    assert coproduct_experimental_candidate_comparison_df["eligible_official"].eq(False).all()
    assert coproduct_experimental_candidate_comparison_df["status"].astype(str).eq("comparison only").all()
    assert coproduct_experimental_candidate_comparison_df["forward_usability_rating"].astype(str).isin({"high", "medium", "low"}).all()
    assert coproduct_experimental_candidate_comparison_df["complexity_rating"].astype(str).isin({"low", "moderate", "high"}).all()
    legacy_coproduct_reference_row = result.get("coproduct_experimental_legacy_reference_row")
    assert isinstance(legacy_coproduct_reference_row, dict)
    assert str(result.get("coproduct_experimental_legacy_reference_model_key") or "") == "simple_plus_half_credit"
    assert str(legacy_coproduct_reference_row.get("model_key") or "") == "simple_plus_half_credit"
    assert "simple_plus_half_credit" not in set(coproduct_experimental_candidate_comparison_df["model_key"].astype(str))
    assert str(result.get("best_coproduct_experimental_historical_model_key") or "").strip()
    assert str(result.get("best_coproduct_experimental_compromise_model_key") or "").strip()
    assert str(result.get("best_coproduct_experimental_forward_model_key") or "").strip()
    assert str(result.get("best_coproduct_experimental_model_key") or "").strip()
    assert str(result.get("best_coproduct_experimental_model_key") or "") == str(
        result.get("best_coproduct_experimental_compromise_model_key") or ""
    )
    coproduct_experimental_frame_values = result.get("coproduct_experimental_frame_values")
    assert isinstance(coproduct_experimental_frame_values, dict)
    best_coproduct_model_key = str(result.get("best_coproduct_experimental_model_key") or "")
    assert best_coproduct_model_key in coproduct_experimental_frame_values
    assert "simple_plus_half_credit" in coproduct_experimental_frame_values
    assert {
        "prior_quarter",
        "quarter_open",
        "current_qtd",
        "next_quarter_thesis",
    } <= set((coproduct_experimental_frame_values.get(best_coproduct_model_key) or {}).keys())
    fractional_row = coproduct_experimental_candidate_comparison_df[
        coproduct_experimental_candidate_comparison_df["model_key"].astype(str) == "simple_plus_25pct_credit_less_2c"
    ].iloc[0].to_dict()
    assert pd.to_numeric(fractional_row.get("friction_offset_usd_per_gal"), errors="coerce") == pytest.approx(0.02)
    formula_check_df = quarterly_df[
        pd.to_numeric(quarterly_df.get("official_simple_proxy_usd_per_gal"), errors="coerce").notna()
        & pd.to_numeric(quarterly_df.get("coproduct_approximate_credit_usd_per_gal"), errors="coerce").notna()
        & pd.to_numeric(quarterly_df.get("coproduct_simple_plus_25pct_credit_less_2c_usd_per_gal"), errors="coerce").notna()
        & pd.to_numeric(quarterly_df.get("coproduct_credit_coverage_ratio"), errors="coerce").notna()
        & pd.to_numeric(quarterly_df.get("coproduct_simple_plus_30pct_coverage_credit_less_2c_usd_per_gal"), errors="coerce").notna()
    ].copy()
    if not formula_check_df.empty:
        formula_rec = formula_check_df.iloc[0].to_dict()
        official_val = float(pd.to_numeric(formula_rec.get("official_simple_proxy_usd_per_gal"), errors="coerce"))
        credit_val = float(pd.to_numeric(formula_rec.get("coproduct_approximate_credit_usd_per_gal"), errors="coerce"))
        coverage_val = float(pd.to_numeric(formula_rec.get("coproduct_credit_coverage_ratio"), errors="coerce"))
        assert float(pd.to_numeric(formula_rec.get("coproduct_simple_plus_25pct_credit_less_2c_usd_per_gal"), errors="coerce")) == pytest.approx(
            official_val + (0.25 * credit_val) - 0.02
        )
        assert float(pd.to_numeric(formula_rec.get("coproduct_simple_plus_30pct_coverage_credit_less_2c_usd_per_gal"), errors="coerce")) == pytest.approx(
            official_val + (0.30 * coverage_val * credit_val) - 0.02
        )
    else:
        assert "25% of approximate coproduct credit - $0.02/gal friction" in str(fractional_row.get("rule") or "")
        coverage_netted_row = coproduct_experimental_candidate_comparison_df[
            coproduct_experimental_candidate_comparison_df["model_key"].astype(str) == "simple_plus_30pct_coverage_credit_less_2c"
        ].iloc[0].to_dict()
        assert pd.to_numeric(coverage_netted_row.get("friction_offset_usd_per_gal"), errors="coerce") == pytest.approx(0.02)
        assert "30% x coverage x approximate coproduct credit - $0.02/gal friction" in str(coverage_netted_row.get("rule") or "")
    assert "coproduct-aware experimental lenses" in str(result.get("coproduct_experimental_summary_markdown") or "").lower()
    assert "previous best coproduct-aware reference" in str(result.get("coproduct_experimental_summary_markdown") or "").lower()
    assert "experimental signal audit" in summary_lower
    assert "experimental realization / regime comparison" in summary_lower
    assert "best experimental candidate" in summary_lower
    assert "best historical fit" in summary_lower
    assert "best compromise" in summary_lower
    assert "best forward lens" in summary_lower
    assert "promoted:" in summary_lower
    assert "gallons_produced" in summary_lower
    assert "gallons_sold" in summary_lower
    assert "sold_minus_produced_gap" in summary_lower
    assert "quarter_open_anchor" in summary_lower
    assert "utilization" in summary_lower
    assert "maintenance_outage_delay" in summary_lower
    assert "inventory_nrv_timing_drag" in summary_lower
    assert "locked_setup" in summary_lower
    assert "hard_quarter_flags" in summary_lower
    assert "preview_helper_support" in summary_lower
    assert str(hedge_study.get("target_col") or "") == "reported_consolidated_crush_margin_usd_per_gal"
    assert str(hedge_study.get("target_label") or "") == "Reported consolidated crush margin ($/gal)"
    hedge_candidates = hedge_study.get("candidate_leaderboard_df")
    hedge_quarter_fit = hedge_study.get("quarter_fit_df")
    assert isinstance(hedge_candidates, pd.DataFrame) and not hedge_candidates.empty
    assert isinstance(hedge_quarter_fit, pd.DataFrame) and not hedge_quarter_fit.empty
    assert pd.to_numeric(hedge_quarter_fit["target_value_usd_per_gal"], errors="coerce").notna().all()
    assert {"style_key", "family", "mae", "best_fit_quarter_count"} <= set(hedge_candidates.columns)
    assert {"best_fit_style_key", "best_fit_family", "best_fit_value_usd_per_gal", "weak_fit_flag", "weak_fit_display"} <= set(hedge_quarter_fit.columns)
    assert str(hedge_study.get("backtest_window_display") or "").strip() == "2023-Q1 to 2025-Q4"
    assert "single lowest-mae candidate style" in str(hedge_study.get("best_style_vs_family_explanation") or "").lower()
    assert "diagnostic only" in str(hedge_study.get("diagnostic_only_note") or "").lower()
    weak_fit_from_rows = (
        hedge_quarter_fit.loc[hedge_quarter_fit["weak_fit_display"].astype(str) == "Yes", "quarter_label"].astype(str).tolist()
    )
    assert weak_fit_from_rows == list(hedge_study.get("weak_fit_quarters") or [])
    assert "implied hedge / realization style study" in summary_lower
    assert "backtest window: 2023-q1 to 2025-q4." in summary_lower
    assert "style vs family" in summary_lower
    assert "diagnostic scope" in summary_lower
    assert "roles / consistency check" in summary_lower
    assert str(hedge_study.get("best_overall_style_label") or "").lower() in summary_lower
    assert str(hedge_study.get("best_overall_style_family_label") or "").lower() in summary_lower
    for weak_q in list(hedge_study.get("weak_fit_quarters") or []):
        assert str(weak_q).lower() in summary_lower
    assert str(result["production_winner_model_key"] or "") in str(result["summary_markdown"] or "")
    assert str(result["expanded_best_candidate_model_key"] or "") in str(result["summary_markdown"] or "")
    assert str(result.get("best_historical_fit_model_key") or "") in str(result["summary_markdown"] or "")
    assert str(result.get("best_compromise_model_key") or "") in str(result["summary_markdown"] or "")
    assert str(result.get("best_forward_lens_model_key") or "") in str(result["summary_markdown"] or "")
    assert str(result["gpre_proxy_live_preview_quality_status"] or "").lower() in summary_lower
    assert str(result.get("best_historical_fit_model_key") or "").strip()
    assert str(result.get("best_compromise_model_key") or "").strip()
    assert str(result.get("best_forward_lens_model_key") or "").strip()
    system_audit = result.get("system_audit")
    assert isinstance(system_audit, dict)
    assert bool(system_audit.get("internal_consistency_detected")) is False
    assert "diagnostic only" in str(system_audit.get("hedge_style_study_role") or "").lower()
    assert "best historical fit" in str(system_audit.get("best_historical_fit_role") or "").lower()
    assert "best compromise" in str(system_audit.get("best_compromise_role") or "").lower()
    assert "best forward lens" in str(system_audit.get("best_forward_lens_role") or "").lower()


def test_gpre_utilization_overlay_penalty_is_bounded_and_small_near_full_utilization() -> None:
    assert market_service._gpre_utilization_overlay_penalty(None) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_utilization_overlay_penalty(98.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_utilization_overlay_penalty(95.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_utilization_overlay_penalty(90.0) == pytest.approx(0.0125, abs=1e-12)
    assert market_service._gpre_utilization_overlay_penalty(70.0) == pytest.approx(0.035, abs=1e-12)


def test_gpre_low_coverage_mae_requires_at_least_two_low_coverage_quarters() -> None:
    quarterly_df = pd.DataFrame(
        {
            "pred_col": [0.10, 0.20, 0.30],
            "coverage_col": [0.96, 0.94, 0.97],
            "evaluation_target_margin_usd_per_gal": [0.11, 0.18, 0.29],
        }
    )

    low_coverage_mae, low_coverage_note = market_service._gpre_low_coverage_mae(
        quarterly_df,
        pred_col="pred_col",
        coverage_col="coverage_col",
    )

    assert low_coverage_mae is None
    assert low_coverage_note == "insufficient low-coverage quarters"


def test_gpre_maintenance_delay_penalty_only_moves_on_explicit_ops_terms() -> None:
    none_details = market_service._gpre_maintenance_delay_penalty_details("")
    mild_details = market_service._gpre_maintenance_delay_penalty_details("planned maintenance")
    severe_details = market_service._gpre_maintenance_delay_penalty_details("planned maintenance, outage")

    assert float(none_details["penalty_usd_per_gal"]) == pytest.approx(0.0, abs=1e-12)
    assert float(mild_details["penalty_usd_per_gal"]) == pytest.approx(0.015, abs=1e-12)
    assert float(severe_details["penalty_usd_per_gal"]) == pytest.approx(0.025, abs=1e-12)
    assert severe_details["trigger_terms"] == ["outage", "planned maintenance"]


def test_gpre_inventory_timing_drag_penalty_requires_explicit_inventory_or_timing_hits() -> None:
    none_details = market_service._gpre_inventory_timing_penalty_details("", "")
    inventory_only = market_service._gpre_inventory_timing_penalty_details("inventory nrv", "")
    both_details = market_service._gpre_inventory_timing_penalty_details("inventory nrv", "inventory timing")

    assert float(none_details["penalty_usd_per_gal"]) == pytest.approx(0.0, abs=1e-12)
    assert float(inventory_only["penalty_usd_per_gal"]) == pytest.approx(0.015, abs=1e-12)
    assert float(both_details["penalty_usd_per_gal"]) == pytest.approx(0.025, abs=1e-12)


def test_gpre_locked_setup_value_only_moves_toward_quarter_open_anchor_when_share_exists() -> None:
    assert market_service._gpre_locked_setup_value(
        0.20,
        disclosed_share=0.0,
        pattern_share=0.0,
        quarter_open_anchor=0.10,
    ) == pytest.approx(0.20, abs=1e-12)
    assert market_service._gpre_locked_setup_value(
        0.20,
        disclosed_share=None,
        pattern_share=0.25,
        quarter_open_anchor=0.10,
    ) == pytest.approx(0.175, abs=1e-12)
    assert market_service._gpre_locked_setup_value(
        0.20,
        disclosed_share=0.80,
        pattern_share=0.10,
        quarter_open_anchor=0.10,
    ) == pytest.approx(0.16, abs=1e-12)


def test_gpre_basis_passthrough_value_is_monotonic_in_beta() -> None:
    beta_35 = market_service._gpre_basis_passthrough_value(-0.30, -0.10, beta=0.35)
    beta_65 = market_service._gpre_basis_passthrough_value(-0.30, -0.10, beta=0.65)

    assert beta_35 == pytest.approx((-0.30 * 0.35) + (-0.10 * 0.65), abs=1e-12)
    assert beta_65 == pytest.approx((-0.30 * 0.65) + (-0.10 * 0.35), abs=1e-12)
    assert beta_65 < beta_35


def test_gpre_regime_basis_passthrough_beta_moves_toward_front_loaded_when_locked() -> None:
    open_beta = market_service._gpre_regime_basis_passthrough_beta(0.0, 0.0)
    locked_beta = market_service._gpre_regime_basis_passthrough_beta(0.30, 0.0)

    assert open_beta == pytest.approx(0.60, abs=1e-12)
    assert locked_beta == pytest.approx(0.50, abs=1e-12)
    assert locked_beta < open_beta


def test_gpre_realization_residual_penalty_is_bounded_and_zero_without_signals() -> None:
    none_penalty = market_service._gpre_realization_residual_penalty(0.0, 0.0, 0.0)
    capped_penalty = market_service._gpre_realization_residual_penalty(0.03, 0.025, 0.025)

    assert none_penalty == pytest.approx(0.0, abs=1e-12)
    assert capped_penalty == pytest.approx(0.055, abs=1e-12)


def test_gpre_sold_minus_produced_gap_ratio_has_explicit_sign_convention_and_bounds() -> None:
    positive_gap = market_service._gpre_sold_minus_produced_gap_ratio(110.0, 100.0)
    negative_gap = market_service._gpre_sold_minus_produced_gap_ratio(90.0, 100.0)
    capped_gap = market_service._gpre_sold_minus_produced_gap_ratio(180.0, 100.0)

    assert positive_gap == pytest.approx(0.10, abs=1e-12)
    assert negative_gap == pytest.approx(-0.10, abs=1e-12)
    assert capped_gap == pytest.approx(0.20, abs=1e-12)
    assert market_service._gpre_sold_minus_produced_gap_ratio(100.0, 0.0) is None


def test_gpre_inventory_gap_disturbance_score_requires_material_gap_and_is_bounded() -> None:
    assert market_service._gpre_inventory_gap_disturbance_score(None) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_inventory_gap_disturbance_score(0.02) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_inventory_gap_disturbance_score(0.06) == pytest.approx((0.06 - 0.03) / 0.09, abs=1e-12)
    assert market_service._gpre_inventory_gap_disturbance_score(-0.12) == pytest.approx(1.0, abs=1e-12)


def test_gpre_utilization_regime_scores_are_bounded_and_differ_for_high_and_low_utilization() -> None:
    assert market_service._gpre_low_utilization_regime_score(100.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_high_utilization_regime_score(90.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_low_utilization_regime_score(80.0) == pytest.approx(1.0, abs=1e-12)
    assert market_service._gpre_high_utilization_regime_score(100.0) == pytest.approx(1.0, abs=1e-12)
    assert market_service._gpre_low_utilization_regime_score(92.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_high_utilization_regime_score(95.0) == pytest.approx(0.0, abs=1e-12)


def test_gpre_exec_inventory_combo_penalty_requires_both_signals_and_stays_bounded() -> None:
    assert market_service._gpre_exec_inventory_combo_penalty(0.0, 1.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_exec_inventory_combo_penalty(0.015, 0.0) == pytest.approx(0.0, abs=1e-12)
    assert market_service._gpre_exec_inventory_combo_penalty(0.015, 0.50) == pytest.approx(0.0125, abs=1e-12)
    assert market_service._gpre_exec_inventory_combo_penalty(0.030, 1.00) == pytest.approx(0.025, abs=1e-12)


def test_gpre_asymmetric_passthrough_value_weights_downside_more_than_upside() -> None:
    upside = market_service._gpre_asymmetric_passthrough_value(0.20, 0.30)
    downside = market_service._gpre_asymmetric_passthrough_value(0.20, 0.10)

    assert upside == pytest.approx(0.245, abs=1e-12)
    assert downside == pytest.approx(0.12, abs=1e-12)
    assert abs(downside - 0.20) > abs(upside - 0.20)


def test_gpre_regime_and_gated_model_helpers_switch_only_in_the_intended_regimes() -> None:
    disturbed_val = market_service._gpre_residual_regime_value(True, True, 0.10, 0.20, 0.30)
    locked_val = market_service._gpre_residual_regime_value(False, True, 0.10, 0.20, 0.30)
    normal_val = market_service._gpre_residual_regime_value(False, False, 0.10, 0.20, 0.30)
    gated_hard = market_service._gpre_gated_model_value(True, False, 0.15, 0.05)
    gated_normal = market_service._gpre_gated_model_value(False, False, 0.15, 0.05)

    assert disturbed_val == pytest.approx(0.10, abs=1e-12)
    assert locked_val == pytest.approx(0.20, abs=1e-12)
    assert normal_val == pytest.approx(0.30, abs=1e-12)
    assert gated_hard == pytest.approx(0.15, abs=1e-12)
    assert gated_normal == pytest.approx(0.05, abs=1e-12)


def test_gpre_experimental_signal_audit_counts_locked_setup_quarters_once_per_quarter() -> None:
    quarterly_df = pd.DataFrame(
        [
            {
                "reported_ethanol_gallons_sold_raw": 105_000_000.0,
                "reported_ethanol_gallons_produced_raw": 100_000_000.0,
                "sold_minus_produced_gap_ratio": 0.05,
                "inventory_gap_disturbance_score": 0.222,
                "process_quarter_open_anchor_usd_per_gal": 0.12,
                "hedge_share_disclosed": 0.20,
                "hedge_share_pattern": 0.15,
                "ops_utilization_pct": 92.0,
                "low_utilization_regime_score": 0.0,
                "high_utilization_regime_score": 0.0,
                "ops_signal_terms": "planned maintenance",
                "maintenance_delay_penalty_usd_per_gal": 0.015,
                "inventory_drag_terms": "",
                "inventory_timing_drag_penalty_usd_per_gal": 0.0,
                "process_quarter_open_blend_locked_setup_usd_per_gal": 0.12,
                "locked_or_setup_quarter_flag": True,
                "hard_quarter_flag": True,
            },
            {
                "reported_ethanol_gallons_sold_raw": 95_000_000.0,
                "reported_ethanol_gallons_produced_raw": 100_000_000.0,
                "sold_minus_produced_gap_ratio": -0.05,
                "inventory_gap_disturbance_score": 0.222,
                "process_quarter_open_anchor_usd_per_gal": 0.10,
                "hedge_share_disclosed": 0.0,
                "hedge_share_pattern": 0.10,
                "ops_utilization_pct": 96.0,
                "low_utilization_regime_score": 0.0,
                "high_utilization_regime_score": 0.20,
                "ops_signal_terms": "",
                "maintenance_delay_penalty_usd_per_gal": 0.0,
                "inventory_drag_terms": "inventory nrv",
                "inventory_timing_drag_penalty_usd_per_gal": 0.015,
                "process_quarter_open_blend_locked_setup_usd_per_gal": 0.10,
                "locked_or_setup_quarter_flag": True,
                "hard_quarter_flag": False,
            },
        ]
    )

    audit = market_service._gpre_experimental_signal_audit(quarterly_df)
    locked_row = next(rec for rec in audit["signal_rows"] if str(rec.get("signal") or "") == "locked_setup")

    assert int(locked_row["available_quarters"]) == 2
    assert int(locked_row["active_signal_quarters"]) == 2


def test_gpre_experimental_signal_audit_reports_realization_and_regime_signal_rows() -> None:
    quarterly_df = pd.DataFrame(
        [
            {
                "reported_ethanol_gallons_sold_raw": 105_000_000.0,
                "reported_ethanol_gallons_produced_raw": 100_000_000.0,
                "sold_minus_produced_gap_ratio": 0.05,
                "inventory_gap_disturbance_score": 0.222,
                "process_quarter_open_anchor_usd_per_gal": 0.12,
                "hedge_share_disclosed": 0.20,
                "hedge_share_pattern": 0.15,
                "ops_utilization_pct": 92.0,
                "low_utilization_regime_score": 0.10,
                "high_utilization_regime_score": 0.0,
                "ops_signal_terms": "planned maintenance",
                "maintenance_delay_penalty_usd_per_gal": 0.015,
                "inventory_drag_terms": "inventory nrv",
                "inventory_timing_drag_penalty_usd_per_gal": 0.015,
                "locked_or_setup_quarter_flag": True,
                "hard_quarter_flag": True,
            }
        ]
    )

    audit = market_service._gpre_experimental_signal_audit(quarterly_df)
    signal_names = {str(rec.get("signal") or "") for rec in audit["signal_rows"]}

    assert {
        "gallons_produced",
        "gallons_sold",
        "sold_minus_produced_gap",
        "quarter_open_anchor",
        "utilization",
        "maintenance_outage_delay",
        "inventory_nrv_timing_drag",
        "locked_setup",
        "hard_quarter_flags",
        "preview_helper_support",
    } <= signal_names


def test_gpre_hedge_style_blends_are_monotonic_and_front_loaded_differs_from_equal_monthly() -> None:
    lock_25 = market_service._gpre_hedge_style_blend_value(0.60, 0.20, anchor_weight=0.25)
    lock_50 = market_service._gpre_hedge_style_blend_value(0.60, 0.20, anchor_weight=0.50)
    lock_75 = market_service._gpre_hedge_style_blend_value(0.60, 0.20, anchor_weight=0.75)

    assert lock_25 == pytest.approx(0.30, abs=1e-12)
    assert lock_50 == pytest.approx(0.40, abs=1e-12)
    assert lock_75 == pytest.approx(0.50, abs=1e-12)
    assert lock_25 < lock_50 < lock_75

    quarterly_df = _gpre_hedge_style_quarterly_fixture()
    study = market_service._build_gpre_hedge_style_study(quarterly_df)
    leaderboard = study["candidate_leaderboard_df"]
    front_mae = float(pd.to_numeric(leaderboard.loc[leaderboard["style_key"].astype(str) == "front_loaded_layering", "mae"].iloc[0], errors="coerce"))
    equal_mae = float(pd.to_numeric(leaderboard.loc[leaderboard["style_key"].astype(str) == "equal_monthly_layering", "mae"].iloc[0], errors="coerce"))
    assert front_mae != pytest.approx(equal_mae, abs=1e-12)


def test_gpre_hedge_style_study_computes_best_fit_family_and_keeps_ties_deterministic() -> None:
    quarterly_df = _gpre_hedge_style_quarterly_fixture()
    quarterly_before = quarterly_df.copy(deep=True)

    study = market_service._build_gpre_hedge_style_study(quarterly_df)

    assert_frame_equal(quarterly_df, quarterly_before)
    quarter_fit_df = study["quarter_fit_df"]
    assert not quarter_fit_df.empty
    assert str(quarter_fit_df.iloc[0]["quarter_label"] or "").strip() == "2024-Q4"
    q2_row = quarter_fit_df[quarter_fit_df["quarter_label"].astype(str) == "2024-Q2"].iloc[0]
    q3_row = quarter_fit_df[quarter_fit_df["quarter_label"].astype(str) == "2024-Q3"].iloc[0]
    assert str(q2_row["best_fit_style_key"] or "") == "ops_disruption_overlay"
    assert bool(q3_row["weak_fit_flag"])
    assert str(q3_row["weak_fit_display"] or "") == "Yes"
    assert "weak fit" in str(q3_row["fit_note"] or "").lower()
    assert str(study.get("best_overall_style_key") or "").strip()
    assert str(study.get("best_overall_style_family") or "").strip()
    assert str(study.get("backtest_window_display") or "").strip() == "2024-Q1 to 2024-Q4"
    assert "single lowest-mae candidate style" in str(study.get("best_style_vs_family_explanation") or "").lower()
    assert "diagnostic only" in str(study.get("diagnostic_only_note") or "").lower()


def test_gpre_hedge_style_study_uses_candidate_order_as_tie_break() -> None:
    tied_df = pd.DataFrame(
        [
            {
                "quarter": date(2025, 3, 31),
                "quarter_label": "2025-Q1",
                "reported_consolidated_crush_margin_usd_per_gal": 0.250,
                "official_simple_proxy_usd_per_gal": 0.200,
                "process_quarter_open_anchor_usd_per_gal": np.nan,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.300,
                "process_proxy_front_loaded_usd_per_gal": np.nan,
                "process_quarter_open_blend_usd_per_gal": np.nan,
                "process_quarter_open_blend_hedge_realization_usd_per_gal": np.nan,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": np.nan,
                "hard_quarter_flag": False,
                "hard_quarter_reason": "",
            }
        ]
    )

    study = market_service._build_gpre_hedge_style_study(tied_df)
    quarter_fit_df = study["quarter_fit_df"]

    assert str(quarter_fit_df.iloc[0]["best_fit_style_key"] or "") == "spot_simple"


def test_gpre_hedge_style_realization_and_ops_candidates_only_help_when_signals_exist() -> None:
    quarterly_df = _gpre_hedge_style_quarterly_fixture()
    study = market_service._build_gpre_hedge_style_study(quarterly_df)
    quarter_fit_df = study["quarter_fit_df"]

    q2_row = quarter_fit_df[quarter_fit_df["quarter_label"].astype(str) == "2024-Q2"].iloc[0]
    q4_row = quarter_fit_df[quarter_fit_df["quarter_label"].astype(str) == "2024-Q4"].iloc[0]

    assert str(q2_row["best_fit_family"] or "") == "realization_drag"
    assert "ops" in str(q2_row["fit_note"] or "").lower() or "realization" in str(q2_row["fit_note"] or "").lower()
    assert str(q4_row["best_fit_style_key"] or "") in {"equal_monthly_layering", "quarter_open_plus_current_blend", "quarter_open_lock_25", "spot_simple"}
    assert str(q4_row["best_fit_family"] or "") != "realization_drag"


def test_gpre_hedge_style_study_readability_fields_are_chronological_and_visible() -> None:
    study = market_service._build_gpre_hedge_style_study(_gpre_hedge_style_quarterly_fixture())
    quarter_fit_df = study["quarter_fit_df"]

    assert str(study.get("backtest_window_display") or "").strip() == "2024-Q1 to 2024-Q4"
    assert quarter_fit_df["weak_fit_display"].astype(str).isin({"Yes", "No"}).all()
    assert "single lowest-mae candidate style" in str(study.get("best_style_vs_family_explanation") or "").lower()
    assert "diagnostic only" in str(study.get("diagnostic_only_note") or "").lower()


def test_gpre_hedge_style_weak_fit_quarter_list_matches_flagged_rows_exactly() -> None:
    study = market_service._build_gpre_hedge_style_study(_gpre_hedge_style_quarterly_fixture())
    quarter_fit_df = study["quarter_fit_df"].copy()

    weak_fit_rows = quarter_fit_df.loc[
        quarter_fit_df["weak_fit_display"].astype(str) == "Yes",
        "quarter_label",
    ].astype(str).tolist()

    assert weak_fit_rows == list(study.get("weak_fit_quarters") or [])
    assert weak_fit_rows
    assert "2024-Q3" in weak_fit_rows


def test_gpre_hedge_style_study_can_favor_quarter_open_locking_when_setup_matches_reported_quarter() -> None:
    locking_df = pd.DataFrame(
        [
            {
                "quarter": date(2025, 6, 30),
                "quarter_label": "2025-Q2",
                "reported_consolidated_crush_margin_usd_per_gal": 0.535,
                "official_simple_proxy_usd_per_gal": 0.280,
                "process_quarter_open_anchor_usd_per_gal": 0.560,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.280,
                "process_proxy_front_loaded_usd_per_gal": 0.460,
                "process_quarter_open_blend_usd_per_gal": 0.490,
                "process_quarter_open_blend_hedge_realization_usd_per_gal": 0.490,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.490,
                "hard_quarter_flag": False,
                "hard_quarter_reason": "",
            }
        ]
    )

    study = market_service._build_gpre_hedge_style_study(locking_df)
    fit_row = study["quarter_fit_df"].iloc[0]

    assert str(fit_row["best_fit_family"] or "") == "quarter_open_locking"
    assert str(fit_row["best_fit_style_key"] or "") == "quarter_open_lock_75"


def test_gpre_hedge_style_study_is_diagnostic_only_and_does_not_change_overlay_bundle_values() -> None:
    rows = _gpre_overlay_fixture_rows(
        ethanol_base=1.54,
        corn_front=4.32,
        gas_front=2.82,
        qd=date(2026, 3, 31),
        obs_dt=date(2026, 3, 28),
    ) + _gpre_overlay_fixture_rows()
    basis_model_result = {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.18,
                    "gpre_proxy_model_key": "process_quarter_open_blend_exec_penalty",
                    "gpre_proxy_official_usd_per_gal": -0.012,
                    "approx_market_bridge_proxy_usd_per_gal": 0.004,
                    "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.010,
                    "process_proxy_front_loaded_usd_per_gal": -0.020,
                    "process_quarter_open_blend_exec_penalty_usd_per_gal": -0.012,
                }
            ]
        ),
        "gpre_proxy_model_key": "process_quarter_open_blend_exec_penalty",
        "gpre_proxy_family": "process_blend_exec_penalty",
        "gpre_proxy_family_label": "Process blend + severe ops penalty",
        "gpre_proxy_timing_rule": "Quarter-open/current blend - severe execution penalty",
        "production_winner_model_key": "process_quarter_open_blend_exec_penalty",
        "hedge_style_study": market_service._build_gpre_hedge_style_study(_gpre_hedge_style_quarterly_fixture()),
    }
    basis_without_study = dict(basis_model_result)
    basis_without_study.pop("hedge_style_study", None)

    bundle_with_study = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_model_result,
    )
    bundle_without_study = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_without_study,
    )

    for frame_key in ("prior_quarter", "current_qtd", "quarter_open", "next_quarter_thesis"):
        official_with = pd.to_numeric(bundle_with_study["official_frames"][frame_key]["value"], errors="coerce")
        official_without = pd.to_numeric(bundle_without_study["official_frames"][frame_key]["value"], errors="coerce")
        fitted_with = pd.to_numeric(bundle_with_study["gpre_proxy_frames"][frame_key]["value"], errors="coerce")
        fitted_without = pd.to_numeric(bundle_without_study["gpre_proxy_frames"][frame_key]["value"], errors="coerce")
        if pd.isna(official_with) and pd.isna(official_without):
            assert True
        else:
            assert float(official_with) == pytest.approx(float(official_without), abs=1e-9)
        if pd.isna(fitted_with) and pd.isna(fitted_without):
            assert True
        else:
            assert float(fitted_with) == pytest.approx(float(fitted_without), abs=1e-9)


def test_gpre_proxy_implied_results_bundle_uses_produced_gallons_and_prior_quarter_fallback() -> None:
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        _gpre_proxy_implied_overlay_bundle_fixture(),
        reported_gallons_produced_by_quarter={
            date(2025, 3, 31): 190_000_000.0,
            date(2025, 6, 30): 170_000_000.0,
            date(2025, 9, 30): 178_000_000.0,
        },
        denominator_policy_by_quarter={
            date(2025, 9, 30): "ethanol gallons produced",
        },
        ticker_root=None,
    )

    prior_frame = bundle["frames"]["prior_quarter"]
    quarter_open_frame = bundle["frames"]["quarter_open"]
    current_frame = bundle["frames"]["current_qtd"]
    next_frame = bundle["frames"]["next_quarter_thesis"]
    prior_scale = 730.0 / 784.0

    assert str(prior_frame["volume_basis_display"] or "") == "Fallback: YoY produced gallons adjusted to active capacity"
    assert str(prior_frame["gallons_source_kind"] or "") == "fallback_yoy_same_quarter_gallons_produced_capacity_scaled"
    assert prior_frame["current_operating_plant_count"] == 8
    assert prior_frame["ly_operating_plant_count"] == 9
    assert float(pd.to_numeric(prior_frame["current_active_capacity_mmgy"], errors="coerce")) == pytest.approx(730.0, abs=1e-9)
    assert float(pd.to_numeric(prior_frame["ly_active_capacity_mmgy"], errors="coerce")) == pytest.approx(784.0, abs=1e-9)
    assert float(pd.to_numeric(prior_frame["implied_gallons"], errors="coerce")) == pytest.approx(190_000_000.0 * prior_scale, abs=1e-6)
    assert float(pd.to_numeric(prior_frame["implied_gallons_raw"], errors="coerce")) == pytest.approx(190_000_000.0 * prior_scale, abs=1e-6)
    assert float(pd.to_numeric(prior_frame["implied_gallons_million_display"], errors="coerce")) == pytest.approx((190_000_000.0 * prior_scale) / 1_000_000.0, abs=1e-9)
    assert "2025-Q1 gallons produced scaled by 730/784 MMgy active capacity." in str(prior_frame["volume_basis_comment"] or "")
    assert str(prior_frame["reasonableness_status"] or "") == "within_tolerance"
    assert float(pd.to_numeric(prior_frame["official_proxy_implied_result_usd_m"], errors="coerce")) == pytest.approx(
        0.110 * (190_000_000.0 * prior_scale) / 1_000_000.0,
        abs=1e-6,
    )

    for frame in (quarter_open_frame, current_frame):
        assert frame["current_operating_plant_count"] == 8
        assert frame["ly_operating_plant_count"] == 9
        assert float(pd.to_numeric(frame["current_active_capacity_mmgy"], errors="coerce")) == pytest.approx(730.0, abs=1e-9)
        assert float(pd.to_numeric(frame["ly_active_capacity_mmgy"], errors="coerce")) == pytest.approx(784.0, abs=1e-9)
        assert float(pd.to_numeric(frame["footprint_scale_factor"], errors="coerce")) == pytest.approx(730.0 / 784.0, abs=1e-12)
        assert float(pd.to_numeric(frame["implied_gallons"], errors="coerce")) == pytest.approx(170_000_000.0 * 730.0 / 784.0, abs=1e-6)
        assert float(pd.to_numeric(frame["implied_gallons_raw"], errors="coerce")) == pytest.approx(170_000_000.0 * 730.0 / 784.0, abs=1e-6)
        assert float(pd.to_numeric(frame["implied_gallons_million_display"], errors="coerce")) == pytest.approx((170_000_000.0 * 730.0 / 784.0) / 1_000_000.0, abs=1e-9)
        assert str(frame["volume_basis_display"] or "") == "YoY same quarter produced gallons, adjusted to current active capacity footprint (730 MMgy)"
        assert "2025-Q2 gallons produced scaled by 730/784 MMgy active capacity." in str(frame["volume_basis_comment"] or "")
        assert str(frame["reasonableness_status"] or "") == "within_tolerance"

    assert next_frame["current_operating_plant_count"] == 8
    assert next_frame["ly_operating_plant_count"] == 9
    assert float(pd.to_numeric(next_frame["current_active_capacity_mmgy"], errors="coerce")) == pytest.approx(730.0, abs=1e-9)
    assert float(pd.to_numeric(next_frame["ly_active_capacity_mmgy"], errors="coerce")) == pytest.approx(784.0, abs=1e-9)
    assert float(pd.to_numeric(next_frame["implied_gallons"], errors="coerce")) == pytest.approx(178_000_000.0 * 730.0 / 784.0, abs=1e-6)
    assert float(pd.to_numeric(next_frame["implied_gallons_raw"], errors="coerce")) == pytest.approx(178_000_000.0 * 730.0 / 784.0, abs=1e-6)
    assert float(pd.to_numeric(next_frame["implied_gallons_million_display"], errors="coerce")) == pytest.approx((178_000_000.0 * 730.0 / 784.0) / 1_000_000.0, abs=1e-9)
    assert float(pd.to_numeric(next_frame["gpre_proxy_implied_result_usd_m"], errors="coerce")) == pytest.approx(
        0.310 * (178_000_000.0 * 730.0 / 784.0) / 1_000_000.0,
        abs=1e-6,
    )
    assert str(next_frame["volume_basis_display"] or "") == "YoY same quarter produced gallons, adjusted to current active capacity footprint (730 MMgy)"
    assert "2025-Q3 gallons produced scaled by 730/784 MMgy active capacity." in str(next_frame["volume_basis_comment"] or "")
    assert str(next_frame["reasonableness_status"] or "") == "within_tolerance"


def test_gpre_proxy_implied_results_bundle_surfaces_unavailable_and_blank_proxy_behavior() -> None:
    overlay_bundle = _gpre_proxy_implied_overlay_bundle_fixture()
    overlay_bundle["official_frames"]["current_qtd"]["value"] = None
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        overlay_bundle,
        reported_gallons_produced_by_quarter={
            date(2025, 3, 31): 190_000_000.0,
            date(2025, 6, 30): 170_000_000.0,
        },
        denominator_policy_by_quarter={
            date(2025, 6, 30): "ethanol gallons produced",
        },
        ticker_root=None,
    )

    prior_frame = bundle["frames"]["prior_quarter"]
    current_frame = bundle["frames"]["current_qtd"]
    next_frame = bundle["frames"]["next_quarter_thesis"]

    assert str(prior_frame["status"] or "") == "ok"
    assert str(current_frame["status"] or "") == "ok"
    assert current_frame["official_proxy_implied_result_usd_m"] is None
    assert float(pd.to_numeric(current_frame["gpre_proxy_implied_result_usd_m"], errors="coerce")) > 0.0
    assert str(next_frame["status"] or "") == "unavailable"
    assert next_frame["implied_gallons"] is None
    assert next_frame["implied_gallons_raw"] is None
    assert next_frame["implied_gallons_million_display"] is None
    assert next_frame["official_proxy_implied_result_usd_m"] is None
    assert str(next_frame["volume_basis_display"] or "") == "Unavailable"
    assert "2025-Q3" in str(next_frame["reason_unavailable"] or "")


def test_gpre_proxy_implied_results_bundle_updates_with_current_footprint_regime() -> None:
    overlay_bundle = {
        "official_frames": {
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.180},
        },
        "gpre_proxy_frames": {
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.200},
        },
    }
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        overlay_bundle,
        reported_gallons_produced_by_quarter={
            date(2025, 6, 30): 170_000_000.0,
        },
        denominator_policy_by_quarter={
            date(2025, 6, 30): "ethanol gallons produced",
        },
        ticker_root=None,
    )

    frame = bundle["frames"]["quarter_open"]
    assert frame["current_operating_plant_count"] == 8
    assert frame["ly_operating_plant_count"] == 9
    assert float(pd.to_numeric(frame["current_active_capacity_mmgy"], errors="coerce")) == pytest.approx(730.0, abs=1e-9)
    assert float(pd.to_numeric(frame["ly_active_capacity_mmgy"], errors="coerce")) == pytest.approx(784.0, abs=1e-9)
    assert float(pd.to_numeric(frame["footprint_scale_factor"], errors="coerce")) == pytest.approx(730.0 / 784.0, abs=1e-12)
    assert str(frame["volume_basis_display"] or "") == "YoY same quarter produced gallons, adjusted to current active capacity footprint (730 MMgy)"
    assert "2025-Q2 gallons produced scaled by 730/784 MMgy active capacity." in str(frame["volume_basis_comment"] or "")


def test_gpre_proxy_implied_results_bundle_falls_back_to_active_plant_count_ratio_when_capacity_missing() -> None:
    overlay_bundle = {
        "official_frames": {
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.180},
        },
        "gpre_proxy_frames": {
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.200},
        },
    }
    plant_capacity_history = {
        "source_mode": "test_fixture",
        "snapshots": [
            {
                "snapshot_year": 2025,
                "snapshot_quarter_end": date(2025, 12, 31),
                "plants": {},
            }
        ],
        "plants": {
            "alpha": {
                "plant_key": "alpha",
                "location": "Alpha",
                "state": "Nebraska",
                "region": "nebraska",
                "capacity_by_snapshot_year": {},
                "footnotes_by_snapshot_year": {},
                "flags": {},
            },
            "beta": {
                "plant_key": "beta",
                "location": "Beta",
                "state": "Iowa",
                "region": "iowa_west",
                "capacity_by_snapshot_year": {},
                "footnotes_by_snapshot_year": {},
                "flags": {},
            },
            "gamma": {
                "plant_key": "gamma",
                "location": "Gamma",
                "state": "Minnesota",
                "region": "minnesota",
                "capacity_by_snapshot_year": {},
                "footnotes_by_snapshot_year": {},
                "flags": {},
                "active_through": date(2025, 9, 30),
            },
        },
    }
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        overlay_bundle,
        reported_gallons_produced_by_quarter={
            date(2025, 6, 30): 240_000_000.0,
        },
        denominator_policy_by_quarter={
            date(2025, 6, 30): "ethanol gallons produced",
        },
        ticker_root=None,
        plant_capacity_history=plant_capacity_history,
    )

    frame = bundle["frames"]["quarter_open"]

    assert frame["status"] == "ok"
    assert frame["gallons_source_kind"] == "fallback_yoy_same_quarter_gallons_produced_active_plant_count_ratio"
    assert frame["current_operating_plant_count"] == 2
    assert frame["ly_operating_plant_count"] == 3
    assert frame["current_active_capacity_mmgy"] is None
    assert frame["ly_active_capacity_mmgy"] is None
    assert float(pd.to_numeric(frame["footprint_scale_factor"], errors="coerce")) == pytest.approx(2.0 / 3.0, abs=1e-12)
    assert float(pd.to_numeric(frame["implied_gallons"], errors="coerce")) == pytest.approx(160_000_000.0, abs=1e-6)
    assert float(pd.to_numeric(frame["implied_gallons_raw"], errors="coerce")) == pytest.approx(160_000_000.0, abs=1e-6)
    assert float(pd.to_numeric(frame["implied_gallons_million_display"], errors="coerce")) == pytest.approx(160.0, abs=1e-9)
    assert str(frame["volume_basis_display"] or "") == "Fallback: YoY produced gallons adjusted to active plant-count ratio"
    assert "2025-Q2 gallons produced scaled by 2/3 active plants" in str(frame["volume_basis_comment"] or "")


def test_gpre_proxy_implied_results_bundle_uses_actual_prior_quarter_produced_gallons_when_available() -> None:
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        _gpre_proxy_implied_overlay_bundle_fixture(),
        reported_gallons_produced_by_quarter={
            date(2026, 3, 31): 180_000_000.0,
            date(2025, 3, 31): 190_000_000.0,
            date(2025, 6, 30): 170_000_000.0,
            date(2025, 9, 30): 178_000_000.0,
        },
        denominator_policy_by_quarter={},
        ticker_root=None,
    )

    prior_frame = bundle["frames"]["prior_quarter"]

    assert str(prior_frame["status"] or "") == "ok"
    assert str(prior_frame["gallons_source_kind"] or "") == "actual_prior_quarter_gallons_produced"
    assert str(prior_frame["volume_basis_display"] or "") == "Prior quarter actual produced gallons"
    assert float(pd.to_numeric(prior_frame["implied_gallons_raw"], errors="coerce")) == pytest.approx(180_000_000.0, abs=1e-6)
    assert "Uses actual 2026-Q1 gallons produced." in str(prior_frame["volume_basis_comment"] or "")
    assert str(prior_frame["reasonableness_status"] or "") == "within_tolerance"


def test_gpre_proxy_implied_results_bundle_flags_reasonableness_above_capacity_tolerance() -> None:
    overlay_bundle = {
        "official_frames": {
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.180},
        },
        "gpre_proxy_frames": {
            "quarter_open": {"quarter_end": date(2026, 6, 30), "value": 0.200},
        },
    }
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        overlay_bundle,
        reported_gallons_produced_by_quarter={
            date(2025, 6, 30): 210_000_000.0,
        },
        denominator_policy_by_quarter={},
        ticker_root=None,
    )

    frame = bundle["frames"]["quarter_open"]

    assert float(pd.to_numeric(frame["quarter_capacity_ceiling_gallons_raw"], errors="coerce")) == pytest.approx(730_000_000.0 / 4.0, abs=1e-6)
    assert float(pd.to_numeric(frame["quarter_capacity_ceiling_gallons_million_display"], errors="coerce")) == pytest.approx(182.5, abs=1e-9)
    assert float(pd.to_numeric(frame["reasonableness_tolerance_ratio"], errors="coerce")) == pytest.approx(0.05, abs=1e-12)
    assert str(frame["reasonableness_status"] or "") == "above_tolerance"
    assert float(pd.to_numeric(frame["reasonableness_excess_ratio"], errors="coerce")) > 0.05
    assert "Implied gallons exceed quarter-capacity ceiling" in str(frame["reasonableness_comment"] or "")
    assert "Implied gallons exceed quarter-capacity ceiling" in str(frame["volume_basis_comment"] or "")


def test_gpre_proxy_implied_results_bundle_keeps_prior_frame_unavailable_when_only_plant_counts_exist() -> None:
    overlay_bundle = {
        "official_frames": {
            "prior_quarter": {"quarter_end": date(2026, 3, 31), "value": 0.110},
        },
        "gpre_proxy_frames": {
            "prior_quarter": {"quarter_end": date(2026, 3, 31), "value": 0.120},
        },
    }
    plant_capacity_history = {
        "source_mode": "test_fixture",
        "snapshots": [
            {
                "snapshot_year": 2025,
                "snapshot_quarter_end": date(2025, 12, 31),
                "plants": {},
            }
        ],
        "plants": {
            "alpha": {
                "plant_key": "alpha",
                "location": "Alpha",
                "state": "Nebraska",
                "region": "nebraska",
                "capacity_by_snapshot_year": {},
                "footnotes_by_snapshot_year": {},
                "flags": {},
            },
            "beta": {
                "plant_key": "beta",
                "location": "Beta",
                "state": "Iowa",
                "region": "iowa_west",
                "capacity_by_snapshot_year": {},
                "footnotes_by_snapshot_year": {},
                "flags": {},
            },
            "gamma": {
                "plant_key": "gamma",
                "location": "Gamma",
                "state": "Minnesota",
                "region": "minnesota",
                "capacity_by_snapshot_year": {},
                "footnotes_by_snapshot_year": {},
                "flags": {},
                "active_through": date(2025, 12, 31),
            },
        },
    }
    bundle = market_service._build_gpre_proxy_implied_results_bundle(
        overlay_bundle,
        reported_gallons_produced_by_quarter={
            date(2025, 3, 31): 210_000_000.0,
        },
        denominator_policy_by_quarter={},
        ticker_root=None,
        plant_capacity_history=plant_capacity_history,
    )

    frame = bundle["frames"]["prior_quarter"]

    assert str(frame["status"] or "") == "unavailable"
    assert frame["implied_gallons"] is None
    assert frame["implied_gallons_raw"] is None
    assert str(frame["volume_basis_display"] or "") == "Unavailable"
    assert "prior-quarter produced fallback" in str(frame["reason_unavailable"] or "")


def test_gpre_build_plant_capacity_history_parses_capacity_table_and_footnotes_from_html() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_gpre_capacity_history_")
    try:
        html_path = tmp_path / "gpre-20251231.htm"
        html_path.write_text(
            """
            <html><body>
            <table>
              <tr><th>Plant Location</th><th>Plant Production Capacity (mmgy)</th></tr>
              <tr><td>Central City, Nebraska (1) (2)</td><td>116</td></tr>
              <tr><td>Fairmont, Minnesota (3) (4)</td><td>119</td></tr>
              <tr><td>Total</td><td>235</td></tr>
            </table>
            <p>(1) Produces Ultra-High Protein.</p>
            <p>(2) Connected to Tallgrass Trailblazer Pipeline.</p>
            <p>(3) Committed to Summit Carbon Solutions Pipeline.</p>
            <p>(4) Plant idled in January 2025.</p>
            </body></html>
            """,
            encoding="utf-8",
        )

        history = market_service.build_gpre_plant_capacity_history(html_paths=[html_path])
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)

    assert history["source_mode"] in {"sec_cache_html", "fallback_seed"}
    plants = history.get("plants") or {}
    assert "central_city" in plants
    assert "fairmont" in plants
    assert float(pd.to_numeric(plants["central_city"]["capacity_by_snapshot_year"][2025], errors="coerce")) == pytest.approx(116.0, abs=1e-9)
    assert float(pd.to_numeric(plants["fairmont"]["capacity_by_snapshot_year"][2025], errors="coerce")) == pytest.approx(119.0, abs=1e-9)
    assert bool((plants["central_city"].get("flags") or {}).get("uhp"))
    assert bool((plants["central_city"].get("flags") or {}).get("trailblazer"))
    assert bool((plants["fairmont"].get("flags") or {}).get("summit_carbon"))
    assert plants["fairmont"].get("inactive_from") == date(2025, 1, 1)


def test_gpre_official_market_weights_use_active_capacity_not_simple_plant_count() -> None:
    weights = market_service._gpre_official_market_weights_for_quarter(date(2026, 6, 30))

    assert float(weights["nebraska"]) == pytest.approx(300.0 / 730.0, abs=1e-12)
    assert float(weights["illinois"]) == pytest.approx(100.0 / 730.0, abs=1e-12)
    assert float(weights["indiana"]) == pytest.approx(110.0 / 730.0, abs=1e-12)
    assert float(weights["iowa_west"]) == pytest.approx(150.0 / 730.0, abs=1e-12)
    assert float(weights["minnesota"]) == pytest.approx(70.0 / 730.0, abs=1e-12)
    assert float(weights["nebraska"]) > (3.0 / 8.0)
    assert float(weights["minnesota"]) < (1.0 / 8.0)


def test_select_gpre_proxy_model_guardrails_reject_marginal_hybrid_q1_loser() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "family": "bridge_timing",
                "eligible_official": True,
                "hybrid_score": 0.049,
                "underlying_mae": 0.048,
                "clean_mae": 0.050,
                "q1_mae": 0.090,
                "q1_mean_error": 0.010,
            },
            {
                "model_key": "process_front_loaded",
                "family": "process_family",
                "eligible_official": True,
                "hybrid_score": 0.050,
                "underlying_mae": 0.051,
                "clean_mae": 0.051,
                "q1_mae": 0.040,
                "q1_mean_error": 0.012,
            },
        ]
    )

    chosen_key, reason = market_service._select_gpre_proxy_model_from_leaderboard(
        leaderboard_df,
        family_preference={"process_family": 0, "bridge_timing": 1},
    )

    assert chosen_key == "process_front_loaded"
    assert reason == "passed_guardrails"


def test_select_gpre_proxy_model_guardrails_fall_back_when_no_candidate_passes() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "family": "bridge_timing",
                "eligible_official": True,
                "hybrid_score": 0.049,
                "underlying_mae": 0.048,
                "clean_mae": 0.050,
                "q1_mae": 0.055,
                "q1_mean_error": 0.090,
            },
            {
                "model_key": "process_front_loaded",
                "family": "process_family",
                "eligible_official": True,
                "hybrid_score": 0.052,
                "underlying_mae": 0.052,
                "clean_mae": 0.051,
                "q1_mae": 0.052,
                "q1_mean_error": 0.080,
            },
        ]
    )

    chosen_key, reason = market_service._select_gpre_proxy_model_from_leaderboard(
        leaderboard_df,
        family_preference={"process_family": 0, "bridge_timing": 1},
    )

    assert chosen_key == "bridge_front_loaded"
    assert reason == "fallback_no_candidate_passed_guardrails"


def test_gpre_overlay_prior_quarter_fitted_frame_uses_selected_model_and_not_official_row() -> None:
    basis_model_result = {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.18,
                    "gpre_proxy_model_key": "process_front_loaded",
                    "official_simple_proxy_usd_per_gal": 0.081,
                    "gpre_proxy_official_usd_per_gal": -0.012,
                    "approx_market_bridge_proxy_usd_per_gal": 0.004,
                    "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.010,
                    "process_proxy_front_loaded_usd_per_gal": -0.012,
                }
            ]
        ),
        "gpre_proxy_model_key": "process_front_loaded",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Front-loaded current",
    }
    rows = _gpre_overlay_fixture_rows(
        ethanol_base=1.54,
        corn_front=4.32,
        gas_front=2.82,
        qd=date(2026, 3, 31),
        obs_dt=date(2026, 3, 28),
    ) + _gpre_overlay_fixture_rows(
        ethanol_base=1.60,
        corn_front=4.50,
        gas_front=3.00,
        qd=date(2026, 6, 30),
        obs_dt=date(2026, 4, 3),
    )

    bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_model_result,
    )

    prior_fitted = float(pd.to_numeric(bundle["gpre_proxy_frames"]["prior_quarter"]["value"], errors="coerce"))
    prior_official = float(pd.to_numeric(bundle["official_frames"]["prior_quarter"]["value"], errors="coerce"))
    assert bundle["gpre_proxy_frames"]["prior_quarter"]["status"] == "ok"
    assert prior_fitted == pytest.approx(-0.012, abs=1e-9)
    assert prior_official != pytest.approx(prior_fitted, abs=1e-9)


def test_gpre_overlay_fitted_row_changes_with_model_choice_while_official_row_stays_fixed() -> None:
    rows = _gpre_overlay_fixture_rows(
        ethanol_base=1.54,
        corn_front=4.32,
        gas_front=2.82,
        qd=date(2026, 3, 31),
        obs_dt=date(2026, 3, 28),
    ) + _gpre_overlay_fixture_rows()
    basis_current = {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.18,
                    "gpre_proxy_model_key": "process_current_quarter_avg",
                    "gpre_proxy_official_usd_per_gal": -0.010,
                    "approx_market_bridge_proxy_usd_per_gal": 0.004,
                    "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.010,
                    "process_proxy_front_loaded_usd_per_gal": -0.020,
                }
            ]
        ),
        "gpre_proxy_model_key": "process_current_quarter_avg",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Current qtr avg",
    }
    basis_front = {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.18,
                    "gpre_proxy_model_key": "process_front_loaded",
                    "gpre_proxy_official_usd_per_gal": -0.020,
                    "approx_market_bridge_proxy_usd_per_gal": 0.004,
                    "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.010,
                    "process_proxy_front_loaded_usd_per_gal": -0.020,
                }
            ]
        ),
        "gpre_proxy_model_key": "process_front_loaded",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Front-loaded current",
    }

    bundle_current = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_current,
    )
    bundle_front = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_front,
    )

    for frame_key in ("prior_quarter", "current_qtd", "next_quarter_thesis"):
        assert bundle_current["official_frames"][frame_key]["value"] == pytest.approx(
            float(pd.to_numeric(bundle_front["official_frames"][frame_key]["value"], errors="coerce")),
            abs=1e-9,
        )
    assert float(pd.to_numeric(bundle_current["gpre_proxy_frames"]["prior_quarter"]["value"], errors="coerce")) == pytest.approx(-0.010, abs=1e-9)
    assert float(pd.to_numeric(bundle_front["gpre_proxy_frames"]["prior_quarter"]["value"], errors="coerce")) == pytest.approx(-0.020, abs=1e-9)


def test_gpre_overlay_prior_quarter_fitted_frame_uses_model_pred_col_when_summary_col_is_blank() -> None:
    rows = _gpre_overlay_fixture_rows(
        ethanol_base=1.54,
        corn_front=4.32,
        gas_front=2.82,
        qd=date(2026, 3, 31),
        obs_dt=date(2026, 3, 28),
    ) + _gpre_overlay_fixture_rows()
    basis_model_result = {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.18,
                    "gpre_proxy_model_key": "process_front_loaded",
                    "gpre_proxy_official_usd_per_gal": np.nan,
                    "approx_market_bridge_proxy_usd_per_gal": 0.004,
                    "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.010,
                    "process_proxy_front_loaded_usd_per_gal": -0.077,
                }
            ]
        ),
        "gpre_proxy_model_key": "process_front_loaded",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Front-loaded current",
    }

    bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_model_result,
    )

    prior_frame = dict(bundle["gpre_proxy_frames"]["prior_quarter"] or {})
    assert prior_frame["status"] == "ok"
    assert prior_frame["source"] == "quarterly_df.process_proxy_front_loaded_usd_per_gal"
    assert float(pd.to_numeric(prior_frame["value"], errors="coerce")) == pytest.approx(-0.077, abs=1e-9)


def test_gpre_overlay_prior_quarter_new_candidate_uses_model_pred_col_when_summary_col_is_blank() -> None:
    rows = _gpre_overlay_fixture_rows(
        ethanol_base=1.54,
        corn_front=4.32,
        gas_front=2.82,
        qd=date(2026, 3, 31),
        obs_dt=date(2026, 3, 28),
    ) + _gpre_overlay_fixture_rows()
    basis_model_result = {
        "quarterly_df": pd.DataFrame(
            [
                {
                    "quarter": date(2026, 3, 31),
                    "weighted_basis_plant_count_usd_per_bu": -0.18,
                    "gpre_proxy_model_key": "process_quarter_open_blend",
                    "gpre_proxy_official_usd_per_gal": np.nan,
                    "official_simple_proxy_usd_per_gal": 0.081,
                    "process_proxy_current_quarter_avg_usd_per_gal": -0.010,
                    "process_proxy_front_loaded_usd_per_gal": -0.020,
                    "process_quarter_open_blend_usd_per_gal": -0.015,
                }
            ]
        ),
        "gpre_proxy_model_key": "process_quarter_open_blend",
        "gpre_proxy_family": "process_blend",
        "gpre_proxy_family_label": "Process blend",
        "gpre_proxy_timing_rule": "Quarter-open/current blend",
    }

    bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_model_result,
    )

    prior_frame = dict(bundle["gpre_proxy_frames"]["prior_quarter"] or {})
    assert prior_frame["status"] == "ok"
    assert prior_frame["source"] == "quarterly_df.process_quarter_open_blend_usd_per_gal"
    assert float(pd.to_numeric(prior_frame["value"], errors="coerce")) == pytest.approx(-0.015, abs=1e-9)


def test_gpre_overlay_quarter_open_blend_uses_snapshot_anchor_and_current_progress_weights() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_qopen_blend_")
    try:
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "official_market_snapshot": {
                    "status": "ok_thesis",
                    "display_quarter": date(2026, 6, 30),
                    "calendar_quarter": date(2026, 6, 30),
                    "current_market": {
                        "ethanol_price": 1.58,
                        "cbot_corn_front_price": 4.42,
                        "natural_gas_price": 3.05,
                    },
                    "current_process": {},
                    "market_meta": {
                        "corn_price": {
                            "official_weighted_corn_basis_usd_per_bu": -0.24,
                            "cbot_corn_front_price_usd_per_bu": 4.42,
                        },
                        "ethanol_price": {
                            "east_geo_spread_usd_per_gal": 0.03,
                        },
                    },
                    "process_status": "ok",
                },
                "official_simple_proxy_usd_per_gal": 0.0123,
                "gpre_proxy_official_usd_per_gal": None,
                "gpre_proxy_model_key": "process_front_loaded",
            },
        )
        rows = _gpre_overlay_fixture_rows(qd=date(2026, 6, 30), obs_dt=date(2026, 4, 3)) + _gpre_overlay_fixture_rows(
            qd=date(2026, 6, 30),
            obs_dt=date(2026, 4, 10),
        )
        basis_current = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_current_quarter_avg",
            "gpre_proxy_family": "process_family",
            "gpre_proxy_family_label": "Process",
            "gpre_proxy_timing_rule": "Current qtr avg",
        }
        basis_front = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_front_loaded",
            "gpre_proxy_family": "process_family",
            "gpre_proxy_family_label": "Process",
            "gpre_proxy_timing_rule": "Front-loaded current",
        }
        basis_blend = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_quarter_open_blend",
            "gpre_proxy_family": "process_blend",
            "gpre_proxy_family_label": "Process blend",
            "gpre_proxy_timing_rule": "Quarter-open/current blend",
        }

        bundle_current = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_current,
        )
        bundle_front = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_front,
        )
        bundle_blend = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_blend,
        )

        anchor_val = float(pd.to_numeric(bundle_front["gpre_proxy_frames"]["quarter_open"]["value"], errors="coerce"))
        current_val = float(pd.to_numeric(bundle_current["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
        blended_val = float(pd.to_numeric(bundle_blend["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))

        assert blended_val == pytest.approx((0.75 * anchor_val) + (0.25 * current_val), abs=1e-9)
        assert bundle_blend["official_frames"]["current_qtd"]["value"] == pytest.approx(
            float(pd.to_numeric(bundle_current["official_frames"]["current_qtd"]["value"], errors="coerce")),
            abs=1e-9,
        )
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_overlay_quarter_open_blend_reverts_to_current_when_snapshot_is_unavailable() -> None:
    rows = _gpre_overlay_fixture_rows() + _gpre_overlay_fixture_rows(obs_dt=date(2026, 4, 10))
    basis_current = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_current_quarter_avg",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Current qtr avg",
    }
    basis_blend = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_quarter_open_blend",
        "gpre_proxy_family": "process_blend",
        "gpre_proxy_family_label": "Process blend",
        "gpre_proxy_timing_rule": "Quarter-open/current blend",
    }

    bundle_current = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 11),
        ticker_root=None,
        gpre_basis_model_result=basis_current,
    )
    bundle_blend = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 11),
        ticker_root=None,
        gpre_basis_model_result=basis_blend,
    )

    assert float(pd.to_numeric(bundle_blend["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")) == pytest.approx(
        float(pd.to_numeric(bundle_current["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")),
        abs=1e-9,
    )


def test_gpre_overlay_ops_penalty_candidate_applies_capped_penalty_from_local_docs() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_ops_penalty_")
    try:
        transcript_dir = ticker_root / "earnings_transcripts"
        transcript_dir.mkdir(parents=True, exist_ok=True)
        (transcript_dir / "GPRE_Q2_2026_transcript.txt").write_text(
            "The quarter included planned maintenance and downtime. Utilization was 88%.",
            encoding="utf-8",
        )
        rows = _gpre_overlay_fixture_rows()
        basis_front = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_front_loaded",
            "gpre_proxy_family": "process_family",
            "gpre_proxy_family_label": "Process",
            "gpre_proxy_timing_rule": "Front-loaded current",
        }
        basis_penalty = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_front_loaded_ops_penalty",
            "gpre_proxy_family": "process_ops_penalty",
            "gpre_proxy_family_label": "Process + ops penalty",
            "gpre_proxy_timing_rule": "Front-loaded current - ops penalty",
        }

        bundle_front = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_front,
        )
        bundle_penalty = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_penalty,
        )

        delta = float(pd.to_numeric(bundle_front["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")) - float(
            pd.to_numeric(bundle_penalty["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")
        )
        assert delta == pytest.approx(0.04, abs=1e-9)
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_overlay_ops_penalty_candidate_leaves_clean_quarter_unpenalized() -> None:
    rows = _gpre_overlay_fixture_rows()
    basis_front = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_front_loaded",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Front-loaded current",
    }
    basis_penalty = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_front_loaded_ops_penalty",
        "gpre_proxy_family": "process_ops_penalty",
        "gpre_proxy_family_label": "Process + ops penalty",
        "gpre_proxy_timing_rule": "Front-loaded current - ops penalty",
    }

    bundle_front = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_front,
    )
    bundle_penalty = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_penalty,
    )

    assert float(pd.to_numeric(bundle_front["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")) == pytest.approx(
        float(pd.to_numeric(bundle_penalty["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")),
        abs=1e-9,
    )


def test_gpre_overlay_quarter_open_blend_ops_penalty_candidate_combines_blend_and_bounded_penalty() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_qopen_blend_ops_")
    try:
        transcript_dir = ticker_root / "earnings_transcripts"
        transcript_dir.mkdir(parents=True, exist_ok=True)
        (transcript_dir / "GPRE_Q2_2026_transcript.txt").write_text(
            "The quarter included planned maintenance and downtime. Utilization was 88%.",
            encoding="utf-8",
        )
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "official_market_snapshot": {
                    "status": "ok_thesis",
                    "display_quarter": date(2026, 6, 30),
                    "calendar_quarter": date(2026, 6, 30),
                    "current_market": {
                        "ethanol_price": 1.58,
                        "cbot_corn_front_price": 4.42,
                        "natural_gas_price": 3.05,
                    },
                    "current_process": {},
                    "market_meta": {},
                    "process_status": "ok",
                },
                "official_simple_proxy_usd_per_gal": 0.0123,
                "gpre_proxy_official_usd_per_gal": None,
                "gpre_proxy_model_key": "process_front_loaded",
            },
        )
        rows = _gpre_overlay_fixture_rows(qd=date(2026, 6, 30), obs_dt=date(2026, 4, 3)) + _gpre_overlay_fixture_rows(
            qd=date(2026, 6, 30),
            obs_dt=date(2026, 4, 10),
        )
        basis_blend = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_quarter_open_blend",
            "gpre_proxy_family": "process_blend",
            "gpre_proxy_family_label": "Process blend",
            "gpre_proxy_timing_rule": "Quarter-open/current blend",
        }
        basis_combo = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_quarter_open_blend_ops_penalty",
            "gpre_proxy_family": "process_blend_ops_penalty",
            "gpre_proxy_family_label": "Process blend + ops penalty",
            "gpre_proxy_timing_rule": "Quarter-open/current blend - ops penalty",
        }

        bundle_blend = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_blend,
        )
        bundle_combo = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_combo,
        )

        current_delta = float(pd.to_numeric(bundle_blend["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")) - float(
            pd.to_numeric(bundle_combo["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")
        )
        quarter_open_delta = float(pd.to_numeric(bundle_blend["gpre_proxy_frames"]["quarter_open"]["value"], errors="coerce")) - float(
            pd.to_numeric(bundle_combo["gpre_proxy_frames"]["quarter_open"]["value"], errors="coerce")
        )
        current_helper = dict((bundle_combo.get("gpre_proxy_formula_helpers") or {}).get("current_qtd") or {})

        assert current_delta == pytest.approx(0.04, abs=1e-9)
        assert quarter_open_delta == pytest.approx(0.04, abs=1e-9)
        assert str(current_helper.get("live_preview_mode") or "") == "exact_formula"
        assert "ops penalty" in str(current_helper.get("live_preview_note") or "").lower()
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_overlay_quarter_open_blend_ops_penalty_candidate_reverts_to_plain_blend_without_signal() -> None:
    rows = _gpre_overlay_fixture_rows() + _gpre_overlay_fixture_rows(obs_dt=date(2026, 4, 10))
    basis_blend = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_quarter_open_blend",
        "gpre_proxy_family": "process_blend",
        "gpre_proxy_family_label": "Process blend",
        "gpre_proxy_timing_rule": "Quarter-open/current blend",
    }
    basis_combo = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_quarter_open_blend_ops_penalty",
        "gpre_proxy_family": "process_blend_ops_penalty",
        "gpre_proxy_family_label": "Process blend + ops penalty",
        "gpre_proxy_timing_rule": "Quarter-open/current blend - ops penalty",
    }

    bundle_blend = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 11),
        ticker_root=None,
        gpre_basis_model_result=basis_blend,
    )
    bundle_combo = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 11),
        ticker_root=None,
        gpre_basis_model_result=basis_combo,
    )
    bundle_combo_repeat = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 11),
        ticker_root=None,
        gpre_basis_model_result=basis_combo,
    )

    combo_val = float(pd.to_numeric(bundle_combo["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
    repeat_val = float(pd.to_numeric(bundle_combo_repeat["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
    blend_val = float(pd.to_numeric(bundle_blend["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
    next_helper = dict((bundle_combo.get("gpre_proxy_formula_helpers") or {}).get("next_quarter_thesis") or {})

    assert combo_val == pytest.approx(blend_val, abs=1e-9)
    assert repeat_val == pytest.approx(combo_val, abs=1e-9)
    assert str(next_helper.get("live_preview_mode") or "") == "reduced_form_approximation"
    assert "thesis leg" in str(next_helper.get("live_preview_note") or "").lower()


def test_gpre_overlay_quarter_open_blend_hedge_realization_candidate_is_supported_and_pulls_toward_hedge_reference() -> None:
    rows = _gpre_overlay_fixture_rows(qd=date(2025, 9, 30), obs_dt=date(2025, 8, 1)) + _gpre_overlay_fixture_rows(
        qd=date(2025, 9, 30),
        obs_dt=date(2025, 8, 8),
    )
    prior_quarterly = pd.DataFrame(
        [
            {
                "quarter": date(2025, 6, 30),
                "weighted_basis_plant_count_usd_per_bu": -0.20,
                "gpre_proxy_official_usd_per_gal": -0.012,
                "approx_market_bridge_proxy_usd_per_gal": 0.004,
                "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                "process_proxy_current_quarter_avg_usd_per_gal": -0.012,
                "process_proxy_front_loaded_usd_per_gal": -0.200,
            }
        ]
    )
    basis_blend = {
        "quarterly_df": prior_quarterly,
        "gpre_proxy_model_key": "process_quarter_open_blend",
        "gpre_proxy_family": "process_blend",
        "gpre_proxy_family_label": "Process blend",
        "gpre_proxy_timing_rule": "Quarter-open/current blend",
    }
    basis_hedge = {
        "quarterly_df": prior_quarterly,
        "gpre_proxy_model_key": "process_quarter_open_blend_hedge_realization",
        "gpre_proxy_family": "process_blend_hedge_realization",
        "gpre_proxy_family_label": "Process blend + hedge realization",
        "gpre_proxy_timing_rule": "Quarter-open/current blend + capped hedge realization adjustment",
    }

    bundle_blend = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2025, 8, 10),
        ticker_root=None,
        gpre_basis_model_result=basis_blend,
    )
    bundle_hedge = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2025, 8, 10),
        ticker_root=None,
        gpre_basis_model_result=basis_hedge,
    )

    blend_val = float(pd.to_numeric(bundle_blend["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
    hedge_val = float(pd.to_numeric(bundle_hedge["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
    current_helper = dict((bundle_hedge.get("gpre_proxy_formula_helpers") or {}).get("current_qtd") or {})

    assert hedge_val < blend_val
    assert str(current_helper.get("status") or "") == "ok"
    assert "hedge-realization adjustment" in str(current_helper.get("live_preview_note") or "").lower()


def test_gpre_overlay_quarter_open_blend_exec_penalty_candidate_is_stronger_than_plain_ops_penalty_in_severe_quarter() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_exec_penalty_")
    try:
        transcript_dir = ticker_root / "earnings_transcripts"
        transcript_dir.mkdir(parents=True, exist_ok=True)
        (transcript_dir / "GPRE_Q2_2026_transcript.txt").write_text(
            "The quarter included planned maintenance, outage downtime, and a cold snap. Utilization was 82%.",
            encoding="utf-8",
        )
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "official_market_snapshot": {
                    "status": "ok_thesis",
                    "display_quarter": date(2026, 6, 30),
                    "calendar_quarter": date(2026, 6, 30),
                    "current_market": {
                        "ethanol_price": 1.58,
                        "cbot_corn_front_price": 4.42,
                        "natural_gas_price": 3.05,
                    },
                    "current_process": {},
                    "market_meta": {},
                    "process_status": "ok",
                },
                "official_simple_proxy_usd_per_gal": 0.0123,
                "gpre_proxy_official_usd_per_gal": None,
                "gpre_proxy_model_key": "process_front_loaded",
            },
        )
        rows = _gpre_overlay_fixture_rows(qd=date(2026, 6, 30), obs_dt=date(2026, 4, 3)) + _gpre_overlay_fixture_rows(
            qd=date(2026, 6, 30),
            obs_dt=date(2026, 4, 10),
        )
        basis_plain_ops = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_quarter_open_blend_ops_penalty",
            "gpre_proxy_family": "process_blend_ops_penalty",
            "gpre_proxy_family_label": "Process blend + ops penalty",
            "gpre_proxy_timing_rule": "Quarter-open/current blend - ops penalty",
        }
        basis_exec = {
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_quarter_open_blend_exec_penalty",
            "gpre_proxy_family": "process_blend_exec_penalty",
            "gpre_proxy_family_label": "Process blend + severe ops penalty",
            "gpre_proxy_timing_rule": "Quarter-open/current blend - severe execution penalty",
        }

        bundle_plain_ops = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_plain_ops,
        )
        bundle_exec = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 11),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_exec,
        )

        plain_val = float(pd.to_numeric(bundle_plain_ops["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
        exec_val = float(pd.to_numeric(bundle_exec["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce"))
        exec_helper = dict((bundle_exec.get("gpre_proxy_formula_helpers") or {}).get("current_qtd") or {})

        assert plain_val - exec_val == pytest.approx(0.02, abs=1e-9)
        assert str(exec_helper.get("status") or "") == "ok"
        assert "severe execution penalty" in str(exec_helper.get("live_preview_note") or "").lower()
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_overlay_ethanol_geo_candidate_adds_small_capped_east_premium_and_reverts_without_east_data() -> None:
    rows = _gpre_overlay_fixture_rows()
    no_east_rows = [rec for rec in rows if str(rec.get("series_key") or "") not in {"ethanol_illinois", "ethanol_indiana"}]
    basis_front = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_front_loaded",
        "gpre_proxy_family": "process_family",
        "gpre_proxy_family_label": "Process",
        "gpre_proxy_timing_rule": "Front-loaded current",
    }
    basis_geo = {
        "quarterly_df": pd.DataFrame(),
        "gpre_proxy_model_key": "process_front_loaded_ethanol_geo",
        "gpre_proxy_family": "process_geo",
        "gpre_proxy_family_label": "Process + ethanol geo",
        "gpre_proxy_timing_rule": "Front-loaded current + east spread",
    }

    bundle_front = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_front,
    )
    bundle_geo = market_service.build_gpre_overlay_proxy_preview_bundle(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_geo,
    )
    geo_delta = float(pd.to_numeric(bundle_geo["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")) - float(
        pd.to_numeric(bundle_front["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")
    )
    assert 0.0 < geo_delta <= 0.04

    bundle_geo_no_east = market_service.build_gpre_overlay_proxy_preview_bundle(
        no_east_rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_geo,
    )
    bundle_front_no_east = market_service.build_gpre_overlay_proxy_preview_bundle(
        no_east_rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result=basis_front,
    )
    assert float(pd.to_numeric(bundle_geo_no_east["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")) == pytest.approx(
        float(pd.to_numeric(bundle_front_no_east["gpre_proxy_frames"]["current_qtd"]["value"], errors="coerce")),
        abs=1e-9,
    )


def test_gpre_proxy_leaderboard_builder_adds_dual_baseline_and_official_diff_diagnostics() -> None:
    quarterly_df = pd.DataFrame(
        [
            {
                "quarter": date(2025, 3, 31),
                "official_simple_proxy_usd_per_gal": 0.10,
                "bridge_proxy_front_loaded_usd_per_gal": 0.16,
                "process_front_loaded_usd_per_gal": 0.14,
                "process_front_loaded_ethanol_geo_usd_per_gal": 0.18,
            },
            {
                "quarter": date(2025, 6, 30),
                "official_simple_proxy_usd_per_gal": 0.08,
                "bridge_proxy_front_loaded_usd_per_gal": 0.13,
                "process_front_loaded_usd_per_gal": 0.11,
                "process_front_loaded_ethanol_geo_usd_per_gal": 0.15,
            },
        ]
    )
    metrics_df = pd.DataFrame(
        [
            {"model_key": "bridge_front_loaded", "split": "clean_reported_window", "mae": 0.060},
            {"model_key": "bridge_front_loaded", "split": "diag_underlying", "mae": 0.070},
            {"model_key": "bridge_front_loaded", "split": "full", "mean_error": 0.012, "q1_mae": 0.08, "q2_mae": 0.06, "q3_mae": 0.05, "q4_mae": 0.04, "q1_mean_error": 0.01, "q2_mean_error": 0.00, "q3_mean_error": 0.00, "q4_mean_error": 0.00, "top_miss_quarters": "2025-Q2, 2025-Q1"},
            {"model_key": "bridge_front_loaded", "split": "test", "mae": 0.065, "correlation": 0.91, "mean_error": 0.010, "sign_hit_rate": 0.75},
            {"model_key": "process_front_loaded", "split": "clean_reported_window", "mae": 0.058},
            {"model_key": "process_front_loaded", "split": "diag_underlying", "mae": 0.072},
            {"model_key": "process_front_loaded", "split": "full", "mean_error": -0.005, "q1_mae": 0.05, "q2_mae": 0.07, "q3_mae": 0.06, "q4_mae": 0.05, "q1_mean_error": -0.01, "q2_mean_error": 0.01, "q3_mean_error": 0.00, "q4_mean_error": 0.00, "top_miss_quarters": "2025-Q1"},
            {"model_key": "process_front_loaded", "split": "test", "mae": 0.061, "correlation": 0.92, "mean_error": -0.004, "sign_hit_rate": 0.80},
            {"model_key": "process_front_loaded_ethanol_geo", "split": "clean_reported_window", "mae": 0.057},
            {"model_key": "process_front_loaded_ethanol_geo", "split": "diag_underlying", "mae": 0.068},
            {"model_key": "process_front_loaded_ethanol_geo", "split": "full", "mean_error": 0.004, "q1_mae": 0.06, "q2_mae": 0.05, "q3_mae": 0.04, "q4_mae": 0.04, "q1_mean_error": 0.005, "q2_mean_error": 0.002, "q3_mean_error": 0.001, "q4_mean_error": 0.000, "top_miss_quarters": "2025-Q2"},
            {"model_key": "process_front_loaded_ethanol_geo", "split": "test", "mae": 0.059, "correlation": 0.94, "mean_error": 0.003, "sign_hit_rate": 0.85},
        ]
    )
    specs = [
        {"model_key": "bridge_front_loaded", "pred_col": "bridge_proxy_front_loaded_usd_per_gal", "family": "bridge_timing", "family_label": "Bridge timing", "timing_rule": "Front-loaded current", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_front_loaded", "pred_col": "process_front_loaded_usd_per_gal", "family": "process_family", "family_label": "Process", "timing_rule": "Front-loaded current", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_front_loaded_ethanol_geo", "pred_col": "process_front_loaded_ethanol_geo_usd_per_gal", "family": "process_geo", "family_label": "Process + ethanol geo", "timing_rule": "Front-loaded current + east spread", "eligible_official": True, "preview_supported": True},
    ]

    leaderboard_df = market_service._build_gpre_proxy_leaderboard(
        quarterly_df,
        metrics_df,
        specs,
        incumbent_baseline_model_key="bridge_front_loaded",
        process_comparator_model_key="process_front_loaded",
        new_candidate_keys={"process_front_loaded_ethanol_geo"},
    )
    leaderboard_df = market_service._annotate_gpre_selection_guardrails(
        leaderboard_df,
        clean_mae_slack=0.010,
        q1_mae_slack=0.015,
        q1_bias_limit=0.050,
    )
    leaderboard_df, _, _ = market_service._annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_candidate_model_key="process_front_loaded_ethanol_geo",
        new_candidate_keys={"process_front_loaded_ethanol_geo"},
    )

    assert {
        "avg_abs_diff_vs_official",
        "diff_quarters_gt_0_02_vs_official",
        "diff_quarters_gt_0_05_vs_official",
        "bias_direction",
        "baseline_status",
        "selection_guard_pass",
        "selection_guard_failures",
        "promotion_guard_pass",
        "promotion_guard_failures",
        "incremental_value_status",
        "promotion_guard_reason",
        "top_miss_quarters",
        "hard_quarter_mae",
        "hard_quarter_top_miss_quarters",
        "live_preview_mae",
        "live_preview_max_error",
        "live_preview_quality_status",
    } <= set(leaderboard_df.columns)
    status_map = dict(zip(leaderboard_df["model_key"].astype(str), leaderboard_df["baseline_status"].astype(str)))
    assert status_map["bridge_front_loaded"] == "incumbent_current_state"
    assert status_map["process_front_loaded"] == "incumbent_process_comparator"
    assert status_map["process_front_loaded_ethanol_geo"] == "new_candidate"
    inc_status = leaderboard_df.set_index("model_key").loc["process_front_loaded_ethanol_geo", "incremental_value_status"]
    assert str(inc_status) == "low"


def test_gpre_proxy_promotion_guard_blocks_low_incremental_value_candidate() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.090,
                "hybrid_score": 0.1180,
                "clean_mae": 0.0850,
                "q1_mae": 0.0700,
                "underlying_mae": 0.1400,
                "full_mean_error": 0.010,
                "avg_abs_diff_vs_official": 0.030,
                "diff_quarters_gt_0_02_vs_official": 5,
            },
            {
                "model_key": "process_front_loaded_ethanol_geo",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "acceptable",
                "hard_quarter_mae": 0.085,
                "hybrid_score": 0.1165,
                "clean_mae": 0.0845,
                "q1_mae": 0.0710,
                "underlying_mae": 0.1200,
                "full_mean_error": 0.004,
                "avg_abs_diff_vs_official": 0.009,
                "diff_quarters_gt_0_02_vs_official": 1,
            },
        ]
    )

    annotated, winner_key, reason = market_service._annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_candidate_model_key="process_front_loaded_ethanol_geo",
        new_candidate_keys={"process_front_loaded_ethanol_geo"},
    )

    assert winner_key == "bridge_front_loaded"
    assert reason == "incremental_distance_vs_official_too_low"
    chosen = annotated[annotated["model_key"].astype(str) == "process_front_loaded_ethanol_geo"].iloc[0]
    assert bool(chosen["promotion_guard_pass"]) is False
    assert "incremental_distance_vs_official_too_low" in str(chosen["promotion_guard_failures"] or "")


def test_gpre_proxy_promotion_guard_promotes_expanded_best_candidate_when_bridge_thresholds_are_beaten() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.090,
                "hybrid_score": 0.1187,
                "clean_mae": 0.0847,
                "q1_mae": 0.0919,
                "underlying_mae": 0.1527,
                "full_mean_error": 0.0681,
                "avg_abs_diff_vs_official": 0.0739,
                "diff_quarters_gt_0_02_vs_official": 12,
            },
            {
                "model_key": "process_front_loaded",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.100,
                "hybrid_score": 0.0735,
                "clean_mae": 0.0772,
                "q1_mae": 0.1233,
                "underlying_mae": 0.0697,
                "full_mean_error": -0.0126,
                "avg_abs_diff_vs_official": 0.0117,
                "diff_quarters_gt_0_02_vs_official": 3,
            },
            {
                "model_key": "process_quarter_open_blend",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "acceptable",
                "hard_quarter_mae": 0.082,
                "hybrid_score": 0.0681,
                "clean_mae": 0.0684,
                "q1_mae": 0.0968,
                "underlying_mae": 0.0679,
                "full_mean_error": -0.0193,
                "avg_abs_diff_vs_official": 0.0483,
                "diff_quarters_gt_0_02_vs_official": 8,
            },
        ]
    )

    annotated, winner_key, reason = market_service._annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_candidate_model_key="process_quarter_open_blend",
        new_candidate_keys={"process_quarter_open_blend"},
    )

    assert winner_key == "process_quarter_open_blend"
    assert reason == "promoted_over_incumbent_baseline"
    blend_row = annotated[annotated["model_key"].astype(str) == "process_quarter_open_blend"].iloc[0]
    assert bool(blend_row["promotion_guard_pass"]) is True
    assert str(blend_row["incremental_value_status"] or "") == "high"
    assert bool(blend_row["expanded_best_candidate"]) is True
    assert bool(blend_row["production_winner"]) is True


def test_gpre_proxy_promotion_guard_blocks_q1_loser_and_preview_incomplete_candidate() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.090,
                "hybrid_score": 0.1187,
                "clean_mae": 0.0847,
                "q1_mae": 0.0919,
                "underlying_mae": 0.1527,
                "full_mean_error": 0.0100,
                "avg_abs_diff_vs_official": 0.0739,
                "diff_quarters_gt_0_02_vs_official": 12,
            },
            {
                "model_key": "process_quarter_open_blend",
                "eligible_official": True,
                "preview_supported": False,
                "live_preview_quality_status": "not_faithful_enough",
                "hard_quarter_mae": 0.082,
                "hybrid_score": 0.0600,
                "clean_mae": 0.0700,
                "q1_mae": 0.1300,
                "underlying_mae": 0.0600,
                "full_mean_error": -0.0100,
                "avg_abs_diff_vs_official": 0.0400,
                "diff_quarters_gt_0_02_vs_official": 7,
            },
        ]
    )

    annotated, winner_key, reason = market_service._annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_candidate_model_key="process_quarter_open_blend",
        new_candidate_keys={"process_quarter_open_blend"},
    )

    assert winner_key == "bridge_front_loaded"
    assert reason == "preview_support_incomplete"
    blend_row = annotated[annotated["model_key"].astype(str) == "process_quarter_open_blend"].iloc[0]
    assert bool(blend_row["promotion_guard_pass"]) is False
    assert "preview_support_incomplete" in str(blend_row["promotion_guard_failures"] or "")
    assert "q1_mae_exceeded_incumbent_tolerance" in str(blend_row["promotion_guard_failures"] or "")


def test_gpre_selection_vs_promotion_story_is_explicit_for_all_three_core_cases() -> None:
    disagree_story, disagree_expl = market_service._gpre_selection_vs_promotion_story(
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_row={
            "model_key": "process_quarter_open_blend",
            "selection_guard_pass": False,
            "selection_guard_reason": "q1_mean_error_exceeded_bias_limit",
            "selection_guard_failures": "q1_mean_error_exceeded_bias_limit",
            "promotion_guard_pass": True,
            "promotion_guard_reason": "passed_promotion_guardrails",
            "promotion_guard_failures": "",
        },
        production_winner_key="process_quarter_open_blend",
    )
    retain_story, retain_expl = market_service._gpre_selection_vs_promotion_story(
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_row={
            "model_key": "process_quarter_open_blend",
            "selection_guard_pass": False,
            "selection_guard_reason": "q1_mean_error_exceeded_bias_limit",
            "selection_guard_failures": "q1_mean_error_exceeded_bias_limit",
            "promotion_guard_pass": False,
            "promotion_guard_reason": "preview_support_incomplete",
            "promotion_guard_failures": "preview_support_incomplete; q1_mae_exceeded_incumbent_tolerance",
        },
        production_winner_key="bridge_front_loaded",
    )
    agree_story, agree_expl = market_service._gpre_selection_vs_promotion_story(
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_row={
            "model_key": "process_quarter_open_blend_ops_penalty",
            "selection_guard_pass": True,
            "selection_guard_reason": "passed_guardrails",
            "selection_guard_failures": "",
            "promotion_guard_pass": True,
            "promotion_guard_reason": "passed_promotion_guardrails",
            "promotion_guard_failures": "",
        },
        production_winner_key="process_quarter_open_blend_ops_penalty",
    )

    assert "failed the broad selection guardrails" in disagree_story.lower()
    assert "selection and promotion disagreed" in disagree_expl.lower()
    assert "failed both selection and promotion guardrails" in retain_story.lower()
    assert "retaining the incumbent" in retain_expl.lower()
    assert "passing both selection and promotion guardrails" in agree_story.lower()
    assert "selection and promotion agreed" in agree_expl.lower()


def test_gpre_preview_quality_classification_thresholds_cover_close_to_not_faithful() -> None:
    assert market_service._gpre_preview_quality_status(0.009, 0.019) == "close"
    assert market_service._gpre_preview_quality_status(0.018, 0.039) == "acceptable"
    assert market_service._gpre_preview_quality_status(0.029, 0.059) == "loose"
    assert market_service._gpre_preview_quality_status(0.031, 0.061) == "not_faithful_enough"


def test_gpre_process_blend_formula_helper_missing_anchor_uses_full_current_leg() -> None:
    full_current = 0.120
    ethanol = 1.500
    current_noneth = full_current - ethanol

    helper = market_service._gpre_process_blend_formula_helper(
        anchor_proxy=None,
        current_nonethanol=current_noneth,
        quarter_open_weight=0.75,
        current_weight=0.25,
        penalty=0.02,
        phase_label="current",
    )

    preview_value = market_service._gpre_evaluate_formula_helper_payload(helper, ethanol)
    old_weighted_preview = (0.25 * ethanol) + (0.25 * current_noneth) - 0.02

    assert str(helper.get("status") or "") == "ok"
    assert float(pd.to_numeric(helper.get("slope"), errors="coerce")) == pytest.approx(1.0, abs=1e-12)
    assert float(pd.to_numeric(helper.get("intercept"), errors="coerce")) == pytest.approx(current_noneth - 0.02, abs=1e-12)
    assert preview_value == pytest.approx(full_current - 0.02, abs=1e-12)
    assert abs(float(preview_value) - (full_current - 0.02)) < 1e-12
    assert abs(old_weighted_preview - (full_current - 0.02)) > 0.05
    assert "collapses to the current observed leg" in str(helper.get("live_preview_note") or "").lower()


def test_gpre_preview_accuracy_for_blend_family_reports_all_modes_and_is_close_after_missing_anchor_fix() -> None:
    quarterly_df = pd.DataFrame(
        [
            {
                "quarter": date(2023, 3, 31),
                "weighted_ethanol_benchmark_usd_per_gal": 1.50,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.120,
                "process_quarter_open_anchor_usd_per_gal": np.nan,
                "quarter_open_weight": 0.75,
                "current_weight": 0.25,
                "ops_penalty_usd_per_gal": 0.020,
                "ops_total_execution_penalty_usd_per_gal": 0.030,
                "process_quarter_open_blend_usd_per_gal": 0.120,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.100,
                "process_quarter_open_blend_exec_penalty_usd_per_gal": 0.090,
            },
            {
                "quarter": date(2023, 6, 30),
                "weighted_ethanol_benchmark_usd_per_gal": 1.60,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.180,
                "process_quarter_open_anchor_usd_per_gal": 0.090,
                "quarter_open_weight": 0.50,
                "current_weight": 0.50,
                "ops_penalty_usd_per_gal": 0.010,
                "ops_total_execution_penalty_usd_per_gal": 0.015,
                "process_quarter_open_blend_usd_per_gal": 0.135,
                "process_quarter_open_blend_ops_penalty_usd_per_gal": 0.125,
                "process_quarter_open_blend_exec_penalty_usd_per_gal": 0.120,
            },
        ]
    )

    for model_key, pred_col in (
        ("process_quarter_open_blend", "process_quarter_open_blend_usd_per_gal"),
        ("process_quarter_open_blend_ops_penalty", "process_quarter_open_blend_ops_penalty_usd_per_gal"),
        ("process_quarter_open_blend_exec_penalty", "process_quarter_open_blend_exec_penalty_usd_per_gal"),
    ):
        stats = market_service._gpre_preview_accuracy_for_model(
            quarterly_df,
            model_key=model_key,
            pred_col=pred_col,
        )

        assert {"prior", "quarter_open", "current", "next"} <= set(stats["preview_accuracy"].keys())
        assert stats["preview_accuracy"]["current"]["preview_mae"] is not None
        assert stats["preview_accuracy"]["current"]["preview_max_error"] is not None
        assert stats["preview_accuracy"]["next"]["preview_mae"] is not None
        assert stats["preview_accuracy"]["next"]["preview_max_error"] is not None
        assert stats["live_preview_mae"] == pytest.approx(0.0, abs=1e-12)
        assert stats["live_preview_max_error"] == pytest.approx(0.0, abs=1e-12)
        assert str(stats["live_preview_quality_status"] or "") == "close"
        assert str(stats["live_preview_worst_phase"] or "") == ""


def test_gpre_preview_accuracy_next_uses_real_formula_helper_evaluation(monkeypatch: pytest.MonkeyPatch) -> None:
    quarterly_df = pd.DataFrame(
        [
            {
                "quarter": date(2023, 3, 31),
                "weighted_ethanol_benchmark_usd_per_gal": 1.50,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.120,
                "process_quarter_open_anchor_usd_per_gal": np.nan,
                "quarter_open_weight": 0.75,
                "current_weight": 0.25,
                "process_quarter_open_blend_usd_per_gal": 0.120,
            },
            {
                "quarter": date(2023, 6, 30),
                "weighted_ethanol_benchmark_usd_per_gal": np.nan,
                "process_proxy_current_quarter_avg_usd_per_gal": 0.180,
                "process_quarter_open_anchor_usd_per_gal": 0.090,
                "quarter_open_weight": 0.50,
                "current_weight": 0.50,
                "process_quarter_open_blend_usd_per_gal": 0.135,
            },
        ]
    )

    helper_calls: list[tuple[dict[str, object], object]] = []
    original_eval = market_service._gpre_evaluate_formula_helper_payload

    def _tracking_eval(helper: dict[str, object], ethanol_value: object) -> float | None:
        helper_calls.append((dict(helper or {}), ethanol_value))
        return original_eval(helper, ethanol_value)

    monkeypatch.setattr(market_service, "_gpre_evaluate_formula_helper_payload", _tracking_eval)
    stats = market_service._gpre_preview_accuracy_for_model(
        quarterly_df,
        model_key="process_quarter_open_blend",
        pred_col="process_quarter_open_blend_usd_per_gal",
    )

    assert int(stats["preview_accuracy"]["next"]["preview_test_count"] or 0) >= 1
    assert len(helper_calls) == 4


def test_gpre_preview_accuracy_for_basis_passthrough_candidate_stays_close() -> None:
    quarterly_df = pd.DataFrame(
        [
            {
                "quarter": date(2023, 3, 31),
                "weighted_ethanol_benchmark_usd_per_gal": 1.50,
                "process_basis_passthrough_beta35_usd_per_gal": 0.120,
            },
            {
                "quarter": date(2023, 6, 30),
                "weighted_ethanol_benchmark_usd_per_gal": 1.60,
                "process_basis_passthrough_beta35_usd_per_gal": 0.180,
            },
        ]
    )

    stats = market_service._gpre_preview_accuracy_for_model(
        quarterly_df,
        model_key="process_basis_passthrough_beta35",
        pred_col="process_basis_passthrough_beta35_usd_per_gal",
    )

    assert stats["live_preview_mae"] == pytest.approx(0.0, abs=1e-12)
    assert stats["live_preview_max_error"] == pytest.approx(0.0, abs=1e-12)
    assert str(stats["live_preview_quality_status"] or "") == "close"



def test_gpre_overlay_quarter_open_blend_helper_note_mentions_current_leg_collapse_without_anchor() -> None:
    bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
        _gpre_overlay_fixture_rows() + _gpre_overlay_fixture_rows(obs_dt=date(2026, 4, 10)),
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 11),
        ticker_root=None,
        gpre_basis_model_result={
            "quarterly_df": pd.DataFrame(),
            "gpre_proxy_model_key": "process_quarter_open_blend",
            "gpre_proxy_family": "process_blend",
            "gpre_proxy_family_label": "Process blend",
            "gpre_proxy_timing_rule": "Quarter-open/current blend",
        },
    )

    current_helper = dict((bundle.get("gpre_proxy_formula_helpers") or {}).get("current_qtd") or {})
    assert "collapses to the current observed leg" in str(current_helper.get("live_preview_note") or "").lower()


def test_gpre_hedge_realization_value_caps_weight_and_reverts_without_share() -> None:
    pulled = market_service._gpre_hedge_realization_value(
        0.100,
        disclosed_share=0.60,
        pattern_share=0.10,
        disclosed_reference=0.020,
        pattern_reference=0.080,
        cap=0.35,
    )
    reverted = market_service._gpre_hedge_realization_value(
        0.100,
        disclosed_share=0.0,
        pattern_share=0.0,
        disclosed_reference=0.020,
        pattern_reference=0.080,
        cap=0.35,
    )

    assert pulled == pytest.approx((0.65 * 0.100) + (0.35 * 0.020), abs=1e-9)
    assert reverted == pytest.approx(0.100, abs=1e-9)


def test_gpre_execution_penalty_details_adds_severe_penalty_and_caps_total() -> None:
    severe = market_service._gpre_execution_penalty_details(
        0.04,
        "outage, delay, cold snap",
        82.0,
    )
    clean = market_service._gpre_execution_penalty_details(
        0.0,
        "",
        92.0,
    )

    assert float(severe["extra_execution_penalty_usd_per_gal"]) == pytest.approx(0.02, abs=1e-9)
    assert float(severe["total_execution_penalty_usd_per_gal"]) == pytest.approx(0.06, abs=1e-9)
    assert int(severe["severe_term_flag"]) == 1
    assert int(severe["very_low_util_flag"]) == 1
    assert float(clean["total_execution_penalty_usd_per_gal"]) == pytest.approx(0.0, abs=1e-9)


def test_gpre_proxy_promotion_guard_blocks_loose_preview_and_hard_quarter_regression() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.060,
                "hybrid_score": 0.1187,
                "clean_mae": 0.0847,
                "q1_mae": 0.0919,
                "underlying_mae": 0.1527,
                "full_mean_error": 0.0100,
                "avg_abs_diff_vs_official": 0.0739,
                "diff_quarters_gt_0_02_vs_official": 12,
            },
            {
                "model_key": "process_quarter_open_blend_exec_penalty",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "loose",
                "hard_quarter_mae": 0.080,
                "hybrid_score": 0.0600,
                "clean_mae": 0.0700,
                "q1_mae": 0.0950,
                "underlying_mae": 0.0600,
                "full_mean_error": -0.0100,
                "avg_abs_diff_vs_official": 0.0400,
                "diff_quarters_gt_0_02_vs_official": 7,
            },
        ]
    )

    annotated, winner_key, reason = market_service._annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_candidate_model_key="process_quarter_open_blend_exec_penalty",
        new_candidate_keys={"process_quarter_open_blend_exec_penalty"},
    )

    assert winner_key == "bridge_front_loaded"
    assert reason == "live_preview_quality_not_faithful_enough"
    challenger = annotated[annotated["model_key"].astype(str) == "process_quarter_open_blend_exec_penalty"].iloc[0]
    assert "live_preview_quality_not_faithful_enough" in str(challenger["promotion_guard_failures"] or "")
    assert "hard_quarter_mae_materially_worse_than_incumbent" in str(challenger["promotion_guard_failures"] or "")


def test_gpre_proxy_promotion_guard_allows_strong_blend_exec_candidate_once_preview_is_close() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "model_key": "bridge_front_loaded",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.1017,
                "hybrid_score": 0.1187,
                "clean_mae": 0.0847,
                "q1_mae": 0.0919,
                "underlying_mae": 0.1527,
                "full_mean_error": 0.0100,
                "avg_abs_diff_vs_official": 0.0739,
                "diff_quarters_gt_0_02_vs_official": 12,
            },
            {
                "model_key": "process_quarter_open_blend_exec_penalty",
                "eligible_official": True,
                "preview_supported": True,
                "live_preview_quality_status": "close",
                "hard_quarter_mae": 0.0649,
                "hybrid_score": 0.0615,
                "clean_mae": 0.0684,
                "q1_mae": 0.0968,
                "underlying_mae": 0.0546,
                "full_mean_error": -0.0226,
                "avg_abs_diff_vs_official": 0.0516,
                "diff_quarters_gt_0_02_vs_official": 8,
            },
        ]
    )

    annotated, winner_key, reason = market_service._annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key="bridge_front_loaded",
        expanded_best_candidate_model_key="process_quarter_open_blend_exec_penalty",
        new_candidate_keys={"process_quarter_open_blend_exec_penalty"},
    )

    assert winner_key == "process_quarter_open_blend_exec_penalty"
    assert reason == "promoted_over_incumbent_baseline"
    challenger = annotated[annotated["model_key"].astype(str) == "process_quarter_open_blend_exec_penalty"].iloc[0]
    assert bool(challenger["promotion_guard_pass"]) is True
    assert str(challenger["live_preview_quality_status"] or "") == "close"


def test_material_difference_counter_flags_non_cosmetic_fitted_history() -> None:
    official = pd.Series([0.010, 0.020, 0.015, 0.030, 0.025, 0.040, 0.018, 0.022, 0.017, 0.019, 0.021, 0.023])
    fitted = pd.Series([0.040, 0.020, -0.015, 0.060, 0.025, 0.040, 0.018, -0.010, 0.017, 0.019, 0.055, 0.023])

    count = market_service._count_material_difference_quarters_vs_official(
        fitted,
        official,
        threshold=0.025,
    )

    assert count == 5


def test_gpre_official_simple_proxy_reacts_directionally_to_ethanol_corn_basis_and_gas() -> None:
    def _snapshot(
        *,
        ethanol_price: float,
        corn_price: float,
        corn_basis: float,
        gas_price: float,
    ) -> dict[str, object]:
        return build_gpre_next_quarter_preview_snapshot(
            [],
            next_quarter_thesis_snapshot={
                "target_quarter_start": date(2026, 7, 1),
                "target_quarter_end": date(2026, 9, 30),
                "corn": {
                    "price_value": corn_price,
                    "observation_date": date(2026, 4, 2),
                    "official_weighted_corn_basis_usd_per_bu": corn_basis,
                    "official_corn_basis_source_kind": "actual_bid_plus_ams_fallback",
                    "official_corn_basis_source_label": "actual GPRE bids + AMS fallback",
                    "official_corn_basis_provenance": "actual GPRE plant bids when available, AMS fallback otherwise",
                },
                "ethanol": {
                    "status": "ok",
                    "price_value": ethanol_price,
                    "observation_date": date(2026, 4, 2),
                    "contract_tenors": ["jul26", "aug26", "sep26"],
                    "contract_labels": ["Jul 2026", "Aug 2026", "Sep 2026"],
                    "strip_method": "day_weighted",
                    "source_type": "local_chicago_ethanol_futures_csv",
                },
                "natural_gas": {
                    "price_value": gas_price,
                    "observation_date": date(2026, 4, 2),
                },
            },
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 2),
        )

    base = _snapshot(ethanol_price=1.90, corn_price=4.40, corn_basis=-0.20, gas_price=3.00)
    ethanol_up = _snapshot(ethanol_price=2.00, corn_price=4.40, corn_basis=-0.20, gas_price=3.00)
    corn_up = _snapshot(ethanol_price=1.90, corn_price=4.50, corn_basis=-0.20, gas_price=3.00)
    basis_more_negative = _snapshot(ethanol_price=1.90, corn_price=4.40, corn_basis=-0.30, gas_price=3.00)
    gas_up = _snapshot(ethanol_price=1.90, corn_price=4.40, corn_basis=-0.20, gas_price=4.00)

    base_val = float(pd.to_numeric(base["official_simple_proxy_usd_per_gal"], errors="coerce"))
    ethanol_up_val = float(pd.to_numeric(ethanol_up["official_simple_proxy_usd_per_gal"], errors="coerce"))
    corn_up_val = float(pd.to_numeric(corn_up["official_simple_proxy_usd_per_gal"], errors="coerce"))
    basis_more_negative_val = float(pd.to_numeric(basis_more_negative["official_simple_proxy_usd_per_gal"], errors="coerce"))
    gas_up_val = float(pd.to_numeric(gas_up["official_simple_proxy_usd_per_gal"], errors="coerce"))

    assert ethanol_up_val > base_val
    assert corn_up_val < base_val
    assert basis_more_negative_val > base_val
    assert gas_up_val < base_val
    assert basis_more_negative_val - base_val == pytest.approx(0.10 / 2.9, abs=1e-9)


def test_gpre_official_component_records_surface_deterministic_fallbacks() -> None:
    qd = date(2023, 3, 31)
    rows = [
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="ethanol_price",
            series_key="ethanol_nebraska",
            instrument="Ethanol",
            location="Nebraska",
            region="nebraska",
            price_value=1.48,
            source_type="nwer_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="ethanol_price",
            series_key="ethanol_illinois",
            instrument="Ethanol",
            location="Illinois",
            region="illinois",
            price_value=1.55,
            source_type="nwer_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="ethanol_price",
            series_key="ethanol_indiana",
            instrument="Ethanol",
            location="Indiana",
            region="indiana",
            price_value=22.20,
            source_type="nwer_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="ethanol_price",
            series_key="ethanol_iowa",
            instrument="Ethanol",
            location="Iowa",
            region="iowa",
            price_value=1.61,
            source_type="nwer_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="corn_basis",
            series_key="corn_basis_nebraska",
            instrument="Corn basis",
            location="Nebraska",
            region="nebraska",
            price_value=-0.30,
            unit="$/bushel",
            source_type="ams_3617_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="corn_basis",
            series_key="corn_basis_indiana",
            instrument="Corn basis",
            location="Indiana",
            region="indiana",
            price_value=-0.12,
            unit="$/bushel",
            source_type="ams_3617_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="corn_basis",
            series_key="corn_basis_iowa_west",
            instrument="Corn basis",
            location="Iowa West",
            region="iowa_west",
            price_value=-0.18,
            unit="$/bushel",
            source_type="ams_3617_pdf",
        ),
        _parsed_row(
            aggregation_level="quarter_avg",
            observation_date=qd.isoformat(),
            publication_date=qd.isoformat(),
            quarter=qd.isoformat(),
            market_family="corn_basis",
            series_key="corn_basis_minnesota",
            instrument="Corn basis",
            location="Minnesota",
            region="minnesota",
            price_value=-0.26,
            unit="$/bushel",
            source_type="ams_3617_pdf",
        ),
    ]

    components = market_service._gpre_official_quarter_component_records(rows, [qd])

    assert qd in components
    component_df = pd.DataFrame(components[qd]["component_rows"])
    assert not component_df.empty
    assert {"illinois", "indiana", "iowa_west", "minnesota", "nebraska", "tennessee"} <= set(component_df["region"].astype(str))
    minnesota_row = component_df.loc[component_df["region"].astype(str) == "minnesota"].iloc[0]
    indiana_row = component_df.loc[component_df["region"].astype(str) == "indiana"].iloc[0]
    illinois_row = component_df.loc[component_df["region"].astype(str) == "illinois"].iloc[0]
    tennessee_row = component_df.loc[component_df["region"].astype(str) == "tennessee"].iloc[0]
    assert str(minnesota_row["ethanol_series_key"]) == "ethanol_iowa"
    assert "ethanol_south_dakota unavailable; fallback to ethanol_iowa" in str(minnesota_row["fallback_note"] or "")
    assert str(indiana_row["ethanol_series_key"]) == "ethanol_iowa"
    assert "ethanol_indiana implausible" in str(indiana_row["fallback_note"] or "")
    assert str(tennessee_row["basis_series_key"]) == "corn_basis_indiana"
    assert "tennessee uses AMS fallback indiana" in str(tennessee_row["fallback_note"] or "")
    assert str(illinois_row["basis_series_key"] or "") == ""
    assert "No actual bid or AMS proxy available" in str(illinois_row["fallback_note"] or "")
    assert float(pd.to_numeric(components[qd]["ethanol_coverage_ratio"], errors="coerce")) == pytest.approx(1.0, abs=1e-9)
    assert 0.0 < float(pd.to_numeric(components[qd]["basis_coverage_ratio"], errors="coerce")) < 1.0


def test_gpre_official_history_uses_ethanol_fallbacks_and_moves_with_fallback_series() -> None:
    obs_dt = date(2023, 3, 24)
    qd = date(2023, 3, 31)

    def _rows_for_iowa_ethanol(iowa_ethanol: float) -> list[dict[str, object]]:
        return [
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="corn_futures",
                series_key="cbot_corn_usd_per_bu",
                instrument="Corn futures",
                price_value=4.35,
                source_type="nwer_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="natural_gas_price",
                series_key="nymex_gas",
                instrument="Natural gas",
                price_value=3.15,
                unit="$/MMBtu",
                contract_tenor="front",
                source_type="nwer_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="ethanol_price",
                series_key="ethanol_nebraska",
                instrument="Ethanol",
                price_value=1.47,
                source_type="nwer_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="ethanol_price",
                series_key="ethanol_illinois",
                instrument="Ethanol",
                price_value=1.55,
                source_type="nwer_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="ethanol_price",
                series_key="ethanol_indiana",
                instrument="Ethanol",
                price_value=1.53,
                source_type="nwer_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="ethanol_price",
                series_key="ethanol_iowa",
                instrument="Ethanol",
                price_value=iowa_ethanol,
                source_type="nwer_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="corn_basis",
                series_key="corn_basis_nebraska",
                instrument="Corn basis",
                region="nebraska",
                price_value=-0.28,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="corn_basis",
                series_key="corn_basis_illinois",
                instrument="Corn basis",
                region="illinois",
                price_value=-0.08,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="corn_basis",
                series_key="corn_basis_indiana",
                instrument="Corn basis",
                region="indiana",
                price_value=-0.10,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="corn_basis",
                series_key="corn_basis_iowa_west",
                instrument="Corn basis",
                region="iowa_west",
                price_value=-0.14,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            ),
            _parsed_row(
                aggregation_level="observation",
                observation_date=obs_dt.isoformat(),
                publication_date=obs_dt.isoformat(),
                quarter=qd.isoformat(),
                market_family="corn_basis",
                series_key="corn_basis_minnesota",
                instrument="Corn basis",
                region="minnesota",
                price_value=-0.24,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            ),
        ]

    low_series = build_gpre_official_proxy_history_series(
        _rows_for_iowa_ethanol(1.44),
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=qd,
        start_date=date(2023, 3, 1),
        lookback_weeks=None,
    )
    high_series = build_gpre_official_proxy_history_series(
        _rows_for_iowa_ethanol(1.74),
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=qd,
        start_date=date(2023, 3, 1),
        lookback_weeks=None,
    )

    assert low_series and high_series
    low_rec = low_series[-1]
    high_rec = high_series[-1]
    assert float(pd.to_numeric(low_rec["weighted_ethanol_benchmark_usd_per_gal"], errors="coerce")) > 1.47
    assert float(pd.to_numeric(high_rec["weighted_ethanol_benchmark_usd_per_gal"], errors="coerce")) > float(pd.to_numeric(low_rec["weighted_ethanol_benchmark_usd_per_gal"], errors="coerce"))
    assert float(pd.to_numeric(high_rec["simple_crush_per_gal"], errors="coerce")) > float(pd.to_numeric(low_rec["simple_crush_per_gal"], errors="coerce"))


def test_gpre_current_snapshot_prefers_actual_bids_for_official_basis() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.50, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    bids_snapshot = {
        "status": "ok",
        "source_kind": "fixture",
        "source_url": "fixture://gpre-bids",
        "rows": [
            {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.20},
            {"location": "Wood River", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.24},
            {"location": "York", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.22},
            {"location": "Madison", "region": "illinois", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.10},
            {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.12},
            {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.08},
            {"location": "Superior", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.18},
            {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.35},
        ],
    }

    snapshot = build_gpre_official_proxy_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 15),
        prior_quarter=False,
        bids_snapshot=bids_snapshot,
    )

    meta = snapshot["market_meta"]["corn_price"]
    assert snapshot["status"] == "ok_current"
    assert str(meta["official_corn_basis_source_kind"]) == "actual_gpre_bids"
    assert meta["official_corn_basis_snapshot_date"] == date(2026, 4, 30)
    assert snapshot["official_corn_basis_snapshot_date"] == date(2026, 4, 30)
    basis_payload = market_service._gpre_official_current_forward_basis_payload(
        rows,
        target_date=date(2026, 4, 15),
        target_quarter_end=qd,
        as_of_date=date(2026, 4, 15),
        ticker_root=None,
        bids_snapshot=bids_snapshot,
    )
    component_rows = list((basis_payload or {}).get("component_rows") or [])
    assert component_rows
    assert all(str(rec.get("source_kind") or "") == "actual_gpre_bid" for rec in component_rows)
    weighted_basis = sum(
        float(pd.to_numeric(rec.get("weight"), errors="coerce"))
        * float(pd.to_numeric(rec.get("basis_usd_per_bu"), errors="coerce"))
        for rec in component_rows
    )
    assert float(pd.to_numeric(meta["official_weighted_corn_basis_usd_per_bu"], errors="coerce")) == pytest.approx(weighted_basis, abs=1e-9)
    futures_price = 4.50
    gas_price = 3.00
    ethanol_price = float(pd.to_numeric(snapshot["current_market"]["ethanol_price"], errors="coerce"))
    corn_price = float(pd.to_numeric(snapshot["current_market"]["corn_price"], errors="coerce"))
    simple_crush_per_gal = float(pd.to_numeric(snapshot["current_process"]["simple_crush_per_gal"], errors="coerce"))
    assert corn_price == pytest.approx(futures_price + weighted_basis, abs=1e-9)
    assert simple_crush_per_gal == pytest.approx(ethanol_price - (corn_price / 2.9) - (0.028 * gas_price), abs=1e-9)
    assert "actual gpre plant-bid basis" in str(meta["official_corn_basis_provenance"] or "").lower()


def test_gpre_historical_quarter_uses_retained_snapshot_when_eligible() -> None:
    obs_dt = date(2026, 3, 20)
    qd = date(2026, 3, 31)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.40, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_prior_quarter_snapshot_retained")
    try:
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 3, 20),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.20},
                {"location": "Wood River", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.24},
                {"location": "York", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.22},
                {"location": "Madison", "region": "illinois", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": 0.10},
                {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": 0.12},
                {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.08},
                {"location": "Superior", "region": "iowa_west", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.18},
                {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.35},
            ],
        )

        snapshot = build_gpre_official_proxy_snapshot(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 15),
            prior_quarter=True,
            ticker_root=tmp_path,
        )

        meta = snapshot["market_meta"]["corn_price"]
        assert str(meta["official_corn_basis_source_kind"]) == "actual_gpre_bids"
        assert meta["official_corn_basis_snapshot_date"] == date(2026, 3, 20)
        assert snapshot["official_corn_basis_snapshot_date"] == date(2026, 3, 20)
        assert str(meta["official_corn_basis_selection_rule"] or "") == "latest_snapshot_on_or_before_quarter_end"
        assert str(snapshot["official_corn_basis_selection_rule"] or "") == "latest_snapshot_on_or_before_quarter_end"
        assert str(meta["official_corn_basis_provenance"] or "").lower().find("2026-03-20") >= 0
        assert str(meta["official_corn_basis_provenance"] or "").lower().find("latest_snapshot_on_or_before_quarter_end") >= 0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_current_snapshot_uses_cash_price_as_delivered_corn_without_double_counting() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.50, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
    ]
    snapshot = build_gpre_official_proxy_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=obs_dt,
        prior_quarter=False,
        bids_snapshot=_gpre_bids_snapshot_fixture(date(2026, 4, 30), basis_value=-0.20, cash_price=4.10),
    )

    meta = snapshot["market_meta"]["corn_price"]
    assert float(pd.to_numeric(snapshot["current_market"]["corn_price"], errors="coerce")) == pytest.approx(4.10, abs=1e-12)
    assert str(meta.get("proxy_mode") or "") == "gpre_cash_price_with_fallback"
    assert str(meta.get("official_corn_price_source_kind") or "") == "actual_gpre_cash_prices"
    assert float(pd.to_numeric(meta.get("official_weighted_corn_basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.20, abs=1e-12)
    assert float(pd.to_numeric(meta.get("official_weighted_corn_cash_price_usd_per_bu"), errors="coerce")) == pytest.approx(4.10, abs=1e-12)
    expected = 1.60 - (4.10 / 2.9) - (0.028 * 3.00)
    assert float(pd.to_numeric(snapshot["current_process"]["simple_crush_per_gal"], errors="coerce")) == pytest.approx(expected, abs=1e-12)


def test_gpre_current_snapshot_stale_cash_price_falls_back_to_cbot_plus_ams_basis() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.50, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_stale_cash_price_fallback")
    try:
        stale_rows = [
            {**rec, "cash_price": 3.50}
            for rec in _gpre_bids_snapshot_fixture(date(2026, 4, 30), basis_value=-0.90)["rows"]
        ]
        _write_gpre_corn_bids_archive_snapshot(tmp_path, snapshot_date=date(2026, 4, 1), rows=stale_rows)
        snapshot = build_gpre_official_proxy_snapshot(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=obs_dt,
            prior_quarter=False,
            ticker_root=tmp_path,
        )

        meta = snapshot["market_meta"]["corn_price"]
        assert str(meta.get("proxy_mode") or "") == "cbot_plus_official_weighted_basis"
        assert str(meta.get("official_corn_price_source_kind") or "") == "cbot_plus_official_weighted_basis"
        assert float(pd.to_numeric(snapshot["current_market"]["corn_price"], errors="coerce")) == pytest.approx(4.20, abs=1e-12)
        assert float(pd.to_numeric(snapshot["current_market"]["corn_price"], errors="coerce")) != pytest.approx(3.50, abs=1e-12)
        assert str(snapshot.get("official_corn_basis_source_kind") or "") == "weighted_ams_proxy"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_current_snapshot_does_not_use_future_latest_local_file_for_earlier_week() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_future_latest_file_ignored")
    try:
        latest_html = """
        <html><body>Last Updated 04/21/2026
        Central City
        Commodity Delivery End Cash Price Basis Symbol Futures Price Change
        Corn Apr 2026 3.50 -0.90 @C6K 440'0 0'0
        </body></html>
        """.strip()
        latest_path = tmp_path / "corn_bids" / "grain_gpre_home.html"
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(latest_html, encoding="utf-8")

        payload = market_service.weighted_corn_basis_context(
            rows,
            target_date=obs_dt,
            target_quarter_end=qd,
            anchor_date=obs_dt,
            ticker_root=tmp_path,
            bids_snapshot=None,
            frame_key="current_qtd",
        )

        assert payload.get("snapshot_date") is None
        assert payload.get("official_weighted_corn_cash_price_usd_per_bu") is None
        assert str(payload.get("official_corn_basis_source_kind") or "") == "weighted_ams_proxy"
        assert float(pd.to_numeric(payload.get("official_weighted_corn_basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.30, abs=1e-12)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_current_snapshot_partial_actual_bids_are_deterministic_and_explicit() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.50, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    bids_snapshot = {
        "status": "ok",
        "source_kind": "fixture",
        "source_url": "fixture://gpre-bids",
        "rows": [
            {"location": "Wood River", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.24},
            {"location": "York", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.22},
            {"location": "Madison", "region": "illinois", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.10},
            {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.12},
            {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.08},
            {"location": "Superior", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.18},
            {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.35},
        ],
    }

    snapshot = build_gpre_official_proxy_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 15),
        prior_quarter=False,
        bids_snapshot=bids_snapshot,
    )

    meta = snapshot["market_meta"]["corn_price"]
    assert str(meta["official_corn_basis_source_kind"]) == "actual_gpre_bids_with_ams_fallback"
    assert int(meta["official_actual_bid_plant_count"]) == 7
    assert int(meta["official_fallback_plant_count"]) == 1
    basis_payload = market_service._gpre_official_current_forward_basis_payload(
        rows,
        target_date=date(2026, 4, 15),
        target_quarter_end=qd,
        as_of_date=date(2026, 4, 15),
        ticker_root=None,
        bids_snapshot=bids_snapshot,
    )
    component_rows = list((basis_payload or {}).get("component_rows") or [])
    assert component_rows
    assert any(str(rec.get("source_kind") or "") == "ams_proxy_fallback" for rec in component_rows)
    weighted_basis = sum(
        float(pd.to_numeric(rec.get("weight"), errors="coerce"))
        * float(pd.to_numeric(rec.get("basis_usd_per_bu"), errors="coerce"))
        for rec in component_rows
    )
    assert float(pd.to_numeric(meta["official_weighted_corn_basis_usd_per_bu"], errors="coerce")) == pytest.approx(weighted_basis, abs=1e-9)
    assert "AMS fallback" in str(meta["official_corn_basis_provenance"] or "")


def test_gpre_historical_quarter_falls_back_to_ams_when_no_eligible_snapshot_exists() -> None:
    obs_dt = date(2026, 3, 20)
    qd = date(2026, 3, 31)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.40, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_prior_quarter_snapshot_missing")
    try:
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 4, 9),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.20},
                {"location": "Wood River", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.24},
                {"location": "York", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.22},
            ],
        )
        snapshot = build_gpre_official_proxy_snapshot(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 15),
            prior_quarter=True,
            ticker_root=tmp_path,
        )
        meta = snapshot["market_meta"]["corn_price"]
        assert str(meta["official_corn_basis_source_kind"]) == "weighted_ams_proxy"
        assert meta["official_corn_basis_snapshot_date"] is None
        assert snapshot["official_corn_basis_snapshot_date"] is None
        assert str(meta["official_corn_basis_selection_rule"] or "") == "latest_snapshot_on_or_before_quarter_end"
        assert str(snapshot["official_corn_basis_selection_rule"] or "") == "latest_snapshot_on_or_before_quarter_end"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_historical_quarter_stale_snapshot_falls_back_after_fourteen_days() -> None:
    obs_dt = date(2026, 3, 20)
    qd = date(2026, 3, 31)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.40, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_historical_stale_snapshot")
    try:
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 3, 10),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.20},
                {"location": "Wood River", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.24},
                {"location": "York", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.22},
                {"location": "Madison", "region": "illinois", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": 0.10},
                {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": 0.12},
                {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.08},
                {"location": "Superior", "region": "iowa_west", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.18},
                {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.35},
            ],
        )
        snapshot = build_gpre_official_proxy_snapshot(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 15),
            prior_quarter=True,
            ticker_root=tmp_path,
        )
        meta = snapshot["market_meta"]["corn_price"]
        assert str(meta["official_corn_basis_source_kind"]) == "weighted_ams_proxy"
        assert meta["official_corn_basis_snapshot_date"] is None
        assert snapshot["official_corn_basis_snapshot_date"] is None
        assert "stale" in str(meta["official_corn_basis_provenance"] or "").lower()
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_local_gpre_corn_bids_snapshot_prefers_latest_snapshot_on_or_before_quarter_end() -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_history_selection")
    try:
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 2, 14),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Feb 2026", "delivery_end": date(2026, 2, 28), "basis_usd_per_bu": -0.11},
            ],
        )
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 3, 20),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.21},
            ],
        )
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 4, 9),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.31},
            ],
        )
        snap = market_service._load_local_gpre_corn_bids_snapshot(
            ticker_root=tmp_path,
            as_of_date=date(2026, 4, 15),
            target_date=date(2026, 3, 31),
            target_quarter_end=date(2026, 3, 31),
            selection_mode="historical_quarter",
        )
        assert str(snap.get("status") or "") == "ok"
        assert snap.get("snapshot_date") == date(2026, 3, 20)
        nearest_rows = list(snap.get("nearest_rows") or [])
        assert nearest_rows
        assert float(pd.to_numeric(nearest_rows[0].get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.21, abs=1e-9)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_local_gpre_corn_bids_snapshot_migrates_newer_legacy_archive_into_canonical_root() -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_legacy_migration")
    try:
        gpre_root = tmp_path / "GPRE"
        gpre_root.mkdir(parents=True, exist_ok=True)
        _write_gpre_corn_bids_archive_snapshot(
            gpre_root,
            snapshot_date=date(2026, 4, 11),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.21},
            ],
        )
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 4, 16),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.18},
            ],
        )

        snap = market_service._load_local_gpre_corn_bids_snapshot(
            ticker_root=gpre_root,
            as_of_date=date(2026, 4, 17),
            target_date=date(2026, 4, 17),
            target_quarter_end=date(2026, 6, 30),
            selection_mode="current_qtd",
        )
        manifest_entries = market_service._gpre_corn_bids_manifest_entries(gpre_root / "corn_bids")

        assert str(snap.get("status") or "") == "ok"
        assert snap.get("snapshot_date") == date(2026, 4, 16)
        assert [entry["snapshot_date"] for entry in manifest_entries] == [date(2026, 4, 11), date(2026, 4, 16)]
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_load_local_gpre_corn_bids_snapshot_uses_canonical_archive_after_legacy_root_rename() -> None:
    tmp_path = _local_test_dir("gpre_corn_bids_legacy_rename")
    try:
        gpre_root = tmp_path / "GPRE"
        gpre_root.mkdir(parents=True, exist_ok=True)
        _write_gpre_corn_bids_archive_snapshot(
            gpre_root,
            snapshot_date=date(2026, 4, 16),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.18},
            ],
        )
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 4, 10),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.21},
            ],
        )
        (tmp_path / "corn_bids").rename(tmp_path / "corn_bids_legacy_backup")

        snap = market_service._load_local_gpre_corn_bids_snapshot(
            ticker_root=gpre_root,
            as_of_date=date(2026, 4, 17),
            target_date=date(2026, 4, 17),
            target_quarter_end=date(2026, 6, 30),
            selection_mode="current_qtd",
        )

        assert str(snap.get("status") or "") == "ok"
        assert snap.get("snapshot_date") == date(2026, 4, 16)
        assert Path(snap["manifest_path"]).parent == (gpre_root / "corn_bids")
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_official_quarter_component_records_use_retained_snapshot_for_historical_quarter() -> None:
    qd = date(2026, 3, 31)
    rows = [
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_historical_quarter_component_retained")
    try:
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 3, 20),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.20},
                {"location": "Wood River", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.24},
                {"location": "York", "region": "nebraska", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.22},
                {"location": "Madison", "region": "illinois", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": 0.10},
                {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": 0.12},
                {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.08},
                {"location": "Superior", "region": "iowa_west", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.18},
                {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Mar 2026", "delivery_end": date(2026, 3, 31), "basis_usd_per_bu": -0.35},
            ],
        )
        records = market_service._gpre_official_quarter_component_records(
            rows,
            [qd],
            ticker_root=tmp_path,
        )
        rec = dict(records.get(qd) or {})
        assert str(rec.get("official_corn_basis_source_kind") or "") == "actual_gpre_bids"
        assert rec.get("official_corn_basis_snapshot_date") == date(2026, 3, 20)
        assert float(pd.to_numeric(rec.get("weighted_ams_basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.11821917808219182, abs=1e-9)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_gpre_current_snapshot_actual_bid_change_moves_basis_in_expected_direction() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    base_rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.50, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    base_snapshot = {
        "status": "ok",
        "source_kind": "fixture",
        "source_url": "fixture://gpre-bids",
        "rows": [
            {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.20},
            {"location": "Wood River", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.24},
            {"location": "York", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.22},
            {"location": "Madison", "region": "illinois", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.10},
            {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.12},
            {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.08},
            {"location": "Superior", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.18},
            {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.35},
        ],
    }
    higher_minnesota_snapshot = {
        **base_snapshot,
        "rows": [
            dict(rec) if str(rec.get("location") or "") != "Otter Tail" else {**rec, "basis_usd_per_bu": -0.25}
            for rec in base_snapshot["rows"]
        ],
    }

    low_snap = build_gpre_official_proxy_snapshot(
        base_rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 15),
        prior_quarter=False,
        bids_snapshot=base_snapshot,
    )
    high_snap = build_gpre_official_proxy_snapshot(
        base_rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 15),
        prior_quarter=False,
        bids_snapshot=higher_minnesota_snapshot,
    )

    assert float(pd.to_numeric(high_snap["market_meta"]["corn_price"]["official_weighted_corn_basis_usd_per_bu"], errors="coerce")) > float(pd.to_numeric(low_snap["market_meta"]["corn_price"]["official_weighted_corn_basis_usd_per_bu"], errors="coerce"))
    assert float(pd.to_numeric(high_snap["current_market"]["corn_price"], errors="coerce")) > float(pd.to_numeric(low_snap["current_market"]["corn_price"], errors="coerce"))
    assert float(pd.to_numeric(high_snap["current_process"]["simple_crush"], errors="coerce")) < float(pd.to_numeric(low_snap["current_process"]["simple_crush"], errors="coerce"))


def test_gpre_current_qtd_stale_snapshot_falls_back_after_seven_days() -> None:
    obs_dt = date(2026, 4, 10)
    qd = date(2026, 6, 30)
    rows = [
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=4.50, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=3.00, unit="$/MMBtu", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=1.60, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=1.61, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=1.59, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=1.58, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date=obs_dt.isoformat(), publication_date=obs_dt.isoformat(), quarter=qd.isoformat(), market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    tmp_path = _local_test_dir("gpre_current_qtd_stale_snapshot")
    try:
        _write_gpre_corn_bids_archive_snapshot(
            tmp_path,
            snapshot_date=date(2026, 4, 1),
            rows=[
                {"location": "Central City", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.20},
                {"location": "Wood River", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.24},
                {"location": "York", "region": "nebraska", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.22},
                {"location": "Madison", "region": "illinois", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.10},
                {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": 0.12},
                {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.08},
                {"location": "Superior", "region": "iowa_west", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.18},
                {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Apr 2026", "delivery_end": date(2026, 4, 30), "basis_usd_per_bu": -0.35},
            ],
        )
        snapshot = build_gpre_official_proxy_snapshot(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 10),
            prior_quarter=False,
            ticker_root=tmp_path,
        )
        meta = snapshot["market_meta"]["corn_price"]
        assert str(meta["official_corn_basis_source_kind"]) == "weighted_ams_proxy"
        assert snapshot["official_corn_basis_snapshot_date"] is None
        assert "stale" in str(meta["official_corn_basis_provenance"] or "").lower()
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_weighted_corn_basis_context_historical_anchor_does_not_drift_when_later_ams_rows_exist() -> None:
    quarter_end = date(2026, 3, 31)
    base_rows: list[dict[str, object]] = []
    later_rows: list[dict[str, object]] = []
    for region, base_value, later_value in (
        ("nebraska", -0.30, -0.55),
        ("illinois", -0.05, -0.35),
        ("indiana", -0.06, -0.40),
        ("iowa_west", -0.15, -0.45),
        ("minnesota", -0.25, -0.60),
    ):
        base_rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-03-31",
                publication_date="2026-03-31",
                quarter=quarter_end.isoformat(),
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=base_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )
        later_rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-04-15",
                publication_date="2026-04-15",
                quarter="2026-06-30",
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=later_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )

    before = market_service.weighted_corn_basis_context(
        base_rows,
        target_date=quarter_end,
        target_quarter_end=quarter_end,
        anchor_date=quarter_end,
        ticker_root=None,
        bids_snapshot=None,
        frame_key="historical_quarter",
    )
    after = market_service.weighted_corn_basis_context(
        [*base_rows, *later_rows],
        target_date=quarter_end,
        target_quarter_end=quarter_end,
        anchor_date=quarter_end,
        ticker_root=None,
        bids_snapshot=None,
        frame_key="historical_quarter",
    )

    assert str(before.get("official_corn_basis_source_kind") or "") == "weighted_ams_proxy"
    assert str(before.get("ams_basis_strategy") or "") == "exact1"
    assert str(after.get("ams_basis_strategy") or "") == "exact1"
    assert float(pd.to_numeric(after.get("official_weighted_corn_basis_usd_per_bu"), errors="coerce")) == pytest.approx(
        float(pd.to_numeric(before.get("official_weighted_corn_basis_usd_per_bu"), errors="coerce")),
        abs=1e-12,
    )
    assert all(
        rec.get("reference_as_of") == quarter_end
        for rec in list(after.get("ams_reference_rows") or [])
        if str(rec.get("region") or "").strip().lower() in {"nebraska", "illinois", "indiana", "iowa_west", "minnesota"}
    )
    assert all(
        str(rec.get("reference_method") or "").startswith("Exact same-date AMS basis 2026-03-31")
        for rec in list(after.get("ams_reference_rows") or [])
        if str(rec.get("region") or "").strip().lower() in {"nebraska", "illinois", "indiana", "iowa_west", "minnesota"}
    )
    assert all(
        (not isinstance(rec.get("reference_as_of"), date)) or rec.get("reference_as_of") <= quarter_end
        for rec in list(after.get("ams_reference_rows") or [])
    )
    assert f"anchor {quarter_end.isoformat()}" in str(after.get("official_corn_basis_provenance") or "")


def test_weighted_corn_basis_context_historical_exact1_without_same_day_stays_blank_when_later_ams_rows_exist() -> None:
    quarter_end = date(2026, 3, 31)
    base_rows: list[dict[str, object]] = []
    later_rows: list[dict[str, object]] = []
    for region, base_value, later_value in (
        ("nebraska", -0.30, -0.55),
        ("illinois", -0.05, -0.35),
        ("indiana", -0.06, -0.40),
        ("iowa_west", -0.15, -0.45),
        ("minnesota", -0.25, -0.60),
    ):
        base_rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-03-20",
                publication_date="2026-03-20",
                quarter=quarter_end.isoformat(),
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=base_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )
        later_rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-04-15",
                publication_date="2026-04-15",
                quarter="2026-06-30",
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=later_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )

    before = market_service.weighted_corn_basis_context(
        base_rows,
        target_date=quarter_end,
        target_quarter_end=quarter_end,
        anchor_date=quarter_end,
        ticker_root=None,
        bids_snapshot=None,
        frame_key="historical_quarter",
    )
    after = market_service.weighted_corn_basis_context(
        [*base_rows, *later_rows],
        target_date=quarter_end,
        target_quarter_end=quarter_end,
        anchor_date=quarter_end,
        ticker_root=None,
        bids_snapshot=None,
        frame_key="historical_quarter",
    )

    assert str(before.get("ams_basis_strategy") or "") == "exact1"
    assert str(after.get("ams_basis_strategy") or "") == "exact1"
    assert before.get("official_weighted_corn_basis_usd_per_bu") is None
    assert after.get("official_weighted_corn_basis_usd_per_bu") is None
    assert all(rec.get("reference_as_of") is None for rec in list(before.get("ams_reference_rows") or []))
    assert all(rec.get("reference_as_of") is None for rec in list(after.get("ams_reference_rows") or []))
    assert all(
        str(rec.get("reference_method") or "") == "No AMS reference available"
        for rec in list(after.get("ams_reference_rows") or [])
    )


def test_weighted_corn_basis_context_quarter_open_uses_frozen_anchor_date() -> None:
    quarter_start = date(2026, 4, 1)
    quarter_end = date(2026, 6, 30)
    frozen_anchor = date(2026, 3, 31)
    rows: list[dict[str, object]] = []
    for region, anchor_value, later_value in (
        ("nebraska", -0.30, -0.10),
        ("illinois", -0.05, 0.05),
        ("indiana", -0.06, 0.04),
        ("iowa_west", -0.15, -0.01),
        ("minnesota", -0.25, -0.02),
    ):
        rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date=frozen_anchor.isoformat(),
                publication_date=frozen_anchor.isoformat(),
                quarter="2026-03-31",
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=anchor_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )
        rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-04-10",
                publication_date="2026-04-10",
                quarter=quarter_end.isoformat(),
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=later_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )

    payload = market_service.weighted_corn_basis_context(
        rows,
        target_date=quarter_start,
        target_quarter_end=quarter_end,
        anchor_date=frozen_anchor,
        ticker_root=None,
        bids_snapshot=None,
        frame_key="quarter_open",
    )

    nebraska_ref = next(
        rec for rec in list(payload.get("ams_reference_rows") or [])
        if str(rec.get("region") or "").strip() == "nebraska"
    )
    assert str(payload.get("ams_basis_strategy") or "") == "same_or_prior3_then_avg5"
    assert nebraska_ref["reference_as_of"] == frozen_anchor
    assert float(pd.to_numeric(nebraska_ref.get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.30, abs=1e-12)
    assert f"anchor {frozen_anchor.isoformat()}" in str(payload.get("official_corn_basis_provenance") or "")


def test_weighted_corn_basis_context_next_quarter_uses_live_as_of_anchor() -> None:
    as_of_date = date(2026, 4, 15)
    quarter_end = date(2026, 9, 30)
    rows: list[dict[str, object]] = []
    for region, anchor_value, later_value in (
        ("nebraska", -0.22, -0.55),
        ("illinois", -0.03, -0.21),
        ("indiana", -0.04, -0.20),
        ("iowa_west", -0.11, -0.36),
        ("minnesota", -0.18, -0.44),
    ):
        rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-04-14",
                publication_date="2026-04-14",
                quarter="2026-06-30",
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=anchor_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )
        rows.append(
            _parsed_row(
                aggregation_level="observation",
                observation_date="2026-04-16",
                publication_date="2026-04-16",
                quarter="2026-06-30",
                market_family="corn_basis",
                series_key=f"corn_basis_{region}",
                instrument="Corn basis",
                region=region,
                price_value=later_value,
                unit="$/bushel",
                source_type="ams_3617_pdf",
            )
        )

    payload = market_service.weighted_corn_basis_context(
        rows,
        target_date=date(2026, 7, 15),
        target_quarter_end=quarter_end,
        anchor_date=as_of_date,
        ticker_root=None,
        bids_snapshot=None,
        frame_key="next_quarter_thesis",
    )

    nebraska_ref = next(
        rec for rec in list(payload.get("ams_reference_rows") or [])
        if str(rec.get("region") or "").strip() == "nebraska"
    )
    assert str(payload.get("ams_basis_strategy") or "") == "same_or_prior3_then_avg5"
    assert nebraska_ref["reference_as_of"] == date(2026, 4, 14)
    assert float(pd.to_numeric(nebraska_ref.get("basis_usd_per_bu"), errors="coerce")) == pytest.approx(-0.22, abs=1e-12)
    assert f"anchor {as_of_date.isoformat()}" in str(payload.get("official_corn_basis_provenance") or "")


def test_gpre_ams_basis_strategy_leaderboard_prefers_exact_same_date_on_quarter_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    q1 = date(2024, 3, 31)
    q2 = date(2024, 6, 30)
    monkeypatch.setattr(
        market_service,
        "_gpre_active_plants_for_quarter",
        lambda quarter_end, plant_capacity_history=None, ticker_root=None: [
            {"location": "Central City", "region": "nebraska", "capacity_mmgy": 100.0},
            {"location": "Madison", "region": "illinois", "capacity_mmgy": 60.0},
            {"location": "Mount Vernon", "region": "indiana", "capacity_mmgy": 40.0},
            {"location": "Shenandoah", "region": "iowa_west", "capacity_mmgy": 80.0},
            {"location": "Otter Tail", "region": "minnesota", "capacity_mmgy": 50.0},
        ],
    )
    rows: list[dict[str, object]] = []
    for quarter_end, ethanol, cbot, gas, exact_basis, noisy_basis in (
        (q1, 1.62, 4.30, 3.10, -0.18, -0.42),
        (q2, 1.74, 4.48, 3.25, -0.11, -0.36),
    ):
        rows.extend(
            [
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=ethanol, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=ethanol + 0.02, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=ethanol + 0.01, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=ethanol - 0.01, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=ethanol - 0.02, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=cbot, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=gas, unit="$/MMBtu", source_type="nwer_pdf"),
            ]
        )
        for region in ("nebraska", "illinois", "indiana", "iowa_west", "minnesota"):
            rows.append(
                _parsed_row(
                    aggregation_level="observation",
                    observation_date=(quarter_end - timedelta(days=4)).isoformat(),
                    publication_date=(quarter_end - timedelta(days=4)).isoformat(),
                    quarter=quarter_end.isoformat(),
                    market_family="corn_basis",
                    series_key=f"corn_basis_{region}",
                    instrument="Corn basis",
                    region=region,
                    price_value=noisy_basis,
                    unit="$/bushel",
                    source_type="ams_3617_pdf",
                )
            )
            rows.append(
                _parsed_row(
                    aggregation_level="observation",
                    observation_date=quarter_end.isoformat(),
                    publication_date=quarter_end.isoformat(),
                    quarter=quarter_end.isoformat(),
                    market_family="corn_basis",
                    series_key=f"corn_basis_{region}",
                    instrument="Corn basis",
                    region=region,
                    price_value=exact_basis,
                    unit="$/bushel",
                    source_type="ams_3617_pdf",
                )
            )

    exact_components = market_service._gpre_official_quarter_component_records(
        rows,
        [q1, q2],
        ticker_root=None,
        bids_snapshot=None,
        as_of_date=q2,
        plant_capacity_history=None,
        ams_basis_strategy="exact1",
    )
    actual_by_quarter = {
        q1: float(pd.to_numeric(exact_components[q1]["weighted_ethanol_benchmark_usd_per_gal"], errors="coerce"))
        - ((4.30 + float(pd.to_numeric(exact_components[q1]["weighted_ams_basis_usd_per_bu"], errors="coerce"))) / 2.9)
        - (28000.0 / 1_000_000.0 * 3.10),
        q2: float(pd.to_numeric(exact_components[q2]["weighted_ethanol_benchmark_usd_per_gal"], errors="coerce"))
        - ((4.48 + float(pd.to_numeric(exact_components[q2]["weighted_ams_basis_usd_per_bu"], errors="coerce"))) / 2.9)
        - (28000.0 / 1_000_000.0 * 3.25),
    }

    error_rows = market_service._gpre_ams_basis_strategy_error_rows(
        rows,
        actual_by_quarter,
        strategies=("avg21", "avg5", "exact1", "same_or_prior3", "same_or_prior3_then_avg5"),
        ticker_root=None,
        bids_snapshot=None,
        plant_capacity_history=None,
    )
    leaderboard = market_service._gpre_ams_basis_strategy_leaderboard(error_rows)

    assert not error_rows.empty
    assert list(leaderboard["strategy_key"].astype(str))[:2] == ["exact1", "same_or_prior3"]
    best_row = leaderboard.iloc[0].to_dict()
    avg21_row = leaderboard[leaderboard["strategy_key"].astype(str) == "avg21"].iloc[0].to_dict()
    assert float(pd.to_numeric(best_row.get("mae"), errors="coerce")) == pytest.approx(0.0, abs=1e-12)
    assert float(pd.to_numeric(avg21_row.get("mae"), errors="coerce")) > 0.0


def test_gpre_official_proxy_weekly_rows_resolve_ams_basis_per_checkpoint_date() -> None:
    quarter_end = date(2026, 6, 30)
    rows: list[dict[str, object]] = []
    for obs_date, basis_value, ethanol_value, gas_value, corn_value in (
        (date(2026, 4, 3), -0.10, 1.60, 3.00, 4.40),
        (date(2026, 4, 10), -0.30, 1.62, 3.05, 4.45),
    ):
        rows.extend(
            [
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=corn_value, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=gas_value, unit="$/MMBtu", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=ethanol_value, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_illinois", instrument="Ethanol", price_value=ethanol_value + 0.02, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_indiana", instrument="Ethanol", price_value=ethanol_value + 0.01, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_iowa", instrument="Ethanol", price_value=ethanol_value - 0.01, source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date=obs_date.isoformat(), publication_date=obs_date.isoformat(), quarter=quarter_end.isoformat(), market_family="ethanol_price", series_key="ethanol_south_dakota", instrument="Ethanol", price_value=ethanol_value - 0.02, source_type="nwer_pdf"),
            ]
        )
        for region in ("nebraska", "illinois", "indiana", "iowa_west", "minnesota"):
            rows.append(
                _parsed_row(
                    aggregation_level="observation",
                    observation_date=obs_date.isoformat(),
                    publication_date=obs_date.isoformat(),
                    quarter=quarter_end.isoformat(),
                    market_family="corn_basis",
                    series_key=f"corn_basis_{region}",
                    instrument="Corn basis",
                    region=region,
                    price_value=basis_value,
                    unit="$/bushel",
                    source_type="ams_3617_pdf",
                )
            )

    snapshot = build_gpre_official_proxy_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 10),
        prior_quarter=False,
        ticker_root=None,
        bids_snapshot=None,
    )

    weekly_rows = {
        rec["week_end"]: dict(rec)
        for rec in list(snapshot.get("weekly_rows") or [])
        if isinstance(rec.get("week_end"), date)
    }
    assert float(pd.to_numeric(weekly_rows[date(2026, 4, 3)]["official_weighted_corn_basis_usd_per_bu"], errors="coerce")) == pytest.approx(-0.10, abs=1e-12)
    assert float(pd.to_numeric(weekly_rows[date(2026, 4, 10)]["official_weighted_corn_basis_usd_per_bu"], errors="coerce")) == pytest.approx(-0.30, abs=1e-12)
    assert str(weekly_rows[date(2026, 4, 3)]["corn_basis_source_kind"] or "") == "weighted_ams_proxy"


def test_next_quarter_thesis_snapshot_prefers_actual_bids_when_available() -> None:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-04-10", quarter="2026-06-30", publication_date="2026-04-10", market_family="corn_futures", series_key="cbot_corn_sep26_usd", instrument="Corn futures", price_value=4.72, contract_tenor="sep26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-10", quarter="2026-06-30", publication_date="2026-04-10", market_family="natural_gas_price", series_key="nymex_gas_aug26_usd", instrument="Natural gas futures", price_value=3.40, contract_tenor="aug26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_basis", series_key="corn_basis_nebraska", instrument="Corn basis", region="nebraska", price_value=-0.30, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_basis", series_key="corn_basis_illinois", instrument="Corn basis", region="illinois", price_value=-0.05, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_basis", series_key="corn_basis_indiana", instrument="Corn basis", region="indiana", price_value=-0.06, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_basis", series_key="corn_basis_iowa_west", instrument="Corn basis", region="iowa_west", price_value=-0.15, unit="$/bushel", source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_basis", series_key="corn_basis_minnesota", instrument="Corn basis", region="minnesota", price_value=-0.25, unit="$/bushel", source_type="ams_3617_pdf"),
    ]
    bids_snapshot = {
        "status": "ok",
        "source_kind": "fixture",
        "source_url": "fixture://gpre-bids",
        "rows": [
            {"location": "Central City", "region": "nebraska", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": -0.20},
            {"location": "Wood River", "region": "nebraska", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": -0.24},
            {"location": "York", "region": "nebraska", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": -0.22},
            {"location": "Madison", "region": "illinois", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": 0.10},
            {"location": "Mount Vernon", "region": "indiana", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": 0.12},
            {"location": "Shenandoah", "region": "iowa_west", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": -0.08},
            {"location": "Superior", "region": "iowa_west", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": -0.18},
            {"location": "Otter Tail", "region": "minnesota", "delivery_label": "Jul 2026", "delivery_end": date(2026, 7, 31), "basis_usd_per_bu": -0.35},
        ],
    }

    thesis = build_next_quarter_thesis_snapshot(
        rows,
        as_of_date=date(2026, 4, 15),
        bids_snapshot=bids_snapshot,
    )

    # Forward official basis now follows quarter-aware active-capacity weighting,
    # so the old equal-weighted plant average is intentionally stale.
    active_plants = market_service._gpre_active_plants_for_quarter(date(2026, 9, 30))
    capacity_by_location = {
        str(rec.get("location") or "").strip(): float(pd.to_numeric(rec.get("capacity_mmgy"), errors="coerce"))
        for rec in active_plants
        if str(rec.get("location") or "").strip()
        and pd.notna(pd.to_numeric(rec.get("capacity_mmgy"), errors="coerce"))
    }
    expected_weighted = sum(
        capacity_by_location[str(rec["location"])] * float(rec["basis_usd_per_bu"])
        for rec in bids_snapshot["rows"]
        if str(rec.get("location") or "").strip() in capacity_by_location
    ) / sum(
        capacity_by_location[str(rec["location"])]
        for rec in bids_snapshot["rows"]
        if str(rec.get("location") or "").strip() in capacity_by_location
    )

    assert thesis["corn"]["contract_tenor"] == "sep26"
    assert str(thesis["corn"]["official_corn_basis_source_kind"]) == "actual_gpre_bids"
    assert float(pd.to_numeric(thesis["corn"]["official_weighted_corn_basis_usd_per_bu"], errors="coerce")) == pytest.approx(expected_weighted, abs=1e-12)
    assert "actual gpre plant-bid basis" in str(thesis["corn"]["official_corn_basis_provenance"] or "").lower()


def test_next_quarter_thesis_snapshot_respects_as_of_date_for_frozen_quarter_boundary() -> None:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_futures", series_key="cbot_corn_sep26_usd", instrument="Corn futures", price_value=4.72, contract_tenor="sep26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-10", publication_date="2026-04-10", quarter="2026-06-30", market_family="corn_futures", series_key="cbot_corn_sep26_usd", instrument="Corn futures", price_value=5.20, contract_tenor="sep26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="natural_gas_price", series_key="nymex_gas_aug26_usd", instrument="Natural gas futures", price_value=3.35, contract_tenor="aug26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-10", publication_date="2026-04-10", quarter="2026-06-30", market_family="natural_gas_price", series_key="nymex_gas_aug26_usd", instrument="Natural gas futures", price_value=4.10, contract_tenor="aug26", source_type="nwer_pdf"),
    ]

    thesis = build_next_quarter_thesis_snapshot(rows, as_of_date=date(2026, 3, 31))

    assert float(pd.to_numeric(thesis["corn"]["price_value"], errors="coerce")) == pytest.approx(4.72, abs=1e-6)
    assert thesis["corn"]["observation_date"] == date(2026, 3, 31)
    assert float(pd.to_numeric(thesis["natural_gas"]["price_value"], errors="coerce")) == pytest.approx(3.35, abs=1e-6)
    assert thesis["natural_gas"]["observation_date"] == date(2026, 3, 31)


def test_cme_ethanol_provider_parses_local_barchart_contract_rows_and_ignores_footer() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_cme_ethanol_parse_")
    try:
        csv_path = tmp_path / "ethanol-chicago-prices-end-of-day-04-04-2026.csv"
        csv_path.write_text(
            "Contract,Last,Change,Open,High,Low,Previous,Volume,Open Interest,Time\n"
            "\"FLJ26 (Apr '26)\",1.995,0.0000,1.995,1.995,1.995,1.995,0,466,2026-04-02\n"
            "\"FLK26 (May '26)\",2.005,0.0000,2.005,2.005,2.005,2.005,0,469,2026-04-02\n"
            "\"FLM26 (Jun '26)\",1.9825,0.0000,1.9825,1.9825,1.9825,1.9825,0,212,2026-04-02\n"
            "\"Downloaded from Barchart.com as of 04-04-2026 03:55am CDT\"\n",
            encoding="utf-8",
        )

        rows = parse_cme_ethanol_settlement_table(csv_path, fallback_date=None)

        assert len(rows) == 3
        assert {str(rec["series_key"]) for rec in rows} == {
            "cme_ethanol_chicago_platts_apr26_usd_per_gal",
            "cme_ethanol_chicago_platts_may26_usd_per_gal",
            "cme_ethanol_chicago_platts_jun26_usd_per_gal",
        }
        assert all(str(rec["source_type"]) == "local_chicago_ethanol_futures_csv" for rec in rows)
        assert all(pd.to_numeric(rec["price_value"], errors="coerce") > 1.9 for rec in rows)
        assert all(rec["observation_date"] == date(2026, 4, 2) for rec in rows)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_cme_ethanol_provider_refresh_is_local_only_and_writes_debug() -> None:
    provider = CMEChicagoEthanolPlattsProvider()

    tmp_path = _local_test_dir(".pytest_tmp_cme_ethanol_remote_")
    try:
        cache_root = tmp_path / "sec_cache" / "market_data"
        ensure_market_cache_dirs(cache_root)
        ticker_root = tmp_path / "GPRE"
        futures_dir = ticker_root / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        source_csv = futures_dir / "ethanol-chicago-prices-end-of-day-04-04-2026.csv"
        source_csv.write_text(
            "Contract,Last,Change,Open,High,Low,Previous,Volume,Open Interest,Time\n"
            "\"FLN26 (Jul '26)\",1.9525,0.0,1.9525,1.9525,1.9525,1.9525,0,100,2026-04-02\n"
            "\"FLQ26 (Aug '26)\",1.9225,0.0,1.9225,1.9225,1.9225,1.9225,0,100,2026-04-02\n"
            "\"FLU26 (Sep '26)\",1.8925,0.0,1.8925,1.8925,1.8925,1.8925,0,100,2026-04-02\n",
            encoding="utf-8",
        )

        sync_result = provider.sync_raw(cache_root, ticker_root, refresh=True)
        debug_path = remote_debug_path(cache_root, "cme_ethanol_platts")
        debug_payload = json.loads(debug_path.read_text(encoding="utf-8"))

        assert sync_result["raw_added"] == 1
        assert (cache_root / "raw" / "cme_ethanol_platts" / "2026" / "ethanol-chicago-prices-end-of-day-04-04-2026.csv").exists()
        latest_refresh = debug_payload.get("latest_refresh") or {}
        assert str((latest_refresh.get("landing_fetch") or {}).get("classification") or "") == "local_only_source"
        assert str(latest_refresh.get("final_classification") or "") == "local_only_source"
        assert list(latest_refresh.get("download_attempts") or []) == []

        parsed_df = provider.parse_raw_to_rows(cache_root, ticker_root, sync_result["entries"])
        debug_payload = json.loads(debug_path.read_text(encoding="utf-8"))
        latest_parse = debug_payload.get("latest_parse") or {}
        assert not parsed_df.empty
        assert int(latest_parse.get("parsed_rows") or 0) == len(parsed_df)
        assert any(str(item.get("source_file") or "").endswith("ethanol-chicago-prices-end-of-day-04-04-2026.csv") for item in list(latest_parse.get("rows_by_source_file") or []))
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_cme_ethanol_provider_maps_contract_codes_across_quarters() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_cme_ethanol_contract_map_")
    try:
        csv_path = tmp_path / "manual_cme_ethanol_chicago_eod_2026-04-05.csv"
        csv_path.write_text(
            "Contract,Last,Time\n"
            "\"FLN26 (Jul '26)\",1.9525,2026-04-02\n"
            "\"FLQ26 (Aug '26)\",1.9225,2026-04-02\n"
            "\"FLU26 (Sep '26)\",1.8925,2026-04-02\n"
            "\"FLV26 (Oct '26)\",1.8800,2026-04-02\n"
            "\"FLF27 (Jan '27)\",1.8450,2026-04-02\n",
            encoding="utf-8",
        )

        rows = parse_cme_ethanol_settlement_table(csv_path, fallback_date=None)
        tenors = {str(rec["contract_tenor"]) for rec in rows}

        assert {"jul26", "aug26", "sep26", "oct26", "jan27"}.issubset(tenors)
        assert {str(rec["contract_label"]) for rec in rows if str(rec["contract_tenor"]) in {"jul26", "aug26", "sep26"}} == {
            "Jul 2026",
            "Aug 2026",
            "Sep 2026",
        }
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_cme_ethanol_provider_prefers_manual_file_deterministically() -> None:
    provider = CMEChicagoEthanolPlattsProvider()
    tmp_path = _local_test_dir(".pytest_tmp_cme_ethanol_manual_priority_")
    try:
        cache_root = tmp_path / "sec_cache" / "market_data"
        ensure_market_cache_dirs(cache_root)
        manual_path = tmp_path / "manual_cme_ethanol_chicago_eod_2026-04-05.csv"
        vendor_path = tmp_path / "ethanol-chicago-prices-end-of-day-04-04-2026.csv"
        vendor_path.write_text(
            "Contract,Last,Time\n"
            "\"FLN26 (Jul '26)\",1.9525,2026-04-02\n",
            encoding="utf-8",
        )
        manual_path.write_text(
            "Contract,Last,Time\n"
            "\"FLN26 (Jul '26)\",2.1050,2026-04-02\n",
            encoding="utf-8",
        )

        raw_entries = [
            {"local_path": str(vendor_path), "report_date": "2026-04-04"},
            {"local_path": str(manual_path), "report_date": "2026-04-05"},
        ]
        parsed_df = provider.parse_raw_to_rows(cache_root, tmp_path, raw_entries)
        row = parsed_df.loc[parsed_df["series_key"] == "cme_ethanol_chicago_platts_jul26_usd_per_gal"].iloc[0]

        assert float(pd.to_numeric(row["price_value"], errors="coerce")) == pytest.approx(2.1050, abs=1e-9)
        assert str(row["source_file"]) == "manual_cme_ethanol_chicago_eod_2026-04-05.csv"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_next_quarter_thesis_snapshot_builds_day_weighted_cme_ethanol_strip_for_q2() -> None:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_futures", series_key="cbot_corn_may26_usd", instrument="Corn futures", price_value=4.72, contract_tenor="may26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="natural_gas_price", series_key="nymex_gas_jun26_usd", instrument="Natural gas futures", price_value=3.35, contract_tenor="jun26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_apr26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.61, contract_tenor="apr26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_may26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.66, contract_tenor="may26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_jun26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.71, contract_tenor="jun26", source_type="local_chicago_ethanol_futures_csv"),
    ]

    thesis = build_next_quarter_thesis_snapshot(rows, as_of_date=date(2026, 3, 31))

    expected = ((1.61 * 30.0) + (1.66 * 31.0) + (1.71 * 30.0)) / 91.0
    assert thesis["ethanol"]["status"] == "ok"
    assert thesis["ethanol"]["contract_tenors"] == ["apr26", "may26", "jun26"]
    assert thesis["ethanol"]["strip_method"] == "day_weighted"
    assert float(pd.to_numeric(thesis["ethanol"]["price_value"], errors="coerce")) == pytest.approx(expected, abs=1e-9)


def test_next_quarter_thesis_snapshot_uses_expected_contract_months_for_q3() -> None:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", publication_date="2026-04-03", quarter="2026-06-30", market_family="corn_futures", series_key="cbot_corn_sep26_usd", instrument="Corn futures", price_value=4.72, contract_tenor="sep26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", publication_date="2026-04-03", quarter="2026-06-30", market_family="natural_gas_price", series_key="nymex_gas_aug26_usd", instrument="Natural gas futures", price_value=3.35, contract_tenor="aug26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", publication_date="2026-04-03", quarter="2026-06-30", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_jul26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.61, contract_tenor="jul26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", publication_date="2026-04-03", quarter="2026-06-30", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_aug26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.66, contract_tenor="aug26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", publication_date="2026-04-03", quarter="2026-06-30", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_sep26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.71, contract_tenor="sep26", source_type="local_chicago_ethanol_futures_csv"),
    ]

    thesis = build_next_quarter_thesis_snapshot(rows, as_of_date=date(2026, 4, 3))

    assert thesis["target_quarter_end"] == date(2026, 9, 30)
    assert thesis["ethanol"]["contract_tenors"] == ["jul26", "aug26", "sep26"]
    assert thesis["corn"]["contract_tenor"] == "sep26"
    assert thesis["natural_gas"]["contract_tenor"] == "aug26"


def test_next_quarter_thesis_snapshot_marks_missing_cme_contract_months_explicitly() -> None:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_futures", series_key="cbot_corn_may26_usd", instrument="Corn futures", price_value=4.72, contract_tenor="may26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="natural_gas_price", series_key="nymex_gas_jun26_usd", instrument="Natural gas futures", price_value=3.35, contract_tenor="jun26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_apr26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.61, contract_tenor="apr26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_may26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.66, contract_tenor="may26", source_type="local_chicago_ethanol_futures_csv"),
    ]

    thesis = build_next_quarter_thesis_snapshot(rows, as_of_date=date(2026, 3, 31))

    assert thesis["ethanol"]["status"] == "missing_contract_months"
    assert thesis["ethanol"]["missing_contract_tenors"] == ["jun26"]


def test_next_quarter_thesis_snapshot_moves_when_one_contract_month_changes() -> None:
    base_rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", publication_date="2026-04-02", quarter="2026-06-30", market_family="corn_futures", series_key="cbot_corn_sep26_usd", instrument="Corn futures", price_value=4.72, contract_tenor="sep26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", publication_date="2026-04-02", quarter="2026-06-30", market_family="natural_gas_price", series_key="nymex_gas_aug26_usd", instrument="Natural gas futures", price_value=3.35, contract_tenor="aug26", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", publication_date="2026-04-02", quarter="2026-06-30", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_jul26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.9525, contract_tenor="jul26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", publication_date="2026-04-02", quarter="2026-06-30", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_aug26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.9225, contract_tenor="aug26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", publication_date="2026-04-02", quarter="2026-06-30", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_sep26_usd_per_gal", instrument="Chicago Ethanol (Platts) Futures", price_value=1.8925, contract_tenor="sep26", source_type="local_chicago_ethanol_futures_csv"),
    ]
    bumped_rows = [dict(rec) for rec in base_rows]
    bumped_rows[3]["price_value"] = 2.0225

    base_thesis = build_next_quarter_thesis_snapshot(base_rows, as_of_date=date(2026, 4, 4))
    bumped_thesis = build_next_quarter_thesis_snapshot(bumped_rows, as_of_date=date(2026, 4, 4))

    assert float(pd.to_numeric(bumped_thesis["ethanol"]["price_value"], errors="coerce")) > float(pd.to_numeric(base_thesis["ethanol"]["price_value"], errors="coerce"))


def test_local_ethanol_futures_do_not_contaminate_current_observed_ethanol_logic() -> None:
    rows = [
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", quarter="2026-06-30", publication_date="2026-04-03", market_family="corn_price", series_key="corn_nebraska", price_value=4.05, source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-04", quarter="2026-06-30", publication_date="2026-04-04", market_family="corn_price", series_key="corn_nebraska", price_value=4.15, source_type="ams_3617_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", quarter="2026-06-30", publication_date="2026-04-03", market_family="ethanol_price", series_key="ethanol_nebraska", price_value=1.62, source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-03", quarter="2026-06-30", publication_date="2026-04-03", market_family="natural_gas_price", series_key="nymex_gas", price_value=3.10, contract_tenor="front", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", quarter="2026-06-30", publication_date="2026-04-02", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_jul26_usd_per_gal", price_value=1.9525, contract_tenor="jul26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", quarter="2026-06-30", publication_date="2026-04-02", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_aug26_usd_per_gal", price_value=1.9225, contract_tenor="aug26", source_type="local_chicago_ethanol_futures_csv"),
        _parsed_row(aggregation_level="observation", observation_date="2026-04-02", quarter="2026-06-30", publication_date="2026-04-02", market_family="ethanol_futures", series_key="cme_ethanol_chicago_platts_sep26_usd_per_gal", price_value=1.8925, contract_tenor="sep26", source_type="local_chicago_ethanol_futures_csv"),
    ]

    current = build_current_qtd_simple_crush_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 4),
    )

    assert float(pd.to_numeric((current.get("current_market") or {}).get("ethanol_price"), errors="coerce")) == pytest.approx(1.62, abs=1e-6)


def test_manual_quarter_open_snapshot_parser_reads_rows_and_contracts() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_manual_qopen_parse_")
    try:
        futures_dir = tmp_path / "GPRE" / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = _write_manual_quarter_open_snapshot(futures_dir / "ethanol_chicago_futures_2026_Q2.txt")

        files = find_local_manual_ethanol_quarter_open_files(tmp_path / "GPRE")
        rows = parse_manual_ethanol_quarter_open_snapshot_table(snapshot_path)

        assert [path.name for path in files] == ["ethanol_chicago_futures_2026_Q2.txt"]
        assert len(rows) == 3
        assert [str(rec["contract_tenor"]) for rec in rows] == ["apr26", "may26", "jun26"]
        assert all(rec["target_quarter_end"] == date(2026, 6, 30) for rec in rows)
        assert all(float(pd.to_numeric(rec["settle_usd_per_gal"], errors="coerce")) > 0.0 for rec in rows)
        assert all(rec["snapshot_date"] == date(2026, 3, 31) for rec in rows)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_manual_quarter_open_snapshot_prefers_stable_filename_deterministically() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_manual_qopen_priority_")
    try:
        futures_dir = tmp_path / "GPRE" / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        _write_manual_quarter_open_snapshot(
            futures_dir / "ethanol_chicago_futures_2026_Q2.txt",
            rows=[
                ("2026-03-31", "2026-Q2", "2026-04", "1.9000", "vendor_a"),
                ("2026-03-31", "2026-Q2", "2026-05", "1.9050", "vendor_a"),
                ("2026-03-31", "2026-Q2", "2026-06", "1.9100", "vendor_a"),
            ],
        )
        _write_manual_quarter_open_snapshot(
            futures_dir / "manual_ethanol_chicago_quarter_open_2026-03-31.csv",
            rows=[
                ("2026-03-31", "2026-Q2", "2026-04", "2.0025", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-05", "2.0050", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-06", "1.9825", "barchart_manual"),
            ],
        )

        rows = load_local_manual_ethanol_quarter_open_snapshot_rows(tmp_path / "GPRE")

        assert [str(rec["source_file"]) for rec in rows] == [
            "manual_ethanol_chicago_quarter_open_2026-03-31.csv",
            "manual_ethanol_chicago_quarter_open_2026-03-31.csv",
            "manual_ethanol_chicago_quarter_open_2026-03-31.csv",
        ]
        assert [float(pd.to_numeric(rec["settle_usd_per_gal"], errors="coerce")) for rec in rows] == [
            pytest.approx(2.0025, abs=1e-9),
            pytest.approx(2.0050, abs=1e-9),
            pytest.approx(1.9825, abs=1e-9),
        ]
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_manual_quarter_open_snapshot_ignores_malformed_rows_without_poisoning_valid_rows() -> None:
    tmp_path = _local_test_dir(".pytest_tmp_manual_qopen_malformed_")
    try:
        futures_dir = tmp_path / "GPRE" / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        _write_manual_quarter_open_snapshot(
            futures_dir / "manual_ethanol_chicago_quarter_open.csv",
            rows=[
                ("2026-03-31", "2026-Q2", "2026-04", "2.0025", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-05", "oops", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-05", "2.0050", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-06", "-1.0000", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-06", "1.9825", "barchart_manual"),
                ("bad-date", "2026-Q2", "2026-06", "1.9900", "barchart_manual"),
            ],
        )

        rows = load_local_manual_ethanol_quarter_open_snapshot_rows(tmp_path / "GPRE")

        assert [str(rec["contract_tenor"]) for rec in rows] == ["apr26", "may26", "jun26"]
        assert [float(pd.to_numeric(rec["settle_usd_per_gal"], errors="coerce")) for rec in rows] == [
            pytest.approx(2.0025, abs=1e-9),
            pytest.approx(2.0050, abs=1e-9),
            pytest.approx(1.9825, abs=1e-9),
        ]
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_manual_quarter_open_strip_backfills_current_quarter_when_frozen_missing() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_qopen_manual_fill_")
    try:
        futures_dir = ticker_root / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        _write_manual_quarter_open_snapshot(futures_dir / "ethanol_chicago_futures_2026_Q2.txt")
        rows = _gpre_overlay_fixture_rows()
        rows.extend(
            [
                _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_futures", series_key="cbot_corn_jun26_usd", instrument="Corn futures", price_value=4.62, contract_tenor="jun26", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="natural_gas_price", series_key="nymex_gas_jun26_usd", instrument="Natural gas futures", price_value=3.18, contract_tenor="jun26", source_type="nwer_pdf"),
            ]
        )

        bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            bids_snapshot=_gpre_bids_snapshot_fixture(date(2026, 5, 31), basis_value=-0.20),
            gpre_basis_model_result=_gpre_basis_model_result_fixture(),
        )

        expected_strip = ((2.0025 * 30.0) + (2.0050 * 31.0) + (1.9825 * 30.0)) / 91.0
        expected_proxy = expected_strip - (4.62 / 2.9) - ((28000.0 / 1_000_000.0) * 3.18)
        quarter_open_meta = dict((bundle.get("quarter_open_market_snapshot") or {}).get("market_meta") or {})
        corn_meta = dict(quarter_open_meta.get("corn_price") or {})
        ethanol_meta = dict(quarter_open_meta.get("ethanol_price") or {})

        assert bundle["quarter_open_snapshot_status"] == "ok"
        assert bundle["quarter_open_provenance"] == "manual_local_snapshot"
        assert float(pd.to_numeric(bundle["quarter_open_official_proxy_usd_per_gal"], errors="coerce")) == pytest.approx(expected_proxy, abs=1e-9)
        assert pd.isna(pd.to_numeric(bundle["quarter_open_gpre_proxy_usd_per_gal"], errors="coerce"))
        assert float(pd.to_numeric(((bundle.get("quarter_open_market_snapshot") or {}).get("current_market") or {}).get("ethanol_price"), errors="coerce")) == pytest.approx(expected_strip, abs=1e-9)
        assert corn_meta.get("official_corn_basis_source_kind") == "weighted_ams_proxy"
        assert corn_meta.get("official_corn_basis_snapshot_date") is None
        assert ethanol_meta.get("source_type") == "manual_local_snapshot"
        assert ethanol_meta.get("quarter_open_provenance") == "manual_local_snapshot"
        assert ethanol_meta.get("contract_tenors") == ["apr26", "may26", "jun26"]
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_quarter_open_frozen_snapshot_wins_over_manual_local_snapshot() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_qopen_precedence_")
    try:
        futures_dir = ticker_root / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        _write_manual_quarter_open_snapshot(futures_dir / "manual_ethanol_chicago_quarter_open.csv")
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "official_market_snapshot": {
                    "status": "ok_thesis",
                    "display_quarter": date(2026, 6, 30),
                    "calendar_quarter": date(2026, 6, 30),
                    "current_market": {"ethanol_price": 1.5400},
                    "current_process": {"simple_crush_per_gal": -0.0362},
                    "market_meta": {"ethanol_price": {"source_type": "local_chicago_ethanol_futures_csv"}},
                    "process_status": "ok",
                },
                "official_simple_proxy_usd_per_gal": -0.0362,
                "gpre_proxy_official_usd_per_gal": -0.0184,
                "gpre_proxy_model_key": "process_front_loaded",
            },
        )

        resolved = market_service.resolve_gpre_quarter_open_snapshot(
            ticker_root,
            current_quarter_end=date(2026, 6, 30),
            rows=[],
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
        )

        assert resolved["status"] == "ok"
        assert resolved["provenance"] == "frozen_snapshot"
        assert float(pd.to_numeric(resolved["official_simple_proxy_usd_per_gal"], errors="coerce")) == pytest.approx(-0.0362, abs=1e-9)
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_manual_quarter_open_snapshot_requires_all_three_months() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_qopen_missing_month_")
    try:
        futures_dir = ticker_root / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        _write_manual_quarter_open_snapshot(
            futures_dir / "manual_ethanol_chicago_quarter_open.csv",
            rows=[
                ("2026-03-31", "2026-Q2", "2026-04", "2.0025", "barchart_manual"),
                ("2026-03-31", "2026-Q2", "2026-05", "2.0050", "barchart_manual"),
            ],
        )
        rows = _gpre_overlay_fixture_rows()
        rows.extend(
            [
                _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_futures", series_key="cbot_corn_jun26_usd", instrument="Corn futures", price_value=4.62, contract_tenor="jun26", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="natural_gas_price", series_key="nymex_gas_jun26_usd", instrument="Natural gas futures", price_value=3.18, contract_tenor="jun26", source_type="nwer_pdf"),
            ]
        )

        bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            bids_snapshot=_gpre_bids_snapshot_fixture(date(2026, 5, 31), basis_value=-0.20),
            gpre_basis_model_result=_gpre_basis_model_result_fixture(),
        )

        assert bundle["quarter_open_snapshot_status"] == "no_snapshot"
        assert bundle["quarter_open_provenance"] == "unavailable"
        assert bundle["quarter_open_official_proxy_usd_per_gal"] is None
        assert "missing contract months: jun26" in str((bundle.get("quarter_open_market_snapshot") or {}).get("message") or "").lower()
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_manual_quarter_open_snapshot_does_not_leak_into_next_quarter_thesis_logic() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_qopen_no_leak_")
    try:
        futures_dir = ticker_root / "Ethanol_futures"
        futures_dir.mkdir(parents=True, exist_ok=True)
        _write_manual_quarter_open_snapshot(futures_dir / "manual_ethanol_chicago_quarter_open.csv")
        rows = _gpre_overlay_fixture_rows()
        rows.extend(
            [
                _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="corn_futures", series_key="cbot_corn_jun26_usd", instrument="Corn futures", price_value=4.62, contract_tenor="jun26", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date="2026-03-31", publication_date="2026-03-31", quarter="2026-03-31", market_family="natural_gas_price", series_key="nymex_gas_jun26_usd", instrument="Natural gas futures", price_value=3.18, contract_tenor="jun26", source_type="nwer_pdf"),
            ]
        )

        bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
            rows,
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            bids_snapshot=_gpre_bids_snapshot_fixture(date(2026, 5, 31), basis_value=-0.20),
            gpre_basis_model_result=_gpre_basis_model_result_fixture(),
        )

        next_ethanol = dict(((bundle.get("next_thesis_preview_snapshot") or {}).get("market_meta") or {}).get("ethanol_price") or {})
        assert next_ethanol.get("source_type") == "local_chicago_ethanol_futures_csv"
        assert next_ethanol.get("contract_tenors") == ["jul26", "aug26", "sep26"]
        assert bundle["quarter_open_provenance"] == "manual_local_snapshot"
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_quarter_open_proxy_uses_prior_frozen_snapshot_and_stays_frozen() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_quarter_open_")
    try:
        frozen_entry = {
            "snapshot_as_of": date(2026, 3, 31),
            "source_quarter_end": date(2026, 3, 31),
            "target_quarter_end": date(2026, 6, 30),
            "official_market_snapshot": {
                "status": "ok_thesis",
                "display_quarter": date(2026, 6, 30),
                "calendar_quarter": date(2026, 6, 30),
                "message": "",
                "current_market": {
                    "corn_price": 4.31,
                    "ethanol_price": 1.54,
                    "natural_gas_price": 3.22,
                    "cbot_corn_front_price": 4.45,
                },
                "current_process": {
                    "ethanol_revenue": 4.466,
                    "feedstock_cost": -4.31,
                    "natural_gas_burden": -0.261,
                    "simple_crush": -0.105,
                    "simple_crush_per_gal": -0.0362,
                },
                "market_meta": {},
                "process_status": "ok",
            },
            "official_simple_proxy_usd_per_gal": -0.0362,
            "gpre_proxy_official_usd_per_gal": -0.0184,
            "gpre_proxy_model_key": "process_front_loaded",
        }
        market_service.persist_gpre_frozen_thesis_snapshot(ticker_root, frozen_entry)
        basis_model_result = {
            "quarterly_df": pd.DataFrame(
                [
                    {
                        "quarter": date(2026, 3, 31),
                        "weighted_basis_plant_count_usd_per_bu": -0.18,
                        "gpre_proxy_official_usd_per_gal": -0.012,
                        "approx_market_bridge_proxy_usd_per_gal": 0.004,
                        "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                        "process_proxy_current_quarter_avg_usd_per_gal": -0.012,
                        "process_proxy_front_loaded_usd_per_gal": -0.020,
                    }
                ]
            ),
            "gpre_proxy_model_key": "process_front_loaded",
            "gpre_proxy_family": "process_family",
            "gpre_proxy_family_label": "Process",
            "gpre_proxy_timing_rule": "Front-loaded current",
        }
        bundle_a = market_service.build_gpre_overlay_proxy_preview_bundle(
            _gpre_overlay_fixture_rows(ethanol_base=1.60, corn_front=4.50, gas_front=3.00),
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_model_result,
        )
        bundle_b = market_service.build_gpre_overlay_proxy_preview_bundle(
            _gpre_overlay_fixture_rows(ethanol_base=1.90, corn_front=4.95, gas_front=4.40),
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_model_result,
        )

        assert bundle_a["quarter_open_snapshot_status"] == "ok"
        assert bundle_a["quarter_open_official_proxy_usd_per_gal"] == pytest.approx(-0.0362, abs=1e-6)
        assert bundle_a["quarter_open_gpre_proxy_usd_per_gal"] == pytest.approx(-0.0184, abs=1e-6)
        assert bundle_b["quarter_open_official_proxy_usd_per_gal"] == pytest.approx(-0.0362, abs=1e-6)
        assert bundle_b["quarter_open_gpre_proxy_usd_per_gal"] == pytest.approx(-0.0184, abs=1e-6)
        assert bundle_a["official_frames"]["current_qtd"]["value"] != bundle_b["official_frames"]["current_qtd"]["value"]
        current_helper = dict((bundle_a.get("gpre_proxy_formula_helpers") or {}).get("current_qtd") or {})
        next_helper = dict((bundle_a.get("gpre_proxy_formula_helpers") or {}).get("next_quarter_thesis") or {})
        assert current_helper["status"] == "ok"
        assert current_helper["mode"] == "process"
        assert float(pd.to_numeric(current_helper["slope"], errors="coerce")) == pytest.approx(1.0, abs=1e-9)
        assert pd.notna(pd.to_numeric(current_helper["intercept"], errors="coerce"))
        assert str(next_helper.get("mode") or "") == "process"
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_quarter_open_snapshot_carries_local_futures_based_ethanol_thesis() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_quarter_open_local_ethanol_")
    try:
        ethanol_thesis = {
            "status": "ok",
            "price_value": 1.922826087,
            "source_type": "local_chicago_ethanol_futures_csv",
            "source_label": "local Chicago ethanol futures CSV",
            "contract_tenors": ["jul26", "aug26", "sep26"],
            "contract_labels": ["Jul 2026", "Aug 2026", "Sep 2026"],
            "strip_method": "day_weighted",
            "observation_date": date(2026, 4, 2),
        }
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "next_quarter_thesis_snapshot": {"ethanol": ethanol_thesis},
                "official_market_snapshot": {
                    "status": "ok_thesis",
                    "display_quarter": date(2026, 6, 30),
                    "calendar_quarter": date(2026, 6, 30),
                    "message": "",
                    "current_market": {
                        "corn_price": 4.31,
                        "ethanol_price": 1.922826087,
                        "natural_gas_price": 3.22,
                        "cbot_corn_front_price": 4.45,
                    },
                    "current_process": {
                        "simple_crush_per_gal": 0.0123,
                    },
                    "market_meta": {
                        "ethanol_price": {
                            "source_type": "local_chicago_ethanol_futures_csv",
                            "contract_tenors": ["jul26", "aug26", "sep26"],
                        }
                    },
                    "process_status": "ok",
                },
                "official_simple_proxy_usd_per_gal": 0.0123,
                "gpre_proxy_official_usd_per_gal": 0.0456,
                "gpre_proxy_model_key": "process_front_loaded",
            },
        )

        resolved = market_service.resolve_gpre_quarter_open_snapshot(
            ticker_root,
            current_quarter_end=date(2026, 6, 30),
        )

        assert resolved["status"] == "ok"
        assert resolved["next_quarter_thesis_snapshot"]["ethanol"]["source_type"] == "local_chicago_ethanol_futures_csv"
        assert float(pd.to_numeric((((resolved.get("official_market_snapshot") or {}).get("current_market") or {}).get("ethanol_price")), errors="coerce")) == pytest.approx(1.922826087, abs=1e-9)
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_overlay_preview_bundle_populates_next_quarter_thesis_from_local_futures_without_manual_input() -> None:
    bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
        _gpre_overlay_fixture_rows(),
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
        gpre_basis_model_result={
            "quarterly_df": pd.DataFrame(
                [
                    {
                        "quarter": date(2026, 3, 31),
                        "weighted_basis_plant_count_usd_per_bu": -0.18,
                        "gpre_proxy_official_usd_per_gal": -0.012,
                        "approx_market_bridge_proxy_usd_per_gal": 0.004,
                        "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                        "process_proxy_current_quarter_avg_usd_per_gal": -0.012,
                        "process_proxy_front_loaded_usd_per_gal": -0.020,
                    }
                ]
            ),
            "gpre_proxy_model_key": "process_front_loaded",
            "gpre_proxy_family": "process_family",
            "gpre_proxy_family_label": "Process",
            "gpre_proxy_timing_rule": "Front-loaded current",
        },
    )

    next_snapshot = dict(bundle.get("next_thesis_preview_snapshot") or {})
    ethanol_meta = dict((next_snapshot.get("market_meta") or {}).get("ethanol_price") or {})
    assert next_snapshot["status"] == "ok_thesis"
    assert pd.notna(pd.to_numeric((next_snapshot.get("current_market") or {}).get("ethanol_price"), errors="coerce"))
    assert ethanol_meta.get("proxy_mode") == "local_chicago_ethanol_futures_strip"
    assert ethanol_meta.get("contract_tenors") == ["jul26", "aug26", "sep26"]
    assert ethanol_meta.get("strip_method") == "day_weighted"
    assert ethanol_meta.get("source_type") == "local_chicago_ethanol_futures_csv"
    assert "manual" not in str(next_snapshot.get("message") or "").lower()


def test_build_gpre_official_proxy_snapshot_accepts_prebuilt_dataframe_without_output_drift() -> None:
    rows = _gpre_overlay_fixture_rows()
    rows_df = market_service._market_rows_df(rows)

    list_snapshot = build_gpre_official_proxy_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
    )
    df_snapshot = build_gpre_official_proxy_snapshot(
        rows_df,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
    )

    assert df_snapshot["current_market"] == list_snapshot["current_market"]
    assert df_snapshot["current_process"] == list_snapshot["current_process"]
    assert df_snapshot["market_meta"] == list_snapshot["market_meta"]
    assert df_snapshot["weekly_rows"] == list_snapshot["weekly_rows"]


def test_build_gpre_official_proxy_history_series_accepts_prebuilt_dataframe_without_output_drift() -> None:
    rows = _gpre_overlay_fixture_rows()
    rows_df = market_service._market_rows_df(rows)

    list_history = build_gpre_official_proxy_history_series(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        start_date=date(2026, 1, 1),
        ticker_root=None,
    )
    df_history = build_gpre_official_proxy_history_series(
        rows_df,
        ethanol_yield=2.9,
        natural_gas_usage=28000.0,
        as_of_date=date(2026, 4, 3),
        start_date=date(2026, 1, 1),
        ticker_root=None,
    )

    assert df_history == list_history


def test_build_next_quarter_thesis_snapshot_accepts_prebuilt_dataframe_without_output_drift() -> None:
    rows = _gpre_overlay_fixture_rows()
    rows_df = market_service._market_rows_df(rows)

    list_snapshot = build_next_quarter_thesis_snapshot(
        rows,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
    )
    df_snapshot = build_next_quarter_thesis_snapshot(
        rows_df,
        as_of_date=date(2026, 4, 3),
        ticker_root=None,
    )

    assert df_snapshot == list_snapshot


def test_gpre_overlay_preview_bundle_exposes_hedge_formula_helper_for_live_thesis_cells() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_quarter_open_hedge_")
    try:
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "official_market_snapshot": {
                    "status": "ok_thesis",
                    "display_quarter": date(2026, 6, 30),
                    "calendar_quarter": date(2026, 6, 30),
                    "message": "",
                    "current_market": {},
                    "current_process": {},
                    "market_meta": {},
                    "process_status": "no_data",
                },
                "official_simple_proxy_usd_per_gal": 0.0123,
                "gpre_proxy_official_usd_per_gal": -0.0184,
                "gpre_proxy_model_key": "hedge_pattern_process_prior_current",
            },
        )
        basis_model_result = {
            "quarterly_df": pd.DataFrame(
                [
                    {
                        "quarter": date(2026, 3, 31),
                        "weighted_basis_plant_count_usd_per_bu": -0.18,
                        "gpre_proxy_official_usd_per_gal": -0.030,
                        "approx_market_bridge_proxy_usd_per_gal": 0.004,
                        "bridge_proxy_front_loaded_usd_per_gal": 0.007,
                        "process_proxy_current_quarter_avg_usd_per_gal": -0.022,
                        "process_proxy_front_loaded_usd_per_gal": -0.020,
                    }
                ]
            ),
            "gpre_proxy_model_key": "hedge_pattern_process_prior_current",
            "gpre_proxy_family": "hedge_memo",
            "gpre_proxy_family_label": "Hedge memo",
            "gpre_proxy_timing_rule": "Pattern process prior-current",
        }
        bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
            _gpre_overlay_fixture_rows(ethanol_base=1.70, corn_front=4.60, gas_front=3.10),
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            gpre_basis_model_result=basis_model_result,
        )

        next_helper = dict((bundle.get("gpre_proxy_formula_helpers") or {}).get("next_quarter_thesis") or {})
        assert next_helper["status"] == "ok"
        assert next_helper["mode"] == "hedge_process"
        assert float(pd.to_numeric(next_helper["slope"], errors="coerce")) == pytest.approx(0.35, abs=1e-9)
        assert float(pd.to_numeric(next_helper["hedge_share"], errors="coerce")) == pytest.approx(0.65, abs=1e-9)
        assert pd.notna(pd.to_numeric(next_helper["anchor"], errors="coerce"))
        assert pd.notna(pd.to_numeric(next_helper["intercept"], errors="coerce"))
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_usda_archive_backfill_continues_per_source_when_one_provider_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = _local_test_dir(".pytest_tmp_usda_backfill_continue_")
    try:
        class _FakeProvider:
            def __init__(self, source: str) -> None:
                self.source = source

            def _local_dir(self, ticker_root: Path) -> Path:
                path = ticker_root / f"{self.source}_data"
                path.mkdir(parents=True, exist_ok=True)
                return path

            def _write_remote_debug(self, cache_root: Path | None, payload: dict[str, object], *, merge: bool = False) -> None:
                del cache_root, payload, merge

        fake_providers = {
            "nwer": _FakeProvider("nwer"),
            "ams_3617": _FakeProvider("ams_3617"),
        }
        monkeypatch.setattr(usda_backfill_module, "PROVIDERS", fake_providers)

        def _fake_download(provider: object, ticker_root: Path, start_date: date, end_date: date, *, cache_root: Path | None = None):
            del cache_root
            source = str(getattr(provider, "source", ""))
            if source == "nwer":
                raise TimeoutError("read timed out")
            return usda_backfill_module.USDAProviderBackfillSummary(
                source=source,
                local_dir=getattr(provider, "_local_dir")(ticker_root),
                start_date=start_date,
                end_date=end_date,
                discovered_assets=3,
                downloaded_files=2,
                skipped_existing=1,
            )

        monkeypatch.setattr(usda_backfill_module, "download_archive_assets", _fake_download)
        summary = usda_backfill_module.run_usda_archive_backfill(
            repo_root=repo_root,
            ticker="GPRE",
            start_date=date(2026, 1, 23),
            end_date=date(2026, 3, 31),
            sources=("nwer", "ams_3617"),
            sync_cache=False,
        )

        assert [item.source for item in summary.provider_summaries] == ["nwer", "ams_3617"]
        assert summary.provider_summaries[0].error_text.startswith("TimeoutError:")
        assert summary.provider_summaries[0].downloaded_files == 0
        assert summary.provider_summaries[1].error_text == ""
        assert summary.provider_summaries[1].downloaded_files == 2
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_usda_archive_backfill_marks_failed_sources_explicitly(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = _local_test_dir(".pytest_tmp_usda_backfill_fail_")
    try:
        class _FakeProvider:
            def __init__(self, source: str) -> None:
                self.source = source

            def _local_dir(self, ticker_root: Path) -> Path:
                path = ticker_root / f"{self.source}_data"
                path.mkdir(parents=True, exist_ok=True)
                return path

            def _write_remote_debug(self, cache_root: Path | None, payload: dict[str, object], *, merge: bool = False) -> None:
                del cache_root, payload, merge

        fake_providers = {
            "nwer": _FakeProvider("nwer"),
            "ams_3617": _FakeProvider("ams_3617"),
        }
        monkeypatch.setattr(usda_backfill_module, "PROVIDERS", fake_providers)
        monkeypatch.setattr(
            usda_backfill_module,
            "download_archive_assets",
            lambda provider, ticker_root, start_date, end_date, cache_root=None: (_ for _ in ()).throw(TimeoutError(f"{getattr(provider, 'source', 'unknown')} timed out")),
        )

        summary = usda_backfill_module.run_usda_archive_backfill(
            repo_root=repo_root,
            ticker="GPRE",
            start_date=date(2026, 1, 23),
            end_date=date(2026, 3, 31),
            sources=("nwer", "ams_3617"),
            sync_cache=False,
        )

        assert len(summary.provider_summaries) == 2
        assert all(item.error_text.startswith("TimeoutError:") for item in summary.provider_summaries)
        assert all(item.downloaded_files == 0 for item in summary.provider_summaries)
        assert all(item.discovered_assets == 0 for item in summary.provider_summaries)
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_gpre_quarter_open_proxy_surfaces_missing_snapshot_explicitly() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_quarter_open_missing_")
    try:
        bundle = market_service.build_gpre_overlay_proxy_preview_bundle(
            _gpre_overlay_fixture_rows(),
            ethanol_yield=2.9,
            natural_gas_usage=28000.0,
            as_of_date=date(2026, 4, 3),
            ticker_root=ticker_root,
            gpre_basis_model_result={
                "quarterly_df": pd.DataFrame(),
                "gpre_proxy_model_key": "process_front_loaded",
                "gpre_proxy_family": "process_family",
                "gpre_proxy_family_label": "Process",
                "gpre_proxy_timing_rule": "Front-loaded current",
            },
        )

        assert bundle["quarter_open_snapshot_status"] == "no_snapshot"
        assert bundle["quarter_open_official_proxy_usd_per_gal"] is None
        assert bundle["quarter_open_gpre_proxy_usd_per_gal"] is None
        assert "No frozen prior-quarter thesis snapshot for 2026-Q2." in str(bundle["quarter_open_market_snapshot"]["message"] or "")
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_quarter_open_snapshot_resolves_exactly_on_quarter_rollover() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_quarter_roll_")
    try:
        market_service.persist_gpre_frozen_thesis_snapshot(
            ticker_root,
            {
                "snapshot_as_of": date(2026, 3, 31),
                "source_quarter_end": date(2026, 3, 31),
                "target_quarter_end": date(2026, 6, 30),
                "official_market_snapshot": {"status": "ok_thesis", "display_quarter": date(2026, 6, 30), "current_market": {}, "current_process": {}, "market_meta": {}, "process_status": "no_data"},
                "official_simple_proxy_usd_per_gal": 0.0123,
                "gpre_proxy_official_usd_per_gal": 0.0456,
                "gpre_proxy_model_key": "bridge_front_loaded",
            },
        )
        resolved = market_service.resolve_gpre_quarter_open_snapshot(
            ticker_root,
            current_quarter_end=date(2026, 6, 30),
        )

        assert resolved["status"] == "ok"
        assert resolved["source_quarter_end"] == date(2026, 3, 31)
        assert resolved["target_quarter_end"] == date(2026, 6, 30)
        assert float(pd.to_numeric(resolved["official_simple_proxy_usd_per_gal"], errors="coerce")) == pytest.approx(0.0123, abs=1e-9)
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_build_gpre_basis_proxy_model_supports_mixed_underlying_target_and_front_loaded_basis() -> None:
    quarters = pd.date_range("2023-03-31", periods=12, freq="QE")
    rows: list[dict[str, object]] = []
    reported: dict[date, float] = {}
    underlying: dict[date, float] = {}
    denominator_policy: dict[date, str] = {}
    region_offsets = {
        "nebraska": -0.28,
        "iowa_west": -0.22,
        "minnesota": -0.18,
        "indiana": -0.15,
        "illinois": -0.12,
        "iowa_east": -0.16,
    }
    plant_count_weights = {
        "nebraska": 3 / 8,
        "iowa_west": 2 / 8,
        "minnesota": 1 / 8,
        "indiana": 1 / 8,
        "illinois": 1 / 8,
        "iowa_east": 0.0,
    }
    front_profile = np.array([1.0, 0.45, 0.20], dtype=float)
    front_profile = front_profile / float(front_profile.sum())
    for idx, ts in enumerate(quarters):
        qd = ts.date()
        ethanol = 1.48 + (0.018 * idx)
        futures = 4.10 + (0.025 * idx)
        gas = 3.00 + (0.03 * ((idx % 4) - 1))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=ethanol, source_type="nwer_pdf"))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=futures, source_type="nwer_pdf"))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=gas, source_type="nwer_pdf", unit="$/MMBtu"))
        q_start = pd.Timestamp(qd) - pd.offsets.QuarterEnd() + pd.Timedelta(days=1)
        obs_dates = [q_start.date(), (q_start + pd.Timedelta(days=28)).date(), (q_start + pd.Timedelta(days=65)).date()]
        weighted_basis_avg = 0.0
        weighted_basis_front = 0.0
        for region, base in region_offsets.items():
            path = np.array(
                [
                    base - 0.09 + (0.005 * (idx % 2)),
                    base + 0.01,
                    base + 0.08 - (0.004 * (idx % 3)),
                ],
                dtype=float,
            )
            avg_basis = float(path.mean())
            front_basis = float(np.dot(path, front_profile))
            weighted_basis_avg += float(plant_count_weights.get(region, 0.0)) * avg_basis
            weighted_basis_front += float(plant_count_weights.get(region, 0.0)) * front_basis
            rows.append(
                _parsed_row(
                    aggregation_level="quarter_avg",
                    observation_date=qd.isoformat(),
                    publication_date=qd.isoformat(),
                    quarter=qd.isoformat(),
                    market_family="corn_basis",
                    series_key=f"corn_basis_{region}",
                    instrument="Corn basis",
                    location=region.replace("_", " ").title(),
                    region=region,
                    price_value=avg_basis,
                    unit="$/bushel",
                    source_type="ams_3617_pdf",
                )
            )
            for obs_dt, basis_val in zip(obs_dates, path):
                rows.append(
                    _parsed_row(
                        aggregation_level="observation",
                        observation_date=obs_dt.isoformat(),
                        publication_date=obs_dt.isoformat(),
                        quarter=qd.isoformat(),
                        market_family="corn_basis",
                        series_key=f"corn_basis_{region}",
                        instrument="Corn basis",
                        location=region.replace("_", " ").title(),
                        region=region,
                        price_value=float(basis_val),
                        unit="$/bushel",
                        source_type="ams_3617_pdf",
                    )
                )
        baseline = ethanol - (futures / 2.9) - (0.028 * gas)
        reported[qd] = baseline - (weighted_basis_avg / 2.9)
        if idx >= 9:
            underlying[qd] = baseline - (weighted_basis_front / 2.9)
        denominator_policy[qd] = "ethanol gallons sold"

    result = build_gpre_basis_proxy_model(
        rows,
        ticker_root=None,
        reported_margin_by_quarter=reported,
        underlying_margin_by_quarter=underlying,
        denominator_policy_by_quarter=denominator_policy,
        as_of_date=date(2026, 4, 1),
    )

    quarterly_df = result["quarterly_df"].copy()
    metrics_df = result["metrics_df"].copy()
    quarterly_df["quarter_date"] = pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date

    latest_three = list(quarterly_df.sort_values("quarter_date")["quarter_date"].tail(3))
    assert latest_three == sorted(underlying.keys())
    assert quarterly_df[quarterly_df["quarter_date"].isin(latest_three)]["underlying_crush_margin_usd_per_gal"].notna().all()
    assert quarterly_df[quarterly_df["quarter_date"].isin(latest_three)]["evaluation_target_margin_usd_per_gal"].notna().all()
    assert set(quarterly_df[quarterly_df["quarter_date"].isin(latest_three)]["target_basis"].astype(str)) == {"underlying"}
    assert set(quarterly_df[quarterly_df["quarter_date"] <= date(2025, 3, 31)]["target_basis"].astype(str)) == {"reported_clean"}
    chosen_key = str(result["gpre_proxy_model_key"] or "")
    assert chosen_key
    assert set(quarterly_df["bridge_official_model_key"].astype(str)) == {chosen_key}
    plant_test = metrics_df[(metrics_df["model_key"].astype(str) == "bridge_current_quarter_avg") & (metrics_df["split"].astype(str) == "test")].copy()
    front_test = metrics_df[(metrics_df["model_key"].astype(str) == "bridge_front_loaded") & (metrics_df["split"].astype(str) == "test")].copy()
    assert not plant_test.empty and not front_test.empty
    assert pd.notna(pd.to_numeric(quarterly_df["approx_market_bridge_proxy_usd_per_gal"], errors="coerce")).any()
    assert pd.notna(pd.to_numeric(quarterly_df["hedge_memo_disclosed_bridge_prior_current_usd_per_gal"], errors="coerce")).all()
    assert pd.notna(pd.to_numeric(quarterly_df["hedge_memo_pattern_process_prior_current_usd_per_gal"], errors="coerce")).all()
    leaderboard_df = result["leaderboard_df"].copy()
    assert not leaderboard_df.empty
    chosen = leaderboard_df[leaderboard_df["chosen"] == True].copy()
    assert len(chosen) == 1
    assert str(result["incumbent_baseline_model_key"] or "")
    assert str(result["expanded_candidate_model_key"] or "")
    assert str(result["expanded_best_candidate_model_key"] or "")
    assert str(result["production_winner_model_key"] or "")
    assert {
        "process_quarter_open_blend",
        "process_quarter_open_blend_ops_penalty",
        "process_quarter_open_blend_hedge_realization",
        "process_quarter_open_blend_exec_penalty",
        "process_front_loaded_ops_penalty",
        "process_front_loaded_ethanol_geo",
    } <= set(
        leaderboard_df["model_key"].astype(str)
    )
    chosen_hybrid = float(pd.to_numeric(chosen.iloc[0]["hybrid_score"], errors="coerce"))
    assert np.isfinite(chosen_hybrid)
    assert str(chosen.iloc[0].get("selection_guard_reason") or "")
    assert {
        "selection_guard_pass",
        "promotion_guard_pass",
        "incremental_value_status",
        "live_preview_mae",
        "live_preview_quality_status",
        "hard_quarter_mae",
    } <= set(leaderboard_df.columns)
    assert "preview" in str(result["summary_markdown"] or "").lower()
    assert "hard-quarter" in str(result["summary_markdown"] or "").lower()
    assert isinstance(result.get("preview_accuracy_by_model"), dict)
    assert {"prior", "quarter_open", "current", "next"} <= set((result["preview_accuracy_by_model"].get(chosen_key) or {}).keys())
    assert isinstance(result.get("recent_quarter_comparison_df"), pd.DataFrame)
    assert not result["recent_quarter_comparison_df"].empty
    chosen_preview_quality = str(chosen.iloc[0].get("live_preview_quality_status") or "")
    assert chosen_preview_quality in {"close", "acceptable", "loose", "not_faithful_enough"}
    assert "actual incumbent baseline" in str(result["summary_markdown"] or "").lower()
    assert "process comparator" in str(result["summary_markdown"] or "").lower()
    assert "expanded-pass best candidate" in str(result["summary_markdown"] or "").lower()
    assert "production winner" in str(result["summary_markdown"] or "").lower()
    assert "production decision story" in str(result["summary_markdown"] or "").lower()
    assert "selection vs promotion" in str(result["summary_markdown"] or "").lower()
    assert str(result.get("production_decision_story") or "")
    assert str(result.get("selection_vs_promotion_explanation") or "")
    assert "hybrid score" in str(result["summary_markdown"] or "").lower()


def test_build_gpre_basis_proxy_model_supports_bid_adjusted_offset_comparison() -> None:
    quarters = pd.date_range("2023-03-31", periods=12, freq="QE")
    rows: list[dict[str, object]] = []
    reported: dict[date, float] = {}
    denominator_policy: dict[date, str] = {}
    region_base = {
        "nebraska": -0.32,
        "iowa_west": -0.26,
        "minnesota": -0.41,
        "indiana": 0.03,
        "illinois": 0.01,
    }
    region_bid_offsets = {
        "nebraska": 0.06,
        "iowa_west": 0.03,
        "minnesota": -0.04,
        "indiana": 0.12,
        "illinois": 0.10,
    }
    plant_count_weights = {
        "nebraska": 3 / 8,
        "iowa_west": 2 / 8,
        "minnesota": 1 / 8,
        "indiana": 1 / 8,
        "illinois": 1 / 8,
    }
    for idx, ts in enumerate(quarters):
        qd = ts.date()
        ethanol = 1.46 + (0.016 * idx)
        futures = 4.05 + (0.03 * idx)
        gas = 3.10 + (0.02 * ((idx % 4) - 1))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="ethanol_price", series_key="ethanol_nebraska", instrument="Ethanol", price_value=ethanol, source_type="nwer_pdf"))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="corn_futures", series_key="cbot_corn_usd_per_bu", instrument="Corn futures", price_value=futures, source_type="nwer_pdf"))
        rows.append(_parsed_row(aggregation_level="quarter_avg", observation_date=qd.isoformat(), publication_date=qd.isoformat(), quarter=qd.isoformat(), market_family="natural_gas_price", series_key="nymex_gas", instrument="Natural gas", price_value=gas, source_type="nwer_pdf", unit="$/MMBtu"))
        weighted_basis_official = 0.0
        weighted_basis_bid_adjusted = 0.0
        for region, base in region_base.items():
            basis_val = base + (0.015 * np.sin(idx / 2.0))
            weighted_basis_official += plant_count_weights[region] * basis_val
            weighted_basis_bid_adjusted += plant_count_weights[region] * (basis_val + region_bid_offsets[region])
            rows.append(
                _parsed_row(
                    aggregation_level="quarter_avg",
                    observation_date=qd.isoformat(),
                    publication_date=qd.isoformat(),
                    quarter=qd.isoformat(),
                    market_family="corn_basis",
                    series_key=f"corn_basis_{region}",
                    instrument="Corn basis",
                    location=region.replace("_", " ").title(),
                    region=region,
                    price_value=basis_val,
                    unit="$/bushel",
                    source_type="ams_3617_pdf",
                )
            )
        baseline = ethanol - (futures / 2.9) - (0.028 * gas)
        reported[qd] = baseline - (weighted_basis_bid_adjusted / 2.9)
        denominator_policy[qd] = "ethanol gallons sold"

    bids_snapshot = {
        "status": "ok",
        "source_kind": "fixture",
        "source_url": "fixture://gpre-bids",
        "nearest_rows": [
            {"location": "Central City", "region": "nebraska", "basis_usd_per_bu": region_base["nebraska"] + region_bid_offsets["nebraska"]},
            {"location": "Wood River", "region": "nebraska", "basis_usd_per_bu": region_base["nebraska"] + region_bid_offsets["nebraska"]},
            {"location": "York", "region": "nebraska", "basis_usd_per_bu": region_base["nebraska"] + region_bid_offsets["nebraska"]},
            {"location": "Shenandoah", "region": "iowa_west", "basis_usd_per_bu": region_base["iowa_west"] + region_bid_offsets["iowa_west"]},
            {"location": "Superior", "region": "iowa_west", "basis_usd_per_bu": region_base["iowa_west"] + region_bid_offsets["iowa_west"]},
            {"location": "Otter Tail", "region": "minnesota", "basis_usd_per_bu": region_base["minnesota"] + region_bid_offsets["minnesota"]},
            {"location": "Mount Vernon", "region": "indiana", "basis_usd_per_bu": region_base["indiana"] + region_bid_offsets["indiana"]},
            {"location": "Madison", "region": "illinois", "basis_usd_per_bu": region_base["illinois"] + region_bid_offsets["illinois"]},
        ],
    }

    result = build_gpre_basis_proxy_model(
        rows,
        ticker_root=None,
        reported_margin_by_quarter=reported,
        denominator_policy_by_quarter=denominator_policy,
        as_of_date=date(2026, 4, 2),
        bids_snapshot=bids_snapshot,
    )

    quarterly_df = result["quarterly_df"].copy()
    metrics_df = result["metrics_df"].copy()
    offsets_df = result["bid_adjusted_offsets_df"].copy()

    assert not offsets_df.empty
    assert {"region", "gpre_bid_basis_cents_per_bu", "ams_reference_basis_cents_per_bu", "offset_cents_per_bu"} <= set(offsets_df.columns)
    assert pd.notna(pd.to_numeric(quarterly_df["basis_adjusted_bid_adjusted_offset_usd_per_gal"], errors="coerce")).all()
    official_test = metrics_df[(metrics_df["model_key"].astype(str) == "bridge_current75_prev25") & (metrics_df["split"].astype(str) == "test")].copy()
    bid_adjusted_test = metrics_df[(metrics_df["model_key"].astype(str) == "bid_adjusted_offset") & (metrics_df["split"].astype(str) == "test")].copy()
    assert not official_test.empty and not bid_adjusted_test.empty
    assert float(pd.to_numeric(bid_adjusted_test.iloc[0]["mae"], errors="coerce")) < float(pd.to_numeric(official_test.iloc[0]["mae"], errors="coerce"))
    assert "bid-offset" in str(result["summary_markdown"] or "").lower()


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

    assert current["status"] == "ok_current"
    assert current["process_status"] == "ok"
    assert current["display_quarter"] == date(2026, 3, 31)
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


def test_prior_quarter_simple_crush_snapshot_stays_in_closed_quarter_frame() -> None:
    rows = [
        {"observation_date": date(2026, 1, 5), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.00, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 6), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.10, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 7), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.20, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 8), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.10, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 9), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.00, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 1, 9), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.60, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 16), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.65, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 9), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.40, "contract_tenor": "front", "source_type": "nwer_pdf"},
        {"observation_date": date(2026, 1, 16), "quarter": date(2026, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.10, "contract_tenor": "front", "source_type": "nwer_pdf"},
    ]

    prior = build_prior_quarter_simple_crush_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=27000.0,
        as_of_date=date(2026, 4, 1),
    )
    current = build_current_qtd_simple_crush_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=27000.0,
        as_of_date=date(2026, 4, 1),
    )

    assert prior["status"] == "ok_prior"
    assert prior["process_status"] == "ok"
    assert prior["display_quarter"] == date(2026, 3, 31)
    assert prior["calendar_quarter"] == date(2026, 6, 30)
    assert prior["weeks_included"] == 1
    assert prior["current_market"]["ethanol_price"] == pytest.approx(1.625, abs=0.0001)
    assert current["status"] == "no_data"
    assert current["display_quarter"] == date(2026, 6, 30)
    assert current["current_market"] == {}


def test_qtd_simple_crush_snapshot_uses_partial_current_quarter_observations_but_leaves_process_blank() -> None:
    rows = [
        {"observation_date": date(2026, 4, 3), "quarter": date(2026, 6, 30), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.05, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 4, 4), "quarter": date(2026, 6, 30), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.15, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2026, 4, 3), "quarter": date(2026, 6, 30), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.62, "contract_tenor": "", "source_type": "nwer_pdf"},
    ]

    current = build_current_qtd_simple_crush_snapshot(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=27000.0,
        as_of_date=date(2026, 4, 5),
    )

    assert current["status"] == "ok_current"
    assert current["process_status"] == "no_data"
    assert current["display_quarter"] == date(2026, 6, 30)
    assert current["current_market"]["corn_price"] == pytest.approx(4.10, abs=0.0001)
    assert current["current_market"]["ethanol_price"] == pytest.approx(1.62, abs=0.0001)
    assert "natural_gas_price" not in current["current_market"] or current["current_market"]["natural_gas_price"] is None
    assert current["current_process"] == {}
    assert current["weeks_included"] == 0


def test_gpre_current_qtd_snapshot_history_dedupes_identical_reruns_and_appends_changed_inputs() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_current_qtd_history_")
    try:
        bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 6, 10),
            current_ethanol_price=2.38,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
        )

        current_row = dict(bundle.get("current_snapshot") or {})
        required_fields = {
            "captured_at",
            "as_of_date",
            "quarter_label",
            "quarter_start",
            "quarter_end",
            "input_fingerprint",
            "current_qtd_all_in_usd_per_gal",
            "current_qtd_official_simple_usd_per_gal",
            "current_qtd_coproduct_credit_usd_per_gal",
            "ethanol_component_usd_per_gal",
            "flat_corn_component_usd_per_gal",
            "corn_basis_component_usd_per_gal",
            "gas_component_usd_per_gal",
            "coproduct_component_usd_per_gal",
            "quarter_open_all_in_usd_per_gal",
            "quarter_open_official_simple_usd_per_gal",
            "corn_basis_source_kind",
            "corn_basis_source_label",
            "corn_basis_snapshot_date",
            "corn_basis_selection_rule",
        }
        assert required_fields <= set(current_row.keys())
        assert pd.notna(pd.to_numeric(current_row.get("current_qtd_official_simple_usd_per_gal"), errors="coerce"))
        assert pd.notna(pd.to_numeric(current_row.get("quarter_open_official_simple_usd_per_gal"), errors="coerce"))
        assert market_service.persist_gpre_current_qtd_snapshot_history(
            ticker_root,
            bundle["pending_history_write"],
        )

        history_df = market_service.load_gpre_current_qtd_snapshot_history(ticker_root)
        assert len(history_df.index) == 1
        assert bool(history_df.iloc[0]["is_weekly_checkpoint"])
        assert str(history_df.iloc[0]["input_fingerprint"] or "").strip()

        assert market_service.persist_gpre_current_qtd_snapshot_history(
            ticker_root,
            bundle["pending_history_write"],
        )
        rerun_history_df = market_service.load_gpre_current_qtd_snapshot_history(ticker_root)
        assert len(rerun_history_df.index) == 1

        changed_bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 6, 10),
            current_ethanol_price=2.44,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
        )
        assert market_service.persist_gpre_current_qtd_snapshot_history(
            ticker_root,
            changed_bundle["pending_history_write"],
        )
        changed_history_df = market_service.load_gpre_current_qtd_snapshot_history(ticker_root)
        assert len(changed_history_df.index) == 2
        assert (
            str(changed_history_df.iloc[-1]["input_fingerprint"] or "").strip()
            != str(changed_history_df.iloc[0]["input_fingerprint"] or "").strip()
        )
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_weighted_coproduct_record_exposes_corn_oil_contribution_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    quarter_end = date(2026, 3, 31)
    monkeypatch.setattr(
        market_service,
        "_gpre_official_market_weights_for_quarter",
        lambda quarter_end, ticker_root=None, plant_capacity_history=None: {"nebraska": 1.0},
    )
    rows = [
        _parsed_row(
            observation_date=quarter_end.isoformat(),
            quarter=quarter_end,
            aggregation_level="quarter_avg",
            market_family="renewable_corn_oil_price",
            series_key="corn_oil_nebraska",
            instrument="Renewable corn oil",
            location="Nebraska",
            region="nebraska",
            price_value=0.50,
            unit="$/lb",
            source_type="nwer_pdf",
        ),
        _parsed_row(
            observation_date=quarter_end.isoformat(),
            quarter=quarter_end,
            aggregation_level="quarter_avg",
            market_family="distillers_grains_price",
            series_key="ddgs_10_nebraska",
            instrument="Distillers grains",
            location="Nebraska",
            region="nebraska",
            price_value=0.04,
            unit="$/lb",
            source_type="nwer_pdf",
        ),
    ]
    profile = get_company_profile("GPRE")
    coeff_map = {
        str(getattr(coef, "key", "") or "").strip(): float(pd.to_numeric(getattr(coef, "default_value", None), errors="coerce"))
        for coef in tuple(getattr(profile, "economics_overlay_coefficients", ()) or ())
        if str(getattr(coef, "key", "") or "").strip()
    }
    ethanol_yield = float(coeff_map["ethanol_yield"])
    corn_oil_yield = float(coeff_map["renewable_corn_oil_yield"])
    expected_per_bushel = corn_oil_yield * 0.50
    expected_per_gal = expected_per_bushel / ethanol_yield
    expected_usd_m_proxy = expected_per_gal * 100.0

    record = market_service._gpre_build_weighted_coproduct_record(
        rows,
        quarter_end,
        ticker_root=None,
        plant_capacity_history=None,
        reported_gallons_produced_by_quarter={quarter_end: 100_000_000.0},
    )

    assert float(pd.to_numeric(record.get("renewable_corn_oil_price"), errors="coerce")) == pytest.approx(0.50, abs=1e-9)
    assert float(pd.to_numeric(record.get("renewable_corn_oil_contribution_per_bushel"), errors="coerce")) == pytest.approx(expected_per_bushel, abs=1e-9)
    assert float(pd.to_numeric(record.get("renewable_corn_oil_contribution_per_gal"), errors="coerce")) == pytest.approx(expected_per_gal, abs=1e-9)
    assert float(pd.to_numeric(record.get("renewable_corn_oil_contribution_usd_m_proxy"), errors="coerce")) == pytest.approx(expected_usd_m_proxy, abs=1e-9)


def test_weighted_coproduct_context_exposes_capacity_weighted_plant_rows_for_ddgs_and_corn_oil(monkeypatch: pytest.MonkeyPatch) -> None:
    quarter_end = date(2026, 3, 31)
    monkeypatch.setattr(
        market_service,
        "_gpre_active_plants_for_quarter",
        lambda quarter_end, plant_capacity_history=None, ticker_root=None: [
            {"location": "Plant A", "region": "nebraska", "capacity_mmgy": 120.0},
            {"location": "Plant B", "region": "nebraska", "capacity_mmgy": 80.0},
            {"location": "Plant C", "region": "illinois", "capacity_mmgy": 50.0},
        ],
    )
    rows = [
        _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end, market_family="renewable_corn_oil_price", series_key="corn_oil_nebraska", instrument="Renewable corn oil", region="nebraska", price_value=0.50, unit="$/lb", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end, market_family="renewable_corn_oil_price", series_key="corn_oil_illinois", instrument="Renewable corn oil", region="illinois", price_value=0.20, unit="$/lb", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end, market_family="distillers_grains_price", series_key="ddgs_10_nebraska", instrument="Distillers grains", region="nebraska", price_value=80.0, unit="$/ton", source_type="nwer_pdf"),
        _parsed_row(aggregation_level="quarter_avg", observation_date=quarter_end.isoformat(), publication_date=quarter_end.isoformat(), quarter=quarter_end, market_family="distillers_grains_price", series_key="ddgs_10_illinois", instrument="Distillers grains", region="illinois", price_value=40.0, unit="$/ton", source_type="nwer_pdf"),
    ]

    record = market_service.weighted_coproduct_context(
        rows,
        quarter_end,
        ticker_root=None,
        plant_capacity_history=None,
        reported_gallons_produced_by_quarter={quarter_end: 100_000_000.0},
        mode="quarter_avg",
    )

    assert float(pd.to_numeric(record.get("renewable_corn_oil_price"), errors="coerce")) == pytest.approx(0.44, abs=1e-12)
    assert float(pd.to_numeric(record.get("distillers_grains_price"), errors="coerce")) == pytest.approx(0.036, abs=1e-12)
    corn_oil_rows = list(record.get("renewable_corn_oil_plant_rows") or [])
    ddgs_rows = list(record.get("distillers_grains_plant_rows") or [])
    assert [str(rec.get("location") or "") for rec in corn_oil_rows] == ["Plant A", "Plant B", "Plant C"]
    assert [str(rec.get("location") or "") for rec in ddgs_rows] == ["Plant A", "Plant B", "Plant C"]
    assert [float(pd.to_numeric(rec.get("weight"), errors="coerce")) for rec in corn_oil_rows] == pytest.approx([0.48, 0.32, 0.20], abs=1e-12)
    assert [str(rec.get("series_key") or "") for rec in ddgs_rows] == ["ddgs_10_nebraska", "ddgs_10_nebraska", "ddgs_10_illinois"]


def test_next_quarter_coproduct_frame_freezes_to_quarter_open_even_when_current_qtd_moves(monkeypatch: pytest.MonkeyPatch) -> None:
    q1 = date(2026, 3, 31)
    q2 = date(2026, 6, 30)
    q3 = date(2026, 9, 30)
    monkeypatch.setattr(
        market_service,
        "_gpre_active_plants_for_quarter",
        lambda quarter_end, plant_capacity_history=None, ticker_root=None: [
            {"location": "Plant A", "region": "nebraska", "capacity_mmgy": 100.0},
        ],
    )

    def _coproduct_rows(*, q1_ddgs: float, q1_oil: float, q2_open_ddgs: float, q2_open_oil: float, q2_live_ddgs: float, q2_live_oil: float) -> list[dict[str, object]]:
        return [
                _parsed_row(aggregation_level="quarter_avg", observation_date=q1.isoformat(), publication_date=q1.isoformat(), quarter=q1, market_family="renewable_corn_oil_price", series_key="corn_oil_nebraska", instrument="Renewable corn oil", region="nebraska", price_value=q1_oil, unit="$/lb", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=q1.isoformat(), publication_date=q1.isoformat(), quarter=q1, market_family="distillers_grains_price", series_key="ddgs_10_nebraska", instrument="Distillers grains", region="nebraska", price_value=q1_ddgs, unit="$/ton", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date="2026-04-01", publication_date="2026-04-01", quarter=q2, market_family="renewable_corn_oil_price", series_key="corn_oil_nebraska", instrument="Renewable corn oil", region="nebraska", price_value=q2_open_oil, unit="$/lb", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="observation", observation_date="2026-04-01", publication_date="2026-04-01", quarter=q2, market_family="distillers_grains_price", series_key="ddgs_10_nebraska", instrument="Distillers grains", region="nebraska", price_value=q2_open_ddgs, unit="$/ton", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=q2.isoformat(), publication_date=q2.isoformat(), quarter=q2, market_family="renewable_corn_oil_price", series_key="corn_oil_nebraska", instrument="Renewable corn oil", region="nebraska", price_value=q2_live_oil, unit="$/lb", source_type="nwer_pdf"),
                _parsed_row(aggregation_level="quarter_avg", observation_date=q2.isoformat(), publication_date=q2.isoformat(), quarter=q2, market_family="distillers_grains_price", series_key="ddgs_10_nebraska", instrument="Distillers grains", region="nebraska", price_value=q2_live_ddgs, unit="$/ton", source_type="nwer_pdf"),
        ]

    prior_record = market_service.weighted_coproduct_context(
        _coproduct_rows(q1_ddgs=60.0, q1_oil=0.20, q2_open_ddgs=80.0, q2_open_oil=0.25, q2_live_ddgs=90.0, q2_live_oil=0.30),
        q1,
        ticker_root=None,
        plant_capacity_history=None,
        reported_gallons_produced_by_quarter={q1: 100_000_000.0},
        mode="quarter_avg",
    )
    quarter_open_record = market_service.weighted_coproduct_context(
        _coproduct_rows(q1_ddgs=60.0, q1_oil=0.20, q2_open_ddgs=80.0, q2_open_oil=0.25, q2_live_ddgs=90.0, q2_live_oil=0.30),
        q2,
        ticker_root=None,
        plant_capacity_history=None,
        reported_gallons_produced_by_quarter={q2: 100_000_000.0},
        mode="quarter_open",
    )
    current_low = market_service.weighted_coproduct_context(
        _coproduct_rows(q1_ddgs=60.0, q1_oil=0.20, q2_open_ddgs=80.0, q2_open_oil=0.25, q2_live_ddgs=90.0, q2_live_oil=0.30),
        q2,
        ticker_root=None,
        plant_capacity_history=None,
        reported_gallons_produced_by_quarter={q2: 100_000_000.0},
        mode="quarter_avg",
    )
    current_high = market_service.weighted_coproduct_context(
        _coproduct_rows(q1_ddgs=60.0, q1_oil=0.20, q2_open_ddgs=80.0, q2_open_oil=0.25, q2_live_ddgs=140.0, q2_live_oil=0.60),
        q2,
        ticker_root=None,
        plant_capacity_history=None,
        reported_gallons_produced_by_quarter={q2: 100_000_000.0},
        mode="quarter_avg",
    )

    next_from_low = market_service._gpre_coproduct_frame_record(
        "next_quarter_thesis",
        target_quarter_end=q3,
        base_record=quarter_open_record,
        fallback_record=prior_record,
    )
    next_from_high = market_service._gpre_coproduct_frame_record(
        "next_quarter_thesis",
        target_quarter_end=q3,
        base_record=quarter_open_record,
        fallback_record=prior_record,
    )

    assert float(pd.to_numeric(current_high.get("approximate_coproduct_credit_per_gal"), errors="coerce")) > float(
        pd.to_numeric(current_low.get("approximate_coproduct_credit_per_gal"), errors="coerce")
    )
    assert float(pd.to_numeric(next_from_low.get("approximate_coproduct_credit_per_gal"), errors="coerce")) == pytest.approx(
        float(pd.to_numeric(next_from_high.get("approximate_coproduct_credit_per_gal"), errors="coerce")),
        abs=1e-12,
    )
    assert float(pd.to_numeric(next_from_low.get("approximate_coproduct_credit_per_gal"), errors="coerce")) == pytest.approx(
        float(pd.to_numeric(quarter_open_record.get("approximate_coproduct_credit_per_gal"), errors="coerce")),
        abs=1e-12,
    )
    assert "freeze the resolved quarter-open weighted coproduct frame" in str(next_from_low.get("rule") or "").lower()


def test_gpre_current_qtd_weekly_checkpoints_and_same_quarter_lookbacks_use_retained_history() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_current_qtd_lookbacks_")
    try:
        historical_specs = [
            (date(2026, 3, 28), date(2026, 3, 31), 2.02, 4.03, 0.22, 3.20, 0.39),
            (date(2026, 4, 10), date(2026, 6, 30), 2.08, 4.04, 0.24, 3.22, 0.40),
            (date(2026, 4, 12), date(2026, 6, 30), 2.09, 4.04, 0.24, 3.22, 0.40),
            (date(2026, 5, 8), date(2026, 6, 30), 2.18, 4.08, 0.26, 3.30, 0.42),
            (date(2026, 5, 10), date(2026, 6, 30), 2.20, 4.08, 0.26, 3.30, 0.42),
            (date(2026, 6, 2), date(2026, 6, 30), 2.30, 4.10, 0.27, 3.36, 0.43),
        ]
        for as_of_date, quarter_end, ethanol_price, cbot_corn_price, corn_basis, gas_price, coproduct_credit in historical_specs:
            bundle = _build_gpre_qtd_tracking_bundle(
                ticker_root,
                current_as_of=as_of_date,
                current_ethanol_price=ethanol_price,
                current_cbot_corn_price=cbot_corn_price,
                current_corn_basis_usd_per_bu=corn_basis,
                current_natural_gas_price=gas_price,
                current_coproduct_credit_usd_per_gal=coproduct_credit,
                quarter_open_as_of=date(2026, 3, 31) if quarter_end == date(2026, 6, 30) else date(2025, 12, 31),
                quarter_open_ethanol_price=2.14,
                quarter_open_cbot_corn_price=4.02,
                quarter_open_corn_basis_usd_per_bu=0.23,
                quarter_open_natural_gas_price=3.25,
                quarter_open_coproduct_credit_usd_per_gal=0.40,
                quarter_end=quarter_end,
            )
            assert market_service.persist_gpre_current_qtd_snapshot_history(
                ticker_root,
                bundle["pending_history_write"],
            )

        history_df = market_service.load_gpre_current_qtd_snapshot_history(ticker_root)
        q2_history = history_df[history_df["quarter_end"] == date(2026, 6, 30)].copy()
        week_15_rows = q2_history[q2_history["checkpoint_iso_week"] == "2026-W15"].copy()
        assert len(week_15_rows.index) == 2
        assert week_15_rows["is_weekly_checkpoint"].astype(bool).tolist() == [False, True]

        current_bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 6, 10),
            current_ethanol_price=2.38,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
        )
        refs = dict(current_bundle.get("reference_comparisons") or {})
        assert refs["1w"]["reference_date"] == date(2026, 6, 2)
        assert refs["4w"]["reference_date"] == date(2026, 5, 10)
        assert refs["8w"]["reference_date"] == date(2026, 4, 12)
        assert refs["8w"]["reference_date"] != date(2026, 3, 28)
        expected_1w_value = float(pd.to_numeric(history_df.loc[history_df["as_of_date"].eq(date(2026, 6, 2)), "current_qtd_official_simple_usd_per_gal"], errors="coerce").iloc[-1])
        expected_4w_value = float(pd.to_numeric(history_df.loc[history_df["as_of_date"].eq(date(2026, 5, 10)), "current_qtd_official_simple_usd_per_gal"], errors="coerce").iloc[-1])
        expected_8w_value = float(pd.to_numeric(history_df.loc[history_df["as_of_date"].eq(date(2026, 4, 12)), "current_qtd_official_simple_usd_per_gal"], errors="coerce").iloc[-1])
        assert refs["quarter_open"]["reference_value_usd_per_gal"] == pytest.approx(
            float(pd.to_numeric((current_bundle.get("current_snapshot") or {}).get("quarter_open_official_simple_usd_per_gal"), errors="coerce")),
            abs=1e-9,
        )
        assert refs["1w"]["reference_value_usd_per_gal"] == pytest.approx(expected_1w_value, abs=1e-9)
        assert refs["4w"]["reference_value_usd_per_gal"] == pytest.approx(expected_4w_value, abs=1e-9)
        assert refs["8w"]["reference_value_usd_per_gal"] == pytest.approx(expected_8w_value, abs=1e-9)
        assert refs["1w"]["status"] == "ok"
        assert refs["4w"]["status"] == "ok"
        assert refs["8w"]["status"] == "ok"
        assert refs["quarter_open"]["note"] == ""
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_current_qtd_same_quarter_only_lookbacks_stay_blank_without_retained_history() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_current_qtd_same_quarter_only_")
    try:
        previous_quarter_bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 3, 28),
            current_ethanol_price=2.02,
            current_cbot_corn_price=4.03,
            current_corn_basis_usd_per_bu=0.22,
            current_natural_gas_price=3.20,
            current_coproduct_credit_usd_per_gal=0.39,
            quarter_open_as_of=date(2025, 12, 31),
            quarter_open_ethanol_price=1.98,
            quarter_open_cbot_corn_price=3.96,
            quarter_open_corn_basis_usd_per_bu=0.20,
            quarter_open_natural_gas_price=3.18,
            quarter_open_coproduct_credit_usd_per_gal=0.38,
            quarter_end=date(2026, 3, 31),
        )
        assert market_service.persist_gpre_current_qtd_snapshot_history(
            ticker_root,
            previous_quarter_bundle["pending_history_write"],
        )

        current_bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 6, 10),
            current_ethanol_price=2.38,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
        )
        refs = dict(current_bundle.get("reference_comparisons") or {})
        for ref_key in ("1w", "4w", "8w"):
            assert refs[ref_key]["status"] == "insufficient_history"
            assert refs[ref_key]["reference_date"] is None
            assert refs[ref_key]["reference_value_usd_per_gal"] is None
            assert refs[ref_key]["delta_usd_per_gal"] is None
            assert refs[ref_key]["note"] == "insufficient history"
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_current_qtd_same_quarter_backfill_populates_1w_reference_and_drivers() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_current_qtd_backfill_1w_")
    try:
        bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 4, 10),
            current_ethanol_price=2.38,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
            rows=_gpre_qtd_backfill_rows_fixture(),
        )
        refs = dict(bundle.get("reference_comparisons") or {})
        assert refs["1w"]["status"] == "ok"
        assert refs["1w"]["reference_date"] == date(2026, 4, 2)
        assert refs["1w"]["history_source_kind"] == "backfilled_weekly_checkpoint"
        rows_by_driver = {
            str(rec.get("driver") or ""): dict(rec)
            for rec in list(bundle.get("driver_attribution_rows") or [])
            if isinstance(rec, dict)
        }
        assert set(rows_by_driver.keys()) == {"Ethanol", "Flat corn", "Corn basis", "Gas"}
        for driver in ("Ethanol", "Flat corn", "Corn basis", "Gas"):
            assert pd.notna(pd.to_numeric(rows_by_driver[driver]["1w"], errors="coerce"))

        assert market_service.persist_gpre_current_qtd_snapshot_history(
            ticker_root,
            bundle["pending_history_write"],
        )
        history_df = market_service.load_gpre_current_qtd_snapshot_history(ticker_root)
        backfilled_row = history_df.loc[
            history_df["history_source_kind"].eq("backfilled_weekly_checkpoint")
            & history_df["as_of_date"].eq(date(2026, 4, 2))
        ]
        assert not backfilled_row.empty
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_current_qtd_backfill_skips_incomplete_weeks() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_current_qtd_backfill_incomplete_")
    try:
        incomplete_rows = [
            dict(rec)
            for rec in _gpre_qtd_backfill_rows_fixture()
            if not (
                rec.get("series_key") == "nymex_gas"
                and rec.get("observation_date") == "2026-04-02"
            )
        ]
        bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 4, 10),
            current_ethanol_price=2.38,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
            rows=incomplete_rows,
        )
        refs = dict(bundle.get("reference_comparisons") or {})
        assert refs["1w"]["status"] == "insufficient_history"
        assert refs["1w"]["reference_date"] is None
        assert refs["1w"]["reference_value_usd_per_gal"] is None
        assert refs["1w"]["delta_usd_per_gal"] is None
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_gpre_current_qtd_driver_attribution_sums_to_displayed_delta() -> None:
    ticker_root = _local_test_dir(".pytest_tmp_gpre_current_qtd_attribution_")
    try:
        bundle = _build_gpre_qtd_tracking_bundle(
            ticker_root,
            current_as_of=date(2026, 6, 10),
            current_ethanol_price=2.38,
            current_cbot_corn_price=4.12,
            current_corn_basis_usd_per_bu=0.28,
            current_natural_gas_price=3.42,
            current_coproduct_credit_usd_per_gal=0.44,
            quarter_open_as_of=date(2026, 3, 31),
            quarter_open_ethanol_price=2.14,
            quarter_open_cbot_corn_price=4.02,
            quarter_open_corn_basis_usd_per_bu=0.23,
            quarter_open_natural_gas_price=3.25,
            quarter_open_coproduct_credit_usd_per_gal=0.40,
        )
        rows_by_driver = {
            str(rec.get("driver") or ""): dict(rec)
            for rec in list(bundle.get("driver_attribution_rows") or [])
            if isinstance(rec, dict)
        }
        refs = dict(bundle.get("reference_comparisons") or {})
        component_total = sum(
            float(pd.to_numeric(rows_by_driver[driver]["quarter_open"], errors="coerce"))
            for driver in ("Ethanol", "Flat corn", "Corn basis", "Gas")
        )
        total_delta = float(pd.to_numeric(refs["quarter_open"]["delta_usd_per_gal"], errors="coerce"))
        assert set(rows_by_driver.keys()) == {"Ethanol", "Flat corn", "Corn basis", "Gas"}
        assert component_total == pytest.approx(total_delta, abs=1e-9)
    finally:
        shutil.rmtree(ticker_root, ignore_errors=True)


def test_simple_crush_history_series_returns_trailing_weekly_per_gallon_values() -> None:
    rows = [
        {"observation_date": date(2024, 3, 29), "quarter": date(2024, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.00, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2024, 3, 28), "quarter": date(2024, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.10, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2024, 4, 5), "quarter": date(2024, 6, 30), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.20, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2024, 4, 4), "quarter": date(2024, 6, 30), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.30, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2024, 3, 29), "quarter": date(2024, 3, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.60, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2024, 4, 5), "quarter": date(2024, 6, 30), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.70, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2024, 3, 29), "quarter": date(2024, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.20, "contract_tenor": "front", "source_type": "nwer_pdf"},
        {"observation_date": date(2024, 4, 5), "quarter": date(2024, 6, 30), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.30, "contract_tenor": "front", "source_type": "nwer_pdf"},
    ]

    series = build_simple_crush_history_series(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=27000.0,
        as_of_date=date(2024, 4, 6),
        lookback_weeks=104,
    )

    assert [rec["week_end"] for rec in series] == [date(2024, 3, 29), date(2024, 4, 5)]
    assert series[0]["simple_crush_per_gal"] == pytest.approx((((2.9 * 1.60) - 4.05 - ((27000.0 / 1_000_000.0) * 2.9 * 3.20)) / 2.9), abs=0.0001)
    assert series[1]["simple_crush_per_gal"] == pytest.approx((((2.9 * 1.70) - 4.25 - ((27000.0 / 1_000_000.0) * 2.9 * 3.30)) / 2.9), abs=0.0001)


def test_simple_crush_history_series_respects_explicit_start_date_without_two_year_cap() -> None:
    rows = [
        {"observation_date": date(2022, 12, 30), "quarter": date(2022, 12, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.00, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2022, 12, 30), "quarter": date(2022, 12, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.55, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2022, 12, 30), "quarter": date(2022, 12, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.10, "contract_tenor": "front", "source_type": "nwer_pdf"},
        {"observation_date": date(2023, 1, 6), "quarter": date(2023, 3, 31), "aggregation_level": "observation", "series_key": "corn_nebraska", "price_value": 4.05, "contract_tenor": "", "source_type": "ams_3617_pdf"},
        {"observation_date": date(2023, 1, 6), "quarter": date(2023, 3, 31), "aggregation_level": "observation", "series_key": "ethanol_nebraska", "price_value": 1.58, "contract_tenor": "", "source_type": "nwer_pdf"},
        {"observation_date": date(2023, 1, 6), "quarter": date(2023, 3, 31), "aggregation_level": "observation", "series_key": "nymex_gas", "price_value": 3.20, "contract_tenor": "front", "source_type": "nwer_pdf"},
    ]

    series = build_simple_crush_history_series(
        rows,
        ethanol_yield=2.9,
        natural_gas_usage=27000.0,
        as_of_date=date(2023, 1, 10),
        lookback_weeks=None,
        start_date=date(2023, 1, 1),
    )

    assert [rec["week_end"] for rec in series] == [date(2023, 1, 6)]
