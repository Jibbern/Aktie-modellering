"""Market-data sync, parse, and export service for workbook overlays.

The market-data cache has three important layers:
- `raw/`: source files discovered or restored from disk
- `parsed/`: source-specific observation and quarterly parquet frames
- `parsed/exports/`: ticker-shaped export rows consumed by workbook logic
"""
from __future__ import annotations

import json
import calendar
import html
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..debt_parser import read_html_tables_any
from .aggregations import aggregate_quarterly, parse_quarter_like, quarter_end_from_date
from .cache import (
    batch_fingerprint,
    ensure_market_cache_dirs,
    export_rows_path,
    file_fingerprint,
    load_manifest,
    normalize_manifest_list,
    parsed_manifest_path,
    parsed_obs_path,
    parsed_quarter_path,
    raw_manifest_path,
    resolve_market_cache_root,
    save_manifest,
)
from .contracts import (
    MARKET_ROWS_DF_NORMALIZED_ATTR,
    normalize_market_rows_df,
    require_market_columns,
)
from .mappings import series_meta_from_key
from .models import SourceFrameSpec, SyncSummary
from .providers import PROVIDERS
from .providers.cme_ethanol_platts import load_local_manual_ethanol_quarter_open_snapshot_rows


PARSED_SCHEMA_COLUMNS = [
    "observation_date",
    "quarter",
    "aggregation_level",
    "publication_date",
    "source",
    "report_type",
    "source_type",
    "market_family",
    "series_key",
    "instrument",
    "location",
    "region",
    "tenor",
    "price_value",
    "unit",
    "quality",
    "source_file",
    "parsed_note",
    "origin",
    "_priority",
    "_obs_count",
]


_GPRE_CORN_BIDS_ENTRY_URL = "https://gpreinc.com/corn-bids/"
_GPRE_CORN_BIDS_DIRECT_URLS: Tuple[str, ...] = (
    "https://grain.gpreinc.com/index.cfm",
    "https://grain.gpreinc.com/index.cfm?show=0&mid=1",
)
_GPRE_CORN_BIDS_DIRNAME = "corn_bids"
_GPRE_CORN_BIDS_HTML_FILENAME = "grain_gpre_home.html"
_GPRE_CORN_BIDS_CSV_FILENAME = "gpre_corn_bids_snapshot.csv"


def _normalize_raw_manifest_entry(entry: Dict[str, Any], *, source: str, provider: Any) -> Optional[Dict[str, Any]]:
    try:
        local_path = Path(str(entry.get("local_path") or "")).expanduser()
    except Exception:
        return None
    if not local_path.exists() or not local_path.is_file():
        return None
    try:
        resolved = local_path.resolve()
    except Exception:
        resolved = local_path
    try:
        st = resolved.stat()
    except Exception:
        return None
    try:
        report_ts = provider._date_from_name(resolved)
    except Exception:
        report_ts = None
    report_txt = str(entry.get("report_date") or "").strip()
    publication_txt = str(entry.get("publication_date") or "").strip()
    if isinstance(report_ts, pd.Timestamp) and not pd.isna(report_ts):
        inferred = report_ts.date().isoformat()
        if not report_txt:
            report_txt = inferred
        if not publication_txt:
            publication_txt = inferred
    asset_type = str(entry.get("asset_type") or "").strip() or str(provider._asset_type_for_name(resolved.name) or "").strip()
    checksum = str(entry.get("checksum") or "").strip() or file_fingerprint(resolved)
    return {
        "source": str(entry.get("source") or source),
        "source_id": str(entry.get("source_id") or resolved.stem),
        "report_date": report_txt,
        "publication_date": publication_txt,
        "local_path": str(resolved),
        "size": int(st.st_size),
        "checksum": checksum,
        "download_status": str(entry.get("download_status") or "cached"),
        "asset_type": asset_type,
    }


def _raw_entries_from_disk(cache_root: Path, source: str, provider: Any) -> List[Dict[str, Any]]:
    # Disk backfill keeps restored raw files usable even when `raw_manifest.json` is
    # empty or stale. The files on disk are allowed to repair the manifest state.
    raw_root = cache_root / "raw" / str(source)
    if not raw_root.exists() or not raw_root.is_dir():
        return []
    entries: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(raw_root.rglob("*")):
        if not path.is_file():
            continue
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        path_key = str(resolved)
        if path_key in seen:
            continue
        seen.add(path_key)
        entry = _normalize_raw_manifest_entry(
            {
                "source": source,
                "source_id": resolved.stem,
                "report_date": "",
                "publication_date": "",
                "local_path": str(resolved),
                "asset_type": str(provider._asset_type_for_name(resolved.name) or "").strip(),
                "download_status": "cached",
            },
            source=source,
            provider=provider,
        )
        if entry is not None:
            entries.append(entry)
    entries.sort(key=lambda row: (str(row.get("report_date") or ""), str(row.get("local_path") or "")))
    return entries


def _merge_raw_entries(
    existing_entries: List[Dict[str, Any]],
    disk_entries: List[Dict[str, Any]],
    *,
    source: str,
    provider: Any,
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for entry in existing_entries:
        normalized = _normalize_raw_manifest_entry(entry, source=source, provider=provider)
        if normalized is None:
            continue
        merged[str(normalized.get("local_path") or "")] = normalized
    for entry in disk_entries:
        normalized = _normalize_raw_manifest_entry(entry, source=source, provider=provider)
        if normalized is None:
            continue
        merged[str(normalized.get("local_path") or "")] = normalized
    out = list(merged.values())
    out.sort(key=lambda row: (str(row.get("report_date") or ""), str(row.get("local_path") or "")))
    return out


def _ticker_root_from_cache_dir(cache_dir: Path, ticker: str) -> Path:
    ticker_u = str(ticker or "").strip().upper()
    croot = Path(cache_dir).expanduser().resolve()
    candidates: List[Path] = []
    if len(croot.parents) >= 2:
        candidates.append(croot.parents[1] / ticker_u)
    if len(croot.parents) >= 1:
        candidates.append(croot.parent / ticker_u)
    candidates.append(Path.cwd() / ticker_u)
    for cand in candidates:
        if cand.exists():
            return cand
    return candidates[0]


def _enabled_sources_for_profile(profile: Any) -> Tuple[str, ...]:
    enabled = tuple(str(x or "").strip() for x in (getattr(profile, "enabled_market_sources", ()) or ()) if str(x or "").strip())
    if enabled:
        return enabled
    return tuple(PROVIDERS.keys())


def _safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _first_existing_bootstrap_csv(ticker_root: Path, *relative_paths: str) -> Optional[Path]:
    # `data/` remains the original bootstrap home, while the USDA folders are the
    # user-facing place for manually dropped weekly/daily files. We keep the search
    # order stable so existing curated CSVs in `data/` continue to win if both exist.
    seen: set[str] = set()
    for rel in relative_paths:
        path = ticker_root / rel
        try:
            resolved = str(path.resolve())
        except Exception:
            resolved = str(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        if path.exists() and path.is_file():
            return path
    return None


def _bootstrap_specs_for_source(source: str, ticker_root: Path) -> List[Tuple[SourceFrameSpec, Path]]:
    # Bootstrap CSVs are an optional local override/source-adjacent layer. The normal
    # live path is provider raw -> parsed parquet -> exported parquet, but these CSVs
    # let us seed or patch weekly/daily series from curated local files when needed.
    specs: List[Tuple[SourceFrameSpec, Path]] = []
    if source == "nwer":
        weekly_path = _first_existing_bootstrap_csv(
            ticker_root,
            "data/nwer_weekly.csv",
            "USDA_bioenergy_reports/nwer_weekly.csv",
            "USDA_weekly_data/nwer_weekly.csv",
        )
        weekly = _safe_read_csv(weekly_path) if weekly_path is not None else None
        if weekly is not None:
            specs.append(
                (
                    SourceFrameSpec(
                        df=weekly,
                        date_col="week_end",
                        source_file_col="source_pdf",
                        source_type="nwer_weekly_bootstrap",
                        aggregation_level="observation",
                        priority=40,
                    ),
                    weekly_path,
                )
            )
        quarterly_path = _first_existing_bootstrap_csv(
            ticker_root,
            "data/nwer_quarterly.csv",
            "USDA_bioenergy_reports/nwer_quarterly.csv",
            "USDA_weekly_data/nwer_quarterly.csv",
        )
        quarterly = _safe_read_csv(quarterly_path) if quarterly_path is not None else None
        if quarterly is not None:
            specs.append(
                (
                    SourceFrameSpec(
                        df=quarterly,
                        date_col="quarter",
                        source_file_col="",
                        source_type="nwer_quarterly_bootstrap",
                        aggregation_level="quarter_avg",
                        priority=10,
                    ),
                    quarterly_path,
                )
            )
    elif source == "ams_3617":
        daily_path = _first_existing_bootstrap_csv(
            ticker_root,
            "data/ams_3617_daily_corn.csv",
            "USDA_daily_data/ams_3617_daily_corn.csv",
        )
        daily = _safe_read_csv(daily_path) if daily_path is not None else None
        if daily is not None:
            specs.append(
                (
                    SourceFrameSpec(
                        df=daily,
                        date_col="report_date",
                        source_file_col="source_pdf",
                        source_type="ams_3617_daily_corn_bootstrap",
                        aggregation_level="observation",
                        priority=40,
                    ),
                    daily_path,
                )
            )
        weekly_path = _first_existing_bootstrap_csv(
            ticker_root,
            "data/ams_3617_weekly_corn.csv",
            "USDA_daily_data/ams_3617_weekly_corn.csv",
        )
        weekly = _safe_read_csv(weekly_path) if weekly_path is not None else None
        if weekly is not None:
            specs.append(
                (
                    SourceFrameSpec(
                        df=weekly,
                        date_col="week_end",
                        source_file_col="",
                        source_type="ams_3617_weekly_corn_bootstrap",
                        aggregation_level="observation",
                        priority=20,
                    ),
                    weekly_path,
                )
            )
    return specs


def _normalize_bootstrap_frame(source: str, spec: SourceFrameSpec, csv_path: Path) -> List[Dict[str, Any]]:
    df = spec.df.copy()
    if spec.date_col not in df.columns:
        return []
    rows: List[Dict[str, Any]] = []
    for _, rec in df.iterrows():
        raw_date = rec.get(spec.date_col)
        parsed_q = parse_quarter_like(raw_date)
        if spec.aggregation_level.startswith("quarter"):
            obs_date = parsed_q
            qd = parsed_q
        else:
            ts = pd.to_datetime(raw_date, errors="coerce")
            if pd.isna(ts):
                continue
            obs_date = ts.date()
            qd = quarter_end_from_date(obs_date)
        if qd is None or obs_date is None:
            continue
        if spec.source_file_col and spec.source_file_col in df.columns:
            source_file = str(rec.get(spec.source_file_col) or csv_path.name).strip() or csv_path.name
        else:
            source_file = csv_path.name
        for col in df.columns:
            if col in {spec.date_col, spec.source_file_col, "quarter"}:
                continue
            meta = series_meta_from_key(col, spec.source_type)
            if meta is None:
                continue
            val_num = pd.to_numeric(rec.get(col), errors="coerce")
            if pd.isna(val_num):
                continue
            rows.append(
                {
                    "observation_date": obs_date,
                    "quarter": qd,
                    "aggregation_level": spec.aggregation_level,
                    "publication_date": obs_date,
                    "source": source,
                    "report_type": spec.source_type,
                    "source_type": spec.source_type,
                    "market_family": meta.get("market_family"),
                    "series_key": str(col),
                    "instrument": meta.get("instrument"),
                    "location": meta.get("location"),
                    "region": meta.get("region"),
                    "tenor": meta.get("tenor"),
                    "price_value": float(val_num),
                    "unit": meta.get("unit"),
                    "quality": "high" if spec.priority >= 30 else "medium",
                    "source_file": source_file,
                    "parsed_note": f"Bootstrap import from {csv_path.name}",
                    "origin": "bootstrap",
                    "_priority": int(spec.priority),
                    "_obs_count": 1,
                }
            )
    return rows


def _bootstrap_rows_for_source(source: str, ticker_root: Path) -> Tuple[pd.DataFrame, str]:
    # The returned fingerprint participates in parsed-cache reuse. If a curated USDA
    # bootstrap CSV changes locally, we want the parsed/export layers to refresh even if
    # the raw USDA PDFs did not change.
    rows: List[Dict[str, Any]] = []
    csv_fps: List[str] = []
    for spec, csv_path in _bootstrap_specs_for_source(source, ticker_root):
        csv_fps.append(file_fingerprint(csv_path))
        rows.extend(_normalize_bootstrap_frame(source, spec, csv_path))
    if not rows:
        return pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS), "none"
    df = pd.DataFrame(rows)
    return _standardize_parsed_df(df), batch_fingerprint(csv_fps)


def _standardize_parsed_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    out = df.copy()
    if "obs_count" in out.columns:
        obs_count_num = pd.to_numeric(out["obs_count"], errors="coerce")
        if "_obs_count" in out.columns:
            current_num = pd.to_numeric(out["_obs_count"], errors="coerce")
            out["_obs_count"] = obs_count_num.where(obs_count_num.notna(), current_num)
        else:
            out["_obs_count"] = obs_count_num
    for col in PARSED_SCHEMA_COLUMNS:
        if col not in out.columns:
            out[col] = None
    out["observation_date"] = pd.to_datetime(out["observation_date"], errors="coerce")
    out["publication_date"] = pd.to_datetime(out["publication_date"], errors="coerce")
    out["quarter"] = pd.to_datetime(out["quarter"], errors="coerce")
    out["price_value"] = pd.to_numeric(out["price_value"], errors="coerce")
    out["_priority"] = pd.to_numeric(out["_priority"], errors="coerce").fillna(0).astype(int)
    out["_obs_count"] = pd.to_numeric(out["_obs_count"], errors="coerce").fillna(1).astype(int)
    return out[PARSED_SCHEMA_COLUMNS].copy()


def _dedupe_parsed_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    key_cols = [
        "observation_date",
        "quarter",
        "aggregation_level",
        "source",
        "series_key",
        "region",
        "tenor",
        "unit",
    ]
    work = df.copy()
    work = work.sort_values(
        by=["_priority", "publication_date", "observation_date"],
        ascending=[False, False, False],
        na_position="last",
    )
    work = work.drop_duplicates(subset=key_cols, keep="first")
    return work.reset_index(drop=True)


def _load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _combine_observation_rows(bootstrap_df: pd.DataFrame, provider_df: pd.DataFrame) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    if bootstrap_df is not None and not bootstrap_df.empty:
        parts.append(bootstrap_df[bootstrap_df["aggregation_level"] == "observation"].copy())
    if provider_df is not None and not provider_df.empty:
        parts.append(provider_df[provider_df["aggregation_level"] == "observation"].copy())
    if not parts:
        return pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    return _dedupe_parsed_df(pd.concat(parts, ignore_index=True))


def _build_quarterly_rows(obs_df: pd.DataFrame, bootstrap_df: pd.DataFrame, provider_df: pd.DataFrame) -> pd.DataFrame:
    obs_quarterly = aggregate_quarterly(obs_df) if obs_df is not None and not obs_df.empty else pd.DataFrame()
    direct_parts: List[pd.DataFrame] = []
    for part in (bootstrap_df, provider_df):
        if part is None or part.empty:
            continue
        direct = part[part["aggregation_level"].isin(["quarter_avg", "quarter_end"])].copy()
        if not direct.empty:
            direct_parts.append(direct)
    quarterly_parts: List[pd.DataFrame] = []
    if not obs_quarterly.empty:
        obs_quarterly["origin"] = obs_quarterly.get("origin").fillna("aggregated")
        obs_quarterly["_priority"] = pd.to_numeric(obs_quarterly.get("_priority"), errors="coerce").fillna(30).astype(int)
        quarterly_parts.append(_standardize_parsed_df(obs_quarterly))
    if direct_parts:
        quarterly_parts.append(_standardize_parsed_df(pd.concat(direct_parts, ignore_index=True)))
    if not quarterly_parts:
        return pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    combined = pd.concat(quarterly_parts, ignore_index=True)
    return _dedupe_parsed_df(combined)


def _provider_parse_rows(provider: Any, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
    try:
        parsed = provider.parse_raw_to_rows(cache_root=cache_root, ticker_root=ticker_root, raw_entries=raw_entries)
    except TypeError:
        parsed = provider.parse_raw_to_rows(cache_root, ticker_root, raw_entries)
    except Exception:
        parsed = pd.DataFrame()
    if parsed is None or parsed.empty:
        return pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    out = _standardize_parsed_df(parsed)
    if "origin" not in out.columns or out["origin"].isna().all():
        out["origin"] = "provider_raw"
    if "_priority" not in out.columns or out["_priority"].isna().all():
        out["_priority"] = 50
    return _dedupe_parsed_df(out)


def _build_export_rows(quarterly_df: pd.DataFrame, observations_df: pd.DataFrame) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    if observations_df is not None and not observations_df.empty:
        parts.append(observations_df.copy())
    if quarterly_df is not None and not quarterly_df.empty:
        parts.append(quarterly_df.copy())
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df = df[df["price_value"].notna()].copy()
    if df.empty:
        return df
    out = pd.DataFrame(
        {
            "observation_date": pd.to_datetime(df["observation_date"], errors="coerce"),
            "quarter": pd.to_datetime(df["quarter"], errors="coerce"),
            "aggregation_level": df["aggregation_level"],
            "source_file": df["source_file"],
            "source_type": df["source_type"].where(df["source_type"].notna(), df["report_type"]),
            "market_family": df["market_family"],
            "series_key": df["series_key"],
            "instrument": df["instrument"],
            "region": df["region"],
            "contract_tenor": df["tenor"],
            "price_value": df["price_value"],
            "unit": df["unit"],
            "parsed_text": df["parsed_note"],
            "quality": df["quality"],
            "_obs_count": pd.to_numeric(df.get("_obs_count"), errors="coerce").fillna(0).astype(int),
        }
    )
    out = out.sort_values(["quarter", "series_key", "aggregation_level", "observation_date"], na_position="last").reset_index(drop=True)
    return out


def _save_parsed_frames(cache_root: Path, source: str, obs_df: pd.DataFrame, quarterly_df: pd.DataFrame) -> None:
    # Parsed parquet files are rebuildable accelerators. They preserve the expensive
    # provider-specific extraction work so later exports and workbook reads do not
    # need to revisit raw PDFs/HTML on every run.
    parsed_obs_path(cache_root, source).parent.mkdir(parents=True, exist_ok=True)
    obs_df.to_parquet(parsed_obs_path(cache_root, source), index=False)
    quarterly_df.to_parquet(parsed_quarter_path(cache_root, source), index=False)


def sync_market_cache(
    cache_dir: Path,
    ticker: str,
    profile: Any = None,
    *,
    sync_raw: bool = False,
    refresh: bool = False,
    reparse: bool = False,
) -> SyncSummary:
    # Sync keeps the raw/index/parsed/export layers aligned. The final export parquet is
    # the workbook-facing artifact; manifests are the bookkeeping layer that lets us
    # reuse parsed outputs when the raw fingerprint has not changed.
    ticker_u = str(ticker or "").strip().upper()
    if not ticker_u:
        raise ValueError("ticker is required for market-data sync")
    cache_root = resolve_market_cache_root(Path(cache_dir))
    ensure_market_cache_dirs(cache_root)
    ticker_root = _ticker_root_from_cache_dir(Path(cache_dir), ticker_u)
    if ticker_u == "GPRE" and (sync_raw or refresh):
        _refresh_gpre_corn_bids_download(
            ticker_root,
            refresh=bool(refresh),
        )
    enabled_sources = tuple(src for src in _enabled_sources_for_profile(profile) if src in PROVIDERS)
    raw_manifest = load_manifest(raw_manifest_path(cache_root))
    parsed_manifest = load_manifest(parsed_manifest_path(cache_root))

    raw_added = raw_refreshed = raw_skipped = 0
    raw_entries_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for source in enabled_sources:
        provider = PROVIDERS[source]
        manifest_entries = normalize_manifest_list(raw_manifest, source)
        if sync_raw or refresh:
            sync_result = provider.sync_raw(cache_root, ticker_root, refresh=refresh)
            raw_added += int(sync_result.get("raw_added") or 0)
            raw_refreshed += int(sync_result.get("raw_refreshed") or 0)
            raw_skipped += int(sync_result.get("raw_skipped") or 0)
            manifest_entries = [
                row
                for row in list(sync_result.get("entries") or [])
                if isinstance(row, dict)
            ]
        disk_entries = _raw_entries_from_disk(cache_root, source, provider)
        raw_entries = _merge_raw_entries(
            manifest_entries,
            disk_entries,
            source=source,
            provider=provider,
        )
        raw_entries_by_source[source] = raw_entries
        raw_manifest[source] = raw_entries
    save_manifest(raw_manifest_path(cache_root), raw_manifest)

    parsed_sources: List[str] = []
    all_export_parts: List[pd.DataFrame] = []
    for source in enabled_sources:
        provider = PROVIDERS[source]
        # Parsed/export reuse depends on both the normalized raw fingerprint and any
        # local curated bootstrap CSVs living under `<ticker>/data` or the USDA folders.
        bootstrap_df, bootstrap_fp = _bootstrap_rows_for_source(source, ticker_root)
        raw_entries = raw_entries_by_source.get(source, [])
        raw_fp = batch_fingerprint([str(x.get("checksum") or "") for x in raw_entries])
        combined_raw_fp = batch_fingerprint([bootstrap_fp, raw_fp])
        parse_version = str(getattr(provider, "provider_parse_version", "v1") or "v1")
        manifest_entry = parsed_manifest.get(source) if isinstance(parsed_manifest.get(source), dict) else {}
        obs_path = parsed_obs_path(cache_root, source)
        qtr_path = parsed_quarter_path(cache_root, source)
        # Parsed outputs can be reused only when the normalized raw fingerprint and
        # parser behavior version still match. Otherwise we reparse from raw/bootstrap
        # sources and refresh the parsed manifest entry.
        can_reuse = (
            not reparse
            and obs_path.exists()
            and qtr_path.exists()
            and str(manifest_entry.get("raw_fingerprint") or "") == combined_raw_fp
            and str(manifest_entry.get("parse_version") or "") == parse_version
            and str(manifest_entry.get("parse_status") or "") == "ok"
        )
        if can_reuse:
            obs_df = _load_parquet(obs_path)
            qtr_df = _load_parquet(qtr_path)
        else:
            provider_df = _provider_parse_rows(provider, cache_root, ticker_root, raw_entries)
            obs_df = _combine_observation_rows(bootstrap_df, provider_df)
            qtr_df = _build_quarterly_rows(obs_df, bootstrap_df, provider_df)
            _save_parsed_frames(cache_root, source, obs_df, qtr_df)
            parsed_manifest[source] = {
                "source": source,
                "local_path": str(obs_path.parent),
                "raw_fingerprint": combined_raw_fp,
                "parse_version": parse_version,
                "parsed_at": pd.Timestamp.now("UTC").isoformat(),
                "row_count": int(len(obs_df) + len(qtr_df)),
                "parse_status": "ok",
            }
            parsed_sources.append(source)
        all_export_parts.append(_build_export_rows(qtr_df, obs_df))
    save_manifest(parsed_manifest_path(cache_root), parsed_manifest)

    # The export parquet flattens source-specific parsed frames into one ticker-facing
    # dataset so workbook code can stay provider-agnostic. This is the layer the
    # workbook actually consumes; raw and parsed trees mainly support building it.
    export_df = pd.concat(all_export_parts, ignore_index=True) if all_export_parts else pd.DataFrame()
    export_path = export_rows_path(cache_root, ticker_u)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    if export_df.empty:
        pd.DataFrame(
            columns=[
                "observation_date",
                "quarter",
                "aggregation_level",
                "source_file",
                "source_type",
                "market_family",
                "series_key",
                "instrument",
                "region",
                "contract_tenor",
                "price_value",
                "unit",
                "parsed_text",
                "quality",
                "_obs_count",
            ]
        ).to_parquet(export_path, index=False)
    else:
        export_df.to_parquet(export_path, index=False)

    return SyncSummary(
        sources_enabled=enabled_sources,
        raw_added=raw_added,
        raw_refreshed=raw_refreshed,
        raw_skipped=raw_skipped,
        parsed_sources=tuple(parsed_sources),
        export_rows=int(len(export_df)),
        export_path=export_path,
    )


def _rows_from_export_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    out: List[Dict[str, Any]] = []
    for rec in df.to_dict("records"):
        row = dict(rec)
        for key in ("observation_date", "quarter"):
            val = row.get(key)
            ts = pd.to_datetime(val, errors="coerce")
            row[key] = None if pd.isna(ts) else ts.date()
        out.append(row)
    return out


def _earliest_local_source_date_for_enabled_sources(
    ticker_root: Path,
    enabled_sources: Iterable[str],
) -> Optional[date]:
    earliest: Optional[date] = None
    for source in enabled_sources:
        provider = PROVIDERS.get(str(source or "").strip())
        if provider is None:
            continue
        seen_paths: set[str] = set()
        for pattern in tuple(getattr(provider, "local_patterns", ()) or ()):
            for path in ticker_root.glob(pattern):
                if not path.is_file():
                    continue
                try:
                    resolved = str(path.resolve())
                except Exception:
                    resolved = str(path)
                if resolved in seen_paths:
                    continue
                seen_paths.add(resolved)
                try:
                    report_ts = provider._date_from_name(path)
                except Exception:
                    report_ts = None
                if report_ts is None or pd.isna(report_ts):
                    continue
                report_dt = report_ts.date()
                if earliest is None or report_dt < earliest:
                    earliest = report_dt
    return earliest


def _export_needs_history_repair(
    df: pd.DataFrame,
    *,
    ticker_root: Path,
    enabled_sources: Iterable[str],
) -> bool:
    if df is None or df.empty:
        return True
    export_obs = pd.to_datetime(df.get("observation_date"), errors="coerce").dropna()
    export_quarters = pd.to_datetime(df.get("quarter"), errors="coerce").dropna()
    if export_obs.empty or export_quarters.empty:
        return True
    earliest_local = _earliest_local_source_date_for_enabled_sources(ticker_root, enabled_sources)
    if earliest_local is None:
        return False
    earliest_export = export_obs.min().date()
    if earliest_local >= earliest_export:
        needs_date_repair = False
    else:
        unique_quarters = {ts.date() for ts in export_quarters}
        needs_date_repair = len(unique_quarters) <= 4 or earliest_local.year <= earliest_export.year - 1
    enabled_set = {str(src or "").strip() for src in enabled_sources}
    if "ams_3617" in enabled_set:
        series = {str(x or "").strip().lower() for x in df.get("series_key", pd.Series(dtype=str)).astype(str)}
        has_basis = any(key.startswith("corn_basis_") for key in series)
        has_local_daily = any((ticker_root / "USDA_daily_data").glob("*.pdf")) if ticker_root.exists() else False
        if has_local_daily and not has_basis:
            return True
    if needs_date_repair:
        return True
    unique_quarters = {ts.date() for ts in export_quarters}
    return len(unique_quarters) <= 4 or earliest_local.year <= earliest_export.year - 1


_TENOR_RE = re.compile(r"^(?P<month>[a-z]{3})(?P<year>\d{2})$", re.I)
_MONTH_ABBREV = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def calendar_quarter_bounds(as_of_date: Optional[date] = None) -> tuple[date, date]:
    ref = as_of_date or date.today()
    start_month = ((int(ref.month) - 1) // 3) * 3 + 1
    q_start = date(int(ref.year), start_month, 1)
    if start_month == 10:
        q_end = date(int(ref.year), 12, 31)
    else:
        next_q_start = date(int(ref.year), start_month + 3, 1)
        q_end = next_q_start - timedelta(days=1)
    return q_start, q_end


def next_calendar_quarter_bounds(as_of_date: Optional[date] = None) -> tuple[date, date]:
    _, current_q_end = calendar_quarter_bounds(as_of_date=as_of_date)
    next_q_start = current_q_end + timedelta(days=1)
    return calendar_quarter_bounds(as_of_date=next_q_start)


def prior_calendar_quarter_bounds(as_of_date: Optional[date] = None) -> tuple[date, date]:
    current_q_start, _ = calendar_quarter_bounds(as_of_date=as_of_date)
    return calendar_quarter_bounds(as_of_date=(current_q_start - timedelta(days=1)))


def _market_rows_df(rows: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    return normalize_market_rows_df(rows)


def _series_observation_df(df: pd.DataFrame, series_key: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    require_market_columns(
        df,
        ["aggregation_level", "series_key", "observation_date", "price_value"],
        contract_name="_series_observation_df",
    )
    out = df[
        (df["aggregation_level"].astype(str).str.lower() == "observation")
        & (df["series_key"].astype(str) == str(series_key))
        & df["observation_date"].notna()
        & df["price_value"].notna()
    ].copy()
    if out.empty:
        return out
    return out.sort_values("observation_date").reset_index(drop=True)


def _average_daily_corn_for_week(df_daily: pd.DataFrame, week_end: date) -> Optional[float]:
    if df_daily is None or df_daily.empty:
        return None
    require_market_columns(
        df_daily,
        ["observation_date", "price_value"],
        contract_name="_average_daily_corn_for_week",
    )
    end_ts = pd.Timestamp(week_end)
    start_ts = end_ts - pd.Timedelta(days=6)
    window = df_daily[(df_daily["observation_date"] >= start_ts) & (df_daily["observation_date"] <= end_ts)].copy()
    vals = pd.to_numeric(window.get("price_value"), errors="coerce").dropna()
    if vals.empty:
        return None
    return float(vals.mean())


def _series_window_market_meta(
    df_obs: pd.DataFrame,
    *,
    window_start: date,
    window_end: date,
    cadence: str,
) -> Dict[str, Any]:
    if df_obs is None or df_obs.empty:
        return {
            "value": None,
            "as_of": None,
            "obs_count": 0,
            "cadence": cadence,
        }
    require_market_columns(
        df_obs,
        ["observation_date", "price_value"],
        contract_name="_series_window_market_meta",
    )
    window = df_obs[
        (df_obs["observation_date"].dt.date >= window_start)
        & (df_obs["observation_date"].dt.date <= window_end)
    ].copy()
    vals = pd.to_numeric(window.get("price_value"), errors="coerce").dropna()
    if vals.empty:
        return {
            "value": None,
            "as_of": None,
            "obs_count": 0,
            "cadence": cadence,
        }
    return {
        "value": float(vals.mean()),
        "as_of": pd.Timestamp(window["observation_date"].max()).date(),
        "obs_count": int(vals.shape[0]),
        "cadence": cadence,
    }


_GPRE_BID_LOCATION_REGION: Dict[str, str] = {
    "Central City": "nebraska",
    "Fairmont": "minnesota",
    "Obion": "tennessee",
    "Wood River": "nebraska",
    "York": "nebraska",
    "Madison": "illinois",
    "Mount Vernon": "indiana",
    "Shenandoah": "iowa_west",
    "Superior": "iowa_west",
    "Otter Tail": "minnesota",
}
_GPRE_BID_MONTHS: Dict[str, int] = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
    "hvst": 10,
    "harvest": 10,
}
_GPRE_PLANT_REGISTRY: tuple[Dict[str, Any], ...] = (
    {
        "plant_key": "central_city",
        "location": "Central City",
        "state": "Nebraska",
        "region": "nebraska",
        "fallback_capacity_mmgy": 116.0,
    },
    {
        "plant_key": "wood_river",
        "location": "Wood River",
        "state": "Nebraska",
        "region": "nebraska",
        "fallback_capacity_mmgy": 121.0,
    },
    {
        "plant_key": "york",
        "location": "York",
        "state": "Nebraska",
        "region": "nebraska",
        "fallback_capacity_mmgy": 50.0,
    },
    {
        "plant_key": "madison",
        "location": "Madison",
        "state": "Illinois",
        "region": "illinois",
        "fallback_capacity_mmgy": 90.0,
    },
    {
        "plant_key": "mount_vernon",
        "location": "Mount Vernon",
        "state": "Indiana",
        "region": "indiana",
        "fallback_capacity_mmgy": 90.0,
    },
    {
        "plant_key": "shenandoah",
        "location": "Shenandoah",
        "state": "Iowa",
        "region": "iowa_west",
        "fallback_capacity_mmgy": 82.0,
    },
    {
        "plant_key": "superior",
        "location": "Superior",
        "state": "Iowa",
        "region": "iowa_west",
        "fallback_capacity_mmgy": 60.0,
    },
    {
        "plant_key": "otter_tail",
        "location": "Otter Tail",
        "state": "Minnesota",
        "region": "minnesota",
        "fallback_capacity_mmgy": 55.0,
    },
    {
        "plant_key": "fairmont",
        "location": "Fairmont",
        "state": "Minnesota",
        "region": "minnesota",
        "fallback_capacity_mmgy": 119.0,
        "inactive_from": date(2025, 1, 1),
        "status_note": "Fairmont idled in January 2025.",
    },
    {
        "plant_key": "obion",
        "location": "Obion",
        "state": "Tennessee",
        "region": "tennessee",
        "fallback_capacity_mmgy": 120.0,
        "active_through": date(2025, 9, 30),
        "status_note": "Obion / Tennessee exited with the September 2025 sale.",
    },
)
_GPRE_PLANT_REGISTRY_BY_KEY: Dict[str, Dict[str, Any]] = {
    str(rec.get("plant_key") or ""): dict(rec)
    for rec in _GPRE_PLANT_REGISTRY
    if str(rec.get("plant_key") or "").strip()
}
_GPRE_PLANT_KEY_BY_LOCATION: Dict[str, str] = {
    str(rec.get("location") or "").strip().lower(): str(rec.get("plant_key") or "").strip()
    for rec in _GPRE_PLANT_REGISTRY
    if str(rec.get("location") or "").strip()
}
_GPRE_FALLBACK_CAPACITY_FOOTNOTES: Dict[str, Dict[str, str]] = {
    "2024": {
        "1": "Produces Ultra-High Protein.",
        "2": "Committed to Tallgrass Trailblazer Pipeline.",
        "3": "Committed to Summit Carbon Solutions Pipeline.",
        "4": "Plant idled in January 2025.",
    },
    "2025": {
        "1": "Produces Ultra-High Protein.",
        "2": "Connected to Tallgrass Trailblazer Pipeline.",
        "3": "Committed to Summit Carbon Solutions Pipeline.",
        "4": "Plant idled in January 2025.",
    },
}
_GPRE_FALLBACK_CAPACITY_SNAPSHOTS: tuple[Dict[str, Any], ...] = (
    {
        "snapshot_year": 2024,
        "snapshot_quarter_end": date(2024, 12, 31),
        "table_label": "Plant Production Capacity (mmgy)",
        "source_kind": "fallback_seed",
        "source_path": "",
        "footnotes": dict(_GPRE_FALLBACK_CAPACITY_FOOTNOTES["2024"]),
        "plants": {
            "central_city": {"location": "Central City", "state": "Nebraska", "region": "nebraska", "capacity_mmgy": 116.0, "footnote_ids": ["1", "2"]},
            "fairmont": {"location": "Fairmont", "state": "Minnesota", "region": "minnesota", "capacity_mmgy": 119.0, "footnote_ids": ["3", "4"]},
            "madison": {"location": "Madison", "state": "Illinois", "region": "illinois", "capacity_mmgy": 90.0, "footnote_ids": []},
            "mount_vernon": {"location": "Mount Vernon", "state": "Indiana", "region": "indiana", "capacity_mmgy": 90.0, "footnote_ids": ["1"]},
            "obion": {"location": "Obion", "state": "Tennessee", "region": "tennessee", "capacity_mmgy": 120.0, "footnote_ids": ["1"]},
            "otter_tail": {"location": "Otter Tail", "state": "Minnesota", "region": "minnesota", "capacity_mmgy": 55.0, "footnote_ids": ["3"]},
            "shenandoah": {"location": "Shenandoah", "state": "Iowa", "region": "iowa_west", "capacity_mmgy": 82.0, "footnote_ids": ["1", "3"]},
            "superior": {"location": "Superior", "state": "Iowa", "region": "iowa_west", "capacity_mmgy": 60.0, "footnote_ids": ["3"]},
            "wood_river": {"location": "Wood River", "state": "Nebraska", "region": "nebraska", "capacity_mmgy": 121.0, "footnote_ids": ["1", "2"]},
            "york": {"location": "York", "state": "Nebraska", "region": "nebraska", "capacity_mmgy": 50.0, "footnote_ids": ["2"]},
        },
    },
    {
        "snapshot_year": 2025,
        "snapshot_quarter_end": date(2025, 12, 31),
        "table_label": "Stated Production Capacity (mmgy)",
        "source_kind": "fallback_seed",
        "source_path": "",
        "footnotes": dict(_GPRE_FALLBACK_CAPACITY_FOOTNOTES["2025"]),
        "plants": {
            "central_city": {"location": "Central City", "state": "Nebraska", "region": "nebraska", "capacity_mmgy": 120.0, "footnote_ids": ["1", "2"]},
            "fairmont": {"location": "Fairmont", "state": "Minnesota", "region": "minnesota", "capacity_mmgy": 120.0, "footnote_ids": ["3", "4"]},
            "madison": {"location": "Madison", "state": "Illinois", "region": "illinois", "capacity_mmgy": 100.0, "footnote_ids": []},
            "mount_vernon": {"location": "Mount Vernon", "state": "Indiana", "region": "indiana", "capacity_mmgy": 110.0, "footnote_ids": ["1"]},
            "otter_tail": {"location": "Otter Tail", "state": "Minnesota", "region": "minnesota", "capacity_mmgy": 70.0, "footnote_ids": ["3"]},
            "shenandoah": {"location": "Shenandoah", "state": "Iowa", "region": "iowa_west", "capacity_mmgy": 80.0, "footnote_ids": ["1", "3"]},
            "superior": {"location": "Superior", "state": "Iowa", "region": "iowa_west", "capacity_mmgy": 70.0, "footnote_ids": ["3"]},
            "wood_river": {"location": "Wood River", "state": "Nebraska", "region": "nebraska", "capacity_mmgy": 120.0, "footnote_ids": ["1", "2"]},
            "york": {"location": "York", "state": "Nebraska", "region": "nebraska", "capacity_mmgy": 60.0, "footnote_ids": ["2"]},
        },
    },
)


def _gpre_capacity_cache_roots(ticker_root: Optional[Path]) -> List[Path]:
    roots: List[Path] = []
    seen: set[Path] = set()

    def _add(path_in: Optional[Path]) -> None:
        if path_in is None:
            return
        try:
            resolved = Path(path_in).expanduser().resolve()
        except Exception:
            try:
                resolved = Path(path_in).expanduser()
            except Exception:
                return
        if resolved in seen:
            return
        seen.add(resolved)
        roots.append(resolved)

    if isinstance(ticker_root, Path):
        root = ticker_root.expanduser()
        _add(root / "sec_cache")
        _add(root / "sec_cache" / "GPRE")
        _add(root.parent / "sec_cache" / "GPRE")
        _add(root.parent / "GPRE" / "sec_cache")
    repo_root = Path(__file__).resolve().parents[2]
    _add(repo_root / "sec_cache" / "GPRE")
    _add(repo_root / "GPRE" / "sec_cache")
    return [path for path in roots if path.exists()]


def _gpre_capacity_table_html_paths(ticker_root: Optional[Path]) -> List[Path]:
    paths: List[Path] = []
    seen: set[Path] = set()
    for root in _gpre_capacity_cache_roots(ticker_root):
        for pattern in ("**/gpre-*.htm", "**/gpre-*.html"):
            for path_in in sorted(root.glob(pattern)):
                if not path_in.is_file():
                    continue
                name_low = path_in.name.lower()
                token_match = re.search(r"gpre-(20\d{6})", name_low)
                if token_match is None or not token_match.group(1).endswith("1231"):
                    continue
                try:
                    resolved = path_in.resolve()
                except Exception:
                    resolved = path_in
                if resolved in seen:
                    continue
                seen.add(resolved)
                paths.append(resolved)
    paths.sort(
        key=lambda path_in: (
            int(re.search(r"gpre-(20\d{6})", path_in.name.lower()).group(1)) if re.search(r"gpre-(20\d{6})", path_in.name.lower()) else 0,
            str(path_in),
        )
    )
    return paths


def _gpre_parse_capacity_footnote_flags(text: Any) -> Dict[str, Any]:
    txt = re.sub(r"\s+", " ", str(text or "").strip())
    low = txt.lower()
    flags: Dict[str, Any] = {}
    if not txt:
        return flags
    if "ultra-high protein" in low:
        flags["uhp"] = True
    if "trailblazer" in low:
        flags["trailblazer"] = True
    if "summit carbon" in low:
        flags["summit_carbon"] = True
    if "idled" in low:
        flags["idled"] = True
    if any(term in low for term in ("sold", "sale", "exited", "no longer part")):
        flags["sold_or_exited"] = True
    month_match = re.search(
        r"\b(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\s+(20\d{2})\b",
        txt,
        re.I,
    )
    if month_match:
        month_num = _GPRE_BID_MONTHS.get(str(month_match.group(1) or "").strip().lower())
        year_num = pd.to_numeric(month_match.group(2), errors="coerce")
        if month_num and pd.notna(year_num):
            try:
                flags["effective_date"] = date(int(year_num), int(month_num), 1)
            except Exception:
                pass
    return flags


def _gpre_extract_capacity_footnotes_from_text(text_in: Any) -> Dict[str, Dict[str, Any]]:
    text_norm = re.sub(r"\s+", " ", str(text_in or "").replace("\xa0", " ")).strip()
    if not text_norm:
        return {}
    start = -1
    for token in ("Plant Production Capacity", "Stated Production Capacity"):
        start = text_norm.find(token)
        if start >= 0:
            break
    snippet = text_norm[start:] if start >= 0 else text_norm
    total_idx = snippet.find("Total")
    if total_idx >= 0:
        snippet = snippet[total_idx:]
    snippet = snippet[:2500]
    out: Dict[str, Dict[str, Any]] = {}
    for match in re.finditer(r"\((\d+)\)\s*([^()]+?)(?=(?:\(\d+\)\s)|$)", snippet):
        footnote_id = str(match.group(1) or "").strip()
        footnote_text = re.sub(r"\s+", " ", str(match.group(2) or "").strip()).rstrip(".")
        if not footnote_id or not footnote_text:
            continue
        full_text = footnote_text + "."
        sentence_match = re.match(r"(.+?\.)\s", full_text)
        if sentence_match:
            full_text = str(sentence_match.group(1) or "").strip()
        out[footnote_id] = {
            "text": full_text,
            "flags": _gpre_parse_capacity_footnote_flags(full_text),
        }
    return out


def _gpre_read_cached_doc_text(path_in: Path) -> str:
    try:
        raw = path_in.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            raw = path_in.read_text(errors="ignore")
        except Exception:
            return ""
    if str(path_in.suffix or "").lower() in {".htm", ".html", ".xml"}:
        raw = re.sub(r"<[^>]+>", " ", raw)
        raw = html.unescape(raw)
    return re.sub(r"\s+", " ", str(raw or "")).strip()


def _gpre_parse_capacity_snapshot_from_html_path(path_in: Path) -> Optional[Dict[str, Any]]:
    try:
        html_bytes = Path(path_in).read_bytes()
    except Exception:
        return None
    try:
        tables = read_html_tables_any(html_bytes)
    except Exception:
        tables = []
    chosen_df: Optional[pd.DataFrame] = None
    chosen_idx = -1
    chosen_cols: Tuple[str, str] = ("", "")
    for idx, df in enumerate(tables):
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        columns = [re.sub(r"\s+", " ", str(col or "").replace("\n", " ").strip()) for col in df.columns]
        location_col = next((col for col in columns if "plant location" in col.lower()), "")
        capacity_col = next((col for col in columns if "capacity" in col.lower()), "")
        if location_col and capacity_col and location_col in df.columns and capacity_col in df.columns:
            chosen_df = df.copy()
            chosen_idx = idx
            chosen_cols = (location_col, capacity_col)
            break
        first_row = [
            re.sub(r"\s+", " ", str(val or "").replace("\n", " ").strip())
            for val in list(df.iloc[0].tolist())
        ]
        location_idx = next((i for i, val in enumerate(first_row) if "plant location" in val.lower()), -1)
        capacity_idx = next((i for i, val in enumerate(first_row) if "capacity" in val.lower()), -1)
        if location_idx >= 0 and capacity_idx >= 0:
            normalized = df.iloc[1:].copy().reset_index(drop=True)
            if location_idx < len(normalized.columns) and capacity_idx < len(normalized.columns):
                normalized = normalized.rename(
                    columns={
                        normalized.columns[location_idx]: "plant_location",
                        normalized.columns[capacity_idx]: "plant_capacity_mmgy",
                    }
                )
                chosen_df = normalized
                chosen_idx = idx
                chosen_cols = ("plant_location", "plant_capacity_mmgy")
                break
    if chosen_df is None:
        return None
    period_match = re.search(r"gpre-(20\d{6})", str(path_in.name or "").lower())
    snapshot_qd = None
    snapshot_year = None
    if period_match:
        period_txt = str(period_match.group(1) or "")
        try:
            snapshot_qd = datetime.strptime(period_txt, "%Y%m%d").date()
            snapshot_year = snapshot_qd.year
        except Exception:
            snapshot_qd = None
    footnotes = _gpre_extract_capacity_footnotes_from_text(_gpre_read_cached_doc_text(path_in))
    location_col, capacity_col = chosen_cols
    plants: Dict[str, Dict[str, Any]] = {}
    for rec in chosen_df.to_dict("records"):
        raw_location = re.sub(r"\s+", " ", str(rec.get(location_col) or "").replace("\xa0", " ").strip())
        raw_capacity = rec.get(capacity_col)
        if not raw_location or raw_location.lower().startswith("total"):
            continue
        capacity_num = pd.to_numeric(str(raw_capacity).replace(",", "").strip(), errors="coerce")
        if pd.isna(capacity_num):
            continue
        footnote_ids = sorted(set(re.findall(r"\((\d+)\)", f"{raw_location} {raw_capacity}")))
        clean_location = re.sub(r"\(\d+\)", "", raw_location)
        clean_location = re.sub(r"\s+", " ", clean_location).strip(" ,")
        parts = [part.strip() for part in clean_location.split(",") if str(part or "").strip()]
        location_name = str(parts[0] or "").strip()
        state_name = str(parts[1] or "").strip() if len(parts) > 1 else str((_GPRE_PLANT_REGISTRY_BY_KEY.get(_GPRE_PLANT_KEY_BY_LOCATION.get(location_name.lower(), ""), {}) or {}).get("state") or "").strip()
        plant_key = _GPRE_PLANT_KEY_BY_LOCATION.get(location_name.lower(), re.sub(r"[^a-z0-9]+", "_", location_name.lower()).strip("_"))
        registry_rec = dict(_GPRE_PLANT_REGISTRY_BY_KEY.get(plant_key) or {})
        plants[plant_key] = {
            "plant_key": plant_key,
            "location": location_name,
            "state": state_name,
            "region": str(registry_rec.get("region") or _GPRE_BID_LOCATION_REGION.get(location_name, "")),
            "capacity_mmgy": float(capacity_num),
            "footnote_ids": footnote_ids,
            "footnotes": {footnote_id: dict(footnotes.get(footnote_id) or {}) for footnote_id in footnote_ids},
        }
    if not plants:
        return None
    return {
        "snapshot_year": snapshot_year,
        "snapshot_quarter_end": snapshot_qd,
        "table_label": str(capacity_col or "").strip(),
        "source_kind": "sec_cache_html",
        "source_path": str(path_in),
        "table_index": chosen_idx,
        "footnotes": footnotes,
        "plants": plants,
    }


def _gpre_resolve_plant_capacity_history(
    plant_capacity_history: Optional[Dict[str, Any]],
    *,
    ticker_root: Optional[Path],
) -> Dict[str, Any]:
    if isinstance(plant_capacity_history, dict) and plant_capacity_history.get("plants"):
        return dict(plant_capacity_history)
    snapshots = [
        dict(rec)
        for rec in list((plant_capacity_history or {}).get("snapshots") or [])
        if isinstance(rec, dict) and rec.get("plants")
    ]
    if not snapshots:
        for path_in in _gpre_capacity_table_html_paths(ticker_root):
            snap = _gpre_parse_capacity_snapshot_from_html_path(path_in)
            if snap:
                snapshots.append(snap)
    if not snapshots:
        snapshots = [dict(rec) for rec in _GPRE_FALLBACK_CAPACITY_SNAPSHOTS]
    snapshots = sorted(
        snapshots,
        key=lambda rec: (
            _safe_int_from_numeric(rec.get("snapshot_year")),
            str(rec.get("source_path") or ""),
        ),
    )
    plants: Dict[str, Dict[str, Any]] = {}
    for base_rec in _GPRE_PLANT_REGISTRY:
        plant_key = str(base_rec.get("plant_key") or "").strip()
        if not plant_key:
            continue
        plants[plant_key] = {
            **dict(base_rec),
            "capacity_by_snapshot_year": {},
            "footnotes_by_snapshot_year": {},
            "flags": {},
            "source_paths": [],
        }
    for snapshot in snapshots:
        snapshot_year = _safe_int_from_numeric(snapshot.get("snapshot_year"))
        footnotes = dict(snapshot.get("footnotes") or {})
        for plant_key, plant_rec in dict(snapshot.get("plants") or {}).items():
            plant_key_txt = str(plant_key or "").strip()
            if not plant_key_txt:
                continue
            target = plants.setdefault(plant_key_txt, {"plant_key": plant_key_txt, "capacity_by_snapshot_year": {}, "footnotes_by_snapshot_year": {}, "flags": {}, "source_paths": []})
            target.setdefault("location", plant_rec.get("location"))
            target.setdefault("state", plant_rec.get("state"))
            target.setdefault("region", plant_rec.get("region"))
            target["capacity_by_snapshot_year"][snapshot_year] = float(pd.to_numeric(plant_rec.get("capacity_mmgy"), errors="coerce"))
            footnotes_for_year: Dict[str, Dict[str, Any]] = {}
            for footnote_id in list(plant_rec.get("footnote_ids") or []):
                raw_footnote = footnotes.get(footnote_id) or plant_rec.get("footnotes", {}).get(footnote_id) or {}
                if isinstance(raw_footnote, dict):
                    footnote_rec = dict(raw_footnote)
                else:
                    footnote_text = str(raw_footnote or "").strip()
                    footnote_rec = {
                        "text": footnote_text,
                        "flags": _gpre_parse_capacity_footnote_flags(footnote_text),
                    } if footnote_text else {}
                footnotes_for_year[str(footnote_id)] = footnote_rec
            target["footnotes_by_snapshot_year"][snapshot_year] = footnotes_for_year
            for footnote_id, footnote_rec in target["footnotes_by_snapshot_year"][snapshot_year].items():
                text = str((footnote_rec or {}).get("text") or "").strip()
                if text:
                    target.setdefault("footnote_texts", {})[str(footnote_id)] = text
                for flag_key, flag_val in dict((footnote_rec or {}).get("flags") or {}).items():
                    if flag_key == "effective_date" and isinstance(flag_val, date):
                        target["flags"][flag_key] = flag_val
                    elif bool(flag_val):
                        target["flags"][flag_key] = True
            source_path = str(snapshot.get("source_path") or "").strip()
            if source_path:
                target["source_paths"] = [*list(target.get("source_paths") or []), source_path]
    for plant_key, plant_rec in plants.items():
        if not plant_rec.get("capacity_by_snapshot_year"):
            fallback_capacity = pd.to_numeric(plant_rec.get("fallback_capacity_mmgy"), errors="coerce")
            if pd.notna(fallback_capacity):
                plant_rec["capacity_by_snapshot_year"] = {
                    int(rec.get("snapshot_year")): float(fallback_capacity)
                    for rec in snapshots
                    if pd.notna(pd.to_numeric(rec.get("snapshot_year"), errors="coerce"))
                }
        if plant_key == "fairmont":
            plant_rec["inactive_from"] = date(2025, 1, 1)
        if plant_key == "obion":
            plant_rec["active_through"] = date(2025, 9, 30)
    return {
        "source_mode": "sec_cache_html" if any(str(rec.get("source_kind") or "") == "sec_cache_html" for rec in snapshots) else "fallback_seed",
        "snapshots": snapshots,
        "plants": plants,
    }


def build_gpre_plant_capacity_history(
    *,
    ticker_root: Optional[Path] = None,
    html_paths: Optional[Iterable[Path]] = None,
) -> Dict[str, Any]:
    if html_paths is not None:
        snapshots: List[Dict[str, Any]] = []
        for path_in in html_paths:
            try:
                candidate = Path(path_in)
            except Exception:
                continue
            snap = _gpre_parse_capacity_snapshot_from_html_path(candidate)
            if snap:
                snapshots.append(snap)
        if snapshots:
            return _gpre_resolve_plant_capacity_history({"snapshots": snapshots, "plants": {}}, ticker_root=ticker_root)
    return _gpre_resolve_plant_capacity_history(None, ticker_root=ticker_root)


def _gpre_pick_capacity_snapshot_for_quarter(
    qd: date,
    *,
    plant_capacity_history: Optional[Dict[str, Any]],
    ticker_root: Optional[Path],
) -> Dict[str, Any]:
    history = _gpre_resolve_plant_capacity_history(plant_capacity_history, ticker_root=ticker_root)
    snapshots = list(history.get("snapshots") or [])
    if not snapshots:
        return {}
    chosen = None
    chosen_mode = ""
    for snapshot in snapshots:
        snapshot_qd = parse_quarter_like(snapshot.get("snapshot_quarter_end"))
        if isinstance(snapshot_qd, date) and snapshot_qd <= qd:
            chosen = snapshot
            chosen_mode = "latest_le_snapshot"
    if chosen is None:
        chosen = snapshots[0]
        chosen_mode = "earliest_snapshot_fallback"
    out = dict(chosen or {})
    out["selection_mode"] = chosen_mode
    return out


def _gpre_active_plants_for_quarter(
    qd: date,
    *,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
    ticker_root: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    history = _gpre_resolve_plant_capacity_history(plant_capacity_history, ticker_root=ticker_root)
    chosen_snapshot = _gpre_pick_capacity_snapshot_for_quarter(
        qd,
        plant_capacity_history=history,
        ticker_root=ticker_root,
    )
    snapshot_year = _safe_int_from_numeric(chosen_snapshot.get("snapshot_year"))
    snapshot_plants = dict(chosen_snapshot.get("plants") or {})
    active: List[Dict[str, Any]] = []
    for plant_key, raw_rec in dict(history.get("plants") or {}).items():
        rec = dict(raw_rec or {})
        inactive_from = rec.get("inactive_from")
        active_through = rec.get("active_through")
        inactive_quarter = quarter_end_from_date(inactive_from) if isinstance(inactive_from, date) else None
        active_through_quarter = quarter_end_from_date(active_through) if isinstance(active_through, date) else None
        if isinstance(inactive_quarter, date) and qd >= inactive_quarter:
            continue
        if isinstance(active_through_quarter, date) and qd > active_through_quarter:
            continue
        snapshot_rec = dict(snapshot_plants.get(plant_key) or {})
        capacity_num = pd.to_numeric(snapshot_rec.get("capacity_mmgy"), errors="coerce")
        capacity_source = "snapshot"
        if pd.isna(capacity_num):
            capacity_num = pd.to_numeric((rec.get("capacity_by_snapshot_year") or {}).get(snapshot_year), errors="coerce")
            if pd.notna(capacity_num):
                capacity_source = "snapshot_year_registry"
        if pd.isna(capacity_num):
            capacity_num = pd.to_numeric(rec.get("fallback_capacity_mmgy"), errors="coerce")
            if pd.notna(capacity_num):
                capacity_source = "fallback_registry"
            else:
                capacity_source = "unavailable"
        active.append(
            {
                **rec,
                "plant_key": plant_key,
                "capacity_mmgy": (None if pd.isna(capacity_num) else float(capacity_num)),
                "capacity_source": capacity_source,
                "snapshot_year": snapshot_year if snapshot_year > 0 else None,
                "snapshot_selection_mode": str(chosen_snapshot.get("selection_mode") or ""),
                "snapshot_source_path": str(chosen_snapshot.get("source_path") or ""),
                "footnotes": dict(((rec.get("footnotes_by_snapshot_year") or {}).get(snapshot_year) or {})),
            }
        )
    return active


def _gpre_bid_delivery_date(label: Any, *, as_of_date: Optional[date] = None) -> Optional[date]:
    txt = re.sub(r"\s+", " ", str(label or "").strip())
    if not txt:
        return None
    ref = as_of_date or date.today()
    low = txt.lower()
    half = None
    if low.startswith("fh "):
        half = "first"
        low = low[3:].strip()
    elif low.startswith("lh "):
        half = "last"
        low = low[3:].strip()
    parts = low.split()
    if not parts:
        return None
    month = _GPRE_BID_MONTHS.get(parts[0])
    if month is None:
        return None
    year: Optional[int] = None
    for part in parts[1:]:
        if re.fullmatch(r"20\d{2}", part):
            year = int(part)
            break
        if re.fullmatch(r"\d{2}", part):
            year = 2000 + int(part)
            break
    if year is None:
        year = int(ref.year)
        if month < int(ref.month) - 1:
            year += 1
    try:
        if half == "first":
            return date(year, month, 15)
        if half == "last":
            next_month = date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
            return next_month - timedelta(days=1)
        next_month = date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
        return next_month - timedelta(days=1)
    except Exception:
        return None


def _decode_gpre_display_number(raw_value: Any, offset: Optional[float]) -> Optional[float]:
    num = pd.to_numeric(raw_value, errors="coerce")
    if pd.isna(num):
        return None
    if offset is None or not np.isfinite(float(offset)):
        return float(num)
    return float(num) - float(offset)


def _extract_gpre_display_number_from_html(cell_html: str, *, offset: Optional[float]) -> Optional[float]:
    match = re.search(r"displayNumber\(\s*([+-]?\d+(?:\.\d+)?)\s*,\s*\d+\s*\)", str(cell_html or ""), re.I)
    if not match:
        return None
    return _decode_gpre_display_number(match.group(1), offset)


def parse_gpre_corn_bids_text(
    text: str,
    *,
    as_of_date: Optional[date] = None,
    source_url: str = "",
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    current_location = ""
    current_region = ""
    line_pat = re.compile(
        r"^Corn\s+(?P<delivery>.+?)\s+(?P<cash>-?\d+(?:\.\d+)?)\s+(?P<basis>-?\d+(?:\.\d+)?)\s+(?P<symbol>@[A-Z0-9]+)(?:\s+(?P<futures>[0-9']+(?:\s+[0-9']+)*)\s+(?P<change>-?[0-9']+(?:\s+[0-9']+)*)\s*)?$",
        re.I,
    )
    for raw_line in str(text or "").splitlines():
        line = re.sub(r"\s+", " ", str(raw_line or "").replace("\xa0", " ").strip())
        if not line:
            continue
        if line in _GPRE_BID_LOCATION_REGION:
            current_location = line
            current_region = _GPRE_BID_LOCATION_REGION.get(line, "")
            continue
        if line.lower().startswith(("commodity ", "cash price", "delivery end")):
            continue
        if not current_location:
            continue
        match = line_pat.match(line)
        if not match:
            continue
        delivery_label = str(match.group("delivery") or "").strip()
        rows.append(
            {
                "location": current_location,
                "region": current_region,
                "delivery_label": delivery_label,
                "delivery_end": _gpre_bid_delivery_date(delivery_label, as_of_date=as_of_date),
                "cash_price": float(match.group("cash")),
                "basis_usd_per_bu": float(match.group("basis")),
                "basis_cents_per_bu": float(match.group("basis")) * 100.0,
                "symbol": str(match.group("symbol") or "").strip(),
                "futures_price_text": str(match.group("futures") or "").strip(),
                "change_text": str(match.group("change") or "").strip(),
                "source_url": source_url,
            }
        )
    return rows


def parse_gpre_corn_bids_html(
    html_text: str,
    *,
    as_of_date: Optional[date] = None,
    source_url: str = "",
) -> List[Dict[str, Any]]:
    if not str(html_text or "").strip():
        return []
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception:
        return []
    offset_match = re.search(r"NoScrapeOffset:\s*([+-]?\d+(?:\.\d+)?)", str(html_text or ""), re.I)
    offset = float(offset_match.group(1)) if offset_match else None
    soup = BeautifulSoup(str(html_text or ""), "html.parser")
    known_locations = set(_GPRE_BID_LOCATION_REGION.keys())
    rows: List[Dict[str, Any]] = []
    for header_tag in soup.find_all("b"):
        location = re.sub(r"\s+", " ", header_tag.get_text(" ", strip=True)).strip()
        if location not in known_locations:
            continue
        header_row = header_tag.find_parent("tr")
        if header_row is None:
            continue
        commodity = ""
        for row_tag in header_row.find_next_siblings("tr"):
            next_loc = row_tag.find("b")
            if next_loc is not None:
                next_loc_txt = re.sub(r"\s+", " ", next_loc.get_text(" ", strip=True)).strip()
                if next_loc_txt in known_locations:
                    break
            cells = row_tag.find_all("td")
            if len(cells) < 5:
                continue
            left_txt = re.sub(r"\s+", " ", cells[0].get_text(" ", strip=True)).replace("\xa0", " ").strip()
            delivery_txt = re.sub(r"\s+", " ", cells[1].get_text(" ", strip=True)).replace("\xa0", " ").strip()
            if left_txt:
                commodity = left_txt
            if commodity.lower() != "corn" or not delivery_txt:
                continue
            cash_val = _extract_gpre_display_number_from_html(str(cells[2]), offset=offset)
            basis_val = _extract_gpre_display_number_from_html(str(cells[4]), offset=offset)
            if cash_val is None or basis_val is None:
                continue
            basis_title = str(cells[4].get("title") or "")
            symbol_match = re.search(r"Basis Month:\s*([@A-Z0-9]+)", basis_title, re.I)
            rows.append(
                {
                    "location": location,
                    "region": _GPRE_BID_LOCATION_REGION.get(location, ""),
                    "delivery_label": delivery_txt.title(),
                    "delivery_end": _gpre_bid_delivery_date(delivery_txt, as_of_date=as_of_date),
                    "cash_price": float(cash_val),
                    "basis_usd_per_bu": float(basis_val),
                    "basis_cents_per_bu": float(basis_val) * 100.0,
                    "symbol": str(symbol_match.group(1) or "").strip() if symbol_match else "",
                    "futures_price_text": "",
                    "change_text": "",
                    "source_url": source_url,
                }
            )
    return rows


def _summarize_gpre_corn_bids_rows(
    rows: Iterable[Dict[str, Any]],
    *,
    as_of_date: Optional[date] = None,
    source_url: str = "",
) -> Dict[str, Any]:
    row_list = [dict(rec) for rec in (rows or []) if isinstance(rec, dict)]
    if not row_list:
        return {"status": "unavailable", "rows": [], "source_url": source_url}
    ref = as_of_date or date.today()
    nearest_rows: List[Dict[str, Any]] = []
    for location in _GPRE_BID_LOCATION_REGION:
        location_rows = [rec for rec in row_list if str(rec.get("location") or "").strip() == location]
        if not location_rows:
            continue
        def _sort_key(rec: Dict[str, Any]) -> Tuple[int, int, str]:
            delivery_end = rec.get("delivery_end")
            if isinstance(delivery_end, date):
                distance = abs((delivery_end - ref).days)
                return (0, distance, str(rec.get("delivery_label") or ""))
            return (1, 0, str(rec.get("delivery_label") or ""))
        nearest_rows.append(sorted(location_rows, key=_sort_key)[0])
    if not nearest_rows:
        return {"status": "unavailable", "rows": row_list, "source_url": source_url}
    cash_values = [float(rec["cash_price"]) for rec in nearest_rows if pd.notna(pd.to_numeric(rec.get("cash_price"), errors="coerce"))]
    basis_values = [float(rec["basis_usd_per_bu"]) for rec in nearest_rows if pd.notna(pd.to_numeric(rec.get("basis_usd_per_bu"), errors="coerce"))]
    latest_delivery = max(
        (rec.get("delivery_end") for rec in nearest_rows if isinstance(rec.get("delivery_end"), date)),
        default=None,
    )
    return {
        "status": "ok",
        "rows": row_list,
        "nearest_rows": nearest_rows,
        "source_url": source_url,
        "as_of": ref,
        "representative_delivery_end": latest_delivery,
        "weighted_nearby_cash_price": (float(np.mean(cash_values)) if cash_values else None),
        "weighted_nearby_basis_usd_per_bu": (float(np.mean(basis_values)) if basis_values else None),
        "weighted_nearby_basis_cents_per_bu": (float(np.mean(basis_values)) * 100.0 if basis_values else None),
        "locations_included": [str(rec.get("location") or "").strip() for rec in nearest_rows],
        "method": "simple plant-level equal-weight of the nearest visible delivery row per active GPRE location",
    }


def _gpre_corn_bids_candidate_urls(entry_html: str) -> List[str]:
    urls: List[str] = []
    for match in re.finditer(r'href=["\'](?P<href>[^"\']+)["\']', str(entry_html or ""), re.I):
        href = html.unescape(str(match.group("href") or "").strip())
        if not href:
            continue
        abs_url = urllib.parse.urljoin(_GPRE_CORN_BIDS_ENTRY_URL, href)
        if "grain.gpreinc.com" not in abs_url.lower():
            continue
        if abs_url not in urls:
            urls.append(abs_url)
    for url in _GPRE_CORN_BIDS_DIRECT_URLS:
        if url not in urls:
            urls.append(url)
    return urls


def _fetch_gpre_corn_bids_html_payload(
    *,
    timeout_seconds: float = 8.0,
) -> Dict[str, Any]:
    entry_html = ""
    entry_error = ""
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
    try:
        entry_req = urllib.request.Request(
            _GPRE_CORN_BIDS_ENTRY_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Codex GPRE bids fetch)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
        )
        with opener.open(entry_req, timeout=float(timeout_seconds)) as resp:
            entry_html = resp.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        entry_error = f"{type(exc).__name__}: {exc}"
    candidate_urls = _gpre_corn_bids_candidate_urls(entry_html)
    if not candidate_urls:
        candidate_urls = list(_GPRE_CORN_BIDS_DIRECT_URLS)
    last_error = entry_error
    for url in candidate_urls:
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; Codex GPRE bids fetch)",
                    "Accept": "text/html,application/xhtml+xml,*/*",
                    "Referer": _GPRE_CORN_BIDS_ENTRY_URL,
                },
            )
            with opener.open(req, timeout=float(timeout_seconds)) as resp:
                html_text = resp.read().decode("utf-8", errors="ignore")
            rows = parse_gpre_corn_bids_html(html_text, source_url=url)
            if rows:
                return {
                    "status": "ok",
                    "source_url": url,
                    "entry_url": _GPRE_CORN_BIDS_ENTRY_URL,
                    "entry_html_length": len(entry_html),
                    "html_text": html_text,
                    "rows": rows,
                }
            last_error = f"ValueError: no parsable corn bid rows at {url}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            continue
    return {
        "status": "unavailable",
        "source_url": candidate_urls[0] if candidate_urls else _GPRE_CORN_BIDS_DIRECT_URLS[0],
        "entry_url": _GPRE_CORN_BIDS_ENTRY_URL,
        "error": last_error,
        "html_text": "",
        "rows": [],
    }


def fetch_gpre_corn_bids_snapshot(
    *,
    as_of_date: Optional[date] = None,
    timeout_seconds: float = 8.0,
) -> Dict[str, Any]:
    payload = _fetch_gpre_corn_bids_html_payload(timeout_seconds=timeout_seconds)
    row_list = [
        dict(rec)
        for rec in list(payload.get("rows") or [])
        if isinstance(rec, dict)
    ]
    if not row_list:
        return {
            "status": "unavailable",
            "rows": [],
            "source_url": str(payload.get("source_url") or _GPRE_CORN_BIDS_DIRECT_URLS[0]),
            "entry_url": str(payload.get("entry_url") or _GPRE_CORN_BIDS_ENTRY_URL),
            "error": str(payload.get("error") or ""),
        }
    row_list = parse_gpre_corn_bids_html(
        str(payload.get("html_text") or ""),
        as_of_date=as_of_date,
        source_url=str(payload.get("source_url") or _GPRE_CORN_BIDS_DIRECT_URLS[0]),
    )
    summary = _summarize_gpre_corn_bids_rows(
        row_list,
        as_of_date=as_of_date,
        source_url=str(payload.get("source_url") or _GPRE_CORN_BIDS_DIRECT_URLS[0]),
    )
    summary["html_length"] = len(str(payload.get("html_text") or ""))
    summary["entry_url"] = str(payload.get("entry_url") or _GPRE_CORN_BIDS_ENTRY_URL)
    summary["source_kind"] = "remote_html"
    return summary


def download_gpre_corn_bids_snapshot(
    ticker_root: Path,
    *,
    as_of_date: Optional[date] = None,
    timeout_seconds: float = 8.0,
) -> Dict[str, Any]:
    root = Path(ticker_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    storage_root = root / _GPRE_CORN_BIDS_DIRNAME
    storage_root.mkdir(parents=True, exist_ok=True)
    html_path = storage_root / _GPRE_CORN_BIDS_HTML_FILENAME
    csv_path = storage_root / _GPRE_CORN_BIDS_CSV_FILENAME
    payload = _fetch_gpre_corn_bids_html_payload(timeout_seconds=timeout_seconds)
    html_text = str(payload.get("html_text") or "")
    source_url = str(payload.get("source_url") or "")
    if str(payload.get("status") or "") != "ok" or not html_text.strip():
        return {
            "status": "unavailable",
            "html_path": html_path,
            "csv_path": csv_path,
            "source_url": source_url or _GPRE_CORN_BIDS_DIRECT_URLS[0],
            "entry_url": str(payload.get("entry_url") or _GPRE_CORN_BIDS_ENTRY_URL),
            "error": str(payload.get("error") or ""),
        }
    html_path.write_text(html_text, encoding="utf-8")
    parsed_rows = parse_gpre_corn_bids_html(
        html_text,
        as_of_date=as_of_date,
        source_url=source_url,
    )
    pd.DataFrame(parsed_rows).to_csv(csv_path, index=False)
    summary = _summarize_gpre_corn_bids_rows(
        parsed_rows,
        as_of_date=as_of_date,
        source_url=source_url,
    )
    summary.update(
        {
            "html_path": html_path,
            "csv_path": csv_path,
            "entry_url": str(payload.get("entry_url") or _GPRE_CORN_BIDS_ENTRY_URL),
            "html_length": len(html_text),
            "source_kind": "downloaded_html",
        }
    )
    return summary


def _refresh_gpre_corn_bids_download(
    ticker_root: Path,
    *,
    refresh: bool,
) -> Dict[str, Any]:
    if not bool(refresh):
        return {"status": "skipped", "reason": "refresh_disabled"}
    try:
        return download_gpre_corn_bids_snapshot(
            ticker_root,
            as_of_date=date.today(),
            timeout_seconds=8.0,
        )
    except Exception as exc:
        return {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}


def _load_local_gpre_corn_bids_snapshot(
    *,
    ticker_root: Optional[Path],
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    candidate_paths: List[Path] = []
    if isinstance(ticker_root, Path):
        candidate_paths.extend(
            [
                ticker_root / _GPRE_CORN_BIDS_DIRNAME / _GPRE_CORN_BIDS_HTML_FILENAME,
                ticker_root / _GPRE_CORN_BIDS_HTML_FILENAME,
                ticker_root / "grain_gpre_home.html",
                ticker_root.parent / "grain_gpre_home.html",
            ]
        )
    try:
        candidate_paths.append(Path.cwd() / "grain_gpre_home.html")
    except Exception:
        pass
    seen: Set[str] = set()
    for raw_path in candidate_paths:
        try:
            path = Path(raw_path).resolve()
        except Exception:
            path = Path(raw_path)
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        if not path.exists() or not path.is_file():
            continue
        try:
            html_text = path.read_text(encoding="utf-8", errors="ignore")
            rows = parse_gpre_corn_bids_html(html_text, as_of_date=as_of_date, source_url=str(path))
            if not rows:
                continue
            summary = _summarize_gpre_corn_bids_rows(rows, as_of_date=as_of_date, source_url=str(path))
            summary["html_length"] = len(html_text)
            summary["source_kind"] = "local_html"
            return summary
        except Exception:
            continue
    return {
        "status": "unavailable",
        "rows": [],
        "source_url": "",
        "source_kind": "none",
    }


def _resolve_gpre_corn_bids_snapshot(
    bids_snapshot: Optional[Dict[str, Any]],
    *,
    ticker_root: Optional[Path],
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    effective = bids_snapshot if isinstance(bids_snapshot, dict) else {}
    if str(effective.get("status") or "").strip().lower() == "ok":
        return effective
    if isinstance(ticker_root, Path):
        return _load_local_gpre_corn_bids_snapshot(
            ticker_root=ticker_root,
            as_of_date=as_of_date,
        )
    return effective


def _basis_region_from_series_key(series_key: Any) -> str:
    txt = str(series_key or "").strip().lower()
    if txt.startswith("corn_basis_"):
        return txt[len("corn_basis_") :]
    return txt


def _gpre_basis_fallback_regions(region: str) -> List[str]:
    out: List[str] = []
    for series_key in _GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.get(str(region or "").strip().lower(), tuple()):
        basis_region = _basis_region_from_series_key(series_key)
        if basis_region and basis_region not in out:
            out.append(basis_region)
    return out


def _gpre_select_bid_row_for_target(
    rows: Iterable[Dict[str, Any]],
    *,
    location: str,
    target_date: date,
) -> Optional[Dict[str, Any]]:
    location_rows = [
        dict(rec)
        for rec in (rows or [])
        if isinstance(rec, dict) and str(rec.get("location") or "").strip() == str(location or "").strip()
    ]
    if not location_rows:
        return None

    def _sort_key(rec: Dict[str, Any]) -> Tuple[int, int, int, str]:
        delivery_end = rec.get("delivery_end")
        if isinstance(delivery_end, date):
            distance = abs((delivery_end - target_date).days)
            future_bias = 0 if delivery_end >= target_date else 1
            return (0, distance, future_bias, str(rec.get("delivery_label") or ""))
        return (1, 0, 0, str(rec.get("delivery_label") or ""))

    return sorted(location_rows, key=_sort_key)[0]


def _gpre_official_current_forward_basis_payload(
    rows: Iterable[Dict[str, Any]],
    *,
    target_date: date,
    target_quarter_end: date,
    as_of_date: Optional[date],
    ticker_root: Optional[Path],
    bids_snapshot: Optional[Dict[str, Any]],
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows_df = _market_rows_df(rows)
    active_plants = _gpre_active_plants_for_quarter(
        target_quarter_end,
        plant_capacity_history=plant_capacity_history,
        ticker_root=ticker_root,
    )
    if not active_plants:
        return {
            "official_weighted_corn_basis_usd_per_bu": None,
            "weighted_ams_basis_proxy_usd_per_bu": None,
            "official_corn_basis_source_kind": "weighted_ams_proxy",
            "official_corn_basis_source_label": "weighted AMS basis proxy",
            "official_corn_basis_provenance": "No active GPRE plants resolved for the target quarter.",
            "official_actual_bid_plant_count": 0,
            "official_fallback_plant_count": 0,
            "official_missing_plant_count": 0,
            "component_rows": [],
            "source_url": "",
            "source_kind": "none",
        }
    total_capacity = float(
        sum(
            max(float(pd.to_numeric(rec.get("capacity_mmgy"), errors="coerce") or 0.0), 0.0)
            for rec in active_plants
        )
    )
    if total_capacity <= 0.0:
        return {
            "official_weighted_corn_basis_usd_per_bu": None,
            "weighted_ams_basis_proxy_usd_per_bu": None,
            "official_corn_basis_source_kind": "weighted_ams_proxy",
            "official_corn_basis_source_label": "weighted AMS basis proxy",
            "official_corn_basis_provenance": "No active GPRE plant capacity resolved for the target quarter.",
            "official_actual_bid_plant_count": 0,
            "official_fallback_plant_count": 0,
            "official_missing_plant_count": 0,
            "component_rows": [],
            "source_url": "",
            "source_kind": "none",
        }
    effective_snapshot = _resolve_gpre_corn_bids_snapshot(
        bids_snapshot,
        ticker_root=ticker_root,
        as_of_date=as_of_date,
    )
    snapshot_rows = effective_snapshot.get("rows")
    if not isinstance(snapshot_rows, list) or not snapshot_rows:
        snapshot_rows = effective_snapshot.get("nearest_rows")
    if not isinstance(snapshot_rows, list):
        snapshot_rows = []
    quarter_avg_maps = {
        region: _quarter_avg_map(rows_df, f"corn_basis_{region}")
        for region in _GPRE_BASIS_REGIONS
    }
    reference_rows = _latest_ams_basis_reference_rows(
        rows_df,
        regions=_GPRE_BASIS_REGIONS,
        as_of_date=as_of_date,
        quarter_avg_maps=quarter_avg_maps,
        lookback_days=21,
    )
    reference_map = {
        str(rec.get("region") or "").strip().lower(): dict(rec)
        for rec in reference_rows
        if str(rec.get("region") or "").strip()
    }
    component_rows: List[Dict[str, Any]] = []
    official_sum = 0.0
    official_cov = 0.0
    proxy_sum = 0.0
    proxy_cov = 0.0
    actual_count = 0
    fallback_count = 0
    missing_count = 0
    for plant in active_plants:
        location = str(plant.get("location") or "").strip()
        region = str(plant.get("region") or "").strip().lower()
        plant_capacity = max(float(pd.to_numeric(plant.get("capacity_mmgy"), errors="coerce") or 0.0), 0.0)
        plant_weight = (plant_capacity / total_capacity) if total_capacity > 0.0 else 0.0
        bid_rec = _gpre_select_bid_row_for_target(snapshot_rows, location=location, target_date=target_date)
        bid_basis = pd.to_numeric((bid_rec or {}).get("basis_usd_per_bu"), errors="coerce")
        proxy_basis = None
        proxy_region = ""
        proxy_method = ""
        proxy_note = ""
        for fallback_region in _gpre_basis_fallback_regions(region):
            ref_rec = dict(reference_map.get(fallback_region) or {})
            ref_basis = pd.to_numeric(ref_rec.get("basis_usd_per_bu"), errors="coerce")
            if pd.isna(ref_basis):
                continue
            proxy_basis = float(ref_basis)
            proxy_region = fallback_region
            proxy_method = str(ref_rec.get("reference_method") or "")
            if fallback_region != region:
                proxy_note = f"{region} uses AMS fallback {fallback_region}"
            break
        if proxy_basis is not None and plant_weight > 0.0:
            proxy_sum += plant_weight * float(proxy_basis)
            proxy_cov += plant_weight
        source_kind = "missing"
        note_bits: List[str] = []
        chosen_basis = None
        if pd.notna(bid_basis):
            chosen_basis = float(bid_basis)
            source_kind = "actual_gpre_bid"
            actual_count += 1
            delivery_label = str((bid_rec or {}).get("delivery_label") or "").strip()
            if delivery_label:
                note_bits.append(f"actual bid {delivery_label}")
        elif proxy_basis is not None:
            chosen_basis = float(proxy_basis)
            source_kind = "ams_proxy_fallback"
            fallback_count += 1
            if proxy_note:
                note_bits.append(proxy_note)
            if proxy_method:
                note_bits.append(proxy_method)
        else:
            missing_count += 1
            note_bits.append("No actual bid or AMS proxy available")
        if chosen_basis is not None and plant_weight > 0.0:
            official_sum += plant_weight * float(chosen_basis)
            official_cov += plant_weight
        component_rows.append(
            {
                "location": location,
                "region": region,
                "weight": plant_weight,
                "source_kind": source_kind,
                "basis_usd_per_bu": chosen_basis,
                "basis_cents_per_bu": (None if chosen_basis is None else float(chosen_basis) * 100.0),
                "proxy_basis_usd_per_bu": proxy_basis,
                "proxy_basis_cents_per_bu": (None if proxy_basis is None else float(proxy_basis) * 100.0),
                "proxy_region": proxy_region,
                "proxy_method": proxy_method,
                "delivery_label": str((bid_rec or {}).get("delivery_label") or "").strip(),
                "fallback_note": " | ".join(bit for bit in note_bits if bit),
            }
        )
    official_basis = (official_sum / official_cov) if official_cov > 0.0 else None
    weighted_proxy_basis = (proxy_sum / proxy_cov) if proxy_cov > 0.0 else None
    if actual_count > 0 and fallback_count == 0 and missing_count == 0:
        basis_source_kind = "actual_gpre_bids"
        basis_source_label = "actual GPRE plant-bid basis"
    elif actual_count > 0:
        basis_source_kind = "actual_gpre_bids_with_ams_fallback"
        basis_source_label = "actual GPRE plant-bid basis with AMS fallback"
    else:
        basis_source_kind = "weighted_ams_proxy"
        basis_source_label = "weighted AMS basis proxy"
    source_kind_txt = str(effective_snapshot.get("source_kind") or "").strip() or "live_html"
    source_url_txt = str(effective_snapshot.get("source_url") or "").strip()
    provenance = (
        f"{basis_source_label.title()} for {actual_count}/{int(len(active_plants))} plants"
        if actual_count > 0
        else "No actual current GPRE bids available; using weighted AMS basis proxy"
    )
    if fallback_count > 0:
        provenance += f"; AMS fallback for {fallback_count}/{int(len(active_plants))} plants"
    if missing_count > 0:
        provenance += f"; missing basis for {missing_count}/{int(len(active_plants))} plants"
    if source_url_txt:
        provenance += f". Source: {source_kind_txt}."
    else:
        provenance += "."
    return {
        "official_weighted_corn_basis_usd_per_bu": official_basis,
        "weighted_ams_basis_proxy_usd_per_bu": weighted_proxy_basis,
        "official_corn_basis_source_kind": basis_source_kind,
        "official_corn_basis_source_label": basis_source_label,
        "official_corn_basis_provenance": provenance,
        "official_actual_bid_plant_count": int(actual_count),
        "official_fallback_plant_count": int(fallback_count),
        "official_missing_plant_count": int(missing_count),
        "official_active_capacity_mmgy": total_capacity,
        "component_rows": component_rows,
        "source_url": source_url_txt,
        "source_kind": source_kind_txt,
    }


def _gpre_bid_region_basis_rows(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(snapshot, dict):
        return []
    row_list = snapshot.get("nearest_rows")
    if not isinstance(row_list, list) or not row_list:
        row_list = snapshot.get("rows")
    if not isinstance(row_list, list) or not row_list:
        return []
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for raw_rec in row_list:
        if not isinstance(raw_rec, dict):
            continue
        region = str(raw_rec.get("region") or "").strip().lower()
        if not region:
            continue
        grouped.setdefault(region, []).append(raw_rec)
    out_rows: List[Dict[str, Any]] = []
    for region, recs in grouped.items():
        vals = [pd.to_numeric(rec.get("basis_usd_per_bu"), errors="coerce") for rec in recs]
        nums = [float(val) for val in vals if pd.notna(val)]
        if not nums:
            continue
        locations = sorted({str(rec.get("location") or "").strip() for rec in recs if str(rec.get("location") or "").strip()})
        out_rows.append(
            {
                "region": region,
                "basis_usd_per_bu": float(sum(nums) / len(nums)),
                "basis_cents_per_bu": float(sum(nums) / len(nums)) * 100.0,
                "locations": locations,
                "location_count": len(locations),
            }
        )
    return sorted(out_rows, key=lambda rec: str(rec.get("region") or ""))


def _latest_ams_basis_reference_rows(
    rows: Iterable[Dict[str, Any]],
    *,
    regions: Iterable[str],
    as_of_date: Optional[date] = None,
    quarter_avg_maps: Optional[Dict[str, Dict[date, float]]] = None,
    lookback_days: int = 21,
) -> List[Dict[str, Any]]:
    df = _market_rows_df(rows)
    ref_date = as_of_date or date.today()
    out_rows: List[Dict[str, Any]] = []
    quarter_avg_maps = dict(quarter_avg_maps or {})
    for region in regions:
        obs_df = _series_observation_df(df, f"corn_basis_{region}")
        if not obs_df.empty:
            obs_df = obs_df[obs_df["observation_date"].dt.date <= ref_date].copy()
        if not obs_df.empty:
            latest_obs = pd.Timestamp(obs_df["observation_date"].max()).date()
            window_start = latest_obs - timedelta(days=max(int(lookback_days), 1) - 1)
            window = obs_df[
                (obs_df["observation_date"].dt.date >= window_start)
                & (obs_df["observation_date"].dt.date <= latest_obs)
            ].copy()
            vals = pd.to_numeric(window.get("price_value"), errors="coerce").dropna()
            if not vals.empty:
                basis_usd = float(vals.mean())
                out_rows.append(
                    {
                        "region": region,
                        "basis_usd_per_bu": basis_usd,
                        "basis_cents_per_bu": basis_usd * 100.0,
                        "reference_as_of": latest_obs,
                        "reference_method": f"Latest {max(int(lookback_days), 1)}-day AMS basis average ending {latest_obs.isoformat()}",
                    }
                )
                continue
        valid_quarters = [
            (qd, pd.to_numeric(val, errors="coerce"))
            for qd, val in (quarter_avg_maps.get(region) or {}).items()
            if isinstance(qd, date)
        ]
        valid_quarters = [(qd, float(val)) for qd, val in valid_quarters if pd.notna(val)]
        if valid_quarters:
            qd, basis_usd = max(valid_quarters, key=lambda item: item[0])
            out_rows.append(
                {
                    "region": region,
                    "basis_usd_per_bu": basis_usd,
                    "basis_cents_per_bu": basis_usd * 100.0,
                    "reference_as_of": qd,
                    "reference_method": f"Latest quarter-average AMS basis ({_quarter_label(qd)})",
                }
            )
            continue
        out_rows.append(
            {
                "region": region,
                "basis_usd_per_bu": None,
                "basis_cents_per_bu": None,
                "reference_as_of": None,
                "reference_method": "No AMS reference available",
            }
        )
    return out_rows


def _matched_process_snapshot_for_bounds(
    *,
    ethanol_df: pd.DataFrame,
    gas_df: pd.DataFrame,
    corn_daily_df: pd.DataFrame,
    window_start: date,
    window_end: date,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
) -> Dict[str, Any]:
    ethanol_window = ethanol_df[
        (ethanol_df["observation_date"].dt.date >= window_start)
        & (ethanol_df["observation_date"].dt.date <= window_end)
    ].copy()
    gas_window = gas_df[
        (gas_df["observation_date"].dt.date >= window_start)
        & (gas_df["observation_date"].dt.date <= window_end)
    ].copy()
    if ethanol_window.empty or gas_window.empty or corn_daily_df is None or corn_daily_df.empty:
        return {
            "current_process": {},
            "weekly_rows": [],
            "weeks_included": 0,
            "process_as_of": None,
            "process_status": "no_data",
        }
    gas_by_date = {
        pd.Timestamp(rec["observation_date"]).date(): float(rec["price_value"])
        for rec in gas_window.to_dict("records")
    }
    weekly_rows: List[Dict[str, Any]] = []
    for rec in ethanol_window.to_dict("records"):
        week_end = pd.Timestamp(rec["observation_date"]).date()
        ethanol_price = pd.to_numeric(rec.get("price_value"), errors="coerce")
        gas_price = gas_by_date.get(week_end)
        corn_price = _average_daily_corn_for_week(corn_daily_df, week_end)
        if pd.isna(ethanol_price) or gas_price is None or corn_price is None:
            continue
        ethanol_revenue = None
        feedstock_cost = None
        natural_gas_burden = None
        simple_crush = None
        if ethanol_yield is not None and natural_gas_usage is not None:
            ethanol_revenue = float(ethanol_yield) * float(ethanol_price)
            feedstock_cost = -float(corn_price)
            natural_gas_burden = -((float(natural_gas_usage) / 1_000_000.0) * float(ethanol_yield) * float(gas_price))
            simple_crush = float(ethanol_revenue + feedstock_cost + natural_gas_burden)
        weekly_rows.append(
            {
                "week_end": week_end,
                "ethanol_price": float(ethanol_price),
                "corn_price": float(corn_price),
                "natural_gas_price": float(gas_price),
                "ethanol_revenue": ethanol_revenue,
                "feedstock_cost": feedstock_cost,
                "natural_gas_burden": natural_gas_burden,
                "simple_crush": simple_crush,
            }
        )
    if not weekly_rows:
        return {
            "current_process": {},
            "weekly_rows": [],
            "weeks_included": 0,
            "process_as_of": None,
            "process_status": "no_data",
        }

    def _avg(key: str) -> Optional[float]:
        vals = [pd.to_numeric(row.get(key), errors="coerce") for row in weekly_rows]
        nums = [float(v) for v in vals if pd.notna(v)]
        if not nums:
            return None
        return float(sum(nums) / len(nums))

    return {
        "current_process": {
            "ethanol_revenue": _avg("ethanol_revenue"),
            "feedstock_cost": _avg("feedstock_cost"),
            "natural_gas_burden": _avg("natural_gas_burden"),
            "simple_crush": _avg("simple_crush"),
        },
        "weekly_rows": weekly_rows,
        "weeks_included": len(weekly_rows),
        "process_as_of": max(row["week_end"] for row in weekly_rows),
        "process_status": "ok",
    }


def _build_quarter_market_process_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    window_start: date,
    window_end: date,
    display_quarter: date,
    calendar_quarter: date,
    status_when_available: str,
    no_data_message: str,
) -> Dict[str, Any]:
    df = _market_rows_df(rows)
    ethanol_df = _series_observation_df(df, "ethanol_nebraska")
    gas_df = _series_observation_df(df, "nymex_gas")
    corn_daily_df = _series_observation_df(df, "corn_nebraska")

    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
        natural_gas_usage_num = float(natural_gas_usage) if natural_gas_usage is not None else None
    except Exception:
        ethanol_yield_num = None
        natural_gas_usage_num = None

    market_meta = {
        "corn_price": _series_window_market_meta(
            corn_daily_df,
            window_start=window_start,
            window_end=window_end,
            cadence="daily",
        ),
        "ethanol_price": _series_window_market_meta(
            ethanol_df,
            window_start=window_start,
            window_end=window_end,
            cadence="weekly",
        ),
        "natural_gas_price": _series_window_market_meta(
            gas_df,
            window_start=window_start,
            window_end=window_end,
            cadence="weekly",
        ),
    }
    market_values = {
        key: meta.get("value")
        for key, meta in market_meta.items()
        if isinstance(meta, dict)
    }
    market_as_of_candidates = [
        meta.get("as_of")
        for meta in market_meta.values()
        if isinstance(meta, dict) and isinstance(meta.get("as_of"), date)
    ]
    process_snapshot = _matched_process_snapshot_for_bounds(
        ethanol_df=ethanol_df,
        gas_df=gas_df,
        corn_daily_df=corn_daily_df,
        window_start=window_start,
        window_end=window_end,
        ethanol_yield=ethanol_yield_num,
        natural_gas_usage=natural_gas_usage_num,
    )
    market_available = any(value is not None for value in market_values.values())
    process_as_of = process_snapshot.get("process_as_of")
    snapshot_as_of = max([*market_as_of_candidates, process_as_of] if isinstance(process_as_of, date) else market_as_of_candidates, default=None)
    message = "" if market_available else no_data_message
    if market_available and str(process_snapshot.get("process_status") or "") != "ok":
        message = "Current-quarter market inputs may be partially available, but process proxy remains unavailable until overlapping corn, ethanol and gas observations exist."
    return {
        "quarter_start": window_start,
        "quarter_end": window_end,
        "display_quarter": display_quarter,
        "calendar_quarter": calendar_quarter,
        "as_of": snapshot_as_of,
        "weeks_included": int(process_snapshot.get("weeks_included") or 0),
        "market_meta": market_meta,
        "current_market": market_values if market_available else {},
        "current_process": process_snapshot.get("current_process") if isinstance(process_snapshot.get("current_process"), dict) else {},
        "weekly_rows": process_snapshot.get("weekly_rows") if isinstance(process_snapshot.get("weekly_rows"), list) else [],
        "process_as_of": process_as_of if isinstance(process_as_of, date) else None,
        "process_status": str(process_snapshot.get("process_status") or "no_data"),
        "status": status_when_available if market_available else "no_data",
        "message": message,
    }


def build_current_qtd_simple_crush_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    q_start, q_end = calendar_quarter_bounds(as_of_date=as_of_date)
    return _build_quarter_market_process_snapshot(
        rows,
        ethanol_yield=ethanol_yield,
        natural_gas_usage=natural_gas_usage,
        window_start=q_start,
        window_end=q_end,
        display_quarter=q_end,
        calendar_quarter=q_end,
        status_when_available="ok_current",
        no_data_message="No current-quarter market observations available.",
    )


def build_prior_quarter_simple_crush_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    _, current_q_end = calendar_quarter_bounds(as_of_date=as_of_date)
    prior_start, prior_end = prior_calendar_quarter_bounds(as_of_date=as_of_date)
    return _build_quarter_market_process_snapshot(
        rows,
        ethanol_yield=ethanol_yield,
        natural_gas_usage=natural_gas_usage,
        window_start=prior_start,
        window_end=prior_end,
        display_quarter=prior_end,
        calendar_quarter=current_q_end,
        status_when_available="ok_prior",
        no_data_message="No prior-quarter market observations available.",
    )


def build_simple_crush_history_series(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
    lookback_weeks: Optional[int] = 104,
    start_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    df = _market_rows_df(rows)
    ethanol_df = _series_observation_df(df, "ethanol_nebraska")
    gas_df = _series_observation_df(df, "nymex_gas")
    corn_daily_df = _series_observation_df(df, "corn_nebraska")
    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
        natural_gas_usage_num = float(natural_gas_usage) if natural_gas_usage is not None else None
    except Exception:
        ethanol_yield_num = None
        natural_gas_usage_num = None
    if ethanol_yield_num is None or natural_gas_usage_num is None or ethanol_yield_num == 0:
        return []
    end_date = as_of_date if isinstance(as_of_date, date) else date.today()
    if isinstance(start_date, date):
        window_start = start_date
        weeks = max(int(lookback_weeks or 0), 1) if lookback_weeks is not None else None
    else:
        weeks = max(int(lookback_weeks or 0), 1)
        window_start = end_date - timedelta(days=(weeks * 7) + 6)
    snapshot = _matched_process_snapshot_for_bounds(
        ethanol_df=ethanol_df,
        gas_df=gas_df,
        corn_daily_df=corn_daily_df,
        window_start=window_start,
        window_end=end_date,
        ethanol_yield=ethanol_yield_num,
        natural_gas_usage=natural_gas_usage_num,
    )
    weekly_rows = list(snapshot.get("weekly_rows") or [])
    out_rows: List[Dict[str, Any]] = []
    for rec in weekly_rows:
        week_end = rec.get("week_end")
        simple_crush = pd.to_numeric(rec.get("simple_crush"), errors="coerce")
        if not isinstance(week_end, date) or pd.isna(simple_crush):
            continue
        out_rows.append(
            {
                "week_end": week_end,
                "simple_crush_per_bushel": float(simple_crush),
                "simple_crush_per_gal": float(simple_crush) / float(ethanol_yield_num),
            }
    )
    out_rows = sorted(out_rows, key=lambda rec: rec.get("week_end") or date.min)
    if weeks is not None and len(out_rows) > weeks:
        out_rows = out_rows[-weeks:]
    return out_rows


def _gpre_official_proxy_weekly_rows(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    window_start: date,
    window_end: date,
    ticker_root: Optional[Path] = None,
    basis_override_payload: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
        natural_gas_usage_num = float(natural_gas_usage) if natural_gas_usage is not None else None
    except Exception:
        ethanol_yield_num = None
        natural_gas_usage_num = None
    if ethanol_yield_num is None or natural_gas_usage_num is None or ethanol_yield_num == 0:
        return []
    # Normalize once per call and keep the downstream series maps/weight lookups local.
    # This path is used by the GPRE overlay snapshots, history series, and fitted-model
    # preview bundle, so repeated DataFrame reconstruction is disproportionately costly.
    rows_df = _market_rows_df(rows)
    ethanol_obs_maps = {
        key: _series_observation_value_map(rows_df, key)
        for keys in _GPRE_OFFICIAL_ETHANOL_SERIES_CANDIDATES.values()
        for key in keys
    }
    ethanol_obs_dates = {
        key: sorted(dt for dt in obs_map.keys() if isinstance(dt, date))
        for key, obs_map in ethanol_obs_maps.items()
    }
    gas_map = _series_observation_value_map(rows_df, "nymex_gas")
    cbot_map = _series_observation_value_map(rows_df, "cbot_corn_usd_per_bu")
    basis_obs_maps = {
        key: _series_observation_value_map(rows_df, key)
        for keys in _GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.values()
        for key in keys
    }
    basis_obs_dates = {
        region: sorted(dt for dt in obs_map.keys() if isinstance(dt, date))
        for region, obs_map in basis_obs_maps.items()
    }

    quarter_weight_cache: Dict[date, Dict[str, float]] = {}

    def _weights_for_date(obs_date_in: date) -> Dict[str, float]:
        quarter_end = quarter_end_from_date(obs_date_in)
        cached = quarter_weight_cache.get(quarter_end)
        if cached is None:
            cached = _gpre_official_market_weights_for_quarter(
                quarter_end,
                ticker_root=ticker_root,
                plant_capacity_history=plant_capacity_history,
            )
            quarter_weight_cache[quarter_end] = dict(cached)
        return cached

    def _weighted_value_for_date(
        obs_date_in: date,
        *,
        series_maps: Dict[str, Dict[date, float]],
        series_dates: Dict[str, List[date]],
        series_candidates: Dict[str, tuple[str, ...]],
        max_lag_days: int,
        validator: Optional[Callable[[float, str], Optional[str]]] = None,
    ) -> Tuple[Optional[float], float]:
        weights = _weights_for_date(obs_date_in)
        total_weight = float(sum(max(float(weights.get(region) or 0.0), 0.0) for region in weights))
        if total_weight <= 0.0:
            return None, 0.0
        covered_weight = 0.0
        weighted_sum = 0.0
        for region, raw_weight in weights.items():
            weight = max(float(raw_weight or 0.0), 0.0)
            if weight <= 0.0:
                continue
            value_num, _, _ = _gpre_select_candidate_observation_value(
                series_maps,
                series_dates,
                series_candidates.get(region, tuple()),
                obs_date_in,
                max_lag_days=max_lag_days,
                validator=validator,
            )
            if value_num is None:
                continue
            covered_weight += weight
            weighted_sum += weight * float(value_num)
        if covered_weight <= 0.0:
            return None, 0.0
        return float(weighted_sum / covered_weight), (covered_weight / total_weight if total_weight > 0 else 1.0)

    basis_override_num = pd.to_numeric(
        (basis_override_payload or {}).get("official_weighted_corn_basis_usd_per_bu"),
        errors="coerce",
    )
    basis_override_source_kind = str((basis_override_payload or {}).get("official_corn_basis_source_kind") or "").strip()
    basis_override_source_label = str((basis_override_payload or {}).get("official_corn_basis_source_label") or "").strip()
    basis_override_provenance = str((basis_override_payload or {}).get("official_corn_basis_provenance") or "").strip()
    actual_bid_plant_count = int((basis_override_payload or {}).get("official_actual_bid_plant_count") or 0)
    fallback_plant_count = int((basis_override_payload or {}).get("official_fallback_plant_count") or 0)
    ethanol_candidate_dates = sorted(
        {
            dt
            for dates in ethanol_obs_dates.values()
            for dt in dates
            if isinstance(dt, date) and window_start <= dt <= window_end
        }
    )
    week_ends = sorted(
        dt
        for dt in (set(gas_map.keys()) | set(cbot_map.keys()) | set(ethanol_candidate_dates))
        if isinstance(dt, date) and window_start <= dt <= window_end
    )
    ethanol_anchor_cache: Dict[date, Optional[float]] = {}
    out: List[Dict[str, Any]] = []
    for week_end in week_ends:
        gas_price = pd.to_numeric(gas_map.get(week_end), errors="coerce")
        cbot_corn = pd.to_numeric(cbot_map.get(week_end), errors="coerce")
        ethanol_anchor_value = ethanol_anchor_cache.get(week_end)
        if week_end not in ethanol_anchor_cache:
            ethanol_anchor_value = _gpre_ethanol_anchor_observation_value(
                ethanol_obs_maps,
                ethanol_obs_dates,
                week_end,
                max_lag_days=14,
            )
            ethanol_anchor_cache[week_end] = ethanol_anchor_value
        ethanol_price, ethanol_cov = _weighted_value_for_date(
            week_end,
            series_maps=ethanol_obs_maps,
            series_dates=ethanol_obs_dates,
            series_candidates=_GPRE_OFFICIAL_ETHANOL_SERIES_CANDIDATES,
            max_lag_days=14,
            validator=lambda value_num, series_key, anchor_value=ethanol_anchor_value: _gpre_ethanol_implausibility_note(
                value_num,
                series_key=series_key,
                anchor_value=anchor_value,
            ),
        )
        east_components: List[float] = []
        for east_key in ("ethanol_illinois", "ethanol_indiana"):
            east_val, _, _ = _gpre_select_candidate_observation_value(
                ethanol_obs_maps,
                ethanol_obs_dates,
                (east_key,),
                week_end,
                max_lag_days=14,
                validator=lambda value_num, series_key=east_key, anchor_value=ethanol_anchor_value: _gpre_ethanol_implausibility_note(
                    value_num,
                    series_key=series_key,
                    anchor_value=anchor_value,
                ),
            )
            if east_val is not None:
                east_components.append(float(east_val))
        east_avg = (float(sum(east_components) / len(east_components)) if east_components else None)
        ethanol_geo_spread = (
            None
            if east_avg is None or ethanol_price is None
            else float(east_avg) - float(ethanol_price)
        )
        if pd.notna(basis_override_num):
            weighted_basis = float(basis_override_num)
            basis_cov = 1.0
            basis_source_kind = basis_override_source_kind or "weighted_ams_proxy"
            basis_source_label = basis_override_source_label or "weighted AMS basis proxy"
            basis_provenance = basis_override_provenance
        else:
            weighted_basis, basis_cov = _weighted_value_for_date(
                week_end,
                series_maps=basis_obs_maps,
                series_dates=basis_obs_dates,
                series_candidates=_GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES,
                max_lag_days=7,
            )
            basis_source_kind = "weighted_ams_proxy"
            basis_source_label = "weighted AMS basis proxy"
            basis_provenance = "Weighted AMS basis proxy using mapped state/regional basis series and deterministic fallbacks."
        delivered_corn = None if pd.isna(cbot_corn) or weighted_basis is None else float(cbot_corn) + float(weighted_basis)
        ethanol_revenue = None if ethanol_price is None else float(ethanol_yield_num) * float(ethanol_price)
        feedstock_cost = None if delivered_corn is None else -float(delivered_corn)
        natural_gas_burden = None
        if pd.notna(gas_price):
            natural_gas_burden = -((float(natural_gas_usage_num) / 1_000_000.0) * float(ethanol_yield_num) * float(gas_price))
        simple_crush = None
        if ethanol_revenue is not None and feedstock_cost is not None and natural_gas_burden is not None:
            simple_crush = float(ethanol_revenue + feedstock_cost + natural_gas_burden)
        if (
            ethanol_price is None
            and pd.isna(gas_price)
            and pd.isna(cbot_corn)
            and weighted_basis is None
            and simple_crush is None
        ):
            continue
        out.append(
            {
                "week_end": week_end,
                "ethanol_price": (None if ethanol_price is None else float(ethanol_price)),
                "ethanol_east_avg_usd_per_gal": (None if east_avg is None else float(east_avg)),
                "ethanol_geo_spread_usd_per_gal": (None if ethanol_geo_spread is None else float(ethanol_geo_spread)),
                "cbot_corn_price": (None if pd.isna(cbot_corn) else float(cbot_corn)),
                "weighted_basis_usd_per_bu": (None if weighted_basis is None else float(weighted_basis)),
                "official_weighted_corn_basis_usd_per_bu": (None if weighted_basis is None else float(weighted_basis)),
                "ethanol_coverage_ratio": float(ethanol_cov),
                "basis_coverage_ratio": float(basis_cov),
                "delivered_corn_price": delivered_corn,
                "natural_gas_price": (None if pd.isna(gas_price) else float(gas_price)),
                "ethanol_revenue": ethanol_revenue,
                "feedstock_cost": feedstock_cost,
                "natural_gas_burden": natural_gas_burden,
                "simple_crush": simple_crush,
                "simple_crush_per_gal": (None if simple_crush is None else float(simple_crush) / float(ethanol_yield_num)),
                "corn_basis_source_kind": basis_source_kind,
                "corn_basis_source_label": basis_source_label,
                "corn_basis_provenance": basis_provenance,
                "actual_bid_plant_count": actual_bid_plant_count,
                "fallback_plant_count": fallback_plant_count,
            }
        )
    return out


def build_gpre_official_proxy_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
    prior_quarter: bool = False,
    ticker_root: Optional[Path] = None,
    bids_snapshot: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows_df = _market_rows_df(rows)
    if prior_quarter:
        _, current_q_end = calendar_quarter_bounds(as_of_date=as_of_date)
        window_start, window_end = prior_calendar_quarter_bounds(as_of_date=as_of_date)
        status_when_available = "ok_prior"
        no_data_message = "No prior-quarter market observations available."
        calendar_quarter = current_q_end
        display_quarter = window_end
    else:
        window_start, window_end = calendar_quarter_bounds(as_of_date=as_of_date)
        status_when_available = "ok_current"
        no_data_message = "No current-quarter market observations available."
        calendar_quarter = window_end
        display_quarter = window_end
    basis_payload = None
    if not prior_quarter:
        target_date = as_of_date if isinstance(as_of_date, date) else window_end
        basis_payload = _gpre_official_current_forward_basis_payload(
            rows_df,
            target_date=target_date,
            target_quarter_end=window_end,
            as_of_date=as_of_date,
            ticker_root=ticker_root,
            bids_snapshot=bids_snapshot,
            plant_capacity_history=plant_capacity_history,
        )
    weekly_rows = _gpre_official_proxy_weekly_rows(
        rows_df,
        ethanol_yield=ethanol_yield,
        natural_gas_usage=natural_gas_usage,
        window_start=window_start,
        window_end=window_end,
        ticker_root=ticker_root,
        basis_override_payload=basis_payload,
        plant_capacity_history=plant_capacity_history,
    )
    def _avg(key: str) -> Optional[float]:
        vals = [pd.to_numeric(rec.get(key), errors="coerce") for rec in weekly_rows]
        nums = [float(v) for v in vals if pd.notna(v)]
        if not nums:
            return None
        return float(sum(nums) / len(nums))
    def _meta(key: str, *, cadence: str, proxy_mode: str = "", extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        valid_rows = [
            rec
            for rec in weekly_rows
            if isinstance(rec.get("week_end"), date) and pd.notna(pd.to_numeric(rec.get(key), errors="coerce"))
        ]
        meta_out = {
            "value": _avg(key),
            "as_of": max((rec.get("week_end") for rec in valid_rows if isinstance(rec.get("week_end"), date)), default=None),
            "obs_count": len(valid_rows),
            "cadence": cadence,
        }
        if proxy_mode:
            meta_out["proxy_mode"] = proxy_mode
        if isinstance(extra, dict):
            meta_out.update(extra)
        return meta_out
    current_process = {
        "ethanol_revenue": _avg("ethanol_revenue"),
        "feedstock_cost": _avg("feedstock_cost"),
        "natural_gas_burden": _avg("natural_gas_burden"),
        "simple_crush": _avg("simple_crush"),
        "simple_crush_per_gal": _avg("simple_crush_per_gal"),
    }
    process_ok = bool(weekly_rows)
    current_market = {
        "corn_price": _avg("delivered_corn_price"),
        "ethanol_price": _avg("ethanol_price"),
        "natural_gas_price": _avg("natural_gas_price"),
        "cbot_corn_front_price": _avg("cbot_corn_price"),
    }
    market_meta = {
        "corn_price": _meta(
            "delivered_corn_price",
            cadence="weekly_delivered_corn",
            proxy_mode="cbot_plus_official_weighted_basis",
            extra={
                "official_weighted_corn_basis_usd_per_bu": (
                    _avg("official_weighted_corn_basis_usd_per_bu")
                    if _avg("official_weighted_corn_basis_usd_per_bu") is not None
                    else pd.to_numeric((basis_payload or {}).get("official_weighted_corn_basis_usd_per_bu"), errors="coerce")
                ),
                "official_corn_basis_source_kind": (
                    str((basis_payload or {}).get("official_corn_basis_source_kind") or "").strip()
                    or str(next((rec.get("corn_basis_source_kind") for rec in weekly_rows if str(rec.get("corn_basis_source_kind") or "").strip()), "") or "")
                ),
                "official_corn_basis_source_label": (
                    str((basis_payload or {}).get("official_corn_basis_source_label") or "").strip()
                    or str(next((rec.get("corn_basis_source_label") for rec in weekly_rows if str(rec.get("corn_basis_source_label") or "").strip()), "") or "")
                ),
                "official_corn_basis_provenance": (
                    str((basis_payload or {}).get("official_corn_basis_provenance") or "").strip()
                    or str(next((rec.get("corn_basis_provenance") for rec in weekly_rows if str(rec.get("corn_basis_provenance") or "").strip()), "") or "")
                ),
                "official_actual_bid_plant_count": int((basis_payload or {}).get("official_actual_bid_plant_count") or 0),
                "official_fallback_plant_count": int((basis_payload or {}).get("official_fallback_plant_count") or 0),
                "weighted_ams_basis_proxy_usd_per_bu": pd.to_numeric(
                    (basis_payload or {}).get("weighted_ams_basis_proxy_usd_per_bu"),
                    errors="coerce",
                ),
                "cbot_corn_front_price_usd_per_bu": _avg("cbot_corn_price"),
            },
        ),
        "ethanol_price": _meta(
            "ethanol_price",
            cadence="weighted_weekly_ethanol_benchmark",
            proxy_mode="footprint_weighted_ethanol_benchmark",
            extra={
                "east_avg_usd_per_gal": _avg("ethanol_east_avg_usd_per_gal"),
                "east_geo_spread_usd_per_gal": _avg("ethanol_geo_spread_usd_per_gal"),
            },
        ),
        "natural_gas_price": _meta(
            "natural_gas_price",
            cadence="weekly",
        ),
    }
    market_available = any(value is not None for value in current_market.values())
    process_as_of = max((rec.get("week_end") for rec in weekly_rows if isinstance(rec.get("week_end"), date)), default=None)
    market_as_of_candidates = [meta.get("as_of") for meta in market_meta.values() if isinstance(meta, dict) and isinstance(meta.get("as_of"), date)]
    snapshot_as_of = max([*market_as_of_candidates, process_as_of] if isinstance(process_as_of, date) else market_as_of_candidates, default=None)
    message = "" if market_available else no_data_message
    if market_available and not process_ok:
        message = "Current-quarter market inputs may be partially available, but the simple market proxy remains unavailable until overlapping corn, ethanol and gas observations exist."
    return {
        "quarter_start": window_start,
        "quarter_end": window_end,
        "display_quarter": display_quarter,
        "calendar_quarter": calendar_quarter,
        "as_of": snapshot_as_of,
        "weeks_included": int(len(weekly_rows)),
        "market_meta": market_meta,
        "current_market": current_market if market_available else {},
        "current_process": current_process if process_ok else {},
        "weekly_rows": weekly_rows,
        "process_as_of": process_as_of if isinstance(process_as_of, date) else None,
        "process_status": "ok" if process_ok else "no_data",
        "status": status_when_available if market_available else "no_data",
        "message": message,
        "proxy_definition": "weighted ethanol benchmark - delivered corn (cbot + official weighted corn basis) - natural gas",
        "official_simple_proxy_usd_per_gal": current_process.get("simple_crush_per_gal"),
        "official_weighted_corn_basis_usd_per_bu": market_meta["corn_price"].get("official_weighted_corn_basis_usd_per_bu"),
        "official_corn_basis_source_kind": market_meta["corn_price"].get("official_corn_basis_source_kind"),
        "official_corn_basis_source_label": market_meta["corn_price"].get("official_corn_basis_source_label"),
        "official_corn_basis_provenance": market_meta["corn_price"].get("official_corn_basis_provenance"),
        "official_actual_bid_plant_count": market_meta["corn_price"].get("official_actual_bid_plant_count"),
        "official_fallback_plant_count": market_meta["corn_price"].get("official_fallback_plant_count"),
    }


def build_gpre_official_proxy_history_series(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
    start_date: Optional[date] = None,
    lookback_weeks: Optional[int] = None,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
    except Exception:
        ethanol_yield_num = None
    if ethanol_yield_num is None or ethanol_yield_num == 0:
        return []
    rows_df = _market_rows_df(rows)
    end_date = as_of_date if isinstance(as_of_date, date) else date.today()
    window_start = start_date if isinstance(start_date, date) else (end_date - timedelta(days=((max(int(lookback_weeks or 104), 1) * 7) + 6)))
    weekly_rows = _gpre_official_proxy_weekly_rows(
        rows_df,
        ethanol_yield=ethanol_yield,
        natural_gas_usage=natural_gas_usage,
        window_start=window_start,
        window_end=end_date,
        ticker_root=ticker_root,
        plant_capacity_history=plant_capacity_history,
    )
    out_rows = [
        {
            "week_end": rec["week_end"],
            "weighted_ethanol_benchmark_usd_per_gal": float(rec["ethanol_price"]),
            "simple_crush_per_bushel": float(rec["simple_crush"]),
            "simple_crush_per_gal": float(rec["simple_crush_per_gal"]),
            "delivered_corn_price": float(rec["delivered_corn_price"]),
            "weighted_basis_usd_per_bu": (
                None if pd.isna(pd.to_numeric(rec.get("weighted_basis_usd_per_bu"), errors="coerce"))
                else float(pd.to_numeric(rec.get("weighted_basis_usd_per_bu"), errors="coerce"))
            ),
        }
        for rec in weekly_rows
        if isinstance(rec.get("week_end"), date) and pd.notna(pd.to_numeric(rec.get("simple_crush_per_gal"), errors="coerce"))
    ]
    if lookback_weeks is not None and len(out_rows) > int(lookback_weeks):
        out_rows = out_rows[-int(lookback_weeks):]
    return out_rows


def _tenor_contract_midpoint(tenor: str) -> Optional[date]:
    match = _TENOR_RE.match(str(tenor or "").strip())
    if not match:
        return None
    month = _MONTH_ABBREV.get(str(match.group("month") or "").lower())
    if month is None:
        return None
    year = 2000 + int(match.group("year"))
    return date(year, month, 15)


def _quarter_midpoint(start: date, end: date) -> date:
    return start + timedelta(days=((end - start).days // 2))


def _tenor_label(tenor: str) -> str:
    match = _TENOR_RE.match(str(tenor or "").strip())
    if not match:
        return str(tenor or "")
    month_key = str(match.group("month") or "").lower()
    month_name = month_key.capitalize()
    year = 2000 + int(match.group("year"))
    return f"{month_name} {year}"


def _quarter_contract_month_tenors(target_start: date) -> List[str]:
    out: List[str] = []
    for month_num in range(int(target_start.month), int(target_start.month) + 3):
        out.append(f"{date(int(target_start.year), month_num, 1):%b}".lower() + f"{str(target_start.year)[-2:]}")
    return out


def _quarter_contract_month_components(target_start: date) -> List[Dict[str, Any]]:
    components: List[Dict[str, Any]] = []
    for month_num in range(int(target_start.month), int(target_start.month) + 3):
        month_start = date(int(target_start.year), month_num, 1)
        month_days = int(calendar.monthrange(int(target_start.year), int(month_num))[1])
        tenor = f"{month_start:%b}".lower() + f"{str(target_start.year)[-2:]}"
        components.append(
            {
                "tenor": tenor,
                "label": _tenor_label(tenor),
                "month_start": month_start,
                "month_days": month_days,
            }
        )
    return components


def _ethanol_thesis_source_label(source_type: Any) -> str:
    source_txt = str(source_type or "").strip()
    if source_txt == "local_chicago_ethanol_futures_csv":
        return "local Chicago ethanol futures CSV"
    if source_txt == "manual_local_snapshot":
        return "local manual quarter-open ethanol snapshot"
    if source_txt == "cme_ethanol_settlement":
        return "official CME settlement file"
    return source_txt or "ethanol futures thesis source"


def _build_quarter_strip_payload(
    *,
    target_start: date,
    target_end: date,
    market_family: str,
    records_by_tenor: Dict[str, Dict[str, Any]],
    default_source_type: str,
    default_source_label: str,
    product_code_default: str = "FL",
    extra_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    components = _quarter_contract_month_components(target_start)

    def _missing_payload(
        *,
        missing_contracts: List[str],
        component_rows_in: Optional[List[Dict[str, Any]]] = None,
        source_files_in: Optional[List[str]] = None,
        source_types_in: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "target_quarter_start": target_start,
            "target_quarter_end": target_end,
            "target_quarter_midpoint": _quarter_midpoint(target_start, target_end),
            "market_family": market_family,
            "product_code": product_code_default,
            "source_type": default_source_type,
            "source_label": default_source_label,
            "status": "missing_contract_months",
            "missing_contract_tenors": list(missing_contracts),
            "contract_tenors": [str(comp["tenor"]) for comp in components],
            "contract_labels": [str(comp["label"]) for comp in components],
            "contract_components": list(component_rows_in or []),
            "source_files": sorted({str(item or "").strip() for item in list(source_files_in or []) if str(item or "").strip()}),
        }
        if source_types_in:
            source_types = sorted({str(item or "").strip() for item in source_types_in if str(item or "").strip()})
            if source_types:
                payload["source_type"] = source_types[0]
                payload["source_label"] = _ethanol_thesis_source_label(source_types[0])
        if isinstance(extra_payload, dict):
            payload.update(dict(extra_payload))
        return payload

    if not records_by_tenor:
        return _missing_payload(missing_contracts=[str(comp["tenor"]) for comp in components])

    missing = [str(comp["tenor"]) for comp in components if str(comp["tenor"]) not in records_by_tenor]
    if missing:
        return _missing_payload(missing_contracts=missing)

    component_rows: List[Dict[str, Any]] = []
    weighted_numerator = 0.0
    weighted_denominator = 0.0
    simple_values: List[float] = []
    obs_dates: List[date] = []
    product_codes: List[str] = []
    source_types_seen: List[str] = []
    source_files_seen: List[str] = []
    for comp in components:
        rec = dict(records_by_tenor[str(comp["tenor"])])
        price_num = pd.to_numeric(rec.get("price_value"), errors="coerce")
        if pd.isna(price_num):
            return _missing_payload(
                missing_contracts=[str(comp["tenor"])],
                component_rows_in=component_rows,
                source_files_in=source_files_seen,
                source_types_in=source_types_seen,
            )
        obs_dt = pd.to_datetime(rec.get("observation_date"), errors="coerce")
        obs_date = None if pd.isna(obs_dt) else pd.Timestamp(obs_dt).date()
        if isinstance(obs_date, date):
            obs_dates.append(obs_date)
        source_file = str(rec.get("source_file") or "").strip()
        if source_file:
            source_files_seen.append(source_file)
        source_type = str(rec.get("source_type") or default_source_type).strip() or default_source_type
        source_types_seen.append(source_type)
        product_code = str(rec.get("product_code") or "").strip()
        if product_code:
            product_codes.append(product_code)
        simple_values.append(float(price_num))
        weighted_numerator += float(price_num) * float(comp["month_days"])
        weighted_denominator += float(comp["month_days"])
        component_rows.append(
            {
                "contract_tenor": str(comp["tenor"]),
                "contract_label": str(comp["label"]),
                "month_days": int(comp["month_days"]),
                "observation_date": obs_date,
                "price_value": float(price_num),
                "source_file": source_file,
                "source_type": source_type,
                "source_label": str(rec.get("source_label") or _ethanol_thesis_source_label(source_type)),
                "product_code": product_code,
            }
        )
    strip_method = "day_weighted"
    strip_value = None
    if weighted_denominator > 0:
        strip_value = weighted_numerator / weighted_denominator
    elif simple_values:
        strip_method = "simple_average"
        strip_value = float(np.mean(simple_values))
    unique_source_types = sorted({str(item or "").strip() for item in source_types_seen if str(item or "").strip()})
    source_type = unique_source_types[0] if unique_source_types else default_source_type
    payload = {
        "target_quarter_start": target_start,
        "target_quarter_end": target_end,
        "target_quarter_midpoint": _quarter_midpoint(target_start, target_end),
        "market_family": market_family,
        "product_code": (product_codes[-1] if product_codes else product_code_default),
        "source_type": source_type,
        "source_label": _ethanol_thesis_source_label(source_type) if source_type else default_source_label,
        "status": "ok",
        "price_value": None if strip_value is None else float(strip_value),
        "strip_method": strip_method,
        "contract_tenors": [str(comp["tenor"]) for comp in components],
        "contract_labels": [str(comp["label"]) for comp in components],
        "contract_components": component_rows,
        "observation_date": max(obs_dates) if obs_dates else None,
        "source_files": sorted({str(item or "").strip() for item in source_files_seen if str(item or "").strip()}),
    }
    if isinstance(extra_payload, dict):
        payload.update(dict(extra_payload))
    return payload


def _pick_quarter_strip_reference(
    rows: Iterable[Dict[str, Any]],
    *,
    prefix: str,
    market_family: str,
    as_of_date: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    df = _market_rows_df(rows)
    require_market_columns(
        df,
        ["aggregation_level", "series_key", "observation_date", "price_value"],
        contract_name="_pick_quarter_strip_reference",
    )
    target_start, target_end = next_calendar_quarter_bounds(as_of_date=as_of_date)
    obs = df[
        (df["aggregation_level"].astype(str).str.lower() == "observation")
        & df["series_key"].astype(str).str.match(rf"^{re.escape(prefix)}_[a-z]{{3}}\d{{2}}_usd(?:_per_gal)?$", na=False)
        & df["observation_date"].notna()
        & df["price_value"].notna()
    ].copy()
    if isinstance(as_of_date, date):
        obs_dates = pd.to_datetime(obs["observation_date"], errors="coerce")
        obs = obs[obs_dates.dt.date.le(as_of_date)].copy()
    if obs.empty:
        return _build_quarter_strip_payload(
            target_start=target_start,
            target_end=target_end,
            market_family=market_family,
            records_by_tenor={},
            default_source_type="local_chicago_ethanol_futures_csv",
            default_source_label="local Chicago ethanol futures CSV",
        )

    latest_per_series = (
        obs.sort_values("observation_date")
        .groupby("series_key", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    latest_by_tenor: Dict[str, Dict[str, Any]] = {}
    for rec in latest_per_series.to_dict("records"):
        tenor = str(rec.get("contract_tenor") or "").strip().lower()
        if not tenor:
            series_key = str(rec.get("series_key") or "")
            match = re.search(r"_([a-z]{3}\d{2})_usd(?:_per_gal)?$", series_key, re.I)
            tenor = str(match.group(1) or "").strip().lower() if match else ""
        if tenor:
            rec = dict(rec)
            rec["contract_tenor"] = tenor
            latest_by_tenor[tenor] = rec
    return _build_quarter_strip_payload(
        target_start=target_start,
        target_end=target_end,
        market_family=market_family,
        records_by_tenor=latest_by_tenor,
        default_source_type="local_chicago_ethanol_futures_csv",
        default_source_label="local Chicago ethanol futures CSV",
    )


def _pick_next_quarter_futures_reference(
    rows: Iterable[Dict[str, Any]],
    *,
    prefix: str,
    market_family: str,
    as_of_date: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    df = _market_rows_df(rows)
    require_market_columns(
        df,
        ["aggregation_level", "series_key", "observation_date", "price_value"],
        contract_name="_pick_next_quarter_futures_reference",
    )
    obs = df[
        (df["aggregation_level"].astype(str).str.lower() == "observation")
        & df["series_key"].astype(str).str.match(rf"^{re.escape(prefix)}_[a-z]{{3}}\d{{2}}_usd$", na=False)
        & df["observation_date"].notna()
        & df["price_value"].notna()
    ].copy()
    if isinstance(as_of_date, date):
        obs_dates = pd.to_datetime(obs["observation_date"], errors="coerce")
        obs = obs[obs_dates.dt.date.le(as_of_date)].copy()
    if obs.empty:
        return None
    latest_per_series = (
        obs.sort_values("observation_date")
        .groupby("series_key", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    target_start, target_end = next_calendar_quarter_bounds(as_of_date=as_of_date)
    target_mid = _quarter_midpoint(target_start, target_end)
    candidates: List[Tuple[Tuple[int, int, int], Dict[str, Any]]] = []
    for rec in latest_per_series.to_dict("records"):
        tenor = str(rec.get("contract_tenor") or "")
        if not tenor:
            series_key = str(rec.get("series_key") or "")
            match = re.search(r"_([a-z]{3}\d{2})_usd$", series_key, re.I)
            tenor = str(match.group(1) or "") if match else ""
        contract_mid = _tenor_contract_midpoint(tenor)
        if contract_mid is None:
            continue
        distance_days = abs((contract_mid - target_mid).days)
        sort_key = (distance_days, -contract_mid.toordinal(), -pd.Timestamp(rec["observation_date"]).to_pydatetime().date().toordinal())
        payload = dict(rec)
        payload["contract_tenor"] = tenor
        payload["contract_label"] = _tenor_label(tenor)
        payload["contract_midpoint"] = contract_mid
        payload["market_family"] = market_family
        candidates.append((sort_key, payload))
    if not candidates:
        return None
    chosen = sorted(candidates, key=lambda item: item[0])[0][1]
    chosen["target_quarter_start"] = target_start
    chosen["target_quarter_end"] = target_end
    chosen["target_quarter_midpoint"] = target_mid
    chosen["observation_date"] = pd.Timestamp(chosen["observation_date"]).date()
    chosen["price_value"] = float(pd.to_numeric(chosen.get("price_value"), errors="coerce"))
    return chosen


def build_next_quarter_thesis_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    as_of_date: Optional[date] = None,
    ticker_root: Optional[Path] = None,
    bids_snapshot: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows_df = _market_rows_df(rows)
    corn_ref = _pick_next_quarter_futures_reference(rows_df, prefix="cbot_corn", market_family="corn_futures", as_of_date=as_of_date)
    ethanol_ref = _pick_quarter_strip_reference(rows_df, prefix="cme_ethanol_chicago_platts", market_family="ethanol_futures", as_of_date=as_of_date)
    gas_ref = _pick_next_quarter_futures_reference(rows_df, prefix="nymex_gas", market_family="natural_gas_futures", as_of_date=as_of_date)
    target_start, target_end = next_calendar_quarter_bounds(as_of_date=as_of_date)
    target_mid = _quarter_midpoint(target_start, target_end)
    corn_basis_payload = _gpre_official_current_forward_basis_payload(
        rows_df,
        target_date=target_mid,
        target_quarter_end=target_end,
        as_of_date=as_of_date,
        ticker_root=ticker_root,
        bids_snapshot=bids_snapshot,
        plant_capacity_history=plant_capacity_history,
    )
    if isinstance(corn_ref, dict):
        corn_ref = dict(corn_ref)
        corn_ref.update(
            {
                "official_weighted_corn_basis_usd_per_bu": corn_basis_payload.get("official_weighted_corn_basis_usd_per_bu"),
                "official_corn_basis_source_kind": corn_basis_payload.get("official_corn_basis_source_kind"),
                "official_corn_basis_source_label": corn_basis_payload.get("official_corn_basis_source_label"),
                "official_corn_basis_provenance": corn_basis_payload.get("official_corn_basis_provenance"),
                "official_actual_bid_plant_count": corn_basis_payload.get("official_actual_bid_plant_count"),
                "official_fallback_plant_count": corn_basis_payload.get("official_fallback_plant_count"),
                "weighted_ams_basis_proxy_usd_per_bu": corn_basis_payload.get("weighted_ams_basis_proxy_usd_per_bu"),
            }
        )
    return {
        "target_quarter_start": target_start,
        "target_quarter_end": target_end,
        "target_quarter_midpoint": target_mid,
        "corn": corn_ref,
        "ethanol": ethanol_ref,
        "natural_gas": gas_ref,
    }


_GPRE_FROZEN_THESIS_SNAPSHOTS_FILENAME = "gpre_frozen_thesis_snapshots.json"


def _gpre_basis_proxy_dir(ticker_root: Optional[Path]) -> Optional[Path]:
    if not isinstance(ticker_root, Path):
        return None
    return ticker_root / "basis_proxy"


def _gpre_frozen_thesis_snapshots_path(ticker_root: Optional[Path]) -> Optional[Path]:
    sidecar_dir = _gpre_basis_proxy_dir(ticker_root)
    if sidecar_dir is None:
        return None
    return sidecar_dir / _GPRE_FROZEN_THESIS_SNAPSHOTS_FILENAME


def _gpre_json_ready(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _gpre_json_ready(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_gpre_json_ready(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _gpre_restore_json_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_gpre_restore_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _gpre_restore_json_value(val) for key, val in value.items()}
    txt = str(value or "").strip() if isinstance(value, str) else ""
    if txt and re.fullmatch(r"\d{4}-\d{2}-\d{2}", txt):
        try:
            return date.fromisoformat(txt)
        except Exception:
            return value
    return value


def load_gpre_frozen_thesis_snapshots(
    ticker_root: Optional[Path],
) -> List[Dict[str, Any]]:
    path = _gpre_frozen_thesis_snapshots_path(ticker_root)
    if path is None or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    out: List[Dict[str, Any]] = []
    for raw_rec in payload:
        if isinstance(raw_rec, dict):
            out.append(dict(_gpre_restore_json_value(raw_rec)))
    return out


def persist_gpre_frozen_thesis_snapshot(
    ticker_root: Optional[Path],
    snapshot_entry: Optional[Dict[str, Any]],
) -> bool:
    path = _gpre_frozen_thesis_snapshots_path(ticker_root)
    if path is None or not isinstance(snapshot_entry, dict):
        return False
    snapshot_as_of = snapshot_entry.get("snapshot_as_of")
    source_quarter_end = snapshot_entry.get("source_quarter_end")
    target_quarter_end = snapshot_entry.get("target_quarter_end")
    if not all(isinstance(val, date) for val in (snapshot_as_of, source_quarter_end, target_quarter_end)):
        return False
    existing = load_gpre_frozen_thesis_snapshots(ticker_root)
    dedup_key = (
        snapshot_as_of.isoformat(),
        source_quarter_end.isoformat(),
        target_quarter_end.isoformat(),
    )
    kept: List[Dict[str, Any]] = []
    for rec in existing:
        rec_key = (
            rec.get("snapshot_as_of").isoformat() if isinstance(rec.get("snapshot_as_of"), date) else "",
            rec.get("source_quarter_end").isoformat() if isinstance(rec.get("source_quarter_end"), date) else "",
            rec.get("target_quarter_end").isoformat() if isinstance(rec.get("target_quarter_end"), date) else "",
        )
        if rec_key != dedup_key:
            kept.append(rec)
    kept.append(dict(snapshot_entry))
    kept.sort(
        key=lambda rec: (
            rec.get("target_quarter_end") if isinstance(rec.get("target_quarter_end"), date) else date.min,
            rec.get("source_quarter_end") if isinstance(rec.get("source_quarter_end"), date) else date.min,
            rec.get("snapshot_as_of") if isinstance(rec.get("snapshot_as_of"), date) else date.min,
        )
    )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps([_gpre_json_ready(rec) for rec in kept], ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return True
    except Exception:
        return False


def _resolve_local_manual_gpre_quarter_open_snapshot(
    ticker_root: Optional[Path],
    *,
    current_quarter_end: date,
    rows: Iterable[Dict[str, Any]],
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    bids_snapshot: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    manual_rows = load_local_manual_ethanol_quarter_open_snapshot_rows(ticker_root)
    target_rows = [
        dict(rec)
        for rec in manual_rows
        if rec.get("target_quarter_end") == current_quarter_end
    ]
    if not target_rows:
        return None
    current_quarter_start, _ = calendar_quarter_bounds(as_of_date=current_quarter_end)
    manual_by_tenor: Dict[str, Dict[str, Any]] = {}
    for rec in target_rows:
        tenor = str(rec.get("contract_tenor") or "").strip().lower()
        if tenor:
            manual_by_tenor[tenor] = dict(rec)
    quarter_open_ethanol_ref = _build_quarter_strip_payload(
        target_start=current_quarter_start,
        target_end=current_quarter_end,
        market_family="ethanol_futures",
        records_by_tenor=manual_by_tenor,
        default_source_type="manual_local_snapshot",
        default_source_label="local manual quarter-open ethanol snapshot",
        extra_payload={
            "snapshot_date": max(
                (rec.get("snapshot_date") for rec in target_rows if isinstance(rec.get("snapshot_date"), date)),
                default=None,
            ),
            "target_quarter": _quarter_label(current_quarter_end),
            "source": sorted({str(rec.get("source") or "").strip() for rec in target_rows if str(rec.get("source") or "").strip()}),
        },
    )
    if str(quarter_open_ethanol_ref.get("status") or "") != "ok":
        missing_txt = ", ".join(
            str(item or "").strip()
            for item in list(quarter_open_ethanol_ref.get("missing_contract_tenors") or [])
            if str(item or "").strip()
        )
        message = "Local manual quarter-open snapshot could not be resolved for the target quarter."
        if missing_txt:
            message = f"Local manual quarter-open snapshot missing contract months: {missing_txt}."
        return {
            "status": "no_snapshot",
            "provenance": "unavailable",
            "target_quarter_end": current_quarter_end,
            "source_quarter_end": prior_calendar_quarter_bounds(as_of_date=current_quarter_end)[1],
            "message": message,
        }
    _, prior_quarter_end = prior_calendar_quarter_bounds(as_of_date=current_quarter_end)
    thesis_snapshot = build_next_quarter_thesis_snapshot(
        rows,
        as_of_date=prior_quarter_end,
        ticker_root=ticker_root,
        bids_snapshot=bids_snapshot,
        plant_capacity_history=plant_capacity_history,
    )
    thesis_snapshot = dict(thesis_snapshot or {})
    thesis_snapshot["ethanol"] = dict(quarter_open_ethanol_ref)
    official_market_snapshot = build_gpre_next_quarter_preview_snapshot(
        rows,
        next_quarter_thesis_snapshot=thesis_snapshot,
        ethanol_yield=ethanol_yield,
        natural_gas_usage=natural_gas_usage,
        as_of_date=prior_quarter_end,
    )
    official_market_snapshot = dict(official_market_snapshot or {})
    official_market_snapshot["quarter_open_provenance"] = "manual_local_snapshot"
    official_market_snapshot["manual_snapshot_date"] = quarter_open_ethanol_ref.get("snapshot_date")
    official_market_snapshot["manual_snapshot_source"] = list(quarter_open_ethanol_ref.get("source") or [])
    official_market_snapshot["message"] = str(
        official_market_snapshot.get("message") or "Quarter-open proxy uses local manual snapshot."
    )
    market_meta = dict(official_market_snapshot.get("market_meta") or {})
    ethanol_meta = dict(market_meta.get("ethanol_price") or {})
    ethanol_meta.update(
        {
            "source_type": "manual_local_snapshot",
            "source_label": "local manual quarter-open ethanol snapshot",
            "quarter_open_provenance": "manual_local_snapshot",
            "snapshot_date": quarter_open_ethanol_ref.get("snapshot_date"),
            "manual_snapshot_source": list(quarter_open_ethanol_ref.get("source") or []),
        }
    )
    market_meta["ethanol_price"] = ethanol_meta
    official_market_snapshot["market_meta"] = market_meta
    return {
        "status": "ok",
        "provenance": "manual_local_snapshot",
        "snapshot_as_of": quarter_open_ethanol_ref.get("snapshot_date"),
        "source_quarter_end": prior_quarter_end,
        "target_quarter_end": current_quarter_end,
        "next_quarter_thesis_snapshot": thesis_snapshot,
        "official_market_snapshot": official_market_snapshot,
        "official_simple_proxy_usd_per_gal": _gpre_snapshot_simple_proxy_usd_per_gal(
            official_market_snapshot,
            ethanol_yield=ethanol_yield,
        ),
        "gpre_proxy_official_usd_per_gal": None,
        "gpre_proxy_model_key": "",
        "message": "Quarter-open proxy uses local manual snapshot.",
    }


def resolve_gpre_quarter_open_snapshot(
    ticker_root: Optional[Path],
    *,
    current_quarter_end: Optional[date],
    rows: Optional[Iterable[Dict[str, Any]]] = None,
    ethanol_yield: Optional[float] = None,
    natural_gas_usage: Optional[float] = None,
    bids_snapshot: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(current_quarter_end, date):
        return {
            "status": "no_snapshot",
            "provenance": "unavailable",
            "message": "No current quarter was resolved for the quarter-open proxy.",
        }
    _, prior_quarter_end = prior_calendar_quarter_bounds(as_of_date=current_quarter_end)
    entries = load_gpre_frozen_thesis_snapshots(ticker_root)
    candidates: List[Dict[str, Any]] = []
    for rec in entries:
        if rec.get("target_quarter_end") != current_quarter_end:
            continue
        if rec.get("source_quarter_end") != prior_quarter_end:
            continue
        candidates.append(dict(rec))
    if not candidates:
        manual_fallback = None
        if rows is not None:
            manual_fallback = _resolve_local_manual_gpre_quarter_open_snapshot(
                ticker_root,
                current_quarter_end=current_quarter_end,
                rows=rows,
                ethanol_yield=ethanol_yield,
                natural_gas_usage=natural_gas_usage,
                bids_snapshot=bids_snapshot,
                plant_capacity_history=plant_capacity_history,
            )
        if isinstance(manual_fallback, dict) and str(manual_fallback.get("status") or "") == "ok":
            return manual_fallback
        quarter_txt = _quarter_label(current_quarter_end)
        failure_message = str((manual_fallback or {}).get("message") or "").strip()
        if not failure_message:
            failure_message = f"No frozen prior-quarter thesis snapshot for {quarter_txt}."
        return {
            "status": "no_snapshot",
            "provenance": "unavailable",
            "target_quarter_end": current_quarter_end,
            "source_quarter_end": prior_quarter_end,
            "message": failure_message,
        }
    chosen = sorted(
        candidates,
        key=lambda rec: rec.get("snapshot_as_of") if isinstance(rec.get("snapshot_as_of"), date) else date.min,
    )[-1]
    chosen["status"] = "ok"
    chosen["provenance"] = "frozen_snapshot"
    return chosen


def _gpre_pattern_hedge_share(qd: Optional[date]) -> float:
    if not isinstance(qd, date):
        return 0.20
    return {
        1: 0.20,
        2: 0.55,
        3: 0.65,
        4: 0.75,
    }.get(((qd.month - 1) // 3) + 1, 0.20)


def _gpre_snapshot_simple_proxy_usd_per_gal(
    snapshot: Optional[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    direct_num = pd.to_numeric(snapshot.get("official_simple_proxy_usd_per_gal"), errors="coerce")
    if pd.notna(direct_num):
        return float(direct_num)
    process = snapshot.get("current_process")
    process = process if isinstance(process, dict) else {}
    per_gal_num = pd.to_numeric(process.get("simple_crush_per_gal"), errors="coerce")
    if pd.notna(per_gal_num):
        return float(per_gal_num)
    per_bushel_num = pd.to_numeric(process.get("simple_crush"), errors="coerce")
    yield_num = pd.to_numeric(ethanol_yield, errors="coerce")
    if pd.notna(per_bushel_num) and pd.notna(yield_num) and float(yield_num) != 0.0:
        return float(per_bushel_num) / float(yield_num)
    return None


def build_gpre_next_quarter_preview_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    next_quarter_thesis_snapshot: Optional[Dict[str, Any]],
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
        natural_gas_usage_num = float(natural_gas_usage) if natural_gas_usage is not None else None
    except Exception:
        ethanol_yield_num = None
        natural_gas_usage_num = None
    target_start = next_quarter_thesis_snapshot.get("target_quarter_start") if isinstance(next_quarter_thesis_snapshot, dict) else None
    target_end = next_quarter_thesis_snapshot.get("target_quarter_end") if isinstance(next_quarter_thesis_snapshot, dict) else None
    corn_ref = next_quarter_thesis_snapshot.get("corn") if isinstance(next_quarter_thesis_snapshot, dict) else None
    ethanol_ref = next_quarter_thesis_snapshot.get("ethanol") if isinstance(next_quarter_thesis_snapshot, dict) else None
    gas_ref = next_quarter_thesis_snapshot.get("natural_gas") if isinstance(next_quarter_thesis_snapshot, dict) else None
    corn_price = pd.to_numeric((corn_ref or {}).get("price_value"), errors="coerce")
    ethanol_price = pd.to_numeric((ethanol_ref or {}).get("price_value"), errors="coerce")
    gas_price = pd.to_numeric((gas_ref or {}).get("price_value"), errors="coerce")
    corn_basis = pd.to_numeric((corn_ref or {}).get("official_weighted_corn_basis_usd_per_bu"), errors="coerce")
    delivered_corn = None
    if pd.notna(corn_price):
        delivered_corn = float(corn_price)
        if pd.notna(corn_basis):
            delivered_corn += float(corn_basis)
    ethanol_revenue = None
    feedstock_cost = None
    natural_gas_burden = None
    simple_crush_per_bushel = None
    simple_crush_per_gal = None
    if pd.notna(ethanol_price) and pd.notna(pd.to_numeric(ethanol_yield_num, errors="coerce")):
        ethanol_revenue = float(ethanol_yield_num) * float(ethanol_price)
    if delivered_corn is not None:
        feedstock_cost = -float(delivered_corn)
    if pd.notna(gas_price) and pd.notna(pd.to_numeric(ethanol_yield_num, errors="coerce")) and pd.notna(pd.to_numeric(natural_gas_usage_num, errors="coerce")):
        natural_gas_burden = -((float(natural_gas_usage_num) / 1_000_000.0) * float(ethanol_yield_num) * float(gas_price))
    if ethanol_revenue is not None and feedstock_cost is not None and natural_gas_burden is not None:
        simple_crush_per_bushel = float(ethanol_revenue + feedstock_cost + natural_gas_burden)
        if ethanol_yield_num:
            simple_crush_per_gal = float(simple_crush_per_bushel) / float(ethanol_yield_num)
    corn_as_of = (corn_ref or {}).get("observation_date")
    ethanol_as_of = (ethanol_ref or {}).get("observation_date")
    gas_as_of = (gas_ref or {}).get("observation_date")
    as_of_candidates = [val for val in (corn_as_of, ethanol_as_of, gas_as_of, as_of_date) if isinstance(val, date)]
    snapshot_as_of = max(as_of_candidates) if as_of_candidates else None
    corn_basis_label = str((corn_ref or {}).get("official_corn_basis_source_label") or "").strip() or "weighted AMS basis proxy"
    corn_basis_provenance = str((corn_ref or {}).get("official_corn_basis_provenance") or "").strip()
    ethanol_contract_tenors = list((ethanol_ref or {}).get("contract_tenors") or [])
    ethanol_strip_method = str((ethanol_ref or {}).get("strip_method") or "").strip()
    ethanol_contract_labels = list((ethanol_ref or {}).get("contract_labels") or [])
    ethanol_missing_contracts = list((ethanol_ref or {}).get("missing_contract_tenors") or [])
    ethanol_source_type = str((ethanol_ref or {}).get("source_type") or "local_chicago_ethanol_futures_csv").strip() or "local_chicago_ethanol_futures_csv"
    ethanol_source_label = str((ethanol_ref or {}).get("source_label") or _ethanol_thesis_source_label(ethanol_source_type))
    ethanol_source_files = [str(item or "").strip() for item in list((ethanol_ref or {}).get("source_files") or []) if str(item or "").strip()]
    market_meta = {
        "corn_price": {
            "value": delivered_corn,
            "as_of": corn_as_of if isinstance(corn_as_of, date) else None,
            "obs_count": 1 if delivered_corn is not None else 0,
            "cadence": "futures_plus_basis_thesis",
            "proxy_mode": "cbot_plus_official_weighted_basis",
            "official_weighted_corn_basis_usd_per_bu": (None if pd.isna(corn_basis) else float(corn_basis)),
            "official_corn_basis_source_kind": str((corn_ref or {}).get("official_corn_basis_source_kind") or "").strip(),
            "official_corn_basis_source_label": corn_basis_label,
            "official_corn_basis_provenance": corn_basis_provenance,
            "cbot_corn_front_price_usd_per_bu": (None if pd.isna(corn_price) else float(corn_price)),
        },
        "ethanol_price": {
            "value": (None if pd.isna(ethanol_price) else float(ethanol_price)),
            "as_of": ethanol_as_of if isinstance(ethanol_as_of, date) else None,
            "obs_count": 0 if pd.isna(ethanol_price) else len(ethanol_contract_tenors),
            "cadence": "futures_thesis_strip",
            "proxy_mode": "local_chicago_ethanol_futures_strip",
            "east_avg_usd_per_gal": None,
            "east_geo_spread_usd_per_gal": None,
            "strip_method": ethanol_strip_method or None,
            "contract_tenors": ethanol_contract_tenors,
            "contract_labels": ethanol_contract_labels,
            "missing_contract_tenors": ethanol_missing_contracts,
            "product_code": str((ethanol_ref or {}).get("product_code") or "FL"),
            "source_type": ethanol_source_type,
            "source_label": ethanol_source_label,
            "source_files": ethanol_source_files,
        },
        "natural_gas_price": {
            "value": (None if pd.isna(gas_price) else float(gas_price)),
            "as_of": gas_as_of if isinstance(gas_as_of, date) else None,
            "obs_count": 1 if pd.notna(gas_price) else 0,
            "cadence": "futures_thesis",
        },
    }
    message = ""
    if pd.isna(ethanol_price):
        missing_txt = ", ".join(str(item or "") for item in ethanol_missing_contracts if str(item or "").strip())
        if missing_txt:
            message = f"Local Chicago ethanol futures strip unavailable; missing contract months: {missing_txt}."
        else:
            message = "Local Chicago ethanol futures strip unavailable for the target quarter."
    return {
        "quarter_start": target_start if isinstance(target_start, date) else None,
        "quarter_end": target_end if isinstance(target_end, date) else None,
        "display_quarter": target_end if isinstance(target_end, date) else None,
        "calendar_quarter": target_end if isinstance(target_end, date) else None,
        "as_of": snapshot_as_of,
        "weeks_included": 0,
        "market_meta": market_meta,
        "current_market": {
            "corn_price": delivered_corn,
            "ethanol_price": (None if pd.isna(ethanol_price) else float(ethanol_price)),
            "natural_gas_price": (None if pd.isna(gas_price) else float(gas_price)),
            "cbot_corn_front_price": (None if pd.isna(corn_price) else float(corn_price)),
        },
        "current_process": {
            "ethanol_revenue": ethanol_revenue,
            "feedstock_cost": feedstock_cost,
            "natural_gas_burden": natural_gas_burden,
            "simple_crush": simple_crush_per_bushel,
            "simple_crush_per_gal": simple_crush_per_gal,
        },
        "process_as_of": snapshot_as_of,
        "process_status": "ok" if simple_crush_per_gal is not None else "no_data",
        "status": "ok_thesis" if delivered_corn is not None or pd.notna(gas_price) or pd.notna(ethanol_price) else "no_data",
        "message": message,
        "proxy_definition": "weighted ethanol benchmark - delivered corn (cbot + official weighted corn basis) - natural gas",
        "official_simple_proxy_usd_per_gal": simple_crush_per_gal,
        "official_weighted_corn_basis_usd_per_bu": (None if pd.isna(corn_basis) else float(corn_basis)),
        "official_corn_basis_source_kind": str((corn_ref or {}).get("official_corn_basis_source_kind") or "").strip(),
        "official_corn_basis_source_label": corn_basis_label,
        "official_corn_basis_provenance": corn_basis_provenance,
        "official_actual_bid_plant_count": int((corn_ref or {}).get("official_actual_bid_plant_count") or 0),
        "official_fallback_plant_count": int((corn_ref or {}).get("official_fallback_plant_count") or 0),
        "ethanol_strip_method": ethanol_strip_method or None,
        "ethanol_contract_tenors": ethanol_contract_tenors,
        "ethanol_contract_labels": ethanol_contract_labels,
        "ethanol_missing_contract_tenors": ethanol_missing_contracts,
        "ethanol_product_code": str((ethanol_ref or {}).get("product_code") or "FL"),
        "ethanol_source_type": ethanol_source_type,
        "ethanol_source_label": ethanol_source_label,
        "ethanol_source_files": ethanol_source_files,
    }


def build_gpre_overlay_proxy_preview_bundle(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
    ticker_root: Optional[Path] = None,
    bids_snapshot: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
    gpre_basis_model_result: Optional[Dict[str, Any]] = None,
    prior_market_snapshot: Optional[Dict[str, Any]] = None,
    current_qtd_market_snapshot: Optional[Dict[str, Any]] = None,
    next_quarter_thesis_snapshot: Optional[Dict[str, Any]] = None,
    simple_crush_history_rows: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    rows_df = _market_rows_df(rows)
    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
        natural_gas_usage_num = float(natural_gas_usage) if natural_gas_usage is not None else None
    except Exception:
        ethanol_yield_num = None
        natural_gas_usage_num = None
    today_ref = as_of_date if isinstance(as_of_date, date) else date.today()
    current_quarter_end = quarter_end_from_date(today_ref)
    prior_snapshot = (
        dict(prior_market_snapshot)
        if isinstance(prior_market_snapshot, dict)
        else build_gpre_official_proxy_snapshot(
            rows_df,
            ethanol_yield=ethanol_yield_num,
            natural_gas_usage=natural_gas_usage_num,
            as_of_date=today_ref,
            prior_quarter=True,
            ticker_root=ticker_root,
            plant_capacity_history=plant_capacity_history,
        )
    )
    current_snapshot = (
        dict(current_qtd_market_snapshot)
        if isinstance(current_qtd_market_snapshot, dict)
        else build_gpre_official_proxy_snapshot(
            rows_df,
            ethanol_yield=ethanol_yield_num,
            natural_gas_usage=natural_gas_usage_num,
            as_of_date=today_ref,
            prior_quarter=False,
            ticker_root=ticker_root,
            bids_snapshot=bids_snapshot,
            plant_capacity_history=plant_capacity_history,
        )
    )
    simple_crush_history_rows = list(simple_crush_history_rows or []) or build_gpre_official_proxy_history_series(
        rows_df,
        ethanol_yield=ethanol_yield_num,
        natural_gas_usage=natural_gas_usage_num,
        as_of_date=today_ref,
        lookback_weeks=None,
        start_date=date(2023, 1, 1),
        ticker_root=ticker_root,
        plant_capacity_history=plant_capacity_history,
    )
    next_thesis_snapshot = (
        dict(next_quarter_thesis_snapshot)
        if isinstance(next_quarter_thesis_snapshot, dict)
        else build_next_quarter_thesis_snapshot(
            rows_df,
            as_of_date=today_ref,
            ticker_root=ticker_root,
            bids_snapshot=bids_snapshot,
            plant_capacity_history=plant_capacity_history,
        )
    )
    next_official_snapshot = build_gpre_next_quarter_preview_snapshot(
        rows_df,
        next_quarter_thesis_snapshot=next_thesis_snapshot,
        ethanol_yield=ethanol_yield_num,
        natural_gas_usage=natural_gas_usage_num,
        as_of_date=today_ref,
    )
    quarter_open_entry = resolve_gpre_quarter_open_snapshot(
        ticker_root,
        current_quarter_end=current_quarter_end,
        rows=rows_df,
        ethanol_yield=ethanol_yield_num,
        natural_gas_usage=natural_gas_usage_num,
        bids_snapshot=bids_snapshot,
        plant_capacity_history=plant_capacity_history,
    )
    quarter_open_provenance = str((quarter_open_entry or {}).get("provenance") or "unavailable")
    quarter_open_market_snapshot = quarter_open_entry.get("official_market_snapshot") if isinstance(quarter_open_entry, dict) else None
    if not isinstance(quarter_open_market_snapshot, dict):
        quarter_open_market_snapshot = {
            "status": "no_snapshot",
            "display_quarter": current_quarter_end,
            "calendar_quarter": current_quarter_end,
            "message": str((quarter_open_entry or {}).get("message") or f"No frozen prior-quarter thesis snapshot for {_quarter_label(current_quarter_end)}."),
            "current_market": {},
            "current_process": {},
            "market_meta": {},
            "process_status": "no_data",
            "quarter_open_provenance": quarter_open_provenance,
        }
    quarterly_df = (gpre_basis_model_result or {}).get("quarterly_df")
    if not isinstance(quarterly_df, pd.DataFrame):
        quarterly_df = pd.DataFrame()
    chosen_model_key = str((gpre_basis_model_result or {}).get("gpre_proxy_model_key") or "process_current_quarter_avg")
    chosen_family = str((gpre_basis_model_result or {}).get("gpre_proxy_family") or "")
    chosen_family_label = str((gpre_basis_model_result or {}).get("gpre_proxy_family_label") or "")
    chosen_timing = str((gpre_basis_model_result or {}).get("gpre_proxy_timing_rule") or "")
    quarter_records: Dict[date, Dict[str, Any]] = {}
    if not quarterly_df.empty:
        for rec in quarterly_df.to_dict("records"):
            qd = pd.to_datetime(rec.get("quarter"), errors="coerce")
            if pd.notna(qd):
                quarter_records[pd.Timestamp(qd).date()] = rec
    prior_display_quarter = prior_snapshot.get("display_quarter") if isinstance(prior_snapshot.get("display_quarter"), date) else None
    prior_record = dict(quarter_records.get(prior_display_quarter) or {}) if isinstance(prior_display_quarter, date) else {}
    prior_prior_quarter_end = prior_calendar_quarter_bounds(as_of_date=prior_display_quarter)[1] if isinstance(prior_display_quarter, date) else None
    prior_anchor_record = dict(quarter_records.get(prior_prior_quarter_end) or {}) if isinstance(prior_prior_quarter_end, date) else {}
    prior_basis = pd.to_numeric(prior_record.get("weighted_basis_plant_count_usd_per_bu"), errors="coerce")
    prior_basis_uniform = pd.to_numeric(
        (((prior_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("official_weighted_corn_basis_usd_per_bu")),
        errors="coerce",
    )
    prior_basis_front = _gpre_weighted_basis_for_quarter_from_observations(
        rows,
        prior_display_quarter,
        profile="front_loaded",
        ticker_root=ticker_root,
        plant_capacity_history=plant_capacity_history,
    )
    if prior_basis_front is None and pd.notna(prior_basis_uniform):
        prior_basis_front = float(prior_basis_uniform)
    current_basis_uniform = _gpre_weighted_basis_for_quarter_from_observations(rows, current_quarter_end, profile="uniform", ticker_root=ticker_root, plant_capacity_history=plant_capacity_history)
    current_basis_front = _gpre_weighted_basis_for_quarter_from_observations(rows, current_quarter_end, profile="front_loaded", ticker_root=ticker_root, plant_capacity_history=plant_capacity_history)
    if current_basis_uniform is None:
        meta_basis = pd.to_numeric(
            (((current_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("official_weighted_corn_basis_usd_per_bu")),
            errors="coerce",
        )
        if pd.notna(meta_basis):
            current_basis_uniform = float(meta_basis)
    if current_basis_front is None:
        current_basis_front = current_basis_uniform
    next_basis_uniform = pd.to_numeric(
        (((next_official_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("official_weighted_corn_basis_usd_per_bu")),
        errors="coerce",
    )
    if pd.isna(next_basis_uniform):
        next_basis_uniform = pd.to_numeric(
            (((current_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("official_weighted_corn_basis_usd_per_bu")),
            errors="coerce",
        )
    next_basis_front = next_basis_uniform

    def _bridge_process_spot(
        ethanol_price_in: Any,
        cbot_price_in: Any,
        basis_price_in: Any,
        gas_price_in: Any,
    ) -> Tuple[Optional[float], Optional[float]]:
        ethanol_num = pd.to_numeric(ethanol_price_in, errors="coerce")
        cbot_num = pd.to_numeric(cbot_price_in, errors="coerce")
        basis_num = pd.to_numeric(basis_price_in, errors="coerce")
        gas_num = pd.to_numeric(gas_price_in, errors="coerce")
        if pd.isna(ethanol_num) or pd.isna(cbot_num) or pd.isna(basis_num) or pd.isna(pd.to_numeric(ethanol_yield_num, errors="coerce")) or float(ethanol_yield_num or 0.0) == 0.0:
            return None, None
        bridge_val = float(ethanol_num) - ((float(cbot_num) + float(basis_num)) / float(ethanol_yield_num))
        if pd.isna(gas_num) or pd.isna(pd.to_numeric(natural_gas_usage_num, errors="coerce")):
            return bridge_val, None
        gas_burden = (float(natural_gas_usage_num) / 1_000_000.0) * float(gas_num)
        return bridge_val, (bridge_val - gas_burden)

    def _bridge_process_nonethanol(
        cbot_price_in: Any,
        basis_price_in: Any,
        gas_price_in: Any,
    ) -> Tuple[Optional[float], Optional[float]]:
        cbot_num = pd.to_numeric(cbot_price_in, errors="coerce")
        basis_num = pd.to_numeric(basis_price_in, errors="coerce")
        gas_num = pd.to_numeric(gas_price_in, errors="coerce")
        yield_num = pd.to_numeric(ethanol_yield_num, errors="coerce")
        if pd.isna(cbot_num) or pd.isna(basis_num) or pd.isna(yield_num) or float(yield_num) == 0.0:
            return None, None
        bridge_component = -((float(cbot_num) + float(basis_num)) / float(yield_num))
        usage_num = pd.to_numeric(natural_gas_usage_num, errors="coerce")
        if pd.isna(gas_num) or pd.isna(usage_num):
            return bridge_component, None
        gas_burden_per_gal = (float(usage_num) / 1_000_000.0) * float(gas_num)
        return bridge_component, (bridge_component - gas_burden_per_gal)

    def _basis_mix(current_basis_in: Any, prior_basis_in: Any, current_weight: float, prior_weight: float) -> Optional[float]:
        current_num = pd.to_numeric(current_basis_in, errors="coerce")
        prior_num = pd.to_numeric(prior_basis_in, errors="coerce")
        if pd.notna(current_num) and pd.notna(prior_num):
            return (float(current_num) * current_weight) + (float(prior_num) * prior_weight)
        if pd.notna(current_num):
            return float(current_num)
        if pd.notna(prior_num):
            return float(prior_num)
        return None

    current_ethanol = pd.to_numeric((current_snapshot.get("current_market") or {}).get("ethanol_price"), errors="coerce")
    current_cbot = pd.to_numeric((current_snapshot.get("current_market") or {}).get("cbot_corn_front_price"), errors="coerce")
    if pd.isna(current_cbot):
        current_cbot = pd.to_numeric((((current_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("cbot_corn_front_price_usd_per_bu")), errors="coerce")
    current_gas = pd.to_numeric((current_snapshot.get("current_market") or {}).get("natural_gas_price"), errors="coerce")
    prior_ethanol = pd.to_numeric((prior_snapshot.get("current_market") or {}).get("ethanol_price"), errors="coerce")
    prior_cbot = pd.to_numeric((prior_snapshot.get("current_market") or {}).get("cbot_corn_front_price"), errors="coerce")
    if pd.isna(prior_cbot):
        prior_cbot = pd.to_numeric((((prior_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("cbot_corn_front_price_usd_per_bu")), errors="coerce")
    prior_gas = pd.to_numeric((prior_snapshot.get("current_market") or {}).get("natural_gas_price"), errors="coerce")
    next_ethanol = pd.to_numeric((next_official_snapshot.get("current_market") or {}).get("ethanol_price"), errors="coerce")
    next_cbot = pd.to_numeric((next_official_snapshot.get("current_market") or {}).get("cbot_corn_front_price"), errors="coerce")
    next_gas = pd.to_numeric((next_official_snapshot.get("current_market") or {}).get("natural_gas_price"), errors="coerce")
    quarter_open_ethanol = pd.to_numeric((quarter_open_market_snapshot.get("current_market") or {}).get("ethanol_price"), errors="coerce")
    quarter_open_cbot = pd.to_numeric((quarter_open_market_snapshot.get("current_market") or {}).get("cbot_corn_front_price"), errors="coerce")
    if pd.isna(quarter_open_cbot):
        quarter_open_cbot = pd.to_numeric((((quarter_open_market_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("cbot_corn_front_price_usd_per_bu")), errors="coerce")
    quarter_open_gas = pd.to_numeric((quarter_open_market_snapshot.get("current_market") or {}).get("natural_gas_price"), errors="coerce")
    quarter_open_basis_uniform = pd.to_numeric(
        (((quarter_open_market_snapshot.get("market_meta") or {}).get("corn_price") or {}).get("official_weighted_corn_basis_usd_per_bu")),
        errors="coerce",
    )
    quarter_open_basis_front = quarter_open_basis_uniform
    observation_count_map = _gpre_quarter_observation_count_map(rows)
    ops_signal_map = _gpre_ops_penalty_signal_map(ticker_root)
    inventory_signal_map = _gpre_inventory_timing_signal_map(ticker_root)

    def _snapshot_geo_term(snapshot_in: Dict[str, Any]) -> float:
        ethanol_meta = dict((snapshot_in or {}).get("market_meta") or {}).get("ethanol_price") or {}
        spread_num = pd.to_numeric((ethanol_meta or {}).get("east_geo_spread_usd_per_gal"), errors="coerce")
        if pd.isna(spread_num):
            return 0.0
        return float(np.clip(0.40 * float(spread_num), -0.04, 0.04))

    def _ops_penalty_for_quarter(qd_in: Any) -> float:
        qd_local = parse_quarter_like(qd_in)
        if qd_local is None:
            return 0.0
        penalty_num = pd.to_numeric((ops_signal_map.get(qd_local) or {}).get("ops_penalty_usd_per_gal"), errors="coerce")
        return 0.0 if pd.isna(penalty_num) else float(penalty_num)

    def _total_exec_penalty_for_quarter(qd_in: Any) -> float:
        qd_local = parse_quarter_like(qd_in)
        if qd_local is None:
            return 0.0
        signal_rec = dict(ops_signal_map.get(qd_local) or {})
        details = _gpre_execution_penalty_details(
            signal_rec.get("ops_penalty_usd_per_gal"),
            signal_rec.get("negative_terms"),
            signal_rec.get("utilization_pct"),
        )
        return float(details.get("total_execution_penalty_usd_per_gal") or 0.0)

    def _utilization_penalty_for_quarter(qd_in: Any) -> float:
        qd_local = parse_quarter_like(qd_in)
        if qd_local is None:
            return 0.0
        return _gpre_utilization_overlay_penalty((ops_signal_map.get(qd_local) or {}).get("utilization_pct"))

    def _maintenance_delay_penalty_for_quarter(qd_in: Any) -> float:
        qd_local = parse_quarter_like(qd_in)
        if qd_local is None:
            return 0.0
        details = _gpre_maintenance_delay_penalty_details((ops_signal_map.get(qd_local) or {}).get("negative_terms"))
        return float(details.get("penalty_usd_per_gal") or 0.0)

    def _inventory_drag_penalty_for_quarter(qd_in: Any) -> float:
        qd_local = parse_quarter_like(qd_in)
        if qd_local is None:
            return 0.0
        details = dict(inventory_signal_map.get(qd_local) or {})
        return float(details.get("inventory_drag_penalty_usd_per_gal") or 0.0)

    def _quarter_blend_value(anchor_val: Any, current_val: Any, qd_in: Any) -> Optional[float]:
        qd_local = parse_quarter_like(qd_in)
        blend_info_local = _gpre_quarter_open_blend_weights(observation_count_map.get(qd_local, 0))
        return _blend_optional_values(
            anchor_val,
            current_val,
            anchor_weight=float(blend_info_local.get("quarter_open_weight") or 0.75),
            current_weight=float(blend_info_local.get("current_weight") or 0.25),
        )

    def _formula_helper_payload(
        *,
        status: str,
        mode: str,
        slope: Optional[float],
        intercept: Optional[float],
        hedge_share: float = 0.0,
        anchor: Optional[float] = None,
        live_preview_mode: str = "exact_formula",
        live_preview_note: str = "",
    ) -> Dict[str, Any]:
        return _gpre_formula_helper_payload(
            status=status,
            mode=mode,
            slope=slope,
            intercept=intercept,
            hedge_share=hedge_share,
            anchor=anchor,
            live_preview_mode=live_preview_mode,
            live_preview_note=live_preview_note,
        )

    def _combine_linear_formula_helpers(
        primary_helper: Dict[str, Any],
        secondary_helper: Dict[str, Any],
        *,
        secondary_weight: float,
        live_preview_mode: str,
        live_preview_note: str,
    ) -> Dict[str, Any]:
        return _gpre_combine_linear_formula_helpers(
            primary_helper,
            secondary_helper,
            secondary_weight=secondary_weight,
            live_preview_mode=live_preview_mode,
            live_preview_note=live_preview_note,
        )

    def _process_blend_formula_payload(
        *,
        anchor_proxy: Any,
        current_nonethanol: Any,
        qd_in: Any,
        penalty: float = 0.0,
        phase_label: str = "current",
        penalty_note: str = "",
    ) -> Dict[str, Any]:
        qd_local = parse_quarter_like(qd_in)
        blend_info_local = _gpre_quarter_open_blend_weights(observation_count_map.get(qd_local, 0))
        return _gpre_process_blend_formula_helper(
            anchor_proxy=anchor_proxy,
            current_nonethanol=current_nonethanol,
            quarter_open_weight=blend_info_local.get("quarter_open_weight"),
            current_weight=blend_info_local.get("current_weight"),
            penalty=penalty,
            phase_label=phase_label,
            penalty_note=penalty_note,
            live_preview_mode="exact_formula",
        )

    def _gpre_phase_preview_story(model_key_in: str, *, phase: str) -> Dict[str, str]:
        if model_key_in == "process_quarter_open_blend":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the promoted quarter-open/current blend directly.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview uses the same blend family but reduces to the thesis process leg because a future quarter has no live quarter-open/current progress split yet.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model with the stored quarter-open anchor and current-quarter progress weights when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_ops_penalty":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the promoted quarter-open/current blend and then subtracts the bounded ops penalty for the same quarter.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview uses the blend family thesis leg and subtracts a bounded same-quarter ops penalty only when explicit signal evidence exists.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model and then subtracts the bounded ops penalty when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_hedge_realization":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the quarter-open/current blend and then applies the capped hedge-realization adjustment toward the prior-front hedge memo process path.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview reduces the blend family to the thesis process leg and then applies the capped hedge-realization adjustment toward the prior-front hedge memo path.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model with the capped hedge-realization adjustment when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_exec_penalty":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the quarter-open/current blend and then subtracts the bounded severe execution penalty for clearly hard operational quarters.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview reduces the blend family to the thesis process leg and then subtracts the bounded severe execution penalty only when explicit hard-quarter signals exist.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model and then subtracts the bounded severe execution penalty when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_utilization_penalty":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the quarter-open/current blend and then subtracts a bounded utilization penalty only when utilization is below 95%.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview reduces the blend family to the thesis process leg and then subtracts the bounded utilization penalty when the current utilization signal is weak.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model and then subtracts the bounded utilization penalty when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_maintenance_delay_penalty":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the quarter-open/current blend and then subtracts the bounded maintenance-delay penalty when explicit outage or delay terms exist.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview reduces the blend family to the thesis process leg and then subtracts the bounded maintenance-delay penalty when explicit disruption terms exist.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model and then subtracts the bounded maintenance-delay penalty when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_inventory_timing_drag":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the quarter-open/current blend and then subtracts the bounded inventory or timing drag only when explicit realization-drag language exists.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview reduces the blend family to the thesis process leg and then subtracts the bounded inventory or timing drag only when explicit realization-drag language exists.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model and then subtracts the bounded inventory or timing drag when a frozen/manual snapshot does not already provide the value.",
                }
        if model_key_in == "process_quarter_open_blend_locked_setup":
            if phase == "current":
                return {
                    "live_preview_mode": "exact_formula",
                    "live_preview_note": "Current fitted preview uses the quarter-open/current blend and then pulls it toward the locked setup when hedge-share evidence exists.",
                }
            if phase == "next":
                return {
                    "live_preview_mode": "reduced_form_approximation",
                    "live_preview_note": "Next-quarter fitted preview reduces the blend family to the thesis process leg and then partially pulls it toward the stored quarter-open anchor when hedge-share evidence exists.",
                }
            if phase == "quarter_open":
                return {
                    "live_preview_mode": "model_preview_fallback",
                    "live_preview_note": "Quarter-open fitted preview uses the chosen blend model and then pulls it toward the locked setup when hedge-share evidence exists.",
                }
        if model_key_in == "process_basis_blend_current40_front60":
            return {
                "live_preview_mode": "exact_formula" if phase != "next" else "reduced_form_approximation",
                "live_preview_note": "Fitted preview uses a process basis blend with 40% current basis and 60% front-loaded basis.",
            }
        if model_key_in == "process_basis_passthrough_beta35":
            return {
                "live_preview_mode": "exact_formula" if phase != "next" else "reduced_form_approximation",
                "live_preview_note": "Fitted preview starts from the front-loaded process basis and lets 35% of the current basis move pass through.",
            }
        if model_key_in == "process_basis_passthrough_beta65":
            return {
                "live_preview_mode": "exact_formula" if phase != "next" else "reduced_form_approximation",
                "live_preview_note": "Fitted preview starts from the front-loaded process basis and lets 65% of the current basis move pass through.",
            }
        if model_key_in == "process_quarter_open_current50_exec_penalty":
            return {
                "live_preview_mode": "exact_formula" if phase == "current" else "reduced_form_approximation",
                "live_preview_note": "Fitted preview uses a 50/50 quarter-open/current process blend and then subtracts the bounded severe execution penalty.",
            }
        if model_key_in == "process_regime_basis_passthrough":
            return {
                "live_preview_mode": "exact_formula" if phase != "next" else "reduced_form_approximation",
                "live_preview_note": "Fitted preview uses front-loaded basis plus a regime beta that rises in more open quarters and falls in more locked quarters.",
            }
        if model_key_in == "process_two_stage_realization_residual":
            return {
                "live_preview_mode": "exact_formula" if phase == "current" else "reduced_form_approximation",
                "live_preview_note": "Fitted preview uses a beta-35 basis base, applies a capped locked-setup pull, and subtracts a bounded realization residual.",
            }
        if model_key_in == "process_capacity_weighted_basis_strict":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Fitted preview uses the strict active-capacity-weighted basis process leg.",
            }
        if model_key_in == "process_front_loaded_ops_penalty":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Fitted preview uses the front-loaded process leg and subtracts the bounded ops penalty for the same quarter.",
            }
        if model_key_in == "bridge_front_loaded":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Fitted preview uses the front-loaded bridge timing leg from the market-proxy family while remaining separate from the official simple row.",
            }
        if model_key_in == "bridge_current_quarter_avg":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Fitted preview uses the current-quarter bridge timing leg from the market-proxy family while remaining separate from the official simple row.",
            }
        if model_key_in == "process_front_loaded":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Fitted preview uses the front-loaded process leg directly.",
            }
        if model_key_in == "process_front_loaded_ethanol_geo":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Fitted preview uses the front-loaded process leg plus the capped ethanol geography term.",
            }
        return {
            "live_preview_mode": "exact_formula",
            "live_preview_note": "",
        }

    bridge_spot_current, process_spot_current = _bridge_process_spot(current_ethanol, current_cbot, current_basis_uniform, current_gas)
    bridge_front_current, process_front_current = _bridge_process_spot(current_ethanol, current_cbot, current_basis_front, current_gas)
    bridge_75_current, process_75_current = _bridge_process_spot(current_ethanol, current_cbot, _basis_mix(current_basis_uniform, prior_basis, 0.75, 0.25), current_gas)
    bridge_50_current, process_50_current = _bridge_process_spot(current_ethanol, current_cbot, _basis_mix(current_basis_uniform, prior_basis, 0.50, 0.50), current_gas)
    bridge_spot_prior, process_spot_prior = _bridge_process_spot(prior_ethanol, prior_cbot, prior_basis_uniform, prior_gas)
    bridge_front_prior, process_front_prior = _bridge_process_spot(prior_ethanol, prior_cbot, prior_basis_front, prior_gas)
    prior_anchor_basis = pd.to_numeric(prior_anchor_record.get("weighted_basis_plant_count_usd_per_bu"), errors="coerce")
    bridge_75_prior, process_75_prior = _bridge_process_spot(prior_ethanol, prior_cbot, _basis_mix(prior_basis_uniform, prior_anchor_basis, 0.75, 0.25), prior_gas)
    bridge_50_prior, process_50_prior = _bridge_process_spot(prior_ethanol, prior_cbot, _basis_mix(prior_basis_uniform, prior_anchor_basis, 0.50, 0.50), prior_gas)
    bridge_noneth_current, process_noneth_current = _bridge_process_nonethanol(current_cbot, current_basis_uniform, current_gas)
    bridge_noneth_front_current, process_noneth_front_current = _bridge_process_nonethanol(current_cbot, current_basis_front, current_gas)
    bridge_noneth_75_current, process_noneth_75_current = _bridge_process_nonethanol(current_cbot, _basis_mix(current_basis_uniform, prior_basis, 0.75, 0.25), current_gas)
    bridge_noneth_50_current, process_noneth_50_current = _bridge_process_nonethanol(current_cbot, _basis_mix(current_basis_uniform, prior_basis, 0.50, 0.50), current_gas)

    bridge_spot_next, process_spot_next = _bridge_process_spot(next_ethanol, next_cbot, next_basis_uniform, next_gas)
    bridge_front_next, process_front_next = _bridge_process_spot(next_ethanol, next_cbot, next_basis_front, next_gas)
    bridge_75_next, process_75_next = _bridge_process_spot(next_ethanol, next_cbot, _basis_mix(next_basis_uniform, prior_basis, 0.75, 0.25), next_gas)
    bridge_50_next, process_50_next = _bridge_process_spot(next_ethanol, next_cbot, _basis_mix(next_basis_uniform, prior_basis, 0.50, 0.50), next_gas)
    bridge_noneth_next, process_noneth_next = _bridge_process_nonethanol(next_cbot, next_basis_uniform, next_gas)
    bridge_noneth_front_next, process_noneth_front_next = _bridge_process_nonethanol(next_cbot, next_basis_front, next_gas)
    bridge_noneth_75_next, process_noneth_75_next = _bridge_process_nonethanol(next_cbot, _basis_mix(next_basis_uniform, prior_basis, 0.75, 0.25), next_gas)
    bridge_noneth_50_next, process_noneth_50_next = _bridge_process_nonethanol(next_cbot, _basis_mix(next_basis_uniform, prior_basis, 0.50, 0.50), next_gas)
    bridge_spot_quarter_open, process_spot_quarter_open = _bridge_process_spot(quarter_open_ethanol, quarter_open_cbot, quarter_open_basis_uniform, quarter_open_gas)
    bridge_front_quarter_open, process_front_quarter_open = _bridge_process_spot(quarter_open_ethanol, quarter_open_cbot, quarter_open_basis_front, quarter_open_gas)
    bridge_75_quarter_open, process_75_quarter_open = _bridge_process_spot(quarter_open_ethanol, quarter_open_cbot, _basis_mix(quarter_open_basis_uniform, prior_basis, 0.75, 0.25), quarter_open_gas)
    bridge_50_quarter_open, process_50_quarter_open = _bridge_process_spot(quarter_open_ethanol, quarter_open_cbot, _basis_mix(quarter_open_basis_uniform, prior_basis, 0.50, 0.50), quarter_open_gas)
    _, process_basis_blend_current40_front60_prior = _bridge_process_spot(
        prior_ethanol,
        prior_cbot,
        _basis_mix(prior_basis_uniform, prior_basis_front, 0.40, 0.60),
        prior_gas,
    )
    _, process_basis_blend_current40_front60_current = _bridge_process_spot(
        current_ethanol,
        current_cbot,
        _basis_mix(current_basis_uniform, current_basis_front, 0.40, 0.60),
        current_gas,
    )
    _, process_basis_blend_current40_front60_next = _bridge_process_spot(
        next_ethanol,
        next_cbot,
        _basis_mix(next_basis_uniform, next_basis_front, 0.40, 0.60),
        next_gas,
    )
    _, process_basis_blend_current40_front60_quarter_open = _bridge_process_spot(
        quarter_open_ethanol,
        quarter_open_cbot,
        _basis_mix(quarter_open_basis_uniform, quarter_open_basis_front, 0.40, 0.60),
        quarter_open_gas,
    )
    _, process_basis_passthrough_beta35_prior = _bridge_process_spot(
        prior_ethanol,
        prior_cbot,
        _basis_mix(prior_basis_uniform, prior_basis_front, 0.35, 0.65),
        prior_gas,
    )
    _, process_basis_passthrough_beta35_current = _bridge_process_spot(
        current_ethanol,
        current_cbot,
        _basis_mix(current_basis_uniform, current_basis_front, 0.35, 0.65),
        current_gas,
    )
    _, process_basis_passthrough_beta35_next = _bridge_process_spot(
        next_ethanol,
        next_cbot,
        _basis_mix(next_basis_uniform, next_basis_front, 0.35, 0.65),
        next_gas,
    )
    _, process_basis_passthrough_beta35_quarter_open = _bridge_process_spot(
        quarter_open_ethanol,
        quarter_open_cbot,
        _basis_mix(quarter_open_basis_uniform, quarter_open_basis_front, 0.35, 0.65),
        quarter_open_gas,
    )
    _, process_basis_passthrough_beta65_prior = _bridge_process_spot(
        prior_ethanol,
        prior_cbot,
        _basis_mix(prior_basis_uniform, prior_basis_front, 0.65, 0.35),
        prior_gas,
    )
    _, process_basis_passthrough_beta65_current = _bridge_process_spot(
        current_ethanol,
        current_cbot,
        _basis_mix(current_basis_uniform, current_basis_front, 0.65, 0.35),
        current_gas,
    )
    _, process_basis_passthrough_beta65_next = _bridge_process_spot(
        next_ethanol,
        next_cbot,
        _basis_mix(next_basis_uniform, next_basis_front, 0.65, 0.35),
        next_gas,
    )
    _, process_basis_passthrough_beta65_quarter_open = _bridge_process_spot(
        quarter_open_ethanol,
        quarter_open_cbot,
        _basis_mix(quarter_open_basis_uniform, quarter_open_basis_front, 0.65, 0.35),
        quarter_open_gas,
    )

    prior_bridge_current = pd.to_numeric(prior_record.get("approx_market_bridge_proxy_usd_per_gal"), errors="coerce")
    prior_bridge_front = pd.to_numeric(prior_record.get("bridge_proxy_front_loaded_usd_per_gal"), errors="coerce")
    prior_process_current = pd.to_numeric(prior_record.get("process_proxy_current_quarter_avg_usd_per_gal"), errors="coerce")
    prior_process_front = pd.to_numeric(prior_record.get("process_proxy_front_loaded_usd_per_gal"), errors="coerce")
    prior_anchor_bridge_current = pd.to_numeric(prior_anchor_record.get("approx_market_bridge_proxy_usd_per_gal"), errors="coerce")
    prior_anchor_bridge_front = pd.to_numeric(prior_anchor_record.get("bridge_proxy_front_loaded_usd_per_gal"), errors="coerce")
    prior_anchor_process_current = pd.to_numeric(prior_anchor_record.get("process_proxy_current_quarter_avg_usd_per_gal"), errors="coerce")
    prior_anchor_process_front = pd.to_numeric(prior_anchor_record.get("process_proxy_front_loaded_usd_per_gal"), errors="coerce")
    prior_geo_term = _snapshot_geo_term(prior_snapshot)
    current_geo_term = _snapshot_geo_term(current_snapshot)
    quarter_open_geo_term = _snapshot_geo_term(quarter_open_market_snapshot)
    next_geo_term = _snapshot_geo_term(next_official_snapshot)
    next_target_quarter = next_official_snapshot.get("display_quarter") if isinstance(next_official_snapshot.get("display_quarter"), date) else None
    prior_ops_penalty = _ops_penalty_for_quarter(prior_display_quarter)
    current_ops_penalty = _ops_penalty_for_quarter(current_quarter_end)
    next_ops_penalty = _ops_penalty_for_quarter(next_target_quarter)
    prior_total_exec_penalty = _total_exec_penalty_for_quarter(prior_display_quarter)
    current_total_exec_penalty = _total_exec_penalty_for_quarter(current_quarter_end)
    next_total_exec_penalty = _total_exec_penalty_for_quarter(next_target_quarter)
    prior_utilization_penalty = _utilization_penalty_for_quarter(prior_display_quarter)
    current_utilization_penalty = _utilization_penalty_for_quarter(current_quarter_end)
    next_utilization_penalty = _utilization_penalty_for_quarter(next_target_quarter)
    prior_maintenance_delay_penalty = _maintenance_delay_penalty_for_quarter(prior_display_quarter)
    current_maintenance_delay_penalty = _maintenance_delay_penalty_for_quarter(current_quarter_end)
    next_maintenance_delay_penalty = _maintenance_delay_penalty_for_quarter(next_target_quarter)
    prior_inventory_drag_penalty = _inventory_drag_penalty_for_quarter(prior_display_quarter)
    current_inventory_drag_penalty = _inventory_drag_penalty_for_quarter(current_quarter_end)
    next_inventory_drag_penalty = _inventory_drag_penalty_for_quarter(next_target_quarter)
    prior_disclosed_share = {
        date(2025, 6, 30): 0.55,
        date(2025, 9, 30): 0.65,
        date(2025, 12, 31): 0.75,
    }.get(prior_display_quarter, 0.0)
    current_disclosed_share = {
        date(2025, 6, 30): 0.55,
        date(2025, 9, 30): 0.65,
        date(2025, 12, 31): 0.75,
    }.get(current_quarter_end, 0.0)
    next_disclosed_share = {
        date(2025, 6, 30): 0.55,
        date(2025, 9, 30): 0.65,
        date(2025, 12, 31): 0.75,
    }.get(next_target_quarter, 0.0)
    prior_pattern_share = _gpre_pattern_hedge_share(prior_display_quarter)
    current_pattern_share = _gpre_pattern_hedge_share(current_quarter_end)
    next_pattern_share = _gpre_pattern_hedge_share(next_target_quarter)
    prior_regime_beta = _gpre_regime_basis_passthrough_beta(prior_disclosed_share, prior_pattern_share)
    current_regime_beta = _gpre_regime_basis_passthrough_beta(current_disclosed_share, current_pattern_share)
    next_regime_beta = _gpre_regime_basis_passthrough_beta(next_disclosed_share, next_pattern_share)
    _, process_regime_basis_passthrough_prior = _bridge_process_spot(
        prior_ethanol,
        prior_cbot,
        _basis_mix(prior_basis_uniform, prior_basis_front, float(prior_regime_beta), 1.0 - float(prior_regime_beta)),
        prior_gas,
    )
    _, process_regime_basis_passthrough_current = _bridge_process_spot(
        current_ethanol,
        current_cbot,
        _basis_mix(current_basis_uniform, current_basis_front, float(current_regime_beta), 1.0 - float(current_regime_beta)),
        current_gas,
    )
    _, process_regime_basis_passthrough_next = _bridge_process_spot(
        next_ethanol,
        next_cbot,
        _basis_mix(next_basis_uniform, next_basis_front, float(next_regime_beta), 1.0 - float(next_regime_beta)),
        next_gas,
    )
    _, process_regime_basis_passthrough_quarter_open = _bridge_process_spot(
        quarter_open_ethanol,
        quarter_open_cbot,
        _basis_mix(quarter_open_basis_uniform, quarter_open_basis_front, float(current_regime_beta), 1.0 - float(current_regime_beta)),
        quarter_open_gas,
    )
    process_capacity_weighted_basis_strict_prior = prior_process_current
    process_capacity_weighted_basis_strict_current = process_spot_current
    process_capacity_weighted_basis_strict_next = process_spot_next
    process_capacity_weighted_basis_strict_quarter_open = process_spot_quarter_open

    def _hedge_blend(anchor_val: Any, spot_val: Any, hedge_share: float) -> Optional[float]:
        anchor_num = pd.to_numeric(anchor_val, errors="coerce")
        spot_num = pd.to_numeric(spot_val, errors="coerce")
        if pd.isna(spot_num):
            return None
        if pd.isna(anchor_num) or hedge_share <= 1e-12:
            return float(spot_num)
        return (float(hedge_share) * float(anchor_num)) + ((1.0 - float(hedge_share)) * float(spot_num))

    process_quarter_open_current50_exec_penalty_prior_base = _blend_optional_values(
        prior_anchor_process_front,
        process_spot_prior,
        anchor_weight=0.50,
        current_weight=0.50,
    )
    process_quarter_open_current50_exec_penalty_current_base = _blend_optional_values(
        process_front_quarter_open,
        process_spot_current,
        anchor_weight=0.50,
        current_weight=0.50,
    )
    process_quarter_open_current50_exec_penalty_quarter_open_base = _blend_optional_values(
        process_front_quarter_open,
        process_spot_current,
        anchor_weight=0.50,
        current_weight=0.50,
    )
    process_quarter_open_current50_exec_penalty_next_base = _blend_optional_values(
        process_front_quarter_open,
        process_spot_next,
        anchor_weight=0.50,
        current_weight=0.50,
    )
    process_two_stage_realization_residual_prior = (
        None
        if (locked_base := _gpre_locked_setup_value(
            process_basis_passthrough_beta35_prior,
            disclosed_share=prior_disclosed_share,
            pattern_share=prior_pattern_share,
            quarter_open_anchor=prior_anchor_process_front,
            cap=0.25,
        )) is None
        else float(locked_base)
        - _gpre_realization_residual_penalty(
            prior_utilization_penalty,
            prior_maintenance_delay_penalty,
            prior_inventory_drag_penalty,
        )
    )
    process_two_stage_realization_residual_current = (
        None
        if (locked_base := _gpre_locked_setup_value(
            process_basis_passthrough_beta35_current,
            disclosed_share=current_disclosed_share,
            pattern_share=current_pattern_share,
            quarter_open_anchor=process_front_quarter_open,
            cap=0.25,
        )) is None
        else float(locked_base)
        - _gpre_realization_residual_penalty(
            current_utilization_penalty,
            current_maintenance_delay_penalty,
            current_inventory_drag_penalty,
        )
    )
    process_two_stage_realization_residual_quarter_open = (
        None
        if (locked_base := _gpre_locked_setup_value(
            process_basis_passthrough_beta35_quarter_open,
            disclosed_share=current_disclosed_share,
            pattern_share=current_pattern_share,
            quarter_open_anchor=process_front_quarter_open,
            cap=0.25,
        )) is None
        else float(locked_base)
        - _gpre_realization_residual_penalty(
            current_utilization_penalty,
            current_maintenance_delay_penalty,
            current_inventory_drag_penalty,
        )
    )
    process_two_stage_realization_residual_next = (
        None
        if (locked_base := _gpre_locked_setup_value(
            process_basis_passthrough_beta35_next,
            disclosed_share=next_disclosed_share,
            pattern_share=next_pattern_share,
            quarter_open_anchor=process_front_quarter_open,
            cap=0.25,
        )) is None
        else float(locked_base)
        - _gpre_realization_residual_penalty(
            next_utilization_penalty,
            next_maintenance_delay_penalty,
            next_inventory_drag_penalty,
        )
    )

    def _chosen_model_pred_col(model_key_in: str) -> Optional[str]:
        return {
            "simple_market": "simple_market_proxy_usd_per_gal",
            "bridge_current_quarter_avg": "bridge_proxy_current_quarter_avg_usd_per_gal",
            "bridge_front_loaded": "bridge_proxy_front_loaded_usd_per_gal",
            "bridge_current75_prev25": "bridge_proxy_current75_prev25_usd_per_gal",
            "bridge_current50_prev50": "bridge_proxy_current50_prev50_usd_per_gal",
            "process_current_quarter_avg": "process_proxy_current_quarter_avg_usd_per_gal",
            "process_front_loaded": "process_proxy_front_loaded_usd_per_gal",
            "process_current75_prev25": "process_proxy_current75_prev25_usd_per_gal",
            "process_current50_prev50": "process_proxy_current50_prev50_usd_per_gal",
            "process_quarter_open_blend": "process_quarter_open_blend_usd_per_gal",
            "process_quarter_open_blend_ops_penalty": "process_quarter_open_blend_ops_penalty_usd_per_gal",
            "process_quarter_open_blend_hedge_realization": "process_quarter_open_blend_hedge_realization_usd_per_gal",
            "process_quarter_open_blend_exec_penalty": "process_quarter_open_blend_exec_penalty_usd_per_gal",
            "process_quarter_open_blend_utilization_penalty": "process_quarter_open_blend_utilization_penalty_usd_per_gal",
            "process_quarter_open_blend_maintenance_delay_penalty": "process_quarter_open_blend_maintenance_delay_penalty_usd_per_gal",
            "process_quarter_open_blend_inventory_timing_drag": "process_quarter_open_blend_inventory_timing_drag_usd_per_gal",
            "process_quarter_open_blend_locked_setup": "process_quarter_open_blend_locked_setup_usd_per_gal",
            "process_basis_blend_current40_front60": "process_basis_blend_current40_front60_usd_per_gal",
            "process_basis_passthrough_beta35": "process_basis_passthrough_beta35_usd_per_gal",
            "process_basis_passthrough_beta65": "process_basis_passthrough_beta65_usd_per_gal",
            "process_quarter_open_current50_exec_penalty": "process_quarter_open_current50_exec_penalty_usd_per_gal",
            "process_regime_basis_passthrough": "process_regime_basis_passthrough_usd_per_gal",
            "process_two_stage_realization_residual": "process_two_stage_realization_residual_usd_per_gal",
            "process_capacity_weighted_basis_strict": "process_capacity_weighted_basis_strict_usd_per_gal",
            "process_front_loaded_ops_penalty": "process_front_loaded_ops_penalty_usd_per_gal",
            "process_front_loaded_ethanol_geo": "process_front_loaded_ethanol_geo_usd_per_gal",
            "hedge_disclosed_bridge_prior_current": "hedge_memo_disclosed_bridge_prior_current_usd_per_gal",
            "hedge_disclosed_bridge_prior_front": "hedge_memo_disclosed_bridge_prior_front_usd_per_gal",
            "hedge_disclosed_process_prior_current": "hedge_memo_disclosed_process_prior_current_usd_per_gal",
            "hedge_disclosed_process_prior_front": "hedge_memo_disclosed_process_prior_front_usd_per_gal",
            "hedge_pattern_bridge_prior_current": "hedge_memo_pattern_bridge_prior_current_usd_per_gal",
            "hedge_pattern_bridge_prior_front": "hedge_memo_pattern_bridge_prior_front_usd_per_gal",
            "hedge_pattern_process_prior_current": "hedge_memo_pattern_process_prior_current_usd_per_gal",
            "hedge_pattern_process_prior_front": "hedge_memo_pattern_process_prior_front_usd_per_gal",
            "bid_adjusted_offset": "bridge_proxy_bid_adjusted_offset_usd_per_gal",
        }.get(str(model_key_in or "").strip())

    def _model_preview_value(model_key_in: str, *, phase: str) -> Optional[float]:
        if phase == "prior":
            if model_key_in == "bridge_current_quarter_avg":
                return bridge_spot_prior
            if model_key_in == "process_current_quarter_avg":
                return process_spot_prior
            if model_key_in == "bridge_front_loaded":
                return bridge_front_prior
            if model_key_in == "process_front_loaded":
                return process_front_prior
            if model_key_in == "bridge_current75_prev25":
                return bridge_75_prior
            if model_key_in == "process_current75_prev25":
                return process_75_prior
            if model_key_in == "bridge_current50_prev50":
                return bridge_50_prior
            if model_key_in == "process_current50_prev50":
                return process_50_prior
            if model_key_in == "process_quarter_open_blend":
                return _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
            if model_key_in == "process_quarter_open_blend_ops_penalty":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return None if blend_val is None else float(blend_val) - float(prior_ops_penalty)
            if model_key_in == "process_quarter_open_blend_hedge_realization":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return _gpre_hedge_realization_value(
                    blend_val,
                    disclosed_share=prior_disclosed_share,
                    pattern_share=prior_pattern_share,
                    disclosed_reference=_hedge_blend(prior_anchor_process_front, process_front_prior, prior_disclosed_share),
                    pattern_reference=_hedge_blend(prior_anchor_process_front, process_front_prior, prior_pattern_share),
                )
            if model_key_in == "process_quarter_open_blend_exec_penalty":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return None if blend_val is None else float(blend_val) - float(prior_total_exec_penalty)
            if model_key_in == "process_quarter_open_blend_utilization_penalty":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return None if blend_val is None else float(blend_val) - float(prior_utilization_penalty)
            if model_key_in == "process_quarter_open_blend_maintenance_delay_penalty":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return None if blend_val is None else float(blend_val) - float(prior_maintenance_delay_penalty)
            if model_key_in == "process_quarter_open_blend_inventory_timing_drag":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return None if blend_val is None else float(blend_val) - float(prior_inventory_drag_penalty)
            if model_key_in == "process_quarter_open_blend_locked_setup":
                blend_val = _quarter_blend_value(prior_anchor_process_front, process_spot_prior, prior_display_quarter)
                return _gpre_locked_setup_value(
                    blend_val,
                    disclosed_share=prior_disclosed_share,
                    pattern_share=prior_pattern_share,
                    quarter_open_anchor=prior_anchor_process_front,
                    cap=0.40,
                )
            if model_key_in == "process_basis_blend_current40_front60":
                return process_basis_blend_current40_front60_prior
            if model_key_in == "process_basis_passthrough_beta35":
                return process_basis_passthrough_beta35_prior
            if model_key_in == "process_basis_passthrough_beta65":
                return process_basis_passthrough_beta65_prior
            if model_key_in == "process_quarter_open_current50_exec_penalty":
                return None if process_quarter_open_current50_exec_penalty_prior_base is None else float(process_quarter_open_current50_exec_penalty_prior_base) - float(prior_total_exec_penalty)
            if model_key_in == "process_regime_basis_passthrough":
                return process_regime_basis_passthrough_prior
            if model_key_in == "process_two_stage_realization_residual":
                return process_two_stage_realization_residual_prior
            if model_key_in == "process_capacity_weighted_basis_strict":
                return process_capacity_weighted_basis_strict_prior
            if model_key_in == "process_front_loaded_ops_penalty":
                return None if process_front_prior is None else float(process_front_prior) - float(prior_ops_penalty)
            if model_key_in == "process_front_loaded_ethanol_geo":
                return None if process_front_prior is None else float(process_front_prior) + float(prior_geo_term)
            if model_key_in == "hedge_disclosed_bridge_prior_current":
                return _hedge_blend(prior_anchor_bridge_current, bridge_spot_prior, prior_disclosed_share)
            if model_key_in == "hedge_disclosed_bridge_prior_front":
                return _hedge_blend(prior_anchor_bridge_front, bridge_front_prior, prior_disclosed_share)
            if model_key_in == "hedge_disclosed_process_prior_current":
                return _hedge_blend(prior_anchor_process_current, process_spot_prior, prior_disclosed_share)
            if model_key_in == "hedge_disclosed_process_prior_front":
                return _hedge_blend(prior_anchor_process_front, process_front_prior, prior_disclosed_share)
            if model_key_in == "hedge_pattern_bridge_prior_current":
                return _hedge_blend(prior_anchor_bridge_current, bridge_spot_prior, prior_pattern_share)
            if model_key_in == "hedge_pattern_bridge_prior_front":
                return _hedge_blend(prior_anchor_bridge_front, bridge_front_prior, prior_pattern_share)
            if model_key_in == "hedge_pattern_process_prior_current":
                return _hedge_blend(prior_anchor_process_current, process_spot_prior, prior_pattern_share)
            if model_key_in == "hedge_pattern_process_prior_front":
                return _hedge_blend(prior_anchor_process_front, process_front_prior, prior_pattern_share)
            if model_key_in == "bid_adjusted_offset":
                return bridge_spot_prior
            return process_spot_prior
        if phase == "current":
            if model_key_in == "bridge_current_quarter_avg":
                return bridge_spot_current
            if model_key_in == "process_current_quarter_avg":
                return process_spot_current
            if model_key_in == "bridge_front_loaded":
                return bridge_front_current
            if model_key_in == "process_front_loaded":
                return process_front_current
            if model_key_in == "bridge_current75_prev25":
                return bridge_75_current
            if model_key_in == "process_current75_prev25":
                return process_75_current
            if model_key_in == "bridge_current50_prev50":
                return bridge_50_current
            if model_key_in == "process_current50_prev50":
                return process_50_current
            if model_key_in == "process_quarter_open_blend":
                return _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
            if model_key_in == "process_quarter_open_blend_ops_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_ops_penalty)
            if model_key_in == "process_quarter_open_blend_hedge_realization":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return _gpre_hedge_realization_value(
                    blend_val,
                    disclosed_share=current_disclosed_share,
                    pattern_share=current_pattern_share,
                    disclosed_reference=_hedge_blend(prior_process_front, process_spot_current, current_disclosed_share),
                    pattern_reference=_hedge_blend(prior_process_front, process_spot_current, current_pattern_share),
                )
            if model_key_in == "process_quarter_open_blend_exec_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_total_exec_penalty)
            if model_key_in == "process_quarter_open_blend_utilization_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_utilization_penalty)
            if model_key_in == "process_quarter_open_blend_maintenance_delay_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_maintenance_delay_penalty)
            if model_key_in == "process_quarter_open_blend_inventory_timing_drag":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_inventory_drag_penalty)
            if model_key_in == "process_quarter_open_blend_locked_setup":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return _gpre_locked_setup_value(
                    blend_val,
                    disclosed_share=current_disclosed_share,
                    pattern_share=current_pattern_share,
                    quarter_open_anchor=process_front_quarter_open,
                    cap=0.40,
                )
            if model_key_in == "process_basis_blend_current40_front60":
                return process_basis_blend_current40_front60_current
            if model_key_in == "process_basis_passthrough_beta35":
                return process_basis_passthrough_beta35_current
            if model_key_in == "process_basis_passthrough_beta65":
                return process_basis_passthrough_beta65_current
            if model_key_in == "process_quarter_open_current50_exec_penalty":
                return None if process_quarter_open_current50_exec_penalty_current_base is None else float(process_quarter_open_current50_exec_penalty_current_base) - float(current_total_exec_penalty)
            if model_key_in == "process_regime_basis_passthrough":
                return process_regime_basis_passthrough_current
            if model_key_in == "process_two_stage_realization_residual":
                return process_two_stage_realization_residual_current
            if model_key_in == "process_capacity_weighted_basis_strict":
                return process_capacity_weighted_basis_strict_current
            if model_key_in == "process_front_loaded_ops_penalty":
                return None if process_front_current is None else float(process_front_current) - float(current_ops_penalty)
            if model_key_in == "process_front_loaded_ethanol_geo":
                return None if process_front_current is None else float(process_front_current) + float(current_geo_term)
            if model_key_in == "hedge_disclosed_bridge_prior_current":
                return _hedge_blend(prior_bridge_current, bridge_spot_current, current_disclosed_share)
            if model_key_in == "hedge_disclosed_bridge_prior_front":
                return _hedge_blend(prior_bridge_front, bridge_spot_current, current_disclosed_share)
            if model_key_in == "hedge_disclosed_process_prior_current":
                return _hedge_blend(prior_process_current, process_spot_current, current_disclosed_share)
            if model_key_in == "hedge_disclosed_process_prior_front":
                return _hedge_blend(prior_process_front, process_spot_current, current_disclosed_share)
            if model_key_in == "hedge_pattern_bridge_prior_current":
                return _hedge_blend(prior_bridge_current, bridge_spot_current, current_pattern_share)
            if model_key_in == "hedge_pattern_bridge_prior_front":
                return _hedge_blend(prior_bridge_front, bridge_spot_current, current_pattern_share)
            if model_key_in == "hedge_pattern_process_prior_current":
                return _hedge_blend(prior_process_current, process_spot_current, current_pattern_share)
            if model_key_in == "hedge_pattern_process_prior_front":
                return _hedge_blend(prior_process_front, process_spot_current, current_pattern_share)
            if model_key_in == "bid_adjusted_offset":
                return bridge_spot_current
            return process_spot_current
        if phase == "quarter_open":
            if model_key_in == "bridge_current_quarter_avg":
                return bridge_spot_quarter_open
            if model_key_in == "process_current_quarter_avg":
                return process_spot_quarter_open
            if model_key_in == "bridge_front_loaded":
                return bridge_front_quarter_open
            if model_key_in == "process_front_loaded":
                return process_front_quarter_open
            if model_key_in == "bridge_current75_prev25":
                return bridge_75_quarter_open
            if model_key_in == "process_current75_prev25":
                return process_75_quarter_open
            if model_key_in == "bridge_current50_prev50":
                return bridge_50_quarter_open
            if model_key_in == "process_current50_prev50":
                return process_50_quarter_open
            if model_key_in == "process_quarter_open_blend":
                return _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
            if model_key_in == "process_quarter_open_blend_ops_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_ops_penalty)
            if model_key_in == "process_quarter_open_blend_hedge_realization":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return _gpre_hedge_realization_value(
                    blend_val,
                    disclosed_share=current_disclosed_share,
                    pattern_share=current_pattern_share,
                    disclosed_reference=_hedge_blend(prior_process_front, process_spot_current, current_disclosed_share),
                    pattern_reference=_hedge_blend(prior_process_front, process_spot_current, current_pattern_share),
                )
            if model_key_in == "process_quarter_open_blend_exec_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_total_exec_penalty)
            if model_key_in == "process_quarter_open_blend_utilization_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_utilization_penalty)
            if model_key_in == "process_quarter_open_blend_maintenance_delay_penalty":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_maintenance_delay_penalty)
            if model_key_in == "process_quarter_open_blend_inventory_timing_drag":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return None if blend_val is None else float(blend_val) - float(current_inventory_drag_penalty)
            if model_key_in == "process_quarter_open_blend_locked_setup":
                blend_val = _quarter_blend_value(process_front_quarter_open, process_spot_current, current_quarter_end)
                return _gpre_locked_setup_value(
                    blend_val,
                    disclosed_share=current_disclosed_share,
                    pattern_share=current_pattern_share,
                    quarter_open_anchor=process_front_quarter_open,
                    cap=0.40,
                )
            if model_key_in == "process_basis_blend_current40_front60":
                return process_basis_blend_current40_front60_quarter_open
            if model_key_in == "process_basis_passthrough_beta35":
                return process_basis_passthrough_beta35_quarter_open
            if model_key_in == "process_basis_passthrough_beta65":
                return process_basis_passthrough_beta65_quarter_open
            if model_key_in == "process_quarter_open_current50_exec_penalty":
                return None if process_quarter_open_current50_exec_penalty_quarter_open_base is None else float(process_quarter_open_current50_exec_penalty_quarter_open_base) - float(current_total_exec_penalty)
            if model_key_in == "process_regime_basis_passthrough":
                return process_regime_basis_passthrough_quarter_open
            if model_key_in == "process_two_stage_realization_residual":
                return process_two_stage_realization_residual_quarter_open
            if model_key_in == "process_capacity_weighted_basis_strict":
                return process_capacity_weighted_basis_strict_quarter_open
            if model_key_in == "process_front_loaded_ops_penalty":
                return None if process_front_quarter_open is None else float(process_front_quarter_open) - float(current_ops_penalty)
            if model_key_in == "process_front_loaded_ethanol_geo":
                return None if process_front_quarter_open is None else float(process_front_quarter_open) + float(quarter_open_geo_term)
            if model_key_in == "hedge_disclosed_bridge_prior_current":
                return _hedge_blend(prior_bridge_current, bridge_spot_quarter_open, current_disclosed_share)
            if model_key_in == "hedge_disclosed_bridge_prior_front":
                return _hedge_blend(prior_bridge_front, bridge_front_quarter_open, current_disclosed_share)
            if model_key_in == "hedge_disclosed_process_prior_current":
                return _hedge_blend(prior_process_current, process_spot_quarter_open, current_disclosed_share)
            if model_key_in == "hedge_disclosed_process_prior_front":
                return _hedge_blend(prior_process_front, process_front_quarter_open, current_disclosed_share)
            if model_key_in == "hedge_pattern_bridge_prior_current":
                return _hedge_blend(prior_bridge_current, bridge_spot_quarter_open, current_pattern_share)
            if model_key_in == "hedge_pattern_bridge_prior_front":
                return _hedge_blend(prior_bridge_front, bridge_front_quarter_open, current_pattern_share)
            if model_key_in == "hedge_pattern_process_prior_current":
                return _hedge_blend(prior_process_current, process_spot_quarter_open, current_pattern_share)
            if model_key_in == "hedge_pattern_process_prior_front":
                return _hedge_blend(prior_process_front, process_front_quarter_open, current_pattern_share)
            if model_key_in == "bid_adjusted_offset":
                return bridge_spot_quarter_open
            return process_spot_quarter_open
        if model_key_in == "bridge_current_quarter_avg":
            return bridge_spot_next
        if model_key_in == "process_current_quarter_avg":
            return process_spot_next
        if model_key_in == "bridge_front_loaded":
            return bridge_front_next
        if model_key_in == "process_front_loaded":
            return process_front_next
        if model_key_in == "bridge_current75_prev25":
            return bridge_75_next
        if model_key_in == "process_current75_prev25":
            return process_75_next
        if model_key_in == "bridge_current50_prev50":
            return bridge_50_next
        if model_key_in == "process_current50_prev50":
            return process_50_next
        if model_key_in == "process_quarter_open_blend":
            return process_spot_next
        if model_key_in == "process_quarter_open_blend_ops_penalty":
            return None if process_spot_next is None else float(process_spot_next) - float(next_ops_penalty)
        if model_key_in == "process_quarter_open_blend_hedge_realization":
            return _gpre_hedge_realization_value(
                process_spot_next,
                disclosed_share=next_disclosed_share,
                pattern_share=next_pattern_share,
                disclosed_reference=_hedge_blend(prior_process_front, process_spot_next, next_disclosed_share),
                pattern_reference=_hedge_blend(prior_process_front, process_spot_next, next_pattern_share),
            )
        if model_key_in == "process_quarter_open_blend_exec_penalty":
            return None if process_spot_next is None else float(process_spot_next) - float(next_total_exec_penalty)
        if model_key_in == "process_quarter_open_blend_utilization_penalty":
            return None if process_spot_next is None else float(process_spot_next) - float(next_utilization_penalty)
        if model_key_in == "process_quarter_open_blend_maintenance_delay_penalty":
            return None if process_spot_next is None else float(process_spot_next) - float(next_maintenance_delay_penalty)
        if model_key_in == "process_quarter_open_blend_inventory_timing_drag":
            return None if process_spot_next is None else float(process_spot_next) - float(next_inventory_drag_penalty)
        if model_key_in == "process_quarter_open_blend_locked_setup":
            return _gpre_locked_setup_value(
                process_spot_next,
                disclosed_share=next_disclosed_share,
                pattern_share=next_pattern_share,
                quarter_open_anchor=process_front_quarter_open,
                cap=0.40,
            )
        if model_key_in == "process_basis_blend_current40_front60":
            return process_basis_blend_current40_front60_next
        if model_key_in == "process_basis_passthrough_beta35":
            return process_basis_passthrough_beta35_next
        if model_key_in == "process_basis_passthrough_beta65":
            return process_basis_passthrough_beta65_next
        if model_key_in == "process_quarter_open_current50_exec_penalty":
            return None if process_quarter_open_current50_exec_penalty_next_base is None else float(process_quarter_open_current50_exec_penalty_next_base) - float(next_total_exec_penalty)
        if model_key_in == "process_regime_basis_passthrough":
            return process_regime_basis_passthrough_next
        if model_key_in == "process_two_stage_realization_residual":
            return process_two_stage_realization_residual_next
        if model_key_in == "process_capacity_weighted_basis_strict":
            return process_capacity_weighted_basis_strict_next
        if model_key_in == "process_front_loaded_ops_penalty":
            return None if process_front_next is None else float(process_front_next) - float(next_ops_penalty)
        if model_key_in == "process_front_loaded_ethanol_geo":
            return None if process_front_next is None else float(process_front_next) + float(next_geo_term)
        if model_key_in == "hedge_disclosed_bridge_prior_current":
            return _hedge_blend(prior_bridge_current, bridge_spot_next, next_disclosed_share)
        if model_key_in == "hedge_disclosed_bridge_prior_front":
            return _hedge_blend(prior_bridge_front, bridge_spot_next, next_disclosed_share)
        if model_key_in == "hedge_disclosed_process_prior_current":
            return _hedge_blend(prior_process_current, process_spot_next, next_disclosed_share)
        if model_key_in == "hedge_disclosed_process_prior_front":
            return _hedge_blend(prior_process_front, process_spot_next, next_disclosed_share)
        if model_key_in == "hedge_pattern_bridge_prior_current":
            return _hedge_blend(prior_bridge_current, bridge_spot_next, next_pattern_share)
        if model_key_in == "hedge_pattern_bridge_prior_front":
            return _hedge_blend(prior_bridge_front, bridge_spot_next, next_pattern_share)
        if model_key_in == "hedge_pattern_process_prior_current":
            return _hedge_blend(prior_process_current, process_spot_next, next_pattern_share)
        if model_key_in == "hedge_pattern_process_prior_front":
            return _hedge_blend(prior_process_front, process_spot_next, next_pattern_share)
        if model_key_in == "bid_adjusted_offset":
            return bridge_spot_next
        return process_spot_next

    def _model_formula_helper(model_key_in: str, *, phase: str) -> Dict[str, Any]:
        if phase == "current":
            disclosed_share = current_disclosed_share
            pattern_share = current_pattern_share
            bridge_components = {
                "bridge_current_quarter_avg": bridge_noneth_current,
                "bridge_front_loaded": bridge_noneth_front_current,
                "bridge_current75_prev25": bridge_noneth_75_current,
                "bridge_current50_prev50": bridge_noneth_50_current,
                "bid_adjusted_offset": bridge_noneth_current,
            }
            process_components = {
                "process_current_quarter_avg": process_noneth_current,
                "process_front_loaded": process_noneth_front_current,
                "process_current75_prev25": process_noneth_75_current,
                "process_current50_prev50": process_noneth_50_current,
                "process_quarter_open_blend_ops_penalty": None,
                "process_quarter_open_blend_hedge_realization": None,
                "process_quarter_open_blend_exec_penalty": None,
                "process_quarter_open_blend_utilization_penalty": None,
                "process_quarter_open_blend_maintenance_delay_penalty": None,
                "process_quarter_open_blend_inventory_timing_drag": None,
                "process_quarter_open_blend_locked_setup": None,
                "process_front_loaded_ops_penalty": (
                    None
                    if process_noneth_front_current is None
                    else float(process_noneth_front_current) - float(current_ops_penalty)
                ),
                "process_front_loaded_ethanol_geo": (
                    None
                    if process_noneth_front_current is None
                    else float(process_noneth_front_current) + float(current_geo_term)
                ),
            }
        else:
            disclosed_share = next_disclosed_share
            pattern_share = next_pattern_share
            bridge_components = {
                "bridge_current_quarter_avg": bridge_noneth_next,
                "bridge_front_loaded": bridge_noneth_front_next,
                "bridge_current75_prev25": bridge_noneth_75_next,
                "bridge_current50_prev50": bridge_noneth_50_next,
                "bid_adjusted_offset": bridge_noneth_next,
            }
            process_components = {
                "process_current_quarter_avg": process_noneth_next,
                "process_front_loaded": process_noneth_front_next,
                "process_current75_prev25": process_noneth_75_next,
                "process_current50_prev50": process_noneth_50_next,
                "process_quarter_open_blend_ops_penalty": None,
                "process_quarter_open_blend_hedge_realization": None,
                "process_quarter_open_blend_exec_penalty": None,
                "process_quarter_open_blend_utilization_penalty": None,
                "process_quarter_open_blend_maintenance_delay_penalty": None,
                "process_quarter_open_blend_inventory_timing_drag": None,
                "process_quarter_open_blend_locked_setup": None,
                "process_front_loaded_ops_penalty": (
                    None
                    if process_noneth_front_next is None
                    else float(process_noneth_front_next) - float(next_ops_penalty)
                ),
                "process_front_loaded_ethanol_geo": (
                    None
                    if process_noneth_front_next is None
                    else float(process_noneth_front_next) + float(next_geo_term)
                ),
            }
        if model_key_in == "process_quarter_open_blend":
            if phase == "current":
                return _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=0.0,
                    phase_label="current",
                )
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "exact_formula",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_ops_penalty":
            if phase == "current":
                return _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=float(current_ops_penalty),
                    phase_label="current",
                    penalty_note=f"Includes the bounded ops penalty of {float(current_ops_penalty):.3f} $/gal.",
                )
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            intercept = (intercept - float(next_ops_penalty)) if pd.notna(intercept) else intercept
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "reduced_form_approximation",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_hedge_realization":
            disclosed_share_raw = current_disclosed_share if phase == "current" else next_disclosed_share
            pattern_share_raw = current_pattern_share if phase == "current" else next_pattern_share
            hedge_weight, hedge_source = _gpre_effective_hedge_share(disclosed_share_raw, pattern_share_raw, cap=0.35)
            base_helper = (
                _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=0.0,
                    phase_label="current",
                )
                if phase == "current"
                else _formula_helper_payload(
                    status="ok" if pd.notna(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")) else "no_data",
                    mode="process",
                    slope=1.0 if pd.notna(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")) else None,
                    intercept=None if pd.isna(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")) else float(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")),
                    hedge_share=0.0,
                    anchor=None,
                    live_preview_mode="reduced_form_approximation",
                    live_preview_note="",
                )
            )
            hedge_model_key = (
                "hedge_disclosed_process_prior_front"
                if hedge_source == "disclosed"
                else "hedge_pattern_process_prior_front"
            )
            hedge_helper = _model_formula_helper(hedge_model_key, phase=phase)
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _combine_linear_formula_helpers(
                base_helper,
                hedge_helper,
                secondary_weight=hedge_weight,
                live_preview_mode=preview_story.get("live_preview_mode") or ("exact_formula" if phase == "current" else "reduced_form_approximation"),
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_exec_penalty":
            if phase == "current":
                return _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=float(current_total_exec_penalty),
                    phase_label="current",
                    penalty_note=f"Includes the bounded severe execution penalty of {float(current_total_exec_penalty):.3f} $/gal.",
                )
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            intercept = (intercept - float(next_total_exec_penalty)) if pd.notna(intercept) else intercept
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "reduced_form_approximation",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_utilization_penalty":
            if phase == "current":
                return _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=float(current_utilization_penalty),
                    phase_label="current",
                    penalty_note=f"Includes the bounded utilization penalty of {float(current_utilization_penalty):.3f} $/gal.",
                )
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            intercept = (intercept - float(next_utilization_penalty)) if pd.notna(intercept) else intercept
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "reduced_form_approximation",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_maintenance_delay_penalty":
            if phase == "current":
                return _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=float(current_maintenance_delay_penalty),
                    phase_label="current",
                    penalty_note=f"Includes the bounded maintenance-delay penalty of {float(current_maintenance_delay_penalty):.3f} $/gal.",
                )
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            intercept = (intercept - float(next_maintenance_delay_penalty)) if pd.notna(intercept) else intercept
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "reduced_form_approximation",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_inventory_timing_drag":
            if phase == "current":
                return _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=float(current_inventory_drag_penalty),
                    phase_label="current",
                    penalty_note=f"Includes the bounded inventory/timing drag of {float(current_inventory_drag_penalty):.3f} $/gal.",
                )
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            intercept = (intercept - float(next_inventory_drag_penalty)) if pd.notna(intercept) else intercept
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "reduced_form_approximation",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in == "process_quarter_open_blend_locked_setup":
            locked_share, _ = _gpre_effective_hedge_share(disclosed_share, pattern_share, cap=0.40)
            base_helper = (
                _process_blend_formula_payload(
                    anchor_proxy=process_front_quarter_open,
                    current_nonethanol=process_noneth_current,
                    qd_in=current_quarter_end,
                    penalty=0.0,
                    phase_label="current",
                )
                if phase == "current"
                else _formula_helper_payload(
                    status="ok" if pd.notna(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")) else "no_data",
                    mode="process",
                    slope=1.0 if pd.notna(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")) else None,
                    intercept=None if pd.isna(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")) else float(pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")),
                    hedge_share=0.0,
                    anchor=None,
                    live_preview_mode="reduced_form_approximation",
                    live_preview_note="",
                )
            )
            anchor_helper = _formula_helper_payload(
                status="ok" if pd.notna(pd.to_numeric(process_front_quarter_open, errors="coerce")) else "no_data",
                mode="process_anchor",
                slope=0.0 if pd.notna(pd.to_numeric(process_front_quarter_open, errors="coerce")) else None,
                intercept=None if pd.isna(pd.to_numeric(process_front_quarter_open, errors="coerce")) else float(pd.to_numeric(process_front_quarter_open, errors="coerce")),
                hedge_share=0.0,
                anchor=None if pd.isna(pd.to_numeric(process_front_quarter_open, errors="coerce")) else float(pd.to_numeric(process_front_quarter_open, errors="coerce")),
                live_preview_mode="reduced_form_approximation" if phase != "current" else "exact_formula",
                live_preview_note="",
            )
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _combine_linear_formula_helpers(
                base_helper,
                anchor_helper,
                secondary_weight=locked_share,
                live_preview_mode=preview_story.get("live_preview_mode") or ("exact_formula" if phase == "current" else "reduced_form_approximation"),
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in in {
            "process_basis_blend_current40_front60",
            "process_basis_passthrough_beta35",
            "process_basis_passthrough_beta65",
            "process_quarter_open_current50_exec_penalty",
            "process_regime_basis_passthrough",
            "process_two_stage_realization_residual",
            "process_capacity_weighted_basis_strict",
        }:
            ethanol_phase = current_ethanol if phase == "current" else next_ethanol
            helper = _simple_formula_helper_from_full(
                _model_preview_value(model_key_in, phase=phase),
                ethanol_phase,
                mode="process",
            )
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            helper["live_preview_mode"] = preview_story.get("live_preview_mode") or ("exact_formula" if phase == "current" else "reduced_form_approximation")
            helper["live_preview_note"] = preview_story.get("live_preview_note") or ""
            return helper
        if model_key_in in bridge_components:
            intercept = pd.to_numeric(bridge_components.get(model_key_in), errors="coerce")
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="bridge",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "exact_formula",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        if model_key_in in process_components:
            intercept = pd.to_numeric(process_components.get(model_key_in), errors="coerce")
            preview_story = _gpre_phase_preview_story(model_key_in, phase=phase)
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
                live_preview_mode=preview_story.get("live_preview_mode") or "exact_formula",
                live_preview_note=preview_story.get("live_preview_note") or "",
            )
        hedge_specs = {
            "hedge_disclosed_bridge_prior_current": ("bridge", prior_bridge_current, bridge_noneth_current if phase == "current" else bridge_noneth_next, disclosed_share),
            "hedge_disclosed_bridge_prior_front": ("bridge", prior_bridge_front, bridge_noneth_current if phase == "current" else bridge_noneth_next, disclosed_share),
            "hedge_disclosed_process_prior_current": ("process", prior_process_current, process_noneth_current if phase == "current" else process_noneth_next, disclosed_share),
            "hedge_disclosed_process_prior_front": ("process", prior_process_front, process_noneth_current if phase == "current" else process_noneth_next, disclosed_share),
            "hedge_pattern_bridge_prior_current": ("bridge", prior_bridge_current, bridge_noneth_current if phase == "current" else bridge_noneth_next, pattern_share),
            "hedge_pattern_bridge_prior_front": ("bridge", prior_bridge_front, bridge_noneth_current if phase == "current" else bridge_noneth_next, pattern_share),
            "hedge_pattern_process_prior_current": ("process", prior_process_current, process_noneth_current if phase == "current" else process_noneth_next, pattern_share),
            "hedge_pattern_process_prior_front": ("process", prior_process_front, process_noneth_current if phase == "current" else process_noneth_next, pattern_share),
        }
        hedge_spec = hedge_specs.get(model_key_in)
        if hedge_spec is None:
            intercept = pd.to_numeric(process_components.get("process_current_quarter_avg"), errors="coerce")
            return _formula_helper_payload(
                status="ok" if pd.notna(intercept) else "no_data",
                mode="process",
                slope=1.0 if pd.notna(intercept) else None,
                intercept=None if pd.isna(intercept) else float(intercept),
                hedge_share=0.0,
                anchor=None,
            )
        mode, anchor_raw, spot_component_raw, hedge_share_raw = hedge_spec
        anchor_num = pd.to_numeric(anchor_raw, errors="coerce")
        spot_component_num = pd.to_numeric(spot_component_raw, errors="coerce")
        hedge_share_num = pd.to_numeric(hedge_share_raw, errors="coerce")
        if pd.isna(spot_component_num):
            return {
                "status": "no_data",
                "mode": f"hedge_{mode}",
                "slope": None,
                "intercept": None,
                "hedge_share": 0.0 if pd.isna(hedge_share_num) else float(hedge_share_num),
                "anchor": None if pd.isna(anchor_num) else float(anchor_num),
            }
        effective_share = 0.0 if pd.isna(anchor_num) or pd.isna(hedge_share_num) or float(hedge_share_num) <= 1e-12 else float(hedge_share_num)
        slope = 1.0 - effective_share
        intercept = (effective_share * float(anchor_num)) + ((1.0 - effective_share) * float(spot_component_num)) if effective_share > 0 else float(spot_component_num)
        return {
            "status": "ok",
            "mode": f"hedge_{mode}",
            "slope": float(slope),
            "intercept": float(intercept),
            "hedge_share": float(effective_share),
            "anchor": None if pd.isna(anchor_num) else float(anchor_num),
        }

    def _prior_quarter_fitted_frame() -> Dict[str, Any]:
        if not isinstance(prior_display_quarter, date):
            return {
                "quarter_end": None,
                "value": None,
                "status": "no_data",
                "model_key": str(chosen_model_key or ""),
                "source": "quarterly_df.gpre_proxy_official_usd_per_gal",
            }
        value_num = pd.to_numeric(prior_record.get("gpre_proxy_official_usd_per_gal"), errors="coerce")
        source_txt = "quarterly_df.gpre_proxy_official_usd_per_gal"
        if pd.isna(value_num):
            pred_col = _chosen_model_pred_col(chosen_model_key)
            if pred_col:
                value_num = pd.to_numeric(prior_record.get(pred_col), errors="coerce")
                if pd.notna(value_num):
                    source_txt = f"quarterly_df.{pred_col}"
        if pd.isna(value_num):
            value_num = pd.to_numeric(_model_preview_value(chosen_model_key, phase="prior"), errors="coerce")
            source_txt = "prior_snapshot.model_preview"
        return {
            "quarter_end": prior_display_quarter,
            "value": None if pd.isna(value_num) else float(value_num),
            "status": "ok" if pd.notna(value_num) else "no_data",
            "model_key": str(prior_record.get("gpre_proxy_model_key") or chosen_model_key or ""),
            "source": source_txt,
        }

    current_fitted_val = _model_preview_value(chosen_model_key, phase="current")
    next_fitted_val = _model_preview_value(chosen_model_key, phase="next")
    current_formula_helper = _model_formula_helper(chosen_model_key, phase="current")
    next_formula_helper = _model_formula_helper(chosen_model_key, phase="next")
    quarter_open_preview_story = _gpre_phase_preview_story(chosen_model_key, phase="quarter_open")

    def _frame_with_preview_meta(
        frame_in: Dict[str, Any],
        *,
        live_preview_mode: str = "",
        live_preview_note: str = "",
    ) -> Dict[str, Any]:
        frame_out = dict(frame_in or {})
        if str(live_preview_mode or "").strip():
            frame_out["live_preview_mode"] = str(live_preview_mode or "").strip()
        if str(live_preview_note or "").strip():
            frame_out["live_preview_note"] = str(live_preview_note or "").strip()
        return frame_out

    official_frames = {
        "prior_quarter": {
            "quarter_end": prior_display_quarter,
            "value": _gpre_snapshot_simple_proxy_usd_per_gal(prior_snapshot, ethanol_yield=ethanol_yield_num),
            "status": str(prior_snapshot.get("process_status") or "no_data"),
        },
        "quarter_open": {
            "quarter_end": current_quarter_end,
            "value": pd.to_numeric((quarter_open_entry or {}).get("official_simple_proxy_usd_per_gal"), errors="coerce"),
            "status": str((quarter_open_entry or {}).get("status") or "no_snapshot"),
        },
        "current_qtd": {
            "quarter_end": current_quarter_end,
            "value": _gpre_snapshot_simple_proxy_usd_per_gal(current_snapshot, ethanol_yield=ethanol_yield_num),
            "status": str(current_snapshot.get("process_status") or "no_data"),
        },
        "next_quarter_thesis": {
            "quarter_end": next_target_quarter,
            "value": _gpre_snapshot_simple_proxy_usd_per_gal(next_official_snapshot, ethanol_yield=ethanol_yield_num),
            "status": str(next_official_snapshot.get("process_status") or "no_data"),
        },
    }
    quarter_open_official_val = pd.to_numeric(official_frames["quarter_open"]["value"], errors="coerce")
    official_frames["quarter_open"]["value"] = None if pd.isna(quarter_open_official_val) else float(quarter_open_official_val)
    quarter_open_fitted_raw = pd.to_numeric((quarter_open_entry or {}).get("gpre_proxy_official_usd_per_gal"), errors="coerce")
    quarter_open_fitted_mode = "snapshot_value"
    quarter_open_fitted_note = "Quarter-open fitted preview uses the stored chosen-model snapshot when available."
    if pd.isna(quarter_open_fitted_raw) and str((quarter_open_entry or {}).get("status") or "") == "ok":
        quarter_open_fitted_raw = pd.to_numeric(_model_preview_value(chosen_model_key, phase="quarter_open"), errors="coerce")
        quarter_open_fitted_mode = str(quarter_open_preview_story.get("live_preview_mode") or "model_preview_fallback")
        quarter_open_fitted_note = str(quarter_open_preview_story.get("live_preview_note") or "")
    prior_fitted_frame = _prior_quarter_fitted_frame()
    gpre_frames = {
        "prior_quarter": prior_fitted_frame,
        "quarter_open": _frame_with_preview_meta({
            "quarter_end": current_quarter_end,
            "value": None if pd.isna(quarter_open_fitted_raw) else float(quarter_open_fitted_raw),
            "status": str((quarter_open_entry or {}).get("status") or "no_snapshot"),
        }, live_preview_mode=quarter_open_fitted_mode, live_preview_note=quarter_open_fitted_note),
        "current_qtd": _frame_with_preview_meta({
            "quarter_end": current_quarter_end,
            "value": current_fitted_val,
            "status": "ok" if current_fitted_val is not None else "no_data",
        }, live_preview_mode=current_formula_helper.get("live_preview_mode"), live_preview_note=current_formula_helper.get("live_preview_note")),
        "next_quarter_thesis": _frame_with_preview_meta({
            "quarter_end": next_target_quarter,
            "value": next_fitted_val,
            "status": "ok" if next_fitted_val is not None else "no_data",
        }, live_preview_mode=next_formula_helper.get("live_preview_mode"), live_preview_note=next_formula_helper.get("live_preview_note")),
    }
    frozen_snapshot_entry = {
        "snapshot_as_of": today_ref,
        "source_quarter_end": current_quarter_end,
        "target_quarter_end": next_target_quarter,
        "provenance": "frozen_snapshot",
        "next_quarter_thesis_snapshot": next_thesis_snapshot,
        "official_market_snapshot": next_official_snapshot,
        "official_simple_proxy_usd_per_gal": _gpre_snapshot_simple_proxy_usd_per_gal(next_official_snapshot, ethanol_yield=ethanol_yield_num),
        "gpre_proxy_official_usd_per_gal": next_fitted_val,
        "gpre_proxy_model_key": chosen_model_key,
        "gpre_proxy_family": chosen_family,
        "gpre_proxy_family_label": chosen_family_label,
        "gpre_proxy_timing_rule": chosen_timing,
        "official_corn_basis_provenance": str(next_official_snapshot.get("official_corn_basis_provenance") or ""),
        "message": str(next_official_snapshot.get("message") or ""),
    }
    if not isinstance(next_target_quarter, date):
        frozen_snapshot_entry = {}
    return {
        "prior_market_snapshot": prior_snapshot,
        "current_qtd_market_snapshot": current_snapshot,
        "next_quarter_thesis_snapshot": next_thesis_snapshot,
        "simple_crush_history_rows": list(simple_crush_history_rows or []),
        "official_frames": official_frames,
        "gpre_proxy_frames": gpre_frames,
        "quarter_open_snapshot_status": str((quarter_open_entry or {}).get("status") or "no_snapshot"),
        "quarter_open_target_quarter_end": quarter_open_entry.get("target_quarter_end") if isinstance(quarter_open_entry, dict) else current_quarter_end,
        "quarter_open_official_proxy_usd_per_gal": official_frames["quarter_open"]["value"],
        "quarter_open_gpre_proxy_usd_per_gal": gpre_frames["quarter_open"]["value"],
        "quarter_open_market_inputs": dict((quarter_open_market_snapshot or {}).get("current_market") or {}),
        "quarter_open_process_inputs": dict((quarter_open_market_snapshot or {}).get("current_process") or {}),
        "quarter_open_snapshot_as_of": (quarter_open_entry or {}).get("snapshot_as_of"),
        "quarter_open_snapshot_model_key": str((quarter_open_entry or {}).get("gpre_proxy_model_key") or ""),
        "quarter_open_provenance": quarter_open_provenance,
        "quarter_open_market_snapshot": quarter_open_market_snapshot,
        "next_thesis_preview_snapshot": next_official_snapshot,
        "next_thesis_frozen_snapshot_entry": frozen_snapshot_entry,
        "gpre_proxy_formula_helpers": {
            "current_qtd": current_formula_helper,
            "next_quarter_thesis": next_formula_helper,
        },
    }


def _same_quarter_last_year_date(qd: Any) -> Optional[date]:
    if not isinstance(qd, date):
        return None
    try:
        return date(qd.year - 1, qd.month, qd.day)
    except Exception:
        return None


def _build_gpre_proxy_implied_results_bundle(
    overlay_preview_bundle: Optional[Dict[str, Any]],
    *,
    reported_gallons_produced_by_quarter: Optional[Dict[date, float]],
    denominator_policy_by_quarter: Optional[Dict[date, str]],
    ticker_root: Optional[Path],
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    frame_order = ("prior_quarter", "quarter_open", "current_qtd", "next_quarter_thesis")
    frame_labels = {
        "prior_quarter": "Prior quarter",
        "quarter_open": "Quarter-open proxy",
        "current_qtd": "Current QTD",
        "next_quarter_thesis": "Next quarter thesis",
    }
    official_frames = dict((overlay_preview_bundle or {}).get("official_frames") or {})
    gpre_frames = dict((overlay_preview_bundle or {}).get("gpre_proxy_frames") or {})
    gallons_by_quarter: Dict[date, float] = {}
    for raw_qd, raw_val in dict(reported_gallons_produced_by_quarter or {}).items():
        qd = parse_quarter_like(raw_qd)
        gallons_num = pd.to_numeric(raw_val, errors="coerce")
        if qd is None or pd.isna(gallons_num):
            continue
        gallons_float = float(gallons_num)
        if not np.isfinite(gallons_float) or gallons_float <= 0.0:
            continue
        gallons_by_quarter[qd] = gallons_float
    result: Dict[str, Any] = {
        "title": "Proxy-implied results ($m)",
        "note": "Proxy-implied translation only; not an actual future reported result forecast.",
        "frame_order": list(frame_order),
        "frames": {},
        "frame_records": [],
    }
    effective_history = _gpre_resolve_plant_capacity_history(plant_capacity_history, ticker_root=ticker_root)
    for frame_key in frame_order:
        official_frame = dict(official_frames.get(frame_key) or {})
        gpre_frame = dict(gpre_frames.get(frame_key) or {})
        target_quarter_end = parse_quarter_like(
            official_frame.get("quarter_end") or gpre_frame.get("quarter_end")
        )
        same_quarter_last_year = (
            _same_quarter_last_year_date(target_quarter_end)
            if isinstance(target_quarter_end, date)
            else None
        )
        official_proxy_num = pd.to_numeric(official_frame.get("value"), errors="coerce")
        gpre_proxy_num = pd.to_numeric(gpre_frame.get("value"), errors="coerce")
        record: Dict[str, Any] = {
            "frame_key": frame_key,
            "frame_label": frame_labels.get(frame_key, str(frame_key or "").replace("_", " ").title()),
            "target_quarter_end": target_quarter_end,
            "same_quarter_last_year": same_quarter_last_year,
            "gallons_source_kind": "",
            "implied_gallons": None,
            "implied_gallons_raw": None,
            "implied_gallons_million_display": None,
            "volume_basis_display": "Unavailable",
            "volume_basis_comment": "",
            "footprint_scale_factor": None,
            "current_operating_plant_count": None,
            "ly_operating_plant_count": None,
            "current_active_capacity_mmgy": None,
            "ly_active_capacity_mmgy": None,
            "quarter_capacity_ceiling_gallons_raw": None,
            "quarter_capacity_ceiling_gallons_million_display": None,
            "reasonableness_tolerance_ratio": 0.05,
            "reasonableness_status": "unavailable",
            "reasonableness_excess_ratio": None,
            "reasonableness_comment": "",
            "official_proxy_usd_per_gal": (None if pd.isna(official_proxy_num) else float(official_proxy_num)),
            "gpre_proxy_usd_per_gal": (None if pd.isna(gpre_proxy_num) else float(gpre_proxy_num)),
            "official_proxy_implied_result_usd_m": None,
            "gpre_proxy_implied_result_usd_m": None,
            "status": "unavailable",
            "reason_unavailable": "",
        }
        if not isinstance(target_quarter_end, date):
            record["reason_unavailable"] = "No target quarter resolved for this frame."
        elif frame_key == "prior_quarter":
            current_footprint = _gpre_footprint_for_quarter(
                target_quarter_end,
                ticker_root=ticker_root,
                plant_capacity_history=effective_history,
            )
            current_plants = int(
                pd.to_numeric(current_footprint.get("operating_plant_count"), errors="coerce") or 0
            )
            current_capacity = pd.to_numeric(current_footprint.get("active_capacity_mmgy"), errors="coerce")
            record["current_operating_plant_count"] = current_plants if current_plants > 0 else None
            record["current_active_capacity_mmgy"] = (
                None
                if pd.isna(current_capacity) or float(current_capacity) <= 0.0
                else float(current_capacity)
            )
            actual_gallons = gallons_by_quarter.get(target_quarter_end)
            if actual_gallons is not None:
                record["implied_gallons"] = float(actual_gallons)
                record["implied_gallons_raw"] = float(actual_gallons)
                record["implied_gallons_million_display"] = float(actual_gallons) / 1_000_000.0
                record["gallons_source_kind"] = "actual_prior_quarter_gallons_produced"
                record["volume_basis_display"] = "Prior quarter actual produced gallons"
                record["volume_basis_comment"] = (
                    f"Uses actual {_quarter_label(target_quarter_end)} gallons produced."
                )
                record["status"] = "ok"
                record["reason_unavailable"] = ""
            elif not isinstance(same_quarter_last_year, date):
                record["reason_unavailable"] = "Could not resolve the same quarter last year for prior-quarter fallback."
            else:
                ly_actual_gallons = gallons_by_quarter.get(same_quarter_last_year)
                if ly_actual_gallons is None:
                    record["reason_unavailable"] = (
                        f"No gallons produced available for {_quarter_label(target_quarter_end)} or {_quarter_label(same_quarter_last_year)}."
                    )
                else:
                    ly_footprint = _gpre_footprint_for_quarter(
                        same_quarter_last_year,
                        ticker_root=ticker_root,
                        plant_capacity_history=effective_history,
                    )
                    ly_plants = int(
                        pd.to_numeric(ly_footprint.get("operating_plant_count"), errors="coerce") or 0
                    )
                    ly_capacity = pd.to_numeric(ly_footprint.get("active_capacity_mmgy"), errors="coerce")
                    record["ly_operating_plant_count"] = ly_plants if ly_plants > 0 else None
                    record["ly_active_capacity_mmgy"] = (
                        None
                        if pd.isna(ly_capacity) or float(ly_capacity) <= 0.0
                        else float(ly_capacity)
                    )
                    if (
                        pd.notna(current_capacity)
                        and pd.notna(ly_capacity)
                        and float(current_capacity) > 0.0
                        and float(ly_capacity) > 0.0
                    ):
                        scale_factor = float(current_capacity) / float(ly_capacity)
                        implied_gallons = float(ly_actual_gallons) * scale_factor
                        record["gallons_source_kind"] = "fallback_yoy_same_quarter_gallons_produced_capacity_scaled"
                        record["footprint_scale_factor"] = scale_factor
                        record["implied_gallons"] = implied_gallons
                        record["implied_gallons_raw"] = implied_gallons
                        record["implied_gallons_million_display"] = implied_gallons / 1_000_000.0
                        record["volume_basis_display"] = "Fallback: YoY produced gallons adjusted to active capacity"
                        record["volume_basis_comment"] = (
                            f"{_quarter_label(same_quarter_last_year)} gallons produced scaled by "
                            f"{float(current_capacity):.0f}/{float(ly_capacity):.0f} MMgy active capacity."
                        )
                        record["status"] = "ok"
                        record["reason_unavailable"] = ""
                    else:
                        record["reason_unavailable"] = (
                            "Quarter-aware active capacity could not be resolved for prior-quarter produced fallback."
                        )
        else:
            if not isinstance(same_quarter_last_year, date):
                record["reason_unavailable"] = "Could not resolve the same quarter last year."
            else:
                ly_actual_gallons = gallons_by_quarter.get(same_quarter_last_year)
                if ly_actual_gallons is None:
                    record["reason_unavailable"] = (
                        f"No gallons produced available for {_quarter_label(same_quarter_last_year)}."
                    )
                else:
                    current_footprint = _gpre_footprint_for_quarter(
                        target_quarter_end,
                        ticker_root=ticker_root,
                        plant_capacity_history=effective_history,
                    )
                    ly_footprint = _gpre_footprint_for_quarter(
                        same_quarter_last_year,
                        ticker_root=ticker_root,
                        plant_capacity_history=effective_history,
                    )
                    current_plants = int(
                        pd.to_numeric(current_footprint.get("operating_plant_count"), errors="coerce") or 0
                    )
                    ly_plants = int(
                        pd.to_numeric(ly_footprint.get("operating_plant_count"), errors="coerce") or 0
                    )
                    current_capacity = pd.to_numeric(current_footprint.get("active_capacity_mmgy"), errors="coerce")
                    ly_capacity = pd.to_numeric(ly_footprint.get("active_capacity_mmgy"), errors="coerce")
                    record["current_operating_plant_count"] = current_plants if current_plants > 0 else None
                    record["ly_operating_plant_count"] = ly_plants if ly_plants > 0 else None
                    record["current_active_capacity_mmgy"] = (
                        None
                        if pd.isna(current_capacity) or float(current_capacity) <= 0.0
                        else float(current_capacity)
                    )
                    record["ly_active_capacity_mmgy"] = (
                        None
                        if pd.isna(ly_capacity) or float(ly_capacity) <= 0.0
                        else float(ly_capacity)
                    )
                    if pd.notna(current_capacity) and pd.notna(ly_capacity) and float(ly_capacity) > 0.0 and float(current_capacity) > 0.0:
                        scale_factor = float(current_capacity) / float(ly_capacity)
                        current_capacity_disp = int(round(float(current_capacity)))
                        record["gallons_source_kind"] = "yoy_same_quarter_gallons_produced_capacity_scaled"
                        record["footprint_scale_factor"] = scale_factor
                        record["implied_gallons"] = float(ly_actual_gallons) * scale_factor
                        record["implied_gallons_raw"] = float(ly_actual_gallons) * scale_factor
                        record["implied_gallons_million_display"] = (float(ly_actual_gallons) * scale_factor) / 1_000_000.0
                        record["volume_basis_display"] = "YoY same quarter produced gallons, adjusted to current active capacity footprint"
                        record["volume_basis_comment"] = (
                            f"{_quarter_label(same_quarter_last_year)} gallons produced scaled by "
                            f"{float(current_capacity):.0f}/{float(ly_capacity):.0f} MMgy active capacity."
                        )
                        if current_capacity_disp > 0:
                            record["volume_basis_display"] = (
                                f"YoY same quarter produced gallons, adjusted to current active capacity footprint ({current_capacity_disp} MMgy)"
                            )
                        record["status"] = "ok"
                        record["reason_unavailable"] = ""
                    elif current_plants > 0 and ly_plants > 0:
                        scale_factor = float(current_plants) / float(ly_plants)
                        record["gallons_source_kind"] = "fallback_yoy_same_quarter_gallons_produced_active_plant_count_ratio"
                        record["footprint_scale_factor"] = scale_factor
                        record["implied_gallons"] = float(ly_actual_gallons) * scale_factor
                        record["implied_gallons_raw"] = float(ly_actual_gallons) * scale_factor
                        record["implied_gallons_million_display"] = (float(ly_actual_gallons) * scale_factor) / 1_000_000.0
                        record["volume_basis_display"] = "Fallback: YoY produced gallons adjusted to active plant-count ratio"
                        record["volume_basis_comment"] = (
                            f"{_quarter_label(same_quarter_last_year)} gallons produced scaled by "
                            f"{current_plants}/{ly_plants} active plants because quarter-aware capacity was unavailable."
                        )
                        record["status"] = "ok"
                        record["reason_unavailable"] = ""
                    else:
                        record["reason_unavailable"] = "Quarter-aware active footprint could not be resolved."
        implied_gallons_num = pd.to_numeric(record.get("implied_gallons_raw", record.get("implied_gallons")), errors="coerce")
        if pd.notna(implied_gallons_num):
            if pd.notna(official_proxy_num):
                record["official_proxy_implied_result_usd_m"] = (
                    float(official_proxy_num) * float(implied_gallons_num) / 1_000_000.0
                )
            if pd.notna(gpre_proxy_num):
                record["gpre_proxy_implied_result_usd_m"] = (
                    float(gpre_proxy_num) * float(implied_gallons_num) / 1_000_000.0
                )
        current_capacity_num = pd.to_numeric(record.get("current_active_capacity_mmgy"), errors="coerce")
        if pd.notna(current_capacity_num) and float(current_capacity_num) > 0.0:
            quarter_capacity_ceiling_raw = float(current_capacity_num) * 1_000_000.0 / 4.0
            record["quarter_capacity_ceiling_gallons_raw"] = quarter_capacity_ceiling_raw
            record["quarter_capacity_ceiling_gallons_million_display"] = quarter_capacity_ceiling_raw / 1_000_000.0
            if pd.notna(implied_gallons_num):
                tolerance_ratio = float(pd.to_numeric(record.get("reasonableness_tolerance_ratio"), errors="coerce") or 0.05)
                excess_ratio = (float(implied_gallons_num) / quarter_capacity_ceiling_raw) - 1.0
                record["reasonableness_excess_ratio"] = excess_ratio
                if float(implied_gallons_num) > quarter_capacity_ceiling_raw * (1.0 + tolerance_ratio):
                    record["reasonableness_status"] = "above_tolerance"
                    record["reasonableness_comment"] = (
                        "Implied gallons exceed quarter-capacity ceiling by "
                        f"{excess_ratio * 100.0:.1f}% "
                        f"({float(implied_gallons_num) / 1_000_000.0:.1f}m vs "
                        f"{quarter_capacity_ceiling_raw / 1_000_000.0:.1f}m)."
                    )
                else:
                    record["reasonableness_status"] = "within_tolerance"
            else:
                record["reasonableness_status"] = "unavailable"
        if str(record.get("reasonableness_comment") or "").strip():
            existing_comment = str(record.get("volume_basis_comment") or "").strip()
            reasonableness_comment = str(record.get("reasonableness_comment") or "").strip()
            record["volume_basis_comment"] = (
                f"{existing_comment} {reasonableness_comment}".strip()
                if existing_comment
                else reasonableness_comment
            )
        result["frames"][frame_key] = record
        result["frame_records"].append(record)
    return result


_GPRE_BASIS_REGIONS: tuple[str, ...] = (
    "illinois",
    "indiana",
    "iowa_east",
    "iowa_west",
    "minnesota",
    "nebraska",
)
_GPRE_OFFICIAL_MARKET_REGION_ORDER: tuple[str, ...] = (
    "nebraska",
    "illinois",
    "indiana",
    "iowa_east",
    "iowa_west",
    "minnesota",
    "tennessee",
)
_GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES: Dict[str, tuple[str, ...]] = {
    "nebraska": ("corn_basis_nebraska",),
    "illinois": ("corn_basis_illinois",),
    "indiana": ("corn_basis_indiana",),
    "iowa_east": ("corn_basis_iowa_east",),
    "iowa_west": ("corn_basis_iowa_west",),
    "minnesota": ("corn_basis_minnesota",),
    "tennessee": ("corn_basis_illinois", "corn_basis_indiana"),
}
_GPRE_OFFICIAL_ETHANOL_SERIES_CANDIDATES: Dict[str, tuple[str, ...]] = {
    "nebraska": ("ethanol_nebraska",),
    "illinois": ("ethanol_illinois", "ethanol_iowa", "ethanol_nebraska"),
    "indiana": ("ethanol_indiana", "ethanol_iowa", "ethanol_nebraska"),
    "iowa_east": ("ethanol_iowa", "ethanol_nebraska"),
    "iowa_west": ("ethanol_iowa", "ethanol_nebraska"),
    "minnesota": ("ethanol_south_dakota", "ethanol_iowa", "ethanol_nebraska"),
    "tennessee": ("ethanol_illinois", "ethanol_indiana", "ethanol_iowa", "ethanol_nebraska"),
}
_GPRE_ETHANOL_ANCHOR_SERIES_KEYS: tuple[str, ...] = ("ethanol_nebraska", "ethanol_iowa", "ethanol_south_dakota")
_GPRE_ETHANOL_MIN_USD_PER_GAL = 0.75
_GPRE_ETHANOL_MAX_USD_PER_GAL = 3.25
_GPRE_ETHANOL_MAX_ANCHOR_SPREAD = 0.75
_GPRE_REGION_CAPACITY_MGY: Dict[str, float] = {
    "nebraska": 300.0,
    "illinois": 100.0,
    "indiana": 110.0,
    "iowa_west": 150.0,
    "minnesota": 70.0,
}
_GPRE_UNSUPPORTED_CAPACITY_MGY: Dict[str, float] = {
    "tennessee": 120.0,
}


def _safe_int_from_numeric(raw_value: Any) -> int:
    num = pd.to_numeric(raw_value, errors="coerce")
    return int(num) if pd.notna(num) else 0


def _quarter_label(qd: Any) -> str:
    qdate = parse_quarter_like(qd)
    if qdate is None:
        return str(qd or "")
    return f"{qdate.year}-Q{((qdate.month - 1) // 3) + 1}"


def _sorted_quarters(values: Iterable[Any]) -> List[date]:
    out: List[date] = []
    seen: set[date] = set()
    for raw in values:
        qd = parse_quarter_like(raw)
        if isinstance(qd, pd.Timestamp):
            qd = qd.date()
        if qd is None or qd in seen:
            continue
        seen.add(qd)
        out.append(qd)
    out.sort()
    return out


def _gpre_footprint_for_quarter(
    qd: date,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    stephens_doc = _gpre_local_doc_name(ticker_root, "conferences/Stephens_Annual_Investment_Conference_2025.txt")
    q3_doc = _gpre_local_doc_name(ticker_root, "earnings_transcripts/GPRE_Q3_2025_transcript.txt")
    q4_doc = _gpre_local_doc_name(ticker_root, "earnings_transcripts/GPRE_Q4_2025_transcript.txt")
    obion_doc = _gpre_local_doc_name(ticker_root, "press_release/8-K_2025-09-26_press_release_doc_*")
    history = _gpre_resolve_plant_capacity_history(plant_capacity_history, ticker_root=ticker_root)
    chosen_snapshot = _gpre_pick_capacity_snapshot_for_quarter(
        qd,
        plant_capacity_history=history,
        ticker_root=ticker_root,
    )
    active_plants = _gpre_active_plants_for_quarter(
        qd,
        plant_capacity_history=history,
        ticker_root=ticker_root,
    )
    region_counts: Dict[str, int] = {}
    unsupported_regions: Dict[str, int] = {}
    region_capacities_supported: Dict[str, float] = {}
    region_capacities_unsupported: Dict[str, float] = {}
    active_plant_names: List[str] = []
    idled_or_exited: List[str] = []
    metadata_flags: List[str] = []
    capacity_unavailable_plants: List[str] = []
    total_active_capacity = 0.0
    for rec in active_plants:
        location = str(rec.get("location") or "").strip()
        region = str(rec.get("region") or "").strip().lower()
        cap_num = max(float(pd.to_numeric(rec.get("capacity_mmgy"), errors="coerce") or 0.0), 0.0)
        if not location or not region:
            continue
        active_plant_names.append(location)
        if cap_num > 0.0:
            total_active_capacity += cap_num
        else:
            capacity_unavailable_plants.append(location)
        if region in _GPRE_BASIS_REGIONS:
            region_counts[region] = region_counts.get(region, 0) + 1
            if cap_num > 0.0:
                region_capacities_supported[region] = region_capacities_supported.get(region, 0.0) + cap_num
        else:
            unsupported_regions[region] = unsupported_regions.get(region, 0) + 1
            if cap_num > 0.0:
                region_capacities_unsupported[region] = region_capacities_unsupported.get(region, 0.0) + cap_num
        flags = dict(rec.get("flags") or {})
        if bool(flags.get("uhp")):
            metadata_flags.append(f"{location}: UHP")
        if bool(flags.get("trailblazer")):
            metadata_flags.append(f"{location}: Trailblazer")
        if bool(flags.get("summit_carbon")):
            metadata_flags.append(f"{location}: Summit Carbon")
    for plant_key in ("fairmont", "obion"):
        plant_rec = dict((history.get("plants") or {}).get(plant_key) or {})
        location = str(plant_rec.get("location") or plant_key).strip()
        inactive_from = plant_rec.get("inactive_from")
        active_through = plant_rec.get("active_through")
        inactive_quarter = quarter_end_from_date(inactive_from) if isinstance(inactive_from, date) else None
        active_through_quarter = quarter_end_from_date(active_through) if isinstance(active_through, date) else None
        if isinstance(inactive_quarter, date) and qd >= inactive_quarter:
            idled_or_exited.append(f"{location} idled from {_quarter_label(inactive_quarter)}")
        elif isinstance(active_through_quarter, date) and qd > active_through_quarter:
            idled_or_exited.append(f"{location} exited after {_quarter_label(active_through_quarter)}")
    regime_flags = "; ".join(
        part
        for part in (
            "capacity_weighted_footprint",
            "fairmont_offline" if any("Fairmont" in item for item in idled_or_exited) else "fairmont_active",
            "post_obion_sale" if any("Obion" in item for item in idled_or_exited) else "obion_active",
            "partial_tennessee_coverage" if unsupported_regions else "all_regions_supported",
        )
        if part
    )
    note_parts = []
    if active_plant_names:
        note_parts.append(f"Active plants: {', '.join(active_plant_names)}.")
    if idled_or_exited:
        note_parts.append("; ".join(idled_or_exited) + ".")
    if metadata_flags:
        note_parts.append("Metadata flags: " + "; ".join(metadata_flags[:5]) + ("." if len(metadata_flags) <= 5 else "; ..."))
    if capacity_unavailable_plants:
        note_parts.append(
            "Capacity unavailable for: "
            + ", ".join(capacity_unavailable_plants)
            + "."
        )
    selection_mode = str(chosen_snapshot.get("selection_mode") or "")
    if selection_mode == "earliest_snapshot_fallback":
        note_parts.append("Capacity snapshot uses earliest available filing fallback for this quarter.")
    notes = " ".join(part for part in note_parts if part).strip()
    source_refs = " | ".join(
        part for part in (
            str(chosen_snapshot.get("source_path") or ""),
            obion_doc,
            q4_doc,
            q3_doc,
            stephens_doc,
        ) if part
    )
    active_regions = ", ".join(region for region, count in region_counts.items() if int(count or 0) > 0)
    return {
        "quarter": qd,
        "quarter_label": _quarter_label(qd),
        "operating_plant_count": len(active_plant_names),
        "active_capacity_mmgy": total_active_capacity,
        "active_regions": active_regions,
        "region_counts": dict(region_counts),
        "unsupported_regions": dict(unsupported_regions),
        "region_capacities_supported": dict(region_capacities_supported),
        "region_capacities_unsupported": dict(region_capacities_unsupported),
        "regime_flags": regime_flags,
        "notes": notes,
        "source_refs": source_refs,
        "active_plants": list(active_plant_names),
        "inactive_or_exited_notes": list(idled_or_exited),
        "capacity_snapshot_year": chosen_snapshot.get("snapshot_year"),
        "capacity_snapshot_selection_mode": selection_mode,
    }


def _gpre_plant_count_weights_for_quarter(
    qd: date,
    *,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    rec = _gpre_footprint_for_quarter(qd, ticker_root=ticker_root, plant_capacity_history=plant_capacity_history)
    region_counts = dict(rec.get("region_counts") or {})
    total = float(sum(max(float(region_counts.get(region) or 0.0), 0.0) for region in _GPRE_BASIS_REGIONS))
    return {
        region: (max(float(region_counts.get(region) or 0.0), 0.0) / total) if total > 0 else 0.0
        for region in _GPRE_BASIS_REGIONS
    }


def _gpre_official_market_region_counts_for_quarter(
    qd: date,
    *,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    rec = _gpre_footprint_for_quarter(qd, ticker_root=ticker_root, plant_capacity_history=plant_capacity_history)
    out: Dict[str, float] = {}
    for source_map in (rec.get("region_counts") or {}, rec.get("unsupported_regions") or {}):
        for region, raw_count in dict(source_map).items():
            region_txt = str(region or "").strip().lower()
            count_num = max(float(raw_count or 0.0), 0.0)
            if region_txt not in _GPRE_OFFICIAL_MARKET_REGION_ORDER or count_num <= 0.0:
                continue
            out[region_txt] = out.get(region_txt, 0.0) + count_num
    return {region: out.get(region, 0.0) for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER if out.get(region, 0.0) > 0.0}


def _gpre_official_market_weights_for_quarter(
    qd: date,
    *,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    footprint_rec = _gpre_footprint_for_quarter(qd, ticker_root=ticker_root, plant_capacity_history=plant_capacity_history)
    region_capacities: Dict[str, float] = {}
    for source_map in (
        footprint_rec.get("region_capacities_supported") or {},
        footprint_rec.get("region_capacities_unsupported") or {},
    ):
        for region, raw_val in dict(source_map).items():
            region_txt = str(region or "").strip().lower()
            cap_num = max(float(pd.to_numeric(raw_val, errors="coerce") or 0.0), 0.0)
            if region_txt not in _GPRE_OFFICIAL_MARKET_REGION_ORDER or cap_num <= 0.0:
                continue
            region_capacities[region_txt] = region_capacities.get(region_txt, 0.0) + cap_num
    total = float(sum(max(float(region_capacities.get(region) or 0.0), 0.0) for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER))
    return {
        region: (max(float(region_capacities.get(region) or 0.0), 0.0) / total) if total > 0 else 0.0
        for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER
        if max(float(region_capacities.get(region) or 0.0), 0.0) > 0.0
    }


def _series_observation_value_map(rows: Iterable[Dict[str, Any]], series_key: str) -> Dict[date, float]:
    df = _market_rows_df(rows)
    if df.empty:
        return {}
    obs = _series_observation_df(df, series_key)
    if obs.empty:
        return {}
    out: Dict[date, float] = {}
    for rec in obs.to_dict("records"):
        obs_dt = pd.to_datetime(rec.get("observation_date"), errors="coerce")
        val = pd.to_numeric(rec.get("price_value"), errors="coerce")
        if pd.isna(obs_dt) or pd.isna(val):
            continue
        out[pd.Timestamp(obs_dt).date()] = float(val)
    return out


def _window_average_from_map(value_map: Dict[date, float], *, window_start: date, window_end: date) -> Optional[float]:
    vals = [float(val) for dt, val in (value_map or {}).items() if isinstance(dt, date) and window_start <= dt <= window_end and pd.notna(val)]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def _gpre_select_candidate_quarter_value(
    series_maps: Dict[str, Dict[date, float]],
    candidate_keys: Iterable[str],
    qd: date,
    *,
    validator: Optional[Callable[[float, str], Optional[str]]] = None,
) -> Tuple[Optional[float], str, str]:
    candidates = [str(key or "").strip() for key in candidate_keys if str(key or "").strip()]
    if not candidates:
        return None, "", ""
    primary = candidates[0]
    skipped_notes: List[str] = []
    for key in candidates:
        val = pd.to_numeric((series_maps.get(key) or {}).get(qd), errors="coerce")
        if pd.notna(val):
            reject_note = validator(float(val), key) if validator is not None else None
            if reject_note:
                skipped_notes.append(reject_note)
                continue
            note_parts = list(skipped_notes)
            if key != primary:
                note_parts.append(f"{primary} unavailable; fallback to {key}")
            note = "; ".join(part for part in note_parts if str(part or "").strip())
            return float(val), key, note
    missing_note = f"Missing {' -> '.join(candidates)}"
    note = "; ".join([*skipped_notes, missing_note]) if skipped_notes else missing_note
    return None, "", note


def _gpre_select_candidate_observation_value(
    series_maps: Dict[str, Dict[date, float]],
    series_dates: Dict[str, List[date]],
    candidate_keys: Iterable[str],
    obs_date_in: date,
    *,
    max_lag_days: int,
    validator: Optional[Callable[[float, str], Optional[str]]] = None,
) -> Tuple[Optional[float], str, str]:
    candidates = [str(key or "").strip() for key in candidate_keys if str(key or "").strip()]
    if not candidates or not isinstance(obs_date_in, date):
        return None, "", ""
    primary = candidates[0]
    floor_date = obs_date_in - timedelta(days=max(int(max_lag_days or 0), 0))
    skipped_notes: List[str] = []
    for key in candidates:
        obs_map = series_maps.get(key) or {}
        obs_keys = series_dates.get(key) or []
        chosen_date: Optional[date] = None
        for candidate in reversed(obs_keys):
            if candidate > obs_date_in:
                continue
            if candidate < floor_date:
                break
            chosen_date = candidate
            break
        if chosen_date is None:
            continue
        val = pd.to_numeric(obs_map.get(chosen_date), errors="coerce")
        if pd.isna(val):
            continue
        reject_note = validator(float(val), key) if validator is not None else None
        if reject_note:
            skipped_notes.append(reject_note)
            continue
        note_parts: List[str] = []
        note_parts.extend(skipped_notes)
        if key != primary:
            note_parts.append(f"{primary} unavailable; fallback to {key}")
        if chosen_date != obs_date_in:
            note_parts.append(f"latest <= {max_lag_days}d lag ({chosen_date.isoformat()})")
        return float(val), key, "; ".join(note_parts)
    missing_note = f"Missing {' -> '.join(candidates)} within {int(max_lag_days)}d"
    note = "; ".join([*skipped_notes, missing_note]) if skipped_notes else missing_note
    return None, "", note


def _gpre_ethanol_anchor_quarter_value(
    ethanol_series_maps: Dict[str, Dict[date, float]],
    qd: date,
) -> Optional[float]:
    anchor_vals: List[float] = []
    for key in _GPRE_ETHANOL_ANCHOR_SERIES_KEYS:
        val = pd.to_numeric((ethanol_series_maps.get(key) or {}).get(qd), errors="coerce")
        if pd.notna(val):
            val_float = float(val)
            if _GPRE_ETHANOL_MIN_USD_PER_GAL <= val_float <= _GPRE_ETHANOL_MAX_USD_PER_GAL:
                anchor_vals.append(val_float)
    if not anchor_vals:
        return None
    return float(sum(anchor_vals) / len(anchor_vals))


def _gpre_ethanol_anchor_observation_value(
    ethanol_series_maps: Dict[str, Dict[date, float]],
    ethanol_series_dates: Dict[str, List[date]],
    obs_date_in: date,
    *,
    max_lag_days: int,
) -> Optional[float]:
    anchor_vals: List[float] = []
    for key in _GPRE_ETHANOL_ANCHOR_SERIES_KEYS:
        value_num, _, _ = _gpre_select_candidate_observation_value(
            ethanol_series_maps,
            ethanol_series_dates,
            (key,),
            obs_date_in,
            max_lag_days=max_lag_days,
        )
        if value_num is not None:
            value_float = float(value_num)
            if _GPRE_ETHANOL_MIN_USD_PER_GAL <= value_float <= _GPRE_ETHANOL_MAX_USD_PER_GAL:
                anchor_vals.append(value_float)
    if not anchor_vals:
        return None
    return float(sum(anchor_vals) / len(anchor_vals))


def _gpre_ethanol_implausibility_note(
    value_num: float,
    *,
    series_key: str,
    anchor_value: Optional[float],
) -> Optional[str]:
    try:
        value = float(value_num)
    except Exception:
        return f"{series_key} invalid numeric value"
    if not np.isfinite(value):
        return f"{series_key} invalid numeric value"
    if value < _GPRE_ETHANOL_MIN_USD_PER_GAL or value > _GPRE_ETHANOL_MAX_USD_PER_GAL:
        return (
            f"{series_key} implausible ({value:.3f} $/gal outside "
            f"{_GPRE_ETHANOL_MIN_USD_PER_GAL:.2f}-{_GPRE_ETHANOL_MAX_USD_PER_GAL:.2f})"
        )
    if anchor_value is not None and np.isfinite(anchor_value):
        anchor = float(anchor_value)
        if abs(value - anchor) > _GPRE_ETHANOL_MAX_ANCHOR_SPREAD:
            return (
                f"{series_key} implausible ({value:.3f} $/gal vs Midwest anchor {anchor:.3f}; "
                f"spread > {_GPRE_ETHANOL_MAX_ANCHOR_SPREAD:.2f})"
            )
    return None


def _gpre_official_quarter_component_records(
    rows: Iterable[Dict[str, Any]],
    quarters: Iterable[date],
    *,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[date, Dict[str, Any]]:
    ethanol_series_maps = {
        key: _quarter_avg_map(rows, key)
        for keys in _GPRE_OFFICIAL_ETHANOL_SERIES_CANDIDATES.values()
        for key in keys
    }
    basis_series_maps = {
        key: _quarter_avg_map(rows, key)
        for keys in _GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.values()
        for key in keys
    }
    out: Dict[date, Dict[str, Any]] = {}
    for qd in _sorted_quarters(quarters):
        footprint_rec = _gpre_footprint_for_quarter(qd, ticker_root=ticker_root, plant_capacity_history=plant_capacity_history)
        region_counts = _gpre_official_market_region_counts_for_quarter(
            qd,
            ticker_root=ticker_root,
            plant_capacity_history=plant_capacity_history,
        )
        region_capacities = {
            **dict(footprint_rec.get("region_capacities_supported") or {}),
            **dict(footprint_rec.get("region_capacities_unsupported") or {}),
        }
        weights = _gpre_official_market_weights_for_quarter(
            qd,
            ticker_root=ticker_root,
            plant_capacity_history=plant_capacity_history,
        )
        ethanol_sum = 0.0
        ethanol_cov = 0.0
        basis_sum = 0.0
        basis_cov = 0.0
        component_rows: List[Dict[str, Any]] = []
        for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER:
            plant_count = max(float(region_counts.get(region) or 0.0), 0.0)
            active_capacity = max(float(pd.to_numeric(region_capacities.get(region), errors="coerce") or 0.0), 0.0)
            weight = max(float(weights.get(region) or 0.0), 0.0)
            if plant_count <= 0.0 and active_capacity <= 0.0 and weight <= 0.0:
                continue
            ethanol_anchor_value = _gpre_ethanol_anchor_quarter_value(ethanol_series_maps, qd)
            ethanol_value, ethanol_series_key, ethanol_note = _gpre_select_candidate_quarter_value(
                ethanol_series_maps,
                _GPRE_OFFICIAL_ETHANOL_SERIES_CANDIDATES.get(region, tuple()),
                qd,
                validator=lambda value_num, series_key, anchor_value=ethanol_anchor_value: _gpre_ethanol_implausibility_note(
                    value_num,
                    series_key=series_key,
                    anchor_value=anchor_value,
                ),
            )
            basis_value, basis_series_key, basis_note = _gpre_select_candidate_quarter_value(
                basis_series_maps,
                _GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.get(region, tuple()),
                qd,
            )
            if weight > 0.0 and ethanol_value is not None:
                ethanol_sum += weight * float(ethanol_value)
                ethanol_cov += weight
            if weight > 0.0 and basis_value is not None:
                basis_sum += weight * float(basis_value)
                basis_cov += weight
            note_parts = [part for part in (ethanol_note, basis_note) if str(part or "").strip()]
            component_rows.append(
                {
                    "quarter": qd,
                    "quarter_label": _quarter_label(qd),
                    "region": region,
                    "region_label": region.replace("_", " ").title(),
                    "plant_count": plant_count,
                    "active_capacity_mmgy": active_capacity,
                    "weight": weight,
                    "ethanol_series_key": ethanol_series_key,
                    "ethanol_series_label": ethanol_series_key.replace("_", " ") if ethanol_series_key else "",
                    "ethanol_value_usd_per_gal": ethanol_value,
                    "basis_series_key": basis_series_key,
                    "basis_series_label": basis_series_key.replace("_", " ") if basis_series_key else "",
                    "basis_value_usd_per_bu": basis_value,
                    "basis_value_cents_per_bu": (None if basis_value is None else float(basis_value) * 100.0),
                    "fallback_note": " | ".join(note_parts),
                }
            )
        out[qd] = {
            "quarter": qd,
            "quarter_label": _quarter_label(qd),
            "region_counts": region_counts,
            "region_capacities": region_capacities,
            "weights": weights,
            "component_rows": component_rows,
            "weighted_ethanol_benchmark_usd_per_gal": (ethanol_sum / ethanol_cov) if ethanol_cov > 0 else None,
            "ethanol_coverage_ratio": ethanol_cov,
            "weighted_ams_basis_usd_per_bu": (basis_sum / basis_cov) if basis_cov > 0 else None,
            "basis_coverage_ratio": basis_cov,
        }
    return out


def _gpre_weighted_basis_daily_map(
    rows: Iterable[Dict[str, Any]],
    *,
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Dict[date, float]:
    region_daily_maps = {
        region: _series_observation_value_map(rows, f"corn_basis_{region}")
        for region in _GPRE_BASIS_REGIONS
    }
    all_dates = sorted({dt for reg_map in region_daily_maps.values() for dt in reg_map.keys() if isinstance(dt, date)})
    out: Dict[date, float] = {}
    for obs_dt in all_dates:
        qd = quarter_end_from_date(obs_dt)
        weights = _gpre_official_market_weights_for_quarter(
            qd,
            ticker_root=ticker_root,
            plant_capacity_history=plant_capacity_history,
        )
        weighted_sum = 0.0
        covered_weight = 0.0
        total_weight = float(sum(max(float(weights.get(region) or 0.0), 0.0) for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER))
        for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER:
            weight = max(float(weights.get(region) or 0.0), 0.0)
            basis_region = _basis_region_from_series_key(
                (_GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.get(region, ("",))[0] if _GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.get(region) else region)
            )
            val = pd.to_numeric((region_daily_maps.get(basis_region) or {}).get(obs_dt), errors="coerce")
            if weight <= 0.0 or pd.isna(val):
                continue
            weighted_sum += weight * float(val)
            covered_weight += weight
        if covered_weight <= 0.0:
            continue
        coverage_ratio = covered_weight / total_weight if total_weight > 0 else 1.0
        if coverage_ratio < 0.75:
            continue
        out[obs_dt] = weighted_sum / covered_weight
    return out


def _quarter_avg_map(rows: Iterable[Dict[str, Any]], series_key: str) -> Dict[date, float]:
    df = _market_rows_df(rows)
    if df.empty:
        return {}
    sub = df[
        (df["aggregation_level"].astype(str).str.lower() == "quarter_avg")
        & (df["series_key"].astype(str).str.lower() == str(series_key or "").strip().lower())
        & df["quarter"].notna()
        & df["price_value"].notna()
    ].copy()
    if sub.empty:
        return {}
    out: Dict[date, float] = {}
    for rec in sub.sort_values(["quarter", "observation_date"], na_position="last").to_dict("records"):
        qd = parse_quarter_like(rec.get("quarter"))
        if isinstance(qd, pd.Timestamp):
            qd = qd.date()
        val = pd.to_numeric(rec.get("price_value"), errors="coerce")
        if qd is None or pd.isna(val):
            continue
        out[qd] = float(val)
    return out


def _quarter_observation_weighted_avg_map(
    rows: Iterable[Dict[str, Any]],
    series_key: str,
    *,
    profile: str = "uniform",
) -> Dict[date, float]:
    df = _market_rows_df(rows)
    if df.empty:
        return {}
    sub = df[
        (df["aggregation_level"].astype(str).str.lower() == "observation")
        & (df["series_key"].astype(str).str.lower() == str(series_key or "").strip().lower())
        & df["quarter"].notna()
        & df["observation_date"].notna()
        & df["price_value"].notna()
    ].copy()
    if sub.empty:
        return _quarter_avg_map(rows, series_key)
    sub = (
        sub.groupby(["quarter", "observation_date"], as_index=False)["price_value"]
        .mean()
        .sort_values(["quarter", "observation_date"], na_position="last")
    )
    out: Dict[date, float] = {}
    for quarter_raw, qsub in sub.groupby("quarter", dropna=False):
        qd = parse_quarter_like(quarter_raw)
        if isinstance(qd, pd.Timestamp):
            qd = qd.date()
        if qd is None:
            continue
        vals = pd.to_numeric(qsub["price_value"], errors="coerce").to_numpy(dtype=float)
        vals = vals[np.isfinite(vals)]
        if vals.size == 0:
            continue
        if str(profile or "").strip().lower() == "front_loaded":
            weights = np.arange(vals.size, 0, -1, dtype=float)
        else:
            weights = np.ones(vals.size, dtype=float)
        denom = float(weights.sum())
        if denom <= 0.0:
            continue
        out[qd] = float(np.dot(vals, weights) / denom)
    return out


def _gpre_weighted_basis_for_quarter_from_observations(
    rows: Iterable[Dict[str, Any]],
    qd: date,
    *,
    profile: str = "uniform",
    ticker_root: Optional[Path] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    region_maps = {
        region: _quarter_observation_weighted_avg_map(rows, f"corn_basis_{region}", profile=profile)
        for region in _GPRE_BASIS_REGIONS
    }
    weights = _gpre_official_market_weights_for_quarter(
        qd,
        ticker_root=ticker_root,
        plant_capacity_history=plant_capacity_history,
    )
    total_weight = float(sum(max(float(weights.get(region) or 0.0), 0.0) for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER))
    if total_weight <= 0.0:
        return None
    weighted_sum = 0.0
    covered_weight = 0.0
    for region in _GPRE_OFFICIAL_MARKET_REGION_ORDER:
        weight = max(float(weights.get(region) or 0.0), 0.0)
        if weight <= 0.0:
            continue
        basis_region = _basis_region_from_series_key(
            (_GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.get(region, ("",))[0] if _GPRE_OFFICIAL_BASIS_SERIES_CANDIDATES.get(region) else region)
        )
        val = pd.to_numeric((region_maps.get(basis_region) or {}).get(qd), errors="coerce")
        if pd.isna(val):
            continue
        weighted_sum += weight * float(val)
        covered_weight += weight
    if covered_weight <= 0.0:
        return None
    return float(weighted_sum / covered_weight)


def _shift_quarter_map(values: Dict[date, float], target_quarters: Iterable[date], *, lag_quarters: int) -> Dict[date, float]:
    ordered = _sorted_quarters(target_quarters)
    if not ordered or lag_quarters == 0:
        return dict(values or {})
    shifted: Dict[date, float] = {}
    for idx, qd in enumerate(ordered):
        source_idx = idx - int(lag_quarters)
        if source_idx < 0 or source_idx >= len(ordered):
            continue
        source_qd = ordered[source_idx]
        val = pd.to_numeric((values or {}).get(source_qd), errors="coerce")
        if pd.notna(val):
            shifted[qd] = float(val)
    return shifted


def _gpre_evaluation_target_maps(
    target_quarters: Iterable[date],
    reported_margin_by_quarter: Dict[date, float],
    underlying_margin_by_quarter: Optional[Dict[date, float]] = None,
) -> Tuple[Dict[date, float], Dict[date, str], List[date]]:
    official_window_end = date(2025, 3, 31)
    reported_clean: Dict[date, float] = {}
    underlying_clean: Dict[date, float] = {}
    for qd in _sorted_quarters(target_quarters):
        rep_val = pd.to_numeric((reported_margin_by_quarter or {}).get(qd), errors="coerce")
        und_val = pd.to_numeric((underlying_margin_by_quarter or {}).get(qd), errors="coerce")
        if pd.notna(rep_val):
            reported_clean[qd] = float(rep_val)
        if pd.notna(und_val):
            underlying_clean[qd] = float(und_val)
    diagnostic_underlying_quarters = sorted(qd for qd in underlying_clean.keys() if qd > official_window_end)
    target_map: Dict[date, float] = {}
    target_basis_map: Dict[date, str] = {}
    for qd in _sorted_quarters(target_quarters):
        if qd <= official_window_end and qd in reported_clean:
            target_map[qd] = float(reported_clean[qd])
            target_basis_map[qd] = "reported_clean"
        elif qd <= official_window_end and qd in underlying_clean:
            target_map[qd] = float(underlying_clean[qd])
            target_basis_map[qd] = "underlying"
        elif qd > official_window_end and qd in underlying_clean:
            target_map[qd] = float(underlying_clean[qd])
            target_basis_map[qd] = "underlying"
    return target_map, target_basis_map, diagnostic_underlying_quarters


def _gpre_local_doc_name(ticker_root: Optional[Path], *patterns: str) -> str:
    if ticker_root is None:
        return ""
    for pattern in patterns:
        try:
            matches = sorted(ticker_root.glob(pattern))
        except Exception:
            matches = []
        for path in matches:
            if path.exists() and path.is_file():
                return path.name
    return ""


def _gpre_local_doc_paths(ticker_root: Optional[Path], *patterns: str) -> List[Path]:
    if ticker_root is None:
        return []
    matches: List[Path] = []
    seen: set[str] = set()
    for pattern in patterns:
        try:
            found = sorted(ticker_root.glob(pattern))
        except Exception:
            found = []
        for path in found:
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            key = str(resolved)
            if key in seen or not resolved.exists() or not resolved.is_file():
                continue
            seen.add(key)
            matches.append(resolved)
    return matches


def _gpre_read_local_textish_doc(path: Path) -> str:
    suffix = str(path.suffix or "").strip().lower()
    if suffix not in {".txt", ".htm", ".html"}:
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    if suffix in {".htm", ".html"}:
        text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
        text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
        text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _gpre_local_doc_quarter(path: Path) -> Optional[date]:
    name = str(path.name or "")
    m_transcript = re.search(r"GPRE_Q([1-4])_(20\d{2})_transcript", name, re.I)
    if m_transcript:
        quarter_num = int(m_transcript.group(1))
        year_num = int(m_transcript.group(2))
        quarter_month = quarter_num * 3
        return date(year_num, quarter_month, calendar.monthrange(year_num, quarter_month)[1])
    m_press = re.search(r"8-K_(20\d{2})-(\d{2})-(\d{2})_", name, re.I)
    if m_press:
        try:
            return quarter_end_from_date(date(int(m_press.group(1)), int(m_press.group(2)), int(m_press.group(3))))
        except Exception:
            return None
    return None


def _gpre_quarter_observation_count_map(rows: Iterable[Dict[str, Any]]) -> Dict[date, int]:
    df = _market_rows_df(rows)
    if df.empty:
        return {}
    require_market_columns(
        df,
        ["aggregation_level", "quarter", "observation_date"],
        contract_name="_gpre_quarter_observation_count_map",
    )
    obs = df[df["aggregation_level"].astype(str).str.lower().eq("observation")].copy()
    if obs.empty:
        return {}
    obs["quarter_date"] = pd.to_datetime(obs["quarter"], errors="coerce").dt.date
    obs["obs_date"] = pd.to_datetime(obs["observation_date"], errors="coerce").dt.date
    obs = obs.dropna(subset=["quarter_date", "obs_date"]).copy()
    if obs.empty:
        return {}
    grouped = obs.groupby("quarter_date", dropna=False)["obs_date"].nunique()
    return {
        qd: int(count or 0)
        for qd, count in grouped.items()
        if isinstance(qd, date)
    }


def _gpre_quarter_open_blend_weights(obs_count: Any) -> Dict[str, Any]:
    count_num = pd.to_numeric(obs_count, errors="coerce")
    if pd.isna(count_num):
        count_num = 0.0
    coverage_ratio = min(max(float(count_num), 0.0) / 13.0, 1.0)
    if coverage_ratio < 0.34:
        return {
            "quarter_open_weight": 0.75,
            "current_weight": 0.25,
            "coverage_ratio": coverage_ratio,
            "progress_bucket": "early",
        }
    if coverage_ratio < 0.67:
        return {
            "quarter_open_weight": 0.50,
            "current_weight": 0.50,
            "coverage_ratio": coverage_ratio,
            "progress_bucket": "mid",
        }
    return {
        "quarter_open_weight": 0.25,
        "current_weight": 0.75,
        "coverage_ratio": coverage_ratio,
        "progress_bucket": "late",
    }


def _blend_optional_values(
    anchor_value: Any,
    current_value: Any,
    *,
    anchor_weight: float,
    current_weight: float,
) -> Optional[float]:
    anchor_num = pd.to_numeric(anchor_value, errors="coerce")
    current_num = pd.to_numeric(current_value, errors="coerce")
    if pd.notna(anchor_num) and pd.notna(current_num):
        return (float(anchor_num) * float(anchor_weight)) + (float(current_num) * float(current_weight))
    if pd.notna(current_num):
        return float(current_num)
    if pd.notna(anchor_num):
        return float(anchor_num)
    return None


def _gpre_formula_helper_payload(
    *,
    status: str,
    mode: str,
    slope: Optional[float],
    intercept: Optional[float],
    hedge_share: float = 0.0,
    anchor: Optional[float] = None,
    live_preview_mode: str = "exact_formula",
    live_preview_note: str = "",
) -> Dict[str, Any]:
    return {
        "status": str(status or "no_data"),
        "mode": str(mode or ""),
        "slope": slope,
        "intercept": intercept,
        "hedge_share": float(hedge_share or 0.0),
        "anchor": anchor,
        "live_preview_mode": str(live_preview_mode or "exact_formula"),
        "live_preview_note": str(live_preview_note or "").strip(),
    }


def _gpre_combine_linear_formula_helpers(
    primary_helper: Dict[str, Any],
    secondary_helper: Dict[str, Any],
    *,
    secondary_weight: float,
    live_preview_mode: str,
    live_preview_note: str,
) -> Dict[str, Any]:
    weight = float(np.clip(float(pd.to_numeric(secondary_weight, errors="coerce") or 0.0), 0.0, 1.0))
    primary_status = str((primary_helper or {}).get("status") or "no_data")
    secondary_status = str((secondary_helper or {}).get("status") or "no_data")
    if primary_status != "ok":
        return _gpre_formula_helper_payload(
            status="no_data",
            mode="process",
            slope=None,
            intercept=None,
            hedge_share=weight,
            anchor=None,
            live_preview_mode=live_preview_mode,
            live_preview_note=live_preview_note,
        )
    if weight <= 1e-12 or secondary_status != "ok":
        return _gpre_formula_helper_payload(
            status="ok",
            mode=str((primary_helper or {}).get("mode") or "process"),
            slope=pd.to_numeric((primary_helper or {}).get("slope"), errors="coerce"),
            intercept=pd.to_numeric((primary_helper or {}).get("intercept"), errors="coerce"),
            hedge_share=weight,
            anchor=(primary_helper or {}).get("anchor"),
            live_preview_mode=live_preview_mode,
            live_preview_note=live_preview_note,
        )
    primary_slope = pd.to_numeric((primary_helper or {}).get("slope"), errors="coerce")
    primary_intercept = pd.to_numeric((primary_helper or {}).get("intercept"), errors="coerce")
    secondary_slope = pd.to_numeric((secondary_helper or {}).get("slope"), errors="coerce")
    secondary_intercept = pd.to_numeric((secondary_helper or {}).get("intercept"), errors="coerce")
    if any(pd.isna(val) for val in (primary_slope, primary_intercept, secondary_slope, secondary_intercept)):
        return _gpre_formula_helper_payload(
            status="ok",
            mode=str((primary_helper or {}).get("mode") or "process"),
            slope=primary_slope,
            intercept=primary_intercept,
            hedge_share=weight,
            anchor=(primary_helper or {}).get("anchor"),
            live_preview_mode=live_preview_mode,
            live_preview_note=live_preview_note,
        )
    return _gpre_formula_helper_payload(
        status="ok",
        mode="process_blend_hedge",
        slope=((1.0 - weight) * float(primary_slope)) + (weight * float(secondary_slope)),
        intercept=((1.0 - weight) * float(primary_intercept)) + (weight * float(secondary_intercept)),
        hedge_share=weight,
        anchor=(primary_helper or {}).get("anchor"),
        live_preview_mode=live_preview_mode,
        live_preview_note=live_preview_note,
    )


def _gpre_process_blend_formula_helper(
    *,
    anchor_proxy: Any,
    current_nonethanol: Any,
    quarter_open_weight: Any,
    current_weight: Any,
    penalty: float = 0.0,
    phase_label: str = "current",
    penalty_note: str = "",
    live_preview_mode: str = "exact_formula",
) -> Dict[str, Any]:
    anchor_weight = float(pd.to_numeric(quarter_open_weight, errors="coerce") or 0.75)
    current_weight_num = float(pd.to_numeric(current_weight, errors="coerce") or 0.25)
    anchor_num = pd.to_numeric(anchor_proxy, errors="coerce")
    current_noneth_num = pd.to_numeric(current_nonethanol, errors="coerce")
    penalty_num = pd.to_numeric(penalty, errors="coerce")
    penalty_val = 0.0 if pd.isna(penalty_num) else float(penalty_num)
    penalty_suffix = f" {penalty_note.strip()}" if str(penalty_note or "").strip() else ""
    if pd.notna(current_noneth_num):
        note = (
            f"{phase_label.capitalize()} fitted preview uses the quarter-open/current blend with "
            f"{anchor_weight:.0%}/{current_weight_num:.0%} weights."
        )
        if pd.notna(anchor_num):
            slope = current_weight_num
            intercept = (current_weight_num * float(current_noneth_num)) + (anchor_weight * float(anchor_num)) - penalty_val
            note += " The quarter-open anchor is carried in the intercept."
        else:
            slope = 1.0
            intercept = float(current_noneth_num) - penalty_val
            note += " No stored quarter-open anchor was available, so the live formula collapses to the current observed leg."
        if penalty_val > 1e-12:
            note += penalty_suffix or f" Includes a bounded ops penalty of {penalty_val:.3f} $/gal."
        return _gpre_formula_helper_payload(
            status="ok",
            mode="process_blend",
            slope=float(slope),
            intercept=float(intercept),
            hedge_share=0.0,
            anchor=None if pd.isna(anchor_num) else float(anchor_num),
            live_preview_mode=live_preview_mode,
            live_preview_note=note,
        )
    if pd.notna(anchor_num):
        note = f"{phase_label.capitalize()} fitted preview falls back to the stored quarter-open anchor."
        if penalty_val > 1e-12:
            note += penalty_suffix or f" Includes a bounded ops penalty of {penalty_val:.3f} $/gal."
        return _gpre_formula_helper_payload(
            status="ok",
            mode="process_blend",
            slope=0.0,
            intercept=float(anchor_num) - penalty_val,
            hedge_share=0.0,
            anchor=float(anchor_num),
            live_preview_mode=live_preview_mode,
            live_preview_note=note,
        )
    return _gpre_formula_helper_payload(
        status="no_data",
        mode="process_blend",
        slope=None,
        intercept=None,
        hedge_share=0.0,
        anchor=None,
        live_preview_mode=live_preview_mode,
        live_preview_note=f"{phase_label.capitalize()} fitted preview could not be formed because both quarter-open anchor and current observed leg were unavailable.",
    )


def _gpre_evaluate_formula_helper_payload(helper: Dict[str, Any], ethanol_value: Any) -> Optional[float]:
    helper_dict = dict(helper or {})
    if str(helper_dict.get("status") or "no_data") != "ok":
        return None
    slope_num = pd.to_numeric(helper_dict.get("slope"), errors="coerce")
    intercept_num = pd.to_numeric(helper_dict.get("intercept"), errors="coerce")
    if pd.isna(intercept_num):
        return None
    if pd.isna(slope_num) or abs(float(slope_num)) <= 1e-12:
        return float(intercept_num)
    ethanol_num = pd.to_numeric(ethanol_value, errors="coerce")
    if pd.isna(ethanol_num):
        return None
    return float(slope_num) * float(ethanol_num) + float(intercept_num)


def _gpre_ops_penalty_signal_map(ticker_root: Optional[Path]) -> Dict[date, Dict[str, Any]]:
    if ticker_root is None:
        return {}
    negative_terms = (
        "outage",
        "outages",
        "downtime",
        "delay",
        "delays",
        "planned maintenance",
        "unplanned downtime",
        "cold snap",
        "care and maintenance",
    )
    neutralizer_terms = (
        "reliability improved",
        "improved reliability",
        "reduced downtime",
        "downtime improved",
        "exceptionally strong utilization",
        "strong utilization",
        "high utilization",
    )
    utilization_patterns = (
        re.compile(r"(?:utilization|operating rate|operating rates|plant utilization)[^%]{0,30}(\d{2,3}(?:\.\d+)?)\s*%", re.I),
        re.compile(r"(\d{2,3}(?:\.\d+)?)\s*%\s*(?:utilization|operating rate|operating rates|plant utilization)", re.I),
    )
    out: Dict[date, Dict[str, Any]] = {}
    paths = _gpre_local_doc_paths(
        ticker_root,
        "earnings_transcripts/GPRE_Q*_transcript.txt",
        "press_release/8-K_*_press_release_doc_*.htm",
        "press_release/8-K_*_press_release_doc_*.html",
        "press_release/8-K_*_press_release_doc_*.txt",
    )
    for path in paths:
        qd = _gpre_local_doc_quarter(path)
        if not isinstance(qd, date):
            continue
        text = _gpre_read_local_textish_doc(path).lower()
        if not text:
            continue
        rec = out.setdefault(
            qd,
            {
                "quarter": qd,
                "negative_terms": set(),
                "neutralizer_terms": set(),
                "utilization_pcts": [],
                "source_docs": [],
            },
        )
        neg_hits = [term for term in negative_terms if term in text]
        neutral_hits = [term for term in neutralizer_terms if term in text]
        rec["negative_terms"].update(neg_hits)
        rec["neutralizer_terms"].update(neutral_hits)
        rec["source_docs"].append(path.name)
        for pat in utilization_patterns:
            for match in pat.finditer(text):
                val = pd.to_numeric(match.group(1), errors="coerce")
                if pd.notna(val):
                    rec["utilization_pcts"].append(float(val))
    final_out: Dict[date, Dict[str, Any]] = {}
    for qd, rec in out.items():
        utilization_vals = [
            float(val)
            for val in list(rec.get("utilization_pcts") or [])
            if pd.notna(pd.to_numeric(val, errors="coerce"))
        ]
        min_utilization = min(utilization_vals) if utilization_vals else None
        negative_ops_flag = bool(rec.get("negative_terms")) and not bool(rec.get("neutralizer_terms"))
        low_util_flag = min_utilization is not None and float(min_utilization) < 90.0
        penalty = min(0.04, (0.02 if negative_ops_flag else 0.0) + (0.02 if low_util_flag else 0.0))
        final_out[qd] = {
            "quarter": qd,
            "negative_ops_flag": int(bool(negative_ops_flag)),
            "low_util_flag": int(bool(low_util_flag)),
            "ops_penalty_usd_per_gal": float(penalty),
            "utilization_pct": min_utilization,
            "negative_terms": sorted(str(item) for item in set(rec.get("negative_terms") or set())),
            "neutralizer_terms": sorted(str(item) for item in set(rec.get("neutralizer_terms") or set())),
            "source_docs": sorted(str(item) for item in set(rec.get("source_docs") or [])),
        }
    return final_out


def _gpre_inventory_timing_signal_map(ticker_root: Optional[Path]) -> Dict[date, Dict[str, Any]]:
    if ticker_root is None:
        return {}
    inventory_terms = (
        "inventory nrv",
        "inventory lower of cost",
        "net realizable value",
    )
    timing_terms = (
        "realized pricing fell below production cost",
        "inventory timing",
    )
    out: Dict[date, Dict[str, Any]] = {}
    paths = _gpre_local_doc_paths(
        ticker_root,
        "earnings_transcripts/GPRE_Q*_transcript.txt",
        "earnings_release/*",
        "press_release/8-K_*_press_release_doc_*.htm",
        "press_release/8-K_*_press_release_doc_*.html",
        "press_release/8-K_*_press_release_doc_*.txt",
    )
    for path in paths:
        qd = _gpre_local_doc_quarter(path)
        if not isinstance(qd, date):
            continue
        text = _gpre_read_local_textish_doc(path).lower()
        if not text:
            continue
        rec = out.setdefault(
            qd,
            {
                "quarter": qd,
                "inventory_terms": set(),
                "timing_terms": set(),
                "source_docs": [],
            },
        )
        rec["inventory_terms"].update(term for term in inventory_terms if term in text)
        rec["timing_terms"].update(term for term in timing_terms if term in text)
        rec["source_docs"].append(path.name)
    final_out: Dict[date, Dict[str, Any]] = {}
    for qd, rec in out.items():
        inventory_hits = sorted(str(item) for item in set(rec.get("inventory_terms") or set()))
        timing_hits = sorted(str(item) for item in set(rec.get("timing_terms") or set()))
        all_terms = [*inventory_hits, *timing_hits]
        if inventory_hits and timing_hits:
            penalty = 0.025
        elif all_terms:
            penalty = 0.015
        else:
            penalty = 0.0
        final_out[qd] = {
            "quarter": qd,
            "inventory_drag_signal_flag": int(bool(all_terms)),
            "inventory_terms": inventory_hits,
            "timing_terms": timing_hits,
            "all_terms": all_terms,
            "inventory_drag_penalty_usd_per_gal": float(penalty),
            "source_docs": sorted(str(item) for item in set(rec.get("source_docs") or [])),
        }
    return final_out


def _gpre_split_signal_terms(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        out = [str(item or "").strip().lower() for item in value if str(item or "").strip()]
    else:
        out = [part.strip().lower() for part in str(value or "").split(",") if part.strip()]
    deduped: List[str] = []
    seen: set[str] = set()
    for item in out:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _gpre_effective_hedge_share(
    disclosed_share: Any,
    pattern_share: Any,
    *,
    cap: float = 0.35,
) -> Tuple[float, str]:
    disclosed_num = pd.to_numeric(disclosed_share, errors="coerce")
    if pd.notna(disclosed_num) and float(disclosed_num) > 1e-12:
        return float(np.clip(float(disclosed_num), 0.0, float(cap))), "disclosed"
    pattern_num = pd.to_numeric(pattern_share, errors="coerce")
    if pd.notna(pattern_num) and float(pattern_num) > 1e-12:
        return float(np.clip(float(pattern_num), 0.0, float(cap))), "pattern"
    return 0.0, "pattern"


def _gpre_hedge_realization_value(
    base_value: Any,
    *,
    disclosed_share: Any,
    pattern_share: Any,
    disclosed_reference: Any,
    pattern_reference: Any,
    cap: float = 0.35,
) -> Optional[float]:
    base_num = pd.to_numeric(base_value, errors="coerce")
    if pd.isna(base_num):
        return None
    hedge_weight, source_kind = _gpre_effective_hedge_share(
        disclosed_share,
        pattern_share,
        cap=cap,
    )
    if hedge_weight <= 1e-12:
        return float(base_num)
    reference_num = pd.to_numeric(
        disclosed_reference if source_kind == "disclosed" else pattern_reference,
        errors="coerce",
    )
    if pd.isna(reference_num):
        return float(base_num)
    return ((1.0 - float(hedge_weight)) * float(base_num)) + (float(hedge_weight) * float(reference_num))


def _gpre_locked_setup_value(
    base_value: Any,
    *,
    disclosed_share: Any,
    pattern_share: Any,
    quarter_open_anchor: Any,
    cap: float = 0.40,
) -> Optional[float]:
    base_num = pd.to_numeric(base_value, errors="coerce")
    anchor_num = pd.to_numeric(quarter_open_anchor, errors="coerce")
    if pd.isna(base_num):
        return None
    if pd.isna(anchor_num):
        return float(base_num)
    locked_share, _ = _gpre_effective_hedge_share(
        disclosed_share,
        pattern_share,
        cap=cap,
    )
    if locked_share <= 1e-12:
        return float(base_num)
    return float(base_num) + (float(locked_share) * (float(anchor_num) - float(base_num)))


def _gpre_basis_passthrough_value(
    current_basis_value: Any,
    front_loaded_basis_value: Any,
    *,
    beta: float,
) -> Optional[float]:
    return _blend_optional_values(
        current_basis_value,
        front_loaded_basis_value,
        anchor_weight=float(np.clip(beta, 0.0, 1.0)),
        current_weight=float(np.clip(1.0 - float(beta), 0.0, 1.0)),
    )


def _gpre_regime_basis_passthrough_beta(
    disclosed_share: Any,
    pattern_share: Any,
    *,
    default_beta: float = 0.60,
    min_beta: float = 0.45,
    max_beta: float = 0.80,
    share_cap: float = 0.35,
) -> float:
    locked_share, _ = _gpre_effective_hedge_share(
        disclosed_share,
        pattern_share,
        cap=share_cap,
    )
    if locked_share <= 1e-12:
        return float(default_beta)
    return float(np.clip(float(max_beta) - float(locked_share), float(min_beta), float(max_beta)))


def _gpre_realization_residual_penalty(
    utilization_penalty: Any,
    maintenance_delay_penalty: Any,
    inventory_timing_drag_penalty: Any,
) -> float:
    util_num = pd.to_numeric(utilization_penalty, errors="coerce")
    maintenance_num = pd.to_numeric(maintenance_delay_penalty, errors="coerce")
    inventory_num = pd.to_numeric(inventory_timing_drag_penalty, errors="coerce")
    penalty = (
        (0.0 if pd.isna(util_num) else float(util_num))
        + (0.75 * (0.0 if pd.isna(maintenance_num) else float(maintenance_num)))
        + (0.0 if pd.isna(inventory_num) else float(inventory_num))
    )
    return float(np.clip(penalty, 0.0, 0.055))


def _gpre_sold_minus_produced_gap_ratio(
    sold_gallons_raw: Any,
    produced_gallons_raw: Any,
) -> Optional[float]:
    sold_num = pd.to_numeric(sold_gallons_raw, errors="coerce")
    produced_num = pd.to_numeric(produced_gallons_raw, errors="coerce")
    if pd.isna(produced_num) or float(produced_num) <= 0.0:
        return None
    gap_ratio = (0.0 if pd.isna(sold_num) else float(sold_num)) - float(produced_num)
    gap_ratio = gap_ratio / float(produced_num)
    return float(np.clip(gap_ratio, -0.20, 0.20))


def _gpre_inventory_gap_disturbance_score(gap_ratio: Any) -> float:
    gap_num = pd.to_numeric(gap_ratio, errors="coerce")
    if pd.isna(gap_num):
        return 0.0
    return float(np.clip((abs(float(gap_num)) - 0.03) / 0.09, 0.0, 1.0))


def _gpre_low_utilization_regime_score(utilization_pct: Any) -> float:
    util_num = pd.to_numeric(utilization_pct, errors="coerce")
    if pd.isna(util_num):
        return 0.0
    return float(np.clip((92.0 - float(util_num)) / 12.0, 0.0, 1.0))


def _gpre_high_utilization_regime_score(utilization_pct: Any) -> float:
    util_num = pd.to_numeric(utilization_pct, errors="coerce")
    if pd.isna(util_num):
        return 0.0
    return float(np.clip((float(util_num) - 95.0) / 5.0, 0.0, 1.0))


def _gpre_inventory_gap_penalty(
    disturbance_score: Any,
    *,
    cap: float,
) -> float:
    disturbance_num = pd.to_numeric(disturbance_score, errors="coerce")
    if pd.isna(disturbance_num):
        return 0.0
    return float(np.clip(float(disturbance_num) * float(cap), 0.0, float(cap)))


def _gpre_exec_inventory_combo_penalty(
    maintenance_delay_penalty: Any,
    disturbance_score: Any,
) -> float:
    maintenance_num = pd.to_numeric(maintenance_delay_penalty, errors="coerce")
    disturbance_num = pd.to_numeric(disturbance_score, errors="coerce")
    maintenance_scale = 0.0 if pd.isna(maintenance_num) else min(1.0, float(maintenance_num) / 0.015)
    disturbance_scale = 0.0 if pd.isna(disturbance_num) else float(disturbance_num)
    combo_activation = maintenance_scale * disturbance_scale
    return float(np.clip(combo_activation * 0.025, 0.0, 0.025))


def _gpre_asymmetric_passthrough_value(
    beta35_value: Any,
    beta65_value: Any,
) -> Optional[float]:
    base_num = pd.to_numeric(beta35_value, errors="coerce")
    high_num = pd.to_numeric(beta65_value, errors="coerce")
    if pd.isna(base_num):
        return None
    if pd.isna(high_num):
        return float(base_num)
    delta = float(high_num) - float(base_num)
    passthrough_weight = 0.45 if delta > 0.0 else 0.80
    return float(base_num) + (passthrough_weight * delta)


def _gpre_utilization_regime_blend_value(
    base_exec_value: Any,
    high_util_score: Any,
    low_util_score: Any,
    current50_exec_value: Any,
    locked_setup_value: Any,
) -> Optional[float]:
    base_num = pd.to_numeric(base_exec_value, errors="coerce")
    current50_num = pd.to_numeric(current50_exec_value, errors="coerce")
    locked_num = pd.to_numeric(locked_setup_value, errors="coerce")
    if pd.isna(base_num):
        return None
    if pd.isna(current50_num) or pd.isna(locked_num):
        return float(base_num)
    high_num = pd.to_numeric(high_util_score, errors="coerce")
    low_num = pd.to_numeric(low_util_score, errors="coerce")
    return float(base_num) + (
        (0.0 if pd.isna(high_num) else float(high_num)) * 0.50 * (float(current50_num) - float(base_num))
    ) + (
        (0.0 if pd.isna(low_num) else float(low_num)) * 0.50 * (float(locked_num) - float(base_num))
    )


def _gpre_utilization_regime_residual_value(
    base_exec_value: Any,
    low_util_score: Any,
    residual_penalty: Any,
) -> Optional[float]:
    base_num = pd.to_numeric(base_exec_value, errors="coerce")
    if pd.isna(base_num):
        return None
    low_num = pd.to_numeric(low_util_score, errors="coerce")
    penalty_num = pd.to_numeric(residual_penalty, errors="coerce")
    extra_drag = float(
        np.clip(
            (0.0 if pd.isna(low_num) else float(low_num)) * 0.60 * (0.0 if pd.isna(penalty_num) else float(penalty_num)),
            0.0,
            0.020,
        )
    )
    return float(base_num) - extra_drag


def _gpre_residual_regime_value(
    disturbed_flag: Any,
    locked_flag: Any,
    disturbed_value: Any,
    locked_value: Any,
    normal_value: Any,
) -> Optional[float]:
    disturbed_num = pd.to_numeric(disturbed_value, errors="coerce")
    locked_num = pd.to_numeric(locked_value, errors="coerce")
    normal_num = pd.to_numeric(normal_value, errors="coerce")
    if bool(disturbed_flag) and pd.notna(disturbed_num):
        return float(disturbed_num)
    if bool(locked_flag) and pd.notna(locked_num):
        return float(locked_num)
    if pd.notna(normal_num):
        return float(normal_num)
    return None


def _gpre_gated_model_value(
    hard_quarter_flag: Any,
    disturbed_flag: Any,
    residual_value: Any,
    incumbent_value: Any,
) -> Optional[float]:
    residual_num = pd.to_numeric(residual_value, errors="coerce")
    incumbent_num = pd.to_numeric(incumbent_value, errors="coerce")
    if (bool(hard_quarter_flag) or bool(disturbed_flag)) and pd.notna(residual_num):
        return float(residual_num)
    if pd.notna(incumbent_num):
        return float(incumbent_num)
    return None


def _gpre_prior_gap_carryover_value(
    base_value: Any,
    prior_gap_penalty: Any,
    *,
    multiplier: float = 0.50,
    cap: float = 0.03,
) -> Optional[float]:
    base_num = pd.to_numeric(base_value, errors="coerce")
    if pd.isna(base_num):
        return None
    prior_penalty_num = pd.to_numeric(prior_gap_penalty, errors="coerce")
    if pd.isna(prior_penalty_num):
        return float(base_num)
    carryover = float(np.clip(float(multiplier) * float(prior_penalty_num), 0.0, float(cap)))
    return float(base_num) - carryover


def _gpre_prior_disturbance_carryover_value(
    base_value: Any,
    *,
    prior_disturbed_flag: Any,
    prior_hard_flag: Any,
    prior_residual_penalty: Any,
    multiplier: float = 0.60,
    cap: float = 0.04,
) -> Optional[float]:
    base_num = pd.to_numeric(base_value, errors="coerce")
    if pd.isna(base_num):
        return None
    if not (bool(prior_disturbed_flag) or bool(prior_hard_flag)):
        return float(base_num)
    prior_penalty_num = pd.to_numeric(prior_residual_penalty, errors="coerce")
    if pd.isna(prior_penalty_num):
        return float(base_num)
    carryover = float(np.clip(float(multiplier) * float(prior_penalty_num), 0.0, float(cap)))
    return float(base_num) - carryover


def _gpre_walk_forward_tail_mae(
    quarterly_df: pd.DataFrame,
    *,
    pred_col: str,
    actual_col: str = "evaluation_target_margin_usd_per_gal",
    tail_quarters: int = 4,
) -> float:
    if (
        quarterly_df is None
        or quarterly_df.empty
        or pred_col not in quarterly_df.columns
        or actual_col not in quarterly_df.columns
    ):
        return float("nan")
    sub = pd.DataFrame(
        {
            "quarter": pd.to_datetime(quarterly_df["quarter"], errors="coerce"),
            "pred": pd.to_numeric(quarterly_df[pred_col], errors="coerce"),
            "actual": pd.to_numeric(quarterly_df[actual_col], errors="coerce"),
        }
    )
    sub = sub[sub["quarter"].notna() & sub["pred"].notna() & sub["actual"].notna()].copy()
    if sub.empty:
        return float("nan")
    sub = sub.sort_values("quarter").tail(max(int(tail_quarters), 1)).copy()
    if sub.empty:
        return float("nan")
    return float((sub["pred"] - sub["actual"]).abs().mean())


def _gpre_signal_coverage_stats(
    quarterly_df: pd.DataFrame,
    *,
    pred_col: str,
    actual_col: str = "evaluation_target_margin_usd_per_gal",
) -> Tuple[int, float]:
    if (
        quarterly_df is None
        or quarterly_df.empty
        or pred_col not in quarterly_df.columns
        or actual_col not in quarterly_df.columns
    ):
        return 0, 0.0
    actual_series = pd.to_numeric(quarterly_df[actual_col], errors="coerce")
    pred_series = pd.to_numeric(quarterly_df[pred_col], errors="coerce")
    evaluable_mask = actual_series.notna()
    evaluable_count = int(evaluable_mask.sum())
    if evaluable_count <= 0:
        return 0, 0.0
    coverage_count = int((evaluable_mask & pred_series.notna()).sum())
    return coverage_count, float(coverage_count / evaluable_count)


def _gpre_forward_usability_rating(
    model_key: Any,
    *,
    family: Any = "",
) -> str:
    key_txt = str(model_key or "").strip()
    family_txt = str(family or "").strip()
    explicit: Dict[str, str] = {
        "process_market_process_ensemble_35_65": "medium",
        "process_locked_share_asymmetric_passthrough": "high",
        "process_prior_gap_carryover_small": "high",
        "process_prior_disturbance_carryover": "high",
        "process_inventory_gap_penalty_small": "low",
        "process_inventory_gap_penalty_medium": "low",
        "process_utilization_regime_blend": "medium",
        "process_utilization_regime_residual": "low",
        "process_exec_inventory_combo_medium": "low",
        "process_asymmetric_basis_passthrough": "high",
        "process_residual_regime_locked_vs_disturbed": "low",
        "process_gated_incumbent_vs_residual": "low",
        "process_quarter_open_blend_exec_penalty": "medium",
        "process_quarter_open_blend_ops_penalty": "medium",
        "process_quarter_open_blend_hedge_realization": "medium",
        "process_quarter_open_blend_utilization_penalty": "medium",
        "process_quarter_open_blend_maintenance_delay_penalty": "medium",
        "process_quarter_open_blend_inventory_timing_drag": "medium",
        "process_front_loaded_ops_penalty": "medium",
        "process_two_stage_realization_residual": "low",
    }
    if key_txt in explicit:
        return explicit[key_txt]
    if family_txt in {
        "bridge_timing",
        "disclosed_hedge_memo",
        "pattern_hedge_memo",
        "bid_offset",
        "process_family",
        "process_blend",
        "process_blend_locked_setup",
        "basis_blend_current_front",
        "basis_passthrough_beta",
        "regime_basis_passthrough",
        "capacity_weighted_basis_strict",
        "simple_market",
    }:
        return "high"
    if family_txt in {
        "process_ops_penalty",
        "process_geo",
    }:
        return "medium"
    return "medium"


def _gpre_complexity_rating(
    model_key: Any,
    *,
    family: Any = "",
) -> str:
    key_txt = str(model_key or "").strip()
    family_txt = str(family or "").strip()
    explicit: Dict[str, str] = {
        "process_market_process_ensemble_35_65": "low",
        "process_locked_share_asymmetric_passthrough": "moderate",
        "process_prior_gap_carryover_small": "low",
        "process_prior_disturbance_carryover": "low",
        "process_inventory_gap_penalty_small": "low",
        "process_inventory_gap_penalty_medium": "low",
        "process_utilization_regime_blend": "moderate",
        "process_utilization_regime_residual": "moderate",
        "process_exec_inventory_combo_medium": "moderate",
        "process_asymmetric_basis_passthrough": "low",
        "process_residual_regime_locked_vs_disturbed": "high",
        "process_gated_incumbent_vs_residual": "high",
        "process_quarter_open_blend_exec_penalty": "low",
        "process_quarter_open_blend_ops_penalty": "low",
        "process_quarter_open_blend_hedge_realization": "moderate",
        "process_quarter_open_blend_utilization_penalty": "low",
        "process_quarter_open_blend_maintenance_delay_penalty": "low",
        "process_quarter_open_blend_inventory_timing_drag": "low",
        "process_front_loaded_ops_penalty": "low",
        "process_front_loaded_ethanol_geo": "low",
        "process_two_stage_realization_residual": "moderate",
    }
    if key_txt in explicit:
        return explicit[key_txt]
    if family_txt in {
        "simple_market",
        "bridge_timing",
        "process_family",
        "disclosed_hedge_memo",
        "pattern_hedge_memo",
        "bid_offset",
        "process_blend",
        "process_blend_locked_setup",
        "basis_blend_current_front",
        "basis_passthrough_beta",
        "regime_basis_passthrough",
        "capacity_weighted_basis_strict",
    }:
        return "low"
    if family_txt in {
        "utilization_regime",
        "exec_inventory_combo",
        "residual_regime",
        "gated_ensemble",
    }:
        return "moderate"
    return "moderate"


def _gpre_phase_preview_story(model_key: str, *, phase: str) -> Dict[str, str]:
    key = str(model_key or "")
    if key == "process_inventory_gap_penalty_small":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview subtracts a small bounded sold-minus-produced inventory-gap penalty from the incumbent process execution model.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview carries forward the same small bounded inventory-gap penalty on top of the incumbent process execution model.",
            }
    if key == "process_inventory_gap_penalty_medium":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview subtracts a medium bounded sold-minus-produced inventory-gap penalty from the incumbent process execution model.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview carries forward the same medium bounded inventory-gap penalty on top of the incumbent process execution model.",
            }
    if key == "process_utilization_regime_blend":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview keeps the incumbent severe-execution base, then tilts toward more current-process passthrough at high utilization and toward locked-setup behavior at low utilization.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same bounded utilization-regime blend between the incumbent process-execution base, the 50/50 q-open/current blend, and locked-setup behavior.",
            }
    if key == "process_utilization_regime_residual":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview keeps the incumbent severe-execution base and adds extra bounded residual drag only in lower-utilization quarters.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same low-utilization residual-drag overlay on top of the incumbent process-execution base.",
            }
    if key == "process_exec_inventory_combo_medium":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview subtracts a bounded combo penalty that only activates when explicit maintenance/outage evidence and a meaningful sold-produced gap appear together.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview keeps the same bounded execution-plus-inventory combo penalty when both signals are active.",
            }
    if key == "process_asymmetric_basis_passthrough":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview starts from the beta-0.35 basis passthrough and lets downside passthrough matter more than upside passthrough.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same asymmetric current-basis passthrough, with stronger downside than upside transmission.",
            }
    if key == "process_market_process_ensemble_35_65":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview blends 35% of the official/simple market row with 65% of the quarter-open severe-execution process proxy.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview keeps the same 35/65 market-process blend, using the official next-quarter lens plus the bounded severe-execution process preview.",
            }
    if key == "process_locked_share_asymmetric_passthrough":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview uses capped locked-share evidence to blend locked/setup behavior with the asymmetric basis-passthrough lens.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview reuses the same capped locked-share blend between locked/setup behavior and the asymmetric passthrough lens.",
            }
    if key == "process_prior_gap_carryover_small":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview subtracts a small bounded carryover from the prior quarter's sold-produced gap penalty from the quarter-open/current process blend.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview carries the current quarter's small inventory-gap penalty forward one quarter on top of the future process blend.",
            }
    if key == "process_prior_disturbance_carryover":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview keeps the beta-0.35 passthrough base and only subtracts carryover drag when the prior quarter was hard or disturbed.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview keeps the same bounded prior-disturbance carryover on top of the beta-0.35 passthrough base.",
            }
    if key == "process_residual_regime_locked_vs_disturbed":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview switches explicitly between disturbed-quarter residual logic, locked/setup regime passthrough, and the normal beta-0.35 basis passthrough base.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview carries forward the same explicit disturbed-vs-locked-vs-normal residual regime split.",
            }
    if key == "process_gated_incumbent_vs_residual":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview gates between the current production winner and the two-stage residual challenger, using hard-quarter and disturbed-quarter flags.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same explicit incumbent-vs-residual regime gate for hard or disturbed quarters.",
            }
    if key == "process_basis_blend_current40_front60":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview uses a 40/60 blend of current and front-loaded process basis.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview carries forward the same 40/60 current-vs-front-loaded process basis blend.",
            }
    if key == "process_basis_passthrough_beta35":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview starts from front-loaded basis and lets 35% of the current basis move pass through.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same 0.35 current-basis passthrough against the front-loaded basis anchor.",
            }
    if key == "process_basis_passthrough_beta65":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview starts from front-loaded basis and lets 65% of the current basis move pass through.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same 0.65 current-basis passthrough against the front-loaded basis anchor.",
            }
    if key == "process_quarter_open_current50_exec_penalty":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview uses a 50/50 quarter-open/current process blend and subtracts the bounded severe execution penalty.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview applies the same 50/50 process blend family with the bounded severe execution penalty when signals are present.",
            }
    if key == "process_regime_basis_passthrough":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview uses a locked-share-aware current-basis passthrough beta on top of the front-loaded basis anchor.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview keeps the same regime beta logic, letting current basis matter less in locked quarters and more in open quarters.",
            }
    if key == "process_two_stage_realization_residual":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview uses a basis-passthrough base, then applies the capped locked-setup pull and bounded realization residual penalty.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview uses the same two-stage basis plus residual structure with capped locked-setup and realization penalties.",
            }
    if key == "process_capacity_weighted_basis_strict":
        if phase == "current":
            return {
                "live_preview_mode": "exact_formula",
                "live_preview_note": "Current fitted preview uses the strict active-capacity-weighted basis-adjusted process proxy directly.",
            }
        if phase == "next":
            return {
                "live_preview_mode": "reduced_form_approximation",
                "live_preview_note": "Next-quarter fitted preview carries forward the strict active-capacity-weighted basis-adjusted process proxy.",
            }
    return {}


def _gpre_execution_penalty_details(
    base_ops_penalty: Any,
    negative_terms: Any,
    utilization_pct: Any,
) -> Dict[str, Any]:
    base_penalty_num = pd.to_numeric(base_ops_penalty, errors="coerce")
    base_penalty = 0.0 if pd.isna(base_penalty_num) else float(base_penalty_num)
    term_list = _gpre_split_signal_terms(negative_terms)
    severe_terms = {"care and maintenance", "unplanned downtime", "cold snap"}
    severe_term_flag = int(len(term_list) >= 2 or any(term in severe_terms for term in term_list))
    util_num = pd.to_numeric(utilization_pct, errors="coerce")
    very_low_util_flag = int(pd.notna(util_num) and float(util_num) < 85.0)
    extra_penalty = min(0.02, (0.01 * severe_term_flag) + (0.01 * very_low_util_flag))
    total_penalty = min(0.06, base_penalty + extra_penalty)
    return {
        "negative_term_count": int(len(term_list)),
        "severe_term_flag": int(severe_term_flag),
        "very_low_util_flag": int(very_low_util_flag),
        "extra_execution_penalty_usd_per_gal": float(extra_penalty),
        "total_execution_penalty_usd_per_gal": float(total_penalty),
    }


def _gpre_utilization_overlay_penalty(utilization_pct: Any) -> float:
    util_num = pd.to_numeric(utilization_pct, errors="coerce")
    if pd.isna(util_num) or float(util_num) >= 95.0:
        return 0.0
    return float(np.clip((95.0 - float(util_num)) * 0.0025, 0.0, 0.035))


def _gpre_maintenance_delay_penalty_details(negative_terms: Any) -> Dict[str, Any]:
    trigger_terms = {
        "planned maintenance",
        "unplanned downtime",
        "outage",
        "outages",
        "delay",
        "delays",
        "restart",
        "ramp",
        "cold snap",
        "care and maintenance",
    }
    severe_terms = {"unplanned downtime", "outage", "outages", "cold snap", "care and maintenance"}
    term_list = _gpre_split_signal_terms(negative_terms)
    hits = sorted(term for term in term_list if term in trigger_terms)
    distinct_count = len(set(hits))
    penalty = 0.015 if hits else 0.0
    if any(term in severe_terms for term in hits):
        penalty += 0.005
    if distinct_count >= 2:
        penalty += 0.005
    penalty = float(min(penalty, 0.025))
    return {
        "trigger_terms": hits,
        "trigger_term_count": distinct_count,
        "penalty_usd_per_gal": penalty,
    }


def _gpre_inventory_timing_penalty_details(
    inventory_terms: Any,
    timing_terms: Any,
) -> Dict[str, Any]:
    inventory_hits = _gpre_split_signal_terms(inventory_terms)
    timing_hits = _gpre_split_signal_terms(timing_terms)
    if inventory_hits and timing_hits:
        penalty = 0.025
    elif inventory_hits or timing_hits:
        penalty = 0.015
    else:
        penalty = 0.0
    return {
        "inventory_terms": inventory_hits,
        "timing_terms": timing_hits,
        "penalty_usd_per_gal": float(penalty),
    }


def _gpre_preview_quality_status(mae_value: Any, max_error_value: Any) -> str:
    mae_num = pd.to_numeric(mae_value, errors="coerce")
    max_num = pd.to_numeric(max_error_value, errors="coerce")
    if pd.isna(mae_num) or pd.isna(max_num):
        return "not_faithful_enough"
    mae_val = float(mae_num)
    max_val = float(max_num)
    if mae_val <= 0.010 and max_val <= 0.020:
        return "close"
    if mae_val <= 0.020 and max_val <= 0.040:
        return "acceptable"
    if mae_val <= 0.030 and max_val <= 0.060:
        return "loose"
    return "not_faithful_enough"


def _gpre_preview_phase_metrics(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {
            "preview_abs_error_usd_per_gal": [],
            "preview_mae": None,
            "preview_max_error": None,
            "preview_bias": None,
            "preview_top_miss_quarters": "",
            "preview_test_count": 0,
        }
    abs_errors = [float(rec.get("abs_error") or 0.0) for rec in records]
    signed_errors = [float(rec.get("signed_error") or 0.0) for rec in records]
    max_abs_error = float(max(abs_errors))
    top_recs = [] if max_abs_error <= 1e-12 else sorted(records, key=lambda rec: float(rec.get("abs_error") or 0.0), reverse=True)[:5]
    top_labels = []
    for rec in top_recs:
        quarter_label = str(rec.get("quarter_label") or "").strip()
        phase_label = str(rec.get("phase") or "").strip()
        if quarter_label and phase_label:
            top_labels.append(f"{phase_label}:{quarter_label}")
        elif quarter_label:
            top_labels.append(quarter_label)
    return {
        "preview_abs_error_usd_per_gal": [
            {
                "quarter": str(rec.get("quarter_label") or ""),
                "phase": str(rec.get("phase") or ""),
                "abs_error": float(rec.get("abs_error") or 0.0),
                "signed_error": float(rec.get("signed_error") or 0.0),
                "full_value": rec.get("full_value"),
                "preview_value": rec.get("preview_value"),
            }
            for rec in records
        ],
        "preview_mae": float(np.mean(abs_errors)),
        "preview_max_error": max_abs_error,
        "preview_bias": float(np.mean(signed_errors)),
        "preview_top_miss_quarters": ", ".join(top_labels),
        "preview_test_count": int(len(records)),
    }


def _gpre_bias_direction(mean_error: Any, *, neutral_threshold: float = 0.010) -> str:
    mean_error_num = pd.to_numeric(mean_error, errors="coerce")
    if pd.isna(mean_error_num):
        return ""
    if float(mean_error_num) > float(neutral_threshold):
        return "overpredict"
    if float(mean_error_num) < -float(neutral_threshold):
        return "underpredict"
    return "neutral"


def _gpre_diff_vs_official_stats(
    pred_series: pd.Series,
    official_series: pd.Series,
) -> Dict[str, Any]:
    pred_num = pd.to_numeric(pred_series, errors="coerce")
    official_num = pd.to_numeric(official_series, errors="coerce")
    valid = pred_num.notna() & official_num.notna()
    if not valid.any():
        return {
            "avg_abs_diff_vs_official": None,
            "diff_quarters_gt_0_02_vs_official": 0,
            "diff_quarters_gt_0_05_vs_official": 0,
        }
    diff = pred_num[valid].sub(official_num[valid]).abs()
    return {
        "avg_abs_diff_vs_official": float(diff.mean()),
        "diff_quarters_gt_0_02_vs_official": int(diff.gt(0.02).sum()),
        "diff_quarters_gt_0_05_vs_official": int(diff.gt(0.05).sum()),
    }


def _gpre_hard_quarter_reason(row: pd.Series) -> str:
    reasons: List[str] = []
    if bool(row.get("ops_negative_signal_flag")):
        reasons.append("ops_signal")
    if bool(row.get("ops_low_util_flag")):
        reasons.append("low_util")
    disclosed_share = pd.to_numeric(row.get("hedge_share_disclosed"), errors="coerce")
    pattern_share = pd.to_numeric(row.get("hedge_share_pattern"), errors="coerce")
    if (
        (pd.notna(disclosed_share) and float(disclosed_share) >= 0.20)
        or (pd.notna(pattern_share) and float(pattern_share) >= 0.20)
    ):
        reasons.append("hedge_share")
    target_num = pd.to_numeric(row.get("evaluation_target_margin_usd_per_gal"), errors="coerce")
    official_num = pd.to_numeric(row.get("official_simple_proxy_usd_per_gal"), errors="coerce")
    if pd.notna(target_num) and pd.notna(official_num) and abs(float(target_num) - float(official_num)) > 0.075:
        reasons.append("official_gap")
    return ", ".join(reasons)


def _gpre_hard_quarter_stats(
    frame: pd.DataFrame,
    pred_col: str,
    *,
    actual_col: str = "evaluation_target_margin_usd_per_gal",
) -> Dict[str, Any]:
    if (
        frame is None
        or frame.empty
        or pred_col not in frame.columns
        or actual_col not in frame.columns
        or "hard_quarter_flag" not in frame.columns
    ):
        return {
            "hard_quarter_mae": None,
            "hard_quarter_mean_error": None,
            "hard_quarter_count": 0,
            "hard_quarter_top_miss_quarters": "",
        }
    sub = frame[frame["hard_quarter_flag"].astype(bool)].copy()
    sub["pred"] = pd.to_numeric(sub[pred_col], errors="coerce")
    sub["actual"] = pd.to_numeric(sub[actual_col], errors="coerce")
    sub["quarter_ts"] = pd.to_datetime(sub.get("quarter"), errors="coerce")
    sub = sub[sub["pred"].notna() & sub["actual"].notna() & sub["quarter_ts"].notna()].copy()
    if sub.empty:
        return {
            "hard_quarter_mae": None,
            "hard_quarter_mean_error": None,
            "hard_quarter_count": 0,
            "hard_quarter_top_miss_quarters": "",
        }
    sub["err"] = sub["pred"] - sub["actual"]
    top_recs = sub.assign(abs_err=sub["err"].abs()).sort_values("abs_err", ascending=False).head(5)
    top_labels = [_quarter_label(pd.Timestamp(qd).date()) for qd in top_recs["quarter_ts"] if pd.notna(qd)]
    return {
        "hard_quarter_mae": float(np.mean(np.abs(pd.to_numeric(sub["err"], errors="coerce")))),
        "hard_quarter_mean_error": float(np.mean(pd.to_numeric(sub["err"], errors="coerce"))),
        "hard_quarter_count": int(len(sub)),
        "hard_quarter_top_miss_quarters": ", ".join(top_labels),
    }


def _gpre_quarterly_footprint(
    quarters: Iterable[date],
    ticker_root: Optional[Path],
    plant_capacity_history: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    records = [
        _gpre_footprint_for_quarter(
            qd,
            ticker_root=ticker_root,
            plant_capacity_history=plant_capacity_history,
        )
        for qd in _sorted_quarters(quarters)
    ]
    return pd.DataFrame(records)


def _project_to_capped_simplex(values: np.ndarray, cap: float = 0.5) -> np.ndarray:
    vec = np.asarray(values, dtype=float).reshape(-1)
    if vec.size == 0:
        return vec
    if cap * vec.size < 1.0:
        raise ValueError("cap too small for simplex projection")
    lo = float(np.min(vec) - cap)
    hi = float(np.max(vec))
    for _ in range(120):
        mid = (lo + hi) / 2.0
        projected = np.clip(vec - mid, 0.0, cap)
        total = float(projected.sum())
        if abs(total - 1.0) <= 1e-10:
            return projected
        if total > 1.0:
            lo = mid
        else:
            hi = mid
    projected = np.clip(vec - hi, 0.0, cap)
    total = float(projected.sum())
    if total <= 0.0:
        return np.full(vec.size, 1.0 / float(vec.size))
    return projected / total


def _optimize_basis_weights(
    basis_matrix: np.ndarray,
    target_gap: np.ndarray,
    *,
    yield_per_bushel: float = 2.9,
    cap: float = 0.5,
    iterations: int = 2500,
    lr: float = 0.2,
) -> np.ndarray:
    rows = np.asarray(basis_matrix, dtype=float)
    target = np.asarray(target_gap, dtype=float).reshape(-1)
    if rows.ndim != 2 or rows.shape[0] == 0 or rows.shape[1] == 0:
        return np.array([], dtype=float)
    valid_mask = np.isfinite(rows).all(axis=1) & np.isfinite(target)
    if not valid_mask.any():
        return _project_to_capped_simplex(np.ones(rows.shape[1], dtype=float), cap=cap)
    x = rows[valid_mask]
    y = target[valid_mask]
    w = _project_to_capped_simplex(np.ones(x.shape[1], dtype=float), cap=cap)
    denom = max(float(yield_per_bushel or 2.9), 1e-9)
    for _ in range(max(int(iterations or 0), 1)):
        pred = -(x @ w) / denom
        err = pred - y
        grad = (2.0 / max(len(y), 1)) * (-(x.T @ err) / denom)
        w = _project_to_capped_simplex(w - (float(lr) * grad), cap=cap)
    return w


def _directional_accuracy(frame: pd.DataFrame, pred_col: str, actual_col: str) -> Optional[float]:
    if frame is None or frame.empty or pred_col not in frame.columns or actual_col not in frame.columns:
        return None
    sub = frame[["quarter", pred_col, actual_col]].copy()
    sub = sub[pd.to_numeric(sub[pred_col], errors="coerce").notna() & pd.to_numeric(sub[actual_col], errors="coerce").notna()]
    if len(sub) < 2:
        return None
    sub["quarter"] = pd.to_datetime(sub["quarter"], errors="coerce")
    sub = sub.dropna(subset=["quarter"]).sort_values("quarter")
    pred_delta = pd.to_numeric(sub[pred_col], errors="coerce").diff()
    actual_delta = pd.to_numeric(sub[actual_col], errors="coerce").diff()
    valid = pred_delta.notna() & actual_delta.notna() & (actual_delta != 0)
    if not valid.any():
        return None
    hits = (np.sign(pred_delta[valid]) == np.sign(actual_delta[valid])).astype(float)
    return float(hits.mean())


def _metrics_for_prediction(
    frame: pd.DataFrame,
    pred_col: str,
    *,
    label: str,
    split: str,
    actual_col: str = "target_reported_crush_margin_usd_per_gal",
) -> Dict[str, Any]:
    sub = frame.copy()
    if actual_col not in sub.columns or pred_col not in sub.columns:
        return {
            "model_key": label,
            "split": split,
            "n_quarters": 0,
            "correlation": None,
            "rmse": None,
            "mae": None,
            "mean_error": None,
            "directional_accuracy": None,
            "sign_hit_rate": None,
            "q1_mae": None,
            "q2_mae": None,
            "q3_mae": None,
            "q4_mae": None,
            "q1_mean_error": None,
            "q2_mean_error": None,
            "q3_mean_error": None,
            "q4_mean_error": None,
            "top_miss_quarters": "",
        }
    sub["actual"] = pd.to_numeric(sub[actual_col], errors="coerce")
    sub["pred"] = pd.to_numeric(sub[pred_col], errors="coerce")
    sub = sub[sub["actual"].notna() & sub["pred"].notna()].copy()
    if sub.empty:
        return {
            "model_key": label,
            "split": split,
            "n_quarters": 0,
            "correlation": None,
            "rmse": None,
            "mae": None,
            "mean_error": None,
            "directional_accuracy": None,
            "sign_hit_rate": None,
            "q1_mae": None,
            "q2_mae": None,
            "q3_mae": None,
            "q4_mae": None,
            "q1_mean_error": None,
            "q2_mean_error": None,
            "q3_mean_error": None,
            "q4_mean_error": None,
            "top_miss_quarters": "",
        }
    err = sub["pred"] - sub["actual"]
    sub["quarter_ts"] = pd.to_datetime(sub.get("quarter"), errors="coerce")
    sub["quarter_num"] = (((sub["quarter_ts"].dt.month.fillna(0).astype(int) - 1) // 3) + 1).where(sub["quarter_ts"].notna(), np.nan)
    corr = None
    if len(sub) >= 2 and float(sub["actual"].std(ddof=0) or 0.0) > 0 and float(sub["pred"].std(ddof=0) or 0.0) > 0:
        corr = float(np.corrcoef(sub["pred"], sub["actual"])[0, 1])
    sign_hit_rate = None
    sign_valid = sub["actual"].ne(0)
    if sign_valid.any():
        sign_hit_rate = float((np.sign(sub.loc[sign_valid, "pred"]) == np.sign(sub.loc[sign_valid, "actual"])).astype(float).mean())

    def _quarter_mae(quarter_num: int) -> Optional[float]:
        q_sub = sub[sub["quarter_num"] == int(quarter_num)].copy()
        if q_sub.empty:
            return None
        return float(np.mean(np.abs(pd.to_numeric(q_sub["pred"], errors="coerce") - pd.to_numeric(q_sub["actual"], errors="coerce"))))

    def _quarter_mean_error(quarter_num: int) -> Optional[float]:
        q_sub = sub[sub["quarter_num"] == int(quarter_num)].copy()
        if q_sub.empty:
            return None
        return float(
            np.mean(
                pd.to_numeric(q_sub["pred"], errors="coerce")
                - pd.to_numeric(q_sub["actual"], errors="coerce")
            )
        )

    top_miss_quarters = ""
    if "quarter_ts" in sub.columns:
        top_miss = sub.assign(abs_err=np.abs(err)).sort_values("abs_err", ascending=False).head(5)
        labels = []
        for _, rec in top_miss.iterrows():
            qd = rec.get("quarter_ts")
            if pd.notna(qd):
                labels.append(_quarter_label(pd.Timestamp(qd).date()))
        top_miss_quarters = ", ".join(labels)
    return {
        "model_key": label,
        "split": split,
        "n_quarters": int(len(sub)),
        "correlation": corr,
        "rmse": float(np.sqrt(np.mean(np.square(err)))),
        "mae": float(np.mean(np.abs(err))),
        "mean_error": float(np.mean(err)),
        "directional_accuracy": _directional_accuracy(sub, "pred", "actual"),
        "sign_hit_rate": sign_hit_rate,
        "q1_mae": _quarter_mae(1),
        "q2_mae": _quarter_mae(2),
        "q3_mae": _quarter_mae(3),
        "q4_mae": _quarter_mae(4),
        "q1_mean_error": _quarter_mean_error(1),
        "q2_mean_error": _quarter_mean_error(2),
        "q3_mean_error": _quarter_mean_error(3),
        "q4_mean_error": _quarter_mean_error(4),
        "top_miss_quarters": top_miss_quarters,
    }


def _count_material_difference_quarters_vs_official(
    pred_series: pd.Series,
    official_series: pd.Series,
    *,
    threshold: float = 0.025,
) -> int:
    pred_num = pd.to_numeric(pred_series, errors="coerce")
    official_num = pd.to_numeric(official_series, errors="coerce")
    valid = pred_num.notna() & official_num.notna()
    if not valid.any():
        return 0
    return int(pred_num[valid].sub(official_num[valid]).abs().gt(float(threshold)).sum())


def _gpre_join_guard_failures(failures: Iterable[str]) -> str:
    parts = [str(part or "").strip() for part in failures if str(part or "").strip()]
    return "; ".join(parts)


def _gpre_incremental_value_status(
    avg_abs_diff_vs_official: Any,
    diff_quarters_gt_0_02_vs_official: Any,
) -> str:
    avg_abs_diff_num = pd.to_numeric(avg_abs_diff_vs_official, errors="coerce")
    diff_quarters_num = pd.to_numeric(diff_quarters_gt_0_02_vs_official, errors="coerce")
    if pd.notna(avg_abs_diff_num) and pd.notna(diff_quarters_num):
        if float(avg_abs_diff_num) >= 0.03 and float(diff_quarters_num) >= 6.0:
            return "high"
        if float(avg_abs_diff_num) >= 0.020 and float(diff_quarters_num) >= 4.0:
            return "moderate"
    return "low"


def _gpre_selection_guard_failures(
    row: pd.Series,
    *,
    clean_best: float,
    q1_best: float,
    clean_mae_slack: float,
    q1_mae_slack: float,
    q1_bias_limit: float,
) -> List[str]:
    if not bool(row.get("eligible_official")):
        return ["not_eligible_official"]
    failures: List[str] = []
    clean_mae = pd.to_numeric(row.get("clean_mae"), errors="coerce")
    q1_mae = pd.to_numeric(row.get("q1_mae"), errors="coerce")
    q1_mean_error = pd.to_numeric(row.get("q1_mean_error"), errors="coerce")
    if np.isfinite(clean_best):
        if pd.isna(clean_mae) or float(clean_mae) > float(clean_best) + float(clean_mae_slack):
            failures.append("clean_mae_exceeded_best_window_tolerance")
    if np.isfinite(q1_best):
        if pd.isna(q1_mae) or float(q1_mae) > float(q1_best) + float(q1_mae_slack):
            failures.append("q1_mae_exceeded_best_window_tolerance")
    if pd.isna(q1_mean_error) or abs(float(q1_mean_error)) > float(q1_bias_limit):
        failures.append("q1_mean_error_exceeded_bias_limit")
    return failures


def _annotate_gpre_selection_guardrails(
    leaderboard_df: pd.DataFrame,
    *,
    clean_mae_slack: float,
    q1_mae_slack: float,
    q1_bias_limit: float,
) -> pd.DataFrame:
    if leaderboard_df is None or leaderboard_df.empty:
        return leaderboard_df.copy() if isinstance(leaderboard_df, pd.DataFrame) else pd.DataFrame()
    annotated = leaderboard_df.copy()
    eligible = annotated[annotated["eligible_official"] == True].copy()
    clean_vals = pd.to_numeric(eligible["clean_mae"], errors="coerce").dropna()
    q1_vals = pd.to_numeric(eligible["q1_mae"], errors="coerce").dropna()
    clean_best = float(clean_vals.min()) if not clean_vals.empty else float("inf")
    q1_best = float(q1_vals.min()) if not q1_vals.empty else float("inf")
    selection_pass: List[bool] = []
    selection_reason: List[str] = []
    selection_failures: List[str] = []
    for _, row in annotated.iterrows():
        failures = _gpre_selection_guard_failures(
            row,
            clean_best=clean_best,
            q1_best=q1_best,
            clean_mae_slack=clean_mae_slack,
            q1_mae_slack=q1_mae_slack,
            q1_bias_limit=q1_bias_limit,
        )
        passed = bool(row.get("eligible_official")) and not failures
        selection_pass.append(bool(passed))
        selection_reason.append("passed_guardrails" if passed else (failures[0] if failures else "not_eligible_official"))
        selection_failures.append(_gpre_join_guard_failures(failures))
    annotated["selection_guard_pass"] = selection_pass
    annotated["selection_guard_reason"] = selection_reason
    annotated["selection_guard_failures"] = selection_failures
    return annotated


def _rank_gpre_proxy_models(
    leaderboard_df: pd.DataFrame,
    *,
    family_preference: Optional[Dict[str, int]] = None,
    require_selection_pass: bool = False,
) -> pd.DataFrame:
    if leaderboard_df is None or leaderboard_df.empty:
        return pd.DataFrame()
    ranked = leaderboard_df.copy()
    ranked = ranked[ranked["eligible_official"] == True].copy()
    if require_selection_pass and "selection_guard_pass" in ranked.columns:
        ranked = ranked[ranked["selection_guard_pass"].astype(bool)].copy()
    if ranked.empty:
        return ranked
    family_preference = family_preference or {}
    ranked["family_pref"] = ranked["family"].map(family_preference).fillna(99)
    return ranked.sort_values(
        ["hybrid_score", "underlying_mae", "clean_mae", "family_pref", "model_key"],
        na_position="last",
    )


def _gpre_promotion_guard_failures(
    row: pd.Series,
    *,
    incumbent_row: pd.Series,
    new_candidate_keys: set[str],
) -> List[str]:
    model_key = str(row.get("model_key") or "")
    incumbent_key = str(incumbent_row.get("model_key") or "")
    if model_key == incumbent_key:
        return []
    if model_key not in set(new_candidate_keys or set()):
        return ["not_new_candidate"]
    if not bool(row.get("eligible_official")):
        return ["not_eligible_official"]
    failures: List[str] = []
    if not bool(row.get("preview_supported")):
        failures.append("preview_support_incomplete")
    preview_quality_status = str(row.get("live_preview_quality_status") or "").strip().lower()
    if preview_quality_status not in {"close", "acceptable"}:
        failures.append("live_preview_quality_not_faithful_enough")
    avg_abs_diff = pd.to_numeric(row.get("avg_abs_diff_vs_official"), errors="coerce")
    if pd.isna(avg_abs_diff) or float(avg_abs_diff) < 0.020:
        failures.append("incremental_distance_vs_official_too_low")
    diff_gt_0_02 = pd.to_numeric(row.get("diff_quarters_gt_0_02_vs_official"), errors="coerce")
    if pd.isna(diff_gt_0_02) or float(diff_gt_0_02) < 4.0:
        failures.append("too_few_material_diff_quarters_vs_official")
    incumbent_clean = pd.to_numeric(incumbent_row.get("clean_mae"), errors="coerce")
    candidate_clean = pd.to_numeric(row.get("clean_mae"), errors="coerce")
    if (
        pd.isna(candidate_clean)
        or pd.isna(incumbent_clean)
        or float(candidate_clean) > float(incumbent_clean) + 0.005
    ):
        failures.append("clean_mae_exceeded_incumbent_tolerance")
    incumbent_q1 = pd.to_numeric(incumbent_row.get("q1_mae"), errors="coerce")
    candidate_q1 = pd.to_numeric(row.get("q1_mae"), errors="coerce")
    if pd.isna(candidate_q1) or pd.isna(incumbent_q1) or float(candidate_q1) > float(incumbent_q1) + 0.020:
        failures.append("q1_mae_exceeded_incumbent_tolerance")
    mean_error_num = pd.to_numeric(row.get("full_mean_error"), errors="coerce")
    if pd.isna(mean_error_num):
        mean_error_num = pd.to_numeric(row.get("test_mean_error"), errors="coerce")
    if pd.isna(mean_error_num) or abs(float(mean_error_num)) > 0.050:
        failures.append("mean_error_exceeded_tolerance")
    candidate_hybrid = pd.to_numeric(row.get("hybrid_score"), errors="coerce")
    incumbent_hybrid = pd.to_numeric(incumbent_row.get("hybrid_score"), errors="coerce")
    candidate_underlying = pd.to_numeric(row.get("underlying_mae"), errors="coerce")
    incumbent_underlying = pd.to_numeric(incumbent_row.get("underlying_mae"), errors="coerce")
    clear_hybrid_win = (
        pd.notna(candidate_hybrid)
        and pd.notna(incumbent_hybrid)
        and float(candidate_hybrid) <= float(incumbent_hybrid) - 0.010
    )
    tie_break_win = (
        pd.notna(candidate_hybrid)
        and pd.notna(incumbent_hybrid)
        and pd.notna(candidate_underlying)
        and pd.notna(incumbent_underlying)
        and float(candidate_hybrid) <= float(incumbent_hybrid) - 0.005
        and float(candidate_underlying) <= float(incumbent_underlying) - 0.010
    )
    if not (clear_hybrid_win or tie_break_win):
        failures.append("incumbent_improvement_threshold_not_met")
    challenger_hard_mae = pd.to_numeric(row.get("hard_quarter_mae"), errors="coerce")
    incumbent_hard_mae = pd.to_numeric(incumbent_row.get("hard_quarter_mae"), errors="coerce")
    if (
        pd.notna(challenger_hard_mae)
        and pd.notna(incumbent_hard_mae)
        and float(challenger_hard_mae) > float(incumbent_hard_mae) + 0.015
    ):
        failures.append("hard_quarter_mae_materially_worse_than_incumbent")
    return failures


def _annotate_gpre_promotion_guardrails(
    leaderboard_df: pd.DataFrame,
    *,
    incumbent_baseline_model_key: str,
    expanded_best_candidate_model_key: str,
    new_candidate_keys: set[str],
) -> Tuple[pd.DataFrame, str, str]:
    if leaderboard_df is None or leaderboard_df.empty:
        return pd.DataFrame(), str(incumbent_baseline_model_key or "process_current_quarter_avg"), "incumbent_retained_no_leaderboard"
    annotated = leaderboard_df.copy()
    annotated["incremental_value_status"] = [
        _gpre_incremental_value_status(
            rec.get("avg_abs_diff_vs_official"),
            rec.get("diff_quarters_gt_0_02_vs_official"),
        )
        for rec in annotated.to_dict("records")
    ]
    promotion_pass: List[bool] = []
    promotion_reason: List[str] = []
    promotion_failures: List[str] = []
    incumbent_sub = annotated[annotated["model_key"].astype(str) == str(incumbent_baseline_model_key or "")].copy()
    if incumbent_sub.empty:
        annotated["promotion_guard_pass"] = False
        annotated["promotion_guard_reason"] = "incumbent_baseline_missing"
        annotated["promotion_guard_failures"] = "incumbent_baseline_missing"
        annotated["expanded_best_candidate"] = annotated["model_key"].astype(str) == str(expanded_best_candidate_model_key or "")
        annotated["production_winner"] = annotated["expanded_best_candidate"].astype(bool)
        winner_key = str(expanded_best_candidate_model_key or "process_current_quarter_avg")
        return annotated, winner_key, "promoted_no_incumbent_baseline_available"
    incumbent_row = incumbent_sub.iloc[0]
    for _, row in annotated.iterrows():
        model_key = str(row.get("model_key") or "")
        if model_key == str(incumbent_baseline_model_key or ""):
            promotion_pass.append(True)
            promotion_reason.append("incumbent_baseline")
            promotion_failures.append("")
            continue
        failures = _gpre_promotion_guard_failures(
            row,
            incumbent_row=incumbent_row,
            new_candidate_keys=new_candidate_keys,
        )
        passed = not failures
        promotion_pass.append(bool(passed))
        promotion_reason.append("passed_promotion_guardrails" if passed else failures[0])
        promotion_failures.append(_gpre_join_guard_failures(failures))
    annotated["promotion_guard_pass"] = promotion_pass
    annotated["promotion_guard_reason"] = promotion_reason
    annotated["promotion_guard_failures"] = promotion_failures
    annotated["expanded_best_candidate"] = annotated["model_key"].astype(str) == str(expanded_best_candidate_model_key or "")
    production_winner_key = str(incumbent_baseline_model_key or "process_current_quarter_avg")
    production_reason = "expanded_best_is_incumbent_baseline"
    expanded_sub = annotated[annotated["expanded_best_candidate"].astype(bool)].copy()
    if not expanded_sub.empty:
        expanded_row = expanded_sub.iloc[0]
        expanded_key = str(expanded_row.get("model_key") or "")
        if expanded_key == str(incumbent_baseline_model_key or ""):
            production_winner_key = str(incumbent_baseline_model_key or expanded_key)
            production_reason = "expanded_best_is_incumbent_baseline"
        elif bool(expanded_row.get("promotion_guard_pass")):
            production_winner_key = expanded_key
            production_reason = "promoted_over_incumbent_baseline"
        else:
            production_winner_key = str(incumbent_baseline_model_key or production_winner_key)
            production_reason = str(expanded_row.get("promotion_guard_reason") or "incumbent_retained_promotion_guard_failed")
    annotated["production_winner"] = annotated["model_key"].astype(str) == str(production_winner_key or "")
    return annotated, production_winner_key, production_reason


def _gpre_guard_reason_human(reason: Any) -> str:
    reason_txt = str(reason or "").strip()
    return {
        "passed_guardrails": "passed selection guardrails",
        "clean_mae_exceeded_best_window_tolerance": "clean_mae exceeded best-window tolerance",
        "q1_mae_exceeded_best_window_tolerance": "q1_mae exceeded best-window tolerance",
        "q1_mean_error_exceeded_bias_limit": "q1 mean error exceeded the bias limit",
        "not_eligible_official": "it was not fully eligible",
        "incumbent_baseline": "incumbent baseline",
        "passed_promotion_guardrails": "passed promotion guardrails",
        "preview_support_incomplete": "preview support was incomplete",
        "live_preview_quality_not_faithful_enough": "live preview was not faithful enough to the full model",
        "incremental_distance_vs_official_too_low": "incremental distance vs official was too low",
        "too_few_material_diff_quarters_vs_official": "too few quarters differed materially from the official row",
        "clean_mae_exceeded_incumbent_tolerance": "clean_mae exceeded incumbent tolerance",
        "q1_mae_exceeded_incumbent_tolerance": "q1_mae exceeded incumbent tolerance",
        "mean_error_exceeded_tolerance": "mean error exceeded tolerance",
        "incumbent_improvement_threshold_not_met": "it did not clear the incumbent improvement threshold",
        "hard_quarter_mae_materially_worse_than_incumbent": "hard-quarter MAE was materially worse than the incumbent",
        "not_new_candidate": "it was not a new candidate",
        "expanded_best_is_incumbent_baseline": "expanded-pass best was already the incumbent baseline",
        "promoted_over_incumbent_baseline": "promoted over incumbent baseline",
        "incumbent_baseline_missing": "incumbent baseline row was missing",
        "promoted_no_incumbent_baseline_available": "promoted because no incumbent baseline row was available",
    }.get(reason_txt, reason_txt.replace("_", " "))


def _gpre_guard_failures_human(failures_in: Any) -> str:
    parts = [str(part or "").strip() for part in str(failures_in or "").split(";") if str(part or "").strip()]
    if not parts:
        return "n/a"
    return "; ".join(_gpre_guard_reason_human(part) for part in parts)


def _gpre_selection_vs_promotion_story(
    *,
    incumbent_baseline_model_key: str,
    expanded_best_row: Dict[str, Any],
    production_winner_key: str,
) -> Tuple[str, str]:
    incumbent_key = str(incumbent_baseline_model_key or "").strip()
    expanded_key = str((expanded_best_row or {}).get("model_key") or "").strip()
    winner_key = str(production_winner_key or incumbent_key or "").strip()
    if not expanded_key:
        return (
            f"{winner_key or incumbent_key or 'No model'} stayed production winner because no expanded-pass challenger was identified.",
            "Selection vs promotion could not be compared because no expanded-pass best candidate was available.",
        )
    selection_pass = bool((expanded_best_row or {}).get("selection_guard_pass"))
    promotion_pass = bool((expanded_best_row or {}).get("promotion_guard_pass"))
    selection_reason = _gpre_guard_reason_human((expanded_best_row or {}).get("selection_guard_reason"))
    promotion_reason = _gpre_guard_reason_human((expanded_best_row or {}).get("promotion_guard_reason"))
    selection_failures = _gpre_guard_failures_human((expanded_best_row or {}).get("selection_guard_failures"))
    promotion_failures = _gpre_guard_failures_human((expanded_best_row or {}).get("promotion_guard_failures"))
    if expanded_key == incumbent_key:
        return (
            f"{incumbent_key} stayed production winner because it was already the incumbent baseline and also the expanded-pass best candidate.",
            "Selection and promotion agreed: the incumbent baseline already ranked first in the expanded pass.",
        )
    if winner_key == expanded_key and (not selection_pass) and promotion_pass:
        return (
            f"{expanded_key} became production winner even though it failed the broad selection guardrails, because it still passed the direct promotion test against {incumbent_key}.",
            f"Selection and promotion disagreed. Selection flagged {selection_failures}, but promotion still passed because {promotion_reason}.",
        )
    if winner_key == expanded_key and selection_pass and promotion_pass:
        return (
            f"{expanded_key} became production winner after passing both selection and promotion guardrails over {incumbent_key}.",
            "Selection and promotion agreed: the challenger passed both screens and was promoted.",
        )
    if winner_key == incumbent_key and selection_pass and (not promotion_pass):
        return (
            f"{incumbent_key} stayed production winner because {expanded_key} passed broad selection but failed the direct promotion test.",
            f"Selection and promotion disagreed. The challenger won the expanded pass, but promotion blocked it because {promotion_failures}.",
        )
    if winner_key == incumbent_key and (not selection_pass) and (not promotion_pass):
        return (
            f"{incumbent_key} stayed production winner because {expanded_key} failed both selection and promotion guardrails.",
            f"Selection and promotion agreed on retaining the incumbent. Selection failed because {selection_failures}; promotion failed because {promotion_failures}.",
        )
    return (
        f"{winner_key or incumbent_key or expanded_key} stayed production winner after comparing {expanded_key} against {incumbent_key}.",
        f"Selection status: {selection_reason}. Promotion status: {promotion_reason}.",
    )


def _gpre_hedge_style_label(style_key: Any) -> str:
    key_txt = str(style_key or "").strip()
    return {
        "spot_simple": "Spot simple",
        "quarter_open_lock_25": "Quarter-open lock 25%",
        "quarter_open_lock_50": "Quarter-open lock 50%",
        "quarter_open_lock_75": "Quarter-open lock 75%",
        "front_loaded_layering": "Front-loaded layering",
        "equal_monthly_layering": "Equal monthly layering",
        "quarter_open_plus_current_blend": "Quarter-open + current blend",
        "good_setup_realization_drag": "Good setup + realization drag",
        "ops_disruption_overlay": "Ops disruption overlay",
    }.get(key_txt, key_txt.replace("_", " ").title())


def _gpre_hedge_style_family_label(family_key: Any) -> str:
    key_txt = str(family_key or "").strip()
    return {
        "spot_like": "Spot-like",
        "quarter_open_locking": "Quarter-open locking",
        "layered_setup": "Layered setup",
        "realization_drag": "Realization drag",
    }.get(key_txt, key_txt.replace("_", " ").title())


def _gpre_hedge_style_blend_value(
    anchor_value: Any,
    current_value: Any,
    *,
    anchor_weight: float,
) -> Optional[float]:
    anchor_num = pd.to_numeric(anchor_value, errors="coerce")
    current_num = pd.to_numeric(current_value, errors="coerce")
    if pd.notna(anchor_num) and pd.notna(current_num):
        current_weight = 1.0 - float(anchor_weight)
        return (float(anchor_num) * float(anchor_weight)) + (float(current_num) * current_weight)
    if pd.notna(anchor_num):
        return float(anchor_num)
    if pd.notna(current_num):
        return float(current_num)
    return None


def _gpre_hedge_style_fit_note(
    *,
    family_key: str,
    weak_fit: bool,
    hard_quarter_flag: bool,
) -> str:
    if weak_fit and hard_quarter_flag:
        return "Weak fit; likely dominated by realization/ops misses"
    if weak_fit:
        return "Weak fit; no simple style explained well"
    return {
        "spot_like": "Best explained by spot-like behavior",
        "quarter_open_locking": "Best explained by quarter-open locking",
        "layered_setup": "Best explained by layered setup",
        "realization_drag": "Best explained by realization/ops drag",
    }.get(str(family_key or "").strip(), "Best explained by a simple hedge-style analogue")


def _build_gpre_hedge_style_study(quarterly_df: pd.DataFrame) -> Dict[str, Any]:
    empty_result = {
        "target_col": "reported_consolidated_crush_margin_usd_per_gal",
        "target_label": "Reported consolidated crush margin ($/gal)",
        "target_definition": "Reported consolidated crush margin converted to $/gal on the same quarterly gallons basis already used in the GPRE model.",
        "backtest_window_quarters": [],
        "backtest_window_display": "",
        "candidate_specs": [],
        "candidate_leaderboard_df": pd.DataFrame(),
        "family_summary_df": pd.DataFrame(),
        "quarter_fit_df": pd.DataFrame(),
        "best_overall_style_key": "",
        "best_overall_style_label": "",
        "best_overall_style_family": "",
        "best_overall_style_family_label": "",
        "best_style_vs_family_explanation": "",
        "diagnostic_only_note": "Diagnostic only; does not change official row, fitted row, or winner selection.",
        "weak_fit_quarters": [],
        "interpretation_lines": [],
    }
    if quarterly_df is None or not isinstance(quarterly_df, pd.DataFrame) or quarterly_df.empty:
        return empty_result

    study_df = quarterly_df.copy()

    def _series_num(col_name: str) -> pd.Series:
        if col_name in study_df.columns:
            return pd.to_numeric(study_df[col_name], errors="coerce")
        return pd.Series(np.nan, index=study_df.index, dtype=float)

    quarter_series = pd.to_datetime(study_df.get("quarter"), errors="coerce")
    if "quarter_label" in study_df.columns:
        quarter_labels = study_df["quarter_label"].astype(str)
    else:
        quarter_labels = quarter_series.apply(lambda ts: _quarter_label(ts.date()) if pd.notna(ts) else "")
    target_series = _series_num("reported_consolidated_crush_margin_usd_per_gal")
    anchor_series = _series_num("process_quarter_open_anchor_usd_per_gal")
    current_series = _series_num("process_proxy_current_quarter_avg_usd_per_gal")
    hard_flag_series = study_df.get("hard_quarter_flag")
    if isinstance(hard_flag_series, pd.Series):
        hard_flag_series = hard_flag_series.fillna(False).astype(bool)
    else:
        hard_flag_series = pd.Series(False, index=study_df.index, dtype=bool)
    hard_reason_series = study_df.get("hard_quarter_reason")
    if not isinstance(hard_reason_series, pd.Series):
        hard_reason_series = pd.Series("", index=study_df.index, dtype=object)
    else:
        hard_reason_series = hard_reason_series.fillna("").astype(str)

    candidate_specs: List[Dict[str, Any]] = [
        {"style_key": "spot_simple", "style_label": _gpre_hedge_style_label("spot_simple"), "family": "spot_like", "family_label": _gpre_hedge_style_family_label("spot_like"), "source_col": "official_simple_proxy_usd_per_gal"},
        {"style_key": "quarter_open_lock_25", "style_label": _gpre_hedge_style_label("quarter_open_lock_25"), "family": "quarter_open_locking", "family_label": _gpre_hedge_style_family_label("quarter_open_locking"), "source_col": "hedge_style_quarter_open_lock_25_usd_per_gal"},
        {"style_key": "quarter_open_lock_50", "style_label": _gpre_hedge_style_label("quarter_open_lock_50"), "family": "quarter_open_locking", "family_label": _gpre_hedge_style_family_label("quarter_open_locking"), "source_col": "hedge_style_quarter_open_lock_50_usd_per_gal"},
        {"style_key": "quarter_open_lock_75", "style_label": _gpre_hedge_style_label("quarter_open_lock_75"), "family": "quarter_open_locking", "family_label": _gpre_hedge_style_family_label("quarter_open_locking"), "source_col": "hedge_style_quarter_open_lock_75_usd_per_gal"},
        {"style_key": "front_loaded_layering", "style_label": _gpre_hedge_style_label("front_loaded_layering"), "family": "layered_setup", "family_label": _gpre_hedge_style_family_label("layered_setup"), "source_col": "process_proxy_front_loaded_usd_per_gal"},
        {"style_key": "equal_monthly_layering", "style_label": _gpre_hedge_style_label("equal_monthly_layering"), "family": "layered_setup", "family_label": _gpre_hedge_style_family_label("layered_setup"), "source_col": "process_proxy_current_quarter_avg_usd_per_gal"},
        {"style_key": "quarter_open_plus_current_blend", "style_label": _gpre_hedge_style_label("quarter_open_plus_current_blend"), "family": "layered_setup", "family_label": _gpre_hedge_style_family_label("layered_setup"), "source_col": "process_quarter_open_blend_usd_per_gal"},
        {"style_key": "good_setup_realization_drag", "style_label": _gpre_hedge_style_label("good_setup_realization_drag"), "family": "realization_drag", "family_label": _gpre_hedge_style_family_label("realization_drag"), "source_col": "process_quarter_open_blend_hedge_realization_usd_per_gal"},
        {"style_key": "ops_disruption_overlay", "style_label": _gpre_hedge_style_label("ops_disruption_overlay"), "family": "realization_drag", "family_label": _gpre_hedge_style_family_label("realization_drag"), "source_col": "process_quarter_open_blend_ops_penalty_usd_per_gal"},
    ]

    study_df["hedge_style_quarter_open_lock_25_usd_per_gal"] = [
        _gpre_hedge_style_blend_value(anchor_val, current_val, anchor_weight=0.25)
        for anchor_val, current_val in zip(anchor_series.tolist(), current_series.tolist())
    ]
    study_df["hedge_style_quarter_open_lock_50_usd_per_gal"] = [
        _gpre_hedge_style_blend_value(anchor_val, current_val, anchor_weight=0.50)
        for anchor_val, current_val in zip(anchor_series.tolist(), current_series.tolist())
    ]
    study_df["hedge_style_quarter_open_lock_75_usd_per_gal"] = [
        _gpre_hedge_style_blend_value(anchor_val, current_val, anchor_weight=0.75)
        for anchor_val, current_val in zip(anchor_series.tolist(), current_series.tolist())
    ]

    candidate_eval_records: List[Dict[str, Any]] = []
    quarter_fit_records: List[Dict[str, Any]] = []
    for idx, quarter_ts in quarter_series.items():
        target_num = pd.to_numeric(target_series.iloc[idx], errors="coerce")
        if pd.isna(target_num):
            continue
        quarter_candidate_rows: List[Dict[str, Any]] = []
        quarter_label_txt = str(quarter_labels.iloc[idx] or "")
        for rank, spec in enumerate(candidate_specs):
            pred_num = pd.to_numeric(study_df.at[idx, str(spec["source_col"])], errors="coerce")
            if pd.isna(pred_num):
                continue
            error_num = float(pred_num) - float(target_num)
            abs_error_num = abs(error_num)
            row = {
                "quarter": (quarter_ts.date() if pd.notna(quarter_ts) else pd.NaT),
                "quarter_label": quarter_label_txt,
                "style_key": str(spec["style_key"]),
                "style_label": str(spec["style_label"]),
                "family": str(spec["family"]),
                "family_label": str(spec["family_label"]),
                "candidate_rank": rank,
                "pred_value_usd_per_gal": float(pred_num),
                "target_value_usd_per_gal": float(target_num),
                "error_usd_per_gal": error_num,
                "abs_error_usd_per_gal": abs_error_num,
            }
            candidate_eval_records.append(row)
            quarter_candidate_rows.append(row)
        if not quarter_candidate_rows:
            continue
        best_row = min(
            quarter_candidate_rows,
            key=lambda rec: (float(rec["abs_error_usd_per_gal"]), int(rec["candidate_rank"])),
        )
        hard_flag = bool(hard_flag_series.iloc[idx])
        hard_reason = str(hard_reason_series.iloc[idx] or "").strip()
        weak_fit = bool(float(best_row["abs_error_usd_per_gal"]) > 0.05)
        quarter_fit_records.append(
            {
                "quarter": best_row["quarter"],
                "quarter_label": quarter_label_txt,
                "target_value_usd_per_gal": float(target_num),
                "best_fit_style_key": str(best_row["style_key"]),
                "best_fit_style_label": str(best_row["style_label"]),
                "best_fit_family": str(best_row["family"]),
                "best_fit_family_label": str(best_row["family_label"]),
                "best_fit_value_usd_per_gal": float(best_row["pred_value_usd_per_gal"]),
                "best_fit_error_usd_per_gal": float(best_row["error_usd_per_gal"]),
                "best_fit_abs_error_usd_per_gal": float(best_row["abs_error_usd_per_gal"]),
                "hard_quarter_flag": hard_flag,
                "hard_quarter_reason": hard_reason,
                "fit_note": _gpre_hedge_style_fit_note(
                    family_key=str(best_row["family"]),
                    weak_fit=weak_fit,
                    hard_quarter_flag=hard_flag,
                ),
                "weak_fit_flag": weak_fit,
                "weak_fit_display": "Yes" if weak_fit else "No",
            }
        )

    quarter_fit_df = pd.DataFrame(quarter_fit_records)
    candidate_eval_df = pd.DataFrame(candidate_eval_records)
    if candidate_eval_df.empty:
        return {**empty_result, "candidate_specs": candidate_specs}

    best_fit_counts = (
        quarter_fit_df["best_fit_style_key"].value_counts().to_dict()
        if not quarter_fit_df.empty and "best_fit_style_key" in quarter_fit_df.columns
        else {}
    )
    leaderboard_rows: List[Dict[str, Any]] = []
    for rank, spec in enumerate(candidate_specs):
        style_key = str(spec["style_key"])
        sub = candidate_eval_df[candidate_eval_df["style_key"].astype(str) == style_key].copy()
        if sub.empty:
            mae_val = float("nan")
            mean_error_val = float("nan")
            sign_hit_val = float("nan")
            usable_count = 0
        else:
            mae_val = float(pd.to_numeric(sub["abs_error_usd_per_gal"], errors="coerce").mean())
            mean_error_val = float(pd.to_numeric(sub["error_usd_per_gal"], errors="coerce").mean())
            sign_hit_val = float(
                (
                    np.sign(pd.to_numeric(sub["pred_value_usd_per_gal"], errors="coerce"))
                    == np.sign(pd.to_numeric(sub["target_value_usd_per_gal"], errors="coerce"))
                ).mean()
            )
            usable_count = int(len(sub))
        leaderboard_rows.append(
            {
                "style_key": style_key,
                "style_label": str(spec["style_label"]),
                "family": str(spec["family"]),
                "family_label": str(spec["family_label"]),
                "candidate_rank": rank,
                "mae": mae_val,
                "mean_error": mean_error_val,
                "sign_hit_rate": sign_hit_val,
                "best_fit_quarter_count": int(best_fit_counts.get(style_key, 0)),
                "usable_quarter_count": usable_count,
            }
        )
    candidate_leaderboard_df = pd.DataFrame(leaderboard_rows).sort_values(
        ["mae", "candidate_rank"],
        na_position="last",
    ).reset_index(drop=True)

    family_summary_df = pd.DataFrame()
    if not candidate_leaderboard_df.empty:
        family_summary_df = (
            candidate_leaderboard_df.groupby(["family", "family_label"], dropna=False)
            .agg(
                family_average_mae=("mae", "mean"),
                family_average_mean_error=("mean_error", "mean"),
                family_average_sign_hit_rate=("sign_hit_rate", "mean"),
                best_fit_quarter_count=("best_fit_quarter_count", "sum"),
                style_count=("style_key", "count"),
            )
            .reset_index()
            .sort_values(["family_average_mae", "family"], na_position="last")
            .reset_index(drop=True)
        )

    best_style_key = ""
    best_style_label = ""
    best_family_key = ""
    best_family_label = ""
    if not candidate_leaderboard_df.empty:
        best_style_row = candidate_leaderboard_df.iloc[0].to_dict()
        best_style_key = str(best_style_row.get("style_key") or "")
        best_style_label = str(best_style_row.get("style_label") or "")
    if not family_summary_df.empty:
        best_family_row = family_summary_df.iloc[0].to_dict()
        best_family_key = str(best_family_row.get("family") or "")
        best_family_label = str(best_family_row.get("family_label") or "")

    weak_fit_quarters = (
        quarter_fit_df.loc[quarter_fit_df["weak_fit_flag"].astype(bool), "quarter_label"].astype(str).tolist()
        if not quarter_fit_df.empty and "weak_fit_flag" in quarter_fit_df.columns
        else []
    )
    family_best_quarters = (
        quarter_fit_df.groupby("best_fit_family_label", dropna=False)["quarter_label"].apply(list).to_dict()
        if not quarter_fit_df.empty
        else {}
    )
    interpretation_lines: List[str] = []
    if family_best_quarters.get("Spot-like"):
        interpretation_lines.append(
            f"Quarters best explained by spot-like behavior: {', '.join(family_best_quarters['Spot-like'][:4])}."
        )
    if family_best_quarters.get("Quarter-open locking"):
        interpretation_lines.append(
            f"Quarters best explained by quarter-open locking: {', '.join(family_best_quarters['Quarter-open locking'][:4])}."
        )
    if family_best_quarters.get("Realization drag"):
        interpretation_lines.append(
            f"Quarters best explained by realization/ops drag: {', '.join(family_best_quarters['Realization drag'][:4])}."
        )
    if weak_fit_quarters:
        interpretation_lines.append(
            f"Quarters not well explained by any simple hedge style: {', '.join(weak_fit_quarters[:4])}."
        )
    elif family_best_quarters.get("Layered setup"):
        interpretation_lines.append(
            f"Quarters best explained by layered setup: {', '.join(family_best_quarters['Layered setup'][:4])}."
        )
    if not interpretation_lines and best_style_label:
        interpretation_lines.append(
            f"Best overall diagnostic style was {best_style_label} across {int(len(quarter_fit_df))} usable quarters."
        )

    backtest_window_quarters = quarter_fit_df["quarter_label"].astype(str).tolist() if not quarter_fit_df.empty else []
    backtest_window_display = "n/a"
    if not quarter_fit_df.empty:
        window_df = quarter_fit_df.copy()
        window_df["_quarter_sort"] = pd.to_datetime(window_df.get("quarter"), errors="coerce")
        if window_df["_quarter_sort"].notna().any():
            window_df = window_df.sort_values(["_quarter_sort", "quarter_label"], na_position="last")
        ordered_labels = [
            str(item or "").strip()
            for item in window_df.get("quarter_label", pd.Series(dtype=object)).tolist()
            if str(item or "").strip()
        ]
        if len(ordered_labels) >= 2:
            backtest_window_display = f"{ordered_labels[0]} to {ordered_labels[-1]}"
        elif ordered_labels:
            backtest_window_display = ordered_labels[0]
    best_style_vs_family_explanation = (
        "Best overall style = the single lowest-MAE candidate style. "
        "Best overall family = the family bucket with the lowest average MAE across its member styles."
    )
    diagnostic_only_note = "Diagnostic only; does not change official row, fitted row, or winner selection."
    return {
        "target_col": "reported_consolidated_crush_margin_usd_per_gal",
        "target_label": "Reported consolidated crush margin ($/gal)",
        "target_definition": "Reported consolidated crush margin converted to $/gal on the same quarterly gallons basis already used in the GPRE model.",
        "backtest_window_quarters": backtest_window_quarters,
        "backtest_window_display": backtest_window_display,
        "candidate_specs": candidate_specs,
        "candidate_leaderboard_df": candidate_leaderboard_df,
        "family_summary_df": family_summary_df,
        "quarter_fit_df": quarter_fit_df.sort_values("quarter", ascending=False, na_position="last").reset_index(drop=True),
        "best_overall_style_key": best_style_key,
        "best_overall_style_label": best_style_label,
        "best_overall_style_family": best_family_key,
        "best_overall_style_family_label": best_family_label,
        "best_style_vs_family_explanation": best_style_vs_family_explanation,
        "diagnostic_only_note": diagnostic_only_note,
        "weak_fit_quarters": weak_fit_quarters,
        "interpretation_lines": interpretation_lines[:4],
    }


def _select_gpre_proxy_model_from_leaderboard(
    leaderboard_df: pd.DataFrame,
    *,
    family_preference: Optional[Dict[str, int]] = None,
    clean_mae_slack: float = 0.010,
    q1_mae_slack: float = 0.015,
    q1_bias_limit: float = 0.050,
) -> Tuple[str, str]:
    default_key = "process_current_quarter_avg"
    if leaderboard_df is None or leaderboard_df.empty:
        return default_key, "fallback_no_eligible_candidate"
    annotated = _annotate_gpre_selection_guardrails(
        leaderboard_df,
        clean_mae_slack=clean_mae_slack,
        q1_mae_slack=q1_mae_slack,
        q1_bias_limit=q1_bias_limit,
    )
    eligible = annotated[annotated["eligible_official"] == True].copy()
    if eligible.empty:
        return default_key, "fallback_no_eligible_candidate"
    guarded = _rank_gpre_proxy_models(
        annotated,
        family_preference=family_preference,
        require_selection_pass=True,
    )
    if guarded.empty:
        fallback = _rank_gpre_proxy_models(
            annotated,
            family_preference=family_preference,
            require_selection_pass=False,
        )
        return str(fallback.iloc[0]["model_key"] or default_key), "fallback_no_candidate_passed_guardrails"
    return str(guarded.iloc[0]["model_key"] or default_key), "passed_guardrails"


def _gpre_leaderboard_metric_pick(metrics_df: pd.DataFrame, model_key_in: str, split_in: str, field_in: str) -> float:
    if metrics_df is None or metrics_df.empty:
        return float("nan")
    sub = metrics_df[
        (metrics_df["model_key"].astype(str) == str(model_key_in or ""))
        & (metrics_df["split"].astype(str) == str(split_in or ""))
    ].copy()
    if sub.empty:
        return float("nan")
    return float(pd.to_numeric(sub.iloc[0].get(field_in), errors="coerce"))


def _gpre_preview_accuracy_for_model(
    quarterly_df: pd.DataFrame,
    *,
    model_key: str,
    pred_col: str,
) -> Dict[str, Any]:
    empty_phase = {
        "preview_abs_error_usd_per_gal": [],
        "preview_mae": None,
        "preview_max_error": None,
        "preview_bias": None,
        "preview_top_miss_quarters": "",
        "preview_test_count": 0,
    }
    default_out = {
        "preview_accuracy": {
            "prior": dict(empty_phase),
            "quarter_open": dict(empty_phase),
            "current": dict(empty_phase),
            "next": dict(empty_phase),
        },
        "live_preview_mae": None,
        "live_preview_max_error": None,
        "live_preview_bias": None,
        "live_preview_top_miss_quarters": "",
        "live_preview_quality_status": "not_faithful_enough",
        "live_preview_worst_phase": "",
    }
    if quarterly_df is None or quarterly_df.empty or pred_col not in quarterly_df.columns:
        return default_out
    quarter_series = pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date
    ordered_quarters = _sorted_quarters([qd for qd in quarter_series.tolist() if isinstance(qd, date)])
    if not ordered_quarters:
        return default_out

    def _map_num(col_name: str) -> Dict[date, float]:
        if col_name not in quarterly_df.columns:
            return {}
        return {
            qd: float(val)
            for qd, val in zip(quarter_series, pd.to_numeric(quarterly_df[col_name], errors="coerce"))
            if isinstance(qd, date) and pd.notna(val)
        }

    ethanol_map = _map_num("weighted_ethanol_benchmark_usd_per_gal")
    pred_map = _map_num(pred_col)
    bridge_current_map = _map_num("bridge_proxy_current_quarter_avg_usd_per_gal")
    bridge_front_map = _map_num("bridge_proxy_front_loaded_usd_per_gal")
    bridge_75_map = _map_num("bridge_proxy_current75_prev25_usd_per_gal")
    bridge_50_map = _map_num("bridge_proxy_current50_prev50_usd_per_gal")
    process_current_map = _map_num("process_proxy_current_quarter_avg_usd_per_gal")
    process_front_map = _map_num("process_proxy_front_loaded_usd_per_gal")
    process_75_map = _map_num("process_proxy_current75_prev25_usd_per_gal")
    process_50_map = _map_num("process_proxy_current50_prev50_usd_per_gal")
    process_anchor_map = _map_num("process_quarter_open_anchor_usd_per_gal")
    quarter_open_weight_map = _map_num("quarter_open_weight")
    current_weight_map = _map_num("current_weight")
    ops_penalty_map = _map_num("ops_penalty_usd_per_gal")
    total_exec_penalty_map = _map_num("ops_total_execution_penalty_usd_per_gal")
    utilization_penalty_map = _map_num("utilization_overlay_penalty_usd_per_gal")
    maintenance_delay_penalty_map = _map_num("maintenance_delay_penalty_usd_per_gal")
    inventory_drag_penalty_map = _map_num("inventory_timing_drag_penalty_usd_per_gal")
    realization_residual_penalty_map = _map_num("realization_residual_penalty_usd_per_gal")
    disclosed_share_map = _map_num("hedge_share_disclosed")
    pattern_share_map = _map_num("hedge_share_pattern")
    disturbed_flag_map = _map_num("disturbed_quarter_flag")
    hard_flag_map = _map_num("hard_quarter_flag")
    process_blend_map = _map_num("process_quarter_open_blend_usd_per_gal")
    process_blend_util_map = _map_num("process_quarter_open_blend_utilization_penalty_usd_per_gal")
    process_blend_maintenance_map = _map_num("process_quarter_open_blend_maintenance_delay_penalty_usd_per_gal")
    process_blend_inventory_map = _map_num("process_quarter_open_blend_inventory_timing_drag_usd_per_gal")
    process_blend_locked_setup_map = _map_num("process_quarter_open_blend_locked_setup_usd_per_gal")
    process_basis_blend_current40_front60_map = _map_num("process_basis_blend_current40_front60_usd_per_gal")
    process_basis_passthrough_beta35_map = _map_num("process_basis_passthrough_beta35_usd_per_gal")
    process_basis_passthrough_beta65_map = _map_num("process_basis_passthrough_beta65_usd_per_gal")
    process_quarter_open_current50_exec_penalty_map = _map_num("process_quarter_open_current50_exec_penalty_usd_per_gal")
    process_regime_basis_passthrough_map = _map_num("process_regime_basis_passthrough_usd_per_gal")
    process_two_stage_realization_residual_map = _map_num("process_two_stage_realization_residual_usd_per_gal")
    process_capacity_weighted_basis_strict_map = _map_num("process_capacity_weighted_basis_strict_usd_per_gal")
    process_inventory_gap_penalty_small_map = _map_num("process_inventory_gap_penalty_small_usd_per_gal")
    process_inventory_gap_penalty_medium_map = _map_num("process_inventory_gap_penalty_medium_usd_per_gal")
    process_utilization_regime_blend_map = _map_num("process_utilization_regime_blend_usd_per_gal")
    process_utilization_regime_residual_map = _map_num("process_utilization_regime_residual_usd_per_gal")
    process_exec_inventory_combo_medium_map = _map_num("process_exec_inventory_combo_medium_usd_per_gal")
    process_asymmetric_basis_passthrough_map = _map_num("process_asymmetric_basis_passthrough_usd_per_gal")
    process_market_process_ensemble_35_65_map = _map_num("process_market_process_ensemble_35_65_usd_per_gal")
    process_locked_share_asymmetric_passthrough_map = _map_num("process_locked_share_asymmetric_passthrough_usd_per_gal")
    process_prior_gap_carryover_small_map = _map_num("process_prior_gap_carryover_small_usd_per_gal")
    process_prior_disturbance_carryover_map = _map_num("process_prior_disturbance_carryover_usd_per_gal")
    process_residual_regime_locked_vs_disturbed_map = _map_num("process_residual_regime_locked_vs_disturbed_usd_per_gal")
    process_gated_incumbent_vs_residual_map = _map_num("process_gated_incumbent_vs_residual_usd_per_gal")
    process_front_ops_map = _map_num("process_front_loaded_ops_penalty_usd_per_gal")
    process_front_geo_map = _map_num("process_front_loaded_ethanol_geo_usd_per_gal")
    hedge_disclosed_bridge_current_map = _map_num("hedge_memo_disclosed_bridge_prior_current_usd_per_gal")
    hedge_disclosed_bridge_front_map = _map_num("hedge_memo_disclosed_bridge_prior_front_usd_per_gal")
    hedge_disclosed_process_current_map = _map_num("hedge_memo_disclosed_process_prior_current_usd_per_gal")
    hedge_disclosed_process_front_map = _map_num("hedge_memo_disclosed_process_prior_front_usd_per_gal")
    hedge_pattern_bridge_current_map = _map_num("hedge_memo_pattern_bridge_prior_current_usd_per_gal")
    hedge_pattern_bridge_front_map = _map_num("hedge_memo_pattern_bridge_prior_front_usd_per_gal")
    hedge_pattern_process_current_map = _map_num("hedge_memo_pattern_process_prior_current_usd_per_gal")
    hedge_pattern_process_front_map = _map_num("hedge_memo_pattern_process_prior_front_usd_per_gal")
    bid_adjusted_map = _map_num("bridge_proxy_bid_adjusted_offset_usd_per_gal")

    def _lead_map(map_in: Dict[date, float]) -> Dict[date, float]:
        return _shift_quarter_map(map_in, ordered_quarters, lag_quarters=-1)

    ethanol_next_map = _lead_map(ethanol_map)
    bridge_current_next_map = _lead_map(bridge_current_map)
    bridge_front_next_map = _lead_map(bridge_front_map)
    bridge_75_next_map = _lead_map(bridge_75_map)
    bridge_50_next_map = _lead_map(bridge_50_map)
    process_current_next_map = _lead_map(process_current_map)
    process_front_next_map = _lead_map(process_front_map)
    process_75_next_map = _lead_map(process_75_map)
    process_50_next_map = _lead_map(process_50_map)
    process_front_ops_next_map = _lead_map(process_front_ops_map)
    process_front_geo_next_map = _lead_map(process_front_geo_map)
    process_basis_blend_current40_front60_next_map = _lead_map(process_basis_blend_current40_front60_map)
    process_basis_passthrough_beta35_next_map = _lead_map(process_basis_passthrough_beta35_map)
    process_basis_passthrough_beta65_next_map = _lead_map(process_basis_passthrough_beta65_map)
    process_quarter_open_current50_exec_penalty_next_map = _lead_map(process_quarter_open_current50_exec_penalty_map)
    process_regime_basis_passthrough_next_map = _lead_map(process_regime_basis_passthrough_map)
    process_two_stage_realization_residual_next_map = _lead_map(process_two_stage_realization_residual_map)
    process_capacity_weighted_basis_strict_next_map = _lead_map(process_capacity_weighted_basis_strict_map)
    process_inventory_gap_penalty_small_next_map = _lead_map(process_inventory_gap_penalty_small_map)
    process_inventory_gap_penalty_medium_next_map = _lead_map(process_inventory_gap_penalty_medium_map)
    process_utilization_regime_blend_next_map = _lead_map(process_utilization_regime_blend_map)
    process_utilization_regime_residual_next_map = _lead_map(process_utilization_regime_residual_map)
    process_exec_inventory_combo_medium_next_map = _lead_map(process_exec_inventory_combo_medium_map)
    process_asymmetric_basis_passthrough_next_map = _lead_map(process_asymmetric_basis_passthrough_map)
    process_market_process_ensemble_35_65_next_map = _lead_map(process_market_process_ensemble_35_65_map)
    process_locked_share_asymmetric_passthrough_next_map = _lead_map(process_locked_share_asymmetric_passthrough_map)
    process_prior_gap_carryover_small_next_map = _lead_map(process_prior_gap_carryover_small_map)
    process_prior_disturbance_carryover_next_map = _lead_map(process_prior_disturbance_carryover_map)
    process_residual_regime_locked_vs_disturbed_next_map = _lead_map(process_residual_regime_locked_vs_disturbed_map)
    process_gated_incumbent_vs_residual_next_map = _lead_map(process_gated_incumbent_vs_residual_map)
    hedge_disclosed_bridge_current_next_map = _lead_map(hedge_disclosed_bridge_current_map)
    hedge_disclosed_bridge_front_next_map = _lead_map(hedge_disclosed_bridge_front_map)
    hedge_disclosed_process_current_next_map = _lead_map(hedge_disclosed_process_current_map)
    hedge_disclosed_process_front_next_map = _lead_map(hedge_disclosed_process_front_map)
    hedge_pattern_bridge_current_next_map = _lead_map(hedge_pattern_bridge_current_map)
    hedge_pattern_bridge_front_next_map = _lead_map(hedge_pattern_bridge_front_map)
    hedge_pattern_process_current_next_map = _lead_map(hedge_pattern_process_current_map)
    hedge_pattern_process_front_next_map = _lead_map(hedge_pattern_process_front_map)
    bid_adjusted_next_map = _lead_map(bid_adjusted_map)
    ops_penalty_next_map = _lead_map(ops_penalty_map)
    total_exec_penalty_next_map = _lead_map(total_exec_penalty_map)
    utilization_penalty_next_map = _lead_map(utilization_penalty_map)
    maintenance_delay_penalty_next_map = _lead_map(maintenance_delay_penalty_map)
    inventory_drag_penalty_next_map = _lead_map(inventory_drag_penalty_map)
    disclosed_share_next_map = _lead_map(disclosed_share_map)
    pattern_share_next_map = _lead_map(pattern_share_map)
    prior_pred_map = _shift_quarter_map(pred_map, ordered_quarters, lag_quarters=1)

    def _blend_preview_current(qd: date, *, penalty_lookup: Optional[Dict[date, float]] = None) -> Optional[float]:
        current_val = pd.to_numeric(process_current_map.get(qd), errors="coerce")
        anchor_val = pd.to_numeric(process_anchor_map.get(qd), errors="coerce")
        ethanol_val = pd.to_numeric(ethanol_map.get(qd), errors="coerce")
        penalty_num = pd.to_numeric((penalty_lookup or {}).get(qd), errors="coerce")
        penalty_val = 0.0 if pd.isna(penalty_num) else float(penalty_num)
        if pd.notna(current_val) and pd.notna(ethanol_val):
            if pd.notna(anchor_val):
                anchor_weight = float(pd.to_numeric(quarter_open_weight_map.get(qd), errors="coerce") or 0.75)
                current_weight = float(pd.to_numeric(current_weight_map.get(qd), errors="coerce") or 0.25)
                current_noneth = float(current_val) - float(ethanol_val)
                intercept = (current_weight * current_noneth) - penalty_val
                intercept += anchor_weight * float(anchor_val)
                return (current_weight * float(ethanol_val)) + intercept
            return float(current_val) - penalty_val
        if pd.notna(anchor_val):
            return float(anchor_val) - penalty_val
        if pd.notna(current_val):
            return float(current_val) - penalty_val
        return None

    def _hedge_realization_preview(
        base_val: Any,
        *,
        qd: date,
        disclosed_lookup: Dict[date, float],
        pattern_lookup: Dict[date, float],
        disclosed_ref_lookup: Dict[date, float],
        pattern_ref_lookup: Dict[date, float],
    ) -> Optional[float]:
        return _gpre_hedge_realization_value(
            base_val,
            disclosed_share=disclosed_lookup.get(qd),
            pattern_share=pattern_lookup.get(qd),
            disclosed_reference=disclosed_ref_lookup.get(qd),
            pattern_reference=pattern_ref_lookup.get(qd),
        )

    def _current_preview_value(qd: date) -> Optional[float]:
        if model_key == "bridge_current_quarter_avg":
            return bridge_current_map.get(qd)
        if model_key == "bridge_front_loaded":
            return bridge_front_map.get(qd)
        if model_key == "bridge_current75_prev25":
            return bridge_75_map.get(qd)
        if model_key == "bridge_current50_prev50":
            return bridge_50_map.get(qd)
        if model_key == "process_current_quarter_avg":
            return process_current_map.get(qd)
        if model_key == "process_front_loaded":
            return process_front_map.get(qd)
        if model_key == "process_current75_prev25":
            return process_75_map.get(qd)
        if model_key == "process_current50_prev50":
            return process_50_map.get(qd)
        if model_key == "process_quarter_open_blend":
            return _blend_preview_current(qd)
        if model_key == "process_quarter_open_blend_ops_penalty":
            return _blend_preview_current(qd, penalty_lookup=ops_penalty_map)
        if model_key == "process_quarter_open_blend_hedge_realization":
            return _hedge_realization_preview(
                _blend_preview_current(qd),
                qd=qd,
                disclosed_lookup=disclosed_share_map,
                pattern_lookup=pattern_share_map,
                disclosed_ref_lookup=hedge_disclosed_process_front_map,
                pattern_ref_lookup=hedge_pattern_process_front_map,
            )
        if model_key == "process_quarter_open_blend_exec_penalty":
            return _blend_preview_current(qd, penalty_lookup=total_exec_penalty_map)
        if model_key == "process_quarter_open_blend_utilization_penalty":
            return _blend_preview_current(qd, penalty_lookup=utilization_penalty_map)
        if model_key == "process_quarter_open_blend_maintenance_delay_penalty":
            return _blend_preview_current(qd, penalty_lookup=maintenance_delay_penalty_map)
        if model_key == "process_quarter_open_blend_inventory_timing_drag":
            return _blend_preview_current(qd, penalty_lookup=inventory_drag_penalty_map)
        if model_key == "process_quarter_open_blend_locked_setup":
            return _gpre_locked_setup_value(
                _blend_preview_current(qd),
                disclosed_share=disclosed_share_map.get(qd),
                pattern_share=pattern_share_map.get(qd),
                quarter_open_anchor=process_anchor_map.get(qd),
                cap=0.40,
            )
        if model_key == "process_basis_blend_current40_front60":
            return process_basis_blend_current40_front60_map.get(qd)
        if model_key == "process_basis_passthrough_beta35":
            return process_basis_passthrough_beta35_map.get(qd)
        if model_key == "process_basis_passthrough_beta65":
            return process_basis_passthrough_beta65_map.get(qd)
        if model_key == "process_inventory_gap_penalty_small":
            return process_inventory_gap_penalty_small_map.get(qd)
        if model_key == "process_inventory_gap_penalty_medium":
            return process_inventory_gap_penalty_medium_map.get(qd)
        if model_key == "process_utilization_regime_blend":
            return process_utilization_regime_blend_map.get(qd)
        if model_key == "process_utilization_regime_residual":
            return process_utilization_regime_residual_map.get(qd)
        if model_key == "process_exec_inventory_combo_medium":
            return process_exec_inventory_combo_medium_map.get(qd)
        if model_key == "process_asymmetric_basis_passthrough":
            return process_asymmetric_basis_passthrough_map.get(qd)
        if model_key == "process_market_process_ensemble_35_65":
            return process_market_process_ensemble_35_65_map.get(qd)
        if model_key == "process_locked_share_asymmetric_passthrough":
            return process_locked_share_asymmetric_passthrough_map.get(qd)
        if model_key == "process_prior_gap_carryover_small":
            return process_prior_gap_carryover_small_map.get(qd)
        if model_key == "process_prior_disturbance_carryover":
            return process_prior_disturbance_carryover_map.get(qd)
        if model_key == "process_residual_regime_locked_vs_disturbed":
            return process_residual_regime_locked_vs_disturbed_map.get(qd)
        if model_key == "process_gated_incumbent_vs_residual":
            return process_gated_incumbent_vs_residual_map.get(qd)
        if model_key == "process_quarter_open_current50_exec_penalty":
            return process_quarter_open_current50_exec_penalty_map.get(qd)
        if model_key == "process_regime_basis_passthrough":
            return process_regime_basis_passthrough_map.get(qd)
        if model_key == "process_two_stage_realization_residual":
            return process_two_stage_realization_residual_map.get(qd)
        if model_key == "process_capacity_weighted_basis_strict":
            return process_capacity_weighted_basis_strict_map.get(qd)
        if model_key == "process_front_loaded_ops_penalty":
            return process_front_ops_map.get(qd)
        if model_key == "process_front_loaded_ethanol_geo":
            return process_front_geo_map.get(qd)
        if model_key == "hedge_disclosed_bridge_prior_current":
            return hedge_disclosed_bridge_current_map.get(qd)
        if model_key == "hedge_disclosed_bridge_prior_front":
            return hedge_disclosed_bridge_front_map.get(qd)
        if model_key == "hedge_disclosed_process_prior_current":
            return hedge_disclosed_process_current_map.get(qd)
        if model_key == "hedge_disclosed_process_prior_front":
            return hedge_disclosed_process_front_map.get(qd)
        if model_key == "hedge_pattern_bridge_prior_current":
            return hedge_pattern_bridge_current_map.get(qd)
        if model_key == "hedge_pattern_bridge_prior_front":
            return hedge_pattern_bridge_front_map.get(qd)
        if model_key == "hedge_pattern_process_prior_current":
            return hedge_pattern_process_current_map.get(qd)
        if model_key == "hedge_pattern_process_prior_front":
            return hedge_pattern_process_front_map.get(qd)
        if model_key == "bid_adjusted_offset":
            return bid_adjusted_map.get(qd)
        return pred_map.get(qd)

    def _next_preview_value(qd: date) -> Optional[float]:
        if model_key == "bridge_current_quarter_avg":
            return bridge_current_next_map.get(qd)
        if model_key == "bridge_front_loaded":
            return bridge_front_next_map.get(qd)
        if model_key == "bridge_current75_prev25":
            return bridge_75_next_map.get(qd)
        if model_key == "bridge_current50_prev50":
            return bridge_50_next_map.get(qd)
        if model_key == "process_current_quarter_avg":
            return process_current_next_map.get(qd)
        if model_key == "process_front_loaded":
            return process_front_next_map.get(qd)
        if model_key == "process_current75_prev25":
            return process_75_next_map.get(qd)
        if model_key == "process_current50_prev50":
            return process_50_next_map.get(qd)
        if model_key == "process_quarter_open_blend":
            return process_current_next_map.get(qd)
        if model_key == "process_quarter_open_blend_ops_penalty":
            base_val = pd.to_numeric(process_current_next_map.get(qd), errors="coerce")
            penalty_val = pd.to_numeric(ops_penalty_next_map.get(qd), errors="coerce")
            return None if pd.isna(base_val) else float(base_val) - (0.0 if pd.isna(penalty_val) else float(penalty_val))
        if model_key == "process_quarter_open_blend_hedge_realization":
            return _hedge_realization_preview(
                process_current_next_map.get(qd),
                qd=qd,
                disclosed_lookup=disclosed_share_next_map,
                pattern_lookup=pattern_share_next_map,
                disclosed_ref_lookup=hedge_disclosed_process_front_next_map,
                pattern_ref_lookup=hedge_pattern_process_front_next_map,
            )
        if model_key == "process_quarter_open_blend_exec_penalty":
            base_val = pd.to_numeric(process_current_next_map.get(qd), errors="coerce")
            penalty_val = pd.to_numeric(total_exec_penalty_next_map.get(qd), errors="coerce")
            return None if pd.isna(base_val) else float(base_val) - (0.0 if pd.isna(penalty_val) else float(penalty_val))
        if model_key == "process_quarter_open_blend_utilization_penalty":
            base_val = pd.to_numeric(process_current_next_map.get(qd), errors="coerce")
            penalty_val = pd.to_numeric(utilization_penalty_next_map.get(qd), errors="coerce")
            return None if pd.isna(base_val) else float(base_val) - (0.0 if pd.isna(penalty_val) else float(penalty_val))
        if model_key == "process_quarter_open_blend_maintenance_delay_penalty":
            base_val = pd.to_numeric(process_current_next_map.get(qd), errors="coerce")
            penalty_val = pd.to_numeric(maintenance_delay_penalty_next_map.get(qd), errors="coerce")
            return None if pd.isna(base_val) else float(base_val) - (0.0 if pd.isna(penalty_val) else float(penalty_val))
        if model_key == "process_quarter_open_blend_inventory_timing_drag":
            base_val = pd.to_numeric(process_current_next_map.get(qd), errors="coerce")
            penalty_val = pd.to_numeric(inventory_drag_penalty_next_map.get(qd), errors="coerce")
            return None if pd.isna(base_val) else float(base_val) - (0.0 if pd.isna(penalty_val) else float(penalty_val))
        if model_key == "process_quarter_open_blend_locked_setup":
            return _gpre_locked_setup_value(
                process_current_next_map.get(qd),
                disclosed_share=disclosed_share_next_map.get(qd),
                pattern_share=pattern_share_next_map.get(qd),
                quarter_open_anchor=process_anchor_map.get(qd),
                cap=0.40,
            )
        if model_key == "process_basis_blend_current40_front60":
            return process_basis_blend_current40_front60_next_map.get(qd)
        if model_key == "process_basis_passthrough_beta35":
            return process_basis_passthrough_beta35_next_map.get(qd)
        if model_key == "process_basis_passthrough_beta65":
            return process_basis_passthrough_beta65_next_map.get(qd)
        if model_key == "process_inventory_gap_penalty_small":
            return process_inventory_gap_penalty_small_next_map.get(qd)
        if model_key == "process_inventory_gap_penalty_medium":
            return process_inventory_gap_penalty_medium_next_map.get(qd)
        if model_key == "process_utilization_regime_blend":
            return process_utilization_regime_blend_next_map.get(qd)
        if model_key == "process_utilization_regime_residual":
            return process_utilization_regime_residual_next_map.get(qd)
        if model_key == "process_exec_inventory_combo_medium":
            return process_exec_inventory_combo_medium_next_map.get(qd)
        if model_key == "process_asymmetric_basis_passthrough":
            return process_asymmetric_basis_passthrough_next_map.get(qd)
        if model_key == "process_market_process_ensemble_35_65":
            process_exec_next = pd.to_numeric(process_current_next_map.get(qd), errors="coerce")
            exec_penalty_next = pd.to_numeric(total_exec_penalty_next_map.get(qd), errors="coerce")
            process_exec_component = (
                None
                if pd.isna(process_exec_next)
                else float(process_exec_next) - (0.0 if pd.isna(exec_penalty_next) else float(exec_penalty_next))
            )
            return _blend_optional_values(
                bridge_current_next_map.get(qd),
                process_exec_component,
                anchor_weight=0.35,
                current_weight=0.65,
            )
        if model_key == "process_locked_share_asymmetric_passthrough":
            locked_share, _ = _gpre_effective_hedge_share(
                disclosed_share_next_map.get(qd),
                pattern_share_next_map.get(qd),
                cap=0.35,
            )
            locked_setup_next = _gpre_locked_setup_value(
                process_current_next_map.get(qd),
                disclosed_share=disclosed_share_next_map.get(qd),
                pattern_share=pattern_share_next_map.get(qd),
                quarter_open_anchor=process_anchor_map.get(qd),
                cap=0.40,
            )
            asym_next = _gpre_asymmetric_passthrough_value(
                process_basis_passthrough_beta35_next_map.get(qd),
                process_basis_passthrough_beta65_next_map.get(qd),
            )
            return _blend_optional_values(
                locked_setup_next,
                asym_next,
                anchor_weight=float(locked_share),
                current_weight=1.0 - float(locked_share),
            )
        if model_key == "process_prior_gap_carryover_small":
            return _gpre_prior_gap_carryover_value(
                process_current_next_map.get(qd),
                process_inventory_gap_penalty_small_map.get(qd),
                multiplier=0.50,
                cap=0.03,
            )
        if model_key == "process_prior_disturbance_carryover":
            return _gpre_prior_disturbance_carryover_value(
                process_basis_passthrough_beta35_next_map.get(qd),
                prior_disturbed_flag=(float(pd.to_numeric(disturbed_flag_map.get(qd), errors="coerce")) > 0.0) if pd.notna(pd.to_numeric(disturbed_flag_map.get(qd), errors="coerce")) else False,
                prior_hard_flag=(float(pd.to_numeric(hard_flag_map.get(qd), errors="coerce")) > 0.0) if pd.notna(pd.to_numeric(hard_flag_map.get(qd), errors="coerce")) else False,
                prior_residual_penalty=realization_residual_penalty_map.get(qd),
                multiplier=0.60,
                cap=0.04,
            )
        if model_key == "process_residual_regime_locked_vs_disturbed":
            return process_residual_regime_locked_vs_disturbed_next_map.get(qd)
        if model_key == "process_gated_incumbent_vs_residual":
            return process_gated_incumbent_vs_residual_next_map.get(qd)
        if model_key == "process_quarter_open_current50_exec_penalty":
            return process_quarter_open_current50_exec_penalty_next_map.get(qd)
        if model_key == "process_regime_basis_passthrough":
            return process_regime_basis_passthrough_next_map.get(qd)
        if model_key == "process_two_stage_realization_residual":
            return process_two_stage_realization_residual_next_map.get(qd)
        if model_key == "process_capacity_weighted_basis_strict":
            return process_capacity_weighted_basis_strict_next_map.get(qd)
        if model_key == "process_front_loaded_ops_penalty":
            return process_front_ops_next_map.get(qd)
        if model_key == "process_front_loaded_ethanol_geo":
            return process_front_geo_next_map.get(qd)
        if model_key == "hedge_disclosed_bridge_prior_current":
            return hedge_disclosed_bridge_current_next_map.get(qd)
        if model_key == "hedge_disclosed_bridge_prior_front":
            return hedge_disclosed_bridge_front_next_map.get(qd)
        if model_key == "hedge_disclosed_process_prior_current":
            return hedge_disclosed_process_current_next_map.get(qd)
        if model_key == "hedge_disclosed_process_prior_front":
            return hedge_disclosed_process_front_next_map.get(qd)
        if model_key == "hedge_pattern_bridge_prior_current":
            return hedge_pattern_bridge_current_next_map.get(qd)
        if model_key == "hedge_pattern_bridge_prior_front":
            return hedge_pattern_bridge_front_next_map.get(qd)
        if model_key == "hedge_pattern_process_prior_current":
            return hedge_pattern_process_current_next_map.get(qd)
        if model_key == "hedge_pattern_process_prior_front":
            return hedge_pattern_process_front_next_map.get(qd)
        if model_key == "bid_adjusted_offset":
            return bid_adjusted_next_map.get(qd)
        return None

    def _simple_formula_helper_from_full(full_value: Any, ethanol_value: Any, *, mode: str) -> Dict[str, Any]:
        full_num = pd.to_numeric(full_value, errors="coerce")
        ethanol_num = pd.to_numeric(ethanol_value, errors="coerce")
        if pd.notna(full_num) and pd.notna(ethanol_num):
            return _gpre_formula_helper_payload(
                status="ok",
                mode=mode,
                slope=1.0,
                intercept=float(full_num) - float(ethanol_num),
            )
        if pd.notna(full_num):
            return _gpre_formula_helper_payload(
                status="ok",
                mode=mode,
                slope=0.0,
                intercept=float(full_num),
            )
        return _gpre_formula_helper_payload(
            status="no_data",
            mode=mode,
            slope=None,
            intercept=None,
        )

    def _phase_mode(model_key_in: str) -> str:
        key = str(model_key_in or "")
        if key.startswith("hedge_"):
            return "hedge_process" if "process" in key else "hedge_bridge"
        if "bridge" in key or key == "bid_adjusted_offset":
            return "bridge"
        return "process"

    def _phase_formula_helper(qd: date, phase: str) -> Dict[str, Any]:
        ethanol_phase = ethanol_map.get(qd) if phase == "current" else ethanol_next_map.get(qd)
        if phase == "current":
            if model_key in {
                "process_quarter_open_blend",
                "process_quarter_open_blend_ops_penalty",
                "process_quarter_open_blend_exec_penalty",
                "process_quarter_open_blend_hedge_realization",
            }:
                current_val = pd.to_numeric(process_current_map.get(qd), errors="coerce")
                current_noneth = (
                    None
                    if pd.isna(current_val) or pd.isna(pd.to_numeric(ethanol_phase, errors="coerce"))
                    else float(current_val) - float(pd.to_numeric(ethanol_phase, errors="coerce"))
                )
                penalty_lookup: Dict[date, float] = {}
                penalty_note = ""
                if model_key == "process_quarter_open_blend_ops_penalty":
                    penalty_lookup = ops_penalty_map
                    penalty_note = (
                        f"Includes a bounded ops penalty of "
                        f"{float(pd.to_numeric(ops_penalty_map.get(qd), errors='coerce') or 0.0):.3f} $/gal."
                    )
                elif model_key == "process_quarter_open_blend_exec_penalty":
                    penalty_lookup = total_exec_penalty_map
                    penalty_note = (
                        f"Includes the bounded severe execution penalty of "
                        f"{float(pd.to_numeric(total_exec_penalty_map.get(qd), errors='coerce') or 0.0):.3f} $/gal."
                    )
                base_helper = _gpre_process_blend_formula_helper(
                    anchor_proxy=process_anchor_map.get(qd),
                    current_nonethanol=current_noneth,
                    quarter_open_weight=quarter_open_weight_map.get(qd),
                    current_weight=current_weight_map.get(qd),
                    penalty=pd.to_numeric(penalty_lookup.get(qd), errors="coerce") if penalty_lookup else 0.0,
                    phase_label="current",
                    penalty_note=penalty_note,
                    live_preview_mode="exact_formula",
                )
                if model_key != "process_quarter_open_blend_hedge_realization":
                    return base_helper
                hedge_weight, hedge_source = _gpre_effective_hedge_share(
                    disclosed_share_map.get(qd),
                    pattern_share_map.get(qd),
                    cap=0.35,
                )
                hedge_full = (
                    hedge_disclosed_process_front_map.get(qd)
                    if hedge_source == "disclosed"
                    else hedge_pattern_process_front_map.get(qd)
                )
                hedge_helper = _simple_formula_helper_from_full(
                    hedge_full,
                    ethanol_phase,
                    mode="hedge_process",
                )
                return _gpre_combine_linear_formula_helpers(
                    base_helper,
                    hedge_helper,
                    secondary_weight=hedge_weight,
                    live_preview_mode="exact_formula",
                    live_preview_note="",
                )
            if model_key in {
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
                "process_basis_blend_current40_front60",
                "process_basis_passthrough_beta35",
                "process_basis_passthrough_beta65",
                "process_quarter_open_current50_exec_penalty",
                "process_regime_basis_passthrough",
                "process_two_stage_realization_residual",
                "process_capacity_weighted_basis_strict",
            }:
                preview_story = _gpre_phase_preview_story(model_key, phase="current")
                helper = _simple_formula_helper_from_full(
                    pred_map.get(qd),
                    ethanol_phase,
                    mode="process",
                )
                helper["live_preview_mode"] = preview_story.get("live_preview_mode") or "exact_formula"
                helper["live_preview_note"] = preview_story.get("live_preview_note") or ""
                return helper
            return _simple_formula_helper_from_full(
                _current_preview_value(qd),
                ethanol_phase,
                mode=_phase_mode(model_key),
            )

        if model_key in {
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
            "process_basis_blend_current40_front60",
            "process_basis_passthrough_beta35",
            "process_basis_passthrough_beta65",
            "process_quarter_open_current50_exec_penalty",
            "process_regime_basis_passthrough",
            "process_two_stage_realization_residual",
            "process_capacity_weighted_basis_strict",
        }:
            preview_story = _gpre_phase_preview_story(model_key, phase="next")
            helper = _simple_formula_helper_from_full(
                _next_preview_value(qd),
                ethanol_phase,
                mode="process",
            )
            helper["live_preview_mode"] = preview_story.get("live_preview_mode") or "reduced_form_approximation"
            helper["live_preview_note"] = preview_story.get("live_preview_note") or ""
            return helper
        if model_key == "process_quarter_open_blend_hedge_realization":
            hedge_weight, hedge_source = _gpre_effective_hedge_share(
                disclosed_share_next_map.get(qd),
                pattern_share_next_map.get(qd),
                cap=0.35,
            )
            base_helper = _simple_formula_helper_from_full(
                process_current_next_map.get(qd),
                ethanol_phase,
                mode="process",
            )
            hedge_full = (
                hedge_disclosed_process_front_next_map.get(qd)
                if hedge_source == "disclosed"
                else hedge_pattern_process_front_next_map.get(qd)
            )
            hedge_helper = _simple_formula_helper_from_full(
                hedge_full,
                ethanol_phase,
                mode="hedge_process",
            )
            return _gpre_combine_linear_formula_helpers(
                base_helper,
                hedge_helper,
                secondary_weight=hedge_weight,
                live_preview_mode="reduced_form_approximation",
                live_preview_note="",
            )
        return _simple_formula_helper_from_full(
            _next_preview_value(qd),
            ethanol_phase,
            mode=_phase_mode(model_key),
        )

    phase_records: Dict[str, List[Dict[str, Any]]] = {
        "prior": [],
        "quarter_open": [],
        "current": [],
        "next": [],
    }
    for qd in ordered_quarters:
        prior_val = pd.to_numeric(prior_pred_map.get(qd), errors="coerce")
        if pd.notna(prior_val):
            phase_records["prior"].append(
                {
                    "phase": "prior",
                    "quarter_label": _quarter_label(qd),
                    "abs_error": 0.0,
                    "signed_error": 0.0,
                    "full_value": float(prior_val),
                    "preview_value": float(prior_val),
                }
            )
        quarter_open_val = pd.to_numeric(_current_preview_value(qd), errors="coerce")
        if pd.notna(quarter_open_val):
            phase_records["quarter_open"].append(
                {
                    "phase": "quarter_open",
                    "quarter_label": _quarter_label(qd),
                    "abs_error": 0.0,
                    "signed_error": 0.0,
                    "full_value": float(quarter_open_val),
                    "preview_value": float(quarter_open_val),
                }
            )
        current_full = pd.to_numeric(pred_map.get(qd), errors="coerce")
        current_helper = _phase_formula_helper(qd, "current")
        current_preview = pd.to_numeric(
            _gpre_evaluate_formula_helper_payload(current_helper, ethanol_map.get(qd)),
            errors="coerce",
        )
        if pd.notna(current_full) and pd.notna(current_preview):
            current_err = float(current_preview) - float(current_full)
            phase_records["current"].append(
                {
                    "phase": "current",
                    "quarter_label": _quarter_label(qd),
                    "abs_error": abs(current_err),
                    "signed_error": current_err,
                    "full_value": float(current_full),
                    "preview_value": float(current_preview),
                }
            )
        next_full = pd.to_numeric(_next_preview_value(qd), errors="coerce")
        next_helper = _phase_formula_helper(qd, "next")
        next_preview = pd.to_numeric(
            _gpre_evaluate_formula_helper_payload(next_helper, ethanol_next_map.get(qd)),
            errors="coerce",
        )
        if pd.notna(next_full) and pd.notna(next_preview):
            next_err = float(next_preview) - float(next_full)
            phase_records["next"].append(
                {
                    "phase": "next",
                    "quarter_label": _quarter_label(qd),
                    "abs_error": abs(next_err),
                    "signed_error": next_err,
                    "full_value": float(next_full),
                    "preview_value": float(next_preview),
                }
            )
    preview_accuracy = {
        phase_name: _gpre_preview_phase_metrics(records)
        for phase_name, records in phase_records.items()
    }
    overall_records = [rec for records in phase_records.values() for rec in records]
    overall_metrics = _gpre_preview_phase_metrics(overall_records)
    worst_phase = ""
    worst_phase_error = -1.0
    for phase_name, metrics in preview_accuracy.items():
        phase_max = pd.to_numeric(metrics.get("preview_max_error"), errors="coerce")
        if pd.notna(phase_max) and float(phase_max) > worst_phase_error:
            worst_phase_error = float(phase_max)
            worst_phase = phase_name
    if worst_phase_error <= 1e-12:
        worst_phase = ""
    return {
        "preview_accuracy": preview_accuracy,
        "live_preview_mae": overall_metrics.get("preview_mae"),
        "live_preview_max_error": overall_metrics.get("preview_max_error"),
        "live_preview_bias": overall_metrics.get("preview_bias"),
        "live_preview_top_miss_quarters": overall_metrics.get("preview_top_miss_quarters"),
        "live_preview_quality_status": _gpre_preview_quality_status(
            overall_metrics.get("preview_mae"),
            overall_metrics.get("preview_max_error"),
        ),
        "live_preview_worst_phase": worst_phase,
    }


def _build_gpre_proxy_leaderboard(
    quarterly_df: pd.DataFrame,
    metrics_df: pd.DataFrame,
    model_specs: List[Dict[str, Any]],
    *,
    incumbent_baseline_model_key: str = "",
    process_comparator_model_key: str = "process_front_loaded",
    new_candidate_keys: Optional[set[str]] = None,
) -> pd.DataFrame:
    leaderboard_rows: List[Dict[str, Any]] = []
    official_simple_series = (
        pd.to_numeric(quarterly_df["official_simple_proxy_usd_per_gal"], errors="coerce")
        if isinstance(quarterly_df, pd.DataFrame) and "official_simple_proxy_usd_per_gal" in quarterly_df.columns
        else pd.Series(dtype=float)
    )
    new_candidate_keys = set(new_candidate_keys or set())
    for spec in model_specs:
        model_key = str(spec.get("model_key") or "")
        pred_col = str(spec.get("pred_col") or "")
        pred_series = pd.to_numeric(quarterly_df[pred_col], errors="coerce") if pred_col in quarterly_df.columns else pd.Series(dtype=float)
        coverage_quarters, coverage_ratio = _gpre_signal_coverage_stats(
            quarterly_df,
            pred_col=pred_col,
        )
        clean_mae = _gpre_leaderboard_metric_pick(metrics_df, model_key, "clean_reported_window", "mae")
        underlying_mae = _gpre_leaderboard_metric_pick(metrics_df, model_key, "diag_underlying", "mae")
        hybrid_score = (
            (0.50 * clean_mae) + (0.50 * underlying_mae)
            if np.isfinite(clean_mae) and np.isfinite(underlying_mae)
            else float("nan")
        )
        walk_forward_tail_mae = _gpre_walk_forward_tail_mae(
            quarterly_df,
            pred_col=pred_col,
        )
        forward_usability_rating = _gpre_forward_usability_rating(
            model_key,
            family=spec.get("family"),
        )
        complexity_rating = _gpre_complexity_rating(
            model_key,
            family=spec.get("family"),
        )
        eligible_official = bool(spec.get("eligible_official")) and (not pred_series.empty) and pred_series.notna().all() and np.isfinite(hybrid_score)
        diff_stats = _gpre_diff_vs_official_stats(pred_series, official_simple_series)
        full_mean_error = _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "mean_error")
        if model_key == str(incumbent_baseline_model_key or ""):
            baseline_status = "incumbent_current_state"
        elif model_key == str(process_comparator_model_key or ""):
            baseline_status = "incumbent_process_comparator"
        elif model_key in new_candidate_keys:
            baseline_status = "new_candidate"
        else:
            baseline_status = "existing_candidate"
        hard_stats = _gpre_hard_quarter_stats(quarterly_df, pred_col)
        preview_stats = (
            _gpre_preview_accuracy_for_model(
                quarterly_df,
                model_key=model_key,
                pred_col=pred_col,
            )
            if bool(spec.get("preview_supported", spec.get("eligible_official")))
            else {
                "preview_accuracy": {
                    "prior": _gpre_preview_phase_metrics([]),
                    "quarter_open": _gpre_preview_phase_metrics([]),
                    "current": _gpre_preview_phase_metrics([]),
                    "next": _gpre_preview_phase_metrics([]),
                },
                "live_preview_mae": None,
                "live_preview_max_error": None,
                "live_preview_bias": None,
                "live_preview_top_miss_quarters": "",
                "live_preview_quality_status": "not_faithful_enough",
                "live_preview_worst_phase": "",
            }
        )
        top_miss_sub = metrics_df[
            (metrics_df["model_key"].astype(str) == model_key)
            & (metrics_df["split"].astype(str) == "full")
        ].copy()
        leaderboard_rows.append(
            {
                "model_key": model_key,
                "pred_col": pred_col,
                "family": str(spec.get("family") or ""),
                "family_label": str(spec.get("family_label") or ""),
                "experimental_method_family": str(spec.get("experimental_method_family") or ""),
                "signal_dependency_note": str(spec.get("signal_dependency_note") or ""),
                "timing_rule": str(spec.get("timing_rule") or ""),
                "clean_mae": clean_mae,
                "underlying_mae": underlying_mae,
                "hybrid_score": hybrid_score,
                "eligible_official": eligible_official,
                "preview_supported": bool(spec.get("preview_supported", spec.get("eligible_official"))),
                "chosen": False,
                "comparison_only": not bool(spec.get("eligible_official")),
                "full_mean_error": full_mean_error,
                "test_mae": _gpre_leaderboard_metric_pick(metrics_df, model_key, "test", "mae"),
                "test_corr": _gpre_leaderboard_metric_pick(metrics_df, model_key, "test", "correlation"),
                "test_mean_error": _gpre_leaderboard_metric_pick(metrics_df, model_key, "test", "mean_error"),
                "test_sign_hit_rate": _gpre_leaderboard_metric_pick(metrics_df, model_key, "test", "sign_hit_rate"),
                "q1_mean_error": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q1_mean_error"),
                "q2_mean_error": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q2_mean_error"),
                "q3_mean_error": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q3_mean_error"),
                "q4_mean_error": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q4_mean_error"),
                "q1_mae": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q1_mae"),
                "q2_mae": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q2_mae"),
                "q3_mae": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q3_mae"),
                "q4_mae": _gpre_leaderboard_metric_pick(metrics_df, model_key, "full", "q4_mae"),
                "material_diff_quarter_count_vs_official": _count_material_difference_quarters_vs_official(
                    pred_series,
                    official_simple_series,
                    threshold=0.025,
                ),
                "walk_forward_tail_mae": walk_forward_tail_mae,
                "signal_coverage_quarters": coverage_quarters,
                "signal_coverage_ratio": coverage_ratio,
                "forward_usability_rating": forward_usability_rating,
                "complexity_rating": complexity_rating,
                "avg_abs_diff_vs_official": diff_stats["avg_abs_diff_vs_official"],
                "diff_quarters_gt_0_02_vs_official": diff_stats["diff_quarters_gt_0_02_vs_official"],
                "diff_quarters_gt_0_05_vs_official": diff_stats["diff_quarters_gt_0_05_vs_official"],
                "bias_direction": _gpre_bias_direction(full_mean_error),
                "baseline_status": baseline_status,
                "hard_quarter_mae": hard_stats.get("hard_quarter_mae"),
                "hard_quarter_mean_error": hard_stats.get("hard_quarter_mean_error"),
                "hard_quarter_count": hard_stats.get("hard_quarter_count"),
                "hard_quarter_top_miss_quarters": hard_stats.get("hard_quarter_top_miss_quarters"),
                "preview_accuracy": preview_stats.get("preview_accuracy"),
                "live_preview_mae": preview_stats.get("live_preview_mae"),
                "live_preview_max_error": preview_stats.get("live_preview_max_error"),
                "live_preview_bias": preview_stats.get("live_preview_bias"),
                "live_preview_top_miss_quarters": preview_stats.get("live_preview_top_miss_quarters"),
                "live_preview_quality_status": preview_stats.get("live_preview_quality_status"),
                "live_preview_worst_phase": preview_stats.get("live_preview_worst_phase"),
                "selection_guard_reason": "",
                "promotion_guard_reason": "",
                "top_miss_quarters": (str(top_miss_sub.iloc[0].get("top_miss_quarters") or "") if not top_miss_sub.empty else ""),
            }
        )
    return pd.DataFrame(leaderboard_rows)


def _gpre_candidate_delta_quarters_vs_incumbent(
    quarterly_df: pd.DataFrame,
    *,
    candidate_col: str,
    incumbent_col: str,
    actual_col: str = "evaluation_target_margin_usd_per_gal",
) -> Dict[str, str]:
    if (
        quarterly_df is None
        or quarterly_df.empty
        or candidate_col not in quarterly_df.columns
        or incumbent_col not in quarterly_df.columns
        or actual_col not in quarterly_df.columns
    ):
        return {
            "top_improved_quarters_vs_incumbent": "",
            "top_worsened_quarters_vs_incumbent": "",
            "improved_quarter_count_vs_incumbent": 0,
            "worsened_quarter_count_vs_incumbent": 0,
            "largest_improvement_abs_delta_vs_incumbent": None,
        }
    sub = pd.DataFrame(
        {
            "quarter": quarterly_df["quarter"],
            "candidate": pd.to_numeric(quarterly_df[candidate_col], errors="coerce"),
            "incumbent": pd.to_numeric(quarterly_df[incumbent_col], errors="coerce"),
            "actual": pd.to_numeric(quarterly_df[actual_col], errors="coerce"),
        }
    )
    sub["quarter_ts"] = pd.to_datetime(sub["quarter"], errors="coerce")
    sub = sub[sub["candidate"].notna() & sub["incumbent"].notna() & sub["actual"].notna() & sub["quarter_ts"].notna()].copy()
    if sub.empty:
        return {
            "top_improved_quarters_vs_incumbent": "",
            "top_worsened_quarters_vs_incumbent": "",
            "improved_quarter_count_vs_incumbent": 0,
            "worsened_quarter_count_vs_incumbent": 0,
            "largest_improvement_abs_delta_vs_incumbent": None,
        }
    sub["delta_vs_incumbent"] = (sub["incumbent"] - sub["actual"]).abs() - (sub["candidate"] - sub["actual"]).abs()

    def _fmt_rows(frame_in: pd.DataFrame, *, ascending: bool) -> str:
        if frame_in.empty:
            return ""
        ordered = frame_in.sort_values("delta_vs_incumbent", ascending=ascending).head(5)
        parts: List[str] = []
        for _, rec in ordered.iterrows():
            qd = rec.get("quarter_ts")
            delta_num = pd.to_numeric(rec.get("delta_vs_incumbent"), errors="coerce")
            if pd.isna(qd) or pd.isna(delta_num):
                continue
            parts.append(f"{_quarter_label(pd.Timestamp(qd).date())} ({float(delta_num):+.3f})")
        return ", ".join(parts)

    improved = sub[sub["delta_vs_incumbent"] > 1e-12].copy()
    worsened = sub[sub["delta_vs_incumbent"] < -1e-12].copy()
    largest_improvement = (
        float(pd.to_numeric(improved["delta_vs_incumbent"], errors="coerce").max())
        if not improved.empty
        else None
    )
    return {
        "top_improved_quarters_vs_incumbent": _fmt_rows(improved, ascending=False),
        "top_worsened_quarters_vs_incumbent": _fmt_rows(worsened, ascending=True),
        "improved_quarter_count_vs_incumbent": int(len(improved)),
        "worsened_quarter_count_vs_incumbent": int(len(worsened)),
        "largest_improvement_abs_delta_vs_incumbent": largest_improvement,
    }


def _gpre_experimental_signal_audit(quarterly_df: pd.DataFrame) -> Dict[str, Any]:
    if quarterly_df is None or quarterly_df.empty:
        return {"signal_rows": [], "interpretation_lines": []}

    def _count_non_blank(col_name: str) -> int:
        if col_name not in quarterly_df.columns:
            return 0
        series = quarterly_df[col_name]
        return int(series.astype(str).str.strip().ne("").sum())

    def _count_positive(col_name: str) -> int:
        if col_name not in quarterly_df.columns:
            return 0
        return int(pd.to_numeric(quarterly_df[col_name], errors="coerce").fillna(0.0).gt(0.0).sum())

    def _count_numeric(col_name: str) -> int:
        if col_name not in quarterly_df.columns:
            return 0
        return int(pd.to_numeric(quarterly_df[col_name], errors="coerce").notna().sum())

    def _count_true(col_name: str) -> int:
        if col_name not in quarterly_df.columns:
            return 0
        return int(pd.Series(quarterly_df[col_name]).astype(bool).sum())

    def _count_locked_setup_available() -> int:
        disclosed = (
            pd.to_numeric(quarterly_df.get("hedge_share_disclosed"), errors="coerce").fillna(0.0)
            if "hedge_share_disclosed" in quarterly_df.columns
            else pd.Series(dtype=float)
        )
        pattern = (
            pd.to_numeric(quarterly_df.get("hedge_share_pattern"), errors="coerce").fillna(0.0)
            if "hedge_share_pattern" in quarterly_df.columns
            else pd.Series(dtype=float)
        )
        if disclosed.empty and pattern.empty:
            return 0
        if disclosed.empty:
            return int(pattern.gt(0.0).sum())
        if pattern.empty:
            return int(disclosed.gt(0.0).sum())
        return int((disclosed.gt(0.0) | pattern.gt(0.0)).sum())

    signal_rows = [
        {
            "signal": "gallons_produced",
            "classification": "directly_usable_now",
            "source": "reported_ethanol_gallons_produced_raw",
            "available_quarters": _count_numeric("reported_ethanol_gallons_produced_raw"),
            "active_signal_quarters": _count_numeric("reported_ethanol_gallons_produced_raw"),
            "note": "Reported ethanol gallons produced are available as the physical production anchor by quarter.",
        },
        {
            "signal": "gallons_sold",
            "classification": "directly_usable_now",
            "source": "reported_ethanol_gallons_sold_raw",
            "available_quarters": _count_numeric("reported_ethanol_gallons_sold_raw"),
            "active_signal_quarters": _count_numeric("reported_ethanol_gallons_sold_raw"),
            "note": "Reported ethanol gallons sold are available as the realized-volume counterpart by quarter.",
        },
        {
            "signal": "sold_minus_produced_gap",
            "classification": "derivable_with_light_work",
            "source": "reported_ethanol_gallons_sold_raw - reported_ethanol_gallons_produced_raw",
            "available_quarters": _count_numeric("sold_minus_produced_gap_ratio"),
            "active_signal_quarters": _count_positive("inventory_gap_disturbance_score"),
            "note": "Positive gap means sold > produced (inventory unwind / timing pull-forward); negative gap means produced > sold (inventory build / deferred sales).",
        },
        {
            "signal": "utilization",
            "classification": "directly_usable_now",
            "source": "ops_utilization_pct",
            "available_quarters": _count_numeric("ops_utilization_pct"),
            "active_signal_quarters": _count_positive("low_utilization_regime_score") + _count_positive("high_utilization_regime_score"),
            "note": "Usable both as a bounded penalty seam and as a bounded regime-tilt seam.",
        },
        {
            "signal": "maintenance_outage_delay",
            "classification": "directly_usable_now",
            "source": "ops_signal_terms",
            "available_quarters": _count_non_blank("ops_signal_terms"),
            "active_signal_quarters": _count_positive("maintenance_delay_penalty_usd_per_gal"),
            "note": "Explicit maintenance/outage/delay terms only.",
        },
        {
            "signal": "inventory_nrv_timing_drag",
            "classification": "derivable_with_light_work",
            "source": "local_doc_phrase_map",
            "available_quarters": _count_non_blank("inventory_drag_terms"),
            "active_signal_quarters": _count_positive("inventory_timing_drag_penalty_usd_per_gal"),
            "note": "Only explicit inventory/NRV/timing phrases are counted.",
        },
        {
            "signal": "quarter_open_anchor",
            "classification": "directly_usable_now",
            "source": "process_quarter_open_anchor_usd_per_gal",
            "available_quarters": _count_numeric("process_quarter_open_anchor_usd_per_gal"),
            "active_signal_quarters": _count_numeric("process_quarter_open_anchor_usd_per_gal"),
            "note": "Stored prior-front process anchor for quarter-open and locked/setup-style candidates.",
        },
        {
            "signal": "locked_setup",
            "classification": "directly_usable_now",
            "source": "hedge_share_disclosed / hedge_share_pattern / process_quarter_open_anchor_usd_per_gal",
            "available_quarters": _count_locked_setup_available(),
            "active_signal_quarters": _count_true("locked_or_setup_quarter_flag"),
            "note": "Uses capped locked-share evidence, not hedge-book reconstruction.",
        },
        {
            "signal": "hard_quarter_flags",
            "classification": "directly_usable_now",
            "source": "hard_quarter_flag / hard_quarter_reason",
            "available_quarters": _count_numeric("hard_quarter_flag"),
            "active_signal_quarters": _count_true("hard_quarter_flag"),
            "note": "Explicit hard-quarter flags remain available for diagnostics and simple gating ideas.",
        },
        {
            "signal": "preview_helper_support",
            "classification": "directly_usable_now",
            "source": "_gpre_preview_accuracy_for_model current/next helper branches",
            "available_quarters": len(quarterly_df),
            "active_signal_quarters": len(quarterly_df),
            "note": "All experimental candidates in this pass keep explicit current/next preview helper support.",
        },
    ]
    interpretation_lines = [
        "This pass stayed inside reported produced/sold gallons, utilization, explicit maintenance/outage, explicit inventory/timing drag, quarter-open anchors, and locked-setup seams already available by quarter.",
        "Sold minus produced is treated as a bounded realization / timing-mismatch signal with an explicit sign convention rather than a free-form inventory model.",
        "All promoted candidates still have explicit preview-helper coverage; no opaque fitting or new NLP-driven signal family was added.",
    ]
    return {
        "signal_rows": signal_rows,
        "interpretation_lines": interpretation_lines,
    }


def _gpre_experimental_candidate_comparison(
    quarterly_df: pd.DataFrame,
    leaderboard_df: pd.DataFrame,
    *,
    incumbent_model_key: str,
    experimental_candidate_keys: set[str],
) -> pd.DataFrame:
    if leaderboard_df is None or leaderboard_df.empty:
        return pd.DataFrame()
    pred_col_map = {
        str(rec.get("model_key") or ""): str(rec.get("pred_col") or "")
        for rec in leaderboard_df.to_dict("records")
        if str(rec.get("model_key") or "").strip() and str(rec.get("pred_col") or "").strip()
    }
    rows_to_keep = [str(incumbent_model_key or "").strip(), *sorted(str(item or "").strip() for item in experimental_candidate_keys if str(item or "").strip())]
    sub = leaderboard_df[leaderboard_df["model_key"].astype(str).isin(rows_to_keep)].copy()
    if sub.empty:
        return pd.DataFrame()
    incumbent_sub = sub[sub["model_key"].astype(str) == str(incumbent_model_key or "")].copy()
    incumbent_row = incumbent_sub.iloc[0].to_dict() if not incumbent_sub.empty else {}
    incumbent_hybrid = pd.to_numeric(incumbent_row.get("hybrid_score"), errors="coerce")
    incumbent_clean = pd.to_numeric(incumbent_row.get("clean_mae"), errors="coerce")
    incumbent_hard = pd.to_numeric(incumbent_row.get("hard_quarter_mae"), errors="coerce")
    out_rows: List[Dict[str, Any]] = []
    for rec in sub.to_dict("records"):
        model_key = str(rec.get("model_key") or "")
        delta_quarters = _gpre_candidate_delta_quarters_vs_incumbent(
            quarterly_df,
            candidate_col=pred_col_map.get(model_key, ""),
            incumbent_col=pred_col_map.get(str(incumbent_model_key or ""), ""),
        )
        improved_count = int(delta_quarters.get("improved_quarter_count_vs_incumbent") or 0)
        largest_improvement = pd.to_numeric(
            delta_quarters.get("largest_improvement_abs_delta_vs_incumbent"),
            errors="coerce",
        )
        if improved_count <= 2:
            concentration_note = "mostly_1_2_quarters"
        elif improved_count > 3 and (pd.isna(largest_improvement) or float(largest_improvement) <= 0.035):
            concentration_note = "broad"
        elif pd.notna(largest_improvement) and float(largest_improvement) > 0.035:
            concentration_note = "mixed"
        else:
            concentration_note = "mixed"
        hybrid_num = pd.to_numeric(rec.get("hybrid_score"), errors="coerce")
        clean_num = pd.to_numeric(rec.get("clean_mae"), errors="coerce")
        hard_num = pd.to_numeric(rec.get("hard_quarter_mae"), errors="coerce")
        out_rows.append(
            {
                "model_key": model_key,
                "family_label": str(rec.get("family_label") or rec.get("family") or ""),
                "candidate_method_family": str(rec.get("experimental_method_family") or rec.get("family_label") or rec.get("family") or ""),
                "signal_dependency_note": (
                    str(rec.get("signal_dependency_note") or "").strip()
                    or (
                        "Current production winner baseline comparator for this experimental pass."
                        if model_key == str(incumbent_model_key or "")
                        else str(rec.get("family_label") or rec.get("family") or "").strip()
                    )
                ),
                "clean_window_mae": clean_num,
                "underlying_window_mae": pd.to_numeric(rec.get("underlying_mae"), errors="coerce"),
                "hybrid_score": hybrid_num,
                "mean_error": pd.to_numeric(rec.get("full_mean_error"), errors="coerce"),
                "correlation": pd.to_numeric(rec.get("test_corr"), errors="coerce"),
                "sign_hit_rate": pd.to_numeric(rec.get("test_sign_hit_rate"), errors="coerce"),
                "sign_accuracy": pd.to_numeric(rec.get("test_sign_hit_rate"), errors="coerce"),
                "q1_mae": pd.to_numeric(rec.get("q1_mae"), errors="coerce"),
                "q2_mae": pd.to_numeric(rec.get("q2_mae"), errors="coerce"),
                "q3_mae": pd.to_numeric(rec.get("q3_mae"), errors="coerce"),
                "q4_mae": pd.to_numeric(rec.get("q4_mae"), errors="coerce"),
                "q1_mean_error": pd.to_numeric(rec.get("q1_mean_error"), errors="coerce"),
                "q2_mean_error": pd.to_numeric(rec.get("q2_mean_error"), errors="coerce"),
                "q3_mean_error": pd.to_numeric(rec.get("q3_mean_error"), errors="coerce"),
                "q4_mean_error": pd.to_numeric(rec.get("q4_mean_error"), errors="coerce"),
                "hard_quarter_mae": hard_num,
                "hard_quarter_mean_error": pd.to_numeric(rec.get("hard_quarter_mean_error"), errors="coerce"),
                "walk_forward_tail_mae": pd.to_numeric(rec.get("walk_forward_tail_mae"), errors="coerce"),
                "signal_coverage_quarters": int(pd.to_numeric(rec.get("signal_coverage_quarters"), errors="coerce") or 0),
                "signal_coverage_ratio": pd.to_numeric(rec.get("signal_coverage_ratio"), errors="coerce"),
                "forward_usability_rating": str(rec.get("forward_usability_rating") or ""),
                "complexity_rating": str(rec.get("complexity_rating") or ""),
                "preview_supported": bool(rec.get("preview_supported")),
                "eligible_official": bool(rec.get("eligible_official")),
                "avg_abs_diff_vs_official": pd.to_numeric(rec.get("avg_abs_diff_vs_official"), errors="coerce"),
                "diff_quarters_gt_0_02_vs_official": pd.to_numeric(rec.get("diff_quarters_gt_0_02_vs_official"), errors="coerce"),
                "diff_quarters_gt_0_05_vs_official": pd.to_numeric(rec.get("diff_quarters_gt_0_05_vs_official"), errors="coerce"),
                "preview_mae": pd.to_numeric(rec.get("live_preview_mae"), errors="coerce"),
                "preview_max_error": pd.to_numeric(rec.get("live_preview_max_error"), errors="coerce"),
                "preview_quality_class": str(rec.get("live_preview_quality_status") or ""),
                "top_miss_quarters": str(rec.get("top_miss_quarters") or ""),
                "top_improved_quarters_vs_incumbent": delta_quarters["top_improved_quarters_vs_incumbent"],
                "top_worsened_quarters_vs_incumbent": delta_quarters["top_worsened_quarters_vs_incumbent"],
                "concentration_note": concentration_note,
                "hybrid_score_delta_vs_incumbent": None if pd.isna(hybrid_num) or pd.isna(incumbent_hybrid) else float(hybrid_num) - float(incumbent_hybrid),
                "clean_window_mae_delta_vs_incumbent": None if pd.isna(clean_num) or pd.isna(incumbent_clean) else float(clean_num) - float(incumbent_clean),
                "hard_quarter_mae_delta_vs_incumbent": None if pd.isna(hard_num) or pd.isna(incumbent_hard) else float(hard_num) - float(incumbent_hard),
                "promotion_status": "winner" if bool(rec.get("production_winner")) else "blocked" if str(rec.get("promotion_guard_reason") or "").strip() not in {"", "incumbent_baseline", "passed_promotion_guardrails"} else "eligible",
                "promotion_reason": str(rec.get("promotion_guard_reason") or ""),
                "promotion_reason_human": _gpre_guard_reason_human(rec.get("promotion_guard_reason")),
            }
        )
    return pd.DataFrame(out_rows)


def _choose_gpre_proxy_winner_with_promotion(
    expanded_leaderboard_df: pd.DataFrame,
    *,
    incumbent_baseline_model_key: str,
    expanded_best_candidate_model_key: str = "",
    new_candidate_keys: set[str],
) -> Tuple[str, str]:
    annotated, winner_key, reason = _annotate_gpre_promotion_guardrails(
        expanded_leaderboard_df,
        incumbent_baseline_model_key=incumbent_baseline_model_key,
        expanded_best_candidate_model_key=expanded_best_candidate_model_key,
        new_candidate_keys=new_candidate_keys,
    )
    _ = annotated
    return winner_key, reason


def _recommended_model_key(metrics_df: pd.DataFrame) -> str:
    if metrics_df is None or metrics_df.empty:
        return "plant_count_weighted"
    test_rows = metrics_df[
        (metrics_df["split"].astype(str) == "test")
        & metrics_df["mae"].notna()
        & metrics_df["model_key"].isin(["equal_weighted", "plant_count_weighted", "capacity_weighted", "optimized_weights"])
    ].copy()
    if test_rows.empty:
        return "plant_count_weighted"
    best_mae = float(test_rows["mae"].min())
    eligible = test_rows[test_rows["mae"] <= (best_mae * 1.10 + 1e-12)].copy()
    preference = {
        "plant_count_weighted": 0,
        "capacity_weighted": 1,
        "equal_weighted": 2,
        "optimized_weights": 3,
    }
    eligible["pref"] = eligible["model_key"].map(preference).fillna(99)
    eligible = eligible.sort_values(["pref", "mae"])
    return str(eligible.iloc[0]["model_key"])


def _rolling_optimized_predictions(
    frame: pd.DataFrame,
    region_cols: List[str],
    *,
    min_train: int = 8,
    yield_per_bushel: float = 2.9,
    actual_col: str = "target_reported_crush_margin_usd_per_gal",
) -> Dict[date, float]:
    if frame is None or frame.empty or not region_cols:
        return {}
    resolved_cols = [col for col in region_cols if col in frame.columns]
    if not resolved_cols:
        resolved_cols = [f"basis_{str(col or '').strip()}_usd_per_bu" for col in region_cols if f"basis_{str(col or '').strip()}_usd_per_bu" in frame.columns]
    if not resolved_cols:
        return {}
    ordered = frame.sort_values("quarter").reset_index(drop=True)
    out: Dict[date, float] = {}
    for idx in range(max(int(min_train), 1), len(ordered)):
        train = ordered.iloc[:idx].copy()
        current = ordered.iloc[idx]
        train_valid = train[
            train[actual_col].notna()
            & train["baseline_market_proxy_usd_per_gal"].notna()
            & train[resolved_cols].notna().all(axis=1)
        ].copy()
        if len(train_valid) < max(int(min_train), 1):
            continue
        x_train = train_valid[resolved_cols].to_numpy(dtype=float)
        y_gap = (train_valid[actual_col] - train_valid["baseline_market_proxy_usd_per_gal"]).to_numpy(dtype=float)
        weights = _optimize_basis_weights(x_train, y_gap, yield_per_bushel=yield_per_bushel)
        if current[resolved_cols].isna().any() or pd.isna(current["baseline_market_proxy_usd_per_gal"]):
            continue
        qd = parse_quarter_like(current.get("quarter"))
        if qd is None:
            continue
        weighted_basis = float(np.dot(current[resolved_cols].to_numpy(dtype=float), weights))
        out[qd] = float(current["baseline_market_proxy_usd_per_gal"]) - (weighted_basis / float(yield_per_bushel))
    return out


def _gpre_production_formula_text(
    model_key: str,
    weights_df: pd.DataFrame,
    *,
    yield_per_bushel: float = 2.9,
    natural_gas_usage_btu_per_gal: float = 28000.0,
) -> str:
    display = {
        "equal_weighted": "equal-weight Midwest basis",
        "plant_count_weighted": "plant-count weighted GPRE footprint basis",
        "capacity_weighted": "capacity-weighted GPRE footprint basis",
        "optimized_weights": "constrained optimized regional basis",
    }
    gas_usage_txt = f"{float(natural_gas_usage_btu_per_gal or 0.0) / 1_000_000.0:.3f}"
    if weights_df is None or weights_df.empty:
        return (
            f"proxy = weighted_ethanol_benchmark - (cbot_corn_front + weighted_ams_basis) / {float(yield_per_bushel):.1f} "
            f"- ({gas_usage_txt} * nymex_gas)"
        )
    chosen = weights_df[(weights_df["model_key"].astype(str) == str(model_key)) & (weights_df["weight"].notna())].copy()
    if "quarter" in chosen.columns:
        chosen["quarter_ts"] = pd.to_datetime(chosen["quarter"], errors="coerce")
        if str(model_key) == "optimized_weights":
            chosen = chosen[chosen["quarter_ts"].isna()].copy()
        else:
            latest_quarter = chosen["quarter_ts"].dropna().max()
            if pd.notna(latest_quarter):
                chosen = chosen[chosen["quarter_ts"] == latest_quarter].copy()
    chosen = chosen.sort_values("weight", ascending=False)
    weight_txt = ", ".join(
        f"{str(rec['region']).replace('_', ' ').title()} {float(rec['weight']):.0%}"
        for rec in chosen.to_dict("records")
        if float(rec.get("weight") or 0.0) > 0
    )
    if not weight_txt:
        weight_txt = display.get(str(model_key), str(model_key))
    return (
        f"proxy = weighted_ethanol_benchmark - (cbot_corn_front + weighted_ams_basis) / {float(yield_per_bushel):.1f} "
        f"- ({gas_usage_txt} * nymex_gas), where weighted_ams_basis uses {weight_txt}"
    )


def _gpre_bridge_formula_text(
    timing_label: str,
    *,
    yield_per_bushel: float = 2.9,
) -> str:
    return (
        f"approx_market_crush = weighted_ethanol_benchmark - (cbot_corn_front + weighted_basis_{str(timing_label or '').strip()}) "
        f"/ {float(yield_per_bushel):.1f}"
    )


def build_gpre_basis_proxy_model(
    rows: Iterable[Dict[str, Any]],
    *,
    ticker_root: Optional[Path],
    reported_margin_by_quarter: Dict[date, float],
    underlying_margin_by_quarter: Optional[Dict[date, float]] = None,
    denominator_policy_by_quarter: Optional[Dict[date, str]] = None,
    reported_gallons_sold_by_quarter: Optional[Dict[date, float]] = None,
    reported_gallons_produced_by_quarter: Optional[Dict[date, float]] = None,
    as_of_date: Optional[date] = None,
    ethanol_yield: float = 2.9,
    natural_gas_usage: float = 28000.0,
    bids_snapshot: Optional[Dict[str, Any]] = None,
    plant_capacity_history: Optional[Dict[str, Any]] = None,
    prior_market_snapshot: Optional[Dict[str, Any]] = None,
    current_qtd_market_snapshot: Optional[Dict[str, Any]] = None,
    next_quarter_thesis_snapshot: Optional[Dict[str, Any]] = None,
    simple_crush_history_rows: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    row_list = list(rows or [])
    rows_df = _market_rows_df(row_list)
    target_quarters = _sorted_quarters(reported_margin_by_quarter.keys())
    try:
        yield_per_bushel = float(ethanol_yield or 2.9)
    except Exception:
        yield_per_bushel = 2.9
    if not np.isfinite(yield_per_bushel) or yield_per_bushel <= 0:
        yield_per_bushel = 2.9
    try:
        natural_gas_usage_num = float(natural_gas_usage or 28000.0)
    except Exception:
        natural_gas_usage_num = 28000.0
    if not np.isfinite(natural_gas_usage_num) or natural_gas_usage_num < 0:
        natural_gas_usage_num = 28000.0
    gas_usage_mmbtu_per_gal = natural_gas_usage_num / 1_000_000.0
    effective_plant_capacity_history = _gpre_resolve_plant_capacity_history(
        plant_capacity_history,
        ticker_root=ticker_root,
    )
    if not row_list or not target_quarters:
        return {
            "quarterly_df": pd.DataFrame(),
            "metrics_df": pd.DataFrame(),
            "weights_df": pd.DataFrame(),
            "footprint_df": pd.DataFrame(),
            "recommended_model_key": "plant_count_weighted",
            "recommended_formula": "",
            "summary_markdown": "",
            "proxy_implied_results": {},
            "experimental_signal_audit": {"signal_rows": [], "interpretation_lines": []},
            "experimental_candidate_comparison_df": pd.DataFrame(),
        }

    footprint_df = _gpre_quarterly_footprint(
        target_quarters,
        ticker_root,
        effective_plant_capacity_history,
    )
    footprint_map = {
        parse_quarter_like(rec.get("quarter")): rec
        for rec in footprint_df.to_dict("records")
        if parse_quarter_like(rec.get("quarter")) is not None
    }
    denominator_policy_by_quarter = dict(denominator_policy_by_quarter or {})
    reported_gallons_sold_by_quarter = {
        qd: float(num)
        for raw_qd, raw_val in dict(reported_gallons_sold_by_quarter or {}).items()
        for qd in [parse_quarter_like(raw_qd)]
        for num in [pd.to_numeric(raw_val, errors="coerce")]
        if isinstance(qd, date) and pd.notna(num)
    }
    reported_gallons_produced_by_quarter = {
        qd: float(num)
        for raw_qd, raw_val in dict(reported_gallons_produced_by_quarter or {}).items()
        for qd in [parse_quarter_like(raw_qd)]
        for num in [pd.to_numeric(raw_val, errors="coerce")]
        if isinstance(qd, date) and pd.notna(num)
    }
    evaluation_target_map, evaluation_target_basis_map, latest_underlying_quarters = _gpre_evaluation_target_maps(
        target_quarters,
        reported_margin_by_quarter,
        underlying_margin_by_quarter,
    )
    official_components_by_quarter = _gpre_official_quarter_component_records(
        rows_df,
        target_quarters,
        ticker_root=ticker_root,
        plant_capacity_history=effective_plant_capacity_history,
    )
    baseline_ethanol = {
        qd: float(rec["weighted_ethanol_benchmark_usd_per_gal"])
        for qd, rec in official_components_by_quarter.items()
        if pd.notna(pd.to_numeric(rec.get("weighted_ethanol_benchmark_usd_per_gal"), errors="coerce"))
    }
    official_basis_quarter_map = {
        qd: float(rec["weighted_ams_basis_usd_per_bu"])
        for qd, rec in official_components_by_quarter.items()
        if pd.notna(pd.to_numeric(rec.get("weighted_ams_basis_usd_per_bu"), errors="coerce"))
    }
    baseline_corn_futures = _quarter_avg_map(rows_df, "cbot_corn_usd_per_bu")
    baseline_natural_gas = _quarter_avg_map(rows_df, "nymex_gas")
    observation_count_map = _gpre_quarter_observation_count_map(rows_df)
    ethanol_illinois_map = _quarter_avg_map(rows_df, "ethanol_illinois")
    ethanol_indiana_map = _quarter_avg_map(rows_df, "ethanol_indiana")
    ops_signal_map = _gpre_ops_penalty_signal_map(ticker_root)
    inventory_signal_map = _gpre_inventory_timing_signal_map(ticker_root)
    basis_maps = {
        region: _quarter_avg_map(rows_df, f"corn_basis_{region}")
        for region in _GPRE_BASIS_REGIONS
    }
    basis_maps_front_loaded = {
        region: _quarter_observation_weighted_avg_map(rows_df, f"corn_basis_{region}", profile="front_loaded")
        for region in _GPRE_BASIS_REGIONS
    }
    relevant_regions = [region for region in _GPRE_BASIS_REGIONS if basis_maps.get(region)]
    if not relevant_regions:
        return {
            "quarterly_df": pd.DataFrame(),
            "metrics_df": pd.DataFrame(),
            "weights_df": pd.DataFrame(),
            "footprint_df": footprint_df,
            "recommended_model_key": "plant_count_weighted",
            "recommended_formula": "",
            "summary_markdown": "",
            "proxy_implied_results": {},
            "experimental_signal_audit": {"signal_rows": [], "interpretation_lines": []},
            "experimental_candidate_comparison_df": pd.DataFrame(),
        }

    effective_bids_snapshot = _resolve_gpre_corn_bids_snapshot(
        bids_snapshot,
        ticker_root=ticker_root,
        as_of_date=as_of_date,
    )
    bid_region_rows = _gpre_bid_region_basis_rows(effective_bids_snapshot)
    bid_region_map = {
        str(rec.get("region") or "").strip().lower(): rec
        for rec in bid_region_rows
        if str(rec.get("region") or "").strip()
    }
    ams_reference_rows = _latest_ams_basis_reference_rows(
        row_list,
        regions=relevant_regions,
        as_of_date=as_of_date,
        quarter_avg_maps=basis_maps,
        lookback_days=21,
    )
    ams_reference_map = {
        str(rec.get("region") or "").strip().lower(): rec
        for rec in ams_reference_rows
        if str(rec.get("region") or "").strip()
    }
    bid_offset_rows: List[Dict[str, Any]] = []
    bid_offset_map: Dict[str, float] = {}
    for region in relevant_regions:
        bid_rec = dict(bid_region_map.get(region) or {})
        ref_rec = dict(ams_reference_map.get(region) or {})
        bid_basis = pd.to_numeric(bid_rec.get("basis_usd_per_bu"), errors="coerce")
        ref_basis = pd.to_numeric(ref_rec.get("basis_usd_per_bu"), errors="coerce")
        offset_basis = None
        if pd.notna(bid_basis) and pd.notna(ref_basis):
            offset_basis = float(bid_basis) - float(ref_basis)
            bid_offset_map[region] = float(offset_basis)
        bid_offset_rows.append(
            {
                "region": region,
                "locations": ", ".join(bid_rec.get("locations") or []),
                "gpre_bid_basis_cents_per_bu": (None if pd.isna(bid_basis) else float(bid_basis) * 100.0),
                "gpre_bid_basis_usd_per_bu": (None if pd.isna(bid_basis) else float(bid_basis)),
                "ams_reference_basis_cents_per_bu": (None if pd.isna(ref_basis) else float(ref_basis) * 100.0),
                "ams_reference_basis_usd_per_bu": (None if pd.isna(ref_basis) else float(ref_basis)),
                "offset_cents_per_bu": (None if offset_basis is None else float(offset_basis) * 100.0),
                "offset_usd_per_bu": offset_basis,
                "reference_as_of": ref_rec.get("reference_as_of"),
                "reference_method": ref_rec.get("reference_method"),
                "bid_source_kind": str(effective_bids_snapshot.get("source_kind") or ""),
                "bid_source_url": str(effective_bids_snapshot.get("source_url") or ""),
            }
        )
    bid_adjusted_offsets_df = pd.DataFrame(bid_offset_rows)

    train_cut = max(1, min(8, max(len(target_quarters) - 4, 1)))
    clean_reported_cutoff = date(2025, 3, 31)
    hedge_share_disclosed_map: Dict[date, float] = {
        date(2025, 6, 30): 0.55,
        date(2025, 9, 30): 0.65,
        date(2025, 12, 31): 0.75,
    }
    hedge_share_pattern_map: Dict[date, float] = {
        qd: {
            1: 0.20,
            2: 0.55,
            3: 0.65,
            4: 0.75,
        }.get(((qd.month - 1) // 3) + 1, 0.20)
        for qd in target_quarters
    }
    quarterly_records: List[Dict[str, Any]] = []
    weight_rows: List[Dict[str, Any]] = []
    basis_matrix_rows: List[List[float]] = []
    train_rows_for_optimization: List[int] = []

    for idx, qd in enumerate(target_quarters):
        footprint_rec = dict(footprint_map.get(qd) or {})
        region_counts = dict(footprint_rec.get("region_counts") or {})
        unsupported_regions = dict(footprint_rec.get("unsupported_regions") or {})
        official_component_rec = dict(official_components_by_quarter.get(qd) or {})
        basis_values = {
            region: pd.to_numeric((basis_maps.get(region) or {}).get(qd), errors="coerce")
            for region in relevant_regions
        }
        basis_values_front_loaded = {
            region: pd.to_numeric((basis_maps_front_loaded.get(region) or {}).get(qd), errors="coerce")
            for region in relevant_regions
        }
        basis_vector = [float(basis_values[region]) if pd.notna(basis_values[region]) else np.nan for region in relevant_regions]
        basis_matrix_rows.append(basis_vector)
        baseline_proxy = None
        baseline_proxy_alt = None
        ethanol_num = pd.to_numeric(baseline_ethanol.get(qd), errors="coerce")
        futures_num = pd.to_numeric(baseline_corn_futures.get(qd), errors="coerce")
        gas_num = pd.to_numeric(baseline_natural_gas.get(qd), errors="coerce")
        official_basis_num = pd.to_numeric(official_basis_quarter_map.get(qd), errors="coerce")
        official_ethanol_cov = pd.to_numeric(official_component_rec.get("ethanol_coverage_ratio"), errors="coerce")
        official_basis_cov = pd.to_numeric(official_component_rec.get("basis_coverage_ratio"), errors="coerce")
        gas_burden_per_gal = None
        if pd.notna(gas_num):
            gas_burden_per_gal = gas_usage_mmbtu_per_gal * float(gas_num)
        if pd.notna(ethanol_num) and pd.notna(futures_num) and gas_burden_per_gal is not None:
            baseline_proxy = float(ethanol_num) - (float(futures_num) / yield_per_bushel) - float(gas_burden_per_gal)
            baseline_proxy_alt = float(ethanol_num) - (float(futures_num) / 2.8) - float(gas_burden_per_gal)
        official_simple_proxy = (
            None
            if baseline_proxy is None or pd.isna(official_basis_num)
            else float(baseline_proxy) - (float(official_basis_num) / yield_per_bushel)
        )

        def _covered_weighted_basis(
            weight_map_in: Dict[str, float],
            *,
            basis_lookup: Dict[str, Any],
        ) -> Tuple[Optional[float], float]:
            total_weight = float(sum(max(float(weight_map_in.get(region) or 0.0), 0.0) for region in weight_map_in))
            covered_weight = 0.0
            weighted_sum = 0.0
            for region, raw_weight in weight_map_in.items():
                weight = max(float(raw_weight or 0.0), 0.0)
                val = basis_lookup.get(region)
                if weight <= 0.0 or pd.isna(val):
                    continue
                covered_weight += weight
                weighted_sum += weight * float(val)
            if covered_weight <= 1e-12:
                return None, 0.0 if total_weight <= 1e-12 else 0.0
            coverage_ratio = covered_weight / total_weight if total_weight > 1e-12 else 1.0
            return weighted_sum / covered_weight, coverage_ratio

        equal_weights = {region: 1.0 / float(len(relevant_regions)) for region in relevant_regions}
        plant_count_den = float(sum(region_counts.get(region, 0) for region in relevant_regions))
        plant_count_weights = {
            region: (float(region_counts.get(region, 0)) / plant_count_den) if plant_count_den > 0 else 0.0
            for region in relevant_regions
        }
        capacity_counts = {
            region: float(pd.to_numeric((footprint_rec.get("region_capacities_supported") or {}).get(region), errors="coerce") or 0.0)
            for region in relevant_regions
        }
        capacity_den = float(sum(capacity_counts.get(region, 0.0) for region in relevant_regions))
        capacity_weights = {
            region: (float(capacity_counts.get(region, 0.0)) / capacity_den) if capacity_den > 0 else 0.0
            for region in relevant_regions
        }

        equal_basis, equal_cov = _covered_weighted_basis(equal_weights, basis_lookup=basis_values)
        plant_basis, plant_cov = _covered_weighted_basis(plant_count_weights, basis_lookup=basis_values)
        cap_basis, cap_cov = _covered_weighted_basis(capacity_weights, basis_lookup=basis_values)
        front_loaded_basis, front_loaded_cov = _covered_weighted_basis(plant_count_weights, basis_lookup=basis_values_front_loaded)
        bid_adjusted_basis = None
        bid_adjusted_cov = 0.0
        if bid_offset_map:
            bid_adjusted_lookup = {
                region: (
                    np.nan
                    if pd.isna(basis_values.get(region))
                    else float(basis_values.get(region)) + float(bid_offset_map.get(region, 0.0))
                )
                for region in relevant_regions
            }
            bid_adjusted_basis, bid_adjusted_cov = _covered_weighted_basis(
                plant_count_weights,
                basis_lookup=bid_adjusted_lookup,
            )
        obs_count_q = int(observation_count_map.get(qd, 0) or 0)
        blend_info = _gpre_quarter_open_blend_weights(obs_count_q)
        east_vals = [
            float(val)
            for val in (
                pd.to_numeric(ethanol_illinois_map.get(qd), errors="coerce"),
                pd.to_numeric(ethanol_indiana_map.get(qd), errors="coerce"),
            )
            if pd.notna(val)
        ]
        east_avg = (float(sum(east_vals) / len(east_vals)) if east_vals else None)
        ethanol_geo_spread = (
            None
            if east_avg is None or pd.isna(pd.to_numeric(ethanol_num, errors="coerce"))
            else float(east_avg) - float(ethanol_num)
        )
        ops_info = dict(ops_signal_map.get(qd) or {})
        inventory_info = dict(inventory_signal_map.get(qd) or {})
        sold_gallons_raw = pd.to_numeric(reported_gallons_sold_by_quarter.get(qd), errors="coerce")
        produced_gallons_raw = pd.to_numeric(reported_gallons_produced_by_quarter.get(qd), errors="coerce")

        record: Dict[str, Any] = {
            "quarter": qd,
            "quarter_label": _quarter_label(qd),
            "train_test_flag": "train" if idx < train_cut else "test",
            "split": "train" if idx < train_cut else "test",
            "target_basis": str(evaluation_target_basis_map.get(qd) or "reported"),
            "regime_flags": str(footprint_rec.get("regime_flags") or ""),
            "coverage_notes": "",
            "operating_plant_count": footprint_rec.get("operating_plant_count"),
            "active_regions": str(footprint_rec.get("active_regions") or ""),
            "denominator_policy": str(denominator_policy_by_quarter.get(qd) or ""),
            "reported_ethanol_gallons_sold_raw": (None if pd.isna(sold_gallons_raw) else float(sold_gallons_raw)),
            "reported_ethanol_gallons_produced_raw": (None if pd.isna(produced_gallons_raw) else float(produced_gallons_raw)),
            "distinct_obs_dates_in_quarter": obs_count_q,
            "quarter_progress_bucket": str(blend_info.get("progress_bucket") or ""),
            "quarter_progress_ratio": pd.to_numeric(blend_info.get("coverage_ratio"), errors="coerce"),
            "quarter_open_weight": pd.to_numeric(blend_info.get("quarter_open_weight"), errors="coerce"),
            "current_weight": pd.to_numeric(blend_info.get("current_weight"), errors="coerce"),
            "reported_consolidated_crush_margin_usd_per_gal": pd.to_numeric(reported_margin_by_quarter.get(qd), errors="coerce"),
            "underlying_crush_margin_usd_per_gal": pd.to_numeric((underlying_margin_by_quarter or {}).get(qd), errors="coerce"),
            "target_reported_crush_margin_usd_per_gal": pd.to_numeric(reported_margin_by_quarter.get(qd), errors="coerce"),
            "evaluation_target_margin_usd_per_gal": pd.to_numeric(evaluation_target_map.get(qd), errors="coerce"),
            "ethanol_price_usd_per_gal": (None if pd.isna(ethanol_num) else float(ethanol_num)),
            "weighted_ethanol_benchmark_usd_per_gal": (None if pd.isna(ethanol_num) else float(ethanol_num)),
            "ethanol_east_avg_usd_per_gal": east_avg,
            "ethanol_geo_spread_usd_per_gal": ethanol_geo_spread,
            "cbot_corn_futures_usd_per_bu": (None if pd.isna(futures_num) else float(futures_num)),
            "natural_gas_price_usd_per_mmbtu": (None if pd.isna(gas_num) else float(gas_num)),
            "natural_gas_burden_usd_per_gal": gas_burden_per_gal,
            "baseline_market_proxy_usd_per_gal": baseline_proxy,
            "weighted_ams_basis_usd_per_bu": (None if pd.isna(official_basis_num) else float(official_basis_num)),
            "weighted_ams_basis_cents_per_bu": (None if pd.isna(official_basis_num) else float(official_basis_num) * 100.0),
            "official_simple_proxy_usd_per_gal": official_simple_proxy,
            "simple_market_proxy_usd_per_gal": official_simple_proxy,
            "baseline_alt_market_proxy_usd_per_gal": baseline_proxy_alt,
            "weighted_basis_equal_cents_per_bu": (None if equal_basis is None else float(equal_basis) * 100.0),
            "weighted_basis_equal_usd_per_bu": equal_basis,
            "basis_adjusted_equal_usd_per_gal": (None if baseline_proxy is None or equal_basis is None else float(baseline_proxy) - (float(equal_basis) / yield_per_bushel)),
            "weighted_basis_plant_count_cents_per_bu": (None if plant_basis is None else float(plant_basis) * 100.0),
            "weighted_basis_plant_count_usd_per_bu": plant_basis,
            "basis_adjusted_plant_count_usd_per_gal": (None if baseline_proxy is None or plant_basis is None else float(baseline_proxy) - (float(plant_basis) / yield_per_bushel)),
            "weighted_basis_capacity_cents_per_bu": (None if cap_basis is None else float(cap_basis) * 100.0),
            "weighted_basis_capacity_usd_per_bu": cap_basis,
            "basis_adjusted_capacity_usd_per_gal": (None if baseline_proxy is None or cap_basis is None else float(baseline_proxy) - (float(cap_basis) / yield_per_bushel)),
            "weighted_basis_plant_count_front_loaded_cents_per_bu": (None if front_loaded_basis is None else float(front_loaded_basis) * 100.0),
            "weighted_basis_plant_count_front_loaded_usd_per_bu": front_loaded_basis,
            "basis_adjusted_plant_count_front_loaded_usd_per_gal": (
                None if baseline_proxy is None or front_loaded_basis is None else float(baseline_proxy) - (float(front_loaded_basis) / yield_per_bushel)
            ),
            "weighted_basis_bid_adjusted_offset_cents_per_bu": (None if bid_adjusted_basis is None else float(bid_adjusted_basis) * 100.0),
            "weighted_basis_bid_adjusted_offset_usd_per_bu": bid_adjusted_basis,
            "basis_adjusted_bid_adjusted_offset_usd_per_gal": (
                None if baseline_proxy is None or bid_adjusted_basis is None else float(baseline_proxy) - (float(bid_adjusted_basis) / yield_per_bushel)
            ),
            "official_ethanol_coverage_ratio": (None if pd.isna(official_ethanol_cov) else float(official_ethanol_cov)),
            "official_basis_coverage_ratio": (None if pd.isna(official_basis_cov) else float(official_basis_cov)),
            "unsupported_regions": ", ".join(f"{region}:{count}" for region, count in unsupported_regions.items()),
            "hedge_share_disclosed": hedge_share_disclosed_map.get(qd, 0.0),
            "hedge_share_pattern": hedge_share_pattern_map.get(qd, 0.20),
            "ops_negative_signal_flag": int(ops_info.get("negative_ops_flag") or 0),
            "ops_low_util_flag": int(ops_info.get("low_util_flag") or 0),
            "ops_penalty_usd_per_gal": pd.to_numeric(ops_info.get("ops_penalty_usd_per_gal"), errors="coerce"),
            "ops_utilization_pct": pd.to_numeric(ops_info.get("utilization_pct"), errors="coerce"),
            "ops_signal_terms": ", ".join(str(item) for item in list(ops_info.get("negative_terms") or [])),
            "ops_signal_docs": ", ".join(str(item) for item in list(ops_info.get("source_docs") or [])),
            "inventory_drag_signal_flag": int(inventory_info.get("inventory_drag_signal_flag") or 0),
            "inventory_drag_penalty_usd_per_gal": pd.to_numeric(inventory_info.get("inventory_drag_penalty_usd_per_gal"), errors="coerce"),
            "inventory_drag_terms": ", ".join(str(item) for item in list(inventory_info.get("all_terms") or [])),
            "inventory_terms": ", ".join(str(item) for item in list(inventory_info.get("inventory_terms") or [])),
            "inventory_timing_terms": ", ".join(str(item) for item in list(inventory_info.get("timing_terms") or [])),
            "inventory_signal_docs": ", ".join(str(item) for item in list(inventory_info.get("source_docs") or [])),
        }
        record["official_proxy_usd_per_gal"] = record["official_simple_proxy_usd_per_gal"]
        for region in relevant_regions:
            basis_val = basis_values.get(region)
            record[f"basis_{region}_usd_per_bu"] = None if pd.isna(basis_val) else float(basis_val)
            record[f"basis_{region}_cents_per_bu"] = None if pd.isna(basis_val) else float(basis_val) * 100.0
        notes = []
        if pd.notna(official_ethanol_cov) and float(official_ethanol_cov) < 0.999:
            notes.append(f"official ethanol covered weight={float(official_ethanol_cov):.0%}")
        if pd.notna(official_basis_cov) and float(official_basis_cov) < 0.999:
            notes.append(f"official basis covered weight={float(official_basis_cov):.0%}")
        for component_row in official_component_rec.get("component_rows") or []:
            note_txt = str(component_row.get("fallback_note") or "").strip()
            if note_txt:
                notes.append(f"{str(component_row.get('region_label') or component_row.get('region') or '')}: {note_txt}")
        if plant_cov < 0.999:
            notes.append(f"plant-count covered weight={plant_cov:.0%}")
        if cap_cov < 0.999:
            notes.append(f"capacity covered weight={cap_cov:.0%}")
        if front_loaded_cov < 0.999:
            notes.append(f"front-loaded covered weight={front_loaded_cov:.0%}")
        if bid_offset_map and bid_adjusted_cov < 0.999:
            notes.append(f"bid-adjusted covered weight={bid_adjusted_cov:.0%}")
        record["coverage_notes"] = " | ".join(notes)
        quarterly_records.append(record)

        for model_key, weight_map, note_txt in (
            ("equal_weighted", equal_weights, "Equal-weight relevant regions."),
            ("plant_count_weighted", plant_count_weights, "Plant-count footprint weights across the covered AMS regions."),
            ("capacity_weighted", capacity_weights, "Quarter-aware active-capacity footprint using filing-backed plant capacities."),
            ("plant_count_front_loaded", plant_count_weights, "Same plant-count footprint weights, but basis is front-loaded within the quarter to test early-quarter commercial timing."),
        ):
            for region in relevant_regions:
                weight_rows.append(
                    {
                        "model_key": model_key,
                        "quarter": qd,
                        "region": region,
                        "weight": float(weight_map.get(region, 0.0)),
                        "note": note_txt,
                    }
                )

        for component_row in official_component_rec.get("component_rows") or []:
            weight_rows.append(
                {
                    "model_key": "official_market_model",
                    "quarter": qd,
                    "region": str(component_row.get("region") or ""),
                    "weight": float(component_row.get("weight") or 0.0),
                    "note": str(component_row.get("fallback_note") or "Official simple market/process weighting."),
                }
            )

        if bid_offset_map and idx == 0:
            for region in relevant_regions:
                weight_rows.append(
                    {
                        "model_key": "plant_count_bid_adjusted_offset",
                        "quarter": pd.NaT,
                        "region": region,
                        "weight": float(plant_count_weights.get(region, 0.0)),
                        "note": "Same plant-count footprint weights, but adds static region offsets from current GPRE bids versus the latest AMS reference basis.",
                    }
                )

        if idx < train_cut and baseline_proxy is not None and pd.notna(record["evaluation_target_margin_usd_per_gal"]) and all(pd.notna(x) for x in basis_vector):
            train_rows_for_optimization.append(idx)

    quarterly_df = pd.DataFrame(quarterly_records)
    weights_df = pd.DataFrame(weight_rows)
    basis_matrix = np.asarray(basis_matrix_rows, dtype=float) if basis_matrix_rows else np.empty((0, 0), dtype=float)

    quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"] = quarterly_df["basis_adjusted_plant_count_usd_per_gal"]
    quarterly_df["official_process_proxy_usd_per_gal"] = quarterly_df["official_simple_proxy_usd_per_gal"]

    lagged_plant_basis_map = _shift_quarter_map(
        {
            qd: float(val)
            for qd, val in zip(
                pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date,
                pd.to_numeric(quarterly_df["weighted_basis_plant_count_usd_per_bu"], errors="coerce"),
            )
            if isinstance(qd, date) and pd.notna(val)
        },
        target_quarters,
        lag_quarters=1,
    )
    quarterly_df["weighted_basis_plant_count_prev_quarter_usd_per_bu"] = quarterly_df["quarter"].map(lagged_plant_basis_map)
    quarterly_df["weighted_basis_plant_count_prev_quarter_cents_per_bu"] = quarterly_df["weighted_basis_plant_count_prev_quarter_usd_per_bu"] * 100.0
    quarterly_df["basis_adjusted_plant_count_prev_quarter_usd_per_gal"] = (
        quarterly_df["baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_prev_quarter_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["bridge_baseline_market_proxy_usd_per_gal"] = (
        quarterly_df["ethanol_price_usd_per_gal"]
        - (quarterly_df["cbot_corn_futures_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["approx_market_bridge_proxy_usd_per_gal"] = (
        quarterly_df["bridge_baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["bridge_proxy_front_loaded_usd_per_gal"] = (
        quarterly_df["bridge_baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_front_loaded_usd_per_bu"] / yield_per_bushel)
    )
    current_basis_series = pd.to_numeric(quarterly_df["weighted_basis_plant_count_usd_per_bu"], errors="coerce")
    prev_basis_series = pd.to_numeric(quarterly_df["weighted_basis_plant_count_prev_quarter_usd_per_bu"], errors="coerce")
    quarterly_df["weighted_basis_plant_count_current75_prev25_usd_per_bu"] = np.where(
        current_basis_series.notna() & prev_basis_series.notna(),
        (0.75 * current_basis_series) + (0.25 * prev_basis_series),
        np.where(current_basis_series.notna(), current_basis_series, prev_basis_series),
    )
    quarterly_df["weighted_basis_plant_count_current75_prev25_cents_per_bu"] = quarterly_df["weighted_basis_plant_count_current75_prev25_usd_per_bu"] * 100.0
    quarterly_df["bridge_proxy_current75_prev25_usd_per_gal"] = (
        quarterly_df["bridge_baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_current75_prev25_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["weighted_basis_plant_count_current50_prev50_usd_per_bu"] = np.where(
        current_basis_series.notna() & prev_basis_series.notna(),
        (0.50 * current_basis_series) + (0.50 * prev_basis_series),
        np.where(current_basis_series.notna(), current_basis_series, prev_basis_series),
    )
    quarterly_df["weighted_basis_plant_count_current50_prev50_cents_per_bu"] = quarterly_df["weighted_basis_plant_count_current50_prev50_usd_per_bu"] * 100.0
    quarterly_df["bridge_proxy_current50_prev50_usd_per_gal"] = (
        quarterly_df["bridge_baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_current50_prev50_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["bridge_proxy_current_quarter_avg_usd_per_gal"] = quarterly_df["approx_market_bridge_proxy_usd_per_gal"]
    quarterly_df["bridge_proxy_bid_adjusted_offset_usd_per_gal"] = (
        quarterly_df["bridge_baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_bid_adjusted_offset_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["process_proxy_front_loaded_usd_per_gal"] = quarterly_df["basis_adjusted_plant_count_front_loaded_usd_per_gal"]
    quarterly_df["process_proxy_current75_prev25_usd_per_gal"] = (
        quarterly_df["baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_current75_prev25_usd_per_bu"] / yield_per_bushel)
    )
    quarterly_df["process_proxy_current50_prev50_usd_per_gal"] = (
        quarterly_df["baseline_market_proxy_usd_per_gal"]
        - (quarterly_df["weighted_basis_plant_count_current50_prev50_usd_per_bu"] / yield_per_bushel)
    )
    current_basis_bu_series = pd.to_numeric(quarterly_df["weighted_basis_plant_count_usd_per_bu"], errors="coerce")
    front_loaded_basis_bu_series = pd.to_numeric(
        quarterly_df["weighted_basis_plant_count_front_loaded_usd_per_bu"],
        errors="coerce",
    )
    quarterly_df["weighted_basis_current40_front60_usd_per_bu"] = pd.to_numeric(
        pd.Series(
            [
                _blend_optional_values(
                    current_basis_bu,
                    front_basis_bu,
                    anchor_weight=0.40,
                    current_weight=0.60,
                )
                for current_basis_bu, front_basis_bu in zip(
                    current_basis_bu_series,
                    front_loaded_basis_bu_series,
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_basis_blend_current40_front60_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["baseline_market_proxy_usd_per_gal"], errors="coerce")
        - (pd.to_numeric(quarterly_df["weighted_basis_current40_front60_usd_per_bu"], errors="coerce") / yield_per_bushel)
    )
    quarterly_df["weighted_basis_passthrough_beta35_usd_per_bu"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_basis_passthrough_value(
                    current_basis_bu,
                    front_basis_bu,
                    beta=0.35,
                )
                for current_basis_bu, front_basis_bu in zip(
                    current_basis_bu_series,
                    front_loaded_basis_bu_series,
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_basis_passthrough_beta35_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["baseline_market_proxy_usd_per_gal"], errors="coerce")
        - (pd.to_numeric(quarterly_df["weighted_basis_passthrough_beta35_usd_per_bu"], errors="coerce") / yield_per_bushel)
    )
    quarterly_df["weighted_basis_passthrough_beta65_usd_per_bu"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_basis_passthrough_value(
                    current_basis_bu,
                    front_basis_bu,
                    beta=0.65,
                )
                for current_basis_bu, front_basis_bu in zip(
                    current_basis_bu_series,
                    front_loaded_basis_bu_series,
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_basis_passthrough_beta65_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["baseline_market_proxy_usd_per_gal"], errors="coerce")
        - (pd.to_numeric(quarterly_df["weighted_basis_passthrough_beta65_usd_per_bu"], errors="coerce") / yield_per_bushel)
    )
    quarterly_df["process_capacity_weighted_basis_strict_usd_per_gal"] = pd.to_numeric(
        quarterly_df["basis_adjusted_capacity_usd_per_gal"],
        errors="coerce",
    )
    quarter_date_series = pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date
    bridge_current_map = {
        qd: float(val)
        for qd, val in zip(
            quarter_date_series,
            pd.to_numeric(quarterly_df["approx_market_bridge_proxy_usd_per_gal"], errors="coerce"),
        )
        if isinstance(qd, date) and pd.notna(val)
    }
    bridge_front_map = {
        qd: float(val)
        for qd, val in zip(
            quarter_date_series,
            pd.to_numeric(quarterly_df["bridge_proxy_front_loaded_usd_per_gal"], errors="coerce"),
        )
        if isinstance(qd, date) and pd.notna(val)
    }
    bridge_prev_current_map = _shift_quarter_map(
        bridge_current_map,
        target_quarters,
        lag_quarters=1,
    )
    bridge_prev_front_map = _shift_quarter_map(
        bridge_front_map,
        target_quarters,
        lag_quarters=1,
    )
    process_current_map = {
        qd: float(val)
        for qd, val in zip(
            quarter_date_series,
            pd.to_numeric(quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"], errors="coerce"),
        )
        if isinstance(qd, date) and pd.notna(val)
    }
    process_front_map = {
        qd: float(val)
        for qd, val in zip(
            quarter_date_series,
            pd.to_numeric(quarterly_df["process_proxy_front_loaded_usd_per_gal"], errors="coerce"),
        )
        if isinstance(qd, date) and pd.notna(val)
    }
    process_prev_current_map = _shift_quarter_map(
        process_current_map,
        target_quarters,
        lag_quarters=1,
    )
    process_prev_front_map = _shift_quarter_map(
        process_front_map,
        target_quarters,
        lag_quarters=1,
    )
    quarterly_df["process_quarter_open_anchor_usd_per_gal"] = quarterly_df["quarter"].map(process_prev_front_map)
    quarter_open_weight_series = pd.to_numeric(quarterly_df["quarter_open_weight"], errors="coerce").fillna(0.75)
    current_weight_series = pd.to_numeric(quarterly_df["current_weight"], errors="coerce").fillna(0.25)
    quarterly_df["process_quarter_open_blend_usd_per_gal"] = [
        _blend_optional_values(
            anchor_val,
            current_val,
            anchor_weight=float(anchor_w),
            current_weight=float(current_w),
        )
        for anchor_val, current_val, anchor_w, current_w in zip(
            quarterly_df["process_quarter_open_anchor_usd_per_gal"],
            quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"],
            quarter_open_weight_series,
            current_weight_series,
        )
    ]
    quarterly_df["process_quarter_open_current50_base_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _blend_optional_values(
                    anchor_val,
                    current_val,
                    anchor_weight=0.50,
                    current_weight=0.50,
                )
                for anchor_val, current_val in zip(
                    quarterly_df["process_quarter_open_anchor_usd_per_gal"],
                    quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_front_loaded_ops_penalty_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_proxy_front_loaded_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["ops_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_quarter_open_blend_ops_penalty_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["ops_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    exec_penalty_details = [
        _gpre_execution_penalty_details(
            base_ops_penalty=base_penalty,
            negative_terms=negative_terms,
            utilization_pct=utilization_pct,
        )
        for base_penalty, negative_terms, utilization_pct in zip(
            quarterly_df["ops_penalty_usd_per_gal"],
            quarterly_df["ops_signal_terms"],
            quarterly_df["ops_utilization_pct"],
        )
    ]
    quarterly_df["ops_negative_term_count"] = [int(rec.get("negative_term_count") or 0) for rec in exec_penalty_details]
    quarterly_df["ops_severe_term_flag"] = [int(rec.get("severe_term_flag") or 0) for rec in exec_penalty_details]
    quarterly_df["ops_very_low_util_flag"] = [int(rec.get("very_low_util_flag") or 0) for rec in exec_penalty_details]
    quarterly_df["ops_extra_execution_penalty_usd_per_gal"] = [
        float(rec.get("extra_execution_penalty_usd_per_gal") or 0.0) for rec in exec_penalty_details
    ]
    quarterly_df["ops_total_execution_penalty_usd_per_gal"] = [
        float(rec.get("total_execution_penalty_usd_per_gal") or 0.0) for rec in exec_penalty_details
    ]
    quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["ops_total_execution_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_quarter_open_current50_exec_penalty_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_current50_base_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["ops_total_execution_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["utilization_overlay_penalty_usd_per_gal"] = [
        _gpre_utilization_overlay_penalty(utilization_pct)
        for utilization_pct in quarterly_df["ops_utilization_pct"]
    ]
    maintenance_penalty_details = [
        _gpre_maintenance_delay_penalty_details(negative_terms)
        for negative_terms in quarterly_df["ops_signal_terms"]
    ]
    quarterly_df["maintenance_delay_penalty_usd_per_gal"] = [
        float(rec.get("penalty_usd_per_gal") or 0.0) for rec in maintenance_penalty_details
    ]
    quarterly_df["maintenance_delay_terms"] = [
        ", ".join(str(item) for item in list(rec.get("trigger_terms") or []))
        for rec in maintenance_penalty_details
    ]
    inventory_penalty_details = [
        _gpre_inventory_timing_penalty_details(
            inventory_terms,
            timing_terms,
        )
        for inventory_terms, timing_terms in zip(
            quarterly_df["inventory_terms"],
            quarterly_df["inventory_timing_terms"],
        )
    ]
    quarterly_df["inventory_timing_drag_penalty_usd_per_gal"] = [
        float(rec.get("penalty_usd_per_gal") or 0.0) for rec in inventory_penalty_details
    ]
    quarterly_df["process_quarter_open_blend_utilization_penalty_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["utilization_overlay_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_quarter_open_blend_maintenance_delay_penalty_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["maintenance_delay_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_quarter_open_blend_inventory_timing_drag_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["inventory_timing_drag_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_quarter_open_blend_locked_setup_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_locked_setup_value(
                    base_val,
                    disclosed_share=disclosed_share,
                    pattern_share=pattern_share,
                    quarter_open_anchor=quarter_open_anchor,
                    cap=0.40,
                )
                for base_val, disclosed_share, pattern_share, quarter_open_anchor in zip(
                    quarterly_df["process_quarter_open_blend_usd_per_gal"],
                    quarterly_df["hedge_share_disclosed"],
                    quarterly_df["hedge_share_pattern"],
                    quarterly_df["process_quarter_open_anchor_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["regime_basis_passthrough_beta"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_regime_basis_passthrough_beta(
                    disclosed_share,
                    pattern_share,
                )
                for disclosed_share, pattern_share in zip(
                    quarterly_df["hedge_share_disclosed"],
                    quarterly_df["hedge_share_pattern"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["weighted_basis_regime_passthrough_usd_per_bu"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_basis_passthrough_value(
                    current_basis_bu,
                    front_basis_bu,
                    beta=float(beta_num),
                )
                for current_basis_bu, front_basis_bu, beta_num in zip(
                    current_basis_bu_series,
                    front_loaded_basis_bu_series,
                    quarterly_df["regime_basis_passthrough_beta"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_regime_basis_passthrough_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["baseline_market_proxy_usd_per_gal"], errors="coerce")
        - (pd.to_numeric(quarterly_df["weighted_basis_regime_passthrough_usd_per_bu"], errors="coerce") / yield_per_bushel)
    )
    quarterly_df["realization_residual_penalty_usd_per_gal"] = [
        _gpre_realization_residual_penalty(
            utilization_penalty,
            maintenance_delay_penalty,
            inventory_timing_drag_penalty,
        )
        for utilization_penalty, maintenance_delay_penalty, inventory_timing_drag_penalty in zip(
            quarterly_df["utilization_overlay_penalty_usd_per_gal"],
            quarterly_df["maintenance_delay_penalty_usd_per_gal"],
            quarterly_df["inventory_timing_drag_penalty_usd_per_gal"],
        )
    ]
    quarterly_df["process_two_stage_realization_residual_locked_base_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_locked_setup_value(
                    base_val,
                    disclosed_share=disclosed_share,
                    pattern_share=pattern_share,
                    quarter_open_anchor=quarter_open_anchor,
                    cap=0.25,
                )
                for base_val, disclosed_share, pattern_share, quarter_open_anchor in zip(
                    quarterly_df["process_basis_passthrough_beta35_usd_per_gal"],
                    quarterly_df["hedge_share_disclosed"],
                    quarterly_df["hedge_share_pattern"],
                    quarterly_df["process_quarter_open_anchor_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_two_stage_realization_residual_usd_per_gal"] = (
        pd.to_numeric(
            quarterly_df["process_two_stage_realization_residual_locked_base_usd_per_gal"],
            errors="coerce",
        )
        - pd.to_numeric(quarterly_df["realization_residual_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["sold_minus_produced_gap_raw"] = (
        pd.to_numeric(quarterly_df["reported_ethanol_gallons_sold_raw"], errors="coerce")
        - pd.to_numeric(quarterly_df["reported_ethanol_gallons_produced_raw"], errors="coerce")
    )
    quarterly_df["sold_minus_produced_gap_million"] = (
        pd.to_numeric(quarterly_df["sold_minus_produced_gap_raw"], errors="coerce") / 1_000_000.0
    )
    quarterly_df["sold_minus_produced_gap_ratio"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_sold_minus_produced_gap_ratio(sold_raw, produced_raw)
                for sold_raw, produced_raw in zip(
                    quarterly_df["reported_ethanol_gallons_sold_raw"],
                    quarterly_df["reported_ethanol_gallons_produced_raw"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["inventory_gap_disturbance_score"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_inventory_gap_disturbance_score(gap_ratio)
                for gap_ratio in quarterly_df["sold_minus_produced_gap_ratio"]
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["low_utilization_regime_score"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_low_utilization_regime_score(utilization_pct)
                for utilization_pct in quarterly_df["ops_utilization_pct"]
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["high_utilization_regime_score"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_high_utilization_regime_score(utilization_pct)
                for utilization_pct in quarterly_df["ops_utilization_pct"]
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    locked_setup_evidence_weights: List[float] = []
    for disclosed_share, pattern_share in zip(
        quarterly_df["hedge_share_disclosed"],
        quarterly_df["hedge_share_pattern"],
    ):
        locked_weight, _ = _gpre_effective_hedge_share(
            disclosed_share,
            pattern_share,
            cap=0.35,
        )
        locked_setup_evidence_weights.append(float(locked_weight))
    quarterly_df["locked_setup_evidence_share"] = locked_setup_evidence_weights
    quarterly_df["locked_or_setup_quarter_flag"] = (
        pd.to_numeric(quarterly_df["locked_setup_evidence_share"], errors="coerce").fillna(0.0).gt(0.0)
        | pd.to_numeric(quarterly_df["process_quarter_open_anchor_usd_per_gal"], errors="coerce").notna()
    )
    quarterly_df["disturbed_quarter_flag"] = (
        pd.to_numeric(quarterly_df["maintenance_delay_penalty_usd_per_gal"], errors="coerce").fillna(0.0).gt(0.0)
        | pd.to_numeric(quarterly_df["inventory_timing_drag_penalty_usd_per_gal"], errors="coerce").fillna(0.0).gt(0.0)
        | pd.to_numeric(quarterly_df["sold_minus_produced_gap_ratio"], errors="coerce").abs().fillna(0.0).gt(0.05)
    )
    hard_quarter_reason_series = quarterly_df.apply(_gpre_hard_quarter_reason, axis=1)
    quarterly_df["hard_quarter_reason"] = hard_quarter_reason_series.astype(str)
    quarterly_df["hard_quarter_flag"] = hard_quarter_reason_series.astype(str).str.strip().ne("")
    quarterly_df["inventory_gap_penalty_small_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_inventory_gap_penalty(
                    disturbance_score,
                    cap=0.015,
                )
                for disturbance_score in quarterly_df["inventory_gap_disturbance_score"]
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["inventory_gap_penalty_medium_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_inventory_gap_penalty(
                    disturbance_score,
                    cap=0.028,
                )
                for disturbance_score in quarterly_df["inventory_gap_disturbance_score"]
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_inventory_gap_penalty_small_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["inventory_gap_penalty_small_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_inventory_gap_penalty_medium_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["inventory_gap_penalty_medium_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_utilization_regime_blend_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_utilization_regime_blend_value(
                    base_exec,
                    high_score,
                    low_score,
                    current50_exec,
                    locked_setup_val,
                )
                for base_exec, high_score, low_score, current50_exec, locked_setup_val in zip(
                    pd.to_numeric(quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"], errors="coerce"),
                    pd.to_numeric(quarterly_df["high_utilization_regime_score"], errors="coerce").fillna(0.0),
                    pd.to_numeric(quarterly_df["low_utilization_regime_score"], errors="coerce").fillna(0.0),
                    pd.to_numeric(quarterly_df["process_quarter_open_current50_exec_penalty_usd_per_gal"], errors="coerce"),
                    pd.to_numeric(quarterly_df["process_quarter_open_blend_locked_setup_usd_per_gal"], errors="coerce"),
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_utilization_regime_residual_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_utilization_regime_residual_value(
                    base_exec,
                    low_score,
                    residual_penalty,
                )
                for base_exec, low_score, residual_penalty in zip(
                    quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"],
                    quarterly_df["low_utilization_regime_score"],
                    quarterly_df["realization_residual_penalty_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["extra_low_utilization_drag_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["process_utilization_regime_residual_usd_per_gal"], errors="coerce")
    )
    quarterly_df["exec_inventory_combo_penalty_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_exec_inventory_combo_penalty(
                    maintenance_delay_penalty,
                    disturbance_score,
                )
                for maintenance_delay_penalty, disturbance_score in zip(
                    quarterly_df["maintenance_delay_penalty_usd_per_gal"],
                    quarterly_df["inventory_gap_disturbance_score"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_exec_inventory_combo_medium_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"], errors="coerce")
        - pd.to_numeric(quarterly_df["exec_inventory_combo_penalty_usd_per_gal"], errors="coerce").fillna(0.0)
    )
    quarterly_df["process_asymmetric_basis_passthrough_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_asymmetric_passthrough_value(beta35_value, beta65_value)
                for beta35_value, beta65_value in zip(
                    quarterly_df["process_basis_passthrough_beta35_usd_per_gal"],
                    quarterly_df["process_basis_passthrough_beta65_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_market_process_ensemble_35_65_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _blend_optional_values(
                    official_simple_val,
                    process_exec_val,
                    anchor_weight=0.35,
                    current_weight=0.65,
                )
                for official_simple_val, process_exec_val in zip(
                    quarterly_df["official_simple_proxy_usd_per_gal"],
                    quarterly_df["process_quarter_open_blend_exec_penalty_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["locked_share_for_asymmetric_passthrough"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_effective_hedge_share(disclosed_share, pattern_share, cap=0.35)[0]
                for disclosed_share, pattern_share in zip(
                    quarterly_df["hedge_share_disclosed"],
                    quarterly_df["hedge_share_pattern"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_locked_share_asymmetric_passthrough_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _blend_optional_values(
                    locked_setup_val,
                    asym_val,
                    anchor_weight=float(pd.to_numeric(locked_share, errors="coerce") or 0.0),
                    current_weight=1.0 - float(pd.to_numeric(locked_share, errors="coerce") or 0.0),
                )
                for locked_share, locked_setup_val, asym_val in zip(
                    quarterly_df["locked_share_for_asymmetric_passthrough"],
                    quarterly_df["process_quarter_open_blend_locked_setup_usd_per_gal"],
                    quarterly_df["process_asymmetric_basis_passthrough_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    prior_gap_penalty_small_map = _shift_quarter_map(
        {
            qd: float(val)
            for qd, val in zip(
                pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date,
                pd.to_numeric(quarterly_df["inventory_gap_penalty_small_usd_per_gal"], errors="coerce"),
            )
            if isinstance(qd, date) and pd.notna(val)
        },
        target_quarters,
        lag_quarters=1,
    )
    quarterly_df["prior_inventory_gap_penalty_small_usd_per_gal"] = quarterly_df["quarter"].map(prior_gap_penalty_small_map)
    quarterly_df["process_prior_gap_carryover_small_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_prior_gap_carryover_value(
                    base_val,
                    prior_gap_penalty,
                    multiplier=0.50,
                    cap=0.03,
                )
                for base_val, prior_gap_penalty in zip(
                    quarterly_df["process_quarter_open_blend_usd_per_gal"],
                    quarterly_df["prior_inventory_gap_penalty_small_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    prior_residual_penalty_map = _shift_quarter_map(
        {
            qd: float(val)
            for qd, val in zip(
                pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date,
                pd.to_numeric(quarterly_df["realization_residual_penalty_usd_per_gal"], errors="coerce"),
            )
            if isinstance(qd, date) and pd.notna(val)
        },
        target_quarters,
        lag_quarters=1,
    )
    prior_disturbed_flag_map = _shift_quarter_map(
        {
            qd: 1.0 if bool(flag) else 0.0
            for qd, flag in zip(
                pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date,
                quarterly_df["disturbed_quarter_flag"],
            )
            if isinstance(qd, date)
        },
        target_quarters,
        lag_quarters=1,
    )
    prior_hard_flag_map = _shift_quarter_map(
        {
            qd: 1.0 if bool(flag) else 0.0
            for qd, flag in zip(
                pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date,
                quarterly_df["hard_quarter_flag"],
            )
            if isinstance(qd, date)
        },
        target_quarters,
        lag_quarters=1,
    )
    quarterly_df["prior_realization_residual_penalty_usd_per_gal"] = quarterly_df["quarter"].map(prior_residual_penalty_map)
    quarterly_df["prior_disturbed_quarter_flag"] = (
        pd.to_numeric(quarterly_df["quarter"].map(prior_disturbed_flag_map), errors="coerce").fillna(0.0).gt(0.0)
    )
    quarterly_df["prior_hard_quarter_flag"] = (
        pd.to_numeric(quarterly_df["quarter"].map(prior_hard_flag_map), errors="coerce").fillna(0.0).gt(0.0)
    )
    quarterly_df["process_prior_disturbance_carryover_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_prior_disturbance_carryover_value(
                    base_val,
                    prior_disturbed_flag=prior_disturbed_flag,
                    prior_hard_flag=prior_hard_flag,
                    prior_residual_penalty=prior_residual_penalty,
                    multiplier=0.60,
                    cap=0.04,
                )
                for base_val, prior_disturbed_flag, prior_hard_flag, prior_residual_penalty in zip(
                    quarterly_df["process_basis_passthrough_beta35_usd_per_gal"],
                    quarterly_df["prior_disturbed_quarter_flag"],
                    quarterly_df["prior_hard_quarter_flag"],
                    quarterly_df["prior_realization_residual_penalty_usd_per_gal"],
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    quarterly_df["process_residual_regime_locked_vs_disturbed_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_residual_regime_value(
                    disturbed_flag,
                    locked_flag,
                    disturbed_val,
                    locked_val,
                    normal_val,
                )
                for disturbed_flag, locked_flag, disturbed_val, locked_val, normal_val in zip(
                    quarterly_df["disturbed_quarter_flag"],
                    quarterly_df["locked_or_setup_quarter_flag"],
                    pd.to_numeric(quarterly_df["process_two_stage_realization_residual_usd_per_gal"], errors="coerce"),
                    pd.to_numeric(quarterly_df["process_regime_basis_passthrough_usd_per_gal"], errors="coerce"),
                    pd.to_numeric(quarterly_df["process_basis_passthrough_beta35_usd_per_gal"], errors="coerce"),
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )
    geo_term_series = (
        pd.to_numeric(quarterly_df["ethanol_geo_spread_usd_per_gal"], errors="coerce")
        .mul(0.40)
        .clip(lower=-0.04, upper=0.04)
    )
    quarterly_df["ethanol_geo_term_usd_per_gal"] = geo_term_series
    quarterly_df["process_front_loaded_ethanol_geo_usd_per_gal"] = (
        pd.to_numeric(quarterly_df["process_proxy_front_loaded_usd_per_gal"], errors="coerce")
        + geo_term_series.fillna(0.0)
    )
    quarterly_df["process_gated_incumbent_vs_residual_usd_per_gal"] = pd.to_numeric(
        pd.Series(
            [
                _gpre_gated_model_value(
                    hard_flag,
                    disturbed_flag,
                    residual_val,
                    incumbent_val,
                )
                for hard_flag, disturbed_flag, residual_val, incumbent_val in zip(
                    quarterly_df["hard_quarter_flag"],
                    quarterly_df["disturbed_quarter_flag"],
                    pd.to_numeric(quarterly_df["process_two_stage_realization_residual_usd_per_gal"], errors="coerce"),
                    pd.to_numeric(quarterly_df["bridge_proxy_front_loaded_usd_per_gal"], errors="coerce"),
                )
            ],
            index=quarterly_df.index,
        ),
        errors="coerce",
    )

    def _hedge_memo_series(
        share_map: Dict[date, float],
        *,
        hedged_anchor_map: Dict[date, float],
        spot_series: pd.Series,
    ) -> pd.Series:
        out_vals: List[float] = []
        for qd, spot_val in zip(quarter_date_series, pd.to_numeric(spot_series, errors="coerce")):
            if pd.isna(spot_val):
                out_vals.append(np.nan)
                continue
            hedge_share = float(share_map.get(qd, 0.0) or 0.0) if isinstance(qd, date) else 0.0
            hedge_share = min(max(hedge_share, 0.0), 1.0)
            anchor_val = pd.to_numeric(hedged_anchor_map.get(qd), errors="coerce")
            if hedge_share <= 1e-12 or pd.isna(anchor_val):
                out_vals.append(float(spot_val))
                continue
            out_vals.append((hedge_share * float(anchor_val)) + ((1.0 - hedge_share) * float(spot_val)))
        return pd.Series(out_vals, index=quarterly_df.index, dtype=float)

    quarterly_df["hedge_memo_disclosed_bridge_prior_current_usd_per_gal"] = _hedge_memo_series(
        hedge_share_disclosed_map,
        hedged_anchor_map=bridge_prev_current_map,
        spot_series=quarterly_df["approx_market_bridge_proxy_usd_per_gal"],
    )
    quarterly_df["hedge_memo_disclosed_bridge_prior_front_usd_per_gal"] = _hedge_memo_series(
        hedge_share_disclosed_map,
        hedged_anchor_map=bridge_prev_front_map,
        spot_series=quarterly_df["approx_market_bridge_proxy_usd_per_gal"],
    )
    quarterly_df["hedge_memo_disclosed_process_prior_current_usd_per_gal"] = _hedge_memo_series(
        hedge_share_disclosed_map,
        hedged_anchor_map=process_prev_current_map,
        spot_series=quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"],
    )
    quarterly_df["hedge_memo_disclosed_process_prior_front_usd_per_gal"] = _hedge_memo_series(
        hedge_share_disclosed_map,
        hedged_anchor_map=process_prev_front_map,
        spot_series=quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"],
    )
    quarterly_df["hedge_memo_pattern_bridge_prior_current_usd_per_gal"] = _hedge_memo_series(
        hedge_share_pattern_map,
        hedged_anchor_map=bridge_prev_current_map,
        spot_series=quarterly_df["approx_market_bridge_proxy_usd_per_gal"],
    )
    quarterly_df["hedge_memo_pattern_bridge_prior_front_usd_per_gal"] = _hedge_memo_series(
        hedge_share_pattern_map,
        hedged_anchor_map=bridge_prev_front_map,
        spot_series=quarterly_df["approx_market_bridge_proxy_usd_per_gal"],
    )
    quarterly_df["hedge_memo_pattern_process_prior_current_usd_per_gal"] = _hedge_memo_series(
        hedge_share_pattern_map,
        hedged_anchor_map=process_prev_current_map,
        spot_series=quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"],
    )
    quarterly_df["hedge_memo_pattern_process_prior_front_usd_per_gal"] = _hedge_memo_series(
        hedge_share_pattern_map,
        hedged_anchor_map=process_prev_front_map,
        spot_series=quarterly_df["process_proxy_current_quarter_avg_usd_per_gal"],
    )
    quarterly_df["hedge_memo_disclosed_prior_current_usd_per_gal"] = quarterly_df["hedge_memo_disclosed_bridge_prior_current_usd_per_gal"]
    quarterly_df["hedge_memo_disclosed_prior_front_usd_per_gal"] = quarterly_df["hedge_memo_disclosed_bridge_prior_front_usd_per_gal"]
    quarterly_df["hedge_memo_fill20_prior_current_usd_per_gal"] = quarterly_df["hedge_memo_pattern_bridge_prior_current_usd_per_gal"]
    quarterly_df["hedge_memo_fill20_prior_front_usd_per_gal"] = quarterly_df["hedge_memo_pattern_bridge_prior_front_usd_per_gal"]
    hedge_realization_values: List[Optional[float]] = []
    hedge_realization_weights: List[float] = []
    hedge_realization_sources: List[str] = []
    hedge_realization_refs: List[Optional[float]] = []
    for base_val, disclosed_share, pattern_share, disclosed_ref, pattern_ref in zip(
        quarterly_df["process_quarter_open_blend_usd_per_gal"],
        quarterly_df["hedge_share_disclosed"],
        quarterly_df["hedge_share_pattern"],
        quarterly_df["hedge_memo_disclosed_process_prior_front_usd_per_gal"],
        quarterly_df["hedge_memo_pattern_process_prior_front_usd_per_gal"],
    ):
        hedge_weight, hedge_source = _gpre_effective_hedge_share(disclosed_share, pattern_share, cap=0.35)
        ref_num = pd.to_numeric(disclosed_ref if hedge_source == "disclosed" else pattern_ref, errors="coerce")
        hedge_realization_values.append(
            _gpre_hedge_realization_value(
                base_val,
                disclosed_share=disclosed_share,
                pattern_share=pattern_share,
                disclosed_reference=disclosed_ref,
                pattern_reference=pattern_ref,
                cap=0.35,
            )
        )
        hedge_realization_weights.append(float(hedge_weight))
        hedge_realization_sources.append(str(hedge_source))
        hedge_realization_refs.append(None if pd.isna(ref_num) else float(ref_num))
    quarterly_df["process_quarter_open_blend_hedge_weight"] = hedge_realization_weights
    quarterly_df["process_quarter_open_blend_hedge_source"] = hedge_realization_sources
    quarterly_df["process_quarter_open_blend_hedge_reference_usd_per_gal"] = hedge_realization_refs
    quarterly_df["process_quarter_open_blend_hedge_realization_usd_per_gal"] = pd.to_numeric(
        pd.Series(hedge_realization_values, index=quarterly_df.index),
        errors="coerce",
    )
    for region, weight in plant_count_weights.items():
        weights_df = pd.concat(
            [
                weights_df,
                pd.DataFrame(
                    [
                        {
                            "model_key": "plant_count_prev_quarter",
                            "quarter": pd.NaT,
                            "region": region,
                            "weight": float(weight or 0.0),
                            "note": "Same plant-count footprint weights, but uses the prior quarter's weighted basis as a lagged control.",
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    optimized_weights = np.array([], dtype=float)
    if train_rows_for_optimization and basis_matrix.size:
        train_idx = np.asarray(train_rows_for_optimization, dtype=int)
        train_frame = quarterly_df.iloc[train_idx].copy()
        target_gap = (
            pd.to_numeric(train_frame["evaluation_target_margin_usd_per_gal"], errors="coerce")
            - pd.to_numeric(train_frame["baseline_market_proxy_usd_per_gal"], errors="coerce")
        ).to_numpy(dtype=float)
        optimized_weights = _optimize_basis_weights(basis_matrix[train_idx], target_gap, yield_per_bushel=yield_per_bushel, cap=0.5)
    if optimized_weights.size == len(relevant_regions):
        for region, weight in zip(relevant_regions, optimized_weights):
            weights_df = pd.concat(
                [
                    weights_df,
                    pd.DataFrame(
                        [
                            {
                                "model_key": "optimized_weights",
                                "quarter": pd.NaT,
                                "region": region,
                                "weight": float(weight),
                                "note": "Constrained nonnegative fit on the train split with a 50% per-region cap.",
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
        quarterly_df["weighted_basis_optimized_usd_per_bu"] = basis_matrix @ optimized_weights
        quarterly_df["weighted_basis_optimized_cents_per_bu"] = quarterly_df["weighted_basis_optimized_usd_per_bu"] * 100.0
        quarterly_df["basis_adjusted_optimized_usd_per_gal"] = quarterly_df["baseline_market_proxy_usd_per_gal"] - (quarterly_df["weighted_basis_optimized_usd_per_bu"] / yield_per_bushel)
    else:
        quarterly_df["weighted_basis_optimized_usd_per_bu"] = np.nan
        quarterly_df["weighted_basis_optimized_cents_per_bu"] = np.nan
        quarterly_df["basis_adjusted_optimized_usd_per_gal"] = np.nan

    calibrated_col = "basis_adjusted_calibrated_usd_per_gal"
    quarterly_df[calibrated_col] = np.nan
    calib_train = quarterly_df[
        quarterly_df["evaluation_target_margin_usd_per_gal"].notna()
        & quarterly_df["baseline_alt_market_proxy_usd_per_gal"].notna()
        & quarterly_df["weighted_basis_plant_count_usd_per_bu"].notna()
        & (quarterly_df["split"].astype(str) == "train")
    ].copy()
    if len(calib_train) >= 4:
        x0 = np.ones((len(calib_train), 1), dtype=float)
        x1 = pd.to_numeric(calib_train["baseline_alt_market_proxy_usd_per_gal"], errors="coerce").to_numpy(dtype=float)
        x2 = (pd.to_numeric(calib_train["weighted_basis_plant_count_usd_per_bu"], errors="coerce") / yield_per_bushel).to_numpy(dtype=float)
        X = np.column_stack([x0, x1, x2])
        y = pd.to_numeric(calib_train["evaluation_target_margin_usd_per_gal"], errors="coerce").to_numpy(dtype=float)
        coeffs, *_ = np.linalg.lstsq(X, y, rcond=None)
        all_x1 = pd.to_numeric(quarterly_df["baseline_alt_market_proxy_usd_per_gal"], errors="coerce").to_numpy(dtype=float)
        all_x2 = (pd.to_numeric(quarterly_df["weighted_basis_plant_count_usd_per_bu"], errors="coerce") / yield_per_bushel).to_numpy(dtype=float)
        quarterly_df[calibrated_col] = coeffs[0] + coeffs[1] * all_x1 + coeffs[2] * all_x2
    rolling_pred_map = _rolling_optimized_predictions(
        quarterly_df,
        relevant_regions,
        min_train=min(train_cut, 8),
        yield_per_bushel=yield_per_bushel,
        actual_col="evaluation_target_margin_usd_per_gal",
    )
    quarterly_df["basis_adjusted_optimized_rolling_usd_per_gal"] = quarterly_df["quarter"].map(rolling_pred_map)

    metrics_rows: List[Dict[str, Any]] = []
    metrics_splits: List[Tuple[str, pd.Series]] = [
        ("full", pd.Series(True, index=quarterly_df.index)),
        ("train", quarterly_df["split"].astype(str) == "train"),
        ("test", quarterly_df["split"].astype(str) == "test"),
        (
            "clean_reported_window",
            pd.to_datetime(quarterly_df["quarter"], errors="coerce").dt.date.le(clean_reported_cutoff),
        ),
        (
            "diag_underlying",
            quarterly_df["target_basis"].astype(str).str.lower().eq("underlying"),
        ),
    ]
    base_model_specs: List[Dict[str, Any]] = [
        {"model_key": "simple_market", "pred_col": "simple_market_proxy_usd_per_gal", "family": "simple_market", "family_label": "Simple market", "timing_rule": "Simple market", "eligible_official": False, "preview_supported": False},
        {"model_key": "bridge_current_quarter_avg", "pred_col": "bridge_proxy_current_quarter_avg_usd_per_gal", "family": "bridge_timing", "family_label": "Bridge timing", "timing_rule": "Current qtr avg", "eligible_official": True, "preview_supported": True},
        {"model_key": "bridge_front_loaded", "pred_col": "bridge_proxy_front_loaded_usd_per_gal", "family": "bridge_timing", "family_label": "Bridge timing", "timing_rule": "Front-loaded current", "eligible_official": True, "preview_supported": True},
        {"model_key": "bridge_current75_prev25", "pred_col": "bridge_proxy_current75_prev25_usd_per_gal", "family": "bridge_timing", "family_label": "Bridge timing", "timing_rule": "75/25 current+prior", "eligible_official": True, "preview_supported": True},
        {"model_key": "bridge_current50_prev50", "pred_col": "bridge_proxy_current50_prev50_usd_per_gal", "family": "bridge_timing", "family_label": "Bridge timing", "timing_rule": "50/50 current+prior", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_current_quarter_avg", "pred_col": "process_proxy_current_quarter_avg_usd_per_gal", "family": "process_family", "family_label": "Process", "timing_rule": "Current qtr avg", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_front_loaded", "pred_col": "process_proxy_front_loaded_usd_per_gal", "family": "process_family", "family_label": "Process", "timing_rule": "Front-loaded current", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_current75_prev25", "pred_col": "process_proxy_current75_prev25_usd_per_gal", "family": "process_family", "family_label": "Process", "timing_rule": "75/25 current+prior", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_current50_prev50", "pred_col": "process_proxy_current50_prev50_usd_per_gal", "family": "process_family", "family_label": "Process", "timing_rule": "50/50 current+prior", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_disclosed_bridge_prior_current", "pred_col": "hedge_memo_disclosed_bridge_prior_current_usd_per_gal", "family": "disclosed_hedge_memo", "family_label": "Disclosed hedge memo", "timing_rule": "Prior-current bridge", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_disclosed_bridge_prior_front", "pred_col": "hedge_memo_disclosed_bridge_prior_front_usd_per_gal", "family": "disclosed_hedge_memo", "family_label": "Disclosed hedge memo", "timing_rule": "Prior-front bridge", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_disclosed_process_prior_current", "pred_col": "hedge_memo_disclosed_process_prior_current_usd_per_gal", "family": "disclosed_hedge_memo", "family_label": "Disclosed hedge memo", "timing_rule": "Prior-current process", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_disclosed_process_prior_front", "pred_col": "hedge_memo_disclosed_process_prior_front_usd_per_gal", "family": "disclosed_hedge_memo", "family_label": "Disclosed hedge memo", "timing_rule": "Prior-front process", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_pattern_bridge_prior_current", "pred_col": "hedge_memo_pattern_bridge_prior_current_usd_per_gal", "family": "pattern_hedge_memo", "family_label": "Pattern hedge memo", "timing_rule": "Prior-current bridge", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_pattern_bridge_prior_front", "pred_col": "hedge_memo_pattern_bridge_prior_front_usd_per_gal", "family": "pattern_hedge_memo", "family_label": "Pattern hedge memo", "timing_rule": "Prior-front bridge", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_pattern_process_prior_current", "pred_col": "hedge_memo_pattern_process_prior_current_usd_per_gal", "family": "pattern_hedge_memo", "family_label": "Pattern hedge memo", "timing_rule": "Prior-current process", "eligible_official": True, "preview_supported": True},
        {"model_key": "hedge_pattern_process_prior_front", "pred_col": "hedge_memo_pattern_process_prior_front_usd_per_gal", "family": "pattern_hedge_memo", "family_label": "Pattern hedge memo", "timing_rule": "Prior-front process", "eligible_official": True, "preview_supported": True},
        {"model_key": "bid_adjusted_offset", "pred_col": "bridge_proxy_bid_adjusted_offset_usd_per_gal", "family": "bid_offset", "family_label": "Bid-offset", "timing_rule": "Current qtr avg", "eligible_official": True, "preview_supported": True},
    ]
    new_model_specs: List[Dict[str, Any]] = [
        {"model_key": "process_quarter_open_blend", "pred_col": "process_quarter_open_blend_usd_per_gal", "family": "process_blend", "family_label": "Process blend", "timing_rule": "Quarter-open/current blend", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_quarter_open_blend_ops_penalty", "pred_col": "process_quarter_open_blend_ops_penalty_usd_per_gal", "family": "process_blend_ops_penalty", "family_label": "Process blend + ops penalty", "timing_rule": "Quarter-open/current blend - ops penalty", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_quarter_open_blend_hedge_realization", "pred_col": "process_quarter_open_blend_hedge_realization_usd_per_gal", "family": "process_blend_hedge_realization", "family_label": "Process blend + hedge realization", "timing_rule": "Quarter-open/current blend + capped hedge realization adjustment", "eligible_official": True, "preview_supported": True},
        {
            "model_key": "process_quarter_open_blend_exec_penalty",
            "pred_col": "process_quarter_open_blend_exec_penalty_usd_per_gal",
            "family": "process_blend_exec_penalty",
            "family_label": "Process blend + severe ops penalty",
            "timing_rule": "Quarter-open/current blend - severe execution penalty",
            "eligible_official": True,
            "preview_supported": True,
            "experimental_method_family": "incumbent blend/ops baseline",
            "signal_dependency_note": "Quarter-open/current process blend with the bounded severe execution penalty used as the production baseline.",
        },
        {"model_key": "process_quarter_open_blend_utilization_penalty", "pred_col": "process_quarter_open_blend_utilization_penalty_usd_per_gal", "family": "process_blend_utilization_penalty", "family_label": "Process blend + utilization penalty", "timing_rule": "Quarter-open/current blend - utilization penalty", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_quarter_open_blend_maintenance_delay_penalty", "pred_col": "process_quarter_open_blend_maintenance_delay_penalty_usd_per_gal", "family": "process_blend_maintenance_delay_penalty", "family_label": "Process blend + maintenance delay", "timing_rule": "Quarter-open/current blend - maintenance/delay penalty", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_quarter_open_blend_inventory_timing_drag", "pred_col": "process_quarter_open_blend_inventory_timing_drag_usd_per_gal", "family": "process_blend_inventory_timing_drag", "family_label": "Process blend + inventory timing drag", "timing_rule": "Quarter-open/current blend - inventory/timing drag", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_quarter_open_blend_locked_setup", "pred_col": "process_quarter_open_blend_locked_setup_usd_per_gal", "family": "process_blend_locked_setup", "family_label": "Process blend + locked setup", "timing_rule": "Quarter-open/current blend pulled toward the locked setup", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_basis_blend_current40_front60", "pred_col": "process_basis_blend_current40_front60_usd_per_gal", "family": "basis_blend_current_front", "family_label": "Basis blend 40/60", "timing_rule": "40% current basis + 60% front-loaded basis", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_basis_passthrough_beta35", "pred_col": "process_basis_passthrough_beta35_usd_per_gal", "family": "basis_passthrough_beta", "family_label": "Basis passthrough beta 0.35", "timing_rule": "Front-loaded basis + 35% current basis passthrough", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_basis_passthrough_beta65", "pred_col": "process_basis_passthrough_beta65_usd_per_gal", "family": "basis_passthrough_beta", "family_label": "Basis passthrough beta 0.65", "timing_rule": "Front-loaded basis + 65% current basis passthrough", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_quarter_open_current50_exec_penalty", "pred_col": "process_quarter_open_current50_exec_penalty_usd_per_gal", "family": "quarter_open_current_exec_penalty", "family_label": "Q-open/current 50/50 + exec penalty", "timing_rule": "50/50 quarter-open/current blend - severe execution penalty", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_regime_basis_passthrough", "pred_col": "process_regime_basis_passthrough_usd_per_gal", "family": "regime_basis_passthrough", "family_label": "Regime basis passthrough", "timing_rule": "Front-loaded/current basis with locked-share regime beta", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_two_stage_realization_residual", "pred_col": "process_two_stage_realization_residual_usd_per_gal", "family": "two_stage_realization_residual", "family_label": "Two-stage realization residual", "timing_rule": "Beta-35 basis base + locked setup - bounded realization residual", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_capacity_weighted_basis_strict", "pred_col": "process_capacity_weighted_basis_strict_usd_per_gal", "family": "capacity_weighted_basis_strict", "family_label": "Capacity-weighted basis strict", "timing_rule": "Strict active-capacity-weighted basis process leg", "eligible_official": True, "preview_supported": True},
        {
            "model_key": "process_inventory_gap_penalty_small",
            "pred_col": "process_inventory_gap_penalty_small_usd_per_gal",
            "family": "inventory_gap_penalty",
            "family_label": "Inventory gap penalty small",
            "timing_rule": "Incumbent process execution model - small sold/produced gap penalty",
            "experimental_method_family": "sold minus produced as inventory / timing signal",
            "signal_dependency_note": "Uses the absolute sold-minus-produced gap ratio as a bounded timing-mismatch penalty on top of the incumbent process-execution base.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_inventory_gap_penalty_medium",
            "pred_col": "process_inventory_gap_penalty_medium_usd_per_gal",
            "family": "inventory_gap_penalty",
            "family_label": "Inventory gap penalty medium",
            "timing_rule": "Incumbent process execution model - medium sold/produced gap penalty",
            "experimental_method_family": "sold minus produced as inventory / timing signal",
            "signal_dependency_note": "Uses the same sold-minus-produced timing-mismatch signal with a stronger but still bounded cap.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_utilization_regime_blend",
            "pred_col": "process_utilization_regime_blend_usd_per_gal",
            "family": "utilization_regime",
            "family_label": "Utilization regime blend",
            "timing_rule": "Incumbent process execution base with high-util current tilt and low-util locked/setup tilt",
            "experimental_method_family": "utilization as regime",
            "signal_dependency_note": "Uses bounded high- and low-utilization regime scores to tilt the incumbent process-execution base toward current-process or locked/setup behavior.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_utilization_regime_residual",
            "pred_col": "process_utilization_regime_residual_usd_per_gal",
            "family": "utilization_regime",
            "family_label": "Utilization regime residual",
            "timing_rule": "Incumbent process execution base - extra low-util residual drag",
            "experimental_method_family": "utilization as regime",
            "signal_dependency_note": "Adds extra bounded residual drag only in lower-utilization quarters, while leaving near-full-utilization quarters close to the base model.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_exec_inventory_combo_medium",
            "pred_col": "process_exec_inventory_combo_medium_usd_per_gal",
            "family": "exec_inventory_combo",
            "family_label": "Exec + inventory combo",
            "timing_rule": "Incumbent process execution model - combo penalty when maintenance/outage and inventory-gap evidence align",
            "experimental_method_family": "sold minus produced combined with maintenance / outage",
            "signal_dependency_note": "Only becomes meaningfully active when explicit maintenance/outage evidence and a material sold-produced gap appear together.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_asymmetric_basis_passthrough",
            "pred_col": "process_asymmetric_basis_passthrough_usd_per_gal",
            "family": "asymmetric_basis_passthrough",
            "family_label": "Asymmetric basis passthrough",
            "timing_rule": "Beta-35 base with stronger downside than upside passthrough",
            "experimental_method_family": "asymmetric passthrough",
            "signal_dependency_note": "Starts from the beta-0.35 passthrough and allows negative passthrough deltas to matter more than positive ones.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_market_process_ensemble_35_65",
            "pred_col": "process_market_process_ensemble_35_65_usd_per_gal",
            "family": "market_process_ensemble",
            "family_label": "Market/process ensemble 35/65",
            "timing_rule": "35% official/simple + 65% process q-open severe-execution proxy",
            "experimental_method_family": "market/process ensemble",
            "signal_dependency_note": "Blends the official/simple market row with the hard-quarter-aware process execution winner to test a bounded compromise candidate.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_locked_share_asymmetric_passthrough",
            "pred_col": "process_locked_share_asymmetric_passthrough_usd_per_gal",
            "family": "locked_share_asymmetric",
            "family_label": "Locked-share asymmetric passthrough",
            "timing_rule": "Locked-setup share blended with asymmetric passthrough",
            "experimental_method_family": "locked-share asymmetric passthrough",
            "signal_dependency_note": "Uses capped locked-share evidence to blend locked/setup behavior with asymmetric basis passthrough that is already available in preview mode.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_prior_gap_carryover_small",
            "pred_col": "process_prior_gap_carryover_small_usd_per_gal",
            "family": "prior_gap_carryover",
            "family_label": "Prior-gap carryover small",
            "timing_rule": "Quarter-open/current blend - bounded prior inventory-gap carryover",
            "experimental_method_family": "prior-gap carryover",
            "signal_dependency_note": "Carries forward half of the prior quarter's small sold-produced gap penalty with a tight cap, without relying on same-quarter ex-post realization signals.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_prior_disturbance_carryover",
            "pred_col": "process_prior_disturbance_carryover_usd_per_gal",
            "family": "prior_disturbance_carryover",
            "family_label": "Prior-disturbance carryover",
            "timing_rule": "Beta-35 base - bounded prior disturbed-quarter carryover",
            "experimental_method_family": "prior-disturbance carryover",
            "signal_dependency_note": "Starts from the simple beta-0.35 passthrough and only adds bounded carryover drag when the prior quarter was hard/disturbed.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_residual_regime_locked_vs_disturbed",
            "pred_col": "process_residual_regime_locked_vs_disturbed_usd_per_gal",
            "family": "residual_regime",
            "family_label": "Residual regime split",
            "timing_rule": "Disturbed quarters use residual model, locked/setup quarters use regime passthrough, normal quarters use beta-35",
            "experimental_method_family": "residual model by quarter regime",
            "signal_dependency_note": "Uses explicit disturbed-quarter and locked/setup regime flags to switch between the residual, regime-passthrough, and normal basis-passthrough families.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {
            "model_key": "process_gated_incumbent_vs_residual",
            "pred_col": "process_gated_incumbent_vs_residual_usd_per_gal",
            "family": "gated_ensemble",
            "family_label": "Gated incumbent vs residual",
            "timing_rule": "Hard/disturbed quarters use residual challenger, otherwise keep bridge front-loaded winner",
            "experimental_method_family": "ensemble / gated model between strong families",
            "signal_dependency_note": "Explicitly gates between the current production winner and the two-stage residual challenger using hard-quarter and disturbed-quarter flags.",
            "eligible_official": True,
            "preview_supported": True,
        },
        {"model_key": "process_front_loaded_ops_penalty", "pred_col": "process_front_loaded_ops_penalty_usd_per_gal", "family": "process_ops_penalty", "family_label": "Process + ops penalty", "timing_rule": "Front-loaded current - ops penalty", "eligible_official": True, "preview_supported": True},
        {"model_key": "process_front_loaded_ethanol_geo", "pred_col": "process_front_loaded_ethanol_geo_usd_per_gal", "family": "process_geo", "family_label": "Process + ethanol geo", "timing_rule": "Front-loaded current + east spread", "eligible_official": True, "preview_supported": True},
    ]
    model_specs: List[Dict[str, Any]] = [*base_model_specs, *new_model_specs]
    model_key_to_spec = {str(spec["model_key"]): spec for spec in model_specs}
    model_key_to_pred_col = {
        str(spec.get("model_key") or ""): str(spec.get("pred_col") or "")
        for spec in model_specs
        if str(spec.get("model_key") or "").strip()
    }
    experimental_candidate_keys = {
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
    }
    for spec in model_specs:
        model_key = str(spec["model_key"] or "")
        pred_col = str(spec["pred_col"] or "")
        for split_name, split_mask in metrics_splits:
            metric_row = _metrics_for_prediction(
                quarterly_df[split_mask].copy(),
                pred_col,
                label=model_key,
                split=split_name,
                actual_col="evaluation_target_margin_usd_per_gal",
            )
            metric_row["family"] = str(spec.get("family") or "")
            metric_row["family_label"] = str(spec.get("family_label") or "")
            metric_row["timing_rule"] = str(spec.get("timing_rule") or "")
            metrics_rows.append(metric_row)
    metrics_df = pd.DataFrame(metrics_rows)

    recommended_model_key = "plant_count_weighted"
    model_cols = {
        "equal_weighted": ("weighted_basis_equal_cents_per_bu", "weighted_basis_equal_usd_per_bu", "basis_adjusted_equal_usd_per_gal"),
        "plant_count_weighted": ("weighted_basis_plant_count_cents_per_bu", "weighted_basis_plant_count_usd_per_bu", "basis_adjusted_plant_count_usd_per_gal"),
        "capacity_weighted": ("weighted_basis_capacity_cents_per_bu", "weighted_basis_capacity_usd_per_bu", "basis_adjusted_capacity_usd_per_gal"),
        "optimized_weights": ("weighted_basis_optimized_cents_per_bu", "weighted_basis_optimized_usd_per_bu", "basis_adjusted_optimized_usd_per_gal"),
    }
    chosen_cents_col, chosen_usd_col, chosen_proxy_col = model_cols.get(recommended_model_key, model_cols["plant_count_weighted"])
    quarterly_df["recommended_model_key"] = recommended_model_key
    quarterly_df["weighted_basis_recommended_cents_per_bu"] = quarterly_df[chosen_cents_col]
    quarterly_df["weighted_basis_recommended_usd_per_bu"] = quarterly_df[chosen_usd_col]
    quarterly_df["basis_adjusted_recommended_usd_per_gal"] = quarterly_df[chosen_proxy_col]

    def _metrics_pick(model_key_in: str, split_in: str, field_in: str) -> float:
        sub = metrics_df[
            (metrics_df["model_key"].astype(str) == str(model_key_in or ""))
            & (metrics_df["split"].astype(str) == str(split_in or ""))
        ].copy()
        if sub.empty:
            return float("nan")
        return float(pd.to_numeric(sub.iloc[0].get(field_in), errors="coerce"))

    family_preference = {
        "process_blend": 0,
        "process_blend_ops_penalty": 1,
        "process_blend_hedge_realization": 2,
        "process_blend_exec_penalty": 3,
        "inventory_gap_penalty": 4,
        "utilization_regime": 5,
        "exec_inventory_combo": 6,
        "asymmetric_basis_passthrough": 7,
        "market_process_ensemble": 8,
        "locked_share_asymmetric": 9,
        "prior_gap_carryover": 10,
        "prior_disturbance_carryover": 11,
        "residual_regime": 12,
        "gated_ensemble": 13,
        "process_blend_utilization_penalty": 14,
        "process_blend_maintenance_delay_penalty": 15,
        "process_blend_inventory_timing_drag": 16,
        "process_blend_locked_setup": 17,
        "process_family": 18,
        "process_geo": 19,
        "process_ops_penalty": 20,
        "bridge_timing": 21,
        "disclosed_hedge_memo": 22,
        "pattern_hedge_memo": 23,
        "bid_offset": 24,
        "simple_market": 25,
    }
    new_candidate_keys = set(str(item or "") for item in experimental_candidate_keys if str(item or "").strip())
    incumbent_baseline_leaderboard_df = _build_gpre_proxy_leaderboard(
        quarterly_df,
        metrics_df,
        base_model_specs,
        incumbent_baseline_model_key="",
        process_comparator_model_key="process_front_loaded",
        new_candidate_keys=new_candidate_keys,
    )
    incumbent_baseline_leaderboard_df = _annotate_gpre_selection_guardrails(
        incumbent_baseline_leaderboard_df,
        clean_mae_slack=0.010,
        q1_mae_slack=0.015,
        q1_bias_limit=0.050,
    )
    incumbent_baseline_model_key, incumbent_selection_guard_reason = _select_gpre_proxy_model_from_leaderboard(
        incumbent_baseline_leaderboard_df,
        family_preference=family_preference,
        clean_mae_slack=0.010,
        q1_mae_slack=0.015,
        q1_bias_limit=0.050,
    )
    incumbent_baseline_leaderboard_df["chosen"] = incumbent_baseline_leaderboard_df["model_key"].astype(str) == incumbent_baseline_model_key
    incumbent_baseline_leaderboard_df["expanded_best_candidate"] = False
    incumbent_baseline_leaderboard_df["production_winner"] = incumbent_baseline_leaderboard_df["chosen"].astype(bool)
    incumbent_baseline_leaderboard_df["promotion_guard_pass"] = incumbent_baseline_leaderboard_df["chosen"].astype(bool)
    incumbent_baseline_leaderboard_df["promotion_guard_reason"] = np.where(
        incumbent_baseline_leaderboard_df["chosen"].astype(bool),
        "incumbent_baseline",
        "not_applicable_base_pass",
    )
    incumbent_baseline_leaderboard_df["promotion_guard_failures"] = ""
    incumbent_baseline_leaderboard_df["incremental_value_status"] = [
        _gpre_incremental_value_status(
            rec.get("avg_abs_diff_vs_official"),
            rec.get("diff_quarters_gt_0_02_vs_official"),
        )
        for rec in incumbent_baseline_leaderboard_df.to_dict("records")
    ]
    leaderboard_df = _build_gpre_proxy_leaderboard(
        quarterly_df,
        metrics_df,
        model_specs,
        incumbent_baseline_model_key=incumbent_baseline_model_key,
        process_comparator_model_key="process_front_loaded",
        new_candidate_keys=new_candidate_keys,
    )
    leaderboard_df = _annotate_gpre_selection_guardrails(
        leaderboard_df,
        clean_mae_slack=0.010,
        q1_mae_slack=0.015,
        q1_bias_limit=0.050,
    )
    expanded_ranked_df = _rank_gpre_proxy_models(
        leaderboard_df,
        family_preference=family_preference,
        require_selection_pass=False,
    )
    expanded_best_candidate_model_key = (
        str(expanded_ranked_df.iloc[0]["model_key"] or "")
        if not expanded_ranked_df.empty
        else str(incumbent_baseline_model_key or "process_current_quarter_avg")
    )
    expanded_candidate_model_key = expanded_best_candidate_model_key
    leaderboard_df, gpre_proxy_model_key, promotion_guard_reason = _annotate_gpre_promotion_guardrails(
        leaderboard_df,
        incumbent_baseline_model_key=incumbent_baseline_model_key,
        expanded_best_candidate_model_key=expanded_best_candidate_model_key,
        new_candidate_keys=new_candidate_keys,
    )
    leaderboard_df["chosen"] = leaderboard_df["production_winner"].astype(bool)
    chosen_row = leaderboard_df[leaderboard_df["production_winner"]].iloc[0].to_dict() if not leaderboard_df[leaderboard_df["production_winner"]].empty else {}
    incumbent_row = incumbent_baseline_leaderboard_df[
        incumbent_baseline_leaderboard_df["model_key"].astype(str) == incumbent_baseline_model_key
    ].iloc[0].to_dict() if not incumbent_baseline_leaderboard_df[incumbent_baseline_leaderboard_df["model_key"].astype(str) == incumbent_baseline_model_key].empty else {}
    process_comparator_row = leaderboard_df[
        leaderboard_df["model_key"].astype(str) == "process_front_loaded"
    ].iloc[0].to_dict() if not leaderboard_df[leaderboard_df["model_key"].astype(str) == "process_front_loaded"].empty else {}
    expanded_best_row = leaderboard_df[
        leaderboard_df["model_key"].astype(str) == expanded_best_candidate_model_key
    ].iloc[0].to_dict() if not leaderboard_df[leaderboard_df["model_key"].astype(str) == expanded_best_candidate_model_key].empty else {}
    experimental_signal_audit = _gpre_experimental_signal_audit(quarterly_df)
    experimental_candidate_comparison_df = _gpre_experimental_candidate_comparison(
        quarterly_df,
        leaderboard_df,
        incumbent_model_key=str(incumbent_baseline_model_key or gpre_proxy_model_key),
        experimental_candidate_keys=experimental_candidate_keys,
    )

    def _pick_best_role_row(
        frame_in: pd.DataFrame,
        *,
        role_name: str,
    ) -> Dict[str, Any]:
        if frame_in is None or frame_in.empty:
            return {}
        sub = frame_in.copy()
        sub["complexity_rank"] = sub["complexity_rating"].astype(str).map({"low": 0, "moderate": 1, "high": 2}).fillna(9)
        if role_name == "historical":
            sub = sub[sub["eligible_official"] == True].copy()
            sort_cols = ["clean_mae", "hybrid_score", "walk_forward_tail_mae", "complexity_rank", "model_key"]
        elif role_name == "compromise":
            sub = sub[
                (sub["preview_supported"] == True)
                & sub["forward_usability_rating"].astype(str).ne("low")
                & sub["complexity_rating"].astype(str).ne("high")
            ].copy()
            sort_cols = ["hybrid_score", "hard_quarter_mae", "walk_forward_tail_mae", "clean_mae", "complexity_rank", "model_key"]
        elif role_name == "forward":
            sub = sub[
                (sub["preview_supported"] == True)
                & sub["forward_usability_rating"].astype(str).eq("high")
            ].copy()
            sort_cols = ["walk_forward_tail_mae", "hybrid_score", "complexity_rank", "clean_mae", "model_key"]
        else:
            return {}
        sub = sub[sub["model_key"].astype(str).str.strip().ne("")].copy()
        if sub.empty:
            return {}
        return sub.sort_values(sort_cols, na_position="last").iloc[0].to_dict()

    best_historical_fit_row = _pick_best_role_row(leaderboard_df, role_name="historical")
    best_compromise_row = _pick_best_role_row(leaderboard_df, role_name="compromise")
    best_forward_lens_row = _pick_best_role_row(leaderboard_df, role_name="forward")
    if not best_historical_fit_row and isinstance(leaderboard_df, pd.DataFrame) and not leaderboard_df.empty:
        hist_fallback = leaderboard_df[pd.to_numeric(leaderboard_df.get("clean_mae"), errors="coerce").notna()].copy()
        if not hist_fallback.empty:
            best_historical_fit_row = hist_fallback.sort_values(["clean_mae", "hybrid_score"], na_position="last").iloc[0].to_dict()
    if not best_compromise_row and isinstance(leaderboard_df, pd.DataFrame) and not leaderboard_df.empty:
        compromise_fallback = leaderboard_df[leaderboard_df["preview_supported"] == True].copy()
        if not compromise_fallback.empty:
            best_compromise_row = compromise_fallback.sort_values(["hybrid_score", "clean_mae"], na_position="last").iloc[0].to_dict()
    if not best_forward_lens_row:
        best_forward_lens_row = dict(best_compromise_row or best_historical_fit_row or {})
    best_historical_fit_model_key = str(best_historical_fit_row.get("model_key") or "")
    best_compromise_model_key = str(best_compromise_row.get("model_key") or "")
    best_forward_lens_model_key = str(best_forward_lens_row.get("model_key") or "")
    production_decision_story, selection_vs_promotion_explanation = _gpre_selection_vs_promotion_story(
        incumbent_baseline_model_key=incumbent_baseline_model_key,
        expanded_best_row=expanded_best_row,
        production_winner_key=gpre_proxy_model_key,
    )
    chosen_spec = dict(model_key_to_spec.get(gpre_proxy_model_key) or {})
    gpre_proxy_pred_col = str(chosen_spec.get("pred_col") or "process_proxy_current_quarter_avg_usd_per_gal")
    quarterly_df["gpre_proxy_model_key"] = gpre_proxy_model_key
    quarterly_df["gpre_proxy_family"] = str(chosen_spec.get("family") or chosen_row.get("family") or "")
    quarterly_df["gpre_proxy_family_label"] = str(chosen_spec.get("family_label") or chosen_row.get("family_label") or "")
    quarterly_df["gpre_proxy_timing_rule"] = str(chosen_spec.get("timing_rule") or chosen_row.get("timing_rule") or "")
    quarterly_df["gpre_proxy_clean_mae"] = float(pd.to_numeric(chosen_row.get("clean_mae"), errors="coerce"))
    quarterly_df["gpre_proxy_underlying_mae"] = float(pd.to_numeric(chosen_row.get("underlying_mae"), errors="coerce"))
    quarterly_df["gpre_proxy_hybrid_score"] = float(pd.to_numeric(chosen_row.get("hybrid_score"), errors="coerce"))
    quarterly_df["gpre_proxy_selection_guard_reason"] = str(chosen_row.get("selection_guard_reason") or "")
    quarterly_df["gpre_proxy_selection_guard_pass"] = bool(chosen_row.get("selection_guard_pass"))
    quarterly_df["gpre_proxy_promotion_guard_reason"] = str(promotion_guard_reason or "")
    quarterly_df["gpre_proxy_promotion_guard_pass"] = bool(chosen_row.get("promotion_guard_pass"))
    quarterly_df["gpre_proxy_expanded_best_candidate_model_key"] = str(expanded_best_candidate_model_key or "")
    quarterly_df["gpre_proxy_production_winner_model_key"] = str(gpre_proxy_model_key or "")
    quarterly_df["gpre_proxy_best_historical_fit_model_key"] = str(best_historical_fit_model_key or "")
    quarterly_df["gpre_proxy_best_compromise_model_key"] = str(best_compromise_model_key or "")
    quarterly_df["gpre_proxy_best_forward_lens_model_key"] = str(best_forward_lens_model_key or "")
    quarterly_df["gpre_proxy_production_decision_story"] = str(production_decision_story or "")
    quarterly_df["gpre_proxy_selection_vs_promotion_explanation"] = str(selection_vs_promotion_explanation or "")
    quarterly_df["gpre_proxy_official_usd_per_gal"] = pd.to_numeric(quarterly_df[gpre_proxy_pred_col], errors="coerce")
    quarterly_df["bridge_official_model_key"] = gpre_proxy_model_key
    quarterly_df["bridge_official_proxy_usd_per_gal"] = quarterly_df["gpre_proxy_official_usd_per_gal"]
    quarterly_df["official_proxy_usd_per_gal"] = quarterly_df["official_simple_proxy_usd_per_gal"]

    quarterly_df["simple_market_proxy_error_usd_per_gal"] = quarterly_df["simple_market_proxy_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bridge_baseline_error_usd_per_gal"] = quarterly_df["bridge_baseline_market_proxy_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bridge_front_loaded_error_usd_per_gal"] = quarterly_df["bridge_proxy_front_loaded_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bridge_current75_prev25_error_usd_per_gal"] = quarterly_df["bridge_proxy_current75_prev25_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bridge_current50_prev50_error_usd_per_gal"] = quarterly_df["bridge_proxy_current50_prev50_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bridge_bid_adjusted_offset_error_usd_per_gal"] = quarterly_df["bridge_proxy_bid_adjusted_offset_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["hedge_memo_disclosed_prior_current_error_usd_per_gal"] = quarterly_df["hedge_memo_disclosed_bridge_prior_current_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["hedge_memo_disclosed_prior_front_error_usd_per_gal"] = quarterly_df["hedge_memo_disclosed_bridge_prior_front_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["hedge_memo_fill20_prior_current_error_usd_per_gal"] = quarterly_df["hedge_memo_pattern_bridge_prior_current_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["hedge_memo_fill20_prior_front_error_usd_per_gal"] = quarterly_df["hedge_memo_pattern_bridge_prior_front_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["process_proxy_error_usd_per_gal"] = quarterly_df["official_process_proxy_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["gpre_proxy_error_usd_per_gal"] = quarterly_df["gpre_proxy_official_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bridge_official_error_usd_per_gal"] = quarterly_df["gpre_proxy_error_usd_per_gal"]
    quarterly_df["baseline_error_usd_per_gal"] = quarterly_df["simple_market_proxy_error_usd_per_gal"]
    quarterly_df["recommended_error_usd_per_gal"] = quarterly_df["gpre_proxy_error_usd_per_gal"]
    quarterly_df["front_loaded_error_usd_per_gal"] = quarterly_df["process_proxy_front_loaded_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["prev_quarter_error_usd_per_gal"] = quarterly_df["basis_adjusted_plant_count_prev_quarter_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["bid_adjusted_offset_error_usd_per_gal"] = quarterly_df["basis_adjusted_bid_adjusted_offset_usd_per_gal"] - quarterly_df["evaluation_target_margin_usd_per_gal"]
    quarterly_df["calibrated_error_usd_per_gal"] = quarterly_df[calibrated_col] - quarterly_df["evaluation_target_margin_usd_per_gal"]

    latest_official_quarter = max(official_components_by_quarter) if official_components_by_quarter else None
    latest_official_components = dict(official_components_by_quarter.get(latest_official_quarter) or {}) if isinstance(latest_official_quarter, date) else {}
    latest_official_label = _quarter_label(latest_official_quarter) if isinstance(latest_official_quarter, date) else ""
    current_official_basis_payload = _gpre_official_current_forward_basis_payload(
        row_list,
        target_date=as_of_date if isinstance(as_of_date, date) else date.today(),
        target_quarter_end=quarter_end_from_date(as_of_date if isinstance(as_of_date, date) else date.today()),
        as_of_date=as_of_date,
        ticker_root=ticker_root,
        bids_snapshot=effective_bids_snapshot,
        plant_capacity_history=effective_plant_capacity_history,
    )
    next_target_start, next_target_end = next_calendar_quarter_bounds(as_of_date=as_of_date)
    next_official_basis_payload = _gpre_official_current_forward_basis_payload(
        row_list,
        target_date=_quarter_midpoint(next_target_start, next_target_end),
        target_quarter_end=next_target_end,
        as_of_date=as_of_date,
        ticker_root=ticker_root,
        bids_snapshot=effective_bids_snapshot,
        plant_capacity_history=effective_plant_capacity_history,
    )
    official_market_rows = list(latest_official_components.get("component_rows") or [])
    official_market_summary = (
        f"Official market model | Representative quarter: {latest_official_label}."
        if latest_official_label
        else "Official market model."
    )
    production_formula = _gpre_production_formula_text(
        recommended_model_key,
        weights_df,
        yield_per_bushel=yield_per_bushel,
        natural_gas_usage_btu_per_gal=natural_gas_usage_num,
    )
    simple_market_test_mae = _metrics_pick("simple_market", "test", "mae")
    gpre_proxy_test_mae = _metrics_pick(gpre_proxy_model_key, "test", "mae")
    chosen_top_miss = str(chosen_row.get("top_miss_quarters") or "").strip()
    preview_accuracy_by_model = {
        str(rec.get("model_key") or ""): dict(rec.get("preview_accuracy") or {})
        for rec in leaderboard_df.to_dict("records")
        if str(rec.get("model_key") or "").strip()
    }

    def _recent_quarter_comp_records() -> pd.DataFrame:
        if quarterly_df is None or quarterly_df.empty:
            return pd.DataFrame()
        sub = quarterly_df.copy()
        sub["quarter_ts"] = pd.to_datetime(sub.get("quarter"), errors="coerce")
        sub["target_num"] = pd.to_numeric(sub.get("evaluation_target_margin_usd_per_gal"), errors="coerce")
        sub = sub[sub["quarter_ts"].notna() & sub["target_num"].notna()].copy()
        if sub.empty:
            return pd.DataFrame()
        sub = sub.sort_values("quarter_ts").tail(4).copy()

        def _pred_val(rec: Dict[str, Any], model_key_in: str) -> Any:
            pred_col_in = str(model_key_to_pred_col.get(str(model_key_in or ""), "") or "")
            if not pred_col_in:
                return None
            return pd.to_numeric(rec.get(pred_col_in), errors="coerce")

        records: List[Dict[str, Any]] = []
        for rec in sub.to_dict("records"):
            winner_val = _pred_val(rec, gpre_proxy_model_key)
            target_val = pd.to_numeric(rec.get("evaluation_target_margin_usd_per_gal"), errors="coerce")
            winner_err = None
            if pd.notna(winner_val) and pd.notna(target_val):
                winner_err = float(winner_val) - float(target_val)
            hard_reason = str(rec.get("hard_quarter_reason") or "").strip()
            records.append(
                {
                    "Quarter": _quarter_label(pd.Timestamp(rec["quarter_ts"]).date()),
                    "Official": pd.to_numeric(rec.get("official_simple_proxy_usd_per_gal"), errors="coerce"),
                    "Incumbent": _pred_val(rec, incumbent_baseline_model_key),
                    "Process comp": _pred_val(rec, "process_front_loaded"),
                    "Expanded best": _pred_val(rec, expanded_best_candidate_model_key),
                    "Winner": winner_val,
                    "Target": target_val,
                    "Winner err": winner_err,
                    "Hard?": ("Yes: " + hard_reason) if str(hard_reason).strip() else "No",
                }
            )
        return pd.DataFrame(records)

    recent_quarter_comparison_df = _recent_quarter_comp_records()
    hedge_style_study = _build_gpre_hedge_style_study(quarterly_df)
    hedge_candidate_leaderboard_df = (
        hedge_style_study.get("candidate_leaderboard_df")
        if isinstance(hedge_style_study.get("candidate_leaderboard_df"), pd.DataFrame)
        else pd.DataFrame()
    )
    hedge_quarter_fit_df = (
        hedge_style_study.get("quarter_fit_df")
        if isinstance(hedge_style_study.get("quarter_fit_df"), pd.DataFrame)
        else pd.DataFrame()
    )
    hedge_best_style_label = str(hedge_style_study.get("best_overall_style_label") or "")
    hedge_best_family_label = str(hedge_style_study.get("best_overall_style_family_label") or "")
    hedge_window_quarters = [str(item or "").strip() for item in list(hedge_style_study.get("backtest_window_quarters") or []) if str(item or "").strip()]
    hedge_window_display = str(hedge_style_study.get("backtest_window_display") or "").strip()
    hedge_usable_quarter_count = int(len(hedge_quarter_fit_df)) if isinstance(hedge_quarter_fit_df, pd.DataFrame) else 0
    hedge_weak_fit_quarters = [str(item or "").strip() for item in list(hedge_style_study.get("weak_fit_quarters") or []) if str(item or "").strip()]
    hedge_target_label = str(hedge_style_study.get("target_label") or "Reported consolidated crush margin ($/gal)")
    hedge_target_definition = str(hedge_style_study.get("target_definition") or hedge_target_label).strip()
    hedge_style_vs_family_explanation = str(hedge_style_study.get("best_style_vs_family_explanation") or "").strip()
    hedge_diagnostic_only_note = str(hedge_style_study.get("diagnostic_only_note") or "").strip()

    def _fmt_metric(value: Any, *, digits: int = 4) -> str:
        num = pd.to_numeric(value, errors="coerce")
        return "n/a" if pd.isna(num) else f"{float(num):.{digits}f}"

    def _fmt_row_summary(rec: Dict[str, Any]) -> str:
        if not rec:
            return "n/a"
        return (
            f"{str(rec.get('model_key') or 'n/a')} | clean {_fmt_metric(rec.get('clean_mae'))} | "
            f"underlying {_fmt_metric(rec.get('underlying_mae'))} | hybrid {_fmt_metric(rec.get('hybrid_score'))} | "
            f"mean error {_fmt_metric(rec.get('full_mean_error'))} | sign {_fmt_metric(pd.to_numeric(rec.get('test_sign_hit_rate'), errors='coerce') * 100.0, digits=1)}% | "
            f"avg abs diff vs official {_fmt_metric(rec.get('avg_abs_diff_vs_official'))} | "
            f"hard-quarter MAE {_fmt_metric(rec.get('hard_quarter_mae'))} | "
            f"preview {_fmt_metric(rec.get('live_preview_mae'))} / {str(rec.get('live_preview_quality_status') or 'n/a')}"
        )

    expanded_selection_status = "pass" if bool(expanded_best_row.get("selection_guard_pass")) else "fail"
    expanded_promotion_status = "pass" if bool(expanded_best_row.get("promotion_guard_pass")) else "fail"
    if gpre_proxy_model_key == incumbent_baseline_model_key and expanded_best_candidate_model_key == incumbent_baseline_model_key:
        guardrail_summary = (
            f"Expanded-pass best candidate matched incumbent `{incumbent_baseline_model_key}`. "
            f"Production winner stayed `{gpre_proxy_model_key}`."
        )
    elif gpre_proxy_model_key == incumbent_baseline_model_key and expanded_best_candidate_model_key:
        guardrail_summary = (
            f"`{expanded_best_candidate_model_key}` not promoted because "
            f"{_gpre_guard_reason_human(promotion_guard_reason)}."
        )
    else:
        guardrail_summary = (
            f"`{gpre_proxy_model_key}` promoted over `{incumbent_baseline_model_key}` after passing "
            f"promotion guardrails."
        )
    clean_best = (
        metrics_df[metrics_df["split"].astype(str) == "clean_reported_window"].sort_values("mae", na_position="last").iloc[0].to_dict()
        if not metrics_df[metrics_df["split"].astype(str) == "clean_reported_window"].empty
        else {}
    )
    diag_best = (
        metrics_df[metrics_df["split"].astype(str) == "diag_underlying"].sort_values("mae", na_position="last").iloc[0].to_dict()
        if not metrics_df[metrics_df["split"].astype(str) == "diag_underlying"].empty
        else {}
    )
    internal_consistency_detected = bool(
        (expanded_best_candidate_model_key and str(expanded_best_row.get("model_key") or "") not in {"", str(expanded_best_candidate_model_key)})
        or (gpre_proxy_model_key and str(chosen_row.get("model_key") or "") not in {"", str(gpre_proxy_model_key)})
    )
    system_audit = {
        "official_row_role": "Approximate market crush = simple market/process proxy",
        "fitted_row_role": "GPRE crush proxy = fitted production model",
        "expanded_pass_role": "Expanded-pass best = best challenger in the expanded test set",
        "production_winner_role": "Production winner = model that cleared promotion guardrails",
        "best_historical_fit_role": "Best historical fit = lowest clean-window MAE among eligible official rows",
        "best_compromise_role": "Best compromise = preview-supported medium/high-forward-usable model with the best hybrid / hard-quarter / recent-tail balance",
        "best_forward_lens_role": "Best forward lens = highest-forward-usability preview model with the strongest recent tail fit",
        "best_historical_fit_model_key": best_historical_fit_model_key,
        "best_compromise_model_key": best_compromise_model_key,
        "best_forward_lens_model_key": best_forward_lens_model_key,
        "winner_forward_usability": str(chosen_row.get("forward_usability_rating") or ""),
        "winner_preview_quality": str(chosen_row.get("live_preview_quality_status") or "n/a"),
        "hedge_style_study_role": hedge_diagnostic_only_note or "Diagnostic only; does not change official row, fitted row, or winner selection.",
        "internal_consistency_detected": internal_consistency_detected,
    }
    candidate_lines = [
        (
            f"- {str(rec.get('model_key') or '')}: family {str(rec.get('experimental_method_family') or rec.get('family_label') or 'n/a')}, "
            f"clean {_fmt_metric(rec.get('clean_mae'))}, "
            f"underlying {_fmt_metric(rec.get('underlying_mae'))}, hybrid {_fmt_metric(rec.get('hybrid_score'))}, "
            f"tail {_fmt_metric(rec.get('walk_forward_tail_mae'))}, "
            f"avg abs diff vs official {_fmt_metric(rec.get('avg_abs_diff_vs_official'))}, "
            f"hard-quarter MAE {_fmt_metric(rec.get('hard_quarter_mae'))}, "
            f"preview {_fmt_metric(rec.get('live_preview_mae'))}/{str(rec.get('live_preview_quality_status') or 'n/a')}, "
            f"coverage {_fmt_metric(pd.to_numeric(rec.get('signal_coverage_ratio'), errors='coerce') * 100.0, digits=1)}%, "
            f"forward {str(rec.get('forward_usability_rating') or 'n/a')}, "
            f"complexity {str(rec.get('complexity_rating') or 'n/a')}, "
            f"selection {'pass' if bool(rec.get('selection_guard_pass')) else 'fail'}, "
            f"promotion {'pass' if bool(rec.get('promotion_guard_pass')) else 'fail'}, "
            f"incremental {str(rec.get('incremental_value_status') or 'low')}, "
            f"signal {str(rec.get('signal_dependency_note') or 'n/a')}, "
            f"top misses {str(rec.get('top_miss_quarters') or 'n/a')}"
        )
        for rec in leaderboard_df[leaderboard_df["model_key"].astype(str).isin(sorted(experimental_candidate_keys))].sort_values(
            ["hybrid_score", "clean_mae"],
            na_position="last",
        ).to_dict("records")
    ]
    candidate_lines_or_placeholder = candidate_lines or ["- n/a"]
    experimental_best_row = (
        experimental_candidate_comparison_df[
            experimental_candidate_comparison_df["model_key"].astype(str).isin(sorted(experimental_candidate_keys))
        ].sort_values(["hybrid_score", "clean_window_mae"], na_position="last").iloc[0].to_dict()
        if isinstance(experimental_candidate_comparison_df, pd.DataFrame)
        and not experimental_candidate_comparison_df.empty
        and not experimental_candidate_comparison_df[
            experimental_candidate_comparison_df["model_key"].astype(str).isin(sorted(experimental_candidate_keys))
        ].empty
        else {}
    )
    experimental_best_candidate_key = str(experimental_best_row.get("model_key") or "")
    experimental_promoted = experimental_best_candidate_key == str(gpre_proxy_model_key or "") and experimental_best_candidate_key in experimental_candidate_keys
    experimental_block_lines = [
        (
            f"- {str(rec.get('model_key') or '')}: "
            f"hybrid Δ vs incumbent {_fmt_metric(rec.get('hybrid_score_delta_vs_incumbent'))}, "
            f"clean Δ {_fmt_metric(rec.get('clean_window_mae_delta_vs_incumbent'))}, "
            f"hard-quarter Δ {_fmt_metric(rec.get('hard_quarter_mae_delta_vs_incumbent'))}, "
            f"preview {str(rec.get('preview_quality_class') or 'n/a')}, "
            f"status {str(rec.get('promotion_status') or 'n/a')}, "
            f"reason {str(rec.get('promotion_reason_human') or 'n/a')}, "
            f"improved {str(rec.get('top_improved_quarters_vs_incumbent') or 'n/a')}, "
            f"worsened {str(rec.get('top_worsened_quarters_vs_incumbent') or 'n/a')}"
        )
        for rec in experimental_candidate_comparison_df.to_dict("records")
        if str(rec.get("model_key") or "") in experimental_candidate_keys
    ] or ["- n/a"]
    experimental_block_lines = [
        str(line or "").replace("Î”", "delta").replace("Δ", "delta")
        for line in experimental_block_lines
    ]
    experimental_block_lines = [
        line.replace("vs incumbent", "vs current winner")
        for line in experimental_block_lines
    ]
    experimental_block_lines = [
        (
            f"- {str(rec.get('model_key') or '')}: "
            f"family {str(rec.get('candidate_method_family') or 'n/a')}, "
            f"hybrid delta vs current winner {_fmt_metric(rec.get('hybrid_score_delta_vs_incumbent'))}, "
            f"clean delta {_fmt_metric(rec.get('clean_window_mae_delta_vs_incumbent'))}, "
            f"hard-quarter delta {_fmt_metric(rec.get('hard_quarter_mae_delta_vs_incumbent'))}, "
            f"preview {str(rec.get('preview_quality_class') or 'n/a')}, "
            f"concentration {str(rec.get('concentration_note') or 'n/a')}, "
            f"status {str(rec.get('promotion_status') or 'n/a')}, "
            f"reason {str(rec.get('promotion_reason_human') or 'n/a')}, "
            f"signal {str(rec.get('signal_dependency_note') or 'n/a')}, "
            f"improved {str(rec.get('top_improved_quarters_vs_incumbent') or 'n/a')}, "
            f"worsened {str(rec.get('top_worsened_quarters_vs_incumbent') or 'n/a')}"
        )
        for rec in experimental_candidate_comparison_df.to_dict("records")
        if str(rec.get("model_key") or "") in experimental_candidate_keys
    ] or ["- n/a"]
    experimental_interpretation_lines = [
        (
            f"- {str(rec.get('model_key') or '')}: "
            f"{str(rec.get('candidate_method_family') or 'candidate').capitalize()} | "
            f"helped {str(rec.get('top_improved_quarters_vs_incumbent') or 'no clear quarter cluster')} | "
            f"hurt {str(rec.get('top_worsened_quarters_vs_incumbent') or 'no clear quarter cluster')} | "
            f"status {str(rec.get('promotion_reason_human') or 'n/a')}."
        )
        for rec in experimental_candidate_comparison_df.to_dict("records")
        if str(rec.get("model_key") or "") in experimental_candidate_keys
    ] or ["- n/a"]
    signal_audit_lines = [
        f"- {str(rec.get('signal') or '')}: {str(rec.get('classification') or '')} | source {str(rec.get('source') or '')} | available quarters {int(rec.get('available_quarters') or 0)} | active quarters {int(rec.get('active_signal_quarters') or 0)} | note {str(rec.get('note') or '')}"
        for rec in list(experimental_signal_audit.get("signal_rows") or [])
    ] or ["- n/a"]
    summary_md = "\n".join(
        [
            "## Executive summary",
            (
                f"The workbook now uses two official rows: `Approximate market crush` is the non-fitted simple market/process row "
                f"(`weighted ethanol benchmark - ((cbot corn + official weighted corn basis) / {yield_per_bushel:.1f}) - gas`), while `GPRE crush proxy` is selected on a hybrid score "
                f"that blends clean reported quarters through {_quarter_label(clean_reported_cutoff)} with the underlying-only "
                f"diagnostic window for {', '.join(_quarter_label(qd) for qd in latest_underlying_quarters) or 'later quarters'}."
            ),
            (
                f"The current winning GPRE proxy is `{gpre_proxy_model_key}` ({str(chosen_row.get('family_label') or '').lower()}) "
                f"with clean-window MAE {float(pd.to_numeric(chosen_row.get('clean_mae'), errors='coerce')):.4f} $/gal, "
                f"underlying-window MAE {float(pd.to_numeric(chosen_row.get('underlying_mae'), errors='coerce')):.4f} $/gal, "
                f"and hybrid score {float(pd.to_numeric(chosen_row.get('hybrid_score'), errors='coerce')):.4f}."
                if chosen_row
                else "No eligible GPRE proxy candidate produced a finite hybrid score."
            ),
            guardrail_summary,
            "",
            "## Methodology",
            "Approximate market crush uses an active-capacity-weighted GPRE ethanol benchmark, front-month CBOT corn, official weighted corn basis, fixed 2.9 gal/bu yield, and fixed natural-gas burden.",
            "Official corn basis uses actual GPRE plant bids for current / forward periods when available, and weighted AMS basis proxy otherwise; historical quarterly model rows remain AMS-backed unless true historical bid history exists.",
            "GPRE proxy candidates are scored across bridge timing, process timing, quarter-open/current blend, severe execution overlays, sold-produced timing-gap overlays, utilization-regime overlays, residual-regime splits, simple gated ensembles, ethanol-geography, and hedge-memo families using the same quarterly target set.",
            "Official weighting now uses quarter-aware active plant capacity from filing-backed footprint metadata; bid-offset remains comparison-only unless it clearly wins in the fitted competition.",
            "",
            "## Dual-baseline diagnostics",
            f"Actual incumbent baseline: {_fmt_row_summary(incumbent_row)}",
            f"Explicit process comparator: {_fmt_row_summary(process_comparator_row)}",
            (
                f"Expanded-pass best candidate: {_fmt_row_summary(expanded_best_row)} | "
                f"selection guard {expanded_selection_status} | "
                f"promotion guard {expanded_promotion_status} | "
                f"selection reason {_gpre_guard_reason_human(expanded_best_row.get('selection_guard_reason')) or 'n/a'} | "
                f"promotion reason {_gpre_guard_reason_human(expanded_best_row.get('promotion_guard_reason')) or 'n/a'} | "
                f"incremental value {str(expanded_best_row.get('incremental_value_status') or 'low')} | "
                f"preview {str(expanded_best_row.get('live_preview_quality_status') or 'n/a')} | "
                f"hard-quarter MAE {_fmt_metric(expanded_best_row.get('hard_quarter_mae'))}"
            ),
            (
                f"Production winner: {gpre_proxy_model_key} | "
                f"reason {_gpre_guard_reason_human(promotion_guard_reason) or 'n/a'} | "
                f"preview {str(chosen_row.get('live_preview_quality_status') or 'n/a')} | "
                f"forward usability {str(chosen_row.get('forward_usability_rating') or 'n/a')} | "
                f"hard-quarter MAE {_fmt_metric(chosen_row.get('hard_quarter_mae'))}"
            ),
            (
                f"Best historical fit: {best_historical_fit_model_key or 'n/a'} | "
                f"clean-window MAE {_fmt_metric(best_historical_fit_row.get('clean_mae'))} | "
                f"forward usability {str(best_historical_fit_row.get('forward_usability_rating') or 'n/a')}."
            ),
            (
                f"Best compromise: {best_compromise_model_key or 'n/a'} | "
                f"hybrid {_fmt_metric(best_compromise_row.get('hybrid_score'))} | "
                f"hard-quarter MAE {_fmt_metric(best_compromise_row.get('hard_quarter_mae'))} | "
                f"tail {_fmt_metric(best_compromise_row.get('walk_forward_tail_mae'))}."
            ),
            (
                f"Best forward lens: {best_forward_lens_model_key or 'n/a'} | "
                f"tail {_fmt_metric(best_forward_lens_row.get('walk_forward_tail_mae'))} | "
                f"hybrid {_fmt_metric(best_forward_lens_row.get('hybrid_score'))} | "
                f"forward usability {str(best_forward_lens_row.get('forward_usability_rating') or 'n/a')}."
            ),
            f"Production decision story: {production_decision_story}",
            f"Selection vs promotion: {selection_vs_promotion_explanation}",
            "",
            "## Experimental signal audit",
            *signal_audit_lines,
            *[
                f"- {line}"
                for line in list(experimental_signal_audit.get("interpretation_lines") or [])
                if str(line or "").strip()
            ],
            "",
            "## Experimental realization / regime candidates",
            *candidate_lines_or_placeholder,
            "",
            "## Experimental realization / regime comparison",
            (
                f"Current production winner: {incumbent_baseline_model_key or gpre_proxy_model_key}. "
                f"Best experimental candidate: {experimental_best_candidate_key or 'n/a'}. "
                f"Promoted: {'yes' if experimental_promoted else 'no'}."
            ),
            *experimental_block_lines,
            "",
            "## Experimental interpretation",
            *experimental_interpretation_lines,
            "",
            "## Results",
            f"Simple market test MAE: {simple_market_test_mae:.4f} $/gal" if np.isfinite(simple_market_test_mae) else "Simple market test MAE: n/a",
            f"Chosen GPRE proxy test MAE: {gpre_proxy_test_mae:.4f} $/gal" if np.isfinite(gpre_proxy_test_mae) else "Chosen GPRE proxy test MAE: n/a",
            f"Current-quarter official corn basis provenance: {str(current_official_basis_payload.get('official_corn_basis_provenance') or 'n/a')}",
            f"Next-quarter official corn basis provenance: {str(next_official_basis_payload.get('official_corn_basis_provenance') or 'n/a')}",
            (
                f"Best clean-window model: {str(clean_best.get('model_key') or 'n/a')} "
                f"with MAE {float(pd.to_numeric(clean_best.get('mae'), errors='coerce')):.4f} $/gal."
                if clean_best and pd.notna(pd.to_numeric(clean_best.get('mae'), errors='coerce'))
                else "Best clean-window model: n/a"
            ),
            (
                f"Best underlying-window model: {str(diag_best.get('model_key') or 'n/a')} "
                f"with MAE {float(pd.to_numeric(diag_best.get('mae'), errors='coerce')):.4f} $/gal."
                if diag_best and pd.notna(pd.to_numeric(diag_best.get('mae'), errors='coerce'))
                else "Best underlying-window model: n/a"
            ),
            (
                f"Best historical fit (official-eligible): {best_historical_fit_model_key or 'n/a'}."
            ),
            (
                f"Best compromise: {best_compromise_model_key or 'n/a'}."
            ),
            (
                f"Best forward lens: {best_forward_lens_model_key or 'n/a'}."
            ),
            (f"Top miss quarters for incumbent baseline: {str(incumbent_row.get('top_miss_quarters') or 'n/a')}." if incumbent_row else "Top miss quarters for incumbent baseline: n/a"),
            (f"Top miss quarters for process comparator: {str(process_comparator_row.get('top_miss_quarters') or 'n/a')}." if process_comparator_row else "Top miss quarters for process comparator: n/a"),
            (f"Top miss quarters for chosen GPRE proxy: {chosen_top_miss}." if chosen_top_miss else "Top miss quarters for chosen GPRE proxy: n/a"),
            "",
            "## Implied hedge / realization style study",
            f"Target definition: {str(hedge_target_definition or hedge_target_label).strip().rstrip('.')}.",
            (
                f"Best overall style: {hedge_best_style_label or 'n/a'} | "
                f"best overall family: {hedge_best_family_label or 'n/a'} | "
                f"usable quarters: {hedge_usable_quarter_count}."
            ),
            (
                f"Backtest window: {hedge_window_display}."
                if hedge_window_display
                else "Backtest window: n/a."
            ),
            (
                f"Style vs family: {hedge_style_vs_family_explanation}"
                if hedge_style_vs_family_explanation
                else "Style vs family: n/a."
            ),
            (
                f"Diagnostic scope: {hedge_diagnostic_only_note}"
                if hedge_diagnostic_only_note
                else "Diagnostic scope: diagnostic only."
            ),
            (
                f"Weak-fit quarters: {', '.join(hedge_weak_fit_quarters)}."
                if hedge_weak_fit_quarters
                else "Weak-fit quarters: none."
            ),
            "",
            "## Roles / consistency check",
            f"{system_audit['official_row_role']}.",
            f"{system_audit['fitted_row_role']}.",
            f"{system_audit['expanded_pass_role']}.",
            f"{system_audit['production_winner_role']}.",
            f"Winner preview quality: {system_audit['winner_preview_quality'] or 'n/a'}.",
            f"Hedge-style study: {str(system_audit['hedge_style_study_role'] or '').strip().rstrip('.')}.",
            f"Any internal inconsistency detected: {'yes' if system_audit['internal_consistency_detected'] else 'no'}.",
            "",
            "## Recommended production-ready proxy formula",
            f"Approximate market crush row: weighted_ethanol_benchmark - ((cbot_corn_front + official_weighted_corn_basis) / {yield_per_bushel:.1f}) - ({gas_usage_mmbtu_per_gal:.3f} * nymex_gas)",
            f"GPRE crush proxy row: {gpre_proxy_model_key} ({str(chosen_row.get('timing_rule') or '').strip()}; family = {str(chosen_row.get('family_label') or '').strip()})",
        ]
    )
    overlay_preview_bundle = build_gpre_overlay_proxy_preview_bundle(
        rows_df,
        ethanol_yield=yield_per_bushel,
        natural_gas_usage=natural_gas_usage_num,
        as_of_date=as_of_date,
        ticker_root=ticker_root,
        bids_snapshot=effective_bids_snapshot,
        plant_capacity_history=effective_plant_capacity_history,
        prior_market_snapshot=prior_market_snapshot,
        current_qtd_market_snapshot=current_qtd_market_snapshot,
        next_quarter_thesis_snapshot=next_quarter_thesis_snapshot,
        simple_crush_history_rows=simple_crush_history_rows,
        gpre_basis_model_result={
            "quarterly_df": quarterly_df,
            "gpre_proxy_model_key": gpre_proxy_model_key,
            "gpre_proxy_family": str(chosen_spec.get("family") or chosen_row.get("family") or ""),
            "gpre_proxy_family_label": str(chosen_spec.get("family_label") or chosen_row.get("family_label") or ""),
            "gpre_proxy_timing_rule": str(chosen_spec.get("timing_rule") or chosen_row.get("timing_rule") or ""),
        },
    )
    proxy_implied_results = _build_gpre_proxy_implied_results_bundle(
        overlay_preview_bundle,
        reported_gallons_produced_by_quarter=reported_gallons_produced_by_quarter,
        denominator_policy_by_quarter=denominator_policy_by_quarter,
        ticker_root=ticker_root,
        plant_capacity_history=effective_plant_capacity_history,
    )
    overlay_preview_bundle = dict(overlay_preview_bundle or {})
    overlay_preview_bundle["proxy_implied_results"] = proxy_implied_results
    return {
        "quarterly_df": quarterly_df,
        "metrics_df": metrics_df,
        "leaderboard_df": leaderboard_df,
        "incumbent_baseline_leaderboard_df": incumbent_baseline_leaderboard_df,
        "weights_df": weights_df,
        "footprint_df": footprint_df,
        "recommended_model_key": recommended_model_key,
        "recommended_formula": production_formula,
        "summary_markdown": summary_md,
        "proxy_implied_results": proxy_implied_results,
        "relevant_regions": list(relevant_regions),
        "evaluation_target_underlying_quarters": list(latest_underlying_quarters),
        "experimental_best_model_key": expanded_best_candidate_model_key,
        "incumbent_baseline_model_key": incumbent_baseline_model_key,
        "process_comparator_model_key": "process_front_loaded",
        "expanded_candidate_model_key": expanded_best_candidate_model_key,
        "expanded_best_candidate_model_key": expanded_best_candidate_model_key,
        "best_historical_fit_model_key": best_historical_fit_model_key,
        "best_compromise_model_key": best_compromise_model_key,
        "best_forward_lens_model_key": best_forward_lens_model_key,
        "production_winner_model_key": gpre_proxy_model_key,
        "gpre_proxy_model_key": gpre_proxy_model_key,
        "gpre_proxy_selection_guard_reason": str(chosen_row.get("selection_guard_reason") or ""),
        "gpre_proxy_selection_guard_pass": bool(chosen_row.get("selection_guard_pass")),
        "gpre_proxy_promotion_guard_reason": str(promotion_guard_reason or ""),
        "gpre_proxy_promotion_guard_pass": bool(chosen_row.get("promotion_guard_pass")),
        "gpre_proxy_live_preview_mae": float(pd.to_numeric(chosen_row.get("live_preview_mae"), errors="coerce")),
        "gpre_proxy_live_preview_max_error": float(pd.to_numeric(chosen_row.get("live_preview_max_error"), errors="coerce")),
        "gpre_proxy_live_preview_bias": float(pd.to_numeric(chosen_row.get("live_preview_bias"), errors="coerce")),
        "gpre_proxy_live_preview_quality_status": str(chosen_row.get("live_preview_quality_status") or ""),
        "gpre_proxy_live_preview_top_miss_quarters": str(chosen_row.get("live_preview_top_miss_quarters") or ""),
        "gpre_proxy_live_preview_worst_phase": str(chosen_row.get("live_preview_worst_phase") or ""),
        "gpre_proxy_hard_quarter_mae": float(pd.to_numeric(chosen_row.get("hard_quarter_mae"), errors="coerce")),
        "gpre_proxy_hard_quarter_count": int(pd.to_numeric(chosen_row.get("hard_quarter_count"), errors="coerce") or 0),
        "gpre_proxy_hard_quarter_top_miss_quarters": str(chosen_row.get("hard_quarter_top_miss_quarters") or ""),
        "production_decision_story": str(production_decision_story or ""),
        "selection_vs_promotion_explanation": str(selection_vs_promotion_explanation or ""),
        "preview_accuracy_by_model": preview_accuracy_by_model,
        "recent_quarter_comparison_df": recent_quarter_comparison_df,
        "system_audit": system_audit,
        "hedge_style_study": hedge_style_study,
        "model_key_to_pred_col": model_key_to_pred_col,
        "ethanol_yield": yield_per_bushel,
        "natural_gas_usage_btu_per_gal": natural_gas_usage_num,
        "bid_adjusted_offsets_df": bid_adjusted_offsets_df,
        "gpre_bid_snapshot": effective_bids_snapshot,
        "bridge_official_model_key": gpre_proxy_model_key,
        "gpre_proxy_model_key": gpre_proxy_model_key,
        "gpre_proxy_hybrid_score": float(pd.to_numeric(chosen_row.get("hybrid_score"), errors="coerce")),
        "official_market_summary": official_market_summary,
        "official_market_rows": official_market_rows,
        "plant_capacity_history": effective_plant_capacity_history,
        "current_official_basis_source_kind": str(current_official_basis_payload.get("official_corn_basis_source_kind") or ""),
        "current_official_basis_provenance": str(current_official_basis_payload.get("official_corn_basis_provenance") or ""),
        "next_official_basis_source_kind": str(next_official_basis_payload.get("official_corn_basis_source_kind") or ""),
        "next_official_basis_provenance": str(next_official_basis_payload.get("official_corn_basis_provenance") or ""),
        "overlay_preview_bundle": overlay_preview_bundle,
        "quarter_open_snapshot_status": str((overlay_preview_bundle or {}).get("quarter_open_snapshot_status") or "no_snapshot"),
        "quarter_open_target_quarter_end": (overlay_preview_bundle or {}).get("quarter_open_target_quarter_end"),
        "quarter_open_official_proxy_usd_per_gal": (overlay_preview_bundle or {}).get("quarter_open_official_proxy_usd_per_gal"),
        "quarter_open_gpre_proxy_usd_per_gal": (overlay_preview_bundle or {}).get("quarter_open_gpre_proxy_usd_per_gal"),
        "quarter_open_market_inputs": (overlay_preview_bundle or {}).get("quarter_open_market_inputs"),
        "quarter_open_process_inputs": (overlay_preview_bundle or {}).get("quarter_open_process_inputs"),
        "quarter_open_snapshot_as_of": (overlay_preview_bundle or {}).get("quarter_open_snapshot_as_of"),
        "quarter_open_snapshot_model_key": str((overlay_preview_bundle or {}).get("quarter_open_snapshot_model_key") or ""),
        "next_thesis_frozen_snapshot_entry": (overlay_preview_bundle or {}).get("next_thesis_frozen_snapshot_entry"),
        "experimental_signal_audit": experimental_signal_audit,
        "experimental_candidate_comparison_df": experimental_candidate_comparison_df,
        "official_weighting_method": "Active-capacity weighted GPRE footprint by quarter-aware plant metadata",
        "official_ethanol_method": (
            "Weighted ethanol benchmark using quarter-aware active-capacity footprint weights, "
            "mapped state/regional ethanol series, Midwest-anchor plausibility screens, and deterministic fallbacks"
        ),
        "official_basis_method": (
            "Official corn basis uses actual GPRE plant bids when available for current / forward periods; "
            "otherwise it falls back to active-capacity-weighted AMS basis using mapped state/regional series and deterministic fallbacks"
        ),
        "official_gas_method": f"Fixed {gas_usage_mmbtu_per_gal:.3f} MMBtu/gal burden multiplied by benchmark gas",
        "official_fallback_policy": (
            "Use explicit mapped nearest regional proxy when a plant/state benchmark is unavailable or implausible; "
            "renormalize across covered weight and surface the fallback in notes."
        ),
    }


def load_market_export_rows(
    cache_dir: Path,
    ticker: str,
    profile: Any = None,
    *,
    ensure_cache: bool = True,
) -> List[Dict[str, Any]]:
    # Workbook callers ask for normalized export rows, not provider-specific raw or
    # parsed files. If the export is missing, we opportunistically rebuild it from
    # whatever raw/index/parsed state is already available locally.
    ticker_u = str(ticker or "").strip().upper()
    if not ticker_u:
        return []
    cache_root = resolve_market_cache_root(Path(cache_dir))
    ensure_market_cache_dirs(cache_root)
    export_path = export_rows_path(cache_root, ticker_u)
    enabled_sources = tuple(src for src in _enabled_sources_for_profile(profile) if src in PROVIDERS)
    ticker_root = _ticker_root_from_cache_dir(Path(cache_dir), ticker_u)
    if not export_path.exists() and ensure_cache:
        sync_market_cache(cache_dir, ticker_u, profile=profile, sync_raw=False, refresh=False, reparse=False)
    if not export_path.exists():
        return []
    df = _load_parquet(export_path)
    if ensure_cache and _export_needs_history_repair(df, ticker_root=ticker_root, enabled_sources=enabled_sources):
        sync_market_cache(cache_dir, ticker_u, profile=profile, sync_raw=True, refresh=False, reparse=True)
        df = _load_parquet(export_path)
    return _rows_from_export_df(df)
