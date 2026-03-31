"""Market-data sync, parse, and export service for workbook overlays.

The market-data cache has three important layers:
- `raw/`: source files discovered or restored from disk
- `parsed/`: source-specific observation and quarterly parquet frames
- `parsed/exports/`: ticker-shaped export rows consumed by workbook logic
"""
from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

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
from .mappings import series_meta_from_key
from .models import SourceFrameSpec, SyncSummary
from .providers import PROVIDERS


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
                "parsed_at": pd.Timestamp.utcnow().isoformat(),
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


def _market_rows_df(rows: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(list(rows or []))
    if df.empty:
        return pd.DataFrame(
            columns=[
                "observation_date",
                "quarter",
                "aggregation_level",
                "series_key",
                "price_value",
                "contract_tenor",
                "source_type",
                "source_file",
                "parsed_text",
            ]
        )
    df = df.copy()
    for col, default in {
        "observation_date": None,
        "quarter": None,
        "price_value": None,
        "aggregation_level": "",
        "series_key": "",
        "contract_tenor": "",
        "source_type": "",
        "source_file": "",
        "parsed_text": "",
    }.items():
        if col not in df.columns:
            df[col] = default
    df["observation_date"] = pd.to_datetime(df.get("observation_date"), errors="coerce")
    df["quarter"] = pd.to_datetime(df.get("quarter"), errors="coerce")
    df["price_value"] = pd.to_numeric(df.get("price_value"), errors="coerce")
    df["aggregation_level"] = df.get("aggregation_level").astype(str)
    df["series_key"] = df.get("series_key").astype(str)
    df["contract_tenor"] = df.get("contract_tenor").fillna("").astype(str)
    return df


def _series_observation_df(df: pd.DataFrame, series_key: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
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
    end_ts = pd.Timestamp(week_end)
    start_ts = end_ts - pd.Timedelta(days=6)
    window = df_daily[(df_daily["observation_date"] >= start_ts) & (df_daily["observation_date"] <= end_ts)].copy()
    vals = pd.to_numeric(window.get("price_value"), errors="coerce").dropna()
    if vals.empty:
        return None
    return float(vals.mean())


def build_current_qtd_simple_crush_snapshot(
    rows: Iterable[Dict[str, Any]],
    *,
    ethanol_yield: Optional[float],
    natural_gas_usage: Optional[float],
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    df = _market_rows_df(rows)
    q_start, q_end = calendar_quarter_bounds(as_of_date=as_of_date)
    ethanol_df = _series_observation_df(df, "ethanol_nebraska")
    gas_df = _series_observation_df(df, "nymex_gas")
    corn_daily_df = _series_observation_df(df, "corn_nebraska")
    if ethanol_df.empty or gas_df.empty or corn_daily_df.empty:
        return {
            "quarter_start": q_start,
            "quarter_end": q_end,
            "as_of": None,
            "weeks_included": 0,
            "current_market": {},
            "current_process": {},
            "weekly_rows": [],
            "status": "no_data",
            "message": "No complete current-quarter weekly observations available.",
        }
    ethanol_df = ethanol_df[
        (ethanol_df["observation_date"].dt.date >= q_start) & (ethanol_df["observation_date"].dt.date <= q_end)
    ].copy()
    gas_df = gas_df[
        (gas_df["observation_date"].dt.date >= q_start) & (gas_df["observation_date"].dt.date <= q_end)
    ].copy()
    if ethanol_df.empty or gas_df.empty:
        return {
            "quarter_start": q_start,
            "quarter_end": q_end,
            "as_of": None,
            "weeks_included": 0,
            "current_market": {},
            "current_process": {},
            "weekly_rows": [],
            "status": "no_data",
            "message": "No complete current-quarter weekly observations available.",
        }
    gas_by_date = {
        pd.Timestamp(rec["observation_date"]).date(): float(rec["price_value"])
        for rec in gas_df.to_dict("records")
    }
    try:
        ethanol_yield_num = float(ethanol_yield) if ethanol_yield is not None else None
        natural_gas_usage_num = float(natural_gas_usage) if natural_gas_usage is not None else None
    except Exception:
        ethanol_yield_num = None
        natural_gas_usage_num = None
    weekly_rows: List[Dict[str, Any]] = []
    for rec in ethanol_df.to_dict("records"):
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
        if ethanol_yield_num is not None and natural_gas_usage_num is not None:
            ethanol_revenue = ethanol_yield_num * float(ethanol_price)
            feedstock_cost = -float(corn_price)
            natural_gas_burden = -((natural_gas_usage_num / 1_000_000.0) * ethanol_yield_num * float(gas_price))
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
            "quarter_start": q_start,
            "quarter_end": q_end,
            "as_of": None,
            "weeks_included": 0,
            "current_market": {},
            "current_process": {},
            "weekly_rows": [],
            "status": "no_data",
            "message": "No complete current-quarter weekly observations available.",
        }

    def _avg(key: str) -> Optional[float]:
        vals = [pd.to_numeric(row.get(key), errors="coerce") for row in weekly_rows]
        vals = [float(v) for v in vals if pd.notna(v)]
        if not vals:
            return None
        return float(sum(vals) / len(vals))

    return {
        "quarter_start": q_start,
        "quarter_end": q_end,
        "as_of": max(row["week_end"] for row in weekly_rows),
        "weeks_included": len(weekly_rows),
        "current_market": {
            "corn_price": _avg("corn_price"),
            "ethanol_price": _avg("ethanol_price"),
            "natural_gas_price": _avg("natural_gas_price"),
        },
        "current_process": {
            "ethanol_revenue": _avg("ethanol_revenue"),
            "feedstock_cost": _avg("feedstock_cost"),
            "natural_gas_burden": _avg("natural_gas_burden"),
            "simple_crush": _avg("simple_crush"),
        },
        "weekly_rows": weekly_rows,
        "status": "ok",
        "message": "",
    }


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


def _pick_next_quarter_futures_reference(
    rows: Iterable[Dict[str, Any]],
    *,
    prefix: str,
    market_family: str,
    as_of_date: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    df = _market_rows_df(rows)
    obs = df[
        (df["aggregation_level"].astype(str).str.lower() == "observation")
        & df["series_key"].astype(str).str.match(rf"^{re.escape(prefix)}_[a-z]{{3}}\d{{2}}_usd$", na=False)
        & df["observation_date"].notna()
        & df["price_value"].notna()
    ].copy()
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
) -> Dict[str, Any]:
    corn_ref = _pick_next_quarter_futures_reference(rows, prefix="cbot_corn", market_family="corn_futures", as_of_date=as_of_date)
    gas_ref = _pick_next_quarter_futures_reference(rows, prefix="nymex_gas", market_family="natural_gas_futures", as_of_date=as_of_date)
    target_start, target_end = next_calendar_quarter_bounds(as_of_date=as_of_date)
    return {
        "target_quarter_start": target_start,
        "target_quarter_end": target_end,
        "target_quarter_midpoint": _quarter_midpoint(target_start, target_end),
        "corn": corn_ref,
        "natural_gas": gas_ref,
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
    if not export_path.exists() and ensure_cache:
        sync_market_cache(cache_dir, ticker_u, profile=profile, sync_raw=False, refresh=False, reparse=False)
    if not export_path.exists():
        return []
    df = _load_parquet(export_path)
    return _rows_from_export_df(df)
