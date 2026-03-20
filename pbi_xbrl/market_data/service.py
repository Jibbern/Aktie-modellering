from __future__ import annotations

import json
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


def _bootstrap_specs_for_source(source: str, ticker_root: Path) -> List[Tuple[SourceFrameSpec, Path]]:
    data_dir = ticker_root / "data"
    specs: List[Tuple[SourceFrameSpec, Path]] = []
    if source == "nwer":
        weekly = _safe_read_csv(data_dir / "nwer_weekly.csv")
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
                    data_dir / "nwer_weekly.csv",
                )
            )
        quarterly = _safe_read_csv(data_dir / "nwer_quarterly.csv")
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
                    data_dir / "nwer_quarterly.csv",
                )
            )
    elif source == "ams_3617":
        daily = _safe_read_csv(data_dir / "ams_3617_daily_corn.csv")
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
                    data_dir / "ams_3617_daily_corn.csv",
                )
            )
        weekly = _safe_read_csv(data_dir / "ams_3617_weekly_corn.csv")
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
                    data_dir / "ams_3617_weekly_corn.csv",
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
        sync_result = provider.sync_raw(cache_root, ticker_root, refresh=refresh) if (sync_raw or refresh or reparse) else {"entries": raw_manifest.get(source, []), "raw_added": 0, "raw_refreshed": 0, "raw_skipped": 0}
        raw_entries = list(sync_result.get("entries") or [])
        raw_entries_by_source[source] = raw_entries
        raw_manifest[source] = raw_entries
        raw_added += int(sync_result.get("raw_added") or 0)
        raw_refreshed += int(sync_result.get("raw_refreshed") or 0)
        raw_skipped += int(sync_result.get("raw_skipped") or 0)
    save_manifest(raw_manifest_path(cache_root), raw_manifest)

    parsed_sources: List[str] = []
    all_export_parts: List[pd.DataFrame] = []
    for source in enabled_sources:
        provider = PROVIDERS[source]
        bootstrap_df, bootstrap_fp = _bootstrap_rows_for_source(source, ticker_root)
        raw_entries = raw_entries_by_source.get(source, [])
        raw_fp = batch_fingerprint([str(x.get("checksum") or "") for x in raw_entries])
        combined_raw_fp = batch_fingerprint([bootstrap_fp, raw_fp])
        parse_version = str(getattr(provider, "provider_parse_version", "v1") or "v1")
        manifest_entry = parsed_manifest.get(source) if isinstance(parsed_manifest.get(source), dict) else {}
        obs_path = parsed_obs_path(cache_root, source)
        qtr_path = parsed_quarter_path(cache_root, source)
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


def load_market_export_rows(
    cache_dir: Path,
    ticker: str,
    profile: Any = None,
    *,
    ensure_cache: bool = True,
) -> List[Dict[str, Any]]:
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
