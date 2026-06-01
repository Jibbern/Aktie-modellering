"""Market/economics source helpers for workbook economics surfaces."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .legacy_support import _coerce_next_quarter_end
from .market_data.service import load_market_export_rows


@dataclass(frozen=True)
class EconomicsMarketRowsDeps:
    cache_dir: Any
    ticker: Any
    company_profile: Any
    first_existing_material_dir: Callable[[str], Optional[Path]]
    coerce_next_quarter_end: Callable[[Any], Any] = _coerce_next_quarter_end
    load_market_export_rows: Callable[..., Any] = load_market_export_rows


def _economics_market_region_tags(region_txt: Any) -> set[str]:
    region_norm = str(region_txt or "").strip().lower()
    if not region_norm:
        return set()
    tags = {region_norm}
    for part in re.split(r"[^a-z0-9]+", region_norm):
        if part:
            tags.add(part)
    if any(tok in region_norm for tok in ("illinois", "indiana", "iowa", "kansas", "michigan", "minnesota", "missouri", "nebraska", "ohio", "south_dakota", "wisconsin")):
        tags.add("midwest")
    return tags


def _economics_market_series_meta(series_key: Any, source_type: str) -> Optional[Dict[str, str]]:
    key = str(series_key or "").strip().lower()
    if not key or key in {"week_end", "report_date", "quarter", "source_pdf", "weeks", "gpre_weight_coverage", "gpre_weight_coverage_core", "gpre_weight_coverage_cash", "gpre_weight_included", "gas_cost_gal"}:
        return None
    if key.startswith(("crush_", "board_crush_", "gpre_underlying_")) or key in {"cbot_corn_cents"}:
        return None

    def _meta(market_family: str, instrument: str, region: str, unit: str, tenor: str = "") -> Dict[str, str]:
        return {
            "market_family": market_family,
            "instrument": instrument,
            "region": region,
            "unit": unit,
            "contract_tenor": tenor,
        }

    m_corn_fut = re.match(r"cbot_corn(?:_([a-z]{3}\d{2}))?_usd(?:_per_bu)?$", key)
    if m_corn_fut:
        tenor = (m_corn_fut.group(1) or "front").lower()
        fam = "corn_futures" if tenor != "front" else "corn_price"
        return _meta(fam, "Corn futures" if fam == "corn_futures" else "Corn price", "cbot", "$/bushel", tenor)

    if key == "nymex_gas":
        return _meta("natural_gas_price", "Natural gas price", "nymex", "$/MMBtu", "front")
    m_gas_fut = re.match(r"nymex_gas_([a-z]{3}\d{2})_usd$", key)
    if m_gas_fut:
        tenor = (m_gas_fut.group(1) or "").lower()
        return _meta("natural_gas_futures", "Natural gas futures", "nymex", "$/MMBtu", tenor)

    if key.startswith("corn_cash_"):
        return _meta("corn_price", "Corn cash price", key.replace("corn_cash_", "", 1), "$/bushel")
    if source_type == "ams_3617_daily_corn" and key.startswith("corn_"):
        return _meta("corn_price", "Corn cash price", key.replace("corn_", "", 1), "$/bushel")
    if key.startswith("ethanol_"):
        return _meta("ethanol_price", "Ethanol price", key.replace("ethanol_", "", 1), "$/gal")
    if key.startswith("ddgs_"):
        return _meta("ddgs_price", "DDGS price", key.replace("ddgs_", "", 1), "$/ton")
    if key.startswith("corn_oil_"):
        return _meta("renewable_corn_oil_price", "Renewable corn oil price", key.replace("corn_oil_", "", 1), "c/lb")
    return None


def _convert_market_price_value(value: Any, unit_from: str, unit_to: str) -> Tuple[Optional[float], bool]:
    val_num = pd.to_numeric(value, errors="coerce")
    if pd.isna(val_num):
        return None, False
    val = float(val_num)
    from_u = str(unit_from or "").strip()
    to_u = str(unit_to or "").strip()
    if not to_u or from_u == to_u:
        return val, False
    if from_u == "$/ton" and to_u == "$/lb":
        return val / 2000.0, True
    if from_u == "c/lb" and to_u == "$/lb":
        return val / 100.0, True
    if from_u == "c/lb" and to_u == "c/lb":
        return val, False
    return None, False


def build_economics_market_rows(deps: EconomicsMarketRowsDeps) -> List[Dict[str, Any]]:
    try:
        cached_rows = deps.load_market_export_rows(
            cache_dir=deps.cache_dir,
            ticker=str(deps.ticker or ""),
            profile=deps.company_profile,
            ensure_cache=True,
        )
        if cached_rows:
            return cached_rows
    except Exception:
        pass

    data_dir = deps.first_existing_material_dir("data")
    bioenergy_dir = deps.first_existing_material_dir("USDA_bioenergy_reports")
    weekly_dir = deps.first_existing_material_dir("USDA_weekly_data")
    daily_dir = deps.first_existing_material_dir("USDA_daily_data")
    if data_dir is None and bioenergy_dir is None and weekly_dir is None and daily_dir is None:
        return []

    raw_rows: List[Dict[str, Any]] = []

    def _first_existing_market_csv(*candidates: Optional[Path]) -> Optional[Path]:
        # Workbook fallback keeps the original `data/` convention working, but it can
        # now also read the user-facing USDA folders directly. This matters when the
        # market-data export cache is missing and we still want the overlay to recover
        # from local curated CSVs without a fresh sync.
        for path_in in candidates:
            if path_in is not None and path_in.exists() and path_in.is_file():
                return path_in
        return None

    def _append_rows_from_csv(path_in: Path, date_col: str, source_file_col: str, source_type: str) -> None:
        if not path_in.exists():
            return
        try:
            df_in = pd.read_csv(path_in)
        except Exception:
            return
        if date_col not in df_in.columns:
            return
        df_in[date_col] = pd.to_datetime(df_in[date_col], errors="coerce")
        df_in = df_in[df_in[date_col].notna()].copy()
        if df_in.empty:
            return
        meta_rows: List[Dict[str, Any]] = []
        value_cols: List[str] = []
        for col in df_in.columns:
            if col in {date_col, source_file_col}:
                continue
            meta = _economics_market_series_meta(col, source_type)
            if meta is None:
                continue
            value_cols.append(col)
            meta_rows.append(
                {
                    "series_key": str(col),
                    "market_family": meta["market_family"],
                    "instrument": meta["instrument"],
                    "region": meta["region"],
                    "contract_tenor": meta["contract_tenor"],
                    "unit": meta["unit"],
                }
            )
        if not value_cols:
            return
        work_cols = [date_col] + ([source_file_col] if source_file_col in df_in.columns else []) + value_cols
        df_work = df_in[work_cols].copy()
        long_df = df_work.melt(
            id_vars=[c for c in [date_col, source_file_col] if c in df_work.columns],
            value_vars=value_cols,
            var_name="series_key",
            value_name="price_value",
        )
        long_df["price_value"] = pd.to_numeric(long_df["price_value"], errors="coerce")
        long_df = long_df[long_df["price_value"].notna()].copy()
        if long_df.empty:
            return
        meta_df = pd.DataFrame(meta_rows)
        long_df = long_df.merge(meta_df, on="series_key", how="inner")
        if long_df.empty:
            return
        long_df["observation_date"] = pd.to_datetime(long_df[date_col], errors="coerce").dt.date
        long_df["quarter"] = long_df["observation_date"].map(deps.coerce_next_quarter_end)
        long_df = long_df[long_df["quarter"].map(lambda qd: isinstance(qd, date))].copy()
        if long_df.empty:
            return
        if source_file_col in long_df.columns:
            source_file_series = long_df[source_file_col].astype(str).str.strip()
            long_df["source_file"] = source_file_series.mask(source_file_series.eq("") | source_file_series.eq("nan"), path_in.name)
        else:
            long_df["source_file"] = path_in.name
        long_df["source_type"] = source_type
        long_df["aggregation_level"] = "observation"
        long_df["parsed_text"] = long_df["series_key"].astype(str)
        long_df["quality"] = "high"
        long_df["_obs_count"] = 1
        raw_rows.extend(
            long_df[
                [
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
            ].to_dict("records")
        )

    nwer_weekly_csv = _first_existing_market_csv(
        data_dir / "nwer_weekly.csv" if data_dir is not None else None,
        bioenergy_dir / "nwer_weekly.csv" if bioenergy_dir is not None else None,
        weekly_dir / "nwer_weekly.csv" if weekly_dir is not None else None,
    )
    ams_daily_csv = _first_existing_market_csv(
        data_dir / "ams_3617_daily_corn.csv" if data_dir is not None else None,
        daily_dir / "ams_3617_daily_corn.csv" if daily_dir is not None else None,
    )
    if nwer_weekly_csv is not None:
        _append_rows_from_csv(nwer_weekly_csv, "week_end", "source_pdf", "nwer_weekly")
    if ams_daily_csv is not None:
        _append_rows_from_csv(ams_daily_csv, "report_date", "source_pdf", "ams_3617_daily_corn")

    if not raw_rows:
        return []

    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for rec in raw_rows:
        gkey = (
            rec.get("quarter"),
            rec.get("source_type"),
            rec.get("market_family"),
            rec.get("series_key"),
            rec.get("instrument"),
            rec.get("region"),
            rec.get("contract_tenor"),
            rec.get("unit"),
        )
        grouped.setdefault(gkey, []).append(rec)

    agg_rows: List[Dict[str, Any]] = []
    for gkey, items in grouped.items():
        vals = [float(x["price_value"]) for x in items if pd.notna(pd.to_numeric(x.get("price_value"), errors="coerce"))]
        if not vals:
            continue
        items_sorted = sorted(items, key=lambda x: x.get("observation_date") or date.min)
        obs_count = len(vals)
        quality = "high" if obs_count >= 8 else "medium" if obs_count >= 3 else "low"
        base = {
            "observation_date": items_sorted[-1].get("observation_date"),
            "quarter": gkey[0],
            "source_file": Path(str(items_sorted[-1].get("source_file") or "")).name or str(gkey[1]),
            "source_type": gkey[1],
            "market_family": gkey[2],
            "series_key": gkey[3],
            "instrument": gkey[4],
            "region": gkey[5],
            "contract_tenor": gkey[6],
            "unit": gkey[7],
            "quality": quality,
            "_obs_count": obs_count,
        }
        avg_row = dict(base)
        avg_row["aggregation_level"] = "quarter_avg"
        avg_row["price_value"] = float(sum(vals) / obs_count)
        avg_row["parsed_text"] = f"Quarter average from {obs_count} observations."
        agg_rows.append(avg_row)
        end_row = dict(base)
        end_row["aggregation_level"] = "quarter_end"
        end_row["price_value"] = float(items_sorted[-1]["price_value"])
        end_row["parsed_text"] = f"Quarter-end reference from {obs_count} observations."
        agg_rows.append(end_row)

    return sorted(
        raw_rows + agg_rows,
        key=lambda r: (
            pd.to_datetime(r.get("quarter"), errors="coerce") if r.get("quarter") else pd.Timestamp("1900-01-01"),
            str(r.get("series_key") or ""),
            0 if r.get("aggregation_level") == "observation" else 1 if r.get("aggregation_level") == "quarter_avg" else 2,
            pd.to_datetime(r.get("observation_date"), errors="coerce") if r.get("observation_date") else pd.Timestamp("1900-01-01"),
        ),
    )


def _build_economics_market_rows(deps: EconomicsMarketRowsDeps) -> List[Dict[str, Any]]:
    return build_economics_market_rows(deps)
