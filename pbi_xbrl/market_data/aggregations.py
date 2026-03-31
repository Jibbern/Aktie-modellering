"""Quarter-level helpers for provider observations and export shaping.

These functions keep the aggregation math isolated from provider-specific
parsers so raw observations can be normalized once and then reused by both the
market-data export layer and the workbook overlay logic.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


def quarter_end_from_date(obs_date: date) -> date:
    month = ((int(obs_date.month) - 1) // 3 + 1) * 3
    if month == 3:
        day = 31
    elif month == 6:
        day = 30
    elif month == 9:
        day = 30
    else:
        day = 31
    return date(int(obs_date.year), month, day)


def parse_quarter_like(value: object) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    txt = str(value).strip()
    if not txt:
        return None
    m = pd.Series([txt]).str.extract(r"^\s*(\d{4})[- ]?Q([1-4])\s*$")
    if not m.empty and pd.notna(m.iloc[0, 0]) and pd.notna(m.iloc[0, 1]):
        year = int(m.iloc[0, 0])
        q = int(m.iloc[0, 1])
        return quarter_end_from_date(date(year, q * 3 - 2, 1))
    ts = pd.to_datetime(txt, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def aggregate_quarterly(obs_df: pd.DataFrame) -> pd.DataFrame:
    if obs_df is None or obs_df.empty:
        return pd.DataFrame()
    df = obs_df.copy()
    df["observation_date"] = pd.to_datetime(df.get("observation_date"), errors="coerce")
    df = df[df["observation_date"].notna()].copy()
    if df.empty:
        return pd.DataFrame()
    df["quarter"] = df["observation_date"].dt.date.apply(quarter_end_from_date)
    key_cols = [
        "quarter",
        "source",
        "report_type",
        "source_type",
        "market_family",
        "series_key",
        "instrument",
        "location",
        "region",
        "tenor",
        "unit",
    ]
    rows = []
    for _, grp in df.groupby(key_cols, dropna=False):
        grp = grp.sort_values("observation_date")
        vals = pd.to_numeric(grp.get("price_value"), errors="coerce")
        vals = vals[vals.notna()]
        if vals.empty:
            continue
        obs_count = int(vals.count())
        quality = "high" if obs_count >= 8 else "medium" if obs_count >= 3 else "low"
        last_row = grp.iloc[-1].to_dict()
        avg_row = dict(last_row)
        avg_row["aggregation_level"] = "quarter_avg"
        avg_row["price_value"] = float(vals.mean())
        avg_row["quality"] = quality
        avg_row["obs_count"] = obs_count
        avg_row["parsed_note"] = f"Quarter average from {obs_count} observations."
        rows.append(avg_row)

        end_row = dict(last_row)
        end_row["aggregation_level"] = "quarter_end"
        end_row["price_value"] = float(vals.iloc[-1])
        end_row["quality"] = quality
        end_row["obs_count"] = obs_count
        end_row["parsed_note"] = f"Quarter-end reference from {obs_count} observations."
        rows.append(end_row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["quarter", "series_key", "aggregation_level", "observation_date"]).reset_index(drop=True)
