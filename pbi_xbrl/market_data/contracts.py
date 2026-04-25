from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable, List

import pandas as pd


MARKET_ROWS_DF_NORMALIZED_ATTR = "_market_rows_df_normalized"
MARKET_ROWS_SCHEMA_DEFAULTS: Dict[str, Any] = {
    "observation_date": None,
    "quarter": None,
    "price_value": None,
    "aggregation_level": "",
    "series_key": "",
    "contract_tenor": "",
    "source_type": "",
    "source_file": "",
    "parsed_text": "",
}
MARKET_ROWS_EMPTY_COLUMNS: List[str] = [
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


def normalize_market_rows_df(rows: Iterable[Dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
    """Normalize market rows into the shared empty/schema-stable frame contract."""

    def _typed_empty_frame() -> pd.DataFrame:
        empty = pd.DataFrame(columns=MARKET_ROWS_EMPTY_COLUMNS)
        empty["observation_date"] = pd.to_datetime(empty["observation_date"], errors="coerce")
        empty["quarter"] = pd.to_datetime(empty["quarter"], errors="coerce")
        empty["price_value"] = pd.to_numeric(empty["price_value"], errors="coerce")
        empty.attrs[MARKET_ROWS_DF_NORMALIZED_ATTR] = True
        return empty

    if isinstance(rows, pd.DataFrame):
        if bool(rows.attrs.get(MARKET_ROWS_DF_NORMALIZED_ATTR)):
            return rows
        df = rows.copy()
    else:
        df = pd.DataFrame(list(rows or []))
    if df.empty:
        return _typed_empty_frame()
    for col, default in MARKET_ROWS_SCHEMA_DEFAULTS.items():
        if col not in df.columns:
            df[col] = default
    df["observation_date"] = pd.to_datetime(df.get("observation_date"), errors="coerce")
    df["quarter"] = pd.to_datetime(df.get("quarter"), errors="coerce")
    df["price_value"] = pd.to_numeric(df.get("price_value"), errors="coerce")
    df["aggregation_level"] = df.get("aggregation_level").astype(str)
    df["series_key"] = df.get("series_key").astype(str)
    df["contract_tenor"] = df.get("contract_tenor").fillna("").astype(str)
    df.attrs[MARKET_ROWS_DF_NORMALIZED_ATTR] = True
    return df


def require_market_columns(df: pd.DataFrame, required: Iterable[str], *, contract_name: str) -> pd.DataFrame:
    """Fail fast for internal code paths that assume a normalized market frame."""

    required_cols = [str(col or "").strip() for col in required if str(col or "").strip()]
    missing = [col for col in required_cols if col not in set(df.columns)]
    if missing:
        missing_txt = ", ".join(sorted(missing))
        raise ValueError(f"{contract_name} requires normalized market columns: {missing_txt}")
    return df
