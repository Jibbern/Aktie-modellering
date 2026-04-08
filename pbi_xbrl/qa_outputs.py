from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd


AUDIT_LIKE_DEDUPE_KEY = ("quarter", "metric", "severity", "message", "source")


def normalize_audit_like_frame(
    df: Optional[pd.DataFrame],
    *,
    defaults: Dict[str, Any],
    keep_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        base_cols = list(keep_cols or defaults.keys())
        return pd.DataFrame(columns=base_cols)
    out = df.copy()
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default
    if keep_cols is not None:
        cols = [col for col in keep_cols if col in out.columns]
        out = out[cols].copy()
    return out


def rows_to_audit_like_frame(
    rows: Iterable[Dict[str, Any]],
    *,
    defaults: Dict[str, Any],
    keep_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    row_list = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    if not row_list:
        base_cols = list(keep_cols or defaults.keys())
        return pd.DataFrame(columns=base_cols)
    return normalize_audit_like_frame(pd.DataFrame(row_list), defaults=defaults, keep_cols=keep_cols)


def dedupe_audit_like_rows(
    df: Optional[pd.DataFrame],
    *,
    key_cols: Sequence[str] = AUDIT_LIKE_DEDUPE_KEY,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=list(getattr(df, "columns", []) or []))
    out = df.copy()
    missing = [col for col in key_cols if col not in out.columns]
    for col in missing:
        out[col] = ""
    dedupe_cols = list(key_cols)
    normalized = out[dedupe_cols].copy()
    for col in dedupe_cols:
        if col == "quarter":
            normalized[col] = pd.to_datetime(normalized[col], errors="coerce").astype(str)
        else:
            normalized[col] = normalized[col].fillna("").astype(str).str.strip()
    keep_mask = ~normalized.duplicated(keep="first")
    return out.loc[keep_mask].copy()


def concat_audit_like_frames(
    *frames: Optional[pd.DataFrame],
    key_cols: Sequence[str] = AUDIT_LIKE_DEDUPE_KEY,
) -> pd.DataFrame:
    usable = [frame.copy() for frame in frames if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not usable:
        return pd.DataFrame()
    out = pd.concat(usable, ignore_index=True)
    return dedupe_audit_like_rows(out, key_cols=key_cols)
