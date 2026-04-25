"""Canonical local Chicago ethanol futures provider.

The active GPRE workflow consumes locally archived Chicago ethanol futures CSVs
and optional quarter-open manual snapshot files. The legacy
`cme_ethanol_platts` module/provider remains available as a compatibility shim,
but this module is the canonical home for the local-source workflow.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .cme_ethanol_platts import (
    CMEChicagoEthanolPlattsProvider,
    find_local_manual_ethanol_quarter_open_files,
    load_local_manual_ethanol_quarter_open_snapshot_rows,
    parse_cme_ethanol_settlement_table,
    parse_manual_ethanol_quarter_open_snapshot_table,
)


def parse_local_chicago_ethanol_futures_table(
    path: Path,
    *,
    fallback_date: pd.Timestamp | None,
) -> List[Dict[str, Any]]:
    rows = parse_cme_ethanol_settlement_table(path, fallback_date=fallback_date)
    out: List[Dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        row["source"] = "local_chicago_ethanol_futures"
        out.append(row)
    return out


class LocalChicagoEthanolFuturesProvider(CMEChicagoEthanolPlattsProvider):
    """Canonical provider id for the local Chicago ethanol futures workflow."""

    source = "local_chicago_ethanol_futures"
    stable_name_prefix = "local_chicago_ethanol_futures"
    report_token = "local_chicago_ethanol_futures"

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        parsed_df = super().parse_raw_to_rows(cache_root, ticker_root, raw_entries)
        if parsed_df is None or parsed_df.empty:
            return parsed_df
        out = parsed_df.copy()
        out["source"] = self.source
        return out


__all__ = [
    "LocalChicagoEthanolFuturesProvider",
    "find_local_manual_ethanol_quarter_open_files",
    "load_local_manual_ethanol_quarter_open_snapshot_rows",
    "parse_local_chicago_ethanol_futures_table",
    "parse_manual_ethanol_quarter_open_snapshot_table",
]
