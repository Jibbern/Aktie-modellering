"""Typed containers shared across market-data providers and service code.

These dataclasses describe the normalized frames and metadata that move from
provider parsers into cache persistence and finally into provider-agnostic
exports consumed by workbook overlays.
"""
from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclasses.dataclass(frozen=True)
class SourceFrameSpec:
    df: pd.DataFrame
    date_col: str
    source_file_col: str
    source_type: str
    aggregation_level: str = "observation"
    priority: int = 0


@dataclasses.dataclass(frozen=True)
class RawManifestEntry:
    source: str
    source_id: str
    report_date: str
    publication_date: str
    local_path: str
    size: int
    checksum: str
    download_status: str


@dataclasses.dataclass(frozen=True)
class ParsedManifestEntry:
    source: str
    local_path: str
    raw_fingerprint: str
    parse_version: str
    parsed_at: str
    row_count: int
    parse_status: str


@dataclasses.dataclass(frozen=True)
class SyncSummary:
    sources_enabled: tuple[str, ...]
    raw_added: int = 0
    raw_refreshed: int = 0
    raw_skipped: int = 0
    parsed_sources: tuple[str, ...] = tuple()
    export_rows: int = 0
    export_path: Optional[Path] = None
