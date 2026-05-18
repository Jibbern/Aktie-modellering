"""Helpers for local conference transcript metadata companions.

Conference folders often contain two files for the same event:
- a raw transcript/source text, useful for source QA and audit trail
- a `*_METADATA_EN.txt` companion, useful for deterministic extraction

These helpers keep that split explicit so downstream model code can prefer the
structured metadata without losing the raw transcript as provenance support.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict


def is_conference_metadata_path(path: Any) -> bool:
    """Return True for structured conference metadata companion files."""
    name = str(getattr(path, "name", path) or "").strip()
    return name.upper().endswith("_METADATA_EN.TXT")


def conference_source_role(path: Any) -> str:
    """Workbook/audit role label for conference files."""
    return "metadata_primary" if is_conference_metadata_path(path) else "source_qa_raw"


def parse_metadata_key_values(text: str) -> Dict[str, str]:
    """Parse simple `key = value` metadata lines into lowercase keys.

    Section headers are intentionally ignored; these metadata files are curated
    to keep key names unique enough for deterministic extraction.
    """
    out: Dict[str, str] = {}
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("[") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key_norm = re.sub(r"\s+", "_", key.strip().lower())
        value_norm = value.strip()
        if key_norm and value_norm:
            out[key_norm] = value_norm
    return out


def parse_metadata_number(value: Any) -> float | None:
    """Extract the first numeric token from metadata values.

    Handles values like `approximately_75`, `15_to_25`, `$188m`, and `1.75`.
    Range handling is deliberately conservative: callers that need a range
    should parse both endpoints themselves; this helper returns the first value.
    """
    raw = str(value or "").replace("_", " ")
    match = re.search(r"-?\d+(?:\.\d+)?", raw)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None

