"""Helpers for local structured transcript/source metadata companions.

Conference and transcript folders often contain two files for the same event:
- a raw transcript/source text, useful for source QA and audit trail
- a `*_METADATA_EN.txt` companion, useful for deterministic extraction

These helpers keep that split explicit so downstream model code can prefer the
structured metadata without losing the raw transcript as provenance support.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Tuple


METADATA_SUFFIX = "_METADATA_EN.TXT"
METADATA_SOURCE_FILE_KEYS = (
    "source_file",
    "source_txt_file",
    "source_pdf_file",
    "source_htm_file",
    "source_html_file",
    "source_document",
    "source_doc",
)


def is_conference_metadata_path(path: Any) -> bool:
    """Return True for structured metadata companion files.

    The original helper name is kept for compatibility; the suffix convention is
    now shared by conferences, earnings transcripts, and similar narrative
    source companions.
    """
    return is_structured_metadata_path(path)


def is_structured_metadata_path(path: Any) -> bool:
    """Return True for curated `*_METADATA_EN.txt` source companions."""
    name = str(getattr(path, "name", path) or "").strip()
    return name.upper().endswith(METADATA_SUFFIX)


def conference_source_role(path: Any) -> str:
    """Workbook/audit role label for structured conference files."""
    return source_material_role(path)


def source_material_role(path: Any) -> str:
    """Workbook/audit role label for a local source companion."""
    return "metadata_primary" if is_structured_metadata_path(path) else "source_qa_raw"


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


def metadata_source_file(values: Any) -> str:
    """Return the raw source companion filename declared by metadata.

    Older conference files used explicit keys such as `source_txt_file`, while
    transcript/CEO-letter files mostly use `source_file`. Treat these as the same
    provenance pointer so metadata can be the primary extraction source and the
    raw transcript/PDF can still be retained as source-QA support.
    """
    if not isinstance(values, dict):
        values = parse_metadata_key_values(str(values or ""))
    for key in METADATA_SOURCE_FILE_KEYS:
        raw = str(values.get(key) or "").strip()
        if raw:
            return raw
    return ""


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


def metadata_audit_flags(value: Any) -> Tuple[str, ...]:
    """Return normalized audit flags from metadata text or parsed key-values.

    Flags are advisory guardrails for downstream code: they tell model builders
    which curated datapoints still need filing confirmation before becoming
    filing-grade facts.
    """
    if isinstance(value, dict):
        raw = value.get("audit_flag") or value.get("audit_flags") or ""
    else:
        raw = parse_metadata_key_values(str(value or "")).get("audit_flag", "")
    parts = re.split(r"[;\n,]+", str(raw or ""))
    return tuple(part.strip() for part in parts if part.strip())


def metadata_has_audit_flag_for(value: Any, *terms: str) -> bool:
    """Return True when metadata audit flags mention one of the requested terms."""
    flags_blob = " ".join(metadata_audit_flags(value)).replace("_", " ").lower()
    return any(str(term or "").replace("_", " ").lower() in flags_blob for term in terms if str(term or "").strip())
