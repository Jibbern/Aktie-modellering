"""Deterministic UTF-8 sidecar serialization without generated timestamps."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from .identity import build_identity, canonical_company_id


IDENTITY_ARRAY_KEYS = frozenset(
    {
        "candidate_record_ids",
        "maximal_candidate_ids",
        "input_record_ids",
        "entity_ids",
        "evidence_occurrence_ids",
        "review_issue_ids",
    }
)
IDENTITY_OBJECT_KEYS = (
    "relation_id",
    "resolution_id",
    "record_id",
    "entity_id",
    "evidence_occurrence_id",
    "source_document_id",
    "issue_id",
    "dimension_set_id",
    "member_id",
    "metric_id",
    "definition_id",
    "basis_id",
    "unit_id",
    "dimension_id",
    "policy_id",
    "rule_id",
    "method_id",
    "period_id",
    "calendar_id",
)


class SerializationError(ValueError):
    """Raised when JSON output would be ambiguous or non-deterministic."""


def _object_identity(value: Mapping[str, Any]) -> str | None:
    header = value.get("header")
    if isinstance(header, Mapping):
        for key in ("entity_id", "record_id"):
            if header.get(key):
                return str(header[key])
    for key in IDENTITY_OBJECT_KEYS:
        if value.get(key):
            return str(value[key])
    return None


def canonicalize(value: Any, *, parent_key: str = "") -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): canonicalize(child, parent_key=str(key))
            for key, child in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        canonical = [canonicalize(child) for child in value]
        if parent_key in IDENTITY_ARRAY_KEYS:
            return sorted(canonical)
        if parent_key == "members" and all(isinstance(child, Mapping) for child in canonical):
            return sorted(canonical, key=lambda child: (str(child.get("dimension_id", "")), str(child.get("member_id", ""))))
        if canonical and all(isinstance(child, Mapping) and _object_identity(child) is not None for child in canonical):
            return sorted(canonical, key=lambda child: str(_object_identity(child)))
        if all(isinstance(child, str) for child in canonical):
            return sorted(canonical)
        return canonical
    if isinstance(value, float):
        raise SerializationError("Floats are forbidden; use canonical decimal strings.")
    return value


def serialize_package(package: Mapping[str, Any], path: Path | str | None = None) -> bytes:
    if "generated_at_utc" in package:
        raise SerializationError("Longitudinal sidecars cannot contain a generated timestamp.")
    canonical = canonicalize(package)
    try:
        text = json.dumps(
            canonical,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
    except (TypeError, ValueError) as exc:
        raise SerializationError(f"Package cannot be serialized losslessly: {exc}") from exc
    payload = text.replace("\r\n", "\n").encode("utf-8")
    if path is not None:
        Path(path).write_bytes(payload)
    return payload


def semantic_snapshot_identity(normalized_package: Mapping[str, Any]) -> str:
    """Identify a normalized semantic snapshot, excluding only root generated_at_utc."""

    snapshot = deepcopy(dict(normalized_package))
    snapshot.pop("generated_at_utc", None)
    payload = json.dumps(
        canonicalize(snapshot),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return build_identity("normalized-snapshot", (("sha256", digest),))


def runtime_sidecar_filename(company_id: str) -> str:
    return f"{canonical_company_id(company_id)}_longitudinal_company_memory.v1.json"
