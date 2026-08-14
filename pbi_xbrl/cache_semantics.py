"""Canonical semantic versions and deterministic identities for persisted caches.

This module owns cache *identity mechanics*, not cache business logic or storage.
Each producer remains responsible for choosing the source, configuration, profile,
period, and code inputs that can change its own output.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence


class CacheIdentityError(ValueError):
    """Raised when a required semantic cache identity cannot be constructed."""


CACHE_IDENTITY_CONTRACT = "contract:semantic-cache-identity@1"
CACHE_IDENTITY_SERIALIZATION_VERSION = "v1_canonical_json_sha256"

# Existing product/extraction versions are gathered here without changing their
# accepted meaning.  Cache-specific builders import only the dimensions they own.
PIPELINE_BUNDLE_CACHE_VERSION = 2
PIPELINE_STAGE_CACHE_VERSION = 8
REVOLVER_CACHE_VERSION = 4

GAAP_HISTORY_STAGE_VERSION = "v4"
DEBT_TRANCHES_STAGE_VERSION = "v2"
TIER3_NON_GAAP_STAGE_VERSION = "v3"
LOCAL_NON_GAAP_FALLBACK_VERSION = 32
LOCAL_NON_GAAP_PDF_PAGE_CACHE_VERSION = 1
LOCAL_NON_GAAP_PDF_MANIFEST_VERSION = "v2_content_sha256"
DEBT_SCHEDULE_STAGE_VERSION = "v2"
DEBT_CREDIT_NOTES_STAGE_VERSION = "v1"
REVOLVER_STAGE_VERSION = "v2"
DOC_INTEL_BEHAVIOR_VERSION = "v20_adjusted_metric_owner"
COMPANY_OVERVIEW_BEHAVIOR_VERSION = "v9_anf_summary_sanitize"

ADJUSTED_METRIC_UNIT_NORMALIZATION_VERSION = "v1_table_local_source_unit"
NON_GAAP_ADJUSTMENT_DOMAIN_VERSION = "v1_table_role_measure_domain"
FINANCIAL_STATEMENT_DOCUMENT_PERIOD_VERSION = "v2_registered_document_identity"
INLINE_XBRL_FACT_TEXT_VERSION = "v1_continued_at_chain"
DEBT_TABLE_PERIOD_OWNERSHIP_VERSION = "v1_visual_xbrl_context"
DEBT_RATE_SEMANTIC_OWNERSHIP_VERSION = "v1_role_period_authority"
ADJUSTED_METRIC_HISTORY_SELECTION_VERSION = "v1_metric_definition_scope"
DOC_TEXT_EXTRACTOR_VERSION = "v2"

MARKET_INPUT_FINGERPRINT_VERSION = "v2_content_sha256"
MARKET_EXPORT_CACHE_VERSION = "v2"
MARKET_PROVIDER_PARSE_DEFAULT_VERSION = "v1"
MARKET_PROVIDER_PARSE_VERSIONS = MappingProxyType(
    {
        "ams_3617": "v8",
        "ams_3618": "v5",
        "cme_ethanol_platts": "v2",
        "local_barchart_corn_futures": "v1",
        "local_barchart_gas_futures": "v1",
        "local_chicago_ethanol_futures": "v2",
        "nwer": "v9",
    }
)
GENERIC_SOURCE_NOTE_RESCUE_CACHE_VERSION = "generic_source_note_rescue_cache_v4_content_sha256"
BUYBACK_AUTH_CACHE_VERSION = "buyback_auth_remaining_cache_v2_content_sha256"
BUYBACK_AUTH_DIRECT_CACHE_VERSION = "buyback_auth_remaining_cache_v2_content_sha256_direct_docs"
GPRE_BASIS_PROXY_WRITER_CACHE_VERSION = "gpre_basis_proxy_writer_cache_v6"


SEMANTIC_CACHE_VERSIONS = MappingProxyType(
    {
        "cache_identity": CACHE_IDENTITY_SERIALIZATION_VERSION,
        "pipeline_bundle": PIPELINE_BUNDLE_CACHE_VERSION,
        "pipeline_stage": PIPELINE_STAGE_CACHE_VERSION,
        "revolver_document": REVOLVER_CACHE_VERSION,
        "gaap_history": GAAP_HISTORY_STAGE_VERSION,
        "debt_tranches": DEBT_TRANCHES_STAGE_VERSION,
        "tier3_non_gaap": TIER3_NON_GAAP_STAGE_VERSION,
        "local_non_gaap_fallback": LOCAL_NON_GAAP_FALLBACK_VERSION,
        "local_non_gaap_pdf_page": LOCAL_NON_GAAP_PDF_PAGE_CACHE_VERSION,
        "local_non_gaap_pdf_manifest": LOCAL_NON_GAAP_PDF_MANIFEST_VERSION,
        "debt_schedule": DEBT_SCHEDULE_STAGE_VERSION,
        "debt_credit_notes": DEBT_CREDIT_NOTES_STAGE_VERSION,
        "revolver_stage": REVOLVER_STAGE_VERSION,
        "doc_intel": DOC_INTEL_BEHAVIOR_VERSION,
        "company_overview": COMPANY_OVERVIEW_BEHAVIOR_VERSION,
        "unit_norm": ADJUSTED_METRIC_UNIT_NORMALIZATION_VERSION,
        "adjustment_domain": NON_GAAP_ADJUSTMENT_DOMAIN_VERSION,
        "document_period": FINANCIAL_STATEMENT_DOCUMENT_PERIOD_VERSION,
        "inline_xbrl_text": INLINE_XBRL_FACT_TEXT_VERSION,
        "debt_period": DEBT_TABLE_PERIOD_OWNERSHIP_VERSION,
        "debt_rate": DEBT_RATE_SEMANTIC_OWNERSHIP_VERSION,
        "adjusted_history": ADJUSTED_METRIC_HISTORY_SELECTION_VERSION,
        "doc_text": DOC_TEXT_EXTRACTOR_VERSION,
        "market_input": MARKET_INPUT_FINGERPRINT_VERSION,
        "market_export": MARKET_EXPORT_CACHE_VERSION,
        "market_provider_parse": MARKET_PROVIDER_PARSE_VERSIONS,
        "generic_source_note_rescue": GENERIC_SOURCE_NOTE_RESCUE_CACHE_VERSION,
        "buyback_authorization": BUYBACK_AUTH_CACHE_VERSION,
        "buyback_authorization_direct": BUYBACK_AUTH_DIRECT_CACHE_VERSION,
        "gpre_basis_proxy_writer": GPRE_BASIS_PROXY_WRITER_CACHE_VERSION,
    }
)


_WEAK_REQUIRED_STRINGS = frozenset({"", "none", "unknown", "default"})


def _canonical(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CacheIdentityError("cache identity cannot serialize NaN or infinity")
        return 0.0 if value == 0.0 else value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return _canonical(value.value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"byte_length": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, Path):
        raise CacheIdentityError(
            "filesystem paths are not canonical cache identity values; pass a logical name or content digest"
        )
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CacheIdentityError(f"cache identity mapping key must be str, got {type(key).__name__}")
            out[key] = _canonical(item)
        return {key: out[key] for key in sorted(out)}
    if isinstance(value, (set, frozenset)):
        items = [_canonical(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    raise CacheIdentityError(f"unsupported cache identity value type: {type(value).__name__}")


def canonical_cache_json_bytes(value: Any) -> bytes:
    """Serialize supported identity values without repr, address, or dict-order drift."""

    return json.dumps(
        _canonical(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _require_identity_fields(payload: Mapping[str, Any], required_fields: Sequence[str]) -> None:
    for field in required_fields:
        if field not in payload:
            raise CacheIdentityError(f"required cache identity field is missing: {field}")
        value = payload[field]
        if value is None:
            raise CacheIdentityError(f"required cache identity field is null: {field}")
        if isinstance(value, str) and value.strip().lower() in _WEAK_REQUIRED_STRINGS:
            raise CacheIdentityError(f"required cache identity field is weak: {field}={value!r}")


@dataclass(frozen=True)
class CacheIdentity:
    contract_id: str
    digest: str
    canonical_payload: bytes

    @property
    def key(self) -> str:
        return f"{CACHE_IDENTITY_SERIALIZATION_VERSION}:{self.contract_id}:{self.digest}"


def build_cache_identity(
    contract_id: str,
    payload: Mapping[str, Any],
    *,
    required_fields: Sequence[str] = (),
) -> CacheIdentity:
    """Build a fail-closed SHA-256 identity from a cache-owned semantic payload."""

    contract = str(contract_id or "").strip()
    if contract.lower() in _WEAK_REQUIRED_STRINGS:
        raise CacheIdentityError(f"cache contract id is unresolved: {contract_id!r}")
    if not isinstance(payload, Mapping):
        raise CacheIdentityError("cache identity payload must be a mapping")
    _require_identity_fields(payload, required_fields)
    envelope = {
        "cache_identity_contract": CACHE_IDENTITY_CONTRACT,
        "contract_id": contract,
        "payload": dict(payload),
        "serialization_version": CACHE_IDENTITY_SERIALIZATION_VERSION,
    }
    canonical = canonical_cache_json_bytes(envelope)
    return CacheIdentity(contract_id=contract, digest=hashlib.sha256(canonical).hexdigest(), canonical_payload=canonical)


def file_content_sha256(path: Path) -> str:
    """Return the SHA-256 of actual bytes or fail explicitly."""

    source = Path(path)
    digest = hashlib.sha256()
    try:
        with source.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CacheIdentityError(f"cannot read cache-identity source: {source}") from exc
    return digest.hexdigest()


def sec_cache_source_identity(cache_dir: Path) -> str:
    """Identify published SEC metadata and document bytes consumed by the pipeline."""

    root = Path(cache_dir).expanduser().resolve()
    paths: list[Path] = []
    for pattern in (
        "submissions_*.json",
        "companyfacts_*.json",
        "index_*.json",
        "doc_*",
    ):
        paths.extend(path for path in root.glob(pattern) if path.is_file())
    return content_file_set_identity(
        paths,
        contract_id="pipeline-sec-source-set",
        logical_root=root,
        include_logical_names=True,
    )


def content_file_set_identity(
    paths: Iterable[Path],
    *,
    contract_id: str,
    logical_root: Path | None = None,
    include_logical_names: bool = True,
    max_files: int | None = None,
) -> str:
    """Hash a bounded set of file bytes without machine-specific absolute paths."""

    root = Path(logical_root).expanduser().resolve() if logical_root is not None else None
    records: list[dict[str, Any]] = []
    candidates = {Path(path) for path in paths if path is not None}
    for source in sorted(candidates, key=lambda item: item.as_posix().casefold()):
        if max_files is not None and len(records) >= int(max_files):
            break
        if not source.is_file():
            continue
        record: dict[str, Any] = {"sha256": file_content_sha256(source)}
        if include_logical_names:
            if root is not None:
                try:
                    label = source.expanduser().resolve().relative_to(root).as_posix()
                except (OSError, ValueError) as exc:
                    raise CacheIdentityError(f"cache source is outside logical root: {source} root={root}") from exc
            else:
                label = source.name
            record["logical_name"] = label
        records.append(record)
    records.sort(key=lambda item: canonical_cache_json_bytes(item))
    return build_cache_identity(
        contract_id,
        {"files": records},
    ).digest


def module_content_identity(package_root: Path, relative_names: Iterable[str], *, contract_id: str) -> str:
    """Hash explicitly named owning modules by relative name and content."""

    root = Path(package_root).expanduser().resolve()
    records: list[dict[str, str]] = []
    for relative_name in sorted({str(name).replace("\\", "/") for name in relative_names if str(name).strip()}):
        source = root / Path(relative_name)
        if not source.is_file():
            raise CacheIdentityError(f"required cache-owning module is missing: {source}")
        records.append({"module": relative_name, "sha256": file_content_sha256(source)})
    if not records:
        raise CacheIdentityError("no cache-owning modules were supplied")
    return build_cache_identity(contract_id, {"modules": records}, required_fields=("modules",)).digest


__all__ = [
    "ADJUSTED_METRIC_HISTORY_SELECTION_VERSION",
    "ADJUSTED_METRIC_UNIT_NORMALIZATION_VERSION",
    "BUYBACK_AUTH_CACHE_VERSION",
    "BUYBACK_AUTH_DIRECT_CACHE_VERSION",
    "CACHE_IDENTITY_CONTRACT",
    "CACHE_IDENTITY_SERIALIZATION_VERSION",
    "COMPANY_OVERVIEW_BEHAVIOR_VERSION",
    "CacheIdentity",
    "CacheIdentityError",
    "DEBT_CREDIT_NOTES_STAGE_VERSION",
    "DEBT_SCHEDULE_STAGE_VERSION",
    "DEBT_TABLE_PERIOD_OWNERSHIP_VERSION",
    "DEBT_RATE_SEMANTIC_OWNERSHIP_VERSION",
    "DEBT_TRANCHES_STAGE_VERSION",
    "DOC_INTEL_BEHAVIOR_VERSION",
    "DOC_TEXT_EXTRACTOR_VERSION",
    "FINANCIAL_STATEMENT_DOCUMENT_PERIOD_VERSION",
    "GAAP_HISTORY_STAGE_VERSION",
    "GENERIC_SOURCE_NOTE_RESCUE_CACHE_VERSION",
    "GPRE_BASIS_PROXY_WRITER_CACHE_VERSION",
    "INLINE_XBRL_FACT_TEXT_VERSION",
    "LOCAL_NON_GAAP_FALLBACK_VERSION",
    "LOCAL_NON_GAAP_PDF_MANIFEST_VERSION",
    "LOCAL_NON_GAAP_PDF_PAGE_CACHE_VERSION",
    "MARKET_EXPORT_CACHE_VERSION",
    "MARKET_INPUT_FINGERPRINT_VERSION",
    "MARKET_PROVIDER_PARSE_DEFAULT_VERSION",
    "MARKET_PROVIDER_PARSE_VERSIONS",
    "NON_GAAP_ADJUSTMENT_DOMAIN_VERSION",
    "PIPELINE_BUNDLE_CACHE_VERSION",
    "PIPELINE_STAGE_CACHE_VERSION",
    "REVOLVER_CACHE_VERSION",
    "REVOLVER_STAGE_VERSION",
    "SEMANTIC_CACHE_VERSIONS",
    "TIER3_NON_GAAP_STAGE_VERSION",
    "build_cache_identity",
    "canonical_cache_json_bytes",
    "content_file_set_identity",
    "file_content_sha256",
    "module_content_identity",
    "sec_cache_source_identity",
]
