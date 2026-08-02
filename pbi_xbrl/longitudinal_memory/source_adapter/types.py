"""Closed in-memory types for deterministic source-native extraction."""
from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping


SOURCE_SCHEMA_VERSION = "1.0.0"


class SourceAdapterError(ValueError):
    """Base failure for contract, discovery, extraction or mapping errors."""


class SourceContractError(SourceAdapterError):
    """Raised when a source-set contract is not closed and internally coherent."""


class SourceDiscoveryError(SourceAdapterError):
    """Raised when declared bytes cannot be resolved exactly and safely."""


class LocatorError(SourceAdapterError):
    """Raised when a locator does not replay against the declared source bytes."""


class MappingError(SourceAdapterError):
    """Raised rather than guessing an alias, period, value form or relation."""


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DocumentSpec:
    document_key: str
    publisher_id: str
    document_type: str
    source_family: str
    relative_path: str
    expected_sha256: str
    revision: int
    authority_class: str
    publication_date: str
    publication_date_basis: str
    embedded_publication_date: str | None
    publication_date_locator: Mapping[str, Any] | None
    report_date: str | None
    accession: str | None
    canonical_url: str | None
    origin_document_key: str | None
    required: bool
    review_state: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DocumentSpec":
        fields = {field.name: value[field.name] for field in dataclasses.fields(cls)}
        locator = fields["publication_date_locator"]
        if locator is not None:
            fields["publication_date_locator"] = MappingProxyType(dict(locator))
        return cls(**fields)


@dataclass(frozen=True)
class SourceSet:
    schema_id: str
    schema_version: str
    source_set_id: str
    company_id: str
    knowledge_cutoff: str
    sector_pack_id: str
    ticker_profile_id: str
    normalized_package_ref: Mapping[str, Any]
    profile: Mapping[str, Any]
    reviewed_links: tuple[Mapping[str, Any], ...]
    periods: tuple[Mapping[str, Any], ...]
    documents: tuple[DocumentSpec, ...]
    required_assertions: tuple[Mapping[str, Any], ...]
    reviewed_model_inputs: tuple[Mapping[str, Any], ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SourceSet":
        return cls(
            schema_id=str(value["schema_id"]),
            schema_version=str(value["schema_version"]),
            source_set_id=str(value["source_set_id"]),
            company_id=str(value["company_id"]),
            knowledge_cutoff=str(value["knowledge_cutoff"]),
            sector_pack_id=str(value["sector_pack_id"]),
            ticker_profile_id=str(value["ticker_profile_id"]),
            normalized_package_ref=MappingProxyType(dict(value["normalized_package_ref"])),
            profile=MappingProxyType(dict(value["profile"])),
            reviewed_links=tuple(MappingProxyType(dict(row)) for row in value["reviewed_links"]),
            periods=tuple(MappingProxyType(dict(row)) for row in value["periods"]),
            documents=tuple(DocumentSpec.from_mapping(row) for row in value["documents"]),
            required_assertions=tuple(MappingProxyType(dict(row)) for row in value["required_assertions"]),
            reviewed_model_inputs=tuple(MappingProxyType(dict(row)) for row in value["reviewed_model_inputs"]),
        )


@dataclass(frozen=True)
class DiscoveredDocument:
    spec: DocumentSpec
    absolute_path: Path
    verified_bytes: bytes
    content_sha256: str
    source_document_id: str


@dataclass(frozen=True)
class ExtractedEvidence:
    assertion_key: str
    document_key: str
    locator_kind: str
    locator_key: str
    ordinal: int
    extraction_method_id: str
    excerpt: str
    excerpt_sha256: str
    value_text: str | None
    comparison_text: str | None
    review_state: str
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "diagnostics", MappingProxyType(dict(self.diagnostics)))
        if text_sha256(self.excerpt) != self.excerpt_sha256:
            raise LocatorError(f"Excerpt digest mismatch for {self.assertion_key!r}.")


@dataclass(frozen=True)
class MappedCandidate:
    assertion_key: str
    candidate_kind: str
    document_key: str
    evidence: ExtractedEvidence
    period_key: str | None
    semantic_key: str
    dimension_alias: str | None
    value: Mapping[str, Any] | None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.value is not None:
            object.__setattr__(self, "value", MappingProxyType(dict(self.value)))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class AdapterIssue:
    severity: str
    rule_id: str
    subject: str
    message: str
    promotion_blocking: bool


@dataclass(frozen=True)
class AdapterBuildResult:
    source_set: SourceSet
    documents: tuple[DiscoveredDocument, ...]
    extracted_evidence: tuple[ExtractedEvidence, ...]
    candidates: tuple[MappedCandidate, ...]
    package: Mapping[str, Any]
    payload: bytes
    sidecar_sha256: str
    adapter_issues: tuple[AdapterIssue, ...] = ()
