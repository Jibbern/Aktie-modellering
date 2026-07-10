"""Source-native evidence contracts for the future generic new-ticker path.

These dataclasses deliberately contain no parser or workbook behavior.  They make
the handoff explicit: extraction produces evidence, normalization resolves it, and
the workbook layer receives only normalized values plus lineage references.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class SourceDocument:
    document_id: str
    ticker: str
    source_type: str
    source_url: str
    content_hash: str
    published_at: str = ""
    filing_form: str = ""
    accession: str = ""


@dataclass(frozen=True)
class SourceSnippet:
    snippet_id: str
    document_id: str
    locator: str
    text: str
    page_number: int | None = None
    start_offset: int | None = None
    end_offset: int | None = None


@dataclass(frozen=True)
class TableCellEvidence:
    evidence_id: str
    document_id: str
    table_locator: str
    row_label: str
    column_label: str
    raw_value: Any
    unit: str = ""
    period: str = ""
    dimensions: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class XbrlFactEvidence:
    evidence_id: str
    document_id: str
    concept: str
    value: Any
    unit: str
    period_start: str = ""
    period_end: str = ""
    dimensions: Mapping[str, str] = field(default_factory=dict)
    context_id: str = ""


@dataclass(frozen=True)
class EvidenceCandidate:
    candidate_id: str
    field_family: str
    evidence_role: str
    value: Any
    source_refs: Sequence[str]
    period: str = ""
    unit: str = ""
    dimensions: Mapping[str, str] = field(default_factory=dict)
    text: str = ""
    confidence: float | None = None
    extraction_method: str = ""
    parser_version: str = ""


@dataclass(frozen=True)
class EvidenceSet:
    evidence_set_id: str
    field_path: str
    candidates: Sequence[EvidenceCandidate]
    resolution_policy: str
    selected_candidate_ids: Sequence[str] = field(default_factory=tuple)
    conflict_status: str = "none"


@dataclass(frozen=True)
class NormalizedFieldEvidenceRef:
    field_path: str
    evidence_set_id: str
    selected_candidate_ids: Sequence[str]
    resolution_rule_id: str
    calculation_id: str = ""
    confidence: float | None = None
