"""Pure domain types for the longitudinal company-memory contract."""
from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Literal, Mapping, TypedDict, cast

from .identity import identity_digest


SCHEMA_VERSION = "1.0.0"
DECIMAL_PATTERN = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


class DomainValidationError(ValueError):
    """Raised when a domain value would lose meaning or precision."""


def canonical_decimal(value: str | int | Decimal) -> str:
    """Return a non-exponent decimal string without coercing through float."""

    raw = str(value)
    if not DECIMAL_PATTERN.fullmatch(raw):
        raise DomainValidationError(f"Expected a canonical decimal string, received {value!r}.")
    try:
        parsed = Decimal(raw)
    except InvalidOperation as exc:  # pragma: no cover - guarded by regex
        raise DomainValidationError(f"Invalid decimal {value!r}.") from exc
    if not parsed.is_finite():
        raise DomainValidationError(f"Decimal must be finite, received {value!r}.")
    if parsed == 0:
        return "0"
    normalized = format(parsed, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


@dataclass(frozen=True)
class ExactValue:
    value: str
    kind: str = field(default="exact", init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", canonical_decimal(self.value))

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "value": self.value}


@dataclass(frozen=True)
class ApproximateValue:
    value: str
    qualifier: str
    tolerance: str | None = None
    kind: str = field(default="approximate", init=False)

    def __post_init__(self) -> None:
        if self.qualifier not in {"around", "about", "approximately", "tilde"}:
            raise DomainValidationError(f"Unsupported approximation qualifier {self.qualifier!r}.")
        object.__setattr__(self, "value", canonical_decimal(self.value))
        if self.tolerance is not None:
            tolerance = canonical_decimal(self.tolerance)
            if Decimal(tolerance) < 0:
                raise DomainValidationError("Approximation tolerance cannot be negative.")
            object.__setattr__(self, "tolerance", tolerance)

    def to_dict(self) -> dict[str, str | None]:
        return {"kind": self.kind, "value": self.value, "qualifier": self.qualifier, "tolerance": self.tolerance}


@dataclass(frozen=True)
class RangeValue:
    low: str
    high: str
    low_inclusive: bool = True
    high_inclusive: bool = True
    kind: str = field(default="range", init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "low", canonical_decimal(self.low))
        object.__setattr__(self, "high", canonical_decimal(self.high))
        if Decimal(self.low) > Decimal(self.high):
            raise DomainValidationError("Range low cannot exceed range high.")

    def to_dict(self) -> dict[str, str | bool]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class BoundValue:
    operator: str
    value: str
    kind: str = field(default="bound", init=False)

    def __post_init__(self) -> None:
        if self.operator not in {"gt", "gte", "lt", "lte"}:
            raise DomainValidationError(f"Unsupported bound operator {self.operator!r}.")
        object.__setattr__(self, "value", canonical_decimal(self.value))

    def to_dict(self) -> dict[str, str]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class QualitativeValue:
    text: str
    normalized_band: str | None = None
    kind: str = field(default="qualitative", init=False)

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise DomainValidationError("Qualitative values require text.")

    def to_dict(self) -> dict[str, str | None]:
        return dataclasses.asdict(self)


ValueSpec = ExactValue | ApproximateValue | RangeValue | BoundValue | QualitativeValue


class ExactValueRecord(TypedDict):
    kind: Literal["exact"]
    value: str


class ApproximateValueRecord(TypedDict):
    kind: Literal["approximate"]
    value: str
    qualifier: str
    tolerance: str | None


class RangeValueRecord(TypedDict):
    kind: Literal["range"]
    low: str
    high: str
    low_inclusive: bool
    high_inclusive: bool


class BoundValueRecord(TypedDict):
    kind: Literal["bound"]
    operator: str
    value: str


class QualitativeValueRecord(TypedDict):
    kind: Literal["qualitative"]
    text: str
    normalized_band: str | None


ValueSpecRecord = ExactValueRecord | ApproximateValueRecord | RangeValueRecord | BoundValueRecord | QualitativeValueRecord


class DeadlineRecord(TypedDict):
    kind: Literal["date", "period"]
    value: str
    precision: str


class GuidanceSeriesPayload(TypedDict):
    kind: Literal["GuidanceSeries"]
    metric_id: str
    definition_id: str
    basis_id: str
    horizon_period_id: str
    dimension_set_id: str
    unit_id: str
    currency: str | None


class PromisePayload(TypedDict):
    kind: Literal["Promise"]
    promise_subject_id: str
    program_id: str | None
    origin_occurrence_id: str
    origin_version_id: str
    original_wording: str
    original_target: ValueSpecRecord | None
    original_baseline: ValueSpecRecord | None
    original_deadline: DeadlineRecord | None


EntityPayload = GuidanceSeriesPayload | PromisePayload


class NumericalFactPayload(TypedDict):
    kind: Literal["NumericalFact"]
    business_key: str
    metric_id: str
    definition_id: str
    basis_id: str
    unit_id: str
    currency: str | None
    value: ExactValueRecord | ApproximateValueRecord | RangeValueRecord | BoundValueRecord


class GuidanceVersionPayload(TypedDict):
    kind: Literal["GuidanceVersion"]
    guidance_series_id: str
    version_kind: str
    value: ValueSpecRecord
    wording: str


class PromiseVersionPayload(TypedDict):
    kind: Literal["PromiseVersion"]
    promise_id: str
    previous_version_id: str | None
    change_kind: str
    version_state: str
    wording: str
    target: ValueSpecRecord | None
    baseline: ValueSpecRecord | None
    deadline: DeadlineRecord | None


class ManagementStatementPayload(TypedDict):
    kind: Literal["ManagementStatement"]
    statement_kind: str
    topic_id: str
    statement_period_id: str
    speaker_id: str
    statement: str


class CompanyEventPayload(TypedDict):
    kind: Literal["CompanyEvent"]
    event_type: str
    event_subject_id: str
    event_stage: str
    description: str
    effective_date: str | None
    effective_month: str | None
    effective_precision: str


class ModelInterpretationPayload(TypedDict):
    kind: Literal["ModelInterpretation"]
    interpretation_key: str
    as_of_period_id: str
    method_id: str
    producer_id: str
    input_record_ids: list[str]
    revision: int
    interpretation: str
    authority_class: str


class AvailabilityObservationPayload(TypedDict):
    kind: Literal["AvailabilityObservation"]
    business_key: str
    availability_state: str
    reason: str


class ComparabilityRecord(TypedDict):
    comparable: Literal[True]
    reason: str
    checks: dict[str, bool]


class ChangeObservationPayload(TypedDict):
    kind: Literal["ChangeObservation"]
    change_kind: str
    from_record_id: str
    to_record_id: str
    input_record_ids: list[str]
    rule_id: str
    comparability: ComparabilityRecord
    value: ExactValueRecord
    unit_id: str


ObservationPayload = (
    NumericalFactPayload
    | GuidanceVersionPayload
    | PromiseVersionPayload
    | ManagementStatementPayload
    | CompanyEventPayload
    | ModelInterpretationPayload
    | AvailabilityObservationPayload
    | ChangeObservationPayload
)


@dataclass(frozen=True)
class SourceDocument:
    source_document_id: str
    company_id: str
    publisher_id: str
    document_type: str
    publication_date: str
    document_key: str
    revision: int
    origin_document_id: str | None
    title: str
    source_path_hint: str
    canonical_url: str | None
    content_sha256: str | None
    authority_class: str
    review_state: str = "accepted"
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["identity_digest"] = identity_digest(self.source_document_id)
        return result


@dataclass(frozen=True)
class EvidenceOccurrence:
    evidence_occurrence_id: str
    company_id: str
    source_document_id: str
    occurrence_key: str
    locator_kind: str
    locator_key: str
    ordinal: int
    excerpt: str
    review_state: str = "accepted"
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["identity_digest"] = identity_digest(self.evidence_occurrence_id)
        return result


@dataclass(frozen=True)
class ObservationHeader:
    record_id: str
    record_type: str
    company_id: str
    subject_id: str
    publication_date: str | None
    knowledge_date: str
    effective_period_id: str
    fiscal_period_id: str | None
    period_type: str
    dimension_set_id: str
    assertion_mode: str
    evidence_occurrence_ids: tuple[str, ...] = ()
    review_state: str = "accepted"
    confidence: str | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["identity_digest"] = identity_digest(self.record_id)
        result["evidence_occurrence_ids"] = sorted(set(self.evidence_occurrence_ids))
        return result


@dataclass(frozen=True)
class Observation:
    header: ObservationHeader
    payload: ObservationPayload

    def __post_init__(self) -> None:
        payload = dict(self.payload)
        if payload.get("kind") != self.header.record_type:
            raise DomainValidationError("Observation header and payload kinds must match.")
        object.__setattr__(self, "payload", MappingProxyType(payload))

    def to_dict(self) -> dict[str, Any]:
        return {"header": self.header.to_dict(), "payload": dict(self.payload)}


@dataclass(frozen=True)
class Entity:
    entity_id: str
    entity_type: str
    company_id: str
    payload: EntityPayload
    evidence_occurrence_ids: tuple[str, ...] = ()
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        payload = dict(self.payload)
        if payload.get("kind") != self.entity_type:
            raise DomainValidationError("Entity header and payload kinds must match.")
        object.__setattr__(self, "payload", MappingProxyType(payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "header": {
                "entity_id": self.entity_id,
                "identity_digest": identity_digest(self.entity_id),
                "entity_type": self.entity_type,
                "schema_version": self.schema_version,
                "company_id": self.company_id,
                "evidence_occurrence_ids": sorted(set(self.evidence_occurrence_ids)),
            },
            "payload": dict(self.payload),
        }


@dataclass(frozen=True)
class ReviewIssue:
    issue_id: str
    severity: str
    rule_id: str
    entity_ids: tuple[str, ...]
    business_key: str
    message: str
    evidence_occurrence_ids: tuple[str, ...]
    candidate_record_ids: tuple[str, ...]
    suggested_action: str
    promotion_blocking: bool
    review_state: str = "needs_review"

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        for key in ("entity_ids", "evidence_occurrence_ids", "candidate_record_ids"):
            result[key] = sorted(set(result[key]))
        return result


def value_spec_to_dict(value: ValueSpec | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return value.to_dict()


def observation_from_dict(value: Mapping[str, Any]) -> Observation:
    raw_header = dict(value["header"])
    raw_header.pop("identity_digest", None)
    raw_header["evidence_occurrence_ids"] = tuple(raw_header.get("evidence_occurrence_ids", ()))
    header = ObservationHeader(**raw_header)
    return Observation(header=header, payload=cast(ObservationPayload, dict(value["payload"])))
