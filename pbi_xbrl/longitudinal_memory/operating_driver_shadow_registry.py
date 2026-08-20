"""Canonical, workbook-independent Operating Drivers shadow registry.

The registry consumes already-extracted evidence.  It does not acquire source
documents, select financial-statement owners, write workbooks, or forecast.
Every source record receives exactly one deterministic disposition and only
unambiguous, source-backed numeric records become canonical observations.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from .calendar_rules import (
    CALENDAR_YEAR_RULE_ID,
    SOURCE_LABELLED_52_53_WEEK_RULE_ID,
)
from .identity import canonical_company_id, validate_semantic_id
from .operating_driver_foundation import (
    AggregationSemantics,
    DefinitionContinuity,
    DefinitionContinuityState,
    DriverDimension,
    DriverIdentity,
    EvidenceAvailability,
    EvidenceClassification,
    EvidenceSourceReference,
    EvidenceSourceType,
    EvidenceTransformation,
    EvidenceValueKind,
    FiscalCalendarIdentity,
    FiscalQuarterPeriod,
    OperatingDriverEvidence,
    OperatingDriverFoundationError,
    PeriodKind,
    calendar_year_quarter_period,
)
from .serialization import serialize_package
from .types import canonical_decimal


OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION = (
    "operating-drivers-canonical-shadow-registry@1"
)


class OperatingDriverShadowRegistryError(OperatingDriverFoundationError):
    """Raised when shadow registry construction would be ambiguous."""


class DriverScope(str, Enum):
    GENERIC = "GENERIC"
    SECTOR_SPECIFIC = "SECTOR_SPECIFIC"
    TICKER_SPECIFIC = "TICKER_SPECIFIC"


class DriverAvailabilityState(str, Enum):
    AVAILABLE_COMPARABLE = "AVAILABLE_COMPARABLE"
    AVAILABLE_DEFINITION_CHANGED = "AVAILABLE_DEFINITION_CHANGED"
    AVAILABLE_NOT_COMPARABLE = "AVAILABLE_NOT_COMPARABLE"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_RELEVANT = "NOT_RELEVANT"
    NEEDS_REVIEW = "NEEDS_REVIEW"

    # Compatibility aliases for callers that use the accepted foundation's
    # coarser availability vocabulary.  Serialization always uses the exact
    # shadow-registry values above.
    AVAILABLE = AVAILABLE_COMPARABLE
    NOT_APPLICABLE = NOT_RELEVANT


class EvidenceDisposition(str, Enum):
    CANONICAL_OBSERVATION = "CANONICAL_OBSERVATION"
    DUPLICATE_EVIDENCE = "DUPLICATE_EVIDENCE"
    QUALITATIVE_SUPPORT = "QUALITATIVE_SUPPORT"
    GUIDANCE_REFERENCE = "GUIDANCE_REFERENCE"
    OWNER_ELSEWHERE = "OWNER_ELSEWHERE"
    DEFINITION_INCOMPATIBLE = "DEFINITION_INCOMPATIBLE"
    PERIOD_INCOMPATIBLE = "PERIOD_INCOMPATIBLE"
    UNIT_UNRESOLVED = "UNIT_UNRESOLVED"
    DIMENSION_UNRESOLVED = "DIMENSION_UNRESOLVED"
    IDENTITY_UNRESOLVED = "IDENTITY_UNRESOLVED"
    LOW_VALUE_SUPPORT = "LOW_VALUE_SUPPORT"
    NEEDS_REVIEW = "NEEDS_REVIEW"

    MAPPED_OBSERVATION = CANONICAL_OBSERVATION
    MAPPED_QUALITATIVE_SUPPORT = QUALITATIVE_SUPPORT
    DUPLICATE_CORROBORATION = DUPLICATE_EVIDENCE
    UNIT_INCOMPATIBLE = UNIT_UNRESOLVED
    DIMENSION_INCOMPATIBLE = DIMENSION_UNRESOLVED
    UNSUPPORTED = NEEDS_REVIEW


class VisibilityTier(str, Enum):
    CORE_DRIVER = "CORE_DRIVER"
    SECONDARY_DRIVER = "SECONDARY_DRIVER"
    WATCH_DRIVER = "WATCH_DRIVER"
    SUPPORT_ONLY = "SUPPORT_ONLY"
    RETIRED = "RETIRED"

    PRIMARY = CORE_DRIVER
    SECONDARY = SECONDARY_DRIVER
    AUDIT_ONLY = SUPPORT_ONLY


class FinancialLinkageKind(str, Enum):
    SOURCE_DEFINED_CAUSAL_RELATION = "SOURCE_DEFINED_CAUSAL_RELATION"
    ACCOUNTING_IDENTITY = "ACCOUNTING_IDENTITY"
    ECONOMICALLY_JUSTIFIED_MODEL = "ECONOMICALLY_JUSTIFIED_MODEL"
    EMPIRICAL_ASSOCIATION = "EMPIRICAL_ASSOCIATION"
    SPECULATIVE_ASSOCIATION = "SPECULATIVE_ASSOCIATION"
    NONE = "NONE"

    OPERATING_LEADING_INDICATOR = EMPIRICAL_ASSOCIATION
    OPERATING_VOLUME = ECONOMICALLY_JUSTIFIED_MODEL
    OPERATING_PRICE_MIX = ECONOMICALLY_JUSTIFIED_MODEL
    CAPACITY_OR_FOOTPRINT = ECONOMICALLY_JUSTIFIED_MODEL
    MARGIN_CONTEXT = EMPIRICAL_ASSOCIATION
    FINANCIAL_OWNER_ELSEWHERE = ACCOUNTING_IDENTITY
    QUALITATIVE_CONTEXT = SPECULATIVE_ASSOCIATION


class ForecastEvidenceCapability(str, Enum):
    DIRECT_FORECAST_INPUT = "DIRECT_FORECAST_INPUT"
    LEADING_INDICATOR = "LEADING_INDICATOR"
    FORECAST_CONTEXT = "FORECAST_CONTEXT"
    SENSITIVITY_INPUT = "SENSITIVITY_INPUT"
    HISTORICAL_ONLY = "HISTORICAL_ONLY"
    NOT_FORECASTABLE = "NOT_FORECASTABLE"

    ACTUAL_ONLY = HISTORICAL_ONLY
    MAY_INFORM_FORECAST = FORECAST_CONTEXT
    GUIDANCE_ONLY = DIRECT_FORECAST_INPUT
    NOT_FORECAST_EVIDENCE = NOT_FORECASTABLE


class ValueQualifier(str, Enum):
    EXACT = "EXACT"
    APPROXIMATE = "APPROXIMATE"
    REPORTED_ROUNDED = "REPORTED_ROUNDED"
    QUALITATIVE = "QUALITATIVE"


class MappingAction(str, Enum):
    CANONICAL_DRIVER = "CANONICAL_DRIVER"
    OWNER_ELSEWHERE = "OWNER_ELSEWHERE"
    GUIDANCE_REFERENCE = "GUIDANCE_REFERENCE"
    LOW_VALUE_SUPPORT = "LOW_VALUE_SUPPORT"
    UNSUPPORTED = "UNSUPPORTED"


class CalendarMode(str, Enum):
    CALENDAR_QUARTER = "CALENDAR_QUARTER"
    SOURCE_LABELLED_52_53_WEEK = "SOURCE_LABELLED_52_53_WEEK"


@dataclass(frozen=True, slots=True)
class CanonicalDriverDefinition:
    driver_id: str
    driver_family: str
    canonical_label: str
    display_label: str
    definition_id: str
    definition_version: int
    definition_text: str
    unit_id: str
    scale: str
    sign_convention: str
    aggregation_semantics: AggregationSemantics
    scope: DriverScope
    visibility_tier: VisibilityTier
    financial_linkage: FinancialLinkageKind
    forecast_capability: ForecastEvidenceCapability
    source_owner: str = "owner:operating-drivers:source-native@1"
    period_kind: PeriodKind = PeriodKind.FISCAL_QUARTER
    period_behavior: str = "fiscal-quarter-observation"
    sector_scope: str | None = None
    ticker_profile_scope: tuple[str, ...] = ()
    continuity_policy: str = "typed-foundation-fail-closed"

    def __post_init__(self) -> None:
        object.__setattr__(self, "driver_id", validate_semantic_id(self.driver_id, prefix="driver"))
        object.__setattr__(self, "definition_id", validate_semantic_id(self.definition_id, prefix="definition"))
        object.__setattr__(self, "unit_id", validate_semantic_id(self.unit_id, prefix="unit"))
        object.__setattr__(self, "source_owner", validate_semantic_id(self.source_owner, prefix="owner"))
        object.__setattr__(self, "scale", canonical_decimal(self.scale))
        if self.definition_version < 1:
            raise OperatingDriverShadowRegistryError("Definition versions must be positive.")
        for value, name in (
            (self.driver_family, "driver_family"),
            (self.canonical_label, "canonical_label"),
            (self.display_label, "display_label"),
            (self.definition_text, "definition_text"),
            (self.sign_convention, "sign_convention"),
            (self.period_behavior, "period_behavior"),
            (self.continuity_policy, "continuity_policy"),
        ):
            if not value or value != value.strip():
                raise OperatingDriverShadowRegistryError(f"{name} must be non-empty and trimmed.")

    @property
    def definition_key(self) -> tuple[str, int]:
        return self.definition_id, self.definition_version

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        for key in (
            "aggregation_semantics",
            "scope",
            "visibility_tier",
            "financial_linkage",
            "forecast_capability",
            "period_kind",
        ):
            result[key] = getattr(self, key).value
        result["visibility_candidate"] = self.visibility_tier.value
        result["ticker_profile_scope"] = list(self.ticker_profile_scope)
        return result


@dataclass(frozen=True, slots=True)
class DriverMappingRule:
    rule_id: str
    raw_label: str
    action: MappingAction
    canonical_driver_id: str | None = None
    definition_version: int | None = None
    dimensions: tuple[DriverDimension, ...] = ()
    required_commentary_tokens: tuple[str, ...] = ()
    forbidden_commentary_tokens: tuple[str, ...] = ()
    effective_from_serial: int | None = None
    effective_through_serial: int | None = None
    priority: int = 0
    reason: str = "Declarative ticker-profile mapping."
    owner_id: str | None = None
    transition_state: DefinitionContinuityState | None = None
    transition_from_definition_version: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", validate_semantic_id(self.rule_id, prefix="rule"))
        if not self.raw_label or self.raw_label != self.raw_label.strip():
            raise OperatingDriverShadowRegistryError("Mapping raw_label must be non-empty and trimmed.")
        if self.action is MappingAction.CANONICAL_DRIVER:
            if self.canonical_driver_id is None or self.definition_version is None:
                raise OperatingDriverShadowRegistryError(
                    "Canonical driver mappings require driver and definition-version identities."
                )
            object.__setattr__(
                self,
                "canonical_driver_id",
                validate_semantic_id(self.canonical_driver_id, prefix="driver"),
            )
            if not self.dimensions:
                raise OperatingDriverShadowRegistryError(
                    "Canonical driver mappings require explicit dimensions."
                )
        elif self.canonical_driver_id is not None or self.definition_version is not None or self.dimensions:
            raise OperatingDriverShadowRegistryError(
                "Non-canonical mapping actions cannot claim driver identity."
            )
        if self.owner_id is not None:
            object.__setattr__(self, "owner_id", validate_semantic_id(self.owner_id, prefix="owner"))
        if self.transition_state is not None:
            if self.action is not MappingAction.CANONICAL_DRIVER:
                raise OperatingDriverShadowRegistryError(
                    "Definition transitions belong only to canonical driver mappings."
                )
            if self.effective_from_serial is None or self.transition_from_definition_version is None:
                raise OperatingDriverShadowRegistryError(
                    "Definition transitions require an effective start and prior definition version."
                )
            if self.transition_state not in {
                DefinitionContinuityState.DEFINITION_CHANGED_BREAK_SERIES,
                DefinitionContinuityState.RESTATED_SAME_SERIES,
                DefinitionContinuityState.SUCCESSOR_METRIC,
            }:
                raise OperatingDriverShadowRegistryError(
                    "Mapping transition state must express an explicit definition relation."
                )
        elif self.transition_from_definition_version is not None:
            raise OperatingDriverShadowRegistryError(
                "Prior definition version requires an explicit transition state."
            )
        if self.effective_from_serial is not None and self.effective_through_serial is not None:
            if self.effective_from_serial > self.effective_through_serial:
                raise OperatingDriverShadowRegistryError("Mapping effective range is reversed.")
        object.__setattr__(
            self,
            "required_commentary_tokens",
            tuple(sorted({token.casefold().strip() for token in self.required_commentary_tokens if token.strip()})),
        )
        object.__setattr__(
            self,
            "forbidden_commentary_tokens",
            tuple(sorted({token.casefold().strip() for token in self.forbidden_commentary_tokens if token.strip()})),
        )
        if not self.reason or self.reason != self.reason.strip():
            raise OperatingDriverShadowRegistryError("Mapping reason must be non-empty and trimmed.")

    def matches(self, row: "RawDriverEvidenceRecord") -> bool:
        if row.raw_label != self.raw_label:
            return False
        if self.effective_from_serial is not None and row.period_serial < self.effective_from_serial:
            return False
        if self.effective_through_serial is not None and row.period_serial > self.effective_through_serial:
            return False
        text = row.commentary.casefold()
        if any(token not in text for token in self.required_commentary_tokens):
            return False
        if any(token in text for token in self.forbidden_commentary_tokens):
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action.value,
            "canonical_driver_id": self.canonical_driver_id,
            "definition_version": self.definition_version,
            "dimensions": [item.to_dict() for item in self.dimensions],
            "effective_from_serial": self.effective_from_serial,
            "effective_through_serial": self.effective_through_serial,
            "forbidden_commentary_tokens": list(self.forbidden_commentary_tokens),
            "owner_id": self.owner_id,
            "priority": self.priority,
            "raw_label": self.raw_label,
            "reason": self.reason,
            "required_commentary_tokens": list(self.required_commentary_tokens),
            "rule_id": self.rule_id,
            "transition_from_definition_version": self.transition_from_definition_version,
            "transition_state": self.transition_state.value if self.transition_state is not None else None,
        }


@dataclass(frozen=True, slots=True)
class TickerShadowProfile:
    ticker: str
    calendar_mode: CalendarMode
    calendar_id: str
    mapping_rules: tuple[DriverMappingRule, ...]
    definitions: tuple[CanonicalDriverDefinition, ...]
    source_priority: tuple[str, ...]
    fiscal_anchor_year: int | None = None
    fiscal_anchor_quarter: int | None = None
    fiscal_anchor_serial: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", canonical_company_id(self.ticker))
        object.__setattr__(self, "calendar_id", validate_semantic_id(self.calendar_id, prefix="calendar"))
        definition_keys = [item.definition_key for item in self.definitions]
        if len(definition_keys) != len(set(definition_keys)):
            raise OperatingDriverShadowRegistryError("Duplicate canonical definition identity/version.")
        driver_versions = {(item.driver_id, item.definition_version) for item in self.definitions}
        if len(driver_versions) != len(self.definitions):
            raise OperatingDriverShadowRegistryError("Duplicate driver/version entry in profile.")
        rule_ids = [item.rule_id for item in self.mapping_rules]
        if len(rule_ids) != len(set(rule_ids)):
            raise OperatingDriverShadowRegistryError("Duplicate mapping rule identity.")
        if self.calendar_mode is CalendarMode.SOURCE_LABELLED_52_53_WEEK:
            required = (self.fiscal_anchor_year, self.fiscal_anchor_quarter, self.fiscal_anchor_serial)
            if any(value is None for value in required):
                raise OperatingDriverShadowRegistryError(
                    "Source-labelled fiscal profiles require an explicit accepted anchor."
                )
            if self.fiscal_anchor_quarter not in {1, 2, 3, 4}:
                raise OperatingDriverShadowRegistryError("Fiscal anchor quarter must be Q1-Q4.")

    def definition(self, driver_id: str, version: int) -> CanonicalDriverDefinition:
        matches = [
            item
            for item in self.definitions
            if item.driver_id == driver_id and item.definition_version == version
        ]
        if len(matches) != 1:
            raise OperatingDriverShadowRegistryError(
                f"Profile {self.ticker} has no unique definition for {driver_id} v{version}."
            )
        return matches[0]

    def to_dict(self) -> dict[str, Any]:
        return {
            "calendar_id": self.calendar_id,
            "calendar_mode": self.calendar_mode.value,
            "definitions": [item.to_dict() for item in self.definitions],
            "fiscal_anchor_quarter": self.fiscal_anchor_quarter,
            "fiscal_anchor_serial": self.fiscal_anchor_serial,
            "fiscal_anchor_year": self.fiscal_anchor_year,
            "mapping_rules": [item.to_dict() for item in self.mapping_rules],
            "source_priority": list(self.source_priority),
            "ticker": self.ticker,
        }


def _canonical_raw_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise OperatingDriverShadowRegistryError("Boolean driver values are unsupported.")
    if isinstance(value, (int, float, Decimal)):
        return canonical_decimal(Decimal(str(value)))
    return canonical_decimal(str(value))


def _raw_record_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "commentary": str(row.get("Commentary") or "").strip(),
        "driver_group": str(row.get("Driver group") or "").strip(),
        "qoq_change": None if row.get("QoQ change") is None else str(row.get("QoQ change")).strip(),
        "quality": str(row.get("Quality") or "").strip(),
        "raw_label": str(row.get("Driver") or "").strip(),
        "raw_value": _canonical_raw_value(row.get("Value")),
        "source_label": str(row.get("Source") or "").strip(),
        "source_unit": None if row.get("Unit") is None else str(row.get("Unit")).strip(),
        "period_serial": int(row["Quarter"]),
        "yoy_change": None if row.get("YoY change") is None else str(row.get("YoY change")).strip(),
    }


@dataclass(frozen=True, slots=True)
class RawDriverEvidenceRecord:
    raw_record_id: str
    ticker: str
    occurrence: int
    period_serial: int
    driver_group: str
    raw_label: str
    raw_value: str | None
    source_unit: str | None
    qoq_change: str | None
    yoy_change: str | None
    source_label: str
    commentary: str
    quality: str
    raw_digest_sha256: str

    @property
    def value_kind(self) -> EvidenceValueKind:
        return EvidenceValueKind.NUMERIC if self.raw_value is not None else EvidenceValueKind.QUALITATIVE

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["value_kind"] = self.value_kind.value
        return result


def normalize_raw_evidence(
    ticker: str, rows: Sequence[Mapping[str, Any]]
) -> tuple[RawDriverEvidenceRecord, ...]:
    """Normalize raw rows without making their source order an economic key."""

    ticker = canonical_company_id(ticker)
    payloads = [_raw_record_payload(row) for row in rows]
    canonical_payloads = [
        json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for item in payloads
    ]
    occurrences: Counter[str] = Counter()
    normalized: list[RawDriverEvidenceRecord] = []
    for canonical in sorted(canonical_payloads):
        occurrences[canonical] += 1
        occurrence = occurrences[canonical]
        payload = json.loads(canonical)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        normalized.append(
            RawDriverEvidenceRecord(
                raw_record_id=f"raw:{ticker.lower()}:{digest[:24]}:{occurrence}",
                ticker=ticker,
                occurrence=occurrence,
                raw_digest_sha256=digest,
                **payload,
            )
        )
    return tuple(sorted(normalized, key=lambda item: item.raw_record_id))


def excel_serial_date(serial: int) -> date:
    return date(1899, 12, 30) + timedelta(days=int(serial))


def _calendar_for(profile: TickerShadowProfile) -> FiscalCalendarIdentity:
    rule_id = (
        CALENDAR_YEAR_RULE_ID
        if profile.calendar_mode is CalendarMode.CALENDAR_QUARTER
        else SOURCE_LABELLED_52_53_WEEK_RULE_ID
    )
    return FiscalCalendarIdentity(
        calendar_id=profile.calendar_id,
        company_id=profile.ticker,
        calendar_rule_id=rule_id,
        week_pattern=("calendar-year" if profile.calendar_mode is CalendarMode.CALENDAR_QUARTER else "source-labelled-52-53-week"),
    )


def resolve_profile_periods(
    profile: TickerShadowProfile,
    records: Sequence[RawDriverEvidenceRecord],
) -> dict[int, FiscalQuarterPeriod]:
    """Resolve source periods from explicit profile calendar semantics."""

    calendar = _calendar_for(profile)
    serials = sorted({item.period_serial for item in records})
    if profile.calendar_mode is CalendarMode.CALENDAR_QUARTER:
        periods: dict[int, FiscalQuarterPeriod] = {}
        quarter_by_month_day = {(3, 31): 1, (6, 30): 2, (9, 30): 3, (12, 31): 4}
        for serial in serials:
            end = excel_serial_date(serial)
            quarter = quarter_by_month_day.get((end.month, end.day))
            if quarter is None:
                raise OperatingDriverShadowRegistryError(
                    f"{profile.ticker} source serial {serial} is not an exact calendar quarter end."
                )
            periods[serial] = calendar_year_quarter_period(
                company_id=profile.ticker,
                calendar=calendar,
                fiscal_year=end.year,
                fiscal_quarter=quarter,
                period_id=f"period:{profile.ticker.lower()}:{end.year}-q{quarter}@1",
            )
        return periods

    assert profile.fiscal_anchor_serial is not None
    assert profile.fiscal_anchor_year is not None
    assert profile.fiscal_anchor_quarter is not None
    if profile.fiscal_anchor_serial not in serials:
        raise OperatingDriverShadowRegistryError("Accepted fiscal anchor is absent from source evidence.")
    anchor_index = serials.index(profile.fiscal_anchor_serial)
    anchor_ordinal = profile.fiscal_anchor_year * 4 + profile.fiscal_anchor_quarter - 1
    tuples: dict[int, tuple[int, int, int, date, date, int]] = {}
    for index, serial in enumerate(serials):
        ordinal = anchor_ordinal + index - anchor_index
        fiscal_year, zero_based = divmod(ordinal, 4)
        quarter = zero_based + 1
        end = excel_serial_date(serial)
        if index == 0:
            next_end = excel_serial_date(serials[index + 1])
            inferred_days = (next_end - end).days
            if inferred_days not in {91, 98}:
                raise OperatingDriverShadowRegistryError("Cannot source-reconcile first 52/53-week quarter.")
            start = end - timedelta(days=inferred_days - 1)
        else:
            start = excel_serial_date(serials[index - 1]) + timedelta(days=1)
        days = (end - start).days + 1
        if days not in {91, 98}:
            raise OperatingDriverShadowRegistryError(
                f"Source-labelled period {serial} has {days} days, not 13/14 weeks."
            )
        tuples[serial] = (fiscal_year, quarter, ordinal, start, end, days // 7)
    fifty_three_years = {
        fiscal_year
        for fiscal_year in {item[0] for item in tuples.values()}
        if sum(item[5] for item in tuples.values() if item[0] == fiscal_year) == 53
    }
    return {
        serial: FiscalQuarterPeriod(
            period_id=f"period:{profile.ticker.lower()}:{fiscal_year}-q{quarter}@1",
            company_id=profile.ticker,
            calendar=calendar,
            fiscal_year=fiscal_year,
            fiscal_quarter=quarter,
            fiscal_ordinal=ordinal,
            start_date=start,
            end_date=end,
            week_count=weeks,
            is_53_week_year=fiscal_year in fifty_three_years,
        )
        for serial, (fiscal_year, quarter, ordinal, start, end, weeks) in tuples.items()
    }


def _source_type(label: str) -> EvidenceSourceType:
    return {
        "10-k": EvidenceSourceType.SEC_FILING,
        "10-q": EvidenceSourceType.SEC_FILING,
        "earnings_release": EvidenceSourceType.EARNINGS_RELEASE,
        "presentation": EvidenceSourceType.PRESENTATION,
        "transcript": EvidenceSourceType.TRANSCRIPT,
        "internal_metric": EvidenceSourceType.INTERNAL_METRIC,
    }.get(label.casefold(), EvidenceSourceType.OTHER)


def _unit_id(raw_unit: str | None) -> str:
    units = {
        "%": "unit:core:percent@1",
        "$/share": "unit:core:usd-per-share@1",
        "$m": "unit:core:usd-million@1",
        "bps": "unit:core:basis-points@1",
        "m shares": "unit:core:million-shares@1",
        "pts": "unit:core:percentage-points@1",
        "stores": "unit:operating-driver:stores@1",
        "k tons": "unit:operating-driver:thousand-tons@1",
        "m bushels": "unit:operating-driver:million-bushels@1",
        "m gallons": "unit:operating-driver:million-gallons@1",
        "m lbs": "unit:operating-driver:million-pounds@1",
    }
    if raw_unit is None:
        return "unit:core:qualitative@1"
    return units.get(raw_unit, "unit:core:unsupported@1")


def _select_rule(
    profile: TickerShadowProfile, row: RawDriverEvidenceRecord
) -> tuple[DriverMappingRule | None, bool]:
    matches = [item for item in profile.mapping_rules if item.matches(row)]
    if not matches:
        return None, False
    highest = max(item.priority for item in matches)
    finalists = [item for item in matches if item.priority == highest]
    outcomes = {
        (
            item.action,
            item.canonical_driver_id,
            item.definition_version,
            item.dimensions,
            item.owner_id,
            item.transition_state,
            item.transition_from_definition_version,
        )
        for item in finalists
    }
    if len(outcomes) != 1:
        return None, True
    return sorted(finalists, key=lambda item: item.rule_id)[0], False


def _driver_identity(
    profile: TickerShadowProfile,
    definition: CanonicalDriverDefinition,
    dimensions: tuple[DriverDimension, ...],
) -> DriverIdentity:
    return DriverIdentity(
        driver_id=definition.driver_id,
        company_id=profile.ticker,
        ticker=profile.ticker,
        driver_family=definition.driver_family,
        canonical_label=definition.canonical_label,
        display_label=definition.display_label,
        unit_id=definition.unit_id,
        scale=definition.scale,
        sign_convention=definition.sign_convention,
        dimensions=dimensions,
        period_kind=definition.period_kind,
        source_owner=definition.source_owner,
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        aggregation_semantics=definition.aggregation_semantics,
    )


@dataclass(frozen=True, slots=True)
class EvidenceCensusRecord:
    raw_record: RawDriverEvidenceRecord
    disposition: EvidenceDisposition
    reason: str
    rule_id: str | None
    canonical_driver_id: str | None
    definition_version: int | None
    dimension_set_id: str | None
    period_id: str
    observation_id: str | None = None
    owner_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_driver_id": self.canonical_driver_id,
            "definition_version": self.definition_version,
            "dimension_set_id": self.dimension_set_id,
            "disposition": self.disposition.value,
            "observation_id": self.observation_id,
            "owner_id": self.owner_id,
            "period_id": self.period_id,
            "raw_record": self.raw_record.to_dict(),
            "reason": self.reason,
            "rule_id": self.rule_id,
        }


@dataclass(frozen=True, slots=True)
class CanonicalObservation:
    observation_id: str
    evidence: OperatingDriverEvidence
    value_qualifier: ValueQualifier
    raw_record_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence": self.evidence.to_dict(),
            "observation_id": self.observation_id,
            "raw_record_ids": list(self.raw_record_ids),
            "source_backed_zero": self.evidence.source_backed_zero,
            "value_qualifier": self.value_qualifier.value,
        }


@dataclass(frozen=True, slots=True)
class EvidenceAttachment:
    attachment_id: str
    raw_record_id: str
    canonical_driver_id: str
    definition_version: int
    dimension_set_id: str
    period_id: str
    attachment_kind: str
    source_label: str
    commentary: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class LongitudinalSeriesSegment:
    series_id: str
    ticker: str
    canonical_driver_id: str
    definition_id: str
    definition_version: int
    dimension_set_id: str
    unit_id: str
    observation_ids: tuple[str, ...]
    start_period_id: str
    end_period_id: str
    break_before_reason: str | None
    comparable_quarter_count: int
    qoq_eligible_count: int
    yoy_eligible_count: int
    ttm_eligible_ending_period_count: int
    annual_only_period_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class ShadowRegistryPackage:
    profile: TickerShadowProfile
    raw_record_count: int
    evidence_census: tuple[EvidenceCensusRecord, ...]
    observations: tuple[CanonicalObservation, ...]
    attachments: tuple[EvidenceAttachment, ...]
    series: tuple[LongitudinalSeriesSegment, ...]
    availability: tuple[dict[str, Any], ...]
    source_precedence_contract: dict[str, Any]

    @property
    def mapped_record_count(self) -> int:
        return sum(
            item.disposition
            in {
                EvidenceDisposition.MAPPED_OBSERVATION,
                EvidenceDisposition.MAPPED_QUALITATIVE_SUPPORT,
                EvidenceDisposition.DUPLICATE_CORROBORATION,
            }
            for item in self.evidence_census
        )

    @property
    def disposition_counts(self) -> dict[str, int]:
        counts = Counter(item.disposition.value for item in self.evidence_census)
        return {item.value: counts.get(item.value, 0) for item in EvidenceDisposition}

    @property
    def reconciliation(self) -> dict[str, Any]:
        classified = len(self.evidence_census)
        return {
            "classified_record_count": classified,
            "duplicate_raw_record_id_count": classified
            - len({item.raw_record.raw_record_id for item in self.evidence_census}),
            "raw_record_count": self.raw_record_count,
            "reconciles": classified == self.raw_record_count,
            "unclassified_record_count": self.raw_record_count - classified,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "attachments": [item.to_dict() for item in self.attachments],
            "availability": list(self.availability),
            "contract_version": OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION,
            "evidence_census": [item.to_dict() for item in self.evidence_census],
            "observations": [item.to_dict() for item in self.observations],
            "profile": self.profile.to_dict(),
            "raw_record_count": self.raw_record_count,
            "reconciliation": self.reconciliation,
            "series": [item.to_dict() for item in self.series],
            "source_precedence_contract": self.source_precedence_contract,
            "utilization": {
                "disposition_counts": self.disposition_counts,
                "mapped_record_count": self.mapped_record_count,
                "mapped_record_ratio": (
                    canonical_decimal(Decimal(self.mapped_record_count) / Decimal(self.raw_record_count))
                    if self.raw_record_count
                    else "0"
                ),
                "numeric_observation_count": len(self.observations),
                "qualitative_attachment_count": sum(
                    item.attachment_kind == "QUALITATIVE_SUPPORT" for item in self.attachments
                ),
            },
        }

    def serialize(self) -> bytes:
        return serialize_package(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.serialize()).hexdigest()


def _direct_disposition(action: MappingAction) -> EvidenceDisposition:
    return {
        MappingAction.OWNER_ELSEWHERE: EvidenceDisposition.OWNER_ELSEWHERE,
        MappingAction.GUIDANCE_REFERENCE: EvidenceDisposition.GUIDANCE_REFERENCE,
        MappingAction.LOW_VALUE_SUPPORT: EvidenceDisposition.LOW_VALUE_SUPPORT,
        MappingAction.UNSUPPORTED: EvidenceDisposition.UNSUPPORTED,
    }[action]


def _business_key(
    driver: DriverIdentity, period: FiscalQuarterPeriod
) -> tuple[str, int, str, str, str]:
    return (
        driver.driver_id,
        driver.definition_version,
        driver.dimension_set_id,
        period.period_id,
        driver.unit_id,
    )


def _observation_id(driver: DriverIdentity, period: FiscalQuarterPeriod) -> str:
    payload = "|".join(
        (
            driver.company_id,
            driver.driver_id,
            driver.definition_id,
            str(driver.definition_version),
            driver.dimension_set_id,
            period.period_id,
            driver.unit_id,
        )
    )
    return f"observation:{driver.company_id.lower()}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]}"


def _source_rank(profile: TickerShadowProfile, source_label: str) -> tuple[int, str]:
    normalized = source_label.casefold()
    priority = [item.casefold() for item in profile.source_priority]
    try:
        return priority.index(normalized), normalized
    except ValueError:
        return len(priority), normalized


def _value_qualifier(row: RawDriverEvidenceRecord) -> ValueQualifier:
    quality = row.quality.casefold()
    commentary = row.commentary.casefold()
    if "approx" in quality or "approximately" in commentary or "~" in commentary:
        return ValueQualifier.APPROXIMATE
    if quality in {"modeled", "text-derived"}:
        return ValueQualifier.REPORTED_ROUNDED
    return ValueQualifier.EXACT


def _availability_rows(
    profile: TickerShadowProfile,
    census: Sequence[EvidenceCensusRecord],
    observations: Sequence[CanonicalObservation],
) -> tuple[dict[str, Any], ...]:
    observed = Counter(item.evidence.driver.driver_id for item in observations)
    qualitative = Counter(
        item.canonical_driver_id
        for item in census
        if item.canonical_driver_id is not None
        and item.disposition is EvidenceDisposition.MAPPED_QUALITATIVE_SUPPORT
    )
    conflicts = Counter(
        item.canonical_driver_id
        for item in census
        if item.canonical_driver_id is not None
        and item.disposition
        in {
            EvidenceDisposition.PERIOD_INCOMPATIBLE,
            EvidenceDisposition.DEFINITION_INCOMPATIBLE,
            EvidenceDisposition.DIMENSION_INCOMPATIBLE,
            EvidenceDisposition.UNIT_INCOMPATIBLE,
            EvidenceDisposition.IDENTITY_UNRESOLVED,
        }
    )
    rows: list[dict[str, Any]] = []
    by_driver: dict[str, list[CanonicalDriverDefinition]] = defaultdict(list)
    for definition in profile.definitions:
        by_driver[definition.driver_id].append(definition)
    for driver_id, definitions in sorted(by_driver.items()):
        definition_versions = sorted({item.definition_version for item in definitions})
        if observed[driver_id] and len(definition_versions) > 1:
            state = DriverAvailabilityState.AVAILABLE_DEFINITION_CHANGED
        elif observed[driver_id] and not conflicts[driver_id]:
            state = DriverAvailabilityState.AVAILABLE_COMPARABLE
        elif observed[driver_id] or qualitative[driver_id]:
            state = DriverAvailabilityState.AVAILABLE_NOT_COMPARABLE
        elif conflicts[driver_id]:
            state = DriverAvailabilityState.NEEDS_REVIEW
        else:
            state = DriverAvailabilityState.UNAVAILABLE
        rows.append(
            {
                "canonical_driver_id": driver_id,
                "definition_versions": definition_versions,
                "numeric_observation_count": observed[driver_id],
                "qualitative_support_count": qualitative[driver_id],
                "review_conflict_count": conflicts[driver_id],
                "state": state.value,
            }
        )
    return tuple(rows)


def _build_series(
    observations: Sequence[CanonicalObservation],
) -> tuple[LongitudinalSeriesSegment, ...]:
    groups: dict[tuple[str, str, int, str, str], list[CanonicalObservation]] = defaultdict(list)
    for item in observations:
        driver = item.evidence.driver
        groups[
            (
                driver.driver_id,
                driver.definition_id,
                driver.definition_version,
                driver.dimension_set_id,
                driver.unit_id,
            )
        ].append(item)
    result: list[LongitudinalSeriesSegment] = []
    for key, items in sorted(groups.items()):
        ordered = sorted(
            items,
            key=lambda item: (
                item.evidence.period.fiscal_ordinal,
                item.observation_id,
            ),
        )
        segments: list[list[CanonicalObservation]] = []
        for item in ordered:
            if not segments:
                segments.append([item])
                continue
            earlier = segments[-1][-1].evidence.period
            later = item.evidence.period
            if later.fiscal_ordinal == earlier.fiscal_ordinal + 1:
                segments[-1].append(item)
            else:
                segments.append([item])
        for number, segment in enumerate(segments, start=1):
            first = segment[0]
            last = segment[-1]
            driver = first.evidence.driver
            payload = "|".join(
                tuple(str(value) for value in key)
                + (
                    str(number),
                    first.evidence.period.period_id,
                    last.evidence.period.period_id,
                )
            )
            result.append(
                LongitudinalSeriesSegment(
                    series_id=f"series:{driver.company_id.lower()}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]}",
                    ticker=driver.company_id,
                    canonical_driver_id=driver.driver_id,
                    definition_id=driver.definition_id,
                    definition_version=driver.definition_version,
                    dimension_set_id=driver.dimension_set_id,
                    unit_id=driver.unit_id,
                    observation_ids=tuple(item.observation_id for item in segment),
                    start_period_id=first.evidence.period.period_id,
                    end_period_id=last.evidence.period.period_id,
                    break_before_reason=(None if number == 1 else "MISSING_OR_INCOMPATIBLE_PERIOD"),
                    comparable_quarter_count=len(segment),
                    qoq_eligible_count=max(len(segment) - 1, 0),
                    yoy_eligible_count=max(len(segment) - 4, 0),
                    ttm_eligible_ending_period_count=(
                        max(len(segment) - 3, 0)
                        if driver.aggregation_semantics is AggregationSemantics.SUMMABLE
                        else 0
                    ),
                )
            )
    return tuple(sorted(result, key=lambda item: item.series_id))


def build_shadow_registry(
    raw_rows: Sequence[Mapping[str, Any]],
    profile: TickerShadowProfile,
) -> ShadowRegistryPackage:
    """Build one deterministic shadow package with an exhaustive raw census."""

    records = normalize_raw_evidence(profile.ticker, raw_rows)
    periods = resolve_profile_periods(profile, records)
    staged: list[dict[str, Any]] = []
    census: list[EvidenceCensusRecord] = []
    attachments: list[EvidenceAttachment] = []

    for row in records:
        period = periods[row.period_serial]
        rule, ambiguous = _select_rule(profile, row)
        if ambiguous:
            census.append(
                EvidenceCensusRecord(
                    raw_record=row,
                    disposition=EvidenceDisposition.IDENTITY_UNRESOLVED,
                    reason="Multiple equal-priority declarative rules claim materially different identities.",
                    rule_id=None,
                    canonical_driver_id=None,
                    definition_version=None,
                    dimension_set_id=None,
                    period_id=period.period_id,
                )
            )
            continue
        if rule is None:
            census.append(
                EvidenceCensusRecord(
                    raw_record=row,
                    disposition=EvidenceDisposition.UNSUPPORTED,
                    reason="No accepted declarative mapping rule exists for this raw identity.",
                    rule_id=None,
                    canonical_driver_id=None,
                    definition_version=None,
                    dimension_set_id=None,
                    period_id=period.period_id,
                )
            )
            continue
        if rule.action is not MappingAction.CANONICAL_DRIVER:
            census.append(
                EvidenceCensusRecord(
                    raw_record=row,
                    disposition=_direct_disposition(rule.action),
                    reason=rule.reason,
                    rule_id=rule.rule_id,
                    canonical_driver_id=None,
                    definition_version=None,
                    dimension_set_id=None,
                    period_id=period.period_id,
                    owner_id=rule.owner_id,
                )
            )
            continue

        assert rule.canonical_driver_id is not None
        assert rule.definition_version is not None
        definition = profile.definition(rule.canonical_driver_id, rule.definition_version)
        driver = _driver_identity(profile, definition, rule.dimensions)
        if row.raw_value is not None and _unit_id(row.source_unit) != definition.unit_id:
            census.append(
                EvidenceCensusRecord(
                    raw_record=row,
                    disposition=EvidenceDisposition.UNIT_INCOMPATIBLE,
                    reason=(
                        f"Raw unit {row.source_unit!r} does not equal canonical unit "
                        f"{definition.unit_id!r}; no safe conversion contract exists."
                    ),
                    rule_id=rule.rule_id,
                    canonical_driver_id=definition.driver_id,
                    definition_version=definition.definition_version,
                    dimension_set_id=driver.dimension_set_id,
                    period_id=period.period_id,
                )
            )
            continue
        staged.append(
            {
                "definition": definition,
                "driver": driver,
                "period": period,
                "raw": row,
                "rule": rule,
            }
        )

    numeric_groups: dict[tuple[str, int, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    qualitative: list[dict[str, Any]] = []
    for item in staged:
        if item["raw"].raw_value is None:
            qualitative.append(item)
        else:
            numeric_groups[_business_key(item["driver"], item["period"])].append(item)

    for item in qualitative:
        row = item["raw"]
        driver = item["driver"]
        period = item["period"]
        rule = item["rule"]
        attachment_id = f"attachment:{profile.ticker.lower()}:{hashlib.sha256((row.raw_record_id + driver.driver_id).encode('utf-8')).hexdigest()[:32]}"
        attachments.append(
            EvidenceAttachment(
                attachment_id=attachment_id,
                raw_record_id=row.raw_record_id,
                canonical_driver_id=driver.driver_id,
                definition_version=driver.definition_version,
                dimension_set_id=driver.dimension_set_id,
                period_id=period.period_id,
                attachment_kind="QUALITATIVE_SUPPORT",
                source_label=row.source_label,
                commentary=row.commentary,
            )
        )
        census.append(
            EvidenceCensusRecord(
                raw_record=row,
                disposition=EvidenceDisposition.MAPPED_QUALITATIVE_SUPPORT,
                reason="Qualitative evidence is attached as support and never coerced to a numeric observation.",
                rule_id=rule.rule_id,
                canonical_driver_id=driver.driver_id,
                definition_version=driver.definition_version,
                dimension_set_id=driver.dimension_set_id,
                period_id=period.period_id,
            )
        )

    observations: list[CanonicalObservation] = []
    for _key, items in sorted(numeric_groups.items()):
        distinct_values = {item["raw"].raw_value for item in items}
        if len(distinct_values) != 1:
            for item in items:
                row = item["raw"]
                driver = item["driver"]
                period = item["period"]
                census.append(
                    EvidenceCensusRecord(
                        raw_record=row,
                        disposition=EvidenceDisposition.PERIOD_INCOMPATIBLE,
                        reason=(
                            "Conflicting values share one raw label/period identity; the raw surface "
                            "does not distinguish quarter from annual context, so selection fails closed."
                        ),
                        rule_id=item["rule"].rule_id,
                        canonical_driver_id=driver.driver_id,
                        definition_version=driver.definition_version,
                        dimension_set_id=driver.dimension_set_id,
                        period_id=period.period_id,
                    )
                )
            continue
        ordered = sorted(
            items,
            key=lambda item: (
                _source_rank(profile, item["raw"].source_label),
                item["raw"].raw_record_id,
            ),
        )
        primary = ordered[0]
        row = primary["raw"]
        driver = primary["driver"]
        period = primary["period"]
        definition = primary["definition"]
        primary_rule = primary["rule"]
        observation_id = _observation_id(driver, period)
        source = EvidenceSourceReference(
            source_document_id=(
                f"raw-operating-drivers:{profile.ticker}:{row.source_label}:{period.period_id}"
            ),
            source_type=_source_type(row.source_label),
            source_location=f"operating_drivers_raw:{row.raw_digest_sha256}:{row.occurrence}",
            publication_date=None,
            knowledge_date=None,
        )
        is_transition = (
            primary_rule.transition_state is not None
            and row.period_serial == primary_rule.effective_from_serial
        )
        continuity = DefinitionContinuity(
            state=(
                primary_rule.transition_state
                if is_transition
                else DefinitionContinuityState.SAME_SERIES
            ),
            from_definition_id=definition.definition_id,
            from_definition_version=(
                primary_rule.transition_from_definition_version
                if is_transition
                else definition.definition_version
            ),
            to_definition_id=definition.definition_id,
            to_definition_version=definition.definition_version,
            reason=(
                primary_rule.reason
                if is_transition
                else "Raw evidence directly satisfies this accepted definition version."
            ),
        )
        evidence = OperatingDriverEvidence(
            evidence_id=f"evidence:{profile.ticker.lower()}:{row.raw_digest_sha256[:32]}:{row.occurrence}",
            driver=driver,
            period=period,
            source=source,
            value_kind=EvidenceValueKind.NUMERIC,
            raw_value=row.raw_value,
            normalized_value=row.raw_value,
            source_unit_id=definition.unit_id,
            classification=EvidenceClassification.ACTUAL,
            availability=EvidenceAvailability.AVAILABLE,
            unavailable_reason=None,
            continuity=continuity,
            transformations=(
                EvidenceTransformation(
                    method_id="method:operating-drivers:identity-normalization@1",
                    description="Declarative label, dimension, period, definition, and unit normalization without value inference.",
                    input_record_ids=tuple(item["raw"].raw_record_id for item in ordered),
                ),
            ),
        )
        observations.append(
            CanonicalObservation(
                observation_id=observation_id,
                evidence=evidence,
                value_qualifier=_value_qualifier(row),
                raw_record_ids=tuple(item["raw"].raw_record_id for item in ordered),
            )
        )
        for index, item in enumerate(ordered):
            duplicate = index > 0
            raw = item["raw"]
            census.append(
                EvidenceCensusRecord(
                    raw_record=raw,
                    disposition=(
                        EvidenceDisposition.DUPLICATE_CORROBORATION
                        if duplicate
                        else EvidenceDisposition.MAPPED_OBSERVATION
                    ),
                    reason=(
                        "Identical-value evidence corroborates the canonical observation; source precedence never selects between different values."
                        if duplicate
                        else "Unambiguous source-backed numeric evidence became one canonical observation."
                    ),
                    rule_id=item["rule"].rule_id,
                    canonical_driver_id=driver.driver_id,
                    definition_version=driver.definition_version,
                    dimension_set_id=driver.dimension_set_id,
                    period_id=period.period_id,
                    observation_id=observation_id,
                )
            )
            if duplicate:
                attachment_id = f"attachment:{profile.ticker.lower()}:{hashlib.sha256((raw.raw_record_id + observation_id).encode('utf-8')).hexdigest()[:32]}"
                attachments.append(
                    EvidenceAttachment(
                        attachment_id=attachment_id,
                        raw_record_id=raw.raw_record_id,
                        canonical_driver_id=driver.driver_id,
                        definition_version=driver.definition_version,
                        dimension_set_id=driver.dimension_set_id,
                        period_id=period.period_id,
                        attachment_kind="DUPLICATE_CORROBORATION",
                        source_label=raw.source_label,
                        commentary=raw.commentary,
                    )
                )

    census_tuple = tuple(sorted(census, key=lambda item: item.raw_record.raw_record_id))
    if len(census_tuple) != len(records):
        raise OperatingDriverShadowRegistryError("Evidence census does not reconcile to raw input.")
    if len({item.raw_record.raw_record_id for item in census_tuple}) != len(records):
        raise OperatingDriverShadowRegistryError("Evidence census contains duplicate raw identities.")
    observation_tuple = tuple(sorted(observations, key=lambda item: item.observation_id))
    return ShadowRegistryPackage(
        profile=profile,
        raw_record_count=len(records),
        evidence_census=census_tuple,
        observations=observation_tuple,
        attachments=tuple(sorted(attachments, key=lambda item: item.attachment_id)),
        series=_build_series(observation_tuple),
        availability=_availability_rows(profile, census_tuple, observation_tuple),
        source_precedence_contract={
            "conflicting_values": "FAIL_CLOSED_PERIOD_INCOMPATIBLE",
            "identical_values": "PRIMARY_BY_DECLARATIVE_SOURCE_PRIORITY_WITH_ALL_DUPLICATES_ATTACHED",
            "source_order_is_economic_owner": False,
            "source_priority": list(profile.source_priority),
        },
    )


def combined_registry_digest(packages: Iterable[ShadowRegistryPackage]) -> str:
    payload = serialize_package(
        {
            "contract_version": OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION,
            "package_hashes": {
                package.profile.ticker: package.sha256
                for package in sorted(packages, key=lambda item: item.profile.ticker)
            },
        }
    )
    return hashlib.sha256(payload).hexdigest()
