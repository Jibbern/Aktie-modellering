"""Fail-closed longitudinal analytics over canonical Operating Driver series.

The module consumes :class:`ShadowRegistryPackage` objects and never acquires
source data, changes canonical observations, writes workbooks, interprets a
movement as good/bad, or emits a forecast.  All comparison and aggregation
identity comes from the accepted typed Operating Drivers foundation.
"""
from __future__ import annotations

import dataclasses
import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, localcontext
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from .operating_driver_foundation import (
    AggregateReason,
    AggregationMethod,
    AggregationSemantics,
    ComparisonKind,
    ComparisonReason,
    ComparisonState,
    DefinitionContinuityState,
    DurationAggregateRequest,
    EvidenceClassification,
    FiscalQuarterPeriod,
    FiscalYearPeriod,
    TrailingTwelveMonthsPeriod,
    aggregate_duration_fail_closed,
    safe_qoq,
    safe_yoy,
    ttm_quarter_keys,
)
from .operating_driver_shadow_registry import (
    CanonicalObservation,
    FinancialLinkageKind,
    ForecastEvidenceCapability,
    MappingAction,
    ShadowRegistryPackage,
)
from .identity import dimension_set_identity
from .serialization import serialize_package
from .types import canonical_decimal


OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION = (
    "operating-drivers-derived-longitudinal-analytics@1"
)
OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION = (
    "operating-drivers-longitudinal-math@1"
)


class DerivedAnalyticsError(ValueError):
    """Raised when analytics construction would weaken an accepted contract."""


class AnalyticsAvailability(str, Enum):
    AVAILABLE = "AVAILABLE"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    PRIOR_PERIOD_MISSING = "PRIOR_PERIOD_MISSING"
    PERIOD_INCOMPATIBLE = "PERIOD_INCOMPATIBLE"
    DEFINITION_BREAK = "DEFINITION_BREAK"
    DIMENSION_MISMATCH = "DIMENSION_MISMATCH"
    UNIT_INCOMPATIBLE = "UNIT_INCOMPATIBLE"
    AGGREGATION_NOT_ALLOWED = "AGGREGATION_NOT_ALLOWED"
    INCOMPLETE_PERIOD_SET = "INCOMPLETE_PERIOD_SET"
    RELATIVE_CHANGE_UNDEFINED = "RELATIVE_CHANGE_UNDEFINED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NEEDS_REVIEW = "NEEDS_REVIEW"


class AnalysisType(str, Enum):
    LATEST_STATE = "LATEST_STATE"
    QOQ = "QOQ"
    YOY = "YOY"
    TTM = "TTM"
    FISCAL_YEAR = "FISCAL_YEAR"
    TTM_YOY = "TTM_YOY"
    TREND_4Q = "TREND_4Q"
    ACCELERATION = "ACCELERATION"
    INFLECTION = "INFLECTION"
    CONSISTENCY_4Q = "CONSISTENCY_4Q"
    VARIABILITY_12Q = "VARIABILITY_12Q"


class TrendState(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    UNCHANGED = "UNCHANGED"
    MIXED = "MIXED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class AccelerationState(str, Enum):
    POSITIVE_ACCELERATION = "POSITIVE_ACCELERATION"
    NEGATIVE_ACCELERATION = "NEGATIVE_ACCELERATION"
    UNCHANGED_RATE = "UNCHANGED_RATE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class InflectionState(str, Enum):
    DOWN_TO_UP = "DOWN_TO_UP"
    UP_TO_DOWN = "UP_TO_DOWN"
    CONTINUED_UP = "CONTINUED_UP"
    CONTINUED_DOWN = "CONTINUED_DOWN"
    MIXED_OR_FLAT = "MIXED_OR_FLAT"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ConsistencyState(str, Enum):
    CONSISTENT_UP = "CONSISTENT_UP"
    CONSISTENT_DOWN = "CONSISTENT_DOWN"
    MIXED = "MIXED"
    UNCHANGED = "UNCHANGED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class SignTransition(str, Enum):
    NONE = "NONE"
    NEGATIVE_TO_POSITIVE = "NEGATIVE_TO_POSITIVE"
    POSITIVE_TO_NEGATIVE = "POSITIVE_TO_NEGATIVE"
    ZERO_TO_POSITIVE = "ZERO_TO_POSITIVE"
    ZERO_TO_NEGATIVE = "ZERO_TO_NEGATIVE"
    POSITIVE_TO_ZERO = "POSITIVE_TO_ZERO"
    NEGATIVE_TO_ZERO = "NEGATIVE_TO_ZERO"
    ZERO_TO_ZERO = "ZERO_TO_ZERO"


class ForecastEvidenceReadiness(str, Enum):
    FORECAST_EVIDENCE_READY = "FORECAST_EVIDENCE_READY"
    CONTEXT_ONLY = "CONTEXT_ONLY"
    NEEDS_RELATIONSHIP_REVIEW = "NEEDS_RELATIONSHIP_REVIEW"
    NOT_FORECAST_RELEVANT = "NOT_FORECAST_RELEVANT"


class GuidanceComparisonReadiness(str, Enum):
    GUIDANCE_COMPARISON_READY = "GUIDANCE_COMPARISON_READY"
    GUIDANCE_REFERENCE_EXISTS_NOT_NORMALIZED = (
        "GUIDANCE_REFERENCE_EXISTS_NOT_NORMALIZED"
    )
    NO_GUIDANCE_REFERENCE = "NO_GUIDANCE_REFERENCE"
    DEFINITION_MISMATCH = "DEFINITION_MISMATCH"
    PERIOD_MISMATCH = "PERIOD_MISMATCH"


RATE_UNIT_IDS = frozenset(
    {
        "unit:core:percent@1",
        "unit:core:percentage-points@1",
        "unit:core:basis-points@1",
    }
)
BREAK_STATES = frozenset(
    {
        DefinitionContinuityState.DEFINITION_CHANGED_BREAK_SERIES,
        DefinitionContinuityState.SEGMENT_REORG_BREAK_SERIES,
        DefinitionContinuityState.SUCCESSOR_METRIC,
        DefinitionContinuityState.UNRESOLVED,
    }
)


def _decimal(value: str | Decimal) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(value)


def _hash_id(prefix: str, *parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"{prefix}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]}"


def _knowledge_boundary(observations: Iterable[CanonicalObservation]) -> str | None:
    values = sorted(
        {
            item.evidence.source.knowledge_date.isoformat()
            for item in observations
            if item.evidence.source.knowledge_date is not None
        }
    )
    return values[-1] if values else None


def _source_evidence_ids(
    observations: Iterable[CanonicalObservation],
) -> tuple[str, ...]:
    return tuple(sorted({item.evidence.evidence_id for item in observations}))


def _dimensions(observation: CanonicalObservation) -> tuple[dict[str, Any], ...]:
    return tuple(item.to_dict() for item in observation.evidence.driver.dimensions)


def _group_key(observation: CanonicalObservation) -> tuple[str, int, str, str]:
    driver = observation.evidence.driver
    return (
        driver.driver_id,
        driver.definition_version,
        driver.dimension_set_id,
        driver.unit_id,
    )


def _comparison_pool_key(observation: CanonicalObservation) -> tuple[str, str, str]:
    driver = observation.evidence.driver
    return driver.driver_id, driver.dimension_set_id, driver.unit_id


def _ordered(observations: Iterable[CanonicalObservation]) -> tuple[CanonicalObservation, ...]:
    return tuple(
        sorted(
            observations,
            key=lambda item: (
                item.evidence.period.fiscal_ordinal
                if isinstance(item.evidence.period, FiscalQuarterPeriod)
                else -1,
                item.observation_id,
            ),
        )
    )


def _sign_transition(prior: Decimal, current: Decimal) -> SignTransition:
    if prior < 0 < current:
        return SignTransition.NEGATIVE_TO_POSITIVE
    if prior > 0 > current:
        return SignTransition.POSITIVE_TO_NEGATIVE
    if prior == 0 < current:
        return SignTransition.ZERO_TO_POSITIVE
    if prior == 0 > current:
        return SignTransition.ZERO_TO_NEGATIVE
    if prior > 0 and current == 0:
        return SignTransition.POSITIVE_TO_ZERO
    if prior < 0 and current == 0:
        return SignTransition.NEGATIVE_TO_ZERO
    if prior == 0 and current == 0:
        return SignTransition.ZERO_TO_ZERO
    return SignTransition.NONE


def _relative_change(
    *, current: Decimal, prior: Decimal, unit_id: str
) -> tuple[str | None, AnalyticsAvailability, str]:
    if unit_id in RATE_UNIT_IDS:
        return (
            None,
            AnalyticsAvailability.NOT_APPLICABLE,
            "Rate metrics use native percentage-point/basis-point deltas; relative change is not contracted.",
        )
    if prior <= 0 or current < 0:
        return (
            None,
            AnalyticsAvailability.RELATIVE_CHANGE_UNDEFINED,
            "Relative change requires a strictly positive prior and non-negative current value; negative bases and sign crossings fail closed.",
        )
    return (
        canonical_decimal((current - prior) / prior),
        AnalyticsAvailability.AVAILABLE,
        "Relative change uses (current-prior)/prior with a strictly positive prior.",
    )


def _percentage_point_change(
    current: Decimal, prior: Decimal, unit_id: str
) -> str | None:
    return canonical_decimal(current - prior) if unit_id in RATE_UNIT_IDS else None


def _map_comparison_availability(
    reason: ComparisonReason,
    current: CanonicalObservation,
    prior: CanonicalObservation | None,
) -> AnalyticsAvailability:
    if reason is ComparisonReason.COMPLETE:
        return AnalyticsAvailability.AVAILABLE
    if reason in {
        ComparisonReason.PRIOR_PERIOD_MISSING,
        ComparisonReason.PRIOR_YEAR_PERIOD_MISSING,
    }:
        return AnalyticsAvailability.PRIOR_PERIOD_MISSING
    if reason is ComparisonReason.DIMENSION_INCOMPATIBLE:
        return AnalyticsAvailability.DIMENSION_MISMATCH
    if reason is ComparisonReason.UNIT_INCOMPATIBLE:
        return AnalyticsAvailability.UNIT_INCOMPATIBLE
    if reason is ComparisonReason.DEFINITION_INCOMPATIBLE:
        if (
            current.evidence.continuity.state in BREAK_STATES
            or (
                prior is not None
                and current.evidence.driver.definition_version
                != prior.evidence.driver.definition_version
            )
        ):
            return AnalyticsAvailability.DEFINITION_BREAK
        return AnalyticsAvailability.PERIOD_INCOMPATIBLE
    if reason is ComparisonReason.VALUE_UNAVAILABLE:
        return AnalyticsAvailability.NEEDS_REVIEW
    return AnalyticsAvailability.PERIOD_INCOMPATIBLE


@dataclass(frozen=True, slots=True)
class LatestStateAnalytics:
    analysis_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    latest_observation_id: str
    latest_period_id: str
    latest_value: str
    latest_knowledge_date: str | None
    comparable_history_depth: int
    latest_comparable_observation_id: str | None
    latest_comparable_period_id: str | None
    latest_is_comparable_to_predecessor: bool
    continuity_state: str
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    availability: AnalyticsAvailability = AnalyticsAvailability.AVAILABLE
    analysis_type: AnalysisType = AnalysisType.LATEST_STATE
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["availability"] = self.availability.value
        result["dimensions"] = list(self.dimensions)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


@dataclass(frozen=True, slots=True)
class ComparisonAnalytics:
    analysis_id: str
    analysis_type: AnalysisType
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    as_of_period_id: str
    prior_period_id: str | None
    current_observation_id: str
    prior_observation_id: str | None
    current_value: str
    prior_value: str | None
    native_unit_change: str | None
    relative_change: str | None
    relative_change_availability: AnalyticsAvailability
    relative_change_reason: str
    percentage_point_change: str | None
    sign_transition: SignTransition
    availability: AnalyticsAvailability
    reason: str
    continuity_result: str
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        for name in (
            "analysis_type",
            "relative_change_availability",
            "sign_transition",
            "availability",
        ):
            result[name] = getattr(self, name).value
        result["dimensions"] = list(self.dimensions)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


def derive_comparison(
    current: CanonicalObservation,
    candidates: Sequence[CanonicalObservation],
    *,
    analysis_type: AnalysisType,
) -> ComparisonAnalytics:
    """Derive one exact-period QoQ/YoY comparison without nearest fallback."""

    if analysis_type not in {AnalysisType.QOQ, AnalysisType.YOY}:
        raise DerivedAnalyticsError("Comparison analytics must be QOQ or YOY.")
    foundation = (
        safe_qoq(current.evidence, (item.evidence for item in candidates))
        if analysis_type is AnalysisType.QOQ
        else safe_yoy(current.evidence, (item.evidence for item in candidates))
    )
    prior_matches = [
        item for item in candidates if item.evidence.period.period_id == foundation.prior_period_id
    ]
    prior = prior_matches[0] if len(prior_matches) == 1 else None
    availability = _map_comparison_availability(foundation.reason, current, prior)
    current_value = current.evidence.normalized_value
    if current_value is None:
        raise DerivedAnalyticsError("Canonical numeric observations require a current value.")
    prior_value = prior.evidence.normalized_value if prior is not None else None
    native_change = None
    relative = None
    relative_state = AnalyticsAvailability.NOT_APPLICABLE
    relative_reason = "Comparison is unavailable."
    pp_change = None
    sign_transition = SignTransition.NONE
    if foundation.state is ComparisonState.COMPLETE:
        if prior_value is None:
            raise DerivedAnalyticsError("Complete comparison lacks a prior value.")
        current_decimal = _decimal(current_value)
        prior_decimal = _decimal(prior_value)
        native_change = canonical_decimal(current_decimal - prior_decimal)
        relative, relative_state, relative_reason = _relative_change(
            current=current_decimal,
            prior=prior_decimal,
            unit_id=current.evidence.driver.unit_id,
        )
        pp_change = _percentage_point_change(
            current_decimal, prior_decimal, current.evidence.driver.unit_id
        )
        sign_transition = _sign_transition(prior_decimal, current_decimal)
    input_rows = tuple(
        sorted(
            {
                current.observation_id,
                *(tuple([prior.observation_id]) if prior is not None else ()),
            }
        )
    )
    evidence_ids = tuple(
        sorted(
            {
                current.evidence.evidence_id,
                *(tuple([prior.evidence.evidence_id]) if prior is not None else ()),
            }
        )
    )
    return ComparisonAnalytics(
        analysis_id=_hash_id(
            "analysis",
            current.evidence.driver.company_id,
            analysis_type.value,
            current.observation_id,
        ),
        analysis_type=analysis_type,
        ticker=current.evidence.driver.company_id,
        driver_id=current.evidence.driver.driver_id,
        definition_version=current.evidence.driver.definition_version,
        dimension_set_id=current.evidence.driver.dimension_set_id,
        dimensions=_dimensions(current),
        unit_id=current.evidence.driver.unit_id,
        as_of_period_id=current.evidence.period.period_id,
        prior_period_id=foundation.prior_period_id,
        current_observation_id=current.observation_id,
        prior_observation_id=prior.observation_id if prior is not None else None,
        current_value=current_value,
        prior_value=prior_value,
        native_unit_change=native_change,
        relative_change=relative,
        relative_change_availability=relative_state,
        relative_change_reason=relative_reason,
        percentage_point_change=pp_change,
        sign_transition=sign_transition,
        availability=availability,
        reason=foundation.reason.value,
        continuity_result=current.evidence.continuity.state.value,
        input_observation_ids=input_rows,
        source_evidence_ids=evidence_ids,
        knowledge_date_boundary=_knowledge_boundary(
            item for item in (current, prior) if item is not None
        ),
    )


@dataclass(frozen=True, slots=True)
class AggregateAnalytics:
    analysis_id: str
    analysis_type: AnalysisType
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    as_of_period_id: str
    aggregation_semantics: str
    availability: AnalyticsAvailability
    reason: str
    value: str | None
    required_constituent_period_ids: tuple[str, ...]
    observed_constituent_period_ids: tuple[str, ...]
    missing_constituent_period_ids: tuple[str, ...]
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["availability"] = self.availability.value
        result["dimensions"] = list(self.dimensions)
        for name in (
            "required_constituent_period_ids",
            "observed_constituent_period_ids",
            "missing_constituent_period_ids",
            "input_observation_ids",
            "source_evidence_ids",
        ):
            result[name] = list(getattr(self, name))
        return result


def _aggregate_availability(reason: AggregateReason) -> AnalyticsAvailability:
    return {
        AggregateReason.COMPLETE: AnalyticsAvailability.AVAILABLE,
        AggregateReason.UNAVAILABLE_INCOMPLETE_PERIOD_SET: AnalyticsAvailability.INCOMPLETE_PERIOD_SET,
        AggregateReason.AGGREGATION_SEMANTICS_INVALID: AnalyticsAvailability.AGGREGATION_NOT_ALLOWED,
        AggregateReason.DEFINITION_INCOMPATIBLE: AnalyticsAvailability.DEFINITION_BREAK,
        AggregateReason.DIMENSION_INCOMPATIBLE: AnalyticsAvailability.DIMENSION_MISMATCH,
        AggregateReason.UNIT_INCOMPATIBLE: AnalyticsAvailability.UNIT_INCOMPATIBLE,
        AggregateReason.CLASSIFICATION_INCOMPATIBLE: AnalyticsAvailability.NEEDS_REVIEW,
        AggregateReason.VALUE_UNAVAILABLE: AnalyticsAvailability.INCOMPLETE_PERIOD_SET,
        AggregateReason.NOT_APPLICABLE: AnalyticsAvailability.NOT_APPLICABLE,
        AggregateReason.DUPLICATE_CONSTITUENT_PERIOD: AnalyticsAvailability.PERIOD_INCOMPATIBLE,
        AggregateReason.UNEXPECTED_CONSTITUENT_PERIOD: AnalyticsAvailability.PERIOD_INCOMPATIBLE,
    }[reason]


def _period_maps(
    package: ShadowRegistryPackage,
) -> tuple[dict[tuple[int, int], FiscalQuarterPeriod], dict[str, CanonicalObservation]]:
    by_key: dict[tuple[int, int], FiscalQuarterPeriod] = {}
    by_id: dict[str, CanonicalObservation] = {}
    for item in package.observations:
        by_id[item.observation_id] = item
        period = item.evidence.period
        if not isinstance(period, FiscalQuarterPeriod):
            continue
        key = (period.fiscal_year, period.fiscal_quarter)
        prior = by_key.get(key)
        if prior is not None and prior.to_dict() != period.to_dict():
            raise DerivedAnalyticsError("One fiscal key resolved to competing typed periods.")
        by_key[key] = period
    return by_key, by_id


def _blocked_aggregate(
    observation: CanonicalObservation,
    *,
    analysis_type: AnalysisType,
    as_of_period_id: str,
    availability: AnalyticsAvailability,
    reason: str,
    required_ids: Iterable[str] = (),
    observed_ids: Iterable[str] = (),
    missing_ids: Iterable[str] = (),
    input_observation_ids: Iterable[str] = (),
    source_evidence_ids: Iterable[str] = (),
    knowledge_date_boundary: str | None = None,
) -> AggregateAnalytics:
    driver = observation.evidence.driver
    return AggregateAnalytics(
        analysis_id=_hash_id(
            "analysis",
            driver.company_id,
            analysis_type.value,
            driver.driver_id,
            driver.definition_version,
            driver.dimension_set_id,
            as_of_period_id,
        ),
        analysis_type=analysis_type,
        ticker=driver.company_id,
        driver_id=driver.driver_id,
        definition_version=driver.definition_version,
        dimension_set_id=driver.dimension_set_id,
        dimensions=_dimensions(observation),
        unit_id=driver.unit_id,
        as_of_period_id=as_of_period_id,
        aggregation_semantics=driver.aggregation_semantics.value,
        availability=availability,
        reason=reason,
        value=None,
        required_constituent_period_ids=tuple(required_ids),
        observed_constituent_period_ids=tuple(observed_ids),
        missing_constituent_period_ids=tuple(missing_ids),
        input_observation_ids=tuple(sorted(input_observation_ids)),
        source_evidence_ids=tuple(sorted(source_evidence_ids)),
        knowledge_date_boundary=knowledge_date_boundary,
    )


def _derive_aggregate(
    observation: CanonicalObservation,
    group: Sequence[CanonicalObservation],
    period_map: Mapping[tuple[int, int], FiscalQuarterPeriod],
    *,
    analysis_type: AnalysisType,
) -> AggregateAnalytics:
    if analysis_type not in {AnalysisType.TTM, AnalysisType.FISCAL_YEAR}:
        raise DerivedAnalyticsError("Aggregate type must be TTM or FISCAL_YEAR.")
    period = observation.evidence.period
    if not isinstance(period, FiscalQuarterPeriod):
        return _blocked_aggregate(
            observation,
            analysis_type=analysis_type,
            as_of_period_id=period.period_id,
            availability=AnalyticsAvailability.PERIOD_INCOMPATIBLE,
            reason="Analytics require a typed fiscal quarter.",
            input_observation_ids=(observation.observation_id,),
            source_evidence_ids=(observation.evidence.evidence_id,),
            knowledge_date_boundary=_knowledge_boundary((observation,)),
        )
    driver = observation.evidence.driver
    if driver.aggregation_semantics is not AggregationSemantics.SUMMABLE:
        return _blocked_aggregate(
            observation,
            analysis_type=analysis_type,
            as_of_period_id=period.period_id,
            availability=AnalyticsAvailability.AGGREGATION_NOT_ALLOWED,
            reason=AggregateReason.AGGREGATION_SEMANTICS_INVALID.value,
            input_observation_ids=(observation.observation_id,),
            source_evidence_ids=(observation.evidence.evidence_id,),
            knowledge_date_boundary=_knowledge_boundary((observation,)),
        )
    if analysis_type is AnalysisType.TTM:
        keys = ttm_quarter_keys(period.fiscal_year, period.fiscal_quarter)
        as_of_period_id = (
            f"period:{driver.company_id.lower()}:ttm-{period.fiscal_year}-q{period.fiscal_quarter}@1"
        )
    else:
        fiscal_year = period.fiscal_year if period.fiscal_quarter == 4 else period.fiscal_year - 1
        keys = tuple((fiscal_year, quarter) for quarter in range(1, 5))
        as_of_period_id = f"period:{driver.company_id.lower()}:{fiscal_year}-fy@1"
    missing_keys = [key for key in keys if key not in period_map]
    if missing_keys:
        required_ids = tuple(
            period_map[key].period_id
            if key in period_map
            else f"period:{driver.company_id.lower()}:{key[0]}-q{key[1]}@1"
            for key in keys
        )
        observed_rows = tuple(
            item
            for item in group
            if isinstance(item.evidence.period, FiscalQuarterPeriod)
            and (item.evidence.period.fiscal_year, item.evidence.period.fiscal_quarter) in keys
        )
        observed = tuple(item.evidence.period.period_id for item in observed_rows)
        missing_ids = tuple(
            required_ids[index] for index, key in enumerate(keys) if key in missing_keys
        )
        return _blocked_aggregate(
            observation,
            analysis_type=analysis_type,
            as_of_period_id=as_of_period_id,
            availability=AnalyticsAvailability.INCOMPLETE_PERIOD_SET,
            reason=AggregateReason.UNAVAILABLE_INCOMPLETE_PERIOD_SET.value,
            required_ids=required_ids,
            observed_ids=observed,
            missing_ids=missing_ids,
            input_observation_ids=(item.observation_id for item in observed_rows),
            source_evidence_ids=(item.evidence.evidence_id for item in observed_rows),
            knowledge_date_boundary=_knowledge_boundary(observed_rows),
        )
    quarters = tuple(period_map[key] for key in keys)
    if analysis_type is AnalysisType.TTM:
        requested = TrailingTwelveMonthsPeriod(
            period_id=as_of_period_id,
            company_id=driver.company_id,
            ending_quarter=quarters[-1],
            constituent_quarters=quarters,
        )
    else:
        requested = FiscalYearPeriod(
            period_id=as_of_period_id,
            company_id=driver.company_id,
            calendar=quarters[0].calendar,
            fiscal_year=quarters[0].fiscal_year,
            start_date=quarters[0].start_date,
            end_date=quarters[-1].end_date,
            week_count=(
                sum(item.week_count for item in quarters if item.week_count is not None)
                if all(item.week_count is not None for item in quarters)
                else None
            ),
            is_53_week_year=any(item.is_53_week_year for item in quarters),
        )
    required_ids = {item.period_id for item in quarters}
    constituents = tuple(
        item
        for item in group
        if item.evidence.period.period_id in required_ids
    )
    request = DurationAggregateRequest(
        request_id=_hash_id(
            "aggregate-request",
            driver.company_id,
            analysis_type.value,
            driver.driver_id,
            driver.definition_version,
            driver.dimension_set_id,
            as_of_period_id,
        ),
        driver=driver,
        requested_period=requested,
        required_constituent_quarters=quarters,
        aggregation_method=AggregationMethod.SUM,
    )
    result = aggregate_duration_fail_closed(
        request, (item.evidence for item in constituents)
    )
    by_evidence = {item.evidence.evidence_id: item for item in constituents}
    used = tuple(
        sorted(
            {
                by_evidence[evidence_id].observation_id
                for evidence_id in result.evidence_ids
                if evidence_id in by_evidence
            }
        )
    )
    return AggregateAnalytics(
        analysis_id=_hash_id(
            "analysis",
            driver.company_id,
            analysis_type.value,
            driver.driver_id,
            driver.definition_version,
            driver.dimension_set_id,
            as_of_period_id,
        ),
        analysis_type=analysis_type,
        ticker=driver.company_id,
        driver_id=driver.driver_id,
        definition_version=driver.definition_version,
        dimension_set_id=driver.dimension_set_id,
        dimensions=_dimensions(observation),
        unit_id=driver.unit_id,
        as_of_period_id=as_of_period_id,
        aggregation_semantics=driver.aggregation_semantics.value,
        availability=_aggregate_availability(result.reason),
        reason=result.reason.value,
        value=result.value,
        required_constituent_period_ids=result.required_constituent_period_ids,
        observed_constituent_period_ids=result.observed_constituent_period_ids,
        missing_constituent_period_ids=result.missing_constituent_period_ids,
        input_observation_ids=used,
        source_evidence_ids=result.evidence_ids,
        knowledge_date_boundary=_knowledge_boundary(
            by_evidence[evidence_id]
            for evidence_id in result.evidence_ids
            if evidence_id in by_evidence
        ),
    )


@dataclass(frozen=True, slots=True)
class AggregateChangeAnalytics:
    analysis_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    current_aggregate_id: str
    prior_aggregate_id: str | None
    current_period_id: str
    prior_period_id: str | None
    current_value: str | None
    prior_value: str | None
    native_unit_change: str | None
    relative_change: str | None
    relative_change_availability: AnalyticsAvailability
    availability: AnalyticsAvailability
    reason: str
    input_aggregate_ids: tuple[str, ...]
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    analysis_type: AnalysisType = AnalysisType.TTM_YOY
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["relative_change_availability"] = self.relative_change_availability.value
        result["availability"] = self.availability.value
        result["dimensions"] = list(self.dimensions)
        result["input_aggregate_ids"] = list(self.input_aggregate_ids)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


@dataclass(frozen=True, slots=True)
class TrendAnalytics:
    analysis_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    availability: AnalyticsAvailability
    state: TrendState
    reason: str
    window_start_period_id: str | None
    window_end_period_id: str | None
    start_value: str | None
    end_value: str | None
    native_change: str | None
    upward_move_count: int
    downward_move_count: int
    unchanged_move_count: int
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    analysis_type: AnalysisType = AnalysisType.TREND_4Q
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["availability"] = self.availability.value
        result["state"] = self.state.value
        result["dimensions"] = list(self.dimensions)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


@dataclass(frozen=True, slots=True)
class AccelerationAnalytics:
    analysis_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    availability: AnalyticsAvailability
    state: AccelerationState
    inflection_state: InflectionState
    first_native_delta: str | None
    second_native_delta: str | None
    second_difference: str | None
    delta_semantics: str
    reason: str
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    analysis_type: AnalysisType = AnalysisType.ACCELERATION
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["availability"] = self.availability.value
        result["state"] = self.state.value
        result["inflection_state"] = self.inflection_state.value
        result["dimensions"] = list(self.dimensions)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


@dataclass(frozen=True, slots=True)
class ConsistencyAnalytics:
    analysis_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    availability: AnalyticsAvailability
    state: ConsistencyState
    upward_move_count: int
    downward_move_count: int
    unchanged_move_count: int
    dominant_direction_share: str | None
    input_observation_ids: tuple[str, ...]
    reason: str
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    analysis_type: AnalysisType = AnalysisType.CONSISTENCY_4Q
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["availability"] = self.availability.value
        result["state"] = self.state.value
        result["dimensions"] = list(self.dimensions)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


@dataclass(frozen=True, slots=True)
class VariabilityAnalytics:
    analysis_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    unit_id: str
    availability: AnalyticsAvailability
    reason: str
    window_observation_count: int
    minimum: str | None
    maximum: str | None
    value_range: str | None
    arithmetic_mean: str | None
    population_standard_deviation: str | None
    coefficient_of_variation: str | None
    coefficient_of_variation_availability: AnalyticsAvailability
    input_observation_ids: tuple[str, ...]
    source_evidence_ids: tuple[str, ...]
    knowledge_date_boundary: str | None
    analysis_type: AnalysisType = AnalysisType.VARIABILITY_12Q
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analysis_type"] = self.analysis_type.value
        result["availability"] = self.availability.value
        result["coefficient_of_variation_availability"] = (
            self.coefficient_of_variation_availability.value
        )
        result["dimensions"] = list(self.dimensions)
        result["input_observation_ids"] = list(self.input_observation_ids)
        result["source_evidence_ids"] = list(self.source_evidence_ids)
        return result


def _latest_segment_observations(
    package: ShadowRegistryPackage,
    group_key: tuple[str, int, str, str],
    observation_by_id: Mapping[str, CanonicalObservation],
) -> tuple[CanonicalObservation, ...]:
    candidates = [
        segment
        for segment in package.series
        if (
            segment.canonical_driver_id,
            segment.definition_version,
            segment.dimension_set_id,
            segment.unit_id,
        )
        == group_key
    ]
    if not candidates:
        return ()
    chosen = max(
        candidates,
        key=lambda segment: max(
            observation_by_id[item].evidence.period.fiscal_ordinal
            for item in segment.observation_ids
        ),
    )
    return _ordered(observation_by_id[item] for item in chosen.observation_ids)


def _move_counts(values: Sequence[Decimal]) -> tuple[int, int, int, tuple[Decimal, ...]]:
    deltas = tuple(later - earlier for earlier, later in zip(values, values[1:]))
    return (
        sum(item > 0 for item in deltas),
        sum(item < 0 for item in deltas),
        sum(item == 0 for item in deltas),
        deltas,
    )


def _trend_for_group(
    package: ShadowRegistryPackage,
    group_key: tuple[str, int, str, str],
    observation_by_id: Mapping[str, CanonicalObservation],
) -> TrendAnalytics:
    segment = _latest_segment_observations(package, group_key, observation_by_id)
    representative = max(
        (item for item in package.observations if _group_key(item) == group_key),
        key=lambda item: item.evidence.period.fiscal_ordinal,
    )
    driver = representative.evidence.driver
    analysis_id = _hash_id("analysis", driver.company_id, "trend", *group_key)
    if len(segment) < 4:
        return TrendAnalytics(
            analysis_id=analysis_id,
            ticker=driver.company_id,
            driver_id=driver.driver_id,
            definition_version=driver.definition_version,
            dimension_set_id=driver.dimension_set_id,
            dimensions=_dimensions(representative),
            unit_id=driver.unit_id,
            availability=AnalyticsAvailability.INSUFFICIENT_HISTORY,
            state=TrendState.INSUFFICIENT_DATA,
            reason="Latest uninterrupted series segment has fewer than four observations.",
            window_start_period_id=None,
            window_end_period_id=None,
            start_value=None,
            end_value=None,
            native_change=None,
            upward_move_count=0,
            downward_move_count=0,
            unchanged_move_count=0,
            input_observation_ids=tuple(item.observation_id for item in segment),
            source_evidence_ids=_source_evidence_ids(segment),
            knowledge_date_boundary=_knowledge_boundary(segment),
        )
    window = segment[-4:]
    values = [_decimal(item.evidence.normalized_value or "0") for item in window]
    up, down, unchanged, _ = _move_counts(values)
    state = (
        TrendState.UP
        if up == 3
        else TrendState.DOWN
        if down == 3
        else TrendState.UNCHANGED
        if unchanged == 3
        else TrendState.MIXED
    )
    return TrendAnalytics(
        analysis_id=analysis_id,
        ticker=driver.company_id,
        driver_id=driver.driver_id,
        definition_version=driver.definition_version,
        dimension_set_id=driver.dimension_set_id,
        dimensions=_dimensions(representative),
        unit_id=driver.unit_id,
        availability=AnalyticsAvailability.AVAILABLE,
        state=state,
        reason="Exact four-observation window; UP/DOWN require all three sequential moves in one direction, UNCHANGED requires three exact zeros, otherwise MIXED.",
        window_start_period_id=window[0].evidence.period.period_id,
        window_end_period_id=window[-1].evidence.period.period_id,
        start_value=window[0].evidence.normalized_value,
        end_value=window[-1].evidence.normalized_value,
        native_change=canonical_decimal(values[-1] - values[0]),
        upward_move_count=up,
        downward_move_count=down,
        unchanged_move_count=unchanged,
        input_observation_ids=tuple(item.observation_id for item in window),
        source_evidence_ids=_source_evidence_ids(window),
        knowledge_date_boundary=_knowledge_boundary(window),
    )


def _acceleration_for_group(
    package: ShadowRegistryPackage,
    group_key: tuple[str, int, str, str],
    observation_by_id: Mapping[str, CanonicalObservation],
) -> AccelerationAnalytics:
    segment = _latest_segment_observations(package, group_key, observation_by_id)
    representative = max(
        (item for item in package.observations if _group_key(item) == group_key),
        key=lambda item: item.evidence.period.fiscal_ordinal,
    )
    driver = representative.evidence.driver
    analysis_id = _hash_id("analysis", driver.company_id, "acceleration", *group_key)
    semantics = (
        "CONSECUTIVE_PERCENTAGE_POINT_DELTAS"
        if driver.unit_id in RATE_UNIT_IDS
        else "CONSECUTIVE_NATIVE_UNIT_DELTAS"
    )
    if len(segment) < 3:
        return AccelerationAnalytics(
            analysis_id=analysis_id,
            ticker=driver.company_id,
            driver_id=driver.driver_id,
            definition_version=driver.definition_version,
            dimension_set_id=driver.dimension_set_id,
            dimensions=_dimensions(representative),
            unit_id=driver.unit_id,
            availability=AnalyticsAvailability.INSUFFICIENT_HISTORY,
            state=AccelerationState.INSUFFICIENT_DATA,
            inflection_state=InflectionState.INSUFFICIENT_DATA,
            first_native_delta=None,
            second_native_delta=None,
            second_difference=None,
            delta_semantics=semantics,
            reason="Latest uninterrupted series segment has fewer than three observations.",
            input_observation_ids=tuple(item.observation_id for item in segment),
            source_evidence_ids=_source_evidence_ids(segment),
            knowledge_date_boundary=_knowledge_boundary(segment),
        )
    window = segment[-3:]
    values = [_decimal(item.evidence.normalized_value or "0") for item in window]
    first = values[1] - values[0]
    second = values[2] - values[1]
    difference = second - first
    state = (
        AccelerationState.POSITIVE_ACCELERATION
        if difference > 0
        else AccelerationState.NEGATIVE_ACCELERATION
        if difference < 0
        else AccelerationState.UNCHANGED_RATE
    )
    inflection = (
        InflectionState.DOWN_TO_UP
        if first < 0 < second
        else InflectionState.UP_TO_DOWN
        if first > 0 > second
        else InflectionState.CONTINUED_UP
        if first > 0 and second > 0
        else InflectionState.CONTINUED_DOWN
        if first < 0 and second < 0
        else InflectionState.MIXED_OR_FLAT
    )
    return AccelerationAnalytics(
        analysis_id=analysis_id,
        ticker=driver.company_id,
        driver_id=driver.driver_id,
        definition_version=driver.definition_version,
        dimension_set_id=driver.dimension_set_id,
        dimensions=_dimensions(representative),
        unit_id=driver.unit_id,
        availability=AnalyticsAvailability.AVAILABLE,
        state=state,
        inflection_state=inflection,
        first_native_delta=canonical_decimal(first),
        second_native_delta=canonical_decimal(second),
        second_difference=canonical_decimal(difference),
        delta_semantics=semantics,
        reason="Neutral second difference over three exact consecutive observations.",
        input_observation_ids=tuple(item.observation_id for item in window),
        source_evidence_ids=_source_evidence_ids(window),
        knowledge_date_boundary=_knowledge_boundary(window),
    )


def _consistency_for_group(
    package: ShadowRegistryPackage,
    group_key: tuple[str, int, str, str],
    observation_by_id: Mapping[str, CanonicalObservation],
) -> ConsistencyAnalytics:
    segment = _latest_segment_observations(package, group_key, observation_by_id)
    representative = max(
        (item for item in package.observations if _group_key(item) == group_key),
        key=lambda item: item.evidence.period.fiscal_ordinal,
    )
    driver = representative.evidence.driver
    analysis_id = _hash_id("analysis", driver.company_id, "consistency", *group_key)
    if len(segment) < 4:
        return ConsistencyAnalytics(
            analysis_id=analysis_id,
            ticker=driver.company_id,
            driver_id=driver.driver_id,
            definition_version=driver.definition_version,
            dimension_set_id=driver.dimension_set_id,
            dimensions=_dimensions(representative),
            availability=AnalyticsAvailability.INSUFFICIENT_HISTORY,
            state=ConsistencyState.INSUFFICIENT_DATA,
            upward_move_count=0,
            downward_move_count=0,
            unchanged_move_count=0,
            dominant_direction_share=None,
            input_observation_ids=tuple(item.observation_id for item in segment),
            reason="Consistency requires four exact consecutive observations.",
            source_evidence_ids=_source_evidence_ids(segment),
            knowledge_date_boundary=_knowledge_boundary(segment),
        )
    window = segment[-4:]
    values = [_decimal(item.evidence.normalized_value or "0") for item in window]
    up, down, unchanged, _ = _move_counts(values)
    state = (
        ConsistencyState.CONSISTENT_UP
        if up == 3
        else ConsistencyState.CONSISTENT_DOWN
        if down == 3
        else ConsistencyState.UNCHANGED
        if unchanged == 3
        else ConsistencyState.MIXED
    )
    dominant = canonical_decimal(Decimal(max(up, down, unchanged)) / Decimal(3))
    return ConsistencyAnalytics(
        analysis_id=analysis_id,
        ticker=driver.company_id,
        driver_id=driver.driver_id,
        definition_version=driver.definition_version,
        dimension_set_id=driver.dimension_set_id,
        dimensions=_dimensions(representative),
        availability=AnalyticsAvailability.AVAILABLE,
        state=state,
        upward_move_count=up,
        downward_move_count=down,
        unchanged_move_count=unchanged,
        dominant_direction_share=dominant,
        input_observation_ids=tuple(item.observation_id for item in window),
        reason="Direction counts cover the three sequential moves in the exact 4Q window.",
        source_evidence_ids=_source_evidence_ids(window),
        knowledge_date_boundary=_knowledge_boundary(window),
    )


def _variability_for_group(
    package: ShadowRegistryPackage,
    group_key: tuple[str, int, str, str],
    observation_by_id: Mapping[str, CanonicalObservation],
) -> VariabilityAnalytics:
    segment = _latest_segment_observations(package, group_key, observation_by_id)[-12:]
    representative = max(
        (item for item in package.observations if _group_key(item) == group_key),
        key=lambda item: item.evidence.period.fiscal_ordinal,
    )
    driver = representative.evidence.driver
    analysis_id = _hash_id("analysis", driver.company_id, "variability", *group_key)
    if len(segment) < 2:
        return VariabilityAnalytics(
            analysis_id=analysis_id,
            ticker=driver.company_id,
            driver_id=driver.driver_id,
            definition_version=driver.definition_version,
            dimension_set_id=driver.dimension_set_id,
            dimensions=_dimensions(representative),
            unit_id=driver.unit_id,
            availability=AnalyticsAvailability.INSUFFICIENT_HISTORY,
            reason="Variability requires at least two observations in one uninterrupted segment.",
            window_observation_count=len(segment),
            minimum=None,
            maximum=None,
            value_range=None,
            arithmetic_mean=None,
            population_standard_deviation=None,
            coefficient_of_variation=None,
            coefficient_of_variation_availability=AnalyticsAvailability.NOT_APPLICABLE,
            input_observation_ids=tuple(item.observation_id for item in segment),
            source_evidence_ids=_source_evidence_ids(segment),
            knowledge_date_boundary=_knowledge_boundary(segment),
        )
    values = [_decimal(item.evidence.normalized_value or "0") for item in segment]
    with localcontext() as context:
        context.prec = 34
        mean = sum(values, Decimal("0")) / Decimal(len(values))
        variance = sum((item - mean) ** 2 for item in values) / Decimal(len(values))
        standard_deviation = variance.sqrt()
        cv_available = all(item > 0 for item in values) and mean > 0
        coefficient = standard_deviation / mean if cv_available else None
    return VariabilityAnalytics(
        analysis_id=analysis_id,
        ticker=driver.company_id,
        driver_id=driver.driver_id,
        definition_version=driver.definition_version,
        dimension_set_id=driver.dimension_set_id,
        dimensions=_dimensions(representative),
        unit_id=driver.unit_id,
        availability=AnalyticsAvailability.AVAILABLE,
        reason="Transparent min/max/range and population standard deviation over up to 12 uninterrupted quarters.",
        window_observation_count=len(segment),
        minimum=canonical_decimal(min(values)),
        maximum=canonical_decimal(max(values)),
        value_range=canonical_decimal(max(values) - min(values)),
        arithmetic_mean=canonical_decimal(mean),
        population_standard_deviation=canonical_decimal(standard_deviation),
        coefficient_of_variation=(
            canonical_decimal(coefficient) if coefficient is not None else None
        ),
        coefficient_of_variation_availability=(
            AnalyticsAvailability.AVAILABLE
            if coefficient is not None
            else AnalyticsAvailability.NOT_APPLICABLE
        ),
        input_observation_ids=tuple(item.observation_id for item in segment),
        source_evidence_ids=_source_evidence_ids(segment),
        knowledge_date_boundary=_knowledge_boundary(segment),
    )


def _forecast_readiness(
    linkage: FinancialLinkageKind,
    capability: ForecastEvidenceCapability,
) -> ForecastEvidenceReadiness:
    if capability in {
        ForecastEvidenceCapability.HISTORICAL_ONLY,
        ForecastEvidenceCapability.NOT_FORECASTABLE,
    } or linkage is FinancialLinkageKind.NONE:
        return ForecastEvidenceReadiness.NOT_FORECAST_RELEVANT
    if linkage in {
        FinancialLinkageKind.SOURCE_DEFINED_CAUSAL_RELATION,
        FinancialLinkageKind.ACCOUNTING_IDENTITY,
        FinancialLinkageKind.ECONOMICALLY_JUSTIFIED_MODEL,
    } and capability in {
        ForecastEvidenceCapability.DIRECT_FORECAST_INPUT,
        ForecastEvidenceCapability.LEADING_INDICATOR,
        ForecastEvidenceCapability.SENSITIVITY_INPUT,
    }:
        return ForecastEvidenceReadiness.FORECAST_EVIDENCE_READY
    if linkage is FinancialLinkageKind.SPECULATIVE_ASSOCIATION:
        return ForecastEvidenceReadiness.NEEDS_RELATIONSHIP_REVIEW
    return ForecastEvidenceReadiness.CONTEXT_ONLY


@dataclass(frozen=True, slots=True)
class AnalyticalSignal:
    signal_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    availability: AnalyticsAvailability
    latest_observation_id: str | None
    latest_period_id: str | None
    latest_value: str | None
    latest_source_evidence_ids: tuple[str, ...]
    qoq_analysis_id: str | None
    qoq_availability: str
    yoy_analysis_id: str | None
    yoy_availability: str
    trend_analysis_id: str | None
    trend_state: str
    acceleration_analysis_id: str | None
    acceleration_state: str
    consistency_state: str
    financial_linkage: str
    financial_target_owner_id: str | None
    forecast_capability: str
    forecast_evidence_readiness: ForecastEvidenceReadiness
    qualitative_attachment_ids: tuple[str, ...]
    evidence_completeness: str
    knowledge_date_boundary: str | None
    semantic_trend_interpretation: str = "SEMANTIC_TREND_INTERPRETATION_DEFERRED"
    forecast_number: None = None
    calculation_contract_version: str = (
        OPERATING_DRIVER_ANALYTICS_CALCULATION_CONTRACT_VERSION
    )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["availability"] = self.availability.value
        result["forecast_evidence_readiness"] = self.forecast_evidence_readiness.value
        result["dimensions"] = list(self.dimensions)
        result["latest_source_evidence_ids"] = list(self.latest_source_evidence_ids)
        result["qualitative_attachment_ids"] = list(self.qualitative_attachment_ids)
        return result


@dataclass(frozen=True, slots=True)
class BlockedAnalyticsRecord:
    blocked_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    analysis_type: str
    component: str
    as_of_period_id: str | None
    reason: AnalyticsAvailability
    source_analysis_id: str | None

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["reason"] = self.reason.value
        return result


@dataclass(frozen=True, slots=True)
class DerivedAnalyticsPackage:
    registry_package_sha256: str
    ticker: str
    latest_states: tuple[LatestStateAnalytics, ...]
    qoq_analytics: tuple[ComparisonAnalytics, ...]
    yoy_analytics: tuple[ComparisonAnalytics, ...]
    ttm_analytics: tuple[AggregateAnalytics, ...]
    fiscal_year_analytics: tuple[AggregateAnalytics, ...]
    ttm_change_analytics: tuple[AggregateChangeAnalytics, ...]
    trend_analytics: tuple[TrendAnalytics, ...]
    acceleration_analytics: tuple[AccelerationAnalytics, ...]
    consistency_analytics: tuple[ConsistencyAnalytics, ...]
    variability_analytics: tuple[VariabilityAnalytics, ...]
    analytical_signals: tuple[AnalyticalSignal, ...]
    blocked_analytics: tuple[BlockedAnalyticsRecord, ...]
    guidance_readiness: dict[str, Any]
    semantic_trend_interpretation: str = "SEMANTIC_TREND_INTERPRETATION_DEFERRED"
    new_ticker_specific_python_analytics_branch_count: int = 0
    duplicate_economic_owner_count: int = 0
    forecast_number_emission_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "acceleration_analytics": [item.to_dict() for item in self.acceleration_analytics],
            "analytical_signals": [item.to_dict() for item in self.analytical_signals],
            "blocked_analytics": [item.to_dict() for item in self.blocked_analytics],
            "consistency_analytics": [item.to_dict() for item in self.consistency_analytics],
            "contract_version": OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION,
            "duplicate_economic_owner_count": self.duplicate_economic_owner_count,
            "fiscal_year_analytics": [item.to_dict() for item in self.fiscal_year_analytics],
            "forecast_number_emission_count": self.forecast_number_emission_count,
            "guidance_readiness": self.guidance_readiness,
            "latest_states": [item.to_dict() for item in self.latest_states],
            "new_ticker_specific_python_analytics_branch_count": self.new_ticker_specific_python_analytics_branch_count,
            "qoq_analytics": [item.to_dict() for item in self.qoq_analytics],
            "registry_package_sha256": self.registry_package_sha256,
            "semantic_trend_interpretation": self.semantic_trend_interpretation,
            "ticker": self.ticker,
            "trend_analytics": [item.to_dict() for item in self.trend_analytics],
            "ttm_analytics": [item.to_dict() for item in self.ttm_analytics],
            "ttm_change_analytics": [item.to_dict() for item in self.ttm_change_analytics],
            "variability_analytics": [item.to_dict() for item in self.variability_analytics],
            "yoy_analytics": [item.to_dict() for item in self.yoy_analytics],
        }

    def serialize(self) -> bytes:
        return serialize_package(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.serialize()).hexdigest()


def _configured_signal_keys(
    package: ShadowRegistryPackage,
) -> dict[tuple[str, int, str, str], tuple[dict[str, Any], ...]]:
    result: dict[tuple[str, int, str, str], tuple[dict[str, Any], ...]] = {}
    for rule in package.profile.mapping_rules:
        if rule.action is not MappingAction.CANONICAL_DRIVER:
            continue
        assert rule.canonical_driver_id is not None
        assert rule.definition_version is not None
        definition = package.profile.definition(
            rule.canonical_driver_id, rule.definition_version
        )
        dimension_set_id = dimension_set_identity(
            (item.dimension_id, item.member_id) for item in rule.dimensions
        )
        key = (
            definition.driver_id,
            definition.definition_version,
            dimension_set_id,
            definition.unit_id,
        )
        result[key] = tuple(item.to_dict() for item in rule.dimensions)
    for observation in package.observations:
        result[_group_key(observation)] = _dimensions(observation)
    return result


def _latest_states(
    package: ShadowRegistryPackage,
    groups: Mapping[tuple[str, int, str, str], Sequence[CanonicalObservation]],
    observation_by_id: Mapping[str, CanonicalObservation],
) -> tuple[LatestStateAnalytics, ...]:
    result: list[LatestStateAnalytics] = []
    for key, items in sorted(groups.items()):
        ordered = _ordered(items)
        latest = ordered[-1]
        latest_segment = _latest_segment_observations(package, key, observation_by_id)
        comparable_segments = [
            segment
            for segment in package.series
            if (
                segment.canonical_driver_id,
                segment.definition_version,
                segment.dimension_set_id,
                segment.unit_id,
            )
            == key
            and len(segment.observation_ids) >= 2
        ]
        comparable = None
        comparable_depth = 0
        if comparable_segments:
            segment = max(
                comparable_segments,
                key=lambda value: max(
                    observation_by_id[item].evidence.period.fiscal_ordinal
                    for item in value.observation_ids
                ),
            )
            comparable_rows = _ordered(
                observation_by_id[item] for item in segment.observation_ids
            )
            comparable = comparable_rows[-1]
            comparable_depth = len(comparable_rows)
        result.append(
            LatestStateAnalytics(
                analysis_id=_hash_id(
                    "analysis", latest.evidence.driver.company_id, "latest", *key
                ),
                ticker=latest.evidence.driver.company_id,
                driver_id=latest.evidence.driver.driver_id,
                definition_version=latest.evidence.driver.definition_version,
                dimension_set_id=latest.evidence.driver.dimension_set_id,
                dimensions=_dimensions(latest),
                unit_id=latest.evidence.driver.unit_id,
                latest_observation_id=latest.observation_id,
                latest_period_id=latest.evidence.period.period_id,
                latest_value=latest.evidence.normalized_value or "",
                latest_knowledge_date=_knowledge_boundary((latest,)),
                comparable_history_depth=comparable_depth,
                latest_comparable_observation_id=(
                    comparable.observation_id if comparable is not None else None
                ),
                latest_comparable_period_id=(
                    comparable.evidence.period.period_id
                    if comparable is not None
                    else None
                ),
                latest_is_comparable_to_predecessor=len(latest_segment) >= 2,
                continuity_state=latest.evidence.continuity.state.value,
                input_observation_ids=(latest.observation_id,),
                source_evidence_ids=(latest.evidence.evidence_id,),
            )
        )
    return tuple(sorted(result, key=lambda item: item.analysis_id))


def _ttm_changes(
    aggregates: Sequence[AggregateAnalytics],
    period_ordinals: Mapping[str, int],
) -> tuple[AggregateChangeAnalytics, ...]:
    available = [
        item
        for item in aggregates
        if item.availability is AnalyticsAvailability.AVAILABLE and item.value is not None
    ]
    by_key: dict[tuple[str, int, str, str, int], AggregateAnalytics] = {}
    for item in available:
        ending = item.required_constituent_period_ids[-1]
        if ending not in period_ordinals:
            continue
        by_key[
            (
                item.driver_id,
                item.definition_version,
                item.dimension_set_id,
                item.unit_id,
                period_ordinals[ending],
            )
        ] = item
    result: list[AggregateChangeAnalytics] = []
    for item in available:
        ending = item.required_constituent_period_ids[-1]
        ordinal = period_ordinals.get(ending)
        if ordinal is None:
            continue
        prior = by_key.get(
            (
                item.driver_id,
                item.definition_version,
                item.dimension_set_id,
                item.unit_id,
                ordinal - 4,
            )
        )
        current_value = _decimal(item.value or "0")
        prior_value = _decimal(prior.value or "0") if prior is not None else None
        if prior_value is None:
            availability = AnalyticsAvailability.PRIOR_PERIOD_MISSING
            native = relative = None
            relative_state = AnalyticsAvailability.NOT_APPLICABLE
            reason = "Exact prior-year TTM window is unavailable."
        else:
            availability = AnalyticsAvailability.AVAILABLE
            native = canonical_decimal(current_value - prior_value)
            relative, relative_state, reason = _relative_change(
                current=current_value, prior=prior_value, unit_id=item.unit_id
            )
        result.append(
            AggregateChangeAnalytics(
                analysis_id=_hash_id("analysis", item.ticker, "ttm-yoy", item.analysis_id),
                ticker=item.ticker,
                driver_id=item.driver_id,
                definition_version=item.definition_version,
                dimension_set_id=item.dimension_set_id,
                dimensions=item.dimensions,
                unit_id=item.unit_id,
                current_aggregate_id=item.analysis_id,
                prior_aggregate_id=prior.analysis_id if prior is not None else None,
                current_period_id=item.as_of_period_id,
                prior_period_id=prior.as_of_period_id if prior is not None else None,
                current_value=item.value,
                prior_value=prior.value if prior is not None else None,
                native_unit_change=native,
                relative_change=relative,
                relative_change_availability=relative_state,
                availability=availability,
                reason=reason,
                input_aggregate_ids=tuple(
                    item_value.analysis_id
                    for item_value in (item, prior)
                    if item_value is not None
                ),
                input_observation_ids=tuple(
                    sorted(
                        {
                            observation_id
                            for item_value in (item, prior)
                            if item_value is not None
                            for observation_id in item_value.input_observation_ids
                        }
                    )
                ),
                source_evidence_ids=tuple(
                    sorted(
                        {
                            evidence_id
                            for item_value in (item, prior)
                            if item_value is not None
                            for evidence_id in item_value.source_evidence_ids
                        }
                    )
                ),
                knowledge_date_boundary=max(
                    (
                        item_value.knowledge_date_boundary
                        for item_value in (item, prior)
                        if item_value is not None
                        and item_value.knowledge_date_boundary is not None
                    ),
                    default=None,
                ),
            )
        )
    return tuple(sorted(result, key=lambda item: item.analysis_id))


def _blocked_records(
    ticker: str,
    comparisons: Iterable[ComparisonAnalytics],
    aggregates: Iterable[AggregateAnalytics],
    aggregate_changes: Iterable[AggregateChangeAnalytics],
    trends: Iterable[TrendAnalytics],
    accelerations: Iterable[AccelerationAnalytics],
    consistencies: Iterable[ConsistencyAnalytics],
    variabilities: Iterable[VariabilityAnalytics],
    signals: Iterable[AnalyticalSignal],
) -> tuple[BlockedAnalyticsRecord, ...]:
    result: list[BlockedAnalyticsRecord] = []

    def add(
        *,
        driver_id: str,
        version: int,
        dimension: str,
        analysis_type: str,
        component: str,
        period: str | None,
        reason: AnalyticsAvailability,
        source_id: str | None,
    ) -> None:
        result.append(
            BlockedAnalyticsRecord(
                blocked_id=_hash_id(
                    "blocked", ticker, driver_id, version, dimension, analysis_type, component, period
                ),
                ticker=ticker,
                driver_id=driver_id,
                definition_version=version,
                dimension_set_id=dimension,
                analysis_type=analysis_type,
                component=component,
                as_of_period_id=period,
                reason=reason,
                source_analysis_id=source_id,
            )
        )

    for item in comparisons:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="COMPARISON",
                period=item.as_of_period_id,
                reason=item.availability,
                source_id=item.analysis_id,
            )
        elif item.relative_change_availability is AnalyticsAvailability.RELATIVE_CHANGE_UNDEFINED:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="RELATIVE_CHANGE",
                period=item.as_of_period_id,
                reason=item.relative_change_availability,
                source_id=item.analysis_id,
            )
    for item in aggregates:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="AGGREGATE",
                period=item.as_of_period_id,
                reason=item.availability,
                source_id=item.analysis_id,
            )
    for item in aggregate_changes:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="AGGREGATE_COMPARISON",
                period=item.current_period_id,
                reason=item.availability,
                source_id=item.analysis_id,
            )
        elif item.relative_change_availability is AnalyticsAvailability.RELATIVE_CHANGE_UNDEFINED:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="RELATIVE_CHANGE",
                period=item.current_period_id,
                reason=item.relative_change_availability,
                source_id=item.analysis_id,
            )
    for item in trends:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="TREND",
                period=item.window_end_period_id,
                reason=item.availability,
                source_id=item.analysis_id,
            )
    for item in accelerations:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="ACCELERATION",
                period=None,
                reason=item.availability,
                source_id=item.analysis_id,
            )
    for item in consistencies:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="CONSISTENCY",
                period=None,
                reason=item.availability,
                source_id=item.analysis_id,
            )
    for item in variabilities:
        if item.availability is not AnalyticsAvailability.AVAILABLE:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type=item.analysis_type.value,
                component="VARIABILITY",
                period=None,
                reason=item.availability,
                source_id=item.analysis_id,
            )
    for item in signals:
        if item.availability is AnalyticsAvailability.INSUFFICIENT_HISTORY:
            add(
                driver_id=item.driver_id,
                version=item.definition_version,
                dimension=item.dimension_set_id,
                analysis_type="ALL_NUMERIC_ANALYTICS",
                component="NO_NUMERIC_OBSERVATION",
                period=None,
                reason=AnalyticsAvailability.INSUFFICIENT_HISTORY,
                source_id=item.signal_id,
            )
    return tuple(sorted(result, key=lambda item: item.blocked_id))


def build_derived_analytics(
    package: ShadowRegistryPackage,
) -> DerivedAnalyticsPackage:
    """Build deterministic analytics without changing canonical registry data."""

    observation_by_id = {item.observation_id: item for item in package.observations}
    groups: dict[tuple[str, int, str, str], list[CanonicalObservation]] = defaultdict(list)
    pools: dict[tuple[str, str, str], list[CanonicalObservation]] = defaultdict(list)
    for item in package.observations:
        groups[_group_key(item)].append(item)
        pools[_comparison_pool_key(item)].append(item)
    latest = _latest_states(package, groups, observation_by_id)

    qoq: list[ComparisonAnalytics] = []
    yoy: list[ComparisonAnalytics] = []
    for pool in pools.values():
        ordered_pool = _ordered(pool)
        for current in ordered_pool:
            qoq.append(
                derive_comparison(current, ordered_pool, analysis_type=AnalysisType.QOQ)
            )
            yoy.append(
                derive_comparison(current, ordered_pool, analysis_type=AnalysisType.YOY)
            )

    period_map, _ = _period_maps(package)
    ttm: list[AggregateAnalytics] = []
    fiscal_year: list[AggregateAnalytics] = []
    for key, items in sorted(groups.items()):
        ordered_group = _ordered(items)
        for current in ordered_group:
            ttm.append(
                _derive_aggregate(
                    current, ordered_group, period_map, analysis_type=AnalysisType.TTM
                )
            )
        seen_years: set[int] = set()
        for current in reversed(ordered_group):
            period = current.evidence.period
            if not isinstance(period, FiscalQuarterPeriod):
                continue
            fiscal_year_value = (
                period.fiscal_year if period.fiscal_quarter == 4 else period.fiscal_year - 1
            )
            if fiscal_year_value in seen_years:
                continue
            seen_years.add(fiscal_year_value)
            fiscal_year.append(
                _derive_aggregate(
                    current,
                    ordered_group,
                    period_map,
                    analysis_type=AnalysisType.FISCAL_YEAR,
                )
            )

    trends = tuple(
        sorted(
            (
                _trend_for_group(package, key, observation_by_id)
                for key in groups
            ),
            key=lambda item: item.analysis_id,
        )
    )
    accelerations = tuple(
        sorted(
            (
                _acceleration_for_group(package, key, observation_by_id)
                for key in groups
            ),
            key=lambda item: item.analysis_id,
        )
    )
    consistencies = tuple(
        sorted(
            (
                _consistency_for_group(package, key, observation_by_id)
                for key in groups
            ),
            key=lambda item: item.analysis_id,
        )
    )
    variabilities = tuple(
        sorted(
            (
                _variability_for_group(package, key, observation_by_id)
                for key in groups
            ),
            key=lambda item: item.analysis_id,
        )
    )
    qoq_tuple = tuple(sorted(qoq, key=lambda item: item.analysis_id))
    yoy_tuple = tuple(sorted(yoy, key=lambda item: item.analysis_id))
    ttm_tuple = tuple(sorted(ttm, key=lambda item: item.analysis_id))
    fiscal_tuple = tuple(sorted(fiscal_year, key=lambda item: item.analysis_id))
    period_ordinals = {
        item.evidence.period.period_id: item.evidence.period.fiscal_ordinal
        for item in package.observations
        if isinstance(item.evidence.period, FiscalQuarterPeriod)
    }
    ttm_changes = _ttm_changes(ttm_tuple, period_ordinals)

    latest_by_key = {
        (item.driver_id, item.definition_version, item.dimension_set_id): item
        for item in latest
    }
    qoq_by_observation = {item.current_observation_id: item for item in qoq_tuple}
    yoy_by_observation = {item.current_observation_id: item for item in yoy_tuple}
    trend_by_key = {
        (item.driver_id, item.definition_version, item.dimension_set_id): item
        for item in trends
    }
    acceleration_by_key = {
        (item.driver_id, item.definition_version, item.dimension_set_id): item
        for item in accelerations
    }
    consistency_by_key = {
        (item.driver_id, item.definition_version, item.dimension_set_id): item
        for item in consistencies
    }
    attachments_by_key: dict[tuple[str, int, str], list[str]] = defaultdict(list)
    for item in package.attachments:
        if item.attachment_kind == "QUALITATIVE_SUPPORT":
            attachments_by_key[
                (item.canonical_driver_id, item.definition_version, item.dimension_set_id)
            ].append(item.attachment_id)

    signal_keys = _configured_signal_keys(package)
    signals: list[AnalyticalSignal] = []
    for key, dimensions in sorted(signal_keys.items()):
        driver_id, version, dimension_set_id, _unit_id_value = key
        definition = package.profile.definition(driver_id, version)
        latest_record = latest_by_key.get((driver_id, version, dimension_set_id))
        current_observation_id = (
            latest_record.latest_observation_id if latest_record is not None else None
        )
        qoq_record = (
            qoq_by_observation.get(current_observation_id)
            if current_observation_id is not None
            else None
        )
        yoy_record = (
            yoy_by_observation.get(current_observation_id)
            if current_observation_id is not None
            else None
        )
        trend_record = trend_by_key.get((driver_id, version, dimension_set_id))
        acceleration_record = acceleration_by_key.get(
            (driver_id, version, dimension_set_id)
        )
        consistency_record = consistency_by_key.get(
            (driver_id, version, dimension_set_id)
        )
        qualitative = tuple(
            sorted(attachments_by_key[(driver_id, version, dimension_set_id)])
        )
        signals.append(
            AnalyticalSignal(
                signal_id=_hash_id(
                    "analytical-signal",
                    package.profile.ticker,
                    driver_id,
                    version,
                    dimension_set_id,
                ),
                ticker=package.profile.ticker,
                driver_id=driver_id,
                definition_version=version,
                dimension_set_id=dimension_set_id,
                dimensions=dimensions,
                availability=(
                    AnalyticsAvailability.AVAILABLE
                    if latest_record is not None
                    else AnalyticsAvailability.INSUFFICIENT_HISTORY
                ),
                latest_observation_id=current_observation_id,
                latest_period_id=(
                    latest_record.latest_period_id if latest_record is not None else None
                ),
                latest_value=(
                    latest_record.latest_value if latest_record is not None else None
                ),
                latest_source_evidence_ids=(
                    latest_record.source_evidence_ids
                    if latest_record is not None
                    else ()
                ),
                qoq_analysis_id=qoq_record.analysis_id if qoq_record is not None else None,
                qoq_availability=(
                    qoq_record.availability.value
                    if qoq_record is not None
                    else AnalyticsAvailability.INSUFFICIENT_HISTORY.value
                ),
                yoy_analysis_id=yoy_record.analysis_id if yoy_record is not None else None,
                yoy_availability=(
                    yoy_record.availability.value
                    if yoy_record is not None
                    else AnalyticsAvailability.INSUFFICIENT_HISTORY.value
                ),
                trend_analysis_id=(
                    trend_record.analysis_id if trend_record is not None else None
                ),
                trend_state=(
                    trend_record.state.value
                    if trend_record is not None
                    else TrendState.INSUFFICIENT_DATA.value
                ),
                acceleration_analysis_id=(
                    acceleration_record.analysis_id
                    if acceleration_record is not None
                    else None
                ),
                acceleration_state=(
                    acceleration_record.state.value
                    if acceleration_record is not None
                    else AccelerationState.INSUFFICIENT_DATA.value
                ),
                consistency_state=(
                    consistency_record.state.value
                    if consistency_record is not None
                    else ConsistencyState.INSUFFICIENT_DATA.value
                ),
                financial_linkage=definition.financial_linkage.value,
                financial_target_owner_id=None,
                forecast_capability=definition.forecast_capability.value,
                forecast_evidence_readiness=_forecast_readiness(
                    definition.financial_linkage, definition.forecast_capability
                ),
                qualitative_attachment_ids=qualitative,
                evidence_completeness=(
                    "NUMERIC_HISTORY_AVAILABLE"
                    if latest_record is not None
                    else "NUMERIC_HISTORY_UNAVAILABLE_QUALITATIVE_SUPPORT_PRESERVED"
                    if qualitative
                    else "NUMERIC_HISTORY_UNAVAILABLE"
                ),
                knowledge_date_boundary=(
                    latest_record.latest_knowledge_date
                    if latest_record is not None
                    else None
                ),
            )
        )
    signals_tuple = tuple(sorted(signals, key=lambda item: item.signal_id))
    comparisons = (*qoq_tuple, *yoy_tuple)
    aggregates = (*ttm_tuple, *fiscal_tuple)
    blocked = _blocked_records(
        package.profile.ticker,
        comparisons,
        aggregates,
        ttm_changes,
        trends,
        accelerations,
        consistencies,
        variabilities,
        signals_tuple,
    )
    guidance_rows = [
        item
        for item in package.evidence_census
        if item.disposition.value == "GUIDANCE_REFERENCE"
    ]
    guidance_readiness = {
        "driver_records": [
            {
                "definition_version": signal.definition_version,
                "dimension_set_id": signal.dimension_set_id,
                "driver_id": signal.driver_id,
                "readiness": GuidanceComparisonReadiness.NO_GUIDANCE_REFERENCE.value,
                "reason": "No guidance reference carries the canonical driver, definition, dimension, and target-period identities required for comparison.",
            }
            for signal in signals_tuple
        ],
        "guidance_comparison_ready_count": 0,
        "unmatched_reference_records": [
            {
                "raw_record_id": item.raw_record.raw_record_id,
                "raw_label": item.raw_record.raw_label,
                "readiness": GuidanceComparisonReadiness.GUIDANCE_REFERENCE_EXISTS_NOT_NORMALIZED.value,
                "reason": "Guidance ownership is preserved, but canonical driver/definition/period matching is not normalized in registry@1.",
            }
            for item in sorted(
                guidance_rows, key=lambda value: value.raw_record.raw_record_id
            )
        ],
    }
    return DerivedAnalyticsPackage(
        registry_package_sha256=package.sha256,
        ticker=package.profile.ticker,
        latest_states=latest,
        qoq_analytics=qoq_tuple,
        yoy_analytics=yoy_tuple,
        ttm_analytics=ttm_tuple,
        fiscal_year_analytics=fiscal_tuple,
        ttm_change_analytics=ttm_changes,
        trend_analytics=trends,
        acceleration_analytics=accelerations,
        consistency_analytics=consistencies,
        variability_analytics=variabilities,
        analytical_signals=signals_tuple,
        blocked_analytics=blocked,
        guidance_readiness=guidance_readiness,
    )


def combined_analytics_digest(packages: Iterable[DerivedAnalyticsPackage]) -> str:
    payload = serialize_package(
        {
            "contract_version": OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION,
            "package_hashes": {
                package.ticker: package.sha256
                for package in sorted(packages, key=lambda item: item.ticker)
            },
        }
    )
    return hashlib.sha256(payload).hexdigest()


def availability_counts(records: Iterable[Any]) -> dict[str, int]:
    """Return deterministic closed availability counts for audit consumers."""

    counter = Counter(item.availability.value for item in records)
    return {item.value: counter.get(item.value, 0) for item in AnalyticsAvailability}
