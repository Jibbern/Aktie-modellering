"""Typed, fail-closed Operating Drivers longitudinal foundations.

This module is intentionally workbook-independent.  It extends the accepted
longitudinal-memory calendar and serialization contracts with the minimum
period, definition-continuity, evidence, comparison, and duration-aggregation
types needed by the Operating Drivers shadow product.
"""
from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence, TypeAlias

from .calendar_rules import (
    CALENDAR_YEAR_RULE_ID,
    FISCAL_CALENDAR_RULES,
    evaluate_period_compatibility,
    validate_period_for_calendar_rule,
)
from .identity import (
    canonical_company_id,
    dimension_set_identity,
    validate_semantic_id,
)
from .serialization import serialize_package
from .types import DomainValidationError, canonical_decimal


OPERATING_DRIVER_FOUNDATION_CONTRACT_VERSION = (
    "operating-drivers-typed-continuity-foundation@1"
)


class OperatingDriverFoundationError(DomainValidationError):
    """Raised when a typed Operating Drivers record would be ambiguous."""


class PeriodKind(str, Enum):
    FISCAL_QUARTER = "FISCAL_QUARTER"
    FISCAL_YEAR = "FISCAL_YEAR"
    TTM = "TTM"
    INSTANT = "INSTANT"
    DURATION = "DURATION"


class DefinitionContinuityState(str, Enum):
    SAME_SERIES = "SAME_SERIES"
    RESTATED_SAME_SERIES = "RESTATED_SAME_SERIES"
    DEFINITION_CHANGED_BREAK_SERIES = "DEFINITION_CHANGED_BREAK_SERIES"
    UNIT_CONVERSION_SAFE = "UNIT_CONVERSION_SAFE"
    SEGMENT_REORG_BREAK_SERIES = "SEGMENT_REORG_BREAK_SERIES"
    SUCCESSOR_METRIC = "SUCCESSOR_METRIC"
    UNRESOLVED = "UNRESOLVED"


class EvidenceClassification(str, Enum):
    ACTUAL = "ACTUAL"
    GUIDANCE = "GUIDANCE"
    MANAGEMENT_TARGET = "MANAGEMENT_TARGET"
    ESTIMATE = "ESTIMATE"
    UNKNOWN = "UNKNOWN"


class EvidenceSourceType(str, Enum):
    SEC_FILING = "SEC_FILING"
    EARNINGS_RELEASE = "EARNINGS_RELEASE"
    PRESENTATION = "PRESENTATION"
    TRANSCRIPT = "TRANSCRIPT"
    INTERNAL_METRIC = "INTERNAL_METRIC"
    QUARTER_NOTE = "QUARTER_NOTE"
    PROMISE = "PROMISE"
    PROMISE_PROGRESS = "PROMISE_PROGRESS"
    LEGACY_WORKBOOK = "LEGACY_WORKBOOK"
    OTHER = "OTHER"


class EvidenceValueKind(str, Enum):
    NUMERIC = "NUMERIC"
    QUALITATIVE = "QUALITATIVE"


class EvidenceAvailability(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NEEDS_REVIEW = "NEEDS_REVIEW"


class AggregationSemantics(str, Enum):
    SUMMABLE = "SUMMABLE"
    AVERAGE_REQUIRES_CONTRACT = "AVERAGE_REQUIRES_CONTRACT"
    PERIOD_END = "PERIOD_END"
    NON_AGGREGATABLE = "NON_AGGREGATABLE"
    UNKNOWN = "UNKNOWN"


class AggregationMethod(str, Enum):
    SUM = "SUM"


class AggregateCompleteness(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    INCOMPATIBLE = "INCOMPATIBLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AggregateReason(str, Enum):
    COMPLETE = "COMPLETE"
    UNAVAILABLE_INCOMPLETE_PERIOD_SET = "UNAVAILABLE_INCOMPLETE_PERIOD_SET"
    DUPLICATE_CONSTITUENT_PERIOD = "DUPLICATE_CONSTITUENT_PERIOD"
    UNEXPECTED_CONSTITUENT_PERIOD = "UNEXPECTED_CONSTITUENT_PERIOD"
    DEFINITION_INCOMPATIBLE = "DEFINITION_INCOMPATIBLE"
    DIMENSION_INCOMPATIBLE = "DIMENSION_INCOMPATIBLE"
    UNIT_INCOMPATIBLE = "UNIT_INCOMPATIBLE"
    CLASSIFICATION_INCOMPATIBLE = "CLASSIFICATION_INCOMPATIBLE"
    AGGREGATION_SEMANTICS_INVALID = "AGGREGATION_SEMANTICS_INVALID"
    VALUE_UNAVAILABLE = "VALUE_UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class PeriodResolutionState(str, Enum):
    RESOLVED = "RESOLVED"
    PRIOR_PERIOD_MISSING = "PRIOR_PERIOD_MISSING"
    PRIOR_YEAR_PERIOD_MISSING = "PRIOR_YEAR_PERIOD_MISSING"
    DUPLICATE_PERIOD = "DUPLICATE_PERIOD"
    INCOMPATIBLE_PERIOD = "INCOMPATIBLE_PERIOD"


class ComparisonKind(str, Enum):
    QOQ = "QOQ"
    YOY = "YOY"


class ComparisonState(str, Enum):
    COMPLETE = "COMPLETE"
    UNAVAILABLE = "UNAVAILABLE"
    INCOMPATIBLE = "INCOMPATIBLE"


class ComparisonReason(str, Enum):
    COMPLETE = "COMPLETE"
    PRIOR_PERIOD_MISSING = "PRIOR_PERIOD_MISSING"
    PRIOR_YEAR_PERIOD_MISSING = "PRIOR_YEAR_PERIOD_MISSING"
    DUPLICATE_PERIOD = "DUPLICATE_PERIOD"
    VALUE_UNAVAILABLE = "VALUE_UNAVAILABLE"
    DEFINITION_INCOMPATIBLE = "DEFINITION_INCOMPATIBLE"
    DIMENSION_INCOMPATIBLE = "DIMENSION_INCOMPATIBLE"
    UNIT_INCOMPATIBLE = "UNIT_INCOMPATIBLE"
    CLASSIFICATION_INCOMPATIBLE = "CLASSIFICATION_INCOMPATIBLE"


def _require_nonempty(value: Any, field_name: str) -> str:
    text = str(value)
    if not text or text != text.strip():
        raise OperatingDriverFoundationError(
            f"{field_name} must be non-empty and trimmed."
        )
    return text


def _decimal(value: str | int | Decimal) -> Decimal:
    return Decimal(canonical_decimal(value))


def _quarter_identity_tuple(fiscal_year: int, fiscal_quarter: int) -> tuple[int, int]:
    if not isinstance(fiscal_year, int):
        raise OperatingDriverFoundationError("Fiscal year must be an integer.")
    if fiscal_quarter not in {1, 2, 3, 4}:
        raise OperatingDriverFoundationError(
            "Fiscal quarter must be an integer from 1 through 4."
        )
    return fiscal_year, fiscal_quarter


def prior_quarter_key(fiscal_year: int, fiscal_quarter: int) -> tuple[int, int]:
    """Return the exact preceding fiscal-quarter key without using a display label."""

    fiscal_year, fiscal_quarter = _quarter_identity_tuple(
        fiscal_year, fiscal_quarter
    )
    if fiscal_quarter == 1:
        return fiscal_year - 1, 4
    return fiscal_year, fiscal_quarter - 1


def prior_year_quarter_key(
    fiscal_year: int, fiscal_quarter: int
) -> tuple[int, int]:
    """Return the exact same fiscal quarter in the preceding fiscal year."""

    fiscal_year, fiscal_quarter = _quarter_identity_tuple(
        fiscal_year, fiscal_quarter
    )
    return fiscal_year - 1, fiscal_quarter


def ttm_quarter_keys(
    ending_fiscal_year: int, ending_fiscal_quarter: int
) -> tuple[tuple[int, int], ...]:
    """Return the exact four-quarter TTM window, oldest to newest."""

    current = _quarter_identity_tuple(ending_fiscal_year, ending_fiscal_quarter)
    reverse_keys = [current]
    for _ in range(3):
        reverse_keys.append(prior_quarter_key(*reverse_keys[-1]))
    return tuple(reversed(reverse_keys))


@dataclass(frozen=True, slots=True)
class FiscalCalendarIdentity:
    calendar_id: str
    company_id: str
    calendar_rule_id: str
    week_pattern: str
    reconciliation_state: str = "reconciled"

    def __post_init__(self) -> None:
        object.__setattr__(self, "calendar_id", validate_semantic_id(self.calendar_id, prefix="calendar"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        object.__setattr__(self, "calendar_rule_id", validate_semantic_id(self.calendar_rule_id, prefix="rule"))
        if self.calendar_rule_id not in FISCAL_CALENDAR_RULES:
            raise OperatingDriverFoundationError(
                f"Unsupported fiscal-calendar rule {self.calendar_rule_id!r}."
            )
        _require_nonempty(self.week_pattern, "week_pattern")
        if self.reconciliation_state != "reconciled":
            raise OperatingDriverFoundationError(
                "Typed Operating Drivers periods require a reconciled fiscal calendar."
            )

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class FiscalQuarterPeriod:
    period_id: str
    company_id: str
    calendar: FiscalCalendarIdentity
    fiscal_year: int
    fiscal_quarter: int
    fiscal_ordinal: int
    start_date: date
    end_date: date
    week_count: int | None
    is_53_week_year: bool
    reconciliation_state: str = "reconciled"
    period_kind: PeriodKind = field(default=PeriodKind.FISCAL_QUARTER, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "period_id", validate_semantic_id(self.period_id, prefix="period"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        if self.company_id != self.calendar.company_id:
            raise OperatingDriverFoundationError(
                "Fiscal quarter and fiscal calendar must share one company identity."
            )
        _quarter_identity_tuple(self.fiscal_year, self.fiscal_quarter)
        if not isinstance(self.fiscal_ordinal, int):
            raise OperatingDriverFoundationError("Fiscal quarter requires an integer ordinal.")
        validate_period_for_calendar_rule(self.to_dict(), self.calendar.to_dict())

    @property
    def day_count(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def display_label(self) -> str:
        return f"{self.fiscal_year}-Q{self.fiscal_quarter}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "calendar_id": self.calendar.calendar_id,
            "company_id": self.company_id,
            "day_count": self.day_count,
            "end_date": self.end_date.isoformat(),
            "fiscal_ordinal": self.fiscal_ordinal,
            "fiscal_quarter": self.fiscal_quarter,
            "fiscal_year": self.fiscal_year,
            "is_53_week_year": self.is_53_week_year,
            "period_id": self.period_id,
            "period_kind": self.period_kind.value,
            "period_type": "quarter",
            "reconciliation_state": self.reconciliation_state,
            "start_date": self.start_date.isoformat(),
            "week_count": self.week_count,
        }


@dataclass(frozen=True, slots=True)
class FiscalYearPeriod:
    period_id: str
    company_id: str
    calendar: FiscalCalendarIdentity
    fiscal_year: int
    start_date: date
    end_date: date
    week_count: int | None
    is_53_week_year: bool
    reconciliation_state: str = "reconciled"
    period_kind: PeriodKind = field(default=PeriodKind.FISCAL_YEAR, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "period_id", validate_semantic_id(self.period_id, prefix="period"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        if self.company_id != self.calendar.company_id:
            raise OperatingDriverFoundationError(
                "Fiscal year and fiscal calendar must share one company identity."
            )
        if not isinstance(self.fiscal_year, int):
            raise OperatingDriverFoundationError("Fiscal year must be an integer.")
        validate_period_for_calendar_rule(self.to_dict(), self.calendar.to_dict())

    @property
    def day_count(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def display_label(self) -> str:
        return str(self.fiscal_year)

    def to_dict(self) -> dict[str, Any]:
        return {
            "calendar_id": self.calendar.calendar_id,
            "company_id": self.company_id,
            "day_count": self.day_count,
            "end_date": self.end_date.isoformat(),
            "fiscal_quarter": None,
            "fiscal_year": self.fiscal_year,
            "is_53_week_year": self.is_53_week_year,
            "period_id": self.period_id,
            "period_kind": self.period_kind.value,
            "period_type": "annual",
            "reconciliation_state": self.reconciliation_state,
            "start_date": self.start_date.isoformat(),
            "week_count": self.week_count,
        }


def _quarters_form_exact_sequence(
    quarters: Sequence[FiscalQuarterPeriod],
) -> bool:
    if not quarters:
        return False
    for earlier, later in zip(quarters, quarters[1:]):
        if earlier.company_id != later.company_id:
            return False
        if earlier.calendar.calendar_id != later.calendar.calendar_id:
            return False
        if prior_quarter_key(later.fiscal_year, later.fiscal_quarter) != (
            earlier.fiscal_year,
            earlier.fiscal_quarter,
        ):
            return False
        if later.fiscal_ordinal != earlier.fiscal_ordinal + 1:
            return False
        if later.start_date != earlier.end_date + timedelta(days=1):
            return False
    return True


@dataclass(frozen=True, slots=True)
class TrailingTwelveMonthsPeriod:
    period_id: str
    company_id: str
    ending_quarter: FiscalQuarterPeriod
    constituent_quarters: tuple[FiscalQuarterPeriod, ...]
    period_kind: PeriodKind = field(default=PeriodKind.TTM, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "period_id", validate_semantic_id(self.period_id, prefix="period"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        if len(self.constituent_quarters) != 4:
            raise OperatingDriverFoundationError("TTM requires exactly four fiscal quarters.")
        if self.constituent_quarters[-1] != self.ending_quarter:
            raise OperatingDriverFoundationError(
                "TTM ending quarter must be the final constituent quarter."
            )
        if self.company_id != self.ending_quarter.company_id:
            raise OperatingDriverFoundationError(
                "TTM and ending quarter must share one company identity."
            )
        if not _quarters_form_exact_sequence(self.constituent_quarters):
            raise OperatingDriverFoundationError(
                "TTM constituents must be four exact, adjacent fiscal quarters."
            )

    @property
    def start_date(self) -> date:
        return self.constituent_quarters[0].start_date

    @property
    def end_date(self) -> date:
        return self.ending_quarter.end_date

    @property
    def display_label(self) -> str:
        return f"TTM through {self.ending_quarter.display_label}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "constituent_period_ids": [
                quarter.period_id for quarter in self.constituent_quarters
            ],
            "end_date": self.end_date.isoformat(),
            "ending_quarter_id": self.ending_quarter.period_id,
            "period_id": self.period_id,
            "period_kind": self.period_kind.value,
            "start_date": self.start_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class InstantPeriod:
    period_id: str
    company_id: str
    instant_date: date
    fiscal_calendar_id: str | None = None
    period_kind: PeriodKind = field(default=PeriodKind.INSTANT, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "period_id", validate_semantic_id(self.period_id, prefix="period"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        if self.fiscal_calendar_id is not None:
            object.__setattr__(
                self,
                "fiscal_calendar_id",
                validate_semantic_id(self.fiscal_calendar_id, prefix="calendar"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "fiscal_calendar_id": self.fiscal_calendar_id,
            "instant_date": self.instant_date.isoformat(),
            "period_id": self.period_id,
            "period_kind": self.period_kind.value,
        }


@dataclass(frozen=True, slots=True)
class DurationPeriod:
    period_id: str
    company_id: str
    start_date: date
    end_date: date
    fiscal_calendar_id: str | None = None
    period_kind: PeriodKind = field(default=PeriodKind.DURATION, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "period_id", validate_semantic_id(self.period_id, prefix="period"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        if self.start_date > self.end_date:
            raise OperatingDriverFoundationError("Duration boundaries are reversed.")
        if self.fiscal_calendar_id is not None:
            object.__setattr__(
                self,
                "fiscal_calendar_id",
                validate_semantic_id(self.fiscal_calendar_id, prefix="calendar"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "end_date": self.end_date.isoformat(),
            "fiscal_calendar_id": self.fiscal_calendar_id,
            "period_id": self.period_id,
            "period_kind": self.period_kind.value,
            "start_date": self.start_date.isoformat(),
        }


OperatingDriverPeriod: TypeAlias = (
    FiscalQuarterPeriod
    | FiscalYearPeriod
    | TrailingTwelveMonthsPeriod
    | InstantPeriod
    | DurationPeriod
)


@dataclass(frozen=True, slots=True)
class PeriodResolution:
    state: PeriodResolutionState
    expected_fiscal_year: int
    expected_fiscal_quarter: int | None
    period: FiscalQuarterPeriod | FiscalYearPeriod | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "expected_fiscal_quarter": self.expected_fiscal_quarter,
            "expected_fiscal_year": self.expected_fiscal_year,
            "period": self.period.to_dict() if self.period is not None else None,
            "reason": self.reason,
            "state": self.state.value,
        }


def _resolve_quarter(
    current: FiscalQuarterPeriod,
    candidates: Iterable[FiscalQuarterPeriod],
    *,
    comparison_kind: ComparisonKind,
) -> PeriodResolution:
    expected_year, expected_quarter = (
        prior_quarter_key(current.fiscal_year, current.fiscal_quarter)
        if comparison_kind is ComparisonKind.QOQ
        else prior_year_quarter_key(current.fiscal_year, current.fiscal_quarter)
    )
    matches = [
        period
        for period in candidates
        if period.company_id == current.company_id
        and period.calendar.calendar_id == current.calendar.calendar_id
        and period.fiscal_year == expected_year
        and period.fiscal_quarter == expected_quarter
    ]
    missing_state = (
        PeriodResolutionState.PRIOR_PERIOD_MISSING
        if comparison_kind is ComparisonKind.QOQ
        else PeriodResolutionState.PRIOR_YEAR_PERIOD_MISSING
    )
    if not matches:
        return PeriodResolution(
            state=missing_state,
            expected_fiscal_year=expected_year,
            expected_fiscal_quarter=expected_quarter,
            period=None,
            reason=missing_state.value,
        )
    if len(matches) != 1:
        return PeriodResolution(
            state=PeriodResolutionState.DUPLICATE_PERIOD,
            expected_fiscal_year=expected_year,
            expected_fiscal_quarter=expected_quarter,
            period=None,
            reason="Multiple records claim the exact required fiscal period.",
        )
    prior = matches[0]
    compatibility = evaluate_period_compatibility(
        prior.to_dict(),
        current.to_dict(),
        earlier_calendar=prior.calendar.to_dict(),
        later_calendar=current.calendar.to_dict(),
        change_kind=(
            "qoq-percentage-point"
            if comparison_kind is ComparisonKind.QOQ
            else "yoy-percentage-point"
        ),
    )
    if not compatibility["comparable"]:
        return PeriodResolution(
            state=PeriodResolutionState.INCOMPATIBLE_PERIOD,
            expected_fiscal_year=expected_year,
            expected_fiscal_quarter=expected_quarter,
            period=None,
            reason=str(compatibility["reason"]),
        )
    return PeriodResolution(
        state=PeriodResolutionState.RESOLVED,
        expected_fiscal_year=expected_year,
        expected_fiscal_quarter=expected_quarter,
        period=prior,
        reason=str(compatibility["reason"]),
    )


def resolve_exact_prior_quarter(
    current: FiscalQuarterPeriod,
    candidates: Iterable[FiscalQuarterPeriod],
) -> PeriodResolution:
    return _resolve_quarter(current, candidates, comparison_kind=ComparisonKind.QOQ)


def resolve_exact_prior_year_quarter(
    current: FiscalQuarterPeriod,
    candidates: Iterable[FiscalQuarterPeriod],
) -> PeriodResolution:
    return _resolve_quarter(current, candidates, comparison_kind=ComparisonKind.YOY)


def resolve_exact_prior_fiscal_year(
    current: FiscalYearPeriod,
    candidates: Iterable[FiscalYearPeriod],
) -> PeriodResolution:
    expected_year = current.fiscal_year - 1
    matches = [
        period
        for period in candidates
        if period.company_id == current.company_id
        and period.calendar.calendar_id == current.calendar.calendar_id
        and period.fiscal_year == expected_year
    ]
    if not matches:
        return PeriodResolution(
            state=PeriodResolutionState.PRIOR_YEAR_PERIOD_MISSING,
            expected_fiscal_year=expected_year,
            expected_fiscal_quarter=None,
            period=None,
            reason=PeriodResolutionState.PRIOR_YEAR_PERIOD_MISSING.value,
        )
    if len(matches) != 1:
        return PeriodResolution(
            state=PeriodResolutionState.DUPLICATE_PERIOD,
            expected_fiscal_year=expected_year,
            expected_fiscal_quarter=None,
            period=None,
            reason="Multiple records claim the exact required fiscal year.",
        )
    prior = matches[0]
    if prior.end_date >= current.start_date:
        return PeriodResolution(
            state=PeriodResolutionState.INCOMPATIBLE_PERIOD,
            expected_fiscal_year=expected_year,
            expected_fiscal_quarter=None,
            period=None,
            reason="Fiscal-year boundaries overlap or are reversed.",
        )
    return PeriodResolution(
        state=PeriodResolutionState.RESOLVED,
        expected_fiscal_year=expected_year,
        expected_fiscal_quarter=None,
        period=prior,
        reason="Exact prior fiscal year resolved on the same canonical calendar.",
    )


@dataclass(frozen=True, slots=True)
class DriverDimension:
    dimension_id: str
    member_id: str
    label: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimension_id", validate_semantic_id(self.dimension_id, prefix="dimension"))
        object.__setattr__(self, "member_id", validate_semantic_id(self.member_id, prefix="member"))
        _require_nonempty(self.label, "dimension label")

    def to_dict(self) -> dict[str, str]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class DriverIdentity:
    driver_id: str
    company_id: str
    ticker: str
    driver_family: str
    canonical_label: str
    display_label: str
    unit_id: str
    scale: str
    sign_convention: str
    dimensions: tuple[DriverDimension, ...]
    period_kind: PeriodKind
    source_owner: str
    definition_id: str
    definition_version: int
    aggregation_semantics: AggregationSemantics

    def __post_init__(self) -> None:
        object.__setattr__(self, "driver_id", validate_semantic_id(self.driver_id, prefix="driver"))
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        object.__setattr__(self, "ticker", canonical_company_id(self.ticker))
        if self.ticker != self.company_id:
            raise OperatingDriverFoundationError(
                "The bounded foundation requires ticker and company identity to agree."
            )
        _require_nonempty(self.driver_family, "driver_family")
        _require_nonempty(self.canonical_label, "canonical_label")
        _require_nonempty(self.display_label, "display_label")
        object.__setattr__(self, "unit_id", validate_semantic_id(self.unit_id, prefix="unit"))
        object.__setattr__(self, "scale", canonical_decimal(self.scale))
        _require_nonempty(self.sign_convention, "sign_convention")
        if not self.dimensions:
            raise OperatingDriverFoundationError(
                "Driver identity requires an explicit dimension, including total company."
            )
        ordered_dimensions = tuple(
            sorted(
                self.dimensions,
                key=lambda item: (item.dimension_id, item.member_id),
            )
        )
        dimension_set_identity(
            (item.dimension_id, item.member_id) for item in ordered_dimensions
        )
        object.__setattr__(self, "dimensions", ordered_dimensions)
        object.__setattr__(self, "source_owner", validate_semantic_id(self.source_owner, prefix="owner"))
        object.__setattr__(self, "definition_id", validate_semantic_id(self.definition_id, prefix="definition"))
        if not isinstance(self.definition_version, int) or self.definition_version < 1:
            raise OperatingDriverFoundationError(
                "Driver definition version must be a positive integer."
            )

    @property
    def dimension_set_id(self) -> str:
        return dimension_set_identity(
            (item.dimension_id, item.member_id) for item in self.dimensions
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregation_semantics": self.aggregation_semantics.value,
            "canonical_label": self.canonical_label,
            "company_id": self.company_id,
            "definition_id": self.definition_id,
            "definition_version": self.definition_version,
            "dimension_set_id": self.dimension_set_id,
            "dimensions": [item.to_dict() for item in self.dimensions],
            "display_label": self.display_label,
            "driver_family": self.driver_family,
            "driver_id": self.driver_id,
            "period_kind": self.period_kind.value,
            "scale": self.scale,
            "sign_convention": self.sign_convention,
            "source_owner": self.source_owner,
            "ticker": self.ticker,
            "unit_id": self.unit_id,
        }


@dataclass(frozen=True, slots=True)
class UnitConversionReceipt:
    rule_id: str
    from_unit_id: str
    to_unit_id: str
    multiplier: str
    from_scale: str
    to_scale: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", validate_semantic_id(self.rule_id, prefix="rule"))
        object.__setattr__(self, "from_unit_id", validate_semantic_id(self.from_unit_id, prefix="unit"))
        object.__setattr__(self, "to_unit_id", validate_semantic_id(self.to_unit_id, prefix="unit"))
        object.__setattr__(self, "multiplier", canonical_decimal(self.multiplier))
        object.__setattr__(self, "from_scale", canonical_decimal(self.from_scale))
        object.__setattr__(self, "to_scale", canonical_decimal(self.to_scale))
        if _decimal(self.multiplier) <= 0:
            raise OperatingDriverFoundationError(
                "Safe unit conversion requires a positive multiplier."
            )

    def convert(self, value: str | int | Decimal) -> str:
        return canonical_decimal(_decimal(value) * _decimal(self.multiplier))

    def to_dict(self) -> dict[str, str]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class DefinitionContinuity:
    state: DefinitionContinuityState
    from_definition_id: str
    from_definition_version: int
    to_definition_id: str
    to_definition_version: int
    reason: str
    unit_conversion: UnitConversionReceipt | None = None
    restatement_id: str | None = None
    successor_driver_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "from_definition_id", validate_semantic_id(self.from_definition_id, prefix="definition"))
        object.__setattr__(self, "to_definition_id", validate_semantic_id(self.to_definition_id, prefix="definition"))
        if self.from_definition_version < 1 or self.to_definition_version < 1:
            raise OperatingDriverFoundationError(
                "Continuity definition versions must be positive integers."
            )
        _require_nonempty(self.reason, "continuity reason")
        if self.state is DefinitionContinuityState.SAME_SERIES:
            if (
                self.from_definition_id != self.to_definition_id
                or self.from_definition_version != self.to_definition_version
            ):
                raise OperatingDriverFoundationError(
                    "SAME_SERIES requires identical definition identity and version."
                )
        if self.state is DefinitionContinuityState.RESTATED_SAME_SERIES:
            _require_nonempty(self.restatement_id, "restatement_id")
        if self.state is DefinitionContinuityState.UNIT_CONVERSION_SAFE:
            if self.unit_conversion is None:
                raise OperatingDriverFoundationError(
                    "UNIT_CONVERSION_SAFE requires a typed conversion receipt."
                )
        elif self.unit_conversion is not None:
            raise OperatingDriverFoundationError(
                "Unit conversion receipts belong only to UNIT_CONVERSION_SAFE transitions."
            )
        if self.state is DefinitionContinuityState.SUCCESSOR_METRIC:
            if self.successor_driver_id is None:
                raise OperatingDriverFoundationError(
                    "SUCCESSOR_METRIC requires an explicit successor driver identity."
                )
            object.__setattr__(
                self,
                "successor_driver_id",
                validate_semantic_id(self.successor_driver_id, prefix="driver"),
            )

    @property
    def automatic_join_safe(self) -> bool:
        return self.state in {
            DefinitionContinuityState.SAME_SERIES,
            DefinitionContinuityState.RESTATED_SAME_SERIES,
            DefinitionContinuityState.UNIT_CONVERSION_SAFE,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_definition_id": self.from_definition_id,
            "from_definition_version": self.from_definition_version,
            "reason": self.reason,
            "restatement_id": self.restatement_id,
            "state": self.state.value,
            "successor_driver_id": self.successor_driver_id,
            "to_definition_id": self.to_definition_id,
            "to_definition_version": self.to_definition_version,
            "unit_conversion": (
                self.unit_conversion.to_dict()
                if self.unit_conversion is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class EvidenceSourceReference:
    source_document_id: str
    source_type: EvidenceSourceType
    source_location: str
    publication_date: date | None
    knowledge_date: date | None

    def __post_init__(self) -> None:
        _require_nonempty(self.source_document_id, "source_document_id")
        _require_nonempty(self.source_location, "source_location")
        if (
            self.publication_date is not None
            and self.knowledge_date is not None
            and self.knowledge_date < self.publication_date
        ):
            raise OperatingDriverFoundationError(
                "Knowledge date cannot precede publication date."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "knowledge_date": (
                self.knowledge_date.isoformat()
                if self.knowledge_date is not None
                else None
            ),
            "publication_date": (
                self.publication_date.isoformat()
                if self.publication_date is not None
                else None
            ),
            "source_document_id": self.source_document_id,
            "source_location": self.source_location,
            "source_type": self.source_type.value,
        }


@dataclass(frozen=True, slots=True)
class EvidenceTransformation:
    method_id: str
    description: str
    input_record_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "method_id", validate_semantic_id(self.method_id, prefix="method"))
        _require_nonempty(self.description, "transformation description")
        object.__setattr__(
            self,
            "input_record_ids",
            tuple(sorted({_require_nonempty(item, "input_record_id") for item in self.input_record_ids})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "description": self.description,
            "input_record_ids": list(self.input_record_ids),
            "method_id": self.method_id,
        }


@dataclass(frozen=True, slots=True)
class OperatingDriverEvidence:
    evidence_id: str
    driver: DriverIdentity
    period: OperatingDriverPeriod
    source: EvidenceSourceReference
    value_kind: EvidenceValueKind
    raw_value: str | None
    normalized_value: str | None
    source_unit_id: str
    classification: EvidenceClassification
    availability: EvidenceAvailability
    unavailable_reason: str | None
    continuity: DefinitionContinuity
    transformations: tuple[EvidenceTransformation, ...] = ()

    def __post_init__(self) -> None:
        _require_nonempty(self.evidence_id, "evidence_id")
        if self.period.company_id != self.driver.company_id:
            raise OperatingDriverFoundationError(
                "Evidence period and driver must share one company identity."
            )
        object.__setattr__(self, "source_unit_id", validate_semantic_id(self.source_unit_id, prefix="unit"))
        if self.continuity.to_definition_id != self.driver.definition_id:
            raise OperatingDriverFoundationError(
                "Evidence continuity must terminate in the driver definition."
            )
        if self.continuity.to_definition_version != self.driver.definition_version:
            raise OperatingDriverFoundationError(
                "Evidence continuity must terminate in the driver definition version."
            )
        if self.value_kind is EvidenceValueKind.NUMERIC:
            if self.raw_value is not None:
                object.__setattr__(self, "raw_value", canonical_decimal(self.raw_value))
            if self.normalized_value is not None:
                object.__setattr__(
                    self, "normalized_value", canonical_decimal(self.normalized_value)
                )
            if self.availability is EvidenceAvailability.AVAILABLE:
                if self.raw_value is None or self.normalized_value is None:
                    raise OperatingDriverFoundationError(
                        "Available numeric evidence requires raw and normalized values."
                    )
        else:
            if self.raw_value is not None:
                _require_nonempty(self.raw_value, "qualitative raw_value")
            if self.normalized_value is not None:
                raise OperatingDriverFoundationError(
                    "Qualitative evidence cannot carry a numeric normalized value."
                )
            if (
                self.availability is EvidenceAvailability.AVAILABLE
                and self.raw_value is None
            ):
                raise OperatingDriverFoundationError(
                    "Available qualitative evidence requires source text."
                )
        if self.availability is EvidenceAvailability.AVAILABLE:
            if self.unavailable_reason is not None:
                raise OperatingDriverFoundationError(
                    "Available evidence cannot carry an unavailable reason."
                )
        else:
            _require_nonempty(self.unavailable_reason, "unavailable_reason")
            if self.normalized_value is not None:
                raise OperatingDriverFoundationError(
                    "Unavailable evidence cannot carry a normalized numeric value."
                )
        if self.source_unit_id != self.driver.unit_id:
            receipt = self.continuity.unit_conversion
            if (
                self.continuity.state
                is not DefinitionContinuityState.UNIT_CONVERSION_SAFE
                or receipt is None
                or receipt.from_unit_id != self.source_unit_id
                or receipt.to_unit_id != self.driver.unit_id
            ):
                raise OperatingDriverFoundationError(
                    "Different source/driver units require a matching safe conversion receipt."
                )
            if self.raw_value is not None and self.normalized_value is not None:
                if receipt.convert(self.raw_value) != self.normalized_value:
                    raise OperatingDriverFoundationError(
                        "Normalized value does not match the typed unit conversion receipt."
                    )

    @property
    def source_backed_zero(self) -> bool:
        return (
            self.value_kind is EvidenceValueKind.NUMERIC
            and self.availability is EvidenceAvailability.AVAILABLE
            and self.normalized_value is not None
            and _decimal(self.normalized_value) == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "availability": self.availability.value,
            "classification": self.classification.value,
            "continuity": self.continuity.to_dict(),
            "driver": self.driver.to_dict(),
            "evidence_id": self.evidence_id,
            "normalized_value": self.normalized_value,
            "period": self.period.to_dict(),
            "raw_value": self.raw_value,
            "source": self.source.to_dict(),
            "source_unit_id": self.source_unit_id,
            "transformations": [item.to_dict() for item in self.transformations],
            "unavailable_reason": self.unavailable_reason,
            "value_kind": self.value_kind.value,
        }


@dataclass(frozen=True, slots=True)
class DurationAggregateRequest:
    request_id: str
    driver: DriverIdentity
    requested_period: FiscalYearPeriod | TrailingTwelveMonthsPeriod | DurationPeriod
    required_constituent_quarters: tuple[FiscalQuarterPeriod, ...]
    aggregation_method: AggregationMethod = AggregationMethod.SUM
    allowed_classifications: tuple[EvidenceClassification, ...] = (
        EvidenceClassification.ACTUAL,
    )

    def __post_init__(self) -> None:
        _require_nonempty(self.request_id, "request_id")
        if not self.required_constituent_quarters:
            raise OperatingDriverFoundationError(
                "Duration aggregate requires explicit constituent fiscal quarters."
            )
        if not _quarters_form_exact_sequence(self.required_constituent_quarters):
            raise OperatingDriverFoundationError(
                "Aggregate constituents must be an exact adjacent fiscal-quarter sequence."
            )
        if any(
            quarter.company_id != self.driver.company_id
            for quarter in self.required_constituent_quarters
        ):
            raise OperatingDriverFoundationError(
                "Aggregate constituents and driver must share one company identity."
            )
        if isinstance(self.requested_period, TrailingTwelveMonthsPeriod):
            if self.required_constituent_quarters != self.requested_period.constituent_quarters:
                raise OperatingDriverFoundationError(
                    "TTM request constituents must equal the typed TTM period."
                )
        if isinstance(self.requested_period, FiscalYearPeriod):
            expected = tuple((self.requested_period.fiscal_year, quarter) for quarter in range(1, 5))
            actual = tuple(
                (period.fiscal_year, period.fiscal_quarter)
                for period in self.required_constituent_quarters
            )
            if actual != expected:
                raise OperatingDriverFoundationError(
                    "Fiscal-year SUM requires exact Q1 through Q4 constituents."
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregation_method": self.aggregation_method.value,
            "allowed_classifications": [item.value for item in self.allowed_classifications],
            "driver_id": self.driver.driver_id,
            "request_id": self.request_id,
            "requested_period": self.requested_period.to_dict(),
            "required_constituent_period_ids": [
                period.period_id for period in self.required_constituent_quarters
            ],
        }


@dataclass(frozen=True, slots=True)
class DurationAggregateResult:
    request_id: str
    completeness: AggregateCompleteness
    reason: AggregateReason
    result_available: bool
    value: str | None
    aggregation_method: AggregationMethod
    required_constituent_period_ids: tuple[str, ...]
    observed_constituent_period_ids: tuple[str, ...]
    missing_constituent_period_ids: tuple[str, ...]
    duplicate_constituent_period_ids: tuple[str, ...]
    unexpected_constituent_period_ids: tuple[str, ...]
    evidence_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregation_method": self.aggregation_method.value,
            "completeness": self.completeness.value,
            "duplicate_constituent_period_ids": list(self.duplicate_constituent_period_ids),
            "evidence_ids": list(self.evidence_ids),
            "missing_constituent_period_ids": list(self.missing_constituent_period_ids),
            "observed_constituent_period_ids": list(self.observed_constituent_period_ids),
            "reason": self.reason.value,
            "request_id": self.request_id,
            "required_constituent_period_ids": list(self.required_constituent_period_ids),
            "result_available": self.result_available,
            "unexpected_constituent_period_ids": list(self.unexpected_constituent_period_ids),
            "value": self.value,
        }


def _aggregate_result(
    request: DurationAggregateRequest,
    *,
    completeness: AggregateCompleteness,
    reason: AggregateReason,
    value: Decimal | None = None,
    observed: Iterable[str] = (),
    missing: Iterable[str] = (),
    duplicates: Iterable[str] = (),
    unexpected: Iterable[str] = (),
    evidence_ids: Iterable[str] = (),
) -> DurationAggregateResult:
    return DurationAggregateResult(
        request_id=request.request_id,
        completeness=completeness,
        reason=reason,
        result_available=(completeness is AggregateCompleteness.COMPLETE),
        value=canonical_decimal(value) if value is not None else None,
        aggregation_method=request.aggregation_method,
        required_constituent_period_ids=tuple(
            period.period_id for period in request.required_constituent_quarters
        ),
        observed_constituent_period_ids=tuple(sorted(set(observed))),
        missing_constituent_period_ids=tuple(sorted(set(missing))),
        duplicate_constituent_period_ids=tuple(sorted(set(duplicates))),
        unexpected_constituent_period_ids=tuple(sorted(set(unexpected))),
        evidence_ids=tuple(sorted(set(evidence_ids))),
    )


def aggregate_duration_fail_closed(
    request: DurationAggregateRequest,
    observations: Iterable[OperatingDriverEvidence],
) -> DurationAggregateResult:
    """Aggregate only an exact, compatible, complete constituent period set."""

    observations = tuple(observations)
    if (
        request.aggregation_method is not AggregationMethod.SUM
        or request.driver.aggregation_semantics is not AggregationSemantics.SUMMABLE
    ):
        return _aggregate_result(
            request,
            completeness=AggregateCompleteness.INCOMPATIBLE,
            reason=AggregateReason.AGGREGATION_SEMANTICS_INVALID,
        )

    required_ids = {
        period.period_id for period in request.required_constituent_quarters
    }
    by_period: dict[str, list[OperatingDriverEvidence]] = {}
    unexpected: list[str] = []
    for observation in observations:
        if not isinstance(observation.period, FiscalQuarterPeriod):
            unexpected.append(observation.period.period_id)
            continue
        if observation.period.period_id not in required_ids:
            unexpected.append(observation.period.period_id)
            continue
        by_period.setdefault(observation.period.period_id, []).append(observation)
    if unexpected:
        return _aggregate_result(
            request,
            completeness=AggregateCompleteness.INCOMPATIBLE,
            reason=AggregateReason.UNEXPECTED_CONSTITUENT_PERIOD,
            unexpected=unexpected,
        )

    duplicates = [period_id for period_id, rows in by_period.items() if len(rows) != 1]
    if duplicates:
        return _aggregate_result(
            request,
            completeness=AggregateCompleteness.INCOMPATIBLE,
            reason=AggregateReason.DUPLICATE_CONSTITUENT_PERIOD,
            duplicates=duplicates,
        )

    missing: list[str] = []
    observed: list[str] = []
    evidence_ids: list[str] = []
    values: list[Decimal] = []
    for required_period in request.required_constituent_quarters:
        rows = by_period.get(required_period.period_id, [])
        if not rows:
            missing.append(required_period.period_id)
            continue
        observation = rows[0]
        if observation.driver.driver_id != request.driver.driver_id:
            return _aggregate_result(
                request,
                completeness=AggregateCompleteness.INCOMPATIBLE,
                reason=AggregateReason.DEFINITION_INCOMPATIBLE,
            )
        if (
            observation.driver.definition_id != request.driver.definition_id
            or observation.driver.definition_version != request.driver.definition_version
            or not observation.continuity.automatic_join_safe
        ):
            reason = (
                AggregateReason.DIMENSION_INCOMPATIBLE
                if observation.continuity.state
                is DefinitionContinuityState.SEGMENT_REORG_BREAK_SERIES
                else AggregateReason.DEFINITION_INCOMPATIBLE
            )
            return _aggregate_result(
                request,
                completeness=AggregateCompleteness.INCOMPATIBLE,
                reason=reason,
            )
        if observation.driver.dimension_set_id != request.driver.dimension_set_id:
            return _aggregate_result(
                request,
                completeness=AggregateCompleteness.INCOMPATIBLE,
                reason=AggregateReason.DIMENSION_INCOMPATIBLE,
            )
        if observation.driver.unit_id != request.driver.unit_id:
            return _aggregate_result(
                request,
                completeness=AggregateCompleteness.INCOMPATIBLE,
                reason=AggregateReason.UNIT_INCOMPATIBLE,
            )
        if observation.classification not in request.allowed_classifications:
            return _aggregate_result(
                request,
                completeness=AggregateCompleteness.INCOMPATIBLE,
                reason=AggregateReason.CLASSIFICATION_INCOMPATIBLE,
            )
        if (
            observation.availability is not EvidenceAvailability.AVAILABLE
            or observation.value_kind is not EvidenceValueKind.NUMERIC
            or observation.normalized_value is None
        ):
            missing.append(required_period.period_id)
            continue
        observed.append(required_period.period_id)
        evidence_ids.append(observation.evidence_id)
        values.append(_decimal(observation.normalized_value))

    if missing:
        return _aggregate_result(
            request,
            completeness=AggregateCompleteness.INCOMPLETE,
            reason=AggregateReason.UNAVAILABLE_INCOMPLETE_PERIOD_SET,
            observed=observed,
            missing=missing,
            evidence_ids=evidence_ids,
        )
    return _aggregate_result(
        request,
        completeness=AggregateCompleteness.COMPLETE,
        reason=AggregateReason.COMPLETE,
        value=sum(values, Decimal("0")),
        observed=observed,
        evidence_ids=evidence_ids,
    )


@dataclass(frozen=True, slots=True)
class SafeComparisonResult:
    comparison_kind: ComparisonKind
    state: ComparisonState
    reason: ComparisonReason
    current_period_id: str
    prior_period_id: str | None
    absolute_change: str | None
    percent_change: str | None
    percent_change_reason: str | None
    evidence_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "absolute_change": self.absolute_change,
            "comparison_kind": self.comparison_kind.value,
            "current_period_id": self.current_period_id,
            "evidence_ids": list(self.evidence_ids),
            "percent_change": self.percent_change,
            "percent_change_reason": self.percent_change_reason,
            "prior_period_id": self.prior_period_id,
            "reason": self.reason.value,
            "state": self.state.value,
        }


def _comparison_failure(
    current: OperatingDriverEvidence,
    kind: ComparisonKind,
    state: ComparisonState,
    reason: ComparisonReason,
    *,
    prior_period_id: str | None = None,
) -> SafeComparisonResult:
    return SafeComparisonResult(
        comparison_kind=kind,
        state=state,
        reason=reason,
        current_period_id=current.period.period_id,
        prior_period_id=prior_period_id,
        absolute_change=None,
        percent_change=None,
        percent_change_reason=None,
        evidence_ids=(current.evidence_id,),
    )


def _safe_comparison(
    current: OperatingDriverEvidence,
    observations: Iterable[OperatingDriverEvidence],
    *,
    kind: ComparisonKind,
    allowed_classifications: tuple[EvidenceClassification, ...],
) -> SafeComparisonResult:
    if not isinstance(current.period, FiscalQuarterPeriod):
        return _comparison_failure(
            current,
            kind,
            ComparisonState.INCOMPATIBLE,
            ComparisonReason.DEFINITION_INCOMPATIBLE,
        )
    observations = tuple(observations)
    quarter_periods = tuple(
        observation.period
        for observation in observations
        if isinstance(observation.period, FiscalQuarterPeriod)
    )
    resolution = (
        resolve_exact_prior_quarter(current.period, quarter_periods)
        if kind is ComparisonKind.QOQ
        else resolve_exact_prior_year_quarter(current.period, quarter_periods)
    )
    if resolution.state is not PeriodResolutionState.RESOLVED:
        reason_by_state = {
            PeriodResolutionState.PRIOR_PERIOD_MISSING: ComparisonReason.PRIOR_PERIOD_MISSING,
            PeriodResolutionState.PRIOR_YEAR_PERIOD_MISSING: ComparisonReason.PRIOR_YEAR_PERIOD_MISSING,
            PeriodResolutionState.DUPLICATE_PERIOD: ComparisonReason.DUPLICATE_PERIOD,
            PeriodResolutionState.INCOMPATIBLE_PERIOD: ComparisonReason.DEFINITION_INCOMPATIBLE,
        }
        return _comparison_failure(
            current,
            kind,
            (
                ComparisonState.UNAVAILABLE
                if resolution.state
                in {
                    PeriodResolutionState.PRIOR_PERIOD_MISSING,
                    PeriodResolutionState.PRIOR_YEAR_PERIOD_MISSING,
                }
                else ComparisonState.INCOMPATIBLE
            ),
            reason_by_state[resolution.state],
        )
    assert resolution.period is not None
    prior_rows = [
        observation
        for observation in observations
        if observation.period.period_id == resolution.period.period_id
    ]
    if len(prior_rows) != 1:
        return _comparison_failure(
            current,
            kind,
            ComparisonState.INCOMPATIBLE,
            ComparisonReason.DUPLICATE_PERIOD,
            prior_period_id=resolution.period.period_id,
        )
    prior = prior_rows[0]
    if (
        current.availability is not EvidenceAvailability.AVAILABLE
        or prior.availability is not EvidenceAvailability.AVAILABLE
        or current.normalized_value is None
        or prior.normalized_value is None
        or current.value_kind is not EvidenceValueKind.NUMERIC
        or prior.value_kind is not EvidenceValueKind.NUMERIC
    ):
        return _comparison_failure(
            current,
            kind,
            ComparisonState.UNAVAILABLE,
            ComparisonReason.VALUE_UNAVAILABLE,
            prior_period_id=prior.period.period_id,
        )
    if (
        current.classification not in allowed_classifications
        or prior.classification not in allowed_classifications
    ):
        return _comparison_failure(
            current,
            kind,
            ComparisonState.INCOMPATIBLE,
            ComparisonReason.CLASSIFICATION_INCOMPATIBLE,
            prior_period_id=prior.period.period_id,
        )
    if (
        current.driver.driver_id != prior.driver.driver_id
        or current.driver.definition_id != prior.driver.definition_id
        or current.driver.definition_version != prior.driver.definition_version
        or not current.continuity.automatic_join_safe
    ):
        return _comparison_failure(
            current,
            kind,
            ComparisonState.INCOMPATIBLE,
            ComparisonReason.DEFINITION_INCOMPATIBLE,
            prior_period_id=prior.period.period_id,
        )
    if current.driver.dimension_set_id != prior.driver.dimension_set_id:
        return _comparison_failure(
            current,
            kind,
            ComparisonState.INCOMPATIBLE,
            ComparisonReason.DIMENSION_INCOMPATIBLE,
            prior_period_id=prior.period.period_id,
        )
    if current.driver.unit_id != prior.driver.unit_id:
        return _comparison_failure(
            current,
            kind,
            ComparisonState.INCOMPATIBLE,
            ComparisonReason.UNIT_INCOMPATIBLE,
            prior_period_id=prior.period.period_id,
        )
    current_value = _decimal(current.normalized_value)
    prior_value = _decimal(prior.normalized_value)
    absolute = current_value - prior_value
    percent_change = None
    percent_change_reason = None
    if prior_value == 0:
        percent_change_reason = "ZERO_BASE_PERCENT_CHANGE_UNAVAILABLE"
    else:
        percent_change = canonical_decimal(absolute / abs(prior_value))
    return SafeComparisonResult(
        comparison_kind=kind,
        state=ComparisonState.COMPLETE,
        reason=ComparisonReason.COMPLETE,
        current_period_id=current.period.period_id,
        prior_period_id=prior.period.period_id,
        absolute_change=canonical_decimal(absolute),
        percent_change=percent_change,
        percent_change_reason=percent_change_reason,
        evidence_ids=tuple(sorted({current.evidence_id, prior.evidence_id})),
    )


def safe_qoq(
    current: OperatingDriverEvidence,
    observations: Iterable[OperatingDriverEvidence],
    *,
    allowed_classifications: tuple[EvidenceClassification, ...] = (
        EvidenceClassification.ACTUAL,
    ),
) -> SafeComparisonResult:
    return _safe_comparison(
        current,
        observations,
        kind=ComparisonKind.QOQ,
        allowed_classifications=allowed_classifications,
    )


def safe_yoy(
    current: OperatingDriverEvidence,
    observations: Iterable[OperatingDriverEvidence],
    *,
    allowed_classifications: tuple[EvidenceClassification, ...] = (
        EvidenceClassification.ACTUAL,
    ),
) -> SafeComparisonResult:
    return _safe_comparison(
        current,
        observations,
        kind=ComparisonKind.YOY,
        allowed_classifications=allowed_classifications,
    )


def serialize_foundation_record(record: Any) -> bytes:
    """Serialize a typed foundation record under one deterministic contract."""

    if not hasattr(record, "to_dict"):
        raise OperatingDriverFoundationError(
            "Foundation serialization requires an explicit to_dict contract."
        )
    return serialize_package(
        {
            "contract_version": OPERATING_DRIVER_FOUNDATION_CONTRACT_VERSION,
            "record": record.to_dict(),
        }
    )


def foundation_record_sha256(record: Any) -> str:
    return hashlib.sha256(serialize_foundation_record(record)).hexdigest()


def calendar_year_quarter_period(
    *,
    company_id: str,
    calendar: FiscalCalendarIdentity,
    fiscal_year: int,
    fiscal_quarter: int,
    period_id: str,
) -> FiscalQuarterPeriod:
    """Construct one exact calendar-year quarter for a reconciled calendar."""

    if calendar.calendar_rule_id != CALENDAR_YEAR_RULE_ID:
        raise OperatingDriverFoundationError(
            "calendar_year_quarter_period requires the calendar-year rule."
        )
    _quarter_identity_tuple(fiscal_year, fiscal_quarter)
    start_month = (fiscal_quarter - 1) * 3 + 1
    start = date(fiscal_year, start_month, 1)
    if fiscal_quarter == 4:
        end = date(fiscal_year, 12, 31)
    else:
        end = date(fiscal_year, start_month + 3, 1) - timedelta(days=1)
    return FiscalQuarterPeriod(
        period_id=period_id,
        company_id=company_id,
        calendar=calendar,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        fiscal_ordinal=fiscal_year * 4 + fiscal_quarter - 1,
        start_date=start,
        end_date=end,
        week_count=None,
        is_53_week_year=False,
    )


def calendar_year_fiscal_year_period(
    *,
    company_id: str,
    calendar: FiscalCalendarIdentity,
    fiscal_year: int,
    period_id: str,
) -> FiscalYearPeriod:
    """Construct one exact calendar-year fiscal year."""

    if calendar.calendar_rule_id != CALENDAR_YEAR_RULE_ID:
        raise OperatingDriverFoundationError(
            "calendar_year_fiscal_year_period requires the calendar-year rule."
        )
    return FiscalYearPeriod(
        period_id=period_id,
        company_id=company_id,
        calendar=calendar,
        fiscal_year=fiscal_year,
        start_date=date(fiscal_year, 1, 1),
        end_date=date(fiscal_year, 12, 31),
        week_count=None,
        is_53_week_year=False,
    )
