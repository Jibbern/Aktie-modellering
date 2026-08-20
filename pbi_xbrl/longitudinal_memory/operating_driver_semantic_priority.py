"""Context-aware Operating Driver semantics and explainable prioritization.

This module is deliberately downstream of the accepted canonical shadow registry
and derived longitudinal analytics.  It never owns historical facts, never
changes mathematical analytics, never emits forecast numbers, and contains no
workbook coordinates.  Economic interpretation is resolved through the explicit
declarative hierarchy shared rules -> sector pack -> ticker profile.
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from .operating_driver_derived_analytics import (
    AccelerationState,
    AnalyticsAvailability,
    DerivedAnalyticsPackage,
    TrendState,
)
from .operating_driver_shadow_registry import (
    CanonicalDriverDefinition,
    ShadowRegistryPackage,
    VisibilityTier,
)
from .serialization import serialize_package


OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION = (
    "operating-drivers-context-semantic-priority@1"
)
OPERATING_DRIVER_SEMANTIC_TREND_CONTRACT_VERSION = (
    "operating-drivers-semantic-trend@1"
)
OPERATING_DRIVER_CONTEXT_RELATIONSHIP_CONTRACT_VERSION = (
    "operating-drivers-context-relationship@1"
)
OPERATING_DRIVER_DRIVER_PRIORITY_CONTRACT_VERSION = (
    "operating-drivers-investor-priority@1"
)
OPERATING_DRIVER_NEW_TICKER_PROFILE_CONTRACT_VERSION = (
    "operating-drivers-new-ticker-profile-readiness@1"
)


class SemanticPriorityError(ValueError):
    """Raised when a semantic rule would weaken an accepted boundary."""


class SemanticMode(str, Enum):
    HIGHER_BETTER = "HIGHER_BETTER"
    LOWER_BETTER = "LOWER_BETTER"
    TARGET_RANGE = "TARGET_RANGE"
    CONTEXT_DEPENDENT = "CONTEXT_DEPENDENT"
    DIRECTION_ONLY = "DIRECTION_ONLY"
    NO_GOOD_BAD_SEMANTICS = "NO_GOOD_BAD_SEMANTICS"


class SemanticAuthority(str, Enum):
    SOURCE_DEFINED = "SOURCE_DEFINED"
    ACCOUNTING_IDENTITY = "ACCOUNTING_IDENTITY"
    SECTOR_PACK_RULE = "SECTOR_PACK_RULE"
    TICKER_PROFILE_RULE = "TICKER_PROFILE_RULE"
    ANALYST_CURATED = "ANALYST_CURATED"
    UNRESOLVED = "UNRESOLVED"


class EconomicInterpretation(str, Enum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"
    MIXED = "MIXED"
    CONTEXT_DEPENDENT = "CONTEXT_DEPENDENT"
    WITHIN_TARGET = "WITHIN_TARGET"
    OUTSIDE_TARGET_HIGH = "OUTSIDE_TARGET_HIGH"
    OUTSIDE_TARGET_LOW = "OUTSIDE_TARGET_LOW"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    NOT_INTERPRETABLE = "NOT_INTERPRETABLE"


class MathematicalDirection(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    UNCHANGED = "UNCHANGED"
    MIXED = "MIXED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ContextRelationshipType(str, Enum):
    PREREQUISITE = "PREREQUISITE"
    TRADEOFF = "TRADEOFF"
    CONFIRMING = "CONFIRMING"
    CONSTRAINT = "CONSTRAINT"
    DENOMINATOR_CONTEXT = "DENOMINATOR_CONTEXT"
    DIVERGENCE = "DIVERGENCE"
    REGIME_MODIFIER = "REGIME_MODIFIER"
    LEADING_LAGGING_CONTEXT = "LEADING_LAGGING_CONTEXT"


class ContextEffectState(str, Enum):
    CONFIRMED = "CONFIRMED"
    ATTENUATED = "ATTENUATED"
    MIXED = "MIXED"
    CONTEXT_REQUIRED = "CONTEXT_REQUIRED"
    OVERRIDE_TO_NOT_INTERPRETABLE = "OVERRIDE_TO_NOT_INTERPRETABLE"
    NO_EFFECT = "NO_EFFECT"
    NOT_EVALUABLE = "NOT_EVALUABLE"


class ContextConditionKind(str, Enum):
    PREDICATE_SET = "PREDICATE_SET"
    AGGREGATE_COMPONENT_SIGN_DIVERGENCE = (
        "AGGREGATE_COMPONENT_SIGN_DIVERGENCE"
    )


class PredicateField(str, Enum):
    MATHEMATICAL_DIRECTION = "MATHEMATICAL_DIRECTION"
    BASE_INTERPRETATION = "BASE_INTERPRETATION"
    SEMANTIC_MODE = "SEMANTIC_MODE"
    LATEST_VALUE_SIGN = "LATEST_VALUE_SIGN"
    NUMERIC_AVAILABLE = "NUMERIC_AVAILABLE"
    ACCELERATION_STATE = "ACCELERATION_STATE"


class PredicateMatch(str, Enum):
    ANY = "ANY"
    ALL = "ALL"


class RuleMatchKind(str, Enum):
    DRIVER_ID = "DRIVER_ID"
    DRIVER_FAMILY = "DRIVER_FAMILY"
    UNIT_ID = "UNIT_ID"
    FALLBACK = "FALLBACK"


class RuleLayer(str, Enum):
    SHARED = "SHARED"
    SECTOR_PACK = "SECTOR_PACK"
    TICKER_PROFILE = "TICKER_PROFILE"


class EconomicGroup(str, Enum):
    DEMAND_VOLUME = "DEMAND_VOLUME"
    PRICE_MIX = "PRICE_MIX"
    CAPACITY_UTILIZATION = "CAPACITY_UTILIZATION"
    COSTS_UNIT_ECONOMICS = "COSTS_UNIT_ECONOMICS"
    LEADING_INDICATORS = "LEADING_INDICATORS"
    OTHER_MATERIAL_DRIVER = "OTHER_MATERIAL_DRIVER"


class OrdinalRating(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ColoringReadiness(str, Enum):
    SAFE_FOR_POSITIVE_NEGATIVE_FILL = "SAFE_FOR_POSITIVE_NEGATIVE_FILL"
    SAFE_FOR_DIRECTION_ONLY = "SAFE_FOR_DIRECTION_ONLY"
    SAFE_FOR_TARGET_RANGE_DISPLAY = "SAFE_FOR_TARGET_RANGE_DISPLAY"
    NEUTRAL_ONLY = "NEUTRAL_ONLY"
    NOT_READY = "NOT_READY"


class ForecastSemanticReadiness(str, Enum):
    FORECAST_EVIDENCE_READY = "FORECAST_EVIDENCE_READY"
    FORECAST_CONTEXT_READY = "FORECAST_CONTEXT_READY"
    NEEDS_CONTEXT = "NEEDS_CONTEXT"
    NEEDS_RELATIONSHIP_REVIEW = "NEEDS_RELATIONSHIP_REVIEW"
    NEEDS_DATA = "NEEDS_DATA"
    NOT_FORECAST_RELEVANT = "NOT_FORECAST_RELEVANT"


class ProfileDriverReadinessState(str, Enum):
    PROFILE_READY = "PROFILE_READY"
    PROFILE_READY_WITH_NEUTRAL_SEMANTICS = (
        "PROFILE_READY_WITH_NEUTRAL_SEMANTICS"
    )
    PROFILE_NEEDS_REVIEW = "PROFILE_NEEDS_REVIEW"
    PROFILE_BLOCKED = "PROFILE_BLOCKED"


class ProfileProductReadiness(str, Enum):
    OPERATING_DRIVERS_PROFILE_READY = "OPERATING_DRIVERS_PROFILE_READY"
    OPERATING_DRIVERS_PROFILE_REVIEW_REQUIRED = (
        "OPERATING_DRIVERS_PROFILE_REVIEW_REQUIRED"
    )


def _hash_id(prefix: str, *parts: object) -> str:
    material = "|".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}:{hashlib.sha256(material).hexdigest()[:24]}"


def _decimal(value: str | int | Decimal) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SemanticPriorityError(f"Invalid decimal value {value!r}.") from exc


def _dimension_members(dimensions: Iterable[Mapping[str, Any]]) -> tuple[str, ...]:
    values: list[str] = []
    for item in dimensions:
        member = item.get("member_id") or item.get("member")
        if member is not None:
            values.append(str(member))
    return tuple(sorted(values))


def _value_sign(value: str | None) -> str:
    if value is None:
        return "UNAVAILABLE"
    number = _decimal(value)
    if number > 0:
        return "POSITIVE"
    if number < 0:
        return "NEGATIVE"
    return "ZERO"


@dataclass(frozen=True, slots=True)
class TargetRange:
    lower_bound: str
    upper_bound: str
    unit_id: str
    scope: str
    valid_from: str | None = None
    valid_to: str | None = None
    authority_reference: str = ""

    def __post_init__(self) -> None:
        lower = _decimal(self.lower_bound)
        upper = _decimal(self.upper_bound)
        if lower > upper:
            raise SemanticPriorityError("Target-range lower bound exceeds upper bound.")
        if not self.unit_id or not self.scope or not self.authority_reference:
            raise SemanticPriorityError(
                "Target ranges require unit, scope, and accepted authority."
            )

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class SemanticRule:
    rule_id: str
    match_kind: RuleMatchKind
    match_value: str
    semantic_mode: SemanticMode
    authority: SemanticAuthority
    reason: str
    layer: RuleLayer
    target_range: TargetRange | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    regime_id: str | None = None
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.rule_id or not self.reason:
            raise SemanticPriorityError("Semantic rules require identity and reason.")
        if self.match_kind is RuleMatchKind.FALLBACK and self.match_value != "*":
            raise SemanticPriorityError("Fallback semantic rules must match '*'.")
        if self.semantic_mode is SemanticMode.TARGET_RANGE:
            if self.target_range is None:
                raise SemanticPriorityError(
                    "TARGET_RANGE semantics require accepted explicit bounds."
                )
        elif self.target_range is not None:
            raise SemanticPriorityError(
                "Only TARGET_RANGE semantic rules may carry target bounds."
            )

    def matches(self, definition: CanonicalDriverDefinition) -> bool:
        return {
            RuleMatchKind.DRIVER_ID: definition.driver_id == self.match_value,
            RuleMatchKind.DRIVER_FAMILY: definition.driver_family == self.match_value,
            RuleMatchKind.UNIT_ID: definition.unit_id == self.match_value,
            RuleMatchKind.FALLBACK: True,
        }[self.match_kind]

    def to_dict(self) -> dict[str, Any]:
        return {
            "authority": self.authority.value,
            "layer": self.layer.value,
            "limitations": list(self.limitations),
            "match_kind": self.match_kind.value,
            "match_value": self.match_value,
            "reason": self.reason,
            "regime_id": self.regime_id,
            "rule_id": self.rule_id,
            "semantic_mode": self.semantic_mode.value,
            "target_range": self.target_range.to_dict() if self.target_range else None,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
        }


@dataclass(frozen=True, slots=True)
class ContextPredicate:
    reference_driver_id: str
    field: PredicateField
    allowed_values: tuple[str, ...]
    match: PredicateMatch = PredicateMatch.ANY
    dimension_member: str | None = None

    def __post_init__(self) -> None:
        if not self.reference_driver_id or not self.allowed_values:
            raise SemanticPriorityError(
                "Context predicates require a reference and allowed values."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed_values": list(self.allowed_values),
            "dimension_member": self.dimension_member,
            "field": self.field.value,
            "match": self.match.value,
            "reference_driver_id": self.reference_driver_id,
        }


@dataclass(frozen=True, slots=True)
class ContextCondition:
    kind: ContextConditionKind
    predicates: tuple[ContextPredicate, ...] = ()
    aggregate_dimension_member: str | None = None

    def __post_init__(self) -> None:
        if self.kind is ContextConditionKind.PREDICATE_SET and not self.predicates:
            raise SemanticPriorityError("Predicate-set conditions cannot be empty.")
        if (
            self.kind is ContextConditionKind.AGGREGATE_COMPONENT_SIGN_DIVERGENCE
            and not self.aggregate_dimension_member
        ):
            raise SemanticPriorityError(
                "Aggregate/component divergence requires an aggregate member."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregate_dimension_member": self.aggregate_dimension_member,
            "kind": self.kind.value,
            "predicates": [item.to_dict() for item in self.predicates],
        }


@dataclass(frozen=True, slots=True)
class ContextRelationship:
    relationship_id: str
    primary_driver_id: str
    modifier_driver_or_accepted_product_fact: tuple[str, ...]
    relationship_type: ContextRelationshipType
    condition: ContextCondition
    effect_on_interpretation: ContextEffectState
    authority: SemanticAuthority
    evidence_reference: str
    limitations: tuple[str, ...]
    scope: str
    dimensions: tuple[str, ...] = ()
    financial_target: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    primary_dimension_member: str | None = None
    final_interpretation_when_true: EconomicInterpretation | None = None
    allows_directional_resolution: bool = False

    def __post_init__(self) -> None:
        if not all(
            (
                self.relationship_id,
                self.primary_driver_id,
                self.evidence_reference,
                self.scope,
            )
        ):
            raise SemanticPriorityError(
                "Context relationships require identity, primary driver, evidence, and scope."
            )
        if self.authority is SemanticAuthority.UNRESOLVED and (
            self.final_interpretation_when_true
            in {EconomicInterpretation.POSITIVE, EconomicInterpretation.NEGATIVE}
        ):
            raise SemanticPriorityError(
                "Unresolved context authority cannot manufacture strong interpretation."
            )
        if self.allows_directional_resolution and (
            self.final_interpretation_when_true
            not in {EconomicInterpretation.POSITIVE, EconomicInterpretation.NEGATIVE}
        ):
            raise SemanticPriorityError(
                "Directional resolution must explicitly resolve to POSITIVE or NEGATIVE."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allows_directional_resolution": self.allows_directional_resolution,
            "authority": self.authority.value,
            "condition": self.condition.to_dict(),
            "dimensions": list(self.dimensions),
            "effect_on_interpretation": self.effect_on_interpretation.value,
            "evidence_reference": self.evidence_reference,
            "final_interpretation_when_true": (
                self.final_interpretation_when_true.value
                if self.final_interpretation_when_true
                else None
            ),
            "financial_target": self.financial_target,
            "limitations": list(self.limitations),
            "modifier_driver_or_accepted_product_fact": list(
                self.modifier_driver_or_accepted_product_fact
            ),
            "primary_dimension_member": self.primary_dimension_member,
            "primary_driver": self.primary_driver_id,
            "relationship_id": self.relationship_id,
            "relationship_type": self.relationship_type.value,
            "scope": self.scope,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
        }


@dataclass(frozen=True, slots=True)
class ContextBundle:
    bundle_id: str
    economic_question: str
    primary_drivers: tuple[str, ...]
    modifier_drivers: tuple[str, ...]
    relationship_types: tuple[ContextRelationshipType, ...]
    financial_target: str | None
    scope: str
    validity_window: str

    def __post_init__(self) -> None:
        if not self.bundle_id or not self.economic_question or not self.primary_drivers:
            raise SemanticPriorityError(
                "Context bundles require identity, question, and primary drivers."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "economic_owner": False,
            "economic_question": self.economic_question,
            "financial_target": self.financial_target,
            "modifier_drivers": list(self.modifier_drivers),
            "primary_drivers": list(self.primary_drivers),
            "relationship_types": [item.value for item in self.relationship_types],
            "scope": self.scope,
            "validity_window": self.validity_window,
        }


@dataclass(frozen=True, slots=True)
class PriorityDimensions:
    financial_materiality: OrdinalRating
    forward_relevance: OrdinalRating
    management_emphasis: OrdinalRating
    disclosure_continuity: OrdinalRating
    data_quality: OrdinalRating
    historical_depth: OrdinalRating
    explanatory_usefulness: OrdinalRating
    uniqueness: OrdinalRating

    def to_dict(self) -> dict[str, str]:
        return {field.name: getattr(self, field.name).value for field in dataclasses.fields(self)}


@dataclass(frozen=True, slots=True)
class PrioritySpec:
    driver_id: str
    visibility_tier: VisibilityTier
    economic_group: EconomicGroup
    dimensions: PriorityDimensions
    reason: str
    current_relevance: bool = True
    unique_explanatory_value: bool = True
    material_relevance: bool = True
    context_aware_tier_change: bool = False
    baseline_tier: VisibilityTier | None = None
    onboarding_review_required: bool = False

    def __post_init__(self) -> None:
        if not self.driver_id or not self.reason:
            raise SemanticPriorityError("Priority specs require identity and reasoning.")
        if self.context_aware_tier_change and self.baseline_tier is None:
            raise SemanticPriorityError(
                "Context-aware priority changes require an explicit baseline tier."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline_tier": self.baseline_tier.value if self.baseline_tier else None,
            "context_aware_tier_change": self.context_aware_tier_change,
            "current_relevance": self.current_relevance,
            "dimensions": self.dimensions.to_dict(),
            "driver_id": self.driver_id,
            "economic_group": self.economic_group.value,
            "material_relevance": self.material_relevance,
            "onboarding_review_required": self.onboarding_review_required,
            "reason": self.reason,
            "unique_explanatory_value": self.unique_explanatory_value,
            "visibility_tier": self.visibility_tier.value,
        }


@dataclass(frozen=True, slots=True)
class SectorSemanticPack:
    sector_pack_id: str
    rules: tuple[SemanticRule, ...]
    relationships: tuple[ContextRelationship, ...]
    bundles: tuple[ContextBundle, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundles": [item.to_dict() for item in self.bundles],
            "relationships": [item.to_dict() for item in self.relationships],
            "rules": [item.to_dict() for item in self.rules],
            "sector_pack_id": self.sector_pack_id,
        }


@dataclass(frozen=True, slots=True)
class TickerSemanticProfile:
    ticker: str
    sector_pack_id: str
    rules: tuple[SemanticRule, ...]
    relationships: tuple[ContextRelationship, ...]
    bundles: tuple[ContextBundle, ...]
    priority_specs: tuple[PrioritySpec, ...]
    accepted_profile_version: str

    def __post_init__(self) -> None:
        ids = [item.driver_id for item in self.priority_specs]
        if len(ids) != len(set(ids)):
            raise SemanticPriorityError("Ticker profile priority driver IDs must be unique.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted_profile_version": self.accepted_profile_version,
            "bundles": [item.to_dict() for item in self.bundles],
            "priority_specs": [item.to_dict() for item in self.priority_specs],
            "relationships": [item.to_dict() for item in self.relationships],
            "rules": [item.to_dict() for item in self.rules],
            "sector_pack_id": self.sector_pack_id,
            "ticker": self.ticker,
        }


@dataclass(frozen=True, slots=True)
class SemanticConfiguration:
    shared_rules: tuple[SemanticRule, ...]
    sector_packs: Mapping[str, SectorSemanticPack]
    ticker_profiles: Mapping[str, TickerSemanticProfile]

    def profile(self, ticker: str) -> TickerSemanticProfile:
        try:
            return self.ticker_profiles[ticker]
        except KeyError as exc:
            raise SemanticPriorityError(
                "NEW_TICKER_DRIVER_PROFILE_NEEDS_REVIEW"
            ) from exc

    def sector_pack(self, profile: TickerSemanticProfile) -> SectorSemanticPack:
        try:
            return self.sector_packs[profile.sector_pack_id]
        except KeyError as exc:
            raise SemanticPriorityError(
                f"Unknown sector semantic pack {profile.sector_pack_id!r}."
            ) from exc


@dataclass(frozen=True, slots=True)
class BaseSemanticSignal:
    semantic_signal_id: str
    source_analytical_signal_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    dimensions: tuple[dict[str, Any], ...]
    latest_value: str | None
    mathematical_direction: MathematicalDirection
    direction_basis: str
    mathematical_momentum: str
    semantic_mode: SemanticMode
    base_interpretation: EconomicInterpretation
    interpretation_basis: tuple[str, ...]
    semantic_authority: SemanticAuthority
    semantic_rule_id: str
    target_range: TargetRange | None
    definition_break_present: bool
    numeric_semantic_output: bool
    source_lineage_ids: tuple[str, ...]
    financial_linkage: str
    forecast_capability: str
    qoq_available: bool
    yoy_available: bool
    trend_available: bool
    comparable_history_depth: int
    lower_layer_analytics_sha256: str
    validity_window: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_interpretation": self.base_interpretation.value,
            "comparable_history_depth": self.comparable_history_depth,
            "definition_break_present": self.definition_break_present,
            "definition_version": self.definition_version,
            "dimension_set_id": self.dimension_set_id,
            "dimensions": list(self.dimensions),
            "direction_basis": self.direction_basis,
            "financial_linkage": self.financial_linkage,
            "forecast_capability": self.forecast_capability,
            "interpretation_basis": list(self.interpretation_basis),
            "latest_value": self.latest_value,
            "lower_layer_analytics_sha256": self.lower_layer_analytics_sha256,
            "mathematical_direction": self.mathematical_direction.value,
            "mathematical_momentum": self.mathematical_momentum,
            "numeric_semantic_output": self.numeric_semantic_output,
            "qoq_available": self.qoq_available,
            "semantic_authority": self.semantic_authority.value,
            "semantic_mode": self.semantic_mode.value,
            "semantic_rule_id": self.semantic_rule_id,
            "semantic_signal_id": self.semantic_signal_id,
            "source_analytical_signal_id": self.source_analytical_signal_id,
            "source_lineage_ids": list(self.source_lineage_ids),
            "target_range": self.target_range.to_dict() if self.target_range else None,
            "ticker": self.ticker,
            "trend_available": self.trend_available,
            "validity_window": self.validity_window,
            "yoy_available": self.yoy_available,
        }


@dataclass(frozen=True, slots=True)
class ContextEvaluation:
    relationship_id: str
    relationship_type: ContextRelationshipType
    condition_result: str
    effect_state: ContextEffectState
    requested_final_interpretation: EconomicInterpretation | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "condition_result": self.condition_result,
            "effect_state": self.effect_state.value,
            "reason": self.reason,
            "relationship_id": self.relationship_id,
            "relationship_type": self.relationship_type.value,
            "requested_final_interpretation": (
                self.requested_final_interpretation.value
                if self.requested_final_interpretation
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class ContextualSemanticSignal:
    semantic_signal_id: str
    ticker: str
    driver_id: str
    definition_version: int
    dimension_set_id: str
    mathematical_direction: MathematicalDirection
    mathematical_momentum: str
    semantic_mode: SemanticMode
    base_interpretation: EconomicInterpretation
    context_modifiers: tuple[ContextEvaluation, ...]
    context_interaction_result: ContextEffectState
    final_interpretation: EconomicInterpretation
    interpretation_basis: tuple[str, ...]
    semantic_authority: SemanticAuthority
    validity_window: str
    coloring_readiness: ColoringReadiness
    source_lineage_ids: tuple[str, ...]
    forecast_number: None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_interpretation": self.base_interpretation.value,
            "coloring_readiness": self.coloring_readiness.value,
            "context_interaction_result": self.context_interaction_result.value,
            "context_modifiers": [item.to_dict() for item in self.context_modifiers],
            "definition_version": self.definition_version,
            "dimension_set_id": self.dimension_set_id,
            "driver_id": self.driver_id,
            "final_interpretation": self.final_interpretation.value,
            "forecast_number": None,
            "interpretation_basis": list(self.interpretation_basis),
            "mathematical_direction": self.mathematical_direction.value,
            "mathematical_momentum": self.mathematical_momentum,
            "semantic_authority": self.semantic_authority.value,
            "semantic_mode": self.semantic_mode.value,
            "semantic_signal_id": self.semantic_signal_id,
            "source_lineage_ids": list(self.source_lineage_ids),
            "ticker": self.ticker,
            "validity_window": self.validity_window,
        }


@dataclass(frozen=True, slots=True)
class DriverPriority:
    priority_id: str
    ticker: str
    driver_id: str
    active_definition_version: int
    signal_ids: tuple[str, ...]
    visibility_tier: VisibilityTier
    economic_group: EconomicGroup
    dimensions: PriorityDimensions
    hard_gates: Mapping[str, bool]
    reason: str
    baseline_tier: VisibilityTier | None
    context_aware_tier_change: bool
    coloring_readiness: ColoringReadiness
    sparkline_12q_ready: bool
    trend_4q_ready: bool
    qoq_ready: bool
    yoy_ready: bool
    forecast_readiness: ForecastSemanticReadiness
    profile_review_required_during_onboarding: bool

    def __post_init__(self) -> None:
        if self.visibility_tier is VisibilityTier.CORE_DRIVER:
            if not self.reason or not all(self.hard_gates.values()):
                raise SemanticPriorityError(
                    "CORE drivers require explicit reasoning and every hard gate."
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "active_definition_version": self.active_definition_version,
            "baseline_tier": self.baseline_tier.value if self.baseline_tier else None,
            "coloring_readiness": self.coloring_readiness.value,
            "context_aware_tier_change": self.context_aware_tier_change,
            "dimensions": self.dimensions.to_dict(),
            "driver_id": self.driver_id,
            "economic_group": self.economic_group.value,
            "forecast_readiness": self.forecast_readiness.value,
            "hard_gates": dict(sorted(self.hard_gates.items())),
            "priority_id": self.priority_id,
            "profile_review_required_during_onboarding": self.profile_review_required_during_onboarding,
            "qoq_ready": self.qoq_ready,
            "reason": self.reason,
            "signal_ids": list(self.signal_ids),
            "sparkline_12q_ready": self.sparkline_12q_ready,
            "ticker": self.ticker,
            "trend_4q_ready": self.trend_4q_ready,
            "visibility_tier": self.visibility_tier.value,
            "yoy_ready": self.yoy_ready,
        }


@dataclass(frozen=True, slots=True)
class ProfileDriverReadiness:
    ticker: str
    driver_id: str
    identity_ready: bool
    definition_ready: bool
    owner_ready: bool
    period_comparability_ready: bool
    semantic_mode_ready: bool
    context_dependencies_declared: bool
    financial_linkage_ready: bool
    forecast_capability_ready: bool
    visibility_tier_ready: bool
    state: ProfileDriverReadinessState
    reason: str

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["state"] = self.state.value
        return result


@dataclass(frozen=True, slots=True)
class AnalystSemanticOverride:
    override_id: str
    target_driver_id: str
    reason: str
    effective_from: str
    semantic_mode: SemanticMode | None = None
    visibility_tier: VisibilityTier | None = None
    context_relationship_id: str | None = None
    target_range: TargetRange | None = None

    def __post_init__(self) -> None:
        if not all((self.override_id, self.target_driver_id, self.reason, self.effective_from)):
            raise SemanticPriorityError(
                "Analyst overrides must be versioned, reasoned, and effective-dated."
            )
        if all(
            value is None
            for value in (
                self.semantic_mode,
                self.visibility_tier,
                self.context_relationship_id,
                self.target_range,
            )
        ):
            raise SemanticPriorityError("Analyst override changes no allowed semantic field.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "context_relationship_id": self.context_relationship_id,
            "effective_from": self.effective_from,
            "historical_observation_mutation_allowed": False,
            "override_id": self.override_id,
            "reason": self.reason,
            "semantic_mode": self.semantic_mode.value if self.semantic_mode else None,
            "target_driver_id": self.target_driver_id,
            "target_range": self.target_range.to_dict() if self.target_range else None,
            "visibility_tier": self.visibility_tier.value if self.visibility_tier else None,
        }


@dataclass(frozen=True, slots=True)
class SemanticPriorityPackage:
    ticker: str
    registry_package_sha256: str
    derived_analytics_sha256: str
    profile_version: str
    sector_pack_id: str
    semantic_rules: tuple[SemanticRule, ...]
    context_relationships: tuple[ContextRelationship, ...]
    context_bundles: tuple[ContextBundle, ...]
    base_semantic_signals: tuple[BaseSemanticSignal, ...]
    contextual_semantic_signals: tuple[ContextualSemanticSignal, ...]
    driver_priorities: tuple[DriverPriority, ...]
    profile_readiness: tuple[ProfileDriverReadiness, ...]
    profile_product_readiness: ProfileProductReadiness
    data_cleanup_priorities: tuple[dict[str, Any], ...]
    extraction_priorities: tuple[dict[str, Any], ...]
    lower_layer_fact_mutation_count: int = 0
    qualitative_to_numeric_count: int = 0
    forecast_number_emission_count: int = 0
    duplicate_economic_owner_count: int = 0
    context_bundle_ownership_violation_count: int = 0
    opaque_priority_output_count: int = 0
    new_ticker_specific_python_semantic_branch_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_semantic_signals": [item.to_dict() for item in self.base_semantic_signals],
            "context_bundle_ownership_violation_count": self.context_bundle_ownership_violation_count,
            "context_bundles": [item.to_dict() for item in self.context_bundles],
            "context_relationships": [item.to_dict() for item in self.context_relationships],
            "contextual_semantic_signals": [
                item.to_dict() for item in self.contextual_semantic_signals
            ],
            "contract_version": OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION,
            "data_cleanup_priorities": list(self.data_cleanup_priorities),
            "derived_analytics_sha256": self.derived_analytics_sha256,
            "driver_priorities": [item.to_dict() for item in self.driver_priorities],
            "duplicate_economic_owner_count": self.duplicate_economic_owner_count,
            "extraction_priorities": list(self.extraction_priorities),
            "forecast_number_emission_count": self.forecast_number_emission_count,
            "lower_layer_fact_mutation_count": self.lower_layer_fact_mutation_count,
            "new_ticker_specific_python_semantic_branch_count": self.new_ticker_specific_python_semantic_branch_count,
            "opaque_priority_output_count": self.opaque_priority_output_count,
            "profile_product_readiness": self.profile_product_readiness.value,
            "profile_readiness": [item.to_dict() for item in self.profile_readiness],
            "profile_version": self.profile_version,
            "qualitative_to_numeric_count": self.qualitative_to_numeric_count,
            "registry_package_sha256": self.registry_package_sha256,
            "sector_pack_id": self.sector_pack_id,
            "semantic_rules": [item.to_dict() for item in self.semantic_rules],
            "ticker": self.ticker,
        }

    def serialize(self) -> bytes:
        return serialize_package(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.serialize()).hexdigest()


def _specificity(rule: SemanticRule) -> int:
    return {
        RuleMatchKind.FALLBACK: 0,
        RuleMatchKind.UNIT_ID: 1,
        RuleMatchKind.DRIVER_FAMILY: 2,
        RuleMatchKind.DRIVER_ID: 3,
    }[rule.match_kind]


def resolve_semantic_rule(
    definition: CanonicalDriverDefinition,
    *,
    shared_rules: Sequence[SemanticRule],
    sector_rules: Sequence[SemanticRule],
    ticker_rules: Sequence[SemanticRule],
) -> SemanticRule:
    """Resolve one rule by explicit layer/specificity, failing on ambiguity."""

    selected: SemanticRule | None = None
    for expected_layer, rules in (
        (RuleLayer.SHARED, shared_rules),
        (RuleLayer.SECTOR_PACK, sector_rules),
        (RuleLayer.TICKER_PROFILE, ticker_rules),
    ):
        matches = [item for item in rules if item.matches(definition)]
        if not matches:
            continue
        if any(item.layer is not expected_layer for item in matches):
            raise SemanticPriorityError("Semantic rule is registered in the wrong layer.")
        maximum = max(_specificity(item) for item in matches)
        finalists = [item for item in matches if _specificity(item) == maximum]
        signatures = {
            (item.semantic_mode, item.authority, item.target_range) for item in finalists
        }
        if len(signatures) != 1:
            raise SemanticPriorityError(
                f"Ambiguous semantic rules for {definition.driver_id}."
            )
        selected = sorted(finalists, key=lambda item: item.rule_id)[0]
    if selected is None:
        raise SemanticPriorityError(
            f"No fail-closed semantic rule for {definition.driver_id}."
        )
    return selected


def derive_base_interpretation(
    *,
    semantic_mode: SemanticMode,
    mathematical_direction: MathematicalDirection,
    latest_value: str | None,
    target_range: TargetRange | None = None,
    definition_break_present: bool = False,
) -> EconomicInterpretation:
    """Interpret one mathematical signal without consulting cross-driver context."""

    if definition_break_present:
        return EconomicInterpretation.INSUFFICIENT_EVIDENCE
    if latest_value is None:
        return EconomicInterpretation.INSUFFICIENT_EVIDENCE
    if semantic_mode is SemanticMode.NO_GOOD_BAD_SEMANTICS:
        return EconomicInterpretation.NOT_INTERPRETABLE
    if semantic_mode is SemanticMode.DIRECTION_ONLY:
        return EconomicInterpretation.NOT_INTERPRETABLE
    if semantic_mode is SemanticMode.CONTEXT_DEPENDENT:
        return EconomicInterpretation.CONTEXT_DEPENDENT
    if semantic_mode is SemanticMode.TARGET_RANGE:
        if target_range is None:
            raise SemanticPriorityError(
                "TARGET_RANGE interpretation attempted without accepted bounds."
            )
        value = _decimal(latest_value)
        if value < _decimal(target_range.lower_bound):
            return EconomicInterpretation.OUTSIDE_TARGET_LOW
        if value > _decimal(target_range.upper_bound):
            return EconomicInterpretation.OUTSIDE_TARGET_HIGH
        return EconomicInterpretation.WITHIN_TARGET
    if mathematical_direction is MathematicalDirection.INSUFFICIENT_DATA:
        return EconomicInterpretation.INSUFFICIENT_EVIDENCE
    if mathematical_direction is MathematicalDirection.MIXED:
        return EconomicInterpretation.MIXED
    if mathematical_direction is MathematicalDirection.UNCHANGED:
        return EconomicInterpretation.NEUTRAL
    higher = mathematical_direction is MathematicalDirection.UP
    if semantic_mode is SemanticMode.HIGHER_BETTER:
        return EconomicInterpretation.POSITIVE if higher else EconomicInterpretation.NEGATIVE
    if semantic_mode is SemanticMode.LOWER_BETTER:
        return EconomicInterpretation.NEGATIVE if higher else EconomicInterpretation.POSITIVE
    raise SemanticPriorityError(f"Unsupported semantic mode {semantic_mode.value}.")


def acceleration_semantic_effect(base: BaseSemanticSignal) -> str:
    """Explain momentum without allowing it to create an economic signal alone."""

    acceleration = base.mathematical_momentum
    if acceleration not in {
        AccelerationState.POSITIVE_ACCELERATION.value,
        AccelerationState.NEGATIVE_ACCELERATION.value,
    }:
        return "NO_EFFECT"
    if base.semantic_mode in {
        SemanticMode.CONTEXT_DEPENDENT,
        SemanticMode.DIRECTION_ONLY,
        SemanticMode.NO_GOOD_BAD_SEMANTICS,
        SemanticMode.TARGET_RANGE,
    }:
        return "MATHEMATICALLY_DESCRIPTIVE_ONLY"
    favorable_acceleration = (
        acceleration == AccelerationState.POSITIVE_ACCELERATION.value
        if base.semantic_mode is SemanticMode.HIGHER_BETTER
        else acceleration == AccelerationState.NEGATIVE_ACCELERATION.value
    )
    if base.base_interpretation is EconomicInterpretation.POSITIVE:
        return "STRENGTHENS" if favorable_acceleration else "ATTENUATES"
    if base.base_interpretation is EconomicInterpretation.NEGATIVE:
        return "ATTENUATES" if favorable_acceleration else "STRENGTHENS"
    return "NO_EFFECT"


def _comparison_direction(value: str | None) -> MathematicalDirection:
    if value is None:
        return MathematicalDirection.INSUFFICIENT_DATA
    number = _decimal(value)
    if number > 0:
        return MathematicalDirection.UP
    if number < 0:
        return MathematicalDirection.DOWN
    return MathematicalDirection.UNCHANGED


def _direction_for_signal(
    signal: Any,
    analytics: DerivedAnalyticsPackage,
) -> tuple[MathematicalDirection, str, bool, bool, bool, bool]:
    qoq_by_id = {item.analysis_id: item for item in analytics.qoq_analytics}
    yoy_by_id = {item.analysis_id: item for item in analytics.yoy_analytics}
    trend_by_id = {item.analysis_id: item for item in analytics.trend_analytics}
    qoq = qoq_by_id.get(signal.qoq_analysis_id)
    yoy = yoy_by_id.get(signal.yoy_analysis_id)
    trend = trend_by_id.get(signal.trend_analysis_id)
    definition_break = any(
        item is not None and item.availability is AnalyticsAvailability.DEFINITION_BREAK
        for item in (qoq, yoy, trend)
    )
    qoq_available = qoq is not None and qoq.availability is AnalyticsAvailability.AVAILABLE
    yoy_available = yoy is not None and yoy.availability is AnalyticsAvailability.AVAILABLE
    trend_available = (
        trend is not None
        and trend.availability is AnalyticsAvailability.AVAILABLE
        and trend.state is not TrendState.INSUFFICIENT_DATA
    )
    if qoq_available:
        value = qoq.percentage_point_change or qoq.native_unit_change
        return (
            _comparison_direction(value),
            f"QOQ:{qoq.analysis_id}",
            qoq_available,
            yoy_available,
            trend_available,
            definition_break,
        )
    if yoy_available:
        value = yoy.percentage_point_change or yoy.native_unit_change
        return (
            _comparison_direction(value),
            f"YOY:{yoy.analysis_id}",
            qoq_available,
            yoy_available,
            trend_available,
            definition_break,
        )
    if trend_available:
        direction = {
            TrendState.UP: MathematicalDirection.UP,
            TrendState.DOWN: MathematicalDirection.DOWN,
            TrendState.UNCHANGED: MathematicalDirection.UNCHANGED,
            TrendState.MIXED: MathematicalDirection.MIXED,
        }[trend.state]
        return (
            direction,
            f"TREND_4Q:{trend.analysis_id}",
            qoq_available,
            yoy_available,
            trend_available,
            definition_break,
        )
    return (
        MathematicalDirection.INSUFFICIENT_DATA,
        "NO_EXACT_COMPARABLE_CHANGE",
        qoq_available,
        yoy_available,
        trend_available,
        definition_break,
    )


def _predicate_value(signal: BaseSemanticSignal, field: PredicateField) -> str:
    return {
        PredicateField.MATHEMATICAL_DIRECTION: signal.mathematical_direction.value,
        PredicateField.BASE_INTERPRETATION: signal.base_interpretation.value,
        PredicateField.SEMANTIC_MODE: signal.semantic_mode.value,
        PredicateField.LATEST_VALUE_SIGN: _value_sign(signal.latest_value),
        PredicateField.NUMERIC_AVAILABLE: str(signal.latest_value is not None).lower(),
        PredicateField.ACCELERATION_STATE: signal.mathematical_momentum,
    }[field]


def _evaluate_predicate(
    predicate: ContextPredicate,
    primary: BaseSemanticSignal,
    universe: Sequence[BaseSemanticSignal],
) -> bool | None:
    candidates = (
        [primary]
        if predicate.reference_driver_id == "PRIMARY"
        else [
            item
            for item in universe
            if item.ticker == primary.ticker
            and item.driver_id == predicate.reference_driver_id
        ]
    )
    if predicate.dimension_member:
        candidates = [
            item
            for item in candidates
            if predicate.dimension_member in _dimension_members(item.dimensions)
        ]
    if not candidates:
        return None
    evaluations = [
        _predicate_value(item, predicate.field) in predicate.allowed_values
        for item in candidates
    ]
    return any(evaluations) if predicate.match is PredicateMatch.ANY else all(evaluations)


def evaluate_context_relationship(
    primary: BaseSemanticSignal,
    universe: Sequence[BaseSemanticSignal],
    relationship: ContextRelationship,
) -> ContextEvaluation:
    """Evaluate one accepted declarative relationship without modifying inputs."""

    if relationship.primary_driver_id != primary.driver_id:
        return ContextEvaluation(
            relationship.relationship_id,
            relationship.relationship_type,
            "NOT_APPLICABLE",
            ContextEffectState.NO_EFFECT,
            None,
            "Relationship primary driver does not match the signal.",
        )
    if relationship.primary_dimension_member and (
        relationship.primary_dimension_member not in _dimension_members(primary.dimensions)
    ):
        return ContextEvaluation(
            relationship.relationship_id,
            relationship.relationship_type,
            "NOT_APPLICABLE",
            ContextEffectState.NO_EFFECT,
            None,
            "Relationship primary dimension does not match the signal.",
        )

    if (
        relationship.condition.kind
        is ContextConditionKind.AGGREGATE_COMPONENT_SIGN_DIVERGENCE
    ):
        aggregate_member = relationship.condition.aggregate_dimension_member
        if aggregate_member not in _dimension_members(primary.dimensions):
            outcome: bool | None = False
        elif primary.latest_value is None:
            outcome = None
        else:
            primary_sign = _value_sign(primary.latest_value)
            components = [
                item
                for item in universe
                if item.ticker == primary.ticker
                and item.driver_id == primary.driver_id
                and aggregate_member not in _dimension_members(item.dimensions)
                and item.latest_value is not None
            ]
            if not components:
                outcome = None
            else:
                outcome = any(
                    _value_sign(item.latest_value)
                    not in {primary_sign, "ZERO", "UNAVAILABLE"}
                    and primary_sign not in {"ZERO", "UNAVAILABLE"}
                    for item in components
                )
    else:
        results = [
            _evaluate_predicate(item, primary, universe)
            for item in relationship.condition.predicates
        ]
        outcome = None if any(item is None for item in results) else all(results)

    if outcome is None:
        state = (
            ContextEffectState.CONTEXT_REQUIRED
            if relationship.relationship_type is ContextRelationshipType.PREREQUISITE
            else ContextEffectState.NOT_EVALUABLE
        )
        return ContextEvaluation(
            relationship.relationship_id,
            relationship.relationship_type,
            "NOT_EVALUABLE",
            state,
            None,
            "Required accepted modifier evidence is unavailable.",
        )
    if not outcome:
        return ContextEvaluation(
            relationship.relationship_id,
            relationship.relationship_type,
            "FALSE",
            ContextEffectState.NO_EFFECT,
            None,
            "Accepted condition is not satisfied.",
        )

    requested = relationship.final_interpretation_when_true
    if requested in {EconomicInterpretation.POSITIVE, EconomicInterpretation.NEGATIVE}:
        if primary.semantic_mode in {
            SemanticMode.NO_GOOD_BAD_SEMANTICS,
            SemanticMode.DIRECTION_ONLY,
        }:
            raise SemanticPriorityError(
                "Context cannot create strong interpretation for a no-good/bad rule."
            )
        if (
            primary.semantic_mode is SemanticMode.CONTEXT_DEPENDENT
            and not relationship.allows_directional_resolution
        ):
            raise SemanticPriorityError(
                "Context-dependent signal lacks an accepted directional resolution rule."
            )
    return ContextEvaluation(
        relationship.relationship_id,
        relationship.relationship_type,
        "TRUE",
        relationship.effect_on_interpretation,
        requested,
        "Accepted declarative context condition is satisfied.",
    )


def _combined_context_result(evaluations: Sequence[ContextEvaluation]) -> ContextEffectState:
    precedence = (
        ContextEffectState.OVERRIDE_TO_NOT_INTERPRETABLE,
        ContextEffectState.MIXED,
        ContextEffectState.CONTEXT_REQUIRED,
        ContextEffectState.ATTENUATED,
        ContextEffectState.CONFIRMED,
        ContextEffectState.NOT_EVALUABLE,
        ContextEffectState.NO_EFFECT,
    )
    states = {item.effect_state for item in evaluations}
    return next((item for item in precedence if item in states), ContextEffectState.NO_EFFECT)


def _coloring_readiness(
    mode: SemanticMode,
    final: EconomicInterpretation,
) -> ColoringReadiness:
    if final in {EconomicInterpretation.INSUFFICIENT_EVIDENCE}:
        return ColoringReadiness.NOT_READY
    if mode is SemanticMode.TARGET_RANGE:
        return ColoringReadiness.SAFE_FOR_TARGET_RANGE_DISPLAY
    if mode in {SemanticMode.HIGHER_BETTER, SemanticMode.LOWER_BETTER} and final in {
        EconomicInterpretation.POSITIVE,
        EconomicInterpretation.NEGATIVE,
    }:
        return ColoringReadiness.SAFE_FOR_POSITIVE_NEGATIVE_FILL
    if mode in {SemanticMode.CONTEXT_DEPENDENT, SemanticMode.DIRECTION_ONLY}:
        return ColoringReadiness.SAFE_FOR_DIRECTION_ONLY
    if final in {
        EconomicInterpretation.NEUTRAL,
        EconomicInterpretation.MIXED,
        EconomicInterpretation.NOT_INTERPRETABLE,
        EconomicInterpretation.CONTEXT_DEPENDENT,
    }:
        return ColoringReadiness.NEUTRAL_ONLY
    return ColoringReadiness.NOT_READY


def apply_context_relationships(
    base: BaseSemanticSignal,
    universe: Sequence[BaseSemanticSignal],
    relationships: Sequence[ContextRelationship],
) -> ContextualSemanticSignal:
    applicable = [
        item
        for item in relationships
        if item.primary_driver_id == base.driver_id
        and (
            item.primary_dimension_member is None
            or item.primary_dimension_member in _dimension_members(base.dimensions)
        )
    ]
    evaluations = tuple(
        sorted(
            (
                evaluate_context_relationship(base, universe, item)
                for item in applicable
            ),
            key=lambda item: item.relationship_id,
        )
    )
    final = base.base_interpretation
    requested = {
        item.requested_final_interpretation
        for item in evaluations
        if item.condition_result == "TRUE"
        and item.requested_final_interpretation is not None
    }
    if base.definition_break_present:
        final = base.base_interpretation
    elif len(requested) > 1:
        final = EconomicInterpretation.MIXED
    elif requested:
        final = next(iter(requested))
        if final in {
            EconomicInterpretation.POSITIVE,
            EconomicInterpretation.NEGATIVE,
        } and any(
            item.effect_state
            in {ContextEffectState.ATTENUATED, ContextEffectState.MIXED}
            for item in evaluations
        ):
            final = EconomicInterpretation.MIXED
    elif any(
        item.effect_state is ContextEffectState.OVERRIDE_TO_NOT_INTERPRETABLE
        for item in evaluations
    ):
        final = EconomicInterpretation.NOT_INTERPRETABLE
    elif any(item.effect_state is ContextEffectState.MIXED for item in evaluations):
        final = EconomicInterpretation.MIXED
    elif any(
        item.effect_state is ContextEffectState.ATTENUATED for item in evaluations
    ) and final in {EconomicInterpretation.POSITIVE, EconomicInterpretation.NEGATIVE}:
        final = EconomicInterpretation.MIXED
    elif any(
        item.effect_state is ContextEffectState.CONTEXT_REQUIRED for item in evaluations
    ):
        final = EconomicInterpretation.CONTEXT_DEPENDENT

    context_result = (
        ContextEffectState.NOT_EVALUABLE
        if base.definition_break_present and evaluations
        else _combined_context_result(evaluations)
    )
    basis = (*base.interpretation_basis, f"ACCELERATION:{acceleration_semantic_effect(base)}")
    basis += tuple(
        f"CONTEXT:{item.relationship_id}:{item.effect_state.value}"
        for item in evaluations
        if item.effect_state is not ContextEffectState.NO_EFFECT
    )
    return ContextualSemanticSignal(
        semantic_signal_id=base.semantic_signal_id,
        ticker=base.ticker,
        driver_id=base.driver_id,
        definition_version=base.definition_version,
        dimension_set_id=base.dimension_set_id,
        mathematical_direction=base.mathematical_direction,
        mathematical_momentum=base.mathematical_momentum,
        semantic_mode=base.semantic_mode,
        base_interpretation=base.base_interpretation,
        context_modifiers=evaluations,
        context_interaction_result=context_result,
        final_interpretation=final,
        interpretation_basis=basis,
        semantic_authority=base.semantic_authority,
        validity_window=base.validity_window,
        coloring_readiness=_coloring_readiness(base.semantic_mode, final),
        source_lineage_ids=base.source_lineage_ids,
    )


def _forecast_readiness(
    priority: PrioritySpec,
    signals: Sequence[BaseSemanticSignal],
) -> ForecastSemanticReadiness:
    if priority.visibility_tier in {VisibilityTier.SUPPORT_ONLY, VisibilityTier.RETIRED}:
        return ForecastSemanticReadiness.NOT_FORECAST_RELEVANT
    if not any(item.latest_value is not None for item in signals):
        return ForecastSemanticReadiness.NEEDS_DATA
    capabilities = {item.forecast_capability for item in signals}
    linkages = {item.financial_linkage for item in signals}
    if "SPECULATIVE_ASSOCIATION" in linkages:
        return ForecastSemanticReadiness.NEEDS_RELATIONSHIP_REVIEW
    if any(item.semantic_mode is SemanticMode.CONTEXT_DEPENDENT for item in signals):
        return ForecastSemanticReadiness.FORECAST_CONTEXT_READY
    if capabilities & {
        "DIRECT_FORECAST_INPUT",
        "LEADING_INDICATOR",
        "SENSITIVITY_INPUT",
    }:
        return ForecastSemanticReadiness.FORECAST_EVIDENCE_READY
    if "FORECAST_CONTEXT" in capabilities:
        return ForecastSemanticReadiness.FORECAST_CONTEXT_READY
    return ForecastSemanticReadiness.NOT_FORECAST_RELEVANT


def _strongest_coloring(
    signals: Sequence[ContextualSemanticSignal],
) -> ColoringReadiness:
    order = (
        ColoringReadiness.SAFE_FOR_POSITIVE_NEGATIVE_FILL,
        ColoringReadiness.SAFE_FOR_TARGET_RANGE_DISPLAY,
        ColoringReadiness.SAFE_FOR_DIRECTION_ONLY,
        ColoringReadiness.NEUTRAL_ONLY,
        ColoringReadiness.NOT_READY,
    )
    present = {item.coloring_readiness for item in signals}
    return next(item for item in order if item in present)


def _profile_readiness(
    definition: CanonicalDriverDefinition,
    rule: SemanticRule,
    priority: DriverPriority,
    base_signals: Sequence[BaseSemanticSignal],
    relationships: Sequence[ContextRelationship],
) -> ProfileDriverReadiness:
    lineage = bool(base_signals) and all(item.source_lineage_ids for item in base_signals)
    semantic_ready = rule.authority is not SemanticAuthority.UNRESOLVED
    context_declared = (
        rule.semantic_mode is not SemanticMode.CONTEXT_DEPENDENT
        or any(
            item.primary_driver_id == definition.driver_id
            or definition.driver_id
            in item.modifier_driver_or_accepted_product_fact
            for item in relationships
        )
        or rule.authority
        in {
            SemanticAuthority.SOURCE_DEFINED,
            SemanticAuthority.ACCOUNTING_IDENTITY,
            SemanticAuthority.SECTOR_PACK_RULE,
            SemanticAuthority.TICKER_PROFILE_RULE,
            SemanticAuthority.ANALYST_CURATED,
        }
    )
    period_ready = not any(
        item.definition_break_present and item.mathematical_direction is not MathematicalDirection.INSUFFICIENT_DATA
        for item in base_signals
    )
    if not lineage and priority.visibility_tier is VisibilityTier.CORE_DRIVER:
        state = ProfileDriverReadinessState.PROFILE_BLOCKED
        reason = "A CORE driver cannot be accepted without source/shadow lineage."
    elif not semantic_ready or not context_declared:
        state = ProfileDriverReadinessState.PROFILE_NEEDS_REVIEW
        reason = "Material semantic mode or required context remains unresolved."
    elif not lineage:
        state = ProfileDriverReadinessState.PROFILE_READY_WITH_NEUTRAL_SEMANTICS
        reason = (
            "Candidate semantics are explicit, but no current observation/attachment exists; "
            "the non-CORE driver remains neutral and produces no numeric output."
        )
    elif rule.semantic_mode in {
        SemanticMode.CONTEXT_DEPENDENT,
        SemanticMode.DIRECTION_ONLY,
        SemanticMode.NO_GOOD_BAD_SEMANTICS,
    }:
        state = ProfileDriverReadinessState.PROFILE_READY_WITH_NEUTRAL_SEMANTICS
        reason = "Profile is explicit and fail-closed without forced green/red meaning."
    else:
        state = ProfileDriverReadinessState.PROFILE_READY
        reason = "Identity, lineage, semantics, context, and visibility are accepted."
    return ProfileDriverReadiness(
        ticker=priority.ticker,
        driver_id=definition.driver_id,
        identity_ready=True,
        definition_ready=True,
        owner_ready=True,
        period_comparability_ready=period_ready,
        semantic_mode_ready=semantic_ready,
        context_dependencies_declared=context_declared,
        financial_linkage_ready=definition.financial_linkage.value != "NONE",
        forecast_capability_ready=bool(definition.forecast_capability.value),
        visibility_tier_ready=True,
        state=state,
        reason=reason,
    )


def _build_priority_records(
    registry: ShadowRegistryPackage,
    profile: TickerSemanticProfile,
    base_signals: Sequence[BaseSemanticSignal],
    contextual_signals: Sequence[ContextualSemanticSignal],
) -> tuple[DriverPriority, ...]:
    specs = {item.driver_id: item for item in profile.priority_specs}
    definitions: dict[str, CanonicalDriverDefinition] = {}
    for item in registry.profile.definitions:
        current = definitions.get(item.driver_id)
        if current is None or item.definition_version > current.definition_version:
            definitions[item.driver_id] = item
    if set(specs) != set(definitions):
        missing = sorted(set(definitions) - set(specs))
        extra = sorted(set(specs) - set(definitions))
        raise SemanticPriorityError(
            f"Priority profile does not reconcile definitions; missing={missing}, extra={extra}."
        )
    by_driver_base: dict[str, list[BaseSemanticSignal]] = defaultdict(list)
    by_driver_context: dict[str, list[ContextualSemanticSignal]] = defaultdict(list)
    for item in base_signals:
        by_driver_base[item.driver_id].append(item)
    for item in contextual_signals:
        by_driver_context[item.driver_id].append(item)

    result: list[DriverPriority] = []
    for driver_id in sorted(definitions):
        definition = definitions[driver_id]
        spec = specs[driver_id]
        bases = by_driver_base[driver_id]
        contexts = by_driver_context[driver_id]
        hard_gates = {
            "accepted_identity": True,
            "current_relevance": spec.current_relevance,
            "lineage": bool(bases) and all(item.source_lineage_ids for item in bases),
            "material_relevance": spec.material_relevance,
            "no_duplicate_owner": True,
            "no_unresolved_definition_incompatibility": True,
            "unique_explanatory_value": spec.unique_explanatory_value,
        }
        tier = spec.visibility_tier
        if tier is VisibilityTier.CORE_DRIVER and not all(hard_gates.values()):
            tier = VisibilityTier.WATCH_DRIVER
        max_history = max((item.comparable_history_depth for item in bases), default=0)
        record = DriverPriority(
            priority_id=_hash_id("driver-priority", profile.ticker, driver_id),
            ticker=profile.ticker,
            driver_id=driver_id,
            active_definition_version=definition.definition_version,
            signal_ids=tuple(sorted(item.semantic_signal_id for item in bases)),
            visibility_tier=tier,
            economic_group=spec.economic_group,
            dimensions=spec.dimensions,
            hard_gates=hard_gates,
            reason=spec.reason,
            baseline_tier=spec.baseline_tier,
            context_aware_tier_change=spec.context_aware_tier_change,
            coloring_readiness=_strongest_coloring(contexts),
            sparkline_12q_ready=max_history >= 12,
            trend_4q_ready=any(item.trend_available for item in bases),
            qoq_ready=any(item.qoq_available for item in bases),
            yoy_ready=any(item.yoy_available for item in bases),
            forecast_readiness=_forecast_readiness(spec, bases),
            profile_review_required_during_onboarding=spec.onboarding_review_required,
        )
        result.append(record)
    return tuple(result)


def _priority_levels(
    financial: OrdinalRating,
    forward: OrdinalRating,
    explanation: OrdinalRating,
    *,
    management: OrdinalRating = OrdinalRating.MEDIUM,
    continuity: OrdinalRating = OrdinalRating.MEDIUM,
    quality: OrdinalRating = OrdinalRating.MEDIUM,
    history: OrdinalRating = OrdinalRating.MEDIUM,
    uniqueness: OrdinalRating = OrdinalRating.MEDIUM,
) -> PriorityDimensions:
    return PriorityDimensions(
        financial_materiality=financial,
        forward_relevance=forward,
        management_emphasis=management,
        disclosure_continuity=continuity,
        data_quality=quality,
        historical_depth=history,
        explanatory_usefulness=explanation,
        uniqueness=uniqueness,
    )


def _rule(
    rule_id: str,
    match_kind: RuleMatchKind,
    match_value: str,
    mode: SemanticMode,
    authority: SemanticAuthority,
    layer: RuleLayer,
    reason: str,
) -> SemanticRule:
    return SemanticRule(
        rule_id=rule_id,
        match_kind=match_kind,
        match_value=match_value,
        semantic_mode=mode,
        authority=authority,
        reason=reason,
        layer=layer,
    )


def _predicate(
    driver_id: str,
    field: PredicateField,
    *allowed: str,
    dimension_member: str | None = None,
    match: PredicateMatch = PredicateMatch.ANY,
) -> ContextPredicate:
    return ContextPredicate(
        reference_driver_id=driver_id,
        field=field,
        allowed_values=tuple(allowed),
        dimension_member=dimension_member,
        match=match,
    )


def _priority(
    driver_id: str,
    tier: VisibilityTier,
    group: EconomicGroup,
    financial: OrdinalRating,
    forward: OrdinalRating,
    explanation: OrdinalRating,
    reason: str,
    *,
    onboarding_review: bool = False,
    context_change: bool = False,
    baseline: VisibilityTier | None = None,
    current_relevance: bool = True,
    unique: bool = True,
    material: bool = True,
    history: OrdinalRating = OrdinalRating.MEDIUM,
    quality: OrdinalRating = OrdinalRating.MEDIUM,
) -> PrioritySpec:
    return PrioritySpec(
        driver_id=driver_id,
        visibility_tier=tier,
        economic_group=group,
        dimensions=_priority_levels(
            financial,
            forward,
            explanation,
            history=history,
            quality=quality,
            uniqueness=OrdinalRating.HIGH if unique else OrdinalRating.LOW,
        ),
        reason=reason,
        current_relevance=current_relevance,
        unique_explanatory_value=unique,
        material_relevance=material,
        context_aware_tier_change=context_change,
        baseline_tier=baseline,
        onboarding_review_required=onboarding_review,
    )


SHARED_SEMANTIC_RULES = (
    _rule(
        "semantic-rule:shared:qualitative-neutral@1",
        RuleMatchKind.UNIT_ID,
        "unit:core:qualitative@1",
        SemanticMode.NO_GOOD_BAD_SEMANTICS,
        SemanticAuthority.ANALYST_CURATED,
        RuleLayer.SHARED,
        "Qualitative evidence may supply context but cannot create numeric good/bad output.",
    ),
    _rule(
        "semantic-rule:shared:fail-closed@1",
        RuleMatchKind.FALLBACK,
        "*",
        SemanticMode.NO_GOOD_BAD_SEMANTICS,
        SemanticAuthority.UNRESOLVED,
        RuleLayer.SHARED,
        "Unreviewed drivers fail closed and receive no automatic good/bad semantics.",
    ),
)


RETAIL_RULES = (
    _rule("semantic-rule:retail:demand@1", RuleMatchKind.DRIVER_FAMILY, "demand", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Stable comparable/organic demand growth has a higher-is-better base semantic."),
    _rule("semantic-rule:retail:inventory@1", RuleMatchKind.DRIVER_FAMILY, "inventory", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Inventory requires demand, plan, mix, and clearance context; no arbitrary target is used."),
    _rule("semantic-rule:retail:footprint@1", RuleMatchKind.DRIVER_FAMILY, "footprint", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Store footprint changes require productivity and demand context."),
    _rule("semantic-rule:retail:channel@1", RuleMatchKind.DRIVER_FAMILY, "channel", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Channel mix is not intrinsically good or bad without economics and demand context."),
)


LOGISTICS_RULES = (
    _rule("semantic-rule:logistics:volume@1", RuleMatchKind.DRIVER_FAMILY, "volume", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Volume requires price, retention, and cost-to-serve context."),
    _rule("semantic-rule:logistics:price-mix@1", RuleMatchKind.DRIVER_FAMILY, "price-mix", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Price/mix requires volume and retention context."),
)


COMMODITY_RULES = (
    _rule("semantic-rule:commodity:production@1", RuleMatchKind.DRIVER_FAMILY, "production", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Production/volume requires unit-economics context."),
    _rule("semantic-rule:commodity:utilization@1", RuleMatchKind.DRIVER_FAMILY, "utilization", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "No accepted utilization target range exists; capacity and margin context are required."),
    _rule("semantic-rule:commodity:inputs@1", RuleMatchKind.DRIVER_FAMILY, "inputs", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Input volumes/cost context require output and unit-economics context."),
    _rule("semantic-rule:commodity:coproducts@1", RuleMatchKind.DRIVER_FAMILY, "coproducts", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Coproduct output requires realization and production context."),
    _rule("semantic-rule:commodity:margin@1", RuleMatchKind.DRIVER_FAMILY, "margin", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Comparable operating spread/margin has a higher-is-better base semantic."),
    _rule("semantic-rule:commodity:policy@1", RuleMatchKind.DRIVER_FAMILY, "policy-credit", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Policy value requires eligibility, monetization, and regime context."),
    _rule("semantic-rule:commodity:carbon@1", RuleMatchKind.DRIVER_FAMILY, "carbon", SemanticMode.NO_GOOD_BAD_SEMANTICS, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Carbon milestones are context until accepted economics are attached."),
    _rule("semantic-rule:commodity:footprint@1", RuleMatchKind.DRIVER_FAMILY, "footprint", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Plant footprint is read with capacity and unit economics."),
    _rule("semantic-rule:commodity:risk@1", RuleMatchKind.DRIVER_FAMILY, "risk-management", SemanticMode.NO_GOOD_BAD_SEMANTICS, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Risk-management commentary is not a directional numeric fact."),
)


CLOUD_RULES = (
    _rule("semantic-rule:cloud:seats@1", RuleMatchKind.DRIVER_FAMILY, "paid-seats", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Stable paid-seat population growth has a higher-is-better base semantic."),
    _rule("semantic-rule:cloud:revenue@1", RuleMatchKind.DRIVER_FAMILY, "cloud-revenue", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Comparable cloud revenue growth has a higher-is-better base semantic."),
    _rule("semantic-rule:cloud:price@1", RuleMatchKind.DRIVER_FAMILY, "price-mix", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "ARPU/mix requires seats and retention context."),
    _rule("semantic-rule:cloud:backlog@1", RuleMatchKind.DRIVER_FAMILY, "backlog", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Backlog/RPO requires duration, concentration, cancellation, and conversion context."),
    _rule("semantic-rule:cloud:margin@1", RuleMatchKind.DRIVER_FAMILY, "margin", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Comparable gross margin has a higher-is-better base semantic."),
    _rule("semantic-rule:cloud:capacity@1", RuleMatchKind.DRIVER_FAMILY, "capacity", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Capacity is read with demand and investment regime."),
    _rule("semantic-rule:cloud:capex@1", RuleMatchKind.DRIVER_FAMILY, "capex", SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.SECTOR_PACK_RULE, RuleLayer.SECTOR_PACK, "Capacity investment is not intrinsically positive or negative."),
)


def _relationship(
    relationship_id: str,
    primary: str,
    modifiers: tuple[str, ...],
    relationship_type: ContextRelationshipType,
    predicates: tuple[ContextPredicate, ...],
    effect: ContextEffectState,
    evidence: str,
    scope: str,
    *,
    final: EconomicInterpretation | None = None,
    resolves: bool = False,
    dimension: str | None = None,
    condition_kind: ContextConditionKind = ContextConditionKind.PREDICATE_SET,
    aggregate_member: str | None = None,
    financial_target: str | None = None,
) -> ContextRelationship:
    return ContextRelationship(
        relationship_id=relationship_id,
        primary_driver_id=primary,
        modifier_driver_or_accepted_product_fact=modifiers,
        relationship_type=relationship_type,
        condition=ContextCondition(
            kind=condition_kind,
            predicates=predicates,
            aggregate_dimension_member=aggregate_member,
        ),
        effect_on_interpretation=effect,
        authority=SemanticAuthority.SECTOR_PACK_RULE,
        evidence_reference=evidence,
        limitations=("No statistical causality inference.",),
        scope=scope,
        primary_dimension_member=dimension,
        final_interpretation_when_true=final,
        allows_directional_resolution=resolves,
        financial_target=financial_target,
    )


ANF_RELATIONSHIPS = (
    _relationship(
        "context:anf:total-component-comps-divergence@1",
        "driver:operating:comparable-sales@1",
        ("driver:operating:comparable-sales@1",),
        ContextRelationshipType.DIVERGENCE,
        (),
        ContextEffectState.MIXED,
        "Accepted ANF canonical total, brand, and geography comparable-sales observations.",
        "ANF comparable-sales dimensions",
        final=EconomicInterpretation.MIXED,
        dimension="member:operating-driver:total-company@1",
        condition_kind=ContextConditionKind.AGGREGATE_COMPONENT_SIGN_DIVERGENCE,
        aggregate_member="member:operating-driver:total-company@1",
        financial_target="owner:income-statement:revenue@1",
    ),
    _relationship(
        "context:anf:inventory-demand@1",
        "driver:operating:inventory-unit-growth@1",
        ("driver:operating:comparable-sales@1",),
        ContextRelationshipType.DENOMINATOR_CONTEXT,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(
                "driver:operating:comparable-sales@1",
                PredicateField.MATHEMATICAL_DIRECTION,
                "DOWN",
                dimension_member="member:operating-driver:total-company@1",
            ),
        ),
        ContextEffectState.MIXED,
        "Retail sector inventory/demand semantic pack; no arbitrary inventory target.",
        "ANF total-company inventory",
        final=EconomicInterpretation.MIXED,
        financial_target="owner:balance-sheet:inventory@1",
    ),
    _relationship(
        "context:anf:footprint-productivity@1",
        "driver:operating:company-owned-stores-end@1",
        ("driver:operating:comparable-sales@1",),
        ContextRelationshipType.PREREQUISITE,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(
                "driver:operating:comparable-sales@1",
                PredicateField.BASE_INTERPRETATION,
                "POSITIVE",
                dimension_member="member:operating-driver:total-company@1",
            ),
        ),
        ContextEffectState.CONFIRMED,
        "Retail footprint requires accepted demand/productivity context.",
        "ANF total-company footprint",
        financial_target="owner:income-statement:revenue@1",
    ),
)


PBI_RELATIONSHIPS = (
    _relationship(
        "context:pbi:presort-volume-price@1",
        "driver:operating:presort-volume-context@1",
        ("driver:operating:presort-pricing-mix-context@1", "accepted-product-fact:client-retention@1"),
        ContextRelationshipType.TRADEOFF,
        (
            _predicate("PRIMARY", PredicateField.NUMERIC_AVAILABLE, "true"),
            _predicate("driver:operating:presort-pricing-mix-context@1", PredicateField.NUMERIC_AVAILABLE, "true"),
        ),
        ContextEffectState.MIXED,
        "Logistics/service sector volume-price-retention interaction; numeric PBI history is not yet canonical.",
        "PBI Presort Services",
        financial_target="owner:segment:presort-revenue@1",
    ),
    _relationship(
        "context:pbi:sendtech-leading-lagging@1",
        "driver:operating:sendtech-activity-context@1",
        ("driver:operating:sendtech-pricing-mix-context@1",),
        ContextRelationshipType.LEADING_LAGGING_CONTEXT,
        (
            _predicate("PRIMARY", PredicateField.NUMERIC_AVAILABLE, "true"),
            _predicate("driver:operating:sendtech-pricing-mix-context@1", PredicateField.NUMERIC_AVAILABLE, "true"),
        ),
        ContextEffectState.CONFIRMED,
        "SendTech activity/bookings may lead revenue but is not a guaranteed causal forecast.",
        "PBI SendTech Solutions",
        financial_target="owner:segment:sendtech-revenue@1",
    ),
)


GPRE_RELATIONSHIPS = (
    _relationship(
        "context:gpre:production-positive-unit-economics@1",
        "driver:operating:ethanol-gallons-produced@1",
        ("driver:operating:underlying-crush-margin@1",),
        ContextRelationshipType.PREREQUISITE,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate("driver:operating:underlying-crush-margin@1", PredicateField.BASE_INTERPRETATION, "POSITIVE"),
        ),
        ContextEffectState.CONFIRMED,
        "Commodity production growth requires accepted positive comparable unit economics.",
        "GPRE production portfolio",
        final=EconomicInterpretation.POSITIVE,
        resolves=True,
        financial_target="owner:income-statement:operating-economics@1",
    ),
    _relationship(
        "context:gpre:production-negative-unit-economics@1",
        "driver:operating:ethanol-gallons-produced@1",
        ("driver:operating:underlying-crush-margin@1",),
        ContextRelationshipType.PREREQUISITE,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "DOWN"),
            _predicate("driver:operating:underlying-crush-margin@1", PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
        ),
        ContextEffectState.CONFIRMED,
        "Concurrent production and unit-economics deterioration supports a negative operating interpretation.",
        "GPRE production portfolio",
        final=EconomicInterpretation.NEGATIVE,
        resolves=True,
        financial_target="owner:income-statement:operating-economics@1",
    ),
    _relationship(
        "context:gpre:sold-volume-negative-unit-economics@1",
        "driver:operating:ethanol-gallons-sold@1",
        ("driver:operating:underlying-crush-margin@1",),
        ContextRelationshipType.PREREQUISITE,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "DOWN"),
            _predicate("driver:operating:underlying-crush-margin@1", PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
        ),
        ContextEffectState.CONFIRMED,
        "Concurrent sold-volume and unit-economics deterioration supports a negative operating interpretation.",
        "GPRE production portfolio",
        final=EconomicInterpretation.NEGATIVE,
        resolves=True,
        financial_target="owner:income-statement:operating-economics@1",
    ),
    _relationship(
        "context:gpre:utilization-margin-tradeoff@1",
        "driver:operating:ethanol-plant-utilization@1",
        ("driver:operating:underlying-crush-margin@1",),
        ContextRelationshipType.TRADEOFF,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP", "UNCHANGED"),
            _predicate("driver:operating:underlying-crush-margin@1", PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
        ),
        ContextEffectState.MIXED,
        "High/rising utilization with deteriorating unit economics is not automatically positive.",
        "GPRE production portfolio",
        final=EconomicInterpretation.MIXED,
        financial_target="owner:income-statement:operating-economics@1",
    ),
    _relationship(
        "context:gpre:policy-eligibility@1",
        "driver:operating:45z-value-realized@1",
        ("driver:operating:45z-monetization-context@1",),
        ContextRelationshipType.PREREQUISITE,
        (
            _predicate("PRIMARY", PredicateField.NUMERIC_AVAILABLE, "true"),
            _predicate("driver:operating:45z-monetization-context@1", PredicateField.NUMERIC_AVAILABLE, "true"),
        ),
        ContextEffectState.CONFIRMED,
        "Policy value requires eligibility/monetization context and remains distinct from crush and RIN.",
        "GPRE policy-credit regime",
        financial_target="owner:income-statement:policy-credit@1",
    ),
    _relationship(
        "context:gpre:input-cost-constraint@1",
        "driver:operating:corn-consumed@1",
        ("driver:operating:input-cost-context@1",),
        ContextRelationshipType.CONSTRAINT,
        (
            _predicate("PRIMARY", PredicateField.NUMERIC_AVAILABLE, "true"),
            _predicate("driver:operating:input-cost-context@1", PredicateField.NUMERIC_AVAILABLE, "true"),
        ),
        ContextEffectState.ATTENUATED,
        "Input volume interpretation is constrained by accepted corn/natural-gas economics.",
        "GPRE production portfolio",
        financial_target="owner:income-statement:cost-of-goods@1",
    ),
)


CLOUD_RELATIONSHIPS = (
    _relationship(
        "context:cloud:seats-monetization@1",
        "driver:synthetic:cloud-revenue-growth@1",
        ("driver:synthetic:paid-seats@1", "driver:synthetic:arpu-mix@1"),
        ContextRelationshipType.CONFIRMING,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate("driver:synthetic:paid-seats@1", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
        ),
        ContextEffectState.CONFIRMED,
        "Synthetic cross-sector subscription/cloud fixture.",
        "synthetic cloud control",
        final=EconomicInterpretation.POSITIVE,
    ),
    _relationship(
        "context:cloud:backlog-quality@1",
        "driver:synthetic:rpo@1",
        ("accepted-product-fact:rpo-duration@1", "accepted-product-fact:rpo-concentration@1"),
        ContextRelationshipType.PREREQUISITE,
        (_predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),),
        ContextEffectState.CONTEXT_REQUIRED,
        "Synthetic backlog quality control; absent duration/concentration must not be invented.",
        "synthetic cloud control",
    ),
    _relationship(
        "context:cloud:demand-capacity@1",
        "driver:synthetic:cloud-revenue-growth@1",
        ("driver:synthetic:capacity-constraint@1",),
        ContextRelationshipType.CONSTRAINT,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate("driver:synthetic:capacity-constraint@1", PredicateField.NUMERIC_AVAILABLE, "true"),
        ),
        ContextEffectState.ATTENUATED,
        "Synthetic demand/capacity constraint control.",
        "synthetic cloud control",
    ),
    _relationship(
        "context:cloud:margin-investment-regime@1",
        "driver:synthetic:cloud-gross-margin@1",
        ("driver:synthetic:capacity-investment@1", "driver:synthetic:capacity-constraint@1"),
        ContextRelationshipType.REGIME_MODIFIER,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "DOWN"),
            _predicate("driver:synthetic:capacity-investment@1", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
        ),
        ContextEffectState.MIXED,
        "Synthetic capacity-investment/excess-demand regime fixture.",
        "synthetic cloud control",
        final=EconomicInterpretation.MIXED,
    ),
)


ANF_BUNDLES = (
    ContextBundle("bundle:anf:demand-inventory@1", "INVENTORY_AND_DEMAND", ("driver:operating:comparable-sales@1", "driver:operating:inventory-unit-growth@1"), ("driver:operating:inventory-cost-growth@1",), (ContextRelationshipType.DENOMINATOR_CONTEXT,), "owner:balance-sheet:inventory@1", "ANF total company", "accepted profile until superseded"),
    ContextBundle("bundle:anf:total-brand-divergence@1", "SEGMENT_DIVERGENCE", ("driver:operating:comparable-sales@1",), ("driver:operating:brand-momentum-context@1",), (ContextRelationshipType.DIVERGENCE,), "owner:income-statement:revenue@1", "ANF brand/geography", "accepted profile until superseded"),
    ContextBundle("bundle:anf:footprint-productivity@1", "STORE_FOOTPRINT_AND_PRODUCTIVITY_CONTEXT", ("driver:operating:company-owned-stores-end@1",), ("driver:operating:new-stores@1", "driver:operating:closed-stores@1", "driver:operating:comparable-sales@1"), (ContextRelationshipType.PREREQUISITE, ContextRelationshipType.TRADEOFF), "owner:income-statement:revenue@1", "ANF total company", "accepted profile until superseded"),
)


PBI_BUNDLES = (
    ContextBundle("bundle:pbi:presort-volume-price-retention@1", "VOLUME_AND_UNIT_ECONOMICS", ("driver:operating:presort-volume-context@1",), ("driver:operating:presort-pricing-mix-context@1", "accepted-product-fact:client-retention@1"), (ContextRelationshipType.TRADEOFF, ContextRelationshipType.PREREQUISITE), "owner:segment:presort-revenue@1", "PBI Presort Services", "accepted qualitative profile until numeric extraction"),
    ContextBundle("bundle:pbi:sendtech-backlog-quality@1", "BACKLOG_QUALITY", ("driver:operating:sendtech-activity-context@1",), ("driver:operating:sendtech-pricing-mix-context@1",), (ContextRelationshipType.LEADING_LAGGING_CONTEXT,), "owner:segment:sendtech-revenue@1", "PBI SendTech Solutions", "accepted qualitative profile until numeric extraction"),
)


GPRE_BUNDLES = (
    ContextBundle("bundle:gpre:volume-unit-economics@1", "VOLUME_AND_UNIT_ECONOMICS", ("driver:operating:ethanol-gallons-produced@1", "driver:operating:ethanol-gallons-sold@1"), ("driver:operating:underlying-crush-margin@1",), (ContextRelationshipType.PREREQUISITE, ContextRelationshipType.CONFIRMING), "owner:income-statement:operating-economics@1", "GPRE production portfolio", "accepted profile until superseded"),
    ContextBundle("bundle:gpre:utilization-margin@1", "CAPACITY_AND_DEMAND", ("driver:operating:ethanol-plant-utilization@1",), ("driver:operating:underlying-crush-margin@1", "driver:operating:operating-plants-context@1"), (ContextRelationshipType.TRADEOFF, ContextRelationshipType.CONSTRAINT), "owner:income-statement:operating-economics@1", "GPRE production portfolio", "accepted definition-version regimes"),
    ContextBundle("bundle:gpre:policy-eligibility@1", "POLICY_VALUE_AND_ELIGIBILITY", ("driver:operating:45z-value-realized@1",), ("driver:operating:45z-monetization-context@1",), (ContextRelationshipType.PREREQUISITE, ContextRelationshipType.REGIME_MODIFIER), "owner:income-statement:policy-credit@1", "GPRE policy regime", "accepted 45Z regime until superseded"),
)


CLOUD_BUNDLES = (
    ContextBundle("bundle:cloud:seats-monetization@1", "DEMAND_AND_MONETIZATION", ("driver:synthetic:cloud-revenue-growth@1",), ("driver:synthetic:paid-seats@1", "driver:synthetic:arpu-mix@1"), (ContextRelationshipType.CONFIRMING,), "synthetic:cloud-revenue", "synthetic control only", "test fixture"),
    ContextBundle("bundle:cloud:backlog-quality@1", "BACKLOG_QUALITY", ("driver:synthetic:rpo@1",), ("accepted-product-fact:rpo-duration@1", "accepted-product-fact:rpo-concentration@1"), (ContextRelationshipType.PREREQUISITE,), "synthetic:cloud-revenue", "synthetic control only", "test fixture"),
    ContextBundle("bundle:cloud:demand-capacity@1", "CAPACITY_AND_DEMAND", ("driver:synthetic:cloud-revenue-growth@1",), ("driver:synthetic:capacity-constraint@1",), (ContextRelationshipType.CONSTRAINT,), "synthetic:cloud-revenue", "synthetic control only", "test fixture"),
    ContextBundle("bundle:cloud:margin-investment@1", "MARGIN_AND_INVESTMENT_REGIME", ("driver:synthetic:cloud-gross-margin@1",), ("driver:synthetic:capacity-investment@1",), (ContextRelationshipType.REGIME_MODIFIER,), "synthetic:cloud-margin", "synthetic control only", "test fixture"),
)


ANF_TICKER_RULES = (
    _rule(
        "semantic-rule:anf:brand-context-neutral@1",
        RuleMatchKind.DRIVER_ID,
        "driver:operating:brand-momentum-context@1",
        SemanticMode.NO_GOOD_BAD_SEMANTICS,
        SemanticAuthority.TICKER_PROFILE_RULE,
        RuleLayer.TICKER_PROFILE,
        "Qualitative brand commentary may modify divergence but is not a numeric demand observation.",
    ),
    _rule(
        "semantic-rule:anf:store-digital-context-neutral@1",
        RuleMatchKind.DRIVER_ID,
        "driver:operating:store-digital-activity-context@1",
        SemanticMode.NO_GOOD_BAD_SEMANTICS,
        SemanticAuthority.TICKER_PROFILE_RULE,
        RuleLayer.TICKER_PROFILE,
        "Qualitative store/digital activity is context rather than an owned numeric channel signal.",
    ),
)


ANF_PRIORITIES = (
    _priority("driver:operating:comparable-sales@1", VisibilityTier.CORE_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Primary demand signal; brand/geography divergence adds unique explanatory value.", context_change=True, baseline=VisibilityTier.CORE_DRIVER, history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:inventory-unit-growth@1", VisibilityTier.CORE_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Inventory units complete the demand/inventory context bundle.", onboarding_review=True, context_change=True, baseline=VisibilityTier.SECONDARY_DRIVER),
    _priority("driver:operating:inventory-cost-growth@1", VisibilityTier.CORE_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.MEDIUM, OrdinalRating.HIGH, "Inventory cost growth distinguishes units from cost/mix effects.", onboarding_review=True, context_change=True, baseline=VisibilityTier.SECONDARY_DRIVER),
    _priority("driver:operating:company-owned-stores-end@1", VisibilityTier.CORE_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Ending store footprint is the durable capacity state and requires productivity context.", onboarding_review=True),
    _priority("driver:operating:digital-sales-mix@1", VisibilityTier.CORE_DRIVER, EconomicGroup.PRICE_MIX, OrdinalRating.MEDIUM, OrdinalRating.HIGH, OrdinalRating.HIGH, "Channel mix is strategically relevant and context-dependent rather than intrinsically positive.", onboarding_review=True),
    _priority("driver:operating:new-stores@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.MEDIUM, OrdinalRating.HIGH, OrdinalRating.MEDIUM, "Opening activity explains footprint change but is not sufficient alone."),
    _priority("driver:operating:closed-stores@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Closures qualify footprint change and may reflect optimization or deterioration."),
    _priority("driver:operating:right-sized-stores@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Right-sizing is material context but does not independently encode good/bad meaning.", onboarding_review=True),
    _priority("driver:operating:remodeled-stores@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.LOW, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Remodels are a watch signal for footprint investment."),
    _priority("driver:operating:company-owned-stores-start@1", VisibilityTier.SUPPORT_ONLY, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.LOW, OrdinalRating.LOW, OrdinalRating.LOW, "Opening state supports footprint reconciliation and is redundant for visible priority."),
    _priority("driver:operating:franchise-stores@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.LOW, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Franchise footprint is relevant but secondary to company-owned economics."),
    _priority("driver:operating:total-stores-including-franchise@1", VisibilityTier.SUPPORT_ONLY, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.LOW, OrdinalRating.LOW, OrdinalRating.LOW, "Total footprint is derivational support where ownership components are visible."),
    _priority("driver:operating:inventory-unit-growth-erp-points@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "ERP effect is a temporary regime modifier, not a durable core KPI.", onboarding_review=True),
    _priority("driver:operating:brand-momentum-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.LEADING_INDICATORS, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Qualitative brand context may explain divergence but cannot create numeric output.", material=False),
    _priority("driver:operating:store-digital-activity-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.LEADING_INDICATORS, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Qualitative store/digital context supports the channel bundle.", material=False),
)


PBI_PRIORITIES = (
    _priority("driver:operating:presort-volume-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Presort volume is a high-value extraction candidate but has no canonical numeric history today.", onboarding_review=True, history=OrdinalRating.LOW, quality=OrdinalRating.LOW),
    _priority("driver:operating:presort-pricing-mix-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.PRICE_MIX, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Price/mix is necessary to interpret Presort volume and retention economics.", onboarding_review=True, history=OrdinalRating.LOW, quality=OrdinalRating.LOW),
    _priority("driver:operating:sendtech-activity-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.LEADING_INDICATORS, OrdinalRating.MEDIUM, OrdinalRating.HIGH, OrdinalRating.HIGH, "SendTech bookings/activity is a high-value leading-indicator extraction candidate.", onboarding_review=True, history=OrdinalRating.LOW, quality=OrdinalRating.LOW),
    _priority("driver:operating:sendtech-pricing-mix-context@1", VisibilityTier.SUPPORT_ONLY, EconomicGroup.PRICE_MIX, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Qualitative pricing/mix context supports later SendTech extraction.", onboarding_review=True, material=False, history=OrdinalRating.LOW, quality=OrdinalRating.LOW),
)


GPRE_PRIORITIES = (
    _priority("driver:operating:ethanol-gallons-produced@1", VisibilityTier.CORE_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Production volume is core only together with accepted unit economics.", onboarding_review=True, context_change=True, baseline=VisibilityTier.SECONDARY_DRIVER, history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:ethanol-gallons-sold@1", VisibilityTier.CORE_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Sold volume connects production to realized operating economics.", onboarding_review=True, history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:ethanol-plant-utilization@1", VisibilityTier.CORE_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Utilization is material but requires definition-version, capacity, and margin context.", onboarding_review=True, history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:corn-consumed@1", VisibilityTier.CORE_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Corn throughput connects operating scale to input economics.", history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:underlying-crush-margin@1", VisibilityTier.CORE_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Underlying crush margin is the key comparable unit-economics modifier.", onboarding_review=True, history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:consolidated-ethanol-crush-margin@1", VisibilityTier.CORE_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.MEDIUM, OrdinalRating.HIGH, "Consolidated crush economics reconcile underlying operations plus distinct policy components.", history=OrdinalRating.HIGH, quality=OrdinalRating.HIGH),
    _priority("driver:operating:45z-value-realized@1", VisibilityTier.CORE_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "45Z is material policy value but remains separate from crush and RIN and needs eligibility context.", onboarding_review=True),
    _priority("driver:operating:renewable-corn-oil-produced@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Renewable corn oil provides material coproduct context."),
    _priority("driver:operating:distillers-grains-produced@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Distillers grains provides material coproduct context."),
    _priority("driver:operating:ultra-high-protein-produced@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.DEMAND_VOLUME, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Ultra-high protein output supports coproduct mix analysis."),
    _priority("driver:operating:crush-margin-ex-45z@1", VisibilityTier.SECONDARY_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.MEDIUM, OrdinalRating.HIGH, "Ex-45Z margin isolates policy value from operations."),
    _priority("driver:operating:crush-margin-ex-rin@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.MEDIUM, OrdinalRating.LOW, OrdinalRating.MEDIUM, "Ex-RIN margin is useful decomposition support but sparse."),
    _priority("driver:operating:rin-impact@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.MEDIUM, OrdinalRating.LOW, OrdinalRating.MEDIUM, "RIN impact remains distinct policy/economic support."),
    _priority("driver:operating:45z-monetization-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.LEADING_INDICATORS, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Eligibility and monetization context is required to interpret 45Z.", onboarding_review=True, material=False),
    _priority("driver:operating:carbon-capture-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.LEADING_INDICATORS, OrdinalRating.MEDIUM, OrdinalRating.HIGH, OrdinalRating.MEDIUM, "CCS/CI milestones are strategically relevant context, not numeric economics."),
    _priority("driver:operating:input-cost-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.COSTS_UNIT_ECONOMICS, OrdinalRating.HIGH, OrdinalRating.HIGH, OrdinalRating.HIGH, "Corn and natural-gas context constrains throughput interpretation.", material=False),
    _priority("driver:operating:coproduct-context@1", VisibilityTier.SUPPORT_ONLY, EconomicGroup.OTHER_MATERIAL_DRIVER, OrdinalRating.LOW, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Qualitative coproduct context supports numeric output series.", material=False),
    _priority("driver:operating:operating-plants-context@1", VisibilityTier.WATCH_DRIVER, EconomicGroup.CAPACITY_UTILIZATION, OrdinalRating.MEDIUM, OrdinalRating.HIGH, OrdinalRating.MEDIUM, "Operating-plant scope is needed to interpret utilization definition regimes.", onboarding_review=True, material=False),
    _priority("driver:operating:risk-management-context@1", VisibilityTier.SUPPORT_ONLY, EconomicGroup.OTHER_MATERIAL_DRIVER, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, OrdinalRating.MEDIUM, "Risk-management commentary is support context and not a causal numeric owner.", material=False),
)


SECTOR_SEMANTIC_PACKS = {
    "sector-pack:retail@1": SectorSemanticPack("sector-pack:retail@1", RETAIL_RULES, (), ()),
    "sector-pack:mail-logistics-service@1": SectorSemanticPack("sector-pack:mail-logistics-service@1", LOGISTICS_RULES, (), ()),
    "sector-pack:commodity-industrial@1": SectorSemanticPack("sector-pack:commodity-industrial@1", COMMODITY_RULES, (), ()),
    "sector-pack:subscription-cloud@1": SectorSemanticPack("sector-pack:subscription-cloud@1", CLOUD_RULES, CLOUD_RELATIONSHIPS, CLOUD_BUNDLES),
}


def _ticker_relationships(
    relationships: tuple[ContextRelationship, ...],
) -> tuple[ContextRelationship, ...]:
    return tuple(
        dataclasses.replace(item, authority=SemanticAuthority.TICKER_PROFILE_RULE)
        for item in relationships
    )


TICKER_SEMANTIC_PROFILES = {
    "ANF": TickerSemanticProfile("ANF", "sector-pack:retail@1", ANF_TICKER_RULES, _ticker_relationships(ANF_RELATIONSHIPS), ANF_BUNDLES, ANF_PRIORITIES, "operating-driver-semantic-profile:anf@1"),
    "PBI": TickerSemanticProfile("PBI", "sector-pack:mail-logistics-service@1", (), _ticker_relationships(PBI_RELATIONSHIPS), PBI_BUNDLES, PBI_PRIORITIES, "operating-driver-semantic-profile:pbi@1"),
    "GPRE": TickerSemanticProfile("GPRE", "sector-pack:commodity-industrial@1", (), _ticker_relationships(GPRE_RELATIONSHIPS), GPRE_BUNDLES, GPRE_PRIORITIES, "operating-driver-semantic-profile:gpre@1"),
}


DEFAULT_SEMANTIC_CONFIGURATION = SemanticConfiguration(
    shared_rules=SHARED_SEMANTIC_RULES,
    sector_packs=SECTOR_SEMANTIC_PACKS,
    ticker_profiles=TICKER_SEMANTIC_PROFILES,
)


DATA_CLEANUP_PRIORITIES = {
    "ANF": (
        {"cleanup_id": "cleanup:anf:period-conflicts@1", "count": 36, "priority": "HIGH", "impact": ["ANALYTICS_COVERAGE", "SEMANTIC_INTERPRETATION", "CORE_VISIBILITY"], "disposition": "BOUNDED_SOURCE_CLEANUP_CANDIDATE"},
        {"cleanup_id": "cleanup:anf:guidance-references@1", "count": 63, "priority": "MEDIUM", "impact": ["GUIDANCE_COMPARISON", "FORECAST_EVIDENCE"], "disposition": "GUIDANCE_NORMALIZATION_CANDIDATE"},
    ),
    "PBI": (
        {"cleanup_id": "cleanup:pbi:numeric-kpi-history@1", "count": 1, "priority": "HIGH", "impact": ["ANALYTICS_COVERAGE", "CORE_VISIBILITY", "SEMANTIC_INTERPRETATION", "FORECAST_EVIDENCE"], "disposition": "BOUNDED_EXTRACTION_CANDIDATE"},
        {"cleanup_id": "cleanup:pbi:guidance-references@1", "count": 3, "priority": "MEDIUM", "impact": ["GUIDANCE_COMPARISON"], "disposition": "GUIDANCE_NORMALIZATION_CANDIDATE"},
    ),
    "GPRE": (
        {"cleanup_id": "cleanup:gpre:45z-quarter-gaps@1", "count": 2, "priority": "MEDIUM", "impact": ["TTM_FY_ANALYTICS", "GUIDANCE_COMPARISON"], "disposition": "RETAIN_FAIL_CLOSED_UNTIL_SOURCE_BACKED"},
        {"cleanup_id": "cleanup:gpre:guidance-references@1", "count": 5, "priority": "MEDIUM", "impact": ["GUIDANCE_COMPARISON", "FORECAST_EVIDENCE"], "disposition": "GUIDANCE_NORMALIZATION_CANDIDATE"},
    ),
}


PBI_EXTRACTION_PRIORITIES = (
    {"rank": 1, "candidate": "PRESORT_VOLUME", "product_value": "HIGH", "reason": "Core demand/throughput signal and required side of the price-volume-retention bundle."},
    {"rank": 2, "candidate": "PRESORT_PRICE_MIX_REVENUE_PER_PIECE", "product_value": "HIGH", "reason": "Required to interpret volume and monetization jointly."},
    {"rank": 3, "candidate": "CLIENT_RETENTION_LOSS", "product_value": "HIGH", "reason": "Prevents pricing improvement from masking customer deterioration."},
    {"rank": 4, "candidate": "COST_TO_SERVE", "product_value": "HIGH", "reason": "Completes incremental economics for volume."},
    {"rank": 5, "candidate": "SENDTECH_BOOKINGS_BACKLOG", "product_value": "MEDIUM", "reason": "Potential leading indicator requiring backlog-quality context."},
    {"rank": 6, "candidate": "MAILING_INSTALL_BASE", "product_value": "MEDIUM", "reason": "Supports installed-base and migration analysis."},
)

EXTRACTION_PRIORITIES = {
    "ANF": (),
    "PBI": PBI_EXTRACTION_PRIORITIES,
    "GPRE": (),
}


def _base_signals(
    registry: ShadowRegistryPackage,
    analytics: DerivedAnalyticsPackage,
    configuration: SemanticConfiguration,
    profile: TickerSemanticProfile,
    sector_pack: SectorSemanticPack,
) -> tuple[BaseSemanticSignal, ...]:
    definitions = {
        (item.driver_id, item.definition_version): item
        for item in registry.profile.definitions
    }
    latest_by_key = {
        (item.driver_id, item.definition_version, item.dimension_set_id): item
        for item in analytics.latest_states
    }
    result: list[BaseSemanticSignal] = []
    for signal in sorted(analytics.analytical_signals, key=lambda item: item.signal_id):
        definition = definitions[(signal.driver_id, signal.definition_version)]
        rule = resolve_semantic_rule(
            definition,
            shared_rules=configuration.shared_rules,
            sector_rules=sector_pack.rules,
            ticker_rules=profile.rules,
        )
        direction, basis, qoq, yoy, trend, definition_break = _direction_for_signal(
            signal, analytics
        )
        latest = latest_by_key.get(
            (signal.driver_id, signal.definition_version, signal.dimension_set_id)
        )
        history = latest.comparable_history_depth if latest else 0
        interpretation = derive_base_interpretation(
            semantic_mode=rule.semantic_mode,
            mathematical_direction=direction,
            latest_value=signal.latest_value,
            target_range=rule.target_range,
            definition_break_present=definition_break,
        )
        lineage = tuple(
            sorted(
                {
                    *signal.latest_source_evidence_ids,
                    *signal.qualitative_attachment_ids,
                }
            )
        )
        numeric_output = signal.latest_value is not None
        if definition.unit_id == "unit:core:qualitative@1" and numeric_output:
            raise SemanticPriorityError("Qualitative evidence became numeric semantic output.")
        result.append(
            BaseSemanticSignal(
                semantic_signal_id=_hash_id("semantic-signal", signal.signal_id, rule.rule_id),
                source_analytical_signal_id=signal.signal_id,
                ticker=signal.ticker,
                driver_id=signal.driver_id,
                definition_version=signal.definition_version,
                dimension_set_id=signal.dimension_set_id,
                dimensions=signal.dimensions,
                latest_value=signal.latest_value,
                mathematical_direction=direction,
                direction_basis=basis,
                mathematical_momentum=signal.acceleration_state,
                semantic_mode=rule.semantic_mode,
                base_interpretation=interpretation,
                interpretation_basis=(
                    f"RULE:{rule.rule_id}",
                    f"DIRECTION:{direction.value}",
                    f"DIRECTION_BASIS:{basis}",
                ),
                semantic_authority=rule.authority,
                semantic_rule_id=rule.rule_id,
                target_range=rule.target_range,
                definition_break_present=definition_break,
                numeric_semantic_output=numeric_output,
                source_lineage_ids=lineage,
                financial_linkage=signal.financial_linkage,
                forecast_capability=signal.forecast_capability,
                qoq_available=qoq,
                yoy_available=yoy,
                trend_available=trend,
                comparable_history_depth=history,
                lower_layer_analytics_sha256=analytics.sha256,
                validity_window=(
                    f"{rule.valid_from or 'OPEN'}..{rule.valid_to or 'OPEN'}"
                ),
            )
        )
    return tuple(result)


def build_context_semantic_priority(
    registry: ShadowRegistryPackage,
    analytics: DerivedAnalyticsPackage,
    *,
    configuration: SemanticConfiguration = DEFAULT_SEMANTIC_CONFIGURATION,
) -> SemanticPriorityPackage:
    """Build semantics/priorities without changing registry or analytics inputs."""

    if analytics.registry_package_sha256 != registry.sha256:
        raise SemanticPriorityError("Analytics package does not consume this registry identity.")
    before_registry = registry.serialize()
    before_analytics = analytics.serialize()
    ticker = registry.profile.ticker
    if analytics.ticker != ticker:
        raise SemanticPriorityError("Registry and analytics ticker identities differ.")
    profile = configuration.profile(ticker)
    sector_pack = configuration.sector_pack(profile)
    relationships = tuple(
        sorted((*sector_pack.relationships, *profile.relationships), key=lambda item: item.relationship_id)
    )
    bundles = tuple(
        sorted((*sector_pack.bundles, *profile.bundles), key=lambda item: item.bundle_id)
    )
    bases = _base_signals(registry, analytics, configuration, profile, sector_pack)
    contexts = tuple(
        sorted(
            (
                apply_context_relationships(item, bases, relationships)
                for item in bases
            ),
            key=lambda item: item.semantic_signal_id,
        )
    )
    priorities = _build_priority_records(registry, profile, bases, contexts)
    definitions = {
        item.driver_id: item
        for item in sorted(
            registry.profile.definitions,
            key=lambda item: (item.driver_id, item.definition_version),
        )
    }
    resolved_rules = {
        item.driver_id: resolve_semantic_rule(
            item,
            shared_rules=configuration.shared_rules,
            sector_rules=sector_pack.rules,
            ticker_rules=profile.rules,
        )
        for item in definitions.values()
    }
    by_driver_base: dict[str, list[BaseSemanticSignal]] = defaultdict(list)
    for item in bases:
        by_driver_base[item.driver_id].append(item)
    priority_by_driver = {item.driver_id: item for item in priorities}
    readiness = tuple(
        _profile_readiness(
            definitions[driver_id],
            resolved_rules[driver_id],
            priority_by_driver[driver_id],
            by_driver_base[driver_id],
            relationships,
        )
        for driver_id in sorted(definitions)
    )
    product_readiness = (
        ProfileProductReadiness.OPERATING_DRIVERS_PROFILE_REVIEW_REQUIRED
        if any(
            item.state
            in {
                ProfileDriverReadinessState.PROFILE_NEEDS_REVIEW,
                ProfileDriverReadinessState.PROFILE_BLOCKED,
            }
            for item in readiness
        )
        else ProfileProductReadiness.OPERATING_DRIVERS_PROFILE_READY
    )
    rules = tuple(
        sorted(
            {item.rule_id: item for item in resolved_rules.values()}.values(),
            key=lambda item: item.rule_id,
        )
    )
    package = SemanticPriorityPackage(
        ticker=ticker,
        registry_package_sha256=registry.sha256,
        derived_analytics_sha256=analytics.sha256,
        profile_version=profile.accepted_profile_version,
        sector_pack_id=profile.sector_pack_id,
        semantic_rules=rules,
        context_relationships=relationships,
        context_bundles=bundles,
        base_semantic_signals=bases,
        contextual_semantic_signals=contexts,
        driver_priorities=priorities,
        profile_readiness=readiness,
        profile_product_readiness=product_readiness,
        data_cleanup_priorities=DATA_CLEANUP_PRIORITIES.get(ticker, ()),
        extraction_priorities=EXTRACTION_PRIORITIES.get(ticker, ()),
    )
    if registry.serialize() != before_registry or analytics.serialize() != before_analytics:
        raise SemanticPriorityError("Semantic construction mutated a lower-layer package.")
    return package


def combined_semantic_priority_digest(
    packages: Iterable[SemanticPriorityPackage],
) -> str:
    payload = serialize_package(
        {
            "contract_version": OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION,
            "package_hashes": {
                item.ticker: item.sha256
                for item in sorted(packages, key=lambda package: package.ticker)
            },
        }
    )
    return hashlib.sha256(payload).hexdigest()


def interpretation_counts(
    records: Iterable[ContextualSemanticSignal],
) -> dict[str, int]:
    counter = Counter(item.final_interpretation.value for item in records)
    return {
        item.value: counter.get(item.value, 0) for item in EconomicInterpretation
    }


def visibility_counts(records: Iterable[DriverPriority]) -> dict[str, int]:
    counter = Counter(item.visibility_tier.value for item in records)
    return {item.value: counter.get(item.value, 0) for item in VisibilityTier}


def semantic_contracts() -> dict[str, Any]:
    """Return the durable closed contracts consumed by audit/registry clients."""

    return {
        "analyst_override": {
            "allowed_fields": ["semantic_mode", "visibility_tier", "context_rule", "target_range"],
            "historical_observation_mutation_allowed": False,
            "requirements": ["declarative", "versioned", "traceable", "reasoned", "effective_dated"],
        },
        "context_relationship": {
            "contract_version": OPERATING_DRIVER_CONTEXT_RELATIONSHIP_CONTRACT_VERSION,
            "effect_states": [item.value for item in ContextEffectState],
            "relationship_types": [item.value for item in ContextRelationshipType],
            "workbook_coordinates": False,
        },
        "new_ticker_onboarding": {
            "automatic_stages": ["SOURCE_CENSUS", "CANDIDATE_DRIVER_EXTRACTION", "OWNER_FILTER", "CANONICAL_MAPPING_CANDIDATES", "SECTOR_PACK_MATCH", "MATERIALITY_FILTER", "ANALYTICS"],
            "contract_version": OPERATING_DRIVER_NEW_TICKER_PROFILE_CONTRACT_VERSION,
            "review_stages": ["MATERIAL_UNRESOLVED_SEMANTIC_PROFILE", "CONTEXT_DEPENDENCY_REVIEW", "TICKER_PROFILE_ACCEPTANCE"],
            # Explicit ordinals survive the repository serializer, which
            # intentionally sorts plain string arrays as set-like values.
            "sequence": [
                {"ordinal": ordinal, "stage": stage}
                for ordinal, stage in enumerate(
                    (
                        "SOURCE_CENSUS",
                        "CANDIDATE_DRIVER_EXTRACTION",
                        "OWNER_FILTER",
                        "CANONICAL_MAPPING",
                        "SECTOR_PACK_MATCH",
                        "MATERIALITY_FILTER",
                        "SEMANTIC_PROFILE",
                        "CONTEXT_DEPENDENCY_REVIEW",
                        "TICKER_PROFILE",
                        "ANALYTICS",
                        "SHADOW_ACCEPTANCE",
                        "INVESTOR_PRODUCT",
                    ),
                    start=1,
                )
            ],
            "unresolved_material_result": "NEW_TICKER_DRIVER_PROFILE_NEEDS_REVIEW",
        },
        "priority": {
            "contract_version": OPERATING_DRIVER_DRIVER_PRIORITY_CONTRACT_VERSION,
            "dimensions": [item.name for item in dataclasses.fields(PriorityDimensions)],
            "hidden_weighted_score": False,
            "lexicographic_tie_break": [
                {"dimension": dimension, "ordinal": ordinal}
                for ordinal, dimension in enumerate(
                    (
                        "FINANCIAL_MATERIALITY",
                        "FORWARD_RELEVANCE",
                        "EXPLANATORY_USEFULNESS",
                        "MANAGEMENT_EMPHASIS",
                        "DISCLOSURE_CONTINUITY",
                        "DATA_QUALITY",
                        "HISTORICAL_DEPTH",
                    ),
                    start=1,
                )
            ],
            "visibility_tiers": [item.value for item in VisibilityTier],
        },
        "semantic_trend": {
            "authority_states": [item.value for item in SemanticAuthority],
            "contract_version": OPERATING_DRIVER_SEMANTIC_TREND_CONTRACT_VERSION,
            "final_states": [item.value for item in EconomicInterpretation],
            "mathematical_direction_is_not_good_bad": True,
            "rule_resolution_order": [
                {"layer": layer, "ordinal": ordinal}
                for ordinal, layer in enumerate(
                    (
                        "SHARED_ENGINE",
                        "SECTOR_SEMANTIC_PACK",
                        "DECLARATIVE_TICKER_PROFILE",
                    ),
                    start=1,
                )
            ],
            "semantic_modes": [item.value for item in SemanticMode],
        },
        "target_range": {
            "fallback_without_bounds": "CONTEXT_DEPENDENT",
            "required_fields": ["lower_bound", "upper_bound", "unit_id", "scope", "validity", "authority_reference"],
            "unsupported_target_range_allowed": False,
        },
    }
