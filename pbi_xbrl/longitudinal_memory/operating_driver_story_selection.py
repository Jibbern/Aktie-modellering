"""Orthogonal Operating Driver product selection and investor-story contracts.

This module consumes the accepted context-aware semantic package and derived
analytics without mutating either input.  Economic importance, monitoring
status, data readiness, and display role are independent declarative axes.
The deprecated combined visibility tier is retained only in an explicit
migration receipt.
"""
from __future__ import annotations

import dataclasses
import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping

from .operating_driver_derived_analytics import DerivedAnalyticsPackage
from .operating_driver_semantic_priority import (
    ColoringReadiness,
    EconomicGroup,
    EconomicInterpretation,
    SemanticPriorityPackage,
)
from .serialization import serialize_package


OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION = (
    "operating-drivers-orthogonal-story-selection@1"
)
OPERATING_DRIVER_INVESTOR_STORY_CONTRACT_VERSION = (
    "operating-drivers-investor-story@1"
)
OPERATING_DRIVER_LEGACY_TIER_MIGRATION_CONTRACT_VERSION = (
    "operating-drivers-legacy-visibility-tier-migration@1"
)
OPERATING_DRIVER_NEW_TICKER_SELECTION_CONTRACT_VERSION = (
    "operating-drivers-new-ticker-product-selection@1"
)


class StorySelectionError(ValueError):
    """Raised when product selection would cross an accepted boundary."""


class EconomicImportance(str, Enum):
    KEY_DRIVER = "KEY_DRIVER"
    MATERIAL_DRIVER = "MATERIAL_DRIVER"
    SUPPORTING_DRIVER = "SUPPORTING_DRIVER"
    UNRESOLVED = "UNRESOLVED"


class MonitoringStatus(str, Enum):
    NORMAL = "NORMAL"
    WATCH = "WATCH"
    EMERGING = "EMERGING"
    RESOLVING = "RESOLVING"
    RETIRED = "RETIRED"


class DataReadiness(str, Enum):
    READY_NUMERIC = "READY_NUMERIC"
    READY_QUALITATIVE = "READY_QUALITATIVE"
    NEEDS_DATA = "NEEDS_DATA"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    NOT_COMPARABLE = "NOT_COMPARABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class DisplayRole(str, Enum):
    PRIMARY = "PRIMARY"
    CONTEXT = "CONTEXT"
    DIAGNOSTIC = "DIAGNOSTIC"
    DEFINITION_SUPPORT = "DEFINITION_SUPPORT"
    HIDDEN_SUPPORT = "HIDDEN_SUPPORT"


class ProductReviewState(str, Enum):
    NO_REVIEW_REQUIRED = "NO_REVIEW_REQUIRED"
    PRODUCT_REVIEW_REQUIRED = "PRODUCT_REVIEW_REQUIRED"


class UIReadiness(str, Enum):
    READY_FOR_INVESTOR_UI_PREVIEW = "READY_FOR_INVESTOR_UI_PREVIEW"
    READY_FOR_UI_WITH_VISIBLE_DATA_GAPS = "READY_FOR_UI_WITH_VISIBLE_DATA_GAPS"
    NOT_READY_NEEDS_PRODUCT_REVIEW = "NOT_READY_NEEDS_PRODUCT_REVIEW"


@dataclass(frozen=True, slots=True)
class AnalyticsReadiness:
    latest_ready: bool
    qoq_ready: bool
    yoy_ready: bool
    trend_4q_ready: bool
    sparkline_12q_ready: bool
    ttm_ready: bool
    semantic_ready: bool

    def to_dict(self) -> dict[str, bool]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class DriverSelectionSpec:
    driver_id: str
    economic_importance: EconomicImportance
    monitoring_status: MonitoringStatus
    display_role: DisplayRole
    story_id: str
    importance_reason: str
    selection_reason: str
    data_readiness_override: DataReadiness | None = None
    monitoring_reason: str = "Ordinary accepted analytical regime."
    monitoring_authority: str = "DECLARATIVE_TICKER_PROFILE"
    monitoring_effective_period: str = "OPEN"
    monitoring_review_condition: str | None = None
    onboarding_review_required: bool = False

    def __post_init__(self) -> None:
        if not all(
            (
                self.driver_id,
                self.story_id,
                self.importance_reason,
                self.selection_reason,
                self.monitoring_reason,
                self.monitoring_authority,
                self.monitoring_effective_period,
            )
        ):
            raise StorySelectionError("Driver selections require explicit reasoning.")
        if self.monitoring_status is not MonitoringStatus.NORMAL:
            if not self.monitoring_review_condition:
                raise StorySelectionError(
                    "Every non-NORMAL monitoring state requires a review condition."
                )
        if (
            self.economic_importance is EconomicImportance.UNRESOLVED
            and not self.onboarding_review_required
        ):
            raise StorySelectionError(
                "UNRESOLVED economic importance must enter product review."
            )

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["economic_importance"] = self.economic_importance.value
        result["monitoring_status"] = self.monitoring_status.value
        result["display_role"] = self.display_role.value
        result["data_readiness_override"] = (
            self.data_readiness_override.value
            if self.data_readiness_override is not None
            else None
        )
        return result


@dataclass(frozen=True, slots=True)
class InvestorStorySpec:
    story_id: str
    economic_question: str
    economic_group: EconomicGroup
    primary_drivers: tuple[str, ...]
    context_drivers: tuple[str, ...] = ()
    diagnostic_drivers: tuple[str, ...] = ()
    definition_support_drivers: tuple[str, ...] = ()
    hidden_support_drivers: tuple[str, ...] = ()
    context_bundle_ids: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()
    selection_reasoning: str = ""

    def __post_init__(self) -> None:
        if not self.story_id or not self.economic_question or not self.primary_drivers:
            raise StorySelectionError("Investor stories require identity, question, and PRIMARY.")
        if not self.selection_reasoning:
            raise StorySelectionError("Investor stories require explicit selection reasoning.")
        all_drivers = self.all_driver_ids
        if len(all_drivers) != len(set(all_drivers)):
            raise StorySelectionError("One driver cannot hold two roles in one story.")

    @property
    def all_driver_ids(self) -> tuple[str, ...]:
        return (
            *self.primary_drivers,
            *self.context_drivers,
            *self.diagnostic_drivers,
            *self.definition_support_drivers,
            *self.hidden_support_drivers,
        )

    def role_for(self, driver_id: str) -> DisplayRole:
        role_sets = (
            (DisplayRole.PRIMARY, self.primary_drivers),
            (DisplayRole.CONTEXT, self.context_drivers),
            (DisplayRole.DIAGNOSTIC, self.diagnostic_drivers),
            (DisplayRole.DEFINITION_SUPPORT, self.definition_support_drivers),
            (DisplayRole.HIDDEN_SUPPORT, self.hidden_support_drivers),
        )
        for role, values in role_sets:
            if driver_id in values:
                return role
        raise StorySelectionError(f"Driver {driver_id!r} is not in story {self.story_id!r}.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "context_bundle_ids": list(self.context_bundle_ids),
            "context_drivers": list(self.context_drivers),
            "definition_support_drivers": list(self.definition_support_drivers),
            "diagnostic_drivers": list(self.diagnostic_drivers),
            "economic_group": self.economic_group.value,
            "economic_owner": False,
            "economic_question": self.economic_question,
            "hidden_support_drivers": list(self.hidden_support_drivers),
            "limitations": list(self.limitations),
            "primary_drivers": list(self.primary_drivers),
            "selection_reasoning": self.selection_reasoning,
            "story_id": self.story_id,
        }


@dataclass(frozen=True, slots=True)
class OnboardingBurden:
    canonical_driver_count: int
    economic_importance_automatic: int
    semantic_mode_automatic: int
    context_relationships_automatically_reusable: int
    display_role_automatically_proposed: int
    human_or_codex_review_required: int

    def __post_init__(self) -> None:
        for field in dataclasses.fields(self):
            if getattr(self, field.name) < 0:
                raise StorySelectionError("Onboarding burden counts cannot be negative.")
        for field_name in (
            "economic_importance_automatic",
            "semantic_mode_automatic",
            "context_relationships_automatically_reusable",
            "display_role_automatically_proposed",
            "human_or_codex_review_required",
        ):
            if getattr(self, field_name) > self.canonical_driver_count:
                raise StorySelectionError("Onboarding burden exceeds canonical universe.")

    def to_dict(self) -> dict[str, int]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class StorySelectionProfile:
    ticker: str
    profile_version: str
    selections: tuple[DriverSelectionSpec, ...]
    stories: tuple[InvestorStorySpec, ...]
    onboarding_burden: OnboardingBurden
    ui_readiness: UIReadiness

    def __post_init__(self) -> None:
        if not self.ticker or not self.profile_version:
            raise StorySelectionError("Selection profiles require ticker and version.")
        selection_ids = [item.driver_id for item in self.selections]
        if len(selection_ids) != len(set(selection_ids)):
            raise StorySelectionError("Selection profile contains duplicate driver identity.")
        story_ids = [item.story_id for item in self.stories]
        if len(story_ids) != len(set(story_ids)):
            raise StorySelectionError("Selection profile contains duplicate story identity.")
        story_drivers = [driver for story in self.stories for driver in story.all_driver_ids]
        if Counter(story_drivers) != Counter(selection_ids):
            raise StorySelectionError(
                "Every canonical driver must occur exactly once in story compression."
            )
        specs = {item.driver_id: item for item in self.selections}
        for story in self.stories:
            for driver_id in story.all_driver_ids:
                spec = specs[driver_id]
                if spec.story_id != story.story_id:
                    raise StorySelectionError("Selection/story membership differs.")
                if spec.display_role is not story.role_for(driver_id):
                    raise StorySelectionError("Selection/story display role differs.")
        if self.onboarding_burden.canonical_driver_count != len(self.selections):
            raise StorySelectionError("Onboarding universe differs from selection universe.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "onboarding_burden": self.onboarding_burden.to_dict(),
            "profile_version": self.profile_version,
            "selections": [item.to_dict() for item in self.selections],
            "stories": [item.to_dict() for item in self.stories],
            "ticker": self.ticker,
            "ui_readiness": self.ui_readiness.value,
        }


@dataclass(frozen=True, slots=True)
class DriverProductSelection:
    selection_id: str
    ticker: str
    driver_id: str
    active_definition_version: int
    economic_importance: EconomicImportance
    monitoring_status: MonitoringStatus
    monitoring_reason: str
    monitoring_authority: str
    monitoring_effective_period: str
    monitoring_review_condition: str | None
    data_readiness: DataReadiness
    analytics_readiness: AnalyticsReadiness
    semantic_profile_status: str
    context_profile_status: str
    display_role: DisplayRole
    economic_group: EconomicGroup
    story_id: str
    importance_reason: str
    selection_reason: str
    current_mathematical_state: str
    current_semantic_interpretation: str
    context_interaction_result: str
    financial_linkages: tuple[str, ...]
    forecast_relevance: tuple[str, ...]
    coloring_readiness: ColoringReadiness
    semantic_signal_ids: tuple[str, ...]
    source_lineage_ids: tuple[str, ...]
    onboarding_review_required: bool

    def __post_init__(self) -> None:
        if self.monitoring_status is not MonitoringStatus.NORMAL:
            if not all(
                (
                    self.monitoring_reason,
                    self.monitoring_authority,
                    self.monitoring_effective_period,
                    self.monitoring_review_condition,
                )
            ):
                raise StorySelectionError("Non-NORMAL monitoring must be explainable.")

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analytics_readiness"] = self.analytics_readiness.to_dict()
        result["coloring_readiness"] = self.coloring_readiness.value
        result["data_readiness"] = self.data_readiness.value
        result["display_role"] = self.display_role.value
        result["economic_group"] = self.economic_group.value
        result["economic_importance"] = self.economic_importance.value
        result["monitoring_status"] = self.monitoring_status.value
        for name in (
            "financial_linkages",
            "forecast_relevance",
            "semantic_signal_ids",
            "source_lineage_ids",
        ):
            result[name] = list(getattr(self, name))
        return result


@dataclass(frozen=True, slots=True)
class InvestorDriverStory:
    story_id: str
    ticker: str
    economic_question: str
    economic_group: EconomicGroup
    primary_drivers: tuple[str, ...]
    context_drivers: tuple[str, ...]
    diagnostic_drivers: tuple[str, ...]
    definition_support_drivers: tuple[str, ...]
    hidden_support_drivers: tuple[str, ...]
    current_mathematical_state: str
    current_semantic_interpretation: str
    context_interaction_result: str
    context_bundle_references: tuple[str, ...]
    economic_importance: EconomicImportance
    monitoring_status: MonitoringStatus
    data_readiness: DataReadiness
    analytics_readiness: AnalyticsReadiness
    financial_linkages: tuple[str, ...]
    forecast_relevance: tuple[str, ...]
    limitations: tuple[str, ...]
    review_state: ProductReviewState
    selection_reasoning: str
    economic_owner: bool = False

    def __post_init__(self) -> None:
        if self.economic_owner:
            raise StorySelectionError("Investor stories cannot become economic owners.")

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["analytics_readiness"] = self.analytics_readiness.to_dict()
        result["data_readiness"] = self.data_readiness.value
        result["economic_group"] = self.economic_group.value
        result["economic_importance"] = self.economic_importance.value
        result["monitoring_status"] = self.monitoring_status.value
        result["review_state"] = self.review_state.value
        for name in (
            "context_bundle_references",
            "context_drivers",
            "definition_support_drivers",
            "diagnostic_drivers",
            "financial_linkages",
            "forecast_relevance",
            "hidden_support_drivers",
            "limitations",
            "primary_drivers",
        ):
            result[name] = list(getattr(self, name))
        return result


@dataclass(frozen=True, slots=True)
class LegacyTierMigration:
    migration_id: str
    ticker: str
    driver_id: str
    legacy_visibility_tier: str
    legacy_contract_disposition: str
    legacy_authoritative: bool
    economic_importance: EconomicImportance
    monitoring_status: MonitoringStatus
    data_readiness: DataReadiness
    display_role: DisplayRole
    migration_reason: str

    def __post_init__(self) -> None:
        if self.legacy_authoritative:
            raise StorySelectionError("Legacy combined tier cannot remain authoritative.")

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["data_readiness"] = self.data_readiness.value
        result["display_role"] = self.display_role.value
        result["economic_importance"] = self.economic_importance.value
        result["monitoring_status"] = self.monitoring_status.value
        return result


@dataclass(frozen=True, slots=True)
class ProductReviewItem:
    review_id: str
    rank: int
    ticker: str
    driver_id: str
    economic_importance: EconomicImportance
    display_role: DisplayRole
    forward_relevance: str
    issue_type: str
    reason: str
    resolution_condition: str

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["display_role"] = self.display_role.value
        result["economic_importance"] = self.economic_importance.value
        return result


@dataclass(frozen=True, slots=True)
class OrthogonalStorySelectionPackage:
    ticker: str
    profile_version: str
    source_semantic_package_sha256: str
    source_analytics_package_sha256: str
    selections: tuple[DriverProductSelection, ...]
    stories: tuple[InvestorDriverStory, ...]
    legacy_migrations: tuple[LegacyTierMigration, ...]
    review_queue: tuple[ProductReviewItem, ...]
    onboarding_burden: OnboardingBurden
    ui_readiness: UIReadiness
    authoritative_product_selection_uses_legacy_combined_tier: bool = False
    economic_importance_readiness_conflation_count: int = 0
    economic_importance_monitoring_conflation_count: int = 0
    economic_importance_display_role_conflation_count: int = 0
    monitoring_semantic_interpretation_conflation_count: int = 0
    data_readiness_display_role_conflation_count: int = 0
    key_downgraded_for_missing_numeric_data_count: int = 0
    ready_numeric_promoted_for_data_density_count: int = 0
    watch_treated_as_negative_count: int = 0
    negative_automatically_treated_as_watch_count: int = 0
    canonical_metric_deleted_through_compression_count: int = 0
    lower_layer_semantic_mutation_count: int = 0
    lower_layer_analytic_mutation_count: int = 0
    context_bundle_ownership_violation_count: int = 0
    legacy_authoritative_survivor_count: int = 0
    unexplained_monitoring_state_count: int = 0
    opaque_product_selection_output_count: int = 0
    new_ticker_specific_python_selection_branch_count: int = 0
    unresolved_material_missing_review_count: int = 0

    def __post_init__(self) -> None:
        if self.authoritative_product_selection_uses_legacy_combined_tier:
            raise StorySelectionError("Legacy visibility tier cannot own vNext selection.")
        selection_ids = {item.driver_id for item in self.selections}
        story_ids = {
            driver_id
            for story in self.stories
            for driver_id in (
                *story.primary_drivers,
                *story.context_drivers,
                *story.diagnostic_drivers,
                *story.definition_support_drivers,
                *story.hidden_support_drivers,
            )
        }
        if selection_ids != story_ids:
            raise StorySelectionError("Story compression deleted a canonical metric.")
        counters = (
            self.economic_importance_readiness_conflation_count,
            self.economic_importance_monitoring_conflation_count,
            self.economic_importance_display_role_conflation_count,
            self.monitoring_semantic_interpretation_conflation_count,
            self.data_readiness_display_role_conflation_count,
            self.key_downgraded_for_missing_numeric_data_count,
            self.ready_numeric_promoted_for_data_density_count,
            self.watch_treated_as_negative_count,
            self.negative_automatically_treated_as_watch_count,
            self.canonical_metric_deleted_through_compression_count,
            self.lower_layer_semantic_mutation_count,
            self.lower_layer_analytic_mutation_count,
            self.context_bundle_ownership_violation_count,
            self.legacy_authoritative_survivor_count,
            self.unexplained_monitoring_state_count,
            self.opaque_product_selection_output_count,
            self.new_ticker_specific_python_selection_branch_count,
            self.unresolved_material_missing_review_count,
        )
        if any(counters):
            raise StorySelectionError("Orthogonal selection acceptance counters must be zero.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "authoritative_product_selection_uses_legacy_combined_tier": self.authoritative_product_selection_uses_legacy_combined_tier,
            "canonical_metric_deleted_through_compression_count": self.canonical_metric_deleted_through_compression_count,
            "context_bundle_ownership_violation_count": self.context_bundle_ownership_violation_count,
            "contract_version": OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION,
            "data_readiness_display_role_conflation_count": self.data_readiness_display_role_conflation_count,
            "economic_importance_display_role_conflation_count": self.economic_importance_display_role_conflation_count,
            "economic_importance_monitoring_conflation_count": self.economic_importance_monitoring_conflation_count,
            "economic_importance_readiness_conflation_count": self.economic_importance_readiness_conflation_count,
            "key_downgraded_for_missing_numeric_data_count": self.key_downgraded_for_missing_numeric_data_count,
            "legacy_authoritative_survivor_count": self.legacy_authoritative_survivor_count,
            "legacy_migrations": [item.to_dict() for item in self.legacy_migrations],
            "lower_layer_analytic_mutation_count": self.lower_layer_analytic_mutation_count,
            "lower_layer_semantic_mutation_count": self.lower_layer_semantic_mutation_count,
            "monitoring_semantic_interpretation_conflation_count": self.monitoring_semantic_interpretation_conflation_count,
            "negative_automatically_treated_as_watch_count": self.negative_automatically_treated_as_watch_count,
            "new_ticker_specific_python_selection_branch_count": self.new_ticker_specific_python_selection_branch_count,
            "onboarding_burden": self.onboarding_burden.to_dict(),
            "opaque_product_selection_output_count": self.opaque_product_selection_output_count,
            "profile_version": self.profile_version,
            "ready_numeric_promoted_for_data_density_count": self.ready_numeric_promoted_for_data_density_count,
            "review_queue": [item.to_dict() for item in self.review_queue],
            "selections": [item.to_dict() for item in self.selections],
            "source_analytics_package_sha256": self.source_analytics_package_sha256,
            "source_semantic_package_sha256": self.source_semantic_package_sha256,
            "stories": [item.to_dict() for item in self.stories],
            "ticker": self.ticker,
            "ui_readiness": self.ui_readiness.value,
            "unexplained_monitoring_state_count": self.unexplained_monitoring_state_count,
            "unresolved_material_missing_review_count": self.unresolved_material_missing_review_count,
            "watch_treated_as_negative_count": self.watch_treated_as_negative_count,
        }

    def serialize(self) -> bytes:
        return serialize_package(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.serialize()).hexdigest()


@dataclass(frozen=True, slots=True)
class StorySelectionConfiguration:
    profiles: Mapping[str, StorySelectionProfile]

    def profile(self, ticker: str) -> StorySelectionProfile:
        try:
            return self.profiles[ticker]
        except KeyError as exc:
            raise StorySelectionError(
                "NEW_TICKER_PRODUCT_SELECTION_NEEDS_REVIEW"
            ) from exc


def _selection(
    driver_id: str,
    importance: EconomicImportance,
    role: DisplayRole,
    story_id: str,
    importance_reason: str,
    selection_reason: str,
    *,
    monitoring: MonitoringStatus = MonitoringStatus.NORMAL,
    data: DataReadiness | None = None,
    monitoring_reason: str = "Ordinary accepted analytical regime.",
    review_condition: str | None = None,
    onboarding_review: bool = False,
) -> DriverSelectionSpec:
    return DriverSelectionSpec(
        driver_id=driver_id,
        economic_importance=importance,
        monitoring_status=monitoring,
        display_role=role,
        story_id=story_id,
        importance_reason=importance_reason,
        selection_reason=selection_reason,
        data_readiness_override=data,
        monitoring_reason=monitoring_reason,
        monitoring_authority="DECLARATIVE_TICKER_PROFILE",
        monitoring_effective_period="LATEST_ACCEPTED_SEMANTIC_WINDOW",
        monitoring_review_condition=review_condition,
        onboarding_review_required=onboarding_review,
    )


def _story(
    story_id: str,
    question: str,
    group: EconomicGroup,
    primary: tuple[str, ...],
    *,
    context: tuple[str, ...] = (),
    diagnostic: tuple[str, ...] = (),
    definition: tuple[str, ...] = (),
    hidden: tuple[str, ...] = (),
    bundles: tuple[str, ...] = (),
    limitations: tuple[str, ...] = (),
    reason: str,
) -> InvestorStorySpec:
    return InvestorStorySpec(
        story_id=story_id,
        economic_question=question,
        economic_group=group,
        primary_drivers=primary,
        context_drivers=context,
        diagnostic_drivers=diagnostic,
        definition_support_drivers=definition,
        hidden_support_drivers=hidden,
        context_bundle_ids=bundles,
        limitations=limitations,
        selection_reasoning=reason,
    )


ANF_SELECTIONS = (
    _selection("driver:operating:comparable-sales@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:anf:demand-and-divergence@1", "Comparable sales is the principal accepted demand trajectory.", "Primary demand expression; material component divergence remains explicit.", monitoring=MonitoringStatus.WATCH, monitoring_reason="Accepted aggregate and material component comparable-sales signals diverge.", review_condition="Return to NORMAL only after accepted component divergence resolves or is redefined.", onboarding_review=True),
    _selection("driver:operating:brand-momentum-context@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:anf:demand-and-divergence@1", "Brand momentum materially qualifies aggregate demand.", "Context preserves component deterioration without creating a second owner.", monitoring=MonitoringStatus.WATCH, data=DataReadiness.READY_QUALITATIVE, monitoring_reason="Qualitative brand context remains material while aggregate/component demand diverges.", review_condition="Review when canonical numeric brand history or resolved divergence is accepted.", onboarding_review=True),
    _selection("driver:operating:inventory-unit-growth@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:anf:inventory-and-demand@1", "Inventory units are central to demand alignment and clearance risk.", "Units directly express physical inventory posture.", onboarding_review=True),
    _selection("driver:operating:inventory-cost-growth@1", EconomicImportance.KEY_DRIVER, DisplayRole.CONTEXT, "story:anf:inventory-and-demand@1", "Inventory cost growth is central to cost/mix interpretation.", "Cost growth qualifies the PRIMARY unit trajectory without duplicating the story.", onboarding_review=True),
    _selection("driver:operating:inventory-unit-growth-erp-points@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DEFINITION_SUPPORT, "story:anf:inventory-and-demand@1", "ERP effect supports comparability rather than owning inventory economics.", "Definition support isolates a temporary reporting regime.", monitoring=MonitoringStatus.RESOLVING, monitoring_reason="ERP-related inventory measurement effect is a temporary accepted regime modifier.", review_condition="Retire monitoring after the accepted ERP effect no longer affects comparability.", onboarding_review=True),
    _selection("driver:operating:company-owned-stores-end@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.PRIMARY, "story:anf:store-footprint-and-productivity@1", "Ending company-owned footprint is economically material capacity state.", "Primary state metric compresses opening/closing activity.", onboarding_review=True),
    _selection("driver:operating:new-stores@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:anf:store-footprint-and-productivity@1", "New stores explain material footprint growth.", "Context rather than a standalone good/bad signal."),
    _selection("driver:operating:closed-stores@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DIAGNOSTIC, "story:anf:store-footprint-and-productivity@1", "Closures diagnose footprint reconciliation.", "Diagnostic because closures may reflect optimization or deterioration."),
    _selection("driver:operating:right-sized-stores@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.CONTEXT, "story:anf:store-footprint-and-productivity@1", "Right-sizing materially qualifies footprint change.", "Context captures productivity action without claiming direction.", onboarding_review=True),
    _selection("driver:operating:remodeled-stores@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DIAGNOSTIC, "story:anf:store-footprint-and-productivity@1", "Remodels support footprint-investment analysis.", "Diagnostic depth below the primary footprint state."),
    _selection("driver:operating:franchise-stores@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DIAGNOSTIC, "story:anf:store-footprint-and-productivity@1", "Franchise footprint is useful decomposition.", "Diagnostic because company-owned economics remain primary."),
    _selection("driver:operating:company-owned-stores-start@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DEFINITION_SUPPORT, "story:anf:store-footprint-and-productivity@1", "Opening footprint supports reconciliation.", "Definition support for period movement."),
    _selection("driver:operating:total-stores-including-franchise@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DEFINITION_SUPPORT, "story:anf:store-footprint-and-productivity@1", "Total footprint supports scope reconciliation.", "Definition support avoids a duplicate top-level footprint story."),
    _selection("driver:operating:digital-sales-mix@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.PRIMARY, "story:anf:channel-mix-and-digital@1", "Digital mix is a material channel and operating-model driver.", "Primary expression of channel mix, without intrinsic good/bad meaning.", onboarding_review=True),
    _selection("driver:operating:store-digital-activity-context@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.CONTEXT, "story:anf:channel-mix-and-digital@1", "Store/digital activity qualifies channel mix.", "Qualitative context remains visible beside the primary mix metric.", data=DataReadiness.READY_QUALITATIVE),
)


ANF_STORIES = (
    _story("story:anf:demand-and-divergence@1", "DEMAND_AND_COMPONENT_DIVERGENCE", EconomicGroup.DEMAND_VOLUME, ("driver:operating:comparable-sales@1",), context=("driver:operating:brand-momentum-context@1",), bundles=("bundle:anf:total-brand-divergence@1",), reason="One demand story preserves aggregate and component context."),
    _story("story:anf:inventory-and-demand@1", "INVENTORY_AND_DEMAND", EconomicGroup.COSTS_UNIT_ECONOMICS, ("driver:operating:inventory-unit-growth@1",), context=("driver:operating:inventory-cost-growth@1",), definition=("driver:operating:inventory-unit-growth-erp-points@1",), bundles=("bundle:anf:demand-inventory@1",), limitations=("No arbitrary inventory target range.",), reason="Unit posture is PRIMARY; cost/mix and ERP regime remain explicit context."),
    _story("story:anf:store-footprint-and-productivity@1", "STORE_FOOTPRINT_AND_PRODUCTIVITY", EconomicGroup.CAPACITY_UTILIZATION, ("driver:operating:company-owned-stores-end@1",), context=("driver:operating:new-stores@1", "driver:operating:right-sized-stores@1"), diagnostic=("driver:operating:closed-stores@1", "driver:operating:franchise-stores@1", "driver:operating:remodeled-stores@1"), definition=("driver:operating:company-owned-stores-start@1", "driver:operating:total-stores-including-franchise@1"), bundles=("bundle:anf:footprint-productivity@1",), reason="Ending footprint answers the top question; activity and scope rows explain it."),
    _story("story:anf:channel-mix-and-digital@1", "CHANNEL_MIX_AND_DIGITAL", EconomicGroup.PRICE_MIX, ("driver:operating:digital-sales-mix@1",), context=("driver:operating:store-digital-activity-context@1",), limitations=("Mix is context-dependent rather than automatically positive.",), reason="Digital mix is the compact numeric expression with qualitative activity context."),
)


PBI_SELECTIONS = (
    _selection("driver:operating:presort-volume-context@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:pbi:presort-economics@1", "Presort volume is essential to understanding the core processing economics.", "PRIMARY despite missing numeric history; missing data is a readiness state.", monitoring=MonitoringStatus.WATCH, data=DataReadiness.NEEDS_DATA, monitoring_reason="The accepted volume/price/retention bundle carries material client-retention and volume risk.", review_condition="Review after accepted numeric volume and retention history is wired.", onboarding_review=True),
    _selection("driver:operating:presort-pricing-mix-context@1", EconomicImportance.KEY_DRIVER, DisplayRole.CONTEXT, "story:pbi:presort-economics@1", "Price/mix is essential to interpreting Presort volume and retention economics.", "KEY context beside volume rather than a duplicate primary row.", data=DataReadiness.NEEDS_DATA, onboarding_review=True),
    _selection("driver:operating:sendtech-activity-context@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.PRIMARY, "story:pbi:sendtech-activity-and-backlog@1", "SendTech activity/backlog is a material leading operating concept.", "PRIMARY qualitative story awaiting numeric extraction.", monitoring=MonitoringStatus.EMERGING, data=DataReadiness.NEEDS_DATA, monitoring_reason="SendTech activity/backlog is an emerging disclosure with maturing history and quality context.", review_condition="Review when accepted bookings/backlog history and quality attributes are available.", onboarding_review=True),
    _selection("driver:operating:sendtech-pricing-mix-context@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.CONTEXT, "story:pbi:sendtech-activity-and-backlog@1", "Pricing/mix supports interpretation of SendTech activity.", "Qualitative context remains beside the primary activity concept.", data=DataReadiness.READY_QUALITATIVE, onboarding_review=True),
)


PBI_STORIES = (
    _story("story:pbi:presort-economics@1", "PRESORT_ECONOMICS", EconomicGroup.DEMAND_VOLUME, ("driver:operating:presort-volume-context@1",), context=("driver:operating:presort-pricing-mix-context@1",), bundles=("bundle:pbi:presort-volume-price-retention@1",), limitations=("Numeric volume, retention, and cost-to-serve history is not yet accepted.",), reason="Volume is PRIMARY; price/mix is economically KEY context and remains visible despite data gaps."),
    _story("story:pbi:sendtech-activity-and-backlog@1", "SENDTECH_ACTIVITY_AND_BACKLOG_QUALITY", EconomicGroup.LEADING_INDICATORS, ("driver:operating:sendtech-activity-context@1",), context=("driver:operating:sendtech-pricing-mix-context@1",), bundles=("bundle:pbi:sendtech-backlog-quality@1",), limitations=("No numeric backlog-quality conclusion without duration/concentration evidence.",), reason="Emerging activity is PRIMARY with pricing/mix as qualitative context."),
)


GPRE_SELECTIONS = (
    _selection("driver:operating:underlying-crush-margin@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:gpre:crush-economics@1", "Underlying crush margin is the principal comparable unit-economics driver.", "PRIMARY isolates underlying operating economics.", onboarding_review=True),
    _selection("driver:operating:consolidated-ethanol-crush-margin@1", EconomicImportance.KEY_DRIVER, DisplayRole.CONTEXT, "story:gpre:crush-economics@1", "Consolidated crush margin captures economically essential realized total economics.", "KEY context reconciles underlying and policy components without duplicate primary display."),
    _selection("driver:operating:crush-margin-ex-45z@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.DIAGNOSTIC, "story:gpre:crush-economics@1", "Ex-45Z margin decomposes material policy value.", "Diagnostic decomposition below the primary margin."),
    _selection("driver:operating:crush-margin-ex-rin@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DIAGNOSTIC, "story:gpre:crush-economics@1", "Ex-RIN margin provides sparse decomposition support.", "Diagnostic because history is limited and RIN remains separately owned."),
    _selection("driver:operating:rin-impact@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:gpre:crush-economics@1", "RIN impact materially qualifies realized crush economics.", "Context preserves its separate policy identity."),
    _selection("driver:operating:input-cost-context@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:gpre:crush-economics@1", "Input-cost context materially constrains margin interpretation.", "Qualitative context; no causal numeric owner.", data=DataReadiness.READY_QUALITATIVE),
    _selection("driver:operating:risk-management-context@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.HIDDEN_SUPPORT, "story:gpre:crush-economics@1", "Risk-management commentary supports margin interpretation.", "Retained machine-readably without top-surface duplication.", data=DataReadiness.READY_QUALITATIVE),
    _selection("driver:operating:ethanol-gallons-sold@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:gpre:volume-and-utilization@1", "Sold gallons most directly connect operating volume to realized economics.", "PRIMARY volume expression." , onboarding_review=True),
    _selection("driver:operating:ethanol-plant-utilization@1", EconomicImportance.KEY_DRIVER, DisplayRole.CONTEXT, "story:gpre:volume-and-utilization@1", "Utilization is economically essential capacity context.", "KEY context rather than a duplicate primary volume row.", monitoring=MonitoringStatus.WATCH, data=DataReadiness.NOT_COMPARABLE, monitoring_reason="Accepted utilization history contains a material definition/capacity-scope break.", review_condition="Review after a comparable accepted utilization definition segment is established.", onboarding_review=True),
    _selection("driver:operating:ethanol-gallons-produced@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.DIAGNOSTIC, "story:gpre:volume-and-utilization@1", "Production volume materially diagnoses the sold-volume chain.", "Diagnostic because sold gallons more directly answer realized volume.", onboarding_review=True),
    _selection("driver:operating:corn-consumed@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.DIAGNOSTIC, "story:gpre:volume-and-utilization@1", "Corn throughput materially explains input scale.", "Diagnostic input-side volume rather than another primary row."),
    _selection("driver:operating:operating-plants-context@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.DEFINITION_SUPPORT, "story:gpre:volume-and-utilization@1", "Operating-plant scope supports utilization comparability.", "Definition support for capacity regime.", data=DataReadiness.READY_QUALITATIVE),
    _selection("driver:operating:renewable-corn-oil-produced@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.PRIMARY, "story:gpre:coproduct-output-mix@1", "Renewable corn oil is a material coproduct output and mix indicator.", "Compact PRIMARY for the coproduct output story."),
    _selection("driver:operating:distillers-grains-produced@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:gpre:coproduct-output-mix@1", "Distillers grains materially explains coproduct output.", "Context beside the compact coproduct primary."),
    _selection("driver:operating:ultra-high-protein-produced@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:gpre:coproduct-output-mix@1", "Ultra-high-protein output materially explains mix development.", "Context rather than another top-level row."),
    _selection("driver:operating:coproduct-context@1", EconomicImportance.SUPPORTING_DRIVER, DisplayRole.CONTEXT, "story:gpre:coproduct-output-mix@1", "Qualitative coproduct evidence supports numeric output series.", "Context remains non-owning.", data=DataReadiness.READY_QUALITATIVE),
    _selection("driver:operating:45z-value-realized@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:gpre:policy-value-and-eligibility@1", "45Z is a major distinct policy-linked economic driver.", "PRIMARY and separate from ordinary crush economics.", monitoring=MonitoringStatus.WATCH, data=DataReadiness.NEEDS_DATA, monitoring_reason="45Z policy/eligibility remains material and the accepted historical period set is incomplete.", review_condition="Review after missing source-backed quarters and policy validity are resolved.", onboarding_review=True),
    _selection("driver:operating:45z-monetization-context@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:gpre:policy-value-and-eligibility@1", "Eligibility and monetization context is material to 45Z realization.", "Context cannot merge 45Z into crush or RIN.", monitoring=MonitoringStatus.WATCH, data=DataReadiness.READY_QUALITATIVE, monitoring_reason="Eligibility and monetization remain evolving material policy conditions.", review_condition="Review when accepted policy eligibility and monetization state stabilizes.", onboarding_review=True),
    _selection("driver:operating:carbon-capture-context@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:gpre:policy-value-and-eligibility@1", "Carbon-capture/CI development is a material policy-value enabler.", "Emerging qualitative context beside 45Z.", monitoring=MonitoringStatus.EMERGING, data=DataReadiness.READY_QUALITATIVE, monitoring_reason="Carbon-capture/CI capability is newly material and disclosure is still maturing.", review_condition="Review after accepted operating milestones and stable definition emerge."),
)


GPRE_STORIES = (
    _story("story:gpre:crush-economics@1", "UNDERLYING_AND_REALIZED_CRUSH_ECONOMICS", EconomicGroup.COSTS_UNIT_ECONOMICS, ("driver:operating:underlying-crush-margin@1",), context=("driver:operating:consolidated-ethanol-crush-margin@1", "driver:operating:input-cost-context@1", "driver:operating:rin-impact@1"), diagnostic=("driver:operating:crush-margin-ex-45z@1", "driver:operating:crush-margin-ex-rin@1"), hidden=("driver:operating:risk-management-context@1",), bundles=("bundle:gpre:volume-unit-economics@1",), limitations=("No causal inference from input commentary.",), reason="Underlying margin is PRIMARY; consolidated/policy/input metrics reconcile and diagnose it."),
    _story("story:gpre:volume-and-utilization@1", "VOLUME_AND_UTILIZATION", EconomicGroup.DEMAND_VOLUME, ("driver:operating:ethanol-gallons-sold@1",), context=("driver:operating:ethanol-plant-utilization@1",), diagnostic=("driver:operating:corn-consumed@1", "driver:operating:ethanol-gallons-produced@1"), definition=("driver:operating:operating-plants-context@1",), bundles=("bundle:gpre:utilization-margin@1", "bundle:gpre:volume-unit-economics@1"), limitations=("Utilization comparison is blocked across the accepted definition break.",), reason="Sold volume is PRIMARY; utilization, production, and input throughput remain explicit context/diagnostics."),
    _story("story:gpre:coproduct-output-mix@1", "COPRODUCT_OUTPUT_MIX", EconomicGroup.DEMAND_VOLUME, ("driver:operating:renewable-corn-oil-produced@1",), context=("driver:operating:coproduct-context@1", "driver:operating:distillers-grains-produced@1", "driver:operating:ultra-high-protein-produced@1"), reason="One compact coproduct story preserves every canonical output metric."),
    _story("story:gpre:policy-value-and-eligibility@1", "POLICY_VALUE_AND_ELIGIBILITY", EconomicGroup.COSTS_UNIT_ECONOMICS, ("driver:operating:45z-value-realized@1",), context=("driver:operating:45z-monetization-context@1", "driver:operating:carbon-capture-context@1"), bundles=("bundle:gpre:policy-eligibility@1",), limitations=("Missing historical 45Z quarters remain explicit; no partial-period aggregate.",), reason="45Z remains a distinct PRIMARY policy story with eligibility and CI context."),
)


SELECTION_PROFILES = {
    "ANF": StorySelectionProfile(
        ticker="ANF",
        profile_version="operating-driver-story-profile:anf@1",
        selections=ANF_SELECTIONS,
        stories=ANF_STORIES,
        onboarding_burden=OnboardingBurden(15, 9, 9, 8, 15, 6),
        ui_readiness=UIReadiness.READY_FOR_INVESTOR_UI_PREVIEW,
    ),
    "PBI": StorySelectionProfile(
        ticker="PBI",
        profile_version="operating-driver-story-profile:pbi@1",
        selections=PBI_SELECTIONS,
        stories=PBI_STORIES,
        onboarding_burden=OnboardingBurden(4, 0, 0, 4, 4, 4),
        ui_readiness=UIReadiness.READY_FOR_UI_WITH_VISIBLE_DATA_GAPS,
    ),
    "GPRE": StorySelectionProfile(
        ticker="GPRE",
        profile_version="operating-driver-story-profile:gpre@1",
        selections=GPRE_SELECTIONS,
        stories=GPRE_STORIES,
        onboarding_burden=OnboardingBurden(19, 12, 12, 10, 19, 7),
        ui_readiness=UIReadiness.READY_FOR_UI_WITH_VISIBLE_DATA_GAPS,
    ),
}


DEFAULT_STORY_SELECTION_CONFIGURATION = StorySelectionConfiguration(
    profiles=SELECTION_PROFILES
)


def _hash_id(kind: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join((kind, *parts)).encode("utf-8")).hexdigest()[:24]
    return f"{kind}:{digest}"


def _aggregate_state(values: Iterable[str], *, empty: str) -> str:
    unique = sorted(set(values))
    if not unique:
        return empty
    if len(unique) == 1:
        return unique[0]
    return "MIXED"


def _aggregate_interpretation(values: Iterable[EconomicInterpretation]) -> str:
    unique = set(values)
    if not unique:
        return EconomicInterpretation.INSUFFICIENT_EVIDENCE.value
    if len(unique) == 1:
        return next(iter(unique)).value
    if EconomicInterpretation.MIXED in unique or (
        EconomicInterpretation.POSITIVE in unique
        and EconomicInterpretation.NEGATIVE in unique
    ):
        return EconomicInterpretation.MIXED.value
    if EconomicInterpretation.CONTEXT_DEPENDENT in unique:
        return EconomicInterpretation.CONTEXT_DEPENDENT.value
    return EconomicInterpretation.MIXED.value


def _derive_data_readiness(
    spec: DriverSelectionSpec,
    base_signals: tuple[Any, ...],
) -> DataReadiness:
    if spec.data_readiness_override is not None:
        return spec.data_readiness_override
    if any(item.definition_break_present for item in base_signals):
        return DataReadiness.NOT_COMPARABLE
    if any(item.numeric_semantic_output and item.latest_value is not None for item in base_signals):
        return DataReadiness.READY_NUMERIC
    if base_signals and any(item.source_lineage_ids for item in base_signals):
        return DataReadiness.READY_QUALITATIVE
    return DataReadiness.NEEDS_DATA


def _analytics_readiness(
    base_signals: tuple[Any, ...],
    priority: Any,
    analytics: DerivedAnalyticsPackage,
    driver_id: str,
) -> AnalyticsReadiness:
    ttm_ready = any(
        item.driver_id == driver_id and item.availability.value == "AVAILABLE"
        for item in analytics.ttm_analytics
    )
    semantic_ready = any(
        item.base_interpretation
        not in {
            EconomicInterpretation.INSUFFICIENT_EVIDENCE,
            EconomicInterpretation.NOT_INTERPRETABLE,
        }
        for item in base_signals
    )
    return AnalyticsReadiness(
        latest_ready=any(item.latest_value is not None for item in base_signals),
        qoq_ready=priority.qoq_ready,
        yoy_ready=priority.yoy_ready,
        trend_4q_ready=priority.trend_4q_ready,
        sparkline_12q_ready=priority.sparkline_12q_ready,
        ttm_ready=ttm_ready,
        semantic_ready=semantic_ready,
    )


def _review_queue(
    selections: tuple[DriverProductSelection, ...],
    priorities: Mapping[str, Any],
) -> tuple[ProductReviewItem, ...]:
    issue_order = {
        "UNRESOLVED_ECONOMIC_IMPORTANCE": 0,
        DataReadiness.NEEDS_REVIEW.value: 1,
        DataReadiness.NOT_COMPARABLE.value: 2,
        DataReadiness.NEEDS_DATA.value: 3,
    }
    importance_order = {
        EconomicImportance.KEY_DRIVER: 0,
        EconomicImportance.UNRESOLVED: 0,
        EconomicImportance.MATERIAL_DRIVER: 1,
        EconomicImportance.SUPPORTING_DRIVER: 2,
    }
    display_order = {item: index for index, item in enumerate(DisplayRole)}
    rating_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    pending: list[tuple[tuple[Any, ...], DriverProductSelection, str]] = []
    for item in selections:
        issue: str | None = None
        if item.economic_importance is EconomicImportance.UNRESOLVED:
            issue = "UNRESOLVED_ECONOMIC_IMPORTANCE"
        elif (
            item.economic_importance
            in {EconomicImportance.KEY_DRIVER, EconomicImportance.MATERIAL_DRIVER}
            and item.data_readiness
            in {
                DataReadiness.NEEDS_DATA,
                DataReadiness.NEEDS_REVIEW,
                DataReadiness.NOT_COMPARABLE,
            }
        ):
            issue = item.data_readiness.value
        if issue is None:
            continue
        forward = priorities[item.driver_id].dimensions.forward_relevance.value
        key = (
            importance_order[item.economic_importance],
            display_order[item.display_role],
            rating_order[forward],
            issue_order[issue],
            item.driver_id,
        )
        pending.append((key, item, issue))
    result = []
    for rank, (_, item, issue) in enumerate(sorted(pending, key=lambda value: value[0]), start=1):
        result.append(
            ProductReviewItem(
                review_id=_hash_id("driver-selection-review", item.ticker, item.driver_id, issue),
                rank=rank,
                ticker=item.ticker,
                driver_id=item.driver_id,
                economic_importance=item.economic_importance,
                display_role=item.display_role,
                forward_relevance=priorities[item.driver_id].dimensions.forward_relevance.value,
                issue_type=issue,
                reason=f"{item.economic_importance.value} / {item.display_role.value} requires explicit {issue} disposition.",
                resolution_condition=item.monitoring_review_condition
                or "Accept sufficient source-backed evidence and rerun product selection.",
            )
        )
    return tuple(result)


def _combine_analytics(items: Iterable[AnalyticsReadiness]) -> AnalyticsReadiness:
    values = tuple(items)
    return AnalyticsReadiness(
        latest_ready=any(item.latest_ready for item in values),
        qoq_ready=any(item.qoq_ready for item in values),
        yoy_ready=any(item.yoy_ready for item in values),
        trend_4q_ready=any(item.trend_4q_ready for item in values),
        sparkline_12q_ready=any(item.sparkline_12q_ready for item in values),
        ttm_ready=any(item.ttm_ready for item in values),
        semantic_ready=any(item.semantic_ready for item in values),
    )


def _story_importance(items: Iterable[DriverProductSelection]) -> EconomicImportance:
    order = {
        EconomicImportance.KEY_DRIVER: 0,
        EconomicImportance.MATERIAL_DRIVER: 1,
        EconomicImportance.SUPPORTING_DRIVER: 2,
        EconomicImportance.UNRESOLVED: 3,
    }
    return min((item.economic_importance for item in items), key=order.__getitem__)


def _story_monitoring(items: Iterable[DriverProductSelection]) -> MonitoringStatus:
    values = tuple(items)
    order = {
        MonitoringStatus.WATCH: 0,
        MonitoringStatus.EMERGING: 1,
        MonitoringStatus.RESOLVING: 2,
        MonitoringStatus.NORMAL: 3,
        MonitoringStatus.RETIRED: 4,
    }
    if values and all(item.monitoring_status is MonitoringStatus.RETIRED for item in values):
        return MonitoringStatus.RETIRED
    return min((item.monitoring_status for item in values), key=order.__getitem__)


def _story_data_readiness(items: Iterable[DriverProductSelection]) -> DataReadiness:
    order = {
        DataReadiness.NEEDS_REVIEW: 0,
        DataReadiness.NOT_COMPARABLE: 1,
        DataReadiness.NEEDS_DATA: 2,
        DataReadiness.READY_QUALITATIVE: 3,
        DataReadiness.READY_NUMERIC: 4,
        DataReadiness.NOT_APPLICABLE: 5,
    }
    return min((item.data_readiness for item in items), key=order.__getitem__)


def build_orthogonal_story_selection(
    semantic_package: SemanticPriorityPackage,
    analytics: DerivedAnalyticsPackage,
    *,
    configuration: StorySelectionConfiguration = DEFAULT_STORY_SELECTION_CONFIGURATION,
) -> OrthogonalStorySelectionPackage:
    """Build orthogonal selection without mutating semantic or analytical truth."""

    before_semantics = semantic_package.serialize()
    before_analytics = analytics.serialize()
    if semantic_package.derived_analytics_sha256 != analytics.sha256:
        raise StorySelectionError("Semantic package does not consume this analytics identity.")
    if semantic_package.ticker != analytics.ticker:
        raise StorySelectionError("Semantic and analytics tickers differ.")
    profile = configuration.profile(semantic_package.ticker)
    priorities = {item.driver_id: item for item in semantic_package.driver_priorities}
    profile_readiness = {
        item.driver_id: item for item in semantic_package.profile_readiness
    }
    specs = {item.driver_id: item for item in profile.selections}
    if set(priorities) != set(specs):
        raise StorySelectionError("Product-selection profile does not cover canonical universe.")
    if set(profile_readiness) != set(specs):
        raise StorySelectionError(
            "Semantic-profile readiness does not cover canonical universe."
        )
    base_by_driver: dict[str, list[Any]] = defaultdict(list)
    context_by_driver: dict[str, list[Any]] = defaultdict(list)
    for item in semantic_package.base_semantic_signals:
        base_by_driver[item.driver_id].append(item)
    for item in semantic_package.contextual_semantic_signals:
        context_by_driver[item.driver_id].append(item)
    selections: list[DriverProductSelection] = []
    for driver_id in sorted(specs):
        spec = specs[driver_id]
        priority = priorities[driver_id]
        semantic_profile = profile_readiness[driver_id]
        bases = tuple(base_by_driver[driver_id])
        contexts = tuple(context_by_driver[driver_id])
        data_readiness = _derive_data_readiness(spec, bases)
        analytics_readiness = _analytics_readiness(bases, priority, analytics, driver_id)
        selections.append(
            DriverProductSelection(
                selection_id=_hash_id("driver-product-selection", semantic_package.ticker, driver_id, profile.profile_version),
                ticker=semantic_package.ticker,
                driver_id=driver_id,
                active_definition_version=priority.active_definition_version,
                economic_importance=spec.economic_importance,
                monitoring_status=spec.monitoring_status,
                monitoring_reason=spec.monitoring_reason,
                monitoring_authority=spec.monitoring_authority,
                monitoring_effective_period=spec.monitoring_effective_period,
                monitoring_review_condition=spec.monitoring_review_condition,
                data_readiness=data_readiness,
                analytics_readiness=analytics_readiness,
                semantic_profile_status=semantic_profile.state.value,
                context_profile_status=(
                    "CONTEXT_PROFILE_DECLARED"
                    if semantic_profile.context_dependencies_declared
                    else "CONTEXT_PROFILE_NEEDS_REVIEW"
                ),
                display_role=spec.display_role,
                economic_group=priority.economic_group,
                story_id=spec.story_id,
                importance_reason=spec.importance_reason,
                selection_reason=spec.selection_reason,
                current_mathematical_state=_aggregate_state((item.mathematical_direction.value for item in bases), empty="INSUFFICIENT_EVIDENCE"),
                current_semantic_interpretation=_aggregate_interpretation(item.final_interpretation for item in contexts),
                context_interaction_result=_aggregate_state((item.context_interaction_result.value for item in contexts), empty="NOT_EVALUABLE"),
                financial_linkages=tuple(sorted({item.financial_linkage for item in bases})),
                forecast_relevance=tuple(sorted({item.forecast_capability for item in bases})),
                coloring_readiness=priority.coloring_readiness,
                semantic_signal_ids=tuple(sorted(item.semantic_signal_id for item in contexts)),
                source_lineage_ids=tuple(sorted({lineage for item in contexts for lineage in item.source_lineage_ids})),
                onboarding_review_required=spec.onboarding_review_required,
            )
        )
    selection_tuple = tuple(selections)
    selected = {item.driver_id: item for item in selection_tuple}
    reviews = _review_queue(selection_tuple, priorities)
    reviewed_ids = {item.driver_id for item in reviews}
    accepted_bundle_ids = {item.bundle_id for item in semantic_package.context_bundles}
    stories: list[InvestorDriverStory] = []
    for spec in profile.stories:
        if not set(spec.context_bundle_ids).issubset(accepted_bundle_ids):
            raise StorySelectionError("Story references an unaccepted context bundle.")
        story_items = tuple(selected[item] for item in spec.all_driver_ids)
        contexts = tuple(
            signal
            for driver_id in spec.all_driver_ids
            for signal in context_by_driver[driver_id]
        )
        bases = tuple(
            signal for driver_id in spec.all_driver_ids for signal in base_by_driver[driver_id]
        )
        stories.append(
            InvestorDriverStory(
                story_id=spec.story_id,
                ticker=semantic_package.ticker,
                economic_question=spec.economic_question,
                economic_group=spec.economic_group,
                primary_drivers=spec.primary_drivers,
                context_drivers=spec.context_drivers,
                diagnostic_drivers=spec.diagnostic_drivers,
                definition_support_drivers=spec.definition_support_drivers,
                hidden_support_drivers=spec.hidden_support_drivers,
                current_mathematical_state=_aggregate_state((item.mathematical_direction.value for item in bases), empty="INSUFFICIENT_EVIDENCE"),
                current_semantic_interpretation=_aggregate_interpretation(item.final_interpretation for item in contexts),
                context_interaction_result=_aggregate_state((item.context_interaction_result.value for item in contexts), empty="NOT_EVALUABLE"),
                context_bundle_references=spec.context_bundle_ids,
                economic_importance=_story_importance(story_items),
                monitoring_status=_story_monitoring(story_items),
                data_readiness=_story_data_readiness(story_items),
                analytics_readiness=_combine_analytics(item.analytics_readiness for item in story_items),
                financial_linkages=tuple(sorted({value for item in story_items for value in item.financial_linkages})),
                forecast_relevance=tuple(sorted({value for item in story_items for value in item.forecast_relevance})),
                limitations=spec.limitations,
                review_state=(ProductReviewState.PRODUCT_REVIEW_REQUIRED if any(item.driver_id in reviewed_ids for item in story_items) else ProductReviewState.NO_REVIEW_REQUIRED),
                selection_reasoning=spec.selection_reasoning,
            )
        )
    migrations = tuple(
        LegacyTierMigration(
            migration_id=_hash_id("legacy-tier-migration", semantic_package.ticker, item.driver_id),
            ticker=semantic_package.ticker,
            driver_id=item.driver_id,
            legacy_visibility_tier=priorities[item.driver_id].visibility_tier.value,
            legacy_contract_disposition="LEGACY_COMBINED_VISIBILITY_TIER_DEPRECATED",
            legacy_authoritative=False,
            economic_importance=item.economic_importance,
            monitoring_status=item.monitoring_status,
            data_readiness=item.data_readiness,
            display_role=item.display_role,
            migration_reason="Old combined tier is retained only as migration evidence; the four orthogonal axes are authoritative.",
        )
        for item in selection_tuple
    )
    unresolved_missing = sum(
        1
        for item in selection_tuple
        if item.economic_importance is EconomicImportance.UNRESOLVED
        and item.driver_id not in reviewed_ids
    )
    package = OrthogonalStorySelectionPackage(
        ticker=semantic_package.ticker,
        profile_version=profile.profile_version,
        source_semantic_package_sha256=semantic_package.sha256,
        source_analytics_package_sha256=analytics.sha256,
        selections=selection_tuple,
        stories=tuple(stories),
        legacy_migrations=migrations,
        review_queue=reviews,
        onboarding_burden=profile.onboarding_burden,
        ui_readiness=profile.ui_readiness,
        unresolved_material_missing_review_count=unresolved_missing,
    )
    if semantic_package.serialize() != before_semantics:
        raise StorySelectionError("Product selection mutated accepted semantic truth.")
    if analytics.serialize() != before_analytics:
        raise StorySelectionError("Product selection mutated accepted analytical truth.")
    return package


def combined_story_selection_digest(
    packages: Iterable[OrthogonalStorySelectionPackage],
) -> str:
    payload = serialize_package(
        {
            "contract_version": OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION,
            "package_hashes": {
                item.ticker: item.sha256
                for item in sorted(packages, key=lambda package: package.ticker)
            },
        }
    )
    return hashlib.sha256(payload).hexdigest()


def combined_product_review_queue(
    packages: Iterable[OrthogonalStorySelectionPackage],
) -> tuple[ProductReviewItem, ...]:
    """Return one deterministic cross-ticker queue without an opaque score."""

    importance_order = {
        EconomicImportance.KEY_DRIVER: 0,
        EconomicImportance.UNRESOLVED: 0,
        EconomicImportance.MATERIAL_DRIVER: 1,
        EconomicImportance.SUPPORTING_DRIVER: 2,
    }
    display_order = {item: index for index, item in enumerate(DisplayRole)}
    rating_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    issue_order = {
        "UNRESOLVED_ECONOMIC_IMPORTANCE": 0,
        DataReadiness.NEEDS_REVIEW.value: 1,
        DataReadiness.NOT_COMPARABLE.value: 2,
        DataReadiness.NEEDS_DATA.value: 3,
    }
    records = tuple(
        item for package in packages for item in package.review_queue
    )
    ordered = sorted(
        records,
        key=lambda item: (
            importance_order[item.economic_importance],
            display_order[item.display_role],
            rating_order[item.forward_relevance],
            issue_order[item.issue_type],
            item.ticker,
            item.driver_id,
        ),
    )
    return tuple(
        dataclasses.replace(item, rank=rank)
        for rank, item in enumerate(ordered, start=1)
    )


CLOUD_CONTROL_SELECTIONS = (
    _selection("driver:synthetic:cloud-revenue-growth@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:cloud:demand-and-monetization@1", "Cloud revenue is a key demand/monetization outcome.", "PRIMARY outcome."),
    _selection("driver:synthetic:paid-seats@1", EconomicImportance.KEY_DRIVER, DisplayRole.CONTEXT, "story:cloud:demand-and-monetization@1", "Seats are a key demand quantity.", "KEY context avoids a duplicate primary row."),
    _selection("driver:synthetic:arpu-mix@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:cloud:demand-and-monetization@1", "ARPU/mix materially qualifies monetization.", "Context beside revenue/seats."),
    _selection("driver:synthetic:rpo@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.PRIMARY, "story:cloud:backlog-quality@1", "RPO is a material leading demand indicator.", "PRIMARY only with visible quality limitation."),
    _selection("driver:synthetic:cloud-gross-margin@1", EconomicImportance.KEY_DRIVER, DisplayRole.PRIMARY, "story:cloud:capacity-and-margin@1", "Gross margin is key unit economics.", "PRIMARY margin outcome."),
    _selection("driver:synthetic:capacity-constraint@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:cloud:capacity-and-margin@1", "Capacity constraints materially qualify demand and margin.", "Non-owning context."),
    _selection("driver:synthetic:capacity-investment@1", EconomicImportance.MATERIAL_DRIVER, DisplayRole.CONTEXT, "story:cloud:capacity-and-margin@1", "Capacity investment materially qualifies current margin.", "Regime context rather than separate primary."),
)


CLOUD_CONTROL_STORIES = (
    _story("story:cloud:demand-and-monetization@1", "DEMAND_AND_MONETIZATION", EconomicGroup.DEMAND_VOLUME, ("driver:synthetic:cloud-revenue-growth@1",), context=("driver:synthetic:arpu-mix@1", "driver:synthetic:paid-seats@1"), reason="Revenue is PRIMARY; seats and ARPU remain economically important context."),
    _story("story:cloud:backlog-quality@1", "BACKLOG_QUALITY", EconomicGroup.LEADING_INDICATORS, ("driver:synthetic:rpo@1",), limitations=("Duration, concentration, cancellation, and conversion timing remain required context.",), reason="RPO receives one bounded primary story with explicit limitations."),
    _story("story:cloud:capacity-and-margin@1", "CAPACITY_AND_MARGIN", EconomicGroup.COSTS_UNIT_ECONOMICS, ("driver:synthetic:cloud-gross-margin@1",), context=("driver:synthetic:capacity-constraint@1", "driver:synthetic:capacity-investment@1"), reason="Margin is PRIMARY; capacity constraint and investment are context."),
)


CLOUD_CONTROL_SELECTION_PROFILE = StorySelectionProfile(
    ticker="CLOUD-CONTROL",
    profile_version="operating-driver-story-profile:cloud-control@1",
    selections=CLOUD_CONTROL_SELECTIONS,
    stories=CLOUD_CONTROL_STORIES,
    onboarding_burden=OnboardingBurden(7, 7, 7, 7, 7, 0),
    ui_readiness=UIReadiness.READY_FOR_INVESTOR_UI_PREVIEW,
)


def microsoft_cross_sector_product_selection_fixture() -> dict[str, Any]:
    """Return a synthetic shared-engine fixture, never a production profile."""

    primary_count = sum(
        item.display_role is DisplayRole.PRIMARY
        for item in CLOUD_CONTROL_SELECTION_PROFILE.selections
    )
    return {
        "fixture_kind": "SYNTHETIC_CROSS_SECTOR_CONTROL_ONLY",
        "production_ticker_created": False,
        "new_ticker_specific_python_selection_branch_count": 0,
        "profile": CLOUD_CONTROL_SELECTION_PROFILE.to_dict(),
        "canonical_driver_count": 7,
        "story_count": 3,
        "primary_count": primary_count,
        "economically_important_non_primary_count": sum(
            item.economic_importance
            in {EconomicImportance.KEY_DRIVER, EconomicImportance.MATERIAL_DRIVER}
            and item.display_role is not DisplayRole.PRIMARY
            for item in CLOUD_CONTROL_SELECTION_PROFILE.selections
        ),
    }


def orthogonal_story_selection_contracts() -> dict[str, Any]:
    """Return durable product-selection contracts with explicit ordered stages."""

    return {
        "analytics_readiness": {
            "capabilities": [
                "LATEST_READY",
                "QOQ_READY",
                "YOY_READY",
                "TREND_4Q_READY",
                "SPARKLINE_12Q_READY",
                "TTM_READY",
                "SEMANTIC_READY",
            ],
            "economic_importance_gate": False,
        },
        "data_readiness": {
            "question": "WHAT_CAN_THE_ENGINE_SAFELY_ANALYZE_TODAY",
            "states": [item.value for item in DataReadiness],
            "economic_importance_gate": False,
        },
        "display_role": {
            "states": [item.value for item in DisplayRole],
            "economic_importance_mutation_allowed": False,
        },
        "economic_importance": {
            "states": [item.value for item in EconomicImportance],
            "forbidden_automatic_inputs": [
                "DATA_QUALITY",
                "HISTORICAL_DEPTH",
                "LEGACY_WORKBOOK_VISIBILITY",
                "NUMERIC_OBSERVATION_COUNT",
                "CURRENT_SIGNAL_POLARITY",
            ],
        },
        "investor_story": {
            "contract_version": OPERATING_DRIVER_INVESTOR_STORY_CONTRACT_VERSION,
            "economic_owner": False,
            "statistical_clustering": False,
            "canonical_metric_deletion_allowed": False,
        },
        "legacy_visibility_tier": {
            "contract_version": OPERATING_DRIVER_LEGACY_TIER_MIGRATION_CONTRACT_VERSION,
            "disposition": "LEGACY_COMBINED_VISIBILITY_TIER_DEPRECATED",
            "authoritative": False,
        },
        "monitoring_status": {
            "states": [item.value for item in MonitoringStatus],
            "non_normal_requires": [
                "MONITORING_REASON",
                "AUTHORITY_OR_EVIDENCE",
                "EFFECTIVE_PERIOD",
                "REVIEW_CONDITION",
            ],
            "negative_semantic_signal_implies_watch": False,
            "watch_implies_negative": False,
        },
        "new_ticker_product_selection": {
            "contract_version": OPERATING_DRIVER_NEW_TICKER_SELECTION_CONTRACT_VERSION,
            "required_outputs": [
                "ECONOMIC_IMPORTANCE",
                "MONITORING_STATUS",
                "DATA_READINESS",
                "DISPLAY_ROLE",
                "SEMANTIC_PROFILE_STATUS",
                "CONTEXT_PROFILE_STATUS",
                "STORY_MEMBERSHIP",
            ],
            "unresolved_material_result": "NEW_TICKER_PRODUCT_SELECTION_NEEDS_REVIEW",
            "review_focus": "MATERIAL_UNRESOLVED_QUESTIONS_NOT_EVERY_RAW_KPI",
        },
        "orthogonality": {
            "contract_version": OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION,
            "authoritative_axes": [
                "ECONOMIC_IMPORTANCE",
                "MONITORING_STATUS",
                "DATA_READINESS",
                "DISPLAY_ROLE",
            ],
            "cross_axis_automatic_rewrites_allowed": False,
        },
        "story_compression": {
            "accepted_inputs": [
                "CANONICAL_DRIVER_FAMILY",
                "CONTEXT_BUNDLE",
                "DEFINITION",
                "ECONOMIC_GROUP",
                "EXPLANATORY_RELATIONSHIP",
                "FINANCIAL_LINKAGE",
            ],
            "economic_importance_changed_due_only_to_display_compression": False,
            "statistical_correlation_clustering": False,
        },
    }


def selection_counts(
    records: Iterable[DriverProductSelection],
    attribute: str,
) -> dict[str, int]:
    counter = Counter(getattr(item, attribute).value for item in records)
    enum_type = {
        "economic_importance": EconomicImportance,
        "monitoring_status": MonitoringStatus,
        "data_readiness": DataReadiness,
        "display_role": DisplayRole,
    }[attribute]
    return {item.value: counter.get(item.value, 0) for item in enum_type}
