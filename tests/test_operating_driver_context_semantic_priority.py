from __future__ import annotations

import dataclasses
import inspect
import json

import pytest

from pbi_xbrl.longitudinal_memory.serialization import serialize_package

from pbi_xbrl.longitudinal_memory.operating_driver_foundation import (
    AggregationSemantics,
)
from pbi_xbrl.longitudinal_memory.operating_driver_semantic_priority import (
    ANF_PRIORITIES,
    CLOUD_BUNDLES,
    CLOUD_RELATIONSHIPS,
    CLOUD_RULES,
    COMMODITY_RULES,
    DEFAULT_SEMANTIC_CONFIGURATION,
    GPRE_PRIORITIES,
    LOGISTICS_RULES,
    OPERATING_DRIVER_CONTEXT_RELATIONSHIP_CONTRACT_VERSION,
    OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION,
    OPERATING_DRIVER_DRIVER_PRIORITY_CONTRACT_VERSION,
    OPERATING_DRIVER_NEW_TICKER_PROFILE_CONTRACT_VERSION,
    OPERATING_DRIVER_SEMANTIC_TREND_CONTRACT_VERSION,
    PBI_EXTRACTION_PRIORITIES,
    PBI_PRIORITIES,
    RETAIL_RULES,
    SHARED_SEMANTIC_RULES,
    AnalystSemanticOverride,
    BaseSemanticSignal,
    ColoringReadiness,
    ContextBundle,
    ContextCondition,
    ContextConditionKind,
    ContextEffectState,
    ContextPredicate,
    ContextRelationship,
    ContextRelationshipType,
    DriverPriority,
    EconomicGroup,
    EconomicInterpretation,
    ForecastSemanticReadiness,
    MathematicalDirection,
    OrdinalRating,
    PredicateField,
    PredicateMatch,
    PriorityDimensions,
    ProfileDriverReadinessState,
    ProfileProductReadiness,
    RuleLayer,
    RuleMatchKind,
    SemanticAuthority,
    SemanticMode,
    SemanticPriorityError,
    SemanticRule,
    TargetRange,
    TICKER_SEMANTIC_PROFILES,
    acceleration_semantic_effect,
    apply_context_relationships,
    derive_base_interpretation,
    evaluate_context_relationship,
    resolve_semantic_rule,
    semantic_contracts,
)
from pbi_xbrl.longitudinal_memory.operating_driver_shadow_profiles import (
    OPERATING_DRIVER_SHADOW_PROFILES,
)
from pbi_xbrl.longitudinal_memory.operating_driver_shadow_registry import (
    CanonicalDriverDefinition,
    DriverScope,
    FinancialLinkageKind,
    ForecastEvidenceCapability,
    VisibilityTier,
)


TOTAL = ({"dimension_id": "dimension:operating-driver:scope@1", "member_id": "member:operating-driver:total-company@1"},)
COMPONENT = ({"dimension_id": "dimension:operating-driver:segment@1", "member_id": "member:operating-driver:component@1"},)


def _definition(
    *,
    driver_id: str = "driver:synthetic:metric@1",
    family: str = "synthetic",
    unit: str = "unit:core:percent@1",
) -> CanonicalDriverDefinition:
    return CanonicalDriverDefinition(
        driver_id=driver_id,
        driver_family=family,
        canonical_label="Synthetic metric",
        display_label="Synthetic metric",
        definition_id=f"definition:{driver_id.split(':')[-1].split('@')[0]}@1",
        definition_version=1,
        definition_text="Synthetic stable metric definition.",
        unit_id=unit,
        scale="1",
        sign_convention="reported-positive",
        aggregation_semantics=AggregationSemantics.PERIOD_END,
        scope=DriverScope.GENERIC,
        visibility_tier=VisibilityTier.WATCH_DRIVER,
        financial_linkage=FinancialLinkageKind.EMPIRICAL_ASSOCIATION,
        forecast_capability=ForecastEvidenceCapability.FORECAST_CONTEXT,
    )


def _base(
    driver_id: str,
    mode: SemanticMode,
    direction: MathematicalDirection,
    *,
    latest: str | None = "1",
    base: EconomicInterpretation | None = None,
    momentum: str = "UNCHANGED_RATE",
    dimensions: tuple[dict[str, str], ...] = TOTAL,
    ticker: str = "SYNTHETIC",
    definition_break: bool = False,
) -> BaseSemanticSignal:
    interpretation = base or derive_base_interpretation(
        semantic_mode=mode,
        mathematical_direction=direction,
        latest_value=latest,
        definition_break_present=definition_break,
    )
    return BaseSemanticSignal(
        semantic_signal_id=f"semantic:{ticker}:{driver_id}:{dimensions[0]['member_id']}",
        source_analytical_signal_id=f"analysis:{driver_id}",
        ticker=ticker,
        driver_id=driver_id,
        definition_version=1,
        dimension_set_id=f"dimset:{dimensions[0]['member_id']}",
        dimensions=dimensions,
        latest_value=latest,
        mathematical_direction=direction,
        direction_basis="QOQ:synthetic",
        mathematical_momentum=momentum,
        semantic_mode=mode,
        base_interpretation=interpretation,
        interpretation_basis=("RULE:synthetic",),
        semantic_authority=SemanticAuthority.SECTOR_PACK_RULE,
        semantic_rule_id="semantic-rule:synthetic@1",
        target_range=None,
        definition_break_present=definition_break,
        numeric_semantic_output=latest is not None,
        source_lineage_ids=("evidence:synthetic@1",),
        financial_linkage="ECONOMICALLY_JUSTIFIED_MODEL",
        forecast_capability="LEADING_INDICATOR",
        qoq_available=True,
        yoy_available=True,
        trend_available=True,
        comparable_history_depth=12,
        lower_layer_analytics_sha256="a" * 64,
        validity_window="OPEN..OPEN",
    )


def _predicate(
    driver: str,
    field: PredicateField,
    *values: str,
    dimension: str | None = None,
) -> ContextPredicate:
    return ContextPredicate(
        reference_driver_id=driver,
        field=field,
        allowed_values=tuple(values),
        dimension_member=dimension,
    )


def _relationship(
    primary: str,
    predicates: tuple[ContextPredicate, ...],
    *,
    effect: ContextEffectState = ContextEffectState.CONFIRMED,
    final: EconomicInterpretation | None = None,
    resolves: bool = False,
    relationship_type: ContextRelationshipType = ContextRelationshipType.PREREQUISITE,
) -> ContextRelationship:
    return ContextRelationship(
        relationship_id=f"context:synthetic:{primary.split(':')[-1]}@1",
        primary_driver_id=primary,
        modifier_driver_or_accepted_product_fact=tuple(
            sorted(
                {
                    item.reference_driver_id
                    for item in predicates
                    if item.reference_driver_id != "PRIMARY"
                }
            )
        ),
        relationship_type=relationship_type,
        condition=ContextCondition(ContextConditionKind.PREDICATE_SET, predicates),
        effect_on_interpretation=effect,
        authority=SemanticAuthority.SECTOR_PACK_RULE,
        evidence_reference="Accepted synthetic economic archetype.",
        limitations=("No causal inference.",),
        scope="synthetic fixture",
        final_interpretation_when_true=final,
        allows_directional_resolution=resolves,
    )


def test_contract_versions_and_closed_states_are_explicit() -> None:
    assert OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION.endswith("@1")
    assert OPERATING_DRIVER_SEMANTIC_TREND_CONTRACT_VERSION.endswith("@1")
    assert OPERATING_DRIVER_CONTEXT_RELATIONSHIP_CONTRACT_VERSION.endswith("@1")
    assert OPERATING_DRIVER_DRIVER_PRIORITY_CONTRACT_VERSION.endswith("@1")
    assert OPERATING_DRIVER_NEW_TICKER_PROFILE_CONTRACT_VERSION.endswith("@1")
    assert {item.value for item in SemanticMode} == {
        "HIGHER_BETTER", "LOWER_BETTER", "TARGET_RANGE", "CONTEXT_DEPENDENT",
        "DIRECTION_ONLY", "NO_GOOD_BAD_SEMANTICS",
    }
    assert {item.value for item in ContextRelationshipType} == {
        "PREREQUISITE", "TRADEOFF", "CONFIRMING", "CONSTRAINT",
        "DENOMINATOR_CONTEXT", "DIVERGENCE", "REGIME_MODIFIER",
        "LEADING_LAGGING_CONTEXT",
    }


def test_default_configuration_is_shared_sector_ticker_declarative() -> None:
    assert set(DEFAULT_SEMANTIC_CONFIGURATION.ticker_profiles) == {"ANF", "PBI", "GPRE"}
    assert "MSFT" not in DEFAULT_SEMANTIC_CONFIGURATION.ticker_profiles
    assert "sector-pack:subscription-cloud@1" in DEFAULT_SEMANTIC_CONFIGURATION.sector_packs
    assert all(item.layer is RuleLayer.SHARED for item in SHARED_SEMANTIC_RULES)
    assert all(item.layer is RuleLayer.SECTOR_PACK for item in (*RETAIL_RULES, *LOGISTICS_RULES, *COMMODITY_RULES, *CLOUD_RULES))
    assert all(item.authority is SemanticAuthority.TICKER_PROFILE_RULE for profile in TICKER_SEMANTIC_PROFILES.values() for item in profile.relationships)


def test_no_ticker_specific_python_semantic_branch() -> None:
    import pbi_xbrl.longitudinal_memory.operating_driver_semantic_priority as module

    source = inspect.getsource(module)
    for ticker in ("ANF", "PBI", "GPRE", "MSFT"):
        assert f'if ticker == "{ticker}"' not in source
        assert f"if ticker == '{ticker}'" not in source


def test_unreviewed_driver_fails_closed() -> None:
    definition = _definition(family="unknown")
    rule = resolve_semantic_rule(
        definition,
        shared_rules=SHARED_SEMANTIC_RULES,
        sector_rules=(),
        ticker_rules=(),
    )
    assert rule.semantic_mode is SemanticMode.NO_GOOD_BAD_SEMANTICS
    assert rule.authority is SemanticAuthority.UNRESOLVED


def test_rule_precedence_is_shared_then_sector_then_ticker() -> None:
    definition = _definition(driver_id="driver:synthetic:demand@1", family="demand")
    sector = SemanticRule("rule:sector", RuleMatchKind.DRIVER_FAMILY, "demand", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, "sector", RuleLayer.SECTOR_PACK)
    ticker = SemanticRule("rule:ticker", RuleMatchKind.DRIVER_ID, definition.driver_id, SemanticMode.CONTEXT_DEPENDENT, SemanticAuthority.TICKER_PROFILE_RULE, "ticker", RuleLayer.TICKER_PROFILE)
    assert resolve_semantic_rule(definition, shared_rules=SHARED_SEMANTIC_RULES, sector_rules=(sector,), ticker_rules=(ticker,)) is ticker


def test_competing_same_layer_rules_fail_closed() -> None:
    definition = _definition(family="demand")
    rules = (
        SemanticRule("rule:a", RuleMatchKind.DRIVER_FAMILY, "demand", SemanticMode.HIGHER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, "a", RuleLayer.SECTOR_PACK),
        SemanticRule("rule:b", RuleMatchKind.DRIVER_FAMILY, "demand", SemanticMode.LOWER_BETTER, SemanticAuthority.SECTOR_PACK_RULE, "b", RuleLayer.SECTOR_PACK),
    )
    with pytest.raises(SemanticPriorityError, match="Ambiguous"):
        resolve_semantic_rule(definition, shared_rules=SHARED_SEMANTIC_RULES, sector_rules=rules, ticker_rules=())


def test_target_range_requires_accepted_bounds() -> None:
    with pytest.raises(SemanticPriorityError, match="explicit bounds"):
        SemanticRule("rule:range", RuleMatchKind.DRIVER_ID, "driver:x@1", SemanticMode.TARGET_RANGE, SemanticAuthority.ANALYST_CURATED, "range", RuleLayer.TICKER_PROFILE)


def test_target_range_interpretation_is_explicit() -> None:
    target = TargetRange("80", "95", "unit:core:percent@1", "stable capacity", authority_reference="profile:accepted@1")
    assert derive_base_interpretation(semantic_mode=SemanticMode.TARGET_RANGE, mathematical_direction=MathematicalDirection.UP, latest_value="90", target_range=target) is EconomicInterpretation.WITHIN_TARGET
    assert derive_base_interpretation(semantic_mode=SemanticMode.TARGET_RANGE, mathematical_direction=MathematicalDirection.UP, latest_value="96", target_range=target) is EconomicInterpretation.OUTSIDE_TARGET_HIGH
    assert derive_base_interpretation(semantic_mode=SemanticMode.TARGET_RANGE, mathematical_direction=MathematicalDirection.DOWN, latest_value="79", target_range=target) is EconomicInterpretation.OUTSIDE_TARGET_LOW


@pytest.mark.parametrize(
    ("mode", "direction", "expected"),
    [
        (SemanticMode.HIGHER_BETTER, MathematicalDirection.UP, EconomicInterpretation.POSITIVE),
        (SemanticMode.HIGHER_BETTER, MathematicalDirection.DOWN, EconomicInterpretation.NEGATIVE),
        (SemanticMode.LOWER_BETTER, MathematicalDirection.UP, EconomicInterpretation.NEGATIVE),
        (SemanticMode.LOWER_BETTER, MathematicalDirection.DOWN, EconomicInterpretation.POSITIVE),
        (SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP, EconomicInterpretation.CONTEXT_DEPENDENT),
        (SemanticMode.DIRECTION_ONLY, MathematicalDirection.UP, EconomicInterpretation.NOT_INTERPRETABLE),
        (SemanticMode.NO_GOOD_BAD_SEMANTICS, MathematicalDirection.UP, EconomicInterpretation.NOT_INTERPRETABLE),
    ],
)
def test_base_semantic_modes(mode: SemanticMode, direction: MathematicalDirection, expected: EconomicInterpretation) -> None:
    assert derive_base_interpretation(semantic_mode=mode, mathematical_direction=direction, latest_value="1") is expected


def test_missing_and_definition_break_are_insufficient_evidence() -> None:
    assert derive_base_interpretation(semantic_mode=SemanticMode.HIGHER_BETTER, mathematical_direction=MathematicalDirection.UP, latest_value=None) is EconomicInterpretation.INSUFFICIENT_EVIDENCE
    assert derive_base_interpretation(semantic_mode=SemanticMode.HIGHER_BETTER, mathematical_direction=MathematicalDirection.UP, latest_value="1", definition_break_present=True) is EconomicInterpretation.INSUFFICIENT_EVIDENCE


def test_context_cannot_cross_a_definition_break() -> None:
    primary = _base(
        "driver:synthetic:utilization@1",
        SemanticMode.CONTEXT_DEPENDENT,
        MathematicalDirection.UP,
        definition_break=True,
    )
    economics = _base(
        "driver:synthetic:margin@1",
        SemanticMode.HIGHER_BETTER,
        MathematicalDirection.UP,
    )
    relationship = _relationship(
        primary.driver_id,
        (_predicate(economics.driver_id, PredicateField.BASE_INTERPRETATION, "POSITIVE"),),
        final=EconomicInterpretation.POSITIVE,
        resolves=True,
    )
    result = apply_context_relationships(primary, (primary, economics), (relationship,))
    assert result.final_interpretation is EconomicInterpretation.INSUFFICIENT_EVIDENCE
    assert result.context_interaction_result is ContextEffectState.NOT_EVALUABLE


def test_positive_acceleration_does_not_create_positive_semantics() -> None:
    contextual = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP, momentum="POSITIVE_ACCELERATION")
    assert contextual.base_interpretation is EconomicInterpretation.CONTEXT_DEPENDENT
    assert acceleration_semantic_effect(contextual) == "MATHEMATICALLY_DESCRIPTIVE_ONLY"


def test_acceleration_strengthens_only_existing_base_semantic() -> None:
    positive = _base("driver:synthetic:demand@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UP, momentum="POSITIVE_ACCELERATION")
    slowing_decline = _base("driver:synthetic:demand@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.DOWN, momentum="POSITIVE_ACCELERATION")
    assert acceleration_semantic_effect(positive) == "STRENGTHENS"
    assert acceleration_semantic_effect(slowing_decline) == "ATTENUATES"
    assert slowing_decline.base_interpretation is EconomicInterpretation.NEGATIVE


def test_volume_up_without_unit_economics_remains_context_dependent() -> None:
    volume = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    result = apply_context_relationships(volume, (volume,), ())
    assert result.final_interpretation is EconomicInterpretation.CONTEXT_DEPENDENT


def test_volume_up_with_positive_unit_economics_can_resolve_explicitly() -> None:
    volume = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    economics = _base("driver:synthetic:margin@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UP)
    relationship = _relationship(
        volume.driver_id,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(economics.driver_id, PredicateField.BASE_INTERPRETATION, "POSITIVE"),
        ),
        final=EconomicInterpretation.POSITIVE,
        resolves=True,
    )
    result = apply_context_relationships(volume, (volume, economics), (relationship,))
    assert result.final_interpretation is EconomicInterpretation.POSITIVE
    assert result.context_interaction_result is ContextEffectState.CONFIRMED


def test_volume_up_with_negative_economics_not_automatically_positive() -> None:
    volume = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    economics = _base("driver:synthetic:margin@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.DOWN)
    relationship = _relationship(
        volume.driver_id,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(economics.driver_id, PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
        ),
        effect=ContextEffectState.MIXED,
        final=EconomicInterpretation.MIXED,
        relationship_type=ContextRelationshipType.TRADEOFF,
    )
    assert apply_context_relationships(volume, (volume, economics), (relationship,)).final_interpretation is EconomicInterpretation.MIXED


def test_price_up_with_stable_volume_and_retention_is_confirmed_by_explicit_rule() -> None:
    price = _base("driver:synthetic:price@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    volume = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UNCHANGED)
    retention = _base("driver:synthetic:retention@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UNCHANGED)
    relationship = _relationship(
        price.driver_id,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(volume.driver_id, PredicateField.MATHEMATICAL_DIRECTION, "UNCHANGED", "UP"),
            _predicate(retention.driver_id, PredicateField.MATHEMATICAL_DIRECTION, "UNCHANGED", "UP"),
        ),
        final=EconomicInterpretation.POSITIVE,
        resolves=True,
        relationship_type=ContextRelationshipType.CONFIRMING,
    )
    assert apply_context_relationships(price, (price, volume, retention), (relationship,)).final_interpretation is EconomicInterpretation.POSITIVE


def test_price_up_volume_down_client_losses_up_is_mixed() -> None:
    price = _base("driver:synthetic:price@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    volume = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.DOWN)
    client_loss = _base("driver:synthetic:client-loss@1", SemanticMode.LOWER_BETTER, MathematicalDirection.UP)
    relationship = _relationship(
        price.driver_id,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(volume.driver_id, PredicateField.MATHEMATICAL_DIRECTION, "DOWN"),
            _predicate(client_loss.driver_id, PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
        ),
        effect=ContextEffectState.MIXED,
        final=EconomicInterpretation.MIXED,
        relationship_type=ContextRelationshipType.TRADEOFF,
    )
    assert apply_context_relationships(price, (price, volume, client_loss), (relationship,)).final_interpretation is EconomicInterpretation.MIXED


def test_inventory_up_demand_down_clearance_is_mixed() -> None:
    inventory = _base("driver:synthetic:inventory@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    demand = _base("driver:synthetic:demand@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.DOWN)
    clearance = _base("driver:synthetic:clearance@1", SemanticMode.LOWER_BETTER, MathematicalDirection.UP)
    relationship = _relationship(
        inventory.driver_id,
        (
            _predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),
            _predicate(demand.driver_id, PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
            _predicate(clearance.driver_id, PredicateField.BASE_INTERPRETATION, "NEGATIVE"),
        ),
        effect=ContextEffectState.MIXED,
        final=EconomicInterpretation.MIXED,
        relationship_type=ContextRelationshipType.DENOMINATOR_CONTEXT,
    )
    assert apply_context_relationships(inventory, (inventory, demand, clearance), (relationship,)).final_interpretation is EconomicInterpretation.MIXED


def test_inventory_up_demand_up_growth_is_not_automatically_negative() -> None:
    inventory = _base("driver:synthetic:inventory@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    demand = _base("driver:synthetic:demand@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UP)
    result = apply_context_relationships(inventory, (inventory, demand), ())
    assert result.final_interpretation is EconomicInterpretation.CONTEXT_DEPENDENT


def test_utilization_up_without_range_or_economics_is_context_dependent() -> None:
    utilization = _base("driver:synthetic:utilization@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    assert apply_context_relationships(utilization, (utilization,), ()).final_interpretation is EconomicInterpretation.CONTEXT_DEPENDENT


def test_utilization_up_with_positive_economics_can_resolve() -> None:
    utilization = _base("driver:synthetic:utilization@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    economics = _base("driver:synthetic:margin@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UP)
    relationship = _relationship(
        utilization.driver_id,
        (_predicate(economics.driver_id, PredicateField.BASE_INTERPRETATION, "POSITIVE"),),
        final=EconomicInterpretation.POSITIVE,
        resolves=True,
    )
    assert apply_context_relationships(utilization, (utilization, economics), (relationship,)).final_interpretation is EconomicInterpretation.POSITIVE


def test_backlog_up_without_quality_context_requires_context() -> None:
    backlog = _base("driver:synthetic:backlog@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    relationship = _relationship(
        backlog.driver_id,
        (_predicate("driver:synthetic:duration@1", PredicateField.NUMERIC_AVAILABLE, "true"),),
        effect=ContextEffectState.CONTEXT_REQUIRED,
    )
    result = apply_context_relationships(backlog, (backlog,), (relationship,))
    assert result.context_interaction_result is ContextEffectState.CONTEXT_REQUIRED
    assert result.final_interpretation is EconomicInterpretation.CONTEXT_DEPENDENT


def test_margin_down_under_capacity_investment_regime_is_base_negative_final_mixed() -> None:
    margin = _base("driver:synthetic:margin@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.DOWN)
    investment = _base("driver:synthetic:investment@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    relationship = _relationship(
        margin.driver_id,
        (_predicate(investment.driver_id, PredicateField.MATHEMATICAL_DIRECTION, "UP"),),
        effect=ContextEffectState.MIXED,
        final=EconomicInterpretation.MIXED,
        relationship_type=ContextRelationshipType.REGIME_MODIFIER,
    )
    result = apply_context_relationships(margin, (margin, investment), (relationship,))
    assert result.base_interpretation is EconomicInterpretation.NEGATIVE
    assert result.final_interpretation is EconomicInterpretation.MIXED


def test_aggregate_component_divergence_is_surfaced() -> None:
    total = _base("driver:synthetic:comps@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UP, latest="3", dimensions=TOTAL)
    component = _base("driver:synthetic:comps@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.DOWN, latest="-7", dimensions=COMPONENT)
    relationship = ContextRelationship(
        relationship_id="context:synthetic:divergence@1",
        primary_driver_id=total.driver_id,
        modifier_driver_or_accepted_product_fact=(total.driver_id,),
        relationship_type=ContextRelationshipType.DIVERGENCE,
        condition=ContextCondition(ContextConditionKind.AGGREGATE_COMPONENT_SIGN_DIVERGENCE, aggregate_dimension_member=TOTAL[0]["member_id"]),
        effect_on_interpretation=ContextEffectState.MIXED,
        authority=SemanticAuthority.SECTOR_PACK_RULE,
        evidence_reference="Synthetic aggregate/component definition.",
        limitations=(),
        scope="synthetic",
        primary_dimension_member=TOTAL[0]["member_id"],
        final_interpretation_when_true=EconomicInterpretation.MIXED,
    )
    result = apply_context_relationships(total, (total, component), (relationship,))
    assert result.context_interaction_result is ContextEffectState.MIXED
    assert result.final_interpretation is EconomicInterpretation.MIXED


def test_context_rule_cannot_rewrite_lower_layer_math() -> None:
    primary = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    before = dataclasses.asdict(primary)
    relationship = _relationship(primary.driver_id, (_predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),), final=EconomicInterpretation.POSITIVE, resolves=True)
    result = apply_context_relationships(primary, (primary,), (relationship,))
    assert dataclasses.asdict(primary) == before
    assert result.mathematical_direction is MathematicalDirection.UP


def test_context_cannot_manufacture_strong_signal_without_resolution_authority() -> None:
    primary = _base(
        "driver:synthetic:volume@1",
        SemanticMode.CONTEXT_DEPENDENT,
        MathematicalDirection.UP,
    )
    relationship = _relationship(
        primary.driver_id,
        (_predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),),
        final=EconomicInterpretation.POSITIVE,
        resolves=False,
    )
    with pytest.raises(SemanticPriorityError, match="directional resolution"):
        evaluate_context_relationship(primary, (primary,), relationship)


def test_context_cannot_make_qualitative_no_good_bad_signal_positive() -> None:
    primary = _base("driver:synthetic:milestone@1", SemanticMode.NO_GOOD_BAD_SEMANTICS, MathematicalDirection.UP)
    relationship = _relationship(primary.driver_id, (_predicate("PRIMARY", PredicateField.MATHEMATICAL_DIRECTION, "UP"),), final=EconomicInterpretation.POSITIVE, resolves=True)
    with pytest.raises(SemanticPriorityError, match="no-good/bad"):
        evaluate_context_relationship(primary, (primary,), relationship)


def test_context_bundles_are_never_economic_owners() -> None:
    for bundle in (*CLOUD_BUNDLES, *(bundle for profile in TICKER_SEMANTIC_PROFILES.values() for bundle in profile.bundles)):
        assert bundle.to_dict()["economic_owner"] is False
        assert "value" not in bundle.to_dict()


def test_core_priority_requires_all_hard_gates_and_reason() -> None:
    dims = PriorityDimensions(*(OrdinalRating.HIGH for _ in range(8)))
    with pytest.raises(SemanticPriorityError, match="CORE"):
        DriverPriority(
            priority_id="priority:test",
            ticker="TEST",
            driver_id="driver:test@1",
            active_definition_version=1,
            signal_ids=("signal:test",),
            visibility_tier=VisibilityTier.CORE_DRIVER,
            economic_group=EconomicGroup.DEMAND_VOLUME,
            dimensions=dims,
            hard_gates={"lineage": False},
            reason="Explicit but failed gate.",
            baseline_tier=None,
            context_aware_tier_change=False,
            coloring_readiness=ColoringReadiness.NOT_READY,
            sparkline_12q_ready=False,
            trend_4q_ready=False,
            qoq_ready=False,
            yoy_ready=False,
            forecast_readiness=ForecastSemanticReadiness.NEEDS_DATA,
            profile_review_required_during_onboarding=False,
        )


def test_priority_contract_has_no_opaque_weighted_score() -> None:
    contract = semantic_contracts()["priority"]
    assert contract["hidden_weighted_score"] is False
    assert contract["lexicographic_tie_break"][0] == {
        "dimension": "FINANCIAL_MATERIALITY",
        "ordinal": 1,
    }
    assert [item["ordinal"] for item in contract["lexicographic_tie_break"]] == list(
        range(1, 8)
    )
    for profile in TICKER_SEMANTIC_PROFILES.values():
        for priority in profile.priority_specs:
            assert "score" not in priority.to_dict()

    onboarding = semantic_contracts()["new_ticker_onboarding"]
    assert onboarding["sequence"][0] == {"ordinal": 1, "stage": "SOURCE_CENSUS"}
    assert onboarding["sequence"][-1] == {"ordinal": 12, "stage": "INVESTOR_PRODUCT"}

    serialized = json.loads(serialize_package({"contracts": semantic_contracts()}))
    assert serialized["contracts"]["priority"]["lexicographic_tie_break"][0][
        "dimension"
    ] == "FINANCIAL_MATERIALITY"
    assert serialized["contracts"]["new_ticker_onboarding"]["sequence"][0][
        "stage"
    ] == "SOURCE_CENSUS"


def test_new_ticker_without_profile_fails_closed() -> None:
    with pytest.raises(SemanticPriorityError, match="NEW_TICKER_DRIVER_PROFILE_NEEDS_REVIEW"):
        DEFAULT_SEMANTIC_CONFIGURATION.profile("NEWCO")


def test_analyst_override_is_semantic_only_and_effective_dated() -> None:
    override = AnalystSemanticOverride(
        override_id="override:test@1",
        target_driver_id="driver:test@1",
        reason="Accepted analyst review.",
        effective_from="2026-08-17",
        visibility_tier=VisibilityTier.WATCH_DRIVER,
    )
    assert override.to_dict()["historical_observation_mutation_allowed"] is False
    assert "value" not in override.to_dict()


def test_priority_profiles_reconcile_all_canonical_driver_ids() -> None:
    configured = {"ANF": ANF_PRIORITIES, "PBI": PBI_PRIORITIES, "GPRE": GPRE_PRIORITIES}
    for ticker, profile in OPERATING_DRIVER_SHADOW_PROFILES.items():
        actual = {item.driver_id for item in profile.definitions}
        priorities = {item.driver_id for item in configured[ticker]}
        assert priorities == actual


def test_pbi_remains_watch_support_without_numeric_promotion() -> None:
    assert {item.visibility_tier for item in PBI_PRIORITIES} == {VisibilityTier.WATCH_DRIVER, VisibilityTier.SUPPORT_ONLY}
    assert all(item.onboarding_review_required for item in PBI_PRIORITIES)
    assert [item["rank"] for item in PBI_EXTRACTION_PRIORITIES] == list(range(1, 7))


def test_microsoft_like_cloud_fixture_is_not_a_production_profile() -> None:
    assert "MSFT" not in TICKER_SEMANTIC_PROFILES
    assert {item.match_value for item in CLOUD_RULES} >= {"paid-seats", "cloud-revenue", "backlog", "margin", "capacity"}
    assert {item.relationship_type for item in CLOUD_RELATIONSHIPS} >= {
        ContextRelationshipType.CONFIRMING,
        ContextRelationshipType.PREREQUISITE,
        ContextRelationshipType.CONSTRAINT,
        ContextRelationshipType.REGIME_MODIFIER,
    }


def test_all_profile_relationships_are_traceable_and_versioned() -> None:
    for profile in TICKER_SEMANTIC_PROFILES.values():
        for relationship in profile.relationships:
            assert relationship.relationship_id.endswith("@1")
            assert relationship.evidence_reference
            assert relationship.scope
            assert relationship.authority is SemanticAuthority.TICKER_PROFILE_RULE


def test_profile_readiness_contract_is_closed() -> None:
    assert {item.value for item in ProfileDriverReadinessState} == {
        "PROFILE_READY", "PROFILE_READY_WITH_NEUTRAL_SEMANTICS",
        "PROFILE_NEEDS_REVIEW", "PROFILE_BLOCKED",
    }
    assert {item.value for item in ProfileProductReadiness} == {
        "OPERATING_DRIVERS_PROFILE_READY",
        "OPERATING_DRIVERS_PROFILE_REVIEW_REQUIRED",
    }


def test_forecast_numbers_are_absent_from_contextual_signal() -> None:
    signal = _base("driver:synthetic:demand@1", SemanticMode.HIGHER_BETTER, MathematicalDirection.UP)
    contextual = apply_context_relationships(signal, (signal,), ())
    assert contextual.to_dict()["forecast_number"] is None


def test_color_readiness_does_not_force_context_dependent_green_red() -> None:
    signal = _base("driver:synthetic:volume@1", SemanticMode.CONTEXT_DEPENDENT, MathematicalDirection.UP)
    contextual = apply_context_relationships(signal, (signal,), ())
    assert contextual.coloring_readiness is ColoringReadiness.SAFE_FOR_DIRECTION_ONLY


def test_context_registry_implements_all_required_relationship_types() -> None:
    implemented = set(ContextRelationshipType)
    configured = {
        item.relationship_type
        for profile in TICKER_SEMANTIC_PROFILES.values()
        for item in profile.relationships
    } | {item.relationship_type for item in CLOUD_RELATIONSHIPS}
    assert implemented >= configured
    assert implemented == set(ContextRelationshipType)


def test_serialized_contracts_have_no_workbook_coordinates() -> None:
    payload = repr(semantic_contracts()).lower()
    assert "cell" not in payload
    assert "worksheet" not in payload
    assert "workbook_coordinates" in payload
    assert semantic_contracts()["context_relationship"]["workbook_coordinates"] is False


def test_semantic_contract_rejects_bullish_bearish_buy_sell_states() -> None:
    payload = repr(semantic_contracts())
    for forbidden in ("BULLISH", "BEARISH", "BUY", "SELL"):
        assert forbidden not in payload
