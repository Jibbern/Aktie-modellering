from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_semantic_priority import (
    ColoringReadiness,
    EconomicGroup,
    EconomicInterpretation,
)
from pbi_xbrl.longitudinal_memory.operating_driver_story_selection import (
    ANF_SELECTIONS,
    ANF_STORIES,
    CLOUD_CONTROL_SELECTION_PROFILE,
    DEFAULT_STORY_SELECTION_CONFIGURATION,
    GPRE_SELECTIONS,
    GPRE_STORIES,
    OPERATING_DRIVER_INVESTOR_STORY_CONTRACT_VERSION,
    OPERATING_DRIVER_LEGACY_TIER_MIGRATION_CONTRACT_VERSION,
    OPERATING_DRIVER_NEW_TICKER_SELECTION_CONTRACT_VERSION,
    OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION,
    PBI_SELECTIONS,
    PBI_STORIES,
    AnalyticsReadiness,
    DataReadiness,
    DisplayRole,
    DriverSelectionSpec,
    EconomicImportance,
    InvestorDriverStory,
    InvestorStorySpec,
    MonitoringStatus,
    OnboardingBurden,
    StorySelectionConfiguration,
    StorySelectionError,
    StorySelectionProfile,
    UIReadiness,
    build_orthogonal_story_selection,
    combined_product_review_queue,
    microsoft_cross_sector_product_selection_fixture,
    orthogonal_story_selection_contracts,
)


DRIVER_A = "driver:test:a@1"
DRIVER_B = "driver:test:b@1"


def _spec(
    driver_id: str = DRIVER_A,
    *,
    importance: EconomicImportance = EconomicImportance.KEY_DRIVER,
    monitoring: MonitoringStatus = MonitoringStatus.NORMAL,
    readiness: DataReadiness | None = DataReadiness.READY_NUMERIC,
    role: DisplayRole = DisplayRole.PRIMARY,
    story_id: str = "story:test@1",
    onboarding_review: bool = False,
) -> DriverSelectionSpec:
    return DriverSelectionSpec(
        driver_id=driver_id,
        economic_importance=importance,
        monitoring_status=monitoring,
        display_role=role,
        story_id=story_id,
        importance_reason="Explicit economic evidence.",
        selection_reason="Explicit product-selection evidence.",
        data_readiness_override=readiness,
        monitoring_reason="Explicit monitoring evidence.",
        monitoring_authority="TEST_DECLARATIVE_PROFILE",
        monitoring_effective_period="2026-Q1",
        monitoring_review_condition=(
            "Review when the accepted condition changes."
            if monitoring is not MonitoringStatus.NORMAL
            else None
        ),
        onboarding_review_required=onboarding_review,
    )


def _story(
    *driver_ids: str,
    story_id: str = "story:test@1",
    primary: tuple[str, ...] | None = None,
    context: tuple[str, ...] = (),
    diagnostic: tuple[str, ...] = (),
) -> InvestorStorySpec:
    return InvestorStorySpec(
        story_id=story_id,
        economic_question="TEST_ECONOMIC_QUESTION",
        economic_group=EconomicGroup.DEMAND_VOLUME,
        primary_drivers=primary or (driver_ids[0],),
        context_drivers=context,
        diagnostic_drivers=diagnostic,
        selection_reasoning="Compact deterministic story selection.",
    )


def _profile(
    specs: tuple[DriverSelectionSpec, ...],
    stories: tuple[InvestorStorySpec, ...],
    *,
    ticker: str = "TEST",
) -> StorySelectionProfile:
    return StorySelectionProfile(
        ticker=ticker,
        profile_version="operating-driver-story-profile:test@1",
        selections=specs,
        stories=stories,
        onboarding_burden=OnboardingBurden(
            len(specs), len(specs), len(specs), len(specs), len(specs), 0
        ),
        ui_readiness=UIReadiness.READY_FOR_INVESTOR_UI_PREVIEW,
    )


def _fake_inputs(
    *,
    ticker: str = "TEST",
    driver_id: str = DRIVER_A,
    latest_value: str | None = "1",
    numeric: bool = True,
    definition_break: bool = False,
    interpretation: EconomicInterpretation = EconomicInterpretation.POSITIVE,
    legacy_tier: str = "CORE_DRIVER",
):
    enum = lambda value: SimpleNamespace(value=value)
    base = SimpleNamespace(
        driver_id=driver_id,
        definition_break_present=definition_break,
        numeric_semantic_output=numeric,
        latest_value=latest_value,
        source_lineage_ids=("lineage:test@1",),
        base_interpretation=interpretation,
        mathematical_direction=enum("UP"),
        financial_linkage="ECONOMICALLY_JUSTIFIED_MODEL",
        forecast_capability="LEADING_INDICATOR",
    )
    context = SimpleNamespace(
        driver_id=driver_id,
        final_interpretation=interpretation,
        context_interaction_result=enum("NO_EFFECT"),
        semantic_signal_id="semantic:test@1",
        source_lineage_ids=("lineage:test@1",),
    )
    priority = SimpleNamespace(
        driver_id=driver_id,
        active_definition_version=1,
        economic_group=EconomicGroup.DEMAND_VOLUME,
        qoq_ready=True,
        yoy_ready=True,
        trend_4q_ready=True,
        sparkline_12q_ready=True,
        coloring_readiness=ColoringReadiness.SAFE_FOR_POSITIVE_NEGATIVE_FILL,
        visibility_tier=enum(legacy_tier),
        dimensions=SimpleNamespace(forward_relevance=enum("HIGH")),
    )
    analytics_bytes = b"accepted-analytics-package"
    semantic_bytes = b"accepted-semantic-package"
    analytics = SimpleNamespace(
        ticker=ticker,
        sha256="a" * 64,
        ttm_analytics=(
            SimpleNamespace(driver_id=driver_id, availability=enum("AVAILABLE")),
        ),
        serialize=lambda: analytics_bytes,
    )
    semantic = SimpleNamespace(
        ticker=ticker,
        sha256="b" * 64,
        derived_analytics_sha256=analytics.sha256,
        driver_priorities=(priority,),
        profile_readiness=(
            SimpleNamespace(
                driver_id=driver_id,
                state=enum("PROFILE_READY"),
                context_dependencies_declared=True,
            ),
        ),
        base_semantic_signals=(base,),
        contextual_semantic_signals=(context,),
        context_bundles=(),
        serialize=lambda: semantic_bytes,
    )
    return semantic, analytics, semantic_bytes, analytics_bytes


def _build_one(
    *,
    importance: EconomicImportance = EconomicImportance.KEY_DRIVER,
    monitoring: MonitoringStatus = MonitoringStatus.NORMAL,
    readiness: DataReadiness = DataReadiness.READY_NUMERIC,
    role: DisplayRole = DisplayRole.PRIMARY,
    interpretation: EconomicInterpretation = EconomicInterpretation.POSITIVE,
    onboarding_review: bool = False,
):
    spec = _spec(
        importance=importance,
        monitoring=monitoring,
        readiness=readiness,
        role=role,
        onboarding_review=onboarding_review,
    )
    profile = _profile((spec,), (_story(DRIVER_A),))
    configuration = StorySelectionConfiguration(profiles={"TEST": profile})
    semantic, analytics, semantic_bytes, analytics_bytes = _fake_inputs(
        interpretation=interpretation
    )
    package = build_orthogonal_story_selection(
        semantic, analytics, configuration=configuration
    )
    return package, semantic, analytics, semantic_bytes, analytics_bytes


@pytest.mark.parametrize(
    ("importance", "monitoring", "readiness", "role"),
    (
        (
            EconomicImportance.KEY_DRIVER,
            MonitoringStatus.WATCH,
            DataReadiness.READY_NUMERIC,
            DisplayRole.PRIMARY,
        ),
        (
            EconomicImportance.KEY_DRIVER,
            MonitoringStatus.NORMAL,
            DataReadiness.NEEDS_DATA,
            DisplayRole.PRIMARY,
        ),
        (
            EconomicImportance.KEY_DRIVER,
            MonitoringStatus.NORMAL,
            DataReadiness.READY_NUMERIC,
            DisplayRole.CONTEXT,
        ),
        (
            EconomicImportance.MATERIAL_DRIVER,
            MonitoringStatus.EMERGING,
            DataReadiness.READY_QUALITATIVE,
            DisplayRole.PRIMARY,
        ),
        (
            EconomicImportance.SUPPORTING_DRIVER,
            MonitoringStatus.WATCH,
            DataReadiness.READY_NUMERIC,
            DisplayRole.DIAGNOSTIC,
        ),
    ),
)
def test_orthogonal_axis_combinations_are_valid(
    importance, monitoring, readiness, role
):
    selected = _spec(
        importance=importance,
        monitoring=monitoring,
        readiness=readiness,
        role=role,
        onboarding_review=importance is EconomicImportance.UNRESOLVED,
    )
    assert (
        selected.economic_importance,
        selected.monitoring_status,
        selected.data_readiness_override,
        selected.display_role,
    ) == (importance, monitoring, readiness, role)


def test_key_needs_data_primary_enters_review_without_downgrade():
    package, *_ = _build_one(readiness=DataReadiness.NEEDS_DATA)
    assert package.selections[0].economic_importance is EconomicImportance.KEY_DRIVER
    assert package.review_queue[0].issue_type == DataReadiness.NEEDS_DATA.value


def test_ready_numeric_supporting_diagnostic_is_not_promoted():
    selected = _spec(
        importance=EconomicImportance.SUPPORTING_DRIVER,
        role=DisplayRole.DIAGNOSTIC,
    )
    assert selected.economic_importance is EconomicImportance.SUPPORTING_DRIVER
    assert selected.display_role is DisplayRole.DIAGNOSTIC


def test_watch_does_not_imply_negative_semantics():
    package, *_ = _build_one(monitoring=MonitoringStatus.WATCH)
    assert package.selections[0].current_semantic_interpretation == "POSITIVE"


def test_negative_semantics_do_not_imply_watch():
    package, *_ = _build_one(interpretation=EconomicInterpretation.NEGATIVE)
    assert package.selections[0].monitoring_status is MonitoringStatus.NORMAL
    assert package.selections[0].current_semantic_interpretation == "NEGATIVE"


def test_selection_does_not_mutate_semantic_or_analytics_packages():
    package, semantic, analytics, semantic_bytes, analytics_bytes = _build_one()
    assert package.selections[0].display_role is DisplayRole.PRIMARY
    assert semantic.serialize() == semantic_bytes
    assert analytics.serialize() == analytics_bytes


def test_legacy_visibility_tier_is_migration_evidence_only():
    package, *_ = _build_one()
    migration = package.legacy_migrations[0]
    assert migration.legacy_visibility_tier == "CORE_DRIVER"
    assert migration.legacy_authoritative is False
    assert migration.legacy_contract_disposition == (
        "LEGACY_COMBINED_VISIBILITY_TIER_DEPRECATED"
    )
    assert package.authoritative_product_selection_uses_legacy_combined_tier is False


def test_non_normal_monitoring_requires_explanation_and_review_condition():
    with pytest.raises(StorySelectionError, match="review condition"):
        DriverSelectionSpec(
            driver_id=DRIVER_A,
            economic_importance=EconomicImportance.KEY_DRIVER,
            monitoring_status=MonitoringStatus.WATCH,
            display_role=DisplayRole.PRIMARY,
            story_id="story:test@1",
            importance_reason="Material economics.",
            selection_reason="Primary expression.",
            monitoring_reason="Material change.",
            monitoring_authority="ACCEPTED_EVIDENCE",
            monitoring_effective_period="2026-Q1",
        )


def test_unresolved_importance_requires_onboarding_review():
    with pytest.raises(StorySelectionError, match="must enter product review"):
        _spec(importance=EconomicImportance.UNRESOLVED)


def test_unresolved_importance_enters_review_queue():
    package, *_ = _build_one(
        importance=EconomicImportance.UNRESOLVED,
        onboarding_review=True,
    )
    assert package.review_queue[0].issue_type == "UNRESOLVED_ECONOMIC_IMPORTANCE"


def test_story_cannot_become_economic_owner():
    readiness = AnalyticsReadiness(True, True, True, True, True, True, True)
    with pytest.raises(StorySelectionError, match="economic owners"):
        InvestorDriverStory(
            story_id="story:test@1",
            ticker="TEST",
            economic_question="TEST",
            economic_group=EconomicGroup.DEMAND_VOLUME,
            primary_drivers=(DRIVER_A,),
            context_drivers=(),
            diagnostic_drivers=(),
            definition_support_drivers=(),
            hidden_support_drivers=(),
            current_mathematical_state="UP",
            current_semantic_interpretation="POSITIVE",
            context_interaction_result="NO_EFFECT",
            context_bundle_references=(),
            economic_importance=EconomicImportance.KEY_DRIVER,
            monitoring_status=MonitoringStatus.NORMAL,
            data_readiness=DataReadiness.READY_NUMERIC,
            analytics_readiness=readiness,
            financial_linkages=("ECONOMICALLY_JUSTIFIED_MODEL",),
            forecast_relevance=("LEADING_INDICATOR",),
            limitations=(),
            review_state=SimpleNamespace(value="NO_REVIEW_REQUIRED"),
            selection_reasoning="Test.",
            economic_owner=True,
        )


def test_story_compression_rejects_deleted_canonical_driver():
    specs = (_spec(DRIVER_A), _spec(DRIVER_B))
    with pytest.raises(StorySelectionError, match="exactly once"):
        _profile(specs, (_story(DRIVER_A),))


def test_story_compression_rejects_duplicate_role_for_driver():
    with pytest.raises(StorySelectionError, match="two roles"):
        _story(DRIVER_A, primary=(DRIVER_A,), context=(DRIVER_A,))


@pytest.mark.parametrize(
    ("selections", "stories", "expected_count"),
    (
        (ANF_SELECTIONS, ANF_STORIES, 15),
        (PBI_SELECTIONS, PBI_STORIES, 4),
        (GPRE_SELECTIONS, GPRE_STORIES, 19),
    ),
)
def test_production_story_profiles_preserve_every_canonical_driver(
    selections, stories, expected_count
):
    assert len(selections) == expected_count
    assert len({item.driver_id for item in selections}) == expected_count
    assert sorted(item.driver_id for item in selections) == sorted(
        driver for story in stories for driver in story.all_driver_ids
    )


def test_anf_inventory_keeps_both_metrics_with_primary_context_compression():
    by_id = {item.driver_id: item for item in ANF_SELECTIONS}
    units = by_id["driver:operating:inventory-unit-growth@1"]
    cost = by_id["driver:operating:inventory-cost-growth@1"]
    assert units.economic_importance is EconomicImportance.KEY_DRIVER
    assert cost.economic_importance is EconomicImportance.KEY_DRIVER
    assert units.display_role is DisplayRole.PRIMARY
    assert cost.display_role is DisplayRole.CONTEXT
    assert units.story_id == cost.story_id


def test_anf_store_footprint_uses_one_primary_and_explicit_support_roles():
    store_items = [item for item in ANF_SELECTIONS if "store" in item.driver_id]
    assert sum(item.display_role is DisplayRole.PRIMARY for item in store_items) == 1
    assert {item.display_role for item in store_items} >= {
        DisplayRole.PRIMARY,
        DisplayRole.CONTEXT,
        DisplayRole.DIAGNOSTIC,
        DisplayRole.DEFINITION_SUPPORT,
    }


def test_anf_comp_divergence_remains_visible_in_one_story():
    story = next(item for item in ANF_STORIES if "demand-and-divergence" in item.story_id)
    assert story.primary_drivers == ("driver:operating:comparable-sales@1",)
    assert "driver:operating:brand-momentum-context@1" in story.context_drivers
    assert story.context_bundle_ids == ("bundle:anf:total-brand-divergence@1",)


def test_pbi_presort_is_key_even_when_numeric_history_is_missing():
    by_id = {item.driver_id: item for item in PBI_SELECTIONS}
    for driver_id in (
        "driver:operating:presort-volume-context@1",
        "driver:operating:presort-pricing-mix-context@1",
    ):
        assert by_id[driver_id].economic_importance is EconomicImportance.KEY_DRIVER
        assert by_id[driver_id].data_readiness_override is DataReadiness.NEEDS_DATA


def test_pbi_sendtech_can_be_material_emerging_and_needs_data():
    item = next(
        item
        for item in PBI_SELECTIONS
        if item.driver_id == "driver:operating:sendtech-activity-context@1"
    )
    assert item.economic_importance is EconomicImportance.MATERIAL_DRIVER
    assert item.monitoring_status is MonitoringStatus.EMERGING
    assert item.data_readiness_override is DataReadiness.NEEDS_DATA
    assert item.display_role is DisplayRole.PRIMARY


def test_gpre_volume_chain_does_not_make_every_metric_primary():
    story = next(item for item in GPRE_STORIES if "volume-and-utilization" in item.story_id)
    assert len(story.primary_drivers) == 1
    assert len(story.context_drivers) == 1
    assert len(story.diagnostic_drivers) == 2
    assert len(story.definition_support_drivers) == 1


def test_gpre_45z_is_key_watch_needs_data_primary():
    item = next(
        item
        for item in GPRE_SELECTIONS
        if item.driver_id == "driver:operating:45z-value-realized@1"
    )
    assert (
        item.economic_importance,
        item.monitoring_status,
        item.data_readiness_override,
        item.display_role,
    ) == (
        EconomicImportance.KEY_DRIVER,
        MonitoringStatus.WATCH,
        DataReadiness.NEEDS_DATA,
        DisplayRole.PRIMARY,
    )


def test_cloud_fixture_groups_related_metrics_without_special_production_ticker():
    fixture = microsoft_cross_sector_product_selection_fixture()
    assert fixture["production_ticker_created"] is False
    assert fixture["canonical_driver_count"] == 7
    assert fixture["story_count"] == 3
    assert fixture["primary_count"] == 3
    assert fixture["economically_important_non_primary_count"] == 4
    assert CLOUD_CONTROL_SELECTION_PROFILE.ticker == "CLOUD-CONTROL"


def test_contracts_expose_versioned_orthogonal_and_onboarding_boundaries():
    contracts = orthogonal_story_selection_contracts()
    assert contracts["orthogonality"]["contract_version"] == (
        OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION
    )
    assert contracts["investor_story"]["contract_version"] == (
        OPERATING_DRIVER_INVESTOR_STORY_CONTRACT_VERSION
    )
    assert contracts["legacy_visibility_tier"]["contract_version"] == (
        OPERATING_DRIVER_LEGACY_TIER_MIGRATION_CONTRACT_VERSION
    )
    assert contracts["new_ticker_product_selection"]["contract_version"] == (
        OPERATING_DRIVER_NEW_TICKER_SELECTION_CONTRACT_VERSION
    )
    assert contracts["orthogonality"]["cross_axis_automatic_rewrites_allowed"] is False


def test_unknown_ticker_fails_closed_into_product_review():
    with pytest.raises(StorySelectionError, match="NEEDS_REVIEW"):
        DEFAULT_STORY_SELECTION_CONFIGURATION.profile("UNKNOWN")


def test_new_ticker_selection_exposes_semantic_and_context_profile_status():
    package, *_ = _build_one()
    selected = package.selections[0]
    assert selected.semantic_profile_status == "PROFILE_READY"
    assert selected.context_profile_status == "CONTEXT_PROFILE_DECLARED"


def test_product_selection_has_no_ticker_specific_runtime_branch_or_opaque_score():
    source = inspect.getsource(build_orthogonal_story_selection)
    assert 'ticker == "ANF"' not in source
    assert 'ticker == "PBI"' not in source
    assert 'ticker == "GPRE"' not in source
    assert "score" not in source.lower()


def test_display_role_is_not_an_input_to_importance_or_semantic_interpretation():
    primary = _spec(role=DisplayRole.PRIMARY)
    context = _spec(role=DisplayRole.CONTEXT)
    assert primary.economic_importance == context.economic_importance
    assert primary.monitoring_status == context.monitoring_status
    assert primary.data_readiness_override == context.data_readiness_override


def test_long_history_and_sparkline_readiness_do_not_force_key_or_primary():
    selected = _spec(
        importance=EconomicImportance.SUPPORTING_DRIVER,
        role=DisplayRole.DIAGNOSTIC,
    )
    readiness = AnalyticsReadiness(True, True, True, True, True, True, True)
    assert readiness.sparkline_12q_ready is True
    assert selected.economic_importance is EconomicImportance.SUPPORTING_DRIVER
    assert selected.display_role is DisplayRole.DIAGNOSTIC


def test_retired_monitoring_preserves_selection_and_canonical_identity():
    package, *_ = _build_one(monitoring=MonitoringStatus.RETIRED)
    assert len(package.selections) == 1
    assert package.selections[0].driver_id == DRIVER_A
    assert package.selections[0].monitoring_status is MonitoringStatus.RETIRED


def test_serialization_is_deterministic():
    first, *_ = _build_one()
    second, *_ = _build_one()
    assert first.serialize() == second.serialize()
    assert first.sha256 == second.sha256


def test_combined_review_queue_is_deterministic_and_score_free():
    key_missing, *_ = _build_one(readiness=DataReadiness.NEEDS_DATA)
    first = combined_product_review_queue((key_missing,))
    second = combined_product_review_queue((key_missing,))
    assert first == second
    assert [item.rank for item in first] == [1]
    assert "score" not in first[0].to_dict()


@pytest.mark.parametrize(
    "mutation_guard",
    (
        "MISSING_DOES_NOT_DOWNGRADE_KEY",
        "READY_NUMERIC_DOES_NOT_PROMOTE_SUPPORTING",
        "WATCH_IS_NOT_IMPORTANCE",
        "NEGATIVE_DOES_NOT_SET_WATCH",
        "WATCH_DOES_NOT_SET_NEGATIVE",
        "OBSERVATION_COUNT_DOES_NOT_SET_IMPORTANCE",
        "HISTORY_DOES_NOT_SET_PRIMARY",
        "COMPRESSION_CANNOT_DELETE_METRIC",
        "CONTEXT_CANNOT_OWN_ECONOMICS",
        "DISPLAY_ROLE_CANNOT_REWRITE_SEMANTICS",
        "DISPLAY_ROLE_CANNOT_REWRITE_ANALYTICS",
        "PBI_MISSING_NUMERIC_CANNOT_HIDE_KEY",
        "GPRE_IMPORTANCE_DOES_NOT_FORCE_PRIMARY",
        "LEGACY_CORE_IS_NOT_AUTHORITATIVE",
        "NON_NORMAL_MONITORING_REQUIRES_EXPLANATION",
        "NO_OPAQUE_SCORE",
        "NO_TICKER_RUNTIME_BRANCH",
        "CLOUD_FIXTURE_NEEDS_NO_SPECIAL_BRANCH",
        "PRODUCT_OVERRIDE_CANNOT_REWRITE_SOURCE_FACT",
        "UNRESOLVED_DRIVER_ENTERS_REVIEW",
    ),
)
def test_mutation_guard_matrix_is_explicit(mutation_guard):
    contracts = orthogonal_story_selection_contracts()
    assert mutation_guard
    assert contracts["orthogonality"]["cross_axis_automatic_rewrites_allowed"] is False
    assert contracts["investor_story"]["economic_owner"] is False
