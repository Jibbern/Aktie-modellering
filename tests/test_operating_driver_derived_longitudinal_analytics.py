from __future__ import annotations

import dataclasses
import inspect
from datetime import date

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_derived_analytics import (
    OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION,
    AccelerationState,
    AnalysisType,
    AnalyticsAvailability,
    ConsistencyState,
    ForecastEvidenceReadiness,
    InflectionState,
    SignTransition,
    TrendState,
    build_derived_analytics,
    combined_analytics_digest,
    derive_comparison,
)
from pbi_xbrl.longitudinal_memory.operating_driver_foundation import (
    AggregationSemantics,
    DefinitionContinuity,
    DefinitionContinuityState,
    DriverDimension,
    UnitConversionReceipt,
)
from pbi_xbrl.longitudinal_memory.operating_driver_shadow_profiles import (
    ANF_PROFILE,
    GPRE_PROFILE,
    PBI_PROFILE,
)
from pbi_xbrl.longitudinal_memory.operating_driver_shadow_registry import (
    CalendarMode,
    CanonicalDriverDefinition,
    DriverMappingRule,
    DriverScope,
    FinancialLinkageKind,
    ForecastEvidenceCapability,
    MappingAction,
    TickerShadowProfile,
    VisibilityTier,
    build_shadow_registry,
)


TOTAL = (
    DriverDimension(
        dimension_id="dimension:operating-driver:scope@1",
        member_id="member:operating-driver:total-company@1",
        label="Total company",
    ),
)
SEGMENT = (
    DriverDimension(
        dimension_id="dimension:operating-driver:segment@1",
        member_id="member:operating-driver:segment-a@1",
        label="Segment A",
    ),
)


def _serial(year: int, month: int, day: int) -> int:
    return (date(year, month, day) - date(1899, 12, 30)).days


QUARTERS = (
    _serial(2024, 3, 31),
    _serial(2024, 6, 30),
    _serial(2024, 9, 30),
    _serial(2024, 12, 31),
    _serial(2025, 3, 31),
    _serial(2025, 6, 30),
    _serial(2025, 9, 30),
    _serial(2025, 12, 31),
    _serial(2026, 3, 31),
)


def _definition(
    *,
    version: int = 1,
    unit: str = "unit:core:usd-million@1",
    aggregation: AggregationSemantics = AggregationSemantics.SUMMABLE,
    linkage: FinancialLinkageKind = FinancialLinkageKind.ECONOMICALLY_JUSTIFIED_MODEL,
    forecast: ForecastEvidenceCapability = ForecastEvidenceCapability.LEADING_INDICATOR,
) -> CanonicalDriverDefinition:
    return CanonicalDriverDefinition(
        driver_id="driver:operating:test-throughput@1",
        driver_family="volume",
        canonical_label="Test throughput",
        display_label="Test throughput",
        definition_id="definition:operating-driver:test-throughput@1",
        definition_version=version,
        definition_text=f"Source-backed test throughput version {version}.",
        unit_id=unit,
        scale="1",
        sign_convention="positive means a higher reported value",
        aggregation_semantics=aggregation,
        scope=DriverScope.GENERIC,
        visibility_tier=VisibilityTier.CORE_DRIVER,
        financial_linkage=linkage,
        forecast_capability=forecast,
    )


def _profile(
    *,
    ticker: str = "TST",
    definitions: tuple[CanonicalDriverDefinition, ...] | None = None,
    rules: tuple[DriverMappingRule, ...] | None = None,
) -> TickerShadowProfile:
    definitions = definitions or (_definition(),)
    rules = rules or (
        DriverMappingRule(
            rule_id="rule:operating-driver:test-throughput@1",
            raw_label="Test throughput",
            action=MappingAction.CANONICAL_DRIVER,
            canonical_driver_id="driver:operating:test-throughput@1",
            definition_version=1,
            dimensions=TOTAL,
        ),
    )
    return TickerShadowProfile(
        ticker=ticker,
        calendar_mode=CalendarMode.CALENDAR_QUARTER,
        calendar_id=f"calendar:{ticker.lower()}:calendar-year-fiscal@1",
        mapping_rules=rules,
        definitions=definitions,
        source_priority=("10-K", "10-Q", "earnings_release", "presentation"),
    )


def _row(
    period: int,
    value: object,
    *,
    label: str = "Test throughput",
    unit: str | None = "$m",
    commentary: str = "Source-backed test throughput.",
    source: str = "10-Q",
) -> dict[str, object]:
    return {
        "Quarter": period,
        "Driver group": "Test",
        "Driver": label,
        "Value": value,
        "Unit": unit,
        "QoQ change": None,
        "YoY change": None,
        "Source": source,
        "Commentary": commentary,
        "Quality": "exact",
    }


def _analytics(
    values: list[object],
    *,
    periods: tuple[int, ...] | None = None,
    definition: CanonicalDriverDefinition | None = None,
) -> object:
    periods = periods or QUARTERS[: len(values)]
    profile = _profile(definitions=(definition or _definition(),))
    package = build_shadow_registry(
        [_row(period, value, unit=("%" if (definition and definition.unit_id == "unit:core:percent@1") else "$m")) for period, value in zip(periods, values)],
        profile,
    )
    return build_derived_analytics(package)


def _latest_comparison(package: object, name: str):
    records = getattr(package, name)
    return max(records, key=lambda item: item.as_of_period_id)


def test_contract_and_availability_states_are_closed() -> None:
    assert OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION == (
        "operating-drivers-derived-longitudinal-analytics@1"
    )
    assert {item.value for item in AnalyticsAvailability} == {
        "AVAILABLE",
        "INSUFFICIENT_HISTORY",
        "PRIOR_PERIOD_MISSING",
        "PERIOD_INCOMPATIBLE",
        "DEFINITION_BREAK",
        "DIMENSION_MISMATCH",
        "UNIT_INCOMPATIBLE",
        "AGGREGATION_NOT_ALLOWED",
        "INCOMPLETE_PERIOD_SET",
        "RELATIVE_CHANGE_UNDEFINED",
        "NOT_APPLICABLE",
        "NEEDS_REVIEW",
    }


def test_latest_state_and_latest_comparable_state_are_distinct() -> None:
    package = _analytics(
        [1, 2, 9],
        periods=(QUARTERS[0], QUARTERS[1], QUARTERS[3]),
    )
    latest = package.latest_states[0]
    assert latest.latest_value == "9"
    assert latest.latest_is_comparable_to_predecessor is False
    assert latest.latest_comparable_observation_id is not None
    assert latest.comparable_history_depth == 2


def test_exact_qoq_and_yoy() -> None:
    package = _analytics([100, 110, 120, 130, 150])
    qoq = _latest_comparison(package, "qoq_analytics")
    yoy = _latest_comparison(package, "yoy_analytics")
    assert qoq.availability is AnalyticsAvailability.AVAILABLE
    assert qoq.native_unit_change == "20"
    assert qoq.relative_change == "0.1538461538461538461538461538"
    assert yoy.availability is AnalyticsAvailability.AVAILABLE
    assert yoy.native_unit_change == "50"
    assert yoy.relative_change == "0.5"


def test_qoq_never_uses_nearest_quarter() -> None:
    package = _analytics([10, 30], periods=(QUARTERS[0], QUARTERS[2]))
    latest = _latest_comparison(package, "qoq_analytics")
    assert latest.availability is AnalyticsAvailability.PRIOR_PERIOD_MISSING
    assert latest.prior_period_id is None


def test_yoy_never_uses_nearest_year_quarter() -> None:
    package = _analytics([10, 20], periods=(QUARTERS[0], QUARTERS[5]))
    latest = _latest_comparison(package, "yoy_analytics")
    assert latest.availability is AnalyticsAvailability.PRIOR_PERIOD_MISSING


def test_zero_denominator_keeps_native_delta_and_blocks_relative_change() -> None:
    package = _analytics([0, 10])
    latest = _latest_comparison(package, "qoq_analytics")
    assert latest.native_unit_change == "10"
    assert latest.relative_change is None
    assert latest.relative_change_availability is (
        AnalyticsAvailability.RELATIVE_CHANGE_UNDEFINED
    )
    assert latest.sign_transition is SignTransition.ZERO_TO_POSITIVE


@pytest.mark.parametrize(
    ("values", "transition"),
    [
        ([-2, 3], SignTransition.NEGATIVE_TO_POSITIVE),
        ([2, -3], SignTransition.POSITIVE_TO_NEGATIVE),
        ([-2, -3], SignTransition.NONE),
        ([0, 0], SignTransition.ZERO_TO_ZERO),
    ],
)
def test_sign_crossing_and_negative_bases_fail_relative_closed(
    values: list[int], transition: SignTransition
) -> None:
    package = _analytics(values)
    latest = _latest_comparison(package, "qoq_analytics")
    assert latest.native_unit_change is not None
    assert latest.relative_change is None
    assert latest.relative_change_availability is (
        AnalyticsAvailability.RELATIVE_CHANGE_UNDEFINED
    )
    assert latest.sign_transition is transition


def test_percentage_metric_uses_percentage_point_delta_not_relative_change() -> None:
    definition = _definition(
        unit="unit:core:percent@1",
        aggregation=AggregationSemantics.NON_AGGREGATABLE,
    )
    package = _analytics([85, 90], definition=definition)
    latest = _latest_comparison(package, "qoq_analytics")
    assert latest.native_unit_change == "5"
    assert latest.percentage_point_change == "5"
    assert latest.relative_change is None
    assert latest.relative_change_availability is AnalyticsAvailability.NOT_APPLICABLE


def test_safe_unit_conversion_receipt_preserves_comparable_native_unit_math() -> None:
    registry = build_shadow_registry(
        [_row(QUARTERS[0], 1), _row(QUARTERS[1], 1.25)], _profile()
    )
    prior, current = sorted(
        registry.observations,
        key=lambda item: item.evidence.period.fiscal_ordinal,
    )
    receipt = UnitConversionReceipt(
        rule_id="rule:operating-drivers:usd-thousand-to-million@1",
        from_unit_id="unit:core:usd-thousand@1",
        to_unit_id=current.evidence.driver.unit_id,
        multiplier="0.001",
        from_scale="1000",
        to_scale="1000000",
    )
    continuity = DefinitionContinuity(
        state=DefinitionContinuityState.UNIT_CONVERSION_SAFE,
        from_definition_id=current.evidence.driver.definition_id,
        from_definition_version=current.evidence.driver.definition_version,
        to_definition_id=current.evidence.driver.definition_id,
        to_definition_version=current.evidence.driver.definition_version,
        reason="Only the disclosed source scale changed.",
        unit_conversion=receipt,
    )
    converted = dataclasses.replace(
        current,
        evidence=dataclasses.replace(
            current.evidence,
            continuity=continuity,
            raw_value="1250",
            source_unit_id=receipt.from_unit_id,
        ),
    )
    result = derive_comparison(
        converted, (prior, converted), analysis_type=AnalysisType.QOQ
    )
    assert receipt.convert("1250") == converted.evidence.normalized_value == "1.25"
    assert result.availability is AnalyticsAvailability.AVAILABLE
    assert result.native_unit_change == "0.25"
    assert result.continuity_result == DefinitionContinuityState.UNIT_CONVERSION_SAFE.value


def test_complete_summable_ttm_and_fy() -> None:
    package = _analytics([1, 2, 3, 4])
    ttm = _latest_comparison(package, "ttm_analytics")
    fy = package.fiscal_year_analytics[0]
    assert ttm.availability is AnalyticsAvailability.AVAILABLE
    assert ttm.value == "10"
    assert fy.availability is AnalyticsAvailability.AVAILABLE
    assert fy.value == "10"


def test_incomplete_ttm_and_fy_fail_closed() -> None:
    package = _analytics(
        [1, 3, 4], periods=(QUARTERS[0], QUARTERS[2], QUARTERS[3])
    )
    assert _latest_comparison(package, "ttm_analytics").availability is (
        AnalyticsAvailability.INCOMPLETE_PERIOD_SET
    )
    assert package.fiscal_year_analytics[0].availability is (
        AnalyticsAvailability.INCOMPLETE_PERIOD_SET
    )


@pytest.mark.parametrize(
    "aggregation",
    [
        AggregationSemantics.PERIOD_END,
        AggregationSemantics.NON_AGGREGATABLE,
        AggregationSemantics.UNKNOWN,
        AggregationSemantics.AVERAGE_REQUIRES_CONTRACT,
    ],
)
def test_uncontracted_aggregation_semantics_are_rejected(
    aggregation: AggregationSemantics,
) -> None:
    package = _analytics([1, 2, 3, 4], definition=_definition(aggregation=aggregation))
    assert all(
        item.availability is AnalyticsAvailability.AGGREGATION_NOT_ALLOWED
        for item in package.ttm_analytics
    )
    assert all(
        item.availability is AnalyticsAvailability.AGGREGATION_NOT_ALLOWED
        for item in package.fiscal_year_analytics
    )


def test_ttm_change_requires_exact_prior_year_window() -> None:
    package = _analytics([1, 2, 3, 4, 2, 3, 4, 5])
    available = [
        item
        for item in package.ttm_change_analytics
        if item.availability is AnalyticsAvailability.AVAILABLE
    ]
    assert len(available) == 1
    assert available[0].native_unit_change == "4"


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1, 2, 3, 4], TrendState.UP),
        ([4, 3, 2, 1], TrendState.DOWN),
        ([2, 2, 2, 2], TrendState.UNCHANGED),
        ([1, 3, 2, 4], TrendState.MIXED),
    ],
)
def test_exact_four_quarter_trend_contract(
    values: list[int], expected: TrendState
) -> None:
    package = _analytics(values)
    trend = package.trend_analytics[0]
    assert trend.availability is AnalyticsAvailability.AVAILABLE
    assert trend.state is expected
    assert len(trend.input_observation_ids) == 4


def test_trend_does_not_bridge_missing_quarter() -> None:
    package = _analytics(
        [1, 2, 3, 4],
        periods=(QUARTERS[0], QUARTERS[1], QUARTERS[3], QUARTERS[4]),
    )
    assert package.trend_analytics[0].state is TrendState.INSUFFICIENT_DATA


@pytest.mark.parametrize(
    ("values", "state"),
    [
        ([1, 2, 4], AccelerationState.POSITIVE_ACCELERATION),
        ([1, 3, 4], AccelerationState.NEGATIVE_ACCELERATION),
        ([1, 2, 3], AccelerationState.UNCHANGED_RATE),
    ],
)
def test_acceleration_contract(values: list[int], state: AccelerationState) -> None:
    package = _analytics(values)
    acceleration = package.acceleration_analytics[0]
    assert acceleration.availability is AnalyticsAvailability.AVAILABLE
    assert acceleration.state is state


def test_percentage_point_acceleration() -> None:
    definition = _definition(
        unit="unit:core:percent@1",
        aggregation=AggregationSemantics.NON_AGGREGATABLE,
    )
    package = _analytics([80, 82, 86], definition=definition)
    result = package.acceleration_analytics[0]
    assert result.delta_semantics == "CONSECUTIVE_PERCENTAGE_POINT_DELTAS"
    assert result.second_difference == "2"


@pytest.mark.parametrize(
    ("values", "state"),
    [
        ([3, 2, 4], InflectionState.DOWN_TO_UP),
        ([1, 3, 2], InflectionState.UP_TO_DOWN),
        ([1, 2, 3], InflectionState.CONTINUED_UP),
        ([3, 2, 1], InflectionState.CONTINUED_DOWN),
        ([1, 1, 2], InflectionState.MIXED_OR_FLAT),
    ],
)
def test_inflection_contract(values: list[int], state: InflectionState) -> None:
    package = _analytics(values)
    assert package.acceleration_analytics[0].inflection_state is state


@pytest.mark.parametrize(
    ("values", "state"),
    [
        ([1, 2, 3, 4], ConsistencyState.CONSISTENT_UP),
        ([4, 3, 2, 1], ConsistencyState.CONSISTENT_DOWN),
        ([1, 2, 1, 2], ConsistencyState.MIXED),
        ([1, 1, 1, 1], ConsistencyState.UNCHANGED),
    ],
)
def test_consistency_is_transparent_move_count(
    values: list[int], state: ConsistencyState
) -> None:
    package = _analytics(values)
    result = package.consistency_analytics[0]
    assert result.state is state
    assert result.upward_move_count + result.downward_move_count + result.unchanged_move_count == 3


def test_variability_reports_range_and_population_standard_deviation() -> None:
    package = _analytics([1, 2, 3, 4])
    result = package.variability_analytics[0]
    assert result.minimum == "1"
    assert result.maximum == "4"
    assert result.value_range == "3"
    assert result.population_standard_deviation is not None
    assert result.coefficient_of_variation_availability is AnalyticsAvailability.AVAILABLE


def test_cv_rejected_for_zero_or_negative_history() -> None:
    package = _analytics([-1, 0, 1, 2])
    result = package.variability_analytics[0]
    assert result.availability is AnalyticsAvailability.AVAILABLE
    assert result.coefficient_of_variation is None
    assert result.coefficient_of_variation_availability is AnalyticsAvailability.NOT_APPLICABLE


def test_dimension_mismatch_comparison_fails_closed() -> None:
    rules = (
        DriverMappingRule(
            rule_id="rule:operating-driver:test-total@1",
            raw_label="Test throughput",
            action=MappingAction.CANONICAL_DRIVER,
            canonical_driver_id="driver:operating:test-throughput@1",
            definition_version=1,
            dimensions=TOTAL,
            required_commentary_tokens=("total",),
            priority=20,
        ),
        DriverMappingRule(
            rule_id="rule:operating-driver:test-segment@1",
            raw_label="Test throughput",
            action=MappingAction.CANONICAL_DRIVER,
            canonical_driver_id="driver:operating:test-throughput@1",
            definition_version=1,
            dimensions=SEGMENT,
            required_commentary_tokens=("segment",),
            priority=20,
        ),
    )
    registry = build_shadow_registry(
        [
            _row(QUARTERS[0], 1, commentary="Total source-backed value."),
            _row(QUARTERS[1], 2, commentary="Segment source-backed value."),
        ],
        _profile(rules=rules),
    )
    prior, current = sorted(
        registry.observations, key=lambda item: item.evidence.period.fiscal_ordinal
    )
    result = derive_comparison(
        current, (prior, current), analysis_type=AnalysisType.QOQ
    )
    assert result.availability is AnalyticsAvailability.DIMENSION_MISMATCH


def test_unit_mismatch_comparison_fails_closed() -> None:
    prior_registry = build_shadow_registry(
        [_row(QUARTERS[0], 1, unit="$m")], _profile()
    )
    percent_definition = _definition(unit="unit:core:percent@1")
    current_registry = build_shadow_registry(
        [_row(QUARTERS[1], 2, unit="%")],
        _profile(definitions=(percent_definition,)),
    )
    prior = prior_registry.observations[0]
    current = current_registry.observations[0]
    result = derive_comparison(
        current, (prior, current), analysis_type=AnalysisType.QOQ
    )
    assert result.availability is AnalyticsAvailability.UNIT_INCOMPATIBLE


def test_definition_break_comparison_is_explicit() -> None:
    rules = (
        DriverMappingRule(
            rule_id="rule:operating-driver:test-v1@1",
            raw_label="Test throughput",
            action=MappingAction.CANONICAL_DRIVER,
            canonical_driver_id="driver:operating:test-throughput@1",
            definition_version=1,
            dimensions=TOTAL,
            effective_through_serial=QUARTERS[0],
        ),
        DriverMappingRule(
            rule_id="rule:operating-driver:test-v2@1",
            raw_label="Test throughput",
            action=MappingAction.CANONICAL_DRIVER,
            canonical_driver_id="driver:operating:test-throughput@1",
            definition_version=2,
            dimensions=TOTAL,
            effective_from_serial=QUARTERS[1],
            transition_state=DefinitionContinuityState.DEFINITION_CHANGED_BREAK_SERIES,
            transition_from_definition_version=1,
        ),
    )
    registry = build_shadow_registry(
        [_row(QUARTERS[0], 1), _row(QUARTERS[1], 2)],
        _profile(definitions=(_definition(version=1), _definition(version=2)), rules=rules),
    )
    analytics = build_derived_analytics(registry)
    current = max(analytics.qoq_analytics, key=lambda item: item.as_of_period_id)
    assert current.availability is AnalyticsAvailability.DEFINITION_BREAK
    assert all(item.state is TrendState.INSUFFICIENT_DATA for item in analytics.trend_analytics)


def test_definition_break_inside_aggregate_fails_closed() -> None:
    rule = DriverMappingRule(
        rule_id="rule:operating-driver:test-v2@1",
        raw_label="Test throughput",
        action=MappingAction.CANONICAL_DRIVER,
        canonical_driver_id="driver:operating:test-throughput@1",
        definition_version=2,
        dimensions=TOTAL,
        effective_from_serial=QUARTERS[0],
        transition_state=DefinitionContinuityState.DEFINITION_CHANGED_BREAK_SERIES,
        transition_from_definition_version=1,
    )
    registry = build_shadow_registry(
        [_row(period, value) for period, value in zip(QUARTERS[:4], (1, 2, 3, 4))],
        _profile(definitions=(_definition(version=2),), rules=(rule,)),
    )
    analytics = build_derived_analytics(registry)
    assert _latest_comparison(analytics, "ttm_analytics").availability is (
        AnalyticsAvailability.DEFINITION_BREAK
    )


def test_pbi_sparse_qualitative_support_never_becomes_numeric_analytics() -> None:
    registry = build_shadow_registry(
        [
            _row(
                QUARTERS[-1],
                None,
                label="Volume / throughput",
                unit=None,
                commentary="Presort Services pipeline robust.",
            )
        ],
        PBI_PROFILE,
    )
    analytics = build_derived_analytics(registry)
    assert not analytics.latest_states
    assert not analytics.qoq_analytics
    assert not analytics.trend_analytics
    assert any(item.qualitative_attachment_ids for item in analytics.analytical_signals)
    assert all(
        item.availability is AnalyticsAvailability.INSUFFICIENT_HISTORY
        for item in analytics.analytical_signals
    )


def test_anf_period_conflict_does_not_create_analytics() -> None:
    rows = [
        _row(
            period,
            value,
            label="Total Company comparable sales",
            unit="%",
            source=source,
        )
        for period in (46053, 46144)
        for value, source in ((5, "10-Q"), (10, "10-K"))
    ]
    registry = build_shadow_registry(rows, ANF_PROFILE)
    assert not registry.observations
    analytics = build_derived_analytics(registry)
    assert not analytics.qoq_analytics
    assert analytics.forecast_number_emission_count == 0


def test_gpre_utilization_definition_break_never_compares_normally() -> None:
    registry = build_shadow_registry(
        [
            _row(
                _serial(2025, 9, 30), 101, label="Utilization", unit="%",
                commentary="Nine operating ethanol plants ran at 101%.",
            ),
            _row(
                _serial(2025, 12, 31), 97, label="Utilization", unit="%",
                commentary="Eight operating ethanol plants ran at 97%.",
            ),
        ],
        GPRE_PROFILE,
    )
    analytics = build_derived_analytics(registry)
    assert any(
        item.availability is AnalyticsAvailability.DEFINITION_BREAK
        for item in analytics.qoq_analytics
    )


def test_gpre_45z_incomplete_ttm_and_fy_remain_unavailable() -> None:
    registry = build_shadow_registry(
        [
            _row(_serial(2025, 9, 30), 26, label="45Z value realized"),
            _row(_serial(2025, 12, 31), 27, label="45Z value realized"),
            _row(_serial(2026, 3, 31), 55.2, label="45Z value realized"),
        ],
        GPRE_PROFILE,
    )
    analytics = build_derived_analytics(registry)
    assert _latest_comparison(analytics, "ttm_analytics").availability is (
        AnalyticsAvailability.INCOMPLETE_PERIOD_SET
    )
    assert analytics.fiscal_year_analytics[0].availability is (
        AnalyticsAvailability.INCOMPLETE_PERIOD_SET
    )


def test_financial_linkage_and_forecast_capability_only_propagate() -> None:
    definition = _definition(
        linkage=FinancialLinkageKind.SPECULATIVE_ASSOCIATION,
        forecast=ForecastEvidenceCapability.FORECAST_CONTEXT,
    )
    package = _analytics([1], definition=definition)
    signal = package.analytical_signals[0]
    assert signal.financial_linkage == "SPECULATIVE_ASSOCIATION"
    assert signal.forecast_capability == "FORECAST_CONTEXT"
    assert signal.forecast_evidence_readiness is (
        ForecastEvidenceReadiness.NEEDS_RELATIONSHIP_REVIEW
    )
    assert signal.forecast_number is None


def test_economically_justified_leading_indicator_is_forecast_evidence_ready() -> None:
    package = _analytics([1, 2, 3, 4])
    assert package.analytical_signals[0].forecast_evidence_readiness is (
        ForecastEvidenceReadiness.FORECAST_EVIDENCE_READY
    )


def test_guidance_references_remain_unmatched_without_new_owner() -> None:
    rule = DriverMappingRule(
        rule_id="rule:operating-driver:test-guidance@1",
        raw_label="Test throughput",
        action=MappingAction.GUIDANCE_REFERENCE,
        owner_id="owner:guidance:source-native@1",
        reason="Guidance remains owner.",
    )
    registry = build_shadow_registry([_row(QUARTERS[0], 10)], _profile(rules=(rule,)))
    analytics = build_derived_analytics(registry)
    assert analytics.guidance_readiness["guidance_comparison_ready_count"] == 0
    assert analytics.guidance_readiness["unmatched_reference_records"][0]["readiness"] == (
        "GUIDANCE_REFERENCE_EXISTS_NOT_NORMALIZED"
    )


def test_serialization_has_no_good_bad_semantics_or_forecast_numbers() -> None:
    package = _analytics([1, 2, 3, 4])
    payload = package.serialize().decode("utf-8").casefold()
    for forbidden in (
        "bullish",
        "bearish",
        "higher_better",
        "lower_better",
        "target_range",
        "positive catalyst",
        "negative catalyst",
    ):
        assert forbidden not in payload
    assert '"forecast_number": null' in payload
    assert package.forecast_number_emission_count == 0


def test_serialization_has_no_workbook_coordinate_identity() -> None:
    package = _analytics([1, 2, 3, 4])
    payload = package.serialize().decode("utf-8").casefold()
    assert "workbook" not in payload
    assert "worksheet" not in payload
    assert "target_cell" not in payload
    assert "row_index" not in payload


def test_package_build_is_deterministic_and_source_order_independent() -> None:
    rows = [_row(period, value) for period, value in zip(QUARTERS[:4], (1, 2, 3, 4))]
    first_registry = build_shadow_registry(rows, _profile())
    second_registry = build_shadow_registry(list(reversed(rows)), _profile())
    first = build_derived_analytics(first_registry)
    second = build_derived_analytics(second_registry)
    assert first.serialize() == second.serialize()
    assert first.sha256 == second.sha256


def test_combined_digest_is_ticker_order_independent() -> None:
    first = _analytics([1, 2, 3, 4])
    other_registry = build_shadow_registry(
        [_row(period, value) for period, value in zip(QUARTERS[:4], (2, 3, 4, 5))],
        _profile(ticker="TSU"),
    )
    other = build_derived_analytics(other_registry)
    assert combined_analytics_digest((first, other)) == combined_analytics_digest((other, first))


def test_core_analytics_has_no_ticker_specific_python_branch() -> None:
    source = inspect.getsource(build_derived_analytics).casefold()
    full_source = inspect.getsource(inspect.getmodule(build_derived_analytics)).casefold()
    assert "if ticker" not in source
    assert '== "anf"' not in full_source
    assert '== "pbi"' not in full_source
    assert '== "gpre"' not in full_source


def test_cross_ticker_contract_handles_anf_pbi_and_gpre() -> None:
    anf_registry = build_shadow_registry(
        [
            _row(46053, 1, label="Total Company comparable sales", unit="%"),
            _row(46144, 2, label="Total Company comparable sales", unit="%"),
        ],
        ANF_PROFILE,
    )
    pbi_registry = build_shadow_registry(
        [
            _row(
                QUARTERS[-1], None, label="Volume / throughput", unit=None,
                commentary="Presort Services volume context.",
            )
        ],
        PBI_PROFILE,
    )
    gpre_registry = build_shadow_registry(
        [
            _row(
                QUARTERS[-1], 174.196, label="Ethanol gallons produced", unit="m gallons"
            )
        ],
        GPRE_PROFILE,
    )
    packages = tuple(
        build_derived_analytics(item)
        for item in (anf_registry, pbi_registry, gpre_registry)
    )
    assert {item.ticker for item in packages} == {"ANF", "PBI", "GPRE"}
    assert all(
        item.to_dict()["contract_version"]
        == "operating-drivers-derived-longitudinal-analytics@1"
        for item in packages
    )
    assert packages[1].latest_states == ()


def test_analytics_never_mutates_registry_package() -> None:
    registry = build_shadow_registry(
        [_row(period, value) for period, value in zip(QUARTERS[:4], (1, 2, 3, 4))],
        _profile(),
    )
    before = registry.serialize()
    build_derived_analytics(registry)
    assert registry.serialize() == before


def test_available_analytics_retain_typed_observation_and_source_lineage() -> None:
    package = _analytics([1, 2, 3, 4, 5, 6, 7, 8])
    assert package.latest_states[0].input_observation_ids
    assert package.latest_states[0].source_evidence_ids
    collections = (
        package.qoq_analytics,
        package.yoy_analytics,
        package.ttm_analytics,
        package.fiscal_year_analytics,
        package.ttm_change_analytics,
        package.trend_analytics,
        package.acceleration_analytics,
        package.consistency_analytics,
        package.variability_analytics,
    )
    for records in collections:
        for record in records:
            if record.availability is not AnalyticsAvailability.AVAILABLE:
                continue
            assert record.dimensions
            assert record.input_observation_ids
            assert record.source_evidence_ids
            assert record.knowledge_date_boundary is None or isinstance(
                record.knowledge_date_boundary, str
            )


def test_blocked_inventory_reconciles_fail_closed_components() -> None:
    package = _analytics([0, 2])
    reasons = {item.reason for item in package.blocked_analytics}
    assert AnalyticsAvailability.RELATIVE_CHANGE_UNDEFINED in reasons
    assert AnalyticsAvailability.INSUFFICIENT_HISTORY in reasons
    assert AnalyticsAvailability.INCOMPLETE_PERIOD_SET in reasons


def test_blocked_inventory_includes_unavailable_ttm_change() -> None:
    package = _analytics([1, 2, 3, 4])
    blocked = [
        item
        for item in package.blocked_analytics
        if item.analysis_type == AnalysisType.TTM_YOY.value
    ]
    assert len(blocked) == 1
    assert blocked[0].component == "AGGREGATE_COMPARISON"
    assert blocked[0].reason is AnalyticsAvailability.PRIOR_PERIOD_MISSING


def test_missing_never_becomes_zero_in_analytics() -> None:
    registry = build_shadow_registry(
        [_row(QUARTERS[0], None, unit=None)], _profile()
    )
    analytics = build_derived_analytics(registry)
    assert not analytics.latest_states
    assert all(item.latest_value is None for item in analytics.analytical_signals)
    assert all(item.forecast_number is None for item in analytics.analytical_signals)
