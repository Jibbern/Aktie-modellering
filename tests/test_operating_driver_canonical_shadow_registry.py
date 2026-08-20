from __future__ import annotations

import dataclasses
import json
from copy import deepcopy

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_foundation import (
    AggregationSemantics,
    DriverDimension,
)
from pbi_xbrl.longitudinal_memory.operating_driver_shadow_profiles import (
    ANF_PROFILE,
    GPRE_PROFILE,
    OPERATING_DRIVER_SHADOW_PROFILES,
    PBI_PROFILE,
    operating_driver_shadow_profile,
)
from pbi_xbrl.longitudinal_memory.operating_driver_shadow_registry import (
    OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION,
    CalendarMode,
    CanonicalDriverDefinition,
    DriverAvailabilityState,
    DriverMappingRule,
    DriverScope,
    EvidenceDisposition,
    FinancialLinkageKind,
    ForecastEvidenceCapability,
    MappingAction,
    OperatingDriverShadowRegistryError,
    TickerShadowProfile,
    VisibilityTier,
    build_shadow_registry,
    combined_registry_digest,
    excel_serial_date,
    normalize_raw_evidence,
)


TOTAL = (
    DriverDimension(
        dimension_id="dimension:operating-driver:scope@1",
        member_id="member:operating-driver:total-company@1",
        label="Total company",
    ),
)


def _definition(*, version: int = 1, unit: str = "unit:core:percent@1") -> CanonicalDriverDefinition:
    return CanonicalDriverDefinition(
        driver_id="driver:operating:test-volume@1",
        driver_family="volume",
        canonical_label="Test volume",
        display_label="Test volume",
        definition_id="definition:operating-driver:test-volume@1",
        definition_version=version,
        definition_text=f"Test volume definition v{version}.",
        unit_id=unit,
        scale="1",
        sign_convention="positive means a higher reported value",
        aggregation_semantics=AggregationSemantics.NON_AGGREGATABLE,
        scope=DriverScope.GENERIC,
        visibility_tier=VisibilityTier.PRIMARY,
        financial_linkage=FinancialLinkageKind.OPERATING_VOLUME,
        forecast_capability=ForecastEvidenceCapability.MAY_INFORM_FORECAST,
    )


def _canonical_rule(
    *,
    rule_id: str = "rule:operating-driver:test-map@1",
    unit_version: int = 1,
    priority: int = 10,
    required: tuple[str, ...] = (),
    dimensions: tuple[DriverDimension, ...] = TOTAL,
) -> DriverMappingRule:
    return DriverMappingRule(
        rule_id=rule_id,
        raw_label="Test metric",
        action=MappingAction.CANONICAL_DRIVER,
        canonical_driver_id="driver:operating:test-volume@1",
        definition_version=unit_version,
        dimensions=dimensions,
        required_commentary_tokens=required,
        priority=priority,
    )


def _profile(
    *,
    rules: tuple[DriverMappingRule, ...] | None = None,
    definitions: tuple[CanonicalDriverDefinition, ...] | None = None,
) -> TickerShadowProfile:
    return TickerShadowProfile(
        ticker="TST",
        calendar_mode=CalendarMode.CALENDAR_QUARTER,
        calendar_id="calendar:tst:calendar-year-fiscal@1",
        mapping_rules=rules or (_canonical_rule(),),
        definitions=definitions or (_definition(),),
        source_priority=("10-K", "10-Q", "earnings_release", "presentation"),
    )


def _row(
    *,
    period: int = 45747,  # 2025-03-31
    value: object = 10,
    unit: str | None = "%",
    source: str = "10-Q",
    commentary: str = "Source-backed test metric.",
    label: str = "Test metric",
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


def test_contract_and_closed_dispositions_are_explicit() -> None:
    assert OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION == (
        "operating-drivers-canonical-shadow-registry@1"
    )
    assert {item.value for item in EvidenceDisposition} == {
        "CANONICAL_OBSERVATION",
        "DUPLICATE_EVIDENCE",
        "QUALITATIVE_SUPPORT",
        "GUIDANCE_REFERENCE",
        "OWNER_ELSEWHERE",
        "DEFINITION_INCOMPATIBLE",
        "PERIOD_INCOMPATIBLE",
        "UNIT_UNRESOLVED",
        "DIMENSION_UNRESOLVED",
        "IDENTITY_UNRESOLVED",
        "LOW_VALUE_SUPPORT",
        "NEEDS_REVIEW",
    }
    assert {item.value for item in DriverAvailabilityState} == {
        "AVAILABLE_COMPARABLE",
        "AVAILABLE_DEFINITION_CHANGED",
        "AVAILABLE_NOT_COMPARABLE",
        "UNAVAILABLE",
        "NOT_RELEVANT",
        "NEEDS_REVIEW",
    }


def test_profile_registry_is_closed_and_declarative() -> None:
    assert set(OPERATING_DRIVER_SHADOW_PROFILES) == {"ANF", "PBI", "GPRE"}
    assert operating_driver_shadow_profile("anf") is ANF_PROFILE
    with pytest.raises(KeyError):
        operating_driver_shadow_profile("UNKNOWN")


def test_one_numeric_source_record_becomes_typed_observation() -> None:
    package = build_shadow_registry([_row()], _profile())
    assert package.reconciliation["reconciles"] is True
    assert len(package.observations) == 1
    observation = package.observations[0]
    assert observation.evidence.normalized_value == "10"
    assert observation.evidence.period.display_label == "2025-Q1"
    assert observation.evidence.driver.dimension_set_id
    assert package.disposition_counts["CANONICAL_OBSERVATION"] == 1


def test_source_backed_zero_remains_numeric_zero() -> None:
    package = build_shadow_registry([_row(value=0)], _profile())
    assert package.observations[0].evidence.normalized_value == "0"
    assert package.observations[0].evidence.source_backed_zero is True


def test_missing_value_never_becomes_zero() -> None:
    package = build_shadow_registry([_row(value=None, unit=None)], _profile())
    assert not package.observations
    assert package.disposition_counts["QUALITATIVE_SUPPORT"] == 1
    assert package.attachments[0].attachment_kind == "QUALITATIVE_SUPPORT"


def test_qualitative_support_never_has_numeric_normalization() -> None:
    package = build_shadow_registry([_row(value=None, unit=None)], _profile())
    payload = package.to_dict()
    assert payload["observations"] == []
    assert "Source-backed test metric" in payload["attachments"][0]["commentary"]


def test_conflicting_values_fail_closed_without_source_precedence_selection() -> None:
    package = build_shadow_registry(
        [_row(value=10, source="10-Q"), _row(value=11, source="10-K")],
        _profile(),
    )
    assert not package.observations
    assert package.disposition_counts["PERIOD_INCOMPATIBLE"] == 2


def test_identical_values_use_precedence_only_for_primary_and_retain_corroboration() -> None:
    package = build_shadow_registry(
        [_row(value=10, source="presentation"), _row(value=10, source="10-Q")],
        _profile(),
    )
    assert len(package.observations) == 1
    assert package.observations[0].evidence.source.source_type.value == "SEC_FILING"
    assert package.disposition_counts["DUPLICATE_EVIDENCE"] == 1
    assert len(package.observations[0].raw_record_ids) == 2


def test_source_order_cannot_change_registry_identity() -> None:
    rows = [_row(value=10, source="presentation"), _row(value=10, source="10-Q")]
    first = build_shadow_registry(rows, _profile())
    second = build_shadow_registry(list(reversed(rows)), _profile())
    assert first.serialize() == second.serialize()
    assert first.sha256 == second.sha256


def test_raw_occurrence_ids_are_order_independent() -> None:
    rows = [_row(value=1), _row(value=2, period=45838)]
    first = normalize_raw_evidence("TST", rows)
    second = normalize_raw_evidence("TST", list(reversed(rows)))
    assert [item.raw_record_id for item in first] == [item.raw_record_id for item in second]


def test_wrong_unit_fails_closed() -> None:
    package = build_shadow_registry([_row(unit="$m")], _profile())
    assert not package.observations
    assert package.disposition_counts["UNIT_UNRESOLVED"] == 1


def test_unmapped_label_is_exhaustively_classified_as_unsupported() -> None:
    package = build_shadow_registry([_row(label="Unknown metric")], _profile())
    assert package.reconciliation == {
        "classified_record_count": 1,
        "duplicate_raw_record_id_count": 0,
        "raw_record_count": 1,
        "reconciles": True,
        "unclassified_record_count": 0,
    }
    assert package.disposition_counts["NEEDS_REVIEW"] == 1


def test_owner_elsewhere_never_becomes_observation() -> None:
    rule = DriverMappingRule(
        rule_id="rule:operating-driver:test-owner@1",
        raw_label="Test metric",
        action=MappingAction.OWNER_ELSEWHERE,
        owner_id="owner:financial-products:source-native@1",
        reason="Financial owner remains authoritative.",
    )
    package = build_shadow_registry([_row()], _profile(rules=(rule,)))
    assert not package.observations
    assert package.disposition_counts["OWNER_ELSEWHERE"] == 1


def test_guidance_reference_never_becomes_actual_observation() -> None:
    rule = DriverMappingRule(
        rule_id="rule:operating-driver:test-guidance@1",
        raw_label="Test metric",
        action=MappingAction.GUIDANCE_REFERENCE,
        owner_id="owner:guidance:source-native@1",
        reason="Guidance is not actual evidence.",
    )
    package = build_shadow_registry([_row()], _profile(rules=(rule,)))
    assert package.disposition_counts["GUIDANCE_REFERENCE"] == 1
    assert not package.observations


def test_equal_priority_competing_rules_fail_closed() -> None:
    other_dimension = (
        DriverDimension(
            dimension_id="dimension:operating-driver:scope@1",
            member_id="member:operating-driver:segment-a@1",
            label="Segment A",
        ),
    )
    rules = (
        _canonical_rule(rule_id="rule:operating-driver:test-a@1"),
        _canonical_rule(rule_id="rule:operating-driver:test-b@1", dimensions=other_dimension),
    )
    package = build_shadow_registry([_row()], _profile(rules=rules))
    assert package.disposition_counts["IDENTITY_UNRESOLVED"] == 1
    assert not package.observations


def test_required_commentary_tokens_are_declarative() -> None:
    specific = _canonical_rule(
        rule_id="rule:operating-driver:test-specific@1",
        priority=20,
        required=("segment a",),
    )
    fallback = DriverMappingRule(
        rule_id="rule:operating-driver:test-fallback@1",
        raw_label="Test metric",
        action=MappingAction.LOW_VALUE_SUPPORT,
        reason="No segment identity.",
    )
    package = build_shadow_registry(
        [_row(value=None, unit=None, commentary="Segment A demand improved.")],
        _profile(rules=(specific, fallback)),
    )
    assert package.disposition_counts["QUALITATIVE_SUPPORT"] == 1


def test_calendar_quarter_rejects_non_quarter_end() -> None:
    with pytest.raises(OperatingDriverShadowRegistryError, match="quarter end"):
        build_shadow_registry([_row(period=45748)], _profile())


def test_source_labelled_profile_requires_accepted_anchor() -> None:
    mutated = dataclasses.replace(ANF_PROFILE, fiscal_anchor_serial=99999)
    with pytest.raises(OperatingDriverShadowRegistryError, match="anchor"):
        build_shadow_registry([_row(period=46144)], mutated)


def test_series_breaks_on_missing_period() -> None:
    package = build_shadow_registry(
        [_row(period=45747), _row(period=45930)],
        _profile(),
    )
    assert len(package.series) == 2
    assert {item.break_before_reason for item in package.series} == {
        None,
        "MISSING_OR_INCOMPATIBLE_PERIOD",
    }


def test_definition_versions_are_separate_series() -> None:
    rules = (
        dataclasses.replace(
            _canonical_rule(rule_id="rule:operating-driver:test-v1@1"),
            effective_through_serial=45747,
        ),
        dataclasses.replace(
            _canonical_rule(
                rule_id="rule:operating-driver:test-v2@1",
                unit_version=2,
            ),
            effective_from_serial=45838,
        ),
    )
    package = build_shadow_registry(
        [_row(period=45747), _row(period=45838)],
        _profile(rules=rules, definitions=(_definition(version=1), _definition(version=2))),
    )
    assert len(package.series) == 2
    assert {item.definition_version for item in package.series} == {1, 2}


def test_profile_definition_requires_unique_version() -> None:
    with pytest.raises(OperatingDriverShadowRegistryError, match="Duplicate"):
        _profile(definitions=(_definition(), _definition()))


def test_mapping_rule_requires_dimensions() -> None:
    with pytest.raises(OperatingDriverShadowRegistryError, match="dimensions"):
        DriverMappingRule(
            rule_id="rule:operating-driver:test-invalid@1",
            raw_label="Test metric",
            action=MappingAction.CANONICAL_DRIVER,
            canonical_driver_id="driver:operating:test-volume@1",
            definition_version=1,
        )


def test_float_inputs_serialize_as_canonical_decimal_strings() -> None:
    package = build_shadow_registry([_row(value=10.25)], _profile())
    assert package.observations[0].evidence.normalized_value == "10.25"
    assert b"10.25" in package.serialize()


def test_combined_digest_is_package_order_independent() -> None:
    first = build_shadow_registry([_row(value=1)], _profile())
    second_profile = dataclasses.replace(_profile(), ticker="TSU", calendar_id="calendar:tsu:calendar-year-fiscal@1")
    second = build_shadow_registry([_row(value=2)], second_profile)
    assert combined_registry_digest((first, second)) == combined_registry_digest((second, first))


def test_mutating_value_changes_registry_digest() -> None:
    first = build_shadow_registry([_row(value=1)], _profile())
    second = build_shadow_registry([_row(value=2)], _profile())
    assert first.sha256 != second.sha256


def test_every_profile_rule_points_to_an_existing_definition() -> None:
    for profile in OPERATING_DRIVER_SHADOW_PROFILES.values():
        for rule in profile.mapping_rules:
            if rule.action is MappingAction.CANONICAL_DRIVER:
                assert rule.canonical_driver_id is not None
                assert rule.definition_version is not None
                profile.definition(rule.canonical_driver_id, rule.definition_version)


def test_gpre_utilization_has_explicit_definition_break() -> None:
    utilization = [
        definition
        for definition in GPRE_PROFILE.definitions
        if definition.driver_id == "driver:operating:ethanol-plant-utilization@1"
    ]
    assert [item.definition_version for item in utilization] == [1, 2]
    rules = [rule for rule in GPRE_PROFILE.mapping_rules if rule.raw_label == "Utilization"]
    assert {(rule.effective_through_serial, rule.effective_from_serial) for rule in rules} == {
        (45930, None),
        (None, 46022),
    }


def test_gpre_45z_crush_and_rin_are_distinct_canonical_drivers() -> None:
    ids = {item.driver_id for item in GPRE_PROFILE.definitions}
    assert {
        "driver:operating:45z-value-realized@1",
        "driver:operating:consolidated-ethanol-crush-margin@1",
        "driver:operating:crush-margin-ex-45z@1",
        "driver:operating:crush-margin-ex-rin@1",
        "driver:operating:rin-impact@1",
    } <= ids


def test_pbi_profile_has_no_numeric_driver_definition() -> None:
    assert all(item.unit_id == "unit:core:qualitative@1" for item in PBI_PROFILE.definitions)


def test_profiles_do_not_use_workbook_coordinates_as_identity() -> None:
    payload = json.dumps(
        {ticker: profile.to_dict() for ticker, profile in OPERATING_DRIVER_SHADOW_PROFILES.items()},
        sort_keys=True,
    )
    assert "!" not in payload
    assert "target_cell" not in payload.casefold()
    assert "row_index" not in payload.casefold()
    assert "workbook" not in payload.casefold()


def test_raw_evidence_payload_is_immutable_to_caller_mutation() -> None:
    row = _row()
    package = build_shadow_registry([deepcopy(row)], _profile())
    row["Value"] = 999
    assert package.observations[0].evidence.normalized_value == "10"


def test_excel_serial_conversion_is_explicit_and_stable() -> None:
    assert excel_serial_date(45747).isoformat() == "2025-03-31"


def test_anf_profile_maps_non_anf_named_core_without_row_identity() -> None:
    rows = [
        _row(
            period=46053,
            value=1,
            label="Total Company comparable sales",
        ),
        _row(
            period=46144,
            value=829,
            unit="stores",
            label="Total Company Company-owned stores, end",
        ),
    ]
    package = build_shadow_registry(rows, ANF_PROFILE)
    assert {item.evidence.driver.driver_id for item in package.observations} == {
        "driver:operating:comparable-sales@1",
        "driver:operating:company-owned-stores-end@1",
    }
    assert {item.evidence.period.display_label for item in package.observations} == {
        "2025-Q4",
        "2026-Q1",
    }


def test_pbi_sparse_evidence_stays_qualitative() -> None:
    package = build_shadow_registry(
        [
            _row(
                period=46112,
                value=None,
                unit=None,
                label="Volume / throughput",
                commentary="Presort Services volume improved.",
            )
        ],
        PBI_PROFILE,
    )
    assert not package.observations
    assert package.disposition_counts["QUALITATIVE_SUPPORT"] == 1
    assert package.attachments[0].canonical_driver_id == (
        "driver:operating:presort-volume-context@1"
    )


def test_gpre_definition_break_never_joins_eight_and_nine_plant_series() -> None:
    rows = [
        _row(
            period=45930,
            value=101,
            label="Utilization",
            commentary="Nine operating ethanol plants ran at 101%.",
        ),
        _row(
            period=46022,
            value=97,
            label="Utilization",
            commentary="Eight operating ethanol plants ran at 97%.",
        ),
    ]
    package = build_shadow_registry(rows, GPRE_PROFILE)
    assert len(package.observations) == 2
    assert len(package.series) == 2
    assert {item.definition_version for item in package.series} == {1, 2}
    transition = next(
        item for item in package.observations if item.evidence.driver.definition_version == 2
    )
    assert transition.evidence.continuity.state.value == (
        "DEFINITION_CHANGED_BREAK_SERIES"
    )
    assert transition.evidence.continuity.from_definition_version == 1


def test_cross_ticker_packages_share_one_record_contract() -> None:
    anf = build_shadow_registry(
        [
            _row(period=46053, value=1, label="Total Company comparable sales"),
            _row(period=46144, value=2, label="Total Company comparable sales"),
        ],
        ANF_PROFILE,
    )
    pbi = build_shadow_registry(
        [_row(period=46112, value=None, unit=None, label="Volume / throughput", commentary="Presort volume context.")],
        PBI_PROFILE,
    )
    gpre = build_shadow_registry(
        [_row(period=46112, value=174.196, unit="m gallons", label="Ethanol gallons produced")],
        GPRE_PROFILE,
    )
    for package in (anf, pbi, gpre):
        assert package.to_dict()["contract_version"] == (
            "operating-drivers-canonical-shadow-registry@1"
        )
        assert package.reconciliation["reconciles"] is True


def test_financial_and_forecast_capabilities_serialize_closed_values() -> None:
    values = {
        definition.financial_linkage.value
        for profile in OPERATING_DRIVER_SHADOW_PROFILES.values()
        for definition in profile.definitions
    }
    forecasts = {
        definition.forecast_capability.value
        for profile in OPERATING_DRIVER_SHADOW_PROFILES.values()
        for definition in profile.definitions
    }
    assert values <= {
        "SOURCE_DEFINED_CAUSAL_RELATION",
        "ACCOUNTING_IDENTITY",
        "ECONOMICALLY_JUSTIFIED_MODEL",
        "EMPIRICAL_ASSOCIATION",
        "SPECULATIVE_ASSOCIATION",
        "NONE",
    }
    assert forecasts <= {
        "DIRECT_FORECAST_INPUT",
        "LEADING_INDICATOR",
        "FORECAST_CONTEXT",
        "SENSITIVITY_INPUT",
        "HISTORICAL_ONLY",
        "NOT_FORECASTABLE",
    }


def test_visibility_candidates_use_closed_product_tiers() -> None:
    values = {
        definition.visibility_tier.value
        for profile in OPERATING_DRIVER_SHADOW_PROFILES.values()
        for definition in profile.definitions
    }
    assert values <= {
        "CORE_DRIVER",
        "SECONDARY_DRIVER",
        "WATCH_DRIVER",
        "SUPPORT_ONLY",
        "RETIRED",
    }
