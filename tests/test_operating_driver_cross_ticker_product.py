from __future__ import annotations

from decimal import Decimal
import hashlib
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_product import (
    OperatingDriverCrossTickerError,
    build_cross_ticker_operating_driver_package,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_profiles import PROFILES
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_source_parsing import (
    DeclarativeExtractionRule,
    OperatingDriverSourceParsingError,
    extract_source_native_facts,
)


@pytest.fixture(scope="module")
def packages():
    return {key: build_cross_ticker_operating_driver_package(value) for key, value in PROFILES.items()}


def test_profiles_use_dynamic_latest_quarter(packages):
    assert {item.latest_period_label for item in packages.values()} == {"2026-Q2"}
    assert all(item.quarter_labels[-1] == item.latest_period_label for item in packages.values())


def test_pbi_missing_is_not_zero(packages):
    row = next(item for item in packages["PBI"].history_rows if item.driver_id == "pbi.presort.mail_pieces_bn")
    missing = [item for item in row.points if item.status == "NOT_DISCLOSED"]
    assert missing
    assert all(item.value is None and item.display_value == "" for item in missing)


def test_pbi_qualitative_evidence_remains_non_numeric(packages):
    qualitative = [item for item in packages["PBI"].observations if item.precision == "QUALITATIVE"]
    assert qualitative
    assert all(item.value is None for item in qualitative)


def test_pbi_investor_presentations_are_first_class_sources(packages):
    presentations = [item for item in packages["PBI"].source_documents if item.source_type == "OFFICIAL_INVESTOR_PRESENTATION"]
    assert len(presentations) == 12
    assert all(Path(item.local_path).is_file() for item in presentations if item.local_path)


def test_pbi_latest_exact_volume_and_growth(packages):
    observations = {(item.driver_id, item.period_label): item for item in packages["PBI"].observations}
    assert observations[("pbi.presort.mail_pieces_bn", "2026-Q2")].value == Decimal("3.3")
    assert observations[("pbi.presort.volume_growth_yoy", "2026-Q2")].value == Decimal("-3")


def test_pbi_core_decline_moderation_is_typed_pp(packages):
    row = next(item for item in packages["PBI"].core_drivers if item.driver_id == "pbi.presort.volume_growth_yoy")
    assert row.qoq_value == Decimal("3")
    assert row.qoq_status == "AVAILABLE"
    assert row.yoy_value is None


def test_pbi_core_is_organized_as_an_economic_driver_map(packages):
    rows = packages["PBI"].core_drivers
    assert [item.group_label for item in rows] == [
        "Presort Economics",
        "Presort Economics",
        "SendTech Leading Indicators",
        "SendTech Leading Indicators",
        "SendTech Leading Indicators",
    ]
    assert "Signals future SendTech" in next(item for item in rows if item.driver_id == "pbi.sendtech.sales_bookings_direction").why_it_matters
    assert "committed demand" in next(item for item in rows if item.driver_id == "pbi.sendtech.backlog_state").why_it_matters


def test_gpre_utilization_comparison_stops_at_definition_break(packages):
    row = next(item for item in packages["GPRE"].core_drivers if item.driver_id == "gpre.utilization.percent")
    assert row.qoq_value == Decimal("-9")
    assert row.yoy_value is None
    assert row.yoy_status == "UNAVAILABLE_DEFINITION_BREAK"


def test_gpre_45z_missing_quarters_never_become_zero(packages):
    row = next(item for item in packages["GPRE"].history_rows if item.driver_id == "gpre.45z.realized_benefit_usd_m")
    missing = [item for item in row.points if item.status == "NOT_DISCLOSED"]
    assert missing
    assert all(item.value is None for item in missing)


def test_gpre_complete_ttm_45z_is_safe_sum(packages):
    item = next(value for value in packages["GPRE"].safe_derivations if value.result_period_label == "TTM through 2026-Q2")
    assert item.result_status == "AVAILABLE"
    assert item.result_value == Decimal("166.9")
    assert len(item.input_observation_ids) == 4


def test_gpre_incomplete_fy_45z_fails_closed(packages):
    item = next(value for value in packages["GPRE"].safe_derivations if value.result_period_label == "2025-FY")
    assert item.result_status == "UNAVAILABLE_INCOMPLETE_PERIOD_SET"
    assert item.result_value is None


def test_gpre_crush_definitions_are_not_collapsed(packages):
    ids = {item["driver_id"] for item in packages["GPRE"].driver_registry}
    assert "gpre.crush.consolidated_usd_m" in ids
    assert "gpre.crush.underlying_ex45z_usd_m" in ids
    assert "gpre.45z.realized_benefit_usd_m" in ids


def test_gpre_core_separates_production_unit_economics_and_policy(packages):
    rows = packages["GPRE"].core_drivers
    assert [(item.group_label, item.driver_id) for item in rows] == [
        ("Production & Asset Utilization", "gpre.ethanol.sold_mgal"),
        ("Production & Asset Utilization", "gpre.utilization.percent"),
        ("Commodity Unit Economics", "gpre.crush.underlying_ex45z_usd_m"),
        ("Commodity Unit Economics", "gpre.crush.consolidated_usd_m"),
        ("Policy & Low-Carbon Economics", "gpre.45z.realized_benefit_usd_m"),
    ]


def test_gpre_underlying_crush_core_fails_closed_when_current_is_not_disclosed(packages):
    row = next(item for item in packages["GPRE"].core_drivers if item.driver_id == "gpre.crush.underlying_ex45z_usd_m")
    assert row.latest_value is None
    assert row.latest_display == "Not disclosed"
    assert row.qoq_status == row.yoy_status == "UNAVAILABLE_CURRENT"
    assert row.broader_trend == "Needs current disclosure"


def test_gpre_corn_moves_to_evidence_layer_without_data_deletion(packages):
    core_ids = {item.driver_id for item in packages["GPRE"].core_drivers}
    history_ids = {item.driver_id for item in packages["GPRE"].history_rows}
    assert "gpre.corn.consumed_mbu" not in core_ids
    assert "gpre.corn.consumed_mbu" in history_ids
    assert "gpre.crush.underlying_ex45z_usd_m" in history_ids


def test_gpre_supporting_throughput_stays_in_history_not_core(packages):
    core_ids = {item.driver_id for item in packages["GPRE"].core_drivers}
    history_ids = {item.driver_id for item in packages["GPRE"].history_rows}
    assert "gpre.ethanol.produced_mgal" not in core_ids
    assert "gpre.ethanol.produced_mgal" in history_ids


def test_why_it_matters_copy_is_concise_investor_language(packages):
    rows = [item for package in packages.values() for item in package.core_drivers]
    assert all(item.why_it_matters.endswith(".") for item in rows)
    assert all(len(item.why_it_matters) <= 70 for item in rows)
    assert not any("helps assess" in item.why_it_matters.lower() for item in rows)


def test_gpre_ci_and_ccs_are_guide_only_not_fabricated_numeric_drivers(packages):
    package = packages["GPRE"]
    terms = {item.term: item for item in package.guide_terms}
    assert terms["Carbon intensity (CI)"].definition_authority == "PROFILE_DERIVED"
    assert terms["Carbon capture and storage (CCS)"].definition_authority == "SOURCE_DEFINED"
    ids = {item["driver_id"] for item in package.driver_registry}
    assert not any(token in driver_id.lower() for driver_id in ids for token in ("carbon_intensity", ".ci.", ".ccs."))


def test_safe_derivations_do_not_sum_rates(packages):
    assert all(item.driver_id != "gpre.utilization.percent" for item in packages["GPRE"].safe_derivations)


def test_direct_observations_are_not_replaced_by_derivations(packages):
    assert all(item.derivation_id is None for package in packages.values() for item in package.observations)


def test_overview_contains_no_management_commentary_or_forecast(packages):
    forbidden = ("management said", "management expects", "we expect", "guidance implies")
    assert not any(token in item.text.lower() for package in packages.values() for item in package.overview for token in forbidden)


def test_internal_enums_do_not_leak_into_visible_language(packages):
    visible = " ".join(
        [item.text for package in packages.values() for item in package.overview]
        + [item.broader_trend for package in packages.values() for item in package.core_drivers]
    )
    assert "UNAVAILABLE_" not in visible
    assert "DEFINITION_BREAK@" not in visible


def test_driver_guides_have_meaning_and_economic_role_only(packages):
    assert all(item.guide_terms for item in packages.values())
    assert all(item.meaning and item.economic_role for package in packages.values() for item in package.guide_terms)
    assert all(not hasattr(item, "measurement") for package in packages.values() for item in package.guide_terms)


def test_profiles_reject_qualitative_numeric_fabrication():
    profile = dict(PROFILES["PBI"])
    observations = [dict(item) for item in profile["observations"]]
    target = next(item for item in observations if item["precision"] == "QUALITATIVE")
    target["value"] = 1
    profile["observations"] = observations
    with pytest.raises(OperatingDriverCrossTickerError, match="may not carry exact numbers"):
        build_cross_ticker_operating_driver_package(profile)


def test_shared_parser_extracts_exact_values_without_ticker_branch():
    rule = DeclarativeExtractionRule(
        rule_id="mail-pieces",
        driver_id="mail.pieces",
        pattern=r"was\s+(?P<value>3\.3)\s+billion pieces",
        unit="billion_pieces",
        definition_id="mail-pieces@1",
        precision="EXACT",
    )
    facts = extract_source_native_facts(
        "Total volume sorted in the quarter was 3.3 billion pieces of mail.",
        source_id="official-release",
        period_label="2026-Q2",
        rules=(rule,),
    )
    assert facts[0].value == Decimal("3.3")


def test_shared_parser_refuses_numeric_output_for_qualitative_rule():
    rule = DeclarativeExtractionRule(
        rule_id="qualitative",
        driver_id="bookings",
        pattern=r"bookings increased",
        unit="qualitative",
        definition_id="bookings@1",
        precision="QUALITATIVE",
        value_group="value",
    )
    with pytest.raises(OperatingDriverSourceParsingError, match="may not emit exact"):
        extract_source_native_facts(
            "Bookings increased.", source_id="letter", period_label="2026-Q2", rules=(rule,)
        )


def test_no_ticker_specific_economic_conditionals_in_shared_modules():
    root = Path(__file__).resolve().parents[1] / "pbi_xbrl" / "longitudinal_memory"
    modules = (
        root / "operating_driver_cross_ticker_product.py",
        root / "operating_driver_cross_ticker_source_parsing.py",
        root / "operating_driver_cross_ticker_workbook.py",
    )
    content = "\n".join(path.read_text(encoding="utf-8") for path in modules)
    assert 'if ticker == "PBI"' not in content
    assert 'if ticker == "GPRE"' not in content


def test_accepted_anf_visible_authority_is_unchanged():
    path = Path(
        r"C:\Users\Jibbe\Aktier\StockModelData\audit\anf_operating_drivers_footprint_economic_guide_measurement_hidden_2026-08-20\ANF_operating_drivers_footprint_economic_guide_preview.xlsx"
    )
    assert hashlib.sha256(path.read_bytes()).hexdigest() == "3a99f3dd098884744b71313fb9d44ad02da0fb8906a6e6567c28f290bf4dcc8e"
