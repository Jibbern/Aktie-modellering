from __future__ import annotations

from pathlib import Path

import pytest

from pbi_xbrl.anf_capital_return_source_adapter import (
    build_anf_capital_return_collection,
)
from pbi_xbrl.new_ticker_capital_return import (
    build_capital_return_workbook_projection,
    validate_capital_return_records,
)
from scripts.build_anf_shadow_normalized_package import _default_data_root


SEC_CACHE = _default_data_root() / "sec_cache" / "ANF"


@pytest.fixture(scope="module")
def extraction():
    return build_anf_capital_return_collection(SEC_CACHE)


def _record(extraction, metric_id: str, period: str, period_type: str):
    matches = [
        row
        for row in extraction.records
        if row["metric_id"] == metric_id
        and row["fiscal_period"] == period
        and row["period_type"] == period_type
    ]
    assert len(matches) == 1
    return matches[0]


def test_source_native_collection_has_exact_typed_inventory(extraction) -> None:
    section = extraction.package_section()
    assert section["collection_version"] == "1.0"
    assert len(section["records"]) == 538
    assert len(section["guidance"]) == 8
    assert len(section["period_reconciliations"]) == 19
    assert validate_capital_return_records(section["records"])
    assert all(
        not str(row["source_document"]).lower().endswith("anf_model.xlsx")
        for row in section["records"]
    )
    assert all(
        "legacy_artifact_backed" != row["source_classification"]
        for row in section["records"]
    )


def test_fy2025_buyback_and_share_roll_forward_reconcile(extraction) -> None:
    period = "2025-FY"
    values = {
        metric: _record(extraction, metric, period, "annual")["value"]
        for metric in (
            "repurchase_cash_program",
            "accounting_program_shares_repurchased",
            "share_issuance_sbc",
            "net_share_reduction",
            "beginning_period_end_shares",
            "ending_period_end_shares",
            "diluted_weighted_average_shares",
            "cash_per_program_share",
            "authorization_remaining",
            "free_cash_flow",
            "buybacks_to_fcf",
        )
    }
    assert values["repurchase_cash_program"] == pytest.approx(451.224)
    assert values["accounting_program_shares_repurchased"] == pytest.approx(5.365)
    assert values["share_issuance_sbc"] == pytest.approx(0.635)
    assert values["net_share_reduction"] == pytest.approx(4.73)
    assert values["beginning_period_end_shares"] == pytest.approx(49.735)
    assert values["ending_period_end_shares"] == pytest.approx(45.005)
    assert values["diluted_weighted_average_shares"] == pytest.approx(48.476)
    assert values["cash_per_program_share"] == pytest.approx(84.105126)
    assert values["authorization_remaining"] == pytest.approx(850.080717)
    assert values["free_cash_flow"] == pytest.approx(378.368)
    assert values["buybacks_to_fcf"] == pytest.approx(1.192553)


def test_q1_2026_program_and_withholding_identities_remain_distinct(extraction) -> None:
    period = "2026-Q1"
    values = {
        metric: _record(extraction, metric, period, "quarter")["value"]
        for metric in (
            "repurchase_cash_program",
            "accounting_program_shares_repurchased",
            "public_program_shares_repurchased",
            "total_issuer_purchases",
            "employee_tax_withholding_shares",
            "share_issuance_sbc",
            "beginning_period_end_shares",
            "ending_period_end_shares",
            "diluted_weighted_average_shares",
            "reported_average_all_purchases",
            "cash_per_program_share",
            "authorization_remaining",
        )
    }
    assert values["repurchase_cash_program"] == pytest.approx(105.018)
    assert values["accounting_program_shares_repurchased"] == pytest.approx(1.156)
    assert values["public_program_shares_repurchased"] == pytest.approx(1.155996)
    assert values["total_issuer_purchases"] == pytest.approx(1.590558)
    assert values["employee_tax_withholding_shares"] == pytest.approx(0.434562)
    assert values["share_issuance_sbc"] == pytest.approx(0.582)
    assert values["beginning_period_end_shares"] == pytest.approx(45.005)
    assert values["ending_period_end_shares"] == pytest.approx(44.431)
    assert values["diluted_weighted_average_shares"] == pytest.approx(45.677)
    assert values["reported_average_all_purchases"] == pytest.approx(90.18)
    assert values["cash_per_program_share"] == pytest.approx(90.846021)
    assert values["authorization_remaining"] == pytest.approx(745.080737)
    assert values["cash_per_program_share"] != values["reported_average_all_purchases"]


def test_exact_ttm_uses_four_compatible_quarters_and_terminal_snapshots(extraction) -> None:
    period = "TTM through 2026-Q1"
    values = {
        metric: _record(extraction, metric, period, "ttm")["value"]
        for metric in (
            "repurchase_cash_program",
            "accounting_program_shares_repurchased",
            "share_issuance_sbc",
            "net_share_reduction",
            "beginning_period_end_shares",
            "ending_period_end_shares",
            "diluted_weighted_average_shares",
            "cash_per_program_share",
            "authorization_remaining",
            "free_cash_flow",
            "buybacks_to_fcf",
        )
    }
    assert values["repurchase_cash_program"] == pytest.approx(356.242)
    assert values["accounting_program_shares_repurchased"] == pytest.approx(3.872)
    assert values["share_issuance_sbc"] == pytest.approx(0.66)
    assert values["net_share_reduction"] == pytest.approx(3.212)
    assert values["beginning_period_end_shares"] == pytest.approx(47.643)
    assert values["ending_period_end_shares"] == pytest.approx(44.431)
    assert values["diluted_weighted_average_shares"] == pytest.approx(47.23675)
    assert values["cash_per_program_share"] == pytest.approx(92.004649)
    assert values["authorization_remaining"] == pytest.approx(745.080737)
    assert values["free_cash_flow"] == pytest.approx(416.047)
    assert values["buybacks_to_fcf"] == pytest.approx(0.856254)

    reconciliation = next(
        row
        for row in extraction.period_reconciliations
        if row["fiscal_period"] == period
    )
    assert reconciliation["component_periods"] == [
        "2025-Q2",
        "2025-Q3",
        "2025-Q4",
        "2026-Q1",
    ]
    assert reconciliation["method"] == "exact four-consecutive-quarter aggregation"
    authorization = _record(extraction, "authorization_remaining", period, "ttm")
    assert authorization["aggregation_role"] == "point_in_time"
    assert "terminal" in authorization["derivation_identity"]


def test_q4_and_ytd_reconciliations_are_explicit(extraction) -> None:
    reconciliations = {
        (row["fiscal_period"], row["period_type"]): row
        for row in extraction.period_reconciliations
    }
    for fiscal_year in (2024, 2025):
        for quarter in range(1, 5):
            assert (f"{fiscal_year}-Q{quarter}", "quarter") in reconciliations
        assert (f"{fiscal_year}-FY", "annual") in reconciliations
        assert "annual minus Q3 YTD" in reconciliations[
            (f"{fiscal_year}-Q4", "quarter")
        ]["method"]
    assert ("2026-Q1", "quarter") in reconciliations
    assert ("2026-Q1-YTD", "year_to_date") in reconciliations


def test_dividend_and_historical_eps_states_are_unavailable_not_zero(extraction) -> None:
    for period, period_type in (
        ("2025-FY", "annual"),
        ("2026-Q1", "quarter"),
        ("TTM through 2026-Q1", "ttm"),
    ):
        dividend = _record(extraction, "dividends_paid", period, period_type)
        total = _record(extraction, "total_capital_return", period, period_type)
        historical_eps = _record(
            extraction,
            "historical_buyback_eps_attribution",
            period,
            period_type,
        )
        assert dividend["value"] is None
        assert dividend["source_classification"] == "unavailable"
        assert total["value"] is None
        assert historical_eps["value"] is None
        assert "unavailable" in historical_eps["reason"].lower()

    q1_coverage = _record(extraction, "buybacks_to_fcf", "2026-Q1", "quarter")
    assert q1_coverage["value"] is None
    assert "zero or negative" in q1_coverage["reason"]


def test_guidance_is_typed_superseded_and_separate_from_actuals(extraction) -> None:
    guidance = extraction.guidance
    accepted = [row for row in guidance if row["status"] == "accepted"]
    superseded = [row for row in guidance if row["status"] == "superseded"]
    assert len(accepted) == 6
    assert len(superseded) == 2
    latest_fy_buyback = next(
        row
        for row in accepted
        if row["metric_id"] == "repurchase_cash_program"
        and row["applicable_period"] == "2026-FY"
    )
    assert latest_fy_buyback["publication_date"] == "2026-05-27"
    assert latest_fy_buyback["numeric_state"] == "approximate_point"
    assert latest_fy_buyback["point"] == 450
    assert latest_fy_buyback["supersedes_guidance_ids"]
    assert all(row["period_type"] == "guidance" for row in guidance)
    assert all(
        row["period_type"] != "guidance"
        for row in extraction.records
    )


def test_workbook_projection_uses_exact_actual_periods(extraction) -> None:
    projection = build_capital_return_workbook_projection(
        {"capital_returns": extraction.package_section()}
    )
    assert projection.collection_state == "source_native"
    assert projection.latest_quarter_label == "2026-Q1"
    assert projection.ttm_label == "TTM through 2026-Q1"
    assert projection.annual_label == "2025-FY"
    assert len(projection.product_rows) == 15
    buybacks = next(
        row for row in projection.product_rows if row["row_key"] == "repurchase_cash_program"
    )
    assert buybacks["latest_quarter"] == pytest.approx(105.018)
    assert buybacks["ttm"] == pytest.approx(356.242)
    assert buybacks["latest_completed_year"] == pytest.approx(451.224)
    dividends = next(
        row for row in projection.product_rows if row["row_key"] == "dividends_paid"
    )
    assert dividends["latest_quarter"] is None
    assert dividends["ttm"] is None
    assert dividends["latest_completed_year"] is None
    assert dividends["state_context"].startswith("Unavailable:")
