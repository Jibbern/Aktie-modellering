import copy
import json
import os
import random
from pathlib import Path

import pytest

from pbi_xbrl.new_ticker_investment_case_projection import (
    build_investment_case_workbook_projection,
    format_typed_guidance_display,
)


DATA_ROOT = Path(os.environ.get("STOCK_MODEL_DATA_ROOT", r"C:\Users\Jibbe\Aktier\StockModelData"))
ANF_PACKAGE = DATA_ROOT / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_normalized_data_package.json"
PROFILE_PACKS = {"retail_operating_pack"}


@pytest.fixture(scope="module")
def anf_package() -> dict:
    return json.loads(ANF_PACKAGE.read_text(encoding="utf-8"))


def _projection(package: dict):
    return build_investment_case_workbook_projection(package, profile_pack_ids=PROFILE_PACKS)


def _inputs(projection) -> dict:
    return {row.metric_id: row for row in projection.market_inputs}


def test_anf_projection_resolves_exact_fy_ttm_guidance_segments_and_debates(anf_package: dict) -> None:
    projection = _projection(anf_package)
    inputs = _inputs(projection)

    assert projection.fy_period == "2025-FY"
    assert projection.ttm_period == "TTM through 2026-Q1"
    assert projection.ttm_quarters == ("2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1")
    assert projection.full_year_guidance_period == "FY2026"
    assert projection.quarter_guidance_period == "2026-Q1"
    assert len(projection.market_inputs) == 29
    assert len(projection.segment_inputs) == 6
    assert len(projection.debates) == 2

    assert inputs["revenue"].fy_value == pytest.approx(5266.292)
    assert inputs["revenue"].ttm_value == pytest.approx(5282.802)
    assert inputs["base_ebitda"].ttm_value == pytest.approx(845.156)
    assert inputs["adjusted_ebitda"].ttm_value == pytest.approx(806.582)
    assert inputs["free_cash_flow"].ttm_value == pytest.approx(416.047)
    assert inputs["diluted_shares"].ttm_value == pytest.approx(45.677)
    assert inputs["revenue_growth"].full_year_guidance_display == "3–5%"
    assert inputs["revenue_growth"].quarter_guidance_display == "1–3%"
    assert inputs["revenue_growth"].publication_date == "2026-03-04"
    assert inputs["operating_margin"].full_year_guidance_display == "12–12.5%"
    assert inputs["operating_margin"].quarter_guidance_display == "~7.0%"
    assert inputs["operating_margin"].quarter_guidance_numeric_state == "qualitative_only"
    assert inputs["operating_margin"].quarter_guidance_low is None
    assert inputs["operating_margin"].quarter_guidance_high is None
    assert inputs["adjusted_eps_guidance"].selected_value is None
    assert inputs["adjusted_eps_guidance"].status == "reference_only"
    assert inputs["adjusted_eps_guidance"].full_year_guidance_display == "10.20–11.00"
    assert inputs["adjusted_eps_guidance"].quarter_guidance_display == "1.20–1.30"
    assert inputs["capital_expenditures"].full_year_guidance_display == "200–225"
    assert inputs["buyback_cash"].selected_value is None
    assert inputs["buyback_cash"].basis_kind == "historical_context_only"
    assert inputs["buyback_cash"].full_year_guidance_display == "~450"
    assert inputs["buyback_cash"].quarter_guidance_display == "≥100"
    assert inputs["diluted_shares"].full_year_guidance_display == "~45"
    assert inputs["diluted_shares"].quarter_guidance_display == "~46"
    assert inputs["price"].selected_source == "Unavailable"
    assert inputs["price"].basis_kind == "latest_snapshot"
    assert inputs["price"].basis_period == ""
    assert inputs["net_debt"].selected_value is None
    assert inputs["revenue"].basis_kind == "exact_four_quarter_ttm"
    assert inputs["revenue"].basis_period == "TTM through 2026-Q1"
    assert inputs["diluted_shares"].basis_kind == "latest_snapshot"
    assert inputs["diluted_shares"].basis_period == "2026-Q1"

    segment_keys = [(row.dimension_id, row.member) for row in projection.segment_inputs]
    assert segment_keys == [
        ("total_company", "total_company"),
        ("brand", "hollister"),
        ("brand", "abercrombie"),
        ("geography", "americas"),
        ("geography", "emea"),
        ("geography", "apac"),
    ]
    assert projection.dimension_options == ("total_company", "brand", "geography")
    assert projection.segment_inputs[0].ttm_value == pytest.approx(5282.8)
    assert all(row.ttm_value is None for row in projection.segment_inputs[1:])
    assert all(
        row.basis_kind == "latest_completed_fy"
        for row in projection.segment_inputs[1:]
    )
    assert all(row.basis_period == "2025-FY" for row in projection.segment_inputs[1:])
    assert all("\\" not in row.source_alias and "/" not in row.source_alias for row in projection.debates)


def test_latest_quarter_mutation_changes_ttm_without_changing_completed_fy(anf_package: dict) -> None:
    before = _projection(anf_package)
    package = copy.deepcopy(anf_package)
    latest = copy.deepcopy(package["quarterly_financials"]["rows"][-1])
    latest["period"] = "2026-Q2"
    latest["fiscal_year"] = 2026
    latest["fiscal_quarter"] = 2
    latest["period_end"] = "2026-08-01"
    latest["revenue"]["value"] = 1500.0
    latest["gross_profit"]["value"] = 900.0
    latest["operating_income"]["value"] = 180.0
    latest["base_ebitda"]["value"] = 220.0
    latest["adjusted_ebitda"]["value"] = 210.0
    latest["net_income"]["value"] = 130.0
    latest["free_cash_flow"]["value"] = 120.0
    latest["depreciation_amortization"]["value"] = 40.0
    latest["capital_expenditures"]["value"] = 60.0
    latest["buybacks_cash"]["value"] = 75.0
    package["quarterly_financials"]["rows"].append(latest)

    after = _projection(package)
    assert after.fy_period == before.fy_period == "2025-FY"
    assert after.ttm_quarters == ("2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2")
    assert _inputs(after)["revenue"].fy_value == _inputs(before)["revenue"].fy_value
    assert _inputs(after)["revenue"].ttm_value != _inputs(before)["revenue"].ttm_value


def test_new_completed_fiscal_year_advances_label_and_values(anf_package: dict) -> None:
    package = copy.deepcopy(anf_package)
    latest = copy.deepcopy(package["annual_financials"]["rows"][-1])
    latest["period"] = "2026-FY"
    latest["fiscal_year"] = 2026
    latest["revenue"]["value"] = 6000.0
    for metric in ("gross_profit", "operating_income", "net_income", "free_cash_flow"):
        assert latest[metric]["status"] == "populated"
    package["annual_financials"]["rows"].append(latest)

    projection = _projection(package)
    assert projection.fy_period == "2026-FY"
    assert _inputs(projection)["revenue"].fy_value == 6000.0


def test_missing_one_ttm_quarter_fails_closed_instead_of_presenting_partial_ttm(anf_package: dict) -> None:
    package = copy.deepcopy(anf_package)
    package["quarterly_financials"]["rows"] = [
        row for row in package["quarterly_financials"]["rows"] if row["period"] != "2025-Q3"
    ]
    projection = _projection(package)
    assert projection.ttm_quarters == ()
    assert projection.ttm_period == ""
    assert _inputs(projection)["revenue"].ttm_value is None
    assert _inputs(projection)["operating_margin"].ttm_value is None
    assert _inputs(projection)["revenue"].fy_value == pytest.approx(5266.292)


def test_newer_full_year_guidance_supersedes_only_the_full_year_scope(anf_package: dict) -> None:
    package = copy.deepcopy(anf_package)
    current = next(
        row
        for row in package["normalized_guidance"]["items"]
        if row.get("evidence_key") == "17892347cc65bfd8"
    )
    current["display_role"] = "superseded"
    current["visibility_disposition"] = "superseded"
    update = copy.deepcopy(current)
    update["value"]["value"] = "4%, 6%"
    update["comparison_contract"]["low"] = 4.0
    update["comparison_contract"]["high"] = 6.0
    update["publication_date"] = "2026-04-01"
    update["source_date"] = "2026-04-01"
    update["evidence_key"] = "focused_pass_b1_new_fy_revenue"
    update["source_ref"] = "fixture:new-fy-guidance"
    update["display_role"] = "current_primary"
    update["visibility_disposition"] = "current_primary"
    update["superseded_by_evidence_key"] = None
    package["normalized_guidance"]["items"].append(update)

    projection = _projection(package)
    revenue_growth = _inputs(projection)["revenue_growth"]
    assert revenue_growth.full_year_guidance_display == "4–6%"
    assert revenue_growth.full_year_guidance_low == pytest.approx(0.04)
    assert revenue_growth.full_year_guidance_high == pytest.approx(0.06)
    assert revenue_growth.quarter_guidance_display == "1–3%"


def test_newer_quarter_guidance_supersedes_only_the_quarter_scope(anf_package: dict) -> None:
    package = copy.deepcopy(anf_package)
    current = next(
        row
        for row in package["normalized_guidance"]["items"]
        if row.get("evidence_key") == "1eebe3d331734079"
    )
    current["display_role"] = "superseded"
    current["visibility_disposition"] = "superseded"
    update = copy.deepcopy(current)
    update["value"]["value"] = "2%, 4%"
    update["comparison_contract"]["low"] = 2.0
    update["comparison_contract"]["high"] = 4.0
    update["publication_date"] = "2026-04-01"
    update["source_date"] = "2026-04-01"
    update["evidence_key"] = "focused_pass_b1_new_q_revenue"
    update["source_ref"] = "fixture:new-quarter-guidance"
    update["display_role"] = "current_primary"
    update["visibility_disposition"] = "current_primary"
    update["superseded_by_evidence_key"] = None
    package["normalized_guidance"]["items"].append(update)

    projection = _projection(package)
    revenue_growth = _inputs(projection)["revenue_growth"]
    assert revenue_growth.full_year_guidance_display == "3–5%"
    assert revenue_growth.quarter_guidance_display == "2–4%"
    assert revenue_growth.quarter_guidance_low == pytest.approx(0.02)
    assert revenue_growth.quarter_guidance_high == pytest.approx(0.04)
    assert revenue_growth.selected_source == "Model default (TTM)"
    assert revenue_growth.selected_value == revenue_growth.ttm_value


def test_projection_is_source_order_independent(anf_package: dict) -> None:
    package = copy.deepcopy(anf_package)
    rng = random.Random(4127)
    rng.shuffle(package["annual_financials"]["rows"])
    rng.shuffle(package["quarterly_financials"]["rows"])
    rng.shuffle(package["segments"]["items"])
    rng.shuffle(package["normalized_guidance"]["items"])
    assert _projection(package).to_dict() == _projection(anf_package).to_dict()


def test_projection_support_rows_have_unique_typed_slot_identities(anf_package: dict) -> None:
    projection = _projection(anf_package)
    slot_keys = [row["slot_key"] for row in projection.workbook_rows]
    row_keys = [row["row_key"] for row in projection.workbook_rows]
    assert len(slot_keys) == len(set(slot_keys))
    assert len(row_keys) == len(set(row_keys))
    assert {row["row_type"] for row in projection.workbook_rows} == {
        "dimension_option",
        "market_input",
        "segment_input",
        "debate",
    }
    assert all(row["source_ref"] for row in projection.workbook_rows)
    guidance_rows = [
        row
        for row in projection.workbook_rows
        if row["row_type"] == "market_input" and (
            row["full_year_guidance_display"] or row["quarter_guidance_display"]
        )
    ]
    assert len(guidance_rows) == 6
    assert {row["publication_date"] for row in guidance_rows} == {"2026-03-04"}
    assert all(row["unit"] == "choice" for row in projection.workbook_rows if row["row_type"] == "dimension_option")
    assert all(row["unit"] == "text" for row in projection.workbook_rows if row["row_type"] == "debate")
    assert all("(" in row.metric_label and ")" in row.metric_label for row in projection.market_inputs)
    assert all(
        str(row["metric_label"]).endswith("($m)")
        for row in projection.workbook_rows
        if row["row_type"] == "segment_input"
    )
    assert projection.market_inputs[0].metric_label == "Current share price ($/share)"
    assert projection.market_inputs[-1].metric_label == "DCF forecast period (years)"
    assert all(
        set(("basis_kind", "basis_period", "full_year_guidance_numeric_state",
             "quarter_guidance_numeric_state", "source_alias")).issubset(row)
        for row in projection.workbook_rows
    )
    operating_margin = next(
        row for row in projection.workbook_rows
        if row["slot_key"] == "market_input|operating_margin"
    )
    assert operating_margin["quarter_guidance_display"] == "~7.0%"
    assert operating_margin["quarter_guidance_numeric_state"] == "qualitative_only"
    assert operating_margin["quarter_guidance_low"] is None
    assert operating_margin["quarter_guidance_high"] is None
    assert "Latest-quarter source guidance: around 7.0%" in operating_margin["notes"]
    assert all(
        "\\" not in str(row["source_alias"]) and "/" not in str(row["source_alias"])
        for row in projection.workbook_rows
    )


def test_dimension_options_are_derived_from_canonical_segment_identities(anf_package: dict) -> None:
    package = copy.deepcopy(anf_package)
    for row in package["segments"]["items"]:
        if row.get("dimension") == "geography":
            row["dimension"] = "channel"

    projection = _projection(package)
    assert projection.dimension_options == ("total_company", "brand", "channel")
    assert "geography" not in projection.dimension_options
    assert {
        row.dimension_id
        for row in projection.segment_inputs
        if row.dimension_id != "total_company"
    } == {"brand", "channel"}


def test_projection_runtime_has_no_ticker_branch_or_positional_item_selection() -> None:
    source = Path("pbi_xbrl/new_ticker_investment_case_projection.py").read_text(encoding="utf-8")
    assert ".items.0" not in source
    assert "ticker ==" not in source
    assert "'ANF'" not in source
    assert '"ANF"' not in source


@pytest.mark.parametrize(
    ("raw_display", "numeric_state", "low", "high", "unit", "expected"),
    (
        ("45 million", "typed_point", 45.0, 45.0, "m shares", "45"),
        ("around 45 million", "typed_approximate_point", 45.0, 45.0, "m shares", "~45"),
        ("around 45 million", "qualitative_only", None, None, "m shares", "~45"),
        ("around $450 million", "qualitative_only", None, None, "$m", "~450"),
        ("at least $100 million", "qualitative_only", None, None, "$m", "≥100"),
        ("at least 6%", "typed_minimum_point", 0.06, 0.06, "%", "≥6%"),
        ("$200 million, $225 million", "typed_range", 200.0, 225.0, "$m", "200–225"),
        ("3 percent, 5 percent", "typed_range", 0.03, 0.05, "%", "3–5%"),
        ("around 7.0%", "typed_approximate_point", 0.07, 0.07, "%", "~7.0%"),
        ("$10.20, $11.00", "typed_range", 10.2, 11.0, "$/share", "10.20–11.00"),
        ("No numeric point", "qualitative_only", None, None, "%", "No numeric point"),
        ("", "unavailable", None, None, "$m", ""),
    ),
)
def test_typed_guidance_display_is_compact_unit_aware_and_bounded(
    raw_display: str,
    numeric_state: str,
    low: float | None,
    high: float | None,
    unit: str,
    expected: str,
) -> None:
    assert format_typed_guidance_display(
        raw_display=raw_display,
        numeric_state=numeric_state,
        low=low,
        high=high,
        unit=unit,
    ) == expected
