from __future__ import annotations

import json
from pathlib import Path

import pytest
from openpyxl import load_workbook

from scripts.build_anf_shadow_normalized_package import build_anf_normalized_package


ROOT = Path(__file__).resolve().parents[1]


def _data_root() -> Path:
    for parent in (ROOT, *ROOT.parents):
        candidate = parent / "StockModelData"
        if candidate.exists():
            return candidate
    return ROOT.parent / "StockModelData"


DATA_ROOT = _data_root()
ANF_WORKBOOK = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
ANF_PACKAGE = DATA_ROOT / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_normalized_data_package.json"
BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"


def _bindings() -> dict[str, dict]:
    return {
        binding["binding_id"]: binding
        for binding in json.loads(BINDING_MAP.read_text(encoding="utf-8"))["bindings"]
    }


def test_anf_legacy_oracle_confirms_the_six_business_key_contracts_read_only() -> None:
    """ANF is a migration oracle, not input logic for generic onboarding."""

    assert ANF_WORKBOOK.exists()
    package = build_anf_normalized_package(data_root=DATA_ROOT, workbook_path=ANF_WORKBOOK)
    bindings = _bindings()

    wb = load_workbook(ANF_WORKBOOK, read_only=True, data_only=False)
    try:
        summary = wb["SUMMARY"]
        valuation = wb["Valuation"]
        segments = wb["BS_Segments"]
        drivers = wb["Operating_Drivers"]
        notes = wb["Quarter_Notes_UI"]
        promise = wb["Promise_Progress_UI"]
        investment_case = wb["ANF_Investment_Case"]

        # 1. Quarterly financials: ANF's original labels prove B26 is an as-of
        # value, while B28/B30 and Valuation's period/value rows are distinct.
        assert summary["A26"].value == "As of quarter"
        assert summary["A28"].value == "Revenue (latest quarter)"
        assert summary["A30"].value == "Net income (latest quarter)"
        assert valuation["A6"].value == "Quarter"
        assert valuation["A9"].value == "Revenue"
        assert valuation["A24"].value == "Adj EBITDA"
        assert valuation["A36"].value == "Net income attrib. to A&F"
        assert valuation["A43"].value == "CFO"
        for offset, row in enumerate(package["quarterly_financials"]["rows"], start=2):
            assert row["revenue"]["value"] == valuation.cell(9, offset).value
            assert row["adjusted_ebitda"]["value"] == valuation.cell(24, offset).value
            assert row["net_income"]["value"] == valuation.cell(36, offset).value
            assert row["operating_cash_flow"]["value"] == valuation.cell(43, offset).value
            assert "ANF_model.xlsx!History_Q" in row["revenue"]["source_ref"]
        assert package["quarterly_financials"]["rows"][8]["base_ebitda"]["value"] == valuation["J18"].value
        assert package["quarterly_financials"]["rows"][8]["adjusted_ebitda"]["value"] == valuation["J24"].value
        annuals = {row["period"]: row for row in package["annual_financials"]["rows"]}
        assert annuals["2025-FY"]["revenue"]["value"] == 5266.292
        assert annuals["2025-FY"]["adjusted_ebitda"]["value"] == 815.59
        assert annuals["2025-FY"]["net_income"]["value"] == 506.921
        assert annuals["2025-FY"]["free_cash_flow"]["value"] == 378.368
        assert annuals["2025-FY"]["operating_cash_flow"]["value"] == 619.142
        assert bindings["summary_quarterly_revenue"]["source_field"] == "period"
        assert bindings["summary_latest_revenue"]["source_field"] == "revenue"
        assert bindings["summary_latest_net_income"]["source_field"] == "net_income"
        assert bindings["valuation_period_headers"]["source_field"] == "period"

        # 2. Guidance: the legacy UI supplies the column semantics. Lineage
        # remains on planned writes; T9 is a non-anchor inside S9:Z9.
        assert (valuation["O8"].value, valuation["R8"].value, valuation["S8"].value) == (
            "Metric",
            "Applies to",
            "Guidance",
        )
        current_guidance = sorted(
            (item for item in package["normalized_guidance"]["items"] if item.get("display_role") == "current_primary"),
            key=lambda item: item["display_priority"],
        )
        assert len(current_guidance) == 7
        guidance = current_guidance[0]
        assert guidance["metric"]["source_ref"] == guidance["value"]["source_ref"]
        assert guidance["publication_date"] == "2026-03-04"
        assert guidance["horizon"]["value"] in {"2026 year", "2026-Q1"}
        assert [(item["metric"]["value"], item["horizon"]["value"]) for item in current_guidance] == [
            ("Revenue", "2026 year"),
            ("Revenue", "2026-Q1"),
            ("Operating margin", "2026 year"),
            ("Operating margin", "2026-Q1"),
            ("Adj EPS", "2026 year"),
            ("Adj EPS", "2026-Q1"),
            ("Real estate activity", "2026 year"),
        ]
        guidance_sources = {column["source_field"] for column in bindings["valuation_guidance_rows"]["target_columns"]}
        assert {"metric", "stated_in_period", "horizon", "value"} == guidance_sources
        assert "T" not in {column["target_column"] for column in bindings["valuation_guidance_rows"]["target_columns"]}

        # 3. Segment rows: the generic contract uses neutral dimension slots.
        assert segments["A7"].value == "Quarter"
        assert segments["A50"].value == "ANF retail BS drivers"
        assert package["segments"]["items"]
        assert bindings["bs_segment_quarterly_rows"]["planning_state"] == "active"
        assert bindings["bs_segment_quarterly_rows"]["planning_mode"] == "pivot_rows"
        assert bindings["bs_segment_quarterly_rows"]["planner_target"] == "A61:I67"
        assert bindings["bs_segment_quarterly_rows"]["row_key"] == ["period", "dimension", "member", "metric"]
        hollister = next(item for item in package["segments"]["items"] if item["member"] == "Hollister" and item["metric"] == "revenue" and item["period"] == "2025-Q4")
        assert hollister["revenue"]["value"] == 863.3
        americas = next(item for item in package["segments"]["items"] if item["member"] == "Americas" and item["metric"] == "revenue" and item["period"] == "2025-FY")
        assert americas["annual_revenue"]["value"] == pytest.approx(4290.4, abs=0.01)

        # 4. Operating drivers use the two independently addressable merge anchors.
        assert drivers["A2"].value == "Operating Drivers"
        assert drivers["A5"].value == "Watch item"
        assert package["operating_drivers"]["items"]
        assert bindings["od_watchlist_rows"]["planning_state"] == "active"
        assert bindings["od_watchlist_rows"]["planner_target"] == "A6:N9"
        assert {column["target_column"] for column in bindings["od_watchlist_rows"]["target_columns"]} == {"A", "B", "H"}
        current_drivers = sorted((item for item in package["operating_drivers"]["items"] if item["display_role"] == "current_watchlist"), key=lambda item: item["display_priority"])
        assert len(current_drivers) == 4
        for row_number, item in enumerate(current_drivers, start=6):
            assert item["topic"]["value"] == drivers.cell(row_number, 1).value
            assert item["current_read"]["value"] == drivers.cell(row_number, 2).value
            assert item["why_it_matters"]["value"] == drivers.cell(row_number, 8).value

        # 5. Quarter notes: the six source-backed fields have distinct legacy UI
        # headers, so concatenating them into one merged cell is prohibited.
        assert notes["C9"].value == "What happened"
        assert notes["H9"].value == "Model / valuation implication"
        assert notes["M9"].value == "Source / confidence"
        assert package["quarter_notes"]["items"]
        assert bindings["qn_quarter_note_rows"]["planning_state"] == "active"
        assert bindings["qn_quarter_note_rows"]["planner_target"] == "A10:M15"
        assert {column["target_column"] for column in bindings["qn_quarter_note_rows"]["target_columns"]} == {"A", "C", "H", "M"}
        current_notes = sorted((item for item in package["quarter_notes"]["items"] if item["display_role"] == "current_note"), key=lambda item: item["display_priority"])
        assert len(current_notes) == 6
        for row_number, item in enumerate(current_notes, start=10):
            assert item["theme"]["value"] == notes.cell(row_number, 1).value
            assert item["commentary"]["value"] == notes.cell(row_number, 3).value
            if row_number < 14:
                assert item["model_implication"]["value"] == notes.cell(row_number, 8).value
            else:
                assert item["model_implication"]["source_ref"].startswith("ANF_model.xlsx!")

        # 6. Investment case: the generic tokenized target is a scalar surface;
        # ANF's text remains read-only migration evidence, never template text.
        assert investment_case["A4"].value == "Investment Snapshot"
        assert investment_case["B5"].value
        assert package["investment_case"]["key_debate"]["value"] == investment_case["B7"].value
        assert package["valuation_inputs"]["net_debt"]["value"] == valuation["D198"].value
        assert package["valuation_inputs"]["base_ebitda_ttm"]["value"] == valuation["D199"].value
        assert package["valuation_inputs"]["adjusted_ebitda_ttm"]["value"] == valuation["D200"].value
        assert package["valuation_inputs"]["revenue_ttm"]["value"] == valuation["D203"].value
        assert bindings["ic_investment_summary"]["planner_target"] == "B5"
        assert package["investment_case"]["summary"]["source_ref"].startswith("ANF_model.xlsx!")

        # The legacy annual progression block remains inactive. The generic
        # executable contract uses the neutral primary revision table.
        assert [promise.cell(12, column).value for column in range(1, 10)] == [
            "Metric",
            "Initial guide",
            "Q1 update",
            "Q2 update",
            "Q3 update",
            "Q4 update",
            "Actual",
            "Status",
            "Notes/source",
        ]
        assert bindings["pp_annual_guidance_rows"]["planning_state"] == "inactive_legacy_contract"
        assert bindings["pp_guidance_timeline_rows"]["planning_state"] == "active"
        assert bindings["pp_guidance_timeline_rows"]["planner_target"] == "A61:K67"
        assert bindings["summary_liquidity"]["planner_target"] == "B45"
        assert bindings["summary_net_debt"]["planning_state"] == "inactive_legacy_contract"
    finally:
        wb.close()
