from __future__ import annotations

import json
from html import unescape
import re
from collections import Counter
from pathlib import Path

import pytest
from openpyxl import load_workbook

from pbi_xbrl.new_ticker_binding_planner import plan_standard_template_writes
from pbi_xbrl.standard_template_shell_identity import verify_shell_identity
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
MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
ANF_2019_RELEASE = DATA_ROOT / "tickers" / "ANF" / "earnings_release" / "8-K_2019-03-07_earnings_release.htm"


def _bindings() -> dict[str, dict]:
    return {
        binding["binding_id"]: binding
        for binding in json.loads(BINDING_MAP.read_text(encoding="utf-8"))["bindings"]
    }


def test_fy2018_cash_and_inventory_match_the_source_release_scale() -> None:
    assert ANF_2019_RELEASE.exists()
    source_html = ANF_2019_RELEASE.read_text(encoding="utf-8", errors="ignore")
    source_text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(source_html)))
    assert re.search(
        r"Consolidated Balance Sheets \(in thousands\).*?Cash and equivalents \$ 723,135.*?Inventories 437,879",
        source_text,
    )

    wb = load_workbook(ANF_WORKBOOK, read_only=True, data_only=True)
    try:
        history = wb["History_Q"]
        headers = {str(history.cell(1, column).value or ""): column for column in range(1, history.max_column + 1)}
        row_number = next(
            row
            for row in range(2, history.max_row + 1)
            if str(history.cell(row, headers["fiscal_label"]).value or "") == "2018-Q4"
        )
        assert history.cell(row_number, headers["cash"]).value == 723_135_000_000
        assert history.cell(row_number, headers["inventory"]).value == 437_879_000_000
    finally:
        wb.close()

    package = build_anf_normalized_package(data_root=DATA_ROOT, workbook_path=ANF_WORKBOOK)
    annual = next(row for row in package["annual_financials"]["rows"] if row["period"] == "2018-FY")

    assert annual["cash"]["value"] == 723.135
    assert annual["inventory"]["value"] == 437.879
    assert "8-K_2019-03-07_earnings_release.htm" in annual["cash"]["source_ref"]
    assert "8-K_2019-03-07_earnings_release.htm" in annual["inventory"]["source_ref"]
    assert annual["cash"]["value"] + annual["inventory"]["value"] < annual["current_assets"]["value"]
    assert annual["current_assets"]["value"] < annual["total_assets"]["value"]


def test_anf_legacy_oracle_confirms_the_six_business_key_contracts_read_only() -> None:
    """ANF is a migration oracle, not input logic for generic onboarding."""

    assert ANF_WORKBOOK.exists()
    package = build_anf_normalized_package(data_root=DATA_ROOT, workbook_path=ANF_WORKBOOK)
    binding_payload = json.loads(BINDING_MAP.read_text(encoding="utf-8"))
    bindings = _bindings()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    identity = verify_shell_identity(SHELL, manifest=manifest, binding_payload=binding_payload)
    plan = plan_standard_template_writes(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        shell_identity_report=identity,
    )
    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    writes = {(write.target_sheet, write.target_cell): write for write in plan.planned_writes}

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
        assert annuals["2018-FY"]["cash"]["value"] == 723.135
        assert annuals["2018-FY"]["inventory"]["value"] == 437.879
        assert writes[("BS_Segments", "B102")].value == 723.135
        assert bindings["summary_as_of_quarter"]["source_field"] == "period"
        assert bindings["summary_latest_revenue"]["source_field"] == "revenue"
        assert bindings["summary_latest_net_income"]["source_field"] == "net_income"
        assert bindings["valuation_period_headers"]["source_field"] == "period"
        assert writes[("SUMMARY", "A3")].value == package["company_profile"]["business_description"]["value"]
        assert writes[("SUMMARY", "A5")].value == package["company_profile"]["strategic_context"]["value"]
        assert writes[("SUMMARY", "A3")].source_ref in package["company_profile"]["business_description"]["evidence_refs"]
        assert writes[("SUMMARY", "A5")].source_ref in package["company_profile"]["strategic_context"]["evidence_refs"]
        assert writes[("SUMMARY", "A9")].value == "Americas"
        assert writes[("SUMMARY", "B9")].value == 81.5

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
        assert guidance["source_date"] == "2026-01-31"
        assert guidance["stated_in_period"] == "2025-Q4"
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
        assert writes[("Valuation", "O9")].value == "Revenue"
        assert writes[("Valuation", "Q9")].value == "2025-Q4"
        assert writes[("Valuation", "R9")].value == "2026 year"
        assert writes[("Promise_Progress_UI", "I61")].value == "2025-Q4"
        assert writes[("Promise_Progress_UI", "J61")].value == "2026-03-04"
        current_secondary = {
            (item["metric"]["value"], item["horizon"]["value"])
            for item in package["normalized_guidance"]["items"]
            if item.get("display_role") == "current_secondary"
        }
        assert {
            ("Capex", "2026 year"),
            ("Diluted shares", "2026 year"),
            ("Diluted shares", "2026-Q1"),
            ("Share repurchases", "2026 year"),
            ("Share repurchases", "2026-Q1"),
        } <= current_secondary

        # 3. Segment rows: the generic contract uses neutral dimension slots.
        assert segments["A7"].value == "Quarter"
        assert segments["A50"].value == "ANF retail BS drivers"
        assert package["segments"]["items"]
        assert bindings["bs_segment_quarterly_rows"]["planning_state"] == "active"
        assert bindings["bs_segment_quarterly_rows"]["planning_mode"] == "pivot_rows"
        assert bindings["bs_segment_quarterly_rows"]["planner_target"] == "A61:M67"
        assert bindings["bs_segment_quarterly_rows"]["row_key"] == ["period", "dimension", "member", "metric"]
        hollister = next(item for item in package["segments"]["items"] if item["member"] == "Hollister" and item["metric"] == "revenue" and item["period"] == "2025-Q4")
        assert hollister["revenue"]["value"] == 863.3
        americas = next(item for item in package["segments"]["items"] if item["member"] == "Americas" and item["metric"] == "revenue" and item["period"] == "2025-FY")
        assert americas["annual_revenue"]["value"] == pytest.approx(4290.4, abs=0.01)
        assert [writes[("BS_Segments", f"{column}7")].value for column in "BCDEFGHIJKLM"] == [
            "2023-Q2",
            "2023-Q3",
            "2023-Q4",
            "2024-Q1",
            "2024-Q2",
            "2024-Q3",
            "2024-Q4",
            "2025-Q1",
            "2025-Q2",
            "2025-Q3",
            "2025-Q4",
            "2026-Q1",
        ]
        assert writes[("BS_Segments", "L66")].value == pytest.approx(863.3)
        assert writes[("BS_Segments", "L66")].row_key == "2025-Q4|brand|Hollister|revenue"
        assert writes[("BS_Segments", "L61")].value == pytest.approx(segments["H61"].value)
        assert [writes[("BS_Segments", f"{column}70")].value for column in "BCDEFGHI"] == [
            "2018-FY",
            "2019-FY",
            "2020-FY",
            "2021-FY",
            "2022-FY",
            "2023-FY",
            "2024-FY",
            "2025-FY",
        ]
        assert writes[("BS_Segments", "G72")].value == pytest.approx(segments["B72"].value)
        assert writes[("BS_Segments", "I72")].value == pytest.approx(segments["D72"].value)

        # 4. Operating drivers preserve the generic three-column contract while
        # the ANF adapter supplies clean source-backed monitoring themes.
        assert drivers["A2"].value == "Operating Drivers"
        assert drivers["A5"].value == "Watch item"
        assert package["operating_drivers"]["items"]
        assert bindings["od_watchlist_rows"]["planning_state"] == "active"
        assert bindings["od_watchlist_rows"]["planner_target"] == "A6:N9"
        assert {column["target_column"] for column in bindings["od_watchlist_rows"]["target_columns"]} == {"A", "B", "H"}
        current_drivers = sorted((item for item in package["operating_drivers"]["items"] if item["display_role"] == "current_watchlist"), key=lambda item: item["display_priority"])
        assert len(current_drivers) == 4
        assert [item["topic"]["value"] for item in current_drivers] == [
            "Sales execution",
            "Margin durability",
            "Inventory quality",
            "Capital returns",
        ]
        for row_number, item in enumerate(current_drivers, start=6):
            assert writes[("Operating_Drivers", f"A{row_number}")].value == item["topic"]["value"]
            assert writes[("Operating_Drivers", f"B{row_number}")].value == item["current_read"]["value"]
            assert writes[("Operating_Drivers", f"H{row_number}")].value == item["why_it_matters"]["value"]
            assert item["current_read"]["source_ref"]
            assert item["current_read"]["source_ref"] == item["why_it_matters"]["source_ref"]

        # 5. Quarter notes retain legacy column intent without copying noisy
        # legacy UI prose or substituting Operating Drivers lineage.
        assert notes["C9"].value == "What happened"
        assert notes["H9"].value == "Model / valuation implication"
        assert notes["M9"].value == "Source / confidence"
        assert package["quarter_notes"]["items"]
        assert bindings["qn_quarter_note_rows"]["planning_state"] == "active"
        assert bindings["qn_quarter_note_rows"]["planner_target"] == "A10:M15"
        assert {column["target_column"] for column in bindings["qn_quarter_note_rows"]["target_columns"]} == {"A", "C", "F", "H", "M"}
        current_notes = sorted((item for item in package["quarter_notes"]["items"] if item["display_role"] == "current_note"), key=lambda item: item["display_priority"])
        assert len(current_notes) == 6
        assert [item["theme"]["value"] for item in current_notes] == [
            "Q4 results",
            "Brand mix",
            "Inventory",
            "2026 margin bridge",
            "Capital allocation",
            "Growth channels",
        ]
        for row_number, item in enumerate(current_notes, start=10):
            assert writes[("Quarter_Notes_UI", f"A{row_number}")].value == item["theme"]["value"]
            assert writes[("Quarter_Notes_UI", f"C{row_number}")].value == item["commentary"]["value"]
            assert writes[("Quarter_Notes_UI", f"F{row_number}")].value == item["why_it_matters"]["value"]
            assert writes[("Quarter_Notes_UI", f"H{row_number}")].value == item["model_implication"]["value"]
            assert writes[("Quarter_Notes_UI", f"M{row_number}")].value == item["source_display"]["value"]
            assert item["commentary"]["source_ref"] in item["evidence_refs"]
            visible_text = " ".join(str(item[field]["value"]) for field in ("commentary", "why_it_matters", "model_implication"))
            assert "Operating_Drivers" not in visible_text
            assert "binding" not in visible_text.casefold()
            assert "parser" not in visible_text.casefold()

        # 6. Investment case: the generic tokenized target is a scalar surface;
        # ANF's text remains read-only migration evidence, never template text.
        assert investment_case["A4"].value == "Investment Snapshot"
        assert investment_case["B5"].value
        assert package["investment_case"]["key_debate"]["evidence_classification"] == "analyst_interpretation_requiring_review"
        for cell, field in {
            "B5": "summary",
            "B6": "why_it_can_work",
            "B7": "key_debate",
            "B8": "upside_factors",
            "B9": "downside_factors",
            "B10": "watch_next",
            "B11": "current_stance",
        }.items():
            assert writes[("ANF_Investment_Case", cell)].value == package["investment_case"][field]["value"]
            assert package["investment_case"][field]["source_ref"]
        assert valuation["D198"].value is not None  # legacy display is an oracle, not source lineage
        assert package["valuation_inputs"]["net_debt"]["status"] == "missing_source"
        assert package["valuation_inputs"]["net_debt"]["value"] is None
        assert "D198 was not treated as evidence" in package["valuation_inputs"]["net_debt"]["reason"]
        assert package["valuation_inputs"]["base_ebitda_ttm"]["value"] == valuation["D199"].value
        assert package["valuation_inputs"]["adjusted_ebitda_ttm"]["value"] == valuation["D200"].value
        assert package["valuation_inputs"]["revenue_ttm"]["value"] == valuation["D203"].value
        assert package["debt_liquidity"]["total_debt"]["status"] == "missing_source"
        assert package["debt_liquidity"]["total_debt"]["value"] is None
        assert package["debt_liquidity"]["net_leverage"]["status"] == "missing_source"
        assert package["debt_liquidity"]["total_liquidity"]["value"] == pytest.approx(1209.086)
        assert package["debt_liquidity"]["as_of_date"]["value"] == "2026-01-31"
        assert package["debt_liquidity"]["liquidity_freshness"]["disposition"] == "stale_but_displayable_with_date"
        assert writes[("SUMMARY", "B45")].value == pytest.approx(1209.086)
        assert writes[("SUMMARY", "B45")].normalized_path == "debt_liquidity.summary_liquidity_display"
        assert writes[("SUMMARY", "D45")].value == "As of 2026-01-31 (stale)"
        assert bindings["ic_investment_summary"]["planner_target"] == "B5"
        investment_summary = package["investment_case"]["summary"]
        assert investment_summary["source_ref"] in investment_summary["evidence_refs"]
        assert investment_summary["source_ref"].startswith("tickers/ANF/")
        assert investment_summary["evidence_classification"] == "evidence_backed_synthesis"

        # The old broad progression contract remains inactive. Typed current,
        # secondary, and historical rowsets own distinct visible blocks.
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
        assert bindings["pp_progress_fy2025_rows"]["planning_state"] == "active"
        assert bindings["pp_progress_fy2024_rows"]["planning_state"] == "active"
        assert bindings["pp_current_secondary_guidance_rows"]["planning_state"] == "active"
        assert bindings["pp_guidance_timeline_rows"]["planning_state"] == "active"
        assert bindings["pp_guidance_timeline_rows"]["planner_target"] == "A61:K67"
        assert writes[("Promise_Progress_UI", "A13")].value == "FY2025 Revenue"
        assert writes[("Promise_Progress_UI", "A24")].value == "FY2024 Revenue"
        assert writes[("Promise_Progress_UI", "A39")].value == "Capex"
        assert writes[("Promise_Progress_UI", "C39")].value == "2026 year"
        assert bindings["summary_liquidity"]["planner_target"] == "B45"
        assert bindings["summary_net_leverage"]["planner_target"] == "B41"
        assert bindings["summary_net_leverage"]["normalized_field"] == "debt_liquidity.net_leverage"
        assert ("SUMMARY", "B41") not in writes
        leverage_gap = next(gap for gap in plan.mapping_gaps if gap.get("binding_id") == "summary_net_leverage")
        leverage_issue = next(issue for issue in plan.planner_issues if issue.binding_id == "summary_net_leverage")
        assert leverage_gap["source_ref"] == "ANF_model.xlsx!History_Q!row:50"
        assert leverage_issue.source_ref == leverage_gap["source_ref"]
    finally:
        wb.close()


def test_anf_planner_preserves_business_semantics_and_reconciles_final_qa_snapshot() -> None:
    assert ANF_WORKBOOK.exists()
    package = build_anf_normalized_package(data_root=DATA_ROOT, workbook_path=ANF_WORKBOOK)
    binding_payload = json.loads(BINDING_MAP.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    identity = verify_shell_identity(SHELL, manifest=manifest, binding_payload=binding_payload)

    plan = plan_standard_template_writes(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        shell_identity_report=identity,
    )
    business_writes = [write for write in plan.planned_writes if write.target_sheet not in {"QA_Log", "Needs_Review", "QA_Checks"}]
    summary = plan.issue_ledger["summary"]
    qa_writes = Counter(write.target_sheet for write in plan.planned_writes if write.target_sheet in {"QA_Log", "Needs_Review", "QA_Checks"})
    qa_bindings = {
        binding["sheet"]: binding
        for binding in binding_payload["bindings"]
        if binding.get("source_policy") == "validation-output" and binding.get("planning_state") == "active"
    }

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    assert plan.qa_snapshot_status == "stable"
    assert not plan.has_blockers
    assert len({(write.target_sheet, write.target_cell) for write in business_writes}) == len(business_writes)
    business_sheets = {write.target_sheet for write in business_writes}
    assert business_sheets == {
        "SUMMARY",
        "Valuation",
        "BS_Segments",
        "Operating_Drivers",
        "ANF_Investment_Case",
        "Quarter_Notes_UI",
            "Promise_Progress_UI",
            "History_Q",
        }
    by_binding = Counter(write.binding_id for write in business_writes)
    assert by_binding["valuation_period_headers"] == 12
    assert by_binding["valuation_revenue_series"] == 12
    assert by_binding["valuation_net_income_series"] == 12
    assert by_binding["valuation_operating_cash_flow_series"] == 12
    assert by_binding["bs_annual_financial_period_headers"] == 8
    assert by_binding["bs_annual_revenue_series"] == 8
    assert summary["detailed_occurrence_count"] == len(plan.issue_ledger["occurrences"])
    assert summary["detailed_occurrence_count"] == len(plan.manual_review_flags) + len(plan.mapping_gaps) + len(plan.issues)
    assert summary["detailed_occurrence_count"] == sum(issue["occurrence_count"] for issue in plan.issue_ledger["issues"])
    assert summary["canonical_unique_issue_count"] == len(plan.issue_ledger["issues"])
    assert summary["actionable_issue_count"] == len(plan.issue_ledger["qa_presentation"]["needs_review_rows"])
    presentation_rows = {
        "QA_Log": plan.issue_ledger["qa_presentation"]["qa_log_rows"],
        "Needs_Review": plan.issue_ledger["qa_presentation"]["needs_review_rows"],
        "QA_Checks": plan.issue_ledger["qa_presentation"]["qa_check_rows"],
    }
    for sheet_name, rows in presentation_rows.items():
        mapped_fields = [column["source_field"] for column in qa_bindings[sheet_name]["target_columns"]]
        expected_cells = sum(
            1
            for row in rows
            for field in mapped_fields
            if row.get(field) not in (None, "")
        )
        assert qa_writes[sheet_name] == expected_cells
