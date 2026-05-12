import datetime as dt

import pandas as pd
import pytest

from pbi_xbrl.pipeline_orchestration import (
    _apply_anf_company_overview_overrides,
    _parse_anf_adjusted_metrics_from_lines,
    _parse_anf_guidance_rows_from_lines,
    _parse_anf_statement_values_from_lines,
)


Q4_2025_LINES = [
    "Thirteen Weeks Ended Thirteen Weeks Ended",
    "January 31, 2026 Sales February 1, 2025 Sales",
    "Net sales $ 1,669,802 100.0 % $ 1,584,917 100.0 %",
    "Cost of sales, exclusive of depreciation and amortization 676,491 40.5 % 610,907 38.5 %",
    "Operating income 235,931 14.1 % 256,064 16.2 %",
    "Net income attributable to A&F $ 172,130 10.3 % $ 187,226 11.8 %",
    "Net income per share attributable to A&F",
    "Diluted $ 3.68 $ 3.57",
    "Weighted-average shares outstanding:",
    "Diluted 46,837 52,461",
    "Schedule of Non-GAAP Financial Measures",
    "Fifty-Two Weeks Ended January 31, 2026",
    "GAAP (1) Net Sales Excluded item (2) non-GAAP Net Sales",
    "Litigation settlement $ (38,574) $ (38,574) $ —",
    "Operating income 699,143 13.3 % 38,574 660,569 12.5 %",
    "Net income attributable to A&F 506,921 9.6 % 28,882 478,039 9.1 %",
    "Net income per diluted share attributable to A&F $ 10.46 $ 0.60 $ 9.86",
    "Diluted weighted-average shares outstanding 48,476 48,476",
    "Reconciliation of EBITDA and Adjusted EBITDA",
    "Thirteen Weeks Ended January 31, 2026 and Thirteen Weeks Ended February 1, 2025",
    "EBITDA (1) $ 276,386 16.6 $ 293,227 18.5",
    "Reconciliation of EBITDA and Adjusted EBITDA",
    "Fifty-Two Weeks Ended January 31, 2026 and Fifty-Two Weeks Ended February 1, 2025",
    "EBITDA (1) $ 854,164 16.2 $ 894,593 18.1",
    "Adjustments to EBITDA",
    "Litigation settlement (38,574) (0.7) — —",
    "Adjusted EBITDA (1) $ 815,590 15.5 $ 894,593 18.1",
]


def test_anf_statement_parser_captures_q4_shares_eps_and_ebitda() -> None:
    values = _parse_anf_statement_values_from_lines(Q4_2025_LINES, scale=1000.0)

    assert values["revenue"] == pytest.approx(1_669_802_000.0)
    assert values["gross_profit"] == pytest.approx(993_311_000.0)
    assert values["op_income"] == pytest.approx(235_931_000.0)
    assert values["net_income"] == pytest.approx(172_130_000.0)
    assert values["eps_diluted"] == pytest.approx(3.68)
    assert values["shares_diluted"] == pytest.approx(46_837_000.0)
    assert values["ebitda"] == pytest.approx(276_386_000.0)


def test_anf_adjusted_parser_keeps_quarter_and_annual_metrics_separate() -> None:
    rows = _parse_anf_adjusted_metrics_from_lines(
        Q4_2025_LINES,
        quarter_end=dt.date(2026, 1, 31),
        scale=1000.0,
        source_doc="ANF_Q4_2025_earnings_presentation_financial_schedules.pdf",
        source="slides",
    )

    by_period = {row["period_type"]: row for row in rows}
    assert by_period["quarter"]["adj_ebitda"] == pytest.approx(276_386_000.0)
    assert by_period["annual"]["adj_ebitda"] == pytest.approx(815_590_000.0)
    assert by_period["annual"]["adj_ebit"] == pytest.approx(660_569_000.0)
    assert by_period["annual"]["adj_eps"] == pytest.approx(9.86)
    assert "favorable settlement" in by_period["annual"]["source_snippet"].lower()


def test_anf_guidance_parser_captures_q1_and_fy2026_outlook() -> None:
    lines = [
        "Fiscal 2026 First Quarter and Full Year Outlook",
        "For fiscal 2026, the company expects:",
        "First Quarter Outlook (1) Full Year Outlook (1)",
        "Net sales Growth In The Range of 1% to 3% Growth In The Range of 3% to 5%",
        "Operating margin Around 7.0% In the Range of 12.0% to 12.5%",
        "Net income per diluted share (3) (4) In The Range of $1.20 to $1.30 In The Range of $10.20 to $11.00",
        "Share repurchases (4) At least $100 million Around $450 million",
        "Diluted weighted average shares (3) (4) Around 46 million Around 45 million",
        "Capital expenditures In The Range of $200 to $225 million",
        "Real estate activity (5)(all approximate) ~30 Net Store Openings",
        "Real estate activity (5)(all approximate) 55 Openings, 25 Closures",
        "Real estate activity (5)(all approximate) 70 Remodels and Right-Sizes",
        "the outlook assumes a year-over-year tariff impact as a percentage of net sales of approximately 290 basis points for the first quarter and 70 basis points for the full year.",
    ]

    rows = _parse_anf_guidance_rows_from_lines(lines, dt.date(2026, 1, 31))
    labels = {(row["period_label"], row["metric_hint"]): row for row in rows}

    assert labels[("Q1 FY2026", "Revenue")]["numbers"] == "1%, 3%"
    assert labels[("FY2026", "Revenue")]["numbers"] == "3%, 5%"
    assert labels[("FY2026", "Capex")]["numbers"] == "$200 million, $225 million"
    assert labels[("Q1 FY2026", "Share repurchases")]["numbers"] == "at least $100 million"
    assert labels[("FY2026", "Real estate activity")]["numbers"] == "55 openings, 25 closures; 70 remodels/right-sizes"
    assert labels[("Q1 FY2026", "Tariffs")]["numbers"] == "290 bps"


def test_anf_summary_override_replaces_noisy_overview_and_uses_apac_mix() -> None:
    overview = {
        "what_it_does": "It operates through Corporate / Other.",
        "key_advantage": "Form of Retention Restricted Stock Unit Award Agreement under the plan.",
        "current_strategic_context": "Code of Business Conduct and corporate governance.",
        "revenue_streams": [{"name": "Americas", "pct": 0.92}, {"name": "EMEA", "pct": 0.08}],
    }
    segments = pd.DataFrame(
        [
            {"quarter": "2026-01-31", "segment": "Americas", "metric": "revenue", "value": 4_290_395_000.0, "period_type": "annual"},
            {"quarter": "2026-01-31", "segment": "EMEA", "metric": "revenue", "value": 818_140_000.0, "period_type": "annual"},
            {"quarter": "2026-01-31", "segment": "APAC", "metric": "revenue", "value": 157_757_000.0, "period_type": "annual"},
        ]
    )

    cleaned = _apply_anf_company_overview_overrides(overview, slides_segments=segments)

    assert "Corporate / Other" not in cleaned["what_it_does"]
    assert "restricted stock unit" not in cleaned["key_advantage"].lower()
    assert "Code of Business Conduct" not in cleaned["current_strategic_context"]
    stream_names = {row["name"] for row in cleaned["revenue_streams"]}
    assert stream_names == {"Americas", "EMEA", "APAC"}
    apac = next(row for row in cleaned["revenue_streams"] if row["name"] == "APAC")
    assert apac["pct"] == pytest.approx(0.03, abs=0.001)
