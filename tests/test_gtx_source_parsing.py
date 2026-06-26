from __future__ import annotations

import datetime as dt

import pandas as pd

import pbi_xbrl.debt_parser as debt_parser
from pbi_xbrl.company_profiles import get_company_profile
from pbi_xbrl.excel_writer_latest_quarter_qa import _is_quarter_financial_qa_source_candidate
from pbi_xbrl.metrics import get_income_statement_rules
from pbi_xbrl.non_gaap import parse_adjusted_from_ex99
from pbi_xbrl.pipeline import (
    _extract_balance_sheet_from_text,
    _extract_eps_shares_from_html,
    _extract_income_statement_from_html,
    _infer_tranche_meta,
    build_debt_profile,
)


Q1_2026_NON_GAAP_HTML = b"""
<html><body><table>
  <tr>
    <td>$ millions (unless otherwise noted)</td>
    <td>Q1 2026</td>
    <td>Q1 2025</td>
  </tr>
  <tr><td>Adjusted EBIT*</td><td>151</td><td>131</td></tr>
  <tr><td>Adjusted EBIT margin*</td><td>15.3%</td><td>14.9%</td></tr>
  <tr><td>Adjusted EBITDA*</td><td>183</td><td>159</td></tr>
  <tr><td>Adjusted EBITDA margin*</td><td>18.6%</td><td>18.1%</td></tr>
  <tr><td>Adjusted free cash flow*</td><td>49</td><td>36</td></tr>
</table></body></html>
"""


Q4_2025_NON_GAAP_HTML = b"""
<html><body><table>
  <tr>
    <td>$ millions (unless otherwise noted)</td>
    <td>Q4 2025</td>
    <td>Q4 2024</td>
    <td>Full Year 2025</td>
    <td>Full Year 2024</td>
  </tr>
  <tr><td>Adjusted EBIT*</td><td>122</td><td>124</td><td>510</td><td>485</td></tr>
  <tr><td>Adjusted EBIT margin*</td><td>13.7%</td><td>14.7%</td><td>14.2%</td><td>14.0%</td></tr>
  <tr><td>Adjusted EBITDA*</td><td>159</td><td>153</td><td>636</td><td>598</td></tr>
  <tr><td>Adjusted EBITDA margin*</td><td>17.8%</td><td>18.1%</td><td>17.7%</td><td>17.2%</td></tr>
  <tr><td>Adjusted free cash flow*</td><td>139</td><td>157</td><td>403</td><td>358</td></tr>
</table></body></html>
"""


Q1_2026_STATEMENT_HTML = b"""
<html><body>
  <p>Condensed Consolidated Statements of Operations</p>
  <table>
    <tr><td>Three Months Ended March 31,</td><td>2026</td><td>2025</td></tr>
    <tr><td>(Dollars in millions, except per share amounts)</td><td></td><td></td></tr>
    <tr><td>Net sales</td><td>985</td><td>878</td></tr>
    <tr><td>Cost of goods sold</td><td>789</td><td>699</td></tr>
    <tr><td>Gross profit</td><td>196</td><td>179</td></tr>
    <tr><td>Selling, general and administrative expenses</td><td>58</td><td>59</td></tr>
    <tr><td>Other expense, net</td><td>1</td><td>7</td></tr>
    <tr><td>Interest expense</td><td>27</td><td>29</td></tr>
    <tr><td>Non-operating income, net</td><td>(8)</td><td>(1)</td></tr>
    <tr><td>Income before taxes</td><td>118</td><td>85</td></tr>
    <tr><td>Tax expense</td><td>23</td><td>23</td></tr>
    <tr><td>Net income</td><td>95</td><td>62</td></tr>
    <tr><td>Research, development and engineering costs</td><td>37</td><td>40</td></tr>
  </table>
</body></html>
"""


Q1_2026_SPLIT_CURRENCY_STATEMENT_HTML = b"""
<html><body><table>
  <tr><td>Three Months Ended March 31,</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>2026</td><td>2025</td><td></td><td></td><td></td></tr>
  <tr><td>(Dollars in millions, except per share amounts)</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Net sales (Note 3)</td><td>$</td><td>985</td><td>$</td><td>878</td></tr>
  <tr><td>Cost of goods sold</td><td>789</td><td>699</td><td></td><td></td></tr>
  <tr><td>Gross profit</td><td>196</td><td>179</td><td></td><td></td></tr>
  <tr><td>Selling, general and administrative expenses</td><td>58</td><td>59</td><td></td><td></td></tr>
  <tr><td>Other expense, net</td><td>1</td><td>7</td><td></td><td></td></tr>
  <tr><td>Interest expense</td><td>27</td><td>29</td><td></td><td></td></tr>
  <tr><td>Non-operating income, net</td><td>( 8 )</td><td>( 1 )</td><td></td><td></td></tr>
  <tr><td>Income before taxes</td><td>118</td><td>85</td><td></td><td></td></tr>
  <tr><td>Tax expense (Note 5)</td><td>23</td><td>23</td><td></td><td></td></tr>
  <tr><td>Net income</td><td>$</td><td>95</td><td>$</td><td>62</td></tr>
</table></body></html>
"""


Q3_2025_MIXED_3M_9M_STATEMENT_HTML = b"""
<html><body><table>
  <tr><td>Three Months Ended September 30,</td><td>Nine Months Ended September 30,</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
  <tr><td>2025</td><td>2024</td><td>2025</td><td>2024</td><td></td><td></td><td></td><td></td><td></td></tr>
  <tr><td>(Dollars in millions, except per share amounts)</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Net sales (Note 3)</td><td>$</td><td>902</td><td>$</td><td>826</td><td>$</td><td>2,693</td><td>$</td><td>2,631</td></tr>
  <tr><td>Cost of goods sold</td><td>716</td><td>660</td><td>2,147</td><td>2,108</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Gross profit</td><td>186</td><td>166</td><td>546</td><td>523</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Selling, general and administrative expenses</td><td>57</td><td>53</td><td>175</td><td>178</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Other expense, net</td><td>1</td><td>1</td><td>9</td><td>5</td><td></td><td></td><td></td><td></td></tr>
</table></body></html>
"""


Q4_2025_EPS_HTML_WITH_NUMERIC_IDS = b"""
<html><body>
  <p>Three Months Ended December 31, 2025 and 2024</p>
  <p>In thousands</p>
  <p>Weighted-average shares used in diluted earnings per share</p>
  <ix:nonfraction id="ic6679" name="us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding">197,514</ix:nonfraction>
  <ix:nonfraction id="ic2024">193,000</ix:nonfraction>
</body></html>
"""


Q4_2025_EPS_HTML_RELEASE_ROW = b"""
<html><body>
  <p>CONSOLIDATED STATEMENTS OF OPERATIONS</p>
  <p>Three Months Ended December 31, Year Ended December 31, 2025 2024 2025 2024</p>
  <p>(Dollars in millions, except per share amounts)</p>
  <p>Earnings per common share Basic $0.43 $0.47 $1.55 $1.27</p>
  <p>Diluted $0.42 $0.47 $1.52 $1.26</p>
  <p>Weighted average common shares outstanding Basic 192,725,655 211,173,860 199,758,058 222,316,484
  Diluted 197,514,327 212,955,723 203,623,998 224,121,156</p>
</body></html>
"""


Q1_2026_BALANCE_SHEET_HTML = b"""
<html><body><table>
  <tr><td></td><td>March 31, 2026</td><td>December 31, 2025</td><td></td><td></td></tr>
  <tr><td>(Dollars in millions)</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>ASSETS</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Cash and cash equivalents</td><td>$</td><td>142</td><td>$</td><td>177</td></tr>
  <tr><td>Restricted cash</td><td>2</td><td>2</td><td></td><td></td></tr>
  <tr><td>Total assets</td><td>$</td><td>2,373</td><td>$</td><td>2,367</td></tr>
  <tr><td>LIABILITIES</td><td></td><td></td><td></td><td></td></tr>
  <tr><td>Current maturities of long-term debt</td><td>7</td><td>7</td><td></td><td></td></tr>
  <tr><td>Long-term debt</td><td>1,410</td><td>1,411</td><td></td><td></td></tr>
  <tr><td>Total liabilities</td><td>$</td><td>3,154</td><td>$</td><td>3,169</td></tr>
</table></body></html>
"""


Q1_2026_DEBT_HTML = b"""
<html><body>
  <table><tr><td>Other filing disclosure</td></tr></table>
  <p>(Dollars in millions)</p>
  <table>
    <tr>
      <th>Debt instrument</th><th>Due</th><th>Interest Rate</th>
      <th>March 31, 2026</th><th>December 31, 2025</th>
    </tr>
    <tr><td>2025 Dollar Term Facility</td><td>1/30/2032</td><td>SOFR plus 200 bps</td><td>635</td><td>637</td></tr>
    <tr><td>2032 Senior Notes</td><td>5/31/2032</td><td>7.75%</td><td>800</td><td>800</td></tr>
    <tr><td>Other</td><td></td><td></td><td>2</td><td>2</td></tr>
    <tr><td>Total principal outstanding</td><td></td><td></td><td>1,437</td><td>1,439</td></tr>
    <tr><td>Less: unamortized deferred financing costs</td><td></td><td></td><td>(20)</td><td>(21)</td></tr>
    <tr><td>Less: current portion of long-term debt</td><td></td><td></td><td>(7)</td><td>(7)</td></tr>
    <tr><td>Total long-term debt</td><td></td><td></td><td>1,410</td><td>1,411</td></tr>
  </table>
  <table>
    <tr>
      <th>Debt instrument</th>
      <th>March 31, 2026 Carrying Value</th><th>March 31, 2026 Fair Value</th>
      <th>December 31, 2025 Carrying Value</th><th>December 31, 2025 Fair Value</th>
    </tr>
    <tr><td>Term Loan Facilities</td><td>624</td><td>635</td><td>626</td><td>639</td></tr>
    <tr><td>2032 Senior Notes</td><td>791</td><td>828</td><td>790</td><td>850</td></tr>
  </table>
</body></html>
"""


def test_gtx_q1_2026_non_gaap_uses_dollar_rows_not_margin_rows() -> None:
    adj_ebit, adj_ebitda, _adj_eps, adjustments, status, _source = parse_adjusted_from_ex99(
        Q1_2026_NON_GAAP_HTML,
        pd.Timestamp("2026-03-31"),
        mode="relaxed",
    )

    assert status == "ok_relaxed"
    assert adj_ebit == 151_000_000.0
    assert adj_ebitda == 183_000_000.0
    assert adjustments["__adj_fcf"] == 49_000_000.0
    assert adj_ebit not in {15.3, 15_300_000.0}
    assert adj_ebitda not in {18.6, 18_600_000.0}


def test_gtx_q4_2025_non_gaap_prefers_quarter_columns_over_full_year() -> None:
    adj_ebit, adj_ebitda, _adj_eps, adjustments, status, source = parse_adjusted_from_ex99(
        Q4_2025_NON_GAAP_HTML,
        pd.Timestamp("2025-12-31"),
        mode="relaxed",
    )

    assert status == "ok_relaxed"
    assert adj_ebit == 122_000_000.0
    assert adj_ebitda == 159_000_000.0
    assert adjustments["__adj_fcf"] == 139_000_000.0
    assert "Q4 2025" in str(source)
    assert adj_ebit != 510_000_000.0
    assert adj_ebitda != 636_000_000.0
    assert adjustments["__adj_fcf"] != 403_000_000.0


def test_gtx_operating_income_derivation_does_not_double_count_rde() -> None:
    result = _extract_income_statement_from_html(
        Q1_2026_STATEMENT_HTML,
        dt.date(2026, 3, 31),
        rules=get_income_statement_rules("GTX"),
    )

    assert result is not None
    assert result["values"]["revenue"] == 985_000_000.0
    assert result["values"]["cogs"] == 789_000_000.0
    assert result["values"]["gross_profit"] == 196_000_000.0
    assert result["values"]["op_income"] == 137_000_000.0
    assert "research" not in result["labels"]["op_income"].lower()


def test_gtx_income_statement_handles_split_currency_columns() -> None:
    result = _extract_income_statement_from_html(
        Q1_2026_SPLIT_CURRENCY_STATEMENT_HTML,
        dt.date(2026, 3, 31),
        rules=get_income_statement_rules("GTX"),
    )

    assert result is not None
    assert result["values"]["revenue"] == 985_000_000.0
    assert result["values"]["op_income"] == 137_000_000.0


def test_gtx_9m_income_statement_uses_nine_month_columns_not_three_month_columns() -> None:
    result = _extract_income_statement_from_html(
        Q3_2025_MIXED_3M_9M_STATEMENT_HTML,
        dt.date(2025, 9, 30),
        rules=get_income_statement_rules("GTX"),
        period_hint="9M",
    )

    assert result is not None
    assert result["values"]["revenue"] == 2_693_000_000.0
    assert result["values"]["gross_profit"] == 546_000_000.0
    assert result["values"]["op_income"] == 362_000_000.0
    assert result["values"]["op_income"] != 128_000_000.0


def test_gtx_eps_share_extraction_ignores_html_id_numbers() -> None:
    result = _extract_eps_shares_from_html(
        Q4_2025_EPS_HTML_WITH_NUMERIC_IDS,
        dt.date(2025, 12, 31),
    )

    assert result is not None
    assert result["shares_diluted"] == 197_514_000.0
    assert result["shares_diluted"] != 6_679_000.0


def test_gtx_eps_share_extraction_handles_release_outstanding_row_with_fy_columns() -> None:
    result = _extract_eps_shares_from_html(
        Q4_2025_EPS_HTML_RELEASE_ROW,
        dt.date(2025, 12, 31),
    )

    assert result is not None
    assert result["shares_diluted"] == 197_514_327.0
    assert result["shares_diluted"] != 203_623_998.0


def test_gtx_debt_parser_keeps_principal_carrying_value_and_fees_separate() -> None:
    summary = debt_parser.parse_debt_summary_from_primary_doc(
        Q1_2026_DEBT_HTML,
        quarter_end=dt.date(2026, 3, 31),
    )

    by_name = {row["name"]: row for row in summary["tranches"]}
    term_loan = by_name["2025 Dollar Term Facility"]
    senior_notes = by_name["2032 Senior Notes"]

    assert term_loan["principal_amount"] == 635_000_000.0
    assert term_loan["carrying_value"] == 624_000_000.0
    assert senior_notes["coupon"] == 0.0775
    assert senior_notes["maturity_year"] == 2032
    assert senior_notes["principal_amount"] == 800_000_000.0
    assert senior_notes["carrying_value"] == 791_000_000.0
    assert summary["principal_total"] == 1_437_000_000.0
    assert summary["carrying_value_total"] == 1_417_000_000.0
    assert summary["unamortized_deferred_financing_costs"] == 20_000_000.0
    assert summary["current_portion"] == 7_000_000.0
    assert summary["long_term_debt_carrying_value"] == 1_410_000_000.0
    assert summary["period_match"] is True


def test_gtx_term_facility_name_year_is_not_misread_as_maturity_without_due_context() -> None:
    meta = _infer_tranche_meta("2025 Dollar Term Facility", "2025 Dollar Term Facility")

    assert meta["maturity_year"] is None
    assert meta["maturity_display"] in (None, "")


def test_gtx_cash_parser_and_net_debt_use_unrestricted_cash_only() -> None:
    balance_sheet = _extract_balance_sheet_from_text(
        """
        Consolidated Balance Sheet
        March 31, 2026 December 31, 2025
        (Dollars in millions)
        Cash and cash equivalents $ 142 $ 177
        Restricted cash 2 2
        Total assets 2,500 2,400
        Total liabilities 2,000 1,900
        """,
        dt.date(2026, 3, 31),
    )

    assert balance_sheet is not None
    assert balance_sheet["values"]["cash"] == 142_000_000.0
    assert balance_sheet["values"]["restricted_cash"] == 2_000_000.0

    hist = pd.DataFrame(
        [
            {
                "quarter": "2026-03-31",
                "debt_core": 1_417_000_000.0,
                "cash": balance_sheet["values"]["cash"],
                "restricted_cash": balance_sheet["values"]["restricted_cash"],
            }
        ]
    )
    facts = pd.DataFrame(
        columns=["tag", "end_d", "unit", "val", "form", "filed_d", "accn", "start_d"]
    )
    debt_profile, _tranches, _maturity, _qa, _info = build_debt_profile(
        hist,
        facts,
        pd.DataFrame(),
    )
    net_debt = debt_profile.loc[debt_profile["metric"].eq("net_debt_core"), "value"].iloc[0]

    assert net_debt == 1_275_000_000.0
    assert net_debt != 1_273_000_000.0


def test_gtx_balance_sheet_html_keeps_cash_restricted_cash_and_total_debt_separate() -> None:
    from pbi_xbrl.pipeline import _extract_balance_sheet_from_html

    balance_sheet = _extract_balance_sheet_from_html(
        Q1_2026_BALANCE_SHEET_HTML,
        dt.date(2026, 3, 31),
    )

    assert balance_sheet is not None
    assert balance_sheet["values"]["cash"] == 142_000_000.0
    assert balance_sheet["values"]["restricted_cash"] == 2_000_000.0
    assert balance_sheet["values"]["debt_core"] == 1_417_000_000.0


def test_gtx_profile_marks_2023_preferred_conversion_comparability_break() -> None:
    profile = get_company_profile("GTX")

    assert "turbocharging" in profile.industry_keywords
    assert profile.segment_patterns == tuple()
    assert profile.enable_operating_drivers_sheet is True
    assert profile.comparability_breaks

    conversion_break = next(item for item in profile.comparability_breaks if item.period == "2023")
    assert {
        "shares_outstanding",
        "shares_diluted",
        "eps",
        "preferred_dividends",
    }.issubset(set(conversion_break.metrics))
    assert "series a preferred" in conversion_break.note.lower()
    assert "conversion" in conversion_break.note.lower()


def test_gtx_may_2026_debt_event_is_pro_forma_only() -> None:
    event = debt_parser.parse_gtx_debt_event_from_text(
        """
        Garrett Motion announces partial repayment and successful repricing of Term Loan
        May 18, 2026. The Company announced a $50M early repayment of its existing
        $635M term loan due in 2032. Borrowings under the facility will bear interest
        at SOFR”) plus <span>175</span> basis points per annum, a 25-basis point reduction.
        """,
        reported_quarter_end=dt.date(2026, 3, 31),
    )

    assert event["event_date"] == dt.date(2026, 5, 18)
    assert event["reported_quarter_end"] == dt.date(2026, 3, 31)
    assert event["reported_history_debt_adjustment"] == 0.0
    assert event["pro_forma_debt_adjustment"] == -50_000_000.0
    assert event["term_loan_principal_before"] == 635_000_000.0
    assert event["term_loan_principal_after"] == 585_000_000.0
    assert event["new_spread_bps"] == 175
    assert event["spread_reduction_bps"] == 25
    assert event["application"] == "pro_forma_valuation_only"


def test_gtx_latest_quarter_qa_rejects_event_docs_with_submission_only_quarter_match() -> None:
    investor_day_text = (
        "Garrett Motion Technology and Investor Day May 2026. "
        "Adjusted free cash flow yield 4%. Enterprise value to Adjusted EBITDA. "
        "Targeting return of 75% of Adjusted FCF to shareholders over time."
    )
    q1_release_text = (
        "Garrett Motion reports first quarter 2026 results. "
        "Three months ended March 31, 2026 net sales and adjusted free cash flow."
    )

    assert not _is_quarter_financial_qa_source_candidate(
        "earnings_release",
        investor_day_text,
        selection_reason="audit accession doc",
        match_reasons=("submission quarter match",),
    )
    assert not _is_quarter_financial_qa_source_candidate(
        "earnings_release",
        investor_day_text,
        selection_reason="audit accession doc",
        match_reasons=("text quarter match",),
    )
    assert _is_quarter_financial_qa_source_candidate(
        "earnings_release",
        q1_release_text,
        selection_reason="audit accession doc",
        match_reasons=("text quarter match",),
    )
