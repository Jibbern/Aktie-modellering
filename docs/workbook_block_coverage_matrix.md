# Workbook Block Coverage Matrix

This matrix compares the standard block map across ANF, PBI, and GPRE. ANF is the visual lab base; PBI and GPRE are cross-checks. GPRE-only sector overlays are explicitly excluded from standard-template behavior.

## Coverage Rows

| Block | Sheet | Include | Range | Style | Merge | Freeze | Differences |
| --- | --- | --- | --- | --- | --- | --- | --- |
| summary_company_description_value | SUMMARY | yes | same | similar | same | same | - |
| summary_strategic_context_value | SUMMARY | yes | same | similar | same | same | - |
| summary_key_advantage_value | SUMMARY | yes | same | similar | same | same | - |
| summary_revenue_model_values | SUMMARY | yes | same | similar | same | same | - |
| summary_segment_model_values | SUMMARY | yes | same | similar | same | same | - |
| summary_key_dependencies_values | SUMMARY | yes | same | similar | same | same | - |
| summary_wrong_if_values | SUMMARY | yes | same | similar | same | same | - |
| summary_latest_period_value | SUMMARY | yes | same | similar | similar | same | - |
| summary_latest_revenue_value | SUMMARY | yes | same | similar | same | same | - |
| summary_latest_net_income_value | SUMMARY | yes | same | similar | same | same | - |
| summary_net_leverage_value | SUMMARY | yes | same | similar | similar | same | - |
| summary_revolver_availability_value | SUMMARY | yes | same | similar | same | same | - |
| summary_liquidity_value | SUMMARY | yes | same | similar | same | same | - |
| summary_revenue_mix_label | SUMMARY | yes | same | similar | same | same | - |
| summary_revolver_availability_as_of_value | SUMMARY | yes | same | same | same | same | - |
| summary_liquidity_as_of_value | SUMMARY | yes | same | same | same | same | - |
| valuation_guidance_current_primary_rows | Valuation | yes | same | similar | different | same | - |
| valuation_guidance_current_secondary_rows | Valuation | yes | same | similar | different | same | - |
| valuation_guidance_historical_rows | Valuation | yes | same | similar | different | same | - |
| valuation_thesis_debate_rows | Valuation | yes | same | similar | different | same | - |
| valuation_debt_snapshot_values | Valuation | yes | same | similar | same | same | - |
| valuation_debt_snapshot_periods | Valuation | yes | same | same | same | same | - |
| valuation_debt_snapshot_statuses | Valuation | yes | same | same | same | same | - |
| valuation_debt_snapshot_evidence | Valuation | yes | same | similar | same | same | - |
| module_hidden_value_signals_valuation_rows | Valuation | yes | same | similar | same | same | - |
| valuation_input_values | Valuation | yes | same | similar | same | same | - |
| valuation_period_headers | Valuation | yes | same | similar | same | same | - |
| valuation_raw_revenue | Valuation | yes | same | similar | same | same | - |
| valuation_raw_base_ebitda | Valuation | yes | same | similar | same | same | - |
| valuation_raw_adjusted_ebitda | Valuation | yes | same | similar | same | same | - |
| valuation_raw_operating_income | Valuation | yes | same | similar | same | same | - |
| valuation_raw_net_income | Valuation | yes | same | similar | same | same | - |
| valuation_raw_operating_cash_flow | Valuation | yes | same | similar | same | same | - |
| valuation_raw_capital_expenditures | Valuation | yes | same | similar | same | same | - |
| valuation_raw_interest_paid | Valuation | yes | same | similar | same | same | - |
| valuation_raw_buybacks_cash | Valuation | yes | same | similar | same | same | - |
| valuation_raw_cash | Valuation | yes | same | similar | same | same | - |
| valuation_raw_marketable_securities | Valuation | yes | same | similar | same | same | - |
| valuation_raw_debt_core | Valuation | yes | same | similar | same | same | - |
| valuation_raw_lease_liabilities | Valuation | yes | same | similar | same | same | - |
| valuation_raw_pension_obligation_net | Valuation | yes | same | similar | same | same | - |
| valuation_raw_revolver_availability | Valuation | yes | same | similar | same | same | - |
| valuation_raw_diluted_shares | Valuation | yes | same | similar | same | same | - |
| valuation_raw_shares_outstanding | Valuation | yes | same | similar | same | same | - |
| valuation_raw_eps | Valuation | yes | same | similar | similar | same | - |
| valuation_raw_adjusted_eps | Valuation | yes | same | similar | same | same | - |
| valuation_raw_gross_profit | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_interest_expense | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_dividends_cash | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_acquisitions_cash | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_debt_repayment | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_debt_issuance | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_total_equity | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_goodwill | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| valuation_raw_intangibles | Valuation | yes | missing | same | same | same | ANF: missing block or sheet, ANF: source sheet shorter than block range, PBI: missing block or sheet, +3 more |
| bs_segment_quarterly_values | BS_Segments | yes | same | similar | same | same | - |
| bs_annual_period_values | BS_Segments | yes | same | similar | same | same | - |
| bs_segment_annual_values | BS_Segments | yes | missing | similar | same | same | ANF: source sheet shorter than block range, GPRE: missing block or sheet, GPRE: source sheet shorter than block range |
| bs_quarterly_period_headers | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_cash | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_restricted_cash | BS_Segments | yes | same | same | same | same | - |
| bs_raw_marketable_securities | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_accounts_receivable | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_inventory | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_current_assets | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_property_plant_equipment_net | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_goodwill | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_intangibles | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_other_assets_noncurrent | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_total_assets | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_accounts_payable | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_accrued_liabilities | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_short_term_borrowings | BS_Segments | yes | same | same | same | same | - |
| bs_raw_debt_current | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_lease_liabilities_current | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_current_liabilities | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_debt_core | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_lease_liabilities_noncurrent | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_pension_obligation_net | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_other_liabilities_noncurrent | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_total_liabilities | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_total_equity | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_shares_outstanding | BS_Segments | yes | same | similar | same | same | - |
| bs_raw_diluted_shares | BS_Segments | yes | same | similar | same | same | - |
| od_watchlist_values | Operating_Drivers | yes | same | similar | same | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_topic_current_read_values | Operating_Drivers | yes | same | similar | similar | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_horizon_commentary_values | Operating_Drivers | yes | same | similar | same | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_current_outlook_values | Operating_Drivers | yes | same | similar | different | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_driver_actuals_values | Operating_Drivers | yes | same | similar | different | same | ANF: source sheet shorter than block range, PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range, +1 more |
| ic_snapshot_values | {ticker}_Investment_Case | yes | same | similar | same | same | - |
| ic_title_value | {ticker}_Investment_Case | yes | same | similar | same | same | - |
| qn_quarter_summary_values | Quarter_Notes_UI | yes | same | similar | same | same | - |
| qn_quarter_block_values | Quarter_Notes_UI | yes | same | similar | same | same | - |
| pp_scorecard_values | Promise_Progress_UI | yes | same | same | same | same | - |
| pp_annual_guidance_values | Promise_Progress_UI | yes | same | similar | similar | same | - |
| pp_annual_guidance_values_block_2 | Promise_Progress_UI | yes | same | similar | different | same | - |
| pp_annual_guidance_values_block_3 | Promise_Progress_UI | yes | same | similar | different | same | - |
| pp_annual_guidance_values_block_4 | Promise_Progress_UI | yes | same | similar | similar | same | - |
| pp_open_guidance_values | Promise_Progress_UI | yes | same | similar | different | same | GPRE: source sheet shorter than block range |
| pp_guidance_revision_primary_values | Promise_Progress_UI | yes | missing | similar | similar | same | GPRE: missing block or sheet, GPRE: source sheet shorter than block range |
| pp_guidance_timeline_values_block_2 | Promise_Progress_UI | yes | missing | similar | similar | same | GPRE: missing block or sheet, GPRE: source sheet shorter than block range |
| pp_guidance_timeline_values_block_3 | Promise_Progress_UI | yes | missing | similar | similar | same | PBI: source sheet shorter than block range, GPRE: missing block or sheet, GPRE: source sheet shorter than block range |
| pp_guidance_timeline_values_block_4 | Promise_Progress_UI | yes | missing | similar | similar | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: missing block or sheet, +1 more |
| pp_guidance_timeline_values_block_5 | Promise_Progress_UI | yes | missing | similar | similar | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: missing block or sheet, +1 more |
| pp_guidance_timeline_values_block_6 | Promise_Progress_UI | yes | missing | similar | same | same | ANF: source sheet shorter than block range, PBI: missing block or sheet, PBI: source sheet shorter than block range, +2 more |
| qa_log_rows | QA_Log | yes | same | same | same | same | ANF: source sheet shorter than block range, PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range |
| needs_review_rows | Needs_Review | yes | same | same | same | same | ANF: source sheet shorter than block range, PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range |
| qa_checks_rows | QA_Checks | yes | same | same | same | same | ANF: source sheet shorter than block range, PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range |

## Excluded Sector Overlays

- `Economics_Overlay`: excluded because GPRE-specific sector overlay; not part of standard visible template family. Exists in: GPRE.
- `Basis_Proxy_Sandbox`: excluded because GPRE-specific sector overlay; not part of standard visible template family. Exists in: GPRE.
