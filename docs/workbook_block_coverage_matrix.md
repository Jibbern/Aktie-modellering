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
| summary_key_financials_values | SUMMARY | yes | same | similar | similar | same | - |
| summary_leverage_liquidity_values | SUMMARY | yes | same | similar | similar | same | - |
| valuation_actuals_quarterly_values | Valuation | yes | same | similar | same | same | - |
| valuation_guidance_values | Valuation | yes | same | similar | similar | same | - |
| valuation_guidance_status_values | Valuation | yes | same | similar | similar | same | - |
| valuation_guidance_values_lower | Valuation | yes | same | similar | different | same | - |
| valuation_operating_driver_values | Valuation | yes | same | similar | similar | same | - |
| valuation_thesis_bridge_values | Valuation | yes | same | similar | different | same | - |
| valuation_output_values | Valuation | yes | same | similar | different | same | - |
| valuation_guidance_status_values_lower | Valuation | yes | same | similar | different | same | - |
| valuation_share_count_values | Valuation | yes | same | similar | different | same | - |
| valuation_debt_liquidity_values | Valuation | yes | same | similar | similar | same | - |
| valuation_cash_values | Valuation | yes | same | similar | same | same | - |
| valuation_capital_return_values | Valuation | yes | same | same | same | same | - |
| valuation_input_values | Valuation | yes | same | similar | same | same | - |
| bs_liquidity_values | BS_Segments | yes | same | similar | same | same | - |
| bs_segment_quarterly_values | BS_Segments | yes | same | similar | same | same | - |
| bs_annual_period_values | BS_Segments | yes | same | similar | same | same | - |
| bs_segment_annual_values | BS_Segments | yes | missing | similar | same | same | GPRE: missing block or sheet, GPRE: source sheet shorter than block range |
| od_watchlist_values | Operating_Drivers | yes | same | similar | same | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_topic_current_read_values | Operating_Drivers | yes | same | similar | similar | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_horizon_commentary_values | Operating_Drivers | yes | same | similar | same | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_current_outlook_values | Operating_Drivers | yes | same | similar | different | same | GPRE has separate sector overlays that are excluded from standard blocks |
| od_driver_actuals_values | Operating_Drivers | yes | same | similar | different | same | ANF: source sheet shorter than block range, PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range, +1 more |
| ic_snapshot_values | {ticker}_Investment_Case | yes | same | similar | same | same | - |
| ic_manual_input_values | {ticker}_Investment_Case | yes | same | similar | similar | same | - |
| ic_scenario_bridge_values | {ticker}_Investment_Case | yes | same | similar | similar | same | PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range |
| ic_lower_comp_history_labels | {ticker}_Investment_Case | yes | missing | similar | different | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: source sheet shorter than block range |
| ic_lower_business_health_values | {ticker}_Investment_Case | yes | missing | similar | similar | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: missing block or sheet, +1 more |
| ic_lower_inventory_values | {ticker}_Investment_Case | yes | missing | similar | different | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: missing block or sheet, +1 more |
| ic_lower_asset_productivity_values | {ticker}_Investment_Case | yes | missing | similar | different | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: missing block or sheet, +1 more |
| ic_lower_guidance_setup_values | {ticker}_Investment_Case | yes | missing | similar | different | same | PBI: missing block or sheet, PBI: source sheet shorter than block range, GPRE: missing block or sheet, +1 more |
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
