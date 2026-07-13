# ANF Binding Coverage Audit

Coverage check for how the ANF shadow normalized package maps to the current workbook binding map.

- Generated: `2026-07-12T22:11:12+00:00`
- Bindings with populated data: `63` / `78`
- Bindings that would write useful output: `35`

| Binding | Sheet | Field | Values | Rows | Would write | Reason if blank |
| --- | --- | --- | ---: | ---: | --- | --- |
| `summary_company_description` | `SUMMARY` | `company_profile.business_description` | 1 | 1/1 | True |  |
| `summary_key_advantage` | `SUMMARY` | `company_profile.key_advantages` | 1 | 1/1 | True |  |
| `summary_strategic_context` | `SUMMARY` | `company_profile.strategic_context` | 1 | 1/1 | True |  |
| `summary_revenue_stream_rows` | `SUMMARY` | `company_profile.revenue_streams` | 3 | 3/3 | True |  |
| `summary_segment_model` | `SUMMARY` | `segments.items.0.revenue` | 14 | 14/3 | False | binding planning_state is inactive_legacy_contract |
| `summary_key_risks` | `SUMMARY` | `company_profile.key_risks` | 1 | 1/5 | False | binding planning_state is inactive_legacy_contract |
| `summary_wrong_if` | `SUMMARY` | `investment_case.key_debate` | 1 | 1/2 | False | binding planning_state is inactive_legacy_contract |
| `summary_as_of_quarter` | `SUMMARY` | `quarterly_financials.rows.0.period` | 0 | 0/4 | True |  |
| `summary_latest_net_income` | `SUMMARY` | `quarterly_financials.rows.0.net_income` | 12 | 12/10 | True |  |
| `summary_net_leverage` | `SUMMARY` | `debt_liquidity.net_leverage` | 0 | 0/1 | False | normalized field is absent or not populated |
| `summary_liquidity` | `SUMMARY` | `debt_liquidity.summary_liquidity_display` | 1 | 1/1 | True |  |
| `valuation_revenue_series` | `Valuation` | `quarterly_financials.rows.0.revenue` | 12 | 12/23 | True |  |
| `valuation_guidance_rows` | `Valuation` | `normalized_guidance.items.value` | 0 | 0/19 | True |  |
| `valuation_guidance_rows_lower` | `Valuation` | `normalized_guidance.items.0.value` | 30 | 30/8 | False | binding planning_state is inactive_legacy_contract |
| `valuation_operating_driver_sidecar_rows` | `Valuation` | `operating_drivers.items.0.driver` | 34 | 34/9 | False | binding planning_state is inactive_legacy_contract |
| `valuation_thesis_bridge_rows` | `Valuation` | `investment_case.key_debate` | 1 | 1/12 | False | binding planning_state is inactive_legacy_contract |
| `valuation_output_rows` | `Valuation` | `valuation_outputs.items.value` | 0 | 0/107 | False | normalized field is absent or not populated |
| `valuation_guidance_status_rows` | `Valuation` | `normalized_guidance.items.progress_status` | 0 | 0/19 | True |  |
| `valuation_guidance_status_rows_lower` | `Valuation` | `normalized_guidance.items.0.horizon` | 30 | 30/8 | False | binding planning_state is inactive_legacy_contract |
| `valuation_operating_income_series` | `Valuation` | `quarterly_financials.rows.0.operating_income` | 12 | 12/8 | False | binding planning_state is inactive_legacy_contract |
| `valuation_ebitda_series` | `Valuation` | `quarterly_financials.rows.0.adjusted_ebitda` | 12 | 12/1 | True |  |
| `valuation_fcf_series` | `Valuation` | `quarterly_financials.rows.0.free_cash_flow` | 12 | 12/10 | False | binding planning_state is inactive_legacy_contract |
| `valuation_diluted_shares_series` | `Valuation` | `quarterly_financials.rows.0.diluted_shares` | 12 | 12/10 | False | binding planning_state is inactive_legacy_contract |
| `valuation_total_debt_series` | `Valuation` | `debt_liquidity.total_debt` | 0 | 0/9 | False | binding planning_state is inactive_legacy_contract |
| `valuation_cash_series` | `Valuation` | `debt_liquidity.cash` | 1 | 1/5 | False | binding planning_state is inactive_legacy_contract |
| `valuation_buybacks_series` | `Valuation` | `capital_returns.buybacks` | 1 | 1/5 | False | binding planning_state is inactive_legacy_contract |
| `bs_cash_series` | `BS_Segments` | `debt_liquidity.cash` | 1 | 1/12 | False | binding planning_state is inactive_legacy_contract |
| `bs_debt_series` | `BS_Segments` | `debt_liquidity.total_debt` | 0 | 0/31 | False | binding planning_state is inactive_legacy_contract |
| `bs_segment_quarterly_rows` | `BS_Segments` | `segments.items.revenue` | 14 | 14/7 | True |  |
| `bs_segment_annual_rows` | `BS_Segments` | `segments.items.annual_revenue` | 6 | 6/3 | True |  |
| `od_watchlist_rows` | `Operating_Drivers` | `operating_drivers.items.current_read` | 136 | 34/4 | True |  |
| `od_topic_current_read_rows` | `Operating_Drivers` | `operating_drivers.items.0.current_read` | 34 | 34/6 | False | binding planning_state is inactive_legacy_contract |
| `od_horizon_commentary_rows` | `Operating_Drivers` | `operating_drivers.items.0.driver` | 34 | 34/11 | False | binding planning_state is inactive_legacy_contract |
| `od_current_outlook_rows` | `Operating_Drivers` | `operating_drivers.items.0.driver` | 34 | 34/25 | False | binding planning_state is inactive_legacy_contract |
| `od_driver_actuals_rows` | `Operating_Drivers` | `operating_drivers.items.0.metric_value` | 19 | 19/70 | False | binding planning_state is inactive_legacy_contract |
| `ic_investment_summary` | `{ticker}_Investment_Case` | `investment_case.summary` | 1 | 1/26 | True |  |
| `ic_key_debate` | `{ticker}_Investment_Case` | `investment_case.key_debate` | 1 | 1/1 | True |  |
| `ic_bull_base_bear_rows` | `{ticker}_Investment_Case` | `investment_case.scenario_drivers` | 1 | 1/50 | False | binding planning_state is inactive_legacy_contract |
| `ic_scenario_bridge_rows` | `{ticker}_Investment_Case` | `segments.items.0.revenue` | 14 | 14/60 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_comp_history_labels` | `{ticker}_Investment_Case` | `operating_drivers.items.0.driver` | 34 | 34/7 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_business_health_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.current_read` | 34 | 34/6 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_inventory_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.metric_value` | 19 | 19/8 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_asset_productivity_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.metric_value` | 19 | 19/14 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_guidance_setup_rows` | `{ticker}_Investment_Case` | `normalized_guidance.items.0.value` | 30 | 30/6 | False | binding planning_state is inactive_legacy_contract |
| `qn_quarter_note_rows` | `Quarter_Notes_UI` | `quarter_notes.items.commentary` | 36 | 6/6 | True |  |
| `qn_quarter_summary_rows` | `Quarter_Notes_UI` | `quarter_notes.items.commentary` | 0 | 0/4 | True |  |
| `qn_quarter_model_implication_rows` | `Quarter_Notes_UI` | `quarter_notes.items.0.model_implication` | 6 | 6/353 | False | binding planning_state is inactive_legacy_contract |
| `pp_scorecard_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.metric` | 30 | 30/7 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 90 | 30/10 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows_block_2` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 30 | 30/5 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows_block_3` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 30 | 30/4 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows_block_4` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 30 | 30/1 | False | binding planning_state is inactive_legacy_contract |
| `pp_open_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 90 | 30/21 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows` | `Promise_Progress_UI` | `normalized_guidance.items` | 30 | 30/7 | True |  |
| `pp_guidance_timeline_rows_block_2` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/6 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_3` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/7 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_4` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/5 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_5` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/6 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_6` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/17 | False | binding planning_state is inactive_legacy_contract |
| `qa_log_validation_rows` | `QA_Log` | `issue_ledger.qa_presentation.qa_log_rows` | 0 | 0/4999 | False | validation output binding |
| `needs_review_validation_rows` | `Needs_Review` | `issue_ledger.qa_presentation.needs_review_rows` | 0 | 0/4999 | False | validation output binding |
| `qa_checks_mapping_gap_rows` | `QA_Checks` | `issue_ledger.qa_presentation.qa_check_rows` | 0 | 0/4999 | False | validation output binding |
| `summary_latest_revenue` | `SUMMARY` | `quarterly_financials.rows.0.revenue` | 12 | 12/14 | True |  |
| `valuation_period_headers` | `Valuation` | `quarterly_financials.rows.0.period` | 0 | 0/23 | True |  |
| `valuation_net_income_series` | `Valuation` | `quarterly_financials.rows.0.net_income` | 12 | 12/1 | True |  |
| `valuation_operating_cash_flow_series` | `Valuation` | `quarterly_financials.rows.0.operating_cash_flow` | 12 | 12/1 | True |  |
| `bs_quarterly_period_headers` | `BS_Segments` | `quarterly_financials.rows.period` | 0 | 0/1 | True |  |
| `bs_annual_period_headers` | `BS_Segments` | `annual_financials.rows.period` | 0 | 0/1 | True |  |
| `bs_annual_revenue_series` | `BS_Segments` | `annual_financials.rows.revenue` | 0 | 0/1 | True |  |
| `valuation_input_as_of` | `Valuation` | `valuation_inputs.as_of_date` | 1 | 1/1 | True |  |
| `valuation_input_shares_outstanding` | `Valuation` | `valuation_inputs.shares_outstanding` | 1 | 1/1 | True |  |
| `valuation_input_diluted_shares` | `Valuation` | `valuation_inputs.diluted_shares` | 1 | 1/1 | True |  |
| `valuation_input_net_debt` | `Valuation` | `valuation_inputs.net_debt` | 1 | 1/1 | True |  |
| `valuation_input_base_ebitda_ttm` | `Valuation` | `valuation_inputs.base_ebitda_ttm` | 1 | 1/1 | True |  |
| `valuation_input_adjusted_ebitda_ttm` | `Valuation` | `valuation_inputs.adjusted_ebitda_ttm` | 1 | 1/1 | True |  |
| `valuation_input_fcf_ttm` | `Valuation` | `valuation_inputs.free_cash_flow_ttm` | 1 | 1/1 | True |  |
| `valuation_input_revenue_ttm` | `Valuation` | `valuation_inputs.revenue_ttm` | 1 | 1/1 | True |  |
| `valuation_input_capex_ttm` | `Valuation` | `valuation_inputs.capex_ttm` | 1 | 1/1 | True |  |

## Row Schema Observation

Table-row bindings now expose row-schema columns in the JSON binding map. The ANF shadow package populates enough row-shaped data to audit whether future filler output would be useful, without creating an ANF workbook in this pass.
