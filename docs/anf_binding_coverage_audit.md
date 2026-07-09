# ANF Binding Coverage Audit

Coverage check for how the ANF shadow normalized package maps to the current workbook binding map.

- Generated: `2026-07-08T00:42:21+00:00`
- Bindings with populated data: `60` / `62`
- Bindings that would write useful output: `58`

| Binding | Sheet | Field | Values | Rows | Would write | Reason if blank |
| --- | --- | --- | ---: | ---: | --- | --- |
| `summary_company_description` | `SUMMARY` | `company_profile.business_description` | 1 | 1/1 | True |  |
| `summary_key_advantage` | `SUMMARY` | `company_profile.key_advantages` | 1 | 1/1 | True |  |
| `summary_strategic_context` | `SUMMARY` | `company_profile.business_description` | 1 | 1/1 | True |  |
| `summary_revenue_model` | `SUMMARY` | `company_profile.revenue_model` | 1 | 1/3 | True |  |
| `summary_segment_model` | `SUMMARY` | `segments.items.0.revenue` | 14 | 14/3 | True |  |
| `summary_key_risks` | `SUMMARY` | `company_profile.key_risks` | 1 | 1/5 | True |  |
| `summary_wrong_if` | `SUMMARY` | `investment_case.key_debate` | 1 | 1/2 | True |  |
| `summary_quarterly_revenue` | `SUMMARY` | `quarterly_financials.rows.0.revenue` | 12 | 12/4 | True |  |
| `summary_quarterly_operating_income` | `SUMMARY` | `quarterly_financials.rows.0.operating_income` | 12 | 12/10 | True |  |
| `summary_net_debt` | `SUMMARY` | `debt_liquidity.net_debt` | 1 | 1/2 | True |  |
| `summary_liquidity` | `SUMMARY` | `debt_liquidity.liquidity` | 1 | 1/3 | True |  |
| `valuation_revenue_series` | `Valuation` | `quarterly_financials.rows.0.revenue` | 12 | 12/23 | True |  |
| `valuation_guidance_rows` | `Valuation` | `normalized_guidance.items.0.value` | 30 | 30/19 | True |  |
| `valuation_guidance_rows_lower` | `Valuation` | `normalized_guidance.items.0.value` | 30 | 30/8 | True |  |
| `valuation_operating_driver_sidecar_rows` | `Valuation` | `operating_drivers.items.0.driver` | 30 | 30/9 | True |  |
| `valuation_thesis_bridge_rows` | `Valuation` | `investment_case.key_debate` | 1 | 1/12 | True |  |
| `valuation_output_rows` | `Valuation` | `mapping_gaps` | 0 | 0/107 | False | normalized field is absent or not populated |
| `valuation_guidance_status_rows` | `Valuation` | `normalized_guidance.items.0.horizon` | 30 | 30/19 | True |  |
| `valuation_guidance_status_rows_lower` | `Valuation` | `normalized_guidance.items.0.horizon` | 30 | 30/8 | True |  |
| `valuation_operating_income_series` | `Valuation` | `quarterly_financials.rows.0.operating_income` | 12 | 12/8 | True |  |
| `valuation_ebitda_series` | `Valuation` | `quarterly_financials.rows.0.adjusted_ebitda` | 12 | 12/8 | True |  |
| `valuation_fcf_series` | `Valuation` | `quarterly_financials.rows.0.free_cash_flow` | 12 | 12/10 | True |  |
| `valuation_diluted_shares_series` | `Valuation` | `quarterly_financials.rows.0.diluted_shares` | 12 | 12/10 | True |  |
| `valuation_total_debt_series` | `Valuation` | `debt_liquidity.total_debt` | 1 | 1/9 | True |  |
| `valuation_cash_series` | `Valuation` | `debt_liquidity.cash` | 1 | 1/5 | True |  |
| `valuation_buybacks_series` | `Valuation` | `capital_returns.buybacks` | 1 | 1/5 | True |  |
| `bs_cash_series` | `BS_Segments` | `debt_liquidity.cash` | 1 | 1/12 | True |  |
| `bs_debt_series` | `BS_Segments` | `debt_liquidity.total_debt` | 1 | 1/31 | True |  |
| `bs_segment_quarterly_rows` | `BS_Segments` | `segments.items.0.revenue` | 14 | 14/41 | True |  |
| `bs_segment_annual_rows` | `BS_Segments` | `segments.items.0.annual_revenue` | 6 | 6/30 | True |  |
| `od_watchlist_rows` | `Operating_Drivers` | `operating_drivers.items.0.current_read` | 120 | 30/6 | True |  |
| `od_topic_current_read_rows` | `Operating_Drivers` | `operating_drivers.items.0.current_read` | 30 | 30/6 | True |  |
| `od_horizon_commentary_rows` | `Operating_Drivers` | `operating_drivers.items.0.driver` | 30 | 30/11 | True |  |
| `od_current_outlook_rows` | `Operating_Drivers` | `operating_drivers.items.0.driver` | 30 | 30/25 | True |  |
| `od_driver_actuals_rows` | `Operating_Drivers` | `operating_drivers.items.0.metric_value` | 25 | 25/70 | True |  |
| `ic_investment_summary` | `{ticker}_Investment_Case` | `investment_case.summary` | 1 | 1/26 | True |  |
| `ic_key_debate` | `{ticker}_Investment_Case` | `investment_case.key_debate` | 1 | 1/50 | True |  |
| `ic_bull_base_bear_rows` | `{ticker}_Investment_Case` | `investment_case.scenario_drivers` | 1 | 1/50 | True |  |
| `ic_scenario_bridge_rows` | `{ticker}_Investment_Case` | `segments.items.0.revenue` | 14 | 14/60 | True |  |
| `ic_lower_comp_history_labels` | `{ticker}_Investment_Case` | `operating_drivers.items.0.driver` | 30 | 30/7 | True |  |
| `ic_lower_brand_health_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.current_read` | 30 | 30/6 | True |  |
| `ic_lower_inventory_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.metric_value` | 25 | 25/8 | True |  |
| `ic_lower_store_productivity_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.metric_value` | 25 | 25/14 | True |  |
| `ic_lower_guidance_setup_rows` | `{ticker}_Investment_Case` | `normalized_guidance.items.0.value` | 30 | 30/6 | True |  |
| `qn_quarter_note_rows` | `Quarter_Notes_UI` | `quarter_notes.items.0.note` | 240 | 40/353 | True |  |
| `qn_quarter_summary_rows` | `Quarter_Notes_UI` | `quarter_notes.items.0.note` | 40 | 40/4 | True |  |
| `qn_quarter_model_implication_rows` | `Quarter_Notes_UI` | `quarter_notes.items.0.model_implication` | 40 | 40/353 | True |  |
| `pp_scorecard_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.metric` | 30 | 30/7 | True |  |
| `pp_annual_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 120 | 30/10 | True |  |
| `pp_annual_guidance_rows_block_2` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 30 | 30/5 | True |  |
| `pp_annual_guidance_rows_block_3` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 30 | 30/4 | True |  |
| `pp_annual_guidance_rows_block_4` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 30 | 30/1 | True |  |
| `pp_open_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 120 | 30/21 | True |  |
| `pp_guidance_timeline_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/9 | True |  |
| `pp_guidance_timeline_rows_block_2` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/6 | True |  |
| `pp_guidance_timeline_rows_block_3` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/7 | True |  |
| `pp_guidance_timeline_rows_block_4` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/5 | True |  |
| `pp_guidance_timeline_rows_block_5` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/6 | True |  |
| `pp_guidance_timeline_rows_block_6` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 30 | 30/17 | True |  |
| `qa_log_validation_rows` | `QA_Log` | `manual_review_flags` | 4032 | 672/4999 | False | validation output binding |
| `needs_review_validation_rows` | `Needs_Review` | `manual_review_flags` | 4032 | 672/4999 | False | validation output binding |
| `qa_checks_mapping_gap_rows` | `QA_Checks` | `mapping_gaps` | 0 | 0/4999 | False | validation output binding |

## Row Schema Observation

Table-row bindings now expose row-schema columns in the JSON binding map. The ANF shadow package populates enough row-shaped data to audit whether future filler output would be useful, without creating an ANF workbook in this pass.
