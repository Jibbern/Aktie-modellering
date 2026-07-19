# ANF Binding Coverage Audit

Coverage check for how the ANF shadow normalized package maps to the current workbook binding map.

- Generated: `2026-07-19T10:51:07+00:00`
- Bindings with populated data: `80` / `182`
- Bindings that would write useful output: `121`

| Binding | Sheet | Field | Values | Rows | Would write | Reason if blank |
| --- | --- | --- | ---: | ---: | --- | --- |
| `calculation_history_quarterly_rows` | `History_Q` | `calculation_history.quarterly_items` | 635 | 635/999 | True |  |
| `summary_company_description` | `SUMMARY` | `company_profile.business_description` | 1 | 1/1 | True |  |
| `summary_key_advantage` | `SUMMARY` | `company_profile.key_advantages` | 1 | 1/1 | True |  |
| `summary_strategic_context` | `SUMMARY` | `company_profile.strategic_context` | 1 | 1/1 | True |  |
| `summary_revenue_stream_rows` | `SUMMARY` | `company_profile.revenue_streams` | 3 | 3/3 | True |  |
| `summary_segment_model` | `SUMMARY` | `company_profile.operating_model_rows` | 3 | 3/3 | True |  |
| `summary_key_risks` | `SUMMARY` | `company_profile.key_dependencies` | 5 | 5/5 | True |  |
| `summary_wrong_if` | `SUMMARY` | `investment_case.invalidators` | 2 | 2/2 | True |  |
| `summary_as_of_quarter` | `SUMMARY` | `quarterly_financials.rows.period` | 0 | 0/1 | True |  |
| `summary_latest_net_income` | `SUMMARY` | `quarterly_financials.rows.net_income` | 0 | 0/1 | True |  |
| `summary_net_leverage` | `SUMMARY` | `debt_liquidity.net_leverage` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_guidance_rows` | `Valuation` | `normalized_guidance.items.value` | 0 | 0/19 | True |  |
| `valuation_guidance_rows_lower` | `Valuation` | `normalized_guidance.items.0.value` | 196 | 196/8 | False | binding planning_state is inactive_legacy_contract |
| `valuation_operating_driver_sidecar_rows` | `Valuation` | `operating_drivers.items.0.driver` | 34 | 34/9 | False | binding planning_state is inactive_legacy_contract |
| `valuation_thesis_bridge_rows` | `Valuation` | `investment_case.key_debate` | 1 | 1/12 | False | binding planning_state is inactive_legacy_contract |
| `valuation_output_rows` | `Valuation` | `valuation_outputs.items.value` | 0 | 0/107 | False | binding planning_state is formula_owned |
| `valuation_guidance_status_rows` | `Valuation` | `normalized_guidance.items.progress_status` | 0 | 0/19 | True |  |
| `valuation_guidance_status_rows_lower` | `Valuation` | `normalized_guidance.items.0.horizon` | 196 | 196/8 | False | binding planning_state is inactive_legacy_contract |
| `bs_segment_quarterly_rows` | `BS_Segments` | `segments.items.revenue` | 105 | 105/7 | True |  |
| `bs_segment_annual_rows` | `BS_Segments` | `segments.items.annual_revenue` | 21 | 21/7 | True |  |
| `od_watchlist_rows` | `Operating_Drivers` | `operating_drivers.items.current_read` | 136 | 34/4 | True |  |
| `od_current_actual_read` | `Operating_Drivers` | `operating_drivers.current_outlook.current_actual_read` | 1 | 1/1 | True |  |
| `od_current_actual_use` | `Operating_Drivers` | `operating_drivers.current_outlook.current_actual_use` | 1 | 1/1 | True |  |
| `od_current_guidance_read` | `Operating_Drivers` | `operating_drivers.current_outlook.current_guidance_read` | 1 | 1/1 | True |  |
| `od_current_guidance_use` | `Operating_Drivers` | `operating_drivers.current_outlook.current_guidance_use` | 1 | 1/1 | True |  |
| `od_margin_bridge_read` | `Operating_Drivers` | `operating_drivers.current_outlook.margin_bridge_read` | 1 | 1/1 | True |  |
| `od_margin_bridge_use` | `Operating_Drivers` | `operating_drivers.current_outlook.margin_bridge_use` | 1 | 1/1 | True |  |
| `od_topic_current_read_rows` | `Operating_Drivers` | `operating_drivers.items.0.current_read` | 34 | 34/6 | False | binding planning_state is inactive_legacy_contract |
| `od_horizon_commentary_rows` | `Operating_Drivers` | `operating_drivers.items.0.driver` | 34 | 34/11 | False | binding planning_state is inactive_legacy_contract |
| `od_current_outlook_rows` | `Operating_Drivers` | `operating_drivers.items.0.driver` | 34 | 34/25 | False | binding planning_state is inactive_legacy_contract |
| `od_driver_actuals_rows` | `Operating_Drivers` | `operating_drivers.items.0.metric_value` | 19 | 19/70 | False | binding planning_state is inactive_legacy_contract |
| `ic_investment_summary` | `{ticker}_Investment_Case` | `investment_case.summary` | 1 | 1/1 | True |  |
| `ic_key_debate` | `{ticker}_Investment_Case` | `investment_case.key_debate` | 1 | 1/1 | True |  |
| `ic_why_it_can_work` | `{ticker}_Investment_Case` | `investment_case.why_it_can_work` | 1 | 1/1 | True |  |
| `ic_upside_factors` | `{ticker}_Investment_Case` | `investment_case.upside_factors` | 1 | 1/1 | True |  |
| `ic_downside_factors` | `{ticker}_Investment_Case` | `investment_case.downside_factors` | 1 | 1/1 | True |  |
| `ic_watch_next` | `{ticker}_Investment_Case` | `investment_case.watch_next` | 1 | 1/1 | True |  |
| `ic_current_stance` | `{ticker}_Investment_Case` | `investment_case.current_stance` | 1 | 1/1 | True |  |
| `ic_bull_base_bear_rows` | `Scenario_Driver_Assumptions` | `investment_case.scenario_items` | 16 | 16/200 | True |  |
| `ic_scenario_bridge_rows` | `{ticker}_Investment_Case_Data` | `investment_case.scenario_driver_bridge` | 6 | 6/200 | True |  |
| `ic_lower_comp_history_labels` | `{ticker}_Investment_Case` | `operating_drivers.items.0.driver` | 34 | 34/7 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_business_health_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.current_read` | 34 | 34/6 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_inventory_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.metric_value` | 19 | 19/8 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_asset_productivity_rows` | `{ticker}_Investment_Case` | `operating_drivers.items.0.metric_value` | 19 | 19/14 | False | binding planning_state is inactive_legacy_contract |
| `ic_lower_guidance_setup_rows` | `{ticker}_Investment_Case` | `normalized_guidance.items.0.value` | 196 | 196/6 | False | binding planning_state is inactive_legacy_contract |
| `qn_quarter_note_rows` | `Quarter_Notes_UI` | `quarter_notes.items.commentary` | 42 | 6/6 | True |  |
| `qn_quarter_summary_rows` | `Quarter_Notes_UI` | `quarter_notes.summary.model_read` | 1 | 1/1 | True |  |
| `qn_quarter_summary_what_changed` | `Quarter_Notes_UI` | `quarter_notes.summary.what_changed` | 1 | 1/1 | True |  |
| `qn_quarter_summary_watch_next` | `Quarter_Notes_UI` | `quarter_notes.summary.watch_next` | 1 | 1/1 | True |  |
| `qn_quarter_summary_key_caveat` | `Quarter_Notes_UI` | `quarter_notes.summary.key_caveat` | 1 | 1/1 | True |  |
| `qn_quarter_model_implication_rows` | `Quarter_Notes_UI` | `quarter_notes.items.0.model_implication` | 6 | 6/353 | False | binding planning_state is inactive_legacy_contract |
| `pp_scorecard_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.metric` | 196 | 196/7 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 787 | 196/8 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows_block_2` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 196 | 196/4 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows_block_3` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 196 | 196/3 | False | binding planning_state is inactive_legacy_contract |
| `pp_annual_guidance_rows_block_4` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 196 | 196/2 | False | binding planning_state is inactive_legacy_contract |
| `pp_open_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items.0.value` | 787 | 196/20 | False | binding planning_state is inactive_legacy_contract |
| `pp_progress_fy2025_rows` | `Promise_Progress_UI` | `promise_progress.items` | 82 | 13/8 | True |  |
| `pp_progress_fy2024_rows` | `Promise_Progress_UI` | `promise_progress.items` | 82 | 13/4 | True |  |
| `pp_current_secondary_guidance_rows` | `Promise_Progress_UI` | `normalized_guidance.items` | 980 | 196/20 | True |  |
| `pp_guidance_timeline_rows` | `Promise_Progress_UI` | `normalized_guidance.items` | 196 | 196/7 | True |  |
| `pp_guidance_timeline_rows_block_2` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 196 | 196/6 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_3` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 196 | 196/7 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_4` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 196 | 196/5 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_5` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 196 | 196/6 | False | binding planning_state is inactive_legacy_contract |
| `pp_guidance_timeline_rows_block_6` | `Promise_Progress_UI` | `normalized_guidance.items.0.horizon` | 196 | 196/17 | False | binding planning_state is inactive_legacy_contract |
| `hidden_value_base_rows` | `Hidden_Value_Base` | `_derived_workbook.hidden_value.base_rows` | 0 | 0/5000 | True |  |
| `hidden_value_audit_rows` | `Hidden_Value_Audit` | `_derived_workbook.hidden_value.audit_rows` | 0 | 0/7 | True |  |
| `hidden_value_recompute_rows` | `Hidden_Value_Recompute` | `_derived_workbook.hidden_value.recompute_rows` | 0 | 0/91 | True |  |
| `hidden_value_flags_rows` | `Hidden_Value_Flags` | `_derived_workbook.hidden_value.flags_rows` | 0 | 0/7 | False | normalized field is absent or not populated |
| `hidden_value_valuation_rows` | `Valuation` | `_derived_workbook.hidden_value.flags_rows` | 0 | 0/5 | False | normalized field is absent or not populated |
| `qa_log_validation_rows` | `QA_Log` | `issue_ledger.qa_presentation.qa_log_rows` | 0 | 0/4999 | False | validation output binding |
| `needs_review_validation_rows` | `Needs_Review` | `issue_ledger.qa_presentation.needs_review_rows` | 0 | 0/4999 | False | validation output binding |
| `qa_checks_mapping_gap_rows` | `QA_Checks` | `issue_ledger.qa_presentation.qa_check_rows` | 0 | 0/4999 | False | validation output binding |
| `summary_latest_revenue` | `SUMMARY` | `quarterly_financials.rows.revenue` | 0 | 0/1 | True |  |
| `summary_revenue_mix_label` | `SUMMARY` | `company_profile.revenue_mix_label` | 1 | 1/1 | True |  |
| `summary_liquidity` | `SUMMARY` | `debt_liquidity.summary_liquidity_display` | 1 | 1/1 | True |  |
| `summary_liquidity_as_of` | `SUMMARY` | `debt_liquidity.summary_liquidity_as_of_display` | 1 | 1/1 | True |  |
| `summary_revolver_availability` | `SUMMARY` | `debt_liquidity.revolver_availability` | 1 | 1/1 | True |  |
| `summary_revolver_availability_as_of` | `SUMMARY` | `debt_liquidity.summary_liquidity_as_of_display` | 1 | 1/1 | True |  |
| `valuation_debt_snapshot_cash_value` | `Valuation` | `debt_liquidity.cash` | 1 | 1/1 | True |  |
| `valuation_debt_snapshot_cash_as_of` | `Valuation` | `debt_liquidity.cash.period` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_cash_evidence` | `Valuation` | `debt_liquidity.cash.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_cash_status` | `Valuation` | `debt_liquidity.cash.status` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_revolver_value` | `Valuation` | `debt_liquidity.revolver_availability` | 1 | 1/1 | True |  |
| `valuation_debt_snapshot_revolver_as_of` | `Valuation` | `debt_liquidity.revolver_availability.period` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_revolver_evidence` | `Valuation` | `debt_liquidity.revolver_availability.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_revolver_status` | `Valuation` | `debt_liquidity.revolver_availability.status` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_liquidity_value` | `Valuation` | `debt_liquidity.total_liquidity` | 1 | 1/1 | True |  |
| `valuation_debt_snapshot_liquidity_as_of` | `Valuation` | `debt_liquidity.total_liquidity.period` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_liquidity_evidence` | `Valuation` | `debt_liquidity.total_liquidity.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_liquidity_status` | `Valuation` | `debt_liquidity.total_liquidity.status` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_leases_value` | `Valuation` | `debt_liquidity.lease_liabilities` | 1 | 1/1 | True |  |
| `valuation_debt_snapshot_leases_as_of` | `Valuation` | `debt_liquidity.lease_liabilities.period` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_leases_evidence` | `Valuation` | `debt_liquidity.lease_liabilities.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_leases_status` | `Valuation` | `debt_liquidity.lease_liabilities.status` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_core_debt_value` | `Valuation` | `debt_liquidity.total_debt` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_snapshot_core_debt_as_of` | `Valuation` | `debt_liquidity.total_debt.period` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_snapshot_core_debt_evidence` | `Valuation` | `debt_liquidity.total_debt.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_core_debt_status` | `Valuation` | `debt_liquidity.total_debt.status` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_net_debt_value` | `Valuation` | `debt_liquidity.net_debt` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_snapshot_net_debt_as_of` | `Valuation` | `debt_liquidity.net_debt.period` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_snapshot_net_debt_evidence` | `Valuation` | `debt_liquidity.net_debt.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_net_debt_status` | `Valuation` | `debt_liquidity.net_debt.status` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_net_leverage_value` | `Valuation` | `debt_liquidity.net_leverage` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_snapshot_net_leverage_as_of` | `Valuation` | `debt_liquidity.net_leverage.period` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_snapshot_net_leverage_evidence` | `Valuation` | `debt_liquidity.net_leverage.source_ref` | 0 | 0/1 | True |  |
| `valuation_debt_snapshot_net_leverage_status` | `Valuation` | `debt_liquidity.net_leverage.status` | 0 | 0/1 | True |  |
| `ic_investment_case_title` | `{ticker}_Investment_Case` | `ticker_metadata.investment_case_title` | 1 | 1/1 | True |  |
| `valuation_period_headers` | `Valuation` | `quarterly_financials.rows.period` | 0 | 0/1 | True |  |
| `valuation_revenue_series` | `Valuation` | `quarterly_financials.rows.revenue` | 0 | 0/1 | True |  |
| `valuation_ebitda_series` | `Valuation` | `quarterly_financials.rows.base_ebitda` | 0 | 0/1 | True |  |
| `valuation_adjusted_ebitda_series` | `Valuation` | `quarterly_financials.rows.adjusted_ebitda` | 0 | 0/1 | True |  |
| `valuation_operating_income_series` | `Valuation` | `quarterly_financials.rows.operating_income` | 0 | 0/1 | True |  |
| `valuation_net_income_series` | `Valuation` | `quarterly_financials.rows.net_income` | 0 | 0/1 | True |  |
| `valuation_operating_cash_flow_series` | `Valuation` | `quarterly_financials.rows.operating_cash_flow` | 0 | 0/1 | True |  |
| `valuation_capital_expenditures_series` | `Valuation` | `quarterly_financials.rows.capital_expenditures` | 0 | 0/1 | True |  |
| `valuation_interest_paid_series` | `Valuation` | `quarterly_financials.rows.interest_paid` | 0 | 0/1 | True |  |
| `valuation_buybacks_cash_series` | `Valuation` | `quarterly_financials.rows.buybacks_cash` | 0 | 0/1 | True |  |
| `valuation_cash_series` | `Valuation` | `quarterly_financials.rows.cash` | 0 | 0/1 | True |  |
| `valuation_marketable_securities_series` | `Valuation` | `quarterly_financials.rows.marketable_securities` | 0 | 0/1 | True |  |
| `valuation_debt_core_series` | `Valuation` | `quarterly_financials.rows.debt_core` | 0 | 0/1 | True |  |
| `valuation_lease_liabilities_series` | `Valuation` | `quarterly_financials.rows.lease_liabilities` | 0 | 0/1 | True |  |
| `valuation_pension_obligation_net_series` | `Valuation` | `quarterly_financials.rows.pension_obligation_net` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_revolver_availability_series` | `Valuation` | `quarterly_financials.rows.revolver_availability` | 0 | 0/1 | True |  |
| `valuation_diluted_shares_series` | `Valuation` | `quarterly_financials.rows.diluted_shares` | 0 | 0/1 | True |  |
| `valuation_shares_outstanding_series` | `Valuation` | `quarterly_financials.rows.shares_outstanding` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_eps_series` | `Valuation` | `quarterly_financials.rows.eps` | 0 | 0/1 | True |  |
| `valuation_adjusted_eps_series` | `Valuation` | `quarterly_financials.rows.adjusted_eps` | 0 | 0/1 | True |  |
| `valuation_gross_profit_series` | `Valuation` | `quarterly_financials.rows.gross_profit` | 0 | 0/1 | True |  |
| `valuation_interest_expense_series` | `Valuation` | `quarterly_financials.rows.interest_expense` | 0 | 0/1 | True |  |
| `valuation_dividends_cash_series` | `Valuation` | `quarterly_financials.rows.dividends_cash` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_acquisitions_cash_series` | `Valuation` | `quarterly_financials.rows.acquisitions_cash` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_repayment_series` | `Valuation` | `quarterly_financials.rows.debt_repayment` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_debt_issuance_series` | `Valuation` | `quarterly_financials.rows.debt_issuance` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_total_equity_series` | `Valuation` | `quarterly_financials.rows.total_equity` | 0 | 0/1 | True |  |
| `valuation_goodwill_series` | `Valuation` | `quarterly_financials.rows.goodwill` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_intangibles_series` | `Valuation` | `quarterly_financials.rows.intangibles` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_quarterly_period_headers` | `BS_Segments` | `quarterly_financials.rows.period` | 0 | 0/1 | True |  |
| `bs_cash_series` | `BS_Segments` | `quarterly_financials.rows.cash` | 0 | 0/1 | True |  |
| `bs_restricted_cash_series` | `BS_Segments` | `quarterly_financials.rows.restricted_cash` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_marketable_securities_series` | `BS_Segments` | `quarterly_financials.rows.marketable_securities` | 0 | 0/1 | True |  |
| `bs_accounts_receivable_series` | `BS_Segments` | `quarterly_financials.rows.accounts_receivable` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_inventory_series` | `BS_Segments` | `quarterly_financials.rows.inventory` | 0 | 0/1 | True |  |
| `bs_current_assets_series` | `BS_Segments` | `quarterly_financials.rows.current_assets` | 0 | 0/1 | True |  |
| `bs_property_plant_equipment_net_series` | `BS_Segments` | `quarterly_financials.rows.property_plant_equipment_net` | 0 | 0/1 | True |  |
| `bs_goodwill_series` | `BS_Segments` | `quarterly_financials.rows.goodwill` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_intangibles_series` | `BS_Segments` | `quarterly_financials.rows.intangibles` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_other_assets_noncurrent_series` | `BS_Segments` | `quarterly_financials.rows.other_assets_noncurrent` | 0 | 0/1 | True |  |
| `bs_total_assets_series` | `BS_Segments` | `quarterly_financials.rows.total_assets` | 0 | 0/1 | True |  |
| `bs_accounts_payable_series` | `BS_Segments` | `quarterly_financials.rows.accounts_payable` | 0 | 0/1 | True |  |
| `bs_accrued_liabilities_series` | `BS_Segments` | `quarterly_financials.rows.accrued_liabilities` | 0 | 0/1 | True |  |
| `bs_short_term_borrowings_series` | `BS_Segments` | `quarterly_financials.rows.short_term_borrowings` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_debt_current_series` | `BS_Segments` | `quarterly_financials.rows.debt_current` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_lease_liabilities_current_series` | `BS_Segments` | `quarterly_financials.rows.lease_liabilities_current` | 0 | 0/1 | True |  |
| `bs_current_liabilities_series` | `BS_Segments` | `quarterly_financials.rows.current_liabilities` | 0 | 0/1 | True |  |
| `bs_debt_core_series` | `BS_Segments` | `quarterly_financials.rows.debt_core` | 0 | 0/1 | True |  |
| `bs_lease_liabilities_noncurrent_series` | `BS_Segments` | `quarterly_financials.rows.lease_liabilities_noncurrent` | 0 | 0/1 | True |  |
| `bs_pension_obligation_net_series` | `BS_Segments` | `quarterly_financials.rows.pension_obligation_net` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_other_liabilities_noncurrent_series` | `BS_Segments` | `quarterly_financials.rows.other_liabilities_noncurrent` | 0 | 0/1 | True |  |
| `bs_total_liabilities_series` | `BS_Segments` | `quarterly_financials.rows.total_liabilities` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_total_equity_series` | `BS_Segments` | `quarterly_financials.rows.total_equity` | 0 | 0/1 | True |  |
| `bs_shares_outstanding_series` | `BS_Segments` | `quarterly_financials.rows.shares_outstanding` | 0 | 0/1 | False | normalized field is absent or not populated |
| `bs_diluted_shares_series` | `BS_Segments` | `quarterly_financials.rows.diluted_shares` | 0 | 0/1 | True |  |
| `bs_annual_period_headers` | `BS_Segments` | `annual_financials.rows.period` | 0 | 0/1 | True |  |
| `bs_annual_revenue_series` | `BS_Segments` | `annual_financials.rows.revenue` | 0 | 0/1 | True |  |
| `valuation_input_as_of` | `Valuation` | `valuation_inputs.as_of_date` | 1 | 1/1 | True |  |
| `valuation_input_shares_outstanding` | `Valuation` | `valuation_inputs.shares_outstanding` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_input_diluted_shares` | `Valuation` | `valuation_inputs.diluted_shares` | 1 | 1/1 | True |  |
| `valuation_input_net_debt` | `Valuation` | `valuation_inputs.net_debt` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_input_base_ebitda_ttm` | `Valuation` | `valuation_inputs.base_ebitda_ttm` | 1 | 1/1 | True |  |
| `valuation_input_adjusted_ebitda_ttm` | `Valuation` | `valuation_inputs.adjusted_ebitda_ttm` | 1 | 1/1 | True |  |
| `valuation_input_fcf_ttm` | `Valuation` | `valuation_inputs.free_cash_flow_ttm` | 1 | 1/1 | True |  |
| `valuation_input_operating_cash_flow_ttm` | `Valuation` | `valuation_inputs.operating_cash_flow_ttm` | 1 | 1/1 | True |  |
| `valuation_input_revenue_ttm` | `Valuation` | `valuation_inputs.revenue_ttm` | 1 | 1/1 | True |  |
| `valuation_input_eps_ttm` | `Valuation` | `valuation_inputs.eps_ttm` | 1 | 1/1 | True |  |
| `valuation_input_adjusted_eps_ttm` | `Valuation` | `valuation_inputs.adjusted_eps_ttm` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_input_book_value_per_share` | `Valuation` | `valuation_inputs.book_value_per_share` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_input_tangible_book_value_per_share` | `Valuation` | `valuation_inputs.tangible_book_value_per_share` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_input_capex_ttm` | `Valuation` | `valuation_inputs.capex_ttm` | 1 | 1/1 | True |  |
| `valuation_input_interest_paid_ttm` | `Valuation` | `valuation_inputs.interest_paid_ttm` | 0 | 0/1 | False | normalized field is absent or not populated |
| `valuation_input_net_income_ttm` | `Valuation` | `valuation_inputs.net_income_ttm` | 1 | 1/1 | True |  |

## Row Schema Observation

Table-row bindings now expose row-schema columns in the JSON binding map. The ANF shadow package populates enough row-shaped data to audit whether future filler output would be useful, without creating an ANF workbook in this pass.
