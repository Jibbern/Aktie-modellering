# Workbook Block Architecture

This is a read-only block map for the future rich standard template shell. It is generated from the ANF lab workbook, PBI/GPRE cross-check workbooks, the shell manifest, the binding map, and the sheet data-flow map.

It is not a runtime filler and it must not be used to patch or promote ticker workbooks.

## Template Lab

- Lab workbook: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\lab\ANF_template_lab.xlsx`
- Source workbook: `C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\ANF_model.xlsx`
- Byte-identical copy: `True`
- Source SHA256: `ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd`

## Standard vs Optional Sector Packs

The standard shell keeps generic block slots only. Sector/company member names from ANF/PBI/GPRE are evidence for optional packs or clearing, not standard template labels.

| Pack | Status | Example members | Activation rule |
| --- | --- | --- | --- |
| Retail operating pack | optional_not_in_standard_shell | stores, closures, openings, remodels, tariffs, freight, marketing | Future runtime may include only when ticker profile explicitly selects retail/store-footprint drivers. |
| Commodity/ethanol pack | optional_not_in_standard_shell | crush margin, 45Z, RINs, corn, natural gas, oil | Future runtime may include only when ticker profile explicitly selects commodity/ethanol economics. |
| Shipping/mail pack | optional_not_in_standard_shell | Presort, SendTech, USPS, GEC | Future runtime may include only when ticker profile explicitly selects mail/shipping operating drivers. |
| Auto supplier pack | optional_not_in_standard_shell | production, turbo penetration, BEV, OEM mix | Future runtime may include only when ticker profile explicitly selects auto-supplier drivers. |

## Block Summary

| Block | Sheet | Range | Fields | Policy | Standardization |
| --- | --- | --- | --- | --- | --- |
| summary_company_description_value | SUMMARY | A3:F3 | company_profile.business_description | profile-backed | standard |
| summary_strategic_context_value | SUMMARY | A5:F5 | company_profile.strategic_context | profile-backed | standard |
| summary_key_advantage_value | SUMMARY | A7:F7 | company_profile.key_advantages | profile-backed | standard |
| summary_revenue_model_values | SUMMARY | A9:F11 | company_profile.revenue_streams | profile-backed | standard |
| summary_segment_model_values | SUMMARY | A13:F15 | company_profile.operating_model_rows | source-backed | standard |
| summary_key_dependencies_values | SUMMARY | A17:F21 | company_profile.key_dependencies | source-backed | standard |
| summary_wrong_if_values | SUMMARY | A23:F24 | investment_case.invalidators | source-backed | standard |
| summary_latest_period_value | SUMMARY | B26:B26 | quarterly_financials.rows.period | source-backed | standard |
| summary_latest_revenue_value | SUMMARY | B28:B28 | quarterly_financials.rows.revenue | source-backed | standard |
| summary_latest_net_income_value | SUMMARY | B30:B30 | quarterly_financials.rows.net_income | source-backed | standard |
| summary_net_leverage_value | SUMMARY | B41:B41 | debt_liquidity.net_leverage | source-backed | standard |
| summary_revolver_availability_value | SUMMARY | B44:B44 | debt_liquidity.revolver_availability | source-backed | standard |
| summary_liquidity_value | SUMMARY | B45:B45 | debt_liquidity.summary_liquidity_display | source-backed | standard |
| summary_revenue_mix_label | SUMMARY | A8:F8 | company_profile.revenue_mix_label | profile-backed | standard |
| summary_revolver_availability_as_of_value | SUMMARY | D44:F44 | debt_liquidity.summary_liquidity_as_of_display | source-backed | standard |
| summary_liquidity_as_of_value | SUMMARY | D45:F45 | debt_liquidity.summary_liquidity_as_of_display | source-backed | standard |
| valuation_guidance_values | Valuation | O9:T27 | normalized_guidance.items.value | source-backed | standard |
| valuation_guidance_status_values | Valuation | AA9:AA27 | normalized_guidance.items.progress_status | source-backed | standard |
| valuation_guidance_values_lower | Valuation | O29:T36 | normalized_guidance.items.0.value | source-backed | standard |
| valuation_operating_driver_values | Valuation | O39:AA47 | operating_drivers.items.0.driver | source-backed | standard |
| valuation_thesis_bridge_values | Valuation | O51:AA62 | investment_case.key_debate | manual | standard |
| valuation_guidance_status_values_lower | Valuation | X29:AA36 | normalized_guidance.items.0.horizon | source-backed | standard |
| valuation_debt_snapshot_values | Valuation | B124:B130 | debt_liquidity.cash, debt_liquidity.lease_liabilities, debt_liquidity.net_debt, debt_liquidity.net_leverage, +3 more | source-backed | standard |
| valuation_debt_snapshot_periods | Valuation | D124:D130 | debt_liquidity.cash.period, debt_liquidity.lease_liabilities.period, debt_liquidity.net_debt.period, debt_liquidity.net_leverage.period, +3 more | source-backed | standard |
| valuation_debt_snapshot_statuses | Valuation | E124:E130 | debt_liquidity.cash.status, debt_liquidity.lease_liabilities.status, debt_liquidity.net_debt.status, debt_liquidity.net_leverage.status, +3 more | source-backed | standard |
| valuation_debt_snapshot_evidence | Valuation | F124:M130 | debt_liquidity.cash.source_ref, debt_liquidity.lease_liabilities.source_ref, debt_liquidity.net_debt.source_ref, debt_liquidity.net_leverage.source_ref, +3 more | source-backed | standard |
| module_hidden_value_signals_valuation_rows | Valuation | A139:M143 | _derived_workbook.hidden_value.flags_rows | derived | standard |
| valuation_input_values | Valuation | D194:D217 | valuation_inputs.adjusted_ebitda_ttm, valuation_inputs.adjusted_eps_ttm, valuation_inputs.as_of_date, valuation_inputs.base_ebitda_ttm, +12 more | source-backed | standard |
| valuation_period_headers | Valuation | B6:M6 | quarterly_financials.rows.period | source-backed | standard |
| valuation_raw_revenue | Valuation | B9:M9 | quarterly_financials.rows.revenue | source-backed | standard |
| valuation_raw_base_ebitda | Valuation | B18:M18 | quarterly_financials.rows.base_ebitda | source-backed | standard |
| valuation_raw_adjusted_ebitda | Valuation | B24:M24 | quarterly_financials.rows.adjusted_ebitda | source-backed | standard |
| valuation_raw_operating_income | Valuation | B32:M32 | quarterly_financials.rows.operating_income | source-backed | standard |
| valuation_raw_net_income | Valuation | B36:M36 | quarterly_financials.rows.net_income | source-backed | standard |
| valuation_raw_operating_cash_flow | Valuation | B43:M43 | quarterly_financials.rows.operating_cash_flow | source-backed | standard |
| valuation_raw_capital_expenditures | Valuation | B44:M44 | quarterly_financials.rows.capital_expenditures | source-backed | standard |
| valuation_raw_interest_paid | Valuation | B59:M59 | quarterly_financials.rows.interest_paid | source-backed | standard |
| valuation_raw_buybacks_cash | Valuation | B62:M62 | quarterly_financials.rows.buybacks_cash | source-backed | standard |
| valuation_raw_cash | Valuation | B70:M70 | quarterly_financials.rows.cash | source-backed | standard |
| valuation_raw_marketable_securities | Valuation | B71:M71 | quarterly_financials.rows.marketable_securities | source-backed | standard |
| valuation_raw_debt_core | Valuation | B72:M72 | quarterly_financials.rows.debt_core | source-backed | standard |
| valuation_raw_lease_liabilities | Valuation | B79:M79 | quarterly_financials.rows.lease_liabilities | source-backed | standard |
| valuation_raw_pension_obligation_net | Valuation | B82:M82 | quarterly_financials.rows.pension_obligation_net | source-backed | standard |
| valuation_raw_revolver_availability | Valuation | B95:M95 | quarterly_financials.rows.revolver_availability | source-backed | standard |
| valuation_raw_diluted_shares | Valuation | B102:M102 | quarterly_financials.rows.diluted_shares | source-backed | standard |
| valuation_raw_shares_outstanding | Valuation | B103:M103 | quarterly_financials.rows.shares_outstanding | source-backed | standard |
| valuation_raw_eps | Valuation | B107:M107 | quarterly_financials.rows.eps | source-backed | standard |
| valuation_raw_adjusted_eps | Valuation | B110:M110 | quarterly_financials.rows.adjusted_eps | source-backed | standard |
| valuation_raw_gross_profit | Valuation | B262:M262 | quarterly_financials.rows.gross_profit | source-backed | standard |
| valuation_raw_interest_expense | Valuation | B263:M263 | quarterly_financials.rows.interest_expense | source-backed | standard |
| valuation_raw_dividends_cash | Valuation | B264:M264 | quarterly_financials.rows.dividends_cash | source-backed | standard |
| valuation_raw_acquisitions_cash | Valuation | B265:M265 | quarterly_financials.rows.acquisitions_cash | source-backed | standard |
| valuation_raw_debt_repayment | Valuation | B266:M266 | quarterly_financials.rows.debt_repayment | source-backed | standard |
| valuation_raw_debt_issuance | Valuation | B267:M267 | quarterly_financials.rows.debt_issuance | source-backed | standard |
| valuation_raw_total_equity | Valuation | B268:M268 | quarterly_financials.rows.total_equity | source-backed | standard |
| valuation_raw_goodwill | Valuation | B269:M269 | quarterly_financials.rows.goodwill | source-backed | standard |
| valuation_raw_intangibles | Valuation | B270:M270 | quarterly_financials.rows.intangibles | source-backed | standard |
| bs_segment_quarterly_values | BS_Segments | A61:M67 | segments.items.revenue | source-backed | standard |
| bs_annual_period_values | BS_Segments | B70:I71 | annual_financials.rows.period, annual_financials.rows.revenue | source-backed | standard |
| bs_segment_annual_values | BS_Segments | A72:I78 | segments.items.annual_revenue | source-backed | standard |
| bs_quarterly_period_headers | BS_Segments | B7:M7 | quarterly_financials.rows.period | source-backed | standard |
| bs_raw_cash | BS_Segments | B9:M9 | quarterly_financials.rows.cash | source-backed | standard |
| bs_raw_restricted_cash | BS_Segments | B10:M10 | quarterly_financials.rows.restricted_cash | source-backed | standard |
| bs_raw_marketable_securities | BS_Segments | B13:M13 | quarterly_financials.rows.marketable_securities | source-backed | standard |
| bs_raw_accounts_receivable | BS_Segments | B14:M14 | quarterly_financials.rows.accounts_receivable | source-backed | standard |
| bs_raw_inventory | BS_Segments | B15:M15 | quarterly_financials.rows.inventory | source-backed | standard |
| bs_raw_current_assets | BS_Segments | B18:M18 | quarterly_financials.rows.current_assets | source-backed | standard |
| bs_raw_property_plant_equipment_net | BS_Segments | B19:M19 | quarterly_financials.rows.property_plant_equipment_net | source-backed | standard |
| bs_raw_goodwill | BS_Segments | B22:M22 | quarterly_financials.rows.goodwill | source-backed | standard |
| bs_raw_intangibles | BS_Segments | B23:M23 | quarterly_financials.rows.intangibles | source-backed | standard |
| bs_raw_other_assets_noncurrent | BS_Segments | B24:M24 | quarterly_financials.rows.other_assets_noncurrent | source-backed | standard |
| bs_raw_total_assets | BS_Segments | B25:M25 | quarterly_financials.rows.total_assets | source-backed | standard |
| bs_raw_accounts_payable | BS_Segments | B28:M28 | quarterly_financials.rows.accounts_payable | source-backed | standard |
| bs_raw_accrued_liabilities | BS_Segments | B29:M29 | quarterly_financials.rows.accrued_liabilities | source-backed | standard |
| bs_raw_short_term_borrowings | BS_Segments | B32:M32 | quarterly_financials.rows.short_term_borrowings | source-backed | standard |
| bs_raw_debt_current | BS_Segments | B33:M33 | quarterly_financials.rows.debt_current | source-backed | standard |
| bs_raw_lease_liabilities_current | BS_Segments | B34:M34 | quarterly_financials.rows.lease_liabilities_current | source-backed | standard |
| bs_raw_current_liabilities | BS_Segments | B35:M35 | quarterly_financials.rows.current_liabilities | source-backed | standard |
| bs_raw_debt_core | BS_Segments | B40:M40 | quarterly_financials.rows.debt_core | source-backed | standard |
| bs_raw_lease_liabilities_noncurrent | BS_Segments | B42:M42 | quarterly_financials.rows.lease_liabilities_noncurrent | source-backed | standard |
| bs_raw_pension_obligation_net | BS_Segments | B43:M43 | quarterly_financials.rows.pension_obligation_net | source-backed | standard |
| bs_raw_other_liabilities_noncurrent | BS_Segments | B44:M44 | quarterly_financials.rows.other_liabilities_noncurrent | source-backed | standard |
| bs_raw_total_liabilities | BS_Segments | B45:M45 | quarterly_financials.rows.total_liabilities | source-backed | standard |
| bs_raw_total_equity | BS_Segments | B47:M47 | quarterly_financials.rows.total_equity | source-backed | standard |
| bs_raw_shares_outstanding | BS_Segments | B48:M48 | quarterly_financials.rows.shares_outstanding | source-backed | standard |
| bs_raw_diluted_shares | BS_Segments | B49:M49 | quarterly_financials.rows.diluted_shares | source-backed | standard |
| od_watchlist_values | Operating_Drivers | A6:N9 | operating_drivers.items.current_read | source-backed | standard |
| od_topic_current_read_values | Operating_Drivers | B13:N18 | operating_drivers.current_outlook.current_actual_read, operating_drivers.current_outlook.current_actual_use, operating_drivers.current_outlook.current_guidance_read, operating_drivers.current_outlook.current_guidance_use, +3 more | source-backed | standard |
| od_horizon_commentary_values | Operating_Drivers | B20:N30 | operating_drivers.items.0.driver | source-backed | standard |
| od_current_outlook_values | Operating_Drivers | B31:N55 | operating_drivers.items.0.driver | source-backed | standard |
| od_driver_actuals_values | Operating_Drivers | B56:N125 | operating_drivers.items.0.metric_value | source-backed | standard |
| ic_snapshot_values | {ticker}_Investment_Case | B5:B11 | investment_case.current_stance, investment_case.downside_factors, investment_case.key_debate, investment_case.summary, +3 more | mixed | standard |
| ic_lower_comp_history_labels | {ticker}_Investment_Case | A185:K191 | operating_drivers.items.0.driver | source-backed | standard |
| ic_lower_business_health_values | {ticker}_Investment_Case | B194:K199 | operating_drivers.items.0.current_read | source-backed | standard |
| ic_lower_inventory_values | {ticker}_Investment_Case | B202:K209 | operating_drivers.items.0.metric_value | source-backed | standard |
| ic_lower_asset_productivity_values | {ticker}_Investment_Case | B212:K225 | operating_drivers.items.0.metric_value | source-backed | standard |
| ic_lower_guidance_setup_values | {ticker}_Investment_Case | B228:K233 | normalized_guidance.items.0.value | source-backed | standard |
| ic_title_value | {ticker}_Investment_Case | A1:J1 | ticker_metadata.investment_case_title | derived | standard |
| qn_quarter_summary_values | Quarter_Notes_UI | B3:O6 | quarter_notes.summary.key_caveat, quarter_notes.summary.model_read, quarter_notes.summary.watch_next, quarter_notes.summary.what_changed | source-backed | standard |
| qn_quarter_block_values | Quarter_Notes_UI | A10:O15 | quarter_notes.items.commentary | source-backed | standard |
| pp_scorecard_values | Promise_Progress_UI | B5:O11 | normalized_guidance.items.0.metric | source-backed | standard |
| pp_annual_guidance_values | Promise_Progress_UI | A13:I20 | normalized_guidance.items.0.value, promise_progress.items | source-backed | standard |
| pp_annual_guidance_values_block_2 | Promise_Progress_UI | A24:I27 | normalized_guidance.items.0.value, promise_progress.items | source-backed | standard |
| pp_annual_guidance_values_block_3 | Promise_Progress_UI | A30:I32 | normalized_guidance.items.0.value | source-backed | standard |
| pp_annual_guidance_values_block_4 | Promise_Progress_UI | A35:I36 | normalized_guidance.items.0.value | source-backed | standard |
| pp_open_guidance_values | Promise_Progress_UI | A39:E58 | normalized_guidance.items, normalized_guidance.items.0.value | source-backed | standard |
| pp_guidance_revision_primary_values | Promise_Progress_UI | A61:K67 | normalized_guidance.items | source-backed | standard |
| pp_guidance_timeline_values_block_2 | Promise_Progress_UI | B71:O76 | normalized_guidance.items.0.horizon | source-backed | standard |
| pp_guidance_timeline_values_block_3 | Promise_Progress_UI | B78:O84 | normalized_guidance.items.0.horizon | source-backed | standard |
| pp_guidance_timeline_values_block_4 | Promise_Progress_UI | B86:O90 | normalized_guidance.items.0.horizon | source-backed | standard |
| pp_guidance_timeline_values_block_5 | Promise_Progress_UI | B92:O97 | normalized_guidance.items.0.horizon | source-backed | standard |
| pp_guidance_timeline_values_block_6 | Promise_Progress_UI | B99:O115 | normalized_guidance.items.0.horizon | source-backed | standard |
| qa_log_rows | QA_Log | A2:Z5000 | issue_ledger.qa_presentation.qa_log_rows | validation-output | standard |
| needs_review_rows | Needs_Review | A2:Z5000 | issue_ledger.qa_presentation.needs_review_rows | validation-output | standard |
| qa_checks_rows | QA_Checks | A2:Z5000 | issue_ledger.qa_presentation.qa_check_rows | validation-output | standard |

## Sheet Blocks

### SUMMARY

- `summary_company_description_value` `A3:F3`
  - Normalized fields: company_profile.business_description
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave shell label intact; emit mapping gap and Needs_Review row
  - Validation: unexplained_empty_core_field

- `summary_strategic_context_value` `A5:F5`
  - Normalized fields: company_profile.strategic_context
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave shell text blank; emit profile/source gap if promotion asks for full profile
  - Validation: unexplained_empty_core_field

- `summary_key_advantage_value` `A7:F7`
  - Normalized fields: company_profile.key_advantages
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave shell text blank; emit profile/source gap if promotion asks for full profile
  - Validation: unexplained_empty_core_field

- `summary_revenue_model_values` `A9:F11`
  - Normalized fields: company_profile.revenue_streams
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave shell text blank; emit profile/source gap
  - Validation: unexplained_empty_core_field

- `summary_segment_model_values` `A13:F15`
  - Normalized fields: company_profile.operating_model_rows
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank typed operating-model rows until source-backed descriptions exist
  - Validation: unsupported_sector_specific_leakage

- `summary_key_dependencies_values` `A17:F21`
  - Normalized fields: company_profile.key_dependencies
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: optional blank with manual_review flag only if promotion asks for full thesis
  - Validation: unsupported_sector_specific_leakage

- `summary_wrong_if_values` `A23:F24`
  - Normalized fields: investment_case.invalidators
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: optional blank with manual_review flag only if promotion asks for full thesis
  - Validation: placeholder_investment_case

- `summary_latest_period_value` `B26:B26`
  - Normalized fields: quarterly_financials.rows.period
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: unexplained_empty_core_field

- `summary_latest_revenue_value` `B28:B28`
  - Normalized fields: quarterly_financials.rows.revenue
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: unexplained_empty_core_field

- `summary_latest_net_income_value` `B30:B30`
  - Normalized fields: quarterly_financials.rows.net_income
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: unexplained_empty_core_field

- `summary_net_leverage_value` `B41:B41`
  - Normalized fields: debt_liquidity.net_leverage
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: unexplained_empty_core_field

- `summary_revolver_availability_value` `B44:B44`
  - Normalized fields: debt_liquidity.revolver_availability
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

- `summary_liquidity_value` `B45:B45`
  - Normalized fields: debt_liquidity.summary_liquidity_display
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

- `summary_revenue_mix_label` `A8:F8`
  - Normalized fields: company_profile.revenue_mix_label
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

- `summary_revolver_availability_as_of_value` `D44:F44`
  - Normalized fields: debt_liquidity.summary_liquidity_as_of_display
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

- `summary_liquidity_as_of_value` `D45:F45`
  - Normalized fields: debt_liquidity.summary_liquidity_as_of_display
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

### Valuation

- `valuation_guidance_values` `O9:T27`
  - Normalized fields: normalized_guidance.items.value
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank guidance panel rows; emit mapping gaps/manual review flags for missing or parser-conflicted guidance
  - Validation: guidance_metric_misclassification

- `valuation_guidance_status_values` `AA9:AA27`
  - Normalized fields: normalized_guidance.items.progress_status
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank guidance status rows until realized/status evidence exists
  - Validation: boilerplate_guidance

- `valuation_guidance_values_lower` `O29:T36`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank lower guidance panel rows; emit mapping gaps/manual review flags for missing or parser-conflicted guidance
  - Validation: guidance_metric_misclassification

- `valuation_operating_driver_values` `O39:AA47`
  - Normalized fields: operating_drivers.items.0.driver
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank operating-driver sidecar rows until source-backed driver evidence exists
  - Validation: unsupported_sector_specific_leakage

- `valuation_thesis_bridge_values` `O51:AA62`
  - Normalized fields: investment_case.key_debate
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank thesis bridge rows until investment-case evidence is populated
  - Validation: placeholder_investment_case_promotion

- `valuation_guidance_status_values_lower` `X29:AA36`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank lower guidance status rows until realized/status evidence exists
  - Validation: boilerplate_guidance

- `valuation_debt_snapshot_values` `B124:B130`
  - Normalized fields: debt_liquidity.cash, debt_liquidity.lease_liabilities, debt_liquidity.net_debt, debt_liquidity.net_leverage, debt_liquidity.revolver_availability, debt_liquidity.total_debt, debt_liquidity.total_liquidity
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave unsupported debt blank and emit a structured mapping gap | leave unsupported leverage blank and emit a structured mapping gap | leave unsupported net debt blank and emit a structured mapping gap | leave value blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

- `valuation_debt_snapshot_periods` `D124:D130`
  - Normalized fields: debt_liquidity.cash.period, debt_liquidity.lease_liabilities.period, debt_liquidity.net_debt.period, debt_liquidity.net_leverage.period, debt_liquidity.revolver_availability.period, debt_liquidity.total_debt.period, debt_liquidity.total_liquidity.period
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave date blank and emit a structured mapping gap | leave date blank when debt is unavailable | leave date blank when leverage is unavailable | leave date blank when net debt is unavailable
  - Validation: unexplained_empty_core_field

- `valuation_debt_snapshot_statuses` `E124:E130`
  - Normalized fields: debt_liquidity.cash.status, debt_liquidity.lease_liabilities.status, debt_liquidity.net_debt.status, debt_liquidity.net_leverage.status, debt_liquidity.revolver_availability.status, debt_liquidity.total_debt.status, debt_liquidity.total_liquidity.status
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave status blank only when the scalar contract is absent
  - Validation: unexplained_empty_core_field

- `valuation_debt_snapshot_evidence` `F124:M130`
  - Normalized fields: debt_liquidity.cash.source_ref, debt_liquidity.lease_liabilities.source_ref, debt_liquidity.net_debt.source_ref, debt_liquidity.net_leverage.source_ref, debt_liquidity.revolver_availability.source_ref, debt_liquidity.total_debt.source_ref, debt_liquidity.total_liquidity.source_ref
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave evidence blank only when the scalar contract is absent
  - Validation: unexplained_empty_core_field

- `module_hidden_value_signals_valuation_rows` `A139:M143`
  - Normalized fields: _derived_workbook.hidden_value.flags_rows
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: retain the neutral no-trigger empty state
  - Validation: hidden_value_triggered_only_visible_projection

- `valuation_input_values` `D194:D217`
  - Normalized fields: valuation_inputs.adjusted_ebitda_ttm, valuation_inputs.adjusted_eps_ttm, valuation_inputs.as_of_date, valuation_inputs.base_ebitda_ttm, valuation_inputs.book_value_per_share, valuation_inputs.capex_ttm, valuation_inputs.diluted_shares, valuation_inputs.eps_ttm, +8 more
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a structured mapping gap | leave blank and emit a structured mapping gap; point-in-time shares outstanding must not be replaced by diluted weighted-average shares | leave blank and emit a structured mapping gap; scenario EPS must not be reconstructed from a mismatched share denominator
  - Validation: optional_source_backed_scenario_input, unexplained_empty_core_field

- `valuation_period_headers` `B6:M6`
  - Normalized fields: quarterly_financials.rows.period
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave period headers blank and block render
  - Validation: financial_history_mapping_gap

- `valuation_raw_revenue` `B9:M9`
  - Normalized fields: quarterly_financials.rows.revenue
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_base_ebitda` `B18:M18`
  - Normalized fields: quarterly_financials.rows.base_ebitda
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_adjusted_ebitda` `B24:M24`
  - Normalized fields: quarterly_financials.rows.adjusted_ebitda
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_operating_income` `B32:M32`
  - Normalized fields: quarterly_financials.rows.operating_income
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_net_income` `B36:M36`
  - Normalized fields: quarterly_financials.rows.net_income
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_operating_cash_flow` `B43:M43`
  - Normalized fields: quarterly_financials.rows.operating_cash_flow
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_capital_expenditures` `B44:M44`
  - Normalized fields: quarterly_financials.rows.capital_expenditures
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_interest_paid` `B59:M59`
  - Normalized fields: quarterly_financials.rows.interest_paid
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_buybacks_cash` `B62:M62`
  - Normalized fields: quarterly_financials.rows.buybacks_cash
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_cash` `B70:M70`
  - Normalized fields: quarterly_financials.rows.cash
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_marketable_securities` `B71:M71`
  - Normalized fields: quarterly_financials.rows.marketable_securities
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_debt_core` `B72:M72`
  - Normalized fields: quarterly_financials.rows.debt_core
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_lease_liabilities` `B79:M79`
  - Normalized fields: quarterly_financials.rows.lease_liabilities
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_pension_obligation_net` `B82:M82`
  - Normalized fields: quarterly_financials.rows.pension_obligation_net
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_revolver_availability` `B95:M95`
  - Normalized fields: quarterly_financials.rows.revolver_availability
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_diluted_shares` `B102:M102`
  - Normalized fields: quarterly_financials.rows.diluted_shares
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_shares_outstanding` `B103:M103`
  - Normalized fields: quarterly_financials.rows.shares_outstanding
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_eps` `B107:M107`
  - Normalized fields: quarterly_financials.rows.eps
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_adjusted_eps` `B110:M110`
  - Normalized fields: quarterly_financials.rows.adjusted_eps
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_gross_profit` `B262:M262`
  - Normalized fields: quarterly_financials.rows.gross_profit
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_interest_expense` `B263:M263`
  - Normalized fields: quarterly_financials.rows.interest_expense
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_dividends_cash` `B264:M264`
  - Normalized fields: quarterly_financials.rows.dividends_cash
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_acquisitions_cash` `B265:M265`
  - Normalized fields: quarterly_financials.rows.acquisitions_cash
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_debt_repayment` `B266:M266`
  - Normalized fields: quarterly_financials.rows.debt_repayment
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_debt_issuance` `B267:M267`
  - Normalized fields: quarterly_financials.rows.debt_issuance
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_total_equity` `B268:M268`
  - Normalized fields: quarterly_financials.rows.total_equity
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_goodwill` `B269:M269`
  - Normalized fields: quarterly_financials.rows.goodwill
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `valuation_raw_intangibles` `B270:M270`
  - Normalized fields: quarterly_financials.rows.intangibles
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

### BS_Segments

- `bs_segment_quarterly_values` `A61:M67`
  - Normalized fields: segments.items.revenue
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: show no segment row until taxonomy/value exists; emit mapping gap
  - Validation: unsupported_sector_specific_leakage

- `bs_annual_period_values` `B70:I71`
  - Normalized fields: annual_financials.rows.period, annual_financials.rows.revenue
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave annual headers blank and block render | leave annual revenue blank and block render
  - Validation: financial_history_mapping_gap

- `bs_segment_annual_values` `A72:I78`
  - Normalized fields: segments.items.annual_revenue
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: show no annual segment row until source-backed
  - Validation: unsupported_sector_specific_leakage

- `bs_quarterly_period_headers` `B7:M7`
  - Normalized fields: quarterly_financials.rows.period
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave period headers blank and block render
  - Validation: financial_history_mapping_gap

- `bs_raw_cash` `B9:M9`
  - Normalized fields: quarterly_financials.rows.cash
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_restricted_cash` `B10:M10`
  - Normalized fields: quarterly_financials.rows.restricted_cash
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_marketable_securities` `B13:M13`
  - Normalized fields: quarterly_financials.rows.marketable_securities
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_accounts_receivable` `B14:M14`
  - Normalized fields: quarterly_financials.rows.accounts_receivable
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_inventory` `B15:M15`
  - Normalized fields: quarterly_financials.rows.inventory
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_current_assets` `B18:M18`
  - Normalized fields: quarterly_financials.rows.current_assets
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_property_plant_equipment_net` `B19:M19`
  - Normalized fields: quarterly_financials.rows.property_plant_equipment_net
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_goodwill` `B22:M22`
  - Normalized fields: quarterly_financials.rows.goodwill
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_intangibles` `B23:M23`
  - Normalized fields: quarterly_financials.rows.intangibles
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_other_assets_noncurrent` `B24:M24`
  - Normalized fields: quarterly_financials.rows.other_assets_noncurrent
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_total_assets` `B25:M25`
  - Normalized fields: quarterly_financials.rows.total_assets
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_accounts_payable` `B28:M28`
  - Normalized fields: quarterly_financials.rows.accounts_payable
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_accrued_liabilities` `B29:M29`
  - Normalized fields: quarterly_financials.rows.accrued_liabilities
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_short_term_borrowings` `B32:M32`
  - Normalized fields: quarterly_financials.rows.short_term_borrowings
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_debt_current` `B33:M33`
  - Normalized fields: quarterly_financials.rows.debt_current
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_lease_liabilities_current` `B34:M34`
  - Normalized fields: quarterly_financials.rows.lease_liabilities_current
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_current_liabilities` `B35:M35`
  - Normalized fields: quarterly_financials.rows.current_liabilities
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_debt_core` `B40:M40`
  - Normalized fields: quarterly_financials.rows.debt_core
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_lease_liabilities_noncurrent` `B42:M42`
  - Normalized fields: quarterly_financials.rows.lease_liabilities_noncurrent
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_pension_obligation_net` `B43:M43`
  - Normalized fields: quarterly_financials.rows.pension_obligation_net
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_other_liabilities_noncurrent` `B44:M44`
  - Normalized fields: quarterly_financials.rows.other_liabilities_noncurrent
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_total_liabilities` `B45:M45`
  - Normalized fields: quarterly_financials.rows.total_liabilities
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_total_equity` `B47:M47`
  - Normalized fields: quarterly_financials.rows.total_equity
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_shares_outstanding` `B48:M48`
  - Normalized fields: quarterly_financials.rows.shares_outstanding
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

- `bs_raw_diluted_shares` `B49:M49`
  - Normalized fields: quarterly_financials.rows.diluted_shares
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: leave missing periods blank and emit a structured mapping gap
  - Validation: financial_history_mapping_gap

### Operating_Drivers

- `od_watchlist_values` `A6:N9`
  - Normalized fields: operating_drivers.items.current_read
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: do not use sector fallback; emit manual review flag
  - Validation: unsupported_sector_specific_leakage

- `od_topic_current_read_values` `B13:N18`
  - Normalized fields: operating_drivers.current_outlook.current_actual_read, operating_drivers.current_outlook.current_actual_use, operating_drivers.current_outlook.current_guidance_read, operating_drivers.current_outlook.current_guidance_use, operating_drivers.current_outlook.margin_bridge_read, operating_drivers.current_outlook.margin_bridge_use, operating_drivers.items.0.current_read
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: do not use sector fallback; emit manual review flag | leave blank with explicit review disposition
  - Validation: unsupported_sector_specific_leakage, visible_narrative_missing_evidence_refs

- `od_horizon_commentary_values` `B20:N30`
  - Normalized fields: operating_drivers.items.0.driver
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: blank visible row family; emit mapping gap
  - Validation: parser_noise_snippet

- `od_current_outlook_values` `B31:N55`
  - Normalized fields: operating_drivers.items.0.driver
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: blank visible row family; emit mapping gap
  - Validation: parser_noise_snippet

- `od_driver_actuals_values` `B56:N125`
  - Normalized fields: operating_drivers.items.0.metric_value
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: optional blank until driver/value pair is source-backed
  - Validation: parser_noise_snippet

### {ticker}_Investment_Case

- `ic_snapshot_values` `B5:B11`
  - Normalized fields: investment_case.current_stance, investment_case.downside_factors, investment_case.key_debate, investment_case.summary, investment_case.upside_factors, investment_case.watch_next, investment_case.why_it_can_work
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: block promotion; emit manual review flag | leave blank and retain a structured review disposition
  - Validation: placeholder_investment_case, visible_narrative_missing_evidence_refs

- `ic_lower_comp_history_labels` `A185:K191`
  - Normalized fields: operating_drivers.items.0.driver
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: blank lower comparison period labels until source-backed history exists
  - Validation: unsupported_sector_specific_leakage

- `ic_lower_business_health_values` `B194:K199`
  - Normalized fields: operating_drivers.items.0.current_read
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: blank optional lower block until source-backed brand/driver data exists
  - Validation: unsupported_sector_specific_leakage

- `ic_lower_inventory_values` `B202:K209`
  - Normalized fields: operating_drivers.items.0.metric_value
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: blank optional lower block until source-backed inventory/driver data exists
  - Validation: unsupported_sector_specific_leakage

- `ic_lower_asset_productivity_values` `B212:K225`
  - Normalized fields: operating_drivers.items.0.metric_value
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: blank optional lower block until source-backed store/driver data exists
  - Validation: unsupported_sector_specific_leakage

- `ic_lower_guidance_setup_values` `B228:K233`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: blank optional guidance setup until source-backed guidance exists
  - Validation: guidance_metric_misclassification

- `ic_title_value` `A1:J1`
  - Normalized fields: ticker_metadata.investment_case_title
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: leave blank and emit a structured mapping gap
  - Validation: unexplained_empty_core_field

### Quarter_Notes_UI

- `qn_quarter_summary_values` `B3:O6`
  - Normalized fields: quarter_notes.summary.key_caveat, quarter_notes.summary.model_read, quarter_notes.summary.watch_next, quarter_notes.summary.what_changed
  - Support sheets: Guidance_Normalized, Quarter_Narrative_Data, Quarter_Notes, Quarter_Notes_Evidence
  - Current owner: pbi_xbrl/excel_writer_quarter_notes_ui_orchestrator.py, pbi_xbrl/excel_writer_quarter_notes_ui_*, pbi_xbrl/quarter_notes.py
  - Future owner: future quarter_notes normalizer, normalized_company_data_validation.py
  - Missing data: blank summary rows until source-backed quarter narrative exists | leave blank with explicit review disposition
  - Validation: visible_narrative_missing_evidence_refs

- `qn_quarter_block_values` `A10:O15`
  - Normalized fields: quarter_notes.items.commentary
  - Support sheets: Guidance_Normalized, Quarter_Narrative_Data, Quarter_Notes, Quarter_Notes_Evidence
  - Current owner: pbi_xbrl/excel_writer_quarter_notes_ui_orchestrator.py, pbi_xbrl/excel_writer_quarter_notes_ui_*, pbi_xbrl/quarter_notes.py
  - Future owner: future quarter_notes normalizer, normalized_company_data_validation.py
  - Missing data: omit quarter block content; emit manual review flag
  - Validation: parser_noise_snippet

### Promise_Progress_UI

- `pp_scorecard_values` `B5:O11`
  - Normalized fields: normalized_guidance.items.0.metric
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible scorecard row until metric/value/source are clean
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values` `A13:I20`
  - Normalized fields: normalized_guidance.items.0.value, promise_progress.items
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: leave unsupported progression cells blank and retain exact review detail | no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values_block_2` `A24:I27`
  - Normalized fields: normalized_guidance.items.0.value, promise_progress.items
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: leave unsupported progression cells blank and retain exact review detail | no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values_block_3` `A30:I32`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values_block_4` `A35:I36`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_open_guidance_values` `A39:E58`
  - Normalized fields: normalized_guidance.items, normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: leave unsupported cells blank and retain exact review detail | no visible guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_guidance_revision_primary_values` `A61:K67`
  - Normalized fields: normalized_guidance.items
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: optional blank until source-backed guidance timeline exists
  - Validation: boilerplate_guidance

- `pp_guidance_timeline_values_block_2` `B71:O76`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: optional blank until source-backed guidance timeline exists
  - Validation: boilerplate_guidance

- `pp_guidance_timeline_values_block_3` `B78:O84`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: optional blank until source-backed guidance timeline exists
  - Validation: boilerplate_guidance

- `pp_guidance_timeline_values_block_4` `B86:O90`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: optional blank until source-backed guidance timeline exists
  - Validation: boilerplate_guidance

- `pp_guidance_timeline_values_block_5` `B92:O97`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: optional blank until source-backed guidance timeline exists
  - Validation: boilerplate_guidance

- `pp_guidance_timeline_values_block_6` `B99:O115`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: optional blank until source-backed guidance timeline exists
  - Validation: boilerplate_guidance

### QA_Log

- `qa_log_rows` `A2:Z5000`
  - Normalized fields: issue_ledger.qa_presentation.qa_log_rows
  - Support sheets: Info_Log, OCR_Text_Log, Promise_Evidence, Quarter_Notes_Evidence, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_core.py, pbi_xbrl/writer_qa_policy.py
  - Future owner: pbi_xbrl/normalized_company_data_validation.py, future filler QA writer
  - Missing data: write no rows only when the canonical issue ledger is empty
  - Validation: canonical_issue_ledger_summary_shape

### Needs_Review

- `needs_review_rows` `A2:Z5000`
  - Normalized fields: issue_ledger.qa_presentation.needs_review_rows
  - Support sheets: QA_Log
  - Current owner: pbi_xbrl/excel_writer_core.py, pbi_xbrl/writer_qa_policy.py, pbi_xbrl/pipeline_qa.py
  - Future owner: pbi_xbrl/normalized_company_data_validation.py, future filler QA writer
  - Missing data: write no rows when no unresolved actionable issue exists
  - Validation: canonical_issue_ledger_actionable_filter

### QA_Checks

- `qa_checks_rows` `A2:Z5000`
  - Normalized fields: issue_ledger.qa_presentation.qa_check_rows
  - Support sheets: DATA_Facts_Long, Guidance_Raw, QA_Log, SEC_Audit_Log, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_core.py, pbi_xbrl/workbook_validation_runner.py, pbi_xbrl/workbook_quality_guardrails.py
  - Future owner: docs/workbook_binding_map.json, pbi_xbrl/normalized_company_data_validation.py, future filler QA writer
  - Missing data: write no rows only when the canonical issue ledger has no checks
  - Validation: canonical_issue_ledger_rule_check_shape
