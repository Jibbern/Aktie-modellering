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
| summary_segment_model_values | SUMMARY | A13:F15 | segments.items.0.revenue | source-backed | standard |
| summary_key_dependencies_values | SUMMARY | A17:F21 | company_profile.key_risks | profile-backed | standard |
| summary_wrong_if_values | SUMMARY | A23:F24 | investment_case.key_debate | manual | standard |
| summary_key_financials_values | SUMMARY | B26:B39 | quarterly_financials.rows.0.net_income, quarterly_financials.rows.0.period, quarterly_financials.rows.0.revenue | source-backed | standard |
| summary_leverage_liquidity_values | SUMMARY | B41:B45 | debt_liquidity.net_leverage, debt_liquidity.summary_liquidity_display | source-backed | standard |
| valuation_actuals_quarterly_values | Valuation | B6:M80 | quarterly_financials.rows.0.adjusted_ebitda, quarterly_financials.rows.0.free_cash_flow, quarterly_financials.rows.0.net_income, quarterly_financials.rows.0.operating_cash_flow, +3 more | source-backed | standard |
| valuation_guidance_values | Valuation | O9:T27 | normalized_guidance.items.value | source-backed | standard |
| valuation_guidance_status_values | Valuation | AA9:AA27 | normalized_guidance.items.progress_status | source-backed | standard |
| valuation_guidance_values_lower | Valuation | O29:T36 | normalized_guidance.items.0.value | source-backed | standard |
| valuation_operating_driver_values | Valuation | O39:AA47 | operating_drivers.items.0.driver | source-backed | standard |
| valuation_thesis_bridge_values | Valuation | O51:AA62 | investment_case.key_debate | manual | standard |
| valuation_output_values | Valuation | O64:AA170 | valuation_outputs.items.value | derived | standard |
| valuation_guidance_status_values_lower | Valuation | X29:AA36 | normalized_guidance.items.0.horizon | source-backed | standard |
| valuation_share_count_values | Valuation | B81:M90 | quarterly_financials.rows.0.diluted_shares | source-backed | standard |
| valuation_debt_liquidity_values | Valuation | B124:M132 | debt_liquidity.total_debt | source-backed | standard |
| valuation_cash_values | Valuation | B133:M137 | debt_liquidity.cash | source-backed | standard |
| valuation_capital_return_values | Valuation | B152:M156 | capital_returns.buybacks | source-backed | standard |
| valuation_input_values | Valuation | D194:D216 | valuation_inputs.adjusted_ebitda_ttm, valuation_inputs.as_of_date, valuation_inputs.base_ebitda_ttm, valuation_inputs.capex_ttm, +5 more | mixed | standard |
| bs_liquidity_values | BS_Segments | B7:I49 | debt_liquidity.cash, debt_liquidity.total_debt, quarterly_financials.rows.period | source-backed | standard |
| bs_segment_quarterly_values | BS_Segments | A61:I67 | segments.items.revenue | source-backed | standard |
| bs_annual_period_values | BS_Segments | B70:I71 | annual_financials.rows.period, annual_financials.rows.revenue | source-backed | standard |
| bs_segment_annual_values | BS_Segments | A72:I74 | segments.items.annual_revenue | source-backed | standard |
| od_watchlist_values | Operating_Drivers | A6:N9 | operating_drivers.items.current_read | source-backed | standard |
| od_topic_current_read_values | Operating_Drivers | B13:N18 | operating_drivers.items.0.current_read | source-backed | standard |
| od_horizon_commentary_values | Operating_Drivers | B20:N30 | operating_drivers.items.0.driver | source-backed | standard |
| od_current_outlook_values | Operating_Drivers | B31:N55 | operating_drivers.items.0.driver | source-backed | standard |
| od_driver_actuals_values | Operating_Drivers | B56:N125 | operating_drivers.items.0.metric_value | source-backed | standard |
| ic_snapshot_values | {ticker}_Investment_Case | B5:K30 | investment_case.key_debate, investment_case.summary | manual | standard |
| ic_manual_input_values | {ticker}_Investment_Case | B81:K130 | investment_case.scenario_drivers | manual | standard |
| ic_scenario_bridge_values | {ticker}_Investment_Case | B131:K190 | segments.items.0.revenue | derived | standard |
| ic_lower_comp_history_labels | {ticker}_Investment_Case | A185:K191 | operating_drivers.items.0.driver | source-backed | standard |
| ic_lower_business_health_values | {ticker}_Investment_Case | B194:K199 | operating_drivers.items.0.current_read | source-backed | standard |
| ic_lower_inventory_values | {ticker}_Investment_Case | B202:K209 | operating_drivers.items.0.metric_value | source-backed | standard |
| ic_lower_asset_productivity_values | {ticker}_Investment_Case | B212:K225 | operating_drivers.items.0.metric_value | source-backed | standard |
| ic_lower_guidance_setup_values | {ticker}_Investment_Case | B228:K233 | normalized_guidance.items.0.value | source-backed | standard |
| qn_quarter_summary_values | Quarter_Notes_UI | B3:O6 | quarter_notes.items.commentary | source-backed | standard |
| qn_quarter_block_values | Quarter_Notes_UI | A10:O15 | quarter_notes.items.commentary | source-backed | standard |
| pp_scorecard_values | Promise_Progress_UI | B5:O11 | normalized_guidance.items.0.metric | source-backed | standard |
| pp_annual_guidance_values | Promise_Progress_UI | B13:O22 | normalized_guidance.items.0.value | source-backed | standard |
| pp_annual_guidance_values_block_2 | Promise_Progress_UI | B24:O28 | normalized_guidance.items.0.value | source-backed | standard |
| pp_annual_guidance_values_block_3 | Promise_Progress_UI | B30:O33 | normalized_guidance.items.0.value | source-backed | standard |
| pp_annual_guidance_values_block_4 | Promise_Progress_UI | B35:O35 | normalized_guidance.items.0.value | source-backed | standard |
| pp_open_guidance_values | Promise_Progress_UI | B39:O59 | normalized_guidance.items.0.value | source-backed | standard |
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
  - Normalized fields: segments.items.0.revenue
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank segment rows until source-backed segment labels and values exist
  - Validation: unsupported_sector_specific_leakage

- `summary_key_dependencies_values` `A17:F21`
  - Normalized fields: company_profile.key_risks
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: optional blank with manual_review flag only if promotion asks for full thesis
  - Validation: unsupported_sector_specific_leakage

- `summary_wrong_if_values` `A23:F24`
  - Normalized fields: investment_case.key_debate
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: optional blank with manual_review flag only if promotion asks for full thesis
  - Validation: placeholder_investment_case

- `summary_key_financials_values` `B26:B39`
  - Normalized fields: quarterly_financials.rows.0.net_income, quarterly_financials.rows.0.period, quarterly_financials.rows.0.revenue
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: unexplained_empty_core_field

- `summary_leverage_liquidity_values` `B41:B45`
  - Normalized fields: debt_liquidity.net_leverage, debt_liquidity.summary_liquidity_display
  - Support sheets: Debt_Tranches_Q, History_Q, Leverage_Liquidity, SEC_Audit_Log
  - Current owner: pbi_xbrl/excel_writer_summary_builder.py, pbi_xbrl/excel_writer_summary_sheet.py, pbi_xbrl/summary_overview.py
  - Future owner: future normalized_company_data_builder, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: unexplained_empty_core_field

### Valuation

- `valuation_actuals_quarterly_values` `B6:M80`
  - Normalized fields: quarterly_financials.rows.0.adjusted_ebitda, quarterly_financials.rows.0.free_cash_flow, quarterly_financials.rows.0.net_income, quarterly_financials.rows.0.operating_cash_flow, quarterly_financials.rows.0.operating_income, quarterly_financials.rows.0.period, quarterly_financials.rows.0.revenue
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit valuation mapping gap | do not infer from profile; emit valuation mapping gap | optional blank with missing_source reason; do not proxy without review
  - Validation: unexplained_empty_core_field, valuation_core_mapping_gap

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

- `valuation_output_values` `O64:AA170`
  - Normalized fields: valuation_outputs.items.value
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank output rows until required valuation inputs are mapped and validated
  - Validation: valuation_output_contract

- `valuation_guidance_status_values_lower` `X29:AA36`
  - Normalized fields: normalized_guidance.items.0.horizon
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank lower guidance status rows until realized/status evidence exists
  - Validation: boilerplate_guidance

- `valuation_share_count_values` `B81:M90`
  - Normalized fields: quarterly_financials.rows.0.diluted_shares
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: share_count_outlier

- `valuation_debt_liquidity_values` `B124:M132`
  - Normalized fields: debt_liquidity.total_debt
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: valuation_core_mapping_gap

- `valuation_cash_values` `B133:M137`
  - Normalized fields: debt_liquidity.cash
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: blank values; emit mapping gap
  - Validation: valuation_core_mapping_gap

- `valuation_capital_return_values` `B152:M156`
  - Normalized fields: capital_returns.buybacks
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: optional blank with not_applicable or missing_source reason
  - Validation: unexplained_empty_core_field

- `valuation_input_values` `D194:D216`
  - Normalized fields: valuation_inputs.adjusted_ebitda_ttm, valuation_inputs.as_of_date, valuation_inputs.base_ebitda_ttm, valuation_inputs.capex_ttm, valuation_inputs.diluted_shares, valuation_inputs.free_cash_flow_ttm, valuation_inputs.net_debt, valuation_inputs.revenue_ttm, +1 more
  - Support sheets: Debt_Profile, Debt_Tranches_Q, Guidance_Normalized, Hidden_Value_Base, Hidden_Value_Flags, History_Q, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_valuation_orchestrator.py, pbi_xbrl/excel_writer_valuation_*, pbi_xbrl/valuation.py
  - Future owner: frozen template shell, docs/workbook_binding_map.json, future value-only filler
  - Missing data: leave blank and emit a P1 mapping gap | leave blank and emit a mapping gap
  - Validation: share_count_outlier, valuation_core_mapping_gap

### BS_Segments

- `bs_liquidity_values` `B7:I49`
  - Normalized fields: debt_liquidity.cash, debt_liquidity.total_debt, quarterly_financials.rows.period
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: blank values; emit mapping gap | leave quarterly period headers blank and emit a P1 mapping gap
  - Validation: unexplained_empty_core_field

- `bs_segment_quarterly_values` `A61:I67`
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
  - Missing data: leave annual period headers blank and emit a P1 mapping gap | leave annual revenue values blank and emit a P1 mapping gap
  - Validation: unexplained_empty_core_field

- `bs_segment_annual_values` `A72:I74`
  - Normalized fields: segments.items.annual_revenue
  - Support sheets: DATA_Facts_Long, Debt_Tranches_Q, History_Q, Slides_Segments
  - Current owner: pbi_xbrl/excel_writer_bs_segments.py, pbi_xbrl/excel_writer_segment_sources.py
  - Future owner: future segments normalizer, docs/workbook_binding_map.json
  - Missing data: show no annual segment row until source-backed
  - Validation: unsupported_sector_specific_leakage

### Operating_Drivers

- `od_watchlist_values` `A6:N9`
  - Normalized fields: operating_drivers.items.current_read
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: do not use sector fallback; emit manual review flag
  - Validation: unsupported_sector_specific_leakage

- `od_topic_current_read_values` `B13:N18`
  - Normalized fields: operating_drivers.items.0.current_read
  - Support sheets: History_Q, Quarter_Notes, Slides_Segments, operating_drivers_raw
  - Current owner: pbi_xbrl/excel_writer_drivers.py, pbi_xbrl/operating_drivers_runtime.py, pbi_xbrl/excel_writer_operating_drivers.py
  - Future owner: future operating_drivers normalizer, normalized_company_data_validation.py
  - Missing data: do not use sector fallback; emit manual review flag
  - Validation: unsupported_sector_specific_leakage

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

- `ic_snapshot_values` `B5:K30`
  - Normalized fields: investment_case.key_debate, investment_case.summary
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: block promotion; emit manual review flag
  - Validation: placeholder_investment_case

- `ic_manual_input_values` `B81:K130`
  - Normalized fields: investment_case.scenario_drivers
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: keep manual shell inputs blank; emit review flag if promotion requires scenario detail
  - Validation: placeholder_investment_case

- `ic_scenario_bridge_values` `B131:K190`
  - Normalized fields: segments.items.0.revenue
  - Support sheets: Guidance_Normalized, History_Q, Promise_Progress, Quarter_Notes, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Slides_Guidance, Slides_Segments, +1 more
  - Current owner: pbi_xbrl/excel_writer_sector_investment_case.py, pbi_xbrl/excel_writer_investment_case_support.py, pbi_xbrl/excel_writer_anf_investment_case.py
  - Future owner: future investment_case normalizer/review workflow, docs/workbook_binding_map.json
  - Missing data: do not fabricate segment bridge rows; emit mapping gap only if promotion requires it
  - Validation: valuation_core_mapping_gap

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

### Quarter_Notes_UI

- `qn_quarter_summary_values` `B3:O6`
  - Normalized fields: quarter_notes.items.commentary
  - Support sheets: Guidance_Normalized, Quarter_Narrative_Data, Quarter_Notes, Quarter_Notes_Evidence
  - Current owner: pbi_xbrl/excel_writer_quarter_notes_ui_orchestrator.py, pbi_xbrl/excel_writer_quarter_notes_ui_*, pbi_xbrl/quarter_notes.py
  - Future owner: future quarter_notes normalizer, normalized_company_data_validation.py
  - Missing data: blank summary rows until source-backed quarter narrative exists
  - Validation: parser_noise_snippet

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

- `pp_annual_guidance_values` `B13:O22`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values_block_2` `B24:O28`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values_block_3` `B30:O33`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_annual_guidance_values_block_4` `B35:O35`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible annual guidance row; emit parser_conflict/manual review flag
  - Validation: guidance_metric_misclassification

- `pp_open_guidance_values` `B39:O59`
  - Normalized fields: normalized_guidance.items.0.value
  - Support sheets: Guidance_Normalized, Guidance_Raw, Promise_Evidence, Promise_Progress, Slides_Guidance
  - Current owner: pbi_xbrl/excel_writer_promise_progress.py, pbi_xbrl/excel_writer_promise_progress_*, pbi_xbrl/doc_intel.py
  - Future owner: future normalized_guidance builder, normalized_company_data_validation.py
  - Missing data: no visible guidance row; emit parser_conflict/manual review flag
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
