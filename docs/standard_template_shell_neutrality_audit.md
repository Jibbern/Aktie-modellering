# Standard Template Shell Neutrality Audit

This audit scans the frozen standard shell for visible and retained-hidden content that would make the template company-specific or sector-specific.

- Template: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\standard_stock_model_template.xlsx`
- Generated at: `2026-07-18T00:57:46+00:00`
- Visible sheets: SUMMARY, Valuation, BS_Segments, Operating_Drivers, {ticker}_Investment_Case, Quarter_Notes_UI, Promise_Progress_UI, QA_Log, Needs_Review, QA_Checks
- Retained hidden sheets: REPORT_IS_Q, REPORT_BS_Q, REPORT_CF_Q, Quarter_Notes, Quarter_Notes_Evidence, Quarter_Narrative_Data, Valuation_Summary, Promise_Evidence, Promise_Progress, Guidance_Normalized, History_Q, operating_drivers_raw, DATA_Period_Index, Hidden_Value_Flags, Hidden_Value_Audit, Hidden_Value_Recompute, Hidden_Value_Base, Revolver_History, Debt_Tranches_Latest, Debt_Profile, Debt_Credit_Notes, Leverage_Liquidity, NonGAAP_Credibility, Adjusted_Metrics, NonGAAP_Bridge, {ticker}_Investment_Case_Data, Scenario_Bridge_Tax_Treatment, Scenario_Driver_Assumptions, Debt_Maturity_Ladder, Debt_Buckets, Debt_Recon, Debt_Tranches_Q, Valuation_Grid, Promise_Tracker, Adjustments_Breakdown, OCR_Text_Log

## Post-Neutrality Summary

| Metric | Count |
| --- | ---: |
| `company_specific_value_count` | 0 |
| `company_specific_text_count` | 0 |
| `sector_specific_label_count` | 0 |
| `fixed_dimension_member_count` | 0 |
| `source_specific_text_count` | 0 |
| `valuation_numeric_constant_count` | 0 |
| `signal_fill_without_value_count` | 0 |
| `blank_writable_non_neutral_fill_count` | 0 |
| `visible_blank_gray_fill_count` | 0 |
| `valuation_signal_fill_count` | 0 |
| `blank_status_or_value_fill_count` | 0 |
| `red_green_status_output_count` | 0 |
| `visible_value_date_status_constant_count` | 0 |
| `visible_company_source_text_count` | 0 |
| `missing_required_support_shell_sheet_count` | 0 |
| `uncertain_manual_review_count` | 0 |
| `non_neutral_item_count` | 0 |

## Classification Counts

| Classification | Count |
| --- | ---: |
| `formula_static` | 2200 |
| `generic_block_label` | 613 |
| `row_label_generic` | 585 |
| `universal_template_label` | 464 |

## Remaining Non-Neutral Items

No remaining non-neutral items found.
