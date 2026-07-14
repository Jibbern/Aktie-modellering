# Standard Template Shell Neutrality Audit

This audit scans the frozen standard shell for visible and retained-hidden content that would make the template company-specific or sector-specific.

- Template: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\standard_stock_model_template.xlsx`
- Generated at: `2026-07-14T16:44:07+00:00`
- Visible sheets: SUMMARY, Valuation, BS_Segments, Operating_Drivers, {ticker}_Investment_Case, Quarter_Notes_UI, Promise_Progress_UI, QA_Log, Needs_Review, QA_Checks
- Retained hidden sheets: Hidden_Value_Flags, Revolver_History, Debt_Tranches_Latest, Debt_Profile, Quarter_Notes, Promise_Progress, Guidance_Normalized, History_Q

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
| `formula_static` | 1016 |
| `generic_block_label` | 535 |
| `row_label_generic` | 639 |
| `universal_template_label` | 75 |

## Remaining Non-Neutral Items

No remaining non-neutral items found.
