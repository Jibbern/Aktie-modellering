# Standard Template Hidden Support Audit

Generated at: 2026-07-14T16:45:00+00:00
Template: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\standard_stock_model_template.xlsx`
ANF lab source: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\lab\ANF_template_lab.xlsx`

## Summary

- Candidate hidden/support sheets from lab or shell: `47`
- Company/source leakage cells before neutralization: `9015`
- Hidden sheets retained in shell: `8`
- Company/source leakage cells after neutralization: `0`
- Missing visible formula sheet refs: `0`
- Missing defined-name sheet refs: `0`

## Hidden Support Sheets

| Sheet | Present | Classification | Non-empty | Formulas | Tables | Leakage | Reason |
|---|---:|---|---:|---:|---:|---:|---|
| ANF_Investment_Case_Data | no | delete_from_shell | 1043 | 8 | 0 | 110 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Adjusted_Metrics | no | delete_from_shell | 412 | 0 | 1 | 99 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Adjustments_Breakdown | no | delete_from_shell | 1 | 0 | 0 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| DATA_Facts_Long | no | delete_from_shell | 30874 | 0 | 1 | 162 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| DATA_IS_Rules | no | delete_from_shell | 32 | 0 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| DATA_LineItem_Map | no | delete_from_shell | 139 | 0 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| DATA_Period_Index | no | delete_from_shell | 39 | 0 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Debt_Buckets | no | delete_from_shell | 1 | 0 | 0 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| Debt_Credit_Notes | no | delete_from_shell | 572 | 0 | 1 | 61 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Debt_Maturity_Ladder | no | delete_from_shell | 1 | 0 | 0 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| Debt_Profile | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Neutral debt profile shell retained with headers only for valuation/liquidity workflows. |
| Debt_Recon | no | delete_from_shell | 1 | 0 | 0 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| Debt_Tranches_Latest | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Neutral debt-tranche support shell retained with headers only; runtime fills rows from normalized debt_liquidity. |
| Debt_Tranches_Q | no | delete_from_shell | 1 | 0 | 0 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| Guidance_Normalized | yes | keep_neutral_helper_shell | 8 | 0 | 0 | 0 | Neutral guidance support shell retained with headers only; normalized_guidance owns future values. |
| Guidance_Raw | no | delete_from_shell | 5410 | 0 | 1 | 531 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Hidden_Value_Audit | no | delete_from_shell | 104 | 11 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Hidden_Value_Base | no | delete_from_shell | 1515 | 0 | 1 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| Hidden_Value_Flags | yes | keep_formula_dependency | 12 | 0 | 0 | 0 | Valuation!AI139 uses Hidden_Value_Flags!L2:L100 as a neutral hidden-value flag lookup helper. |
| Hidden_Value_Recompute | no | delete_from_shell | 44 | 0 | 1 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| History_Q | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Neutral quarterly history support shell retained with headers only; runtime fills from quarterly_financials. |
| Info_Log | no | delete_from_shell | 4367 | 0 | 1 | 713 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Leverage_Liquidity | no | delete_from_shell | 638 | 0 | 1 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| NonGAAP_Bridge | no | delete_from_shell | 123 | 0 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| NonGAAP_Credibility | no | delete_from_shell | 689 | 0 | 1 | 84 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| NonGAAP_Files | no | delete_from_shell | 1218 | 0 | 1 | 118 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| OCR_Text_Log | no | delete_from_shell | 1 | 0 | 0 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Promise_Evidence | no | delete_from_shell | 1130 | 0 | 1 | 281 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Promise_Progress | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Neutral promise-progress support shell retained with headers only; runtime fills from normalized_guidance evidence. |
| Promise_Tracker | no | delete_from_shell | 1 | 0 | 0 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Quarter_Narrative_Data | no | delete_from_shell | 1720 | 0 | 0 | 121 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Quarter_Notes | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Neutral quarter-note support shell retained with headers only; runtime fills from quarter_notes. |
| Quarter_Notes_Audit | no | delete_from_shell | 7568 | 0 | 0 | 650 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Quarter_Notes_Evidence | no | delete_from_shell | 5440 | 0 | 1 | 87 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| REPORT_BS_Q | no | delete_from_shell | 100 | 0 | 0 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| REPORT_CF_Q | no | delete_from_shell | 124 | 0 | 0 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| REPORT_IS_Q | no | delete_from_shell | 235 | 0 | 0 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Revolver_History | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Neutral debt/liquidity support shell retained with headers only; runtime fills rows from normalized debt_liquidity. |
| SEC_Audit_Log | no | delete_from_shell | 25779 | 0 | 1 | 162 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Scenario_Bridge_Tax_Treatment | no | delete_from_shell | 37 | 0 | 0 | 5 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Scenario_Driver_Assumptions | no | delete_from_shell | 78 | 25 | 0 | 7 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Slides_Debt_Profile | no | delete_from_shell | 92 | 0 | 1 | 9 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Slides_Guidance | no | delete_from_shell | 3239 | 0 | 1 | 196 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Slides_Segments | no | delete_from_shell | 8951 | 0 | 1 | 2213 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Valuation_Grid | no | delete_from_shell | 1 | 0 | 0 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| Valuation_Summary | no | delete_from_shell | 82 | 0 | 1 | 0 | Unreferenced hidden lab sheet is excluded from the frozen neutral shell. |
| operating_drivers_raw | no | delete_from_shell | 7606 | 0 | 0 | 530 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
