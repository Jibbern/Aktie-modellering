# Standard Template Hidden Support Audit

Generated at: 2026-07-18T00:58:31+00:00
Template: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\standard_stock_model_template.xlsx`
ANF lab source: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\lab\ANF_template_lab.xlsx`

## Summary

- Candidate hidden/support sheets from lab or shell: `48`
- Company/source leakage cells before neutralization: `9015`
- Hidden sheets retained in shell: `36`
- Company/source leakage cells after neutralization: `0`
- Missing visible formula sheet refs: `0`
- Missing defined-name sheet refs: `0`

## Hidden Support Sheets

| Sheet | Present | Classification | Non-empty | Formulas | Tables | Leakage | Reason |
|---|---:|---|---:|---:|---:|---:|---|
| ANF_Investment_Case_Data | no | delete_from_shell | 1043 | 8 | 0 | 110 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Adjusted_Metrics | yes | keep_optional_runtime_output_shell | 7 | 0 | 0 | 0 | Header-only until source-backed adjusted metrics exist. |
| Adjustments_Breakdown | yes | keep_optional_runtime_output_shell | 9 | 0 | 0 | 0 | Reserved hidden capacity; no fabricated bridge rows. |
| DATA_Facts_Long | no | delete_from_shell | 30874 | 0 | 1 | 162 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| DATA_IS_Rules | no | delete_from_shell | 32 | 0 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| DATA_LineItem_Map | no | delete_from_shell | 139 | 0 | 1 | 0 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| DATA_Period_Index | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Header-only period index; full raw period detail remains external. |
| Debt_Buckets | yes | keep_optional_runtime_output_shell | 6 | 0 | 0 | 0 | Reserved hidden capacity for maturity buckets. |
| Debt_Credit_Notes | yes | keep_optional_runtime_output_shell | 6 | 0 | 0 | 0 | Header-only until reliable credit evidence exists. |
| Debt_Maturity_Ladder | yes | keep_optional_runtime_output_shell | 7 | 0 | 0 | 0 | Reserved hidden capacity; no user-facing ladder is shown without evidence. |
| Debt_Profile | yes | keep_optional_runtime_output_shell | 6 | 0 | 0 | 0 | Header-only until source-backed debt or liquidity facts exist. |
| Debt_Recon | yes | keep_optional_runtime_output_shell | 8 | 0 | 0 | 0 | Reserved hidden capacity for a source-backed debt roll-forward. |
| Debt_Tranches_Latest | yes | keep_optional_runtime_output_shell | 7 | 0 | 0 | 0 | Header-only until source-backed tranche facts exist. |
| Debt_Tranches_Q | yes | keep_optional_runtime_output_shell | 8 | 0 | 0 | 0 | Reserved hidden capacity for period-keyed tranche history. |
| Guidance_Normalized | yes | keep_neutral_helper_shell | 8 | 0 | 0 | 0 | Header-only normalized guidance projection. |
| Guidance_Raw | no | delete_from_shell | 5410 | 0 | 1 | 531 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Hidden_Value_Audit | yes | keep_formula_dependency | 57 | 35 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| Hidden_Value_Base | yes | keep_formula_dependency | 13 | 0 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| Hidden_Value_Flags | yes | keep_formula_dependency | 13 | 0 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| Hidden_Value_Recompute | yes | keep_formula_dependency | 820 | 770 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| History_Q | yes | keep_formula_dependency | 7 | 0 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| Info_Log | no | delete_from_shell | 4367 | 0 | 1 | 713 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Leverage_Liquidity | yes | keep_optional_runtime_output_shell | 8 | 0 | 0 | 0 | Header-only and fail-closed when debt evidence is unavailable. |
| NonGAAP_Bridge | yes | keep_optional_runtime_output_shell | 7 | 0 | 0 | 0 | Header-only until adjustment evidence exists. |
| NonGAAP_Credibility | yes | keep_optional_runtime_output_shell | 8 | 0 | 0 | 0 | Header-only until reliable adjusted disclosures exist. |
| NonGAAP_Files | no | delete_from_shell | 1218 | 0 | 1 | 118 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| OCR_Text_Log | yes | keep_optional_runtime_output_shell | 6 | 0 | 0 | 0 | Reserved hidden capacity only; full OCR detail remains external JSON. |
| Promise_Evidence | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Header-only evidence projection. |
| Promise_Progress | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Header-only normalized Promise progression. |
| Promise_Tracker | yes | keep_optional_runtime_output_shell | 8 | 0 | 0 | 0 | Reserved hidden capacity for typed progression relationships. |
| Quarter_Narrative_Data | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Header-only source-backed narrative projection. |
| Quarter_Notes | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Header-only normalized note projection. |
| Quarter_Notes_Audit | no | delete_from_shell | 7568 | 0 | 0 | 650 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Quarter_Notes_Evidence | yes | keep_neutral_helper_shell | 6 | 0 | 0 | 0 | Header-only accepted evidence index. |
| REPORT_BS_Q | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Header-only hidden projection until validated balance-sheet facts exist. |
| REPORT_CF_Q | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Header-only hidden projection until validated cash-flow facts exist. |
| REPORT_IS_Q | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Header-only hidden projection until normalized income-statement history is available. |
| Revolver_History | yes | keep_optional_runtime_output_shell | 7 | 0 | 0 | 0 | Header-only until source-backed revolver facts exist. |
| SEC_Audit_Log | no | delete_from_shell | 25779 | 0 | 1 | 162 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Scenario_Bridge_Tax_Treatment | yes | keep_optional_runtime_output_shell | 7 | 0 | 0 | 0 | Header-only until scenario adjustments have explicit tax treatment. |
| Scenario_Driver_Assumptions | yes | keep_formula_dependency | 17 | 0 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| Slides_Debt_Profile | no | delete_from_shell | 92 | 0 | 1 | 9 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Slides_Guidance | no | delete_from_shell | 3239 | 0 | 1 | 196 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Slides_Segments | no | delete_from_shell | 8951 | 0 | 1 | 2213 | Source/raw/audit/runtime-output sheet from the ANF lab is not part of the frozen neutral shell. |
| Valuation_Grid | yes | keep_optional_runtime_output_shell | 252 | 205 | 0 | 0 | Formula-owned grid outputs remain blank until explicit axes and inputs are complete. |
| Valuation_Summary | yes | keep_formula_dependency | 148 | 61 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
| operating_drivers_raw | yes | keep_neutral_helper_shell | 7 | 0 | 0 | 0 | Header-only normalized driver projection. |
| {ticker}_Investment_Case_Data | yes | keep_formula_dependency | 18 | 0 | 0 | 0 | Neutral module sheet is retained because the visible shell, a defined name, or a validation contract references it. |
