# Support Sheet Lifecycle Contract

The normalized company data package is the source of values. Support/audit sheets are either neutral frozen helpers or runtime-generated projections; the shell must not inherit ANF/PBI/GPRE source data.

| Sheet | Owner | Lifecycle | Neutral shell | Visibility | Created when |
| --- | --- | --- | --- | --- | --- |
| `Adjusted_Metrics` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Adjustments_Breakdown` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Basis_Proxy_Sandbox` | `optional_sector_pack` | `optional_sector_output` | False | hidden_or_visible_by_pack_contract | only when an explicit sector pack is selected |
| `DATA_Facts_Long` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `DATA_IS_Rules` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `DATA_LineItem_Map` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `DATA_Period_Index` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Debt_Buckets` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Debt_Credit_Notes` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Debt_Maturity_Ladder` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Debt_Profile` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Debt_Recon` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Debt_Tranches_Latest` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Debt_Tranches_Q` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Economics_Overlay` | `optional_sector_pack` | `optional_sector_output` | False | hidden_or_visible_by_pack_contract | only when an explicit sector pack is selected |
| `Guidance_Normalized` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Guidance_Raw` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Hidden_Value_Audit` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Hidden_Value_Base` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Hidden_Value_Flags` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Hidden_Value_Recompute` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `History_Q` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Info_Log` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Leverage_Liquidity` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `NonGAAP_Bridge` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `NonGAAP_Credibility` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `NonGAAP_Files` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `OCR_Text_Log` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Promise_Evidence` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Promise_Progress` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Promise_Tracker` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Quarter_Narrative_Data` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Quarter_Notes` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `Quarter_Notes_Audit` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Quarter_Notes_Evidence` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `REPORT_BS_Q` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `REPORT_CF_Q` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `REPORT_IS_Q` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Revolver_History` | `frozen_shell` | `static_template` | True | hidden | materialized into the frozen standard shell as a hidden neutral header-only sheet |
| `SEC_Audit_Log` | `value_only_runtime` | `audit_output` | False | hidden | only during runtime report/workbook generation |
| `Scenario_Bridge_Tax_Treatment` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Scenario_Driver_Assumptions` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Slides_Debt_Profile` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Slides_Guidance` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Slides_Segments` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Valuation_Grid` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `Valuation_Summary` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |
| `economics_market_raw` | `optional_sector_pack` | `optional_sector_output` | False | hidden_or_visible_by_pack_contract | only when an explicit sector pack is selected |
| `operating_drivers_raw` | `value_only_runtime` | `runtime_output` | False | hidden | only when a promoted output workbook needs the support projection |

## Visible QA Surfaces

Full issue occurrences remain in JSON. Visible QA sheets are bounded presentation projections only.

| Sheet | Data owner | Source | Writable zone | Policy |
| --- | --- | --- | --- | --- |
| `QA_Log` | `value_only_runtime` | canonical issue-ledger summaries | A2:L5000 | one row per stable issue_id; full occurrences remain JSON-authoritative; explicit overflow only |
| `Needs_Review` | `value_only_runtime` | canonical issues with visibility_disposition=needs_review | A2:K5000 | audit-only evidence excluded; promotion blockers retained; explicit overflow only |
| `QA_Checks` | `value_only_runtime` | canonical issue-ledger rule aggregates | A2:I5000 | rule-level aggregation; blocking counts reconcile to ledger; explicit overflow only |
