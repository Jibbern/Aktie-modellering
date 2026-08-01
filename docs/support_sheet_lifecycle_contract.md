# Support Sheet Lifecycle Contract

The normalized company data package is the source of values. Support/audit sheets are either neutral frozen helpers or runtime-generated projections; the shell must not inherit ANF/PBI/GPRE source data.

| Sheet | Owner | Lifecycle | Neutral shell | Visibility | Created when |
| --- | --- | --- | --- | --- | --- |
| `Adjusted_Metrics` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Adjustments_Breakdown` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Basis_Proxy_Sandbox` | `optional_sector_pack` | `optional_sector_output` | False | hidden_or_visible_by_pack_contract | only when an explicit sector pack is selected |
| `DATA_Facts_Long` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `DATA_IS_Rules` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `DATA_LineItem_Map` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `DATA_Period_Index` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Buckets` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Credit_Notes` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Maturity_Ladder` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Profile` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Recon` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Tranches_Latest` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Debt_Tranches_Q` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Economics_Overlay` | `optional_sector_pack` | `optional_sector_output` | False | hidden_or_visible_by_pack_contract | only when an explicit sector pack is selected |
| `Guidance_Normalized` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Guidance_Raw` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `Hidden_Value_Audit` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Hidden_Value_Base` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Hidden_Value_Flags` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Hidden_Value_Recompute` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `History_Q` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Info_Log` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `Leverage_Liquidity` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `NonGAAP_Bridge` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `NonGAAP_Credibility` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `NonGAAP_Files` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `OCR_Text_Log` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Promise_Evidence` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Promise_Progress` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Promise_Tracker` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Quarter_Narrative_Data` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Quarter_Notes` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Quarter_Notes_Audit` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `Quarter_Notes_Evidence` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `REPORT_BS_Q` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `REPORT_CF_Q` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `REPORT_IS_Q` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Revolver_History` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `SEC_Audit_Log` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `Scenario_Bridge_Tax_Treatment` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Scenario_Driver_Assumptions` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `Slides_Debt_Profile` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `Slides_Segments` | `external_normalized_json` | `external_detail` | False | external_only | generated outside Excel and linked through concise workbook lineage references |
| `economics_market_raw` | `optional_sector_pack` | `optional_sector_output` | False | hidden_or_visible_by_pack_contract | only when an explicit sector pack is selected |
| `operating_drivers_raw` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |
| `{ticker}_Investment_Case_Data` | `frozen_shell` | `static_template` | True | hidden | materialized into the union shell as a neutral hidden header-only module sheet |

## Visible QA Surfaces

Full issue occurrences remain in JSON. Visible QA sheets are bounded presentation projections only.

| Sheet | Data owner | Source | Writable zone | Policy |
| --- | --- | --- | --- | --- |
| `QA_Log` | `value_only_runtime` | canonical issue-ledger summaries | A2:L5000 | one row per stable issue_id; full occurrences remain JSON-authoritative; explicit overflow only |
| `Needs_Review` | `value_only_runtime` | canonical issues with visibility_disposition=needs_review | A2:K5000 | audit-only evidence excluded; promotion blockers retained; explicit overflow only |
| `QA_Checks` | `value_only_runtime` | canonical issue-ledger rule aggregates | A2:I5000 | rule-level aggregation; blocking counts reconcile to ledger; explicit overflow only |
