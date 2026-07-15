# Standard Template Sheet Inventory

Read-only comparison of the frozen shell against saved PBI/GPRE/ANF workbooks. Raw/source/audit sheets are classified as runtime outputs unless the shell needs a neutral header-only helper.

| Sheet | Class | Shell | PBI | GPRE | ANF | Runtime fill/create | Reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `SUMMARY` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `Valuation` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `BS_Segments` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `Operating_Drivers` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `{ticker}_Investment_Case` | `standard_visible_shell_sheet` | visible | missing | missing | missing | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `Quarter_Notes_UI` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `Promise_Progress_UI` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `QA_Log` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `Needs_Review` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `QA_Checks` | `standard_visible_shell_sheet` | visible | visible | visible | visible | False | Visible product sheet owned by the selected frozen-shell module profile. |
| `REPORT_IS_Q` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `REPORT_BS_Q` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `REPORT_CF_Q` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Quarter_Notes` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Quarter_Notes_Evidence` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Quarter_Narrative_Data` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Valuation_Summary` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Promise_Evidence` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Promise_Progress` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Guidance_Normalized` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `History_Q` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `operating_drivers_raw` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `DATA_Period_Index` | `required_support_shell_sheet` | hidden | visible | visible | visible | True | Required neutral hidden support sheet owned by a reusable module. |
| `Hidden_Value_Flags` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Hidden_Value_Audit` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Hidden_Value_Recompute` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Hidden_Value_Base` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Revolver_History` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Debt_Tranches_Latest` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Debt_Profile` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Debt_Credit_Notes` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Leverage_Liquidity` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `NonGAAP_Credibility` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Adjusted_Metrics` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `NonGAAP_Bridge` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `{ticker}_Investment_Case_Data` | `optional_module_shell_sheet` | hidden | missing | missing | missing | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Scenario_Bridge_Tax_Treatment` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Scenario_Driver_Assumptions` | `optional_module_shell_sheet` | hidden | visible | visible | visible | True | Reusable optional module sheet retained as a neutral inactive header-only shell. |
| `Debt_Maturity_Ladder` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `Debt_Buckets` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `Debt_Recon` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `Debt_Tranches_Q` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `Valuation_Grid` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `Promise_Tracker` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `Adjustments_Breakdown` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `OCR_Text_Log` | `fixture_capacity_shell_sheet` | hidden | visible | visible | visible | True | Neutral fixture-capacity sheet reserved by an explicit module contract. |
| `ANF_Investment_Case` | `legacy_module_source_sheet` | missing | missing | missing | visible | False | Legacy source equivalent of '{ticker}_Investment_Case'; values are not copied into the shell. |
| `ANF_Investment_Case_Data` | `legacy_module_source_sheet` | missing | missing | missing | visible | False | Legacy source equivalent of '{ticker}_Investment_Case_Data'; values are not copied into the shell. |
| `Basis_Proxy_Sandbox` | `optional_sector_pack_sheet` | missing | missing | visible | missing | True | GPRE-only sector/commodity pack; excluded from the default standard shell. |
| `DATA_Facts_Long` | `external_detail_sheet` | missing | visible | visible | visible | False | Full long-form facts remain JSON-authoritative. |
| `DATA_IS_Rules` | `external_detail_sheet` | missing | visible | visible | visible | False | Parser implementation detail remains external. |
| `DATA_LineItem_Map` | `external_detail_sheet` | missing | visible | visible | visible | False | Parser mapping detail remains external. |
| `Derivative_Crush_Tests` | `exclude_from_standard_shell` | missing | missing | visible | missing | False | Not part of the standard neutral shell unless a future explicit contract promotes it. |
| `Derivative_OCI_Bridge` | `exclude_from_standard_shell` | missing | missing | visible | missing | False | Not part of the standard neutral shell unless a future explicit contract promotes it. |
| `Economics_Overlay` | `optional_sector_pack_sheet` | missing | missing | visible | missing | True | GPRE-only sector/commodity pack; excluded from the default standard shell. |
| `GPRE_Investment_Case` | `ticker_specific_sheet` | missing | missing | visible | missing | False | Ticker-specific investment-case sheet name/data projection; runtime resolves from tokenized shell or normalized package. |
| `GPRE_Investment_Case_Data` | `ticker_specific_sheet` | missing | missing | visible | missing | False | Ticker-specific investment-case sheet name/data projection; runtime resolves from tokenized shell or normalized package. |
| `Guidance_Raw` | `external_detail_sheet` | missing | visible | visible | visible | False | Full raw extraction remains JSON-authoritative; workbook uses normalized guidance and evidence indexes. |
| `History_A` | `exclude_from_standard_shell` | missing | missing | missing | missing | False | Not part of the standard neutral shell unless a future explicit contract promotes it. |
| `Info_Log` | `external_detail_sheet` | missing | visible | visible | visible | False | Raw machine log remains external. |
| `NonGAAP_Files` | `external_detail_sheet` | missing | visible | visible | visible | False | Raw file detail remains external JSON. |
| `PBI_Investment_Case` | `ticker_specific_sheet` | missing | visible | missing | missing | False | Ticker-specific investment-case sheet name/data projection; runtime resolves from tokenized shell or normalized package. |
| `PBI_Investment_Case_Data` | `ticker_specific_sheet` | missing | visible | missing | missing | False | Ticker-specific investment-case sheet name/data projection; runtime resolves from tokenized shell or normalized package. |
| `PostQuarter_Capital_Events` | `exclude_from_standard_shell` | missing | missing | visible | missing | False | Not part of the standard neutral shell unless a future explicit contract promotes it. |
| `Quarter_Notes_Audit` | `external_detail_sheet` | missing | visible | visible | visible | False | Workbook keeps accepted evidence index; complete candidate detail remains external. |
| `SEC_Audit_Log` | `external_detail_sheet` | missing | visible | visible | visible | False | Raw machine log remains external. |
| `Slides_Debt_Profile` | `external_detail_sheet` | missing | visible | visible | visible | False | Workbook keeps normalized debt projections; raw slide detail remains external. |
| `Slides_Guidance` | `rejected_redundant_sheet` | missing | visible | visible | visible | False | Its useful semantics are already represented by typed normalized guidance and Promise Progress; a second slides projection would duplicate evidence. |
| `Slides_Segments` | `external_detail_sheet` | missing | visible | visible | visible | False | Workbook keeps normalized segment projection; raw slide detail remains external. |
| `economics_market_raw` | `optional_sector_pack_sheet` | missing | missing | visible | missing | True | GPRE-only sector/commodity pack; excluded from the default standard shell. |
