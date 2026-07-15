# Workbook Module Manifest

`docs/workbook_module_manifest.json` is the ticker-neutral source of truth for
the full-feature union shell and its activation profiles. The checked-in frozen
shell contains the union of accepted A, B, C, and E legacy capabilities. D-class
raw detail remains external, and F-class redundant behavior is rejected.

The value filler is unchanged: it writes only exact cells from a fully reproduced
binding plan. Before planning, a profile resolves sheet state, disjoint visible
block ranges, executable formula IDs, defined names, style-owned ranges, and the
eligible binding set. Disabled shared-sheet blocks are materialized blank and
inactive; the planner and filler contain no ticker-specific branches.

## Module Matrix

| Module | Type | Owned sheets | Main dependencies |
| --- | --- | --- | --- |
| `core_financial_history` | core | `SUMMARY`, `Valuation`, `REPORT_IS_Q`, `History_Q`, `DATA_Period_Index` | none |
| `balance_cash_flow` | core | `BS_Segments`, `REPORT_BS_Q`, `REPORT_CF_Q` | core history |
| `segments_dimensions` | core shared blocks | segment blocks on `BS_Segments` and `SUMMARY` | core history, balance/cash flow |
| `debt_liquidity` | optional | `Revolver_History`, `Debt_Tranches_Latest`, `Debt_Profile`, `Debt_Credit_Notes`, `Leverage_Liquidity`, `Debt_Maturity_Ladder`, `Debt_Buckets`, `Debt_Recon`, `Debt_Tranches_Q` | core history, balance/cash flow |
| `non_gaap_adjustments` | optional | `NonGAAP_Credibility`, `Adjusted_Metrics`, `NonGAAP_Bridge`, `Scenario_Bridge_Tax_Treatment`, `Adjustments_Breakdown` | core history, balance/cash flow |
| `guidance_promises` | core | `Promise_Progress_UI`, `Promise_Evidence`, `Promise_Progress`, `Guidance_Normalized`, `Promise_Tracker` | core history, QA/lineage |
| `quarter_notes_evidence` | core | `Quarter_Notes_UI`, `Quarter_Notes`, `Quarter_Notes_Evidence`, `Quarter_Narrative_Data` | guidance/promises, QA/lineage |
| `operating_drivers` | core | `Operating_Drivers`, `operating_drivers_raw` | core history, QA/lineage |
| `valuation_scenarios` | optional | `Valuation_Summary`, `Scenario_Driver_Assumptions`, `Valuation_Grid` | core history, balance/cash flow, debt, Non-GAAP |
| `investment_case_market_implied` | core | `{ticker}_Investment_Case`, `{ticker}_Investment_Case_Data` | segments, valuation, QA/lineage |
| `hidden_value_signals` | optional | `Hidden_Value_Flags`, `Hidden_Value_Audit`, `Hidden_Value_Recompute`, `Hidden_Value_Base` | core history, balance/cash flow, debt |
| `qa_lineage` | core | `QA_Log`, `Needs_Review`, `QA_Checks`, `OCR_Text_Log` | none |
| `profile_packs` | explicit pack host | shared visible blocks only | profile declaration |

Every module also declares normalized contracts, binding IDs, actual formula IDs,
defined names, exact style-owned ranges, capacities, activation criteria,
empty-state behavior, and required tests. Formula targets and names are checked
against the materialized workbook, and visible block overlaps fail unless profile
packs explicitly share one mutually exclusive slot.

## Profiles

| Profile | Enabled modules | Profile packs | Dimensions |
| --- | --- | --- | --- |
| `full_union` | all 13 | none | total company, business segment |
| `core_only` | core history, balance/cash flow, QA/lineage | none | total company |
| `anf` | all 13 | `retail_operating_pack` | total company, geography, brand family |
| `pbi` | all 13 | `shipping_mail_pack`, `bank_pack` | total company, business segment, service line |
| `gpre` | all 13 | `commodity_ethanol_pack` | total company, business segment, product, commodity exposure |

Ticker mappings are explicit. Unknown tickers have no inferred profile and must be
declared before a profile shell can be materialized.

## Legacy Dispositions

The 57-sheet ANF fixture inventory is complete and unique:

- A: 10 core visible model sheets retained in the union shell.
- B: 13 required hidden support capabilities retained as neutral header-only sheets.
- C: 15 optional reusable module capabilities retained as neutral header-only sheets.
- D: 10 raw/detail functions kept in normalized JSON and referenced from workbook QA/lineage.
- E: 8 empty fixture/capacity functions retained as hidden neutral module capacity.
- F: `Slides_Guidance` is rejected as redundant; `Guidance_Normalized` owns the function.

No legacy values, assumptions, source paths, comments, or company-specific formulas
are copied into module sheets.

## Materialization

The controlled materializer resolves a profile before formula application or
planning and emits the shell, manifest, and binding contract as one identity-bound
set. Whole-sheet state and shared-sheet sub-block activation are both enforced:

```powershell
python scripts/materialize_standard_template_shell.py `
  --module-profile anf `
  --profile-manifest-output <manifest.json> `
  --profile-binding-map-output <bindings.json> `
  --output <profile-shell.xlsx> `
  --update-identity
```

The checked-in artifact uses `full_union`. Non-union profiles require isolated
contract output paths, preventing an accidental overwrite of the canonical union
contracts. `core_only`, for example, retains no active debt, Non-GAAP, scenario,
hidden-value, narrative, profile-pack, formula, name, binding, or styled block in
the shared visible sheets.
