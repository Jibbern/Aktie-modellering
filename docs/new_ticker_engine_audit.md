# New-Ticker Engine Architecture Audit

This audit is based on the current `main` workbook generation flow plus a
read-only inspection of the failed GTX dry-run branch and local artifacts. GTX
is a stress-test example only. Its rescue code must not be copied into the
standard engine.

## GTX Dry-Run Evidence To Reject

GTX-specific code paths that must not be copied:

- `pbi_xbrl/gtx_content_quality.py`: hard-coded GTX guidance normalization
  from named Q1/Q4 documents. This solved visible rows for one ticker, not the
  reusable data problem.
- `pbi_xbrl/company_profiles.py`: the dry-run added a canonical GTX profile.
  This pass must not add GTX to the production profile registry.
- `tests/test_gtx_standard_template_onboarding.py`: asserts a GTX dry-run
  workbook and scaffold audit. This is the opposite of the desired pre-render
  normalized package gate.
- `scripts/generate_gtx_content_coverage_report.py`: reads `GTX_model.xlsx`
  after render to find sparse/wrong content. The new engine must validate
  normalized data before render.
- `scripts/validate_gtx_dryrun_safety.py`: validates the dry-run workbook and
  scaffold audit. GTX workbook validation is out of scope for this pass.

Post-render scaffold/repair logic that must not become the standard engine:

- `pbi_xbrl/workbook_template_scaffold.py` copies PBI prototype layout after
  workbook generation for non-template-family tickers.
- It mutates row heights, column widths, styles, merges, sheet views, SUMMARY
  visible range, Valuation merge alignment, and scaffold audit reports after the
  writer has already produced a workbook.
- It can trim SUMMARY rows/columns, insert rows, top up blank merges, remove
  extra merge shapes, and rewrite the saved workbook file. These are repair
  operations, not a clean new-ticker architecture.

Production writers touched during GTX rescue attempts:

- `pbi_xbrl/excel_writer.py`
- `pbi_xbrl/excel_writer_core.py`
- `pbi_xbrl/excel_writer_investment_case_support.py`
- `pbi_xbrl/excel_writer_promise_progress_orchestrator.py`
- `pbi_xbrl/excel_writer_promise_progress_rewrite.py`
- `pbi_xbrl/excel_writer_sector_investment_case.py`
- `pbi_xbrl/excel_writer_summary_builder.py`
- `pbi_xbrl/excel_writer_ui.py`
- `pbi_xbrl/pipeline.py`
- `pbi_xbrl/quarter_notes.py`
- `pbi_xbrl/sec_xbrl.py`
- `pbi_xbrl/summary_overview.py`
- `stock_models.py`

Source/content failure modes the new engine must prevent:

This source/content failure modes list is the pre-render quality bar for the
new engine:

- Guidance parser rows classified under the wrong metric, such as net income or
  FCF evidence feeding a revenue guidance row.
- Boilerplate safe-harbor or legal text promoted as operating guidance.
- Parser/scaffold snippets such as `Guidance signal in filing text`,
  `Revenue signal in filing text`, `source_txt_file`, `raw_json`, and
  `fcf guidance 1 to 1` reaching visible UI sheets.
- Empty Valuation core rows without an explicit source or mapping gap.
- Diluted share count unit errors and period outliers.
- Investment case placeholders being promoted as a real thesis.
- PBI/GPRE/ANF sector language leaking into an unrelated ticker.

## Sheet-By-Sheet Audit

| Visible sheet | Current writer path | Layout creation | Data selection | Ticker-specific branches | Missing-source behavior | Post-render repairs/scaffold | Why GTX required fixes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `SUMMARY` | `pbi_xbrl/excel_writer.py` orchestrates; summary content comes through `pbi_xbrl/excel_writer_summary_sheet.py`, `pbi_xbrl/excel_writer_summary_builder.py`, and `pbi_xbrl/summary_overview.py`. | Writer builds the sheet directly with openpyxl rows, styles, merges, widths, and section placement. | Profile fallback text, summary overview, key financials, leverage/liquidity, filing freshness, and post-quarter event context. | PBI/GPRE/ANF profile fallbacks and sector terminology shape the narrative. GTX rescue attempted GTX profile text. | Generic fallback text and blanks can still render visibly without a normalized missing-source reason. | GTX dry-run used scaffold trimming to keep SUMMARY inside A:F/standard rows and move/clear spillover. | Generic SUMMARY content spilled outside the standard shell and content remained profile-heavy/sparse. |
| `Valuation` | `pbi_xbrl/excel_writer_valuation_orchestrator.py` plus render/style/precompute helpers. | Writer creates the full visible valuation grid, formulas, hidden support, section headers, colors, and merges. | `History_Q`, valuation precompute bundles, market data, guidance support, capital return/debt inputs. | ANF side panel logic, PBI/GPRE guidance label normalization, sector-specific hidden value flags. | Missing data can produce blanks, N/M, fallback rows, or post-render guardrail issues. | GTX scaffold added/removed/top-up merges and aligned Valuation core section merge rows after render. | GTX had sparse valuation core mapping, wrong guidance metrics, and share-count outliers that layout validation did not catch. |
| `BS_Segments` | `pbi_xbrl/excel_writer_bs_segments.py` and adapter helpers. | Writer lays out balance sheet and segment sections directly. | Balance sheet facts, debt/liquidity, quarterly and annual segment maps. | GPRE carbon rows, ANF retail/fiscal segment rows, PBI segment repairs. | Optional rows may render blank; source-backed gaps are later caught by quality guardrails. | GTX scaffold copied standard row/column/merge/style shell after render. | GTX lacked a normalized segment taxonomy and relied on generic/consolidated fallback rows. |
| `Operating_Drivers` | `pbi_xbrl/excel_writer_operating_drivers.py` and `pbi_xbrl/operating_drivers_runtime.py`. | Writer generates watchlist/current outlook/commentary/actuals sections. | Company profile templates, candidate extraction, operating driver cache, segment support, source text. | GPRE market/economics drivers, ANF retail drivers, profile-specific priority terms. | Missing driver evidence may fall back to profile terms or blank source-backed rows. | GTX scaffold preserved visible shell after the generic content had already rendered. | GTX had parser-noise operating commentary and no normalized driver package to separate evidence from display text. |
| `{ticker}_Investment_Case` | `pbi_xbrl/excel_writer_sector_investment_case.py` plus ANF-specific renderer and `pbi_xbrl/excel_writer_investment_case_support.py`. | Writer builds scenario inputs, thesis/debate blocks, manual input styling, formulas, and bridge sections. | Profile text, valuation support, segment scenarios, guidance/manual inputs. | PBI/GPRE sector defaults and ANF retail logic; GTX rescue added GTX-specific profile/thesis content. | Generic placeholders/manual review text can survive into visible UI if no source-backed case exists. | GTX scaffold aligned the visual shell but could not make the thesis source-backed. | GTX remained sparse/generic and needed hand-curated thesis/guidance patches. |
| `Quarter_Notes_UI` | `pbi_xbrl/excel_writer_quarter_notes_ui_orchestrator.py`, source/selection/render helpers. | Writer emits repeated quarter blocks and applies UI styles/merges. | Quarter notes, filing evidence, doc-intel candidates, narrative rows. | Priority terms and fiscal handling differ by profile/ticker. | Existing validation catches some evidence gaps, but parser snippets can still become candidates. | GTX scaffold topped up large merge families after render. | GTX quarter notes contained boilerplate/parser noise from source extraction. |
| `Promise_Progress_UI` | `pbi_xbrl/excel_writer_promise_progress.py`, rewrite/orchestrator helpers, and ANF fiscal support. | Writer builds scorecard, guidance progression, open guidance, and timeline sections. | Guidance normalized/raw tables, promise evidence, current quarter context. | ANF fiscal guidance, PBI/GPRE normalized guidance markers, profile priority terms. | Missing hidden keys/source rows are caught after workbook render by guardrails. | Existing final repairs plus GTX scaffold/cleanup were used after render. | GTX guidance was misclassified and had to be curated by ticker-specific code. |
| `QA_Log` | Generic dataframe writer through workbook context/QA export path. | Generic tabular sheet layout. | Writer/pipeline QA rows. | Mostly generic. | Blank/nan QA statuses are caught by workbook validation. | No GTX-specific visual scaffold needed. | GTX needed QA to report content gaps earlier than workbook validation. |
| `Needs_Review` | Generic dataframe writer through workbook context/QA export path. | Generic tabular sheet layout. | Needs-review rows from validators, pipeline, and writer policy. | Mostly generic. | P1 rows fail saved-workbook validation. | No GTX-specific visual scaffold needed. | New engine needs normalized mapping/manual-review flags before render. |
| `QA_Checks` | Generic dataframe writer through workbook context/QA export path. | Generic tabular sheet layout. | QA check rows and statuses. | Mostly generic. | Blank/nan status is caught by workbook validation. | No GTX-specific visual scaffold needed. | New engine should create content-validation checks before Excel output exists. |

## Architecture Conclusion

The current system mixes layout, source parsing, ticker profile defaults, content
selection, and saved-workbook QA inside and around the Excel writer. GTX showed
that layout can be repaired after render while content remains sparse or wrong.
The reusable new-ticker engine should invert that flow:

1. Produce a normalized company data package with explicit status on every core
   field.
2. Validate content and mapping gaps before any workbook shell is filled.
3. Fill a frozen `.xlsx` shell by binding map only.
4. Report missing sources and manual review flags without mutating visible
   layout or running post-render scaffold repairs.
