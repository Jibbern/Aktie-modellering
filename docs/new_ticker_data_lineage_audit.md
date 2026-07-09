# New-Ticker Data Lineage Audit

This is a read-only audit of the current workbook data path before any
value-only filler runtime is implemented. It documents where information is
stored, transformed, used, validated, and presented today, and where the future
new-ticker engine should take ownership.

This audit does not change production writers, does not build GTX, and does not
patch current sheet content.

## Current Lineage Model

The current system is writer-centric. Source ingestion, parsed dataframes,
source ranking, profile fallbacks, Excel layout, formulas, visible text, support
sheets, and saved-workbook QA are all orchestrated around the Excel writer.

The target new-ticker architecture should be package-centric:

1. Raw/cache sources stay in `StockModelData`.
2. Parser outputs become a normalized data package with status/source metadata.
3. The validator blocks bad or unmapped content before render.
4. The binding map writes approved values only into a frozen workbook shell.
5. Missing sources become mapping gaps or manual review flags, not layout
   mutation or post-render repair.

## Source Input Inventory

| Source class | Current storage/cache | Current parsed/intermediate form | Future normalized destination |
| --- | --- | --- | --- |
| SEC/XBRL | `StockModelData/sec_cache/{ticker}` via `pbi_xbrl/sec_xbrl.py` and `pbi_xbrl/sec_ingest.py` | `companyfacts`, filing docs, `DATA_Facts_Long`, `History_Q`, `SEC_Audit_Log` | `quarterly_financials`, `annual_financials`, `debt_liquidity`, `capital_returns`, `source_coverage` |
| IR files | `StockModelData/tickers/{ticker}` and material roots resolved by writer/pipeline helpers | presentations, releases, financial schedules, `Slides_Guidance`, `Slides_Segments`, `Slides_Debt_Profile`, `NonGAAP_*` | `normalized_guidance`, `segments`, `quarter_notes`, `source_coverage` |
| Transcripts | ticker material roots / source directories, read by doc-intel and operating-driver loaders | promise/quarter-note candidates, operating-driver source records | `normalized_guidance`, `operating_drivers`, `quarter_notes` |
| Presentations | ticker material roots, PDF/text cache through `doc_intel` and writer cache | `Slides_Guidance`, `Slides_Segments`, `Quarter_Notes_Evidence`, `Promise_Evidence` | `normalized_guidance`, `segments`, `operating_drivers`, `source_coverage` |
| Manually configured profile data | `pbi_xbrl/company_profiles.py` and profile-derived helper config | `company_overview`, driver templates, sector flags, investment-case defaults | `ticker_metadata`, `company_profile`, manual-review investment-case fields |
| Derived/calculated fields | `pipeline.py`, `valuation.py`, `excel_writer_core.py`, valuation precompute helpers, Excel formulas | `History_Q`, `Valuation_Summary`, `Valuation_Grid`, `Leverage_Liquidity`, formula cells | normalized calculated fields only when source-backed; formulas stay owned by shell |

## Storage Layer Audit

Current storage is split across disk cache and workbook support sheets:

- Raw/source cache lives under `StockModelData/sec_cache/{ticker}`,
  `StockModelData/tickers/{ticker}`, `StockModelData/market_cache`, and
  `StockModelData/writer_cache`.
- Parsed financial facts live in dataframe inputs produced by `pipeline.py` and
  are written back to support sheets such as `History_Q`, `DATA_Facts_Long`,
  `DATA_LineItem_Map`, `DATA_Period_Index`, `REPORT_*`, and `SEC_Audit_Log`.
- Parsed text/evidence lives in `Quarter_Notes`, `Quarter_Notes_Evidence`,
  `Promise_Tracker`, `Promise_Evidence`, `Promise_Progress`,
  `Guidance_Raw`, `Guidance_Normalized`, `Slides_Guidance`,
  `Slides_Segments`, `OCR_Text_Log`, and ticker-specific investment-case data
  sheets.
- Audit-only data is mixed with visible-support data. For example,
  `Guidance_Normalized` is both support evidence for `Promise_Progress_UI` and a
  readback marker used by validation; `Quarter_Narrative_Data` is support data
  for narrative UI and an audit trail.
- Future normalized fields should live outside the workbook first, in the
  normalized package governed by `docs/normalized_company_data.schema.json`.
  Support sheets can remain workbook audit outputs later, but they should not be
  the primary runtime contract for new ticker fills.

## Cross-Sheet Dependencies

| From/support layer | Feeds visible sheets | Dependency type | Future rule |
| --- | --- | --- | --- |
| `History_Q` | `SUMMARY`, `Valuation`, `BS_Segments`, `Operating_Drivers`, `{ticker}_Investment_Case` | Source-derived quarterly and annual financial facts | Normalize into `quarterly_financials`, `annual_financials`, `debt_liquidity`, and `capital_returns` before render. |
| `DATA_Facts_Long` / `SEC_Audit_Log` | `SUMMARY`, `BS_Segments`, `QA_Checks` | SEC source trace and workbook QA | Use as source coverage and source refs; never show raw tag/cache noise in visible UI. |
| `Guidance_Raw`, `Guidance_Normalized`, `Slides_Guidance` | `Promise_Progress_UI`, `Valuation`, `{ticker}_Investment_Case`, `Quarter_Notes_UI` | Source-derived guidance and classification | Normalize to `normalized_guidance.items` and validate metric/value/horizon before render. |
| `Quarter_Notes`, `Quarter_Notes_Evidence`, `Quarter_Narrative_Data` | `Quarter_Notes_UI`, `{ticker}_Investment_Case`, `SUMMARY` context | Text evidence and selected narrative rows | Normalize to `quarter_notes.items`; parser snippets and boilerplate stay audit-only. |
| `Promise_Progress`, `Promise_Evidence` | `Promise_Progress_UI`, `{ticker}_Investment_Case`, `Valuation` guidance panels | Guidance lifecycle/progress evidence | Normalize to `normalized_guidance.items` with status and source refs. |
| `Slides_Segments`, segment parsers | `BS_Segments`, `Operating_Drivers`, `{ticker}_Investment_Case` | Segment taxonomy and segment values | Normalize to `segments.items` before any visible segment row is rendered. |
| `operating_drivers_raw` | `Operating_Drivers`, `BS_Segments` consistency checks | Operating driver candidates and source rows | Normalize to `operating_drivers.items`; fallback profile templates must not create visible facts. |
| `Debt_Tranches_Q`, `Debt_Profile`, `Debt_Maturity_Ladder`, `Leverage_Liquidity` | `Valuation`, `BS_Segments`, `SUMMARY` | Debt/liquidity support | Normalize to `debt_liquidity`; workbook formulas can summarize but should not invent facts. |
| `Hidden_Value_Flags`, `Hidden_Value_Base` | `Valuation` | Formula/readback support | Keep as shell/audit support; do not use as core new-ticker source input. |
| `QA_Log`, `Needs_Review`, `QA_Checks` | QA visible sheets only | Validation output | Future filler writes validator issues and mapping gaps only; these sheets must not feed business values. |

## Standard Visible Sheet Audit

### SUMMARY

- Source inputs: SEC/XBRL via `History_Q` and `SEC_Audit_Log`; IR/profile
  overview from `company_profiles.py` and `summary_overview.py`; derived
  leverage/liquidity and freshness context.
- Storage layer: raw SEC cache and ticker source files remain on disk; parsed
  financials live in `History_Q`, `Leverage_Liquidity`, `SEC_Audit_Log`, and
  `summary_df`; normalized targets should be `company_profile`,
  `quarterly_financials`, `debt_liquidity`, and `source_coverage`.
- Transformation logic: `excel_writer_core.ensure_summary_inputs()` calls the
  summary builder after valuation inputs are derived; `excel_writer_summary_*`
  selects profile text, latest financials, source notes, and filing freshness.
  Ticker-specific behavior comes from profile fallbacks and sector terminology.
- Workbook presentation: `summary_company_description`,
  `summary_revenue_model`, `summary_key_risks`,
  `summary_quarterly_revenue`, `summary_quarterly_operating_income`,
  `summary_net_debt`, and `summary_liquidity` should fill only the writable
  shell zones. Missing required values should show blanks plus mapping gaps or
  `Needs_Review`, not generic narrative.
- Must never show: raw SEC tag names as prose, cache paths, parser snippets,
  copied PBI/GPRE/ANF sector text, or `N/A` promoted as a thesis.
- Validation and QA: pre-render rules include
  `unexplained_empty_core_field`, `unsupported_sector_specific_leakage`, and
  mapping-gap checks. Workbook validation scans user-facing sheets for bad
  markers and cross-company leakage.
- Ownership: current owner is `excel_writer_summary_builder.py`,
  `excel_writer_summary_sheet.py`, `summary_overview.py`, and
  `excel_writer_core.py`. Future owner should be normalized package builder plus
  binding map; keep source ranking ideas, refactor value selection out of the
  visible writer, avoid profile fallback masquerading as source-backed content.

### Valuation

- Source inputs: `History_Q`, adjusted metrics, debt/liquidity frames, capital
  return evidence, market data, guidance support, hidden-value inputs, and
  derived valuation grids.
- Storage layer: raw facts and SEC audit live in `DATA_Facts_Long` and
  `SEC_Audit_Log`; parsed quarterly series live in `History_Q`; debt support
  lives in `Debt_Tranches_Q`, `Debt_Profile`, and `Leverage_Liquidity`;
  derived outputs live in `Valuation_Summary`, `Valuation_Grid`, and formula
  support areas.
- Transformation logic: `excel_writer_core.ensure_valuation_inputs()` builds
  source views and precompute bundles; `excel_writer_valuation_orchestrator.py`
  renders history grid, formulas, debt detail, hidden value, guidance, trend
  flags, sensitivity, and final layout. Ticker branches include ANF side panels,
  PBI/GPRE guidance-label handling, GPRE warrant overlays, and sector-specific
  hidden-value flags.
- Workbook presentation: valuation bindings cover revenue, operating income,
  EBITDA, FCF, diluted shares, total debt, cash, and buybacks. Missing core
  values should remain blank and emit `valuation_core_mapping_gap`; formulas and
  labels remain shell-owned.
- Must never show: raw guidance candidates, parser noise, impossible share
  counts, unsupported sector rows, source file paths, or rescue scaffolding
  artifacts.
- Validation and QA: pre-render rules include `valuation_core_mapping_gap`,
  `share_count_outlier`, `guidance_metric_misclassification`, and
  `parser_noise_snippet`. Workbook readback also checks formulas, error tokens,
  bad markers, hidden-value sync, and P1 `Needs_Review`.
- Ownership: current owner is `excel_writer_valuation_orchestrator.py`,
  valuation render helpers, `valuation.py`, and `valuation_precompute_runtime`.
  Future owner should be frozen shell plus normalized valuation bindings; keep
  calculation/formula knowledge in the shell, refactor data selection out of the
  renderer, avoid post-render scaffold fixes.

### BS_Segments

- Source inputs: SEC/XBRL balance sheet facts, `History_Q`, local balance sheet
  detail payloads, slides segment data, financial statement files, and
  profile-driven segment aliases.
- Storage layer: parsed balances live in `History_Q`, `DATA_Facts_Long`, and
  local balance-sheet detail structures; segment extracts live in
  `Slides_Segments` and writer helper outputs; normalized targets should be
  `debt_liquidity` and `segments`.
- Transformation logic: `excel_writer_bs_segments.py`,
  `excel_writer_bs_segments_sheet_adapter.py`, `excel_writer_segments.py`, and
  `excel_writer_segment_sources.py` select balance-sheet and segment rows. The
  writer currently has PBI repairs, ANF fiscal/brand handling, and GPRE carbon
  equipment policy.
- Workbook presentation: bindings `bs_cash_series`, `bs_debt_series`,
  `bs_segment_quarterly_rows`, and `bs_segment_annual_rows` fill explicit
  value zones. Missing segment taxonomy should omit visible rows and emit a
  mapping gap/manual-review flag.
- Must never show: sector-specific segment rows without matching source data,
  copied ANF/GPRE/PBI segment labels for another ticker, or raw text fragments.
- Validation and QA: pre-render rules include
  `unsupported_sector_specific_leakage` and empty core checks. Workbook
  guardrails compare segment and balance-sheet source support against visible
  rows.
- Ownership: current owner is `excel_writer_bs_segments.py` and segment source
  helpers. Future owner should normalize `segments.items` before render; keep
  parsers, refactor row selection into package build, avoid sheet-local ticker
  repairs as the standard path.

### Operating_Drivers

- Source inputs: presentations, releases, transcripts, SEC filing text,
  `quarter_notes`, `promises`, `promise_progress`, `History_Q`, segment data,
  derivative OCI bridge, and manual profile driver templates.
- Storage layer: raw documents live in ticker material roots and SEC cache;
  parsed candidates are loaded by `load_operating_driver_source_records()` and
  cached as `operating_driver_history_rows` / `operating_drivers_raw`; normalized
  target is `operating_drivers.items`.
- Transformation logic: `excel_writer_drivers.py` derives driver inputs once;
  `operating_drivers_runtime.py` and `excel_writer_operating_drivers.py` select
  source-ranked lines, templates, segment support, and commentary. Ticker
  branches include ANF retail cleanup/fiscal labels, GPRE commercial/economics
  inputs, and profile-specific templates.
- Workbook presentation: bindings `od_watchlist_rows`,
  `od_current_outlook_rows`, and `od_driver_actuals_rows` fill the visible
  driver sections. Missing driver evidence should produce a mapping gap or
  manual review flag, not a sector fallback row.
- Must never show: parser fragments, incomplete copied source lines,
  unsupported sector terminology, or raw source/cache paths in user-facing text.
- Validation and QA: pre-render rules include `parser_noise_snippet` and
  `unsupported_sector_specific_leakage`. Workbook guardrails compare
  `Operating_Drivers` against `BS_Segments` and source support.
- Ownership: current owner is `excel_writer_drivers.py`,
  `operating_drivers_runtime.py`, and `excel_writer_operating_drivers.py`.
  Future owner should be normalized driver extraction; keep source ranking,
  refactor visible row assembly, avoid profile template rows as facts.

### {ticker}_Investment_Case

- Source inputs: company profile, valuation summary, `History_Q`,
  `Operating_Drivers`, normalized guidance, segment data, manual scenario
  inputs, and post-quarter capital event support.
- Storage layer: current support sheet is `{ticker}_Investment_Case_Data`, plus
  `Scenario_Bridge_Tax_Treatment` and `Scenario_Driver_Assumptions`; normalized
  targets should be `investment_case`, `segments`, `normalized_guidance`, and
  selected valuation hooks.
- Transformation logic: `excel_writer_sector_investment_case.py` builds the
  visible case and formulas; `excel_writer_investment_case_support.py` orders
  sheets and builds ticker-specific data, with ANF-specific and sector-specific
  behavior. It currently mixes manual inputs, source text, formulas, and
  scenario bridge layout.
- Workbook presentation: bindings `ic_investment_summary`, `ic_key_debate`,
  `ic_bull_base_bear_rows`, and `ic_scenario_bridge_rows` fill only declared
  zones. Promotion should be blocked if summary/debate are placeholders or
  manual-review-only.
- Must never show: placeholder investment cases, generic thesis text,
  source/parser blobs, copied sector vocabulary, or manually invented scenario
  facts.
- Validation and QA: pre-render rules include `placeholder_investment_case`,
  `unsupported_sector_specific_leakage`, and valuation mapping gaps. Workbook
  validation scans the visible sheet for bad markers and cross-company leakage.
- Ownership: current owner is sector/ANF investment-case writers and support
  helpers. Future owner should be normalized investment-case package plus
  manual review workflow; keep formula/scenario shell concepts, refactor content
  curation out of the renderer, avoid ticker-specific rescue text.

### Quarter_Notes_UI

- Source inputs: `quarter_notes`, SEC filing evidence, IR files, transcripts,
  presentations, promises, `History_Q`, and doc-intel candidates.
- Storage layer: raw docs are cached on disk; parsed/audit rows live in
  `Quarter_Notes`, `Quarter_Notes_Evidence`, `Quarter_Narrative_Data`, and
  internal quarter-note runtime caches. Normalized target is
  `quarter_notes.items`.
- Transformation logic: `excel_writer_quarter_notes_ui_orchestrator.py`
  coordinates source harvest, candidate creation, selection, audit trace,
  capital allocation handling, render, and render repairs. Ticker branches
  include ANF quarter label/cleanup behavior and profile-specific priority
  terms.
- Workbook presentation: bindings `qn_quarter_note_rows` and
  `qn_quarter_model_implication_rows` fill repeating quarter blocks. Missing
  required note content should omit block content and emit manual review.
- Must never show: boilerplate safe-harbor text, parser snippets, XBRL/XML
  fragments, raw JSON, source cache paths, or incomplete duplicate snippets.
- Validation and QA: pre-render rules include `parser_noise_snippet` and
  boilerplate guidance checks when notes carry guidance. Workbook guardrails
  inspect `Quarter_Narrative_Data` and visible note quality.
- Ownership: current owner is quarter-notes UI orchestrator and its extracted
  source/selection/render/audit helpers. Future owner should normalize note
  candidates before render; keep candidate audit concepts, refactor final
  visible selection into package build, avoid post-render repairs as a runtime
  dependency.

### Promise_Progress_UI

- Source inputs: `Guidance_Raw`, `Guidance_Normalized`, `Slides_Guidance`,
  `Promise_Tracker`, `Promise_Progress`, `Promise_Evidence`, quarter notes,
  transcripts, presentations, and source documents.
- Storage layer: parsed guidance and promise rows live in support sheets and
  doc-intel outputs; hidden/source-key fields are currently used for readback;
  normalized target is `normalized_guidance.items`.
- Transformation logic: `excel_writer_promise_progress.py` renders the visible
  surface while promise-progress selection/rewrite/orchestrator modules build
  rows and lifecycle state. Ticker branches include ANF fiscal guidance and
  PBI/GPRE guidance source labels.
- Workbook presentation: bindings `pp_scorecard_rows`,
  `pp_open_guidance_rows`, and `pp_guidance_timeline_rows` fill explicit
  sections. Missing or conflicted guidance should create no visible guidance row
  and should emit parser-conflict/manual-review flags.
- Must never show: legal boilerplate, wrong metric classifications, duplicate
  metric/horizon rows, hidden source keys as visible text, raw parser labels, or
  unsupported sector terms.
- Validation and QA: pre-render rules include
  `guidance_metric_misclassification`, `boilerplate_guidance`, and
  `parser_noise_snippet`. Workbook guardrails check duplicate guidance,
  stated-in/horizon routing, hidden source keys, and source-backed value
  hydration.
- Ownership: current owner is promise-progress writer/orchestrator/selection
  modules and `doc_intel.py`. Future owner should normalize guidance lifecycle
  before render; keep lifecycle QA rules, refactor row construction out of the
  visible writer, avoid ticker-specific guidance patches.

### QA_Log

- Source inputs: pipeline validation rows, writer QA rows, UI QA rows, readback
  expectations, and normalized-data validator issues in the future.
- Storage layer: current dataframe is assembled in `excel_writer_core.write_qa_sheets()`.
  Future source should be `manual_review_flags` plus workbook readback QA
  output.
- Transformation logic: writer policy functions classify issue families,
  current relevance, review status, and recommended actions. Mostly generic,
  but current rows may include ticker/profile-specific issue context.
- Workbook presentation: binding `qa_log_validation_rows` writes validation rows
  to `A2:Z1000`. Missing issues should write an empty/pass row only after the
  validator returns no issues.
- Must never show: raw source blobs, duplicated noisy checks, or missing
  required structured issue fields.
- Validation and QA: pre-render issue shape must include severity, rule ID,
  field, message, source ref, and suggested action. Workbook validation checks
  blank/nan QA markers.
- Ownership: current owner is `excel_writer_core.write_qa_sheets()` and
  `writer_qa_policy.py`. Future owner should combine normalized validation
  output and readback QA; keep issue shaping, refactor to consume structured
  pre-render issues.

### Needs_Review

- Source inputs: curated `needs_review`, QA rows, writer policy output, and
  future normalized manual review flags.
- Storage layer: current dataframe comes from pipeline validators and writer
  QA shaping; future primary source should be `manual_review_flags`.
- Transformation logic: `write_qa_sheets()` filters current review relevance,
  coalesces issue families, assigns review status, and exports a compact table.
- Workbook presentation: binding `needs_review_validation_rows` writes review
  rows. Required normalized gaps should appear here instead of being patched in
  visible sheets.
- Must never show: low-value duplicate noise as P1, raw parser blobs, or hidden
  support details unless needed for review.
- Validation and QA: workbook validation fails on P1 `Needs_Review`; pre-render
  package validation should create structured flags before any workbook fill.
- Ownership: current owner is writer QA policy and pipeline QA. Future owner
  should be normalized validator/manual review registry; keep curated issue
  taxonomy, refactor to consume normalized data issues.

### QA_Checks

- Source inputs: pipeline QA checks, hidden-value checks, UI QA rows,
  share-count diagnostics, derivative OCI QA, and future binding-map gap checks.
- Storage layer: current dataframe is assembled in `write_qa_sheets()` and
  workbook quality guardrails; future primary source should be `mapping_gaps`
  and validator checks.
- Transformation logic: current logic normalizes audit-like rows, status tokens,
  severity, issue family, and recommended action. It is generic but receives
  sheet-specific rows from visible writers.
- Workbook presentation: binding `qa_checks_mapping_gap_rows` writes one check
  per binding gap or validation check.
- Must never show: unstructured missing values, blank status fields, raw cache
  blobs, or a clean status if required bindings are unfilled.
- Validation and QA: pre-render rules include `valuation_core_mapping_gap` and
  `normalized_data_issue_shape`. Workbook validation checks blank/nan statuses
  and bad markers.
- Ownership: current owner is `excel_writer_core.write_qa_sheets()`,
  `workbook_validation_runner.py`, and `workbook_quality_guardrails.py`. Future
  owner should be binding-map validator plus readback validation; keep severity
  taxonomy, refactor to fail before render when core bindings are missing.

## What Should Be Refactored Before Runtime

- Guidance, segments, drivers, and investment-case text should be normalized
  before any workbook render. The writer should never decide whether a source
  snippet is a valid visible thesis/guidance row.
- `History_Q` and support sheets should remain audit/readback outputs, not the
  runtime binding contract for new tickers.
- Workbook formulas and static labels should stay in the frozen shell. Runtime
  should only write mapped values into declared writable zones.
- Missing data should be represented in `mapping_gaps` and
  `manual_review_flags`. It should not trigger sheet scaffolding, visible layout
  mutation, or last-mile content patching.
