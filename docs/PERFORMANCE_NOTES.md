# Performance Notes

## Purpose
This note explains how to reason about runtime hotspots in the current workbook build without changing behavior or weakening source-faithful safeguards.

## How To Profile Locally
- Use the normal workbook entrypoint with timing enabled:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker PBI --profile-timings`
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --profile-timings`
- Read timings as hotspot indicators, not additive wall-clock budgets.
- Prefer comparing the largest repeating labels across runs rather than summing every nested stage.

## Current Hotspot Families
These are current observations from recent local runs and may vary by machine, cache warmth, and source state.

- `write_excel.ui.render.quarter_notes`
  - The dominant workbook hotspot in recent PBI and GPRE runs.
  - This is where visible quarter-note selection, grouping, formatting, and QA shaping concentrate.
- `write_excel.valuation.precompute.buyback_dividend_maps`
  - Expensive because it reads and interprets filing/document evidence to resolve quarter-safe buyback and dividend support.
- `write_excel.derive.driver_inputs.template_rows`
  - Heavy because it performs quarter-by-quarter template matching across text-derived operating-driver records.
- `write_excel.derive.driver_inputs.operating_history`
  - Builds the visible `Operating_Drivers` row set after template extraction.
- `write_excel.ui.progress_rows.select` and `write_excel.ui.progress_rows.follow_through`
  - Promise-progress selection and lifecycle reconciliation can be materially expensive even when the raw promise tables are not large.
- `doc_intel_bundle`
  - Expensive upstream stage that turns filings and local narrative materials into quarter notes, promises, promise-progress, and non-GAAP credibility outputs.

## Cache Layers and What They Accelerate
- Bundle cache
  - Lives in `stock_models.py`.
  - Stores the full pipeline output bundle that is ready for workbook rendering.
  - Best for workbook-only rerenders and UI/layout passes.
- Stage cache
  - Lives in `pipeline_orchestration.py` via `PipelineStageCache`.
  - Stores expensive intermediate artifacts such as GAAP history, debt outputs, non-GAAP outputs, local fallback extracts, `doc_intel_bundle`, and company overview.
  - Best for avoiding repeated upstream recomputation when only part of the pipeline changes.
- Writer precompute / memoization
  - Lives mainly in `excel_writer_context.py`.
  - Uses `writer_runtime_cache.py` plus the `quarter_notes_runtime`, `valuation_precompute_runtime`, and `operating_drivers_runtime` helpers to cache repeated source analysis during a single workbook write.
  - Best for preventing repeated heavy selection work inside one workbook build.
- Market-data parsed / export cache
  - Lives under `sec_cache/market_data`.
  - `parsed/` preserves provider-specific extraction work.
  - `parsed/exports/` is the provider-agnostic export layer that workbook overlays actually consume.
  - Live USDA refresh first downloads into ticker-local USDA working folders, then syncs those files into raw cache.

## Current USDA Market-Data Note
- The current USDA report pages for NWER and AMS 3617 are AJAX-driven.
- Latest-release refreshes are now supported directly in the provider layer.
- Deeper historical backfill can still require multiple archive-month requests and is more network-bound than the normal latest-release refresh.
- Treat USDA network timing as external I/O variability, not as a workbook-render hotspot.

## What To Optimize First Later
If runtime optimization becomes its own pass, the best first targets are:

- `Quarter_Notes_UI` rendering and latest-quarter QA shaping
- buyback/dividend document precompute in valuation
- operating-driver template extraction and row caching
- promise-progress selection/follow-through
- `doc_intel_bundle` source scanning and evidence shaping

These are better first targets than broad refactors because they already show up as isolated timing buckets.

## What Not To “Optimize”
- Do not bypass cache invalidation rules just to get faster runs.
- Do not replace safe blanks with guessed values.
- Do not collapse source precedence if it would mix context text into quarter-safe numeric outputs.
- Do not treat heatmap-only hidden-history comparisons as permission to backfill missing visible values; coloring may use hidden history, but the saved numbers still need explicit support.
- Do not treat `sec_cache` as disposable temp storage when the workbook still reads from it.

## Interpreting Timing Numbers
- Timing numbers are machine-dependent.
- Timing numbers are cache-state dependent.
- Timing numbers should be treated as current observations, not hard SLA targets.
- A slower but source-faithful stage is often preferable to a faster stage that contaminates visible workbook output.
