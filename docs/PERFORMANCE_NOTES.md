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
  - After the latest GPRE writer pass, the expensive part is now clearly `write_excel.ui.render.quarter_notes.selection`, not the visible block rendering.
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

## Current Practical Optimization Direction
For the current writer/runtime split, the safest higher-payoff improvements are:

- quarter notes selection
  - focus on document selection / section scanning / evidence shaping before touching visible sheet formatting
  - recent profiling shows `selection` dominates while `render_blocks` is comparatively cheap
- buyback/dividend precompute
  - reuse per-document execution extraction instead of rescanning the same filing text in multiple fallback passes
  - prefer run-scoped caches keyed by document identity + quarter
- operating-driver template rows
  - cache candidate-record sets per `(quarter, template)` before the row-building step
  - avoid rebuilding the same candidate list when the workbook asks for the same template repeatedly inside one export
- promise-progress
  - memoize visible metric/category/theme metadata on the row objects once they are stable
  - avoid rerunning the same text normalization and regex categorization in sort/group/display passes

These are better short-term bets than broad workbook rewrites because they can save visible runtime without weakening workbook truth.

## Latest Measured Result
Recent profiled runs after the latest GPRE-first write-path pass showed that the old `Economics_Overlay` hotspot was mostly repeated market-data preparation, not visible openpyxl cell-writing.

- Full `GPRE` build
  - `write_excel`
    - before: `1693.78s`
    - after: `893.70s`
    - delta: `-800.08s`
  - `write_excel.drivers`
    - before: `868.60s`
    - after: `33.58s`
    - delta: `-835.02s`
  - `write_excel.drivers.render.economics_overlay`
    - before: `844.66s`
    - after: `9.49s`
    - delta: `-835.17s`
- `GPRE` drivers debug scope
  - `write_excel`
    - before: `881.94s`
    - after: `42.66s`
    - delta: `-839.28s`
  - `write_excel.drivers.render.economics_overlay`
    - before: `849.64s`
    - after: `9.86s`
    - delta: `-839.78s`

The kept changes were:
- fine-grained timing buckets for `Economics_Overlay` and `Quarter_Notes_UI`
- reuse of prebuilt GPRE snapshots/history between overlay rendering and fitted-model preview
- reuse of a normalized market `DataFrame` across the heavy GPRE market-data helpers
- single-pass reuse of filing-backed GPRE plant-capacity history inside the overlay writer

What did not materially move yet:
- `write_excel.ui.render.quarter_notes`
  - still the dominant hotspot on full `GPRE`
  - recent observed run:
    - `write_excel.ui.render.quarter_notes=731.01s`
    - `write_excel.ui.render.quarter_notes.selection=726.89s`
    - `write_excel.ui.render.quarter_notes.render_blocks=4.11s`
- `write_excel.ui.progress_rows.select`
  - still material at about `14.6s` on the latest profiled `GPRE` run

So the next realistic optimization target is no longer `Economics_Overlay`. It is `Quarter_Notes_UI` selection.

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
