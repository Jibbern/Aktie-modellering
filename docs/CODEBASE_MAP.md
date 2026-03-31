# Codebase Map

## Purpose
This map explains which modules own each major stage of the runtime so the handoff between ingest, pipeline assembly, workbook rendering, and validation is easy to follow.

## Stage Ownership

### 1. SEC ingest and cache seeding
- [`pbi_xbrl/sec_ingest.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_ingest.py)
  - Downloads SEC filing packages into `sec_cache`.
  - Materializes statement-like 10-Q / 10-K documents into `PBI/financial_statement` and `GPRE/financial_statement`.
- [`pbi_xbrl/sec_xbrl.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_xbrl.py)
  - SEC HTTP client and companyfacts/submissions access.

### 2. Runtime cache layout and environment discovery
- [`pbi_xbrl/cache_layout.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/cache_layout.py)
  - Resolves canonical ticker cache roots and shared cache roots.
- [`pbi_xbrl/pipeline_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_runtime.py)
  - Stage-cache helpers, runtime signatures, and root resolution.

### 3. Pipeline assembly and derived dataframe creation
- [`pbi_xbrl/pipeline_orchestration.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_orchestration.py)
  - Builds the expensive intermediate bundles.
  - Owns stage-cache persistence for GAAP history, debt outputs, local non-GAAP fallback, `doc_intel`, and company overview.
- [`pbi_xbrl/pipeline.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline.py)
  - Thin orchestration-facing API that bridges the pipeline bundle to workbook inputs.
- [`pbi_xbrl/pipeline_types.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_types.py)
  - Dataclasses for config, artifacts, and workbook handoff inputs.

### 4. Source interpretation and evidence shaping
- [`pbi_xbrl/doc_intel.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/doc_intel.py)
  - Builds quarter notes, promises, promise-progress evidence, and non-GAAP credibility outputs from documents.
- [`pbi_xbrl/source_material_refresh.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/source_material_refresh.py)
  - Local source-material discovery, normalization, manifest rebuild, and coverage reporting.
- [`pbi_xbrl/summary_overview.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/summary_overview.py)
  - Topic-aware `SUMMARY` source ranking and visible summary text selection.

### 5. Workbook rendering
- [`pbi_xbrl/excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py)
  - Main workbook renderer and the largest concentration of visible product logic.
  - Owns many final write paths for `Valuation`, `Quarter_Notes_UI`, `Promise_Progress_UI`, `Economics_Overlay`, and supporting QA surfaces.
- [`pbi_xbrl/excel_writer.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer.py)
  - Workbook save/readback helpers and export validation entrypoints.
- Run-scoped writer runtime helpers:
  - [`pbi_xbrl/writer_runtime_cache.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/writer_runtime_cache.py)
    - Groups per-export caches so repeated heavy source analysis does not leak across workbook runs.
  - [`pbi_xbrl/quarter_notes_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/quarter_notes_runtime.py)
    - Shared document-analysis cache for quarter-note rendering inside one export.
  - [`pbi_xbrl/valuation_precompute_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/valuation_precompute_runtime.py)
    - Low-level valuation document parsing and reuse helpers.
  - [`pbi_xbrl/operating_drivers_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/operating_drivers_runtime.py)
    - Run-scoped row selection and cache state for `Operating_Drivers`.
- Supporting writer modules:
  - [`pbi_xbrl/excel_writer_drivers.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_drivers.py)
  - [`pbi_xbrl/excel_writer_sources.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_sources.py)
  - [`pbi_xbrl/excel_writer_segments.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_segments.py)
  - [`pbi_xbrl/excel_writer_financials.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_financials.py)
  - [`pbi_xbrl/excel_writer_ui.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_ui.py)
  - [`pbi_xbrl/excel_writer_core.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_core.py)

### 6. Market-data pipeline
- [`pbi_xbrl/market_data/service.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/service.py)
  - Syncs raw inputs, parsed parquet frames, manifests, and exported rows used by the workbook.
  - Also bridges ticker-local USDA working folders / bootstrap CSVs into the shared export layer.
- [`pbi_xbrl/market_data/providers/`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/providers)
  - Source-specific discovery and parsing.
  - Current USDA providers now handle Drupal/AJAX “latest/previous release” fragments instead of relying only on static landing-page links.
- [`pbi_xbrl/market_data/cache.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/cache.py)
  - Path and manifest helpers for the market-data cache layout.
- [`usda_backfill.py`](/c:/Users/Jibbe/Aktier/Code/usda_backfill.py)
  - Operator CLI for targeted USDA archive backfills when `--refresh-market-data` is not enough.

For the operational USDA download/backfill flow, see
[`MARKET_DATA_USDA.md`](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md).

### 7. QA, audit, and comparison support
- [`pbi_xbrl/pipeline_qa.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_qa.py)
  - Final QA/Needs_Review shaping.
- [`pbi_xbrl/sec_cache_audit.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_cache_audit.py)
  - Audit-only reporting for mixed cache cleanup decisions.
- [`pbi_xbrl/workbook_gap_audit.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/workbook_gap_audit.py)
  - Workbook comparison helpers against saved artifacts and cache outputs.

## Hand-off Model
1. SEC and local materials enter through ingest, refresh, and market-data sync.
2. `pipeline_orchestration` builds reusable stage outputs and final pipeline artifacts.
3. `pipeline.py` packages those artifacts into `WorkbookInputs`.
4. `excel_writer_context` coordinates the workbook write and delegates repeated per-export analysis to the writer runtime helpers.
5. `excel_writer.py` and `stock_models.py` save, reopen, and validate the delivered workbook.

## Most Important Files To Read First
1. [`stock_models.py`](/c:/Users/Jibbe/Aktier/Code/stock_models.py)
2. [`pbi_xbrl/pipeline_orchestration.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_orchestration.py)
3. [`pbi_xbrl/excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py)

For runtime hotspots, cache layering, and current profiling guidance, see
[`PERFORMANCE_NOTES.md`](/c:/Users/Jibbe/Aktier/Code/docs/PERFORMANCE_NOTES.md).
