# System Overview

## What The System Does
- Ingest SEC filings, local narrative material, and structured quarter history.
- Build evidence-aware workbook surfaces for `SUMMARY`, `Valuation`, `Quarter_Notes_UI`, QA, and audit sheets.
- Validate the saved workbook after export instead of trusting in-memory previews.

## Current Architecture Surfaces

Three surfaces coexist. Their lifecycle and authority are machine-readable in
[`SYSTEM_LIFECYCLE_REGISTRY.json`](SYSTEM_LIFECYCLE_REGISTRY.json).

1. **Legacy workbook production — active.** `stock_models.py`, the pipeline, and
   legacy writers build the currently delivered PBI/GPRE workbooks. The saved,
   readback-validated workbook is the delivered artifact for this path. Legacy
   writers still contain compatibility economics and must not become the owner of
   new source-native semantics.
2. **Normalized/frozen-shell engine — transition.** The normalized package,
   binding planner, frozen shell, value-only filler, and style-only applicator are
   validated transition capabilities. They have not replaced every production
   consumer and are not the source-native Promise Progress workbook bridge.
3. **Source-native longitudinal/product architecture — active for its accepted
   scope.** C1/C2/C3 records, `PromiseProgressProduct@1`, and the 452-field
   Summary/BS source-native product have closed contracts, deterministic goldens,
   and tests. Promise Progress has no workbook bridge. The accepted lossless
   Summary/BS replay bridge can build scratch goldens, but both production workbook
   integrations remain **target_not_wired** and Summary/BS is not a production default.

Legacy workbooks may be read-only product, capability, behavior, or visual oracles.
They are not canonical source authority for economic facts.

## Main Inputs
- `sec_cache/<TICKER>/`
  - Canonical filing and narrative cache layout.
  - `pbi_xbrl/source_acquisition.py` owns verified staged byte publication and atomic replacement; SEC ingest and source refresh retain source-specific discovery and validation policy.
- `sec_cache/market_data/`
  - Shared market-data raw, parsed, export, and manifest layout used by GPRE economics and market-driver sheets.
- `GPRE/USDA_weekly_data` and `GPRE/USDA_daily_data`
  - Ticker-local USDA working folders for NWER / AMS PDFs and optional curated bootstrap CSVs.
- Optional portable data root
  - Preferred local root is `StockModelData/`, containing `sec_cache`, `tickers/<TICKER>`, `market_cache`, `writer_cache`, `basis_proxy`, and workbook outputs.
  - Path priority is explicit `--data-root`, `STOCK_MODEL_DATA_ROOT`, repo config, auto-detected local `StockModelData`, then legacy folders.
  - `stock_models.py data config show/set-root/clear-root` manages the repo-local default.
  - Live data roots inside OneDrive are refused unless explicitly allowed; OneDrive should carry snapshot zips rather than the working folder.
- `History_Q`
  - Structured quarter history for deterministic metric rows.
- Earnings releases / CEO letters / annual letters
  - Current-quarter framing, guidance, policy, and management emphasis.
- Presentations / transcripts / local materials
  - Supporting narrative evidence where useful.

## Authoritative Paths
- `docs/SYSTEM_LIFECYCLE_REGISTRY.json`
  - Current lifecycle, authority and production status for active, compatibility,
    transition, oracle, generated, and not-wired surfaces.
- `docs/OWNERSHIP_REGISTRY.json`
  - Canonical owner and declared parallel compatibility owner for important concepts.
- `docs/EXTENSION_POINTS.md`
  - Task-oriented starting points and prohibited implementation locations.
- `Code/pbi_xbrl/summary_overview.py`
  - Live topic-aware `SUMMARY` builder.
- `Code/pbi_xbrl/excel_writer_context.py`
  - Live workbook rendering and most `Quarter_Notes_UI` / `Valuation` logic.
- `Code/pbi_xbrl/excel_writer.py`
  - Saved-workbook provenance and readback validation.
- `Code/stock_models.py`
  - CLI entrypoint and export/readback enforcement.

## Continuity Rule
- Treat **git + docs + saved workbooks** as the durable project memory.
- Do not assume Codex/Chat thread history will be available or identical on another machine.
- For machine changes or fresh restarts, begin with:
  - [README.md](/c:/Users/Jibbe/Aktier/Code/README.md)
  - [SYSTEM_LIFECYCLE_REGISTRY.json](SYSTEM_LIFECYCLE_REGISTRY.json)
  - [CODEBASE_MAP.md](/c:/Users/Jibbe/Aktier/Code/docs/CODEBASE_MAP.md)
  - [OWNERSHIP_REGISTRY.json](OWNERSHIP_REGISTRY.json)
  - [EXTENSION_POINTS.md](EXTENSION_POINTS.md)
  - [CHANGE_IMPACT_REGISTRY.json](CHANGE_IMPACT_REGISTRY.json)
  - [APPROVAL_GATES.json](APPROVAL_GATES.json)
  - [SEC_CACHE_REFERENCE.md](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md)
  - [SETUP_ON_NEW_MACHINE.md](/c:/Users/Jibbe/Aktier/Code/docs/SETUP_ON_NEW_MACHINE.md)
  - [BASELINE_FREEZE_2026-03-20.md](/c:/Users/Jibbe/Aktier/Code/docs/BASELINE_FREEZE_2026-03-20.md)
  - [CURRENT_PASS.md](/c:/Users/Jibbe/Aktier/Code/docs/CURRENT_PASS.md)

## Active Legacy Workbook Dataflow
1. Pipeline artifacts are built from filings, structured facts, and narrative evidence.
2. `summary_overview.build_company_overview()` resolves topic-aware `SUMMARY` rows.
3. `excel_writer_context` resolves `Valuation` inputs and final visible note rows, while run-scoped helper modules cache repeated quarter-note, valuation-doc, operating-driver, and market-data fallback analysis inside one export.
4. `Quarter_Notes_UI`, `SUMMARY`, and `Valuation` are written to the workbook.
5. Material finalization and in-memory validation must succeed before an isolated candidate is serialized; serialized readback is validated before atomic promotion to the accepted output.

## Data Handoff Boundaries
- `stock_models.py`
  - Operator-facing workflow switchboard.
  - Decides whether the run stops at ingest/refresh, market-data maintenance, or proceeds into workbook export.
  - Resolves the effective data root and prints the root/source with `--print-paths`.
- `pipeline_orchestration.py`
  - Main artifact assembler.
  - Produces the normalized intermediate bundle that downstream code should treat as the canonical pipeline output.
- `pipeline.py`
  - Compatibility bridge.
  - Keeps the older tuple/wide-call surfaces alive while internally routing work through `PipelineArtifacts` and `WorkbookInputs`.
- `excel_writer_context.py`
  - Run-scoped workbook state boundary.
  - Initializes the workbook plus the per-export caches that later sheet writers reuse.
- `excel_writer.py`
  - Save/readback truth boundary within the cross-module workbook finalization/publication contract.
  - Converts a finalized in-memory workbook into an isolated candidate and validates the serialized file before atomic promotion; material failures cannot publish an accepted output.
- `market_data/service.py`
  - Market-data cache boundary.
  - Translates raw source files and local bootstrap inputs into the parsed/export artifacts the workbook consumes.

## Cache Policy
- `pbi_xbrl/cache_semantics.py` owns `contract:semantic-cache-identity@1`, canonical JSON/SHA-256 identity primitives, and the discoverable semantic-version registry. Cache-specific payload meaning stays with each producer.
- Current shared semantic versions are `unit_norm=v1_table_local_source_unit`, `adjustment_domain=v1_table_role_measure_domain`, `document_period=v2_registered_document_identity`, `debt_period=v1_visual_xbrl_context`, `adjusted_history=v1_metric_definition_scope`, `inline_xbrl_text=v1_continued_at_chain`, and `debt_rate=v1_role_period_authority`.
- `doc_intel_bundle` and `company_overview` stage-cache keys now include explicit behavior versions plus code signatures.
- `doc_intel_bundle` also tracks the local material directory signature so new transcripts, presentations, press releases, or CEO letters invalidate stale narrative outputs.
- Market-data export cache keys track enabled sources, parser versions, raw/bootstrap fingerprints, and market-input fingerprints.
- This is intended to keep code patches from being hidden behind stale stage cache.
- `sec_cache` should be treated as a mixed runtime store, not a generic temp folder.
- See [SEC_CACHE_REFERENCE.md](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md) for keep/delete guidance by subtree.
- See [PERFORMANCE_NOTES.md](/c:/Users/Jibbe/Aktier/Code/docs/PERFORMANCE_NOTES.md) for current hotspot interpretation and cache-layer profiling guidance.
- See [MARKET_DATA_USDA.md](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md) for the live NWER / AMS download and archive-backfill flow.

## Key Product Rules
- The saved workbook is the delivered/readback truth boundary for the active legacy
  production path; it is not upstream source or canonical semantic authority.
- New semantic work should follow shared engine -> sector pack -> ticker profile ->
  typed product. Workbook presentation consumes approved outputs and must not
  reinterpret them.
- The accepted source-native Promise Progress projection is workbook-independent
  until a separately reviewed shadow-first bridge and cutover are implemented.
- Conservative blanks are better than contaminated values.
- Common dividends require explicit common-stock support.
- Quarter buyback execution requires explicit quarter-safe evidence.
- Program context and remaining authorization may appear as context, but not as execution metrics.
- Visible `Quarter_Notes_UI` badges should be limited to `NEW`, `CONTINUED`, and `REAFFIRMED`.
- Origin-quarter-only events should not auto-carry forward as continued notes.
- `Valuation` leverage / coverage labels must match the actual denominator family.
- Use `N/M` when the relevant EBITDA denominator is non-meaningful.
- `Needs_Review` is a curated action queue; row counts should be interpreted as data rows, excluding the header.
- `quarter_text_no_explicit_support` stays visible when current-quarter support is missing, but ordering may be softened for metrics that are often omitted from release text.
- Visible QA `source` fields should stay concise and human-readable; full provenance remains in the underlying evidence path and raw logs.
- Curated queue rows may use a more readable display metric than the raw internal metric name when closely related issue families are coalesced.

## Summary Architecture
- `SUMMARY` is topic-aware, not single-document-driven.
- `What the company does`
  - Prefer original `10-K`, then `10-Q`, then `8-K` “About” fallback.
- `Current strategic context`
  - Prefer latest earnings `8-K` / `EX-99.1`, then CEO letter / `EX-99.2`, then `10-Q` MD&A context.
- `Key competitive advantage`
  - Prefer `10-K` competition / segment language, with current-quarter materials only as support.
- Administrative amendments should not replace the real business / risk source.

## Valuation Architecture
- `Valuation` now uses a resolved capital-return layer rather than letting note text or generic program text drive numeric output.
- That resolved layer separates:
  - quarter-safe buyback execution
  - common-dividend support
  - authorization / remaining-capacity context
  - provenance and suppress reasons
- `Valuation` and `Quarter_Notes_UI` should converge when the same explicit SEC buyback evidence is available.
- Hidden-history heatmap fills are allowed to look one year behind the visible window when a real prior comparator exists; this is render-only and does not change the visible numeric values.
- Market-data refresh for `GPRE` now relies on USDA AJAX release fragments for the freshest NWER / AMS reports; ticker-local USDA folders are the first on-disk handoff before raw-cache sync.

## Current Workbook Truth
- PBI `SUMMARY`
  - Source-driven company description from the `10-K`
  - Current strategic context focused on capital allocation, cost discipline, execution, and guidance accuracy into 2026
- GPRE `SUMMARY`
  - Source-driven company description from the `10-K`
  - Current strategic context focused on `45Z` monetization, CCS execution, and broader low-carbon value realization into 2026
- PBI `Valuation`
  - Latest-quarter buybacks now use filing-table truth: `12.614m` shares, `$126.6m`, `$10.04/share`
- GPRE `Valuation`
  - Latest-quarter buybacks now use explicit Q4 execution truth: `2.9m` shares, `$30.0m`
- `Quarter_Notes_Audit`
  - Visible and rightmost in current CLI-delivered workbook exports

## Current Watchlist
- 2024 historical note coverage is still thinner than 2025 for both PBI and GPRE.
- `QA_Buybacks` / `QA_Checks` still lag the final visible product quality in a few places.
- Net income / EBITDA / Adjusted EBITDA provenance review is improved but still not fully complete.
- Some GPRE labels and management-note wording can still be polished further.
