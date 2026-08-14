# Aktie-modellering Codebase

This repository builds and verifies the delivered `PBI` and `GPRE` Excel workbooks from SEC filings, local narrative materials, structured quarter history, and market-data inputs.

## What Lives Here
- [`stock_models.py`](/c:/Users/Jibbe/Aktier/Code/stock_models.py)
  - CLI entrypoint for pipeline runs, workbook export, market-data refresh, and financial-statement materialization.
- [`pbi_xbrl/`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl)
  - Runtime package for ingest, pipeline orchestration, workbook rendering, QA, and source selection.
  - Core writer/runtime split now also includes explicit run-scoped helper modules such as:
    - [`quarter_notes_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/quarter_notes_runtime.py)
    - [`valuation_precompute_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/valuation_precompute_runtime.py)
    - [`operating_drivers_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/operating_drivers_runtime.py)
    - [`writer_runtime_cache.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/writer_runtime_cache.py)
- [`tests/`](/c:/Users/Jibbe/Aktier/Code/tests)
  - Readback-heavy regression suite for workbook behavior and supporting parsers.
- [`docs/`](/c:/Users/Jibbe/Aktier/Code/docs)
  - Durable project memory, operating notes, and runtime acceptance rules.

## Recommended Reading Order
1. [`docs/SYSTEM_OVERVIEW.md`](/c:/Users/Jibbe/Aktier/Code/docs/SYSTEM_OVERVIEW.md)
2. [`docs/SYSTEM_LIFECYCLE_REGISTRY.json`](docs/SYSTEM_LIFECYCLE_REGISTRY.json)
3. [`docs/CODEBASE_MAP.md`](/c:/Users/Jibbe/Aktier/Code/docs/CODEBASE_MAP.md)
4. [`docs/OWNERSHIP_REGISTRY.json`](docs/OWNERSHIP_REGISTRY.json)
5. [`docs/EXTENSION_POINTS.md`](docs/EXTENSION_POINTS.md)
6. [`docs/CHANGE_IMPACT_REGISTRY.json`](docs/CHANGE_IMPACT_REGISTRY.json)
7. [`docs/APPROVAL_GATES.json`](docs/APPROVAL_GATES.json)
8. [`docs/SEC_CACHE_REFERENCE.md`](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md)
9. [`docs/MARKET_DATA_USDA.md`](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md)
10. [`docs/GPRE_ECONOMICS_OVERLAY.md`](/c:/Users/Jibbe/Aktier/Code/docs/GPRE_ECONOMICS_OVERLAY.md)
11. [`docs/GPRE_DERIVATIVE_HEDGE_DIAGNOSTICS.md`](/c:/Users/Jibbe/Aktier/Code/docs/GPRE_DERIVATIVE_HEDGE_DIAGNOSTICS.md)
12. [`docs/PERFORMANCE_NOTES.md`](/c:/Users/Jibbe/Aktier/Code/docs/PERFORMANCE_NOTES.md)
13. [`docs/WORKBOOK_ACCEPTANCE.md`](/c:/Users/Jibbe/Aktier/Code/docs/WORKBOOK_ACCEPTANCE.md)
14. [`docs/CURRENT_PASS.md`](/c:/Users/Jibbe/Aktier/Code/docs/CURRENT_PASS.md) (active legacy-production status log; not the current new-engine migration authority)
15. [`docs/new_engine_operator_workflow.md`](/c:/Users/Jibbe/Aktier/Code/docs/new_engine_operator_workflow.md) (supported normalized/frozen-shell transition workflow; not a source-native workbook bridge)

## Runtime Model
- For the active legacy production path, the saved and readback-validated workbook is the delivered product artifact.
- Source authority and canonical source-native semantics belong to their closed source, longitudinal-memory, and product contracts; a workbook is not upstream economic authority.
- The accepted source-native Promise Progress product is active in memory and fixtures, but has no workbook bridge. Summary/BS golden `summary-bs-source-native:anf@1.0.0` is accepted with a lossless scratch replay bridge that remains `target_not_wired` and nonproduction. The normalized/frozen-shell engine is a validated transition path, not a universal production replacement.
- Use `docs/SYSTEM_LIFECYCLE_REGISTRY.json` to select the current lifecycle surface before choosing implementation files.
- `sec_cache` is a mixed runtime store:
  - source-like SEC and local-document cache
  - derived pipeline/stage artifacts
  - market-data raw/index/parsed/export data
  - small debug/temp subtrees
- A portable data layout is now supported:
  - preferred local working root: `C:\Users\Jibbe\Aktier\StockModelData`
  - layout: `StockModelData/sec_cache`, `StockModelData/tickers/PBI`, `StockModelData/tickers/GPRE`, `StockModelData/tickers/ANF`, `StockModelData/market_cache`, `StockModelData/outputs/Excel stock models`
  - configure once with `stock_models.py data config set-root C:\Users\Jibbe\Aktier\StockModelData`, then normal rebuild commands no longer need `--data-root`
  - priority is explicit `--data-root`, then `STOCK_MODEL_DATA_ROOT`, then repo config, then auto-detected `StockModelData`, then legacy paths
  - do not run the live working `StockModelData` directly from OneDrive; use a OneDrive snapshot zip for portability
  - use `--material-root <StockModelData>\tickers\<TICKER>` only when overriding one ticker material folder explicitly
- For `GPRE`, live USDA refresh now writes ticker-local working copies into:
  - [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)
  - [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
  - [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)
  before syncing them into [`sec_cache/market_data/raw`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/raw).
- For `GPRE`, thesis ethanol is now practical local-market-data driven:
  - `Next quarter outlook` uses the local Chicago ethanol futures CSVs under [`GPRE/Ethanol_futures`](/c:/Users/Jibbe/Aktier/GPRE/Ethanol_futures)
  - `Quarter-open outlook` first prefers a real frozen prior-quarter snapshot and then falls back to a local manual quarter-open snapshot file when frozen history is missing
  - current observed ethanol still comes from the observed NWER path and should not be contaminated by those futures files
  - the full overlay/source-precedence note now lives in [`docs/GPRE_ECONOMICS_OVERLAY.md`](/c:/Users/Jibbe/Aktier/Code/docs/GPRE_ECONOMICS_OVERLAY.md)
- For `GPRE`, `Current QTD` trend tracking now also keeps a canonical retained sidecar under:
  - [`GPRE/basis_proxy/gpre_current_qtd_snapshots.parquet`](/c:/Users/Jibbe/Aktier/GPRE/basis_proxy/gpre_current_qtd_snapshots.parquet)
  - [`GPRE/basis_proxy/gpre_current_qtd_snapshots.csv`](/c:/Users/Jibbe/Aktier/GPRE/basis_proxy/gpre_current_qtd_snapshots.csv)
  - the workbook shows a compact overlay surface; the sidecar is the retained audit/history store
- For `GPRE`, derivative and hedge disclosures are split into two workbook surfaces:
  - `Derivative_OCI_Bridge` is the accounting source/audit sheet for P&L derivative impact, OCI/AOCI, net derivative exposure, and open hedge notional.
  - `Derivative_Crush_Tests` is diagnostic only; it tests whether reported income-statement derivative P&L helps explain reported margin versus market/proxy crush lenses.
  - OCI/AOCI and net derivative asset/liability never feed current-quarter reported margin, valuation, or the production GPRE crush proxy.
- For conference, earnings-transcript, and CEO-letter folders, curated `*_METADATA_EN.txt` companion files are the preferred deterministic extraction source. The matching raw `.txt`, `.htm/.html`, or PDF stays useful as source QA / audit material, but metadata should win when both files cover the same event. Use `source_file` when possible; legacy-specific keys such as `source_txt_file` and `source_pdf_file` are treated as equivalent raw-source pointers. Metadata `audit_flag` values are carried into source-material provenance so questionable transcript-only datapoints stay review-gated instead of becoming filing-grade facts silently.
- For USDA market data, structured `public_data` JSON is the primary parse source when available. Matching PDFs are still downloaded and retained as audit/provenance companions, with filenames keyed to the report's own period date/title rather than the download date.
- Source selection should prefer explicit support and safe blanks over contaminated values.
- Readback validation exists so fixes are measured against the saved workbook, not only in-memory dataframes.

## End-to-End Handoff
1. `stock_models.py`
   - chooses the coarse workflow: cache maintenance, market-data-only, or full workbook export.
2. `pbi_xbrl/pipeline_orchestration.py`
   - builds the expensive normalized artifact bundle from SEC facts, local materials, and evidence stages.
3. `pbi_xbrl/pipeline.py`
   - keeps a stable external API and bridges those artifacts into `WorkbookInputs`.
4. `pbi_xbrl/excel_writer_context.py`
   - creates run-scoped writer state and caches, then supplies every sheet writer with one consistent context.
5. `pbi_xbrl/excel_writer.py`
   - saves the workbook, reopens it, and validates the delivered file so readback rather than in-memory state decides success.
6. `pbi_xbrl/market_data/service.py`
   - maintains the market-data raw/parsed/export layers consumed by GPRE overlay logic and related sandbox diagnostics.
7. `pbi_xbrl/derivative_oci_bridge.py` and `pbi_xbrl/derivative_crush_tests.py`
   - shape GPRE derivative/hedge accounting disclosure into workbook memo sheets without changing production actuals, valuation, or crush-proxy math.

## Current Workspace Notes
- The git repo root is [`Code/`](/c:/Users/Jibbe/Aktier/Code), while the active workspace also includes sibling directories such as:
  - [`sec_cache`](/c:/Users/Jibbe/Aktier/sec_cache)
  - [`PBI`](/c:/Users/Jibbe/Aktier/PBI)
  - [`GPRE`](/c:/Users/Jibbe/Aktier/GPRE)
  - [`Excel stock models`](/c:/Users/Jibbe/Aktier/Excel%20stock%20models)
- Because of that split, repo-local `.gitignore` only governs files inside `Code/`. Workspace cleanup decisions should be documented explicitly rather than assumed from git status.

## Useful Commands
- Rebuild a workbook:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker PBI`
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE`
- Refresh market data for a market-enabled ticker:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --refresh-market-data`
  - This now prefers USDA `public_data` JSON for NWER `3616`, AMS daily `3617`, and AMS co-products `3618`, then falls back to the older AJAX release-fragment/PDF path if needed.
  - `local_chicago_ethanol_futures` is now the canonical provider in the active GPRE workflow; refresh writes debug artifacts but thesis ethanol comes from the local CSV/manual snapshot files in `GPRE/Ethanol_futures`. The legacy `cme_ethanol_platts` id remains as a compatibility alias during the transition.
  - Live GPRE forward corn now prefers local Barchart CSVs in `GPRE/corn_futures` before NWER fallback; live forward gas is wired the same way for `GPRE/naturalGas_futures` when local files exist. Per-contract `*_price-history-*.csv` files are used for dated quarter-open and next-quarter futures baskets when available.
  - The 7/14-day carry-forward rule remains for GPRE cash/basis snapshots only. Quarter-open local futures use same-date, then nearest prior local price-history row within 7 calendar days; NWER fallback must still be on or before the same anchor date.
- Fast daily GPRE source refresh:
  - `.\.venv\Scripts\python.exe Code\gpre_daily_sources.py`
  - Use this for the common "download today's GPRE corn-bids and USDA files" task. It avoids the full market-cache parse/export rebuild that can run long in interactive sessions, and uses hard per-USDA-source timeouts by default.
- Reconcile market cache without network refresh:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --market-reparse --market-only`
  - `--market-reparse` is incremental: unchanged raw/source fingerprints reuse parsed frames and the ticker export.
  - Use `--market-force-reparse` only when you intentionally want every enabled source reparsed and the export rebuilt even if fingerprints match.
- Configure and run from the local portable data root:
  - `.\.venv\Scripts\python.exe Code\stock_models.py data config show`
  - `.\.venv\Scripts\python.exe Code\stock_models.py data config set-root C:\Users\Jibbe\Aktier\StockModelData`
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker PBI`
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE`
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker ANF`
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker ANF --print-paths`
- Validate/snapshot/restore the portable root:
  - `.\.venv\Scripts\python.exe Code\stock_models.py data validate-root`
  - `.\.venv\Scripts\python.exe Code\stock_models.py data snapshot --out C:\Users\Jibbe\OneDrive\AktierBackup\StockModelData_snapshot.zip`
  - `.\.venv\Scripts\python.exe Code\stock_models.py data restore --snapshot C:\Users\Jibbe\OneDrive\AktierBackup\StockModelData_snapshot.zip --data-root C:\Users\Jibbe\Aktier\StockModelData_restore_test`
  - keep `OldDataArchive_*` for a while before considering permanent deletion
- Backfill historical USDA gaps:
  - `.\.venv\Scripts\python.exe Code\usda_backfill.py --ticker GPRE --start 2026-01-23 --end 2026-03-31`
  - Use `--refresh-market-data` for the newest releases and `usda_backfill.py` for targeted historical windows.
- Materialize 10-Q / 10-K statement files:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --download-financial-statements`

## Documentation Conventions
- Keep durable architectural and runtime truth in `docs/`.
- Route high-level changes through the lifecycle, ownership, extension, impact, and approval registries before reading generated audits or historical artifacts.
- Use module docstrings and short section comments to explain intent, persistence, handoff boundaries, and expected downstream consumers.
- Avoid comments that restate obvious syntax or pandas/openpyxl mechanics.
- Prefer adding comments at cache boundaries, safe-blank decisions, and workbook handoff points instead of commenting every helper line.
