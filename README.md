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
2. [`docs/CODEBASE_MAP.md`](/c:/Users/Jibbe/Aktier/Code/docs/CODEBASE_MAP.md)
3. [`docs/SEC_CACHE_REFERENCE.md`](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md)
4. [`docs/MARKET_DATA_USDA.md`](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md)
5. [`docs/PERFORMANCE_NOTES.md`](/c:/Users/Jibbe/Aktier/Code/docs/PERFORMANCE_NOTES.md)
6. [`docs/WORKBOOK_ACCEPTANCE.md`](/c:/Users/Jibbe/Aktier/Code/docs/WORKBOOK_ACCEPTANCE.md)
7. [`docs/CURRENT_PASS.md`](/c:/Users/Jibbe/Aktier/Code/docs/CURRENT_PASS.md)

## Runtime Model
- The saved workbook is the product truth.
- `sec_cache` is a mixed runtime store:
  - source-like SEC and local-document cache
  - derived pipeline/stage artifacts
  - market-data raw/index/parsed/export data
  - small debug/temp subtrees
- For `GPRE`, live USDA refresh now writes ticker-local working copies into:
  - [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
  - [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)
  before syncing them into [`sec_cache/market_data/raw`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/raw).
- Source selection should prefer explicit support and safe blanks over contaminated values.
- Readback validation exists so fixes are measured against the saved workbook, not only in-memory dataframes.

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
  - This now supports the current USDA AJAX-based report pages for NWER and AMS 3617 latest releases.
- Backfill historical USDA gaps:
  - `.\.venv\Scripts\python.exe Code\usda_backfill.py --ticker GPRE --start 2026-01-23 --end 2026-03-31`
  - Use `--refresh-market-data` for the newest releases and `usda_backfill.py` for targeted historical windows.
- Materialize 10-Q / 10-K statement files:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --download-financial-statements`

## Documentation Conventions
- Keep durable architectural and runtime truth in `docs/`.
- Use module docstrings and short section comments to explain intent, persistence, handoff boundaries, and expected downstream consumers.
- Avoid comments that restate obvious syntax or pandas/openpyxl mechanics.
- Prefer adding comments at cache boundaries, safe-blank decisions, and workbook handoff points instead of commenting every helper line.
