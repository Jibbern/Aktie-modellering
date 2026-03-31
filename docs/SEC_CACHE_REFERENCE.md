# SEC Cache Reference

## What `sec_cache` Is
`sec_cache` is a mixed runtime store used by workbook builds. It is not just a download cache and it is not just temporary data. Different subtrees have different cleanup rules.

## Keep As Active Runtime / Source Data
- [`sec_cache/PBI`](/c:/Users/Jibbe/Aktier/sec_cache/PBI)
- [`sec_cache/GPRE`](/c:/Users/Jibbe/Aktier/sec_cache/GPRE)
- [`sec_cache/market_data`](/c:/Users/Jibbe/Aktier/sec_cache/market_data)
- [`sec_cache/_reports`](/c:/Users/Jibbe/Aktier/sec_cache/_reports)
- Nested accession packages and filing support files
- `submissions_*.json`
- `companyfacts_*.json`
- Source-material manifests and coverage reports
- Curated local OCR/text outputs that are used as evidence inputs

These are treated as active runtime/source inputs unless a specific audit shows they are obsolete.

## Ticker-Local USDA Working Folders
- [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
- [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)

These sit outside `sec_cache`, but they are now part of the active market-data workflow:
- live USDA refresh writes newly downloaded NWER / AMS PDFs there first
- `sync_market_cache()` then copies those files into [`sec_cache/market_data/raw`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/raw)
- workbook fallback logic can also read curated bootstrap CSVs from these folders if the export parquet is missing

Treat them as source-adjacent working folders, not disposable scratch space.

For the exact NWER / AMS download and backfill flow, see
[`MARKET_DATA_USDA.md`](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md).

## Rebuildable But Still Runtime-Useful
- `pipeline_bundle_cache`
- `pipeline_stage_cache`
- Parsed market-data parquet outputs
- Exported market-data parquet outputs

These can usually be regenerated, but they still matter for runtime speed and reproducibility. Do not delete them casually during normal debugging.

## Safe-To-Remove Temp / Debug Areas
- [`sec_cache/_tmp_pdf_text_cache`](/c:/Users/Jibbe/Aktier/sec_cache/_tmp_pdf_text_cache)
- [`sec_cache/_pdf_text_cache_debug`](/c:/Users/Jibbe/Aktier/sec_cache/_pdf_text_cache_debug)

These are disposable scratch/debug outputs and are reasonable cleanup targets.

## Why `sec_cache` Should Not Be Broadly Filtered Out
- Workbook logic reads directly from `sec_cache` for filings, SEC text, and derived evidence.
- Market-data sync and export rely on `sec_cache/market_data`.
- USDA ticker-local working folders feed into `sec_cache/market_data/raw`, so deleting raw cache without rebuilding can strand the workbook away from the current local USDA snapshot.
- Some local material workflows intentionally store OCR/text derivations under ticker cache roots.
- Cleanup that treats `sec_cache` as generic temp storage can silently remove data that the workbook still depends on.

## Cleanup Rules
- Safe default:
  - remove only explicit temp/debug roots
  - audit before deleting mixed-content cache roots
- Unsafe default:
  - deleting ticker cache roots wholesale
  - deleting nested filing packages
  - deleting market-data raw or parsed files without rebuilding and verifying outputs

## Current Workspace Layout Note
- The git repo root is [`Code/`](/c:/Users/Jibbe/Aktier/Code).
- `sec_cache` lives outside that repo root at [`c:/Users/Jibbe/Aktier/sec_cache`](/c:/Users/Jibbe/Aktier/sec_cache).
- That means repo-local `.gitignore` is not the main control surface for cache hygiene in the current setup.
- Repo-local scratch such as `pytest-cache-files-*`, `.pytest_tmp_*`, and `__pycache__/` under [`Code/`](/c:/Users/Jibbe/Aktier/Code) is a separate category from `sec_cache` and is normally disposable.

## Future Git Hygiene Note
If the workspace root ever becomes the git root, the ignore policy should exclude at least:
- `_compare/`
- `Excel stock models/`
- local workbook exports
- temp/debug cache roots
- repo-local virtualenvs and pytest scratch directories

## Recommended Audit First, Delete Second
- Use [`pbi_xbrl/sec_cache_audit.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_cache_audit.py) for audit-only reporting before deleting mixed or ambiguous cache content.
- Treat cache cleanup as a separate pass from logic changes whenever possible.
