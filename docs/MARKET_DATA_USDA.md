# USDA Market-Data Flow

## Purpose
This note explains exactly how NWER and AMS 3617 USDA files are downloaded, where they land on disk, how they move into `sec_cache`, and what the workbook ultimately reads.

It is meant to be the single source of truth for the current `GPRE` USDA market-data workflow.

## Sources Covered
- `nwer`
  - USDA report `3616`
  - National Weekly Ethanol Report
- `ams_3617`
  - USDA report `3617`
  - National Daily Ethanol Report

## Current Local Working Folders
- Weekly USDA files:
  - [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
- Daily USDA files:
  - [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)

These are ticker-local working folders.

They are now part of the active runtime flow:
- live downloads land here first
- `sync_market_cache()` copies those files into `sec_cache`
- workbook fallback can also read curated bootstrap CSVs from these folders

They should not be treated as disposable scratch folders.

## Canonical Cache / Export Layers
After local download, the file flow is:

1. ticker-local USDA folder
2. [`sec_cache/market_data/raw`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/raw)
3. [`sec_cache/market_data/parsed`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/parsed)
4. [`sec_cache/market_data/parsed/exports/GPRE.parquet`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/parsed/exports/GPRE.parquet)
5. workbook overlays read the exported parquet rows

Important:
- the workbook does not read USDA PDFs directly
- the workbook reads the provider-agnostic exported parquet rows
- raw PDFs matter because they are the source input for reparsing and export rebuilds

## Normal Latest-Refresh Command
Use:

```powershell
.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --refresh-market-data
```

This does all of the following:
- runs market-data discovery for enabled sources
- downloads the newest NWER / AMS files into the USDA working folders
- syncs those local files into `sec_cache/market_data/raw`
- reparses provider outputs when raw fingerprints changed
- rebuilds the exported parquet rows the workbook consumes

## What The Current Live USDA Site Looks Like
The current USDA report pages do **not** expose the freshest report files as plain static links in the landing page HTML.

Instead, the landing page boots a `slugId` and then loads document fragments via AJAX.

### Landing pages
- NWER:
  - [viewReport/3616](https://mymarketnews.ams.usda.gov/viewReport/3616)
- AMS 3617:
  - [viewReport/3617](https://mymarketnews.ams.usda.gov/viewReport/3617)

### AJAX fragment endpoints used by the site
- latest release:
  - `/get_latest_release/<slugId>`
- previous release navigation:
  - `/get_previous_release/<slugId>`

The code now supports that current AJAX-based latest-release flow.

## What The Downloader Does Today
### Automatic latest refresh
The provider layer now:
- fetches the landing page
- extracts or reconstructs the `slugId`
- requests the latest/previous release fragments
- extracts real file links from those fragment responses
- downloads the latest matching assets into the USDA working folders

### What is downloaded automatically
Current automatic refresh is intended to get the freshest available release files.

In practice, that means:
- NWER latest PDF
- AMS 3617 latest PDF
- any matching direct data asset if USDA exposes it as a direct downloadable file link in the release fragment

### What is not automatically backfilled by default
The normal latest refresh does **not** walk all archive months automatically.

That is a separate history/backfill task.

## Historical Archive Backfill
USDA's archive UI works differently from the latest-release fragment.

The "previous releases" block first returns only the year/month tree.
When a user clicks a month in the browser, the site makes another AJAX request:

```text
/get_previous_release/<slugId>?type=month&month=MM&year=YYYY
```

That month endpoint returns JSON rows containing:
- report title
- report date
- file extension
- document URL

This is the endpoint used for targeted historical backfill.

### Important distinction
- latest refresh:
  - supported directly in the current provider code
- deep archive month-by-month backfill:
  - operationally understood and usable
  - exposed through the dedicated helper script rather than the standard `--refresh-market-data` flow

## Current File Naming
Downloaded files are normalized to stable local names:

- NWER PDF:
  - `nwer_YYYY-MM-DD.pdf`
- NWER direct data file:
  - `nwer_YYYY-MM-DD_data.<ext>`
- AMS PDF:
  - `ams_3617_YYYY-MM-DD.pdf`
- AMS direct data file:
  - `ams_3617_YYYY-MM-DD_data.<ext>`

The date in the stable name is the inferred report date used by the provider.

## Expected Cadence
- `nwer`
  - Weekly report cadence. Missing weekend dates are normal because the source is weekly, not daily.
- `ams_3617`
  - Publication-day cadence. Missing calendar dates are normal when USDA does not publish a report for that day.

These cadence rules matter when checking a backfill window. A date gap does not automatically mean the downloader or parser is broken.

## Bootstrap CSV Support
The market-data service can also read curated local CSVs if they exist.

### Supported weekly CSV names
- [`GPRE/USDA_weekly_data/nwer_weekly.csv`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
- [`GPRE/USDA_weekly_data/nwer_quarterly.csv`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)

### Supported daily CSV names
- [`GPRE/USDA_daily_data/ams_3617_daily_corn.csv`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)
- [`GPRE/USDA_daily_data/ams_3617_weekly_corn.csv`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)

### Search order
If both old `data/` CSVs and USDA-folder CSVs exist, the code prefers:
1. `<ticker>/data/...`
2. `<ticker>/USDA_weekly_data/...` or `<ticker>/USDA_daily_data/...`

That keeps older curated setups stable.

## Workbook Consumption
The workbook normally reads:
- [`sec_cache/market_data/parsed/exports/GPRE.parquet`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/parsed/exports/GPRE.parquet)

If that export is missing, workbook fallback logic can still recover from local curated CSVs in:
- `data/`
- `USDA_weekly_data`
- `USDA_daily_data`

The workbook does not parse USDA PDFs directly during normal render.

## What We Verified In The Current Workspace
The current live setup has already been validated to:
- download latest NWER and AMS PDFs into the USDA working folders
- sync them into `sec_cache/market_data/raw`
- rebuild the exported parquet rows
- support targeted historical backfill for:
  - NWER `2026-01-23 -> 2026-03-23`
  - AMS `2026-01-23 -> 2026-03-31`

Current local working sets now include:
- `GPRE/USDA_weekly_data`: 2023+ PDF history
- `GPRE/USDA_daily_data`: 2023+ PDF history

## Troubleshooting
### If `--refresh-market-data` says `raw_added=0`
Check:
- whether USDA latest-release pages are reachable from the current network
- whether the newest file already exists in the USDA working folder
- whether the provider extracted any direct asset links from the AJAX fragment

### If files exist locally but workbook data does not update
Check:
- that `sync_market_cache()` was run after local files were added
- that `sec_cache/market_data/parsed/exports/GPRE.parquet` was rebuilt
- that the provider parser produced rows for the new raw files

### If you need deeper history than latest refresh gives you
Use the archive-month path conceptually:
- `get_previous_release/<slugId>?type=month&month=MM&year=YYYY`

That is the correct historical source on the live USDA site.

## Current Practical Rules
- Prefer PDF downloads as the main source path.
- Keep USDA working folders on disk.
- Treat `sec_cache/market_data/raw` as canonical raw cache after sync.
- Do not assume latest refresh equals full historical backfill.
- Use bootstrap CSVs only when they add a real need; PDFs are the primary path today.

## Operator Checklist
Use this as the shortest practical runbook.

### 1. Refresh the latest USDA files
Run:

```powershell
.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --refresh-market-data
```

Expect:
- newest NWER PDF lands in [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
- newest AMS PDF lands in [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)
- market-data summary prints `raw_added`, `parsed`, and `export_rows`

### 2. Backfill historical USDA files when needed
Use the USDA archive-month flow, not only `--refresh-market-data`.

Dedicated helper:

```powershell
.\.venv\Scripts\python.exe Code\usda_backfill.py --ticker GPRE --start 2026-01-23 --end 2026-03-31
```

Example for only NWER:

```powershell
.\.venv\Scripts\python.exe Code\usda_backfill.py --ticker GPRE --start 2026-01-23 --end 2026-03-23 --sources nwer
```

Source pattern:

```text
/get_previous_release/<slugId>?type=month&month=MM&year=YYYY
```

Use it when:
- the latest refresh only gives the newest release
- you need a gap filled between older and newer local PDFs
- you want a specific monthly history window

### 3. Sync local USDA files into `sec_cache`
If files were added manually or by a custom backfill script, run a cache sync/reparse path so the exported parquet sees them.

Typical path:
- `sync_market_cache(..., sync_raw=True, refresh=False, reparse=False)`

The helper script above does this automatically unless `--skip-sync` is used.

### 4. Verify the export parquet updated
Check:
- [`sec_cache/market_data/parsed/exports/GPRE.parquet`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/parsed/exports/GPRE.parquet)

Expect:
- new `source_file` values for the freshly added PDFs
- new or updated `observation` / `quarter_avg` / `quarter_end` rows

### 5. Rebuild the workbook if you want the new data visible in Excel
Run:

```powershell
.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE
```

Then check workbook-visible market-data surfaces such as:
- `Economics_Overlay`
- `Operating_Drivers` where relevant
- any market-input commentary that depends on the exported parquet rows
