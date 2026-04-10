# Repo Refactor Stage 10

## Scope
Stage 10 is a very narrow `GPRE` coproduct pass on top of Stage 9.

It does:
- fix the manual-drop ingest path for `AMS 3618`
- verify the real role of `AMS 3618` using local manual PDFs
- keep `NWER` as the sufficient primary source for the visible block

It does not:
- change the visible Stage 9 block shape
- make `AMS 3618` a blocker again
- add a soybean path
- add a broader DDGS / UHP model
- touch remote USDA refresh logic

## Verified Problem
Before Stage 10:
- local manual `AMS 3618` PDFs existed under [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)
- direct parser calls could extract coproduct rows from real PDFs
- but `GPRE.parquet` still had `0` `ams_3618_pdf` coproduct rows

The concrete gap was:
- manual files were named like `ams_3618_00183.pdf`
- shared discovery only inferred `report_date` from `YYYY-MM-DD` in filenames
- `AMS3618Provider.parse_raw_to_rows()` previously skipped entries when `report_date` was blank
- the parser's report-date regex was also too strict for the real USDA text:
  - actual PDFs use `Livestock, Poultry and Grain Market News ...`

## Code Change
Stage 10 keeps the fix provider-local.

Changes:
- [`pbi_xbrl/market_data/providers/ams_3618.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/providers/ams_3618.py)
  - `provider_parse_version` bumped to `v2`
  - `parse_raw_to_rows()` no longer drops entries just because `report_date` is blank
  - report-date extraction now accepts the real USDA line format without the extra comma before `and`

No shared base/service refactor was needed.

## Verified Result
After local sync/reparse with manual files:
- `ams_3618_pdf` coproduct rows now appear in [`GPRE.parquet`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/parsed/exports/GPRE.parquet)
- verified exported series now include:
  - corn oil:
    - `corn_oil_eastern_cornbelt`
    - `corn_oil_iowa_avg`
    - `corn_oil_kansas`
    - `corn_oil_minnesota`
    - `corn_oil_missouri`
    - `corn_oil_nebraska`
    - `corn_oil_south_dakota`
    - `corn_oil_wisconsin`
  - DDGS:
    - `ddgs_10_illinois`
    - `ddgs_10_indiana`
    - `ddgs_10_iowa`
    - `ddgs_10_kansas`
    - `ddgs_10_michigan`
    - `ddgs_10_minnesota`
    - `ddgs_10_missouri`
    - `ddgs_10_nebraska`
    - `ddgs_10_ohio`
    - `ddgs_10_south_dakota`
    - `ddgs_10_wisconsin`

Current workbook-facing state in [`GPRE_model.xlsx`](/c:/Users/Jibbe/Aktier/Excel%20stock%20models/GPRE_model.xlsx):
- `NWER coproduct rows = YES`
- `AMS 3618 coproduct rows = YES`
- `Renewable corn oil price = YES`
- `Distillers grains price = YES`
- `Approximate coproduct credit = YES`
- `Overlay activation = GO`

The visible `Economics_Overlay` block remains the same compact Stage 9 surface at
rows `176:179`.

## Product Decision
Stage 10 locks in the role split:
- `NWER`
  - primary live source
  - sufficient source for the first visible coproduct block
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback
  - historical/manual backfill source
  - not a blocker

## Verification Notes
Verified in this pass:
- provider-level unit tests for blank `report_date`
- manual-drop sync/export test for undated `ams_3618_00183.pdf` style files
- live `GPRE` export reparse
- `GPRE_model.xlsx` rebuild and readback
- non-`GPRE` no-leak workbook test remains green

Remote USDA download remains out of scope in Stage 10.
