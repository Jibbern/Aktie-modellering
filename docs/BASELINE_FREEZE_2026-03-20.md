# Baseline Freeze 2026-03-20

## Purpose
- Freeze the current Excel-modeling baseline before new feature work.
- Keep one clear reference point for runtime, accepted workbook truth, and the current watchlist.

## Current Runtime Baseline
- Full-mode benchmark after the quarter-notes runtime pass:
  - PBI `write_excel`: `882.81s -> 554.91s`
  - PBI `write_excel.ui.render.quarter_notes`: `746.41s -> 418.84s`
  - GPRE `write_excel`: `710.31s -> 558.59s`
- These gains came from:
  - removing redundant temp-workbook provenance/audit work in the CLI path
  - compacting `Quarter_Notes_Audit` candidate-stage rows in full mode
  - reusing run-scoped quarter-notes metadata and text-quality results
- They did **not** come from weakening source ranking, buyback truth logic, or saved-workbook validation.

## Current Accepted Workbook Truth
- Saved-workbook readback is the acceptance truth.
- `Quarter_Notes_UI` visible snapshots are now regression-protected for the current delivered PBI and GPRE workbooks.
- No visible `[REPEAT]` remains in `Quarter_Notes_UI`.
- Latest-quarter buyback truth is currently accepted on visible surfaces:
  - PBI `Valuation` latest-quarter buyback note:
    - `Cash buybacks spent latest quarter $126.6m | Latest quarter +12.614m at $10.04/share`
  - GPRE `Valuation` latest-quarter buyback note:
    - `Cash buybacks spent latest quarter $30.0m | Latest quarter +2.900m at $10.34/share`
- `SUMMARY` is materially improved in both delivered workbooks and now has source-noted `Current strategic context`.

## Current Watchlist
- PBI historical / TTM buyback watchlist:
  - `Buybacks (TTM, cash)` still needs historical verification; latest quarter is clean, full historical/TTM series is not yet baseline-trusted.
- GPRE stale revenue-streams label:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)` is still stale in the current delivered workbook.
- `Quarter_Notes_Audit` noise:
  - `saved_workbook_missing` still contains duplicate rescue rows and some noisy XBRL/blob-like excerpts.

## Forward Rule
- New feature passes must not regress:
  - saved-workbook `Quarter_Notes_UI` snapshots
  - latest-quarter buyback truth on visible surfaces
  - runtime in any broad/material way
- If a new pass does regress one of those:
  - fix it in the same pass, or
  - back the change out before moving on

## Scope Of This Freeze
- This baseline freeze is meant to protect the current workbook quality level.
- It is not a claim that every historical series or audit row is fully clean.
- Conservative wording should win over overclaiming completeness.
