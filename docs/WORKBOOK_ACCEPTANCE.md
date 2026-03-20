# Workbook Acceptance

## Core Rule
- The **actual saved workbook** is truth.
- Preview, test snapshots, or in-memory selections are not enough.
- `SUMMARY`, `Valuation`, and `Quarter_Notes_UI` should all be judged from saved-workbook readback.

## Minimum Integrity Requirements
- The saved workbook must open cleanly.
- `SUMMARY` must exist in the saved workbook.
- `Quarter_Notes_UI` must exist in the saved workbook.
- Comment/audit XML must be valid and XML-safe.

## Quarter Notes Acceptance Mindset
- Judge success block by block in `Quarter_Notes_UI`.
- A pass is successful only when stronger notes are visible in the saved workbook.
- Good guidance should remain when it is still useful.
- Improvements must be visible to a human opening the file, not just visible in internal state.
- Runtime optimizations only count as acceptable when the visible `Quarter_Notes_UI` snapshot is unchanged on the saved workbook.
- `QA_Checks` is a secondary confidence surface, not the primary product surface.
  - It is useful for spotting provenance or parsing problems after a pass, especially for latest-quarter Revenue / EBITDA / Net income / Adj EBITDA / buybacks.

## Summary Acceptance Mindset
- Judge `SUMMARY` row by row in the saved workbook.
- `What the company does` should come from business / segment sources, not from a generic fallback if a better source exists.
- `Current strategic context` should reflect why the case matters now, not restate the timeless company description.
- Source notes should match the actual chosen document family.
- Conservative `N/A` is acceptable for revenue-stream rows when the latest reliable segment source is missing or too noisy.

## What Counts As A Successful Pass
- A targeted quarter block is visibly better in the saved workbook.
- A stronger note survives all the way to `readback_verified`.
- Weak generic rows lose before good guidance loses.
- Origin-quarter-only event notes do not repeat into later quarters unless there is a true new update.
- Cross-sheet capital-return rows should not materially contradict `Quarter_Notes_UI` when both surfaces can see the same explicit SEC evidence.
- Filing-table repurchase numerics should beat rounded earnings-release wording when both describe the same execution.
- Generic distribution facts do not create common-stock dividend rows.
- Conservative blanks are acceptable when common-dividend, buyback, or adjusted-EBITDA support is not explicit enough.
- `N/M` is acceptable and preferred when an EBITDA-based leverage / coverage denominator is non-meaningful.
- A summary row can remain blank or `N/A` when the safer alternative is stale, administrative, or fallback-only text.
- No filler is added just to make blocks look larger.
- No improvement claims are made without saved workbook readback.

## What Does Not Count
- A helper test passing without workbook-level confirmation.
- A candidate that reaches `final_selected` but not the saved workbook.
- A preview-only note that disappears after save.

## How To Use `Quarter_Notes_Audit`
- `final_selected`
  - The note won internal selection.
- `readback_verified`
  - The note is present in the actual saved workbook.
- `saved_workbook_missing`
  - The note did not survive save/export/readback.
- `export_provenance_mismatch`
  - Treat as a real correctness problem, not a cosmetic mismatch.

## Current Practical Standard
- `Quarter_Notes_Audit` is available when audit mode is enabled for that export.
- Current CLI behavior enables it by default; it can still be turned off explicitly if needed.
- The current delivered `PBI_model.xlsx` and `GPRE_model.xlsx` **do** contain a visible `Quarter_Notes_Audit` sheet as the rightmost sheet.
- Even when `Quarter_Notes_Audit` is present, `Quarter_Notes_UI` readback is still the primary acceptance surface.
- Do not describe a note as fixed just because the audit sheet shows `final_selected`; it must also be visible in the delivered workbook.
- `QA_Checks` may still show useful WARN or INFO rows when source parsing is conservative or partially unresolved.
  - In the current delivered workbooks, GPRE common-dividend contamination is gone and GPRE Q4 buyback cash aligns to `$30.0m`.
  - In the current delivered workbooks, PBI latest-quarter buybacks now align to filing-table truth at `$126.6m` and `$10.04/share`.
  - In the current delivered workbooks, `QA_Buybacks` is now aligned with the same latest-quarter execution truth on the visible surfaces for both PBI and GPRE.
- In the current delivered workbooks, `SUMMARY` now includes a visible `Current strategic context` row.
  - PBI now uses a concise synthesized management-focus row around capital allocation, cost discipline, execution, and guidance accuracy into 2026.
  - GPRE now uses a concise synthesized management-focus row around `45Z` monetization, CCS execution, and broader low-carbon value realization into 2026.

## Current Good Examples
- PBI `Valuation` latest-quarter buyback note is now clean and filing-table-faithful:
  - `Cash buybacks spent latest quarter $126.6m | Latest quarter +12.614m at $10.04/share`
- GPRE `Valuation` latest-quarter buyback note is now clean and quarter-safe:
  - `Cash buybacks spent latest quarter $30.0m | Latest quarter +2.900m at $10.34/share`
- GPRE `Quarter_Notes_UI` no longer shows the false Q3 2025 buyback execution, and Q4 2025 keeps the real repurchase / exchange / subscription / carbon-capture / `45Z` notes.
- No visible `[REPEAT]` badge remains in the current delivered `Quarter_Notes_UI`.
- The current delivered PBI and GPRE `Quarter_Notes_UI` quarter blocks are now frozen by explicit saved-workbook snapshot tests.
- `SUMMARY` is materially improved in both delivered workbooks:
  - PBI current strategic context and key competitive advantage are now strong saved-workbook examples.
  - GPRE current strategic context and key competitive advantage are now clearly better than the older fallback-driven wording.

## Current Open / Bad Examples
- PBI `Valuation` is not fully clean historically yet:
  - `Buybacks (TTM, cash)` still shows `524.91407196` for `2025-Q4` in the current delivered workbook.
  - Treat latest-quarter truth as clean, but treat historical / TTM buyback-cash series as still needing verification.
- GPRE `SUMMARY` still has a stale revenue-streams period label in the current delivered workbook:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)`
- `Quarter_Notes_Audit` is useful but not fully clean:
  - `saved_workbook_missing` still contains duplicate rescue rows and some noisy XBRL/blob-like excerpts.
  - Treat this as an audit watchlist issue, not automatic evidence that the visible workbook is wrong.
