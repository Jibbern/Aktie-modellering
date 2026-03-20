# Current Pass

## Focus
- Harden workbook dataflow so fixes reach the saved workbook reliably.
- Reduce late-stage leakage between source resolution, visible note rendering, and `Valuation`.
- Sync docs with actual saved workbook truth.
- Freeze the current runtime/correctness baseline before new feature work in [BASELINE_FREEZE_2026-03-20.md](/c:/Users/Jibbe/Aktier/Code/docs/BASELINE_FREEZE_2026-03-20.md).

## What Changed
- Stage cache keys for `doc_intel_bundle` and `company_overview` now include behavior versions and code signatures.
- `SUMMARY` readback validation is now part of normal export verification.
- `Valuation` readback validation is now part of normal export verification.
- `Valuation` capital-return output is more tightly tied to resolved quarter-safe SEC evidence.
- Filing-table repurchase truth now wins over rounded PR text when both exist for the same latest-quarter execution.
- `N/M` is used when EBITDA-based leverage or coverage denominators are non-meaningful.
- Visible `Quarter_Notes_UI` badges are normalized to `NEW`, `CONTINUED`, and `REAFFIRMED`.
- `REPEAT` is no longer meant to survive into the visible workbook.
- `Quarter_Notes_UI` runtime caches are run-scoped inside a single export call and are not shared globally across exports.
- Visible `Quarter_Notes_UI` regression protection now includes frozen saved-workbook snapshots for the current delivered PBI and GPRE quarter blocks.

## Runtime Freeze Note
- The quarter-notes runtime pass is intentionally frozen at the current quality/output level.
- Benchmark note from the current full-mode baseline:
  - PBI `write_excel`: `882.81s -> 554.91s`
  - PBI `write_excel.ui.render.quarter_notes`: `746.41s -> 418.84s`
  - GPRE `write_excel`: `710.31s -> 558.59s`
- These gains came from removing redundant audit/readback overhead and reusing run-scoped quarter-notes metadata, not from weakening source selection or saved-workbook validation.

## Current Delivered Workbook Truth
- PBI
  - `SUMMARY` is topic-aware and source-noted.
  - `Valuation` latest-quarter buybacks now read `12.614m` shares, `$126.6m`, `$10.04/share`.
  - `QA_Buybacks` is aligned with that same latest-quarter execution truth on the visible saved-workbook surfaces.
  - `Quarter_Notes_UI` 2025 blocks remain strong and guidance-first.
- GPRE
  - `SUMMARY` is topic-aware and source-noted.
  - `Valuation` latest-quarter buybacks now read `2.9m` shares and `$30.0m`.
  - `QA_Buybacks` is aligned with that same latest-quarter execution truth on the visible saved-workbook surfaces.
  - `Quarter_Notes_UI` Q2/Q3 no longer show false buyback execution.
  - Q4 retains the real October 27, 2025 repurchase / exchange / subscription notes.

## Still Open
- PBI `Valuation` historical / TTM buyback cash is still not fully clean:
  - `Buybacks (TTM, cash)` still shows `524.91407196` for `2025-Q4` in the current delivered workbook.
- GPRE `SUMMARY` still has a stale revenue-streams period label:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)`
- `Quarter_Notes_Audit` still contains `saved_workbook_missing` noise, including duplicate rescue rows and noisy XBRL/blob-like excerpts.
- 2024 historical note coverage remains thinner than 2025.
- `QA_Checks` and `Needs_Review` still surface some noisy provenance issues for metrics beyond buybacks.
- Some GPRE wording and labels could still be polished further.
- Runtime remains high, especially for PBI, and should be treated as a separate optimization pass.
