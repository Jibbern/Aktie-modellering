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
- `Valuation` `Hidden value flags` is now a live formula-driven visible panel anchored at `A137`, with `Hidden Value Panel` to the right and price-linked support flowing through the hidden-sheet formula path.
- Visible hidden-flag support text is now shorter and investor-facing:
  - no `Current:` prefix
  - price-linked rows show `(price-linked)` instead of longer gate prose
- `Valuation` `Management commentary` now renders clean sentence text while the `Context` column carries the family / period metadata.
- `Valuation` `Trend / realized` now separates normal guidance deltas from carry-forward realized text, and PBI cost-savings target rows now surface a prior-target comparison in the saved workbook.
- `Promise_Progress_UI` now applies final result fills after the last visible repair pass, so `Updated` is consistently blue and the generated title row no longer picks up a stray status fill.

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
  - `Promise_Tracker_UI` is no longer a visible sheet.
  - `Promise_Progress_UI` is now the sole visible promise surface and the latest block cleanly keeps:
    - `Strategic milestone`
    - `Adjusted EBIT guidance`
    - `EPS guidance`
    - `FCF target`
    - `Revenue guidance`
  - `Valuation` now shows prefix-free management commentary with context in `AC`, for example:
    - `Management expects to continue the buyback program and retire the 2027 Notes in full when callable in March 2026.`
    - `Realizing the potential of PB Bank to optimize cash, strengthen the balance sheet, and drive profitable growth.`
    - `Presort will more aggressively pursue accretive tuck-in acquisition opportunities.`
  - `Valuation` lower guidance now shows quarter-over-quarter delta in `Trend / realized` for updated rows and keeps `$157m realized` on the cost-savings carry row.
- GPRE
  - `SUMMARY` is topic-aware and source-noted.
  - `Valuation` latest-quarter buybacks now read `2.9m` shares and `$30.0m`.
  - `QA_Buybacks` is aligned with that same latest-quarter execution truth on the visible saved-workbook surfaces.
  - `Quarter_Notes_UI` Q2/Q3 no longer show false buyback execution.
  - Q4 retains the real October 27, 2025 repurchase / exchange / subscription notes.
  - `Promise_Tracker_UI` is no longer a visible sheet.
  - `Promise_Progress_UI` is now the sole visible promise surface and the latest block is materially cleaner:
    - strong rows remain for `Advantage Nebraska startup`, `Advantage Nebraska EBITDA opportunity`, `45Z from remaining facilities`, `45Z-related Adjusted EBITDA`, and `Interest expense outlook`
    - junk labels like `least 45Z monetization / EBITDA`, `evaluation Results of Debt reduction`, `in all of our Strategic milestone`, and `nan` do not survive visibly
  - `Valuation` hidden flags now use the same live left-panel layout and price-linked helper formulas as PBI.

## Still Open
- PBI `Valuation` historical / TTM buyback cash is still not fully clean:
  - `Buybacks (TTM, cash)` still shows `524.91407196` for `2025-Q4` in the current delivered workbook.
- GPRE `SUMMARY` still has a stale revenue-streams period label:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)`
- `Quarter_Notes_Audit` still contains `saved_workbook_missing` noise, including duplicate rescue rows and noisy XBRL/blob-like excerpts.
- GPRE `Promise_Progress_UI` still does not surface a separate Q4 2025 `45Z monetization / EBITDA` visible row in the delivered workbook, even though `Quarter_Notes_UI` carries that guidance note.
- GPRE `Valuation` forward-commentary panel is materially better than before but still has some wording noise in the current delivered workbook.
- 2024 historical note coverage remains thinner than 2025.
- `QA_Checks` and `Needs_Review` still surface some noisy provenance issues for metrics beyond buybacks.
- Some GPRE wording and labels could still be polished further.
- Runtime remains high, especially for PBI, and should be treated as a separate optimization pass.
- Manual desktop-Excel acceptance is still required for the final live-recalc check:
  - type a `Price` that should activate a price-linked hidden-value flag
  - confirm the visible `Hidden value flags` row appears on `Valuation` without re-export
