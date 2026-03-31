# Current Pass

## Focus
- Harden workbook dataflow so fixes reach the saved workbook reliably.
- Reduce late-stage leakage between source resolution, visible note rendering, and `Valuation`.
- Sync docs with actual saved workbook truth.
- Document the runtime layout and clarify which workspace artifacts are source-like, rebuildable, or disposable.
- Freeze the current runtime/correctness baseline before new feature work in [BASELINE_FREEZE_2026-03-20.md](/c:/Users/Jibbe/Aktier/Code/docs/BASELINE_FREEZE_2026-03-20.md).

## What Changed
- Repo docs and module headers were refreshed so the current writer/runtime split is easier to follow from disk without relying on thread history.
- Market-data docs now reflect the live USDA behavior:
  - current NWER / AMS report pages load documents through AJAX release fragments
  - `GPRE/USDA_weekly_data` and `GPRE/USDA_daily_data` are now the ticker-local working folders for those downloads
- Writer runtime is now described explicitly in docs:
  - `quarter_notes_runtime`
  - `valuation_precompute_runtime`
  - `operating_drivers_runtime`
  - `writer_runtime_cache`
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
- `Needs_Review` now preserves the current QA taxonomy but reads more cleanly:
  - row counts should be interpreted as data rows, not worksheet `max_row`
  - `quarter_text_no_explicit_support` rows keep their visibility but use softer internal ordering for metrics that are often absent from release text
  - visible `source` fields now prefer a short selected-doc list instead of a noisy bundle summary
  - curated debt-basis rows may use a more readable display metric while keeping the same canonical coalescing behavior

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
  - USDA market-data local working folders now contain 2023+ PDF history, and the 2026-01-23 -> 2026-03-31 daily / 2026-01-23 -> 2026-03-23 weekly gaps were backfilled from live USDA archive endpoints.
  - `QA_Buybacks` is aligned with that same latest-quarter execution truth on the visible saved-workbook surfaces.
  - `Quarter_Notes_UI` Q2/Q3 no longer show false buyback execution.
  - Q4 retains the real October 27, 2025 repurchase / exchange / subscription notes.
  - `Promise_Tracker_UI` is no longer a visible sheet.
  - `Promise_Progress_UI` is now the sole visible promise surface and the latest block is materially cleaner:
    - strong rows remain for `Advantage Nebraska startup`, `Advantage Nebraska EBITDA opportunity`, `45Z from remaining facilities`, `45Z-related Adjusted EBITDA`, and `Interest expense outlook`
    - junk labels like `least 45Z monetization / EBITDA`, `evaluation Results of Debt reduction`, `in all of our Strategic milestone`, and `nan` do not survive visibly
  - `Valuation` hidden flags now use the same live left-panel layout and price-linked helper formulas as PBI.
  - `Buybacks (shares)` can now append a latest-quarter convertible-linked suffix when the concurrent repurchase is explicitly supported.

## Still Open
- GPRE `SUMMARY` still has a stale revenue-streams period label:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)`
- `Quarter_Notes_Audit` still contains `saved_workbook_missing` noise, including duplicate rescue rows and noisy XBRL/blob-like excerpts.
- GPRE `Promise_Progress_UI` still does not surface a separate Q4 2025 `45Z monetization / EBITDA` visible row in the delivered workbook, even though `Quarter_Notes_UI` carries that guidance note.
- GPRE `Valuation` forward-commentary panel is materially better than before but still has some wording noise in the current delivered workbook.
- 2024 historical note coverage remains thinner than 2025.
- `QA_Checks` and `Needs_Review` are materially cleaner, but some source-gap and debt-definition rows remain as truthful current-quarter QA rather than cosmetic noise.
- Some GPRE wording and labels could still be polished further.
- Runtime remains high, especially for PBI, and should be treated as a separate optimization pass.
- Manual desktop-Excel acceptance is still required for the final live-recalc check:
  - type a `Price` that should activate a price-linked hidden-value flag
  - confirm the visible `Hidden value flags` row appears on `Valuation` without re-export

## Workspace Hygiene
- `_compare/` is local comparison output, not active runtime state, and can be removed once durable conclusions are captured in repo docs.
- `sec_cache` should be cleaned selectively, not broadly:
  - keep ticker caches, market-data roots, and reports
  - treat `pipeline_bundle_cache` / `pipeline_stage_cache` as rebuildable but runtime-useful
  - temp/debug roots such as `_tmp_pdf_text_cache` and `_pdf_text_cache_debug` are the safe cleanup targets
- ticker-local USDA working folders should also be treated as active source-adjacent state, not disposable scratch
- See [SEC_CACHE_REFERENCE.md](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md) for the detailed policy.
