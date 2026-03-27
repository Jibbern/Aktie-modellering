# Workbook Acceptance

## Core Rule
- The **actual saved workbook** is truth.
- Preview, test snapshots, or in-memory selections are not enough.
- `SUMMARY`, `Valuation`, and `Quarter_Notes_UI` should all be judged from saved-workbook readback.

## Analysis-Sheet Style System
- Use one shared structural palette across the visible analysis sheets:
  - `SUMMARY`
  - `Valuation`
  - `BS_Segments`
  - `Quarter_Notes_UI`
  - `Promise_Progress_UI`
  - `Economics_Overlay`
  - `Operating_Drivers`
- Structural colors:
  - `Primary Section Blue`
    - main section headers / headline bands
    - approx `#6FA8DC`
  - `Secondary Header Blue`
    - subheaders / column headers
    - approx `#EAF3FB`
  - `Panel Mist Blue`
    - light panel / background blocks
    - approx `#EDF4FA`
  - `Neutral Surface`
    - normal data rows / empty surface
    - approx white or `#F7F9FC`
  - `Grid Blue-Gray`
    - borders / grid lines
    - approx `#AAB7C4`
  - `Body Text Charcoal`
    - normal text
    - approx `#1F1F1F`
- Status / heatmap colors stay separate from the structural palette:
  - `Negative Amber`
  - `Strong Negative Orange`
  - `Neutral Gray`
  - `Positive Sky`
  - `Strong Positive Blue`
- Hierarchy rule:
  - darker blue is for major section bands and product title rows
  - lighter blue is for column headers / local subheaders
  - data rows stay white or near-white
- Zebra striping is subtle and limited to text-heavy analysis surfaces where it improves readability:
  - apply only on `Quarter_Notes_UI`, `Promise_Progress_UI`, and `Economics_Overlay`
  - do not broadly zebra-stripe `Valuation`, `BS_Segments`, or `Operating_Drivers`
- Never paint over status, heatmap, or result-state semantics with the structural blue palette.
- `Valuation` now has a softer left-side major-band tier for:
  - `Operating`
  - `Cash Flow`
  - `Leverage & Liquidity`
  - `Equity / Per-share`
  - use a blue-gray fill near `#D9E7F3`
  - keep the darker title/section blue for the more prominent panel and headline surfaces

## New-Ticker Automation Default
- New tickers should inherit the current writer defaults before ticker-specific rescue logic is added.
- The default bias should be:
  - visible saved-workbook truth over preview truth
  - conservative blanks over weak inference
  - instrument-specific matching over generic document matching
  - stable design-family reuse over one-off sheet styling
- `Valuation` defaults that should hold automatically on new tickers:
  - use the current left-side subgroup order and light subheaders
  - attach the hidden `Current ratio` and `Quick ratio` definition comments on the label cells
  - keep `Adj FCF (TTM)` marked with the short comment `company-defined`
  - keep debt-flow labels explicit as `Debt repaid (gross, TTM)` and `Debt issued (gross, TTM)`
- Convertible defaults that should hold automatically on new tickers:
  - populate conversion price / full-conversion shares only from source-backed terms
  - normalize a conversion-rate fact when the source clearly expresses shares-per-$1 rather than shares-per-$1,000 and an explicit conversion price confirms the scale
  - populate concurrent repurchase shares only when the support is tied to the same visible instrument row
  - do not let a multi-instrument filing paragraph leak repurchase shares from one convertible row into another
  - when strong hedge / settlement support exists, attach the same short hidden dilution note to both:
    - `Convertible notes` `Shares on full conversion (m)`
    - `Debt Detail (latest)` `Added shares on full conversion (m)`
- Quarter-note defaults that should hold automatically on new tickers:
  - realized contribution text should stay in a realized-results family, not collapse back into guidance/outlook
  - origin-quarter capital-markets events should not repeat into later quarters without a real new update

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

## QA Surface Acceptance
- `Needs_Review` is the curated working queue; treat counts as data rows and exclude the header row.
- `QA_Log` remains the raw history surface even when a related issue is coalesced or displayed more readably in `Needs_Review`.
- `quarter_text_no_explicit_support` should remain visible, but metrics often absent from quarter release text may sort below stronger expected-support gaps inside the same queue bucket.
- Visible QA `source` cells should show the 1-2 most relevant selected quarter-scoped documents, not long concatenated bundle strings.
- Readable queue labels are preferred in curated views when they improve clarity without changing the underlying canonical issue key.

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
- `Valuation` hidden-value area is now a good saved-workbook example when it stays formula-driven and readable:
  - `Hidden value flags` begins at `A137`
  - the visible helper rows stay linked to `Hidden_Value_Flags`
  - price-linked support uses compact `(price-linked)` wording instead of long gate prose
  - `Hidden Value Panel` sits to the right and the title band runs across the full panel width
- PBI `Valuation` latest-quarter buyback note is now clean and filing-table-faithful:
  - `Cash buybacks spent latest quarter $126.6m | Latest quarter +12.614m at $10.04/share`
- GPRE `Valuation` latest-quarter buyback note is now clean and quarter-safe:
  - `Cash buybacks spent latest quarter $30.0m | Latest quarter +2.900m at $10.34/share`
- PBI `Valuation` management commentary is now a good visibility example:
  - the visible sentence starts directly with the message
  - family / period context stays in the `Context` column rather than being prefixed into the sentence
- PBI `Valuation` guidance now shows the intended split in `Trend / realized`:
  - updated numeric rows show `Δ`
  - carry-forward rows keep realized text like `$157m realized`
  - cost-savings target rows can show a prior-target comparison like `from $170m-$190m`
- `Valuation` left-side section bands are now a good consistency check:
  - `Operating`
  - `Cash Flow`
  - `Leverage & Liquidity`
  - `Equity / Per-share`
  - all should read back with the softer blue-gray fill near `#D9E7F3`
- Convertible dilution comments are now a good acceptance check:
  - PBI `1.5% notes due August 2032`
    - visible dilution-cell comment: `Capped call may reduce dilution.`
    - matching debt-detail added-shares cell carries the same hidden note
  - GPRE `2.25% notes due 2027`
    - visible full-conversion shares remain visible
    - concurrent repurchase shares stay blank
    - the short settlement-structure note still appears on the shares cell
  - GPRE `5.25% notes due 2030`
    - full-conversion shares read back as `12.72264m`, not `0.01272264m`
    - concurrent repurchase shares remain `2.9m`
- `Promise_Tracker_UI` is no longer part of the visible workbook product.
  - `Promise_Progress_UI` is the single visible promise UI.
  - Raw sheets `Promise_Tracker`, `Promise_Evidence`, and `Promise_Progress` still remain on the right.
- PBI `Promise_Progress_UI` latest block is a good clean example:
  - `Strategic milestone`
  - `Adjusted EBIT guidance`
  - `EPS guidance`
  - `FCF target`
  - `Revenue guidance`
- GPRE `Promise_Progress_UI` latest block is materially cleaner than before:
  - `Advantage Nebraska startup`
  - `Advantage Nebraska EBITDA opportunity`
  - `45Z from remaining facilities`
  - `45Z-related Adjusted EBITDA`
  - `Interest expense outlook`
- GPRE `Quarter_Notes_UI` no longer shows the false Q3 2025 buyback execution, and Q4 2025 keeps the real repurchase / exchange / subscription / carbon-capture / `45Z` notes.
- No visible `[REPEAT]` badge remains in the current delivered `Quarter_Notes_UI`.
- The current delivered PBI and GPRE `Quarter_Notes_UI` quarter blocks are now frozen by explicit saved-workbook snapshot tests.
- `SUMMARY` is materially improved in both delivered workbooks:
  - PBI current strategic context and key competitive advantage are now strong saved-workbook examples.
  - GPRE current strategic context and key competitive advantage are now clearly better than the older fallback-driven wording.
- `Promise_Progress_UI` result formatting is now a good saved-workbook acceptance check:
  - `Updated` is blue everywhere
  - the generated top row is not accidentally color-coded like a status row

## Current Open / Bad Examples
- PBI `Valuation` is not fully clean historically yet:
  - `Buybacks (TTM, cash)` still shows `524.91407196` for `2025-Q4` in the current delivered workbook.
  - Treat latest-quarter truth as clean, but treat historical / TTM buyback-cash series as still needing verification.
- GPRE `SUMMARY` still has a stale revenue-streams period label in the current delivered workbook:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)`
- GPRE `Promise_Progress_UI` latest block still has one notable open item:
  - a distinct Q4 2025 `45Z monetization / EBITDA` visible row does not yet survive in the delivered workbook even though the corresponding guidance note is present in `Quarter_Notes_UI`
- GPRE `Valuation` management-commentary panel is improved but not fully polished yet:
  - it now surfaces meaningful forward commentary, but some wording is still rough
- Hidden-value flags still require one manual desktop-Excel confirmation step:
  - workbook formulas are in place
  - `openpyxl` does not recalculate Excel formulas
  - the final acceptance proof is entering a `Price` in Excel and confirming that a price-linked visible flag appears without re-export
- `Quarter_Notes_Audit` is useful but not fully clean:
  - `saved_workbook_missing` still contains duplicate rescue rows and some noisy XBRL/blob-like excerpts.
  - Treat this as an audit watchlist issue, not automatic evidence that the visible workbook is wrong.
