# Summary Overview

## Purpose
- `SUMMARY` is a topic-aware overview surface, not a single-document dump.
- The active builder is `Code/pbi_xbrl/summary_overview.py`.
- The legacy overview logic in `Code/pbi_xbrl/pipeline.py` is not the authoritative path for delivered workbook output.

## Visible Rows
- `What the company does`
- `Current strategic context`
- `Key competitive advantage`
- Other summary rows may stay conservative or `N/A` when the latest reliable source is not strong enough.

## Topic-Aware Source Ranking
- `What the company does`
  - Original `10-K Item 1`
  - Then original `10-Q`
  - Then `8-K` earnings-release “About” paragraph as fallback only
- `Current strategic context`
  - Latest earnings `8-K` / `EX-99.1`
  - Then CEO letter / `EX-99.2`
  - Then `10-Q` MD&A context
- `Key competitive advantage`
  - `10-K` competition / segment language first
  - Earnings release / CEO letter only as supporting context

## Amendment Policy
- Administrative `10-K/A` / `10-Q/A` filings are downranked.
- Clerical amendments should not replace the original business / competition / risk source.
- Current known truth example:
  - PBI 2025 `10-K/A` is administrative only and should not displace the primary `10-K` for summary sourcing.

## Cache Policy
- `company_overview` is stage-cached.
- The cache key now includes:
  - input material signature
  - explicit behavior version
  - code signature of the active summary module
- This is intended to keep summary patches from being masked by stale stage cache.

## Fallback Policy
- Profile fallback text is last-resort safety net only.
- Fallback should prevent empty output, not preempt better filing-driven text.
- Conservative `N/A` is preferred over stale, administrative, or weak fallback copy.

## Authoritative Dataflow
1. SEC / narrative materials are indexed.
2. `summary_overview.build_company_overview()` resolves topic-specific source candidates.
3. `excel_writer_context` renders visible `SUMMARY` rows plus source notes.
4. The saved workbook is read back after export.
5. `SUMMARY` acceptance is based on saved workbook readback, not in-memory preview.

## Current Truth Examples
- PBI
  - `What the company does` is anchored to the `10-K` business description.
  - `Current strategic context` centers on capital allocation, cost discipline, execution, and improving guidance accuracy into 2026.
  - `Key competitive advantage` centers on financing capability, software-enabled workflows, and presort-network scale.
- GPRE
  - `What the company does` is anchored to the `10-K` business description and both segments.
  - `Current strategic context` centers on `45Z` monetization, CCS execution, and broader low-carbon value realization into 2026.
  - `Key competitive advantage` is broad biorefinery-platform language, not an overly narrow protein-only description.

## Current Good Examples
- PBI visible saved-workbook examples are now genuinely useful:
  - `Current strategic context` is a good example of a concise management-focus row.
  - `Key competitive advantage` is a good example of a source-aware synthesized edge sentence rather than generic fallback copy.
- GPRE visible saved-workbook examples are clearly improved:
  - `Current strategic context` now cleanly surfaces `45Z` / CCS / low-carbon framing.
  - `Key competitive advantage` stays broad and does not collapse into a narrow protein-only description.

## Current Open Examples
- GPRE revenue-streams labeling is still not fully polished in the saved workbook:
  - `Business model / revenue streams (% of total revenue) (Quarter end 2025-09-30)`
- Treat this as a visible label-cleanup watchlist item, not as evidence that the broader topic-aware summary architecture regressed.

## Acceptance Rules
- Source notes in the saved workbook must match the actual selected source family.
- `Current strategic context` must not collapse into timeless business-description text.
- Summary rows may stay blank or `N/A` if the safer alternative is weak or stale.
