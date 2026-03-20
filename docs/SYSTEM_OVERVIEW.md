# System Overview

## What The System Does
- Ingest SEC filings, local narrative material, and structured quarter history.
- Build evidence-aware workbook surfaces for `SUMMARY`, `Valuation`, `Quarter_Notes_UI`, QA, and audit sheets.
- Validate the saved workbook after export instead of trusting in-memory previews.

## Main Inputs
- `sec_cache/<TICKER>/`
  - Canonical filing and narrative cache layout.
- `History_Q`
  - Structured quarter history for deterministic metric rows.
- Earnings releases / CEO letters / annual letters
  - Current-quarter framing, guidance, policy, and management emphasis.
- Presentations / transcripts / local materials
  - Supporting narrative evidence where useful.

## Authoritative Paths
- `Code/pbi_xbrl/summary_overview.py`
  - Live topic-aware `SUMMARY` builder.
- `Code/pbi_xbrl/excel_writer_context.py`
  - Live workbook rendering and most `Quarter_Notes_UI` / `Valuation` logic.
- `Code/pbi_xbrl/excel_writer.py`
  - Saved-workbook provenance and readback validation.
- `Code/stock_models.py`
  - CLI entrypoint and export/readback enforcement.

## Continuity Rule
- Treat **git + docs + saved workbooks** as the durable project memory.
- Do not assume Codex/Chat thread history will be available or identical on another machine.
- For machine changes or fresh restarts, begin with:
  - [SETUP_ON_NEW_MACHINE.md](/c:/Users/Jibbe/Aktier/Code/docs/SETUP_ON_NEW_MACHINE.md)
  - [BASELINE_FREEZE_2026-03-20.md](/c:/Users/Jibbe/Aktier/Code/docs/BASELINE_FREEZE_2026-03-20.md)
  - [CURRENT_PASS.md](/c:/Users/Jibbe/Aktier/Code/docs/CURRENT_PASS.md)

## Active Workbook Dataflow
1. Pipeline artifacts are built from filings, structured facts, and narrative evidence.
2. `summary_overview.build_company_overview()` resolves topic-aware `SUMMARY` rows.
3. `excel_writer_context` resolves `Valuation` inputs and final visible note rows.
4. `Quarter_Notes_UI`, `SUMMARY`, and `Valuation` are written to the workbook.
5. The saved workbook is reopened and verified against expected output.

## Cache Policy
- `doc_intel_bundle` and `company_overview` stage-cache keys now include explicit behavior versions plus code signatures.
- This is intended to keep code patches from being hidden behind stale stage cache.

## Key Product Rules
- Saved workbook is truth.
- Conservative blanks are better than contaminated values.
- Common dividends require explicit common-stock support.
- Quarter buyback execution requires explicit quarter-safe evidence.
- Program context and remaining authorization may appear as context, but not as execution metrics.
- Visible `Quarter_Notes_UI` badges should be limited to `NEW`, `CONTINUED`, and `REAFFIRMED`.
- Origin-quarter-only events should not auto-carry forward as continued notes.
- `Valuation` leverage / coverage labels must match the actual denominator family.
- Use `N/M` when the relevant EBITDA denominator is non-meaningful.

## Summary Architecture
- `SUMMARY` is topic-aware, not single-document-driven.
- `What the company does`
  - Prefer original `10-K`, then `10-Q`, then `8-K` “About” fallback.
- `Current strategic context`
  - Prefer latest earnings `8-K` / `EX-99.1`, then CEO letter / `EX-99.2`, then `10-Q` MD&A context.
- `Key competitive advantage`
  - Prefer `10-K` competition / segment language, with current-quarter materials only as support.
- Administrative amendments should not replace the real business / risk source.

## Valuation Architecture
- `Valuation` now uses a resolved capital-return layer rather than letting note text or generic program text drive numeric output.
- That resolved layer separates:
  - quarter-safe buyback execution
  - common-dividend support
  - authorization / remaining-capacity context
  - provenance and suppress reasons
- `Valuation` and `Quarter_Notes_UI` should converge when the same explicit SEC buyback evidence is available.

## Current Workbook Truth
- PBI `SUMMARY`
  - Source-driven company description from the `10-K`
  - Current strategic context focused on capital allocation, cost discipline, execution, and guidance accuracy into 2026
- GPRE `SUMMARY`
  - Source-driven company description from the `10-K`
  - Current strategic context focused on `45Z` monetization, CCS execution, and broader low-carbon value realization into 2026
- PBI `Valuation`
  - Latest-quarter buybacks now use filing-table truth: `12.614m` shares, `$126.6m`, `$10.04/share`
- GPRE `Valuation`
  - Latest-quarter buybacks now use explicit Q4 execution truth: `2.9m` shares, `$30.0m`
- `Quarter_Notes_Audit`
  - Visible and rightmost in current CLI-delivered workbook exports

## Current Watchlist
- 2024 historical note coverage is still thinner than 2025 for both PBI and GPRE.
- `QA_Buybacks` / `QA_Checks` still lag the final visible product quality in a few places.
- Net income / EBITDA / Adjusted EBITDA provenance review is improved but still not fully complete.
- Some GPRE labels and management-note wording can still be polished further.
