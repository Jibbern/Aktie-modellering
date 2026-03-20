# Quarter Notes Logic Tree

## Start: source support exists?
- Structured support exists?
  - Yes -> metric-note path
  - No -> check narrative support
- Narrative support exists?
  - Yes -> narrative / policy / management-note path
  - No -> no note candidate

## Can this become a note candidate?
- Metric-note path
  - Use structured data when the operation is deterministic and low-ambiguity.
  - Typical outputs: YoY/QoQ delta, from/to change, bps move, TTM change.
- Narrative / policy path
  - Use explicit source text from earnings releases, CEO letters, annual letters, presentations, or transcripts.
  - Typical outputs: capital allocation, policy changes, management framing, causal notes.

## Family assignment
- `explanatory_driver`
- `guidance_target`
- `actual_performance_change`
- `capital_allocation`
- `financing_action`
- `debt_liquidity_refinancing`
- `monetization_commercialization`
- `operational_milestone`
- `management_framing`
- `headwind_constraint_framing`
- `other`

## Sector-pack influence
- Generic phrase families run first.
- Current sector-pack spines then add terminology and rescue coverage:
  - `capital_markets`
  - `industrial_capital_return`
  - `biofuels`
- New tickers should inherit these packs before ticker-specific rescue logic is considered.

## Blockers / filters
- Boilerplate or legal/governance filler -> drop
- Fragmentary text / table fragments -> drop
- Non-preferred narrative source -> usually drop unless allowed by profile logic
- Weak generic phrasing -> downgrade or drop
- Quarter-unsafe capital-allocation wording -> keep only if safe wording exists
- Generic `payments of dividends and distributions` / `PaymentsOfDividends` -> do not treat as common-stock dividend support
- Cumulative/program text such as `since inception`, `to date`, `under the program`, or `authorization remained` -> context only, not quarter execution
- Debt/noteholder repurchase language -> do not treat as common-stock buyback execution

## Scoring
- Quantified bonus
  - `$`, `%`, `bps`, delta, from/to, YoY/QoQ, TTM
- Explanatory bonus
  - `driven by`, `due to`, `reflecting`, `primarily from`, `ahead of plan`
- Preferred-source bonus
  - strong narrative source with explicit quarter support
- Weak-generic penalty
  - vague `improved`, `updated`, `supports margins`, generic tone rows

## Selection
- Rank by family, summary quality, detail score, and source quality.
- Keep good guidance.
- Prefer adding strong notes rather than forcing guidance out.
- Let distinct subject variants coexist when they explain different things.
- One-time event notes follow origin-quarter-only behavior by default.
  - Financing actions, exchange/subscription rows, capped-call rows, and similar event notes should live in the event quarter unless a later quarter has a true explicit update.

## Soft-cap logic
- Start from a base cap.
- Expand only for strong, distinct notes.
- Do not expand for filler, weak generic rows, or near-duplicates.

## Final visible-text dedupe
- Exact duplicate -> one survives
- Near duplicate -> richer/self-contained version survives
- Equivalent guidance target -> one survives even if formats differ (`bn` vs `m`)
- Same-family collapse -> only when notes are not meaningfully distinct

## Workbook rendering
- Final note text is rendered into `Quarter_Notes_UI`.
- Visible summary can differ from raw candidate text.
- XML safety is enforced for comments/audit text.

## Saved workbook readback
- `final_selected` means the row won the internal selection.
- `readback_verified` means it survived into the **actual saved workbook**.
- `saved_workbook_missing` means it won internally but did not survive save/export/readback.
- Preview never outranks saved workbook truth.
- If `Quarter_Notes_Audit` is enabled for the export, it should be visible as the rightmost sheet in the delivered workbook.

## Concrete examples from current truth
- PBI Q4 2025
  - `Repurchase authorization increased by $250.0m.`
  - separate `Remaining share repurchase capacity was $359.0m at quarter-end.`
  - explicit narrative support, capital-allocation family, both survive readback as separate rows
- PBI Q3 2025
  - `Repurchase authorization increased to $500.0m, up from $400.0m.`
  - explicit narrative support, survives separately from the dividend row
- PBI Q4 2025
  - `Repurchased 12.6m shares for $126.6m with an average price of $10.04/share in Q4.`
  - explicit table-backed buyback execution now survives alongside auth/dividend
- PBI Q3 2025
  - `Entered capped call transactions expected to reduce dilution from convertible notes conversion.`
  - one-time capital-markets note now stays in the event quarter instead of carrying into Q4
- PBI Q2 2025
  - `Repurchased 7.6m shares for $75.3m with an average price of $9.92/share in Q2.`
  - HTML filing-table text now survives as a clean quarter-specific capital-allocation row
- PBI Q3 2025
  - `Used $61.9m from convertible notes proceeds to repurchase 5.5m shares.`
  - origin-quarter-only use-of-proceeds note now stays in the event quarter instead of repeating into Q4
- GPRE Q2 / Q3 2025
  - cumulative wording such as `since inception ... 7.4m shares for $92.8m` is now blocked from becoming fake `in Q2/Q3` execution
  - quarter execution appears only in Q4 where the October 27, 2025 event is explicit
- PBI Q1 2025
  - duplicate FY2025 revenue guidance in `bn` and `m` formats now collapses to one visible row
  - capital-allocation rows remain in `Capital allocation / shareholder returns`, not `Guidance / outlook`
- GPRE Q1 2025
  - `Cost reduction initiatives are progressing ahead of plan...`
  - `Management is pursuing non-core asset monetization to enhance liquidity...`
  - two management notes coexist because they are distinct subjects
- GPRE Q4 2025
  - `FY 2026 45Z-related Adjusted EBITDA outlook is at least $188.0m.`
  - explicit outlook note now survives as a separate guidance row
- GPRE Q4 2025
  - `Actively marketing 2026 45Z production tax credits.`
  - survives as a separate commercialization note instead of collapsing into other 45Z notes
- GPRE Q4 2025
  - `45Z production tax credits contributed $23.4m net of discounts and other costs.`
  - explicit narrative note survives in a realized-results bucket, not as a duplicate guidance row
- GPRE Q4 2025
  - `Exchanged $170.0m of 2.25% convertible senior notes due 2027 for $170.0m of 5.25% convertible senior notes due 2030.`
  - `Issued an additional $30.0m of 5.25% convertible senior notes due 2030 to repurchase 2.9m shares.`
  - `Annualized 2026 interest expense is expected at about $30.0m-$35.0m...`
  - all three now route to Q4, while the earlier weak Q3 `2030 Notes` carry-forward row is suppressed
