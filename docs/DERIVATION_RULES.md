# Derivation Rules

## Safe To Derive
- YoY / QoQ deltas
- From/to changes
- Bps changes
- TTM changes
- Deterministic metric summaries from structured support

Examples:
- `FCF TTM improved by $198.7m YoY.`
- `Revolver availability increased from $265.0m to $400.0m.`

## Must Be Explicit
- Common-stock dividends
- Dividend changes
- Authorization increases
- Remaining capacity
- Buyback share count
- Financing / convertible / use-of-proceeds notes
- Dilution-mitigation notes such as capped-call language
- Management-tone / policy notes
- Causal / `driven by` notes

Examples:
- `Quarterly dividend set at $0.09/share.`
- `Quarterly dividend increased to $0.09/share.`
- `Repurchase authorization increased by $250m.`
- `Remaining share repurchase capacity was $359m at quarter-end.`
- `Issued $230m of 1.50% convertible senior notes due 2030; net proceeds were $221m.`
- `Used $61.9m of proceeds to repurchase 5.5m shares.`
- `Gross margin expanded ... driven by ...`

## Cumulative-Only Wording
- Use cumulative/program-safe wording when quarter execution is not explicitly stated.
- Do not convert cumulative buyback detail into quarter-specific wording unless the source says it.
- Generic `PaymentsOfDividends`, `payments of dividends and distributions`, or similar distribution/disbursement facts do not count as common-stock dividend support.
- Phrases such as `since inception`, `to date`, `under the program`, `may repurchase`, or `authorization remained` are context only, not quarter execution.

Example:
- Safe cumulative wording: `Repurchased 25.9m shares for $281.2m since starting the program earlier this year.`

## Quarter-Safe Wording Rules
- Prefer `in Q1/Q2/Q3/Q4` only when the source is explicit.
- If amount is explicit but share count is not, keep dollar-only wording.
- If buyback tables explicitly provide shares + dollars + average price, include all three in the note.
- If a repurchase table gives any two of: shares, dollars, average price, the third may be derived only when the math is deterministic and rounding risk is low.
- When `Quarter_Notes_UI` and `Valuation` can both see the same explicit SEC repurchase evidence, they should converge on the same latest-quarter buyback facts.
- If policy/action is explicit but timing is broader than the quarter, use period-safe wording.
- One-time event notes should live in the quarter of occurrence by default.
  - financing actions, exchange/subscription rows, capped-call rows, and similar event notes should not repeat into later quarters without a new explicit update.
- If adjusted EBITDA is not explicit or auditable from a real bridge, do not silently substitute adjusted EBIT.

Example:
- Safe when only dollars are available: `Repurchased $75.0m of shares in Q2.`
- Preferred when explicit: `Repurchased 14.1m shares for $161.3m with an average price of $11.44/share in Q3.`
- Preferred when a filing table is explicit: `Repurchased 7.6m shares for $75.3m with an average price of $9.92/share in Q2.`
- Preferred capital-markets wording when explicit: `Issued an additional $30.0m of 5.25% convertible senior notes due 2030 to repurchase 2.9m shares.`
- Preferred valuation-aligned wording when explicit: `Repurchased 2.9m shares for $30.0m with an average price of $10.34/share in Q4.`
- Unsafe without explicit support: buyback share count when the source only discloses dollar spend

## Partial Support Rule
- When support is partial, use the safer weaker wording rather than more specific inferred wording.

## Short Decision Rule
- Structured data can support metric notes.
- Narrative text must support policy, capital-allocation, dividend, authorization, management-tone, and causal notes.
