# Repo Refactor Stage 14: GPRE Coproduct Frames, Weighted History, and Readable Quarterly Charts

## Scope
Stage 14 is a broader but still focused coproduct-economics pass on top of Stages 9
through 13.

It does:
- fix the two quarterly charts so they use explicit quarter-string categories
- add a weighted `Coproduct frame summary` for prior/open/current/next
- rebuild coproduct quarterly history on the same weighted lens
- add a compact visible mini-history table under the coproduct chart

It does not:
- add a new sheet
- add a new fitted model
- change the source gate
- broaden into soybean or a full DDGS/UHP model
- turn `AMS 3618` into a blocker again

## Quarter-label policy
The workbook now treats both quarterly charts as quarter-string charts, not generic
date-axis charts.

Implementation rule:
- categories come from same-sheet helper cells on `Economics_Overlay`
- chart categories are bound through explicit `strRef`
- label format is `YYYY-Q#`

This applies to:
- `Approximate market crush vs fitted models (quarterly)`
- `Approximate coproduct credit ($/gal, quarterly history)`

## Coproduct frame summary
`Basis_Proxy_Sandbox` now contains a compact `Coproduct frame summary` section above
`Coproduct quarterly history`.

Frames:
- `Prior quarter`
- `Quarter-open proxy`
- `Current QTD`
- `Next quarter thesis`

Columns:
- `Frame`
- `Renewable corn oil price`
- `Distillers grains price`
- `Approximate coproduct credit ($/bushel)`
- `Approximate coproduct credit ($/gal)`
- `Approximate coproduct credit ($m)`
- `Resolved source mode`
- `Coverage`
- `Rule`

Frame policy:
- prior uses weighted last-completed-quarter values
- quarter-open uses an early-quarter weighted snapshot when available
- quarter-open otherwise falls back to prior-quarter carry-forward
- current uses weighted resolved current-quarter values
- next carries forward the latest resolved weighted coproduct value because there is
  no direct forward coproduct curve

## Weighting rule
Stage 14 moves coproduct history away from the old single resolved row lens and onto
a quarter-aware active-capacity weighting approach.

Current rule:
- DDGS:
  - weighted across the active ethanol plant footprint
- corn oil:
  - uses the same active-footprint weighting
  - explicitly treated as a producer-subset approximation because repo metadata does
    not yet expose plant-level corn-oil producer flags

Coverage policy:
- unsupported active-footprint share is not silently absorbed
- the weighted supported share is shown as `Coverage`

## History + visible mini-history
`Coproduct quarterly history` now stores:
- `Quarter`
- `Renewable corn oil price`
- `Distillers grains price`
- `Approximate coproduct credit ($/bushel)`
- `Approximate coproduct credit ($/gal)`
- `Approximate coproduct credit ($m)`
- `Resolved source mode`
- `Coverage`

The visible overlay now adds a compact mini-history table below the coproduct chart.

Visible table:
- title row `203`: `Recent coproduct history`
- header row `204`
- data rows `205:212`

Columns:
- `Quarter`
- `Approximate coproduct credit ($/gal)`
- `Approximate coproduct credit ($m)`
- `Source mode`
- `Coverage`

## Source roles
Stage 14 does not change source roles:
- `NWER`
  - primary live activation source
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback / backfill
- `3511`
  - deferred / manual

## Remaining limitation
The main remaining limitation is not source gating. It is plant-level coproduct
coverage precision.

What is still missing:
- exact plant-level corn-oil producer flags
- exact producer-subset coverage for each coproduct family
- a true forward market curve for coproduct legs

That is why Stage 14:
- exposes `Coverage`
- writes the `Rule`
- keeps `Next quarter thesis` as an explicit carry-forward when no better direct
  coproduct forward lens exists
