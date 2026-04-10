# GPRE Coproduct Stage B.6: Comparable Units + Quarter-Safe Credit Lens

## Summary
Stage B.6 is a narrow writer/readback pass on top of Stage B.5.

It does not:
- add new coproduct families
- change the primary activation rule away from `NWER`
- make `AMS 3618` a blocker again
- add a new sheet or a broader coproduct panel

It does:
- make the visible coproduct credit lens more comparable to crush
- add `$/gal` and `$m` backing for quarterly coproduct history
- switch the first visible coproduct chart to `Approximate coproduct credit ($/gal)`
- lock both quarterly charts to explicit `YYYY-Q#` helper labels

## Product Decisions
- `Renewable corn oil price` stays in `$ / lb`
- `Distillers grains price` stays in `$ / lb`
- the crush-comparable coproduct lens is:
  - `Approximate coproduct credit`
- visible overlay should stay compact, so the prior visible `$ / bushel` credit row is
  replaced by:
  - `Approximate coproduct credit ($/gal)`
  - `Approximate coproduct credit ($m)`

## Workbook Changes

### Visible block
`Economics_Overlay` now uses:
- row `176`
  - `Coproduct economics`
- row `177`
  - `Renewable corn oil price`
- row `178`
  - `Distillers grains price`
- row `179`
  - `Approximate coproduct credit ($/gal)`
- row `180`
  - `Approximate coproduct credit ($m)`

The visible `$ / bushel` credit row remains in the sandbox only.

### Sandbox history
`Basis_Proxy_Sandbox` `Coproduct quarterly history` now stores:
- `Quarter`
- `Renewable corn oil price`
- `Distillers grains price`
- `Approximate coproduct credit ($/bushel)`
- `Approximate coproduct credit ($/gal)`
- `Approximate coproduct credit ($m)`
- `Resolved source mode`

## Derivation Rules
- `Approximate coproduct credit ($/bushel)`
  - keep the existing permissive build-up definition
- `Approximate coproduct credit ($/gal)`
  - `Approximate coproduct credit ($/bushel) / ethanol yield`
- `Approximate coproduct credit ($m)`
  - `Approximate coproduct credit ($/gal) * implied gallons basis`

Rules:
- reuse the same ethanol-yield coefficient already used in the overlay
- reuse the same quarter-aware implied-gallons basis already used by the crush `($m)` lens
- if the gallons basis is unavailable, keep `($m)` blank

## Chart Policy
- first visible coproduct chart now uses:
  - `Approximate coproduct credit ($/gal, quarterly history)`
- keep it single-series and single-unit
- do not mix corn-oil and DDGS price history into the same visible chart

## Quarter Labels
Both quarterly charts should use helper-backed `YYYY-Q#` labels:
- `Approximate market crush vs Fitted models (quarterly)`
- `Approximate coproduct credit ($/gal, quarterly history)`

The category refs should point to helper strings on `Economics_Overlay`, not generic date-axis refs.

## Source Role Continuity
- `NWER`
  - primary live activation source
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback / backfill source
- `3511`
  - deferred / manual

Stage B.6 does not change those roles.
