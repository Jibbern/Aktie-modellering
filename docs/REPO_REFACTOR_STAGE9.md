# Repo Refactor Stage 9

## Scope
Stage 9 is a very narrow `GPRE` workbook/gating pass on top of Stage 8.

It does:
- allow the first visible coproduct block to activate with `NWER` as the sufficient primary source
- keep `AMS 3618` visible as a secondary / corroborating source
- keep the visible block compact and explicitly economic

It does not:
- add a soybean path
- add a DDGS/UHP full model
- add a new sheet
- add a fitted winner or broader coproduct panel

## Verified Gate Decision
Stage 8 already proved the live `GPRE` path had:
- `NWER coproduct rows = YES`
- `Renewable corn oil price = YES`
- `Distillers grains price = YES`
- `Approximate coproduct credit = YES`

The only remaining blocker was that `AMS 3618` was still `NO`.

Stage 9 changes the activation rule so the first visible coproduct surface can go live
when the current-quarter `NWER`-backed legs are non-blank.

`AMS 3618` now remains:
- secondary
- corroborating
- future improvement

It no longer blocks the first visible block by itself.

## Workbook Change
`Economics_Overlay` now uses rows `176:179` for a compact visible quarter-grid:
- row `176`
  - `Coproduct economics (NWER-backed)`
- row `177`
  - `Renewable corn oil price`
- row `178`
  - `Distillers grains price`
- row `179`
  - `Approximate coproduct credit`

The visible rows are intentionally small:
- no physical/yield mini-table
- no extra note row
- no larger panel below the quarterly chart

Source wiring:
- `Renewable corn oil price`
  - links to the existing hidden overlay market row
- `Distillers grains price`
  - links to the existing hidden overlay market row
- `Approximate coproduct credit`
  - links to the existing `Basis_Proxy_Sandbox` build-up quarter cells
  - uses sandbox columns `C / E / G / I`

## Sandbox Story
`Basis_Proxy_Sandbox` keeps:
- `Coproduct source gate`
- `Coproduct signal readiness`

Stage 9 changes the story from:
- `hold visible block until AMS 3618 is green`

to:
- `allow first visible block when NWER-backed current-quarter legs are sufficient`

The saved workbook now shows:
- `NWER coproduct rows = YES`
- `AMS 3618 coproduct rows = NO`
- `Renewable corn oil price = YES`
- `Distillers grains price = YES`
- `Approximate coproduct credit = YES`
- `Overlay activation = GO`

## Product Boundary
This visible block is economic only.

It does not change the ownership split:
- `Operating_Drivers`
  - physical / yield / mix
- `Economics_Overlay`
  - price / contribution / economic explanation
- `Basis_Proxy_Sandbox`
  - source gate / readiness / build-up backing

## Verification Notes
Stage 9 acceptance is based primarily on:
- workbook labels
- workbook formulas
- sandbox gate state

This is intentional because local Excel COM/VBA injection remains unstable, so
`data_only=True` cache values are not the primary acceptance signal here.
