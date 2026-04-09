# Repo Refactor Stage 7

## Scope
Stage 7 is a very narrow `GPRE` corn-oil-only gate/readiness pass. It does not add
a visible coproduct block to `Economics_Overlay`, and it does not expand the market-
data layer to build a new soybean-oil ingest.

## What Changed
- `Basis_Proxy_Sandbox` now includes a compact `Corn oil gate check` section.
- The section checks these four legs plus the final workbook decision:
  - `Soybean oil price proxy`
  - `Corn oil premium policy`
  - `Renewable corn oil price`
  - `Approximate coproduct credit`
  - `Overlay activation`
- The section also adds a short provenance note that:
  - premium stays manual
  - management commentary supports premium-to-soy directionally
  - commentary is not treated as an auto-filled quarterly market series

## Gate Outcome
Current verified Stage 7 outcome is `NO-GO` for a visible corn-oil block.

Why:
- no verified soybean-oil market series was found in the current `GPRE` export path
- the corn-oil premium remains manual-only
- the resolved renewable-corn-oil price remains blank
- `Approximate coproduct credit` remains blocked on the corn-oil path

That means:
- `Economics_Overlay` row `176` stays visually free
- no visible corn-oil panel is added yet

## Placement Decision
- `Operating_Drivers`
  - still owns physical/yield/mix
- `Economics_Overlay`
  - still remains the intended home for the eventual compact corn-oil economics view
  - not activated in this stage
- `Basis_Proxy_Sandbox`
  - remains the backing layer for gate status, provenance, readiness, and activation

## Explicitly Deferred
- no visible corn-oil block yet
- no DDGS / UHP / protein-mix activation in the same pass
- no new market-data/provider work for soybean oil
- no coproduct-aware fitted winner or larger overlay redesign
