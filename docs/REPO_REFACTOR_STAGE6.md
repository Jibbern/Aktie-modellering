# Repo Refactor Stage 6

## Scope
Stage 6 is a narrow GPRE coproduct inventory/readiness pass. It does not add a new
visible coproduct panel to `Economics_Overlay`, and it does not add a new coproduct
model-selection layer.

## What Changed
- `Basis_Proxy_Sandbox` now includes a compact `Coproduct signal readiness` section.
- The section tracks these first-pass signals:
  - `Renewable corn oil price`
  - `Soybean oil price proxy`
  - `Corn oil premium assumption`
  - `Implied renewable corn oil proxy price`
  - `Distillers grains price`
  - `UHP price`
  - `Approximate coproduct credit`
- Each row shows:
  - source mode
  - direct / proxy / assumption / derived status
  - whether the signal is currently filled or blank in the live GPRE path
  - historical / current / next-quarter readiness

## Placement Decision
- `Operating_Drivers`
  - primary home for physical yield / mix / process commentary
  - examples:
    - `Renewable corn oil`
    - `Protein / coproduct mix`
    - `Distillers grains / Ultra-high protein commentary`
- `Economics_Overlay`
  - remains the eventual home for the compact economic coproduct story
  - no visible coproduct block is added yet
- `Basis_Proxy_Sandbox`
  - current home for coproduct coverage, fill-state, proxy provenance, and backing logic

## Gating Rule
No visible coproduct block is added to `Economics_Overlay` until both of these are
true in the GPRE path:
- renewable-corn-oil price / proxy resolves non-blank
- approximate coproduct credit resolves non-blank

That keeps row `176` visually free until the workbook can show a small coproduct
surface without blank or misleading rows.

## First Model Order
- Stage A
  - inventory + readiness only
  - sandbox coverage / gating
- Stage B
  - corn oil first
  - direct quote or soybean-oil-proxy + premium path
- Stage C
  - DDGS second, after a real market input path is populated
- Stage D
  - UHP / protein-mix later and cautiously
  - commentary / diagnostics first

## Explicitly Deferred
- no visible coproduct block yet
- no separate coproduct sheet
- no coproduct-aware fitted winner model
- no broad writer refactor
