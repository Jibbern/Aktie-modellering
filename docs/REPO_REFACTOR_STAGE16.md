# Repo Refactor Stage 16: GPRE Coproduct-Aware Crush Experiments

## Scope
Stage 16 is a focused experimental-modeling pass on top of Stages B.2-B.8.

It does:
- accept the current DDGS / corn-oil approximation as good enough for a first coproduct-aware lens
- add a separate GPRE-only coproduct-aware experimental family
- evaluate those methods with the existing GPRE comparison framework
- surface them in `Basis_Proxy_Sandbox` as comparison-only models

It does not:
- change the official/simple row
- replace the current production winner
- replace the current best forward lens
- auto-promote a coproduct-aware method
- add a new sheet

## Experimental Family

### Locked method set
- `simple_plus_full_credit`
- `simple_plus_half_credit`
- `simple_plus_coverage_credit`
- `winner_plus_full_credit`
- `forward_plus_full_credit`
- `winner_plus_conservative_credit`

### Locked policy
- all methods are ranked in `$/gal`
- all methods use `Approximate coproduct credit ($/gal)` as the overlay signal
- `Coverage` only enters explicitly coverage-aware variants
- `$m` remains bridge/support only
- every coproduct-aware method is:
  - `comparison_only = True`
  - `eligible_official = False`

## Roles
Within the coproduct-aware family Stage 16 now tracks:
- `Best historical coproduct-aware`
- `Best forward coproduct-aware`
- `Best coproduct-aware experimental lens`

Important policy:
- `Best coproduct-aware experimental lens` follows the coproduct-family compromise role
- this does not change the main production winner-story

## Workbook Result

### Economics_Overlay
- keeps the production/reference rows unchanged
- adds only one short pointer note in the proxy-comparison area:
  - `Coproduct-aware experimental lenses live in Basis_Proxy_Sandbox and are comparison-only.`

### Basis_Proxy_Sandbox
- adds a dedicated `Coproduct-aware experimental lenses` section
- the section includes:
  - a compact summary block
  - a compact comparison table
- every experimental candidate is explicitly marked:
  - `comparison only`

## Guardrails
- the production winner-story stays intact
- coproduct-aware methods are visible for evaluation, not promotion
- if a method looks interesting, that is a separate follow-up decision, not an automatic winner change
