# Repo Refactor Stage 15: GPRE Coproduct Weighting, Coverage, and Volume-Support Audit

## Scope
Stage 15 is a focused audit and clarification pass on top of Stages B.2-B.7.

It does:
- verify the current coproduct weighting / coverage interpretation more explicitly
- add a short visible coverage note
- add a compact sandbox audit for Operating_Drivers coproduct volumes

It does not:
- add a new sheet
- add a new fitted model
- add a soybean path
- broaden into a DDGS/UHP model-expansion project
- redefine the coproduct `$m` metric

## Verified Weighting Truth

### DDGS
- weighting basis:
  - quarter-aware active-capacity footprint
- active plants assumed:
  - all active ethanol plants
- producing plants known exactly:
  - no explicit plant-level DDGS producer flag in repo metadata
- practical assessment:
  - reasonable all-active-plant approximation

### Renewable corn oil
- weighting basis:
  - same quarter-aware active-capacity footprint
- active plants assumed:
  - all active ethanol plants in the active GPRE footprint
- producing plants known exactly:
  - no
- practical assessment:
  - producer-subset approximation, not exact producer coverage

## Verified Coverage Meaning
- `Coverage` means:
  - covered active-capacity share for the direct market legs that resolved
- values shown in the coproduct lens are:
  - covered-footprint weighted averages
- important implication:
  - the current visible economics do not impute uncovered plants
  - they summarize the supported footprint only

Visible workbook note:
- `Coverage reflects covered active-capacity footprint; values are covered-footprint weighted averages.`

## Verified `$m` Meaning
- `Approximate coproduct credit ($/gal)`:
  - weighted coproduct credit `($/bushel)` divided by ethanol yield
- `Approximate coproduct credit ($m)`:
  - weighted coproduct credit `($/gal)` times the existing implied gallons basis used by the crush `$m` rows

Interpretation:
- the current coproduct `$m` is still a compact full-company approximation
- it is not a strict covered-footprint-only `$m`
- it therefore implicitly assumes the uncovered footprint has economics similar to the covered footprint

Stage 15 decision:
- do not change the metric meaning silently
- clarify the interpretation instead

## Operating_Drivers Volume Audit

### Verified volume rows
- `Distillers grains (k tons)`
- `Ultra-high protein (k tons)`
- `Renewable corn oil (million lbs)`

### Verified usability
- these rows are historical actuals
- they are useful for QA / reasonableness checks
- they are not frame-ready for:
  - `Prior quarter`
  - `Quarter-open proxy`
  - `Current QTD`
  - `Next quarter thesis`

### Locked use-case decision
- `Distillers grains volume`
  - historical usable: `YES`
  - current usable: `NO`
  - next usable: `NO`
  - best use: `QA only`
- `Renewable corn oil volume`
  - historical usable: `YES`
  - current usable: `NO`
  - next usable: `NO`
  - best use: `QA only`
- `Ultra-high protein volume`
  - historical usable: `YES`
  - current usable: `NO`
  - next usable: `NO`
  - best use: `secondary QA only`
- `Protein / coproduct mix commentary`
  - historical usable: `commentary only`
  - current usable: `NO`
  - next usable: `NO`
  - best use: `context only`

## Workbook Result

### Economics_Overlay
- keeps the existing visible coproduct block, chart, and recent-history table
- adds one short visible coverage note under `Recent coproduct history`

### Basis_Proxy_Sandbox
- keeps:
  - `Coproduct frame summary`
  - `Coproduct quarterly history`
- adds:
  - `Coproduct volume support audit`

The new sandbox audit is intentionally compact and documentation-oriented. It is there to make the QA/support role of Operating_Drivers volumes explicit without turning the workbook into a larger diagnostics product.
