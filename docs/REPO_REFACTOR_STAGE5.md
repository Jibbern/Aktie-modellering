## Repo Refactor Stage 5

This document records the current Economics_Overlay / GPRE proxy completion pass taken
after stage 4.

### Scope

This stage stays inside the `Economics_Overlay` / `Basis_Proxy_Sandbox` sphere.

Implemented:

- explicit sheet-order placement of `Basis_Proxy_Sandbox` directly after
  `Promise_Progress_UI`
- four new bounded GPRE basis/proxy candidates added to the comparison set
- explicit separation between:
  - `Best historical fit`
  - `Best compromise`
  - `Best forward lens`
  - `Production winner`
- a short stage-5 note row under `Proxy comparison ($/gal)` on `Economics_Overlay`
- a second quarterly GPRE proxy chart under the existing weekly chart
- readback/test protection for the new story, chart, and sheet-order contract

### What Changed

`pbi_xbrl/market_data/service.py` now evaluates four additional bounded candidates:

- `process_market_process_ensemble_35_65`
- `process_locked_share_asymmetric_passthrough`
- `process_prior_gap_carryover_small`
- `process_prior_disturbance_carryover`

The candidate comparison now also records forward-useful diagnostics such as:

- `walk_forward_tail_mae`
- `signal_coverage_ratio`
- `forward_usability_rating`
- `complexity_rating`

The GPRE result bundle now exposes explicit role keys:

- `best_historical_fit_model_key`
- `best_compromise_model_key`
- `best_forward_lens_model_key`

### Workbook Surface Outcome

`Economics_Overlay` now communicates the proxy split more clearly:

- official row = `Approximate market crush`
- fitted row = `GPRE crush proxy`
- production winner is named explicitly
- best forward lens is named explicitly when it differs

`Basis_Proxy_Sandbox` now carries those same winner roles in the `Winner story` block so
the workbook keeps one product-facing story instead of making the user infer it from the
raw comparison table.

### Charts

The existing weekly chart remains in place:

- `Simple crush margin proxy (weekly)`

Because no separate verified weekly fitted-GPRE history series exists in the current
path, stage 5 adds a second quarterly chart instead of overloading the weekly one:

- `Approximate market crush vs Fitted models (quarterly)`

This keeps the official/simple weekly view intact while still giving the user a clean
fitted-vs-official visual comparison.

In the current saved-workbook surface, that quarterly chart is a simple quarter-labeled
3-series chart:

- `Approximate market crush`
- `GPRE crush proxy`
- `Best forward lens`

### Verified Outcomes

- live temp-workbook acceptance for GPRE overlay / sandbox passed
- regenerated local `GPRE_model.xlsx` readback now shows:
  - `Basis_Proxy_Sandbox` directly after `Promise_Progress_UI`
  - weekly chart still present
  - new quarterly chart present under the weekly chart
  - quarterly chart title aligned with the current fitted-model wording
  - updated winner story with `Best historical fit`, `Best compromise`, and
    `Best forward lens`
- stage-1–4 nearby protections still pass in the targeted slices

### Production Winner Note

Stage 5 does **not** force a new production winner.

Current verified result:

- production winner: `process_utilization_regime_residual`
- best historical fit: `process_utilization_regime_residual`
- best compromise: `process_utilization_regime_blend`
- best forward lens: `process_quarter_open_blend`

This is intentional. The workbook now distinguishes forward-useful lenses from the
promotion-guarded production winner instead of collapsing them into one label.

### Explicit Non-Goals

This stage does not:

- refactor `Quarter_Notes_UI`
- add memoization or runtime caches
- introduce an optimizer or opaque fitting layer
- claim a runtime speed-up
- widen into a broad writer-architecture pass

### Runtime Note

This stage should be treated as correctness / clarity work inside the existing overlay
surface. No speed-up is claimed.
