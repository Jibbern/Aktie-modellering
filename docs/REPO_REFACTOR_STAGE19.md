# Repo Refactor Stage 19

## GPRE Current QTD Snapshot Trend Tracking

### What changed
- `GPRE` now retains `Current QTD` snapshot history outside the workbook in:
  - `GPRE/basis_proxy/gpre_current_qtd_snapshots.parquet`
  - `GPRE/basis_proxy/gpre_current_qtd_snapshots.csv`
- The retained primary lens is now the crush-margin frame:
  - `Approximate market crush`
- Legacy all-in / coproduct fields remain in the sidecar for compatibility and audit support, but they are no longer the primary tracking definition.
- `Economics_Overlay` now shows a compact `Current QTD trend tracking ($/gal, crush margin lens)` section directly under the quarterly crush chart.
- The visible `Coproduct economics` block remains intact in substance but is shifted lower so the new tracking area has dedicated space.

### Why it changed
- A single static `Current QTD` number was not enough to explain what changed and why.
- The workbook needed a compact decision surface that can answer:
  - where `Current QTD` stands versus quarter-open
  - whether the move is new or persistent over recent weeks
  - which first-order drivers explain the move
- The history had to be real retained state, not reconstructed after the fact from quarterly series.

### Retention policy
- A retained row is appended only when the current-quarter input fingerprint changes.
- Unchanged reruns do not create duplicate retained rows.
- Weekly checkpoints are the latest retained row in each ISO week.
- Retention is GPRE-only.
- The workbook is not the canonical history store.

### Lookback policy
- `Quarter-open`
  - existing retained/frozen quarter-open reference for the same quarter
- `1w`
  - latest same-quarter weekly checkpoint with `as_of_date <= current_as_of - 7 days`
- `4w`
  - latest same-quarter weekly checkpoint with `as_of_date <= current_as_of - 28 days`
- `8w`
  - latest same-quarter weekly checkpoint with `as_of_date <= current_as_of - 56 days`
- No cross-quarter fallback is used.
- Missing history stays blank / `insufficient history`.
- Same-point-last-quarter is intentionally not the main tracking lens because GPRE quarter economics are too cyclical.

### Driver attribution
- The displayed crush-margin move is decomposed into:
  - `Ethanol`
  - `Flat corn`
  - `Corn basis`
  - `Gas`
- No `Coproducts` or `Residual` row is displayed in this tracker.

### Guardrails
- no new sheet in v1
- no change to official/simple rows
- no change to production winner selection
- no change to best-forward logic
- no non-GPRE leakage
