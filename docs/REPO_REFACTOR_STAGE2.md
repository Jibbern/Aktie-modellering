## Repo Refactor Stage 2

This document records the next safe writer-surface step taken after stage 1.

### Scope

This stage intentionally stays narrow.

Implemented:

- baseline re-check against the stage-1 branch
- explicit documentation of the still-red market-data snapshot baseline failure
- extraction of the GPRE-critical `Economics_Overlay` support path into
  `pbi_xbrl/excel_writer_economics_overlay.py`
- cheap regression coverage so `Basis_Proxy_Sandbox` and the GPRE proxy rows do not
  disappear again

### What Moved

The new overlay surface module now owns the GPRE-specific post-market-input support path:

- `Basis_Proxy_Sandbox` sidecar write orchestration
- proxy comparison panel on `Economics_Overlay`
- proxy-implied bridge panel on `Economics_Overlay`

The main `Economics_Overlay` sheet callback still lives in
`pbi_xbrl/excel_writer_context.py`, but it now delegates the GPRE basis/proxy support
surface into the dedicated module through explicit input dataclasses.

### Why This Stage Exists

The highest-cost writer regressions in recent passes were not generic overlay styling.
They were the GPRE-specific basis/proxy support path:

- `Basis_Proxy_Sandbox` disappearing
- GPRE proxy rows disappearing from `Economics_Overlay`
- overlay formulas no longer pointing into the sandbox

This stage isolates that risk-bearing writer surface without attempting a full, high-risk
split of `excel_writer_context.py`.

### Verified Outcomes

- direct writer-path GPRE tests still show:
  - `Basis_Proxy_Sandbox`
  - `Approximate market crush ($/gal)`
  - `GPRE crush proxy ($/gal)`
  - formula links from overlay into the sandbox
- non-GPRE overlay writes do not create `Basis_Proxy_Sandbox` or GPRE-only proxy rows
- overlay timing bucket names remain stable

### Explicit Non-Goals

This stage does not:

- split the full `Economics_Overlay` sheet writer end-to-end
- refactor `Quarter_Notes_UI`
- add new hot-path memoization
- claim a runtime win
- change GPRE basis/proxy business logic

### Known Remaining Work

Next safe steps after this stage:

1. extract another low-risk writer surface:
   - `Hidden_Value_Flags`
   - `Promise_Progress_UI`
2. continue shrinking `Economics_Overlay` only if the next slice keeps the same low
   regression risk
3. either fix or formally quarantine the snapshot baseline failure in
   `test_next_quarter_thesis_snapshot_prefers_actual_bids_when_available`

### Runtime Note

This stage should be treated as runtime-neutral structural work. No speed-up is claimed.
