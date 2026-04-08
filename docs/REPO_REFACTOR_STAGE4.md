## Repo Refactor Stage 4

This document records the next safe writer-surface step taken after stage 3.

### Scope

This stage intentionally stays narrow.

Implemented:

- baseline re-check against the stage-3 branch
- explicit confirmation that the remaining market-data thesis snapshot failure is still a
  pre-existing baseline issue
- extraction of the visible `Promise_Progress_UI` sheet surface into
  `pbi_xbrl/excel_writer_promise_progress.py`
- small regression coverage for the Promise Progress surface contract and debug-scope
  behavior

### What Moved

The new Promise Progress surface module now owns the visible worksheet contract for
`Promise_Progress_UI`:

- sheet creation for the visible Promise Progress sheet
- title / generated-at scaffold
- Promise Progress block header rendering
- orchestration of stacked-quarter block rendering
- post-render cleanup execution on the visible worksheet
- final visible formatting, fills, widths, row heights, and freeze-pane behavior

### What Stayed Put

The highest-risk shared logic intentionally stays in
`pbi_xbrl/excel_writer_context.py` for this stage:

- `_ensure_promise_progress_ui_bundle(...)`
- bundle-cache wiring and reuse
- visible row assembly / hydration / dedupe / PBI-/GPRE-row repair
- the Promise Progress row writer, which still depends on shared quarter-notes /
  tracker helpers
- Promise-specific cleanup logic bodies that still rely on shared helper families

### Why This Stage Exists

`Promise_Progress_UI` is a useful next surface because it has a real visible contract, but
its data assembly is more tightly coupled to shared quarter-notes / tracker logic than
`Hidden_Value_Flags`.

This stage therefore moves the visible worksheet surface while leaving the shared
hydration risk zone in place.

### Verified Outcomes

- stage-2 GPRE overlay / sandbox slices still pass
- stage-3 hidden-value slices still pass
- Promise Progress direct writer slices still pass
- Promise Progress delivered workbook readback still passes
- `drivers` / `ui` debug scopes explicitly do not emit `Promise_Progress_UI`

### Explicit Non-Goals

This stage does not:

- refactor `Quarter_Notes_UI`
- move Promise Progress bundle assembly / hydration into a new module
- refactor the market-data layer
- add memoization or new cache layers
- claim a runtime win
- fix the remaining market-data thesis snapshot baseline failure

### Known Remaining Work

Next safe steps after this stage:

1. extract another low-risk writer surface that is less coupled than `Quarter_Notes_UI`
2. handle the remaining market-data thesis / bid-weighting failure separately from writer
   refactors
3. continue shrinking `excel_writer_context.py` only where the next slice keeps the same
   low regression risk

### Runtime Note

This stage should be treated as runtime-neutral structural work. No speed-up is claimed.
