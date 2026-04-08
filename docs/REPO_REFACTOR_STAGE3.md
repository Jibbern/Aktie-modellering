## Repo Refactor Stage 3

This document records the next safe writer-surface step taken after stage 2.

### Scope

This stage intentionally stays narrow.

Implemented:

- baseline re-check against the current stage-2 branch
- explicit confirmation that the remaining market-data thesis snapshot failure is still a
  pre-existing baseline issue
- a small hidden-value baseline fix in `pbi_xbrl/signals.py` so `build_signals_base(...)`
  falls back to GAAP EBITDA TTM when adjusted EBITDA TTM is unavailable
- extraction of the `Hidden_Value_Flags` sheet surface into
  `pbi_xbrl/excel_writer_hidden_value_flags.py`
- small regression coverage so the `Valuation -> Hidden_Value_Flags` formula contract and
  debug-scope behavior stay stable

### What Moved

The new hidden-value surface module now owns the `Hidden_Value_Flags` sheet write path:

- row append / sheet creation
- sheet-local widths and evidence wrapping
- score heatmap and row-level status coloring
- legacy fallback formatting for flag-style fallback headers
- table creation for the visible flags sheet

The main writer still derives hidden-value inputs in `pbi_xbrl/excel_writer_core.py`, and
`finalize_workbook(...)` still owns the audit-to-flags formula sync.

### Why This Stage Exists

`Hidden_Value_Flags` is a good low-risk surface because:

- it already has a stable visible contract
- `Valuation` formulas explicitly point into it
- it can move without touching `Quarter_Notes_UI`
- the risky cross-sheet synchronization already lives in `finalize_workbook(...)` and can
  stay there for now

### Verified Outcomes

- stage-2 GPRE overlay/sandbox slices still pass
- delivered workbook readback for `Valuation` still passes
- drivers/ui debug-scope slices still pass
- the hidden-value signals baseline now expects GAAP EBITDA TTM fallback when adjusted
  EBITDA TTM is unavailable

### Explicit Non-Goals

This stage does not:

- refactor the full hidden-value domain
- move `Hidden_Value_Audit`, `Hidden_Value_Recompute`, or `Hidden_Value_Base`
- touch `Quarter_Notes_UI`
- refactor `Promise_Progress_UI`
- claim a runtime win
- fix the remaining market-data thesis snapshot baseline failure

### Known Remaining Work

Next safe steps after this stage:

1. extract another low-risk writer surface:
   - `Promise_Progress_UI`
   - or a smaller hidden-value-adjacent report surface
2. handle the remaining market-data thesis/bid-weighting failure separately from writer
   refactors
3. continue shrinking `excel_writer_context.py` only where the next slice keeps the same
   low regression risk

### Runtime Note

This stage should be treated as runtime-neutral structural work. No speed-up is claimed.
