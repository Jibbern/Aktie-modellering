## Repo Refactor Stage 1

This document records the first safe implementation slice from the broad repo check.

### Scope

This stage intentionally focuses on structure and contracts, not on changing business logic
or claiming runtime wins.

Implemented:

- writer core types moved out of `excel_writer_context.py` into
  `pbi_xbrl/writer_types.py`
- centralized empty-sheet placeholder contract in
  `pbi_xbrl/excel_writer_placeholders.py`
- deterministic audit/info-log assembly helpers in
  `pbi_xbrl/qa_outputs.py`
- shared market-data schema/column contract helpers in
  `pbi_xbrl/market_data/contracts.py`
- explicit GPRE traceability map in
  `docs/GPRE_TRACEABILITY_MAP.md`
- focused smoke tests in
  `tests/test_writer_and_market_contracts.py`

### Why This Stage Exists

The codebase still has two large responsibility concentrations:

- `pbi_xbrl/excel_writer_context.py`
- `pbi_xbrl/market_data/service.py`

Before moving more visible product surfaces out of those files, we needed a smaller set of
shared contracts and types that other modules can depend on without increasing coupling.

### Verified Outcomes

- empty product sheets now use the shared `"No data for current build"` placeholder
- market-data internal paths can fail fast with a clear contract when required columns are
  missing
- QA/info-like row assembly now has a deterministic exact-duplicate dedupe step
- the new traceability map documents verified links from cache/material inputs to pipeline
  artifacts and workbook surfaces

### Explicit Non-Goals

This stage does not:

- split `Quarter_Notes_UI`
- add new hot-path memoization
- change official/fitted workbook logic
- claim a speed-up without measured before/after proof

### Known Remaining Work

Next safe steps:

1. move low-risk writer surfaces out of `excel_writer_context.py`
   - `Economics_Overlay`
   - `Hidden_Value_Flags`
   - `Promise_Progress_UI`
2. break `market_data/service.py` into responsibility-specific modules behind the current
   public API
3. resolve or formally quarantine the remaining market-data snapshot baseline failure before
   deeper refactors

### Runtime Note

This stage should be treated as a structure/robustness pass. It is expected to be runtime
neutral. No quarter-notes hot-loop optimization is claimed here.
