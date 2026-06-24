# Excel Writer Context Architecture Contract

This note records the current, behavior-preserving modularization boundary around
`pbi_xbrl/excel_writer_context.py`. It is an architecture contract for future
maintenance, not an instruction to change workbook output.

## Current Shape

- `excel_writer_context.py` has 4,869 physical lines.
- `build_writer_context` spans lines 2042-4869, for 2,828 lines.
- The file contains 132 top-level helpers plus `build_writer_context`.
- `build_writer_context` contains 291 nested helpers.

The recent remaining-context audit recommended: pause small adapter extractions.
Further changes should be justified by risk-adjusted maintainability payoff, not
line count alone.

## Context Role

`excel_writer_context.py` is now primarily the Excel writer integration surface.
It assembles workbook state, runtime state, profile flags, cached inputs,
callback objects, compatibility aliases, and helper maps for the writer runtime.

Many writer and support responsibilities have been extracted, but context still
owns the places where extracted modules are wired together. This is intentional:
those boundaries preserve the stable callback names and runtime keys used by
`excel_writer.py`, `excel_writer_ui.py`, `excel_writer_core.py`, tests, and
extracted modules.

## Why `build_writer_context` Still Owns Wiring

`build_writer_context` still owns runtime and callback wiring because it closes
over the workbook, current pipeline inputs, source/cache state, profile-specific
runtime values, writer caches, and compatibility state. Moving that wiring
casually risks changing callback identity, callback registration order, runtime
publication, sheet availability, or helper injection.

It should continue to own:

- `WriterRuntimeData` construction.
- `WriterCallbacks` construction.
- desired sheet order.
- `extra_callbacks`.
- callback and helper map construction.
- runtime publication into `ctx.state`.
- compatibility state construction.

## Extracted Modules

The context currently relies on these extracted modules:

- `excel_writer_bs_segments_sheet_adapter.py`: builds `BsSegmentsWriterDeps` and
  delegates the visible `BS_Segments` sheet adapter.
- `excel_writer_operating_drivers_sheet_adapter.py`: builds
  `OperatingDriversWriterDeps` and delegates the visible `Operating_Drivers`
  sheet adapter.
- `excel_writer_chart_text_support.py`: owns chart text/category helper support
  used by Economics Overlay chart rendering.
- `excel_writer_investment_case_readability.py`: owns the Investment Case
  readability layout pass.
- `excel_writer_sector_investment_case_support.py`: owns the sector/segment
  Investment Case helper cluster for margin/default support.
- `excel_writer_sector_operating_driver_intro_support.py`: owns sector
  Operating Drivers intro table support.
- `excel_writer_evidence_source_support.py`: owns Quarter Notes and Promise
  evidence DataFrame builders.
- `excel_writer_economics_overlay_sheet.py`: owns the context adapter that
  delegates to the Economics Overlay orchestrator.
- `excel_writer_hidden_value_support.py`: owns Hidden Value fallback and
  flags-sheet adapter support.
- `excel_writer_analysis_sheet_layout_support.py`: owns analysis sheet title,
  metadata, and stacked-quarter layout helpers.
- `excel_writer_operating_drivers_raw_sheet.py`: owns the
  `operating_drivers_raw` sheet writer.
- `excel_writer_anf_qa_support.py`: owns ANF QA status normalization support.
- `excel_writer_legacy_ui_writers.py`: owns legacy Quarter Notes and Promise
  Tracker UI writers while keeping them unwired from active callbacks.
- `excel_writer_source_root_support.py`: owns source root support wrappers.
- `excel_writer_cached_document_support.py`: owns cached document support
  wrappers.
- `excel_writer_sec_cache_support.py`: owns SEC cache support wrappers.

These modules must not import `excel_writer_context.py`. Context may import them
and expose compatibility wrappers.

## Context-Level Wrapper Policy

Context-level wrappers do not all have the same status. Keep the distinction
visible so future cleanup does not confuse active production wiring with retired
legacy compatibility names.

### Active Production And Callback Wrappers

These wrappers are active production paths. They are registered through
`WriterCallbacks`, exposed through `writer_types.as_state_mapping()`, called by
the writer runtime, or directly determine generated workbook surfaces:

- `_write_bs_segments_sheet`
- `_write_operating_drivers_sheet`
- `_write_operating_drivers_raw_sheet`
- `_write_economics_overlay_sheet`
- `_write_flags_sheet`
- `_build_hidden_value_flags_fallback`
- `_build_qn_evidence_src`
- `_build_promise_evidence_src`
- `_write_quarter_notes_ui_v2`
- `_write_promise_tracker_ui_v2`
- `_write_promise_progress_ui_v2`

Do not delete or bypass these wrappers without a dedicated behavior-preserving
callback/runtime change and workbook parity plan.

### Runtime-Injected Compatibility Wrappers

These wrappers are not necessarily direct `WriterCallbacks` fields, but extracted
modules still receive them through runtime/deps maps. They intentionally preserve
context-level helper names for injected dependencies:

- `_write_analysis_sheet_title_and_metadata`
- `_render_stacked_quarter_blocks`
- `_apply_chart_text_categories`
- `_polish_investment_case_readability`
- `_sector_operating_driver_intro_tables`
- `_company_operating_margin_proxy_from_workbook`
- `_bs_segments_latest_segment_margin_from_workbook`

These are compatibility wrappers, not deletion candidates in ordinary refactors.
Remove them only after a focused compatibility-removal audit proves extracted
modules, runtime maps, tests, and monkeypatch usage no longer depend on the
context-level name.

### Retire-Later / Test-Contract Wrappers

These names are retained temporarily for tests, import compatibility, and
manual/debug history. They are not active production callbacks:

- `_write_quarter_notes_ui`
- `_write_promise_tracker_ui`

The legacy UI wrappers remain unwired. Production uses
`write_quarter_notes_ui_v2(...)`,
`write_promise_tracker_ui_v2(render_visible=False)`, and
`write_promise_progress_ui_v2()`. `Promise_Tracker_UI` is currently absent in
production outputs.

Do not delete retire-later wrappers in unrelated cleanup. They can only be
removed after tests/docs are migrated and a dedicated deletion PR or audit proves
there is no production, import, callback, runtime, or manual-debug dependency.
Behavior tests for extracted functionality target the extracted module APIs;
context-level retire-later wrappers receive only lightweight presence and policy
classification checks.
`LegacyUIWriters.write_quarter_notes_ui` and
`LegacyUIWriters.write_promise_tracker_ui` now have direct in-memory behavior
tests; their context wrappers remain RETIRE-LATER candidates for a dedicated
deletion change.

The supported behavior-test surfaces for the retired sector-label and ANF QA
helpers are `SectorInvestmentCaseSupport.segment_scenario_label_aliases` and
`AnfQASupport.normalize_qa_status_rows`.

## Context-Owned Seams To Keep

These seams should stay in context for now:

- latest-quarter QA/cache seam.
- `WriterRuntimeData`.
- `WriterCallbacks`.
- desired sheet order.
- `extra_callbacks`.
- callback construction.
- runtime publication.
- Valuation adapter/orchestrator glue.
- active Promise/Quarter v2 callbacks.
- generic `_write_sheet`.

The latest-quarter QA/cache seam is especially sensitive because it bridges
source text, cached document support, SEC cache support, review status, and QA
surfaces.

## High-Risk Areas

The following remaining blocks are active, cross-cutting, or output-sensitive
and should not be moved casually:

- `_write_valuation_sheet`: high-risk central Valuation logic.
- `_write_promise_progress_ui_v2`: focused Promise audit required first.
- `_write_promise_tracker_ui_v2`: focused Promise audit required first.
- `_economics_overlay_sheet_runtime`: runtime/dependency assembly, keep.
- `_quarter_notes_context_adapter_deps`: Quarter Notes dependency glue, keep.
- `_get_latest_quarter_qa_support`: source/cache/QA factory seam, keep.
- `_operating_drivers_support_runtime`: Operating Drivers runtime glue, keep.
- `_write_sheet`: generic writer fallback, keep.
- `_sector_investment_case_render_deps`: render dependency glue, keep.
- `_anf_investment_case_render_deps`: ANF render dependency glue, keep.
- `_build_compat_state`: compatibility publication, keep.

## Workbook Behavior

Workbook behavior includes all generated workbook-observable output, not just
cell values. Future changes must treat the following as behavior:

- sheet presence and absence.
- sheet order.
- values and formulas.
- source maps and source keys.
- source docs, source types, and source snippets.
- source notes and source comments.
- row order.
- comments.
- styles, fills, fonts, and borders.
- merges.
- widths and heights.
- freeze panes.
- hidden rows and columns.
- `QA_Log`.
- `QA_Checks`.
- `Needs_Review`.
- Hidden Value sheets.
- validation output.
- guardrail output.

## Future Extraction Validation Standard

For docs-only or tests-only changes that do not touch production writer code,
run:

- imports plus `py_compile`.
- `PYTHONPATH=. pytest -q tests/test_excel_writer_refactor.py`.
- targeted `tests/test_excel_writer_refactor.py` selection for the changed
  architecture contracts.
- `python stock_models.py data validate-root`.

For any production writer-code change, regenerate fresh parent and branch
macro-free workbooks and compare PBI, GPRE, and ANF after generated metadata
normalization. Required checks include:

- fresh parent/main baseline workbooks generated with
  `--only-write-excel --skip-macro-injection`.
- fresh branch workbooks generated with the same command, data root, and output
  mode.
- workbook modified times verified after each generation start.
- PBI, GPRE, and ANF workbook parity: PASS, 0 diffs after generated metadata
  normalization.
- affected sheets unchanged.
- source maps, source keys, source notes, and source comments unchanged.
- formulas, styles, comments, merges, widths, heights, freeze panes, hidden
  rows, hidden columns, and sheet order unchanged.
- `QA_Log`, `QA_Checks`, `Needs_Review`, `Hidden_Value_Flags`, and
  `Hidden_Value_Audit` unchanged.
- `validate-root` PASS for PBI, GPRE, and ANF.
- formula errors = 0.
- `Needs_Review` P1 = 0.
- guardrail P0/P1 = 0.
- guardrail P2 = 0.
- visible coverage tests PASS.

## Macro-Free And Production Baselines

Workbooks generated with `--only-write-excel --skip-macro-injection` are clean
fresh macro-free validation baselines. They are not macro-enabled production
baselines.

Macro-enabled `.xlsm` production validation remains a separate manual production
gate when Excel COM and macro injection are available.

## Recommended Next-Step Categories

Prefer one of these before starting more small adapter moves:

- docs, tests, and readback fixtures that clarify current behavior.
- deletion or legacy audit for unwired compatibility code, without deleting
  anything until usage is proven clear.
- focused Promise/Quarter audit before moving active v2 UI paths.
- dedicated Valuation architecture plan before touching `_write_valuation_sheet`.
- dedicated latest-quarter QA/cache plan before moving cache/source QA seams.

## Current Recommendation

Pause small adapter extractions for now. The remaining high-payoff work is
architecture clarity, stronger readbacks, and focused audits for the few
cross-cutting areas that still matter. Further modularization should start with
a dedicated plan and parity harness for the affected workbook surfaces.
