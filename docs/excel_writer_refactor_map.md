# Excel Writer Refactor Map

This note tracks the behavior-preserving module extraction work around
`pbi_xbrl/excel_writer_context.py`.  It is a map for future cleanup and should
not be treated as a request to change workbook output.

## Extracted modules

- `excel_writer_quarter_narrative.py`: quarter narrative records, narrative data
  sheet, and Quarter_Notes_UI narrative renderer.
- `excel_writer_coloring.py`: quarterly comparison color policy and fill
  helpers.
- `excel_writer_segment_sources.py`: shared ANF/PBI segment source helpers used
  by BS_Segments and Operating_Drivers.
- `excel_writer_bs_segments.py`: BS_Segments writer, with a context wrapper that
  builds `BsSegmentsWriterDeps`.
- `excel_writer_operating_drivers.py`: visible Operating_Drivers writer, with a
  context wrapper that builds `OperatingDriversWriterDeps`.
- `excel_writer_market_data_sources.py`: market/economics source row helpers.
- `excel_writer_economics_raw.py`: `economics_market_raw` audit sheet writer.
- `excel_writer_valuation.py`: valuation source-map and history helpers.
- `excel_writer_hidden_value_surface.py`: Hidden Value visible surface
  model-building helpers.
- `excel_writer_hidden_value_flags.py`: Hidden_Value_Flags sheet writer.

## Still in `excel_writer_context.py`

- `build_writer_context` and callback registration.
- Quarter_Notes_UI writer and ranking/rendering pipeline.
- Promise_Progress_UI writer, Promise tracker, and last-mile Promise repairs.
- Valuation writer, Valuation formulas, side panels, Hidden Value panel renderer,
  row anchors, merges, styles, hidden AI column writes, and defined names.
- Valuation render/precompute bundles and capital-return source/cache logic.
- Economics_Overlay writer, Basis_Proxy_Sandbox writer, chart helpers, and GPRE
  45Z/RIN/crush parsing logic.
- Source/cache loader wiring, runtime caches, and context-local path helpers.
- Shared UI polish/layout conventions that mutate existing workbook sheets.
- Investment Case visible/data writers.

## Compatibility aliases and wrappers

Keep these until imports and external callback usage are proven clear:

- Module-level re-exports from extracted helper modules, especially quarter
  narrative, coloring, segment source, valuation, and Hidden Value surface names.
- Context wrappers for extracted writers:
  - `_write_bs_segments_sheet`
  - `_write_operating_drivers_sheet`
  - `_build_economics_market_rows`
  - `_write_economics_market_raw_sheet`
  - quarter narrative callback wrappers
- `ctx.extra_helpers` entries for:
  - `_ensure_valuation_render_bundle`
  - `_ensure_valuation_precompute_bundle`
  - quarter narrative callback helpers

## Recommended next steps

1. Continue migrating tests away from `excel_writer_context.py` private helper
   imports when a helper already has a stable extracted-module home.
2. Leave compatibility aliases in production until tests, production imports,
   callbacks, and `extra_helpers` prove they are unused.
3. Prefer small style/layout helper cleanup next, such as valuation or analysis
   style bundle extraction.
4. Defer major writer extractions for Hidden Value renderer, full Valuation,
   Promise_Progress_UI, Economics_Overlay, and Basis_Proxy_Sandbox until their
   source/cache dependencies are flatter and parity readbacks are scripted.

## Required parity posture

Any future extraction should keep:

- workbook values, formulas, fills, styles, hidden keys, row order, and sheet
  order unchanged;
- PBI/GPRE/ANF `data validate-root` green;
- guardrail P0/P1 = 0 and P2 = 0;
- targeted sheet XML/readback parity for the sheet being touched.
