# Sheet Data Flow Map

This map pairs the standard visible workbook sheets with their current source
inputs, storage layers, transformation owners, normalized fields, workbook
bindings, validation behavior, and future ownership. The machine-readable
version is `docs/sheet_data_flow_map.json`.

## Conventions

- Source-backed: value must come from SEC/XBRL, IR files, transcripts,
  presentations, or another accepted source with a source reference.
- Profile-backed: value can come from manually configured profile data only when
  the binding map allows it.
- Manual: value requires analyst/manual review before promotion.
- Inferred/derived: value is calculated from source-backed fields; the
  calculation must be documented and source refs must point to inputs.
- Missing values: required missing values become mapping gaps or manual review
  flags before render.
- Visible UI: must never show raw cache paths, parser snippets, raw JSON, XBRL
  XML fragments, safe-harbor boilerplate, unsupported sector text, or template
  placeholders.

## Support Sheet Dependencies

| Support sheet or cache | Current role | Feeds | Future role |
| --- | --- | --- | --- |
| `StockModelData/sec_cache/{ticker}` | SEC/companyfacts/document cache | financial facts, audit logs, OCR text | raw source cache only |
| `StockModelData/tickers/{ticker}` | IR files, transcripts, presentations, manual source material | doc-intel, guidance, segments, drivers | raw source cache only |
| `History_Q` | quarterly financial history and derived facts | SUMMARY, Valuation, BS_Segments, Operating_Drivers, Investment Case | audit/output support; normalized package is runtime input |
| `DATA_Facts_Long` | long-form SEC/XBRL facts | QA, source coverage, BS/financial checks | source coverage/audit support |
| `SEC_Audit_Log` | tag/source/filing audit | SUMMARY notes, QA | source refs and audit support |
| `Guidance_Raw` | raw extracted guidance | Promise/Guidance QA | audit support; do not bind directly |
| `Guidance_Normalized` / `Slides_Guidance` | current normalized-ish guidance sheet | Promise_Progress_UI, Valuation, Investment Case | source for normalized package builder, not value-only filler |
| `Quarter_Notes` / `Quarter_Notes_Evidence` | quarter-note candidates and evidence | Quarter_Notes_UI, QA | source for normalized `quarter_notes.items` |
| `Quarter_Narrative_Data` | selected narrative/audit rows | Quarter_Notes_UI readback and narrative support | audit support |
| `Promise_Progress` / `Promise_Evidence` | promise lifecycle and backing rows | Promise_Progress_UI, QA | source for normalized guidance lifecycle |
| `Slides_Segments` | segment values/taxonomy from IR files | BS_Segments, Operating_Drivers, Investment Case | source for normalized `segments.items` |
| `operating_drivers_raw` | driver rows and source snippets | Operating_Drivers, QA | audit support |
| `Debt_Tranches_Q` / `Debt_Profile` | debt schedule and tranche support | Valuation, BS_Segments, SUMMARY | source for normalized `debt_liquidity` |
| `Hidden_Value_Flags` / `Hidden_Value_Base` | valuation formula/readback support | Valuation | shell/audit support, not normalized core |
| `QA_Log`, `Needs_Review`, `QA_Checks` | validation and review outputs | QA visible sheets | output-only from validator and readback checks |

## Visible Sheet Flow Summary

| Visible sheet | Current owner | Key normalized fields | Binding IDs | Missing behavior |
| --- | --- | --- | --- | --- |
| `SUMMARY` | `excel_writer_summary_builder.py`, `excel_writer_summary_sheet.py` | `company_profile`, `quarterly_financials`, `debt_liquidity` | `summary_*` | Blank mapped value plus mapping gap or review flag. |
| `Valuation` | `excel_writer_valuation_orchestrator.py` and valuation render/precompute helpers | `quarterly_financials`, `debt_liquidity`, `capital_returns`, `normalized_guidance` | `valuation_*` | Block promotion for required valuation core gaps. |
| `BS_Segments` | `excel_writer_bs_segments.py`, segment source helpers | `debt_liquidity`, `segments` | `bs_*` | Omit unsupported rows; emit segment/debt mapping gaps. |
| `Operating_Drivers` | `excel_writer_drivers.py`, `operating_drivers_runtime.py`, `excel_writer_operating_drivers.py` | `operating_drivers`, `segments`, `quarter_notes` | `od_*` | No sector/profile fallback row without source; manual review. |
| `{ticker}_Investment_Case` | sector/ANF investment-case writers and support helpers | `investment_case`, `segments`, `normalized_guidance` | `ic_*` | Block promotion for placeholder/manual-review core thesis fields. |
| `Quarter_Notes_UI` | quarter notes UI orchestrator and source/selection/render helpers | `quarter_notes.items` | `qn_*` | Omit bad content and route to review. |
| `Promise_Progress_UI` | promise-progress writer/orchestrator/selection helpers | `normalized_guidance.items` | `pp_*` | Do not render conflicted guidance; emit parser conflict/review. |
| `QA_Log` | `write_qa_sheets()` and writer QA policy | `manual_review_flags` | `qa_log_validation_rows` | Empty/pass row only after validator returns no issues. |
| `Needs_Review` | `write_qa_sheets()` and writer QA policy | `manual_review_flags` | `needs_review_validation_rows` | Required gaps surface here, not in visible UI patches. |
| `QA_Checks` | `write_qa_sheets()`, workbook validation, quality guardrails | `mapping_gaps` | `qa_checks_mapping_gap_rows` | One structured check per binding gap/validation issue. |

## Cross-Sheet Rules For Runtime

- `History_Q` can be used by a future normalizer to build source-backed
  financial fields. The value-only filler should not scrape it at render time.
- `Guidance_Normalized`, `Guidance_Raw`, `Slides_Guidance`,
  `Promise_Progress`, and `Promise_Evidence` should be reconciled into
  `normalized_guidance.items` before render. Metric, horizon, status, and source
  excerpt must pass validation first.
- `Slides_Segments`, `History_Q`, and segment parser outputs should be
  reconciled into `segments.items`. Unsupported sector leakage should be caught
  before any row is visible.
- `operating_drivers_raw`, quarter notes, transcripts, presentations, and
  profile templates should be reconciled into `operating_drivers.items`.
  Templates may define labels and expected driver families, but not source facts.
- Investment-case text should be built from normalized profile, guidance,
  segments, drivers, valuation hooks, and manual review. Promotion must be
  blocked while core text is placeholder/generic.
- QA sheets should be generated from normalized validation issues and binding
  gaps, then augmented by saved-workbook readback validation after render.

## Sheet-Level Presentation Rules

| Visible sheet | Expected visible missing state | Must never be visible |
| --- | --- | --- |
| `SUMMARY` | Blank value or concise review row reference | source paths, generic copied profile text as source fact, unsupported sector terms |
| `Valuation` | Blank value cells; formulas/static labels intact | wrong-unit share counts, parser snippets, raw guidance blobs |
| `BS_Segments` | No segment row until taxonomy/value exists | copied sector rows, unsupported carbon/retail/presort labels |
| `Operating_Drivers` | No driver fact until source-backed | raw source snippets, profile fallback facts, unsupported sector terms |
| `{ticker}_Investment_Case` | Manual review flag or blocked promotion | placeholders, generic thesis, copied PBI/GPRE/ANF cases |
| `Quarter_Notes_UI` | Omitted note row plus review flag | safe-harbor boilerplate, XBRL/XML fragments, raw JSON |
| `Promise_Progress_UI` | No visible guidance row until classified | misclassified guidance, legal boilerplate, hidden source keys |
| `QA_Log` | Structured issue rows or explicit pass | unstructured blanks/nan status |
| `Needs_Review` | Curated review rows | low-value duplicate noise as P1 |
| `QA_Checks` | Structured checks with status/severity | clean status while required binding is missing |

## Ownership Direction

The existing parsers and workbook validators contain useful source-ranking and
quality knowledge. The existing visible writers should not become the new
runtime contract. Future ownership should be:

- Normalized package builder: all source parsing, data selection, source refs,
  normalized statuses, and missing reasons.
- Normalized validator: pre-render content quality and mapping gaps.
- Binding map: writable zones, value shapes, required/optional policy, and
  missing behavior.
- Frozen shell: layout, formulas, labels, styles, merges, dimensions, and sheet
  order.
- Value-only filler: copy approved normalized values into writable zones only.
