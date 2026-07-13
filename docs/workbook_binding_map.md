# Workbook Binding Map

The binding map defines how a future workbook filler may write normalized data
into the frozen workbook shell. It is not a renderer and it is not a source
parser. The filler may write mapped values only. Missing data must become
mapping gaps or manual review flags.

The frozen shell artifact is
`templates/standard_stock_model_template.xlsx`. The binding map is validated
against that artifact by `scripts/validate_standard_template_shell.py`.

## Required Binding Metadata

Every entry in `docs/workbook_binding_map.json` includes:

- `binding_id`
- `sheet`
- `section`
- `target`
- `shell_zone`
- `anchor_label` or `named_range`
- `row_family`
- `normalized_field`
- `value_shape`
- `required`
- `source_policy`
- `missing_source_behavior`
- `promotion_requirement`
- `validation_rule`
- `writable`

Executable collection bindings also carry a typed planner contract:

- `planning_mode`
- `planner_target`
- `row_selector`
- `row_key`
- `sort_order`
- `capacity`
- `overflow_behavior`
- `required_columns`
- `target_columns`
- `source_ref_required`
- optional `target_rows` when the shell contains intentional spacer rows
- explicit `pick_exclusion_disposition` whenever `pick=first/latest`

`target` remains the declared shell envelope used for layout/block ownership.
`planner_target` is the smaller exact range a planner may emit cells for. A
row contract must map every required visible field to its own target column.
Fields used only to identify/order a companion sidecar row must be declared in
`row_key_only_columns`; otherwise the plan fails. No table may rely on
sequential range dumping.

Every active target also resolves through the manifest's
`planner_cell_contracts` and, where applicable, `planner_merge_families`.
Those contracts own the exact writable cell, semantic target role, allowed
target type, binding owner, and merge anchor. A target inside a merge but not at
its anchor is a P1 contract failure. `target_rows` is used for non-contiguous
families such as Valuation outputs, where row 71 is a shell-owned spacer.

`pbi_xbrl/new_ticker_binding_planner.py` is JSON-only: it validates the
normalized JSON Schema before semantic validation, then emits an auditable
planned-write report without opening an Excel workbook. It reports row keys,
source references, capacity, overflow, skipped rows, mapping gaps, and manual
review flags. The value-only executor may apply only those exact planned cells.
Missing values for `required` or `blocked_if_missing` bindings are P1 and fail
the plan; they are never demoted to ordinary P2 coverage gaps.

The executable identity signs the complete binding-map document, including its
schema version, `binding_planner_contract_version`, top-level policy, and every
binding. Serialized plans and typed snapshots are audit/cache output only.
Coverage, filling, and post-fill validation independently reproduce the plan
from the normalized package, exact approved shell, manifest, and binding
contract; arbitrary `PASS` mappings or recomputed hashes have no authority.

`shell_zone` must match a writable zone in
`docs/standard_template_shell_manifest.json`. The binding map owns writable
fields, not layout. The shell owns the layout and static labels.

Value shapes:

- `scalar`
- `quarterly_series`
- `annual_series`
- `table_rows`
- `pivot_matrix`
- `text_block`
- `validation_rows`

For `table_rows` and `validation_rows`, broad ranges are not enough. A binding
may define `row_source` and `row_schema` so the future filler can write complete
rows without guessing column semantics. A row schema column includes:

- `column_id`: stable semantic column name, for example `metric` or
  `notes_source`.
- `source_field`: field within the normalized row object.
- `target_column`: sheet column where the value may be written.
- `missing_behavior`: what to do when that row value is absent.

The ANF migration fixture exercises row schemas for the main table surfaces:
`Promise_Progress_UI` guidance rows, `Quarter_Notes_UI` quarter-note rows,
`Operating_Drivers` watchlist rows, and QA/manual-review rows. This does not
authorize workbook rendering by itself; it only makes the next value-only filler
pass unambiguous.

`pivot_matrix` is used where business keys determine both row and column. The
standard segment bindings pivot `dimension + member + metric` onto a row and
`period` onto an exact period column. Geography, brand, reported segment, and
total-company rows remain distinct block types. Source order never determines
their Excel row.

## Deterministic Shared Rowsets

Bindings that present the same economic rowset declare a shared `rowset_id` and
the same selector/sort contract:

- current guidance selects `display_role=current_primary`, ordered by explicit
  priority and publication date; historical guidance stays audit/history-only;
- Promise Progress consumes the same current-primary guidance business keys as
  the Valuation guidance sidecar, with separate exact cells for guidance and
  status;
- Operating Drivers selects `display_role=current_watchlist`, one latest clean
  row per investor-relevant theme, with a visible capacity of four;
- Quarter Notes selects `display_role=current_note`, the latest valid quarter,
  and one clean row per theme.

Every selector exclusion, capacity overflow, missing selected value, or
inactive legacy contract produces a structured planner record. No `zip`,
`break`, slice, source order, or implicit first-item policy may silently remove
a selected business row.

## Corrected Exact Targets

The planner contracts encode business meaning, not merely writable space:

- `SUMMARY!A3` consumes `business_description`, `A5` consumes the distinct
  `strategic_context`, and `A9:B11` consume structured revenue-stream members
  and numeric mix values rather than narrative revenue-model text;
- `SUMMARY!B41` accepts only a source-backed `net_leverage` ratio and remains
  blank with an explicit review gap when unavailable; `B45` receives the
  source-reconciled `summary_liquidity_display`, including an exact as-of date
  whenever total liquidity is older than SUMMARY, never net debt or cash-only
  liquidity;
- Quarter Notes data starts on row 10, leaving row 9 header-only;
- Operating Drivers maps topic to column A, current read to B, source to its
  declared source column, and why/use to H;
- segment values use member-by-period pivot cells rather than sequential rows;
- Valuation quarterly actuals, BS quarterly segments, and BS annual segments
  resolve their columns from explicit shared `period_axis_id` contracts. Header
  bindings own the period-to-column map; dependent values cannot derive a
  separate alignment and fail if a selected period has no visible column;
- Valuation input cells `D195:D211` receive typed price/date, shares, net debt,
  TTM financials, and explicit assumptions according to their individual
  bindings;
- Valuation output rows remain formula-owned or require explicit typed
  `valuation_outputs`; QA and `mapping_gaps` are restricted to QA sheets.

Bindings marked `inactive_legacy_contract` document historical broad regions
but are outside the executable planner surface and are not counted as coverage.
They must be replaced by exact typed contracts before activation.

Source policy values:

- `source-backed`: must come from filings/releases/presentations/transcripts or
  normalized financial schedules.
- `profile-backed`: may come from reviewed company profile data.
- `manual`: manual input/review is allowed.
- `derived`: computed from normalized source-backed fields.
- `validation-output`: generated by normalized-data validation.

## Visible Sheets

The standard visible sheets covered by the map are:

- `SUMMARY`
- `Valuation`
- `BS_Segments`
- `Operating_Drivers`
- `{ticker}_Investment_Case`
- `Quarter_Notes_UI`
- `Promise_Progress_UI`
- `QA_Log`
- `Needs_Review`
- `QA_Checks`

## Missing Data Behavior

Required source-backed fields that are not populated must not create visible
placeholder text. The binding should record a mapping gap and, when promotion is
requested, block fill/promotion. Optional fields may stay blank only when the
normalized field status explains why.

Post-render layout patching is explicitly rejected as a runtime strategy. The
binding map can identify missing values, but it cannot insert rows, change
styles, top up merges, clear spillover, or repair a workbook after render.

## Writable Zones

Writable zones are declared by the shell manifest and referenced by each binding
through `shell_zone`. A future filler should validate that every writable
binding points to a manifest writable zone before opening the workbook. A
binding must never target a manifest non-writable zone, static label, formula
band, title area, or style-only spacer.

Binding targets must also be contained inside their declared writable shell
zone. If a binding target overlaps any non-writable zone on the same sheet, the
future filler must fail before writing values.

In the materialized shell, writable zones are blank/neutral. Static labels,
defined anchors, QA headers, formulas, merges, row heights, column widths, and
freeze panes are shell-owned and must not be overwritten by the filler.

For `Promise_Progress_UI`, the guidance/revision column headers are shell-owned
non-writable rows. Writable bindings target only the blank rows beneath those
headers. Annual guidance progression and guidance timeline bindings are split by
repeated block, so every annual/quarterly `Metric` block keeps the standard
`Initial guide`, `Q1 update`, `Q2 update`, `Q3 update`, `Q4 update`, `Actual`,
`Status`, and `Notes/source` or revision-column structure without giving the
future filler permission to overwrite it.

The same rule applies to the Valuation right-side guidance/driver sidecar and
Operating Drivers support tables. Repeated Valuation headers such as lower
`Metric / Stated in / Applies to / Guidance`, `Operating Drivers`,
`Thesis Bridge`, and `Output / Value / Interpretation` are shell-owned
non-writable rows. Operating_Drivers subheaders such as `Topic / Current read /
Source / use` and `Horizon / Stated in / Commentary` are also shell-owned. The
binding map splits writable targets around those rows.

Valuation's lower `Debt Detail`, hidden-value flag, trend, and red/green flag
blocks follow the same contract. Standard row/header labels and the canonical
red/green flag-rule names are template-owned; debt, cash, capital-return,
trend, status, evidence, and as-of outputs are blank value/status zones until
the normalized package and validator produce source-backed content.

Binding values must come from the normalized company data package, never from
hidden worksheets inherited from a template source workbook. The frozen shell's
hidden support package is validated separately by
`docs/standard_template_hidden_support_audit.json`, and sheet ownership is
documented in `docs/support_sheet_lifecycle_contract.json`; retained hidden
helpers are formula/structure scaffolds only, not a content cache for the
filler.

Generic labels such as `[Dimension member 1]`, `[Operating driver slot N]`,
`[Guidance metric slot N]`, and `[Scenario driver slot N]` are placeholders for
binding destinations, not example data. A future filler must replace values only
inside declared writable targets and must not interpret those generic labels as
source-backed normalized values.

The binding map was tightened after the visual gap audit so early Valuation
period/value rows, SUMMARY narrative/value rows, Quarter Notes summary rows,
Promise Progress scorecard rows, and lower Investment Case optional blocks are
explicit writable targets rather than accidental static labels. This prevents a
future runtime from copying ANF/PBI/GPRE source values while preserving the rich
visual shell.

For formulas, the binding contract is strict: helper formulas outside writable
zones are shell-owned. Any ANF formula inside a writable value zone is treated as
source-derived content and cleared from the frozen shell; future formula support
must be added as a non-writable formula/helper block before runtime can depend on
it.

## Block IDs

The workbook block architecture in `docs/workbook_block_architecture.json`
resolves writable bindings to block IDs. The binding map still owns the target
cell/range and normalized field, while the block map owns the larger workbook
section context used to keep the frozen shell as a rich visual template.

Future binding maintenance must keep these relationships true:

- every writable binding resolves to exactly one standard block;
- block ranges are broad enough to contain their binding targets but do not
  authorize writes outside manifest writable zones;
- required blocks carry normalized fields, missing-data behavior, and validation
  rules;
- company-specific source values in ANF/PBI/GPRE are evidence for clearing or
  review, not standard template labels;
- sector-specific blocks such as GPRE-only economics overlays are excluded or
  explicitly optional, never default standard-template behavior.

## Runtime Use

The future runtime should:

1. Validate the normalized JSON Schema and semantic package rules.
2. Build a JSON-only binding plan from exact `planner_target` cells.
3. Reject P0/P1 package, row-contract, target-collision, source-lineage, or
   capacity failures before a shell is copied or opened.
4. Confirm required shell anchors have bindings.
5. Apply only the exact planned cells inside declared writable zones.
6. Write planner-owned QA/Needs Review rows only to QA sheets.
7. Refuse promotion when P1 content validation issues remain.

Post-render scaffold/repair is not allowed as runtime. If normalized data does
not satisfy a binding, the runtime reports the gap; it does not insert rows,
copy a ticker workbook, trim ranges, top up merges, or patch visible content
after the workbook is filled.

## Canonical QA Presentation

The binding planner builds `issue_ledger` before any QA cells are planned. Full
source-level occurrences remain in `issue_ledger.occurrences`; workbook QA is a
bounded presentation only.

- `QA_Log` binds one deduplicated summary per stable `issue_id`.
- `Needs_Review` binds only issues whose visibility disposition is
  `needs_review`.
- `QA_Checks` binds one aggregate row per issue `rule_id` plus explicit
  `PASS`/`INFO` rows for executed or prerequisite-blocked validation stages.

Each QA binding declares its row schema, stable row key, sorting, capacity,
aggregation policy, visibility filter, and explicit overflow behavior. Capacity
may limit workbook summaries, but it may never remove JSON occurrences.

Only the primary Quarter Notes rows currently have approved bindings. The
retained history layout below row 15 is shell-owned and blank; it is not a
license for sequential dumping. A future history binding must define exact row
keys, capacity, columns, and overflow behavior before that area becomes writable.
