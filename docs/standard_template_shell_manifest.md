# Standard Template Shell Manifest

This pass defines and materializes the frozen standard workbook shell at
`templates/standard_stock_model_template.xlsx`. The future filler writes values
only into the writable zones declared here and in
`docs/workbook_binding_map.json`.

## Shell Identity

- File type: `.xlsx`
- Shell artifact: `templates/standard_stock_model_template.xlsx`
- Materialization helper: `scripts/materialize_standard_template_shell.py`
- Shell validator: `scripts/validate_standard_template_shell.py`
- Visible sheet order:
  `SUMMARY`, `Valuation`, `BS_Segments`, `Operating_Drivers`,
  `{ticker}_Investment_Case`, `Quarter_Notes_UI`, `Promise_Progress_UI`,
  `QA_Log`, `Needs_Review`, `QA_Checks`
- Ticker-token rule: the investment case shell sheet is stored as
  `{ticker}_Investment_Case` and resolved at runtime, for example
  `PBI_Investment_Case`.

## Ownership

The shell owns layout: sheet order, static labels, merged cells, formulas,
styles, row heights, column widths, freeze panes, print/view settings, formulas,
and protected/static areas.

The materialized shell is a neutral template. It uses PBI/GPRE/ANF saved
workbooks as the standard layout family for broad dimensions/freeze-pane
conventions, but writable zones are blank and company-specific values are not
retained as template data.

Neutrality also means the shell does not preserve ANF/retail-specific member
rows as fixed labels. Segment, geography, guidance, operating-driver, and
scenario rows are generic block slots. Examples include `[Dimension member 1]`,
`[Operating driver slot N]`, `[Guidance metric slot N]`, and
`[Scenario driver slot N]`.

The current shell has a source-grounded visual parity audit at
`docs/standard_template_shell_visual_gap_audit.json` and
`docs/standard_template_shell_visual_gap_audit.md`. The audit compares the
materialized shell with `templates/lab/ANF_template_lab.xlsx` across every
standard visible sheet. Its previews are openpyxl/static contact sheets, not an
Excel/COM visual certification.

The binding map owns writable fields: which zones can receive values, the anchor
or named range used to find each zone, and the normalized field that supplies the
value.

The normalized package owns values/status/sources. Every core value carries a
status and source/reference policy before the workbook is opened.

The validator blocks bad content before render. A future filler must refuse
promotion when P1 normalized-data issues remain.

The shell validator also blocks non-neutral template content before runtime:
visible company values, source/thesis text, fixed sector labels, fixed dimension
members, valuation numeric constants, and heatmap/data-signal fills in blank
writable zones.

## Writable vs Non-Writable Zones

Writable zones are value-only destinations. They may receive scalars, text
blocks, series, table rows, pivot-matrix values, or validation rows according
to the binding map.
Writable zones and non-writable zones must be disjoint by A1 range on the same
sheet. This lets a future filler reject unsafe writes before opening the
workbook.

Non-writable zones include title rows, section headers, static row labels,
formula-only bands, hidden support formulas, style-only spacer rows, and any
static labels. Formulas/static labels are protected by precise non-writable
zones rather than broad whole-sheet exclusions. Formulas/static labels must
never be overwritten by the filler.

Reusable helper formulas outside writable zones are shell-owned and retained.
ANF formulas that lived inside source-backed writable value zones are treated as
company/source-specific evidence and cleared with the values, so the future
filler does not have to distinguish formula subcells inside broad write targets.

## Block Architecture Link

The richer shell upgrade is now described by
`docs/workbook_block_architecture.json`. Each writable manifest zone should map
to at least one block ID, and each block ID should carry its binding IDs,
normalized fields, source policy, missing-data behavior, validation rules, and
standardization status.

The manifest remains the source of truth for writable and non-writable zone
safety. The block architecture adds the visual/workbook context used to keep the
rich shell aligned with the binding map without making layout decisions inside
the future filler.

For the rich shell:

- ANF is the visual lab base through `templates/lab/ANF_template_lab.xlsx`;
- PBI and GPRE are coverage cross-checks, not dominant sources;
- company-specific visible values, source-specific notes, thesis text, guidance
  commentary, and financial numbers are cleared from writable zones;
- valuation data constants are cleared across input/output/value areas, leaving
  only protected formulas/static labels;
- fixed source members such as geography names, store-footprint rows, tariff
  rows, and guidance metric examples are converted to generic slots;
- blank writable/output slots must not retain heatmap/data-signal fills,
  including gray fills that read as data/output signals;
- standard labels/formulas/styles may be retained only when the block coverage
  marks them as standard;
- GPRE-only sector overlays remain excluded from the standard visible shell.

## Anchors

Each required anchor in the JSON manifest must have at least one required
binding. Anchors are intentionally human-readable labels or future named ranges,
so the runtime can be simple: locate anchor, fill declared zone, do not alter
layout.

The materialized shell includes defined names for required anchors and visible
anchor labels. The validator fails if required anchors are missing, binding
targets fall outside their writable shell zones, or binding targets overlap
non-writable zones.

## QA Areas

`QA_Log`, `Needs_Review`, and `QA_Checks` are writable only in their declared
table-row areas. They receive validator and mapping-gap rows. They do not own
business logic.

## Promise Progress Headers

`Promise_Progress_UI` has repeated annual-guidance and guidance-revision blocks.
The repeated column header rows are non-writable shell-owned structure, while
the rows beneath each header are separate writable zones. This preserves the
full standard workbook column contract on every block without allowing a future
filler to overwrite headers.

## Valuation And Operating Driver Sidecars

The lower Valuation input block declares `D194:D216` as the
`valuation_input_values` shell zone. Each active input has its own exact planner
cell contract and semantic target role; adjacent labels, date/period headers,
formula outputs, and interpretation cells remain protected. The current
guidance sidecar similarly separates guidance-value cells from status cells and
keeps helper/formula columns non-writable.

Segment matrix contracts authorize only exact pivot cells selected by
dimension, member, metric, and period. Operating Drivers and Quarter Notes use
their exact visible columns and start rows; row 9 on `Quarter_Notes_UI` remains
header-only. Declaring a broad visual shell zone never authorizes sequential
row dumping.

The Valuation right-side guidance/driver sidecar also uses repeated protected
header rows. Lower guidance, operating-driver, thesis-bridge, and output headers
are non-writable shell structure; the corresponding value rows beneath them are
separate writable zones. `Operating_Drivers` follows the same pattern for its
topic/current-read and horizon/commentary subheaders.

Valuation's lower reusable blocks are also split around protected headers:
`Debt Detail (latest)`, hidden-value flag headers, `Trend/Δ (last 4Q)`, and
`Red/Green Flags` are shell-owned. Their value/status areas remain blank and
neutral in the frozen shell; the future filler must write only the declared
value zones and must not replace the standard flag-rule labels.

The lower Valuation header bands keep the ANF-led visual contract: Debt Detail
uses `A122:N122`, Operating Signals and Capital Return use `A:M`, Trend uses
`A:D`, and the secondary Valuation title at row 192 is protected. Header text in
these bands is styled as template UI, while all ticker-specific values beneath
the bands remain blank until the normalized package is bound.

`Operating_Drivers` uses its title on row 1 and freezes only row 1 (`A2` freeze
pane). The former row-2 title band is intentionally blank in the frozen shell.

## Support Sheet Lifecycle

The frozen shell now keeps only neutral hidden support shells when the template
contract needs a structural placeholder:

- `Hidden_Value_Flags`
- `Revolver_History`
- `Debt_Tranches_Latest`
- `Debt_Profile`
- `Guidance_Normalized`
- `Quarter_Notes`
- `Promise_Progress`
- `History_Q`

They are hidden, header-only, and validated as package-neutral. Runtime-created
support/audit sheets such as `Guidance_Raw`, `Promise_Evidence`,
`Quarter_Notes_Audit`, `DATA_Facts_Long`, SEC/OCR logs, and sector-pack outputs
are documented in `docs/support_sheet_lifecycle_contract.json` but are not
stored in the frozen shell. The normalized data package is the runtime data
source, not workbook support sheets scraped during fill.

## Hidden Package Contract

The frozen shell is neutral across the whole workbook package. Hidden sheets are
allowed only when they are listed in the hidden support audit with a
classification, dependency reason, and leakage check.

The current retained hidden sheets are listed in
`docs/standard_template_hidden_support_audit.json`; each has a classification,
reason, leakage count, and dependency check. `Hidden_Value_Flags` is kept as a
formula dependency, while the debt/guidance/quarter/history support sheets are
kept as neutral helper shells.

The materializer deletes hidden raw/source/audit/runtime-output sheets inherited
from `templates/lab/ANF_template_lab.xlsx` when they are not required by visible
formulas, defined names, data validations, or shell validation. It also removes
defined names that point to deleted sheets. The validator fails if any retained
hidden sheet is unclassified, contains company/source leakage, or leaves visible
formulas/defined names pointing to missing sheets.

The package contract also clears workbook comments and rejects XML-level source
strings in the `.xlsx` archive. A shell is not neutral if ANF/PBI/GPRE/GTX
source references survive in comments, table parts, stale relationships, or
other workbook package parts.

ANF lab data is never a source layer for future ticker output. Source facts,
guidance, quarter notes, investment-case evidence, and QA rows must flow from
the normalized company data package and future runtime outputs, not from hidden
worksheets preserved in the template shell.

## Runtime Rule

Before any workbook is opened, the JSON-only planner resolves each active
binding through `planner_cell_contracts`. Each contract declares the exact
writable cell family, semantic target role, allowed value type, and owning
binding. `planner_merge_families` additionally declares the only writable merge
anchor for repeated merged rows. A non-anchor target, protected target, role
mismatch, or type mismatch is P1.

Intentional spacer rows are not writable by implication. Bindings use explicit
`target_rows` when a visual table is non-contiguous; for example, Valuation
outputs use rows 64-70 and 72-75 while row 71 remains shell-owned.

Future filler writes values only. It must not create ticker-specific visible UI,
run post-render scaffold/repair, create `.xlsm`, or overwrite static shell
labels/formulas. It must fail if a binding target overlaps a non-writable zone.
