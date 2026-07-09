# Workbook Template Shell Strategy

## Recommendation

Use a frozen standard `.xlsx` workbook shell for visible UI. Future new-ticker
runtime should copy/open that shell and write mapped values only.

The shell file now lives at:

- `templates/standard_stock_model_template.xlsx`

The concrete shell manifest is
`docs/standard_template_shell_manifest.json`, and the shell can be validated
with `scripts/validate_standard_template_shell.py`.

The repeatable materialization helper is
`scripts/materialize_standard_template_shell.py`. It is an authoring aid only
and must not be called by the future new-ticker runtime.

## Why Frozen Shell Beats Runtime Scaffold

The GTX dry-run showed that a workbook can become layout-clean only after
post-render scaffolding while the content remains sparse, generic, or wrong.
That approach hides the real failure: the normalized data package was not ready
to fill the workbook.

A frozen shell makes ownership clear:

- The shell owns visible UI layout, styles, formulas, merges, row/column
  dimensions, sheet order, freeze panes, print/view settings, and static labels.
- The normalized package owns values, status, sources, mapping gaps, and manual
  review flags.
- The binding map owns where values may be written.
- The validator owns whether content is good enough to render or promote.

Put another way: shell owns layout, binding map owns writable zones/fields,
normalized package owns values/status/sources, and validator blocks bad content
before render.

## Runtime Rules For The Future Filler

- The runtime may write values only to writable zones declared in
  `docs/standard_template_shell_manifest.json` and bound in
  `docs/workbook_binding_map.json`.
- It must confirm every required shell anchor has a required binding.
- It must run normalized-data validation before opening/filling the workbook.
- It must report missing required bindings as mapping gaps.
- It must refuse promotion when P1 normalized-data issues remain.
- It must not add ticker-specific visible UI layout.
- It must not run post-render scaffold/repair as the standard path.
- It must not create `.xlsm`.

## Role Of PBI/GPRE/ANF

PBI, GPRE, and ANF remain the visual/content reference family. They can be used
to design, materialize, or audit the frozen shell, but this architecture pass
must not modify their workbook values or generation behavior.

The materialized shell uses PBI/GPRE/ANF saved workbooks only as the standard
template family for broad dimensions, freeze panes, and layout conventions.
Writable zones are blank, and visible company-specific values from those
workbooks are not carried into the shell as fill data.

The same neutrality rule applies to the workbook package, not only visible
cells. The ANF lab workbook is a visual authoring source only. Hidden/source
support data from ANF/PBI/GPRE must not be inherited into the frozen shell or
used as a runtime data source.

## Rich Shell Materialization

The materialized shell is now a rich visual shell derived from
`templates/lab/ANF_template_lab.xlsx` as the primary visual lab source, with
PBI/GPRE used as cross-check workbooks through the block coverage matrix.

The rich shell follows the block architecture in
`docs/workbook_block_architecture.json` and the coverage matrix in
`docs/workbook_block_coverage_matrix.json`:

- preserve standard ANF/PBI/GPRE blocks that appear across the standard visible
  sheet family;
- keep static labels, formulas, styles, hidden columns, merges, row heights,
  column widths, freeze panes, and QA table headers;
- clear company-specific values, source-specific notes, thesis text, guidance
  commentary, and financial numbers from writable zones before the shell is used
  as a template;
- convert fixed sector/company member rows into generic slots such as
  `[Dimension member 1]`, `[Operating driver slot N]`, `[Guidance metric slot N]`,
  and `[Scenario driver slot N]`;
- clear valuation input/output constants across TTM metrics, multiples, shares,
  debt, EBITDA, revenue, EPS/per-share earnings, ratios, and as-of values;
- remove heatmap/data-signal fills, including gray data/output fills, from blank
  writable/output areas so styling does not imply a source-backed signal before
  normalized data is written;
- exclude GPRE-specific overlays and sector logic unless a future optional
  support contract explicitly adds them;
- require every writable region to have both a block ID and binding IDs before a
  runtime may write values.

Neutrality is audited by:

- `docs/standard_template_shell_neutrality_audit.json`
- `docs/standard_template_shell_neutrality_audit.md`

Visual parity is audited by
`docs/standard_template_shell_visual_gap_audit.json` and
`docs/standard_template_shell_visual_gap_audit.md`. The generated contact sheets
under `templates/lab/previews/` are openpyxl/static previews only, not Excel/COM
rendered visual PASS artifacts. They compare the frozen shell against the ANF
lab workbook and report used range, labels, row labels, formulas, merges,
row/column dimensions, hidden columns, freeze panes, blank writable cells, and
remaining gap classifications.

The formula rule is zone-based: reusable helper/formula cells outside writable
zones are preserved as shell-owned structure. ANF formulas embedded inside
source-backed writable value zones are cleared with the company-specific values
because the future filler must own those values through the normalized package
and binding map.

`templates/lab/ANF_template_lab.xlsx` remains a lab artifact, not canonical
output. `templates/standard_stock_model_template.xlsx` is the canonical frozen
shell artifact for future filler work.

`Promise_Progress_UI` keeps the repeated annual-guidance and guidance-revision
column structures from the real workbook family. Each repeated `Metric` header
row is non-writable shell structure; each value block below it has its own
binding range.

The Valuation sidecar and Operating_Drivers sheet use the same contract:
repeated guidance, operating-driver, thesis-bridge, output, topic, and horizon
headers are retained as non-writable shell structure, while adjacent/below value
areas are separate binding ranges.

## Hidden Support Package Neutrality

The frozen shell may retain hidden worksheets only when they are explicitly
classified, dependency-checked, and neutralized. Package-level neutrality is
audited by:

- `docs/standard_template_hidden_support_audit.json`
- `docs/standard_template_hidden_support_audit.md`

The current shell retains a small set of neutral hidden support shells with
headers only:

- `Hidden_Value_Flags`
- `Revolver_History`
- `Debt_Tranches_Latest`
- `Debt_Profile`
- `Guidance_Normalized`
- `Quarter_Notes`
- `Promise_Progress`
- `History_Q`

These sheets are structural placeholders for formula/support workflows and
future runtime projections. They contain no source rows, raw filing text,
guidance values, quarter-note content, audit evidence, or company-specific
business text.

Workbook comments, stale table definitions, raw source filenames, and XML-level
company/source strings are also treated as package leakage. The shell validator
scans the `.xlsx` archive so source comments from the ANF lab workbook cannot
survive outside visible cell checks.

The lifecycle for every common support/audit sheet found in PBI/GPRE/ANF is
documented in:

- `docs/standard_template_sheet_inventory.json`
- `docs/support_sheet_lifecycle_contract.json`

All other ANF lab support/source sheets are deleted from the frozen shell unless
the lifecycle contract marks them as required neutral support shells. Runtime-
generated sheets such as raw guidance, promise evidence, quarter-note audits,
long fact tables, SEC logs, and other source/audit projections must be created
from the normalized company data package in the future runtime. They must not be
copied forward from ANF/PBI/GPRE workbooks.

## ANF Shadow Package Before Shadow Render

The first real-ticker step before an ANF shadow workbook is a normalized-data
shadow package, not another workbook render. The read-only builder
`scripts/build_anf_shadow_normalized_package.py` reads saved ANF source/workbook
artifacts and writes:

- `StockModelData/outputs/stress_tests/ANF_new_ticker_engine/ANF_normalized_data_package.json`
- `StockModelData/outputs/stress_tests/ANF_new_ticker_engine/ANF_mapping_gaps_report.json`
- `StockModelData/outputs/stress_tests/ANF_new_ticker_engine/ANF_content_validation_report.json`
- `docs/anf_normalized_package_source_audit.json`
- `docs/anf_binding_coverage_audit.json`

This keeps ANF source extraction and binding coverage visible before the value-
only filler is asked to write a real ANF shadow workbook. The builder does not
modify production writers, does not replace the legacy ANF workbook, and does
not use shell/layout patching to make missing data look populated.

## Optional Sector Packs

The standard shell contains only generic block slots. Sector/member rows from
the source workbooks are not standard content. They are optional packs that a
future runtime may add only when the ticker profile explicitly selects them and
the normalized data package carries source-backed values.

Current optional pack examples:

- Retail operating pack: stores, closures, openings, remodels, tariffs,
  freight, marketing.
- Commodity/ethanol pack: crush margin, 45Z, RINs, corn, natural gas, oil.
- Shipping/mail pack: Presort, SendTech, USPS, GEC.
- Auto supplier pack: production, turbo penetration, BEV, OEM mix.

Optional sector packs are not included in
`templates/standard_stock_model_template.xlsx`. Missing sector-specific data
must become mapping gaps/manual review flags until a sector pack is explicitly
selected later.

## Role Of Deterministic Scaffold

A deterministic scaffold generated from PBI/GPRE/ANF snapshots can be useful as
a one-time shell authoring or audit aid. It must not be runtime behavior for new
tickers. Runtime scaffold copying, merge top-ups, SUMMARY spillover trimming,
and saved-file repair would recreate the GTX failure mode.

The materialization helper is therefore an authoring tool, not a runtime path.
Future new-ticker runtime must load the frozen `.xlsx` shell and write values
only through the binding map. It must not rebuild the visible UI from PBI/GPRE/
ANF or repair a generated workbook after render.

## Acceptance Criteria For A Future Shell

- File format is `.xlsx`.
- Visible sheets match the standard sheet family.
- Static layout is stable without source data.
- Shell artifact exists at `templates/standard_stock_model_template.xlsx`.
- Shell validation passes with `scripts/validate_standard_template_shell.py`.
- Value cells are clearly bindable from the JSON map.
- Writable zones and non-writable zones are explicit.
- Writable zones and non-writable zones are disjoint by A1 range.
- Formula/static-label protection is expressed through precise non-writable
  zones, not broad whole-sheet blocks that overlap value targets.
- Validation/QA sheets can receive normalized issues and mapping gaps.
- No ticker-specific visible UI code is needed to create a new workbook.

## Static And Writable Ownership

The frozen shell owns:

- sheet order and names, including `{ticker}_Investment_Case`;
- static labels and required anchor labels;
- title/header styling, merges, row heights, column widths, and freeze panes;
- formulas in non-writable support/formula bands;
- QA table headers.

The binding map owns:

- writable targets;
- value shape;
- required/optional policy;
- missing-source behavior;
- promotion and validation rules.

The future filler must fail if it attempts to write outside writable zones, into
non-writable zones, over formulas/static labels, or into a sheet not represented
by the manifest.
