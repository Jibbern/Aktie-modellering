# Normalized Company Data Contract

The normalized company data package is the only input a future workbook filler
should need. It separates source extraction from Excel layout. The package must
be produced before render, validated, and either accepted for fill or rejected
with mapping gaps/manual review flags.

Architecture ownership:

- The frozen shell owns layout, static labels, formulas, styles, sheet order,
  and non-writable zones.
- The binding map owns writable fields, shell zones, anchors, row families, and
  value shapes.
- The normalized package owns values, field status, source references, mapping
  gaps, and manual review flags.
- The validator blocks bad content before render so the future filler can write
  values only.
- The canonical issue ledger owns deduplicated issue identity and lossless
  source-level occurrences; workbook QA sheets are presentation projections of
  that ledger, not independent issue stores.

## Source-Native Boundary And Legacy Fixtures

The generic new-ticker path starts from source-native evidence candidates,
normalizes selected evidence into this package, validates it, and only then
plans workbook writes. It must not obtain its values from a legacy workbook.

scripts/build_anf_shadow_normalized_package.py is explicitly a
**legacy-workbook adapter fixture**. It may read the saved ANF workbook and its
support artifacts for migration/shadow comparison, but it is not a generic
onboarding builder and must never become the source-native path for a new
ticker. ANF remains a read-only oracle, not a schema, sector pack, or
implementation dependency.

Before any Excel file is copied or opened, the package must pass:

1. JSON Schema validation against docs/normalized_company_data.schema.json.
2. Semantic/content validation, including source lineage for populated
   source-backed core fields.
3. Typed binding planning against the shell manifest and binding map.

The schema gate is fail-closed and also governs the canonical issue ledger,
shell manifest, binding map, and serialized binding plan. Current contracts
enforce composition, constants/enums, numeric bounds, string/array cardinality,
uniqueness, patterns, formats, required properties, and additional-property
rules before planning or report serialization.

The planner emits exact planned cells, capacity use, overflow, skipped rows,
mapping gaps, and manual-review flags. It never parses sources or opens a
workbook.

## Field Status Contract

Every core field is represented as an object with at least:

- `value`: normalized scalar, text, array, or object.
- `status`: one of `populated`, `missing_source`, `missing_mapping`,
  `not_applicable`, `manual_review_required`, `parser_conflict`.
- `source_ref`: source document, cache key, profile reference, or empty string.
- `core`: boolean indicating whether the field can block promotion/render.
- `reason`: required when a core value is missing, conflicted, or manual-review.

Statuses mean:

- `populated`: source/profile/manual policy permits the value to feed bindings.
- `missing_source`: no acceptable source was found.
- `missing_mapping`: source may exist but no normalized mapping exists yet.
- `not_applicable`: field is intentionally not applicable for this company.
- `manual_review_required`: value may exist but needs human review.
- `parser_conflict`: extracted candidates disagree or look noisy/misclassified.

A populated core field always requires a non-empty `source_ref`, independent of
whether a workbook binding currently consumes it. Numeric units use the schema
taxonomy; arbitrary labels such as parser text are invalid units.

## Required Sections

### ticker_metadata

Identity and reporting metadata:

- `ticker`
- `exchange`
- `cik`
- `fiscal_year_end`
- `reporting_currency`
- `package_version`
- `generated_at_utc`

### company_profile

Source/profile-backed company context:

- `company_name`
- `sector`
- `industry`
- `business_description`
- `strategic_context`
- `revenue_model`
- `revenue_streams[]`: source-backed `member`, numeric `mix`, `unit`,
  `period`, `source_ref`, and deterministic `display_order`
- `key_advantages`
- `key_risks`
- `allowed_sector_terms`

Profile-backed values are allowed only when the binding map marks the target as
profile-backed or manual-review. They must not masquerade as source-backed facts.

### quarterly_financials

Normalized quarterly rows with period labels and units:

- revenue
- gross profit
- operating income
- reported/base EBITDA where source-backed
- adjusted EBITDA/EBIT where source-backed
- net income
- EPS
- operating cash flow
- free cash flow
- diluted shares

Rows use `quarterly_financials.rows[*].<field>` and `YYYY-Qn` period keys.
Executable bindings select rows with `row_selector` and business keys; they do
not use `.rows.0` or `.items.0` shortcuts.

### annual_financials

Annual rows use the same field semantics as quarterly financials and `YYYY-FY`
period keys. `fiscal_year` must agree with the period. Reported/base EBITDA and
adjusted EBITDA are distinct fields and may not be substituted for each other.
Annual CFO and capex remain separate source-backed fields so FCF can be traced
without treating a workbook formula as source evidence.

Every fiscal year represented in quarterly evidence is reconciled against an
explicit Q1-Q4 set. Complete years enter `annual_financials.rows`; years that
cannot be aggregated enter `annual_financials.incomplete_candidates` with the
present quarters, missing quarters, source references, and reason. A historical
year may never disappear merely because Q4 is absent.

### valuation_inputs

Explicit, typed source inputs for the canonical Investment Case valuation model:

- price and price as-of date
- shares outstanding
- net debt or net cash
- revenue TTM
- reported/base EBITDA TTM
- adjusted EBITDA TTM
- net income TTM
- CFO and FCF TTM
- reviewed valuation assumptions where applicable

These values are inputs only. Forward valuation outputs are formula-owned by the
Investment Case canonical matrix. Mapping gaps, QA rows, and review flags can
never supply valuation output cells.

### debt_liquidity

Debt and liquidity fields:

- cash
- total debt
- net debt
- net leverage, only when its numerator, denominator, scope, and period are source-backed
- revolver availability
- cash used in the liquidity total
- other available liquidity
- total liquidity
- liquidity definition/scope
- total-liquidity as-of date
- latest SUMMARY point-in-time as-of date
- `liquidity_freshness` disposition: `current`,
  `stale_but_displayable_with_date`, `blocked_from_current_summary`, or
  `incomplete_components`
- source-backed `summary_liquidity_display`; stale values include their exact
  as-of date in the visible value
- deprecated `liquidity` alias, which must agree with `total_liquidity`
- lease liabilities
- interest expense
- maturity schedule references

Optional typed debt collections provide source-native analytical depth without
changing the scalar SUMMARY/Valuation contract:

- `facilities`: facility identity, commitment, loan cap, drawn state, letters of
  credit, gross capacity, minimum excess availability, net availability,
  same-date cash/liquidity and facility expiry
- `instruments`: funded-debt or lease identity, balances, rate metadata, maturity,
  security/seniority and an explicit aggregation role
- `maturities`: exact funded-debt instrument, due date/bucket and principal amount;
  facility expiry is not a debt maturity
- `credit_notes`: bounded typed source statements for draw status, covenants,
  amendments, refinancing, redemption, restrictions or ratings

Every collection row has one canonical business identity and exact as-of and
publication dates. Currency, normalized unit, source unit/scale, source-table
scope, source status, evidence IDs/references, source-row reference and source
document SHA-256 are mandatory. Amounts fail closed when any companion identity
or lineage is incompatible. Operating leases are explicitly excluded from core
debt, restricted cash is excluded from available liquidity, and unavailable is
never normalized to zero.

Liquidity components must share one as-of date. A newer cash observation may
not be combined with older revolver evidence. If the latest complete total is
older than SUMMARY, it is either visibly dated or excluded from the current
SUMMARY with an actionable review issue.

### capital_returns

Capital Return is a typed, period-aware collection. Every record keeps its metric
and semantic role, fiscal period, duration or instant identity, publication date,
unit, currency, scale, source document and section, evidence reference,
classification, derivation identity and supersession state. Supported periods are
quarter, year-to-date, annual, exact four-quarter TTM, point-in-time and guidance.

Repurchase cash, treasury-stock accounting cost, employee tax-withholding cash,
program shares, total issuer purchases and tax-withholding shares are distinct
identities. Period-end shares and weighted-average shares are also distinct.
Authorization balances are point-in-time snapshots and are never summed.

Exact derivations require compatible period, role, unit, currency and source
identity. These include cash per program share, net share reduction, total capital
return and FCF coverage. Missing dividends remain unavailable rather than becoming
zero. Historical EPS attribution to buybacks is unavailable unless separately
reported; model-derived forecast effects remain owned by the Investment Case.

Guidance is stored separately from actuals with applicable period, publication
date, point/range/approximate state, numeric usability, original wording and
supersession. Guidance never enters actual quarterly, annual or TTM collections.

The legacy scalar aliases `buybacks`, `dividends` and `share_issuance` remain
temporarily available for existing bindings. New Capital Return products consume
the typed `records`, `guidance` and `period_reconciliations` collections.

### normalized_guidance

Guidance rows must keep `publication_date`, source/reporting `source_date` and
`stated_in_period`, guidance `horizon`, `update_stage`, and current/history
visibility as distinct fields. Guidance rows also contain metric, value/range,
source excerpt, status, and classification. Boilerplate,
safe-harbor text, parser snippets, and metric conflicts must stay out of mapped
guidance rows and go to manual review flags.

Guidance fields mapped into `Promise_Progress_UI` must be source-backed and pass
metric classification before render.

Publication date and guidance horizon are separate concepts. `publication_date`
orders updates, while `horizon` identifies the period management is guiding.
`display_role` routes rows deterministically: `current_primary` feeds current
visible guidance blocks and historical rows remain available to history/audit
surfaces. `display_priority` provides a stable order inside each role.

Every guidance row carries a stable, source-derived `evidence_key`. The primary
Promise Progress binding follows the frozen shell headers: metric, previous or
initial guide, current guide, actual, status, horizon, stated-in period, source
date, and notes/source. Revision fields without a distinct shell cell remain in
the normalized package and are not concatenated into another cell.

Table-row guidance surfaces may also carry row-shaped fields for progression
columns:

- `initial_guide`
- `q1_update`
- `q2_update`
- `q3_update`
- `q4_update`
- `actual`
- `progress_status`
- `notes_source`

`progress_status` is intentionally not named `status` in the package row object;
`status` remains reserved for normalized field status inside `{value, status,
source_ref, core}` field objects. The binding map may still expose a visible
column called `status` through `row_schema.column_id`.

### segments

Segment taxonomy and values:

- segment name
- revenue
- operating income/EBIT/EBITDA if source-backed
- margin
- period
- source reference
- whether the segment feeds scenario bridges

Rows require a supported `dimension`, `member`, `metric`, `period`, and
`period_type`. Supported dimensions are generic taxonomy values such as
`reported_segment`, `business_line`, `geography`, `brand`, `product`,
`category`, and `total_company`; ticker labels are members, not schema logic.

### operating_drivers

Driver rows for visible current outlook/watchlist/actuals:

- topic / driver group
- driver key
- label
- current read
- metric/value/unit
- period
- source reference
- driver group
- why it matters / use
- stable source-derived `evidence_key`

Visible watchlist rows use `display_role=current_watchlist` and an explicit
priority. Definition, policy, parser-fragment, and historical evidence remains
in the package or audit trail rather than entering the visible watchlist.

### quarter_notes

Quarter narrative notes:

- period
- theme
- metric
- commentary / what happened
- what happened
- management framing
- why it matters
- model implication
- valuation implication
- amount/unit
- source reference
- confidence
- stable source-derived `evidence_key`

Visible quarter-note rows use `display_role=current_note`. The selector chooses
the latest valid source-backed quarter and one curated note per theme; later,
invalid, duplicate, or audit-only snippets are retained as structured review
evidence rather than silently discarded.

### investment_case

Promotion-sensitive thesis content:

- summary
- key debate
- bull/base/bear framing
- scenario drivers
- valuation hooks
- source-backed evidence references

Placeholders or generic text must block promotion.

Investment-case fields are promotion-sensitive. Stress-test packages may carry
manual review statuses, but a promoted package must not contain placeholders.

### source_coverage

Inventory of available and missing sources:

- SEC filings
- earnings releases
- earnings presentations
- transcripts
- financial schedules
- market data
- profile/manual references

Legacy adapter fixtures must expose any row limits, exact-evidence
deduplication, or unit normalization under `source_coverage` and corresponding
manual-review flags. Such policies are migration evidence only and must not be
copied into a source-native ticker builder.

### mapping_gaps

Binding gaps that prevent workbook cells/sections from being filled:

- sheet
- section
- target
- normalized field
- required/optional
- reason
- suggested action

### manual_review_flags

Validation issues and review flags:

- severity
- rule_id
- field
- message
- source_ref
- suggested_action

## Promotion Rule

A package can be stress-tested with gaps. It cannot be promoted to workbook fill
when P1 content-validation issues remain, required bindings are missing without
reason, or investment-case content is placeholder/generic.
