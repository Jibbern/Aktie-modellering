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
- `revenue_model`
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
- adjusted EBITDA/EBIT where source-backed
- net income
- EPS
- operating cash flow
- free cash flow
- diluted shares

Rows should use `quarterly_financials.rows[*].<field>` in the package; binding
metadata may use `.rows.0.<field>` as the representative path for a row family.

### annual_financials

Annual rows using the same field semantics as quarterly financials. Annual rows
must identify fiscal/calendar basis explicitly.

### debt_liquidity

Debt and liquidity fields:

- cash
- total debt
- net debt
- revolver availability
- liquidity
- lease liabilities
- interest expense
- maturity schedule references

### capital_returns

Capital return fields:

- buybacks
- dividends
- share issuance
- debt repayment/refinancing
- capital allocation notes

### normalized_guidance

Guidance rows must be normalized into metric, value/range, horizon, stated-in
period, source date, source excerpt, status, and classification. Boilerplate,
safe-harbor text, parser snippets, and metric conflicts must stay out of mapped
guidance rows and go to manual review flags.

Guidance fields mapped into `Promise_Progress_UI` must be source-backed and pass
metric classification before render.

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
