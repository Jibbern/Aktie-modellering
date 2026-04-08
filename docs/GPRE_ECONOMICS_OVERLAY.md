# GPRE Economics Overlay

## Purpose
This note documents the current `GPRE` economics-overlay workflow so ethanol futures, quarter-open fallback behavior, and crush-proxy expectations are explicit in one place.

Use this doc when changing:
- local Chicago ethanol futures inputs under [`GPRE/Ethanol_futures`](/c:/Users/Jibbe/Aktier/GPRE/Ethanol_futures)
- `Economics_Overlay` source wording or preview behavior
- quarter-open / current-QTD / thesis separation
- `Approximate market crush` or `GPRE crush proxy`

## Scope Boundaries
This doc is about the overlay and its market-input provenance.

It does **not** own:
- observed USDA refresh mechanics in detail
  - see [`MARKET_DATA_USDA.md`](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md)
- broad writer/runtime performance guidance
  - see [`PERFORMANCE_NOTES.md`](/c:/Users/Jibbe/Aktier/Code/docs/PERFORMANCE_NOTES.md)

## Current Source Precedence

### Ethanol
- `Prior quarter`
  - observed historical / quarter-safe path
- `Quarter-open proxy`
  1. strict frozen prior-quarter thesis snapshot
  2. local manual quarter-open snapshot file
  3. unavailable
- `Current QTD`
  - observed-only
  - uses the NWER observed path
  - must not be contaminated by futures or manual quarter-open files
- `Next quarter thesis`
  1. local Chicago ethanol futures EOD CSV
  2. unavailable

### Corn basis
- actual GPRE plant bids when available
- AMS fallback otherwise

### Natural gas / other thesis inputs
- preserve the current thesis path unless a pass explicitly changes it

## Local File Inputs

### Local ethanol futures EOD files
Folder:
- [`GPRE/Ethanol_futures`](/c:/Users/Jibbe/Aktier/GPRE/Ethanol_futures)

Supported filename patterns:
- `manual_cme_ethanol_chicago_eod*.csv`
- `ethanol-chicago-prices-end-of-day-*.csv`

Practical role:
- primary thesis futures source for `Next quarter thesis`

Relevant columns:
- `Contract`
- `Last`
- `Time`

Interpretation:
- `Contract` resolves contract month/year
- `Last` is the thesis futures price in `$ / gal`
- `Time` is the local as-of / trade date

Provider compatibility note:
- the active source id remains `cme_ethanol_platts`
- this is a compatibility label only
- the active workflow is local-file driven, not live CME download driven

### Manual quarter-open snapshot files
Folder:
- [`GPRE/Ethanol_futures`](/c:/Users/Jibbe/Aktier/GPRE/Ethanol_futures)

Supported stable patterns:
- `manual_ethanol_chicago_quarter_open*.csv`
- `manual_ethanol_chicago_snapshot*.csv`

Current parser also tolerates older/ad hoc local files when they follow the same schema.

Required columns:
- `snapshot_date`
- `target_quarter`
- `contract_month`
- `settle_usd_per_gal`
- `source`

Practical role:
- fallback seeding for `Quarter-open proxy` when a real frozen prior-quarter thesis snapshot does not already exist

## Contract And Quarter Mapping
Chicago ethanol futures month mapping follows standard quarter construction:

- `Q1` = Jan / Feb / Mar
- `Q2` = Apr / May / Jun
- `Q3` = Jul / Aug / Sep
- `Q4` = Oct / Nov / Dec

Examples:
- Apr 2026, May 2026, Jun 2026 -> `2026-Q2`
- Jul 2026, Aug 2026, Sep 2026 -> `2026-Q3`
- Oct 2026, Nov 2026, Dec 2026 -> `2026-Q4`
- Jan 2027, Feb 2027, Mar 2027 -> `2027-Q1`

Supported contract-string forms include:
- `FLJ26 (Apr '26)`
- `FLK26 (May '26)`
- symbol-month code parsing from the contract root

## Quarterly Strip Logic
Ethanol thesis and manual quarter-open snapshot rows both use the same quarter-strip construction philosophy:

- use the target quarter's three contract months
- prefer day-weighted averaging by calendar days in those months
- allow simple average only as a technical fallback when all three months are present but day-weighting cannot be completed
- if one or more required months are missing, fail explicitly
- do not fabricate partial strips

Expected metadata:
- as-of / snapshot date
- target quarter
- contract months used
- readable contract labels
- per-contract prices
- strip method used
- missing months
- source file(s)
- provenance label

## Quarter-Open Provenance
`Quarter-open proxy` provenance must remain explicit.

Allowed provenance states:
- `frozen_snapshot`
- `manual_local_snapshot`
- `unavailable`

Rules:
- a real frozen prior-quarter thesis snapshot always wins
- the manual local snapshot is only a fallback
- the manual local snapshot must not masquerade as a true frozen historical snapshot

Visible wording should stay short. Examples:
- `Quarter-open proxy uses local manual snapshot.`
- `Quarter-open proxy unavailable.`

Longer provenance belongs in metadata / notes / snapshot payloads, not the short visible status line.

## Overlay Rows And Their Meaning

### Official row
- `Approximate market crush`
- this is the simple official row
- keep it unchanged unless a pass explicitly changes official economics logic

### Fitted row
- `GPRE crush proxy`
- this is the separate fitted/model-selected row
- it must not silently collapse back to the official row

### Naming note
- the visible workbook row is now consistently `Approximate market crush`
- some internal technical keys still use legacy names such as `process_margin`; those are wiring details, not user-facing labels

## Isolation Rules
These should hold unless a pass explicitly changes them:

- `Current QTD` remains observed-only
- local futures files must not leak into current observed ethanol logic
- manual quarter-open backfill must not affect `Next quarter thesis`
- quarter-open fallback must not overwrite a real frozen snapshot
- official row and fitted row must remain separate

## Visible Workbook Wording
Keep visible wording concise.

Current ethanol examples:
- available thesis:
  - `Thesis uses local Chicago ethanol futures strip.`
- unavailable thesis:
  - `Thesis ethanol unavailable.`

Quarter-open examples:
- `Quarter-open proxy uses local manual snapshot.`
- `Quarter-open proxy unavailable.`

Do not dump long provenance strings into the short visible source line.

## Key Code Ownership
Main implementation surfaces:
- [`pbi_xbrl/market_data/providers/cme_ethanol_platts.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/providers/cme_ethanol_platts.py)
  - local futures CSV parsing
  - manual quarter-open snapshot parsing
- [`pbi_xbrl/market_data/service.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/service.py)
  - quarterly strip construction
  - thesis snapshot and quarter-open precedence logic
  - fitted-vs-official proxy preview preparation
- [`pbi_xbrl/excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py)
  - visible overlay wording
  - chart/output integration
  - workbook-facing provenance display

## Practical Rebuild / Verification
Typical local commands:

- rebuild workbook
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE`
- explicit market sync/reparse
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --market-sync --market-reparse --market-only`
- market refresh
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --refresh-market-data`

What to verify after a change:
- `Next quarter thesis` ethanol fills from the local futures CSV when the needed three months exist
- `Quarter-open proxy` uses frozen history first, then manual local fallback
- `Current QTD` remains observed-only
- `Approximate market crush` remains the official row
- `GPRE crush proxy` remains the fitted row
- visible source wording stays concise

## Acceptance Notes
Saved-workbook acceptance should check:
- quarter-open provenance is truthful
- thesis ethanol provenance is truthful
- official vs fitted rows stay separate
- a blank value is blank for a real reason
  - missing months
  - missing frozen history
  - missing observed overlap
  - not because of a silent fallback to the wrong source

For broader workbook acceptance rules, see
- [`WORKBOOK_ACCEPTANCE.md`](/c:/Users/Jibbe/Aktier/Code/docs/WORKBOOK_ACCEPTANCE.md)
