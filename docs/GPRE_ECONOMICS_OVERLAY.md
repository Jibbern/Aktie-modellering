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

## Stage C.2: Historical Corn-Bid Retention

### Product rule
- dated GPRE corn-bid snapshots are no longer only a current / forward convenience
- if a usable dated GPRE snapshot exists for the relevant quarter or frame, the official corn-basis path should keep using it even after that quarter rolls from:
  - `next quarter thesis`
  - to `current`
  - to `prior quarter`
  - to historical quarterly rows
- AMS remains fallback only when the relevant retained GPRE snapshot is unavailable or unusable

### Snapshot selection policy
- `Current QTD`
  - latest usable GPRE snapshot on or before `as_of_date`
- `Quarter-open proxy`
  - latest usable GPRE snapshot on or before quarter start, while preserving the established freeze/open policy where that policy already exists
- `Next quarter thesis`
  - latest usable GPRE snapshot on or before `as_of_date` that still contains forward-delivery rows for the target quarter
- `Prior quarter`
  - latest usable retained GPRE snapshot on or before target quarter end
- `Historical quarter`
  - latest usable retained GPRE snapshot on or before target quarter end
  - preference goes to snapshots captured inside the target quarter when such snapshots exist

### Snapshot archive layout
- retained GPRE corn-bid history is now archived under:
  - [`GPRE/corn_bids/raw_snapshots`](/c:/Users/Jibbe/Aktier/GPRE/corn_bids/raw_snapshots)
  - [`GPRE/corn_bids/parsed_snapshots`](/c:/Users/Jibbe/Aktier/GPRE/corn_bids/parsed_snapshots)
  - [`GPRE/corn_bids/manifest.json`](/c:/Users/Jibbe/Aktier/GPRE/corn_bids/manifest.json)
- canonical selection input is the parsed snapshot CSV
- raw HTML is retained for audit / reparsing
- legacy mutable latest files may still exist, but they are no longer the intended historical source of truth

### Visible wording
- the workbook note for the official market row should now say, in substance:
  - `Official corn basis prefers dated GPRE plant bids when available for the relevant frame or quarter, including historical quarters with retained snapshots; otherwise it falls back to active-capacity-weighted AMS basis ...`
- the `Basis_Proxy_Sandbox` build-up block now also shows:
  - `Official corn basis snapshot date`
  - `Official corn basis selection rule`
  - both are frame-specific auditability rows, not separate policy logic

### Visual readability polish
- `Basis_Proxy_Sandbox` now uses a clearer visual hierarchy without changing row order or logic:
  - primary analysis blocks are styled more prominently
  - secondary support blocks are still readable but slightly less dominant
  - experimental / diagnostic blocks remain visible but visually separated from the main decision path
- longer policy / provenance / interpretation rows are now intentionally styled as note boxes instead of blending into nearby data tables
- frame-based areas keep the same four-frame content:
  - `Prior quarter`
  - `Quarter-open proxy`
  - `Current QTD`
  - `Next quarter thesis`
  - but now use clearer widths, alignment, and borders so source selection and frame values can be scanned faster
- `Coproduct quarterly history` and the coproduct experimental table keep all rows and methods, but now read more like tables and less like raw dumps through stronger headers, lighter striping, and better wrap/alignment

## Stage C: Coproduct-Aware Experimental Lenses

### Product rule
- coproduct-aware crush methods are now allowed as a first experimental lens
- they are intentionally:
  - `comparison only`
  - `eligible_official = False`
  - GPRE-only
- they do **not** replace:
  - `Approximate market crush`
  - `GPRE crush proxy`
  - `Best forward lens`

### Experimental family
- Stage C adds a separate coproduct-aware experimental family in the same GPRE comparison framework.
- The current bounded fractional/netted method set is:
  - `simple_plus_10pct_credit`
  - `simple_plus_15pct_credit`
  - `simple_plus_20pct_credit`
  - `simple_plus_25pct_credit`
  - `simple_plus_30pct_credit`
  - `simple_plus_10pct_coverage_credit`
  - `simple_plus_20pct_coverage_credit`
  - `simple_plus_30pct_coverage_credit`
  - `simple_plus_25pct_credit_less_2c`
  - `simple_plus_30pct_coverage_credit_less_2c`
- `simple_plus_half_credit` remains visible only as the prior-family reference in the sandbox summary so the newer fractional/netted family can be compared against the old best coproduct-aware method.
- All of them are defined in `$/gal`.
- `Approximate coproduct credit ($/gal)` is the overlay signal.
- `Coverage` only enters methods that explicitly say so.
- `Source mode` remains diagnostic, not a model input.

### Workbook surface
- `Economics_Overlay`
  - keeps the existing production/reference rows untouched
  - adds only one short discoverability note in `Proxy comparison ($/gal)`:
    - `Coproduct-aware experimental lenses live in Basis_Proxy_Sandbox and are comparison-only.`
- `Basis_Proxy_Sandbox`
  - adds a dedicated `Coproduct-aware experimental lenses` section
  - the section is intentionally separate from the current winner-story so comparison rows are not confused with promotion candidates

### Ranking policy
- coproduct-aware experimental methods are ranked in `$/gal`, not `$m`
- Stage C tracks:
  - `Best historical coproduct-aware`
  - `Best forward coproduct-aware`
  - `Best coproduct-aware experimental lens`
- `Best coproduct-aware experimental lens` intentionally follows the coproduct-family compromise role
- a strong experimental result does not auto-promote; promotion remains a separate decision

## Stage B.8 Coverage And Volume-Support Guardrails

### Coverage meaning
- `Coverage` means covered active-capacity footprint, not confidence and not imputed full-footprint truth.
- The visible coproduct note is intentionally short:
  - `Coverage reflects covered active-capacity footprint; values are covered-footprint weighted averages.`

### Weighting truth
- DDGS uses quarter-aware active-capacity weighting across the active GPRE footprint.
- Corn oil currently uses the same active-footprint weighting as a practical producer-subset approximation.
- Important limitation:
  - repo metadata still does not expose plant-level corn-oil producer flags
  - corn-oil weighting is therefore still an approximation, not proven exact producer-subset weighting

### `$m` interpretation
- `Approximate coproduct credit ($m)` is not redefined in Stage B.8.
- It continues to mean:
  - weighted coproduct credit `($/gal)`
  - multiplied by the existing implied gallons basis already used by the crush `$m` rows
- That means the current coproduct `$m` remains a compact full-company approximation rather than a strict covered-footprint-only `$m`.

### Operating_Drivers volume support
- Historical Operating_Drivers coproduct volumes are useful for QA / reasonableness checks:
  - `Distillers grains (k tons)`
  - `Renewable corn oil (million lbs)`
  - `Ultra-high protein (k tons)`
- They are **not** currently used for:
  - live weighting
  - live coverage
  - `Prior/Open/Current/Next` frame building
  - live `$m` replacement
- Reason:
  - they are historical actuals only
  - they are not plant-level or region-level
  - they do not provide frame-ready current / next-quarter support

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
- actual GPRE plant bids when a usable dated retained snapshot is available for the relevant frame or quarter
- AMS fallback otherwise

### Quarterly crush chart zero-line policy
- the quarterly crush chart keeps quarter labels at the bottom
- the category axis stays at the bottom; it should not visually appear to cross at zero
- when the visible y-range crosses zero, the chart may add a separate thin neutral `Zero reference line`
- that zero-reference line is diagnostic only:
  - not part of winner logic
  - not a promoted series
  - not a replacement for bottom quarter labels

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

## Stage 5 Winner Roles
Stage 5 keeps one production winner, but makes the decision roles explicit instead of
pretending one model is "best" on every axis.

- `Production winner`
  - the fitted model that still has to pass the existing selection / promotion guardrails
  - current verified winner: `process_utilization_regime_residual`
- `Best historical fit`
  - lowest clean-window MAE among official-eligible candidates
  - current verified winner: `process_utilization_regime_residual`
- `Best compromise`
  - best preview-supported / forward-usable compromise on hybrid score, hard-quarter MAE,
    and late-window tail MAE
  - current verified winner: `process_utilization_regime_blend`
- `Best forward lens`
  - best preview-supported high-forward-usability lens, even when it is not the production
    winner
  - current verified winner: `process_quarter_open_blend`

The key product rule is:
- do not silently promote the best forward lens into the production winner
- do show it explicitly when it differs, so future-quarter discussion does not overfit to
  ex-post fit

## Stage 5.1 Presentation Pass
Stage 5.1 is a small workbook/readability pass on top of stage 5. It does not change
production-winner logic or add a new model-ranking layer.

Verified stage-5.1 workbook surfaces:
- `Basis_Proxy_Sandbox`
  - adds a compact `Role summary` block for:
    - `Production winner`
    - `Best historical fit`
    - `Best compromise`
    - `Best forward lens`
  - each row stays compact and uses the same metrics:
    - `Hybrid`
    - `MAE`
    - `Forward`
  - keeps the longer `Winner story` block below it
- `Economics_Overlay`
  - `Bridge to reported` now includes:
    - `Approximate market crush ($m)`
    - `GPRE crush proxy ($m)`
    - `Best forward lens ($m)`
  - `Proxy comparison ($/gal)` now includes:
    - `Approximate market crush ($/gal)`
    - `GPRE crush proxy ($/gal)`
    - `Best forward lens ($/gal)`
  - the quarterly chart now plots three quarterly series:
    - `Approximate market crush`
    - `GPRE crush proxy`
    - `Best forward lens`

Legend note:
- top-left was the preferred visual goal
- the chart legend is intentionally placed at `top` because that is the clearest
  supported Excel/openpyxl position that avoids colliding with the x-axis date labels

Short interpretation note now surfaced in the sandbox:
- `Production winner = fitted row used in production`
- `Best forward lens = preview-oriented future-quarter lens`

## Stage 5.2 Readability Pass
Stage 5.2 is a narrow workbook/readability pass on top of stage 5.1. It does not
change model roles, selection logic, or promotion guardrails.

Verified stage-5.2 workbook surfaces:
- `Economics_Overlay`
  - `Implied gallons assumption` now spans `V:X`
  - `Volume basis` now spans `V:X`
  - the `Proxy comparison ($/gal)` note now spans `A:U`
  - the proxy note uses the same light note treatment and row height as the earlier
    note surface on the sheet
  - fitted / forward proxy-comparison comments are writer-compacted to a maximum of
    12 words so the saved workbook stays readable
- quarterly chart
  - title now reads:
    - `Approximate market crush vs Fitted models (quarterly)`
  - continues to show the three business series:
    - `Approximate market crush`
    - `GPRE crush proxy`
    - `Best forward lens`
  - now extends the chart path with preview / future-quarter proxy values from the
    existing proxy-comparison inputs instead of stopping at the historical quarterly
    frame
  - now uses a simpler quarter-labeled 3-series line chart with visible `YYYY-Q#`
    categories instead of the older quarterly boundary/helper-series approach
  - no quarterly `Quarter boundary` helper series remain in the chart surface

Legend note:
- top-left remained the visual preference
- `top` is still the chosen chart-API placement because it is the clearest supported
  non-overlapping position for the quarterly chart

## Stage 8 Coproduct Source Gate
Stage 8 is a narrow ingestion/readiness pass for `GPRE` coproduct inputs. It does
not activate a visible coproduct block on `Economics_Overlay`.

Verified Stage-8 source decisions:
- `NWER`
  - remains active
  - now parses coproduct rows for:
    - `Distillers Corn Oil Feed Grade`
    - `Distillers Grain Dried 10%`
- `AMS 3618`
  - is now the primary new coproduct source for this track
  - is wired as a provider off `viewReport/3618`
  - targets the ticker-local bioenergy folder
- `AMS 3511`
  - stays secondary / manual only in this stage
  - is not enabled as a live parsed source yet
- soybean stays out of scope in Stage B.1

Verified local folder policy:
- active bioenergy working folder:
  - [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)
- legacy folders still read for compatibility:
  - [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
  - [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)

Verified workbook-facing Stage-8 behavior at that point:
- `Basis_Proxy_Sandbox` now shows:
  - `Coproduct source gate`
  - `Coproduct signal readiness`
- current live gate result after the saved-workbook rebuild is:
  - `NWER coproduct rows = YES`
  - `AMS 3618 coproduct rows = NO`
  - `Renewable corn oil price = YES`
  - `Distillers grains price = YES`
  - `Approximate coproduct credit = YES`
  - `Overlay activation = HOLD`
- `Economics_Overlay` row `176` remains visually empty

Source reliability note:
- the `ams.usda.gov` category pages are useful manual/latest discovery pages
- the `viewReport/...` pages remain the canonical automated landing pages
- in the current local environment, USDA landing fetches are still flaky enough that
  manual drop + sync must remain the official fallback for `NWER` / `AMS 3618`

## Stage 14 Frames, Weighted History, and Readable Quarterly Charts
Stage 14 is a broader but still focused coproduct-economics pass on top of Stage 13.
It does not add a new sheet or a new gate panel, but it makes the coproduct lens
meaningfully more usable across frames and history.

Verified quarter-label fix:
- both quarterly charts now use same-sheet helper ranges on `Economics_Overlay`
- both quarterly charts now bind their x-axis categories through explicit `strRef`
  quarter labels
- both quarterly charts now also serialize the category axis explicitly as visible:
  - `delete = 0`
  - `auto = 0`
- the intended visible labels are `YYYY-Q#`, for example:
  - `2023-Q1`
  - `2023-Q2`
- the coproduct quarterly chart no longer depends on direct sandbox category refs
- this is meant to make the charts readable in Excel itself, not just structurally
  correct in the backing cells

Verified frame expansion:
- `Basis_Proxy_Sandbox` now includes `Coproduct frame summary`
- it summarizes:
  - `Prior quarter`
  - `Quarter-open proxy`
  - `Current QTD`
  - `Next quarter thesis`
- for each frame it shows:
  - `Renewable corn oil price`
  - `Distillers grains price`
  - `Approximate coproduct credit ($/bushel)`
  - `Approximate coproduct credit ($/gal)`
  - `Approximate coproduct credit ($m)`
  - `Resolved source mode`
  - `Coverage`
  - `Rule`
- the visible rows at `176:180` now read from this frame summary instead of the older
  current-heavy hidden-row path

Verified weighting rule:
- coproduct history and frame values now use quarter-aware active-capacity weighting
- DDGS is modeled across the active ethanol plant footprint
- corn oil uses the same active-footprint weighting as an explicit approximation,
  because repo metadata does not yet prove the exact corn-oil-producing subset at the
  plant level
- unsupported active-footprint share remains visible through `Coverage`

Verified history change:
- `Coproduct quarterly history` now stores:
  - `Quarter`
  - `Renewable corn oil price`
  - `Distillers grains price`
  - `Approximate coproduct credit ($/bushel)`
  - `Approximate coproduct credit ($/gal)`
  - `Approximate coproduct credit ($m)`
  - `Resolved source mode`
  - `Coverage`
- the same weighted resolver now drives both the frame summary and the quarterly
  history table

Verified visible addition:
- the visible block at rows `176:180` stays compact
- a compact mini-history table now appears below the coproduct chart:
  - title row `203`
    - `Recent coproduct history`
  - header row `204`
  - data rows `205:212`
- it shows the latest `8` quarters for:
  - `Quarter`
  - `Approximate coproduct credit ($/gal)`
  - `Approximate coproduct credit ($m)`
  - `Coverage`
  - `Source mode`
- the visible mini-history table is newest-first:
  - the top visible row is the most recent available quarter
  - the bottom visible row is the oldest quarter in the visible `8`-quarter window

## Stage 13 Comparable Units + Quarter-Safe Credit Lens
Stage 13 is a narrow writer/readback pass on top of Stage 12. It keeps the current
coproduct surface compact, but makes the credit lens more comparable to crush.

Verified visible block change:
- the visible coproduct block now uses:
  - row `176`
    - `Coproduct economics`
  - row `177`
    - `Renewable corn oil price`
  - row `178`
    - `Distillers grains price`
  - row `179`
    - `Approximate coproduct credit ($/gal)`
  - row `180`
    - `Approximate coproduct credit ($m)`
- the older visible `$ / bushel` credit row is no longer shown in the overlay
- the visible chart title now sits at row `182`:
  - `Approximate coproduct credit ($/gal, quarterly history)`

Verified sandbox/backing change:
- `Coproduct quarterly history` now stores:
  - `Quarter`
  - `Renewable corn oil price`
  - `Distillers grains price`
  - `Approximate coproduct credit ($/bushel)`
  - `Approximate coproduct credit ($/gal)`
  - `Approximate coproduct credit ($m)`
  - `Resolved source mode`
- `Approximate coproduct credit ($/gal)` is derived from:
  - `Approximate coproduct credit ($/bushel) / ethanol yield`
- `Approximate coproduct credit ($m)` is derived from:
  - `Approximate coproduct credit ($/gal) * quarter-aware implied gallons basis`
- the `($m)` row remains intentionally blank when that gallons basis is unavailable

Comparison rule:
- corn-oil and DDGS price legs remain visible in `$ / lb`
- the crush-comparable coproduct lens is the credit surface, not the price-leg surface
- the first visible coproduct chart therefore now uses `Approximate coproduct credit ($/gal)`,
  not `$ / bushel`

Quarter-label rule:
- both quarterly charts now use same-sheet helper-backed `YYYY-Q#` labels
- the coproduct chart no longer points directly at sandbox categories for its x-axis
- generic date labels like `2023-03-31` are intentionally avoided

## Stage 12 History + First Coproduct Chart
Stage 12 is a narrow writer/readback pass on top of Stage 11. It keeps the visible
coproduct block shape unchanged and adds historical backing plus a first compact
chart.

Verified sandbox additions:
- `Basis_Proxy_Sandbox` now includes `Coproduct quarterly history`
- the history table stores:
  - `Quarter`
  - `Renewable corn oil price`
  - `Distillers grains price`
  - `Approximate coproduct credit`
  - `Resolved source mode`
- the history rows use the same resolved quarterly source precedence as the visible
  current price rows
- `Resolved source mode` is compact provenance only:
  - `NWER`
  - `AMS 3618`
  - `Mixed`
  - `Unknown/blank`

Verified visible-history surface:
- rows `176:179` remain the only visible coproduct rows
- a new chart title sits below them:
  - row `181`
    - `Approximate coproduct credit (quarterly history)`
- the first visible coproduct chart is a single-series line chart for
  `Approximate coproduct credit`
- corn-oil and DDGS price history stay in the sandbox as backing data and are not
  mixed into the first visible chart

Policy continuity:
- `NWER` remains the primary live activation source
- `AMS 3618` remains secondary/corroborating/manual fallback-backfill
- Stage 12 adds history and a first chart, not a new gate or a broader coproduct
  panel

## Stage 11 Provenance + Manual Workflow Clarity
Stage 11 is a very narrow clarity/provenance/manual-workflow pass on top of Stage 10.
It does not add any new visible coproduct rows.

Verified workbook-facing clarification:
- the visible block stays at rows `176:179`
- row `176` is now labeled:
  - `Coproduct economics`
- the visible price rows still link to their real hidden source strings
- the credit-row source note is now neutral and truthful:
  - visible activation is `NWER`-sufficient
  - resolved price legs may come from `NWER` or `AMS 3618`

Verified sandbox-facing clarification:
- `Coproduct source gate` still treats `NWER` as the primary live activation source
- `AMS 3618` remains visible, but not blocking
- a compact `Source provenance` block now makes the role split explicit:
  - `Primary live activation source`
    - `NWER`
  - `Secondary corroborating source`
    - `AMS 3618`
  - `Current resolved workbook source`
    - `NWER`
    - `AMS 3618`
    - or `Mixed`, depending on the linked visible price-row source strings

Official manual workflow:
1. place manual PDFs in [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)
2. keep legacy folders only for compatibility:
   - [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
   - [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)
3. run:
   - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --refresh-market-data`
4. verify ingestion:
   - `GPRE.parquet` contains `nwer_pdf` and/or `ams_3618_pdf`
   - `Coproduct source gate` shows the relevant rows as `YES`
   - `Source provenance` shows which source currently feeds the visible price rows

Locked role interpretation:
- `NWER`
  - primary live activation source
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback / backfill source
- `3511`
  - deferred / manual

## Stage 9 First Visible NWER-Backed Block
Stage 9 is a narrow workbook/gating pass on top of Stage 8.

Verified gate change:
- the first visible coproduct block is now allowed with `NWER` as the sufficient
  primary source
- `AMS 3618` remains visible in the sandbox as:
  - secondary
  - corroborating
  - future improvement
- `AMS 3618 = NO` no longer blocks the first visible block by itself

Verified saved-workbook gate result at Stage-9 activation time:
- `NWER coproduct rows = YES`
- `AMS 3618 coproduct rows = NO`
- `Renewable corn oil price = YES`
- `Distillers grains price = YES`
- `Approximate coproduct credit = YES`
- `Overlay activation = GO`

Verified visible workbook surface:
- `Economics_Overlay` now uses rows `176:179` for a compact quarter-grid block:
  - row `176`
    - `Coproduct economics (NWER-backed)`
  - row `177`
    - `Renewable corn oil price`
  - row `178`
    - `Distillers grains price`
  - row `179`
    - `Approximate coproduct credit`

Verified source wiring:
- `Renewable corn oil price`
  - links to the existing hidden/live market row
- `Distillers grains price`
  - links to the existing hidden/live market row
- `Approximate coproduct credit`
  - links to the sandbox build-up row
  - uses the intended quarter cells `C / E / G / I` from `Basis_Proxy_Sandbox`

Boundary note:
- this visible block is economic only
- no new physical/yield rows are mirrored into the overlay
- `Operating_Drivers` remains the home for physical / yield / mix

## Stage 10 AMS 3618 Secondary Manual Source
Stage 10 is a very narrow manual-ingest pass on top of Stage 9. It does not change
the visible block shape and does not make `AMS 3618` a blocker again.

Verified manual-ingest fix:
- `AMS3618Provider` now accepts manual raw entries even when `report_date` is blank
- undated manual filenames like `ams_3618_00183.pdf` are now supported
- the parser now accepts the real USDA report-date line:
  - `Livestock, Poultry and Grain Market News ...`

Verified current manual-source behavior:
- manually dropped PDFs in [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)
  now flow through raw -> parsed -> export
- current `GPRE` export now contains `ams_3618_pdf` coproduct rows for:
  - corn oil:
    - `corn_oil_eastern_cornbelt`
    - `corn_oil_iowa_avg`
    - `corn_oil_kansas`
    - `corn_oil_minnesota`
    - `corn_oil_missouri`
    - `corn_oil_nebraska`
    - `corn_oil_south_dakota`
    - `corn_oil_wisconsin`
  - DDGS:
    - `ddgs_10_illinois`
    - `ddgs_10_indiana`
    - `ddgs_10_iowa`
    - `ddgs_10_kansas`
    - `ddgs_10_michigan`
    - `ddgs_10_minnesota`
    - `ddgs_10_missouri`
    - `ddgs_10_nebraska`
    - `ddgs_10_ohio`
    - `ddgs_10_south_dakota`
    - `ddgs_10_wisconsin`

Verified current workbook-facing result:
- `Basis_Proxy_Sandbox` now shows:
  - `NWER coproduct rows = YES`
  - `AMS 3618 coproduct rows = YES`
  - `Overlay activation = GO`
- the visible `Economics_Overlay` coproduct block at rows `176:179` remains unchanged

Locked product role:
- `NWER`
  - primary live source
  - first visible block source
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback / backfill source
  - not a blocker

## Owner Earnings + Economics QA Pass
This was a selective QA-/presentation-pass on top of the stage-5 overlay work. It
did not change model roles or add a new diagnostics panel.

Verified additions:
- `Valuation`
  - the GPRE `Owner earnings (proxy)` / `Cash-flow quality` issue was a row-placement
    bug, not a stale-test-only problem
  - hidden GPRE thesis-bridge labels are now filtered before row placement, so the
    visible rows stay present in the saved workbook
- `Economics_Overlay` / `Basis_Proxy_Sandbox`
  - `Role summary`, `Winner story`, `Bridge to reported`, `Proxy comparison`, and the
    quarterly chart were rechecked against the current `model_result` surface
  - `Best forward lens` remains consistent across summary, bridge, proxy comparison,
    and the quarterly chart
  - the three plant-execution commentary rows remain routed to `Operating_Drivers`
    instead of leaking back into `Economics_Overlay`

Workbook note:
- no extra QA table was added to `Basis_Proxy_Sandbox`
- the existing role summary plus the current method / leaderboard surfaces were judged
  sufficient and less noisy for normal workbook use

## Stage 6 Coproduct Readiness Pass
Stage 6 is a narrow coproduct inventory/readiness pass. It does not add a visible
coproduct panel to `Economics_Overlay` yet.

Verified stage-6 workbook surfaces:
- `Economics_Overlay`
  - still carries the hidden coproduct scaffolding rows for:
    - `Renewable corn oil yield`
    - `Distillers yield`
    - `Ultra-high protein yield`
    - `Distillers grains price`
    - `Ultra-high protein price`
    - `Renewable corn oil price`
    - `Soybean oil price proxy`
    - `Corn oil premium assumption`
    - `Implied renewable corn oil proxy price`
  - does **not** add a visible coproduct block yet
  - row `176` remains visually available until the visible block is ready
- `Basis_Proxy_Sandbox`
  - keeps the existing `Approximate market crush build-up ($/gal)` section as the
    simple coproduct/backing build-up
  - now adds `Coproduct signal readiness`, a compact coverage/readiness section for:
    - `Renewable corn oil price`
    - `Soybean oil price proxy`
    - `Corn oil premium assumption`
    - `Implied renewable corn oil proxy price`
    - `Distillers grains price`
    - `UHP price`
    - `Approximate coproduct credit`
  - shows:
    - source mode
    - direct / proxy / assumption / derived status
    - current fill state
    - historical / current / next readiness

Placement note:
- `Operating_Drivers` remains the first home for physical yield / mix signals such as
  renewable corn oil volume, protein / coproduct mix, and DDGS / UHP commentary
- `Economics_Overlay` remains the intended home for the future compact **economic**
  coproduct story
- `Basis_Proxy_Sandbox` is the current backing home for coverage, provenance, and
  gating

Current gating rule:
- no visible `Economics_Overlay` coproduct block until at least:
  - renewable-corn-oil price / proxy resolves non-blank
  - approximate coproduct credit resolves non-blank

Current sequencing:
- corn oil first
- DDGS second
- UHP / protein mix later and more cautiously

## Stage 7 Corn Oil Gate Pass
Stage 7 is a very narrow corn-oil-only pass on top of Stage 6. It does not activate
the visible overlay block yet.

Verified stage-7 workbook surfaces:
- `Basis_Proxy_Sandbox`
  - now adds `Corn oil gate check` directly ahead of `Coproduct signal readiness`
  - the gate block checks:
    - `Soybean oil price proxy`
    - `Corn oil premium policy`
    - `Renewable corn oil price`
    - `Approximate coproduct credit`
    - `Overlay activation`
  - the block keeps the current result explicit:
    - `NO` / `NO` / `NO` / `NO`
    - `HOLD` for overlay activation
  - the block also adds a short provenance note:
    - corn-oil premium stays manual
    - verified management commentary supports premium to soybean oil directionally
    - commentary is not treated as an auto-filled quarter default
- `Economics_Overlay`
  - still does not add a visible corn-oil block
  - row `176` remains the activation point, but stays blank in the current gate state

Current Stage 7 blockers:
- no verified soybean-oil market series in the current GPRE export path
- corn-oil premium is still manual-only
- resolved renewable-corn-oil price remains blank
- approximate coproduct credit remains blocked on the corn-oil path

Placement note:
- `Operating_Drivers` still owns physical/yield/mix
- `Economics_Overlay` is still the intended home for the eventual compact economic
  corn-oil story
- `Basis_Proxy_Sandbox` remains the backing layer for gate status, provenance, and
  activation logic

## Candidate Families And Stage 5 Additions
The current GPRE proxy pass still compares the existing bounded families:
- official / simple
- bridge timing
- process timing
- quarter-open / current blend
- execution / utilization overlays
- inventory-gap penalties
- asymmetric passthrough
- residual / regime splits
- gated ensembles
- hedge-memo families

Stage 5 adds four bounded candidates that stay inside preview-available signals:
- `process_market_process_ensemble_35_65`
  - bounded market/process compromise
- `process_locked_share_asymmetric_passthrough`
  - forward-first locked-share / asymmetric passthrough blend
- `process_prior_gap_carryover_small`
  - bounded prior-gap carryover
- `process_prior_disturbance_carryover`
  - bounded prior-disturbance carryover

These are intentionally simple enough to stay interpretable in the workbook and cheap
enough to avoid turning the pass into a modeling / optimization project.

## Charts
The current workbook now uses two GPRE proxy charts on `Economics_Overlay`.

### Weekly chart
- title: `Simple crush margin proxy (weekly)`
- keeps the official/simple weekly history path
- continues to include the thesis helper series already used by the overlay

### Quarterly chart
- title: `Approximate market crush vs Fitted models (quarterly)`
- sits directly under the weekly chart
- uses quarterly data because the fitted GPRE proxy does not have a verified separate
  weekly history path in the current runtime
- compares:
  - `Approximate market crush ($/gal)`
  - `GPRE crush proxy ($/gal)`
  - `Best forward lens ($/gal)`
- extends beyond pure historical quarterly rows by appending preview / future-quarter
  points from the already-selected proxy-comparison path
- keeps a date-axis scatter path underneath, but surfaces readable quarter labels such
  as `2023-Q1` and `2026-Q3` directly inside the chart

This chart is meant to stay visually light. It is a quarterly comparison aid, not a new
dashboard section.

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
  - GPRE basis/proxy candidate comparison, forward-role scoring, and winner story inputs
- [`pbi_xbrl/excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py)
  - visible overlay wording
  - chart/output integration
  - final sheet order, including `Promise_Progress_UI -> Basis_Proxy_Sandbox -> Hidden_Value_Flags`
  - workbook-facing provenance display
- [`pbi_xbrl/excel_writer_economics_overlay.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_economics_overlay.py)
  - `Basis_Proxy_Sandbox`
  - proxy comparison panel
  - proxy-implied results panel
  - the short stage-5 note that explains official row, fitted row, production winner, and
    best forward lens

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
- coproduct source-gate statuses stay truthful and do not go green on missing sources
- `Basis_Proxy_Sandbox` sits directly to the right of `Promise_Progress_UI`
- the workbook still shows the production winner and a separate best forward lens when they differ
- visible source wording stays concise

## Acceptance Notes
Saved-workbook acceptance should check:
- quarter-open provenance is truthful
- thesis ethanol provenance is truthful
- official vs fitted rows stay separate
- weekly official/simple chart still exists
- quarterly fitted-vs-official chart exists directly under the weekly chart
- `Basis_Proxy_Sandbox` winner story includes:
  - `Best historical fit`
  - `Best compromise`
  - `Best forward lens`
- a blank value is blank for a real reason
  - missing months
  - missing frozen history
  - missing observed overlap
  - not because of a silent fallback to the wrong source

For broader workbook acceptance rules, see
- [`WORKBOOK_ACCEPTANCE.md`](/c:/Users/Jibbe/Aktier/Code/docs/WORKBOOK_ACCEPTANCE.md)
