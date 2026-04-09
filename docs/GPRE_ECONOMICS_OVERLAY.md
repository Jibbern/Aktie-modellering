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
- secondary manual feedstuffs folder:
  - [`GPRE/USDA_feedstuffs_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_feedstuffs_reports)
- legacy folders still read for compatibility:
  - [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
  - [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)

Verified workbook-facing Stage-8 behavior:
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
