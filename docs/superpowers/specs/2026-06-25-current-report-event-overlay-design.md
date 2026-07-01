# Current Report Capital Structure Overlay Design

## Objective

Make newly collected post-quarter SEC capital-structure events visible, traceable, and decision-useful in the PBI and GPRE Excel models without rewriting reported historical quarters. Also make filing freshness and the material current/post-quarter effects immediately visible on `SUMMARY`, using the same normalized source and event records as `Valuation` and support/audit surfaces.

## Scope

### PBI

Use the June 25, 2026 Form 8-K package:

- Form 8-K accession `0001193125-26-281893`
- Exhibit 10.1, Fourth Amendment to Credit Agreement
- Exhibit 99.1, redemption and Term Loan A announcement

Extract and retain these event facts:

- `$347 million` of 6.875% senior notes due 2027 were redeemed.
- Term Loan A was increased by `$150 million`.
- Term Loan A principal became `$302 million`.
- Gross principal debt changed by `-$197 million` before fees, premium, accrued interest, expenses, cash use, revolver use, or other liquidity sources.
- The next scheduled maturity became March 2029.
- The amended Term Loan A retains its May 18, 2031 maturity and existing terms.
- The event occurred after the March 31, 2026 reported quarter.

Reported March 31, 2026 debt, cash, net debt, earnings, cash flow, and debt-tranche history must remain unchanged in `History_Q`, `Debt_Profile`, and `Debt_Tranches_Latest`.

The user-facing `Valuation` Debt Detail must, however, represent the current post-event principal structure rather than presenting the redeemed 2027 notes as active debt:

- Remove the 2027 senior notes from the active current Debt Detail, or label them unambiguously as redeemed and excluded.
- Show Term Loan A principal of `$302 million`.
- Show the next scheduled maturity as March 2029.
- Show the Term Loan A maturity as May 18, 2031.
- Label the section as current/post-quarter or pro-forma principal structure and distinguish it from reported March 31, 2026 values.

The workbook must also include a visible reconciliation:

- reported March 31, 2026 debt detail before refinancing;
- 2027 notes redeemed: `-$347 million`;
- incremental Term Loan A: `+$150 million`;
- amended Term Loan A total: `$302 million`;
- current post-event principal detail;
- gross principal delta: `-$197 million`.

The source states that existing cash, other liquidity, and potentially revolving borrowings were used together with the incremental term loan to redeem the notes and pay fees, costs, and expenses. Therefore:

- cash/liquidity use must be described as at least the gap between redeemed principal and incremental term debt;
- exact cash use, revolver use, fees, costs, premium, accrued interest, and other liquidity sources remain unresolved unless separately source-backed;
- automatic pro-forma net-debt adjustment remains disabled/manual;
- reported March 31, 2026 cash and net debt remain unchanged.

### GPRE

Use the June 22, 2026 Form S-3ASR package and its June 16, 2026 transaction exhibits:

- Form S-3ASR accession `0001104659-26-076397`
- Membership Interest Purchase Agreement
- Four warrant exhibits

Extract and retain these event facts:

- The Purchase Agreement states `500,000` warrants to purchase common stock.
- The four warrant exhibits contain `366,240`, `37,120`, `10,360`, and `86,280` warrants, totaling exactly `500,000`.
- The S-3ASR prospectus registers up to `550,000` shares of common stock offered by selling stockholders and issuable upon exercise of the outstanding warrants.
- The prospectus states that the registered amount covers the maximum shares issuable without regard to the beneficial ownership limitation.
- Exercise price is `$0.01` per share.
- Expiration is June 16, 2036.
- Beneficial ownership limitation is `19.8%`.
- The warrants and maximum issuable shares represent potential dilution, not reported outstanding common shares.
- The transaction occurred after the March 31, 2026 reported quarter.

The workbook must retain both quantities as separate, source-traceable fields:

- `warrants_issued = 500000`
- `potential_common_shares_issuable_max = 550000`
- `exercise_price = 0.01`
- `expiration_date = 2036-06-16`
- `beneficial_ownership_limitation = 0.198`

Reported March 31, 2026 shares, diluted shares, EPS, debt, cash, and net debt must remain unchanged. The legal warrant count and maximum issuable share count must not be folded into `History_Q`.

Valuation must expose a full-dilution overlay using `0.550 million` incremental shares:

- `reported_diluted_shares_m`
- `post_quarter_potential_dilution_shares_m = 0.550`
- `diluted_shares_full_dilution_overlay_m = reported_diluted_shares_m + 0.550`

The core reported `SharesDiluted` named range must not be silently replaced. The workbook must either add a third clearly labelled per-share mode, `Diluted + post-quarter warrants`, or add a separate visible value-per-share sensitivity row using reported diluted shares plus `0.550 million`. The 19.8% beneficial ownership limitation does not cap the full-dilution sensitivity because the prospectus registers the maximum number issuable without regard to that limitation.

The visible GPRE narrative must state:

> Post-quarter BlackRock warrant overlay: 500k warrants issued; S-3 registers up to 550k common shares issuable on exercise. Reported 2026-Q1 shares/EPS unchanged; valuation full-dilution sensitivity uses +0.55m shares.

## Data Flow

1. Discover the relevant locally cached SEC filing and exhibits from the ticker material roots and SEC cache.
2. Parse only explicit, source-backed event facts from the documents.
3. Emit a small normalized post-quarter event record containing:
   - ticker
   - reported-quarter anchor
   - event date and filing date
   - filing type
   - downloaded-at timestamp
   - event type
   - legal instrument quantities
   - maximum valuation-overlay quantities
   - numeric amounts and units
   - history treatment
   - valuation/dilution treatment
   - whether and where the event is used in the workbook
   - accession, document, and source path
   - whether the source path currently exists
4. Feed the normalized records into an existing support/audit surface when its schema is sufficient.
5. If existing schemas cannot represent legal warrants, maximum issuable shares, reported values, and overlay values without ambiguity, create a narrowly scoped `PostQuarter_Capital_Events` support sheet.
6. Apply the normalized records to current/pro-forma presentation surfaces without mutating reported historical datasets.
7. Build a small filing-freshness record for the workbook ticker from:
   - the latest reported quarter and its filing metadata already represented in `History_Q`, `SEC_Audit_Log`, or the financial-statement manifest;
   - the latest normalized model-relevant additional/post-quarter filing, if one exists;
   - the source-refresh log or manifest download timestamp;
   - the normalized event's usage surfaces and source-path status.
8. Render concise visible companion rows in `Valuation` and, where useful, the ticker investment-case sheet.
9. Render `Source / Filing Freshness` and `Post-quarter / Current Effects` on `SUMMARY` from those same normalized records.

The implementation should reuse existing source-root, cached-document, quarter-note, and valuation support boundaries. It must not add a broad new ingestion framework.

## Workbook Behavior

### Required visible behavior

- PBI `Valuation` shows current post-refinancing Debt Detail, a reported-to-current reconciliation, and a clearly labelled post-quarter refinancing note.
- PBI active current Debt Detail does not show the 2027 senior notes as outstanding debt.
- PBI current Debt Detail shows Term Loan A of `$302 million`, next scheduled maturity in March 2029, and Term Loan A maturity on May 18, 2031.
- GPRE shows a clearly labelled post-quarter potential-dilution note and a value-per-share sensitivity using `+0.550 million` shares.
- GPRE keeps `500,000` warrants and `550,000` maximum issuable shares separately visible.
- Each visible note identifies that reported quarter values are unchanged.
- Source metadata remains traceable to the SEC accession and document.

### Summary behavior

`SUMMARY` must add two user-facing sections after the existing company and financial overview. These sections are views over normalized source/event records; they must not become independent data sources or calculate unsupported estimates.

#### Source / Filing Freshness

The section contains one row for the ticker whose workbook is being generated. It does not list the whole portfolio inside every workbook.

Required columns:

- Ticker
- Latest reported quarter
- Latest reported filing type
- Latest reported filing accession
- Latest reported filing date
- Latest reported filing `downloaded_at`
- Latest additional/post-quarter filing type
- Latest additional/post-quarter filing accession
- Latest additional/post-quarter filing date
- Latest additional/post-quarter `downloaded_at`
- Event type
- Used in workbook? (`Yes`, `No`, or `Review`)
- Used surfaces
- Source path exists? (`Yes` or `No`)

Rules:

- The latest reported filing must be derived from the latest reported quarter and existing filing/audit/manifest metadata, not inferred from a newer unrelated filing.
- The latest additional filing must be the latest downloaded filing that produced a model-relevant normalized event, not simply the latest SEC filing by date.
- `downloaded_at` should use an explicit source-refresh log or manifest timestamp when available. A filesystem timestamp may be used only as a clearly identified fallback.
- `Used surfaces` must be generated from the same event usage metadata that drives workbook rendering.
- `Source path exists?` must be evaluated against the actual selected source path at build time.
- PBI must identify the June 2026 8-K package and exhibits as a refinancing/redemption event used in `Valuation` current Debt Detail and support/audit.
- GPRE must identify the June 2026 S-3ASR and warrant exhibits as a warrant-dilution event used in Valuation dilution sensitivity and support/audit.
- If no newer model-relevant normalized event exists, ANF, GTX, and any other ticker must show `None newer / no model-relevant post-quarter event`. They must not receive synthetic or inferred post-quarter events.

#### Post-quarter / Current Effects

Required columns:

- Ticker
- Event date
- Filing date
- Area
- Reported-quarter anchor
- Reported value
- Current / overlay value
- Change
- Unit
- Confidence / treatment
- Historical treatment
- Valuation treatment
- Source document / accession

The rows must be deterministic projections of the normalized event record. They must not repeat parsing or independently derive valuation values.

Required PBI rows:

- 2027 senior notes: reported Q1 active principal of approximately `$347 million`, current active principal `$0` / redeemed, change `-$347 million`, source-backed, current `Valuation` Debt Detail updated.
- Term Loan A: reported Q1 principal of approximately `$152 million`, current principal `$302 million`, change `+$150 million`, source-backed, current `Valuation` Debt Detail updated.
- Gross principal delta: `-$197 million`, explicitly limited to source-backed gross principal.
- Cash / net debt: reported Q1 values unchanged; current exact values `Unresolved / manual review` because cash use, revolver use, fees, costs, premium, accrued interest, and expenses are not fully source-backed.
- Next scheduled maturity: March 2029.
- Term Loan A maturity: May 18, 2031.

Required GPRE rows:

- Warrants issued: `500,000`.
- Potential common shares issuable maximum: `550,000`.
- Valuation full-dilution overlay: `+0.550 million` shares.
- Exercise price: `$0.01`.
- Expiration: June 16, 2036.
- Reported shares and EPS: unchanged.
- Valuation treatment: a clearly labelled full-dilution value-per-share sensitivity or `Diluted + post-quarter warrants` mode.

Treatment labels must be explicit:

- Confidence / treatment: `Source-backed`, `Partial / unresolved`, or `Manual review`.
- Historical treatment: for example `History_Q unchanged`, `Debt_Profile unchanged`, or `Shares/EPS unchanged`.
- Valuation treatment: for example `Current Debt Detail updated`, `Full-dilution sensitivity`, or `No auto net-debt adjustment`.

The Summary tables must remain readable without hiding source uncertainty. In particular, PBI cash and current net debt must not be displayed as exact values while their components remain unresolved.

### Required support behavior

- The normalized event facts are available in an existing support/audit surface, or in one narrowly scoped support surface if existing schemas cannot represent them safely.
- PBI's debt movements are not folded into `History_Q`, `Debt_Profile`, or `Debt_Tranches_Latest`.
- PBI's current/pro-forma Debt Detail is generated as a presentation overlay from the reported debt table plus source-backed post-quarter principal changes.
- GPRE's warrants and maximum issuable shares are not folded into reported share-count rows or reported EPS.
- Duplicate filing, cache, and material-root copies collapse to one normalized capital event.

### Valuation behavior

- PBI current gross principal and maturity structure must reflect the completed refinancing.
- Automatic PBI pro-forma net debt must remain disabled unless all cash and transaction-cost components are source-backed.
- GPRE full-dilution valuation must use `550,000` potential incremental shares.
- Automatic GPRE reported-share or reported-EPS adjustment must remain disabled.
- GPRE exercise proceeds may be shown separately if helpful, but the `$0.01` exercise price must not be confused with a meaningful financing offset.

## Error Handling

- Missing or malformed event documents must leave the existing workbook behavior unchanged.
- Partial matches must generate audit/review output rather than estimated values.
- Percentage values, dates, rates, and maturity years must not be interpreted as dollar or share amounts.
- Duplicate filing and local-material copies must collapse to one normalized event.
- Failure to resolve PBI cash, revolver use, fees, premium, accrued interest, costs, or expenses must disable automatic pro-forma net debt while still allowing source-backed principal and maturity presentation.
- Failure to resolve the distinction between GPRE warrant count and maximum issuable shares must disable the valuation overlay rather than defaulting to `500,000`.

## Test Strategy

Tests are added before production changes and must initially fail for the missing behavior.

### Parser tests

- PBI extracts `$347 million`, `$150 million`, and `$302 million` from representative official filing text.
- PBI does not interpret `6.875%`, `2027`, `2029`, or `2031` as debt amounts.
- GPRE aggregates the four warrant exhibits to exactly `500,000` warrants.
- GPRE extracts `550,000` maximum shares issuable/registered from the S-3 prospectus.
- GPRE extracts `$0.01`, June 16, 2036, and `19.8%` into distinct typed fields.
- GPRE does not treat registration, authorization, or maximum issuable shares as exercised or reported shares.
- Both events are anchored after March 31, 2026 and marked as non-historical overlays.
- Duplicate source copies produce one event.

### Writer tests

- PBI receives a visible post-quarter refinancing note and reported-to-current debt reconciliation with source traceability.
- PBI current `Valuation` Debt Detail excludes the redeemed 2027 notes as active debt.
- PBI current `Valuation` Debt Detail shows `$302 million` Term Loan A and March 2029 as the next scheduled maturity.
- PBI `History_Q`, `Debt_Profile`, and `Debt_Tranches_Latest` reported rows remain unchanged.
- PBI does not calculate automatic pro-forma net debt while cash, fees, premium, accrued interest, costs, expenses, or revolver use remain unresolved.
- PBI `Valuation` or `PBI_Investment_Case` states that refinancing risk improved while reported Q1 debt, cash, and net debt remain unchanged.
- GPRE receives a visible potential-dilution note with source traceability.
- GPRE reported shares and EPS history remain unchanged.
- GPRE valuation full-dilution sensitivity uses exactly `+0.550 million` shares.
- GPRE retains both the `500,000` legal warrant count and `550,000` maximum issuable shares.
- Duplicate source copies do not create duplicate support or visible rows.
- `SUMMARY` contains `Source / Filing Freshness` and `Post-quarter / Current Effects`.
- PBI and GPRE Summary freshness rows include the latest model-relevant additional filing date and `downloaded_at`.
- PBI Summary identifies the refinancing/redemption event as used in `Valuation` current Debt Detail.
- GPRE Summary identifies the warrant event as used in the Valuation full-dilution sensitivity.
- PBI Summary displays current cash/net debt as unresolved/manual rather than an exact current value.
- PBI Summary states that `History_Q` and `Debt_Profile` remain unchanged.
- GPRE Summary states that reported shares and EPS remain unchanged.
- Every source path represented as active in Summary resolves to an existing file.
- ANF, GTX, and synthetic unknown tickers do not receive a post-quarter effect when no model-relevant normalized event exists.

### Regression tests

- Existing PBI, GPRE, and ANF investment-case behavior.
- Generic unknown-ticker investment-case behavior.
- Workbook validation runner.
- Visible data coverage and valuation/hidden-value guardrails.

## Build and Promotion

All workbook builds use staging paths under `StockModelData\staging`. Existing canonical PBI, GPRE, and ANF outputs remain untouched until:

1. targeted parser and writer tests pass;
2. focused regressions pass;
3. staged PBI and GPRE workbooks build successfully;
4. workbook and render/style validation pass;
5. content inspection confirms the new event overlays and Summary freshness/effects tables are visible and historical rows remain unchanged.

Canonical promotion, if approved after verification, replaces only the affected PBI and GPRE `.xlsx` files and preserves rollback copies or hashes of the prior files.

## Cache Cleanup Guardrails

The accidental cache root `C:\Users\Jibbe\Aktier\sec_cache\PBI` contains 288 files created by the June 25 refresh. A read-only inventory found:

- 114 files that are byte-for-byte duplicates of files under the canonical `StockModelData\sec_cache\PBI`;
- 174 generated indexes, manifests, package metadata, or differently packaged source files without a byte-identical canonical match;
- existing financial-statement manifests that reference historical files in the accidental cache;
- the current canonical PBI workbook contains source-path text referencing the accidental cache.

Therefore the accidental cache root must not be recursively deleted or moved.

Cleanup must follow this sequence:

1. Print and retain an exact candidate list including path, size, timestamp, and SHA-256.
2. Match each candidate to an existing canonical file by SHA-256 or verified source identity.
3. Search code, manifests, logs, debug profiles, staging outputs, and workbook package XML for the exact candidate path.
4. Do not remove a referenced candidate unless the reference is first regenerated to an existing canonical path and the regenerated artifact is validated.
5. Limit initial cleanup to the June 25 refresh artifacts that are both duplicate and unreferenced. The latest accidental package `000119312526281893` is a cleanup candidate only after exact reference checks and canonical source traceability pass.
6. Prefer moving removable candidates to a clearly named quarantine directory over permanent deletion unless the candidate is trivially reproducible and verified byte-identical.
7. Leave all ambiguous, unique, or referenced files in place and report why.
8. Rebuild PBI and GPRE staging workbooks after cleanup and confirm every workbook source path resolves to an existing file.

The cleanup report must list every removed, quarantined, and retained path with its reason.

## Explicit Non-Goals

- No GTX implementation changes.
- No ANF behavior changes.
- No restatement of reported history.
- No automatic warrant exercise assumption.
- No use of the 19.8% beneficial ownership limitation to reduce the GPRE full-dilution sensitivity.
- No automatic PBI net-debt adjustment based only on gross debt movements.
- No presentation of redeemed PBI 2027 notes as active current debt in the user-facing `Valuation` Debt Detail.
- No independent Summary parsing, estimates, or second source of truth.
- No fake post-quarter events for ANF, GTX, or generic tickers without model-relevant normalized records.
- No unrelated source-refresh refactor or filename cleanup.
- No recursive deletion or broad move of `C:\Users\Jibbe\Aktier\sec_cache\PBI`.
