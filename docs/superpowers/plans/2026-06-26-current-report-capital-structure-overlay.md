# Current Report Capital Structure Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse the June 2026 PBI refinancing and GPRE BlackRock warrant events, preserve reported history, and expose source-traceable current/pro-forma capital-structure overlays plus filing freshness/current effects on `SUMMARY` in staged Excel models.

**Architecture:** Add one focused parser module that scans only the known official filing families and returns one normalized row per event. Add one focused workbook module that writes `PostQuarter_Capital_Events` and supplies presentation helpers. Add one narrow Summary helper that projects the normalized event frame plus existing History/Audit/manifest metadata into filing-freshness and current-effects tables. Build these frames once in the writer context and pass them to both Summary and Valuation; transform only the PBI display copy and add a separate GPRE full-dilution sensitivity without mutating pipeline inputs or named reported-share ranges.

**Tech Stack:** Python 3.13, pandas, BeautifulSoup, openpyxl, pytest, existing stock-model pipeline and validators.

---

## File Structure

- Create `pbi_xbrl/post_quarter_capital_events.py`: source discovery, text parsing, event normalization, duplicate collapse, and pure PBI debt-display transformation.
- Create `pbi_xbrl/excel_writer_post_quarter_capital_events.py`: support-sheet writer and compact PBI/GPRE Valuation overlay rendering.
- Create `pbi_xbrl/excel_writer_summary_freshness.py`: pure filing-freshness/current-effects frame builders and compact Summary table renderer.
- Modify `pbi_xbrl/excel_writer_context.py`: build the normalized event and Summary projection frames from scoped material/cache roots plus existing History/Audit/manifest data, pass them to Summary and Valuation, and order the support sheet.
- Modify `pbi_xbrl/excel_writer_summary_sheet.py`: append the two structured Summary tables after the existing overview sections.
- Modify `pbi_xbrl/excel_writer_valuation_orchestrator.py`: accept the event frame, write the support sheet, and expose it to the debt-detail renderer.
- Modify `pbi_xbrl/excel_writer_valuation_debt_detail_render.py`: render the PBI current debt table from a transformed copy and append ticker-specific event overlays.
- Modify `pbi_xbrl/writer_types.py`: retain the three normalized run-scoped frames in `WriterDerivedData`.
- Create `tests/test_post_quarter_capital_events.py`: parser, duplicate, pure transformation, and production-writer behavior tests.
- Create `tests/test_summary_filing_freshness.py`: pure Summary projection and production-writer tests, including generic no-event behavior.
- Create staging-only cleanup inventory under `StockModelData\staging\source-refresh-2026-06-26\cache_cleanup`; do not add cleanup artifacts to Git.

### Task 1: Normalize PBI and GPRE events

**Files:**
- Create: `pbi_xbrl/post_quarter_capital_events.py`
- Create: `tests/test_post_quarter_capital_events.py`

- [ ] **Step 1: Write failing parser tests**

Tests construct representative official HTML fixtures and call:

```python
events = build_post_quarter_capital_events(
    ticker="PBI",
    material_roots=[pbi_root],
    cache_roots=[pbi_cache],
)
```

Required PBI assertions:

```python
assert event["principal_redeemed"] == 347_000_000.0
assert event["incremental_term_loan"] == 150_000_000.0
assert event["term_loan_total"] == 302_000_000.0
assert event["gross_principal_delta"] == -197_000_000.0
assert event["next_scheduled_maturity"] == "March 2029"
assert event["term_loan_maturity"] == "2031-05-18"
assert event["automatic_net_debt_adjustment"] is False
```

Required GPRE assertions:

```python
assert event["warrants_issued"] == 500_000.0
assert event["potential_common_shares_issuable_max"] == 550_000.0
assert event["exercise_price"] == 0.01
assert event["expiration_date"] == "2036-06-16"
assert event["beneficial_ownership_limitation"] == 0.198
```

Duplicate material/cache copies must still produce exactly one event row.

- [ ] **Step 2: Run parser tests and verify RED**

Run:

```powershell
C:\Users\Jibbe\Aktier\.venv\Scripts\python.exe -m pytest tests/test_post_quarter_capital_events.py -k "parser or duplicate" -v
```

Expected: collection/import failure because `post_quarter_capital_events` does not exist.

- [ ] **Step 3: Implement minimal parser**

Create:

```python
POST_QUARTER_EVENT_COLUMNS = (
    "ticker",
    "event_key",
    "event_type",
    "reported_quarter_anchor",
    "event_date",
    "filing_type",
    "filing_date",
    "downloaded_at",
    "accession",
    "principal_redeemed",
    "incremental_term_loan",
    "term_loan_total",
    "gross_principal_delta",
    "next_scheduled_maturity",
    "term_loan_maturity",
    "warrants_issued",
    "potential_common_shares_issuable_max",
    "exercise_price",
    "expiration_date",
    "beneficial_ownership_limitation",
    "automatic_net_debt_adjustment",
    "history_treatment",
    "valuation_treatment",
    "used_in_workbook",
    "used_surfaces",
    "source_documents",
    "source_paths",
    "source_urls",
    "source_path_exists",
    "qa_status",
)
```

The parser must:

- scan only filenames/accessions matching `d88573` / `000119312526281893` for PBI;
- scan only filenames/accessions matching `tm2618355` / `000110465926076397` for GPRE;
- strip HTML before matching;
- require labelled monetary/share contexts;
- aggregate GPRE warrant exhibits by warrant number and unique amount;
- use the S-3 prospectus, not the legal warrant count, for `potential_common_shares_issuable_max`;
- serialize unique source documents and paths deterministically;
- resolve `downloaded_at` from the existing source-refresh log or manifest, falling back to a clearly marked file timestamp only when no explicit timestamp exists;
- set usage surfaces and source-path existence on the same normalized event row used by workbook renderers;
- return an empty frame with the declared schema when no complete event exists.

- [ ] **Step 4: Run parser tests and verify GREEN**

Run the Task 1 test command. Expected: all selected tests pass.

- [ ] **Step 5: Show diff stat**

Run:

```powershell
git diff --stat
git diff --check
```

Explain that only the parser and its tests were added.

### Task 2: Preserve reported debt and build PBI current debt display

**Files:**
- Modify: `pbi_xbrl/post_quarter_capital_events.py`
- Modify: `pbi_xbrl/excel_writer_valuation_debt_detail_render.py`
- Modify: `tests/test_post_quarter_capital_events.py`

- [ ] **Step 1: Write failing pure transformation tests**

Create a reported PBI tranche frame containing the 2027 notes, reported Term Loan A, and later maturities. Assert:

```python
current = apply_pbi_current_debt_overlay(reported, event)
assert not current["tranche_name"].str.contains("2027 Notes", case=False).any()
term_loan = current[current["tranche_name"].str.contains("Term Loan A", case=False)].iloc[0]
assert term_loan["amount_principal"] == 302_000_000.0
assert term_loan["maturity_display"] == "May 18, 2031"
pd.testing.assert_frame_equal(reported, original_reported)
```

- [ ] **Step 2: Run transformation tests and verify RED**

Expected: missing `apply_pbi_current_debt_overlay`.

- [ ] **Step 3: Implement the minimal display-only transformation**

The helper must copy the input frame, remove the source-backed redeemed 6.875% 2027 notes, update or append one Term Loan A row to `$302 million`, and mark changed rows:

```python
row["source_kind"] = "PostQuarter_Capital_Events"
row["source_basis"] = "current_principal_overlay"
row["maturity_display"] = "May 18, 2031"
row["maturity_year"] = 2031
```

It must not mutate the caller frame.

- [ ] **Step 4: Run transformation tests and verify GREEN**

Expected: transformation tests pass.

- [ ] **Step 5: Write failing PBI renderer test**

Call the production writer with:

- reported Q1 `History_Q` debt/cash/net-debt inputs;
- reported `Debt_Profile`;
- reported `Debt_Tranches_Latest` containing the 2027 notes and old Term Loan A;
- PBI event source fixtures.

Assert:

```python
assert valuation_current_rows_have_no_active_2027_notes
assert valuation_term_loan_a_principal_m == pytest.approx(302.0)
assert valuation_contains("Next scheduled maturity", "March 2029")
assert valuation_contains("Automatic pro-forma net debt adjustment", "Disabled/manual")
assert history_q_values == reported_history_q_values
assert debt_profile_values == reported_debt_profile_values
assert debt_tranches_latest_values == reported_tranche_values
```

- [ ] **Step 6: Run PBI renderer test and verify RED**

Expected: Valuation still shows the reported 2027 note and old term loan.

- [ ] **Step 7: Render PBI current debt and reconciliation**

In `render_valuation_debt_detail`:

- retain the original input frame for reported references;
- use `apply_pbi_current_debt_overlay` only for the active table when a complete PBI event exists;
- retain the existing `Debt Detail (latest)` header for layout compatibility and add a visible subtitle `Current / post-quarter principal structure; reported Q1 history unchanged`;
- calculate current principal totals and near-term maturities from the transformed copy;
- label carrying debt/cash/net-debt rows as reported Q1;
- append reconciliation rows for `-$347m`, `+$150m`, `$302m`, `-$197m`, March 2029, May 18 2031, and disabled/manual net-debt adjustment;
- add SEC source-path comments.

- [ ] **Step 8: Run PBI renderer and contract tests**

Expected: new PBI test passes and existing debt-render contract remains green.

- [ ] **Step 9: Show diff stat**

Run `git diff --stat` and `git diff --check`; explain the display-only transformation and unchanged historical inputs.

### Task 3: Write support sheet and GPRE full-dilution overlay

**Files:**
- Create: `pbi_xbrl/excel_writer_post_quarter_capital_events.py`
- Modify: `pbi_xbrl/excel_writer_context.py`
- Modify: `pbi_xbrl/excel_writer_valuation_orchestrator.py`
- Modify: `pbi_xbrl/excel_writer_valuation_debt_detail_render.py`
- Modify: `tests/test_post_quarter_capital_events.py`
- Modify: `tests/test_excel_writer_refactor.py` only for orchestration contract field expectations.

- [ ] **Step 1: Write failing support-sheet and GPRE writer tests**

Assert:

```python
assert "PostQuarter_Capital_Events" in workbook.sheetnames
assert support_row["warrants_issued"] == 500_000
assert support_row["potential_common_shares_issuable_max"] == 550_000
assert valuation_contains("Post-quarter BlackRock warrant overlay")
assert valuation_contains("Post-quarter potential dilution shares (m)", 0.550)
assert valuation_formula_contains("SharesDiluted+0.55")
assert reported_history_shares_eps_are_unchanged
assert duplicate_sources_create_one_support_row
```

- [ ] **Step 2: Run GPRE tests and verify RED**

Expected: no support sheet or full-dilution rows exist.

- [ ] **Step 3: Build events in writer context**

After source-root discovery:

```python
post_quarter_capital_events = build_post_quarter_capital_events(
    ticker=profile_ticker,
    material_roots=material_roots,
    cache_roots=[cache_dir],
)
```

Pass the frame through `ValuationOrchestratorDeps`.

- [ ] **Step 4: Write the support sheet**

Use the existing generic DataFrame writer semantics in a focused wrapper:

```python
write_post_quarter_capital_events_sheet(wb, post_quarter_capital_events)
```

Place `PostQuarter_Capital_Events` after `Debt_Credit_Notes` in `desired_sheet_order`.

- [ ] **Step 5: Render GPRE full-dilution sensitivity**

Append a compact Valuation block before row 137 with:

```text
Warrants issued                              0.500m
Maximum common shares issuable              0.550m
Exercise price                              $0.01
Expiration                                  2036-06-16
Beneficial ownership limitation             19.8%
Reported diluted shares                     =SharesDiluted
Post-quarter potential dilution             0.550m
Full-dilution overlay shares                =SharesDiluted+0.550
Eq/Share @ target EV/Adj EBITDA, full dil.  =(Target_EV_AdjEBITDA*Adj_EBITDA-NetDebt)/(SharesDiluted+0.550)
```

Include the exact user-requested narrative and source comments. Do not change `SharesDiluted`, `PerShareMode`, reported EPS, or reported share history.

- [ ] **Step 6: Run GPRE tests and verify GREEN**

Expected: all GPRE parser/writer tests pass.

- [ ] **Step 7: Run focused regressions**

Run:

```powershell
C:\Users\Jibbe\Aktier\.venv\Scripts\python.exe -m pytest `
  tests/test_post_quarter_capital_events.py `
  tests/test_excel_writer_refactor.py::test_valuation_debt_detail_render_module_exposes_render_contract `
  tests/test_excel_writer_refactor.py::test_valuation_orchestrator_module_exposes_thin_wrapper_contract `
  tests/test_new_ticker_dry_run.py `
  tests/test_workbook_validation_runner.py -q
```

- [ ] **Step 8: Show diff stat**

Explain the support-sheet and GPRE overlay additions.

### Task 4: Add Summary filing freshness and current-effects views

**Files:**
- Create: `pbi_xbrl/excel_writer_summary_freshness.py`
- Create: `tests/test_summary_filing_freshness.py`
- Modify: `pbi_xbrl/excel_writer_context.py`
- Modify: `pbi_xbrl/excel_writer_summary_sheet.py`
- Modify: `pbi_xbrl/writer_types.py`
- Modify: `tests/test_excel_writer_refactor.py` only for explicit dataclass/render dependency contract updates.

- [ ] **Step 1: Write failing pure projection tests**

Build representative `hist`, `audit`, financial-statement manifest, refresh-log, and normalized event frames. Assert that the freshness projection returns one row for the current workbook ticker:

```python
freshness = build_source_filing_freshness(
    ticker="PBI",
    hist=hist,
    audit=audit,
    manifest_df=manifest_df,
    post_quarter_events=events,
    source_refresh_records=refresh_records,
)

assert freshness.iloc[0]["latest_reported_quarter"] == "2026-Q1"
assert freshness.iloc[0]["latest_reported_filing_type"] == "10-Q"
assert freshness.iloc[0]["latest_additional_filing_type"] == "8-K package / exhibits"
assert freshness.iloc[0]["latest_additional_filing_accession"] == "000119312526281893"
assert freshness.iloc[0]["latest_additional_downloaded_at"]
assert freshness.iloc[0]["used_in_workbook"] == "Yes"
assert "Valuation current Debt Detail" in freshness.iloc[0]["used_surfaces"]
assert freshness.iloc[0]["source_path_exists"] == "Yes"
```

Required generic assertions:

```python
assert len(anf_freshness) == 1
assert anf_freshness.iloc[0]["latest_additional_filing_type"] == "None newer / no model-relevant post-quarter event"
assert anf_current_effects.empty
assert gtx_current_effects.empty
```

- [ ] **Step 2: Write failing current-effects projection tests**

Assert exact PBI rows for:

- 2027 notes reported/current/change;
- Term Loan A reported/current/change;
- gross principal delta;
- unresolved current cash/net debt with no exact current numeric value;
- March 2029 next maturity;
- May 18, 2031 Term Loan A maturity.

Assert exact GPRE rows for:

- `500,000` legal warrants;
- `550,000` maximum issuable shares;
- `+0.550m` full-dilution overlay;
- `$0.01` exercise price;
- June 16, 2036 expiration;
- reported shares/EPS unchanged.

The builders must be pure projections from `post_quarter_events`; they must not parse files or calculate independent event facts.

- [ ] **Step 3: Run Summary projection tests and verify RED**

Run:

```powershell
C:\Users\Jibbe\Aktier\.venv\Scripts\python.exe -m pytest tests/test_summary_filing_freshness.py -k "projection or no_event" -v
```

Expected: import failure because `excel_writer_summary_freshness` does not exist.

- [ ] **Step 4: Implement minimal projection builders**

Create:

```python
def build_source_filing_freshness(...) -> pd.DataFrame: ...
def build_post_quarter_current_effects(...) -> pd.DataFrame: ...
```

Rules:

- derive the latest reported filing from the latest `History_Q` quarter plus existing Audit/manifest metadata;
- use the normalized event row for the latest model-relevant additional filing;
- do not select a newer filing that produced no normalized model-relevant event;
- use explicit refresh-log/manifest `downloaded_at`, with a labelled filesystem fallback only if needed;
- produce exactly one freshness row for the workbook ticker;
- produce zero current-effects rows when no normalized event exists;
- preserve `Unresolved / manual review` as text for PBI current cash/net debt;
- evaluate source-path existence at build time;
- never synthesize ANF/GTX events.

- [ ] **Step 5: Run projection tests and verify GREEN**

Expected: pure Summary projection tests pass.

- [ ] **Step 6: Write failing production Summary tests**

Call the production writer for PBI and GPRE fixtures and assert:

```python
assert summary_contains_section("Source / Filing Freshness")
assert summary_contains_section("Post-quarter / Current Effects")
assert summary_row_contains("PBI", "2026-06-25", downloaded_at)
assert summary_row_contains("Valuation current Debt Detail")
assert summary_row_contains("Cash / net debt", "Unresolved / manual review")
assert summary_row_contains("History_Q unchanged; Debt_Profile unchanged")
assert summary_row_contains("GPRE", "Full-dilution sensitivity")
assert summary_row_contains("Reported shares/EPS unchanged")
assert all(active_summary_source_paths_exist)
```

Also render ANF or a synthetic unknown ticker and assert that the freshness row says `None newer / no model-relevant post-quarter event` and no current-effects rows are present.

- [ ] **Step 7: Append structured tables to SUMMARY**

Keep the existing six-column overview renderer unchanged for existing sections. Append two dedicated tables below it:

- `Source / Filing Freshness`, with 14 columns;
- `Post-quarter / Current Effects`, with 13 columns.

Use compact headers, filters, wrapped text, sensible widths, frozen panes compatible with the existing sheet, alternating row fills, explicit date formats, and source-path comments where a full path would be too wide. The tables must receive the prebuilt frames; the renderer must not parse files or recalculate event facts.

- [ ] **Step 8: Store and pass frames once**

Add run-scoped fields:

```python
post_quarter_capital_events: Optional[pd.DataFrame]
source_filing_freshness: Optional[pd.DataFrame]
post_quarter_current_effects: Optional[pd.DataFrame]
```

Build them once in `excel_writer_context.py`, then pass the same event frame to Valuation/support and the two projection frames to Summary.

- [ ] **Step 9: Run production Summary tests and focused contracts**

Run:

```powershell
C:\Users\Jibbe\Aktier\.venv\Scripts\python.exe -m pytest `
  tests/test_summary_filing_freshness.py `
  tests/test_post_quarter_capital_events.py `
  tests/test_excel_writer_refactor.py -k "summary or post_quarter or valuation_debt_detail or valuation_orchestrator" -q
```

- [ ] **Step 10: Show diff stat**

Explain that Summary is a read-only projection over the same normalized records and that no new ingestion framework or independent estimates were added.

### Task 5: Inventory and quarantine only safe duplicate cache artifacts

**Files:**
- Create outside Git: `StockModelData\staging\source-refresh-2026-06-26\cache_cleanup\cache_inventory.csv`
- Create outside Git: `StockModelData\staging\source-refresh-2026-06-26\cache_cleanup\cache_actions.json`
- Quarantine outside Git only if candidates pass all guards.

- [ ] **Step 1: Produce exact inventory**

Record every file under `C:\Users\Jibbe\Aktier\sec_cache\PBI` with path, size, creation time, modification time, and SHA-256.

- [ ] **Step 2: Match canonical duplicates**

Match against `StockModelData\sec_cache\PBI` by SHA-256 and record the canonical counterpart.

- [ ] **Step 3: Search references**

Search:

- repository text;
- source manifests/logs/debug profiles;
- staging text outputs;
- canonical and staged `.xlsx` package XML.

- [ ] **Step 4: Quarantine only unreferenced byte-identical duplicates**

Move, do not permanently delete, only candidates satisfying:

```text
created by June 25 refresh
AND byte-identical canonical counterpart exists
AND exact accidental path has no active reference
```

Use:

```text
StockModelData\staging\source-refresh-2026-06-26\cache_cleanup\quarantine\PBI\
```

Before recursive moves, resolve and verify every source and target remains under the explicitly named roots.

- [ ] **Step 5: Leave referenced/ambiguous files**

Record retained reason per path. Never remove the whole accidental cache root.

### Task 6: Staged builds, validation, readback, and visual QA

**Files:**
- Build only:
  - `StockModelData\staging\source-refresh-2026-06-26\models\PBI_model.xlsx`
  - `StockModelData\staging\source-refresh-2026-06-26\models\GPRE_model.xlsx`

- [ ] **Step 1: Hash canonical baselines**

Hash PBI, GPRE, and ANF canonical `.xlsx` files before builds.

- [ ] **Step 2: Run targeted and PBI/GPRE/ANF regressions**

Include:

- new event tests;
- generic unknown-ticker investment-case test;
- PBI UI behavior;
- ANF investment-case tests;
- workbook validation runner;
- visible data coverage;
- valuation/hidden-value guardrails.

- [ ] **Step 3: Build PBI and GPRE staged `.xlsx`**

Use explicit canonical `--data-root`, `--cache-dir`, and `--material-root`; skip macros and history export.

- [ ] **Step 4: Validate staged workbooks**

Run workbook validation explicitly per ticker and render/style validation.

- [ ] **Step 5: Perform exact readback**

Read and report:

- PBI current active debt rows;
- PBI Term Loan A `$302m`;
- PBI next maturity March 2029;
- PBI event reconciliation;
- PBI `SUMMARY` freshness row, additional filing date/downloaded-at, usage surfaces, and source-path existence;
- PBI `SUMMARY` current-effects rows including unresolved/manual cash and net debt;
- PBI Q1 historical debt/cash/net-debt values across `History_Q`, `Debt_Profile`, and `Debt_Tranches_Latest`;
- GPRE warrants `500k`;
- GPRE maximum issuable shares `550k`;
- GPRE `+0.550m` denominator overlay and full-dilution formula/value;
- GPRE `SUMMARY` freshness row, additional filing date/downloaded-at, usage surfaces, and source-path existence;
- GPRE `SUMMARY` current-effects rows;
- GPRE reported Q1 shares and EPS.

- [ ] **Step 6: Render visual previews**

Render PBI `Valuation`, GPRE `Valuation`, both `SUMMARY` sheets, and both support-sheet event rows with `@oai/artifact-tool`. Inspect for overlap, clipping, excessive table width, unreadable wrapped text, and formula errors.

- [ ] **Step 7: Verify source paths after cleanup**

Every active source path in `PostQuarter_Capital_Events`, `SUMMARY`, comments, and workbook audit sheets must exist. Generic no-event rows are exempt because they must not claim an active source.

- [ ] **Step 8: Verify canonical models remain unchanged**

Rehash PBI, GPRE, and ANF canonical files. Do not promote in this run.

- [ ] **Step 9: Final Git verification**

Run:

```powershell
git status --short --branch
git diff --stat HEAD^
git diff --check
```

Report readiness to commit separately from canonical workbook promotion.
