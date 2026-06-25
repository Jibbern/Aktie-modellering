# Current Report Capital Structure Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse the June 2026 PBI refinancing and GPRE BlackRock warrant events, preserve reported history, and expose source-traceable current/pro-forma capital-structure overlays in staged Excel models.

**Architecture:** Add one focused parser module that scans only the known official filing families and returns one normalized row per event. Add one focused workbook module that writes `PostQuarter_Capital_Events` and supplies presentation helpers. Pass the normalized frame through the existing writer context into the Valuation debt-detail renderer; transform only the PBI display copy and add a separate GPRE full-dilution sensitivity without mutating pipeline inputs or named reported-share ranges.

**Tech Stack:** Python 3.13, pandas, BeautifulSoup, openpyxl, pytest, existing stock-model pipeline and validators.

---

## File Structure

- Create `pbi_xbrl/post_quarter_capital_events.py`: source discovery, text parsing, event normalization, duplicate collapse, and pure PBI debt-display transformation.
- Create `pbi_xbrl/excel_writer_post_quarter_capital_events.py`: support-sheet writer and compact PBI/GPRE Valuation overlay rendering.
- Modify `pbi_xbrl/excel_writer_context.py`: build the normalized event frame from scoped material/cache roots, pass it to Valuation, and order the support sheet.
- Modify `pbi_xbrl/excel_writer_valuation_orchestrator.py`: accept the event frame, write the support sheet, and expose it to the debt-detail renderer.
- Modify `pbi_xbrl/excel_writer_valuation_debt_detail_render.py`: render the PBI current debt table from a transformed copy and append ticker-specific event overlays.
- Create `tests/test_post_quarter_capital_events.py`: parser, duplicate, pure transformation, and production-writer behavior tests.
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
    "filing_date",
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
    "source_documents",
    "source_paths",
    "source_urls",
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

### Task 4: Inventory and quarantine only safe duplicate cache artifacts

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

### Task 5: Staged builds, validation, readback, and visual QA

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
- PBI Q1 historical debt/cash/net-debt values across `History_Q`, `Debt_Profile`, and `Debt_Tranches_Latest`;
- GPRE warrants `500k`;
- GPRE maximum issuable shares `550k`;
- GPRE `+0.550m` denominator overlay and full-dilution formula/value;
- GPRE reported Q1 shares and EPS.

- [ ] **Step 6: Render visual previews**

Render PBI `Valuation`, GPRE `Valuation`, and both support-sheet event rows with `@oai/artifact-tool`. Inspect for overlap, clipping, and formula errors.

- [ ] **Step 7: Verify source paths after cleanup**

Every source path in `PostQuarter_Capital_Events`, comments, and workbook audit sheets must exist.

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
