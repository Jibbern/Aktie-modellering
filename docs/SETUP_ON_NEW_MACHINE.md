# Setup On New Machine

## What To Treat As Source Of Truth
- Treat **git + docs + saved workbooks** as the portable source of truth.
- Do **not** rely on Codex/Chat history being available on a new machine in the same form.
- The most important handoff docs right now are:
  - [README.md](/c:/Users/Jibbe/Aktier/Code/README.md)
  - [CODEBASE_MAP.md](/c:/Users/Jibbe/Aktier/Code/docs/CODEBASE_MAP.md)
  - [SEC_CACHE_REFERENCE.md](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md)
  - [BASELINE_FREEZE_2026-03-20.md](/c:/Users/Jibbe/Aktier/Code/docs/BASELINE_FREEZE_2026-03-20.md)
  - [CURRENT_PASS.md](/c:/Users/Jibbe/Aktier/Code/docs/CURRENT_PASS.md)
  - [WORKBOOK_ACCEPTANCE.md](/c:/Users/Jibbe/Aktier/Code/docs/WORKBOOK_ACCEPTANCE.md)
  - [SYSTEM_OVERVIEW.md](/c:/Users/Jibbe/Aktier/Code/docs/SYSTEM_OVERVIEW.md)

## Recommended Move-To-New-Machine Flow
1. Push current work to GitHub before switching machines.
2. On the new machine, clone the repo:
   - `git clone https://github.com/Jibbern/Aktie-modellering.git`
3. Open the repo and confirm the baseline/tag you want:
   - `git tag --list`
   - `git checkout baseline/excel-freeze-2026-03-20`
   - or stay on `main` if you want the latest state
4. Read the baseline docs first before continuing work.
5. Recreate the Python environment locally.
6. Run a small sanity check before making changes.

## Recommended Shared Data Layout
For another computer, the most practical portable data root is:

```text
StockModelData/
  sec_cache/
  tickers/
    PBI/
    GPRE/
    ANF/
  market_cache/
  writer_cache/
  basis_proxy/
  outputs/
    Excel stock models/
  render_checks/
  validation_reports/
  logs/
```

Keep that folder local on the machine that is building models. Do not run the live
working `StockModelData` directly from OneDrive; OneDrive should carry a portable
snapshot zip instead, for example:

```text
C:\Users\Jibbe\OneDrive\AktierBackup\StockModelData_snapshot.zip
```

Configure the local root once, then run from the code checkout without repeating
`--data-root`:

```powershell
.\.venv\Scripts\python.exe Code\stock_models.py data config set-root C:\Users\Jibbe\Aktier\StockModelData
.\.venv\Scripts\python.exe Code\stock_models.py data config show
.\.venv\Scripts\python.exe Code\stock_models.py --ticker PBI
.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE
.\.venv\Scripts\python.exe Code\stock_models.py --ticker ANF
```

This keeps the code repository separate from large/cache-heavy runtime data. It also
avoids relying on `C:\Users\Jibbe\Aktier` being the same absolute path on every machine.

The data-root priority is:
1. explicit `--data-root`
2. `STOCK_MODEL_DATA_ROOT`
3. `stock_model_config.json` / `.stock_model_config.json`
4. auto-detected local `StockModelData`
5. legacy folders

Use `--material-root C:\Path\To\StockModelData\tickers\GPRE` only when the ticker material
folder is not under the same root passed to `--data-root`.

Useful portability commands:

```powershell
.\.venv\Scripts\python.exe Code\stock_models.py data validate-root
.\.venv\Scripts\python.exe Code\stock_models.py data snapshot --out C:\Users\Jibbe\OneDrive\AktierBackup\StockModelData_snapshot.zip
.\.venv\Scripts\python.exe Code\stock_models.py data restore --snapshot C:\Users\Jibbe\OneDrive\AktierBackup\StockModelData_snapshot.zip --data-root C:\Users\Jibbe\Aktier\StockModelData_restore_test
.\.venv\Scripts\python.exe Code\stock_models.py data cleanup-old --dry-run
.\.venv\Scripts\python.exe Code\stock_models.py data cleanup-old --archive
```

Keep `OldDataArchive_*` for a while after migration. Permanent deletion should be a
separate, explicit decision after snapshot, restore-test, rebuilds, and validation pass.

## Python Environment
- The repo root now includes [requirements.txt](/c:/Users/Jibbe/Aktier/Code/requirements.txt).
- Current practical setup is:
  - create a Python virtual environment outside the repo if you want the repo to stay easy to zip/share
  - install from `requirements.txt`
- Current working local pattern on this machine:
  - repo: [Code](/c:/Users/Jibbe/Aktier/Code)
  - external venv: `C:\Users\Jibbe\Aktier\.venv_code`
  - base Python: `C:\Users\Jibbe\Python313\python.exe`
- Example setup:
  - `C:\Users\Jibbe\Python313\python.exe -m venv C:\Users\Jibbe\Aktier\.venv_code`
  - `C:\Users\Jibbe\Aktier\.venv_code\Scripts\python.exe -m pip install -r C:\Users\Jibbe\Aktier\Code\requirements.txt`
- If someone prefers a repo-local venv later, that still works, but it is less convenient for zipping/sharing the repo.

## Minimal Sanity Check On A New Machine
- Confirm git state:
  - `git status`
- Confirm the baseline/tag you expect:
  - `git log --oneline --decorate -n 5`
- Open the current delivered workbooks if available and sanity-check:
  - `Quarter_Notes_UI`
  - `SUMMARY`
  - `Valuation`
- If the Python environment is working, run a small targeted test set before new edits.
- If using a shared data root, run a fast cache smoke test:
  - `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --market-only --profile-timings`
  - On an unchanged warm cache this should usually be sub-second to a few seconds, not a full market reparse.

## How To Resume Work With Codex
- Start a fresh Codex thread on the new machine.
- Point it first to:
  - [README.md](/c:/Users/Jibbe/Aktier/Code/README.md)
  - [CODEBASE_MAP.md](/c:/Users/Jibbe/Aktier/Code/docs/CODEBASE_MAP.md)
  - [SEC_CACHE_REFERENCE.md](/c:/Users/Jibbe/Aktier/Code/docs/SEC_CACHE_REFERENCE.md)
  - [BASELINE_FREEZE_2026-03-20.md](/c:/Users/Jibbe/Aktier/Code/docs/BASELINE_FREEZE_2026-03-20.md)
  - [CURRENT_PASS.md](/c:/Users/Jibbe/Aktier/Code/docs/CURRENT_PASS.md)
  - [WORKBOOK_ACCEPTANCE.md](/c:/Users/Jibbe/Aktier/Code/docs/WORKBOOK_ACCEPTANCE.md)
- That is enough for a safe restart even if the old chat thread is unavailable.

## What Needs To Be Captured Before Switching Machines
- If a thread contains a real new decision, baseline change, accepted output change, or watchlist update, write it into docs before switching machines.
- Good default:
  - `CURRENT_PASS.md` for latest state
  - `BASELINE_FREEZE_2026-03-20.md` only for true freeze-point updates
  - `WORKBOOK_ACCEPTANCE.md` when acceptance rules/examples change

## What Does Not Need To Be Preserved Separately
- Ordinary back-and-forth discussion that did not change:
  - workbook truth
  - acceptance policy
  - runtime baseline
  - watchlist
- If those four are current in docs, the chat itself is not critical.
