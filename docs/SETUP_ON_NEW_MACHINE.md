# Setup On New Machine

## What To Treat As Source Of Truth
- Treat **git + docs + saved workbooks** as the portable source of truth.
- Do **not** rely on Codex/Chat history being available on a new machine in the same form.
- The most important handoff docs right now are:
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

## Python Environment
- There is currently **no pinned dependency file** in the repo root.
- That means the environment is still partly knowledge-based rather than fully automated.
- Practical default:
  - create a local `.venv`
  - install dependencies as needed from import/test failures
- If we want machine setup to be more repeatable later, the best next step would be adding a proper `requirements.txt` or `pyproject.toml`.

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

## How To Resume Work With Codex
- Start a fresh Codex thread on the new machine.
- Point it first to:
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
