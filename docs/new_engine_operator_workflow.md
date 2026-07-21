# New-engine shadow workflow

`python -m pbi_xbrl.new_engine` is the supported operator entrypoint for planning,
versioned shadow rendering, immutable validation, canonical promotion, and rollback. It orchestrates existing
package, planner, frozen-shell, filler, style, formula, and validation authorities;
it does not contain economic selection logic.

Canonical promotion and workbook-specific rollback are available through explicit
dry-run-first commands. Neither command changes workbook economics or trusts a
serialized plan or receipt in place of fresh validation.

## Composed check tiers

`scripts/run_new_engine_checks.py` is the supported composition layer for existing
checks. It does not implement validation logic. Fast and checkpoint runs put pytest,
bytecode, and validator reports under one owned temporary directory and remove it
before returning.

Run a focused development tier against the current worktree:

```powershell
python -B scripts/run_new_engine_checks.py fast `
  --changed-from HEAD `
  --pytest-target tests/test_new_engine_cli.py `
  --pytest-target tests/test_new_engine_orchestration.py `
  --package C:\path\to\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union
```

The fast tier compiles changed Python in memory, strictly parses changed JSON, runs
both unstaged and staged `git diff --check`, and executes only the supplied pytest
targets with its cache provider disabled. `--package`, `--ticker`, and `--profile-id`
are optional as a group. When present, the runner delegates semantic and optional
expected-digest verification to `python -m pbi_xbrl.new_engine plan` inside its owned
temporary directory; the runner contains no digest logic. The optional flags are
`--expected-contract-digest`, `--expected-value-plan-digest`, and
`--expected-style-plan-digest`.

Checkpoint adds shell validation and explicitly affected deterministic audit replay.
Saved-workbook validation is included only when both its workbook location and ticker
set are declared. Checkpoint fails closed unless at least one relevant
`--cross-profile-pytest-target` is declared:

```powershell
python -B scripts/run_new_engine_checks.py checkpoint `
  --changed-from HEAD `
  --pytest-target tests/test_new_engine_cli.py `
  --cross-profile-pytest-target tests/test_workbook_module_manifest.py `
  --package C:\path\to\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union `
  --audit-generator scripts/build_standard_template_binding_audit.py `
  --saved-workbook-dir C:\path\to\saved-models `
  --saved-ticker PBI --saved-ticker GPRE --saved-ticker ANF
```

Release requires a clean repository at the exact expected HEAD. It replays all
declared deterministic audits, reproduces plans, transactionally renders one new
versioned shadow with required desktop Excel, validates that shadow immutably with a
second Excel roundtrip, writes the existing visual audit reports, and finishes with
canonical promotion dry-run. It never passes `--execute`.

```powershell
python -B scripts/run_new_engine_checks.py release `
  --changed-from HEAD^ `
  --pytest-target tests/test_new_engine_cli.py `
  --pytest-target tests/test_new_engine_orchestration.py `
  --cross-profile-pytest-target tests/test_workbook_module_manifest.py `
  --full-pytest-target tests/test_new_engine_cli.py `
  --full-pytest-target tests/test_new_engine_orchestration.py `
  --package C:\path\to\ANF_normalized_data_package.json `
  --ticker ANF --profile-id full_union `
  --output-root C:\path\to\versioned-shadows `
  --version v8 `
  --reports-dir C:\path\to\release-reports\ANF-v8 `
  --canonical-workbook "C:\path\to\Excel stock models\ANF_model.xlsx" `
  --rollback-dir C:\path\to\rollback\ANF `
  --product-approval-reference approval:ANF-v8 `
  --expected-head <EXACT_GIT_HEAD> `
  --excel-locale-id 1053
```

`--output-root` and `--reports-dir` must be outside the repository and must not
contain the requested version already. The only persistent release artifacts are
the explicit `<TICKER>_shadow_model_<version>.xlsx`, its adjacent run receipt, and
the declared report directory. Technical visual failures are blocking. A COM visual
render that is explicitly skipped is advisory and yields `PASS_WITH_ADVISORIES`;
the required Excel-native workbook validation remains blocking. Release fails before
creating outputs unless both relevant cross-profile and full-release pytest selections
are explicitly declared, and canonical promotion is always dry-run without `--execute`.

## Plan

Planning validates the normalized package and independently reproduces the value and
style plans. It writes cache/evidence artifacts but touches no workbook.

```powershell
python -m pbi_xbrl.new_engine plan `
  --package C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union `
  --run-dir C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\runs\v8-plan
```

The run directory receives `binding_plan.json`, `style_plan.json`, and
`run_receipt.json`. These files are reproducibility evidence, never execution
authority. Reproduction from the package and committed contracts still occurs at
every execution boundary.

## Render a versioned shadow

```powershell
python -m pbi_xbrl.new_engine render-shadow `
  --package C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union `
  --run-dir C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\runs\v8-render `
  --plan-receipt C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\runs\v8-plan\run_receipt.json `
  --output-root C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine `
  --version v8 `
  --excel-native required `
  --excel-locale-id 1053
```

The final file is `ANF_shadow_model_v8.xlsx`. Rendering stops if either that file
or its adjacent `.run.json` receipt already exists. The candidate is created in the
destination directory, validated before publication, normalized to inherited parent
ACLs on Windows, and published with a no-overwrite filesystem primitive.

Use `--excel-native off` only for a development shadow that does not require desktop
Excel acceptance. Required mode fails rather than skipping if Excel cannot open,
recalculate, save, reopen, or clean up its owned process.

## Validate without mutating the input

```powershell
python -m pbi_xbrl.new_engine validate `
  --package C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union `
  --run-dir C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\runs\v8-validate `
  --plan-receipt C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\runs\v8-plan\run_receipt.json `
  --workbook C:\Users\Jibbe\Aktier\StockModelData\outputs\stress_tests\ANF_new_ticker_engine\ANF_shadow_model_v8.xlsx `
  --excel-native required `
  --excel-locale-id 1053
```

Saved-only validation reads the supplied workbook without saving it. Excel-native
validation copies it to an isolated temporary directory and deletes that copy after
the roundtrip. Both modes verify that the supplied workbook SHA-256 is unchanged.

## Digest pinning

Release automation may pin independently reproduced identities with
`--expected-contract-digest`, `--expected-value-plan-digest`, and
`--expected-style-plan-digest`. A mismatch blocks before a workbook candidate is
created. Values in an old or edited receipt cannot authorize execution.

## Dry-run canonical promotion

Promotion requires an accepted shadow produced with `--excel-native required`, the
exact shadow SHA-256, a product-approval reference, the expected repository HEAD,
and a clean repository. The canonical filename must match the generic
`<TICKER>_model.xlsx` identity. Dry-run is the default and does not create a rollback
copy, promotion receipt, or canonical mutation.

```powershell
python -m pbi_xbrl.new_engine promote `
  --package C:\path\to\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union `
  --run-dir C:\path\to\runs\v8-promote `
  --plan-receipt C:\path\to\runs\v8-plan\run_receipt.json `
  --shadow-workbook C:\path\to\ANF_shadow_model_v8.xlsx `
  --shadow-receipt C:\path\to\ANF_shadow_model_v8.run.json `
  --canonical-workbook "C:\path\to\Excel stock models\ANF_model.xlsx" `
  --rollback-dir C:\path\to\rollback\ANF `
  --expected-shadow-sha256 <EXACT_SHA256> `
  --product-approval-reference <APPROVAL_REFERENCE> `
  --expected-head <EXACT_GIT_HEAD> `
  --excel-locale-id 1053
```

After an accepted dry run, repeat the same command with `--execute`. Execution
creates a byte-exact workbook-specific rollback copy and immutable rollback record
before staging the shadow in the canonical directory. The staged candidate receives
parent ACL inheritance, is validated with required desktop Excel, and atomically
replaces the canonical workbook. The promoted canonical is then validated again.
Any handled failure after replacement automatically restores the previous bytes.

## Dry-run and execute rollback

The promotion result reports both `rollback_record` and
`rollback_record_sha256`. Supply both to rollback; the independently supplied digest
prevents an edited record from authorizing a restore. Rollback is also dry-run by
default. Supply a freshly reproduced package and plan receipt for the workbook version
stored in the rollback record. The rollback source, staged candidate, and restored
canonical workbook all undergo strict, saved-workbook, and required Excel-native
validation against that context.

```powershell
python -m pbi_xbrl.new_engine rollback `
  --package C:\path\to\ANF_normalized_data_package.json `
  --ticker ANF `
  --profile-id full_union `
  --run-dir C:\path\to\runs\v8-rollback `
  --plan-receipt C:\path\to\runs\previous-version-plan\run_receipt.json `
  --canonical-workbook "C:\path\to\Excel stock models\ANF_model.xlsx" `
  --rollback-record C:\path\to\rollback\ANF\ANF_model.<operation>.rollback.json `
  --expected-rollback-record-sha256 <EXACT_RECORD_SHA256> `
  --product-approval-reference <ROLLBACK_APPROVAL_REFERENCE> `
  --expected-head <EXACT_GIT_HEAD> `
  --excel-locale-id 1053
```

Repeat with `--execute` only after the dry run passes. Rollback requires the current
canonical hash to equal the promoted hash recorded by the corresponding promotion;
it refuses to overwrite a workbook changed by another operation. Immediately before
replacement it strictly reloads and hash-verifies both the immutable rollback record
and rollback workbook. A validation or receipt failure after replacement reapplies
the exact pre-rollback promoted bytes. Its receipt is stored in `--run-dir` and is
operational evidence only. The receipt records the fresh package and plan-receipt
hashes, ticker/profile identity, and required Excel locale used for validation.
