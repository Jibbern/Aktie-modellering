# New-engine shadow workflow

`python -m pbi_xbrl.new_engine` is the supported operator entrypoint for planning,
versioned shadow rendering, and immutable validation. It orchestrates existing
package, planner, frozen-shell, filler, style, formula, and validation authorities;
it does not contain economic selection logic.

Canonical promotion and rollback are intentionally not available in Architecture
Enablement 1. A shadow workbook remains an output artifact until a later supported
promotion command is implemented and reviewed.

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
