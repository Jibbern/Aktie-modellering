# GPRE Derivative and Hedge Diagnostics

## Purpose
This note documents the GPRE derivative and hedge diagnostic flow so future
changes keep the accounting boundaries clear.

The derivative surfaces are memo/audit features. They do not overwrite reported
actuals, adjusted EBITDA, valuation math, the production GPRE crush proxy, or
futures sandbox status.

## Accounting Boundaries
- P&L derivative impact affects reported earnings because it is booked in
  revenue, COGS, other income/expense, or another income-statement line.
- OCI movement is the quarter's new unrealized cash-flow hedge movement outside
  earnings.
- AOCI is accumulated OCI in equity.
- AOCI reclassification is the later movement from AOCI into the relevant P&L
  line when the hedged transaction affects earnings.
- Net derivative asset/liability is a period-end balance-sheet snapshot. It is
  not a current-quarter margin impact.
- Open hedge notional is position size and direction. It is not fair value, OCI,
  AOCI, or P&L.

The code should keep those concepts separate. In particular, do not include OCI,
AOCI ending balance, or net derivative asset/liability in current-quarter margin
or valuation calculations.

## Code Flow
1. [`pbi_xbrl/derivative_oci_bridge.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/derivative_oci_bridge.py)
   - reads local GPRE 10-Q / 10-K filing tables,
   - extracts income-statement derivative P&L components,
   - extracts OCI/AOCI and balance-sheet derivative exposure,
   - parses open notional exposure while preserving the company's scale,
   - derives Q4 flow values from annual-minus-Q1-Q3 when needed.
2. [`pbi_xbrl/excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py)
   - writes `Derivative_OCI_Bridge` as the accounting source sheet,
   - appends open-position exposure and margin-context diagnostics,
   - gates the sheet to GPRE or future explicit derivative/OCI profiles.
3. [`pbi_xbrl/derivative_crush_tests.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/derivative_crush_tests.py)
   - joins derivative bridge rows with operating-driver gallons and the GPRE
     basis-model quarterly frame,
   - tests market/proxy crush lenses with and without derivative P&L,
   - uses OCI/AOCI/net derivative balances only as lead/lag or exposure signals.
4. `excel_writer_context.py`
   - writes `Derivative_Crush_Tests` after `Basis_Proxy_Sandbox`, because that
     diagnostic sheet needs the basis model's quarterly frame.

## Workbook Surfaces
### Derivative_OCI_Bridge
This sheet is the accounting/audit source. It contains:
- historical derivative P&L bridge rows,
- OCI/AOCI and net derivative asset/liability fields,
- `Open Derivative Position Exposure`,
- derivative P&L per gallon diagnostics,
- deferred/balance-sheet exposure per gallon diagnostics,
- hedge exposure by margin bucket.

The open exposure table preserves GPRE notional amounts as disclosed in
thousands of units. The `Scale` column is part of the visible contract and should
not be removed or silently normalized away.

### Derivative_Crush_Tests
This sheet is the modeling diagnostic surface. It contains:
- `Model Accuracy Summary`,
- `Reported Margin Reconciliation: Market Proxy vs Derivative-Adjusted`,
- quarterly derivative impact on reported margin equivalent,
- lead/lag tests for OCI/AOCI/net derivative balances,
- hedge slippage diagnostics,
- open hedge exposure by margin bucket,
- residual analysis after derivative adjustment.

The sheet compares two baseline lenses when available:
- `Approximate market crush`,
- `GPRE crush proxy`.

The diagnostic adjustment is:

```text
derivative-adjusted margin / gal
  = baseline margin / gal + total derivative P&L / gal
```

Positive error improvement means derivative P&L helped explain reported margin
for that quarter. Negative improvement means it worsened the baseline fit. The
result is a diagnostic, not a promotion decision.

## Denominator Policy
Per-gallon diagnostics use the same denominator policy across the derivative
workbook surfaces:
1. prefer `ethanol_gallons_produced`,
2. fallback to `ethanol_gallons_sold`,
3. leave `$ / gal` blank with a visible note if no denominator is available.

For GPRE Q1 2026, the expected diagnostic denominator is about `174.196m`
ethanol gallons produced. Total derivative P&L of about `-$12.594m` therefore
maps to about `-$0.0723/gal`.

## Non-Goals
- Do not use derivative diagnostics to overwrite reported earnings or reported
  crush margin.
- Do not add OCI, AOCI, or net derivative balances to current-quarter P&L.
- Do not infer unrealized P&L if the filing does not disclose it.
- Do not calculate commodity-level derivative P&L or fair value unless the
  company explicitly discloses it.
- Do not calculate hedge coverage ratios until compatible physical-volume
  denominators exist on the same scale as notional exposure.

## Validation Checklist
Run targeted validation after changes to this area:

```powershell
.\.venv\Scripts\python.exe -m py_compile `
  pbi_xbrl\derivative_oci_bridge.py `
  pbi_xbrl\derivative_crush_tests.py `
  pbi_xbrl\excel_writer_context.py

.\.venv\Scripts\python.exe -m pytest tests\test_derivative_oci_bridge.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_derivative_crush_tests.py -q
```

When workbook layout or sheet-gating changes, also run the targeted
`tests/test_excel_writer_refactor.py` readback slices for GPRE and PBI.
