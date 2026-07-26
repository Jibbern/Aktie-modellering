# Standard Template Style Policy Audit

Status: **PASS**

- Authoritative contract: `docs/standard_template_style_policy.json`
- Contract digest: `2c7f1bb733a8f51e5c691c888dc08d717d92f4acbaadf96b6d86e1b3bd1d9e53`
- Numerical policies: 44
- Categorical state policies: 10
- Total policies: 54
- Exact target selectors: 124
- Explicit no-style formula targets: 41

## Profiles

| Profile | Active policies |
|---|---:|
| full_union | 54 |
| anf | 54 |
| pbi | 54 |
| gpre | 54 |
| core_only | 28 |

## Intentional Corrections

- `annual_segment_prior_year`: fiscal_year lag 1. Annual YoY compares the immediately preceding fiscal year, never a four-column quarterly lag.
- `exact_positive_boundaries`: +5% enters positive and +15% enters strong_positive. Boundary inclusivity is explicit and no longer inherited from procedural branch order.
- `direct_formula_deltas`: already-calculated FCF and diluted-share deltas are classified directly. A calculated delta is a signal, not a value to compare against another period.
- `fcf_conversion_base_ebitda`: FCF conversion uses the accepted generic base-EBITDA formula and prior-TTM comparison. Legacy preferred adjusted EBITDA when available; style planning must preserve the accepted generic formula definition.
