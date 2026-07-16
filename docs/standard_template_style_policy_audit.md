# Standard Template Style Policy Audit

Status: **PASS**

- Authoritative contract: `docs/standard_template_style_policy.json`
- Contract digest: `56062d31e8b474b317a8e79439a221c690b445e57c92f003a2c7b9589788b506`
- Policies: 51
- Exact target selectors: 134
- Explicit no-style formula targets: 19

## Profiles

| Profile | Active policies |
|---|---:|
| full_union | 51 |
| anf | 51 |
| pbi | 51 |
| gpre | 51 |
| core_only | 31 |

## Intentional Corrections

- `annual_segment_prior_year`: fiscal_year lag 1. Annual YoY compares the immediately preceding fiscal year, never a four-column quarterly lag.
- `exact_positive_boundaries`: +5% enters positive and +15% enters strong_positive. Boundary inclusivity is explicit and no longer inherited from procedural branch order.
- `direct_formula_deltas`: already-calculated FCF and diluted-share deltas are classified directly. A calculated delta is a signal, not a value to compare against another period.
- `fcf_conversion_base_ebitda`: FCF conversion uses the accepted generic base-EBITDA formula and prior-TTM comparison. Legacy preferred adjusted EBITDA when available; style planning must preserve the accepted generic formula definition.
