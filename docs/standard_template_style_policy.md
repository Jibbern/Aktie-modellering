# Standard Template Style Policy

`standard_template_style_policy.json` is the single maintained source for generic heatmap behavior. Python code validates and executes it; audits are generated projections and do not define policy.

## Legacy Policy

The accepted five-band palette is:

| Signal | Band | Fill |
|---|---|---|
| `<= -15%` | strong negative | `A63A00` |
| `> -15%` and `<= -5%` | negative | `D55E00` |
| `> -5%` and `< +5%` | neutral | `DDDDDD` |
| `>= +5%` and `< +15%` | positive | `9BD3F5` |
| `>= +15%` | strong positive | `2F80ED` |

The signal overlay owns only the solid fill unless a policy explicitly declares a font color. Number formats, fonts, borders, alignment, protection, formulas, values, dimensions, validation and unrelated conditional formatting remain owned by the frozen shell.

## Comparison Rules

- Quarterly QoQ uses one preceding fiscal quarter.
- Quarterly YoY uses the same fiscal quarter one year earlier.
- TTM rows compare with the prior TTM ending four fiscal quarters earlier.
- Annual YoY uses the immediately preceding fiscal year. This corrects the legacy annual-segment four-column lag defect.
- Direct delta and percentage rows classify their already calculated signal.
- Informational and deliberately neutral rows use `disabled` and keep the shell's base style.
- FCF YoY deltas and diluted-share QoQ/YoY deltas are already-calculated signals; they are classified directly rather than compared with another period.
- FCF conversion compares the accepted generic base-EBITDA formula with the prior TTM. The legacy writer's adjusted-EBITDA fallback is intentionally not reproduced because it would change the accepted formula definition.

Comparisons require the same metric definition, compatible explicit units, trusted source status and exact fiscal-period continuity. Missing, `missing_source`, `manual_review_required`, conflicting or economically non-calculable cells receive no signal overlay.

## Execution Boundary

The style planner independently reproduces the value plan, evaluates only declared generic metric/formula contracts, and emits exact `(sheet, cell)` actions plus explicit no-style decisions. The style applicator runs after value/formula verification, cannot write values, and changes only declared overlay properties. Strict post-fill validation reproduces the same plan and compares the styled workbook against an in-memory expected shell, so an extra or altered fill remains structural drift.

Module-profile resolution occurs before style planning. Policies owned by disabled modules produce no actions. The engine never consults ticker names; ANF, PBI, GPRE and future ticker differences come only from declarative module and profile-pack contracts.

Every active policy axis must resolve from an active header binding and then reappear with the same period type in the independently reproduced value plan. Unknown axes, quarterly/annual swaps, inactive target surfaces and incompatible basis/lag contracts block style planning; an unknown axis is never treated as an empty axis.

The contract also owns a `style_disabled` inventory. Every active formula target overlapping an active style-owned range must have exactly one formula selector or one exact disabled disposition with an economic reason. Disabled-module targets are removed only after profile resolution, while adding or removing an active formula target without updating this inventory fails closed.
