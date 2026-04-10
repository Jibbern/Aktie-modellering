# GPRE Coproduct Stage B.5: History + First Coproduct Chart

## Summary
Stage B.5 is a narrow writer/readback pass on top of Stage B.4.

It does not:
- add new visible coproduct rows beyond the existing block at `Economics_Overlay`
  rows `176:179`
- change the primary activation rule away from `NWER`
- broaden into soybean, DDGS/UHP expansion, or new panels

It does:
- add quarterly coproduct history to `Basis_Proxy_Sandbox`
- add a first visible chart for `Approximate coproduct credit`

## Verified Workbook Changes
- `Basis_Proxy_Sandbox`
  - adds `Coproduct quarterly history`
  - stores quarterly history for:
    - `Renewable corn oil price`
    - `Distillers grains price`
    - `Approximate coproduct credit`
    - `Resolved source mode`
- `Economics_Overlay`
  - keeps rows `176:179` unchanged
  - adds a chart title at row `181`:
    - `Approximate coproduct credit (quarterly history)`
  - adds a single-series line chart below the title

## Design Choice
The first visible chart is only `Approximate coproduct credit`.

Reason:
- it is a single-unit economics series
- it connects directly to the visible coproduct block
- it avoids a first-pass chart that mixes corn-oil and DDGS price series with
  different interpretive roles

Corn-oil and DDGS quarterly history remain visible only in the sandbox backing
section for now.

## Source Policy
- `NWER`
  - primary live activation source
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback / backfill source
- `3511`
  - deferred / manual

Stage B.5 does not change those roles.

## Acceptance
Stage B.5 is considered landed when:
- `Basis_Proxy_Sandbox` contains `Coproduct quarterly history`
- the history table has real quarterly rows and non-blank coproduct credit values
- `Economics_Overlay` contains a new `Approximate coproduct credit (quarterly history)` chart
- the current visible block at rows `176:179` still renders unchanged
