# Repo Refactor Stage 8

## Scope
Stage 8 is a very narrow `GPRE` coproduct ingestion/readiness pass.

It does:
- expand `NWER` so the weekly PDF parser captures usable coproduct rows
- add `AMS 3618` as the primary new coproduct provider
- formalize new ticker-local USDA folder structure for bioenergy vs feedstuffs
- update `Basis_Proxy_Sandbox` gating around actual parsed source status

It does not:
- add a visible coproduct block to `Economics_Overlay`
- bring soybean back into scope
- implement a live `AMS 3511` provider
- broaden into DDGS / UHP / protein-mix modeling

## Source Decisions
- `NWER`
  - stays active
  - now parses:
    - `Distillers Corn Oil Feed Grade`
    - `Distillers Grain Dried 10%`
- `AMS 3618`
  - is now the primary new coproduct source for the track
  - is wired from `viewReport/3618`
  - currently focuses only on:
    - `Distillers Corn Oil`
    - `Distillers Grain Dried 10%`
- `AMS 3511`
  - remains secondary / manual only in this stage
  - not enabled as a live parsed source
- soybean
  - explicitly deferred in Stage B.1

## Local Folder Structure
Active folder:
- [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)
  - `nwer_*.pdf`
  - `nwer_*_data.csv`
  - `ams_3618_*.pdf`
  - optional `ams_3618_*_data.csv`

Secondary manual folder:
- `GPRE/USDA_feedstuffs_reports` was part of the early local structure but was later removed because it was not used in the active GPRE workflow.

Compatibility rule:
- legacy `USDA_weekly_data` and `USDA_daily_data` reads still work
- new bioenergy downloads and local manual drops should prefer `USDA_bioenergy_reports`

## Workbook-Facing Result
`Basis_Proxy_Sandbox` now uses a source-based gate:
- `NWER coproduct rows`
- `AMS 3618 coproduct rows`
- `Renewable corn oil price`
- `Distillers grains price`
- `Approximate coproduct credit`
- `Overlay activation`

Verified live saved-workbook result for Stage 8:
- `NWER coproduct rows = YES`
- `AMS 3618 coproduct rows = NO`
- `Renewable corn oil price = YES`
- `Distillers grains price = YES`
- `Approximate coproduct credit = YES`
- `Overlay activation = HOLD`

That means:
- `Economics_Overlay` row `176` stays visually empty
- no visible coproduct block is activated yet

## Operational Conclusion
Stage 8 improves the ingestion/backing path, but it is still not a visible rollout.

Current best interpretation:
- `NWER` is now genuinely useful for live coproduct backing
- `AMS 3618` is the right next source to pursue
- `AMS 3511` remains a manual secondary reference
- remote USDA landing fetches are still flaky enough that manual drop + sync remains an official operating mode
