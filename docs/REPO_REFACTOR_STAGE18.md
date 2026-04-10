# Repo Refactor Stage 18

## GPRE Historical Corn Basis Retention + Snapshot Archive

### What changed
- GPRE official corn basis now prefers retained dated GPRE plant-bid snapshots for the relevant frame or quarter, including prior and historical quarters.
- AMS basis remains fallback only when the relevant retained GPRE snapshot is unavailable or unusable.
- GPRE corn-bid storage now supports dated snapshot archival:
  - `GPRE/corn_bids/raw_snapshots/YYYY-MM-DD/grain_gpre_home.html`
  - `GPRE/corn_bids/parsed_snapshots/YYYY-MM-DD/gpre_corn_bids_snapshot.csv`
  - `GPRE/corn_bids/manifest.json`
- Legacy mutable latest files remain for compatibility, but retained parsed snapshots are now the canonical quarter-selection input.

### Why it changed
- The old behavior treated GPRE plant bids as primarily current / forward inputs.
- That was no longer acceptable once local GPRE corn-bid files proved that the source naturally supports dated delivery rows and should therefore retain quarter-specific history.
- Historical quarter rollover should preserve GPRE basis when a relevant dated snapshot exists, not silently degrade to AMS.

### Snapshot policy
- `Current QTD`
  - latest usable GPRE snapshot on or before `as_of_date`
- `Quarter-open proxy`
  - latest usable GPRE snapshot on or before quarter start, while preserving the separate established freeze/open policy where it already exists
- `Next quarter thesis`
  - latest usable GPRE snapshot on or before `as_of_date` that contains target-quarter forward delivery rows
- `Prior quarter`
  - latest usable retained GPRE snapshot on or before target quarter end
- `Historical quarter`
  - latest usable retained GPRE snapshot on or before target quarter end
  - prefer snapshots captured inside the target quarter when available

### Workbook impact
- official market wording now reflects retained historical snapshots
- quarterly/historical official corn-basis provenance can carry retained snapshot date + selection rule
- quarterly crush chart now keeps bottom quarter labels while using a separate neutral zero-reference line when the visible range crosses zero

### Guardrails
- no new sheet
- no broad refactor
- no change to non-GPRE behavior
- no automatic rewrite of quarters where a relevant retained GPRE snapshot still does not exist
