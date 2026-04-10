# GPRE Coproduct Stage B.4: Provenance + Manual Workflow Clarity

## Summary
Stage B.4 is a narrow clarity/provenance/manual-workflow pass on top of Stage B.3.
It does not add new visible coproduct rows and does not change the activation rule
away from `NWER`.

Ground truth at the start of Stage B.4:
- the visible `Economics_Overlay` coproduct block already exists at rows `176:179`
- `NWER` is still sufficient for visible activation
- `AMS 3618` is working as a manual secondary source
- the current workbook can legitimately have:
  - `NWER` as the activation basis
  - `AMS 3618` as the current resolved workbook source for the visible price rows

The Stage B.4 goal is to make that distinction explicit in both the workbook and the
docs, while keeping the product shape unchanged.

## Verified Workbook Changes
- `Economics_Overlay`
  - row `176` is now labeled `Coproduct economics`
  - rows `177:179` remain:
    - `Renewable corn oil price`
    - `Distillers grains price`
    - `Approximate coproduct credit`
  - the credit-row source note now states that:
    - visible activation is `NWER`-sufficient
    - resolved price legs may come from `NWER` or `AMS 3618`
- `Basis_Proxy_Sandbox`
  - keeps `Coproduct source gate`
  - adds a compact `Source provenance` block with:
    - `Primary live activation source = NWER`
    - `Secondary corroborating source = AMS 3618`
    - `Current resolved workbook source = workbook-dependent classification from the linked visible price-row source strings`

## Manual Workflow
Primary manual folder:
- [`GPRE/USDA_bioenergy_reports`](/c:/Users/Jibbe/Aktier/GPRE/USDA_bioenergy_reports)

Legacy folders still tolerated:
- [`GPRE/USDA_weekly_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_weekly_data)
- [`GPRE/USDA_daily_data`](/c:/Users/Jibbe/Aktier/GPRE/USDA_daily_data)

Recommended filenames:
- `nwer_*.pdf`
- `ams_3618_*.pdf`

Supported manual-file behavior:
- dated names work
- undated names like `ams_3618_00183.pdf` also work
- no rename is required for the normal manual-drop path

Official workflow:
1. place manual PDFs in `USDA_bioenergy_reports`
2. run `.\.venv\Scripts\python.exe Code\stock_models.py --ticker GPRE --refresh-market-data`
3. verify `GPRE.parquet` contains `nwer_pdf` and/or `ams_3618_pdf`
4. verify the workbook through `Coproduct source gate` and `Source provenance`

## Locked Interpretation
- `NWER`
  - primary live activation source
- `AMS 3618`
  - secondary
  - corroborating
  - manual fallback / backfill source
- `3511`
  - deferred / manual

Important boundary:
- `AMS 3618` may be `YES`
- `AMS 3618` may be the current resolved workbook source
- `AMS 3618` still does **not** become a blocker again
