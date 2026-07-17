# ANF Normalized Package Source Audit

Read-only audit for the ANF shadow normalized-data package. This document does not define workbook rendering behavior.

- Generated: `2026-07-16T22:00:05+00:00`
- Legacy workbook: `C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models\ANF_model.xlsx`

| Section | Classification | Source candidates | Populated fields |
| --- | --- | --- | ---: |
| `ticker_metadata` | source-backed available, profile-backed available, legacy-workbook-derived available | SEC company_tickers; sec_cache/ANF/0001018840; ANF_model.xlsx!SUMMARY | 6 |
| `company_profile` | source-backed available, profile-backed available, legacy-workbook-derived available | ANF_model.xlsx!SUMMARY; company profile configuration; earnings release About section | 23 |
| `quarterly_financials` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!History_Q; SEC/XBRL cache; earnings release financial schedules | 400 |
| `calculation_history` | source-backed available, legacy-workbook-derived available | ANF_model.xlsx!History_Q projected as a period-keyed formula input ledger | 635 |
| `annual_financials` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!History_Q aggregated by fiscal_year; annual reports; earnings release annual schedules | 305 |
| `debt_liquidity` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!Leverage_Liquidity; ANF_model.xlsx!History_Q; ANF_model.xlsx!Slides_Debt_Profile | 12 |
| `capital_returns` | source-backed available, legacy-workbook-derived available | ANF_model.xlsx!History_Q; earnings release capital allocation text | 1 |
| `normalized_guidance` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!Guidance_Normalized; ANF_model.xlsx!Promise_Progress; earnings releases; transcripts | 787 |
| `promise_progress` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!Guidance_Normalized; ANF_model.xlsx!Promise_Progress; annual reports; earnings releases; transcripts | 146 |
| `segments` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!Slides_Segments; earnings release segment tables; presentation tables | 1121 |
| `operating_drivers` | source-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!operating_drivers_raw; transcripts; earnings presentations | 161 |
| `quarter_notes` | source-backed available, legacy-workbook-derived available, manual review required | ANF_model.xlsx!Quarter_Notes; ANF_model.xlsx!Quarter_Notes_Evidence | 58 |
| `investment_case` | source-backed available, profile-backed available, legacy-workbook-derived available, missing source, manual review required | ANF_model.xlsx!SUMMARY; ANF_model.xlsx!ANF_Investment_Case_Data | 15 |
| `valuation_outputs` | missing source | explicit normalized valuation output builder (not available in the ANF legacy adapter fixture) | 0 |
| `source_coverage` | missing source | StockModelData/tickers/ANF; StockModelData/sec_cache/ANF | 0 |
| `mapping_gaps` | none | docs/workbook_binding_map.json; normalized package | 0 |
| `manual_review_flags` | manual review required | pre-render validation; mapping gap report | 0 |

## Notes

- ANF shadow data is read from saved source/workbook artifacts only.
- Missing data remains a mapping gap or manual-review item; no generic filler text is introduced.
- Real workbook rendering is intentionally out of scope for this pass.
