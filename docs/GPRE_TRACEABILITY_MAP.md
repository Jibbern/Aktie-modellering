# GPRE Traceability Map

This map documents the verified path from local cache/material inputs into pipeline artifacts and workbook surfaces.

## Verified chain

| Source root | Materialized artifact | Stage / pipeline artifact | Consuming code | Workbook surface | Verification |
|---|---|---|---|---|---|
| [`sec_cache/GPRE/<cik>/submissions.json`](/c:/Users/Jibbe/Aktier/sec_cache/GPRE) and filing packages | `materials/sec_primary`, `materials/sec_exhibits`, `materials/sec_xbrl` via [`sec_ingest.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_ingest.py) | `hist`, `audit`, `gaap_history_bundle`, debt artifacts via [`run_pipeline_impl()`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_orchestration.py) | pipeline + writer core | `SUMMARY`, `Valuation`, debt sheets, `SEC_Audit_Log`, `Info_Log` | Verified directly in code |
| [`sec_cache/GPRE/materials/source_material_manifest.json`](/c:/Users/Jibbe/Aktier/sec_cache/GPRE/materials/source_material_manifest.json) and local GPRE material roots | local materials refreshed by [`source_material_refresh.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/source_material_refresh.py) | `local_non_gaap_fallback`, `doc_intel_bundle` keyed by local material signature in [`pipeline_orchestration.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_orchestration.py) | `build_doc_intel_outputs()` and writer-local fallback branches | `Quarter_Notes_UI`, `Promise_Tracker`, `Promise_Progress_UI`, visible evidence sections | Verified directly in code |
| [`sec_cache/GPRE/pipeline_stage_cache/doc_intel_bundle_*.pkl`](/c:/Users/Jibbe/Aktier/sec_cache/GPRE/pipeline_stage_cache) | serialized stage cache bundle | `quarter_notes`, `promises`, `promise_progress`, `non_gaap_cred` | `run_pipeline_impl()` -> `WorkbookInputs` -> writer callbacks | `Quarter_Notes_UI`, `Quarter_Notes`, `Promise_Tracker`, `Promise_Progress_UI` | Verified directly in code |
| [`sec_cache/market_data/raw/*`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/raw) plus [`GPRE/USDA_*`](/c:/Users/Jibbe/Aktier/GPRE) working folders | parsed parquet and [`parsed/exports/GPRE.parquet`](/c:/Users/Jibbe/Aktier/sec_cache/market_data/parsed/exports/GPRE.parquet) via market-data sync | normalized export rows and GPRE snapshot/model inputs | [`load_market_export_rows()`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/service.py), [`build_gpre_basis_proxy_model()`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/service.py), [`excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py), [`excel_writer_economics_overlay.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_economics_overlay.py) | `Economics_Overlay`, `Basis_Proxy_Sandbox`, parts of `Operating_Drivers` | Verified directly in code |

## Verified writer bypasses

These branches still read directly from `cache_dir` / `material_roots` rather than only through stage cache artifacts.

- [`excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py): quarter-notes rescue branches that scan `materials/` and ticker cache roots
- [`excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py): some `Economics_Overlay` / `Operating_Drivers` local helper branches that look for GPRE/PBI local docs
- [`excel_writer_economics_overlay.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_economics_overlay.py): the extracted GPRE basis/proxy support surface still consumes workbook-local callbacks and does not fully remove writer-local fallback behavior

These links are verified in code, but they bypass the cleaner `stage cache -> WorkbookInputs` path and should be documented whenever workbook behavior is audited.

## Gaps / likely-but-not-fully-audited

- Some GPRE overlay rescue/commentary branches appear to depend on writer-local scans of `materials/` and `cache_dir`, but I did not fully audit every branch end-to-end.
- The map above is complete for the main visible GPRE paths, not for every peripheral fallback.
