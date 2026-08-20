# Codebase Map

## Purpose
This map explains which modules own each major stage of the runtime so the handoff between ingest, pipeline assembly, workbook rendering, and validation is easy to follow.

## Select The Lifecycle Before The Files

The repository currently contains three legitimate but different architecture
surfaces. Do not combine their owners:

| Surface | Lifecycle | Current ownership | Must not be treated as |
| --- | --- | --- | --- |
| Legacy workbook production | active / compatibility | `stock_models.py`, pipeline/orchestration, legacy writers, saved-workbook readback | canonical source-native economic authority |
| Normalized/frozen-shell engine | transition | normalized schema/validation, binding planner, frozen shell, value/style applicators | universal production or the Promise Progress workbook bridge |
| Source-native longitudinal products | active for accepted in-memory scope | longitudinal contracts, source adapter, sector packs, ticker profiles, typed products | already wired to normalized/workbook production |

Machine-readable routing:

- [`SYSTEM_LIFECYCLE_REGISTRY.json`](SYSTEM_LIFECYCLE_REGISTRY.json) — lifecycle,
  authority and production status;
- [`OWNERSHIP_REGISTRY.json`](OWNERSHIP_REGISTRY.json) — canonical and parallel
  owners;
- [`EXTENSION_POINTS.md`](EXTENSION_POINTS.md) — task-oriented public extension
  surfaces;
- [`CHANGE_IMPACT_REGISTRY.json`](CHANGE_IMPACT_REGISTRY.json) — likely change
  blast radius;
- [`APPROVAL_GATES.json`](APPROVAL_GATES.json) — mandatory stop points.

These artifacts are routing metadata. Live code and the referenced closed contracts
remain behavioral authority.

## Stage Ownership

### 1. SEC ingest and cache seeding
- [`pbi_xbrl/source_acquisition.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/source_acquisition.py)
  - Canonical transaction owner for verified source bytes: stage, validate, hash, flush, atomically replace, and return published content identity.
  - Source-specific discovery and policy remain with the callers; no caller should publish directly to the canonical final path.
- [`pbi_xbrl/sec_ingest.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_ingest.py)
  - Downloads SEC filing packages into `sec_cache`.
  - Materializes statement-like 10-Q / 10-K documents into `PBI/financial_statement` and `GPRE/financial_statement`.
- [`pbi_xbrl/sec_xbrl.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_xbrl.py)
  - SEC HTTP client and companyfacts/submissions access.

### 2. Runtime cache layout and environment discovery
- [`pbi_xbrl/cache_semantics.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/cache_semantics.py)
  - Owns `contract:semantic-cache-identity@1`, deterministic canonical JSON/SHA-256 identity primitives, content identities, and the shared semantic-version registry.
  - Cache-specific producers continue to own the business meaning and relevant inputs of their payloads.
- [`pbi_xbrl/cache_layout.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/cache_layout.py)
  - Resolves canonical ticker cache roots and shared cache roots.
- [`pbi_xbrl/pipeline_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_runtime.py)
  - Stage-cache helpers, runtime signatures, and root resolution.
- [`stock_models.py`](/c:/Users/Jibbe/Aktier/Code/stock_models.py)
  - Owns operator-facing path flags such as `--data-root`, `--material-root`, `--cache-dir`, and output-path defaults.
  - `--data-root` is the portable layout switch for shared data roots that contain both `sec_cache` and ticker material folders.

### 3. Pipeline assembly and derived dataframe creation
- [`pbi_xbrl/pipeline_orchestration.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_orchestration.py)
  - Builds the expensive intermediate bundles.
  - Owns stage-cache persistence for GAAP history, debt outputs, local non-GAAP fallback, `doc_intel`, and company overview.
- [`pbi_xbrl/pipeline.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline.py)
  - Thin orchestration-facing API that bridges the pipeline bundle to workbook inputs.
- [`pbi_xbrl/pipeline_types.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_types.py)
  - Dataclasses for config, artifacts, and workbook handoff inputs.

### 4. Source interpretation and evidence shaping
- [`pbi_xbrl/inline_xbrl_text.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/inline_xbrl_text.py)
  - Owns `contract:inline-xbrl-fact-text@1` and deterministic `continuedAt` reconstruction before downstream semantic parsing; it does not own all XBRL interpretation.
- [`pbi_xbrl/debt_rate_semantics.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/debt_rate_semantics.py)
  - Owns `contract:debt-rate-semantic-ownership@1`: role-aware debt-rate identity, source authority, corroboration, conflict failure, and lineage.
- [`pbi_xbrl/debt_source_registry.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/debt_source_registry.py)
  - Owns semantic facility/debt-row duplicate resolution before physical row order; rate-fact identity remains a distinct layer owned by `debt_rate_semantics.py`.
- [`pbi_xbrl/doc_intel.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/doc_intel.py)
  - Builds quarter notes, promises, promise-progress evidence, and non-GAAP credibility outputs from documents.
- [`pbi_xbrl/derivative_oci_bridge.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/derivative_oci_bridge.py)
  - Extracts GPRE-style derivative P&L, OCI/AOCI, net derivative exposure, and open hedge notional into memo/audit tables.
  - Keeps income-statement impact separate from OCI and balance-sheet exposure so downstream sheets cannot accidentally treat deferred hedge movement as current-quarter margin.
- [`pbi_xbrl/derivative_crush_tests.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/derivative_crush_tests.py)
  - Builds GPRE-only diagnostic tables that test whether derivative P&L improves reported-margin explanation versus market/proxy crush lenses.
  - Does not feed production `Economics_Overlay`, valuation, reported actuals, or the GPRE crush proxy.
- [`pbi_xbrl/source_material_refresh.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/source_material_refresh.py)
  - Local source-material discovery, normalization, manifest rebuild, and coverage reporting.
- [`pbi_xbrl/summary_overview.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/summary_overview.py)
  - Topic-aware `SUMMARY` source ranking and visible summary text selection.

### 4a. Longitudinal company-memory contract (sidecar foundation)
- [`docs/longitudinal_company_memory_contract.md`](/c:/Users/Jibbe/Aktier/Code/docs/longitudinal_company_memory_contract.md)
  - Owns the v1 identity, catalog, fiscal-period, reconciliation, promise-history, sidecar, and product-consumer boundaries.
- [`docs/longitudinal_company_memory.schema.json`](/c:/Users/Jibbe/Aktier/Code/docs/longitudinal_company_memory.schema.json)
  - Closed Draft 2020-12 authority for one lossless company-scoped memory sidecar.
- [`pbi_xbrl/longitudinal_memory/`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory)
  - Workbook-independent pure domain types, deterministic readable identities, semantic validation, assertion-specific reconciliation, safe QoQ/YoY change derivation, and deterministic JSON serialization.
  - Reuses the strict JSON loader and existing Needs Review issue-ledger projection; it does not reuse lossy latest-row selection.
  - Remains a validated sidecar linked non-authoritatively to the normalized package. No workbook binding, formula, writer, template, or normalized-package producer reads it.
  - It now has one accepted source-native product consumer: `PromiseProgressProduct@1`. That consumer is in-memory only and does not imply workbook integration.

### 4b. Source-native longitudinal-memory adapter (isolated C2 path)
- [`docs/longitudinal_memory_source_adapter_contract.md`](/c:/Users/Jibbe/Aktier/Code/docs/longitudinal_memory_source_adapter_contract.md)
  - Defines the root-injected, hash-verified, locator-replay boundary that feeds the unchanged C1 sidecar contract.
- [`docs/longitudinal_memory_source_adapter_input.schema.json`](/c:/Users/Jibbe/Aktier/Code/docs/longitudinal_memory_source_adapter_input.schema.json)
  - Closed Draft 2020-12 authority for declared source documents, reviewed links, explicit fiscal periods, complete fiscal-claim membership, exact reviewed horizon authorities, format locators, extraction assertions, and reviewed model inputs.
- [`pbi_xbrl/longitudinal_memory/source_adapter/`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/source_adapter)
  - Generic deterministic discovery from immutable hash-verified byte snapshots, closed document-role checks, HTML/PDF/XLSX/TXT locator and temporal-evidence replay, source-text-derived fiscal claims, complete same-period evidence closure, exact reviewed-horizon reconciliation, temporary candidate mapping, C1 projection, and in-memory sidecar orchestration.
  - Requires injected source, reviewed-model, sector-pack, and ticker-profile boundaries; it has no production writer or source-download path.
- [`pbi_xbrl/longitudinal_memory/sector_packs/retail.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/sector_packs/retail.py)
  - Owns retail metric, unit, dimension, lossless guidance parsing, source eligibility, comparable-sales trend pairing, and signed net-openings semantics.
- [`pbi_xbrl/longitudinal_memory/ticker_profiles/anf.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/ticker_profiles/anf.py)
  - Validates only the declarative company/publisher aliases, activated metrics, calendar hints, and reviewed source/event links supplied by the ANF source set.
- [`tests/fixtures/longitudinal_memory/anf_source_set.v1.json`](/c:/Users/Jibbe/Aktier/Code/tests/fixtures/longitudinal_memory/anf_source_set.v1.json)
  - Pins the eight external source paths, exact SHA-256 hashes, publication metadata, locators, declarative mappings, complete fiscal evidence groups, the exact reviewed FY2026 horizon, reviewed links, and proof periods without copying source files into Git.

This C2 path remains disconnected from normalized-package production and every
workbook product, binding, formula, template, and writer.

### 4c. PBI source-native generalization proof (isolated C3 path)
- [`docs/longitudinal_memory_pbi_source_proof_contract.md`](/c:/Users/Jibbe/Aktier/Code/docs/longitudinal_memory_pbi_source_proof_contract.md)
  - Defines the bounded 18-source PBI proof, source-authority boundary, calendar-year semantics, cost-savings Promise history, definition break and explicit exclusions.
- [`pbi_xbrl/longitudinal_memory/source_adapter/inline_xbrl.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/source_adapter/inline_xbrl.py)
  - Replays numeric Inline-XBRL facts with exact concept, context, dimensions, unit, decimals, scale, continuation and DOM identity from immutable SEC bytes.
- [`pbi_xbrl/longitudinal_memory/source_adapter/reviewed_metadata.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/source_adapter/reviewed_metadata.py)
  - Verifies reviewed transcript-metadata revisions against the immutable raw transcript; metadata is a locator/index and never independent economics.
- [`pbi_xbrl/longitudinal_memory/sector_packs/business_services.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/sector_packs/business_services.py)
  - Owns reusable mailing/business-services metrics, definitions, units, segment dimensions, value parsers, margin derivation and cost-savings evidence distinctions.
- [`pbi_xbrl/longitudinal_memory/ticker_profiles/pbi.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/longitudinal_memory/ticker_profiles/pbi.py)
  - Validates the declarative PBI company/CIK/publisher identities, SendTech and Presort aliases, activated registry references, reviewed links and calendar-year rule.
- [`tests/fixtures/longitudinal_memory/pbi_source_set.v1.json`](/c:/Users/Jibbe/Aktier/Code/tests/fixtures/longitudinal_memory/pbi_source_set.v1.json)
  - Pins the 18 external source paths, full hashes, source roles, locators, reviewed transcript metadata, exact calendar periods, semantic bindings and expected Needs Review policies.

The C3 path has no production writer, workbook consumer, normalized-package binding,
source acquisition or ModelInterpretation. It exists only as a source-native
generalization proof over the unchanged C1 sidecar.

### 4d. Source-native Promise Progress product (active, workbook not wired)
- [`docs/promise_progress_product_contract.md`](promise_progress_product_contract.md)
  - Owns the versioned product boundary, four distinct blocks, typed Actual,
    Progress and Status semantics, dates, ordering, display text, lineage and parity.
- [`docs/promise_progress_shadow_projection.schema.json`](promise_progress_shadow_projection.schema.json)
  - Closed shadow-row/field and lineage contract.
- [`pbi_xbrl/longitudinal_memory/promise_progress_projection.py`](../pbi_xbrl/longitudinal_memory/promise_progress_projection.py)
  - Builds and deterministically serializes one immutable
    `PromiseProgressProduct@1` and its shadow/parity reports.
- [`tests/test_promise_progress_source_native_projection.py`](../tests/test_promise_progress_source_native_projection.py)
  - Semantic, negative, mutation, parity, schema and determinism acceptance.

This product is active and accepted. The source-native -> workbook bridge is
`target_not_wired`; the legacy Promise Progress writer is a compatibility/UI owner,
not a consumer of this projection.

### 4e. Source-native Summary and BS_Segments product (golden accepted, workbook not wired)
- [`pbi_xbrl/longitudinal_memory/summary_bs_products.py`](../pbi_xbrl/longitudinal_memory/summary_bs_products.py) and
  [`pbi_xbrl/longitudinal_memory/ticker_profiles/anf_summary_bs_foundation.py`](../pbi_xbrl/longitudinal_memory/ticker_profiles/anf_summary_bs_foundation.py)
  - Own the accepted 452-field Summary/BS economic product, derivations,
    availability and typed lineage.
- [`pbi_xbrl/longitudinal_memory/summary_bs_workbook_projection.py`](../pbi_xbrl/longitudinal_memory/summary_bs_workbook_projection.py) and
  [`pbi_xbrl/longitudinal_memory/summary_bs_workbook_materialization.py`](../pbi_xbrl/longitudinal_memory/summary_bs_workbook_materialization.py)
  - Own the immutable 452-field binding/presentation plan and targeted lossless
    OOXML scratch materialization. They do not select economics.
- [`tests/fixtures/summary_bs/anf_summary_bs_golden_manifest.v1.json`](../tests/fixtures/summary_bs/anf_summary_bs_golden_manifest.v1.json)
  - Registers golden `summary-bs-source-native:anf@1.0.0`, its product/shadow,
    binding, workbook and acceptance identities.

The source-native product and replayable golden are accepted. The production workbook
bridge remains `target_not_wired`, `production_default=false`; artifact-tool is
read/inspection/render only for this bridge.

### 4f. Source-native Valuation product (golden accepted, workbook not wired)
- [`pbi_xbrl/longitudinal_memory/valuation_source_native_projection.py`](../pbi_xbrl/longitudinal_memory/valuation_source_native_projection.py)
  - Owns the immutable Valuation value/formula/name/layout plan, accepted historical
    consumer corrections, canonical Investment Case dependency closure, and the
    Valuation-specific calculation-metadata policy.
- [`pbi_xbrl/longitudinal_memory/formula_aware_workbook_materialization.py`](../pbi_xbrl/longitudinal_memory/formula_aware_workbook_materialization.py)
  - Applies caller-owned lossless formula/value/name/layout mutations and the bounded
    pre-open `forceFullCalc` finalization. It performs no source selection or economics.
- [`pbi_xbrl/longitudinal_memory/valuation_golden.py`](../pbi_xbrl/longitudinal_memory/valuation_golden.py) and
  [`tests/fixtures/valuation/anf_valuation_golden_manifest.v1.json`](../tests/fixtures/valuation/anf_valuation_golden_manifest.v1.json)
  - Register and replay golden `valuation-source-native:anf@1.0.0` from committed
    content identities. Native Excel outputs are acceptance evidence, not the
    deterministic workbook identity.

The source-native product and replayable golden are accepted. The workbook bridge
remains `target_not_wired`, `production_default=false`; artifact-tool is
read/inspection/render only and native Excel is used only at the registered acceptance
boundary.

### 4g. Source-native Capital Allocation / Capital Return (golden accepted, workbook not wired)
- [`pbi_xbrl/longitudinal_memory/capital_allocation_return_product_expansion.py`](../pbi_xbrl/longitudinal_memory/capital_allocation_return_product_expansion.py)
  - Composes typed Capital Return, normalized financial, Debt/Liquidity, and Summary/BS
    owners through declarative metric routes, activity families, semantic periods,
    relevance rules, and missing-is-not-zero behavior.
- [`pbi_xbrl/longitudinal_memory/valuation_guidance_net_share_polish.py`](../pbi_xbrl/longitudinal_memory/valuation_guidance_net_share_polish.py)
  - Owns the accepted net-share percentage derivation, final Capital binding plan,
    hidden lineage support, Guidance compression, and Operating Drivers retirement.
- [`pbi_xbrl/longitudinal_memory/capital_allocation_return_golden.py`](../pbi_xbrl/longitudinal_memory/capital_allocation_return_golden.py) and
  [`tests/fixtures/capital_allocation_return/anf_capital_allocation_return_golden_manifest.v1.json`](../tests/fixtures/capital_allocation_return/anf_capital_allocation_return_golden_manifest.v1.json)
  - Register product golden `capital-allocation-return-source-native:anf@1.0.0` and
    workbook successor `valuation-source-native-workbook:anf@2.0.0`. Replay uses the
    immutable Valuation v1 golden plus a closed content-addressed OOXML delta.

The ANF product is golden accepted. PBI is not wired or production-supported by this
golden; its remaining requirement is a ticker-specific presentation binding profile.
The shared workbook bridge remains `target_not_wired`, `production_default=false`.

### 4h. Source-native Operating Drivers (golden accepted, workbook not wired)
- [`pbi_xbrl/longitudinal_memory/operating_driver_foundation.py`](../pbi_xbrl/longitudinal_memory/operating_driver_foundation.py),
  [`operating_driver_shadow_registry.py`](../pbi_xbrl/longitudinal_memory/operating_driver_shadow_registry.py),
  [`operating_driver_derived_analytics.py`](../pbi_xbrl/longitudinal_memory/operating_driver_derived_analytics.py), and
  [`operating_driver_semantic_priority.py`](../pbi_xbrl/longitudinal_memory/operating_driver_semantic_priority.py)
  - Own typed continuity, canonical driver/observation identity, bounded longitudinal
    analytics, context semantics, and fail-closed prioritization.
- [`pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_product.py`](../pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_product.py) and
  [`operating_driver_cross_ticker_profiles.py`](../pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_profiles.py)
  - Compose the generic investor product through the shared-engine, sector-pack, and
    declarative-profile boundary without ticker-specific Python economics.
- [`pbi_xbrl/longitudinal_memory/operating_driver_golden.py`](../pbi_xbrl/longitudinal_memory/operating_driver_golden.py) and
  [`tests/fixtures/operating_drivers/operating_drivers_golden_manifest.v1.json`](../tests/fixtures/operating_drivers/operating_drivers_golden_manifest.v1.json)
  - Register product golden `operating-drivers-source-native:cross-ticker@1.0.0` and
    ANF/PBI/GPRE workbook goldens. Each workbook replays from its protected shell plus
    a closed content-addressed OOXML delta; GPRE VBA remains byte-identical.

Operating Drivers owns driver observations, longitudinal history, bounded analytics,
context interpretation, economic-role semantics, and investor presentation. It does
not own financial statements, Investment Case forward assumptions, Quarter Notes
management commentary, or Valuation. The workbook bridge remains `target_not_wired`,
`production_default=false`.

### 4i. Normalized/frozen-shell engine (validated transition)
- [`docs/normalized_company_data.schema.json`](normalized_company_data.schema.json)
  - Transition package contract consumed by the new-engine planner; it is not the
    longitudinal schema and no accepted general bridge joins the two.
- [`pbi_xbrl/normalized_company_data_validation.py`](../pbi_xbrl/normalized_company_data_validation.py)
  - Normalized-package semantic validation.
- [`docs/workbook_binding_map.json`](workbook_binding_map.json) and
  [`pbi_xbrl/new_ticker_binding_planner.py`](../pbi_xbrl/new_ticker_binding_planner.py)
  - Exact normalized-field-to-cell planning; no source selection authority.
- [`templates/standard_stock_model_template.xlsx`](../templates/standard_stock_model_template.xlsx),
  [`pbi_xbrl/new_ticker_value_filler.py`](../pbi_xbrl/new_ticker_value_filler.py), and
  [`pbi_xbrl/new_ticker_style_application.py`](../pbi_xbrl/new_ticker_style_application.py)
  - Frozen presentation shell, value-only execution, and style-only overlays.

This is a validated transition path. It is not universal production and must not be
described as the implemented Promise Progress workbook bridge.

### 5. Active legacy workbook rendering
- [`pbi_xbrl/excel_writer_context.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_context.py)
  - Main workbook renderer and the largest concentration of visible product logic.
  - Owns many final write paths for `Valuation`, `Quarter_Notes_UI`, `Promise_Progress_UI`, `Economics_Overlay`, and supporting QA surfaces.
  - In the current GPRE runtime layout, it also owns the precompute/reuse boundary for expensive overlay market snapshots and fitted-model preview inputs.
- [`pbi_xbrl/excel_writer_economics_overlay.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_economics_overlay.py)
  - Dedicated stage-2/stage-5 writer surface for the GPRE-specific `Economics_Overlay` support path.
  - Owns `Basis_Proxy_Sandbox` write orchestration plus the proxy comparison / proxy-implied panels that must stay aligned with the GPRE basis model.
  - In the current stage-5 layout, it also owns the short workbook-facing note that separates the official row, fitted row, production winner, and best forward lens.
- [`pbi_xbrl/excel_writer_hidden_value_flags.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_hidden_value_flags.py)
  - Dedicated stage-3 writer surface for the `Hidden_Value_Flags` sheet.
  - Owns the sheet-local formatting and visible contract that `Valuation` formulas read back through `Hidden_Value_Flags`.
- [`pbi_xbrl/excel_writer_promise_progress.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_promise_progress.py)
  - Dedicated stage-4 writer surface for the visible `Promise_Progress_UI` sheet.
  - Owns the visible sheet scaffold, Promise Progress block-header rendering, and the final worksheet formatting contract while shared hydration logic stays in `excel_writer_context.py`.
  - Compatibility owner for the currently delivered legacy workbook; it does not
    consume or override `PromiseProgressProduct@1`.
- [`pbi_xbrl/excel_writer.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer.py)
  - Workbook candidate serialization, readback validation, and export validation entrypoints within the accepted publication contract.
- [`pbi_xbrl/excel_writer_core.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_core.py), [`pbi_xbrl/excel_writer.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer.py), and [`stock_models.py`](/c:/Users/Jibbe/Aktier/Code/stock_models.py)
  - Jointly implement the single accepted publication sequence: classified materialization/finalization, validation, isolated serialization, serialized readback, and atomic promotion. Material failures block publication; explicitly optional enrichment remains classified rather than silently becoming success.
- Run-scoped writer runtime helpers:
  - [`pbi_xbrl/writer_runtime_cache.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/writer_runtime_cache.py)
    - Groups per-export caches so repeated heavy source analysis does not leak across workbook runs.
  - [`pbi_xbrl/quarter_notes_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/quarter_notes_runtime.py)
    - Shared document-analysis cache for quarter-note rendering inside one export.
  - [`pbi_xbrl/valuation_precompute_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/valuation_precompute_runtime.py)
    - Low-level valuation document parsing and reuse helpers.
  - [`pbi_xbrl/operating_drivers_runtime.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/operating_drivers_runtime.py)
    - Run-scoped row selection and cache state for `Operating_Drivers`.
- Supporting writer modules:
  - [`pbi_xbrl/excel_writer_drivers.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_drivers.py)
  - [`pbi_xbrl/excel_writer_sources.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_sources.py)
  - [`pbi_xbrl/excel_writer_segments.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_segments.py)
  - [`pbi_xbrl/excel_writer_financials.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_financials.py)
  - [`pbi_xbrl/excel_writer_ui.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_ui.py)
  - [`pbi_xbrl/excel_writer_core.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/excel_writer_core.py)
  - [`pbi_xbrl/writer_qa_policy.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/writer_qa_policy.py)
    - Declarative writer-side QA severity and queue policy for visible QA sheets.

### 6. Market-data pipeline
- [`pbi_xbrl/market_data/service.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/service.py)
  - Syncs raw inputs, parsed parquet frames, manifests, and exported rows used by the workbook.
  - Also bridges ticker-local USDA working folders / bootstrap CSVs into the shared export layer.
  - Owns incremental `--market-reparse` semantics via raw-tree fingerprints, parsed manifests, and ticker export cache keys.
  - Keeps `--market-force-reparse` available as the explicit full rebuild path when an operator wants to bypass cache reuse.
  - For GPRE, it also owns the official-proxy snapshots, weekly history series, filing-backed plant-capacity timeline, and fitted-model preview bundle.
  - Heavy GPRE snapshot/history helpers now accept normalized market-row `DataFrame` inputs so the writer can reuse one prepared frame instead of rebuilding it repeatedly.
- [`pbi_xbrl/market_data/providers/`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/providers)
  - Source-specific discovery and parsing.
  - In the active GPRE workflow, `local_chicago_ethanol_futures` is the canonical local ethanol strip provider:
    - local Chicago ethanol futures CSVs feed `Next quarter thesis`
    - local manual snapshot files can seed `Quarter-open proxy` when frozen prior-quarter history is missing
  - Current USDA providers now handle Drupal/AJAX “latest/previous release” fragments instead of relying only on static landing-page links.
- [`pbi_xbrl/market_data/cache.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/market_data/cache.py)
  - Path and manifest helpers for the market-data cache layout.
- [`usda_backfill.py`](/c:/Users/Jibbe/Aktier/Code/usda_backfill.py)
  - Operator CLI for targeted USDA archive backfills when `--refresh-market-data` is not enough.

For the operational USDA download/backfill flow, see
[`MARKET_DATA_USDA.md`](/c:/Users/Jibbe/Aktier/Code/docs/MARKET_DATA_USDA.md).

For the current `GPRE` economics-overlay source precedence, local ethanol-futures files, and crush-proxy behavior, see
[`GPRE_ECONOMICS_OVERLAY.md`](/c:/Users/Jibbe/Aktier/Code/docs/GPRE_ECONOMICS_OVERLAY.md).

For GPRE derivative/hedge accounting boundaries, open notional exposure, and
the diagnostic `Derivative_Crush_Tests` sheet, see
[`GPRE_DERIVATIVE_HEDGE_DIAGNOSTICS.md`](/c:/Users/Jibbe/Aktier/Code/docs/GPRE_DERIVATIVE_HEDGE_DIAGNOSTICS.md).

### 7. QA, audit, and comparison support
- [`pbi_xbrl/pipeline_qa.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/pipeline_qa.py)
  - Final QA/Needs_Review shaping.
- [`pbi_xbrl/sec_cache_audit.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/sec_cache_audit.py)
  - Audit-only reporting for mixed cache cleanup decisions.
- [`pbi_xbrl/workbook_gap_audit.py`](/c:/Users/Jibbe/Aktier/Code/pbi_xbrl/workbook_gap_audit.py)
  - Workbook comparison helpers against saved artifacts and cache outputs.

## Hand-off Models

### Active legacy production
1. SEC and local materials enter through ingest, refresh, and market-data sync.
2. `pipeline_orchestration` builds reusable stage outputs and final pipeline artifacts.
3. `pipeline.py` packages those artifacts into `WorkbookInputs`.
4. `excel_writer_context` coordinates the workbook write and delegates repeated per-export analysis to the writer runtime helpers.
5. `excel_writer.py` and `stock_models.py` save, reopen, and validate the delivered workbook.

### Source-native accepted scope

`reviewed immutable sources -> source adapter -> longitudinal memory -> canonical resolution/change observations -> PromiseProgressProduct -> product/shadow/parity fixtures`

The flow stops there. No normalized or workbook consumer is connected.

### Normalized/frozen-shell transition

`normalized package -> validation -> binding/style plans -> copied frozen shell -> readback/immutable validation -> separately approved promotion`

This transition flow is not automatically fed by longitudinal memory.

## What Each Stage Hands To The Next One
- Ingest / refresh
  - hands local cache trees, filing metadata, and market-data raw files to the runtime.
- `pipeline_orchestration`
  - hands a `PipelineArtifacts` bundle containing normalized history, audit frames, evidence outputs, QA, and workbook-side support frames.
- `pipeline.py`
  - hands a single `WorkbookInputs` object to the writer instead of many ad hoc dataframe parameters.
- `excel_writer_context`
  - hands a populated `WriterContext` plus run-scoped helper caches to the individual sheet writers.
- Sheet writers
  - hand a fully rendered in-memory workbook back to `excel_writer.py`.
- `excel_writer.py`
  - serializes an isolated candidate and hands its validated readback provenance to `stock_models.py`, which atomically promotes only an accepted candidate.

## Most Important Files To Read First
1. [`SYSTEM_LIFECYCLE_REGISTRY.json`](SYSTEM_LIFECYCLE_REGISTRY.json)
2. [`OWNERSHIP_REGISTRY.json`](OWNERSHIP_REGISTRY.json)
3. [`EXTENSION_POINTS.md`](EXTENSION_POINTS.md)
4. For active legacy production: [`stock_models.py`](../stock_models.py), [`pbi_xbrl/pipeline_orchestration.py`](../pbi_xbrl/pipeline_orchestration.py), and [`pbi_xbrl/excel_writer_context.py`](../pbi_xbrl/excel_writer_context.py).
5. For source-native semantics: the applicable longitudinal/source/product contract and owner named by the ownership registry.
6. For the normalized transition: [`docs/new_engine_operator_workflow.md`](new_engine_operator_workflow.md), the normalized schema, binding map, and shell manifest.

## Terminology Guardrails

- `legacy` means current compatibility/production history, not canonical source truth.
- `oracle` means read-only product, capability, behavior, or visual reference.
- `source-native` and `normalized` are distinct current contracts; no general bridge
  joins them.
- a product projection owns product economics and display meaning; a workbook writer
  owns only its declared compatibility/presentation behavior.
- the binding-map `manual` category allows reviewed input. It does not override
  upstream evidence lineage; for example, the ANF Investment Case summary is built as
  reviewed `evidence_backed_synthesis`.

For runtime hotspots, cache layering, and current profiling guidance, see
[`PERFORMANCE_NOTES.md`](/c:/Users/Jibbe/Aktier/Code/docs/PERFORMANCE_NOTES.md).
