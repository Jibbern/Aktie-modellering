# Extension Points and Agent Navigation

This is the task-oriented entrypoint for deciding where a change belongs. It is not a runtime configuration file and does not replace the referenced contracts or code.

Start with:

1. `SYSTEM_LIFECYCLE_REGISTRY.json` — whether a surface is active, compatibility, transition, oracle, generated, or not wired.
2. `OWNERSHIP_REGISTRY.json` — the canonical owner and any still-active parallel owner.
3. `CHANGE_IMPACT_REGISTRY.json` — likely contracts, tests, goldens, products, workbook effects, migrations, and approval gates.
4. `APPROVAL_GATES.json` — the point where automation must stop for review or authorization.

Live code and closed contracts remain behavioral authority. These documents route an agent to that authority; they do not create a second selector or economic model.

Machine-addressable routing is the exact `concept:*@1` and `change:*@1` relationship in the ownership and change-impact registries. The prose here explains those links but cannot redefine ownership; a prose edit alone never changes a route.

Navigation measurements must name their starting point. Opening this task router directly can identify a documented owner in one file; a true repository cold start through `README.md` counts the README plus this router (two files). Count code confirmation separately from documented-owner discovery.

## Architecture boundary

The intended direction for new semantic work is:

`shared source/domain engine -> reusable sector pack -> declarative ticker profile -> typed product projection -> normalized/binding transition -> workbook presentation`

- **Shared engine** owns immutable evidence, identities, dates, reconciliation, periods, and generic calculations. It must not branch on ticker/company.
- **Sector pack** owns reusable metric, definition, basis, unit, dimension, parsing, and sector-calculation semantics.
- **Ticker profile** owns reviewed company/publisher/member aliases, links, calendar selection, and semantic-binding activation. It must not define reusable economics or fabricate records.
- **Product** owns product-specific selection, ordering, calculations, display text, status, lineage, validation, and parity.
- **Workbook presentation** owns approved destinations, layout, style, formula shell, and rendering. It must not select or reinterpret upstream economics.

The source-native Promise Progress product is active and accepted. Its workbook bridge is `target_not_wired`; do not modify a workbook writer as though that bridge already exists.

## Task-oriented extension table

| Extension | Start here | Required contracts/registries | Expected tests and goldens | Prohibited locations | Approval |
|---|---|---|---|---|---|
| New source role | `concept:source-discovery@1`; `pbi_xbrl/longitudinal_memory/source_adapter/discovery.py` | source-adapter contract/schema; `DocumentRole`; assertion eligibility/authority | discovery/role/locator/authority tests; activated source package golden only | workbook writer, ticker heuristic, legacy workbook | `gate:authority-order-change@1`; source gates when bytes/scope change |
| New source format or locator | source-adapter format module under `pbi_xbrl/longitudinal_memory/source_adapter/` | source role, locator identity, immutable replay contract | format extractor and stale/missing locator mutations; activated package | product projection, workbook parser, source-order fallback | source acquisition/overwrite gates as applicable |
| New source acquisition type | `concept:source-acquisition@1`; source-specific discovery/policy stays in `sec_ingest.py` or `source_material_refresh.py`, while `pbi_xbrl/source_acquisition.py` owns byte publication | `change:source-acquisition-publication@1`; verified bytes, source-specific validator, content hash, staged same-filesystem publish | acquisition mutation tests for truncation, invalid content, preserved prior source, atomic replacement, receipt identity and cleanup | direct write to the canonical final source, nonempty-only acceptance, ticker-specific transaction code | `gate:reviewed-source-acquisition@1`; `gate:source-overwrite-replacement@1` when replacing accepted bytes |
| New cache semantic version | `contract:semantic-cache-identity@1`; `change:semantic-cache-identity@1`; `pbi_xbrl/cache_semantics.py` | add one canonical named version; update only cache-specific payload builders whose output semantics depend on it | version-isolation, invalidation-matrix, stale-receipt and cold/warm identity tests | duplicated version literal, unrelated global cache payload, stat-only correctness key | `gate:semantic-cache-contract-change@1` |
| New cache-specific payload | `change:semantic-cache-identity@1`; the producing cache module plus `pbi_xbrl/cache_semantics.py` primitives | canonical JSON payload with code/source identity and only relevant semantic/config/profile/period inputs | ordering, source/code mutation, mtime-only, unrelated-version/path and stale-receipt tests | centralizing producer business semantics in `cache_semantics.py`, weak `none` identity, machine path when content identity suffices | `gate:semantic-cache-contract-change@1` when compatibility or meaning changes |
| New inline-XBRL semantic consumer | `contract:inline-xbrl-fact-text@1`; `change:inline-xbrl-fact-text@1`; call `reconstruct_inline_xbrl_fact_text()` before consumer-specific parsing | explicit continuation-chain failure handling and downstream semantic contract | continuation chain/missing/cycle/duplicate-target/order tests plus consumer regression | independent `continuedAt` joiner, neighboring-DOM concatenation, context-year text repair | `gate:authority-order-change@1` when reconstructed source text affects accepted authority |
| New debt rate role | `contract:debt-rate-semantic-ownership@1`; `change:debt-rate-semantic-ownership@1`; `pbi_xbrl/debt_rate_semantics.py` | extend `DebtRateRole`, structured concept/display evidence mapping and role-aware semantic identity | distinct-role, same-role corroboration/conflict, period, lineage, order and workbook-consumer tests | first/last percentage, magnitude inference, footnote or label-only ownership, Debt Detail presentation code | `gate:authority-order-change@1`; semantic-golden gate if accepted economics change |
| New debt source adapter | `concept:source-discovery@1`, `concept:debt-source-duplicate-ownership@1`, `change:debt-source-duplicate-ownership@1`, and the applicable source-adapter package | document/fact/period/instrument identity, source authority, exact lineage, semantic duplicate policy | source registry, period/current-prior, duplicate/corroboration/conflict, missing/zero and downstream Debt Detail tests | physical row/source order, workbook cell parsing, ticker/date branch | `gate:authority-order-change@1`; source gates when reviewed bytes/scope change |
| New source-native/sector metric | `concept:metric-identity@1` through `change:metric-definition@1`; selected shared or sector pack | versioned metric/definition/basis/unit/dimension catalog | sector mapping/incompatibility tests; activated source package | ticker profile economics, shared company branch, workbook row label | semantic golden and fallback gates |
| Existing split-owner FCF or net-debt change | `concept:free-cash-flow@1` through `change:free-cash-flow-definition@1`, or `concept:net-debt@1` through `change:net-debt-definition@1` | exact executable formula/source contracts and all active/transition consumers identified by the ownership entry | calculation, definition/basis, consumer, workbook/readback and affected golden tests | inventing a sector-pack owner, writer-only patch, ticker branch, collapsing definition variants | authority, semantic-golden, fallback and owner-migration gates when triggered |
| Presentation-only display of an existing derived metric | identify legacy versus transition output, then `concept:workbook-style@1` or `concept:workbook-binding@1` | existing metric/formula remains byte- and semantically unchanged; style/binding contract only | layout/style/readback plus economic invariance | calculation, source selection, metric identity, product semantics | presentation/layout gate; cutover/oracle gates if applicable |
| New dimension | `concept:dimensions@1` | sector dimension catalog; ticker member aliases/activation | dimension compatibility, alias collision, mapping and determinism | display text, row position, writer | semantic golden gate when accepted output changes |
| New sector concept/pack | `component:source-native-sector-packs@1` | sector pack semantic bindings, parsers/derived requests, source-set activation | reusable sector tests, no-ticker-branch tests, activated package | ticker profile reusable economics, generic runtime company logic | `gate:semantic-golden-regeneration@1`; fallback gate |
| New ticker | reviewed source set + `component:source-native-ticker-profiles@1` | source-adapter schema/contract; existing sector pack; calendar rule; immutable hashes/locators | source contract, profile/alias/dimension, semantic mapping, determinism, new package golden | shared-runtime ticker branch, legacy workbook as source authority | source acquisition/overwrite, ticker activation, golden gates; workbook cutover later |
| New FiscalCalendar rule | `concept:fiscal-calendar@1`; `calendar_rules.py` | longitudinal/source-set schemas and contracts; versioned rule | boundary, QoQ/YoY, missing rule, ChangeObservation and activated ticker tests | ticker branch, month-name inference, workbook header | schema and semantic golden gates |
| Canonical schema field change | first name the exact schema: longitudinal memory, source set, Promise Progress product/shadow, normalized package, or another versioned contract | that schema's lifecycle/ownership entry, serializer, validators, readers and compatibility policy | old/new fixture validation, deterministic round trip, every actual consumer and affected golden | a universal schema owner, in-place reinterpretation, workbook-cell-first rename | schema-compatibility, semantic-golden and owner-migration gates as triggered |
| New Guidance or Promise concept | `concept:guidance-series-version@1` or `concept:promise-version@1` | longitudinal schema/validation; explicit version relations; product contract when consumed | chronology, relation, conflict, knowledge-date and product tests | workbook quarter position, newest-only, writer copy-forward/status | authority, golden, product-contract gates |
| New product projection | canonical longitudinal owner plus a versioned product contract | typed input/output, selection/order, lineage, serializer, validator, parity policy if needed | semantic/mutation/shuffle/hash-seed tests and independently reviewed goldens | source adapter UI logic, workbook writer selection, support-sheet truth engine | product-contract, golden, owner-migration gates |
| New Promise Progress field | `concept:promise-progress-projection@1`; `PromiseProgressProduct@1` | product contract/version; closed block role/destination; shadow schema; parity/capture/scope | field/schema/lineage/parity/determinism tests; affected product/shadow goldens | Excel calculation, legacy writer selection, parallel support matrix | product-contract, golden, oracle, future cutover gates |
| New workbook binding | `concept:workbook-binding@1`; binding map and planner | binding schema; shell writable zones/merge contracts; source field contract | binding-plan, allowed-cell, capacity, readback and destination tests | source/economic selection, broad range dump, missing-to-zero | presentation/layout and cutover gates |
| New runtime workbook sheet | declarative module/sector ownership plus `concept:workbook-finalization-publication@1` and `change:workbook-finalization-publication@1` | materialization result, required visibility/order/cleanup state, structural/readback validation and explicit optionality | materialization/finalization mutation tests, saved readback, ordering, visibility, protection and atomic-publication tests | direct final-path save, swallowed material failure, test-only runtime branch | `gate:workbook-publication-contract-change@1`; presentation/layout and cutover gates as applicable |
| New Quarter Notes intentionally-empty producer | `concept:quarter-notes-intentionally-empty@1`; `change:quarter-notes-empty-state@1`; `WorkbookInputs.quarter_notes_intentionally_empty` | producer must explicitly establish valid empty; missing input, parse failure and unexpected empty remain distinct | empty-state producer/consumer/readback tests and false-green mutation coverage | arbitrary caller bypass, empty object as success, inference from missing/failed parse | `gate:product-contract-change@1` |
| Presentation-only workbook change | `concept:workbook-style@1`; identify legacy versus transition output first | shell/manifest/style policy; binding map only if geometry moves | shell identity, style protection, structural/readback/visual comparison; economic goldens unchanged | source adapter, sector/ticker semantics, product selection, normalized values | `gate:workbook-presentation-layout@1`; cutover/oracle gates if applicable |

## Public versus internal signals

Treat the following as intended public extension surfaces:

- `pbi_xbrl.longitudinal_memory` exports: identity, reconciliation, change derivation, validation, deterministic serialization.
- `pbi_xbrl.longitudinal_memory.source_adapter` exports: source-set loading, discovery, reviewed-input verification, inline-XBRL evidence capture, and sidecar build.
- Closed registries/contracts: fiscal-calendar rules, source roles, sector semantic bindings, Promise Progress `BLOCK_ORDER`, `CLOSED_PROGRESS_ROLE_IDS` and `CLOSED_STATUS_RULE_IDS`, normalized schema, workbook binding map, shell manifest, and style policy.
- Operator facades: `stock_models.py` for active legacy production and the documented new-engine workflow for transition planning/render/promotion.

Treat underscore-prefixed helpers, fixture builders, audit builders, writer repair helpers, and generated report JSON as internal unless an owning contract explicitly says otherwise. Do not infer public stability merely because a symbol is imported somewhere.

Current limitations, not invitations to improvise:

- Source-native ticker profiles still require bounded Python registration; do not compensate with shared-runtime ticker branches.
- The normalized root schema and schema-migration framework remain incomplete.
- Writer-owned economics and the large legacy dependency cycle remain active compatibility debt.
- Accepted workbook publication now classifies material versus explicitly optional finalization, blocks publication on material failure, serializes to an isolated candidate, validates serialized readback, and promotes atomically through `concept:workbook-finalization-publication@1`. New paths must reuse that contract rather than introduce a parallel save owner.
- No source-native-to-workbook bridge is implemented.

## Terminology

- **Active:** executed or consumed in the accepted current system for its declared scope.
- **Compatibility:** still required by current production, but not the preferred owner for new semantics.
- **Transition:** validated new-engine/normalized capability that has not replaced every production consumer.
- **Target not wired:** designed boundary with no active producer/consumer connection. Promise Progress workbook integration is in this state.
- **Legacy workbook oracle:** read-only product, behavior, capability, or visual reference. It is not source authority for economic facts.
- **Source-native:** records and products derived from immutable sources, explicit semantics, canonical resolution, and lineage.
- **Normalized:** the separate transition package consumed by the frozen-shell engine. It is not currently synonymous with source-native.
- **Product projection:** owns product-specific economics, selection, display and lineage. A workbook writer only consumes it.
- **Source authority:** determines eligible source-backed assertions. **Parity reference:** compares product/UI behavior and cannot promote a legacy value to canonical fact.
- **Manual:** in the binding map, a planner/source-policy category allowing reviewed input. It does not erase upstream evidence. For example, the ANF Investment Case summary is built as reviewed `evidence_backed_synthesis`; the current `manual` binding label is compatibility metadata, not a claim that the value lacks evidence lineage.

## Search guidance

Search active code and contracts before generated or historical material:

1. `README.md`, `SYSTEM_OVERVIEW.md`, `CODEBASE_MAP.md`, then these registries.
2. The canonical contract/schema named by the ownership entry.
3. The owner module and focused tests.
4. Accepted fixtures/goldens only after the semantic owner is known.
5. Generated audits, receipts, previews, external `StockModelData/audit` bundles, and legacy workbooks only as classified evidence.

Recommended repository searches:

```powershell
rg -n "<concept or symbol>" docs pbi_xbrl tests --glob "!docs/*audit*.json" --glob "!docs/audit_receipts/**" --glob "!tests/fixtures/**"
rg -n "<exact id>" docs pbi_xbrl tests
rg --files docs pbi_xbrl tests | rg "<owner term>"
```

Do not add global ignore rules that hide fixtures or generated artifacts from normal tooling. Classify them explicitly and include them when reviewing contracts, parity, or goldens.

## Approval and protected scope

Read-only discovery, static searches, temporary in-memory projections, validation, and comparison reports are normally allowed before approval. Stop at the referenced gate before changing authority, source bytes, schema compatibility, canonical ownership, semantic goldens, product contracts, workbook layout/cutover, or destructive state.

If the high-level request does not identify whether it targets current legacy production, the normalized transition engine, or a source-native product, report that ambiguity before choosing implementation files.
