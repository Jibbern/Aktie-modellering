# Stock Model Engine — 2026-08-14 repository-native accepted vision

**Dated snapshot:** 2026-08-14
**Project:** Stock Model Engine
**Historical predecessor:** `Stock_Model_Engine_vision_2026-08-08.md`
**Predecessor SHA-256:** `45CB8217057A26A457E5E8BA7230C4DDCBE4145E6A2AC8A6530570BB069291ED`
**Repository checkpoint:** `c84a82bd8e12c9dc9a73d543cc8ef546eef6baf5`
**Checkpoint status:** **LOCAL ACCEPTED — NOT YET PUSHED**

This is a new immutable direction snapshot. It preserves the lineage of the
2026-08-08 snapshot; it does not replace, rename, or reinterpret that historical
document. The earlier snapshot remains the record of what the product and
architecture were understood to be on 2026-08-08.

This document explains product direction, accepted implementation, transition
state, and roadmap. It is not executable authority. Current Git, closed runtime
contracts, schemas, accepted products, tests, and goldens remain authoritative.

---

## 1. Executive vision

Stock Model Engine is not merely an ANF, PBI, or GPRE workbook generator.

It is a reusable, source-aware, agent-friendly, and self-auditing equity-analysis
platform in which:

> Source-native code owns semantics and economics. Excel is the final
> investor-facing presentation product.

The long-term goal is that normal onboarding follows:

```text
shared engine
→ sector pack
→ declarative ticker profile
→ unique code only when genuinely necessary
```

A normal ticker should increasingly be added through source registration,
profile data, sector semantics, and declarative modules rather than a new
ticker-specific Python execution path.

The governing safety principle remains:

> A correct incomplete model is better than a complete model with silent errors.

Missing is not zero. Ambiguous is not resolved. A prior-period fact is not a
current-period fact. A plausible-looking number is not accepted merely because
it fits a cell.

---

## 2. How to read this snapshot

This document distinguishes three kinds of statement.

### 2.1 Long-term vision

The product capabilities and architecture toward which the repository should
move. These statements guide design but do not claim current implementation.

### 2.2 Accepted implementation

Behavior that exists in the accepted local Git checkpoint and is protected by
runtime contracts, focused tests, regression gates, schemas, goldens, or
publication receipts.

### 2.3 Transition and compatibility

Capabilities that legitimately coexist while ownership migrates. A transition
may have a preferred semantic owner, a legacy compatibility owner, a normalized
projection, and an accepted product that is not yet workbook-wired.

The repository must describe this state honestly. Executable legacy code is not
automatically canonical, and a target architecture is not implemented merely
because it appears in documentation.

---

## 3. Repository-native acceptance checkpoint

The repository repair, hardening, non-native acceptance, native Excel acceptance,
registry refresh, and logical decomposition are **DONE / ACCEPTED** at this local
checkpoint.

The accepted six-commit series is:

1. `e509dcf42e1ae2ed5848e77e96f5a6cb6e457049` — Make publication, acquisition, and cache identity fail closed
2. `6d0878341700680100062f877a3387a7baca95c8` — Own source semantics and declarative runtime modules
3. `bfff7ee1128f8ef8ef9a2eca31226f9745f0267b` — Unify Quarter Notes, Promise, and current-writer semantics
4. `8c83d0e49c12995b6706032b857ffa0570c1c234` — Make repository tests portable and enforce the native boundary
5. `222e8b644c59de469f1396dcfa4db002c8f7f5c9` — Refresh generated workbook and parity contracts
6. `c84a82bd8e12c9dc9a73d543cc8ef546eef6baf5` — Refresh architecture registries and navigation

Stable Candidate V3 committed-content digest:

```text
d32e54e635ef4f8cefc20c6d497a54a9b801b68d26468a86ecf3b44d616dfc9b
```

The acceptance evidence includes:

- focused source, cache, debt, writer, portability, generated-artifact, and
  documentation contract gates;
- the frozen 111-node production regression;
- one complete non-native/pre-native repository gate with only the contractual
  optional-data skips and native deselections;
- all six registered native Excel tests;
- exact protected-workbook and Product@2.1 preservation;
- machine-readable owner discovery with no ambiguous or dangling owner route;
- a clean six-commit tree exactly equivalent to Stable Candidate V3.

These commits are local. This snapshot does not claim that the branch has been
pushed, tagged, merged, cut over, or integrated into the Summary/BS worktree.

---

## 4. What changed since 2026-08-08?

The core product vision did not change. The repository learned how much rigor is
required to make that vision reproducible.

Repository hardening exposed legacy shortcuts that could pass ordinary happy-path
tests while leaving semantic ownership, publication failure, cache identity, or
historical portability underspecified. The accepted repair strengthened the
architecture in five product-level ways:

1. **Semantic ownership became more typed.** Debt facts, debt rates, document
   periods, table roles, measure domains, units, and adjusted-history definitions
   now establish identity before selection.
2. **Source reconstruction became explicit.** Inline-XBRL continuation chains are
   reconstructed before downstream date or text interpretation.
3. **Publication became transactional and fail-closed.** Source acquisition and
   workbook publication now isolate candidates, validate them, and promote them
   atomically.
4. **Cache identity became part of economic reproducibility.** Code content,
   source content, semantic versions, and relevant configuration now participate
   in deterministic cache identity; repository-generated text identities are
   portable across checkout EOL materialization.
5. **Repository acceptance became explicit.** The native Excel boundary is
   registered, portability and historical fixtures have owners, and lifecycle,
   ownership, extension, impact, and approval metadata describe the accepted
   architecture.

This is architectural hardening, not a claim that all legacy economics have been
migrated to source-native products.

---

## 5. Target product architecture

The preferred end-to-end direction is:

```text
verified source bytes
        ↓
source-defined fact reconstruction
        ↓
typed source facts and evidence occurrences
        ↓
shared semantic engine / sector pack / ticker profile
        ↓
canonical longitudinal company memory
        ↓
canonical resolutions and change observations
        ↓
immutable source-native product projections
        ↓
shadow lineage / parity / Needs Review
        ↓
reviewed workbook bridge
        ↓
validated Excel presentation
```

Each layer should narrow ambiguity rather than hide it. Workbook materialization
must consume accepted semantics; it must not become a second upstream economic
selector.

Operating Drivers, Quarter Notes, Promise Progress, Summary, Valuation, Debt
Detail, and Capital Return are product surfaces. They should converge on shared
facts and lineage where their semantics overlap, while retaining distinct product
responsibilities.

---

## 6. Current architecture surfaces

Four architecture surfaces coexist at the accepted checkpoint.

### 6.1 Legacy workbook production and compatibility

The active CLI, pipeline/orchestration, writer context, and workbook writers still
produce delivered PBI/GPRE workbooks and current legacy projections. They include
compatibility behavior and some writer-owned economic selection.

This surface is active and useful. It is not the preferred owner for new
source-native semantics, and its continued execution does not make all of its
economics canonical.

### 6.2 Normalized/frozen-shell transition architecture

The normalized package, binding planner, frozen template shell, value-only filler,
style applicator, validation, promotion, and rollback paths are validated
transition capabilities.

They have not replaced every production consumer. They are not a completed general
source-native workbook bridge.

### 6.3 Source-native longitudinal/product architecture

The typed longitudinal company-memory contract, source adapters, sector packs,
ticker profiles, and immutable product projections are the preferred semantic
direction.

ANF/PBI source-native proofs and the accepted Promise Progress product demonstrate
the architecture. The accepted Product@2.1 golden remains workbook-independent in
ownership terms.

### 6.4 Validation and promotion infrastructure

Repository tests, schemas, generated contracts, mutation suites, protected oracles,
scratch/current projections, serialized readback, and atomic promotion form a
cross-cutting acceptance surface.

This infrastructure determines whether an output may be accepted. It does not own
the source economics that the output represents.

---

## 7. Accepted semantic and safety principles

### A. Source fact reconstruction before semantic parsing

An inline-XBRL display fragment is not necessarily the full source fact. Valid
`continuedAt` chains must be resolved in explicit chain order before downstream
date or text semantics are interpreted. Missing, duplicate, cyclic, or malformed
continuations fail closed. Neighboring DOM text is not implicit continuation.

### B. Semantic identity before selection

Canonical ownership must not come from first/last row, append order, source order,
physical table order, first numeric token, or first percentage.

The engine establishes semantic identity first and only then performs authority,
corroboration, derivation, or conflict resolution.

### C. Document period is not fact period or model/display period

Document identity, source-fact context period, fiscal period, as-of/knowledge date,
and workbook display period are independently typed concerns. Internal facts do
not redefine top-level document identity, and current-period absence does not
authorize prior-period substitution.

### D. Table role before metric extraction

A table's role constrains which labels, concepts, rows, and columns are eligible
for a metric. Adjustment, EPS, amount, reconciliation, debt, and other table roles
must be established before extraction flattens their evidence into one pool.

### E. Measure domain before numeric interpretation

Monetary amounts, per-share amounts, percentages, counts, ratios, and other domains
remain distinct even when labels resemble one another. A matching label does not
authorize a domain conversion.

### F. Rate role before percentage selection

Debt percentages must be classified before canonical selection. Supported roles
include coupon/stated rate, effective interest rate, floating base rate,
spread/margin, all-in rate, conversion-related rate, other percentage, and
not-a-rate.

Instrument, reporting period, rate role, basis/scope, and applicable effective
period own economic identity. Source occurrence is lineage. Footnotes, maturity
years, principal amounts, and unrelated percentage concepts cannot become coupon
rates merely because they are numeric.

### G. Table-local unit ownership

One source document may contain values in ones, thousands, millions, per-share
units, percentages, or counts at the same time. Unit ownership is table/fact-local,
not document-global. Raw unit, normalized unit, scale application, and lineage must
remain distinguishable.

### H. Explicit definition boundaries

Historical metrics cannot be silently spliced across incompatible reported,
recast, continuing-operations, adjusted, basis, or scope definitions. A TTM or
history is available only when its component periods share a compatible accepted
definition.

### I. Transactional source acquisition

Accepted source publication follows:

```text
acquire
→ same-filesystem staging
→ source-specific minimum validation
→ content hash
→ durable write where applicable
→ atomic replacement
→ final receipt/identity
```

A nonempty file is not automatically a valid source. A failed refresh must not
destroy a previously valid final source.

### J. Transactional workbook publication

Accepted workbook publication follows:

```text
materialize
→ classified finalization
→ in-memory validation
→ temporary serialization
→ serialized readback validation
→ atomic promotion
```

A material finalization, calculation, cleanup, ordering, visibility, structural,
or readback failure blocks accepted publication. Explicit optional enrichment may
remain optional only when its contract says so.

### K. Cache identity is part of economic reproducibility

Where output semantics depend on them, cache identity includes:

- content-derived code identity;
- verified source-content identity;
- relevant semantic contract versions;
- relevant configuration and profile/module identity;
- period/as-of identity.

Weak `none`, `unknown`, default, mtime-only, or size-only correctness identities
are not acceptable. Stats may remain as performance hints when content-backed
validation owns correctness. Cache-specific producers retain responsibility for
the semantic inputs of their payloads.

### L. Repository-generated contracts require portable source identity

Generated repository contracts that identify tracked text must not change merely
because a checkout materializes equivalent text as LF, CRLF, or mixed EOL.

Canonical repository-text identity normalizes checkout-only newline encoding while
preserving all other text bytes. Binary/workbook identity remains byte-exact.

### M. Protected oracle is not the current generated product

Protected legacy workbooks may remain product, capability, behavior, or visual
oracles. Current corrected behavior may be exercised through test-owned scratch
copies or generated projections.

Oracle, source authority, current generated candidate, source-native golden, and
accepted delivered product are different roles and must not be collapsed.

### N. Fail closed on ambiguity

Unknown, conflicting, incomplete, malformed, unavailable, and unsupported states
must not silently become:

- zero;
- prior-period value;
- GAAP substitute for an adjusted metric;
- first source or first row;
- an arbitrary percentage;
- empty success;
- a published workbook;
- a cache hit under a weak identity.

Needs Review or explicit unavailability is preferable to plausible contamination.
Explicit source-backed zero remains valid.

---

## 8. Accepted implementation ownership

The accepted checkpoint adds or strengthens discoverable owners without making one
module own every business meaning:

- semantic cache identity owns canonical serialization, content identity, weak
  identity rejection, and the shared semantic-version catalog; producers own
  cache-specific payload semantics;
- inline-XBRL fact text owns continuation reconstruction before consumer-specific
  semantic parsing;
- debt-rate semantics owns role-aware rate identity and conflict handling;
- debt-source duplicate resolution separately owns facility/principal-row semantic
  reconciliation before physical order;
- source acquisition owns byte-publication transactionality while SEC ingest and
  source refresh own source-specific discovery and policy;
- workbook finalization/publication spans material finalization, candidate
  serialization/readback, and final promotion without creating a second economic
  owner;
- Quarter Notes intentionally-empty state distinguishes valid empty output from
  missing input, parse failure, and unexpected empty readback;
- derivative dependency unavailability may be explicitly optional, while renderer
  or materialization failure after a valid build is material.

The current shared cache semantic dimensions include unit normalization,
adjustment-domain ownership, registered document identity, debt fact-period
ownership, adjusted-history definition/scope, inline-XBRL continuation text, and
debt-rate role/period/authority.

These contracts strengthen existing product semantics; they do not claim that all
legacy workbook logic has migrated to source-native owners.

---

## 9. Source, evidence, history, and period integrity

Every material observation should be capable of carrying:

- source document and content hash;
- source role and authority;
- exact locator and occurrence;
- publication, knowledge, fiscal, effective, and reporting dates as applicable;
- raw value/text and canonical value/text;
- concept, role, basis, scope, dimensions, and unit;
- derivation eligibility and formula lineage;
- conflict, invalid, missing, unavailable, or Needs Review disposition.

Source occurrence is evidence lineage; it is not automatically a separate economic
concept. Identical compatible facts may corroborate one another while retaining
their contributing lineage. Same-authority conflicting facts fail closed.

Historical views use only evidence eligible as of the requested knowledge date.
Definitions, guidance, promises, and management statements are versioned rather
than overwritten.

Fiscal calendars remain typed and rule-driven. Calendar-year and source-labelled
52/53-week periods must be compared through their accepted calendar rules;
unknown or missing calendar ownership fails closed.

---

## 10. Product ownership and workbook roles

### 10.1 Promise Progress

Promise Progress is an accepted source-native product over longitudinal company
memory. Actual, Progress, and Status remain distinct typed concepts, and its
Product@2.1 golden is protected.

The source-native workbook bridge remains:

```text
target_not_wired
```

The current legacy Promise Progress writer is a compatibility/UI owner, not a
consumer that proves the source-native bridge is complete.

### 10.2 Investment Case and Valuation

Investment Case remains the accepted detailed forward-valuation owner. Valuation
should consume a compact read-only forward summary rather than create a competing
detailed scenario engine.

Valuation economic/formula reconciliation and final Debt Detail presentation/source
reconciliation remain roadmap work.

### 10.3 Summary and BS_segment

Summary should be a decision surface consuming canonical read-only outputs rather
than a new calculation engine. The separate Summary/BS source-native work remains
unintegrated with this accepted repository branch and has not completed workbook
cutover or golden acceptance.

### 10.4 Debt, liquidity, and rates

Facility/principal identity and rate-fact identity are adjacent but distinct
layers. Debt/Liquidity summary economics, Debt Detail instrument presentation,
rate roles, net debt, leverage, liquidity, revolver, and maturity profile must not
be conflated merely because they share a workbook area.

### 10.5 Operating Drivers and Quarter Notes

Operating Drivers should become the operational company model over longitudinal
memory. Quarter Notes should answer “What changed this quarter?” while separating
reported fact, management explanation, and model interpretation.

Both currently retain legacy/current-writer product paths. Their future
source-native products must reuse shared evidence and semantic primitives rather
than introduce new sheet-specific parsers.

Improvement, deterioration, trajectory, and inflection should remain transparent
presentations of underlying evidence and change observations, not independent
black-box scores.

### 10.6 FCF, net debt, and other split concepts

FCF and net debt still have legitimate split/transition ownership across current
consumers. The architecture must expose that state rather than designate a false
single owner. Migration should proceed only when definitions, consumers, lineage,
and rollback are explicit.

---

## 11. New-ticker success criteria

A normal new ticker should increasingly require:

- registered, immutable source identities and source roles;
- fiscal-calendar and period rules;
- sector-level concepts, mappings, and source semantics;
- a declarative ticker profile and activated modules;
- explicit source authority, aliases, dimensions, and reviewed exceptions;
- bounded goldens and mutation tests;
- unique runtime code only for genuinely unique behavior.

The engine must correctly determine or explicitly reject:

- document identity;
- document, fact, fiscal, effective, knowledge, and display periods;
- metric and table role;
- definition and basis/scope;
- measure domain and unit;
- source authority and corroboration;
- debt facility role and debt-rate role;
- derivation eligibility;
- cache and publication identity.

Success does not mean forcing every source into an existing row. It means safe
facts become deterministic, ambiguity is isolated, review items are few and
material, and generic discoveries improve the shared engine or sector pack rather
than add ticker branches.

---

## 12. Accepted validation and publication model

Validation is layered:

1. **Focused contract tests** prove local invariants and mutation behavior.
2. **Frozen production regression** protects accepted economic behavior across the
   defined production inventory.
3. **Full non-native/pre-native repository gate** proves cross-surface repository
   portability, integration, and skip classification.
4. **Explicit native Excel boundary** executes only cases where real Excel adds
   materially different confidence beyond OOXML/openpyxl, structural validation,
   rendering, and serialized readback.

The registered native boundary currently contains exactly six `native_excel`
tests under `contract:repository-native-excel-test-boundary@1`.

Native Excel is not a default implementation loop and is not a substitute for
programmatic workbook contracts. It is used where recalculation, COM ownership,
native serialization, protection, rollback, style, or rendering behavior requires
real Excel evidence.

Acceptance counts and timings are checkpoint evidence, not permanent architectural
performance requirements. Test inventories remain machine-readable and changes to
their boundary require explicit review.

---

## 13. Machine readability and agent-friendly ownership

A fresh capable agent should be able to find, without conversation history:

- lifecycle state;
- canonical owner and compatibility/transition owner;
- consumers;
- extension route;
- change-impact route;
- approval boundary;
- relevant contracts, tests, goldens, and oracles.

The accepted discoverability chain is:

```text
README / SYSTEM_OVERVIEW / CODEBASE_MAP
→ SYSTEM_LIFECYCLE_REGISTRY
→ OWNERSHIP_REGISTRY
→ EXTENSION_POINTS
→ CHANGE_IMPACT_REGISTRY
→ APPROVAL_GATES
→ referenced runtime contracts, schemas, tests, and goldens
```

Structured IDs and exact references own routing. Prose explains intent; keyword
similarity does not establish ownership.

The registries identify, among other things, the owners of semantic cache identity,
inline-XBRL text reconstruction, debt-rate semantics, source-file transactionality,
and workbook finalization/publication.

Documentation registries are discoverability metadata. They are not executable
economic authority and must not redefine runtime behavior merely by changing text.

---

## 14. File structure, abstractions, and deferred cleanup

The architecture review found large transition modules, dependency cycles, legacy
economic selection inside writers, dynamic-exec writer support, scattered domain
files, uneven lineage migration, and expensive test architecture.

The new safety/domain modules generally represent justified boundaries: they own
transactionality, semantic identity, reconstruction, conflict behavior, or
lineage. A broad package move is not required now.

Future reorganization should occur when it materially improves:

- ownership clarity;
- testability;
- extension safety;
- machine readability;
- legacy retirement;
- rollback or deployment safety.

It should not occur merely to produce a visually cleaner directory tree.

Deferred categories include:

- legacy pipeline/orchestrator decomposition and SCC reduction;
- retirement of dynamic-exec writer modules;
- migration of writer-owned compatibility economics to source-native products;
- broader end-to-end typed lineage;
- test-monolith and repeated-live-generation performance work;
- deliberate domain/package moves after product boundaries stabilize.

These are real debt and future work. They are not blockers for the accepted local
checkpoint or the next product roadmap.

---

## 15. Performance position

Correct content-addressed caching materially improves repeated builds. Warm-path
evidence confirms that reusable identity-backed artifacts can avoid redundant
pipeline and workbook regeneration.

Performance remains separate from correctness acceptance. Future optimization must
preserve:

- exact source and semantic identity;
- deterministic invalidation;
- full test coverage;
- no shared mutable workbook state;
- no weakened publication or cache validation;
- no silent fallback economics.

A likely direction is immutable, source/code-hash-keyed reusable test artifacts,
more precise cache invalidation, and reduced repeated live workbook/pipeline
generation. Measured ticker timings and individual slow-node durations are
observations, not permanent contracts.

---

## 16. Roadmap from the accepted local checkpoint

Repository repair and native acceptance are **DONE / ACCEPTED**.

The next ordered roadmap is:

### A. Publish/push the accepted repository commit series

Review this local checkpoint for publication, then push without rewriting the
accepted six-commit history or this dedicated vision commit.

### B. Integrate accepted commits into Summary/BS

Bring the dependency-ordered accepted series into
`fix/summary-bs-segment-source-native-reconciliation` while preserving its current
four-file source-native work.

### C. Resume Summary + BS_segment source-native workbook projection/preview

Continue the existing source-native implementation from the integrated checkpoint
and generate a fresh isolated preview.

### D. Exhaustive Summary + BS structural, semantic, and visual recheck

Reconcile sources, rows, formulas, styles, protection, layout, lineage, missing/zero
states, and native behavior as required.

### E. Accepted Summary/BS golden/checkpoint

Accept only a reproducible fresh product with a clear cutover/rollback boundary.

### F. Valuation — exhaustive economic and formula reconciliation

Audit the complete Valuation product while preserving Investment Case as detailed
forward-valuation owner.

### G. Final Debt Detail source/presentation reconciliation within Valuation audit

Complete instrument, balance, maturity, rate-role, lineage, and visible consumer
reconciliation without moving semantic ownership into the workbook.

### H. Capital Return / Debt Detail ownership reconciliation

Clarify remaining overlaps between capital return, financing, debt, liquidity,
shareholder distributions, and presentation consumers.

### I. Operating Drivers evidence-utilization / longitudinal-change audit

Understand current evidence use and change semantics before designing the
source-native product.

### J. Quarter Notes evidence-utilization / longitudinal-change audit

Audit current fact/explanation/interpretation use and temporal behavior before
replacing the legacy/current-writer path.

### K. Source-native Operating Drivers product

Build the product over longitudinal company memory and shared sector primitives.

### L. Source-native Quarter Notes product

Build the product over the same evidence foundation, focused on quarter-over-quarter
change in the information set.

### M. Broader workbook bridge and generalized cutover

Extend reviewed product-to-workbook materialization only after product semantics,
shadow lineage, parity, promotion, and rollback are proven.

### N. Eventual legacy retirement

Retire compatibility implementations when replacements exist, consumers are
retargeted, parity is accepted, and rollback is explicit.

Product development remains ahead of aesthetic large-scale refactoring.

Beyond this ordered roadmap, the longer-term product vision still includes
forecast vintages, underwriting-versus-actual analysis, and a shareholder-return
bridge. They remain future product capabilities rather than commitments at this
checkpoint.

---

## 17. Success criteria

### 17.1 Source-native core

- immutable verified evidence;
- explicit source roles and assertion-specific authority;
- typed periods, definitions, domains, units, and rate roles;
- deterministic semantic identity and canonical resolution;
- no source/row-order ownership;
- preserved corroborating and conflict lineage;
- typed missing, unavailable, invalid, and Needs Review states.

### 17.2 Product projections

- one clear semantic owner per accepted concept or an honest transition record;
- knowledge-date-correct history;
- no incompatible definition splice;
- missing is not zero;
- source-backed lineage and explicit derivation;
- fail-closed unsupported cases.

### 17.3 Workbook output

- polished investor-facing product;
- deterministic bindings and planned formulas;
- source-native or explicitly documented compatibility semantics;
- material finalization and readback validation before publication;
- scratch/candidate isolation and atomic promotion;
- structural, semantic, visual, protection, and native validation proportional to
  risk.

### 17.4 New-ticker onboarding

- shared engine and sector semantics reused;
- declarative ticker profile preferred;
- no generic-runtime ticker branch for ordinary variation;
- ambiguity surfaced rather than guessed;
- new general source behavior improves reusable owners.

### 17.5 Machine readability

- lifecycle, owner, consumer, extension, impact, approval, tests, and goldens are
  discoverable through structured references;
- exact IDs resolve with no ambiguous or dangling owner;
- active, compatibility, transition, oracle, and target-not-wired states are
  truthful;
- documentation routes to economic authority but does not become that authority.

---

## 18. Statements intentionally not claimed

This snapshot does **not** claim that:

- all legacy economics are source-native;
- every workbook writer is pure presentation;
- all ticker onboarding is data-only;
- lineage is source-native end-to-end for every product;
- Promise Progress source-native workbook output is wired;
- Summary/BS source-native cutover is complete;
- Valuation reconciliation is complete;
- broad workbook cutover or legacy retirement has occurred;
- the local accepted commits have been pushed or published.

These limits are part of the architecture truth, not caveats to hide.

---

## 19. Current authority hierarchy

When this vision and current repository state appear to differ, use this order:

1. accepted Git HEAD/checkpoint;
2. executable runtime contracts and schemas;
3. accepted source-native/golden product artifacts;
4. machine-readable lifecycle, ownership, impact, and approval metadata as
   discoverability;
5. legacy protected workbooks as read-only product/capability oracles;
6. dated vision and audit documents as historical context.

The vision is subordinate to executable authority. Audit prose is evidence and
history, not a substitute for the current tree.

---

## 20. Snapshot status

This document records the Stock Model Engine vision, accepted implementation,
transition state, and roadmap as understood on **2026-08-14** at local commit
`c84a82bd8e12c9dc9a73d543cc8ef546eef6baf5` plus this dedicated documentation
snapshot.

The repository-native checkpoint is:

```text
LOCAL ACCEPTED
NOT YET PUSHED
```

The immutable 2026-08-08 predecessor remains historical context. Future snapshots
should preserve both documents, cite their exact checkpoint, distinguish target
from accepted implementation, and record any ownership migration explicitly.
