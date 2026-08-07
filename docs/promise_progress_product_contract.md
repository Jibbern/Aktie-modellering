# Promise Progress source-native product contract

Contract identity: `contract:promise-progress-product@1`
Product type: `PromiseProgressProduct@1`

This contract defines a pure, deterministic projection from one accepted
longitudinal-company-memory package. It does not define an Excel writer and does not
authorize workbook, source, or normalized-package writes.

## Single product owner

One immutable product owns record selection, business ordering, display text, Actual,
Progress, Status, visible rows, hidden row keys, field lineage, validation, and parity
exceptions. A UI, support sheet, status renderer, or future workbook writer may only
consume this product; it may not repeat or reinterpret its economic selection logic.

The product contains four distinct typed blocks in this fixed order:

1. `block:promise-progress:management-credibility-scorecard@1`
2. `block:promise-progress:annual-guidance-progression@1`
3. `block:promise-progress:open-guidance@1`
4. `block:promise-progress:quarterly-revision-timeline@1`

The blocks share value, lineage, and assessment types, but are not coerced into one
generic Promise row type.

## Management Credibility Scorecard

The scorecard is an analytical assessment product, not a source fact. A score requires
a separately accepted artifact with assessment identity, method and version, canonical
inputs, knowledge date, producer, review state, evidence, score rule, score, and
interpretation. Without that artifact, the five legacy rows remain present while Score
and Read display `Needs Review`. Legacy hard-coded score text remains parity-only and
cannot become a NumericalFact, ManagementStatement, or ModelInterpretation.

## Annual Guidance Progression

Each row contains Metric, Initial guide, Q1 guide, Q2 guide, Q3 guide, Q4 guide,
Actual, Status, and Notes/source. A quarter bucket is derived from reviewed periods or
an explicit reviewed product-plan override; workbook position is never authority.
Origin, update, reaffirmation, supersession, withdrawal, basis change, range, bound,
approximation, and unavailable states remain distinct. A Q4 period is never accepted
as a full-year Actual.

Every override owns the exact version digest and reviewed fiscal reporting-event
identity. After applying overrides, the projection replays the complete
`initial -> Q1 -> Q2 -> Q3 -> Q4` sequence: knowledge and publication dates cannot
decrease without an explicit correction, version-chain position cannot move backward,
and each successor must descend from its displayed predecessor. A later version cannot
be copied into an earlier bucket or cross a series, horizon, definition, or basis.

## Open Guidance

Each row contains Metric, Current guide, Horizon, Status, and Notes/source. The
governing version is the unique eligible terminal node in explicit version relations
for the same metric, definition, basis, dimensions, unit, and horizon. A newest-only,
first-match, source-order, fuzzy-text, or workbook-row fallback is forbidden. Ambiguous
or conflicting governing state is `Needs Review`.

## Quarterly Revision Timeline

Each row contains Metric, Previous guide, New/current guide, Change type, Actual,
Progress, Status, Horizon, Stated in, Source date, and Source/note. GuidanceVersion and
PromiseVersion histories use explicit predecessor and relation identities. Deterministic
order is business order, knowledge/publication date, effective period, version identity.
Material origin, target increase/decrease, update, reaffirmation, withdrawal, basis
change, and unresolved comparison remain visible without duplicates.

## Actual

Actual is the canonical source-backed observation for one explicit requested metric,
definition, basis, unit, dimensions, and period at the row as-of. Closed Actual roles
cover fiscal-year, quarter, YTD, cumulative, milestone, and labelled composite outcomes.
Each binding must assert the role's closed semantic class plus known definition, basis,
and/or period-type constraints. The projection replays those constraints over the
selected records and serializes the closed semantic class with the Actual selection.
Supported forms are exact, approximate, range, bound, percentage, qualitative
milestone, and typed missing. Blank is not zero; approximate is not exact; Q4 is not FY;
GAAP and adjusted definitions are not interchangeable. Selection requires exactly one
accepted CanonicalResolution or a deterministic missing/conflicting state.

A milestone Actual may additionally carry only a reviewed `completed`, `in_progress`,
`not_started`, `failed`, `withdrawn`, or `unknown` state. The state retains exact source
text, source occurrence, method, knowledge date, horizon, review state, and lineage.
Changing the source text, horizon, or selected evidence invalidates stale state; prose
alone is never classified by substring matching.

## Progress

Progress is a display role, not a universal numeric value. The closed registry contains:

- fiscal-year actual;
- year-to-date actual;
- cumulative actual;
- annualized run rate;
- realized-period amount;
- identified or initiated amount;
- remaining amount;
- delta to target;
- milestone state;
- directional qualitative progress.

Every Progress value retains canonical input IDs, semantic identity, period/horizon,
method, as-of date, display text, review state, and evidence lineage. Run rate cannot be
shown as realized savings, period values cannot be relabelled cumulative, gross/net
bases cannot be bridged silently, and unsupported completion percentages are forbidden.
The reviewed Progress assertion is validated against the closed role/semantic-class
registry before any selected input may be displayed.

`remaining-amount` is a calculation, never a relabel: for a compatible upward-monotonic
point or minimum target it is `max(target_floor - observed_progress, 0)`.
`delta-to-target` is the signed `observed - target` difference for a compatible point
target. Both retain the governing target version plus observed input and evidence.
Ranges without a reviewed floor, maximums, approximate targets without tolerance,
qualitative values, missing targets, and incompatible unit/basis/dimensions fail closed.

## Status

Status is recomputed by one of twelve closed rules: point target, range, minimum bound,
maximum bound, approximate target, cumulative target, annualized/run-rate target,
date/milestone, qualitative commitment, active guidance, basis composite, and
conflicting/insufficient evidence. The approved visible labels are Completed, Hit,
Beat, On track, Open, Mixed, Missed, Basis-dependent, Needs Review, and Withdrawn.

Each assessment retains the rule, canonical inputs, governing target version, Actual or
Progress role, as-of date, result, review state, issues, and explanation. A copied legacy
label or producer-stored comparability flag is non-authoritative. Missing direction,
tolerance, deadline, basis bridge, or compatible evidence fails closed.

Milestone `Completed` requires accepted reviewed state `completed`, eligible knowledge,
compatible horizon/deadline, and no conflicting accepted evidence. Merely selecting a
qualitative Actual is insufficient. Unknown, missing, stale, or conflicting state is
`Needs Review`; explicit failure/withdrawal and reviewed open-horizon states use only
their corresponding closed rules.

## Temporal contract

Effective/fiscal period, event date, publication date, knowledge date, and UI as-of date
are separate. Every displayed field requires `knowledge_date <= ui_as_of_date`.
Historical rows use their event-specific as-of; current blocks use the product as-of.
Later final Actuals, corrections, and guidance versions cannot leak into earlier rows.

## Stable row keys and shadow lineage

Every visible row has a versioned `row_id` derived only from product identity, block,
and typed economic business key. Enumeration and dictionary order are excluded. In a
future workbook phase M and N remain blank and unwritable; O may contain only this
`row_id`. Full lineage stays in the shadow matrix described by
`promise_progress_shadow_projection.schema.json`.

Every visible field has exactly one shadow entry with destination, display value,
canonical records, version/target, Actual and Progress selections, Status assessment,
semantic identity, period/horizon, dates, source documents and occurrences, review
issues, parity exception, method, lineage state, and digest. Visible rows and shadow
fields are created in one pass from the same immutable product.

The standalone shadow schema closes block IDs, block-specific field roles, destinations,
semantic identities, dates, and every machine-value form. A separate deterministic
cross-reference replay verifies row ownership, block/role/destination compatibility,
record/evidence/selection references, exact field coverage, and field/row/root digests.
Recomputing a root digest cannot legitimize a stale or semantically misplaced field.

## Parity and capacity

Parity authorization separates immutable scope, observation, reviewed binding, and policy
authorities. Independently pinned manifests fix the frozen legacy capture and the complete
source-native row/field scope. A complete reviewed disposition graph partitions every row
on both sides exactly once as `paired`, `legacy_only`, or `source_native_only`. Runtime
counterpart search and a closed reason classifier replay each disposition without trusting
its stored reason. Field differences are independently classified before their reviewed
bindings are considered. Structural conditions likewise require explicit observations and
bindings. Exactly one type-compatible, versioned policy must authorize each field binding,
row disposition, or structural binding and its own authorization digest. Global activation,
a binding-supplied reason, automatic matching failure, or a root-report digest is never
authorization. The first shadow retains the ANF `Promise_Progress_UI` geometry and fixed
row capacities. Capacity overflow fails before any later writer is invoked; no truncation,
row insertion, or block overlap is allowed.

The empty legacy tracker, lossy support matrices, and fuzzy hidden trace keys are observed
structural differences rather than visible-value exceptions. Each active structural policy
must have one current reviewed binding, and its product, sheet, block, condition, reason,
class, and binding-ID scope must equal its observed use exactly. The fuzzy trace condition
is limited to the timeline rows where the legacy O-column slugs exist; the two product-wide
ownership conditions retain all four blocks. In particular, the lossy-support-matrix policy
cannot authorize a visible field mismatch or a row disposition merely because it is active.
Field, row-disposition, and structural policies have disjoint scope fields; wildcards and
unused authorization atoms are forbidden.

For ANF, parity is calculated from a frozen field matrix identified by workbook SHA, sheet,
stable legacy row ID, reviewed business key, block, row type, semantic identity, role,
destination, display value, and structural classification. A separately pinned capture
manifest fixes the ordered row set, destination set, per-row field inventories, counts, and
matrix digest. A source-scope manifest independently fixes product/block identity and every
source-native row, business key, typed identity, field role, destination, and inventory
digest. Deleting, adding, replacing, or silently reclassifying a row therefore fails even if
a local parity digest is recomputed.

Each row disposition retains exact row/business identities, typed counterpart signature,
field-inventory digests, independently replayed counterpart-search result, closed reason,
review owner, duration, policy identity/version, and authorization digest. A one-sided row
is valid only when the full opposite-side scope contains no compatible typed counterpart.
No row may disappear, appear twice, move from a pair to one-sided ownership, or become
one-sided merely because matching failed. Same-cell collisions do not imply semantic
equivalence; reviewed destination remaps retain both destinations and exact row identities.

Each comparable field is classified as exact, normalized semantic match, registered and
authorized exception, unauthorized binding, unregistered difference, mapping defect,
legacy-only, source-native-only, or structurally incomparable. Authorization replays both
the observed comparison digest and an authorization digest covering the selected policy,
independently derived reason, and complete reviewed binding scope. Zero or multiple
authorizing policy scopes fail. The report exposes field, row-disposition, structural, and
capture-completeness results separately. Unused bindings, missing dispositions, counterpart
conflicts, and unused or overbroad active policy definitions are blocking and reported.
Products without a full legacy oracle report `not-declared`, never an automatic clean
comparison. Accepted bindings are pinned reviewed configuration; the comparator never
generates or writes them during product construction.

## Serialization

Products and shadow matrices use UTF-8, LF, sorted-key canonical JSON with no generated
timestamp and no floating-point values. Source and collection order cannot affect bytes.
The accepted longitudinal-memory package remains authoritative and is not mutated.
