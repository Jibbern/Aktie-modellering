# Longitudinal Memory Source Adapter Contract

## Boundary

The source adapter is a workbook-independent, deterministic ingress path into the
unchanged v1 longitudinal-company-memory contract. It does not become a second
truth layer. Its temporary candidates either project into the C1 records and pass
the unchanged C1 schema, reconciliation and semantic validator, or the build stops.

The runtime flow is:

1. load one closed source-set document with the strict duplicate-key loader;
2. resolve only its declared root-relative files below an injected root;
3. open each source once, retain one immutable byte snapshot and verify its exact
   SHA-256 before creating a source identity;
4. replay document datelines and every format locator only against that verified
   snapshot; the source path is diagnostic after discovery;
5. verify reviewed model-input bytes below a separately injected root and replay
   their recorded acceptance date;
6. map evidence through a sector pack and a declarative ticker profile;
7. project evidence-anchored candidates into C1 records;
8. run C1 reconciliation, change derivation, validation and serialization.

No workbook, normalized-package producer, source refresh, manifest or product
consumer imports this package in C2.

## Source-set authority

`docs/longitudinal_memory_source_adapter_input.schema.json` is the Draft 2020-12
authority. Every object is closed. A source root is a caller-owned absolute path;
it is never persisted in the source set or used as an identity component.

`DocumentSpec` identity is the readable C1 SourceDocument identity over company,
publisher, document type, publication date, document key and declared revision.
The content hash verifies bytes but never replaces that readable identity. Reusing
one readable identity for different bytes is a P1 failure.

Absolute paths, traversal, globs, symlinks/reparse ambiguity, missing files,
case-ambiguous matches, wrong hashes, duplicate document keys, unknown source roles
and unresolved publication metadata fail before extraction. Existing refresh
manifest destination paths, mtime and directory order have no authority.

`source_family`, `document_type`, `authority_class` and
`publication_date_basis` form one closed document role. A role also determines
whether an accession, embedded-dateline locator, origin relationship or reviewed
link is required and which assertion policies it may support. External documents
must use the profile publisher. SEC accessions must match the profile CIK. A
transcript cannot claim filing authority, and a reviewed model input has the fixed
internal-research/reviewed-model role rather than producer-supplied issuer authority.

## Locator semantics

All locators carry a version, human-readable locator key, ordinal, extraction
method, bounded excerpt, excerpt SHA-256 and review state.

- HTML locators require a unique semantic table or text-node match. The node path,
  table index, row and cell positions are verified diagnostics combined with
  section, span, row and column fingerprints; no position is sufficient alone.
- PDF locators use the existing text layer and reproducible table extraction.
  Page, region, row and column fingerprints are required. Empty text is blocking;
  OCR is not attempted.
- XLSX locators require exact sheet and A1 coordinates, header fingerprints, cell
  type, number format, formula state and cached-value state. Workbooks are opened
  read-only from two separate `BytesIO` streams created from the same verified
  snapshot and are never recalculated or saved.
- TXT locators use one-based line ranges, an exact line digest and reproducible
  speaker/turn diagnostics. Filename dates have no semantic role.

Any change to source bytes, the format-specific extraction method, node/turn
diagnostics, selected headers, extracted text, number format, formula/cached state
or excerpt digest fails closed.

## Ownership

`source_adapter` owns generic contracts, discovery, format extraction, generic
period reconciliation, orchestration and C1 projection. It contains no company,
brand or geography literals and no ticker conditionals.

`sector_packs.retail` owns versioned retail metric IDs, unit/dimension concepts,
lossless percent/count/guidance parsing and the explicit net-openings arithmetic
rule. It also declares assertion-specific eligible source families and the retail
QoQ/YoY period-pair rules. It contains no issuer paths, dates, values, brands or
issuer identifiers.

`ticker_profiles.anf` validates the declarative profile carried by the source set:
company and publisher identity, CIK/host aliases, dimension-member aliases,
activated retail metrics, calendar hints and reviewed source/event links. It owns
no reported number, guidance range, precedence rule or generic calculation.

## Publication and periods

SEC exhibits use the SEC filed date as SourceDocument publication date; an embedded
issuer dateline remains separate adapter metadata and is replayed by a closed HTML
dateline locator. The filing accession remains curated SEC metadata and is not
inferred from exhibit text. Direct issuer releases use a closed PDF dateline locator
whose replayed date must exactly equal the declared publication date. Transcript
dates are accepted only through an explicit
reviewed link, never through a filename. The transcript retains the linked event's
publication date, while records extracted from it use the reviewed link's later
knowledge date so the adapter cannot backdate when that date relationship became
accepted. A reviewed same-event or event-date link must join distinct documents
with matching publication dates and, where both are declared, matching report
dates; an unrelated document cannot supply the date context.

Fiscal starts may be derived from an explicit end date and week count only through
`rule:core:inclusive-weeks-ending@1`; its week count and end date must replay from
the cited source occurrence and agree with the source document's report-period
linkage. Every HTML fiscal-label locator replays a bounded, NFC-normalized source
excerpt from the verified document snapshot. A closed generic grammar derives its
year, period type, quarter and specificity. The locator's declared `claim_kind` is
only an expected result; disagreement with the source-derived meaning fails before
period reconciliation.

`period_key` is the typed fiscal-evidence group. For each fiscal period the adapter
enumerates every extracted claim on every assertion linked to that group. The closed
`fiscal_claim_assertion_keys` list is checked for exact equality with the independently
enumerated eligible set and cannot select a subset. Claims retain their distinct
occurrences whether they are same-origin duplicates or independent corroboration;
no deduplication may remove a contradiction from eligibility analysis.
`evidence_assertion_key` identifies the primary duration/end-date locator only; it
has no authority to select or exclude fiscal-label claims.

The complete claim closure is reconciled atomically into exactly one compatible
fiscal-year/period-type/quarter/ordinal/start/end/duration/week-count/calendar tuple
before that tuple is compared with the declared period. Independently valid but
mutually contradictory claims fail closed. Declared fiscal year, quarter, period
type and ordinal must agree with the reconciled source tuple and with the versioned
reviewed calendar rule. The reviewed rule binds its display hint, permitted
52/53-week durations, fiscal-year-end window and ordinal anchor. A profile hint may
corroborate direct labels but cannot replace or contradict them.

A reviewed expected full-year guidance horizon uses
`rule:core:reviewed-calendar-horizon@1`. Its closed authority entry names the
source-backed anchor period and
`rule:core:contiguous-reviewed-fiscal-horizon@1`, and fixes the calendar, fiscal
year/type/ordinal, exact contiguous start and end, duration and week count. A month
window or another otherwise valid 52-week period is not equivalent. The reviewed
rule cannot relabel a direct Full Year Outlook as a quarter. A source phrase such as
`this month` may
resolve to a month only when an accepted reviewed document-date/event link is
present; the exact month boundaries are derived from that linked document date and
must match the declared period. Calendar hints corroborate and cannot override
source evidence.

Ambiguous publication dates, missing period evidence, conflicting duration,
unsafe 52/53-week comparison, nonadjacent QoQ and wrong-quarter YoY fail closed.

## Mapping and history

Every evidence occurrence becomes at most one evidence-anchored assertion record.
Exact, approximate, range and bound forms remain distinct; no midpoint is created.
Observed zero is an exact zero. Negative store closures remain negative.

Supersession is emitted only from a declared source assertion whose explicit
replacement wording or reproducible current/previous columns replay against one
explicit predecessor. Chronology alone cannot create it. Same-origin repeated table
rows are duplicates; independent documents may corroborate only after source
eligibility is established. Transcript guidance additionally requires a reviewed
same-event link to its issuer release.

Promise versions use explicit predecessor keys and match an origin by company,
subject, explicit-null program, versioned target metric/definition/basis,
dimensional scope and applicable deadline. Zero or multiple compatible origins,
cross-promise predecessors and cycles fail closed as mandatory Needs Review rather
than being attached by a sole-origin or first-match shortcut. Reaffirmations must
preserve the origin wording, target, baseline and deadline. An approximate target without a
source tolerance is never assessed as achieved automatically. Store openings and
signed closures also produce a separately evidence-anchored derived net-opening
fact; the arithmetic result does not silently become a promise assessment.

Reported facts, issuer explanations, company events and reviewed model
interpretations remain different C1 record types. A reviewed model interpretation
uses its actual acceptance date and selected canonical fact/guidance inputs; it is
not issuer speech and is never backdated to the source quarter. Its bounded audit
file, content hash, unique interpretation text and acceptance timestamp are replayed
below the caller-injected reviewed-model root.

## Determinism and output

All document, evidence, candidate, relation and resolution collections are sorted
by readable identity before projection. Reversing or seeded-shuffling any input
collection must produce byte-identical C1 serialization. No filesystem enumeration,
source order, first/latest/best match, mtime or parser traversal order may select a
record.

The adapter returns bytes in memory. Tests may write the runtime sidecar only below
pytest `tmp_path`. It has no production write path and introduces no compatibility
wrapper or dual authority.
