# Longitudinal Company Memory v1 contract

Status: first contract implementation pass. This sidecar is workbook-independent and is not yet an upstream package authority.

## Boundary and authority

The runtime artifact is `{TICKER}_longitudinal_company_memory.v1.json`. During this pass it is generated only under `pytest`'s `tmp_path`; no checked-in generated sidecar exists. `docs/longitudinal_company_memory.schema.json` is the closed Draft 2020-12 schema authority and semantic validation lives in `pbi_xbrl/longitudinal_memory/validation.py`.

The existing normalized package and workbook projection remain unchanged. `normalized_package_ref.semantic_snapshot_id` is a non-authoritative semantic identity over the complete normalized package after removing only the root `generated_at_utc`. A later cutover must either promote the sidecar to upstream package authority or delete it; no dual-authority compatibility layer is permitted.

The layer is lossless: source documents, evidence occurrences, typed records, relations, resolutions, and review issues remain separate. Operating Drivers, Promise Progress, Quarter Notes, and SUMMARY are future consumers and do not own truth or change detection here.

The shared engine owns generic identities, record semantics, validation, reconciliation, and change rules. Future sector packs may contribute versioned metric/definition/basis catalogs and explicitly selected trend rules, but cannot branch on a ticker. Ticker profiles may provide aliases, activation, and expected-calendar hints; they do not own generic economics, source precedence, or reconciliation.

## Identity contract

All readable identities use fixed field order:

`<kind>:v1|<key>=<RFC3986-percent-encoded-value>|...`

Before encoding, text is UTF-8 NFC, company IDs are uppercase, and semantic slugs are lowercase kebab-case. Values use RFC 3986 percent encoding with only `A-Z a-z 0-9 - . _ ~` left unescaped. A stored checksum accompanies every readable record identity:

`sha256-160:<lowercase unpadded base32 of the first 20 SHA-256 bytes of the readable identity>`

The readable identity is authoritative. A digest is only an integrity/index checksum and cannot be used alone to select or reconcile. A digest collision, a wrong digest, or one readable identity associated with different immutable payload is a P1 failure. Schema identity changes require a new identity-contract version; existing v1 identities remain historical and are never reinterpreted.

Exact v1 formats are:

- SourceDocument: `doc:v1|co=<company>|publisher=<publisher slug>|type=<document type slug>|pub=<publication date>|key=<document key slug>|rev=<positive source revision>`.
- EvidenceOccurrence: `occ:v1|co=<company>|doc=<document key slug>|rev=<source revision>|loc=<locator kind>:<immutable locator key>|n=<positive ordinal>`.
- NumericalFact business key: `business-fact:v1|co=<company>|metric=<metric_id>|def=<definition_id>|basis=<basis_id>|period=<period_id>|dims=<dimension_set_id>|unit=<unit_id>|ccy=<ISO currency or na>`.
- NumericalFact: `fact:v1|business=<NumericalFact business key>|prov=<immutable evidence occurrence identity or explicit derivation provenance key>`.
- GuidanceSeries: `gseries:v1|co=<company>|metric=<metric_id>|def=<definition_id>|basis=<basis_id>|horizon=<period_id>|dims=<dimension_set_id>|unit=<unit_id>|ccy=<ISO currency or na>`.
- GuidanceVersion: `gver:v1|series=<GuidanceSeries identity>|occ=<immutable evidence occurrence identity>`.
- Promise: `promise:v1|co=<company>|subject=<promise subject identity>|program=<program identity or na>|origin=<origin evidence occurrence identity>`.
- PromiseVersion: `pver:v1|promise=<Promise identity>|occ=<immutable evidence occurrence identity>`.
- ManagementStatement: `statement:v1|co=<company>|kind=<statement kind>|topic=<topic identity>|period=<statement period identity>|speaker=<speaker identity>|occ=<immutable evidence occurrence identity>`.
- CompanyEvent: `event:v1|co=<company>|type=<event type>|subject=<event subject identity>|stage=<event stage>|effective=<effective period identity>|occ=<immutable evidence occurrence identity>`.
- ModelInterpretation: `interp:v1|co=<company>|key=<interpretation key>|asof=<as-of period>|method=<versioned method_id>|producer=<producer identity>|inputs=<digest of sorted unique input record identities>|rev=<explicit positive revision>`.
- AvailabilityObservation: `availability:v1|co=<company>|business=<unavailable business key>|state=<unavailable, not-disclosed, or not-applicable>|occ=<evidence occurrence identity>`.
- Relation: `relation:v1|type=<relation type>|from=<source record identity>|to=<target record identity>|rule=<versioned rule_id>`.
- CanonicalResolution: `resolution:v1|type=<record type>|business=<business key>|asof=<knowledge cutoff date>|policy=<versioned policy_id>|candidates=<digest of sorted unique eligible candidate identities>`.
- ChangeObservation: `change:v1|co=<company>|kind=<change kind>|from=<selected earlier NumericalFact>|to=<selected later NumericalFact>|rule=<versioned change rule_id>`.

Values, targets, wording, display labels, aliases, review state, confidence, and source input order never participate in identity. A value-bearing fact or version is instead anchored to immutable evidence, so a correction creates a distinct record and an explicit relation.

`PromiseVersion.previous_version_id` is required lineage but is deliberately excluded from the readable identity. The origin has `null`; every later version points to the immediately preceding version of the same Promise. Evidence occurrences carry their own review state so selection can fail closed across record -> occurrence -> source document without changing occurrence identity.

## Catalog and semantic versioning

Catalog IDs are independently versioned:

- `metric:<namespace>:<slug>@N`
- `definition:<namespace>:<slug>@N`
- `basis:<namespace>:<slug>@N`
- `unit:<namespace>:<slug>@N`
- `dimension:<namespace>:<slug>@N`
- `member:<namespace>:<scope>:<dimension>:<slug>@N`

A semantic change creates a new `@N`; aliases, spelling, or display changes do not. Historical records retain their original IDs. In particular, GAAP and adjusted definitions, reported and ex-policy-credit bases, run-rate and realized savings, and revised company definitions cannot share an identity.

A dimension set contains one or more `(dimension_id, member_id)` pairs, sorted by `dimension_id`, with at most one member per axis. Its identity encodes the sorted complete member mapping. Company total is an explicit member and an empty dimension set is invalid.

## Time, availability, units, and values

Ticker-profile calendar declarations are expected-calendar hints only. Filing or source evidence owns actual period start, end, inclusive duration, week count, fiscal ordinal, and 52/53-week state. Calendar and evidence must reconcile before acceptance.

QoQ requires adjacent ordinals and adjacent source-backed dates. YoY requires the same fiscal-quarter ordinal in the next fiscal year. Both require matching calendar, period type, and duration. Ambiguity, overlap, a gap in a declared-complete calendar, 52/53-week mismatch, unsafe YTD subtraction, non-adjacent QoQ, wrong-quarter YoY, or incomplete four-quarter TTM fails closed. This runtime contains no month-shift or default-calendar fallback.

Numerical values are canonical decimal strings, never JSON floats. Exact, approximate, range, bound, and qualitative values are closed and remain distinct. NumericalFact excludes qualitative values. Observed zero is the exact value `"0"`. Missing means no observation exists. Explicit `unavailable`, `not-disclosed`, and `not-applicable` require an AvailabilityObservation and may not be encoded as zero or an empty string.

Numerical comparison requires identical company, metric, definition, basis, unit, currency, and dimension-set identities. No unit, basis, dimension, or period conversion is implicit.

## Reconciliation

Reconciliation is order-independent and policy-specific:

- `policy:core:reported-numerical@1`
- `policy:core:guidance@1`
- `policy:core:management-explanation@1`
- `policy:core:company-event@1`
- `policy:core:model-interpretation@1`

The fixed stages are schema and semantic eligibility, business-key grouping, duplicate/corroboration relations, explicit correction/supersession graph, assertion-specific maximal candidates, exact/approximate compatibility, and cardinality validation. A stored resolution retains the complete candidate set, the eligible subset, maximal candidates, selection cardinality, state, deterministic reason codes, and linked review issues; semantic validation independently replays all of them. Historical as-of resolutions may coexist, but every represented resolution group also requires a current resolution at `knowledge_cutoff`, and only current-cutoff selections can support accepted changes or reviewed interpretations. Input position, file enumeration order, and a universal source rank are never selection criteria.

Same-origin mirrors are duplicates, not independent corroboration. Independent sources with the same assertion corroborate. Stored duplicate, corroboration, and contradiction relations must themselves match deterministic evidence-and-assertion replay. An explicit correction or supersession edge removes its target from the terminal set while preserving it historically; every valid target is demoted independently of relation enumeration order, so a multi-edge chain cannot strand an intermediate version. Malformed, backward, unsupported, or cyclic history edges never demote a candidate. Exact values dominate compatible approximate/range/bound records only when all semantic identities match and the exact value falls inside the stated tolerance or bounds. An approximation without a tolerance cannot prove target achievement. Equal-authority incompatible terminal candidates produce an unresolved P1 CanonicalResolution and a Needs Review issue.

Incompatible candidates remain linked by a `contradicts` relation even when assertion-specific authority can select one of them. Compatible but different non-exact terminal values do not receive an arbitrary tie-break; absent an exact dominating value or explicit history edge, they remain unresolved.

Reported facts, management explanations, and model interpretations have different closed record types and policies. A reviewed interpretation cannot overwrite or masquerade as a reported fact.

## Promise history

A Promise is anchored to its origin occurrence. The origin PromiseVersion's wording, target, baseline, and deadline are copied into immutable `original_*` fields on the Promise entity.

For an explicitly matched later statement:

- no changed original field is a reaffirmation;
- only target changed is a target update;
- only deadline changed is a deadline update;
- withdrawal requires an explicit withdrawn state;
- multiple field changes, changed wording, or changed baseline are a reformulation;
- a different subject/program is a fundamentally new promise.

Supersession is an explicit relation. Silence never means withdrawal. A source-backed unmatched promise update remains a `ManagementStatement` with `statement_kind=commitment`; its `topic_id` supplies the promise subject for deterministic candidate matching. Zero or multiple possible promise matches create Needs Review and no `PromiseVersion` is invented. Original wording and later reformulations remain concurrently visible in history.

`version_state` is checked against both intrinsic change kind and the validated history graph: current origins/updates are active, current reaffirmations are reaffirmed, explicit withdrawals are withdrawn, and only a version targeted by a valid supersession edge may be superseded.

## Change observations

The first pass derives only QoQ and YoY percentage-point changes from already selected, exact, compatible NumericalFacts. Each change-rule catalog row declares `input_unit_kind=percent` and its exact percentage-point `output_unit_id`. A ChangeObservation retains the two input IDs, sorted input IDs, versioned rule, exact comparability result, derived exact value, and output unit. Incompatible periods, semantics, units, or values fail closed. Percentage change (when used by a future rule) also fails closed for a zero denominator. No turnaround score exists.

## Review and product ownership

All unresolved canonical cardinality, conflicting equal-authority candidates, identity/digest defects, ambiguous periods, unsafe conversions, missing evidence, promise-match ambiguity, or invalid references create P1 Needs Review. Semantic acceptance derives the mandatory P0/P1 condition set independently, requires exact rule/business/candidate coverage in `review_issues`, and requires `artifact_state=needs_review` whenever that set is non-empty. A producer cannot make an artifact accepted by omitting a required issue, and stale blocking issues cannot manufacture blocked state. Non-blocking audit observations may be P2. The existing `build_canonical_issue_ledger` projects these issues into the current QA/Needs Review vocabulary; the sidecar remains the detailed contract authority.

Operating Drivers will consume numerical history and derived changes. Promise Progress will consume immutable promise/version history and evidence. Quarter Notes will consume latest facts, management explanations, model interpretations, and implications as separate types. SUMMARY may later show compact read-only references only.

## First-pass proof and exclusions

The curated ANF fixture proves dimensioned comparable-sales facts, explicit zero, guidance versions, a store-plan promise and later evidence, a management explanation, a company event, a reviewed interpretation, safe QoQ/YoY changes, duplication/corroboration, supersession, contradiction handling, and deterministic output. The accepted golden fixture contains no fabricated conflict; the APAC conflict is cloned only in memory by a negative test.

Workbook writes, bindings, formulas, templates, product UI migrations, SUMMARY changes, PBI/GPRE implementation, downloads, ticker-specific runtime branches, and compatibility wrappers are excluded.
