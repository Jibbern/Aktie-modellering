# PBI Source-Native Longitudinal Memory Proof

## Purpose and boundary

This bounded C3 proof demonstrates that the C1/C2 source-native path generalizes
beyond ANF without becoming a PBI workbook adapter. It reads exactly 18 declared,
hash-pinned local sources, builds temporary typed candidates through the generic
adapter and business-services sector pack, and projects them into the unchanged C1
contract. It does not feed the normalized package or any workbook product.

The source set is
`tests/fixtures/longitudinal_memory/pbi_source_set.v1.json`; the deterministic
semantic oracle is
`tests/fixtures/longitudinal_memory/pbi_source_adapter_expected.v1.json`. External
SEC, issuer, transcript and reviewed-metadata bytes remain outside Git and are never
modified by this path.

## Authority and roles

SEC primary filings and exhibits retain accession-backed filing authority. Issuer
releases, CEO letters and PDFs retain issuer authority. The May 22 and July 1, 2024
official-page snapshots are reviewed immutable PDF captures with canonical issuer
URLs, not SEC filings. The Q2 2026 transcript is the raw speech source; metadata
revision 2 is a reviewed locator/index whose fields must replay against exact raw
transcript lines. It supplies no independent economic evidence.

Each source is opened once, hashed from one immutable byte snapshot and parsed only
from that snapshot. HTML/PDF/TXT locators and Inline-XBRL fact, context and unit
locators replay before mapping. Publication dates, accessions, origin links and
reviewed links are role-specific; source priority never suppresses a direct conflict.

## Semantic boundaries

The shared adapter owns discovery, immutable bytes, generic roles and locators,
calendar candidates, orchestration and C1 projection. The business-services pack
owns reusable metric/definition/basis/unit bindings and derivations. The PBI profile
owns only company/CIK/publisher identities, SendTech and Presort aliases, activated
bindings, reviewed links and `rule:core:calendar-year-fiscal@1`.

PBI's Q1 and Q2 2026 periods are exact calendar quarters. Canonical C1 comparison
therefore accepts the 90-day to 91-day Presort reported-rate movement from -8% to
-5% as +3 percentage points while independently replaying dates, ordinals,
dimensions and economics. No duration tolerance or ticker branch is used.

Adjusted segment EBIT excludes pension expense associated with terminated plans in
the accepted Q1/Q2 2026 definition. The Q2 2025 comparative EBIT values are retained
as source-backed observations under their earlier definition but are not exposed as
YoY ChangeObservations. Segment margins are transparent derived facts from selected
revenue and adjusted-EBIT records; their compound evidence identifies both immutable
inputs.

## Histories and epistemic separation

The cost-savings Promise begins with the May 22, 2024 $60-$100 million target. The
July $120-$160 million statement is an explicit target update; later issuer releases
form a deterministic chain through Q1 2025. Reaffirmation and updates are distinct,
there is exactly one active governing target, no deadline is invented, and annualized
run-rate evidence does not become realized savings.

Revenue guidance and the definition-compatible adjusted-EBIT guidance series retain
explicit replacements and reaffirmations. The earlier pension-treatment-ambiguous
adjusted-EBIT guide remains a separate series. Transcript-backed numerical facts,
management statements, the June refinancing CompanyEvent and review issues remain
separate record classes. Analyst-introduced claims are not issuer statements. No
ModelInterpretation is created because no separately accepted reviewed artifact
exists.

## Acceptance surface

The proof requires all 18 source hashes, strict schema and semantic validation,
complete locator replay, canonical calendar-year ChangeObservations, deterministic
reverse/shuffle output and a byte-pinned golden. Expected exclusions and ambiguity
are represented as nonblocking P2 Needs Review issues; blocked adjusted-EBIT
comparisons stay blocked. Runtime sidecars may be written only under pytest
temporary directories.
