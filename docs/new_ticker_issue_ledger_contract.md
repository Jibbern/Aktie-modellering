# New-Ticker Canonical Issue Ledger

The issue ledger is the authoritative QA contract between normalized-data
validation, legacy/source adapters, binding planning, and workbook QA views.
It is ticker-neutral and exists before Excel rendering.

## Identity And Deduplication

Each issue receives a deterministic `issue_id` derived from rule, issue type,
section, canonical normalized path, business row key, binding, evidence
identity, root cause, and normalized message. Numeric collection indexes are
canonicalized only when evidence identity still distinguishes the underlying
row. Exact or semantically equivalent copies collapse into one issue; distinct
business keys, evidence keys, source excerpts, or unit-review fields remain
distinct.

Actual source identity remains part of canonical identity even when an
`evidence_key` is present. Reusing one evidence key across different documents,
locations, periods, or business rows does not merge those issues. A deliberate
multi-source synthesis may group contributing sources only when it supplies an
explicit synthesis identifier and the complete ordered set of contributing
`source_ref` values; every source-level occurrence is still retained.

Every input record remains in `occurrences` with its original detail payload and
a stable linked `occurrence_id`. Deduplication therefore changes presentation,
not evidence retention.

`canonical_issue_key` is a grouping hint, not the complete identity. A key may
correlate one planner issue with its matching mapping gap only when the planner
explicitly marks the key trusted and its embedded binding, normalized path, and
business row key match those records. Caller keys
never erase period, source/evidence, business-row, binding, path, or root-cause
boundaries. The ledger groups a validated planner event into one issue while
retaining every child occurrence. No rule-specific rewriting or pre-ledger gap
deduplication is allowed.

## Categories

- `actionable_exception`
- `text_quality_demotion`
- `adapter_truncation_metadata`
- `duplicate_evidence`
- `unit_normalization_review`
- `planner_mapping_gap`
- `planner_overflow`
- `validation_failure`
- `audit_only_information`

P0/P1 issues always block rendering and promotion and appear in `Needs_Review`.
An explicit `promotion_blocking` or `render_blocking` flag also blocks the final
plan even when the issue is stored as a mapping gap or manual-review occurrence.
Routine text
demotions, adapter selection metadata, and exact duplicate evidence remain
JSON/audit-only. P2 normalization or mapping questions remain actionable unless
explicitly classified audit-only.

## Workbook Presentation

`QA_Log` receives one summary row per canonical `issue_id`, including occurrence
count and a JSON detail reference. `Needs_Review` receives only unresolved
issues with `visibility_disposition=needs_review`. `QA_Checks` receives one
aggregate row per issue `rule_id` plus an explicit row for every executed
generic validation stage. Successful stages are retained as `PASS`; stages
not run because a prerequisite failed are `INFO`, never a false `PASS`.
Each row carries unique, occurrence, actionable, and blocking counts.

Workbook capacity never limits JSON detail. Any QA overflow remains an explicit
planner gap/overflow record; the complete ledger is still serialized in the
binding-plan report. QA presentation is planned once from the completed ledger.
If QA planning itself creates an issue, no stale QA writes are retained and the
plan fails with `qa_presentation_snapshot_unstable`.
