"""Deterministic source-native projection into the canonical C1 package."""
from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from pbi_xbrl.longitudinal_memory.changes import derive_percentage_point_change
from pbi_xbrl.longitudinal_memory.identity import (
    build_identity,
    company_event_identity,
    evidence_occurrence_identity,
    guidance_series_identity,
    guidance_version_identity,
    identity_digest,
    management_statement_identity,
    model_interpretation_identity,
    numerical_business_key,
    numerical_fact_identity,
    promise_identity,
    promise_version_identity,
    relation_identity,
    source_document_identity,
)
from pbi_xbrl.longitudinal_memory.reconciliation import resolve_observations
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.validation import validate_package

from .discovery import discover_sources, load_source_set, verify_reviewed_model_inputs
from .html import extract_html_evidence, replay_html_dateline
from .mapping import map_candidates
from .pdf import extract_pdf_evidence, replay_pdf_dateline
from .periods import reconcile_periods, reviewed_calendar_rule_id
from .spreadsheet import extract_spreadsheet_evidence
from .text import extract_text_evidence
from .types import (
    AdapterBuildResult,
    AdapterIssue,
    DiscoveredDocument,
    ExtractedEvidence,
    MappedCandidate,
    MappingError,
    SourceAdapterError,
    SourceSet,
)


SCHEMA_VERSION = "1.0.0"
ELIGIBLE_REVIEW_STATES = frozenset({"accepted", "reviewed"})
REVIEW_RANK = {"accepted": 0, "reviewed": 0, "needs_review": 1, "rejected": 2}


def _effective_review_state(*states: str) -> str:
    worst = max(states, key=lambda value: REVIEW_RANK[value])
    if REVIEW_RANK[worst] > 0:
        return worst
    return "reviewed" if "reviewed" in states else "accepted"


def _document_knowledge_date(source_set: SourceSet, document: DiscoveredDocument) -> str:
    if document.spec.publication_date_basis != "reviewed-same-event-link":
        return document.spec.publication_date
    matches = [
        row
        for row in source_set.reviewed_links
        if row.get("relation_type") == "same-event"
        and row.get("from_document_key") == document.spec.document_key
        and row.get("review_state") in {"accepted", "reviewed"}
    ]
    if len(matches) != 1:
        raise MappingError(
            f"Document {document.spec.document_key!r} lacks one accepted knowledge-date link."
        )
    return str(matches[0]["knowledge_date"])


def _source_documents(
    source_set: SourceSet,
    documents: tuple[DiscoveredDocument, ...],
) -> tuple[dict[str, Any], ...]:
    by_key = {row.spec.document_key: row for row in documents}
    result: list[dict[str, Any]] = []
    for row in documents:
        origin_id = None
        if row.spec.origin_document_key is not None:
            origin = by_key.get(row.spec.origin_document_key)
            if origin is None:
                raise MappingError(f"Unknown origin document {row.spec.origin_document_key!r}.")
            origin_id = origin.source_document_id
        result.append(
            {
                "source_document_id": row.source_document_id,
                "identity_digest": identity_digest(row.source_document_id),
                "schema_version": SCHEMA_VERSION,
                "company_id": source_set.company_id,
                "publisher_id": row.spec.publisher_id,
                "document_type": row.spec.document_type,
                "publication_date": row.spec.publication_date,
                "document_key": row.spec.document_key,
                "revision": row.spec.revision,
                "origin_document_id": origin_id,
                "title": row.spec.document_key.replace("-", " ").title(),
                "source_path_hint": row.spec.relative_path.replace("\\", "/"),
                "canonical_url": row.spec.canonical_url,
                "content_sha256": row.content_sha256,
                "authority_class": row.spec.authority_class,
                "review_state": row.spec.review_state,
            }
        )
    return tuple(sorted(result, key=lambda value: value["source_document_id"]))


def _extract(
    source_set: SourceSet,
    documents: tuple[DiscoveredDocument, ...],
) -> tuple[ExtractedEvidence, ...]:
    assertions_by_document: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for assertion in source_set.required_assertions:
        assertions_by_document[str(assertion["document_key"])].append(assertion)
    result: list[ExtractedEvidence] = []
    for document in sorted(documents, key=lambda row: row.spec.document_key):
        embedded_date = document.spec.embedded_publication_date
        if embedded_date is not None:
            if document.spec.source_family == "sec-exhibit":
                replayed_date = replay_html_dateline(document)
            elif document.spec.source_family == "issuer-pdf":
                replayed_date = replay_pdf_dateline(document)
            else:  # pragma: no cover - the role matrix rejects this before discovery
                raise MappingError(
                    f"Source family {document.spec.source_family!r} cannot replay a dateline."
                )
            if replayed_date != embedded_date:
                raise MappingError(
                    f"Embedded publication date changed for {document.spec.document_key!r}."
                )
            if (
                document.spec.publication_date_basis == "embedded-dateline"
                and replayed_date != document.spec.publication_date
            ):
                raise MappingError(
                    f"Publication date disagrees with embedded evidence for {document.spec.document_key!r}."
                )
        assertions = assertions_by_document.get(document.spec.document_key, [])
        if document.spec.source_family == "sec-exhibit":
            result.extend(extract_html_evidence(document, assertions))
        elif document.spec.source_family == "issuer-pdf":
            result.extend(extract_pdf_evidence(document, assertions))
        elif document.spec.source_family == "issuer-spreadsheet":
            result.extend(extract_spreadsheet_evidence(document, assertions))
        elif document.spec.source_family == "issuer-transcript":
            result.extend(extract_text_evidence(document, assertions))
        else:  # pragma: no cover - source schema is closed
            raise MappingError(f"Unknown source family {document.spec.source_family!r}.")
    produced = {row.assertion_key for row in result}
    required = {str(row["assertion_key"]) for row in source_set.required_assertions}
    if produced != required:
        raise MappingError(
            f"Extraction cardinality mismatch: missing={sorted(required - produced)}, "
            f"unexpected={sorted(produced - required)}."
        )
    return tuple(sorted(result, key=lambda row: (row.assertion_key, row.locator_key)))


def _evidence_occurrences(
    source_set: SourceSet,
    documents: tuple[DiscoveredDocument, ...],
    evidence: tuple[ExtractedEvidence, ...],
) -> tuple[dict[str, Any], ...]:
    by_key = {row.spec.document_key: row for row in documents}
    result: list[dict[str, Any]] = []
    for extracted in evidence:
        document = by_key[extracted.document_key]
        review_state = _effective_review_state(
            document.spec.review_state,
            extracted.review_state,
        )
        occurrence_id = evidence_occurrence_identity(
            company_id=source_set.company_id,
            document_key=document.spec.document_key,
            document_revision=document.spec.revision,
            locator_kind=extracted.locator_kind,
            locator_key=extracted.locator_key,
            ordinal=extracted.ordinal,
        )
        result.append(
            {
                "evidence_occurrence_id": occurrence_id,
                "identity_digest": identity_digest(occurrence_id),
                "schema_version": SCHEMA_VERSION,
                "company_id": source_set.company_id,
                "source_document_id": document.source_document_id,
                "occurrence_key": extracted.assertion_key,
                "locator_kind": extracted.locator_kind,
                "locator_key": extracted.locator_key,
                "ordinal": extracted.ordinal,
                "excerpt": extracted.excerpt,
                "review_state": review_state,
            }
        )
    return tuple(sorted(result, key=lambda row: row["evidence_occurrence_id"]))


def _header(
    *,
    record_id: str,
    record_type: str,
    company_id: str,
    subject_id: str,
    publication_date: str | None,
    knowledge_date: str,
    period: Mapping[str, Any],
    dimension_set_id: str,
    assertion_mode: str,
    occurrence_id: str,
    review_state: str,
    fiscal_period: bool = True,
    confidence: str | None = None,
) -> dict[str, Any]:
    return {
        "record_id": record_id,
        "identity_digest": identity_digest(record_id),
        "record_type": record_type,
        "schema_version": SCHEMA_VERSION,
        "company_id": company_id,
        "subject_id": subject_id,
        "publication_date": publication_date,
        "knowledge_date": knowledge_date,
        "effective_period_id": period["period_id"],
        "fiscal_period_id": period["period_id"] if fiscal_period else None,
        "period_type": period["period_type"],
        "dimension_set_id": dimension_set_id,
        "assertion_mode": assertion_mode,
        "evidence_occurrence_ids": [occurrence_id],
        "review_state": review_state,
        "confidence": confidence,
    }


def _relation(
    relation_type: str,
    source_id: str,
    target_id: str,
    rule_id: str,
    evidence_ids: Iterable[str] = (),
) -> dict[str, Any]:
    relation_id = relation_identity(
        relation_type=relation_type,
        from_record_id=source_id,
        to_record_id=target_id,
        rule_id=rule_id,
    )
    return {
        "relation_id": relation_id,
        "identity_digest": identity_digest(relation_id),
        "schema_version": SCHEMA_VERSION,
        "relation_type": relation_type,
        "from_record_id": source_id,
        "to_record_id": target_id,
        "rule_id": rule_id,
        "evidence_occurrence_ids": sorted(set(evidence_ids)),
    }


def _review_issue(
    *,
    rule_id: str,
    business_key: str,
    entity_ids: Iterable[str],
    candidate_ids: Iterable[str],
    evidence_ids: Iterable[str],
    message: str,
    action: str,
    severity: str = "P2",
) -> dict[str, Any]:
    issue_id = build_identity(
        "review",
        (("rule", rule_id.replace("_", "-")), ("business", business_key)),
    )
    return {
        "issue_id": issue_id,
        "severity": severity,
        "rule_id": rule_id,
        "entity_ids": sorted(set(entity_ids)),
        "business_key": business_key,
        "message": message,
        "evidence_occurrence_ids": sorted(set(evidence_ids)),
        "candidate_record_ids": sorted(set(candidate_ids)),
        "suggested_action": action,
        "promotion_blocking": severity in {"P0", "P1"},
        "review_state": "needs_review",
    }


def _merge_identity_rows(rows: Iterable[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        identity = str(row[key])
        prior = result.get(identity)
        if prior is not None and prior != row:
            raise MappingError(f"Identity {identity!r} has conflicting immutable payloads.")
        result[identity] = row
    return [result[identity] for identity in sorted(result)]


def _promise_semantic_key(candidate: MappedCandidate) -> tuple[Any, ...]:
    return (
        candidate.metadata["promise_subject_id"],
        candidate.metadata["program_id"],
        candidate.metadata["target_metric_id"],
        candidate.metadata["target_definition_id"],
        candidate.metadata["target_basis_id"],
        candidate.metadata["target_dimension_alias"],
    )


def _validate_promise_candidate_chain(
    candidates: list[MappedCandidate],
) -> MappedCandidate:
    by_assertion = {row.assertion_key: row for row in candidates}
    if len(by_assertion) != len(candidates):
        raise MappingError("Promise assertion keys must be unique.")
    origins = [row for row in candidates if row.metadata["change_kind"] == "origin"]
    origins_by_key: dict[tuple[Any, ...], list[MappedCandidate]] = defaultdict(list)
    for origin in origins:
        if origin.metadata["previous_assertion_key"] is not None:
            raise MappingError("Promise origin cannot have a predecessor.")
        origins_by_key[_promise_semantic_key(origin)].append(origin)

    matched_origin: dict[str, MappedCandidate] = {}
    for candidate in candidates:
        if candidate.metadata["change_kind"] == "origin":
            matched_origin[candidate.assertion_key] = candidate
            continue
        compatible = origins_by_key.get(_promise_semantic_key(candidate), [])
        if not compatible:
            raise MappingError(
                f"Mandatory Needs Review: promise version {candidate.assertion_key!r} has no "
                "compatible subject, program and target origin."
            )
        if len(compatible) != 1:
            raise MappingError(
                f"Mandatory Needs Review: promise version {candidate.assertion_key!r} has multiple "
                "compatible origins."
            )
        origin = compatible[0]
        matched_origin[candidate.assertion_key] = origin
        predecessor_key = candidate.metadata["previous_assertion_key"]
        predecessor = by_assertion.get(str(predecessor_key))
        if predecessor is None:
            raise MappingError(
                f"Promise version {candidate.assertion_key!r} has no explicit predecessor."
            )
        if _promise_semantic_key(predecessor) != _promise_semantic_key(candidate):
            raise MappingError(
                f"Promise version {candidate.assertion_key!r} has a predecessor in another promise."
            )
        if (
            candidate.metadata["change_kind"] != "deadline_update"
            and candidate.period_key != origin.period_key
        ):
            raise MappingError(
                f"Promise version {candidate.assertion_key!r} changes its deadline without a deadline update."
            )

    for candidate in candidates:
        if candidate.metadata["change_kind"] == "origin":
            continue
        expected_origin = matched_origin[candidate.assertion_key]
        cursor = candidate
        seen: set[str] = set()
        while cursor.metadata["change_kind"] != "origin":
            if cursor.assertion_key in seen:
                raise MappingError("Promise predecessor chains cannot contain cycles.")
            seen.add(cursor.assertion_key)
            predecessor = by_assertion.get(str(cursor.metadata["previous_assertion_key"]))
            if predecessor is None:
                raise MappingError(
                    f"Promise version {cursor.assertion_key!r} has no explicit predecessor."
                )
            cursor = predecessor
        if cursor is not expected_origin:
            raise MappingError(
                f"Promise version {candidate.assertion_key!r} does not reach its compatible origin."
            )

    if len(origins) != 1:
        raise MappingError("The bounded promise projection requires exactly one deterministic origin.")
    return origins[0]


def _project(
    source_set: SourceSet,
    discovered: tuple[DiscoveredDocument, ...],
    extracted: tuple[ExtractedEvidence, ...],
    candidates: tuple[MappedCandidate, ...],
    *,
    profile: Any,
    pack: Any,
) -> dict[str, Any]:
    external_source_documents = list(_source_documents(source_set, discovered))
    source_by_key = {
        row.spec.document_key: (row, next(value for value in external_source_documents if value["source_document_id"] == row.source_document_id))
        for row in discovered
    }
    evidence_occurrences = list(_evidence_occurrences(source_set, discovered, extracted))
    occurrence_by_assertion = {row["occurrence_key"]: row for row in evidence_occurrences}
    extracted_by_assertion = {row.assertion_key: row for row in extracted}
    evidence_pairs = {
        key: (occurrence_by_assertion[key], extracted_by_assertion[key])
        for key in occurrence_by_assertion
    }

    periods = list(
        reconcile_periods(
            source_set,
            evidence_pairs,
            calendar_id=profile.calendar_id,
        )
    )
    period_by_key = {
        str(raw["period_key"]): next(row for row in periods if row["period_id"] == raw["period_id"])
        for raw in source_set.periods
    }
    fiscal_calendar = {
        "calendar_id": profile.calendar_id,
        "calendar_rule_id": reviewed_calendar_rule_id(source_set),
        "company_id": source_set.company_id,
        "profile_hint": profile.calendar_hint,
        "week_pattern": "source-declared",
        "coverage_state": "partial",
        "evidence_occurrence_ids": sorted(
            {
                occurrence_by_assertion[str(raw["evidence_assertion_key"])][
                    "evidence_occurrence_id"
                ]
                for raw in source_set.periods
            }
        ),
        "reconciliation_state": "reconciled",
    }
    dimension_sets = pack.dimension_sets(profile.member_aliases)
    total_dimension_id = dimension_sets[pack.total_dimension_alias][0]

    methods = [
        {
            "method_id": str(row["method_id"]),
            "producer_id": str(row["producer_id"]),
            "description": "Reviewed source-native model interpretation method.",
        }
        for row in source_set.reviewed_model_inputs
    ]
    catalog = pack.catalog(dimension_sets, profile.member_aliases, methods)
    activated = set(profile.activated_metric_ids)

    observations: list[dict[str, Any]] = []
    entities: list[dict[str, Any]] = []
    relations: list[dict[str, Any]] = []
    resolutions: list[dict[str, Any]] = []
    review_issues: list[dict[str, Any]] = []
    record_by_assertion: dict[str, dict[str, Any]] = {}

    numerical_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in (row for row in candidates if row.candidate_kind == "numerical_fact"):
        metric_id, definition_id, basis_id, unit_id = pack.metric_semantics(candidate.semantic_key)
        if metric_id not in activated:
            raise MappingError(f"Metric {metric_id!r} is not activated by the ticker profile.")
        period = period_by_key[str(candidate.period_key)]
        alias = " ".join(str(candidate.dimension_alias).split()).casefold()
        if alias not in dimension_sets:
            raise MappingError(f"Unknown or ambiguous dimension alias {candidate.dimension_alias!r}.")
        dimension_set_id = dimension_sets[alias][0]
        occurrence = occurrence_by_assertion[candidate.assertion_key]
        document, _ = source_by_key[candidate.document_key]
        review_state = _effective_review_state(
            str(candidate.metadata["review_state"]),
            candidate.evidence.review_state,
            document.spec.review_state,
        )
        business_key = numerical_business_key(
            company_id=source_set.company_id,
            metric_id=metric_id,
            definition_id=definition_id,
            basis_id=basis_id,
            period_id=period["period_id"],
            dimension_set_id=dimension_set_id,
            unit_id=unit_id,
            currency=None,
        )
        record_id = numerical_fact_identity(
            provenance_key=occurrence["evidence_occurrence_id"],
            company_id=source_set.company_id,
            metric_id=metric_id,
            definition_id=definition_id,
            basis_id=basis_id,
            period_id=period["period_id"],
            dimension_set_id=dimension_set_id,
            unit_id=unit_id,
            currency=None,
        )
        record = {
            "header": _header(
                record_id=record_id,
                record_type="NumericalFact",
                company_id=source_set.company_id,
                subject_id=metric_id,
                publication_date=document.spec.publication_date,
                knowledge_date=_document_knowledge_date(source_set, document),
                period=period,
                dimension_set_id=dimension_set_id,
                assertion_mode=pack.assertion_mode(candidate.semantic_key),
                occurrence_id=occurrence["evidence_occurrence_id"],
                review_state=review_state,
            ),
            "payload": {
                "kind": "NumericalFact",
                "business_key": business_key,
                "metric_id": metric_id,
                "definition_id": definition_id,
                "basis_id": basis_id,
                "unit_id": unit_id,
                "currency": None,
                "value": dict(candidate.value or {}),
            },
        }
        observations.append(record)
        record_by_assertion[candidate.assertion_key] = record
        numerical_groups[business_key].append(record)

    for business_key in sorted(numerical_groups):
        result = resolve_observations(
            numerical_groups[business_key],
            policy_id="policy:core:reported-numerical@1",
            as_of_date=source_set.knowledge_cutoff,
            source_documents=external_source_documents,
            evidence_occurrences=evidence_occurrences,
        )
        resolutions.append(result.resolution)
        relations.extend(result.inferred_relations)
        review_issues.extend(result.review_issues)

    guidance_candidates = [row for row in candidates if row.candidate_kind == "guidance"]
    guidance_groups: dict[tuple[str, str], list[MappedCandidate]] = defaultdict(list)
    for candidate in guidance_candidates:
        guidance_groups[(str(candidate.period_key), candidate.semantic_key)].append(candidate)
    for group_key in sorted(guidance_groups):
        horizon_key, metric_key = group_key
        metric_id, definition_id, basis_id, unit_id = pack.metric_semantics(metric_key, guidance=True)
        if metric_id not in activated:
            raise MappingError(f"Metric {metric_id!r} is not activated by the ticker profile.")
        period = period_by_key[horizon_key]
        series_id = guidance_series_identity(
            company_id=source_set.company_id,
            metric_id=metric_id,
            definition_id=definition_id,
            basis_id=basis_id,
            horizon_period_id=period["period_id"],
            dimension_set_id=total_dimension_id,
            unit_id=unit_id,
            currency=None,
        )
        entities.append(
            {
                "header": {
                    "entity_id": series_id,
                    "identity_digest": identity_digest(series_id),
                    "entity_type": "GuidanceSeries",
                    "schema_version": SCHEMA_VERSION,
                    "company_id": source_set.company_id,
                    "evidence_occurrence_ids": [],
                },
                "payload": {
                    "kind": "GuidanceSeries",
                    "metric_id": metric_id,
                    "definition_id": definition_id,
                    "basis_id": basis_id,
                    "horizon_period_id": period["period_id"],
                    "dimension_set_id": total_dimension_id,
                    "unit_id": unit_id,
                    "currency": None,
                },
            }
        )
        records: list[dict[str, Any]] = []
        for candidate in sorted(guidance_groups[group_key], key=lambda row: row.assertion_key):
            occurrence = occurrence_by_assertion[candidate.assertion_key]
            document, _ = source_by_key[candidate.document_key]
            record_id = guidance_version_identity(
                guidance_series_id=series_id,
                occurrence_id=occurrence["evidence_occurrence_id"],
            )
            review_state = _effective_review_state(
                str(candidate.metadata["review_state"]),
                candidate.evidence.review_state,
                document.spec.review_state,
            )
            record = {
                "header": _header(
                    record_id=record_id,
                    record_type="GuidanceVersion",
                    company_id=source_set.company_id,
                    subject_id=series_id,
                    publication_date=document.spec.publication_date,
                    knowledge_date=_document_knowledge_date(source_set, document),
                    period=period,
                    dimension_set_id=total_dimension_id,
                    assertion_mode="guided",
                    occurrence_id=occurrence["evidence_occurrence_id"],
                    review_state=review_state,
                ),
                "payload": {
                    "kind": "GuidanceVersion",
                    "guidance_series_id": series_id,
                    "version_kind": candidate.metadata["version_kind"],
                    "value": dict(candidate.value or {}),
                    "wording": candidate.evidence.excerpt,
                },
            }
            records.append(record)
            observations.append(record)
            record_by_assertion[candidate.assertion_key] = record

        explicit: list[dict[str, Any]] = []
        for candidate in guidance_groups[group_key]:
            predecessor_key = candidate.metadata["supersedes_assertion_key"]
            if predecessor_key is None:
                continue
            predecessor = next(
                (row for row in guidance_groups[group_key] if row.assertion_key == predecessor_key),
                None,
            )
            if predecessor is None:
                raise MappingError(
                    f"Guidance {candidate.assertion_key!r} supersedes an unknown or cross-series assertion."
                )
            if dict(candidate.metadata["previous_value"] or {}) != dict(predecessor.value or {}):
                raise MappingError(
                    f"Guidance previous column for {candidate.assertion_key!r} does not replay its predecessor."
                )
            source_record = record_by_assertion[candidate.assertion_key]
            target_record = record_by_assertion[predecessor.assertion_key]
            explicit.append(
                _relation(
                    "supersedes",
                    source_record["header"]["record_id"],
                    target_record["header"]["record_id"],
                    "rule:core:guidance-explicit-replacement@1",
                    source_record["header"]["evidence_occurrence_ids"],
                )
            )
        ordered_candidates = sorted(
            guidance_groups[group_key],
            key=lambda row: (
                source_by_key[row.document_key][0].spec.publication_date,
                row.assertion_key,
            ),
        )
        for index, candidate in enumerate(ordered_candidates[1:], start=1):
            if candidate.metadata["supersedes_assertion_key"] is not None:
                continue
            prior_values = {
                tuple(sorted(dict(prior.value or {}).items()))
                for prior in ordered_candidates[:index]
            }
            current_value = tuple(sorted(dict(candidate.value or {}).items()))
            if current_value not in prior_values:
                raise MappingError(
                    "A changed guidance value requires explicit replacement evidence; chronology cannot supersede."
                )
        result = resolve_observations(
            records,
            policy_id="policy:core:guidance@1",
            as_of_date=source_set.knowledge_cutoff,
            source_documents=external_source_documents,
            evidence_occurrences=evidence_occurrences,
            relations=explicit,
        )
        relations.extend(explicit)
        relations.extend(result.inferred_relations)
        resolutions.append(result.resolution)
        review_issues.extend(result.review_issues)

    promise_candidates = sorted(
        (row for row in candidates if row.candidate_kind == "promise_version"),
        key=lambda row: row.assertion_key,
    )
    promise_id: str | None = None
    promise_version_ids: dict[str, str] = {}
    if promise_candidates:
        origin = _validate_promise_candidate_chain(promise_candidates)
        origin_occurrence = occurrence_by_assertion[origin.assertion_key]
        promise_id = promise_identity(
            company_id=source_set.company_id,
            subject_id=str(origin.metadata["promise_subject_id"]),
            program_id=origin.metadata["program_id"],
            origin_occurrence_id=origin_occurrence["evidence_occurrence_id"],
        )
        for candidate in promise_candidates:
            occurrence = occurrence_by_assertion[candidate.assertion_key]
            promise_version_ids[candidate.assertion_key] = promise_version_identity(
                promise_id=promise_id,
                occurrence_id=occurrence["evidence_occurrence_id"],
            )
        deadline_period = period_by_key[str(origin.period_key)]
        deadline = {
            "kind": "period",
            "value": deadline_period["period_id"],
            "precision": "fiscal-period",
        }
        original_wording = origin.evidence.excerpt
        entities.append(
            {
                "header": {
                    "entity_id": promise_id,
                    "identity_digest": identity_digest(promise_id),
                    "entity_type": "Promise",
                    "schema_version": SCHEMA_VERSION,
                    "company_id": source_set.company_id,
                    "evidence_occurrence_ids": [origin_occurrence["evidence_occurrence_id"]],
                },
                "payload": {
                    "kind": "Promise",
                    "promise_subject_id": origin.metadata["promise_subject_id"],
                    "program_id": origin.metadata["program_id"],
                    "origin_occurrence_id": origin_occurrence["evidence_occurrence_id"],
                    "origin_version_id": promise_version_ids[origin.assertion_key],
                    "original_wording": original_wording,
                    "original_target": dict(origin.value or {}),
                    "original_baseline": None,
                    "original_deadline": deadline,
                },
            }
        )
        promise_records: dict[str, dict[str, Any]] = {}
        for candidate in promise_candidates:
            predecessor_key = candidate.metadata["previous_assertion_key"]
            if candidate.metadata["change_kind"] == "reaffirmation" and dict(candidate.value or {}) != dict(origin.value or {}):
                raise MappingError("A promise reaffirmation cannot silently change the source target.")
            occurrence = occurrence_by_assertion[candidate.assertion_key]
            document, _ = source_by_key[candidate.document_key]
            period = period_by_key[str(candidate.period_key)]
            record_id = promise_version_ids[candidate.assertion_key]
            record = {
                "header": _header(
                    record_id=record_id,
                    record_type="PromiseVersion",
                    company_id=source_set.company_id,
                    subject_id=promise_id,
                    publication_date=document.spec.publication_date,
                    knowledge_date=_document_knowledge_date(source_set, document),
                    period=period,
                    dimension_set_id=total_dimension_id,
                    assertion_mode="stated",
                    occurrence_id=occurrence["evidence_occurrence_id"],
                    review_state=_effective_review_state(
                        str(candidate.metadata["review_state"]),
                        candidate.evidence.review_state,
                        document.spec.review_state,
                    ),
                ),
                "payload": {
                    "kind": "PromiseVersion",
                    "promise_id": promise_id,
                    "previous_version_id": promise_version_ids.get(str(predecessor_key)),
                    "change_kind": candidate.metadata["change_kind"],
                    "version_state": candidate.metadata["version_state"],
                    "wording": original_wording if candidate.metadata["change_kind"] == "reaffirmation" else candidate.evidence.excerpt,
                    "target": dict(candidate.value or {}),
                    "baseline": None,
                    "deadline": {
                        "kind": "period",
                        "value": period["period_id"],
                        "precision": "fiscal-period",
                    },
                },
            }
            promise_records[candidate.assertion_key] = record
            observations.append(record)
            record_by_assertion[candidate.assertion_key] = record
        for candidate in promise_candidates:
            predecessor_key = candidate.metadata["previous_assertion_key"]
            if candidate.metadata["change_kind"] == "reaffirmation" and predecessor_key is not None:
                relations.append(
                    _relation(
                        "reaffirms",
                        promise_records[candidate.assertion_key]["header"]["record_id"],
                        promise_records[str(predecessor_key)]["header"]["record_id"],
                        "rule:core:promise-reaffirmation@1",
                        promise_records[candidate.assertion_key]["header"]["evidence_occurrence_ids"],
                    )
                )

    statement_records: list[dict[str, Any]] = []
    for candidate in (row for row in candidates if row.candidate_kind == "management_statement"):
        occurrence = occurrence_by_assertion[candidate.assertion_key]
        document, _ = source_by_key[candidate.document_key]
        period = period_by_key[str(candidate.period_key)]
        statement_id = management_statement_identity(
            company_id=source_set.company_id,
            statement_kind=str(candidate.metadata["statement_kind"]),
            topic_id=str(candidate.metadata["topic_id"]),
            period_id=period["period_id"],
            speaker_id=str(candidate.metadata["speaker_id"]),
            occurrence_id=occurrence["evidence_occurrence_id"],
        )
        record = {
            "header": _header(
                record_id=statement_id,
                record_type="ManagementStatement",
                company_id=source_set.company_id,
                subject_id=str(candidate.metadata["topic_id"]),
                publication_date=document.spec.publication_date,
                knowledge_date=_document_knowledge_date(source_set, document),
                period=period,
                dimension_set_id=total_dimension_id,
                assertion_mode="stated",
                occurrence_id=occurrence["evidence_occurrence_id"],
                review_state=_effective_review_state(
                    str(candidate.metadata["review_state"]),
                    candidate.evidence.review_state,
                    document.spec.review_state,
                ),
            ),
            "payload": {
                "kind": "ManagementStatement",
                "statement_kind": candidate.metadata["statement_kind"],
                "topic_id": candidate.metadata["topic_id"],
                "statement_period_id": period["period_id"],
                "speaker_id": candidate.metadata["speaker_id"],
                "statement": candidate.evidence.excerpt,
            },
        }
        observations.append(record)
        record_by_assertion[candidate.assertion_key] = record
        statement_records.append(record)

    event_records: list[dict[str, Any]] = []
    for candidate in (row for row in candidates if row.candidate_kind == "company_event"):
        occurrence = occurrence_by_assertion[candidate.assertion_key]
        document, _ = source_by_key[candidate.document_key]
        period = period_by_key[str(candidate.period_key)]
        event_id = company_event_identity(
            company_id=source_set.company_id,
            event_type=str(candidate.metadata["event_type"]),
            event_subject_id=str(candidate.metadata["event_subject_id"]),
            event_stage=str(candidate.metadata["event_stage"]),
            effective_period_id=period["period_id"],
            occurrence_id=occurrence["evidence_occurrence_id"],
        )
        record = {
            "header": _header(
                record_id=event_id,
                record_type="CompanyEvent",
                company_id=source_set.company_id,
                subject_id=str(candidate.metadata["event_subject_id"]),
                publication_date=document.spec.publication_date,
                knowledge_date=_document_knowledge_date(source_set, document),
                period=period,
                dimension_set_id=total_dimension_id,
                assertion_mode="stated",
                occurrence_id=occurrence["evidence_occurrence_id"],
                review_state=_effective_review_state(
                    str(candidate.metadata["review_state"]),
                    candidate.evidence.review_state,
                    document.spec.review_state,
                ),
                fiscal_period=False,
            ),
            "payload": {
                "kind": "CompanyEvent",
                "event_type": candidate.metadata["event_type"],
                "event_subject_id": candidate.metadata["event_subject_id"],
                "event_stage": candidate.metadata["event_stage"],
                "description": candidate.evidence.excerpt,
                "effective_date": None,
                "effective_month": str(period["start_date"])[:7],
                "effective_precision": candidate.metadata["effective_precision"],
            },
        }
        observations.append(record)
        record_by_assertion[candidate.assertion_key] = record
        event_records.append(record)

    for records, policy_id in (
        (statement_records, "policy:core:management-explanation@1"),
        (event_records, "policy:core:company-event@1"),
    ):
        for record in records:
            result = resolve_observations(
                [record],
                policy_id=policy_id,
                as_of_date=source_set.knowledge_cutoff,
                source_documents=external_source_documents,
                evidence_occurrences=evidence_occurrences,
            )
            resolutions.append(result.resolution)
            relations.extend(result.inferred_relations)
            review_issues.extend(result.review_issues)

    selected_by_candidate: dict[str, str] = {}
    for resolution in resolutions:
        selected = resolution.get("selected_record_id")
        if selected is None:
            continue
        for candidate_id in resolution.get("candidate_record_ids", ()):
            selected_by_candidate[str(candidate_id)] = str(selected)

    model_records: list[dict[str, Any]] = []
    for model_input in sorted(source_set.reviewed_model_inputs, key=lambda row: str(row["input_key"])):
        publication_date = str(model_input["knowledge_date"])
        document_key = str(model_input["input_key"])
        model_document_id = source_document_identity(
            company_id=source_set.company_id,
            publisher_id="internal-research",
            document_type="model-review",
            publication_date=publication_date,
            document_key=document_key,
            revision=int(model_input["revision"]),
        )
        model_document = {
            "source_document_id": model_document_id,
            "identity_digest": identity_digest(model_document_id),
            "schema_version": SCHEMA_VERSION,
            "company_id": source_set.company_id,
            "publisher_id": "internal-research",
            "document_type": "model-review",
            "publication_date": publication_date,
            "document_key": document_key,
            "revision": int(model_input["revision"]),
            "origin_document_id": None,
            "title": "Reviewed model interpretation input",
            "source_path_hint": str(model_input["source_ref"]),
            "canonical_url": None,
            "content_sha256": str(model_input["source_content_sha256"]),
            "authority_class": "reviewed-model",
            "review_state": "reviewed",
        }
        external_source_documents.append(model_document)
        locator_key = str(model_input["source_ref"])
        occurrence_id = evidence_occurrence_identity(
            company_id=source_set.company_id,
            document_key=document_key,
            document_revision=int(model_input["revision"]),
            locator_kind="normalized-path",
            locator_key=locator_key,
            ordinal=1,
        )
        occurrence = {
            "evidence_occurrence_id": occurrence_id,
            "identity_digest": identity_digest(occurrence_id),
            "schema_version": SCHEMA_VERSION,
            "company_id": source_set.company_id,
            "source_document_id": model_document_id,
            "occurrence_key": document_key,
            "locator_kind": "normalized-path",
            "locator_key": locator_key,
            "ordinal": 1,
            "excerpt": str(model_input["interpretation"]),
            "review_state": "reviewed",
        }
        evidence_occurrences.append(occurrence)

        selected_inputs: list[str] = []
        for assertion_key in model_input["input_assertion_keys"]:
            referenced = record_by_assertion.get(str(assertion_key))
            if referenced is None:
                raise MappingError(f"Model input references unknown assertion {assertion_key!r}.")
            candidate_id = str(referenced["header"]["record_id"])
            selected_id = selected_by_candidate.get(candidate_id)
            if selected_id is None:
                raise MappingError(f"Model input assertion {assertion_key!r} has no selected canonical record.")
            selected_inputs.append(selected_id)
        selected_inputs = sorted(set(selected_inputs))
        if len(selected_inputs) != len(model_input["input_assertion_keys"]):
            raise MappingError("Reviewed model input collapses to duplicate canonical records.")
        period = period_by_key[str(model_input["as_of_period_key"])]
        interpretation_id = model_interpretation_identity(
            company_id=source_set.company_id,
            interpretation_key=document_key,
            as_of_period_id=period["period_id"],
            method_id=str(model_input["method_id"]),
            producer_id=str(model_input["producer_id"]),
            input_record_ids=selected_inputs,
            revision=int(model_input["revision"]),
        )
        record = {
            "header": _header(
                record_id=interpretation_id,
                record_type="ModelInterpretation",
                company_id=source_set.company_id,
                subject_id=document_key,
                publication_date=publication_date,
                knowledge_date=publication_date,
                period=period,
                dimension_set_id=total_dimension_id,
                assertion_mode="interpreted",
                occurrence_id=occurrence_id,
                review_state="reviewed",
            ),
            "payload": {
                "kind": "ModelInterpretation",
                "interpretation_key": document_key,
                "as_of_period_id": period["period_id"],
                "method_id": model_input["method_id"],
                "producer_id": model_input["producer_id"],
                "input_record_ids": selected_inputs,
                "revision": int(model_input["revision"]),
                "interpretation": model_input["interpretation"],
                "authority_class": "reviewed-model",
            },
        }
        observations.append(record)
        model_records.append(record)
        for input_id in selected_inputs:
            relations.append(
                _relation(
                    "interprets",
                    interpretation_id,
                    input_id,
                    "rule:core:model-interpretation-input@1",
                    [occurrence_id],
                )
            )

    for record in model_records:
        result = resolve_observations(
            [record],
            policy_id="policy:core:model-interpretation@1",
            as_of_date=source_set.knowledge_cutoff,
            source_documents=external_source_documents,
            evidence_occurrences=evidence_occurrences,
        )
        resolutions.append(result.resolution)
        relations.extend(result.inferred_relations)
        review_issues.extend(result.review_issues)

    selected_numerical: dict[tuple[str, str, str], dict[str, Any]] = {}
    observations_by_id = {row["header"]["record_id"]: row for row in observations}
    for resolution in resolutions:
        selected_id = resolution.get("selected_record_id")
        if selected_id is None:
            continue
        selected = observations_by_id.get(str(selected_id))
        if selected is None or selected["payload"]["kind"] != "NumericalFact":
            continue
        selected_numerical[
            (
                selected["payload"]["metric_id"],
                selected["header"]["effective_period_id"],
                selected["header"]["dimension_set_id"],
            )
        ] = selected
    for change_kind, earlier, later, earlier_period, later_period in pack.percentage_point_change_requests(
        periods,
        selected_numerical,
        total_dimension_id=total_dimension_id,
        calendar=fiscal_calendar,
    ):
        observations.append(
            derive_percentage_point_change(
                earlier,
                later,
                earlier_period=earlier_period,
                later_period=later_period,
                earlier_calendar=fiscal_calendar,
                later_calendar=fiscal_calendar,
                change_kind=change_kind,
                rule_id=f"rule:core:{change_kind}@1",
                change_unit_id=pack.percentage_point_unit_id,
            )
        )

    if promise_id is not None:
        assessment = pack.promise_evidence_assessment(
            observations, ELIGIBLE_REVIEW_STATES
        )
        evidence_record = assessment["evidence_record"]
        relations.append(
            _relation(
                "evidences",
                evidence_record["header"]["record_id"],
                promise_id,
                assessment["relation_rule_id"],
                evidence_record["header"]["evidence_occurrence_ids"],
            )
        )
        origin_candidate = next(row for row in promise_candidates if row.metadata["change_kind"] == "origin")
        review_issues.append(
            _review_issue(
                rule_id=assessment["review_rule_id"],
                business_key=promise_id,
                entity_ids=[promise_id],
                candidate_ids=[
                    promise_version_ids[origin_candidate.assertion_key],
                    *[
                        row["header"]["record_id"]
                        for row in assessment["candidate_records"]
                    ],
                ],
                evidence_ids=[occurrence_by_assertion[origin_candidate.assertion_key]["evidence_occurrence_id"]],
                message=assessment["message"],
                action=assessment["action"],
            )
        )

    source_documents = _merge_identity_rows(external_source_documents, "source_document_id")
    evidence_occurrences = _merge_identity_rows(evidence_occurrences, "evidence_occurrence_id")
    entities = sorted(entities, key=lambda row: row["header"]["entity_id"])
    relations = _merge_identity_rows(relations, "relation_id")
    resolutions = _merge_identity_rows(resolutions, "resolution_id")
    review_issues = _merge_identity_rows(review_issues, "issue_id")
    blocking = any(row["severity"] in {"P0", "P1"} and row["promotion_blocking"] for row in review_issues)
    package = {
        "schema_id": "longitudinal-company-memory",
        "schema_version": SCHEMA_VERSION,
        "identity_contract_version": "1",
        "artifact_state": "needs_review" if blocking else "accepted",
        "company_id": source_set.company_id,
        "knowledge_cutoff": source_set.knowledge_cutoff,
        "normalized_package_ref": dict(source_set.normalized_package_ref),
        "catalog": catalog,
        "fiscal_calendars": [fiscal_calendar],
        "periods": sorted(periods, key=lambda row: row["period_id"]),
        "source_documents": source_documents,
        "evidence_occurrences": evidence_occurrences,
        "entities": entities,
        "observations": sorted(observations, key=lambda row: row["header"]["record_id"]),
        "relations": relations,
        "resolutions": resolutions,
        "review_issues": review_issues,
    }
    issues = validate_package(package)
    if issues:
        details = "; ".join(
            f"{issue.normalized_path} [{issue.rule_id}] {issue.message}" for issue in issues
        )
        raise SourceAdapterError(f"Projected C1 package failed unchanged semantic validation: {details}")
    return package


def build_source_native_sidecar(
    source_set_path: Path | str,
    *,
    source_root: Path | str,
    reviewed_model_root: Path | str,
    sector_pack: Any,
    ticker_profile_loader: Callable[[SourceSet], Any],
) -> AdapterBuildResult:
    """Build one validated sidecar in memory; callers own any tmp-path write."""

    source_set = load_source_set(source_set_path)
    verify_reviewed_model_inputs(source_set, reviewed_model_root)
    discovered = discover_sources(source_set, source_root)
    extracted = _extract(source_set, discovered)
    profile = ticker_profile_loader(source_set)
    candidates = map_candidates(
        source_set,
        extracted,
        sector_pack=sector_pack,
        ticker_profile=profile,
    )
    package = _project(
        source_set,
        discovered,
        extracted,
        candidates,
        profile=profile,
        pack=sector_pack,
    )
    payload = serialize_package(package)
    return AdapterBuildResult(
        source_set=source_set,
        documents=discovered,
        extracted_evidence=extracted,
        candidates=candidates,
        package=package,
        payload=payload,
        sidecar_sha256=hashlib.sha256(payload).hexdigest(),
        adapter_issues=tuple(
            AdapterIssue(
                severity=row["severity"],
                rule_id=row["rule_id"],
                subject=row["business_key"],
                message=row["message"],
                promotion_blocking=row["promotion_blocking"],
            )
            for row in package["review_issues"]
        ),
    )
