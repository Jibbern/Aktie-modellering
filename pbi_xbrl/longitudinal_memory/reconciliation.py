"""Assertion-specific, source-order-invariant canonical reconciliation."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from .identity import (
    build_identity,
    canonical_resolution_identity,
    identity_digest,
    relation_identity,
)


POLICY_RECORD_TYPES: dict[str, frozenset[str]] = {
    "policy:core:reported-numerical@1": frozenset({"NumericalFact"}),
    "policy:core:guidance@1": frozenset({"GuidanceVersion"}),
    "policy:core:management-explanation@1": frozenset({"ManagementStatement"}),
    "policy:core:company-event@1": frozenset({"CompanyEvent"}),
    "policy:core:model-interpretation@1": frozenset({"ModelInterpretation"}),
}
POLICY_ASSERTION_TYPES: dict[str, str] = {
    "policy:core:reported-numerical@1": "reported-numerical",
    "policy:core:guidance@1": "guidance",
    "policy:core:management-explanation@1": "management-explanation",
    "policy:core:company-event@1": "company-event",
    "policy:core:model-interpretation@1": "model-interpretation",
}

AUTHORITY: dict[str, dict[str, int]] = {
    "policy:core:reported-numerical@1": {
        "audited-filing": 600,
        "filed-exhibit": 550,
        "company-release": 500,
        "company-presentation": 450,
        "company-transcript": 300,
        "accepted-normalized": 250,
    },
    "policy:core:guidance@1": {
        "filed-exhibit": 600,
        "company-release": 550,
        "company-presentation": 450,
        "company-transcript": 400,
        "audited-filing": 350,
    },
    "policy:core:management-explanation@1": {
        "company-transcript": 600,
        "company-release": 500,
        "filed-exhibit": 450,
        "company-presentation": 400,
        "audited-filing": 350,
    },
    "policy:core:company-event@1": {
        "audited-filing": 600,
        "filed-exhibit": 575,
        "company-release": 550,
        "company-presentation": 425,
        "company-transcript": 400,
    },
    "policy:core:model-interpretation@1": {
        "reviewed-model": 600,
        "accepted-normalized": 500,
        "model-generated": 200,
    },
}

ELIGIBLE_REVIEW_STATES = frozenset({"accepted", "reviewed"})
HISTORY_RELATION_TYPES = frozenset({"corrects", "supersedes"})


class ReconciliationError(ValueError):
    """Raised for a malformed reconciliation request, not a business conflict."""


@dataclass(frozen=True)
class ReconciliationResult:
    resolution: dict[str, Any]
    inferred_relations: tuple[dict[str, Any], ...]
    review_issues: tuple[dict[str, Any], ...]


def business_key(record: Mapping[str, Any]) -> str:
    payload = record.get("payload", {})
    header = record.get("header", {})
    if payload.get("business_key"):
        return str(payload["business_key"])
    kind = str(payload.get("kind") or header.get("record_type") or "")
    if kind == "GuidanceVersion":
        return str(payload.get("guidance_series_id", ""))
    if kind == "ManagementStatement":
        return "|".join(str(payload.get(key, "")) for key in ("statement_kind", "topic_id", "statement_period_id"))
    if kind == "CompanyEvent":
        return "|".join(
            [
                *(str(payload.get(key, "")) for key in ("event_type", "event_subject_id", "event_stage", "effective_precision")),
                str(header.get("effective_period_id", "")),
            ]
        )
    if kind == "ModelInterpretation":
        return "|".join(str(payload.get(key, "")) for key in ("interpretation_key", "as_of_period_id", "method_id", "revision"))
    raise ReconciliationError(f"Record {header.get('record_id')!r} has no canonical business key.")


def _value(record: Mapping[str, Any]) -> Mapping[str, Any] | None:
    payload = record.get("payload", {})
    value = payload.get("value")
    return value if isinstance(value, Mapping) else None


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _contains(container: Mapping[str, Any], exact: Decimal) -> bool:
    kind = container.get("kind")
    if kind == "exact":
        return exact == _decimal(container.get("value"))
    if kind == "approximate":
        tolerance = container.get("tolerance")
        return tolerance is not None and abs(exact - _decimal(container.get("value"))) <= _decimal(tolerance)
    if kind == "range":
        low, high = _decimal(container.get("low")), _decimal(container.get("high"))
        return (exact > low or (exact == low and bool(container.get("low_inclusive")))) and (
            exact < high or (exact == high and bool(container.get("high_inclusive")))
        )
    if kind == "bound":
        bound = _decimal(container.get("value"))
        return {
            "gt": exact > bound,
            "gte": exact >= bound,
            "lt": exact < bound,
            "lte": exact <= bound,
        }.get(str(container.get("operator")), False)
    return False


def values_compatible(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None) -> bool:
    """Return compatibility without collapsing exact, approximate or qualitative forms."""

    if left is None or right is None:
        return left == right
    left_kind, right_kind = left.get("kind"), right.get("kind")
    if left_kind == "exact":
        return _contains(right, _decimal(left.get("value")))
    if right_kind == "exact":
        return _contains(left, _decimal(right.get("value")))
    if left_kind == right_kind == "approximate":
        if left.get("tolerance") is None or right.get("tolerance") is None:
            return left.get("value") == right.get("value") and left.get("qualifier") == right.get("qualifier")
        left_low = _decimal(left.get("value")) - _decimal(left.get("tolerance"))
        left_high = _decimal(left.get("value")) + _decimal(left.get("tolerance"))
        right_low = _decimal(right.get("value")) - _decimal(right.get("tolerance"))
        right_high = _decimal(right.get("value")) + _decimal(right.get("tolerance"))
        return max(left_low, right_low) <= min(left_high, right_high)
    if left_kind == right_kind == "range":
        left_low, left_high = _decimal(left.get("low")), _decimal(left.get("high"))
        right_low, right_high = _decimal(right.get("low")), _decimal(right.get("high"))
        return max(left_low, right_low) <= min(left_high, right_high)
    if left_kind == right_kind == "bound":
        return left == right
    if left_kind == right_kind == "qualitative":
        left_band = left.get("normalized_band")
        right_band = right.get("normalized_band")
        return bool(left_band and right_band and left_band == right_band) or left.get("text") == right.get("text")
    return False


def _value_specificity(record: Mapping[str, Any]) -> int:
    kind = (_value(record) or {}).get("kind")
    return {"exact": 5, "range": 4, "bound": 3, "approximate": 2, "qualitative": 1}.get(str(kind), 0)


def _occurrence_documents(
    record: Mapping[str, Any],
    occurrences: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    for occurrence_id in record.get("header", {}).get("evidence_occurrence_ids", ()):
        occurrence = occurrences.get(str(occurrence_id))
        document = documents.get(str(occurrence.get("source_document_id"))) if occurrence else None
        if document is not None:
            result.append(document)
    return result


def _occurrence_ids_eligible(
    occurrence_ids: Sequence[str],
    *,
    company_id: str,
    occurrences: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
    policy_id: str | None = None,
    allowed_authority_classes: frozenset[str] | None = None,
) -> bool:
    if not occurrence_ids:
        return False
    allowed_authorities: Mapping[str, int] | frozenset[str] | None = AUTHORITY.get(policy_id, {}) if policy_id is not None else allowed_authority_classes
    for occurrence_id in occurrence_ids:
        occurrence = occurrences.get(str(occurrence_id))
        if occurrence is None or occurrence.get("review_state") not in ELIGIBLE_REVIEW_STATES:
            return False
        document = documents.get(str(occurrence.get("source_document_id", "")))
        if occurrence.get("company_id") != company_id:
            return False
        seen_documents: set[str] = set()
        while document is not None:
            document_id = str(document.get("source_document_id", ""))
            if document_id in seen_documents:
                return False
            seen_documents.add(document_id)
            if document.get("review_state") not in ELIGIBLE_REVIEW_STATES or document.get("company_id") != company_id:
                return False
            if allowed_authorities is not None and str(document.get("authority_class", "")) not in allowed_authorities:
                return False
            origin_id = document.get("origin_document_id")
            if origin_id is None or str(origin_id) == document_id:
                break
            document = documents.get(str(origin_id))
        if document is None:
            return False
    return True


def evidence_chain_eligible(
    record: Mapping[str, Any],
    *,
    policy_id: str | None,
    occurrences: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
    allowed_authority_classes: frozenset[str] | None = None,
) -> bool:
    """Return source eligibility across record -> occurrence -> document."""

    header = record.get("header", {})
    return _occurrence_ids_eligible(
        [str(value) for value in header.get("evidence_occurrence_ids", ())],
        company_id=str(header.get("company_id", "")),
        occurrences=occurrences,
        documents=documents,
        policy_id=policy_id,
        allowed_authority_classes=allowed_authority_classes,
    )


def _source_roots(
    documents_for_record: Sequence[Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
) -> frozenset[str]:
    roots: set[str] = set()
    for row in documents_for_record:
        current = row
        seen: set[str] = set()
        while True:
            document_id = str(current.get("source_document_id", ""))
            if document_id in seen:
                roots.add(document_id)
                break
            seen.add(document_id)
            origin_id = current.get("origin_document_id")
            if origin_id is None or str(origin_id) == document_id:
                roots.add(document_id)
                break
            next_document = documents.get(str(origin_id))
            if next_document is None:
                roots.add(str(origin_id))
                break
            current = next_document
    return frozenset(roots)


def _root_document(
    row: Mapping[str, Any], documents: Mapping[str, Mapping[str, Any]]
) -> Mapping[str, Any]:
    current = row
    seen: set[str] = set()
    while True:
        document_id = str(current.get("source_document_id", ""))
        if document_id in seen:
            return current
        seen.add(document_id)
        origin_id = current.get("origin_document_id")
        if origin_id is None or str(origin_id) == document_id:
            return current
        next_document = documents.get(str(origin_id))
        if next_document is None:
            return current
        current = next_document


def _authority(
    record: Mapping[str, Any],
    policy_id: str,
    occurrences: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
) -> int:
    if not evidence_chain_eligible(record, policy_id=policy_id, occurrences=occurrences, documents=documents):
        return -1
    if policy_id == "policy:core:model-interpretation@1":
        return AUTHORITY[policy_id].get(str(record.get("payload", {}).get("authority_class", "")), -1)
    source_rows = _occurrence_documents(record, occurrences, documents)
    if not source_rows:
        return -1
    return max(
        AUTHORITY[policy_id].get(str(_root_document(row, documents).get("authority_class", "")), -1)
        for row in source_rows
    )


def _eligible_history_relations(
    relations: Sequence[Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    occurrences: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    valid: list[Mapping[str, Any]] = []
    for relation in sorted(relations, key=lambda row: str(row.get("relation_id", ""))):
        if relation.get("relation_type") not in HISTORY_RELATION_TYPES:
            continue
        source_id = str(relation.get("from_record_id", ""))
        target_id = str(relation.get("to_record_id", ""))
        try:
            expected_relation_id = relation_identity(
                relation_type=str(relation.get("relation_type", "")),
                from_record_id=source_id,
                to_record_id=target_id,
                rule_id=str(relation.get("rule_id", "")),
            )
        except ValueError:
            continue
        if relation.get("relation_id") != expected_relation_id or relation.get("identity_digest") != identity_digest(expected_relation_id):
            continue
        source, target = candidates.get(source_id), candidates.get(target_id)
        if source is None or target is None or source_id == target_id:
            continue
        source_header, target_header = source.get("header", {}), target.get("header", {})
        if source_header.get("record_type") != target_header.get("record_type"):
            continue
        if source_header.get("company_id") != target_header.get("company_id"):
            continue
        try:
            if business_key(source) != business_key(target):
                continue
            if date.fromisoformat(str(source_header.get("knowledge_date"))) < date.fromisoformat(str(target_header.get("knowledge_date"))):
                continue
            source_publication = source_header.get("publication_date")
            target_publication = target_header.get("publication_date")
            if source_publication is not None and target_publication is not None and date.fromisoformat(str(source_publication)) < date.fromisoformat(str(target_publication)):
                continue
        except (ReconciliationError, ValueError):
            continue
        evidence_ids = [str(value) for value in relation.get("evidence_occurrence_ids", ())]
        if not set(evidence_ids).issubset(set(str(value) for value in source_header.get("evidence_occurrence_ids", ()))):
            continue
        if not _occurrence_ids_eligible(
            evidence_ids,
            company_id=str(source_header.get("company_id", "")),
            occurrences=occurrences,
            documents=documents,
        ):
            continue
        valid.append(relation)

    graph: dict[str, set[str]] = {}
    for relation in valid:
        graph.setdefault(str(relation["from_record_id"]), set()).add(str(relation["to_record_id"]))

    def reaches(start: str, target: str, seen: set[str]) -> bool:
        if start == target:
            return True
        if start in seen:
            return False
        seen.add(start)
        return any(reaches(child, target, seen) for child in graph.get(start, ()))

    cyclic_ids = {
        str(relation.get("relation_id", ""))
        for relation in valid
        if reaches(str(relation.get("to_record_id", "")), str(relation.get("from_record_id", "")), set())
    }
    return tuple(relation for relation in valid if str(relation.get("relation_id", "")) not in cyclic_ids)


def _signature(record: Mapping[str, Any]) -> str:
    payload = dict(record.get("payload", {}))
    for non_assertion_field in ("wording", "version_kind"):
        payload.pop(non_assertion_field, None)
    if payload.get("kind") == "CompanyEvent":
        payload.pop("description", None)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _assertions_compatible(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_value, right_value = _value(left), _value(right)
    if left_value is not None or right_value is not None:
        return left_value is not None and right_value is not None and values_compatible(left_value, right_value)
    return _signature(left) == _signature(right)


def _review_issue(
    *, rule_id: str, business_key_value: str, candidates: Sequence[str], message: str, severity: str = "P1"
) -> dict[str, Any]:
    candidate_digest = identity_digest(json.dumps(sorted(set(candidates)), separators=(",", ":")))
    issue_id = build_identity("review", (("rule", rule_id), ("business", business_key_value), ("candidates", candidate_digest)))
    return {
        "issue_id": issue_id,
        "severity": severity,
        "rule_id": rule_id,
        "entity_ids": [],
        "business_key": business_key_value,
        "message": message,
        "evidence_occurrence_ids": [],
        "candidate_record_ids": sorted(set(candidates)),
        "suggested_action": "Review the terminal candidates and add explicit correction or supersession evidence.",
        "promotion_blocking": severity in {"P0", "P1"},
        "review_state": "needs_review",
    }


def _inferred_relations(
    candidates: Sequence[Mapping[str, Any]],
    occurrences: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    ordered = sorted(candidates, key=lambda row: str(row.get("header", {}).get("record_id", "")))
    for index, left in enumerate(ordered):
        for right in ordered[index + 1 :]:
            if business_key(left) != business_key(right):
                continue
            left_id = str(left["header"]["record_id"])
            right_id = str(right["header"]["record_id"])
            if _value(left) is not None and _value(right) is not None and not values_compatible(_value(left), _value(right)):
                relation_type = "contradicts"
                rule_id = "rule:core:contradiction@1"
                relation_id = relation_identity(
                    relation_type=relation_type,
                    from_record_id=right_id,
                    to_record_id=left_id,
                    rule_id=rule_id,
                )
                result.append(
                    {
                        "relation_id": relation_id,
                        "identity_digest": identity_digest(relation_id),
                        "schema_version": "1.0.0",
                        "relation_type": relation_type,
                        "from_record_id": right_id,
                        "to_record_id": left_id,
                        "rule_id": rule_id,
                        "evidence_occurrence_ids": [],
                    }
                )
                continue
            if _signature(left) != _signature(right):
                continue
            left_roots = _source_roots(_occurrence_documents(left, occurrences, documents), documents)
            right_roots = _source_roots(_occurrence_documents(right, occurrences, documents), documents)
            relation_type = "duplicate" if left_roots and left_roots == right_roots else "corroborates"
            rule_id = f"rule:core:{relation_type}@1"
            relation_id = relation_identity(
                relation_type=relation_type,
                from_record_id=right_id,
                to_record_id=left_id,
                rule_id=rule_id,
            )
            result.append(
                {
                    "relation_id": relation_id,
                    "identity_digest": identity_digest(relation_id),
                    "schema_version": "1.0.0",
                    "relation_type": relation_type,
                    "from_record_id": right_id,
                    "to_record_id": left_id,
                    "rule_id": rule_id,
                    "evidence_occurrence_ids": [],
                }
            )
    return result


def infer_assertion_relations(
    candidates: Sequence[Mapping[str, Any]],
    *,
    source_documents: Sequence[Mapping[str, Any]] = (),
    evidence_occurrences: Sequence[Mapping[str, Any]] = (),
) -> tuple[dict[str, Any], ...]:
    """Replay deterministic duplicate, corroboration, and contradiction relations."""

    documents = {str(row.get("source_document_id", "")): row for row in source_documents}
    occurrences = {str(row.get("evidence_occurrence_id", "")): row for row in evidence_occurrences}
    return tuple(sorted(_inferred_relations(candidates, occurrences, documents), key=lambda row: row["relation_id"]))


def resolve_observations(
    candidates: Sequence[Mapping[str, Any]],
    *,
    policy_id: str,
    as_of_date: str,
    source_documents: Sequence[Mapping[str, Any]] = (),
    evidence_occurrences: Sequence[Mapping[str, Any]] = (),
    relations: Sequence[Mapping[str, Any]] = (),
) -> ReconciliationResult:
    """Resolve one business-key group with no dependence on input ordering."""

    if policy_id not in POLICY_RECORD_TYPES:
        raise ReconciliationError(f"Unknown assertion-specific policy {policy_id!r}.")
    try:
        cutoff = date.fromisoformat(as_of_date)
    except ValueError as exc:
        raise ReconciliationError(f"Invalid resolution as-of date {as_of_date!r}.") from exc
    documents = {str(row.get("source_document_id")): row for row in source_documents}
    occurrences = {str(row.get("evidence_occurrence_id")): row for row in evidence_occurrences}

    candidate_ids = sorted({str(record.get("header", {}).get("record_id", "")) for record in candidates})
    eligible: list[Mapping[str, Any]] = []
    for record in candidates:
        header = record.get("header", {})
        if header.get("record_type") not in POLICY_RECORD_TYPES[policy_id]:
            continue
        if header.get("review_state") not in ELIGIBLE_REVIEW_STATES:
            continue
        try:
            if date.fromisoformat(str(header.get("knowledge_date"))) > cutoff:
                continue
            source_rows = _occurrence_documents(record, occurrences, documents)
            if not source_rows or any(date.fromisoformat(str(row.get("publication_date"))) > cutoff for row in source_rows):
                continue
        except ValueError:
            continue
        if _authority(record, policy_id, occurrences, documents) < 0:
            continue
        eligible.append(record)

    candidate_keys = sorted({business_key(row) for row in candidates})
    if len(candidate_keys) > 1:
        raise ReconciliationError("resolve_observations accepts exactly one business-key group.")
    business_key_value = candidate_keys[0] if candidate_keys else "empty"
    eligible_ids = sorted(str(row["header"]["record_id"]) for row in eligible)
    record_types = sorted({str(row.get("header", {}).get("record_type", "")) for row in candidates})
    if len(record_types) > 1:
        raise ReconciliationError("resolve_observations accepts exactly one record type.")
    record_type = record_types[0] if record_types and record_types[0] else "NumericalFact"

    inferred = _inferred_relations(eligible, occurrences, documents)
    terminal = {str(row["header"]["record_id"]): row for row in eligible}
    eligible_history = _eligible_history_relations(relations, terminal, occurrences, documents)
    for relation in eligible_history:
        target = str(relation.get("to_record_id", ""))
        if target in terminal:
            terminal.pop(target, None)

    issues: list[dict[str, Any]] = []
    maximal: list[Mapping[str, Any]] = []
    if terminal:
        highest = max(_authority(row, policy_id, occurrences, documents) for row in terminal.values())
        maximal = [row for row in terminal.values() if _authority(row, policy_id, occurrences, documents) == highest]
    maximal.sort(key=lambda row: str(row["header"]["record_id"]))

    selected: str | None = None
    reason_codes: list[str] = []
    if not maximal:
        reason_codes.append("canonical_zero_match")
        issues.append(_review_issue(rule_id="canonical_zero_match", business_key_value=business_key_value, candidates=eligible_ids, message="No eligible canonical candidate remains."))
    else:
        incompatible = any(
            not _assertions_compatible(left, right)
            for index, left in enumerate(maximal)
            for right in maximal[index + 1 :]
        )
        if incompatible:
            reason_codes.append("canonical_equal_authority_conflict")
            issues.append(_review_issue(rule_id="canonical_equal_authority_conflict", business_key_value=business_key_value, candidates=[str(row["header"]["record_id"]) for row in maximal], message="Equal-authority terminal candidates are incompatible."))
        else:
            ranked = sorted(maximal, key=lambda row: (-_value_specificity(row), str(row["header"]["record_id"])))
            exact_candidates = [row for row in ranked if (_value(row) or {}).get("kind") == "exact"]
            assertion_signatures = {_signature(row) for row in ranked}
            if len(assertion_signatures) == 1:
                selected = str(ranked[0]["header"]["record_id"])
                reason_codes.append("canonical_single_maximum" if len(ranked) == 1 else "canonical_equivalent_maxima")
            elif exact_candidates:
                best = exact_candidates[0]
                if all(values_compatible(_value(best), _value(other)) for other in ranked):
                    selected = str(best["header"]["record_id"])
                    reason_codes.append("canonical_exact_dominates_compatible")
                else:  # defensive; pairwise compatibility normally catches this
                    reason_codes.append("canonical_approximation_ambiguity")
                    issues.append(_review_issue(rule_id="canonical_approximation_ambiguity", business_key_value=business_key_value, candidates=[str(row["header"]["record_id"]) for row in maximal], message="An exact value cannot safely dominate the other terminal candidate."))
            else:
                reason_codes.append("canonical_non_exact_ambiguity")
                issues.append(_review_issue(rule_id="canonical_non_exact_ambiguity", business_key_value=business_key_value, candidates=[str(row["header"]["record_id"]) for row in maximal], message="Compatible non-exact terminal candidates differ and neither safely dominates."))

    resolution_id = canonical_resolution_identity(
        record_type=record_type,
        business_key=business_key_value,
        as_of_date=as_of_date,
        policy_id=policy_id,
        candidate_record_ids=eligible_ids,
    )
    resolution = {
        "resolution_id": resolution_id,
        "identity_digest": identity_digest(resolution_id),
        "schema_version": "1.0.0",
        "policy_id": policy_id,
        "record_type": record_type,
        "business_key": business_key_value,
        "as_of_date": as_of_date,
        "candidate_record_ids": candidate_ids,
        "eligible_candidate_ids": eligible_ids,
        "maximal_candidate_ids": sorted(str(row["header"]["record_id"]) for row in maximal),
        "selected_record_id": selected,
        "selection_cardinality": 1 if selected is not None else 0,
        "status": "selected" if selected else "unresolved",
        "reason_codes": sorted(reason_codes),
        "rationale": "assertion-specific authority, explicit history graph and compatible terminal values",
        "review_issue_ids": sorted(row["issue_id"] for row in issues),
    }
    return ReconciliationResult(resolution, tuple(sorted(inferred, key=lambda row: row["relation_id"])), tuple(issues))


PROMISE_FIELDS = ("wording", "target", "baseline", "deadline")


def classify_promise_change(origin: Mapping[str, Any], later: Mapping[str, Any]) -> str:
    """Classify an explicitly matched later promise statement deterministically."""

    if later.get("version_state") == "withdrawn":
        return "withdrawal"
    if later.get("promise_subject_id") != origin.get("promise_subject_id") or later.get("program_id") != origin.get("program_id"):
        return "new_promise"
    changed = {field for field in PROMISE_FIELDS if later.get(field) != origin.get(field)}
    if not changed:
        return "reaffirmation"
    if changed == {"target"}:
        return "target_update"
    if changed == {"deadline"}:
        return "deadline_update"
    return "reformulation"


def match_promise_candidate(
    later: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]]
) -> tuple[Mapping[str, Any] | None, dict[str, Any] | None]:
    """Return one deterministic promise match or a blocking review issue."""

    matches = [
        row
        for row in candidates
        if row.get("payload", {}).get("promise_subject_id") == later.get("promise_subject_id")
        and row.get("payload", {}).get("program_id") == later.get("program_id")
    ]
    if len(matches) == 1:
        return matches[0], None
    candidate_ids = sorted(str(row.get("header", {}).get("entity_id", "")) for row in matches)
    message = "No existing promise matches the later statement." if not matches else "Multiple existing promises match the later statement."
    return None, _review_issue(
        rule_id="promise_match_cardinality",
        business_key_value=str(later.get("promise_subject_id", "")),
        candidates=candidate_ids,
        message=message,
    )


def validate_promise_origin_immutable(
    promise_entity: Mapping[str, Any], versions: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    payload = promise_entity.get("payload", {})
    origin_id = str(payload.get("origin_version_id", ""))
    origin = next((row for row in versions if str(row.get("header", {}).get("record_id")) == origin_id), None)
    if origin is None:
        return [_review_issue(rule_id="promise_origin_missing", business_key_value=str(promise_entity.get("header", {}).get("entity_id", "")), candidates=[], message="Promise origin version is missing.")]
    version_payload = origin.get("payload", {})
    changed = [field for field in PROMISE_FIELDS if payload.get(f"original_{field}") != version_payload.get(field)]
    if changed:
        return [_review_issue(rule_id="promise_origin_immutable", business_key_value=str(promise_entity.get("header", {}).get("entity_id", "")), candidates=[origin_id], message=f"Promise origin differs from immutable fields: {', '.join(changed)}.")]
    return []
