from __future__ import annotations

from copy import deepcopy

import pytest

from pbi_xbrl.longitudinal_memory.identity import (
    build_identity,
    evidence_occurrence_identity,
    identity_digest,
    relation_identity,
    source_document_identity,
)
from pbi_xbrl.longitudinal_memory.reconciliation import (
    classify_promise_change,
    match_promise_candidate,
    resolve_observations,
    validate_promise_origin_immutable,
    values_compatible,
)
from pbi_xbrl.longitudinal_memory.validation import build_review_ledger


POLICY = "policy:core:reported-numerical@1"
BUSINESS_KEY = build_identity("business-fact", (("key", "same-economic-reality"),))


def _source(key: str, authority: str = "filed-exhibit", *, origin: str | None = None):
    source_id = source_document_identity(
        company_id="TEST",
        publisher_id="company",
        document_type="release",
        publication_date="2026-03-04",
        document_key=key,
    )
    occurrence_id = evidence_occurrence_identity(
        company_id="TEST",
        document_key=key,
        document_revision=1,
        locator_kind="line",
        locator_key="1",
    )
    document = {
        "source_document_id": source_id,
        "identity_digest": identity_digest(source_id),
        "company_id": "TEST",
        "publication_date": "2026-03-04",
        "authority_class": authority,
        "origin_document_id": origin,
        "review_state": "accepted",
    }
    occurrence = {"evidence_occurrence_id": occurrence_id, "source_document_id": source_id, "company_id": "TEST", "review_state": "accepted"}
    return document, occurrence


def _fact(key: str, occurrence_id: str, value: dict, *, record_type: str = "NumericalFact", review_state: str = "accepted"):
    record_id = build_identity("fact", (("key", key),))
    return {
        "header": {
            "record_id": record_id,
            "record_type": record_type,
            "company_id": "TEST",
            "review_state": review_state,
            "knowledge_date": "2026-03-04",
            "evidence_occurrence_ids": [occurrence_id],
        },
        "payload": {"kind": record_type, "business_key": BUSINESS_KEY, "value": value},
    }


def _relation(relation_type: str, newer: dict, older: dict):
    rule_id = f"rule:core:{relation_type}@1"
    relation_id = relation_identity(
        relation_type=relation_type,
        from_record_id=newer["header"]["record_id"],
        to_record_id=older["header"]["record_id"],
        rule_id=rule_id,
    )
    return {
        "relation_id": relation_id,
        "identity_digest": identity_digest(relation_id),
        "relation_type": relation_type,
        "from_record_id": newer["header"]["record_id"],
        "to_record_id": older["header"]["record_id"],
        "rule_id": rule_id,
        "evidence_occurrence_ids": list(newer["header"]["evidence_occurrence_ids"]),
    }


def test_reconciliation_is_source_order_invariant_and_same_origin_is_duplicate():
    document_a, occurrence_a = _source("release-a")
    document_b, occurrence_b = _source("mirror-b", origin=document_a["source_document_id"])
    document_a["origin_document_id"] = document_a["source_document_id"]
    first = _fact("first", occurrence_a["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    second = _fact("second", occurrence_b["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    kwargs = {
        "policy_id": POLICY,
        "as_of_date": "2026-03-04",
        "source_documents": [document_b, document_a],
        "evidence_occurrences": [occurrence_b, occurrence_a],
    }
    forward = resolve_observations([first, second], **kwargs)
    reverse = resolve_observations([second, first], **kwargs)
    assert forward.resolution == reverse.resolution
    assert forward.inferred_relations == reverse.inferred_relations
    assert forward.resolution["status"] == "selected"
    assert {row["relation_type"] for row in forward.inferred_relations} == {"duplicate"}


def test_independent_repetition_corroborates_but_does_not_create_two_truths():
    document_a, occurrence_a = _source("release-a")
    document_b, occurrence_b = _source("release-b")
    first = _fact("first", occurrence_a["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    second = _fact("second", occurrence_b["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    result = resolve_observations(
        [second, first],
        policy_id=POLICY,
        as_of_date="2026-03-04",
        source_documents=[document_a, document_b],
        evidence_occurrences=[occurrence_a, occurrence_b],
    )
    assert result.resolution["status"] == "selected"
    assert len(result.resolution["maximal_candidate_ids"]) == 2
    assert {row["relation_type"] for row in result.inferred_relations} == {"corroborates"}


@pytest.mark.parametrize("relation_type", ["corrects", "supersedes"])
def test_explicit_correction_and_supersession_replace_without_erasing_history(relation_type):
    document_a, occurrence_a = _source("release-a")
    document_b, occurrence_b = _source("release-b")
    older = _fact("older", occurrence_a["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    corrected = _fact("corrected", occurrence_b["evidence_occurrence_id"], {"kind": "exact", "value": "2"})
    result = resolve_observations(
        [older, corrected],
        policy_id=POLICY,
        as_of_date="2026-03-04",
        source_documents=[document_a, document_b],
        evidence_occurrences=[occurrence_a, occurrence_b],
        relations=[_relation(relation_type, corrected, older)],
    )
    assert result.resolution["candidate_record_ids"] == sorted([older["header"]["record_id"], corrected["header"]["record_id"]])
    assert result.resolution["maximal_candidate_ids"] == [corrected["header"]["record_id"]]
    assert result.resolution["selected_record_id"] == corrected["header"]["record_id"]


def test_exact_dominates_only_compatible_approximation_range_or_bound():
    exact = {"kind": "exact", "value": "40"}
    assert values_compatible(exact, {"kind": "approximate", "value": "40", "qualifier": "around", "tolerance": "2"})
    assert not values_compatible(exact, {"kind": "approximate", "value": "40", "qualifier": "around", "tolerance": None})
    assert values_compatible(exact, {"kind": "range", "low": "39", "high": "41", "low_inclusive": True, "high_inclusive": True})
    assert values_compatible(exact, {"kind": "bound", "operator": "gte", "value": "40"})
    assert not values_compatible({"kind": "qualitative", "text": "strong", "normalized_band": "positive"}, exact)


def test_exact_candidate_can_dominate_compatible_range_but_two_different_ranges_cannot():
    document_a, occurrence_a = _source("release-a")
    document_b, occurrence_b = _source("release-b")
    exact = _fact("exact", occurrence_a["evidence_occurrence_id"], {"kind": "exact", "value": "4"})
    ranged = _fact("range", occurrence_b["evidence_occurrence_id"], {"kind": "range", "low": "3", "high": "5", "low_inclusive": True, "high_inclusive": True})
    result = resolve_observations([ranged, exact], policy_id=POLICY, as_of_date="2026-03-04", source_documents=[document_a, document_b], evidence_occurrences=[occurrence_a, occurrence_b])
    assert result.resolution["selected_record_id"] == exact["header"]["record_id"]

    second_range = _fact("range-two", occurrence_a["evidence_occurrence_id"], {"kind": "range", "low": "4", "high": "6", "low_inclusive": True, "high_inclusive": True})
    ambiguous = resolve_observations([ranged, second_range], policy_id=POLICY, as_of_date="2026-03-04", source_documents=[document_a, document_b], evidence_occurrences=[occurrence_a, occurrence_b])
    assert ambiguous.resolution["status"] == "unresolved"
    assert ambiguous.review_issues[0]["rule_id"] == "canonical_non_exact_ambiguity"


def test_equal_authority_conflict_is_unresolved_p1_and_enters_needs_review():
    document_a, occurrence_a = _source("release-a")
    document_b, occurrence_b = _source("release-b")
    zero = _fact("zero", occurrence_a["evidence_occurrence_id"], {"kind": "exact", "value": "0"})
    one = _fact("one", occurrence_b["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    result = resolve_observations(
        [one, zero],
        policy_id=POLICY,
        as_of_date="2026-03-04",
        source_documents=[document_a, document_b],
        evidence_occurrences=[occurrence_a, occurrence_b],
    )
    assert result.resolution["status"] == "unresolved"
    assert result.resolution["selected_record_id"] is None
    assert result.review_issues[0]["severity"] == "P1"
    assert {row["relation_type"] for row in result.inferred_relations} == {"contradicts"}
    ledger = build_review_ledger(result.review_issues)
    assert ledger["summary"]["blocking_issue_count"] == 1


def test_zero_candidates_and_rejected_candidate_fail_closed():
    empty = resolve_observations([], policy_id=POLICY, as_of_date="2026-03-04")
    assert empty.resolution["status"] == "unresolved"
    assert empty.review_issues[0]["rule_id"] == "canonical_zero_match"

    document, occurrence = _source("release-a")
    rejected = _fact("rejected", occurrence["evidence_occurrence_id"], {"kind": "exact", "value": "1"}, review_state="rejected")
    result = resolve_observations([rejected], policy_id=POLICY, as_of_date="2026-03-04", source_documents=[document], evidence_occurrences=[occurrence])
    assert result.resolution["status"] == "unresolved"


def test_one_eligible_candidate_is_selected_without_source_order_fallback():
    document, occurrence = _source("release-a")
    candidate = _fact("only", occurrence["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    result = resolve_observations([candidate], policy_id=POLICY, as_of_date="2026-03-04", source_documents=[document], evidence_occurrences=[occurrence])
    assert result.resolution["status"] == "selected"
    assert result.resolution["selected_record_id"] == candidate["header"]["record_id"]


def test_assertion_specific_authority_selects_maximal_source_not_input_position():
    filing, filing_occurrence = _source("filing", "filed-exhibit")
    transcript, transcript_occurrence = _source("transcript", "company-transcript")
    authoritative = _fact("filing", filing_occurrence["evidence_occurrence_id"], {"kind": "exact", "value": "1"})
    lower = _fact("transcript", transcript_occurrence["evidence_occurrence_id"], {"kind": "exact", "value": "2"})
    result = resolve_observations([lower, authoritative], policy_id=POLICY, as_of_date="2026-03-04", source_documents=[transcript, filing], evidence_occurrences=[transcript_occurrence, filing_occurrence])
    assert result.resolution["selected_record_id"] == authoritative["header"]["record_id"]
    assert {row["relation_type"] for row in result.inferred_relations} == {"contradicts"}


def test_assertion_policies_preserve_fact_statement_and_interpretation_separation():
    document, occurrence = _source("release-a")
    statement = _fact("statement", occurrence["evidence_occurrence_id"], {"kind": "qualitative", "text": "tariffs pressured margin", "normalized_band": None}, record_type="ManagementStatement")
    result = resolve_observations([statement], policy_id=POLICY, as_of_date="2026-03-04", source_documents=[document], evidence_occurrences=[occurrence])
    assert result.resolution["status"] == "unresolved"
    management = resolve_observations([statement], policy_id="policy:core:management-explanation@1", as_of_date="2026-03-04", source_documents=[document], evidence_occurrences=[occurrence])
    assert management.resolution["selected_record_id"] == statement["header"]["record_id"]


def test_promise_reformulation_rules_and_origin_are_deterministic_and_immutable():
    origin = {
        "promise_subject_id": "stores",
        "program_id": "plan",
        "wording": "Approximately 40 net openings",
        "target": {"kind": "approximate", "value": "40", "qualifier": "approximately", "tolerance": None},
        "baseline": None,
        "deadline": {"kind": "period", "value": "FY2025", "precision": "fiscal-period"},
    }
    assert classify_promise_change(origin, deepcopy(origin)) == "reaffirmation"
    target = deepcopy(origin); target["target"] = {"kind": "exact", "value": "45"}
    assert classify_promise_change(origin, target) == "target_update"
    deadline = deepcopy(origin); deadline["deadline"] = {"kind": "period", "value": "FY2026", "precision": "fiscal-period"}
    assert classify_promise_change(origin, deadline) == "deadline_update"
    reformulated = deepcopy(origin); reformulated["wording"] = "About 40 openings net"
    assert classify_promise_change(origin, reformulated) == "reformulation"
    withdrawn = deepcopy(origin); withdrawn["version_state"] = "withdrawn"
    assert classify_promise_change(origin, withdrawn) == "withdrawal"
    new = deepcopy(origin); new["program_id"] = "new-plan"
    assert classify_promise_change(origin, new) == "new_promise"

    entity_id = build_identity("promise", (("key", "stores"),))
    origin_id = build_identity("pver", (("key", "origin"),))
    entity = {
        "header": {"entity_id": entity_id},
        "payload": {
            "origin_version_id": origin_id,
            "original_wording": origin["wording"],
            "original_target": origin["target"],
            "original_baseline": origin["baseline"],
            "original_deadline": origin["deadline"],
        },
    }
    version = {"header": {"record_id": origin_id}, "payload": origin}
    assert validate_promise_origin_immutable(entity, [version]) == []
    mutated = deepcopy(entity); mutated["payload"]["original_wording"] = "mutated"
    assert validate_promise_origin_immutable(mutated, [version])[0]["rule_id"] == "promise_origin_immutable"


def test_multiple_promise_matches_produce_review_and_no_new_version():
    later = {"promise_subject_id": "stores", "program_id": "plan"}
    candidates = [
        {"header": {"entity_id": build_identity("promise", (("key", key),))}, "payload": later}
        for key in ("a", "b")
    ]
    match, issue = match_promise_candidate(later, candidates)
    assert match is None
    assert issue["severity"] == "P1"
    assert len(issue["candidate_record_ids"]) == 2
