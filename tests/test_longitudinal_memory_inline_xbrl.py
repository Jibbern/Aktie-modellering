from __future__ import annotations

import copy
from collections import Counter
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.source_adapter.discovery import (
    discover_sources,
    load_source_set,
)
from pbi_xbrl.longitudinal_memory.source_adapter.inline_xbrl import (
    capture_inline_xbrl_locator,
    extract_inline_xbrl_evidence,
    replay_inline_xbrl_locator,
)
from pbi_xbrl.longitudinal_memory.source_adapter.types import LocatorError


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "pbi_source_set.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")


@pytest.fixture(scope="module")
def inline_context():
    source_set = load_source_set(FIXTURE)
    documents = {
        row.spec.document_key: row for row in discover_sources(source_set, SOURCE_ROOT)
    }
    assertions = [
        row
        for row in source_set.required_assertions
        if row["locator"]["locator_kind"] == "inline-xbrl-fact"
    ]
    return documents, assertions


def test_all_twelve_inline_xbrl_locators_replay_from_verified_bytes(inline_context) -> None:
    documents, assertions = inline_context
    replayed = [
        replay_inline_xbrl_locator(documents[row["document_key"]], row["locator"])
        for row in assertions
    ]
    assert len(replayed) == 12
    assert Counter(row["period_end"] for row in replayed) == {
        "2026-03-31": 4,
        "2026-06-30": 4,
        "2025-06-30": 4,
    }
    assert {row["entity_identifier"] for row in replayed} == {"0000078814"}
    assert {tuple(row["unit_numerator_measures"]) for row in replayed} == {
        ("iso4217:USD",)
    }


def test_inline_xbrl_source_values_are_not_table_position_constants(inline_context) -> None:
    documents, assertions = inline_context
    observed = {
        row["assertion_key"]: replay_inline_xbrl_locator(
            documents[row["document_key"]], row["locator"]
        )["canonical_value"]
        for row in assertions
    }
    assert observed == {
        "presort-ebit-q1-2026": "39178000",
        "presort-ebit-q2-2025-comparator": "35940000",
        "presort-ebit-q2-2026": "20006000",
        "presort-revenue-q1-2026": "163466000",
        "presort-revenue-q2-2025-comparator": "150193000",
        "presort-revenue-q2-2026": "142568000",
        "sendtech-ebit-q1-2026": "113530000",
        "sendtech-ebit-q2-2025-comparator": "101255000",
        "sendtech-ebit-q2-2026": "122678000",
        "sendtech-revenue-q1-2026": "313947000",
        "sendtech-revenue-q2-2025-comparator": "311716000",
        "sendtech-revenue-q2-2026": "308930000",
    }


def test_capture_and_replay_preserve_context_unit_and_dom_identity(inline_context) -> None:
    documents, assertions = inline_context
    expected = next(row for row in assertions if row["assertion_key"] == "sendtech-revenue-q2-2026")
    locator = capture_inline_xbrl_locator(
        documents[expected["document_key"]],
        fact_id=expected["locator"]["fact_id"],
        locator_key="test-captured-sendtech-revenue",
    )
    replayed = replay_inline_xbrl_locator(documents[expected["document_key"]], locator)
    assert replayed["concept"] == expected["locator"]["concept"]
    assert replayed["context_dimensions"] == expected["locator"]["context_dimensions"]
    assert replayed["period_start"] == "2026-04-01"
    assert replayed["period_end"] == "2026-06-30"
    assert replayed["canonical_value"] == "308930000"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("concept", "us-gaap:WrongConcept"),
        ("context_id", "c-wrong"),
        ("unit_ref", "u-wrong"),
        ("period_start", "2026-04-02"),
        ("period_end", "2026-06-29"),
        ("canonical_value", "1"),
        ("dom_node_path", "/html[1]/wrong[1]"),
        ("excerpt_sha256", "0" * 64),
    ],
)
def test_inline_xbrl_locator_mutations_fail(inline_context, field: str, value: object) -> None:
    documents, assertions = inline_context
    assertion = next(row for row in assertions if row["assertion_key"] == "sendtech-revenue-q2-2026")
    locator = copy.deepcopy(dict(assertion["locator"]))
    locator[field] = value
    with pytest.raises(LocatorError):
        replay_inline_xbrl_locator(documents[assertion["document_key"]], locator)


def test_inline_xbrl_wrong_fact_id_fails_without_first_match(inline_context) -> None:
    documents, assertions = inline_context
    assertion = assertions[0]
    locator = copy.deepcopy(dict(assertion["locator"]))
    locator["fact_id"] = "missing-fact-id"
    with pytest.raises(LocatorError, match="matched 0 nodes"):
        replay_inline_xbrl_locator(documents[assertion["document_key"]], locator)


def test_inline_xbrl_batch_extraction_is_assertion_order_invariant(inline_context) -> None:
    documents, assertions = inline_context
    by_document: dict[str, list] = {}
    for assertion in assertions:
        by_document.setdefault(str(assertion["document_key"]), []).append(assertion)
    for document_key, rows in by_document.items():
        forward = extract_inline_xbrl_evidence(documents[document_key], rows)
        reverse = extract_inline_xbrl_evidence(documents[document_key], list(reversed(rows)))
        assert forward == reverse
