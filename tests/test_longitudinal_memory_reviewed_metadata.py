from __future__ import annotations

import dataclasses
from pathlib import Path
from types import MappingProxyType

import pytest

from pbi_xbrl.longitudinal_memory.source_adapter.discovery import (
    discover_sources,
    load_source_set,
)
from pbi_xbrl.longitudinal_memory.source_adapter.reviewed_metadata import (
    parse_reviewed_metadata_bytes,
    verify_reviewed_metadata_documents,
)
from pbi_xbrl.longitudinal_memory.source_adapter.types import LocatorError


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "pbi_source_set.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")


@pytest.fixture(scope="module")
def metadata_context():
    source_set = load_source_set(FIXTURE)
    documents = discover_sources(source_set, SOURCE_ROOT)
    return source_set, documents


def _mutate_metadata_role(source_set, documents, **changes):
    replaced = []
    for document in documents:
        if document.spec.document_key != "q2-2026-transcript-metadata-v2":
            replaced.append(document)
            continue
        role = dict(document.spec.role_metadata or {})
        role.update(changes)
        spec = dataclasses.replace(document.spec, role_metadata=MappingProxyType(role))
        replaced.append(dataclasses.replace(document, spec=spec))
    return tuple(replaced)


def test_revision_two_replays_against_raw_transcript(metadata_context) -> None:
    source_set, documents = metadata_context
    revisions = verify_reviewed_metadata_documents(source_set, documents)
    assert len(revisions) == 1
    revision = revisions[0]
    assert revision.revision == 2
    assert revision.review_date == "2026-08-02"
    assert revision.transcript_sha256 == "e730aa61670393a2fcdd3915d114d95a86e55ddfe18c70f2820d81aefa8130e4"
    assert revision.metadata_sha256 == "0461e7aadaec8f61cd98b5bc44089c45cc45fde60b5379936c481799b01bf515"
    assert revision.predecessor_metadata_sha256 == "3045344f94ec5889969975176dc523f929c4920f1df21f71d9f3160c0636e951"
    assert revision.predecessor_bytes_available == "no"
    assert revision.change_reason == (
        "corrected_source_file_reference_and_removed_unconfirmed_analyst_introduced_cost_reduction_driver"
    )
    assert len(revision.material_locators) == 10


def test_metadata_is_index_only_and_has_no_economic_assertion(metadata_context) -> None:
    source_set, documents = metadata_context
    metadata = next(row for row in documents if row.spec.document_key == "q2-2026-transcript-metadata-v2")
    assert metadata.spec.authority_class == "reviewed-index"
    assert metadata.spec.role_metadata["assertion_authority"] == "index-only"
    assert not any(
        row["document_key"] == metadata.spec.document_key
        for row in source_set.required_assertions
    )


def test_analyst_introduced_cost_reduction_is_not_management_confirmed(metadata_context) -> None:
    _source_set, documents = metadata_context
    metadata_document = next(
        row for row in documents if row.spec.document_key == "q2-2026-transcript-metadata-v2"
    )
    parsed = parse_reviewed_metadata_bytes(metadata_document.verified_bytes)
    assert parsed["SENDTECH"]["sendtech_cost_reductions"] == (
        "analyst_referenced_and_not_explicitly_confirmed"
    )
    assert "cost_reductions" not in parsed["SENDTECH"]["sendtech_margin_drivers"].casefold()


def test_company_specific_review_guards_are_declarative(metadata_context) -> None:
    _source_set, documents = metadata_context
    metadata_document = next(
        row for row in documents if row.spec.document_key == "q2-2026-transcript-metadata-v2"
    )
    assert metadata_document.spec.role_metadata["reviewed_field_guards"] == [
        {
            "section": "SENDTECH",
            "metadata_key": "sendtech_margin_drivers",
            "operator": "not-contains",
            "expected_value": "cost_reductions",
        },
        {
            "section": "SENDTECH",
            "metadata_key": "sendtech_cost_reductions",
            "operator": "equals",
            "expected_value": "analyst_referenced_and_not_explicitly_confirmed",
        },
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("transcript_sha256", "0" * 64, "wrong transcript SHA"),
        ("metadata_sha256", "0" * 64, "wrong metadata SHA"),
        ("review_date", "2026-07-30", "wrong review date"),
        ("predecessor_metadata_sha256", "0" * 64, "supersedes_metadata_sha256"),
        ("change_reason", "formatting_only", "metadata_change_reason"),
    ],
)
def test_reviewed_metadata_provenance_mutations_fail(
    metadata_context, field: str, value: str, message: str
) -> None:
    source_set, documents = metadata_context
    mutated = _mutate_metadata_role(source_set, documents, **{field: value})
    with pytest.raises(LocatorError, match=message):
        verify_reviewed_metadata_documents(source_set, mutated)


def test_changed_material_line_digest_fails(metadata_context) -> None:
    source_set, documents = metadata_context
    metadata = next(row for row in documents if row.spec.document_key == "q2-2026-transcript-metadata-v2")
    role = dict(metadata.spec.role_metadata or {})
    locators = [dict(row) for row in role["material_quote_locators"]]
    locators[0]["line_digest"] = "0" * 64
    mutated = _mutate_metadata_role(
        source_set, documents, material_quote_locators=locators
    )
    with pytest.raises(LocatorError, match="line digest changed"):
        verify_reviewed_metadata_documents(source_set, mutated)


@pytest.mark.parametrize(
    ("operator", "expected_value"),
    [("equals", "management_confirmed"), ("not-contains", "analyst_referenced")],
)
def test_reviewed_field_guard_mismatch_fails(
    metadata_context, operator: str, expected_value: str
) -> None:
    source_set, documents = metadata_context
    guards = [
        {
            "section": "SENDTECH",
            "metadata_key": "sendtech_cost_reductions",
            "operator": operator,
            "expected_value": expected_value,
        }
    ]
    mutated = _mutate_metadata_role(source_set, documents, reviewed_field_guards=guards)
    with pytest.raises(LocatorError, match="field guard failed"):
        verify_reviewed_metadata_documents(source_set, mutated)


def test_duplicate_metadata_semantic_key_fails() -> None:
    with pytest.raises(LocatorError, match="duplicate or empty key"):
        parse_reviewed_metadata_bytes(b"[METADATA]\nsource_file = a.txt\nsource_file = b.txt\n")


def test_metadata_content_before_section_fails() -> None:
    with pytest.raises(LocatorError, match="before its first section"):
        parse_reviewed_metadata_bytes(b"source_file = a.txt\n[METADATA]\nrevision = 2\n")
