from __future__ import annotations

import json
from pathlib import Path

import pytest

from pbi_xbrl.json_schema_validation import (
    DuplicateJsonKeyError,
    SUPPORTED_SCHEMA_KEYWORDS,
    load_json_strict,
    schema_keywords,
    validate_json_schema,
)
from pbi_xbrl.longitudinal_memory.serialization import SerializationError, serialize_package
from pbi_xbrl.longitudinal_memory.identity import identity_digest, source_document_identity
from pbi_xbrl.longitudinal_memory.validation import DEFAULT_SCHEMA_PATH, validate_package_schema, validate_package_semantics


def _empty_package() -> dict:
    return {
        "schema_id": "longitudinal-company-memory",
        "schema_version": "1.0.0",
        "identity_contract_version": "1",
        "artifact_state": "draft",
        "company_id": "TEST",
        "knowledge_cutoff": "2026-03-04",
        "normalized_package_ref": {
            "semantic_snapshot_id": "normalized-snapshot:v1|sha256=abc",
            "source_package_schema_version": "1.0.0",
            "source_package_company_id": "TEST",
            "source_package_ref": "in-memory",
        },
        "catalog": {
            "metrics": [],
            "definitions": [],
            "bases": [],
            "units": [],
            "dimensions": [],
            "dimension_members": [],
            "dimension_sets": [],
            "policies": [],
            "change_rules": [],
            "methods": [],
        },
        "fiscal_calendars": [],
        "periods": [],
        "source_documents": [],
        "evidence_occurrences": [],
        "entities": [],
        "observations": [],
        "relations": [],
        "resolutions": [],
        "review_issues": [],
    }


def _walk_object_schemas(node):
    if isinstance(node, dict):
        if node.get("type") == "object":
            yield node
        for value in node.values():
            yield from _walk_object_schemas(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk_object_schemas(value)


def test_schema_is_supported_draft_2020_12_and_closed_everywhere():
    schema = load_json_strict(DEFAULT_SCHEMA_PATH)
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema_keywords(schema) <= SUPPORTED_SCHEMA_KEYWORDS
    object_schemas = list(_walk_object_schemas(schema))
    assert object_schemas
    assert all(node.get("additionalProperties") is False for node in object_schemas)
    assert validate_package_schema(_empty_package()) == []


def test_schema_rejects_unknown_root_and_nested_properties():
    package = _empty_package()
    package["unexpected"] = True
    failures = validate_package_schema(package)
    assert any(row.rule_id == "longitudinal_schema_closed" and row.normalized_path == "$.unexpected" for row in failures)

    package = _empty_package()
    package["normalized_package_ref"]["generated_at_utc"] = "2026-03-04T00:00:00Z"
    failures = validate_package_schema(package)
    assert any(row.normalized_path.endswith("generated_at_utc") for row in failures)


def test_existing_strict_loader_rejects_duplicate_keys(tmp_path: Path):
    path = tmp_path / "duplicate.json"
    path.write_text('{"schema_id":"a","schema_id":"b"}', encoding="utf-8")
    with pytest.raises(DuplicateJsonKeyError):
        load_json_strict(path)


@pytest.mark.parametrize(
    "value",
    [
        {"kind": "exact", "value": "0"},
        {"kind": "approximate", "value": "40", "qualifier": "approximately", "tolerance": None},
        {"kind": "range", "low": "3", "high": "5", "low_inclusive": True, "high_inclusive": True},
        {"kind": "bound", "operator": "gte", "value": "6"},
        {"kind": "qualitative", "text": "improving", "normalized_band": "positive"},
    ],
)
def test_closed_value_union_preserves_value_kind(value):
    schema = load_json_strict(DEFAULT_SCHEMA_PATH)
    wrapper = {"$defs": schema["$defs"], "$ref": "#/$defs/valueSpec"}
    assert validate_json_schema(value, wrapper) == []


def test_value_union_rejects_float_and_extra_property():
    schema = load_json_strict(DEFAULT_SCHEMA_PATH)
    wrapper = {"$defs": schema["$defs"], "$ref": "#/$defs/numericValue"}
    assert validate_json_schema({"kind": "exact", "value": 0.0}, wrapper)
    assert validate_json_schema({"kind": "exact", "value": "0", "unit": "%"}, wrapper)


def test_serialization_rejects_floats_and_generated_timestamp():
    with pytest.raises(SerializationError):
        serialize_package({"value": 1.0})
    with pytest.raises(SerializationError):
        serialize_package({"generated_at_utc": "2026-03-04T00:00:00Z"})


def test_new_runtime_contains_no_company_literals_or_ticker_conditionals():
    package_dir = Path(__file__).resolve().parents[1] / "pbi_xbrl" / "longitudinal_memory"
    text = "\n".join(path.read_text(encoding="utf-8") for path in sorted(package_dir.glob("*.py")))
    for forbidden in ("ANF", "PBI", "GPRE"):
        assert forbidden not in text
    lowered = text.lower()
    assert "if ticker" not in lowered
    assert "ticker ==" not in lowered
    assert "turnaround score" not in lowered
    assert "month_shift" not in lowered


def test_immutable_payload_must_match_readable_identity():
    package = _empty_package()
    readable = source_document_identity(company_id="TEST", publisher_id="company", document_type="release", publication_date="2026-03-04", document_key="q4", revision=1)
    package["source_documents"] = [{
        "source_document_id": readable,
        "identity_digest": identity_digest(readable),
        "company_id": "TEST",
        "publisher_id": "company",
        "document_type": "release",
        "publication_date": "2026-03-04",
        "document_key": "mutated-key",
        "revision": 1,
    }]
    issues = validate_package_semantics(package)
    assert any(row.rule_id == "identity_immutable_payload" for row in issues)


def test_serialization_sorts_but_does_not_hide_duplicate_identity_references():
    duplicate = "fact:v1|key=a"
    payload = serialize_package({"candidate_record_ids": [duplicate, duplicate]})
    decoded = json.loads(payload)
    assert decoded["candidate_record_ids"] == [duplicate, duplicate]
