from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path

import pytest

from pbi_xbrl.json_schema_validation import DuplicateJsonKeyError, load_json_strict
from pbi_xbrl.longitudinal_memory.source_adapter.discovery import document_role, load_source_set
from pbi_xbrl.longitudinal_memory.source_adapter.types import SourceContractError


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
SCHEMA = REPO / "docs" / "longitudinal_memory_source_adapter_input.schema.json"


def _raw() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _write(tmp_path: Path, value: dict) -> Path:
    path = tmp_path / "source-set.json"
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8", newline="\n")
    return path


def test_accepted_source_set_is_closed_and_loads() -> None:
    source_set = load_source_set(FIXTURE)
    assert source_set.source_set_id == "source-set:anf:c2@1"
    assert len(source_set.documents) == 8
    assert len(source_set.required_assertions) == 51


def test_every_object_schema_boundary_is_closed() -> None:
    schema = load_json_strict(SCHEMA)
    open_paths: list[str] = []

    def walk(node: object, path: str) -> None:
        if isinstance(node, dict):
            if node.get("type") == "object" and node.get("additionalProperties") is not False:
                open_paths.append(path)
            for key, value in node.items():
                walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]")

    walk(schema, "$")
    assert open_paths == []


def test_duplicate_json_keys_fail_strictly(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"schema_id":"one","schema_id":"two"}', encoding="utf-8")
    with pytest.raises(DuplicateJsonKeyError):
        load_source_set(path)


@pytest.mark.parametrize(
    "relative_path",
    [r"C:\\private\\source.htm", r"..\\escape.htm", r"tickers\\*\\source.htm"],
)
def test_absolute_traversal_and_glob_paths_fail(tmp_path: Path, relative_path: str) -> None:
    value = _raw()
    value["documents"][0]["relative_path"] = relative_path
    with pytest.raises(SourceContractError):
        load_source_set(_write(tmp_path, value))


def test_unknown_root_property_fails_closed(tmp_path: Path) -> None:
    value = _raw()
    value["producer_escape_hatch"] = True
    with pytest.raises(SourceContractError, match="schema validation"):
        load_source_set(_write(tmp_path, value))


@pytest.mark.parametrize(
    ("collection", "identity_field"),
    [
        ("documents", "document_key"),
        ("required_assertions", "assertion_key"),
        ("periods", "period_key"),
        ("reviewed_links", "link_key"),
    ],
)
def test_duplicate_logical_keys_fail(
    tmp_path: Path, collection: str, identity_field: str
) -> None:
    value = _raw()
    duplicate = copy.deepcopy(value[collection][0])
    assert duplicate[identity_field] == value[collection][0][identity_field]
    value[collection].append(duplicate)
    with pytest.raises(SourceContractError):
        load_source_set(_write(tmp_path, value))


def test_duplicate_locator_key_fails(tmp_path: Path) -> None:
    value = _raw()
    value["required_assertions"][1]["locator"]["locator_key"] = value[
        "required_assertions"
    ][0]["locator"]["locator_key"]
    with pytest.raises(SourceContractError, match="Duplicate locator"):
        load_source_set(_write(tmp_path, value))


def test_unknown_document_role_and_publication_metadata_fail(tmp_path: Path) -> None:
    value = _raw()
    value["documents"][0]["source_family"] = "mystery-source"
    with pytest.raises(SourceContractError):
        load_source_set(_write(tmp_path, value))

    value = _raw()
    value["documents"][0]["publication_date"] = "unknown"
    with pytest.raises(SourceContractError):
        load_source_set(_write(tmp_path, value))


def test_valid_document_role_matrix_is_closed() -> None:
    source_set = load_source_set(FIXTURE)
    roles = {document_role(row).role_id for row in source_set.documents}
    assert roles == {
        "sec-filed-earnings-release-exhibit",
        "issuer-business-update-pdf",
        "issuer-earnings-history-workbook",
        "earnings-call-transcript",
    }
    pdf = next(row for row in source_set.documents if row.source_family == "issuer-pdf")
    issuer_release = dataclasses.replace(pdf, document_type="earnings-release")
    assert document_role(issuer_release).role_id == "issuer-earnings-release-pdf"

def test_reviewed_publication_link_and_origin_chain_fail_closed(tmp_path: Path) -> None:
    value = _raw()
    value["reviewed_links"] = [
        row
        for row in value["reviewed_links"]
        if row["relation_type"] != "same-event"
    ]
    with pytest.raises(SourceContractError, match="publication-date link"):
        load_source_set(_write(tmp_path, value))

    value = _raw()
    value["documents"][1]["origin_document_key"] = value["documents"][2]["document_key"]
    value["documents"][2]["origin_document_key"] = value["documents"][1]["document_key"]
    with pytest.raises(SourceContractError, match="cycles"):
        load_source_set(_write(tmp_path, value))


@pytest.mark.parametrize(
    "link_key",
    ["transcript-same-event-release-2026-03-04", "transcript-event-month-support"],
)
def test_reviewed_event_link_cannot_point_to_a_different_source_event(
    tmp_path: Path, link_key: str
) -> None:
    value = _raw()
    link = next(row for row in value["reviewed_links"] if row["link_key"] == link_key)
    link["to_document_key"] = "anf-release-2025-03-06"
    with pytest.raises(SourceContractError, match="different publication dates"):
        load_source_set(_write(tmp_path, value))


def test_revision_is_declared_and_not_order_derived() -> None:
    source_set = load_source_set(FIXTURE)
    assert {row.revision for row in source_set.documents} == {1}
    assert "mtime" not in FIXTURE.read_text(encoding="utf-8").casefold()


def test_source_contract_contains_no_absolute_source_root_or_copied_document_text() -> None:
    raw = FIXTURE.read_text(encoding="utf-8")
    assert r"C:\Users\Jibbe\Aktier\StockModelData" not in raw
    assert "source_material_manifest" not in raw
    assert max(len(row["locator"]["excerpt"]) for row in _raw()["required_assertions"]) < 1000


def test_normalized_package_reference_remains_non_authoritative() -> None:
    value = _raw()["normalized_package_ref"]
    assert value["semantic_snapshot_id"].startswith("normalized-snapshot:v1|")
    assert set(value) == {
        "semantic_snapshot_id",
        "source_package_schema_version",
        "source_package_company_id",
        "source_package_ref",
    }
