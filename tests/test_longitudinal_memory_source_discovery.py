from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path

import pytest

import pbi_xbrl.longitudinal_memory.source_adapter.discovery as discovery_module
from pbi_xbrl.longitudinal_memory.source_adapter.discovery import (
    _resolve_declared_file,
    discover_sources,
    load_source_set,
    verify_reviewed_model_inputs,
)
from pbi_xbrl.longitudinal_memory.source_adapter.types import (
    SourceContractError,
    SourceDiscoveryError,
)


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")


def _raw() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _write(tmp_path: Path, value: dict) -> Path:
    path = tmp_path / "source-set.json"
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8", newline="\n")
    return path


def test_exact_eight_declared_source_hashes_are_verified() -> None:
    source_set = load_source_set(FIXTURE)
    discovered = discover_sources(source_set, SOURCE_ROOT)
    expected = {row.document_key: row.expected_sha256 for row in source_set.documents}
    assert len(discovered) == 8
    assert {row.spec.document_key: row.content_sha256 for row in discovered} == expected
    assert {
        row.spec.document_key: hashlib.sha256(row.verified_bytes).hexdigest()
        for row in discovered
    } == expected


def test_document_order_reversal_does_not_change_discovery() -> None:
    source_set = load_source_set(FIXTURE)
    baseline = discover_sources(source_set, SOURCE_ROOT)
    reversed_set = dataclasses.replace(source_set, documents=tuple(reversed(source_set.documents)))
    reversed_result = discover_sources(reversed_set, SOURCE_ROOT)
    assert [(row.source_document_id, row.content_sha256) for row in reversed_result] == [
        (row.source_document_id, row.content_sha256) for row in baseline
    ]


def test_missing_required_file_fails_closed(tmp_path: Path) -> None:
    source_set = load_source_set(FIXTURE)
    document = source_set.documents[0]
    missing_root = tmp_path / "root"
    missing_root.joinpath(*Path(document.relative_path.replace("\\", "/")).parts[:-1]).mkdir(
        parents=True
    )
    bounded = dataclasses.replace(source_set, documents=(document,))
    with pytest.raises(SourceDiscoveryError, match="found 0"):
        discover_sources(bounded, missing_root)


def test_wrong_hash_fails_before_identity_creation() -> None:
    source_set = load_source_set(FIXTURE)
    wrong = dataclasses.replace(source_set.documents[0], expected_sha256="0" * 64)
    bounded = dataclasses.replace(source_set, documents=(wrong,))
    with pytest.raises(SourceDiscoveryError, match="SHA-256 mismatch"):
        discover_sources(bounded, SOURCE_ROOT)


def test_changed_revision_changes_readable_identity() -> None:
    source_set = load_source_set(FIXTURE)
    document = source_set.documents[1]
    original = discover_sources(dataclasses.replace(source_set, documents=(document,)), SOURCE_ROOT)[0]
    revised = dataclasses.replace(document, revision=document.revision + 1)
    changed = discover_sources(dataclasses.replace(source_set, documents=(revised,)), SOURCE_ROOT)[0]
    assert original.source_document_id != changed.source_document_id
    assert original.content_sha256 == changed.content_sha256


def test_reused_readable_identity_with_different_bytes_is_p1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    (root / "one").mkdir(parents=True)
    (root / "two").mkdir(parents=True)
    first_path = root / "one" / "first.htm"
    second_path = root / "two" / "second.htm"
    first_path.write_bytes(b"first")
    second_path.write_bytes(b"second")
    source_set = load_source_set(FIXTURE)
    template = source_set.documents[1]
    first = dataclasses.replace(
        template,
        document_key="first-document",
        relative_path=r"one\first.htm",
        expected_sha256=hashlib.sha256(b"first").hexdigest(),
    )
    second = dataclasses.replace(
        template,
        document_key="second-document",
        relative_path=r"two\second.htm",
        expected_sha256=hashlib.sha256(b"second").hexdigest(),
    )
    monkeypatch.setattr(discovery_module, "source_document_identity", lambda **_: "source:v1|same")
    with pytest.raises(SourceDiscoveryError, match="different bytes"):
        discover_sources(dataclasses.replace(source_set, documents=(first, second)), root)


def test_zero_and_multiple_casefold_matches_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_set = load_source_set(FIXTURE)
    document = dataclasses.replace(
        source_set.documents[1], relative_path=r"declared\source.htm"
    )
    root = tmp_path / "root"
    parent = root / "declared"
    parent.mkdir(parents=True)
    with pytest.raises(SourceDiscoveryError, match="found 0"):
        _resolve_declared_file(root.resolve(), document)

    source = parent / "source.htm"
    source.write_bytes(b"x")
    original_iterdir = Path.iterdir

    def duplicated_iterdir(path: Path):
        if path == parent:
            return iter((source, source))
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", duplicated_iterdir)
    with pytest.raises(SourceDiscoveryError, match="found 2"):
        _resolve_declared_file(root.resolve(), document)


def test_reparse_root_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_set = load_source_set(FIXTURE)
    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setattr(discovery_module, "_is_reparse_point", lambda _: True)
    with pytest.raises(SourceDiscoveryError, match="reparse"):
        discover_sources(dataclasses.replace(source_set, documents=()), root)


def test_stale_manifest_destination_is_not_a_discovery_input() -> None:
    source = Path(discovery_module.__file__).read_text(encoding="utf-8")
    assert "source_material_manifest" not in source
    assert "destination" not in source
    assert "first-existing" not in source


def test_reviewed_model_input_bytes_and_acceptance_date_replay() -> None:
    source_set = load_source_set(FIXTURE)
    verify_reviewed_model_inputs(source_set, REPO)


def test_backdated_reviewed_model_input_fails(tmp_path: Path) -> None:
    value = _raw()
    value["reviewed_model_inputs"][0]["knowledge_date"] = "2026-03-04"
    source_set = load_source_set(_write(tmp_path, value))
    with pytest.raises(SourceDiscoveryError, match="backdated"):
        verify_reviewed_model_inputs(source_set, REPO)


def test_changed_reviewed_model_locator_fails(tmp_path: Path) -> None:
    value = _raw()
    value["reviewed_model_inputs"][0]["source_ref"] = (
        "docs/anf_normalized_text_quality_audit.json#rows[field=wrong.path]"
    )
    source_set = load_source_set(_write(tmp_path, value))
    with pytest.raises(SourceDiscoveryError, match="missing or ambiguous"):
        verify_reviewed_model_inputs(source_set, REPO)


def test_duplicate_document_key_fails_before_last_row_wins(tmp_path: Path) -> None:
    value = _raw()
    value["documents"].append(dict(value["documents"][0]))
    with pytest.raises(SourceContractError, match="Duplicate document"):
        load_source_set(_write(tmp_path, value))
