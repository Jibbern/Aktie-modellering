from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path, PureWindowsPath

import pytest

from pbi_xbrl.longitudinal_memory.calendar_rules import CALENDAR_YEAR_RULE_ID
from pbi_xbrl.longitudinal_memory.source_adapter.discovery import (
    discover_sources,
    load_source_set,
)
from pbi_xbrl.longitudinal_memory.source_adapter.types import SourceContractError


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "pbi_source_set.v1.json"
SCHEMA = REPO / "docs" / "longitudinal_memory_source_adapter_input.schema.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")

EXPECTED_SOURCES = {
    "april-2026-preliminary": ("9b1fca09cf651c5723ef0de424b082e34ffd9470b65d8f607a20703554986802", 152287),
    "july-2026-earnings-8k": ("601a2c9f89bc7596929f4e98ebf608f6254420dcbdbb4efb06240cc8e161edf6", 30043),
    "june-2026-refinancing-8k": ("a89a86bdb466e67f2ad9b70040e91aab09c16a87da539157e7110c12d93b3555", 32252),
    "promise-july-update": ("4ba0a5cd6abb62b99f5d1f704a798a50c601dd6de442fda9550dccef5e4e5e4c", 117226),
    "promise-origin": ("4009277d88236f90195547a7f263a2e1216483874f0ae3909e378cd47ac1fc15", 136155),
    "q1-2025-release": ("84b4ea43d27103c1a3addffc63065fcfee7c4d018144c45aaa7c428201162f90", 28479),
    "q1-2026-10q": ("460edda51f692c0c47f6c4ddd379ee0845f0769d6dd38e396d54555c32ff71c0", 1755319),
    "q1-2026-release": ("45bd9efd05d11e8bfa8845badf02ca2c428b87af8e5778c44925361612194a8c", 479453),
    "q2-2024-release": ("73493122036061a5ae4d27bc3f8334a1b1e3445ae73a75430e682d4982fae805", 200781),
    "q2-2025-release": ("eb4c958060dbf67f388fcd76c4f69a4d606a541f3ddbbfadd67f769389a6eaac", 37283),
    "q2-2026-10q": ("c806538c659e7452b334afd9251317d7e7f5d6635c27110a23c7124a1ee0e4b5", 2187305),
    "q2-2026-ceo-letter-exhibit": ("bc6fc3ccabef4c4fe77b14bca27628893f360be444be92f317da8b26e7cebc28", 8934),
    "q2-2026-release-exhibit": ("5ac749393a4e970cbc5e638cbc59c5e1f5112497383bfb0b42b9ed6c70f47b28", 24983),
    "q2-2026-transcript": ("e730aa61670393a2fcdd3915d114d95a86e55ddfe18c70f2820d81aefa8130e4", 29064),
    "q2-2026-transcript-metadata-v2": ("0461e7aadaec8f61cd98b5bc44089c45cc45fde60b5379936c481799b01bf515", 18988),
    "q3-2024-release": ("518c7539212abc5b07f9f565980ef82ce6ed187b0c4dcfb39889574c0523e0e6", 33917),
    "q4-2024-release": ("b5ca777d0d2184465cbdbc2c554dcdc6902dda00e976628c39f00a574a919144", 39995),
    "q4-2025-release": ("92efee09ea40718b6248b3842ce4bf60e4fa41735e781ed501f777ecd51828a4", 28096),
}


def _write(tmp_path: Path, raw: dict) -> Path:
    path = tmp_path / "source-set.json"
    path.write_text(json.dumps(raw), encoding="utf-8", newline="\n")
    return path


def test_source_set_is_closed_draft_2020_12_and_strictly_loadable() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["additionalProperties"] is False
    source_set = load_source_set(FIXTURE)
    assert source_set.schema_version == "1.0.0"
    assert len(source_set.documents) == 18
    assert len(source_set.required_assertions) == 61
    assert len(source_set.periods) == 8


def test_all_eighteen_external_sources_match_full_hash_and_size() -> None:
    source_set = load_source_set(FIXTURE)
    assert {row.document_key for row in source_set.documents} == set(EXPECTED_SOURCES)
    for document in source_set.documents:
        expected_sha, expected_size = EXPECTED_SOURCES[document.document_key]
        path = SOURCE_ROOT.joinpath(*PureWindowsPath(document.relative_path).parts)
        data = path.read_bytes()
        assert (hashlib.sha256(data).hexdigest(), len(data)) == (expected_sha, expected_size)
        assert document.expected_sha256 == expected_sha
    assert len(discover_sources(source_set, SOURCE_ROOT)) == 18


def test_document_roles_are_generic_and_authority_specific() -> None:
    source_set = load_source_set(FIXTURE)
    roles = {row.document_key: row.role_id for row in source_set.documents}
    assert roles["q1-2026-10q"] == "sec-primary-10-q"
    assert roles["q2-2026-release-exhibit"] == "sec-filed-exhibit"
    assert roles["promise-origin"] == "reviewed-official-page-pdf-snapshot"
    assert roles["q2-2026-transcript"] == "earnings-call-transcript"
    assert roles["q2-2026-transcript-metadata-v2"] == "reviewed-transcript-metadata"
    assert all(not PureWindowsPath(row.relative_path).is_absolute() for row in source_set.documents)


def test_sec_accessions_origins_and_reviewed_snapshot_urls_are_explicit() -> None:
    source_set = load_source_set(FIXTURE)
    by_key = {row.document_key: row for row in source_set.documents}
    assert by_key["q1-2026-10q"].accession == "0001628280-26-031003"
    assert by_key["q2-2026-10q"].accession == "0001628280-26-050908"
    assert by_key["q2-2026-release-exhibit"].origin_document_key == "july-2026-earnings-8k"
    assert by_key["q2-2026-ceo-letter-exhibit"].origin_document_key == "july-2026-earnings-8k"
    for key in ("promise-origin", "promise-july-update"):
        assert by_key[key].canonical_url.startswith(
            "https://www.investorrelations.pitneybowes.com/"
        )
        assert by_key[key].role_metadata["assertion_authority"] == "issuer-content"


def test_calendar_year_rule_and_exact_period_boundaries_are_declared() -> None:
    source_set = load_source_set(FIXTURE)
    rule = source_set.profile["reviewed_calendar_rule"]
    assert rule["rule_id"] == CALENDAR_YEAR_RULE_ID
    assert set(rule["quarter_boundaries"]) == {
        "Q1:01-01:03-31",
        "Q2:04-01:06-30",
        "Q3:07-01:09-30",
        "Q4:10-01:12-31",
    }
    periods = {row["period_key"]: row for row in source_set.periods}
    assert (periods["cy2026-q1"]["start_date"], periods["cy2026-q1"]["end_date"]) == (
        "2026-01-01",
        "2026-03-31",
    )
    assert (periods["cy2026-q2"]["start_date"], periods["cy2026-q2"]["end_date"]) == (
        "2026-04-01",
        "2026-06-30",
    )


@pytest.mark.parametrize(
    ("document_key", "field", "value"),
    [
        ("q1-2026-10q", "accession", None),
        ("q2-2026-transcript", "authority_class", "filed-exhibit"),
        ("q2-2026-transcript-metadata-v2", "authority_class", "company-release"),
        ("promise-origin", "canonical_url", None),
    ],
)
def test_incoherent_document_role_mutations_fail(
    tmp_path: Path, document_key: str, field: str, value: object
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    document = next(row for row in raw["documents"] if row["document_key"] == document_key)
    document[field] = value
    with pytest.raises(SourceContractError):
        load_source_set(_write(tmp_path, raw))


def test_duplicate_json_key_fails_strict_parsing(tmp_path: Path) -> None:
    raw = FIXTURE.read_text(encoding="utf-8")
    raw = raw.replace('"schema_id":', '"schema_id": "duplicate",\n  "schema_id":', 1)
    path = tmp_path / "duplicate.json"
    path.write_text(raw, encoding="utf-8", newline="\n")
    with pytest.raises(ValueError, match="Duplicate JSON key"):
        load_source_set(path)


def test_absolute_and_traversal_paths_fail(tmp_path: Path) -> None:
    for relative_path in (r"C:\source.html", r"tickers\PBI\..\secret.html"):
        raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
        raw["documents"][0]["relative_path"] = relative_path
        with pytest.raises(SourceContractError):
            load_source_set(_write(tmp_path, raw))
