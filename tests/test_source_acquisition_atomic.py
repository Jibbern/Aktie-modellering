from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import pandas as pd

from pbi_xbrl import sec_ingest, source_acquisition
from pbi_xbrl.source_acquisition import (
    SourceAcquisitionError,
    atomic_publish_source_bytes,
    validate_published_source,
    validate_source_bytes,
)
from pbi_xbrl.source_material_refresh import MaterialCandidate, _destination_name, _manifest_key, _materialize_candidate


HTML_A = b"<html><body>complete source A</body></html>"
HTML_B = b"<html><body>complete source B</body></html>"


def test_successful_source_acquisition_atomically_publishes_and_records_hash(tmp_path: Path) -> None:
    final = tmp_path / "filing.htm"
    receipt = atomic_publish_source_bytes(final, HTML_A)
    assert final.read_bytes() == HTML_A
    assert receipt.sha256 == hashlib.sha256(HTML_A).hexdigest()
    assert receipt.size == len(HTML_A)
    assert not list(tmp_path.glob("*.partial"))


def test_interrupted_atomic_replace_never_exposes_new_bytes_and_cleans_stage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    final = tmp_path / "filing.htm"
    final.write_bytes(HTML_A)

    def _fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("simulated interrupted publication")

    monkeypatch.setattr(source_acquisition.os, "replace", _fail_replace)
    with pytest.raises(OSError, match="interrupted publication"):
        atomic_publish_source_bytes(final, HTML_B)
    assert final.read_bytes() == HTML_A
    assert not list(tmp_path.glob(".*.partial"))


@pytest.mark.parametrize(
    ("name", "payload", "content_type", "message"),
    [
        ("zero.htm", b"", None, "zero bytes"),
        ("truncated.pdf", b"%PDF-1.7\nno eof", None, "truncated"),
        ("wrong.pdf", HTML_A, "text/html", "returned HTML"),
        ("garbage.htm", b"nonempty garbage", None, "recognized document root"),
    ],
)
def test_invalid_staged_sources_fail_closed(
    tmp_path: Path,
    name: str,
    payload: bytes,
    content_type: str | None,
    message: str,
) -> None:
    final = tmp_path / name
    with pytest.raises(SourceAcquisitionError, match=message):
        atomic_publish_source_bytes(final, payload, content_type=content_type)
    assert not final.exists()
    assert not list(tmp_path.glob(".*.partial"))


def test_failed_refresh_preserves_prior_valid_source(tmp_path: Path) -> None:
    final = tmp_path / "filing.htm"
    atomic_publish_source_bytes(final, HTML_A)
    with pytest.raises(SourceAcquisitionError, match="byte count mismatch"):
        atomic_publish_source_bytes(final, HTML_B, expected_size=len(HTML_B) + 1)
    assert final.read_bytes() == HTML_A


def test_successful_refresh_replaces_prior_source_as_one_complete_identity(tmp_path: Path) -> None:
    final = tmp_path / "filing.htm"
    atomic_publish_source_bytes(final, HTML_A)
    receipt = atomic_publish_source_bytes(final, HTML_B)
    assert final.read_bytes() == HTML_B
    assert receipt.sha256 == hashlib.sha256(HTML_B).hexdigest()


def test_cache_reuse_requires_matching_content_identity(tmp_path: Path) -> None:
    final = tmp_path / "filing.htm"
    final.write_bytes(HTML_A)
    expected = hashlib.sha256(HTML_A).hexdigest()
    assert validate_published_source(final, expected_sha256=expected).sha256 == expected
    with pytest.raises(SourceAcquisitionError, match="SHA-256 mismatch"):
        validate_published_source(final, expected_sha256="0" * 64)


def test_expected_size_detects_truncated_transfer_even_when_format_is_parseable(tmp_path: Path) -> None:
    with pytest.raises(SourceAcquisitionError, match="byte count mismatch"):
        validate_source_bytes(HTML_A, path=tmp_path / "filing.htm", expected_size=len(HTML_A) + 10)


def test_duplicate_concurrent_publish_never_produces_torn_final_bytes(tmp_path: Path) -> None:
    final = tmp_path / "filing.htm"
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = list(pool.map(lambda payload: atomic_publish_source_bytes(final, payload), (HTML_A, HTML_B)))
    assert final.read_bytes() in {HTML_A, HTML_B}
    assert {receipt.sha256 for receipt in receipts} == {
        hashlib.sha256(HTML_A).hexdigest(),
        hashlib.sha256(HTML_B).hexdigest(),
    }
    assert not list(tmp_path.glob(".*.partial"))


def test_sec_ingest_rejects_nonempty_garbage_cache_and_reacquires(tmp_path: Path) -> None:
    cik_int = 123456
    accn = "0000123456-25-000001"
    accn_nd = sec_ingest.normalize_accession(accn)
    cik10 = sec_ingest.cik10_from_int(cik_int)
    cfg = sec_ingest.IngestConfig(
        cache_dir=tmp_path / "cache",
        user_agent="test-agent@example.com",
        materialize=False,
        quiet_download_logs=True,
    )
    accn_dir = cfg.cache_dir / cik10 / accn_nd
    docs_dir = accn_dir / "docs"
    docs_dir.mkdir(parents=True)
    primary_path = docs_dir / "q1.htm"
    primary_path.write_bytes(b"nonempty garbage")
    index_payload = {
        "directory": {
            "item": [
                {"name": "q1.htm", "type": "10-Q", "size": len(HTML_A)},
            ]
        }
    }
    (accn_dir / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")

    class _Sec:
        def get(self, url: str, *, as_json: bool = False):
            assert not as_json
            assert url.endswith("/q1.htm")
            return HTML_A

    rows, _exhibits, _instances = sec_ingest.download_filing_package(
        cfg,
        _Sec(),
        cik_int,
        {
            "accession": accn,
            "form": "10-Q",
            "filedDate": "2025-05-01",
            "reportDate": "2025-03-31",
            "primaryDoc": "q1.htm",
            "ticker": "DEMO",
        },
    )
    primary_row = next(row for row in rows if row["kind"] == "primary")
    assert primary_row["status"] == "ok"
    assert primary_row["sha256"] == hashlib.sha256(HTML_A).hexdigest()
    assert primary_path.read_bytes() == HTML_A


def test_source_refresh_rejects_nonempty_garbage_and_records_published_hash(tmp_path: Path) -> None:
    candidate = MaterialCandidate(
        canonical_family="earnings_release",
        quarter=pd.Timestamp("2025-12-31").date(),
        local_path=None,
        source_url="https://example.test/q4.htm",
        title="Q4 release",
        origin="official_ir",
        report_date="2025-12-31",
        filed_date="2026-02-01",
        source_doc_title="Q4 release",
    )
    destination_dir = tmp_path / "PBI" / "earnings_release"
    destination_dir.mkdir(parents=True)
    destination = destination_dir / _destination_name(candidate, ext=".htm")
    destination.write_bytes(b"nonempty garbage")

    class _Response:
        content = HTML_B
        headers = {"Content-Length": str(len(HTML_B)), "Content-Type": "text/html; charset=utf-8"}

        @staticmethod
        def raise_for_status() -> None:
            return None

    class _Session:
        @staticmethod
        def get(_url: str, timeout: int):
            assert timeout == 30
            return _Response()

    manifest: dict[str, dict[str, object]] = {}
    event = _materialize_candidate(
        repo_root=tmp_path,
        ticker="PBI",
        manifest=manifest,
        candidate=candidate,
        dry_run=False,
        download_session=_Session(),
    )
    assert event.status == "added"
    assert destination.read_bytes() == HTML_B
    assert manifest[_manifest_key(candidate)]["sha256"] == hashlib.sha256(HTML_B).hexdigest()
