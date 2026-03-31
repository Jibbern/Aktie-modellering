from __future__ import annotations

import json
import shutil
from pathlib import Path
from uuid import uuid4

import pandas as pd

from pbi_xbrl import sec_ingest

_TMP_ROOT = Path(__file__).resolve().parent / ".tmp_sec_ingest"


class _FakeSecClient:
    def __init__(self, cfg: sec_ingest.IngestConfig) -> None:
        self.cfg = cfg
        self.calls: list[tuple[str, bool]] = []

    def get(self, url: str, *, as_json: bool = False):
        self.calls.append((url, as_json))
        raise AssertionError(f"Unexpected SEC GET: {url}")


def _make_cfg(tmp_path: Path) -> sec_ingest.IngestConfig:
    return sec_ingest.IngestConfig(
        cache_dir=tmp_path / "cache",
        user_agent="test-agent@example.com",
        quiet_download_logs=True,
    )


def _make_case_dir() -> Path:
    _TMP_ROOT.mkdir(parents=True, exist_ok=True)
    case_dir = _TMP_ROOT / f"case_{uuid4().hex}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def test_list_filings_uses_cached_submissions_json(monkeypatch) -> None:
    case_dir = _make_case_dir()
    try:
        cfg = _make_cfg(case_dir)
        cik_int = 123456
        cik10 = sec_ingest.cik10_from_int(cik_int)
        cache_dir = cfg.cache_dir / cik10
        cache_dir.mkdir(parents=True, exist_ok=True)
        submissions_path = cache_dir / "submissions.json"
        submissions = {
            "filings": {
                "recent": {
                    "form": ["10-Q"],
                    "accessionNumber": ["0000123456-25-000001"],
                    "filingDate": ["2025-05-01"],
                    "reportDate": ["2025-03-31"],
                    "primaryDocument": ["q1.htm"],
                }
            }
        }
        submissions_path.write_text(json.dumps(submissions), encoding="utf-8")
        monkeypatch.setattr(sec_ingest, "SecClient", _FakeSecClient)

        got_cik, filings_df, got_path = sec_ingest.list_filings(cfg, cik=cik_int)

        assert got_cik == cik_int
        assert got_path == submissions_path
        assert list(filings_df["accession"]) == ["0000123456-25-000001"]
        assert list(filings_df["primaryDoc"]) == ["q1.htm"]
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_list_filings_uses_root_submissions_alias_when_nested_missing(monkeypatch) -> None:
    case_dir = _make_case_dir()
    try:
        cfg = _make_cfg(case_dir)
        cik_int = 123456
        cik10 = sec_ingest.cik10_from_int(cik_int)
        root_submissions_path = cfg.cache_dir / f"submissions_{cik10}.json"
        root_submissions_path.parent.mkdir(parents=True, exist_ok=True)
        submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K"],
                    "accessionNumber": ["0000123456-26-000001"],
                    "filingDate": ["2026-02-20"],
                    "reportDate": ["2025-12-31"],
                    "primaryDocument": ["fy.htm"],
                }
            }
        }
        root_submissions_path.write_text(json.dumps(submissions), encoding="utf-8")
        monkeypatch.setattr(sec_ingest, "SecClient", _FakeSecClient)

        got_cik, filings_df, got_path = sec_ingest.list_filings(cfg, cik=cik_int)

        assert got_cik == cik_int
        assert got_path == cfg.cache_dir / cik10 / "submissions.json"
        assert got_path.exists()
        assert list(filings_df["accession"]) == ["0000123456-26-000001"]
        assert list(filings_df["primaryDoc"]) == ["fy.htm"]
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_list_filings_infers_local_cik_without_ticker_lookup(monkeypatch) -> None:
    case_dir = _make_case_dir()
    try:
        cfg = _make_cfg(case_dir)
        cik_int = 1309402
        cik10 = sec_ingest.cik10_from_int(cik_int)
        root_submissions_path = cfg.cache_dir / f"submissions_{cik10}.json"
        root_submissions_path.parent.mkdir(parents=True, exist_ok=True)
        submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K"],
                    "accessionNumber": ["0001309402-26-000001"],
                    "filingDate": ["2026-02-07"],
                    "reportDate": ["2025-12-31"],
                    "primaryDocument": ["fy.htm"],
                }
            }
        }
        root_submissions_path.write_text(json.dumps(submissions), encoding="utf-8")
        monkeypatch.setattr(sec_ingest, "SecClient", _FakeSecClient)
        monkeypatch.setattr(
            sec_ingest,
            "ticker_to_cik",
            lambda sec, ticker: (_ for _ in ()).throw(AssertionError("ticker_to_cik should not be called")),
        )

        got_cik, filings_df, got_path = sec_ingest.list_filings(cfg, ticker="GPRE")

        assert got_cik == cik_int
        assert got_path == cfg.cache_dir / cik10 / "submissions.json"
        assert got_path.exists()
        assert list(filings_df["accession"]) == ["0001309402-26-000001"]
        assert list(filings_df["primaryDoc"]) == ["fy.htm"]
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_download_filing_package_uses_cached_index_json_without_network() -> None:
    case_dir = _make_case_dir()
    try:
        cfg = _make_cfg(case_dir)
        cik_int = 123456
        accn = "0000123456-25-000001"
        accn_nd = sec_ingest.normalize_accession(accn)
        cik10 = sec_ingest.cik10_from_int(cik_int)
        accn_dir = cfg.cache_dir / cik10 / accn_nd
        accn_dir.mkdir(parents=True, exist_ok=True)
        index_path = accn_dir / "index.json"
        index_payload = {"directory": {"item": []}}
        index_path.write_text(json.dumps(index_payload), encoding="utf-8")

        class _NoGetSec:
            def get(self, url: str, *, as_json: bool = False):
                raise AssertionError(f"Unexpected SEC GET: {url}")

        files_rows, exhibits_rows, instance_paths = sec_ingest.download_filing_package(
            cfg,
            _NoGetSec(),
            cik_int,
            {
                "accession": accn,
                "form": "10-Q",
                "filedDate": "2025-05-01",
                "reportDate": "2025-03-31",
                "primaryDoc": "",
                "ticker": "DEMO",
            },
        )

        assert exhibits_rows == []
        assert instance_paths == []
        assert len(files_rows) == 1
        assert files_rows[0]["status"] == "cache_hit"
        assert files_rows[0]["sec_type"] == "INDEX"
        assert files_rows[0]["local_path"] == str(index_path)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_download_all_passes_record_dicts_to_download_filing_package(monkeypatch) -> None:
    case_dir = _make_case_dir()
    try:
        cfg = _make_cfg(case_dir)
        submissions_path = cfg.cache_dir / "0000123456" / "submissions.json"
        submissions_path.parent.mkdir(parents=True, exist_ok=True)
        submissions_path.write_text("{}", encoding="utf-8")
        filings_df = pd.DataFrame(
            [
                {
                    "accession": "0000123456-25-000001",
                    "form": "10-Q",
                    "filedDate": "2025-05-01",
                    "reportDate": "2025-03-31",
                    "primaryDoc": "q1.htm",
                },
                {
                    "accession": "0000123456-25-000002",
                    "form": "10-K",
                    "filedDate": "2026-02-20",
                    "reportDate": "2025-12-31",
                    "primaryDoc": "fy.htm",
                },
            ]
        )
        seen_filings: list[dict[str, object]] = []

        monkeypatch.setattr(sec_ingest, "SecClient", _FakeSecClient)
        monkeypatch.setattr(
            sec_ingest,
            "list_filings",
            lambda cfg, ticker=None, cik=None: (123456, filings_df.copy(), submissions_path),
        )
        monkeypatch.setattr(sec_ingest, "_load_prior_hash_lookup", lambda *paths: {})

        def _fake_download_filing_package(cfg, sec, cik_int, filing, prior_hash_lookup=None):
            seen_filings.append(dict(filing))
            return (
                [
                    {
                        "accession": filing["accession"],
                        "form": filing["form"],
                        "filedDate": filing["filedDate"],
                        "reportDate": filing["reportDate"],
                        "primaryDoc": filing["primaryDoc"],
                        "kind": "meta",
                        "sec_type": "INDEX",
                        "filename": "index.json",
                        "url": "",
                        "local_path": "",
                        "bytes": 0,
                        "sha256": "",
                        "status": "ok",
                        "error": "",
                        "materialized_path": "",
                    }
                ],
                [],
                [],
            )

        monkeypatch.setattr(sec_ingest, "download_filing_package", _fake_download_filing_package)

        out_filings_df, files_df, exhibits_df, instance_paths = sec_ingest.download_all(cfg, ticker="demo")

        assert out_filings_df.equals(filings_df)
        assert exhibits_df.empty
        assert instance_paths == []
        assert list(files_df["accession"]) == ["", "0000123456-25-000001", "0000123456-25-000002"]
        assert [filing["accession"] for filing in seen_filings] == [
            "0000123456-25-000001",
            "0000123456-25-000002",
        ]
        assert all(filing["ticker"] == "DEMO" for filing in seen_filings)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_materialize_financial_statement_files_keeps_primary_and_good_exhibits_only() -> None:
    case_dir = _make_case_dir()
    try:
        output_dir = case_dir / "GPRE" / "financial_statement"
        source_dir = case_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)

        primary = source_dir / "gpre-20250630.htm"
        primary.write_text("<html><body>10-Q filing</body></html>", encoding="utf-8")
        annual_pdf = source_dir / "annual_report_ex13.pdf"
        annual_pdf.write_bytes(b"%PDF-1.4 annual report data")
        logo_pdf = source_dir / "company_logo.pdf"
        logo_pdf.write_bytes(b"%PDF-1.4 logo noise")
        cert_pdf = source_dir / "ex31_certification.pdf"
        cert_pdf.write_bytes(b"%PDF-1.4 certification")

        files_df = pd.DataFrame(
            [
                {
                    "form": "10-Q",
                    "reportDate": "2025-06-30",
                    "filedDate": "2025-08-05",
                    "kind": "primary",
                    "sec_type": "PRIMARY",
                    "filename": "gpre-20250630.htm",
                    "local_path": str(primary),
                    "bytes": primary.stat().st_size,
                    "status": "cache_hit",
                },
                {
                    "form": "10-K",
                    "reportDate": "2025-12-31",
                    "filedDate": "2026-02-20",
                    "kind": "exhibit",
                    "sec_type": "EX-13",
                    "filename": "annual_report_ex13.pdf",
                    "local_path": str(annual_pdf),
                    "bytes": 12000,
                    "status": "cache_hit",
                },
                {
                    "form": "10-K",
                    "reportDate": "2025-12-31",
                    "filedDate": "2026-02-20",
                    "kind": "exhibit",
                    "sec_type": "EX-99",
                    "filename": "company_logo.pdf",
                    "local_path": str(logo_pdf),
                    "bytes": 12000,
                    "status": "cache_hit",
                },
                {
                    "form": "10-K",
                    "reportDate": "2025-12-31",
                    "filedDate": "2026-02-20",
                    "kind": "exhibit",
                    "sec_type": "EX-31",
                    "filename": "ex31_certification.pdf",
                    "local_path": str(cert_pdf),
                    "bytes": 12000,
                    "status": "cache_hit",
                },
            ]
        )

        manifest_df, summary = sec_ingest.materialize_financial_statement_files(
            files_df,
            output_dir=output_dir,
            ticker="GPRE",
            method="copy",
        )

        materialized_names = sorted(p.name for p in output_dir.iterdir() if p.is_file() and p.name != summary.manifest_path.name)
        assert summary.materialized_count == 2
        assert summary.primary_count == 1
        assert summary.exhibit_count == 1
        assert summary.skipped_decorative == 1
        assert summary.skipped_noncandidate == 1
        assert len(manifest_df) == 2
        assert any(name.endswith("_financial_statement.htm") for name in materialized_names)
        assert any("__EX-13__" in name for name in materialized_names)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)
