from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd
import requests

import pbi_xbrl.source_material_refresh as smr

from pbi_xbrl.company_profiles import CompanyProfile, SourceMaterialSeed
from pbi_xbrl.source_material_refresh import (
    MaterialCandidate,
    SourceMaterialRefreshSummary,
    _build_coverage_report,
    _classify_material_family,
    _collect_sec_material_candidates,
    _detect_filing_package_presence,
    _destination_name,
    _discover_ir_candidates_for_seed,
    _list_recent_filings_with_legacy_support,
    _manifest_key,
    _materialize_candidate,
    _prune_stale_manifest_entries,
    _quarter_match_text,
    _resolved_destination_dir,
    format_refresh_summary,
    supports_source_material_refresh,
)

_TMP_ROOT = Path(__file__).resolve().parent / ".tmp_source_material_refresh"


def _make_case_dir() -> Path:
    _TMP_ROOT.mkdir(parents=True, exist_ok=True)
    case_dir = _TMP_ROOT / f"case_{uuid4().hex}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def test_detect_filing_package_presence_recognizes_flat_complete() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "PBI"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn = "0000078814-25-000004"
        accn_nd = "000007881425000004"
        (cache_dir / f"index_{accn_nd}.json").write_text('{"directory":{"item":[]}}', encoding="utf-8")
        (cache_dir / f"doc_{accn_nd}_pbi-20250211.htm").write_text("<html>primary</html>", encoding="utf-8")

        filing = {
            "accession": accn,
            "primaryDoc": "pbi-20250211.htm",
        }
        got = _detect_filing_package_presence(cache_dir, 78814, filing)
        assert got.status == "present_complete"
        assert got.index_path is not None
        assert got.primary_path is not None
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_detect_filing_package_presence_marks_incomplete_when_only_index_exists() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "PBI"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn_nd = "000007881425000004"
        (cache_dir / f"index_{accn_nd}.json").write_text('{"directory":{"item":[]}}', encoding="utf-8")

        filing = {
            "accession": "0000078814-25-000004",
            "primaryDoc": "pbi-20250211.htm",
        }
        got = _detect_filing_package_presence(cache_dir, 78814, filing)
        assert got.status == "present_incomplete"
        assert got.primary_path is None
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_list_recent_filings_with_legacy_support_uses_flat_submissions() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "PBI"
        cache_dir.mkdir(parents=True, exist_ok=True)
        submissions = {
            "filings": {
                "recent": {
                    "form": ["10-Q"],
                    "accessionNumber": ["0000078814-25-000023"],
                    "filingDate": ["2025-05-02"],
                    "reportDate": ["2025-03-31"],
                    "primaryDocument": ["pbi-20250331.htm"],
                }
            }
        }
        (cache_dir / "submissions_0000078814.json").write_text(json.dumps(submissions), encoding="utf-8")

        cik_int, filings_df, path_in = _list_recent_filings_with_legacy_support(
            type("Cfg", (), {"cache_dir": cache_dir, "forms": ("10-Q", "10-K", "8-K"), "max_filings": None, "user_agent": "test@example.com"})(),
            cik=78814,
        )
        assert cik_int == 78814
        assert path_in.name == "submissions_0000078814.json"
        assert list(filings_df["accession"]) == ["0000078814-25-000023"]
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_collect_sec_material_candidates_classifies_release_and_presentation_and_skips_decorative() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "PBI"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn = "0000078814-25-000023"
        accn_nd = "000007881425000023"
        index_payload = {
            "directory": {
                "item": [
                    {"name": "q12025_earnings_release.htm", "type": "EX-99.1"},
                    {"name": "q12025_results_presentation.pdf", "type": "EX-99.2"},
                    {"name": "favicon.ico", "type": "GRAPHIC"},
                    {"name": "company_logo.jpg", "type": "GRAPHIC"},
                ]
            }
        }
        (cache_dir / f"index_{accn_nd}.json").write_text(json.dumps(index_payload), encoding="utf-8")
        (cache_dir / f"doc_{accn_nd}_q12025_earnings_release.htm").write_text(
            "<html><title>Q1 2025 Earnings Release</title></html>",
            encoding="utf-8",
        )
        (cache_dir / f"doc_{accn_nd}_q12025_results_presentation.pdf").write_bytes(b"%PDF-1.4 fake")
        (cache_dir / f"doc_{accn_nd}_favicon.ico").write_bytes(b"ico")
        (cache_dir / f"doc_{accn_nd}_company_logo.jpg").write_bytes(b"small-logo")

        filing = {
            "accession": accn,
            "form": "8-K",
            "reportDate": "2025-03-31",
            "filedDate": "2025-05-02",
        }
        got = _collect_sec_material_candidates(cache_dir, 78814, filing)
        families = {row.canonical_family for row in got}
        assert families == {"earnings_release", "earnings_presentation"}
        assert all("favicon" not in str(row.local_path or "").lower() for row in got)
        assert all("logo" not in str(row.local_path or "").lower() for row in got)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_collect_sec_material_candidates_reclassifies_generic_ex99_non_results_as_press_release() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "GPRE"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn = "0001309402-25-000158"
        accn_nd = "000130940225000158"
        index_payload = {
            "directory": {
                "item": [
                    {"name": "Document.htm", "type": "EX-99.1"},
                    {"name": "gpre-20251027.htm", "type": "10-Q"},
                ]
            }
        }
        (cache_dir / f"index_{accn_nd}.json").write_text(json.dumps(index_payload), encoding="utf-8")
        (cache_dir / f"doc_{accn_nd}_Document.htm").write_text(
            "<html><title>Document</title><body>Green Plains announces board committee changes and governance updates.</body></html>",
            encoding="utf-8",
        )
        (cache_dir / f"doc_{accn_nd}_gpre-20251027.htm").write_text(
            "<html><body>Item 8.01 Other Events.</body></html>",
            encoding="utf-8",
        )

        filing = {
            "accession": accn,
            "form": "8-K",
            "primaryDoc": "gpre-20251027.htm",
            "reportDate": "2025-10-27",
            "filedDate": "2025-10-27",
        }
        got = _collect_sec_material_candidates(cache_dir, 1309402, filing)
        assert len(got) == 1
        assert got[0].canonical_family == "press_release"
        assert got[0].quarter_assignment_status == "non_quarter_event"
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_pbi_chair_change_release_is_press_release_not_earnings_release() -> None:
    family = _classify_material_family(
        nm="pressrelease-pbchairchange.htm",
        title="Document",
        sec_type="TEXT.GIF",
        seed_family_hint="",
        text_excerpt="Pitney Bowes announces board chair change and governance updates.",
        source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828025043900/pressrelease-pbchairchange.htm",
    )
    assert family == "press_release"


def test_ceo_letter_with_quarter_results_stays_earnings_release() -> None:
    family = _classify_material_family(
        nm="q32025earningsceoletter.htm",
        title="Q3 2025 Earnings CEO Letter",
        sec_type="TEXT.GIF",
        seed_family_hint="",
        text_excerpt="Third Quarter 2025 financial results and earnings discussion from management.",
        source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828025047122/q32025earningsceoletter.htm",
        default_q=pd.Timestamp("2025-09-30").date(),
        filing_is_earnings_relevant=True,
    )
    assert family == "earnings_release"


def test_earnings_press_release_stays_earnings_release() -> None:
    family = _classify_material_family(
        nm="q42025earningspressrelea.htm",
        title="Q4 2025 Earnings Press Release",
        sec_type="TEXT.GIF",
        seed_family_hint="",
        text_excerpt="Fourth Quarter 2025 financial results and earnings press release.",
        source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828026008604/q42025earningspressrelea.htm",
        default_q=pd.Timestamp("2025-12-31").date(),
        filing_is_earnings_relevant=True,
    )
    assert family == "earnings_release"


def test_explicit_release_url_is_not_misclassified_as_presentation() -> None:
    family = _classify_material_family(
        nm="gpre-q42025earningsrelease.htm",
        title="2",
        sec_type="TEXT.GIF",
        seed_family_hint="",
        text_excerpt="Supplemental financial tables accompany the fourth quarter 2025 earnings release.",
        source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940226000016/gpre-q42025earningsrelease.htm",
        default_q=pd.Timestamp("2025-12-31").date(),
        filing_is_earnings_relevant=True,
    )
    assert family == "earnings_release"


def test_results_release_is_not_demoted_by_cfo_quote_language() -> None:
    family = _classify_material_family(
        nm="q42025earningspressrelea.htm",
        title="Q4 2025 Earnings Press Release",
        sec_type="TEXT.GIF",
        seed_family_hint="",
        text_excerpt="Fourth Quarter 2025 financial results. Chief Financial Officer comments on margin and cash flow.",
        source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828026008604/q42025earningspressrelea.htm",
        default_q=pd.Timestamp("2025-12-31").date(),
        filing_is_earnings_relevant=True,
    )
    assert family == "earnings_release"


def test_transaction_pro_forma_exhibit_is_ignored() -> None:
    family = _classify_material_family(
        nm="exhibit991-unauditedprofor.htm",
        title="Unaudited Pro Forma Financial Information",
        sec_type="TEXT.GIF",
        seed_family_hint="",
        text_excerpt="Unaudited pro forma financial information for transaction closing.",
        source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940225000151/exhibit991-unauditedprofor.htm",
    )
    assert family is None


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeSession:
    def __init__(self, payloads: dict[str, object]) -> None:
        self.payloads = payloads

    def get(self, url: str, timeout: object = 30) -> _FakeResponse:
        payload = self.payloads[url]
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, tuple):
            text, status_code = payload
            return _FakeResponse(str(text), int(status_code))
        return _FakeResponse(str(payload))


def test_discover_ir_candidates_supports_q4_and_full_year_labels() -> None:
    seed = SourceMaterialSeed(
        family="earnings_presentation",
        seed_url="https://example.com/ir",
        follow_detail_pages=False,
        allowed_hosts=("example.com",),
    )
    session = _FakeSession(
        {
            "https://example.com/ir": """
                <html><body>
                  <a href="/files/q4_2025_presentation.pdf">Fourth Quarter and Full Year 2025 Results Presentation</a>
                  <a href="/files/fy2025_transcript.html">FY2025 Earnings Call Transcript</a>
                </body></html>
            """
        }
    )
    got = _discover_ir_candidates_for_seed(session, seed, {"2025-12-31"})
    families = {row.canonical_family for row in got.candidates}
    assert "earnings_presentation" in families
    assert "earnings_transcripts" in families
    assert any(str(row.quarter) == "2025-12-31" for row in got.candidates)
    assert got.diagnostics[0].outcome == "ok_assets_found"


def test_discover_ir_candidates_does_not_force_seed_family_without_matching_text() -> None:
    seed = SourceMaterialSeed(
        family="earnings_transcripts",
        seed_url="https://example.com/results",
        follow_detail_pages=False,
        allowed_hosts=("example.com",),
    )
    session = _FakeSession(
        {
            "https://example.com/results": """
                <html><body>
                  <a href="/files/q4_2025_overview.pdf">Q4 2025 Overview</a>
                </body></html>
            """
        }
    )
    got = _discover_ir_candidates_for_seed(session, seed, {"2025-12-31"})
    assert got.candidates == []
    assert got.diagnostics[0].outcome == "ok_no_matching_assets"


def test_discover_ir_candidates_reports_timeout_outcome() -> None:
    seed = SourceMaterialSeed(
        family="earnings_presentation",
        seed_url="https://example.com/ir",
        follow_detail_pages=False,
        allowed_hosts=("example.com",),
    )
    session = _FakeSession({"https://example.com/ir": requests.exceptions.ReadTimeout("boom")})
    got = _discover_ir_candidates_for_seed(session, seed, {"2025-12-31"})
    assert got.candidates == []
    assert got.diagnostics[0].outcome == "timeout"


def test_discover_ir_candidates_reports_forbidden_outcome() -> None:
    seed = SourceMaterialSeed(
        family="earnings_presentation",
        seed_url="https://example.com/ir",
        follow_detail_pages=False,
        allowed_hosts=("example.com",),
    )
    session = _FakeSession({"https://example.com/ir": ("forbidden", 403)})
    got = _discover_ir_candidates_for_seed(session, seed, {"2025-12-31"})
    assert got.candidates == []
    assert got.diagnostics[0].outcome == "forbidden_403"


def test_discover_ir_candidates_reports_parse_failure(monkeypatch) -> None:
    seed = SourceMaterialSeed(
        family="earnings_presentation",
        seed_url="https://example.com/ir",
        follow_detail_pages=False,
        allowed_hosts=("example.com",),
    )
    session = _FakeSession({"https://example.com/ir": "<html><body><a href='/q4.pdf'>Q4 2025 Presentation</a></body></html>"})
    monkeypatch.setattr(smr, "_extract_page_links", lambda base_url, html: (_ for _ in ()).throw(RuntimeError("bad parse")))
    got = _discover_ir_candidates_for_seed(session, seed, {"2025-12-31"})
    assert got.candidates == []
    assert got.diagnostics[0].outcome == "parse_failure"


def test_discover_ir_candidates_reports_webcast_only_transcript_gap() -> None:
    seed = SourceMaterialSeed(
        family="earnings_transcripts",
        seed_url="https://example.com/events",
        follow_detail_pages=False,
        allowed_hosts=("example.com",),
    )
    session = _FakeSession(
        {
            "https://example.com/events": """
                <html><body>
                  <a href="/events/q4-2025-webcast">Q4 2025 Webcast Event</a>
                </body></html>
            """
        }
    )
    got = _discover_ir_candidates_for_seed(session, seed, {"2025-12-31"})
    assert got.candidates == []
    assert got.diagnostics[0].outcome == "ok_no_matching_assets"
    assert got.diagnostics[0].webcast_only_count >= 1


def test_quarter_match_text_accepts_full_year_for_q4() -> None:
    assert _quarter_match_text("Fourth Quarter and Full Year 2025 Results", pd.Timestamp("2025-12-31").date())
    assert _quarter_match_text("FY2025 Earnings Call Transcript", pd.Timestamp("2025-12-31").date())


def test_collect_sec_material_candidates_assigns_pbi_q2_2024_from_body_results_heading() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "PBI"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn = "0000078814-24-000050"
        accn_nd = "000007881424000050"
        index_payload = {"directory": {"item": [{"name": "Document.htm", "type": "EX-99.1"}]}}
        (cache_dir / f"index_{accn_nd}.json").write_text(json.dumps(index_payload), encoding="utf-8")
        (cache_dir / f"doc_{accn_nd}_Document.htm").write_text(
            "<html><title>Document</title><body>"
            "Pitney Bowes Announces Financial Results for Second Quarter of Fiscal Year 2024. "
            "Second Quarter Financial Highlights."
            "</body></html>",
            encoding="utf-8",
        )
        filing = {
            "accession": accn,
            "form": "8-K",
            "reportDate": "2024-08-08",
            "filedDate": "2024-08-08",
        }
        got = _collect_sec_material_candidates(cache_dir, 78814, filing)
        assert len(got) == 1
        assert got[0].canonical_family == "earnings_release"
        assert str(got[0].quarter) == "2024-06-30"
        assert got[0].quarter_assignment_status == "matched_quarter_end"
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_collect_sec_material_candidates_keeps_non_results_body_quarter_mentions_out_of_quarter_coverage() -> None:
    case_dir = _make_case_dir()
    try:
        cache_dir = case_dir / "sec_cache" / "GPRE"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn = "0001309402-24-000002"
        accn_nd = "000130940224000002"
        index_payload = {"directory": {"item": [{"name": "pressrelease-chairchange.htm", "type": "EX-99.1"}]}}
        (cache_dir / f"index_{accn_nd}.json").write_text(json.dumps(index_payload), encoding="utf-8")
        (cache_dir / f"doc_{accn_nd}_pressrelease-chairchange.htm").write_text(
            "<html><title>Document</title><body><h1>Board Chair Change</h1>"
            "Green Plains announces board chair change and governance updates. "
            "Risk factors include Quarterly Reports on Form 10-Q for the three months ended March 31, 2023, June 30, 2023 and September 30, 2023."
            "</body></html>",
            encoding="utf-8",
        )
        filing = {
            "accession": accn,
            "form": "8-K",
            "reportDate": "2024-01-04",
            "filedDate": "2024-01-09",
        }
        got = _collect_sec_material_candidates(cache_dir, 1309402, filing)
        assert len(got) == 1
        assert got[0].canonical_family == "press_release"
        assert got[0].quarter_assignment_status == "non_quarter_event"
        assert str(got[0].quarter) == "2024-01-04"
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_materialize_candidate_dry_run_records_manifest_and_reuses_alias_dir() -> None:
    case_dir = _make_case_dir()
    try:
        repo_root = case_dir
        ticker_root = repo_root / "PBI"
        slides_dir = ticker_root / "slides"
        slides_dir.mkdir(parents=True, exist_ok=True)
        src = case_dir / "source.pdf"
        src.write_bytes(b"%PDF-1.4 fake")
        cand = MaterialCandidate(
            canonical_family="earnings_presentation",
            quarter=pd.Timestamp("2025-12-31").date(),
            local_path=src,
            source_url="https://example.com/q4_2025_presentation.pdf",
            title="Q4 2025 Earnings Presentation",
            origin="sec_exhibit",
            accession="0001309402-26-000016",
            form="8-K",
            report_date="2025-12-31",
            filed_date="2026-02-05",
            exhibit_type="EX-99.2",
            selection_reason="SEC exhibit classification",
            source_doc_title="Q4 2025 Earnings Presentation",
        )
        manifest: dict[str, dict[str, object]] = {}
        event = _materialize_candidate(
            repo_root=repo_root,
            ticker="PBI",
            manifest=manifest,
            candidate=cand,
            dry_run=True,
        )
        assert event.status == "added"
        assert not any(slides_dir.iterdir())
        entry = manifest[_manifest_key(cand)]
        assert entry["canonical_family"] == "earnings_presentation"
        assert entry["resolved_destination_dir"] == str(slides_dir)
        assert _resolved_destination_dir(repo_root, "PBI", "earnings_presentation") == slides_dir
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_destination_name_normalizes_wrapper_titles() -> None:
    cand = MaterialCandidate(
        canonical_family="earnings_release",
        quarter=pd.Timestamp("2025-12-31").date(),
        local_path=None,
        source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940226000016/gpre-q42025earningsrelease.htm",
        title="Document",
        origin="sec_exhibit",
        accession="0001309402-26-000016",
        form="8-K",
        report_date="2025-12-31",
        filed_date="2026-02-05",
        exhibit_type="TEXT.GIF",
        source_doc_title="Document",
    )
    got = _destination_name(cand, ext=".htm")
    assert "__TEXT.GIF__" not in got
    assert "Document" not in got
    assert got == "8-K_2026-02-05_earnings_release_q4_2025.htm"


def test_materialize_candidate_renames_existing_manifest_backed_file_to_normalized_name() -> None:
    case_dir = _make_case_dir()
    try:
        repo_root = case_dir
        rel_dir = repo_root / "GPRE" / "earnings_release"
        rel_dir.mkdir(parents=True, exist_ok=True)
        old_path = rel_dir / "SEC_EXHIBIT_8-K_2026-02-05_000130940226000016__TEXT.GIF__Document.htm"
        old_path.write_text("release body", encoding="utf-8")
        cand = MaterialCandidate(
            canonical_family="earnings_release",
            quarter=pd.Timestamp("2025-12-31").date(),
            local_path=old_path,
            source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940226000016/gpre-q42025earningsrelease.htm",
            title="Document",
            origin="sec_exhibit",
            accession="0001309402-26-000016",
            form="8-K",
            report_date="2025-12-31",
            filed_date="2026-02-05",
            exhibit_type="TEXT.GIF",
            source_doc_title="Document",
        )
        manifest = {
            _manifest_key(cand): {
                "canonical_family": "earnings_release",
                "resolved_destination_dir": str(rel_dir),
                "origin": "sec_exhibit",
                "accession": "0001309402-26-000016",
                "form": "8-K",
                "report_date": "2025-12-31",
                "filed_date": "2026-02-05",
                "quarter": "2025-12-31",
                "source_url": cand.source_url,
                "exhibit_type": "TEXT.GIF",
                "source_doc_title": "Document",
                "destination_path": str(old_path),
                "sha256": "",
                "status": "ok",
                "selection_reason": "existing",
            }
        }
        event = _materialize_candidate(
            repo_root=repo_root,
            ticker="GPRE",
            manifest=manifest,
            candidate=cand,
            dry_run=False,
        )
        assert event.status == "skipped"
        assert "normalized destination" in event.reason
        new_path = rel_dir / "8-K_2026-02-05_earnings_release_q4_2025.htm"
        assert new_path.exists()
        assert not old_path.exists()
        assert manifest[_manifest_key(cand)]["destination_path"] == str(new_path)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_prune_stale_manifest_entries_removes_unselected_sec_generated_file() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI"
        stale_dir = material_root / "earnings_presentation"
        stale_dir.mkdir(parents=True, exist_ok=True)
        stale_path = stale_dir / "8-K_2025-11-05_earnings_presentation_q3_2025.htm"
        stale_path.write_text("stale", encoding="utf-8")
        manifest = {
            "sec_exhibit|0001|TEXT.GIF|https://example.com/stale.htm": {
                "canonical_family": "earnings_presentation",
                "resolved_destination_dir": str(stale_dir),
                "origin": "sec_exhibit",
                "destination_path": str(stale_path),
            }
        }
        _prune_stale_manifest_entries(manifest, selected_keys=set(), material_root=material_root)
        assert manifest == {}
        assert not stale_path.exists()
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_coverage_report_uses_quarter_end_history_and_separates_non_quarter_entries() -> None:
    filings_df = pd.DataFrame(
        [
            {"base_form": "10-K", "form": "10-K", "reportDate": "2025-12-31"},
            {"base_form": "10-Q", "form": "10-Q", "reportDate": "2025-09-30"},
            {"base_form": "8-K", "form": "8-K", "reportDate": "2025-09-29"},
        ]
    )
    manifest = {
        "a": {
            "canonical_family": "earnings_release",
            "quarter": "2025-12-31",
            "quarter_assignment_status": "matched_quarter_end",
            "quarter_assignment_reason": "title_quarter_signal",
            "filed_date": "2026-02-05",
            "report_date": "2025-12-31",
            "source_doc_title": "Q4 2025 Earnings Release",
            "destination_path": "C:/tmp/release.htm",
            "source_url": "https://example.com/release.htm",
        },
        "b": {
            "canonical_family": "press_release",
            "quarter": "2025-09-29",
            "quarter_assignment_status": "non_quarter_event",
            "quarter_assignment_reason": "non_quarter_report_date",
            "filed_date": "2025-10-03",
            "report_date": "2025-09-29",
            "source_doc_title": "Chair Change Press Release",
            "destination_path": "C:/tmp/press.htm",
            "source_url": "https://example.com/press.htm",
        },
    }
    report = _build_coverage_report(
        ticker="PBI",
        manifest=manifest,
        filings_df=filings_df,
        max_quarters=4,
        ir_diagnostics=(
            smr.IRSeedDiagnostic(
                family="earnings_presentation",
                seed_url="https://example.com/presentations",
                outcome="timeout",
            ),
            smr.IRSeedDiagnostic(
                family="earnings_transcripts",
                seed_url="https://example.com/transcripts",
                outcome="ok_no_matching_assets",
                webcast_only_count=1,
            ),
        ),
    )
    assert report["latest_quarter"] == "2025-12-31"
    qrows = {row["quarter"]: row for row in report["quarters"]}
    assert qrows["2025-12-31"]["release_found"]
    assert not qrows["2025-12-31"]["presentation_found"]
    assert qrows["2025-09-30"]["missing_expected_families"] == ["earnings_release", "earnings_presentation", "earnings_transcripts"]
    assert qrows["2025-09-30"]["missing_reasons_by_family"]["earnings_release"] == "no SEC exhibit found"
    assert qrows["2025-09-30"]["missing_reasons_by_family"]["earnings_presentation"] == "IR timeout"
    assert qrows["2025-09-30"]["missing_reasons_by_family"]["earnings_transcripts"] == "only webcast/event page was found without downloadable transcript"
    assert len(report["non_quarter_materials"]) == 1
    assert report["non_quarter_materials"][0]["source_doc_title"] == "Chair Change Press Release"


def test_supports_source_material_refresh_requires_official_seed_config() -> None:
    profile = CompanyProfile(
        ticker="TEST",
        has_bank=False,
        industry_keywords=tuple(),
        segment_patterns=tuple(),
        segment_alias_patterns=tuple(),
        key_adv_require_keywords=tuple(),
        key_adv_deny_keywords=tuple(),
    )
    ok, reason = supports_source_material_refresh(profile)
    assert not ok
    assert "official source seeds" in reason


def test_format_refresh_summary_includes_counts() -> None:
    summary = SourceMaterialRefreshSummary(
        ticker="PBI",
        filings_added=1,
        filings_refreshed=2,
        filings_skipped=3,
        material_added=4,
        material_skipped=5,
    )
    text = format_refresh_summary(summary)
    assert "ticker=PBI" in text
    assert "filings_refreshed=2" in text
    assert "material_added=4" in text


def test_stock_models_cli_refresh_source_materials_branch(monkeypatch, capsys) -> None:
    import stock_models as sm
    case_dir = _make_case_dir()

    try:
        summary = SourceMaterialRefreshSummary(ticker="PBI", material_added=1)
        summary.coverage_lines = ["[source_materials] coverage ticker=PBI quarter=2025-12-31 release=Y press=N presentation=N transcript=N missing=earnings_presentation,earnings_transcripts"]
        monkeypatch.setattr(
            sm,
            "refresh_source_materials",
            lambda **kwargs: [summary],
        )
        monkeypatch.setattr(sm, "_project_root", lambda: case_dir)
        monkeypatch.setattr(sm, "_require_user_agent", lambda ua: ua)
        monkeypatch.setattr(sys, "argv", ["stock_models.py", "--ticker", "PBI", "--refresh-source-materials", "--dry-run"])

        sm.main()
        out = capsys.readouterr().out
        assert "ticker=PBI" in out
        assert "material_added=1" in out
        assert "coverage ticker=PBI" in out
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_normalize_and_collect_local_materials_merges_transcript_alias_and_renames_file() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "GPRE"
        alias_dir = material_root / "earnings_transcript"
        alias_dir.mkdir(parents=True, exist_ok=True)
        src = alias_dir / "GPRE-Q4-2025-Earnings-Call-Transcript.pdf"
        src.write_bytes(b"%PDF-1.4 fake transcript")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="GPRE",
            manifest={},
            dry_run=False,
        )

        final_path = material_root / "earnings_transcripts" / "GPRE_Q4_2025_transcript.pdf"
        assert final_path.exists()
        assert any("earnings_transcript" in row["from_path"] for row in result.moved_files)
        assert any(row["to_path"].endswith("GPRE_Q4_2025_transcript.pdf") for row in result.renamed_files)
        families = {cand.canonical_family for cand in result.candidates}
        assert "earnings_transcripts" in families
        assert any((cand.quarter.isoformat() if cand.quarter else "") == "2025-12-31" for cand in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_normalize_and_collect_local_materials_renames_manual_presentations_and_ceo_letters() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI"
        pres_dir = material_root / "earnings_presentation"
        ceo_dir = material_root / "CEO_letters"
        pres_dir.mkdir(parents=True, exist_ok=True)
        ceo_dir.mkdir(parents=True, exist_ok=True)
        (pres_dir / "Q2_202024_20Earnings_20Financial_20Schedules.pdf").write_bytes(b"%PDF-1.4 q2")
        (pres_dir / "Q4 2024 Earnings Slides_.pdf").write_bytes(b"%PDF-1.4 q4")
        (ceo_dir / "Q3 2025 Earnings CEO Letter.pdf").write_bytes(b"%PDF-1.4 ceo")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        assert (pres_dir / "PBI_Q2_2024_earnings_presentation_financial_schedules.pdf").exists()
        assert (pres_dir / "PBI_Q4_2024_earnings_presentation.pdf").exists()
        assert (material_root / "ceo_letters" / "PBI_Q3_2025_ceo_letter.pdf").exists()
        assert any(row["to_path"].endswith("PBI_Q2_2024_earnings_presentation_financial_schedules.pdf") for row in result.renamed_files)
        assert any(row["to_path"].endswith("PBI_Q3_2025_ceo_letter.pdf") for row in result.renamed_files)
        ceo_candidates = [cand for cand in result.candidates if cand.subject_slug == "ceo_letter"]
        assert ceo_candidates
        assert ceo_candidates[0].canonical_family == "earnings_release"
        assert ceo_candidates[0].quarter.isoformat() == "2025-09-30"
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_normalize_and_collect_local_materials_marks_quarter_unknown_manual_file_for_review() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "earnings_presentation"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "InvestorDeck.pdf"
        path_in.write_bytes(b"%PDF-1.4 unknown quarter")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        assert path_in.exists()
        assert not result.candidates
        assert any(row["reason"] == "quarter_not_clear" for row in result.manual_review_files)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_normalize_and_collect_local_materials_moves_annual_report_to_annual_reports() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "GPRE" / "earnings_presentation"
        material_root.mkdir(parents=True, exist_ok=True)
        annual_report = material_root / "Green-Plains_Annual-Report-2024_WR.pdf"
        annual_report.write_bytes(b"%PDF-1.4 annual report")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="GPRE",
            manifest={},
            dry_run=False,
        )

        final_path = case_dir / "GPRE" / "annual_reports" / "GPRE_2024_annual_report.pdf"
        assert final_path.exists()
        assert not any(c.canonical_family == "annual_reports" for c in result.candidates)
        assert any(row["to_path"].endswith("GPRE_2024_annual_report.pdf") for row in result.renamed_files)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_annual_report_named_file_with_quarter_presentation_content_moves_back_to_presentation(monkeypatch) -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "annual_reports"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "PBI_2025_annual_report.pdf"
        path_in.write_bytes(b"%PDF-1.4 deck")
        monkeypatch.setattr(
            smr,
            "extract_pdf_text_cached",
            lambda *args, **kwargs: "Pitney Bowes First Quarter Earnings May 7, 2025 Financial Presentation",
        )

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        final_path = case_dir / "PBI" / "earnings_presentation" / "PBI_Q1_2025_earnings_presentation.pdf"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_presentation" and c.quarter.isoformat() == "2025-03-31" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_normalize_and_collect_local_materials_reports_exact_duplicate_without_deleting() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "GPRE"
        canonical_dir = material_root / "earnings_transcripts"
        alias_dir = material_root / "earnings_transcript"
        canonical_dir.mkdir(parents=True, exist_ok=True)
        alias_dir.mkdir(parents=True, exist_ok=True)
        existing = canonical_dir / "GPRE_Q4_2025_transcript.txt"
        duplicate = alias_dir / "GPRE_Q4_2025_transcript.txt"
        existing.write_text("same transcript", encoding="utf-8")
        duplicate.write_text("same transcript", encoding="utf-8")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="GPRE",
            manifest={},
            dry_run=False,
        )

        assert existing.exists()
        assert duplicate.exists()
        assert any(row["reason"] == "exact_duplicate_same_content" for row in result.duplicate_files)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_manual_local_candidates_flow_into_manifest_and_coverage_report() -> None:
    case_dir = _make_case_dir()
    try:
        repo_root = case_dir
        material_root = repo_root / "PBI"
        (material_root / "earnings_presentation").mkdir(parents=True, exist_ok=True)
        (material_root / "earnings_transcripts").mkdir(parents=True, exist_ok=True)
        (material_root / "CEO_letters").mkdir(parents=True, exist_ok=True)
        (material_root / "earnings_presentation" / "Q4 2025 Earnings Presentation.pdf").write_bytes(b"%PDF-1.4 deck")
        (material_root / "earnings_transcripts" / "PB_Q4_2025_transcript.txt").write_text("Q4 2025 earnings call transcript", encoding="utf-8")
        (material_root / "CEO_letters" / "Q4 2025 Earnings CEO Letter.pdf").write_bytes(b"%PDF-1.4 ceo")

        local_scan = smr._normalize_and_collect_local_materials(
            repo_root=repo_root,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )
        manifest: dict[str, dict[str, object]] = {}
        for cand in local_scan.candidates:
            smr._upsert_manual_local_candidate(
                manifest=manifest,
                candidate=cand,
                ticker="PBI",
                dry_run=False,
            )

        filings_df = pd.DataFrame(
            [
                {"base_form": "10-K", "form": "10-K", "reportDate": "2025-12-31"},
                {"base_form": "10-Q", "form": "10-Q", "reportDate": "2025-09-30"},
            ]
        )
        report = _build_coverage_report(
            ticker="PBI",
            manifest=manifest,
            filings_df=filings_df,
            max_quarters=4,
            local_scan=local_scan,
        )

        qrows = {row["quarter"]: row for row in report["quarters"]}
        assert qrows["2025-12-31"]["release_found"]
        assert qrows["2025-12-31"]["presentation_found"]
        assert qrows["2025-12-31"]["transcript_found"]
        assert any("PBI_Q4_2025_earnings_presentation.pdf" in row["destination_path"] for row in qrows["2025-12-31"]["materials"]["earnings_presentation"])
        assert any("PBI_Q4_2025_transcript.txt" in row["destination_path"] for row in qrows["2025-12-31"]["materials"]["earnings_transcripts"])
        assert report["renamed_files"]
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_misfiled_quarter_ceo_letter_moves_to_ceo_letters_and_counts_as_release() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "press_release"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "PBI_Q3_2025_ceo_letter.htm"
        path_in.write_text("<html><title>Q3 2025 Earnings CEO Letter</title></html>", encoding="utf-8")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        final_path = case_dir / "PBI" / "ceo_letters" / "PBI_Q3_2025_ceo_letter.htm"
        assert final_path.exists()
        assert any(row["to_path"].endswith("PBI_Q3_2025_ceo_letter.htm") for row in result.renamed_files)
        ceo_candidates = [cand for cand in result.candidates if cand.subject_slug == "ceo_letter"]
        assert ceo_candidates
        assert ceo_candidates[0].canonical_family == "earnings_release"
        assert ceo_candidates[0].quarter.isoformat() == "2025-09-30"
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_local_presentation_folder_can_rescue_quarter_file_with_weak_name() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "GPRE" / "earnings_presentation"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "GPRE-Q2-2023-FINAL.pdf"
        path_in.write_bytes(b"%PDF-1.4 deck")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="GPRE",
            manifest={},
            dry_run=False,
        )

        final_path = material_root / "GPRE_Q2_2023_earnings_presentation.pdf"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_presentation" and c.quarter.isoformat() == "2023-06-30" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_local_presentation_folder_recognizes_one_q_two_digit_year_pattern() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "GPRE" / "earnings_presentation"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "1Q22-Earnings_vFINAL.pdf"
        path_in.write_bytes(b"%PDF-1.4 deck")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="GPRE",
            manifest={},
            dry_run=False,
        )

        final_path = material_root / "GPRE_Q1_2022_earnings_presentation.pdf"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_presentation" and c.quarter.isoformat() == "2022-03-31" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_misfiled_date_led_transcript_pdf_moves_to_transcripts_with_pdf_text(monkeypatch) -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "earnings_presentation"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "2022-Apr-28-PBI.N-139636302806-Transcript.pdf"
        path_in.write_bytes(b"%PDF-1.4 transcript")
        monkeypatch.setattr(
            smr,
            "extract_pdf_text_cached",
            lambda *args, **kwargs: "REFINITIV STREETEVENTS Q1 2022 Pitney Bowes Inc Earnings Call",
        )

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        final_path = case_dir / "PBI" / "earnings_transcripts" / "PBI_Q1_2022_transcript.pdf"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_transcripts" and c.quarter.isoformat() == "2022-03-31" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_financial_schedules_get_distinct_suffix_to_avoid_presentation_collision() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "earnings_presentation"
        material_root.mkdir(parents=True, exist_ok=True)
        (material_root / "Q2 2022 Earnings Slides.pdf").write_bytes(b"%PDF-1.4 slides")
        (material_root / "Q2 2022 Earnings Financial Schedules.pdf").write_bytes(b"%PDF-1.4 schedules")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        assert (material_root / "PBI_Q2_2022_earnings_presentation.pdf").exists()
        assert (material_root / "PBI_Q2_2022_earnings_presentation_financial_schedules.pdf").exists()
        assert len(result.duplicate_files) == 0
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_local_transcript_folder_can_rescue_earnings_call_pdf_without_transcript_word() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "GPRE" / "earnings_transcript"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "Green-Plains-Inc-Green-Plains-Partners-LP-Q1-2023-Earnings-Call-May-04-2023.pdf"
        path_in.write_bytes(b"%PDF-1.4 transcript")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="GPRE",
            manifest={},
            dry_run=False,
        )

        final_path = case_dir / "GPRE" / "earnings_transcripts" / "GPRE_Q1_2023_transcript.pdf"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_transcripts" and c.quarter.isoformat() == "2023-03-31" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_transcript_text_mentioning_ceo_letter_does_not_override_transcript_family() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "earnings_transcripts"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "PB_Q4_2025_transcript.txt"
        path_in.write_text(
            "Full transcript - Pitney Bowes (PBI) Q4 2025. We are now issuing a short CEO letter to accompany our press release.",
            encoding="utf-8",
        )

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        final_path = material_root / "PBI_Q4_2025_transcript.txt"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_transcripts" and c.quarter.isoformat() == "2025-12-31" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_explicit_release_title_overrides_stale_ceo_letter_filename_for_local_html() -> None:
    case_dir = _make_case_dir()
    try:
        material_root = case_dir / "PBI" / "press_release"
        material_root.mkdir(parents=True, exist_ok=True)
        path_in = material_root / "PBI_Q3_2025_ceo_letter.htm"
        path_in.write_text("<html><title>q32025earningspressrelea</title></html>", encoding="utf-8")

        result = smr._normalize_and_collect_local_materials(
            repo_root=case_dir,
            ticker="PBI",
            manifest={},
            dry_run=False,
        )

        final_path = case_dir / "PBI" / "earnings_release" / "PBI_Q3_2025_earnings_release.htm"
        assert final_path.exists()
        assert any(c.canonical_family == "earnings_release" and c.quarter.isoformat() == "2025-09-30" for c in result.candidates)
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)
