from __future__ import annotations

import datetime as dt
import shutil
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pandas as pd
from pandas.testing import assert_frame_equal

from pbi_xbrl import excel_writer, pipeline, pipeline_orchestration
from pbi_xbrl.pipeline_qa import finalize_needs_review, finalize_qa_checks
from pbi_xbrl.pipeline_runtime import (
    PipelineStageCache,
    path_belongs_to_ticker,
    resolve_pipeline_roots,
)
from pbi_xbrl.pipeline_types import PipelineArtifacts, PipelineConfig, WorkbookInputs


def test_local_non_gaap_metric_filter_skips_only_fully_covered_quarters_and_keeps_order() -> None:
    existing_metrics = {
        pd.Timestamp("2025-12-31"): {"adj_ebitda", "adj_ebit", "adj_eps", "adj_fcf"},
        pd.Timestamp("2025-03-31"): {"adj_ebitda"},
    }

    kept = pipeline_orchestration._filter_missing_local_non_gaap_metric_quarters(
        [
            pd.Timestamp("2025-12-31").date(),
            pd.Timestamp("2025-09-30").date(),
            pd.Timestamp("2025-09-30").date(),
            pd.Timestamp("2025-06-30").date(),
            pd.Timestamp("2025-03-31").date(),
        ],
        existing_metrics,
    )

    assert kept == [
        pd.Timestamp("2025-09-30").date(),
        pd.Timestamp("2025-06-30").date(),
        pd.Timestamp("2025-03-31").date(),
    ]


def test_local_non_gaap_pruning_keeps_only_missing_metrics_without_overwriting_strict() -> None:
    strict_metrics = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "adj_ebitda": 100.0,
                "adj_ebit": 80.0,
                "adj_eps": pd.NA,
                "adj_fcf": pd.NA,
            }
        ]
    )
    local_metrics = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "adj_ebitda": 95.0,
                "adj_ebit": 78.0,
                "adj_eps": 1.25,
                "adj_fcf": 12.0,
            }
        ]
    )

    existing_metrics = pipeline_orchestration._existing_local_non_gaap_metrics_by_quarter(strict_metrics)
    pruned = pipeline_orchestration._prune_local_non_gaap_metrics_against_existing(local_metrics, existing_metrics)

    assert len(pruned) == 1
    assert pd.isna(pruned.loc[0, "adj_ebitda"])
    assert pd.isna(pruned.loc[0, "adj_ebit"])
    assert pruned.loc[0, "adj_eps"] == 1.25
    assert pruned.loc[0, "adj_fcf"] == 12.0


def test_local_non_gaap_pruning_drops_rows_when_no_metric_gap_remains() -> None:
    strict_metrics = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "adj_ebitda": 100.0,
                "adj_ebit": 80.0,
                "adj_eps": 1.2,
                "adj_fcf": 11.0,
            }
        ]
    )
    local_metrics = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "adj_ebitda": 95.0,
                "adj_ebit": 78.0,
                "adj_eps": 1.25,
                "adj_fcf": 12.0,
            }
        ]
    )

    existing_metrics = pipeline_orchestration._existing_local_non_gaap_metrics_by_quarter(strict_metrics)
    pruned = pipeline_orchestration._prune_local_non_gaap_metrics_against_existing(local_metrics, existing_metrics)

    assert pruned.empty


def test_local_non_gaap_pdf_cache_layout_is_stable_for_slides_and_other_sources() -> None:
    with _case_dir() as case_dir:
        slide_text_dir, slide_ocr_dir = pipeline_orchestration._local_non_gaap_pdf_cache_dirs(case_dir, "slides")
        other_text_dir, other_ocr_dir = pipeline_orchestration._local_non_gaap_pdf_cache_dirs(case_dir, "earnings_release")

        assert slide_text_dir.name == "slides_text"
        assert slide_ocr_dir.name == "slides_ocr"
        assert other_text_dir.name == "local_non_gaap_earnings_release_text"
        assert other_ocr_dir.name == "local_non_gaap_earnings_release_ocr"

        pdf_path = case_dir / "materials" / "demo deck.pdf"
        slide_key = pipeline_orchestration._local_non_gaap_pdf_cache_key(pdf_path, src_name="slides", page_number=2)
        other_key = pipeline_orchestration._local_non_gaap_pdf_cache_key(pdf_path, src_name="earnings_release", page_number=2)
        other_key_again = pipeline_orchestration._local_non_gaap_pdf_cache_key(pdf_path, src_name="earnings_release", page_number=2)

        assert slide_key == "demo deck_p2"
        assert other_key == other_key_again
        assert other_key.endswith("_p2")
        assert other_key != slide_key


def test_local_non_gaap_page_scores_detect_debt_profile_markers_from_slide_text() -> None:
    text = """
    Select Balance Sheet Data
    Working capital financing consists of revolvers for the Finance Company and Trade Group.
    Long-term debt includes convertible debt $228.2 million, junior mezzanine notes $130.7 million and term loan balances.
    """

    scores = pipeline_orchestration._local_non_gaap_page_scores(text)

    assert scores["debt"] >= 2


def test_local_non_gaap_debt_source_allows_annual_reports_only_when_financial_statement_missing() -> None:
    assert pipeline_orchestration._local_non_gaap_debt_source_allowed(
        "annual_reports",
        has_financial_statement_files=False,
    )
    assert not pipeline_orchestration._local_non_gaap_debt_source_allowed(
        "annual_reports",
        has_financial_statement_files=True,
    )
    assert pipeline_orchestration._local_non_gaap_debt_source_allowed(
        "slides",
        has_financial_statement_files=True,
    )


def test_parse_financial_statement_debt_table_html_extracts_modern_debt_rows() -> None:
    with _case_dir() as case_dir:
        path_in = case_dir / "GPRE_FY2025_10K_2025-12-31_financial_statement.htm"
        path_in.write_text(
            """
            <html><body>
            <p>The initial conversion rate is 31.6206 shares per $1,000 principal amount of the 2.25% notes.</p>
            <table>
              <tr><th>Corporate</th><th>2025</th><th>2024</th></tr>
              <tr><td>2.25% convertible notes due 2027 (1)</td><td>60,000</td><td>230,000</td></tr>
              <tr><td>5.25% convertible notes due 2030 (2)</td><td>200,000</td><td>&mdash;</td></tr>
              <tr><td>Term loan due 2035 (4)</td><td>70,125</td><td>71,625</td></tr>
              <tr><td>Tallgrass Term loan due 2037</td><td>34,523</td><td>&mdash;</td></tr>
              <tr><td>Other</td><td>9,842</td><td>11,163</td></tr>
              <tr><td>Total long-term debt</td><td>374,490</td><td>437,788</td></tr>
            </table>
            </body></html>
            """,
            encoding="utf-8",
        )

        rows = pipeline_orchestration._parse_financial_statement_debt_table_html(
            path_in,
            dt.date(2025, 12, 31),
        )

        assert len(rows) == 5
        assert rows[0]["tranche"] == "2.25% convertible notes due 2027 (1)"
        assert rows[0]["amount"] == 60_000_000.0
        assert rows[1]["maturity_year"] == 2030
        assert rows[-1]["tranche"] == "Other"
        assert all(row["quarter"] == dt.date(2025, 12, 31) for row in rows)


def test_parse_financial_statement_debt_table_html_skips_interest_rate_cells() -> None:
    with _case_dir() as case_dir:
        path_in = case_dir / "PBI_FY2025_10K_2025-12-31_financial_statement.htm"
        path_in.write_text(
            """
            <html><body>
            <table>
              <tr><th></th><th></th><th></th><th>Interest rate</th><th>Interest rate</th><th>Interest rate</th><th>2025</th><th>2025</th><th>2024</th><th>2024</th></tr>
              <tr><td>Notes due March 2027</td><td>Notes due March 2027</td><td>Notes due March 2027</td><td>6.875%</td><td>6.875%</td><td>6.875%</td><td>346,700</td><td>346,700</td><td>380,000</td><td>380,000</td></tr>
              <tr><td>Convertible Notes due August 2030</td><td>Convertible Notes due August 2030</td><td>Convertible Notes due August 2030</td><td>1.50%</td><td>1.50%</td><td>1.50%</td><td>230,000</td><td>230,000</td><td>&mdash;</td><td>&mdash;</td></tr>
            </table>
            </body></html>
            """,
            encoding="utf-8",
        )

        rows = pipeline_orchestration._parse_financial_statement_debt_table_html(
            path_in,
            dt.date(2025, 12, 31),
        )

        assert len(rows) == 2
        assert rows[0]["amount"] == 346_700_000.0
        assert rows[1]["amount"] == 230_000_000.0
        assert rows[1]["maturity_year"] == 2030


def test_limit_recent_financial_statement_debt_rows_trims_old_statement_quarters() -> None:
    df = pd.DataFrame(
        [
            {"quarter": "2021-03-31", "tranche": "Old A", "source": "financial_statement"},
            {"quarter": "2022-03-31", "tranche": "Old B", "source": "financial_statement"},
            {"quarter": "2023-03-31", "tranche": "Mid A", "source": "financial_statement"},
            {"quarter": "2024-03-31", "tranche": "Mid B", "source": "financial_statement"},
            {"quarter": "2025-03-31", "tranche": "New A", "source": "financial_statement"},
            {"quarter": "2025-06-30", "tranche": "New B", "source": "financial_statement"},
            {"quarter": "2025-09-30", "tranche": "New C", "source": "financial_statement"},
            {"quarter": "2025-12-31", "tranche": "New D", "source": "financial_statement"},
            {"quarter": "2020-12-31", "tranche": "Slide row", "source": "slides"},
        ]
    )

    out = pipeline_orchestration._limit_recent_financial_statement_debt_rows(
        df,
        max_recent_quarters=4,
    )

    kept = {(str(q), str(t), str(s)) for q, t, s in out[["quarter", "tranche", "source"]].itertuples(index=False, name=None)}
    assert ("2021-03-31", "Old A", "financial_statement") not in kept
    assert ("2022-03-31", "Old B", "financial_statement") not in kept
    assert ("2024-03-31", "Mid B", "financial_statement") not in kept
    assert ("2025-12-31", "New D", "financial_statement") in kept
    assert ("2020-12-31", "Slide row", "slides") in kept


def test_drop_financial_statement_debt_rows_covered_by_slides() -> None:
    df = pd.DataFrame(
        [
            {"quarter": "2025-12-31", "tranche": "Slide A", "source": "slides"},
            {"quarter": "2025-12-31", "tranche": "FS A", "source": "financial_statement"},
            {"quarter": "2025-09-30", "tranche": "FS B", "source": "financial_statement"},
            {"quarter": "2025-06-30", "tranche": "Deck", "source": "slides"},
            {"quarter": "2025-06-30", "tranche": "FS C", "source": "financial_statement"},
        ]
    )

    out = pipeline_orchestration._drop_financial_statement_debt_rows_covered_by_slides(df)
    kept = {(str(q), str(t), str(s)) for q, t, s in out[["quarter", "tranche", "source"]].itertuples(index=False, name=None)}

    assert ("2025-12-31", "FS A", "financial_statement") not in kept
    assert ("2025-06-30", "FS C", "financial_statement") not in kept
    assert ("2025-09-30", "FS B", "financial_statement") in kept
    assert ("2025-12-31", "Slide A", "slides") in kept


def test_local_non_gaap_header_dates_parse_month_name_slide_headers() -> None:
    dates = pipeline_orchestration._parse_local_non_gaap_header_dates(
        "For the period ending Sep. 30, 2025 Dec. 31, 2024"
    )

    assert dates == [dt.date(2025, 9, 30), dt.date(2024, 12, 31)]


def test_local_non_gaap_infers_annual_report_period_end_from_filename() -> None:
    assert pipeline_orchestration._infer_local_non_gaap_period_end_from_name(
        "GPRE_2024_annual_report.pdf"
    ) == dt.date(2024, 12, 31)


class _FakeEx99Sec:
    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = cache_dir
        self.index_map: dict[str, dict[str, object]] = {}
        self.doc_map: dict[tuple[str, str], bytes] = {}
        self.image_map: dict[str, list[str]] = {}
        self.ocr_text_map: dict[str, str] = {}
        self.ocr_log_rows: list[dict[str, object]] = []
        self.calls = {
            "accession_index_json": 0,
            "download_document": 0,
            "download_index_images": 0,
            "ocr_html_assets": 0,
        }

    def accession_index_json(self, cik_int: int, accn_nd: str) -> dict[str, object]:
        self.calls["accession_index_json"] += 1
        return self.index_map[accn_nd]

    def download_document(self, cik_int: int, accn_nd: str, doc_name: str) -> bytes:
        self.calls["download_document"] += 1
        return self.doc_map[(accn_nd, doc_name)]

    def download_html_assets(self, cik_int: int, accn_nd: str, data: bytes) -> None:
        return None

    def download_index_images(self, cik_int: int, accn_nd: str, idx: dict[str, object]) -> list[str]:
        self.calls["download_index_images"] += 1
        return list(self.image_map.get(accn_nd, []))

    def ocr_html_assets(self, accn_nd: str, data, context: dict[str, object]) -> str:
        self.calls["ocr_html_assets"] += 1
        doc = str(context.get("doc") or "")
        return self.ocr_text_map.get(doc, self.ocr_text_map.get("*", ""))


def _submission_recent(rows: list[dict[str, str]]) -> dict[str, object]:
    return {
        "filings": {
            "recent": {
                "form": [row["form"] for row in rows],
                "accessionNumber": [row["accn"] for row in rows],
                "reportDate": [row.get("report_date") for row in rows],
                "filingDate": [row.get("filing_date") for row in rows],
                "primaryDocument": [row.get("primary_doc") for row in rows],
            }
        }
    }


def test_build_company_overview_prefers_original_10k_over_administrative_10ka() -> None:
    sec = _FakeEx99Sec()
    old_accn = "0000123456-26-000003"
    amend_accn = "0000123456-26-000004"
    submissions = _submission_recent(
        [
            {"form": "10-K", "accn": old_accn, "report_date": "2025-12-31", "filing_date": "2026-02-19", "primary_doc": "orig10k.htm"},
            {"form": "10-K/A", "accn": amend_accn, "report_date": "2025-12-31", "filing_date": "2026-02-20", "primary_doc": "amend10ka.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(old_accn), "orig10k.htm")] = (
        b"<html><body>Item 1. Business Pitney Bowes is a technology-driven shipping and mailing company. "
        b"SendTech Solutions provides shipping and mailing technology, software, supplies, services and financing. "
        b"Presort Services sorts client mail to help clients qualify for postal workshare discounts. "
        b"Item 1A. Risk Factors USPS pricing changes and mail-volume declines may adversely affect results.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(amend_accn), "amend10ka.htm")] = (
        b"<html><body>This Amendment No. 1 is being filed solely for the purpose of correcting the office location "
        b"in the auditor's report. No other changes are being made. Item 1. Business unchanged.</body></html>"
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="PBI")

    assert "Pitney Bowes" in overview["what_it_does"]
    assert old_accn in overview["what_it_does_source"]
    assert amend_accn not in overview["what_it_does_source"]


def test_build_company_overview_uses_earnings_ex99_for_current_context_and_10k_for_business() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000010"
    eightk_accn = "0000123456-26-000011"
    submissions = _submission_recent(
        [
            {"form": "8-K", "accn": eightk_accn, "report_date": "2026-02-05", "filing_date": "2026-02-05", "primary_doc": "gpre-20260205.htm"},
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-10", "primary_doc": "gpre-20251231.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "gpre-20251231.htm")] = (
        b"<html><body>Item 1. Business Green Plains is a renewable fuels and agricultural technology company. "
        b"It operates through Ethanol Production and Agribusiness and Energy Services. "
        b"The company converts corn into low-carbon ethanol and related co-products including protein and corn oil. "
        b"Competition We compete through our biorefinery platform, low-carbon positioning, co-products and operational scale. "
        b"Item 1A. Risk Factors Commodity spreads, carbon-intensity scores and plant uptime may affect results.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "gpre-20260205.htm")] = (
        b"<html><body>Item 2.02 Results of Operations. The company issued a press release attached as Exhibit 99.1.</body></html>"
    )
    sec.index_map[pipeline.normalize_accession(eightk_accn)] = {
        "directory": {"item": [{"name": "gpre-q42025earningsrelease.htm"}, {"name": "ex99-2shareholderletter.htm"}]}
    }
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "gpre-q42025earningsrelease.htm")] = (
        b"<html><body>Green Plains is focused on improving low-carbon value realization through 45Z, carbon capture and commercialization initiatives over the next several quarters.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "ex99-2shareholderletter.htm")] = (
        b"<html><body>Management remains focused on CCS execution and monetizing low-carbon opportunities.</body></html>"
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="GPRE")

    assert "ethanol production" in overview["what_it_does"].lower()
    assert "agribusiness and energy services" in overview["what_it_does"].lower()
    assert tenk_accn in overview["what_it_does_source"]
    assert "45Z" in overview["current_strategic_context"] or "low-carbon" in overview["current_strategic_context"]
    assert eightk_accn in overview["current_strategic_context_source"]


def test_build_company_overview_current_context_skips_newer_non_earnings_8k_when_older_earnings_release_exists() -> None:
    sec = _FakeEx99Sec()
    earnings_accn = "0000123456-26-000020"
    admin_accn = "0000123456-26-000021"
    tenk_accn = "0000123456-26-000022"
    submissions = _submission_recent(
        [
            {"form": "8-K", "accn": admin_accn, "report_date": "2026-02-25", "filing_date": "2026-02-25", "primary_doc": "board-update.htm"},
            {"form": "8-K", "accn": earnings_accn, "report_date": "2026-02-05", "filing_date": "2026-02-05", "primary_doc": "earnings8k.htm"},
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-10", "primary_doc": "orig10k.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "orig10k.htm")] = (
        b"<html><body>Item 1. Business Pitney Bowes is a technology-driven shipping and mailing company. "
        b"SendTech Solutions provides shipping and mailing technology, software and financing. "
        b"Presort Services sorts client mail to help clients qualify for postal workshare discounts. "
        b"Competition Pitney Bowes benefits from its installed base, presort network and integrated workflows. "
        b"Item 1A. Risk Factors Postal pricing changes and mail-volume declines may affect results.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(admin_accn), "board-update.htm")] = (
        b"<html><body>Item 5.02 Departure of Directors or Certain Officers. Everett will focus on leveraging his expertise "
        b"in operational excellence and knowledge of the shipping software space.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(earnings_accn), "earnings8k.htm")] = (
        b"<html><body>Item 2.02 Results of Operations and Financial Condition. A press release is attached as Exhibit 99.1. "
        b"A CEO letter is attached as Exhibit 99.2.</body></html>"
    )
    sec.index_map[pipeline.normalize_accession(earnings_accn)] = {
        "directory": {"item": [{"name": "q42025earningspressrelea.htm"}, {"name": "q42025earningsceoletter.htm"}]}
    }
    sec.doc_map[(pipeline.normalize_accession(earnings_accn), "q42025earningspressrelea.htm")] = (
        b"<html><body>Pitney Bowes is focused on capital allocation, debt reduction and disciplined execution over the next several quarters.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(earnings_accn), "q42025earningsceoletter.htm")] = (
        b"<html><body>Management remains focused on cost discipline and portfolio execution while improving free cash flow.</body></html>"
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="PBI")

    assert "capital allocation" in overview["current_strategic_context"].lower()
    assert earnings_accn in overview["current_strategic_context_source"]
    assert admin_accn not in overview["current_strategic_context_source"]


def test_build_company_overview_current_context_synthesizes_industrial_focus_themes() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000025"
    eightk_accn = "0000123456-26-000026"
    submissions = _submission_recent(
        [
            {"form": "8-K", "accn": eightk_accn, "report_date": "2026-02-04", "filing_date": "2026-02-04", "primary_doc": "pbi-earnings8k.htm"},
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-19", "primary_doc": "pbi-20251231.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "pbi-20251231.htm")] = (
        b"<html><body>Item 1. Business Pitney Bowes is a technology-driven shipping and mailing company. "
        b"SendTech Solutions provides shipping and mailing technology, software and financing. "
        b"Presort Services sorts client mail to qualify for postal discounts. "
        b"Item 1A. Risk Factors Postal pricing changes and leverage may affect results.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "pbi-earnings8k.htm")] = (
        b"<html><body>Item 2.02 Results of Operations. The company issued a press release attached as Exhibit 99.1.</body></html>"
    )
    sec.index_map[pipeline.normalize_accession(eightk_accn)] = {
        "directory": {"item": [{"name": "pbi-q4-2025earningsrelease.htm"}, {"name": "pbi-ceo-letter.htm"}]}
    }
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "pbi-q4-2025earningsrelease.htm")] = (
        b"<html><body>Management remains focused on capital allocation, debt reduction and cost discipline as it improves execution into 2026.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "pbi-ceo-letter.htm")] = (
        b"<html><body>This effort will allow us to improve capital allocation and provide more accurate guidance to investors beginning with our 2026 guidance.</body></html>"
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="PBI")

    low = overview["current_strategic_context"].lower()
    assert "capital allocation" in low
    assert "cost discipline" in low
    assert ("debt reduction" in low) or ("guidance" in low)


def test_build_company_overview_current_context_synthesizes_biofuels_45z_and_ccs() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000027"
    eightk_accn = "0000123456-26-000028"
    submissions = _submission_recent(
        [
            {"form": "8-K", "accn": eightk_accn, "report_date": "2026-02-05", "filing_date": "2026-02-05", "primary_doc": "gpre-earnings8k.htm"},
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-10", "primary_doc": "gpre-20251231.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "gpre-20251231.htm")] = (
        b"<html><body>Item 1. Business Green Plains is a renewable fuels and agricultural technology company. "
        b"It operates through Ethanol Production and Agribusiness and Energy Services. "
        b"Item 1A. Risk Factors Commodity spreads and carbon-intensity scores may affect results.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "gpre-earnings8k.htm")] = (
        b"<html><body>Item 2.02 Results of Operations. The company issued a press release attached as Exhibit 99.1.</body></html>"
    )
    sec.index_map[pipeline.normalize_accession(eightk_accn)] = {
        "directory": {"item": [{"name": "gpre-q42025earningsrelease.htm"}, {"name": "ex99-2shareholderletter.htm"}]}
    }
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "gpre-q42025earningsrelease.htm")] = (
        b"<html><body>Based on current production outlook and eligible gallons the company expects to generate at least $188 million of 45Z-related Adjusted EBITDA in 2026.</body></html>"
    )
    sec.doc_map[(pipeline.normalize_accession(eightk_accn), "ex99-2shareholderletter.htm")] = (
        b"<html><body>Management remains focused on CCS execution and broader low-carbon value realization.</body></html>"
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="GPRE")

    low = overview["current_strategic_context"].lower()
    assert "45z" in low
    assert "ccs" in low


def test_build_company_overview_key_advantage_prefers_broader_platform_sentence_over_narrow_finance_competition() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000030"
    submissions = _submission_recent(
        [
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-19", "primary_doc": "orig10k.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "orig10k.htm")] = (
        b"<html><body>Item 1. Business Pitney Bowes combines a large installed base, software-enabled workflows and a national presort network "
        b"that support recurring service revenue and customer retention. SendTech Solutions provides shipping and mailing technology, software and financing. "
        b"Presort Services sorts client mail to help clients qualify for workshare discounts. "
        b"Competition Our financing operations face competition, in varying degrees, from large, diversified financial institutions, leasing companies, "
        b"commercial finance companies, commercial banks and smaller specialized firms. Item 1A. Risk Factors Postal pricing changes may affect results.</body></html>"
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="PBI")

    assert "installed base" in overview["key_advantage"].lower()
    assert "financing" in overview["key_advantage"].lower()
    assert "commercial banks" not in overview["key_advantage"].lower()


def test_build_company_overview_revenue_streams_prefers_segment_named_table_over_metric_table() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000040"
    submissions = _submission_recent(
        [
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-19", "primary_doc": "orig10k.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "orig10k.htm")] = (
        b"""<html><body>
        <table>
          <tr><th>Metric</th><th>2025</th></tr>
          <tr><td>Revenue</td><td>3400</td></tr>
          <tr><td>Adjusted segment EBIT</td><td>510</td></tr>
        </table>
        <table>
          <tr><th>Segment revenue</th><th>2025</th></tr>
          <tr><td>SendTech Solutions</td><td>1400</td></tr>
          <tr><td>Presort Services</td><td>1800</td></tr>
          <tr><td>Other operations</td><td>200</td></tr>
          <tr><td>Total</td><td>3400</td></tr>
        </table>
        </body></html>"""
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="PBI")
    stream_names = [str(row.get("name") or "") for row in overview["revenue_streams"]]

    assert stream_names[:2] == ["Presort Services", "SendTech Solutions"]
    assert "Revenue" not in stream_names


def test_build_company_overview_revenue_streams_parses_actual_pbi_style_annual_table_layout() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000041"
    submissions = _submission_recent(
        [
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-19", "primary_doc": "orig10k.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "orig10k.htm")] = (
        b"""<html><body>
        <table>
          <tr><td>Revenue</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
          <tr><td>Years Ended December 31,</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
          <tr><td>2025</td><td>2024</td><td>2023</td><td></td><td></td><td></td><td></td></tr>
          <tr><td>SendTech Solutions</td><td>$</td><td>1,256,001</td><td>$</td><td>1,354,032</td><td>$</td><td>1,405,864</td></tr>
          <tr><td>Presort Services</td><td>636,628</td><td>662,587</td><td>617,599</td><td></td><td></td><td></td></tr>
          <tr><td>Total segment revenue</td><td>1,892,629</td><td>2,016,619</td><td>2,023,463</td><td></td><td></td><td></td></tr>
          <tr><td>Other operations</td><td>&mdash;</td><td>9,979</td><td>55,462</td><td></td><td></td><td></td></tr>
          <tr><td>Total revenue</td><td>$</td><td>1,892,629</td><td>$</td><td>2,026,598</td><td>$</td><td>2,078,925</td></tr>
        </table>
        </body></html>"""
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="PBI")
    stream_names = [str(row.get("name") or "") for row in overview["revenue_streams"]]

    assert overview["revenue_streams_period"] == dt.date(2025, 12, 31)
    assert stream_names[:2] == ["SendTech Solutions", "Presort Services"]
    assert float(overview["revenue_streams"][0]["amount"]) == 1256001.0
    assert tenk_accn in overview["revenue_streams_source"]


def test_build_company_overview_revenue_streams_parses_hierarchical_segment_revenue_table_and_uses_fy_period() -> None:
    sec = _FakeEx99Sec()
    tenk_accn = "0000123456-26-000042"
    submissions = _submission_recent(
        [
            {"form": "10-K", "accn": tenk_accn, "report_date": "2025-12-31", "filing_date": "2026-02-19", "primary_doc": "orig10k.htm"},
        ]
    )
    sec.doc_map[(pipeline.normalize_accession(tenk_accn), "orig10k.htm")] = (
        b"""<html><body>
        <table>
          <tr><td>Year Ended December 31,</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
          <tr><td>2025</td><td>2024</td><td>2023</td><td></td><td></td><td></td><td></td></tr>
          <tr><td>Revenues</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
          <tr><td>Ethanol production</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
          <tr><td>Revenues from external customers</td><td>$</td><td>1,900,999</td><td>$</td><td>2,063,382</td><td>$</td><td>2,819,986</td></tr>
          <tr><td>Intersegment revenues</td><td>859</td><td>3,707</td><td>4,555</td><td></td><td></td><td></td></tr>
          <tr><td>Total segment revenues</td><td>1,901,858</td><td>2,067,089</td><td>2,824,541</td><td></td><td></td><td></td></tr>
          <tr><td>Agribusiness and energy services</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
          <tr><td>Revenues from external customers</td><td>190,681</td><td>395,414</td><td>475,757</td><td></td><td></td><td></td></tr>
          <tr><td>Intersegment revenues</td><td>22,662</td><td>25,693</td><td>25,146</td><td></td><td></td><td></td></tr>
          <tr><td>Total segment revenues</td><td>213,343</td><td>421,107</td><td>500,903</td><td></td><td></td><td></td></tr>
          <tr><td>Revenues including intersegment activity</td><td>2,115,201</td><td>2,488,196</td><td>3,325,444</td><td></td><td></td><td></td></tr>
          <tr><td>Intersegment eliminations</td><td>(23,521)</td><td>(29,400)</td><td>(29,701)</td><td></td><td></td><td></td></tr>
          <tr><td>$</td><td>2,091,680</td><td>$</td><td>2,458,796</td><td>$</td><td>3,295,743</td><td></td></tr>
        </table>
        </body></html>"""
    )

    overview = pipeline.build_company_overview(sec, 123456, submissions, ticker="GPRE")
    stream_names = [str(row.get("name") or "") for row in overview["revenue_streams"]]

    assert overview["revenue_streams_period"] == dt.date(2025, 12, 31)
    assert stream_names[:2] == ["Ethanol production", "Agribusiness and energy services"]
    assert float(overview["revenue_streams"][0]["amount"]) == 1900999.0
    assert tenk_accn in overview["revenue_streams_source"]


def test_ex99_inventory_filters_8k_rows_and_target_years(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    accn_a = "0000123456-25-000001"
    accn_b = "0000123456-25-000002"
    accn_old = "0000123456-23-000003"
    sec.index_map = {
        pipeline.normalize_accession(accn_a): {"exdocs": ["ex99-a.htm"]},
        pipeline.normalize_accession(accn_b): {"exdocs": []},
    }
    submissions = _submission_recent(
        [
            {"form": "8-K", "accn": accn_a, "report_date": "2025-03-31", "filing_date": "2025-04-25"},
            {"form": "8-K", "accn": accn_b, "report_date": "2025-06-30", "filing_date": "2025-07-25"},
            {"form": "10-Q", "accn": "0000123456-25-000010", "report_date": "2025-03-31", "filing_date": "2025-05-01"},
            {"form": "8-K", "accn": accn_old, "report_date": "2023-03-31", "filing_date": "2023-04-25"},
        ]
    )
    monkeypatch.setattr(pipeline, "find_ex99_docs", lambda idx: list(idx.get("exdocs", [])))

    runtime_cache = pipeline._make_ex99_runtime_cache()
    inventory = pipeline._build_ex99_accession_inventory(
        sec,
        123456,
        submissions,
        ex99_runtime_cache=runtime_cache,
        target_years={2025, 2026},
    )

    assert [row["accn"] for row in inventory["eight_k_rows"]] == [accn_a, accn_b]
    assert inventory["eight_k_rows"][0]["exdocs"] == ["ex99-a.htm"]
    assert inventory["eight_k_rows"][1]["is_image_only"] is True
    assert inventory["accn_dates"][pipeline.normalize_accession(accn_a)][0] == dt.date(2025, 3, 31)
    assert sec.calls["accession_index_json"] == 2


def test_select_primary_filing_rows_for_ytd_q4_prefers_latest_filing_per_report_date() -> None:
    q4_end = dt.date(2025, 12, 31)
    q3_end = dt.date(2025, 9, 30)
    rows = [
        {
            "form": "10-Q",
            "accn": "0000123456-25-000001",
            "accn_nd": pipeline.normalize_accession("0000123456-25-000001"),
            "report_date": q3_end,
            "filing_date": dt.date(2025, 10, 20),
            "primary_doc": "old-q3.htm",
        },
        {
            "form": "10-Q/A",
            "accn": "0000123456-25-000002",
            "accn_nd": pipeline.normalize_accession("0000123456-25-000002"),
            "report_date": q3_end,
            "filing_date": dt.date(2025, 11, 5),
            "primary_doc": "new-q3.htm",
        },
        {
            "form": "10-K",
            "accn": "0000123456-26-000003",
            "accn_nd": pipeline.normalize_accession("0000123456-26-000003"),
            "report_date": q4_end,
            "filing_date": dt.date(2026, 2, 10),
            "primary_doc": "old-fy.htm",
        },
        {
            "form": "10-K/A",
            "accn": "0000123456-26-000004",
            "accn_nd": pipeline.normalize_accession("0000123456-26-000004"),
            "report_date": q4_end,
            "filing_date": dt.date(2026, 2, 25),
            "primary_doc": "new-fy.htm",
        },
    ]

    selected = pipeline._select_primary_filing_rows_for_ytd_q4(rows, {q4_end})

    assert selected["q3_rows"][q3_end]["accn"] == "0000123456-25-000002"
    assert selected["fy_rows"][q4_end]["accn"] == "0000123456-26-000004"


def test_select_primary_filing_rows_for_ytd_q4_reuses_inventory_index_and_cache() -> None:
    q4_end = dt.date(2025, 12, 31)
    q3_end = dt.date(2025, 9, 30)
    q3_row = {
        "form": "10-Q/A",
        "accn": "0000123456-25-000002",
        "accn_nd": pipeline.normalize_accession("0000123456-25-000002"),
        "report_date": q3_end,
        "filing_date": dt.date(2025, 11, 5),
        "primary_doc": "new-q3.htm",
    }
    fy_row = {
        "form": "10-K/A",
        "accn": "0000123456-26-000004",
        "accn_nd": pipeline.normalize_accession("0000123456-26-000004"),
        "report_date": q4_end,
        "filing_date": dt.date(2026, 2, 25),
        "primary_doc": "new-fy.htm",
    }
    inventory = {
        "rows": [],
        "form_report_index": {
            "10-Q": {q3_end: dict(q3_row)},
            "10-K": {q4_end: dict(fy_row)},
        },
    }
    runtime_cache = pipeline._make_primary_filing_runtime_cache()

    selected_first = pipeline._select_primary_filing_rows_for_ytd_q4(
        inventory,
        {q4_end},
        filing_runtime_cache=runtime_cache,
    )
    inventory["form_report_index"]["10-Q"][q3_end]["accn"] = "mutated"
    inventory["form_report_index"]["10-K"][q4_end]["accn"] = "mutated"
    selected_second = pipeline._select_primary_filing_rows_for_ytd_q4(
        inventory,
        {q4_end},
        filing_runtime_cache=runtime_cache,
    )

    assert selected_first["q3_rows"][q3_end]["accn"] == "0000123456-25-000002"
    assert selected_first["fy_rows"][q4_end]["accn"] == "0000123456-26-000004"
    assert selected_second == selected_first


def test_primary_filing_html_bundle_cache_reuses_document_parse(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    accn = "0000123456-25-000001"
    accn_nd = pipeline.normalize_accession(accn)
    sec.doc_map[(accn_nd, "primary.htm")] = b"<html><body>demo</body></html>"
    runtime_cache = pipeline._make_primary_filing_runtime_cache()
    parse_calls = 0

    def fake_read_html_tables_any(html_bytes: bytes) -> list[pd.DataFrame]:
        nonlocal parse_calls
        parse_calls += 1
        return [pd.DataFrame({"label": ["Revenue"], "2025": [100.0]})]

    monkeypatch.setattr(pipeline, "read_html_tables_any", fake_read_html_tables_any)

    bundle_first = pipeline._load_primary_filing_html_bundle(sec, 123456, accn_nd, "primary.htm", runtime_cache)
    bundle_second = pipeline._load_primary_filing_html_bundle(sec, 123456, accn_nd, "primary.htm", runtime_cache)

    assert bundle_first is bundle_second
    assert parse_calls == 1
    assert sec.calls["download_document"] == 1


def test_income_statement_ytd_q4_fallback_prefers_latest_filing_and_keeps_audit_shape(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    q4_end = dt.date(2025, 12, 31)
    q3_end = dt.date(2025, 9, 30)
    old_q3 = "0000123456-25-000001"
    new_q3 = "0000123456-25-000002"
    old_fy = "0000123456-26-000003"
    new_fy = "0000123456-26-000004"
    submissions = _submission_recent(
        [
            {"form": "10-Q", "accn": old_q3, "report_date": "2025-09-30", "filing_date": "2025-10-20", "primary_doc": "old-q3.htm"},
            {"form": "10-Q/A", "accn": new_q3, "report_date": "2025-09-30", "filing_date": "2025-11-05", "primary_doc": "new-q3.htm"},
            {"form": "10-K", "accn": old_fy, "report_date": "2025-12-31", "filing_date": "2026-02-10", "primary_doc": "old-fy.htm"},
            {"form": "10-K/A", "accn": new_fy, "report_date": "2025-12-31", "filing_date": "2026-02-25", "primary_doc": "new-fy.htm"},
        ]
    )

    def fake_extract(
        sec_obj,
        cik_int,
        accn_nd: str,
        doc_name: str,
        quarter_end: dt.date,
        filing_runtime_cache,
        *,
        rules=None,
        period_hint: str = "3M",
    ) -> dict[str, object] | None:
        if period_hint == "9M":
            if accn_nd == pipeline.normalize_accession(new_q3):
                return {"values": {"revenue": 90.0, "cogs": 45.0}, "labels": {"revenue": "revenue"}}
            return {"values": {"revenue": 10.0, "cogs": 5.0}, "labels": {"revenue": "revenue"}}
        if period_hint == "FY":
            if accn_nd == pipeline.normalize_accession(new_fy):
                return {"values": {"revenue": 140.0, "cogs": 70.0}, "labels": {"revenue": "revenue"}}
            return {"values": {"revenue": 20.0, "cogs": 10.0}, "labels": {"revenue": "revenue"}}
        return None

    monkeypatch.setattr(pipeline, "_extract_income_statement_from_primary_doc_cached", fake_extract)

    out, audit_rows = pipeline.build_income_statement_ytd_q4_fallback(
        sec,
        123456,
        submissions,
        max_quarters=8,
        target_quarters={q4_end},
        filing_runtime_cache=pipeline._make_primary_filing_runtime_cache(),
    )

    assert out[q4_end]["revenue"] == 50.0
    assert out[q4_end]["cogs"] == 25.0
    assert all(row["source"] == "derived_ytd_q4_table" for row in audit_rows)
    assert all(row["quarter"] == q4_end for row in audit_rows)
    assert all(row["accn"] == new_fy for row in audit_rows)
    assert any("Q4 = FY" in str(row.get("note") or "") for row in audit_rows)


def test_legacy_ex99_cache_entries_are_deterministic_and_parse_metadata() -> None:
    with _case_dir() as case_dir:
        cache_dir = case_dir / "sec_cache"
        cache_dir.mkdir()
        (cache_dir / "doc_000012345625000002_ex99b.pdf").write_bytes(b"%PDF-1.4")
        (cache_dir / "doc_000012345625000001_ex99a.htm").write_text("<html>alpha</html>", encoding="utf-8")
        sec = _FakeEx99Sec(cache_dir=cache_dir)

        entries = pipeline._collect_legacy_ex99_cache_entries(sec)

        assert [entry["name"] for entry in entries] == [
            "doc_000012345625000001_ex99a.htm",
            "doc_000012345625000002_ex99b.pdf",
        ]
        assert entries[0]["accn_nd"] == "000012345625000001"
        assert entries[0]["is_html_like"] is True
        assert entries[1]["is_pdf"] is True


def test_shared_ex99_runtime_cache_reuses_document_downloads_across_consumers(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    accn = "0000123456-25-000001"
    accn_nd = pipeline.normalize_accession(accn)
    sec.index_map[accn_nd] = {"exdocs": ["ex99.htm"]}
    sec.doc_map[(accn_nd, "ex99.htm")] = b"<html>earnings release</html>"
    sec.ocr_text_map["ex99.htm"] = "release text"
    submissions = _submission_recent(
        [{"form": "8-K", "accn": accn, "report_date": "2025-03-31", "filing_date": "2025-04-25"}]
    )
    monkeypatch.setattr(pipeline, "find_ex99_docs", lambda idx: list(idx.get("exdocs", [])))
    monkeypatch.setattr(
        pipeline,
        "_extract_income_statement_from_text",
        lambda txt, q_end, rules=None: {
            "values": {"revenue": 10.0},
            "labels": {"revenue": "revenue"},
            "tokens": {"revenue": "Revenue"},
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_balance_sheet_from_text",
        lambda txt, q_end: {
            "values": {"cash": 5.0},
            "labels": {"cash": "cash"},
            "tokens": {"cash": "Cash"},
        },
    )

    runtime_cache = pipeline._make_ex99_runtime_cache()
    inventory = pipeline._build_ex99_accession_inventory(
        sec,
        123456,
        submissions,
        ex99_runtime_cache=runtime_cache,
        target_years={2025, 2026},
    )
    target_quarters = {dt.date(2025, 3, 31)}

    income_map, _ = pipeline.build_income_statement_fallback_ex99_ocr(
        sec,
        123456,
        submissions,
        max_quarters=4,
        ticker="ABC",
        target_quarters=target_quarters,
        ex99_inventory=inventory,
        ex99_runtime_cache=runtime_cache,
    )
    balance_map, _ = pipeline.build_balance_sheet_fallback_ex99_ocr(
        sec,
        123456,
        submissions,
        max_quarters=4,
        target_quarters=target_quarters,
        ex99_inventory=inventory,
        ex99_runtime_cache=runtime_cache,
    )

    assert dt.date(2025, 3, 31) in income_map
    assert dt.date(2025, 3, 31) in balance_map
    assert sec.calls["accession_index_json"] == 1
    assert sec.calls["download_document"] == 1


def test_image_only_ex99_inventory_row_still_uses_ocr_path(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    accn = "0000123456-25-000002"
    accn_nd = pipeline.normalize_accession(accn)
    sec.index_map[accn_nd] = {"exdocs": []}
    sec.image_map[accn_nd] = ["page-1.png"]
    sec.ocr_text_map["index_images"] = "image-only release"
    submissions = _submission_recent(
        [{"form": "8-K", "accn": accn, "report_date": "2025-06-30", "filing_date": "2025-07-25"}]
    )
    monkeypatch.setattr(pipeline, "find_ex99_docs", lambda idx: list(idx.get("exdocs", [])))
    monkeypatch.setattr(
        pipeline,
        "_extract_income_statement_from_text",
        lambda txt, q_end, rules=None: {
            "values": {"revenue": 22.0},
            "labels": {"revenue": "revenue"},
            "tokens": {"revenue": "Revenue"},
        },
    )

    runtime_cache = pipeline._make_ex99_runtime_cache()
    inventory = pipeline._build_ex99_accession_inventory(
        sec,
        123456,
        submissions,
        ex99_runtime_cache=runtime_cache,
        target_years={2025, 2026},
    )
    out, _ = pipeline.build_income_statement_fallback_ex99_ocr(
        sec,
        123456,
        submissions,
        max_quarters=4,
        ticker="ABC",
        target_quarters={dt.date(2025, 6, 30)},
        ex99_inventory=inventory,
        ex99_runtime_cache=runtime_cache,
    )

    assert inventory["eight_k_rows"][0]["is_image_only"] is True
    assert dt.date(2025, 6, 30) in out
    assert sec.calls["download_index_images"] == 1


def test_eps_ex99_pdf_prefers_pdf_text_before_ocr(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    accn = "0000123456-25-000003"
    accn_nd = pipeline.normalize_accession(accn)
    sec.index_map[accn_nd] = {"exdocs": ["ex99.pdf"]}
    sec.doc_map[(accn_nd, "ex99.pdf")] = b"%PDF-1.4"
    submissions = _submission_recent(
        [{"form": "8-K", "accn": accn, "report_date": "2025-03-31", "filing_date": "2025-04-25"}]
    )
    monkeypatch.setattr(pipeline, "find_ex99_docs", lambda idx: list(idx.get("exdocs", [])))
    monkeypatch.setattr(
        pipeline,
        "_extract_text_from_pdf_bytes",
        lambda data, quiet_pdf_warnings=True: "weighted average diluted shares from pdf",
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_eps_shares_from_text",
        lambda txt, q_end: {
            "shares_diluted": 123_000_000.0,
            "shares_basic": None,
            "label_diluted": "weighted average diluted shares",
            "label_basic": "",
        },
    )

    runtime_cache = pipeline._make_ex99_runtime_cache()
    inventory = pipeline._build_ex99_accession_inventory(
        sec,
        123456,
        submissions,
        ex99_runtime_cache=runtime_cache,
        target_years={2025, 2026},
    )
    out, _ = pipeline.build_eps_shares_fallback_ex99(
        sec,
        123456,
        submissions,
        max_quarters=4,
        target_quarters={dt.date(2025, 3, 31)},
        quiet_pdf_warnings=True,
        ex99_inventory=inventory,
        ex99_runtime_cache=runtime_cache,
    )

    assert out[dt.date(2025, 3, 31)]["shares_diluted"] == 123_000_000.0
    assert sec.calls["ocr_html_assets"] == 0


def test_primary_filing_inventory_filters_target_years_and_tracks_latest_10k() -> None:
    sec = _FakeEx99Sec()
    submissions = _submission_recent(
        [
            {
                "form": "10-Q",
                "accn": "0000123456-25-000001",
                "report_date": "2025-03-31",
                "filing_date": "2025-05-01",
                "primary_doc": "q1.htm",
            },
            {
                "form": "10-K",
                "accn": "0000123456-24-000002",
                "report_date": "2024-12-31",
                "filing_date": "2025-02-20",
                "primary_doc": "fy.htm",
            },
            {
                "form": "10-Q",
                "accn": "0000123456-23-000003",
                "report_date": "2023-03-31",
                "filing_date": "2023-05-01",
                "primary_doc": "old.htm",
            },
        ]
    )

    inventory = pipeline._build_primary_filing_inventory(sec, submissions, target_years={2024, 2025})

    assert [row["accn"] for row in inventory["rows"]] == [
        "0000123456-25-000001",
        "0000123456-24-000002",
    ]
    assert inventory["rows"][0]["primary_doc"] == "q1.htm"
    assert inventory["latest_10k_year"] == 2024


def test_shared_primary_filing_runtime_cache_reuses_document_downloads_across_consumers(monkeypatch) -> None:
    sec = _FakeEx99Sec()
    accn = "0000123456-25-000001"
    accn_nd = pipeline.normalize_accession(accn)
    sec.doc_map[(accn_nd, "primary.htm")] = b"<html>primary filing</html>"
    submissions = _submission_recent(
        [
            {
                "form": "10-Q",
                "accn": accn,
                "report_date": "2025-03-31",
                "filing_date": "2025-05-01",
                "primary_doc": "primary.htm",
            }
        ]
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_income_statement_from_html",
        lambda html_bytes, q_end, rules=None, period_hint="3M": {
            "values": {"revenue": 10.0},
            "labels": {"revenue": "Revenue"},
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_balance_sheet_from_html",
        lambda html_bytes, q_end: {
            "values": {"cash": 5.0},
            "labels": {"cash": "Cash"},
        },
    )

    filing_runtime_cache = pipeline._make_primary_filing_runtime_cache()
    filing_inventory = pipeline._build_primary_filing_inventory(sec, submissions, target_years={2025})

    income_map, _ = pipeline.build_income_statement_fallback(
        sec,
        123456,
        submissions,
        max_quarters=4,
        ticker="ABC",
        filing_inventory=filing_inventory,
        filing_runtime_cache=filing_runtime_cache,
    )
    balance_map, _ = pipeline.build_balance_sheet_fallback_table(
        sec,
        123456,
        submissions,
        max_quarters=4,
        target_quarters={dt.date(2025, 3, 31)},
        filing_inventory=filing_inventory,
        filing_runtime_cache=filing_runtime_cache,
    )

    assert dt.date(2025, 3, 31) in income_map
    assert dt.date(2025, 3, 31) in balance_map
    assert sec.calls["download_document"] == 1


@contextmanager
def _case_dir() -> Path:
    root = Path(__file__).resolve().parents[2] / ".venv" / "tmp_pipeline_refactor_tests"
    root.mkdir(parents=True, exist_ok=True)
    case_dir = root / uuid4().hex
    case_dir.mkdir()
    try:
        yield case_dir
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def _df(name: str) -> pd.DataFrame:
    return pd.DataFrame({"name": [name]})


def _make_artifacts() -> PipelineArtifacts:
    kwargs = {}
    for field_name in PipelineArtifacts.__dataclass_fields__:
        if field_name == "company_overview":
            kwargs[field_name] = {"ticker": "ABC"}
        elif field_name == "stage_timings":
            kwargs[field_name] = {"gaap_history": 1.25}
        else:
            kwargs[field_name] = _df(field_name)
    return PipelineArtifacts(**kwargs)


def test_pipeline_artifacts_keep_legacy_tuple_order_and_workbook_mapping() -> None:
    artifacts = _make_artifacts()

    legacy = artifacts.as_legacy_tuple()
    expected_names = [
        field_name
        for field_name in PipelineArtifacts.__dataclass_fields__
        if field_name != "stage_timings"
    ]
    assert len(legacy) == len(expected_names)
    for idx, field_name in enumerate(expected_names):
        assert legacy[idx] is getattr(artifacts, field_name)

    out_path = Path("demo.xlsx")
    inputs = WorkbookInputs.from_artifacts(
        artifacts,
        out_path=out_path,
        ticker="ABC",
        price=12.5,
        strictness="only3m",
        excel_mode="debug",
        is_rules={"mode": "test"},
        cache_dir=Path("cache"),
        quiet_pdf_warnings=False,
        rebuild_doc_text_cache=True,
        profile_timings=True,
    )
    assert inputs.out_path == out_path
    assert inputs.hist is artifacts.hist
    assert inputs.non_gaap_cred is artifacts.non_gaap_cred
    assert inputs.company_overview == {"ticker": "ABC"}
    assert inputs.ticker == "ABC"
    assert inputs.price == 12.5
    assert inputs.strictness == "only3m"
    assert inputs.excel_mode == "debug"
    assert inputs.is_rules == {"mode": "test"}
    assert inputs.cache_dir == Path("cache")
    assert inputs.quiet_pdf_warnings is False
    assert inputs.rebuild_doc_text_cache is True
    assert inputs.profile_timings is True


def test_pipeline_runtime_cache_and_root_resolution() -> None:
    with _case_dir() as case_dir:
        repo_root = case_dir / "repo"
        default_base = case_dir / "default_base"
        ticker_dir = repo_root / "ABC"
        material_override = case_dir / "manual_materials"
        repo_root.mkdir()
        default_base.mkdir()
        ticker_dir.mkdir(parents=True)
        material_override.mkdir()

        cache = PipelineStageCache(case_dir / "cache", "0000123456", 7)
        payload = _df("gaap_history")
        cache.save("gaap_history_bundle", "key-1", payload)

        loaded = cache.load("gaap_history_bundle", "key-1")
        assert isinstance(loaded, pd.DataFrame)
        assert_frame_equal(loaded, payload)
        assert cache.load("gaap_history_bundle", "wrong-key") is None

        tkr_raw, tkr_u, base_dir = resolve_pipeline_roots(
            repo_root=repo_root,
            default_base_dir=default_base,
            ticker="abc",
        )
        assert tkr_raw == "abc"
        assert tkr_u == "ABC"
        assert base_dir == ticker_dir

        _, _, override_dir = resolve_pipeline_roots(
            repo_root=repo_root,
            default_base_dir=default_base,
            ticker="abc",
            material_root=material_override,
        )
        assert override_dir == material_override.resolve()

        assert path_belongs_to_ticker(case_dir / "ABC research" / "notes.txt", "ABC")
        assert not path_belongs_to_ticker(case_dir / "ABCD" / "notes.txt", "ABC")


def test_pipeline_qa_filters_old_low_value_warns_without_hiding_fails() -> None:
    qa_checks = pd.DataFrame(
        [
            {
                "quarter": "2024-03-31",
                "metric": "promise_tracker",
                "check": "promise_scorable",
                "status": "warn",
                "message": "old low value warn",
                "promise_id": "p1",
            },
            {
                "quarter": "2024-03-31",
                "metric": "promise_tracker",
                "check": "promise_scorable",
                "status": "warn",
                "message": "old low value warn",
                "promise_id": "p1",
            },
            {
                "quarter": "2025-09-30",
                "metric": "promise_tracker",
                "check": "promise_scorable",
                "status": "warn",
                "message": "recent low value warn",
                "promise_id": "p2",
            },
            {
                "quarter": "2025-12-31",
                "metric": "revenue",
                "check": "tieout",
                "status": "warn",
                "message": "recent core warn",
                "promise_id": "",
            },
            {
                "quarter": "2024-03-31",
                "metric": "promise_tracker",
                "check": "promise_scorable",
                "status": "fail",
                "message": "old fail must stay",
                "promise_id": "p3",
            },
        ]
    )
    qa_result = finalize_qa_checks(qa_checks, review_quarters=2)
    assert len(qa_result) == 3
    assert "old low value warn" not in set(qa_result["message"])
    assert "old fail must stay" in set(qa_result["message"])
    assert "recent low value warn" in set(qa_result["message"])
    assert qa_result["message"].tolist().count("old low value warn") == 0

    needs_review = pd.DataFrame(
        [
            {
                "quarter": "2024-03-31",
                "metric": "non_gaap",
                "severity": "warn",
                "message": "old history warn",
                "source": "history_coverage",
            },
            {
                "quarter": "2024-03-31",
                "metric": "non_gaap",
                "severity": "warn",
                "message": "old history warn",
                "source": "history_coverage",
            },
            {
                "quarter": "2025-09-30",
                "metric": "non_gaap",
                "severity": "warn",
                "message": "recent non-gaap warn",
                "source": "history_coverage",
            },
            {
                "quarter": "2025-12-31",
                "metric": "revenue",
                "severity": "warn",
                "message": "recent revenue warn",
                "source": "history_coverage",
            },
            {
                "quarter": "2024-03-31",
                "metric": "non_gaap",
                "severity": "fail",
                "message": "old fail must stay",
                "source": "history_coverage",
            },
        ]
    )
    review_result = finalize_needs_review(needs_review, review_quarters=2)
    assert len(review_result) == 3
    assert "old history warn" not in set(review_result["message"])
    assert "old fail must stay" in set(review_result["message"])
    assert "recent non-gaap warn" in set(review_result["message"])


def test_pipeline_wrappers_delegate_to_extracted_modules(
    monkeypatch,
) -> None:
    artifacts = _make_artifacts()
    config = PipelineConfig(cache_dir=Path("cache"))
    sec_config = object()
    out_path = Path("out.xlsx")
    captured: dict[str, object] = {}

    def fake_run_pipeline_impl(
        config_arg: PipelineConfig,
        sec_config_arg: object,
        *,
        ticker: str | None = None,
        cik: str | None = None,
    ) -> PipelineArtifacts:
        captured["run_pipeline_impl"] = (config_arg, sec_config_arg, ticker, cik)
        return artifacts

    def fake_write_excel_from_inputs(inputs: WorkbookInputs) -> None:
        captured["write_excel_inputs"] = inputs

    monkeypatch.setattr(pipeline_orchestration, "run_pipeline_impl", fake_run_pipeline_impl)
    monkeypatch.setattr(excel_writer, "write_excel_from_inputs", fake_write_excel_from_inputs)

    legacy = pipeline.run_pipeline(config, sec_config, ticker="ABC", cik="1234")
    assert len(legacy) == len(artifacts.as_legacy_tuple())
    assert legacy[0] is artifacts.hist
    assert captured["run_pipeline_impl"] == (config, sec_config, "ABC", "1234")

    write_kwargs = {
        field_name: getattr(artifacts, field_name)
        for field_name in PipelineArtifacts.__dataclass_fields__
        if field_name not in {"stage_timings", "company_overview"}
    }
    pipeline.write_excel(
        out_path,
        **write_kwargs,
        company_overview=artifacts.company_overview,
        ticker="ABC",
        price=17.25,
        strictness="only3m",
        excel_mode="clean",
        is_rules={"variant": "test"},
        cache_dir=Path("cache"),
        quiet_pdf_warnings=False,
        rebuild_doc_text_cache=True,
        profile_timings=True,
        excel_debug_scope="drivers",
    )

    inputs = captured["write_excel_inputs"]
    assert isinstance(inputs, WorkbookInputs)
    assert inputs.out_path == out_path
    assert inputs.hist is artifacts.hist
    assert inputs.company_overview == {"ticker": "ABC"}
    assert inputs.ticker == "ABC"
    assert inputs.price == 17.25
    assert inputs.strictness == "only3m"
    assert inputs.profile_timings is True
    assert inputs.excel_debug_scope == "drivers"


def test_pipeline_write_excel_forwards_ui_debug_scope(monkeypatch) -> None:
    artifacts = _make_artifacts()
    out_path = Path("out.xlsx")
    captured: dict[str, object] = {}

    def fake_write_excel_from_inputs(inputs: WorkbookInputs) -> None:
        captured["write_excel_inputs"] = inputs

    monkeypatch.setattr(excel_writer, "write_excel_from_inputs", fake_write_excel_from_inputs)

    write_kwargs = {
        field_name: getattr(artifacts, field_name)
        for field_name in PipelineArtifacts.__dataclass_fields__
        if field_name not in {"stage_timings", "company_overview"}
    }
    pipeline.write_excel(
        out_path,
        **write_kwargs,
        company_overview=artifacts.company_overview,
        ticker="ABC",
        excel_debug_scope="ui",
    )

    inputs = captured["write_excel_inputs"]
    assert isinstance(inputs, WorkbookInputs)
    assert inputs.excel_debug_scope == "ui"
