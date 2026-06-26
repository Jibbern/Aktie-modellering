from __future__ import annotations

from pathlib import Path

import pandas as pd

from pbi_xbrl.excel_writer_summary_freshness import (
    build_post_quarter_current_effects,
    build_source_filing_freshness,
)


def _pbi_event(source_path: Path) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "PBI",
                "event_type": "refinancing_redemption",
                "reported_quarter_anchor": "2026-Q1",
                "event_date": "2026-06-23",
                "filing_type": "8-K package / exhibits",
                "filing_date": "2026-06-25",
                "downloaded_at": "2026-06-25T19:49:52.168707+00:00",
                "accession": "000119312526281893",
                "principal_redeemed": 347_000_000.0,
                "incremental_term_loan": 150_000_000.0,
                "term_loan_total": 302_000_000.0,
                "gross_principal_delta": -197_000_000.0,
                "next_scheduled_maturity": "March 2029",
                "term_loan_maturity": "2031-05-18",
                "automatic_net_debt_adjustment": False,
                "history_treatment": (
                    "History_Q unchanged; Debt_Profile unchanged; "
                    "Debt_Tranches_Latest unchanged"
                ),
                "valuation_treatment": (
                    "Current Debt Detail updated; no auto net-debt adjustment"
                ),
                "used_in_workbook": "Yes",
                "used_surfaces": (
                    "Valuation current Debt Detail | Investment_Case | Support/Audit"
                ),
                "source_documents": "d88573d8k.htm | d88573dex101.htm | d88573dex991.htm",
                "source_paths": str(source_path),
                "source_path_exists": True,
            }
        ]
    )


def _gpre_event(source_path: Path) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "GPRE",
                "event_type": "warrant_dilution",
                "reported_quarter_anchor": "2026-Q1",
                "event_date": "2026-06-16",
                "filing_type": "S-3ASR / warrant exhibits",
                "filing_date": "2026-06-22",
                "downloaded_at": "2026-06-25T19:49:52.168707+00:00",
                "accession": "000110465926076397",
                "warrants_issued": 500_000.0,
                "potential_common_shares_issuable_max": 550_000.0,
                "exercise_price": 0.01,
                "expiration_date": "2036-06-16",
                "beneficial_ownership_limitation": 0.198,
                "history_treatment": "History_Q shares/EPS unchanged",
                "valuation_treatment": "Full-dilution sensitivity uses +0.550m shares",
                "used_in_workbook": "Yes",
                "used_surfaces": (
                    "Valuation full-dilution sensitivity | Investment_Case | Support/Audit"
                ),
                "source_documents": (
                    "tm2618355-1_s3asr.htm | tm2618355d2_ex4-4a.htm"
                ),
                "source_paths": str(source_path),
                "source_path_exists": True,
            }
        ]
    )


def test_source_filing_freshness_projects_reported_and_additional_filings(
    tmp_path: Path,
) -> None:
    reported_source = tmp_path / "pbi-20260331.htm"
    reported_source.write_text("reported filing", encoding="utf-8")
    event_source = tmp_path / "d88573d8k.htm"
    event_source.write_text("event filing", encoding="utf-8")
    hist = pd.DataFrame({"quarter": pd.to_datetime(["2025-12-31", "2026-03-31"])})
    audit = pd.DataFrame(
        [
            {
                "quarter": "2026-03-31",
                "form": "10-Q",
                "accn": "000162828026031003",
                "filed": "2026-05-06",
                "doc": str(reported_source),
                "downloaded_at": "2026-05-06T12:00:00+00:00",
            }
        ]
    )

    freshness = build_source_filing_freshness(
        ticker="PBI",
        hist=hist,
        audit=audit,
        manifest_df=pd.DataFrame(
            [
                {
                    "reportDate": "2026-03-31",
                    "filedDate": "2026-05-06",
                    "form": pd.NA,
                    "source_local_path": pd.NA,
                }
            ]
        ),
        post_quarter_events=_pbi_event(event_source),
    )

    assert len(freshness) == 1
    row = freshness.iloc[0]
    assert row["ticker"] == "PBI"
    assert row["latest_reported_quarter"] == "2026-Q1"
    assert row["latest_reported_filing_type"] == "10-Q"
    assert row["latest_reported_filing_accession"] == "000162828026031003"
    assert row["latest_reported_filing_date"] == "2026-05-06"
    assert row["latest_reported_downloaded_at"] == "2026-05-06T12:00:00+00:00"
    assert row["latest_additional_filing_type"] == "8-K package / exhibits"
    assert row["latest_additional_filing_accession"] == "000119312526281893"
    assert row["latest_additional_filing_date"] == "2026-06-25"
    assert row["latest_additional_downloaded_at"] == "2026-06-25T19:49:52.168707+00:00"
    assert row["event_type"] == "refinancing/redemption"
    assert row["used_in_workbook"] == "Yes"
    assert "Valuation current Debt Detail" in row["used_surfaces"]
    assert row["source_path_exists"] == "Yes"


def test_post_quarter_current_effects_projects_pbi_uncertainty_without_estimate(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "d88573d8k.htm"
    source_path.write_text("event filing", encoding="utf-8")

    effects = build_post_quarter_current_effects(_pbi_event(source_path))

    areas = set(effects["area"])
    assert {
        "2027 Senior Notes",
        "Term Loan A",
        "Gross principal debt",
        "Cash / net debt",
        "Next scheduled maturity",
        "Term Loan A maturity",
    }.issubset(areas)
    notes = effects[effects["area"] == "2027 Senior Notes"].iloc[0]
    assert notes["reported_value"] == 347.0
    assert notes["current_overlay_value"] == 0.0
    assert notes["change"] == -347.0
    assert notes["confidence_treatment"] == "Source-backed"
    term_loan = effects[effects["area"] == "Term Loan A"].iloc[0]
    assert term_loan["reported_value"] == 152.0
    assert term_loan["current_overlay_value"] == 302.0
    assert term_loan["change"] == 150.0
    cash = effects[effects["area"] == "Cash / net debt"].iloc[0]
    assert cash["current_overlay_value"] == "Unresolved / manual review"
    assert cash["change"] == ""
    assert cash["confidence_treatment"] == "Partial / unresolved"
    assert "History_Q unchanged" in cash["historical_treatment"]
    assert "Debt_Profile unchanged" in cash["historical_treatment"]
    assert cash["valuation_treatment"] == "No auto net-debt adjustment"


def test_post_quarter_current_effects_projects_gpre_legal_and_valuation_counts(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "tm2618355-1_s3asr.htm"
    source_path.write_text("event filing", encoding="utf-8")

    effects = build_post_quarter_current_effects(_gpre_event(source_path))

    values = {
        row["area"]: row
        for row in effects.to_dict("records")
    }
    assert values["Warrants issued"]["current_overlay_value"] == 500_000.0
    assert values["Potential common shares issuable max"]["current_overlay_value"] == 550_000.0
    assert values["Valuation full-dilution overlay"]["change"] == 0.550
    assert values["Exercise price"]["current_overlay_value"] == 0.01
    assert values["Expiration"]["current_overlay_value"] == "2036-06-16"
    assert values["Reported shares / EPS"]["historical_treatment"] == "Shares/EPS unchanged"
    assert values["Valuation full-dilution overlay"]["valuation_treatment"] == "Full-dilution sensitivity"


def test_ticker_without_normalized_event_gets_no_fake_current_effects() -> None:
    hist = pd.DataFrame({"quarter": pd.to_datetime(["2026-03-31"])})

    for ticker in ("ANF", "GTX"):
        freshness = build_source_filing_freshness(
            ticker=ticker,
            hist=hist,
            audit=pd.DataFrame(),
            manifest_df=pd.DataFrame(),
            post_quarter_events=pd.DataFrame(),
        )
        effects = build_post_quarter_current_effects(pd.DataFrame())

        assert len(freshness) == 1
        assert (
            freshness.iloc[0]["latest_additional_filing_type"]
            == "None newer / no model-relevant post-quarter event"
        )
        assert freshness.iloc[0]["used_in_workbook"] == "No"
        assert effects.empty


def test_reported_filing_downloaded_at_falls_back_to_accession_source_mtime(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "sec_cache" / "PBI"
    cache_root.mkdir(parents=True)
    source = cache_root / "doc_000162828026031003_pbi-20260331.htm"
    source.write_text("reported filing", encoding="utf-8")
    hist = pd.DataFrame({"quarter": pd.to_datetime(["2026-03-31"])})
    audit = pd.DataFrame(
        [
            {
                "quarter": "2026-03-31",
                "form": "10-Q",
                "accn": "000162828026031003",
                "filed": "2026-05-06",
            }
        ]
    )

    freshness = build_source_filing_freshness(
        ticker="PBI",
        hist=hist,
        audit=audit,
        manifest_df=pd.DataFrame(),
        post_quarter_events=pd.DataFrame(),
        source_roots=[cache_root],
    )

    row = freshness.iloc[0]
    assert row["latest_reported_downloaded_at"]
    assert "filesystem fallback" in row["latest_reported_downloaded_at"]
