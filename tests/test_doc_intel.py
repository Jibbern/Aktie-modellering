from __future__ import annotations

import datetime as dt
import json
import shutil
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from pbi_xbrl.doc_intel import (
    _build_non_gaap_cred,
    _build_promises,
    _build_progress,
    _dedupe_promises,
    _extract_promise_candidates_from_notes,
    _make_doc_intel_doc_registry,
    build_doc_intel_outputs,
)


def _promise_row(
    *,
    quarter: str,
    statement: str,
    statement_norm: str,
    metric: str,
    target_value: float | None,
    deadline: str,
    confidence: str,
    accn: str,
    doc: str,
    category: str = "Guidance / targets",
    target_high: float | None = None,
    target_unit: str = "ratio",
    target_kind: str = "gte_abs",
    target_year: int = 2025,
    target_bucket: str = "raw:0.0:gte_abs",
) -> dict[str, object]:
    return {
        "quarter": quarter,
        "category": category,
        "statement": statement,
        "statement_norm": statement_norm,
        "metric": metric,
        "target_value": target_value,
        "target_high": target_high,
        "target_unit": target_unit,
        "target_kind": target_kind,
        "promise_type": "operational",
        "target_year": target_year,
        "deadline": deadline,
        "observed_runrate": None,
        "observed_increment": None,
        "scorable": True,
        "soft_promise": False,
        "target_bucket": target_bucket,
        "evidence_snippet": statement,
        "accn": accn,
        "form": "10-Q" if confidence == "high" else "8-K",
        "doc": doc,
        "doc_path": doc,
        "doc_type": "html",
        "section_or_page": "doc_scan",
        "method": "doc_scan",
        "confidence": confidence,
    }


def _progress_hist(revenues: list[float]) -> pd.DataFrame:
    quarters = pd.to_datetime(
        [
            "2024-03-31",
            "2024-06-30",
            "2024-09-30",
            "2024-12-31",
            "2025-03-31",
            "2025-06-30",
            "2025-09-30",
        ][: len(revenues)]
    )
    return pd.DataFrame(
        {
            "quarter": quarters,
            "revenue": revenues,
            "ebitda": [20.0] * len(quarters),
            "debt_core": [60.0] * len(quarters),
            "cash": [10.0] * len(quarters),
            "buybacks_cash": [0.0] * len(quarters),
            "dividends_cash": [0.0] * len(quarters),
            "op_income": [12.0] * len(quarters),
            "sga": [30_000_000.0] * len(quarters),
            "research_and_development": [10_000_000.0] * len(quarters),
        }
    )


def test_extract_promise_candidates_from_notes_filters_historical_and_boilerplate() -> None:
    quarter_notes = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "category": "Guidance / targets",
                "evidence_snippet": "We expect revenue to be flat this year.",
                "doc_type": "earnings_release",
            },
            {
                "quarter": "2025-03-31",
                "category": "Programs / initiatives",
                "evidence_snippet": "We target $80 million to $100 million of annualized cost savings by end of 2026.",
                "doc_type": "earnings_release",
            },
            {
                "quarter": "2025-03-31",
                "category": "Programs / initiatives",
                "evidence_snippet": "Results for the first quarter included net annualized savings of $10 million.",
                "doc_type": "earnings_release",
            },
            {
                "quarter": "2025-03-31",
                "category": "Guidance / targets",
                "evidence_snippet": "Forward-looking statements mean we expect better execution over time.",
                "doc_type": "earnings_release",
            },
        ]
    )

    out = _extract_promise_candidates_from_notes(quarter_notes)

    assert len(out) == 1
    assert set(out["metric"]) == {"cost_savings_run_rate"}
    assert not out["statement"].str.contains("Results for the first quarter", regex=False).any()
    assert not out["statement"].str.contains("Forward-looking statements", case=False, regex=False).any()


def test_extract_promise_candidates_from_notes_rejects_scaffolding_and_keeps_clean_milestone() -> None:
    quarter_notes = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "category": "Guidance / targets",
                "evidence_snippet": "Bowes provided the following Management target",
                "doc_type": "presentation",
            },
            {
                "quarter": "2025-03-31",
                "category": "Guidance / targets",
                "evidence_snippet": "45Z from remaining facilities",
                "doc_type": "presentation",
            },
            {
                "quarter": "2025-03-31",
                "category": "Programs / initiatives",
                "evidence_snippet": "York will be fully operational by Q4 2025.",
                "doc_type": "transcript",
            },
        ]
    )

    out = _extract_promise_candidates_from_notes(quarter_notes)

    assert len(out) == 1
    assert out.iloc[0]["candidate_scope"] == "clean_milestone"
    assert "York will be fully operational" in str(out.iloc[0]["statement_summary"])


def test_dedupe_promises_merges_same_key_and_keeps_highest_confidence() -> None:
    rows = [
        _promise_row(
            quarter="2025-03-31",
            statement="We expect revenue to improve by year end.",
            statement_norm="we expect revenue to improve by year end",
            metric="revenue_yoy",
            target_value=0.05,
            deadline="2025-12-31",
            confidence="low",
            accn="0001",
            doc="march_update.htm",
            target_bucket="raw:0.05:gte_abs",
        ),
        _promise_row(
            quarter="2025-06-30",
            statement="We expect revenue to improve by year end.",
            statement_norm="we expect revenue to improve by year end",
            metric="revenue_yoy",
            target_value=0.05,
            deadline="2025-12-31",
            confidence="high",
            accn="0002",
            doc="june_update.htm",
            target_bucket="raw:0.05:gte_abs",
        ),
    ]

    out = _dedupe_promises(pd.DataFrame(rows))

    assert len(out) == 1
    row = out.iloc[0]
    assert row["confidence"] == "high"
    assert pd.Timestamp(row["created_quarter"]).date() == dt.date(2025, 3, 31)
    assert pd.Timestamp(row["last_seen_quarter"]).date() == dt.date(2025, 6, 30)
    assert pd.Timestamp(row["carried_to_quarter"]).date() == dt.date(2025, 6, 30)
    history = json.loads(str(row["evidence_history_json"]))
    assert len(history) == 2
    assert history[-1]["quarter"] == "2025-06-30"
    assert row["target_value"] == 0.05


def test_build_progress_marks_achieved_when_target_is_met() -> None:
    promises = _dedupe_promises(
        pd.DataFrame(
            [
                _promise_row(
                    quarter="2025-03-31",
                    statement="We expect revenue to increase 5% this year.",
                    statement_norm="we expect revenue to increase 5 this year",
                    metric="revenue_yoy",
                    target_value=0.05,
                    deadline="2025-06-30",
                    confidence="high",
                    accn="0001",
                    doc="achieved_q1.htm",
                    target_bucket="raw:0.05:gte_abs",
                )
            ]
        )
    )

    out = _build_progress(promises, _progress_hist([100.0, 100.0, 100.0, 100.0, 110.0, 120.0]), pd.DataFrame())

    q2 = out[out["quarter"] == dt.date(2025, 6, 30)].iloc[0]
    assert q2["status"] == "achieved"
    assert float(q2["actual"]) >= 0.05


def test_build_progress_marks_at_risk_then_broken_when_trend_moves_away() -> None:
    promises = _dedupe_promises(
        pd.DataFrame(
            [
                _promise_row(
                    quarter="2025-03-31",
                    statement="We expect revenue to increase 20% by Q3 2025.",
                    statement_norm="we expect revenue to increase 20 by q3 2025",
                    metric="revenue_yoy",
                    target_value=0.20,
                    deadline="2025-09-30",
                    confidence="high",
                    accn="0001",
                    doc="risk_q1.htm",
                    target_bucket="raw:0.2:gte_abs",
                )
            ]
        )
    )

    out = _build_progress(promises, _progress_hist([100.0, 100.0, 100.0, 100.0, 110.0, 105.0, 103.0]), pd.DataFrame())

    q2 = out[out["quarter"] == dt.date(2025, 6, 30)].iloc[0]
    q3 = out[out["quarter"] == dt.date(2025, 9, 30)].iloc[0]
    assert q2["status"] == "at_risk"
    assert q3["status"] == "broken"


def test_dedupe_promises_merges_updated_target_range_into_existing_promise() -> None:
    rows = [
        _promise_row(
            quarter="2025-03-31",
            statement="We target $80 million to $100 million of annualized cost savings by end of 2026.",
            statement_norm="we target 80 million to 100 million of annualized cost savings by end of 2026",
            metric="cost_savings_run_rate",
            target_value=80_000_000.0,
            target_high=100_000_000.0,
            deadline="2026-12-31",
            confidence="med",
            accn="0001",
            doc="range_q1.htm",
            category="Programs / initiatives",
            target_unit="USD",
            target_kind="gte_abs",
            target_year=2026,
            target_bucket="usd:75000000:gte_abs",
        ),
        _promise_row(
            quarter="2025-06-30",
            statement="We target $90 million to $110 million of annualized cost savings by end of 2026.",
            statement_norm="we target 90 million to 110 million of annualized cost savings by end of 2026",
            metric="cost_savings_run_rate",
            target_value=90_000_000.0,
            target_high=110_000_000.0,
            deadline="2026-12-31",
            confidence="high",
            accn="0002",
            doc="range_q2.htm",
            category="Programs / initiatives",
            target_unit="USD",
            target_kind="gte_abs",
            target_year=2026,
            target_bucket="usd:100000000:gte_abs",
        ),
    ]

    out = _dedupe_promises(pd.DataFrame(rows))

    assert len(out) == 1
    row = out.iloc[0]
    assert row["target_value"] == 90_000_000.0
    assert row["target_high"] == 110_000_000.0
    assert row["confidence"] == "high"
    assert row["accn"] == "0002"
    assert row["doc"] == "range_q2.htm"
    assert str(row["promise_text"]).startswith("We target $90 million to $110 million")
    assert pd.Timestamp(row["last_seen_quarter"]).date() == dt.date(2025, 6, 30)
    history = json.loads(str(row["evidence_history_json"]))
    assert len(history) == 2


def test_build_progress_carries_forward_latest_cost_savings_runrate() -> None:
    promises = _dedupe_promises(
        pd.DataFrame(
            [
                _promise_row(
                    quarter="2025-03-31",
                    statement="We target $80 million to $100 million of annualized cost savings by end of 2026.",
                    statement_norm="we target 80 million to 100 million of annualized cost savings by end of 2026",
                    metric="cost_savings_run_rate",
                    target_value=80_000_000.0,
                    target_high=100_000_000.0,
                    deadline="2026-12-31",
                    confidence="high",
                    accn="0001",
                    doc="cost_q1.htm",
                    category="Programs / initiatives",
                    target_unit="USD",
                    target_kind="gte_abs",
                    target_year=2026,
                    target_bucket="usd:75000000:gte_abs",
                )
            ]
        )
    ).copy()

    evidence_history = [
        {
            "quarter": "2025-03-31",
            "mention_kind": "numeric",
            "snippet": "We target $80 million to $100 million of annualized cost savings by end of 2026.",
            "target_low": 80_000_000.0,
            "target_high": 100_000_000.0,
            "observed_runrate": None,
            "observed_increment": None,
        },
        {
            "quarter": "2025-06-30",
            "mention_kind": "numeric",
            "snippet": "Net annualized savings are now $40 million.",
            "target_low": None,
            "target_high": None,
            "observed_runrate": 40_000_000.0,
            "observed_increment": None,
        },
    ]
    promises.loc[0, "evidence_history_json"] = json.dumps(evidence_history)
    promises.loc[0, "source_evidence_json"] = json.dumps(evidence_history[0])
    promises.loc[0, "last_seen_evidence_quarter"] = dt.date(2025, 6, 30)
    promises.loc[0, "last_seen_quarter"] = dt.date(2025, 6, 30)
    promises.loc[0, "last_seen_numeric_quarter"] = dt.date(2025, 6, 30)

    out = _build_progress(promises, _progress_hist([100.0, 100.0, 100.0, 100.0, 110.0, 110.0, 110.0]), pd.DataFrame())

    q3 = out[out["quarter"] == dt.date(2025, 9, 30)].iloc[0]
    assert not bool(q3["numeric_update_this_quarter"])
    assert q3["last_seen_numeric_quarter"] == dt.date(2025, 6, 30)
    assert "Carried forward" in str(q3["rationale"])


def test_build_progress_parses_cost_savings_json_once_per_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    promises = _dedupe_promises(
        pd.DataFrame(
            [
                _promise_row(
                    quarter="2025-03-31",
                    statement="We target $80 million to $100 million of annualized cost savings by end of 2026.",
                    statement_norm="we target 80 million to 100 million of annualized cost savings by end of 2026",
                    metric="cost_savings_run_rate",
                    target_value=80_000_000.0,
                    target_high=100_000_000.0,
                    deadline="2026-12-31",
                    confidence="high",
                    accn="0001",
                    doc="cost_q1.htm",
                    category="Programs / initiatives",
                    target_unit="USD",
                    target_kind="gte_abs",
                    target_year=2026,
                    target_bucket="usd:75000000:gte_abs",
                )
            ]
        )
    ).copy()

    evidence_history = [
        {
            "quarter": "2025-03-31",
            "mention_kind": "numeric",
            "snippet": "We target $80 million to $100 million of annualized cost savings by end of 2026.",
            "target_low": 80_000_000.0,
            "target_high": 100_000_000.0,
        },
        {
            "quarter": "2025-06-30",
            "mention_kind": "numeric",
            "snippet": "Net annualized savings are now $40 million.",
            "observed_runrate": 40_000_000.0,
        },
    ]
    promises.loc[0, "evidence_history_json"] = json.dumps(evidence_history)
    promises.loc[0, "source_evidence_json"] = json.dumps(evidence_history[0])

    calls = {"loads": 0}
    original_loads = json.loads

    def counted_loads(raw, *args, **kwargs):
        calls["loads"] += 1
        return original_loads(raw, *args, **kwargs)

    monkeypatch.setattr("pbi_xbrl.doc_intel.json.loads", counted_loads)

    out = _build_progress(
        promises,
        _progress_hist([100.0, 100.0, 100.0, 100.0, 110.0, 110.0, 110.0]),
        pd.DataFrame(),
    )

    assert not out.empty
    assert calls["loads"] == 2


def test_extract_promise_candidates_from_notes_populates_subject_identity_fields() -> None:
    quarter_notes = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-03-31")],
            "note": ["We target $80 million to $100 million of annualized cost savings by end of 2026."],
            "metric_ref": ["Cost savings target"],
            "category": ["Programs / initiatives"],
            "doc_type": ["earnings_release"],
            "doc": ["release_q1.txt"],
            "evidence_snippet": ["We target $80 million to $100 million of annualized cost savings by end of 2026."],
        }
    )

    out = _extract_promise_candidates_from_notes(quarter_notes)

    assert not out.empty
    row = out.iloc[0]
    assert row["candidate_type"] == "measurable_promise_candidate"
    assert row["route_reason"] == "promise_tracker"
    assert row["routing_reason"] == "measurable_target"
    assert row["metric_family"] == "cost_savings"
    assert row["target_period_norm"] == "Q2026Q4"
    assert str(row["canonical_subject_key"]).strip()
    assert str(row["promise_lifecycle_key"]).strip()
    assert row["lifecycle_state"] == "stated"


def test_extract_promise_candidates_from_notes_populates_source_and_subject_hierarchy_fields() -> None:
    quarter_notes = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
            "note": [
                "York will be fully operational by Q4 2025.",
                "We continue to evaluate strategic options over time.",
            ],
            "metric_ref": ["Strategic milestone", "Management tone"],
            "category": ["Programs / initiatives", "Tone / expectations"],
            "doc_type": ["transcript", "earnings_release"],
            "doc": ["transcript_q3.txt", "release_q3.txt"],
            "evidence_snippet": [
                "York will be fully operational by Q4 2025.",
                "We continue to evaluate strategic options over time.",
            ],
        }
    )

    out = _extract_promise_candidates_from_notes(quarter_notes)

    assert len(out) == 1
    row = out.iloc[0]
    assert row["source_class"] == "preferred_narrative"
    assert row["statement_class"] in {"narrative", "investor_phrase"}
    assert str(row["parent_subject_key"]).strip()
    assert "york" in str(row["parent_subject_key"]).lower()
    assert str(row["lifecycle_subject_key"]).strip()
    assert row["evidence_role"] == "promise_origin"


def test_build_non_gaap_cred_warns_for_missing_source_and_unknown_units() -> None:
    hist = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-03-31")],
            "op_income": [25_000_000.0],
            "revenue": [100_000_000.0],
        }
    )
    adj_metrics = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-03-31")],
            "adj_ebit": [30_000_000.0],
            "adj_ebitda": [35_000_000.0],
            "doc": [None],
            "source_snippet": ["Adjusted EBITDA bridge summary"],
        }
    )

    out = _build_non_gaap_cred(hist, adj_metrics, pd.DataFrame())

    row = out.iloc[0]
    assert row["qa_status"] == "WARN"
    assert "source not found" in str(row["qa_reasons_text"]).lower()


def test_build_non_gaap_cred_fails_on_quarter_alignment_mismatch() -> None:
    hist = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-06-30")],
            "op_income": [25_000_000.0],
            "revenue": [100_000_000.0],
        }
    )
    adj_metrics = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-06-30")],
            "adj_ebit": [30_000_000.0],
            "adj_ebitda": [35_000_000.0],
            "doc": ["demo_Q1_2025.pdf"],
            "source_snippet": ["Adjusted EBITDA (in millions)"],
        }
    )

    out = _build_non_gaap_cred(hist, adj_metrics, pd.DataFrame())

    row = out.iloc[0]
    assert row["qa_status"] == "FAIL"
    assert "align with quarter_end" in str(row["qa_reasons_text"])


def test_build_non_gaap_cred_prefers_highest_confidence_row_per_quarter() -> None:
    hist = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-03-31")],
            "op_income": [25_000_000.0],
            "revenue": [100_000_000.0],
        }
    )
    adj_metrics = pd.DataFrame(
        [
            {
                "quarter": pd.Timestamp("2025-03-31"),
                "adj_ebit": 60_000_000.0,
                "adj_ebitda": 65_000_000.0,
                "confidence": "low",
                "doc": "demo_Q1_2025.pdf",
                "source_snippet": "Adjusted EBITDA (in millions)",
            },
            {
                "quarter": pd.Timestamp("2025-03-31"),
                "adj_ebit": 30_000_000.0,
                "adj_ebitda": 35_000_000.0,
                "confidence": "high",
                "doc": "demo_Q1_2025.pdf",
                "source_snippet": "Adjusted EBITDA (in millions)",
            },
        ]
    )

    out = _build_non_gaap_cred(hist, adj_metrics, pd.DataFrame())

    row = out.iloc[0]
    assert row["adj_ebit"] == 30_000_000.0
    assert row["adj_ebitda"] == 35_000_000.0


def test_doc_intel_shared_doc_registry_reuses_same_pdf_text_for_promises_and_non_gaap_cred(monkeypatch) -> None:
    calls = {"pdf": 0}

    def fake_extract_pdf_text_cached(
        pdf_path: Path,
        *,
        cache_root: Path | None = None,
        rebuild_cache: bool = False,
        quiet_pdf_warnings: bool = True,
    ) -> str:
        calls["pdf"] += 1
        return (
            "We target $80 million to $100 million of annualized cost savings by end of 2026. "
            "Adjusted EBITDA bridge summary (in millions) with reconciliation support."
        )

    monkeypatch.setattr("pbi_xbrl.doc_intel._extract_pdf_text_cached", fake_extract_pdf_text_cached)

    hist = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-03-31")],
            "op_income": [25_000_000.0],
            "revenue": [100_000_000.0],
        }
    )
    quarter_notes = pd.DataFrame()
    adj_metrics = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2025-03-31")],
            "adj_ebit": [30_000_000.0],
            "adj_ebitda": [35_000_000.0],
            "doc": [None],
            "source_snippet": ["Adjusted EBITDA bridge summary"],
        }
    )

    tmp_root = Path(__file__).resolve().parents[1] / ".pytest_tmp_doc_intel"
    tmp_root.mkdir(parents=True, exist_ok=True)
    release_dir = tmp_root / uuid4().hex
    release_dir.mkdir()
    try:
        pdf_path = release_dir / "Q1-2025.pdf"
        pdf_path.write_bytes(b"%PDF-1.4")
        adj_metrics.loc[0, "doc"] = str(pdf_path)
        registry = _make_doc_intel_doc_registry()

        promises = _build_promises(
            quarter_notes=quarter_notes,
            sec=None,
            cik_int=0,
            submissions={},
            hist=hist,
            earnings_release_dir=release_dir,
            max_docs=8,
            max_quarters=4,
            cache_dir=release_dir,
            rebuild_doc_text_cache=False,
            quiet_pdf_warnings=True,
            doc_registry=registry,
        )
        cred = _build_non_gaap_cred(
            hist,
            adj_metrics,
            pd.DataFrame(),
            cache_dir=release_dir,
            rebuild_doc_text_cache=False,
            quiet_pdf_warnings=True,
            doc_registry=registry,
        )
    finally:
        shutil.rmtree(release_dir, ignore_errors=True)
        try:
            if tmp_root.exists() and not any(tmp_root.iterdir()):
                tmp_root.rmdir()
        except Exception:
            pass

    assert not promises.empty
    assert not cred.empty
    assert calls["pdf"] == 1


def test_build_doc_intel_outputs_populates_substage_timings(monkeypatch) -> None:
    monkeypatch.setattr("pbi_xbrl.doc_intel.build_quarter_notes_v2", lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr("pbi_xbrl.doc_intel._build_promises", lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr("pbi_xbrl.doc_intel._build_progress", lambda promises, hist, adj_metrics: pd.DataFrame())
    monkeypatch.setattr("pbi_xbrl.doc_intel._build_non_gaap_cred", lambda *args, **kwargs: pd.DataFrame())

    stage_timings: dict[str, float] = {}
    outputs = build_doc_intel_outputs(
        sec=None,
        cik_int=0,
        submissions={},
        hist=pd.DataFrame(),
        adj_metrics=pd.DataFrame(),
        adj_breakdown=pd.DataFrame(),
        stage_timings=stage_timings,
        profile_timings=False,
    )

    assert len(outputs) == 4
    assert {
        "doc_intel.quarter_notes",
        "doc_intel.promises",
        "doc_intel.promise_progress",
        "doc_intel.non_gaap_cred",
    }.issubset(stage_timings.keys())


def test_extract_promise_candidates_from_notes_keeps_clean_bridge_row() -> None:
    quarter_notes = pd.DataFrame(
        [
            {
                "quarter": "2025-03-31",
                "category": "Guidance / targets",
                "evidence_snippet": "We expect $40 million to $50 million of annualized cost savings to be included in the bridge by end of 2026.",
                "doc_type": "presentation",
            },
        ]
    )

    out = _extract_promise_candidates_from_notes(quarter_notes)

    assert len(out) == 1
    assert str(out.iloc[0]["candidate_scope"]) == "hard_target"
    assert "cost savings" in str(out.iloc[0]["statement_summary"]).lower()
