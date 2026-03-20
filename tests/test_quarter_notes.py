from __future__ import annotations

import datetime as dt
import json

import pandas as pd

from pbi_xbrl.quarter_notes import NoteCandidate, _dedupe_candidates, _filing_quarter_end, _select_candidates, validate_quarter_notes


def test_filing_quarter_end_maps_8k_to_latest_completed_quarter() -> None:
    assert _filing_quarter_end("8-K", "2025-07-15", "2025-07-16") == dt.date(2025, 6, 30)
    assert _filing_quarter_end("10-Q", "2025-06-30", "2025-08-05") == dt.date(2025, 6, 30)


def test_dedupe_candidates_keeps_best_candidate_deterministically() -> None:
    quarter_end = dt.date(2025, 6, 30)
    lower = NoteCandidate(
        note_id="low",
        quarter_end=quarter_end,
        topic="Revenue",
        metric="revenue_ttm_yoy",
        headline="Revenue improved",
        body="Revenue improved materially this quarter by contract wins.",
        severity_score=55.0,
        confidence="med",
        evidence=[{"doc_path": "doc_a", "section_or_page": "p1", "snippet": "Revenue improved"}],
        method="metric_delta",
    )
    higher = NoteCandidate(
        note_id="high",
        quarter_end=quarter_end,
        topic="Revenue",
        metric="revenue_ttm_yoy",
        headline="Revenue improved",
        body="Revenue improved materially this quarter by contract wins.",
        severity_score=72.0,
        confidence="high",
        evidence=[{"doc_path": "doc_b", "section_or_page": "p2", "snippet": "Revenue improved", "anchor_hit": "this quarter"}],
        method="metric_delta",
    )

    deduped, dedup_counts = _dedupe_candidates([lower, higher])

    assert [candidate.note_id for candidate in deduped] == ["high"]
    assert dedup_counts == {quarter_end: 1}


def test_select_candidates_prefers_higher_confidence_when_severity_tied() -> None:
    quarter_end = dt.date(2025, 6, 30)
    low = NoteCandidate(
        note_id="low",
        quarter_end=quarter_end,
        topic="Guidance",
        metric="revenue_yoy",
        headline="Revenue stabilizes",
        body="Revenue should stabilize by year end.",
        severity_score=58.0,
        confidence="low",
        evidence=[{"doc_path": "doc_a", "section_or_page": "p1", "snippet": "Revenue should stabilize by year end."}],
        method="topic_scan",
    )
    high = NoteCandidate(
        note_id="high",
        quarter_end=quarter_end,
        topic="Guidance",
        metric="revenue_yoy",
        headline="Revenue stabilizes",
        body="Revenue should stabilize by year end with improved execution.",
        severity_score=58.0,
        confidence="high",
        evidence=[{"doc_path": "doc_b", "section_or_page": "p2", "snippet": "Revenue should stabilize by year end."}],
        method="topic_scan",
    )

    selected = _select_candidates([low, high])

    assert [candidate.note_id for candidate in selected[:2]] == ["high", "low"]


def test_validate_quarter_notes_flags_missing_evidence_and_metric_nan() -> None:
    quarter_end = dt.date(2025, 6, 30)
    quarter_notes = pd.DataFrame(
        [
            {
                "quarter": quarter_end,
                "note_id": "missing-evidence",
                "claim": "Margins should improve.",
                "body": "Margins should improve.",
                "metric_ref": "margin_delta",
                "metric_value": None,
                "evidence_json": "",
            },
            {
                "quarter": quarter_end,
                "note_id": "bad-metric",
                "claim": "Growth accelerates.",
                "body": "Growth accelerates without a dated anchor.",
                "metric_ref": "custom_metric",
                "metric_value": None,
                "evidence_json": json.dumps(
                    [
                        {
                            "doc_path": "doc.html",
                            "section_or_page": "Section 1",
                            "snippet": "Growth accelerates.",
                        }
                    ]
                ),
            },
        ]
    )
    hist = pd.DataFrame({"quarter": [quarter_end], "revenue": [100.0]})

    checks = validate_quarter_notes(quarter_notes, hist)

    assert "quarter_note_evidence_missing" in set(checks["check"])
    assert "quarter_note_metric_nan" in set(checks["check"])
    assert "quarter_note_missing_time_anchor" in set(checks["check"])
