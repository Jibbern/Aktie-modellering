from __future__ import annotations

import json
from pathlib import Path

import pytest


def _gpre_bofa_conference_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "sec_cache" / "GPRE" / "external" / "conferences" / "2026-02-26_bofa"


def _load_structured_rows() -> list[dict]:
    conf_dir = _gpre_bofa_conference_dir()
    json_path = conf_dir / "structured_statements.json"
    if not json_path.exists():
        pytest.skip(f"Normalized conference JSON missing: {json_path}")
    return json.loads(json_path.read_text(encoding="utf-8"))


def test_gpre_external_conference_files_exist_and_are_not_sec_named() -> None:
    conf_dir = _gpre_bofa_conference_dir()
    transcript_path = conf_dir / "transcript.md"
    json_path = conf_dir / "structured_statements.json"

    if not transcript_path.exists() or not json_path.exists():
        pytest.skip(f"Normalized conference source files missing under {conf_dir}")

    assert conf_dir.name == "2026-02-26_bofa"
    assert transcript_path.name == "transcript.md"
    assert json_path.name == "structured_statements.json"
    assert not transcript_path.name.startswith("doc_")
    assert not json_path.name.startswith("doc_")

    transcript = transcript_path.read_text(encoding="utf-8")
    assert "Bank of America 2026 Global Agriculture & Materials Conference" in transcript
    assert "Company: Green Plains Inc. (GPRE)" in transcript
    assert "Chris Osowski, CEO:" in transcript
    assert "Anne Reese, CFO:" in transcript
    assert "â" not in transcript
    assert "�" not in transcript


def test_gpre_external_conference_structured_rows_are_reusable_and_conservative() -> None:
    rows = _load_structured_rows()
    assert isinstance(rows, list)
    assert len(rows) == 34

    required_fields = {
        "company",
        "speaker_role",
        "source_file",
        "source_excerpt",
        "promise_candidate",
        "guidance_candidate",
        "quarter_notes_candidate",
        "promise_family",
        "source_type",
        "source_origin",
        "event",
        "date",
        "speaker",
        "topic",
        "subtopic",
        "statement_type",
        "text",
        "source_location",
        "normalized_points",
        "numbers",
        "timeframe",
        "horizon_label",
        "importance",
        "confidence",
        "needs_manual_review",
        "milestone_type",
        "metric_type",
    }

    for row in rows:
        assert required_fields.issubset(row.keys())
        assert row["company"] == "Green Plains Inc."
        assert row["source_type"] == "conference"
        assert row["source_origin"] == "external"
        assert row["source_file"] == "external/conferences/2026-02-26_bofa/transcript.md"
        assert isinstance(row["source_excerpt"], str) and row["source_excerpt"].strip()
        assert isinstance(row["promise_candidate"], bool)
        assert isinstance(row["guidance_candidate"], bool)
        assert isinstance(row["quarter_notes_candidate"], bool)
        assert isinstance(row["promise_family"], str)

    analyst_rows = [row for row in rows if row.get("speaker_role") == "Analyst"]
    assert analyst_rows
    assert all(
        not row["promise_candidate"] and not row["guidance_candidate"] and not row["quarter_notes_candidate"]
        for row in analyst_rows
    )

    glitch_row = next(row for row in rows if row["statement_type"] == "transcript_glitch")
    assert glitch_row["needs_manual_review"] is True
    assert glitch_row["promise_candidate"] is False
    assert glitch_row["guidance_candidate"] is False
    assert glitch_row["quarter_notes_candidate"] is False
    assert "$188 million" in glitch_row["source_excerpt"]

    promise_rows = [row for row in rows if row["promise_candidate"]]
    assert len(promise_rows) == 1
    assert promise_rows[0]["promise_family"] == "45Z EBITDA outlook"
    assert promise_rows[0]["metric_type"] == "ebitda"
    assert promise_rows[0]["guidance_candidate"] is True
    assert promise_rows[0]["quarter_notes_candidate"] is True

    credit_row = next(row for row in rows if row["promise_family"] == "Credit monetization outlook")
    assert credit_row["promise_candidate"] is False
    assert credit_row["guidance_candidate"] is True
    assert credit_row["quarter_notes_candidate"] is True

    farm_timing_row = next(row for row in rows if row["promise_family"] == "Farm-practices upside timing")
    assert farm_timing_row["guidance_candidate"] is True
    assert farm_timing_row["quarter_notes_candidate"] is True

    co2_eval_row = next(row for row in rows if row["promise_family"] == "CO2 logistics evaluation milestone")
    assert co2_eval_row["guidance_candidate"] is True
    assert co2_eval_row["quarter_notes_candidate"] is False
    assert co2_eval_row["milestone_type"] == "evaluation_in_progress"

    feedstock_row = next(row for row in rows if row["subtopic"] == "Low-CI feedstock role")
    assert "low-CI feedstock" in feedstock_row["text"]
    assert "Canada" not in feedstock_row["text"]
    assert "Mexico" not in feedstock_row["text"]
    assert feedstock_row["needs_manual_review"] is False

    clean_families = {row["promise_family"] for row in rows if row["promise_family"]}
    assert "45Z EBITDA outlook" in clean_families
    assert "Carbon capture operational milestone" in clean_families
    assert "Farm-practices upside timing" in clean_families
    assert "Credit monetization outlook" in clean_families
    assert "CO2 logistics evaluation milestone" in clean_families
    assert not any("nan" in family.lower() for family in clean_families)
