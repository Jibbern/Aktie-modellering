from __future__ import annotations

import datetime as dt
import sys
import types

import pandas as pd

from pbi_xbrl.filing_evidence_shared import (
    classify_statement_evidence_role,
    build_canonical_subject_key,
    build_evidence_event,
    build_follow_through_event,
    build_follow_through_signal,
    build_lifecycle_subject_key,
    build_parent_subject_key,
    build_promise_lifecycle_key,
    confidence_rank,
    decode_blob_text,
    derive_lifecycle_state,
    derive_status_resolution_reason,
    extract_document_text,
    filing_quarter_end,
    format_pct,
    history_quarter_ends,
    iter_submission_batches,
    merge_same_subject_events,
    merge_evidence_events,
    merge_follow_through_signals,
    narrative_drop_reason,
    promise_candidate_drop_reason,
    renderable_note_drop_reason,
    pick_filing_docs,
    source_class,
    statement_class,
    route_to_investor_note_candidate,
    route_to_measurable_promise_candidate,
    qualify_promise_candidate,
    qualify_renderable_note,
    split_sentences,
)


class _FakeSecClient:
    def __init__(self, payloads: dict[str, dict]) -> None:
        self._payloads = payloads

    def get(self, url: str, *, as_json: bool, cache_key: str):  # type: ignore[override]
        assert as_json is True
        assert cache_key.startswith("submissions_")
        return self._payloads[url]


def test_filing_quarter_end_maps_8k_to_latest_completed_quarter() -> None:
    assert filing_quarter_end("8-K", "2025-07-15", "2025-07-16") == dt.date(2025, 6, 30)
    assert filing_quarter_end("10-Q", "2025-06-30", "2025-08-05") == dt.date(2025, 6, 30)


def test_history_quarter_ends_normalizes_and_limits() -> None:
    hist = pd.DataFrame(
        {
            "quarter": [
                "2025-03-31",
                pd.Timestamp("2025-06-30"),
                "bad-value",
                pd.Timestamp("2025-09-30"),
            ]
        }
    )

    assert history_quarter_ends(hist, max_quarters=2) == [
        dt.date(2025, 6, 30),
        dt.date(2025, 9, 30),
    ]


def test_confidence_rank_handles_known_levels_and_unknowns() -> None:
    assert confidence_rank("high") == 3
    assert confidence_rank("med") == 2
    assert confidence_rank("low") == 1
    assert confidence_rank("unknown") == 0


def test_decode_blob_text_prefers_single_byte_fallback_when_utf8_fails() -> None:
    assert decode_blob_text("café".encode("cp1252")) == "café"


def test_extract_document_text_normalizes_html_and_txt_inputs() -> None:
    html_out = extract_document_text(
        "demo.html",
        b"<div>Hello&nbsp;<b>world</b><script>ignore()</script><p>Next</p></div>",
    )
    txt_out = extract_document_text("demo.txt", b"Alpha\n Beta")

    assert "Hello world" in html_out
    assert "Next" in html_out
    assert "ignore" not in html_out
    assert txt_out == "Alpha Beta"


def test_extract_document_text_reads_pdf_pages_via_pdfplumber(monkeypatch) -> None:
    class _FakePage:
        def __init__(self, text: str) -> None:
            self._text = text

        def extract_text(self) -> str:
            return self._text

    class _FakePdf:
        def __init__(self) -> None:
            self.pages = [_FakePage("First page"), _FakePage("Second page")]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    fake_module = types.SimpleNamespace(open=lambda _: _FakePdf())
    monkeypatch.setitem(sys.modules, "pdfplumber", fake_module)

    out = extract_document_text("deck.pdf", b"%PDF-1.4", quiet_pdf_warnings=False)

    assert out == "First page Second page"


def test_format_pct_handles_numeric_and_missing_values() -> None:
    assert format_pct(0.125) == "12.5%"
    assert format_pct(None) == "n/a"


def test_pick_filing_docs_demotes_cover_index_when_requested() -> None:
    picked = pick_filing_docs(
        "cover.htm",
        [
            {"name": "cover.htm"},
            {"name": "ex99-1.htm"},
            {"name": "presentation.pdf"},
            {"name": "index.html"},
        ],
        penalize_admin_docs=True,
        max_docs=4,
    )

    assert picked[:2] == ["ex99-1.htm", "presentation.pdf"]


def test_iter_submission_batches_loads_recent_batches_from_subfiles() -> None:
    sec = _FakeSecClient(
        {
            "https://data.sec.gov/submissions/submissions-extra.json": {
                "filings": {
                    "recent": {
                        "accessionNumber": ["0001"],
                        "form": ["8-K"],
                    }
                }
            }
        }
    )
    base = {
        "filings": {
            "recent": {
                "accessionNumber": ["0000"],
                "form": ["10-Q"],
            },
            "files": [{"name": "submissions-extra.json"}],
        }
    }

    batches = iter_submission_batches(sec, base)

    assert len(batches) == 2
    assert batches[0]["accessionNumber"] == ["0000"]
    assert batches[1]["accessionNumber"] == ["0001"]


def test_split_sentences_drops_short_fragments_and_truncates() -> None:
    text = "Short. This is a much longer sentence that should be kept because it has enough characters. " + ("x" * 600)

    out = split_sentences(text)

    assert len(out) == 2
    assert out[0].startswith("This is a much longer sentence")
    assert len(out[1]) == 500


def test_qualify_renderable_note_accepts_clean_narrative_sentence() -> None:
    note = qualify_renderable_note(
        "Presort margins improved as pricing and mix offset lower volumes in the quarter.",
        source_type="earnings_release",
        metric_hint="Adjusted EBIT / margin",
        theme_hint="Results / drivers",
        base_score=70.0,
    )

    assert note is not None
    assert note.summary.startswith("Presort margins improved")
    assert note.preferred_source is True
    assert note.display_score >= 72.0


def test_qualify_renderable_note_rejects_table_and_scaffolding_fragments() -> None:
    assert qualify_renderable_note(
        "Presort Services 35,940 9,139 4,200 1,100",
        source_type="presentation",
        metric_hint="Revenue",
        theme_hint="Segment signal",
        base_score=90.0,
    ) is None
    assert narrative_drop_reason(
        "would be incremental to Management target",
        "earnings_release",
    ) == "scaffolding"
    assert qualify_renderable_note(
        "Forward-looking statements are subject to risks and uncertainties.",
        source_type="earnings_release",
        metric_hint="Tone",
        theme_hint="Management tone / confidence",
        base_score=90.0,
    ) is None


def test_qualify_promise_candidate_accepts_clean_target_and_milestone_and_rejects_scaffolding() -> None:
    hard_target = qualify_promise_candidate(
        "We expect adjusted EBIT of $450 million to $465 million for FY 2025.",
        source_type="earnings_release",
        metric_hint="Adjusted EBIT guidance",
    )
    milestone = qualify_promise_candidate(
        "York will be fully operational by Q4 2025.",
        source_type="transcript",
        metric_hint="Strategic milestone",
    )

    assert hard_target is not None
    assert hard_target.scope == "hard_target"
    assert milestone is not None
    assert milestone.scope == "clean_milestone"
    assert qualify_promise_candidate(
        "Bowes provided the following Management target",
        source_type="presentation",
        metric_hint="Management target",
    ) is None
    assert qualify_promise_candidate(
        "45Z from remaining facilities",
        source_type="presentation",
        metric_hint="45Z monetization / EBITDA",
    ) is None


def test_renderable_note_allows_short_investor_phrase_and_clean_bridge_rows() -> None:
    phrase = qualify_renderable_note(
        "York carbon capture fully operational",
        source_type="earnings_release",
        metric_hint="Strategic milestone",
        theme_hint="Operational drivers",
        base_score=68.0,
    )
    bridge = qualify_renderable_note(
        "45Z monetization included in crush bridge at $95 million for FY 2025.",
        source_type="presentation",
        metric_hint="45Z monetization / EBITDA",
        theme_hint="Hidden but important",
        base_score=68.0,
    )

    assert phrase is not None
    assert phrase.summary.startswith("York carbon capture fully operational")
    assert bridge is not None
    assert bridge.display_score >= 73.0


def test_renderable_note_allows_clean_model_metric_bridges_for_debt_fcf_and_revolver() -> None:
    revolver = qualify_renderable_note(
        "Revolver availability moved to $275.0m at 2025-06-30 (delta $50.0m).",
        source_type="model_metric",
        metric_hint="revolver_availability_change",
        theme_hint="Debt / liquidity / covenants",
        base_score=60.0,
    )
    fcf = qualify_renderable_note(
        "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m.",
        source_type="history_q",
        metric_hint="FCF",
        theme_hint="Cash flow / FCF / capex",
        base_score=60.0,
    )

    assert revolver is not None
    assert "Revolver availability moved to $275.0m" in revolver.summary
    assert fcf is not None
    assert "FCF TTM at 2024-12-31" in fcf.summary


def test_quality_drop_reason_helpers_use_normalized_values() -> None:
    assert renderable_note_drop_reason(
        "permit map county parcel latitude longitude list $10 million",
        source_type="ocr",
    ) == "fragmentary_text"
    assert renderable_note_drop_reason(
        "Forward-looking statements are subject to risks and uncertainties.",
        source_type="earnings_release",
    ) == "legal_boilerplate"
    assert promise_candidate_drop_reason(
        "Bowes provided the following Management target",
        source_type="presentation",
        metric_hint="Management target",
    ) == "scaffolding"


def test_qualify_promise_candidate_accepts_short_phrase_and_clean_bridge_rows() -> None:
    short_phrase = qualify_promise_candidate(
        "York carbon capture fully operational",
        source_type="earnings_release",
        metric_hint="Strategic milestone",
    )
    bridge = qualify_promise_candidate(
        "45Z monetization included in crush bridge at $95 million for FY 2025.",
        source_type="presentation",
        metric_hint="45Z monetization / EBITDA",
    )

    assert short_phrase is not None
    assert short_phrase.scope == "clean_milestone"
    assert bridge is not None
    assert bridge.scope == "hard_target"


def test_build_evidence_event_normalizes_note_into_structured_event() -> None:
    event = build_evidence_event(
        "Risk management supports margins and cash flow through the cycle.",
        source_type="earnings_release",
        metric_hint="Risk management",
        theme_hint="Results / drivers",
        base_score=72.0,
        period_norm="Q32025",
        source_doc="release_q3.txt",
    )

    assert event is not None
    assert event.event_type == "operational_driver"
    assert event.metric_family == "risk_management"
    assert event.entity_scope == "company_total"
    assert event.period_norm == "Q32025"
    assert event.summary == "Risk management supports margins and cash flow through the cycle."


def test_merge_evidence_events_dedupes_same_event_key_and_keeps_best_summary() -> None:
    ev1 = build_evidence_event(
        "Risk management supports margins and cash flow.",
        source_type="earnings_release",
        metric_hint="Risk management",
        theme_hint="Results / drivers",
        base_score=74.0,
        period_norm="Q32025",
        source_doc="release_q3.txt",
    )
    ev2 = build_evidence_event(
        "Risk management continued to support margins and cash flow.",
        source_type="transcript",
        metric_hint="Risk management",
        theme_hint="Results / drivers",
        base_score=70.0,
        period_norm="Q32025",
        source_doc="transcript_q3.txt",
    )

    assert ev1 is not None and ev2 is not None
    merged = merge_evidence_events([ev1, ev2], hard_cap=12, quietly_removed_cap=3)

    assert len(merged) == 1
    assert merged[0].event_key == ev1.event_key
    assert "Risk management" in merged[0].summary


def test_merge_follow_through_signals_dedupes_and_keeps_best_progress_summary() -> None:
    sig1 = build_follow_through_signal(
        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
        source_type="earnings_release",
        metric_hint="Debt reduction",
        base_score=92.0,
        period_norm="Q32025",
        source_doc="release_q3.txt",
        quarter_end="2025-09-30",
    )
    sig2 = build_follow_through_signal(
        "Obion sale proceeds fully repaid $130.7 million junior mezzanine debt.",
        source_type="presentation",
        metric_hint="Debt reduction",
        base_score=78.0,
        period_norm="Q32025",
        source_doc="slides_q3.txt",
        quarter_end="2025-09-30",
    )

    assert sig1 is not None and sig2 is not None
    merged = merge_follow_through_signals([sig1, sig2], hard_cap=8)

    assert len(merged) == 1
    assert merged[0].event_type == "deleveraging"
    assert "Obion" in merged[0].summary or "repay" in merged[0].summary.lower()
    assert merged[0].quarter_end == "2025-09-30"


def test_canonical_subject_key_distinguishes_related_but_distinct_45z_subjects() -> None:
    monetization = build_canonical_subject_key(
        entity_scope="advantage_nebraska",
        metric_family="regulatory_credit",
        target_period_norm="FY2026",
        stage_token="monetization",
    )
    qualification = build_canonical_subject_key(
        entity_scope="advantage_nebraska",
        metric_family="regulatory_credit",
        target_period_norm="FY2026",
        stage_token="qualification",
    )

    assert monetization != qualification
    assert build_promise_lifecycle_key(monetization, stage_token="monetization").startswith(monetization)


def test_shared_routing_splits_note_vs_measurable_promise_generically() -> None:
    note = route_to_investor_note_candidate(
        "Risk management supports margins and cash flow through the cycle.",
        quarter="2025-09-30",
        source_type="earnings_release",
        source_doc="release_q3.txt",
        metric_hint="Risk management",
        theme_hint="Results / drivers",
        base_score=72.0,
        period_norm="Q2025Q3",
    )
    promise = route_to_measurable_promise_candidate(
        "We expect adjusted EBIT of $450 million to $465 million for FY 2025.",
        quarter="2025-03-31",
        source_type="earnings_release",
        source_doc="release_q1.txt",
        metric_hint="Adjusted EBIT guidance",
        target_period_norm="FY2025",
        base_score=80.0,
    )

    assert note is not None
    assert note.routing_reason == "broad_investor_note"
    assert note.route_reason == "quarter_notes"
    assert note.candidate_type == "investor_note_candidate"
    assert note.canonical_subject_key
    assert promise is not None
    assert promise.routing_reason == "measurable_target"
    assert promise.route_reason == "promise_tracker"
    assert promise.candidate_type == "measurable_promise_candidate"
    assert promise.candidate_scope == "hard_target"
    assert promise.canonical_subject_key
    assert promise.lifecycle_state == "stated"


def test_follow_through_event_and_same_subject_merge_handle_wording_variants() -> None:
    sig1 = build_follow_through_event(
        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
        quarter="2025-09-30",
        source_type="earnings_release",
        source_doc="release_q3.txt",
        metric_hint="Debt reduction",
        period_norm="Q2025Q3",
        base_score=92.0,
    )
    sig2 = build_follow_through_event(
        "Obion sale proceeds fully repaid $130.7 million junior mezzanine debt.",
        quarter="2025-09-30",
        source_type="presentation",
        source_doc="slides_q3.txt",
        metric_hint="Debt reduction",
        period_norm="Q2025Q3",
        base_score=78.0,
    )

    assert sig1 is not None and sig2 is not None
    assert sig1.canonical_subject_key == sig2.canonical_subject_key
    assert sig1.route_reason == "promise_progress"
    assert sig1.candidate_type == "follow_through_event"
    merged = merge_same_subject_events([sig1, sig2], hard_cap=8)
    assert len(merged) == 1
    assert merged[0].canonical_subject_key == sig1.canonical_subject_key
    assert merged[0].merge_reason == "canonical_subject_match"


def test_lifecycle_and_status_reason_helpers_are_deterministic() -> None:
    pending_state = derive_lifecycle_state(
        target_period_norm="FY2026",
        stated_quarter="2025-03-31",
        latest_evidence_quarter="2025-06-30",
        evaluated_through_quarter="2025-09-30",
        carried_to_quarter="2025-09-30",
        current_status="pending",
    )
    updated_state = derive_lifecycle_state(
        target_period_norm="FY2025",
        stated_quarter="2025-03-31",
        latest_evidence_quarter="2025-09-30",
        evaluated_through_quarter="2025-09-30",
        carried_to_quarter="2025-09-30",
        current_status="on_track",
    )
    resolved_state = derive_lifecycle_state(
        target_period_norm="FY2025",
        stated_quarter="2025-03-31",
        latest_evidence_quarter="2025-12-31",
        evaluated_through_quarter="2025-12-31",
        carried_to_quarter="2025-12-31",
        current_status="resolved_beat",
    )

    assert pending_state == "pending_period_end"
    assert updated_state == "updated_by_later_evidence"
    assert resolved_state == "resolved"
    assert derive_status_resolution_reason(
        current_status="resolved_beat",
        latest_value=123.4,
        lifecycle_state=resolved_state,
    ) == "actual_over_text_progress"
    assert derive_status_resolution_reason(
        current_status="pending",
        latest_value="",
        lifecycle_state=pending_state,
    ) == "pending_until_period_end"


def test_source_and_statement_classification_is_stable_for_routing_inputs() -> None:
    assert source_class("earnings_release") == "preferred_narrative"
    assert source_class("presentation") == "preferred_narrative"
    assert source_class("ocr") == "weak_support"

    assert statement_class(
        "Forward-looking statements involve risks and uncertainties.",
        "earnings_release",
    ) == "boilerplate"
    assert statement_class(
        "45Z monetization included in crush bridge at $95 million for FY 2025.",
        "presentation",
        "45Z monetization / EBITDA",
    ) in {"structured_numeric_bridge", "table_fragment"}
    assert statement_class(
        "York carbon capture fully operational",
        "earnings_release",
    ) == "investor_phrase"


def test_classify_statement_evidence_role_distinguishes_origin_from_later_result() -> None:
    role_origin, drop_origin = classify_statement_evidence_role(
        "We target $80 million to $100 million of annualized cost savings by end of 2026.",
        source_type="earnings_release",
        metric_hint="Cost savings target",
        target_period_norm="FY2026",
        promise_type="guidance_range",
    )
    role_result, drop_result = classify_statement_evidence_role(
        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
        source_type="earnings_release",
        metric_hint="Strategic milestone",
        target_period_norm="Q42025",
        promise_type="milestone",
    )

    assert role_origin == "promise_origin"
    assert drop_origin == ""
    assert role_result in {"later_evidence", "result_evidence"}
    assert drop_result == ""


def test_classify_statement_evidence_role_treats_repurchase_and_debt_paydown_as_result_evidence() -> None:
    role_result, drop_result = classify_statement_evidence_role(
        "Deployed significant cash flow into repurchasing 12.6 million shares for $127 million and reducing principal debt in Q4 2025.",
        source_type="earnings_release",
        metric_hint="Capital allocation / debt reduction",
        target_period_norm="Q42025",
        promise_type="operational",
    )

    assert role_result == "result_evidence"
    assert drop_result == ""


def test_parent_and_lifecycle_subject_keys_keep_related_topics_separate() -> None:
    parent = build_parent_subject_key(
        entity_scope="Advantage Nebraska",
        metric_family="regulatory_credit",
        topic_family="45Z / carbon capture",
        program_token="45z",
    )
    canonical_monetization = build_canonical_subject_key(
        entity_scope="Advantage Nebraska",
        metric_family="regulatory_credit",
        target_period_norm="Q42025",
        scope_token="advantage_nebraska",
        program_token="45z",
        stage_token="monetization_agreement",
    )
    canonical_readiness = build_canonical_subject_key(
        entity_scope="Advantage Nebraska",
        metric_family="regulatory_credit",
        target_period_norm="Q42025",
        scope_token="advantage_nebraska",
        program_token="45z",
        stage_token="startup_or_commissioning",
    )
    monetization = build_lifecycle_subject_key(
        parent_subject_key=parent,
        canonical_subject_key=canonical_monetization,
        target_period_norm="Q42025",
        stage_token="monetization_agreement",
    )
    readiness = build_lifecycle_subject_key(
        parent_subject_key=parent,
        canonical_subject_key=canonical_readiness,
        target_period_norm="Q42025",
        stage_token="startup_or_commissioning",
    )

    assert parent
    assert "45z" in parent
    assert monetization != readiness
    assert "monetization_agreement" in monetization
    assert "startup_or_commissioning" in readiness
