from __future__ import annotations

from pathlib import Path

from pbi_xbrl.new_ticker_issue_ledger import build_canonical_issue_ledger
from pbi_xbrl.normalized_company_data_validation import validate_normalized_company_data_schema


ROOT = Path(__file__).resolve().parents[1]
LEDGER_SCHEMA = ROOT / "docs" / "new_ticker_issue_ledger.schema.json"


def _text_issue(*, field: str, excerpt: str, source_ref: str = "release.htm") -> dict[str, object]:
    return {
        "severity": "P2",
        "rule_id": "text_quality_demoted",
        "field": field,
        "section": "quarter_notes",
        "classification": "release_header_or_source_title",
        "message": "Demoted non-visible-ready text.",
        "source_ref": source_ref,
        "original_excerpt": excerpt,
        "suggested_action": "Keep it in JSON only.",
    }


def test_exact_semantic_duplicates_collapse_but_occurrences_remain() -> None:
    ledger = build_canonical_issue_ledger(
        manual_review_flags=[
            _text_issue(field="quarter_notes.items.1.note", excerpt="REPORTS RESULTS"),
            _text_issue(field="quarter_notes.items.9.note", excerpt="REPORTS RESULTS"),
        ]
    )

    assert ledger["summary"]["canonical_unique_issue_count"] == 1
    assert ledger["summary"]["detailed_occurrence_count"] == 2
    assert ledger["issues"][0]["occurrence_count"] == 2
    assert len(ledger["occurrences"]) == 2
    assert all(row["detail"]["original_excerpt"] == "REPORTS RESULTS" for row in ledger["occurrences"])


def test_distinct_evidence_remains_distinct() -> None:
    ledger = build_canonical_issue_ledger(
        manual_review_flags=[
            _text_issue(field="quarter_notes.items.1.note", excerpt="REPORTS RESULTS", source_ref="a.htm"),
            _text_issue(field="quarter_notes.items.2.note", excerpt="REPORTS RESULTS", source_ref="b.htm"),
        ]
    )

    assert ledger["summary"]["canonical_unique_issue_count"] == 2
    assert len({row["issue_id"] for row in ledger["issues"]}) == 2


def test_occurrence_identity_is_stable_when_input_order_changes() -> None:
    first = _text_issue(field="quarter_notes.items.1.note", excerpt="REPORTS RESULTS", source_ref="a.htm")
    second = _text_issue(field="quarter_notes.items.2.note", excerpt="OTHER RESULTS", source_ref="b.htm")

    left = build_canonical_issue_ledger(manual_review_flags=[first, second])
    right = build_canonical_issue_ledger(manual_review_flags=[second, first])

    assert {row["issue_id"] for row in left["issues"]} == {row["issue_id"] for row in right["issues"]}
    assert {row["occurrence_id"] for row in left["occurrences"]} == {row["occurrence_id"] for row in right["occurrences"]}


def test_needs_review_excludes_audit_only_and_checks_group_by_rule() -> None:
    ledger = build_canonical_issue_ledger(
        manual_review_flags=[
            _text_issue(field="segments.items.1.note", excerpt="REPORTS RESULTS"),
            {
                "severity": "P2",
                "rule_id": "legacy_adapter_unit_normalization",
                "field": "segments.items.1.metric_value",
                "message": "Confirm normalized unit.",
                "source_ref": "release.htm",
                "suggested_action": "Review unit taxonomy.",
            },
        ]
    )

    qa = ledger["qa_presentation"]
    assert len(qa["qa_log_rows"]) == 2
    assert len(qa["needs_review_rows"]) == 1
    assert qa["needs_review_rows"][0]["rule_id"] == "legacy_adapter_unit_normalization"
    assert {row["rule_id"] for row in qa["qa_check_rows"]} == {
        "text_quality_demoted",
        "legacy_adapter_unit_normalization",
    }


def test_mapping_gap_is_actionable_and_p1_blocks_promotion() -> None:
    ledger = build_canonical_issue_ledger(
        mapping_gaps=[
            {
                "severity": "P1",
                "rule_id": "binding_plan_manual_review",
                "binding_id": "summary_liquidity",
                "normalized_path": "debt_liquidity.total_liquidity",
                "row_key": "scalar",
                "reason": "Required value missing.",
                "source_ref": "",
            }
        ]
    )

    issue = ledger["issues"][0]
    assert issue["issue_type"] == "planner_mapping_gap"
    assert issue["visibility_disposition"] == "needs_review"
    assert issue["promotion_blocking"] is True
    assert issue["render_blocking"] is True
    assert ledger["checks"][0]["status"] == "FAIL"


def test_missing_binding_planner_issue_and_gap_share_one_canonical_issue() -> None:
    event_key = "planner_event|summary_net_leverage|missing_value|debt_liquidity.net_leverage|scalar"
    ledger = build_canonical_issue_ledger(
        mapping_gaps=[
            {
                "severity": "P2",
                "rule_id": "binding_plan_mapping_gap",
                "binding_id": "summary_net_leverage",
                "normalized_path": "debt_liquidity.net_leverage",
                "row_key": "scalar",
                "reason": "Scalar/text value is not populated.",
                "canonical_issue_key": event_key,
            }
        ],
        validation_issues=[
            {
                "severity": "P2",
                "rule_id": "binding_value_missing",
                "field": "summary_net_leverage:debt_liquidity.net_leverage",
                "message": "Scalar/text value is not populated.",
                "normalized_path": "debt_liquidity.net_leverage",
                "business_row_key": "scalar",
                "binding_id": "summary_net_leverage",
                "issue_type": "planner_mapping_gap",
                "canonical_issue_key": event_key,
            }
        ],
        trusted_canonical_issue_keys=[event_key],
    )

    assert ledger["summary"]["canonical_unique_issue_count"] == 1
    assert ledger["summary"]["detailed_occurrence_count"] == 2
    issue = ledger["issues"][0]
    assert issue["rule_id"] == "binding_plan_mapping_gap"
    assert issue["binding_id"] == "summary_net_leverage"
    assert issue["normalized_path"] == "debt_liquidity.net_leverage"
    assert issue["occurrence_count"] == 2


def test_identical_mapping_gap_occurrences_are_lossless() -> None:
    gap = {
        "severity": "P2",
        "rule_id": "binding_plan_mapping_gap",
        "binding_id": "annual_revenue",
        "normalized_path": "annual_financials.rows.0.revenue",
        "row_key": "2025-FY",
        "reason": "Revenue evidence is missing.",
        "source_ref": "fixture:annual",
    }

    ledger = build_canonical_issue_ledger(mapping_gaps=[gap, dict(gap)])

    assert ledger["summary"]["canonical_unique_issue_count"] == 1
    assert ledger["summary"]["detailed_occurrence_count"] == 2
    assert ledger["issues"][0]["occurrence_count"] == 2
    assert len(ledger["occurrences"]) == 2
    assert len({row["occurrence_id"] for row in ledger["occurrences"]}) == 2


def test_caller_key_cannot_merge_different_period_source_or_business_row() -> None:
    base = {
        "severity": "P2",
        "rule_id": "caller_supplied_review",
        "field": "annual_financials.rows.0.revenue",
        "normalized_path": "annual_financials.rows.0.revenue",
        "row_key": "2025-FY",
        "affected_period": "2025-FY",
        "source_ref": "a.htm",
        "message": "Review revenue evidence.",
        "canonical_issue_key": "caller-key",
    }
    rows = [
        base,
        {**base, "affected_period": "2024-FY"},
        {**base, "source_ref": "b.htm"},
        {**base, "row_key": "2024-FY"},
    ]

    ledger = build_canonical_issue_ledger(manual_review_flags=rows)

    assert ledger["summary"]["canonical_unique_issue_count"] == 4
    assert ledger["summary"]["detailed_occurrence_count"] == 4


def test_same_evidence_key_cannot_merge_different_actual_sources() -> None:
    base = {
        "severity": "P2",
        "rule_id": "shared_evidence_key_review",
        "field": "quarter_notes.items.0.commentary",
        "row_key": "2026-Q1|Demand",
        "affected_period": "2026-Q1",
        "evidence_key": "shared-key",
        "source_ref": "release-a.htm#p4",
        "message": "Review source lineage.",
        "canonical_issue_key": "caller-key",
    }

    ledger = build_canonical_issue_ledger(
        manual_review_flags=[base, {**base, "source_ref": "release-b.htm#p7"}]
    )

    assert ledger["summary"]["canonical_unique_issue_count"] == 2
    assert ledger["summary"]["detailed_occurrence_count"] == 2


def test_explicit_multi_source_synthesis_can_group_contributing_sources() -> None:
    synthesis = {
        "synthesis_id": "margin-bridge-2026q1",
        "source_refs": ["call.txt#L20", "presentation.pdf#p8"],
    }
    base = {
        "severity": "P2",
        "rule_id": "synthesized_evidence_review",
        "field": "operating_drivers.items.0.current_read",
        "row_key": "Margin|2026-Q1",
        "affected_period": "2026-Q1",
        "evidence_key": "margin-synthesis",
        "source_ref": "call.txt#L20",
        "source_synthesis": synthesis,
        "message": "Review synthesized margin evidence.",
    }

    ledger = build_canonical_issue_ledger(
        manual_review_flags=[base, {**base, "source_ref": "presentation.pdf#p8"}]
    )

    assert ledger["summary"]["canonical_unique_issue_count"] == 1
    assert ledger["summary"]["detailed_occurrence_count"] == 2
    assert ledger["issues"][0]["source_refs"] == ["call.txt#L20", "presentation.pdf#p8"]


def test_same_caller_key_and_true_duplicate_remains_one_issue_with_two_occurrences() -> None:
    row = {
        "severity": "P2",
        "rule_id": "caller_supplied_review",
        "field": "annual_financials.rows.0.revenue",
        "normalized_path": "annual_financials.rows.0.revenue",
        "row_key": "2025-FY",
        "affected_period": "2025-FY",
        "source_ref": "a.htm",
        "message": "Review revenue evidence.",
        "canonical_issue_key": "caller-key",
    }

    ledger = build_canonical_issue_ledger(manual_review_flags=[row, dict(row)])

    assert ledger["summary"]["canonical_unique_issue_count"] == 1
    assert ledger["summary"]["detailed_occurrence_count"] == 2
    assert ledger["issues"][0]["occurrence_count"] == 2


def test_untrusted_planner_shaped_key_cannot_bypass_identity_boundaries() -> None:
    event_key = "planner_event|annual_revenue|missing_value|annual_financials.rows.0.revenue|2025-FY"
    base = {
        "severity": "P2",
        "rule_id": "caller_supplied_review",
        "field": "annual_financials.rows.0.revenue",
        "normalized_path": "annual_financials.rows.0.revenue",
        "binding_id": "annual_revenue",
        "row_key": "2025-FY",
        "source_ref": "a.htm",
        "message": "Review revenue evidence.",
        "canonical_issue_key": event_key,
    }

    ledger = build_canonical_issue_ledger(manual_review_flags=[base, {**base, "source_ref": "b.htm"}])

    assert ledger["summary"]["canonical_unique_issue_count"] == 2
    assert ledger["summary"]["detailed_occurrence_count"] == 2


def test_explicit_p2_render_blocker_is_needs_review_and_blocks() -> None:
    ledger = build_canonical_issue_ledger(
        manual_review_flags=[
            {
                "severity": "P2",
                "rule_id": "explicit_render_blocker",
                "field": "normalized_guidance.items",
                "message": "Review before rendering.",
                "source_ref": "release.htm",
                "render_blocking": True,
            }
        ]
    )

    issue = ledger["issues"][0]
    assert issue["severity"] == "P2"
    assert issue["promotion_blocking"] is False
    assert issue["render_blocking"] is True
    assert issue["visibility_disposition"] == "needs_review"
    assert ledger["summary"]["blocking_issue_count"] == 1
    assert ledger["checks"][0]["status"] == "FAIL"


def test_explicit_executed_checks_include_pass_rows() -> None:
    ledger = build_canonical_issue_ledger(
        check_results=[
            {
                "rule_id": "normalized_json_schema_validation",
                "status": "PASS",
                "interpretation": "Completed with no issues.",
                "detail_ref": "binding_plan.schema_issues",
            }
        ]
    )

    assert ledger["checks"] == [
        {
            "rule_id": "normalized_json_schema_validation",
            "status": "PASS",
            "unique_issue_count": 0,
            "occurrence_count": 0,
            "blocking_count": 0,
            "actionable_count": 0,
            "affected_sections": "",
            "interpretation": "Completed with no issues.",
            "detail_ref": "binding_plan.schema_issues",
        }
    ]
    assert ledger["qa_presentation"]["qa_check_rows"][0]["status"] == "PASS"


def test_generated_ledger_matches_checked_in_schema() -> None:
    ledger = build_canonical_issue_ledger(
        manual_review_flags=[
            _text_issue(field="quarter_notes.items.1.note", excerpt="REPORTS RESULTS"),
        ]
    )

    assert validate_normalized_company_data_schema(ledger, schema_path=LEDGER_SCHEMA) == []
