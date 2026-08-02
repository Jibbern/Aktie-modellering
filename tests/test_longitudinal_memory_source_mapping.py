from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.sector_packs.retail import (
    RETAIL_SECTOR_PACK,
    RetailSemanticError,
    parse_guidance_percent,
    parse_net_openings_table,
    parse_percent_text,
)
from pbi_xbrl.longitudinal_memory.source_adapter.builder import (
    _evidence_occurrences,
    _extract,
    build_source_native_sidecar,
)
from pbi_xbrl.longitudinal_memory.source_adapter.discovery import discover_sources, load_source_set
from pbi_xbrl.longitudinal_memory.source_adapter.mapping import map_candidates
from pbi_xbrl.longitudinal_memory.source_adapter.periods import reconcile_periods
from pbi_xbrl.longitudinal_memory.source_adapter.types import (
    MappingError,
    SourceAdapterError,
    SourceContractError,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")


def _raw() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _write(tmp_path: Path, value: dict) -> Path:
    path = tmp_path / "source-set.json"
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8", newline="\n")
    return path


def _build(path: Path = FIXTURE):
    return build_source_native_sidecar(
        path,
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0%", {"kind": "exact", "value": "0"}),
        ("(3)%", {"kind": "exact", "value": "-3"}),
        (
            "Growth in the range of 3% to 5%",
            {"kind": "range", "low": "3", "high": "5", "low_inclusive": True, "high_inclusive": True},
        ),
        ("Growth of at least 6%", {"kind": "bound", "operator": "gte", "value": "6"}),
        (
            "Around 13%",
            {"kind": "approximate", "value": "13", "qualifier": "around", "tolerance": None},
        ),
    ],
)
def test_exact_zero_negative_range_bound_and_approximation_are_lossless(
    text: str, expected: dict
) -> None:
    parser = parse_percent_text if expected["kind"] == "exact" else parse_guidance_percent
    assert parser(text) == expected


def test_net_openings_uses_signed_closure_total() -> None:
    excerpt = "New | 27 | 15 | 4 | 6 | 5 | 5 | 36 | 26 | 62 | Permanently closed | (3) | (4) | (1) | (5) | (4) | (5) | (8) | (14) | (22)"
    assert parse_net_openings_table(excerpt) == {"kind": "exact", "value": "40"}


def test_point_coercion_and_positive_closure_coercion_are_rejected() -> None:
    with pytest.raises(RetailSemanticError):
        parse_guidance_percent("midpoint")
    assert RETAIL_SECTOR_PACK.parse_value("parser:retail:count-text@1", "(22)") == {
        "kind": "exact",
        "value": "-22",
    }


def test_unknown_retail_metric_alias_fails_mapping() -> None:
    source_set = load_source_set(FIXTURE)
    discovered = discover_sources(source_set, SOURCE_ROOT)
    evidence = _extract(source_set, discovered)
    assertions = [dict(row) for row in source_set.required_assertions]
    target = next(row for row in assertions if row["assertion_kind"] == "numerical_fact")
    target["metric_key"] = "unknown-retail-metric"
    mutated = dataclasses.replace(source_set, required_assertions=tuple(assertions))
    with pytest.raises(RetailSemanticError, match="Unknown retail metric"):
        map_candidates(
            mutated,
            evidence,
            sector_pack=RETAIL_SECTOR_PACK,
            ticker_profile=load_anf_profile(mutated),
        )


def test_ambiguous_profile_member_alias_fails(tmp_path: Path) -> None:
    value = _raw()
    duplicate = copy.deepcopy(value["profile"]["member_aliases"][0])
    duplicate["member_id"] = "member:core:company:company:different@1"
    value["profile"]["member_aliases"].append(duplicate)
    with pytest.raises(MappingError, match="Ambiguous"):
        load_anf_profile(load_source_set(_write(tmp_path, value)))


def test_dimension_alias_must_match_source_row_fingerprint(tmp_path: Path) -> None:
    value = _raw()
    row = next(
        item
        for item in value["required_assertions"]
        if item["assertion_key"] == "comp-fy2025-q4-release-apac"
    )
    row["dimension_alias"] = "EMEA"
    with pytest.raises(MappingError, match="disagrees with dimension alias"):
        _build(_write(tmp_path, value))


def test_source_publisher_must_be_declared_by_ticker_profile(tmp_path: Path) -> None:
    value = _raw()
    value["documents"][0]["publisher_id"] = "unreviewed-publisher"
    with pytest.raises(SourceContractError, match="profile publisher"):
        load_source_set(_write(tmp_path, value))


def test_source_native_mapping_keeps_emea_and_apac_zero() -> None:
    result = _build()
    candidate_by_key = {row.assertion_key: row for row in result.candidates}
    assert candidate_by_key["comp-fy2025-q4-release-emea"].value == {
        "kind": "exact",
        "value": "-3",
    }
    assert candidate_by_key["comp-fy2025-q4-release-apac"].value == {
        "kind": "exact",
        "value": "0",
    }


def test_periods_use_named_inclusive_duration_rules() -> None:
    source_set = load_source_set(FIXTURE)
    discovered = discover_sources(source_set, SOURCE_ROOT)
    extracted = _extract(source_set, discovered)
    occurrences = _evidence_occurrences(source_set, discovered, extracted)
    evidence = {
        row["occurrence_key"]: (
            row,
            next(item for item in extracted if item.assertion_key == row["occurrence_key"]),
        )
        for row in occurrences
    }
    periods = reconcile_periods(
        source_set,
        evidence,
        calendar_id=str(source_set.profile["calendar_id"]),
    )
    by_id = {row["period_id"]: row for row in periods}
    assert by_id["period:anf:fy2024-q4@1"]["start_date"] == "2024-11-03"
    assert by_id["period:anf:fy2025-q3@1"]["start_date"] == "2025-08-03"
    assert by_id["period:anf:fy2025-q4@1"]["start_date"] == "2025-11-02"
    assert by_id["period:anf:fy2025@1"]["day_count"] == 364


@pytest.mark.parametrize(
    ("period_key", "field", "value"),
    [
        ("fy2025-q3", "fiscal_ordinal", 102),
        ("fy2024-q4", "fiscal_quarter", 3),
        ("fy2024-q4", "is_53_week_year", True),
    ],
)
def test_nonadjacent_wrong_quarter_and_unsafe_53_week_changes_fail(
    tmp_path: Path, period_key: str, field: str, value: object
) -> None:
    raw = _raw()
    period = next(row for row in raw["periods"] if row["period_key"] == period_key)
    period[field] = value
    with pytest.raises((RetailSemanticError, SourceAdapterError, ValueError)):
        _build(_write(tmp_path, raw))


def test_no_filename_or_month_shift_period_inference_exists() -> None:
    period_source = (REPO / "pbi_xbrl" / "longitudinal_memory" / "source_adapter" / "periods.py").read_text(encoding="utf-8")
    text_source = (REPO / "pbi_xbrl" / "longitudinal_memory" / "source_adapter" / "text.py").read_text(encoding="utf-8")
    combined = (period_source + text_source).casefold()
    assert ".stem" not in combined
    assert "month-shift" not in combined
    assert "mtime" not in combined


def test_transcript_guidance_without_reviewed_event_link_fails(tmp_path: Path) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "guidance-fy2026-revenue-transcript"
    )
    row["required_reviewed_link_key"] = None
    with pytest.raises(MappingError, match="same-event link"):
        _build(_write(tmp_path, raw))


def test_relative_transcript_event_without_reviewed_date_link_fails(tmp_path: Path) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "event-merchandising-erp"
    )
    row["required_reviewed_link_key"] = None
    with pytest.raises(MappingError, match="reviewed date link"):
        _build(_write(tmp_path, raw))


def test_implicit_guidance_supersession_from_chronology_is_blocked(tmp_path: Path) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "guidance-fy2025-revenue-may"
    )
    row["version_kind"] = "origin"
    row["supersedes_assertion_key"] = None
    row["replacement_evidence_kind"] = None
    with pytest.raises(MappingError, match="chronology cannot supersede"):
        _build(_write(tmp_path, raw))


def test_replacement_without_explicit_wording_fails(tmp_path: Path) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "guidance-fy2025-revenue-jan"
    )
    row["replacement_evidence_kind"] = "explicit-replaces-wording"
    with pytest.raises(MappingError, match="explicit replacement wording"):
        _build(_write(tmp_path, raw))


def test_replacement_cannot_name_multiple_predecessors(tmp_path: Path) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "guidance-fy2025-revenue-may"
    )
    row["supersedes_assertion_key"] = [
        "guidance-fy2025-revenue-mar",
        "guidance-fy2025-margin-mar",
    ]
    with pytest.raises(Exception, match="schema validation"):
        _build(_write(tmp_path, raw))


def test_ambiguous_promise_origin_fails_instead_of_matching_speculatively(
    tmp_path: Path,
) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "promise-store-plan-may"
    )
    row["change_kind"] = "origin"
    row["version_state"] = "active"
    row["previous_assertion_key"] = None
    with pytest.raises(MappingError, match="multiple compatible origins"):
        _build(_write(tmp_path, raw))


def test_statement_event_and_model_interpretation_remain_separate_record_types() -> None:
    result = _build()
    types = {row["header"]["record_type"] for row in result.package["observations"]}
    assert {"ManagementStatement", "CompanyEvent", "ModelInterpretation", "NumericalFact"} <= types
    model = next(
        row for row in result.package["observations"] if row["payload"]["kind"] == "ModelInterpretation"
    )
    assert model["header"]["assertion_mode"] == "interpreted"
    assert model["header"]["knowledge_date"] == "2026-07-29"


def test_management_explanation_cannot_be_substituted_for_reported_fact(
    tmp_path: Path,
) -> None:
    raw = _raw()
    row = next(
        item
        for item in raw["required_assertions"]
        if item["assertion_key"] == "management-q4-margin-bridge"
    )
    row.clear()
    row.update(
        {
            "assertion_key": "management-q4-margin-bridge",
            "assertion_kind": "numerical_fact",
            "document_key": "anf-transcript-2026-03-04",
            "metric_key": "comparable-sales",
            "period_key": "fy2025-q4",
            "dimension_alias": "total company",
            "value_parser_id": "parser:retail:percent-text@1",
            "locator": next(
                item["locator"]
                for item in _raw()["required_assertions"]
                if item["assertion_key"] == "management-q4-margin-bridge"
            ),
            "review_state": "accepted",
        }
    )
    with pytest.raises(SourceContractError, match="assertion policy"):
        _build(_write(tmp_path, raw))


def test_approximate_store_target_is_not_auto_achieved() -> None:
    result = _build()
    issue = next(
        row
        for row in result.package["review_issues"]
        if row["rule_id"] == "promise_approximate_tolerance_missing"
    )
    assert issue["severity"] == "P2"
    assert issue["review_state"] == "needs_review"
    promise_versions = [
        row
        for row in result.package["observations"]
        if row["payload"]["kind"] == "PromiseVersion"
    ]
    assert all(row["payload"]["version_state"] != "achieved" for row in promise_versions)
