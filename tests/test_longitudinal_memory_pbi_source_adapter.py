from __future__ import annotations

import copy
import hashlib
import json
import random
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.calendar_rules import CALENDAR_YEAR_RULE_ID
from pbi_xbrl.longitudinal_memory.sector_packs.business_services import (
    BUSINESS_SERVICES_SECTOR_PACK,
)
from pbi_xbrl.longitudinal_memory.source_adapter.builder import (
    _validate_promise_version_source_coherence,
    build_source_native_sidecar,
)
from pbi_xbrl.longitudinal_memory.source_adapter.types import MappingError, SourceAdapterError
from pbi_xbrl.longitudinal_memory.ticker_profiles.pbi import load_pbi_profile
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.validation import (
    validate_package,
    validate_package_schema,
)


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "pbi_source_set.v1.json"
EXPECTED = REPO / "tests" / "fixtures" / "longitudinal_memory" / "pbi_source_adapter_expected.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
GOLDEN_SHA = "da8577e389be383aeec80f481c0889acf62c38edf604e25f62df736cf89c34a6"
PROMISE_WORDING = "Annualized savings objective under the cost rationalization program."


def _build(path: Path = FIXTURE):
    return build_source_native_sidecar(
        path,
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=BUSINESS_SERVICES_SECTOR_PACK,
        ticker_profile_loader=load_pbi_profile,
    )


def _write(tmp_path: Path, raw: dict, name: str = "source-set.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(raw), encoding="utf-8", newline="\n")
    return path


def _strict_json_bytes(path: Path) -> dict:
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r} in {path}")
            result[key] = value
        return result

    return json.loads(path.read_bytes(), object_pairs_hook=object_pairs)


@pytest.fixture(scope="module")
def result():
    return _build()


def _observations(package: dict, kind: str) -> list[dict]:
    return [row for row in package["observations"] if row["payload"]["kind"] == kind]


def _member(package: dict, token: str) -> str:
    matches = [
        row["member_id"]
        for row in package["catalog"]["dimension_members"]
        if token.casefold() in row["display_name"].casefold()
        or any(token.casefold() in alias.casefold() for alias in row["aliases"])
    ]
    assert len(set(matches)) == 1
    return matches[0]


def _dimension_set(package: dict, member_id: str) -> str:
    matches = [
        row["dimension_set_id"]
        for row in package["catalog"]["dimension_sets"]
        if any(member["member_id"] == member_id for member in row["members"])
    ]
    assert len(matches) == 1
    return matches[0]


def _fact(package: dict, metric: str, period: str, dimension: str, definition: str | None = None) -> dict:
    matches = [
        row
        for row in _observations(package, "NumericalFact")
        if row["payload"]["metric_id"] == metric
        and row["header"]["effective_period_id"] == period
        and row["header"]["dimension_set_id"] == dimension
        and (definition is None or row["payload"]["definition_id"] == definition)
    ]
    assert len(matches) == 1
    return matches[0]


def test_full_pbi_build_projects_and_validates_unchanged_c1(result) -> None:
    package = result.package
    validate_package_schema(package)
    assert validate_package(package) == []
    assert package["artifact_state"] == "accepted"
    assert len(package["fiscal_calendars"]) == 1
    assert len(package["periods"]) == 8
    assert len(package["source_documents"]) == 18
    assert len(package["evidence_occurrences"]) == 65
    assert len(package["entities"]) == 4
    assert len(package["observations"]) == 66
    assert len(package["relations"]) == 13
    assert len(package["resolutions"]) == 49
    assert len(package["review_issues"]) == 11
    assert all(not row["promotion_blocking"] for row in package["review_issues"])


def test_pbi_golden_bytes_and_sha_are_exact(result) -> None:
    expected_object = _strict_json_bytes(EXPECTED)
    expected = serialize_package(expected_object)
    assert json.loads(result.payload) == expected_object
    assert result.payload == expected
    assert result.sidecar_sha256 == GOLDEN_SHA
    assert hashlib.sha256(expected).hexdigest() == GOLDEN_SHA


def test_pbi_semantic_golden_rejects_real_json_mutation(result) -> None:
    mutated = _strict_json_bytes(EXPECTED)
    mutated["artifact_state"] = "needs-review"
    mutated_payload = serialize_package(mutated)
    assert mutated_payload != result.payload
    assert hashlib.sha256(mutated_payload).hexdigest() != GOLDEN_SHA


def test_canonical_calendar_year_rule_and_natural_quarter_lengths(result) -> None:
    package = result.package
    calendar = package["fiscal_calendars"][0]
    assert calendar["calendar_id"] == "calendar:pbi:calendar-year@1"
    assert calendar["calendar_rule_id"] == CALENDAR_YEAR_RULE_ID
    assert calendar["week_pattern"] == "calendar"
    assert calendar["reconciliation_state"] == "reconciled"
    assert len(calendar["evidence_occurrence_ids"]) == 8
    periods = {row["period_id"]: row for row in package["periods"]}
    assert periods["period:pbi:cy2026-q1@1"]["day_count"] == 90
    assert periods["period:pbi:cy2026-q2@1"]["day_count"] == 91
    assert periods["period:pbi:cy2026-q1@1"]["week_count"] is None
    assert periods["period:pbi:cy2026-q2@1"]["week_count"] is None


def test_segment_revenue_and_adjusted_ebit_are_source_native(result) -> None:
    package = result.package
    sendtech = _dimension_set(package, _member(package, "SendTech"))
    presort = _dimension_set(package, _member(package, "Presort"))
    values = {
        ("sendtech", "q1", "revenue"): _fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q1@1", sendtech)["payload"]["value"]["value"],
        ("sendtech", "q2", "revenue"): _fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q2@1", sendtech)["payload"]["value"]["value"],
        ("presort", "q1", "revenue"): _fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q1@1", presort)["payload"]["value"]["value"],
        ("presort", "q2", "revenue"): _fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q2@1", presort)["payload"]["value"]["value"],
        ("sendtech", "q1", "ebit"): _fact(package, "metric:business-services:adjusted-segment-ebit@1", "period:pbi:cy2026-q1@1", sendtech)["payload"]["value"]["value"],
        ("sendtech", "q2", "ebit"): _fact(package, "metric:business-services:adjusted-segment-ebit@1", "period:pbi:cy2026-q2@1", sendtech)["payload"]["value"]["value"],
        ("presort", "q1", "ebit"): _fact(package, "metric:business-services:adjusted-segment-ebit@1", "period:pbi:cy2026-q1@1", presort)["payload"]["value"]["value"],
        ("presort", "q2", "ebit"): _fact(package, "metric:business-services:adjusted-segment-ebit@1", "period:pbi:cy2026-q2@1", presort)["payload"]["value"]["value"],
    }
    assert values == {
        ("sendtech", "q1", "revenue"): "313.947",
        ("sendtech", "q2", "revenue"): "308.93",
        ("presort", "q1", "revenue"): "163.466",
        ("presort", "q2", "revenue"): "142.568",
        ("sendtech", "q1", "ebit"): "113.53",
        ("sendtech", "q2", "ebit"): "122.678",
        ("presort", "q1", "ebit"): "39.178",
        ("presort", "q2", "ebit"): "20.006",
    }


def test_derived_margins_and_pbi_style_three_point_change(result) -> None:
    package = result.package
    changes = _observations(package, "ChangeObservation")
    assert len(changes) == 6
    by_kind_value = Counter(
        (row["payload"]["change_kind"], row["payload"]["value"]["value"])
        for row in changes
    )
    assert by_kind_value[("qoq-percentage-point", "3")] == 1
    assert by_kind_value[("qoq-percentage-point", "0")] == 1
    assert by_kind_value[("yoy-percentage-point", "7")] == 1
    assert by_kind_value[("yoy-percentage-point", "-7")] == 1
    assert by_kind_value[("qoq-percentage-point", "3.548459296358155755071212")] == 1
    assert by_kind_value[("qoq-percentage-point", "-9.93446150078783401421540073")] == 1
    presort_qoq = next(
        row
        for row in changes
        if row["payload"]["change_kind"] == "qoq-percentage-point"
        and row["payload"]["value"]["value"] == "3"
    )
    assert presort_qoq["payload"]["comparability"]["checks"]["same_duration"] is False
    assert "calendar-year" in presort_qoq["payload"]["comparability"]["reason"].casefold()


def test_pieces_volume_fuel_and_definition_break_are_preserved(result) -> None:
    package = result.package
    facts = _observations(package, "NumericalFact")
    values = {
        (row["payload"]["metric_id"], row["payload"]["definition_id"]): row["payload"]["value"]["value"]
        for row in facts
        if row["payload"]["metric_id"] in {
            "metric:business-services:pieces-processed@1",
            "metric:business-services:volume-growth@1",
            "metric:business-services:transport-cost-pressure@1",
        }
    }
    assert values[("metric:business-services:pieces-processed@1", "definition:business-services:company-reported-pieces@1")] == "3.3"
    assert values[("metric:business-services:volume-growth@1", "definition:core:company-reported-rounded-rate@1")] == "-3"
    assert values[("metric:business-services:transport-cost-pressure@1", "definition:business-services:company-quantified-fuel-headwind@1")] == "6"
    fuel = next(
        row
        for row in facts
        if row["payload"]["definition_id"]
        == "definition:business-services:company-quantified-fuel-headwind@1"
    )
    assert fuel["payload"]["value"] == {
        "kind": "approximate",
        "value": "6",
        "qualifier": "around",
        "tolerance": None,
    }
    assert fuel["payload"]["currency"] == "USD"
    assert fuel["payload"]["unit_id"] == "unit:core:usd-millions@1"
    occurrence_id = fuel["header"]["evidence_occurrence_ids"][0]
    occurrence = next(
        row for row in package["evidence_occurrences"]
        if row["evidence_occurrence_id"] == occurrence_id
    )
    document = next(
        row for row in package["source_documents"]
        if row["source_document_id"] == occurrence["source_document_id"]
    )
    assert "around $6 million" in occurrence["excerpt"]
    assert occurrence["occurrence_key"] == "fuel-headwind-q2-2026"
    assert document["document_key"] == "q2-2026-transcript"
    assert "metadata" not in occurrence_id
    assert not any(
        row["payload"].get("definition_id")
        == "definition:business-services:company-quantified-fuel-headwind@1"
        and row["payload"]["value"]["kind"] == "exact"
        for row in facts
    )
    by_id = {row["header"]["record_id"]: row for row in package["observations"]}
    for change in _observations(package, "ChangeObservation"):
        inputs = [by_id[identity] for identity in change["payload"]["input_record_ids"]]
        assert all(
            row["payload"].get("definition_id")
            != "definition:business-services:adjusted-segment-ebit-pre-2026@1"
            for row in inputs
        )
    assert any(row["rule_id"] == "adjusted_ebit_definition_break" for row in package["review_issues"])


def test_cost_savings_promise_has_one_origin_one_active_version_and_no_deadline(result) -> None:
    versions = _observations(result.package, "PromiseVersion")
    assert len(versions) == 6
    assert Counter(row["payload"]["change_kind"] for row in versions) == {
        "origin": 1,
        "reaffirmation": 1,
        "target_update": 4,
    }
    assert [row["payload"]["target"] for row in versions if row["payload"]["change_kind"] == "origin"] == [
        {"kind": "range", "low": "60", "high": "100", "low_inclusive": True, "high_inclusive": True}
    ]
    active = [row for row in versions if row["payload"]["version_state"] == "active"]
    assert len(active) == 1
    assert active[0]["payload"]["target"] == {
        "kind": "range", "low": "180", "high": "200", "low_inclusive": True, "high_inclusive": True
    }
    assert all(row["payload"]["deadline"] is None for row in versions)
    assert any(row["rule_id"] == "promise_run_rate_not_realized_savings" for row in result.package["review_issues"])


def test_promise_wording_targets_and_evidence_are_coherent(result) -> None:
    package = result.package
    versions = _observations(package, "PromiseVersion")
    occurrences = {
        row["evidence_occurrence_id"]: row for row in package["evidence_occurrences"]
    }
    assertions = {
        row["assertion_key"]: row
        for row in json.loads(FIXTURE.read_text(encoding="utf-8"))["required_assertions"]
        if row["assertion_kind"] == "promise_version"
    }
    by_assertion = {
        occurrences[row["header"]["evidence_occurrence_ids"][0]]["occurrence_key"]: row
        for row in versions
    }
    assert set(by_assertion) == set(assertions)
    assert {row["payload"]["wording"] for row in versions} == {PROMISE_WORDING}
    assert not re.search(r"[0-9$€£]", PROMISE_WORDING)
    promise = next(row for row in package["entities"] if row["payload"]["kind"] == "Promise")
    assert promise["payload"]["original_wording"] == PROMISE_WORDING

    for assertion_key, record in sorted(by_assertion.items()):
        assertion = assertions[assertion_key]
        value_text = assertion["locator"]["value_text_fingerprint"]
        assert record["payload"]["target"] == BUSINESS_SERVICES_SECTOR_PACK.parse_value(
            assertion["value_parser_id"],
            value_text,
        )
        occurrence = occurrences[record["header"]["evidence_occurrence_ids"][0]]
        assert occurrence["occurrence_key"] == assertion_key
        assert value_text in occurrence["excerpt"]

    assert sum(row["payload"]["change_kind"] == "origin" for row in versions) == 1
    assert by_assertion["promise-cost-july-update"]["payload"]["change_kind"] == "target_update"
    assert by_assertion["promise-cost-q2-2024-reaffirmation"]["payload"]["change_kind"] == "reaffirmation"
    assert all(
        by_assertion[key]["payload"]["change_kind"] == "target_update"
        for key in (
            "promise-cost-q3-2024-update",
            "promise-cost-q4-2024-update",
            "promise-cost-q1-2025-update",
        )
    )
    assert all(row["payload"]["deadline"] is None for row in versions)
    assert {row["payload"]["version_state"] for row in versions} <= {
        "active",
        "reaffirmed",
        "superseded",
    }
    assert any(
        row["rule_id"] == "promise_run_rate_not_realized_savings"
        for row in package["review_issues"]
    )
    assert any(
        row["rule_id"] == "gross_net_savings_bridge_missing"
        for row in package["review_issues"]
    )


def test_promise_target_and_wording_mutations_fail_source_coherence(result) -> None:
    package = result.package
    occurrences_by_id = {
        row["evidence_occurrence_id"]: row for row in package["evidence_occurrences"]
    }
    records = {
        occurrences_by_id[row["header"]["evidence_occurrence_ids"][0]]["occurrence_key"]: row
        for row in _observations(package, "PromiseVersion")
    }
    candidates = {
        row.assertion_key: row
        for row in result.candidates
        if row.candidate_kind == "promise_version"
    }
    occurrences = {
        row["occurrence_key"]: row
        for row in package["evidence_occurrences"]
        if row["occurrence_key"] in candidates
    }
    expected_wordings = {key: PROMISE_WORDING for key in candidates}

    mutated_target = copy.deepcopy(records)
    mutated_target["promise-cost-q1-2025-update"]["payload"]["target"]["low"] = "181"
    with pytest.raises(MappingError, match="differs from its source-derived candidate"):
        _validate_promise_version_source_coherence(
            mutated_target,
            candidates,
            occurrences,
            expected_wordings,
        )

    mutated_wording = copy.deepcopy(records)
    mutated_wording["promise-cost-q1-2025-update"]["payload"]["wording"] = (
        occurrences["promise-cost-origin"]["excerpt"]
    )
    with pytest.raises(MappingError, match="differs from its reviewed normalized wording"):
        _validate_promise_version_source_coherence(
            mutated_wording,
            candidates,
            occurrences,
            expected_wordings,
        )


def test_guidance_series_keep_definition_and_explicit_history_separate(result) -> None:
    versions = _observations(result.package, "GuidanceVersion")
    assert len(versions) == 8
    series = Counter(row["payload"]["guidance_series_id"] for row in versions)
    assert sorted(series.values()) == [1, 3, 4]
    assert Counter(row["payload"]["version_kind"] for row in versions) == {
        "origin": 3,
        "reaffirmation": 3,
        "replacement": 2,
    }
    ambiguous = [
        row for row in versions
        if "pension-treatment-ambiguous" in row["payload"]["guidance_series_id"]
    ]
    assert len(ambiguous) == 1
    supersedes = [row for row in result.package["relations"] if row["relation_type"] == "supersedes"]
    assert all(row["rule_id"] != "chronology" for row in supersedes)


def test_transcript_statements_event_and_no_model_interpretation(result) -> None:
    package = result.package
    assert len(_observations(package, "ManagementStatement")) == 15
    events = _observations(package, "CompanyEvent")
    assert len(events) == 1
    assert events[0]["payload"]["event_type"] == "debt-refinancing"
    assert not _observations(package, "ModelInterpretation")
    assert any(row["rule_id"] == "model_interpretation_not_reviewed" for row in package["review_issues"])
    transcript_statements = [
        row for row in _observations(package, "ManagementStatement")
        if "q2-2026-transcript" in row["header"]["evidence_occurrence_ids"][0]
    ]
    assert transcript_statements
    assert all("analyst" not in row["payload"]["speaker_id"] for row in transcript_statements)


@pytest.mark.parametrize("seed", [7, 2026])
def test_reverse_and_seeded_shuffle_are_byte_identical(tmp_path: Path, result, seed: int) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if seed == 7:
        for key in ("documents", "required_assertions", "periods", "reviewed_links", "review_issue_specs"):
            raw[key].reverse()
        raw["profile"]["member_aliases"].reverse()
        raw["profile"]["activated_semantic_binding_ids"].reverse()
    else:
        rng = random.Random(seed)
        for key in ("documents", "required_assertions", "periods", "reviewed_links", "review_issue_specs"):
            rng.shuffle(raw[key])
        rng.shuffle(raw["profile"]["member_aliases"])
        rng.shuffle(raw["profile"]["activated_semantic_binding_ids"])
    assert _build(_write(tmp_path, raw, f"shuffled-{seed}.json")).payload == result.payload


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("q1_start", "non-calendar source boundaries"),
        ("q2_ordinal", "fiscal ordinal"),
        ("calendar_rule", "calendar"),
        ("promise_program", "compatible subject"),
        ("metadata_transcript_hash", "transcript SHA-256"),
        ("fuel_qualifier", "line text changed"),
    ],
)
def test_full_build_semantic_mutations_fail(
    tmp_path: Path, mutation: str, expected: str
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if mutation == "q1_start":
        next(row for row in raw["periods"] if row["period_key"] == "cy2026-q1")["start_date"] = "2026-01-02"
    elif mutation == "q2_ordinal":
        next(row for row in raw["periods"] if row["period_key"] == "cy2026-q2")["fiscal_ordinal"] = 107
    elif mutation == "calendar_rule":
        raw["profile"]["reviewed_calendar_rule"]["rule_id"] = "rule:core:unknown@1"
    elif mutation == "promise_program":
        next(row for row in raw["required_assertions"] if row["assertion_key"] == "promise-cost-q2-2024-reaffirmation")["program_id"] = "program:pbi:other@1"
    elif mutation == "metadata_transcript_hash":
        next(row for row in raw["documents"] if row["document_key"] == "q2-2026-transcript-metadata-v2")["role_metadata"]["transcript_sha256"] = "0" * 64
    elif mutation == "fuel_qualifier":
        assertion = next(
            row for row in raw["required_assertions"]
            if row["assertion_key"] == "fuel-headwind-q2-2026"
        )
        assertion["locator"]["excerpt"] = assertion["locator"]["excerpt"].replace(
            "around ",
            "",
        )
    with pytest.raises(SourceAdapterError, match=expected):
        _build(_write(tmp_path, raw, f"{mutation}.json"))


def test_source_backed_segment_deltas_can_be_computed_without_opaque_score(result) -> None:
    package = result.package
    sendtech = _dimension_set(package, _member(package, "SendTech"))
    presort = _dimension_set(package, _member(package, "Presort"))
    sendtech_q1 = Decimal(_fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q1@1", sendtech)["payload"]["value"]["value"])
    sendtech_q2 = Decimal(_fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q2@1", sendtech)["payload"]["value"]["value"])
    presort_q1 = Decimal(_fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q1@1", presort)["payload"]["value"]["value"])
    presort_q2 = Decimal(_fact(package, "metric:core:revenue@1", "period:pbi:cy2026-q2@1", presort)["payload"]["value"]["value"])
    assert (sendtech_q2 / sendtech_q1 - 1) * 100 == Decimal("-1.598040433576367985679111440")
    assert (presort_q2 / presort_q1 - 1) * 100 == Decimal("-12.78430988707131758286126779")
