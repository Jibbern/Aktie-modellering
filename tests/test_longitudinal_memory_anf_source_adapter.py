from __future__ import annotations

import ast
import copy
import hashlib
import io
import json
import random
import shutil
from collections import Counter
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.calendar_rules import (
    SOURCE_LABELLED_52_53_WEEK_RULE_ID,
)
from pbi_xbrl.longitudinal_memory.sector_packs.retail import (
    RETAIL_SECTOR_PACK,
    RetailSemanticError,
)
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
import pbi_xbrl.longitudinal_memory.source_adapter.builder as builder_module
import pbi_xbrl.longitudinal_memory.source_adapter.spreadsheet as spreadsheet_module
from pbi_xbrl.longitudinal_memory.source_adapter.builder import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.source_adapter.types import (
    LocatorError,
    MappingError,
    SourceContractError,
    SourceDiscoveryError,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile
from pbi_xbrl.longitudinal_memory.validation import validate_package, validate_package_schema


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
EXPECTED = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_adapter_expected.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")


def _build(path: Path = FIXTURE):
    return build_source_native_sidecar(
        path,
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    )


def _build_with_root(path: Path, source_root: Path):
    return build_source_native_sidecar(
        path,
        source_root=source_root,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    )


def _write_source_set(tmp_path: Path, value: dict, name: str = "source-set.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8", newline="\n")
    return path


def _copy_declared_sources(tmp_path: Path) -> Path:
    root = tmp_path / "source-root"
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for document in raw["documents"]:
        relative = Path(str(document["relative_path"]).replace("\\", "/"))
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE_ROOT / relative, destination)
    return root


@pytest.fixture(scope="module")
def result():
    return _build()


@pytest.fixture(scope="module")
def expected() -> dict:
    return json.loads(EXPECTED.read_text(encoding="utf-8"))


def _selected(package: dict) -> list[dict]:
    selected_ids = {
        row["selected_record_id"]
        for row in package["resolutions"]
        if row["selected_record_id"] is not None
    }
    return [
        row for row in package["observations"] if row["header"]["record_id"] in selected_ids
    ]


def _dimension_set_for_member(package: dict, member_id: str) -> str:
    matches = [
        row
        for row in package["catalog"]["dimension_sets"]
        if any(member["member_id"] == member_id for member in row["members"])
    ]
    if len(matches) > 1:
        matches = [row for row in matches if len(row["members"]) == 1]
    assert len(matches) == 1
    return matches[0]["dimension_set_id"]


def _member_id(package: dict, display_name: str) -> str:
    matches = [
        row["member_id"]
        for row in package["catalog"]["dimension_members"]
        if row["display_name"].casefold() == display_name.casefold()
        or display_name in row["aliases"]
    ]
    assert len(set(matches)) == 1
    return matches[0]


def _selected_fact(
    package: dict, *, metric_id: str, period_id: str, dimension_set_id: str
) -> dict:
    matches = [
        row
        for row in _selected(package)
        if row["payload"]["kind"] == "NumericalFact"
        and row["payload"]["metric_id"] == metric_id
        and row["header"]["effective_period_id"] == period_id
        and row["header"]["dimension_set_id"] == dimension_set_id
    ]
    assert len(matches) == 1
    return matches[0]


def _period_spec(raw: dict, period_key: str) -> dict:
    return next(row for row in raw["periods"] if row["period_key"] == period_key)


def _period_evidence_assertion(raw: dict, period_key: str) -> dict:
    assertion_key = _period_spec(raw, period_key)["evidence_assertion_key"]
    return next(
        row for row in raw["required_assertions"] if row["assertion_key"] == assertion_key
    )


def _assertion(raw: dict, assertion_key: str) -> dict:
    return next(
        row for row in raw["required_assertions"] if row["assertion_key"] == assertion_key
    )


def _fiscal_claim(
    claim_key: str,
    claim_kind: str,
    text: str,
    *,
    ordinal: int = 1,
) -> dict:
    return {
        "claim_key": claim_key,
        "claim_kind": claim_kind,
        "text_fingerprint": text,
        "match_ordinal": ordinal,
        "excerpt_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def _attach_fiscal_claims(assertion: dict, claims: list[dict]) -> None:
    assertion["locator"]["fiscal_label_evidence"] = {
        "locator_kind": "html-fiscal-labels",
        "locator_version": 1,
        "extraction_method_id": "extractor:source:html-fiscal-label@1",
        "claims": claims,
    }


def test_full_external_integration_passes_unchanged_c1_validation(result) -> None:
    assert validate_package_schema(result.package) == []
    assert validate_package(result.package) == []
    assert result.package["artifact_state"] == "accepted"


@pytest.mark.parametrize(
    "change_kind", ["qoq-percentage-point", "yoy-percentage-point"]
)
def test_retail_change_producer_uses_canonical_year_classification_rule(
    result, change_kind
) -> None:
    package = copy.deepcopy(result.package)
    change = next(
        row
        for row in package["observations"]
        if row["payload"].get("change_kind") == change_kind
    )
    observations = {
        row["header"]["record_id"]: row for row in package["observations"]
    }
    periods = {row["period_id"]: row for row in package["periods"]}
    earlier_period_id = observations[change["payload"]["from_record_id"]]["header"][
        "fiscal_period_id"
    ]
    periods[earlier_period_id]["is_53_week_year"] = True
    selected_numerical = {
        (
            row["payload"]["metric_id"],
            row["header"]["effective_period_id"],
            row["header"]["dimension_set_id"],
        ): row
        for row in _selected(package)
        if row["payload"]["kind"] == "NumericalFact"
    }

    with pytest.raises(
        RetailSemanticError,
        match="Source-labelled fiscal-year-length classification differs",
    ):
        RETAIL_SECTOR_PACK.percentage_point_change_requests(
            package["periods"],
            selected_numerical,
            total_dimension_id=change["header"]["dimension_set_id"],
            calendar=package["fiscal_calendars"][0],
        )


def test_verified_snapshot_survives_file_replacement_after_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expected: dict
) -> None:
    source_root = _copy_declared_sources(tmp_path)
    real_discover = builder_module.discover_sources

    def discover_then_replace(source_set, injected_root):
        discovered = real_discover(source_set, injected_root)
        target = next(
            row for row in discovered if row.spec.document_key == "anf-release-2026-03-04"
        )
        target.absolute_path.write_bytes(b"post-verification replacement")
        return discovered

    monkeypatch.setattr(builder_module, "discover_sources", discover_then_replace)
    built = _build_with_root(FIXTURE, source_root)
    assert built.sidecar_sha256 == expected["serialization_sha256"]


def test_extractors_never_reopen_verified_source_paths(
    monkeypatch: pytest.MonkeyPatch, expected: dict
) -> None:
    real_discover = builder_module.discover_sources
    original_open = Path.open

    def discover_then_block_paths(source_set, injected_root):
        discovered = real_discover(source_set, injected_root)
        blocked = {row.absolute_path for row in discovered}

        def guarded_open(path: Path, *args, **kwargs):
            if path in blocked:
                raise AssertionError("extractor reopened a verified source path")
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(Path, "open", guarded_open)
        return discovered

    monkeypatch.setattr(builder_module, "discover_sources", discover_then_block_paths)
    built = _build()
    assert built.sidecar_sha256 == expected["serialization_sha256"]


def test_xlsx_formula_and_cached_views_reject_different_snapshot_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_bytes_io = io.BytesIO
    calls = 0

    def divergent_stream(value: bytes):
        nonlocal calls
        calls += 1
        return real_bytes_io(value if calls != 2 else b"different cached-view bytes")

    monkeypatch.setattr(spreadsheet_module, "BytesIO", divergent_stream)
    with pytest.raises(LocatorError, match="XLSX byte snapshot changed"):
        _build()


def test_source_document_evidence_and_locator_inventory_matches_golden(
    result, expected: dict
) -> None:
    counts = expected["counts"]
    assert len(result.documents) == counts["external_source_documents"]
    assert len(result.package["source_documents"]) == counts["all_source_documents"]
    assert len(result.extracted_evidence) == counts["external_evidence_occurrences"]
    assert len(result.package["evidence_occurrences"]) == counts["all_evidence_occurrences"]
    assert Counter(row.locator_kind for row in result.extracted_evidence) == expected[
        "locator_counts"
    ]
    assert {row.spec.document_key: row.content_sha256 for row in result.documents} == expected[
        "external_source_sha256"
    ]


def test_publication_basis_and_spreadsheet_needs_review_are_preserved(result) -> None:
    specs = {row.spec.document_key: row.spec for row in result.documents}
    assert specs["anf-release-2026-03-04"].publication_date_basis == "sec-filed-date"
    assert specs["anf-business-update-2026-01-12"].publication_date_basis == "embedded-dateline"
    assert specs["anf-transcript-2026-03-04"].publication_date_basis == "reviewed-same-event-link"
    assert specs["anf-q4-2025-history"].review_state == "needs_review"

    package = result.package
    transcript_document_id = next(
        row["source_document_id"]
        for row in package["source_documents"]
        if row["document_key"] == "anf-transcript-2026-03-04"
    )
    transcript_occurrences = {
        row["evidence_occurrence_id"]
        for row in package["evidence_occurrences"]
        if row["source_document_id"] == transcript_document_id
    }
    assert {
        row["header"]["knowledge_date"]
        for row in package["observations"]
        if set(row["header"]["evidence_occurrence_ids"]) & transcript_occurrences
    } == {"2026-07-29"}

    spreadsheet_document_id = next(
        row["source_document_id"]
        for row in package["source_documents"]
        if row["document_key"] == "anf-q4-2025-history"
    )
    spreadsheet_occurrences = {
        row["evidence_occurrence_id"]
        for row in package["evidence_occurrences"]
        if row["source_document_id"] == spreadsheet_document_id
    }
    assert spreadsheet_occurrences
    assert all(
        row["review_state"] == "needs_review"
        for row in package["evidence_occurrences"]
        if row["evidence_occurrence_id"] in spreadsheet_occurrences
    )
    selected_ids = {
        row["selected_record_id"]
        for row in package["resolutions"]
        if row["selected_record_id"] is not None
    }
    assert all(
        not (set(row["header"]["evidence_occurrence_ids"]) & spreadsheet_occurrences)
        for row in package["observations"]
        if row["header"]["record_id"] in selected_ids
    )


def test_comparable_sales_dimensions_and_changes_match_source_golden(
    result, expected: dict
) -> None:
    package = result.package
    metric = "metric:retail:comparable-sales@1"
    total = _dimension_set_for_member(
        package, _member_id(package, "total company")
    )
    expected_values = expected["comparable_sales"]
    cases = [
        ("fy2024_q4_total_company", "period:anf:fy2024-q4@1", total),
        ("fy2025_q3_total_company", "period:anf:fy2025-q3@1", total),
        ("fy2025_q4_total_company", "period:anf:fy2025-q4@1", total),
        ("fy2025_q4_apac", "period:anf:fy2025-q4@1", _dimension_set_for_member(package, _member_id(package, "APAC"))),
        ("fy2025_q4_emea", "period:anf:fy2025-q4@1", _dimension_set_for_member(package, _member_id(package, "EMEA"))),
        ("fy2025_q4_abercrombie", "period:anf:fy2025-q4@1", _dimension_set_for_member(package, _member_id(package, "Abercrombie"))),
        ("fy2025_q4_hollister", "period:anf:fy2025-q4@1", _dimension_set_for_member(package, _member_id(package, "Hollister"))),
    ]
    for key, period, dimensions in cases:
        assert _selected_fact(
            package, metric_id=metric, period_id=period, dimension_set_id=dimensions
        )["payload"]["value"] == expected_values[key]
    changes = {
        row["payload"]["change_kind"]: row["payload"]["value"]
        for row in package["observations"]
        if row["payload"]["kind"] == "ChangeObservation"
    }
    assert changes == {
        "qoq-percentage-point": expected_values["qoq_percentage_point_change"],
        "yoy-percentage-point": expected_values["yoy_percentage_point_change"],
    }


def test_same_document_repetitions_are_duplicates_not_corroboration(result) -> None:
    package = result.package
    relations = package["relations"]
    assert Counter(row["relation_type"] for row in relations)["duplicate"] == 3
    occurrence_to_document = {
        row["evidence_occurrence_id"]: row["source_document_id"]
        for row in package["evidence_occurrences"]
    }
    observation_by_id = {
        row["header"]["record_id"]: row for row in package["observations"]
    }
    for relation in (row for row in relations if row["relation_type"] == "duplicate"):
        left = observation_by_id[relation["from_record_id"]]
        right = observation_by_id[relation["to_record_id"]]
        left_docs = {
            occurrence_to_document[value] for value in left["header"]["evidence_occurrence_ids"]
        }
        right_docs = {
            occurrence_to_document[value] for value in right["header"]["evidence_occurrence_ids"]
        }
        assert left_docs == right_docs


def test_store_actuals_and_promise_assessment_match_golden(result, expected: dict) -> None:
    package = result.package
    total = _dimension_set_for_member(package, _member_id(package, "total company"))
    metric_keys = {
        "openings": "metric:retail:store-openings@1",
        "closures": "metric:retail:store-closures@1",
        "net_openings": "metric:retail:net-store-openings@1",
        "ending_stores": "metric:retail:ending-stores@1",
    }
    for key, metric in metric_keys.items():
        fact = _selected_fact(
            package,
            metric_id=metric,
            period_id="period:anf:fy2025@1",
            dimension_set_id=total,
        )
        assert fact["payload"]["value"] == expected["store_evidence"][key]
        if key == "net_openings":
            assert fact["header"]["assertion_mode"] == "derived"
    assert [row["rule_id"] for row in package["review_issues"]] == [
        "promise_approximate_tolerance_missing"
    ]
    assert Counter(row["relation_type"] for row in package["relations"])["reaffirms"] == 4


def test_guidance_supersession_and_release_transcript_corroboration(
    result, expected: dict
) -> None:
    package = result.package
    relations = package["relations"]
    assert Counter(row["relation_type"] for row in relations)["supersedes"] == 8
    assert Counter(row["relation_type"] for row in relations)["corroborates"] >= 2

    observation_by_id = {
        row["header"]["record_id"]: row for row in package["observations"]
    }
    occurrence_to_source = {
        row["evidence_occurrence_id"]: row["source_document_id"]
        for row in package["evidence_occurrences"]
    }
    source_to_key = {
        row["source_document_id"]: row["document_key"] for row in package["source_documents"]
    }
    release_transcript = []
    for relation in (row for row in relations if row["relation_type"] == "corroborates"):
        endpoints = [observation_by_id[relation[key]] for key in ("from_record_id", "to_record_id")]
        if any(row["payload"]["kind"] != "GuidanceVersion" for row in endpoints):
            continue
        documents = {
            source_to_key[
                occurrence_to_source[row["header"]["evidence_occurrence_ids"][0]]
            ]
            for row in endpoints
        }
        if documents == {"anf-release-2026-03-04", "anf-transcript-2026-03-04"}:
            release_transcript.append(relation)
    assert len(release_transcript) == 2
    transcript_occurrence_ids = {
        row["evidence_occurrence_id"]
        for row in package["evidence_occurrences"]
        if source_to_key[row["source_document_id"]] == "anf-transcript-2026-03-04"
    }
    assert {
        row["header"]["knowledge_date"]
        for row in package["observations"]
        if row["payload"]["kind"] == "GuidanceVersion"
        and set(row["header"]["evidence_occurrence_ids"]) & transcript_occurrence_ids
    } == {expected["fy2026_guidance"]["transcript_knowledge_date"]}

    versions = [
        row for row in package["observations"] if row["payload"]["kind"] == "GuidanceVersion"
    ]
    series = {row["header"]["entity_id"]: row for row in package["entities"]}
    for metric, expected_rows in (
        ("metric:core:revenue-growth@1", expected["fy2025_revenue_guidance"]),
        ("metric:core:operating-margin@1", expected["fy2025_margin_guidance"]),
    ):
        rows = sorted(
            (
                row
                for row in versions
                if series[row["payload"]["guidance_series_id"]]["payload"]["metric_id"] == metric
                and series[row["payload"]["guidance_series_id"]]["payload"]["horizon_period_id"]
                == "period:anf:fy2025@1"
            ),
            key=lambda row: row["header"]["publication_date"],
        )
        assert len(rows) == len(expected_rows)
        for actual, golden in zip(rows, expected_rows, strict=True):
            assert actual["header"]["publication_date"] == golden["date"]
            for key, value in golden.items():
                if key != "date":
                    assert actual["payload"]["value"][key] == value


def test_statement_event_and_interpretation_are_separate_and_correctly_dated(
    result, expected: dict
) -> None:
    package = result.package
    statement = next(
        row for row in package["observations"] if row["payload"]["kind"] == "ManagementStatement"
    )
    event = next(
        row for row in package["observations"] if row["payload"]["kind"] == "CompanyEvent"
    )
    model = next(
        row for row in package["observations"] if row["payload"]["kind"] == "ModelInterpretation"
    )
    assert statement["payload"]["topic_id"] == expected["management_statement"]["topic_id"]
    assert "tariff" in statement["payload"]["statement"].casefold()
    assert "freight" in statement["payload"]["statement"].casefold()
    assert event["payload"]["effective_month"] == expected["company_event"]["effective_month"]
    assert event["header"]["fiscal_period_id"] is None
    assert event["header"]["knowledge_date"] == expected["company_event"]["knowledge_date"]
    assert statement["header"]["knowledge_date"] == expected["management_statement"]["knowledge_date"]
    assert model["header"]["knowledge_date"] == expected["model_interpretation"]["knowledge_date"]
    assert model["payload"]["interpretation"] == expected["model_interpretation"]["text"]
    assert model["header"]["publication_date"] != "2026-03-04"
    observation_by_id = {
        row["header"]["record_id"]: row for row in package["observations"]
    }
    assert {
        observation_by_id[record_id]["payload"]["kind"]
        for record_id in model["payload"]["input_record_ids"]
    } == {"NumericalFact", "GuidanceVersion"}


def test_reversed_and_seeded_shuffled_source_inputs_are_byte_identical(
    tmp_path: Path, result
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    variants = []
    reversed_value = copy.deepcopy(raw)
    for key in ("documents", "required_assertions", "reviewed_links", "periods"):
        reversed_value[key].reverse()
    for assertion in reversed_value["required_assertions"]:
        label_evidence = assertion["locator"].get("fiscal_label_evidence")
        if label_evidence is not None:
            label_evidence["claims"].reverse()
    for period in reversed_value["periods"]:
        period["fiscal_claim_assertion_keys"].reverse()
    calendar_rule = reversed_value["profile"]["reviewed_calendar_rule"]
    for key in (
        "quarter_week_counts",
        "annual_week_counts",
        "fiscal_year_end_months",
        "reviewed_horizons",
    ):
        calendar_rule[key].reverse()
    reversed_value["profile"]["member_aliases"].reverse()
    reversed_value["reviewed_model_inputs"][0]["input_assertion_keys"].reverse()
    variants.append(reversed_value)

    shuffled = copy.deepcopy(raw)
    randomizer = random.Random(20260801)
    for key in ("documents", "required_assertions", "reviewed_links", "periods"):
        randomizer.shuffle(shuffled[key])
    for assertion in shuffled["required_assertions"]:
        label_evidence = assertion["locator"].get("fiscal_label_evidence")
        if label_evidence is not None:
            randomizer.shuffle(label_evidence["claims"])
    for period in shuffled["periods"]:
        randomizer.shuffle(period["fiscal_claim_assertion_keys"])
    calendar_rule = shuffled["profile"]["reviewed_calendar_rule"]
    for key in (
        "quarter_week_counts",
        "annual_week_counts",
        "fiscal_year_end_months",
        "reviewed_horizons",
    ):
        randomizer.shuffle(calendar_rule[key])
    randomizer.shuffle(shuffled["profile"]["member_aliases"])
    randomizer.shuffle(shuffled["reviewed_model_inputs"][0]["input_assertion_keys"])
    variants.append(shuffled)

    for index, value in enumerate(variants):
        path = tmp_path / f"variant-{index}.json"
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8", newline="\n")
        assert _build(path).payload == result.payload


def test_deterministic_serialization_and_golden_sha(result, expected: dict, tmp_path: Path) -> None:
    assert serialize_package(result.package) == result.payload
    assert hashlib.sha256(result.payload).hexdigest() == expected["serialization_sha256"]
    assert result.sidecar_sha256 == expected["serialization_sha256"]
    assert not result.payload.startswith(b"\xef\xbb\xbf")
    assert b"\r\n" not in result.payload
    assert b"generated_at" not in result.payload
    output = tmp_path / "ANF_longitudinal_company_memory.v1.json"
    output.write_bytes(result.payload)
    assert output.read_bytes() == result.payload
    assert list(REPO.rglob("*_longitudinal_company_memory.v1.json")) == []


def test_typed_calendar_rule_is_the_only_c2_canonical_output_delta(result) -> None:
    calendar = result.package["fiscal_calendars"][0]
    assert calendar["calendar_rule_id"] == SOURCE_LABELLED_52_53_WEEK_RULE_ID
    legacy_shape = copy.deepcopy(result.package)
    removed = legacy_shape["fiscal_calendars"][0].pop("calendar_rule_id")
    assert removed == SOURCE_LABELLED_52_53_WEEK_RULE_ID
    assert hashlib.sha256(serialize_package(legacy_shape)).hexdigest() == "4958979b2acd88a4d6590ed8f0d2b8b9c24d44f58d9cf5d0e42f33281ac451c7"
    assert hashlib.sha256(result.payload).hexdigest() == "b25584e692568b460dda20a620a9e8f8f50e80c89d89a5bc41c30fe0dab4e4e0"


def test_shared_adapter_runtime_contains_no_ticker_or_issuer_literals() -> None:
    root = REPO / "pbi_xbrl" / "longitudinal_memory" / "source_adapter"
    banned = ("anf", "pbi", "gpre", "abercrombie", "hollister", "apac", "emea", "americas")
    violations = []
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                lowered = node.value.casefold()
                if any(token in lowered for token in banned):
                    violations.append((path.name, node.lineno, node.value))
    assert violations == []


@pytest.mark.parametrize(
    "mutation",
    ["transcript-filed-authority", "transcript-sec-basis", "missing-sec-accession"],
)
def test_incoherent_document_roles_fail_full_build(
    tmp_path: Path, mutation: str
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if mutation.startswith("transcript"):
        document = next(
            row for row in raw["documents"] if row["source_family"] == "issuer-transcript"
        )
        if mutation == "transcript-filed-authority":
            document["authority_class"] = "filed-exhibit"
        else:
            document["publication_date_basis"] = "sec-filed-date"
    else:
        document = next(row for row in raw["documents"] if row["source_family"] == "sec-exhibit")
        document["accession"] = None
    with pytest.raises(SourceContractError):
        _build(_write_source_set(tmp_path, raw, f"{mutation}.json"))


def test_reviewed_model_artifact_cannot_claim_issuer_authority(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    raw["reviewed_model_inputs"][0]["authority_class"] = "company-release"
    with pytest.raises(SourceContractError, match="schema validation"):
        _build(_write_source_set(tmp_path, raw, "model-role.json"))


def test_embedded_dateline_role_rejects_declared_date_mismatch(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    document = next(row for row in raw["documents"] if row["source_family"] == "issuer-pdf")
    document["publication_date"] = "2026-01-13"
    with pytest.raises(SourceContractError, match="disagrees with publication_date"):
        _build(_write_source_set(tmp_path, raw, "pdf-role-date.json"))


def test_embedded_pdf_dateline_replays_verified_bytes(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    document = next(row for row in raw["documents"] if row["source_family"] == "issuer-pdf")
    document["publication_date"] = "2026-01-13"
    document["embedded_publication_date"] = "2026-01-13"
    document["publication_date_locator"]["text_fingerprint"] = "January 13, 2026"
    document["publication_date_locator"]["excerpt_sha256"] = hashlib.sha256(
        b"January 13, 2026"
    ).hexdigest()
    with pytest.raises(LocatorError, match="dateline locator failed"):
        _build(_write_source_set(tmp_path, raw, "pdf-date.json"))


@pytest.mark.parametrize(
    ("period_key", "start_date", "end_date", "message"),
    [
        ("fy2024-q4", None, "2025-02-08", "source-backed week count or end date"),
        ("fy2025", None, "2026-02-07", "source-backed week count or end date"),
        ("2026-mar", "2026-04-01", "2026-04-30", "relative-month period"),
    ],
)
def test_temporal_period_declarations_replay_source_evidence(
    tmp_path: Path,
    period_key: str,
    start_date: str | None,
    end_date: str,
    message: str,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    period = next(row for row in raw["periods"] if row["period_key"] == period_key)
    period["start_date"] = start_date
    period["end_date"] = end_date
    with pytest.raises(MappingError, match=message):
        _build(_write_source_set(tmp_path, raw, f"period-{period_key}.json"))


@pytest.mark.parametrize(
    "mutation",
    [
        "annual-fiscal-year",
        "q4-fiscal-year",
        "q4-quarter",
        "q3-quarter",
        "fiscal-ordinal",
        "source-label",
        "profile-hint",
        "incompatible-labels",
        "missing-label",
    ],
)
def test_actual_fiscal_identity_replays_source_labels_full_build(
    tmp_path: Path,
    mutation: str,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if mutation == "annual-fiscal-year":
        _period_spec(raw, "fy2025")["fiscal_year"] = 2024
    elif mutation == "q4-fiscal-year":
        _period_spec(raw, "fy2025-q4")["fiscal_year"] = 2024
    elif mutation == "q4-quarter":
        _period_spec(raw, "fy2025-q4")["fiscal_quarter"] = 3
    elif mutation == "q3-quarter":
        _period_spec(raw, "fy2025-q3")["fiscal_quarter"] = 4
    elif mutation == "fiscal-ordinal":
        _period_spec(raw, "fy2025-q4")["fiscal_ordinal"] = 105
    elif mutation == "profile-hint":
        raw["profile"]["calendar_hint"] = "calendar-quarter year ending December 31"
    else:
        locator = _period_evidence_assertion(raw, "fy2025")["locator"]
        label_evidence = locator["fiscal_label_evidence"]
        if mutation == "source-label":
            claim = next(
                row for row in label_evidence["claims"] if row["claim_kind"] == "fiscal-year"
            )
            claim.update(
                {
                    "text_fingerprint": "fiscal 2024",
                    "match_ordinal": 1,
                    "excerpt_sha256": "603eb91d9e90874e4f47a70b65cf703f6f4cf337db0db5796fe0cbdb47329e6e",
                }
            )
        elif mutation == "incompatible-labels":
            label_evidence["claims"].append(
                {
                    "claim_key": "fy2025-annual-conflicting-year",
                    "claim_kind": "fiscal-year",
                    "text_fingerprint": "fiscal 2024",
                    "match_ordinal": 1,
                    "excerpt_sha256": "603eb91d9e90874e4f47a70b65cf703f6f4cf337db0db5796fe0cbdb47329e6e",
                }
            )
        else:
            locator.pop("fiscal_label_evidence")
    messages = {
        "annual-fiscal-year": "source fiscal year disagrees",
        "q4-fiscal-year": "source fiscal year disagrees",
        "q4-quarter": "source fiscal quarter disagrees",
        "q3-quarter": "source fiscal quarter disagrees",
        "fiscal-ordinal": "source-derived ordinal disagrees",
        "source-label": "exactly one compatible fiscal year",
        "profile-hint": "Profile calendar hint conflicts",
        "incompatible-labels": "exactly one compatible fiscal year",
        "missing-label": "declared fiscal-evidence membership is incomplete",
    }
    with pytest.raises(MappingError, match=messages[mutation]):
        _build(_write_source_set(tmp_path, raw, f"fiscal-label-{mutation}.json"))


@pytest.mark.parametrize(
    "mutation",
    [
        "genuine-cross-year-annual-claims",
        "generic-year-specific-quarter-conflict",
        "annual-relabelled-prior-year",
        "q4-relabelled-prior-year",
        "q4-relabelled-annual",
        "annual-relabelled-q4",
        "guidance-full-year-relabelled-q4",
        "guidance-full-year-thirteen-weeks",
        "guidance-full-year-non-null-quarter",
        "guidance-quarter-only-label-evidence",
        "direct-annual-quarter-conflict",
        "direct-fiscal-year-conflict",
        "annual-quarter-like-ordinal",
        "profile-attempts-conflict-override",
        "conflicting-claims-reversed",
        "conflicting-claims-shuffled",
    ],
)
def test_atomic_fiscal_label_tuple_reconciliation_fails_full_build(
    tmp_path: Path,
    mutation: str,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def claims(period_key: str) -> list[dict]:
        return _period_evidence_assertion(raw, period_key)["locator"][
            "fiscal_label_evidence"
        ]["claims"]

    def add_fiscal_2024_claim(rows: list[dict], claim_key: str) -> None:
        rows.append(
            {
                "claim_key": claim_key,
                "claim_kind": "fiscal-year",
                "text_fingerprint": "fiscal 2024",
                "match_ordinal": 1,
                "excerpt_sha256": "603eb91d9e90874e4f47a70b65cf703f6f4cf337db0db5796fe0cbdb47329e6e",
            }
        )

    if mutation == "genuine-cross-year-annual-claims":
        period = _period_spec(raw, "fy2025")
        period["fiscal_year"] = 2024
        period["fiscal_ordinal"] = 100
        year_claim = next(row for row in claims("fy2025") if row["claim_kind"] == "fiscal-year")
        year_claim.update(
            {
                "text_fingerprint": "fiscal 2024",
                "match_ordinal": 1,
                "excerpt_sha256": "603eb91d9e90874e4f47a70b65cf703f6f4cf337db0db5796fe0cbdb47329e6e",
            }
        )
    elif mutation == "generic-year-specific-quarter-conflict":
        year_claim = next(row for row in claims("fy2024-q4") if row["claim_kind"] == "fiscal-year")
        year_claim.update(
            {
                "text_fingerprint": "For fiscal 2025",
                "match_ordinal": 1,
                "excerpt_sha256": "4d1d3946cdb56aaacc7905a5ff4d3dd091861ca0fbf6fcdf042a79d10c2c12d4",
            }
        )
        quarter_claim = next(
            row for row in claims("fy2024-q4") if row["claim_kind"] == "fiscal-quarter"
        )
        quarter_claim.update(
            {
                "text_fingerprint": "Fourth Quarter Full Year 2024",
                "match_ordinal": 1,
                "excerpt_sha256": "5b58f360775b2b072bf1b285e231771ae1285f74531ca523c945e10a3c5e3ec6",
            }
        )
    elif mutation == "annual-relabelled-prior-year":
        period = _period_spec(raw, "fy2025")
        period["fiscal_year"] = 2024
        period["fiscal_ordinal"] = 100
    elif mutation == "q4-relabelled-prior-year":
        period = _period_spec(raw, "fy2025-q4")
        period["fiscal_year"] = 2024
        period["fiscal_ordinal"] = 100
    elif mutation == "q4-relabelled-annual":
        period = _period_spec(raw, "fy2025-q4")
        period["period_type"] = "annual"
        period["fiscal_quarter"] = None
    elif mutation == "annual-relabelled-q4":
        period = _period_spec(raw, "fy2025")
        period["period_type"] = "quarter"
        period["fiscal_quarter"] = 4
    elif mutation == "guidance-full-year-relabelled-q4":
        period = _period_spec(raw, "fy2026")
        period.update(
            {
                "period_type": "quarter",
                "fiscal_quarter": 4,
                "start_date": "2026-11-01",
                "week_count": 13,
            }
        )
    elif mutation == "guidance-full-year-thirteen-weeks":
        period = _period_spec(raw, "fy2026")
        period["start_date"] = "2026-11-01"
        period["week_count"] = 13
    elif mutation == "guidance-full-year-non-null-quarter":
        _period_spec(raw, "fy2026")["fiscal_quarter"] = 4
    elif mutation == "guidance-quarter-only-label-evidence":
        period = _period_spec(raw, "fy2026")
        period.update(
            {
                "period_type": "quarter",
                "fiscal_quarter": 1,
                "fiscal_ordinal": 105,
                "start_date": "2026-02-01",
                "end_date": "2026-05-02",
                "week_count": 13,
            }
        )
        rows = claims("fy2026")
        rows[:] = [row for row in rows if row["claim_kind"] != "annual-period"]
        rows.append(
            {
                "claim_key": "fy2026-guidance-quarter-only",
                "claim_kind": "fiscal-quarter",
                "text_fingerprint": "First Quarter Outlook",
                "match_ordinal": 1,
                "excerpt_sha256": "2074b4b2c47ec06975a5363557373bc1f29edc1ad50fc828cf2193406b95046c",
            }
        )
    elif mutation == "direct-annual-quarter-conflict":
        claims("fy2025").append(
            {
                "claim_key": "fy2025-annual-conflicting-quarter",
                "claim_kind": "fiscal-quarter",
                "text_fingerprint": "During the fourth quarter of 2025",
                "match_ordinal": 1,
                "excerpt_sha256": "b2732678dfd6874891455eaf1e8ea720ea55ab7648b72842c53438214a16be3e",
            }
        )
    elif mutation == "direct-fiscal-year-conflict":
        add_fiscal_2024_claim(claims("fy2025"), "fy2025-direct-conflicting-year")
    elif mutation == "annual-quarter-like-ordinal":
        _period_spec(raw, "fy2025")["fiscal_ordinal"] = 103
    elif mutation == "profile-attempts-conflict-override":
        period = _period_spec(raw, "fy2025")
        period["fiscal_year"] = 2024
        period["fiscal_ordinal"] = 100
        year_claim = next(row for row in claims("fy2025") if row["claim_kind"] == "fiscal-year")
        year_claim.update(
            {
                "text_fingerprint": "fiscal 2024",
                "match_ordinal": 1,
                "excerpt_sha256": "603eb91d9e90874e4f47a70b65cf703f6f4cf337db0db5796fe0cbdb47329e6e",
            }
        )
        raw["profile"]["calendar_hint"] = "profile attempts to prefer the producer tuple"
        raw["profile"]["reviewed_calendar_rule"]["display_hint"] = raw["profile"][
            "calendar_hint"
        ]
    else:
        rows = claims("fy2025")
        add_fiscal_2024_claim(rows, "fy2025-order-conflicting-year")
        if mutation == "conflicting-claims-reversed":
            rows.reverse()
        else:
            random.Random(20260802).shuffle(rows)

    messages = {
        "genuine-cross-year-annual-claims": "exactly one compatible fiscal year",
        "generic-year-specific-quarter-conflict": "multiple incompatible period types",
        "annual-relabelled-prior-year": "source fiscal year disagrees",
        "q4-relabelled-prior-year": "source fiscal year disagrees",
        "q4-relabelled-annual": "source period type disagrees",
        "annual-relabelled-q4": "source period type disagrees",
        "guidance-full-year-relabelled-q4": "exact authority tuple",
        "guidance-full-year-thirteen-weeks": "exact authority tuple",
        "guidance-full-year-non-null-quarter": "exact authority tuple",
        "guidance-quarter-only-label-evidence": "exact authority tuple",
        "direct-annual-quarter-conflict": "exactly one compatible period type",
        "direct-fiscal-year-conflict": "exactly one compatible fiscal year",
        "annual-quarter-like-ordinal": "source-derived ordinal disagrees",
        "profile-attempts-conflict-override": "exactly one compatible fiscal year",
        "conflicting-claims-reversed": "exactly one compatible fiscal year",
        "conflicting-claims-shuffled": "exactly one compatible fiscal year",
    }
    with pytest.raises((LocatorError, MappingError), match=messages[mutation]):
        _build(_write_source_set(tmp_path, raw, f"atomic-fiscal-{mutation}.json"))


@pytest.mark.parametrize(
    "mutation",
    [
        "quarter-declared-annual",
        "annual-declared-quarter",
        "producer-year-mismatch",
        "generic-kind-changed",
        "annual-text-changed-to-quarter",
    ],
)
def test_fiscal_claim_semantics_are_derived_from_verified_text_full_build(
    tmp_path: Path,
    mutation: str,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    quarter_claim = next(
        row
        for row in _period_evidence_assertion(raw, "fy2025-q4")["locator"][
            "fiscal_label_evidence"
        ]["claims"]
        if row["claim_kind"] == "fiscal-quarter"
    )
    annual_claims = _period_evidence_assertion(raw, "fy2025")["locator"][
        "fiscal_label_evidence"
    ]["claims"]
    annual_claim = next(row for row in annual_claims if row["claim_kind"] == "annual-period")
    generic_claim = next(row for row in annual_claims if row["claim_kind"] == "fiscal-year")
    if mutation == "quarter-declared-annual":
        quarter_claim["claim_kind"] = "annual-period"
    elif mutation == "annual-declared-quarter":
        annual_claim["claim_kind"] = "fiscal-quarter"
    elif mutation == "producer-year-mismatch":
        _period_spec(raw, "fy2025")["fiscal_year"] = 2024
    elif mutation == "generic-kind-changed":
        generic_claim["claim_kind"] = "annual-period"
    else:
        annual_claim.update(
            _fiscal_claim(
                annual_claim["claim_key"],
                "annual-period",
                "During the fourth quarter of 2025",
            )
        )
    message = (
        "source fiscal year disagrees"
        if mutation == "producer-year-mismatch"
        else "declares .* but verified source text derives"
    )
    with pytest.raises((LocatorError, MappingError), match=message):
        _build(_write_source_set(tmp_path, raw, f"claim-replay-{mutation}.json"))


@pytest.mark.parametrize(
    "mutation",
    [
        "second-occurrence-quarter-conflict",
        "second-occurrence-year-conflict",
        "omitted-eligible-membership",
        "extra-ineligible-membership",
        "moved-claim-same-period",
        "direct-conflict-priority-reversed",
        "equivalent-duplicate-cannot-hide-conflict",
        "cross-document-year-conflict",
        "cross-document-quarter-conflict",
        "conflicting-source-report-period",
    ],
)
def test_complete_fiscal_evidence_closure_fails_closed_full_build(
    tmp_path: Path,
    mutation: str,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    annual_period = _period_spec(raw, "fy2025")
    closures = _assertion(raw, "store-closures-release")

    if mutation in {
        "second-occurrence-quarter-conflict",
        "direct-conflict-priority-reversed",
    }:
        _attach_fiscal_claims(
            closures,
            [
                _fiscal_claim(
                    "fy2025-second-occurrence-quarter",
                    "fiscal-quarter",
                    "During the fourth quarter of 2025",
                )
            ],
        )
        annual_period["fiscal_claim_assertion_keys"].append("store-closures-release")
        if mutation == "direct-conflict-priority-reversed":
            raw["documents"].reverse()
            raw["required_assertions"].reverse()
    elif mutation == "second-occurrence-year-conflict":
        _attach_fiscal_claims(
            closures,
            [_fiscal_claim("fy2025-second-occurrence-year", "fiscal-year", "fiscal 2024")],
        )
        annual_period["fiscal_claim_assertion_keys"].append("store-closures-release")
    elif mutation == "omitted-eligible-membership":
        _attach_fiscal_claims(
            closures,
            [
                _fiscal_claim(
                    "fy2025-omitted-annual", "annual-period", "record full year 2025 net sales"
                ),
                _fiscal_claim(
                    "fy2025-omitted-year", "fiscal-year", "Reflecting on fiscal 2025"
                ),
            ],
        )
    elif mutation == "extra-ineligible-membership":
        annual_period["fiscal_claim_assertion_keys"].append("period-fy2025-q4")
    elif mutation == "moved-claim-same-period":
        source = _period_evidence_assertion(raw, "fy2025")["locator"]
        closures["locator"]["fiscal_label_evidence"] = source.pop("fiscal_label_evidence")
    elif mutation == "equivalent-duplicate-cannot-hide-conflict":
        _attach_fiscal_claims(
            closures,
            [
                _fiscal_claim(
                    "fy2025-duplicate-annual", "annual-period", "record full year 2025 net sales"
                ),
                _fiscal_claim(
                    "fy2025-duplicate-year", "fiscal-year", "Reflecting on fiscal 2025"
                ),
                _fiscal_claim(
                    "fy2025-duplicate-conflict",
                    "fiscal-quarter",
                    "During the fourth quarter of 2025",
                ),
            ],
        )
        annual_period["fiscal_claim_assertion_keys"].append("store-closures-release")
    elif mutation in {"cross-document-year-conflict", "cross-document-quarter-conflict"}:
        assertion = _assertion(raw, "guidance-fy2025-revenue-mar")
        if mutation == "cross-document-year-conflict":
            claims = [_fiscal_claim("fy2025-cross-doc-year", "fiscal-year", "fiscal 2024")]
        else:
            claims = [
                _fiscal_claim(
                    "fy2025-cross-doc-quarter",
                    "fiscal-quarter",
                    "Fourth Quarter (in thousands) 2024",
                )
            ]
        _attach_fiscal_claims(assertion, claims)
        annual_period["fiscal_claim_assertion_keys"].append(assertion["assertion_key"])
    else:
        assertion = _assertion(raw, "comp-fy2025-q3-release-geography")
        assertion["period_key"] = "fy2025"
        _attach_fiscal_claims(
            assertion,
            [
                _fiscal_claim(
                    "fy2025-wrong-report-annual", "annual-period", "Full Year Outlook"
                ),
                _fiscal_claim(
                    "fy2025-wrong-report-year", "fiscal-year", "For fiscal 2025"
                ),
            ],
        )
        annual_period["fiscal_claim_assertion_keys"].append(assertion["assertion_key"])

    messages = {
        "second-occurrence-quarter-conflict": "exactly one compatible period type",
        "second-occurrence-year-conflict": "exactly one compatible fiscal year",
        "omitted-eligible-membership": "declared fiscal-evidence membership is incomplete",
        "extra-ineligible-membership": "contains ineligible assertions",
        "moved-claim-same-period": "declared fiscal-evidence membership is incomplete",
        "direct-conflict-priority-reversed": "exactly one compatible period type",
        "equivalent-duplicate-cannot-hide-conflict": "exactly one compatible period type",
        "cross-document-year-conflict": "exactly one compatible fiscal year",
        "cross-document-quarter-conflict": "exactly one compatible fiscal year",
        "conflicting-source-report-period": "wrong report-period link",
    }
    with pytest.raises((LocatorError, MappingError), match=messages[mutation]):
        _build(_write_source_set(tmp_path, raw, f"fiscal-closure-{mutation}.json"))


@pytest.mark.parametrize(
    "mutation",
    [
        "shift-forward-seven-days",
        "shift-backward-seven-days",
        "different-january-end",
        "changed-start-only",
        "authority-window-substitution",
        "different-fifty-two-week-tuple",
        "changed-anchor",
        "changed-derivation-version",
    ],
)
def test_exact_reviewed_horizon_authority_fails_closed_full_build(
    tmp_path: Path,
    mutation: str,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    period = _period_spec(raw, "fy2026")
    authority = raw["profile"]["reviewed_calendar_rule"]["reviewed_horizons"][0]
    if mutation == "shift-forward-seven-days":
        period.update({"start_date": "2026-02-08", "end_date": "2027-02-06"})
    elif mutation == "shift-backward-seven-days":
        period.update({"start_date": "2026-01-25", "end_date": "2027-01-23"})
    elif mutation == "different-january-end":
        period["end_date"] = "2027-01-23"
    elif mutation == "changed-start-only":
        period["start_date"] = "2026-01-31"
    elif mutation == "authority-window-substitution":
        authority.update({"start_date": "2026-02-02", "end_date": "2027-01-31"})
    elif mutation == "different-fifty-two-week-tuple":
        period.update({"start_date": "2026-02-02", "end_date": "2027-01-31"})
    elif mutation == "changed-anchor":
        authority["anchor_period_key"] = "fy2024-q4"
    else:
        authority["derivation_rule_id"] = "rule:core:contiguous-reviewed-fiscal-horizon@2"
    with pytest.raises((MappingError, SourceContractError)):
        _build(_write_source_set(tmp_path, raw, f"exact-horizon-{mutation}.json"))


def test_complete_compatible_same_origin_and_independent_claims_are_order_invariant(
    tmp_path: Path,
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    annual_period = _period_spec(raw, "fy2025")
    closures = _assertion(raw, "store-closures-release")
    _attach_fiscal_claims(
        closures,
        [
            _fiscal_claim(
                "fy2025-same-origin-annual", "annual-period", "record full year 2025 net sales"
            ),
            _fiscal_claim(
                "fy2025-same-origin-year", "fiscal-year", "Reflecting on fiscal 2025"
            ),
        ],
    )
    independent = _assertion(raw, "guidance-fy2025-revenue-may")
    _attach_fiscal_claims(
        independent,
        [
            _fiscal_claim("fy2025-independent-annual", "annual-period", "Full Year Outlook"),
            _fiscal_claim("fy2025-independent-year", "fiscal-year", "For fiscal 2025"),
        ],
    )
    annual_period["fiscal_claim_assertion_keys"].extend(
        [closures["assertion_key"], independent["assertion_key"]]
    )
    baseline = _build(_write_source_set(tmp_path, raw, "compatible-closure.json"))
    fy2025 = next(
        row for row in baseline.package["periods"] if row["period_id"] == "period:anf:fy2025@1"
    )
    assert len(fy2025["evidence_occurrence_ids"]) == 3

    reordered = copy.deepcopy(raw)
    _period_spec(reordered, "fy2025")["fiscal_claim_assertion_keys"].reverse()
    for assertion_key in ("store-closures-release", "guidance-fy2025-revenue-may"):
        _assertion(reordered, assertion_key)["locator"]["fiscal_label_evidence"]["claims"].reverse()
    reordered["required_assertions"].reverse()
    reordered["documents"].reverse()
    rebuilt = _build(_write_source_set(tmp_path, reordered, "compatible-closure-reordered.json"))
    assert rebuilt.payload == baseline.payload


def test_unrelated_period_claim_is_excluded_from_annual_closure(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assertion = _assertion(raw, "comp-fy2025-q4-release-emea")
    _attach_fiscal_claims(
        assertion,
        [
            _fiscal_claim(
                "fy2025-q4-unrelated-to-annual",
                "fiscal-quarter",
                "During the fourth quarter of 2025",
            )
        ],
    )
    _period_spec(raw, "fy2025-q4")["fiscal_claim_assertion_keys"].append(
        assertion["assertion_key"]
    )
    built = _build(_write_source_set(tmp_path, raw, "unrelated-period-claim.json"))
    annual = next(
        row for row in built.package["periods"] if row["period_id"] == "period:anf:fy2025@1"
    )
    assert len(annual["evidence_occurrence_ids"]) == 1


def test_actual_and_reviewed_fiscal_identities_are_source_bound(result) -> None:
    periods = {row["period_id"]: row for row in result.package["periods"]}
    expected = {
        "period:anf:fy2024-q4@1": (2024, 4, 100, "quarter"),
        "period:anf:fy2025-q3@1": (2025, 3, 103, "quarter"),
        "period:anf:fy2025-q4@1": (2025, 4, 104, "quarter"),
        "period:anf:fy2025@1": (2025, None, 104, "annual"),
        "period:anf:fy2026@1": (2026, None, 108, "annual"),
    }
    assert {
        period_id: (
            periods[period_id]["fiscal_year"],
            periods[period_id]["fiscal_quarter"],
            periods[period_id]["fiscal_ordinal"],
            periods[period_id]["period_type"],
        )
        for period_id in expected
    } == expected
    assert (
        periods["period:anf:fy2026@1"]["start_date"],
        periods["period:anf:fy2026@1"]["end_date"],
        periods["period:anf:fy2026@1"]["day_count"],
        periods["period:anf:fy2026@1"]["week_count"],
    ) == ("2026-02-01", "2027-01-30", 364, 52)

    evidence = {row.assertion_key: row for row in result.extracted_evidence}
    source_periods = {
        str(row["evidence_assertion_key"])
        for row in result.source_set.periods
        if row["start_rule_id"] == "rule:core:inclusive-weeks-ending@1"
    }
    assert all(evidence[key].diagnostics["fiscal_label_claims"] for key in source_periods)
    expected_claim_fields = {
        "claim_key",
        "claim_kind",
        "fiscal_year",
        "period_type",
        "fiscal_quarter",
        "claim_specificity",
        "source_text",
        "locator_identity",
        "match_ordinal",
        "extraction_method_id",
        "digest",
    }
    for assertion_key in source_periods | {"guidance-fy2026-revenue-release"}:
        assert all(
            set(claim) == expected_claim_fields
            for claim in evidence[assertion_key].diagnostics["fiscal_label_claims"]
        )
    assert {
        claim["claim_kind"]
        for claim in evidence["period-fy2025-q4"].diagnostics["fiscal_label_claims"]
    } == {"fiscal-year", "fiscal-quarter"}
    assert {
        claim["claim_kind"]
        for claim in evidence["store-openings-release"].diagnostics["fiscal_label_claims"]
    } == {"fiscal-year", "annual-period"}
    assert {
        claim["claim_kind"]
        for claim in evidence["guidance-fy2026-revenue-release"].diagnostics[
            "fiscal_label_claims"
        ]
    } == {"fiscal-year", "annual-period"}
    for assertion_key in source_periods | {"guidance-fy2026-revenue-release"}:
        for claim in evidence[assertion_key].diagnostics["fiscal_label_claims"]:
            if claim["claim_kind"] == "fiscal-year":
                assert claim["period_type"] == "unspecified_fiscal_context"
                assert claim["fiscal_quarter"] is None
                assert claim["claim_specificity"] == "generic"
            else:
                assert claim["period_type"] in {"fiscal_quarter", "fiscal_year"}
                assert claim["claim_specificity"] == "specific"


@pytest.mark.parametrize("mutation", ["removed", "unrelated-target"])
def test_relative_event_requires_exact_reviewed_event_link(
    tmp_path: Path, mutation: str
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if mutation == "removed":
        raw["reviewed_links"] = [
            row
            for row in raw["reviewed_links"]
            if row["link_key"] != "transcript-event-month-support"
        ]
    else:
        link = next(
            row
            for row in raw["reviewed_links"]
            if row["link_key"] == "transcript-event-month-support"
        )
        link["to_document_key"] = "anf-business-update-2026-01-12"
    with pytest.raises((MappingError, SourceContractError)):
        _build(_write_source_set(tmp_path, raw, f"event-link-{mutation}.json"))


def test_model_interpretation_cannot_be_backdated_in_full_build(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    raw["reviewed_model_inputs"][0]["knowledge_date"] = "2026-03-04"
    with pytest.raises(SourceDiscoveryError, match="backdated"):
        _build(_write_source_set(tmp_path, raw, "backdated-model.json"))


@pytest.mark.parametrize("mutation", ["subject", "program"])
def test_promise_reaffirmation_cannot_cross_subject_or_program(
    tmp_path: Path, mutation: str
) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    candidate = next(
        row for row in raw["required_assertions"] if row["assertion_key"] == "promise-store-plan-may"
    )
    if mutation == "subject":
        candidate["promise_subject_id"] = "store-openings"
        candidate["target_metric_id"] = "metric:retail:store-openings@1"
    else:
        candidate["program_id"] = "different-store-program"
    with pytest.raises(MappingError):
        _build(_write_source_set(tmp_path, raw, f"promise-{mutation}.json"))


def test_promise_predecessor_cannot_belong_to_another_promise(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    second_origin = next(
        row for row in raw["required_assertions"] if row["assertion_key"] == "promise-store-plan-may"
    )
    second_origin["program_id"] = "different-store-program"
    second_origin["change_kind"] = "origin"
    second_origin["version_state"] = "active"
    second_origin["previous_assertion_key"] = None
    with pytest.raises(MappingError, match="predecessor in another promise"):
        _build(_write_source_set(tmp_path, raw, "promise-cross-predecessor.json"))


def test_multiple_compatible_promise_origins_fail_closed(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    second_origin = next(
        row for row in raw["required_assertions"] if row["assertion_key"] == "promise-store-plan-may"
    )
    second_origin["change_kind"] = "origin"
    second_origin["version_state"] = "active"
    second_origin["previous_assertion_key"] = None
    with pytest.raises(MappingError, match="multiple compatible origins"):
        _build(_write_source_set(tmp_path, raw, "promise-multiple-origins.json"))


def test_no_compatible_promise_origin_fails_closed(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    origin = next(
        row for row in raw["required_assertions"] if row["assertion_key"] == "promise-store-plan-mar"
    )
    origin["program_id"] = "different-store-program"
    with pytest.raises(MappingError, match="no compatible"):
        _build(_write_source_set(tmp_path, raw, "promise-no-origin.json"))


def test_valid_reaffirmations_attach_to_one_source_backed_origin(result) -> None:
    promise_entities = [
        row for row in result.package["entities"] if row["payload"]["kind"] == "Promise"
    ]
    assert len(promise_entities) == 1
    promise_id = promise_entities[0]["header"]["entity_id"]
    versions = [
        row
        for row in result.package["observations"]
        if row["payload"]["kind"] == "PromiseVersion"
    ]
    assert len(versions) == 5
    assert {row["payload"]["promise_id"] for row in versions} == {promise_id}
    assert Counter(row["relation_type"] for row in result.package["relations"])["reaffirms"] == 4


def test_golden_is_not_self_confirming_for_wrong_value_dimension_or_period(
    result, expected: dict
) -> None:
    mutated = copy.deepcopy(result.package)
    apac_member = _member_id(mutated, "APAC")
    apac_dimensions = _dimension_set_for_member(mutated, apac_member)
    fact = _selected_fact(
        mutated,
        metric_id="metric:retail:comparable-sales@1",
        period_id="period:anf:fy2025-q4@1",
        dimension_set_id=apac_dimensions,
    )
    fact["payload"]["value"]["value"] = "1"
    assert fact["payload"]["value"] != expected["comparable_sales"]["fy2025_q4_apac"]
    assert hashlib.sha256(serialize_package(mutated)).hexdigest() != expected[
        "serialization_sha256"
    ]
