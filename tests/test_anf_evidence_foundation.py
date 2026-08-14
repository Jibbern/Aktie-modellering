from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest

from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_evidence_foundation import (
    AUDIT_FILENAMES,
    AUDIT_SHA256,
    FOUNDATION_VERSION,
    HISTORICAL_REQUIRED_PERIODS,
    build_anf_evidence_foundation,
    candidate_artifacts,
    write_evidence_foundation_candidate,
)
from pbi_xbrl.longitudinal_memory.serialization import serialize_package


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT_RESOLUTION = resolve_effective_data_root_from_ancestors(REPOSITORY_ROOT)
if DATA_ROOT_RESOLUTION.data_root is None:
    raise FileNotFoundError("No healthy registered StockModelData root is available for ANF evidence tests")
DATA_ROOT = DATA_ROOT_RESOLUTION.data_root
AUDIT_ROOT = (
    DATA_ROOT
    / "audit"
    / "anf_local_source_review_authority_expansion_audit_2026-08-09"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _repository_text_sha256(path: Path) -> str:
    """Hash committed text bytes independent of Git's checkout EOL conversion."""

    normalized = path.read_bytes().replace(b"\r\n", b"\n")
    assert b"\r" not in normalized
    return hashlib.sha256(normalized).hexdigest()


@pytest.fixture(scope="session")
def foundation() -> dict:
    return build_anf_evidence_foundation(
        source_root=DATA_ROOT,
        audit_root=AUDIT_ROOT,
    )


def _facts(foundation: dict, metric: str, period: str) -> list[dict]:
    return [
        row
        for row in foundation["canonical_facts"]
        if row["metric_key"] == metric and row["period_key"] == period
    ]


def _exact_value(foundation: dict, metric: str, period: str) -> str:
    rows = _facts(foundation, metric, period)
    exact = {
        row["canonical_value"]["value"]
        for row in rows
        if row["canonical_value"]["kind"] == "exact"
    }
    assert len(exact) == 1, (metric, period, rows)
    return exact.pop()


def test_reviewed_audit_contract_is_exact_and_closed(foundation: dict) -> None:
    assert set(AUDIT_FILENAMES) == set(AUDIT_SHA256)
    artifacts = {
        row["relative_path"]: row["sha256"]
        for row in foundation["audit_contract"]["artifacts"]
    }
    assert artifacts == AUDIT_SHA256
    assert all(_sha256(AUDIT_ROOT / name) == digest for name, digest in artifacts.items())


def test_review_states_and_limitations_are_structural(foundation: dict) -> None:
    registrations = foundation["source_registrations"]
    assert Counter(row["review_decision"] for row in registrations) == Counter(
        {
            "REVIEW_ACCEPT": 88,
            "REVIEW_ACCEPT_WITH_LIMITATIONS": 100,
            "REVIEW_DUPLICATE_ONLY": 44,
            "REJECT_AS_SOURCE": 17,
        }
    )
    limited = [
        row
        for row in registrations
        if row["review_decision"] == "REVIEW_ACCEPT_WITH_LIMITATIONS"
    ]
    assert all(row["limitation_ids"] for row in limited)
    assert all(
        not row["economic_evidence_eligible"]
        for row in registrations
        if row["review_decision"] in {"REVIEW_DUPLICATE_ONLY", "REJECT_AS_SOURCE"}
    )
    rejected = [
        row for row in registrations if row["source_type"] == "generated_metadata_sidecar"
    ]
    assert len(rejected) == 17
    assert all(row["review_decision"] == "REJECT_AS_SOURCE" for row in rejected)


def test_all_eighteen_sec_primary_filings_are_registered(foundation: dict) -> None:
    filings = [
        row
        for row in foundation["source_registrations"]
        if row["source_type"] == "sec_filing" and row["economic_evidence_eligible"]
    ]
    assert len(filings) == 18
    assert Counter(row["form"] for row in filings) == Counter({"10-Q": 13, "10-K": 5})
    q1_fy26 = next(
        row for row in filings if row["accession"] == "0001018840-26-000036"
    )
    assert q1_fy26["content_sha256"] == (
        "4bb925d6957c71e2760bc9d6e09bd88d43253a8199c0259b0480d223ed2e3079"
    )
    assert q1_fy26["publication_date"] == "2026-06-05"
    assert q1_fy26["knowledge_date"] == "2026-06-05"


def test_wrappers_are_provenance_relations_without_fact_multiplicity(
    foundation: dict,
) -> None:
    wrappers = [
        row
        for row in foundation["source_registrations"]
        if row["source_type"] == "sec_8k_wrapper"
    ]
    assert len(wrappers) == 92
    assert Counter(
        (row["provenance_only"], row["economic_evidence_eligible"])
        for row in wrappers
    ) == Counter({(True, False): 86, (False, True): 6})
    relations = foundation["source_relations"]
    assert len(relations) == 92
    assert all(row["economic_fact_multiplicity"] == 0 for row in relations)
    assert len({row["accession"] for row in relations}) == 92


def test_semantic_sources_dedupe_local_representations(foundation: dict) -> None:
    documents = foundation["semantic_source_documents"]
    assert len(documents) == 102
    assert len({row["content_sha256"] for row in documents}) == len(documents)
    assert all(row["representation_paths"] for row in documents)
    assert all(row["review_decision"] != "REVIEW_DUPLICATE_ONLY" for row in documents)


def test_sec_release_same_basis_reconciliation_is_exact_and_temporal(
    foundation: dict,
) -> None:
    relations = foundation["sec_release_reconciliation_relations"]
    assert len(relations) == 148
    assert Counter(row["metric_id"] for row in relations) == Counter(
        {
            "metric:core:net-sales@1": 26,
            "metric:core:operating-income@1": 26,
            "metric:core:net-income-attributable@1": 26,
            "metric:core:net-income-per-diluted-share@1": 26,
            "metric:core:diluted-weighted-average-shares@1": 26,
            "metric:core:gross-profit@1": 18,
        }
    )
    assert all(row["later_authority_confirmation"] for row in relations)
    assert all(row["release_knowledge_date"] < row["sec_knowledge_date"] for row in relations)
    assert all(
        "cannot backdate" in row["temporal_rule"].casefold() for row in relations
    )


def test_lower_tier_direct_q4_fact_remains_canonical(foundation: dict) -> None:
    fact = _facts(foundation, "net-sales-amount", "FY2025-Q4")[0]
    preferred = next(
        row
        for row in foundation["canonical_observations"]
        if row["observation_id"] == fact["preferred_direct_observation_id"]
    )
    assert preferred["source_authority_tier"] == 2
    assert preferred["semantic_directness"] == "direct_exact"
    assert preferred["canonical_value"] == {"kind": "exact", "value": "1669.802"}
    assert fact["knowledge_dates"] == ["2026-03-04"]


def test_quarter_guidance_is_complete_and_horizon_typed(foundation: dict) -> None:
    assertions = foundation["quarter_guidance_source_assertions"]
    versions = foundation["quarter_guidance_versions"]
    assert len(assertions) == 60
    assert len(versions) == 60
    assert all(row["horizon_type"] == "quarter" for row in assertions + versions)
    assert all(row["horizon_period_id"] != row["stated_in_period_id"] for row in versions)
    assert len({row["source_assertion_id"] for row in versions}) == 60
    assert all(row["guidance_series_id"] for row in versions)


def test_fy2026_q2_six_guidance_versions_and_tax_separation(foundation: dict) -> None:
    versions = [
        row
        for row in foundation["quarter_guidance_versions"]
        if row["horizon_period_id"] == "period:anf:fy2026-q2@1"
    ]
    assert len(versions) == 6
    assert {row["metric_id"] for row in versions} == {
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:share-repurchases@1",
        "metric:anf:tariff-impact@1",
    }
    assert all("tax" not in row["metric_id"] for row in versions)
    assert foundation["adjacent_non_promise_quarter_outlook"][
        "promise_progress_quarter_version"
    ] is False


def test_quarter_guidance_predecessor_successor_relations_are_closed(
    foundation: dict,
) -> None:
    versions = foundation["quarter_guidance_versions"]
    by_id = {row["guidance_version_id"]: row for row in versions}
    for row in versions:
        predecessor = row["predecessor_guidance_version_id"]
        successor = row["successor_guidance_version_id"]
        if predecessor is not None:
            assert by_id[predecessor]["successor_guidance_version_id"] == row[
                "guidance_version_id"
            ]
        if successor is not None:
            assert by_id[successor]["predecessor_guidance_version_id"] == row[
                "guidance_version_id"
            ]


@pytest.mark.parametrize(
    "metric",
    [
        "reported-diluted-eps",
        "diluted-weighted-average-shares",
        "net-sales-amount",
        "operating-income-amount",
    ],
)
def test_historical_core_fact_cohorts_cover_all_twenty_two_periods(
    foundation: dict, metric: str
) -> None:
    periods = {
        row["period_key"]
        for row in foundation["canonical_facts"]
        if row["metric_key"] == metric
        and row["period_key"] in HISTORICAL_REQUIRED_PERIODS
    }
    assert periods == HISTORICAL_REQUIRED_PERIODS


def test_diluted_share_quarter_ytd_and_fy_identities_are_distinct(
    foundation: dict,
) -> None:
    rows = [
        _facts(foundation, "diluted-weighted-average-shares", period)[0]
        for period in ("FY2024-Q3", "FY2024-YTD-Q3", "FY2024")
    ]
    assert [row["canonical_value"]["value"] for row in rows] == [
        "52.869",
        "53.141",
        "52.971",
    ]
    assert len({row["canonical_fact_id"] for row in rows}) == 3
    assert [row["period_kind"] for row in rows] == ["quarter", "ytd", "annual"]


def test_capex_facts_and_temporal_definition_relations(foundation: dict) -> None:
    expected = {
        "FY2022": ("164.566", "120.282"),
        "FY2023": ("157.797", "128.601"),
        "FY2024": ("182.903", "132.04"),
        "FY2025": ("240.774", "185.212"),
    }
    for year, (annual, nine_month) in expected.items():
        assert _exact_value(foundation, "property-equipment-purchases", year) == annual
        assert _exact_value(
            foundation, "property-equipment-purchases", f"{year}-YTD-Q3"
        ) == nine_month
    relation_dates = {
        (row["period_id"], row["knowledge_date"])
        for row in foundation["definition_relations"]
    }
    assert ("period:anf:fy2022@1", "2023-03-02") in relation_dates
    assert ("period:anf:fy2023@1", "2025-03-31") in relation_dates
    assert ("period:anf:fy2024@1", "2025-03-31") in relation_dates
    assert ("period:anf:fy2025-ytd-q3@1", "2025-12-05") in relation_dates
    assert ("period:anf:fy2025@1", "2026-03-26") in relation_dates
    assert all(
        row["temporal_rule"] == "relation is unavailable before its knowledge date"
        for row in foundation["definition_relations"]
    )


def test_repurchase_cash_periods_and_program_narrative_are_not_conflated(
    foundation: dict,
) -> None:
    assert _exact_value(foundation, "common-stock-purchases-cash", "FY2025-Q1") == "200"
    assert _exact_value(
        foundation, "common-stock-purchases-cash", "FY2025-YTD-Q3"
    ) == "351.224"
    assert _exact_value(foundation, "common-stock-purchases-cash", "FY2025") == "451.224"
    narrative = _facts(foundation, "share-repurchases", "FY2025")
    assert {row["canonical_value"]["value"] for row in narrative} == {"450"}
    cash = _facts(foundation, "common-stock-purchases-cash", "FY2025")[0]
    assert cash["canonical_fact_id"] not in {
        row["canonical_fact_id"] for row in narrative
    }


def test_annual_store_activity_components_remain_separate(foundation: dict) -> None:
    expected = {
        "FY2022": ("59", "26", "1", "8"),
        "FY2023": ("35", "32", "13", "9"),
        "FY2024": ("65", "41", "48", "12"),
        "FY2025": ("62", "22", "47", "11"),
    }
    for period, values in expected.items():
        actual = tuple(
            _exact_value(foundation, metric, period)
            for metric in (
                "store-openings",
                "store-closures-count",
                "store-remodels",
                "store-right-sizes",
            )
        )
        assert actual == values
    assert not [
        row
        for row in foundation["canonical_facts"]
        if row["metric_key"] == "store-closures"
    ]


def test_fy2026_q1_reviewed_fact_family_is_exact(foundation: dict) -> None:
    expected = {
        "net-sales-amount": "1113.821",
        "operating-income-amount": "88.797",
        "reported-diluted-eps": "1.47",
        "diluted-weighted-average-shares": "45.677",
        "property-equipment-purchases": "61.341",
        "common-stock-purchases-cash": "105.018",
        "store-openings": "6",
        "store-closures-count": "1",
        "store-remodels": "24",
        "store-right-sizes": "2",
    }
    for metric, value in expected.items():
        assert _exact_value(foundation, metric, "FY2026-Q1") == value


def test_transcript_numeric_facts_canonicalize_without_release_duplication(
    foundation: dict,
) -> None:
    report = foundation["transcript_canonicalization"]
    assert report["reviewed_transcript_documents"] == 17
    assert report["reviewed_explicit_economic_cluster_lower_bound"] == 51
    assert report["net_new_cohort_occurrences"] == 31
    transcript_observations = [
        row
        for row in foundation["canonical_observations"]
        if row["source_authority_tier"] == 4
    ]
    assert len(transcript_observations) >= 18
    remodel = _facts(foundation, "store-remodels", "FY2025")[0]
    assert len(remodel["source_document_ids"]) == 2
    assert len(_facts(foundation, "store-remodels", "FY2025")) == 1


def test_investor_day_targets_and_representation_limits_are_preserved(
    foundation: dict,
) -> None:
    assert len(foundation["management_target_assertions"]) == 20
    report = foundation["presentation_canonicalization"]
    assert report["investor_day_typed_target_count"] == 20
    codes = {row["code"] for row in report["history_selection_limitations"]}
    assert {
        "mixed_scale_regions",
        "stale_period_labels",
        "fy2025_column_o_is_39_week_ytd_not_annual",
        "annual_mapping_from_column_o_forbidden",
        "missing_visual_image_layer",
    } <= codes
    assert report["known_bad_history_cells_are_authority"] is False


def test_fy2026_capex_comparator_error_cannot_mint_guidance(foundation: dict) -> None:
    conflict = next(
        row
        for row in foundation["source_conflicts"]
        if row["conflict_id"] == "source-conflict:fy2026-capex-previous-comparator"
    )
    assert conflict["resolution"] == "issuer_comparator_error"
    assert conflict["mint_guidance_version"] is False
    assert conflict["canonical_historical_value"] == {
        "kind": "range",
        "low": "200",
        "high": "225",
        "unit": "USD million",
    }
    assert all(
        not (
            row["canonical_value"].get("low") == "200"
            and row["canonical_value"].get("high") == "250"
        )
        for row in foundation["quarter_guidance_versions"]
    )


def test_q4_direct_evidence_and_derivation_graph_are_complete(foundation: dict) -> None:
    assert foundation["q4_evidence_matrix"]["summary"]["classification_counts"] == {
        "direct": 28,
        "derived_exact": 24,
        "unavailable": 4,
    }
    bindings = foundation["q4_direct_evidence_bindings"]
    assert len(bindings) == 28
    assert Counter(row["representation"] for row in bindings) == Counter(
        {"canonical-direct-fact": 28}
    )
    assert all(row["period_id"].endswith("-q4@1") for row in bindings)


def test_derivation_metadata_keeps_identity_and_bounded_precision(
    foundation: dict,
) -> None:
    derivations = foundation["derivation_opportunities"]
    assert derivations["summary"]["classification_counts"] == {
        "derived_exact": 16,
        "derived_components": 8,
        "derived_bounded": 4,
    }
    for row in derivations["records"]:
        if row["classification"] in {"derived_exact", "derived_components"}:
            checks = json.dumps(row["required_identity_checks"]).casefold()
            assert "currency" in checks
            assert "calendar" in checks
        if row["classification"] == "derived_bounded":
            serialized = json.dumps(row, sort_keys=True).casefold()
            assert "round" in serialized or "precision" in serialized
            assert row["classification"] != "derived_exact"


def test_cross_sheet_facts_are_generic_and_segment_recast_is_prepared(
    foundation: dict,
) -> None:
    counts = foundation["cross_sheet_ownership"]["summary"]["destination_counts"]
    assert counts == {
        "Capital Returns": 24,
        "Debt Detail": 4,
        "Operating Drivers": 115,
        "Promise Progress": 144,
        "Promise Progress after definition review": 8,
        "Quarter Notes": 111,
        "Summary": 172,
        "Valuation": 126,
    }
    segment = foundation["segment_definition_evidence"][0]
    assert segment["prior_periods_recast"] is True
    assert segment["candidate_downstream"] == ["Summary", "BS_segment"]
    assert foundation["cross_sheet_ownership"]["summary"][
        "downstream_modification_performed"
    ] is False


def test_audit_confirmed_evidence_has_complete_disposition(foundation: dict) -> None:
    report = foundation["evidence_disposition"]
    assert report["audit_confirmed_gap_count"] == 246
    assert report["implemented_count"] == 231
    assert report["duplicate_or_reconciled_count"] == 0
    assert report["other_product_count"] == 0
    assert report["incompatible_count"] == 0
    assert report["explicitly_deferred_count"] == 15
    assert report["unexplained_count"] == 0
    deferred = next(
        row
        for row in report["cohort_dispositions"]
        if row["cohort_id"] == "gap-cohort:additional-transcript-clusters"
    )
    assert deferred["explicitly_deferred_count"] == 15
    assert all(
        token in deferred["reason"]
        for token in ("source", "line", "speaker", "metric", "period", "unit", "value")
    )


def test_absent_sources_remain_backlog_not_fabricated(foundation: dict) -> None:
    backlog = foundation["remaining_acquisition_backlog"]
    assert len(backlog) == 5
    assert {
        row["source_family"] for row in backlog
    } >= {
        "FY2026 Q1 earnings-call transcript",
        "FY2026 Q1 deck, schedules, and history",
        "2022 Investor Day transcript",
        "2022 Investor Day slide images 51-164",
    }
    assert all("acquisition" in row["required_action"] or "acquire" in row["required_action"] for row in backlog[:4])


def test_successor_foundation_is_deterministic_and_manifested(
    foundation: dict, tmp_path: Path
) -> None:
    rebuilt = build_anf_evidence_foundation(
        source_root=DATA_ROOT,
        audit_root=AUDIT_ROOT,
    )
    assert serialize_package(foundation) == serialize_package(rebuilt)
    first = tmp_path / "first"
    second = tmp_path / "second"
    first_manifest = write_evidence_foundation_candidate(foundation, first)
    second_manifest = write_evidence_foundation_candidate(rebuilt, second)
    assert first_manifest == second_manifest
    expected = {*candidate_artifacts(foundation), "manifest.json"}
    assert {path.name for path in first.iterdir()} == expected
    assert {
        path.name: _sha256(path) for path in first.iterdir()
    } == {
        path.name: _sha256(path) for path in second.iterdir()
    }
    assert first_manifest["golden_pinned"] is False
    assert first_manifest["production_cutover"] is False
    assert foundation["foundation_version"] == FOUNDATION_VERSION
    assert foundation["source_set_id"].endswith("@4")
    assert foundation["predecessor_source_set_id"].endswith("@3")
    assert foundation["accepted_product_v2_source_set_id"].endswith("@2")
    assert foundation["projection_or_workbook_correction_performed"] is False


def test_accepted_product_goldens_and_anf_workbook_remain_exact() -> None:
    fixtures = REPOSITORY_ROOT / "tests" / "fixtures"
    expected = {
        fixtures / "longitudinal_memory" / "anf_source_set.v2.json": (
            "73a385b0d9c351b5356c34b06ef8d3bb71fcc9f9b503f278bdc550621016a877"
        ),
        fixtures / "promise_progress" / "anf_product.v2.json": (
            "72266543e1c122691dfcdd6d0ee0e472707e527029e719a50881c417da328a05"
        ),
        fixtures / "promise_progress" / "anf_shadow.v2.json": (
            "c32fe7e85d69b1811b92e12e2e55d36fdb63930d8704d48638855c08d62de3c6"
        ),
        fixtures / "promise_progress" / "anf_product_v2_golden_manifest.v1.json": (
            "d9cf40475d444043fdb3b21507b1efd2ca05115a62287ce406e77e8e6d5a7d3e"
        ),
        DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx": (
            "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
        ),
    }
    actual = {
        path: (_sha256(path) if path.suffix == ".xlsx" else _repository_text_sha256(path))
        for path in expected
    }
    assert actual == expected


def test_product_v1_oracle_remains_frozen() -> None:
    oracle_path = (
        REPOSITORY_ROOT
        / "tests"
        / "fixtures"
        / "promise_progress"
        / "anf_legacy_oracle.v1.json"
    )
    oracle = json.loads(oracle_path.read_text(encoding="utf-8"))
    assert oracle["source_package_golden_sha256"] == (
        "b25584e692568b460dda20a620a9e8f8f50e80c89d89a5bc41c30fe0dab4e4e0"
    )
    assert oracle["expected_product_sha256"] == (
        "9e9c042289c1d4e424595c12a6d495170e52a46adfea9ce007baf005fb6265b1"
    )
    assert oracle["expected_shadow_sha256"] == (
        "37285c198f975f77e54c17a70abcf0930c81339964fee2d7f6c51da6d64efdb9"
    )
