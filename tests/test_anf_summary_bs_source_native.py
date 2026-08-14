from __future__ import annotations

import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path

import pytest

from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.summary_bs_products import (
    ProductContractError,
    evaluate_derivation,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_summary_bs_foundation import (
    AUDIT_SHA256,
    PROTECTED_PRODUCTION_WORKBOOK_SHA256,
    SEMANTIC_IDENTITY_MIGRATION_CONTRACT,
    SOURCE_SET_SHA256,
    build_anf_summary_bs_products,
    write_anf_summary_bs_candidate_package,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
AUDIT_ROOT = (
    DATA_ROOT
    / "audit"
    / "anf_summary_bs_segment_exhaustive_historical_lineage_audit_2026-08-10"
)
PRODUCTION_WORKBOOK = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="session")
def bundle() -> dict:
    return build_anf_summary_bs_products(DATA_ROOT, AUDIT_ROOT)


@pytest.fixture(scope="session")
def artifacts(bundle: dict) -> dict:
    return bundle["artifacts"]


def _field(product: dict, metric: str, period: str) -> dict:
    rows = [
        row
        for row in product["fields"]
        if row["metric_key"] == metric and row["period_id"] == f"period:anf:{period}@1"
    ]
    assert len(rows) == 1, (metric, period, rows)
    return rows[0]


def _value(product: dict, metric: str, period: str) -> str:
    row = _field(product, metric, period)
    assert row["status"] == "available"
    assert row["value"]["kind"] == "exact"
    return row["value"]["value"]


def _strict_json(path: Path) -> object:
    def reject(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject)


def test_historical_audit_contract_and_protected_workbook_are_exact(bundle: dict) -> None:
    assert {row["relative_path"]: row["sha256"] for row in bundle["metadata"]["audit_artifacts"]} == AUDIT_SHA256
    assert all(_sha256(AUDIT_ROOT / name) == digest for name, digest in AUDIT_SHA256.items())
    assert _sha256(PRODUCTION_WORKBOOK) == PROTECTED_PRODUCTION_WORKBOOK_SHA256
    assert bundle["metadata"]["protected_production_workbook_sha256"] == PROTECTED_PRODUCTION_WORKBOOK_SHA256


def test_products_close_the_exact_452_field_universe(artifacts: dict) -> None:
    summary = artifacts["summary_product.json"]
    bs = artifacts["bs_segment_product.json"]
    assert len(summary["fields"]) == 35
    assert len(bs["fields"]) == 417
    assert Counter(row["status"] for row in summary["fields"]) == Counter(
        available=27, needs_review=2, unavailable=6
    )
    assert Counter(row["status"] for row in bs["fields"]) == Counter(
        available=361, needs_review=24, unavailable=32
    )
    assert len({row["period_id"] for row in summary["fields"] + bs["fields"]}) == 13
    assert all("!" not in row["field_id"] for row in summary["fields"] + bs["fields"])
    assert all(row["definition_id"] for row in summary["fields"] + bs["fields"])


def test_summary_uses_direct_eps_and_exact_source_native_liquidity(artifacts: dict) -> None:
    product = artifacts["summary_product.json"]
    assert _value(product, "quarter_diluted_eps", "fy2026-q1") == "1.47"
    assert _field(product, "quarter_diluted_eps", "fy2026-q1")["directness"] == "direct"
    assert _value(product, "quarter_diluted_eps_yoy_change", "fy2026-q1") == "-0.12"
    assert _value(product, "revolver_availability", "fy2026-q1") == "449.531"
    assert _value(product, "liquidity_cash_plus_revolver", "fy2026-q1") == "1043.611"
    invalid = _field(product, "pnl_interest_coverage", "ttm-at-fy2026-q1")
    assert invalid["status"] == "needs_review"
    assert invalid["value"] is None
    assert invalid["directness"] == "invalid_legacy_derivation"
    assert invalid["unit_id"] == "unit:core:ratio@1"
    assert invalid["definition_id"] == "definition:financial:pnl-interest-coverage-ratio@1"


def test_summary_durable_semantic_identity_migration_is_exact(artifacts: dict) -> None:
    product = artifacts["summary_product.json"]
    shadow = artifacts["summary_shadow.json"]
    fields = {row["field_id"]: row for row in product["fields"]}
    expected = {
        "SUMMARY!A3": ("investment_thesis", "business_description", "dimset:anf:total-company@1"),
        "SUMMARY!A5": ("catalysts", "strategic_context", "dimset:anf:total-company@1"),
        "SUMMARY!A7": ("key_risks", "key_competitive_advantage", "dimset:anf:total-company@1"),
        "SUMMARY!B13": ("gross_margin_assessment", "segment_operating_model", "dimset:anf:geography-americas-recast@1"),
        "SUMMARY!B14": ("operating_expense_assessment", "segment_operating_model", "dimset:anf:geography-emea-recast@1"),
        "SUMMARY!B15": ("capital_intensity_assessment", "segment_operating_model", "dimset:anf:geography-apac-recast@1"),
        "SUMMARY!A19": ("tariff_dependency", "inventory_omnichannel_dependency", "dimset:anf:total-company@1"),
        "SUMMARY!A20": ("erp_dependency", "international_growth_dependency", "dimset:anf:total-company@1"),
        "SUMMARY!A21": ("real_estate_dependency", "liquidity_buyback_dependency", "dimset:anf:total-company@1"),
        "SUMMARY!A24": ("thesis_invalidator_margin", "liquidity_refinancing_invalidator", "dimset:anf:total-company@1"),
    }
    migrated = {
        row["legacy_locator"]: row
        for row in shadow["field_lineage"]
        if row.get("semantic_identity_migration_contract")
    }
    assert set(migrated) == set(expected)
    for locator, (old_key, new_key, dimension_set_id) in expected.items():
        lineage = migrated[locator]
        field = fields[lineage["field_id"]]
        assert lineage["semantic_identity_migration_contract"] == SEMANTIC_IDENTITY_MIGRATION_CONTRACT
        assert lineage["historical_metric_key"] == old_key
        assert lineage["canonical_metric_key"] == new_key
        assert field["metric_key"] == new_key
        assert field["dimension_set_id"] == dimension_set_id
        assert field["status"] == "available"
        assert field["value"]["kind"] == "qualitative"
    old_keys = {item[0] for item in expected.values()}
    assert old_keys.isdisjoint({row["metric_key"] for row in product["fields"]})
    assert len({row["field_id"] for row in product["fields"]}) == 35


def test_summary_mix_ttm_and_fcf_are_typed_and_derived(artifacts: dict) -> None:
    product = artifacts["summary_product.json"]
    assert _value(product, "americas_sales_mix", "fy2025") == "0.814689918"
    assert _value(product, "emea_sales_mix", "fy2025") == "0.15535409"
    assert _value(product, "apac_sales_mix", "fy2025") == "0.029955992"
    assert _value(product, "ttm_net_sales", "ttm-at-fy2026-q1") == "5282.802"
    assert _value(product, "ttm_free_cash_flow", "ttm-at-fy2026-q1") == "416.047"
    assert _field(product, "ttm_free_cash_flow_yoy_growth", "fy2026-q1")["status"] == "needs_review"


def test_temporal_roles_are_explicit_for_summary_and_bs(artifacts: dict) -> None:
    summary = artifacts["summary_product.json"]
    assert _field(summary, "business_description", "current-as-of-2026-06-05")["temporal_role"] == "current_snapshot"
    assert _field(summary, "latest_period_end", "fy2026-q1")["temporal_role"] == "latest_reported_quarter"
    assert _field(summary, "ttm_net_sales", "ttm-at-fy2026-q1")["temporal_role"] == "ttm_current_calculation"
    assert _field(summary, "americas_sales_mix", "fy2025")["temporal_role"] == "current_recast_historical_truth"
    assert _field(summary, "price_earnings", "current-as-of-2026-06-05")["temporal_role"] == "external_valuation_dependency"
    bs = artifacts["bs_segment_product.json"]
    assert _field(bs, "cash", "fy2026-q1")["temporal_role"] == "point_in_time_reporting_date"
    assert _field(bs, "geographic_sales_americas", "fy2026-q1")["temporal_role"] == "current_recast_quarter_flow"
    assert _field(bs, "geographic_sales_americas", "fy2025")["temporal_role"] == "current_recast_annual_flow"


def test_bs_source_native_corrections_and_first_visible_qoq(artifacts: dict) -> None:
    product = artifacts["bs_segment_product.json"]
    assert _value(product, "total_cash", "fy2024-q2") == "746.295"
    assert _value(product, "cash_qoq_change", "fy2024-q2") == "-125.793"
    assert _value(product, "net_working_capital_qoq_change", "fy2024-q2") == "-124.887"
    assert _value(product, "long_term_debt_qoq_change", "fy2024-q2") == "-213.102"
    assert _value(product, "total_lease_liabilities", "fy2026-q1") == "1292.477"
    assert _value(product, "net_cash", "fy2026-q1") == "619.224"


def test_balance_sheet_identity_uses_true_liabilities_and_equity_including_nci(artifacts: dict) -> None:
    product = artifacts["bs_segment_product.json"]
    assert _value(product, "total_liabilities", "fy2024-q2") == "1828.408"
    assert _value(product, "total_equity", "fy2024-q2") == "1221.15"
    report = artifacts["balance_sheet_reconciliation.json"]
    assert report["passed"] is True
    assert report["identity_failure_count"] == 0
    assert len(report["records"]) == 8
    assert all(row["asset_fact_id"] and row["nci_fact_id"] for row in report["records"])


def test_segment_taxonomy_recast_and_latest_quarter_are_complete(artifacts: dict) -> None:
    product = artifacts["bs_segment_product.json"]
    expected = {
        "geographic_sales_americas": "899.944",
        "geographic_sales_emea": "167.373",
        "geographic_sales_apac": "46.504",
        "brand_sales_hollister": "549.102",
        "brand_sales_abercrombie": "564.719",
    }
    assert {metric: _value(product, metric, "fy2026-q1") for metric in expected} == expected
    report = artifacts["segment_reconciliation.json"]
    assert report["passed"] is True
    assert report["invalid_splicing_count"] == 0
    assert len(report["quarter_records"]) == 8
    assert len(report["annual_recast_records"]) == 3


def test_missing_never_becomes_zero_and_all_zeros_are_typed(artifacts: dict) -> None:
    report = artifacts["zero_missing_reconciliation.json"]
    assert report["passed"] is True
    assert report["missing_to_zero_substitution_count"] == 0
    assert report["state_counts"] == {
        "derived_zero": 7,
        "explicit_zero": 25,
        "missing": 64,
        "not_applicable": 0,
        "present": 356,
    }
    assert report["explicit_zero_count"] == 25
    assert report["derived_zero_count"] == 7


def test_all_derivations_have_dereferenceable_exact_inputs(artifacts: dict) -> None:
    shadow = artifacts["bs_segment_shadow.json"]
    foundation = shadow["evidence_foundation"]
    facts = {row["canonical_fact_id"] for row in foundation["canonical_facts"]}
    assert foundation["source_set_sha256"] == SOURCE_SET_SHA256
    assert artifacts["summary_shadow.json"]["evidence_foundation_sha256"] == shadow["evidence_foundation_sha256"]
    assert artifacts["derivation_reconciliation.json"]["derivation_count"] == 141
    for row in foundation["derivations"]:
        assert row["passed"] is True
        assert row["output_fact_id"] in facts
        assert set(row["input_fact_ids"]) <= facts


def test_defect_closure_source_disposition_and_foundation_gaps_are_closed(artifacts: dict) -> None:
    closure = artifacts["defect_closure_report.json"]
    assert closure["closed_defect_count"] == 122
    assert closure["still_defective_count"] == 0
    assert {row["closure"] for row in closure["records"]} <= {
        "fixed_product_value",
        "fixed_derivation",
        "retired_invalid_legacy_semantic",
    }
    assert len({row["audit_field_id"] for row in closure["records"]}) == 122
    disposition = artifacts["source_disposition.json"]
    assert disposition["record_count"] == 172
    assert disposition["unexplained_relevant_evidence_count"] == 0
    gap = artifacts["foundation_gap_remaining.json"]
    assert gap["metric_count"] == 86
    assert gap["unexplained_gap_count"] == 0


def test_product_count_and_q4_snapshots_are_exact(artifacts: dict) -> None:
    report = artifacts["product_count_reconciliation.json"]
    assert report["passed"] is True
    assert report["field_counts"] == {"summary": 35, "bs_segment": 417, "combined": 452}
    assert report["status_counts"] == {
        "available": 388,
        "needs_review": 26,
        "not_applicable": 0,
        "unavailable": 38,
    }
    assert report["economic_defect_count"] == 0
    assert report["correctable_missing_count"] == 0
    assert report["q4_counts"] == {
        "direct": 23,
        "derived_exact": 10,
        "derived_components": 6,
        "derived_bounded": 0,
        "legitimately_unavailable": 9,
    }


def test_products_are_not_wired_or_default_and_shadows_are_complete(artifacts: dict) -> None:
    for product_name, shadow_name in (
        ("summary_product.json", "summary_shadow.json"),
        ("bs_segment_product.json", "bs_segment_shadow.json"),
    ):
        product = artifacts[product_name]
        shadow = artifacts[shadow_name]
        assert product["metadata"]["production_default"] is False
        assert product["metadata"]["workbook_binding_status"] == "not_wired"
        assert shadow["workbook_binding_status"] == "not_wired"
        assert shadow["broken_lineage_count"] == 0
        assert hashlib.sha256(serialize_package(product)).hexdigest() == shadow["product_sha256"]


def test_candidate_and_repeat_packages_are_byte_identical(bundle: dict, tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    repeat = tmp_path / "repeat"
    first = write_anf_summary_bs_candidate_package(bundle, candidate)
    second = write_anf_summary_bs_candidate_package(bundle, repeat)
    first_files = {path.name: path.read_bytes() for path in candidate.iterdir()}
    second_files = {path.name: path.read_bytes() for path in repeat.iterdir()}
    assert first_files == second_files
    assert len(first_files) == 13
    assert first["manifest_sha256"] == second["manifest_sha256"]
    manifest = _strict_json(candidate / "manifest.json")
    assert manifest["artifact_count"] == 12
    for row in manifest["artifacts"]:
        path = candidate / row["path"]
        assert _sha256(path) == row["sha256"]
        assert path.stat().st_size == row["size"]
        _strict_json(path)


def test_exact_derivations_fail_closed_on_missing_input() -> None:
    with pytest.raises(ProductContractError, match="Missing derivation inputs"):
        evaluate_derivation("derivation:financial:sum@1", ("1", None))  # type: ignore[arg-type]
    with pytest.raises(ProductContractError, match="denominator"):
        evaluate_derivation("derivation:financial:ratio@1", ("1", "0"))


def test_product_v2_1_golden_tag_remains_immutable() -> None:
    tag_object = subprocess.check_output(
        ["git", "rev-parse", "promise-progress-product-v2-1-workbook-golden^{tag}"],
        cwd=REPOSITORY_ROOT,
        text=True,
    ).strip()
    peeled = subprocess.check_output(
        ["git", "rev-parse", "promise-progress-product-v2-1-workbook-golden^{}"],
        cwd=REPOSITORY_ROOT,
        text=True,
    ).strip()
    assert tag_object == "a5193e461148671bf54738c8ad8a5d6942295701"
    assert peeled == "ce1f1aea07d98e566a142c8221e53efe2ce692de"
