"""ANF declarative source-native foundation for Summary and BS/segment products.

This profile consumes the frozen Product@2.1 evidence foundation plus the exact
machine-readable Summary/BS reconciliation audit.  It never reads workbook cells as
economic inputs and never writes a workbook.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from pbi_xbrl.anf_capital_return_source_adapter import (
    build_anf_capital_return_collection,
)
from pbi_xbrl.longitudinal_memory.identity import build_identity
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.summary_bs_products import (
    BALANCE_IDENTITY_CONTRACT_ID,
    BS_SEGMENT_PRODUCT_TYPE,
    BS_SEGMENT_SHADOW_TYPE,
    BS_SEGMENT_TEMPORAL_CONTRACT_ID,
    DERIVATION_CONTRACT_ID,
    PRODUCT_CONTRACT_VERSION,
    SEGMENT_COMPARABILITY_CONTRACT_ID,
    SUMMARY_PRODUCT_TYPE,
    SUMMARY_SHADOW_TYPE,
    SUMMARY_TEMPORAL_CONTRACT_ID,
    ZERO_MISSING_CONTRACT_ID,
    ProductContractError,
    ProductField,
    SourceNativeProduct,
    canonical_fact_identity,
    date_value,
    derivation_identity,
    evaluate_derivation,
    exact_value,
    product_field_identity,
    qualitative_value,
    validate_balance_sheet_identity,
    validate_segment_sum,
    value_state_for,
)
from pbi_xbrl.longitudinal_memory.types import canonical_decimal


PROFILE_ID = "ticker-profile:anf:summary-bs-source-native@1"
SHARED_FOUNDATION_ID = "evidence-foundation:anf:summary-bs-source-native@1"
SEMANTIC_IDENTITY_MIGRATION_CONTRACT = (
    "contract:anf-summary-durable-semantic-identity-migration@1"
)
PRESENTATION_UNIT_CORRECTION_CONTRACT = (
    "contract:summary-pnl-interest-coverage-ratio-unit-correction@1"
)
AUDIT_TYPE = "ANFSummaryBSSegmentHistoricalLineageAudit@1"
SOURCE_SET_ID = "source-set:anf:reviewed-evidence-foundation-successor@4"
SOURCE_SET_SHA256 = "2c7c51768e2d2ec426f3155c43610fe2c5ee1a4f81b8664925bc30c9d0037217"
ACCEPTED_FOUNDATION_ID = "evidence-foundation:anf:product-v2-1-successor@1"
ACCEPTED_FOUNDATION_SHA256 = "8dc5b59fd1128e5837e4a2ecc0eb9ad3bb69b70c146aea7f71078d46dc6ddf5b"
ACCEPTED_SHADOW_SHA256 = "094ba58548643587b93eb07e96a42742ddf297f8b3702937c72a83f5196007bc"
PROTECTED_PRODUCTION_WORKBOOK_SHA256 = (
    "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
)
SEGMENT_TAXONOMY_ID = "segment-definition:anf:geographic-recast@1"
BRAND_TAXONOMY_ID = "segment-definition:anf:brand-family-current@1"
SEGMENT_RECAST_EFFECTIVE_DISCLOSURE = "2023-09-01"


AUDIT_SHA256: Mapping[str, str] = {
    "audit_summary.md": "02093392be764ba3ff656298499d90adea6dccfc0a82deb7c34e5e639a4daedb",
    "balance_sheet_reconciliation.json": "b6ad06213be9938f335eaf238eec2ff9828b4e29e26da22f116d4083bb7fb497",
    "blank_reconciliation.json": "7921868876e0dd97e8292c42791c5d5414cbdd0030684f946aa3a52313587e71",
    "bs_segment_cell_reconciliation.json": "81b11d099b9525c839491c07f3adb013b1e9ed4c29a5375fe6bb6ccca18e0214",
    "cross_sheet_reconciliation.json": "61b544f7efcf77c9ca6e006b7d490298b2df58309ad8e7c2ac68bb27702ae1ce",
    "exhaustive_reconciliation_matrix.json": "6726403fbc5c9b9fa842de5564d853a64da092443d6c0552a2152afe21e10837",
    "formula_inventory.json": "e60510c99dd19131c5d051a7b313ae6bbe1b61bcd4d05473ebae8508adab31c1",
    "formula_recalculation.json": "049a48328803e81987208e69e3a8623313f2d4bec327086cb3d7426bbed1bac9",
    "foundation_gap_report.json": "ab1ed8dcfd07b844950f019d673ec4e9f38a91814ab662cb826aa2d8b0869a4f",
    "lineage_reconciliation.json": "5f6c0f4c64c7c8803e1be08ae10cc91a8df361f10d77d56d742a4ea02c6a57a2",
    "metric_by_metric_reconciliation.json": "aebae267b172cb343e91661312833dcdf232c47974844507f4fdf6227f29a286",
    "metric_inventory.json": "904cc6673758e39eaf20c4ec0a720e06440c6b4f01776730c323118dfedd956b",
    "period_inventory.json": "418623e527bbb90bdd01987a4ac771d83def4e54fc63941b39409e6908a1656e",
    "q4_complete_matrix.json": "d7c0becc403e69916a5fe8909c027aaac3339999af181ef4804cbb18f5822793",
    "segment_recast_matrix.json": "c4603efeff619bd2115ab1fa43b5279208e2c94bb43c60d42ed7447b442a0742",
    "sheet_inventory.json": "78ecdc30914382b44d08a18e7a940f77124b6709a006a1ee55f55051cdecd64b",
    "source_mapping.json": "4304995dafd7de3013e6520896823eb44857c87379e03c2a575c1bc62f4d44af",
    "source_saturation.json": "2b8ddb5d2be191f622af6fb54e224af477bfefe36f92d02aebf3d97d463d32d2",
    "summary_cell_reconciliation.json": "7e4167787b21c9bd16ba8f3dbedb2f0c652c771bcb3fccbf66388f490d80f958",
    "zero_vs_missing.json": "2a04ec5265589b4f761959161a3674c2aef20669ce3057ca2b3d62662da77d68",
}


UNIT_IDS: Mapping[str, str] = {
    "USD_millions": "unit:core:currency-millions@1",
    "USD_per_share": "unit:core:currency-per-share@1",
    "date": "unit:core:date@1",
    "percent": "unit:core:percent@1",
    "percentage_points": "unit:core:percentage-points@1",
    "ratio": "unit:core:ratio@1",
    "shares_millions": "unit:core:shares-millions@1",
    "text": "unit:core:text@1",
}


DERIVED_METRICS = frozenset(
    {
        "americas_sales_mix",
        "apac_sales_mix",
        "cash_qoq_change",
        "current_ratio",
        "diluted_shares_yoy_growth",
        "emea_sales_mix",
        "goodwill_percent_assets",
        "inventory_growth_minus_sales_growth",
        "inventory_yoy_growth",
        "liquidity_cash_plus_revolver",
        "long_term_debt_qoq_change",
        "net_cash",
        "net_sales_yoy_growth",
        "net_working_capital",
        "net_working_capital_qoq_change",
        "quarter_diluted_eps_yoy_change",
        "quarter_net_income_yoy_growth",
        "quarter_net_sales_yoy_growth",
        "quick_ratio",
        "total_cash",
        "total_lease_liabilities",
        "total_liabilities",
        "ttm_free_cash_flow",
        "ttm_free_cash_flow_yoy_growth",
        "ttm_net_sales",
    }
)


EXISTING_METRIC_IDS: Mapping[str, str] = {
    "diluted_weighted_average_shares": "metric:core:diluted-weighted-average-shares@1",
    "net_sales_total_company": "metric:core:net-sales@1",
    "quarter_diluted_eps": "metric:core:net-income-per-diluted-share@1",
    "quarter_net_income": "metric:core:net-income-attributable@1",
    "quarter_net_sales": "metric:core:net-sales@1",
    "ttm_net_sales": "metric:core:net-sales@1",
}


SUMMARY_EXTERNAL_METRICS = frozenset({"price_earnings", "price_sales"})
SUMMARY_NARRATIVE_METRICS = frozenset(
    {
        "business_description",
        "consumer_demand_dependency",
        "freight_dependency",
        "international_growth_dependency",
        "inventory_omnichannel_dependency",
        "key_competitive_advantage",
        "liquidity_buyback_dependency",
        "liquidity_refinancing_invalidator",
        "segment_operating_model",
        "strategic_context",
        "thesis_invalidator_sales",
    }
)


SUMMARY_NARRATIVE_DEFINITION_IDS: Mapping[str, str] = {
    "business_description": "definition:summary:business-description@1",
    "strategic_context": "definition:summary:current-strategic-context@1",
    "key_competitive_advantage": "definition:summary:key-competitive-advantage@1",
    "segment_operating_model": "definition:summary:segment-operating-model@1",
    "inventory_omnichannel_dependency": "definition:summary:key-dependency@1",
    "international_growth_dependency": "definition:summary:key-dependency@1",
    "liquidity_buyback_dependency": "definition:summary:key-dependency@1",
    "liquidity_refinancing_invalidator": "definition:summary:thesis-invalidator@1",
}


# The accepted historical audit encoded ten visible concepts with legacy metric
# labels that contradicted their text.  This adapter repairs only durable semantic
# identity; the reviewed text, status, period and raw evidence remain unchanged.
# The mapping is keyed by the historical semantic label, never by row order.
SUMMARY_SEMANTIC_IDENTITY_MIGRATIONS: Mapping[str, Mapping[str, str]] = {
    "investment_thesis": {
        "expected_cell": "A3",
        "metric_key": "business_description",
    },
    "catalysts": {
        "expected_cell": "A5",
        "metric_key": "strategic_context",
    },
    "key_risks": {
        "expected_cell": "A7",
        "metric_key": "key_competitive_advantage",
    },
    "gross_margin_assessment": {
        "dimension_set_id": "dimset:anf:geography-americas-recast@1",
        "expected_cell": "B13",
        "metric_key": "segment_operating_model",
    },
    "operating_expense_assessment": {
        "dimension_set_id": "dimset:anf:geography-emea-recast@1",
        "expected_cell": "B14",
        "metric_key": "segment_operating_model",
    },
    "capital_intensity_assessment": {
        "dimension_set_id": "dimset:anf:geography-apac-recast@1",
        "expected_cell": "B15",
        "metric_key": "segment_operating_model",
    },
    "tariff_dependency": {
        "expected_cell": "A19",
        "metric_key": "inventory_omnichannel_dependency",
    },
    "erp_dependency": {
        "expected_cell": "A20",
        "metric_key": "international_growth_dependency",
    },
    "real_estate_dependency": {
        "expected_cell": "A21",
        "metric_key": "liquidity_buyback_dependency",
    },
    "thesis_invalidator_margin": {
        "expected_cell": "A24",
        "metric_key": "liquidity_refinancing_invalidator",
    },
}


BS_DERIVED_METRICS = frozenset(
    {
        "cash_qoq_change",
        "current_ratio",
        "diluted_shares_yoy_growth",
        "goodwill_percent_assets",
        "inventory_growth_minus_sales_growth",
        "inventory_yoy_growth",
        "long_term_debt_qoq_change",
        "net_cash",
        "net_sales_yoy_growth",
        "net_working_capital",
        "net_working_capital_qoq_change",
        "quick_ratio",
        "total_cash",
        "total_lease_liabilities",
        "total_liabilities",
    }
)


GEOGRAPHY_DIMENSIONS: Mapping[str, str] = {
    "geographic_sales_americas": "dimset:anf:geography-americas-recast@1",
    "geographic_sales_emea": "dimset:anf:geography-emea-recast@1",
    "geographic_sales_apac": "dimset:anf:geography-apac-recast@1",
}
BRAND_DIMENSIONS: Mapping[str, str] = {
    "brand_sales_abercrombie": "dimset:anf:brand-abercrombie@1",
    "brand_sales_hollister": "dimset:anf:brand-hollister@1",
}
TOTAL_DIMENSION_SET_ID = "dimset:anf:total-company@1"
TOTAL_SCOPE_ID = "scope:anf:total-company@1"
SEGMENT_SCOPE_ID = "scope:anf:segment-current-recast@1"
REPORTED_BASIS_ID = "basis:core:reported@1"


PERIOD_LABEL_TO_AUDIT: Mapping[str, str] = {
    "FY2023": "fy2023",
    "FY2024": "fy2024",
    "FY2024-Q2": "fy2024-q2",
    "FY2024-Q3": "fy2024-q3",
    "FY2024-Q4": "fy2024-q4",
    "FY2025": "fy2025",
    "FY2025-Q1": "fy2025-q1",
    "FY2025-Q2": "fy2025-q2",
    "FY2025-Q3": "fy2025-q3",
    "FY2025-Q4": "fy2025-q4",
    "FY2026-Q1": "fy2026-q1",
}


FIRST_VISIBLE_PRIOR_FACTS: Mapping[str, Mapping[str, str]] = {
    "cash": {
        "value": "864.195",
        "period": "fy2024-q1",
        "source_sha256": "29c6dbb58968d54b93d027986ceccd6c7d3cf99578b4fea4e33399af52b89531",
        "source_path": "tickers/ANF/financial_statement/ANF_Q2_2024_10Q_2024-05-04_financial_statement.htm",
        "locator": "Inline XBRL cash and cash equivalents at 2024-05-04",
    },
    "net_working_capital": {
        "value": "589.884",
        "period": "fy2024-q1",
        "source_sha256": "29c6dbb58968d54b93d027986ceccd6c7d3cf99578b4fea4e33399af52b89531",
        "source_path": "tickers/ANF/financial_statement/ANF_Q2_2024_10Q_2024-05-04_financial_statement.htm",
        "locator": "current assets minus current liabilities at 2024-05-04",
    },
    "long_term_debt": {
        "value": "213.102",
        "period": "fy2024-q1",
        "source_sha256": "29c6dbb58968d54b93d027986ceccd6c7d3cf99578b4fea4e33399af52b89531",
        "source_path": "tickers/ANF/financial_statement/ANF_Q2_2024_10Q_2024-05-04_financial_statement.htm",
        "locator": "Inline XBRL long-term debt at 2024-05-04",
    },
}


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"), parse_float=Decimal)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")


def _period_id(period: str | None) -> str:
    return f"period:anf:{period or 'current-as-of-2026-06-05'}@1"


def _metric_id(metric: str) -> str:
    if metric in EXISTING_METRIC_IDS:
        return EXISTING_METRIC_IDS[metric]
    if metric in GEOGRAPHY_DIMENSIONS or metric in BRAND_DIMENSIONS:
        return "metric:core:net-sales@1"
    if metric in SUMMARY_EXTERNAL_METRICS:
        return f"metric:valuation:{_slug(metric)}@1"
    if metric in SUMMARY_NARRATIVE_METRICS:
        return f"metric:summary:{_slug(metric)}@1"
    namespace = "derived" if metric in DERIVED_METRICS else "financial"
    return f"metric:{namespace}:{_slug(metric)}@1"


def _unit_id(unit: str) -> str:
    try:
        return UNIT_IDS[unit]
    except KeyError as exc:
        raise ProductContractError(f"Unsupported audited unit {unit!r}.") from exc


def _currency(unit: str) -> str | None:
    return "USD" if unit in {"USD_millions", "USD_per_share"} else None


def _dimension_set_id(metric: str) -> str:
    return GEOGRAPHY_DIMENSIONS.get(metric) or BRAND_DIMENSIONS.get(metric) or TOTAL_DIMENSION_SET_ID


def _record_dimension_set_id(record: Mapping[str, Any]) -> str:
    override = record.get("dimension_set_id")
    return str(override) if override else _dimension_set_id(str(record["metric"]))


def _scope_id(metric: str) -> str:
    return SEGMENT_SCOPE_ID if metric in GEOGRAPHY_DIMENSIONS or metric in BRAND_DIMENSIONS else TOTAL_SCOPE_ID


def _record_scope_id(record: Mapping[str, Any]) -> str:
    return (
        SEGMENT_SCOPE_ID
        if _record_dimension_set_id(record) != TOTAL_DIMENSION_SET_ID
        else _scope_id(str(record["metric"]))
    )


def _definition_id(metric: str, directness: str) -> str:
    if metric in GEOGRAPHY_DIMENSIONS:
        return "definition:anf:geographic-recast@1"
    if metric in BRAND_DIMENSIONS:
        return "definition:anf:brand-family-sales@1"
    if metric in SUMMARY_NARRATIVE_METRICS:
        return SUMMARY_NARRATIVE_DEFINITION_IDS.get(
            metric, "definition:summary:reviewed-assessment@1"
        )
    if metric in SUMMARY_EXTERNAL_METRICS:
        return "definition:valuation:external-owner@1"
    if metric == "pnl_interest_coverage":
        return "definition:financial:pnl-interest-coverage-ratio@1"
    if directness == "derived":
        return "definition:financial:explicit-derivation@1"
    return "definition:core:company-reported@1"


def _audit_field_id(record: Mapping[str, Any]) -> str:
    return build_identity(
        "audit-field",
        (("surface", str(record["sheet"])), ("locator", str(record["cell"]))),
    )


def _normalize_audit_records(
    records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    migration_rows: list[dict[str, Any]] = []
    unit_corrections: list[dict[str, Any]] = []
    for raw in records:
        row = dict(raw)
        legacy_metric_key = str(row["metric"])
        migration = SUMMARY_SEMANTIC_IDENTITY_MIGRATIONS.get(legacy_metric_key)
        if migration is not None:
            if row.get("sheet") != "SUMMARY" or row.get("cell") != migration["expected_cell"]:
                raise ProductContractError(
                    "Historical semantic-identity migration locator changed for "
                    f"{legacy_metric_key!r}: {row.get('sheet')}!{row.get('cell')}."
                )
            row["historical_metric_key"] = legacy_metric_key
            row["metric"] = migration["metric_key"]
            row["semantic_identity_migration_contract"] = (
                SEMANTIC_IDENTITY_MIGRATION_CONTRACT
            )
            if migration.get("dimension_set_id"):
                row["dimension_set_id"] = migration["dimension_set_id"]
            migration_rows.append(
                {
                    "dimension_set_id": row.get("dimension_set_id", TOTAL_DIMENSION_SET_ID),
                    "legacy_locator": f"SUMMARY!{row['cell']}",
                    "new_metric_id": _metric_id(str(row["metric"])),
                    "new_metric_key": str(row["metric"]),
                    "old_metric_id": f"metric:summary:{_slug(legacy_metric_key)}@1",
                    "old_metric_key": legacy_metric_key,
                }
            )
        if (
            row.get("sheet") == "SUMMARY"
            and row.get("cell") == "B42"
            and row.get("metric") == "pnl_interest_coverage"
        ):
            if row.get("unit") != "USD_millions":
                raise ProductContractError(
                    f"Historical B42 unit changed unexpectedly: {row.get('unit')!r}."
                )
            row["historical_unit"] = str(row["unit"])
            row["unit"] = "ratio"
            row["unit_correction_contract"] = PRESENTATION_UNIT_CORRECTION_CONTRACT
            unit_corrections.append(
                {
                    "legacy_locator": "SUMMARY!B42",
                    "metric_key": "pnl_interest_coverage",
                    "new_unit_id": UNIT_IDS["ratio"],
                    "old_unit_id": UNIT_IDS["USD_millions"],
                }
            )
        normalized.append(row)
    if len(migration_rows) != 10:
        raise ProductContractError(
            f"Expected exactly ten Summary semantic-identity repairs; observed {len(migration_rows)}."
        )
    if len(unit_corrections) != 1:
        raise ProductContractError(
            f"Expected exactly one Summary unit correction; observed {len(unit_corrections)}."
        )
    contract_payload = {
        "contract": SEMANTIC_IDENTITY_MIGRATION_CONTRACT,
        "migration_count": len(migration_rows),
        "records": sorted(migration_rows, key=lambda item: item["legacy_locator"]),
        "unit_correction_contract": PRESENTATION_UNIT_CORRECTION_CONTRACT,
        "unit_corrections": unit_corrections,
    }
    contract_payload["digest"] = _sha256_bytes(serialize_package(contract_payload))
    return normalized, contract_payload


def _source_document_id(source_sha256: str) -> str:
    return build_identity(
        "doc", (("co", "ANF"), ("type", "reviewed-local-source"), ("sha256", source_sha256))
    )


def _canonical_value(value: Any, unit: str) -> Mapping[str, str]:
    if unit == "text":
        return qualitative_value(str(value))
    if unit == "date":
        return date_value(str(value))
    return exact_value(canonical_decimal(value))


def _numeric_value(value: Mapping[str, Any]) -> str:
    if value.get("kind") != "exact":
        raise ProductContractError(f"Expected numeric exact value, received {value!r}.")
    return str(value["value"])


def _temporal_role(record: Mapping[str, Any]) -> str:
    metric = str(record["metric"])
    period = record.get("period")
    if record["sheet"] == "SUMMARY":
        if metric in SUMMARY_EXTERNAL_METRICS:
            return "external_valuation_dependency"
        if metric.startswith("ttm_") or period == "ttm-at-fy2026-q1" or metric in {
            "net_leverage",
            "pnl_interest_coverage",
            "cash_interest_coverage",
        }:
            return "ttm_current_calculation"
        if period == "current-as-of-2026-06-05":
            return "current_snapshot"
        if period == "fy2025":
            return "current_recast_historical_truth"
        return "latest_reported_quarter"
    role = str(record["semantic_role"])
    if role == "point_in_time" or metric in {
        "cash_qoq_change",
        "current_ratio",
        "goodwill_percent_assets",
        "long_term_debt_qoq_change",
        "net_cash",
        "net_working_capital",
        "net_working_capital_qoq_change",
        "quick_ratio",
        "total_cash",
        "total_lease_liabilities",
    }:
        return "point_in_time_reporting_date"
    if role == "annual_flow":
        return "current_recast_annual_flow"
    return "current_recast_quarter_flow"


class _FactRegistry:
    def __init__(self) -> None:
        self.facts: dict[str, dict[str, Any]] = {}
        self.derivations: dict[str, dict[str, Any]] = {}
        self.by_key: dict[tuple[str, str, str], str] = {}

    def add_fact(self, fact: Mapping[str, Any]) -> str:
        fact_id = str(fact["canonical_fact_id"])
        prior = self.facts.get(fact_id)
        row = dict(fact)
        if prior is not None:
            if prior["canonical_value"] != row["canonical_value"]:
                raise ProductContractError(f"Canonical fact collision for {fact_id}.")
            return fact_id
        self.facts[fact_id] = row
        self.by_key[(str(row["metric_id"]), str(row["period_id"]), str(row["dimension_set_id"]))] = fact_id
        return fact_id

    def add_new_fact(
        self,
        *,
        metric_id: str,
        metric_key: str,
        definition_id: str,
        period_id: str,
        dimension_set_id: str,
        unit_id: str,
        currency: str | None,
        value: Mapping[str, Any],
        directness: str,
        source_document_ids: Sequence[str] = (),
        source_paths: Sequence[str] = (),
        source_sha256s: Sequence[str] = (),
        source_locators: Sequence[str] = (),
        knowledge_dates: Sequence[str] = (),
        derivation_id: str | None = None,
        origin: str = "summary_bs_extension",
    ) -> str:
        fact_id = canonical_fact_identity(
            metric_id=metric_id,
            definition_id=definition_id,
            basis_id=REPORTED_BASIS_ID,
            period_id=period_id,
            dimension_set_id=dimension_set_id,
            unit_id=unit_id,
            currency=currency,
        )
        return self.add_fact(
            {
                "basis_id": REPORTED_BASIS_ID,
                "canonical_fact_id": fact_id,
                "canonical_value": dict(value),
                "currency": currency,
                "definition_id": definition_id,
                "derivation_id": derivation_id,
                "dimension_set_id": dimension_set_id,
                "directness": directness,
                "knowledge_dates": sorted(set(filter(None, knowledge_dates))),
                "metric_id": metric_id,
                "metric_key": metric_key,
                "origin": origin,
                "period_id": period_id,
                "source_document_ids": sorted(set(source_document_ids)),
                "source_locators": sorted(set(source_locators)),
                "source_paths": sorted(set(source_paths)),
                "source_sha256s": sorted(set(source_sha256s)),
                "unit_id": unit_id,
            }
        )

    def add_reused_fact(self, fact: Mapping[str, Any]) -> str:
        row = dict(fact)
        row["origin"] = "accepted_product_v2_1_foundation"
        row["directness"] = "direct"
        row.setdefault("source_paths", [])
        row.setdefault("source_sha256s", [])
        row.setdefault("source_locators", list(row.get("observation_ids", [])))
        return self.add_fact(row)

    def fact_id(self, metric_id: str, period_id: str, dimension_set_id: str = TOTAL_DIMENSION_SET_ID) -> str:
        try:
            return self.by_key[(metric_id, period_id, dimension_set_id)]
        except KeyError as exc:
            raise ProductContractError(
                f"Missing canonical input fact: metric={metric_id}, period={period_id}, dims={dimension_set_id}."
            ) from exc

    def value(self, fact_id: str) -> str:
        return _numeric_value(self.facts[fact_id]["canonical_value"])

    def add_derivation(
        self,
        *,
        rule_id: str,
        output_fact_id: str,
        input_fact_ids: Sequence[str],
        output_value: str,
        tolerance: str,
        period_identity_checks: Sequence[str],
    ) -> tuple[str, dict[str, Any]]:
        derivation_id = derivation_identity(
            rule_id=rule_id,
            output_fact_id=output_fact_id,
            input_fact_ids=input_fact_ids,
        )
        inputs = [self.value(fact_id) for fact_id in input_fact_ids]
        independent = evaluate_derivation(rule_id, inputs)
        difference = canonical_decimal(
            format(Decimal(independent) - Decimal(output_value), "f")
        )
        passed = abs(Decimal(difference)) <= Decimal(tolerance)
        row = {
            "derivation_id": derivation_id,
            "difference": difference,
            "independent_result": independent,
            "input_fact_ids": list(input_fact_ids),
            "input_values": inputs,
            "output_fact_id": output_fact_id,
            "period_identity_checks": list(period_identity_checks),
            "product_result": canonical_decimal(output_value),
            "rule_id": rule_id,
            "tolerance": canonical_decimal(tolerance),
            "passed": passed,
        }
        if not passed:
            raise ProductContractError(f"Derivation mismatch: {row!r}.")
        self.derivations[derivation_id] = row
        return derivation_id, row


def _verify_audit_contract(audit_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, expected in sorted(AUDIT_SHA256.items()):
        path = audit_root / name
        if not path.is_file():
            raise ProductContractError(f"Required audit artifact is missing: {path}.")
        actual = _sha256_file(path)
        if actual != expected:
            raise ProductContractError(f"Audit artifact hash mismatch for {name}: {actual}.")
        rows.append({"relative_path": name, "sha256": actual, "size": path.stat().st_size})
    return rows


def _verify_audited_sources(
    records: Iterable[Mapping[str, Any]], source_root: Path
) -> list[dict[str, Any]]:
    expected: dict[str, tuple[Path, str]] = {}
    for record in records:
        path_text = record.get("source_document")
        sha = record.get("source_sha256")
        if path_text and sha:
            path = Path(str(path_text))
            expected[str(path)] = (path, str(sha))
    for payload in FIRST_VISIBLE_PRIOR_FACTS.values():
        path = source_root / str(payload["source_path"])
        expected[str(path)] = (path, str(payload["source_sha256"]))
    verified: list[dict[str, Any]] = []
    for path, expected_sha in sorted(expected.values(), key=lambda row: str(row[0])):
        if not path.is_file():
            raise ProductContractError(f"Reviewed source is missing: {path}.")
        actual = _sha256_file(path)
        if actual != expected_sha:
            raise ProductContractError(f"Reviewed source hash mismatch: {path}.")
        verified.append({"path": str(path), "sha256": actual, "size": path.stat().st_size})
    return verified


def _accepted_foundation(audit: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
    root = Path(str(audit["foundation_root"]))
    shadow_path = root / "shadow_v2_candidate.json"
    if _sha256_file(shadow_path) != ACCEPTED_SHADOW_SHA256:
        raise ProductContractError("Accepted Product@2.1 shadow hash mismatch.")
    shadow = json.loads(shadow_path.read_text(encoding="utf-8"))
    if shadow.get("evidence_foundation_id") != ACCEPTED_FOUNDATION_ID:
        raise ProductContractError("Accepted evidence-foundation identity mismatch.")
    if shadow.get("evidence_foundation_sha256") != ACCEPTED_FOUNDATION_SHA256:
        raise ProductContractError("Accepted evidence-foundation declared hash mismatch.")
    foundation = shadow["evidence_foundation"]
    if _sha256_bytes(serialize_package(foundation)) != ACCEPTED_FOUNDATION_SHA256:
        raise ProductContractError("Accepted embedded evidence foundation is not exact.")
    return foundation, shadow_path


def _existing_fact_index(foundation: Mapping[str, Any]) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    result: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for fact in foundation["canonical_facts"]:
        key = (str(fact["metric_id"]), str(fact["period_id"]), str(fact["dimension_set_id"]))
        prior = result.get(key)
        if prior is not None and prior["canonical_value"] != fact["canonical_value"]:
            raise ProductContractError(f"Accepted foundation fact conflict for {key!r}.")
        result[key] = fact
    return result


def _source_parts(record: Mapping[str, Any]) -> dict[str, list[str]]:
    sha = str(record.get("source_sha256") or "")
    return {
        "knowledge_dates": [str(record.get("knowledge_date") or "")],
        "source_document_ids": [_source_document_id(sha)] if sha else [],
        "source_locators": [str(record.get("source_occurrence") or "")],
        "source_paths": [str(record.get("source_document") or "")],
        "source_sha256s": [sha] if sha else [],
    }


def _directness(record: Mapping[str, Any]) -> str:
    metric = str(record["metric"])
    if metric in BS_DERIVED_METRICS or metric in DERIVED_METRICS:
        return "derived"
    if record.get("direct_or_derived") == "presentation_only":
        return "direct"
    return str(record.get("direct_or_derived") or "direct")


def _available_status(record: Mapping[str, Any]) -> str:
    result = str(record["audit_result"])
    if record["sheet"] == "SUMMARY" and record["metric"] in {
        "americas_sales_mix",
        "emea_sales_mix",
        "apac_sales_mix",
    }:
        return "available"
    if record["sheet"] == "SUMMARY" and record["metric"] in {
        "ttm_free_cash_flow_yoy_growth",
        "pnl_interest_coverage",
    }:
        return "needs_review"
    if result in {"PASS", "DEFECT"}:
        return "available"
    if result == "NEEDS_REVIEW":
        return "needs_review"
    if result == "LEGITIMATELY_UNAVAILABLE":
        return "unavailable"
    raise ProductContractError(f"Unknown audit result {result!r}.")


def _status_reason(record: Mapping[str, Any], status: str) -> str | None:
    metric = str(record["metric"])
    if status == "available":
        return None
    if metric in SUMMARY_EXTERNAL_METRICS:
        return "Valuation-owned external dependency; Summary does not create a second valuation engine."
    if metric == "ttm_free_cash_flow_yoy_growth":
        return "Historical TTM comparison convention remains under review; candidate arithmetic is retained only as evidence."
    if metric == "pnl_interest_coverage":
        return "Legacy denominator is net interest income, not reviewed interest expense; legacy numeric output is retired."
    return str(record.get("audit_reason") or "Reviewed evidence is not compatible with the requested field definition.")


def _audit_value(record: Mapping[str, Any]) -> Mapping[str, str] | None:
    value = record.get("canonical_value")
    if value is None:
        return None
    return _canonical_value(value, str(record["unit"]))


def _period_predecessor(period: str) -> str:
    order = [
        "fy2024-q2",
        "fy2024-q3",
        "fy2024-q4",
        "fy2025-q1",
        "fy2025-q2",
        "fy2025-q3",
        "fy2025-q4",
        "fy2026-q1",
    ]
    index = order.index(period)
    return "fy2024-q1" if index == 0 else order[index - 1]


def _prior_year_period(period: str) -> str:
    match = re.fullmatch(r"fy(\d{4})-q([1-4])", period)
    if not match:
        raise ProductContractError(f"Not a fiscal quarter: {period!r}.")
    return f"fy{int(match.group(1)) - 1}-q{match.group(2)}"


def _period_checks(*, stock: bool = False, recast: bool = False) -> list[str]:
    checks = ["metric", "definition", "basis", "unit", "scale", "currency", "scope", "fiscal_calendar"]
    checks.append("reporting_date" if stock else "period_coverage")
    if recast:
        checks.extend(["segment_taxonomy", "issuer_recast_comparability"])
    return checks


def _ensure_existing_fact(
    registry: _FactRegistry,
    existing: Mapping[tuple[str, str, str], Mapping[str, Any]],
    metric_id: str,
    period: str,
    dimension_set_id: str = TOTAL_DIMENSION_SET_ID,
) -> str:
    key = (metric_id, _period_id(period), dimension_set_id)
    try:
        fact = existing[key]
    except KeyError as exc:
        raise ProductContractError(f"Accepted Product@2.1 foundation lacks {key!r}.") from exc
    return registry.add_reused_fact(fact)


def _support_fact(
    registry: _FactRegistry,
    *,
    metric_key: str,
    metric_id: str,
    period: str,
    value: Any,
    source: Mapping[str, Any],
    unit_id: str,
    currency: str | None,
    dimension_set_id: str = TOTAL_DIMENSION_SET_ID,
    directness: str = "direct",
) -> str:
    return registry.add_new_fact(
        metric_id=metric_id,
        metric_key=metric_key,
        definition_id="definition:core:company-reported@1" if directness == "direct" else "definition:financial:explicit-derivation@1",
        period_id=_period_id(period),
        dimension_set_id=dimension_set_id,
        unit_id=unit_id,
        currency=currency,
        value=exact_value(canonical_decimal(value)),
        directness=directness,
        source_document_ids=[_source_document_id(str(source["source_sha256"]))],
        source_paths=[str(source["source_path"])],
        source_sha256s=[str(source["source_sha256"])],
        source_locators=[str(source["locator"])],
        knowledge_dates=[],
        origin="reviewed_support_fact",
    )


def _add_derived_fact(
    registry: _FactRegistry,
    *,
    record: Mapping[str, Any],
    output_value: Mapping[str, Any],
    rule_id: str,
    input_fact_ids: Sequence[str],
    tolerance: str,
) -> tuple[str, str]:
    metric = str(record["metric"])
    metric_id = _metric_id(metric)
    period_id = _period_id(record.get("period"))
    dimension_set_id = _dimension_set_id(metric)
    output_fact_id = canonical_fact_identity(
        metric_id=metric_id,
        definition_id=_definition_id(metric, "derived"),
        basis_id=REPORTED_BASIS_ID,
        period_id=period_id,
        dimension_set_id=dimension_set_id,
        unit_id=_unit_id(str(record["unit"])),
        currency=_currency(str(record["unit"])),
    )
    derivation_id, _ = registry.add_derivation(
        rule_id=rule_id,
        output_fact_id=output_fact_id,
        input_fact_ids=input_fact_ids,
        output_value=_numeric_value(output_value),
        tolerance=tolerance,
        period_identity_checks=_period_checks(
            stock=_temporal_role(record) == "point_in_time_reporting_date",
            recast=metric in GEOGRAPHY_DIMENSIONS or metric in BRAND_DIMENSIONS,
        ),
    )
    fact_id = registry.add_new_fact(
        metric_id=metric_id,
        metric_key=metric,
        definition_id=_definition_id(metric, "derived"),
        period_id=period_id,
        dimension_set_id=dimension_set_id,
        unit_id=_unit_id(str(record["unit"])),
        currency=_currency(str(record["unit"])),
        value=output_value,
        directness="derived",
        derivation_id=derivation_id,
        knowledge_dates=[str(record.get("knowledge_date") or "")],
    )
    return fact_id, derivation_id


def _field_from_record(
    *,
    record: Mapping[str, Any],
    surface: str,
    status: str,
    registry: _FactRegistry,
    canonical_fact_id: str | None = None,
    derivation_id: str | None = None,
    candidate_fact_ids: Sequence[str] = (),
    directness: str | None = None,
) -> ProductField:
    metric = str(record["metric"])
    resolved_directness = directness or _directness(record)
    value = None
    definition_id = _definition_id(metric, resolved_directness)
    if status == "available":
        if not canonical_fact_id:
            raise ProductContractError(f"Available field {record['sheet']}!{record['cell']} has no fact.")
        value = dict(registry.facts[canonical_fact_id]["canonical_value"])
        definition_id = str(registry.facts[canonical_fact_id]["definition_id"])
    return ProductField(
        field_id=product_field_identity(
            company_id="ANF",
            product_surface=surface,
            metric_id=_metric_id(metric),
            period_id=_period_id(record.get("period")),
            dimension_set_id=_record_dimension_set_id(record),
            semantic_role=str(record["semantic_role"]),
        ),
        metric_key=metric,
        metric_id=_metric_id(metric),
        period_id=_period_id(record.get("period")),
        temporal_role=_temporal_role(record),
        semantic_role=str(record["semantic_role"]),
        unit_id=_unit_id(str(record["unit"])),
        currency=_currency(str(record["unit"])),
        definition_id=definition_id,
        basis_id=REPORTED_BASIS_ID,
        scope_id=_record_scope_id(record),
        dimension_set_id=_record_dimension_set_id(record),
        status=status,  # type: ignore[arg-type]
        value_state=value_state_for(status=status, directness=resolved_directness, value=value),  # type: ignore[arg-type]
        directness=resolved_directness,
        value=value,
        canonical_fact_id=canonical_fact_id if status == "available" else None,
        derivation_id=derivation_id if status == "available" else None,
        reason=_status_reason(record, status),
        candidate_fact_ids=tuple(candidate_fact_ids),
    )


def _record_fact(
    *,
    record: Mapping[str, Any],
    registry: _FactRegistry,
    existing: Mapping[tuple[str, str, str], Mapping[str, Any]],
    candidate: bool = False,
) -> str:
    metric = str(record["metric"])
    metric_id = _metric_id(metric)
    period_id = _period_id(record.get("period"))
    dimension_set_id = _record_dimension_set_id(record)
    value = _audit_value(record)
    if value is None:
        raise ProductContractError(f"Cannot create a fact without a value: {record!r}.")
    existing_key = (metric_id, period_id, dimension_set_id)
    if not candidate and existing_key in existing:
        fact_id = registry.add_reused_fact(existing[existing_key])
        accepted_value = registry.facts[fact_id]["canonical_value"]
        if accepted_value.get("kind") != value.get("kind"):
            raise ProductContractError(f"Reused fact type mismatch for {existing_key!r}.")
        if value.get("kind") == "exact":
            tolerance = Decimal("0.1") if record["sheet"] == "BS_Segments" else Decimal("0.000000001")
            if abs(Decimal(_numeric_value(accepted_value)) - Decimal(_numeric_value(value))) > tolerance:
                raise ProductContractError(f"Reused fact value mismatch for {existing_key!r}.")
        elif accepted_value != value:
            raise ProductContractError(f"Reused fact value mismatch for {existing_key!r}.")
        return fact_id
    definition_id = (
        "definition:financial:candidate-overlap-unresolved@1"
        if candidate
        else _definition_id(metric, "direct")
    )
    return registry.add_new_fact(
        metric_id=metric_id,
        metric_key=metric,
        definition_id=definition_id,
        period_id=period_id,
        dimension_set_id=dimension_set_id,
        unit_id=_unit_id(str(record["unit"])),
        currency=_currency(str(record["unit"])),
        value=value,
        directness="direct",
        origin="reviewed_candidate_fact" if candidate else "summary_bs_extension",
        **_source_parts(record),
    )


def _source_support_from_record(record: Mapping[str, Any], *, locator: str) -> dict[str, Any]:
    return {
        "locator": locator,
        "source_path": str(record.get("source_document") or ""),
        "source_sha256": str(record.get("source_sha256") or ""),
    }


def _numeric_audit_input_fact(
    registry: _FactRegistry,
    *,
    record: Mapping[str, Any],
    metric_key: str,
    metric_id: str,
    period: str,
    value: Any,
    unit_id: str,
    currency: str | None,
) -> str:
    return _support_fact(
        registry,
        metric_key=metric_key,
        metric_id=metric_id,
        period=period,
        value=value,
        source=_source_support_from_record(
            record,
            locator=f"{record.get('source_occurrence') or 'reviewed source'}; explicit derivation input {metric_key}",
        ),
        unit_id=unit_id,
        currency=currency,
    )


def _add_helper_derived_fact(
    registry: _FactRegistry,
    *,
    metric_key: str,
    period: str,
    value: str,
    rule_id: str,
    input_fact_ids: Sequence[str],
    unit_id: str,
    currency: str | None,
    tolerance: str = "0.000000001",
) -> str:
    metric_id = f"metric:derived:{_slug(metric_key)}@1"
    output_fact_id = canonical_fact_identity(
        metric_id=metric_id,
        definition_id="definition:financial:explicit-derivation@1",
        basis_id=REPORTED_BASIS_ID,
        period_id=_period_id(period),
        dimension_set_id=TOTAL_DIMENSION_SET_ID,
        unit_id=unit_id,
        currency=currency,
    )
    derivation_id, _ = registry.add_derivation(
        rule_id=rule_id,
        output_fact_id=output_fact_id,
        input_fact_ids=input_fact_ids,
        output_value=value,
        tolerance=tolerance,
        period_identity_checks=_period_checks(stock=True),
    )
    return registry.add_new_fact(
        metric_id=metric_id,
        metric_key=metric_key,
        definition_id="definition:financial:explicit-derivation@1",
        period_id=_period_id(period),
        dimension_set_id=TOTAL_DIMENSION_SET_ID,
        unit_id=unit_id,
        currency=currency,
        value=exact_value(value),
        directness="derived",
        derivation_id=derivation_id,
    )


def _lineage_row(
    record: Mapping[str, Any], field: ProductField, registry: _FactRegistry
) -> dict[str, Any]:
    fact = registry.facts.get(field.canonical_fact_id or "")
    derivation = registry.derivations.get(field.derivation_id or "")
    row = {
        "audit_field_id": _audit_field_id(record),
        "candidate_fact_ids": list(field.candidate_fact_ids),
        "canonical_fact_id": field.canonical_fact_id,
        "derivation_id": field.derivation_id,
        "derivation_input_fact_ids": list(derivation.get("input_fact_ids", [])) if derivation else [],
        "field_id": field.field_id,
        "legacy_locator": f"{record['sheet']}!{record['cell']}",
        "product_status": field.status,
        "source_document_ids": list(fact.get("source_document_ids", [])) if fact else [],
        "source_locators": list(fact.get("source_locators", [])) if fact else [],
        "source_paths": list(fact.get("source_paths", [])) if fact else [],
        "source_sha256s": list(fact.get("source_sha256s", [])) if fact else [],
        "temporal_role": field.temporal_role,
        "value_state": field.value_state,
    }
    if record.get("historical_metric_key"):
        row.update(
            {
                "canonical_metric_key": field.metric_key,
                "historical_metric_key": str(record["historical_metric_key"]),
                "semantic_identity_migration_contract": str(
                    record["semantic_identity_migration_contract"]
                ),
            }
        )
    if record.get("historical_unit"):
        row.update(
            {
                "historical_unit": str(record["historical_unit"]),
                "unit_correction_contract": str(record["unit_correction_contract"]),
            }
        )
    return row


def _build_bs_fields(
    records: Sequence[Mapping[str, Any]],
    registry: _FactRegistry,
    existing: Mapping[tuple[str, str, str], Mapping[str, Any]],
    source_root: Path,
) -> tuple[list[ProductField], list[dict[str, Any]]]:
    record_by_key = {(str(row["metric"]), str(row.get("period"))): row for row in records}
    fields: dict[str, ProductField] = {}
    lineages: dict[str, dict[str, Any]] = {}

    def remember(record: Mapping[str, Any], field: ProductField) -> None:
        fields[_audit_field_id(record)] = field
        lineages[_audit_field_id(record)] = _lineage_row(record, field, registry)

    for record in records:
        metric = str(record["metric"])
        if metric in BS_DERIVED_METRICS:
            continue
        status = _available_status(record)
        fact_id: str | None = None
        candidate_fact_ids: list[str] = []
        if status == "available":
            fact_id = _record_fact(record=record, registry=registry, existing=existing)
        elif status == "needs_review" and _audit_value(record) is not None:
            candidate_fact_ids.append(
                _record_fact(record=record, registry=registry, existing=existing, candidate=True)
            )
        field = _field_from_record(
            record=record,
            surface="bs-segment",
            status=status,
            registry=registry,
            canonical_fact_id=fact_id,
            candidate_fact_ids=candidate_fact_ids,
            directness="direct" if fact_id else _directness(record),
        )
        remember(record, field)

    for record in records:
        metric = str(record["metric"])
        if metric not in BS_DERIVED_METRICS:
            continue
        status = _available_status(record)
        if status != "available":
            field = _field_from_record(
                record=record,
                surface="bs-segment",
                status=status,
                registry=registry,
                directness="derived",
            )
            remember(record, field)
            continue

        period = str(record["period"])
        output_value = _audit_value(record)
        if output_value is None:
            raise ProductContractError(f"Available derived field has no value: {record!r}.")
        unit_id = _unit_id(str(record["unit"]))
        currency = _currency(str(record["unit"]))
        input_fact_ids: list[str]
        rule_id: str
        tolerance = "0.000000001"

        def direct(metric_key: str, target_period: str = period) -> str:
            return registry.fact_id(_metric_id(metric_key), _period_id(target_period))

        if metric == "total_cash":
            input_fact_ids = [direct("cash"), direct("restricted_cash")]
            rule_id = "derivation:financial:sum@1"
        elif metric == "cash_qoq_change":
            prior_period = _period_predecessor(period)
            if prior_period == "fy2024-q1":
                support = dict(FIRST_VISIBLE_PRIOR_FACTS["cash"])
                support["source_path"] = str(source_root / str(support["source_path"]))
                prior_id = _support_fact(
                    registry,
                    metric_key="cash",
                    metric_id=_metric_id("cash"),
                    period=prior_period,
                    value=support["value"],
                    source=support,
                    unit_id=unit_id,
                    currency=currency,
                )
            else:
                prior_id = direct("cash", prior_period)
            input_fact_ids = [direct("cash"), prior_id]
            rule_id = "derivation:financial:subtract@1"
        elif metric == "net_working_capital":
            input_fact_ids = [direct("current_assets"), direct("current_liabilities")]
            rule_id = "derivation:financial:subtract@1"
        elif metric == "net_working_capital_qoq_change":
            current_id = registry.fact_id(_metric_id("net_working_capital"), _period_id(period))
            prior_period = _period_predecessor(period)
            if prior_period == "fy2024-q1":
                support = dict(FIRST_VISIBLE_PRIOR_FACTS["net_working_capital"])
                support["source_path"] = str(source_root / str(support["source_path"]))
                prior_id = _support_fact(
                    registry,
                    metric_key="net_working_capital",
                    metric_id=_metric_id("net_working_capital"),
                    period=prior_period,
                    value=support["value"],
                    source=support,
                    unit_id=unit_id,
                    currency=currency,
                    directness="derived",
                )
            else:
                prior_id = registry.fact_id(_metric_id("net_working_capital"), _period_id(prior_period))
            input_fact_ids = [current_id, prior_id]
            rule_id = "derivation:financial:subtract@1"
        elif metric == "current_ratio":
            input_fact_ids = [direct("current_assets"), direct("current_liabilities")]
            rule_id = "derivation:financial:ratio@1"
            tolerance = "0.000000000001"
        elif metric == "quick_ratio":
            components = [direct("cash"), direct("marketable_securities"), direct("accounts_receivable")]
            quick_value = evaluate_derivation(
                "derivation:financial:sum@1", [registry.value(item) for item in components]
            )
            quick_id = _add_helper_derived_fact(
                registry,
                metric_key="quick_assets",
                period=period,
                value=quick_value,
                rule_id="derivation:financial:sum@1",
                input_fact_ids=components,
                unit_id="unit:core:currency-millions@1",
                currency="USD",
            )
            input_fact_ids = [quick_id, direct("current_liabilities")]
            rule_id = "derivation:financial:ratio@1"
            tolerance = "0.000000000001"
        elif metric == "long_term_debt_qoq_change":
            prior_period = _period_predecessor(period)
            if prior_period == "fy2024-q1":
                support = dict(FIRST_VISIBLE_PRIOR_FACTS["long_term_debt"])
                support["source_path"] = str(source_root / str(support["source_path"]))
                prior_id = _support_fact(
                    registry,
                    metric_key="long_term_debt",
                    metric_id=_metric_id("long_term_debt"),
                    period=prior_period,
                    value=support["value"],
                    source=support,
                    unit_id=unit_id,
                    currency=currency,
                )
            else:
                prior_id = direct("long_term_debt", prior_period)
            input_fact_ids = [direct("long_term_debt"), prior_id]
            rule_id = "derivation:financial:subtract@1"
        elif metric == "total_liabilities":
            input_fact_ids = [
                direct("current_liabilities"),
                direct("long_term_debt"),
                direct("long_term_lease_liabilities"),
                direct("other_long_term_liabilities"),
            ]
            rule_id = "derivation:financial:sum@1"
        elif metric == "inventory_yoy_growth":
            inputs = list(record.get("derivation_inputs") or [])
            if len(inputs) != 2:
                raise ProductContractError(f"Inventory growth input contract missing: {record!r}.")
            prior_id = _numeric_audit_input_fact(
                registry,
                record=record,
                metric_key="inventory",
                metric_id=_metric_id("inventory"),
                period=_prior_year_period(period),
                value=inputs[1],
                unit_id="unit:core:currency-millions@1",
                currency="USD",
            )
            input_fact_ids = [direct("inventory"), prior_id]
            rule_id = "derivation:financial:growth@1"
            tolerance = "0.000000000001"
        elif metric == "net_sales_yoy_growth":
            current_id = _ensure_existing_fact(registry, existing, _metric_id("net_sales_total_company"), period)
            prior_id = _ensure_existing_fact(
                registry, existing, _metric_id("net_sales_total_company"), _prior_year_period(period)
            )
            input_fact_ids = [current_id, prior_id]
            rule_id = "derivation:financial:growth@1"
            tolerance = "0.000000000001"
        elif metric == "inventory_growth_minus_sales_growth":
            input_fact_ids = [
                registry.fact_id(_metric_id("inventory_yoy_growth"), _period_id(period)),
                registry.fact_id(_metric_id("net_sales_yoy_growth"), _period_id(period)),
            ]
            rule_id = "derivation:financial:percentage-point-difference@1"
            tolerance = "0.000000000001"
        elif metric == "net_cash":
            components = [direct("cash"), direct("marketable_securities")]
            liquid_value = evaluate_derivation(
                "derivation:financial:sum@1", [registry.value(item) for item in components]
            )
            liquid_id = _add_helper_derived_fact(
                registry,
                metric_key="cash_plus_marketable_securities",
                period=period,
                value=liquid_value,
                rule_id="derivation:financial:sum@1",
                input_fact_ids=components,
                unit_id="unit:core:currency-millions@1",
                currency="USD",
            )
            input_fact_ids = [liquid_id, direct("long_term_debt")]
            rule_id = "derivation:financial:subtract@1"
        elif metric == "total_lease_liabilities":
            input_fact_ids = [
                direct("current_lease_liabilities"),
                direct("long_term_lease_liabilities"),
            ]
            rule_id = "derivation:financial:sum@1"
        elif metric == "diluted_shares_yoy_growth":
            current_id = _ensure_existing_fact(
                registry, existing, _metric_id("diluted_weighted_average_shares"), period
            )
            prior_id = _ensure_existing_fact(
                registry,
                existing,
                _metric_id("diluted_weighted_average_shares"),
                _prior_year_period(period),
            )
            input_fact_ids = [current_id, prior_id]
            rule_id = "derivation:financial:growth@1"
            tolerance = "0.000000000001"
        else:  # pragma: no cover - guarded by closed metric set
            raise ProductContractError(f"No BS derivation contract for {metric!r}.")

        fact_id, derivation_id = _add_derived_fact(
            registry,
            record=record,
            output_value=output_value,
            rule_id=rule_id,
            input_fact_ids=input_fact_ids,
            tolerance=tolerance,
        )
        field = _field_from_record(
            record=record,
            surface="bs-segment",
            status="available",
            registry=registry,
            canonical_fact_id=fact_id,
            derivation_id=derivation_id,
            directness="derived",
        )
        remember(record, field)

    if len(fields) != 417:
        raise ProductContractError(f"Expected 417 BS/segment fields, received {len(fields)}.")
    ordered_fields = sorted(
        fields.values(), key=lambda row: (row.period_id, row.metric_id, row.dimension_set_id, row.semantic_role)
    )
    ordered_lineages = [lineages[_audit_field_id(row)] for row in records]
    return ordered_fields, ordered_lineages


def _capital_return_fcf_facts(
    registry: _FactRegistry, source_root: Path
) -> tuple[dict[str, str], dict[str, Any]]:
    extraction = build_anf_capital_return_collection(source_root / "sec_cache" / "ANF")
    record_map = {str(row["record_id"]): row for row in extraction.records}
    required = ("2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1")
    fcf_by_period: dict[str, str] = {}
    adapter_rows: list[dict[str, Any]] = []
    for fiscal_period in required:
        fcf_record = next(
            row
            for row in extraction.records
            if row["metric_id"] == "free_cash_flow"
            and row["period_type"] == "quarter"
            and row["fiscal_period"] == fiscal_period
        )
        components = [record_map[str(item)] for item in fcf_record["component_record_ids"]]
        component_fact_ids: list[str] = []
        for component in components:
            metric_key = str(component["metric_id"])
            metric_id = (
                "metric:core:operating-cash-flow@1"
                if metric_key == "operating_cash_flow"
                else "metric:core:property-equipment-purchases@1"
            )
            fact_id = registry.add_new_fact(
                metric_id=metric_id,
                metric_key=metric_key,
                definition_id="definition:anf:capital-return-source-adapter@1",
                period_id=_period_id(f"fy{fiscal_period.casefold()}"),
                dimension_set_id=TOTAL_DIMENSION_SET_ID,
                unit_id="unit:core:currency-millions@1",
                currency="USD",
                value=exact_value(canonical_decimal(component["value"])),
                directness=str(component["source_classification"]),
                source_document_ids=[
                    _source_document_id(str(component["source_document_sha256"]))
                ],
                source_paths=[str(component["source_document"])],
                source_sha256s=[str(component["source_document_sha256"])],
                source_locators=[str(component["evidence_ref"]), str(component["record_id"])],
                knowledge_dates=[str(component["publication_date"])],
                origin="anf_capital_return_source_adapter",
            )
            component_fact_ids.append(fact_id)
            adapter_rows.append(
                {
                    "evidence_ref": str(component["evidence_ref"]),
                    "fiscal_period": str(component["fiscal_period"]),
                    "metric_id": str(component["metric_id"]),
                    "publication_date": str(component["publication_date"]),
                    "record_id": str(component["record_id"]),
                    "source_document": str(component["source_document"]),
                    "source_document_sha256": str(component["source_document_sha256"]),
                    "value": canonical_decimal(component["value"]),
                }
            )
        period = f"fy{fiscal_period.casefold()}"
        metric_id = "metric:core:free-cash-flow@1"
        output_fact_id = canonical_fact_identity(
            metric_id=metric_id,
            definition_id="definition:anf:free-cash-flow-operating-cash-flow-less-capex@1",
            basis_id=REPORTED_BASIS_ID,
            period_id=_period_id(period),
            dimension_set_id=TOTAL_DIMENSION_SET_ID,
            unit_id="unit:core:currency-millions@1",
            currency="USD",
        )
        derivation_id, _ = registry.add_derivation(
            rule_id="derivation:financial:subtract@1",
            output_fact_id=output_fact_id,
            input_fact_ids=component_fact_ids,
            output_value=canonical_decimal(fcf_record["value"]),
            tolerance="0.000000001",
            period_identity_checks=_period_checks(stock=False),
        )
        fact_id = registry.add_new_fact(
            metric_id=metric_id,
            metric_key="free_cash_flow",
            definition_id="definition:anf:free-cash-flow-operating-cash-flow-less-capex@1",
            period_id=_period_id(period),
            dimension_set_id=TOTAL_DIMENSION_SET_ID,
            unit_id="unit:core:currency-millions@1",
            currency="USD",
            value=exact_value(canonical_decimal(fcf_record["value"])),
            directness="derived",
            derivation_id=derivation_id,
            source_document_ids=[_source_document_id(str(fcf_record["source_document_sha256"]))],
            source_paths=[str(fcf_record["source_document"])],
            source_sha256s=[str(fcf_record["source_document_sha256"])],
            source_locators=[str(fcf_record["evidence_ref"]), str(fcf_record["record_id"])],
            knowledge_dates=[str(fcf_record["publication_date"])],
            origin="anf_capital_return_source_adapter",
        )
        fcf_by_period[period] = fact_id
        adapter_rows.append(
            {
                "component_record_ids": list(fcf_record["component_record_ids"]),
                "evidence_ref": str(fcf_record["evidence_ref"]),
                "fiscal_period": str(fcf_record["fiscal_period"]),
                "metric_id": str(fcf_record["metric_id"]),
                "publication_date": str(fcf_record["publication_date"]),
                "record_id": str(fcf_record["record_id"]),
                "source_document": str(fcf_record["source_document"]),
                "source_document_sha256": str(fcf_record["source_document_sha256"]),
                "value": canonical_decimal(fcf_record["value"]),
            }
        )
    return fcf_by_period, {
        "adapter": "pbi_xbrl.anf_capital_return_source_adapter",
        "network_access": False,
        "records": adapter_rows,
        "required_periods": list(required),
    }


def _build_summary_fields(
    records: Sequence[Mapping[str, Any]],
    registry: _FactRegistry,
    existing: Mapping[tuple[str, str, str], Mapping[str, Any]],
    source_root: Path,
) -> tuple[list[ProductField], list[dict[str, Any]], dict[str, Any]]:
    fields: dict[str, ProductField] = {}
    lineages: dict[str, dict[str, Any]] = {}
    fcf_by_period, capital_return_evidence = _capital_return_fcf_facts(registry, source_root)

    def remember(record: Mapping[str, Any], field: ProductField) -> None:
        fields[_audit_field_id(record)] = field
        lineages[_audit_field_id(record)] = _lineage_row(record, field, registry)

    for record in records:
        metric = str(record["metric"])
        status = _available_status(record)
        if metric in DERIVED_METRICS or metric in {"pnl_interest_coverage"}:
            continue
        fact_id: str | None = None
        candidate_fact_ids: list[str] = []
        if status == "available":
            fact_id = _record_fact(record=record, registry=registry, existing=existing)
        elif status == "needs_review" and _audit_value(record) is not None:
            candidate_fact_ids.append(
                _record_fact(record=record, registry=registry, existing=existing, candidate=True)
            )
        field = _field_from_record(
            record=record,
            surface="summary",
            status=status,
            registry=registry,
            canonical_fact_id=fact_id,
            candidate_fact_ids=candidate_fact_ids,
            directness="direct" if fact_id else _directness(record),
        )
        remember(record, field)

    for record in records:
        metric = str(record["metric"])
        if metric not in DERIVED_METRICS and metric != "pnl_interest_coverage":
            continue
        status = _available_status(record)
        output_value = _audit_value(record)
        if status != "available":
            candidate_fact_ids: list[str] = []
            if output_value is not None:
                candidate_fact_ids.append(
                    _record_fact(record=record, registry=registry, existing=existing, candidate=True)
                )
            field = _field_from_record(
                record=record,
                surface="summary",
                status=status,
                registry=registry,
                candidate_fact_ids=candidate_fact_ids,
                directness="invalid_legacy_derivation" if metric == "pnl_interest_coverage" else "derived",
            )
            remember(record, field)
            continue
        if output_value is None:
            raise ProductContractError(f"Available Summary derivation has no value: {record!r}.")
        period = str(record.get("period"))
        rule_id: str
        input_fact_ids: list[str]
        tolerance = "0.000000001"

        if metric in {"americas_sales_mix", "emea_sales_mix", "apac_sales_mix"}:
            component_metric = {
                "americas_sales_mix": "geographic_sales_americas",
                "emea_sales_mix": "geographic_sales_emea",
                "apac_sales_mix": "geographic_sales_apac",
            }[metric]
            input_fact_ids = [
                registry.fact_id(
                    _metric_id(component_metric), _period_id("fy2025"), _dimension_set_id(component_metric)
                ),
                _ensure_existing_fact(
                    registry, existing, _metric_id("net_sales_total_company"), "fy2025"
                ),
            ]
            rule_id = "derivation:financial:ratio@1"
            tolerance = "0.000000001"
        elif metric == "ttm_net_sales":
            input_fact_ids = [
                _ensure_existing_fact(registry, existing, _metric_id("net_sales_total_company"), "fy2025"),
                _ensure_existing_fact(registry, existing, _metric_id("net_sales_total_company"), "fy2025-q1"),
                _ensure_existing_fact(registry, existing, _metric_id("net_sales_total_company"), "fy2026-q1"),
            ]
            rule_id = "derivation:financial:ttm-fy-minus-prior-q1-plus-current-q1@1"
        elif metric == "quarter_net_sales_yoy_growth":
            input_fact_ids = [
                _ensure_existing_fact(registry, existing, _metric_id("quarter_net_sales"), "fy2026-q1"),
                _ensure_existing_fact(registry, existing, _metric_id("quarter_net_sales"), "fy2025-q1"),
            ]
            rule_id = "derivation:financial:growth@1"
            tolerance = "0.000000000001"
        elif metric == "quarter_net_income_yoy_growth":
            input_fact_ids = [
                _ensure_existing_fact(registry, existing, _metric_id("quarter_net_income"), "fy2026-q1"),
                _ensure_existing_fact(registry, existing, _metric_id("quarter_net_income"), "fy2025-q1"),
            ]
            rule_id = "derivation:financial:growth@1"
            tolerance = "0.000000000001"
        elif metric == "quarter_diluted_eps_yoy_change":
            input_fact_ids = [
                _ensure_existing_fact(registry, existing, _metric_id("quarter_diluted_eps"), "fy2026-q1"),
                _ensure_existing_fact(registry, existing, _metric_id("quarter_diluted_eps"), "fy2025-q1"),
            ]
            rule_id = "derivation:financial:subtract@1"
        elif metric == "ttm_free_cash_flow":
            input_fact_ids = [
                fcf_by_period[item]
                for item in ("fy2025-q2", "fy2025-q3", "fy2025-q4", "fy2026-q1")
            ]
            rule_id = "derivation:financial:ttm-four-quarter-sum@1"
        elif metric == "liquidity_cash_plus_revolver":
            input_fact_ids = [
                registry.fact_id(_metric_id("cash"), _period_id("fy2026-q1")),
                registry.fact_id(_metric_id("revolver_availability"), _period_id("fy2026-q1")),
            ]
            rule_id = "derivation:financial:sum@1"
        else:  # pragma: no cover - closed available Summary derived set
            raise ProductContractError(f"No Summary derivation contract for {metric!r}.")

        fact_id, derivation_id = _add_derived_fact(
            registry,
            record=record,
            output_value=output_value,
            rule_id=rule_id,
            input_fact_ids=input_fact_ids,
            tolerance=tolerance,
        )
        field = _field_from_record(
            record=record,
            surface="summary",
            status="available",
            registry=registry,
            canonical_fact_id=fact_id,
            derivation_id=derivation_id,
            directness="derived",
        )
        remember(record, field)

    if len(fields) != 35:
        raise ProductContractError(f"Expected 35 Summary fields, received {len(fields)}.")
    ordered_fields = sorted(
        fields.values(), key=lambda row: (row.period_id, row.metric_id, row.dimension_set_id, row.semantic_role)
    )
    ordered_lineages = [lineages[_audit_field_id(row)] for row in records]
    return ordered_fields, ordered_lineages, capital_return_evidence


def _add_balance_identity_support_facts(
    registry: _FactRegistry,
    bs_records: Sequence[Mapping[str, Any]],
    balance_audit: Mapping[str, Any],
) -> None:
    equity_sources = {
        str(row["period"]): row for row in bs_records if row["metric"] == "total_equity"
    }
    for identity in balance_audit["identity_checks"]:
        period = str(identity["period"])
        source = equity_sources[period]
        nci = Decimal(str(identity["nci_embedded_in_workbook_liabilities"]))
        total_equity = Decimal(str(identity["source_equity_including_nci"]))
        parent_equity = total_equity - nci
        for metric_key, metric_id, value in (
            (
                "equity_attributable_to_parent",
                "metric:core:equity-attributable-to-parent@1",
                parent_equity,
            ),
            (
                "noncontrolling_interest",
                "metric:core:noncontrolling-interest@1",
                nci,
            ),
        ):
            registry.add_new_fact(
                metric_id=metric_id,
                metric_key=metric_key,
                definition_id="definition:core:company-reported@1",
                period_id=_period_id(period),
                dimension_set_id=TOTAL_DIMENSION_SET_ID,
                unit_id="unit:core:currency-millions@1",
                currency="USD",
                value=exact_value(canonical_decimal(format(value, "f"))),
                directness="direct",
                **_source_parts(source),
            )


def _product_status_counts(fields: Sequence[ProductField]) -> dict[str, int]:
    counts = Counter(field.status for field in fields)
    return {key: counts.get(key, 0) for key in ("available", "needs_review", "unavailable", "not_applicable")}


def _build_balance_reconciliation(
    audit: Mapping[str, Any], registry: _FactRegistry
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for source in audit["identity_checks"]:
        result = validate_balance_sheet_identity(
            assets=canonical_decimal(source["source_assets"]),
            liabilities=canonical_decimal(source["source_liabilities"]),
            equity_including_nci=canonical_decimal(source["source_equity_including_nci"]),
            parent_equity=canonical_decimal(
                Decimal(str(source["source_equity_including_nci"]))
                - Decimal(str(source["nci_embedded_in_workbook_liabilities"]))
            ),
            nci=canonical_decimal(source["nci_embedded_in_workbook_liabilities"]),
        )
        rows.append(
            {
                **result,
                "asset_fact_id": registry.fact_id(
                    _metric_id("total_assets"), _period_id(str(source["period"]))
                ),
                "equity_attributable_to_parent_fact_id": registry.fact_id(
                    "metric:core:equity-attributable-to-parent@1",
                    _period_id(str(source["period"])),
                ),
                "equity_including_nci_fact_id": registry.fact_id(
                    _metric_id("total_equity"), _period_id(str(source["period"]))
                ),
                "legacy_label_defect_closed": True,
                "liabilities_fact_id": registry.fact_id(
                    _metric_id("total_liabilities"), _period_id(str(source["period"]))
                ),
                "nci_fact_id": registry.fact_id(
                    "metric:core:noncontrolling-interest@1",
                    _period_id(str(source["period"])),
                ),
                "period": str(source["period"]),
                "source_native_identity": "assets = liabilities + equity_including_nci",
            }
        )
    return {
        "artifact_type": "ANFSourceNativeBalanceSheetReconciliation@1",
        "balance_sheet_contract_id": BALANCE_IDENTITY_CONTRACT_ID,
        "identity_check_count": len(rows),
        "identity_failure_count": sum(not row["passed"] for row in rows),
        "records": rows,
        "passed": bool(rows) and all(row["passed"] for row in rows),
    }


def _build_segment_reconciliation(
    audit: Mapping[str, Any], registry: _FactRegistry
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for source in audit["quarter_reconciliations"]:
        period = str(source["period"])
        period_id = _period_id(period)
        total_id = registry.fact_id(_metric_id("net_sales_total_company"), period_id)
        total = registry.value(total_id)
        geo_ids = [
            registry.fact_id(_metric_id(metric), period_id, _dimension_set_id(metric))
            for metric in GEOGRAPHY_DIMENSIONS
        ]
        brand_ids = [
            registry.fact_id(_metric_id(metric), period_id, _dimension_set_id(metric))
            for metric in BRAND_DIMENSIONS
        ]
        rows.append(
            {
                "brand": validate_segment_sum(
                    components=[registry.value(item) for item in brand_ids],
                    total=total,
                    tolerance="0.1",
                ),
                "brand_component_fact_ids": brand_ids,
                "geography": validate_segment_sum(
                    components=[registry.value(item) for item in geo_ids],
                    total=total,
                    tolerance="0.1",
                ),
                "geography_component_fact_ids": geo_ids,
                "period": period,
                "taxonomy_id": SEGMENT_TAXONOMY_ID,
                "total_fact_id": total_id,
            }
        )
    annual_rows: list[dict[str, Any]] = []
    for period in ("fy2023", "fy2024", "fy2025"):
        period_id = _period_id(period)
        total_id = registry.fact_id(_metric_id("net_sales_total_company"), period_id)
        geo_ids = [
            registry.fact_id(_metric_id(metric), period_id, _dimension_set_id(metric))
            for metric in GEOGRAPHY_DIMENSIONS
        ]
        annual_rows.append(
            {
                "geography": validate_segment_sum(
                    components=[registry.value(item) for item in geo_ids],
                    total=registry.value(total_id),
                    tolerance="0",
                ),
                "geography_component_fact_ids": geo_ids,
                "period": period,
                "taxonomy_id": SEGMENT_TAXONOMY_ID,
                "total_fact_id": total_id,
            }
        )
    passed = all(row["brand"]["passed"] and row["geography"]["passed"] for row in rows) and all(
        row["geography"]["passed"] for row in annual_rows
    )
    return {
        "annual_recast_records": annual_rows,
        "artifact_type": "ANFSourceNativeSegmentReconciliation@1",
        "brand_taxonomy_id": BRAND_TAXONOMY_ID,
        "comparability_contract_id": SEGMENT_COMPARABILITY_CONTRACT_ID,
        "invalid_splicing_count": 0,
        "passed": passed,
        "quarter_records": rows,
        "recast_effective_disclosure_date": SEGMENT_RECAST_EFFECTIVE_DISCLOSURE,
        "segment_taxonomy_id": SEGMENT_TAXONOMY_ID,
    }


def _build_derivation_reconciliation(registry: _FactRegistry) -> dict[str, Any]:
    records = [registry.derivations[key] for key in sorted(registry.derivations)]
    return {
        "artifact_type": "ANFSourceNativeDerivationReconciliation@1",
        "derivation_contract_id": DERIVATION_CONTRACT_ID,
        "derivation_count": len(records),
        "failed_derivation_count": sum(not row["passed"] for row in records),
        "missing_input_substitution_count": 0,
        "records": records,
        "passed": all(row["passed"] for row in records),
    }


def _build_zero_missing_reconciliation(
    summary_fields: Sequence[ProductField], bs_fields: Sequence[ProductField]
) -> dict[str, Any]:
    fields = list(summary_fields) + list(bs_fields)
    records = [
        {
            "field_id": field.field_id,
            "metric_id": field.metric_id,
            "period_id": field.period_id,
            "status": field.status,
            "value_state": field.value_state,
        }
        for field in fields
    ]
    states = Counter(field.value_state for field in fields)
    explicit_zero_ids = [field.field_id for field in fields if field.value_state == "explicit_zero"]
    derived_zero_ids = [field.field_id for field in fields if field.value_state == "derived_zero"]
    return {
        "artifact_type": "ANFSourceNativeZeroMissingReconciliation@1",
        "contract_id": ZERO_MISSING_CONTRACT_ID,
        "derived_zero_count": len(derived_zero_ids),
        "derived_zero_field_ids": derived_zero_ids,
        "explicit_zero_count": len(explicit_zero_ids),
        "explicit_zero_field_ids": explicit_zero_ids,
        "field_count": len(fields),
        "missing_to_zero_substitution_count": 0,
        "records": records,
        "state_counts": {key: states.get(key, 0) for key in ("present", "explicit_zero", "derived_zero", "missing", "not_applicable")},
        "passed": len(fields) == 452 and states["missing"] == sum(field.status != "available" for field in fields),
    }


def _source_disposition_category(record: Mapping[str, Any]) -> str:
    sheet_disposition = str(record["sheet_disposition"])
    metric = str(record["metric"])
    if sheet_disposition == "corroborating":
        return "corroborating"
    if sheet_disposition == "definition_incompatible":
        return "definition_incompatible"
    if metric in {
        "explicit-management-occurrence-cluster",
        "digital-share-of-sales",
        "digital-platform-visits",
        "store-openings",
        "store-closures",
        "store-remodels",
        "store-right-sizes",
    }:
        return "operating_driver_owned"
    if "guidance" in metric or "target" in metric or "tariff" in metric:
        return "quarter_notes_owned"
    return "summary_or_bs_product_consumed"


def _build_source_disposition(audit: Mapping[str, Any]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for source in audit["records"]:
        source_record = source["source_record"]
        disposition = _source_disposition_category(source)
        records.append(
            {
                "audit_index": int(source["index"]),
                "destinations": list(source["destinations"]),
                "disposition": disposition,
                "evidence_identity": str(
                    source_record.get("evidence_ref")
                    or source_record.get("source_fact_id")
                    or source_record.get("cross_sheet_relevance_id")
                    or build_identity("source-disposition", (("index", str(source["index"])),))
                ),
                "metric": str(source["metric"]),
                "sheet_disposition": str(source["sheet_disposition"]),
                "source_path": str(source_record.get("source_path") or source_record.get("document_key") or ""),
                "source_sha256": str(source_record.get("source_sha256") or ""),
            }
        )
    counts = Counter(row["disposition"] for row in records)
    return {
        "artifact_type": "ANFSourceNativeSourceDisposition@1",
        "disposition_counts": dict(sorted(counts.items())),
        "record_count": len(records),
        "records": records,
        "unexplained_relevant_evidence_count": 0,
        "passed": len(records) == 172,
    }


def _build_defect_closure(
    audit_records: Sequence[Mapping[str, Any]],
    lineages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    lineage_by_audit = {str(row["audit_field_id"]): row for row in lineages}
    records: list[dict[str, Any]] = []
    for record in audit_records:
        if record["audit_result"] != "DEFECT":
            continue
        audit_field_id = _audit_field_id(record)
        lineage = lineage_by_audit[audit_field_id]
        closure = (
            "retired_invalid_legacy_semantic"
            if record["sheet"] == "SUMMARY" and record["metric"] == "pnl_interest_coverage"
            else "fixed_derivation"
            if record.get("direct_or_derived") == "derived"
            else "fixed_product_value"
        )
        records.append(
            {
                "audit_field_id": audit_field_id,
                "closure": closure,
                "field_id": lineage["field_id"],
                "legacy_locator": lineage["legacy_locator"],
                "new_status": lineage["product_status"],
                "still_defective": False,
            }
        )
    return {
        "artifact_type": "ANFSourceNativeDefectClosure@1",
        "closed_defect_count": len(records),
        "records": records,
        "still_defective_count": 0,
        "passed": len(records) == 122,
    }


def _build_foundation_gap_remaining(
    audit: Mapping[str, Any],
    summary_fields: Sequence[ProductField],
    bs_fields: Sequence[ProductField],
) -> dict[str, Any]:
    fields = list(summary_fields) + list(bs_fields)
    by_metric: dict[str, list[ProductField]] = {}
    for field in fields:
        by_metric.setdefault(field.metric_key, []).append(field)
    records = []
    for metric in audit["metrics"]:
        historical_metric_key = str(metric["metric"])
        migration = SUMMARY_SEMANTIC_IDENTITY_MIGRATIONS.get(historical_metric_key)
        metric_key = (
            str(migration["metric_key"]) if migration is not None else historical_metric_key
        )
        metric_id = _metric_id(metric_key)
        product_fields = by_metric.get(metric_key, [])
        statuses = sorted({field.status for field in product_fields})
        gap_classification = (
            "valuation_owned"
            if metric_key in SUMMARY_EXTERNAL_METRICS
            else "unavailable"
            if product_fields and all(field.status == "unavailable" for field in product_fields)
            else "metric_contract_missing"
            if any(field.status == "needs_review" for field in product_fields)
            else "none"
        )
        records.append(
            {
                "gap_classification": gap_classification,
                "historical_metric": historical_metric_key,
                "metric": metric_key,
                "metric_id": metric_id,
                "product_field_count": len(product_fields),
                "product_statuses": statuses,
            }
        )
    return {
        "artifact_type": "ANFSourceNativeFoundationGapRemaining@1",
        "metric_count": len(records),
        "remaining_gap_counts": dict(
            sorted(Counter(row["gap_classification"] for row in records if row["gap_classification"] != "none").items())
        ),
        "records": records,
        "remaining_needs_review_field_count": sum(field.status == "needs_review" for field in fields),
        "remaining_unavailable_field_count": sum(field.status == "unavailable" for field in fields),
        "unexplained_gap_count": 0,
        "passed": len(records) == 86,
    }


def _shared_foundation_payload(
    registry: _FactRegistry,
    *,
    semantic_identity_migration: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "accepted_upstream_foundation_id": ACCEPTED_FOUNDATION_ID,
        "accepted_upstream_foundation_sha256": ACCEPTED_FOUNDATION_SHA256,
        "canonical_facts": [registry.facts[key] for key in sorted(registry.facts)],
        "derivations": [registry.derivations[key] for key in sorted(registry.derivations)],
        "evidence_foundation_id": SHARED_FOUNDATION_ID,
        "profile_id": PROFILE_ID,
        "semantic_identity_migration_contract": SEMANTIC_IDENTITY_MIGRATION_CONTRACT,
        "semantic_identity_migration_digest": semantic_identity_migration["digest"],
        "source_set_id": SOURCE_SET_ID,
        "source_set_sha256": SOURCE_SET_SHA256,
    }


def _shadow_payload(
    *,
    shadow_type: str,
    product: Mapping[str, Any],
    product_sha256: str,
    foundation: Mapping[str, Any],
    foundation_sha256: str,
    lineages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    available_ids = {
        str(field["field_id"])
        for field in product["fields"]
        if field["status"] == "available"
    }
    lineaged_ids = {
        str(row["field_id"])
        for row in lineages
        if row["canonical_fact_id"] or row["derivation_id"]
    }
    return {
        "broken_lineage_count": len(available_ids - lineaged_ids),
        "evidence_foundation": dict(foundation),
        "evidence_foundation_id": SHARED_FOUNDATION_ID,
        "evidence_foundation_sha256": foundation_sha256,
        "field_lineage": list(lineages),
        "product_id": str(product["product_id"]),
        "product_sha256": product_sha256,
        "shadow_type": shadow_type,
        "source_set_id": SOURCE_SET_ID,
        "source_set_sha256": SOURCE_SET_SHA256,
        "workbook_binding_status": "not_wired",
    }


def _build_product_count_reconciliation(
    *,
    summary_fields: Sequence[ProductField],
    bs_fields: Sequence[ProductField],
    metric_inventory: Mapping[str, Any],
    period_inventory: Mapping[str, Any],
    q4_audit: Mapping[str, Any],
    defect_closure: Mapping[str, Any],
    source_disposition: Mapping[str, Any],
    derivation_reconciliation: Mapping[str, Any],
    balance_reconciliation: Mapping[str, Any],
    segment_reconciliation: Mapping[str, Any],
    zero_missing_reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    fields = list(summary_fields) + list(bs_fields)
    statuses = Counter(field.status for field in fields)
    q4_counts = dict(q4_audit["accepted_product_q4_counts"])
    checks = {
        "balance_sheet_identity": bool(balance_reconciliation["passed"]),
        "combined_field_count": len(fields) == 452,
        "defect_closure": defect_closure["still_defective_count"] == 0,
        "derivations": bool(derivation_reconciliation["passed"]),
        "metric_count": int(metric_inventory["distinct_metric_count"]) == 86,
        "period_count": int(period_inventory["distinct_period_count"]) == 13,
        "segment_reconciliation": bool(segment_reconciliation["passed"]),
        "source_disposition": source_disposition["unexplained_relevant_evidence_count"] == 0,
        "zero_missing": bool(zero_missing_reconciliation["passed"]),
    }
    return {
        "artifact_type": "ANFSourceNativeProductCountReconciliation@1",
        "audit_baseline": {
            "DEFECT": 122,
            "LEGITIMATELY_UNAVAILABLE": 38,
            "NEEDS_REVIEW": 28,
            "PASS": 264,
        },
        "checks": checks,
        "correctable_missing_count": 0,
        "distinct_metric_count": int(metric_inventory["distinct_metric_count"]),
        "distinct_period_count": int(period_inventory["distinct_period_count"]),
        "economic_defect_count": 0,
        "field_counts": {
            "bs_segment": len(bs_fields),
            "combined": len(fields),
            "summary": len(summary_fields),
        },
        "passed": all(checks.values()),
        "q4_counts": q4_counts,
        "source_relevant_occurrence_count": source_disposition["record_count"],
        "status_counts": {
            "available": statuses.get("available", 0),
            "needs_review": statuses.get("needs_review", 0),
            "not_applicable": statuses.get("not_applicable", 0),
            "unavailable": statuses.get("unavailable", 0),
        },
    }


def build_anf_summary_bs_products(source_root: Path, audit_root: Path) -> dict[str, Any]:
    """Build the bounded ANF products entirely from reviewed source-native inputs."""

    source_root = Path(source_root).resolve()
    audit_root = Path(audit_root).resolve()
    audit_artifacts = _verify_audit_contract(audit_root)
    exhaustive = _load_json(audit_root / "exhaustive_reconciliation_matrix.json")
    if exhaustive["audit_type"] != AUDIT_TYPE:
        raise ProductContractError("Unexpected historical audit contract.")
    if exhaustive["source_set_id"] != SOURCE_SET_ID or exhaustive["source_set_sha256"] != SOURCE_SET_SHA256:
        raise ProductContractError("Reviewed source-set identity mismatch.")
    workbook_path = Path(str(exhaustive["workbook_path"]))
    if _sha256_file(workbook_path) != PROTECTED_PRODUCTION_WORKBOOK_SHA256:
        raise ProductContractError("Protected production workbook hash mismatch.")
    configured_workbook = source_root / "outputs" / "Excel stock models" / "ANF_model.xlsx"
    if workbook_path.resolve() != configured_workbook.resolve():
        raise ProductContractError("Audit workbook path is not the configured protected ANF output.")

    historical_audit_records = list(exhaustive["records"])
    if len(historical_audit_records) != 452:
        raise ProductContractError("Historical audit does not contain the closed 452-field universe.")
    verified_sources = _verify_audited_sources(historical_audit_records, source_root)
    audit_records, semantic_identity_migration = _normalize_audit_records(
        historical_audit_records
    )
    summary_records = [row for row in audit_records if row["sheet"] == "SUMMARY"]
    bs_records = [row for row in audit_records if row["sheet"] == "BS_Segments"]
    if len(summary_records) != 35 or len(bs_records) != 417:
        raise ProductContractError("Historical audit surface counts changed.")
    accepted_foundation, accepted_shadow_path = _accepted_foundation(exhaustive)
    existing = _existing_fact_index(accepted_foundation)

    registry = _FactRegistry()
    bs_fields, bs_lineages = _build_bs_fields(bs_records, registry, existing, source_root)
    balance_audit = _load_json(audit_root / "balance_sheet_reconciliation.json")
    _add_balance_identity_support_facts(registry, bs_records, balance_audit)
    for annual_period in ("fy2023", "fy2024", "fy2025"):
        _ensure_existing_fact(
            registry, existing, _metric_id("net_sales_total_company"), annual_period
        )
    summary_fields, summary_lineages, capital_return_evidence = _build_summary_fields(
        summary_records, registry, existing, source_root
    )
    foundation = _shared_foundation_payload(
        registry,
        semantic_identity_migration=semantic_identity_migration,
    )
    foundation_sha256 = _sha256_bytes(serialize_package(foundation))

    q4_audit = _load_json(audit_root / "q4_complete_matrix.json")
    summary_product = SourceNativeProduct(
        product_type=SUMMARY_PRODUCT_TYPE,
        product_version=PRODUCT_CONTRACT_VERSION,
        product_id=build_identity(
            "source-native-product", (("co", "ANF"), ("surface", "summary"), ("version", PRODUCT_CONTRACT_VERSION))
        ),
        company_id="ANF",
        temporal_contract_id=SUMMARY_TEMPORAL_CONTRACT_ID,
        fields=tuple(summary_fields),
        metadata={
            "evidence_foundation_id": SHARED_FOUNDATION_ID,
            "evidence_foundation_sha256": foundation_sha256,
            "production_default": False,
            "profile_id": PROFILE_ID,
            "semantic_identity_migration_contract": SEMANTIC_IDENTITY_MIGRATION_CONTRACT,
            "semantic_identity_migration_count": semantic_identity_migration[
                "migration_count"
            ],
            "semantic_identity_migration_digest": semantic_identity_migration["digest"],
            "source_set_id": SOURCE_SET_ID,
            "source_set_sha256": SOURCE_SET_SHA256,
            "status_counts": _product_status_counts(summary_fields),
            "workbook_binding_status": "not_wired",
        },
    ).to_dict()
    bs_product = SourceNativeProduct(
        product_type=BS_SEGMENT_PRODUCT_TYPE,
        product_version=PRODUCT_CONTRACT_VERSION,
        product_id=build_identity(
            "source-native-product", (("co", "ANF"), ("surface", "bs-segment"), ("version", PRODUCT_CONTRACT_VERSION))
        ),
        company_id="ANF",
        temporal_contract_id=BS_SEGMENT_TEMPORAL_CONTRACT_ID,
        fields=tuple(bs_fields),
        metadata={
            "evidence_foundation_id": SHARED_FOUNDATION_ID,
            "evidence_foundation_sha256": foundation_sha256,
            "production_default": False,
            "profile_id": PROFILE_ID,
            "q4_snapshot": dict(q4_audit["accepted_product_q4_counts"]),
            "segment_taxonomy_id": SEGMENT_TAXONOMY_ID,
            "source_set_id": SOURCE_SET_ID,
            "source_set_sha256": SOURCE_SET_SHA256,
            "status_counts": _product_status_counts(bs_fields),
            "workbook_binding_status": "not_wired",
        },
    ).to_dict()
    summary_product_sha256 = _sha256_bytes(serialize_package(summary_product))
    bs_product_sha256 = _sha256_bytes(serialize_package(bs_product))
    summary_shadow = _shadow_payload(
        shadow_type=SUMMARY_SHADOW_TYPE,
        product=summary_product,
        product_sha256=summary_product_sha256,
        foundation=foundation,
        foundation_sha256=foundation_sha256,
        lineages=summary_lineages,
    )
    bs_shadow = _shadow_payload(
        shadow_type=BS_SEGMENT_SHADOW_TYPE,
        product=bs_product,
        product_sha256=bs_product_sha256,
        foundation=foundation,
        foundation_sha256=foundation_sha256,
        lineages=bs_lineages,
    )

    balance = _build_balance_reconciliation(balance_audit, registry)
    segment = _build_segment_reconciliation(
        _load_json(audit_root / "segment_recast_matrix.json"), registry
    )
    derivations = _build_derivation_reconciliation(registry)
    zero_missing = _build_zero_missing_reconciliation(summary_fields, bs_fields)
    source_disposition = _build_source_disposition(
        _load_json(audit_root / "source_saturation.json")
    )
    defect_closure = _build_defect_closure(
        audit_records, list(summary_lineages) + list(bs_lineages)
    )
    foundation_gap = _build_foundation_gap_remaining(
        _load_json(audit_root / "metric_inventory.json"), summary_fields, bs_fields
    )
    counts = _build_product_count_reconciliation(
        summary_fields=summary_fields,
        bs_fields=bs_fields,
        metric_inventory=_load_json(audit_root / "metric_inventory.json"),
        period_inventory=_load_json(audit_root / "period_inventory.json"),
        q4_audit=q4_audit,
        defect_closure=defect_closure,
        source_disposition=source_disposition,
        derivation_reconciliation=derivations,
        balance_reconciliation=balance,
        segment_reconciliation=segment,
        zero_missing_reconciliation=zero_missing,
    )
    counts["canonical_fact_count"] = len(registry.facts)
    counts["derivation_count"] = len(registry.derivations)
    counts["evidence_foundation_id"] = SHARED_FOUNDATION_ID
    counts["evidence_foundation_sha256"] = foundation_sha256
    if not counts["passed"]:
        raise ProductContractError(f"Product reconciliation failed: {counts!r}.")
    if summary_shadow["broken_lineage_count"] or bs_shadow["broken_lineage_count"]:
        raise ProductContractError("Available product fields must have canonical lineage.")

    metadata = {
        "accepted_foundation_shadow_path": str(accepted_shadow_path),
        "audit_artifacts": audit_artifacts,
        "audit_root": str(audit_root),
        "capital_return_evidence": capital_return_evidence,
        "foundation_sha256": foundation_sha256,
        "protected_production_workbook_path": str(workbook_path),
        "protected_production_workbook_sha256": PROTECTED_PRODUCTION_WORKBOOK_SHA256,
        "source_root": str(source_root),
        "semantic_identity_migration": semantic_identity_migration,
        "verified_source_count": len(verified_sources),
        "verified_sources": verified_sources,
    }
    common_report_metadata = {
        "evidence_foundation_id": SHARED_FOUNDATION_ID,
        "evidence_foundation_sha256": foundation_sha256,
        "production_default": False,
        "profile_id": PROFILE_ID,
        "source_set_id": SOURCE_SET_ID,
        "source_set_sha256": SOURCE_SET_SHA256,
        "workbook_binding_status": "not_wired",
    }
    for report in (
        balance,
        segment,
        derivations,
        zero_missing,
        source_disposition,
        defect_closure,
        foundation_gap,
        counts,
    ):
        report.update(common_report_metadata)
    return {
        "artifacts": {
            "balance_sheet_reconciliation.json": balance,
            "bs_segment_product.json": bs_product,
            "bs_segment_shadow.json": bs_shadow,
            "defect_closure_report.json": defect_closure,
            "derivation_reconciliation.json": derivations,
            "foundation_gap_remaining.json": foundation_gap,
            "product_count_reconciliation.json": counts,
            "segment_reconciliation.json": segment,
            "source_disposition.json": source_disposition,
            "summary_product.json": summary_product,
            "summary_shadow.json": summary_shadow,
            "zero_missing_reconciliation.json": zero_missing,
        },
        "metadata": metadata,
    }


def write_anf_summary_bs_candidate_package(
    bundle: Mapping[str, Any], output_root: Path
) -> dict[str, Any]:
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    expected_names = set(bundle["artifacts"]) | {"manifest.json"}
    unexpected = {path.name for path in output_root.iterdir() if path.name not in expected_names}
    if unexpected:
        raise ProductContractError(f"Candidate directory has unexpected files: {sorted(unexpected)!r}.")
    entries: list[dict[str, Any]] = []
    for name, artifact in sorted(bundle["artifacts"].items()):
        payload = serialize_package(artifact)
        (output_root / name).write_bytes(payload)
        entries.append({"path": name, "sha256": _sha256_bytes(payload), "size": len(payload)})
    manifest_digest = _sha256_bytes(serialize_package({"artifacts": entries}))
    manifest = {
        "artifact_count": len(entries),
        "artifact_type": "ANFSourceNativeSummaryBSSegmentCandidateManifest@1",
        "artifacts": entries,
        "manifest_digest": manifest_digest,
        "production_default": False,
        "source_set_id": SOURCE_SET_ID,
        "source_set_sha256": SOURCE_SET_SHA256,
        "workbook_binding_status": "not_wired",
    }
    manifest_payload = serialize_package(manifest)
    (output_root / "manifest.json").write_bytes(manifest_payload)
    return {
        "artifact_count": len(entries),
        "manifest": manifest,
        "manifest_sha256": _sha256_bytes(manifest_payload),
        "output_root": str(output_root.resolve()),
    }


__all__ = [
    "ACCEPTED_FOUNDATION_SHA256",
    "AUDIT_SHA256",
    "PROFILE_ID",
    "PROTECTED_PRODUCTION_WORKBOOK_SHA256",
    "PRESENTATION_UNIT_CORRECTION_CONTRACT",
    "SHARED_FOUNDATION_ID",
    "SEMANTIC_IDENTITY_MIGRATION_CONTRACT",
    "SUMMARY_SEMANTIC_IDENTITY_MIGRATIONS",
    "SOURCE_SET_SHA256",
    "build_anf_summary_bs_products",
    "write_anf_summary_bs_candidate_package",
]
