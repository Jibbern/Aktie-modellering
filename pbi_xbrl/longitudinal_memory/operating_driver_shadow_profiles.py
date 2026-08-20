"""Declarative ANF, PBI, and GPRE Operating Drivers shadow profiles.

This file owns ticker-specific label, dimension, and definition mapping data.
The shared registry engine contains no ticker branches.
"""
from __future__ import annotations

from .operating_driver_foundation import (
    AggregationSemantics,
    DefinitionContinuityState,
    DriverDimension,
)
from .operating_driver_shadow_registry import (
    CalendarMode,
    CanonicalDriverDefinition,
    DriverMappingRule,
    DriverScope,
    FinancialLinkageKind,
    ForecastEvidenceCapability,
    MappingAction,
    TickerShadowProfile,
    VisibilityTier,
)


UNIT_PERCENT = "unit:core:percent@1"
UNIT_PERCENTAGE_POINTS = "unit:core:percentage-points@1"
UNIT_STORES = "unit:operating-driver:stores@1"
UNIT_MILLION_GALLONS = "unit:operating-driver:million-gallons@1"
UNIT_MILLION_BUSHELS = "unit:operating-driver:million-bushels@1"
UNIT_THOUSAND_TONS = "unit:operating-driver:thousand-tons@1"
UNIT_MILLION_POUNDS = "unit:operating-driver:million-pounds@1"
UNIT_USD_MILLION = "unit:core:usd-million@1"
UNIT_QUALITATIVE = "unit:core:qualitative@1"


def _dimension(kind: str, member: str, label: str) -> DriverDimension:
    return DriverDimension(
        dimension_id=f"dimension:operating-driver:{kind}@1",
        member_id=f"member:operating-driver:{member}@1",
        label=label,
    )


TOTAL_COMPANY = (_dimension("scope", "total-company", "Total company"),)
PRODUCTION_PORTFOLIO = (
    _dimension("scope", "production-portfolio", "Production portfolio"),
)


def _definition(
    slug: str,
    label: str,
    *,
    family: str,
    unit: str,
    definition: str,
    scope: DriverScope,
    linkage: FinancialLinkageKind,
    aggregation: AggregationSemantics = AggregationSemantics.NON_AGGREGATABLE,
    forecast: ForecastEvidenceCapability | None = None,
    version: int = 1,
    visibility: VisibilityTier = VisibilityTier.PRIMARY,
    sign: str = "positive means a higher reported driver value",
) -> CanonicalDriverDefinition:
    if forecast is None:
        if unit == UNIT_QUALITATIVE:
            forecast = ForecastEvidenceCapability.FORECAST_CONTEXT
        elif family == "footprint":
            forecast = ForecastEvidenceCapability.DIRECT_FORECAST_INPUT
        elif family in {
            "demand", "inventory", "channel", "utilization", "production",
            "inputs", "coproducts", "volume", "price-mix",
        }:
            forecast = ForecastEvidenceCapability.LEADING_INDICATOR
        elif family in {"margin", "policy-credit"}:
            forecast = ForecastEvidenceCapability.HISTORICAL_ONLY
        else:
            forecast = ForecastEvidenceCapability.NOT_FORECASTABLE
    return CanonicalDriverDefinition(
        driver_id=f"driver:operating:{slug}@1",
        driver_family=family,
        canonical_label=label,
        display_label=label,
        definition_id=f"definition:operating-driver:{slug}@1",
        definition_version=version,
        definition_text=definition,
        unit_id=unit,
        scale="1",
        sign_convention=sign,
        aggregation_semantics=aggregation,
        scope=scope,
        visibility_tier=visibility,
        financial_linkage=linkage,
        forecast_capability=forecast,
    )


def _canonical_rule(
    ticker: str,
    number: int,
    raw_label: str,
    driver_slug: str,
    dimensions: tuple[DriverDimension, ...],
    *,
    version: int = 1,
    priority: int = 10,
    required: tuple[str, ...] = (),
    forbidden: tuple[str, ...] = (),
    effective_from: int | None = None,
    effective_through: int | None = None,
    transition_state: DefinitionContinuityState | None = None,
    transition_from_version: int | None = None,
    reason: str = "Exact accepted raw-label mapping to a typed canonical driver.",
) -> DriverMappingRule:
    return DriverMappingRule(
        rule_id=f"rule:operating-driver:{ticker.lower()}-{number:03d}@1",
        raw_label=raw_label,
        action=MappingAction.CANONICAL_DRIVER,
        canonical_driver_id=f"driver:operating:{driver_slug}@1",
        definition_version=version,
        dimensions=dimensions,
        required_commentary_tokens=required,
        forbidden_commentary_tokens=forbidden,
        effective_from_serial=effective_from,
        effective_through_serial=effective_through,
        priority=priority,
        reason=reason,
        transition_state=transition_state,
        transition_from_definition_version=transition_from_version,
    )


def _disposition_rule(
    ticker: str,
    number: int,
    raw_label: str,
    action: MappingAction,
    *,
    owner: str | None = None,
    priority: int = 0,
    required: tuple[str, ...] = (),
    forbidden: tuple[str, ...] = (),
    reason: str,
) -> DriverMappingRule:
    return DriverMappingRule(
        rule_id=f"rule:operating-driver:{ticker.lower()}-{number:03d}@1",
        raw_label=raw_label,
        action=action,
        owner_id=owner,
        priority=priority,
        required_commentary_tokens=required,
        forbidden_commentary_tokens=forbidden,
        reason=reason,
    )


ANF_DEFINITIONS = (
    _definition(
        "comparable-sales",
        "Comparable sales",
        family="demand",
        unit=UNIT_PERCENT,
        definition="Reported comparable-sales growth for the stated brand, geography, or total-company dimension and fiscal quarter.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_LEADING_INDICATOR,
    ),
    _definition(
        "company-owned-stores-start",
        "Company-owned stores, start",
        family="footprint",
        unit=UNIT_STORES,
        definition="Company-owned store count at the beginning of the reported fiscal period for the stated dimension.",
        scope=DriverScope.GENERIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.PERIOD_END,
    ),
    _definition(
        "company-owned-stores-end",
        "Company-owned stores, end",
        family="footprint",
        unit=UNIT_STORES,
        definition="Company-owned store count at the end of the reported fiscal period for the stated dimension.",
        scope=DriverScope.GENERIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.PERIOD_END,
    ),
    _definition(
        "new-stores",
        "New stores",
        family="footprint",
        unit=UNIT_STORES,
        definition="Stores opened during the reported fiscal period for the stated dimension.",
        scope=DriverScope.GENERIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.SUMMABLE,
    ),
    _definition(
        "closed-stores",
        "Closed stores",
        family="footprint",
        unit=UNIT_STORES,
        definition="Stores closed during the reported fiscal period for the stated dimension.",
        scope=DriverScope.GENERIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.SUMMABLE,
    ),
    _definition(
        "franchise-stores",
        "Franchise stores",
        family="footprint",
        unit=UNIT_STORES,
        definition="Franchise store count at the stated period end and dimension.",
        scope=DriverScope.GENERIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.PERIOD_END,
    ),
    _definition(
        "total-stores-including-franchise",
        "Total stores including franchise",
        family="footprint",
        unit=UNIT_STORES,
        definition="Total company-owned plus franchise store count at the stated period end and dimension.",
        scope=DriverScope.GENERIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.PERIOD_END,
    ),
    _definition(
        "remodeled-stores",
        "Remodeled stores",
        family="footprint",
        unit=UNIT_STORES,
        definition="Stores remodeled during the stated fiscal period.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.SUMMABLE,
    ),
    _definition(
        "right-sized-stores",
        "Right-sized stores",
        family="footprint",
        unit=UNIT_STORES,
        definition="Stores right-sized during the stated fiscal period.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT,
        aggregation=AggregationSemantics.SUMMABLE,
    ),
    _definition(
        "digital-sales-mix",
        "Digital sales mix",
        family="channel",
        unit=UNIT_PERCENT,
        definition="Digital sales as a percentage of reported net sales for the stated period and dimension.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_PRICE_MIX,
    ),
    _definition(
        "inventory-unit-growth",
        "Inventory unit growth",
        family="inventory",
        unit=UNIT_PERCENT,
        definition="Reported year-over-year growth in inventory units for the stated period.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_LEADING_INDICATOR,
    ),
    _definition(
        "inventory-cost-growth",
        "Inventory cost growth",
        family="inventory",
        unit=UNIT_PERCENT,
        definition="Reported year-over-year growth in inventory cost for the stated period.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_LEADING_INDICATOR,
    ),
    _definition(
        "inventory-unit-growth-erp-points",
        "Inventory unit growth ERP effect",
        family="inventory",
        unit=UNIT_PERCENTAGE_POINTS,
        definition="Reported percentage-point contribution of ERP effects to inventory unit growth.",
        scope=DriverScope.TICKER_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_LEADING_INDICATOR,
        visibility=VisibilityTier.SECONDARY,
    ),
    _definition(
        "brand-momentum-context",
        "Brand momentum context",
        family="demand",
        unit=UNIT_QUALITATIVE,
        definition="Source-backed qualitative comparison of Abercrombie and Hollister brand momentum.",
        scope=DriverScope.TICKER_SPECIFIC,
        linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT,
        visibility=VisibilityTier.SECONDARY,
    ),
    _definition(
        "store-digital-activity-context",
        "Store and digital activity context",
        family="channel",
        unit=UNIT_QUALITATIVE,
        definition="Source-backed qualitative context on store and digital activity.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT,
        visibility=VisibilityTier.SECONDARY,
    ),
)


_ANF_DIMENSIONS = {
    "Total Company": TOTAL_COMPANY,
    "Abercrombie": (_dimension("brand", "abercrombie", "Abercrombie"),),
    "Hollister": (_dimension("brand", "hollister", "Hollister"),),
    "Americas": (_dimension("geography", "americas", "Americas"),),
    "EMEA": (_dimension("geography", "emea", "EMEA"),),
    "APAC": (_dimension("geography", "apac", "APAC"),),
}


def _anf_rules() -> tuple[DriverMappingRule, ...]:
    rules: list[DriverMappingRule] = []
    number = 1
    for prefix in ("Total Company", "Abercrombie", "Hollister", "Americas", "EMEA", "APAC"):
        label = f"{prefix} comparable sales"
        rules.append(_canonical_rule("ANF", number, label, "comparable-sales", _ANF_DIMENSIONS[prefix]))
        number += 1
    rules.append(_canonical_rule("ANF", number, "Comparable sales", "comparable-sales", TOTAL_COMPANY)); number += 1
    store_metrics = (
        ("Company-owned stores, start", "company-owned-stores-start"),
        ("Company-owned stores, end", "company-owned-stores-end"),
        ("New stores", "new-stores"),
        ("Closed stores", "closed-stores"),
        ("Franchise stores", "franchise-stores"),
        ("Total stores incl. franchise", "total-stores-including-franchise"),
    )
    for prefix in ("Total Company", "Abercrombie", "Hollister"):
        for suffix, slug in store_metrics:
            rules.append(_canonical_rule("ANF", number, f"{prefix} {suffix}", slug, _ANF_DIMENSIONS[prefix]))
            number += 1
    for suffix, slug in (("Remodeled stores", "remodeled-stores"), ("Right-sized stores", "right-sized-stores")):
        rules.append(_canonical_rule("ANF", number, f"Total Company {suffix}", slug, TOTAL_COMPANY)); number += 1
    for raw_label, slug in (
        ("Total Company Digital sales mix", "digital-sales-mix"),
        ("Total Company Inventory unit growth", "inventory-unit-growth"),
        ("Total Company Inventory cost growth", "inventory-cost-growth"),
        ("Inventory Unit Growth Erp Points", "inventory-unit-growth-erp-points"),
        ("Abercrombie vs Hollister brands", "brand-momentum-context"),
        ("Store and digital activity", "store-digital-activity-context"),
    ):
        rules.append(_canonical_rule("ANF", number, raw_label, slug, TOTAL_COMPANY)); number += 1
    financial_owner_labels = (
        "APAC net sales", "APAC net sales growth", "Abercrombie net sales",
        "Abercrombie net sales growth", "Americas net sales", "Americas net sales growth",
        "Average buyback price", "Buybacks / liquidity", "Capital expenditures",
        "EMEA net sales", "EMEA net sales growth", "Gross margin", "Hollister net sales",
        "Hollister net sales growth", "Inventory",
        "Net sales", "Operating cash flow", "Operating margin", "Remaining buyback authorization",
        "Share repurchases", "Shares repurchased", "Total Company Repurchased shares / opening shares",
        "Total Company net sales",
    )
    for raw_label in financial_owner_labels:
        rules.append(
            _disposition_rule(
                "ANF", number, raw_label, MappingAction.OWNER_ELSEWHERE,
                owner="owner:financial-products:source-native@1",
                reason="The accepted financial, capital-return, or Summary/BS product remains canonical owner.",
            )
        ); number += 1
    guidance_labels = (
        "FY2026 tariff headwind", "Fy2026 Tariff Headwind Bps",
        "Gross margin / tariff / freight", "Q1 FY2026 tariff headwind",
        "Q1 Fy2026 Freight Tailwind Bps", "Q1 Fy2026 Marketing Headwind Bps",
        "Q1 Fy2026 Tariff Headwind Bps",
    )
    for raw_label in guidance_labels:
        rules.append(
            _disposition_rule(
                "ANF", number, raw_label, MappingAction.GUIDANCE_REFERENCE,
                owner="owner:guidance:source-native@1",
                reason="Forward or bridge evidence remains a guidance reference, not an actual driver observation.",
            )
        ); number += 1
    return tuple(rules)


ANF_PROFILE = TickerShadowProfile(
    ticker="ANF",
    calendar_mode=CalendarMode.SOURCE_LABELLED_52_53_WEEK,
    calendar_id="calendar:anf:source-labelled-fiscal@1",
    mapping_rules=_anf_rules(),
    definitions=ANF_DEFINITIONS,
    source_priority=("10-K", "10-Q", "earnings_release", "presentation", "transcript", "internal_metric"),
    fiscal_anchor_year=2026,
    fiscal_anchor_quarter=1,
    fiscal_anchor_serial=46144,
)


PBI_DEFINITIONS = (
    _definition(
        "presort-volume-context", "Presort volume context", family="volume",
        unit=UNIT_QUALITATIVE,
        definition="Source-backed qualitative context explicitly attributable to Presort Services volumes or throughput.",
        scope=DriverScope.TICKER_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_VOLUME,
        visibility=VisibilityTier.AUDIT_ONLY,
    ),
    _definition(
        "sendtech-activity-context", "SendTech activity context", family="volume",
        unit=UNIT_QUALITATIVE,
        definition="Source-backed qualitative context explicitly attributable to SendTech activity or throughput.",
        scope=DriverScope.TICKER_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_VOLUME,
        visibility=VisibilityTier.AUDIT_ONLY,
    ),
    _definition(
        "presort-pricing-mix-context", "Presort pricing and mix context", family="price-mix",
        unit=UNIT_QUALITATIVE,
        definition="Source-backed qualitative pricing or mix context explicitly attributable to Presort Services.",
        scope=DriverScope.TICKER_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_PRICE_MIX,
        visibility=VisibilityTier.AUDIT_ONLY,
    ),
    _definition(
        "sendtech-pricing-mix-context", "SendTech pricing and mix context", family="price-mix",
        unit=UNIT_QUALITATIVE,
        definition="Source-backed qualitative pricing or mix context explicitly attributable to SendTech.",
        scope=DriverScope.TICKER_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_PRICE_MIX,
        visibility=VisibilityTier.AUDIT_ONLY,
    ),
)


PBI_PROFILE = TickerShadowProfile(
    ticker="PBI",
    calendar_mode=CalendarMode.CALENDAR_QUARTER,
    calendar_id="calendar:pbi:calendar-year-fiscal@1",
    definitions=PBI_DEFINITIONS,
    mapping_rules=(
        _canonical_rule("PBI", 1, "Volume / throughput", "presort-volume-context", (_dimension("segment", "presort-services", "Presort Services"),), priority=20, required=("presort",), forbidden=("sendtech",)),
        _canonical_rule("PBI", 2, "Volume / throughput", "sendtech-activity-context", (_dimension("segment", "sendtech-solutions", "SendTech Solutions"),), priority=20, required=("sendtech",), forbidden=("presort",)),
        _disposition_rule("PBI", 3, "Volume / throughput", MappingAction.LOW_VALUE_SUPPORT, reason="The extracted text lacks one unambiguous segment identity."),
        _canonical_rule("PBI", 4, "Pricing / mix", "presort-pricing-mix-context", (_dimension("segment", "presort-services", "Presort Services"),), priority=20, required=("presort",), forbidden=("sendtech",)),
        _canonical_rule("PBI", 5, "Pricing / mix", "sendtech-pricing-mix-context", (_dimension("segment", "sendtech-solutions", "SendTech Solutions"),), priority=20, required=("sendtech",), forbidden=("presort",)),
        _disposition_rule("PBI", 6, "Pricing / mix", MappingAction.LOW_VALUE_SUPPORT, reason="The extracted text lacks one unambiguous segment identity."),
        _disposition_rule("PBI", 7, "Balance sheet / financing", MappingAction.GUIDANCE_REFERENCE, owner="owner:guidance:source-native@1", priority=20, required=("guidance",), reason="Explicit forward guidance remains owned by Guidance."),
        _disposition_rule("PBI", 8, "Balance sheet / financing", MappingAction.OWNER_ELSEWHERE, owner="owner:debt-liquidity:source-native@1", reason="Balance-sheet and financing evidence remains owned by Debt/Liquidity or Summary/BS."),
    ),
    source_priority=("10-K", "10-Q", "earnings_release", "presentation", "transcript", "internal_metric"),
)


GPRE_DEFINITIONS = (
    _definition(
        "ethanol-plant-utilization", "Ethanol plant utilization", family="utilization",
        unit=UNIT_PERCENT,
        definition="Reported utilization of the operating ethanol plant portfolio before the accepted eight-plant footprint change.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_LEADING_INDICATOR,
        version=1,
    ),
    _definition(
        "ethanol-plant-utilization", "Ethanol plant utilization", family="utilization",
        unit=UNIT_PERCENT,
        definition="Reported utilization calculated using the eight operating ethanol plants from 2025-Q4 onward.",
        scope=DriverScope.SECTOR_SPECIFIC,
        linkage=FinancialLinkageKind.OPERATING_LEADING_INDICATOR,
        version=2,
    ),
    _definition("ethanol-gallons-produced", "Ethanol gallons produced", family="production", unit=UNIT_MILLION_GALLONS, definition="Reported ethanol gallons produced during the fiscal quarter.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.OPERATING_VOLUME, aggregation=AggregationSemantics.SUMMABLE),
    _definition("ethanol-gallons-sold", "Ethanol gallons sold", family="production", unit=UNIT_MILLION_GALLONS, definition="Reported ethanol gallons sold during the fiscal quarter.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.OPERATING_VOLUME, aggregation=AggregationSemantics.SUMMABLE),
    _definition("corn-consumed", "Corn consumed", family="inputs", unit=UNIT_MILLION_BUSHELS, definition="Reported corn consumed during the fiscal quarter.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.OPERATING_VOLUME, aggregation=AggregationSemantics.SUMMABLE),
    _definition("distillers-grains-produced", "Distillers grains", family="coproducts", unit=UNIT_THOUSAND_TONS, definition="Reported distillers-grains production during the fiscal quarter.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.OPERATING_VOLUME, aggregation=AggregationSemantics.SUMMABLE),
    _definition("ultra-high-protein-produced", "Ultra-high protein", family="coproducts", unit=UNIT_THOUSAND_TONS, definition="Reported ultra-high-protein production during the fiscal quarter.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.OPERATING_VOLUME, aggregation=AggregationSemantics.SUMMABLE),
    _definition("renewable-corn-oil-produced", "Renewable corn oil", family="coproducts", unit=UNIT_MILLION_POUNDS, definition="Reported renewable-corn-oil production during the fiscal quarter.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.OPERATING_VOLUME, aggregation=AggregationSemantics.SUMMABLE),
    _definition("consolidated-ethanol-crush-margin", "Consolidated ethanol crush margin", family="margin", unit=UNIT_USD_MILLION, definition="Reported consolidated ethanol crush-margin measure, without substituting ex-45Z, ex-RIN, or underlying variants.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.MARGIN_CONTEXT, aggregation=AggregationSemantics.SUMMABLE),
    _definition("underlying-crush-margin", "Underlying crush margin", family="margin", unit=UNIT_USD_MILLION, definition="Explicitly reported underlying crush-margin measure, kept separate from consolidated and adjusted variants.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.MARGIN_CONTEXT, aggregation=AggregationSemantics.SUMMABLE),
    _definition("crush-margin-ex-45z", "Crush margin excluding 45Z", family="margin", unit=UNIT_USD_MILLION, definition="Explicitly reported crush margin excluding 45Z value.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.MARGIN_CONTEXT, aggregation=AggregationSemantics.SUMMABLE),
    _definition("crush-margin-ex-rin", "Crush margin excluding RIN", family="margin", unit=UNIT_USD_MILLION, definition="Explicitly reported crush margin excluding RIN impact.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.MARGIN_CONTEXT, aggregation=AggregationSemantics.SUMMABLE),
    _definition("45z-value-realized", "45Z value realized", family="policy-credit", unit=UNIT_USD_MILLION, definition="Explicitly realized 45Z production-tax-credit value for the fiscal quarter.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.MARGIN_CONTEXT, aggregation=AggregationSemantics.SUMMABLE),
    _definition("rin-impact", "RIN impact", family="policy-credit", unit=UNIT_USD_MILLION, definition="Explicitly reported RIN impact or accumulated RIN sale for the fiscal quarter.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.MARGIN_CONTEXT, aggregation=AggregationSemantics.SUMMABLE),
    _definition("45z-monetization-context", "45Z monetization context", family="policy-credit", unit=UNIT_QUALITATIVE, definition="Source-backed qualitative status of 45Z agreements or monetization.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT, visibility=VisibilityTier.SECONDARY),
    _definition("carbon-capture-context", "Carbon capture context", family="carbon", unit=UNIT_QUALITATIVE, definition="Source-backed qualitative status of carbon capture or Advantage Nebraska activity.", scope=DriverScope.TICKER_SPECIFIC, linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT, visibility=VisibilityTier.SECONDARY),
    _definition("input-cost-context", "Corn and natural-gas input-cost context", family="inputs", unit=UNIT_QUALITATIVE, definition="Source-backed qualitative context for corn, natural gas, and other production inputs.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT, visibility=VisibilityTier.SECONDARY),
    _definition("coproduct-context", "Coproduct context", family="coproducts", unit=UNIT_QUALITATIVE, definition="Source-backed qualitative context for distillers grains, ultra-high protein, and coproduct mix.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT, visibility=VisibilityTier.SECONDARY),
    _definition("operating-plants-context", "Operating plants context", family="footprint", unit=UNIT_QUALITATIVE, definition="Source-backed qualitative context on operating plants and ramping status.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.CAPACITY_OR_FOOTPRINT, visibility=VisibilityTier.SECONDARY),
    _definition("risk-management-context", "Risk-management context", family="risk-management", unit=UNIT_QUALITATIVE, definition="Source-backed qualitative risk-management support without treating derivative balances as driver observations.", scope=DriverScope.SECTOR_SPECIFIC, linkage=FinancialLinkageKind.QUALITATIVE_CONTEXT, visibility=VisibilityTier.AUDIT_ONLY),
)


def _gpre_rules() -> tuple[DriverMappingRule, ...]:
    rules: list[DriverMappingRule] = [
        _canonical_rule("GPRE", 1, "Utilization", "ethanol-plant-utilization", PRODUCTION_PORTFOLIO, version=1, effective_through=45930, reason="Reported portfolio utilization before the explicit eight-plant definition."),
        _canonical_rule(
            "GPRE", 2, "Utilization", "ethanol-plant-utilization", PRODUCTION_PORTFOLIO,
            version=2,
            effective_from=46022,
            transition_state=DefinitionContinuityState.DEFINITION_CHANGED_BREAK_SERIES,
            transition_from_version=1,
            reason="Explicit eight-operating-plant utilization definition from 2025-Q4 onward.",
        ),
    ]
    number = 3
    for raw_label, slug in (
        ("Ethanol gallons produced", "ethanol-gallons-produced"),
        ("Ethanol gallons sold", "ethanol-gallons-sold"),
        ("Corn consumed", "corn-consumed"),
        ("Distillers grains", "distillers-grains-produced"),
        ("Ultra-high protein", "ultra-high-protein-produced"),
        ("Renewable corn oil", "renewable-corn-oil-produced"),
        ("Consolidated ethanol crush margin", "consolidated-ethanol-crush-margin"),
        ("Underlying crush margin", "underlying-crush-margin"),
        ("Crush margin ex-45Z", "crush-margin-ex-45z"),
        ("Crush margin ex-RIN", "crush-margin-ex-rin"),
        ("45Z value realized", "45z-value-realized"),
        ("RIN impact / accumulated RIN sale", "rin-impact"),
        ("45Z agreement / monetization status", "45z-monetization-context"),
        ("Carbon capture / Advantage Nebraska status", "carbon-capture-context"),
        ("Corn / natural gas / input-cost commentary", "input-cost-context"),
        ("Distillers grains / Ultra-high protein commentary", "coproduct-context"),
        ("Protein / coproduct mix", "coproduct-context"),
        ("Operating plants online / ramping", "operating-plants-context"),
        ("Risk management support", "risk-management-context"),
    ):
        rules.append(_canonical_rule("GPRE", number, raw_label, slug, PRODUCTION_PORTFOLIO)); number += 1
    for raw_label in (
        "45Z accounting treatment / COGS reduction",
        "45Z included in Adjusted EBITDA",
        "45Z included in ethanol-production COGS/crush",
        "45Z production tax credits receivable/current asset",
        "Adjusted EBITDA ex-45Z / base business",
        "Adjusted EBITDA reported",
        "Cash-flow hedge reclass to P&L",
        "Derivative AOCI",
        "Derivative OCI movement",
        "Derivative P&L in COGS",
        "Derivative P&L in revenue",
        "Net derivative asset/liability",
        "Production tax credits WC increase",
        "Total derivative P&L",
    ):
        rules.append(
            _disposition_rule(
                "GPRE", number, raw_label, MappingAction.OWNER_ELSEWHERE,
                owner="owner:financial-products:source-native@1",
                reason="Financial statement, working-capital, EBITDA, or derivative facts remain owned by their canonical financial product.",
            )
        ); number += 1
    rules.append(
        _disposition_rule(
            "GPRE", number, "45Z value guided", MappingAction.GUIDANCE_REFERENCE,
            owner="owner:guidance:source-native@1",
            reason="Management guidance is retained as guidance evidence, never actual realized 45Z value.",
        )
    )
    return tuple(rules)


GPRE_PROFILE = TickerShadowProfile(
    ticker="GPRE",
    calendar_mode=CalendarMode.CALENDAR_QUARTER,
    calendar_id="calendar:gpre:calendar-year-fiscal@1",
    mapping_rules=_gpre_rules(),
    definitions=GPRE_DEFINITIONS,
    source_priority=("10-K", "10-Q", "earnings_release", "presentation", "transcript", "internal_metric"),
)


OPERATING_DRIVER_SHADOW_PROFILES = {
    "ANF": ANF_PROFILE,
    "PBI": PBI_PROFILE,
    "GPRE": GPRE_PROFILE,
}


def operating_driver_shadow_profile(ticker: str) -> TickerShadowProfile:
    """Return one closed declarative profile; no ticker-specific runtime branch."""

    normalized = str(ticker).upper()
    try:
        return OPERATING_DRIVER_SHADOW_PROFILES[normalized]
    except KeyError as exc:
        raise KeyError(f"No accepted Operating Drivers shadow profile for {normalized!r}.") from exc
