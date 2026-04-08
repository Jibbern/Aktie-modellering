"""Ticker-specific workbook, guidance, and overlay configuration."""
from __future__ import annotations

import dataclasses
import re
from typing import Any, Dict, Optional, Pattern, Tuple


@dataclasses.dataclass(frozen=True)
class OperatingDriverTemplate:
    group: str
    label: str
    why_it_matters: str
    match_terms: Tuple[str, ...]
    metric_candidates: Tuple[str, ...] = tuple()
    aliases: Tuple[str, ...] = tuple()
    preferred_unit: str = ""
    key: str = ""


@dataclasses.dataclass(frozen=True)
class EconomicsOverlayCoefficient:
    label: str
    unit: str
    aliases: Tuple[str, ...] = tuple()
    default_value: Optional[float] = None
    default_basis: str = ""
    default_source: str = ""
    key: str = ""


@dataclasses.dataclass(frozen=True)
class EconomicsOverlayMarketInput:
    label: str
    unit: str
    aliases: Tuple[str, ...] = tuple()
    key: str = ""
    source_series_keys: Tuple[str, ...] = tuple()
    preferred_regions: Tuple[str, ...] = tuple()
    aggregation_preference: str = "quarter_avg"
    proxy_base_key: str = ""
    proxy_premium_key: str = ""


@dataclasses.dataclass(frozen=True)
class EconomicsOverlayHedgeTemplate:
    label: str
    exposure_unit: str = ""
    aliases: Tuple[str, ...] = tuple()
    key: str = ""


@dataclasses.dataclass(frozen=True)
class EconomicsOverlayBridgeRow:
    label: str
    key: str
    unit: str = "$m"


@dataclasses.dataclass(frozen=True)
class SourceMaterialSeed:
    family: str
    seed_url: str
    follow_detail_pages: bool = False
    allowed_hosts: Tuple[str, ...] = tuple()


@dataclasses.dataclass(frozen=True)
class CompanyProfile:
    ticker: str
    has_bank: bool
    industry_keywords: Tuple[str, ...]
    segment_patterns: Tuple[Tuple[str, Pattern[str]], ...]
    segment_alias_patterns: Tuple[Tuple[Pattern[str], str], ...]
    key_adv_require_keywords: Tuple[str, ...]
    key_adv_deny_keywords: Tuple[str, ...]
    commentary_prefer_terms: Tuple[str, ...] = tuple()
    commentary_deny_terms: Tuple[str, ...] = tuple()
    enable_operating_drivers_sheet: bool = False
    enable_economics_overlay_sheet: bool = False
    enable_economics_market_raw_sheet: bool = False
    enable_annual_segment_block: bool = True
    enable_quarterly_segment_block: bool = False
    quarterly_segment_labels: Tuple[str, ...] = tuple()
    annual_segment_labels: Tuple[str, ...] = tuple()
    annual_segment_alias_patterns: Tuple[Tuple[Pattern[str], str], ...] = tuple()
    quarter_note_priority_terms: Tuple[str, ...] = tuple()
    promise_priority_terms: Tuple[str, ...] = tuple()
    operating_driver_templates: Tuple[OperatingDriverTemplate, ...] = tuple()
    operating_driver_history_templates: Tuple[OperatingDriverTemplate, ...] = tuple()
    economics_overlay_coefficients: Tuple[EconomicsOverlayCoefficient, ...] = tuple()
    economics_overlay_market_inputs: Tuple[EconomicsOverlayMarketInput, ...] = tuple()
    economics_overlay_hedge_templates: Tuple[EconomicsOverlayHedgeTemplate, ...] = tuple()
    economics_overlay_bridge_rows: Tuple[EconomicsOverlayBridgeRow, ...] = tuple()
    enabled_market_sources: Tuple[str, ...] = tuple()
    official_source_seeds: Tuple[SourceMaterialSeed, ...] = tuple()
    thesis_bridge_labels: Tuple[str, ...] = tuple()
    summary_description_fallback: str = ""
    summary_key_advantage_fallback: str = ""
    summary_segment_operating_model_fallbacks: Tuple[str, ...] = tuple()
    summary_dependency_fallbacks: Tuple[str, ...] = tuple()
    summary_wrong_thesis_fallbacks: Tuple[str, ...] = tuple()


def _compile_segments(
    items: Tuple[Tuple[str, str], ...]
) -> Tuple[Tuple[str, Pattern[str]], ...]:
    return tuple((name, re.compile(rx, re.I)) for name, rx in items)


def _compile_aliases(
    items: Tuple[Tuple[str, str], ...]
) -> Tuple[Tuple[Pattern[str], str], ...]:
    return tuple((re.compile(rx, re.I), label) for rx, label in items)


def _driver_key(label: str) -> str:
    txt = re.sub(r"[^a-z0-9]+", "_", str(label or "").strip().lower())
    return txt.strip("_") or "driver"


def _drivers(
    items: Tuple[
        Tuple[
            str,
            str,
            str,
            Tuple[str, ...],
            Tuple[str, ...],
        ],
        ...,
    ]
) -> Tuple[OperatingDriverTemplate, ...]:
    out: list[OperatingDriverTemplate] = []
    for item in items:
        group, label, why_it_matters, match_terms, metric_candidates, *rest = item
        aliases: Tuple[str, ...] = tuple()
        preferred_unit = ""
        key = ""
        if len(rest) >= 1 and rest[0] is not None:
            aliases = tuple(rest[0])
        if len(rest) >= 2 and rest[1] is not None:
            preferred_unit = str(rest[1])
        if len(rest) >= 3 and rest[2] is not None:
            key = str(rest[2])
        out.append(
            OperatingDriverTemplate(
                group=group,
                label=label,
                why_it_matters=why_it_matters,
                match_terms=match_terms,
                metric_candidates=metric_candidates,
                aliases=aliases,
                preferred_unit=preferred_unit,
                key=key or _driver_key(label),
            )
        )
    return tuple(out)


def _overlay_coefficients(
    items: Tuple[
        Tuple[str, str, Tuple[str, ...], Optional[float], str, str, Optional[str]],
        ...,
    ]
) -> Tuple[EconomicsOverlayCoefficient, ...]:
    out: list[EconomicsOverlayCoefficient] = []
    for label, unit, aliases, default_value, default_basis, default_source, *rest in items:
        key = str(rest[0] if rest else "") or _driver_key(label)
        out.append(
            EconomicsOverlayCoefficient(
                label=label,
                unit=unit,
                aliases=tuple(aliases or ()),
                default_value=default_value,
                default_basis=str(default_basis or ""),
                default_source=str(default_source or ""),
                key=key,
            )
        )
    return tuple(out)


def _overlay_market_inputs(
    items: Tuple[Tuple[Any, ...], ...]
) -> Tuple[EconomicsOverlayMarketInput, ...]:
    out: list[EconomicsOverlayMarketInput] = []
    for label, unit, aliases, *rest in items:
        key = str(rest[0] if rest else "") or _driver_key(label)
        source_series_keys: Tuple[str, ...] = tuple(rest[1] or ()) if len(rest) >= 2 else tuple()
        preferred_regions: Tuple[str, ...] = tuple(rest[2] or ()) if len(rest) >= 3 else tuple()
        aggregation_preference = str(rest[3] if len(rest) >= 4 else "") or "quarter_avg"
        proxy_base_key = str(rest[4] if len(rest) >= 5 else "") or ""
        proxy_premium_key = str(rest[5] if len(rest) >= 6 else "") or ""
        out.append(
            EconomicsOverlayMarketInput(
                label=label,
                unit=unit,
                aliases=tuple(aliases or ()),
                key=key,
                source_series_keys=source_series_keys,
                preferred_regions=preferred_regions,
                aggregation_preference=aggregation_preference,
                proxy_base_key=proxy_base_key,
                proxy_premium_key=proxy_premium_key,
            )
        )
    return tuple(out)


def _overlay_hedges(
    items: Tuple[Tuple[str, str, Tuple[str, ...], Optional[str]], ...]
) -> Tuple[EconomicsOverlayHedgeTemplate, ...]:
    out: list[EconomicsOverlayHedgeTemplate] = []
    for label, exposure_unit, aliases, *rest in items:
        key = str(rest[0] if rest else "") or _driver_key(label)
        out.append(
            EconomicsOverlayHedgeTemplate(
                label=label,
                exposure_unit=str(exposure_unit or ""),
                aliases=tuple(aliases or ()),
                key=key,
            )
        )
    return tuple(out)


def _overlay_bridge_rows(
    items: Tuple[Tuple[str, str, Optional[str]], ...]
) -> Tuple[EconomicsOverlayBridgeRow, ...]:
    out: list[EconomicsOverlayBridgeRow] = []
    for label, key, *rest in items:
        unit = str(rest[0] if rest else "") or "$m"
        out.append(EconomicsOverlayBridgeRow(label=label, key=str(key), unit=unit))
    return tuple(out)


_DENY_COMMON = (
    "indenture",
    "securities act",
    "section 3(a)(9)",
    "administrative agent",
    "subscription",
    "offering",
    "registration",
    "forward-looking statements",
    "safe harbor",
    "loan documents",
    "notes due",
    "convertible notes",
)

_DEFAULT_ECONOMICS_OVERLAY_COEFFICIENTS = _overlay_coefficients(
    (
        ("Ethanol yield", "gal/bushel", ("ethanol yield", "gallons per bushel"), None, "", "", "ethanol_yield"),
        ("Renewable corn oil yield", "lbs/bushel", ("corn oil yield", "renewable corn oil yield"), None, "", "", "renewable_corn_oil_yield"),
        ("Distillers yield", "lbs/bushel", ("distillers yield", "distillers grains per bushel"), None, "", "", "distillers_yield"),
        ("Ultra-high protein yield", "lbs/bushel", ("uhp yield", "ultra-high protein yield"), None, "", "", "uhp_yield"),
        ("Natural gas usage", "BTU/gal", ("natural gas usage", "gas usage", "btu per gallon"), None, "", "", "natural_gas_usage"),
        ("Electricity usage", "kWh/gal", ("electricity usage", "kwh per gallon"), None, "", "", "electricity_usage"),
    )
)

_DEFAULT_ECONOMICS_OVERLAY_MARKET_INPUTS = _overlay_market_inputs(
    (
        (
            "Corn price",
            "$/bushel",
            ("corn price", "corn"),
            "corn_price",
            (
                "corn_cash_gpre_core",
                "corn_cash_nebraska",
                "corn_cash_iowa_east",
                "corn_cash_iowa_west",
                "corn_nebraska",
                "corn_iowa_east",
                "corn_iowa_west",
                "cbot_corn_usd_per_bu",
            ),
            ("gpre_core", "nebraska", "iowa", "midwest", "cbot"),
            "quarter_avg",
        ),
        (
            "Ethanol price",
            "$/gal",
            ("ethanol price", "ethanol"),
            "ethanol_price",
            (
                "ethanol_gpre_core",
                "ethanol_iowa_avg",
                "ethanol_iowa",
                "ethanol_nebraska",
                "ethanol_illinois",
                "ethanol_indiana",
            ),
            ("gpre_core", "iowa", "nebraska", "midwest"),
            "quarter_avg",
        ),
        (
            "Distillers grains price",
            "$/lb",
            ("distillers grains price", "distillers grains", "ddgs price"),
            "distillers_grains_price",
            (
                "ddgs_gpre_core",
                "ddgs_10_iowa_avg",
                "ddgs_10_iowa",
                "ddgs_10_nebraska",
                "ddgs_10_iowa_east",
                "ddgs_10_iowa_west",
            ),
            ("gpre_core", "iowa", "nebraska", "midwest"),
            "quarter_avg",
        ),
        ("Ultra-high protein price", "$/lb", ("uhp price", "ultra-high protein price"), "uhp_price"),
        (
            "Renewable corn oil price",
            "$/lb",
            ("renewable corn oil price", "corn oil price"),
            "renewable_corn_oil_price",
            (
                "corn_oil_gpre_core",
                "corn_oil_iowa_avg",
                "corn_oil_nebraska",
                "corn_oil_iowa_east",
                "corn_oil_iowa_west",
            ),
            ("gpre_core", "iowa", "nebraska", "midwest"),
            "quarter_avg",
        ),
        (
            "Natural gas price",
            "$/MMBtu",
            ("natural gas price", "nat gas price"),
            "natural_gas_price",
            ("nymex_gas",),
            ("nymex",),
            "quarter_avg",
        ),
        (
            "Soybean oil price proxy",
            "$/lb",
            ("soybean oil price proxy", "soybean oil"),
            "soybean_oil_price_proxy",
            tuple(),
            tuple(),
            "quarter_avg",
        ),
        (
            "Corn oil premium assumption",
            "$/lb",
            ("corn oil premium assumption", "corn oil premium"),
            "corn_oil_premium_assumption",
            tuple(),
            tuple(),
            "quarter_avg",
        ),
        (
            "Implied renewable corn oil proxy price",
            "$/lb",
            ("implied renewable corn oil proxy price", "corn oil proxy price"),
            "implied_renewable_corn_oil_proxy_price",
            tuple(),
            tuple(),
            "quarter_avg",
            "soybean_oil_price_proxy",
            "corn_oil_premium_assumption",
        ),
    )
)

_DEFAULT_ECONOMICS_OVERLAY_HEDGES = _overlay_hedges(
    (
        ("Corn", "bushels", ("corn", "corn futures", "corn hedge"), "corn"),
        ("Ethanol", "gallons", ("ethanol", "ethanol futures", "ethanol hedge"), "ethanol"),
        ("Distillers grains", "tons", ("distillers grains", "ddgs"), "distillers_grains"),
        ("Renewable corn oil", "lbs", ("renewable corn oil", "corn oil"), "renewable_corn_oil"),
        ("Natural gas", "MMBtu", ("natural gas", "nat gas"), "natural_gas"),
        ("Soybean oil proxy", "lbs", ("soybean oil",), "soybean_oil_proxy"),
        ("Soybean meal proxy", "tons", ("soybean meal",), "soybean_meal_proxy"),
    )
)

_DEFAULT_ECONOMICS_OVERLAY_BRIDGE_ROWS = _overlay_bridge_rows(
    (
        ("Reported consolidated crush margin", "reported_consolidated_crush_margin", "$m"),
        ("45Z impact", "45z", "$m"),
        ("RIN impact", "rin_sale", "$m"),
        ("Inventory NRV / lower-of-cost adjustment", "inventory_lcnrv", "$m"),
        ("Non-ethanol operating activities", "intercompany_nonethanol_net", "$m"),
        ("Impairment / held-for-sale effects", "impairment_assets_held_for_sale", "$m"),
        ("Other explicit bridge items", "other_bridge_items", "$m"),
        ("Underlying crush margin", "underlying_crush_margin", "$m"),
    )
)


COMPANY_PROFILES: Dict[str, CompanyProfile] = {
    "PBI": CompanyProfile(
        ticker="PBI",
        has_bank=True,
        industry_keywords=(
            "mailing",
            "shipping",
            "presort",
            "sending technology",
            "postage",
            "parcel",
            "customer communication",
        ),
        segment_patterns=_compile_segments(
            (
                ("SendTech", r"\b(sendtech|sending technology|mailing|shipping)\b"),
                ("Presort", r"\bpresort\b"),
                ("Global Ecommerce", r"\b(global ecommerce|global e-commerce|cross[- ]border)\b"),
            )
        ),
        segment_alias_patterns=_compile_aliases(
            (
                (r"\b(sendtech|sending technology)\b", "SendTech Solutions"),
                (r"\bpresort\b", "Presort Services"),
                (r"\bglobal e-?commerce\b", "Global Ecommerce"),
                (r"\bother\b", "Other operations"),
            )
        ),
        key_adv_require_keywords=(
            "advantage",
            "competitive",
            "scale",
            "network",
            "installed base",
            "automation",
            "software",
            "recurring",
            "subscription",
            "integrated",
            "cross-sell",
        ),
        key_adv_deny_keywords=_DENY_COMMON,
        commentary_prefer_terms=(
            "revenue",
            "volume",
            "volumes",
            "pricing",
            "mix",
            "margin",
            "adjusted ebit",
            "operating profit",
            "customer losses",
            "migration",
            "recurring revenue",
            "price concessions",
            "labor productivity",
            "transportation costs",
            "meter base",
            "product lifecycle",
            "first class",
            "marketing mail",
            "cross-border",
            "domestic parcel revenue per piece",
            "favorable revenue mix",
            "supply chain",
            "lower cogs",
            "sg&a",
        ),
        commentary_deny_terms=(
            "ethanol",
            "crush",
            "distillers grains",
            "ddgs",
            "rin",
            "45z",
            "corn oil",
            "ultra-high protein",
            "e15",
            "biorefin",
            "bushel",
            "gallons",
        ),
        enable_operating_drivers_sheet=True,
        enable_economics_overlay_sheet=False,
        enable_economics_market_raw_sheet=False,
        enable_annual_segment_block=True,
        enable_quarterly_segment_block=True,
        quarterly_segment_labels=(
            "SendTech Solutions",
            "Presort Services",
            "Other operations",
            "Corporate expense",
        ),
        annual_segment_labels=(
            "SendTech Solutions",
            "Presort Services",
            "Other operations",
            "Corporate expense",
            "Corporate assets",
            "Intersegment eliminations",
        ),
        annual_segment_alias_patterns=_compile_aliases(
            (
                (r"\bsendtech(?: solutions)?\b", "SendTech Solutions"),
                (r"\bpresort(?: services)?\b", "Presort Services"),
                (r"\bother operations?\b", "Other operations"),
                (r"\bcorporate expense\b", "Corporate expense"),
                (r"\bcorporate assets?\b", "Corporate assets"),
                (r"\bintersegment eliminations?\b", "Intersegment eliminations"),
            )
        ),
        quarter_note_priority_terms=(
            "adjusted ebit",
            "margin",
            "free cash flow",
            "guidance",
            "cost savings",
            "pb bank",
            "liquidity",
            "trapped capital",
            "shipping",
            "mailing",
            "presort",
            "sendtech",
            "automation",
            "network",
            "deleveraging",
        ),
        promise_priority_terms=(
            "adjusted ebit",
            "guidance",
            "target",
            "cost savings",
            "margin",
            "deleveraging",
            "free cash flow",
            "liquidity",
            "trapped capital",
            "pb bank",
        ),
        operating_driver_templates=_drivers(
            (
                (
                    "Demand / volume",
                    "Parcel / mail volumes",
                    "Volumes influence throughput, mix, and operating leverage.",
                    ("volume", "mail", "parcel", "presort"),
                    ("revenue",),
                ),
                (
                    "Price / mix",
                    "Pricing / mix",
                    "Pricing and product mix shape gross profit and EBITDA conversion.",
                    ("pricing", "mix", "subscription", "software"),
                    ("revenue", "adj_ebitda", "ebitda"),
                ),
                (
                    "Capital / financing / structural",
                    "Debt / capital allocation",
                    "Leverage and capital allocation affect equity value and optionality.",
                    ("debt", "refinancing", "buyback", "dividend", "leverage"),
                    ("debt_core", "cash"),
                ),
            )
        ),
        operating_driver_history_templates=_drivers(
            (
                (
                    "Demand / volume",
                    "Volume / throughput",
                    "Volumes help frame revenue conversion and operating leverage.",
                    ("volume", "throughput", "shipments", "parcel", "mail"),
                    tuple(),
                    ("throughput", "shipments"),
                    "",
                    "volume_throughput",
                ),
                (
                    "Price / mix",
                    "Pricing / mix",
                    "Pricing and mix influence gross profit and EBITDA conversion.",
                    ("pricing", "mix", "yield", "subscription", "software"),
                    ("revenue",),
                    ("price", "mix"),
                    "",
                    "pricing_mix",
                ),
                (
                    "Capital / financing / structural",
                    "Balance sheet / financing",
                    "Funding and capital allocation shape optionality and equity value.",
                    ("debt", "liquidity", "refinancing", "capital allocation"),
                    ("debt_core", "cash"),
                    ("debt", "capital"),
                    "",
                    "balance_sheet_financing",
                ),
            )
        ),
        economics_overlay_coefficients=_DEFAULT_ECONOMICS_OVERLAY_COEFFICIENTS,
        economics_overlay_market_inputs=_DEFAULT_ECONOMICS_OVERLAY_MARKET_INPUTS,
        economics_overlay_hedge_templates=_DEFAULT_ECONOMICS_OVERLAY_HEDGES,
        economics_overlay_bridge_rows=_DEFAULT_ECONOMICS_OVERLAY_BRIDGE_ROWS,
        enabled_market_sources=("nwer", "ams_3617", "cme_ethanol_platts"),
        official_source_seeds=(
            SourceMaterialSeed(
                family="earnings_presentation",
                seed_url="https://www.investorrelations.pitneybowes.com/events-and-presentations/presentations",
                follow_detail_pages=False,
                allowed_hosts=("www.investorrelations.pitneybowes.com",),
            ),
            SourceMaterialSeed(
                family="press_release",
                seed_url="https://www.investorrelations.pitneybowes.com/financial-information/quarterly-results",
                follow_detail_pages=False,
                allowed_hosts=("www.investorrelations.pitneybowes.com",),
            ),
            SourceMaterialSeed(
                family="earnings_transcripts",
                seed_url="https://www.investorrelations.pitneybowes.com/financial-information/quarterly-results",
                follow_detail_pages=False,
                allowed_hosts=("www.investorrelations.pitneybowes.com",),
            ),
        ),
        thesis_bridge_labels=(
            "Base Adj EBITDA FY",
            "Pricing / mix uplift",
            "Cost savings uplift",
            "Interest savings / debt-paydown uplift",
            "Other",
        ),
        summary_description_fallback=(
            "Pitney Bowes provides shipping and mailing technology, presort services, "
            "and related financial services. Its core operations are SendTech Solutions "
            "and Presort Services, with earnings and liquidity also influenced by PB Bank "
            "and ongoing cost rationalization."
        ),
        summary_key_advantage_fallback=(
            "Pitney Bowes combines a large installed base in mailing and shipping, a national "
            "presort network, and software-enabled workflow tools, which can support recurring "
            "revenue, customer retention, and operating leverage as the portfolio is simplified."
        ),
        summary_segment_operating_model_fallbacks=(
            "SendTech Solutions: provides mailing, shipping, locker, and related software, equipment, supplies, and financial services that support mailers and shippers.",
            "Presort Services: processes mail and sortation workshare volumes to help clients qualify for postal discounts and improve delivery economics.",
        ),
        summary_dependency_fallbacks=(
            "USPS / postal dependency: Postal pricing, mailing volumes, and product economics affect demand in core mailing and presort businesses.",
            "PB Bank liquidity / trapped capital: Parent liquidity and deleveraging depend on releasing funding and trapped capital from PB Bank.",
            "SendTech / Presort demand: Shipping, mailing, and presort volumes and mix drive revenue conversion and margin performance.",
            "Cost rationalization execution: Earnings improvement depends on sustaining cost savings without disrupting service or customer retention.",
            "Leverage / refinancing risk: Debt reduction and refinancing progress remain important to cash flow flexibility and equity value.",
        ),
        summary_wrong_thesis_fallbacks=(
            "Mailing and shipping volumes weaken faster than pricing and productivity can offset.",
            "PB Bank liquidity release or trapped-capital reduction takes longer than expected.",
            "Cost savings fall short or pressure service levels, retention, or execution.",
            "SendTech or Presort margins fail to improve despite simplification efforts.",
            "Leverage, refinancing, or funding constraints limit capital flexibility.",
        ),
    ),
    "GPRE": CompanyProfile(
        ticker="GPRE",
        has_bank=False,
        industry_keywords=(
            "ethanol",
            "renewable fuel",
            "low carbon",
            "biorefin",
            "protein",
            "corn oil",
            "agribusiness",
            "energy services",
            "45z",
            "clean sugar technology",
            "msc",
        ),
        segment_patterns=_compile_segments(
            (
                ("Ethanol", r"\b(ethanol|renewable fuels?|fuel ethanol)\b"),
                (
                    "Agribusiness and Energy Services",
                    r"\b(agribusiness|energy services|commodity marketing|grain)\b",
                ),
                ("Partnership", r"\b(partnership|green plains partners|gpp)\b"),
            )
        ),
        segment_alias_patterns=_compile_aliases(
            (
                (r"\bethanol\b", "Ethanol"),
                (r"\b(agribusiness|energy services)\b", "Agribusiness and Energy Services"),
                (r"\b(partnership|green plains partners|gpp)\b", "Partnership"),
            )
        ),
        key_adv_require_keywords=(
            "advantage",
            "competitive",
            "low carbon",
            "cost position",
            "biorefin",
            "high-protein",
            "protein",
            "corn oil",
            "platform",
            "efficiency",
            "network",
            "integrated",
            "45z",
            "msc",
        ),
        key_adv_deny_keywords=_DENY_COMMON,
        commentary_prefer_terms=(
            "ethanol",
            "crush",
            "margin",
            "corn oil",
            "ddgs",
            "protein",
            "ultra-high protein",
            "45z",
            "rin",
            "export",
            "exports",
            "e15",
            "utilization",
            "maintenance",
            "downtime",
            "capacity utilization",
            "corn",
            "yield",
            "tharaldson",
            "fairmont",
            "care and maintenance",
            "spring maintenance season",
            "production cost",
            "domestic blending",
            "hedged",
            "logged in",
        ),
        commentary_deny_terms=(
            "50 pro product",
            "sequence",
            "pet food customer",
            "pet food customers",
            "sendtech",
            "presort",
            "parcel",
            "postage",
            "mailing",
            "shipping",
            "saas",
            "locker",
            "lease extension",
            "cross-border",
            "digital shipping",
        ),
        enable_operating_drivers_sheet=True,
        enable_economics_overlay_sheet=True,
        enable_economics_market_raw_sheet=True,
        enable_annual_segment_block=True,
        annual_segment_labels=(
            "Ethanol production",
            "Agribusiness and energy services",
            "Corporate activities",
            "Corporate assets",
            "Intersegment eliminations",
        ),
        annual_segment_alias_patterns=_compile_aliases(
            (
                (r"\bethanol production\b", "Ethanol production"),
                (r"\bagribusiness and energy services\b", "Agribusiness and energy services"),
                (r"\bcorporate activities\b", "Corporate activities"),
                (r"\bcorporate assets\b", "Corporate assets"),
                (r"\bintersegment eliminations?\b", "Intersegment eliminations"),
            )
        ),
        quarter_note_priority_terms=(
            "45z",
            "monetization",
            "advantage nebraska",
            "york",
            "central city",
            "wood river",
            "obion",
            "utilization",
            "risk management",
            "cost reduction",
            "annualized savings",
            "carbon capture",
            "fully operational",
            "online",
            "ramping",
            "junior mezzanine debt",
            "adjusted ebitda target",
        ),
        promise_priority_terms=(
            "45z",
            "target",
            "guidance",
            "cost reduction",
            "annualized savings",
            "debt reduction",
            "balance sheet",
            "carbon capture",
            "fully operational",
            "online",
            "ramping",
            "utilization",
        ),
        operating_driver_templates=_drivers(
            (
                (
                    "Inputs / costs",
                    "Corn",
                    "Corn is a core feedstock and directly influences crush margin.",
                    ("corn", "feedstock"),
                    tuple(),
                ),
                (
                    "Inputs / costs",
                    "Natural gas",
                    "Energy input costs influence plant economics and cash margins.",
                    ("natural gas", "gas cost", "energy cost"),
                    tuple(),
                ),
                (
                    "Outputs / realizations",
                    "Ethanol",
                    "Ethanol realizations drive top line and crush economics.",
                    ("ethanol", "crush margin", "realization"),
                    ("revenue",),
                ),
                (
                    "Outputs / realizations",
                    "Renewable corn oil",
                    "Corn oil pricing and low-CI positioning can support coproduct value.",
                    ("corn oil", "renewable corn oil", "low-ci"),
                    tuple(),
                ),
                (
                    "Outputs / realizations",
                    "Distillers grains / Ultra-high protein",
                    "Protein mix and coproduct pricing affect margin quality.",
                    ("protein", "ultra-high protein", "distillers grains", "coproduct"),
                    tuple(),
                ),
                (
                    "Policy / regulation / credits",
                    "45Z / CI advantage / monetization",
                    "45Z and CI monetization can materially change EBITDA and cash flow.",
                    ("45z", "tax credit", "monetization", "ci advantage", "low-carbon"),
                    ("adj_ebitda", "adj_ebit"),
                ),
                (
                    "Utilization / operating intensity",
                    "Utilization / plant ramp / carbon capture status",
                    "Plant uptime, ramp progress, and carbon capture status affect throughput and margin capture.",
                    ("utilization", "fully operational", "online", "ramping", "carbon capture", "york", "central city", "wood river"),
                    tuple(),
                ),
                (
                    "Capital / financing / structural",
                    "Debt reduction / balance-sheet improvement",
                    "Debt reduction improves optionality, lowers interest burden, and changes equity value sensitivity.",
                    ("debt", "delever", "obion", "junior mezzanine", "balance sheet"),
                    ("debt_core", "interest_paid"),
                ),
            )
        ),
        operating_driver_history_templates=_drivers(
            (
                (
                    "Utilization / operating intensity",
                    "Utilization",
                    "Plant utilization affects throughput, absorption, and margin capture.",
                    ("utilization", "operating rate", "capacity utilization", "stated capacity"),
                    tuple(),
                    ("production at", "stated capacity"),
                    "%",
                    "utilization",
                ),
                (
                    "Production / volume",
                    "Ethanol gallons produced / sold",
                    "Production and shipped gallons frame volume recovery and operating leverage.",
                    ("ethanol", "gallons", "production"),
                    tuple(),
                    ("ethanol production", "ethanol sold", "ethanol gallons"),
                    "m gallons",
                    "ethanol_gallons",
                ),
                (
                    "Margin / spread",
                    "Consolidated ethanol crush margin",
                    "Crush margin is a direct driver of segment economics and cash generation.",
                    ("crush margin", "ethanol crush margin"),
                    tuple(),
                    ("crush margin",),
                    "$m",
                    "consolidated_ethanol_crush_margin",
                ),
                (
                    "Margin / spread",
                    "Crush margin ex-45Z",
                    "Ex-45Z crush margin helps separate policy-credit benefit from base operating economics.",
                    ("crush margin ex 45z", "crush margin excluding 45z", "45z benefit"),
                    tuple(),
                    ("ex-45z", "excluding 45z"),
                    "$m",
                    "crush_margin_ex_45z",
                ),
                (
                    "Margin / spread",
                    "RIN impact / accumulated RIN sale",
                    "Explicit RIN-sale benefit helps reconcile reported crush margin to an ex-RIN view.",
                    ("accumulated rins", "sale of accumulated rins", "rins of", "rin benefit"),
                    tuple(),
                    ("rin sale", "rin impact"),
                    "$m",
                    "rin_impact_accumulated_rin_sale",
                ),
                (
                    "Margin / spread",
                    "Crush margin ex-RIN",
                    "Ex-RIN crush margin helps isolate underlying spread economics from accumulated RIN-sale benefit.",
                    ("crush margin ex rin", "crush margin excluding rin", "accumulated rins", "rin benefit"),
                    tuple(),
                    ("ex-rin", "excluding rin"),
                    "$m",
                    "crush_margin_ex_rin",
                ),
                (
                    "Margin / spread",
                    "Underlying crush margin",
                    "Underlying crush margin is the cleanest same-basis view when policy or one-time items are explicitly separable.",
                    ("underlying crush margin", "adjusted crush margin", "crush margin ex"),
                    tuple(),
                    ("underlying crush", "adjusted crush"),
                    "$m",
                    "underlying_crush_margin",
                ),
                (
                    "Coproducts / mix",
                    "Distillers grains",
                    "Distillers grains volume helps explain coproduct contribution and mix quality.",
                    ("distillers grains", "dry equivalent"),
                    tuple(),
                    ("distillers grains",),
                    "k tons",
                    "distillers_grains",
                ),
                (
                    "Coproducts / mix",
                    "Ultra-high protein",
                    "Ultra-high protein volume helps track mix improvement and higher-value coproduct conversion.",
                    ("ultra-high protein", "uhp"),
                    tuple(),
                    ("ultra-high protein", "uhp"),
                    "k tons",
                    "ultra_high_protein",
                ),
                (
                    "Production / volume",
                    "Corn consumed",
                    "Corn processed helps anchor throughput, yields, and feedstock intensity.",
                    ("corn processed", "bushels of corn", "corn consumed"),
                    tuple(),
                    ("corn processed", "corn bushels"),
                    "m bushels",
                    "corn_consumed",
                ),
                (
                    "Policy / credits",
                    "45Z value realized",
                    "Realized 45Z value shows the earnings/cash effect already captured.",
                    ("45z", "production tax credits", "income tax benefit"),
                    tuple(),
                    ("45z realized", "45z production tax credits"),
                    "$m",
                    "45z_value_realized",
                ),
                (
                    "Policy / credits",
                    "45Z value guided",
                    "Guided 45Z value informs forward earnings and monetization potential.",
                    ("45z", "monetization", "adjusted ebitda", "opportunity", "generation"),
                    tuple(),
                    ("45z guidance", "45z monetization", "45z target"),
                    "$m",
                    "45z_value_guided",
                ),
                (
                    "Coproducts / mix",
                    "Renewable corn oil",
                    "Corn oil yields and pricing support coproduct earnings quality.",
                    ("renewable corn oil", "corn oil"),
                    tuple(),
                    ("renewable corn oil",),
                    "m lbs",
                    "renewable_corn_oil",
                ),
                (
                    "Coproducts / mix",
                    "Protein / coproduct mix",
                    "Protein and coproduct mix help explain margin quality and differentiation.",
                    ("protein", "ultra-high protein", "distillers grains", "coproduct"),
                    tuple(),
                    ("protein", "distillers grains"),
                    "",
                    "protein_coproduct_mix",
                ),
                (
                    "Risk management",
                    "Risk management support",
                    "Disciplined hedging/commercial management can support margins and cash flow.",
                    ("risk management", "hedging", "cash flow", "margins"),
                    tuple(),
                    ("risk management", "hedging"),
                    "",
                    "risk_management_support",
                ),
                (
                    "Policy / credits",
                    "45Z agreement / monetization status",
                    "Agreement execution and qualification progress affect timing and realization certainty.",
                    ("45z", "agreement executed", "monetization"),
                    tuple(),
                    ("agreement executed", "monetization status"),
                    "",
                    "45z_agreement_status",
                ),
                (
                    "Utilization / operating intensity",
                    "Carbon capture / Advantage Nebraska status",
                    "Carbon capture readiness and Nebraska platform status drive low-CI economics.",
                    ("advantage nebraska", "carbon capture", "york", "central city", "wood river", "online", "ramping", "fully operational"),
                    tuple(),
                    ("carbon capture", "advantage nebraska"),
                    "",
                    "carbon_capture_status",
                ),
                (
                    "Utilization / operating intensity",
                    "Operating plants online / ramping",
                    "Plant ramp and online status affect throughput and qualification readiness.",
                    ("online", "ramping", "operating plants", "fully operational"),
                    tuple(),
                    ("online", "ramping"),
                    "",
                    "plant_status",
                ),
                (
                    "Cost inputs",
                    "Corn / natural gas / input-cost commentary",
                    "Feedstock and energy commentary helps explain margins when quantified data is sparse.",
                    ("corn", "natural gas", "feedstock", "input cost"),
                    tuple(),
                    ("corn", "natural gas"),
                    "",
                    "input_cost_commentary",
                ),
                (
                    "Coproducts / mix",
                    "Distillers grains / Ultra-high protein commentary",
                    "Distillers grains and ultra-high protein trends inform mix quality and coproduct economics.",
                    ("distillers grains", "ultra-high protein", "uhp"),
                    tuple(),
                    ("distillers grains", "ultra-high protein"),
                    "",
                    "distillers_grains_uhp_commentary",
                ),
                (
                    "Risk management",
                    "Margin support / cash flow support commentary",
                    "Explicit commentary on margin or cash-flow support can explain near-term earnings resilience.",
                    ("margin support", "cash flow support", "supports margins", "supports cash flow"),
                    tuple(),
                    ("margin support", "cash flow support"),
                    "",
                    "margin_cashflow_support",
                ),
            )
        ),
        economics_overlay_coefficients=_overlay_coefficients(
            (
                ("Ethanol yield", "gal/bushel", ("ethanol yield", "gallons per bushel"), 2.9, "report aligned", "Recent GPRE filing / USDA cited average", "ethanol_yield"),
                ("Renewable corn oil yield", "lbs/bushel", ("corn oil yield", "renewable corn oil yield"), 1.0, "inferred", "Platform baseline coefficient", "renewable_corn_oil_yield"),
                ("Distillers yield", "lbs/bushel", ("distillers yield", "distillers grains per bushel"), 17.0, "inferred", "Platform baseline coefficient", "distillers_yield"),
                ("Ultra-high protein yield", "lbs/bushel", ("uhp yield", "ultra-high protein yield"), None, "user assumption", "", "uhp_yield"),
                ("Natural gas usage", "BTU/gal", ("natural gas usage", "gas usage", "btu per gallon"), 28000.0, "report aligned", "Recent GPRE filing process disclosure", "natural_gas_usage"),
                ("Electricity usage", "kWh/gal", ("electricity usage", "kwh per gallon"), 0.9, "user assumption", "Process assumption", "electricity_usage"),
            )
        ),
        economics_overlay_market_inputs=_DEFAULT_ECONOMICS_OVERLAY_MARKET_INPUTS,
        economics_overlay_hedge_templates=_DEFAULT_ECONOMICS_OVERLAY_HEDGES,
        economics_overlay_bridge_rows=_DEFAULT_ECONOMICS_OVERLAY_BRIDGE_ROWS,
        enabled_market_sources=("nwer", "ams_3617", "cme_ethanol_platts"),
        official_source_seeds=(
            SourceMaterialSeed(
                family="earnings_presentation",
                seed_url="https://investor.gpreinc.com/events-and-presentations/",
                follow_detail_pages=True,
                allowed_hosts=("investor.gpreinc.com",),
            ),
            SourceMaterialSeed(
                family="press_release",
                seed_url="https://investor.gpreinc.com/news/news-details/",
                follow_detail_pages=True,
                allowed_hosts=("investor.gpreinc.com",),
            ),
            SourceMaterialSeed(
                family="earnings_transcripts",
                seed_url="https://investor.gpreinc.com/events-and-presentations/",
                follow_detail_pages=True,
                allowed_hosts=("investor.gpreinc.com",),
            ),
        ),
        thesis_bridge_labels=(
            "Base Adj EBITDA FY",
            "45Z uplift / policy uplift",
            "Crush margin uplift",
            "Corn oil / coproduct uplift",
            "Protein / mix uplift",
            "Cost savings uplift",
            "Interest savings / debt-paydown uplift",
            "Other",
        ),
    ),
}


def get_company_profile(ticker: Optional[str]) -> CompanyProfile:
    key = str(ticker or "").strip().upper()
    if key and key in COMPANY_PROFILES:
        return COMPANY_PROFILES[key]
    # Conservative default: no hardcoded business model text.
    return CompanyProfile(
        ticker=key or "DEFAULT",
        has_bank=False,
        industry_keywords=(),
        segment_patterns=tuple(),
        segment_alias_patterns=tuple(),
        key_adv_require_keywords=(
            "advantage",
            "competitive",
            "differentiated",
            "scale",
            "network",
            "technology",
            "cost",
            "efficiency",
            "integrated",
            "recurring",
        ),
        key_adv_deny_keywords=_DENY_COMMON,
        commentary_prefer_terms=tuple(),
        commentary_deny_terms=tuple(),
        quarter_note_priority_terms=tuple(),
        promise_priority_terms=tuple(),
        operating_driver_templates=_drivers(
            (
                (
                    "Inputs / costs",
                    "Core input costs",
                    "Input costs often shape margin conversion and earnings volatility.",
                    ("cost", "input", "commodity", "labor"),
                    tuple(),
                ),
                (
                    "Outputs / realizations",
                    "Realizations / pricing",
                    "Pricing and mix help translate operating activity into margin.",
                    ("price", "pricing", "mix", "realization"),
                    ("revenue",),
                ),
                (
                    "Capital / financing / structural",
                    "Balance sheet / financing",
                    "Funding and capital structure affect flexibility and valuation.",
                    ("debt", "liquidity", "refinancing", "capital allocation"),
                    ("debt_core", "cash"),
                ),
            )
        ),
        operating_driver_history_templates=_drivers(
            (
                (
                    "Utilization / operating intensity",
                    "Utilization / operating intensity",
                    "Utilization can explain volume recovery, fixed-cost absorption, and margin capture.",
                    ("utilization", "operating rate", "capacity", "throughput"),
                    tuple(),
                    ("utilization", "operating rate"),
                    "%",
                    "utilization",
                ),
                (
                    "Production / volume",
                    "Volume / production",
                    "Volume helps anchor what actually moved the business in the quarter.",
                    ("volume", "production", "units", "shipments"),
                    tuple(),
                    ("volume", "shipments"),
                    "",
                    "volume_production",
                ),
                (
                    "Demand / pricing",
                    "Pricing / demand",
                    "Pricing, demand, and mix often explain revenue and margin swings.",
                    ("pricing", "price", "demand", "mix"),
                    ("revenue",),
                    ("pricing", "demand"),
                    "",
                    "pricing_demand",
                ),
                (
                    "Cost inputs",
                    "Cost inputs",
                    "Commodity, labor, and input-cost commentary often explains incremental margin.",
                    ("cost", "commodity", "input", "labor"),
                    tuple(),
                    ("cost", "input"),
                    "",
                    "cost_inputs",
                ),
                (
                    "Policy / credits",
                    "Regulation / credits",
                    "Regulation, subsidies, and credits can materially change economics.",
                    ("regulation", "credit", "incentive", "policy"),
                    tuple(),
                    ("credit", "policy"),
                    "",
                    "regulation_credits",
                ),
                (
                    "Capital / financing / structural",
                    "Capital / financing",
                    "Capital structure and financing developments can alter optionality and cash flow.",
                    ("debt", "financing", "capital return", "liquidity"),
                    ("debt_core", "cash"),
                    ("financing", "liquidity"),
                    "",
                    "capital_financing",
                ),
            )
        ),
        economics_overlay_coefficients=_DEFAULT_ECONOMICS_OVERLAY_COEFFICIENTS,
        economics_overlay_market_inputs=_DEFAULT_ECONOMICS_OVERLAY_MARKET_INPUTS,
        economics_overlay_hedge_templates=_DEFAULT_ECONOMICS_OVERLAY_HEDGES,
        economics_overlay_bridge_rows=_DEFAULT_ECONOMICS_OVERLAY_BRIDGE_ROWS,
        thesis_bridge_labels=(
            "Base Adj EBITDA FY",
            "Policy / regulatory uplift",
            "Price / mix uplift",
            "Coproduct / mix uplift",
            "Cost savings uplift",
            "Interest savings / debt-paydown uplift",
            "Other",
        ),
    )
