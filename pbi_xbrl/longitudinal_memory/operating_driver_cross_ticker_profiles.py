"""Declarative PBI and GPRE profiles for the shared Operating Drivers product.

Ticker names select data only.  All comparison, continuity, fail-closed, and
workbook behavior lives in shared modules.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


PROFILE_CONTRACT = "operating-drivers-declarative-ticker-profile@1"
HISTORY_QUARTERS = (
    "2023-Q3",
    "2023-Q4",
    "2024-Q1",
    "2024-Q2",
    "2024-Q3",
    "2024-Q4",
    "2025-Q1",
    "2025-Q2",
    "2025-Q3",
    "2025-Q4",
    "2026-Q1",
    "2026-Q2",
)

_DATA_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData\tickers")


def _source(
    source_id: str,
    source_type: str,
    period_label: str,
    source_location: str,
    *,
    local_path: Path | None = None,
    source_url: str | None = None,
    disposition: str = "REVIEWED_PRIMARY_SOURCE",
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "source_type": source_type,
        "period_label": period_label,
        "source_location": source_location,
        "source_url": source_url,
        "local_path": None if local_path is None else str(local_path),
        "official": True,
        "review_disposition": disposition,
    }


def _observation(
    ticker: str,
    driver_id: str,
    period_label: str,
    value: Any,
    unit: str,
    definition_id: str,
    source_id: str,
    *,
    display_value: str = "",
    precision: str = "EXACT",
    status: str = "AVAILABLE",
) -> dict[str, Any]:
    return {
        "observation_id": f"observation:{ticker.lower()}:{driver_id}:{period_label}",
        "driver_id": driver_id,
        "period_label": period_label,
        "period_basis": "QUARTER_ACTUAL",
        "value": value,
        "display_value": display_value,
        "precision": precision,
        "status": status,
        "unit": unit,
        "definition_id": definition_id,
        "source_ids": [source_id],
    }


def _pbi_sources() -> list[dict[str, Any]]:
    root = _DATA_ROOT / "PBI"
    releases = {
        "2023-Q3": root / "earnings_release" / "PBI_Q3_2023_earnings_release.htm",
        "2023-Q4": root / "earnings_release" / "PBI_Q4_2023_earnings_release.htm",
        "2024-Q1": root / "earnings_release" / "PBI_Q1_2024_earnings_release.htm",
        "2024-Q2": root / "earnings_release" / "PBI_Q2_2024_earnings_release.htm",
        "2024-Q3": root / "earnings_release" / "PBI_Q3_2024_earnings_release.htm",
        "2024-Q4": root / "earnings_release" / "PBI_Q4_2024_earnings_release.htm",
        "2025-Q1": root / "earnings_release" / "PBI_Q1_2025_earnings_release.htm",
        "2025-Q2": root / "earnings_release" / "PBI_Q2_2025_earnings_release.htm",
        "2025-Q3": root / "earnings_release" / "PBI_Q3_2025_earnings_release.htm",
        "2025-Q4": root / "earnings_release" / "PBI_Q4_2025_earnings_release.htm",
        "2026-Q1": root / "earnings_release" / "PBI_Q1_2026_earnings_release.htm",
        "2026-Q2": root / "sec_exhibits" / "PBI_8-K_2026-07-29_000162828026050614__ATTACHMENT__q22026earningspressrelea.htm",
    }
    presentations = {
        period: root / "earnings_presentation" / f"PBI_Q{period[-1]}_{period[:4]}_earnings_presentation.pdf"
        for period in HISTORY_QUARTERS
    }
    sources = [
        _source(
            f"pbi-release-{period.lower().replace('-', '')}",
            "OFFICIAL_EARNINGS_RELEASE",
            period,
            "Presort and SendTech operating discussion",
            local_path=path,
        )
        for period, path in releases.items()
    ]
    sources.extend(
        _source(
            f"pbi-presentation-{period.lower().replace('-', '')}",
            "OFFICIAL_INVESTOR_PRESENTATION",
            period,
            "Quarterly investor presentation",
            local_path=path,
            disposition="REVIEWED_NO_MORE_PRECISE_COMPATIBLE_FACT",
        )
        for period, path in presentations.items()
    )
    sources.extend(
        (
            _source(
                "pbi-form-10k-2025",
                "SEC_10_K",
                "2025-FY",
                "Business segments and KPI definitions",
                source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828026008386/pbi-20251231.htm",
            ),
            _source(
                "pbi-form-10q-2026q1",
                "SEC_10_Q",
                "2026-Q1",
                "Segment definitions and current operating context",
                local_path=root / "sec_primary" / "PBI_10-Q_2026-03-31_000162828026031003.htm",
                source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828026031003/pbi-20260331.htm",
            ),
            _source(
                "pbi-form-10q-2026q2",
                "SEC_10_Q",
                "2026-Q2",
                "Segment definitions and current operating context",
                local_path=root / "sec_primary" / "PBI_10-Q_2026-06-30_000162828026050908.htm",
                source_url="https://www.sec.gov/Archives/edgar/data/78814/000162828026050908/pbi-20260630.htm",
            ),
            _source(
                "pbi-ceo-letter-2026q1",
                "OFFICIAL_SHAREHOLDER_LETTER",
                "2026-Q1",
                "SendTech bookings, subscriber, churn, and Presort direction",
                local_path=root / "earnings_release" / "PBI_Q1_2026_earnings_release_pbi_q1_2026_earnings_rel.htm",
            ),
            _source(
                "pbi-ceo-letter-2026q2",
                "OFFICIAL_SHAREHOLDER_LETTER",
                "2026-Q2",
                "SendTech bookings, backlog, subscription, and Presort constraints",
                local_path=root / "sec_exhibits" / "PBI_8-K_2026-07-29_000162828026050614__ATTACHMENT__q22026earningsceoletter.htm",
            ),
        )
    )
    return sources


def _pbi_profile() -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    pieces = {
        "2024-Q2": 3.6,
        "2024-Q3": 3.7,
        "2025-Q4": 3.4,
        "2026-Q1": 3.6,
        "2026-Q2": 3.3,
    }
    volume_growth = {
        "2024-Q1": -2,
        "2024-Q2": -2,
        "2024-Q3": 3,
        "2025-Q4": -10,
        "2026-Q1": -6,
        "2026-Q2": -3,
    }
    revenue_per_piece = {
        "2023-Q3": "Higher YoY",
        "2023-Q4": "Higher YoY",
        "2024-Q1": "Higher YoY",
        "2024-Q2": "Higher YoY",
        "2024-Q3": "Higher YoY",
        "2024-Q4": "Higher YoY",
        "2025-Q1": "Higher YoY",
        "2025-Q2": "Higher YoY",
    }
    install_base = {
        "2024-Q3": "Declining",
        "2024-Q4": "Declining",
        "2025-Q2": "Declining",
        "2025-Q3": "Declining",
        "2025-Q4": "Declining",
        "2026-Q1": "Declining",
        "2026-Q2": "Declining",
    }
    for period, value in pieces.items():
        observations.append(
            _observation(
                "PBI", "pbi.presort.mail_pieces_bn", period, value, "billion_pieces",
                "pbi.presort.total_sorted_pieces.quarter@1", f"pbi-release-{period.lower().replace('-', '')}",
            )
        )
    for period, value in volume_growth.items():
        observations.append(
            _observation(
                "PBI", "pbi.presort.volume_growth_yoy", period, value, "percent",
                "pbi.presort.volume_growth_yoy.quarter@1", f"pbi-release-{period.lower().replace('-', '')}",
            )
        )
    for period, value in revenue_per_piece.items():
        observations.append(
            _observation(
                "PBI", "pbi.presort.revenue_per_piece_direction", period, None, "qualitative",
                "pbi.presort.revenue_per_piece_direction@1", f"pbi-release-{period.lower().replace('-', '')}",
                display_value=value, precision="QUALITATIVE",
            )
        )
    for period, value in install_base.items():
        observations.append(
            _observation(
                "PBI", "pbi.sendtech.mailing_install_base_direction", period, None, "qualitative",
                "pbi.sendtech.mailing_install_base_direction@1", f"pbi-release-{period.lower().replace('-', '')}",
                display_value=value, precision="QUALITATIVE",
            )
        )
    observations.extend(
        (
            _observation(
                "PBI", "pbi.sendtech.sales_bookings_direction", "2026-Q1", None, "qualitative",
                "pbi.sendtech.sales_bookings_yoy_direction@1", "pbi-ceo-letter-2026q1",
                display_value="Up YoY", precision="QUALITATIVE",
            ),
            _observation(
                "PBI", "pbi.sendtech.sales_bookings_direction", "2026-Q2", None, "qualitative",
                "pbi.sendtech.sales_bookings_yoy_direction@1", "pbi-ceo-letter-2026q2",
                display_value="Up YoY", precision="QUALITATIVE",
            ),
            _observation(
                "PBI", "pbi.sendtech.backlog_state", "2026-Q2", None, "qualitative",
                "pbi.sendtech.quarter_end_backlog_state@1", "pbi-ceo-letter-2026q2",
                display_value="Highest since 2024 migration", precision="QUALITATIVE",
            ),
            _observation(
                "PBI", "pbi.sendtech.subscription_revenue_direction", "2026-Q2", None, "qualitative",
                "pbi.sendtech.subscription_revenue_direction@1", "pbi-ceo-letter-2026q2",
                display_value="Growing", precision="QUALITATIVE",
            ),
        )
    )
    drivers = [
        ("pbi.presort.mail_pieces_bn", "Presort mail pieces", "VOLUME"),
        ("pbi.presort.volume_growth_yoy", "Presort volume growth", "VOLUME"),
        ("pbi.presort.revenue_per_piece_direction", "Revenue per piece", "PRICE_MIX"),
        ("pbi.sendtech.mailing_install_base_direction", "Mailing install base", "INSTALLED_BASE"),
        ("pbi.sendtech.sales_bookings_direction", "Sales bookings", "LEADING_INDICATOR"),
        ("pbi.sendtech.backlog_state", "Backlog", "LEADING_INDICATOR"),
        ("pbi.sendtech.subscription_revenue_direction", "Subscription revenue", "RECURRING_REVENUE"),
    ]
    return {
        "profile_contract": PROFILE_CONTRACT,
        "ticker": "PBI",
        "company_name": "Pitney Bowes",
        "latest_period_label": "2026-Q2",
        "quarter_labels": list(HISTORY_QUARTERS),
        "source_documents": _pbi_sources(),
        "driver_registry": [
            {
                "driver_id": driver_id,
                "display_name": display_name,
                "driver_family": family,
                "economic_owner": "OPERATING_DRIVERS",
                "architecture_layer": "BUSINESS_SERVICES_SECTOR_PACK",
            }
            for driver_id, display_name, family in drivers
        ],
        "observations": observations,
        "overview": [
            {
                "statement_id": "pbi-interpretation-1",
                "subsection": "OPERATING INTERPRETATION",
                "text": "Presort economics depend on network activity and revenue per piece. Lower volume can pressure operating leverage even when pricing and mix provide support.",
                "source_references": ["pbi-form-10k-2025", "pbi-release-2026q2"],
            },
            {
                "statement_id": "pbi-interpretation-2",
                "subsection": "OPERATING INTERPRETATION",
                "text": "SendTech's bookings and backlog improved, but the shrinking mailing install base remains the main constraint on recurring activity.",
                "source_references": ["pbi-release-2026q2", "pbi-ceo-letter-2026q2"],
            },
            {
                "statement_id": "pbi-latest-1",
                "subsection": "LATEST QUARTER",
                "text": "Presort processed 3.3 billion mail pieces. Volume fell 3% year over year, an improvement from the 6% decline in Q1.",
                "source_references": ["pbi-release-2026q2", "pbi-release-2026q1"],
            },
            {
                "statement_id": "pbi-latest-2",
                "subsection": "LATEST QUARTER",
                "text": "Lower volume still reduced operating leverage, while higher fuel and transportation costs added pressure.",
                "source_references": ["pbi-release-2026q2"],
            },
            {
                "statement_id": "pbi-latest-3",
                "subsection": "LATEST QUARTER",
                "text": "SendTech's mailing install base continued to shrink, partly offset by stronger sales execution and services revenue.",
                "source_references": ["pbi-release-2026q2"],
            },
            {
                "statement_id": "pbi-latest-4",
                "subsection": "LATEST QUARTER",
                "text": "Sales bookings increased again and quarter-end backlog reached its highest level since the 2024 product migration.",
                "source_references": ["pbi-ceo-letter-2026q2"],
            },
            {
                "statement_id": "pbi-broader-1",
                "subsection": "BROADER TREND",
                "text": "Presort volume remains below last year, but the pace of decline moderated in the first half of 2026.",
                "source_references": ["pbi-release-2025q4", "pbi-release-2026q1", "pbi-release-2026q2"],
            },
            {
                "statement_id": "pbi-broader-2",
                "subsection": "BROADER TREND",
                "text": "Pricing and mix supported Presort for much of 2023-2025, but current disclosures do not provide a complete numeric series.",
                "source_references": ["pbi-release-2023q3", "pbi-release-2025q2"],
            },
            {
                "statement_id": "pbi-broader-3",
                "subsection": "BROADER TREND",
                "text": "SendTech's leading indicators improved, but the declining mailing base remains the main structural constraint.",
                "source_references": ["pbi-ceo-letter-2026q1", "pbi-ceo-letter-2026q2", "pbi-release-2026q2"],
            },
        ],
        "core_drivers": [
            {
                "core_id": "pbi-core-presort-pieces",
                "group_label": "Presort Economics",
                "label": "Mail pieces processed",
                "driver_id": "pbi.presort.mail_pieces_bn",
                "comparison_kind": "AMOUNT",
                "broader_trend": "Sparse history",
                "why_it_matters": "Measures Presort network activity and scale.",
            },
            {
                "core_id": "pbi-core-presort-growth",
                "group_label": "Presort Economics",
                "label": "Volume growth (YoY)",
                "driver_id": "pbi.presort.volume_growth_yoy",
                "comparison_kind": "PERCENTAGE_POINT",
                "broader_trend": "Decline moderating",
                "why_it_matters": "Shows whether Presort activity is expanding or contracting.",
            },
            {
                "core_id": "pbi-core-bookings",
                "group_label": "SendTech Leading Indicators",
                "label": "Sales bookings",
                "driver_id": "pbi.sendtech.sales_bookings_direction",
                "comparison_kind": "NONE",
                "broader_trend": "Improving",
                "why_it_matters": "Signals future SendTech equipment and subscription activity.",
            },
            {
                "core_id": "pbi-core-backlog",
                "group_label": "SendTech Leading Indicators",
                "label": "Quarter-end backlog",
                "driver_id": "pbi.sendtech.backlog_state",
                "comparison_kind": "NONE",
                "broader_trend": "Expanding",
                "why_it_matters": "Shows committed demand not yet recognized as revenue.",
            },
            {
                "core_id": "pbi-core-install-base",
                "group_label": "SendTech Leading Indicators",
                "label": "Mailing install base",
                "driver_id": "pbi.sendtech.mailing_install_base_direction",
                "comparison_kind": "NONE",
                "broader_trend": "Contracting",
                "why_it_matters": "Indicates the recurring supplies, service and financing base.",
            },
        ],
        "history_rows": [
            {
                "group_label": "Presort",
                "driver_id": "pbi.presort.mail_pieces_bn",
                "label": "Mail pieces processed (bn)",
                "unit": "billion_pieces",
                "definition_id": "pbi.presort.total_sorted_pieces.quarter@1",
            },
            {
                "group_label": "Presort",
                "driver_id": "pbi.presort.volume_growth_yoy",
                "label": "Volume growth (YoY)",
                "unit": "percent",
                "definition_id": "pbi.presort.volume_growth_yoy.quarter@1",
            },
            {
                "group_label": "Presort",
                "driver_id": "pbi.presort.revenue_per_piece_direction",
                "label": "Revenue per piece",
                "unit": "qualitative",
                "definition_id": "pbi.presort.revenue_per_piece_direction@1",
            },
            {
                "group_label": "SendTech",
                "driver_id": "pbi.sendtech.mailing_install_base_direction",
                "label": "Mailing install base",
                "unit": "qualitative",
                "definition_id": "pbi.sendtech.mailing_install_base_direction@1",
            },
            {
                "group_label": "SendTech",
                "driver_id": "pbi.sendtech.sales_bookings_direction",
                "label": "Sales bookings (YoY)",
                "unit": "qualitative",
                "definition_id": "pbi.sendtech.sales_bookings_yoy_direction@1",
            },
        ],
        "guide_terms": [
            {
                "term": "Presort mail pieces",
                "meaning": "Mail pieces processed through the Presort network during the quarter.",
                "economic_role": "Helps assess network utilization and operating leverage.",
                "definition_authority": "SOURCE_DEFINED",
                "source_references": ["pbi-release-2026q2"],
            },
            {
                "term": "Revenue per piece",
                "meaning": "Revenue earned per mail piece, reflecting pricing and mail mix.",
                "economic_role": "Helps assess whether pricing and mix offset or amplify changing volume.",
                "definition_authority": "PROFILE_DERIVED",
                "source_references": ["pbi-release-2024q3", "pbi-release-2025q2"],
            },
            {
                "term": "Mailing install base",
                "meaning": "Active customer mailing equipment supporting recurring activity.",
                "economic_role": "Helps assess the recurring supplies, service and financing base.",
                "definition_authority": "PROFILE_DERIVED",
                "source_references": ["pbi-form-10k-2025", "pbi-release-2026q2"],
            },
            {
                "term": "Sales bookings",
                "meaning": "Customer orders secured during the period before full revenue recognition.",
                "economic_role": "Provides an early indication of future SendTech activity.",
                "definition_authority": "PROFILE_DERIVED",
                "source_references": ["pbi-ceo-letter-2026q2"],
            },
            {
                "term": "Backlog",
                "meaning": "Booked business not yet fully delivered or recognized as revenue.",
                "economic_role": "Provides context on committed demand carried into future periods.",
                "definition_authority": "PROFILE_DERIVED",
                "source_references": ["pbi-ceo-letter-2026q2"],
            },
        ],
    }


def _gpre_sources() -> list[dict[str, Any]]:
    root = _DATA_ROOT / "GPRE"
    release_names = {
        "2023-Q3": "8-K_2023-10-31_earnings_release_q3_2023.htm",
        "2023-Q4": "8-K_2024-02-07_earnings_release_q4_2023.htm",
        "2024-Q1": "8-K_2024-05-03_earnings_release_q1_2024.htm",
        "2024-Q2": "8-K_2024-08-06_earnings_release_q2_2024.htm",
        "2024-Q3": "8-K_2024-10-31_earnings_release_q3_2024.htm",
        "2024-Q4": "8-K_2025-02-07_earnings_release_q4_2024.htm",
        "2025-Q1": "8-K_2025-05-08_earnings_release_q1_2025.htm",
        "2025-Q2": "8-K_2025-08-11_earnings_release_q2_2025.htm",
        "2025-Q3": "8-K_2025-11-05_earnings_release_q3_2025.htm",
        "2025-Q4": "8-K_2026-02-05_earnings_release_q4_2025.htm",
        "2026-Q1": "GPRE_Q1_2026_earnings_release.pdf",
    }
    sources = [
        _source(
            f"gpre-release-{period.lower().replace('-', '')}",
            "OFFICIAL_EARNINGS_RELEASE",
            period,
            "Selected operating data and crush-margin discussion",
            local_path=root / "earnings_release" / name,
        )
        for period, name in release_names.items()
    ]
    sources.append(
        _source(
            "gpre-release-2026q2",
            "OFFICIAL_EARNINGS_RELEASE",
            "2026-Q2",
            "Selected operating data and second-quarter results",
            source_url="https://investor.gpreinc.com/news/news-details/2026/Green-Plains-Reports-Second-Quarter-2026-Financial-Results/default.aspx",
        )
    )
    for period in HISTORY_QUARTERS[:-1]:
        path = root / "earnings_presentation" / f"GPRE_Q{period[-1]}_{period[:4]}_earnings_presentation.pdf"
        sources.append(
            _source(
                f"gpre-presentation-{period.lower().replace('-', '')}",
                "OFFICIAL_INVESTOR_PRESENTATION",
                period,
                "Quarterly operating and strategic presentation",
                local_path=path,
                disposition="REVIEWED_FOR_DEFINITION_AND_INCREMENTAL_EVIDENCE",
            )
        )
    sources.extend(
        (
            _source(
                "gpre-form-10k-2025",
                "SEC_10_K",
                "2025-FY",
                "Plant population, production capacity, policy and segment definitions",
                source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940226000015/gpre-20251231.htm",
            ),
            _source(
                "gpre-form-10q-2026q1",
                "SEC_10_Q",
                "2026-Q1",
                "Current plant population and operating definitions",
                source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940226000060/gpre-20260331.htm",
            ),
            _source(
                "gpre-form-10q-2026q2",
                "SEC_10_Q",
                "2026-Q2",
                "Current plant population and operating definitions",
                source_url="https://www.sec.gov/Archives/edgar/data/1309402/000130940226000095/gpre-20260630.htm",
            ),
        )
    )
    return sources


def _gpre_profile() -> dict[str, Any]:
    series = {
        "gpre.ethanol.produced_mgal": ([223.5, 215.7, 207.9, 208.5, 220.3, 209.5, 195.328, 193.571, 197.264, 178.777, 174.196, 160.700], "million_gallons", "gpre.ethanol.produced.quarter@1"),
        "gpre.ethanol.sold_mgal": ([None, None, None, 261.461, 262.111, 269.758, 255.721, 225.703, 210.473, 183.065, 176.145, 180.760], "million_gallons", "gpre.ethanol.sold.quarter@1"),
        "gpre.utilization.percent": ([93.9, 95, 92.4, 92.6, 96.8, 92, 100, 99, 101, 97, 97, 88], "percent", "MIXED"),
        "gpre.crush.consolidated_usd_m": ([48.5, 49.7, -9.3, 22.7, 58.3, -15.5, -14.7, 26.286, 59.6, 44.4, 64.616, 95.071], "usd_million", "gpre.consolidated_crush_margin.quarter@1"),
        "gpre.corn.consumed_mbu": ([76.5, 74.2, 71.3, 71.8, 75.1, 71.2, 66.3, 65.3, 66.6, 60.4, 58.8, 54.558], "million_bushels", "gpre.corn_consumed.quarter@1"),
        "gpre.coproduct.ddg_ktons": ([514, 479, 469, 463, 489, 469, 417, 413, 417, 378, 362, 323], "thousand_tons", "gpre.distillers_grains.quarter@1"),
        "gpre.coproduct.uhp_ktons": ([61, 66, 60, 65, 69, 54, 68, 66, 71, 60, 54, 49], "thousand_tons", "gpre.ultra_high_protein.quarter@1"),
        "gpre.coproduct.rco_mlbs": ([74.2, 72.9, 66.7, 73.6, 77.1, 73.4, 64.263, 65.231, 72.345, 64.572, 58.476, 58.332], "million_pounds", "gpre.renewable_corn_oil.quarter@1"),
    }
    observations: list[dict[str, Any]] = []
    for driver_id, (values, unit, definition) in series.items():
        for period, value in zip(HISTORY_QUARTERS, values, strict=True):
            if value is None:
                continue
            actual_definition = definition
            if driver_id == "gpre.utilization.percent":
                actual_definition = (
                    "gpre.utilization.nine_operating_plants@1"
                    if period <= "2025-Q3"
                    else "gpre.utilization.eight_operating_plants@2"
                )
            observations.append(
                _observation(
                    "GPRE", driver_id, period, value, unit, actual_definition,
                    f"gpre-release-{period.lower().replace('-', '')}",
                )
            )
    for period in HISTORY_QUARTERS:
        observations.append(
            _observation(
                "GPRE", "gpre.utilization.basis", period, None, "qualitative",
                "gpre.utilization.population_basis@1", f"gpre-release-{period.lower().replace('-', '')}",
                display_value=("Nine plants" if period <= "2025-Q3" else "Eight plants"),
                precision="QUALITATIVE",
            )
        )
    for period, value in {"2025-Q3": 26.0, "2025-Q4": 27.0, "2026-Q1": 55.2, "2026-Q2": 58.7}.items():
        observations.append(
            _observation(
                "GPRE", "gpre.45z.realized_benefit_usd_m", period, value, "usd_million",
                "gpre.45z.realized_benefit.quarter@1", f"gpre-release-{period.lower().replace('-', '')}",
            )
        )
    for period, value in {"2025-Q2": 3.7, "2025-Q3": 33.1, "2025-Q4": 16.1, "2026-Q1": 8.516}.items():
        observations.append(
            _observation(
                "GPRE", "gpre.crush.underlying_ex45z_usd_m", period, value, "usd_million",
                "gpre.underlying_crush_ex45z.quarter@1", f"gpre-release-{period.lower().replace('-', '')}",
            )
        )
    drivers = [
        ("gpre.ethanol.produced_mgal", "Ethanol produced", "PRODUCTION"),
        ("gpre.ethanol.sold_mgal", "Ethanol sold", "VOLUME"),
        ("gpre.utilization.percent", "Capacity utilization", "UTILIZATION"),
        ("gpre.utilization.basis", "Utilization basis", "DEFINITION_SUPPORT"),
        ("gpre.crush.consolidated_usd_m", "Consolidated crush margin", "UNIT_ECONOMICS"),
        ("gpre.crush.underlying_ex45z_usd_m", "Underlying crush excluding 45Z", "UNIT_ECONOMICS"),
        ("gpre.45z.realized_benefit_usd_m", "45Z realized benefit", "POLICY"),
        ("gpre.corn.consumed_mbu", "Corn consumed", "THROUGHPUT"),
        ("gpre.coproduct.ddg_ktons", "Distillers grains produced", "COPRODUCT"),
        ("gpre.coproduct.uhp_ktons", "Ultra-high protein produced", "COPRODUCT"),
        ("gpre.coproduct.rco_mlbs", "Renewable corn oil produced", "COPRODUCT"),
    ]
    return {
        "profile_contract": PROFILE_CONTRACT,
        "ticker": "GPRE",
        "company_name": "Green Plains",
        "latest_period_label": "2026-Q2",
        "quarter_labels": list(HISTORY_QUARTERS),
        "source_documents": _gpre_sources(),
        "driver_registry": [
            {
                "driver_id": driver_id,
                "display_name": display_name,
                "driver_family": family,
                "economic_owner": "OPERATING_DRIVERS",
                "architecture_layer": "COMMODITY_PROCESSING_SECTOR_PACK",
            }
            for driver_id, display_name, family in drivers
        ],
        "observations": observations,
        "overview": [
            {
                "statement_id": "gpre-interpretation-1",
                "subsection": "OPERATING INTERPRETATION",
                "text": "Production volume and capacity utilization show physical activity, while crush margins show realized unit economics.",
                "source_references": ["gpre-form-10k-2025", "gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-interpretation-2",
                "subsection": "OPERATING INTERPRETATION",
                "text": "45Z is a separate policy-linked layer. Carbon intensity and capture status matter because they influence qualification and credit value.",
                "source_references": ["gpre-release-2025q4", "gpre-presentation-2026q1"],
            },
            {
                "statement_id": "gpre-latest-1",
                "subsection": "LATEST QUARTER",
                "text": "Ethanol production declined to 160.7 million gallons, while sales volume increased modestly from Q1 to 180.8 million gallons.",
                "source_references": ["gpre-release-2026q1", "gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-latest-2",
                "subsection": "LATEST QUARTER",
                "text": "Utilization fell to 88% across the current eight-plant population, down 9 percentage points from Q1.",
                "source_references": ["gpre-release-2026q1", "gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-latest-3",
                "subsection": "LATEST QUARTER",
                "text": "Consolidated crush margin rose to $95.1 million despite lower production. This reported measure includes 45Z when recognized.",
                "source_references": ["gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-latest-4",
                "subsection": "LATEST QUARTER",
                "text": "The quarter included $58.7 million of realized 45Z benefit; that policy value is shown separately from underlying crush economics.",
                "source_references": ["gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-broader-1",
                "subsection": "BROADER TREND",
                "text": "Production and feedstock throughput have trended lower as the operating asset base changed.",
                "source_references": ["gpre-release-2023q3", "gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-broader-2",
                "subsection": "BROADER TREND",
                "text": "Consolidated crush margin improved sharply in 2026, but the reported series includes a material 45Z contribution. Underlying crush was not separately disclosed for Q2.",
                "source_references": ["gpre-release-2025q4", "gpre-release-2026q1", "gpre-release-2026q2"],
            },
            {
                "statement_id": "gpre-broader-3",
                "subsection": "BROADER TREND",
                "text": "Utilization before Q4 2025 is not directly comparable because the disclosed population changed from nine plants to eight.",
                "source_references": ["gpre-form-10k-2025", "gpre-release-2025q4"],
            },
        ],
        "core_drivers": [
            {
                "core_id": "gpre-core-sold",
                "group_label": "Production & Asset Utilization",
                "label": "Ethanol sold",
                "driver_id": "gpre.ethanol.sold_mgal",
                "comparison_kind": "AMOUNT",
                "broader_trend": "Lower YoY",
                "why_it_matters": "Measures commercial output available to customers.",
            },
            {
                "core_id": "gpre-core-utilization",
                "group_label": "Production & Asset Utilization",
                "label": "Capacity utilization",
                "driver_id": "gpre.utilization.percent",
                "comparison_kind": "PERCENTAGE_POINT",
                "broader_trend": "Definition break",
                "why_it_matters": "Shows how fully the operating asset base is being used.",
            },
            {
                "core_id": "gpre-core-underlying-crush",
                "group_label": "Commodity Unit Economics",
                "label": "Underlying crush margin",
                "driver_id": "gpre.crush.underlying_ex45z_usd_m",
                "comparison_kind": "AMOUNT",
                "allow_missing_latest": True,
                "latest_display": "Not disclosed",
                "unit": "usd_million",
                "broader_trend": "Needs current disclosure",
                "why_it_matters": "Isolates commodity economics from policy support.",
            },
            {
                "core_id": "gpre-core-crush",
                "group_label": "Commodity Unit Economics",
                "label": "Consolidated crush margin",
                "driver_id": "gpre.crush.consolidated_usd_m",
                "comparison_kind": "AMOUNT",
                "broader_trend": "Improving; policy-supported",
                "why_it_matters": "Shows realized reported crush economics.",
            },
            {
                "core_id": "gpre-core-45z",
                "group_label": "Policy & Low-Carbon Economics",
                "label": "45Z realized benefit",
                "driver_id": "gpre.45z.realized_benefit_usd_m",
                "comparison_kind": "AMOUNT",
                "broader_trend": "Material contribution",
                "why_it_matters": "Shows policy-linked value alongside operating economics.",
            },
        ],
        "history_rows": [
            {
                "group_label": "Production / Throughput", "driver_id": "gpre.ethanol.produced_mgal",
                "label": "Ethanol produced (m gal)", "unit": "million_gallons", "definition_id": "gpre.ethanol.produced.quarter@1",
            },
            {
                "group_label": "Production / Throughput", "driver_id": "gpre.ethanol.sold_mgal",
                "label": "Ethanol sold (m gal)", "unit": "million_gallons", "definition_id": "gpre.ethanol.sold.quarter@1",
            },
            {
                "group_label": "Production / Throughput", "driver_id": "gpre.utilization.percent",
                "label": "Capacity utilization", "unit": "percent", "definition_id": "gpre.utilization.eight_operating_plants@2",
            },
            {
                "group_label": "Production / Throughput", "driver_id": "gpre.utilization.basis",
                "label": "Utilization basis", "unit": "qualitative", "definition_id": "gpre.utilization.population_basis@1",
            },
            {
                "group_label": "Production / Throughput", "driver_id": "gpre.corn.consumed_mbu",
                "label": "Corn consumed (m bu)", "unit": "million_bushels", "definition_id": "gpre.corn_consumed.quarter@1",
            },
            {
                "group_label": "Commodity Unit Economics", "driver_id": "gpre.crush.underlying_ex45z_usd_m",
                "label": "Underlying crush margin ($m)", "unit": "usd_million", "definition_id": "gpre.underlying_crush_ex45z.quarter@1",
            },
            {
                "group_label": "Commodity Unit Economics", "driver_id": "gpre.crush.consolidated_usd_m",
                "label": "Consolidated crush margin ($m)", "unit": "usd_million", "definition_id": "gpre.consolidated_crush_margin.quarter@1",
            },
            {
                "group_label": "Policy / Carbon", "driver_id": "gpre.45z.realized_benefit_usd_m",
                "label": "45Z realized benefit ($m)", "unit": "usd_million", "definition_id": "gpre.45z.realized_benefit.quarter@1",
            },
            {
                "group_label": "Coproducts", "driver_id": "gpre.coproduct.ddg_ktons",
                "label": "Distillers grains (000 tons)", "unit": "thousand_tons", "definition_id": "gpre.distillers_grains.quarter@1",
            },
            {
                "group_label": "Coproducts", "driver_id": "gpre.coproduct.uhp_ktons",
                "label": "Ultra-high protein (000 tons)", "unit": "thousand_tons", "definition_id": "gpre.ultra_high_protein.quarter@1",
            },
            {
                "group_label": "Coproducts", "driver_id": "gpre.coproduct.rco_mlbs",
                "label": "Renewable corn oil (m lbs)", "unit": "million_pounds", "definition_id": "gpre.renewable_corn_oil.quarter@1",
            },
        ],
        "safe_sum_derivations": [
            {
                "driver_id": "gpre.45z.realized_benefit_usd_m",
                "result_period_label": "TTM through 2026-Q2",
                "input_periods": ["2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2"],
            },
            {
                "driver_id": "gpre.45z.realized_benefit_usd_m",
                "result_period_label": "2025-FY",
                "input_periods": ["2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"],
            },
        ],
        "guide_terms": [
            {
                "term": "Capacity utilization",
                "meaning": "Production relative to the disclosed operating plant capacity.",
                "economic_role": "Helps assess fixed-asset use; the disclosed plant population must match.",
                "definition_authority": "SOURCE_DEFINED",
                "source_references": ["gpre-form-10k-2025", "gpre-release-2026q2"],
            },
            {
                "term": "Underlying crush margin",
                "meaning": "Crush economics disclosed separately from recognized 45Z benefit.",
                "economic_role": "Helps isolate operating economics from policy-linked support.",
                "definition_authority": "SOURCE_DEFINED",
                "source_references": ["gpre-release-2025q2", "gpre-release-2025q3", "gpre-release-2025q4", "gpre-release-2026q1"],
            },
            {
                "term": "Consolidated crush margin",
                "meaning": "Reported aggregate crush economics, including 45Z when recognized.",
                "economic_role": "Shows realized reported crush performance across the operating platform.",
                "definition_authority": "SOURCE_DEFINED",
                "source_references": ["gpre-release-2026q2"],
            },
            {
                "term": "45Z realized benefit",
                "meaning": "Quarterly policy benefit recognized from the clean-fuel production credit.",
                "economic_role": "Shows policy-linked value separately from operating crush economics.",
                "definition_authority": "SOURCE_DEFINED",
                "source_references": ["gpre-release-2026q2"],
            },
            {
                "term": "Carbon intensity (CI)",
                "meaning": "Relative emissions profile used in low-carbon fuel and 45Z qualification.",
                "economic_role": "Helps assess credit value and access to low-carbon fuel markets.",
                "definition_authority": "PROFILE_DERIVED",
                "source_references": ["gpre-release-2025q4", "gpre-presentation-2026q1"],
            },
            {
                "term": "Carbon capture and storage (CCS)",
                "meaning": "Capture and underground storage of biogenic carbon dioxide at selected plants.",
                "economic_role": "Reduces product carbon intensity and supports low-carbon qualification.",
                "definition_authority": "SOURCE_DEFINED",
                "source_references": ["gpre-release-2025q3", "gpre-release-2025q4"],
            },
        ],
    }


PROFILES = {
    "PBI": _pbi_profile(),
    "GPRE": _gpre_profile(),
}


__all__ = ["HISTORY_QUARTERS", "PROFILE_CONTRACT", "PROFILES"]
