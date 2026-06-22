"""Sector Operating_Drivers intro table support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping


@dataclass(frozen=True)
class SectorOperatingDriverIntroSupportDeps:
    runtime: MutableMapping[str, Any]


class SectorOperatingDriverIntroSupport:
    def __init__(self, deps: SectorOperatingDriverIntroSupportDeps) -> None:
        self._runtime = deps.runtime

    def sector_operating_driver_intro_tables(self, ticker: Any) -> List[Dict[str, Any]]:
        """Return sector-specific Operating_Drivers intro tables in shared order."""
        guidance_source_contract_label = self._runtime["_guidance_source_contract_label"]

        ticker_txt = str(ticker or "").strip().upper()
        guidance_source_label = guidance_source_contract_label(ticker_txt)
        if ticker_txt == "ANF":
            return [
                {
                    "title": "Current watchlist",
                    "headers": ["Watch item", "Current read", "Why it matters"],
                    "rows": [
                        ("Sales guide", "2026 sales guide +3-5%", "Demand and comps must hold against tougher comparable-sales laps."),
                        ("Margin durability", "2026 operating margin guide 12.0-12.5%", "Tariff, ERP and marketing headwinds drive the EPS debate."),
                        ("Inventory quality", "Inventory cost/units +5%; tariffs/ERP explain part", "Watch markdown risk rather than treating all inventory growth as excess stock."),
                        ("Capital returns", "2025 buybacks ~$450m vs FCF ~$378m", "EPS support is meaningful, but cash returns exceeded FCF."),
                    ],
                },
                {
                    "title": "Current/latest outlook",
                    "headers": ["Topic", "Current read", "Source / use"],
                    "rows": [
                        ("Q4 actuals", "Sales +5%, comp +1%, operating margin 14.1%", "Q4 release and History_Q."),
                        ("2026 guide", "Sales +3-5%; OM 12.0-12.5%; EPS $10.20-$11.00", f"{guidance_source_label} and Promise_Progress_UI."),
                        ("Margin bridge", "Q1 tariffs ~290 bps/~$30m; freight ~160 bps; ERP >100 bps", "Q4 earnings release and transcript."),
                        ("Stores / buybacks", "55 openings / 25 closures / 70 remodels; buybacks ~$450m", "Q4 outlook table."),
                    ],
                },
            ]
        if ticker_txt == "PBI":
            return [
                {
                    "title": "Current watchlist",
                    "headers": ["Watch item", "Current read", "Why it matters"],
                    "rows": [
                        ("FCF conversion", "FCF guide and cash conversion", "Cash generation must fund debt reduction and the equity case."),
                        ("Cost savings", "Run-rate savings and EBIT flow-through", "Savings need to show up in adjusted EBIT, not just targets."),
                        ("Debt / refinancing", "Maturities, revolver and leverage", "Balance-sheet risk still drives the turnaround multiple."),
                        ("Presort", "Volumes, pricing and margin", "Presort stabilization is central to durable adjusted EBIT."),
                        ("SendTech", "Decline control and customer retention", "A slower decline lowers pressure on the turnaround bridge."),
                    ],
                },
                {
                    "title": "Current/latest outlook",
                    "headers": ["Topic", "Current read", "Source / use"],
                    "rows": [
                        ("Guidance", "Revenue, adjusted EBIT, EPS and FCF", f"{guidance_source_label} and Valuation side-panel."),
                        ("Cost actions", "Annualized savings / productivity", "Earnings releases and management updates."),
                        ("Capital structure", "Debt reduction and refinancing watch", "SEC filings, debt schedules and liquidity notes."),
                        ("Segments", "Presort and SendTech execution", "Segment tables and operating commentary."),
                    ],
                },
            ]
        if ticker_txt == "GPRE":
            return [
                {
                    "title": "Current watchlist",
                    "headers": ["Watch item", "Current read", "Why it matters"],
                    "rows": [
                        ("Crush margins", "Ethanol/corn/coproduct spread", "Margin per gallon drives EBITDA and cash generation."),
                        ("Demand / policy", "Exports, E15, RVO/SRE/RIN setup", "Policy and demand determine whether margins sustain."),
                        ("45Z / carbon", "Credit value and qualification", "45Z monetization can materially lift EBITDA."),
                        ("Production", "Gallons, utilization and downtime", "Volume turns margin into dollars and reveals operating reliability."),
                        ("Capex / balance sheet", "Capex, cash and liquidity", "Commodity cycles require disciplined liquidity management."),
                    ],
                },
                {
                    "title": "Current/latest outlook",
                    "headers": ["Topic", "Current read", "Source / use"],
                    "rows": [
                        ("45Z guidance", "2026 contribution and qualification path", f"{guidance_source_label}, releases and policy notes."),
                        ("Crush economics", "Margin, coproducts and utilization", "Economics_Overlay and Operating_Drivers."),
                        ("Policy watch", "RVO/E15/export/RIN developments", "Policy/regulatory source notes."),
                        ("Cash flow", "Capex and liquidity through cycle", "History_Q and balance-sheet data."),
                    ],
                },
            ]
        return []
