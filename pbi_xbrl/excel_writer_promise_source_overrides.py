"""Promise source-backed override support data.

This module owns pure source/spec helpers for Promise_Progress_UI overrides.
Worksheet mutation stays in the writer context; callers inject runtime state
only to preserve the same closure-style lookup pattern used by nearby adapters.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional

import pandas as pd


@dataclass(frozen=True)
class PromiseSourceOverrideSupportDeps:
    runtime: MutableMapping[str, Any]


class PromiseSourceOverrideSupport:
    def __init__(self, deps: PromiseSourceOverrideSupportDeps) -> None:
        self.runtime = deps.runtime

    def build_specs(self, *, ticker: str, wb: Any | None = None) -> dict[str, Any]:
        ticker_txt = str(ticker or "").strip().upper()
        if ticker_txt == "PBI":
            return {
                "guidance_progression_rows": self.pbi_guidance_progression_rows(),
                "source_record_maps": self.pbi_source_record_maps(),
                "q1_2026_rows": self.pbi_2026_q1_rows(),
                "cost_rows": self.pbi_cost_rows(),
                "q4_promise_semantics": self.pbi_q4_promise_semantics(),
                "quarter_notes": self.pbi_quarter_notes(),
            }
        if ticker_txt == "ANF":
            sales_growth = self.runtime.get("anf_annual_sales_growth") or (lambda _year: "")
            capex_actual = self.runtime.get("anf_annual_capex_actual") or (lambda _year: "")
            return {
                "guidance_progression_rows": self.anf_guidance_progression_rows(
                    annual_sales_growth=sales_growth,
                    annual_capex_actual=capex_actual,
                ),
                "quarter_eps_actuals": self.anf_quarter_eps_actuals(),
                "diluted_share_progress": self.anf_diluted_share_progress(),
                "final_q4_rows": self.anf_final_q4_rows(),
                "q4_note": self.anf_q4_note(),
                "q4_adjusted_eps_note": self.anf_q4_adjusted_eps_note(),
                "q4_share_note": self.anf_q4_share_note(),
                "final_q4_timeline_rows": self.anf_final_q4_timeline_rows(),
                "q2_diluted_share_row": self.anf_q2_diluted_share_row(),
            }
        if ticker_txt == "GPRE":
            return {"source_rows": self.gpre_source_rows()}
        return {}

    def lifecycle_id(self, metric: Any, horizon: Any) -> str:
        metric_slug = re.sub(r"[^a-z0-9]+", "_", str(metric or "").strip().lower()).strip("_")
        horizon_slug = re.sub(r"[^a-z0-9]+", "_", str(horizon or "").strip().lower()).strip("_")
        if not metric_slug:
            return ""
        if metric_slug in {"cost_savings", "cost_savings_target", "cost_reduction", "cost_reduction_target"}:
            return "guidance:cost_savings:ANNUALIZED_PROGRAM"
        return f"guidance:{metric_slug}:{horizon_slug}" if horizon_slug else f"guidance:{metric_slug}"

    def display_section_from_horizon(self, stated: Any, horizon: Any, fallback_section: str) -> str:
        stated_txt = str(stated or "").strip()
        horizon_txt = str(horizon or "").strip()
        fallback_txt = str(fallback_section or "").strip()
        annual_match = re.fullmatch(r"(20\d{2})\s+year", horizon_txt, flags=re.I)
        stated_match = re.search(r"\b(20\d{2})-Q([1-4])\b", stated_txt, flags=re.I)
        horizon_q_match = re.search(r"\b(20\d{2})-Q([1-4])\b", horizon_txt, flags=re.I)
        if annual_match:
            annual_year = int(annual_match.group(1))
            if stated_match:
                stated_year = int(stated_match.group(1))
                stated_q = int(stated_match.group(2))
                if stated_year < annual_year:
                    return f"{annual_year}-Q1 revisions"
                if stated_year == annual_year:
                    return f"{annual_year}-Q{stated_q} revisions"
                return f"{annual_year}-Q4 revisions"
            return f"{annual_year}-Q1 revisions"
        if horizon_q_match:
            horizon_year = int(horizon_q_match.group(1))
            horizon_q = int(horizon_q_match.group(2))
            if stated_match:
                stated_year = int(stated_match.group(1))
                stated_q = int(stated_match.group(2))
                if stated_year == horizon_year and stated_q > horizon_q:
                    return fallback_txt or f"{stated_year}-Q{stated_q} revisions"
            return f"{horizon_year}-Q{horizon_q} revisions"
        return fallback_txt

    def source_date_ordinal(self, value: Any) -> int:
        try:
            return pd.Timestamp(str(value or "")).date().toordinal()
        except Exception:
            return 0

    def append_prior_source_note(self, values: list[Any], existing: Mapping[str, Any]) -> None:
        if len(values) < 11:
            return
        prior_guide = str(existing.get("new/current guide") or "").strip()
        prior_stated = str(existing.get("stated in") or "").strip()
        prior_date = str(existing.get("source date") or "").strip()
        if not prior_guide and not prior_stated and not prior_date:
            return
        pieces = []
        if prior_guide:
            pieces.append(f"Initial guide {prior_guide}")
        if prior_stated:
            pieces.append(f"stated in {prior_stated}")
        if prior_date:
            pieces.append(f"source date {prior_date}")
        prior_note = " ".join(pieces).strip()
        if prior_note and prior_note.lower() not in str(values[10] or "").lower():
            values[10] = f"{prior_note}. {str(values[10] or '').strip()}".strip()

    def pbi_guidance_progression_rows(self) -> list[tuple[int, list[dict[str, Any]]]]:
        return [
            (
                2025,
                [
                    {
                        "metric": "Revenue guidance",
                        "initial": "$1,950m-$2,000m",
                        "q1": "$1.95bn-$2.0bn",
                        "q2": "$1.9bn-$1.95bn",
                        "q3": "$1.9bn-$1.95bn",
                        "q4": "",
                        "actual": "$1.89bn",
                        "status": "Missed",
                        "note": "2025 year revenue landed below the latest annual range.",
                    },
                    {
                        "metric": "Adjusted EBIT guidance",
                        "initial": "$450m-$480m",
                        "q1": "$450m-$480m",
                        "q2": "$450m-$465m",
                        "q3": "$450m-$465m",
                        "q4": "",
                        "actual": "$461.3m",
                        "status": "Hit",
                        "note": "2025 year Adjusted EBIT within latest annual guide.",
                    },
                    {
                        "metric": "Adjusted EPS guidance",
                        "initial": "$1.10-$1.30",
                        "q1": "$1.10-$1.30",
                        "q2": "$1.20-$1.40",
                        "q3": "$1.20-$1.40",
                        "q4": "",
                        "actual": "$1.36",
                        "status": "Hit",
                        "note": "2025 year adjusted diluted EPS within latest annual guide.",
                    },
                    {
                        "metric": "FCF target",
                        "initial": "",
                        "q1": "$330m-$370m",
                        "q2": "$330m-$370m",
                        "q3": "$330m-$370m",
                        "q4": "",
                        "actual": "$358.3m",
                        "status": "Hit",
                        "note": "2025 year source-defined Free Cash Flow within target range.",
                    },
                ],
            ),
            (
                2024,
                [
                    {
                        "metric": "Adjusted EBIT guidance",
                        "initial": "",
                        "q1": "",
                        "q2": "",
                        "q3": "$355m-$360m",
                        "q4": "",
                        "actual": "$385.2m",
                        "status": "Beat",
                        "note": "2024 year Adjusted EBIT exceeded the Q3 guide.",
                    },
                    {
                        "metric": "Cost savings target",
                        "initial": "",
                        "q1": "",
                        "q2": "$120m-$160m",
                        "q3": "$150m-$170m",
                        "q4": "$170m-$190m",
                        "actual": "$120m run-rate",
                        "status": "On track",
                        "note": "Run-rate progress shown separately from final realized savings.",
                    },
                ],
            ),
        ]

    def pbi_source_record_maps(self) -> dict[str, dict[str, tuple[str, ...]]]:
        return {
            "revenue": {
                "2025-Q4": (
                    "$478m",
                    "FY: $1.89bn",
                    "Missed",
                    "Q4 actual shown in Actual; FY result shown in Progress / run-rate.",
                    "Completed",
                ),
            },
            "fcf": {
                "2025-Q1": ("$-20.5m", "YTD: $-20.5m", "On track", "Quarter/YTD source-defined Free Cash Flow; annual guide still open."),
                "2025-Q2": ("$106.5m", "YTD: $86.0m", "On track", "Quarter/YTD source-defined Free Cash Flow; annual guide still open."),
                "2025-Q3": ("$60.4m", "YTD: $146.4m", "On track", "Quarter/YTD source-defined Free Cash Flow; annual guide still open."),
                "2025-Q4": (
                    "$212m",
                    "FY: $358.3m",
                    "Hit",
                    "Q4 actual shown in Actual; FY source-defined Free Cash Flow shown in Progress / run-rate.",
                    "Completed",
                ),
            },
            "adjusted_ebit": {
                "2024-Q3": ("$102.8m", "YTD: $270.8m", "On track", "Quarter/YTD Adjusted EBIT; annual guide still open."),
                "2024-Q4": (
                    "$114m",
                    "FY: $385.2m",
                    "Beat",
                    "Q4 actual shown in Actual; FY Adjusted EBIT shown in Progress / run-rate.",
                    "Completed",
                ),
                "2025-Q1": ("$119.7m", "YTD: $119.7m", "On track", "Quarter/YTD Adjusted EBIT; annual guide still open."),
                "2025-Q2": ("$102.3m", "YTD: $222.0m", "On track", "Quarter/YTD Adjusted EBIT; annual guide still open."),
                "2025-Q3": ("$107.3m", "YTD: $329.3m", "On track", "Quarter/YTD Adjusted EBIT; annual guide still open."),
                "2025-Q4": (
                    "$132m",
                    "FY: $461.3m",
                    "Hit",
                    "Q4 actual shown in Actual; FY Adjusted EBIT shown in Progress / run-rate.",
                    "Completed",
                ),
                "2026-Q1": ("$130.4m", "YTD: $130.4m", "On track", "Quarter/YTD Adjusted EBIT; annual guide still open."),
            },
            "adjusted_eps": {
                "2025-Q4": (
                    "$0.45",
                    "FY: $1.35",
                    "Hit",
                    "Q4 actual shown in Actual; FY adjusted diluted EPS shown in Progress / run-rate.",
                    "Completed",
                ),
                "2026-Q1": ("$0.47", "YTD: $0.47", "On track", "Quarter/YTD adjusted diluted EPS; annual guide still open."),
            },
        }

    def pbi_source_record_values(self, record: Any) -> tuple[str, str, str, str, str]:
        values = list(record or [])
        while len(values) < 5:
            values.append("")
        return str(values[0]), str(values[1]), str(values[2]), str(values[3]), str(values[4])

    def pbi_2026_q1_rows(self) -> list[list[str]]:
        return [
            [
                "Revenue guidance",
                "$1.76bn-$1.86bn",
                "$1.8bn-$1.86bn",
                "Updated",
                "$477.4m",
                "YTD: $477.4m",
                "On track",
                "2026 year",
                "2026-Q1",
                "2026-03-31",
                "2026 year Revenue guidance updated to $1.8bn-$1.86bn.",
            ],
            [
                "Adjusted EBIT guidance",
                "$410m-$460m",
                "$425m-$465m",
                "Updated",
                "$130.4m",
                "YTD: $130.4m",
                "On track",
                "2026 year",
                "2026-Q1",
                "2026-03-31",
                "2026 year Adjusted EBIT guidance updated to $425m-$465m.",
            ],
            [
                "Adjusted EPS guidance",
                "$1.40-$1.60",
                "$1.50-$1.65",
                "Updated",
                "$0.47",
                "YTD: $0.47",
                "On track",
                "2026 year",
                "2026-Q1",
                "2026-03-31",
                "Quarter/YTD adjusted diluted EPS; annual guide still open.",
            ],
            [
                "FCF target",
                "$340m-$370m",
                "$345m-$380m",
                "Updated",
                "$28.3m",
                "YTD: $28.3m",
                "On track",
                "2026 year",
                "2026-Q1",
                "2026-03-31",
                "2026 year source-defined Free Cash Flow target updated to $345m-$380m.",
            ],
        ]

    def pbi_cost_rows(self) -> list[tuple[str, list[str]]]:
        return [
            (
                "2024-Q2 revisions",
                [
                    "Cost savings target",
                    "",
                    "$120m-$160m",
                    "Initial",
                    "$70m",
                    "Run-rate: $70m",
                    "On track",
                    "Annualized program",
                    "2024-Q2",
                    "2024-08-08",
                    "$70m annualized reductions initiated; target $120m-$160m.",
                ],
            ),
            (
                "2024-Q3 revisions",
                [
                    "Cost savings target",
                    "$120m-$160m",
                    "$150m-$170m",
                    "Raised",
                    "$90m",
                    "Run-rate: $90m",
                    "On track",
                    "Annualized program",
                    "2024-Q3",
                    "2024-09-30",
                    "Exited Q3 with $90m annualized savings; target raised.",
                ],
            ),
            (
                "2024-Q4 revisions",
                [
                    "Cost savings target",
                    "$150m-$170m",
                    "$170m-$190m",
                    "Raised",
                    "$120m",
                    "Run-rate: $120m",
                    "On track",
                    "Annualized program",
                    "2024-Q4",
                    "2024-12-31",
                    "Exited 2024 at ~$120m run-rate; target raised.",
                ],
            ),
            (
                "2025-Q1 revisions",
                [
                    "Cost savings target",
                    "$170m-$190m",
                    "$180m-$200m",
                    "Raised",
                    "$157m",
                    "Run-rate: $157m",
                    "On track",
                    "Annualized program",
                    "2025-Q1",
                    "2025-03-31",
                    "$157m run-rate; target raised to $180m-$200m.",
                ],
            ),
            (
                "2026-Q1 revisions",
                [
                    "Cost savings target",
                    "$180m-$200m",
                    "$180m-$200m",
                    "Maintained",
                    "$157m",
                    "Run-rate: $157m",
                    "On track",
                    "Annualized program",
                    "2026-Q1",
                    "2026-03-31",
                    "Latest disclosed run-rate against annualized savings target.",
                ],
            ),
        ]

    def pbi_q4_promise_semantics(self) -> dict[tuple[str, str], tuple[str, str, str, str, str]]:
        return {
            ("2025-Q4 revisions", "Revenue guidance"): (
                "Completed",
                "$478m",
                "FY: $1.89bn",
                "Missed",
                "Q4 actual shown in Actual; FY result shown in Progress / run-rate.",
            ),
            ("2025-Q4 revisions", "Adjusted EBIT guidance"): (
                "Completed",
                "$132m",
                "FY: $461.3m",
                "Hit",
                "Q4 actual shown in Actual; FY Adjusted EBIT shown in Progress / run-rate.",
            ),
            ("2025-Q4 revisions", "Adjusted EPS guidance"): (
                "Completed",
                "$0.45",
                "FY: $1.35",
                "Hit",
                "Q4 actual shown in Actual; FY adjusted diluted EPS shown in Progress / run-rate.",
            ),
            ("2025-Q4 revisions", "FCF target"): (
                "Completed",
                "$212m",
                "FY: $358.3m",
                "Hit",
                "Q4 actual shown in Actual; FY source-defined Free Cash Flow shown in Progress / run-rate.",
            ),
            ("2024-Q4 revisions", "Adjusted EBIT guidance"): (
                "Completed",
                "$114m",
                "FY: $385.2m",
                "Beat",
                "Q4 actual shown in Actual; FY Adjusted EBIT shown in Progress / run-rate.",
            ),
        }

    def pbi_quarter_notes(self) -> list[tuple[str, str, str, str]]:
        return [
            ("2024-06-30", "Cost rationalization", "$70m annualized reductions initiated; target $120m-$160m.", "Cost savings / rationalization"),
            ("2024-06-30", "Cash optimization", "Go-forward cash needs reduced to $240m, up from $200m target.", "Cash optimization"),
            ("2024-06-30", "GEC exit / loss removal", "GEC exit expected to eliminate about $136m of 2023 annualized losses.", "GEC loss removal"),
            ("2024-12-31", "Cost rationalization", "Exited 2024 at about $120m annualized savings; target raised to $170m-$190m.", "Cost savings / rationalization"),
            ("2024-12-31", "Cash optimization", "PB Bank program accelerated $41m of lease cash; initiatives unlocked more than $200m.", "Cash optimization"),
            ("2024-12-31", "GEC exit / loss removal", "GEC exit loss removal tracked separately from cost savings; 2023 losses were $136m.", "GEC loss removal"),
        ]

    def anf_guidance_progression_rows(
        self,
        *,
        annual_sales_growth: Any,
        annual_capex_actual: Any,
    ) -> list[tuple[int, list[dict[str, Any]]]]:
        return [
            (
                2024,
                [
                    {
                        "metric": "Net sales growth",
                        "initial": "",
                        "q1": "around +14%",
                        "q2": "",
                        "q3": "+12-13%",
                        "q4": "+14-15%",
                        "actual": "+14.3%",
                        "status": "Hit",
                        "note": "FY2024 source-backed sales guide and final annual sales growth.",
                    },
                    {
                        "metric": "Operating margin",
                        "initial": "",
                        "q1": "",
                        "q2": "",
                        "q3": "14-15%",
                        "q4": "around 16%",
                        "actual": "16.2%",
                        "status": "Beat",
                        "note": "FY2024 operating margin guide and final annual result.",
                    },
                    {
                        "metric": "Capex",
                        "initial": "~$170m",
                        "q1": "~$170m",
                        "q2": "",
                        "q3": "~$170m",
                        "q4": "",
                        "actual": annual_capex_actual(2024),
                        "status": "Mixed" if annual_capex_actual(2024) else "Open",
                        "note": "FY2024 capex guidance compared with fiscal-year capex from History_Q.",
                    },
                ],
            ),
            (
                2023,
                [
                    {
                        "metric": "Q1 sales growth",
                        "initial": "+1-3%",
                        "q1": "",
                        "q2": "",
                        "q3": "",
                        "q4": "",
                        "actual": "2.9%",
                        "status": "Hit",
                        "note": "2023-Q1 revenue guidance compared with source-backed Q1 sales growth.",
                    },
                    {
                        "metric": "Capex",
                        "initial": "~$160m",
                        "q1": "",
                        "q2": "",
                        "q3": "",
                        "q4": "",
                        "actual": annual_capex_actual(2023),
                        "status": "Hit" if annual_capex_actual(2023) else "Open",
                        "note": "2023 year capex guidance compared with fiscal-year capex from History_Q.",
                    },
                ],
            ),
            (
                2022,
                [
                    {
                        "metric": "Net sales growth",
                        "initial": "",
                        "q1": "~+45%",
                        "q2": "~+70%",
                        "q3": "~+92%",
                        "q4": "+1-3%",
                        "actual": f"FY {annual_sales_growth(2022)} / Q4 +3.3%",
                        "status": "Mixed",
                        "note": "Quarter updates were quarterly growth; FY actual and Q4 actual are shown separately to avoid mixing bases.",
                    },
                ],
            ),
        ]

    def anf_quarter_eps_actuals(self) -> dict[str, tuple[str, str, str, str]]:
        return {
            "2025-Q1": ("$1.59 adjusted", "YTD: $1.59 adjusted", "On track", "Quarter/YTD adjusted EPS; annual guide still open."),
            "2025-Q2": ("$2.32 adjusted", "YTD: $3.91 adjusted", "On track", "Quarter/YTD adjusted EPS; annual guide still open."),
            "2025-Q3": ("$2.36 adjusted", "YTD: $6.27 adjusted", "On track", "Quarter/YTD adjusted EPS; annual guide still open."),
        }

    def anf_diluted_share_progress(self) -> dict[str, tuple[str, str, str, str]]:
        return {
            "2025-Q1": ("50.6m diluted", "Δ vs guide: +1.6m; Δ YTD: -1.8m", "On track", "Q1 diluted shares; Progress shows share-count delta versus guide and fiscal-year start."),
            "2025-Q2": ("48.6m diluted", "Δ vs guide: -0.4m; Δ YTD: -3.9m", "On track", "Q2 diluted shares; Progress shows share-count delta versus guide and fiscal-year start."),
            "2025-Q3": ("47.9m diluted", "Δ vs guide: -0.1m; Δ YTD: -4.6m", "On track", "Q3 diluted shares; Progress shows share-count delta versus guide and fiscal-year start."),
        }

    def anf_final_q4_rows(self) -> dict[str, tuple[str, str, str, str, str]]:
        return {
            "Net sales growth": ("at least +6%", "at least +6%", "+5.4%", "FY: +6%", "Completed"),
            "Operating margin": ("around 13%", "around 13%", "14.1%", "FY: 13.3% GAAP / 12.5% adjusted", "Mixed"),
            "Adjusted EPS": ("$10.30-$10.40", "$10.30-$10.40", "$3.68 adjusted", "FY: $9.86 adjusted", "Missed"),
            "Capex": ("~$245m", "~$245m", "$55.6m", "FY: $240.8m", "Hit"),
            "Diluted shares": ("~48m", "~48m", "46.8m diluted", "Δ vs guide: -1.2m; Δ YTD: -5.6m", "Completed"),
            "Share repurchases": ("~$450m", "~$450m", "$100.0m", "FY: $450m", "Completed"),
        }

    def anf_q4_note(self) -> str:
        return "Q4 actual shown in Actual; FY/YTD result shown in Progress / run-rate."

    def anf_q4_adjusted_eps_note(self) -> str:
        return "Q4 actual shown in Actual (adjusted EPS); official FY adjusted EPS shown in Progress / run-rate. FY can differ from summed rounded quarters."

    def anf_q4_share_note(self) -> str:
        return "Q4 actual shown in Actual (diluted shares); Progress shows share-count reduction versus guide and YTD."

    def anf_final_q4_timeline_rows(self) -> list[tuple[str, list[str]]]:
        rows = []
        for metric_txt, (prev_txt, new_txt, _actual_txt, _progress_txt, status_txt) in self.anf_final_q4_rows().items():
            rows.append(
                (
                    "2025-Q4 revisions",
                    [
                        metric_txt,
                        prev_txt,
                        new_txt,
                        "Completed",
                        "",
                        "",
                        status_txt,
                        "2025 year",
                        "2025-Q4",
                        "2026-03-04",
                        self.anf_q4_note(),
                    ],
                )
            )
        return rows

    def anf_q2_diluted_share_row(self) -> tuple[str, list[str]]:
        return (
            "2025-Q2 revisions",
            [
                "Diluted shares",
                "~49m",
                "~49m",
                "Maintained",
                "48.6m diluted",
                "Δ vs guide: -0.4m; Δ YTD: -3.9m",
                "On track",
                "2025 year",
                "2025-Q2",
                "2025-08-28",
                "Q2 diluted shares; Progress shows share-count delta versus guide and fiscal-year start.",
            ],
        )

    def gpre_source_rows(self) -> list[tuple[str, list[str]]]:
        return [
            (
                "2024-Q4 revisions",
                [
                    "Cost savings target",
                    "",
                    "$50m annualized savings",
                    "Initial",
                    "$30m",
                    "Executed: $30m",
                    "On track",
                    "Annualized program",
                    "2024-Q4",
                    "2025-02-07",
                    "Up to $50m identified; first $30m executed.",
                ],
            ),
            (
                "2024-Q4 revisions",
                [
                    "Capex guidance (2025 year)",
                    "",
                    "$20m-$35m",
                    "Initial",
                    "",
                    "",
                    "Open",
                    "2025 year",
                    "2024-Q4",
                    "2025-02-07",
                    "Plant-related 2025 capex excluding Nebraska carbon equipment.",
                ],
            ),
            (
                "2025-Q1 revisions",
                [
                    "Capex guidance (2025 year)",
                    "$20m-$35m",
                    "~$20m remaining",
                    "Updated",
                    "$16.7m",
                    "YTD: $16.7m",
                    "On track",
                    "2025 year",
                    "2025-Q1",
                    "2025-05-08",
                    "Q1 capex progress; remaining 2025 capex about $20m.",
                ],
            ),
            (
                "2025-Q1 revisions",
                [
                    "Advantage Nebraska startup",
                    "",
                    "early 2025-Q4 startup",
                    "Initial",
                    "",
                    "",
                    "On track",
                    "2025-Q4",
                    "2025-Q1",
                    "2025-05-08",
                    "Construction targeted for late Q3/early Q4 completion.",
                ],
            ),
            (
                "2025-Q1 revisions",
                [
                    "Cost savings target",
                    "$50m annualized savings",
                    "$50m annualized savings",
                    "Maintained",
                    "$45m",
                    "Remaining: $5m",
                    "On track",
                    "Annualized program",
                    "2025-Q1",
                    "2025-05-08",
                    "Approximately $45m annualized savings accomplished; about $5m remaining.",
                ],
            ),
            (
                "2025-Q3 revisions",
                [
                    "45Z monetization",
                    "",
                    "Q3 45Z value recorded",
                    "Initial",
                    "$26.5m",
                    "YTD: $26.5m",
                    "On track",
                    "2025-Q3",
                    "2025-Q3",
                    "2025-09-30",
                    "$26.5m 45Z value recorded YTD through Q3; separate Q4 guide remains in the Q4 row.",
                ],
            ),
        ]
