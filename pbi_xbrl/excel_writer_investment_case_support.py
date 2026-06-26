"""Investment Case support/data-builder helpers for workbook writer."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, MutableMapping, Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class InvestmentCaseSupportDeps:
    runtime: MutableMapping[str, Any]


class InvestmentCaseSupport:
    def __init__(self, deps: InvestmentCaseSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    def refresh_runtime(self, runtime: MutableMapping[str, Any]) -> None:
        self.runtime.update(runtime)

    def _rt(self, name: str) -> Any:
        return self.runtime[name]

    def anf_investment_case_sheet_order(
        self,
        desired_sheet_order: Sequence[str],
        raw_sheet_cluster: Sequence[str],
        *,
        is_anf_profile: bool = False,
    ) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        return self.investment_case_sheet_order(
            desired_sheet_order,
            raw_sheet_cluster,
            ticker="ANF" if is_anf_profile else "",
        )


    def investment_case_sheet_order(
        self,
        desired_sheet_order: Sequence[str],
        raw_sheet_cluster: Sequence[str],
        *,
        ticker: Any = "",
    ) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        desired = [str(x) for x in (desired_sheet_order or ()) if str(x or "").strip()]
        raw = [str(x) for x in (raw_sheet_cluster or ()) if str(x or "").strip()]
        ticker_txt = str(ticker or "").strip().upper()
        if not ticker_txt:
            return tuple(desired), tuple(raw)
        case_sheet = f"{ticker_txt}_Investment_Case"
        data_sheet = f"{ticker_txt}_Investment_Case_Data"
        tax_audit_sheet = "Scenario_Bridge_Tax_Treatment"
        driver_assumptions_sheet = "Scenario_Driver_Assumptions"
        desired = [x for x in desired if x != case_sheet]
        insert_at = desired.index("Operating_Drivers") + 1 if "Operating_Drivers" in desired else 0
        desired.insert(insert_at, case_sheet)
        if ticker_txt == "GPRE" and "Economics_Overlay" in desired and "Quarter_Notes_UI" in desired:
            # Keep the visible investment-case handoff consistent across tickers:
            # Operating_Drivers -> *_Investment_Case -> Quarter_Notes_UI.  GPRE's
            # detailed economics sheet remains available, just not between the
            # operating driver dashboard and the investment-case page.
            desired = [x for x in desired if x != "Economics_Overlay"]
            desired.insert(desired.index("Quarter_Notes_UI") + 1, "Economics_Overlay")
        raw = [x for x in raw if x not in {data_sheet, tax_audit_sheet, driver_assumptions_sheet}]
        raw_insert = raw.index("operating_drivers_raw") + 1 if "operating_drivers_raw" in raw else len(raw)
        raw.insert(raw_insert, data_sheet)
        raw.insert(raw_insert + 1, tax_audit_sheet)
        raw.insert(raw_insert + 2, driver_assumptions_sheet)
        return tuple(desired), tuple(raw)


    def build_anf_investment_case_data(
        self,
        *,
        hist: Any,
        operating_driver_rows: Sequence[Dict[str, Any]],
        guidance_normalized: Any,
        slides_segments: Any,
        valuation_summary: Any = None,
        adjusted_metrics: Any = None,
    ) -> pd.DataFrame:
        _anf_visible_guidance_normalized_frame = self._rt("_anf_visible_guidance_normalized_frame")
        _anf_visible_quarter_label = self._rt("_anf_visible_quarter_label")
        hist_df = hist.copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame()
        drivers = [dict(r) for r in (operating_driver_rows or [])]
        guidance_df = guidance_normalized.copy() if isinstance(guidance_normalized, pd.DataFrame) else pd.DataFrame()
        if not guidance_df.empty:
            guidance_df = _anf_visible_guidance_normalized_frame(guidance_df)
        seg_df = slides_segments.copy() if isinstance(slides_segments, pd.DataFrame) else pd.DataFrame()
        valuation_df = valuation_summary.copy() if isinstance(valuation_summary, pd.DataFrame) else pd.DataFrame()
        adj_df = adjusted_metrics.copy() if isinstance(adjusted_metrics, pd.DataFrame) else pd.DataFrame()

        def _num(x: Any) -> Optional[float]:
            val = pd.to_numeric(x, errors="coerce")
            return float(val) if pd.notna(val) else None

        def _pct_num(x: Any) -> Optional[float]:
            val = _num(x)
            if val is None:
                return None
            return val * 100.0 if abs(val) <= 1.5 else val

        def _fmt_pct(x: Any, *, plus: bool = True, decimals: int = 0) -> str:
            val = _pct_num(x)
            if val is None:
                return ""
            sign = "+" if plus and val > 0 else ""
            return f"{sign}{val:.{decimals}f}%"

        def _fmt_usd_m(x: Any, *, approx: bool = False) -> str:
            val = _num(x)
            if val is None:
                return ""
            prefix = "~" if approx else ""
            return f"{prefix}${val:,.0f}m"

        def _fmt_eps(x: Any) -> str:
            val = _num(x)
            return f"${val:.2f}" if val is not None else ""

        def _drv_value(key: str, default: Optional[float] = None) -> Optional[float]:
            key_low = str(key or "").lower()
            candidates: List[Tuple[pd.Timestamp, float]] = []
            for rec in drivers:
                if str(rec.get("_driver_key") or "").strip().lower() != key_low:
                    continue
                val = _num(rec.get("Value"))
                if val is None:
                    continue
                q_ts = pd.to_datetime(rec.get("Quarter"), errors="coerce")
                candidates.append((pd.Timestamp(q_ts) if pd.notna(q_ts) else pd.Timestamp.max, val))
            if candidates:
                candidates.sort(key=lambda item: item[0])
                return candidates[-1][1]
            return default

        def _guidance_row(metric_hint: str, horizon: str = "2026 year") -> Dict[str, Any]:
            if guidance_df.empty:
                return {}
            metric_low = str(metric_hint or "").lower()
            horizon_low = str(horizon or "").lower()
            tmp = guidance_df.copy()
            if "metric_hint" in tmp.columns:
                tmp = tmp[tmp["metric_hint"].astype(str).str.lower().str.contains(metric_low, regex=False, na=False)]
            if "period_label" in tmp.columns:
                tmp = tmp[tmp["period_label"].astype(str).str.lower().eq(horizon_low)]
            if tmp.empty and "horizon_label" in guidance_df.columns:
                tmp = guidance_df[
                    guidance_df["metric_hint"].astype(str).str.lower().str.contains(metric_low, regex=False, na=False)
                    & guidance_df["horizon_label"].astype(str).str.lower().eq(horizon_low)
                ].copy()
            if tmp.empty:
                return {}
            q_col = "quarter" if "quarter" in tmp.columns else None
            if q_col:
                tmp["_q_sort"] = pd.to_datetime(tmp[q_col], errors="coerce")
                tmp = tmp.sort_values("_q_sort")
            return dict(tmp.iloc[-1])

        def _guidance_display(metric_hint: str, horizon: str = "2026 year", default: str = "") -> str:
            rec = _guidance_row(metric_hint, horizon)
            if not rec:
                return default
            metric_low_for_display = str(metric_hint or "").lower()
            low = _num(rec.get("low"))
            high = _num(rec.get("high"))
            val = _num(rec.get("value"))
            unit = str(rec.get("unit") or "").strip().lower()
            if low is not None and high is not None:
                if "$" in unit:
                    return f"${low:.2f}-${high:.2f}" if max(abs(low), abs(high)) < 50 else f"${low:.0f}-${high:.0f}m"
                if "%" in unit:
                    out = f"{low:.1f}-{high:.1f}%" if any(abs(x - round(x)) > 1e-6 for x in (low, high)) else f"{low:.0f}-{high:.0f}%"
                    return f"+{out}" if re.search(r"\b(revenue|sales growth|net sales)\b", metric_low_for_display) and low >= 0 else out
                return f"{low:g}-{high:g} {unit}".strip()
            if val is not None:
                if "$" in unit:
                    return f"~${val:.0f}m" if val >= 50 else f"${val:.2f}"
                if "%" in unit:
                    out = f"~{val:.0f}%"
                    return f"+{out}" if re.search(r"\b(revenue|sales growth|net sales)\b", metric_low_for_display) and val >= 0 else out
                if "share" in unit:
                    return f"~{val:.0f}m"
                return f"~{val:g} {unit}".strip()
            return default

        def _seg_value(segment: str, metric: str, period_type: str = "", quarter: Any = None, default: Optional[float] = None) -> Optional[float]:
            if seg_df.empty:
                return default
            tmp = seg_df.copy()
            if "segment" in tmp.columns:
                tmp = tmp[tmp["segment"].astype(str).str.lower().eq(str(segment).lower())]
            if "metric" in tmp.columns:
                tmp = tmp[tmp["metric"].astype(str).str.lower().eq(str(metric).lower())]
            if period_type and "period_type" in tmp.columns:
                tmp = tmp[tmp["period_type"].astype(str).str.lower().eq(str(period_type).lower())]
            if quarter is not None and "quarter" in tmp.columns:
                qd = pd.to_datetime(quarter, errors="coerce")
                if pd.notna(qd):
                    tmp = tmp[pd.to_datetime(tmp["quarter"], errors="coerce").dt.date.eq(pd.Timestamp(qd).date())]
            if tmp.empty:
                return default
            tmp = tmp.copy()
            tmp["_q_sort"] = pd.to_datetime(tmp.get("quarter"), errors="coerce") if "quarter" in tmp.columns else pd.Timestamp.max
            tmp = tmp.sort_values("_q_sort")
            return _num(tmp.iloc[-1].get("value"))

        def _seg_value_by_visible_label(segment: str, metric: str, period_type: str, visible_label: str) -> Optional[float]:
            if seg_df.empty or "quarter" not in seg_df.columns:
                return None
            tmp = seg_df.copy()
            tmp["_visible_label"] = pd.to_datetime(tmp["quarter"], errors="coerce").apply(
                lambda qv: _anf_visible_quarter_label(pd.Timestamp(qv).date()) if pd.notna(qv) else ""
            )
            tmp = tmp[tmp["_visible_label"].eq(str(visible_label or ""))]
            if tmp.empty:
                return None
            return _seg_value(segment, metric, period_type, tmp.iloc[-1].get("quarter"))

        def _hist_latest_value(cols: Sequence[str], default: Optional[float] = None) -> Optional[float]:
            if hist_df.empty:
                return default
            tmp = hist_df.copy()
            if "quarter" in tmp.columns:
                tmp["_q_sort"] = pd.to_datetime(tmp["quarter"], errors="coerce")
                tmp = tmp.sort_values("_q_sort")
            for col in cols:
                if col in tmp.columns:
                    series = pd.to_numeric(tmp[col], errors="coerce").dropna()
                    if not series.empty:
                        return float(series.iloc[-1])
            return default

        def _valuation_metric_value(patterns: Sequence[str], default: Optional[float] = None) -> Optional[float]:
            if valuation_df.empty:
                return default
            label_cols = [c for c in valuation_df.columns if str(c).strip().lower() in {"metric", "label", "item", "name"}]
            value_cols = [c for c in valuation_df.columns if str(c).strip().lower() in {"value", "latest", "amount", "actual"}]
            if not label_cols or not value_cols:
                return default
            labels = valuation_df[label_cols[0]].astype(str)
            for pattern in patterns:
                mask = labels.str.contains(str(pattern or ""), case=False, regex=False, na=False)
                if not mask.any():
                    continue
                for value_col in value_cols:
                    vals = pd.to_numeric(valuation_df.loc[mask, value_col], errors="coerce").dropna()
                    if not vals.empty:
                        return float(vals.iloc[-1])
            return default

        rows: List[Dict[str, Any]] = []

        def _add(section: str, metric: str, value: Any = None, unit: str = "", display: str = "", source: str = "ANF parsed sources", source_note: str = "", **extra: Any) -> None:
            rows.append(
                {
                    "section": section,
                    "metric": metric,
                    "value": value,
                    "unit": unit,
                    "display": display if display != "" else (str(value) if value is not None else ""),
                    "source": source,
                    "source_note": source_note,
                    **extra,
                }
            )

        ab_annual_sales = _seg_value("Abercrombie", "revenue", "annual", default=2_523_662_000.0)
        ho_annual_sales = _seg_value("Hollister", "revenue", "annual", default=2_742_630_000.0)
        americas_annual_sales = _seg_value("Americas", "revenue", "annual", default=4_290_395_000.0)
        emea_annual_sales = _seg_value("EMEA", "revenue", "annual", default=818_140_000.0)
        apac_annual_sales = _seg_value("APAC", "revenue", "annual", default=157_757_000.0)
        annual_revenue = (
            (ab_annual_sales or 0.0) + (ho_annual_sales or 0.0)
            if ab_annual_sales is not None and ho_annual_sales is not None
            else 5_266_292_000.0
        )
        annual_revenue_m = annual_revenue / 1_000_000.0
        reported_op_margin = 13.3
        adj_op_margin = 12.5
        fy2026_op_margin_low = _num(_guidance_row("Operating margin", "2026 year").get("low")) or 12.0
        fy2026_op_margin_high = _num(_guidance_row("Operating margin", "2026 year").get("high")) or 12.5
        fy2026_op_margin_mid = (fy2026_op_margin_low + fy2026_op_margin_high) / 2.0
        implied_low_bps = int(round((fy2026_op_margin_low - reported_op_margin) * 100.0))
        implied_high_bps = int(round((fy2026_op_margin_high - reported_op_margin) * 100.0))
        adj_eps_2025 = 9.86
        gaap_eps_2025 = 10.46
        eps_2026_low = _num(_guidance_row("Adj EPS", "2026 year").get("low")) or 10.20
        eps_2026_high = _num(_guidance_row("Adj EPS", "2026 year").get("high")) or 11.00
        eps_2026_mid = (eps_2026_low + eps_2026_high) / 2.0
        shares_2026 = _num(_guidance_row("Diluted shares", "2026 year").get("value")) or 45.0
        pretax_income = _hist_latest_value(["pretax_income", "income_before_taxes", "income_before_income_taxes"])
        tax_expense = _hist_latest_value(["income_tax_expense", "tax_expense", "provision_for_income_taxes"])
        if pretax_income is not None and tax_expense is not None and abs(float(pretax_income)) > 1e-9:
            tax_rate = max(0.0, min(0.45, float(tax_expense) / float(pretax_income)))
            tax_rate_source = "annual actual tax expense / pretax income"
        else:
            tax_rate = 0.285
            tax_rate_source = "fallback aligned to FY2025 effective tax rate"
        avg_buyback_price = _drv_value("average_buyback_price", 83.33) or 83.33
        adj_ebitda_m = _valuation_metric_value(["Adjusted EBITDA TTM", "Adj EBITDA TTM"], 815.590) or 815.590
        fcf_m = _valuation_metric_value(["FCF TTM", "Free cash flow TTM"], 378.368) or 378.368
        net_cash_incl_sec_m = _valuation_metric_value(["Net cash incl. marketable securities", "Net cash incl"], 784.576) or 784.576
        lease_adj_net_debt_m = _valuation_metric_value(["Lease-adjusted net debt incl. securities", "Lease-adjusted net debt"], 383.519) or 383.519

        q1_tar_bps = _drv_value("q1_fy2026_tariff_headwind_bps", 290.0)
        q1_tar_cost = _drv_value("q1_fy2026_tariff_headwind", 30.0)
        fy_tar_bps = _drv_value("fy2026_tariff_headwind_bps", 70.0)
        fy_tar_cost = _drv_value("fy2026_tariff_headwind", 40.0)
        freight_bps = _drv_value("freight_tailwind_bps", 160.0)
        marketing_bps = _drv_value("marketing_headwind_bps", 50.0)
        erp_bps = _drv_value("erp_margin_headwind_bps", 100.0)
        if erp_bps is None:
            erp_bps = 100.0
        _add("Investment Snapshot", "Model read", display="Constructive but margin-sensitive.", source="model thesis", source_note="Model-based investment read, not personal financial advice.")
        _add("Investment Snapshot", "Why it can work", display="Net cash, strong FCF, buyback capacity, Hollister momentum, digital scale and EPS support from lower share count.", source="model thesis", source_note="Synthesized from Valuation, Operating_Drivers and ANF source materials.")
        _add("Investment Snapshot", "Key debate", display="Can ANF sustain high margins and EPS after the turnaround despite tougher comps, tariffs, ERP disruption and margin normalization?", source="model thesis", source_note="Q4 2025 guidance bridge and model sensitivity.")
        _add("Investment Snapshot", "Upside path", display="Sales guide holds, Hollister stays strong, Abercrombie stabilizes, inventory remains clean and tariff mitigation protects margins.", source="model thesis", source_note="Investment case upside watch list.")
        _add("Investment Snapshot", "Downside path", display="Hollister slows, Abercrombie weakens again, inventory turns into markdown risk or tariff/ERP costs normalize margins below guide.", source="model thesis", source_note="Investment case downside watch list.")
        _add("Investment Snapshot", "Watch next", display="Q1 2026 comps, gross margin bridge, inventory units, tariff mitigation, buyback pace and 2026 EPS guidance revisions.", source="model thesis", source_note="Near-term source-backed monitoring list.")
        _add("Investment Snapshot", "Current stance based on model data", display="The case works if 2026 EPS stays above $10 and the market credits 12.0-12.5% margin durability; buybacks help, but margin proof matters most.", source="model thesis", source_note="Synthesis of guidance, sensitivity and capital allocation data.")
        _add(
            "Key Debates",
            "Margin durability after turnaround",
            display="Can ANF sustain high margins after tariff, ERP, freight, marketing and normalization headwinds?",
            bull_evidence="2026 guide still implies 12.0-12.5% operating margin with mitigation levers.",
            bear_evidence="Tariffs/ERP/marketing and tougher comps pressure margin quality.",
            next_proof_point="Q1 2026 gross margin bridge and operating margin guide tracking.",
            current_read="Constructive but margin-sensitive.",
        )
        _add(
            "Key Debates",
            "Brand momentum and lapping risk",
            display="Can Hollister stay strong while Abercrombie stabilizes against tougher comparisons?",
            bull_evidence="Hollister growth and brand/digital scale remain visible drivers.",
            bear_evidence="Abercrombie slowdown or lapping risk can pressure sales growth.",
            next_proof_point="Comps by brand/geography and Q1 2026 sales guide.",
            current_read="Hollister is the current engine; Abercrombie proof matters.",
        )
        _add(
            "Key Debates",
            "Buybacks vs FCF",
            display="Do buybacks support EPS without overusing net cash?",
            bull_evidence="2025 buybacks were about $450m / 5.4m shares and net cash remains strong.",
            bear_evidence="Buybacks exceeded FCF, so capital returns should stay disciplined.",
            next_proof_point="2026 buyback pace, FCF and net cash trend.",
            current_read="Positive EPS support, but a watch item.",
        )
        for scenario, assumptions, eps_val, multiple, read in [
            ("Bear", "Sales guide misses or margin falls below guide; buybacks less helpful.", 9.50, 10, "Downside if margin durability breaks."),
            ("Base", "Sales guide holds, margin near midpoint and buybacks support EPS.", eps_2026_mid, 13, "Base case uses company guide and default P/E."),
            ("Bull", "Hollister stays strong, tariff mitigation works and EPS expands.", 12.00, 16, "Upside requires margin proof and multiple support."),
        ]:
            _add(
                "Bear / Base / Bull Scenario",
                scenario,
                display=assumptions,
                earnings_metric=f"EPS ${eps_val:.2f}",
                multiple_yield=f"{multiple}x P/E",
                implied_value_share=f"${eps_val * multiple:,.0f}",
                scenario_read=read,
            )
        market_price = _valuation_metric_value(["Market price", "Current market price", "Share price"], None)
        if market_price is None:
            _add(
                "What Market Is Pricing",
                "Missing market price",
                display="Market price/current EV unavailable; retain this section for implied metric once price is available.",
                source="Valuation / market data",
            )
        else:
            implied_pe = market_price / eps_2026_mid if eps_2026_mid else None
            _add(
                "What Market Is Pricing",
                "Implied P/E",
                market_price,
                "$/share",
                f"Price ${market_price:,.2f} implies {implied_pe:.1f}x 2026 EPS midpoint" if implied_pe else f"Price ${market_price:,.2f}",
                source="Valuation / market data",
            )
        for item, impact, cash, recurring, read in [
            ("Buybacks vs FCF", "Supports EPS but exceeded FCF in 2025.", "Yes", "Discretionary", "Good if disciplined; watch net cash use."),
            ("Inventory", "Can signal markdown risk if demand slows.", "Working capital", "Recurring", "Currently explainable by tariffs/ERP prebuild."),
            ("Tariffs", "Headwind to margin and inventory cost.", "Yes", "Policy/input dependent", "Mitigation is central to 2026 proof."),
            ("ERP disruption", "Temporary sales/margin friction.", "Yes", "Temporary", "Needs to fade after implementation period."),
            ("Marketing", "Can depress near-term margin but support brand.", "Yes", "Recurring/strategic", "Quality depends on sales response."),
        ]:
            _add("Quality of Earnings", item, display=impact, cash_flag=cash, recurring_flag=recurring, quality_read=read)

        _add("Key Debate", "Key debate", display="Can ANF sustain high margins and EPS after the turnaround, despite tougher comps, tariffs, ERP disruption and margin normalization?", source="model thesis", source_note="Analyst framing from ANF operating drivers, guidance and valuation bridge.")
        _add("What Needs To Happen", "Sales guide must hold", display="2026 sales growth needs to stay inside or above +3-5%.", source="model thesis", source_note="Guidance beat/miss setup.")
        _add("What Needs To Happen", "Margins must stabilize", display="Operating margin needs to hold near 12.0-12.5% after tariff, ERP and marketing headwinds.", source="model thesis", source_note="Tariff / margin bridge.")
        _add("What Needs To Happen", "Inventory must stay clean", display="Inventory growth should remain explainable by tariffs and ERP prebuild, not markdown-heavy excess product.", source="model thesis", source_note="Inventory / markdown risk section.")
        _add("What Needs To Happen", "Buybacks must support EPS", display="Share repurchases need to offset slower growth without overusing net cash.", source="model thesis", source_note="Capital allocation bridge.")
        _add("What Needs To Happen", "Market must believe durability", display="The multiple depends on investors believing margins are structurally higher, not just a peak-turnaround result.", source="model thesis", source_note="Valuation sensitivity.")
        _add("Tariff / Margin Bridge", "Q1 2026 tariff headwind", q1_tar_bps, "bps", f"~{q1_tar_bps:.0f} bps / ~${q1_tar_cost:.0f}m", q1_display=f"~{q1_tar_bps:.0f} bps / ~${q1_tar_cost:.0f}m", year_display="", source_note="Q4 2025 earnings outlook: Q1 tariff pressure.")
        _add("Tariff / Margin Bridge", "2026 tariff headwind", fy_tar_bps, "bps", f"~{fy_tar_bps:.0f} bps / ~${fy_tar_cost:.0f}m incremental", q1_display="", year_display=f"~{fy_tar_bps:.0f} bps / ~${fy_tar_cost:.0f}m incremental", source_note="Q4 2025 earnings outlook: full-year incremental tariff pressure.")
        _add("Tariff / Margin Bridge", "Freight tailwind", freight_bps, "bps", f"~{freight_bps:.0f} bps", q1_display=f"~{freight_bps:.0f} bps", year_display="partial annual offset", source_note="Q4 2025 earnings outlook: Q1 freight tailwind.")
        _add("Tariff / Margin Bridge", "ERP disruption", erp_bps, "bps", f">{erp_bps:.0f} bps op margin headwind", q1_display=f">{erp_bps:.0f} bps op margin headwind", year_display="temporary", source_note="Q4 2025 outlook commentary on ERP go-live disruption.")
        _add("Tariff / Margin Bridge", "Marketing", marketing_bps, "bps", f"+{marketing_bps:.0f} bps headwind Q1", q1_display=f"+{marketing_bps:.0f} bps headwind Q1", year_display="strategic spend", source_note="Q4 2025 earnings outlook: Q1 marketing spend.")
        _add("Tariff / Margin Bridge", "AUR / pricing", display="offset / mitigation", q1_display="offset / mitigation", year_display="partial mitigation", source_note="Source commentary references AUR/selective pricing as mitigation.")
        _add("Tariff / Margin Bridge", "Sourcing / supplier mitigation", display="offset", q1_display="offset", year_display="partial mitigation", source_note="Source commentary references sourcing and supplier negotiations as mitigation.")
        _add("Tariff / Margin Bridge", "Reported 2025 operating margin", reported_op_margin, "%", "13.3%")
        _add("Tariff / Margin Bridge", "2026 guide operating margin", None, "%", f"{fy2026_op_margin_low:.1f}-{fy2026_op_margin_high:.1f}%")
        _add("Tariff / Margin Bridge", "Implied decline", None, "bps", f"{min(abs(implied_low_bps), abs(implied_high_bps)) * -1} to {max(abs(implied_low_bps), abs(implied_high_bps)) * -1} bps")
        _add("Tariff / Margin Bridge", "Bridge read", display="2026 margin guide implies an 80-130 bps decline; tariffs, ERP and marketing are partly offset by freight, AUR/pricing and sourcing.", source="model thesis", source_note="Source-backed bridge read.")

        _add("EPS Bridge", "2025 adjusted EPS", adj_eps_2025, "$/share", _fmt_eps(adj_eps_2025))
        _add("EPS Bridge", "Sales growth", None, "%", _guidance_display("Revenue", "2026 year", "+3-5%"))
        _add("EPS Bridge", "Margin / tariff / freight / AUR", None, "bridge", "tariffs + ERP + marketing, partly offset by freight/AUR/pricing/sourcing")
        _add("EPS Bridge", "SG&A leverage / deleverage", None, "bridge", "depends on sales growth and marketing investment")
        _add("EPS Bridge", "Buyback / share count reduction", None, "bridge", "~45m guided diluted shares vs 48.5m in 2025")
        _add("EPS Bridge", "2026 guided EPS", None, "$/share", f"${eps_2026_low:.2f}-${eps_2026_high:.2f}")
        actual_buybacks_m = _drv_value("share_repurchases", 450.0) or 450.0
        shares_repurchased_m = _drv_value("shares_repurchased", 5.4) or 5.4
        buyback_vs_fcf = actual_buybacks_m - fcf_m
        _add("Buybacks vs FCF", "FCF TTM", fcf_m, "$m", f"${fcf_m:,.1f}m", source="Valuation_Summary", source_note="FCF TTM from CFO minus capex.")
        _add("Buybacks vs FCF", "Buybacks", actual_buybacks_m, "$m", f"~${actual_buybacks_m:,.0f}m", source="Operating_Drivers / cash flow", source_note="2025 year / TTM actual share repurchases.")
        _add("Buybacks vs FCF", "2025 buybacks", actual_buybacks_m, "$m", f"~${actual_buybacks_m:,.0f}m", source="Operating_Drivers / cash flow", source_note="Alias row for audit: 2025 year / TTM actual share repurchases.")
        _add("Buybacks vs FCF", "Shares repurchased", shares_repurchased_m, "m shares", f"{shares_repurchased_m:.1f}m", source="Operating_Drivers / cash flow", source_note="2025 year / TTM shares repurchased.")
        _add("Buybacks vs FCF", "Buyback spread vs FCF", buyback_vs_fcf, "$m", f"${buyback_vs_fcf:,.1f}m above FCF", source="model bridge", source_note="Positive means buybacks exceeded FCF and used some net cash.")
        _add("Buybacks vs FCF", "Net cash context", net_cash_incl_sec_m, "$m", f"${net_cash_incl_sec_m:,.1f}m net cash incl. securities", source="Valuation_Summary", source_note="Balance sheet still materially net cash after buybacks.")
        _add("Buybacks vs FCF", "Investment read", display="Watch: buybacks exceeded FCF but net cash remains strong; good EPS support if repurchases stay disciplined.", source="model thesis", source_note="Capital return should be watched, not treated as an automatic fail.")

        rev_growth_low = _num(_guidance_row("Revenue", "2026 year").get("low"))
        rev_growth_high = _num(_guidance_row("Revenue", "2026 year").get("high"))
        rev_growth_low = 3.0 if rev_growth_low is None else rev_growth_low
        rev_growth_high = 5.0 if rev_growth_high is None else rev_growth_high
        rev_growth_low_pct = rev_growth_low / 100.0 if abs(rev_growth_low) > 1.5 else rev_growth_low
        rev_growth_high_pct = rev_growth_high / 100.0 if abs(rev_growth_high) > 1.5 else rev_growth_high
        implied_revenue_low = annual_revenue_m * (1.0 + rev_growth_low_pct)
        implied_revenue_high = annual_revenue_m * (1.0 + rev_growth_high_pct)
        implied_ebit_low = implied_revenue_low * (fy2026_op_margin_low / 100.0)
        implied_ebit_high = implied_revenue_high * (fy2026_op_margin_high / 100.0)
        net_interest_other_m = 0.0
        implied_eps_low = (implied_ebit_low + net_interest_other_m) * (1.0 - tax_rate) / shares_2026
        implied_eps_high = (implied_ebit_high + net_interest_other_m) * (1.0 - tax_rate) / shares_2026
        guide_overlap = not (implied_eps_high < eps_2026_low or implied_eps_low > eps_2026_high)
        guide_check_note = (
            "Model bridge ties roughly to company guide; small gaps reflect tax/interest/rounding and conservatism."
            if guide_overlap or min(abs(implied_eps_low - eps_2026_low), abs(implied_eps_high - eps_2026_high)) <= 0.35
            else "Model bridge differs from company guide; review below-operating-income assumptions."
        )
        guide_check = "Roughly in line with guide" if "ties roughly" in guide_check_note else "Review assumptions"
        guide_section = "2026 Guide → Implied Earnings"
        _add(guide_section, "2025 revenue", annual_revenue_m, "$m", f"${annual_revenue_m:,.1f}m", source="History_Q / annual schedules", source_note="FY2025 net sales used as the revenue base.")
        _add(guide_section, "2026 revenue growth guide", None, "%", _guidance_display("Revenue", "2026 year", "+3-5%"), source="Guidance_Normalized", source_note="2026 year net sales growth outlook.")
        _add(guide_section, "Implied 2026 revenue", None, "$m", f"${implied_revenue_low:,.1f}-${implied_revenue_high:,.1f}m", value_low=implied_revenue_low, value_high=implied_revenue_high, source="model bridge", source_note="2025 revenue multiplied by low/high sales-growth guide.")
        _add(guide_section, "Operating margin guide", None, "%", f"{fy2026_op_margin_low:.1f}-{fy2026_op_margin_high:.1f}%", value_low=fy2026_op_margin_low, value_high=fy2026_op_margin_high, source="Guidance_Normalized", source_note="2026 year operating margin guide.")
        _add(guide_section, "Implied EBIT", None, "$m", f"${implied_ebit_low:,.1f}-${implied_ebit_high:,.1f}m", value_low=implied_ebit_low, value_high=implied_ebit_high, source="model bridge", source_note="Implied revenue multiplied by operating-margin guide.")
        _add(guide_section, "Tax / interest assumptions", tax_rate, "%", f"{tax_rate * 100:.1f}% tax / ${net_interest_other_m:.0f}m net interest", source=tax_rate_source, source_note="Bridge uses no net interest/other income so the implied EPS remains comparable to company guide.")
        _add(guide_section, "Diluted shares guide", shares_2026, "m shares", f"{shares_2026:.1f}m", source="Guidance_Normalized", source_note="2026 year diluted share guide.")
        _add(guide_section, "Implied EPS low/high", None, "$/share", f"${implied_eps_low:.2f}-${implied_eps_high:.2f}", value_low=implied_eps_low, value_high=implied_eps_high, source="model bridge", source_note="Implied EBIT after tax divided by guided diluted shares.")
        _add(guide_section, "Company EPS guide", None, "$/share", f"${eps_2026_low:.2f}-${eps_2026_high:.2f}", value_low=eps_2026_low, value_high=eps_2026_high, source="Guidance_Normalized", source_note="Company 2026 year EPS guide.")
        _add(guide_section, "Model vs guide check", None, "check", guide_check, source="model bridge", source_note=guide_check_note)

        op_margin_eps = annual_revenue_m * 0.01 * (1.0 - tax_rate) / shares_2026
        sales_growth_eps = annual_revenue_m * 0.01 * (fy2026_op_margin_mid / 100.0) * (1.0 - tax_rate) / shares_2026
        buyback_shares = 100.0 / avg_buyback_price if avg_buyback_price else 0.0
        buyback_eps = eps_2026_mid * (buyback_shares / max(shares_2026 - buyback_shares, 1.0))
        _add("What Moves EPS", "+100 bps operating margin", op_margin_eps, "$/share", f"+${op_margin_eps:.2f} EPS")
        _add("What Moves EPS", "+100 bps gross margin", op_margin_eps, "$/share", f"+${op_margin_eps:.2f} EPS before SG&A leakage")
        _add("What Moves EPS", "+1% sales growth", sales_growth_eps, "$/share", f"+${sales_growth_eps:.2f} EPS")
        _add("What Moves EPS", "$100m buybacks", buyback_eps, "$/share", f"+${buyback_eps:.2f} EPS at ${avg_buyback_price:.2f}/share")
        _add("What Moves EPS", "Roughly +$1 EPS equals", None, "sensitivity", f"~{(1.0 / op_margin_eps) * 100:.0f} bps op margin or ~${(1.0 / max(buyback_eps, 0.01)) * 100:.0f}m buybacks")

        for eps in (9.50, 10.50, 11.50):
            _add("Valuation Sensitivity", f"EPS ${eps:.2f}", eps, "$/share", f"${eps:.2f}", pe_10=round(eps * 10), pe_12=round(eps * 12), pe_14=round(eps * 14), pe_16=round(eps * 16))
        for scenario, eps, multiple in (("Bear", 9.50, 10), ("Base", eps_2026_mid, 13), ("Bull", 12.00, 16)):
            _add("Valuation Sensitivity", f"{scenario} scenario", eps, "$/share", f"${eps * multiple:.0f}", scenario=scenario, eps=eps, multiple=multiple, share_price=round(eps * multiple))

        for multiple in (6.0, 8.0, 10.0):
            ev = adj_ebitda_m * multiple
            equity_core = ev + net_cash_incl_sec_m
            equity_lease = ev - lease_adj_net_debt_m
            _add(
                "Adj EBITDA x EV/EBITDA",
                f"{multiple:.1f}x EV/EBITDA",
                ev,
                "$m",
                f"${ev:,.0f}m EV",
                source="Valuation_Summary / model sensitivity",
                source_note="Uses adjusted EBITDA TTM; equity value adds core net cash or subtracts lease-adjusted net debt.",
                equity_value_core_net_cash=equity_core,
                equity_value_lease_adjusted=equity_lease,
                share_price_core_net_cash=equity_core / shares_2026,
                share_price_lease_adjusted=equity_lease / shares_2026,
            )
        for fcf_yield in (0.04, 0.05, 0.06):
            equity_value = fcf_m / fcf_yield
            _add(
                "FCF Yield Implied Equity Value",
                f"{fcf_yield * 100:.1f}% FCF yield",
                equity_value,
                "$m",
                f"${equity_value:,.0f}m equity value",
                source="Valuation_Summary / model sensitivity",
                source_note="Uses FCF TTM and guided diluted shares; scenario-based when market price is unavailable.",
                share_price=equity_value / shares_2026,
            )

        # Comp stack: latest eight quarters with same-quarter prior-year comp when available.
        if not seg_df.empty and {"quarter", "segment", "metric", "value"}.issubset(set(seg_df.columns)):
            comp_df = seg_df[seg_df["metric"].astype(str).str.lower().eq("comparable_sales")].copy()
            if not comp_df.empty:
                comp_df["quarter_dt"] = pd.to_datetime(comp_df["quarter"], errors="coerce")
                comp_df = comp_df[pd.notna(comp_df["quarter_dt"])]
                quarters = sorted({pd.Timestamp(x).date() for x in comp_df["quarter_dt"].dropna()})[-8:]
                for qd in quarters:
                    label = _anf_visible_quarter_label(qd)
                    prior_label = re.sub(r"(20\d{2})", lambda m: str(int(m.group(1)) - 1), label, count=1)
                    total_comp = _seg_value("Total Company", "comparable_sales", "quarter", qd)
                    prior_total_comp = _seg_value_by_visible_label("Total Company", "comparable_sales", "quarter", prior_label)
                    stack = (_pct_num(total_comp) or 0.0) + (_pct_num(prior_total_comp) or 0.0) if total_comp is not None and prior_total_comp is not None else None
                    _add(
                        "Comp Stack / Lapping Risk",
                        f"{label} 2-year stack",
                        stack,
                        "%",
                        _fmt_pct(stack, decimals=0) if stack is not None else "",
                        quarter_label=label,
                        total_comp=_fmt_pct(total_comp, decimals=0),
                        two_year_stack=_fmt_pct(stack, decimals=0) if stack is not None else "",
                        abercrombie_comp=_fmt_pct(_seg_value("Abercrombie", "comparable_sales", "quarter", qd), decimals=0),
                        hollister_comp=_fmt_pct(_seg_value("Hollister", "comparable_sales", "quarter", qd), decimals=0),
                        americas=_fmt_pct(_seg_value("Americas", "comparable_sales", "quarter", qd), decimals=0),
                        emea=_fmt_pct(_seg_value("EMEA", "comparable_sales", "quarter", qd), decimals=0),
                        apac=_fmt_pct(_seg_value("APAC", "comparable_sales", "quarter", qd), decimals=0),
                        short_read=(
                            "Strong two-year stack; watch lapping risk."
                            if stack is not None and stack >= 25
                            else "Lower comp growth, but compares remain tough."
                            if stack is not None and stack >= 15
                            else "Needs better traffic/brand momentum."
                        ),
                    )

        ab_q4_comp = _seg_value("Abercrombie", "comparable_sales", "quarter", date(2026, 1, 31), -0.01)
        ho_q4_comp = _seg_value("Hollister", "comparable_sales", "quarter", date(2026, 1, 31), 0.03)
        _add("Brand Health", "Abercrombie 2025 sales", ab_annual_sales, "$", f"~${(ab_annual_sales or 0) / 1e9:.2f}bn")
        _add("Brand Health", "Hollister 2025 sales", ho_annual_sales, "$", f"~${(ho_annual_sales or 0) / 1e9:.2f}bn")
        _add("Brand Health", "2025 sales", display=f"Abercrombie ~${(ab_annual_sales or 0) / 1e9:.2f}bn / Hollister ~${(ho_annual_sales or 0) / 1e9:.2f}bn", source="ANF parsed sources", source_note="Audit alias for brand-family 2025 sales.")
        _add("Brand Health", "Abercrombie 2025 sales growth", _seg_value("Abercrombie", "net_sales_growth", "annual", default=-0.01), "%", _fmt_pct(_seg_value("Abercrombie", "net_sales_growth", "annual", default=-0.01), decimals=0))
        _add("Brand Health", "Hollister 2025 sales growth", _seg_value("Hollister", "net_sales_growth", "annual", default=0.15), "%", _fmt_pct(_seg_value("Hollister", "net_sales_growth", "annual", default=0.15), decimals=0))
        _add("Brand Health", "Abercrombie Q4 sales growth", _seg_value("Abercrombie", "net_sales_growth", "quarter", date(2026, 1, 31), 0.04), "%", _fmt_pct(_seg_value("Abercrombie", "net_sales_growth", "quarter", date(2026, 1, 31), 0.04), decimals=0))
        _add("Brand Health", "Hollister Q4 sales growth", _seg_value("Hollister", "net_sales_growth", "quarter", date(2026, 1, 31), 0.06), "%", _fmt_pct(_seg_value("Hollister", "net_sales_growth", "quarter", date(2026, 1, 31), 0.06), decimals=0))
        _add("Brand Health", "Abercrombie Q4 comp", ab_q4_comp, "%", _fmt_pct(ab_q4_comp, decimals=0))
        _add("Brand Health", "Hollister Q4 comp", ho_q4_comp, "%", _fmt_pct(ho_q4_comp, decimals=0))
        _add("Brand Health", "Interpretation", display="Hollister is currently the main growth engine; Abercrombie slowed in 2025 but returned to growth in Q4.")

        for label, category_type, revenue_value in [
            ("Abercrombie (brand)", "Brand", ab_annual_sales),
            ("Hollister (brand)", "Brand", ho_annual_sales),
            ("Americas (geography / stores)", "Geography / stores", americas_annual_sales),
            ("EMEA (geography / stores)", "Geography / stores", emea_annual_sales),
            ("APAC (geography / stores)", "Geography / stores", apac_annual_sales),
        ]:
            revenue_m = (float(revenue_value) / 1_000_000.0) if revenue_value is not None else None
            _add(
                "Segment Scenario Inputs",
                label,
                revenue_m,
                "$m",
                f"${revenue_m:,.1f}m" if revenue_m is not None else "",
                source="Slides_Segments / annual report" if revenue_m is not None else "model-derived",
                source_note=(
                    "Summed if Brand selected"
                    if category_type == "Brand"
                    else "Summed if Geography selected"
                    if category_type == "Geography / stores"
                    else "Informational only"
                )
                if revenue_m is not None
                else "Missing segment revenue",
                segment_type=category_type,
                revenue_basis="2025 year net sales" if revenue_m is not None else "",
                margin_conversion="",
                margin_basis="",
                feeds_bridge="No",
            )

        inv_cost = _drv_value("inventory_cost_growth", 0.05)
        inv_tariff = _drv_value("inventory_cost_tariff_points", 3.0)
        inv_units = _drv_value("inventory_unit_growth", 0.05)
        inv_erp = _drv_value("inventory_unit_growth_erp_points", 3.0)
        inv_ex = _drv_value("inventory_unit_growth_ex_erp", 0.02)
        _add("Inventory / Markdown Risk", "Inventory growth", inv_cost, "%", _fmt_pct(inv_cost, decimals=0))
        _add("Inventory / Markdown Risk", "Sales growth", 0.06, "%", "+5% Q4 / +6% year")
        _add("Inventory / Markdown Risk", "Inventory cost tariff component", inv_tariff, "pts", f"~{inv_tariff:.0f} pts")
        _add("Inventory / Markdown Risk", "Inventory unit growth", inv_units, "%", _fmt_pct(inv_units, decimals=0))
        _add("Inventory / Markdown Risk", "ERP prebuild component", inv_erp, "pts", f"~{inv_erp:.0f} pts")
        _add("Inventory / Markdown Risk", "Ex-ERP unit growth", inv_ex, "%", f"~{_pct_num(inv_ex):.0f}%")
        _add("Inventory / Markdown Risk", "Conclusion", display="Inventory growth does not look obviously dangerous because tariffs and ERP prebuild explain much of it; markdown risk still belongs in gross margin watch-items.")

        stores_start = _drv_value("store_count_beginning", 789.0)
        stores_end = _drv_value("store_count_end", 829.0)
        openings = _drv_value("new_stores", 62.0)
        closures = _drv_value("closed_stores", 22.0)
        franchise = _drv_value("franchise_stores", 60.0)
        total_stores = _drv_value("total_stores_including_franchise", (stores_end or 829.0) + (franchise or 60.0))
        store_growth_pct = ((stores_end or 0.0) - (stores_start or 0.0)) / (stores_start or 1.0) * 100.0
        sales_per_store = annual_revenue_m / (stores_end or 829.0)
        _add("Store Productivity / Real Estate ROI", "Company-owned stores", stores_end, "stores", f"{stores_end:.0f}")
        _add("Store Productivity / Real Estate ROI", "Franchise stores", franchise, "stores", f"{franchise:.0f}")
        _add("Store Productivity / Real Estate ROI", "Total incl franchise", total_stores, "stores", f"{total_stores:.0f}")
        _add("Store Productivity / Real Estate ROI", "2025 openings", openings, "stores", f"{openings:.0f}")
        _add("Store Productivity / Real Estate ROI", "2025 closures", closures, "stores", f"{closures:.0f}")
        _add("Store Productivity / Real Estate ROI", "2026 openings", 55.0, "stores", "55")
        _add("Store Productivity / Real Estate ROI", "2026 closures", 25.0, "stores", "25")
        _add("Store Productivity / Real Estate ROI", "2026 remodels/right-sizes", 70.0, "stores", "70")
        _add(
            "Store Productivity / Real Estate ROI",
            "Sales per owned store",
            sales_per_store,
            "$m/store",
            f"${sales_per_store:.1f}m",
            source_note="Proxy uses 2025 revenue divided by year-end owned stores; average store count is not available.",
        )
        _add("Store Productivity / Real Estate ROI", "Store growth", store_growth_pct, "%", f"+{store_growth_pct:.1f}%")
        _add("Store Productivity / Real Estate ROI", "Revenue growth vs store growth", None, "spread", f"+6.0% sales vs +{store_growth_pct:.1f}% store growth")
        _add("Store Productivity / Real Estate ROI", "Digital mix", _drv_value("digital_sales_mix", 44.0), "%", "44%")
        _add("Digital / omnichannel", "Digital sales mix", _drv_value("digital_sales_mix", 44.0), "%", "44%", source="ANF annual report / operating drivers", source_note="Audit alias for FY2025 digital sales mix.")
        _add(
            "Store Productivity / Real Estate ROI",
            "Method note",
            display="Sales/store is a proxy using year-end owned stores unless average store count is available.",
            source_note="Keeps store productivity visible without implying a true average-store calculation.",
        )

        _add("Guidance Beat/Miss Setup", "Sales growth", None, "", _guidance_display("Revenue", "2026 year", "+3-5%"), current_trend="comps slowing but Hollister strong", beat_miss_risk="neutral/slightly positive")
        _add("Guidance Beat/Miss Setup", "Op margin", None, "", _guidance_display("Operating margin", "2026 year", "12.0-12.5%"), current_trend="tariffs/ERP headwind", beat_miss_risk="depends mitigation")
        _add("Guidance Beat/Miss Setup", "EPS", None, "", _guidance_display("Adj EPS", "2026 year", "$10.20-$11.00"), current_trend="buybacks support", beat_miss_risk="possible if margin holds")
        _add("Guidance Beat/Miss Setup", "Buybacks", None, "", _guidance_display("Share repurchases", "2026 year", "~$450m"), current_trend="balance sheet strong", beat_miss_risk="likely")
        _add("Guidance Beat/Miss Setup", "Capex", None, "", _guidance_display("Capex", "2026 year", "$200-$225m"), current_trend="below 2025", beat_miss_risk="supports FCF")

        # Keep core defaults auditable even when source frames are sparse.
        _add("Assumptions", "2025 revenue", annual_revenue_m, "$m", f"${annual_revenue_m:,.1f}m", source="brand-family annual sales")
        _add("Assumptions", "2025 GAAP EPS", gaap_eps_2025, "$/share", _fmt_eps(gaap_eps_2025))
        _add("Assumptions", "2025 adjusted EPS", adj_eps_2025, "$/share", _fmt_eps(adj_eps_2025))
        _add("Assumptions", "2026 diluted shares", shares_2026, "m shares", f"{shares_2026:.1f}m", source="2026 guidance")
        _add("Assumptions", "Tax rate", tax_rate, "%", f"{tax_rate * 100:.0f}%", source="fallback", source_note="Used for sensitivity math; source materials generally reference mid-20s.")
        _add("Assumptions", "Buyback price", avg_buyback_price, "$/share", f"${avg_buyback_price:.2f}", source="2025 actual buyback average")
        return pd.DataFrame(rows)


    def build_sector_investment_case_data(
        self,
        *,
        ticker: Any,
        hist: Any,
        guidance_normalized: Any = None,
        operating_driver_rows: Sequence[Dict[str, Any]] = (),
        valuation_summary: Any = None,
        economics_market_rows: Sequence[Dict[str, Any]] = (),
        slides_segments: Any = None,
    ) -> pd.DataFrame:
        _anf_clean_visible_ui_text = self._rt("_anf_clean_visible_ui_text")
        _guidance_source_contract_label = self._rt("_guidance_source_contract_label")
        _segment_scenario_revenue_m = self._rt("_segment_scenario_revenue_m")
        _shared_visible_period_text = self._rt("_shared_visible_period_text")
        glx_normalize_text = self._rt("glx_normalize_text")
        """Build a compact sector-specific investment-case audit table for non-ANF tickers."""
        ticker_txt = str(ticker or "").strip().upper()
        hist_df = hist.copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame()
        if not hist_df.empty and "quarter" in hist_df.columns:
            hist_df["quarter"] = pd.to_datetime(hist_df["quarter"], errors="coerce")
            hist_df = hist_df[hist_df["quarter"].notna()].sort_values("quarter").reset_index(drop=True)
        guidance_df = guidance_normalized.copy() if isinstance(guidance_normalized, pd.DataFrame) else pd.DataFrame()
        drivers = [dict(r) for r in (operating_driver_rows or [])]
        econ_rows = [dict(r) for r in (economics_market_rows or [])]
        seg_df = slides_segments.copy() if isinstance(slides_segments, pd.DataFrame) else pd.DataFrame()
        rows: List[Dict[str, Any]] = []

        def _num(x: Any) -> Optional[float]:
            val = pd.to_numeric(x, errors="coerce")
            return float(val) if pd.notna(val) else None

        def _latest(col: str) -> Optional[float]:
            if hist_df.empty or col not in hist_df.columns:
                return None
            return _num(hist_df.iloc[-1].get(col))

        def _ttm(col: str) -> Optional[float]:
            if hist_df.empty or col not in hist_df.columns or len(hist_df) < 4:
                return None
            vals = pd.to_numeric(hist_df.tail(4)[col], errors="coerce")
            return float(vals.sum()) if not vals.isna().all() else None

        def _first_ttm(cols: Sequence[str]) -> Optional[float]:
            for col in cols:
                val = _ttm(col)
                if val is not None:
                    return val
            return None

        def _missing_note(label: str) -> str:
            return f"Needs source-backed {label} to calculate this section."

        def _m(val: Optional[float]) -> Optional[float]:
            return None if val is None or pd.isna(val) else float(val) / 1_000_000.0

        def _fmt_money_m(val: Optional[float], decimals: int = 1) -> str:
            if val is None or pd.isna(val):
                return ""
            return f"${float(val):,.{decimals}f}m"

        def _fmt_pct(val: Optional[float], decimals: int = 1) -> str:
            if val is None or pd.isna(val):
                return ""
            raw = float(val)
            pct = raw * 100.0 if abs(raw) <= 1.5 else raw
            sign = "+" if pct > 0 else ""
            return f"{sign}{pct:.{decimals}f}%"

        def _segment_ttm_revenue_m(segment_aliases: Sequence[str]) -> Tuple[Optional[float], str]:
            if seg_df.empty or not {"quarter", "segment", "metric", "value"}.issubset(set(seg_df.columns)):
                return None, ""
            tmp = seg_df.copy()
            tmp["_q_sort"] = pd.to_datetime(tmp.get("quarter"), errors="coerce")
            tmp["_value"] = pd.to_numeric(tmp.get("value"), errors="coerce")
            tmp = tmp[tmp["_q_sort"].notna() & tmp["_value"].notna()].copy()
            if tmp.empty:
                return None, ""
            aliases = [str(alias or "").strip().lower() for alias in segment_aliases if str(alias or "").strip()]
            seg_ser = tmp["segment"].astype(str).str.strip().str.lower()
            metric_ser = tmp["metric"].astype(str).str.strip().str.lower()
            mask = metric_ser.str.contains("revenue", regex=False, na=False)
            if aliases:
                mask &= seg_ser.apply(lambda txt: any(alias in txt for alias in aliases))
            tmp = tmp[mask].copy()
            if tmp.empty:
                return None, ""
            tmp["_unit"] = tmp.get("unit", pd.Series([""] * len(tmp), index=tmp.index)).astype(str).str.strip().str.lower()
            tmp["_value_m"] = tmp.apply(
                lambda rec: _segment_scenario_revenue_m(rec.get("_value"), rec.get("_unit")),
                axis=1,
            )
            tmp = tmp[pd.to_numeric(tmp["_value_m"], errors="coerce").notna()].copy()
            tmp = tmp[(tmp["_value_m"].abs() >= 10.0) & (tmp["_value_m"].abs() <= 10000.0)].copy()
            if tmp.empty:
                return None, ""
            tmp["_q_date"] = tmp["_q_sort"].dt.date
            latest_by_q = (
                tmp.sort_values(["_q_sort", "_value_m"], kind="stable")
                .groupby("_q_date", as_index=False)
                .tail(1)
                .sort_values("_q_sort")
            )
            if len(latest_by_q) >= 4:
                return float(latest_by_q.tail(4)["_value_m"].sum()), "Slides_Segments TTM revenue"
            latest = latest_by_q.iloc[-1]
            return float(latest.get("_value_m")), "Slides_Segments latest revenue"

        def _guidance_snip(*needles: str, max_chars: int = 96) -> str:
            if guidance_df.empty:
                return ""
            search_cols = [
                c
                for c in (
                    "metric",
                    "target_display",
                    "qualitative_range_text",
                    "source_note",
                    "exact_language",
                    "text",
                    "horizon_label",
                )
                if c in guidance_df.columns
            ]
            if not search_cols:
                return ""
            needle_lows = [str(n or "").strip().lower() for n in needles if str(n or "").strip()]
            for _, grow in guidance_df.iterrows():
                blob = " | ".join(str(grow.get(c) or "") for c in search_cols)
                blob_low = glx_normalize_text(blob).lower()
                if needle_lows and not all(n in blob_low for n in needle_lows):
                    continue
                display_bits = [
                    str(grow.get(c) or "").strip()
                    for c in ("target_display", "qualitative_range_text", "source_note", "exact_language", "text")
                    if c in guidance_df.columns and str(grow.get(c) or "").strip()
                ]
                if not display_bits:
                    continue
                return _shared_visible_period_text(_anf_clean_visible_ui_text(display_bits[0], max_chars=max_chars))
            return ""

        def _add(
            section: str,
            metric: str,
            display: Any = "",
            *,
            value: Optional[float] = None,
            unit: str = "",
            source: str = "model-derived",
            source_note: str = "",
            investment_read: str = "",
            current_trend: str = "",
            beat_miss_risk: str = "",
            **extra: Any,
        ) -> None:
            row = {
                "section": section,
                "metric": metric,
                "value": value,
                "unit": unit,
                "display": "" if display is None else str(display),
                "source": source,
                "source_note": source_note,
                "investment_read": investment_read,
                "current_trend": current_trend,
                "beat_miss_risk": beat_miss_risk,
            }
            row.update(extra)
            rows.append(row)

        latest_revenue_m = _m(_latest("revenue"))
        revenue_ttm_m = _m(_ttm("revenue"))
        ebitda_ttm_m = _m(_ttm("ebitda"))
        op_income_ttm_m = _m(_ttm("op_income"))
        net_income_ttm_m = _m(_ttm("net_income"))
        cfo_ttm_m = _m(_ttm("cfo"))
        capex_ttm_m = _m(_ttm("capex"))
        fcf_ttm_m = (cfo_ttm_m - capex_ttm_m) if cfo_ttm_m is not None and capex_ttm_m is not None else None
        cash_m = _m(_latest("cash"))
        debt_m = _m(_latest("debt_core") if _latest("debt_core") is not None else _latest("total_debt"))
        net_debt_m = (debt_m - cash_m) if debt_m is not None and cash_m is not None else None
        shares_m = _m(_latest("shares_diluted") if _latest("shares_diluted") is not None else _latest("shares_outstanding"))
        eps_base = None
        if shares_m and net_income_ttm_m is not None and shares_m > 0:
            eps_base = net_income_ttm_m / shares_m
        pretax_ttm = _first_ttm(["pretax_income", "income_before_taxes", "income_before_income_taxes"])
        tax_expense_ttm = _first_ttm(["income_tax_expense", "tax_expense", "provision_for_income_taxes"])
        tax_rate = None
        if (
            pretax_ttm is not None
            and tax_expense_ttm is not None
            and float(pretax_ttm) > 0
            and float(tax_expense_ttm) >= 0
        ):
            candidate_tax_rate = float(tax_expense_ttm) / float(pretax_ttm)
            if 0.0 <= candidate_tax_rate <= 0.35:
                tax_rate = candidate_tax_rate
        if tax_rate is not None:
            _add(
                "Assumptions",
                "Tax rate",
                value=tax_rate,
                unit="%",
                display=f"{tax_rate * 100:.1f}%",
                source="History_Q",
                source_note="TTM tax expense divided by positive pretax income; capped to 0-35% for scenario EPS conversion.",
            )

        def _clean_margin_candidate(value: Any) -> Optional[float]:
            val = pd.to_numeric(value, errors="coerce")
            if pd.isna(val):
                return None
            margin = float(val)
            if abs(margin) > 1.5:
                margin /= 100.0
            return margin if math.isfinite(margin) and -0.5 <= margin <= 0.5 else None

        def _company_operating_margin_proxy_for_case() -> Tuple[Optional[float], str]:
            for col in ("operating_margin", "operating_margin_pct", "op_margin", "ebit_margin"):
                candidate = _clean_margin_candidate(_latest(col))
                if candidate is not None:
                    basis = "Company operating margin proxy" if "operating" in col or "op_" in col else "EBIT margin proxy"
                    return candidate, basis
            revenue_ttm_raw = _ttm("revenue")
            if revenue_ttm_raw is None or abs(float(revenue_ttm_raw)) <= 1e-9:
                return None, ""
            numerator_candidates = [
                (("op_income", "operating_income", "operating_profit"), "Company operating margin proxy"),
                (("ebit",), "EBIT margin proxy"),
                (("adjusted_ebit", "adj_ebit", "adjusted_operating_income"), "Adjusted operating margin proxy"),
                (("adjusted_ebitda", "adj_ebitda"), "Adjusted EBITDA margin proxy"),
            ]
            for cols, basis in numerator_candidates:
                numerator = _first_ttm(cols)
                if numerator is None:
                    continue
                candidate = _clean_margin_candidate(float(numerator) / float(revenue_ttm_raw))
                if candidate is not None:
                    return candidate, basis
            return None, ""

        company_operating_margin_proxy, company_operating_margin_basis = _company_operating_margin_proxy_for_case()

        def _add_eps_pe_sensitivity(section: str = "Valuation Sensitivity") -> None:
            if eps_base is None or not math.isfinite(float(eps_base)):
                _add(section, "Missing EPS base", _missing_note("TTM net income and diluted share denominator"), source="History_Q")
                return
            base = float(eps_base)
            if abs(base) < 0.01:
                eps_points = [0.25, 0.50, 0.75]
            elif base > 0:
                eps_points = [max(0.01, base * 0.75), base, base * 1.25]
            else:
                eps_points = [base * 1.25, base, base * 0.75]
            scenario_rows = [("Bear", eps_points[0], 10), ("Base", eps_points[1], 13), ("Bull", eps_points[2], 16)]
            for eps in eps_points:
                _add(
                    section,
                    f"EPS ${eps:.2f}",
                    f"${eps:.2f}",
                    value=eps,
                    unit="$/share",
                    source="History_Q / model-derived",
                    source_note="EPS base from TTM net income divided by latest diluted shares; scenarios are sensitivity cases.",
                    pe_10=round(eps * 10),
                    pe_12=round(eps * 12),
                    pe_14=round(eps * 14),
                    pe_16=round(eps * 16),
                )
            for scenario, eps, multiple in scenario_rows:
                _add(
                    section,
                    f"{scenario} scenario",
                    f"${eps * multiple:.0f}",
                    value=eps,
                    unit="$/share",
                    source="model-derived",
                    source_note="EPS/P/E sensitivity table; not a price target.",
                    scenario=scenario,
                    eps=eps,
                    multiple=multiple,
                    share_price=round(eps * multiple),
                )

        if ticker_txt == "PBI":
            _add("Investment Snapshot", "Model read", "Turnaround/FCF case with debt and execution sensitivity.", investment_read="PBI needs durable adjusted EBIT/FCF improvement, not just one good quarter.")
            _add("Investment Snapshot", "Why it can work", "FCF conversion, cost actions, Presort execution and debt reduction can rebuild equity confidence.", source="Valuation / Operating_Drivers")
            _add("Investment Snapshot", "Key debate", "Can PBI sustain adjusted EBIT and FCF improvement while reducing/refinancing debt and stabilizing Presort / SendTech?", source="model-derived")
            _add("Investment Snapshot", "What would improve the case", "Adjusted EBIT holds, FCF converts, leverage falls, Presort margins stabilize and SendTech decline is controlled.")
            _add("Investment Snapshot", "What would break the case", "EBIT/FCF rollover, refinancing pressure, segment deterioration or savings not flowing through.")
            _add("Investment Snapshot", "Watch next", "Adjusted EBIT, FCF, debt maturities/refinancing, Presort volume/margin and SendTech churn.")
            _add("Investment Snapshot", "Current stance", "Use as a turnaround watchlist: equity value improves if FCF durability lowers refinancing risk.", source="model-derived")
            _add(
                "Key Debates",
                "FCF durability vs debt/refinancing",
                "Can FCF stay strong enough to reduce/refinance debt?",
                bull_evidence=f"FCF TTM {_fmt_money_m(fcf_ttm_m)} and savings target tracked in Promise/Valuation.",
                bear_evidence="Debt/refinancing and interest burden still shape downside.",
                next_proof_point="Next CFO, capex, FCF and maturity/refinancing update.",
                current_read="Constructive only if cash conversion remains durable.",
            )
            _add(
                "Key Debates",
                "Segment stabilization",
                "Can Presort stabilize while SendTech decline is controlled?",
                bull_evidence="Presort / SendTech reads are visible in Operating_Drivers.",
                bear_evidence="Recurring EBIT quality weakens if segment trends roll over.",
                next_proof_point="Segment revenue, margin and customer trend updates.",
                current_read="Mixed; segment proof still matters.",
            )
            for scenario, assumptions, fcf_factor, yield_txt, read in [
                ("Bear", "FCF slips and refinancing risk stays elevated.", 0.75, "11% FCF yield", "Downside if cash conversion fades."),
                ("Base", "FCF holds and debt risk gradually falls.", 1.00, "9% FCF yield", "Turnaround works if FCF is repeatable."),
                ("Bull", "FCF improves and debt paydown/refinancing confidence rises.", 1.25, "7% FCF yield", "Upside requires durable FCF and lower debt risk."),
            ]:
                if fcf_ttm_m is not None and shares_m:
                    implied = (fcf_ttm_m * fcf_factor) / (float(yield_txt.split("%", 1)[0]) / 100.0) / shares_m
                    implied_txt = f"${implied:,.2f}"
                else:
                    implied_txt = _missing_note("FCF and share denominator")
                _add(
                    "Bear / Base / Bull Scenario",
                    scenario,
                    assumptions,
                    earnings_metric=f"FCF {_fmt_money_m(fcf_ttm_m * fcf_factor) if fcf_ttm_m is not None else _missing_note('FCF')}",
                    multiple_yield=yield_txt,
                    implied_value_share=implied_txt,
                    scenario_read=read,
                )
            _add(
                "What Market Is Pricing",
                "Missing market price",
                "Market price/current EV unavailable; retain this section for implied metric once price is available.",
                source="Valuation / market data",
            )
            for item, impact, cash, recurring, read in [
                ("Restructuring / cost savings", "Can lift adjusted EBIT if realized.", "Partly", "Programmatic", "Track savings flow-through, not just target language."),
                ("FCF conversion", "Funds debt paydown and refinancing confidence.", "Yes", "Needs proof", "Most important quality marker for PBI."),
                ("Interest / refinancing", "Can absorb operating progress.", "Yes", "Recurring until refinanced", "Higher interest keeps equity sensitive to debt terms."),
                ("Segment EBIT quality", "Determines durability of turnaround earnings.", "Mostly", "Recurring if segments stabilize", "Presort/SendTech proof is required."),
            ]:
                _add("Quality of Earnings", item, impact, cash_flag=cash, recurring_flag=recurring, quality_read=read)

            for metric, read in [
                ("Adjusted EBIT must hold/improve", "Durability matters more than one-quarter improvement."),
                ("FCF conversion must remain strong", "Debt reduction and equity value depend on cash, not only EBITDA."),
                ("Debt/refinancing risk must fall", "Leverage and maturities drive downside risk."),
                ("Presort volumes / margins must stabilize", "Core operating engine needs stable throughput and margin."),
                ("SendTech decline must be controlled", "Decline rate shapes total revenue durability."),
                ("Cost savings must flow through to EBIT", "Savings credibility should show in adjusted EBIT."),
                ("Market must believe earnings are durable", "Multiple expansion requires confidence in the turnaround."),
            ]:
                _add("What needs to happen for the stock to work", metric, read, source="model-derived")

            pbi_savings_txt = (
                _guidance_snip("cost", "saving")
                or "$180m-$200m annualized cost savings target; latest run-rate is tracked in Promise_Progress_UI."
            )
            _add("Turnaround / EBIT Bridge", "Base adjusted EBIT / EBITDA", f"EBITDA TTM {_fmt_money_m(ebitda_ttm_m)}; revenue TTM {_fmt_money_m(revenue_ttm_m)}.", value=ebitda_ttm_m, unit="$m", source="History_Q / Adjusted_Metrics", source_note="Uses latest available TTM EBITDA/adjusted operating base if available.")
            _add("Turnaround / EBIT Bridge", "Cost savings target", pbi_savings_txt, source="Guidance_Normalized / management guidance", source_note="Also visible in Promise_Progress_UI and the Valuation side-panel.")
            _add("Turnaround / EBIT Bridge", "Segment margin improvement", "Presort and SendTech margin path drives adjusted EBIT.", source="Operating_Drivers")
            _add("Turnaround / EBIT Bridge", "Revenue stabilization", _fmt_money_m(revenue_ttm_m), value=revenue_ttm_m, unit="$m", source="History_Q", source_note="Revenue TTM.")
            _add("Turnaround / EBIT Bridge", "SG&A / productivity", "Productivity should offset volume pressure.", source="Operating_Drivers")
            _add("Turnaround / EBIT Bridge", "Interest burden / refinancing", "Interest cost and maturity timing affect FCF after debt service.", source="Debt_Profile / Revolver_History")

            _add("FCF / Debt Paydown Bridge", "CFO", _fmt_money_m(cfo_ttm_m), value=cfo_ttm_m, unit="$m", source="History_Q")
            _add("FCF / Debt Paydown Bridge", "Capex", _fmt_money_m(capex_ttm_m), value=capex_ttm_m, unit="$m", source="History_Q")
            _add("FCF / Debt Paydown Bridge", "FCF", _fmt_money_m(fcf_ttm_m), value=fcf_ttm_m, unit="$m", source="History_Q", source_note="CFO less capex.")
            _add("FCF / Debt Paydown Bridge", "Interest paid", _fmt_money_m(_m(_ttm("interest_paid"))), value=_m(_ttm("interest_paid")), unit="$m", source="History_Q")
            _add("FCF / Debt Paydown Bridge", "Debt paydown", "Use FCF after reinvestment and interest to reduce/refinance debt.", source="model-derived")
            _add("FCF / Debt Paydown Bridge", "Leverage / net debt", _fmt_money_m(net_debt_m), value=net_debt_m, unit="$m", source="History_Q")
            _add("FCF / Debt Paydown Bridge", "Maturity/refinancing watch", "Debt ladder and revolver terms remain core diligence items.", source="Debt_Maturity_Ladder / Debt_Profile")

            for seg, read in [
                ("Presort Services", "Watch volumes, margin and customer throughput."),
                ("SendTech Solutions", "Watch revenue decline, churn and cash generation."),
                ("Other / corporate", "Keep corporate cost and residual items from masking segment trend."),
            ]:
                _add("Segment Health", seg, read, source="SUMMARY / Operating_Drivers", investment_read="Segment trend must support the turnaround thesis.")

            for label, aliases in [
                ("Presort", ("presort services", "presort")),
                ("SendTech", ("sendtech solutions", "sendtech")),
            ]:
                revenue_m, revenue_basis = _segment_ttm_revenue_m(aliases)
                margin_proxy = company_operating_margin_proxy if revenue_m is not None else None
                _add(
                    "Segment Scenario Inputs",
                    label,
                    _fmt_money_m(revenue_m),
                    value=revenue_m,
                    unit="$m",
                    source="Slides_Segments" if revenue_m is not None else "model-derived",
                    source_note="Company operating margin proxy" if margin_proxy is not None else ("Missing segment margin" if revenue_m is not None else "Missing segment revenue"),
                    segment_type="Segment / business line",
                    revenue_basis=revenue_basis,
                    margin_conversion=margin_proxy if margin_proxy is not None else "",
                    margin_basis=company_operating_margin_basis if margin_proxy is not None else "",
                    feeds_bridge="Yes" if margin_proxy is not None else "No",
                )

            _add("Capital Structure / Refinancing Risk", "Debt core", _fmt_money_m(debt_m), value=debt_m, unit="$m", source="History_Q")
            _add("Capital Structure / Refinancing Risk", "Cash", _fmt_money_m(cash_m), value=cash_m, unit="$m", source="History_Q")
            _add("Capital Structure / Refinancing Risk", "Net debt", _fmt_money_m(net_debt_m), value=net_debt_m, unit="$m", source="History_Q")
            _add("Capital Structure / Refinancing Risk", "Revolver/liquidity", "Use Revolver_History and Leverage_Liquidity for availability/draw detail.", source="Revolver_History")
            _add("Capital Structure / Refinancing Risk", "Leverage", "Debt load makes FCF durability more important.", source="Valuation / Leverage_Liquidity")
            _add("Capital Structure / Refinancing Risk", "Covenant/refinancing notes", "Review debt notes for maturity/refinancing constraints.", source="Debt_Credit_Notes")

            _add("Guidance Beat/Miss Setup", "Current guidance", "Use latest curated guidance in Promise_Progress_UI and Valuation side-panel.", source=_guidance_source_contract_label(ticker_txt))
            _add("Guidance Beat/Miss Setup", "Latest actual", f"Revenue latest quarter {_fmt_money_m(latest_revenue_m)}; EBITDA TTM {_fmt_money_m(ebitda_ttm_m)}.", source="History_Q")
            _add("Guidance Beat/Miss Setup", "Beat/miss risk", "Execution risk is mainly adjusted EBIT, FCF conversion, segment trend and refinancing confidence.", source="model-derived")

            _add_eps_pe_sensitivity()
            buybacks_ttm_m = _m(_first_ttm(("buybacks_cash", "buybacks_ttm_cash", "share_repurchases", "repurchases", "share_repurchases_cash")))
            if buybacks_ttm_m is not None and fcf_ttm_m is not None:
                _add("Buybacks vs FCF", "FCF TTM", _fmt_money_m(fcf_ttm_m), value=fcf_ttm_m, unit="$m", source="History_Q")
                _add("Buybacks vs FCF", "Buybacks", _fmt_money_m(buybacks_ttm_m), value=buybacks_ttm_m, unit="$m", source="History_Q / Operating_Drivers")
                _add(
                    "Buybacks vs FCF",
                    "Investment read",
                    f"Buybacks were {'above' if buybacks_ttm_m > fcf_ttm_m else 'within'} FCF; watch capital returns against refinancing needs.",
                    source="model bridge",
                )
            else:
                _add("Buybacks vs FCF", "Missing data", _missing_note("cash repurchase and share-repurchase authorization data"), source="model-derived")
            _add("Current Guide -> Implied Earnings", "Revenue guide", _guidance_snip("revenue") or _missing_note("current revenue guide"), source="Guidance_Normalized")
            _add("Current Guide -> Implied Earnings", "Adjusted EBIT / EBITDA guide", _guidance_snip("ebit") or _fmt_money_m(ebitda_ttm_m), source="Guidance_Normalized / History_Q")
            _add("Current Guide -> Implied Earnings", "EPS guide", _guidance_snip("eps") or _missing_note("current EPS guide"), source="Guidance_Normalized")
            _add("Current Guide -> Implied Earnings", "FCF guide", _guidance_snip("fcf") or _fmt_money_m(fcf_ttm_m), source="Guidance_Normalized / History_Q")
            eps_ttm_m = _latest("eps_ttm") or _latest("adj_eps_ttm") or _latest("eps") or eps_base
            _add("What Moves EPS", "Adjusted EBIT durability", "EPS works if adjusted EBIT holds and savings convert.", source="model-derived")
            _add("What Moves EPS", "Interest / refinancing", "Lower refinancing risk can preserve more EBIT for equity holders.", source="Debt_Profile")
            _add("What Moves EPS", "Share count / buybacks", _missing_note("buyback share count") if buybacks_ttm_m is None else "Buybacks can support EPS but compete with debt paydown.", source="Operating_Drivers")
            _add("What Moves EPS", "Current EPS base", f"${float(eps_ttm_m):.2f}" if eps_ttm_m is not None else _missing_note("EPS base"), source="History_Q")
            for multiple in (5.0, 6.0, 7.0):
                if ebitda_ttm_m is not None and net_debt_m is not None and shares_m:
                    ev = ebitda_ttm_m * multiple
                    equity = ev - net_debt_m
                    share_px = equity / shares_m
                    display = f"{multiple:.1f}x -> EV {_fmt_money_m(ev)}; equity {_fmt_money_m(equity)}; share ${share_px:,.2f}"
                else:
                    display = _missing_note("EBITDA, net debt and share denominator")
                _add("Adj EBITDA x EV/EBITDA", f"{multiple:.1f}x", display, source="Valuation / History_Q")
            for fcf_yield in (0.07, 0.09, 0.11):
                if fcf_ttm_m is not None and fcf_yield > 0 and shares_m:
                    equity = fcf_ttm_m / fcf_yield
                    display = f"{fcf_yield * 100:.1f}% yield -> equity {_fmt_money_m(equity)}; share ${equity / shares_m:,.2f}"
                else:
                    display = _missing_note("FCF and share denominator")
                _add("FCF Yield Implied Equity Value", f"{fcf_yield * 100:.1f}% FCF yield", display, source="Valuation / History_Q")
            _add("Segment Trend / Lapping Risk", "Presort", "Track volume, pricing and margin against prior-year comparisons.", source="Operating_Drivers")
            _add("Segment Trend / Lapping Risk", "SendTech", "Stabilization needs slower decline and cash generation.", source="Operating_Drivers")
        elif ticker_txt == "GTX":
            adj_ebit_latest_m = _m(_latest("adjusted_ebit") if _latest("adjusted_ebit") is not None else _latest("adj_ebit"))
            adj_ebitda_latest_m = _m(_latest("adjusted_ebitda") if _latest("adjusted_ebitda") is not None else _latest("adj_ebitda"))
            adj_fcf_latest_m = _m(_latest("adjusted_fcf") if _latest("adjusted_fcf") is not None else _latest("adj_fcf"))
            restricted_cash_m = _m(_latest("restricted_cash"))
            if adj_ebit_latest_m is None:
                adj_ebit_latest_m = 151.0
            if adj_ebitda_latest_m is None:
                adj_ebitda_latest_m = 183.0
            if adj_fcf_latest_m is None:
                adj_fcf_latest_m = 49.0
            if restricted_cash_m is None:
                restricted_cash_m = 2.0
            q1_release_source = "Q1 2026 earnings release"
            tenk_source = "2025 Form 10-K sales concentration table"
            tenq_source = "Q1 2026 Form 10-Q sales concentration table"
            event_source = "May 18 2026 press release"

            _add(
                "Investment Snapshot",
                "Model read",
                "Turbocharging and electrification supplier: underwriting hinges on OEM production, product/geography mix, adjusted EBIT durability and FCF conversion.",
                investment_read="Use GTX as an explicit-only model until longer driver history and scenario assumptions are curated.",
                source="SUMMARY / History_Q / Operating_Drivers",
            )
            _add(
                "Investment Snapshot",
                "Why it can work",
                "Global turbo scale, customer integration, commercial/industrial wins, disciplined capex and cash conversion can support earnings and capital returns.",
                source="2025 Form 10-K / Q1 2026 earnings release",
            )
            _add(
                "Investment Snapshot",
                "Key debate",
                "Can GTX convert mature turbo-platform demand and newer electrification/industrial awards into durable adjusted EBIT and free cash flow?",
                source="model-derived",
            )
            _add(
                "Investment Snapshot",
                "What would improve the case",
                "Vehicle production holds, commercial/industrial and aftermarket mix stay firm, adjusted EBIT guide is achieved, FCF converts and leverage falls.",
                source="model-derived",
            )
            _add(
                "Investment Snapshot",
                "What would break the case",
                "Major OEM demand weakens, pricing/productivity miss cost pressure, technology awards do not scale, or FCF is absorbed by working capital, capex or debt service.",
                source="model-derived",
            )
            _add(
                "Investment Snapshot",
                "Watch next",
                "Product-line sales mix, Europe/China demand, customer concentration, adjusted EBIT, adjusted FCF, buybacks and post-quarter debt actions.",
                source="Operating_Drivers",
            )
            _add(
                "Investment Snapshot",
                "Current stance",
                "Source-backed first-pass thesis sheet; no manual price target or recommendation is implied.",
                source="model-derived",
            )

            for metric, display, read, source in [
                (
                    "Turbo / ICE-hybrid content durability",
                    "Gas and Diesel were $2.43bn of FY2025 sales; commercial vehicle and industrial was $654m.",
                    "Bull case needs platform content to remain relevant through hybrid and efficiency cycles.",
                    tenk_source,
                ),
                (
                    "Commercial vehicle / industrial awards",
                    "Q1 2026 release cited off-highway, industrial, power-generation and light-commercial diesel wins.",
                    "Provides a potential offset to light-vehicle production softness.",
                    q1_release_source,
                ),
                (
                    "FCF conversion vs adjusted EBIT",
                    f"Latest quarter adjusted EBIT {_fmt_money_m(adj_ebit_latest_m)}; adjusted FCF {_fmt_money_m(adj_fcf_latest_m)}.",
                    "Quality depends on converting non-GAAP operating profit into cash after capex and working capital.",
                    "History_Q / Q1 2026 earnings release",
                ),
                (
                    "Leverage and capital returns",
                    f"Latest debt {_fmt_money_m(debt_m)}; unrestricted cash {_fmt_money_m(cash_m)}; buybacks remain a capital-allocation variable.",
                    "High leverage makes interest cost, debt repayment and buybacks central to per-share value.",
                    "History_Q / Q1 2026 earnings release",
                ),
                (
                    "Customer/geography concentration",
                    "2025 sales: Stellantis 12%, BMW 11%, Ford 11%; Europe 49%, China 18% of sales.",
                    "Program loss, pricing pressure or regional demand weakness can move the thesis.",
                    tenk_source,
                ),
            ]:
                _add(
                    "Key Debates",
                    metric,
                    display,
                    source=source,
                    current_read=read,
                    next_proof_point="Next quarterly source package and management outlook update.",
                )

            for scenario, assumptions, ebit_factor, multiple_txt, read in [
                ("Bear", "Vehicle production/customer mix weakens and adjusted EBIT guide is missed.", 0.85, "7.0x EV/Adj EBITDA", "Downside if cash conversion and leverage progress disappoint."),
                ("Base", "Adjusted EBIT/FCF track guidance and buybacks stay within cash generation.", 1.00, "8.0x EV/Adj EBITDA", "Base case is execution against current outlook, not multiple expansion by itself."),
                ("Bull", "Commercial/industrial, aftermarket and technology awards lift mix while FCF funds leverage reduction and buybacks.", 1.15, "9.0x EV/Adj EBITDA", "Upside requires durable adjusted EBIT and clean FCF conversion."),
            ]:
                if ebitda_ttm_m is not None and net_debt_m is not None and shares_m:
                    multiple = float(multiple_txt.split("x", 1)[0])
                    implied = ((ebitda_ttm_m * ebit_factor) * multiple - net_debt_m) / shares_m
                    implied_txt = f"${implied:,.2f}"
                else:
                    implied_txt = _missing_note("Adjusted EBITDA, net debt and share denominator")
                _add(
                    "Bear / Base / Bull Scenario",
                    scenario,
                    assumptions,
                    earnings_metric=f"Adj EBITDA proxy {_fmt_money_m(ebitda_ttm_m * ebit_factor) if ebitda_ttm_m is not None else _missing_note('EBITDA')}",
                    multiple_yield=multiple_txt,
                    implied_value_share=implied_txt,
                    scenario_read=read,
                    source="model-derived / History_Q",
                )

            for item, impact, cash, recurring, read in [
                ("Adjusted EBIT", "Primary non-GAAP operating metric used to evaluate execution.", "Partly", "Needs proof", "Track margin dollars and margin %, not only management-adjusted labels."),
                ("Adjusted EBITDA bridge", "Useful leverage bridge but secondary to adjusted EBIT for operating read.", "No", "Bridge item", "Do not let EBITDA obscure RD&E, capex or interest burden."),
                ("Adjusted FCF", "Company-defined cash metric differs from GAAP CFO less capex.", "Yes", "Needs proof", "Compare adjusted FCF with GAAP FCF and working-capital/factoring effects."),
                ("Capex discipline", "Q1 capex was $29m; capex affects FCF and technology pipeline capacity.", "Yes", "Recurring", "Low capex supports FCF but should not starve future awards."),
                ("Interest / debt", "Debt and refinancing terms affect cash available for equity.", "Yes", "Recurring until repaid/refinanced", "Unrestricted cash, not restricted cash or undrawn revolver, offsets net debt."),
                ("Buybacks", "Capital returns can help per-share value but compete with debt reduction.", "Yes", "Discretionary", "Watch buybacks against leverage and FCF conversion."),
            ]:
                _add("Quality of Earnings", item, impact, cash_flag=cash, recurring_flag=recurring, quality_read=read)

            for metric, display, source_note in [
                ("Gas", "$1,592m / 45% of FY2025 sales", "2025 product-line mix"),
                ("Diesel", "$837m / 23% of FY2025 sales", "2025 product-line mix"),
                ("Commercial Vehicle", "$654m / 18% of FY2025 sales", "2025 product-line mix"),
                ("Aftermarket", "$438m / 12% of FY2025 sales", "2025 product-line mix"),
                ("Europe", "$1,745m / 49% of FY2025 sales", "2025 geography mix"),
                ("China", "$638m / 18% of FY2025 sales", "2025 geography mix"),
                ("United States", "$694m / 19% of FY2025 sales", "2025 geography mix"),
                ("Stellantis / BMW / Ford", "12% / 11% / 11% of FY2025 sales", "2025 customer concentration"),
            ]:
                _add(
                    "Product / Geography / Customer Cuts",
                    metric,
                    display,
                    source=tenk_source,
                    source_note=source_note,
                    investment_read="Analytical operating cut; not a reportable segment profit line.",
                )

            for metric, read in [
                ("OEM production / end-market demand", "Light-vehicle and commercial-vehicle production assumptions drive near-term revenue bridge."),
                ("Product mix / turbo demand", "Gas, diesel and CV/industrial mix determine margin and durability."),
                ("Aftermarket", "Aftermarket demand can be more replacement-driven than OEM launch cycles."),
                ("China / Europe exposure", "Regional production, FX and China demand are key sensitivities."),
                ("Customer concentration", "Major OEM platform wins/losses can matter more than aggregate market growth."),
                ("RD&E / technology awards", "E-Powertrain, E-Cooling and industrial awards are the long-run transition proof points."),
                ("Adjusted EBIT / adjusted FCF conversion", "The model should distinguish GAAP FCF from company-defined adjusted FCF."),
                ("Debt, net leverage and buybacks", "Capital allocation depends on FCF after interest, debt repayment and cash needs."),
            ]:
                _add("Operating Driver Watchlist", metric, read, source="Operating_Drivers / official GTX source package")

            _add("Current Guide -> Implied Earnings", "Revenue guide", _guidance_snip("net sales") or "$3.6bn-$3.9bn FY2026 net sales outlook.", source=q1_release_source)
            _add("Current Guide -> Implied Earnings", "Adjusted EBIT guide", _guidance_snip("adjusted ebit") or "$520m-$600m FY2026 adjusted EBIT outlook.", source=q1_release_source)
            _add("Current Guide -> Implied Earnings", "Adjusted FCF guide", _guidance_snip("adjusted free cash flow") or "$355m-$475m FY2026 adjusted free cash flow outlook.", source=q1_release_source)
            _add("Current Guide -> Implied Earnings", "Latest actual", f"Revenue latest quarter {_fmt_money_m(latest_revenue_m)}; adjusted EBIT {_fmt_money_m(adj_ebit_latest_m)}; adjusted EBITDA {_fmt_money_m(adj_ebitda_latest_m)}; adjusted FCF {_fmt_money_m(adj_fcf_latest_m)}.", source="History_Q / Q1 2026 earnings release")

            _add("Capital Structure / Cash", "Unrestricted cash", _fmt_money_m(cash_m), value=cash_m, unit="$m", source="History_Q", source_note="Net debt should use unrestricted cash only.")
            _add("Capital Structure / Cash", "Restricted cash", _fmt_money_m(restricted_cash_m), value=restricted_cash_m, unit="$m", source="History_Q", source_note="Shown separately; not counted as unrestricted cash.")
            _add("Capital Structure / Cash", "Debt", _fmt_money_m(debt_m), value=debt_m, unit="$m", source="History_Q / Debt_Tranches_Latest")
            _add("Capital Structure / Cash", "Net debt", _fmt_money_m(net_debt_m), value=net_debt_m, unit="$m", source="History_Q", source_note="Debt less unrestricted cash.")
            _add("Capital Structure / Cash", "May 18 2026 term loan event", "$50m early repayment and repricing of existing term loan due 2032.", value=50.0, unit="$m", source=event_source, source_note="Post-quarter / pro-forma context only; Q1 reported history unchanged.")

            for metric, read in [
                ("Vehicle production must not roll over", "Revenue guide becomes harder if end-market production weakens below assumptions."),
                ("Adjusted EBIT guide must be credible", "Margin quality matters more than revenue growth alone."),
                ("Adjusted FCF must reconcile to GAAP cash flow", "Cash conversion is the main quality-of-earnings proof."),
                ("Customer programs must remain sticky", "Concentration makes platform losses or pricing pressure material."),
                ("Technology awards must scale", "Electrification/industrial awards should become revenue, not just pipeline language."),
                ("Debt and buybacks must stay balanced", "Capital returns should not undermine leverage reduction or liquidity."),
            ]:
                _add("What needs to happen for the stock to work", metric, read, source="model-derived / source-backed watchlist")

            _add_eps_pe_sensitivity()
        elif ticker_txt == "GPRE":
            _add("Investment Snapshot", "Model read", "Commodity/policy upside case with margin durability risk.", investment_read="GPRE needs sustainable margin and policy support, not just temporary commodity strength.")
            _add("Investment Snapshot", "Why it can work", "Ethanol demand, 45Z/policy support, coproduct economics and disciplined capex can lift EBITDA/FCF.", source="Economics_Overlay / Operating_Drivers")
            _add("Investment Snapshot", "Key debate", "Can higher ethanol demand, 45Z/policy support and stronger crush margins make earnings durable enough for a higher equity multiple?", source="model-derived")
            _add("Investment Snapshot", "What would improve the case", "Crush margins improve, 45Z becomes monetizable, exports/E15 help demand and balance sheet stays disciplined.")
            _add("Investment Snapshot", "What would break the case", "Margins normalize lower, policy support disappoints, coproduct/protein economics weaken or capex absorbs FCF.")
            _add("Investment Snapshot", "Watch next", "Crush margin, ethanol demand, 45Z implementation, RVO/SRE/RIN policy, exports, capex and liquidity.")
            _add("Investment Snapshot", "Current stance", "Use as a commodity/policy watchlist: upside needs 45Z and crush margin durability to convert into FCF.", source="model-derived")
            _add(
                "Key Debates",
                "45Z monetization and policy durability",
                "Can 45Z/policy support become cash EBITDA rather than temporary narrative?",
                bull_evidence="45Z guide and Q1 contribution are visible in Promise/Valuation.",
                bear_evidence="Policy timing, qualification and monetization can shift.",
                next_proof_point="45Z receipts/contribution, remaining facilities and policy updates.",
                current_read="Positive but still execution/policy dependent.",
            )
            _add(
                "Key Debates",
                "Crush margin durability",
                "Can stronger margin/gallons persist enough for a higher multiple?",
                bull_evidence="Economics_Overlay frames crush and coproduct sensitivity.",
                bear_evidence="Margins are cyclical and can normalize quickly.",
                next_proof_point="Ethanol/corn/input spread, gallons/utilization and coproduct pricing.",
                current_read="Watch; cycle proof matters.",
            )
            for scenario, assumptions, ebitda_factor, multiple_txt, read in [
                ("Bear", "Crush margin normalizes and 45Z cash conversion disappoints.", 0.75, "5.0x EV/EBITDA", "Downside if policy and margin both fade."),
                ("Base", "Current EBITDA base plus visible policy/capex discipline.", 1.00, "6.0x EV/EBITDA", "Base case needs 45Z and FCF conversion."),
                ("Bull", "Crush/gallons improve and 45Z proves monetizable.", 1.25, "7.0x EV/EBITDA", "Upside if policy benefit is durable."),
            ]:
                if ebitda_ttm_m is not None and net_debt_m is not None and shares_m:
                    multiple = float(multiple_txt.split("x", 1)[0])
                    implied = ((ebitda_ttm_m * ebitda_factor) * multiple - net_debt_m) / shares_m
                    implied_txt = f"${implied:,.2f}"
                else:
                    implied_txt = _missing_note("EBITDA, net debt and share denominator")
                _add(
                    "Bear / Base / Bull Scenario",
                    scenario,
                    assumptions,
                    earnings_metric=f"Adj EBITDA {_fmt_money_m(ebitda_ttm_m * ebitda_factor) if ebitda_ttm_m is not None else _missing_note('EBITDA')}",
                    multiple_yield=multiple_txt,
                    implied_value_share=implied_txt,
                    scenario_read=read,
                )
            _add(
                "What Market Is Pricing",
                "Missing market price",
                "Market price/current EV unavailable; retain this section for implied metric once price is available.",
                source="Valuation / market data",
            )
            for item, impact, cash, recurring, read in [
                ("45Z / policy credits", "Can materially lift EBITDA/FCF.", "Potentially", "Policy-dependent", "Do not underwrite as recurring until cash conversion is proven."),
                ("Crush margin", "Primary cyclical earnings driver.", "Yes", "Cyclical", "Separate mid-cycle margin from spot strength."),
                ("Derivatives / hedges", "Can affect reported timing and quality.", "Timing-sensitive", "Not core recurring", "Keep hedge/OCI diagnostics separate but visible."),
                ("Capex", "Can absorb EBITDA upside.", "Yes", "Program-dependent", "FCF quality depends on capex discipline."),
                ("Coproduct/protein", "Can support margin quality.", "Yes", "Cycle/market dependent", "Useful only if sustained in realized spreads."),
            ]:
                _add("Quality of Earnings", item, impact, cash_flag=cash, recurring_flag=recurring, quality_read=read)

            for metric, read in [
                ("Crush margins must hold/improve", "Core EBITDA sensitivity sits in ethanol/corn/input spread."),
                ("Exports / E15 demand must improve", "Demand support reduces margin cyclicality."),
                ("45Z must be monetizable", "Policy upside needs cash conversion."),
                ("RVO/SRE/RIN policy support", "Policy can change demand and margin expectations quickly."),
                ("Coproduct / carbon economics help", "Coproduct credits can bridge reported margins."),
                ("Capex and balance sheet disciplined", "Commodity cycles punish overinvestment."),
                ("Market must believe durability", "Multiple depends on margins not being purely temporary."),
            ]:
                _add("What needs to happen for the stock to work", metric, read, source="model-derived")

            _add("Ethanol / Crush Margin Bridge", "Ethanol price", "Economics_Overlay frames ethanol realization versus feedstock and coproduct offsets.", source="Economics_Overlay")
            _add("Ethanol / Crush Margin Bridge", "Corn price", "Corn is the main feedstock cost; the spread matters more than either price alone.", source="Economics_Overlay")
            _add("Ethanol / Crush Margin Bridge", "Natural gas / inputs", "Input costs pressure process margin and FCF when ethanol spread is thin.", source="Economics_Overlay")
            _add("Ethanol / Crush Margin Bridge", "Coproduct credits", "Coproduct/protein/carbon credits can offset weak ethanol spread.", source="Economics_Overlay")
            _add("Ethanol / Crush Margin Bridge", "Crush margin proxy", "Economics_Overlay translates ethanol/corn/coproduct spreads into EBITDA sensitivity.", source="Economics_Overlay")
            _add("Ethanol / Crush Margin Bridge", "Sensitivity to margin per gallon", "+$0.05/gal margin sensitivity belongs in What Moves EBITDA.", source="model-derived")
            _add("Ethanol / Crush Margin Bridge", "Gallons / production volume", "Utilization and gallons translate margin into EBITDA dollars.", source="Operating_Drivers / Economics_Overlay")

            gpre_45z_txt = (
                _guidance_snip("45z")
                or "FY2026 45Z EBITDA guidance is $200m-$225m, led by Advantage Nebraska $140m-$165m and remaining facilities about $60m."
            )
            _add("Policy / 45Z / RFS Bridge", "45Z expected benefit", gpre_45z_txt, source="Guidance_Normalized / management guidance")
            _add("Policy / 45Z / RFS Bridge", "Q1 2026 latest contribution", "$55.2m latest quarterly contribution shown as partial tracking, not annual completion.", source="Promise_Progress_UI / Slides_Guidance")
            _add("Policy / 45Z / RFS Bridge", "Advantage Nebraska contribution", "$140m-$165m FY2026 contribution guide.", source="Valuation side-panel / management guidance")
            _add("Policy / 45Z / RFS Bridge", "Remaining facilities contribution", "About $60m of FY2026 45Z contribution guide.", source="Valuation side-panel / management guidance")
            _add("Policy / 45Z / RFS Bridge", "Farm-practice upside timing", "Excluded from current base; final guidance expected in 2026.", source="Valuation side-panel / management guidance")
            _add("Policy / 45Z / RFS Bridge", "45Q / CO2", "Include only where source-backed; keep carbon upside separate from core crush.", source="SEC filing / operating drivers")
            _add("Policy / 45Z / RFS Bridge", "RVO / implied conventional", "RFS demand support affects ethanol margin setup.", source="Economics_Overlay")
            _add("Policy / 45Z / RFS Bridge", "E15 / export demand", "Demand channel that can tighten balances.", source="press release / industry data")
            _add("Policy / 45Z / RFS Bridge", "RIN / SRE reallocation watch", "Policy uncertainty is a key risk/reward variable.", source="Economics_Overlay")
            _add("Policy / 45Z / RFS Bridge", "Policy status and dates", "Track effective dates and implementation details in raw sources.", source="Guidance_Normalized / raw sources")

            _add("What Moves EBITDA", "+$0.05/gal margin sensitivity", "Bridge per-gallon margin to EBITDA using gallons / production base.", source="model-derived")
            _add("What Moves EBITDA", "Utilization / gallons", "Higher gallons amplify margin move but require demand and operations.", source="Operating_Drivers")
            _add("What Moves EBITDA", "45Z per gallon / annual impact", "Policy upside should be translated into annual EBITDA contribution.", source="management guidance")
            _add("What Moves EBITDA", "Coproduct / protein uplift", "Protein/coproduct spread can materially change EBITDA bridge.", source="Economics_Overlay")
            _add("What Moves EBITDA", "Capex / opex", "Capex and operating cost discipline determine FCF conversion.", source="History_Q")
            _add("What Moves EBITDA", "SG&A / fixed cost leverage", "Fixed cost leverage can help or hurt through the cycle.", source="model-derived")

            _add("FCF / Balance Sheet", "EBITDA", _fmt_money_m(ebitda_ttm_m), value=ebitda_ttm_m, unit="$m", source="History_Q")
            _add("FCF / Balance Sheet", "CFO", _fmt_money_m(cfo_ttm_m), value=cfo_ttm_m, unit="$m", source="History_Q")
            gpre_capex_guide = _guidance_snip("capex") or "2026 capex guidance is $15m-$25m."
            _add("FCF / Balance Sheet", "Capex", gpre_capex_guide, value=capex_ttm_m, unit="$m", source="History_Q / Guidance_Normalized")
            _add("FCF / Balance Sheet", "FCF", _fmt_money_m(fcf_ttm_m), value=fcf_ttm_m, unit="$m", source="History_Q", source_note="CFO less capex.")
            _add("FCF / Balance Sheet", "Debt / cash", f"Debt {_fmt_money_m(debt_m)}; cash {_fmt_money_m(cash_m)}.", value=net_debt_m, unit="$m", source="History_Q")
            _add("FCF / Balance Sheet", "Liquidity", "Use Leverage_Liquidity and Revolver_History for facility and cash detail.", source="Leverage_Liquidity")
            _add("FCF / Balance Sheet", "Capex commitments", "Capex discipline matters because commodity margins are cyclical.", source="History_Q / guidance")

            _add("Guidance Beat/Miss Setup", "Current guidance", "Use latest curated guidance in Promise_Progress_UI and Valuation side-panel.", source=_guidance_source_contract_label(ticker_txt))
            _add("Guidance Beat/Miss Setup", "Key assumptions", "Crush margin, policy/45Z, gallons, coproduct pricing and capex.", source="Economics_Overlay")
            _add("Guidance Beat/Miss Setup", "Latest actual", f"Revenue latest quarter {_fmt_money_m(latest_revenue_m)}; EBITDA TTM {_fmt_money_m(ebitda_ttm_m)}.", source="History_Q")
            _add("Guidance Beat/Miss Setup", "Beat/miss risk", "Biggest swing factors are crush margin, 45Z/policy, gallons and coproduct economics.", source="model-derived")

            _add_eps_pe_sensitivity()
            _add("Buybacks vs FCF", "Capital return read", "No material buyback program modeled; FCF should fund capex and liquidity first.", source="model-derived")
            _add("Current Guide -> Implied Earnings", "45Z / policy guide", gpre_45z_txt, source="Guidance_Normalized")
            _add("Current Guide -> Implied Earnings", "Capex guide", _guidance_snip("capex") or "2026 capex guidance is $15m-$25m.", source="Guidance_Normalized")
            _add("Current Guide -> Implied Earnings", "EBITDA base", _fmt_money_m(ebitda_ttm_m), source="History_Q")
            _add("Current Guide -> Implied Earnings", "Implied earnings read", "Earnings bridge needs crush margin, gallons, 45Z cash conversion and capex discipline.", source="model-derived")
            _add("What Moves EPS", "Crush margin", "EPS bridge needs margin/gal, gallons, tax and share count.", source="Economics_Overlay")
            _add("What Moves EPS", "45Z / policy", "Policy contribution can lift EBITDA/EPS if monetizable and not offset by capex or working capital.", source="Guidance_Normalized")
            _add("What Moves EPS", "Missing EPS bridge inputs", _missing_note("normalized tax rate, share count and clean EPS guide"), source="model-derived")
            for multiple in (5.0, 6.0, 7.0):
                if ebitda_ttm_m is not None and net_debt_m is not None and shares_m:
                    ev = ebitda_ttm_m * multiple
                    equity = ev - net_debt_m
                    share_px = equity / shares_m
                    display = f"{multiple:.1f}x -> EV {_fmt_money_m(ev)}; equity {_fmt_money_m(equity)}; share ${share_px:,.2f}"
                else:
                    display = _missing_note("EBITDA, net debt and share denominator")
                _add("Adj EBITDA x EV/EBITDA", f"{multiple:.1f}x", display, source="Valuation / History_Q")
            for fcf_yield in (0.07, 0.09, 0.11):
                if fcf_ttm_m is not None and fcf_yield > 0 and shares_m:
                    equity = fcf_ttm_m / fcf_yield
                    display = f"{fcf_yield * 100:.1f}% yield -> equity {_fmt_money_m(equity)}; share ${equity / shares_m:,.2f}"
                else:
                    display = _missing_note("FCF and share denominator")
                _add("FCF Yield Implied Equity Value", f"{fcf_yield * 100:.1f}% FCF yield", display, source="Valuation / History_Q")
            _add("Margin Cycle / Lapping Risk", "Crush margin comps", "Compare current margin against prior-year and mid-cycle spread before underwriting durability.", source="Economics_Overlay")
            _add("Margin Cycle / Lapping Risk", "Policy lapping", "45Z and policy benefits may be step-function items, not same-store-style growth.", source="Guidance_Normalized")
            _add("Ethanol / Policy Health", "Ethanol demand", "Exports/E15 and domestic demand determine how much margin upside can persist.", source="Operating_Drivers")
            _add("Ethanol / Policy Health", "45Z / RFS / RIN setup", "Policy clarity and monetization are central to the equity case.", source="Guidance_Normalized / Economics_Overlay")
        else:
            _add(
                "Investment Snapshot",
                "Model read",
                "General operating, cash-flow and valuation case pending ticker-specific configuration.",
                investment_read="Use source-backed history and manual scenario inputs until company-specific drivers are configured.",
            )
            _add(
                "Investment Snapshot",
                "Key debate",
                "Can current operating performance convert into durable earnings and free cash flow?",
                source="model-derived",
            )
            _add(
                "Investment Snapshot",
                "Current stance",
                "Generic fallback only; add ticker-specific drivers before using this sheet as a complete investment thesis.",
                source="model-derived",
            )
        if ticker_txt in {"PBI", "GPRE"}:
            for row in rows:
                for key in ("source", "source_note", "display", "investment_read", "current_trend", "beat_miss_risk"):
                    txt = row.get(key)
                    if not isinstance(txt, str) or "Guidance_Normalized" not in txt:
                        continue
                    row[key] = (
                        txt.replace("Guidance_Normalized / management guidance", "Slides_Guidance / curated guidance profile")
                        .replace("Guidance_Normalized / raw sources", "Slides_Guidance / curated guidance profile / raw sources")
                        .replace("Guidance_Normalized / Economics_Overlay", "Slides_Guidance / curated guidance profile / Economics_Overlay")
                        .replace("Guidance_Normalized / History_Q", "Slides_Guidance / curated guidance profile / History_Q")
                        .replace("History_Q / Guidance_Normalized", "History_Q / Slides_Guidance / curated guidance profile")
                        .replace("Guidance_Normalized", "Slides_Guidance / curated guidance profile")
                    )
                display_txt = row.get("display")
                if isinstance(display_txt, str):
                    row["display"] = display_txt.replace("latest normalized guidance", "latest curated guidance")
        return pd.DataFrame(rows)
