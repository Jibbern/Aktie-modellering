"""Valuation render-bundle cache support for the workbook writer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class ValuationRenderBundleDeps:
    runtime: MutableMapping[str, Any]


def ensure_valuation_render_bundle(
    deps: ValuationRenderBundleDeps,
    qs_local: Any,
    leverage_df_local: Any = None,
) -> Any:
    runtime = deps.runtime
    pd_mod = runtime.get("pd", pd)
    hist = runtime.get("hist")
    ctx_ref = runtime.get("ctx_ref")
    valuation_render_bundle_cache = runtime.get("valuation_render_bundle_cache")
    _hist_view = runtime["_hist_view"]
    _timed_writer_substage = runtime["_timed_writer_substage"]
    _shared_load_local_balance_sheet_detail_payloads = runtime[
        "_shared_load_local_balance_sheet_detail_payloads"
    ]
    _carry_forward_low_change_series = runtime["_carry_forward_low_change_series"]

    # The render bundle is the lighter, quarter-keyed valuation substrate. It
    # normalizes history/leverage inputs once and memoizes the reusable maps that
    # visible valuation rows and downstream QA/precompute logic consume.
    quarter_key = tuple(pd_mod.Timestamp(q).normalize() for q in qs_local if pd_mod.notna(q))
    if (
        valuation_render_bundle_cache is not None
        and tuple(valuation_render_bundle_cache.get("quarter_key") or ()) == quarter_key
    ):
        if ctx_ref is not None:
            ctx_ref.derived.valuation_render_bundle = valuation_render_bundle_cache
        return valuation_render_bundle_cache

    hist_indexed = pd_mod.DataFrame()
    leverage_indexed = pd_mod.DataFrame()
    with _timed_writer_substage("write_excel.valuation.bundle.index_sources"):
        if ctx_ref is not None and ctx_ref.derived.valuation_hist_indexed is not None:
            hist_indexed = ctx_ref.derived.valuation_hist_indexed
        elif hist is not None and not hist.empty and "quarter" in hist.columns:
            hist_local = _hist_view().copy()
            if "_quarter" in hist_local.columns:
                hist_local["quarter"] = hist_local["_quarter"]
            hist_indexed = hist_local[hist_local["quarter"].notna()].drop_duplicates(subset=["quarter"], keep="last").set_index("quarter")

        if leverage_df_local is not None and not leverage_df_local.empty and "quarter" in leverage_df_local.columns:
            lev_local = leverage_df_local.copy()
            lev_local["quarter"] = pd_mod.to_datetime(lev_local["quarter"], errors="coerce")
            leverage_indexed = lev_local[lev_local["quarter"].notna()].drop_duplicates(subset=["quarter"], keep="last").set_index("quarter")
    quarter_index_map = {pd_mod.Timestamp(q): idx for idx, q in enumerate(quarter_key)}
    last4_quarters_map: Dict[pd.Timestamp, Tuple[pd.Timestamp, ...]] = {}
    for idx, q in enumerate(quarter_key):
        if idx < 3:
            continue
        last4_quarters_map[pd_mod.Timestamp(q)] = tuple(pd_mod.Timestamp(v) for v in quarter_key[idx - 3 : idx + 1])

    def _series_map(df_in: Any, col: Optional[str]) -> Dict[pd.Timestamp, Any]:
        if df_in is None or df_in.empty or not col or col not in df_in.columns:
            return {}
        ser = pd_mod.to_numeric(df_in[col], errors="coerce")
        return {pd_mod.Timestamp(k): (float(v) if pd_mod.notna(v) else None) for k, v in ser.items()}

    def _first_existing_numeric_col(df_in: Any, candidates: List[str]) -> Optional[str]:
        if df_in is None or df_in.empty:
            return None
        cols_lc = {str(c).strip().lower(): c for c in df_in.columns}
        for cand in candidates:
            resolved = cols_lc.get(str(cand).strip().lower())
            if resolved is None:
                continue
            ser = pd_mod.to_numeric(df_in[resolved], errors="coerce")
            if ser.notna().any():
                return str(resolved)
        return None

    def _normalize_cash_outflow_sign(src: Dict[pd.Timestamp, Any]) -> Dict[pd.Timestamp, Any]:
        if not src:
            return src
        vals = [float(v) for v in src.values() if v is not None and pd_mod.notna(v)]
        if not vals:
            return src
        neg = sum(1 for v in vals if v < 0)
        pos = sum(1 for v in vals if v > 0)
        if neg > pos:
            return {k: (-float(v) if v is not None and pd_mod.notna(v) else None) for k, v in src.items()}
        return src

    with _timed_writer_substage("write_excel.valuation.bundle.local_bs_payloads"):
        # Local balance-sheet payloads are a narrow rescue path for goodwill and
        # intangibles when GAAP history does not carry enough quarter detail.
        goodwill_map = _series_map(hist_indexed, "goodwill")
        intangibles_map = _series_map(hist_indexed, "intangibles")
        for qv in quarter_key:
            gw_hist = goodwill_map.get(qv)
            if gw_hist is not None and abs(float(gw_hist)) < 1_000_000.0:
                goodwill_map[qv] = None
            int_hist = intangibles_map.get(qv)
            if int_hist is not None and abs(float(int_hist)) < 1_000_000.0:
                intangibles_map[qv] = None
        valuation_bs_payloads = _shared_load_local_balance_sheet_detail_payloads({q.date() for q in quarter_key})
        for qv in quarter_key:
            if goodwill_map.get(qv) is None:
                payload_vals = (valuation_bs_payloads.get(qv.date()) or {}).get("values", {}) or {}
                gw_val = payload_vals.get("goodwill")
                if gw_val is not None:
                    goodwill_map[qv] = float(gw_val)
            if intangibles_map.get(qv) is None:
                payload_vals = (valuation_bs_payloads.get(qv.date()) or {}).get("values", {}) or {}
                int_val = payload_vals.get("intangibles")
                if int_val is not None:
                    intangibles_map[qv] = float(int_val)
        goodwill_map = _carry_forward_low_change_series(goodwill_map, list(quarter_key))
        intangibles_map = _carry_forward_low_change_series(intangibles_map, list(quarter_key))

    with _timed_writer_substage("write_excel.valuation.bundle.return_capital_maps"):
        # These maps are the fast GAAP/facts-side capital-return baseline. The
        # heavier precompute bundle can later refine them with document-derived
        # execution evidence, but this bundle is the first pass.
        buyback_col = _first_existing_numeric_col(
            hist_indexed,
            [
                "buybacks_cash",
                "buybacks",
                "share_repurchases",
                "repurchase_of_common_stock",
                "repurchases_of_common_stock",
                "payments_for_repurchase_of_common_stock",
                "treasury_stock_acquired",
                "common_stock_repurchased",
            ],
        )
        dividend_col = _first_existing_numeric_col(
            hist_indexed,
            [
                "dividends_cash",
                "common_stock_dividends_paid",
                "payments_of_dividends_common_stock",
            ],
        )
        buyback_map = _normalize_cash_outflow_sign(_series_map(hist_indexed, buyback_col)) if buyback_col else {}
        dividend_map = _normalize_cash_outflow_sign(_series_map(hist_indexed, dividend_col)) if dividend_col else {}

        buyback_shares_q_map: Dict[pd.Timestamp, Any] = {}
        shares_out_map = _series_map(hist_indexed, "shares_outstanding")
        for idx_q, qv in enumerate(quarter_key):
            if idx_q == 0:
                buyback_shares_q_map[qv] = None
                continue
            prev_q = quarter_key[idx_q - 1]
            sh_now = shares_out_map.get(qv)
            sh_prev = shares_out_map.get(prev_q)
            buyback_shares_q_map[qv] = (float(sh_prev) - float(sh_now)) if sh_now is not None and sh_prev is not None else None

    valuation_render_bundle_cache = {
        "quarter_key": quarter_key,
        "quarter_index_map": quarter_index_map,
        "last4_quarters_map": last4_quarters_map,
        "hist_indexed": hist_indexed,
        "leverage_indexed": leverage_indexed,
        "rev_map": _series_map(hist_indexed, "revenue"),
        "gross_profit_map": _series_map(hist_indexed, "gross_profit"),
        "ebitda_map": _series_map(hist_indexed, "ebitda"),
        "ebit_map": _series_map(hist_indexed, "op_income"),
        "net_income_map": _series_map(hist_indexed, "net_income"),
        "cfo_map": _series_map(hist_indexed, "cfo"),
        "capex_map": _series_map(hist_indexed, "capex"),
        "price_map": _series_map(hist_indexed, "price"),
        "market_cap_map": _series_map(hist_indexed, "market_cap"),
        "int_paid_map": _series_map(hist_indexed, "interest_paid"),
        "tax_paid_map": _series_map(hist_indexed, "tax_paid"),
        "cash_map": _series_map(hist_indexed, "cash"),
        "total_debt_map": _series_map(hist_indexed, "total_debt"),
        "debt_current_map": _series_map(hist_indexed, "debt_current"),
        "debt_core_map": _series_map(hist_indexed, "debt_core"),
        "shares_map": _series_map(hist_indexed, "shares_diluted"),
        "shares_out_map": shares_out_map,
        "total_equity_map": _series_map(hist_indexed, "total_equity"),
        "goodwill_map": goodwill_map,
        "intangibles_map": intangibles_map,
        "pension_map": _series_map(hist_indexed, "pension_obligation_net"),
        "assets_map": _series_map(hist_indexed, "assets"),
        "liabilities_map": _series_map(hist_indexed, "liabilities"),
        "assets_current_map": _series_map(hist_indexed, "assets_current"),
        "liabilities_current_map": _series_map(hist_indexed, "liabilities_current"),
        "ar_map": _series_map(hist_indexed, "accounts_receivable"),
        "inventory_map": _series_map(hist_indexed, "inventory"),
        "sti_map": _series_map(hist_indexed, "short_term_investments"),
        "rd_map": _series_map(hist_indexed, "research_and_development"),
        "acquisitions_map": _normalize_cash_outflow_sign(_series_map(hist_indexed, "acquisitions_cash")),
        "debt_repay_map": _normalize_cash_outflow_sign(_series_map(hist_indexed, "debt_repayment")),
        "debt_issuance_map": _series_map(hist_indexed, "debt_issuance"),
        "ebitda_ttm_map": _series_map(leverage_indexed, "ebitda_ttm"),
        "net_lev_map": _series_map(leverage_indexed, "corporate_net_leverage"),
        "cov_pnl_map": _series_map(leverage_indexed, "interest_coverage_pnl"),
        "rev_commit_map": _series_map(leverage_indexed, "revolver_commitment"),
        "rev_facility_map": _series_map(leverage_indexed, "revolver_facility_size"),
        "rev_drawn_map": _series_map(leverage_indexed, "revolver_drawn"),
        "rev_lc_map": _series_map(leverage_indexed, "revolver_letters_of_credit"),
        "rev_avail_map": _series_map(leverage_indexed, "revolver_availability"),
        "liquidity_map": _series_map(leverage_indexed, "liquidity"),
        "int_paid_ttm_map": _series_map(leverage_indexed, "interest_paid_ttm"),
        "buyback_map": buyback_map,
        "dividend_map": dividend_map,
        "buyback_cash_facts_map": dict(buyback_map),
        "dividend_cash_facts_map": dict(dividend_map),
        "buyback_shares_q_map": buyback_shares_q_map,
    }
    runtime["valuation_render_bundle_cache"] = valuation_render_bundle_cache
    if ctx_ref is not None:
        ctx_ref.derived.valuation_render_bundle = valuation_render_bundle_cache
    return valuation_render_bundle_cache
