"""Valuation Hidden Value and capital-return state support.

This module owns the non-render state construction consumed by the Valuation
Hidden Value panel. The owning writer injects its live run-scoped dependencies
through a runtime mapping so source, cache, and fallback ordering stay intact.
"""
from __future__ import annotations

import builtins
import html
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class ValuationHiddenValueStateDeps:
    runtime: MutableMapping[str, Any]


@dataclass(frozen=True)
class ValuationHiddenValueStateResult:
    hv_scores: dict[str, int | None]
    hv_obs: list[str]
    hv_buybacks: str
    hv_buybacks_note: str
    hv_dividends: str
    hv_dividends_note: str


def build_valuation_hidden_value_state(
    deps: ValuationHiddenValueStateDeps,
) -> ValuationHiddenValueStateResult:
    __rt = deps.runtime

    def _rt_get(name: str) -> Any:
        if name in __rt:
            return __rt[name]
        if name in globals():
            return globals()[name]
        return getattr(builtins, name, None)

    _anf_buyback_execution_is_year_or_ttm = _rt_get('_anf_buyback_execution_is_year_or_ttm')
    _anf_format_year_ttm_buyback_summary = _rt_get('_anf_format_year_ttm_buyback_summary')
    _anf_prior_year_quarter = _rt_get('_anf_prior_year_quarter')
    _build_hidden_value_flags_fallback = _rt_get('_build_hidden_value_flags_fallback')
    _delta_m = _rt_get('_delta_m')
    _ensure_terminal_period = _rt_get('_ensure_terminal_period')
    _extract_latest_buyback_remaining_from_sec = _rt_get('_extract_latest_buyback_remaining_from_sec')
    _extract_valuation_filing_doc_text = _rt_get('_extract_valuation_filing_doc_text')
    _money_m = _rt_get('_money_m')
    _parse_buyback_money_local = _rt_get('_parse_buyback_money_local')
    _pct = _rt_get('_pct')
    _prev_quarter_end_from_qend = _rt_get('_prev_quarter_end_from_qend')
    _quarter_notes_view = _rt_get('_quarter_notes_view')
    _record_writer_substage = _rt_get('_record_writer_substage')
    _resolve_col = _rt_get('_resolve_col')
    _sec_cache_docs_for_token_local = _rt_get('_sec_cache_docs_for_token_local')
    _ttm_map = _rt_get('_ttm_map')
    adj_ebitda_ttm_map = _rt_get('adj_ebitda_ttm_map')
    adj_metrics = _rt_get('adj_metrics')
    all_qs_ts = _rt_get('all_qs_ts')
    build_hidden_value_flags = _rt_get('build_hidden_value_flags')
    buyback_avg_price_doc_map = _rt_get('buyback_avg_price_doc_map')
    buyback_cash_facts_map = _rt_get('buyback_cash_facts_map')
    buyback_doc_note_map = _rt_get('buyback_doc_note_map')
    buyback_map = _rt_get('buyback_map')
    buyback_shares_map = _rt_get('buyback_shares_map')
    buyback_shares_text_map = _rt_get('buyback_shares_text_map')
    buyback_ttm_map = _rt_get('buyback_ttm_map')
    cache_root = _rt_get('cache_root')
    capital_return_resolved = _rt_get('capital_return_resolved')
    cov_cash_map = _rt_get('cov_cash_map')
    cov_pnl_map = _rt_get('cov_pnl_map')
    date_ref = _rt_get('date_ref')
    debt_credit_notes = _rt_get('debt_credit_notes')
    debt_tranches = _rt_get('debt_tranches')
    dividend_cash_facts_map = _rt_get('dividend_cash_facts_map')
    dividend_doc_note_map = _rt_get('dividend_doc_note_map')
    dividend_map = _rt_get('dividend_map')
    dividend_ps_doc_map = _rt_get('dividend_ps_doc_map')
    dividend_ttm_map = _rt_get('dividend_ttm_map')
    ebitda_ttm_map = _rt_get('ebitda_ttm_map')
    fcf_per_share_ttm = _rt_get('fcf_per_share_ttm')
    fcf_ttm_map = _rt_get('fcf_ttm_map')
    flags_audit_df = _rt_get('flags_audit_df')
    flags_df = _rt_get('flags_df')
    glx_normalize_text = _rt_get('glx_normalize_text')
    hist = _rt_get('hist')
    is_anf_profile = _rt_get('is_anf_profile')
    last4_quarters_map = _rt_get('last4_quarters_map')
    leverage_df = _rt_get('leverage_df')
    manifest_df = _rt_get('manifest_df')
    net_debt_map = _rt_get('net_debt_map')
    price = _rt_get('price')
    promises = _rt_get('promises')
    qs = _rt_get('qs')
    quarter_notes = _rt_get('quarter_notes')
    rev_ttm_map = _rt_get('rev_ttm_map')
    shares_for_value_map = _rt_get('shares_for_value_map')
    shares_out_map = _rt_get('shares_out_map')
    signals_base_df = _rt_get('signals_base_df')
    strip_html = _rt_get('strip_html')
    ticker = _rt_get('ticker')

    # Hidden Value Panel values are computed here and rendered later
    # (under Notes and above sensitivity grid).
    hv_scores: Dict[str, Optional[int]] = {
        "hidden_score": None,
        "comp_profit": None,
        "comp_cash": None,
        "comp_delev": None,
        "comp_quality": None,
        "comp_narr": None,
    }
    hv_obs: List[str] = ["", "", "", "", ""]
    hv_buybacks = "QoQ n/a | TTM n/a | YoY Δ n/a"
    hv_dividends = "Latest Q div/share n/a | TTM div/share n/a | YoY Δ div/share n/a"
    hv_buybacks_note = "Note: buyback cash spend not found in current source window."
    hv_dividends_note = "Note: dividend cash disclosures not found in current source window."

    def _clamp01(x: Optional[float]) -> float:
        if x is None or pd.isna(x):
            return 0.0
        return max(0.0, min(1.0, float(x)))


    def _shares_m(v: Optional[float]) -> str:
        if v is None or pd.isna(v):
            return "n/a"
        sgn = "+" if float(v) >= 0 else "-"
        return f"{sgn}{abs(float(v)) / 1e6:,.3f}m"

    def _ps(v: Optional[float], signed: bool = False) -> str:
        if v is None or pd.isna(v):
            return "n/a"
        x = float(v)
        if signed:
            sgn = "+" if x >= 0 else "-"
            return f"{sgn}${abs(x):,.3f}"
        return f"${x:,.3f}"

    def _ttm_zero_fill(src: Dict[pd.Timestamp, Any], qq: pd.Timestamp) -> Optional[float]:
        if not src:
            return None
        last4 = last4_quarters_map.get(pd.Timestamp(qq))
        if not last4:
            return None
        vals = [src.get(pd.Timestamp(qv)) for qv in last4]
        if all(v is None for v in vals):
            return None
        return float(sum(float(v) if v is not None else 0.0 for v in vals))

    if date_ref is not None:
        q_now = pd.Timestamp(date_ref)
        q_ly = q_now - pd.DateOffset(years=1)
        if is_anf_profile:
            try:
                anf_q_ly = _anf_prior_year_quarter(q_now, all_qs_ts)
            except Exception:
                anf_q_ly = None
            if anf_q_ly is not None:
                q_ly = pd.Timestamp(anf_q_ly)
        q_prev_d = _prev_quarter_end_from_qend(q_now.date())
        q_prev = pd.Timestamp(q_prev_d) if q_prev_d else (q_now - pd.DateOffset(months=3))

        rev_ttm_now = rev_ttm_map.get(q_now)
        rev_ttm_ly = rev_ttm_map.get(q_ly)
        adj_ttm_now = adj_ebitda_ttm_map.get(q_now) if adj_ebitda_ttm_map else None
        adj_ttm_ly = adj_ebitda_ttm_map.get(q_ly) if adj_ebitda_ttm_map else None
        gaap_ttm_now = ebitda_ttm_map.get(q_now) if ebitda_ttm_map else None
        gaap_ttm_ly = ebitda_ttm_map.get(q_ly) if ebitda_ttm_map else None
        if adj_ttm_now is None:
            adj_ttm_now = gaap_ttm_now
        if adj_ttm_ly is None:
            adj_ttm_ly = gaap_ttm_ly

        margin_now = (adj_ttm_now / rev_ttm_now) if (adj_ttm_now is not None and rev_ttm_now not in (None, 0)) else None
        margin_ly = (adj_ttm_ly / rev_ttm_ly) if (adj_ttm_ly is not None and rev_ttm_ly not in (None, 0)) else None
        margin_bps = (margin_now - margin_ly) * 10000.0 if (margin_now is not None and margin_ly is not None) else None

        fcf_ttm_now = fcf_ttm_map.get(q_now) if fcf_ttm_map else None
        fcf_ttm_ly = fcf_ttm_map.get(q_ly) if fcf_ttm_map else None
        fcf_ttm_yoy = (
            (float(fcf_ttm_now) - float(fcf_ttm_ly)) / abs(float(fcf_ttm_ly))
            if (fcf_ttm_now is not None and fcf_ttm_ly not in (None, 0))
            else None
        )
        fcf_share_now = fcf_per_share_ttm.get(q_now) if fcf_per_share_ttm else None
        fcf_share_ly = fcf_per_share_ttm.get(q_ly) if fcf_per_share_ttm else None
        if fcf_share_now is None and fcf_ttm_now is not None:
            try:
                shares_now = shares_for_value_map.get(q_now)
                if shares_now not in (None, 0) and pd.notna(shares_now):
                    fcf_share_now = float(fcf_ttm_now) / float(shares_now)
            except Exception:
                fcf_share_now = None
        if fcf_share_ly is None and fcf_ttm_ly is not None:
            try:
                shares_ly = shares_for_value_map.get(q_ly)
                if shares_ly not in (None, 0) and pd.notna(shares_ly):
                    fcf_share_ly = float(fcf_ttm_ly) / float(shares_ly)
            except Exception:
                fcf_share_ly = None
        fcf_share_yoy = (
            (float(fcf_share_now) - float(fcf_share_ly)) / abs(float(fcf_share_ly))
            if (fcf_share_now is not None and fcf_share_ly not in (None, 0))
            else None
        )
        fcf_conv_now = (fcf_ttm_now / adj_ttm_now) if (fcf_ttm_now is not None and adj_ttm_now not in (None, 0)) else None

        net_debt_now = net_debt_map.get(q_now)
        net_debt_ly = net_debt_map.get(q_ly)
        net_debt_delta = (net_debt_now - net_debt_ly) if (net_debt_now is not None and net_debt_ly is not None) else None
        cov_pnl_now = cov_pnl_map.get(q_now) if cov_pnl_map else None
        cov_pnl_ly = cov_pnl_map.get(q_ly) if cov_pnl_map else None
        cov_cash_now = cov_cash_map.get(q_now)
        cov_cash_ly = cov_cash_map.get(q_ly)
        cov_now = cov_pnl_now if cov_pnl_now is not None else cov_cash_now
        cov_ly = cov_pnl_ly if cov_pnl_ly is not None else cov_cash_ly
        cov_delta = (cov_now - cov_ly) if (cov_now is not None and cov_ly is not None) else None
        cov_label = "P&L" if cov_pnl_now is not None and cov_pnl_ly is not None else "Cash"

        gap_now = (adj_ttm_now - gaap_ttm_now) if (adj_ttm_now is not None and gaap_ttm_now is not None) else None
        gap_ly = (adj_ttm_ly - gaap_ttm_ly) if (adj_ttm_ly is not None and gaap_ttm_ly is not None) else None
        gap_now_pct = (abs(gap_now) / abs(adj_ttm_now)) if (gap_now is not None and adj_ttm_now not in (None, 0)) else None
        gap_ly_pct = (abs(gap_ly) / abs(adj_ttm_ly)) if (gap_ly is not None and adj_ttm_ly not in (None, 0)) else None
        gap_improve = (gap_ly_pct - gap_now_pct) if (gap_now_pct is not None and gap_ly_pct is not None) else None

        ref_now = 0
        ref_prev = 0
        if debt_credit_notes is not None and not debt_credit_notes.empty and "quarter" in debt_credit_notes.columns:
            dcn = debt_credit_notes.copy()
            dcn["quarter"] = pd.to_datetime(dcn["quarter"], errors="coerce")
            txt_col = "snippet" if "snippet" in dcn.columns else ("note" if "note" in dcn.columns else None)
            if txt_col is not None:
                kws = ["ecommerce", "e-commerce", "saas", "transition", "exited", "exit"]
                def _count_refs(sub_df: pd.DataFrame) -> int:
                    c = 0
                    for v in sub_df[txt_col].dropna().astype(str).str.lower():
                        c += sum(v.count(k) for k in kws)
                    return c
                ref_now = _count_refs(dcn[dcn["quarter"] == q_now])
                ref_prev = _count_refs(dcn[dcn["quarter"] == q_prev])

        comp_profit = int(round(20.0 * _clamp01((margin_bps - 150.0) / 600.0 if margin_bps is not None else None)))
        comp_cash = int(round(20.0 * (0.6 * _clamp01((fcf_ttm_yoy - 0.05) / 0.35 if fcf_ttm_yoy is not None else None) + 0.4 * _clamp01((fcf_conv_now - 0.50) / 0.60 if fcf_conv_now is not None else None))))
        comp_delev = int(round(20.0 * (0.6 * _clamp01((-net_debt_delta) / 300_000_000 if net_debt_delta is not None else None) + 0.4 * _clamp01((cov_delta) / 2.0 if cov_delta is not None else None))))
        comp_quality = int(round(20.0 * _clamp01((gap_improve + 0.05) / 0.25 if gap_improve is not None else None)))
        comp_narr = int(round(20.0 * (0.5 * _clamp01(ref_now / 6.0) + 0.5 * _clamp01((ref_now - ref_prev + 2) / 4.0))))
        hidden_score = int(round(max(0, min(100, comp_profit + comp_cash + comp_delev + comp_quality + comp_narr))))

        hv_scores["hidden_score"] = hidden_score
        hv_scores["comp_profit"] = comp_profit
        hv_scores["comp_cash"] = comp_cash
        hv_scores["comp_delev"] = comp_delev
        hv_scores["comp_quality"] = comp_quality
        hv_scores["comp_narr"] = comp_narr

        margin_dir = "up" if (margin_bps is not None and margin_bps >= 0) else "down"
        b1 = f"Adj EBITDA margin TTM {margin_dir} {abs(margin_bps):.0f} bps YoY ({q_ly.date()} -> {q_now.date()})" if margin_bps is not None else "Adj EBITDA margin TTM YoY: n/a"
        b2 = f"FCF TTM {_pct(fcf_ttm_yoy)} YoY; FCF/share TTM {_pct(fcf_share_yoy)}"
        if net_debt_delta is None:
            b3 = "Net debt trend: n/a"
        else:
            try:
                net_debt_now_f = float(net_debt_now) if net_debt_now is not None and pd.notna(net_debt_now) else None
            except Exception:
                net_debt_now_f = None
            if net_debt_now_f is not None and net_debt_now_f < 0:
                nd_dir = "decreased" if net_debt_delta > 0 else "increased"
                b3 = f"Net cash {nd_dir} {_money_m(abs(net_debt_delta))} since {q_ly.date()}"
            else:
                nd_dir = "down" if net_debt_delta < 0 else "up"
                b3 = f"Net debt {nd_dir} {_money_m(abs(net_debt_delta))} since {q_ly.date()}"
        if cov_now is None or cov_ly is None:
            b4 = "Interest coverage trend: n/a"
        else:
            cov_word = "improved" if float(cov_now) > float(cov_ly) else ("declined" if float(cov_now) < float(cov_ly) else "was flat")
            b4 = f"Interest coverage ({cov_label}) {cov_word} from {cov_ly:.2f}x to {cov_now:.2f}x"
        b5 = ""
        try:
            if quarter_notes is not None and not quarter_notes.empty:
                qn = _quarter_notes_view()
                txtcol_qn = _resolve_col(qn, ["note", "text_full"])
                if txtcol_qn:
                    qn = qn[qn["_quarter"] == q_now]
                    qn = qn[qn[txtcol_qn].astype(str).str.contains(r"45z|tax credit|low-ci|low ci|protein|corn oil|margin", case=False, na=False)]
                    if not qn.empty:
                        cand = str(qn.iloc[0][txtcol_qn] or "").strip()
                        b5 = cand[:220]
        except Exception:
            b5 = ""

        hv_obs = [x for x in [b1, b2, b3, b4, b5] if str(x or "").strip()]
        explicit_buyback_shares_map = buyback_shares_map if 'buyback_shares_map' in locals() and buyback_shares_map else {}
        if not explicit_buyback_shares_map:
            explicit_buyback_shares_map = buyback_shares_text_map if 'buyback_shares_text_map' in locals() and buyback_shares_text_map else {}
        explicit_buyback_shares_ttm_map = _ttm_map(explicit_buyback_shares_map) if explicit_buyback_shares_map else {}
        bb_q = explicit_buyback_shares_map.get(q_now) if explicit_buyback_shares_map else None
        bb_ttm = explicit_buyback_shares_ttm_map.get(q_now) if explicit_buyback_shares_ttm_map else None
        bb_ttm_ly = explicit_buyback_shares_ttm_map.get(q_ly) if explicit_buyback_shares_ttm_map else None
        bb_note_fallback = str((buyback_doc_note_map or {}).get(q_now) or "")
        if (bb_q in (None, 0) or (pd.notna(bb_q) and abs(float(bb_q)) < 1.0)) and bb_note_fallback:
            m_bb_note = re.search(r"(?:repurchased|to\s+repurchase)\s+([0-9]+(?:\.\d+)?)m\s+shares", bb_note_fallback, re.I)
            if m_bb_note:
                try:
                    bb_q = float(m_bb_note.group(1)) * 1_000_000.0
                except Exception:
                    pass
        if bb_ttm is None:
            bb_yoy_delta = None
        elif bb_ttm_ly in (None, 0):
            bb_yoy_delta = 0.0
        else:
            bb_yoy_delta = bb_ttm - bb_ttm_ly
        bb_cash_q = buyback_map.get(q_now) if buyback_map else None
        dv_q = dividend_map.get(q_now) if dividend_map else None
        dv_ttm = dividend_ttm_map.get(q_now) if dividend_ttm_map else None
        dv_ttm_ly = dividend_ttm_map.get(q_ly) if dividend_ttm_map else None
        dv_yoy_delta = (dv_ttm - dv_ttm_ly) if (dv_ttm is not None and dv_ttm_ly is not None) else None
        # Dividend per-share (prefer direct disclosure, fallback to cash/share approximation).
        def _share_den(qq: pd.Timestamp) -> Optional[float]:
            sh = shares_out_map.get(qq) if shares_out_map else None
            if sh in (None, 0):
                sh = shares_for_value_map.get(qq)
            if sh in (None, 0):
                return None
            return float(sh)

        def _div_ps_from_cash(qq: pd.Timestamp) -> Optional[float]:
            dv = dividend_map.get(qq) if dividend_map else None
            sh = _share_den(qq)
            if dv is None or sh in (None, 0):
                return None
            return float(dv) / float(sh)

        explicit_div_ps_q = dividend_ps_doc_map.get(q_now) if 'dividend_ps_doc_map' in locals() and dividend_ps_doc_map else None
        explicit_div_ps_ly = dividend_ps_doc_map.get(q_ly) if 'dividend_ps_doc_map' in locals() and dividend_ps_doc_map else None
        implied_div_ps_q = _div_ps_from_cash(q_now)
        implied_div_ps_ly = _div_ps_from_cash(q_ly)
        div_ps_q = explicit_div_ps_q
        div_ps_ly = explicit_div_ps_ly
        div_ps_yoy_delta = (div_ps_q - div_ps_ly) if (div_ps_q is not None and div_ps_ly is not None) else None
        div_ps_ttm = None
        last4 = last4_quarters_map.get(q_now)
        if last4:
            dps_vals: List[float] = []
            ok = True
            for qq in last4:
                dps = dividend_ps_doc_map.get(qq) if 'dividend_ps_doc_map' in locals() and dividend_ps_doc_map else None
                if dps is None:
                    ok = False
                    break
                dps_vals.append(float(dps))
            if ok and dps_vals:
                div_ps_ttm = float(sum(dps_vals))

        hv_buybacks = (
            f"QoQ {_shares_m(bb_q)} | TTM {_shares_m(bb_ttm)} | YoY Δ {_shares_m(bb_yoy_delta)}"
            if any(v is not None for v in [bb_q, bb_ttm, bb_yoy_delta])
            else "n/a"
        )
        bb_src = buyback_doc_note_map.get(q_now) if 'buyback_doc_note_map' in locals() else None
        dv_src = dividend_doc_note_map.get(q_now) if 'dividend_doc_note_map' in locals() else None
        if promises is not None and not promises.empty:
            p = promises.copy()
            qcol_p = _resolve_col(p, ["last_seen_quarter", "created_quarter", "first_seen_quarter", "quarter"])
            txt_col_p = _resolve_col(p, ["statement", "promise_text", "evidence_snippet"])
            if qcol_p is not None and txt_col_p is not None:
                p[qcol_p] = pd.to_datetime(p[qcol_p], errors="coerce")
                p = p.dropna(subset=[qcol_p])
                win = p[
                    (p[qcol_p] >= q_now - pd.Timedelta(days=540))
                    & (p[qcol_p] <= q_now + pd.Timedelta(days=60))
                ]
                if not win.empty:
                    bb_win = win[win[txt_col_p].astype(str).str.contains(r"repurch|buyback|authorization", case=False, na=False)]
                    if not bb_win.empty and (not bb_src or "authorization" not in str(bb_src).lower()):
                        btxt = bb_win[txt_col_p].astype(str)
                        bscore = (
                            btxt.str.contains(r"authorization|available|remaining|execute", case=False, na=False).astype(int) * 3
                            + btxt.str.contains(r"\$", case=False, na=False).astype(int) * 2
                            + btxt.str.contains(r"expect|intend|plan|will", case=False, na=False).astype(int)
                        )
                        txt = str(bb_win.loc[bscore.sort_values(ascending=False).index[0]].get(txt_col_p) or "").strip()
                        if txt:
                            bb_src = (f"{bb_src} " if bb_src else "") + txt[:260]
                    dv_win = win[win[txt_col_p].astype(str).str.contains(r"dividend", case=False, na=False)]
                    if not dv_win.empty and (not dv_src or "expect to continue" not in str(dv_src).lower()):
                        dtxt = dv_win[txt_col_p].astype(str)
                        dscore = (
                            dtxt.str.contains(r"expect|continue|quarterly", case=False, na=False).astype(int) * 3
                            + dtxt.str.contains(r"\$", case=False, na=False).astype(int) * 2
                        )
                        txt = str(dv_win.loc[dscore.sort_values(ascending=False).index[0]].get(txt_col_p) or "").strip()
                        if txt:
                            dv_src = (f"{dv_src} " if dv_src else "") + txt[:260]
        latest_div_cash_fact = buyback_cash_fact = None
        latest_div_cash_fact = dividend_cash_facts_map.get(q_now) if dividend_cash_facts_map else None
        latest_buy_cash_fact = buyback_cash_facts_map.get(q_now) if buyback_cash_facts_map else None
        buyback_cash_ttm_fact = buyback_ttm_map.get(q_now) if buyback_ttm_map else None
        dividend_cash_ttm_fact = dividend_ttm_map.get(q_now) if dividend_ttm_map else None
        latest_buy_cash = latest_buy_cash_fact if latest_buy_cash_fact is not None else buyback_map.get(q_now)
        buyback_cash_ttm = buyback_cash_ttm_fact if buyback_cash_ttm_fact is not None else (buyback_ttm_map.get(q_now) if buyback_ttm_map else None)
        latest_div_cash = latest_div_cash_fact if latest_div_cash_fact is not None else dividend_map.get(q_now)
        dividend_cash_ttm = dividend_cash_ttm_fact if dividend_cash_ttm_fact is not None else (dividend_ttm_map.get(q_now) if dividend_ttm_map else None)
        bb_cash_ttm = buyback_cash_ttm
        latest_buy_cash_fact = latest_buy_cash
        buyback_cash_ttm_fact = buyback_cash_ttm
        latest_div_cash_fact = latest_div_cash
        dividend_cash_ttm_fact = dividend_cash_ttm
        explicit_bb_summary_override = ""
        explicit_bb_parts_override: Dict[str, Any] = {}
        if bb_note_fallback:
            explicit_bb_summary_override = glx_normalize_text(bb_note_fallback)
            m_explicit = re.search(
                r"\b(?:repurchased|to\s+repurchase)\s+([0-9]+(?:\.\d+)?)m\s+shares(?:\s+for\s+\$([0-9]+(?:\.\d+)?)m)?"
                r"(?:\s+with\s+an\s+average\s+price\s+of\s+\$([0-9]+(?:\.\d+)?)/share)?",
                explicit_bb_summary_override,
                re.I,
            )
            if m_explicit:
                try:
                    explicit_bb_parts_override["shares"] = float(m_explicit.group(1)) * 1_000_000.0
                    if m_explicit.group(2) is not None:
                        explicit_bb_parts_override["amount"] = float(m_explicit.group(2)) * 1_000_000.0
                    if m_explicit.group(3) is not None:
                        explicit_bb_parts_override["avg_price"] = float(m_explicit.group(3))
                except Exception:
                    explicit_bb_parts_override = {}
        explicit_bb_shares = pd.to_numeric(explicit_bb_parts_override.get("shares"), errors="coerce")
        explicit_bb_amount = pd.to_numeric(explicit_bb_parts_override.get("amount"), errors="coerce")
        explicit_bb_avg_price = pd.to_numeric(explicit_bb_parts_override.get("avg_price"), errors="coerce")
        bb_cash_ttm_ly = buyback_ttm_map.get(q_ly) if buyback_ttm_map else None
        shares_material_mismatch = bool(
            pd.notna(explicit_bb_shares)
            and (
                bb_q is None
                or not pd.notna(bb_q)
                or abs(float(bb_q) - float(explicit_bb_shares)) > 250_000.0
            )
        )
        amount_material_mismatch = bool(
            pd.notna(explicit_bb_amount)
            and (
                latest_buy_cash is None
                or not pd.notna(latest_buy_cash)
                or abs(float(latest_buy_cash) - float(explicit_bb_amount)) > 5_000_000.0
            )
        )
        if (bb_q is None and latest_buy_cash is None) and (shares_material_mismatch or amount_material_mismatch):
            if explicit_bb_summary_override:
                bb_src = explicit_bb_summary_override
        hv_buybacks = (
            f"QoQ {_shares_m(bb_q)} | TTM {_shares_m(bb_ttm)} | YoY Δ {_shares_m(bb_yoy_delta)}"
            if any(v is not None for v in [bb_q, bb_ttm, bb_yoy_delta])
            else "n/a"
        )
        has_historical_implied_dividend = any(v is not None for v in [implied_div_ps_q, implied_div_ps_ly])
        no_current_dividend_text = "No current common dividend/share signal."
        historical_implied_dividend_text = "Historical implied cash dividend/share observed in older periods, but no current dividend/share signal."
        hv_dividends = (
            f"Latest Q dividend cash {_money_m(latest_div_cash)} | "
            f"TTM dividend cash {_money_m(dividend_cash_ttm)} | "
            f"Latest div/share {_ps(div_ps_q)}"
        ) if div_ps_q is not None else (
            historical_implied_dividend_text if (str(ticker or '').upper() != "GPRE" and has_historical_implied_dividend) else no_current_dividend_text
        )
        if bb_q is not None and bb_cash_q is not None:
            bb_align = "aligned" if (float(bb_q) > 0 and float(bb_cash_q) > 0) else "check mismatch"
        else:
            bb_align = "partial coverage"

        if bb_cash_ttm is None:
            bb_cash_yoy_delta = None
        elif bb_cash_ttm_ly in (None, 0):
            bb_cash_yoy_delta = 0.0
        else:
            bb_cash_yoy_delta = bb_cash_ttm - bb_cash_ttm_ly
        dv_ttm_delta = (dv_ttm - dv_ttm_ly) if (dv_ttm is not None and dv_ttm_ly is not None) else None

        def _format_buyback_note_summary_local(
            latest_cash_in: Optional[float],
            ttm_cash_in: Optional[float],
            yoy_delta_in: Optional[float],
        ) -> str:
            if latest_cash_in is None and ttm_cash_in is None:
                return "Cash buybacks not directly observed in quarterly cashflow facts."
            parts_local: List[str] = []
            if latest_cash_in is not None:
                parts_local.append(f"Cash buybacks spent latest quarter {_money_m(latest_cash_in)}")
            else:
                parts_local.append("Cash buybacks spent latest quarter n/a")
            if ttm_cash_in is not None:
                parts_local.append(f"TTM {_money_m(ttm_cash_in)}")
                parts_local.append(f"YoY Δ {_delta_m(yoy_delta_in)}")
            return " | ".join(parts_local)

        def _parse_amount_from_text(txt: Optional[str]) -> Optional[float]:
            if not txt:
                return None
            m = re.search(
                r"(?:\$?\s*)?([0-9]{1,3}(?:,[0-9]{3})+(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                str(txt),
                re.I,
            )
            if not m:
                return None
            try:
                val = float(str(m.group(1)).replace(",", ""))
            except Exception:
                return None
            unit = str(m.group(2) or "").lower()
            if unit in {"billion", "bn"}:
                val *= 1e9
            elif unit in {"million", "m"}:
                val *= 1e6
            return val

        def _parse_buyback_maturity(txt: Optional[str]) -> Optional[str]:
            if not txt:
                return None
            s = str(txt)
            m = re.search(
                r"(?:by|through|until|matur(?:e|ity)|expir(?:e|es|ation)|end(?:ing)?\s+of)\s+"
                r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{4}|\d{4})",
                s,
                re.I,
            )
            if m:
                return str(m.group(1)).strip()
            return None

        def _parse_buyback_remaining(txt: Optional[str]) -> Optional[float]:
            if not txt:
                return None
            t = str(txt)
            pats = [
                r"(?:remaining|available|unused)[^.]{0,100}\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?[^.]{0,100}(?:authorization|repurchase)",
            ]
            for ptn in pats:
                m = re.search(ptn, t, re.I)
                if not m:
                    continue
                try:
                    v = float(str(m.group(1)).replace(",", ""))
                except Exception:
                    continue
                unit = str(m.group(2) or "").lower()
                if unit in {"billion", "bn"}:
                    v *= 1e9
                elif unit in {"million", "m"}:
                    v *= 1e6
                return v
            return None

        def _parse_buyback_authorization(txt: Optional[str]) -> Optional[float]:
            if not txt:
                return None
            t = str(txt)
            m = re.search(
                r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?[^.]{0,120}authorization",
                t,
                re.I,
            )
            if not m:
                return None
            try:
                v = float(str(m.group(1)).replace(",", ""))
            except Exception:
                return None
            unit = str(m.group(2) or "").lower()
            if unit in {"billion", "bn"}:
                v *= 1e9
            elif unit in {"million", "m"}:
                v *= 1e6
            return v

        # Improve authorization context using Promise_Tracker text when available.
        bb_auth_text = bb_src
        if promises is not None and not promises.empty:
            p = promises.copy()
            qcol_p = _resolve_col(p, ["last_seen_quarter", "created_quarter", "first_seen_quarter", "quarter"])
            txt_col_p = _resolve_col(p, ["statement", "promise_text", "evidence_snippet"])
            if qcol_p is not None and txt_col_p is not None:
                p[qcol_p] = pd.to_datetime(p[qcol_p], errors="coerce")
                p = p.dropna(subset=[qcol_p])
                p = p[
                    (p[qcol_p] >= q_now - pd.Timedelta(days=730))
                    & (p[qcol_p] <= q_now + pd.Timedelta(days=120))
                ]
                if not p.empty:
                    p_auth = p[p[txt_col_p].astype(str).str.contains(r"repurch|buyback", case=False, na=False)]
                    if not p_auth.empty:
                        score = (
                            p_auth[txt_col_p].astype(str).str.contains(r"authorization", case=False, na=False).astype(int) * 4
                            + p_auth[txt_col_p].astype(str).str.contains(r"\$\s*[0-9]", case=False, na=False).astype(int) * 3
                            + p_auth[txt_col_p].astype(str).str.contains(r"\bin\s+20\d{2}\b|by\s+20\d{2}", case=False, na=False).astype(int) * 2
                        )
                        best_idx = score.sort_values(ascending=False).index[0]
                        best_txt = str(p_auth.loc[best_idx, txt_col_p] or "").strip()
                        if best_txt:
                            bb_auth_text = ((bb_auth_text + " | ") if bb_auth_text else "") + best_txt

        bb_maturity = _parse_buyback_maturity(bb_auth_text)
        bb_remaining = _parse_buyback_remaining(bb_auth_text)
        bb_auth = _parse_buyback_authorization(bb_auth_text)

        def _spent_since_auth_year(src: Dict[pd.Timestamp, Any], year_s: Optional[str]) -> Optional[float]:
            if not src or not year_s:
                return None
            try:
                y = int(year_s)
            except Exception:
                return None
            vals: List[float] = []
            q_in_year = [pd.Timestamp(qx) for qx in qs if pd.Timestamp(qx).year == y and pd.Timestamp(qx) <= q_now]
            for qq in q_in_year:
                v = src.get(qq)
                if v is not None and pd.notna(v):
                    vals.append(float(v))
            if not vals:
                return None
            # Heuristic: if values are monotonic non-decreasing, treat them as YTD snapshots.
            mono = all(vals[i] >= vals[i - 1] - 1e-9 for i in range(1, len(vals)))
            return vals[-1] if mono else float(sum(vals))

        bb_spent_since_auth = _spent_since_auth_year(buyback_map, bb_maturity)
        if bb_remaining is None and bb_auth is not None:
            base_spent = bb_spent_since_auth if bb_spent_since_auth is not None else bb_cash_ttm
            if base_spent is not None:
                # If spend materially exceeds stated authorization, leave remaining unknown
                # unless explicit remaining text exists.
                if float(base_spent) > float(bb_auth) * 1.10:
                    bb_remaining = None
                else:
                    bb_remaining = max(float(bb_auth) - float(base_spent), 0.0)

        # Preferred remaining capacity method:
        #   remaining_now = latest SEC "remaining authorization" - buybacks spent after its as-of date
        buyback_auth_started = time.perf_counter()
        sec_auth = _extract_latest_buyback_remaining_from_sec(manifest_df)
        sec_remaining = sec_auth.get("remaining_dollars")
        sec_asof = sec_auth.get("asof_date")
        sec_spent_since: Optional[float] = None
        if sec_remaining is not None and sec_asof is not None:
            q_after = [pd.Timestamp(qq) for qq in qs if pd.Timestamp(qq).date() > sec_asof and pd.Timestamp(qq) <= q_now]
            if not q_after:
                sec_spent_since = 0.0
            else:
                vals: List[float] = []
                missing = False
                for qq in q_after:
                    v = buyback_map.get(pd.Timestamp(qq))
                    if v is None or pd.isna(v):
                        missing = True
                        break
                    vals.append(float(v))
                if not missing:
                    sec_spent_since = float(sum(vals))
            if sec_spent_since is not None:
                bb_remaining = max(float(sec_remaining) - float(sec_spent_since), 0.0)
                print(
                    f"[buyback_auth] spent_since_asof={sec_spent_since/1e6:.1f}m remaining_now={bb_remaining/1e6:.1f}m "
                    f"asof={sec_asof} latest_q={q_now.date()}",
                    flush=True,
                )
            else:
                bb_remaining = None
                print(
                    f"[buyback_auth] spent_since_asof unavailable (missing buyback cash series) asof={sec_asof}",
                    flush=True,
                )
        elif sec_remaining is None:
            # If SEC remaining authorization is not found, do not show a numeric fallback.
            bb_remaining = None
        if sec_auth.get("doc_path"):
            try:
                sec_doc_path = Path(str(sec_auth.get("doc_path") or ""))
                sec_doc_text = _extract_valuation_filing_doc_text(sec_doc_path) if sec_doc_path.exists() else ""
                sec_doc_text = re.sub(r"\s+", " ", str(sec_doc_text or "")).strip()
                sec_exec_shares = pd.to_numeric(None, errors="coerce")
                sec_exec_amount = pd.to_numeric(None, errors="coerce")
                sec_exec_avg_price = pd.to_numeric(None, errors="coerce")
                strict_exec_match = re.search(
                    r"\brepurchas\w*\b.{0,240}?([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
                    r".{0,240}?\bfor(?:\s+(?:a\s+)?total\s+of)?\s+\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                    sec_doc_text,
                    re.I,
                )
                if strict_exec_match:
                    try:
                        strict_shares = float(strict_exec_match.group(1)) * (
                            1_000_000.0 if str(strict_exec_match.group(2) or "").lower() in {"million", "m"} else 1.0
                        )
                    except Exception:
                        strict_shares = None
                    try:
                        strict_amount = _parse_buyback_money_local(strict_exec_match.group(3), strict_exec_match.group(4))
                    except Exception:
                        strict_amount = None
                    if strict_shares is not None and (
                        pd.isna(sec_exec_shares)
                        or abs(float(sec_exec_shares) - float(strict_shares)) > 250_000.0
                    ):
                        sec_exec_shares = float(strict_shares)
                    if strict_amount is not None and (
                        pd.isna(sec_exec_amount)
                        or abs(float(sec_exec_amount) - float(strict_amount)) > 5_000_000.0
                    ):
                        sec_exec_amount = float(strict_amount)
                    if strict_shares is not None and strict_amount is not None and (
                        pd.isna(sec_exec_avg_price) or float(sec_exec_avg_price) <= 0
                    ):
                        sec_exec_avg_price = float(strict_amount) / float(strict_shares)
                if pd.isna(sec_exec_shares):
                    m_sec_sh = re.search(
                        r"\brepurchas\w*\b.{0,220}?(?:approximately\s+)?([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                        sec_doc_text,
                        re.I,
                    )
                    if m_sec_sh:
                        try:
                            sec_exec_shares = float(m_sec_sh.group(1)) * (1_000_000.0 if str(m_sec_sh.group(2) or "").lower() in {"million", "m"} else 1.0)
                        except Exception:
                            sec_exec_shares = sec_exec_shares
                if pd.isna(sec_exec_amount):
                    m_sec_amt = re.search(
                        r"\brepurchas\w*\b.{0,260}?\bfor(?:\s+(?:a\s+)?total\s+of)?\s+(?:approximately\s+)?\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                        sec_doc_text,
                        re.I,
                    )
                    if m_sec_amt:
                        try:
                            sec_exec_amount = _parse_buyback_money_local(m_sec_amt.group(1), m_sec_amt.group(2))
                        except Exception:
                            sec_exec_amount = sec_exec_amount
                if pd.isna(sec_exec_avg_price):
                    m_sec_avg = re.search(
                        r"\baverage price(?: paid)?(?: per share| of)\s+\$?\s*([0-9]+(?:\.\d+)?)\b",
                        sec_doc_text,
                        re.I,
                    )
                    if m_sec_avg:
                        try:
                            sec_exec_avg_price = float(m_sec_avg.group(1))
                        except Exception:
                            sec_exec_avg_price = sec_exec_avg_price
                if pd.notna(sec_exec_shares) and (
                    bb_q is None
                    or not pd.notna(bb_q)
                    or abs(float(bb_q)) < 1.0
                    or abs(float(bb_q) - float(sec_exec_shares)) > 250_000.0
                ):
                    bb_q = float(sec_exec_shares)
                if pd.notna(sec_exec_amount) and (
                    latest_buy_cash is None
                    or not pd.notna(latest_buy_cash)
                    or abs(float(latest_buy_cash) - float(sec_exec_amount)) > 5_000_000.0
                ):
                    latest_buy_cash = float(sec_exec_amount)
                    latest_buy_cash_fact = latest_buy_cash
                if pd.notna(sec_exec_avg_price):
                    buyback_avg_price_doc_map[q_now] = float(sec_exec_avg_price)
                hv_buybacks = (
                    f"QoQ {_shares_m(bb_q)} | TTM {_shares_m(bb_ttm)} | YoY Δ {_shares_m(bb_yoy_delta)}"
                    if any(v is not None for v in [bb_q, bb_ttm, bb_yoy_delta])
                    else "n/a"
                )
            except Exception:
                pass
        _record_writer_substage("write_excel.valuation.render.buyback_auth", buyback_auth_started)
        if latest_buy_cash_fact is None and buyback_cash_ttm_fact is None:
            bb_note_summary = "Cash buybacks not directly observed in quarterly cashflow facts."
        else:
            bb_note_summary = (
                f"Cash buybacks spent latest quarter {_money_m(latest_buy_cash_fact)} | "
                f"TTM {_money_m(buyback_cash_ttm_fact)} | YoY Δ {_delta_m(bb_cash_yoy_delta)}"
            )
        if bb_note_fallback:
            m_note_cash = re.search(
                r"\brepurchased(?:\s+approximately)?\s+[0-9]+(?:\.\d+)?m\s+shares\s+for(?:\s+(?:a\s+)?total\s+of)?\s+(?:approximately\s+)?\$([0-9]+(?:\.\d+)?)m\b",
                bb_note_fallback,
                re.I,
            )
            m_note_avg = re.search(
                r"\baverage price of\s+\$([0-9]+(?:\.\d+)?)/share\b",
                bb_note_fallback,
                re.I,
            )
            if m_note_cash:
                try:
                    explicit_note_cash = float(m_note_cash.group(1)) * 1_000_000.0
                except Exception:
                    explicit_note_cash = None
                if explicit_note_cash is not None and (
                    latest_buy_cash_fact is None
                    or not pd.notna(latest_buy_cash_fact)
                    or abs(float(latest_buy_cash_fact) - float(explicit_note_cash)) > 5_000_000.0
                ):
                    latest_buy_cash_fact = explicit_note_cash
                    bb_note_summary = (
                        f"Cash buybacks spent latest quarter {_money_m(explicit_note_cash)} | "
                        f"TTM {_money_m(explicit_note_cash)} | YoY Δ {_delta_m(bb_cash_yoy_delta)}"
                    )
                if m_note_avg and (q_now not in buyback_avg_price_doc_map):
                    try:
                        buyback_avg_price_doc_map[q_now] = float(m_note_avg.group(1))
                    except Exception:
                        pass
        if sec_auth.get("doc_path"):
            try:
                sec_doc_path = Path(str(sec_auth.get("doc_path") or ""))
                sec_doc_text = _extract_valuation_filing_doc_text(sec_doc_path) if sec_doc_path.exists() else ""
                sec_doc_text = re.sub(r"\s+", " ", str(sec_doc_text or "")).strip()
                m_sec_direct = re.search(
                    r"\brepurchas\w*\b.{0,240}?(?:approximately\s+)?([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
                    r".{0,240}?\bfor(?:\s+(?:a\s+)?total\s+of)?\s+(?:approximately\s+)?\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                    sec_doc_text,
                    re.I,
                )
                if m_sec_direct:
                    sec_shares_note = float(m_sec_direct.group(1)) * (
                        1_000_000.0 if str(m_sec_direct.group(2) or "").lower() in {"million", "m"} else 1.0
                    )
                    sec_cash_note = _parse_buyback_money_local(m_sec_direct.group(3), m_sec_direct.group(4))
                    sec_avg_note = float(sec_cash_note) / float(sec_shares_note) if sec_cash_note and sec_shares_note else None
                    if sec_cash_note is not None and (
                        latest_buy_cash_fact is None
                        or not pd.notna(latest_buy_cash_fact)
                        or abs(float(latest_buy_cash_fact) - float(sec_cash_note)) > 5_000_000.0
                    ):
                        latest_buy_cash_fact = float(sec_cash_note)
                        bb_note_summary = (
                            f"Cash buybacks spent latest quarter {_money_m(sec_cash_note)} | "
                            f"TTM {_money_m(sec_cash_note)} | YoY Δ {_delta_m(bb_cash_yoy_delta)}"
                        )
                    if sec_avg_note is not None and (q_now not in buyback_avg_price_doc_map):
                        buyback_avg_price_doc_map[q_now] = float(sec_avg_note)
            except Exception:
                pass
        bb_avg_price_latest = buyback_avg_price_doc_map.get(q_now) if buyback_avg_price_doc_map else None
        if bb_avg_price_latest is None and bb_q is not None and latest_buy_cash_fact is not None:
            try:
                if float(bb_q) > 0 and float(latest_buy_cash_fact) > 0:
                    bb_avg_price_latest = float(latest_buy_cash_fact) / float(bb_q)
                    buyback_avg_price_doc_map[q_now] = float(bb_avg_price_latest)
            except Exception:
                bb_avg_price_latest = None
        implied_latest_buy_cash = None
        if bb_q is not None and bb_avg_price_latest is not None:
            try:
                implied_latest_buy_cash = float(bb_q) * float(bb_avg_price_latest)
            except Exception:
                implied_latest_buy_cash = None
        if implied_latest_buy_cash is not None:
            current_cash_val = pd.to_numeric(latest_buy_cash_fact, errors="coerce")
            current_cash_val = float(current_cash_val) if pd.notna(current_cash_val) else None
            if current_cash_val is None or abs(float(current_cash_val) - float(implied_latest_buy_cash)) > 5_000_000.0:
                latest_buy_cash = float(implied_latest_buy_cash)
                latest_buy_cash_fact = float(implied_latest_buy_cash)
                bb_note_summary = (
                    f"Cash buybacks spent latest quarter {_money_m(implied_latest_buy_cash)} | "
                    f"TTM {_money_m(implied_latest_buy_cash)} | YoY Î” {_delta_m(bb_cash_yoy_delta)}"
                )
        bb_note_summary = _format_buyback_note_summary_local(
            latest_buy_cash_fact,
            buyback_cash_ttm_fact,
            bb_cash_yoy_delta,
        )
        if bb_q is not None and bb_avg_price_latest is not None:
            bb_note_summary += f" | Latest quarter {_shares_m(bb_q)} at ${float(bb_avg_price_latest):.2f}/share"
        if "not directly observed in quarterly cashflow facts" in bb_note_summary and sec_auth.get("doc_path"):
            try:
                sec_doc_path = Path(str(sec_auth.get("doc_path") or ""))
                sec_doc_text = _extract_valuation_filing_doc_text(sec_doc_path) if sec_doc_path.exists() else ""
                m_bb_amt = re.search(
                    r"\brepurchas\w*\b.{0,260}?\bfor(?:\s+(?:a\s+)?total\s+of)?\s+(?:approximately\s+)?\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                    sec_doc_text,
                    re.I,
                )
                if m_bb_amt:
                    amt_override = _parse_buyback_money_local(m_bb_amt.group(1), m_bb_amt.group(2))
                    bb_note_summary = (
                        f"Buybacks spent latest quarter {_money_m(amt_override)} | "
                        f"TTM {_money_m(amt_override)} | YoY Δ n/a"
                    )
                    latest_buy_cash_fact = amt_override
                    bb_note_summary = _format_buyback_note_summary_local(
                        latest_buy_cash_fact,
                        buyback_cash_ttm_fact,
                        bb_cash_yoy_delta,
                    )
                    if bb_q is not None and bb_avg_price_latest is not None:
                        bb_note_summary += f" | Latest quarter {_shares_m(bb_q)} at ${float(bb_avg_price_latest):.2f}/share"
            except Exception:
                pass
        rem_txt = _money_m(bb_remaining) if bb_remaining is not None else "N/A"
        bb_note_detail = f"Latest authorization / remaining capacity: Maturity {bb_maturity or 'n/a'} | Remaining buyback capacity {rem_txt}"
        if sec_auth.get("asof_date") is not None and sec_auth.get("accn"):
            bb_note_detail += f" | as-of {sec_auth.get('asof_date')} ({sec_auth.get('form')}/{sec_auth.get('accn')})"
        auth_sz = sec_auth.get("authorization_dollars")
        auth_inc_sz = sec_auth.get("authorization_increase_dollars")
        try:
            if auth_sz is not None and (pd.isna(auth_sz) or float(auth_sz) < 1_000_000):
                auth_sz = None
        except Exception:
            auth_sz = None
        try:
            if auth_inc_sz is not None and (pd.isna(auth_inc_sz) or float(auth_inc_sz) < 1_000_000):
                auth_inc_sz = None
        except Exception:
            auth_inc_sz = None
        sec_kind = str(sec_auth.get("kind") or "").lower()
        sec_snip = str(sec_auth.get("snippet") or "")
        inc_m = re.search(
            r"(?:increas(?:e|ing)|raise(?:d)?|update(?:d)?)?[^.]{0,120}?"
            r"(?:share\s+repurchase|buyback)[^.]{0,120}?authorization[^.]{0,80}?\bto\b\s+\$?\s*"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            sec_snip,
            re.I,
        )
        inc_by_m = re.search(
            r"(?:increas(?:e|ing)|raise(?:d)?|update(?:d)?)?[^.]{0,120}?"
            r"(?:share\s+repurchase|buyback)[^.]{0,120}?authorization[^.]{0,80}?\bby\b\s+\$?\s*"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            sec_snip,
            re.I,
        )
        if inc_by_m is None and sec_auth.get("doc_path"):
            try:
                dp = Path(str(sec_auth.get("doc_path") or ""))
                if dp.exists():
                    raw = dp.read_text(encoding="utf-8", errors="ignore")
                    blob = strip_html(raw) if str(dp.suffix).lower() in {".htm", ".html", ".xml"} else raw
                    blob = re.sub(r"\s+", " ", str(blob or "")).strip()
                    if blob:
                        inc_by_m = re.search(
                            r"(?:increas(?:e|ing)|raise(?:d)?|update(?:d)?)?[^.]{0,180}?"
                            r"(?:share\s+repurchase|buyback)[^.]{0,180}?authorization[^.]{0,100}?\bby\b\s+\$?\s*"
                            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                            blob,
                            re.I,
                        )
            except Exception:
                pass
        if auth_inc_sz is not None and pd.notna(auth_inc_sz):
            try:
                inc_dt = sec_auth.get("asof_date")
                if inc_dt is not None:
                    bb_note_detail += f" | Latest increase by {_money_m(float(auth_inc_sz))} on {inc_dt}"
                else:
                    bb_note_detail += f" | Latest increase by {_money_m(float(auth_inc_sz))}"
            except Exception:
                pass
        if auth_sz is not None and pd.notna(auth_sz):
            try:
                inc_dt = sec_auth.get("asof_date")
                # Prefer "increase to" wording when the source kind or snippet indicates it.
                if "increase" in sec_kind or inc_m:
                    if inc_dt is not None:
                        bb_note_detail += f" | Latest increase to {_money_m(float(auth_sz))} on {inc_dt}"
                    else:
                        bb_note_detail += f" | Latest increase to {_money_m(float(auth_sz))}"
                else:
                    if inc_dt is not None:
                        bb_note_detail += f" | Latest authorization {_money_m(float(auth_sz))} on {inc_dt}"
                    else:
                        bb_note_detail += f" | Latest authorization {_money_m(float(auth_sz))}"
            except Exception:
                pass
        if inc_by_m and (auth_inc_sz is None or pd.isna(auth_inc_sz)):
            try:
                inc_v = float(str(inc_by_m.group(1)).replace(",", ""))
                inc_u = str(inc_by_m.group(2) or "").lower()
                if inc_u in {"billion", "bn"}:
                    inc_v *= 1e9
                elif inc_u in {"million", "m"}:
                    inc_v *= 1e6
                elif inc_v < 2000:
                    inc_v *= 1e6
                if inc_v < 1_000_000:
                    raise ValueError("increase_by too small")
                inc_dt = sec_auth.get("asof_date")
                if inc_dt is not None:
                    bb_note_detail += f" | Latest increase by {_money_m(inc_v)} on {inc_dt}"
                else:
                    bb_note_detail += f" | Latest increase by {_money_m(inc_v)}"
            except Exception:
                pass
        elif inc_m and (auth_sz is None or pd.isna(auth_sz)) and (auth_inc_sz is None or pd.isna(auth_inc_sz)):
            try:
                inc_v = float(str(inc_m.group(1)).replace(",", ""))
                inc_u = str(inc_m.group(2) or "").lower()
                if inc_u in {"billion", "bn"}:
                    inc_v *= 1e9
                elif inc_u in {"million", "m"}:
                    inc_v *= 1e6
                elif inc_v < 2000:
                    inc_v *= 1e6
                if inc_v < 1_000_000:
                    raise ValueError("increase_to too small")
                inc_dt = sec_auth.get("asof_date")
                if inc_dt is not None:
                    bb_note_detail += f" | Latest increase to {_money_m(inc_v)} on {inc_dt}"
                else:
                    bb_note_detail += f" | Latest increase to {_money_m(inc_v)}"
            except Exception:
                pass
        if bb_src and re.search(r"(expect|intend|plan|execute|continue)[^.]{0,120}(repurch|buyback)", str(bb_src), re.I):
            bb_note_detail += " | Continuation mentioned"
        hv_buybacks_note = f"{bb_note_summary}\n{bb_note_detail}"
        if sec_auth.get("doc_path"):
            try:
                sec_doc_path = Path(str(sec_auth.get("doc_path") or ""))
                sec_doc_text = _extract_valuation_filing_doc_text(sec_doc_path) if sec_doc_path.exists() else ""
                sec_doc_text = re.sub(r"\s+", " ", str(sec_doc_text or "")).strip()
                m_hv_direct = re.search(
                    r"\brepurchas\w*\b.{0,240}?([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
                    r".{0,240}?\bfor(?:\s+(?:a\s+)?total\s+of)?\s+\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                    sec_doc_text,
                    re.I,
                )
                if m_hv_direct:
                    hv_shares_override = float(m_hv_direct.group(1)) * (
                        1_000_000.0 if str(m_hv_direct.group(2) or "").lower() in {"million", "m"} else 1.0
                    )
                    hv_amount_override = _parse_buyback_money_local(m_hv_direct.group(3), m_hv_direct.group(4))
                    hv_avg_override = (
                        float(hv_amount_override) / float(hv_shares_override)
                        if hv_amount_override and hv_shares_override
                        else None
                    )
                    current_cash_val = pd.to_numeric(latest_buy_cash_fact, errors="coerce")
                    if pd.notna(current_cash_val):
                        current_cash_val = float(current_cash_val)
                    else:
                        current_cash_val = None
                    if hv_amount_override is not None and (
                        current_cash_val is None
                        or abs(float(current_cash_val) - float(hv_amount_override)) > 5_000_000.0
                    ):
                        hv_buybacks_note = (
                            f"Cash buybacks spent latest quarter {_money_m(hv_amount_override)} | "
                            f"TTM {_money_m(hv_amount_override)} | YoY Δ n/a"
                        )
                        hv_buybacks_note = _format_buyback_note_summary_local(
                            float(hv_amount_override),
                            buyback_cash_ttm_fact,
                            bb_cash_yoy_delta,
                        )
                        hv_buybacks_note += (
                            f" | Latest quarter {_shares_m(hv_shares_override)} at ${float(hv_avg_override):.2f}/share"
                            if hv_avg_override is not None
                            else f" | Latest quarter {_shares_m(hv_shares_override)}"
                        )
                        hv_buybacks_note += f"\n{bb_note_detail}"
            except Exception:
                pass
        if "Cash buybacks not directly observed in quarterly cashflow facts." in hv_buybacks_note and sec_auth.get("doc_path"):
            try:
                sec_doc_path = Path(str(sec_auth.get("doc_path") or ""))
                sec_doc_text = _extract_valuation_filing_doc_text(sec_doc_path) if sec_doc_path.exists() else ""
                m_hv_amt = re.search(
                    r"\brepurchas\w*\b.{0,260}?\bfor(?:\s+(?:a\s+)?total\s+of)?\s+\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                    sec_doc_text,
                    re.I,
                )
                if m_hv_amt:
                    amt_override = float(m_hv_amt.group(1))
                    amt_unit = str(m_hv_amt.group(2) or "").lower()
                    if amt_unit in {"billion", "bn"}:
                        amt_override *= 1e9
                    elif amt_unit in {"million", "m"}:
                        amt_override *= 1e6
                    hv_buybacks_note = (
                        f"Buybacks spent latest quarter {_money_m(amt_override)} | "
                        f"TTM {_money_m(amt_override)} | YoY Δ n/a\n"
                        f"{bb_note_detail}"
                    )
            except Exception:
                pass
        hv_buybacks_note = f"{bb_note_summary}\n{bb_note_detail}"
        if (
            "QoQ +0.000m" in str(hv_buybacks)
            and sec_auth.get("doc_path")
        ):
            try:
                sec_doc_path = Path(str(sec_auth.get("doc_path") or ""))
                sec_doc_text = _extract_valuation_filing_doc_text(sec_doc_path) if sec_doc_path.exists() else ""
                m_hv_sh = re.search(
                    r"\brepurchas\w*\b.{0,260}?([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                    sec_doc_text,
                    re.I,
                )
                if m_hv_sh:
                    sh_override = float(m_hv_sh.group(1)) * (1_000_000.0 if str(m_hv_sh.group(2) or "").lower() in {"million", "m"} else 1.0)
                    hv_buybacks = f"QoQ {_shares_m(sh_override)} | TTM {_shares_m(sh_override)} | YoY Δ n/a"
            except Exception:
                pass

        if div_ps_q is not None:
            dv_note_summary = (
                f"Cash dividends spent latest quarter {_money_m(latest_div_cash_fact)} | "
                f"TTM {_money_m(dividend_cash_ttm_fact)} | YoY Δ {_delta_m(dv_ttm_delta)}"
            )
        else:
            dv_note_summary = (
                historical_implied_dividend_text if (str(ticker or '').upper() != "GPRE" and has_historical_implied_dividend) else no_current_dividend_text
            )
        dv_cont = None
        if dv_src:
            mcont = re.search(
                r"(?:we\s+)?(?:currently\s+)?expect[^.]{0,220}continue[^.]{0,220}dividend[^.]{0,220}\.",
                str(dv_src),
                re.I,
            )
            if mcont:
                dv_cont = mcont.group(0).strip()
        hv_dividends_note = (
            f"{dv_note_summary}\n{dv_cont or (dv_src or 'No continuation text found.')}"
            if div_ps_q is not None
            else no_current_dividend_text
        )

        def _best_cache_root_buyback_execution_local(q_ref: pd.Timestamp) -> Optional[Tuple[float, float, float, Optional[float], str]]:
            if not cache_root.exists():
                return None
            qts = pd.Timestamp(q_ref).normalize()
            ymd_txt = qts.strftime("%Y%m%d")
            best_exec: Optional[Tuple[float, float, float, Optional[float], str]] = None
            for dp in _sec_cache_docs_for_token_local(cache_root, ymd_txt):
                if not dp.is_file():
                    continue
                doc_txt = glx_normalize_text(html.unescape(_extract_valuation_filing_doc_text(dp)).replace("\xa0", " "))
                table_ctx = bool(
                    doc_txt
                    and re.search(
                        r"(?:common stock purchases during the three months ended|issuer purchases of equity securities|average price paid per share)",
                        doc_txt,
                        re.I,
                    )
                )
                if not doc_txt or (not re.search(r"\brepurchas\w*\b", doc_txt, re.I) and not table_ctx):
                    continue
                shares_match = re.search(
                    r"\brepurchased(?:\s+(?:approximately|approx\.?|about))?\s+([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                    doc_txt,
                    re.I,
                )
                amount_match = None
                for amount_pattern in [
                    (
                        r"\brepurchas\w*\b.{0,260}?\bfor(?:\s+(?:a\s+)?total\s+of)?(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                        r"([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
                    ),
                    (
                        r"\brepurchas\w*\b.{0,260}?\bat\s+(?:a\s+)?(?:total\s+)?cost\s+of(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                        r"([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
                    ),
                    (
                        r"\b(?:used|deployed)\b.{0,180}?\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
                        r"[^.]{0,160}?\bto\s+repurchas\w*\b"
                    ),
                ]:
                    amount_match = re.search(amount_pattern, doc_txt, re.I)
                    if amount_match:
                        break
                table_total_match = None
                table_total_matches = (
                    list(
                        re.finditer(
                            r"\b([0-9]{1,3}(?:,[0-9]{3})+)\s*\$\s*([0-9]+(?:\.\d+)?)\s+\1\b",
                            doc_txt,
                            re.I,
                        )
                    )
                    if table_ctx
                    else []
                )
                if table_total_matches:
                    table_total_match = table_total_matches[-1]
                    shares_match = table_total_match
                    amount_match = None
                elif not shares_match or not amount_match:
                    continue
                try:
                    share_val = float(str(shares_match.group(1) or "").replace(",", ""))
                    if str(shares_match.group(2) or "").strip().lower() in {"million", "m"}:
                        share_val *= 1_000_000.0
                except Exception:
                    continue
                avg_val = None
                avg_match = re.search(
                    r"\baverage price(?: paid)?(?: per share| of)\s+\$?\s*([0-9]+(?:\.\d+)?)\b",
                    doc_txt,
                    re.I,
                )
                if avg_match:
                    try:
                        avg_val = float(str(avg_match.group(1) or "").replace(",", ""))
                    except Exception:
                        avg_val = None
                if amount_match is not None:
                    try:
                        amount_val = float(str(amount_match.group(1) or "").replace(",", ""))
                    except Exception:
                        amount_val = None
                    amount_unit = str(amount_match.group(2) or "").strip().lower()
                    if amount_val is not None:
                        if amount_unit in {"billion", "bn"}:
                            amount_val *= 1_000_000_000.0
                        elif amount_unit in {"million", "m"} or amount_val < 2_000.0:
                            amount_val *= 1_000_000.0
                else:
                    try:
                        avg_val = float(str(table_total_match.group(2) or "").replace(",", ""))
                    except Exception:
                        avg_val = avg_val
                    amount_val = (
                        float(share_val) * float(avg_val)
                        if avg_val is not None and float(avg_val) > 0
                        else None
                    )
                if amount_val is None:
                    continue
                score = 10.0
                name_low = dp.name.lower()
                if "10k" in name_low or "10q" in name_low or "-2025" in name_low or "_pbi-" in name_low or "_gpre-" in name_low:
                    score += 5.0
                if table_ctx:
                    score += 3.0
                if avg_val is not None:
                    score += 2.0
                summary_txt = _ensure_terminal_period(
                    f"Repurchased {float(share_val) / 1_000_000.0:,.1f}m shares for ${float(amount_val) / 1_000_000.0:,.1f}m"
                    + (
                        f" with an average price of ${float(avg_val):.2f}/share in Q{((qts.month - 1) // 3) + 1}"
                        if avg_val is not None and float(avg_val) > 0
                        else f" in Q{((qts.month - 1) // 3) + 1}"
                    )
                )
                candidate_exec = (score, float(share_val), float(amount_val), avg_val, summary_txt)
                if best_exec is None or candidate_exec[0] > best_exec[0]:
                    best_exec = candidate_exec
            return best_exec

        def _latest_quarter_convertible_buyback_suffix_local(
            q_ref: pd.Timestamp,
            *,
            buyback_shares_q: Any,
            note_source_text: Any = "",
        ) -> str:
            total_shares_num = pd.to_numeric(buyback_shares_q, errors="coerce")
            if pd.isna(total_shares_num) or float(total_shares_num) <= 0:
                return ""

            def _parse_shares_local(text_in: Any) -> Optional[float]:
                text_blob = glx_normalize_text(str(text_in or ""))
                if not text_blob:
                    return None
                shares_match = re.search(
                    r"\brepurchas(?:e|ed)?(?:\s+of)?(?:\s+approximately)?\s+([0-9]+(?:\.\d+)?)\s*(million|m|billion|bn)?\s+shares\b",
                    text_blob,
                    re.I,
                )
                if not shares_match:
                    return None
                try:
                    shares_val = float(str(shares_match.group(1)).replace(",", ""))
                except Exception:
                    return None
                unit_txt = str(shares_match.group(2) or "").strip().lower()
                if unit_txt in {"million", "m"}:
                    shares_val *= 1_000_000.0
                elif unit_txt in {"billion", "bn"}:
                    shares_val *= 1_000_000_000.0
                return shares_val

            def _suffix_from_text_local(text_in: Any, *, variant_hint: str = "") -> str:
                text_blob = glx_normalize_text(str(text_in or ""))
                text_low = text_blob.lower()
                if not text_low:
                    return ""
                shares_val = None
                bucket = ""
                explicit_patterns = [
                    (
                        r"\bused\s+\$?[0-9][0-9,]*(?:\.\d+)?\s*(?:million|billion|m|bn)?\s+from convertible notes proceeds\b[^.]{0,220}?\bto\s+repurchas(?:e|ed)?\s+([0-9]+(?:\.\d+)?)\s*(million|m|billion|bn)?\s+shares\b",
                        "proceeds",
                    ),
                    (
                        r"\bproceeds funded the repurchase of(?: approximately)?\s+([0-9]+(?:\.\d+)?)\s*(million|m|billion|bn)?\s+shares\b",
                        "proceeds",
                    ),
                    (
                        r"\b(?:in conjunction with|subscription agreements?|subscription transactions?)\b[^.]{0,240}?\brepurchased(?:\s+approximately)?\s+([0-9]+(?:\.\d+)?)\s*(million|m|billion|bn)?\s+shares\b",
                        "concurrent",
                    ),
                    (
                        r"\brepurchased(?:\s+approximately)?\s+([0-9]+(?:\.\d+)?)\s*(million|m|billion|bn)?\s+shares\b[^.]{0,240}?\b(?:20\d{2}\s+notes?|convertible)\b",
                        "concurrent",
                    ),
                ]
                for pattern_txt, bucket_hint in explicit_patterns:
                    shares_match = re.search(pattern_txt, text_blob, re.I)
                    if not shares_match:
                        continue
                    try:
                        shares_val = float(str(shares_match.group(1)).replace(",", ""))
                    except Exception:
                        shares_val = None
                    if shares_val is None:
                        continue
                    unit_txt = str(shares_match.group(2) or "").strip().lower()
                    if unit_txt in {"million", "m"}:
                        shares_val *= 1_000_000.0
                    elif unit_txt in {"billion", "bn"}:
                        shares_val *= 1_000_000_000.0
                    bucket = bucket_hint
                    break
                if shares_val is None and variant_hint in {"convertible_subscription_buyback", "proceeds_buyback"}:
                    shares_val = _parse_shares_local(text_blob)
                    bucket = "proceeds" if "proceeds" in text_low or variant_hint == "proceeds_buyback" else "concurrent"
                if shares_val is None or shares_val <= 0:
                    return ""
                if float(shares_val) > float(total_shares_num) * 1.05:
                    return ""
                shares_txt = f"{abs(float(shares_val)) / 1_000_000.0:,.3f}m shares"
                if bucket == "proceeds":
                    return f"includes {shares_txt} funded with convertible notes proceeds"
                return f"includes {shares_txt} concurrent with convertible notes"

            direct_suffix = _suffix_from_text_local(note_source_text)
            if direct_suffix:
                return direct_suffix

            qn_view_local = _quarter_notes_view(quarter_mode="timestamp")
            best_suffix = ""
            best_key: Tuple[int, float] = (-1, -1.0)
            q_ref_ts = pd.Timestamp(q_ref).normalize()
            if isinstance(qn_view_local, pd.DataFrame) and not qn_view_local.empty:
                q_col_local = "_quarter" if "_quarter" in qn_view_local.columns else _resolve_col(
                    qn_view_local,
                    ["quarter", "quarter_end", "as_of_quarter"],
                )
                subject_col_local = _resolve_col(
                    qn_view_local,
                    ["subject_variant", "_split_focus", "variant", "focus"],
                )
                summary_col_local = _resolve_col(
                    qn_view_local,
                    ["_render_summary", "render_summary", "summary"],
                )
                text_cols_local = [
                    cc
                    for cc in [
                        summary_col_local,
                        _resolve_col(qn_view_local, ["note", "claim", "headline"]),
                        _resolve_col(qn_view_local, ["body", "text_full"]),
                    ]
                    if cc and cc in qn_view_local.columns
                ]
                if q_col_local:
                    for _, rr_local in qn_view_local.iterrows():
                        rr_q = pd.to_datetime(rr_local.get(q_col_local), errors="coerce")
                        if pd.isna(rr_q) or pd.Timestamp(rr_q).normalize() != q_ref_ts:
                            continue
                        subject_variant = str(rr_local.get(subject_col_local) or "").strip().lower() if subject_col_local else ""
                        summary_txt = glx_normalize_text(str(rr_local.get(summary_col_local) or "")) if summary_col_local else ""
                        if not summary_txt:
                            summary_txt = glx_normalize_text(
                                " | ".join(
                                    str(rr_local.get(cc) or "").strip()
                                    for cc in text_cols_local
                                    if str(rr_local.get(cc) or "").strip()
                                )
                            )
                        suffix_txt = _suffix_from_text_local(summary_txt, variant_hint=subject_variant)
                        if not suffix_txt:
                            continue
                        shares_val = _parse_shares_local(summary_txt)
                        variant_score = (
                            2
                            if subject_variant == "convertible_subscription_buyback"
                            else 1 if subject_variant == "proceeds_buyback" else 0
                        )
                        sort_key = (variant_score, float(shares_val or 0.0))
                        if sort_key > best_key:
                            best_key = sort_key
                            best_suffix = suffix_txt
            if best_suffix:
                return best_suffix

            ymd_txt = q_ref_ts.strftime("%Y%m%d")
            for dp_local in _sec_cache_docs_for_token_local(cache_root, ymd_txt):
                if not dp_local.is_file():
                    continue
                try:
                    doc_text_local = _extract_valuation_filing_doc_text(dp_local)
                except Exception:
                    continue
                suffix_txt = _suffix_from_text_local(doc_text_local)
                if not suffix_txt:
                    continue
                shares_val = _parse_shares_local(doc_text_local)
                sort_key = (0, float(shares_val or 0.0))
                if sort_key > best_key:
                    best_key = sort_key
                    best_suffix = suffix_txt
            return best_suffix

        resolved_latest_cap_return = dict(capital_return_resolved.get(q_now) or {})
        if resolved_latest_cap_return:
            bb_q = resolved_latest_cap_return.get("buyback_shares_q")
            latest_buy_cash_fact = resolved_latest_cap_return.get("buyback_cash_q")
            buyback_cash_ttm_fact = resolved_latest_cap_return.get("buyback_cash_ttm")
            bb_avg_price_latest = resolved_latest_cap_return.get("buyback_avg_price")
            buyback_bits: List[str] = []
            if bb_q is not None:
                if is_anf_profile and _anf_buyback_execution_is_year_or_ttm(
                    q_now,
                    resolved_latest_cap_return.get("buyback_note_source"),
                    cash_amount=latest_buy_cash_fact,
                    shares_amount=bb_q,
                ):
                    latest_piece = _anf_format_year_ttm_buyback_summary(
                        q_now,
                        shares_amount=bb_q,
                        cash_amount=latest_buy_cash_fact,
                        avg_price=bb_avg_price_latest,
                    )
                else:
                    latest_piece = f"Latest quarter {_shares_m(bb_q)}"
                    if bb_avg_price_latest is not None:
                        latest_piece += f" at ${float(bb_avg_price_latest):.2f}/share"
                    if latest_buy_cash_fact is not None:
                        latest_piece += f" for {_money_m(latest_buy_cash_fact)}"
                buyback_bits.append(latest_piece)
            convertible_component_suffix = _latest_quarter_convertible_buyback_suffix_local(
                pd.Timestamp(q_now),
                buyback_shares_q=bb_q,
                note_source_text=resolved_latest_cap_return.get("buyback_note_source"),
            )
            if convertible_component_suffix:
                buyback_bits.append(convertible_component_suffix)
            if buyback_cash_ttm_fact is not None:
                buyback_bits.append(f"TTM {_money_m(buyback_cash_ttm_fact)}")
            hv_buybacks = " | ".join([bit for bit in buyback_bits if bit]) or str(resolved_latest_cap_return.get("buybacks_text") or hv_buybacks)
            resolved_bb_summary = str(resolved_latest_cap_return.get("buyback_note_summary") or "").strip()
            use_resolved_execution_note = bool(
                resolved_bb_summary
                and resolved_bb_summary != "No current authorization / remaining-capacity disclosure."
                and re.search(r"\b(post-quarter|after quarter-end|not pro forma-adjusted)\b", resolved_bb_summary, re.I)
            )
            if use_resolved_execution_note:
                hv_buybacks_note = resolved_bb_summary
            else:
                bb_note_bits: List[str] = []
                if bb_remaining is not None:
                    bb_note_bits.append(f"Remaining capacity {_money_m(bb_remaining)}")
                latest_auth_date = sec_auth.get("asof_date")
                auth_blob = glx_normalize_text(" | ".join([str(sec_auth.get("snippet") or ""), str(bb_auth_text or ""), str(bb_note_detail or "")]))
                parsed_increase_val = None
                parsed_increase_kind = ""
                if auth_inc_sz is None or pd.isna(auth_inc_sz):
                    inc_fallback = re.search(
                        r"\b(?:increase(?:d)?|raised?|expanded?)\b[^|]{0,140}?\b(by|to)\b\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                        auth_blob,
                        re.I,
                    )
                    if inc_fallback:
                        try:
                            parsed_increase_val = float(str(inc_fallback.group(2)).replace(",", ""))
                            unit_txt = str(inc_fallback.group(3) or "").lower()
                            if unit_txt in {"billion", "bn"}:
                                parsed_increase_val *= 1e9
                            elif unit_txt in {"million", "m"} or parsed_increase_val < 2000:
                                parsed_increase_val *= 1e6
                            parsed_increase_kind = str(inc_fallback.group(1) or "").lower()
                        except Exception:
                            parsed_increase_val = None
                if auth_inc_sz is not None and pd.notna(auth_inc_sz):
                    bb_note_bits.append(
                        f"Latest increase by {_money_m(float(auth_inc_sz))}"
                        + (f" on {latest_auth_date}" if latest_auth_date else "")
                    )
                elif parsed_increase_val is not None:
                    bb_note_bits.append(
                        f"Latest increase {parsed_increase_kind or 'by'} {_money_m(float(parsed_increase_val))}"
                        + (f" on {latest_auth_date}" if latest_auth_date else "")
                    )
                elif auth_sz is not None and pd.notna(auth_sz):
                    latest_auth_kind = "increase to" if ("increase" in str(sec_auth.get("kind") or "").lower()) else "authorization"
                    bb_note_bits.append(
                        f"Latest {latest_auth_kind} {_money_m(float(auth_sz))}"
                        + (f" on {latest_auth_date}" if latest_auth_date else "")
                    )
                if bb_maturity:
                    bb_note_bits.append(f"Maturity date {bb_maturity}")
                if bb_src and re.search(r"(expect|intend|plan|execute|continue)[^.]{0,120}(repurch|buyback)", str(bb_src), re.I):
                    bb_note_bits.append("Continuation mentioned.")
                hv_buybacks_note = " | ".join([bit for bit in bb_note_bits if bit]) or resolved_bb_summary or "No current authorization / remaining-capacity disclosure."
            latest_div_cash_fact = resolved_latest_cap_return.get("dividend_cash_q")
            dividend_cash_ttm_fact = resolved_latest_cap_return.get("dividend_cash_ttm")
            div_ps_q = resolved_latest_cap_return.get("dividend_ps_q")
            resolved_dividend_text = str(resolved_latest_cap_return.get("dividends_text") or "").strip()
            resolved_dividend_note = str(resolved_latest_cap_return.get("dividend_note_summary") or "").strip()
            div_bits: List[str] = []
            if div_ps_q is not None:
                div_bits.append(f"Latest quarter div/share {_ps(div_ps_q)}")
            if dividend_cash_ttm_fact is not None:
                div_bits.append(f"TTM dividend cash {_money_m(dividend_cash_ttm_fact)}")
            hv_dividends = resolved_dividend_text or " | ".join([bit for bit in div_bits if bit]) or hv_dividends
            if resolved_dividend_note:
                hv_dividends_note = resolved_dividend_note
            elif div_ps_q is not None:
                div_note_txt = str(dv_cont or dv_src or "No continuation text found.").strip()
                div_note_txt = re.split(r";|\bhowever\b", div_note_txt, maxsplit=1, flags=re.I)[0].strip()
                hv_dividends_note = _ensure_terminal_period(div_note_txt)
            else:
                hv_dividends_note = no_current_dividend_text

        def _hidden_value_flag_lines_local(max_items: int = 2) -> List[str]:
            out_lines: List[str] = []
            flag_rows = flags_df.copy() if isinstance(flags_df, pd.DataFrame) and not flags_df.empty else pd.DataFrame()
            if flag_rows.empty:
                try:
                    flag_rows = build_hidden_value_flags(
                        hist=hist if isinstance(hist, pd.DataFrame) and not hist.empty else pd.DataFrame(),
                        adj_metrics=adj_metrics if isinstance(adj_metrics, pd.DataFrame) and not adj_metrics.empty else pd.DataFrame(),
                        leverage_df=leverage_df if isinstance(leverage_df, pd.DataFrame) and not leverage_df.empty else pd.DataFrame(),
                        debt_tranches=debt_tranches if isinstance(debt_tranches, pd.DataFrame) and not debt_tranches.empty else pd.DataFrame(),
                        signals_base=signals_base_df if isinstance(signals_base_df, pd.DataFrame) and not signals_base_df.empty else None,
                        price=float(price) if price not in (None, "") and not pd.isna(price) else None,
                        max_flags=max_items,
                    )
                except Exception:
                    flag_rows = pd.DataFrame()
            if not flag_rows.empty and "as_of_quarter" in flag_rows.columns:
                flag_rows["as_of_quarter"] = pd.to_datetime(flag_rows["as_of_quarter"], errors="coerce")
                latest_flag_q = flag_rows["as_of_quarter"].dropna().max()
                if pd.notna(latest_flag_q):
                    flag_rows = flag_rows[flag_rows["as_of_quarter"] == latest_flag_q].copy()
            if not flag_rows.empty:
                sort_cols = [col for col in ["rank", "score"] if col in flag_rows.columns]
                if sort_cols:
                    ascending = [True if col == "rank" else False for col in sort_cols]
                    flag_rows = flag_rows.sort_values(sort_cols, ascending=ascending, na_position="last")
                for _, flag_row in flag_rows.iterrows():
                    title = glx_normalize_text(str(flag_row.get("title") or flag_row.get("Title") or "")).strip()
                    if not title:
                        continue
                    if title not in out_lines:
                        out_lines.append(title)
                    if len(out_lines) >= max_items:
                        break
            if out_lines:
                return out_lines[:max_items]
            fallback_rows = _build_hidden_value_flags_fallback(flags_audit_df)
            if fallback_rows is None or fallback_rows.empty:
                return []
            for _, flag_row in fallback_rows.iterrows():
                status_txt = glx_normalize_text(str(flag_row.get("Status") or "")).strip().lower()
                blocker_txt = glx_normalize_text(str(flag_row.get("Key blocker") or "")).strip().lower()
                title = glx_normalize_text(str(flag_row.get("Title") or "")).strip()
                if not title or status_txt != "near miss":
                    continue
                if "price-linked" in blocker_txt or "missing price" in blocker_txt:
                    continue
                line = f"{title} (near miss)"
                if line not in out_lines:
                    out_lines.append(line)
                if len(out_lines) >= max_items:
                    break
            return out_lines[:max_items]


    return ValuationHiddenValueStateResult(
        hv_scores=hv_scores,
        hv_obs=hv_obs,
        hv_buybacks=hv_buybacks,
        hv_buybacks_note=hv_buybacks_note,
        hv_dividends=hv_dividends,
        hv_dividends_note=hv_dividends_note,
    )
