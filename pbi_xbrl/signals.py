from __future__ import annotations

import json
import re
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

# Threshold config
MIN_MARGIN_BPS = 300
MIN_FCF_YOY = 0.20
MIN_NET_DEBT_DELTA = -50_000_000
MIN_COV_DELTA = 1.0
STREAK_Q = 3
NEAR_TERM_DROP_PP = 0.05
FCF_STDEV_MAX = 0.04
POS_FCF_RATIO_MIN = 0.75
SHARES_YOY_MAX = -0.02
FCFPS_YOY_MIN = 0.15


def _empty_flags_df() -> pd.DataFrame:
    cols = [
        "rank",
        "flag_code",
        "title",
        "score",
        "severity",
        "as_of_quarter",
        "evidence_1",
        "evidence_2",
        "evidence_3",
        "metrics_json",
    ]
    return pd.DataFrame(columns=cols)


def _empty_flags_audit_df() -> pd.DataFrame:
    cols = [
        "flag_id",
        "flag_name",
        "quarter",
        "inputs_json",
        "input_sources_json",
        "calc",
        "output_value",
        "fcf_yield",
        "threshold",
        "pass_fail",
        "qa_severity",
        "qa_message",
        "source_evidence_json",
    ]
    return pd.DataFrame(columns=cols)


def _empty_flags_recompute_df() -> pd.DataFrame:
    cols = [
        "flag_id",
        "quarter",
        "main_present",
        "recompute_present",
        "main_score",
        "recompute_score",
        "match",
        "qa_severity",
        "qa_message",
    ]
    return pd.DataFrame(columns=cols)


def ttm_sum(series: pd.Series, window: int = 4) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.rolling(window=window, min_periods=window).sum()


def yoy(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    prev = s.shift(4)
    out = (s - prev) / prev.abs()
    out[(prev == 0) | prev.isna() | s.isna()] = pd.NA
    return out


def margin(numer: pd.Series, denom: pd.Series) -> pd.Series:
    n = pd.to_numeric(numer, errors="coerce")
    d = pd.to_numeric(denom, errors="coerce")
    out = n / d
    out[(d == 0) | d.isna() | n.isna()] = pd.NA
    return out


def bps(delta: Optional[float]) -> Optional[float]:
    if delta is None or pd.isna(delta):
        return None
    return float(delta) * 10_000.0


def latest_common_quarter(series_list: Sequence[pd.Series], min_points: int = 1) -> Optional[pd.Timestamp]:
    if not series_list:
        return None
    valid_index: Optional[pd.Index] = None
    for s in series_list:
        if s is None or s.empty:
            return None
        idx = s[s.notna()].index
        valid_index = idx if valid_index is None else valid_index.intersection(idx)
    if valid_index is None or len(valid_index) < min_points:
        return None
    return pd.Timestamp(max(valid_index))


def _series(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns or "quarter" not in df.columns:
        return pd.Series(dtype=float)
    s = pd.to_numeric(df[col], errors="coerce")
    s.index = pd.to_datetime(df["quarter"], errors="coerce")
    s = s[~s.index.isna()]
    return s.sort_index()


def _fmt_pct(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    return f"{float(v) * 100:.1f}%"


def _fmt_bps(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    return f"{float(v):.0f} bps"


def _fmt_m(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    return f"${float(v) / 1e6:,.1f}m"


def _fmt_shares(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    return f"{float(v) / 1e6:,.1f}m sh"


def _fmt_per_share(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    return f"${float(v):,.2f}"


def _fmt_now_ly_delta(now: Optional[float], ly: Optional[float], kind: str = "amount") -> str:
    if kind == "pct":
        now_s = _fmt_pct(now)
        ly_s = _fmt_pct(ly)
        if now is None or ly is None or pd.isna(now) or pd.isna(ly):
            delta_s = "n/a"
        else:
            delta_s = f"{(float(now) - float(ly)) * 100:.1f}pp"
    elif kind == "bps":
        now_s = _fmt_bps(now)
        ly_s = _fmt_bps(ly)
        if now is None or ly is None or pd.isna(now) or pd.isna(ly):
            delta_s = "n/a"
        else:
            delta_s = _fmt_bps(float(now) - float(ly))
    elif kind == "ratio":
        now_s = "n/a" if now is None or pd.isna(now) else f"{float(now):.2f}x"
        ly_s = "n/a" if ly is None or pd.isna(ly) else f"{float(ly):.2f}x"
        if now is None or ly is None or pd.isna(now) or pd.isna(ly):
            delta_s = "n/a"
        else:
            delta_s = f"{float(now) - float(ly):.2f}x"
    elif kind == "shares":
        now_s = _fmt_shares(now)
        ly_s = _fmt_shares(ly)
        if now is None or ly is None or pd.isna(now) or pd.isna(ly):
            delta_s = "n/a"
        else:
            delta_s = _fmt_shares(float(now) - float(ly))
    elif kind == "per_share":
        now_s = _fmt_per_share(now)
        ly_s = _fmt_per_share(ly)
        if now is None or ly is None or pd.isna(now) or pd.isna(ly):
            delta_s = "n/a"
        else:
            delta_s = _fmt_per_share(float(now) - float(ly))
    else:
        now_s = _fmt_m(now)
        ly_s = _fmt_m(ly)
        if now is None or ly is None or pd.isna(now) or pd.isna(ly):
            delta_s = "n/a"
        else:
            delta_s = _fmt_m(float(now) - float(ly))
    return f"now {now_s}, LY {ly_s}, delta {delta_s}"


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _score_int(v: float) -> int:
    return int(round(_clamp(v, 0.0, 100.0)))


def _severity(score: int) -> str:
    if score >= 70:
        return "High"
    if score >= 40:
        return "Med"
    return "Low"


def _num_at(metrics: Dict[str, pd.Series], key: str, q: pd.Timestamp) -> Optional[float]:
    s = metrics.get(key)
    if s is None or s.empty:
        return None
    v = s.get(q)
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _flag_bool_at(metrics: Dict[str, pd.Series], key: str, q: pd.Timestamp) -> bool:
    v = _num_at(metrics, key, q)
    if v is not None:
        return bool(int(v))
    s = metrics.get(key)
    if s is None or s.empty:
        return False
    v2 = pd.to_numeric(s, errors="coerce").dropna()
    if v2.empty:
        return False
    return bool(int(v2.iloc[-1]))


def _latest_metric_quarter(metrics: Dict[str, pd.Series], keys: Sequence[str]) -> Optional[pd.Timestamp]:
    idx = pd.DatetimeIndex([], dtype="datetime64[ns]")
    for key in keys:
        s = metrics.get(key)
        if s is None or s.empty:
            continue
        idx = idx.union(pd.DatetimeIndex(s.index))
    if len(idx) == 0:
        return None
    return pd.Timestamp(idx.max())


def _near_term_pct_series(debt_tranches: pd.DataFrame) -> pd.Series:
    if debt_tranches is None or debt_tranches.empty:
        return pd.Series(dtype=float)
    df = debt_tranches.copy()
    if "quarter" not in df.columns or "amount" not in df.columns:
        return pd.Series(dtype=float)
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df = df[df["quarter"].notna()].copy()
    if df.empty:
        return pd.Series(dtype=float)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"].notna()].copy()
    if df.empty:
        return pd.Series(dtype=float)

    if "maturity_year" in df.columns:
        df["maturity_year"] = pd.to_numeric(df["maturity_year"], errors="coerce")
    else:
        df["maturity_year"] = pd.NA

    text_a = df["tranche_name"].astype(str) if "tranche_name" in df.columns else pd.Series([""] * len(df), index=df.index)
    text_b = df["row_text"].astype(str) if "row_text" in df.columns else pd.Series([""] * len(df), index=df.index)
    text = (text_a.fillna("") + " " + text_b.fillna("")).str.strip()

    def _parse_year(s: str) -> Optional[int]:
        if not s:
            return None
        m = re.search(r"(?:due|matures|maturing|maturity)\s*(?:in\s*)?(20\d{2})", s, flags=re.I)
        if m:
            return int(m.group(1))
        m2 = re.search(r"(20\d{2})", s)
        if m2:
            return int(m2.group(1))
        return None

    parsed = pd.to_numeric(text.apply(_parse_year), errors="coerce")
    maturity_used = df["maturity_year"].copy()
    maturity_used[maturity_used.isna()] = parsed[maturity_used.isna()]
    df["maturity_year_parsed"] = parsed
    df["maturity_year_used"] = maturity_used

    total = df.groupby("quarter")["amount"].sum()
    near = (
        df[df["maturity_year_used"].notna() & (df["maturity_year_used"] <= (df["quarter"].dt.year + 2))]
        .groupby("quarter")["amount"]
        .sum()
    )
    out = near.reindex(total.index).fillna(0.0) / total
    out[(total == 0) | total.isna()] = pd.NA
    return out.sort_index()


@dataclass
class _Flag:
    flag_code: str
    title: str
    score: int
    as_of_quarter: str
    evidence_1: str
    evidence_2: str
    evidence_3: str
    metrics_json: str

    def to_row(self) -> Dict[str, Any]:
        return {
            "flag_code": self.flag_code,
            "title": self.title,
            "score": self.score,
            "severity": _severity(self.score),
            "as_of_quarter": self.as_of_quarter,
            "evidence_1": self.evidence_1,
            "evidence_2": self.evidence_2,
            "evidence_3": self.evidence_3,
            "metrics_json": self.metrics_json,
        }


def build_signals_base(
    hist: pd.DataFrame,
    adj_metrics: Optional[pd.DataFrame] = None,
    leverage_df: Optional[pd.DataFrame] = None,
    debt_tranches: Optional[pd.DataFrame] = None,
    price: Optional[float] = None,
) -> pd.DataFrame:
    h = hist.copy() if hist is not None else pd.DataFrame()
    if h.empty or "quarter" not in h.columns:
        return pd.DataFrame()
    h["quarter"] = pd.to_datetime(h["quarter"], errors="coerce")
    h = h[h["quarter"].notna()].sort_values("quarter")
    if h.empty:
        return pd.DataFrame()

    lev = leverage_df.copy() if leverage_df is not None else pd.DataFrame()
    if not lev.empty and "quarter" in lev.columns:
        lev["quarter"] = pd.to_datetime(lev["quarter"], errors="coerce")
        lev = lev[lev["quarter"].notna()].sort_values("quarter")

    adj = adj_metrics.copy() if adj_metrics is not None else pd.DataFrame()
    if not adj.empty and "quarter" in adj.columns:
        adj["quarter"] = pd.to_datetime(adj["quarter"], errors="coerce")
        adj = adj[adj["quarter"].notna()].sort_values("quarter")

    rev_q = _series(h, "revenue")
    cfo_q = _series(h, "cfo")
    capex_q = _series(h, "capex")
    cash_q = _series(h, "cash")
    debt_core_q = _series(h, "debt_core")
    shares_outstanding_q = _series(h, "shares_outstanding")
    shares_diluted_q = _series(h, "shares_diluted")
    ebit_q = _series(h, "ebit")
    if ebit_q.empty:
        ebit_q = _series(h, "op_income")
    if ebit_q.empty:
        ebit_q = _series(h, "operating_income")
    ebitda_q = _series(h, "ebitda")
    market_cap_q = _series(h, "market_cap")
    price_q = _series(h, "price")
    if price_q.empty and price is not None:
        try:
            px = float(price)
            if pd.notna(px) and px > 0:
                price_q = pd.Series(px, index=rev_q.index)
        except Exception:
            pass
    int_exp_q = _series(h, "interest_expense_net")
    dividend_q = pd.Series(dtype=float)
    for _c in [
        "common_stock_dividends_paid",
        "payments_of_dividends_common_stock",
    ]:
        s_div = _series(h, _c)
        if not s_div.empty:
            dividend_q = s_div
            break

    fcf_q = cfo_q - capex_q
    revenue_ttm = ttm_sum(rev_q)
    cfo_ttm = ttm_sum(cfo_q)
    capex_ttm = ttm_sum(capex_q)
    fcf_ttm = ttm_sum(fcf_q)
    revenue_ttm_yoy = yoy(revenue_ttm)
    fcf_ttm_yoy = yoy(fcf_ttm)
    fcf_margin_ttm = margin(fcf_ttm, revenue_ttm)
    fcf_margin_q = margin(fcf_q, rev_q)

    ebit_ttm = _series(lev, "ebit_ttm")
    if ebit_ttm.empty:
        ebit_ttm = ttm_sum(ebit_q)

    ebitda_ttm = _series(lev, "ebitda_ttm")
    if ebitda_ttm.empty:
        ebitda_ttm = ttm_sum(ebitda_q)

    adj_ebitda_ttm_raw = _series(lev, "adj_ebitda_ttm")
    adj_fallback = adj_ebitda_ttm_raw.empty
    adj_ebitda_ttm = adj_ebitda_ttm_raw.copy()
    adj_margin_ttm = margin(adj_ebitda_ttm, revenue_ttm)
    adj_margin_ttm_yoy_bps = (adj_margin_ttm - adj_margin_ttm.shift(4)) * 10_000.0
    ebit_growth_yoy = yoy(ebit_ttm)
    ebitda_growth_yoy = yoy(ebitda_ttm)

    net_debt = _series(lev, "corporate_net_debt")
    if net_debt.empty and not debt_core_q.empty and not cash_q.empty:
        net_debt = debt_core_q - cash_q
    net_debt_yoy_delta = net_debt - net_debt.shift(4)
    net_debt_prev = net_debt.shift(4)
    debt_drop_pct = (net_debt_prev - net_debt) / net_debt_prev.abs()
    debt_drop_pct[(net_debt_prev == 0) | net_debt_prev.isna()] = pd.NA

    cov_pnl = _series(lev, "interest_coverage_pnl")
    cov_cash = _series(lev, "interest_coverage_cash")
    int_exp_ttm = _series(lev, "interest_expense_net_ttm")
    if int_exp_ttm.empty:
        int_exp_ttm = ttm_sum(int_exp_q)
    int_exp_yoy = yoy(int_exp_ttm)
    int_exp_yoy_delta = int_exp_ttm - int_exp_ttm.shift(4)
    int_paid_ttm = _series(lev, "interest_paid_ttm")
    cov_pnl_yoy_delta = cov_pnl - cov_pnl.shift(4)
    cov_cash_yoy_delta = cov_cash - cov_cash.shift(4)
    interest_coverage = cov_pnl.copy()
    if interest_coverage.empty:
        denom = pd.to_numeric(int_exp_ttm, errors="coerce").abs()
        num = pd.to_numeric(ebit_ttm, errors="coerce")
        interest_coverage = num / denom
        interest_coverage[(denom == 0) | denom.isna() | num.isna()] = pd.NA

    revolver_commitment = _series(lev, "revolver_commitment")
    revolver_drawn = _series(lev, "revolver_drawn")
    revolver_availability = _series(lev, "revolver_availability")
    revolver_availability_yoy_delta = revolver_availability - revolver_availability.shift(4)
    liquidity = _series(lev, "liquidity")
    liquidity_yoy_delta = liquidity - liquidity.shift(4)

    shares_out = shares_outstanding_q.copy()
    shares_fallback = shares_out.empty
    if shares_out.empty:
        shares_out = shares_diluted_q.copy()
    shares_yoy = yoy(shares_out)
    fcf_per_share_ttm = fcf_ttm / shares_out
    fcf_per_share_ttm[(shares_out == 0) | shares_out.isna()] = pd.NA
    fcf_per_share_ttm_yoy = yoy(fcf_per_share_ttm)

    if market_cap_q.empty and not price_q.empty and not shares_out.empty:
        market_cap_q = price_q * shares_out
    fcf_yield = fcf_ttm / market_cap_q
    fcf_yield[(market_cap_q <= 0) | market_cap_q.isna() | fcf_ttm.isna()] = pd.NA
    dividend_ttm = ttm_sum(dividend_q)
    dividend_ttm_yoy = yoy(dividend_ttm)
    dividend_ps_q = dividend_q / shares_out
    dividend_ps_q[(shares_out == 0) | shares_out.isna() | dividend_q.isna()] = pd.NA
    dividend_ps_yoy = yoy(dividend_ps_q)
    dividend_ps_qoq = (dividend_ps_q / dividend_ps_q.shift(1)) - 1.0
    dividend_ps_qoq[(dividend_ps_q.shift(1) == 0) | dividend_ps_q.shift(1).isna() | dividend_ps_q.isna()] = pd.NA
    dividend_yield = dividend_ttm / market_cap_q
    dividend_yield[(market_cap_q <= 0) | market_cap_q.isna() | dividend_ttm.isna()] = pd.NA

    eps = 1e-9
    pos_fcf_ratio = fcf_ttm / ebit_ttm
    pos_fcf_ratio[(ebit_ttm.abs() < eps) | ebit_ttm.isna() | fcf_ttm.isna()] = pd.NA
    pos_fcf_ratio.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
    fcf_ttm_pos_years = (fcf_ttm > 0).astype(float).rolling(4, min_periods=4).sum()
    fcf_ttm_pos_years[fcf_ttm.isna()] = pd.NA

    leverage_ratio = net_debt / ebitda_ttm
    leverage_ratio[(ebitda_ttm <= 0) | ebitda_ttm.isna() | net_debt.isna()] = pd.NA

    near_term_pct = _near_term_pct_series(debt_tranches if debt_tranches is not None else pd.DataFrame())
    near_term_pct_yoy_delta = near_term_pct - near_term_pct.shift(4)

    adj_ebitda_q = _series(adj, "adj_ebitda")
    adj_ebit_q = _series(adj, "adj_ebit")
    adj_eps_q = _series(adj, "adj_eps")
    adj_fcf_q = _series(adj, "adj_fcf")

    series_map: Dict[str, pd.Series] = {
        "revenue": rev_q,
        "cfo": cfo_q,
        "capex": capex_q,
        "cash": cash_q,
        "debt_core": debt_core_q,
        "shares_outstanding": shares_outstanding_q,
        "shares_diluted": shares_diluted_q,
        "ebitda_ttm": ebitda_ttm,
        "adj_ebitda_ttm_raw": adj_ebitda_ttm_raw,
        "adj_ebitda_ttm": adj_ebitda_ttm,
        "corporate_net_debt": net_debt,
        "interest_expense_net_ttm": int_exp_ttm,
        "int_exp_ttm": int_exp_ttm,
        "interest_paid_ttm": int_paid_ttm,
        "interest_coverage_pnl": cov_pnl,
        "interest_coverage_cash": cov_cash,
        "revolver_commitment": revolver_commitment,
        "revolver_drawn": revolver_drawn,
        "revolver_availability": revolver_availability,
        "liquidity": liquidity,
        "adj_ebitda_q": adj_ebitda_q,
        "adj_ebit_q": adj_ebit_q,
        "adj_eps_q": adj_eps_q,
        "adj_fcf_q": adj_fcf_q,
        "ebit_ttm": ebit_ttm,
        "cfo_ttm": cfo_ttm,
        "capex_ttm": capex_ttm,
        "fcf_q": fcf_q,
        "fcf_ttm": fcf_ttm,
        "revenue_ttm": revenue_ttm,
        "revenue_ttm_yoy": revenue_ttm_yoy,
        "fcf_ttm_yoy": fcf_ttm_yoy,
        "fcf_margin_q": fcf_margin_q,
        "fcf_margin_ttm": fcf_margin_ttm,
        "adj_margin_ttm": adj_margin_ttm,
        "adj_margin_ttm_yoy_bps": adj_margin_ttm_yoy_bps,
        "ebit_growth_yoy": ebit_growth_yoy,
        "ebitda_growth_yoy": ebitda_growth_yoy,
        "net_debt_yoy_delta": net_debt_yoy_delta,
        "debt_drop_pct": debt_drop_pct,
        "leverage_ratio": leverage_ratio,
        "interest_coverage": interest_coverage,
        "cov_pnl_yoy_delta": cov_pnl_yoy_delta,
        "cov_cash_yoy_delta": cov_cash_yoy_delta,
        "int_exp_yoy": int_exp_yoy,
        "int_exp_yoy_delta": int_exp_yoy_delta,
        "market_cap": market_cap_q,
        "fcf_yield": fcf_yield,
        "dividend_ttm": dividend_ttm,
        "dividend_ttm_yoy": dividend_ttm_yoy,
        "dividend_ps_q": dividend_ps_q,
        "dividend_ps_yoy": dividend_ps_yoy,
        "dividend_ps_qoq": dividend_ps_qoq,
        "dividend_yield": dividend_yield,
        "pos_fcf_ratio": pos_fcf_ratio,
        "fcf_ttm_pos_years": fcf_ttm_pos_years,
        "revolver_availability_yoy_delta": revolver_availability_yoy_delta,
        "liquidity_yoy_delta": liquidity_yoy_delta,
        "shares_out": shares_out,
        "shares_yoy": shares_yoy,
        "fcf_per_share_ttm": fcf_per_share_ttm,
        "fcf_per_share_ttm_yoy": fcf_per_share_ttm_yoy,
        "near_term_pct": near_term_pct,
        "near_term_pct_yoy_delta": near_term_pct_yoy_delta,
    }

    idx = pd.DatetimeIndex([], dtype="datetime64[ns]")
    for s in series_map.values():
        if s is None or s.empty:
            continue
        idx = idx.union(pd.DatetimeIndex(s.index))
    if len(idx) == 0:
        return pd.DataFrame()
    idx = pd.DatetimeIndex(sorted(set(idx)))

    base = pd.DataFrame(index=idx)
    for col, s in series_map.items():
        if s is None or s.empty:
            base[col] = pd.NA
        else:
            base[col] = s.reindex(base.index)
    base["adj_fallback"] = float(1 if adj_fallback else 0)
    base["shares_fallback"] = float(1 if shares_fallback else 0)
    base = base.reset_index().rename(columns={"index": "quarter"})
    return base


def _metrics_from_signals_base(base: pd.DataFrame) -> Dict[str, pd.Series]:
    if base is None or base.empty or "quarter" not in base.columns:
        return {}
    metrics: Dict[str, pd.Series] = {}
    keys = [
        "revenue_ttm",
        "revenue_ttm_yoy",
        "cfo_ttm",
        "capex_ttm",
        "fcf_q",
        "fcf_ttm",
        "fcf_ttm_yoy",
        "fcf_margin_q",
        "fcf_margin_ttm",
        "ebitda_ttm",
        "adj_ebitda_ttm",
        "corporate_net_debt",
        "ebit_ttm",
        "ebit_growth_yoy",
        "ebitda_growth_yoy",
        "adj_margin_ttm",
        "adj_margin_ttm_yoy_bps",
        "net_debt_yoy_delta",
        "debt_drop_pct",
        "leverage_ratio",
        "interest_coverage",
        "cov_pnl_yoy_delta",
        "cov_cash_yoy_delta",
        "int_exp_ttm",
        "int_exp_yoy",
        "int_exp_yoy_delta",
        "int_paid_ttm",
        "market_cap",
        "fcf_yield",
        "dividend_ttm",
        "dividend_ttm_yoy",
        "dividend_ps_q",
        "dividend_ps_yoy",
        "dividend_ps_qoq",
        "dividend_yield",
        "pos_fcf_ratio",
        "fcf_ttm_pos_years",
        "revolver_availability",
        "revolver_availability_yoy_delta",
        "liquidity",
        "liquidity_yoy_delta",
        "shares_out",
        "shares_yoy",
        "fcf_per_share_ttm",
        "fcf_per_share_ttm_yoy",
        "near_term_pct",
        "near_term_pct_yoy_delta",
        "adj_fallback",
        "shares_fallback",
    ]
    for key in keys:
        metrics[key] = _series(base, key)
    return metrics


FLAG_DEFS: Dict[str, Dict[str, Any]] = {
    "A": {
        "name": "EBIT/EBITDA growth with shrinking share count",
        "required": ["ebit_growth_yoy", "ebitda_growth_yoy", "shares_yoy", "ebit_ttm", "ebitda_ttm", "shares_out"],
        "threshold": "ebit_growth_yoy > 0.25 && ebitda_growth_yoy > 0.20 && shares_yoy <= -0.02",
        "calc": "A = (EBIT TTM YoY > 25%) and (EBITDA TTM YoY > 20%) and (shares YoY <= -2%)",
        "cross_check": True,
    },
    "B": {
        "name": "Adjusted margin expansion with streak",
        "required": ["adj_margin_ttm", "adj_margin_ttm_yoy_bps"],
        "threshold": "adj_margin_ttm >= 0.20 && margin_yoy_bps >= 200 && margin_streak >= 2",
        "calc": "B = (Adj EBITDA margin TTM >= 20%) and (YoY margin >= 200 bps) and streak >= 2q",
        "cross_check": False,
    },
    "C": {
        "name": "Cashflow quality and yield support",
        "required": ["fcf_ttm_pos_years", "pos_fcf_ratio", "fcf_yield"],
        "threshold": "fcf_ttm_pos_years >= 1 && pos_fcf_ratio >= 0.75 && fcf_yield >= 0.15",
        "calc": "C = positive FCF-years + FCF/EBIT ratio + FCF yield trigger",
        "cross_check": True,
    },
    "D": {
        "name": "Deleveraging with acceptable leverage",
        "required": ["debt_drop_pct", "leverage_ratio", "corporate_net_debt", "ebitda_ttm"],
        "threshold": "debt_drop_pct >= 0.10 && leverage_ratio <= 3.0",
        "calc": "D = debt drop >= 10% and leverage <= 3.0x",
        "cross_check": True,
    },
    "E": {
        "name": "Coverage strength with cheap cashflow",
        "required": ["interest_coverage", "fcf_yield"],
        "threshold": "interest_coverage >= 3.0 && fcf_yield >= 0.20",
        "calc": "E = coverage >= 3.0x and FCF yield >= 20%",
        "cross_check": True,
    },
    "F": {
        "name": "Share count reduction (buyback support)",
        "required": ["shares_yoy", "shares_out", "fcf_per_share_ttm_yoy"],
        "threshold": "shares_yoy <= -0.02 (bonus if fcf_per_share_ttm_yoy >= 0)",
        "calc": "F = share count YoY <= -2% with optional FCF/share support",
        "cross_check": True,
    },
    "G": {
        "name": "Dividend support (yield or growth)",
        "required": ["dividend_ps_q"],
        "threshold": "dividend_yield >= 0.03 OR dividend_ps_yoy > 0 OR dividend_ps_qoq > 0",
        "calc": "G = dividend support from yield and/or QoQ/YoY dividend/share increase",
        "cross_check": True,
    },
}


def _compute_flag_independent(code: str, m: Dict[str, pd.Series], q: pd.Timestamp) -> Tuple[bool, Optional[int], Dict[str, Any]]:
    if code == "A":
        ebit_growth_yoy = _num_at(m, "ebit_growth_yoy", q)
        ebitda_growth_yoy = _num_at(m, "ebitda_growth_yoy", q)
        shares_yoy = _num_at(m, "shares_yoy", q)
        if ebit_growth_yoy is None or ebitda_growth_yoy is None or shares_yoy is None:
            return False, None, {}
        trigger = (ebit_growth_yoy > 0.25) and (ebitda_growth_yoy > 0.20) and (shares_yoy <= -0.02)
        if not trigger:
            return False, None, {
                "ebit_growth_yoy": ebit_growth_yoy,
                "ebitda_growth_yoy": ebitda_growth_yoy,
                "shares_yoy": shares_yoy,
            }
        s_ebit = _clamp((ebit_growth_yoy - 0.25) / 0.50 + 0.30) * 40.0
        s_ebitda = _clamp((ebitda_growth_yoy - 0.20) / 0.40 + 0.30) * 40.0
        s_shares = _clamp((abs(shares_yoy) - 0.02) / 0.05 + 0.20) * 20.0
        score = _score_int(s_ebit + s_ebitda + s_shares)
        return True, score, {
            "ebit_growth_yoy": ebit_growth_yoy,
            "ebitda_growth_yoy": ebitda_growth_yoy,
            "shares_yoy": shares_yoy,
        }
    if code == "B":
        adj_margin_ttm = _num_at(m, "adj_margin_ttm", q)
        margin_yoy_bps = _num_at(m, "adj_margin_ttm_yoy_bps", q)
        if adj_margin_ttm is None or margin_yoy_bps is None:
            return False, None, {}
        adj_hist = m["adj_margin_ttm"][m["adj_margin_ttm"].index <= q].dropna()
        streak = _streak_len(adj_hist, q)
        trigger = (adj_margin_ttm >= 0.20) and (margin_yoy_bps >= 200.0) and (streak >= 2)
        if not trigger:
            return False, None, {
                "adj_margin_ttm": adj_margin_ttm,
                "margin_yoy_bps": margin_yoy_bps,
                "margin_streak": streak,
            }
        s1 = 40.0 * _clamp((adj_margin_ttm - 0.20) / 0.15 + 0.30)
        s2 = 35.0 * _clamp((margin_yoy_bps - 200.0) / 500.0 + 0.30)
        score = _score_int(s1 + s2)
        return True, score, {
            "adj_margin_ttm": adj_margin_ttm,
            "margin_yoy_bps": margin_yoy_bps,
            "margin_streak": streak,
        }
    if code == "C":
        fcf_ttm_pos_years = _num_at(m, "fcf_ttm_pos_years", q)
        pos_fcf_ratio = _num_at(m, "pos_fcf_ratio", q)
        fcf_yield = _num_at(m, "fcf_yield", q)
        if fcf_ttm_pos_years is None or pos_fcf_ratio is None or fcf_yield is None:
            return False, None, {}
        trigger = (fcf_ttm_pos_years >= 1.0) and (pos_fcf_ratio >= 0.75) and (fcf_yield >= 0.15)
        if not trigger:
            return False, None, {
                "fcf_ttm_pos_years": fcf_ttm_pos_years,
                "pos_fcf_ratio": pos_fcf_ratio,
                "fcf_yield": fcf_yield,
            }
        score = _score_int(
            30.0 * _clamp((fcf_ttm_pos_years - 1.0) / 3.0 + 0.25)
            + 35.0 * _clamp((pos_fcf_ratio - 0.75) / 0.75 + 0.25)
            + 35.0 * _clamp((fcf_yield - 0.15) / 0.25 + 0.25)
        )
        return True, score, {
            "fcf_ttm_pos_years": fcf_ttm_pos_years,
            "pos_fcf_ratio": pos_fcf_ratio,
            "fcf_yield": fcf_yield,
        }
    if code == "D":
        debt_drop_pct = _num_at(m, "debt_drop_pct", q)
        leverage_ratio = _num_at(m, "leverage_ratio", q)
        if debt_drop_pct is None or leverage_ratio is None:
            return False, None, {}
        trigger = (debt_drop_pct >= 0.10) and (leverage_ratio <= 3.0)
        if not trigger:
            return False, None, {"debt_drop_pct": debt_drop_pct, "leverage_ratio": leverage_ratio}
        score = _score_int(
            50.0 * _clamp((debt_drop_pct - 0.10) / 0.25 + 0.25)
            + 50.0 * _clamp((3.0 - leverage_ratio) / 2.0 + 0.25)
        )
        return True, score, {"debt_drop_pct": debt_drop_pct, "leverage_ratio": leverage_ratio}
    if code == "E":
        interest_coverage = _num_at(m, "interest_coverage", q)
        fcf_yield = _num_at(m, "fcf_yield", q)
        if interest_coverage is None or fcf_yield is None:
            return False, None, {}
        trigger = (interest_coverage >= 3.0) and (fcf_yield >= 0.20)
        if not trigger:
            return False, None, {"interest_coverage": interest_coverage, "fcf_yield": fcf_yield}
        score = _score_int(
            50.0 * _clamp((interest_coverage - 3.0) / 3.0 + 0.25)
            + 50.0 * _clamp((fcf_yield - 0.20) / 0.30 + 0.25)
        )
        return True, score, {"interest_coverage": interest_coverage, "fcf_yield": fcf_yield}
    if code == "F":
        shares_yoy = _num_at(m, "shares_yoy", q)
        fcfps_yoy = _num_at(m, "fcf_per_share_ttm_yoy", q)
        if shares_yoy is None:
            return False, None, {}
        trigger = shares_yoy <= -0.02
        if not trigger:
            return False, None, {"shares_yoy": shares_yoy, "fcf_per_share_ttm_yoy": fcfps_yoy}
        s_shares = 70.0 * _clamp((abs(shares_yoy) - 0.02) / 0.08 + 0.15)
        s_fcfps = 30.0 * _clamp(((fcfps_yoy if fcfps_yoy is not None else 0.0) + 0.05) / 0.25)
        score = _score_int(s_shares + s_fcfps)
        return True, score, {"shares_yoy": shares_yoy, "fcf_per_share_ttm_yoy": fcfps_yoy}
    if code == "G":
        dividend_yield = _num_at(m, "dividend_yield", q)
        dividend_ps_yoy = _num_at(m, "dividend_ps_yoy", q)
        dividend_ps_qoq = _num_at(m, "dividend_ps_qoq", q)
        if dividend_yield is None and dividend_ps_yoy is None and dividend_ps_qoq is None:
            return False, None, {}
        trigger = (
            (dividend_yield is not None and dividend_yield >= 0.03)
            or (dividend_ps_yoy is not None and dividend_ps_yoy > 0)
            or (dividend_ps_qoq is not None and dividend_ps_qoq > 0)
        )
        if not trigger:
            return False, None, {
                "dividend_yield": dividend_yield,
                "dividend_ps_yoy": dividend_ps_yoy,
                "dividend_ps_qoq": dividend_ps_qoq,
            }
        score = _score_int(
            40.0 * _clamp((((dividend_yield if dividend_yield is not None else 0.0) - 0.02) / 0.05) + 0.25)
            + 35.0 * _clamp((((dividend_ps_yoy if dividend_ps_yoy is not None else 0.0) - 0.00) / 0.15) + 0.25)
            + 25.0 * _clamp((((dividend_ps_qoq if dividend_ps_qoq is not None else 0.0) - 0.00) / 0.08) + 0.25)
        )
        return True, score, {
            "dividend_yield": dividend_yield,
            "dividend_ps_yoy": dividend_ps_yoy,
            "dividend_ps_qoq": dividend_ps_qoq,
        }
    return False, None, {}


def _flag_quarter(code: str, m: Dict[str, pd.Series]) -> Optional[pd.Timestamp]:
    req = FLAG_DEFS.get(code, {}).get("required", [])
    if not req:
        return None
    return _latest_metric_quarter(m, req)


def _compute_flags_and_audit(
    base_df: pd.DataFrame,
    max_flags: int,
    main_metrics: Optional[Dict[str, pd.Series]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    metrics_recompute = _metrics_from_signals_base(base_df)
    if not metrics_recompute:
        return _empty_flags_df(), _empty_flags_audit_df(), _empty_flags_recompute_df()
    metrics_main = main_metrics if main_metrics else metrics_recompute

    flag_order = list(FLAG_DEFS.keys())
    fn_map = {"A": _flag_a, "B": _flag_b, "C": _flag_c, "D": _flag_d, "E": _flag_e, "F": _flag_f, "G": _flag_g}
    main_flags: Dict[str, Optional[_Flag]] = {}
    flag_errors: Dict[str, str] = {}
    for code, fn in fn_map.items():
        try:
            main_flags[code] = fn(metrics_main)
        except Exception as exc:
            main_flags[code] = None
            tb_lines = [ln.strip() for ln in traceback.format_exc(limit=4).splitlines() if ln.strip()]
            tail = " | ".join(tb_lines[-3:]) if tb_lines else ""
            flag_errors[code] = f"{type(exc).__name__}: {exc}" + (f" | {tail}" if tail else "")

    audit_rows: List[Dict[str, Any]] = []
    recompute_rows: List[Dict[str, Any]] = []
    for code in flag_order:
        spec = FLAG_DEFS[code]
        parse_error = flag_errors.get(code)
        q_main = _flag_quarter(code, metrics_main)
        q_recompute = _flag_quarter(code, metrics_recompute)
        q = q_main if q_main is not None else q_recompute
        if q is None:
            fail_msg = "No common quarter for required inputs."
            if parse_error:
                fail_msg = f"FAIL: parse_error {parse_error}"
            audit_rows.append(
                {
                    "flag_id": code,
                    "flag_name": spec["name"],
                    "quarter": None,
                    "inputs_json": "{}",
                    "input_sources_json": "{}",
                    "calc": spec["calc"],
                    "output_value": None,
                    "fcf_yield": None,
                    "threshold": spec["threshold"],
                    "pass_fail": False,
                    "qa_severity": "FAIL",
                    "qa_message": fail_msg,
                    "source_evidence_json": "[]",
                }
            )
            recompute_rows.append(
                {
                    "flag_id": code,
                    "quarter": None,
                    "main_present": main_flags.get(code) is not None,
                    "recompute_present": False,
                    "main_score": 0 if parse_error else (main_flags.get(code).score if main_flags.get(code) is not None else None),
                    "recompute_score": None,
                    "match": (main_flags.get(code) is None),
                    "qa_severity": "FAIL" if (parse_error or main_flags.get(code) is not None) else "",
                    "qa_message": f"FAIL: parse_error {parse_error}" if parse_error else "No recompute quarter available.",
                }
            )
            continue

        req = spec["required"]
        inputs = {k: _num_at(metrics_main, k, q) for k in req}
        input_sources = {k: {"sheet": "Hidden_Value_Base", "field": k} for k in req}
        source_evidence = [{"sheet": "Hidden_Value_Base", "field": k} for k in req]
        missing = [k for k, v in inputs.items() if v is None or pd.isna(v)]
        fcf_yield_value = _num_at(metrics_main, "fcf_yield", q)

        q_align_ok = True
        has_q = any(k.endswith("_q") for k in req)
        has_ttm = any("_ttm" in k for k in req)
        has_ytd = any("_ytd" in k for k in req)
        if has_q and has_ttm and not has_ytd:
            q_align_ok = False

        qa_sev = ""
        qa_msg: List[str] = []
        if missing:
            if set(missing) == {"fcf_yield"}:
                qa_sev = "WARN"
            else:
                qa_sev = "FAIL"
            qa_msg.append(f"Missing required inputs: {', '.join(missing)}")
            if "fcf_yield" in missing:
                qa_msg.append("fcf_yield needs market_cap or --price input (used with shares).")
        if not q_align_ok:
            qa_sev = "FAIL"
            qa_msg.append("Quarter alignment unclear (mixed TTM/Q without explicit marker).")

        # Fallback warning only when no cross-check.
        adj_fb = _flag_bool_at(metrics_main, "adj_fallback", q)
        if adj_fb and not spec.get("cross_check", False):
            if qa_sev != "FAIL":
                qa_sev = "WARN"
            qa_msg.append("Fallback source used without cross-check.")
        if parse_error:
            qa_sev = "FAIL"
            qa_msg.append(f"FAIL: parse_error {parse_error}")

        audit_rows.append(
            {
                "flag_id": code,
                "flag_name": spec["name"],
                "quarter": q.date(),
                "inputs_json": json.dumps(inputs, separators=(",", ":")),
                "input_sources_json": json.dumps(input_sources, separators=(",", ":")),
                "calc": spec["calc"],
                "output_value": int(1 if main_flags.get(code) is not None else 0),
                "fcf_yield": fcf_yield_value,
                "threshold": spec["threshold"],
                "pass_fail": bool(main_flags.get(code) is not None),
                "qa_severity": qa_sev,
                "qa_message": " | ".join(qa_msg),
                "source_evidence_json": json.dumps(source_evidence, separators=(",", ":")),
            }
        )

        main_obj = main_flags.get(code)
        main_present = main_obj is not None
        rec_q = q_recompute if q_recompute is not None else q
        trig_recompute = False
        ind_score: Optional[int] = None
        if rec_q is not None:
            trig_recompute, ind_score, _ = _compute_flag_independent(code, metrics_recompute, rec_q)
        recompute_present = bool(trig_recompute)
        main_score = 0 if parse_error else (main_obj.score if main_obj is not None else None)
        score_match = (main_score == ind_score) if (main_score is not None or ind_score is not None) else True
        present_match = main_present == recompute_present
        match = bool(score_match and present_match)
        rec_qa_sev = "" if match else "FAIL"
        rec_qa_msg = ""
        if parse_error:
            rec_qa_sev = "FAIL"
            rec_qa_msg = f"FAIL: parse_error {parse_error}"
        if not match:
            mismatch_msg = (
                f"Recompute mismatch (main_present={main_present}, recompute_present={recompute_present}, "
                f"main_score={main_score}, recompute_score={ind_score})"
            )
            rec_qa_msg = f"{rec_qa_msg} | {mismatch_msg}".strip(" |")
        recompute_rows.append(
            {
                "flag_id": code,
                "quarter": rec_q.date() if rec_q is not None else None,
                "main_present": main_present,
                "recompute_present": recompute_present,
                "main_score": main_score,
                "recompute_score": ind_score,
                "match": match,
                "qa_severity": rec_qa_sev,
                "qa_message": rec_qa_msg,
            }
        )

    # Primary flags output
    flags_objs = [f for f in main_flags.values() if f is not None]
    if not flags_objs:
        flags_df = _empty_flags_df()
    else:
        flags_df = pd.DataFrame([f.to_row() for f in flags_objs])
        flags_df = flags_df.sort_values(["score", "flag_code"], ascending=[False, True]).head(max_flags).reset_index(drop=True)
        flags_df.insert(0, "rank", range(1, len(flags_df) + 1))
        out = _empty_flags_df()
        for c in out.columns:
            if c in flags_df.columns:
                out[c] = flags_df[c]
        flags_df = out

    return flags_df, pd.DataFrame(audit_rows), pd.DataFrame(recompute_rows)


def _build_base_metrics(
    hist: pd.DataFrame,
    adj_metrics: pd.DataFrame,
    leverage_df: pd.DataFrame,
    debt_tranches: pd.DataFrame,
    price: Optional[float] = None,
) -> Dict[str, pd.Series]:
    base = build_signals_base(
        hist=hist,
        adj_metrics=adj_metrics,
        leverage_df=leverage_df,
        debt_tranches=debt_tranches,
        price=price,
    )
    return _metrics_from_signals_base(base)


def _flag_a(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["ebit_growth_yoy", "ebitda_growth_yoy", "shares_yoy"])
    if q is None:
        return None
    ebit_growth_yoy = _num_at(m, "ebit_growth_yoy", q)
    ebitda_growth_yoy = _num_at(m, "ebitda_growth_yoy", q)
    shares_yoy = _num_at(m, "shares_yoy", q)
    if ebit_growth_yoy is None or ebitda_growth_yoy is None or shares_yoy is None:
        return None
    trigger = (ebit_growth_yoy > 0.25) and (ebitda_growth_yoy > 0.20) and (shares_yoy <= -0.02)
    if not trigger:
        return None
    s_ebit = _clamp((ebit_growth_yoy - 0.25) / 0.50 + 0.30) * 40.0
    s_ebitda = _clamp((ebitda_growth_yoy - 0.20) / 0.40 + 0.30) * 40.0
    s_shares = _clamp((abs(shares_yoy) - 0.02) / 0.05 + 0.20) * 20.0
    score = _score_int(s_ebit + s_ebitda + s_shares)
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    fallback_note = " (GAAP fallback)" if _flag_bool_at(m, "adj_fallback", q) else ""
    ev1 = (
        f"{as_of} EBIT TTM YoY {_fmt_pct(ebit_growth_yoy)} "
        f"({_fmt_now_ly_delta(m['ebit_ttm'].get(q), m['ebit_ttm'].get(ly), 'amount')})"
    )
    ev2 = (
        f"{as_of} EBITDA TTM YoY {_fmt_pct(ebitda_growth_yoy)}{fallback_note} "
        f"({_fmt_now_ly_delta(m['ebitda_ttm'].get(q), m['ebitda_ttm'].get(ly), 'amount')})"
    )
    ev3 = (
        f"{as_of} Shares YoY {_fmt_pct(shares_yoy)} "
        f"({_fmt_now_ly_delta(m['shares_out'].get(q), m['shares_out'].get(ly), 'shares')})"
    )
    metrics = {
        "quarter": as_of,
        "ebit_growth_yoy": ebit_growth_yoy,
        "ebitda_growth_yoy": ebitda_growth_yoy,
        "shares_yoy": shares_yoy,
    }
    return _Flag("A", "EBIT/EBITDA growth with shrinking share count", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def _streak_len(series: pd.Series, q: pd.Timestamp) -> int:
    idx = list(series.index)
    if q not in idx:
        return 0
    i = idx.index(q)
    cnt = 1
    while i - cnt >= 0:
        cur = series.iloc[i - cnt + 1]
        prv = series.iloc[i - cnt]
        if pd.isna(cur) or pd.isna(prv) or float(cur) < float(prv):
            break
        cnt += 1
    return cnt


def _flag_b(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["adj_margin_ttm", "adj_margin_ttm_yoy_bps"])
    if q is None:
        return None
    adj_margin_ttm = _num_at(m, "adj_margin_ttm", q)
    margin_yoy_bps = _num_at(m, "adj_margin_ttm_yoy_bps", q)
    if adj_margin_ttm is None or margin_yoy_bps is None:
        return None
    adj_hist = m["adj_margin_ttm"][m["adj_margin_ttm"].index <= q].dropna()
    streak = _streak_len(adj_hist, q)
    trigger = (adj_margin_ttm >= 0.20) and (margin_yoy_bps >= 200.0) and (streak >= 2)
    if not trigger:
        return None
    s1 = 40.0 * _clamp((adj_margin_ttm - 0.20) / 0.15 + 0.30)
    s2 = 35.0 * _clamp((margin_yoy_bps - 200.0) / 500.0 + 0.30)
    s3 = 25.0 * _clamp((streak - 2) / 4.0 + 0.25)
    score = _score_int(s1 + s2)
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    tail3 = adj_hist.tail(3)
    ev1 = f"{as_of} Adj EBITDA margin TTM {_fmt_pct(adj_margin_ttm)} ({_fmt_now_ly_delta(m['adj_margin_ttm'].get(q), m['adj_margin_ttm'].get(ly), 'pct')})"
    ev2 = f"{as_of} Margin YoY {_fmt_bps(margin_yoy_bps)} (threshold 200 bps)"
    ev3 = (
        f"{as_of} Margin streak {streak}q "
        f"(q-2 {_fmt_pct(tail3.iloc[0] if len(tail3) > 0 else None)}, "
        f"q-1 {_fmt_pct(tail3.iloc[1] if len(tail3) > 1 else None)}, "
        f"q0 {_fmt_pct(tail3.iloc[2] if len(tail3) > 2 else None)})"
    )
    metrics = {
        "quarter": as_of,
        "adj_margin_ttm": adj_margin_ttm,
        "margin_yoy_bps": margin_yoy_bps,
        "margin_streak": int(streak),
    }
    return _Flag("B", "Adjusted margin expansion with streak", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def _flag_c(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["fcf_ttm_pos_years", "pos_fcf_ratio", "fcf_yield"])
    if q is None:
        return None
    fcf_ttm_pos_years = _num_at(m, "fcf_ttm_pos_years", q)
    pos_fcf_ratio = _num_at(m, "pos_fcf_ratio", q)
    fcf_yield = _num_at(m, "fcf_yield", q)
    if fcf_ttm_pos_years is None or pos_fcf_ratio is None or fcf_yield is None:
        return None
    trigger = (fcf_ttm_pos_years >= 1.0) and (pos_fcf_ratio >= 0.75) and (fcf_yield >= 0.15)
    if not trigger:
        return None
    score = _score_int(
        30.0 * _clamp((fcf_ttm_pos_years - 1.0) / 3.0 + 0.25)
        + 35.0 * _clamp((pos_fcf_ratio - 0.75) / 0.75 + 0.25)
        + 35.0 * _clamp((fcf_yield - 0.15) / 0.25 + 0.25)
    )
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    ev1 = f"{as_of} FCF TTM positive-years={fcf_ttm_pos_years:.0f} (threshold >=1)"
    ev2 = f"{as_of} FCF/EBIT ratio {pos_fcf_ratio:.2f}x ({_fmt_now_ly_delta(m['pos_fcf_ratio'].get(q), m['pos_fcf_ratio'].get(ly), 'ratio')})"
    ev3 = f"{as_of} FCF yield {_fmt_pct(fcf_yield)} ({_fmt_now_ly_delta(m['fcf_yield'].get(q), m['fcf_yield'].get(ly), 'pct')})"
    metrics = {
        "quarter": as_of,
        "fcf_ttm_pos_years": fcf_ttm_pos_years,
        "pos_fcf_ratio": pos_fcf_ratio,
        "fcf_yield": fcf_yield,
    }
    return _Flag("C", "Cashflow quality and yield support", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def _flag_d(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["debt_drop_pct", "leverage_ratio"])
    if q is None:
        return None
    debt_drop_pct = _num_at(m, "debt_drop_pct", q)
    leverage_ratio = _num_at(m, "leverage_ratio", q)
    if debt_drop_pct is None or leverage_ratio is None:
        return None
    trigger = (debt_drop_pct >= 0.10) and (leverage_ratio <= 3.0)
    if not trigger:
        return None
    s1 = 50.0 * _clamp((debt_drop_pct - 0.10) / 0.25 + 0.25)
    s2 = 50.0 * _clamp((3.0 - leverage_ratio) / 2.0 + 0.25)
    score = _score_int(s1 + s2)
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    ev1 = f"{as_of} Net debt drop {debt_drop_pct*100:.1f}% ({_fmt_now_ly_delta(m['corporate_net_debt'].get(q), m['corporate_net_debt'].get(ly), 'amount')})"
    ev2 = f"{as_of} Leverage ratio {leverage_ratio:.2f}x (threshold <= 3.00x)"
    ev3 = f"{as_of} EBITDA TTM {_fmt_now_ly_delta(m['ebitda_ttm'].get(q), m['ebitda_ttm'].get(ly), 'amount')}"
    metrics = {
        "quarter": as_of,
        "debt_drop_pct": debt_drop_pct,
        "leverage_ratio": leverage_ratio,
    }
    return _Flag("D", "Deleveraging with acceptable leverage", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def _flag_e(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["interest_coverage", "fcf_yield"])
    if q is None:
        return None
    interest_coverage = _num_at(m, "interest_coverage", q)
    fcf_yield = _num_at(m, "fcf_yield", q)
    if interest_coverage is None or fcf_yield is None:
        return None
    if not (interest_coverage >= 3.0 and fcf_yield >= 0.20):
        return None
    s1 = 50.0 * _clamp((interest_coverage - 3.0) / 3.0 + 0.25)
    s2 = 50.0 * _clamp((fcf_yield - 0.20) / 0.30 + 0.25)
    score = _score_int(s1 + s2)
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    ev1 = f"{as_of} Interest coverage {interest_coverage:.2f}x ({_fmt_now_ly_delta(m['interest_coverage'].get(q), m['interest_coverage'].get(ly), 'ratio')})"
    ev2 = f"{as_of} FCF yield {_fmt_pct(fcf_yield)} ({_fmt_now_ly_delta(m['fcf_yield'].get(q), m['fcf_yield'].get(ly), 'pct')})"
    ev3 = f"{as_of} Cheap-cashflow test passed (coverage>=3.0x and FCF yield>=20%)"
    metrics = {
        "quarter": as_of,
        "interest_coverage": interest_coverage,
        "fcf_yield": fcf_yield,
    }
    return _Flag("E", "Coverage strength with cheap cashflow", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def _flag_f(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["shares_yoy", "shares_out"])
    if q is None:
        return None
    shares_yoy = _num_at(m, "shares_yoy", q)
    if shares_yoy is None or shares_yoy > -0.02:
        return None
    fcf_per_share_ttm_yoy = _num_at(m, "fcf_per_share_ttm_yoy", q)
    s_shares = 70.0 * _clamp((abs(shares_yoy) - 0.02) / 0.08 + 0.15)
    s_fcfps = 30.0 * _clamp(((fcf_per_share_ttm_yoy if fcf_per_share_ttm_yoy is not None else 0.0) + 0.05) / 0.25)
    score = _score_int(s_shares + s_fcfps)
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    ev1 = (
        f"{as_of} Shares YoY {_fmt_pct(shares_yoy)} "
        f"({_fmt_now_ly_delta(m['shares_out'].get(q), m['shares_out'].get(ly), 'shares')})"
    )
    ev2 = (
        f"{as_of} FCF/share TTM YoY {_fmt_pct(fcf_per_share_ttm_yoy)} "
        f"({_fmt_now_ly_delta(m['fcf_per_share_ttm'].get(q), m['fcf_per_share_ttm'].get(ly), 'per_share')})"
    )
    ev3 = f"{as_of} Buyback support active (shares outstanding contracted vs LY)"
    metrics = {
        "quarter": as_of,
        "shares_yoy": shares_yoy,
        "fcf_per_share_ttm_yoy": fcf_per_share_ttm_yoy,
    }
    return _Flag("F", "Share count reduction (buyback support)", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def _flag_g(m: Dict[str, pd.Series]) -> Optional[_Flag]:
    q = _latest_metric_quarter(m, ["dividend_ps_q"])
    if q is None:
        return None
    dividend_ps_q = _num_at(m, "dividend_ps_q", q)
    if dividend_ps_q is None:
        return None
    dividend_ps_yoy = _num_at(m, "dividend_ps_yoy", q)
    dividend_ps_qoq = _num_at(m, "dividend_ps_qoq", q)
    dividend_yield = _num_at(m, "dividend_yield", q)
    trigger = (
        (dividend_yield is not None and dividend_yield >= 0.03)
        or (dividend_ps_yoy is not None and dividend_ps_yoy > 0)
        or (dividend_ps_qoq is not None and dividend_ps_qoq > 0)
    )
    if not trigger:
        return None
    score = _score_int(
        40.0 * _clamp((((dividend_yield if dividend_yield is not None else 0.0) - 0.02) / 0.05) + 0.25)
        + 35.0 * _clamp((((dividend_ps_yoy if dividend_ps_yoy is not None else 0.0) - 0.00) / 0.15) + 0.25)
        + 25.0 * _clamp((((dividend_ps_qoq if dividend_ps_qoq is not None else 0.0) - 0.00) / 0.08) + 0.25)
    )
    as_of = q.date().isoformat()
    ly = q - pd.DateOffset(years=1)
    prev = q - pd.DateOffset(months=3)
    ev1 = (
        f"{as_of} Dividend/share {_fmt_per_share(dividend_ps_q)} "
        f"({_fmt_now_ly_delta(m['dividend_ps_q'].get(q), m['dividend_ps_q'].get(ly), 'per_share')})"
    )
    ev2 = f"{as_of} Dividend/share QoQ {_fmt_pct(dividend_ps_qoq)} | YoY {_fmt_pct(dividend_ps_yoy)}"
    ev3 = (
        f"{as_of} Dividend yield {_fmt_pct(dividend_yield)} "
        f"(q-1 {_fmt_per_share(m['dividend_ps_q'].get(prev) if 'dividend_ps_q' in m else None)})"
    )
    metrics = {
        "quarter": as_of,
        "dividend_ps_q": dividend_ps_q,
        "dividend_ps_yoy": dividend_ps_yoy,
        "dividend_ps_qoq": dividend_ps_qoq,
        "dividend_yield": dividend_yield,
    }
    return _Flag("G", "Dividend support (yield or growth)", score, as_of, ev1, ev2, ev3, json.dumps(metrics, separators=(",", ":")))


def build_hidden_value_flags(
    hist: Optional[pd.DataFrame] = None,
    adj_metrics: Optional[pd.DataFrame] = None,
    leverage_df: Optional[pd.DataFrame] = None,
    debt_tranches: Optional[pd.DataFrame] = None,
    signals_base: Optional[pd.DataFrame] = None,
    price: Optional[float] = None,
    max_flags: int = 10,
) -> pd.DataFrame:
    flags_df, _, _ = build_hidden_value_outputs(
        hist=hist,
        adj_metrics=adj_metrics,
        leverage_df=leverage_df,
        debt_tranches=debt_tranches,
        signals_base=signals_base,
        price=price,
        max_flags=max_flags,
    )
    return flags_df


def build_hidden_value_outputs(
    hist: Optional[pd.DataFrame] = None,
    adj_metrics: Optional[pd.DataFrame] = None,
    leverage_df: Optional[pd.DataFrame] = None,
    debt_tranches: Optional[pd.DataFrame] = None,
    signals_base: Optional[pd.DataFrame] = None,
    price: Optional[float] = None,
    max_flags: int = 10,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base_df = signals_base.copy() if signals_base is not None else pd.DataFrame()
    metrics_main: Optional[Dict[str, pd.Series]] = None
    if base_df.empty:
        if hist is None or hist.empty:
            return _empty_flags_df(), _empty_flags_audit_df(), _empty_flags_recompute_df()
        base_df = build_signals_base(
            hist=hist,
            adj_metrics=adj_metrics if adj_metrics is not None else pd.DataFrame(),
            leverage_df=leverage_df if leverage_df is not None else pd.DataFrame(),
            debt_tranches=debt_tranches if debt_tranches is not None else pd.DataFrame(),
            price=price,
        )
        if base_df.empty:
            return _empty_flags_df(), _empty_flags_audit_df(), _empty_flags_recompute_df()
        metrics_main = _metrics_from_signals_base(base_df)
    elif hist is not None and not hist.empty:
        # Preserve the explicit recompute-vs-export check when a prebuilt base is supplied.
        metrics_main = _build_base_metrics(
            hist=hist,
            adj_metrics=adj_metrics if adj_metrics is not None else pd.DataFrame(),
            leverage_df=leverage_df if leverage_df is not None else pd.DataFrame(),
            debt_tranches=debt_tranches if debt_tranches is not None else pd.DataFrame(),
            price=price,
        )
    return _compute_flags_and_audit(base_df, max_flags=max_flags, main_metrics=metrics_main)


def build_hidden_value_audit(
    hist: Optional[pd.DataFrame] = None,
    adj_metrics: Optional[pd.DataFrame] = None,
    leverage_df: Optional[pd.DataFrame] = None,
    debt_tranches: Optional[pd.DataFrame] = None,
    signals_base: Optional[pd.DataFrame] = None,
    price: Optional[float] = None,
    max_flags: int = 10,
) -> pd.DataFrame:
    _, audit_df, _ = build_hidden_value_outputs(
        hist=hist,
        adj_metrics=adj_metrics,
        leverage_df=leverage_df,
        debt_tranches=debt_tranches,
        signals_base=signals_base,
        price=price,
        max_flags=max_flags,
    )
    return audit_df


def build_hidden_value_recompute_check(
    hist: Optional[pd.DataFrame] = None,
    adj_metrics: Optional[pd.DataFrame] = None,
    leverage_df: Optional[pd.DataFrame] = None,
    debt_tranches: Optional[pd.DataFrame] = None,
    signals_base: Optional[pd.DataFrame] = None,
    price: Optional[float] = None,
    max_flags: int = 10,
) -> pd.DataFrame:
    _, _, rec_df = build_hidden_value_outputs(
        hist=hist,
        adj_metrics=adj_metrics,
        leverage_df=leverage_df,
        debt_tranches=debt_tranches,
        signals_base=signals_base,
        price=price,
        max_flags=max_flags,
    )
    return rec_df
