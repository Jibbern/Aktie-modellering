from __future__ import annotations

import json
from math import isclose
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pbi_xbrl.signals as signals_module
import pytest

from pbi_xbrl.signals import (
    _flag_a,
    build_hidden_value_flags,
    build_hidden_value_outputs,
    build_signals_base,
)


def _make_hist() -> pd.DataFrame:
    quarters = pd.to_datetime(
        [
            "2023-03-31",
            "2023-06-30",
            "2023-09-30",
            "2023-12-31",
            "2024-03-31",
            "2024-06-30",
            "2024-09-30",
            "2024-12-31",
            "2025-03-31",
            "2025-06-30",
            "2025-09-30",
            "2025-12-31",
        ]
    )
    return pd.DataFrame(
        {
            "quarter": quarters,
            "revenue": [820, 850, 880, 910, 920, 950, 980, 1010, 1040, 1070, 1090, 1120],
            "cfo": [160, 170, 180, 190, 200, 210, 220, 230, 250, 265, 280, 300],
            "capex": [35, 35, 36, 36, 37, 37, 38, 38, 39, 40, 41, 42],
            "ebit": [95, 105, 115, 125, 115, 128, 142, 156, 170, 186, 204, 224],
            "ebitda": [145, 155, 166, 178, 182, 196, 212, 230, 248, 268, 290, 314],
            "interest_expense_net": [44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33],
            "cash": [180, 182, 184, 188, 192, 198, 204, 210, 220, 230, 245, 260],
            "debt_core": [1650, 1635, 1615, 1590, 1550, 1510, 1470, 1430, 1380, 1330, 1260, 1190],
            "shares_outstanding": [205, 204, 203, 202, 200, 198, 196, 194, 191, 188, 185, 182],
            "market_cap": [2800, 2820, 2840, 2860, 2900, 2940, 2980, 3020, 3060, 3100, 3140, 3180],
        }
    )


def _ttm(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rolling(4, min_periods=4).sum()


def _yoy(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    prev = s.shift(4)
    out = (s / prev) - 1.0
    out[(prev == 0) | prev.isna() | s.isna()] = pd.NA
    return out


def _margin(n: pd.Series, d: pd.Series) -> pd.Series:
    n1 = pd.to_numeric(n, errors="coerce")
    d1 = pd.to_numeric(d, errors="coerce")
    out = n1 / d1
    out[(d1 == 0) | d1.isna() | n1.isna()] = pd.NA
    return out


def _latest_common(*series: pd.Series) -> Optional[pd.Timestamp]:
    valid: Optional[pd.Index] = None
    for s in series:
        idx = s[s.notna()].index
        valid = idx if valid is None else valid.intersection(idx)
    if valid is None or len(valid) == 0:
        return None
    return pd.Timestamp(max(valid))


def _streak(series: pd.Series, q: pd.Timestamp) -> int:
    s = series.dropna().sort_index()
    if q not in s.index:
        return 0
    pos = list(s.index).index(q)
    out = 1
    while pos - out >= 0:
        cur = float(s.iloc[pos - out + 1])
        prev = float(s.iloc[pos - out])
        if cur < prev:
            break
        out += 1
    return out


def _assert_close(a: Optional[float], b: Optional[float], tol: float = 1e-9) -> None:
    if (a is None or pd.isna(a)) and (b is None or pd.isna(b)):
        return
    assert a is not None and not pd.isna(a)
    assert b is not None and not pd.isna(b)
    assert isclose(float(a), float(b), rel_tol=tol, abs_tol=tol)


def _assert_series_close(left: pd.Series, right: pd.Series, tol: float = 1e-9) -> None:
    idx = sorted(set(left.index).union(set(right.index)))
    for q in idx:
        _assert_close(left.get(q), right.get(q), tol=tol)


def test_hidden_value_flags_independent_metrics_match() -> None:
    hist = _make_hist()
    base = build_signals_base(hist=hist)
    assert not base.empty
    b = base.set_index(pd.to_datetime(base["quarter"])).sort_index()

    h = hist.copy().set_index(pd.to_datetime(hist["quarter"])).sort_index()
    revenue_ttm = _ttm(h["revenue"])
    cfo_ttm = _ttm(h["cfo"])
    capex_ttm = _ttm(h["capex"])
    ebit_ttm = _ttm(h["ebit"])
    ebitda_ttm = _ttm(h["ebitda"])
    int_exp_ttm = _ttm(h["interest_expense_net"])
    fcf_q = h["cfo"] - h["capex"]
    fcf_ttm = _ttm(fcf_q)

    _assert_series_close(b["revenue_ttm"], revenue_ttm)
    _assert_series_close(b["cfo_ttm"], cfo_ttm)
    _assert_series_close(b["capex_ttm"], capex_ttm)
    _assert_series_close(b["ebit_ttm"], ebit_ttm)
    _assert_series_close(b["ebitda_ttm"], ebitda_ttm)
    _assert_series_close(b["int_exp_ttm"], int_exp_ttm)
    _assert_series_close(b["fcf_ttm"], fcf_ttm)

    _assert_series_close(b["revenue_ttm_yoy"], _yoy(revenue_ttm))
    _assert_series_close(b["fcf_ttm_yoy"], _yoy(fcf_ttm))
    _assert_series_close(b["ebit_growth_yoy"], _yoy(ebit_ttm))
    _assert_series_close(b["ebitda_growth_yoy"], _yoy(ebitda_ttm))

    adj_margin_ttm = _margin(ebitda_ttm, revenue_ttm)
    margin_yoy_bps = (adj_margin_ttm - adj_margin_ttm.shift(4)) * 10_000.0
    _assert_series_close(b["adj_margin_ttm"], adj_margin_ttm)
    _assert_series_close(b["adj_margin_ttm_yoy_bps"], margin_yoy_bps)

    q = _latest_common(b["adj_margin_ttm"], b["adj_margin_ttm_yoy_bps"])
    assert q is not None
    assert _streak(adj_margin_ttm, q) == _streak(b["adj_margin_ttm"], q)

    q_common_a = _latest_common(b["ebit_growth_yoy"], b["ebitda_growth_yoy"], b["shares_yoy"])
    assert q_common_a is not None
    assert pd.notna(b.loc[q_common_a, "ebit_growth_yoy"])
    assert pd.notna(b.loc[q_common_a, "ebitda_growth_yoy"])
    assert pd.notna(b.loc[q_common_a, "shares_yoy"])


def test_hidden_value_signals_base_uses_gaap_ebitda_ttm_fallback_when_adjusted_missing() -> None:
    hist = _make_hist()

    base = build_signals_base(hist=hist)

    assert not base.empty
    base_idx = base.set_index(pd.to_datetime(base["quarter"])).sort_index()
    hist_idx = hist.set_index(pd.to_datetime(hist["quarter"])).sort_index()
    expected_margin = _margin(_ttm(hist_idx["ebitda"]), _ttm(hist_idx["revenue"]))

    _assert_series_close(base_idx["adj_margin_ttm"], expected_margin)
    assert float(pd.to_numeric(base_idx["adj_fallback"].dropna().iloc[-1], errors="coerce")) == pytest.approx(1.0, abs=1e-9)


def test_hidden_value_flags_trigger_logic_matches_spec() -> None:
    hist = _make_hist()
    base_df = build_signals_base(hist=hist)
    base = base_df.set_index(pd.to_datetime(base_df["quarter"])).sort_index()
    flags = build_hidden_value_flags(hist=hist)
    codes = set(flags["flag_code"].tolist())

    qa: Dict[str, bool] = {}

    q_a = pd.Timestamp(base.index.max())
    assert q_a is not None
    qa["A"] = (
        float(base.loc[q_a, "ebit_growth_yoy"]) > 0.25
        and float(base.loc[q_a, "ebitda_growth_yoy"]) > 0.20
        and float(base.loc[q_a, "shares_yoy"]) <= -0.02
    )

    q_b = pd.Timestamp(base.index.max())
    assert q_b is not None
    qa["B"] = (
        float(base.loc[q_b, "adj_margin_ttm"]) >= 0.20
        and float(base.loc[q_b, "adj_margin_ttm_yoy_bps"]) >= 200.0
        and _streak(base["adj_margin_ttm"], q_b) >= 2
    )

    q_c = pd.Timestamp(base.index.max())
    assert q_c is not None
    qa["C"] = (
        float(base.loc[q_c, "fcf_ttm_pos_years"]) >= 1.0
        and float(base.loc[q_c, "pos_fcf_ratio"]) >= 0.75
        and float(base.loc[q_c, "fcf_yield"]) >= 0.15
    )

    q_d = pd.Timestamp(base.index.max())
    assert q_d is not None
    qa["D"] = float(base.loc[q_d, "debt_drop_pct"]) >= 0.10 and float(base.loc[q_d, "leverage_ratio"]) <= 3.0

    q_e = pd.Timestamp(base.index.max())
    assert q_e is not None
    qa["E"] = float(base.loc[q_e, "interest_coverage"]) >= 3.0 and float(base.loc[q_e, "fcf_yield"]) >= 0.20

    for code, expected in qa.items():
        assert (code in codes) == expected
        if expected:
            row = flags.loc[flags["flag_code"] == code].iloc[0]
            metrics = json.loads(row["metrics_json"])
            assert row["as_of_quarter"] in row["evidence_1"]
            assert row["as_of_quarter"] in row["evidence_2"]
            assert row["as_of_quarter"] in row["evidence_3"]
            if code == "A":
                _assert_close(metrics["ebit_growth_yoy"], base.loc[q_a, "ebit_growth_yoy"])
                _assert_close(metrics["ebitda_growth_yoy"], base.loc[q_a, "ebitda_growth_yoy"])
                _assert_close(metrics["shares_yoy"], base.loc[q_a, "shares_yoy"])
            if code == "B":
                _assert_close(metrics["adj_margin_ttm"], base.loc[q_b, "adj_margin_ttm"])
                _assert_close(metrics["margin_yoy_bps"], base.loc[q_b, "adj_margin_ttm_yoy_bps"])
                assert int(metrics["margin_streak"]) == _streak(base["adj_margin_ttm"], q_b)
            if code == "C":
                _assert_close(metrics["fcf_ttm_pos_years"], base.loc[q_c, "fcf_ttm_pos_years"])
                _assert_close(metrics["pos_fcf_ratio"], base.loc[q_c, "pos_fcf_ratio"])
                _assert_close(metrics["fcf_yield"], base.loc[q_c, "fcf_yield"])
            if code == "D":
                _assert_close(metrics["debt_drop_pct"], base.loc[q_d, "debt_drop_pct"])
                _assert_close(metrics["leverage_ratio"], base.loc[q_d, "leverage_ratio"])
            if code == "E":
                _assert_close(metrics["interest_coverage"], base.loc[q_e, "interest_coverage"])
                _assert_close(metrics["fcf_yield"], base.loc[q_e, "fcf_yield"])


def test_hidden_value_flags_edge_cases_disable_triggers() -> None:
    hist = _make_hist()
    hist_edge = hist.copy()
    # Denominator edge cases at the tail.
    hist_edge.loc[hist_edge.index[-4:], "market_cap"] = 0.0
    hist_edge.loc[hist_edge.index[-4:], "interest_expense_net"] = 0.0
    hist_edge.loc[hist_edge.index[-4:], "ebit"] = 0.0
    hist_edge.loc[hist_edge.index[-4:], "ebitda"] = -1.0

    base = build_signals_base(hist=hist_edge)
    b = base.set_index(pd.to_datetime(base["quarter"])).sort_index()
    q = b.index.max()

    assert pd.isna(b.loc[q, "fcf_yield"])
    assert pd.isna(b.loc[q, "interest_coverage"])
    assert pd.isna(b.loc[q, "pos_fcf_ratio"])
    assert pd.isna(b.loc[q, "leverage_ratio"])

    flags = build_hidden_value_flags(hist=hist_edge)
    codes = set(flags["flag_code"].tolist())
    assert "C" not in codes
    assert "D" not in codes
    assert "E" not in codes


def test_flag_a_uses_q_specific_fallback_marker() -> None:
    q1 = pd.Timestamp("2024-12-31")
    q2 = pd.Timestamp("2025-12-31")
    metrics = {
        "ebit_growth_yoy": pd.Series([0.30, 0.35], index=[q1, q2]),
        "ebitda_growth_yoy": pd.Series([0.25, 0.30], index=[q1, q2]),
        "shares_yoy": pd.Series([-0.03, -0.04], index=[q1, q2]),
        "ebit_ttm": pd.Series([500.0, 600.0], index=[q1, q2]),
        "ebitda_ttm": pd.Series([800.0, 1000.0], index=[q1, q2]),
        "shares_out": pd.Series([190.0, 180.0], index=[q1, q2]),
        "adj_fallback": pd.Series([0.0, 1.0], index=[q1, q2]),
    }
    flag = _flag_a(metrics)
    assert flag is not None
    assert "(GAAP fallback)" in flag.evidence_2


def test_hidden_value_recompute_matches_between_pipeline_and_export() -> None:
    hist = _make_hist()
    base = build_signals_base(hist=hist)
    flags, audit, recompute = build_hidden_value_outputs(hist=hist, signals_base=base)
    assert not flags.empty
    assert not audit.empty
    assert not recompute.empty
    assert recompute["match"].astype(bool).all()
    triggered_rows = audit[audit["pass_fail"].astype(bool)]
    assert not (triggered_rows["qa_severity"].astype(str).str.upper() == "FAIL").any()


def test_hidden_value_outputs_builds_signals_base_once_without_prebuilt_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hist = _make_hist()
    original = signals_module.build_signals_base
    calls = {"count": 0}

    def counted_build_signals_base(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(signals_module, "build_signals_base", counted_build_signals_base)
    flags, audit, recompute = build_hidden_value_outputs(hist=hist)

    assert calls["count"] == 1
    assert not flags.empty
    assert not audit.empty
    assert not recompute.empty


def test_hidden_value_known_quarters_from_local_workbook_if_available() -> None:
    xl = Path(__file__).resolve().parents[2] / "Excel stock models" / "PBI_model.xlsx"
    if not xl.exists():
        return
    hist = pd.read_excel(xl, sheet_name="History_Q")
    adj = pd.read_excel(xl, sheet_name="Adjusted_Metrics")
    lev = pd.read_excel(xl, sheet_name="Leverage_Liquidity")
    debt = pd.read_excel(xl, sheet_name="Debt_Tranches_Q")
    base = build_signals_base(hist=hist, adj_metrics=adj, leverage_df=lev, debt_tranches=debt)
    flags, audit, recompute = build_hidden_value_outputs(
        hist=hist,
        adj_metrics=adj,
        leverage_df=lev,
        debt_tranches=debt,
        signals_base=base,
    )
    assert not base.empty
    assert not audit.empty
    assert recompute["match"].astype(bool).all()
    # The latest local file should include the margin-expansion flag.
    assert "B" in set(flags["flag_code"].astype(str))
