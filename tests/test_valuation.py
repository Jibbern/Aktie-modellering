from __future__ import annotations

import pytest

from pbi_xbrl.valuation import valuation_engine, valuation_to_frames


def _base_hist_latest() -> dict[str, float | str]:
    return {
        "quarter": "2026-03-31",
        "shares_outstanding_m": 100.0,
        "shares_diluted_m": 110.0,
        "debt_core_m": 200.0,
        "cash_m": 50.0,
        "net_debt_m": 150.0,
        "ebitda_ttm_m": 50.0,
        "adj_ebitda_ttm_m": 75.0,
        "fcf_ttm_m": 30.0,
        "interest_paid_ttm_m": 10.0,
        "revenue_ttm_m": 500.0,
        "capex_ttm_m": 20.0,
    }


def test_valuation_engine_reports_adj_ebitda_and_equity_fcf_lenses() -> None:
    out = valuation_engine(price=10.0, scenario_inputs={}, hist_latest=_base_hist_latest())

    assert out["implied_ev"] == pytest.approx(1150.0)
    assert out["implied_ev_ebitda"] == pytest.approx(23.0)
    assert out["implied_ev_adj_ebitda"] == pytest.approx(1150.0 / 75.0)
    assert out["implied_fcf_yield"] == pytest.approx(40.0 / 1150.0)
    assert out["equity_fcf_yield"] == pytest.approx(30.0 / 1000.0)
    assert out["ev_tieout_diff_m"] == pytest.approx(0.0)

    summary_df, grid_df = valuation_to_frames(out)
    summary = dict(zip(summary_df["metric"], summary_df["value"]))
    assert summary["implied_ev_adj_ebitda"] == pytest.approx(1150.0 / 75.0)
    assert summary["equity_fcf_yield"] == pytest.approx(30.0 / 1000.0)
    assert "ev_adj_ebitda_multiple" in grid_df.columns


def test_valuation_engine_does_not_call_missing_price_a_debt_cash_tieout_issue() -> None:
    out = valuation_engine(price=None, scenario_inputs={}, hist_latest=_base_hist_latest())

    flags = " | ".join(out["sanity_flags"])
    assert "WARN: price missing/<=0" in flags
    assert "EV tieout incomplete (missing debt/cash)" not in flags


def test_valuation_engine_keeps_debt_cash_tieout_warning_when_components_are_missing() -> None:
    hist_latest = _base_hist_latest()
    hist_latest.pop("debt_core_m")
    hist_latest.pop("cash_m")

    out = valuation_engine(price=10.0, scenario_inputs={}, hist_latest=hist_latest)

    flags = " | ".join(out["sanity_flags"])
    assert "EV tieout incomplete (missing debt/cash)" in flags
