from __future__ import annotations

import pandas as pd
import pytest

from pbi_xbrl.derivative_crush_tests import build_derivative_crush_tests


def _sample_bridge() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "quarter": pd.Timestamp("2025-12-31"),
                "derivative_gain_loss_pnl_total_usd": 2_000_000.0,
                "derivative_gain_loss_revenue_usd": 1_500_000.0,
                "derivative_gain_loss_cogs_usd": 500_000.0,
                "cash_flow_hedge_reclass_total_usd": 300_000.0,
                "fair_value_hedge_total_pnl_usd": 200_000.0,
                "non_designated_derivative_pnl_total_usd": 1_500_000.0,
                "derivative_oci_current_period_usd": -1_000_000.0,
                "derivative_aoci_ending_balance_usd": -4_000_000.0,
                "derivative_net_asset_liability_usd": -8_000_000.0,
                "quarterization_status": "reported",
            },
            {
                "quarter": pd.Timestamp("2026-03-31"),
                "derivative_gain_loss_pnl_total_usd": -12_594_000.0,
                "derivative_gain_loss_revenue_usd": -9_367_000.0,
                "derivative_gain_loss_cogs_usd": -3_227_000.0,
                "cash_flow_hedge_reclass_total_usd": 5_255_000.0,
                "fair_value_hedge_total_pnl_usd": 721_000.0,
                "non_designated_derivative_pnl_total_usd": -18_570_000.0,
                "derivative_oci_current_period_usd": -9_569_000.0,
                "derivative_aoci_ending_balance_usd": -14_107_000.0,
                "derivative_net_asset_liability_usd": -25_080_000.0,
                "quarterization_status": "reported",
            },
        ]
    )


def _sample_exposure() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Quarter": pd.Timestamp("2026-03-31"),
                "Commodity": "Corn",
                "Instrument": "Exchange-traded futures/options",
                "Accounting bucket": "Cash-flow hedge",
                "Direction": "Net long",
                "Net notional": 31_775.0,
                "Unit": "Bushels",
                "Scale": "in thousands",
                "Likely P&L line": "COGS",
            },
            {
                "Quarter": pd.Timestamp("2026-03-31"),
                "Commodity": "Ethanol",
                "Instrument": "Non-exchange-traded forwards",
                "Accounting bucket": "Economic / non-designated",
                "Direction": "Net short",
                "Net notional": -12_000.0,
                "Unit": "Gallons",
                "Scale": "in thousands",
                "Likely P&L line": "Revenue",
            },
        ]
    )


def _sample_drivers() -> list[dict[str, object]]:
    return [
        {"Quarter": pd.Timestamp("2025-12-31"), "_driver_key": "ethanol_gallons_produced", "Value": 170.0},
        {"Quarter": pd.Timestamp("2025-12-31"), "_driver_key": "consolidated_ethanol_crush_margin", "Value": 40.0},
        {"Quarter": pd.Timestamp("2025-12-31"), "_driver_key": "plant_utilization", "Value": 0.93},
        {"Quarter": pd.Timestamp("2026-03-31"), "_driver_key": "ethanol_gallons_produced", "Value": 174.196},
        {"Quarter": pd.Timestamp("2026-03-31"), "_driver_key": "consolidated_ethanol_crush_margin", "Value": 64.616},
        {"Quarter": pd.Timestamp("2026-03-31"), "_driver_key": "crush_margin_ex_45z", "Value": 8.516},
        {"Quarter": pd.Timestamp("2026-03-31"), "_driver_key": "plant_utilization", "Value": 0.97},
    ]


def _sample_quarterly() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "quarter": pd.Timestamp("2025-12-31"),
                "official_simple_proxy_usd_per_gal": 0.20,
                "gpre_proxy_official_usd_per_gal": 0.23,
                "best_forward_lens_proxy_usd_per_gal": 0.22,
                "reported_consolidated_crush_margin_usd_per_gal": 40.0 / 170.0,
                "weighted_basis_recommended_usd_per_bu": -0.10,
                "natural_gas_price_usd_per_mmbtu": 2.5,
                "coproduct_approximate_credit_usd_per_gal": 0.12,
            },
            {
                "quarter": pd.Timestamp("2026-03-31"),
                "official_simple_proxy_usd_per_gal": 0.0824,
                "gpre_proxy_official_usd_per_gal": 0.095,
                "best_forward_lens_proxy_usd_per_gal": 0.105,
                "reported_consolidated_crush_margin_usd_per_gal": 64.616 / 174.196,
                "weighted_basis_recommended_usd_per_bu": -0.12,
                "natural_gas_price_usd_per_mmbtu": 2.8,
                "coproduct_approximate_credit_usd_per_gal": 0.13,
            },
        ]
    )


def test_derivative_crush_tests_build_both_baseline_lenses_and_q1_per_gallon() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )

    reconciliation = result.reconciliation
    assert set(reconciliation["Baseline lens"]) == {"Approximate market crush", "GPRE crush proxy", "Best forward lens"}
    q1 = reconciliation[
        (reconciliation["Quarter"] == pd.Timestamp("2026-03-31"))
        & (reconciliation["Baseline lens"] == "Approximate market crush")
    ].iloc[0]
    assert q1["Total derivative P&L / gal"] == pytest.approx(-12.594 / 174.196, abs=0.0005)
    assert q1["Derivative-adjusted proxy margin / gal"] == pytest.approx(0.0824 + (-12.594 / 174.196), abs=0.0005)
    assert q1["Error improvement / gal"] == pytest.approx(
        abs((64.616 / 174.196) - 0.0824)
        - abs((64.616 / 174.196) - (0.0824 + (-12.594 / 174.196))),
        abs=0.0005,
    )


def test_second_stage_ex_derivative_and_clean_margin_diagnostics() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )

    ex_test = result.ex_derivative_margin_test
    q1_ex = ex_test[
        (ex_test["Quarter"] == pd.Timestamp("2026-03-31"))
        & (ex_test["Baseline lens"] == "Approximate market crush")
    ].iloc[0]
    expected_deriv = -12.594 / 174.196
    expected_reported = 64.616 / 174.196
    assert q1_ex["Reported margin ex derivative / gal"] == pytest.approx(expected_reported - expected_deriv, abs=0.0005)
    assert q1_ex["Error vs ex-derivative margin"] == pytest.approx((expected_reported - expected_deriv) - 0.0824, abs=0.0005)

    clean = result.clean_margin_bridge
    q1_clean = clean[clean["Quarter"] == pd.Timestamp("2026-03-31")].iloc[0]
    expected_45z = 56.1 / 174.196
    assert q1_clean["45Z impact / gal"] == pytest.approx(expected_45z, abs=0.0005)
    assert q1_clean["Clean margin / gal"] == pytest.approx(expected_reported - expected_deriv - expected_45z, abs=0.0005)
    assert "missing explicit items" in str(q1_clean["Notes / flags"])


def test_second_stage_accuracy_regression_lag_and_residual_screens_exist() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )

    assert not result.target_specific_model_accuracy.empty
    assert {"Reported margin / gal", "Reported margin ex derivative / gal", "Clean margin / gal"} <= set(result.target_specific_model_accuracy["Target"])
    assert "Best forward lens" in set(result.target_specific_model_accuracy["Baseline lens"])

    assert not result.coefficient_diagnostic.empty
    assert "Model 2: reported = alpha + beta * proxy + gamma * derivative P&L" in set(result.coefficient_diagnostic["Regression model"])
    assert all("insufficient sample" in str(x) or "diagnostic" in str(x) or "derivative P&L" in str(x) or "possible timing" in str(x) for x in result.coefficient_diagnostic["Interpretation"])

    assert not result.lagged_derivative_pnl_tests.empty
    assert {"Current quarter derivative P&L", "Prior quarter derivative P&L", "Rolling 2Q derivative P&L avg", "Rolling 4Q derivative P&L avg"} <= set(result.lagged_derivative_pnl_tests["Derivative timing variant"])

    assert not result.residual_driver_screen.empty
    assert {"45Z impact / gal", "Coproduct value proxy / gal", "Q4 quarterization flag"} <= set(result.residual_driver_screen["Driver"])
    assert all("current-quarter P&L" not in str(x) for x in result.residual_driver_screen["Driver"])


def test_derivative_crush_tests_exclude_oci_from_current_quarter_models() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )

    summary_blob = "\n".join(result.model_summary["Formula"].astype(str).tolist())
    assert "OCI" not in summary_blob
    assert "AOCI" not in summary_blob
    assert "net derivative" not in summary_blob.lower()

    lead_blob = "\n".join(result.lead_lag_summary["Lead variable"].astype(str).tolist())
    assert "Derivative AOCI / gal" in lead_blob
    assert "Derivative OCI movement / gal" in lead_blob
    assert "Net derivative asset/liability / gal" in lead_blob


def test_derivative_crush_tests_exposure_buckets_preserve_scale_and_skip_coverage_ratios() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )

    exposure = result.exposure_buckets
    assert set(exposure["Scale"]) == {"in thousands"}
    assert set(exposure["Coverage ratio"]) == {"not available"}
    bucket_by_commodity = dict(zip(exposure["Commodity"], exposure["Margin bucket"]))
    assert bucket_by_commodity["Corn"] == "Core crush input"
    assert bucket_by_commodity["Ethanol"] == "Core crush output"


def test_derivative_crush_tests_quarterly_impact_is_pnl_only() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )

    metrics = set(result.quarterly_derivative_impact["Metric"])
    assert "Total derivative P&L / gal" in metrics
    assert "P&L component residual / unallocated / gal" in metrics
    assert "Derivative OCI movement" not in metrics
    assert "Derivative AOCI" not in metrics
    assert "Net derivative asset/liability" not in metrics
    q1_total = result.quarterly_derivative_impact.loc[
        result.quarterly_derivative_impact["Metric"] == "Total derivative P&L / gal",
        "2026-Q1",
    ].iloc[0]
    assert q1_total == pytest.approx(-12.594 / 174.196, abs=0.0005)


def test_stage3_diagnostics_add_q4_decomposition_volume_basis_and_aoci_tables() -> None:
    result = build_derivative_crush_tests(
        _sample_bridge(),
        _sample_exposure(),
        _sample_drivers(),
        _sample_quarterly(),
    )
    tables = result.as_dict()

    expected_tables = {
        "key_takeaways",
        "q4_quarterization_sensitivity",
        "revenue_cogs_decomposition",
        "volume_utilization_summary",
        "volume_utilization_quarterly",
        "basis_energy_summary",
        "basis_energy_quarterly",
        "aoci_future_reclass_summary",
        "aoci_future_reclass_tracker",
    }
    assert expected_tables <= set(tables)

    q4 = result.q4_quarterization_sensitivity
    assert {"All quarters", "Excluding Q4 quarters", "Q4-only"} <= set(q4["Sample"])
    q4_only = q4[q4["Sample"] == "Q4-only"].iloc[0]
    assert "Q4 quarterized / annual-minus-Q1-Q3" in str(q4_only["Notes / flags"])

    decomp = result.revenue_cogs_decomposition
    q1 = decomp[
        (decomp["Quarter"] == pd.Timestamp("2026-03-31"))
        & (decomp["Baseline lens"] == "Approximate market crush")
    ].iloc[0]
    reported = 64.616 / 174.196
    revenue_deriv = -9.367 / 174.196
    cogs_deriv = -3.227 / 174.196
    assert q1["Error after revenue derivative adjustment"] == pytest.approx(reported - (0.0824 + revenue_deriv), abs=0.0005)
    assert q1["Error after COGS derivative adjustment"] == pytest.approx(reported - (0.0824 + cogs_deriv), abs=0.0005)
    assert q1["Error after revenue + COGS derivative adjustment"] == pytest.approx(reported - (0.0824 + revenue_deriv + cogs_deriv), abs=0.0005)

    assert {"Ethanol gallons produced", "Utilization"} <= set(result.volume_utilization_summary["Driver"])
    volume_q1 = result.volume_utilization_quarterly[
        result.volume_utilization_quarterly["Quarter"] == pd.Timestamp("2026-03-31")
    ].iloc[0]
    assert volume_q1["Production QoQ change"] == pytest.approx(174.196 - 170.0, abs=0.0005)

    assert {"Corn basis proxy", "Natural gas proxy"} <= set(result.basis_energy_summary["Driver"])
    assert "Available?" in set(result.basis_energy_summary.columns)

    tracker = result.aoci_future_reclass_tracker
    assert "Next-quarter cash-flow hedge reclass / gal" in set(tracker.columns)
    assert all("Not current-quarter P&L" in str(x) or "insufficient sample" in str(x) for x in tracker["Interpretation"])
    summary = result.aoci_future_reclass_summary
    assert "AOCI / gal vs next-quarter cash-flow reclass / gal" in set(summary["Diagnostic"])

    takeaway_metrics = set(result.key_takeaways["Diagnostic"])
    assert "Production model recommendation" in takeaway_metrics
    assert all("production" in str(x).lower() or "diagnostic" in str(x).lower() or str(x).strip() for x in result.key_takeaways["Reading"])
