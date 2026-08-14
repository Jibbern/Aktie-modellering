from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pbi_xbrl.derivative_oci_bridge import (
    DERIVATIVE_EXPOSURE_COLUMNS,
    DerivativeSourceResolutionError,
    _source_paths_for_ticker,
    build_derivative_oci_bridge_from_sources,
)
from pbi_xbrl.excel_writer_economics_overlay_derivatives import (
    GpreEconomicsOverlayDerivativeSideEffectDeps,
    write_gpre_derivative_crush_tests_side_effect,
)
from tests.workbook_test_resources import registered_ticker_dir


def _gpre_financial_statement_files() -> tuple[Path, ...]:
    source_dir = registered_ticker_dir("GPRE", Path(__file__).resolve()) / "financial_statement"
    paths = sorted(
        {
            *source_dir.glob("GPRE_Q*_10Q_*_financial_statement.htm"),
            *source_dir.glob("GPRE_FY*_10K_*_financial_statement.htm"),
        },
        key=lambda path: path.name,
    )
    assert paths, f"Required registered GPRE derivative/OCI filings are unavailable under {source_dir}"
    return tuple(paths)


def test_derivative_source_resolution_is_order_stable_and_missing_fails_closed(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "GPRE" / "financial_statement"
    source_dir.mkdir(parents=True)
    later = source_dir / "GPRE_Q2_2026_10Q_2026-06-30_financial_statement.htm"
    earlier = source_dir / "GPRE_Q1_2026_10Q_2026-03-31_financial_statement.htm"
    later.write_text("<html>later</html>", encoding="utf-8")
    earlier.write_text("<html>earlier</html>", encoding="utf-8")

    assert _source_paths_for_ticker("GPRE", tmp_path) == [
        earlier.resolve(),
        later.resolve(),
    ]
    with pytest.raises(DerivativeSourceResolutionError, match="source directory is missing"):
        _source_paths_for_ticker("PBI", tmp_path)


def test_pack_owned_derivative_crush_materialization_requires_valid_dependencies() -> None:
    writes: list[dict[str, pd.DataFrame]] = []

    class _Result:
        def as_dict(self) -> dict[str, pd.DataFrame]:
            return {"reconciliation": pd.DataFrame([{"status": "PASS"}])}

    def _build(*_args):
        return _Result()

    ready = GpreEconomicsOverlayDerivativeSideEffectDeps(
        runtime_sheet_owned=True,
        derivative_oci_bridge_df=pd.DataFrame([{"quarter": "2026-03-31"}]),
        derivative_oci_exposure_df=pd.DataFrame(),
        operating_driver_history_rows=[{"quarter": "2026-03-31", "gallons": 1.0}],
        gpre_basis_model_result={"quarterly_df": pd.DataFrame([{"quarter": "2026-03-31"}])},
        info_log=[],
        build_derivative_crush_tests=_build,
        write_derivative_crush_tests_sheet=lambda payload: writes.append(payload),
    )
    result = write_gpre_derivative_crush_tests_side_effect(ready)
    assert result.wrote_sheet is True
    assert result.warning_count == 0
    assert len(writes) == 1
    assert writes[0]["reconciliation"].iloc[0]["status"] == "PASS"

    for blocked in (
        {**vars(ready), "runtime_sheet_owned": False},
        {**vars(ready), "derivative_oci_bridge_df": pd.DataFrame()},
        {**vars(ready), "gpre_basis_model_result": {"quarterly_df": pd.DataFrame()}},
    ):
        writes.clear()
        blocked_result = write_gpre_derivative_crush_tests_side_effect(
            GpreEconomicsOverlayDerivativeSideEffectDeps(**blocked)
        )
        assert blocked_result.wrote_sheet is False
        assert writes == []


def test_pack_owned_derivative_crush_materialization_failure_is_material() -> None:
    class _Result:
        def as_dict(self) -> dict[str, pd.DataFrame]:
            return {"reconciliation": pd.DataFrame([{"status": "PASS"}])}

    def _write_failure(_payload: dict[str, pd.DataFrame]) -> None:
        raise RuntimeError("Derivative_Crush_Tests renderer failed")

    deps = GpreEconomicsOverlayDerivativeSideEffectDeps(
        runtime_sheet_owned=True,
        derivative_oci_bridge_df=pd.DataFrame([{"quarter": "2026-03-31"}]),
        derivative_oci_exposure_df=pd.DataFrame(),
        operating_driver_history_rows=[{"quarter": "2026-03-31", "gallons": 1.0}],
        gpre_basis_model_result={"quarterly_df": pd.DataFrame([{"quarter": "2026-03-31"}])},
        info_log=[],
        build_derivative_crush_tests=lambda *_args: _Result(),
        write_derivative_crush_tests_sheet=_write_failure,
    )

    with pytest.raises(RuntimeError, match="Derivative_Crush_Tests renderer failed"):
        write_gpre_derivative_crush_tests_side_effect(deps)

    assert deps.info_log == []


def test_gpre_derivative_oci_bridge_keeps_pnl_and_oci_separate() -> None:
    source_path = (
        registered_ticker_dir("GPRE", Path(__file__).resolve())
        / "financial_statement"
        / "GPRE_Q1_2026_10Q_2026-03-31_financial_statement.htm"
    )
    assert source_path.is_file(), f"Required registered GPRE Q1 2026 10-Q is unavailable: {source_path}"

    result = build_derivative_oci_bridge_from_sources("GPRE", [source_path])
    rows = result.rows
    assert not rows.empty
    recs = rows[rows["quarter"].astype(str).eq("2026-03-31")].to_dict("records")
    assert recs
    rec = recs[0]

    assert rec["non_designated_derivative_pnl_revenue_usd"] == pytest.approx(-9_757_000.0)
    assert rec["non_designated_derivative_pnl_cogs_usd"] == pytest.approx(-8_813_000.0)
    assert rec["non_designated_derivative_pnl_total_usd"] == pytest.approx(-18_570_000.0)
    assert rec["cash_flow_hedge_reclass_revenue_usd"] == pytest.approx(390_000.0)
    assert rec["cash_flow_hedge_reclass_cogs_usd"] == pytest.approx(4_865_000.0)
    assert rec["cash_flow_hedge_reclass_total_usd"] == pytest.approx(5_255_000.0)
    assert rec["fair_value_hedge_inventory_adjustment_cogs_usd"] == pytest.approx(-385_000.0)
    assert rec["fair_value_hedge_derivative_futures_effect_cogs_usd"] == pytest.approx(1_106_000.0)
    assert rec["fair_value_hedge_total_pnl_usd"] == pytest.approx(721_000.0)
    assert rec["derivative_gain_loss_revenue_usd"] == pytest.approx(-9_367_000.0)
    assert rec["derivative_gain_loss_cogs_usd"] == pytest.approx(-3_227_000.0)
    assert rec["derivative_gain_loss_pnl_total_usd"] == pytest.approx(-12_594_000.0)
    assert rec["derivative_oci_current_period_usd"] == pytest.approx(-9_569_000.0)
    assert rec["derivative_aoci_reclassified_to_earnings_usd"] == pytest.approx(-3_920_000.0)
    assert rec["derivative_aoci_ending_balance_usd"] == pytest.approx(-14_107_000.0)
    assert rec["derivative_assets_current_usd"] == pytest.approx(10_279_000.0)
    assert rec["derivative_liabilities_current_usd"] == pytest.approx(35_359_000.0)
    assert rec["derivative_net_asset_liability_usd"] == pytest.approx(-25_080_000.0)
    assert pd.isna(rec["eps_excluding_unrealized_derivative_pnl"])
    assert "OCI only" in rec["derivative_notes"]
    assert "P&L" in rec["derivative_notes"]

    exposure = result.exposure_rows
    assert not exposure.empty
    assert "Scale" in DERIVATIVE_EXPOSURE_COLUMNS
    assert list(DERIVATIVE_EXPOSURE_COLUMNS).index("Scale") == list(DERIVATIVE_EXPOSURE_COLUMNS).index("Unit") + 1
    assert set(exposure["Scale"].dropna().astype(str)) == {"in thousands"}
    assert set(exposure["Commodity"]) >= {"Corn", "Ethanol", "Natural Gas"}
    corn_cf = exposure[
        exposure["Commodity"].astype(str).eq("Corn")
        & exposure["Accounting bucket"].astype(str).eq("Cash-flow hedge")
    ]
    assert not corn_cf.empty
    assert corn_cf.iloc[0]["Instrument"] == "Exchange-traded futures/options"
    assert corn_cf.iloc[0]["Net notional"] == pytest.approx(32_920.0)
    corn_fv = exposure[
        exposure["Commodity"].astype(str).eq("Corn")
        & exposure["Accounting bucket"].astype(str).eq("Fair-value hedge")
    ]
    assert not corn_fv.empty
    assert corn_fv.iloc[0]["Net notional"] == pytest.approx(-2_845.0)
    forwards = exposure[
        exposure["Commodity"].astype(str).eq("Ethanol")
        & exposure["Instrument"].astype(str).eq("Non-exchange-traded forwards")
    ]
    assert not forwards.empty
    assert forwards.iloc[0]["Long notional"] == pytest.approx(15_967.0)
    assert forwards.iloc[0]["Short notional"] == pytest.approx(-196_477.0)
    assert forwards.iloc[0]["Scale"] == "in thousands"
    assert "no hedge-accounting footnote" in str(forwards.iloc[0]["Source / note"])

    qa = result.qa_rows
    assert not qa.empty
    assert set(qa["issue_family"]) >= {
        "derivative_oci_pnl_separation",
        "derivative_eps_tax_support",
    }


def test_derivative_oci_bridge_fixture_flags_oci_pnl_mixup(tmp_path: Path) -> None:
    fixture = tmp_path / "GPRE_Q1_2026_10Q_2026-03-31_financial_statement.htm"
    fixture.write_text(
        """
        <html><body>
        <table>
        <tr><td>Three Months Ended March 31,</td><td>2026</td></tr>
        <tr><td>Amount of Gain (Loss) Recognized in Income on Derivatives</td><td></td></tr>
        <tr><td>Exchange-traded futures and options</td><td>Revenues</td><td>$</td><td>(100)</td></tr>
        <tr><td>Exchange-traded futures and options</td><td>Cost of goods sold</td><td>(200)</td></tr>
        <tr><td>Net gain (loss) recognized in income (loss) before income taxes</td><td>$</td><td>(300)</td></tr>
        </table>
        <table>
        <tr><td>Consolidated Statements of Comprehensive Income</td><td>Three Months Ended March 31,</td><td>2026</td></tr>
        <tr><td>Unrealized losses on derivatives arising during the period, net of tax benefit of $30</td><td>(300)</td></tr>
        <tr><td>Reclassification of realized gains on derivatives, net of tax expense of $10</td><td>(50)</td></tr>
        </table>
        </body></html>
        """,
        encoding="utf-8",
    )

    result = build_derivative_oci_bridge_from_sources("GPRE", [fixture])
    qa = result.qa_rows
    mix_rows = qa[qa["issue_family"].astype(str).eq("derivative_oci_pnl_separation")]
    assert not mix_rows.empty
    assert "do not include in net income bridge" in str(mix_rows.iloc[0]["message"])


def test_gpre_derivative_oci_bridge_builds_recent_historical_quarters() -> None:
    result = build_derivative_oci_bridge_from_sources("GPRE", _gpre_financial_statement_files())
    rows = result.rows
    assert not rows.empty

    quarters = set(rows["quarter"].astype(str))
    assert {"2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"} <= quarters

    q2 = rows[rows["quarter"].astype(str).eq("2025-06-30")].iloc[0]
    assert q2["non_designated_derivative_pnl_total_usd"] == pytest.approx(4_490_000.0)
    assert q2["non_designated_derivative_pnl_revenue_usd"] == pytest.approx(1_503_000.0)
    assert q2["non_designated_derivative_pnl_cogs_usd"] == pytest.approx(2_987_000.0)
    assert q2["derivative_oci_current_period_usd"] == pytest.approx(-8_191_000.0)
    assert q2["quarterization_status"] == "source_three_month"

    q3 = rows[rows["quarter"].astype(str).eq("2025-09-30")].iloc[0]
    assert q3["derivative_oci_current_period_usd"] == pytest.approx(-12_105_000.0)
    assert q3["derivative_aoci_reclassified_to_earnings_usd"] == pytest.approx(5_831_000.0)

    q4 = rows[rows["quarter"].astype(str).eq("2025-12-31")].iloc[0]
    assert q4["non_designated_derivative_pnl_total_usd"] == pytest.approx(-877_000.0)
    assert q4["non_designated_derivative_pnl_revenue_usd"] == pytest.approx(-3_990_000.0)
    assert q4["non_designated_derivative_pnl_cogs_usd"] == pytest.approx(3_113_000.0)
    assert q4["quarterization_status"] == "annual_minus_q1_q3"

    q3_2023 = rows[rows["quarter"].astype(str).eq("2023-09-30")].iloc[0]
    assert q3_2023["non_designated_derivative_pnl_total_usd"] == pytest.approx(27_247_000.0)

    exposure = result.exposure_rows
    assert not exposure.empty
    assert {"Quarter", "Commodity", "Instrument", "Accounting bucket", "Net notional", "Scale"} <= set(exposure.columns)
    assert set(exposure["Scale"].dropna().astype(str)) == {"in thousands"}
    assert "Cash-flow hedge" in set(exposure["Accounting bucket"].astype(str))
    assert "Fair-value hedge" in set(exposure["Accounting bucket"].astype(str))
    assert "Economic / non-designated" in set(exposure["Accounting bucket"].astype(str))

    qa = result.qa_rows
    assert "derivative_quarterization_inferred" in set(qa["issue_family"].astype(str))
