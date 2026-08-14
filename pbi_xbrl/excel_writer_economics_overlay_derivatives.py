from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import pandas as pd


@dataclass(frozen=True)
class GpreEconomicsOverlayDerivativeSideEffectDeps:
    runtime_sheet_owned: bool
    derivative_oci_bridge_df: Any
    derivative_oci_exposure_df: Any
    operating_driver_history_rows: Sequence[Mapping[str, Any]]
    gpre_basis_model_result: Any
    info_log: list[dict[str, Any]]
    build_derivative_crush_tests: Callable[..., Any]
    write_derivative_crush_tests_sheet: Callable[..., None]


@dataclass(frozen=True)
class GpreEconomicsOverlayDerivativeSideEffectResult:
    wrote_sheet: bool
    warning_count: int


def write_gpre_derivative_crush_tests_side_effect(
    deps: GpreEconomicsOverlayDerivativeSideEffectDeps,
) -> GpreEconomicsOverlayDerivativeSideEffectResult:
    if not (
        deps.runtime_sheet_owned
        and isinstance(deps.derivative_oci_bridge_df, pd.DataFrame)
        and not deps.derivative_oci_bridge_df.empty
        and isinstance(deps.gpre_basis_model_result, dict)
    ):
        return GpreEconomicsOverlayDerivativeSideEffectResult(wrote_sheet=False, warning_count=0)

    quarterly_basis_df = deps.gpre_basis_model_result.get("quarterly_df")
    if not isinstance(quarterly_basis_df, pd.DataFrame) or quarterly_basis_df.empty:
        return GpreEconomicsOverlayDerivativeSideEffectResult(wrote_sheet=False, warning_count=0)

    try:
        # This sheet needs the basis model's quarterly frame, so it
        # is built after write_gpre_basis_proxy_overlay_support()
        # has prepared the market/proxy data for the current export.
        derivative_crush_result = deps.build_derivative_crush_tests(
            deps.derivative_oci_bridge_df,
            deps.derivative_oci_exposure_df
            if isinstance(deps.derivative_oci_exposure_df, pd.DataFrame)
            else pd.DataFrame(),
            deps.operating_driver_history_rows,
            quarterly_basis_df,
        )
    except Exception as exc:
        deps.info_log.append(
            {
                "quarter": None,
                "metric": "Derivative_Crush_Tests",
                "severity": "warn",
                "message": f"Skipped Derivative_Crush_Tests sheet: {exc}",
                "source": "excel_writer_context",
            }
        )
        return GpreEconomicsOverlayDerivativeSideEffectResult(wrote_sheet=False, warning_count=1)

    # Once the owned module's required inputs have produced a valid result,
    # materialization is a correctness boundary.  A renderer/write failure
    # must propagate to the workbook writer so final publication is blocked.
    deps.write_derivative_crush_tests_sheet(derivative_crush_result.as_dict())
    return GpreEconomicsOverlayDerivativeSideEffectResult(wrote_sheet=True, warning_count=0)
