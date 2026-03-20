from __future__ import annotations

from typing import Any, Dict, List

from .excel_writer_context import WriterContext
from .excel_writer_core import (
    ensure_hidden_value_inputs,
    ensure_report_inputs,
    ensure_summary_inputs,
    ensure_valuation_inputs,
)


def write_summary_sheets(ctx: WriterContext) -> None:
    ensure_summary_inputs(ctx)
    ctx.callbacks.write_summary_sheet(ctx.require_derived_frame("summary_df"))


def write_valuation_sheets(ctx: WriterContext) -> List[Dict[str, Any]]:
    ensure_valuation_inputs(ctx)
    ui_qa_rows: List[Dict[str, Any]] = []
    ctx.callbacks.write_valuation_sheet()
    ui_qa_rows.extend(ctx.callbacks.write_bs_segments_sheet(quarters_shown=8))
    ctx.callbacks.write_sheet(
        "Valuation_Summary",
        ctx.require_derived_frame("valuation_summary_df"),
    )
    ctx.callbacks.write_sheet(
        "Valuation_Grid",
        ctx.require_derived_frame("valuation_grid_df"),
    )
    return ui_qa_rows


def write_debt_sheets(ctx: WriterContext) -> None:
    ensure_valuation_inputs(ctx)
    write_sheet = ctx.callbacks.write_sheet
    write_sheet("Debt_Tranches_Latest", ctx.inputs.debt_tranches_latest)
    write_sheet("Revolver_History", ctx.inputs.revolver_history)
    write_sheet("Debt_Profile", ctx.inputs.debt_profile)
    write_sheet("Debt_Maturity_Ladder", ctx.inputs.debt_maturity)
    write_sheet("Debt_Buckets", ctx.inputs.debt_buckets)
    write_sheet("Debt_Recon", ctx.inputs.debt_recon)
    write_sheet("Debt_Tranches_Q", ctx.inputs.debt_tranches)
    write_sheet("Debt_Credit_Notes", ctx.inputs.debt_credit_notes)
    write_sheet("Leverage_Liquidity", ctx.require_derived_frame("leverage_df"))


def write_report_sheets(ctx: WriterContext) -> None:
    ensure_report_inputs(ctx)
    ensure_hidden_value_inputs(ctx)
    ctx.callbacks.write_report_sheet(
        "REPORT_IS_Q",
        ctx.require_derived_frame("report_is"),
        "USD millions",
    )
    ctx.callbacks.write_report_sheet(
        "REPORT_BS_Q",
        ctx.require_derived_frame("report_bs"),
        "USD millions",
    )
    ctx.callbacks.write_report_sheet(
        "REPORT_CF_Q",
        ctx.require_derived_frame("report_cf"),
        "USD millions",
    )
    ctx.callbacks.write_flags_sheet("Hidden_Value_Flags", ctx.require_derived_frame("flags_df"))
    ctx.callbacks.write_sheet(
        "Hidden_Value_Audit",
        ctx.require_derived_frame("flags_audit_df"),
    )
    ctx.callbacks.write_sheet(
        "Hidden_Value_Recompute",
        ctx.require_derived_frame("flags_recompute_df"),
    )
    ctx.callbacks.write_sheet(
        "Hidden_Value_Base",
        ctx.require_derived_frame("signals_base_df"),
    )
