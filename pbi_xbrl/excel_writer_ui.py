"""Presentation-oriented helpers for workbook UI surfaces and note rendering."""
from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

from .excel_writer_core import ensure_ui_evidence, timed_writer_stage
from .writer_types import WriterContext


def _shared_visible_period_text(text_in: Any) -> str:
    txt = str(text_in or "")
    if not txt:
        return ""
    txt = re.sub(r"\bQ([1-4])\s*FY\s*(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bQ([1-4])\s+fiscal\s+(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bQ([1-4])\s*[-/]\s*(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bQ([1-4])\s+(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bFY\s*(20\d{2})\b", r"\1 year", txt, flags=re.I)
    txt = re.sub(r"\bfiscal\s+year\s+(20\d{2})\b", r"\1 year", txt, flags=re.I)
    return txt


def _shared_quarter_label(q_raw: Any) -> str:
    q = pd.to_datetime(q_raw, errors="coerce")
    if pd.isna(q):
        return ""
    ts = pd.Timestamp(q)
    qn = ((int(ts.month) - 1) // 3) + 1
    return f"{int(ts.year)}-Q{qn}"


def _shared_guidance_normalized_frame(guidance_df: Any) -> Any:
    """Give all tickers the same canonical guidance columns for UI/readback."""
    if guidance_df is None:
        return guidance_df
    if not isinstance(guidance_df, pd.DataFrame):
        return guidance_df
    df = guidance_df.copy()
    if df.empty:
        return df
    if "metric" not in df.columns and "metric_hint" in df.columns:
        df["metric"] = df["metric_hint"]
    if "source_date" not in df.columns:
        if "quarter" in df.columns:
            df["source_date"] = pd.to_datetime(df["quarter"], errors="coerce").dt.strftime("%Y-%m-%d")
        elif "filed" in df.columns:
            df["source_date"] = pd.to_datetime(df["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
        else:
            df["source_date"] = ""
    if "stated_in_label" not in df.columns:
        source_col = "source_quarter_label" if "source_quarter_label" in df.columns else None
        if source_col:
            df["stated_in_label"] = df[source_col].map(_shared_visible_period_text)
        elif "quarter" in df.columns:
            df["stated_in_label"] = df["quarter"].map(_shared_quarter_label)
        else:
            df["stated_in_label"] = ""
    else:
        df["stated_in_label"] = df["stated_in_label"].map(_shared_visible_period_text)
    if "horizon_label" not in df.columns:
        if "period_label" in df.columns:
            df["horizon_label"] = df["period_label"].map(_shared_visible_period_text)
        elif "quarter" in df.columns:
            df["horizon_label"] = df["quarter"].map(_shared_quarter_label)
        else:
            df["horizon_label"] = ""
    else:
        df["horizon_label"] = df["horizon_label"].map(_shared_visible_period_text)
    if "period_label" not in df.columns:
        df["period_label"] = df["horizon_label"]
    else:
        df["period_label"] = df["period_label"].map(_shared_visible_period_text)
    if "horizon_type" not in df.columns:
        def _h_type(v: Any) -> str:
            s = str(v or "").strip().lower()
            if re.fullmatch(r"20\d{2}-q[1-4]", s):
                return "quarter"
            if re.fullmatch(r"20\d{2}\s+year", s):
                return "annual"
            return ""
        df["horizon_type"] = df["horizon_label"].map(_h_type)
    if "source_context" not in df.columns:
        if "line" in df.columns:
            df["source_context"] = df["line"]
        elif "source" in df.columns:
            df["source_context"] = df["source"]
        else:
            df["source_context"] = ""
    return df


def _write_ui_raw_frames(ctx: WriterContext, *, debug_subset: bool = False) -> None:
    write_sheet = ctx.callbacks.write_sheet
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui.raw_frames",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        # These raw evidence tabs are written before the visible UI sheets so the
        # saved workbook always contains the backing rows used for readback and
        # provenance, even if later visible rendering is the expensive part.
        write_sheet("Quarter_Notes", ctx.inputs.quarter_notes)
        write_sheet(
            "Quarter_Notes_Evidence",
            ctx.require_derived_frame("quarter_notes_evidence_df"),
        )
        write_sheet("Promise_Tracker", ctx.inputs.promises)
        if not debug_subset:
            write_sheet(
                "Promise_Evidence",
                ctx.require_derived_frame("promise_evidence_df"),
            )
        write_sheet("Promise_Progress", ctx.inputs.promise_progress)
        if not debug_subset:
            write_sheet("NonGAAP_Credibility", ctx.inputs.non_gaap_cred)
            write_sheet("Guidance_Raw", ctx.inputs.guidance_raw)
            guidance_normalized = ctx.inputs.slides_guidance
            ticker = str(getattr(ctx.company_profile, "ticker", "") or getattr(ctx.inputs, "ticker", "") or "").upper()
            if ticker == "ANF":
                from .excel_writer_context import _anf_visible_guidance_normalized_frame

                guidance_normalized = _anf_visible_guidance_normalized_frame(guidance_normalized)
            else:
                guidance_normalized = _shared_guidance_normalized_frame(guidance_normalized)
            write_sheet("Guidance_Normalized", guidance_normalized)
            write_sheet("Slides_Guidance", guidance_normalized)


def write_ui_sheets(ctx: WriterContext) -> List[Dict[str, Any]]:
    ensure_ui_evidence(ctx)
    _write_ui_raw_frames(ctx)

    ui_qa_rows: List[Dict[str, Any]] = []
    ticker = str(getattr(ctx.company_profile, "ticker", "") or getattr(ctx.inputs, "ticker", "") or "").upper()
    if ticker in {"ANF", "PBI", "GPRE"}:
        writer = ctx.callbacks.extra_callbacks.get("_write_investment_case_surfaces")
        if not callable(writer):
            writer = ctx.callbacks.extra_callbacks.get("_write_anf_investment_case_surfaces")
        if callable(writer):
            with timed_writer_stage(
                ctx.writer_timings,
                f"write_excel.ui.render.{ticker.lower()}_investment_case",
                enabled=bool(ctx.inputs.profile_timings),
            ):
                writer()
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui.render.quarter_notes",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        # Quarter notes are profiled separately because they are often the single
        # largest workbook hotspot and deserve their own timing bucket.
        ui_qa_rows.extend(ctx.callbacks.write_quarter_notes_ui_v2(quarters_shown=12 if ticker == "ANF" else 8))
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui.render.promise_tracker",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        ui_qa_rows.extend(ctx.callbacks.write_promise_tracker_ui_v2(render_visible=False))
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui.render.promise_progress",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        # Promise progress stays separate from promise tracker timing because row
        # selection, lifecycle collapse, and follow-through resolution can be
        # costly even when the raw tracker tab is small.
        ui_qa_rows.extend(ctx.callbacks.write_promise_progress_ui_v2())
    return ui_qa_rows


def write_ui_debug_sheets(ctx: WriterContext) -> List[Dict[str, Any]]:
    ensure_ui_evidence(ctx)
    _write_ui_raw_frames(ctx, debug_subset=True)

    ui_qa_rows: List[Dict[str, Any]] = []
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui.render.quarter_notes",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        ui_qa_rows.extend(ctx.callbacks.write_quarter_notes_ui_v2(quarters_shown=8))
    return ui_qa_rows
