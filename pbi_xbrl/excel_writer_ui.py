"""Presentation-oriented helpers for workbook UI surfaces and note rendering."""
from __future__ import annotations

from typing import Any, Dict, List

from .excel_writer_core import ensure_ui_evidence, timed_writer_stage
from .writer_types import WriterContext


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
            write_sheet("Slides_Guidance", ctx.inputs.slides_guidance)


def write_ui_sheets(ctx: WriterContext) -> List[Dict[str, Any]]:
    ensure_ui_evidence(ctx)
    _write_ui_raw_frames(ctx)

    ui_qa_rows: List[Dict[str, Any]] = []
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui.render.quarter_notes",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        # Quarter notes are profiled separately because they are often the single
        # largest workbook hotspot and deserve their own timing bucket.
        ui_qa_rows.extend(ctx.callbacks.write_quarter_notes_ui_v2(quarters_shown=8))
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
