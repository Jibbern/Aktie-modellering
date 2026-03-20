from __future__ import annotations

from typing import Any, Dict, List

from .excel_writer_context import WriterContext
from .excel_writer_core import ensure_ui_evidence, timed_writer_stage


def write_ui_sheets(ctx: WriterContext) -> List[Dict[str, Any]]:
    with timed_writer_stage(
        ctx.writer_timings,
        "write_excel.ui",
        enabled=bool(ctx.inputs.profile_timings),
    ):
        ensure_ui_evidence(ctx)
        write_sheet = ctx.callbacks.write_sheet

        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.raw_frames",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            write_sheet("Quarter_Notes", ctx.inputs.quarter_notes)
            write_sheet(
                "Quarter_Notes_Evidence",
                ctx.require_derived_frame("quarter_notes_evidence_df"),
            )
            write_sheet("Promise_Tracker", ctx.inputs.promises)
            write_sheet(
                "Promise_Evidence",
                ctx.require_derived_frame("promise_evidence_df"),
            )
            write_sheet("Promise_Progress", ctx.inputs.promise_progress)
            write_sheet("NonGAAP_Credibility", ctx.inputs.non_gaap_cred)
            write_sheet("Slides_Guidance", ctx.inputs.slides_guidance)

        ui_qa_rows: List[Dict[str, Any]] = []
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.quarter_notes",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            ui_qa_rows.extend(ctx.callbacks.write_quarter_notes_ui_v2(quarters_shown=8))
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.promise_tracker",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            ui_qa_rows.extend(ctx.callbacks.write_promise_tracker_ui_v2())
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.promise_progress",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            ui_qa_rows.extend(ctx.callbacks.write_promise_progress_ui_v2())
        return ui_qa_rows
