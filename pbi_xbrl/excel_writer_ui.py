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


def _gtx_guidance_normalized_frame(guidance_df: Any) -> Any:
    """Append curated, official GTX outlook/actual rows missing from noisy slide OCR."""
    df = _shared_guidance_normalized_frame(guidance_df)
    if not isinstance(df, pd.DataFrame):
        return df
    if df.empty:
        columns = [
            "quarter",
            "line",
            "numbers",
            "metric_hint",
            "doc",
            "page",
            "source",
            "period_label",
            "metric",
            "source_date",
            "stated_in_label",
            "horizon_label",
            "horizon_type",
            "source_context",
        ]
        df = pd.DataFrame(columns=columns)
    columns = list(df.columns)

    def _curated(
        *,
        stated_quarter: str,
        metric: str,
        line: str,
        numbers: str,
        source_date: str,
        source_doc: str,
        horizon: str = "2026 year",
        period_label: str = "2026 year",
    ) -> Dict[str, Any]:
        row = {col: "" for col in columns}
        row.update(
            {
                "quarter": pd.Timestamp(stated_quarter),
                "line": line,
                "numbers": numbers,
                "metric_hint": metric,
                "doc": source_doc,
                "source": "earnings_release_curated",
                "period_label": period_label,
                "metric": metric,
                "source_date": source_date,
                "stated_in_label": _shared_quarter_label(stated_quarter),
                "horizon_label": horizon,
                "horizon_type": "annual" if "year" in horizon.lower() or horizon.startswith("FY") else "quarter",
                "source_context": line,
            }
        )
        return row

    q1_doc = r"C:\Users\Jibbe\Aktier\StockModelData\tickers\GTX\earnings_release\exhibit99_1-2026q1.htm"
    q4_doc = r"C:\Users\Jibbe\Aktier\StockModelData\tickers\GTX\earnings_release\exhibit99_1-2025q4.htm"
    curated_rows = [
        _curated(
            stated_quarter="2026-03-31",
            metric="Net sales",
            line="2026-Q1 raised FY2026 outlook: net sales $3.6bn-$3.9bn.",
            numbers="$3.6bn-$3.9bn",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Constant-currency sales growth",
            line="2026-Q1 FY2026 outlook assumption: constant-currency sales growth -2% to +6%.",
            numbers="-2% to +6%",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Net income",
            line="2026-Q1 raised FY2026 outlook: net income $300m-$360m.",
            numbers="$300m-$360m",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Adjusted EBIT",
            line="2026-Q1 raised FY2026 outlook: adjusted EBIT $520m-$600m.",
            numbers="$520m-$600m",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="CFO",
            line="2026-Q1 FY2026 outlook: CFO $407m-$522m.",
            numbers="$407m-$522m",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Adjusted FCF",
            line="2026-Q1 raised FY2026 outlook: adjusted FCF $355m-$475m.",
            numbers="$355m-$475m",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Light vehicle production",
            line="2026-Q1 FY2026 assumption: light vehicle industry production down 1%-3%.",
            numbers="-1% to -3%",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Commercial vehicle industry",
            line="2026-Q1 FY2026 assumption: commercial vehicle industry up 1%-2%.",
            numbers="+1% to +2%",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="BEV penetration",
            line="2026-Q1 FY2026 assumption: BEV penetration about 19%.",
            numbers="about 19%",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="EUR/USD",
            line="2026-Q1 FY2026 assumption: EUR/USD 1.17.",
            numbers="1.17",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="RD&E",
            line="2026-Q1 FY2026 assumption: RD&E about 4.2% of sales.",
            numbers="about 4.2% of sales",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2026-03-31",
            metric="Capex",
            line="2026-Q1 FY2026 assumption: capex about 2.5% of sales.",
            numbers="about 2.5% of sales",
            source_date="2026-04-30",
            source_doc=q1_doc,
        ),
        _curated(
            stated_quarter="2025-12-31",
            metric="FY2025 net sales",
            line="2025-Q4 actual: FY2025 net sales $3.584bn.",
            numbers="$3.584bn",
            source_date="2026-02-19",
            source_doc=q4_doc,
            horizon="FY2025 actual",
            period_label="FY2025 actual",
        ),
        _curated(
            stated_quarter="2025-12-31",
            metric="FY2025 adjusted EBIT",
            line="2025-Q4 actual: FY2025 adjusted EBIT $510m.",
            numbers="$510m",
            source_date="2026-02-19",
            source_doc=q4_doc,
            horizon="FY2025 actual",
            period_label="FY2025 actual",
        ),
        _curated(
            stated_quarter="2025-12-31",
            metric="FY2025 adjusted FCF",
            line="2025-Q4 actual: FY2025 adjusted FCF $403m.",
            numbers="$403m",
            source_date="2026-02-19",
            source_doc=q4_doc,
            horizon="FY2025 actual",
            period_label="FY2025 actual",
        ),
        _curated(
            stated_quarter="2025-12-31",
            metric="FY2025 buybacks",
            line="2025-Q4 actual: FY2025 buybacks $208m and common share count reduction 8% year-over-year.",
            numbers="$208m; 8%",
            source_date="2026-02-19",
            source_doc=q4_doc,
            horizon="FY2025 actual",
            period_label="FY2025 actual",
        ),
    ]
    return pd.concat([df, pd.DataFrame(curated_rows, columns=columns)], ignore_index=True)


def _visible_non_gaap_credibility_frame_for_workbook(ticker: str, non_gaap_df: Any) -> Any:
    if ticker != "GTX" or not isinstance(non_gaap_df, pd.DataFrame) or non_gaap_df.empty:
        return non_gaap_df
    required = {"quarter", "gaap_ebit"}
    if not required.issubset(set(non_gaap_df.columns)):
        return non_gaap_df
    out = non_gaap_df.copy()
    q = pd.to_datetime(out["quarter"], errors="coerce")
    gaap = pd.to_numeric(out["gaap_ebit"], errors="coerce")
    impossible_q4 = q.dt.strftime("%Y-%m-%d").isin(
        {"2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31"}
    ) & gaap.isin([345_000_000, 388_000_000, 347_000_000, 353_000_000])
    out.loc[impossible_q4, "gaap_ebit"] = pd.NA
    if "total_adjustments" in out.columns:
        out.loc[impossible_q4, "total_adjustments"] = pd.NA
    return out


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
            ticker = str(getattr(ctx.company_profile, "ticker", "") or getattr(ctx.inputs, "ticker", "") or "").upper()
            write_sheet("NonGAAP_Credibility", _visible_non_gaap_credibility_frame_for_workbook(ticker, ctx.inputs.non_gaap_cred))
            write_sheet("Guidance_Raw", ctx.inputs.guidance_raw)
            guidance_normalized = ctx.inputs.slides_guidance
            if ticker == "ANF":
                from .excel_writer_context import _anf_visible_guidance_normalized_frame

                guidance_normalized = _anf_visible_guidance_normalized_frame(guidance_normalized)
            elif ticker == "GTX":
                guidance_normalized = _gtx_guidance_normalized_frame(guidance_normalized)
            else:
                guidance_normalized = _shared_guidance_normalized_frame(guidance_normalized)
            write_sheet("Guidance_Normalized", guidance_normalized)
            write_sheet("Slides_Guidance", guidance_normalized)


def write_ui_sheets(ctx: WriterContext) -> List[Dict[str, Any]]:
    ensure_ui_evidence(ctx)
    _write_ui_raw_frames(ctx)

    ui_qa_rows: List[Dict[str, Any]] = []
    ticker = str(getattr(ctx.company_profile, "ticker", "") or getattr(ctx.inputs, "ticker", "") or "").upper()
    if ticker:
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
    narrative_ui_writer = ctx.callbacks.extra_callbacks.get("_write_quarter_notes_narrative_ui_sheet")
    if callable(narrative_ui_writer):
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.quarter_notes_narrative",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            narrative_ui_writer()
    narrative_writer = ctx.callbacks.extra_callbacks.get("_write_quarter_narrative_data_sheet")
    if callable(narrative_writer):
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.quarter_narrative_data",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            narrative_writer()
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
    narrative_ui_writer = ctx.callbacks.extra_callbacks.get("_write_quarter_notes_narrative_ui_sheet")
    if callable(narrative_ui_writer):
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.quarter_notes_narrative",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            narrative_ui_writer()
    narrative_writer = ctx.callbacks.extra_callbacks.get("_write_quarter_narrative_data_sheet")
    if callable(narrative_writer):
        with timed_writer_stage(
            ctx.writer_timings,
            "write_excel.ui.render.quarter_narrative_data",
            enabled=bool(ctx.inputs.profile_timings),
        ):
            narrative_writer()
    return ui_qa_rows
