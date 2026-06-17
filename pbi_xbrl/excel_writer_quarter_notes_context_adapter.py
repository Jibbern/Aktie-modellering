"""Quarter Notes context adapter helpers.

This module owns the thin context-facing Quarter Notes adapters that still
need writer-context runtime state. The heavy Quarter_Notes_UI orchestration,
selection, render, and repair logic stays in the extracted Quarter Notes
modules.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping

from .excel_writer_quarter_notes_ui_orchestrator import (
    QuarterNotesUiOrchestratorDeps,
    write_quarter_notes_ui_sheet,
)


@dataclass(frozen=True)
class QuarterNotesContextAdapterDeps:
    runtime: MutableMapping[str, Any]


def standardize_quarter_notes_ui_categories(
    deps: QuarterNotesContextAdapterDeps,
    ws: Any,
    ticker: Any = "",
) -> None:
    """Polish visible Quarter_Notes_UI categories and debug text by ticker."""
    runtime = deps.runtime
    _shared_visible_period_text = runtime["_shared_visible_period_text"]
    glx_normalize_text = runtime["glx_normalize_text"]
    ticker_txt = str(ticker or "").strip().upper()
    if ws is None or str(getattr(ws, "title", "")) != "Quarter_Notes_UI":
        return

    def _clean_note_text(text_in: Any) -> str:
        txt = str(text_in or "")
        if not txt:
            return ""
        txt = re.sub(r"\[(?:NEW|UPDATED|CONTINUED|REAFFIRMED|DROPPED)\]", "", txt, flags=re.I)
        txt = re.sub(r"\bDropped theme:\s*", "", txt, flags=re.I)
        txt = txt.replace("\u2026", "").replace("...", "")
        txt = _shared_visible_period_text(txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        txt = re.sub(
            r",\s*with record second quarter operating\.?$",
            ", with record second-quarter operating-margin expansion.",
            txt,
            flags=re.I,
        )
        txt = re.sub(r"\s+while keeping the\.?$", ".", txt, flags=re.I)
        return txt

    def _is_visible_fragment_text(text_in: Any) -> bool:
        txt = glx_normalize_text(str(text_in or "")).strip()
        if not txt:
            return False
        return bool(
            re.search(r"\bwe will leverage the strong foundation we have built\b", txt, re.I)
            or re.search(r"\bspecifically:\s*two healthy\b", txt, re.I)
            or re.search(r"\bgood morning and thank you\b", txt, re.I)
            or re.search(r"\binterim period results are not necessarily indicative\b", txt, re.I)
            or re.search(r"\bASC\s*(?:Topic\s*)?(?:606|842)\b", txt, re.I)
        )

    def _cat(category_in: Any, note_in: Any, metric_in: Any) -> str:
        raw = " ".join(str(x or "") for x in (category_in, note_in, metric_in)).lower()
        if ticker_txt == "PBI":
            if re.search(r"\bcost savings|run-rate savings|productivity|savings target\b", raw):
                return "Cost savings"
            if re.search(r"\bdebt|refi|refinanc|leverage|liquidity|revolver|maturit|covenant\b", raw):
                return "Balance sheet / liquidity"
            if re.search(r"\bbuyback|dividend|capital allocation|share repurchase\b", raw):
                return "Capital allocation"
            if re.search(r"\bguidance|outlook|target|expects|forecast\b", raw):
                return "Guidance / outlook"
            if re.search(r"\bpresort|sendtech|segment|turnaround\b", raw):
                return "Segment / turnaround"
            if re.search(r"\brevenue|ebit|ebitda|eps|fcf|free cash|income|margin\b", raw):
                return "Results / financials"
        elif ticker_txt == "GPRE":
            if re.search(r"\brestructuring costs?|cost reduction initiative\b", raw):
                return "Results / financials"
            if re.search(r"\b45z|45q|carbon|ccs|tax credit\b", raw):
                return "45Z / carbon"
            if re.search(r"\bpolicy|rvo|sre|rin|e15|export|regulation|epa\b", raw):
                return "Policy / regulation"
            if re.search(r"\bcrush|margin per gallon|ethanol margin|corn spread\b", raw):
                return "Crush margin"
            if re.search(r"\bproduction|produced gallons|sold gallons|utilization|downtime|plant\b", raw):
                return "Production / gallons"
            if re.search(r"\bdebt|cash|liquidity|balance sheet|capex\b", raw):
                return "Balance sheet / liquidity"
            if re.search(r"\bguidance|outlook|target|expects|forecast\b", raw):
                return "Guidance / outlook"
            if re.search(r"\brevenue|ebit|ebitda|eps|fcf|income\b", raw):
                return "Results / financials"
        elif ticker_txt == "ANF":
            if re.search(r"\bbuyback|repurchase|net cash|authorization\b", raw):
                return "Capital allocation"
            if re.search(r"\binventory|working capital|markdown\b", raw):
                return "Inventory / working capital"
            if re.search(r"\bdigital|omnichannel|visits\b", raw):
                return "Digital / omnichannel"
            if re.search(r"\bstores?|openings?|closures?|remodel\b", raw):
                return "Stores / real estate"
            if re.search(r"\bcomp|comparable sales\b", raw):
                return "Comps"
            if re.search(r"\babercrombie|hollister|brand\b", raw):
                return "Brand / demand"
            if re.search(r"\btariff|freight|aur|gross margin|margin bridge\b", raw):
                return "Margin bridge"
            if re.search(r"\bguidance|outlook|guide\b", raw):
                return "Guidance / outlook"
            if re.search(r"\bsales|revenue|eps|income|actuals\b", raw):
                return "Results / financials"
        existing = str(category_in or "").strip()
        if existing:
            existing = _shared_visible_period_text(existing)
            existing = re.sub(r"\s*/\s*shareholder returns\b", "", existing, flags=re.I)
            return existing
        return "Results / financials"

    rows_to_delete: List[int] = []
    for rr in range(1, int(ws.max_row or 0) + 1):
        first = str(ws.cell(rr, 1).value or "").strip().lower()
        if first in {"quarter", "category"} or str(ws.cell(rr, 2).value or "").strip().lower() == "category":
            continue
        note_val = ws.cell(rr, 3).value
        metric_val = ws.cell(rr, 4).value
        if _is_visible_fragment_text(note_val) or _is_visible_fragment_text(metric_val):
            rows_to_delete.append(rr)
            continue
        if isinstance(note_val, str):
            ws.cell(rr, 3).value = _clean_note_text(note_val)
        if isinstance(metric_val, str):
            ws.cell(rr, 4).value = _clean_note_text(metric_val)
        cat_val = ws.cell(rr, 2).value
        if cat_val or note_val or metric_val:
            ws.cell(rr, 2).value = _cat(cat_val, ws.cell(rr, 3).value, metric_val)
    for rr in sorted(set(rows_to_delete), reverse=True):
        ws.delete_rows(rr, 1)


def write_quarter_notes_ui_v2(
    deps: QuarterNotesContextAdapterDeps,
    *,
    rank_cutoff: int = 8,
    severity_cutoff: float = 50.0,
    max_rows_per_category: int = 10,
    quarters_shown: int = 12,
) -> list[dict[str, Any]]:
    runtime = deps.runtime
    orchestrator_deps = runtime.get("QuarterNotesUiOrchestratorDeps", QuarterNotesUiOrchestratorDeps)
    write_sheet = runtime.get("write_quarter_notes_ui_sheet", write_quarter_notes_ui_sheet)
    return write_sheet(
        orchestrator_deps(
            wb=runtime["wb"],
            ticker=runtime["ticker"],
            company_profile=runtime["company_profile"],
            is_pbi_profile=runtime["is_pbi_profile"],
            is_gpre_profile=runtime["is_gpre_profile"],
            is_anf_profile=runtime["is_anf_profile"],
            quarter_notes=runtime["quarter_notes"],
            hist=runtime["hist"],
            promises=runtime["promises"],
            cache_root=runtime["cache_root"],
            inputs=runtime["inputs"],
            ui_state=runtime["ui_state"],
            ui_info_rows=runtime["ui_info_rows"],
            ctx_ref=runtime["ctx_ref"],
            quarter_notes_runtime=runtime["quarter_notes_runtime"],
            context_globals=runtime["context_globals"],
            quarter_notes_ui_selection_outer_scope=runtime["quarter_notes_ui_selection_outer_scope"],
            write_analysis_sheet_title_and_metadata=runtime["write_analysis_sheet_title_and_metadata"],
            get_analysis_sheet_style_bundle=runtime["get_analysis_sheet_style_bundle"],
            quarter_notes_view=runtime["quarter_notes_view"],
            resolve_col=runtime["resolve_col"],
            normalize_text=runtime["normalize_text"],
            split_sentences=runtime["split_sentences"],
            dedup_text_key=runtime["dedup_text_key"],
            extract_numeric_patterns=runtime["extract_numeric_patterns"],
            normalize_period=runtime["normalize_period"],
            compact_snippet=runtime["compact_snippet"],
            quarter_label_short=runtime["quarter_label_short"],
            ensure_terminal_period=runtime["ensure_terminal_period"],
            collapse_repeated_leading_ngram=runtime["collapse_repeated_leading_ngram"],
            dedupe_canonical_text_parts=runtime["dedupe_canonical_text_parts"],
            quarter_note_runtime_qd_token=runtime["quarter_note_runtime_qd_token"],
            quarter_note_runtime_signature=runtime["quarter_note_runtime_signature"],
            quarter_note_runtime_cache_key=runtime["quarter_note_runtime_cache_key"],
            shared_build_evidence_event=runtime["shared_build_evidence_event"],
            audit_view=runtime["audit_view"],
            submission_recent_rows=runtime["submission_recent_rows"],
            submission_recent_row_quarter=runtime["submission_recent_row_quarter"],
            sec_docs_for_accession=runtime["sec_docs_for_accession"],
            resolve_cached_doc_path=runtime["resolve_cached_doc_path"],
            path_cache_key=runtime["path_cache_key"],
            read_cached_doc_text=runtime["read_cached_doc_text"],
            parse_date=runtime["parse_date"],
            anf_visible_quarter_note_summaries=runtime["anf_visible_quarter_note_summaries"],
            anf_clean_visible_ui_text=runtime["anf_clean_visible_ui_text"],
            anf_polish_quarter_note_visible_fields=runtime["anf_polish_quarter_note_visible_fields"],
            record_writer_substage=runtime["record_writer_substage"],
            timed_writer_substage=runtime["timed_writer_substage"],
            record_writer_elapsed=runtime["record_writer_elapsed"],
        ),
        (),
        rank_cutoff=rank_cutoff,
        severity_cutoff=severity_cutoff,
        max_rows_per_category=max_rows_per_category,
        quarters_shown=quarters_shown,
    )


def write_quarter_narrative_data_surface(deps: QuarterNotesContextAdapterDeps) -> None:
    runtime = deps.runtime
    ticker_txt = str(runtime["ticker"] or "").strip().upper()
    history_periods = runtime["quarter_narrative_recent_periods_from_frame"](
        runtime["hist"],
        ticker=ticker_txt,
        limit=12,
    )
    records = runtime["quarter_narrative_records_for_context"](
        ticker_txt,
        workbook=runtime["wb"],
        quarter_notes=runtime["quarter_notes"],
        history_periods=history_periods,
        max_per_period=5,
    )
    runtime["ui_state"]["quarter_narrative_records"] = records
    runtime["write_quarter_narrative_data_sheet"](runtime["wb"], ticker_txt, records)


def write_quarter_notes_narrative_ui_surface(deps: QuarterNotesContextAdapterDeps) -> None:
    runtime = deps.runtime
    ticker_txt = str(runtime["ticker"] or "").strip().upper()
    history_periods = runtime["quarter_narrative_recent_periods_from_frame"](
        runtime["hist"],
        ticker=ticker_txt,
        limit=12,
    )
    records = runtime["quarter_narrative_records_for_context"](
        ticker_txt,
        workbook=runtime["wb"],
        quarter_notes=runtime["quarter_notes"],
        history_periods=history_periods,
        max_per_period=5,
    )
    if records or history_periods:
        runtime["ui_state"]["quarter_narrative_records"] = records
        runtime["write_quarter_notes_ui_narrative_sheet"](
            runtime["wb"],
            ticker_txt,
            records,
            history_periods=history_periods,
        )
