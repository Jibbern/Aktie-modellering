"""Quarter_Notes_UI orchestrator.

This module owns the remaining Quarter_Notes_UI sheet orchestration after the
candidate, audit, source-harvest, selection, render, and repair helpers have
been extracted. Workbook behavior stays in the injected helpers and extracted
modules; the main writer context keeps only the callback wrapper.
"""
from __future__ import annotations

import datetime as dt
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import pandas as pd

from .excel_writer_quarter_notes_ui_sources import (
    QuarterNotesUiSourceRescueDeps,
    QuarterNotesUiSourceRescueSupport,
)
from .excel_writer_quarter_notes_ui_candidate_support import (
    QuarterNotesUiCandidateSupport,
    QuarterNotesUiCandidateSupportDeps,
)
from .excel_writer_quarter_notes_ui_audit import (
    QuarterNotesUiAuditDeps,
    QuarterNotesUiAuditTrace,
)
from .excel_writer_quarter_notes_ui_candidate_pipeline import (
    QuarterNotesUiCandidatePipeline,
    QuarterNotesUiCandidatePipelineDeps,
)
from .excel_writer_quarter_notes_ui_capital_allocation import (
    QuarterNotesUiCapitalAllocationDeps,
    build_quarter_notes_ui_capital_allocation_state,
)
from .excel_writer_quarter_notes_ui_source_harvest import (
    QuarterNotesUiSourceHarvestDeps,
    QuarterNotesUiSourceHarvester,
)
from .excel_writer_quarter_notes_ui_selection import (
    QuarterNotesUiSelectionDeps,
    select_quarter_notes_ui_visible_rows,
)
from .excel_writer_quarter_notes_ui_render import (
    QuarterNotesUiRenderDeps,
    install_quarter_notes_ui_render_helpers,
    write_quarter_notes_ui_rendered_blocks,
)
from .excel_writer_quarter_notes_ui_render_repairs import (
    QuarterNotesUiRenderRepairDeps,
    repair_quarter_notes_ui_after_render,
)


@dataclass(frozen=True)
class QuarterNotesUiOrchestratorDeps:
    wb: Any
    ticker: str
    company_profile: Any
    is_pbi_profile: bool
    is_gpre_profile: bool
    is_anf_profile: bool
    quarter_notes: Any
    hist: Any
    promises: Any
    cache_root: Any
    inputs: Any
    ui_state: MutableMapping[str, Any]
    ui_info_rows: List[Dict[str, Any]]
    ctx_ref: Any
    quarter_notes_runtime: Any
    context_globals: MutableMapping[str, Any]
    quarter_notes_ui_selection_outer_scope: MutableMapping[str, Any]
    write_analysis_sheet_title_and_metadata: Callable[..., Any]
    get_analysis_sheet_style_bundle: Callable[..., Mapping[str, Any]]
    quarter_notes_view: Callable[..., Any]
    resolve_col: Callable[..., Any]
    normalize_text: Callable[..., str]
    split_sentences: Callable[..., Sequence[str]]
    dedup_text_key: Callable[..., str]
    extract_numeric_patterns: Callable[..., Any]
    normalize_period: Callable[..., Any]
    compact_snippet: Callable[..., str]
    quarter_label_short: Callable[..., str]
    ensure_terminal_period: Callable[..., str]
    collapse_repeated_leading_ngram: Callable[..., str]
    dedupe_canonical_text_parts: Callable[..., Sequence[str]]
    quarter_note_runtime_qd_token: Callable[..., str]
    quarter_note_runtime_signature: Callable[..., Any]
    quarter_note_runtime_cache_key: Callable[..., Any]
    shared_build_evidence_event: Callable[..., Any]
    audit_view: Callable[..., Any]
    submission_recent_rows: Callable[..., Any]
    submission_recent_row_quarter: Callable[..., Any]
    sec_docs_for_accession: Callable[..., Any]
    resolve_cached_doc_path: Callable[..., Any]
    path_cache_key: Callable[..., str]
    read_cached_doc_text: Callable[..., str]
    parse_date: Callable[..., Any]
    anf_visible_quarter_note_summaries: Callable[..., Any]
    anf_clean_visible_ui_text: Callable[..., str]
    anf_polish_quarter_note_visible_fields: Callable[..., Any]
    record_writer_substage: Callable[..., None]
    timed_writer_substage: Callable[..., Any]
    record_writer_elapsed: Callable[..., None]


def write_quarter_notes_ui_sheet(
    deps: QuarterNotesUiOrchestratorDeps,
    rows: Sequence[Mapping[str, Any]],
    *,
    rank_cutoff: int = 8,
    severity_cutoff: float = 50.0,
    max_rows_per_category: int = 10,
    quarters_shown: int = 12,
) -> List[Dict[str, Any]]:
    _ = rows
    wb = deps.wb
    ticker = deps.ticker
    company_profile = deps.company_profile
    is_pbi_profile = deps.is_pbi_profile
    is_gpre_profile = deps.is_gpre_profile
    is_anf_profile = deps.is_anf_profile
    quarter_notes = deps.quarter_notes
    hist = deps.hist
    promises = deps.promises
    cache_root = deps.cache_root
    inputs = deps.inputs
    ui_state = deps.ui_state
    ui_info_rows = deps.ui_info_rows
    ctx_ref = deps.ctx_ref
    quarter_notes_runtime = deps.quarter_notes_runtime
    context_globals = deps.context_globals
    runtime_globals = {**context_globals, **globals()}
    _quarter_notes_ui_selection_outer_scope = deps.quarter_notes_ui_selection_outer_scope
    _write_analysis_sheet_title_and_metadata = deps.write_analysis_sheet_title_and_metadata
    _get_analysis_sheet_style_bundle = deps.get_analysis_sheet_style_bundle
    _quarter_notes_view = deps.quarter_notes_view
    _resolve_col = deps.resolve_col
    glx_normalize_text = deps.normalize_text
    glx_split_sentences = deps.split_sentences
    glx_dedup_text_key = deps.dedup_text_key
    glx_extract_numeric_patterns = deps.extract_numeric_patterns
    glx_normalize_period = deps.normalize_period
    qn_compact_snippet = deps.compact_snippet
    _quarter_label_short = deps.quarter_label_short
    _ensure_terminal_period = deps.ensure_terminal_period
    _collapse_repeated_leading_ngram_local = deps.collapse_repeated_leading_ngram
    _dedupe_canonical_text_parts_local = deps.dedupe_canonical_text_parts
    _quarter_note_runtime_qd_token = deps.quarter_note_runtime_qd_token
    _quarter_note_runtime_signature = deps.quarter_note_runtime_signature
    _quarter_note_runtime_cache_key = deps.quarter_note_runtime_cache_key
    shared_build_evidence_event = deps.shared_build_evidence_event
    _audit_view = deps.audit_view
    _submission_recent_rows = deps.submission_recent_rows
    _submission_recent_row_quarter = deps.submission_recent_row_quarter
    _sec_docs_for_accession = deps.sec_docs_for_accession
    _resolve_cached_doc_path = deps.resolve_cached_doc_path
    _path_cache_key = deps.path_cache_key
    _read_cached_doc_text = deps.read_cached_doc_text
    parse_date = deps.parse_date
    _anf_visible_quarter_note_summaries = deps.anf_visible_quarter_note_summaries
    _anf_clean_visible_ui_text = deps.anf_clean_visible_ui_text
    _anf_polish_quarter_note_visible_fields = deps.anf_polish_quarter_note_visible_fields
    _record_writer_substage = deps.record_writer_substage
    _timed_writer_substage = deps.timed_writer_substage
    _record_writer_elapsed = deps.record_writer_elapsed
    setup_start = time.perf_counter()
    ws = wb.create_sheet("Quarter_Notes_UI")
    qa_rows: List[Dict[str, Any]] = []
    qn_note_col_width_default = 150.0
    ts = datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    qn_header_text = f"Generated at {ts} | Quarter blocks | high-signal notes mode | quarters_shown={quarters_shown}"
    _write_analysis_sheet_title_and_metadata(
        ws,
        "Quarter Notes",
        qn_header_text,
        max_col=4,
    )
    ws.row_dimensions[1].height = 27.0
    ws.column_dimensions["A"].width = 14.0
    ws.column_dimensions["B"].width = 30.0
    ws.column_dimensions["C"].width = 108.0
    ws.column_dimensions["D"].width = 28.0
    if quarter_notes is None or quarter_notes.empty:
        ws["A3"] = "No data."
        ws.freeze_panes = "A3"
        _record_writer_elapsed("write_excel.ui.render.quarter_notes.setup", time.perf_counter() - setup_start)
        return qa_rows

    df = _quarter_notes_view()
    q_col = _resolve_col(df, ["quarter", "quarter_end", "as_of_quarter"])
    cat_col = _resolve_col(df, ["category", "tag", "topic"])
    claim_col = _resolve_col(df, ["claim", "note", "headline", "body"])
    note_col = _resolve_col(df, ["note", "body"])
    body_col = _resolve_col(df, ["body"])
    sev_col = _resolve_col(df, ["severity", "qa_severity", "status"])
    sev_score_col = _resolve_col(df, ["severity_score", "score"])
    metric_col = _resolve_col(df, ["metric_ref", "metric", "metric_tag"])
    metric_val_col = _resolve_col(df, ["metric_val", "metric_value", "value"])
    note_id_col = _resolve_col(df, ["note_id", "id"])
    ev_snip_col = _resolve_col(df, ["evidence_snippet", "snippet"])
    ev_json_col = _resolve_col(df, ["evidence_json"])
    if q_col is None or cat_col is None or claim_col is None:
        ws["A3"] = "Missing required source columns."
        _record_writer_elapsed("write_excel.ui.render.quarter_notes.setup", time.perf_counter() - setup_start)
        return qa_rows

    quarter_notes_audit_enabled = bool(getattr(inputs, "quarter_notes_audit", False))
    quarter_notes_audit_compact_mode = quarter_notes_audit_enabled and not bool(
        getattr(inputs, "capture_saved_workbook_provenance", True)
    )
    quarter_notes_audit_trace = QuarterNotesUiAuditTrace(
        QuarterNotesUiAuditDeps(
            enabled=quarter_notes_audit_enabled,
            compact_mode=quarter_notes_audit_compact_mode,
            normalize_text=glx_normalize_text,
            collapse_repeated_leading_ngram=_collapse_repeated_leading_ngram_local,
            dedupe_canonical_text_parts=_dedupe_canonical_text_parts_local,
        )
    )
    quarter_notes_audit_rows = quarter_notes_audit_trace.raw_rows
    _audit_family_hint_local = quarter_notes_audit_trace.family_hint
    _audit_subject_variant_hint_local = quarter_notes_audit_trace.subject_variant_hint
    _capital_allocation_confidence_local = quarter_notes_audit_trace.capital_allocation_confidence
    _clean_audit_excerpt_local = quarter_notes_audit_trace.clean_excerpt
    _canonicalize_audit_excerpt_local = quarter_notes_audit_trace.canonicalize_excerpt
    _audit_doc_family_local = quarter_notes_audit_trace.doc_family
    _quarter_notes_audit_canonical_rows_local = quarter_notes_audit_trace.canonical_rows
    _quarter_note_trace_id_local = quarter_notes_audit_trace.trace_id
    _audit_attrition_class_local = quarter_notes_audit_trace.attrition_class
    _emit_quarter_note_audit_row = quarter_notes_audit_trace.emit
    _ensure_note_trace_id_local = quarter_notes_audit_trace.ensure_trace_id
    _audit_register_existing_candidate_local = quarter_notes_audit_trace.register_existing
    quarter_slice_cache: Dict[str, Dict[date, List[Dict[str, Any]]]] = {}
    # Run-scoped only: recreated on every export, keyed by semantic note state
    # plus quarter context so copied note dicts can safely reuse late-stage work.
    note_runtime_cache: Dict[Tuple[str, str, Tuple[str, ...]], Any] = {}
    text_runtime_cache: Dict[Tuple[str, str], Any] = {}
    evidence_event_runtime_cache: Dict[Tuple[str, ...], Any] = {}

    def _qd_runtime_token_local(qd_ref: Optional[date]) -> str:
        return _quarter_note_runtime_qd_token(qd_ref)

    def _note_runtime_signature_local(item: Dict[str, Any]) -> Tuple[str, ...]:
        return _quarter_note_runtime_signature(item)

    def _note_cached_runtime_value_local(
        cache_name: str,
        item: Dict[str, Any],
        qd_ref: Optional[date],
        builder: Any,
    ) -> Any:
        cache_key = _quarter_note_runtime_cache_key(cache_name, item, qd_ref)
        if cache_key in note_runtime_cache:
            return note_runtime_cache[cache_key]
        value = builder()
        note_runtime_cache[cache_key] = value
        return value

    def _text_cached_runtime_value_local(cache_name: str, text_in: Any, builder: Any) -> Any:
        norm_txt = glx_normalize_text(str(text_in or ""))
        cache_key = (cache_name, norm_txt)
        if cache_key in text_runtime_cache:
            return text_runtime_cache[cache_key]
        value = builder(norm_txt)
        text_runtime_cache[cache_key] = value
        return value

    def _shared_evidence_event_cached_local(
        text_in: Any,
        *,
        source_type: Any,
        metric_hint: Any = "",
        theme_hint: Any = "",
        base_score: float = 0.0,
        quietly_removed: bool = False,
        period_norm: Any = "",
        source_doc: Any = "",
        display_text_hint: Any = "",
    ) -> Any:
        cache_key = (
            glx_normalize_text(str(text_in or "")),
            str(source_type or ""),
            str(metric_hint or ""),
            str(theme_hint or ""),
            f"{float(base_score or 0.0):.6f}",
            "1" if quietly_removed else "0",
            str(period_norm or ""),
            str(source_doc or ""),
            glx_normalize_text(str(display_text_hint or "")),
        )
        if cache_key in evidence_event_runtime_cache:
            return evidence_event_runtime_cache[cache_key]
        event_obj = shared_build_evidence_event(
            text_in,
            source_type=source_type,
            metric_hint=metric_hint,
            theme_hint=theme_hint,
            base_score=base_score,
            quietly_removed=quietly_removed,
            period_norm=period_norm,
            source_doc=source_doc,
            display_text_hint=display_text_hint,
        )
        evidence_event_runtime_cache[cache_key] = event_obj
        return event_obj

    def _rows_grouped_by_quarter_local(cache_key: str, rows_factory: Any) -> Dict[date, List[Dict[str, Any]]]:
        cached = quarter_slice_cache.get(cache_key)
        if cached is not None:
            return cached
        build_start = time.perf_counter()
        grouped: Dict[date, List[Dict[str, Any]]] = {}
        try:
            source_rows = list(rows_factory() or [])
        except Exception:
            source_rows = []
        for item in source_rows:
            q_ts = pd.to_datetime(item.get("quarter"), errors="coerce")
            if pd.isna(q_ts):
                continue
            grouped.setdefault(q_ts.date(), []).append(dict(item))
        quarter_slice_cache[cache_key] = grouped
        safe_cache_key = re.sub(r"[^A-Za-z0-9_]+", "_", str(cache_key or "unknown"))[:80]
        _record_writer_elapsed(
            f"write_excel.ui.render.quarter_notes.setup.rows_grouped.{safe_cache_key}",
            time.perf_counter() - build_start,
        )
        return grouped

    def _quarter_rows_copy_local(cache_key: str, rows_factory: Any, qd_ref: date) -> List[Dict[str, Any]]:
        grouped = _rows_grouped_by_quarter_local(cache_key, rows_factory)
        return [dict(item) for item in grouped.get(qd_ref, [])]

    def _quarter_notes_raw_records_by_quarter_local() -> Dict[date, List[Dict[str, Any]]]:
        cache_key = "__quarter_notes_raw_records__"
        cached = quarter_slice_cache.get(cache_key)
        if cached is not None:
            return cached
        grouped: Dict[date, List[Dict[str, Any]]] = {}
        rescue_view = quarter_notes if isinstance(quarter_notes, pd.DataFrame) else pd.DataFrame()
        if isinstance(rescue_view, pd.DataFrame) and not rescue_view.empty:
            rescue_quarter_col = _resolve_col(rescue_view, ["quarter", "created_quarter", "first_seen_quarter"])
            if rescue_quarter_col:
                rescue_df = rescue_view.copy()
                rescue_df["__quarter_date__"] = pd.to_datetime(
                    rescue_df.get(rescue_quarter_col), errors="coerce"
                ).dt.date
                rescue_df = rescue_df[rescue_df["__quarter_date__"].notna()]
                for qd_key, grp in rescue_df.groupby("__quarter_date__", sort=False):
                    if isinstance(qd_key, date):
                        grouped[qd_key] = grp.drop(columns=["__quarter_date__"], errors="ignore").to_dict("records")
        quarter_slice_cache[cache_key] = grouped
        return grouped

    df["_quarter"] = pd.to_datetime(df[q_col], errors="coerce").dt.date
    df = df[df["_quarter"].notna()].copy()
    quarters = sorted(df["_quarter"].unique().tolist(), reverse=True)[: max(1, quarters_shown)]
    df = df[df["_quarter"].isin(quarters)].copy()
    df["_category"] = df[cat_col].astype(str).str.strip().replace("", "Uncategorized")
    recent_ui_quarters = set(quarters[:2])
    _quarter_notes_candidate_support_state: Dict[str, Any] = {}
    _quarter_notes_candidate_support = QuarterNotesUiCandidateSupport(
        QuarterNotesUiCandidateSupportDeps(
            runtime={
                **runtime_globals,
                **_quarter_notes_ui_selection_outer_scope,
                **locals(),
                "quarter_notes_candidate_support_state": _quarter_notes_candidate_support_state,
                "ctx_ref": ctx_ref,
                "ctx_ref_getter": lambda: ctx_ref,
            }
        )
    )
    _q_label = _quarter_notes_candidate_support._q_label
    _parse_json = _quarter_notes_candidate_support._parse_json
    _source_meta = _quarter_notes_candidate_support._source_meta
    _candidate_texts = _quarter_notes_candidate_support._candidate_texts
    _extract_time_context = _quarter_notes_candidate_support._extract_time_context
    _extract_deadline = _quarter_notes_candidate_support._extract_deadline
    _bucket = _quarter_notes_candidate_support._bucket
    _metric_tag = _quarter_notes_candidate_support._metric_tag
    placeholder_phrases = _quarter_notes_candidate_support.placeholder_phrases
    exclude_phrases = _quarter_notes_candidate_support.exclude_phrases
    hard_drop_re = _quarter_notes_candidate_support.hard_drop_re
    forward_intent_re = _quarter_notes_candidate_support.forward_intent_re
    period_anchor_re = _quarter_notes_candidate_support.period_anchor_re
    year_ref_re = _quarter_notes_candidate_support.year_ref_re
    stale_historical_re = _quarter_notes_candidate_support.stale_historical_re
    segment_driver_boost_re = _quarter_notes_candidate_support.segment_driver_boost_re
    change_re = _quarter_notes_candidate_support.change_re
    driver_re = _quarter_notes_candidate_support.driver_re
    driver_language_re = _quarter_notes_candidate_support.driver_language_re
    footnote_disclosure_re = _quarter_notes_candidate_support.footnote_disclosure_re
    numeric_token_re = _quarter_notes_candidate_support.numeric_token_re
    causality_re = _quarter_notes_candidate_support.causality_re
    driver_noun_re = _quarter_notes_candidate_support.driver_noun_re
    highlight_topic_re = _quarter_notes_candidate_support.highlight_topic_re
    capital_alloc_include_re = _quarter_notes_candidate_support.capital_alloc_include_re
    capital_alloc_context_re = _quarter_notes_candidate_support.capital_alloc_context_re
    capital_alloc_exclude_re = _quarter_notes_candidate_support.capital_alloc_exclude_re
    bank_financing_term_re = _quarter_notes_candidate_support.bank_financing_term_re
    bank_financing_reason_re = _quarter_notes_candidate_support.bank_financing_reason_re
    corporate_revenue_restatement_re = _quarter_notes_candidate_support.corporate_revenue_restatement_re
    bare_restatement_re = _quarter_notes_candidate_support.bare_restatement_re
    tone_topic_re = _quarter_notes_candidate_support.tone_topic_re
    tone_sentiment_re = _quarter_notes_candidate_support.tone_sentiment_re
    future_soft_re = _quarter_notes_candidate_support.future_soft_re
    program_hook_re = _quarter_notes_candidate_support.program_hook_re
    is_tabular_fragment = _quarter_notes_candidate_support.is_tabular_fragment
    margin_driver_re = _quarter_notes_candidate_support.margin_driver_re
    pricing_mix_volume_re = _quarter_notes_candidate_support.pricing_mix_volume_re
    sga_driver_re = _quarter_notes_candidate_support.sga_driver_re
    rd_driver_re = _quarter_notes_candidate_support.rd_driver_re
    restructuring_driver_re = _quarter_notes_candidate_support.restructuring_driver_re
    other_expense_driver_re = _quarter_notes_candidate_support.other_expense_driver_re
    segment_re = _quarter_notes_candidate_support.segment_re
    BACKFILL_PRIOR_YEAR_COMPARISONS = _quarter_notes_candidate_support.BACKFILL_PRIOR_YEAR_COMPARISONS
    INCLUDE_GUIDANCE_SUMMARY_IN_QUARTER_NOTES = _quarter_notes_candidate_support.INCLUDE_GUIDANCE_SUMMARY_IN_QUARTER_NOTES
    SHOW_DROPPED_THEMES = _quarter_notes_candidate_support.SHOW_DROPPED_THEMES
    _has_numeric_range_or_point = _quarter_notes_candidate_support._has_numeric_range_or_point
    _has_numeric_range = _quarter_notes_candidate_support._has_numeric_range
    _cost_savings_numeric_provenance = _quarter_notes_candidate_support._cost_savings_numeric_provenance
    _estimate_amount_m = _quarter_notes_candidate_support._estimate_amount_m
    _detect_segment = _quarter_notes_candidate_support._detect_segment
    _driver_tag = _quarter_notes_candidate_support._driver_tag
    _theme_signature = _quarter_notes_candidate_support._theme_signature
    _parse_money_amount = _quarter_notes_candidate_support._parse_money_amount
    _extract_ytd_quarter_buyback_components_early_local = _quarter_notes_candidate_support._extract_ytd_quarter_buyback_components_early_local
    _format_early_buyback_execution_summary_local = _quarter_notes_candidate_support._format_early_buyback_execution_summary_local
    _extract_executed_buyback_amount = _quarter_notes_candidate_support._extract_executed_buyback_amount
    _extract_buyback_cash_from_text = _quarter_notes_candidate_support._extract_buyback_cash_from_text
    _extract_post_quarter_buyback_commentary_local = _quarter_notes_candidate_support._extract_post_quarter_buyback_commentary_local
    _classify_distribution_signal = _quarter_notes_candidate_support._classify_distribution_signal

    _quarter_notes_capital_allocation_result = build_quarter_notes_ui_capital_allocation_state(
        QuarterNotesUiCapitalAllocationDeps(
            hist=hist,
            promises=promises,
            quarter_notes_df=df,
            cache_root=cache_root,
            candidate_support=_quarter_notes_candidate_support,
            audit_view=_audit_view,
            resolve_col=_resolve_col,
            submission_recent_rows=_submission_recent_rows,
            submission_recent_row_quarter=_submission_recent_row_quarter,
            sec_docs_for_accession=_sec_docs_for_accession,
            read_cached_doc_text=_read_cached_doc_text,
            normalize_text=glx_normalize_text,
            split_sentences=glx_split_sentences,
            dedup_text_key=glx_dedup_text_key,
            compact_snippet=qn_compact_snippet,
        )
    )
    cap_alloc_exec_by_q = _quarter_notes_capital_allocation_result.cap_alloc_exec_by_q
    cap_alloc_tone_by_q = _quarter_notes_capital_allocation_result.cap_alloc_tone_by_q

    _quarter_notes_source_harvester = QuarterNotesUiSourceHarvester(
        QuarterNotesUiSourceHarvestDeps(
            runtime={**runtime_globals, **_quarter_notes_ui_selection_outer_scope, **locals()},
            quarters=quarters,
            quarter_notes_df=df,
            ui_info_rows=ui_info_rows,
            quarter_notes_runtime=quarter_notes_runtime,
            candidate_support=_quarter_notes_candidate_support,
            submission_recent_rows=_submission_recent_rows,
            submission_recent_row_quarter=_submission_recent_row_quarter,
            resolve_cached_doc_path=_resolve_cached_doc_path,
            sec_docs_for_accession=_sec_docs_for_accession,
            path_cache_key=_path_cache_key,
            read_cached_doc_text=_read_cached_doc_text,
            parse_date=parse_date,
        )
    )
    _is_fy_block = _quarter_notes_source_harvester.is_fy_block
    _fy_block_for_year = _quarter_notes_source_harvester.fy_block_for_year
    _harvest_fy_expense_driver_rows = _quarter_notes_source_harvester.harvest_fy_expense_driver_rows
    _harvest_interim_expense_driver_rows = _quarter_notes_source_harvester.harvest_interim_expense_driver_rows
    _harvest_mdna_cashflow_driver_rows = _quarter_notes_source_harvester.harvest_mdna_cashflow_driver_rows
    _harvest_debt_pension_action_rows = _quarter_notes_source_harvester.harvest_debt_pension_action_rows

    _quarter_notes_candidate_pipeline_result = QuarterNotesUiCandidatePipeline(
        QuarterNotesUiCandidatePipelineDeps(
            runtime={**runtime_globals, **_quarter_notes_ui_selection_outer_scope, **locals()},
            audit_trace=quarter_notes_audit_trace,
            candidate_support=_quarter_notes_candidate_support,
            candidate_support_state=_quarter_notes_candidate_support_state,
            ui_info_rows=ui_info_rows,
            harvest_fy_expense_driver_rows=_harvest_fy_expense_driver_rows,
            harvest_interim_expense_driver_rows=_harvest_interim_expense_driver_rows,
            harvest_mdna_cashflow_driver_rows=_harvest_mdna_cashflow_driver_rows,
            harvest_debt_pension_action_rows=_harvest_debt_pension_action_rows,
        )
    ).build()
    records = _quarter_notes_candidate_pipeline_result.records
    best_by_key = _quarter_notes_candidate_pipeline_result.best_by_key
    q_stats = _quarter_notes_candidate_pipeline_result.q_stats
    ui_info_rows = _quarter_notes_candidate_pipeline_result.ui_info_rows
    _quarter_notes_candidate_support = _quarter_notes_candidate_pipeline_result.candidate_support
    _gpre_seed_rescue_rows = _quarter_notes_candidate_support._gpre_seed_rescue_rows
    _gpre_raw_note_rescue_rows = _quarter_notes_candidate_support._gpre_raw_note_rescue_rows
    _gpre_source_note_rescue_rows = _quarter_notes_candidate_support._gpre_source_note_rescue_rows
    _profile_milestone_source_rows = _quarter_notes_candidate_support._profile_milestone_source_rows

    # Profile-declared reviewed milestones are source-owned candidates, not a
    # fallback that depends on another narrative row surviving lexicon filters.
    for candidate in _profile_milestone_source_rows():
        candidate_key = (
            candidate.get("quarter"),
            candidate.get("bucket"),
            str(candidate.get("metric_tag") or "").lower(),
            glx_dedup_text_key(candidate.get("text_full")),
        )
        if not candidate_key[-1]:
            continue
        previous = best_by_key.get(candidate_key)
        if previous is None or float(candidate.get("score") or 0.0) > float(previous.get("score") or 0.0):
            best_by_key[candidate_key] = dict(candidate)

    if not best_by_key:
        ws["A2"] = "No notes after lexicon filtering."
        ws.freeze_panes = "A2"
        return qa_rows

    records = list(best_by_key.values())

    _pbi_seed_rescue_rows_raw = _quarter_notes_candidate_support._pbi_seed_rescue_rows
    _generic_source_note_rescue_rows_raw = _quarter_notes_candidate_support._generic_source_note_rescue_rows
    _pbi_promise_note_rescue_rows_raw = _quarter_notes_candidate_support._pbi_promise_note_rescue_rows
    _pbi_source_note_rescue_rows_raw = _quarter_notes_candidate_support._pbi_source_note_rescue_rows
    def _pbi_seed_rescue_rows() -> List[Dict[str, Any]]:
        return _pbi_seed_rescue_rows_raw()
    def _generic_source_note_rescue_rows() -> List[Dict[str, Any]]:
        return _generic_source_note_rescue_rows_raw()
    def _pbi_promise_note_rescue_rows() -> List[Dict[str, Any]]:
        return _pbi_promise_note_rescue_rows_raw()
    def _pbi_source_note_rescue_rows() -> List[Dict[str, Any]]:
        return _pbi_source_note_rescue_rows_raw()
    _gpre_quantified_note_summary_local = _quarter_notes_candidate_support._gpre_quantified_note_summary_local
    _gpre_structured_support_source_ok_local = _quarter_notes_candidate_support._gpre_structured_support_source_ok_local
    _evidence_snippet_blob_local = _quarter_notes_candidate_support._evidence_snippet_blob_local
    _pbi_note_detail_score_local = _quarter_notes_candidate_support._pbi_note_detail_score_local
    _note_has_weak_generic_verb_local = _quarter_notes_candidate_support._note_has_weak_generic_verb_local
    _note_summary_quality_key_local = _quarter_notes_candidate_support._note_summary_quality_key_local
    _prefer_note_summary_local = _quarter_notes_candidate_support._prefer_note_summary_local
    _pbi_extra_note_labels_local = _quarter_notes_candidate_support._pbi_extra_note_labels_local
    _pbi_detail_preserving_note_summary_local = _quarter_notes_candidate_support._pbi_detail_preserving_note_summary_local
    _pbi_contextual_note_summary_local = _quarter_notes_candidate_support._pbi_contextual_note_summary_local
    _fmt_note_share_count_local = _quarter_notes_candidate_support._fmt_note_share_count_local
    _fmt_short_millions_note_local = _quarter_notes_candidate_support._fmt_short_millions_note_local
    _buyback_anchor_from_text_local = _quarter_notes_candidate_support._buyback_anchor_from_text_local
    _is_cumulative_buyback_context_local = _quarter_notes_candidate_support._is_cumulative_buyback_context_local
    _has_negative_buyback_statement_for_ref_local = _quarter_notes_candidate_support._has_negative_buyback_statement_for_ref_local
    _extract_ytd_buyback_including_quarter_split_local = _quarter_notes_candidate_support._extract_ytd_buyback_including_quarter_split_local
    _is_debt_repurchase_noise_local = _quarter_notes_candidate_support._is_debt_repurchase_noise_local
    _parse_buyback_money_local = _quarter_notes_candidate_support._parse_buyback_money_local
    _extract_buyback_table_execution_local = _quarter_notes_candidate_support._extract_buyback_table_execution_local
    _quarter_end_for_month_local = _quarter_notes_candidate_support._quarter_end_for_month_local
    _explicit_event_quarter_override_local = _quarter_notes_candidate_support._explicit_event_quarter_override_local
    _extract_buyback_execution_components_local = _quarter_notes_candidate_support._extract_buyback_execution_components_local
    _compose_buyback_execution_summary_local = _quarter_notes_candidate_support._compose_buyback_execution_summary_local
    _build_post_quarter_buyback_companion_row_raw_local = _quarter_notes_candidate_support._build_post_quarter_buyback_companion_row_local
    _build_pbi_q1_2026_context_rows_raw_local = _quarter_notes_candidate_support._build_pbi_q1_2026_context_rows_local
    def _build_post_quarter_buyback_companion_row_local(qd: Any, q_items: Sequence[Mapping[str, Any]]) -> Any:
        return _build_post_quarter_buyback_companion_row_raw_local(qd, q_items)
    def _build_pbi_q1_2026_context_rows_local(qd: Any, q_items: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        return _build_pbi_q1_2026_context_rows_raw_local(qd, q_items)
    _buyback_summary_specificity_score_local = _quarter_notes_candidate_support._buyback_summary_specificity_score_local
    _doc_has_buyback_execution_signal_local = _quarter_notes_candidate_support._doc_has_buyback_execution_signal_local
    best_doc_buyback_execution_doc_hit_cache = _quarter_notes_candidate_support.best_doc_buyback_execution_doc_hit_cache
    _best_doc_buyback_execution_doc_hit_local = _quarter_notes_candidate_support._best_doc_buyback_execution_doc_hit_local
    _best_doc_buyback_execution_summary_local = _quarter_notes_candidate_support._best_doc_buyback_execution_summary_local
    _best_doc_buyback_execution_components_local = _quarter_notes_candidate_support._best_doc_buyback_execution_components_local
    _capital_allocation_split_summaries_local = _quarter_notes_candidate_support._capital_allocation_split_summaries_local
    _pbi_capital_allocation_split_summaries_local = _quarter_notes_candidate_support._pbi_capital_allocation_split_summaries_local
    _pbi_explicit_note_split_variants_local = _quarter_notes_candidate_support._pbi_explicit_note_split_variants_local
    _pbi_is_locked_capital_allocation_summary_local = _quarter_notes_candidate_support._pbi_is_locked_capital_allocation_summary_local
    _sector_pack_keys_for_text_local = _quarter_notes_candidate_support._sector_pack_keys_for_text_local
    _profile_signal_terms_local = _quarter_notes_candidate_support._profile_signal_terms_local
    _profile_sector_pack_keys_local = _quarter_notes_candidate_support._profile_sector_pack_keys_local
    _text_contains_symbol_marker_local = _quarter_notes_candidate_support._text_contains_symbol_marker_local
    _narrative_text_matches_current_company_local = _quarter_notes_candidate_support._narrative_text_matches_current_company_local
    _looks_like_xbrl_fact_blob_local = _quarter_notes_candidate_support._looks_like_xbrl_fact_blob_local
    _note_sector_pack_keys_local = _quarter_notes_candidate_support._note_sector_pack_keys_local
    _iter_quarter_scoped_sec_cache_texts_local = _quarter_notes_candidate_support._iter_quarter_scoped_sec_cache_texts_local
    _material_source_text_cache = _quarter_notes_candidate_support._material_source_text_cache
    _material_text_local = _quarter_notes_candidate_support._material_text_local
    _management_text_windows_local = _quarter_notes_candidate_support._management_text_windows_local
    _pattern_match_windows_local = _quarter_notes_candidate_support._pattern_match_windows_local
    _iter_quarter_scoped_material_texts_local = _quarter_notes_candidate_support._iter_quarter_scoped_material_texts_local
    _quarter_scoped_sec_cache_texts_by_quarter_local = _quarter_notes_candidate_support._quarter_scoped_sec_cache_texts_by_quarter_local
    _quarter_scoped_material_texts_by_quarter_local = _quarter_notes_candidate_support._quarter_scoped_material_texts_by_quarter_local
    _build_post_quarter_buyback_companion_row_raw_local = _quarter_notes_candidate_support._build_post_quarter_buyback_companion_row_local
    _build_pbi_q1_2026_context_rows_raw_local = _quarter_notes_candidate_support._build_pbi_q1_2026_context_rows_local
    def _build_post_quarter_buyback_companion_row_local(qd: Any, q_items: Sequence[Mapping[str, Any]]) -> Any:
        return _build_post_quarter_buyback_companion_row_raw_local(qd, q_items)
    def _build_pbi_q1_2026_context_rows_local(qd: Any, q_items: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        return _build_pbi_q1_2026_context_rows_raw_local(qd, q_items)
    _best_doc_buyback_execution_doc_hit_local = _quarter_notes_candidate_support._best_doc_buyback_execution_doc_hit_local
    _best_doc_buyback_execution_summary_local = _quarter_notes_candidate_support._best_doc_buyback_execution_summary_local
    _best_doc_buyback_execution_components_local = _quarter_notes_candidate_support._best_doc_buyback_execution_components_local
    _guidance_summary_result = _quarter_notes_candidate_support.append_guidance_summary_rows(records, quarters)
    records = _guidance_summary_result.records

    span_map: Dict[Tuple[str, str, str], Dict[str, date]] = {}
    mention_map: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for rec in records:
        qd = rec.get("quarter")
        if not isinstance(qd, date):
            continue
        key = (
            str(rec.get("bucket") or ""),
            str(rec.get("metric_tag") or rec.get("metric_canon") or "").lower(),
            glx_dedup_text_key(rec.get("text_full")),
        )
        sp = span_map.get(key)
        if sp is None:
            span_map[key] = {"first": qd, "last": qd}
        else:
            if qd < sp["first"]:
                sp["first"] = qd
            if qd > sp["last"]:
                sp["last"] = qd

        mkey = (
            str(rec.get("bucket") or ""),
            str(rec.get("metric_canon") or rec.get("metric_tag") or "").lower(),
            str(rec.get("target_period_norm") or rec.get("period_key") or "UNK"),
            str(rec.get("guidance_type") or rec.get("candidate_type") or "text").lower(),
        )
        mk = mention_map.get(mkey)
        if mk is None:
            mk = {
                "first": qd,
                "last": qd,
                "last_numeric": None,
                "last_text": None,
            }
            mention_map[mkey] = mk
        else:
            if qd < mk["first"]:
                mk["first"] = qd
            if qd > mk["last"]:
                mk["last"] = qd
        m_kind = str(rec.get("mention_kind") or ("numeric" if bool(rec.get("has_numeric_range_or_point")) else "text")).lower()
        if m_kind == "numeric":
            if mk.get("last_numeric") is None or qd >= mk.get("last_numeric"):
                mk["last_numeric"] = qd
        else:
            if mk.get("last_text") is None or qd >= mk.get("last_text"):
                mk["last_text"] = qd

    for rec in records:
        skey = (
            str(rec.get("bucket") or ""),
            str(rec.get("metric_tag") or rec.get("metric_canon") or "").lower(),
            glx_dedup_text_key(rec.get("text_full")),
        )
        sp = span_map.get(skey)
        if sp is not None:
            rec["first_seen_quarter_end"] = str(sp["first"])
            rec["last_seen_quarter_end"] = str(sp["last"])
        mkey = (
            str(rec.get("bucket") or ""),
            str(rec.get("metric_canon") or rec.get("metric_tag") or "").lower(),
            str(rec.get("target_period_norm") or rec.get("period_key") or "UNK"),
            str(rec.get("guidance_type") or rec.get("candidate_type") or "text").lower(),
        )
        mk = mention_map.get(mkey)
        if mk is None:
            continue
        rec["first_seen_quarter_end"] = str(mk["first"])
        rec["last_seen_quarter_end"] = str(mk["last"])
        rec["last_seen_numeric_quarter_end"] = str(mk["last_numeric"]) if mk.get("last_numeric") is not None else ""
        rec["last_seen_text_quarter_end"] = str(mk["last_text"]) if mk.get("last_text") is not None else ""
    by_quarter: Dict[date, List[Dict[str, Any]]] = {}
    for rec in records:
        by_quarter.setdefault(rec["quarter"], []).append(rec)

    _quarter_notes_ui_runtime = {**runtime_globals, **_quarter_notes_ui_selection_outer_scope, **locals()}
    install_quarter_notes_ui_render_helpers(_quarter_notes_ui_runtime)
    _quarter_notes_selection_result = select_quarter_notes_ui_visible_rows(
        QuarterNotesUiSelectionDeps(runtime=_quarter_notes_ui_runtime)
    )
    rows_by_quarter = _quarter_notes_selection_result.rows_by_quarter
    ui_info_rows[:] = _quarter_notes_selection_result.ui_info_rows
    _quarter_notes_selection_helpers = _quarter_notes_selection_result.helpers
    block_assembly_start = _quarter_notes_selection_helpers["block_assembly_start"]
    selection_start = _quarter_notes_selection_helpers["selection_start"]
    _clean_dropped_label = _quarter_notes_selection_helpers["_clean_dropped_label"]
    _note_final_display_summary_local = _quarter_notes_selection_helpers["_note_final_display_summary_local"]
    _note_preview_summary_local = _quarter_notes_selection_helpers["_note_preview_summary_local"]
    _pbi_bucket_for_label_local = _quarter_notes_selection_helpers["_pbi_bucket_for_label_local"]
    _pbi_guidance_note_labels = _quarter_notes_selection_helpers["_pbi_guidance_note_labels"]
    _pbi_relabel_generic_guidance_note = _quarter_notes_selection_helpers["_pbi_relabel_generic_guidance_note"]

    audit_state_finalize_start = time.perf_counter()
    ui_state["quarter_notes_ui_rows"] = {
        qd: [
            dict(it)
            for it in rows_by_quarter.get(qd, [])
            if str(it.get("candidate_type") or "").strip().lower() != "ui_footer"
        ]
        for qd in quarters
    }
    if quarter_notes_audit_enabled:
        ui_state["quarter_notes_audit_rows_raw"] = list(quarter_notes_audit_rows)
        ui_state["quarter_notes_audit_rows"] = _quarter_notes_audit_canonical_rows_local(list(quarter_notes_audit_rows))
        ui_state["quarter_notes_header_text"] = qn_header_text
    _record_writer_elapsed(
        "write_excel.ui.render.quarter_notes.selection.block_assembly.audit_state_finalize",
        time.perf_counter() - audit_state_finalize_start,
    )
    _record_writer_elapsed(
        "write_excel.ui.render.quarter_notes.selection.block_assembly",
        time.perf_counter() - block_assembly_start,
    )

    _record_writer_elapsed("write_excel.ui.render.quarter_notes.selection", time.perf_counter() - selection_start)

    render_blocks_start = time.perf_counter()
    _quarter_notes_render_result = write_quarter_notes_ui_rendered_blocks(
        QuarterNotesUiRenderDeps(
            ws=ws,
            quarters=quarters,
            rows_by_quarter=rows_by_quarter,
            ticker=ticker,
            is_pbi_profile=is_pbi_profile,
            is_gpre_profile=is_gpre_profile,
            is_anf_profile=is_anf_profile,
            qn_note_col_width_default=qn_note_col_width_default,
            runtime={**_quarter_notes_ui_runtime, **locals()},
        )
    )
    _quarter_notes_repair_result = repair_quarter_notes_ui_after_render(
        QuarterNotesUiRenderRepairDeps(
            ws=ws,
            quarters=quarters,
            is_pbi_profile=is_pbi_profile,
            is_gpre_profile=is_gpre_profile,
            is_anf_profile=is_anf_profile,
            ui_state=ui_state,
            render_blocks_start=render_blocks_start,
            get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
            quarter_label_short=_quarter_label_short,
            normalize_text=glx_normalize_text,
            ensure_terminal_period=_ensure_terminal_period,
            anf_visible_quarter_note_summaries=_anf_visible_quarter_note_summaries,
            anf_clean_visible_ui_text=_anf_clean_visible_ui_text,
            anf_polish_quarter_note_visible_fields=_anf_polish_quarter_note_visible_fields,
            record_writer_elapsed=_record_writer_elapsed,
            perf_counter=time.perf_counter,
        )
    )
    return qa_rows

