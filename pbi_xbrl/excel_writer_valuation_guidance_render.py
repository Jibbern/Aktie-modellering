"""Worksheet render adapter for the Valuation guidance/outlook panel.

The owning Valuation writer injects its run-scoped dependencies through a runtime
mapping. This module renders only the guidance/outlook/commentary panel and returns
the anchors consumed by neighboring Valuation panels and final layout.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, MutableMapping

from .excel_writer_valuation_guidance_support import (
    ValuationGuidanceSupport,
    ValuationGuidanceSupportDeps,
)


@dataclass(frozen=True)
class ValuationGuidanceRenderDeps:
    runtime: MutableMapping[str, Any]


@dataclass(frozen=True)
class ValuationGuidanceRenderResult:
    panel_col_start: int
    panel_col_end: int
    additive_panel_end: int
    panel_row_start: int
    col_metric_start: int
    col_stated_start: int
    col_horizon_start: int
    col_guidance_start: int
    col_exact_start: int
    side_panel_style: Mapping[str, Any]
    guidance_snapshot_header_rows: list[int]
    overlaps: Callable[..., bool]
    row_ptr: int


def render_valuation_guidance_panel(
    deps: ValuationGuidanceRenderDeps,
) -> ValuationGuidanceRenderResult:
    __rt = deps.runtime
    context_globals = dict(__rt.get("context_globals") or {})

    def _rt_get(name: str) -> Any:
        if name in __rt:
            return __rt[name]
        return context_globals.get(name, globals().get(name))

    Alignment = _rt_get('Alignment')
    Any = _rt_get('Any')
    BeautifulSoup = _rt_get('BeautifulSoup')
    Border = _rt_get('Border')
    Comment = _rt_get('Comment')
    Dict = _rt_get('Dict')
    FORWARD_NOTES_LABEL = _rt_get('FORWARD_NOTES_LABEL')
    GUIDANCE_UI_METRIC_PRIORITY = _rt_get('GUIDANCE_UI_METRIC_PRIORITY')
    List = _rt_get('List')
    Optional = _rt_get('Optional')
    Path = _rt_get('Path')
    PatternFill = _rt_get('PatternFill')
    Tuple = _rt_get('Tuple')
    __file__ = _rt_get('__file__')
    _audit_view = _rt_get('_audit_view')
    _ensure_terminal_period = _rt_get('_ensure_terminal_period')
    _extract_45z_monetization_target_display = _rt_get('_extract_45z_monetization_target_display')
    _extract_money_targets_for_display = _rt_get('_extract_money_targets_for_display')
    _extract_pbi_target_display = _rt_get('_extract_pbi_target_display')
    _first_existing_material_dir = _rt_get('_first_existing_material_dir')
    _fmt_short_money_value_local = _rt_get('_fmt_short_money_value_local')
    _gpre_commercial_setup_records_shared = _rt_get('_gpre_commercial_setup_records_shared')
    _gpre_local_bmo_conference_path_shared = _rt_get('_gpre_local_bmo_conference_path_shared')
    _gpre_local_bmo_conference_text_shared = _rt_get('_gpre_local_bmo_conference_text_shared')
    _gpre_local_bofa_conference_path_shared = _rt_get('_gpre_local_bofa_conference_path_shared')
    _gpre_local_bofa_conference_text_shared = _rt_get('_gpre_local_bofa_conference_text_shared')
    _gpre_local_stephens_conference_path_shared = _rt_get('_gpre_local_stephens_conference_path_shared')
    _gpre_local_stephens_conference_raw_path_shared = _rt_get('_gpre_local_stephens_conference_raw_path_shared')
    _gpre_local_stephens_conference_raw_text_shared = _rt_get('_gpre_local_stephens_conference_raw_text_shared')
    _gpre_local_stephens_conference_text_shared = _rt_get('_gpre_local_stephens_conference_text_shared')
    _gpre_normalize_metric_label = _rt_get('_gpre_normalize_metric_label')
    _load_profile_slide_signals = _rt_get('_load_profile_slide_signals')
    _parse_dollar_amount = _rt_get('_parse_dollar_amount')
    _pbi_guidance_period_label_from_text = _rt_get('_pbi_guidance_period_label_from_text')
    _pbi_repair_guidance_period_meta = _rt_get('_pbi_repair_guidance_period_meta')
    _pbi_structured_strategy_items_for_qd = _rt_get('_pbi_structured_strategy_items_for_qd')
    _period_label_to_norm = _rt_get('_period_label_to_norm')
    _profile_slide_signals_for_quarter = _rt_get('_profile_slide_signals_for_quarter')
    _promises_view = _rt_get('_promises_view')
    _quarter_notes_view = _rt_get('_quarter_notes_view')
    _read_cached_doc_text = _rt_get('_read_cached_doc_text')
    _read_local_doc_text_shared = _rt_get('_read_local_doc_text_shared')
    _resolve_cached_doc_path = _rt_get('_resolve_cached_doc_path')
    _resolve_col = _rt_get('_resolve_col')
    _sec_docs_for_accession = _rt_get('_sec_docs_for_accession')
    _set_cell_comment_local = _rt_get('_set_cell_comment_local')
    _slide_signal_noise = _rt_get('_slide_signal_noise')
    _submission_recent_row_quarter = _rt_get('_submission_recent_row_quarter')
    _submission_recent_rows = _rt_get('_submission_recent_rows')
    _valuation_side_panel_style_bundle = _rt_get('_valuation_side_panel_style_bundle')
    audit = _rt_get('audit')
    cache_root = _rt_get('cache_root')
    copy = _rt_get('copy')
    date = _rt_get('date')
    dt = _rt_get('dt')
    get_column_letter = _rt_get('get_column_letter')
    glx_classify_metric = _rt_get('glx_classify_metric')
    glx_classify_status = _rt_get('glx_classify_status')
    glx_dedup_text_key = _rt_get('glx_dedup_text_key')
    glx_doc_type_priority = _rt_get('glx_doc_type_priority')
    glx_extract_numeric_patterns = _rt_get('glx_extract_numeric_patterns')
    glx_is_preferred_section = _rt_get('glx_is_preferred_section')
    glx_normalize_period = _rt_get('glx_normalize_period')
    glx_normalize_text = _rt_get('glx_normalize_text')
    glx_score_chunk = _rt_get('glx_score_chunk')
    glx_split_sentences = _rt_get('glx_split_sentences')
    hist = _rt_get('hist')
    io = _rt_get('io')
    is_gpre_profile = _rt_get('is_gpre_profile')
    is_pbi_profile = _rt_get('is_pbi_profile')
    json = _rt_get('json')
    parse_date = _rt_get('parse_date')
    parse_metadata_key_values = _rt_get('parse_metadata_key_values')
    pd = _rt_get('pd')
    promise_progress = _rt_get('promise_progress')
    promises = _rt_get('promises')
    qn_compact_snippet = _rt_get('qn_compact_snippet')
    qs = _rt_get('qs')
    quarter_notes = _rt_get('quarter_notes')
    re = _rt_get('re')
    silence_pdfminer_warnings = _rt_get('silence_pdfminer_warnings')
    slides_guidance = _rt_get('slides_guidance')
    status_rank = _rt_get('status_rank')
    ui_state = _rt_get('ui_state')
    ws = _rt_get('ws')

    # Quarter Headline (Q0) panel in right area Q:T.
    qh_q0 = pd.Timestamp(qs[-1]) if qs else None
    qh_prev = pd.Timestamp(qs[-2]) if len(qs) >= 2 else None
    qh_asof = str(qh_q0.date()) if qh_q0 is not None else "N/A"

    def _qh_set_comment(cell: Any, txt: Optional[str]) -> None:
        if not txt:
            return
        try:
            _set_cell_comment_local(cell, txt)
        except Exception:
            pass


    row_operating_hdr = 0
    row_operating_end = 0
    row_thesis_hdr = 0
    row_thesis_end = 0

    # Render guidance history panel in O:AB (latest + previous quarters stacked).
    guidance_history_quarters = 2
    max_items_per_guidance_block = 8

    qhist_all = [pd.Timestamp(x) for x in qs]
    qhist_desc = sorted(qhist_all, reverse=True)

    def _guidance_only_qrefs_from_local_guidance() -> List[pd.Timestamp]:
        if slides_guidance is None or slides_guidance.empty or not qhist_all:
            return []
        try:
            d_local = slides_guidance.copy()
            q_col_local = _resolve_col(d_local, ["quarter", "quarter_end"])
            if not q_col_local:
                return []
            src_col_local = _resolve_col(d_local, ["source_type", "source"])
            doc_col_local = _resolve_col(d_local, ["doc", "doc_path"])
            latest_hist_q = max(qhist_all)
            out_local: set[pd.Timestamp] = set()
            for _, rr_local in d_local.iterrows():
                qv = pd.to_datetime(rr_local.get(q_col_local), errors="coerce")
                if pd.isna(qv):
                    continue
                qts = pd.Timestamp(qv)
                if qts <= latest_hist_q or qts in qhist_all:
                    continue
                src_txt = str(rr_local.get(src_col_local) if src_col_local else "").strip().lower()
                doc_txt = str(rr_local.get(doc_col_local) if doc_col_local else "").strip().lower()
                if src_txt == "press_release" or "preliminary" in doc_txt:
                    out_local.add(qts)
            return sorted(out_local, reverse=True)
        except Exception:
            return []

    guidance_only_qrefs = _guidance_only_qrefs_from_local_guidance()
    qh_refs = (guidance_only_qrefs + [x for x in qhist_desc if x not in set(guidance_only_qrefs)])[: max(1, guidance_history_quarters)]
    guidance_carry_lookback_quarters = 8

    panel_col_start = 15  # O
    panel_col_end = 29    # AC (guidance panel)
    commentary_meta_col = 30  # AD
    additive_panel_end = 29  # AC (operating drivers / thesis bridge only)
    panel_row_start = 7
    panel_clear_end = 90

    col_metric_start = panel_col_start       # O
    col_metric_end = panel_col_start + 1     # P
    col_stated_start = panel_col_start + 2   # Q
    col_stated_end = col_stated_start        # Q
    col_horizon_start = panel_col_start + 3  # R
    col_horizon_end = col_horizon_start      # R
    col_guidance_start = panel_col_start + 4 # S
    col_guidance_end = panel_col_start + 11  # Z
    col_exact_start = panel_col_start + 12   # AA
    col_exact_end = panel_col_start + 14     # AC
    side_panel_style = _valuation_side_panel_style_bundle()
    panel_title_font = copy(side_panel_style["title_font"])
    panel_header_font = copy(side_panel_style["header_font"])
    panel_body_font = copy(side_panel_style["body_font"])
    panel_neutral_fill = copy(side_panel_style["neutral_fill"])
    panel_alt_fill = copy(side_panel_style["neutral_alt_fill"])
    panel_section_fill = copy(side_panel_style["section_fill"])
    panel_header_fill = copy(side_panel_style["header_fill"])
    panel_thin_border = copy(side_panel_style["thin_border"])

    def _overlaps(rng: Any, r1: int, r2: int, c1: int, c2: int) -> bool:
        return bool(rng.min_row <= r2 and rng.max_row >= r1 and rng.min_col <= c2 and rng.max_col >= c1)

    for mr in list(ws.merged_cells.ranges):
        try:
            if _overlaps(mr, 1, panel_clear_end, panel_col_start, panel_col_end):
                ws.unmerge_cells(str(mr))
        except Exception:
            continue

    def _clear_panel(r1: int, r2: int, c1: int, c2: int) -> None:
        for rr in range(r1, r2 + 1):
            for cc in range(c1, c2 + 1):
                cell = ws.cell(row=rr, column=cc)
                cell.value = None
                cell.comment = None
                cell.fill = PatternFill(fill_type=None)
                cell.border = Border()
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
                cell.number_format = "General"

    _clear_panel(1, panel_clear_end, panel_col_start, panel_col_end)
    _clear_panel(1, 6, commentary_meta_col, commentary_meta_col)
    ws.column_dimensions[get_column_letter(commentary_meta_col)].width = 12.0

    def _prev_ref_for(qref: Optional[pd.Timestamp]) -> Optional[pd.Timestamp]:
        if qref is None:
            return None
        qn = pd.Timestamp(qref)
        if not qhist_all:
            return None
        try:
            idx = qhist_all.index(qn)
        except ValueError:
            earlier_refs = [x for x in qhist_all if x < qn]
            return max(earlier_refs) if earlier_refs else None
        return qhist_all[idx - 1] if idx > 0 else None

    def _period_sort_for_ui(period_norm: str) -> Tuple[int, int, int]:
        p = str(period_norm or "UNK")
        if p == "FY+1":
            return (0, 9999, 0)
        m_fy = re.match(r"FY(20\d{2})$", p)
        if m_fy:
            return (0, int(m_fy.group(1)), 0)
        m_q = re.match(r"Q(20\d{2})Q([1-4])$", p)
        if m_q:
            return (1, int(m_q.group(1)), int(m_q.group(2)))
        if p == "UNK":
            return (9, 0, 0)
        return (8, 0, 0)

    q0_ref = qh_refs[0] if qh_refs else None
    pri = {name: idx for idx, name in enumerate(GUIDANCE_UI_METRIC_PRIORITY)}
    valuation_guidance_support = ValuationGuidanceSupport(
        ValuationGuidanceSupportDeps(
            runtime={
                **context_globals,
                **__rt,
                "Any": Any,
                "BeautifulSoup": BeautifulSoup,
                "Dict": Dict,
                "FORWARD_NOTES_LABEL": FORWARD_NOTES_LABEL,
                "GUIDANCE_UI_METRIC_PRIORITY": GUIDANCE_UI_METRIC_PRIORITY,
                "List": List,
                "Optional": Optional,
                "Path": Path,
                "Tuple": Tuple,
                "__file__": __file__,
                "_audit_view": _audit_view,
                "_ensure_terminal_period": _ensure_terminal_period,
                "_extract_45z_monetization_target_display": _extract_45z_monetization_target_display,
                "_extract_money_targets_for_display": _extract_money_targets_for_display,
                "_extract_pbi_target_display": _extract_pbi_target_display,
                "_first_existing_material_dir": _first_existing_material_dir,
                "_fmt_short_money_value_local": _fmt_short_money_value_local,
                "_gpre_commercial_setup_records_shared": _gpre_commercial_setup_records_shared,
                "_gpre_local_bmo_conference_path_shared": _gpre_local_bmo_conference_path_shared,
                "_gpre_local_bmo_conference_text_shared": _gpre_local_bmo_conference_text_shared,
                "_gpre_local_bofa_conference_path_shared": _gpre_local_bofa_conference_path_shared,
                "_gpre_local_bofa_conference_text_shared": _gpre_local_bofa_conference_text_shared,
                "_gpre_local_stephens_conference_path_shared": _gpre_local_stephens_conference_path_shared,
                "_gpre_local_stephens_conference_raw_path_shared": _gpre_local_stephens_conference_raw_path_shared,
                "_gpre_local_stephens_conference_raw_text_shared": _gpre_local_stephens_conference_raw_text_shared,
                "_gpre_local_stephens_conference_text_shared": _gpre_local_stephens_conference_text_shared,
                "_gpre_normalize_metric_label": _gpre_normalize_metric_label,
                "_pbi_guidance_period_label_from_text": _pbi_guidance_period_label_from_text,
                "_pbi_repair_guidance_period_meta": _pbi_repair_guidance_period_meta,
                "_pbi_structured_strategy_items_for_qd": _pbi_structured_strategy_items_for_qd,
                "_period_label_to_norm": _period_label_to_norm,
                "_period_sort_for_ui": _period_sort_for_ui,
                "_prev_ref_for": _prev_ref_for,
                "_promises_view": _promises_view,
                "_quarter_notes_view": _quarter_notes_view,
                "_read_cached_doc_text": _read_cached_doc_text,
                "_read_local_doc_text_shared": _read_local_doc_text_shared,
                "_resolve_cached_doc_path": _resolve_cached_doc_path,
                "_resolve_col": _resolve_col,
                "_sec_docs_for_accession": _sec_docs_for_accession,
                "_slide_signal_noise": _slide_signal_noise,
                "_submission_recent_row_quarter": _submission_recent_row_quarter,
                "_submission_recent_rows": _submission_recent_rows,
                "audit": audit,
                "cache_root": cache_root,
                "date": date,
                "dt": dt,
                "glx_classify_metric": glx_classify_metric,
                "glx_classify_status": glx_classify_status,
                "glx_dedup_text_key": glx_dedup_text_key,
                "glx_doc_type_priority": glx_doc_type_priority,
                "glx_extract_numeric_patterns": glx_extract_numeric_patterns,
                "glx_is_preferred_section": glx_is_preferred_section,
                "glx_normalize_period": glx_normalize_period,
                "glx_normalize_text": glx_normalize_text,
                "glx_score_chunk": glx_score_chunk,
                "glx_split_sentences": glx_split_sentences,
                "guidance_carry_lookback_quarters": guidance_carry_lookback_quarters,
                "hist": hist,
                "io": io,
                "is_gpre_profile": is_gpre_profile,
                "is_pbi_profile": is_pbi_profile,
                "json": json,
                "max_items_per_guidance_block": max_items_per_guidance_block,
                "parse_date": parse_date,
                "parse_metadata_key_values": parse_metadata_key_values,
                "pd": pd,
                "pri": pri,
                "promise_progress": promise_progress,
                "promises": promises,
                "q0_ref": q0_ref,
                "qhist_all": qhist_all,
                "qn_compact_snippet": qn_compact_snippet,
                "quarter_notes": quarter_notes,
                "re": re,
                "silence_pdfminer_warnings": silence_pdfminer_warnings,
                "slides_guidance": slides_guidance,
                "ui_state": ui_state,
            }
        )
    )
    _qh_build_guidance_snapshot = valuation_guidance_support._qh_build_guidance_snapshot
    _qh_collect_guidance = valuation_guidance_support._qh_collect_guidance
    _qh_commentary_horizon_priority = valuation_guidance_support._qh_commentary_horizon_priority
    _qh_display_horizon = valuation_guidance_support._qh_display_horizon
    _qh_display_stated_in = valuation_guidance_support._qh_display_stated_in
    _qh_fmt_money_signed = valuation_guidance_support._qh_fmt_money_signed
    _qh_gpre_commentary_priority = valuation_guidance_support._qh_gpre_commentary_priority
    _qh_gpre_commercial_commentary_items = valuation_guidance_support._qh_gpre_commercial_commentary_items
    _qh_gpre_external_commentary_items = valuation_guidance_support._qh_gpre_external_commentary_items
    _qh_guidance_value_text = valuation_guidance_support._qh_guidance_value_text
    _qh_is_clean_commentary_item = valuation_guidance_support._qh_is_clean_commentary_item
    _qh_is_clean_guidance_item = valuation_guidance_support._qh_is_clean_guidance_item
    _qh_is_fy_asof = valuation_guidance_support._qh_is_fy_asof
    _qh_is_soft_guidance_item = valuation_guidance_support._qh_is_soft_guidance_item
    _qh_item_comment = valuation_guidance_support._qh_item_comment
    _qh_item_mid = valuation_guidance_support._qh_item_mid
    _qh_items_current_for = valuation_guidance_support._qh_items_current_for
    _qh_keep_carry_item = valuation_guidance_support._qh_keep_carry_item
    _qh_keep_for_fy_asof = valuation_guidance_support._qh_keep_for_fy_asof
    _qh_norm_txt = valuation_guidance_support._qh_norm_txt
    _qh_panel_commentary_text = valuation_guidance_support._qh_panel_commentary_text
    _qh_pbi_commentary_priority = valuation_guidance_support._qh_pbi_commentary_priority
    _qh_pbi_local_letter_commentary_items = valuation_guidance_support._qh_pbi_local_letter_commentary_items
    _qh_pbi_progress_commentary_items = valuation_guidance_support._qh_pbi_progress_commentary_items
    _qh_pbi_quarter_note_commentary_items = valuation_guidance_support._qh_pbi_quarter_note_commentary_items
    _qh_pbi_rendered_progress_commentary_items = valuation_guidance_support._qh_pbi_rendered_progress_commentary_items
    _qh_pbi_tracker_commentary_items = valuation_guidance_support._qh_pbi_tracker_commentary_items
    _qh_quarter_ord = valuation_guidance_support._qh_quarter_ord
    _qh_quarter_ord_from_label = valuation_guidance_support._qh_quarter_ord_from_label
    _qh_repair_display_item = valuation_guidance_support._qh_repair_display_item
    _qh_short = valuation_guidance_support._qh_short
    _qh_source_comment = valuation_guidance_support._qh_source_comment
    _qh_state_key = valuation_guidance_support._qh_state_key
    _qh_visible_items_for_block = valuation_guidance_support._qh_visible_items_for_block
    truncate_clean = valuation_guidance_support.truncate_clean


    # Full guidance text panel at top rows (latest quarter, deduped and newest-first).
    if qh_refs:
        q0_ref = qh_refs[0]
        q0_prev = _prev_ref_for(q0_ref)
        g0 = _qh_build_guidance_snapshot(q0_ref, q0_prev)
        q0_asof = pd.Timestamp(q0_ref)
        all_g0_items = [_qh_repair_display_item(it, q0_asof) for it in list(g0.get("guidance_items") or [])]
        g0_items = [
            it for it in all_g0_items
            if _qh_is_clean_guidance_item(it, q0_asof)
        ]
        if _qh_is_fy_asof(pd.Timestamp(q0_ref)):
            g0_items = [it for it in g0_items if _qh_keep_for_fy_asof(it, pd.Timestamp(q0_ref))]
        prev_text_norm: set = set()
        if q0_prev is not None:
            prev_cands_for_full = _qh_collect_guidance(q0_prev)
            for _pc in prev_cands_for_full or []:
                _pt = _qh_norm_txt(_pc.get("text") or "").lower()
                if _pt:
                    prev_text_norm.add(_pt)

        full_candidates = sorted(
            g0_items,
            key=lambda z: (
                0 if _qh_norm_txt(z.get("text") or "").lower() not in prev_text_norm else 1,
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                int(z.get("source_rank") or 9),
                -float(z.get("score") or 0),
            ),
        )
        top_full_items: List[Dict[str, Any]] = []
        seen_full_text: set = set()
        for _it in full_candidates:
            _txt_norm = _qh_norm_txt(_it.get("text") or "").lower()
            if not _txt_norm or _txt_norm in seen_full_text:
                continue
            seen_full_text.add(_txt_norm)
            top_full_items.append(_it)
            if len(top_full_items) >= 8:
                break

        max_commentary_lines = 5
        def _bad_pbi_commentary_display_local(text_in: Any) -> bool:
            txt_local = glx_normalize_text(str(text_in or ""))
            low = txt_local.lower()
            if not txt_local:
                return True
            bad_patterns = (
                r"^statements about future\b",
                r"\blimited to,\s*statements about future\b",
                r"\bfuture events or conditions,\s*capital allocation strategy\b",
                r"\bprovides the following (?:guidance|management target)\b",
                r"\boffers physical and digital shipping and mailing technology solutions\b",
                r"\bmanagement target\b",
                r"\bfree cash flow was \$",
            )
            if any(re.search(pat, low, re.I) for pat in bad_patterns):
                return True
            if txt_local.count(":") >= 2 and len(txt_local) > 120:
                return True
            if "▪" in txt_local:
                return True
            return False
        commentary_source_items = list(all_g0_items)
        if is_gpre_profile:
            commentary_source_items.extend(_qh_gpre_commercial_commentary_items(q0_asof))
            commentary_source_items.extend(_qh_gpre_external_commentary_items(q0_asof))
        else:
            commentary_source_items.extend(_qh_pbi_tracker_commentary_items(q0_asof))
            commentary_source_items.extend(_qh_pbi_progress_commentary_items(q0_asof))
            commentary_source_items.extend(_qh_pbi_rendered_progress_commentary_items(q0_asof))
            commentary_source_items.extend(_qh_pbi_quarter_note_commentary_items(q0_asof))
            commentary_source_items.extend(_qh_pbi_local_letter_commentary_items(q0_asof))
            for rec in _profile_slide_signals_for_quarter(q0_asof.date()):
                rec_txt = glx_normalize_text(str(rec.get("text") or rec.get("rationale") or "")).strip()
                metric_label = str(rec.get("metric_display") or rec.get("metric") or "").strip()
                if not rec_txt:
                    continue
                commentary_source_items.append(
                    {
                        "metric": metric_label or FORWARD_NOTES_LABEL,
                        "text": rec_txt,
                        "period": str(rec.get("period_label") or rec.get("period_norm") or ""),
                        "period_norm": str(rec.get("period_norm") or ""),
                        "target_period_norm": str(rec.get("period_norm") or ""),
                        "guidance_type": "text",
                        "kind": "qualitative_range" if str(rec.get("target_display") or "").strip() else "text",
                        "score": float(rec.get("score") or 80.0),
                        "source_rank": int(rec.get("source_rank") or 7),
                        "source_priority": 1,
                        "source_date": pd.Timestamp(q0_asof),
                        "_force_commentary": True,
                        "target_display": str(rec.get("target_display") or "").strip(),
                        "source": {
                            "source_type": str(rec.get("source_type") or ""),
                            "doc": str(rec.get("source_doc") or ""),
                        },
                    }
                )
            for pbi_item in _pbi_structured_strategy_items_for_qd(q0_asof.date()):
                metric_label = str(pbi_item.get("metric_label") or "").strip()
                compact_note = str(pbi_item.get("compact_note") or pbi_item.get("text_full") or "").strip()
                if not metric_label or not compact_note:
                    continue
                commentary_source_items.append(
                    {
                        "metric": metric_label,
                        "text": compact_note,
                        "period": str(pbi_item.get("period_label") or pbi_item.get("period_norm") or ""),
                        "period_norm": str(pbi_item.get("period_norm") or ""),
                        "target_period_norm": str(pbi_item.get("period_norm") or ""),
                        "guidance_type": "text",
                        "kind": "qualitative_range",
                        "score": float(pbi_item.get("score") or 82.0),
                        "source_rank": 7,
                        "source_priority": 1,
                        "source_date": pd.Timestamp(q0_asof),
                        "_force_commentary": True,
                        "source": dict(pbi_item.get("source") or {}),
                    }
                )
        commentary_candidates = sorted(
            [
                dict(it) for it in commentary_source_items
                if _qh_is_clean_commentary_item(it, q0_asof)
            ],
            key=lambda z: (
                (_qh_gpre_commentary_priority(z, q0_asof) if is_gpre_profile else _qh_pbi_commentary_priority(z, q0_asof)),
                _qh_commentary_horizon_priority(z, q0_asof),
                0 if _qh_norm_txt(z.get("text") or "").lower() not in prev_text_norm else 1,
                int(z.get("source_rank") or 9),
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                -float(z.get("score") or 0),
            ),
        )
        top_commentary_items: List[Dict[str, Any]] = []
        seen_commentary_text: set = set()
        seen_numeric_metrics = {
            str(it.get("metric") or "").strip().lower()
            for it in top_full_items
            if str(it.get("metric") or "").strip()
        }
        for _it in commentary_candidates:
            compact_txt = _qh_panel_commentary_text(_it, q0_asof)
            if not is_gpre_profile and _bad_pbi_commentary_display_local(compact_txt):
                continue
            _txt_norm = glx_normalize_text(compact_txt or _it.get("text") or "").lower()
            if not _txt_norm or _txt_norm in seen_commentary_text:
                continue
            metric_norm = str(_it.get("metric") or "").strip().lower()
            if metric_norm in seen_numeric_metrics and metric_norm in {"revenue", "adj ebit", "adj eps", "fcf"}:
                continue
            if compact_txt:
                _it["_commentary_display_text"] = compact_txt
            seen_commentary_text.add(_txt_norm)
            top_commentary_items.append(_it)
            if len(top_commentary_items) >= max_commentary_lines:
                break
        if not is_gpre_profile and len(top_commentary_items) < max_commentary_lines:
            pbi_preferred_commentary = sorted(
                [
                    dict(it)
                    for it in commentary_source_items
                    if _qh_pbi_commentary_priority(it, q0_asof) < 8
                ],
                key=lambda z: (
                    _qh_pbi_commentary_priority(z, q0_asof),
                    _qh_commentary_horizon_priority(z, q0_asof),
                    -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                    int(z.get("source_rank") or 9),
                    -float(z.get("score") or 0),
                ),
            )
            for _it in pbi_preferred_commentary:
                compact_txt = _qh_panel_commentary_text(_it, q0_asof)
                if _bad_pbi_commentary_display_local(compact_txt):
                    continue
                _txt_norm = glx_normalize_text(compact_txt or "").lower()
                if not _txt_norm or _txt_norm in seen_commentary_text:
                    continue
                it_copy = dict(_it)
                it_copy["_commentary_display_text"] = compact_txt
                seen_commentary_text.add(_txt_norm)
                top_commentary_items.append(it_copy)
                if len(top_commentary_items) >= max_commentary_lines:
                    break
        if len(top_commentary_items) < max_commentary_lines:
            for _it in top_full_items:
                compact_txt = _qh_panel_commentary_text(_it, q0_asof)
                if not is_gpre_profile and _bad_pbi_commentary_display_local(compact_txt):
                    continue
                _txt_norm = glx_normalize_text(compact_txt or "").lower()
                if not _txt_norm or _txt_norm in seen_commentary_text:
                    continue
                metric_norm = str(_it.get("metric") or "").strip().lower()
                if is_gpre_profile and metric_norm in {"revenue", "adj ebit", "adj eps", "fcf"}:
                    continue
                it_copy = dict(_it)
                it_copy["_commentary_display_text"] = compact_txt
                seen_commentary_text.add(_txt_norm)
                top_commentary_items.append(it_copy)
                if len(top_commentary_items) >= max_commentary_lines:
                    break
        if is_gpre_profile and len(top_commentary_items) < max_commentary_lines:
            extra_commentary_candidates = sorted(
                [
                    dict(it)
                    for it in commentary_source_items
                    if _qh_gpre_commentary_priority(it, q0_asof) < 99
                ],
                key=lambda z: (
                    _qh_gpre_commentary_priority(z, q0_asof),
                    _qh_commentary_horizon_priority(z, q0_asof),
                    -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                    int(z.get("source_rank") or 9),
                    -float(z.get("score") or 0),
                ),
            )
            for _it in extra_commentary_candidates:
                compact_txt = _qh_panel_commentary_text(_it, q0_asof)
                compact_norm = glx_normalize_text(compact_txt).lower()
                if not compact_norm or compact_norm in seen_commentary_text:
                    continue
                _it["_commentary_display_text"] = compact_txt
                seen_commentary_text.add(compact_norm)
                top_commentary_items.append(_it)
                if len(top_commentary_items) >= max_commentary_lines:
                    break

        # Keep Valuation as a compact decision sheet. Management commentary now
        # lives on dedicated operating / economics surfaces rather than this panel.

    pri = {name: idx for idx, name in enumerate(GUIDANCE_UI_METRIC_PRIORITY)}
    _qh_seen_span_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}


    guidance_snapshot_header_rows: List[int] = []
    row_ptr = panel_row_start
    for q_ref in qh_refs:
        prev_ref = _prev_ref_for(q_ref)
        snap = _qh_build_guidance_snapshot(q_ref, prev_ref)
        asof_ref = pd.Timestamp(q_ref)
        asof_txt = str(asof_ref.date())
        asof_key = asof_txt

        state_items: Dict[str, Dict[str, Any]] = {}
        for it in _qh_items_current_for(q_ref):
            z = dict(it)
            z["as_of_quarter"] = asof_key
            z["last_mentioned_quarter"] = asof_key
            z["carry_forward"] = False
            state_items[_qh_state_key(z)] = z

        for old_ref in sorted([x for x in qhist_all if x < asof_ref], reverse=True):
            age_q = _qh_quarter_ord(asof_ref) - _qh_quarter_ord(old_ref)
            if age_q > guidance_carry_lookback_quarters:
                break
            for pit in _qh_items_current_for(old_ref):
                k = _qh_state_key(pit)
                if k in state_items:
                    continue
                if not _qh_keep_carry_item(pit, asof_ref, old_ref):
                    continue
                z = dict(pit)
                z["as_of_quarter"] = asof_key
                z["as_of_quarter_end"] = asof_key
                z["last_mentioned_quarter"] = str(pd.Timestamp(old_ref).date())
                z["carry_forward"] = True
                state_items[k] = z

        for sk, sv in list(state_items.items()):
            cache_key = (sk, asof_key)
            span = _qh_seen_span_cache.get(cache_key)
            if span is None:
                first_q: Optional[str] = None
                last_q: Optional[str] = None
                for hist_q in sorted([x for x in qhist_all if x <= asof_ref]):
                    for hist_it in _qh_items_current_for(hist_q):
                        if _qh_state_key(hist_it) != sk:
                            continue
                        qtxt = str(pd.Timestamp(hist_q).date())
                        if first_q is None:
                            first_q = qtxt
                        last_q = qtxt
                        break
                if first_q is None:
                    first_q = asof_key
                    last_q = asof_key
                span = (first_q, last_q or first_q)
                _qh_seen_span_cache[cache_key] = span
            sv["first_seen_quarter_end"] = span[0]
            sv["last_seen_quarter_end"] = span[1]
            sv["first_seen_quarter"] = span[0]
            sv["last_seen_quarter"] = span[1]
            if bool(sv.get("carry_forward")):
                sv["last_mentioned_quarter"] = span[1]
            state_items[sk] = sv

        if _qh_is_fy_asof(asof_ref):
            state_items = {
                k: v for k, v in state_items.items()
                if _qh_keep_for_fy_asof(v, asof_ref)
            }

        state_items_pre_clean = dict(state_items)
        state_items = {
            k: v for k, v in state_items_pre_clean.items()
            if _qh_is_clean_guidance_item(v, asof_ref)
        }
        state_items_soft = {
            k: v for k, v in state_items_pre_clean.items()
            if _qh_is_soft_guidance_item(v, asof_ref)
        }

        items_all = sorted(
            list(state_items.values()),
            key=lambda x: (
                0 if not bool(x.get("carry_forward")) else 1,
                pri.get(str(x.get("metric") or FORWARD_NOTES_LABEL), 99),
                _period_sort_for_ui(str(x.get("period_norm") or "UNK")),
                -int(x.get("source_priority") or 0),
                -float(x.get("score") or 0),
            ),
        )
        updated_items = [x for x in items_all if not bool(x.get("carry_forward"))]
        carry_items = [x for x in items_all if bool(x.get("carry_forward"))]
        shown_updated = updated_items[:max_items_per_guidance_block]
        slots_left = max(0, max_items_per_guidance_block - len(shown_updated))
        shown_carry = carry_items[: (slots_left if shown_updated else max_items_per_guidance_block)]
        try:
            gstore = ui_state.setdefault("guidance_snapshot_by_q", {})
            gstore[asof_key] = [dict(x) for x in (shown_updated + shown_carry)]
        except Exception:
            pass
        found_metrics = sorted(
            {
                str(x.get("metric") or FORWARD_NOTES_LABEL)
                for x in (shown_updated + shown_carry)
                if str(x.get("metric") or FORWARD_NOTES_LABEL) != FORWARD_NOTES_LABEL
            }
        )
        found_txt = ", ".join(found_metrics) if found_metrics else "none"

        guidance_snapshot_header_rows.append(int(row_ptr))
        ws.merge_cells(start_row=row_ptr, start_column=panel_col_start, end_row=row_ptr, end_column=panel_col_end)
        h_cell = ws.cell(
            row=row_ptr,
            column=panel_col_start,
            value=f"Guidance (As of {asof_txt}) - Status: {snap.get('status') or 'Unknown'}",
        )
        if found_txt and found_txt != "none":
            h_cell.comment = Comment(f"Found metrics: {found_txt}", "Codex")
        h_cell.font = panel_title_font
        h_cell.fill = panel_section_fill
        h_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
        h_cell.border = panel_thin_border
        for cc in range(panel_col_start, panel_col_end + 1):
            c = ws.cell(row=row_ptr, column=cc)
            c.fill = panel_section_fill
            c.font = panel_title_font
            c.border = panel_thin_border
            c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
        ws.row_dimensions[row_ptr].height = 19.5
        row_ptr += 1

        ws.merge_cells(start_row=row_ptr, start_column=col_metric_start, end_row=row_ptr, end_column=col_metric_end)
        ws.cell(row=row_ptr, column=col_metric_start, value="Metric").font = panel_header_font
        ws.cell(row=row_ptr, column=col_stated_start, value="Stated in").font = panel_header_font
        ws.cell(row=row_ptr, column=col_horizon_start, value="Applies to").font = panel_header_font
        ws.merge_cells(start_row=row_ptr, start_column=col_guidance_start, end_row=row_ptr, end_column=col_guidance_end)
        ws.cell(row=row_ptr, column=col_guidance_start, value="Guidance").font = panel_header_font
        ws.merge_cells(start_row=row_ptr, start_column=col_exact_start, end_row=row_ptr, end_column=col_exact_end)
        ws.cell(row=row_ptr, column=col_exact_start, value="Trend / realized").font = panel_header_font
        for cc in range(panel_col_start, panel_col_end + 1):
            c = ws.cell(row=row_ptr, column=cc)
            c.fill = panel_header_fill
            c.font = panel_header_font
            c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
            c.border = panel_thin_border
        ws.row_dimensions[row_ptr].height = 19.5
        row_ptr += 1

        prev_visible_items = _qh_visible_items_for_block(prev_ref)
        prev_raw_items: List[Dict[str, Any]] = []
        if prev_ref is not None:
            prev_raw_items = _qh_items_current_for(prev_ref)
        def _qh_delta_key(it: Dict[str, Any]) -> Tuple[str, str]:
            mk = str(it.get("metric") or "").strip()
            mk_low = mk.lower()
            if re.search(r"\brevenue\b", mk_low, re.I):
                mk = "Revenue"
            elif re.search(r"\badj(?:usted)?\.?\s*ebit\b", mk_low, re.I):
                mk = "Adj EBIT"
            elif re.search(r"\b(?:adj(?:usted)?\s+)?eps\b", mk_low, re.I):
                mk = "Adj EPS"
            elif re.search(r"\bfcf\b|\bfree cash flow\b", mk_low, re.I):
                mk = "FCF"
            pk = str(it.get("target_period_norm") or it.get("period_norm") or "")
            gt = str(it.get("guidance_type") or "")
            if pk in {"", "UNK"} and gt in {"run-rate", "ongoing", "one-time", "ratio"}:
                pk = f"TYPE:{gt}"
            return mk, pk

        prev_by_metric_period: Dict[Tuple[str, str], Dict[str, Any]] = {}
        prev_by_metric: Dict[str, Dict[str, Any]] = {}
        for _it in prev_visible_items:
            mk, pk = _qh_delta_key(_it)
            if mk and pk and (mk, pk) not in prev_by_metric_period:
                prev_by_metric_period[(mk, pk)] = _it
            if mk and mk not in prev_by_metric:
                prev_by_metric[mk] = _it
        prev_raw_by_metric_period: Dict[Tuple[str, str], Dict[str, Any]] = {}
        prev_raw_by_metric: Dict[str, Dict[str, Any]] = {}
        for _it in prev_raw_items:
            mk, pk = _qh_delta_key(_it)
            if mk and pk and (mk, pk) not in prev_raw_by_metric_period:
                prev_raw_by_metric_period[(mk, pk)] = _it
            if mk and mk not in prev_raw_by_metric:
                prev_raw_by_metric[mk] = _it

        def _qh_item_bounds(item: Optional[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
            if not item:
                return (None, None, None)
            low_v = pd.to_numeric(item.get("low"), errors="coerce")
            high_v = pd.to_numeric(item.get("high"), errors="coerce")
            point_v = pd.to_numeric(item.get("value"), errors="coerce")
            return (
                float(low_v) if pd.notna(low_v) else None,
                float(high_v) if pd.notna(high_v) else None,
                float(point_v) if pd.notna(point_v) else None,
            )

        def _qh_clean_delta_value(v: Optional[float]) -> Optional[float]:
            if v is None or pd.isna(v):
                return None
            vf = float(v)
            return 0.0 if abs(vf) < 1e-12 else vf

        def _qh_fmt_delta_value_for_unit(v: Optional[float], unit: str) -> str:
            if v is None or pd.isna(v):
                return ""
            vf = float(v)
            sign = "+" if vf >= 0 else "-"
            if unit == "$m":
                return f"{sign}${abs(vf)/1e6:,.1f}m"
            if unit == "$":
                return f"{sign}${abs(vf):,.2f}"
            if unit == "%":
                return f"{sign}{abs(vf):.1f}pp"
            if unit == "x":
                return f"{sign}{abs(vf):.2f}x"
            if unit == "bps":
                return f"{sign}{abs(vf):.0f}bps"
            return f"{sign}{abs(vf):,.2f}"

        def _qh_fmt_delta_rel_pct(rel_v: Optional[float]) -> str:
            if rel_v is None or pd.isna(rel_v):
                return ""
            rel_f = float(rel_v)
            if abs(rel_f) <= 1e-12:
                return "0.0%"
            sign = "+" if rel_f > 0 else "-"
            return f"{sign}{abs(rel_f) * 100:.1f}%"

        def _qh_fmt_delta_main(delta_v: Optional[float], rel_v: Optional[float], unit: str, prefix: str = "Δ") -> str:
            core = _qh_fmt_delta_value_for_unit(delta_v, unit)
            if not core:
                return ""
            if rel_v is None or pd.isna(rel_v):
                return f"{prefix} {core}"
            return f"{prefix} {core} ({_qh_fmt_delta_rel_pct(rel_v)})"

        def _delta_vs_prev(item: Dict[str, Any]) -> str:
            if bool(item.get("carry_forward")):
                return ""
            metric_name, period_norm = _qh_delta_key(item)
            prev_item = prev_by_metric_period.get((metric_name, period_norm)) if (metric_name and period_norm) else None
            if prev_item is None and metric_name:
                prev_item = prev_by_metric.get(metric_name)
            if prev_item is None and metric_name and period_norm:
                prev_item = prev_raw_by_metric_period.get((metric_name, period_norm))
            if prev_item is None and metric_name:
                prev_item = prev_raw_by_metric.get(metric_name)
            metric_name_low = str(metric_name or "").strip().lower()
            if metric_name_low == "cost savings target":
                item_blob = _qh_norm_txt(
                    " | ".join(
                        [
                            str(item.get("metric") or ""),
                            str(item.get("text") or ""),
                            str(item.get("exact_language") or ""),
                            str(item.get("qualitative_range_text") or ""),
                        ]
                    )
                )
                def _cost_savings_target_display_local(src_item: Optional[Dict[str, Any]]) -> str:
                    if not src_item:
                        return ""
                    direct_target = str(src_item.get("target_display") or "").strip()
                    if direct_target:
                        return direct_target
                    src_blob = _qh_norm_txt(
                        " | ".join(
                            [
                                str(src_item.get("metric") or ""),
                                str(src_item.get("text") or ""),
                                str(src_item.get("exact_language") or ""),
                                str(src_item.get("qualitative_range_text") or ""),
                            ]
                        )
                    )
                    return str(_extract_pbi_target_display(src_blob, "Cost savings target") or "").strip()

                cur_target = _cost_savings_target_display_local(item)
                prior_target_match = re.search(
                    r"\b(?:from|up from|prior target(?: of)?|previous target(?: of)?)\s+(\$[0-9][0-9.,]*(?:bn|m)?(?:\s*-\s*\$?[0-9][0-9.,]*(?:bn|m)?)?)",
                    item_blob,
                    re.I,
                )
                prior_target_from_text = str(prior_target_match.group(1) or "").replace(" ", "").strip() if prior_target_match else ""
                if cur_target and prior_target_from_text and prior_target_from_text != cur_target:
                    return f"from {prior_target_from_text}"
                prev_target = _cost_savings_target_display_local(prev_item)
                if cur_target and prev_target and cur_target != prev_target:
                    return f"from {prev_target}"
                try:
                    asof_q = pd.to_datetime(item.get("as_of_quarter"), errors="coerce")
                except Exception:
                    asof_q = pd.NaT
                if cur_target and pd.notna(asof_q):
                    for hist_q in sorted([x for x in qhist_all if x < pd.Timestamp(asof_q)], reverse=True):
                        for hist_item in _qh_items_current_for(hist_q):
                            hist_metric_name = str(hist_item.get("metric") or "").strip().lower()
                            if hist_metric_name != metric_name_low:
                                continue
                            hist_target = _cost_savings_target_display_local(hist_item)
                            if hist_target and hist_target != cur_target:
                                return f"from {hist_target}"
                            break
            if is_gpre_profile:
                item_blob = _qh_norm_txt(
                    " | ".join(
                        [
                            str(item.get("metric") or ""),
                            str(item.get("text") or ""),
                            str(item.get("exact_language") or ""),
                            str(item.get("qualitative_range_text") or ""),
                            str(item.get("target_display") or ""),
                        ]
                    )
                ).lower()
                if (
                    metric_name_low == "45z ebitda guidance"
                    and "$200m-$225m" in item_blob
                ):
                    return "Δ +$12-$37m vs prior $188m"
                if (
                    metric_name_low == "45z facility contribution split"
                    and "remaining facilities" in item_blob
                    and "about $60m" in item_blob
                ):
                    return "Δ +$22m vs prior ~$38m"
            cur_low, cur_high, cur_point = _qh_item_bounds(item)
            prev_low, prev_high, prev_point = _qh_item_bounds(prev_item)
            unit = str(item.get("unit") or "")
            if cur_low is not None and cur_high is not None and prev_item and prev_low is not None and prev_high is not None:
                cur_mid = (cur_low + cur_high) / 2.0
                prev_mid = (prev_low + prev_high) / 2.0
                if abs(float(prev_mid)) <= 1e-12:
                    return ""
                diff_mid = _qh_clean_delta_value(cur_mid - prev_mid)
                diff_low = _qh_clean_delta_value(cur_low - prev_low)
                diff_high = _qh_clean_delta_value(cur_high - prev_high)
                rel = float(diff_mid or 0.0) / abs(float(prev_mid))
                parts = [_qh_fmt_delta_main(diff_mid, rel, unit, prefix="Δ")]
                show_range_sides = any(abs(float(v or 0.0)) > 1e-12 for v in (diff_low, diff_high))
                if show_range_sides:
                    if diff_low is not None:
                        parts.append(f"L {_qh_fmt_delta_value_for_unit(diff_low, unit)}")
                    if diff_high is not None:
                        parts.append(f"H {_qh_fmt_delta_value_for_unit(diff_high, unit)}")
                return " | ".join([p for p in parts if p])
            if cur_low is not None and cur_high is None and prev_item and prev_low is not None and prev_high is None:
                diff_low = _qh_clean_delta_value(cur_low - prev_low)
                rel = None if abs(float(prev_low)) <= 1e-12 else (float(diff_low or 0.0) / abs(float(prev_low)))
                return _qh_fmt_delta_main(diff_low, rel, unit, prefix="Δ floor")
            if cur_point is not None and prev_item and prev_point is not None:
                diff_point = _qh_clean_delta_value(cur_point - prev_point)
                rel = None if abs(float(prev_point)) <= 1e-12 else (float(diff_point or 0.0) / abs(float(prev_point)))
                return _qh_fmt_delta_main(diff_point, rel, unit, prefix="Δ")
            if cur_high is not None and cur_low is None and prev_item and prev_high is not None and prev_low is None:
                diff_high = _qh_clean_delta_value(cur_high - prev_high)
                rel = None if abs(float(prev_high)) <= 1e-12 else (float(diff_high or 0.0) / abs(float(prev_high)))
                return _qh_fmt_delta_main(diff_high, rel, unit, prefix="Δ")
            cur_mid = _qh_item_mid(item)
            prev_mid = _qh_item_mid(prev_item)
            unit = str(item.get("unit") or "")
            if cur_mid is not None and prev_mid is not None and abs(float(prev_mid)) > 1e-12:
                diff = float(cur_mid) - float(prev_mid)
                rel = diff / abs(float(prev_mid))
                if abs(rel) > 5.0:
                    return ""
                if unit == "$m":
                    if abs(float(prev_mid)) < 50_000_000:
                        return ""
                    return f"Δ {_qh_fmt_money_signed(diff)} ({_qh_fmt_delta_rel_pct(rel)})"
                if unit == "$":
                    if abs(float(prev_mid)) < 0.10:
                        return ""
                    sign = "+" if diff >= 0 else "-"
                    return f"Δ {sign}${abs(diff):,.2f} ({_qh_fmt_delta_rel_pct(rel)})"
                if unit == "%":
                    sign = "+" if diff >= 0 else "-"
                    return f"Δ {sign}{abs(diff):.1f}pp"
                if unit == "x":
                    sign = "+" if diff >= 0 else "-"
                    return f"Δ {sign}{abs(diff):.2f}x ({_qh_fmt_delta_rel_pct(rel)})"
                if unit == "bps":
                    sign = "+" if diff >= 0 else "-"
                    return f"Δ {sign}{abs(diff):.0f}bps"
                return _qh_fmt_delta_rel_pct(rel)
            if str(item.get("kind") or "") in {"text", "qualitative_range"}:
                cur_txt = _qh_norm_txt(item.get("text") or "").lower()
                prev_txt = _qh_norm_txt(prev_item.get("text") or "").lower() if prev_item else ""
                if not prev_item:
                    return "new"
                return "" if cur_txt == prev_txt else "changed"
            return ""

        latest_asof_ord = _qh_quarter_ord(pd.Timestamp(q0_ref))
        current_asof_ord = _qh_quarter_ord(pd.Timestamp(asof_ref))

        def _qh_allow_guidance_item_in_block(item: Dict[str, Any]) -> bool:
            stated_ord = _qh_quarter_ord_from_label(_qh_display_stated_in(item))
            if stated_ord is None or stated_ord <= current_asof_ord:
                return True
            return current_asof_ord == latest_asof_ord

        shown_updated = [x for x in shown_updated if _qh_allow_guidance_item_in_block(x)]
        shown_carry = [x for x in shown_carry if _qh_allow_guidance_item_in_block(x)]
        if is_gpre_profile and current_asof_ord == latest_asof_ord and (len(shown_updated) + len(shown_carry)) < 4:
            existing_guidance_keys = {
                (
                    str(it.get("metric") or "").strip().lower(),
                    str(it.get("target_period_norm") or it.get("period_norm") or "").strip(),
                    glx_normalize_text(str(it.get("text") or "")).lower(),
                )
                for it in (shown_updated + shown_carry)
            }
            soft_candidates = sorted(
                [dict(v) for v in state_items_soft.values()],
                key=lambda x: (
                    0 if not bool(x.get("carry_forward")) else 1,
                    pri.get(str(x.get("metric") or FORWARD_NOTES_LABEL), 99),
                    _period_sort_for_ui(str(x.get("period_norm") or "UNK")),
                    -int(x.get("source_priority") or 0),
                    -float(x.get("score") or 0),
                ),
            )
            for soft_item in soft_candidates:
                if bool(soft_item.get("carry_forward")):
                    continue
                if not _qh_allow_guidance_item_in_block(soft_item):
                    continue
                soft_key = (
                    str(soft_item.get("metric") or "").strip().lower(),
                    str(soft_item.get("target_period_norm") or soft_item.get("period_norm") or "").strip(),
                    glx_normalize_text(str(soft_item.get("text") or "")).lower(),
                )
                if soft_key in existing_guidance_keys:
                    continue
                shown_updated.append(soft_item)
                existing_guidance_keys.add(soft_key)
                if (len(shown_updated) + len(shown_carry)) >= min(max_items_per_guidance_block, 8):
                    break
        def _qh_has_visible_guidance_payload(item: Dict[str, Any]) -> bool:
            metric_txt = str(item.get("metric") or "").strip()
            kind_txt = str(item.get("kind") or "").strip().lower()
            if metric_txt and metric_txt.lower() != "none":
                return True
            if kind_txt in {"range", "point"} and any(item.get(k) is not None for k in ("low", "high", "value")):
                return True
            for key in ("text", "exact_language", "qualitative_range_text"):
                if str(item.get(key) or "").strip():
                    return True
            return False

        shown_updated = [x for x in shown_updated if _qh_has_visible_guidance_payload(x)]
        shown_carry = [x for x in shown_carry if _qh_has_visible_guidance_payload(x)]
        if is_gpre_profile:
            def _is_gpre_non_formal_guidance_item_for_panel(item_in: Dict[str, Any]) -> bool:
                blob_local = glx_normalize_text(
                    " | ".join(
                        [
                            str(item_in.get("metric") or ""),
                            str(item_in.get("text") or ""),
                            str(item_in.get("exact_language") or ""),
                            str(item_in.get("qualitative_range_text") or ""),
                            str(item_in.get("target_display") or ""),
                        ]
                    )
                ).lower()
                return bool(
                    "q2 commercial setup" in blob_local
                    or (
                        "commercial setup" in blob_local
                        and not re.search(r"\b(capex|ebitda|revenue|eps|fcf|free cash flow|guidance|outlook)\b", blob_local, re.I)
                    )
                )

            shown_updated = [x for x in shown_updated if not _is_gpre_non_formal_guidance_item_for_panel(x)]
            shown_carry = [x for x in shown_carry if not _is_gpre_non_formal_guidance_item_for_panel(x)]
        shown = shown_updated + shown_carry
        found_metrics = sorted(
            {
                str(x.get("metric") or FORWARD_NOTES_LABEL)
                for x in shown
                if str(x.get("metric") or FORWARD_NOTES_LABEL) != FORWARD_NOTES_LABEL
            }
        )
        found_txt = ", ".join(found_metrics) if found_metrics else "none"
        h_cell.value = f"Guidance (As of {asof_txt}) - Status: {snap.get('status') or 'Unknown'}"
        if found_txt and found_txt != "none":
            h_cell.comment = Comment(f"Found metrics: {found_txt}", "Codex")
        key_metric = str(snap.get("key_metric") or "")
        key_period_norm = str(snap.get("key_period_norm") or "")
        exact_snip = truncate_clean(_qh_norm_txt(snap.get("exact_language") or ""), 320)
        exact_source = _qh_source_comment(snap.get("exact_source") or snap.get("source") or {})
        rendered_rows: List[Tuple[int, Dict[str, Any]]] = []

        def _qh_carry_family_key(item: Dict[str, Any]) -> str:
            metric_txt = str(item.get("metric") or FORWARD_NOTES_LABEL).strip()
            blob = _qh_norm_txt(
                " | ".join(
                    [
                        metric_txt,
                        str(item.get("text") or ""),
                        str(item.get("exact_language") or ""),
                        str(item.get("qualitative_range_text") or ""),
                    ]
                )
            ).lower()
            if re.search(r"\bcost savings\b", blob, re.I):
                return "cost_savings"
            if re.search(r"\bpb bank\b|\bbank-held leases\b|\bliquidity release\b", blob, re.I):
                return "pb_bank_liquidity"
            return " | ".join(
                [
                    metric_txt.lower(),
                    str(item.get("period_norm") or ""),
                    str(item.get("guidance_type") or ""),
                ]
            )

        def _qh_best_progress_row_for_family(family_key: str) -> Optional[Dict[str, Any]]:
            if family_key not in {"cost_savings", "pb_bank_liquidity"} or not is_pbi_profile:
                return None
            best_row: Optional[Dict[str, Any]] = None
            best_score: Tuple[int, int, float, int] = (-1, -1, -1.0, -1)

            def _latest_text_local(raw_latest: Any, raw_metric: str) -> str:
                if isinstance(raw_latest, (int, float)) and not pd.isna(raw_latest):
                    latest_num = float(raw_latest)
                    if "eps" in raw_metric.lower():
                        return f"${latest_num:.2f}"
                    if abs(latest_num) >= 1_000_000:
                        return _fmt_short_money_value_local(latest_num)
                    if abs(latest_num) >= 1_000:
                        return f"${latest_num:,.1f}"
                    return f"${latest_num:,.2f}"
                latest_txt_local = glx_normalize_text(str(raw_latest or "")).strip()
                if latest_txt_local.lower() in {"", "nan", "none", "null", "n/a", "not yet measurable"}:
                    return ""
                return latest_txt_local

            def _family_matches_local(metric_txt: str, blob_txt: str) -> bool:
                if family_key == "cost_savings":
                    return bool(re.search(r"\bcost savings|annualized savings|run-rate savings|cost reduction\b", blob_txt, re.I))
                return bool(re.search(r"\bpb bank\b|\bbank-held leases\b|\bliquidity release\b", blob_txt, re.I))

            if isinstance(promise_progress, pd.DataFrame) and not promise_progress.empty and "quarter" in promise_progress.columns:
                prog_local = promise_progress.copy()
                prog_local["quarter"] = pd.to_datetime(prog_local["quarter"], errors="coerce")
                prog_local = prog_local[prog_local["quarter"].notna()]
                for _, rec in prog_local.iterrows():
                    metric_txt = str(rec.get("metric_ref") or rec.get("metric_display") or rec.get("metric") or "").strip()
                    blob_txt = glx_normalize_text(
                        " | ".join(
                            [
                                metric_txt,
                                str(rec.get("target") or ""),
                                str(rec.get("latest") or rec.get("actual") or ""),
                                str(rec.get("rationale") or ""),
                            ]
                        )
                    ).strip()
                    if not _family_matches_local(metric_txt, blob_txt):
                        continue
                    latest_txt_local = _latest_text_local(rec.get("latest") if rec.get("latest") is not None else rec.get("actual"), metric_txt)
                    if not latest_txt_local:
                        continue
                    rec_q = pd.Timestamp(rec.get("quarter")).date()
                    result_txt = str(rec.get("status") or rec.get("result") or "").strip()
                    latest_amt_local = _parse_dollar_amount(latest_txt_local) or 0.0
                    score_local = (
                        rec_q.toordinal(),
                        status_rank.get(result_txt.lower(), 0),
                        latest_amt_local,
                        1 if "run-rate" in latest_txt_local.lower() else 0,
                    )
                    if score_local > best_score:
                        best_score = score_local
                        best_row = {
                            "latest": latest_txt_local,
                            "target": str(rec.get("target") or "").strip(),
                            "evaluated_through": str(rec_q),
                            "last_seen": str(rec.get("last_seen_quarter") or rec.get("last_seen") or ""),
                            "carried_to": str(rec.get("carried_to_quarter") or rec.get("carried_to") or ""),
                            "result": result_txt,
                            "rationale": glx_normalize_text(str(rec.get("rationale") or "")).strip(),
                        }
            if best_row is not None:
                return dict(best_row)

            best_row: Optional[Dict[str, Any]] = None
            best_score = (-1, -1, -1.0, -1)
            for cand in _load_profile_slide_signals():
                cand_q = None
                try:
                    cand_ts = pd.to_datetime(cand.get("quarter"), errors="coerce")
                    cand_q = cand_ts.date() if pd.notna(cand_ts) else None
                except Exception:
                    cand_q = None
                if not isinstance(cand_q, date):
                    continue
                cand_txt = glx_normalize_text(
                    " | ".join(
                        [
                            str(cand.get("metric_display") or cand.get("metric") or ""),
                            str(cand.get("target_display") or ""),
                            str(cand.get("text") or cand.get("rationale") or ""),
                        ]
                    )
                ).strip()
                if not _family_matches_local(str(cand.get("metric_display") or cand.get("metric") or ""), cand_txt):
                    continue
                if not cand_txt:
                    continue
                latest_txt_local = ""
                run_match = re.search(r"(\$[0-9][0-9.,]*(?:bn|m)?\s+run-rate)", cand_txt, re.I)
                impl_match = re.search(r"(\$[0-9][0-9.,]*(?:bn|m)?\s+implemented)", cand_txt, re.I)
                lease_match = re.search(r"(\$[0-9][0-9.,]*(?:bn|m)?[^|]{0,40}\bbank-held leases\b)", cand_txt, re.I)
                if run_match:
                    latest_txt_local = str(run_match.group(1) or "").strip()
                elif impl_match:
                    latest_txt_local = str(impl_match.group(1) or "").strip()
                elif lease_match:
                    latest_txt_local = str(lease_match.group(1) or "").strip()
                if not latest_txt_local:
                    continue
                target_metric = "Cost savings target" if family_key == "cost_savings" else "PB Bank liquidity release"
                target_txt = str(_extract_pbi_target_display(cand_txt, target_metric) or "").strip()
                latest_amt_local = _parse_dollar_amount(latest_txt_local) or 0.0
                score_local = (
                    cand_q.toordinal(),
                    1 if "raised target" in cand_txt.lower() else 0,
                    latest_amt_local,
                    1 if "run-rate" in latest_txt_local.lower() else 0,
                )
                if score_local > best_score:
                    best_score = score_local
                    best_row = {
                        "latest": latest_txt_local,
                        "target": target_txt,
                        "evaluated_through": str(cand_q),
                        "rationale": cand_txt,
                    }
            return dict(best_row) if isinstance(best_row, dict) else None

        def _qh_carry_metric_label(item: Dict[str, Any]) -> str:
            family_key = _qh_carry_family_key(item)
            if family_key == "cost_savings":
                return "Cost savings run-rate"
            if family_key == "pb_bank_liquidity":
                return "PB Bank liquidity"
            return str(item.get("metric") or FORWARD_NOTES_LABEL)

        def _qh_carry_realized_text(item: Dict[str, Any]) -> str:
            family_key = _qh_carry_family_key(item)
            if family_key not in {"cost_savings", "pb_bank_liquidity"}:
                return ""
            blob = _qh_norm_txt(
                " | ".join(
                    [
                        str(item.get("text") or ""),
                        str(item.get("exact_language") or ""),
                        str(item.get("qualitative_range_text") or ""),
                    ]
                )
            )
            latest_basis = ""
            latest_match = re.search(r"\blatest disclosed\s+([^.;|]+)", blob, re.I)
            if latest_match:
                latest_basis = latest_match.group(1).strip()
            elif re.search(r"\$[0-9][0-9.,]*(?:bn|m)?\s+run-rate\b", blob, re.I):
                money_match = re.search(r"(\$[0-9][0-9.,]*(?:bn|m)?\s+run-rate)", blob, re.I)
                latest_basis = str(money_match.group(1) or "").strip() if money_match else ""
            if not latest_basis:
                best_progress_row = _qh_best_progress_row_for_family(family_key)
                latest_basis = glx_normalize_text(str((best_progress_row or {}).get("latest") or "")).strip()
            if family_key == "cost_savings":
                latest_basis = re.sub(r"\s+run-rate\b", "", latest_basis, flags=re.I).strip()
            return f"{latest_basis} realized" if latest_basis else ""

        def _qh_carry_value_text(item: Dict[str, Any]) -> str:
            family_key = _qh_carry_family_key(item)
            if family_key == "cost_savings":
                blob = _qh_norm_txt(
                    " | ".join(
                        [
                            str(item.get("metric") or ""),
                            str(item.get("text") or ""),
                            str(item.get("exact_language") or ""),
                            str(item.get("qualitative_range_text") or ""),
                        ]
                    )
                )
                prior_target_txt = ""
                prior_match = re.search(
                    r"\b(?:from|up from|prior target(?: of)?|previous target(?: of)?)\s+(\$[0-9][0-9.,]*(?:bn|m)?(?:\s*-\s*\$?[0-9][0-9.,]*(?:bn|m)?)?)",
                    blob,
                    re.I,
                )
                if prior_match:
                    prior_target_txt = str(prior_match.group(1) or "").replace(" ", "").strip()
                    prior_target_txt = prior_target_txt.replace("$$", "$")
                target_txt = str(_extract_pbi_target_display(blob, "Cost savings target") or "").strip()
                if not target_txt:
                    best_progress_row = _qh_best_progress_row_for_family(family_key)
                    target_txt = str((best_progress_row or {}).get("target") or "").strip()
                if not prior_target_txt and prev_ref is not None:
                    for prev_item in _qh_items_current_for(prev_ref):
                        if _qh_carry_family_key(prev_item) != family_key:
                            continue
                        prev_blob = _qh_norm_txt(
                            " | ".join(
                                [
                                    str(prev_item.get("metric") or ""),
                                    str(prev_item.get("text") or ""),
                                    str(prev_item.get("exact_language") or ""),
                                    str(prev_item.get("qualitative_range_text") or ""),
                                ]
                            )
                        )
                        prev_target_txt = str(_extract_pbi_target_display(prev_blob, "Cost savings target") or "").strip()
                        if prev_target_txt and prev_target_txt != target_txt:
                            prior_target_txt = prev_target_txt
                            break
                if target_txt:
                    bits = [f"Raised target to {target_txt} annualized savings"]
                    if prior_target_txt and prior_target_txt != target_txt:
                        bits[0] += f" from {prior_target_txt}"
                    if re.search(r"\b(over|during)\s+the\s+next\s+year\b|\bnext\s+year\b|\bremaining actions are expected\b", blob, re.I):
                        bits.append("the remaining actions are expected over the next year")
                    return "; ".join(bits)
            return _qh_guidance_value_text(item)

        def _qh_collapse_carry_rows(rows_local: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            best_by_family: Dict[str, Dict[str, Any]] = {}
            for row_local in rows_local:
                family_key = _qh_carry_family_key(row_local)
                existing = best_by_family.get(family_key)
                if existing is None:
                    best_by_family[family_key] = dict(row_local)
                    continue
                candidate_value = _qh_guidance_value_text(row_local)
                existing_value = _qh_guidance_value_text(existing)
                candidate_score = (
                    float(row_local.get("score") or 0.0),
                    1 if _qh_carry_realized_text(row_local) else 0,
                    1 if "raised target" in candidate_value.lower() else 0,
                    len(candidate_value),
                )
                existing_score = (
                    float(existing.get("score") or 0.0),
                    1 if _qh_carry_realized_text(existing) else 0,
                    1 if "raised target" in existing_value.lower() else 0,
                    len(existing_value),
                )
                if candidate_score > existing_score:
                    best_by_family[family_key] = dict(row_local)
            ordered_rows: List[Dict[str, Any]] = []
            seen_families: set[str] = set()
            for row_local in rows_local:
                family_key = _qh_carry_family_key(row_local)
                if family_key in seen_families:
                    continue
                seen_families.add(family_key)
                ordered_rows.append(best_by_family[family_key])
            return ordered_rows

        def _qh_guidance_display_items_sorted(rows_local: List[Dict[str, Any]], *, carry: bool) -> List[Dict[str, Any]]:
            if not rows_local:
                return []
            if is_pbi_profile:
                ordered_metrics = {
                    "Revenue": 0,
                    "Adj EBIT": 1,
                    "Adj EPS": 2,
                    "FCF": 3,
                    "Cost savings target": 4,
                    "Cost savings run-rate": 5,
                    "PB Bank liquidity": 6,
                }
                numeric_metrics = {"Revenue", "Adj EBIT", "Adj EPS", "FCF", "Cost savings target", "PB Bank liquidity"}
            elif is_gpre_profile:
                ordered_metrics = {
                    "Capex guidance (FY 2026)": 0,
                    "Interest expense outlook": 1,
                    "45Z-related Adjusted EBITDA outlook": 2,
                    "45Z EBITDA guidance": 3,
                    "45Z facility contribution split": 4,
                    "Farm-practice upside timing": 5,
                    "Q2 commercial setup": 6,
                    "45Z base-case improvement": 7,
                    "Commercial positioning / setup": 8,
                    "Risk-management setup": 9,
                    "Coverage / openness": 10,
                }
                numeric_metrics = {"Capex guidance (FY 2026)", "Interest expense outlook"}
            else:
                ordered_metrics = {}
                numeric_metrics = set()

            def _metric_label_local(item: Dict[str, Any]) -> str:
                if carry:
                    return _qh_carry_metric_label(item)
                repaired_item = _qh_repair_display_item(item, asof_ref)
                return str(repaired_item.get("metric") or item.get("metric") or FORWARD_NOTES_LABEL).strip()

            def _sort_key(item: Dict[str, Any]) -> Tuple[int, int, int, int, int, str]:
                metric_label = _metric_label_local(item)
                class_rank = 0 if metric_label in numeric_metrics else 1
                explicit_rank = ordered_metrics.get(metric_label, 100 + class_rank * 50 + int(pri.get(metric_label, 99)))
                period_rank = _period_sort_for_ui(str(item.get("target_period_norm") or item.get("period_norm") or "UNK"))
                source_rank = -int(item.get("source_priority") or item.get("source_rank") or 0)
                score_rank = -int(round(float(item.get("score") or 0.0) * 10))
                metric_rank = str(metric_label or "")
                return (class_rank, explicit_rank, period_rank, source_rank, score_rank, metric_rank)

            return sorted(rows_local, key=_sort_key)

        if not shown:
            ws.merge_cells(start_row=row_ptr, start_column=col_metric_start, end_row=row_ptr, end_column=col_exact_end)
            z = ws.cell(row=row_ptr, column=col_metric_start, value="No guidance items for this quarter.")
            z.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
            z.border = panel_thin_border
            row_ptr += 1
        else:
            shown_updated = _qh_guidance_display_items_sorted(shown_updated, carry=False)
            shown_carry = _qh_guidance_display_items_sorted(shown_carry, carry=True)

            def _render_group(label: str, rows_local: List[Dict[str, Any]]) -> None:
                nonlocal row_ptr
                def _guidance_panel_metric_label(raw_metric: Any) -> str:
                    label_txt = str(raw_metric or FORWARD_NOTES_LABEL).strip()
                    clean_low = glx_normalize_text(label_txt).lower()
                    if is_gpre_profile:
                        replacements = {
                            "45z-related adjusted ebitda outlook": "45Z Adj EBITDA outlook",
                            "capex guidance (fy 2026)": "Capex guidance (2026 year)",
                        }
                        if clean_low in replacements:
                            return replacements[clean_low]
                    return label_txt

                def _is_actual_cashflow_commentary_not_guidance(item_in: Dict[str, Any]) -> bool:
                    blob_local = glx_normalize_text(
                        " | ".join(
                            [
                                str(item_in.get("metric") or ""),
                                str(item_in.get("text") or ""),
                                str(item_in.get("exact_language") or ""),
                                str(item_in.get("qualitative_range_text") or ""),
                                str(item_in.get("target_display") or ""),
                            ]
                        )
                    ).lower()
                    return bool(
                        re.search(r"\binvesting activities\b", blob_local, re.I)
                        and re.search(r"\blower capital expenditures\b|\bcapital expenditures\b", blob_local, re.I)
                        and not re.search(r"\bexpects?|guidance|outlook|target|anticipates?|forecast\b", blob_local, re.I)
                    )

                def _is_gpre_operating_outlook_not_formal_guidance(item_in: Dict[str, Any]) -> bool:
                    blob_local = glx_normalize_text(
                        " | ".join(
                            [
                                str(item_in.get("metric") or ""),
                                str(item_in.get("text") or ""),
                                str(item_in.get("exact_language") or ""),
                                str(item_in.get("qualitative_range_text") or ""),
                                str(item_in.get("target_display") or ""),
                            ]
                        )
                    ).lower()
                    return bool(
                        "q2 commercial setup" in blob_local
                        or (
                            "commercial setup" in blob_local
                            and not re.search(r"\b(capex|ebitda|revenue|eps|fcf|free cash flow|guidance|outlook)\b", blob_local, re.I)
                        )
                    )

                if is_gpre_profile:
                    rows_local = [
                        itm for itm in rows_local
                        if not _is_actual_cashflow_commentary_not_guidance(itm)
                        and not _is_gpre_operating_outlook_not_formal_guidance(itm)
                    ]
                    if not rows_local:
                        return
                if label.startswith("B)"):
                    if is_gpre_profile and pd.Timestamp(asof_ref).date() == pd.Timestamp(q0_ref).date():
                        def _is_stale_gpre_current_45z_starting_point(item_in: Dict[str, Any]) -> bool:
                            blob_local = glx_normalize_text(
                                " | ".join(
                                    [
                                        str(item_in.get("metric") or ""),
                                        str(item_in.get("text") or ""),
                                        str(item_in.get("exact_language") or ""),
                                        str(item_in.get("qualitative_range_text") or ""),
                                    ]
                                )
                            ).lower()
                            return bool(
                                "45z" in blob_local
                                and re.search(r"\bstarting point\b", blob_local, re.I)
                                and re.search(r"\$?\s*188m\b", blob_local, re.I)
                            )

                        rows_local = [
                            itm for itm in rows_local
                            if not _is_stale_gpre_current_45z_starting_point(itm)
                        ]
                        if not rows_local:
                            return
                    rows_local = _qh_collapse_carry_rows(rows_local)
                    rows_local = _qh_guidance_display_items_sorted(rows_local, carry=True)
                for idx_item, itm in enumerate(rows_local):
                    rr = row_ptr
                    row_ptr += 1
                    rendered_rows.append((rr, itm))
                    is_carry_row = bool(itm.get("carry_forward"))
                    ws.merge_cells(start_row=rr, start_column=col_metric_start, end_row=rr, end_column=col_metric_end)
                    ws.merge_cells(start_row=rr, start_column=col_guidance_start, end_row=rr, end_column=col_guidance_end)
                    ws.merge_cells(start_row=rr, start_column=col_exact_start, end_row=rr, end_column=col_exact_end)

                    metric_val = _qh_carry_metric_label(itm) if is_carry_row else _guidance_panel_metric_label(itm.get("metric") or FORWARD_NOTES_LABEL)
                    metric_cell = ws.cell(row=rr, column=col_metric_start, value=metric_val)
                    stated_cell = ws.cell(row=rr, column=col_stated_start, value=_qh_display_stated_in(itm))
                    horizon_cell = ws.cell(row=rr, column=col_horizon_start, value=_qh_display_horizon(itm))

                    val_cell = ws.cell(row=rr, column=col_guidance_start)
                    if metric_val == FORWARD_NOTES_LABEL:
                        val_cell.value = _qh_short(_qh_norm_txt(itm.get("text") or ""), 220)
                    elif is_carry_row:
                        val_cell.value = _qh_short(_qh_carry_value_text(itm), 220)
                    else:
                        val_cell.value = _qh_short(_qh_guidance_value_text(itm), 220)
                    exact_cell = ws.cell(row=rr, column=col_exact_start)
                    if is_carry_row:
                        exact_cell.value = _qh_carry_realized_text(itm)
                    else:
                        exact_cell.value = str(_delta_vs_prev(itm) or "").replace("Î”", "Δ")
                    _qh_set_comment(val_cell, _qh_item_comment(itm))

                    metric_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
                    stated_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
                    horizon_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
                    val_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                    exact_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

                    for cc in range(panel_col_start, panel_col_end + 1):
                        cell = ws.cell(row=rr, column=cc)
                        cell.fill = copy(panel_alt_fill if idx_item % 2 == 0 else panel_neutral_fill)
                        cell.font = copy(panel_body_font)
                        cell.border = panel_thin_border

                    ws.row_dimensions[rr].height = 19.5

            rendered_any_group = False
            if shown_updated:
                _render_group("A) Updated / mentioned this quarter", shown_updated)
                rendered_any_group = True
            if shown_carry:
                _render_group("B) Carry-forward", shown_carry)
                rendered_any_group = True
            if not rendered_any_group:
                ws.merge_cells(start_row=row_ptr, start_column=col_metric_start, end_row=row_ptr, end_column=col_exact_end)
                z = ws.cell(row=row_ptr, column=col_metric_start, value="No guidance items for this quarter.")
                z.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
                z.border = panel_thin_border
                row_ptr += 1

        key_row_idx: Optional[int] = None
        key_row_item: Optional[Dict[str, Any]] = None
        for rr, itm in rendered_rows:
            if str(itm.get("metric") or "") != key_metric:
                continue
            if key_period_norm and str(itm.get("period_norm") or "") != key_period_norm:
                continue
            key_row_idx = rr
            key_row_item = itm
            break
        if key_row_idx is None and rendered_rows:
            key_row_idx = rendered_rows[0][0]
            key_row_item = rendered_rows[0][1]
        if key_row_idx is not None and exact_snip:
            _qh_set_comment(
                ws.cell(row=key_row_idx, column=col_exact_start),
                f"Exact language: {_qh_norm_txt(snap.get('exact_language') or '')}\\n\\n{exact_source}",
            )

        row_ptr += 1


    return ValuationGuidanceRenderResult(
        panel_col_start=panel_col_start,
        panel_col_end=panel_col_end,
        additive_panel_end=additive_panel_end,
        panel_row_start=panel_row_start,
        col_metric_start=col_metric_start,
        col_stated_start=col_stated_start,
        col_horizon_start=col_horizon_start,
        col_guidance_start=col_guidance_start,
        col_exact_start=col_exact_start,
        side_panel_style=side_panel_style,
        guidance_snapshot_header_rows=guidance_snapshot_header_rows,
        overlaps=_overlaps,
        row_ptr=row_ptr,
    )
