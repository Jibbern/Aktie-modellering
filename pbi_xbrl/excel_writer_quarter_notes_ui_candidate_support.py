"""Quarter Notes UI candidate/model support helpers.

This module intentionally keeps candidate-map mutation and audit lifecycle in
excel_writer_context. It only hosts helper functions and row factories used by
that writer.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class QuarterNotesUiCandidateSupportDeps:
    runtime: MutableMapping[str, Any]


@dataclass(frozen=True)
class QuarterNotesUiGuidanceSummaryResult:
    records: List[Dict[str, Any]]
    appended_count: int


_PUBLIC_ALIASES = {
    "q_label": "_q_label",
    "parse_json": "_parse_json",
    "source_meta": "_source_meta",
    "candidate_texts": "_candidate_texts",
    "extract_time_context": "_extract_time_context",
    "extract_deadline": "_extract_deadline",
    "bucket": "_bucket",
    "metric_tag": "_metric_tag",
    "gpre_seed_rescue_rows": "_gpre_seed_rescue_rows",
    "pbi_bootstrap_note_rescue_rows": "_pbi_bootstrap_note_rescue_rows",
    "pbi_local_buyback_table_seed_rows": "_pbi_local_buyback_table_seed_rows",
    "anf_source_note_rescue_rows": "_anf_source_note_rescue_rows",
}


class QuarterNotesUiCandidateSupport:
    def __init__(self, deps: QuarterNotesUiCandidateSupportDeps) -> None:
        self.runtime = deps.runtime
        self._namespace = _build_quarter_notes_ui_candidate_support_namespace(deps.runtime)

    def __getattr__(self, name: str) -> Any:
        key = _PUBLIC_ALIASES.get(name, name)
        try:
            return self._namespace[key]
        except KeyError as exc:
            raise AttributeError(name) from exc

def _build_quarter_notes_ui_candidate_support_namespace(runtime: MutableMapping[str, Any]) -> Dict[str, Any]:
    __rt = runtime
    def _rt_get(name: str) -> Any:
        if name in __rt:
            return __rt[name]
        return globals().get(name)
    __state = _rt_get('quarter_notes_candidate_support_state')
    if not isinstance(__state, MutableMapping):
        __state = __rt
    COMPANY_PROFILES = _rt_get('COMPANY_PROFILES')
    QuarterNotesUiSourceRescueDeps = _rt_get('QuarterNotesUiSourceRescueDeps')
    QuarterNotesUiSourceRescueSupport = _rt_get('QuarterNotesUiSourceRescueSupport')
    _add_candidate = _rt_get('_add_candidate')
    _add_sec_dir_local = _rt_get('_add_sec_dir_local')
    _add_sec_scan_dir = _rt_get('_add_sec_scan_dir')
    _add_seed_dir = _rt_get('_add_seed_dir')
    _add_text = _rt_get('_add_text')
    _add_variant = _rt_get('_add_variant')
    _allow_repo_profile_cache_fallback = _rt_get('_allow_repo_profile_cache_fallback')
    _append_doc_row_local = _rt_get('_append_doc_row_local')
    _build = _rt_get('_build')
    _cache_doc_hit_local = _rt_get('_cache_doc_hit_local')
    _cache_roots = _rt_get('_cache_roots')
    _classify_pbi_metric_label = _rt_get('_classify_pbi_metric_label')
    _clean_driver_phrase = _rt_get('_clean_driver_phrase')
    _coerce_amount_with_unit_local = _rt_get('_coerce_amount_with_unit_local')
    _doc_text_variants_local = _rt_get('_doc_text_variants_local')
    _ensure_terminal_period = _rt_get('_ensure_terminal_period')
    _extract_from_chunk = _rt_get('_extract_from_chunk')
    _extract_pbi_target_display = _rt_get('_extract_pbi_target_display')
    _extract_valuation_filing_doc_text = _rt_get('_extract_valuation_filing_doc_text')
    _find_first = _rt_get('_find_first')
    _finish = _rt_get('_finish')
    _fmt_item_value = _rt_get('_fmt_item_value')
    _fmt_share_count = _rt_get('_fmt_share_count')
    _fmt_short_millions_str = _rt_get('_fmt_short_millions_str')
    _fmt_short_money_value_local = _rt_get('_fmt_short_money_value_local')
    _format_cutoff = _rt_get('_format_cutoff')
    _format_directional_fcf_summary_local = _rt_get('_format_directional_fcf_summary_local')
    _format_directional_from_prior_summary_local = _rt_get('_format_directional_from_prior_summary_local')
    _gpre_carbon_capture_status_summary_local = _rt_get('_gpre_carbon_capture_status_summary_local')
    _has_post_quarter_anchor = _rt_get('_has_post_quarter_anchor')
    _infer_doc_quarter_local = _rt_get('_infer_doc_quarter_local')
    _is_fy_block = _rt_get('_is_fy_block')
    _is_pbi_clean_sentence = _rt_get('_is_pbi_clean_sentence')
    _is_preferred_narrative_source = _rt_get('_is_preferred_narrative_source')
    _is_repo_profile_cache_path = _rt_get('_is_repo_profile_cache_path')
    _num_col = _rt_get('_num_col')
    _parse_gpre_crush_margin_pair_local = _rt_get('_parse_gpre_crush_margin_pair_local')
    _parse_money = _rt_get('_parse_money')
    _parse_quarter_from_filename = _rt_get('_parse_quarter_from_filename')
    _parse_quarter_from_follow_text = _rt_get('_parse_quarter_from_follow_text')
    _parse_shares = _rt_get('_parse_shares')
    _path_belongs_to_ticker = _rt_get('_path_belongs_to_ticker')
    _path_cache_key = _rt_get('_path_cache_key')
    _pbi_adj_fcf_map = _rt_get('_pbi_adj_fcf_map')
    _pbi_guidance_period_label_from_text = _rt_get('_pbi_guidance_period_label_from_text')
    _pbi_guidance_self_contained_summary = _rt_get('_pbi_guidance_self_contained_summary')
    _pbi_hist_buybacks_cash_map = _rt_get('_pbi_hist_buybacks_cash_map')
    _pbi_hist_debt_repayment_map = _rt_get('_pbi_hist_debt_repayment_map')
    _pbi_reported_fcf_payload_for_qd = _rt_get('_pbi_reported_fcf_payload_for_qd')
    _pbi_revolver_availability_map = _rt_get('_pbi_revolver_availability_map')
    _pbi_target_display_ok = _rt_get('_pbi_target_display_ok')
    _prev_available_quarter = _rt_get('_prev_available_quarter')
    _prev_same_quarter_year = _rt_get('_prev_same_quarter_year')
    _promises_view = _rt_get('_promises_view')
    _quarter_notes_raw_records_by_quarter_local = _rt_get('_quarter_notes_raw_records_by_quarter_local')
    _read_cached_doc_raw = _rt_get('_read_cached_doc_raw')
    _read_cached_doc_text = _rt_get('_read_cached_doc_text')
    _read_material_text = _rt_get('_read_material_text')
    _record_writer_elapsed = _rt_get('_record_writer_elapsed')
    _resolve_col = _rt_get('_resolve_col')
    _row = _rt_get('_row')
    _sec_cache_html_paths_local = _rt_get('_sec_cache_html_paths_local')
    _text_cached_runtime_value_local = _rt_get('_text_cached_runtime_value_local')
    _text_fragment_penalty = _rt_get('_text_fragment_penalty')
    _text_rank = _rt_get('_text_rank')
    adj_metrics = _rt_get('adj_metrics')
    as_of_qd = _rt_get('as_of_qd')
    body_col = _rt_get('body_col')
    cache_dir = _rt_get('cache_dir')
    cache_roots = _rt_get('cache_roots')
    candidate_summary = _rt_get('candidate_summary')
    candidate_type = _rt_get('candidate_type')
    cap_alloc_exec_by_q = _rt_get('cap_alloc_exec_by_q')
    cap_alloc_tone_by_q = _rt_get('cap_alloc_tone_by_q')
    capital_return_build_dividend_note = _rt_get('capital_return_build_dividend_note')
    capital_return_build_dividend_note_from_text = _rt_get('capital_return_build_dividend_note_from_text')
    cat = _rt_get('cat')
    chunk_in = _rt_get('chunk_in')
    claim_col = _rt_get('claim_col')
    cols = _rt_get('cols')
    company_profile = _rt_get('company_profile')
    ctx_ref = _rt_get('ctx_ref')
    def _current_ctx_ref() -> Any:
        getter = _rt_get('ctx_ref_getter')
        if callable(getter):
            try:
                return getter()
            except Exception:
                pass
        return ctx_ref
    current_summary = _rt_get('current_summary')
    data_root_from_sec_cache_path = _rt_get('data_root_from_sec_cache_path')
    default_q = _rt_get('default_q')
    df = _rt_get('df')
    dir_in = _rt_get('dir_in')
    dir_specs = _rt_get('dir_specs')
    ev_json_col = _rt_get('ev_json_col')
    ev_snip_col = _rt_get('ev_snip_col')
    extra_terms = _rt_get('extra_terms')
    glx_dedup_text_key = _rt_get('glx_dedup_text_key')
    glx_extract_numeric_patterns = _rt_get('glx_extract_numeric_patterns')
    glx_normalize_period = _rt_get('glx_normalize_period')
    glx_normalize_text = _rt_get('glx_normalize_text')
    glx_split_sentences = _rt_get('glx_split_sentences')
    best_by_key = _rt_get('best_by_key')
    hist = _rt_get('hist')
    hit_in = _rt_get('hit_in')
    html = _rt_get('html')
    infer_quarter_end_from_text = _rt_get('infer_quarter_end_from_text')
    is_anf_profile = _rt_get('is_anf_profile')
    is_gpre_profile = _rt_get('is_gpre_profile')
    is_pbi_profile = _rt_get('is_pbi_profile')
    item = _rt_get('item')
    material_roots = _rt_get('material_roots')
    max_matches = _rt_get('max_matches')
    max_sentences = _rt_get('max_sentences')
    metric_canon = _rt_get('metric_canon')
    metric_col = _rt_get('metric_col')
    metric_ref = _rt_get('metric_ref')
    metric_val_col = _rt_get('metric_val_col')
    min_year = _rt_get('min_year')
    month_in = _rt_get('month_in')
    note_col = _rt_get('note_col')
    note_id_col = _rt_get('note_id_col')
    note_text = _rt_get('note_text')
    pattern_in = _rt_get('pattern_in')
    primary_label = _rt_get('primary_label')
    profile_in = _rt_get('profile_in')
    profile_ticker = _rt_get('profile_ticker')
    q_col = _rt_get('q_col')
    q_items = _rt_get('q_items')
    q_items_in = _rt_get('q_items_in')
    qd = _rt_get('qd')
    qd_ref = _rt_get('qd_ref')
    qn_compact_snippet = _rt_get('qn_compact_snippet')
    quarter_notes = _rt_get('quarter_notes')
    quarter_slice_cache = _rt_get('quarter_slice_cache')
    quarters = _rt_get('quarters')
    raw_driver = _rt_get('raw_driver')
    raw_num = _rt_get('raw_num')
    raw_val = _rt_get('raw_val')
    rec_in = _rt_get('rec_in')
    row_in = _rt_get('row_in')
    sev_score_col = _rt_get('sev_score_col')
    shared_classify_statement_evidence_role = _rt_get('shared_classify_statement_evidence_role')
    shared_renderable_note_drop_reason = _rt_get('shared_renderable_note_drop_reason')
    source_hint = _rt_get('source_hint')
    source_type_in = _rt_get('source_type_in')
    strip_html = _rt_get('strip_html')
    summary_in = _rt_get('summary_in')
    symbol_in = _rt_get('symbol_in')
    text_in = _rt_get('text_in')
    ticker = _rt_get('ticker')
    ticker_cache_roots_from_base_dir = _rt_get('ticker_cache_roots_from_base_dir')
    ticker_roots = _rt_get('ticker_roots')
    txt_in = _rt_get('txt_in')
    txt_low = _rt_get('txt_low')
    ui_info_rows = _rt_get('ui_info_rows')
    ui_state = _rt_get('ui_state')
    unit_in = _rt_get('unit_in')
    value_in = _rt_get('value_in')
    year_in = _rt_get('year_in')


    def _q_label(v: Any) -> str:
        t = pd.to_datetime(v, errors="coerce")
        if pd.isna(t):
            return "N/A"
        qn = ((int(t.month) - 1) // 3) + 1
        return f"Q{qn} {int(t.year)}"

    def _parse_json(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list) and raw and isinstance(raw[0], dict):
            return raw[0]
        if not isinstance(raw, str) or not raw.strip():
            return {}
        try:
            z = json.loads(raw)
            if isinstance(z, dict):
                return z
            if isinstance(z, list) and z and isinstance(z[0], dict):
                return z[0]
        except Exception:
            return {}
        return {}

    def _source_meta(row: pd.Series) -> Dict[str, Any]:
        ev = _parse_json(row.get(ev_json_col) if ev_json_col else None)
        return {
            "source_type": row.get("doc_type") or row.get("method") or ev.get("doc_type") or "quarter_notes_ui",
            "form": row.get("form") or ev.get("form") or "",
            "accn": row.get("accn") or ev.get("accn") or "",
            "filed": row.get("filed") or ev.get("filed") or None,
            "doc": row.get("doc") or row.get("doc_path") or ev.get("doc_path") or "",
            "section": row.get("section_or_page") or ev.get("section") or ev.get("section_or_page") or "",
            "source_doc_end": row.get(q_col) if q_col else ev.get("source_doc_end"),
            "source_document_id": row.get("source_document_id") or ev.get("source_document_id") or "",
            "source_occurrence_id": (
                row.get("source_occurrence_id")
                or row.get("evidence_occurrence_id")
                or ev.get("source_occurrence_id")
                or ev.get("evidence_occurrence_id")
                or ""
            ),
            "source_locator": (
                row.get("source_locator")
                or row.get("section_or_page")
                or ev.get("source_locator")
                or ev.get("section_or_page")
                or ""
            ),
        }

    def _candidate_texts(row: pd.Series) -> List[str]:
        vals = [
            row.get(claim_col),
            row.get(note_col) if note_col else None,
            row.get(body_col) if body_col else None,
            row.get(ev_snip_col) if ev_snip_col else None,
        ]
        out: List[str] = []
        for v in vals:
            t = glx_normalize_text(v)
            if t and t not in out:
                out.append(t)
        def _text_rank(txt: str) -> Tuple[int, int, int]:
            detail_hits = len(re.findall(r"(\$|\d|bps|from .* to|remaining capacity|revolver|repurchas\w*|reduced debt|margin expanded|operating expenses declined)", txt, re.I))
            numeric_hits = len(re.findall(r"\$?\s*\d+(?:\.\d+)?\s*(?:%|m|mm|million|b|bn|x)?", txt, re.I))
            return (detail_hits, numeric_hits, len(txt))
        return sorted(out, key=_text_rank, reverse=True)

    def _extract_time_context(text: str, as_of_qd: date) -> Dict[str, Any]:
        txt = glx_normalize_text(text)
        low = txt.lower()
        years_ref = sorted({int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", txt)})
        anchor_year: Optional[int] = None
        year_pair: Optional[Tuple[int, int]] = None
        is_year_comp = False

        m_pair = re.search(r"\bin\s+(20\d{2})\s+compared\s+to\s+(20\d{2})\b", low, re.I)
        if m_pair:
            y1 = int(m_pair.group(1))
            y2 = int(m_pair.group(2))
            year_pair = (y1, y2)
            anchor_year = y1
            is_year_comp = True
        else:
            m_pair_prior = re.search(
                r"\b(?:for|in)\s+(20\d{2})\b[^.]{0,200}\bcompared\s+to\s+the\s+prior\s+year\b",
                low,
                re.I,
            )
            if m_pair_prior:
                y1 = int(m_pair_prior.group(1))
                y2 = y1 - 1
                year_pair = (y1, y2)
                anchor_year = y1
                is_year_comp = True
            m_pair_loose = re.search(r"\b(20\d{2})[^.]{0,120}\bcompared\s+to\s+(20\d{2})\b", low, re.I)
            if m_pair_loose:
                y1 = int(m_pair_loose.group(1))
                y2 = int(m_pair_loose.group(2))
                year_pair = (y1, y2)
                anchor_year = y1
                is_year_comp = True
            m_anchor = re.search(
                r"\bfor\s+(20\d{2})\s+was\b|\bfor\s+the\s+year\s+(20\d{2})\b|\bfor\s+(20\d{2})\b[^.]{0,120}\bcompared\s+to\s+the\s+prior\s+year\b",
                low,
                re.I,
            )
            if m_anchor:
                anchor_year = int(m_anchor.group(1) or m_anchor.group(2) or m_anchor.group(3))
            if anchor_year is None:
                m_anchor_q = re.search(
                    r"\b(?:during|in|for)\s+the\s+(?:first|second|third|fourth)\s+quarter\s+of\s+(20\d{2})\b",
                    low,
                    re.I,
                )
                if m_anchor_q:
                    anchor_year = int(m_anchor_q.group(1))
            if anchor_year is None:
                m_anchor_fy = re.search(r"\b(?:fiscal|fy)\s*[-/]?\s*(20\d{2})\b", low, re.I)
                if m_anchor_fy:
                    anchor_year = int(m_anchor_fy.group(1))
            if "compared to" in low and len(years_ref) >= 2:
                y1, y2 = max(years_ref), min(years_ref)
                year_pair = (y1, y2)
                if anchor_year is None:
                    anchor_year = y1
                is_year_comp = True

        if re.search(r"\b(we expect|we anticipate|outlook|guidance|target|targets|on track|over the course of)\b", low):
            tense_hint = "future"
        elif re.search(r"\b(was|were|reported|incurred|recorded|decreased|declined|increased|grew|driven by|offset by|primarily)\b", low):
            tense_hint = "past"
        else:
            tense_hint = "unknown"
        has_forward_intent = bool(re.search(r"\b(expect|expects|guidance|outlook|forecast|target|targets|project|anticipate|plan|intend|on track)\b", low))
        has_period_anchor = bool(re.search(r"\b(fy\s*[-/]?\s*(?:20\d{2}|\d{2})|fiscal\s+(?:20\d{2}|\d{2})|full[- ]?year|next year|next quarter|q[1-4]\s*(?:20\d{2}|\d{2})?|by end of (?:the )?year)\b", low))
        period_label, period_key = glx_normalize_period(txt, as_of_qd)
        return {
            "anchor_year": anchor_year,
            "year_pair": year_pair,
            "is_year_comparison": bool(is_year_comp),
            "tense_hint": tense_hint,
            "referenced_years": years_ref,
            "has_forward_intent": has_forward_intent,
            "has_period_anchor": has_period_anchor,
            "target_period_norm": str(period_key or ""),
            "target_period_label": str(period_label or ""),
        }

    def _extract_deadline(text: str, as_of_qd: date) -> Optional[date]:
        low = glx_normalize_text(text).lower()
        m = re.search(r"\bby\s+(?:the\s+)?end\s+of\s+(?:the\s+)?first\s+half\s+of\s+(20\d{2})\b", low, re.I)
        if m:
            return date(int(m.group(1)), 6, 30)
        m = re.search(r"\bby\s+(?:the\s+)?end\s+of\s+(?:the\s+)?second\s+half\s+of\s+(20\d{2})\b", low, re.I)
        if m:
            return date(int(m.group(1)), 12, 31)
        m = re.search(r"\bby\s+(?:the\s+)?end\s+of\s+(20\d{2})\b", low, re.I)
        if m:
            return date(int(m.group(1)), 12, 31)
        m = re.search(r"\bover\s+the\s+course\s+of\s+(20\d{2})\b", low, re.I)
        if m:
            return date(int(m.group(1)), 12, 31)
        if re.search(r"\b(in\s+the\s+next\s+12\s+months|over\s+the\s+next\s+year|in\s+the\s+next\s+year)\b", low, re.I):
            try:
                return (pd.Timestamp(as_of_qd) + pd.DateOffset(months=12)).date()
            except Exception:
                return as_of_qd + timedelta(days=365)
        return None

    def _bucket(cat: str, text: str, metric: str, ctx: Dict[str, Any]) -> str:
        low = f"{cat} {metric} {text}".lower()
        has_fwd = bool(ctx.get("has_forward_intent"))
        tense = str(ctx.get("tense_hint") or "unknown")
        is_year_comp = bool(ctx.get("is_year_comparison"))
        cand_type = str(ctx.get("candidate_type") or "")
        if re.search(r"\b(assumed health care trend rate|market-related valuation|recognized over (?:a )?five-year period|actuarial assumptions)\b", low, re.I):
            return "Other / footnotes"
        if cand_type == "numeric_highlight":
            if any(k in low for k in ["segment", "sendtech", "presort", "operations", "pricing", "volume", "mix"]):
                return "Segments / strategy / operations"
            if any(k in low for k in ["debt", "liquidity", "covenant", "revolver", "refinanc", "maturity", "credit facility", "deleverag"]):
                return "Debt / liquidity / covenants"
            if any(k in low for k in ["one-time", "one time", "restructur", "impair", "special item", "transformation charge"]):
                return "One-time items / restructuring"
            if any(k in low for k in ["nol", "net operating loss", "tax expiration", "tax expirations", "pension", "opeb"]):
                return "Tax / NOL / pension"
            if any(k in low for k in ["fcf", "free cash flow", "capex", "cash flow", "operating cash", "cfo"]):
                return "Cash flow / FCF / capex"
            return "Results / drivers"
        if cand_type == "tone_line":
            return "Tone / expectations"
        if cand_type == "program_line" or program_hook_re.search(low):
            return "Programs / initiatives"
        if any(k in low for k in ["debt", "liquidity", "covenant", "revolver", "refinanc", "maturity", "credit facility", "deleverag"]):
            return "Debt / liquidity / covenants"
        if any(k in low for k in ["one-time", "one time", "restructur", "impair", "special item", "transformation charge"]):
            return "One-time items / restructuring"
        if any(k in low for k in ["nol", "net operating loss", "pension", "opeb", "postretirement", "health care trend rate"]):
            return "Tax / NOL / pension"
        if any(k in low for k in ["segment", "sendtech", "presort", "operations", "strategy", "pricing", "volume", "mix"]):
            if tense in {"past", "unknown"}:
                return "Segments / strategy / operations"
        if any(k in low for k in ["fcf", "free cash flow", "capex", "cash flow", "operating cash", "cfo"]):
            if tense in {"past", "unknown"}:
                return "Cash flow / FCF / capex"
        if tense == "past" and (is_year_comp or any(k in low for k in ["due to", "driven by", "primarily", "offset by", "pricing", "volume", "mix", "margin", "headwind", "tailwind", "pressure"])):
            return "Results / drivers"
        if has_fwd or tense == "future":
            return "Guidance / outlook"
        if any(k in low for k in ["risk", "headwind", "tailwind", "pressure", "lawsuit", "litigation", "regulatory"]):
            return "Other / footnotes"
        return "Other / footnotes"

    def _metric_tag(metric_ref: str, metric_canon: str, text: str) -> str:
        m = str(metric_ref or "").strip()
        if m.lower().startswith("text:"):
            m = m.split(":", 1)[1]
        if m.lower() in {"", "nan", "none", "n/a", "other", "unknown", "forward-looking notes"}:
            m = str(metric_canon or "").strip()
        if m.lower() in {"", "other", "unknown", "forward-looking notes"}:
            low = text.lower()
            if "revenue" in low:
                return "Revenue"
            if "ebitda" in low:
                return "Adj EBITDA"
            if "eps" in low:
                return "Adj EPS"
            if "fcf" in low or "free cash flow" in low:
                return "FCF"
            if "capex" in low:
                return "Capex"
            if "debt" in low or "leverage" in low:
                return "Debt"
            if "margin" in low:
                return "Margin"
            return ""
        return m

    placeholder_phrases = [
        "guidance signal in filing text",
        "[text:guidance]",
        "[text:",
        "signal in filing text",
    ]
    exclude_phrases = [
        "forward-looking statements",
        "safe harbor",
        "undertakes no obligation to update",
        "notes to condensed consolidated financial statements",
        "table amounts in thousands",
        "recently issued accounting pronouncements",
        "we are currently assessing the impact",
        "impact this standard will have",
        "operating results for the periods presented are not necessarily indicative",
        "conference call and webcast",
        "securities act",
        "registration statement",
        "section 3(a)(9)",
    ]
    hard_drop_re = re.compile(
        r"\b(settlement date|promptly following|administrative agent will promptly|"
        r"will be entitled to|securities act|registration statement|section 3\(a\)\(9\)|"
        r"exempt from registration|base salary|target bonus|employment agreement|"
        r"convertible notes|indenture|conversion|not anticipated to be material|not expected to be material|"
        r"you will be eligible for|relocation lump sum|hire date|one-time payment|"
        r"the borrower will not|restricted subsidiary|indebtedness, except|letters of credit|"
        r"prepay, redeem, purchase or otherwise satisfy|options will vest|qualifying terminations|"
        r"continued employment as ceo|eligible to be sold upon such vesting)\b",
        re.I,
    )
    forward_intent_re = re.compile(
        r"\b(expect|expects|guidance|outlook|forecast|target|targets|project|anticipate|plan|intend|on track|next year|next quarter|full[- ]year)\b",
        re.I,
    )
    period_anchor_re = re.compile(
        r"\b(fy\s*[-/]?\s*(?:20\d{2}|\d{2})|fiscal\s+(?:20\d{2}|\d{2})|full[- ]?year|next year|next quarter|q[1-4]\s*(?:20\d{2}|\d{2})?|by end of (?:the )?year)\b",
        re.I,
    )
    year_ref_re = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
    stale_historical_re = re.compile(r"\b(since|over the past|multi-year|cumulative|historically)\b", re.I)
    segment_driver_boost_re = re.compile(r"\b(sendtech|presort|segment|pricing|volume|mix|demand|churn|retention|backlog|pipeline)\b", re.I)
    change_re = re.compile(r"\b(improv\w*|declin\w*|increas\w*|decreas\w*|pressur\w*|headwind\w*|tailwind\w*|expand\w*|compress\w*|stabil\w*)\b", re.I)
    driver_re = re.compile(r"\b(pricing|volume|volumes|mix|demand|churn|retention|backlog|pipeline|restructuring|refinancing|covenant|liquidity|margin|cost|cash flow|fcf|capex)\b", re.I)
    driver_language_re = re.compile(
        r"\b(primarily|driven by|due to|offset by|partially|as a result|because)\b",
        re.I,
    )
    footnote_disclosure_re = re.compile(r"\b(assumed health care trend rate|market-related valuation|recognized over (?:a )?five-year period|actuarial assumptions)\b", re.I)
    numeric_token_re = re.compile(r"(?<![A-Za-z])(?:[$€£]|[+-]?\d[\d,]*(?:\.\d+)?\s*(?:%|x|bps|m|bn|million|billion)?)", re.I)
    causality_re = re.compile(
        r"\b(primarily due to|driven by|offset by|as a result of|reflecting|attributable to|benefited from|partially offset|largely due to|due to lower|due to higher)\b",
        re.I,
    )
    driver_noun_re = re.compile(
        r"\b(employee-related|headcount|salary|variable compensation|insurance|professional fees|outsourcing|marketing|travel|real estate|product cycle|pricing|volume|mix|demand|churn|retention|restructuring|impairment|refinancing|redemption|pension settlement|foreign currency revaluation)\b",
        re.I,
    )
    highlight_topic_re = re.compile(
        r"\b(sendtech|presort|global ecommerce|segment|segments|pricing|price\/mix|mix|volume|volumes|"
        r"headcount|roles?|restructuring charges?|refinancing|redemption|covenant|revolver|liquidity|"
        r"nol|net operating loss|tax expiration|tax expirations|finance receivables|bank deposits|net funding)\b",
        re.I,
    )
    capital_alloc_include_re = re.compile(
        r"\b(share repurchase|repurchas\w*|buyback|authorization|dividend|capital allocation|return of capital|"
        r"pacing|deploy cash|use of cash|deleverag)\b",
        re.I,
    )
    capital_alloc_context_re = re.compile(
        r"\b(intend|plan|expect|will|target|committed|prioritize|disciplined|continue|continued|"
        r"returning|returned|deployed|deploy|focus)\b",
        re.I,
    )
    capital_alloc_exclude_re = re.compile(
        r"\b(safe harbor|private securities litigation|registration|securities act)\b",
        re.I,
    )
    bank_financing_term_re = re.compile(
        r"\b(finance receivables|receivables|deposits|bank deposits|net funding|funding|liquidity|borrowings|revolver|facility|covenant)\b",
        re.I,
    )
    bank_financing_reason_re = re.compile(
        r"\b(primarily due to|driven by|reflecting|as a result of|due to|offset by|because|"
        r"increased[^.]{0,80}(?:due to|driven by|reflecting)|"
        r"decreased[^.]{0,80}(?:due to|driven by|reflecting)|"
        r"declined[^.]{0,80}(?:due to|driven by|reflecting))\b",
        re.I,
    )
    corporate_revenue_restatement_re = re.compile(
        r"\brevenue\b[^.]{0,140}\b(increased|decreased|declined|grew|rose|fell)\b",
        re.I,
    )
    bare_restatement_re = re.compile(
        r"\b(?:expense|expenses|revenue|income|sales|ebit|ebitda|margin)\b[^.]{0,80}\b(increased|decreased|declined|grew|rose|fell)\b[^.]{0,100}\$\s*[0-9]",
        re.I,
    )
    tone_topic_re = re.compile(
        r"\b(sendtech|presort|product cycle|momentum|pipeline|backlog|pricing|demand|macro|headwind|tailwind|"
        r"strategy|initiative|transition|stabilize|re-accelerate|turnaround|plan|capital allocation|"
        r"return of capital|buyback|repurchase|dividend|deleverag)\b",
        re.I,
    )
    tone_sentiment_re = re.compile(
        r"\b(momentum|stabilize|stabilizing|improve|improving|recover|recovery|exit low point|on track|confidence|visibility)\b",
        re.I,
    )
    future_soft_re = re.compile(r"\b(should|likely|we believe)\b", re.I)
    program_hook_re = re.compile(
        r"\b(plan|restructuring plan|headcount reductions?|office closures?|cost savings program|"
        r"refinancing|debt redemption|transformation|turnaround|product cycle|initiative)\b",
        re.I,
    )
    def is_tabular_fragment(text: str) -> bool:
        raw = str(text or "")
        if not raw.strip():
            return False
        t = re.sub(r"\s+", " ", raw).strip().lower()
        if "favorable/(unfavorable)" in t:
            return True
        if "actual % change" in t or ("% change" in t and "actual" in t):
            return True
        nums = re.findall(r"(?<![a-z])[-+]?\$?\d[\d,]*\.?\d*(?:%|m|b|bn|x)?", t)
        words = re.findall(r"[a-z]+", t)
        if len(nums) >= 10:
            return True
        if len(nums) >= 6 and ("  " in raw or "  %" in raw):
            return True
        if len(words) > 0 and (len(nums) / max(len(words), 1)) > 0.35:
            return True
        verb_hits = any(
            v in t
            for v in [
                "increased",
                "decreased",
                "declined",
                "grew",
                "driven",
                "primarily",
                "due to",
                "offset",
                "partially",
            ]
        )
        if len(nums) >= 8 and not verb_hits:
            return True
        if ("nine months ended" in t or "three months ended" in t) and len(nums) >= 4 and not verb_hits:
            return True
        return False
    margin_driver_re = re.compile(
        r"\b(margin|gross profit|gross margin)\b[^.]{0,120}\b(primarily due to|driven by|offset by|benefited from|as a result of)\b",
        re.I,
    )
    pricing_mix_volume_re = re.compile(
        r"\b(pricing|price\/mix|mix|volume|volumes|demand)\b[^.]{0,120}\b(primarily due to|driven by|offset by|benefited from|as a result of)\b",
        re.I,
    )
    sga_driver_re = re.compile(r"\b(sg&a|selling,?\s+general|general and administrative)\b", re.I)
    rd_driver_re = re.compile(r"\b(r&d|research and development)\b", re.I)
    restructuring_driver_re = re.compile(r"\b(restructuring|transformation|one-time|special item)\b", re.I)
    other_expense_driver_re = re.compile(r"\b(other expense|other income|interest expense|tax expense)\b", re.I)
    segment_re = re.compile(
        r"\b(sendtech|presort|global ecommerce|gec|shipping|mailing|e-commerce|parcel)\b",
        re.I,
    )

    BACKFILL_PRIOR_YEAR_COMPARISONS = True
    INCLUDE_GUIDANCE_SUMMARY_IN_QUARTER_NOTES = True
    SHOW_DROPPED_THEMES = not is_gpre_profile

    def _has_numeric_range_or_point(text: str) -> bool:
        for hit in glx_extract_numeric_patterns(text or ""):
            kind = str(hit.get("kind") or "").lower()
            if kind not in {"range", "point", "qualitative_range"}:
                continue
            if kind == "range" and (hit.get("value_low") is not None or hit.get("value_high") is not None):
                return True
            if kind == "point" and hit.get("value_point") is not None:
                return True
            if kind == "qualitative_range":
                return True
        return False

    def _has_numeric_range(text: str) -> bool:
        for hit in glx_extract_numeric_patterns(text or ""):
            kind = str(hit.get("kind") or "").lower()
            if kind != "range":
                continue
            if hit.get("value_low") is not None and hit.get("value_high") is not None:
                return True
        return False

    def _cost_savings_numeric_provenance(text: str) -> bool:
        txt = glx_normalize_text(text)
        low = txt.lower()
        if not re.search(r"\b(cost savings|savings|run[- ]?rate savings|annualized savings|annualised savings)\b", low, re.I):
            return False
        return _has_numeric_range(txt)

    def _estimate_amount_m(text: str) -> float:
        txt = glx_normalize_text(text).lower()
        vals: List[float] = []
        for m in re.finditer(r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(billion|bn|million|m)?", txt, re.I):
            try:
                v = float(str(m.group(1)).replace(",", ""))
            except Exception:
                continue
            unit = str(m.group(2) or "").lower()
            if unit in {"billion", "bn"}:
                v *= 1000.0
            elif unit in {"million", "m"}:
                v *= 1.0
            else:
                # Ignore tiny plain numbers likely not $m amounts.
                if v < 10:
                    continue
            vals.append(abs(v))
        return max(vals) if vals else 0.0

    def _detect_segment(text: str) -> str:
        m = segment_re.search(str(text or ""))
        if not m:
            return ""
        seg = str(m.group(1) or "").strip().lower()
        if seg in {"sendtech"}:
            return "SendTech"
        if seg in {"presort"}:
            return "Presort"
        if seg in {"global ecommerce", "gec", "e-commerce"}:
            return "Global Ecommerce"
        if seg in {"shipping", "parcel"}:
            return "Shipping / Parcel"
        if seg in {"mailing"}:
            return "Mailing"
        return str(m.group(1) or "").strip().title()

    def _driver_tag(text: str) -> str:
        low_txt = str(text or "").lower()
        if margin_driver_re.search(low_txt):
            return "margin_driver"
        if pricing_mix_volume_re.search(low_txt):
            return "pricing_mix_volume"
        if sga_driver_re.search(low_txt) and causality_re.search(low_txt):
            return "sg&a_driver"
        if rd_driver_re.search(low_txt) and causality_re.search(low_txt):
            return "r&d_driver"
        if re.search(r"\b(corporate expenses?|corporate costs?)\b", low_txt, re.I) and causality_re.search(low_txt):
            return "corp_exp_driver"
        if re.search(r"\bother expense\s*\(?income\)?\b|\bother income\s*\(?expense\)?\b", low_txt, re.I):
            return "other_expense_driver"
        if re.search(r"\brestructuring charges?\b", low_txt, re.I):
            return "restructuring_driver"
        if bank_financing_term_re.search(low_txt) and bank_financing_reason_re.search(low_txt):
            return "bank_funding_driver"
        if restructuring_driver_re.search(low_txt) and (causality_re.search(low_txt) or numeric_token_re.search(low_txt)):
            return "restructuring_driver"
        if re.search(r"\b(pension|postretirement|opeb|pension settlement)\b", low_txt, re.I):
            # Keep pension lines only when magnitude is truly material.
            if _estimate_amount_m(low_txt) >= 50.0:
                return "pension_driver"
            return ""
        if other_expense_driver_re.search(low_txt) and causality_re.search(low_txt):
            return "other_expense_driver"
        return ""

    def _theme_signature(item: Dict[str, Any]) -> Tuple[str, str, str]:
        bucket = str(item.get("bucket") or "")
        metric = str(item.get("metric_canon") or item.get("metric_tag") or "").strip().lower()
        sig = str(item.get("driver_tag") or "").strip().lower()
        txt_norm = glx_dedup_text_key(item.get("text_full") or "")
        if not sig:
            txt_norm = re.sub(r"\b20\d{2}\b", " ", txt_norm)
            txt_norm = re.sub(r"[$€£]?\s*\d[\d,]*(?:\.\d+)?%?", " ", txt_norm)
            toks = [t for t in re.findall(r"[a-z]{3,}", txt_norm) if t not in {"the", "and", "with", "from", "that", "this", "were", "was"}]
            sig = " ".join(toks[:10]).strip()
        if not sig:
            sig = txt_norm[:120]
        return (bucket, metric, sig)

    def _parse_money_amount(txt_in: str) -> Optional[float]:
        txt = str(txt_in or "")
        if not txt:
            return None
        vals: List[float] = []
        # Prefer explicit dollar-denominated amounts.
        for m in re.finditer(
            r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            txt,
            re.I,
        ):
            try:
                v = float(str(m.group(1)).replace(",", ""))
            except Exception:
                continue
            u = str(m.group(2) or "").lower()
            if u in {"billion", "bn"}:
                v *= 1e9
            elif u in {"million", "m"}:
                v *= 1e6
            elif v < 100_000:
                continue
            vals.append(float(v))
        if vals:
            return max(vals, key=lambda z: abs(float(z)))

        # Fallback when '$' is missing: keep only money-like contexts and skip share counts.
        for m in re.finditer(
            r"\b([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
            txt,
            re.I,
        ):
            tail = str(txt[m.end(): m.end() + 24]).lower()
            if re.search(r"\bshares?\b", tail, re.I):
                continue
            ctx = str(txt[max(0, m.start() - 60): min(len(txt), m.end() + 60)]).lower()
            if not re.search(r"\b(cost|spent|repurchas|buyback|dividend|paid|cash)\b", ctx, re.I):
                continue
            try:
                v = float(str(m.group(1)).replace(",", ""))
            except Exception:
                continue
            u = str(m.group(2) or "").lower()
            if u in {"billion", "bn"}:
                v *= 1e9
            else:
                v *= 1e6
            vals.append(float(v))
        if vals:
            return max(vals, key=lambda z: abs(float(z)))
        return None

    def _extract_ytd_quarter_buyback_components_early_local(
        txt_in: Any,
        qd_ref: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Split YTD buyback disclosure into in-quarter and post-quarter pieces."""
        txt = glx_normalize_text(html.unescape(str(txt_in or "")).replace("\xa0", " "))
        if not txt or not re.search(r"\b(year[- ]to[- ]date|ytd)\b", txt, re.I):
            return {}
        if not re.search(r"\bincluding\b", txt, re.I):
            return {}
        q_num = ((qd_ref.month - 1) // 3) + 1 if isinstance(qd_ref, date) else 0
        quarter_tokens = {
            1: r"(?:q1|first quarter)",
            2: r"(?:q2|second quarter)",
            3: r"(?:q3|third quarter)",
            4: r"(?:q4|fourth quarter)",
        }.get(q_num, r"(?:q[1-4]|first quarter|second quarter|third quarter|fourth quarter)")
        total_match = re.search(
            r"\brepurchas\w*\b[^.]{0,220}?"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
            r"[^.]{0,160}?\bfor\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*"
            r"(million|billion|m|bn)?\b",
            txt,
            re.I,
        )
        include_match = re.search(
            rf"\bincluding\s+([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
            rf"[^.]{{0,180}}?\bfor\s+\$?\s*([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]+(?:\.\d+)?)\s*"
            rf"(million|billion|m|bn)?\b[^.]{{0,180}}?\bin\s+(?:the\s+)?{quarter_tokens}\b",
            txt,
            re.I,
        )
        if not total_match or not include_match:
            return {}

        def _parse_shares(raw_num: Any, unit_in: Any = "") -> Optional[float]:
            try:
                val = float(str(raw_num or "").replace(",", ""))
            except Exception:
                return None
            unit_low = str(unit_in or "").strip().lower()
            if unit_low in {"million", "m"} or val < 100_000.0:
                val *= 1_000_000.0
            return float(val) if val > 0 else None

        def _parse_money(raw_num: Any, unit_in: Any = "") -> Optional[float]:
            try:
                val = float(str(raw_num or "").replace(",", ""))
            except Exception:
                return None
            unit_low = str(unit_in or "").strip().lower()
            if unit_low in {"billion", "bn"}:
                val *= 1_000_000_000.0
            elif unit_low in {"million", "m"} or val < 2_000.0:
                val *= 1_000_000.0
            return float(val) if val > 0 else None

        total_shares = _parse_shares(total_match.group(1), total_match.group(2))
        total_amount = _parse_money(total_match.group(3), total_match.group(4))
        quarter_shares = _parse_shares(include_match.group(1), include_match.group(2))
        quarter_amount = _parse_money(include_match.group(3), include_match.group(4))
        if (
            total_shares is None
            or total_amount is None
            or quarter_shares is None
            or quarter_amount is None
            or total_shares + 1.0 < quarter_shares
            or total_amount + 1.0 < quarter_amount
        ):
            return {}
        cutoff_match = re.search(
            r"\bthrough\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,\s*20\d{2})?)\b",
            txt,
            re.I,
        )
        cutoff_txt = re.sub(r"\s+", " ", str(cutoff_match.group(1) or "")).strip() if cutoff_match else ""
        if cutoff_txt and isinstance(qd_ref, date):
            cutoff_txt = re.sub(rf",\s*{int(qd_ref.year)}\s*$", "", cutoff_txt).strip()
        if not q_num:
            q_word = str(include_match.group(0) or "").lower()
            if "first quarter" in q_word or "q1" in q_word:
                q_num = 1
            elif "second quarter" in q_word or "q2" in q_word:
                q_num = 2
            elif "third quarter" in q_word or "q3" in q_word:
                q_num = 3
            elif "fourth quarter" in q_word or "q4" in q_word:
                q_num = 4
        return {
            "total_shares": float(total_shares),
            "total_amount": float(total_amount),
            "quarter_shares": float(quarter_shares),
            "quarter_amount": float(quarter_amount),
            "quarter_avg_price": float(quarter_amount) / float(quarter_shares),
            "post_shares": max(0.0, float(total_shares) - float(quarter_shares)),
            "post_amount": max(0.0, float(total_amount) - float(quarter_amount)),
            "quarter_num": q_num,
            "cutoff": cutoff_txt,
        }

    def _format_early_buyback_execution_summary_local(parts_in: Dict[str, Any]) -> str:
        try:
            shares_val = float(parts_in.get("quarter_shares") or 0.0)
            amount_val = float(parts_in.get("quarter_amount") or 0.0)
            avg_val = float(parts_in.get("quarter_avg_price") or 0.0)
        except Exception:
            return ""
        if shares_val <= 0 or amount_val <= 0:
            return ""
        q_num = int(parts_in.get("quarter_num") or 0)
        q_anchor = f" in Q{q_num}" if q_num else ""
        shares_txt = f"{shares_val / 1_000_000.0:,.1f}m shares"
        amount_txt = f"${amount_val / 1_000_000.0:,.1f}m"
        if avg_val > 0:
            return _ensure_terminal_period(
                f"Repurchased {shares_txt} for {amount_txt} with an average price of ${avg_val:.2f}/share{q_anchor}"
            )
        return _ensure_terminal_period(f"Repurchased {shares_txt} for {amount_txt}{q_anchor}")

    def _extract_executed_buyback_amount(txt_in: str) -> Optional[float]:
        txt = glx_normalize_text(txt_in)
        if not txt:
            return None
        if _is_debt_repurchase_noise_local(txt):
            return None
        ytd_split = _extract_ytd_quarter_buyback_components_early_local(txt)
        if ytd_split.get("quarter_amount") is not None:
            return float(ytd_split["quarter_amount"])
        low = txt.lower()
        # Direct executed patterns first: "...repurchased ... for $X..." etc.
        for pat in [
            r"\brepurchas\w*\b[^.]{0,160}?\bfor\b[^.$]{0,24}?\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            r"\brepurchas\w*\b[^.]{0,180}?\b(?:at\s+)?(?:a\s+)?total\s+cost\s+of\b[^.$]{0,24}?\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            r"\b(?:spent|deployed)\b[^.]{0,180}?\brepurchas\w*\b[^.$]{0,40}?\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
        ]:
            m = re.search(pat, txt, re.I)
            if not m:
                continue
            try:
                v = float(str(m.group(1)).replace(",", ""))
            except Exception:
                continue
            u = str(m.group(2) or "").lower()
            if u in {"billion", "bn"}:
                v *= 1e9
            elif u in {"million", "m"}:
                v *= 1e6
            elif v < 100_000:
                continue
            return float(v)

        # Fallback: choose $ amounts tied to repurchase execution, excluding authorization context.
        cand_vals: List[float] = []
        for m in re.finditer(
            r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            txt,
            re.I,
        ):
            try:
                v = float(str(m.group(1)).replace(",", ""))
            except Exception:
                continue
            u = str(m.group(2) or "").lower()
            if u in {"billion", "bn"}:
                v *= 1e9
            elif u in {"million", "m"}:
                v *= 1e6
            elif v < 100_000:
                continue
            ctx = low[max(0, m.start() - 140): min(len(low), m.end() + 140)]
            if not re.search(r"\b(repurchas\w*|buyback|bought back|spent|deployed|cash flow)\b", ctx, re.I):
                continue
            if re.search(r"\b(authoriz|authorization|remaining|capacity|available|increase(?:d)?\s+by)\b", ctx, re.I):
                continue
            cand_vals.append(float(v))
        if cand_vals:
            return max(cand_vals, key=lambda z: abs(float(z)))
        return None

    def _extract_buyback_cash_from_text(txt_in: str) -> Optional[float]:
        txt = glx_normalize_text(txt_in)
        if not txt:
            return None
        if _is_debt_repurchase_noise_local(txt):
            return None
        low = txt.lower()
        if not re.search(
            r"\b(repurchase|repurchased|repurchasing|buyback|bought back|common\s+stock\s+repurchase|repurchase\s+of\s+common\s+stock)\b",
            low,
            re.I,
        ):
            return None
        best: Optional[float] = None
        for sent in glx_split_sentences(txt) or [txt]:
            s_low = sent.lower()
            if not re.search(
                r"\b(repurchase|repurchased|repurchasing|buyback|bought back|common\s+stock\s+repurchase|repurchase\s+of\s+common\s+stock)\b",
                s_low,
                re.I,
            ):
                continue
            executed_hit = bool(
                re.search(
                    r"\b(repurchased|repurchasing|bought back|spent|deployed|purchased|executed|retired)\b|"
                    r"at a total cost of|repurchase of common stock|common stock repurchase",
                    s_low,
                    re.I,
                )
            )
            auth_only = bool(re.search(r"\b(remaining|authorization|authorized|available|capacity)\b", s_low, re.I))
            if auth_only and not executed_hit:
                continue
            if not executed_hit:
                continue
            amt = _extract_executed_buyback_amount(sent)
            if amt is None:
                continue
            if best is None or abs(float(amt)) > abs(float(best)):
                best = float(amt)
        return best

    def _extract_post_quarter_buyback_commentary_local(
        text_in: Any,
        qd_ref: Optional[date],
    ) -> Dict[str, Any]:
        text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
        if not text or _is_debt_repurchase_noise_local(text):
            return {}

        q_num = ((int(qd_ref.month) - 1) // 3) + 1 if isinstance(qd_ref, date) else 0
        quarter_word = {1: "first", 2: "second", 3: "third", 4: "fourth"}.get(q_num, "")

        def _has_post_quarter_anchor(chunk_in: str) -> bool:
            chunk_low = glx_normalize_text(chunk_in).lower()
            if not chunk_low:
                return False
            if re.search(
                r"\b(?:after|following|subsequent to)\s+(?:the\s+)?quarter[- ]end\b",
                chunk_low,
                re.I,
            ):
                return True
            if re.search(
                r"\b(?:after|following|subsequent to)\s+the\s+end\s+of\s+the\s+quarter\b",
                chunk_low,
                re.I,
            ):
                return True
            if quarter_word and re.search(
                rf"\bfrom\s+the\s+end\s+of\s+(?:the\s+)?{quarter_word}\s+quarter\b",
                chunk_low,
                re.I,
            ):
                return True
            return bool(
                re.search(
                    r"\bfrom\s+the\s+end\s+of\s+(?:the\s+)?(?:first|second|third|fourth)\s+quarter\b",
                    chunk_low,
                    re.I,
                )
            )

        def _format_cutoff(chunk_in: str) -> str:
            cutoff_match = re.search(
                r"\bthrough\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,\s*20\d{2})?)\b",
                chunk_in,
                re.I,
            )
            if cutoff_match:
                return re.sub(r"\s+", " ", str(cutoff_match.group(1) or "")).strip()
            return ""

        best_hit: Dict[str, Any] = {}
        best_score = -1.0
        seen_sentences: set[str] = set()
        for sent in glx_split_sentences(text) or [text]:
            chunk = glx_normalize_text(sent)
            chunk_key = chunk.lower()
            if not chunk or chunk_key in seen_sentences:
                continue
            seen_sentences.add(chunk_key)
            if not re.search(r"\b(repurchas\w*|buyback|bought\s+back)\b", chunk, re.I):
                continue
            if _has_negative_buyback_statement_for_ref_local(chunk, qd_ref):
                continue
            ytd_split_hit = _extract_ytd_buyback_including_quarter_split_local(chunk, qd_ref)
            ytd_post = dict(ytd_split_hit.get("post") or {}) if ytd_split_hit else {}
            if ytd_post:
                amount_val = pd.to_numeric(ytd_post.get("amount"), errors="coerce")
                shares_val = pd.to_numeric(ytd_post.get("shares"), errors="coerce")
                if (pd.notna(amount_val) and float(amount_val) > 0) or (pd.notna(shares_val) and float(shares_val) > 0):
                    cutoff_txt = str(ytd_post.get("cutoff") or "").strip()
                    if pd.notna(shares_val) and float(shares_val) > 0 and pd.notna(amount_val) and float(amount_val) > 0:
                        prefix = (
                            f"Additional {_fmt_note_share_count_local(float(shares_val))} repurchased "
                            f"for {_fmt_short_money_value_local(float(amount_val))}"
                        )
                    elif pd.notna(amount_val) and float(amount_val) > 0:
                        prefix = f"Additional {_fmt_short_money_value_local(float(amount_val))} repurchased"
                    else:
                        prefix = f"Additional {_fmt_note_share_count_local(float(shares_val))} repurchased"
                    anchor_txt = f" after quarter-end through {cutoff_txt}" if cutoff_txt else " after quarter-end"
                    summary_txt = _ensure_terminal_period(
                        f"{prefix}{anchor_txt}; excluded from quarter/TTM data."
                    )
                    score = 40.0 + (4.0 if cutoff_txt else 0.0) + (2.0 if pd.notna(shares_val) else 0.0)
                    if score > best_score:
                        best_score = score
                        best_hit = {"summary": summary_txt, "score": score}
                    continue
            if not _has_post_quarter_anchor(chunk):
                continue
            parts = _extract_buyback_execution_components_local(chunk, qd_ref)
            amount_val = pd.to_numeric(parts.get("amount"), errors="coerce")
            shares_val = pd.to_numeric(parts.get("shares"), errors="coerce")
            if pd.isna(amount_val):
                amt_fallback = re.search(
                    r"\badditional\s+\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b[^.]{0,120}\brepurchas\w*\b",
                    chunk,
                    re.I,
                )
                if amt_fallback:
                    try:
                        amount_val = _parse_buyback_money_local(
                            amt_fallback.group(1),
                            amt_fallback.group(2),
                        )
                    except Exception:
                        amount_val = pd.NA
            if pd.isna(amount_val) and pd.isna(shares_val):
                continue
            cutoff_txt = _format_cutoff(chunk)
            if pd.notna(shares_val) and pd.notna(amount_val):
                prefix = (
                    f"Additional {_fmt_note_share_count_local(float(shares_val))} repurchased "
                    f"for {_fmt_short_money_value_local(float(amount_val))}"
                )
            elif pd.notna(amount_val):
                prefix = f"Additional {_fmt_short_money_value_local(float(amount_val))} repurchased"
            else:
                prefix = f"Additional {_fmt_note_share_count_local(float(shares_val))} repurchased"
            anchor_txt = f" after quarter-end through {cutoff_txt}" if cutoff_txt else " after quarter-end"
            summary_txt = _ensure_terminal_period(
                f"{prefix}{anchor_txt}; excluded from quarter/TTM data."
            )
            explicit_count = float(parts.get("explicit_count") or 0.0)
            score = (explicit_count * 10.0) + (4.0 if cutoff_txt else 0.0) + (2.0 if pd.notna(shares_val) else 0.0)
            if score > best_score:
                best_score = score
                best_hit = {"summary": summary_txt, "score": score}
        return best_hit

    def _classify_distribution_signal(note_text: str, source_hint: str = "") -> str:
        blob = glx_normalize_text(" ".join([str(note_text or ""), str(source_hint or "")]))
        if not blob:
            return "other_distribution"
        low = blob.lower()
        if re.search(
            r"\b(non[- ]?controlling interests?|noncontrolling interests?|nci|"
            r"partners?'?\s+capital|partner distributions?|member distributions?)\b",
            low,
            re.I,
        ):
            return "distribution_to_nci"
        if re.search(
            r"\b(common stock dividend|common[- ]stock dividend|dividend per share|per common share|"
            r"common shareholders?|common stockholders?|stockholders of record)\b",
            low,
            re.I,
        ):
            return "common_dividend"
        if re.search(
            r"\b(cash dividends and distributions declared|payments of dividends and distributions|"
            r"dividends and distributions)\b",
            low,
            re.I,
        ):
            return "other_distribution"
        if re.search(r"\bdividend\b", low, re.I) and re.search(r"\bcommon\b", low, re.I):
            return "common_dividend"
        return "other_distribution"

    def _gpre_quantified_note_summary_local(text_in: Any, metric_hint: Any = "") -> str:
        txt_local = glx_normalize_text(str(text_in or ""))
        low = txt_local.lower()
        metric_low = glx_normalize_text(str(metric_hint or "")).lower()
        metric_pref_liquidity = bool(
            re.search(r"\b(liquidity|non-core|balance sheet|monetiz(?:e|ing)|liquidity enhancement)\b", metric_low, re.I)
        )
        basis_label = "YoY" if ("yoy" in metric_low or "yoy" in low) else ("QoQ" if ("qoq" in metric_low or "qoq" in low) else "")

        def _gpre_carbon_capture_status_summary_local(txt_norm: str, txt_low: str) -> str:
            has_all_sites = all(
                re.search(pattern, txt_low, re.I)
                for pattern in (r"\bcentral city\b", r"\bwood river\b", r"\byork\b")
            )
            fully_all_sites = bool(
                has_all_sites
                and re.search(r"\bcarbon capture\b", txt_low, re.I)
                and (
                    re.search(
                        r"\bfully operational at (?:our )?central city,?\s+wood river and york\b",
                        txt_low,
                        re.I,
                    )
                    or re.search(
                        r"\bcentral city\b[^.]{0,120}\bwood river\b[^.]{0,120}\byork\b[^.]{0,120}\bfully operational\b",
                        txt_low,
                        re.I,
                    )
                    or re.search(
                        r"\bfully operational\b[^.]{0,140}\bcentral city\b[^.]{0,80}\bwood river\b[^.]{0,80}\byork\b",
                        txt_low,
                        re.I,
                    )
                )
            )
            if fully_all_sites:
                return _ensure_terminal_period(
                    "Carbon capture was fully operational at Central City, Wood River and York, Nebraska facilities"
                )
            york_fully_operational = bool(
                re.search(
                    r"\byork\b[^.]{0,120}\bfully operational\b|\bfully operational\b[^.]{0,120}\byork\b",
                    txt_low,
                    re.I,
                )
            )
            central_city_wood_river_ramping = bool(
                re.search(r"\bcentral city\b", txt_low, re.I)
                and re.search(r"\bwood river\b", txt_low, re.I)
                and re.search(r"\b(online and ramping|online, ramping|online and continue to ramp|ramping)\b", txt_low, re.I)
            )
            if york_fully_operational and central_city_wood_river_ramping:
                return _ensure_terminal_period(
                    "York carbon capture was fully operational; Central City and Wood River were online and ramping"
                )
            return ""

        if "carbon capture status" in metric_low:
            return _gpre_carbon_capture_status_summary_local(txt_local, low)
        if "45z agreement update" in metric_low:
            tax_credit_agreement_signal = bool(
                re.search(
                    r"\b(?:45z|section\s+45z|tax credit purchase agreement|production tax credits?)\b",
                    low,
                    re.I,
                )
            )
            monetization_date_match = re.search(
                r"\b(september\s+(?:16|17),?\s+2025)\b",
                txt_local,
                re.I,
            )
            monetization_amend_match = re.search(
                r"\bamended on\s+(december\s+10,?\s+2025)\b",
                txt_local,
                re.I,
            )
            if (
                tax_credit_agreement_signal
                and re.search(r"\bagreement\b", low)
                and re.search(r"\bnebraska\b", low)
                and monetization_date_match
            ):
                exec_date = monetization_date_match.group(1)
                if monetization_amend_match:
                    amend_date = monetization_amend_match.group(1)
                    return _ensure_terminal_period(
                        f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date} and amended on {amend_date} to add credits from three additional facilities"
                    )
                return _ensure_terminal_period(
                    f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date}"
                )
            return ""
        def _parse_paren_money(raw_val: str) -> float:
            sval = str(raw_val or "").strip()
            neg = sval.startswith("(") and sval.endswith(")")
            sval = sval.strip("()").replace(",", "")
            try:
                out = float(sval)
            except Exception:
                return 0.0
            return -out if neg else out
        m_util = re.search(r"\butilization\s*(\d{2,3}%)(?:\s+across operating plants|\s+of stated capacity)?", txt_local, re.I)
        if m_util:
            suffix = " of stated capacity" if "stated capacity" in low else " across operating plants"
            return _ensure_terminal_period(f"Utilization reached {m_util.group(1)}{suffix}")
        m_operating_util = re.search(
            r"\bachieved strong utilization in the quarter from the (?:eight|nine) operating ethanol plants of (\d{2,3})%\b",
            txt_local,
            re.I,
        )
        if m_operating_util:
            return _ensure_terminal_period(f"Utilization reached {m_operating_util.group(1)}% across operating plants")
        if "marketing" in metric_low and re.search(r"\bactively marketing 2026 45z production tax credits\b", low, re.I):
            return _ensure_terminal_period("Actively marketing 2026 45Z production tax credits")
        gpre_45z_contrib_match = re.search(
            r"\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+in\s+45z\s+production\s+tax\s+credit\s+value\s+net\s+of\s+discounts\s+and\s+other\s+costs",
            txt_local,
            re.I,
        )
        if gpre_45z_contrib_match:
            period_suffix = " in Q4" if re.search(r"\b(fourth quarter|q4)\b", low, re.I) else ""
            return _ensure_terminal_period(
                f"45Z production tax credits contributed {_fmt_short_money_value_local(float(gpre_45z_contrib_match.group(1)) * 1_000_000.0)} net of discounts and other costs{period_suffix}"
            )
        gpre_45z_fy2026_range_match = re.search(
            r"(?:45z[^.]{0,220}?(?:ebitda|production tax credits?)[^.]{0,220}?\$?\s*200\s*(?:-|to|–)\s*\$?\s*225\s*(?:million|m)|"
            r"\$?\s*200\s*(?:-|to|–)\s*\$?\s*225\s*(?:million|m)[^.]{0,220}?45z[^.]{0,220}?(?:ebitda|production tax credits?))",
            txt_local,
            re.I,
        )
        if gpre_45z_fy2026_range_match:
            return _ensure_terminal_period("FY 2026 45Z EBITDA contribution guidance is $200m-$225m")
        gpre_45z_2026_outlook_match = re.search(
            r"\bat\s+least\s+\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+of\s+45z(?:-related)?\s+adjusted ebitda\s+in\s+2026\b",
            txt_local,
            re.I,
        )
        if gpre_45z_2026_outlook_match:
            return _ensure_terminal_period(
                f"FY 2026 45Z-related Adjusted EBITDA outlook is at least {_fmt_short_money_value_local(float(gpre_45z_2026_outlook_match.group(1)) * 1_000_000.0)}"
            )
        gpre_45z_opportunity_match = re.search(
            r"\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+of\s+45z\s+monetization opportunity",
            txt_local,
            re.I,
        )
        if gpre_45z_opportunity_match:
            util_match = re.search(r"\butilization(?: improved)? to\s*(\d{2,3})%\b", txt_local, re.I)
            if util_match:
                return _ensure_terminal_period(
                    f"Utilization reached {util_match.group(1)}% of stated capacity, supporting {_fmt_short_money_value_local(float(gpre_45z_opportunity_match.group(1)) * 1_000_000.0)} of 45Z monetization opportunity"
                )
            return _ensure_terminal_period(
                f"45Z monetization opportunity estimated at {_fmt_short_money_value_local(float(gpre_45z_opportunity_match.group(1)) * 1_000_000.0)}"
            )
        gpre_management_outlook_match = bool(
            re.search(
                r"\b(cost reduction initiatives?.{0,120}?ahead of plan).{0,220}?(positive ebitda).{0,120}?(market conditions)\b",
                low,
                re.I,
            )
        )
        gpre_liquidity_match = bool(
            re.search(
                r"\benhance liquidity\b.{0,220}?\bmonetiz(?:e|ing)\s+non-core assets?\b.{0,220}?\bstrengthen (?:our )?balance sheet\b",
                low,
                re.I,
            )
        )
        if metric_pref_liquidity and gpre_liquidity_match:
            return _ensure_terminal_period(
                "Management is pursuing non-core asset monetization to enhance liquidity and strengthen the balance sheet"
            )
        if gpre_management_outlook_match:
            return _ensure_terminal_period(
                "Cost reduction initiatives are progressing ahead of plan, supporting positive EBITDA outlook under current market conditions"
            )
        if gpre_liquidity_match:
            return _ensure_terminal_period(
                "Management is pursuing non-core asset monetization to enhance liquidity and strengthen the balance sheet"
            )
        gpre_45z_fy2026_range_match = re.search(
            r"(?:45z[^.]{0,220}?(?:ebitda|production tax credits?)[^.]{0,220}?\$?\s*200\s*(?:-|to|–)\s*\$?\s*225\s*(?:million|m)|"
            r"\$?\s*200\s*(?:-|to|–)\s*\$?\s*225\s*(?:million|m)[^.]{0,220}?45z[^.]{0,220}?(?:ebitda|production tax credits?))",
            txt_local,
            re.I,
        )
        if gpre_45z_fy2026_range_match:
            return _ensure_terminal_period("FY 2026 45Z EBITDA contribution guidance is $200m-$225m")
        working_cap_match = re.search(
            r"\b(?:more than|greater than)\s+\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+improvement in working capital\b",
            txt_local,
            re.I,
        )
        if working_cap_match:
            return _ensure_terminal_period(
                f"Working capital improved by more than {_fmt_short_money_value_local(float(working_cap_match.group(1)) * 1_000_000.0)}"
            )
        savings_target_match = re.search(
            r"\b(?:on pace to exceed|ahead of plan)\b[^.]{0,220}?\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+(?:in\s+)?annualized savings target\b",
            txt_local,
            re.I,
        )
        if savings_target_match:
            return _ensure_terminal_period(
                f"Cost reductions are on pace to exceed the {_fmt_short_money_value_local(float(savings_target_match.group(1)) * 1_000_000.0)} annualized savings target"
            )
        if re.search(
            r"\bactively marketing 2026 45z production tax credits\b",
            low,
            re.I,
        ):
            return _ensure_terminal_period("Actively marketing 2026 45Z production tax credits")
        crush_margin_pair = _parse_gpre_crush_margin_pair_local(txt_local)
        if crush_margin_pair:
            current_val, prior_val, _ = crush_margin_pair
            return _format_directional_from_prior_summary_local(
                "Consolidated ethanol crush margin",
                float(current_val) * 1_000_000.0,
                float(prior_val) * 1_000_000.0,
                basis_label="YoY",
                negative_parens=True,
            )
        if re.search(
            r"\b45z\b.{0,240}?\bbetter financially than originally expected\b",
            low,
            re.I,
        ):
            return _ensure_terminal_period("Nebraska 45Z opportunity could be better than originally expected")
        if re.search(
            r"\bon pace to begin sequestering\b.{0,220}?\bsecond half of 2025\b",
            low,
            re.I,
        ):
            return _ensure_terminal_period("Nebraska carbon capture remained on pace for 2H 2025 sequestration")
        if re.search(r"\bindustry oversupply\b", low, re.I) and re.search(r"\bmild winter\b", low, re.I):
            if re.search(r"\bidled during the january cold snap\b|\bmaintenance programs?\b|\bplanned maintenance\b", low, re.I):
                return _ensure_terminal_period(
                    "Margins were pressured by industry oversupply, a mild winter and plant downtime/maintenance"
                )
            return _ensure_terminal_period("Margins were pressured by industry oversupply and a mild winter")
        restructuring_cost_match = re.search(
            r"\$\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+of\s+restructuring costs\b.*?\bcost reduction initiative\b",
            txt_local,
            re.I,
        )
        if restructuring_cost_match:
            return _ensure_terminal_period(
                f"Corporate activities included {_fmt_short_money_value_local(float(restructuring_cost_match.group(1)) * 1_000_000.0)} of restructuring costs from the cost reduction initiative"
            )
        if re.search(
            r"\bdisciplined risk management strategy continues to support first quarter margins and cash flow\b",
            low,
            re.I,
        ):
            return _ensure_terminal_period(
                "Disciplined risk management strategy continues to support first quarter margins and cash flow"
            )
        if re.search(r"\brisk management\b", low) and re.search(r"\b(margins?|cash flow)\b", low):
            return _ensure_terminal_period("Risk management supports margins and cash flow")
        if re.search(r"\bobion\b", low) and re.search(r"\b(repay|repaid|repayment)\b", low):
            obion_amt_match = re.search(
                r"\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+junior mezzanine debt",
                txt_local,
                re.I,
            )
            if obion_amt_match:
                return _ensure_terminal_period(
                    f"Junior mezzanine debt of {_fmt_short_money_value_local(float(obion_amt_match.group(1)) * 1_000_000.0)} was repaid from Obion sale proceeds"
                )
            return _ensure_terminal_period("Obion sale proceeds used to fully repay junior mezzanine debt")
        generic_carbon_summary = _gpre_carbon_capture_status_summary_local(txt_local, low)
        if generic_carbon_summary:
            return generic_carbon_summary
        monetization_date_match = re.search(
            r"\b(september\s+(?:16|17),?\s+2025)\b",
            txt_local,
            re.I,
        )
        monetization_amend_match = re.search(
            r"\bamended on\s+(december\s+10,?\s+2025)\b",
            txt_local,
            re.I,
        )
        if (
            re.search(r"\b45z\b", low)
            and re.search(r"\bagreement\b", low)
            and re.search(r"\bnebraska\b", low)
            and monetization_date_match
        ):
            exec_date = monetization_date_match.group(1)
            if monetization_amend_match:
                amend_date = monetization_amend_match.group(1)
                return _ensure_terminal_period(
                    f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date} and amended on {amend_date} to add credits from three additional facilities"
                )
            return _ensure_terminal_period(
                f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date}"
            )
        if re.search(r"\bcentral city\b", low) and re.search(r"\bwood river\b", low) and re.search(r"\bonline and ramping\b", low):
            return _ensure_terminal_period("Central City and Wood River online and ramping")
        if re.search(r"\byork\b", low) and re.search(r"\bfully operational\b", low):
            return _ensure_terminal_period("York carbon capture fully operational")
        if re.search(r"\bfully online delivering biogenic co2\b", low) and re.search(r"\byork\b", low):
            return _ensure_terminal_period("York fully online delivering biogenic CO2 to the Trailblazer pipeline")
        if re.search(r"\bcarbon capture infrastructure equipment delivered\b", low) and re.search(r"\bon track for start-?up early in the fourth quarter of 2025\b", low):
            return _ensure_terminal_period("Carbon capture infrastructure delivered; Q4 2025 start-up still on track")
        if re.search(r"\bcommenced construction on compression infrastructure\b", low) and re.search(r"\bfourth quarter of 2025\b", low):
            return _ensure_terminal_period("Compression infrastructure under construction; Q4 2025 start-up still on track")
        if re.search(r"\b45z\b", low) and re.search(r"\$\s*15\s*-\s*\$\s*25\s*million", txt_local, re.I):
            return _ensure_terminal_period("Q4 2025 45Z monetization expected at $15m-$25m")
        fcf_match = re.search(
            r"\bfcf ttm at .*?:\s*\$?(-?\d+(?:\.\d+)?)m,\s*(yoy|qoq)\s*(-?\d+(?:\.\d+)?)%,\s*delta\s+\$?(-?\d+(?:\.\d+)?)m\b",
            low,
            re.I,
        )
        if fcf_match:
            current = _fmt_short_money_value_local(float(fcf_match.group(1)) * 1_000_000.0)
            basis = str(fcf_match.group(2) or "").upper()
            delta_val = float(fcf_match.group(4))
            direction_word = "improved" if delta_val >= 0 else "declined"
            move_word = "up" if delta_val >= 0 else "down"
            return _ensure_terminal_period(
                f"FCF TTM {direction_word} to {current}, {move_word} {_fmt_short_money_value_local(abs(delta_val) * 1_000_000.0)} {basis}"
            )
        fcf_delta_match = re.search(r"\bfcf ttm(?:\s+(?:yoy|qoq))?\s+delta\s+\$?(-?\d+(?:\.\d+)?)m\b", low, re.I)
        if fcf_delta_match:
            delta_val = float(fcf_delta_match.group(1))
            direction = "improved" if delta_val >= 0 else "declined"
            basis_suffix = f" {basis_label}" if basis_label else ""
            return _ensure_terminal_period(
                f"FCF TTM {direction} by {_fmt_short_money_value_local(abs(delta_val) * 1_000_000.0)}{basis_suffix}"
            )
        debt_delta_match = re.search(r"\bnet debt delta\s+\$?(-?\d+(?:\.\d+)?)m\b", low, re.I)
        if debt_delta_match:
            delta = float(debt_delta_match.group(1))
            direction = "declined" if delta < 0 else "increased"
            basis_suffix = f" {basis_label}" if basis_label else ""
            return _ensure_terminal_period(f"Net debt {direction} by {_fmt_short_money_value_local(abs(delta) * 1_000_000.0)}{basis_suffix}")
        margin_bps_match = re.search(r"\b(?:adjusted\s+)?ebitda margin delta\s+([+-]?\d+(?:\.\d+)?)\s*bps\b", low, re.I)
        if margin_bps_match:
            bps = float(margin_bps_match.group(1))
            direction = "expanded" if bps >= 0 else "compressed"
            basis_suffix = f" {basis_label}" if basis_label else ""
            return _ensure_terminal_period(f"EBITDA margin {direction} {abs(bps):.0f} bps{basis_suffix}")
        adj_ebitda_pct_match = re.search(r"\badjusted ebitda yoy\s+(-?\d+(?:\.\d+)?)%", low, re.I)
        if adj_ebitda_pct_match:
            pct = float(adj_ebitda_pct_match.group(1))
            direction = "improved" if pct >= 0 else "declined"
            return _ensure_terminal_period(f"Adjusted EBITDA {direction} {abs(pct):.1f}% YoY")
        revolver_avail_match = re.search(
            r"\brevolver availability moved to\s+\$?(-?\d+(?:\.\d+)?)m\b.*?\bdelta\s+\$?(-?\d+(?:\.\d+)?)m\b",
            low,
            re.I,
        )
        if revolver_avail_match:
            current_val = float(revolver_avail_match.group(1))
            delta_val = float(revolver_avail_match.group(2))
            prior_val = current_val - delta_val
            direction = "increased" if delta_val > 0 else "declined"
            return _ensure_terminal_period(
                f"Revolver availability {direction} from {_fmt_short_money_value_local(prior_val * 1_000_000.0)} to {_fmt_short_money_value_local(current_val * 1_000_000.0)}"
            )
        revolver_use_match = re.search(
            r"\brevolver usage moved to\s+\$?(-?\d+(?:\.\d+)?)m\b.*?\bdelta\s+\$?(-?\d+(?:\.\d+)?)m\b",
            low,
            re.I,
        )
        if revolver_use_match:
            current_val = float(revolver_use_match.group(1))
            delta_val = float(revolver_use_match.group(2))
            prior_val = current_val - delta_val
            direction = "increased" if delta_val > 0 else "declined"
            return _ensure_terminal_period(
                f"Revolver usage {direction} from {_fmt_short_money_value_local(prior_val * 1_000_000.0)} to {_fmt_short_money_value_local(current_val * 1_000_000.0)}"
            )
        revolver_available_now = re.search(
            r"\b(?:had|have)\s+\$?\s*([\d,]+(?:\.\d+)?)\s*million\s+available under (?:our|the)\s+(?:committed\s+)?revolving credit",
            txt_local,
            re.I,
        )
        if revolver_available_now:
            return _ensure_terminal_period(
                f"Revolver availability ended the quarter at {_fmt_short_money_value_local(float(revolver_available_now.group(1)) * 1_000_000.0)}"
            )
        working_cap_revolver_match = re.search(
            r"\$?\s*([\d,]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+(?:in|of)\s+working capital revolver availability\b",
            txt_local,
            re.I,
        )
        if working_cap_revolver_match:
            avail_amt = _coerce_amount_with_unit_local(working_cap_revolver_match.group(1), working_cap_revolver_match.group(2))
            if avail_amt is not None:
                return _ensure_terminal_period(
                    f"Revolver availability ended the quarter at {_fmt_short_money_value_local(float(avail_amt))}"
                )
        capex_guidance_match = re.search(
            r"\bfor\s+2026,\s+we\s+expect\s+sustaining capital expenditures\b[^.]{0,220}?\bto total\s+\$?\s*([\d,]+(?:\.\d+)?)\s*(million|billion|m|bn)\s*-\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
            txt_local,
            re.I,
        )
        if capex_guidance_match:
            lo_amt = _coerce_amount_with_unit_local(capex_guidance_match.group(1), capex_guidance_match.group(2))
            hi_amt = _coerce_amount_with_unit_local(capex_guidance_match.group(3), capex_guidance_match.group(4))
            if lo_amt is not None and hi_amt is not None:
                lo_disp = _fmt_short_money_value_local(min(float(lo_amt), float(hi_amt)))
                hi_disp = _fmt_short_money_value_local(max(float(lo_amt), float(hi_amt)))
                return _ensure_terminal_period(f"FY 2026 sustaining capex guidance is {lo_disp}-{hi_disp}")
        return ""

    def _gpre_structured_support_source_ok_local(
        source_type_in: Any,
        *,
        summary_override: str = "",
        model_metric_source: bool = False,
        capital_structure_signal: bool = False,
        cashflow_or_margin_signal: bool = False,
    ) -> bool:
        src_type_low = str(source_type_in or "").strip().lower()
        support_source = bool(
            model_metric_source
            or "filing" in src_type_low
            or src_type_low in {"debt", "revolver", "non_gaap", "adjusted_metrics", "history_q"}
        )
        if support_source and (capital_structure_signal or cashflow_or_margin_signal):
            return True
        if summary_override:
            summary_quality = _note_summary_quality_key_local(summary_override)
            if summary_quality[0] and (summary_quality[1] or summary_quality[3] or summary_quality[4]):
                return True
        return False

    def _gpre_seed_rescue_rows() -> List[Dict[str, Any]]:
        if not is_gpre_profile:
            return []
        rescue_rows: List[Dict[str, Any]] = []
        preferred_source_terms = ("earnings_release", "press_release", "presentation", "slides", "transcript", "ceo")
        high_signal_re = re.compile(
            r"\b(utilization|risk management|45z|monetization|obion|york|central city|wood river|online and ramping|fully operational|adjusted ebitda|ebitda|free cash flow|fcf|net debt|revolver|credit facility|refinanc|convertible|convert|crush margin|margin)\b",
            re.I,
        )
        fragment_drop_re = re.compile(
            r"^\s*\[(?:dropped|new|repeat)\]\s*|"
            r"\b(map|maps|permit list|county map|table of contents|legend|project map|site map)\b|"
            r"\b(latitude|longitude|parcel|township|range|section)\b",
            re.I,
        )
        for _, raw in df.iterrows():
            q_raw = raw.get("_quarter")
            if not isinstance(q_raw, date):
                q_ts = pd.to_datetime(raw.get(q_col), errors="coerce")
                q_raw = pd.Timestamp(q_ts).to_period("Q").end_time.date() if pd.notna(q_ts) else None
            if q_raw not in quarters:
                continue
            texts = _candidate_texts(raw)
            txt = glx_normalize_text(texts[0] if texts else "")
            detail_blob = _evidence_snippet_blob_local(raw) or txt
            if not txt or not high_signal_re.search(txt):
                continue
            if fragment_drop_re.search(txt) or is_tabular_fragment(txt):
                continue
            if _text_fragment_penalty(txt) >= 2 and len([s for s in re.split(r"[.!?;]+", txt) if s.strip()]) <= 1:
                continue
            src = _source_meta(raw)
            src_type_low = str(src.get("source_type") or "").lower()
            model_metric_source = src_type_low == "model_metric" or str(raw.get("doc") or "").strip().lower() == "history_q"
            capital_structure_signal = bool(re.search(r"\b(revolver|credit facility|debt|repay|repayment|refinanc|convertible|convert)\b", txt, re.I))
            cashflow_or_margin_signal = bool(re.search(r"\b(fcf|free cash flow|adjusted ebitda|ebitda|margin|crush margin)\b", txt, re.I))
            metric_hint = str(raw.get(metric_col) or raw.get("metric_ref") or raw.get("topic") or "").strip()
            summary_override = _gpre_quantified_note_summary_local(detail_blob or txt, metric_hint=metric_hint)
            allow_non_narrative = _gpre_structured_support_source_ok_local(
                src_type_low,
                summary_override=summary_override,
                model_metric_source=model_metric_source,
                capital_structure_signal=capital_structure_signal,
                cashflow_or_margin_signal=cashflow_or_margin_signal,
            )
            if not any(term in src_type_low for term in preferred_source_terms) and not allow_non_narrative:
                continue
            sev_score = pd.to_numeric(raw.get(sev_score_col), errors="coerce") if sev_score_col else pd.NA
            score_f = float(sev_score) if pd.notna(sev_score) else 0.0
            score_f += 12.0
            if capital_structure_signal:
                score_f += 5.0
            if cashflow_or_margin_signal:
                score_f += 4.0
            if model_metric_source:
                score_f += 3.0
            if summary_override and re.search(r"(\$|\d|bps|from .* to|yoy|qoq)", summary_override, re.I):
                score_f += 4.0
            bucket_name = "Results / drivers"
            if re.search(r"\b(45z|tax credit|fully operational|online|ramping|utilization|qualification)\b", txt, re.I):
                bucket_name = "Programs / initiatives"
            elif capital_structure_signal:
                bucket_name = "Debt / liquidity / covenants"
            elif re.search(r"\b(fcf|free cash flow)\b", txt, re.I):
                bucket_name = "Cash flow / FCF / capex"
            rescue_rows.append(
                {
                    "quarter": q_raw,
                    "bucket": bucket_name,
                    "text_full": txt,
                    "comment_full_text": detail_blob or txt,
                    "score": score_f,
                    "candidate_type": "gpre_rescue_note",
                    "has_causality": bool(driver_language_re.search(txt) or change_re.search(txt)),
                    "driver_noun_count": 0,
                    "driver_tag": "",
                    "metric_tag": metric_hint or "Note",
                    "metric_canon": metric_hint or "Note",
                    "segment": "",
                    "has_numeric_range_or_point": bool(_has_numeric_range_or_point(txt)),
                    "mention_kind": "numeric" if _has_numeric_range_or_point(txt) else "text",
                    "evidence_quote": txt,
                    "evidence_has_range": bool(_has_numeric_range(txt)),
                    "numeric_provenance_key": "",
                    "doc_priority": 1,
                    "period_key": "",
                    "period_label": "",
                    "source": src,
                    "severity": "info",
                    "sev_score": sev_score,
                    "metric_value": raw.get(metric_val_col) if metric_val_col else pd.NA,
                    "note_id": str(raw.get(note_id_col) or hashlib.sha1(f"{q_raw}|gpre_rescue|{txt}".encode("utf-8")).hexdigest()[:12]),
                    "as_of_quarter_end": str(q_raw),
                    "source_doc_end": str(src.get("source_doc_end") or q_raw),
                    "source_filed_date": pd.to_datetime(src.get("filed"), errors="coerce"),
                    "first_seen_quarter_end": str(q_raw),
                    "last_seen_quarter_end": str(q_raw),
                    "last_seen_numeric_quarter_end": str(q_raw) if _has_numeric_range_or_point(txt) else "",
                    "last_seen_text_quarter_end": str(q_raw) if not _has_numeric_range_or_point(txt) else "",
                    "referenced_years": sorted({int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", txt)}),
                    "has_forward_intent": bool(re.search(r"\b(expect|expects|target|targets|plan|plans|outlook|will)\b", txt, re.I)),
                    "has_period_anchor": bool(re.search(r"\b(fy|full[- ]?year|quarter|q[1-4]|202\d)\b", txt, re.I)),
                    "target_period_norm": "",
                    "guidance_type": "text",
                    "anchor_year": None,
                    "year_pair": None,
                    "is_year_comparison": False,
                    "tense_hint": "unknown",
                    "backfill_label": "",
                    "source_quarter_end": str(q_raw),
                    "theme_key": f"gpre_rescue|{glx_dedup_text_key(txt)[:120]}",
                    "change_badge": "NEW",
                    "_render_summary": summary_override or txt,
                }
            )
        return rescue_rows

    def _quarter_notes_source_rescue_support_local() -> QuarterNotesUiSourceRescueSupport:
        cached_support = __state.get("quarter_notes_source_rescue_support_cache")
        if cached_support is not None:
            return cached_support
        cached_support = QuarterNotesUiSourceRescueSupport(
            QuarterNotesUiSourceRescueDeps(
                _candidate_texts=_candidate_texts,
                _capital_allocation_split_summaries_local=_capital_allocation_split_summaries_local,
                _classify_pbi_metric_label=_classify_pbi_metric_label,
                _ensure_terminal_period=_ensure_terminal_period,
                _evidence_snippet_blob_local=_evidence_snippet_blob_local,
                _explicit_event_quarter_override_local=_explicit_event_quarter_override_local,
                _extract_pbi_target_display=_extract_pbi_target_display,
                _fmt_note_share_count_local=_fmt_note_share_count_local,
                _fmt_short_money_value_local=_fmt_short_money_value_local,
                _gpre_quantified_note_summary_local=_gpre_quantified_note_summary_local,
                _gpre_structured_support_source_ok_local=_gpre_structured_support_source_ok_local,
                _infer_doc_quarter_local=_infer_doc_quarter_local,
                _is_pbi_clean_sentence=_is_pbi_clean_sentence,
                _is_preferred_narrative_source=_is_preferred_narrative_source,
                _iter_quarter_scoped_material_texts_local=_iter_quarter_scoped_material_texts_local,
                _iter_quarter_scoped_sec_cache_texts_local=_iter_quarter_scoped_sec_cache_texts_local,
                _management_text_windows_local=_management_text_windows_local,
                _narrative_text_matches_current_company_local=_narrative_text_matches_current_company_local,
                _note_sector_pack_keys_local=_note_sector_pack_keys_local,
                _parse_buyback_money_local=_parse_buyback_money_local,
                _path_belongs_to_ticker=_path_belongs_to_ticker,
                _pattern_match_windows_local=_pattern_match_windows_local,
                _pbi_contextual_note_summary_local=_pbi_contextual_note_summary_local,
                _pbi_detail_preserving_note_summary_local=_pbi_detail_preserving_note_summary_local,
                _pbi_explicit_note_split_variants_local=_pbi_explicit_note_split_variants_local,
                _pbi_extra_note_labels_local=_pbi_extra_note_labels_local,
                _pbi_guidance_self_contained_summary=_pbi_guidance_self_contained_summary,
                _pbi_is_locked_capital_allocation_summary_local=_pbi_is_locked_capital_allocation_summary_local,
                _pbi_note_detail_score_local=_pbi_note_detail_score_local,
                _pbi_target_display_ok=_pbi_target_display_ok,
                _profile_sector_pack_keys_local=_profile_sector_pack_keys_local,
                _promises_view=_promises_view,
                _quarter_end_for_month_local=_quarter_end_for_month_local,
                _record_writer_elapsed=_record_writer_elapsed,
                _resolve_col=_resolve_col,
                _sec_cache_html_paths_local=_sec_cache_html_paths_local,
                _source_meta=_source_meta,
                cache_dir=cache_dir,
                cache_roots=cache_roots,
                company_profile=company_profile,
                ctx_ref=_current_ctx_ref(),
                data_root_from_sec_cache_path=data_root_from_sec_cache_path,
                df=df,
                glx_normalize_text=glx_normalize_text,
                is_gpre_profile=is_gpre_profile,
                is_pbi_profile=is_pbi_profile,
                is_tabular_fragment=is_tabular_fragment,
                material_roots=material_roots,
                metric_col=metric_col,
                note_id_col=note_id_col,
                profile_ticker=profile_ticker,
                q_col=q_col,
                qn_compact_snippet=qn_compact_snippet,
                quarter_notes=quarter_notes,
                quarters=quarters,
                sev_score_col=sev_score_col,
                shared_classify_statement_evidence_role=shared_classify_statement_evidence_role,
                shared_renderable_note_drop_reason=shared_renderable_note_drop_reason,
                ticker=ticker,
                ticker_cache_roots_from_base_dir=ticker_cache_roots_from_base_dir,
                ticker_roots=ticker_roots,
            )
        )
        __state["quarter_notes_source_rescue_support_cache"] = cached_support
        return cached_support

    def _gpre_raw_note_rescue_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().gpre_raw_note_rescue_rows()

    def _gpre_source_note_rescue_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().gpre_source_note_rescue_rows()

    def _profile_milestone_source_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().profile_milestone_source_rows()

    def _pbi_seed_rescue_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().pbi_seed_rescue_rows()

    def _generic_source_note_rescue_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().generic_source_note_rescue_rows()

    def _pbi_promise_note_rescue_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().pbi_promise_note_rescue_rows()

    def _pbi_source_note_rescue_rows() -> List[Dict[str, Any]]:
        return _quarter_notes_source_rescue_support_local().pbi_source_note_rescue_rows()




    def _evidence_snippet_blob_local(rec: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in ("evidence_snippet", "text_snippet", "comment_full_text", "text_full", "statement", "promise_text"):
            txt = glx_normalize_text(str(rec.get(key) or ""))
            if txt:
                parts.append(txt)
        for key in ("evidence_json", "source_evidence_json"):
            raw_json = str(rec.get(key) or "").strip()
            if not raw_json:
                continue
            try:
                parsed = json.loads(raw_json)
            except Exception:
                continue
            payloads = parsed if isinstance(parsed, list) else [parsed]
            for payload in payloads:
                if not isinstance(payload, dict):
                    continue
                snippet = glx_normalize_text(
                    str(
                        payload.get("snippet")
                        or payload.get("evidence_snippet")
                        or payload.get("claim")
                        or payload.get("statement")
                        or ""
                    )
                )
                if snippet:
                    parts.append(snippet)
        ordered: List[str] = []
        seen: set[str] = set()
        for txt in parts:
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(txt)
        return " | ".join(ordered)

    def _pbi_note_detail_score_local(text_in: Any) -> int:
        def _build(txt: str) -> int:
            if not txt:
                return 0
            score = 0
            if re.search(r"\$\s*\d", txt):
                score += 4
            if re.search(r"\b\d+(?:\.\d+)?\s*(?:m|mm|million|b|bn|bps|x|shares?)\b", txt, re.I):
                score += 3
            if re.search(r"\b\d+(?:\.\d+)?%", txt, re.I):
                score += 2
            if re.search(r"\bfrom\b.+\bto\b", txt, re.I):
                score += 4
            if re.search(r"\b(delta|up|down)\s+\$?\s*\d", txt, re.I):
                score += 2
            if re.search(r"\b(yoy|qoq|q[1-4]|fy\s*20\d{2}|20\d{2})\b", txt, re.I):
                score += 2
            if re.search(r"\b(increased|reduced|declined|expanded|remaining capacity|authorization)\b", txt, re.I):
                score += 2
            return score
        return int(_text_cached_runtime_value_local("pbi_note_detail_score", text_in, _build))

    def _note_has_weak_generic_verb_local(text_in: Any) -> bool:
        def _build(txt: str) -> bool:
            if not txt:
                return False
            weak = bool(
                re.search(
                    r"\b(improved|changed|accelerated|declined|updated|continued|tracking|supports|advanced|progressed|notable)\b",
                    txt,
                    re.I,
                )
            )
            if not weak:
                return False
            return not bool(
                re.search(r"(\$|\d|bps|from .* to|yoy|qoq|quarter|fy\s*20\d{2})", txt, re.I)
            )
        return bool(_text_cached_runtime_value_local("weak_generic_verb", text_in, _build))

    def _note_summary_quality_key_local(text_in: Any) -> Tuple[int, int, int, int, int, int]:
        def _build(txt: str) -> Tuple[int, int, int, int, int, int]:
            if not txt:
                return (0, 0, 0, 0, 0, 0)
            quantified = 1 if re.search(r"(\$|\d|bps|from .* to|yoy|qoq)", txt, re.I) else 0
            self_contained = 1 if re.search(
                r"\b(fy\s*20\d{2}|q[1-4]\s*20\d{2}|revenue guidance|adjusted ebit guidance|eps guidance|fcf|free cash flow|"
                r"revolver|availability|debt|shares?|authorization|capacity|margin|operating expenses|utilization|45z|"
                r"ebitda|junior mezzanine|restructuring|severance|cost reduction initiative|dividend|leverage|"
                r"strategic review|phase 2|working capital|balance sheet|non-core asset|market conditions|forecast|"
                r"uncertainty|liquidity|tax credits?)\b",
                txt,
                re.I,
            ) else 0
            generic_penalty = 1 if _note_has_weak_generic_verb_local(txt) else 0
            from_to = 1 if re.search(r"\bfrom\b.+\bto\b", txt, re.I) else 0
            causal = 1 if re.search(r"\b(driven by|reflecting|due to|primarily from|stemming from|result of|ahead of plan)\b", txt, re.I) else 0
            basis = 1 if re.search(r"\b(yoy|qoq|q[1-4]|fy\s*20\d{2})\b", txt, re.I) else 0
            return (self_contained, quantified, from_to, causal, basis, -generic_penalty)
        return tuple(_text_cached_runtime_value_local("note_summary_quality", text_in, _build))

    def _prefer_note_summary_local(current_summary: Any, candidate_summary: Any) -> bool:
        cur = glx_normalize_text(str(current_summary or ""))
        cand = glx_normalize_text(str(candidate_summary or ""))
        if not cand:
            return False
        if not cur:
            return True
        cur_key = _note_summary_quality_key_local(cur) + (_pbi_note_detail_score_local(cur), len(cur))
        cand_key = _note_summary_quality_key_local(cand) + (_pbi_note_detail_score_local(cand), len(cand))
        return cand_key > cur_key

    def _pbi_extra_note_labels_local(metric_hint: str, txt: str, primary_label: str) -> List[str]:
        labels: List[str] = []
        blob = " | ".join([metric_hint, txt, primary_label]).lower()
        if primary_label:
            labels.append(primary_label)
        if re.search(r"\b(repurchas\w*|buyback|share repurchase|authorization|remaining capacity|dividend)\b", blob, re.I):
            labels.append("Capital allocation / buyback")
        if re.search(
            r"\b(reduced principal debt|reducing principal debt|principal debt reduction|deleverag|repaid|repayment|revolver|credit agreement|remaining capacity|sub-?\s*3(?:\.0)?x leverage|leverage)\b",
            blob,
            re.I,
        ):
            labels.append("Deleveraging / liquidity")
        if re.search(r"\b(pb bank|bank-held leases|cash optimization|cash release|trapped capital|receivables purchase)\b", blob, re.I):
            labels.append("PB Bank liquidity release")
        if re.search(r"\b(margin expanded|gross margin expanded|operating expenses declined|opex declined|sendtech|presort)\b", blob, re.I):
            labels.append("Adjusted EBIT / margin")
        if re.search(r"\b(adjusted ebitda|adjusted ebit|ebitda|ebit)\b", blob, re.I) and re.search(
            r"\b(improv(?:ed|ement)?|declin(?:ed|e)|compressed|expanded|moved materially|up|down|higher|lower|margin)\b",
            blob,
            re.I,
        ):
            labels.append("Adjusted EBIT / margin")
        if re.search(r"\b(fcf|free cash flow)\b", blob, re.I):
            labels.append("FCF improvement")
        if (
            re.search(r"\bstrategic review\b", blob, re.I)
            and re.search(r"\b(?:phase 2|second phase)\b", blob, re.I)
            and re.search(r"\binitiated\b", blob, re.I)
        ):
            labels.append("Strategic milestone")
        out: List[str] = []
        seen: set[str] = set()
        for label in labels:
            if label and label not in seen:
                seen.add(label)
                out.append(label)
        return out


    def _pbi_detail_preserving_note_summary_local(
        label: str,
        txt: str,
        qd_ref: Optional[date] = None,
    ) -> str:
        text = glx_normalize_text(str(txt or ""))
        if not text:
            return ""
        low = text.lower()

        def _finish(summary_in: str) -> str:
            return _ensure_terminal_period(summary_in)

        def _fmt_short_millions_str(raw_val: str) -> str:
            try:
                return _fmt_short_money_value_local(float(raw_val) * 1_000_000.0)
            except Exception:
                return f"${raw_val}m"

        def _fmt_share_count(raw_val: str) -> str:
            try:
                share_val = float(raw_val)
            except Exception:
                return f"{raw_val}m shares"
            if abs(share_val - round(share_val)) < 1e-9:
                return f"{int(round(share_val))}m shares"
            return f"{share_val:.1f}m shares"

        def _clean_driver_phrase(raw_driver: str) -> str:
            cleaned = glx_normalize_text(str(raw_driver or ""))
            cleaned = re.sub(r"\bother\s+", "", cleaned, flags=re.I)
            cleaned = re.sub(r"\binitiatives?\b", "", cleaned, flags=re.I)
            cleaned = re.sub(r"\bactions?\b", "", cleaned, flags=re.I)
            cleaned = re.sub(r"\s+,", ",", cleaned)
            cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" ,;.")
            return cleaned

        if label == "PB Bank liquidity release":
            amt_match = re.search(r"(?:>=|at least)\s*\$?\s*(\d+(?:\.\d+)?)\s*m", low, re.I)
            if amt_match and re.search(r"\bcash optimization\b", low, re.I):
                amount_txt = f"${float(amt_match.group(1)):,.0f}m"
                return _finish(f"PB Bank cash optimization target is at least {amount_txt}")

        if label == "Capital allocation / buyback":
            buyback_execution_summary = _compose_buyback_execution_summary_local(text, qd_ref)
            share_match = re.search(
                r"\brepurchas\w*\s+(\d+(?:\.\d+)?)\s+million\s+shares.*?\$\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            amount_only_share_match = re.search(
                r"\brepurchas\w*\s+\$?\s*(\d+(?:\.\d+)?)\s*million\s+in\s+shares?\b[^.]{0,180}?\b(?:during|in)\s+the\s+(first|second|third|fourth)\s+quarter\b",
                text,
                re.I,
            )
            debt_match = re.search(
                r"\b(?:reduc(?:ed|ing)\s+principal\s+debt|principal\s+debt\s+reduction)\b(?:\s+by)?\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            auth_match = re.search(
                r"\b(?:repurchase|share repurchase|buyback)\s+authorization\s+(?:increased|expanded|raised)\s+by\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if not auth_match:
                auth_match = re.search(
                    r"\b(?:increased|increasing|raised|raising|updated)\b[^.]{0,60}?\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\b[^.]{0,40}?\bby\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                    text,
                    re.I,
                )
            auth_to_match = re.search(
                r"\b(?:increasing|increased|raising|raised|updated)\b[^.]{0,60}?\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\b[^.]{0,40}?\bto\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if not auth_to_match:
                auth_to_match = re.search(
                    r"\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\s+(?:to|at)\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                    text,
                    re.I,
                )
            capacity_match = re.search(
                r"\$?\s*(\d+(?:\.\d+)?)\s*million\s+(?:in\s+)?capacity\s+remaining\s+under\s+the\s+authorization|"
                r"\$?\s*(\d+(?:\.\d+)?)\s*million\s+(?:of\s+)?remaining\s+capacity|"
                r"\bremaining\s+capacity\s+(?:of\s+)?\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if not capacity_match:
                capacity_match = re.search(
                    r"\b(?:repurchase|buyback)\s+(?:program\s+)?(?:capacity|authorization)\s+(?:remaining|left)\s+(?:at|of)?\s*\$?\s*(\d+(?:\.\d+)?)\s*million",
                    text,
                    re.I,
                )
            parts: List[str] = []
            if buyback_execution_summary:
                parts.append(re.sub(r"\.\s*$", "", buyback_execution_summary))
            elif share_match and not re.search(r"\b(year[- ]to[- ]date|ytd)\b", text, re.I):
                sent_start = max(
                    text.rfind(".", 0, share_match.start()),
                    text.rfind("!", 0, share_match.start()),
                    text.rfind("?", 0, share_match.start()),
                )
                sent_end_candidates = [
                    pos for pos in (
                        text.find(".", share_match.end()),
                        text.find("!", share_match.end()),
                        text.find("?", share_match.end()),
                    ) if pos != -1
                ]
                sent_end = min(sent_end_candidates) if sent_end_candidates else len(text)
                share_context = text[(sent_start + 1) if sent_start >= 0 else 0:sent_end].strip()
                quarter_anchor = ""
                if re.search(r"\b(q4|fourth quarter)\b", share_context, re.I):
                    quarter_anchor = " in Q4"
                elif re.search(r"\b(q3|third quarter)\b", share_context, re.I):
                    quarter_anchor = " in Q3"
                elif re.search(r"\b(q2|second quarter)\b", share_context, re.I):
                    quarter_anchor = " in Q2"
                elif re.search(r"\b(q1|first quarter)\b", share_context, re.I):
                    quarter_anchor = " in Q1"
                elif re.search(r"\bsince\s+starting(?:\s+the\s+program)?(?:[^.]{0,40}?earlier\s+this\s+year)?\b", share_context, re.I):
                    quarter_anchor = " since starting the program earlier this year"
                parts.append(
                    f"Repurchased {_fmt_share_count(share_match.group(1))} for {_fmt_short_millions_str(share_match.group(2))}{quarter_anchor}"
                )
            elif amount_only_share_match:
                quarter_anchor = {
                    "first": " in Q1",
                    "second": " in Q2",
                    "third": " in Q3",
                    "fourth": " in Q4",
                }.get(str(amount_only_share_match.group(2) or "").strip().lower(), "")
                parts.append(
                    f"Repurchased {_fmt_short_millions_str(amount_only_share_match.group(1))} of shares{quarter_anchor}"
                )
            div_from_to_match = re.search(
                r"\b(?:we\s+)?(?:increased|raising|raised)\s+(?:our\s+)?quarterly\s+dividend\s+from\s+\$?\s*(\d+(?:\.\d+)?)\s+to\s+\$?\s*(\d+(?:\.\d+)?)\s+per\s+share",
                text,
                re.I,
            )
            if not div_from_to_match:
                div_from_to_match = re.search(
                    r"\bquarterly\s+dividend\s+(?:of|to)\s+\$?\s*(\d+(?:\.\d+)?)\s+per\s+share\b",
                    text,
                    re.I,
                )
            if debt_match:
                parts.append(f"reduced principal debt by {_fmt_short_millions_str(debt_match.group(1))}")
            if auth_match:
                parts.append(f"Repurchase authorization increased by {_fmt_short_millions_str(auth_match.group(1))}")
            elif auth_to_match:
                parts.append(f"Repurchase authorization increased to {_fmt_short_millions_str(auth_to_match.group(1))}")
            capacity_val = next((grp for grp in capacity_match.groups() if grp), "") if capacity_match else ""
            if capacity_val:
                parts.append(f"{_fmt_short_millions_str(capacity_val)} remaining capacity")
            if div_from_to_match:
                if div_from_to_match.lastindex and div_from_to_match.lastindex >= 2:
                    note = capital_return_build_dividend_note(
                        current_per_share=float(div_from_to_match.group(2)),
                        previous_per_share=float(div_from_to_match.group(1)),
                    )
                else:
                    note = capital_return_build_dividend_note(
                        current_per_share=float(div_from_to_match.group(1)),
                    )
                if note:
                    parts.append(note.rstrip("."))
            if parts:
                return _finish("; ".join(parts[:3]))

        if label in {"Deleveraging / liquidity", "Debt reduction"}:
            from_to = re.search(
                r"\b(?:revolving credit facility|revolver(?: availability)?)\b.*?\b(?:increased|expanded)\s+from\s+\$?\s*(\d+(?:\.\d+)?)\s*million\s+to\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if from_to:
                return _finish(
                    f"Revolver availability increased from {_fmt_short_millions_str(from_to.group(1))} "
                    f"to {_fmt_short_millions_str(from_to.group(2))}"
                )
            embedded_from_to = re.search(
                r"\bprovided\s+(?:a\s+)?\$?\s*(\d+(?:\.\d+)?)\s*million\s+revolving credit facility\b.*?\bincreased to\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if embedded_from_to:
                return _finish(
                    f"Revolver availability increased from {_fmt_short_millions_str(embedded_from_to.group(1))} "
                    f"to {_fmt_short_millions_str(embedded_from_to.group(2))}"
                )
            debt_match = re.search(
                r"\b(?:reduc(?:ed|ing)\s+principal\s+debt|principal\s+debt\s+reduction)\b(?:\s+by)?\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if debt_match:
                quarter_anchor = ""
                if re.search(r"\b(q4|fourth quarter)\b", text, re.I):
                    quarter_anchor = " in Q4"
                elif re.search(r"\b(q3|third quarter)\b", text, re.I):
                    quarter_anchor = " in Q3"
                elif re.search(r"\b(q2|second quarter)\b", text, re.I):
                    quarter_anchor = " in Q2"
                elif re.search(r"\b(q1|first quarter)\b", text, re.I):
                    quarter_anchor = " in Q1"
                return _finish(f"Reduced principal debt by {_fmt_short_millions_str(debt_match.group(1))}{quarter_anchor}")
            leverage_match = re.search(r"\bsub-?\s*(\d+(?:\.\d+)?)x\s+leverage(?: ratio)?\b", low, re.I)
            if leverage_match:
                if re.search(r"\b(greater flexibility under (?:our )?covenants|flexibility under (?:our )?covenants)\b", low, re.I):
                    return _finish(f"Reached sub-{leverage_match.group(1)}x leverage, improving covenant flexibility")
                return _finish(f"Reached sub-{leverage_match.group(1)}x leverage")
            moved_to_delta = re.search(
                r"\brevolver availability moved to\s+\$?\s*(-?\d+(?:\.\d+)?)m\b.*?\bdelta\s+\$?\s*(-?\d+(?:\.\d+)?)m\b",
                low,
                re.I,
            )
            if moved_to_delta:
                current_val = float(moved_to_delta.group(1))
                delta_val = float(moved_to_delta.group(2))
                prior_val = current_val - delta_val
                direction = "increased" if delta_val > 0 else "declined"
                return _finish(
                    f"Revolver availability {direction} from "
                    f"{_fmt_short_money_value_local(prior_val * 1_000_000.0)} to "
                    f"{_fmt_short_money_value_local(current_val * 1_000_000.0)}"
                )

        if label == "Strategic milestone" and (
            re.search(r"\bstrategic review\b", text, re.I)
            and re.search(r"\b(?:phase 2|second phase)\b", text, re.I)
            and re.search(r"\binitiated\b", text, re.I)
        ):
            quarter_label = (
                f"Q{((qd_ref.month - 1) // 3) + 1} {qd_ref.year}"
                if isinstance(qd_ref, date)
                else "the reported quarter"
            )
            return _finish(f"Strategic review phase 2 was initiated in {quarter_label}")
        if label == "Strategic milestone" and re.search(
            r"\bstrategic review\b[^.]{0,240}?\b(?:phase 2|second phase)\b[^.]{0,240}?\b(?:by the end of the second quarter|end of q2(?:\s+2026)?|q2\s+2026)\b",
            text,
            re.I,
        ):
            return _finish("Strategic review phase 2 remains on track by end of Q2 2026")
        if label == "Strategic milestone" and re.search(
            r"\bstrategic review\b[^.]{0,260}?\binitial phase\b[^.]{0,260}?\b(?:internal improvements|operational and personnel enhancements)\b",
            text,
            re.I,
        ):
            return _finish("Strategic review initial phase yielded operational and personnel enhancements")
        if label == "Management framing / strategy" and re.search(
            r"\b(?:wider ranges?|disclosing wider ranges?|forecast(?:ing)?|uncertainty)\b[^.]{0,260}?\b(?:wider ranges?|uncertainty|forecast(?:ing)?)\b",
            text,
            re.I,
        ):
            return _finish("Guidance ranges widened due to market uncertainty and forecasting changes")
        if label == "Strategic milestone":
            phase_two_match = re.search(
                r"\bstrategic review(?:['’]s)? second phase\b[^.]{0,220}?\b(?:by the end of the second quarter|end of q2(?:\s+2026)?|q2\s+2026)\b",
                text,
                re.I,
            )
            if phase_two_match:
                return _finish("Strategic review phase 2 remains on track by end of Q2 2026")

        if label == "Management framing / strategy":
            if re.search(
                r"\b(wider ranges?|disclosing wider ranges?)\b[^.]{0,220}?\b(uncertainty|forecast(?:ing)?)\b",
                text,
                re.I,
            ):
                return _finish("Guidance ranges widened due to market uncertainty and forecasting changes")
            if re.search(
                r"\b(results reflect|focused on)\b[^.]{0,220}?\b(cost management|operational execution)\b",
                text,
                re.I,
            ):
                return _finish("Results reflect disciplined cost management and improved operational execution")

        if label in {"Adjusted EBIT / margin", "SendTech / Presort operating driver"}:
            if re.search(
                r"\bpresort services achieved record revenue and ebit\b",
                low,
                re.I,
            ) and re.search(r"\bsendtech\b", low, re.I):
                return _finish("Presort delivered record revenue and EBIT, while SendTech again improved profit and margins")
            if re.search(
                r"\badjusted ebit grew by more than \$?\s*23(?:\.0+)?\s*million\b",
                low,
                re.I,
            ) and re.search(r"\b8%\s+decline in operating expenses\b", low, re.I):
                return _finish("Adjusted EBIT improved by more than $23m on relatively flat revenue, supported by segment performance and an 8% opex decline")
            adj_ebitda_pct_match = re.search(r"\badjusted ebitda yoy\s+(-?\d+(?:\.\d+)?)%", low, re.I)
            pct_from_to_driver_match = re.search(
                r"\bgross margin percentage increased to\s*(\d+(?:\.\d+)?)%\s*from\s*(\d+(?:\.\d+)?)%[^.]{0,180}?(?:driven by|due to)\s*([^.]+)",
                text,
                re.I,
            )
            sendtech_margin_driver_match = re.search(
                r"\badjusted ebit margins?\s+improved\s+(\d+(?:\.\d+)?)\s+basis points\s+year[- ]over[- ]year\s+(?:due to|driven by)\s+([^.]+)",
                text,
                re.I,
            )
            margin_match = re.search(
                r"\b(?:gross\s+)?margin\s+expanded\s+(\d+(?:\.\d+)?)\s*bps",
                text,
                re.I,
            )
            margin_driver_match = re.search(
                r"\b(?:gross\s+)?margin\s+expanded\s+(\d+(?:\.\d+)?)\s*(?:basis points|bps)[^.]{0,180}?(?:due to|driven by)\s*([^.]+)",
                text,
                re.I,
            )
            opex_match = re.search(
                r"\b(?:operating expenses|opex)\s+declined\s+\$?\s*(\d+(?:\.\d+)?)\s*million(?:\s+yoy)?",
                text,
                re.I,
            )
            opex_driver_match = re.search(
                r"\b(?:operating expenses|opex)\s+declined\s+\$?\s*(\d+(?:\.\d+)?)\s*million(?:\s+year[- ]over[- ]year|\s+yoy)?\s+(?:primarily from|due to)\s+([^.]+)",
                text,
                re.I,
            )
            presort_driver_match = re.search(
                r"\b(higher revenue per piece,\s*improved productivity,\s*and cost reduction initiatives drove the increase in adjusted segment ebitda and ebit)\b",
                text,
                re.I,
            )
            parts: List[str] = []
            if adj_ebitda_pct_match:
                pct_val = float(adj_ebitda_pct_match.group(1))
                parts.append(f"Adjusted EBITDA {'improved' if pct_val >= 0 else 'declined'} {abs(pct_val):.1f}% YoY")
            if pct_from_to_driver_match:
                driver_txt = _clean_driver_phrase(pct_from_to_driver_match.group(3))
                driver_suffix = f", driven by {driver_txt}" if driver_txt else ""
                parts.append(
                    f"Gross margin expanded to {pct_from_to_driver_match.group(1)}% from {pct_from_to_driver_match.group(2)}%{driver_suffix}"
                )
            if sendtech_margin_driver_match:
                driver_txt = _clean_driver_phrase(sendtech_margin_driver_match.group(2))
                driver_suffix = f", driven by {driver_txt}" if driver_txt else ""
                parts.append(
                    f"Adjusted EBIT margin expanded {sendtech_margin_driver_match.group(1)} bps YoY{driver_suffix}"
                )
            if margin_driver_match and not pct_from_to_driver_match:
                driver_txt = _clean_driver_phrase(margin_driver_match.group(2))
                driver_suffix = f", driven by {driver_txt}" if driver_txt else ""
                parts.append(f"Gross margin expanded {margin_driver_match.group(1)} bps{driver_suffix}")
            if re.search(r"\bsendtech\b", low) and margin_match and not (pct_from_to_driver_match or margin_driver_match or sendtech_margin_driver_match):
                parts.append(f"SendTech margin expanded {margin_match.group(1)} bps")
            elif margin_match and not (pct_from_to_driver_match or margin_driver_match or sendtech_margin_driver_match):
                parts.append(f"Margin expanded {margin_match.group(1)} bps")
            if opex_driver_match:
                driver_txt = _clean_driver_phrase(opex_driver_match.group(2))
                driver_suffix = f", primarily from {driver_txt}" if driver_txt else ""
                parts.append(
                    f"Operating expenses declined {_fmt_short_millions_str(opex_driver_match.group(1))} YoY{driver_suffix}"
                )
            if opex_match and not opex_driver_match:
                parts.append(f"Operating expenses declined {_fmt_short_millions_str(opex_match.group(1))} YoY")
            if presort_driver_match:
                parts.append(
                    "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction"
                )
            if parts:
                return _finish("; ".join(parts[:2]))

        if label == "FCF improvement":
            fcf_match = re.search(
                r"\bfree cash flow\s+(was a use of|was|improved to|declined to)\s+\$?\s*(\d+(?:\.\d+)?)\s*million(?:,\s+an improvement of\s+\$?\s*(\d+(?:\.\d+)?)\s*million)?",
                text,
                re.I,
            )
            if fcf_match:
                lead_phrase = str(fcf_match.group(1) or "").strip().lower()
                current_num = float(fcf_match.group(2))
                if "use of" in lead_phrase:
                    current_num = -abs(current_num)
                elif "declined" in lead_phrase:
                    current_num = abs(current_num)
                current = _fmt_short_money_value_local(current_num * 1_000_000.0)
                improvement = fcf_match.group(3)
                if improvement:
                    prior_num = current_num - float(improvement)
                    return _finish(
                        _format_directional_fcf_summary_local(
                            current_num * 1_000_000.0,
                            prior_num * 1_000_000.0,
                            basis_label="YoY",
                        ).rstrip(".")
                    )
                if current_num < 0:
                    return _finish(f"Free cash flow was a use of {_fmt_short_money_value_local(abs(current_num) * 1_000_000.0)}")
                if "declined" in lead_phrase:
                    return _finish(f"Free cash flow declined to {current}")
                if "improved" in lead_phrase:
                    return _finish(f"Free cash flow improved to {current}")
                return _finish(f"Free cash flow was {current}")
            ttm_match = re.search(
                r"\bfcf ttm at .*?:\s*\$?(-?\d+(?:\.\d+)?)m,\s*(yoy|qoq)\s*(-?\d+(?:\.\d+)?)%,\s*delta\s+\$?(-?\d+(?:\.\d+)?)m\b",
                low,
                re.I,
            )
            if ttm_match:
                basis = str(ttm_match.group(2) or "").upper()
                current_num = float(ttm_match.group(1)) * 1_000_000.0
                delta_val = float(ttm_match.group(4))
                prior_num = current_num - (delta_val * 1_000_000.0)
                return _finish(
                    _format_directional_fcf_summary_local(current_num, prior_num, basis_label=basis).rstrip(".")
                )
        if label in {"Deleveraging / liquidity", "Debt reduction"}:
            from_to_match = re.search(
                r"\b(?:revolving credit facility|revolver availability|revolver)\b.*?\b(?:increased|expanded|raised)\s+from\s+\$?\s*(\d+(?:\.\d+)?)\s*million\s+to\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
            if from_to_match:
                return _finish(f"Revolver availability increased from {_fmt_short_millions_str(from_to_match.group(1))} to {_fmt_short_millions_str(from_to_match.group(2))}")
            debt_delta_match = re.search(r"\bnet debt delta\s+\$?(-?\d+(?:\.\d+)?)m\b", low, re.I)
            if debt_delta_match:
                delta = float(debt_delta_match.group(1))
                direction = "declined" if delta < 0 else "increased"
                return _finish(f"Net debt {direction} by {_fmt_short_money_value_local(abs(delta) * 1_000_000.0)}")
        generic_revolver_match = re.search(
            r"\b(?:revolving credit facility|revolver(?: availability)?)\b.*?\b(?:increased|expanded|raised)\s+from\s+\$?\s*(\d+(?:\.\d+)?)\s*million\s+to\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
            text,
            re.I,
        )
        if generic_revolver_match:
            return _finish(f"Revolver availability increased from {_fmt_short_millions_str(generic_revolver_match.group(1))} to {_fmt_short_millions_str(generic_revolver_match.group(2))}")
        generic_buyback_match = re.search(
            r"\brepurchas\w*\s+(\d+(?:\.\d+)?)\s+million\s+shares.*?\$\s*(\d+(?:\.\d+)?)\s*million",
            text,
            re.I,
        )
        if generic_buyback_match:
            quarter_anchor = ""
            if re.search(r"\b(q4|fourth quarter)\b", text, re.I):
                quarter_anchor = " in Q4"
            elif re.search(r"\b(q3|third quarter)\b", text, re.I):
                quarter_anchor = " in Q3"
            elif re.search(r"\b(q2|second quarter)\b", text, re.I):
                quarter_anchor = " in Q2"
            elif re.search(r"\b(q1|first quarter)\b", text, re.I):
                quarter_anchor = " in Q1"
            return _finish(f"Repurchased {_fmt_share_count(generic_buyback_match.group(1))} for {_fmt_short_millions_str(generic_buyback_match.group(2))}{quarter_anchor}")
        generic_debt_match = re.search(
            r"\b(?:reduc(?:ed|ing)\s+principal\s+debt|principal\s+debt\s+reduction)\b(?:\s+by)?\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
            text,
            re.I,
        )
        if generic_debt_match:
            return _finish(f"Reduced principal debt by {_fmt_short_millions_str(generic_debt_match.group(1))}")
        generic_margin_match = re.search(
            r"\b(?:gross\s+)?margin\s+expanded\s+(\d+(?:\.\d+)?)\s*bps",
            text,
            re.I,
        )
        generic_opex_match = re.search(
            r"\b(?:operating expenses|opex)\s+declined\s+\$?\s*(\d+(?:\.\d+)?)\s*million(?:\s+yoy)?",
            text,
            re.I,
        )
        if generic_margin_match and generic_opex_match:
            return _finish(f"Margin expanded {generic_margin_match.group(1)} bps; operating expenses declined {_fmt_short_millions_str(generic_opex_match.group(1))} YoY")
        if generic_margin_match:
            return _finish(f"Margin expanded {generic_margin_match.group(1)} bps")
        if generic_opex_match:
            return _finish(f"Operating expenses declined {_fmt_short_millions_str(generic_opex_match.group(1))} YoY")
        return ""

    def _pbi_contextual_note_summary_local(label: str, qd_ref: Optional[date], txt: str) -> str:
        if not is_pbi_profile or not qd_ref:
            return ""
        if label == "FCF improvement":
            pbi_reported_fcf = _pbi_reported_fcf_payload_for_qd(qd_ref)
            current_fcf = (
                pbi_reported_fcf.get("current")
                if isinstance(pbi_reported_fcf, dict) and pbi_reported_fcf.get("current") is not None
                else _pbi_adj_fcf_map.get(qd_ref)
            )
            if current_fcf is not None:
                prior_yoy = (
                    pbi_reported_fcf.get("prior")
                    if isinstance(pbi_reported_fcf, dict) and pbi_reported_fcf.get("prior") is not None
                    else _prev_same_quarter_year(qd_ref, _pbi_adj_fcf_map)
                )
                if prior_yoy is not None:
                    return _format_directional_fcf_summary_local(current_fcf, prior_yoy, basis_label="YoY")
                return _format_directional_fcf_summary_local(current_fcf, None, basis_label="YoY")
        if label in {"Deleveraging / liquidity", "Debt reduction"}:
            debt_repay = _pbi_hist_debt_repayment_map.get(qd_ref)
            if debt_repay is not None and abs(float(debt_repay)) >= 50_000_000.0:
                q_lbl = f" in Q{((qd_ref.month - 1) // 3) + 1}" if isinstance(qd_ref, date) else ""
                return _ensure_terminal_period(f"Reduced principal debt by {_fmt_short_money_value_local(float(debt_repay))}{q_lbl}")
            revolver_current = _pbi_revolver_availability_map.get(qd_ref)
            prev_pair = _prev_available_quarter(qd_ref, _pbi_revolver_availability_map)
            if revolver_current is not None and prev_pair is not None:
                _, revolver_prev = prev_pair
                if abs(float(revolver_current) - float(revolver_prev)) >= 25_000_000.0:
                    direction = "increased" if float(revolver_current) > float(revolver_prev) else "declined"
                    return _ensure_terminal_period(
                        f"Revolver availability {direction} from "
                        f"{_fmt_short_money_value_local(float(revolver_prev))} to "
                        f"{_fmt_short_money_value_local(float(revolver_current))}"
                    )
        if label == "Capital allocation / buyback":
            ctx_ref_local = _current_ctx_ref()
            valuation_bundle = (
                dict(getattr(getattr(ctx_ref_local, "derived", None), "valuation_precompute_bundle", {}) or {})
                if ctx_ref_local is not None
                else {}
            )
            doc_buyback_summary = _best_doc_buyback_execution_summary_local(qd_ref)
            if doc_buyback_summary:
                return doc_buyback_summary
            if valuation_bundle:
                buyback_doc_note_map = dict(valuation_bundle.get("buyback_doc_note_map") or {})
                buyback_doc_note = buyback_doc_note_map.get(pd.Timestamp(qd_ref))
                if buyback_doc_note:
                    detail_summary = _pbi_detail_preserving_note_summary_local(label, str(buyback_doc_note))
                    if detail_summary:
                        return detail_summary
            buyback_cash = _pbi_hist_buybacks_cash_map.get(qd_ref)
            if buyback_cash is not None and float(buyback_cash) > 0:
                q_lbl = f" in Q{((qd_ref.month - 1) // 3) + 1}" if isinstance(qd_ref, date) else ""
                if re.search(r"\brepurchas\w*\s+(\d+(?:\.\d+)?)\s+million\s+shares\b", txt, re.I):
                    return ""
                return _ensure_terminal_period(f"Repurchased shares for {_fmt_short_money_value_local(float(buyback_cash))}{q_lbl}")
        return ""

    def _fmt_note_share_count_local(value_in: Any) -> str:
        try:
            share_val = float(value_in)
        except Exception:
            return f"{value_in} shares"
        if abs(share_val) >= 1_000_000.0:
            return f"{share_val / 1_000_000.0:.1f}m shares"
        if abs(share_val) >= 1_000.0:
            return f"{share_val:,.0f} shares"
        if abs(share_val - round(share_val)) < 1e-9:
            return f"{int(round(share_val))} shares"
        return f"{share_val:,.1f} shares"

    def _fmt_short_millions_note_local(raw_val: Any) -> str:
        try:
            return _fmt_short_money_value_local(float(raw_val) * 1_000_000.0)
        except Exception:
            return f"${raw_val}m"

    def _buyback_anchor_from_text_local(text_in: Any, qd_ref: Optional[date] = None) -> str:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return ""
        if re.search(r"\b(q4|fourth quarter)\b", text, re.I):
            return " in Q4"
        if re.search(r"\b(q3|third quarter)\b", text, re.I):
            return " in Q3"
        if re.search(r"\b(q2|second quarter)\b", text, re.I):
            return " in Q2"
        if re.search(r"\b(q1|first quarter)\b", text, re.I):
            return " in Q1"
        three_months_match = re.search(
            r"\bthree months ended\s+([A-Za-z]+)\s+\d{1,2},\s*(20\d{2})\b",
            text,
            re.I,
        )
        if three_months_match:
            q_try = pd.to_datetime(
                f"{three_months_match.group(1)} 1 {three_months_match.group(2)}",
                errors="coerce",
            )
            if pd.notna(q_try):
                return f" in Q{((int(q_try.month) - 1) // 3) + 1}"
        if re.search(r"\bthree months ended\b", text, re.I) and isinstance(qd_ref, date):
            return f" in Q{((qd_ref.month - 1) // 3) + 1}"
        if re.search(r"\bsince\s+starting(?:\s+the\s+program)?(?:[^.]{0,40}?earlier\s+this\s+year)?\b", text, re.I):
            return " since starting the program earlier this year"
        return ""

    def _is_cumulative_buyback_context_local(text_in: Any) -> bool:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return False
        return bool(
            re.search(
                r"\b("
                r"since inception|"
                r"to date|"
                r"since starting(?:\s+the\s+program)?|"
                r"since the beginning|"
                r"authorized up to|"
                r"authorization remained|"
                r"remaining authorization|"
                r"remaining capacity|"
                r"may repurchase|"
                r"under the program we may repurchase|"
                r"did not repurchase any shares|"
                r"no shares were repurchased"
                r")\b",
                text,
                re.I,
            )
        )

    def _has_negative_buyback_statement_for_ref_local(text_in: Any, qd_ref: Optional[date] = None) -> bool:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return False
        if not re.search(
            r"\b(?:did not repurchas\w*|no repurchas\w* was made|no other repurchas\w* was made)\b",
            text,
            re.I,
        ):
            return False
        if not isinstance(qd_ref, date):
            return True
        q_num = ((qd_ref.month - 1) // 3) + 1
        quarter_tokens = {
            1: [r"\bq1\b", r"\bfirst quarter\b", rf"\bmarch 31,\s*{qd_ref.year}\b"],
            2: [r"\bq2\b", r"\bsecond quarter\b", rf"\bjune 30,\s*{qd_ref.year}\b"],
            3: [r"\bq3\b", r"\bthird quarter\b", rf"\bseptember 30,\s*{qd_ref.year}\b"],
            4: [r"\bq4\b", r"\bfourth quarter\b", rf"\bdecember 31,\s*{qd_ref.year}\b"],
        }.get(q_num, [])
        if any(re.search(token, text, re.I) for token in quarter_tokens):
            return True
        three_months_match = re.search(
            r"\bthree months ended\s+([A-Za-z]+)\s+\d{1,2},\s*(20\d{2})\b",
            text,
            re.I,
        )
        if three_months_match:
            try:
                ts = pd.to_datetime(
                    f"{three_months_match.group(1)} 1 {three_months_match.group(2)}",
                    errors="raise",
                )
            except Exception:
                ts = pd.NaT
            if pd.notna(ts):
                return ((int(ts.month) - 1) // 3) + 1 == q_num and int(ts.year) == int(qd_ref.year)
        return False

    def _extract_ytd_buyback_including_quarter_split_local(
        text_in: Any,
        qd_ref: Optional[date] = None,
    ) -> Dict[str, Any]:
        text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
        if not text or not isinstance(qd_ref, date):
            return {}
        if not re.search(r"\b(year[- ]to[- ]date|ytd)\b", text, re.I):
            return {}
        if not re.search(r"\bincluding\b", text, re.I):
            return {}

        q_num = ((qd_ref.month - 1) // 3) + 1
        quarter_tokens = {
            1: r"(?:q1|first quarter)",
            2: r"(?:q2|second quarter)",
            3: r"(?:q3|third quarter)",
            4: r"(?:q4|fourth quarter)",
        }.get(q_num, r"(?:q[1-4]|first quarter|second quarter|third quarter|fourth quarter)")

        total_match = re.search(
            r"\brepurchas\w*\b[^.]{0,180}?"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
            r"[^.]{0,120}?\bfor\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*"
            r"(million|billion|m|bn)?\b",
            text,
            re.I,
        )
        include_match = re.search(
            rf"\bincluding\s+([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
            rf"[^. ]{{0,20}}(?:[^.]{{0,140}}?)\bfor\s+\$?\s*([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]+(?:\.\d+)?)\s*"
            rf"(million|billion|m|bn)?\b[^.]{{0,160}}?\bin\s+(?:the\s+)?{quarter_tokens}\b",
            text,
            re.I,
        )
        if not total_match or not include_match:
            return {}

        def _parse_shares(raw_num: Any, unit_in: Any = "") -> Optional[float]:
            try:
                val = float(str(raw_num or "").replace(",", ""))
            except Exception:
                return None
            unit_low = str(unit_in or "").strip().lower()
            if unit_low in {"million", "m"} or val < 100_000.0:
                val *= 1_000_000.0
            return float(val) if val > 0 else None

        total_shares = _parse_shares(total_match.group(1), total_match.group(2))
        total_amount = _parse_buyback_money_local(total_match.group(3), total_match.group(4))
        quarter_shares = _parse_shares(include_match.group(1), include_match.group(2))
        quarter_amount = _parse_buyback_money_local(include_match.group(3), include_match.group(4))
        if (
            total_shares is None
            or total_amount is None
            or quarter_shares is None
            or quarter_amount is None
            or quarter_shares <= 0
            or quarter_amount <= 0
            or total_shares + 1.0 < quarter_shares
            or total_amount + 1.0 < quarter_amount
        ):
            return {}

        cutoff_match = re.search(
            r"\bthrough\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,\s*20\d{2})?)\b",
            text,
            re.I,
        )
        cutoff_txt = re.sub(r"\s+", " ", str(cutoff_match.group(1) or "")).strip() if cutoff_match else ""
        if cutoff_txt:
            cutoff_txt = re.sub(rf",\s*{int(qd_ref.year)}\s*$", "", cutoff_txt).strip()
        post_shares = max(0.0, float(total_shares) - float(quarter_shares))
        post_amount = max(0.0, float(total_amount) - float(quarter_amount))
        quarter_avg = float(quarter_amount) / float(quarter_shares) if quarter_shares > 0 else None
        return {
            "quarter": {
                "shares": float(quarter_shares),
                "amount": float(quarter_amount),
                "avg_price": float(quarter_avg) if quarter_avg and quarter_avg > 0 else None,
                "anchor": f" in Q{q_num}",
                "quarter_scoped": True,
                "from_table": False,
                "from_ytd_split": True,
                "explicit_shares": True,
                "explicit_amount": True,
                "explicit_avg_price": False,
                "explicit_count": 3,
            },
            "post": {
                "shares": float(post_shares),
                "amount": float(post_amount),
                "cutoff": cutoff_txt,
            },
        }

    def _is_debt_repurchase_noise_local(text_in: Any) -> bool:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return False
        low = text.lower()
        if not re.search(r"\brepurchas\w*\b", low, re.I):
            return False
        equity_context = bool(
            re.search(
                r"\b(common stock|share repurchase|buyback|shares?\b|treasury stock|repurchase program)\b",
                low,
                re.I,
            )
        )
        debt_context = bool(
            re.search(
                r"\b(fundamental change|indenture|convertible|senior notes?|2027 notes?|2030 notes?|holders?\b|subscription transactions?)\b",
                low,
                re.I,
            )
        )
        noteholder_put = bool(
            re.search(
                r"\b(require the company to repurchase|repurchase their\b[^.]{0,120}\bnotes?\b|holders?\b[^.]{0,120}\bnotes?\b[^.]{0,120}\brepurchase)\b",
                low,
                re.I,
            )
        )
        return bool(noteholder_put or (debt_context and not equity_context))

    def _parse_buyback_money_local(raw_num: Any, unit_in: Any = "") -> Optional[float]:
        try:
            value = float(str(raw_num or "").replace(",", ""))
        except Exception:
            return None
        unit_low = str(unit_in or "").strip().lower()
        if unit_low in {"billion", "bn"}:
            value *= 1_000_000_000.0
        elif unit_low in {"million", "m"}:
            value *= 1_000_000.0
        elif value < 2_000.0:
            value *= 1_000_000.0
        if value <= 0:
            return None
        return float(value)

    def _extract_buyback_table_execution_local(text_in: Any, qd_ref: Optional[date] = None) -> Dict[str, Any]:
        text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
        if not text:
            return {}
        search_text = glx_normalize_text(re.sub(r"<[^>]+>", " ", text))
        if not search_text:
            search_text = text
        has_header_signal = bool(
            re.search(
            r"\b(?:shares purchased|repurchases of equity securities|repurchases of securities)\b",
            search_text,
            re.I,
            )
        )
        has_period_signal = bool(
            re.search(
            r"\b(?:three months ended|q[1-4]|first quarter|second quarter|third quarter|fourth quarter)\b",
            search_text,
            re.I,
            )
        )
        month_row_re = re.compile(
            r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
            r"(20\d{2})\s+([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\s+\$\s*([0-9]+(?:\.\d+)?)\s+"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\s+\$?([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+))?",
            re.I,
        )
        month_rows = list(month_row_re.finditer(search_text))
        if not month_rows:
            return {}
        if not has_header_signal and len(month_rows) < 2:
            return {}
        if not has_period_signal and qd_ref is None:
            return {}
        parsed_rows: List[Tuple[float, float, int, int, Optional[float]]] = []
        for match in month_rows:
            month_name = str(match.group(1) or "")
            year_val = int(match.group(2))
            try:
                shares_val = float(str(match.group(3) or "").replace(",", ""))
                avg_price_val = float(str(match.group(4) or "").replace(",", ""))
            except Exception:
                continue
            if shares_val < 1_000 or avg_price_val <= 0:
                continue
            remaining_capacity_val: Optional[float] = None
            try:
                remaining_raw = str(match.group(6) or "").replace(",", "").strip()
                if remaining_raw:
                    remaining_capacity_val = float(remaining_raw) * 1_000.0
            except Exception:
                remaining_capacity_val = None
            month_num = pd.to_datetime(f"{month_name} 1 {year_val}", errors="coerce")
            if pd.isna(month_num):
                continue
            parsed_rows.append((shares_val, avg_price_val, int(month_num.month), year_val, remaining_capacity_val))
        if not parsed_rows:
            return {}
        if len({row[3] for row in parsed_rows}) != 1:
            return {}
        total_shares = sum(row[0] for row in parsed_rows)
        total_amount = sum(row[0] * row[1] for row in parsed_rows)
        if total_shares <= 0 or total_amount <= 0:
            return {}
        quarter_nums = {((row[2] - 1) // 3) + 1 for row in parsed_rows}
        derived_quarter_num = next(iter(quarter_nums)) if len(quarter_nums) == 1 else 0
        if isinstance(qd_ref, date) and derived_quarter_num:
            qd_quarter_num = ((qd_ref.month - 1) // 3) + 1
            if derived_quarter_num != qd_quarter_num:
                return {}
        avg_price = total_amount / total_shares
        remaining_capacity = next(
            (
                float(row[4])
                for row in reversed(parsed_rows)
                if row[4] is not None and float(row[4]) > 0
            ),
            None,
        )
        summary_tail = search_text[month_rows[-1].end() : min(len(search_text), month_rows[-1].end() + 180)]
        total_row_match = re.search(
            r"\b([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\s+\$\s*([0-9]+(?:\.\d+)?)\s+([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\b",
            summary_tail,
            re.I,
        )
        if total_row_match:
            try:
                total_row_shares = float(str(total_row_match.group(1) or "").replace(",", ""))
                total_row_avg = float(str(total_row_match.group(2) or "").replace(",", ""))
                total_row_program = float(str(total_row_match.group(3) or "").replace(",", ""))
            except Exception:
                total_row_shares = 0.0
                total_row_avg = 0.0
                total_row_program = 0.0
            if (
                total_row_avg > 0
                and total_row_shares > 0
                and total_row_program > 0
                and abs(total_row_shares - total_row_program) <= max(1.0, total_row_shares * 0.0005)
                and abs(total_row_shares - total_shares) <= max(1_000.0, total_shares * 0.01)
            ):
                avg_price = float(total_row_avg)
        anchor = f" in Q{derived_quarter_num}" if derived_quarter_num else ""
        if not anchor:
            anchor = _buyback_anchor_from_text_local(search_text, qd_ref)
        if not anchor and isinstance(qd_ref, date):
            anchor = f" in Q{((qd_ref.month - 1) // 3) + 1}"
        return {
            "shares": float(total_shares),
            "amount": float(total_amount),
            "avg_price": float(avg_price),
            "anchor": anchor,
            "quarter_scoped": True,
            "from_table": True,
            "remaining_capacity": remaining_capacity,
        }

    def _quarter_end_for_month_local(year_in: int, month_in: int) -> Optional[date]:
        try:
            ts = pd.Timestamp(year=int(year_in), month=int(month_in), day=1) + pd.offsets.QuarterEnd(0)
        except Exception:
            return None
        try:
            return ts.date()
        except Exception:
            return None

    def _explicit_event_quarter_override_local(text_in: Any, default_q: Optional[date] = None) -> Optional[date]:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return None
        best_q: Optional[date] = None
        best_rank: Optional[Tuple[int, int, int, int]] = None
        for match in re.finditer(
            r"(?:\b(?:in|on)\s+|^|[|;:]\s*)(January|February|March|April|May|June|July|August|September|October|November|December)"
            r"(?:\s+(\d{1,2}),)?\s+(20\d{2})\b",
            text,
            re.I,
        ):
            prefix = text[max(0, int(match.start()) - 48) : int(match.start())].lower()
            has_day = bool(str(match.group(2) or "").strip())
            blocked_context = bool(
                re.search(
                    r"\b(?:due|mature(?:s|d)?|year ended|for the year ended|quarter ended|three months ended|as of)\s*$",
                    prefix,
                    re.I,
                )
            )
            if blocked_context:
                continue
            try:
                month_num = int(pd.to_datetime(f"{match.group(1)} 1 {match.group(3)}", errors="raise").month)
                year_num = int(match.group(3))
            except Exception:
                continue
            q_override = _quarter_end_for_month_local(year_num, month_num)
            if q_override is None:
                continue
            if default_q is not None and abs((q_override - default_q).days) > 370:
                continue
            rank = (
                0 if has_day else 1,
                0 if re.search(r"\b(?:on|entered|executed|completed|amended|closed)\s*$", prefix, re.I) else 1,
                0 if default_q is not None and q_override == default_q else 1,
                int(match.start()),
            )
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best_q = q_override
        return best_q

    def _extract_buyback_execution_components_local(text_in: Any, qd_ref: Optional[date] = None) -> Dict[str, Any]:
        text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
        if not text:
            return {}
        if (
            _is_cumulative_buyback_context_local(text)
            and _has_negative_buyback_statement_for_ref_local(text, qd_ref)
            and _explicit_event_quarter_override_local(text, default_q=qd_ref) is None
        ):
            return {}
        table_hit = _extract_buyback_table_execution_local(text, qd_ref)
        if table_hit:
            return table_hit

        def _extract_from_chunk(chunk_in: str) -> Dict[str, Any]:
            chunk = glx_normalize_text(html.unescape(str(chunk_in or "")).replace("\xa0", " "))
            if not chunk:
                return {}
            if _has_negative_buyback_statement_for_ref_local(chunk, qd_ref):
                return {}
            if _is_debt_repurchase_noise_local(chunk):
                return {}
            execution_signal = bool(
                re.search(
                    r"\b(repurchased|repurchasing|bought\s+back|repurchase\b[^.]{0,100}\bshares\b)\b",
                    chunk,
                    re.I,
                )
            )
            if not execution_signal:
                return {}
            ytd_split_hit = _extract_ytd_buyback_including_quarter_split_local(chunk, qd_ref)
            if ytd_split_hit.get("quarter"):
                return dict(ytd_split_hit.get("quarter") or {})
            anchor_local = _buyback_anchor_from_text_local(chunk, qd_ref)
            event_q_local = _explicit_event_quarter_override_local(chunk, default_q=qd_ref)
            if re.search(r"\b(year[- ]to[- ]date|ytd)\b", chunk, re.I):
                return {}
            if _is_cumulative_buyback_context_local(chunk) and not anchor_local.startswith(" in Q") and event_q_local is None:
                return {}
            search_chunks: List[str] = []
            for rep_match in re.finditer(r"\b(repurchas\w*|bought\s+back)\b", chunk, re.I):
                start = max(0, rep_match.start() - 90)
                end = min(len(chunk), rep_match.end() + 260)
                search_chunks.append(chunk[start:end])
            if not search_chunks:
                search_chunks.append(chunk)

            def _find_first(pattern: str) -> Optional[re.Match[str]]:
                for search_chunk in search_chunks:
                    match = re.search(pattern, search_chunk, re.I)
                    if match:
                        return match
                return None

            share_match = _find_first(
                r"\b(?:repurchas\w*|bought\s+back)\b(?:\s+(?:approximately|approx\.?|about|an\s+additional|additional|aggregate|an\s+aggregate))*\s+"
                r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
            )
            amount_match = _find_first(
                r"\b(?:repurchas\w*|bought\s+back)\b.{0,220}?\bfor(?:\s+(?:a\s+)?total\s+of)?(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\b"
            )
            if not amount_match:
                amount_match = _find_first(
                    r"\b(?:repurchas\w*|bought\s+back)\b.{0,220}?\bat\s+(?:a\s+)?(?:total\s+)?cost\s+of(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\b"
                )
            if not amount_match:
                amount_match = _find_first(
                    r"\b(?:repurchas\w*|bought\s+back)\b\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*"
                    r"(million|billion|m|bn)\s+in\s+shares?\b"
                )
            if not amount_match:
                amount_match = _find_first(
                    r"\bused\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+of\s+(?:the\s+)?proceeds\b"
                    r"[^.]{0,120}?\b(?:to\s+)?repurchas\w*\b"
                )
            avg_match = _find_first(
                r"\baverage price(?: paid)?(?: per share| of)\s+\$?\s*([0-9]+(?:\.\d+)?)\b"
            )

            shares_val: Optional[float] = None
            amount_val: Optional[float] = None
            avg_price_val: Optional[float] = None
            explicit_flags = {"shares": False, "amount": False, "avg_price": False}

            if share_match:
                try:
                    shares_val = float(str(share_match.group(1) or "").replace(",", ""))
                    if str(share_match.group(2) or "").strip().lower() in {"million", "m"}:
                        shares_val *= 1_000_000.0
                    explicit_flags["shares"] = shares_val > 0
                except Exception:
                    shares_val = None
            if amount_match:
                amount_val = _parse_buyback_money_local(amount_match.group(1), amount_match.group(2))
                explicit_flags["amount"] = bool(amount_val and amount_val > 0)
            if avg_match:
                try:
                    avg_price_val = float(str(avg_match.group(1) or "").replace(",", ""))
                    explicit_flags["avg_price"] = avg_price_val > 0
                except Exception:
                    avg_price_val = None

            explicit_count_local = sum(1 for flag in explicit_flags.values() if flag)
            if explicit_count_local >= 2:
                if amount_val is None and shares_val is not None and avg_price_val is not None and avg_price_val > 0:
                    amount_val = float(shares_val) * float(avg_price_val)
                if shares_val is None and amount_val is not None and avg_price_val is not None and avg_price_val > 0:
                    shares_val = float(amount_val) / float(avg_price_val)
                if avg_price_val is None and amount_val is not None and shares_val is not None and shares_val > 0:
                    avg_price_val = float(amount_val) / float(shares_val)

            if amount_val is None and shares_val is None:
                return {}
            return {
                "shares": shares_val,
                "amount": amount_val,
                "avg_price": avg_price_val,
                "anchor": anchor_local,
                "quarter_scoped": bool(anchor_local and anchor_local.startswith(" in Q")),
                "from_table": False,
                "explicit_shares": explicit_flags["shares"],
                "explicit_amount": explicit_flags["amount"],
                "explicit_avg_price": explicit_flags["avg_price"],
                "explicit_count": explicit_count_local,
            }

        sentence_candidates: List[str] = []
        seen_chunks: set[str] = set()
        for sent in glx_split_sentences(text):
            chunk = glx_normalize_text(sent)
            if not chunk or len(chunk) < 24:
                continue
            if not re.search(r"\brepurchas\w*\b", chunk, re.I):
                continue
            chunk_key = chunk.lower()
            if chunk_key in seen_chunks:
                continue
            seen_chunks.add(chunk_key)
            sentence_candidates.append(chunk)

        best_parts: Dict[str, Any] = {}
        best_score = -1.0
        for candidate_text in sentence_candidates:
            candidate_parts = _extract_from_chunk(candidate_text)
            if not candidate_parts:
                continue
            candidate_score = float(candidate_parts.get("explicit_count") or 0) * 10.0
            if candidate_parts.get("quarter_scoped"):
                candidate_score += 5.0
            if candidate_parts.get("avg_price") is not None:
                candidate_score += 2.0
            if candidate_parts.get("amount") is not None:
                candidate_score += 1.0
            if candidate_score > best_score:
                best_score = candidate_score
                best_parts = candidate_parts

        if best_parts and float(best_parts.get("explicit_count") or 0.0) >= 2.0:
            return best_parts

        if _has_negative_buyback_statement_for_ref_local(text, qd_ref):
            return {}
        fallback_parts = _extract_from_chunk(text)
        if fallback_parts:
            return fallback_parts
        return {}

    def _compose_buyback_execution_summary_local(text_in: Any, qd_ref: Optional[date] = None) -> str:
        text = glx_normalize_text(str(text_in or ""))
        parts = _extract_buyback_execution_components_local(text_in, qd_ref)
        if not parts:
            return ""
        shares_val = parts.get("shares")
        amount_val = parts.get("amount")
        avg_price_val = parts.get("avg_price")
        anchor = str(parts.get("anchor") or "")
        if not anchor and re.search(
            r"\bfrom\s+[A-Za-z]+\s+\d{1,2},\s+20\d{2}\s+through\s+[A-Za-z]+\s+\d{1,2},\s+20\d{2}\b",
            text,
            re.I,
        ):
            return ""
        if not anchor:
            event_q = _explicit_event_quarter_override_local(text, default_q=qd_ref)
            if isinstance(event_q, date):
                anchor = f" in Q{((event_q.month - 1) // 3) + 1}"
        if not anchor:
            return ""
        if _is_cumulative_buyback_context_local(text) and not bool(parts.get("from_table")):
            return ""
        if shares_val is not None and amount_val is not None and avg_price_val is not None and avg_price_val > 0:
            return _ensure_terminal_period(
                f"Repurchased {_fmt_note_share_count_local(shares_val)} for {_fmt_short_money_value_local(float(amount_val))} "
                f"with an average price of ${float(avg_price_val):.2f}/share{anchor}"
            )
        if shares_val is not None and amount_val is not None:
            return _ensure_terminal_period(
                f"Repurchased {_fmt_note_share_count_local(shares_val)} for {_fmt_short_money_value_local(float(amount_val))}{anchor}"
            )
        if amount_val is not None:
            return _ensure_terminal_period(
                f"Repurchased {_fmt_short_money_value_local(float(amount_val))} of shares{anchor}"
            )
        return ""

    def _build_post_quarter_buyback_companion_row_local(
        qd_ref: date,
        q_items_in: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        candidate_texts: List[Tuple[str, str]] = []
        seen_candidate_texts: set[str] = set()

        def _add_candidate(text_in: Any, source_type: str) -> None:
            txt = glx_normalize_text(text_in)
            if not txt:
                return
            txt_key = txt.lower()
            if txt_key in seen_candidate_texts:
                return
            seen_candidate_texts.add(txt_key)
            candidate_texts.append((txt, source_type))

        for item_in in list(q_items_in or []):
            for raw_txt in [
                item_in.get("text_full"),
                item_in.get("comment_full_text"),
                item_in.get("evidence_snippet"),
                item_in.get("_render_summary"),
            ]:
                _add_candidate(raw_txt, str(dict(item_in.get("source") or {}).get("source_type") or "quarter_notes"))
        for raw_rec in _quarter_notes_raw_records_by_quarter_local().get(qd_ref, []):
            for raw_txt in [
                raw_rec.get("claim"),
                raw_rec.get("note"),
                raw_rec.get("evidence_snippet"),
                raw_rec.get("text_full"),
                raw_rec.get("comment_full_text"),
                raw_rec.get("statement"),
                raw_rec.get("promise_text"),
            ]:
                _add_candidate(raw_txt, str(raw_rec.get("source_type") or raw_rec.get("doc_type") or "quarter_notes"))
        cap_alloc_exec = dict(cap_alloc_exec_by_q.get(qd_ref) or {})
        _add_candidate(cap_alloc_exec.get("buybacks_note"), str(cap_alloc_exec.get("buybacks_source") or "sec_doc_note"))
        cap_alloc_tone = dict(cap_alloc_tone_by_q.get(qd_ref) or {})
        _add_candidate(
            cap_alloc_tone.get("text_full"),
            str(dict(cap_alloc_tone.get("source") or {}).get("source_type") or "promise_text"),
        )
        post_quarter_dir_specs = [
            ("CEO letters", "ceo_letter"),
            ("earnings_release", "earnings_release"),
            ("press_release", "press_release"),
            ("earnings_presentation", "earnings_presentation"),
            ("earnings_transcripts", "transcript"),
        ]
        for source_type, _path_in, joined in _quarter_scoped_material_texts_by_quarter_local(
            post_quarter_dir_specs,
            min_year=max(int(qd_ref.year) - 1, 2024),
        ).get(qd_ref, []):
            _add_candidate(joined, source_type)
        for source_type, _path_in, joined in _quarter_scoped_sec_cache_texts_by_quarter_local(
            min_year=max(int(qd_ref.year) - 1, 2024)
        ).get(qd_ref, []):
            _add_candidate(joined, source_type)

        best_row: Optional[Dict[str, Any]] = None
        best_score = -1.0
        for txt, source_type in candidate_texts:
            parsed = _extract_post_quarter_buyback_commentary_local(txt, qd_ref)
            summary_txt = str(parsed.get("summary") or "").strip()
            if not summary_txt:
                continue
            cand_score = float(parsed.get("score") or 0.0)
            if cand_score <= best_score:
                continue
            best_score = cand_score
            best_row = {
                "quarter": qd_ref,
                "bucket": "Capital allocation / shareholder returns",
                "text_full": summary_txt,
                "comment_full_text": summary_txt,
                "score": 86.0 + cand_score,
                "candidate_type": "buyback_post_quarter_commentary",
                "driver_tag": "buyback_post_quarter_commentary",
                "metric_tag": "Capital allocation / buyback|post_quarter_commentary",
                "metric_canon": "Capital allocation / buyback|post_quarter_commentary",
                "_metric_display": "Capital allocation / buyback",
                "_render_summary": summary_txt,
                "_render_summary_locked": True,
                "_split_focus": "buyback_post_quarter_commentary",
                "_theme_scope_key": "capital allocation / buyback|post_quarter_commentary",
                "mention_kind": "text",
                "source": {
                    "source_type": source_type or "quarter_notes_post_quarter_commentary",
                    "doc": "",
                    "form": "",
                },
            }
        return best_row

    def _build_pbi_q1_2026_context_rows_local(
        qd_ref: date,
        q_items_in: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not is_pbi_profile or qd_ref != date(2026, 3, 31):
            return []
        if not isinstance(hist, pd.DataFrame) or hist.empty or "quarter" not in hist.columns:
            return []
        h_local = hist.copy()
        h_local["quarter"] = pd.to_datetime(h_local["quarter"], errors="coerce")
        h_local = h_local[h_local["quarter"].notna()].copy()
        h_local["_qdate"] = h_local["quarter"].dt.to_period("Q").dt.end_time.dt.date
        row_df = h_local[h_local["_qdate"] == qd_ref]
        if row_df.empty:
            return []
        hist_row = row_df.iloc[-1]

        def _num_col(row_in: Any, *cols: str) -> Optional[float]:
            for col in cols:
                if col not in row_in.index:
                    continue
                val = pd.to_numeric(row_in.get(col), errors="coerce")
                if pd.notna(val):
                    return float(val)
            return None

        def _money_m1(val: float) -> str:
            return f"${float(val) / 1_000_000.0:,.1f}m"

        def _money_m3(val: float) -> str:
            return f"${float(val) / 1_000_000.0:,.3f}m"

        def _row(summary: str, bucket: str, metric: str, candidate_type: str, score: float) -> Dict[str, Any]:
            clean_summary = _ensure_terminal_period(glx_normalize_text(summary).strip(" ."))
            return {
                "quarter": qd_ref,
                "bucket": bucket,
                "text_full": clean_summary,
                "comment_full_text": clean_summary,
                "score": score,
                "candidate_type": candidate_type,
                "driver_tag": candidate_type,
                "metric_tag": metric,
                "metric_canon": metric,
                "_metric_display": metric,
                "_render_summary": clean_summary,
                "_render_summary_locked": True,
                "_force_note_passthrough": True,
                "_theme_scope_key": f"{metric}|{candidate_type}".lower(),
                "mention_kind": "text",
                "source": {"source_type": "q1_2026_context_guardrail", "doc": "Q1 2026 source materials", "form": ""},
            }

        candidate_texts: List[str] = []

        def _add_text(text_in: Any) -> None:
            txt = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
            if txt and txt.lower() not in {x.lower() for x in candidate_texts}:
                candidate_texts.append(txt)

        for item_in in list(q_items_in or []):
            for key in ["text_full", "comment_full_text", "evidence_snippet", "_render_summary"]:
                _add_text(item_in.get(key))
        for raw_rec in _quarter_notes_raw_records_by_quarter_local().get(qd_ref, []):
            for key in ["claim", "note", "evidence_snippet", "text_full", "comment_full_text"]:
                _add_text(raw_rec.get(key))
        try:
            for q_raw, _source_type, _path_in, joined in _iter_quarter_scoped_material_texts_local(
                [
                    ("earnings_release", "earnings_release"),
                    ("CEO letters", "ceo_letter"),
                    ("earnings_transcripts", "transcript"),
                    ("press_release", "press_release"),
                ],
                min_year=2026,
            ):
                if q_raw == qd_ref:
                    _add_text(joined)
        except Exception:
            pass
        blob = " | ".join(candidate_texts)
        blob_low = blob.lower()

        out_rows: List[Dict[str, Any]] = []
        cfo_val = _num_col(hist_row, "cfo", "cash_from_operations", "net_cash_from_operating_activities")
        capex_val = _num_col(hist_row, "capex", "capital_expenditures")
        company_fcf = None
        if isinstance(adj_metrics, pd.DataFrame) and not adj_metrics.empty and "quarter" in adj_metrics.columns:
            try:
                adj_local = adj_metrics.copy()
                adj_local["quarter"] = pd.to_datetime(adj_local["quarter"], errors="coerce")
                adj_local = adj_local[adj_local["quarter"].notna()].copy()
                adj_local["_qdate"] = adj_local["quarter"].dt.to_period("Q").dt.end_time.dt.date
                adj_q = adj_local[adj_local["_qdate"] == qd_ref]
                if not adj_q.empty:
                    adj_row = adj_q.iloc[-1]
                    company_fcf = _num_col(adj_row, "adj_fcf", "adjusted_fcf", "free_cash_flow")
            except Exception:
                company_fcf = None
        for pat in [
            r"\bfree cash flow(?:\s+improved\s+to)?\s+\$?\s*([0-9]+(?:\.\d+)?)\s*m\b",
            r"\bfree cash flow\s+\$?\s*([0-9]{2,3}(?:,[0-9]{3})|[0-9]+(?:\.\d+)?)\b",
        ]:
            if company_fcf is not None:
                break
            for m_fcf in re.finditer(pat, blob, re.I):
                sent_start = max(blob.rfind(".", 0, int(m_fcf.start())), blob.rfind("|", 0, int(m_fcf.start())))
                sent_end_candidates = [pos for pos in [blob.find(".", int(m_fcf.end())), blob.find("|", int(m_fcf.end()))] if pos >= 0]
                sent_end = min(sent_end_candidates) if sent_end_candidates else min(len(blob), int(m_fcf.end()) + 120)
                fcf_window = blob[(sent_start + 1 if sent_start >= 0 else 0):sent_end].lower()
                if re.search(
                    r"\b(guidance|outlook|full[- ]year|fy\s*20\d{2}|target|range|low\s+high)\b",
                    fcf_window,
                    re.I,
                ):
                    continue
                try:
                    raw = float(str(m_fcf.group(1)).replace(",", ""))
                    company_fcf = raw * (1_000_000.0 if raw < 10_000 else 1_000.0)
                except Exception:
                    company_fcf = None
                if company_fcf is not None:
                    break
            if company_fcf is not None:
                break
        div_cash = _num_col(hist_row, "dividends_cash", "payments_of_dividends")
        div_ttm = None
        if div_cash is not None and "dividends_cash" in h_local.columns:
            h_sorted = h_local.sort_values("quarter")
            last4 = h_sorted[h_sorted["_qdate"] <= qd_ref].tail(4)
            vals = pd.to_numeric(last4["dividends_cash"], errors="coerce")
            if len(vals) == 4 and vals.notna().all():
                div_ttm = float(vals.sum())
        div_ps = None
        if div_ps is None and div_cash is not None:
            share_den = _num_col(hist_row, "shares_diluted", "shares_outstanding")
            try:
                if share_den not in (None, 0) and pd.notna(share_den):
                    implied_ps = float(div_cash) / float(share_den)
                    rounded_ps = round(float(implied_ps) + 1e-9, 2)
                    if 0.0 < rounded_ps < 5.0:
                        div_ps = float(rounded_ps)
            except Exception:
                div_ps = None
        dividend_note_text = capital_return_build_dividend_note_from_text(blob, current_per_share=div_ps)
        if dividend_note_text:
            out_rows.append(
                _row(
                    dividend_note_text,
                    "Capital allocation / shareholder returns",
                    "Dividend cash",
                    "dividend_policy_note",
                    98.0,
                )
            )
        if (
            re.search(r"\bsales bookings increased\b", blob_low, re.I)
            and "paid software subscribers" in blob_low
            and "competitive wins outpaced lost business" in blob_low
            and re.search(r"\bturn positive\b[^.]{0,80}\bthird quarter\b", blob_low, re.I)
        ):
            out_rows.append(
                _row(
                    "SendTech bookings/subscribers improved.",
                    "Operating drivers / revenue inflection",
                    "SendTech bookings/subscribers",
                    "pbi_q1_2026_sendtech_inflection",
                    97.0,
                )
            )
            out_rows.append(
                _row(
                    "Presort wins outpaced losses; management expects YoY volume to turn positive by early Q3 if trends hold.",
                    "Operating drivers / revenue inflection",
                    "Presort volume inflection",
                    "pbi_q1_2026_presort_inflection",
                    97.0,
                )
            )

        return out_rows

    def _buyback_summary_specificity_score_local(summary_in: Any) -> float:
        summary_low = glx_normalize_text(str(summary_in or "")).lower()
        if not summary_low:
            return -1.0
        score = 0.0
        if "repurchased" in summary_low:
            score += 1.0
        if re.search(r"\bfor\s+\$", summary_low, re.I):
            score += 2.0
        if "with an average price of" in summary_low:
            score += 4.0
        if re.search(r"\bin q[1-4]\b", summary_low, re.I):
            score += 3.0
        if "since starting the program earlier this year" in summary_low:
            score -= 2.0
        return score

    def _doc_has_buyback_execution_signal_local(text_in: Any) -> bool:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return False
        if re.search(r"\brepurchas\w*\b", text, re.I):
            return True
        return bool(
            re.search(
                r"total number of shares purchased.{0,220}?average price paid per share",
                text,
                re.I,
            )
            or re.search(
                r"common stock purchases during the three months ended",
                text,
                re.I,
            )
        )

    best_doc_buyback_execution_doc_hit_cache: Dict[date, Dict[str, Any]] = {}

    def _best_doc_buyback_execution_doc_hit_local(qd_ref: Optional[date]) -> Dict[str, Any]:
        if not isinstance(qd_ref, date):
            return {}
        cached = best_doc_buyback_execution_doc_hit_cache.get(qd_ref)
        if cached is not None:
            return dict(cached)

        def _cache_doc_hit_local(hit_in: Dict[str, Any]) -> Dict[str, Any]:
            best_doc_buyback_execution_doc_hit_cache[qd_ref] = dict(hit_in or {})
            return dict(hit_in or {})

        ctx_ref_local = _current_ctx_ref()
        valuation_bundle = (
            dict(getattr(getattr(ctx_ref_local, "derived", None), "valuation_precompute_bundle", {}) or {})
            if ctx_ref_local is not None
            else {}
        )
        filing_docs_by_quarter = (
            dict(getattr(getattr(ctx_ref_local, "derived", None), "valuation_filing_docs_by_quarter", {}) or {})
            if ctx_ref_local is not None
            else {}
        )
        docs_by_quarter = dict(valuation_bundle.get("docs_by_quarter") or {})
        doc_rows: List[Dict[str, Any]] = []
        seen_doc_keys: set[str] = set()
        q_num = ((qd_ref.month - 1) // 3) + 1

        def _append_doc_row_local(path_in: Path) -> None:
            if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                return
            try:
                rec_key = str(path_in.resolve()).strip().lower()
            except Exception:
                rec_key = str(path_in).strip().lower()
            if not rec_key or rec_key in seen_doc_keys:
                return
            try:
                txt = _extract_valuation_filing_doc_text(path_in)
            except Exception:
                txt = _read_cached_doc_text(path_in)
            if not txt or not _doc_has_buyback_execution_signal_local(txt):
                return
            seen_doc_keys.add(rec_key)
            doc_rows.append({"path": str(path_in), "name": path_in.name, "text": txt})

        for rec in list(filing_docs_by_quarter.get(pd.Timestamp(qd_ref).normalize()) or []) + list(
            docs_by_quarter.get(pd.Timestamp(qd_ref).normalize()) or []
        ):
            rec_key = str(rec.get("path") or rec.get("name") or "").strip().lower()
            if not rec_key:
                rec_key = glx_dedup_text_key(str(rec.get("text") or ""))
            if not rec_key or rec_key in seen_doc_keys:
                continue
            seen_doc_keys.add(rec_key)
            doc_rows.append(dict(rec))
        q_token = qd_ref.strftime("%Y%m%d")
        direct_sec_cache_dirs: List[Path] = []
        seen_sec_dirs: set[str] = set()

        def _add_sec_dir_local(dir_in: Any) -> None:
            try:
                sec_dir = Path(dir_in).expanduser()
            except Exception:
                return
            if not _allow_repo_profile_cache_fallback() and _is_repo_profile_cache_path(sec_dir):
                return
            if not sec_dir.exists() or not sec_dir.is_dir():
                return
            try:
                sec_key = str(sec_dir.resolve()).lower()
            except Exception:
                sec_key = str(sec_dir).lower()
            if sec_key in seen_sec_dirs:
                return
            seen_sec_dirs.add(sec_key)
            direct_sec_cache_dirs.append(sec_dir)

        if cache_dir is not None:
            _add_sec_dir_local(cache_dir)
        for sec_cache_root in _cache_roots():
            _add_sec_dir_local(sec_cache_root)
        for root in material_roots:
            for sec_cache_dir in ticker_cache_roots_from_base_dir(root):
                _add_sec_dir_local(sec_cache_dir)
        if cache_dir is not None:
            cache_dir_path = Path(cache_dir)
            for ancestor in [cache_dir_path.parent, *cache_dir_path.parents]:
                for nm in [
                    str(profile_ticker or ticker or "").strip(),
                    str(profile_ticker or ticker or "").strip().upper(),
                    str(profile_ticker or ticker or "").strip().lower(),
                ]:
                    nm = str(nm or "").strip()
                    if not nm:
                        continue
                    _add_sec_dir_local(ancestor / "sec_cache" / nm)
                    _add_sec_dir_local(ancestor / nm / "sec_cache")
        for sec_cache_dir in direct_sec_cache_dirs:
            for pattern in [f"*{q_token}*.htm", f"*{q_token}*.html", f"*{q_token}*.xml"]:
                for path_in in sorted(sec_cache_dir.glob(pattern)):
                    _append_doc_row_local(path_in)
        if not doc_rows:
            return _cache_doc_hit_local({})

        def _doc_text_variants_local(rec_in: Dict[str, Any]) -> List[Tuple[str, str]]:
            variants: List[Tuple[str, str]] = []
            seen_keys: set[str] = set()

            def _add_variant(txt_in: Any, variant_name: str) -> None:
                norm_txt = glx_normalize_text(str(txt_in or ""))
                if not norm_txt:
                    return
                norm_key = glx_dedup_text_key(norm_txt)
                if not norm_key or norm_key in seen_keys:
                    return
                seen_keys.add(norm_key)
                variants.append((norm_txt, variant_name))

            _add_variant(rec_in.get("text"), "provided")
            src_path_str = str(rec_in.get("path") or "").strip()
            if src_path_str:
                try:
                    src_path = Path(src_path_str)
                except Exception:
                    src_path = None
                if src_path is not None and src_path.exists() and src_path.is_file():
                    try:
                        _add_variant(_extract_valuation_filing_doc_text(src_path), "valuation_extract")
                    except Exception:
                        pass
                    try:
                        raw_txt = _read_cached_doc_raw(src_path)
                    except Exception:
                        raw_txt = ""
                    if not raw_txt:
                        try:
                            raw_txt = src_path.read_text(encoding="utf-8", errors="ignore")
                        except Exception:
                            raw_txt = ""
                    if raw_txt:
                        try:
                            raw_plain = (
                                strip_html(raw_txt)
                                if str(src_path.suffix).lower() in {".htm", ".html", ".xml"}
                                else str(raw_txt)
                            )
                        except Exception:
                            raw_plain = str(raw_txt or "")
                        _add_variant(raw_plain, "raw_plain")
            return variants

        best_hit: Dict[str, Any] = {}
        best_score = -1.0
        for rec in doc_rows:
            name_low = str(rec.get("name") or rec.get("path") or "").lower()
            for txt, variant_name in _doc_text_variants_local(rec):
                if not txt or not _doc_has_buyback_execution_signal_local(txt):
                    continue
                summary = _compose_buyback_execution_summary_local(txt, qd_ref)
                parts = _extract_buyback_execution_components_local(txt, qd_ref)
                if not summary and parts.get("remaining_capacity") is None:
                    continue
                score = _buyback_summary_specificity_score_local(summary)
                if bool(parts.get("from_table")):
                    score += 12.0
                if bool(parts.get("quarter_scoped")):
                    score += 4.0
                if parts.get("avg_price") is not None:
                    score += 3.0
                if parts.get("shares") is not None:
                    score += 1.5
                if parts.get("amount") is not None:
                    score += 1.0
                if parts.get("remaining_capacity") is not None:
                    score += 2.5
                if variant_name == "valuation_extract":
                    score += 1.0
                elif variant_name == "raw_plain":
                    score += 0.5
                if "sec_cache" in name_low or name_low.endswith(".htm") or name_low.endswith(".html"):
                    score += 3.0
                if "_pbi-" in name_low or "10q" in name_low or "10k" in name_low:
                    score += 4.0
                if "press" in name_low or "earnings" in name_low or "ex99" in name_low:
                    score += 1.0
                explicit_q_match = re.search(r"\bin q([1-4])\b", summary, re.I)
                if explicit_q_match:
                    if int(explicit_q_match.group(1)) == q_num:
                        score += 10.0
                    else:
                        score -= 25.0
                if score > best_score:
                    best_score = score
                    best_hit = {
                        "summary": summary,
                        "parts": dict(parts),
                        "path": str(rec.get("path") or rec.get("name") or ""),
                    }
        return _cache_doc_hit_local(best_hit)

    def _best_doc_buyback_execution_summary_local(qd_ref: Optional[date]) -> str:
        return str((_best_doc_buyback_execution_doc_hit_local(qd_ref) or {}).get("summary") or "")

    def _best_doc_buyback_execution_components_local(qd_ref: Optional[date]) -> Dict[str, Any]:
        return dict((_best_doc_buyback_execution_doc_hit_local(qd_ref) or {}).get("parts") or {})

    def _capital_allocation_split_summaries_local(text_in: Any, qd_ref: Optional[date] = None) -> Dict[str, str]:
        text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
        if not text:
            return {}
        text_low = text.lower()
        new_program_match = re.search(
            r"\b(?:board(?:\s+of\s+directors)?[^.]{0,120}?)?(?:authorized|approved)\b[^.]{0,120}?"
            r"\bnew\b[^.]{0,80}?\$?\s*(\d+(?:\.\d+)?)\s*million\s+(?:share\s+repurchase|repurchase|buyback)\s+program\b",
            text,
            re.I,
        )
        auth_match = re.search(
            r"\b(?:repurchase|share repurchase|buyback)\s+authorization\s+(?:increased|expanded|raised)\s+by\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
            text,
            re.I,
        )
        if not auth_match:
            auth_match = re.search(
                r"\b(?:increased|increasing|raised|raising|updated)\b[^.]{0,60}?\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\b[^.]{0,40}?\bby\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
        auth_to_matches = list(
            re.finditer(
                r"\b(?:increasing|increased|raising|raised|updated)\b[^.]{0,60}?\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\b[^.]{0,40}?\bto\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
        )
        program_from_to_match = re.search(
            r"\b(?:increasing|increased|raising|raised|updated)\b[^.]{0,80}?\b(?:existing\s+)?(?:share\s+repurchase|repurchase|buyback)\s+program\b"
            r"[^.]{0,40}?\bfrom\s+\$?\s*(\d+(?:\.\d+)?)\s*million\b[^.]{0,40}?\bto\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
            text,
            re.I,
        )
        if not auth_to_matches:
            auth_to_matches = list(
                re.finditer(
                    r"\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\s+(?:to|at)\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                    text,
                    re.I,
                )
            )
        program_increase_context = bool(
            re.search(
                r"\b(?:authorized|approved)\b[^.]{0,160}?\b(?:additional\s+)?increase\s+in\s+the\s+program\s+to\b",
                text,
                re.I,
            )
        )
        if not auth_to_matches and (
            re.search(r"\b(repurchas\w*|buyback)\b", text_low, re.I)
            or program_increase_context
        ):
            auth_to_matches = list(
                re.finditer(
                    r"\bauthorized\b[^.]{0,180}?\b(?:increase|expanded|raised|program|share repurchase|buyback)\b[^.]{0,120}?\bto\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                    text,
                    re.I,
                )
            )
        if not auth_to_matches and (
            re.search(r"\b(repurchas\w*|buyback)\b", text_low, re.I)
            or program_increase_context
        ):
            auth_to_matches = list(
                re.finditer(
                    r"\b(?:additional\s+)?increase\s+in\s+the\s+program\s+to\s+\$?\s*(\d+(?:\.\d+)?)\s*million",
                    text,
                    re.I,
                )
            )
        auth_to_match = auth_to_matches[-1] if auth_to_matches else None
        if new_program_match and (
            re.search(
                r"\bexisting\b[^.]{0,100}?\b(?:share\s+repurchase|repurchase|buyback)\s+program\b",
                text,
                re.I,
            )
            or re.search(
                r"\bfrom\s+\$?\s*\d+(?:\.\d+)?\s*million\s+to\s+\$?\s*\d+(?:\.\d+)?\s*million\b",
                text,
                re.I,
            )
        ):
            new_program_match = None
        capacity_match = re.search(
            r"\$?\s*(\d+(?:\.\d+)?)\s*million\s+(?:in\s+)?capacity\s+remaining\s+under\s+the\s+authorization|"
            r"\$?\s*(\d+(?:\.\d+)?)\s*million\s+(?:of\s+)?remaining\s+capacity|"
            r"\bremaining\s+capacity\s+(?:of\s+)?\$?\s*(\d+(?:\.\d+)?)\s*million",
            text,
            re.I,
        )
        if not capacity_match:
            capacity_match = re.search(
                r"\b(?:repurchase|buyback)\s+(?:program\s+)?(?:capacity|authorization)\s+(?:remaining|left)\s+(?:at|of)?\s*\$?\s*(\d+(?:\.\d+)?)\s*million",
                text,
                re.I,
            )
        div_from_to_match = re.search(
            r"\b(?:we\s+)?(?:increased|raising|raised)\s+(?:our\s+)?quarterly\s+dividend\s+from\s+\$?\s*(\d+(?:\.\d+)?)\s+to\s+\$?\s*(\d+(?:\.\d+)?)\s+per\s+share",
            text,
            re.I,
        )
        if not div_from_to_match:
            div_from_to_match = re.search(
                r"\bboard approved a regular quarterly dividend of\s+\$?\s*(\d+(?:\.\d+)?)\s+per\s+share\b",
                text,
                re.I,
            )
        local_buyback_parts = _extract_buyback_execution_components_local(text, qd_ref)
        doc_buyback_parts = _best_doc_buyback_execution_components_local(qd_ref)
        capacity_val = next((grp for grp in capacity_match.groups() if grp), "") if capacity_match else ""
        capacity_amount: Optional[float] = None
        if capacity_val:
            try:
                capacity_amount = float(str(capacity_val).replace(",", "")) * 1_000_000.0
            except Exception:
                capacity_amount = None
        if capacity_amount is None:
            for parts_in in (local_buyback_parts, doc_buyback_parts):
                try:
                    parts_capacity = float(parts_in.get("remaining_capacity") or 0.0)
                except Exception:
                    parts_capacity = 0.0
                if parts_capacity > 0:
                    capacity_amount = parts_capacity
                    break
        auth_summary = ""
        prior_auth_to_amt = 0.0
        if len(auth_to_matches) >= 2:
            try:
                distinct_auth_to_vals = []
                for match in auth_to_matches:
                    amt = float(str(match.group(1) or "").replace(",", ""))
                    if amt > 0 and (not distinct_auth_to_vals or abs(distinct_auth_to_vals[-1] - amt) > 1e-9):
                        distinct_auth_to_vals.append(amt)
                if len(distinct_auth_to_vals) >= 2:
                    prior_auth_to_amt = float(distinct_auth_to_vals[-2])
            except Exception:
                prior_auth_to_amt = 0.0
        if program_from_to_match and prior_auth_to_amt <= 0:
            try:
                prior_auth_to_amt = float(str(program_from_to_match.group(1) or "").replace(",", ""))
            except Exception:
                prior_auth_to_amt = 0.0
        explicit_auth_change = bool(
            auth_match
            or new_program_match
            or program_from_to_match
            or len(auth_to_matches) >= 2
            or re.search(
                r"\b(?:increas(?:e|ed|ing)|raising|raised|expanded|updated|additional increase|increase in the program)\b"
                r"[^.]{0,160}?\b(?:share\s+repurchase|repurchase|buyback|program)\b",
                text,
                re.I,
            )
        )
        if auth_to_match and auth_match:
            try:
                auth_to_amt = float(auth_to_match.group(1))
                auth_by_amt = float(auth_match.group(1))
                auth_from_amt = auth_to_amt - auth_by_amt
            except Exception:
                auth_to_amt = 0.0
                auth_from_amt = 0.0
            if auth_to_amt > 0 and auth_from_amt > 0:
                auth_summary = _ensure_terminal_period(
                    f"Repurchase authorization increased to {_fmt_short_millions_note_local(auth_to_amt)}, "
                    f"up from {_fmt_short_millions_note_local(auth_from_amt)}"
                )
        if not auth_summary and auth_to_match and prior_auth_to_amt > 0:
            try:
                auth_to_amt = float(auth_to_match.group(1))
            except Exception:
                auth_to_amt = 0.0
            if auth_to_amt > prior_auth_to_amt:
                auth_summary = _ensure_terminal_period(
                    f"Repurchase authorization increased to {_fmt_short_millions_note_local(auth_to_amt)}, "
                    f"up from {_fmt_short_millions_note_local(prior_auth_to_amt)}"
                )
        if not auth_summary and program_from_to_match:
            try:
                auth_from_amt = float(str(program_from_to_match.group(1) or "").replace(",", ""))
                auth_to_amt = float(str(program_from_to_match.group(2) or "").replace(",", ""))
            except Exception:
                auth_from_amt = 0.0
                auth_to_amt = 0.0
            if auth_to_amt > 0 and auth_from_amt > 0 and auth_to_amt > auth_from_amt:
                auth_summary = _ensure_terminal_period(
                    f"Repurchase authorization increased to {_fmt_short_millions_note_local(auth_to_amt)}, "
                    f"up from {_fmt_short_millions_note_local(auth_from_amt)}"
                )
        if not auth_summary and new_program_match:
            auth_summary = _ensure_terminal_period(
                f"Repurchase authorization set at {_fmt_short_millions_note_local(new_program_match.group(1))}"
            )
        if not auth_summary:
            auth_parts: List[str] = []
            if auth_to_match and explicit_auth_change:
                auth_parts.append(f"Repurchase authorization increased to {_fmt_short_millions_note_local(auth_to_match.group(1))}")
            elif auth_match:
                auth_parts.append(f"Repurchase authorization increased by {_fmt_short_millions_note_local(auth_match.group(1))}")
            auth_summary = _ensure_terminal_period("; ".join(auth_parts)) if auth_parts else ""
        capacity_summary = ""
        if capacity_amount is not None and capacity_amount > 0:
            capacity_summary = _ensure_terminal_period(
                f"Remaining share repurchase capacity was {_fmt_short_money_value_local(capacity_amount)} at quarter-end"
            )
        dividend_summary = ""
        if div_from_to_match:
            if div_from_to_match.lastindex and div_from_to_match.lastindex >= 2:
                dividend_summary = capital_return_build_dividend_note(
                    current_per_share=float(div_from_to_match.group(2)),
                    previous_per_share=float(div_from_to_match.group(1)),
                )
            else:
                dividend_summary = capital_return_build_dividend_note(
                    current_per_share=float(div_from_to_match.group(1)),
                )
        buyback_summary = _compose_buyback_execution_summary_local(text, qd_ref)
        doc_buyback_summary = _best_doc_buyback_execution_summary_local(qd_ref)
        if _buyback_summary_specificity_score_local(doc_buyback_summary) > _buyback_summary_specificity_score_local(buyback_summary):
            buyback_summary = doc_buyback_summary
        if "$0.0m" in buyback_summary.lower():
            buyback_summary = ""
        if not buyback_summary:
            amount_only_share_match = re.search(
                r"\brepurchas\w*\s+\$?\s*(\d+(?:\.\d+)?)\s*million\s+in\s+shares?\b[^.]{0,180}?\b(?:during|in)\s+the\s+(first|second|third|fourth)\s+quarter\b",
                text,
                re.I,
            )
            if amount_only_share_match:
                quarter_anchor = {
                    "first": " in Q1",
                    "second": " in Q2",
                    "third": " in Q3",
                    "fourth": " in Q4",
                }.get(str(amount_only_share_match.group(2) or "").strip().lower(), "")
                buyback_summary = _ensure_terminal_period(
                    f"Repurchased {_fmt_short_millions_note_local(amount_only_share_match.group(1))} of shares{quarter_anchor}"
                )
        return {
            "authorization_capacity": auth_summary,
            "capacity_remaining": capacity_summary,
            "dividend_policy": dividend_summary,
            "buyback_execution": buyback_summary,
        }

    def _pbi_capital_allocation_split_summaries_local(
        text_in: Any,
        qd_ref: Optional[date] = None,
    ) -> Dict[str, str]:
        return _capital_allocation_split_summaries_local(text_in, qd_ref)

    def _pbi_explicit_note_split_variants_local(
        label: str,
        text_in: Any,
        qd_ref: Optional[date] = None,
    ) -> List[Dict[str, str]]:
        out_rows: List[Dict[str, str]] = []
        label_norm = str(label or "").strip()
        if label_norm == "Capital allocation / buyback":
            split_summaries = _pbi_capital_allocation_split_summaries_local(text_in, qd_ref)
            auth_summary = str(split_summaries.get("authorization_capacity") or "").strip()
            capacity_summary = str(split_summaries.get("capacity_remaining") or "").strip()
            dividend_summary = str(split_summaries.get("dividend_policy") or "").strip()
            buyback_summary = str(split_summaries.get("buyback_execution") or "").strip()
            if auth_summary:
                out_rows.append(
                    {
                        "subject_variant": "authorization_capacity",
                        "summary": auth_summary,
                        "theme_scope_key": "capital_allocation|authorization_capacity",
                    }
                )
            if capacity_summary:
                out_rows.append(
                    {
                        "subject_variant": "capacity_remaining",
                        "summary": capacity_summary,
                        "theme_scope_key": "capital_allocation|capacity_remaining",
                    }
                )
            if dividend_summary:
                out_rows.append(
                    {
                        "subject_variant": "dividend_policy",
                        "summary": dividend_summary,
                        "theme_scope_key": "capital_allocation|dividend_policy",
                    }
                )
            if buyback_summary:
                out_rows.append(
                    {
                        "subject_variant": "buyback_execution",
                        "summary": buyback_summary,
                        "theme_scope_key": "capital_allocation|buyback_execution",
                    }
                )
            return out_rows
        if label_norm in {"Adjusted EBIT / margin", "SendTech / Presort operating driver"}:
            compact_summary = _pbi_detail_preserving_note_summary_local(label_norm, str(text_in or ""))
            if not compact_summary or ";" not in compact_summary:
                return []
            parts = [glx_normalize_text(part).strip(" .;") for part in compact_summary.split(";")]
            for part in parts:
                if not part:
                    continue
                part_low = part.lower()
                if "gross margin expanded" in part_low or "margin expanded" in part_low:
                    out_rows.append(
                        {
                            "subject_variant": "gross_margin_driver",
                            "summary": _ensure_terminal_period(part),
                            "theme_scope_key": "margin_ebitda_cashflow|gross_margin_driver",
                        }
                    )
                elif "operating expenses declined" in part_low or "opex declined" in part_low:
                    out_rows.append(
                        {
                            "subject_variant": "opex_reduction_driver",
                            "summary": _ensure_terminal_period(part),
                            "theme_scope_key": "margin_ebitda_cashflow|opex_reduction_driver",
                        }
                    )
        return out_rows

    def _pbi_is_locked_capital_allocation_summary_local(summary_in: Any) -> bool:
        summary = glx_normalize_text(str(summary_in or ""))
        if not summary:
            return False
        return bool(re.search(r"\b(repurchase authorization|quarterly dividend|remaining capacity|capped call)\b", summary, re.I))

    def _sector_pack_keys_for_text_local(text_in: Any = "", extra_terms: Sequence[str] = tuple()) -> Tuple[str, ...]:
        text = glx_normalize_text(str(text_in or "")).lower()
        blob_parts = [glx_normalize_text(str(term or "")).lower() for term in extra_terms if str(term or "").strip()]
        if text:
            blob_parts.append(text)
        blob = " | ".join(x for x in blob_parts if x)
        packs: List[str] = []
        if re.search(r"\b(ethanol|biofuel|renewable fuel|45z|carbon capture|crush margin|sequestering)\b", blob, re.I):
            packs.append("biofuels")
        if re.search(r"\b(presort|mailing|shipping|postage|parcel|buyback|dividend|authorization|deleverag)\b", blob, re.I):
            packs.append("industrial_capital_return")
        if re.search(r"\b(convertible|capped call|net proceeds|notes due|use of proceeds|dilution)\b", blob, re.I):
            packs.append("capital_markets")
        ordered: List[str] = []
        for pack in packs:
            if pack not in ordered:
                ordered.append(pack)
        return tuple(ordered)

    def _profile_signal_terms_local(profile_in: Any) -> Tuple[str, ...]:
        if profile_in is None:
            return tuple()
        generic_terms = {
            "guidance",
            "target",
            "margin",
            "liquidity",
            "balance sheet",
            "cost reduction",
            "cost reductions",
            "cost savings",
            "deleveraging",
            "fully operational",
            "online",
            "ramping",
            "annualized savings",
            "adjusted ebitda target",
        }
        out_terms: List[str] = []
        for raw in getattr(profile_in, "industry_keywords", ()) or ():
            txt = glx_normalize_text(str(raw or "")).strip().lower()
            if txt and txt not in generic_terms:
                out_terms.append(txt)
        for raw in getattr(profile_in, "quarter_note_priority_terms", ()) or ():
            txt = glx_normalize_text(str(raw or "")).strip().lower()
            if txt and txt not in generic_terms:
                out_terms.append(txt)
        for raw in getattr(profile_in, "quarterly_segment_labels", ()) or ():
            txt = glx_normalize_text(str(raw or "")).strip().lower()
            if txt:
                out_terms.append(txt)
        for raw in getattr(profile_in, "annual_segment_labels", ()) or ():
            txt = glx_normalize_text(str(raw or "")).strip().lower()
            if txt:
                out_terms.append(txt)
        for _, alias_label in getattr(profile_in, "segment_alias_patterns", ()) or ():
            txt = glx_normalize_text(str(alias_label or "")).strip().lower()
            if txt:
                out_terms.append(txt)
        for _, alias_label in getattr(profile_in, "annual_segment_alias_patterns", ()) or ():
            txt = glx_normalize_text(str(alias_label or "")).strip().lower()
            if txt:
                out_terms.append(txt)
        ordered: List[str] = []
        for term in out_terms:
            if len(term) < 4:
                continue
            if term not in ordered:
                ordered.append(term)
        return tuple(ordered)

    def _profile_sector_pack_keys_local(profile_in: Any) -> Tuple[str, ...]:
        return _sector_pack_keys_for_text_local("", _profile_signal_terms_local(profile_in))

    def _text_contains_symbol_marker_local(text_in: str, symbol_in: str) -> bool:
        symbol = str(symbol_in or "").strip().upper()
        if not symbol:
            return False
        return bool(
            re.search(
                rf"\b(?:nasdaq|nyse|amex|nasdaq stock market)\s*[:\-]?\s*{re.escape(symbol)}\b",
                text_in,
                re.I,
            )
        )

    def _narrative_text_matches_current_company_local(path_in: Path, text_in: Any = "") -> bool:
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return False
        text_l = text.lower()
        path_name_l = str(path_in.name or "").lower()
        current_profile_ticker = str(getattr(company_profile, "ticker", "") or ticker or "").strip().upper()
        current_profile_terms = _profile_signal_terms_local(company_profile)
        current_profile_packs = set(_profile_sector_pack_keys_local(company_profile))
        text_packs = set(_sector_pack_keys_for_text_local(text))
        explicit_current = False
        if current_profile_ticker:
            explicit_current = current_profile_ticker.lower() in path_name_l
            if not explicit_current:
                explicit_current = _text_contains_symbol_marker_local(text, current_profile_ticker)

        current_hits = {
            term for term in current_profile_terms
            if len(term) >= 4 and term in text_l
        }

        for other_ticker, other_profile in COMPANY_PROFILES.items():
            other_symbol = str(other_ticker or "").strip().upper()
            if not other_symbol or other_symbol == current_profile_ticker:
                continue
            explicit_other = other_symbol.lower() in path_name_l or _text_contains_symbol_marker_local(text, other_symbol)
            if explicit_other and not explicit_current:
                return False
            other_terms = _profile_signal_terms_local(other_profile)
            other_hits = {
                term for term in other_terms
                if len(term) >= 4 and term in text_l
            }
            other_packs = set(_profile_sector_pack_keys_local(other_profile))
            if len(other_hits) >= 2 and not current_hits:
                if (text_packs & other_packs) and not (text_packs & current_profile_packs):
                    return False
        return True

    def _looks_like_xbrl_fact_blob_local(text_in: Any = "") -> bool:
        text = glx_normalize_text(str(text_in or ""))
        if not text or len(text) < 400:
            return False
        marker_hits = len(
            re.findall(
                r"\b(?:us-gaap|xbrli|iso4217|dei|srt|xlink|link):",
                text,
                re.I,
            )
        )
        if marker_hits >= 5:
            return True
        if marker_hits >= 2 and len(re.findall(r"\b20\d{2}(?:-\d{2}-\d{2})?\b", text)) >= 20:
            return True
        return False

    def _note_sector_pack_keys_local(text_in: Any = "") -> Tuple[str, ...]:
        profile_terms = list(_profile_signal_terms_local(company_profile))
        return _sector_pack_keys_for_text_local(text_in, profile_terms)


    def _iter_quarter_scoped_sec_cache_texts_local(min_year: int = 2024) -> List[Tuple[date, str, Path, str]]:
        out_records: List[Tuple[date, str, Path, str]] = []
        seen_paths: set[str] = set()
        sec_dirs: List[Path] = []
        seen_dirs: set[str] = set()

        def _add_sec_scan_dir(dir_in: Any) -> None:
            try:
                sec_dir = Path(dir_in).expanduser()
            except Exception:
                return
            if not sec_dir.exists() or not sec_dir.is_dir():
                return
            if not _path_belongs_to_ticker(sec_dir, ticker, ticker_roots):
                return
            try:
                dir_key = str(sec_dir.resolve()).lower()
            except Exception:
                dir_key = str(sec_dir).lower()
            if dir_key in seen_dirs:
                return
            seen_dirs.add(dir_key)
            sec_dirs.append(sec_dir)

        for root in material_roots:
            for sec_cache_dir in ticker_cache_roots_from_base_dir(root):
                _add_sec_scan_dir(sec_cache_dir)
        for sec_cache_dir in cache_roots:
            _add_sec_scan_dir(sec_cache_dir)
        if cache_dir is not None:
            try:
                cache_base = Path(cache_dir).expanduser()
            except Exception:
                cache_base = Path(cache_dir)
            ticker_names = {
                str(ticker or "").strip(),
                str(ticker or "").strip().upper(),
                str(profile_ticker or "").strip(),
                str(profile_ticker or "").strip().upper(),
            }
            for ancestor in [cache_base.parent, *list(cache_base.parents)[:4]]:
                for nm in ticker_names:
                    if not nm:
                        continue
                    _add_sec_scan_dir(ancestor / nm / "sec_cache")
                    _add_sec_scan_dir(ancestor / "sec_cache" / nm)
        for sec_cache_dir in sec_dirs:
            for path_in in _sec_cache_html_paths_local(sec_cache_dir):
                if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                    continue
                try:
                    key = str(path_in.resolve())
                except Exception:
                    key = str(path_in)
                if key in seen_paths:
                    continue
                joined = _read_cached_doc_text(path_in)
                if not joined:
                    try:
                        joined = path_in.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        joined = ""
                joined = glx_normalize_text(joined)
                if not joined:
                    continue
                if not _narrative_text_matches_current_company_local(path_in, joined):
                    continue
                q_raw = _infer_doc_quarter_local(path_in, joined)
                if q_raw not in quarters or not isinstance(q_raw, date) or q_raw.year < min_year:
                    continue
                source_type = "sec_cache_filing"
                if re.search(r"(press|earnings|ex99)", path_in.name, re.I):
                    source_type = "earnings_release"
                elif re.search(r"(ceoletter|annualletter|shareholderletter)", path_in.name, re.I):
                    source_type = "ceo_letter"
                seen_paths.add(key)
                out_records.append((q_raw, source_type, path_in, joined))
        return out_records



    _material_source_text_cache = __state.setdefault("material_source_text_cache", {})

    def _material_text_local(path_in: Path) -> str:
        key = _path_cache_key(path_in)
        cached = _material_source_text_cache.get(key)
        if cached is not None:
            return cached
        raw_txt = _read_material_text(path_in)
        if not raw_txt:
            try:
                raw_txt = path_in.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                raw_txt = ""
        txt = glx_normalize_text(raw_txt)
        _material_source_text_cache[key] = txt
        return txt

    def _management_text_windows_local(text_in: Any, *, max_sentences: int = 2) -> List[str]:
        txt = glx_normalize_text(str(text_in or ""))
        if not txt:
            return []
        sentences = [glx_normalize_text(s) for s in glx_split_sentences(txt) if glx_normalize_text(s)]
        if not sentences:
            return [txt]
        windows: List[str] = []
        seen_windows: set[str] = set()
        for idx in range(len(sentences)):
            for span in range(1, max_sentences + 1):
                if idx + span > len(sentences):
                    break
                window_txt = glx_normalize_text(" ".join(sentences[idx: idx + span]))
                if not window_txt or len(window_txt) < 40:
                    continue
                key = window_txt.lower()
                if key in seen_windows:
                    continue
                seen_windows.add(key)
                windows.append(window_txt)
        return windows or [txt]

    def _pattern_match_windows_local(
        text_in: Any,
        pattern_in: str,
        *,
        max_matches: int = 8,
    ) -> List[str]:
        txt = glx_normalize_text(str(text_in or ""))
        if not txt:
            return []
        windows: List[str] = []
        seen_windows: set[str] = set()
        for match in re.finditer(pattern_in, txt, re.I):
            snippet = glx_normalize_text(match.group(0))
            if not snippet:
                continue
            if len(snippet) < 48:
                start = max(0, match.start() - 180)
                end = min(len(txt), match.end() + 180)
                snippet = glx_normalize_text(txt[start:end])
            if not snippet:
                continue
            key = snippet.lower()
            if key in seen_windows:
                continue
            seen_windows.add(key)
            windows.append(snippet)
            if len(windows) >= max_matches:
                break
        return windows

    def _iter_quarter_scoped_material_texts_local(
        dir_specs: Sequence[Tuple[str, str]],
        *,
        min_year: int = 2024,
    ) -> List[Tuple[date, str, Path, str]]:
        alias_dirs: Dict[str, Tuple[str, ...]] = {
            "CEO letters": ("CEO_letters", "ceo_letters"),
        }
        out_records: List[Tuple[date, str, Path, str]] = []
        seen_paths: set[str] = set()
        for root in material_roots:
            for dir_name, source_type in dir_specs:
                candidate_subdirs: List[Path] = [root / dir_name]
                for alias_name in alias_dirs.get(dir_name, tuple()):
                    alias_path = root / alias_name
                    if alias_path not in candidate_subdirs:
                        candidate_subdirs.append(alias_path)
                for subdir in candidate_subdirs:
                    if not subdir.exists() or not subdir.is_dir():
                        continue
                    try:
                        files = sorted(
                            [p for p in subdir.iterdir() if p.is_file()],
                            key=lambda p: p.stat().st_mtime if p.exists() else 0,
                            reverse=True,
                        )[:40]
                    except Exception:
                        continue
                    for path_in in files:
                        if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                            continue
                        if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                            continue
                        try:
                            key = str(path_in.resolve())
                        except Exception:
                            key = str(path_in)
                        if key in seen_paths:
                            continue
                        joined = _material_text_local(path_in)
                        if not joined:
                            continue
                        if not _narrative_text_matches_current_company_local(path_in, joined):
                            continue
                        q_raw = (
                            _parse_quarter_from_filename(path_in.name)
                            or _parse_quarter_from_follow_text(joined)
                            or infer_quarter_end_from_text(joined)
                        )
                        if q_raw not in quarters or not isinstance(q_raw, date) or q_raw.year < min_year:
                            continue
                        seen_paths.add(key)
                        out_records.append((q_raw, source_type, path_in, joined))
        return out_records

    def _quarter_scoped_sec_cache_texts_by_quarter_local(
        *,
        min_year: int = 2024,
    ) -> Dict[date, List[Tuple[str, Path, str]]]:
        cache_key = f"__quarter_scoped_sec_cache_texts__|{int(min_year)}"
        cached = quarter_slice_cache.get(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]
        grouped: Dict[date, List[Tuple[str, Path, str]]] = {}
        for q_raw, source_type, path_in, joined in _iter_quarter_scoped_sec_cache_texts_local(min_year=min_year):
            grouped.setdefault(q_raw, []).append((source_type, path_in, joined))
        quarter_slice_cache[cache_key] = grouped  # type: ignore[assignment]
        return grouped

    def _quarter_scoped_material_texts_by_quarter_local(
        dir_specs: Sequence[Tuple[str, str]],
        *,
        min_year: int = 2024,
    ) -> Dict[date, List[Tuple[str, Path, str]]]:
        specs_key = "|".join(f"{str(a)}:{str(b)}" for a, b in dir_specs)
        cache_key = f"__quarter_scoped_material_texts__|{int(min_year)}|{specs_key}"
        cached = quarter_slice_cache.get(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]
        grouped: Dict[date, List[Tuple[str, Path, str]]] = {}
        for q_raw, source_type, path_in, joined in _iter_quarter_scoped_material_texts_local(
            dir_specs,
            min_year=min_year,
        ):
            grouped.setdefault(q_raw, []).append((source_type, path_in, joined))
        quarter_slice_cache[cache_key] = grouped  # type: ignore[assignment]
        return grouped


    def _pbi_bootstrap_note_rescue_rows() -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        if best_by_key:
            return rows_out
        if not (is_pbi_profile and isinstance(quarter_notes, pd.DataFrame) and not quarter_notes.empty):
            return rows_out
        rescue_quarter_col = _resolve_col(quarter_notes, ["quarter", "created_quarter", "first_seen_quarter"])
        if not rescue_quarter_col:
            return rows_out
        _pbi_bootstrap_guidance_labels = {
            "Adjusted EBIT guidance",
            "Revenue guidance",
            "EPS guidance",
            "FCF target",
            "Cost savings target",
            "Deleveraging target",
        }
        _pbi_bootstrap_allowed_labels = {
            "Adjusted EBIT guidance",
            "Revenue guidance",
            "EPS guidance",
            "FCF target",
            "Cost savings target",
            "Deleveraging target",
            "Adjusted EBIT / margin",
            "FCF improvement",
            "PB Bank liquidity release",
            "Capital allocation / buyback",
            "SendTech / Presort operating driver",
            "Deleveraging / liquidity",
            "Debt reduction",
            "Revenue / volume",
            "Strategic milestone",
        }
        for rec in quarter_notes.to_dict("records"):
            q_ts = pd.to_datetime(rec.get(rescue_quarter_col), errors="coerce")
            if pd.isna(q_ts):
                continue
            q_raw = pd.Timestamp(q_ts).to_period("Q").end_time.date()
            if q_raw not in quarters:
                continue
            detail_rescue_blob = " | ".join(
                [
                    str(rec.get("comment_full_text") or ""),
                    str(rec.get("evidence_snippet") or ""),
                    str(rec.get("claim") or ""),
                    str(rec.get("note") or ""),
                ]
            ).strip()
            if not detail_rescue_blob:
                continue
            raw_metric_rescue = str(rec.get("metric_ref") or rec.get("metric") or rec.get("metric_tag") or "").strip()
            label_rescue = _classify_pbi_metric_label(" | ".join([raw_metric_rescue, detail_rescue_blob]), raw_metric_rescue)
            if not label_rescue or label_rescue not in _pbi_bootstrap_allowed_labels:
                continue
            target_rescue = _extract_pbi_target_display(detail_rescue_blob, label_rescue or raw_metric_rescue)
            compact_rescue = ""
            bucket_rescue = str(rec.get("category") or "Results / drivers")
            if label_rescue in _pbi_bootstrap_guidance_labels:
                if not _pbi_target_display_ok(target_rescue):
                    continue
                compact_rescue = _pbi_guidance_self_contained_summary(
                    label_rescue,
                    target_rescue,
                    detail_rescue_blob,
                    period_label=_pbi_guidance_period_label_from_text(detail_rescue_blob),
                )
                bucket_rescue = "Guidance / outlook"
            else:
                compact_rescue = _pbi_detail_preserving_note_summary_local(label_rescue, detail_rescue_blob, q_raw)
                if not compact_rescue:
                    compact_rescue = _pbi_contextual_note_summary_local(label_rescue, q_raw, detail_rescue_blob)
                if not compact_rescue:
                    continue
                if label_rescue in {"Debt reduction", "Capital allocation / buyback", "PB Bank liquidity release", "Deleveraging / liquidity"}:
                    bucket_rescue = "Cash / liquidity / leverage"
                elif label_rescue in {"Adjusted EBIT / margin", "FCF improvement", "SendTech / Presort operating driver"}:
                    bucket_rescue = "Better / worse vs prior"
            rows_out.append(
                {
                    "quarter": q_raw,
                    "bucket": bucket_rescue,
                    "text_full": glx_normalize_text(str(rec.get("note") or rec.get("claim") or detail_rescue_blob)),
                    "comment_full_text": detail_rescue_blob,
                    "score": float(rec.get("score") or 0.0) + 6.0,
                    "candidate_type": "pbi_bootstrap_note_rescue",
                    "metric_tag": raw_metric_rescue,
                    "metric_canon": raw_metric_rescue,
                    "_metric_display": label_rescue,
                    "_pbi_compact_note": compact_rescue,
                    "_render_summary": compact_rescue,
                    "note_id": str(rec.get("note_id") or hashlib.sha1(f"{q_raw}|pbi_bootstrap_note_rescue|{detail_rescue_blob}".encode("utf-8")).hexdigest()[:12]),
                    "source": {
                        "source_type": str(rec.get("doc_type") or rec.get("source_type") or ""),
                        "doc": str(rec.get("doc") or ""),
                        "form": str(rec.get("form") or ""),
                    },
                }
            )
        return rows_out

    def _pbi_local_buyback_table_seed_rows() -> List[Dict[str, Any]]:
        if not is_pbi_profile:
            return []
        seed_dirs: List[Path] = []
        seen_dirs: set[str] = set()

        def _add_seed_dir(dir_in: Any) -> None:
            try:
                sec_dir = Path(dir_in).expanduser()
            except Exception:
                return
            if not sec_dir.exists() or not sec_dir.is_dir():
                return
            if not _allow_repo_profile_cache_fallback() and _is_repo_profile_cache_path(sec_dir):
                return
            if not _path_belongs_to_ticker(sec_dir, ticker, ticker_roots):
                return
            try:
                key = str(sec_dir.resolve()).lower()
            except Exception:
                key = str(sec_dir).lower()
            if key in seen_dirs:
                return
            seen_dirs.add(key)
            seed_dirs.append(sec_dir)

        for root in material_roots:
            for sec_cache_dir in ticker_cache_roots_from_base_dir(root):
                _add_seed_dir(sec_cache_dir)
        if cache_dir is not None:
            try:
                cache_base = Path(cache_dir).expanduser()
            except Exception:
                cache_base = Path(cache_dir)
            for ancestor in [cache_base, cache_base.parent, *cache_base.parents]:
                _add_seed_dir(ancestor / "sec_cache")
                for nm in {
                    str(ticker or "").strip(),
                    str(ticker or "").strip().upper(),
                    str(profile_ticker or "").strip(),
                    str(profile_ticker or "").strip().upper(),
                }:
                    if nm:
                        _add_seed_dir(ancestor / "sec_cache" / nm)
                        _add_seed_dir(ancestor / nm / "sec_cache")

        rows_out: List[Dict[str, Any]] = []
        seen_paths: set[Tuple[date, str]] = set()
        for qd_seed in quarters:
            if not isinstance(qd_seed, date):
                continue
            q_token = qd_seed.strftime("%Y%m%d")
            for sec_dir in seed_dirs:
                path_candidates: List[Path] = []
                for pattern in [f"*{q_token}*.htm", f"*{q_token}*.html", f"*{q_token}*.xml"]:
                    try:
                        path_candidates.extend(sorted(sec_dir.glob(pattern)))
                    except Exception:
                        continue
                for path_in in path_candidates:
                    if not path_in.exists() or not path_in.is_file():
                        continue
                    if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                        continue
                    try:
                        path_key = str(path_in.resolve()).lower()
                    except Exception:
                        path_key = str(path_in).lower()
                    seen_key = (qd_seed, path_key)
                    if seen_key in seen_paths:
                        continue
                    seen_paths.add(seen_key)
                    try:
                        raw_txt = path_in.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        raw_txt = ""
                    if not raw_txt:
                        try:
                            raw_txt = _read_cached_doc_raw(path_in)
                        except Exception:
                            raw_txt = ""
                    parts = _extract_buyback_table_execution_local(raw_txt, qd_seed)
                    if not parts or parts.get("amount") is None:
                        continue
                    summary = _compose_buyback_execution_summary_local(raw_txt, qd_seed)
                    if not summary:
                        shares_val = parts.get("shares")
                        amount_val = parts.get("amount")
                        avg_price_val = parts.get("avg_price")
                        anchor = str(parts.get("anchor") or f" in Q{((qd_seed.month - 1) // 3) + 1}")
                        if shares_val is not None and avg_price_val is not None:
                            summary = _ensure_terminal_period(
                                f"Repurchased {_fmt_note_share_count_local(shares_val)} "
                                f"for {_fmt_short_money_value_local(float(amount_val))} "
                                f"with an average price of ${float(avg_price_val):.2f}/share{anchor}"
                            )
                        elif shares_val is not None:
                            summary = _ensure_terminal_period(
                                f"Repurchased {_fmt_note_share_count_local(shares_val)} "
                                f"for {_fmt_short_money_value_local(float(amount_val))}{anchor}"
                            )
                    summary = glx_normalize_text(summary)
                    if not summary:
                        continue
                    rows_out.append(
                        {
                            "quarter": qd_seed,
                            "bucket": "Capital allocation / shareholder returns",
                            "text_full": summary,
                            "comment_full_text": summary,
                            "score": 99.0,
                            "candidate_type": "pbi_doc_table_buyback_seed",
                            "metric_tag": "Capital allocation / buyback|buyback_execution",
                            "metric_canon": "Capital allocation / buyback|buyback_execution",
                            "_metric_display": "Capital allocation / buyback",
                            "_pbi_compact_note": summary,
                            "_render_summary": summary,
                            "_render_summary_locked": True,
                            "_split_focus": "buyback_execution",
                            "_force_note_passthrough": True,
                            "_suppress_change_badge": True,
                            "_theme_scope_key": "capital_allocation|buyback_execution",
                            "note_id": hashlib.sha1(
                                f"{qd_seed}|pbi_doc_table_buyback_seed|{summary}|{path_key}".encode("utf-8")
                            ).hexdigest()[:12],
                            "source": {
                                "source_type": "sec_cache_filing",
                                "doc": str(path_in),
                                "form": "",
                            },
                        }
                    )
        return rows_out


    def _anf_source_note_rescue_rows() -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        if not is_anf_profile:
            return rows_out
        try:
            latest_q_for_rescue = max([q for q in quarters if isinstance(q, date)]) if quarters else None
        except Exception:
            latest_q_for_rescue = None
        if latest_q_for_rescue is None:
            return rows_out
        anf_metric_priority = {
            "brand_family_momentum": ("Results / drivers / better vs prior", "Brand-family momentum", 120.0),
            "digital_omnichannel": ("Results / drivers / better vs prior", "Digital / omnichannel", 119.0),
            "inventory_quality": ("Inventory / liquidity", "Inventory quality", 118.0),
            "guidance_margin_bridge": ("Guidance / outlook", "FY2026 margin bridge", 117.0),
            "buyback_bridge": ("Capital allocation / shareholder returns", "Buyback bridge", 116.0),
        }
        raw_q = df.copy()
        raw_q["_q_date"] = pd.to_datetime(raw_q[q_col], errors="coerce").dt.date
        raw_q = raw_q[raw_q["_q_date"].eq(latest_q_for_rescue)].copy()
        if raw_q.empty:
            return rows_out
        raw_q["_metric_ref"] = raw_q[metric_col].astype(str).str.strip() if metric_col else ""
        raw_q["_sev_score"] = pd.to_numeric(raw_q[sev_score_col], errors="coerce").fillna(0.0) if sev_score_col else 0.0
        for metric_ref, (bucket, display, score_override) in anf_metric_priority.items():
            sub = raw_q[raw_q["_metric_ref"].eq(metric_ref)].copy()
            if sub.empty:
                continue
            sub = sub.sort_values("_sev_score", ascending=False, kind="stable")
            rec = sub.iloc[0].to_dict()
            note_txt = glx_normalize_text(str(rec.get(note_col) or rec.get(claim_col) or ""))
            if not note_txt:
                continue
            source_doc = str(rec.get("source_doc") or rec.get("doc") or "")
            rows_out.append(
                {
                    "quarter": latest_q_for_rescue,
                    "bucket": bucket,
                    "text_full": note_txt,
                    "comment_full_text": note_txt,
                    "score": max(float(rec.get("_sev_score") or 0.0), score_override),
                    "candidate_type": "anf_source_note_rescue",
                    "metric_tag": metric_ref,
                    "metric_canon": metric_ref,
                    "_metric_display": display,
                    "_render_summary": note_txt,
                    "_render_summary_locked": True,
                    "_split_focus": metric_ref,
                    "_force_note_passthrough": True,
                    "_suppress_change_badge": False,
                    "_theme_scope_key": f"anf|{metric_ref}",
                    "note_id": str(rec.get(note_id_col) or hashlib.sha1(f"{latest_q_for_rescue}|anf_source_note_rescue|{metric_ref}|{note_txt}".encode("utf-8")).hexdigest()[:12]) if note_id_col else hashlib.sha1(f"{latest_q_for_rescue}|anf_source_note_rescue|{metric_ref}|{note_txt}".encode("utf-8")).hexdigest()[:12],
                    "source": {
                        "source_type": str(rec.get("source_type") or ""),
                        "doc": source_doc,
                        "form": "",
                    },
                    "change_badge": "NEW",
                }
            )
        return rows_out

    def _fmt_guidance_val(hit: Dict[str, Any]) -> str:
        kind = str(hit.get("kind") or "")
        unit = str(hit.get("unit") or "")
        if kind == "range" and hit.get("value_low") is not None and hit.get("value_high") is not None:
            lo = float(hit.get("value_low"))
            hi = float(hit.get("value_high"))
            if unit == "$m":
                return f"${lo/1e6:,.1f}m-${hi/1e6:,.1f}m"
            if unit == "$":
                return f"${lo:,.2f}-${hi:,.2f}"
            if unit == "%":
                return f"{lo:.1f}%–{hi:.1f}%"
            if unit == "x":
                return f"{lo:.2f}x-{hi:.2f}x"
            return f"{lo:,.2f}-{hi:,.2f}"
        if kind == "point" and hit.get("value_point") is not None:
            v = float(hit.get("value_point"))
            if unit == "$m":
                return f"${v/1e6:,.1f}m"
            if unit == "$":
                return f"${v:,.2f}"
            if unit == "%":
                return f"{v:.1f}%"
            if unit == "x":
                return f"{v:.2f}x"
            return f"{v:,.2f}"
        return ""

    def _build_guidance_summary_rows(qd: date, q_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not _is_fy_block(qd):
            return []

        def _fmt_item_value(item: Dict[str, Any]) -> str:
            unit = str(item.get("unit") or "")
            kind = str(item.get("kind") or "")
            lo = item.get("low")
            hi = item.get("high")
            vp = item.get("value")
            try:
                if kind == "range" and lo is not None and hi is not None:
                    lo_f = float(lo)
                    hi_f = float(hi)
                    if unit == "$m":
                        return f"${lo_f/1e6:,.1f}m-${hi_f/1e6:,.1f}m"
                    if unit == "$":
                        return f"${lo_f:,.2f}-${hi_f:,.2f}"
                    if unit == "%":
                        return f"{lo_f:.1f}%–{hi_f:.1f}%"
                    if unit == "x":
                        return f"{lo_f:.2f}x-{hi_f:.2f}x"
                    return f"{lo_f:,.2f}-{hi_f:,.2f}"
                if kind == "point" and vp is not None:
                    vv = float(vp)
                    if unit == "$m":
                        return f"${vv/1e6:,.1f}m"
                    if unit == "$":
                        return f"${vv:,.2f}"
                    if unit == "%":
                        return f"{vv:.1f}%"
                    if unit == "x":
                        return f"{vv:.2f}x"
                    return f"{vv:,.2f}"
            except Exception:
                return ""
            return ""

        wanted_period = ["Revenue", "Adj EBITDA", "Adj EBIT", "Adj EPS", "FCF", "Capex"]
        target_year = int(qd.year) + 1
        best_period: Dict[str, Tuple[str, float, Dict[str, Any], str]] = {}
        runrate_rows: List[Dict[str, Any]] = []
        legacy_program_rows: List[Tuple[str, float, Dict[str, Any], str]] = []
        asof_ord = (int(qd.year) * 4) + (((int(qd.month) - 1) // 3) + 1)

        # Prefer normalized guidance snapshot from valuation (already deduped/parsed).
        gstore = ui_state.get("guidance_snapshot_by_q", {}) if isinstance(ui_state, dict) else {}
        qkey = str(qd)
        g_items = gstore.get(qkey) if isinstance(gstore, dict) else None
        if isinstance(g_items, list) and g_items:
            for gi in g_items:
                metric_name = str(gi.get("metric") or "")
                period_norm = str(gi.get("period_norm") or "UNK")
                period_txt = str(gi.get("period") or "")
                gtype = str(gi.get("guidance_type") or "")
                is_target_period_guidance = (
                    period_norm in {f"FY{target_year}", "FY+1"}
                    or bool(re.search(rf"(?<!\d){target_year}(?!\d)", period_txt))
                    or bool(re.search(rf"(?<!\d){target_year}(?!\d)", str(gi.get('text') or "")))
                )
                val_txt = _fmt_item_value(gi)
                if not val_txt:
                    continue
                score = float(gi.get("score") or 0.0)
                if metric_name in wanted_period and gtype == "period" and is_target_period_guidance:
                    prev = best_period.get(metric_name)
                    if prev is None or score > prev[1]:
                        best_period[metric_name] = (val_txt, score, dict(gi.get("source") or {}), period_norm)
                if metric_name == "Cost savings" and gtype in {"run-rate", "ongoing"}:
                    fs = str(gi.get("first_seen_quarter_end") or "")
                    ls_num = str(
                        gi.get("last_seen_numeric_quarter_end")
                        or gi.get("last_numeric_quarter")
                        or ""
                    )
                    ls_txt = str(
                        gi.get("last_seen_text_quarter_end")
                        or gi.get("last_seen_quarter_end")
                        or gi.get("last_mentioned_quarter")
                        or ""
                    )
                    kind_g = str(gi.get("kind") or "").lower()
                    evidence_quote_g = str(gi.get("evidence_quote") or gi.get("text") or "")
                    strict_numeric_provenance = _cost_savings_numeric_provenance(evidence_quote_g)
                    has_numeric = bool(
                        kind_g in {"range", "point", "qualitative_range"}
                        and (
                            gi.get("low") is not None
                            or gi.get("high") is not None
                            or gi.get("value") is not None
                        )
                    )
                    if not has_numeric and strict_numeric_provenance:
                        has_numeric = _has_numeric_range_or_point(str(gi.get("text") or ""))
                    if has_numeric and not strict_numeric_provenance:
                        has_numeric = False
                        ui_info_rows.append(
                            {
                                "quarter": qd,
                                "metric": "Quarter_Notes_UI",
                                "severity": "info",
                                "message": "suppressed_numeric_target=missing_exact_cost_savings_range_quote",
                                "source": str(dict(gi.get("source") or {}).get("doc") or ""),
                            }
                        )
                    ls_num_use = ls_num or (str(gi.get("last_mentioned_quarter") or "") if has_numeric else "")
                    ls_num_ts = pd.to_datetime(ls_num_use, errors="coerce")
                    ls_num_ord = None
                    if pd.notna(ls_num_ts):
                        ls_num_ord = (int(ls_num_ts.year) * 4) + (((int(ls_num_ts.month) - 1) // 3) + 1)
                    recent_numeric = bool(ls_num_ord is not None and (asof_ord - int(ls_num_ord)) <= 2)
                    explicit_numeric_this_q = bool(
                        has_numeric
                        and (
                            str(gi.get("as_of_quarter") or "") == qkey
                            or str(gi.get("last_mentioned_quarter") or "") == qkey
                            or ls_num_use == qkey
                        )
                    )
                    if has_numeric and (recent_numeric or explicit_numeric_this_q) and val_txt:
                        runrate_rows.append(
                            {
                                "value_text": val_txt,
                                "score": score,
                                "source": dict(gi.get("source") or {}),
                                "first_seen": fs,
                                "last_seen": ls_num_use or ls_txt,
                                "guidance_type": gtype,
                                "recent_numeric": bool(recent_numeric),
                                "explicit_numeric_this_q": bool(explicit_numeric_this_q),
                            }
                        )
                    else:
                        recent_text = False
                        ls_txt_ts = pd.to_datetime(ls_txt, errors="coerce")
                        if pd.notna(ls_txt_ts):
                            ls_txt_ord = (int(ls_txt_ts.year) * 4) + (((int(ls_txt_ts.month) - 1) // 3) + 1)
                            recent_text = (asof_ord - int(ls_txt_ord)) <= 2
                        if recent_text:
                            num_lbl = _q_label(ls_num_use) if ls_num_use else "N/A"
                            legacy_text = (
                                "Cost savings initiatives mentioned "
                                f"(legacy target not reiterated; numeric target last stated {num_lbl})"
                            )
                            legacy_program_rows.append((legacy_text, score, dict(gi.get("source") or {}), gtype or "ongoing"))
                            ui_info_rows.append(
                                {
                                    "quarter": qd,
                                    "metric": "Quarter_Notes_UI",
                                    "severity": "info",
                                    "message": "suppressed_numeric_target=stale_numeric_mention_only_text",
                                    "source": str(dict(gi.get("source") or {}).get("doc") or ""),
                                }
                            )

        # Fallback: parse from already-selected quarter notes guidance lines.
        if not best_period:
            for it in q_items:
                if str(it.get("bucket") or "") != "Guidance / outlook":
                    continue
                txt = str(it.get("text_full") or "")
                if not txt:
                    continue
                p_label, p_key = glx_normalize_period(txt, qd)
                is_target_period = (
                    str(p_key) in {f"FY{target_year}", "FY+1"}
                    or bool(re.search(rf"(?<!\d){target_year}(?!\d)", txt))
                    or bool(re.search(r"\b(next year|next fiscal year)\b", txt, re.I))
                )
                if not is_target_period:
                    continue
                for hit in glx_extract_numeric_patterns(txt):
                    m = str(hit.get("metric_canon") or "")
                    if m not in wanted_period:
                        continue
                    val_txt = _fmt_guidance_val(hit)
                    if not val_txt:
                        continue
                    score = float(it.get("score") or 0.0)
                    prev = best_period.get(m)
                    if prev is None or score > prev[1]:
                        best_period[m] = (val_txt, score, dict(it.get("source") or {}), str(p_label or ""))

        out_rows: List[Dict[str, Any]] = []
        if len(best_period) < 2:
            return out_rows

        label_map = {
            "Revenue": "Rev",
            "Adj EBITDA": "Adj EBITDA",
            "Adj EBIT": "Adj EBIT",
            "Adj EPS": "Adj EPS",
            "FCF": "FCF",
            "Capex": "Capex",
        }
        pieces: List[str] = []
        for metric_name in wanted_period:
            if metric_name not in best_period:
                continue
            pieces.append(f"{label_map.get(metric_name, metric_name)} {best_period[metric_name][0]}")
        if not pieces:
            return out_rows
        src0 = next(iter(best_period.values()))[2]
        summary_txt = f"[FY{target_year} guidance] " + "; ".join(pieces)
        note_id = hashlib.sha1(f"{qd}|guidance_summary|{summary_txt}".encode("utf-8")).hexdigest()[:12]
        out_rows.append({
            "quarter": qd,
            "bucket": "Guidance / outlook",
            "text_full": summary_txt,
            "score": 999.0,
            "metric_tag": "Guidance",
            "metric_canon": "Guidance",
            "doc_priority": 100,
            "period_key": f"FY{target_year}",
            "period_label": f"FY {target_year}",
            "source": src0,
            "severity": "info",
            "sev_score": pd.NA,
            "metric_value": pd.NA,
            "note_id": note_id,
            "as_of_quarter_end": str(qd),
            "source_doc_end": str(qd),
            "source_filed_date": pd.NaT,
            "first_seen_quarter_end": str(qd),
            "last_seen_quarter_end": str(qd),
            "referenced_years": [target_year],
            "has_forward_intent": True,
            "has_period_anchor": True,
            "target_period_norm": f"FY{target_year}",
            "guidance_type": "period",
            "anchor_year": target_year,
            "year_pair": None,
            "is_year_comparison": False,
            "tense_hint": "future",
            "backfill_label": "",
            "source_quarter_end": str(qd),
            "mention_kind": "numeric",
            "has_numeric_range_or_point": True,
            "last_seen_numeric_quarter_end": str(qd),
            "last_seen_text_quarter_end": str(qd),
        })

        runrate_program_added = False
        if runrate_rows:
            best_run = sorted(
                runrate_rows,
                key=lambda x: (-float(x.get("score") or 0.0), str(x.get("value_text") or "")),
            )[0]
            run_txt = str(best_run.get("value_text") or "")
            run_src = dict(best_run.get("source") or {})
            fs_q = str(best_run.get("first_seen") or "")
            ls_q = str(best_run.get("last_seen") or "")
            run_type = str(best_run.get("guidance_type") or "run-rate")
            recent_numeric = bool(best_run.get("recent_numeric"))
            explicit_numeric_this_q = bool(best_run.get("explicit_numeric_this_q"))
            fs_lbl = _q_label(fs_q) if fs_q else "N/A"
            ls_lbl = _q_label(ls_q) if ls_q else "N/A"
            show_in_guidance = bool(recent_numeric and explicit_numeric_this_q)
            if show_in_guidance:
                run_line = (
                    f"[Run-rate | stated {fs_lbl} | last seen {ls_lbl}] "
                    f"Cost savings {run_txt} (exact range quote in comment)"
                )
                run_id = hashlib.sha1(f"{qd}|guidance_runrate|{run_line}".encode("utf-8")).hexdigest()[:12]
                out_rows.append(
                    {
                        "quarter": qd,
                        "bucket": "Guidance / outlook",
                        "text_full": run_line,
                        "score": 995.0,
                        "metric_tag": "Cost savings",
                        "metric_canon": "Cost savings",
                        "doc_priority": 95,
                        "period_key": "RUNRATE",
                        "period_label": "Run-rate",
                        "source": run_src,
                        "severity": "info",
                        "sev_score": pd.NA,
                        "metric_value": pd.NA,
                        "note_id": run_id,
                        "as_of_quarter_end": str(qd),
                        "source_doc_end": str(qd),
                        "source_filed_date": pd.NaT,
                        "first_seen_quarter_end": fs_q or str(qd),
                        "last_seen_quarter_end": ls_q or str(qd),
                        "referenced_years": [target_year],
                        "has_forward_intent": True,
                        "has_period_anchor": True,
                        "target_period_norm": "RUNRATE",
                        "guidance_type": run_type or "run-rate",
                        "anchor_year": target_year,
                        "year_pair": None,
                        "is_year_comparison": False,
                        "tense_hint": "future",
                        "backfill_label": "",
                        "source_quarter_end": str(qd),
                        "mention_kind": "numeric",
                        "has_numeric_range_or_point": True,
                        "last_seen_numeric_quarter_end": ls_q or str(qd),
                        "last_seen_text_quarter_end": ls_q or str(qd),
                    }
                )
            else:
                prog_line = (
                    f"[Program | run-rate | stated {fs_lbl} | last seen {ls_lbl}] "
                    f"Cost savings target {run_txt}; implementation cadence monitored."
                )
                prog_id = hashlib.sha1(f"{qd}|program_runrate|{prog_line}".encode("utf-8")).hexdigest()[:12]
                out_rows.append(
                    {
                        "quarter": qd,
                        "bucket": "Programs / initiatives",
                        "text_full": prog_line,
                        "score": 940.0,
                        "metric_tag": "Program",
                        "metric_canon": "Cost savings",
                        "doc_priority": 92,
                        "period_key": "RUNRATE",
                        "period_label": "Run-rate",
                        "source": run_src,
                        "severity": "info",
                        "sev_score": pd.NA,
                        "metric_value": pd.NA,
                        "note_id": prog_id,
                        "as_of_quarter_end": str(qd),
                        "source_doc_end": str(qd),
                        "source_filed_date": pd.NaT,
                        "first_seen_quarter_end": fs_q or str(qd),
                        "last_seen_quarter_end": ls_q or str(qd),
                        "last_seen_numeric_quarter_end": ls_q or str(qd),
                        "last_seen_text_quarter_end": ls_q or str(qd),
                        "referenced_years": [target_year],
                        "has_forward_intent": True,
                        "has_period_anchor": True,
                        "target_period_norm": "RUNRATE",
                        "guidance_type": run_type or "run-rate",
                        "anchor_year": target_year,
                        "year_pair": None,
                        "is_year_comparison": False,
                        "tense_hint": "future",
                        "backfill_label": "",
                        "source_quarter_end": str(qd),
                        "mention_kind": "numeric",
                        "has_numeric_range_or_point": True,
                        "candidate_type": "program_line",
                    }
                )
                runrate_program_added = True
        if legacy_program_rows and not runrate_rows:
            best_legacy = sorted(legacy_program_rows, key=lambda x: (-x[1], x[0]))[0]
            legacy_txt, legacy_score, legacy_src, legacy_type = best_legacy
            legacy_id = hashlib.sha1(f"{qd}|legacy_program|{legacy_txt}".encode("utf-8")).hexdigest()[:12]
            out_rows.append(
                {
                    "quarter": qd,
                    "bucket": "Programs / initiatives",
                    "text_full": f"[Program | legacy target not reiterated] {legacy_txt}",
                    "score": max(880.0, float(legacy_score)),
                    "metric_tag": "Program",
                    "metric_canon": "Cost savings",
                    "doc_priority": 90,
                    "period_key": "RUNRATE",
                    "period_label": "Run-rate",
                    "source": legacy_src,
                    "severity": "info",
                    "sev_score": pd.NA,
                    "metric_value": pd.NA,
                    "note_id": legacy_id,
                    "as_of_quarter_end": str(qd),
                    "source_doc_end": str(qd),
                    "source_filed_date": pd.NaT,
                    "first_seen_quarter_end": str(qd),
                    "last_seen_quarter_end": str(qd),
                    "last_seen_numeric_quarter_end": "",
                    "last_seen_text_quarter_end": str(qd),
                    "referenced_years": [target_year],
                    "has_forward_intent": True,
                    "has_period_anchor": True,
                    "target_period_norm": "RUNRATE",
                    "guidance_type": legacy_type or "ongoing",
                    "anchor_year": target_year,
                    "year_pair": None,
                    "is_year_comparison": False,
                    "tense_hint": "future",
                    "backfill_label": "",
                    "source_quarter_end": str(qd),
                    "mention_kind": "text",
                    "has_numeric_range_or_point": False,
                }
            )
        return out_rows


    def append_guidance_summary_rows(records: List[Dict[str, Any]], quarters: Sequence[Any]) -> QuarterNotesUiGuidanceSummaryResult:
        appended_count = 0
        temp_by_q: Dict[date, List[Dict[str, Any]]] = {}
        for rec in records:
            if isinstance(rec.get("quarter"), date):
                temp_by_q.setdefault(rec["quarter"], []).append(rec)
        if INCLUDE_GUIDANCE_SUMMARY_IN_QUARTER_NOTES:
            for qd in quarters:
                for row_sum in _build_guidance_summary_rows(qd, temp_by_q.get(qd, [])):
                    records.append(row_sum)
                    appended_count += 1
        return QuarterNotesUiGuidanceSummaryResult(records=records, appended_count=appended_count)

    return dict(locals())
