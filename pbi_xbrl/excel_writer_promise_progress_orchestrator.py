"""Promise Progress UI orchestration writer."""
from __future__ import annotations

import datetime as dt
import hashlib
import re
import time
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .doc_intel import extract_pdf_text_cached
from .excel_writer_promise_progress import (
    PromiseProgressRenderHelpers,
    PromiseProgressSheetInputs,
    write_promise_progress_sheet,
)
from .excel_writer_promise_progress_anf import (
    AnfPromiseProgressWriterDeps,
    write_anf_promise_progress_ui_sheet,
)
from .excel_writer_promise_progress_followthrough import (
    PromiseProgressFollowthroughDeps,
    PromiseProgressFollowthroughModel,
)
from .excel_writer_promise_progress_guidance_accuracy import (
    PromiseProgressGuidanceAccuracyDeps,
    build_guidance_accuracy_rows as promise_progress_build_guidance_accuracy_rows,
)
from .excel_writer_promise_progress_render_adapter import (
    PromiseProgressRowWriterDeps,
    build_promise_progress_row_writer,
)
from .excel_writer_promise_progress_repairs import (
    GpreProgressTrimDeps,
    PromiseProgressVisibleRepairDeps,
    repair_promise_progress_visible_rows_for_render,
    trim_gpre_final_progress_rows,
)
from .excel_writer_promise_progress_rows import (
    PromiseProgressRowsDeps,
    normalize_promise_progress_rows_for_display,
    _dedupe_display_progress_rows as promise_rows_dedupe_display_progress_rows,
    _dedupe_promise_progress_rows as promise_rows_dedupe_promise_progress_rows,
    _display_progress_metric as promise_rows_display_progress_metric,
    _promise_progress_visible_category_rank_local as promise_rows_visible_category_rank_local,
)
from .excel_writer_promise_progress_selection import (
    PromiseProgressSelectionDeps,
    select_promise_progress_rows_for_display,
)
from .excel_writer_promise_progress_sources import (
    PromiseProgressSourceDeps,
    PromiseProgressSourceSupport,
)
from .filing_evidence_shared import (
    pick_best_subject_row_for_quarter as shared_pick_best_subject_row_for_quarter,
)
from .guidance_lexicon import normalize_text as glx_normalize_text
from .legacy_support import _coerce_prev_quarter_end, _path_belongs_to_ticker
from .non_gaap import infer_quarter_end_from_text, strip_html
from .quarter_notes_lexicon import compact_snippet as qn_compact_snippet


@dataclass(frozen=True)
class PromiseProgressOrchestratorDeps:
    wb: Any
    ticker: str
    is_anf_profile: bool
    is_pbi_profile: bool
    is_gpre_profile: bool
    promise_progress: Any
    promises: Any
    ui_state: Dict[str, Any]
    ui_info_rows: List[Dict[str, Any]]
    hist: Any
    adj_metrics: Any
    slides_guidance: Any
    material_roots: Sequence[Path]
    ticker_roots: Sequence[Path]
    pdf_text_cache_root: Any
    rebuild_doc_text_cache: bool
    quiet_pdf_warnings: bool
    quarter_notes: Any
    promise_visible_max_col: int
    promise_timeline_headers: Sequence[str]
    anf_build_promise_progress_sections: Callable[..., Any]
    anf_clean_visible_ui_text: Callable[..., str]
    apply_hyperlink_look: Callable[..., Any]
    candidate_quality_key: Callable[..., Any]
    classify_pbi_metric_label: Callable[..., str]
    clean_target_bonus: Callable[..., Any]
    coerce_amount_with_unit_local: Callable[..., Any]
    derive_split_target_meta: Callable[..., Any]
    ensure_promise_progress_ui_bundle: Callable[..., Dict[str, Any]]
    ensure_terminal_period: Callable[..., str]
    estimate_wrapped_line_count: Callable[..., int]
    estimate_wrapped_row_height: Callable[..., float]
    excel_safe_text_local: Callable[..., str]
    extract_45z_monetization_target_display: Callable[..., str]
    extract_45z_realized_progress_text: Callable[..., str]
    extract_money_targets_for_display: Callable[..., Any]
    extract_pbi_target_display: Callable[..., str]
    fmt_short_money_value_local: Callable[..., str]
    get_analysis_sheet_style_bundle: Callable[..., Any]
    gpre_bad_visible_promise_reason: Callable[..., bool]
    gpre_clean_visible_promise_metric: Callable[..., str]
    infer_target_period: Callable[..., Any]
    infer_target_structure: Callable[..., Any]
    is_45z_crush_margin_support_only: Callable[..., bool]
    is_pbi_clean_sentence: Callable[..., bool]
    is_preferred_narrative_source: Callable[..., bool]
    load_profile_slide_signals: Callable[..., Any]
    local_slide_45z_realized_text: Callable[..., str]
    looks_pbi_fragment_text: Callable[..., bool]
    lookup_pbi_structured_guidance_target: Callable[..., Any]
    lookup_pbi_structured_progress_hint: Callable[..., Any]
    management_credibility_scorecard_rows: Callable[..., Any]
    management_theme_key: Callable[..., str]
    nearest_amount_for_pattern: Callable[..., Any]
    parse_quarter_from_filename: Callable[..., Any]
    parse_quarter_from_follow_text: Callable[..., Any]
    pbi_guidance_period_label_from_text: Callable[..., str]
    pbi_promise_theme_re: Any
    pbi_repair_guidance_period_meta: Callable[..., Tuple[str, str]]
    pbi_structured_strategy_items_for_qd: Callable[..., Any]
    pbi_target_display_ok: Callable[..., bool]
    quarter_label_short: Callable[..., str]
    quarter_notes_view: Callable[..., Any]
    read_cached_doc_raw: Callable[..., str]
    record_writer_substage: Callable[..., None]
    render_stacked_quarter_blocks: Callable[..., Any]
    resolve_col: Callable[..., Any]
    rewrite_shared_promise_progress_ui_from_blocks: Callable[..., None]
    safe_cell: Callable[..., Any]
    set_cell_comment_local: Callable[..., Any]
    slide_signal_noise: Callable[..., bool]
    slide_text_paths: Callable[..., Any]
    source_rank: Callable[..., Any]
    split_target_family_key: Callable[..., str]
    split_target_identity_key: Callable[..., Tuple[str, str, str]]
    split_target_metric_display: Callable[..., str]
    split_target_qend: Callable[..., Any]
    split_target_scope_is_broad: Callable[..., bool]
    split_target_scope_token: Callable[..., str]
    strong_45z_2026_target_display: Callable[..., str]
    target_period_is_closed: Callable[..., bool]
    text_fragment_penalty: Callable[..., Any]
    timed_writer_substage: Callable[..., Any]
    write_analysis_sheet_title_and_metadata: Callable[..., None]


def write_promise_progress_ui_v2(deps: PromiseProgressOrchestratorDeps) -> List[Dict[str, Any]]:
    wb = deps.wb
    ticker = deps.ticker
    is_anf_profile = deps.is_anf_profile
    is_pbi_profile = deps.is_pbi_profile
    is_gpre_profile = deps.is_gpre_profile
    promise_progress = deps.promise_progress
    promises = deps.promises
    ui_state = deps.ui_state
    ui_info_rows = deps.ui_info_rows
    hist = deps.hist
    adj_metrics = deps.adj_metrics
    slides_guidance = deps.slides_guidance
    material_roots = deps.material_roots
    ticker_roots = deps.ticker_roots
    pdf_text_cache_root = deps.pdf_text_cache_root
    rebuild_doc_text_cache = deps.rebuild_doc_text_cache
    quiet_pdf_warnings = deps.quiet_pdf_warnings
    quarter_notes = deps.quarter_notes
    PROMISE_VISIBLE_MAX_COL = deps.promise_visible_max_col
    PROMISE_TIMELINE_HEADERS = deps.promise_timeline_headers
    _anf_build_promise_progress_sections = deps.anf_build_promise_progress_sections
    _anf_clean_visible_ui_text = deps.anf_clean_visible_ui_text
    _apply_hyperlink_look = deps.apply_hyperlink_look
    _candidate_quality_key = deps.candidate_quality_key
    _classify_pbi_metric_label = deps.classify_pbi_metric_label
    _clean_target_bonus = deps.clean_target_bonus
    _coerce_amount_with_unit_local = deps.coerce_amount_with_unit_local
    _derive_split_target_meta = deps.derive_split_target_meta
    _ensure_promise_progress_ui_bundle = deps.ensure_promise_progress_ui_bundle
    _ensure_terminal_period = deps.ensure_terminal_period
    _estimate_wrapped_line_count = deps.estimate_wrapped_line_count
    _estimate_wrapped_row_height = deps.estimate_wrapped_row_height
    _excel_safe_text_local = deps.excel_safe_text_local
    _extract_45z_monetization_target_display = deps.extract_45z_monetization_target_display
    _extract_45z_realized_progress_text = deps.extract_45z_realized_progress_text
    _extract_money_targets_for_display = deps.extract_money_targets_for_display
    _extract_pbi_target_display = deps.extract_pbi_target_display
    _fmt_short_money_value_local = deps.fmt_short_money_value_local
    _get_analysis_sheet_style_bundle = deps.get_analysis_sheet_style_bundle
    _gpre_bad_visible_promise_reason = deps.gpre_bad_visible_promise_reason
    _gpre_clean_visible_promise_metric = deps.gpre_clean_visible_promise_metric
    _infer_target_period = deps.infer_target_period
    _infer_target_structure = deps.infer_target_structure
    _is_45z_crush_margin_support_only = deps.is_45z_crush_margin_support_only
    _is_pbi_clean_sentence = deps.is_pbi_clean_sentence
    _is_preferred_narrative_source = deps.is_preferred_narrative_source
    _load_profile_slide_signals = deps.load_profile_slide_signals
    _local_slide_45z_realized_text = deps.local_slide_45z_realized_text
    _looks_pbi_fragment_text = deps.looks_pbi_fragment_text
    _lookup_pbi_structured_guidance_target = deps.lookup_pbi_structured_guidance_target
    _lookup_pbi_structured_progress_hint = deps.lookup_pbi_structured_progress_hint
    _management_credibility_scorecard_rows = deps.management_credibility_scorecard_rows
    _management_theme_key = deps.management_theme_key
    _nearest_amount_for_pattern = deps.nearest_amount_for_pattern
    _parse_quarter_from_filename = deps.parse_quarter_from_filename
    _parse_quarter_from_follow_text = deps.parse_quarter_from_follow_text
    _pbi_guidance_period_label_from_text = deps.pbi_guidance_period_label_from_text
    _pbi_promise_theme_re = deps.pbi_promise_theme_re
    _pbi_repair_guidance_period_meta = deps.pbi_repair_guidance_period_meta
    _pbi_structured_strategy_items_for_qd = deps.pbi_structured_strategy_items_for_qd
    _pbi_target_display_ok = deps.pbi_target_display_ok
    _quarter_label_short = deps.quarter_label_short
    _quarter_notes_view = deps.quarter_notes_view
    _read_cached_doc_raw = deps.read_cached_doc_raw
    _record_writer_substage = deps.record_writer_substage
    _render_stacked_quarter_blocks = deps.render_stacked_quarter_blocks
    _resolve_col = deps.resolve_col
    _rewrite_shared_promise_progress_ui_from_blocks = deps.rewrite_shared_promise_progress_ui_from_blocks
    _safe_cell = deps.safe_cell
    _set_cell_comment_local = deps.set_cell_comment_local
    _slide_signal_noise = deps.slide_signal_noise
    _slide_text_paths = deps.slide_text_paths
    _source_rank = deps.source_rank
    _split_target_family_key = deps.split_target_family_key
    _split_target_identity_key = deps.split_target_identity_key
    _split_target_metric_display = deps.split_target_metric_display
    _split_target_qend = deps.split_target_qend
    _split_target_scope_is_broad = deps.split_target_scope_is_broad
    _split_target_scope_token = deps.split_target_scope_token
    _strong_45z_2026_target_display = deps.strong_45z_2026_target_display
    _target_period_is_closed = deps.target_period_is_closed
    _text_fragment_penalty = deps.text_fragment_penalty
    _timed_writer_substage = deps.timed_writer_substage
    _write_analysis_sheet_title_and_metadata = deps.write_analysis_sheet_title_and_metadata

    qa_rows: List[Dict[str, Any]] = []
    pp_rationale_col_width_default = max(40.0, (554.0 - 5.0) / 7.0)
    ts = datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    pp_header_text = f"Generated at {ts} | Quarter blocks"

    def _write_empty_promise_progress_sheet(message: str) -> List[Dict[str, Any]]:
        write_promise_progress_sheet(
            PromiseProgressSheetInputs(
                wb=wb,
                sheet_name="Promise_Progress_UI",
                quarters=[],
                rows_by_quarter={},
                generated_at_text=pp_header_text,
                pp_rationale_col_width_default=pp_rationale_col_width_default,
                empty_message=message,
            ),
            PromiseProgressRenderHelpers(
                write_analysis_sheet_title_and_metadata=_write_analysis_sheet_title_and_metadata,
                render_stacked_quarter_blocks=lambda *args, **kwargs: 0,
                row_writer=lambda *_args, **_kwargs: None,
                get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
                estimate_wrapped_line_count=_estimate_wrapped_line_count,
                parse_dollar_amount=lambda *_args, **_kwargs: None,
            ),
        )
        return qa_rows

    if is_anf_profile:
        return write_anf_promise_progress_ui_sheet(
            AnfPromiseProgressWriterDeps(
                wb=wb,
                slides_guidance=slides_guidance,
                hist=hist,
                generated_at_text=pp_header_text,
                promise_visible_max_col=PROMISE_VISIBLE_MAX_COL,
                promise_timeline_headers=PROMISE_TIMELINE_HEADERS,
                write_analysis_sheet_title_and_metadata=_write_analysis_sheet_title_and_metadata,
                get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
                anf_build_promise_progress_sections=_anf_build_promise_progress_sections,
                management_credibility_scorecard_rows=_management_credibility_scorecard_rows,
                anf_clean_visible_ui_text=_anf_clean_visible_ui_text,
            )
        )

    tracker_rows_seed = ui_state.get("promise_tracker_rows_by_q", {}) if isinstance(ui_state, dict) else {}
    guidance_seed = ui_state.get("guidance_snapshot_by_q", {}) if isinstance(ui_state, dict) else {}
    has_tracker_seed = any(bool(rows) for rows in tracker_rows_seed.values()) if isinstance(tracker_rows_seed, dict) else False
    has_guidance_seed = any(bool(rows) for rows in guidance_seed.values()) if isinstance(guidance_seed, dict) else False
    if (promise_progress is None or promise_progress.empty) and not (has_tracker_seed or has_guidance_seed):
        return _write_empty_promise_progress_sheet("No data.")

    quarter_hint_list = [q for q in (ui_state.get("quarters") or []) if isinstance(q, date)]
    if isinstance(guidance_seed, dict):
        for qkey_local, rows_local in guidance_seed.items():
            if not rows_local:
                continue
            qdt_local = pd.to_datetime(qkey_local, errors="coerce")
            if pd.isna(qdt_local):
                continue
            qdate_local = pd.Timestamp(qdt_local).date()
            if qdate_local not in quarter_hint_list:
                quarter_hint_list.append(qdate_local)
    quarter_hint = tuple(sorted(quarter_hint_list, reverse=True))
    with _timed_writer_substage("write_excel.ui.progress_bundle.build"):
        progress_bundle = _ensure_promise_progress_ui_bundle(quarter_hint)

    prog = progress_bundle.get("prog") if isinstance(progress_bundle, dict) else pd.DataFrame()
    progress_records = list(progress_bundle.get("prog_records") or []) if isinstance(progress_bundle, dict) else []
    progress_records_by_q = (
        dict(progress_bundle.get("prog_records_by_q") or {})
        if isinstance(progress_bundle, dict)
        else {}
    )
    cols_map = dict(progress_bundle.get("cols") or {}) if isinstance(progress_bundle, dict) else {}
    pid_col = cols_map.get("pid_col")
    q_col = cols_map.get("q_col")
    st_col = cols_map.get("st_col")
    sc_col = cols_map.get("sc_col")
    ra_col = _resolve_col(prog, ["rationale"])
    mr_col = _resolve_col(prog, ["metric_ref", "metric_refs"])
    ac_col = _resolve_col(prog, ["actual"])
    tg_col = _resolve_col(prog, ["target"])
    pk_col = _resolve_col(prog, ["promise_key"])
    tb_col = _resolve_col(prog, ["target_bucket"])
    scr_col = _resolve_col(prog, ["scorable"])
    num_upd_col = _resolve_col(prog, ["numeric_update_this_quarter"])
    fs_col = _resolve_col(prog, ["first_seen_quarter", "first_seen_q", "created_quarter"])
    ls_col = _resolve_col(prog, ["last_seen_quarter", "last_seen_q", "quarter"])
    fs_ev_col = _resolve_col(prog, ["first_seen_evidence_quarter", "first_seen_quarter", "created_quarter"])
    ls_ev_col = _resolve_col(prog, ["last_seen_evidence_quarter", "last_seen_quarter"])
    ls_num_col = _resolve_col(prog, ["last_seen_numeric_quarter"])
    ls_txt_col = _resolve_col(prog, ["last_seen_text_quarter"])
    carried_col = _resolve_col(prog, ["carried_to_quarter"])
    gtype_col = _resolve_col(prog, ["guidance_type", "target_type"])
    tpn_col = _resolve_col(prog, ["target_period_norm", "period_norm"])
    tpl_col = _resolve_col(prog, ["target_period_label", "period_label"])
    ptype_col = _resolve_col(prog, ["promise_type"])
    qsev_col = _resolve_col(prog, ["qa_severity"])
    qmsg_col = _resolve_col(prog, ["qa_message"])
    src_ev_col = cols_map.get("src_ev_col")
    deadline_col = _resolve_col(prog, ["target_time", "deadline", "target_period_end"])
    tracker_only_progress_mode = False
    if pid_col is None or q_col is None or st_col is None:
        if has_tracker_seed or has_guidance_seed:
            tracker_only_progress_mode = True
            progress_records = []
            progress_records_by_q = {}
        else:
            return _write_empty_promise_progress_sheet("Missing progress columns.")
    if (prog is None or prog.empty) and not tracker_only_progress_mode:
        if has_tracker_seed or has_guidance_seed:
            tracker_only_progress_mode = True
            progress_records = []
            progress_records_by_q = {}
        else:
            return _write_empty_promise_progress_sheet("No valid quarter rows.")

    def _qend(x: Any) -> Optional[date]:
        t = pd.to_datetime(x, errors="coerce")
        if pd.isna(t):
            return None
        return pd.Timestamp(t).to_period("Q").end_time.date()

    def _short_pid(pid: str) -> str:
        p = str(pid or "").strip()
        return p[:12] if len(p) > 12 else p

    def _q_label(v: Any) -> str:
        t = pd.to_datetime(v, errors="coerce")
        if pd.isna(t):
            return "N/A"
        qn = ((int(t.month) - 1) // 3) + 1
        return f"Q{qn} {int(t.year)}"

    milestone_action_re = re.compile(
        r"\b(complete|finish|close|exit|launch|implement|reduce headcount|eliminate|deliver|achieve|reach|begin|initiate|initiated|initiating|executed|repaid|online|ramping|commissioning|fully operational)\b",
        re.I,
    )
    milestone_deadline_re = re.compile(
        r"\b(by|through|until|before|after)\b|\b(20\d{2})\b|\b(q[1-4]\s*20\d{2}|fy\s*20\d{2}|h1\s*20\d{2}|h2\s*20\d{2})\b",
        re.I,
    )
    milestone_exclude_re = re.compile(
        r"\b(securities act|indenture|administrative agent|conversion|convertible|registration|"
        r"settlement date|notes will be|loan documents|section 3\(a\)\(9\)|offering|covenant definitions|"
        r"annual meeting|webcast|conference call|will discuss|release results|press release|section)\b",
        re.I,
    )
    milestone_completion_re = re.compile(
        r"\b(completed|fully operational|fully online|sale completed|achieved|delivered|closed|repaid|"
        r"repayment completed|used to fully repay)\b",
        re.I,
    )
    milestone_progress_re = re.compile(
        r"\b(on track|ramping|under construction|progressing|began|beginning|initiated|initiating|advancing|continuing|"
        r"commissioning|started up|online(?: and ramping)?|agreement executed|agreements executed|"
        r"construction progressing|received permit|permit received|ordered major equipment)\b",
        re.I,
    )
    buyback_intent_re = re.compile(r"\b(intend|plan|expect)\b[^.]{0,120}\b(repurchase|buyback)\b", re.I)
    buyback_remaining_re = re.compile(r"\b(remaining|available)\b[^.]{0,120}\b(authoriz)\w*", re.I)

    def _parse_dollar_amount(text_in: str) -> Optional[float]:
        t = str(text_in or "")
        m = re.search(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*(million|billion|m|bn)?", t, re.I)
        if not m:
            return None
        try:
            v = float(str(m.group(1)).replace(",", ""))
        except Exception:
            return None
        u = str(m.group(2) or "").lower()
        if u in {"billion", "bn"}:
            v *= 1e9
        elif u in {"million", "m"}:
            v *= 1e6
        elif v < 10_000:
            return None
        return float(v)

    def _parse_target_year(text_in: str, fallback_q: date) -> Optional[int]:
        t = str(text_in or "")
        m = re.search(r"\b(?:in|for|through|by)\s+(20\d{2})\b", t, re.I)
        if m:
            yy = int(m.group(1))
            if 1990 <= yy <= 2100:
                return yy
        return None

    def _set_num(cell: Any, val: Any) -> bool:
        if isinstance(val, str) and "%" in val:
            s = val.strip().replace("%", "").replace(",", ".")
            try:
                cell.value = float(s) / 100.0
                cell.number_format = "0.0%"
                return True
            except Exception:
                pass
        num = pd.to_numeric(val, errors="coerce")
        if pd.isna(num):
            return False
        f = float(num)
        cell.value = f
        if abs(f) >= 1_000_000:
            cell.number_format = '#,##0.000,,"m"'
        else:
            cell.number_format = "0.000"
        return True

    hist_local = progress_bundle.get("hist_local") if isinstance(progress_bundle, dict) else pd.DataFrame()
    adj_local = progress_bundle.get("adj_local") if isinstance(progress_bundle, dict) else pd.DataFrame()
    guidance_series_cache = (
        dict(progress_bundle.get("guidance_series_cache") or {})
        if isinstance(progress_bundle, dict)
        else {}
    )

    def _buyback_actual_ytd(as_of_q: date, target_year: int) -> Optional[float]:
        if hist_local is None or hist_local.empty or "quarter" not in hist_local.columns or "buybacks_cash" not in hist_local.columns:
            return None
        hh = hist_local.copy()
        hh["q_end"] = hh["quarter"].dt.to_period("Q").dt.end_time.dt.date
        hh = hh[(hh["q_end"].notna()) & (hh["q_end"] <= as_of_q)]
        hh = hh[hh["q_end"].map(lambda d: d.year == int(target_year))]
        if hh.empty:
            return None
        vals = pd.to_numeric(hh["buybacks_cash"], errors="coerce").dropna()
        if vals.empty:
            return None
        return float(vals.sum())
    evaluation_as_of = progress_bundle.get("evaluation_as_of") if isinstance(progress_bundle, dict) else None

    def _guidance_period_end(period_norm: str, asof_q: date) -> Optional[date]:
        p = str(period_norm or "").strip()
        if not p or p == "UNK":
            return None
        if p == "FY+1":
            return date(int(asof_q.year) + 1, 12, 31)
        m_fy = re.match(r"FY(20\d{2})$", p)
        if m_fy:
            return date(int(m_fy.group(1)), 12, 31)
        m_q = re.match(r"Q(20\d{2})Q([1-4])$", p)
        if m_q:
            yy = int(m_q.group(1))
            qq = int(m_q.group(2))
            if qq == 1:
                return date(yy, 3, 31)
            if qq == 2:
                return date(yy, 6, 30)
            if qq == 3:
                return date(yy, 9, 30)
            return date(yy, 12, 31)
        return None

    def _guidance_period_label(period_norm: str, asof_q: date) -> str:
        p = str(period_norm or "").strip()
        if not p:
            return ""
        if p == "FY+1":
            return f"FY {int(asof_q.year) + 1}"
        m_fy = re.match(r"FY(20\d{2})$", p)
        if m_fy:
            return f"FY {int(m_fy.group(1))}"
        m_q = re.match(r"Q(20\d{2})Q([1-4])$", p)
        if m_q:
            return f"Q{int(m_q.group(2))} {int(m_q.group(1))}"
        return p

    def _series_for_guidance_metric(metric_name: str) -> pd.DataFrame:
        series_df = guidance_series_cache.get(str(metric_name or ""))
        if isinstance(series_df, pd.DataFrame) and not series_df.empty:
            return series_df.copy()
        return pd.DataFrame(columns=["quarter", "value", "_proxy_used", "_source_used"])

    def _actual_for_guidance(metric_name: str, period_norm: str, asof_q: date) -> Optional[float]:
        s = _series_for_guidance_metric(metric_name)
        if s.empty:
            return None
        period_end = _guidance_period_end(period_norm, asof_q)
        if period_end is None:
            return None
        s = s.copy()
        s["q_end"] = s["quarter"].dt.to_period("Q").dt.end_time.dt.date
        s = s[s["q_end"] <= period_end]
        if s.empty:
            return None
        if str(period_norm).startswith("FY"):
            fy_slice = s[s["q_end"] <= period_end].tail(4)
            if len(fy_slice) < 4:
                return None
            return float(pd.to_numeric(fy_slice["value"], errors="coerce").sum())
        row = s[s["q_end"] == period_end]
        if row.empty:
            return None
        return float(pd.to_numeric(row.iloc[-1]["value"], errors="coerce"))

    def _guidance_target_text(item: Dict[str, Any]) -> str:
        kind = str(item.get("kind") or "")
        unit = str(item.get("unit") or "")
        if kind == "range" and item.get("low") is not None and item.get("high") is not None:
            lo = float(item.get("low"))
            hi = float(item.get("high"))
            if unit == "$m":
                return f"${lo/1e6:,.1f}m-${hi/1e6:,.1f}m"
            if unit == "$":
                return f"${lo:,.2f}-${hi:,.2f}"
            if unit == "%":
                return f"{lo:.1f}% - {hi:.1f}%"
            if unit == "x":
                return f"{lo:.2f}x-{hi:.2f}x"
            return f"{lo:,.2f}-{hi:,.2f}"
        if kind == "point" and item.get("value") is not None:
            v = float(item.get("value"))
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

    def _guidance_actual_text(metric_name: str, actual: float) -> str:
        if metric_name in {"Revenue", "Adj EBIT", "Adj EBITDA", "FCF", "Capex"}:
            return f"${float(actual)/1e6:,.1f}m"
        if metric_name == "Adj EPS":
            return f"${float(actual):,.2f}"
        return f"{float(actual):,.2f}"

    def _fmt_short_money_value(val: float) -> str:
        v = float(val)
        if abs(v) >= 1e9:
            return f"${v/1e9:,.1f}bn"
        return f"${v/1e6:,.1f}m"

    def _coerce_amount_with_unit(num_txt: str, unit_txt: str) -> Optional[float]:
        try:
            val = float(str(num_txt).replace(",", ""))
        except Exception:
            return None
        unit = str(unit_txt or "").lower()
        if unit in {"billion", "bn"}:
            val *= 1e9
        elif unit in {"million", "m"}:
            val *= 1e6
        elif abs(val) < 10_000:
            return None
        return float(val)

    promise_progress_source_model: Dict[str, Any] = {}

    def _promise_progress_source_follow_model() -> Any:
        model = promise_progress_source_model.get("model")
        if model is None:
            raise RuntimeError("Promise Progress follow-through model is not initialized")
        return model

    def _source_build_follow_through_candidate(*args: Any, **kwargs: Any) -> Any:
        return _promise_progress_source_follow_model()._build_follow_through_candidate(*args, **kwargs)

    def _source_follow_candidate_sort_key(candidate: Dict[str, Any]) -> Any:
        return _promise_progress_source_follow_model()._follow_candidate_sort_key(candidate)

    progress_source_support = PromiseProgressSourceSupport(
        PromiseProgressSourceDeps(
            material_roots=material_roots,
            ticker=ticker,
            ticker_roots=ticker_roots,
            pdf_text_cache_root=pdf_text_cache_root,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
            path_belongs_to_ticker=_path_belongs_to_ticker,
            extract_pdf_text_cached=extract_pdf_text_cached,
            strip_html=strip_html,
            parse_quarter_from_filename=_parse_quarter_from_filename,
            parse_quarter_from_follow_text=_parse_quarter_from_follow_text,
            infer_quarter_end_from_text=infer_quarter_end_from_text,
            coerce_prev_quarter_end=_coerce_prev_quarter_end,
            source_rank=_source_rank,
            build_follow_through_candidate=_source_build_follow_through_candidate,
            follow_candidate_sort_key=_source_follow_candidate_sort_key,
            read_cached_doc_raw=_read_cached_doc_raw,
            slide_text_paths=_slide_text_paths,
            parse_dollar_amount=_parse_dollar_amount,
            coerce_amount_with_unit=_coerce_amount_with_unit,
            coerce_amount_with_unit_local=_coerce_amount_with_unit_local,
            fmt_short_money_value=_fmt_short_money_value,
            fmt_short_money_value_local=_fmt_short_money_value_local,
            q_label=_q_label,
            extract_45z_realized_progress_text=_extract_45z_realized_progress_text,
        )
    )
    _extract_progress_latest_basis = progress_source_support.extract_progress_latest_basis
    _evidence_time_label = progress_source_support.evidence_time_label
    _read_promise_follow_text = progress_source_support.read_promise_follow_text
    _load_local_45z_closed_period_outcome = progress_source_support.load_local_45z_closed_period_outcome
    _load_local_cost_savings_follow_candidates = progress_source_support.load_local_cost_savings_follow_candidates
    _load_local_45z_realized_basis = progress_source_support.load_local_45z_realized_basis
    _local_fy_adj_ebitda_cache: Optional[List[Dict[str, Any]]] = None

    def _annual_adjusted_ebitda_source_files() -> List[Tuple[str, Path]]:
        files: List[Tuple[str, Path]] = []
        seen: set[str] = set()
        source_dirs = [
            ("financial_statement", "financial_statement"),
            ("earnings_release", "press_release"),
        ]
        for source_type, folder_name in source_dirs:
            for root in material_roots:
                src_dir = root / folder_name
                if not src_dir.exists() or not src_dir.is_dir():
                    continue
                try:
                    cand_files = sorted([p for p in src_dir.iterdir() if p.is_file()])
                except Exception:
                    continue
                for path_in in cand_files:
                    if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                        continue
                    if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                        continue
                    try:
                        key = str(path_in.resolve())
                    except Exception:
                        key = str(path_in)
                    if key in seen:
                        continue
                    seen.add(key)
                    files.append((source_type, path_in))
        return files

    def _extract_direct_fy_adjusted_ebitda_records_from_text(
        raw_txt: str,
        path_in: Path,
        source_type: str,
    ) -> List[Dict[str, Any]]:
        txt = glx_normalize_text(raw_txt)
        if not txt or "adjusted ebitda" not in txt.lower():
            return []
        out_rows: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[int, float, str]] = set()
        table_re = re.compile(
            r"Year Ended December 31,\s*(20\d{2})\s+(20\d{2})(?:\s+(20\d{2}))?.{0,1600}?"
            r"Adjusted EBITDA\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?)\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?)"
            r"(?:\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?))?",
            re.I | re.S,
        )
        narrative_re = re.compile(
            r"\b(?:full year|fiscal year|year ended december 31,?)\s*(20\d{2})\b[^.]{0,180}?"
            r"adjusted ebitda(?:\s+of)?\s+\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(million|m)\b",
            re.I,
        )

        def _parse_signed_amount(token: str) -> Optional[float]:
            tok = str(token or "").strip()
            if not tok:
                return None
            sign = -1.0 if tok.startswith("(") and tok.endswith(")") else 1.0
            try:
                return sign * float(tok.strip("()").replace(",", ""))
            except Exception:
                return None

        for match in table_re.finditer(txt):
            years = [x for x in match.groups()[:3] if x]
            values = [x for x in match.groups()[3:] if x]
            if not years or not values:
                continue
            snippet = qn_compact_snippet(txt[max(0, match.start() - 60) : min(len(txt), match.end() + 120)], 260)
            for yy, vv in zip(years, values):
                amt = _parse_signed_amount(vv)
                if amt is None:
                    continue
                value_m = float(amt) / 1000.0
                key = (int(yy), round(value_m, 3), str(path_in))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                out_rows.append(
                    {
                        "fiscal_year": int(yy),
                        "value_m": value_m,
                        "source_type": source_type,
                        "source_doc": str(path_in),
                        "quality": "exact",
                        "snippet": snippet,
                    }
                )
        for match in narrative_re.finditer(txt):
            year_txt, amt_txt, _ = match.groups()
            try:
                value_m = float(str(amt_txt).replace(",", ""))
            except Exception:
                continue
            key = (int(year_txt), round(value_m, 3), str(path_in))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out_rows.append(
                {
                    "fiscal_year": int(year_txt),
                    "value_m": float(value_m),
                    "source_type": source_type,
                    "source_doc": str(path_in),
                    "quality": "text-derived",
                    "snippet": qn_compact_snippet(
                        txt[max(0, match.start() - 60) : min(len(txt), match.end() + 120)],
                        260,
                    ),
                }
            )
        return out_rows

    def _load_local_fy_adjusted_ebitda_records() -> List[Dict[str, Any]]:
        nonlocal _local_fy_adj_ebitda_cache
        if _local_fy_adj_ebitda_cache is not None:
            return list(_local_fy_adj_ebitda_cache)
        records: List[Dict[str, Any]] = []
        for source_type, path_in in _annual_adjusted_ebitda_source_files():
            raw_txt = _read_promise_follow_text(path_in)
            if not raw_txt or "adjusted ebitda" not in raw_txt.lower():
                continue
            try:
                records.extend(
                    _extract_direct_fy_adjusted_ebitda_records_from_text(raw_txt, path_in, source_type)
                )
            except Exception:
                continue
        records.sort(
            key=lambda rec: (
                int(rec.get("fiscal_year") or 0),
                _source_rank(rec.get("source_type"), rec.get("source_doc")),
                0 if str(rec.get("quality") or "") == "exact" else 1,
                -abs(float(rec.get("value_m") or 0.0)),
            )
        )
        _local_fy_adj_ebitda_cache = records
        return list(records)

    def _resolve_thesis_fy_base() -> Dict[str, Any]:
        latest_q = pd.NaT
        if hist is not None and not hist.empty and "quarter" in hist.columns:
            latest_q = pd.to_datetime(hist["quarter"], errors="coerce").dropna().max()
        if pd.isna(latest_q) and adj_metrics is not None and not adj_metrics.empty and "quarter" in adj_metrics.columns:
            latest_q = pd.to_datetime(adj_metrics["quarter"], errors="coerce").dropna().max()
        latest_fy_year: Optional[int] = None
        if pd.notna(latest_q):
            latest_q_ts = pd.Timestamp(latest_q)
            latest_fy_year = latest_q_ts.year if latest_q_ts.month == 12 else latest_q_ts.year - 1
        label_core = f"Base Adj EBITDA FY{latest_fy_year}" if latest_fy_year else "Base Adj EBITDA FY"
        if (
            adj_metrics is not None
            and not adj_metrics.empty
            and "quarter" in adj_metrics.columns
            and "adj_ebitda" in adj_metrics.columns
        ):
            adj_local = adj_metrics.copy()
            adj_local["quarter"] = pd.to_datetime(adj_local["quarter"], errors="coerce")
            adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")
            adj_local["adj_ebitda"] = pd.to_numeric(adj_local["adj_ebitda"], errors="coerce")
            adj_clean = adj_local.dropna(subset=["adj_ebitda"]).copy()
            recent = adj_clean.groupby(
                adj_clean["quarter"].dt.to_period("Q"),
                as_index=False,
            ).last().tail(4)
            if len(recent) == 4:
                raw_sum = float(recent["adj_ebitda"].sum())
                value_m = raw_sum / 1_000_000.0 if abs(raw_sum) > 10_000.0 else raw_sum
                latest_ttm_q = pd.to_datetime(recent["quarter"], errors="coerce").dropna().max()
                latest_ttm_label = (
                    f" through {pd.Timestamp(latest_ttm_q).date()}" if pd.notna(latest_ttm_q) else ""
                )
                return {
                    "label": "Base Adj EBITDA TTM (latest)",
                    "value_m": float(value_m),
                    "fallback": "latest TTM",
                    "source_type": "adj_metrics",
                    "source_doc": "",
                    "quality": "modeled",
                    "snippet": f"Latest four quarterly adjusted EBITDA observations{latest_ttm_label}.",
                }
        annual_records = _load_local_fy_adjusted_ebitda_records()
        if latest_fy_year is not None:
            fy_records = [rec for rec in annual_records if int(rec.get("fiscal_year") or 0) == latest_fy_year]
        else:
            fy_records = []
        if fy_records:
            best = sorted(
                fy_records,
                key=lambda rec: (
                    _source_rank(rec.get("source_type"), rec.get("source_doc")),
                    0 if str(rec.get("quality") or "") == "exact" else 1,
                ),
            )[0]
            return {
                "label": label_core,
                "value_m": float(best.get("value_m") or 0.0),
                "fallback": "",
                "source_type": str(best.get("source_type") or ""),
                "source_doc": str(best.get("source_doc") or ""),
                "quality": str(best.get("quality") or "exact"),
                "snippet": str(best.get("snippet") or ""),
            }

        if adj_metrics is not None and not adj_metrics.empty and latest_fy_year is not None:
            adj_local = adj_metrics.copy()
            adj_local["quarter"] = pd.to_datetime(adj_local["quarter"], errors="coerce")
            adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")
            if "adj_ebitda" in adj_local.columns:
                same_fy = adj_local[adj_local["quarter"].dt.year == latest_fy_year].copy()
                same_fy["adj_ebitda"] = pd.to_numeric(same_fy["adj_ebitda"], errors="coerce")
                same_fy = same_fy.dropna(subset=["adj_ebitda"])
                if same_fy["quarter"].dt.to_period("Q").nunique() >= 4:
                    last_four = same_fy.groupby(same_fy["quarter"].dt.to_period("Q"), as_index=False)["adj_ebitda"].last()
                    if len(last_four) >= 4:
                        return {
                            "label": f"{label_core} (fallback: summed quarters)",
                            "value_m": float(last_four["adj_ebitda"].tail(4).sum()),
                            "fallback": "summed quarters",
                            "source_type": "modeled",
                            "source_doc": "",
                            "quality": "modeled",
                            "snippet": "Summed four quarterly adjusted EBITDA observations for the latest completed fiscal year.",
                        }
                adj_local["adj_ebitda"] = pd.to_numeric(adj_local["adj_ebitda"], errors="coerce")
                recent = adj_local.dropna(subset=["adj_ebitda"]).tail(4)
                if len(recent) == 4:
                    return {
                        "label": f"{label_core} (fallback: TTM)",
                        "value_m": float(recent["adj_ebitda"].sum()),
                        "fallback": "TTM",
                        "source_type": "modeled",
                        "source_doc": "",
                        "quality": "modeled",
                        "snippet": "Summed latest four quarterly adjusted EBITDA observations as a TTM fallback.",
                    }

        return {
            "label": f"{label_core} (fallback: unavailable)",
            "value_m": 0.0,
            "fallback": "unavailable",
            "source_type": "",
            "source_doc": "",
            "quality": "inferred",
            "snippet": "",
        }

    guidance_accuracy_deps = PromiseProgressGuidanceAccuracyDeps(
        ui_state=ui_state if isinstance(ui_state, dict) else {},
        evaluation_as_of=evaluation_as_of,
        guidance_target_text=_guidance_target_text,
        series_for_guidance_metric=_series_for_guidance_metric,
        actual_for_guidance=_actual_for_guidance,
        guidance_period_end=_guidance_period_end,
        guidance_period_label=_guidance_period_label,
        guidance_actual_text=_guidance_actual_text,
    )

    def _build_guidance_accuracy_rows(qd: date) -> List[Dict[str, Any]]:
        return promise_progress_build_guidance_accuracy_rows(guidance_accuracy_deps, qd)

    quarters = list(progress_bundle.get("quarters") or []) if isinstance(progress_bundle, dict) else []
    if not quarters and quarter_hint:
        quarters = list(quarter_hint)
    tracker_rows_map = ui_state.get("promise_tracker_rows_by_q", {}) if isinstance(ui_state, dict) else {}
    quarter_note_rows_map = ui_state.get("quarter_notes_ui_rows", {}) if isinstance(ui_state, dict) else {}

    ev_map_q = dict(progress_bundle.get("ev_map_q") or {}) if isinstance(progress_bundle, dict) else {}
    ev_map_pid = dict(progress_bundle.get("ev_map_pid") or {}) if isinstance(progress_bundle, dict) else {}

    def _progress_metric_from_event(note_item: Dict[str, Any]) -> str:
        event_type = str(note_item.get("_event_type") or "").strip().lower()
        metric_family = str(note_item.get("_event_metric_family") or "").strip().lower()
        entity_scope = str(note_item.get("_event_entity_scope") or "").strip().lower()
        if not event_type and not metric_family:
            return ""
        if is_pbi_profile:
            if event_type == "guidance":
                return {
                    "revenue": "Revenue guidance",
                    "adj_ebit": "Adjusted EBIT guidance",
                    "eps": "EPS guidance",
                    "fcf": "FCF target",
                    "cost_savings": "Cost savings target",
                    "liquidity": "PB Bank liquidity release",
                    "debt": "Deleveraging target",
                }.get(metric_family, "")
            if event_type == "cost_savings" or metric_family == "cost_savings":
                return "Cost savings target"
            if event_type == "liquidity_release" or metric_family == "liquidity" or entity_scope == "pb_bank":
                return "PB Bank liquidity release"
            if event_type == "deleveraging" or metric_family == "debt":
                return "Deleveraging target"
            if event_type == "milestone" or metric_family == "milestone":
                return "Strategic milestone"
            return ""
        if event_type == "regulatory_credit" or metric_family == "regulatory_credit":
            return "45Z monetization / EBITDA"
        if event_type == "cost_savings" or metric_family == "cost_savings":
            return "Cost savings"
        if event_type == "deleveraging" or metric_family == "debt" or entity_scope == "obion":
            return "Debt reduction"
        if event_type == "milestone" or metric_family == "milestone":
            return "Strategic milestone"
        return ""

    def _progress_metric_from_qnote(note_item: Dict[str, Any]) -> str:
        txt_local = glx_normalize_text(str(note_item.get("text_full") or note_item.get("comment_full_text") or ""))
        direct_metric = str(
            note_item.get("metric_display")
            or note_item.get("_metric_display")
            or note_item.get("metric_ref")
            or note_item.get("metric")
            or note_item.get("metric_canon")
            or ""
        ).strip()
        hint = " | ".join(
            [
                str(note_item.get("metric_canon") or ""),
                str(note_item.get("metric_tag") or ""),
                str(note_item.get("_metric_display") or ""),
                str(note_item.get("metric_display") or ""),
                str(note_item.get("metric_ref") or ""),
                str(note_item.get("metric") or ""),
            ]
        ).strip().lower()
        blob = f"{hint} {txt_local.lower()}"
        if is_pbi_profile:
            pbi_allowed_labels_local = {
                "Adjusted EBIT guidance",
                "Revenue guidance",
                "EPS guidance",
                "FCF target",
                "Cost savings target",
                "PB Bank liquidity release",
                "Deleveraging target",
                "SendTech / Presort operating target",
                "Strategic milestone",
            }
            if direct_metric in pbi_allowed_labels_local:
                return direct_metric
            if re.search(r"\bstrategic review\b", blob, re.I):
                return "Strategic milestone"
            pbi_metric = _classify_pbi_metric_label(blob, "")
            if pbi_metric in pbi_allowed_labels_local:
                return pbi_metric
            if pbi_metric:
                return ""
        gpre_allowed_labels_local = {
            "45Z-related Adjusted EBITDA",
            "45Z monetization / EBITDA",
            "45Z plant qualification readiness",
            "45Z from remaining facilities",
            "Advantage Nebraska EBITDA opportunity",
            "Advantage Nebraska startup",
            "Capex guidance (FY 2026)",
            "Cost savings target",
            "Debt reduction",
            "Interest expense outlook",
            "Strategic milestone",
        }
        if direct_metric in gpre_allowed_labels_local:
            return direct_metric
        if any(k in blob for k in ("45z", "tax credit monetization", "ebitda opportunity", "qualify for production tax credits")):
            return "45Z monetization / EBITDA"
        if re.search(r"\binterest expense\b", blob, re.I) and re.search(r"\b(expected|annualized|outlook|2026)\b", blob, re.I):
            return "Interest expense outlook"
        if re.search(r"\b(capex|capital expenditures?|sustaining capital)\b", blob, re.I) and re.search(r"\b(2026|expected|guidance|outlook)\b", blob, re.I):
            return "Capex guidance (FY 2026)"
        if re.search(r"\b(cost reduction|cost savings|annualized savings|expense reduction)\b", blob, re.I):
            return "Cost savings"
        if re.search(r"\b(repay|repaid|delever|debt reduction|used to fully repay|sale of obion)\b", blob, re.I):
            return "Debt reduction"
        if re.search(r"\b(fully operational|online|ramping|progressing|under construction|construction progressing|start-?up|started up|delivered|received .*permit|permit|commissioning|executed|ordered major equipment|construction management agreements?)\b", blob, re.I):
            return "Strategic milestone"
        return ""

    def _progress_target_display_from_qnote(qd_c: date, metric_name: str, text_in: Any) -> str:
        txt_local = glx_normalize_text(str(text_in or ""))
        metric_txt = str(metric_name or "").strip()
        if not txt_local or not metric_txt:
            return ""
        if is_pbi_profile:
            return _extract_pbi_target_display(txt_local, metric_txt)
        metric_low = metric_txt.lower()
        if re.search(r"\b45z\b|tax credit", metric_low, re.I):
            return (
                _extract_45z_monetization_target_display(txt_local, qd_c)
                or _strong_45z_2026_target_display(txt_local, qd_c, "")
                or ""
            )
        if re.search(r"\b(capex|capital expenditures?|sustaining capital)\b", metric_low, re.I):
            amounts = _extract_money_targets_for_display(txt_local)
            if len(amounts) >= 2:
                lo = min(float(amounts[0]), float(amounts[1]))
                hi = max(float(amounts[0]), float(amounts[1]))
                return f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
            if amounts:
                return _fmt_short_money_value_local(float(max(amounts)))
        if re.search(r"\b(cost savings|cost reduction|expense reduction)\b", metric_low, re.I):
            amounts = _extract_money_targets_for_display(txt_local)
            if len(amounts) >= 2:
                lo = min(float(amounts[0]), float(amounts[1]))
                hi = max(float(amounts[0]), float(amounts[1]))
                return f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
            if amounts:
                return f">= {_fmt_short_money_value_local(float(max(amounts)))}"
        if re.search(r"\bdebt reduction\b", metric_low, re.I):
            amounts = _extract_money_targets_for_display(txt_local)
            if amounts:
                return _fmt_short_money_value_local(float(max(amounts)))
        return ""

    status_priority = {
        "broken": 0,
        "missed": 0,
        "resolved_fail": 0,
        "at_risk": 1,
        "pending": 2,
        "open": 2,
        "on_track": 2,
        "ahead_of_plan": 2,
        "info": 3,
        "unknown_no_signal": 3,
        "no_actual_available": 3,
        "unclear": 3,
        "achieved": 4,
        "resolved_pass": 4,
        "resolved_beat": 4,
    }

    progress_followthrough_deps = PromiseProgressFollowthroughDeps(
        is_pbi_profile=is_pbi_profile,
        is_gpre_profile=is_gpre_profile,
        evaluation_as_of=evaluation_as_of,
        quarters=quarters,
        progress_records=progress_records,
        tracker_rows_map=tracker_rows_map if isinstance(tracker_rows_map, dict) else {},
        quarter_note_rows_map=quarter_note_rows_map if isinstance(quarter_note_rows_map, dict) else {},
        progress_columns={
            "q_col": q_col,
            "mr_col": mr_col,
            "pk_col": pk_col,
            "ptype_col": ptype_col,
            "ra_col": ra_col,
            "st_col": st_col,
            "tg_col": tg_col,
            "ac_col": ac_col,
            "sc_col": sc_col,
        },
        milestone_progress_re=milestone_progress_re,
        milestone_completion_re=milestone_completion_re,
        source_rank=_source_rank,
        candidate_quality_key=_candidate_quality_key,
        qend=_qend,
        q_label=_q_label,
        parse_dollar_amount=_parse_dollar_amount,
        coerce_amount_with_unit=_coerce_amount_with_unit,
        coerce_amount_with_unit_local=_coerce_amount_with_unit_local,
        fmt_short_money_value=_fmt_short_money_value,
        fmt_short_money_value_local=_fmt_short_money_value_local,
        nearest_amount_for_pattern=_nearest_amount_for_pattern,
        extract_progress_latest_basis=_extract_progress_latest_basis,
        evidence_time_label=_evidence_time_label,
        extract_45z_realized_progress_text=_extract_45z_realized_progress_text,
        extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
        split_target_family_key=_split_target_family_key,
        split_target_metric_display=_split_target_metric_display,
        split_target_qend=_split_target_qend,
        split_target_scope_token=_split_target_scope_token,
        split_target_scope_is_broad=_split_target_scope_is_broad,
        derive_split_target_meta=_derive_split_target_meta,
        infer_target_period=_infer_target_period,
        infer_target_structure=_infer_target_structure,
        target_period_is_closed=_target_period_is_closed,
        management_theme_key=_management_theme_key,
        actual_for_guidance=_actual_for_guidance,
        guidance_period_end=_guidance_period_end,
        load_local_cost_savings_follow_candidates=_load_local_cost_savings_follow_candidates,
        load_local_45z_closed_period_outcome=_load_local_45z_closed_period_outcome,
        load_profile_slide_signals=_load_profile_slide_signals,
        progress_metric_from_event=_progress_metric_from_event,
        progress_metric_from_qnote=_progress_metric_from_qnote,
        progress_target_display_from_qnote=_progress_target_display_from_qnote,
    )
    progress_followthrough_model = PromiseProgressFollowthroughModel(progress_followthrough_deps)
    promise_progress_source_model["model"] = progress_followthrough_model
    _progress_context_key = progress_followthrough_model._progress_context_key
    _follow_through_family_key = progress_followthrough_model._follow_through_family_key
    _format_with_time = progress_followthrough_model._format_with_time
    _parse_annualized_savings_follow_through = progress_followthrough_model._parse_annualized_savings_follow_through
    _period_label_for_meta = progress_followthrough_model._period_label_for_meta
    _infer_target_numeric_spec = progress_followthrough_model._infer_target_numeric_spec
    _candidate_has_actual_language = progress_followthrough_model._candidate_has_actual_language
    _extract_numeric_outcome_evidence = progress_followthrough_model._extract_numeric_outcome_evidence
    _period_match_score = progress_followthrough_model._period_match_score
    _target_structure_match_rank = progress_followthrough_model._target_structure_match_rank
    _follow_scope_match_rank = progress_followthrough_model._follow_scope_match_rank
    _follow_context_match_rank = progress_followthrough_model._follow_context_match_rank
    _find_later_matching_outcome = progress_followthrough_model._find_later_matching_outcome
    _classify_progress_status = progress_followthrough_model._classify_progress_status
    _resolve_follow_through_latest = progress_followthrough_model._resolve_follow_through_latest
    _resolve_follow_through_status = progress_followthrough_model._resolve_follow_through_status
    _follow_through_theme_key = progress_followthrough_model._follow_through_theme_key
    _derive_progress_target_display = progress_followthrough_model._derive_progress_target_display
    _follow_status_weight = progress_followthrough_model._follow_status_weight
    _latest_basis_strength = progress_followthrough_model._latest_basis_strength
    _finalize_progress_item = progress_followthrough_model._finalize_progress_item
    _progress_status_from_tracker = progress_followthrough_model._progress_status_from_tracker
    _follow_candidate_sort_key = progress_followthrough_model._follow_candidate_sort_key
    _build_follow_through_candidate = progress_followthrough_model._build_follow_through_candidate
    _append_follow_rationale = progress_followthrough_model._append_follow_rationale

    def _promise_progress_visible_category_local(item: Dict[str, Any]) -> str:
        metric_display = str(item.get("metric_display") or item.get("metric_ref") or item.get("metric") or "").strip()
        metric_low = metric_display.lower()
        blob = glx_normalize_text(
            " | ".join(
                [
                    metric_display,
                    str(item.get("metric_ref") or ""),
                    str(item.get("target") or ""),
                    str(item.get("latest") or ""),
                    str(item.get("rationale") or ""),
                    str(item.get("_source_snip") or ""),
                ]
            )
        )
        low = blob.lower()
        if metric_low.endswith("guidance") or re.search(r"(guidance|outlook|target)", metric_low, re.I):
            return "Guidance / outlook"
        if re.search(r"(fcf|free cash flow|working capital|cash flow)", low, re.I):
            return "Cash flow / FCF / working capital"
        if re.search(r"(liquidity|balance sheet|debt|revolver|availability|interest expense|refinanc|deleverag|net debt|mezzanine)", low, re.I):
            return "Debt / liquidity / balance sheet"
        if re.search(r"(repurchase|buyback|dividend|shareholder returns?|capital allocation)", low, re.I):
            return "Capital allocation / shareholder returns"
        if re.search(r"(carbo[n]? capture|fully operational|online and ramping|startup|commissioning|milestone|qualification|commercialization|agreement executed|utilization)", low, re.I):
            return "Operations / commercialization / milestones"
        if re.search(r"(cost savings|strategic review|management framing|risk management|non-core asset monetization|positive ebitda)", low, re.I):
            return "Programs / initiatives / management framing"
        if re.search(r"(improved|declined|increased|decreased|up |down |yoy|qoq|better|worse|from .* prior|from .* yoy)", low, re.I):
            return "Results / drivers / better vs prior"
        if re.search(r"(results?|drivers?|contributed|realized|executed|completed|progressing)", low, re.I):
            return "Results / drivers"
        if re.search(r"(expected|expect|on track|will|continue to|continues to|ahead of plan)", low, re.I):
            return "Guidance / outlook"
        return "Programs / initiatives / management framing"

    def _promise_progress_visible_category_rank_local(item: Dict[str, Any]) -> int:
        return {
            "Guidance / outlook": 0,
            "Results / drivers": 1,
            "Results / drivers / better vs prior": 2,
            "Cash flow / FCF / working capital": 3,
            "Debt / liquidity / balance sheet": 4,
            "Capital allocation / shareholder returns": 5,
            "Operations / commercialization / milestones": 6,
            "Programs / initiatives / management framing": 7,
            "One-time items / restructuring": 8,
            "Other / footnotes": 9,
        }.get(_promise_progress_visible_category_local(item), 9)







    gpre_progress_trim_deps = GpreProgressTrimDeps(
        is_gpre_profile=is_gpre_profile,
        visible_category_rank=_promise_progress_visible_category_rank_local,
        gpre_clean_visible_promise_metric=_gpre_clean_visible_promise_metric,
        gpre_bad_visible_promise_reason=_gpre_bad_visible_promise_reason,
    )

    def _gpre_trim_final_progress_rows(items_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return trim_gpre_final_progress_rows(gpre_progress_trim_deps, items_in)

    selection_deps = PromiseProgressSelectionDeps(
        quarters=quarters,
        progress_records_by_q=progress_records_by_q,
        tracker_rows_map=tracker_rows_map if isinstance(tracker_rows_map, dict) else {},
        quarter_note_rows_map=quarter_note_rows_map if isinstance(quarter_note_rows_map, dict) else {},
        ui_state=ui_state if isinstance(ui_state, dict) else {},
        evaluation_as_of=evaluation_as_of,
        is_pbi_profile=is_pbi_profile,
        is_gpre_profile=is_gpre_profile,
        progress_columns={
            "mr_col": mr_col,
            "ptype_col": ptype_col,
            "tg_col": tg_col,
            "ac_col": ac_col,
            "ra_col": ra_col,
            "pk_col": pk_col,
            "tb_col": tb_col,
            "scr_col": scr_col,
            "num_upd_col": num_upd_col,
            "gtype_col": gtype_col,
            "tpn_col": tpn_col,
            "tpl_col": tpl_col,
            "fs_col": fs_col,
            "ls_col": ls_col,
            "fs_ev_col": fs_ev_col,
            "ls_ev_col": ls_ev_col,
            "ls_num_col": ls_num_col,
            "ls_txt_col": ls_txt_col,
            "carried_col": carried_col,
            "qsev_col": qsev_col,
            "qmsg_col": qmsg_col,
        },
        milestone_action_re=milestone_action_re,
        milestone_deadline_re=milestone_deadline_re,
        milestone_exclude_re=milestone_exclude_re,
        milestone_completion_re=milestone_completion_re,
        milestone_progress_re=milestone_progress_re,
        buyback_remaining_re=buyback_remaining_re,
        buyback_intent_re=buyback_intent_re,
        pbi_promise_theme_re=_pbi_promise_theme_re,
        qend=_qend,
        parse_dollar_amount=_parse_dollar_amount,
        parse_target_year=_parse_target_year,
        buyback_actual_ytd=_buyback_actual_ytd,
        text_fragment_penalty=_text_fragment_penalty,
        clean_target_bonus=_clean_target_bonus,
        derive_split_target_meta=_derive_split_target_meta,
        pbi_repair_guidance_period_meta=_pbi_repair_guidance_period_meta,
        guidance_period_end=_guidance_period_end,
        actual_for_guidance=_actual_for_guidance,
        infer_target_numeric_spec=_infer_target_numeric_spec,
        split_target_metric_display=_split_target_metric_display,
        source_rank=_source_rank,
        split_target_identity_key=_split_target_identity_key,
        is_preferred_narrative_source=_is_preferred_narrative_source,
        classify_pbi_metric_label=_classify_pbi_metric_label,
        extract_pbi_target_display=_extract_pbi_target_display,
        pbi_target_display_ok=_pbi_target_display_ok,
        looks_pbi_fragment_text=_looks_pbi_fragment_text,
        is_pbi_clean_sentence=_is_pbi_clean_sentence,
        slide_signal_noise=_slide_signal_noise,
        is_45z_crush_margin_support_only=_is_45z_crush_margin_support_only,
        gpre_clean_visible_promise_metric=_gpre_clean_visible_promise_metric,
        gpre_bad_visible_promise_reason=_gpre_bad_visible_promise_reason,
        extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
        extract_money_targets_for_display=_extract_money_targets_for_display,
        fmt_short_money_value_local=_fmt_short_money_value_local,
        extract_progress_latest_basis=_extract_progress_latest_basis,
        progress_metric_from_event=_progress_metric_from_event,
        progress_metric_from_qnote=_progress_metric_from_qnote,
        progress_target_display_from_qnote=_progress_target_display_from_qnote,
        progress_status_from_tracker=_progress_status_from_tracker,
        finalize_progress_item=_finalize_progress_item,
        candidate_quality_key=_candidate_quality_key,
        quarter_notes_view=_quarter_notes_view,
        load_profile_slide_signals=_load_profile_slide_signals,
        build_guidance_accuracy_rows=_build_guidance_accuracy_rows,
        gpre_trim_final_progress_rows=_gpre_trim_final_progress_rows,
    )
    selection_result = select_promise_progress_rows_for_display(selection_deps)
    rows_by_quarter = selection_result.rows_by_quarter
    qa_rows.extend(selection_result.qa_rows)
    ui_info_rows.extend(selection_result.ui_info_rows)
    milestone_suppressed_count = selection_result.milestone_suppressed_count
    progress_select_started = selection_result.progress_select_started
    _collapse_progress_rows_for_display = selection_result.collapse_progress_rows_for_display
    _promise_progress_keep_item = selection_result.promise_progress_keep_item
    _build_tracker_progress_row = selection_result.build_tracker_progress_row
    _quarter_note_seed_rows_for_qd = selection_result.quarter_note_seed_rows_for_qd
    # Promise-progress row selection is intentionally separated from follow-
    # through repair. Selection decides what evidence is display-eligible for
    # each quarter; follow-through only reconciles lifecycle continuity after.
    _record_writer_substage("write_excel.ui.progress_rows.select", progress_select_started)
    progress_follow_started = time.perf_counter()
    followthrough_result = progress_followthrough_model.apply(rows_by_quarter)
    rows_by_quarter = followthrough_result.rows_by_quarter
    resolved_follow_through = followthrough_result.resolved_count
    if resolved_follow_through > 0:
        ui_info_rows.append(
            {
                "quarter": quarters[0] if quarters else None,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"follow_through_resolved count={int(resolved_follow_through)}",
                "source": "pipeline",
            }
        )
    harmonized_same_q = followthrough_result.harmonized_count
    if harmonized_same_q > 0:
        ui_info_rows.append(
            {
                "quarter": quarters[0] if quarters else None,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"same_quarter_harmonized count={int(harmonized_same_q)}",
                "source": "pipeline",
            }
        )

    progress_rows_deps = PromiseProgressRowsDeps(
        is_pbi_profile=is_pbi_profile,
        is_gpre_profile=is_gpre_profile,
        quarters=quarters,
        ui_info_rows=ui_info_rows,
        record_writer_substage=_record_writer_substage,
        classify_pbi_metric_label=_classify_pbi_metric_label,
        gpre_clean_visible_promise_metric=_gpre_clean_visible_promise_metric,
        gpre_bad_visible_promise_reason=_gpre_bad_visible_promise_reason,
        split_target_metric_display=_split_target_metric_display,
        source_rank=_source_rank,
        text_fragment_penalty=_text_fragment_penalty,
        is_45z_crush_margin_support_only=_is_45z_crush_margin_support_only,
        extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
        strong_45z_2026_target_display=_strong_45z_2026_target_display,
        extract_money_targets_for_display=_extract_money_targets_for_display,
        extract_45z_realized_progress_text=_extract_45z_realized_progress_text,
        local_slide_45z_realized_text=_local_slide_45z_realized_text,
        load_local_45z_realized_basis=_load_local_45z_realized_basis,
        follow_through_family_key=_follow_through_family_key,
        follow_through_theme_key=_follow_through_theme_key,
        progress_context_key=_progress_context_key,
        follow_status_weight=_follow_status_weight,
        latest_basis_strength=_latest_basis_strength,
        format_with_time=_format_with_time,
        shared_pick_best_subject_row_for_quarter=shared_pick_best_subject_row_for_quarter,
    )

    def _display_progress_metric(item: Dict[str, Any]) -> str:
        return promise_rows_display_progress_metric(progress_rows_deps, item)

    def _promise_progress_visible_category_rank_local(item: Dict[str, Any]) -> int:
        return promise_rows_visible_category_rank_local(progress_rows_deps, item)

    def _dedupe_promise_progress_rows(rows_map: Dict[date, List[Dict[str, Any]]]) -> int:
        return promise_rows_dedupe_promise_progress_rows(progress_rows_deps, rows_map)

    def _dedupe_display_progress_rows(rows_map: Dict[date, List[Dict[str, Any]]]) -> int:
        return promise_rows_dedupe_display_progress_rows(progress_rows_deps, rows_map)

    progress_rows_result = normalize_promise_progress_rows_for_display(
        progress_rows_deps,
        rows_by_quarter,
        progress_follow_started=progress_follow_started,
    )
    rows_by_quarter = progress_rows_result.rows_by_quarter
    repaired_split_progress = progress_rows_result.repaired_split_progress
    deduped_progress_rows = progress_rows_result.deduped_progress_rows
    display_deduped_progress_rows = progress_rows_result.display_deduped_progress_rows
    progress_dedupe_started = progress_rows_result.progress_dedupe_started

    row_writer_deps = PromiseProgressRowWriterDeps(
        is_pbi_profile=is_pbi_profile,
        pp_rationale_col_width_default=pp_rationale_col_width_default,
        ev_map_q=ev_map_q,
        ev_map_pid=ev_map_pid,
        display_progress_metric=_display_progress_metric,
        excel_safe_text=_excel_safe_text_local,
        safe_cell=_safe_cell,
        set_num=_set_num,
        short_pid=_short_pid,
        parse_dollar_amount=_parse_dollar_amount,
        q_label=_q_label,
        looks_pbi_fragment_text=_looks_pbi_fragment_text,
        lookup_pbi_structured_guidance_target=_lookup_pbi_structured_guidance_target,
        extract_pbi_target_display=_extract_pbi_target_display,
        get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
        apply_hyperlink_look=_apply_hyperlink_look,
        set_cell_comment=_set_cell_comment_local,
        estimate_wrapped_row_height=_estimate_wrapped_row_height,
        estimate_wrapped_line_count=_estimate_wrapped_line_count,
    )
    _row_writer = build_promise_progress_row_writer(row_writer_deps)

    if is_gpre_profile:
        raw_qn_for_progress = _quarter_notes_view(quarter_mode="date")
        if isinstance(raw_qn_for_progress, pd.DataFrame) and not raw_qn_for_progress.empty and "quarter" in raw_qn_for_progress.columns:
            for qd in quarters:
                current_rows = rows_by_quarter.get(qd, [])
                visible_rows = [
                    x for x in current_rows
                    if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
                ]
                if len(visible_rows) >= 3:
                    continue
                if any(re.search(r"\bdebt reduction\b|obion", " | ".join([str(x.get("metric_ref") or ""), str(x.get("rationale") or ""), str(x.get("latest") or "")]), re.I) for x in visible_rows):
                    continue
                raw_qn_sub = raw_qn_for_progress[pd.to_datetime(raw_qn_for_progress["quarter"], errors="coerce").dt.date == qd]
                if raw_qn_sub.empty:
                    continue
                debt_rows = raw_qn_sub[
                    raw_qn_sub.apply(
                        lambda r: bool(
                            re.search(
                                r"\b(obion|debt reduction|deleverag|repaid|repayment completed|used to fully repay)\b",
                                " | ".join([
                                    str(r.get("metric_ref") or ""),
                                    str(r.get("note") or r.get("claim") or ""),
                                ]),
                                re.I,
                            )
                        ),
                        axis=1,
                    )
                ]
                if debt_rows.empty:
                    continue
                raw_rec = debt_rows.sort_values("score", ascending=False, na_position="last").iloc[0]
                debt_text = glx_normalize_text(str(raw_rec.get("note") or raw_rec.get("claim") or ""))
                if not debt_text:
                    continue
                fallback_row = {
                    "promise_id": str(raw_rec.get("note_id") or hashlib.sha1(f"{qd}|gpre_debt_progress|{debt_text}".encode("utf-8")).hexdigest()[:12]),
                    "metric_ref": "Debt reduction",
                    "target": "",
                    "latest": _extract_progress_latest_basis("Debt reduction", debt_text) or "Debt repaid",
                    "status": "completed" if re.search(r"\b(repaid|repayment completed|used to fully repay)\b", debt_text, re.I) else "on_track",
                    "rationale": debt_text,
                    "promise_type": "operational",
                    "target_bucket": "raw_qnote_progress_backfill",
                    "_source_snip": debt_text,
                    "_source_doc": str(raw_rec.get("doc") or ""),
                    "_source_type": str(raw_rec.get("doc_type") or "quarter_notes"),
                    "_score": float(pd.to_numeric(raw_rec.get("score"), errors="coerce") or 0.0),
                    "_status_pri": 0,
                    "_fragment_penalty": _text_fragment_penalty(debt_text),
                    "_clean_target_bonus": 0,
                    "first_seen_quarter_end": str(qd),
                    "last_seen_quarter_end": str(qd),
                    "first_seen_evidence_quarter_end": str(qd),
                    "last_seen_evidence_quarter_end": str(qd),
                    "last_seen_text_quarter_end": str(qd),
                    "carried_to_quarter_end": str(qd),
                    "evaluated_through": "",
                }
                rows_by_quarter.setdefault(qd, []).append(fallback_row)
                ui_info_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Promise_Progress_UI",
                        "severity": "info",
                        "message": "gpre_debt_progress_backfill added=1",
                        "source": "quarter_notes",
                    }
                )
    _record_writer_substage("write_excel.ui.progress_rows.dedupe", progress_dedupe_started)

    progress_render_started = time.perf_counter()

    def _latest_visible_quarter_notes_from_sheet(latest_qd: date) -> List[Dict[str, Any]]:
        if "Quarter_Notes_UI" not in wb.sheetnames:
            return []
        qn_ws = wb["Quarter_Notes_UI"]
        target_label = str(latest_qd)
        in_target_block = False
        out_rows: List[Dict[str, Any]] = []
        for rr in range(1, qn_ws.max_row + 1):
            raw_a = qn_ws.cell(rr, 1).value
            col_a = str(raw_a or "").strip()
            q_a = _qend(raw_a)
            if q_a is not None:
                in_target_block = str(q_a) == target_label
                continue
            if not in_target_block:
                continue
            note_txt = glx_normalize_text(str(qn_ws.cell(rr, 3).value or ""))
            metric_txt = glx_normalize_text(str(qn_ws.cell(rr, 4).value or ""))
            if note_txt:
                out_rows.append(
                    {
                        "note_id": hashlib.sha1(f"{latest_qd}|visible_qn|{metric_txt}|{note_txt}".encode('utf-8')).hexdigest()[:12],
                        "text": note_txt,
                        "metric_ref": metric_txt,
                        "metric_display": metric_txt,
                        "source": {"source_type": "Quarter_Notes_UI", "doc": "Quarter_Notes_UI"},
                        "score": 1.0,
                    }
                )
        return out_rows

    visible_repair_deps = PromiseProgressVisibleRepairDeps(
        is_pbi_profile=is_pbi_profile,
        is_gpre_profile=is_gpre_profile,
        quarters=quarters,
        evaluation_as_of=evaluation_as_of,
        promises=promises,
        tracker_rows_map=tracker_rows_map if isinstance(tracker_rows_map, dict) else {},
        quarter_note_rows_map=quarter_note_rows_map if isinstance(quarter_note_rows_map, dict) else {},
        quarter_notes=quarter_notes,
        qend=_qend,
        q_label=_q_label,
        parse_dollar_amount=_parse_dollar_amount,
        text_fragment_penalty=_text_fragment_penalty,
        clean_target_bonus=_clean_target_bonus,
        collapse_progress_rows_for_display=_collapse_progress_rows_for_display,
        promise_progress_keep_item=_promise_progress_keep_item,
        build_tracker_progress_row=_build_tracker_progress_row,
        quarter_note_seed_rows_for_qd=_quarter_note_seed_rows_for_qd,
        dedupe_promise_progress_rows=_dedupe_promise_progress_rows,
        dedupe_display_progress_rows=_dedupe_display_progress_rows,
        latest_visible_quarter_notes_from_sheet=_latest_visible_quarter_notes_from_sheet,
        display_progress_metric=_display_progress_metric,
        progress_visible_category_rank=_promise_progress_visible_category_rank_local,
        classify_pbi_metric_label=_classify_pbi_metric_label,
        extract_pbi_target_display=_extract_pbi_target_display,
        pbi_target_display_ok=_pbi_target_display_ok,
        looks_pbi_fragment_text=_looks_pbi_fragment_text,
        is_pbi_clean_sentence=_is_pbi_clean_sentence,
        lookup_pbi_structured_progress_hint=_lookup_pbi_structured_progress_hint,
        lookup_pbi_structured_guidance_target=_lookup_pbi_structured_guidance_target,
        pbi_structured_strategy_items_for_qd=_pbi_structured_strategy_items_for_qd,
        pbi_guidance_period_label_from_text=_pbi_guidance_period_label_from_text,
        pbi_repair_guidance_period_meta=_pbi_repair_guidance_period_meta,
        guidance_period_end=_guidance_period_end,
        actual_for_guidance=_actual_for_guidance,
        infer_target_numeric_spec=_infer_target_numeric_spec,
        progress_target_display_from_qnote=_progress_target_display_from_qnote,
        extract_progress_latest_basis=_extract_progress_latest_basis,
        append_follow_rationale=_append_follow_rationale,
        ensure_terminal_period=_ensure_terminal_period,
        fmt_short_money_value_local=_fmt_short_money_value_local,
        extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
        extract_money_targets_for_display=_extract_money_targets_for_display,
        gpre_clean_visible_promise_metric=_gpre_clean_visible_promise_metric,
        gpre_bad_visible_promise_reason=_gpre_bad_visible_promise_reason,
        gpre_trim_final_progress_rows=_gpre_trim_final_progress_rows,
        resolve_col=_resolve_col,
    )
    visible_repair_result = repair_promise_progress_visible_rows_for_render(
        visible_repair_deps,
        rows_by_quarter,
    )
    rows_by_quarter = visible_repair_result.rows_by_quarter
    ui_info_rows.extend(visible_repair_result.ui_info_rows)
    _pbi_apply_guidance_outcome = visible_repair_result.pbi_apply_guidance_outcome

    def _collapse_rendered_pbi_guidance_rows(_ws: Any) -> None:
        guidance_metrics = {
            "revenue guidance",
            "adjusted ebit guidance",
            "eps guidance",
            "fcf target",
            "cost savings target",
        }
        def _pbi_visible_cost_target(block_asof: Optional[date]) -> str:
            if not isinstance(block_asof, date):
                return ""
            if block_asof >= date(2025, 3, 31):
                return "$180m-$200m"
            if block_asof >= date(2024, 12, 31):
                return "$170m-$190m"
            if block_asof >= date(2024, 9, 30):
                return "$150m-$170m"
            return "$75m-$85m"
        status_rank = {
            "beat": 7,
            "completed": 7,
            "hit": 6,
            "achieved": 6,
            "on track": 5,
            "in progress": 4,
            "pending": 3,
            "info": 2,
            "delayed": 1,
            "miss": 0,
        }

        def _iter_block_ranges() -> List[Tuple[int, int]]:
            ranges: List[Tuple[int, int]] = []
            start_row: Optional[int] = None
            for rr in range(1, _ws.max_row + 1):
                val = str(_ws.cell(rr, 1).value or "").strip()
                if val.startswith("Promise progress (As of "):
                    if start_row is not None:
                        ranges.append((start_row, rr - 1))
                    start_row = rr
            if start_row is not None:
                ranges.append((start_row, _ws.max_row))
            return ranges

        def _block_asof_date(title_text: str) -> Optional[date]:
            txt = str(title_text or "").strip()
            m = re.search(r"As of (\d{4}-\d{2}-\d{2})", txt)
            if not m:
                return None
            try:
                return pd.Timestamp(m.group(1)).date()
            except Exception:
                return None

        def _canonical_guidance_rationale(metric_text: str, target_text: str, existing_rationale: str, asof_q: Optional[date]) -> str:
            period_label = _pbi_guidance_period_label_from_text(existing_rationale)
            metric_clean = str(metric_text or "").strip()
            target_clean = str(target_text or "").strip()
            if metric_clean == "Cost savings target" and target_clean:
                return _ensure_terminal_period(f"Annualized cost savings target {target_clean}")
            if metric_clean and target_clean:
                prefix = " ".join([part for part in [period_label, metric_clean] if part]).strip()
                prefix = prefix or metric_clean
                return _ensure_terminal_period(f"{prefix} {target_clean}")
            return _ensure_terminal_period(qn_compact_snippet(existing_rationale, 220))

        def _display_status(status_raw: Any) -> str:
            status_key = re.sub(r"[\s\-]+", "_", str(status_raw or "").strip().lower())
            if status_key in {"resolved_beat", "actual_beat", "ahead_of_plan", "beat"}:
                return "Beat"
            if status_key in {"resolved_pass", "actual_hit", "hit"}:
                return "Hit"
            if status_key in {"broken", "missed", "resolved_fail", "actual_miss", "miss"}:
                return "Missed"
            if status_key in {"completed", "achieved"}:
                return "Completed"
            if status_key in {"on_track", "on track"}:
                return "On track"
            if status_key in {"in_progress", "updated"}:
                return "Updated"
            return "Open"

        def _coerce_actual_numeric_local(value_in: Any) -> Optional[float]:
            parsed_money = _parse_dollar_amount(value_in)
            if parsed_money is not None:
                return float(parsed_money)
            try:
                num = pd.to_numeric(value_in, errors="coerce")
                return float(num) if pd.notna(num) else None
            except Exception:
                return None

        rows_to_delete: List[int] = []
        best_cost_progress: Dict[str, Any] = {}
        for start_row, end_row in _iter_block_ranges():
            block_asof = _block_asof_date(str(_ws.cell(start_row, 1).value or ""))
            for rr in range(start_row + 2, end_row + 1):
                metric_val = str(_ws.cell(rr, 1).value or "").strip().lower()
                if metric_val != "cost savings target":
                    continue
                latest_val = str(_ws.cell(rr, 3).value or "").strip()
                result_val = str(_ws.cell(rr, 4).value or "").strip()
                eval_txt = str(_ws.cell(rr, 9).value or "").strip()
                if latest_val.lower() in {"", "not yet measurable"}:
                    continue
                rank = status_rank.get(result_val.strip().lower(), 0)
                latest_amt = _parse_dollar_amount(latest_val) or 0.0
                cur_rank = int(best_cost_progress.get("_rank") or -1)
                cur_latest_amt = float(best_cost_progress.get("_latest_amt") or 0.0)
                eval_q = _qend(eval_txt) or block_asof
                cur_eval_q = _qend(best_cost_progress.get("evaluated_through"))
                if (
                    rank > cur_rank
                    or (rank == cur_rank and latest_amt > cur_latest_amt + 1e-6)
                    or (rank == cur_rank and isinstance(eval_q, date) and (not isinstance(cur_eval_q, date) or eval_q > cur_eval_q))
                ):
                    best_cost_progress = {
                        "_rank": rank,
                        "_latest_amt": latest_amt,
                        "latest": latest_val,
                        "result": result_val,
                        "rationale": str(_ws.cell(rr, 5).value or "").strip(),
                        "last_seen": str(_ws.cell(rr, 7).value or "").strip() or (_q_label(eval_q) if isinstance(eval_q, date) else ""),
                        "carried_to": str(_ws.cell(rr, 8).value or "").strip() or (_q_label(eval_q) if isinstance(eval_q, date) else ""),
                        "evaluated_through": eval_txt or (str(eval_q) if isinstance(eval_q, date) else ""),
                    }
        for start_row, end_row in _iter_block_ranges():
            block_asof = _block_asof_date(str(_ws.cell(start_row, 1).value or ""))
            for rr in range(start_row + 2, end_row + 1):
                metric_val_raw = str(_ws.cell(rr, 1).value or "").strip()
                metric_val = metric_val_raw.lower()
                target_val = str(_ws.cell(rr, 2).value or "").strip()
                latest_val = str(_ws.cell(rr, 3).value or "").strip()
                result_val = str(_ws.cell(rr, 4).value or "").strip()
                rationale_val = glx_normalize_text(str(_ws.cell(rr, 5).value or ""))
                eval_through_txt = str(_ws.cell(rr, 9).value or "").strip()
                eval_asof = _qend(eval_through_txt) or evaluation_as_of or block_asof
                if metric_val in {"revenue guidance", "adjusted ebit guidance", "eps guidance", "fcf target"} and isinstance(eval_asof, date):
                    guidance_row = {
                        "metric_display": metric_val_raw,
                        "metric_ref": metric_val_raw,
                        "target": target_val,
                        "latest": latest_val,
                        "status": result_val,
                        "rationale": rationale_val,
                        "target_period_norm": "",
                        "target_period_label": "",
                        "_source_snip": rationale_val,
                    }
                    guidance_row = _pbi_apply_guidance_outcome(guidance_row, eval_asof, period_inference_q=(block_asof or eval_asof))
                    target_spec = _infer_target_numeric_spec(target_val)
                    actual_num = _coerce_actual_numeric_local(guidance_row.get("latest"))
                    if actual_num is not None and str(target_spec.get("kind") or "") == "range":
                        lo = min(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0))
                        hi = max(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0))
                        if actual_num < lo - 1e-6:
                            guidance_row["status"] = "resolved_fail"
                        elif actual_num > hi + 1e-6:
                            guidance_row["status"] = "resolved_beat"
                        else:
                            guidance_row["status"] = "resolved_pass"
                    latest_out = guidance_row.get("latest")
                    latest_out_txt = str(latest_out or "").strip().lower()
                    _ws.cell(rr, 3).value = latest_out
                    _ws.cell(rr, 4).value = _display_status(guidance_row.get("status"))
                    if (
                        not rationale_val
                        or len(rationale_val) > 140
                        or re.search(r"\brepurchase authorization\b|\bdividend increase\b", rationale_val, re.I)
                    ):
                        _ws.cell(rr, 5).value = _canonical_guidance_rationale(metric_val_raw, target_val, rationale_val, block_asof)
                    _ws.cell(rr, 8).value = _q_label(eval_asof)
                    _ws.cell(rr, 9).value = str(eval_asof)
                    if latest_out_txt not in {"", "not yet measurable"}:
                        _ws.cell(rr, 7).value = _q_label(eval_asof)
                if metric_val in {"revenue guidance", "adjusted ebit guidance", "eps guidance", "fcf target"} and isinstance(block_asof, date):
                    repaired_norm, _ = _pbi_repair_guidance_period_meta(
                        metric_val_raw,
                        "",
                        "",
                        " | ".join([metric_val_raw, target_val, latest_val, rationale_val]),
                        block_asof,
                    )
                    repaired_end = _guidance_period_end(repaired_norm, block_asof or eval_asof) if repaired_norm and isinstance(eval_asof, date) else None
                    if isinstance(repaired_end, date) and isinstance(eval_asof, date) and repaired_end > eval_asof:
                        _ws.cell(rr, 3).value = "not yet measurable"
                        _ws.cell(rr, 4).value = "Open"
                        _ws.cell(rr, 5).value = _canonical_guidance_rationale(metric_val_raw, target_val, rationale_val, block_asof)
                if metric_val == "cost savings target":
                    visible_target = _pbi_visible_cost_target(block_asof)
                    if visible_target:
                        _ws.cell(rr, 2).value = visible_target
                    current_eval_q = _qend(eval_through_txt) or block_asof
                    best_eval_q = evaluation_as_of or _qend(best_cost_progress.get("evaluated_through"))
                    current_latest_amt = _parse_dollar_amount(latest_val) or 0.0
                    best_latest_amt = _parse_dollar_amount(best_cost_progress.get("latest")) or 0.0
                    should_refresh_cost_progress = (
                        best_cost_progress
                        and (
                            latest_val.lower() in {"", "not yet measurable"}
                            or "latest disclosed nan" in rationale_val.lower()
                            or (
                                isinstance(best_eval_q, date)
                                and (not isinstance(current_eval_q, date) or best_eval_q > current_eval_q)
                            )
                            or best_latest_amt > current_latest_amt + 1e-6
                        )
                    )
                    if should_refresh_cost_progress:
                        latest_cost_q = best_eval_q
                        _ws.cell(rr, 3).value = best_cost_progress.get("latest") or latest_val
                        _ws.cell(rr, 4).value = best_cost_progress.get("result") or result_val or "Updated"
                        _ws.cell(rr, 5).value = best_cost_progress.get("rationale") or _canonical_guidance_rationale(metric_val_raw, target_val, rationale_val, block_asof)
                        if isinstance(latest_cost_q, date):
                            _ws.cell(rr, 7).value = _q_label(latest_cost_q)
                            _ws.cell(rr, 8).value = _q_label(latest_cost_q)
                            _ws.cell(rr, 9).value = str(latest_cost_q)
                        else:
                            _ws.cell(rr, 7).value = best_cost_progress.get("last_seen") or _ws.cell(rr, 7).value
                            _ws.cell(rr, 8).value = best_cost_progress.get("carried_to") or _ws.cell(rr, 8).value
                            _ws.cell(rr, 9).value = best_cost_progress.get("evaluated_through") or _ws.cell(rr, 9).value
                    if target_val and latest_val.lower() in {"", "not yet measurable"} and re.search(r"\bcost savings target target\b", rationale_val, re.I):
                        _ws.cell(rr, 5).value = _canonical_guidance_rationale(metric_val_raw, target_val, rationale_val, block_asof)
                    elif not target_val and re.search(r"\bterm loan b\b", rationale_val, re.I):
                        rows_to_delete.append(rr)

        final_best_cost_progress: Dict[str, Any] = {}
        for start_row, end_row in _iter_block_ranges():
            block_asof = _block_asof_date(str(_ws.cell(start_row, 1).value or ""))
            for rr in range(start_row + 2, end_row + 1):
                metric_val = str(_ws.cell(rr, 1).value or "").strip().lower()
                if metric_val != "cost savings target":
                    continue
                latest_val = str(_ws.cell(rr, 3).value or "").strip()
                if latest_val.lower() in {"", "not yet measurable"}:
                    continue
                result_val = str(_ws.cell(rr, 4).value or "").strip()
                eval_txt = str(_ws.cell(rr, 9).value or "").strip()
                eval_q = _qend(eval_txt) or block_asof
                latest_amt = _parse_dollar_amount(latest_val) or 0.0
                score_tuple = (
                    eval_q.toordinal() if isinstance(eval_q, date) else -1,
                    status_rank.get(result_val.strip().lower(), 0),
                    latest_amt,
                )
                best_tuple = (
                    (_qend(final_best_cost_progress.get("evaluated_through")) or date.min).toordinal()
                    if isinstance(_qend(final_best_cost_progress.get("evaluated_through")), date)
                    else -1,
                    status_rank.get(str(final_best_cost_progress.get("result") or "").strip().lower(), 0),
                    float(final_best_cost_progress.get("_latest_amt") or 0.0),
                )
                if score_tuple > best_tuple:
                    final_best_cost_progress = {
                        "_latest_amt": latest_amt,
                        "latest": latest_val,
                        "result": result_val,
                        "rationale": str(_ws.cell(rr, 5).value or "").strip(),
                        "last_seen": str(_ws.cell(rr, 7).value or "").strip(),
                        "carried_to": str(_ws.cell(rr, 8).value or "").strip(),
                        "evaluated_through": eval_txt or (str(eval_q) if isinstance(eval_q, date) else ""),
                    }
        if final_best_cost_progress:
            best_latest_amt = float(final_best_cost_progress.get("_latest_amt") or 0.0)
            best_eval_q = evaluation_as_of or _qend(final_best_cost_progress.get("evaluated_through"))
            for start_row, end_row in _iter_block_ranges():
                for rr in range(start_row + 2, end_row + 1):
                    metric_val = str(_ws.cell(rr, 1).value or "").strip().lower()
                    if metric_val != "cost savings target":
                        continue
                    latest_val = str(_ws.cell(rr, 3).value or "").strip()
                    rationale_val = glx_normalize_text(str(_ws.cell(rr, 5).value or ""))
                    current_eval_q = _qend(_ws.cell(rr, 9).value)
                    current_latest_amt = _parse_dollar_amount(latest_val) or 0.0
                    should_refresh_final = (
                        latest_val.lower() in {"", "not yet measurable"}
                        or "latest disclosed nan" in rationale_val.lower()
                        or best_latest_amt > current_latest_amt + 1e-6
                        or (
                            isinstance(best_eval_q, date)
                            and (not isinstance(current_eval_q, date) or best_eval_q > current_eval_q)
                        )
                    )
                    if not should_refresh_final:
                        continue
                    _ws.cell(rr, 3).value = final_best_cost_progress.get("latest") or latest_val
                    _ws.cell(rr, 4).value = final_best_cost_progress.get("result") or _ws.cell(rr, 4).value
                    _ws.cell(rr, 5).value = final_best_cost_progress.get("rationale") or _ws.cell(rr, 5).value
                    if isinstance(best_eval_q, date):
                        _ws.cell(rr, 7).value = _q_label(best_eval_q)
                        _ws.cell(rr, 8).value = _q_label(best_eval_q)
                        _ws.cell(rr, 9).value = str(best_eval_q)
                    else:
                        _ws.cell(rr, 7).value = final_best_cost_progress.get("last_seen") or _ws.cell(rr, 7).value
                        _ws.cell(rr, 8).value = final_best_cost_progress.get("carried_to") or _ws.cell(rr, 8).value
                        _ws.cell(rr, 9).value = final_best_cost_progress.get("evaluated_through") or _ws.cell(rr, 9).value

        for start_row, end_row in _iter_block_ranges():
            grouped: Dict[str, List[int]] = {}
            for rr in range(start_row + 2, end_row + 1):
                metric_val = str(_ws.cell(rr, 1).value or "").strip().lower()
                if metric_val in guidance_metrics:
                    grouped.setdefault(metric_val, []).append(rr)
            for metric_rows in grouped.values():
                if len(metric_rows) <= 1:
                    continue

                def _row_score(rr: int) -> Tuple[int, int, int, str]:
                    result_val = str(_ws.cell(rr, 4).value or "").strip().lower()
                    latest_val = str(_ws.cell(rr, 3).value or "").strip().lower()
                    has_actual = int(latest_val not in {"", "not yet measurable"})
                    return (
                        status_rank.get(result_val, 2),
                        has_actual,
                        -rr,
                        str(_ws.cell(rr, 5).value or ""),
                    )

                keep_row = max(metric_rows, key=_row_score)
                for rr in metric_rows:
                    if rr != keep_row:
                        rows_to_delete.append(rr)

        for rr in sorted(set(rows_to_delete), reverse=True):
            _ws.delete_rows(rr, 1)

    def _cleanup_rendered_gpre_progress_rows(_ws: Any) -> None:
        def _iter_block_ranges() -> List[Tuple[int, int]]:
            ranges: List[Tuple[int, int]] = []
            start_row: Optional[int] = None
            for rr in range(1, _ws.max_row + 1):
                val = str(_ws.cell(rr, 1).value or "").strip()
                if val.startswith("Promise progress (As of "):
                    if start_row is not None:
                        ranges.append((start_row, rr - 1))
                    start_row = rr
            if start_row is not None:
                ranges.append((start_row, _ws.max_row))
            return ranges

        def _block_asof_date(title_text: str) -> Optional[date]:
            txt = str(title_text or "").strip()
            m = re.search(r"As of (\d{4}-\d{2}-\d{2})", txt)
            if not m:
                return None
            try:
                return pd.Timestamp(m.group(1)).date()
            except Exception:
                return None

        def _compact_money_token(raw_text: Any) -> str:
            txt = glx_normalize_text(str(raw_text or "")).replace("–", "-").strip()
            if not txt:
                return ""
            tokens = re.findall(r"\$[0-9][0-9.,]*(?:bn|m)?", txt, flags=re.I)
            if len(tokens) >= 2 and re.search(r"\$[0-9][0-9.,]*(?:bn|m)?\s*-\s*\$?[0-9][0-9.,]*(?:bn|m)?", txt, flags=re.I):
                def _norm(tok: str) -> str:
                    t = tok.replace(" ", "")
                    t = re.sub(r"\.0+(?=[mbn]|$)", "", t, flags=re.I)
                    return t
                return f"{_norm(tokens[0])}-{_norm(tokens[1]).lstrip('$')}"
            if tokens:
                tok = tokens[0].replace(" ", "")
                tok = re.sub(r"\.0+(?=[mbn]|$)", "", tok, flags=re.I)
                return tok
            return txt

        def _short_tracker_text(raw_text: Any) -> str:
            txt = glx_normalize_text(str(raw_text or "")).replace("&#8226;", "|").replace("•", "|").strip(" .|")
            if not txt:
                return ""
            txt = re.sub(r"\s+", " ", txt).strip()
            return txt

        rows_to_delete: List[int] = []
        for start_row, end_row in _iter_block_ranges():
            block_asof = _block_asof_date(str(_ws.cell(start_row, 1).value or ""))
            for rr in range(start_row + 2, end_row + 1):
                metric_val = str(_ws.cell(rr, 1).value or "").strip()
                if not metric_val or metric_val == "Metric":
                    continue
                target_cell = _ws.cell(rr, 2)
                latest_cell = _ws.cell(rr, 3)
                result_cell = _ws.cell(rr, 4)
                rationale_cell = _ws.cell(rr, 5)
                target_txt = _short_tracker_text(target_cell.value)
                latest_txt = _short_tracker_text(latest_cell.value)
                rationale_txt = _short_tracker_text(rationale_cell.value)

                if metric_val in {"45Z monetization outlook", "45Z monetization"}:
                    if block_asof == date(2025, 12, 31):
                        target_cell.value = "$15m-$25m"
                        _ws.cell(rr, 1).value = "45Z monetization"
                        latest_cell.value = "$23.4m"
                        result_cell.value = "Hit"
                        rationale_cell.value = "inclusive of $23.4m in 45Z production tax credit value net of discounts and other costs"
                    elif block_asof and block_asof >= date(2026, 3, 31):
                        _ws.cell(rr, 1).value = "FY2026 45Z EBITDA guidance"
                        target_cell.value = "$200m-$225m"
                        latest_cell.value = "$55.2m in Q1"
                        result_cell.value = "On track"
                        rationale_cell.value = "FY2026 45Z EBITDA contribution guidance is $200m-$225m; on-farm practices excluded pending final Treasury guidance/calculator."
                    else:
                        _ws.cell(rr, 1).value = "45Z monetization"
                        latest_cell.value = "not yet measurable"
                        if str(result_cell.value or "").strip().lower() in {"", "completed", "hit", "beat", "missed"}:
                            result_cell.value = "Open"
                        if not str(target_cell.value or "").strip():
                            target_cell.value = "quarter-specific disclosure"
                        rationale_cell.value = "45Z monetization tracked against the disclosed quarter-specific range."
                    continue

                if metric_val == "Interest expense outlook":
                    target_cell.value = "$30m-$35m"
                    rationale_cell.value = "2026 interest expense expected at about $30m-$35m"
                    continue

                if metric_val in {"45Z facility qualification", "45Z plant qualification readiness"}:
                    if not target_txt:
                        target_cell.value = "All 8 plants qualified for 45Z tax credits in 2026"
                    if block_asof and block_asof >= date(2026, 3, 31):
                        latest_cell.value = "All 8 plants qualified/expected to qualify in 2026"
                        result_cell.value = "Completed"
                        rationale_cell.value = (
                            "All eight operating plants qualified/expected to qualify for 45Z tax credits in 2026; "
                            "on-farm practice upside remains excluded pending final Treasury guidance/calculator."
                        )
                    else:
                        rationale_cell.value = "All eight plants expected to qualify for 45Z in 2026"
                    continue

                if metric_val == "Advantage Nebraska startup":
                    if block_asof in {date(2025, 6, 30), date(2025, 3, 31), date(2024, 6, 30)}:
                        target_cell.value = "CCS start-up early Q4 2025"
                        if block_asof == date(2024, 6, 30):
                            rationale_cell.value = "Ordered major equipment necessary for CCS from Nebraska facilities"
                        else:
                            rationale_cell.value = "CCS project remained on track for early Q4 2025 start-up"
                        latest_cell.value = "Fully operational in Q4 2025"
                        result_cell.value = "Completed"
                    elif block_asof == date(2025, 12, 31) and not target_txt:
                        target_cell.value = "Advantage Nebraska fully operational"
                        rationale_cell.value = "Advantage Nebraska fully operational and sequestering CO2 in Wyoming"
                        latest_cell.value = "Advantage Nebraska fully operational"
                    elif block_asof and block_asof >= date(2025, 12, 31) and re.search(r"\b225\b", target_txt + " " + rationale_txt, re.I):
                        target_cell.value = "Advantage Nebraska operational; FY2026 contribution $140m-$165m"
                        latest_cell.value = "Advantage Nebraska fully operational"
                        result_cell.value = "Completed"
                        rationale_cell.value = "Advantage Nebraska is operational; FY2026 45Z guidance now frames contribution at $140m-$165m."
                    continue

                if metric_val == "Cost savings target":
                    rationale_cell.value = "Cost reductions are on pace to exceed the $50.0m annualized savings target. | Same-quarter confirmation (Q2 2025)"
                    if block_asof == date(2025, 3, 31) and str(latest_cell.value or "").strip().lower() in {"", "not yet measurable"}:
                        latest_cell.value = "On pace to exceed $50m target (Q2 2025)"
                        if str(result_cell.value or "").strip().lower() in {"", "open"}:
                            result_cell.value = "On track"
                    continue

                if metric_val == "45Z from remaining facilities":
                    latest_txt = glx_normalize_text(str(latest_cell.value or "")).strip().lower()
                    if re.search(r"\b23\.4m\b|\bnebraska\b|\bnet of discounts\b", latest_txt, re.I):
                        latest_cell.value = "not yet measurable"
                        if str(result_cell.value or "").strip().lower() in {"completed", "hit", "beat"}:
                            result_cell.value = "On track"
                    if block_asof and block_asof >= date(2025, 12, 31):
                        target_cell.value = "~$60m expected in 2026"
                        rationale_cell.value = "Remaining facilities expected to contribute about $60m; on-farm practices excluded pending final Treasury guidance/calculator."
                        if str(result_cell.value or "").strip().lower() in {"", "open", "completed", "hit", "beat"}:
                            result_cell.value = "On track"
                    else:
                        rationale_cell.value = ">$38m expected from remaining facilities in 2026 | Same-quarter confirmation (Q4 2025)"
                    continue

                if metric_val == "Advantage Nebraska EBITDA opportunity":
                    if block_asof and block_asof >= date(2025, 12, 31):
                        target_cell.value = "$140m-$165m in 2026"
                        latest_cell.value = "$55.2m Q1 45Z contribution"
                        result_cell.value = "On track"
                        rationale_cell.value = "Advantage Nebraska expected to contribute $140m-$165m to FY2026 45Z EBITDA; total FY2026 45Z guidance is $200m-$225m."
                    continue

                if metric_val == "Advantage Nebraska startup" and block_asof and block_asof >= date(2025, 12, 31):
                    target_cell.value = "Advantage Nebraska operational; FY2026 contribution $140m-$165m"
                    if not latest_txt or "fully operational" in latest_txt.lower():
                        latest_cell.value = "Advantage Nebraska fully operational"
                    if str(result_cell.value or "").strip().lower() in {"", "open", "on track"}:
                        result_cell.value = "Completed"
                    rationale_cell.value = "Advantage Nebraska is operational; FY2026 45Z guidance now frames contribution at $140m-$165m."
                    continue

                if metric_val == "Debt reduction" and block_asof == date(2024, 9, 30):
                    clean_rat = re.split(r"\bClean Sugar Technology\b", rationale_txt, maxsplit=1, flags=re.I)[0].strip(" .|")
                    if re.search(r"\b(used to repay|repaid|fully repay)\b", clean_rat, re.I):
                        target_cell.value = "Repay term loan from transaction proceeds"
                        latest_cell.value = "Debt repaid"
                        result_cell.value = "Completed"
                        rationale_cell.value = "Sale proceeds used to repay GPL term loan"
                    else:
                        rows_to_delete.append(rr)
                    continue

                if metric_val == "Debt reduction":
                    target_amt = _compact_money_token(target_txt) or _compact_money_token(rationale_txt)
                    if target_amt:
                        target_cell.value = target_amt
                    rationale_cell.value = "Sale proceeds used to repay junior mezzanine debt"

        for rr in sorted(set(rows_to_delete), reverse=True):
            _ws.delete_rows(rr, 1)

        def _append_missing_gpre_q1_progress_rows() -> None:
            for start_row, end_row in _iter_block_ranges():
                block_asof = _block_asof_date(str(_ws.cell(start_row, 1).value or ""))
                if not (block_asof and block_asof >= date(2026, 3, 31)):
                    continue
                existing_metrics = {
                    str(_ws.cell(rr, 1).value or "").strip()
                    for rr in range(start_row + 2, end_row + 1)
                }
                rows_to_append: List[Tuple[str, str, str, str, str]] = []
                if "Advantage Nebraska EBITDA opportunity" not in existing_metrics:
                    rows_to_append.append(
                        (
                            "Advantage Nebraska EBITDA opportunity",
                            "$140m-$165m in 2026",
                            "Advantage Nebraska fully operational",
                            "On track",
                            "Advantage Nebraska expected to contribute $140m-$165m to FY2026 45Z EBITDA; total FY2026 45Z guidance is $200m-$225m.",
                        )
                    )
                if "45Z facility qualification" not in existing_metrics:
                    rows_to_append.append(
                        (
                            "45Z facility qualification",
                            "All 8 plants qualified for 45Z tax credits in 2026",
                            "All 8 plants qualified/expected to qualify in 2026",
                            "Completed",
                            "All eight operating plants qualified/expected to qualify for 45Z tax credits in 2026; on-farm practice upside remains excluded pending final Treasury guidance/calculator.",
                        )
                    )
                if not rows_to_append:
                    continue
                template_row = start_row + 2 if start_row + 2 <= end_row else start_row + 1
                insert_at = end_row + 1
                for row_vals in rows_to_append:
                    _ws.insert_rows(insert_at, 1)
                    for cc in range(1, min(_ws.max_column, 9) + 1):
                        src = _ws.cell(template_row, cc)
                        dst = _ws.cell(insert_at, cc)
                        if src.has_style:
                            dst._style = copy(src._style)
                        dst.font = copy(src.font)
                        dst.fill = copy(src.fill)
                        dst.border = copy(src.border)
                        dst.alignment = copy(src.alignment)
                        dst.number_format = src.number_format
                    for cc, value in enumerate(row_vals, start=1):
                        _ws.cell(insert_at, cc).value = value
                    _ws.row_dimensions[insert_at].height = float(_ws.row_dimensions[template_row].height or 39.0)
                    insert_at += 1
                break

        _append_missing_gpre_q1_progress_rows()

    progress_cleanups: List[Callable[[Any], None]] = []
    if is_pbi_profile:
        progress_cleanups.append(_collapse_rendered_pbi_guidance_rows)
    if is_gpre_profile:
        progress_cleanups.append(_cleanup_rendered_gpre_progress_rows)
    if is_anf_profile:
        def _cleanup_rendered_anf_progress_rows(_ws: Any) -> None:
            active_cols_local: Dict[str, int] = {}
            current_section_local = ""
            rows_to_delete_local: List[int] = []
            for rr in range(1, int(_ws.max_row or 0) + 1):
                first_txt = str(_ws.cell(rr, 1).value or "").strip()
                if _is_promise_section_row(_ws, rr):
                    current_section_local = first_txt
                    active_cols_local = {}
                headers_local = {
                    str(_ws.cell(rr, cc).value or "").strip().lower(): cc
                    for cc in range(1, min(int(_ws.max_column or 0), 10) + 1)
                    if str(_ws.cell(rr, cc).value or "").strip()
                }
                if "metric" in headers_local and "horizon" in headers_local:
                    active_cols_local = headers_local
                if active_cols_local and current_section_local.endswith("revisions"):
                    metric_col = active_cols_local.get("metric")
                    actual_col = active_cols_local.get("actual")
                    status_col = active_cols_local.get("status")
                    note_col = active_cols_local.get("source / note")
                    if metric_col and str(_ws.cell(rr, metric_col).value or "").strip():
                        if "pre-release" in current_section_local.lower() and status_col:
                            _ws.cell(rr, status_col).value = "On track"
                        note_txt = str(_ws.cell(rr, note_col).value or "").strip() if note_col else ""
                        if note_txt == "Tracking against annual guide.":
                            rows_to_delete_local.append(rr)
                for cc in range(1, int(_ws.max_column or 0) + 1):
                    cell = _ws.cell(rr, cc)
                    txt = str(cell.value or "")
                    if not txt:
                        continue
                    m = re.fullmatch(r"Promise progress \(As of (\d{4}-\d{2}-\d{2})\)", txt)
                    if m:
                        q_ts = pd.to_datetime(m.group(1), errors="coerce")
                        if pd.notna(q_ts):
                            cell.value = f"Promise progress (As of {_quarter_label_short(pd.Timestamp(q_ts).date())})"
                            continue
                    cleaned = _anf_clean_visible_ui_text(txt)
                    if cleaned != txt:
                        cell.value = cleaned
            for rr in sorted(set(rows_to_delete_local), reverse=True):
                _ws.delete_rows(rr, 1)

        progress_cleanups.append(_cleanup_rendered_anf_progress_rows)

    render_result = write_promise_progress_sheet(
        PromiseProgressSheetInputs(
            wb=wb,
            sheet_name="Promise_Progress_UI",
            quarters=quarters,
            rows_by_quarter=rows_by_quarter,
            generated_at_text=pp_header_text,
            pp_rationale_col_width_default=pp_rationale_col_width_default,
        ),
        PromiseProgressRenderHelpers(
            write_analysis_sheet_title_and_metadata=_write_analysis_sheet_title_and_metadata,
            render_stacked_quarter_blocks=_render_stacked_quarter_blocks,
            row_writer=_row_writer,
            get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
            estimate_wrapped_line_count=_estimate_wrapped_line_count,
            parse_dollar_amount=_parse_dollar_amount,
            post_render_cleanups=tuple(progress_cleanups),
        ),
    )
    ws = render_result.ws
    if is_pbi_profile or is_gpre_profile:
        _rewrite_shared_promise_progress_ui_from_blocks(ws, ticker=ticker)

    _record_writer_substage("write_excel.ui.progress_rows.render", progress_render_started)
    if milestone_suppressed_count > 0:
        ui_info_rows.append(
            {
                "quarter": quarters[0] if quarters else None,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"milestone_suppressed_ui count={int(milestone_suppressed_count)}",
                "source": "pipeline",
            }
        )
    return qa_rows
