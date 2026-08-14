"""Promise Progress row selection/model helpers."""
from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .filing_evidence_shared import (
    build_canonical_subject_key as shared_build_canonical_subject_key,
    build_evidence_event as shared_build_evidence_event,
    build_follow_through_event as shared_build_follow_through_event,
    build_lifecycle_subject_key as shared_build_lifecycle_subject_key,
    build_parent_subject_key as shared_build_parent_subject_key,
    build_promise_lifecycle_key as shared_build_promise_lifecycle_key,
    derive_lifecycle_state as shared_derive_lifecycle_state,
    derive_status_resolution_reason as shared_derive_status_resolution_reason,
    evidence_role as shared_evidence_role,
    infer_target_period_norm as shared_infer_target_period_norm,
    pick_best_subject_row_for_quarter as shared_pick_best_subject_row_for_quarter,
    progress_status_rank as shared_progress_status_rank,
    qualify_promise_candidate as shared_qualify_promise_candidate,
    source_class as shared_source_class,
    statement_class as shared_statement_class,
)
from .guidance_lexicon import normalize_text as glx_normalize_text
from .quarter_notes_lexicon import compact_snippet as qn_compact_snippet


@dataclass(frozen=True)
class PromiseProgressSelectionDeps:
    quarters: Sequence[date]
    progress_records_by_q: Mapping[date, List[Dict[str, Any]]]
    tracker_rows_map: Mapping[date, List[Dict[str, Any]]]
    quarter_note_rows_map: Mapping[Any, List[Dict[str, Any]]]
    ui_state: Mapping[str, Any]
    evaluation_as_of: Optional[date]
    is_pbi_profile: bool
    is_gpre_profile: bool
    progress_columns: Mapping[str, Optional[str]]
    milestone_action_re: Any
    milestone_deadline_re: Any
    milestone_exclude_re: Any
    milestone_completion_re: Any
    milestone_progress_re: Any
    buyback_remaining_re: Any
    buyback_intent_re: Any
    pbi_promise_theme_re: Any
    qend: Callable[..., Any]
    parse_dollar_amount: Callable[..., Any]
    parse_target_year: Callable[..., Any]
    buyback_actual_ytd: Callable[..., Any]
    text_fragment_penalty: Callable[..., Any]
    clean_target_bonus: Callable[..., Any]
    derive_split_target_meta: Callable[..., Dict[str, Any]]
    pbi_repair_guidance_period_meta: Callable[..., Tuple[str, str]]
    guidance_period_end: Callable[..., Any]
    actual_for_guidance: Callable[..., Any]
    infer_target_numeric_spec: Callable[..., Dict[str, Any]]
    split_target_metric_display: Callable[..., str]
    source_rank: Callable[..., Any]
    split_target_identity_key: Callable[..., Tuple[str, str, str]]
    is_preferred_narrative_source: Callable[..., bool]
    classify_pbi_metric_label: Callable[..., str]
    extract_pbi_target_display: Callable[..., str]
    pbi_target_display_ok: Callable[..., bool]
    looks_pbi_fragment_text: Callable[..., bool]
    is_pbi_clean_sentence: Callable[..., bool]
    slide_signal_noise: Callable[..., bool]
    is_45z_crush_margin_support_only: Callable[..., bool]
    gpre_clean_visible_promise_metric: Callable[..., str]
    gpre_bad_visible_promise_reason: Callable[..., bool]
    extract_45z_monetization_target_display: Callable[..., str]
    extract_money_targets_for_display: Callable[..., Any]
    fmt_short_money_value_local: Callable[..., str]
    extract_progress_latest_basis: Callable[..., str]
    progress_metric_from_event: Callable[..., str]
    progress_metric_from_qnote: Callable[..., str]
    progress_target_display_from_qnote: Callable[..., str]
    progress_status_from_tracker: Callable[..., str]
    finalize_progress_item: Callable[..., Optional[Dict[str, Any]]]
    candidate_quality_key: Callable[..., Any]
    quarter_notes_view: Callable[..., Any]
    load_profile_slide_signals: Callable[[], List[Dict[str, Any]]]
    build_guidance_accuracy_rows: Callable[[date], List[Dict[str, Any]]]
    gpre_trim_final_progress_rows: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]


@dataclass(frozen=True)
class PromiseProgressSelectionResult:
    rows_by_quarter: Dict[date, List[Dict[str, Any]]]
    qa_rows: List[Dict[str, Any]]
    ui_info_rows: List[Dict[str, Any]]
    milestone_suppressed_count: int
    progress_select_started: float
    collapse_progress_rows_for_display: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]
    promise_progress_keep_item: Callable[[Dict[str, Any]], bool]
    build_tracker_progress_row: Callable[[date, Dict[str, Any]], Optional[Dict[str, Any]]]
    quarter_note_seed_rows_for_qd: Callable[[date], List[Dict[str, Any]]]


def select_promise_progress_rows_for_display(
    deps: PromiseProgressSelectionDeps,
) -> PromiseProgressSelectionResult:
    quarters = list(deps.quarters)
    progress_records_by_q = deps.progress_records_by_q
    tracker_rows_map = deps.tracker_rows_map
    quarter_note_rows_map = deps.quarter_note_rows_map
    ui_state = deps.ui_state
    evaluation_as_of = deps.evaluation_as_of
    is_pbi_profile = deps.is_pbi_profile
    is_gpre_profile = deps.is_gpre_profile
    cols = deps.progress_columns
    mr_col = cols.get("mr_col")
    ptype_col = cols.get("ptype_col")
    tg_col = cols.get("tg_col")
    ac_col = cols.get("ac_col")
    ra_col = cols.get("ra_col")
    pk_col = cols.get("pk_col")
    tb_col = cols.get("tb_col")
    scr_col = cols.get("scr_col")
    num_upd_col = cols.get("num_upd_col")
    gtype_col = cols.get("gtype_col")
    tpn_col = cols.get("tpn_col")
    tpl_col = cols.get("tpl_col")
    fs_col = cols.get("fs_col")
    ls_col = cols.get("ls_col")
    fs_ev_col = cols.get("fs_ev_col")
    ls_ev_col = cols.get("ls_ev_col")
    ls_num_col = cols.get("ls_num_col")
    ls_txt_col = cols.get("ls_txt_col")
    carried_col = cols.get("carried_col")
    qsev_col = cols.get("qsev_col")
    qmsg_col = cols.get("qmsg_col")
    milestone_action_re = deps.milestone_action_re
    milestone_deadline_re = deps.milestone_deadline_re
    milestone_exclude_re = deps.milestone_exclude_re
    milestone_completion_re = deps.milestone_completion_re
    milestone_progress_re = deps.milestone_progress_re
    buyback_remaining_re = deps.buyback_remaining_re
    buyback_intent_re = deps.buyback_intent_re
    _pbi_promise_theme_re = deps.pbi_promise_theme_re
    _qend = deps.qend
    _parse_dollar_amount = deps.parse_dollar_amount
    _parse_target_year = deps.parse_target_year
    _buyback_actual_ytd = deps.buyback_actual_ytd
    _text_fragment_penalty = deps.text_fragment_penalty
    _clean_target_bonus = deps.clean_target_bonus
    _derive_split_target_meta = deps.derive_split_target_meta
    _pbi_repair_guidance_period_meta = deps.pbi_repair_guidance_period_meta
    _guidance_period_end = deps.guidance_period_end
    _actual_for_guidance = deps.actual_for_guidance
    _infer_target_numeric_spec = deps.infer_target_numeric_spec
    _split_target_metric_display = deps.split_target_metric_display
    _source_rank = deps.source_rank
    _split_target_identity_key = deps.split_target_identity_key
    _is_preferred_narrative_source = deps.is_preferred_narrative_source
    _classify_pbi_metric_label = deps.classify_pbi_metric_label
    _extract_pbi_target_display = deps.extract_pbi_target_display
    _pbi_target_display_ok = deps.pbi_target_display_ok
    _looks_pbi_fragment_text = deps.looks_pbi_fragment_text
    _is_pbi_clean_sentence = deps.is_pbi_clean_sentence
    _slide_signal_noise = deps.slide_signal_noise
    _is_45z_crush_margin_support_only = deps.is_45z_crush_margin_support_only
    _gpre_clean_visible_promise_metric = deps.gpre_clean_visible_promise_metric
    _gpre_bad_visible_promise_reason = deps.gpre_bad_visible_promise_reason
    _extract_45z_monetization_target_display = deps.extract_45z_monetization_target_display
    _extract_money_targets_for_display = deps.extract_money_targets_for_display
    _fmt_short_money_value_local = deps.fmt_short_money_value_local
    _extract_progress_latest_basis = deps.extract_progress_latest_basis
    _progress_metric_from_event = deps.progress_metric_from_event
    _progress_metric_from_qnote = deps.progress_metric_from_qnote
    _progress_target_display_from_qnote = deps.progress_target_display_from_qnote
    _progress_status_from_tracker = deps.progress_status_from_tracker
    _finalize_progress_item = deps.finalize_progress_item
    _candidate_quality_key = deps.candidate_quality_key
    _quarter_notes_view = deps.quarter_notes_view
    _load_profile_slide_signals = deps.load_profile_slide_signals
    _build_guidance_accuracy_rows = deps.build_guidance_accuracy_rows
    _gpre_trim_final_progress_rows = deps.gpre_trim_final_progress_rows
    qa_rows: List[Dict[str, Any]] = []
    ui_info_rows: List[Dict[str, Any]] = []
    rows_by_quarter: Dict[date, List[Dict[str, Any]]] = {}
    milestone_suppressed_count = 0
    guidance_snapshot_store = ui_state.get("guidance_snapshot_by_q", {}) if isinstance(ui_state, dict) else {}
    progress_select_started = time.perf_counter()
    for qd in quarters:
        sub_records = list(progress_records_by_q.get(qd) or [])
        tracker_seed_rows = tracker_rows_map.get(qd, []) if isinstance(tracker_rows_map, dict) else []
        guidance_seed_rows = guidance_snapshot_store.get(str(qd), []) if isinstance(guidance_snapshot_store, dict) else []
        if not sub_records and not tracker_seed_rows and not guidance_seed_rows:
            rows_by_quarter[qd] = []
            continue
        rows: List[Dict[str, Any]] = []
        for r in sub_records:
            pid = str(r.get("_pid") or "").strip()
            status = str(r.get("_status") or "")
            if not pid or not status:
                continue
            metric_ref_raw = str(r.get(mr_col) or "").strip() if mr_col else ""
            promise_type_val = str(r.get(ptype_col) or "operational") if ptype_col else "operational"
            src_ev = dict(r.get("_src_ev") or {})
            src_snip = glx_normalize_text(str(r.get("_src_snip") or src_ev.get("snippet") or ""))
            src_doc = str(r.get("_src_doc") or src_ev.get("doc_path") or src_ev.get("doc") or "")
            tgt_val = r.get(tg_col) if tg_col else ""
            latest_val = r.get(ac_col) if ac_col else ""
            rationale_val = str(r.get(ra_col) or "") if ra_col else ""

            # Milestone must be a real action + timing signal, not legal boilerplate.
            if promise_type_val.lower() == "milestone" or "milestone" in metric_ref_raw.lower():
                txt_chk = f"{src_snip} {rationale_val}".strip()
                if (not milestone_action_re.search(txt_chk)) or (not milestone_deadline_re.search(txt_chk)) or milestone_exclude_re.search(txt_chk):
                    qa_rows.append(
                        {
                            "quarter": qd,
                            "metric": "Promise_Progress_UI",
                            "check": "milestone_boilerplate_or_legal",
                            "severity": "info",
                            "message": f"dropped milestone pid={pid}",
                            "source": src_doc,
                        }
                    )
                    continue
                status_l = str(status or "").lower()
                if milestone_completion_re.search(txt_chk) or status_l in {"achieved", "resolved_pass", "done", "completed", "hit"}:
                    status = "completed"
                elif milestone_progress_re.search(txt_chk) or status_l in {"pending", "open", "on_track"}:
                    status = "in progress"
                else:
                    status = "not observed"
                metric_ref_raw = "Strategic milestone"

            # Capital allocation: only keep deterministic buyback intent with $ target + FY,
            # or explicit remaining-authorization info rows.
            if "capital_allocation" in metric_ref_raw.lower():
                text_ca = f"{src_snip} {rationale_val}".strip()
                if buyback_remaining_re.search(text_ca):
                    promise_type_val = "capital_alloc_info"
                    metric_ref_raw = "buyback_authorization_remaining"
                    status = "info"
                    parsed_amt = _parse_dollar_amount(text_ca)
                    tgt_val = parsed_amt if parsed_amt is not None else ""
                    latest_val = ""
                    rationale_val = qn_compact_snippet("Authorization remaining disclosed in source filing.", 180)
                elif buyback_intent_re.search(text_ca):
                    amt = _parse_dollar_amount(text_ca)
                    tgt_year = _parse_target_year(text_ca, qd)
                    if amt is None or tgt_year is None:
                        qa_rows.append(
                            {
                                "quarter": qd,
                                "metric": "Promise_Progress_UI",
                                "check": "capital_allocation_non_deterministic",
                                "severity": "info",
                                "message": f"dropped capital_allocation pid={pid} (missing $ target/year)",
                                "source": src_doc,
                            }
                        )
                        continue
                    promise_type_val = "buyback_intent_fy"
                    metric_ref_raw = f"buyback_intent_fy (FY {int(tgt_year)})"
                    tgt_val = float(amt)
                    actual_buy = _buyback_actual_ytd(qd, int(tgt_year))
                    latest_val = actual_buy if actual_buy is not None else ""
                    fy_end = date(int(tgt_year), 12, 31)
                    if qd < fy_end:
                        status = "pending"
                        rationale_val = "FY not complete; tracking YTD buybacks against stated intent."
                    elif actual_buy is None:
                        status = "no_actual_available"
                        rationale_val = "No buybacks_cash series available for FY evaluation."
                    else:
                        status = "achieved" if float(actual_buy) >= float(amt) else "broken"
                        rationale_val = (
                            f"Buybacks actual ${float(actual_buy)/1e6:,.1f}m vs target ${float(amt)/1e6:,.1f}m."
                        )
                else:
                    qa_rows.append(
                        {
                            "quarter": qd,
                            "metric": "Promise_Progress_UI",
                            "check": "capital_allocation_non_target_text",
                            "severity": "info",
                            "message": f"dropped capital_allocation pid={pid} (no deterministic buyback target)",
                            "source": src_doc,
                        }
                    )
                    continue
            status_lc = str(status or "").strip().lower()
            if promise_type_val.lower() != "milestone" and status_lc in {"pending", "open"} and str(metric_ref_raw or "").strip():
                status = "on_track"
            row_rec = {
                "promise_id": pid,
                "metric_ref": metric_ref_raw,
                "target": tgt_val,
                "latest": latest_val,
                "promise_key": str(r.get(pk_col) or "") if pk_col else "",
                "target_bucket": str(r.get(tb_col) or "") if tb_col else "",
                "promise_type": promise_type_val,
                "scorable": bool(r.get(scr_col)) if scr_col else False,
                "numeric_update_this_quarter": bool(r.get(num_upd_col)) if num_upd_col else False,
                "status": status,
                "rationale": rationale_val,
                "guidance_type": str(r.get(gtype_col) or "") if gtype_col else "",
                "target_period_norm": str(r.get(tpn_col) or "") if tpn_col else "",
                "target_period_label": str(r.get(tpl_col) or "") if tpl_col else "",
                "first_seen_quarter_end": str(_qend(r.get(fs_col))) if fs_col and _qend(r.get(fs_col)) is not None else "",
                "last_seen_quarter_end": str(_qend(r.get(ls_col))) if ls_col and _qend(r.get(ls_col)) is not None else "",
                "first_seen_evidence_quarter_end": str(_qend(r.get(fs_ev_col))) if fs_ev_col and _qend(r.get(fs_ev_col)) is not None else "",
                "last_seen_evidence_quarter_end": str(_qend(r.get(ls_ev_col))) if ls_ev_col and _qend(r.get(ls_ev_col)) is not None else "",
                "last_seen_numeric_quarter_end": str(_qend(r.get(ls_num_col))) if ls_num_col and _qend(r.get(ls_num_col)) is not None else "",
                "last_seen_text_quarter_end": str(_qend(r.get(ls_txt_col))) if ls_txt_col and _qend(r.get(ls_txt_col)) is not None else "",
                "carried_to_quarter_end": str(_qend(r.get(carried_col))) if carried_col and _qend(r.get(carried_col)) is not None else str(qd),
                "evaluated_through": "",
                "qa_severity": str(r.get(qsev_col) or "") if qsev_col else "",
                "qa_message": str(r.get(qmsg_col) or "") if qmsg_col else "",
                "_source_snip": src_snip,
                "_source_doc": src_doc,
                "_status_pri": int(r.get("_status_pri") or 9),
                "_score": float(r.get("_score") or 0.0),
                "_fragment_penalty": _text_fragment_penalty(rationale_val),
                "_clean_target_bonus": _clean_target_bonus(rationale_val),
            }
            row_blob = " | ".join([str(tgt_val or ""), str(latest_val or ""), rationale_val, src_snip])
            row_rec.update(
                _derive_split_target_meta(
                    metric_ref_raw,
                    row_blob,
                    row_rec.get("guidance_type") or row_rec.get("target_bucket") or "",
                    qd,
                    src_ev.get("doc_type") or src_ev.get("source_type") or "",
                    src_doc,
                    src_ev.get("section") or "",
                )
            )
            if tpn_col:
                explicit_period_norm = str(r.get(tpn_col) or "").strip()
                if explicit_period_norm:
                    row_rec["target_period_norm"] = explicit_period_norm
            if tpl_col:
                explicit_period_label = str(r.get(tpl_col) or "").strip()
                if explicit_period_label:
                    row_rec["target_period_label"] = explicit_period_label
            if is_pbi_profile:
                repaired_norm, repaired_label = _pbi_repair_guidance_period_meta(
                    metric_ref_raw,
                    row_rec.get("target_period_norm"),
                    row_rec.get("target_period_label"),
                    " | ".join(
                        [
                            str(row_rec.get("target") or ""),
                            str(row_rec.get("latest") or ""),
                            rationale_val,
                            src_snip,
                        ]
                    ),
                    qd,
                )
                if repaired_norm:
                    row_rec["target_period_norm"] = repaired_norm
                if repaired_label:
                    row_rec["target_period_label"] = repaired_label
                    if re.search(r"^Guidance period FY\s*20\d{2} has not ended", rationale_val, re.I):
                        rationale_val = re.sub(r"FY\s*20\d{2}", repaired_label, rationale_val, count=1, flags=re.I)
                        row_rec["rationale"] = rationale_val
            if str(row_rec.get("promise_type") or "").strip().lower() == "guidance_range":
                metric_blob = " | ".join([metric_ref_raw, str(row_rec.get("metric_display") or ""), rationale_val, src_snip])
                metric_name = ""
                if re.search(r"\brevenue\b", metric_blob, re.I):
                    metric_name = "Revenue"
                elif re.search(r"\badjusted?\s+ebit\b|\badj\.?\s*ebit\b", metric_blob, re.I):
                    metric_name = "Adj EBIT"
                elif re.search(r"\beps\b", metric_blob, re.I):
                    metric_name = "Adj EPS"
                elif re.search(r"\bfcf\b|free cash flow", metric_blob, re.I):
                    metric_name = "FCF"
                period_norm = str(row_rec.get("target_period_norm") or "").strip()
                as_of_q = evaluation_as_of or qd
                if metric_name and period_norm not in {"", "UNK"} and isinstance(as_of_q, date):
                    period_end = _guidance_period_end(period_norm, as_of_q)
                    actual_val = _actual_for_guidance(metric_name, period_norm, as_of_q)
                    if actual_val is not None and isinstance(period_end, date) and period_end <= as_of_q:
                        row_rec["latest"] = float(actual_val)
                        target_spec = _infer_target_numeric_spec(row_rec.get("target"))
                        kind = str(target_spec.get("kind") or "")
                        tol_mult = 0.01
                        if kind == "range":
                            lo = float(min(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
                            hi = float(max(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
                            if float(actual_val) < lo - abs(lo) * tol_mult:
                                row_rec["status"] = "resolved_fail"
                            elif float(actual_val) > hi + abs(hi) * tol_mult:
                                row_rec["status"] = "resolved_beat"
                            else:
                                row_rec["status"] = "resolved_pass"
                        elif kind in {"gte", "gt", "point", "lte", "lt"}:
                            tgt = float(target_spec.get("value") or 0.0)
                            tol = max(1e-9, abs(tgt) * tol_mult)
                            if kind == "gte":
                                row_rec["status"] = "resolved_pass" if float(actual_val) >= tgt - tol else "resolved_fail"
                            elif kind == "gt":
                                if float(actual_val) > tgt + tol:
                                    row_rec["status"] = "resolved_beat"
                                elif float(actual_val) >= tgt - tol:
                                    row_rec["status"] = "resolved_pass"
                                else:
                                    row_rec["status"] = "resolved_fail"
                            elif kind == "lte":
                                row_rec["status"] = "resolved_pass" if float(actual_val) <= tgt + tol else "resolved_fail"
                            elif kind == "lt":
                                row_rec["status"] = "resolved_pass" if float(actual_val) < tgt + tol else "resolved_fail"
                            else:
                                row_rec["status"] = "resolved_pass" if abs(float(actual_val) - tgt) <= tol else ("resolved_beat" if float(actual_val) > tgt + tol else "resolved_fail")
                    elif (
                        str(row_rec.get("status") or "").strip().lower() in {"achieved", "resolved_pass", "resolved_beat", "resolved_fail", "broken", "missed"}
                        and str(row_rec.get("latest") or "").strip().lower() in {"", "not yet measurable"}
                        and actual_val is not None
                    ):
                        row_rec["latest"] = float(actual_val)
            row_rec["promise_group"] = str(row_rec.get("target_group_key") or "")
            row_rec["metric_display"] = _split_target_metric_display(metric_ref_raw, row_blob, row_rec)
            rows.append(row_rec)

        # Keep operational promises separate from guidance-range calibration rows.
        oper_rows = [x for x in rows if str(x.get("promise_type") or "").strip().lower() != "guidance_range"]
        if not (isinstance(guidance_snapshot_store, dict) and guidance_snapshot_store.get(str(qd))):
            # When there is no structured guidance snapshot for the quarter,
            # the progress sheet itself must carry open measurable guidance rows.
            # Keep all guidance-range rows here and let the later quality gates
            # decide what survives visibly.
            oper_rows.extend(
                [
                    x
                    for x in rows
                    if str(x.get("promise_type") or "").strip().lower() == "guidance_range"
                ]
            )
        def _progress_identity_key_local(item: Dict[str, Any]) -> Tuple[str, str, str]:
            canonical_key = str(item.get("canonical_subject_key") or "").strip()
            lifecycle_key = str(item.get("promise_lifecycle_key") or item.get("lifecycle_key") or "").strip()
            lifecycle_subject_key = str(item.get("lifecycle_subject_key") or "").strip()
            metric_name_local = str(item.get("metric_display") or item.get("metric_ref") or "").strip()
            metric_low_local = metric_name_local.lower()
            period_norm_existing = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
            if is_pbi_profile:
                guidance_slug = ""
                if metric_low_local == "revenue guidance":
                    guidance_slug = "revenue_guidance"
                elif metric_low_local == "adjusted ebit guidance":
                    guidance_slug = "adjusted_ebit_guidance"
                elif metric_low_local == "eps guidance":
                    guidance_slug = "eps_guidance"
                elif metric_low_local == "fcf target":
                    guidance_slug = "fcf_target"
                elif metric_low_local == "cost savings target":
                    guidance_slug = "cost_savings"
                elif metric_low_local == "pb bank liquidity release":
                    guidance_slug = "pb_bank_liquidity"
                elif metric_low_local == "deleveraging target":
                    guidance_slug = "deleveraging"
                elif metric_low_local == "sendtech / presort operating target":
                    guidance_slug = "segment_target"
                elif str(item.get("promise_type") or "").strip().lower() == "guidance_range":
                    guidance_slug = re.sub(r"[^a-z0-9]+", "_", metric_low_local).strip("_")
                if guidance_slug:
                    period_norm_key = period_norm_existing or (
                        "ANNUALIZED_PROGRAM"
                        if guidance_slug in {"cost_savings", "pb_bank_liquidity", "deleveraging"}
                        else "UNK"
                    )
                    guidance_key = f"guidance:{guidance_slug}:{period_norm_key}"
                    parent_subject_key = shared_build_parent_subject_key(
                        entity_scope=item.get("scope_key") or "company_total",
                        metric_family=guidance_slug,
                        program_token="guidance",
                        topic_family="guidance",
                    )
                    canonical_key = guidance_key
                    lifecycle_key = guidance_key
                    lifecycle_subject_key = guidance_key
                    item["parent_subject_key"] = parent_subject_key
                    item["canonical_subject_key"] = canonical_key
                    item["promise_lifecycle_key"] = lifecycle_key
                    item["lifecycle_subject_key"] = lifecycle_subject_key
                    item["promise_id"] = guidance_key
                    item["target_period_norm"] = period_norm_key
                    item["routing_reason"] = str(item.get("routing_reason") or "promise_progress")
                    return canonical_key, lifecycle_key, lifecycle_subject_key
            if not canonical_key:
                period_norm = shared_infer_target_period_norm(
                    period_norm=item.get("target_period_norm") or item.get("target_period_key") or item.get("guidance_type") or item.get("target_bucket") or "",
                    deadline=item.get("deadline") or item.get("target_time"),
                    quarter=qd,
                    text=" | ".join(
                        [
                            str(item.get("target") or ""),
                            str(item.get("latest") or ""),
                            str(item.get("rationale") or ""),
                            str(item.get("metric_display") or item.get("metric_ref") or ""),
                        ]
                    ),
                )
                follow_event = shared_build_follow_through_event(
                    " | ".join(
                        [
                            str(item.get("latest") or ""),
                            str(item.get("rationale") or ""),
                            str(item.get("_source_snip") or ""),
                        ]
                    ),
                    quarter=qd,
                    source_type=str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui"),
                    metric_hint=str(item.get("metric_display") or item.get("metric_ref") or ""),
                    source_doc=str(item.get("_source_doc") or item.get("doc") or ""),
                    period_norm=period_norm,
                    promise_type_hint=str(item.get("promise_type") or ""),
                    base_score=float(item.get("_score") or 0.0),
                    display_text_hint=str(item.get("latest") or ""),
                )
                if follow_event is not None:
                    canonical_key = follow_event.canonical_subject_key
                    lifecycle_key = follow_event.lifecycle_key
                    lifecycle_subject_key = follow_event.lifecycle_subject_key or follow_event.lifecycle_key
                    item["canonical_subject_key"] = canonical_key
                    item["promise_lifecycle_key"] = lifecycle_key
                    item["lifecycle_subject_key"] = lifecycle_subject_key
                    item["parent_subject_key"] = follow_event.parent_subject_key
                    item["routing_reason"] = follow_event.routing_reason
                    item["source_class"] = follow_event.source_class
                    item["statement_class"] = follow_event.statement_class
                    item["evidence_role"] = follow_event.evidence_role
                    item["metric_family"] = follow_event.metric_family
                    item["entity_scope"] = follow_event.entity_scope
                    item["target_period_norm"] = item.get("target_period_norm") or follow_event.target_period_norm or period_norm
                else:
                    parent_subject_key = shared_build_parent_subject_key(
                        entity_scope=item.get("scope_key") or "company_total",
                        metric_family=item.get("metric_display") or item.get("metric_ref") or "general",
                        program_token=item.get("scope_key") or "",
                        topic_family=item.get("target_bucket") or item.get("guidance_type") or "",
                    )
                    canonical_key = shared_build_canonical_subject_key(
                        entity_scope=item.get("scope_key") or "company_total",
                        metric_family=item.get("metric_display") or item.get("metric_ref") or "general",
                        target_period_norm=period_norm,
                        scope_token=item.get("target_group_key") or item.get("theme_key") or "",
                    )
                    lifecycle_key = shared_build_promise_lifecycle_key(
                        canonical_key,
                        stage_token=item.get("promise_type") or "",
                    )
                    lifecycle_subject_key = shared_build_lifecycle_subject_key(
                        parent_subject_key=parent_subject_key,
                        canonical_subject_key=canonical_key,
                        stage_token=item.get("promise_type") or "",
                        target_period_norm=period_norm,
                    )
                    item["parent_subject_key"] = parent_subject_key
                    item["canonical_subject_key"] = canonical_key
                    item["promise_lifecycle_key"] = lifecycle_key
                    item["lifecycle_subject_key"] = lifecycle_subject_key
                    item["source_class"] = shared_source_class(item.get("_source_type") or item.get("source_type") or "")
                    item["statement_class"] = shared_statement_class(
                        " | ".join([str(item.get("rationale") or ""), str(item.get("latest") or ""), str(item.get("_source_snip") or "")]),
                        source_type=item.get("_source_type") or item.get("source_type") or "",
                        metric_hint=str(item.get("metric_display") or item.get("metric_ref") or ""),
                    )
                    item["evidence_role"] = shared_evidence_role(
                        "follow_through_event",
                        route_reason=item.get("route_reason") or item.get("routing_reason") or "promise_progress",
                        promise_type=item.get("promise_type") or "",
                        current_status=item.get("status") or "",
                    )
            item["candidate_type"] = str(item.get("candidate_type") or "follow_through_event")
            item["route_reason"] = str(item.get("route_reason") or item.get("routing_reason") or "promise_progress")
            stated_q = str(
                item.get("stated_quarter")
                or item.get("first_seen_evidence_quarter_end")
                or item.get("first_seen_quarter_end")
                or qd
            )
            latest_q = str(
                item.get("latest_evidence_quarter")
                or item.get("last_seen_evidence_quarter_end")
                or item.get("last_seen_quarter_end")
                or qd
            )
            carried_q = str(item.get("carried_to_quarter") or item.get("carried_to_quarter_end") or qd)
            eval_q_txt = str(item.get("evaluated_through_quarter") or item.get("evaluated_through") or carried_q or qd)
            item["stated_quarter"] = stated_q
            item["latest_evidence_quarter"] = latest_q
            item["carried_to_quarter"] = carried_q
            item["evaluated_through_quarter"] = eval_q_txt
            item["lifecycle_state"] = str(
                item.get("lifecycle_state")
                or shared_derive_lifecycle_state(
                    target_period_norm=item.get("target_period_norm") or item.get("target_period_key") or item.get("guidance_type") or item.get("target_bucket") or "",
                    stated_quarter=stated_q,
                    latest_evidence_quarter=latest_q,
                    evaluated_through_quarter=eval_q_txt,
                    carried_to_quarter=carried_q,
                    current_status=item.get("status") or "",
                )
            )
            item["status_resolution_reason"] = str(
                item.get("status_resolution_reason")
                or shared_derive_status_resolution_reason(
                    current_status=item.get("status") or "",
                    latest_value=item.get("latest") or "",
                    lifecycle_state=item.get("lifecycle_state") or "",
                )
            )
            if item.get("_source_rank"):
                item["_source_rank"] = int(item.get("_source_rank"))
            else:
                src_doc = str(item.get("_source_doc") or "")
                low = src_doc.lower()
                if "transcript" in low:
                    src_type = "transcript"
                elif "presentation" in low or "slides" in low:
                    src_type = "earnings_presentation"
                elif any(tok in low for tok in ["press", "release", "ex99", "exhibit991"]):
                    src_type = "earnings_release"
                elif low.endswith(".htm") or low.endswith(".html"):
                    src_type = "filing"
                else:
                    src_type = ""
                try:
                    item["_source_rank"] = int(_source_rank(src_type, src_doc))
                except Exception:
                    item["_source_rank"] = 99
            return _split_target_identity_key(
                {
                    **item,
                    "target_group_key": lifecycle_subject_key or lifecycle_key or canonical_key or item.get("target_group_key") or "",
                },
                item.get("metric_display") or item.get("metric_ref"),
                item.get("target_period_norm") or item.get("target_period_key") or item.get("guidance_type") or item.get("target_bucket") or "",
                qd,
            )

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
            if metric_low.endswith("guidance") or re.search(r"\b(guidance|outlook|target)\b", metric_low, re.I):
                return "Guidance / outlook"
            if re.search(r"\b(fcf|free cash flow|working capital|cash flow)\b", low, re.I):
                return "Cash flow / FCF / working capital"
            if re.search(r"\b(liquidity|balance sheet|debt|revolver|availability|interest expense|refinanc|deleverag|net debt|mezzanine)\b", low, re.I):
                return "Debt / liquidity / balance sheet"
            if re.search(r"\b(repurchase|buyback|dividend|shareholder returns?|capital allocation)\b", low, re.I):
                return "Capital allocation / shareholder returns"
            if re.search(r"\b(carbo[n]? capture|fully operational|online and ramping|startup|commissioning|milestone|qualification|commercialization|agreement executed|utilization)\b", low, re.I):
                return "Operations / commercialization / milestones"
            if re.search(r"\b(cost savings|strategic review|management framing|risk management|non-core asset monetization|positive ebitda)\b", low, re.I):
                return "Programs / initiatives / management framing"
            if re.search(r"\b(improved|declined|increased|decreased|up |down |yoy|qoq|better|worse|from .* prior|from .* yoy)\b", low, re.I):
                return "Results / drivers / better vs prior"
            if re.search(r"\b(results?|drivers?|contributed|realized|executed|completed|progressing)\b", low, re.I):
                return "Results / drivers"
            if re.search(r"\b(expected|expect|on track|will|continue to|continues to|ahead of plan)\b", low, re.I):
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

        def _collapse_progress_rows_for_display(items_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            def _merge_progress_display_row(best_row: Dict[str, Any], other_row: Dict[str, Any]) -> Dict[str, Any]:
                best_latest = str(best_row.get("latest") or "").strip()
                other_latest = str(other_row.get("latest") or "").strip()
                best_has_actual = bool(best_latest) and best_latest.lower() != "not yet measurable"
                other_has_actual = bool(other_latest) and other_latest.lower() != "not yet measurable"
                if other_has_actual and not best_has_actual:
                    best_row["latest"] = other_row.get("latest")
                if not str(best_row.get("target") or "").strip() and str(other_row.get("target") or "").strip():
                    best_row["target"] = other_row.get("target")
                if len(str(other_row.get("rationale") or "")) > len(str(best_row.get("rationale") or "")):
                    best_row["rationale"] = other_row.get("rationale")
                if len(str(other_row.get("_source_snip") or "")) > len(str(best_row.get("_source_snip") or "")):
                    best_row["_source_snip"] = other_row.get("_source_snip")
                if float(other_row.get("_score") or other_row.get("confidence_score") or 0.0) > float(best_row.get("_score") or best_row.get("confidence_score") or 0.0):
                    best_row["_score"] = other_row.get("_score") or other_row.get("confidence_score")
                if str(other_row.get("evidence_role") or "").strip().lower() in {"later_evidence", "result_evidence"} and str(best_row.get("evidence_role") or "").strip().lower() not in {"later_evidence", "result_evidence"}:
                    best_row["evidence_role"] = other_row.get("evidence_role")
                best_row["collapse_reason"] = str(best_row.get("collapse_reason") or "same_subject_same_block")
                best_row["conflict_resolution_reason"] = str(best_row.get("conflict_resolution_reason") or "status_precedence")
                return best_row

            sections = [x for x in items_in if str(x.get("row_type") or "").strip().lower() in {"section", "blank"}]
            actual_rows = [x for x in items_in if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}]
            grouped_rows: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
            for item_local in actual_rows:
                grouped_rows.setdefault(_progress_identity_key_local(item_local), []).append(item_local)
            collapsed: List[Dict[str, Any]] = []
            for grouped_items in grouped_rows.values():
                best_item = shared_pick_best_subject_row_for_quarter(grouped_items)
                if best_item is not None:
                    collapsed.append(best_item)
            collapsed = sorted(
                collapsed,
                key=lambda z: (
                    -shared_progress_status_rank(z.get("status")),
                    -int(z.get("_source_rank") or 0),
                    -float(z.get("_score") or z.get("confidence_score") or 0.0),
                    str(z.get("metric_ref") or "").lower(),
                ),
            )
            if is_gpre_profile:
                parent_groups: Dict[str, List[Dict[str, Any]]] = {}
                for item_local in collapsed:
                    parent_key = str(item_local.get("parent_subject_key") or "").strip()
                    if parent_key:
                        parent_groups.setdefault(parent_key, []).append(item_local)
                filtered: List[Dict[str, Any]] = []
                for item_local in collapsed:
                    parent_key = str(item_local.get("parent_subject_key") or "").strip()
                    lifecycle_key = str(item_local.get("lifecycle_subject_key") or "").strip().lower()
                    blob = glx_normalize_text(
                        " | ".join(
                            [
                                str(item_local.get("target") or ""),
                                str(item_local.get("latest") or ""),
                                str(item_local.get("rationale") or ""),
                                str(item_local.get("_source_snip") or ""),
                            ]
                        )
                    )
                    monetization_child = "monetization" in lifecycle_key
                    has_explicit_monetization = bool(
                        re.search(
                            r"\b(monetization|agreement executed|value realized|realized|ebitda|income tax benefit|production tax credit|net of discounts)\b",
                            blob,
                            re.I,
                        )
                    )
                    sibling_operational = any(
                        sib is not item_local
                        and str(sib.get("parent_subject_key") or "").strip() == parent_key
                        and re.search(r"\b(operationalization|startup_or_commissioning|online_or_operational)\b", str(sib.get("lifecycle_subject_key") or ""), re.I)
                        for sib in parent_groups.get(parent_key, [])
                    )
                    if monetization_child and sibling_operational and not has_explicit_monetization:
                        item_local["collapse_reason"] = str(item_local.get("collapse_reason") or "parent_subject_child_preserved")
                        item_local["conflict_resolution_reason"] = str(item_local.get("conflict_resolution_reason") or "status_precedence")
                        continue
                    filtered.append(item_local)
                monetization_by_parent: Dict[str, List[Dict[str, Any]]] = {}
                for item_local in filtered:
                    parent_key = str(item_local.get("parent_subject_key") or "").strip()
                    metric_blob = " | ".join(
                        [
                            str(item_local.get("metric_display") or item_local.get("metric_ref") or ""),
                            str(item_local.get("target") or ""),
                            str(item_local.get("latest") or ""),
                            str(item_local.get("rationale") or ""),
                        ]
                    )
                    if parent_key and re.search(r"\b45z\b", metric_blob, re.I) and re.search(r"\b(monetization|ebitda opportunity|ebitda)\b", metric_blob, re.I) and not re.search(r"\bqualification\b", metric_blob, re.I):
                        monetization_by_parent.setdefault(parent_key, []).append(item_local)
                if monetization_by_parent:
                    keep_ids: set[int] = set()
                    for parent_key, same_parent_rows in monetization_by_parent.items():
                        if len(same_parent_rows) <= 1:
                            keep_ids.add(id(same_parent_rows[0]))
                            continue
                        best_row = shared_pick_best_subject_row_for_quarter(same_parent_rows)
                        if best_row is not None:
                            keep_ids.add(id(best_row))
                    filtered_second: List[Dict[str, Any]] = []
                    for item_local in filtered:
                        parent_key = str(item_local.get("parent_subject_key") or "").strip()
                        metric_blob = " | ".join(
                            [
                                str(item_local.get("metric_display") or item_local.get("metric_ref") or ""),
                                str(item_local.get("target") or ""),
                                str(item_local.get("latest") or ""),
                                str(item_local.get("rationale") or ""),
                            ]
                        )
                        if parent_key in monetization_by_parent and re.search(r"\b45z\b", metric_blob, re.I) and re.search(r"\b(monetization|ebitda opportunity|ebitda)\b", metric_blob, re.I) and not re.search(r"\bqualification\b", metric_blob, re.I):
                            if id(item_local) not in keep_ids:
                                item_local["collapse_reason"] = str(item_local.get("collapse_reason") or "same_subject_same_block")
                                item_local["conflict_resolution_reason"] = str(item_local.get("conflict_resolution_reason") or "status_precedence")
                                continue
                        filtered_second.append(item_local)
                    filtered = filtered_second
                collapsed = filtered
            lifecycle_groups: Dict[str, List[Dict[str, Any]]] = {}
            passthrough_rows: List[Dict[str, Any]] = []
            for item_local in collapsed:
                lifecycle_key = str(item_local.get("lifecycle_subject_key") or item_local.get("promise_lifecycle_key") or "").strip()
                if lifecycle_key:
                    lifecycle_groups.setdefault(lifecycle_key, []).append(item_local)
                else:
                    passthrough_rows.append(item_local)
            collapsed_final: List[Dict[str, Any]] = list(passthrough_rows)
            for grouped_items in lifecycle_groups.values():
                best_item = shared_pick_best_subject_row_for_quarter(grouped_items)
                if best_item is None:
                    continue
                for other_item in grouped_items:
                    if other_item is best_item:
                        continue
                    other_item["collapse_reason"] = str(other_item.get("collapse_reason") or "same_subject_same_block")
                    other_item["conflict_resolution_reason"] = str(other_item.get("conflict_resolution_reason") or "status_precedence")
                    best_item = _merge_progress_display_row(best_item, other_item)
                collapsed_final.append(best_item)
            if is_gpre_profile:
                parent_metric_groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
                for item_local in collapsed_final:
                    parent_key = str(item_local.get("parent_subject_key") or "").strip()
                    lifecycle_key = str(item_local.get("lifecycle_subject_key") or "").strip().lower()
                    metric_blob = " | ".join(
                        [
                            str(item_local.get("metric_display") or item_local.get("metric_ref") or ""),
                            str(item_local.get("target") or ""),
                            str(item_local.get("latest") or ""),
                        ]
                    )
                    if not parent_key:
                        continue
                    if not re.search(r"\b45z\b", metric_blob, re.I):
                        continue
                    if not re.search(r"\b(monetization|ebitda)\b", metric_blob, re.I):
                        continue
                    stage_bucket = "monetization" if "monetization" in lifecycle_key else lifecycle_key
                    group_key = (
                        parent_key,
                        stage_bucket,
                        str(item_local.get("target_period_norm") or item_local.get("period_norm") or "").strip(),
                    )
                    parent_metric_groups.setdefault(group_key, []).append(item_local)
                if parent_metric_groups:
                    keep_ids: set[int] = set()
                    grouped_member_ids: set[int] = set()
                    for grouped_items in parent_metric_groups.values():
                        grouped_member_ids.update(id(x) for x in grouped_items)
                        best_item = shared_pick_best_subject_row_for_quarter(grouped_items)
                        if best_item is None:
                            continue
                        for other_item in grouped_items:
                            if other_item is best_item:
                                continue
                            other_item["collapse_reason"] = str(other_item.get("collapse_reason") or "same_subject_same_block")
                            other_item["conflict_resolution_reason"] = str(other_item.get("conflict_resolution_reason") or "status_precedence")
                            best_item = _merge_progress_display_row(best_item, other_item)
                        keep_ids.add(id(best_item))
                    collapsed_final = [
                        item_local
                        for item_local in collapsed_final
                        if not (
                            id(item_local) in grouped_member_ids
                            and id(item_local) not in keep_ids
                        )
                    ]
            promise_groups: Dict[str, List[Dict[str, Any]]] = {}
            for item_local in collapsed_final:
                promise_id = str(item_local.get("promise_id") or "").strip()
                if promise_id:
                    promise_groups.setdefault(promise_id, []).append(item_local)
            if promise_groups:
                rebuilt_rows: List[Dict[str, Any]] = []
                seen_ids: set[int] = set()
                for promise_id, grouped_items in promise_groups.items():
                    if len(grouped_items) == 1:
                        continue
                    best_item = shared_pick_best_subject_row_for_quarter(grouped_items)
                    if best_item is None:
                        continue
                    for other_item in grouped_items:
                        if other_item is best_item:
                            continue
                        other_item["collapse_reason"] = str(other_item.get("collapse_reason") or "same_subject_same_block")
                        other_item["conflict_resolution_reason"] = str(other_item.get("conflict_resolution_reason") or "status_precedence")
                        best_item = _merge_progress_display_row(best_item, other_item)
                    rebuilt_rows.append(best_item)
                    seen_ids.update(id(x) for x in grouped_items)
                collapsed_final = [item_local for item_local in collapsed_final if id(item_local) not in seen_ids] + rebuilt_rows
            collapsed = sorted(
                collapsed_final,
                key=lambda z: (
                    _promise_progress_visible_category_rank_local(z),
                    -shared_progress_status_rank(z.get("status")),
                    -int(z.get("_source_rank") or 0),
                    -float(z.get("_score") or z.get("confidence_score") or 0.0),
                    str(z.get("metric_display") or z.get("metric_ref") or "").lower(),
                ),
            )
            return sections + collapsed

        # Show one most-relevant promise per scoped metric in each quarter block.
        by_metric: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        for item in oper_rows:
            mkey = _progress_identity_key_local(item)
            by_metric.setdefault(mkey, []).append(item)
        selected_rows: List[Dict[str, Any]] = []
        for mkey, items in by_metric.items():
            best_item = shared_pick_best_subject_row_for_quarter(items)
            if best_item is not None:
                selected_rows.append(best_item)
        selected_rows = _collapse_progress_rows_for_display(selected_rows)
        def _promise_progress_keep_item(item: Dict[str, Any]) -> bool:
            metric_name = str(item.get("metric_display") or item.get("metric_ref") or "").strip()
            metric_low = metric_name.lower()
            promise_type_val = str(item.get("promise_type") or item.get("candidate_scope") or "").strip()
            rationale = glx_normalize_text(str(item.get("rationale") or ""))
            if is_gpre_profile:
                normalized_metric = _gpre_clean_visible_promise_metric(
                    metric_name,
                    " | ".join([rationale, str(item.get("target") or ""), str(item.get("latest") or "")]),
                    item,
                )
                if normalized_metric:
                    metric_name = normalized_metric
                    metric_low = metric_name.lower()
                    item["metric_display"] = metric_name
                bad_reason = _gpre_bad_visible_promise_reason(
                    metric_name,
                    rationale,
                    item.get("latest"),
                    item.get("target"),
                )
                if bad_reason:
                    item["quality_drop_reason"] = bad_reason
                    return False
            source_class = str(item.get("source_class") or shared_source_class(item.get("_source_type") or item.get("source_type") or "")).strip().lower()
            statement_class = str(
                item.get("statement_class")
                or shared_statement_class(
                    " | ".join([rationale, str(item.get("latest") or ""), str(item.get("_source_snip") or "")]),
                    source_type=item.get("_source_type") or item.get("source_type") or "",
                    metric_hint=metric_name,
                )
            ).strip().lower()
            if re.search(
                r"^\s*(which time|portion of|at which time|for which|that will|who will|where the|the partnership|the merger|the transactions?)\b",
                metric_name,
                re.I,
            ):
                return False
            if source_class in {"weak_support", "support"} and statement_class not in {"structured_numeric_bridge"}:
                return False
            if statement_class in {"boilerplate", "scaffolding", "fragmentary_text", "weak_forward_looking"}:
                return False
            is_45z_metric = bool(re.search(r"\b45z\b|tax credit", metric_low, re.I))
            local_timing_hint = bool(re.search(r"\b(fy\s*20\d{2}|20\d{2}|q[1-4]|quarter|full[- ]?year|annualized)\b", rationale, re.I))
            local_numeric_hint = bool(
                pd.notna(pd.to_numeric(item.get("target"), errors="coerce"))
                or re.search(r"\$?\s*\d+(?:\.\d+)?\s*(?:m|mm|million|b|bn|%|x)?", rationale, re.I)
                or re.search(r"\$?\s*\d+(?:\.\d+)?\s*(?:m|mm|million|b|bn|%|x)?", str(item.get("target") or ""), re.I)
            )
            local_action_hint = bool(
                re.search(
                    r"\b(target|guidance|expected|expect|opportunity|on track|fully operational|online|ramping|completed|executed|sale completed|repaid|repayment completed|savings|progress|improved|released)\b",
                    rationale,
                    re.I,
                )
            )
            if _slide_signal_noise(rationale):
                return False
            summary_text = glx_normalize_text(str(item.get("statement_summary") or ""))
            qualified_promise = shared_qualify_promise_candidate(
                rationale,
                source_type=str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui"),
                metric_hint=" | ".join([
                    metric_name,
                    str(item.get("target") or ""),
                    str(item.get("latest") or ""),
                ]),
            )
            if qualified_promise is None and summary_text and summary_text != rationale:
                qualified_promise = shared_qualify_promise_candidate(
                    summary_text,
                    source_type=str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui"),
                    metric_hint=" | ".join([
                        metric_name,
                        str(item.get("target") or ""),
                        str(item.get("latest") or ""),
                    ]),
                )
            if qualified_promise is None:
                source_bucket = str(item.get("target_bucket") or "").strip().lower()
                source_type = str(item.get("_source_type") or item.get("source_type") or "").strip().lower()
                if (
                    is_pbi_profile
                    and (
                        metric_name in {
                            "Revenue guidance",
                            "Adjusted EBIT guidance",
                            "EPS guidance",
                            "FCF target",
                            "Cost savings target",
                            "PB Bank liquidity release",
                            "Deleveraging target",
                            "Strategic milestone",
                        }
                        or metric_low.endswith("guidance")
                    )
                    and not _looks_pbi_fragment_text(" | ".join([rationale, str(item.get("target") or ""), str(item.get("latest") or "")]))
                    and (_is_preferred_narrative_source(source_type) or not source_type)
                    and (
                        local_timing_hint
                        or local_numeric_hint
                        or local_action_hint
                        or metric_name in {"PB Bank liquidity release", "Cost savings target", "Deleveraging target"}
                    )
                ):
                    item.setdefault("statement_summary", summary_text or rationale)
                    item.setdefault(
                        "candidate_scope",
                        "milestone" if metric_name == "Strategic milestone" else "operational",
                    )
                elif (
                    is_gpre_profile
                    and not re.search(
                        r"^\s*\[(?:dropped|new|repeat)\]\s*|"
                        r"\b(map|maps|permit list|county map|table of contents|legend|project map|site map)\b|"
                        r"\b(latitude|longitude|parcel|township|range|section)\b",
                        rationale,
                        re.I,
                    )
                    and not re.search(
                        r"(^\s*[a-z]?\s*(?:for the )?three months ended\b|"
                        r"^\s*\d{1,2},\s*20\d{2}\s+compared to\b|"
                        r"\bcompared to the same period\b|\bconsolidated results\b|"
                        r"\bfor the (?:three|nine|twelve) months ended\b|"
                        r"\binterest expense was\b.*\bcompared to\b)",
                        rationale,
                        re.I,
                    )
                    and re.search(
                        r"\b(45z|debt reduction|cost savings|carbon capture|strategic milestone|interest expense|obion|fully operational|agreement executed|qualify for production tax credits)\b",
                        metric_name + " | " + rationale,
                        re.I,
                    )
                    and (
                        local_timing_hint
                        or local_numeric_hint
                        or local_action_hint
                    )
                ):
                    item.setdefault("statement_summary", summary_text or rationale)
                    item.setdefault(
                        "candidate_scope",
                        "milestone" if "milestone" in metric_low else "operational",
                    )
                elif (
                    is_gpre_profile
                    and source_bucket in {"tracker_ui_recall", "quarter_notes_ui_seed"}
                    and source_type in {"quarter_notes_ui", "tracker_ui", "earnings_release", "earnings_presentation", "presentation"}
                    and re.search(r"\b(45z|debt reduction|cost savings|carbon capture|strategic milestone|interest expense|obion|repaid|fully operational|online and ramping)\b", metric_name + " | " + rationale, re.I)
                    and not _slide_signal_noise(rationale)
                    and not re.search(r"^\s*(table|permit|map|list|appendix|note:)\b", rationale, re.I)
                ):
                    item.setdefault("statement_summary", summary_text or rationale)
                    item.setdefault("candidate_scope", "milestone" if "milestone" in metric_low else "operational")
                else:
                    return False
            if qualified_promise is not None:
                item.setdefault("statement_summary", qualified_promise.summary)
                item.setdefault("candidate_scope", qualified_promise.scope)
            explicit_timing = bool(re.search(r"\b(fy\s*20\d{2}|20\d{2}|q[1-4]|quarter|full[- ]?year|annualized)\b", rationale, re.I))
            numeric_target = bool(
                pd.notna(pd.to_numeric(item.get("target"), errors="coerce"))
                or re.search(r"\$?\s*\d+(?:\.\d+)?\s*(?:m|mm|million|b|bn|%|x)?", rationale, re.I)
            )
            action_or_target = bool(
                re.search(
                    r"\b(target|guidance|expected|expect|opportunity|on track|fully operational|online|ramping|completed|executed|sale completed|repaid|savings)\b",
                    rationale,
                    re.I,
                )
            )
            if is_45z_metric and _is_45z_crush_margin_support_only(rationale):
                return False
            if metric_low == "revenue_yoy":
                return False
            if metric_low in {"capital_allocation", "management target", "tone | corporate"}:
                return False
            if metric_low == "utilization":
                return explicit_timing and bool(
                    re.search(r"\b(utilization|operating rate|capacity utilization)\b", rationale, re.I)
                    and re.search(r"(?<!\d)\d{2,3}%", rationale)
                    and re.search(r"\b(target|expected|on track|objective|goal|maintain|continue to|will)\b", rationale, re.I)
                )
            if metric_low == "risk management":
                return explicit_timing and bool(
                    re.search(r"\brisk management\b", rationale, re.I)
                    and re.search(r"\b(margins?|cash flow|lock in favorable|protect downside|economics)\b", rationale, re.I)
                    and re.search(r"\b(expected|on track|objective|goal|continue to|will|supports?)\b", rationale, re.I)
                )
            if is_pbi_profile:
                latest_txt = str(item.get("latest") or "")
                latest_num = pd.to_numeric(item.get("latest"), errors="coerce")
                combined_blob = rationale if pd.notna(latest_num) else " | ".join([rationale, latest_txt])
                if _looks_pbi_fragment_text(combined_blob):
                    return False
                clean_sentence = _is_pbi_clean_sentence(rationale) or _is_pbi_clean_sentence(latest_txt)
                if metric_low == "strategic milestone" or promise_type_val.lower() == "milestone":
                    return clean_sentence and bool(
                        explicit_timing
                        or action_or_target
                        or re.search(r"\b(fully operational|online and ramping|agreement executed|completed|launched|repaid)\b", combined_blob, re.I)
                    )
                if not _pbi_promise_theme_re.search(f"{metric_name} | {rationale} | {latest_txt}"):
                    return False
                specific_label = _classify_pbi_metric_label(
                    " | ".join([metric_name, rationale, str(item.get("target") or ""), latest_txt]),
                    "",
                )
                src_blob = str(item.get("_source_doc") or "") + " " + str(item.get("promise_type") or "")
                preferred_src = _is_preferred_narrative_source(src_blob) or not str(item.get("_source_doc") or "").strip()
                target_display = _extract_pbi_target_display(
                    " | ".join([metric_name, rationale, latest_txt]),
                    specific_label or metric_name,
                )
                effective_label = specific_label or metric_name
                if not preferred_src:
                    return False
                if effective_label in {"", "Operating target", "Management target", "Tone | Corporate"}:
                    return False
                if effective_label == "Strategic milestone":
                    return clean_sentence and (explicit_timing or action_or_target)
                if effective_label in {
                    "Adjusted EBIT guidance",
                    "Revenue guidance",
                    "EPS guidance",
                    "FCF target",
                    "Cost savings target",
                    "PB Bank liquidity release",
                    "Deleveraging target",
                    "SendTech / Presort operating target",
                }:
                    if not (_pbi_target_display_ok(target_display) or _pbi_target_display_ok(item.get("target"))):
                        return False
                    latest_clean = not latest_txt or pd.notna(latest_num) or (
                        not _looks_pbi_fragment_text(latest_txt) and len(latest_txt) <= 120
                    )
                    return latest_clean
                return clean_sentence and (numeric_target or explicit_timing or action_or_target)
            if is_gpre_profile and (
                is_45z_metric
                or re.search(r"\b(debt reduction|cost savings|carbon capture|strategic milestone)\b", metric_low, re.I)
            ):
                if metric_low == "strategic milestone" and re.search(
                    r"\b(online and ramping(?: up capture volumes)?|fully online delivering biogenic co2)\b",
                    rationale,
                    re.I,
                ) and not re.search(r"\b(york|advantage nebraska|central city|wood river|obion)\b", rationale, re.I):
                    return False
                if metric_name in {
                    "45Z-related Adjusted EBITDA",
                    "45Z monetization / EBITDA",
                    "45Z plant qualification readiness",
                    "45Z from remaining facilities",
                    "Advantage Nebraska EBITDA opportunity",
                    "Cost savings target",
                    "Debt reduction",
                    "Interest expense outlook",
                    "Advantage Nebraska startup",
                }:
                    return bool(
                        numeric_target
                        or explicit_timing
                        or action_or_target
                        or re.search(
                            r"\b(fully operational|online and ramping|agreement executed|repaid|repayment completed|construction progressing|commissioning|on track|expected|opportunity|annualized)\b",
                            rationale,
                            re.I,
                        )
                    )
                return bool(
                    numeric_target
                    or explicit_timing
                    or action_or_target
                    or re.search(
                        r"\b(fully operational|online and ramping|agreement executed|repaid|repayment completed|construction progressing|commissioning|on track)\b",
                        rationale,
                        re.I,
                    )
                )
            if metric_low == "strategic milestone":
                return explicit_timing or action_or_target
            if metric_low in {"45z monetization / ebitda", "cost savings", "debt reduction"} or is_45z_metric:
                return True
            if metric_low == "management target":
                return explicit_timing and (numeric_target or action_or_target)
            return numeric_target and action_or_target

        selected_rows = [z for z in selected_rows if _promise_progress_keep_item(z)]
        if is_pbi_profile and not selected_rows:
            pbi_rescue_candidates = [
                z
                for z in oper_rows
                if str(z.get("metric_ref") or "").strip()
                in {
                    "Revenue guidance",
                    "Adjusted EBIT guidance",
                    "EPS guidance",
                    "FCF target",
                    "Cost savings target",
                    "PB Bank liquidity release",
                    "Deleveraging target",
                    "Strategic milestone",
                }
                and not _looks_pbi_fragment_text(
                    " | ".join(
                        [
                            str(z.get("rationale") or ""),
                            str(z.get("target") or ""),
                            str(z.get("latest") or ""),
                        ]
                    )
                )
                and (
                    (
                        str(z.get("metric_ref") or "").strip() == "PB Bank liquidity release"
                        and re.search(
                            r"\b(pb bank|bank-held leases|trapped capital|cash optimization|liquidity)\b",
                            " | ".join(
                                [
                                    str(z.get("target") or ""),
                                    str(z.get("latest") or ""),
                                    str(z.get("rationale") or ""),
                                    str(z.get("_source_snip") or ""),
                                ]
                            ),
                            re.I,
                        )
                    )
                    or (
                        str(z.get("metric_ref") or "").strip() == "Cost savings target"
                        and re.search(
                            r"\b(cost savings|cost reduction|annualized savings|run-rate)\b",
                            " | ".join(
                                [
                                    str(z.get("target") or ""),
                                    str(z.get("latest") or ""),
                                    str(z.get("rationale") or ""),
                                    str(z.get("_source_snip") or ""),
                                ]
                            ),
                            re.I,
                        )
                    )
                    or (
                        str(z.get("metric_ref") or "").strip() == "Deleveraging target"
                        and re.search(
                            r"\b(deleverag|debt|repay|repayment|paydown|liquidity)\b",
                            " | ".join(
                                [
                                    str(z.get("target") or ""),
                                    str(z.get("latest") or ""),
                                    str(z.get("rationale") or ""),
                                    str(z.get("_source_snip") or ""),
                                ]
                            ),
                            re.I,
                        )
                    )
                    or str(z.get("metric_ref") or "").strip()
                    in {
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "Strategic milestone",
                    }
                )
                and (
                    _pbi_target_display_ok(str(z.get("target") or ""))
                    or bool(
                        re.search(
                            r"\b(fy\s*20\d{2}|20\d{2}|q[1-4]|quarter|full[- ]?year|on track|progress)\b",
                            str(z.get("rationale") or ""),
                            re.I,
                        )
                    )
                )
            ]
            pbi_rescue_candidates = sorted(
                pbi_rescue_candidates,
                key=lambda z: (
                    int(z.get("_status_pri") or 9),
                    -float(z.get("_score") or 0.0),
                    -int(z.get("_clean_target_bonus") or 0),
                ),
            )
            selected_rows = pbi_rescue_candidates[:2]
        supplemental_progress_rows: List[Dict[str, Any]] = []

        def _quarter_note_seed_rows_for_qd(qd_seed: date) -> List[Dict[str, Any]]:
            if not isinstance(quarter_note_rows_map, dict):
                return []
            out_rows: List[Dict[str, Any]] = []
            for q_key, recs in quarter_note_rows_map.items():
                if _qend(q_key) != qd_seed or not isinstance(recs, list):
                    continue
                out_rows.extend([x for x in recs if isinstance(x, dict)])
            return out_rows

        def _build_qnote_progress_seed(qd_seed: date, note_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            metric_name = _progress_metric_from_event(note_item) or _progress_metric_from_qnote(note_item)
            if not metric_name:
                return None
            txt_full = glx_normalize_text(
                str(
                    note_item.get("text_full")
                    or note_item.get("comment_full_text")
                    or note_item.get("text")
                    or note_item.get("comment")
                    or note_item.get("rationale")
                    or ""
                )
            )
            summary_txt = glx_normalize_text(str(note_item.get("_render_summary") or ""))
            basis_txt = summary_txt or txt_full
            if not basis_txt or _slide_signal_noise(basis_txt):
                return None
            entity_blob = " | ".join(
                [
                    txt_full,
                    summary_txt,
                    str(note_item.get("_event_entity_scope") or ""),
                    str(note_item.get("metric_ref") or ""),
                ]
            )
            if (
                metric_name == "Strategic milestone"
                and re.search(r"\bonline and ramping up capture volumes\b|\bplant online / ramping\b", basis_txt, re.I)
                and not re.search(r"\b(york|advantage nebraska|central city|wood river|obion)\b", entity_blob, re.I)
            ):
                return None
            if (
                metric_name == "Strategic milestone"
                and re.search(r"\bfully online delivering biogenic co2\b", basis_txt, re.I)
                and not re.search(r"\b(york|advantage nebraska)\b", entity_blob, re.I)
            ):
                return None
            target_txt = _progress_target_display_from_qnote(qd_seed, metric_name, txt_full or basis_txt)
            latest_txt = ""
            derived_latest = _extract_progress_latest_basis(metric_name, " | ".join([basis_txt, txt_full]))
            if derived_latest:
                latest_txt = str(derived_latest)
            elif metric_name == "Strategic milestone":
                latest_txt = summary_txt or basis_txt
            elif re.search(r"\b(debt reduction|deleverag|liquidity|cost savings)\b", metric_name, re.I):
                latest_txt = summary_txt or ""
            if metric_name == "Capex guidance (FY 2026)":
                latest_txt = ""
            src = dict(note_item.get("source") or {})
            source_type = (
                src.get("source_type")
                or src.get("doc_type")
                or note_item.get("source_type")
                or note_item.get("doc_type")
                or "quarter_notes_ui"
            )
            source_doc = src.get("doc") or note_item.get("doc") or "Quarter_Notes_UI"
            seed = {
                "promise_id": str(
                    note_item.get("note_id")
                    or hashlib.sha1(f"{qd_seed}|qnote_progress|{metric_name}|{basis_txt}".encode("utf-8")).hexdigest()[:12]
                ),
                "metric_ref": metric_name,
                "target": target_txt,
                "latest": latest_txt,
                "promise_key": str(
                    note_item.get("_event_key")
                    or note_item.get("theme_key")
                    or metric_name.lower().replace(" ", "_")
                ),
                "target_bucket": "quarter_notes_ui_seed",
                "promise_type": "milestone" if metric_name == "Strategic milestone" else "operational",
                "scorable": bool(target_txt),
                "numeric_update_this_quarter": False,
                "status": str(_progress_status_from_tracker(metric_name, basis_txt)),
                "rationale": basis_txt,
                "guidance_type": "",
                "target_period_norm": str(
                    note_item.get("_event_period_norm") or note_item.get("period_norm") or ""
                ),
                "target_period_label": str(note_item.get("period_label") or ""),
                "first_seen_quarter_end": str(qd_seed),
                "last_seen_quarter_end": str(qd_seed),
                "first_seen_evidence_quarter_end": str(qd_seed),
                "last_seen_evidence_quarter_end": str(qd_seed),
                "last_seen_numeric_quarter_end": "",
                "last_seen_text_quarter_end": str(qd_seed),
                "carried_to_quarter_end": str(qd_seed),
                "evaluated_through": "",
                "qa_severity": "",
                "qa_message": "quarter_notes_ui_seed",
                "_source_snip": txt_full or basis_txt,
                "_source_doc": str(source_doc),
                "_source_type": str(source_type),
                "_source_document_id": str(
                    src.get("source_document_id")
                    or note_item.get("source_document_id")
                    or ""
                ),
                "_source_occurrence_id": str(
                    src.get("source_occurrence_id")
                    or note_item.get("source_occurrence_id")
                    or note_item.get("evidence_occurrence_id")
                    or ""
                ),
                "_source_locator": str(
                    src.get("source_locator")
                    or src.get("section")
                    or note_item.get("source_locator")
                    or ""
                ),
                "_status_pri": 0 if str(_progress_status_from_tracker(metric_name, basis_txt)).lower() == "completed" else 1,
                "_score": float(note_item.get("score") or 0.0),
                "_fragment_penalty": _text_fragment_penalty(basis_txt),
                "_clean_target_bonus": _clean_target_bonus(" | ".join([target_txt, basis_txt])),
                "statement_summary": summary_txt or basis_txt,
            }
            split_blob = " | ".join([str(seed.get("target") or ""), basis_txt, str(seed.get("_source_snip") or "")])
            seed.update(
                _derive_split_target_meta(
                    seed.get("metric_ref"),
                    split_blob,
                    seed.get("guidance_type") or seed.get("target_bucket") or "",
                    qd_seed,
                    source_type,
                    source_doc,
                    "",
                )
            )
            seed["promise_group"] = str(seed.get("target_group_key") or "")
            seed["metric_display"] = _split_target_metric_display(seed.get("metric_ref"), split_blob, seed)
            return seed

        def _build_tracker_progress_seed(qd_seed: date, tracker_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            metric_name = str(
                tracker_item.get("metric_display")
                or tracker_item.get("metric")
                or tracker_item.get("metric_ref")
                or ""
            ).strip()
            if re.search(
                r"^\s*(which time|portion of|at which time|for which|that will|who will|where the|the partnership|the merger|the transactions?)\b",
                metric_name,
                re.I,
            ):
                return None
            if not metric_name:
                return None
            rationale_txt = glx_normalize_text(
                str(tracker_item.get("statement_summary") or tracker_item.get("text_full") or tracker_item.get("text_snippet") or "")
            )
            if not rationale_txt or _slide_signal_noise(rationale_txt):
                return None
            if metric_name == "Strategic milestone" and re.search(
                r"\bonline and ramping up capture volumes\b|\bplant online / ramping\b",
                rationale_txt,
                re.I,
            ) and not re.search(r"\b(york|advantage nebraska|central city|wood river|obion)\b", rationale_txt, re.I):
                return None
            if metric_name == "Strategic milestone" and re.search(
                r"\bfully online delivering biogenic co2\b",
                rationale_txt,
                re.I,
            ) and not re.search(r"\b(york|advantage nebraska)\b", rationale_txt, re.I):
                return None
            source = dict(tracker_item.get("source") or {})
            target_txt = str(tracker_item.get("target_display") or tracker_item.get("target") or "").strip()
            latest_txt = str(tracker_item.get("latest_display") or "").strip()
            status_txt = str(
                tracker_item.get("status_hint")
                or _progress_status_from_tracker(metric_name, rationale_txt)
                or ""
            ).strip()
            seed = {
                "promise_id": str(
                    tracker_item.get("promise_id")
                    or hashlib.sha1(f"{qd_seed}|tracker_progress|{metric_name}|{rationale_txt}".encode("utf-8")).hexdigest()[:12]
                ),
                "metric_ref": metric_name,
                "target": target_txt,
                "latest": latest_txt,
                "promise_key": str(
                    tracker_item.get("theme_key")
                    or tracker_item.get("promise_key")
                    or metric_name.lower().replace(" ", "_")
                ),
                "target_bucket": "tracker_ui_recall",
                "promise_type": "milestone" if metric_name == "Strategic milestone" else "operational",
                "scorable": bool(target_txt),
                "numeric_update_this_quarter": False,
                "status": status_txt,
                "rationale": rationale_txt,
                "guidance_type": str(tracker_item.get("guidance_type") or ""),
                "target_period_norm": str(
                    tracker_item.get("target_period_norm") or tracker_item.get("period_key") or ""
                ),
                "target_period_label": str(tracker_item.get("period_label") or ""),
                "first_seen_quarter_end": str(qd_seed),
                "last_seen_quarter_end": str(qd_seed),
                "first_seen_evidence_quarter_end": str(qd_seed),
                "last_seen_evidence_quarter_end": str(qd_seed),
                "last_seen_numeric_quarter_end": "",
                "last_seen_text_quarter_end": str(qd_seed),
                "carried_to_quarter_end": str(qd_seed),
                "evaluated_through": "",
                "qa_severity": "",
                "qa_message": "tracker_ui_recall",
                "_source_snip": rationale_txt,
                "_source_doc": str(source.get("doc") or tracker_item.get("source_doc") or ""),
                "_source_type": str(source.get("source_type") or tracker_item.get("source_type") or "tracker_ui"),
                "_status_pri": 0 if status_txt.lower() == "completed" else 1,
                "_score": float(tracker_item.get("score") or 0.0),
                "_fragment_penalty": _text_fragment_penalty(rationale_txt),
                "_clean_target_bonus": _clean_target_bonus(" | ".join([target_txt, rationale_txt])),
                "statement_summary": rationale_txt,
            }
            split_blob = " | ".join([target_txt, rationale_txt, str(seed.get("_source_snip") or "")])
            seed.update(
                _derive_split_target_meta(
                    seed.get("metric_ref"),
                    split_blob,
                    seed.get("guidance_type") or seed.get("target_bucket") or "",
                    qd_seed,
                    seed.get("_source_type") or "tracker_ui",
                    seed.get("_source_doc") or "",
                    "",
                )
            )
            seed["promise_group"] = str(seed.get("target_group_key") or "")
            seed["metric_display"] = _split_target_metric_display(seed.get("metric_ref"), split_blob, seed)
            return seed

        for rec in tracker_seed_rows:
            tracker_seed = _build_tracker_progress_seed(qd, rec)
            if tracker_seed is not None:
                supplemental_progress_rows.append(tracker_seed)

        for rec in _quarter_note_seed_rows_for_qd(qd):
            qnote_seed = _build_qnote_progress_seed(qd, rec)
            if qnote_seed is not None:
                supplemental_progress_rows.append(qnote_seed)

        slide_signals = _load_profile_slide_signals()
        if slide_signals:
            for rec in sorted(
                [x for x in slide_signals if x.get("quarter") == qd],
                key=lambda z: -float(z.get("score") or 0.0),
            ):
                metric_name = str(rec.get("metric") or "").strip()
                txt_full = glx_normalize_text(str(rec.get("text") or ""))
                if not metric_name or not txt_full:
                    continue
                status_hint = str(rec.get("status_hint") or "").strip().lower()
                if not status_hint:
                    continue
                supplemental_progress_rows.append(
                    {
                        "promise_id": hashlib.sha1(f"{qd}|progress|{metric_name}|{txt_full}".encode("utf-8")).hexdigest()[:12],
                        "metric_ref": metric_name,
                        "target": str(rec.get("target_display") or ""),
                        "latest": "",
                        "promise_key": metric_name.lower().replace(" ", "_"),
                        "target_bucket": "slides_signal",
                        "promise_type": "milestone" if bool(rec.get("is_milestone")) else "operational",
                        "scorable": bool(rec.get("is_numeric_target")),
                        "numeric_update_this_quarter": False,
                        "status": status_hint,
                        "rationale": txt_full,
                        "guidance_type": "period" if bool(rec.get("is_numeric_target")) else ("milestone" if bool(rec.get("is_milestone")) else ""),
                        "first_seen_quarter_end": str(qd),
                        "last_seen_quarter_end": str(qd),
                        "first_seen_evidence_quarter_end": str(qd),
                        "last_seen_evidence_quarter_end": str(qd),
                        "last_seen_numeric_quarter_end": "",
                        "last_seen_text_quarter_end": str(qd),
                        "carried_to_quarter_end": str(qd),
                        "evaluated_through": "",
                        "qa_severity": "",
                        "qa_message": "",
                        "_source_snip": txt_full,
                        "_source_doc": "",
                        "_status_pri": 0 if status_hint == "completed" else 1,
                        "_score": float(rec.get("score") or 0.0),
                        "_fragment_penalty": _text_fragment_penalty(txt_full),
                        "_clean_target_bonus": _clean_target_bonus(txt_full),
                    }
                )
        if supplemental_progress_rows:
            for rec in supplemental_progress_rows:
                split_blob = " | ".join([str(rec.get("target") or ""), str(rec.get("rationale") or ""), str(rec.get("_source_snip") or "")])
                rec.update(
                    _derive_split_target_meta(
                        rec.get("metric_ref"),
                        split_blob,
                        rec.get("guidance_type") or rec.get("target_bucket") or "",
                        qd,
                        "earnings_presentation",
                        rec.get("_source_doc") or "",
                        "",
                    )
                )
                rec["promise_group"] = str(rec.get("target_group_key") or "")
                rec["metric_display"] = _split_target_metric_display(rec.get("metric_ref"), split_blob, rec)
            selected_rows = [
                z for z in selected_rows
                if str(z.get("metric_ref") or "").strip().lower() != "revenue_yoy"
            ]
            existing_by_metric = {
                _progress_identity_key_local(z): i
                for i, z in enumerate(selected_rows)
                if str(z.get("metric_ref") or "").strip() or str(z.get("scope_key") or "").strip()
            }
            for rec in supplemental_progress_rows:
                mkey = _progress_identity_key_local(rec)
                if not any(mkey):
                    continue
                if mkey in existing_by_metric:
                    existing_item = selected_rows[existing_by_metric[mkey]]
                    rec_latest = str(rec.get("latest") or "").strip().lower()
                    existing_latest = str(existing_item.get("latest") or "").strip().lower()
                    prefer_candidate = _candidate_quality_key(
                        rec.get("rationale"),
                        "",
                        "",
                        0,
                        rec.get("_score"),
                    ) < _candidate_quality_key(
                        existing_item.get("rationale"),
                        "",
                        "",
                        0,
                        existing_item.get("_score"),
                    )
                    if rec_latest not in {"", "not yet measurable"} and existing_latest in {"", "not yet measurable"}:
                        prefer_candidate = True
                    if mkey == "45z monetization / ebitda" and "ebitda opportunity" in str(rec.get("rationale") or "").lower():
                        prefer_candidate = True
                    if prefer_candidate:
                        selected_rows[existing_by_metric[mkey]] = rec
                    continue
                selected_rows.append(rec)
                existing_by_metric[mkey] = len(selected_rows) - 1
            selected_rows = sorted(
                [z for z in selected_rows if _promise_progress_keep_item(z)],
                key=lambda z: (
                    _promise_progress_visible_category_rank_local(z),
                    int(z.get("_status_pri") or 9),
                    int(z.get("_fragment_penalty") or 0),
                    -int(z.get("_clean_target_bonus") or 0),
                    str(z.get("metric_ref") or "").lower(),
                ),
            )
        def _build_tracker_progress_row(qd_seed: date, rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            metric_name = str(rec.get("metric") or "").strip()
            txt_full = glx_normalize_text(str(rec.get("text_full") or rec.get("text_snippet") or ""))
            if not metric_name or not txt_full or _slide_signal_noise(txt_full):
                return None
            tracker_row = {
                "promise_id": str(rec.get("promise_id") or hashlib.sha1(f"{qd_seed}|tracker_progress|{metric_name}|{txt_full}".encode("utf-8")).hexdigest()[:12]),
                "metric_ref": metric_name,
                "target": str(rec.get("target_display") or ""),
                "latest": str(rec.get("latest_display") or ""),
                "promise_key": str(rec.get("theme_key") or metric_name.lower()),
                "target_bucket": "tracker_ui_recall",
                "promise_type": "milestone" if metric_name == "Strategic milestone" else "operational",
                "scorable": False,
                "numeric_update_this_quarter": False,
                "status": str(rec.get("status_hint") or _progress_status_from_tracker(metric_name, txt_full)),
                "rationale": txt_full,
                "guidance_type": str(rec.get("guidance_type") or ""),
                "target_period_norm": str(rec.get("target_period_norm") or rec.get("period_key") or ""),
                "target_period_label": str(rec.get("period_label") or ""),
                "first_seen_quarter_end": str(rec.get("first_seen_quarter_end") or qd_seed),
                "last_seen_quarter_end": str(rec.get("last_seen_quarter_end") or qd_seed),
                "first_seen_evidence_quarter_end": str(rec.get("first_seen_quarter_end") or qd_seed),
                "last_seen_evidence_quarter_end": str(rec.get("last_seen_quarter_end") or qd_seed),
                "last_seen_numeric_quarter_end": "",
                "last_seen_text_quarter_end": str(rec.get("last_seen_quarter_end") or qd_seed),
                "carried_to_quarter_end": str(qd_seed),
                "evaluated_through": "",
                "qa_severity": "",
                "qa_message": "tracker_ui_recall",
                "_source_snip": txt_full,
                "_source_doc": "",
                "_status_pri": 1,
                "_score": float(rec.get("score") or 0.0),
                "_fragment_penalty": _text_fragment_penalty(txt_full),
                "_clean_target_bonus": _clean_target_bonus(txt_full),
            }
            tracker_row.update(
                _derive_split_target_meta(
                    metric_name,
                    " | ".join([str(tracker_row.get("target") or ""), txt_full]),
                    rec.get("target_period_norm") or rec.get("period_key") or rec.get("period_label") or "",
                    qd_seed,
                    dict(rec.get("source") or {}).get("source_type") or "tracker_ui",
                    dict(rec.get("source") or {}).get("doc") or "",
                    dict(rec.get("source") or {}).get("section") or "",
                )
            )
            if str(rec.get("target_period_norm") or "").strip():
                tracker_row["target_period_norm"] = str(rec.get("target_period_norm") or "").strip()
            if str(rec.get("period_label") or "").strip():
                tracker_row["target_period_label"] = str(rec.get("period_label") or "").strip()
            tracker_row["promise_group"] = str(tracker_row.get("target_group_key") or "")
            tracker_row["metric_display"] = str(rec.get("metric_display") or _split_target_metric_display(metric_name, txt_full, tracker_row))
            tracker_row["statement_summary"] = str(rec.get("statement_summary") or rec.get("text_snippet") or "")
            return tracker_row

        tracker_rows = tracker_rows_map.get(qd, []) if isinstance(tracker_rows_map, dict) else []
        if tracker_rows:
            existing_by_metric = {
                _progress_identity_key_local(z): i
                for i, z in enumerate(selected_rows)
                if str(z.get("metric_ref") or "").strip() or str(z.get("scope_key") or "").strip()
            }
            added_from_tracker = 0
            tracker_rows_iter = list(tracker_rows)
            if not is_gpre_profile:
                def _pbi_tracker_priority_local(rec_local: Dict[str, Any]) -> Tuple[int, float, str]:
                    metric_blob = glx_normalize_text(
                        " | ".join(
                            [
                                str(rec_local.get("metric_display") or rec_local.get("metric") or rec_local.get("metric_ref") or ""),
                                str(rec_local.get("text_full") or rec_local.get("text_snippet") or ""),
                                str(rec_local.get("target_display") or ""),
                                str(rec_local.get("latest_display") or ""),
                            ]
                        )
                    ).lower()
                    if re.search(r"\bstrategic review|strategic milestone\b", metric_blob, re.I):
                        return (0, -float(rec_local.get("score") or 0.0), metric_blob)
                    if re.search(r"\bcost savings|annualized savings|run-rate savings\b", metric_blob, re.I):
                        return (1, -float(rec_local.get("score") or 0.0), metric_blob)
                    if re.search(r"\b(liquidity release|capital allocation|pb bank)\b", metric_blob, re.I):
                        return (2, -float(rec_local.get("score") or 0.0), metric_blob)
                    return (5, -float(rec_local.get("score") or 0.0), metric_blob)
                tracker_rows_iter = sorted(tracker_rows_iter, key=_pbi_tracker_priority_local)
            for rec in tracker_rows_iter:
                tracker_row = _build_tracker_progress_row(qd, rec)
                if tracker_row is None:
                    continue
                metric_name = str(tracker_row.get("metric_ref") or "").strip()
                metric_key = _progress_identity_key_local(tracker_row)
                if not _promise_progress_keep_item(tracker_row):
                    continue
                is_priority_metric = bool(
                    is_gpre_profile
                    and re.search(r"\b(45z|debt reduction|cost savings|carbon capture|strategic milestone)\b", metric_name, re.I)
                )
                if metric_key in existing_by_metric:
                    existing_item = selected_rows[existing_by_metric[metric_key]]
                    prefer_candidate = _candidate_quality_key(
                        tracker_row.get("rationale"),
                        "",
                        "",
                        0,
                        tracker_row.get("_score"),
                    ) < _candidate_quality_key(
                        existing_item.get("rationale"),
                        "",
                        "",
                        0,
                        existing_item.get("_score"),
                    )
                    if prefer_candidate:
                        selected_rows[existing_by_metric[metric_key]] = tracker_row
                    continue
                selected_rows.append(tracker_row)
                existing_by_metric[metric_key] = len(selected_rows) - 1
                added_from_tracker += 1
                if (not is_gpre_profile and len(selected_rows) >= 6) or (is_gpre_profile and added_from_tracker >= 3 and len(selected_rows) >= 5):
                    break
            if added_from_tracker > 0:
                selected_rows = sorted(
                    [z for z in selected_rows if _promise_progress_keep_item(z)],
                    key=lambda z: (
                        _promise_progress_visible_category_rank_local(z),
                        int(z.get("_status_pri") or 9),
                        int(z.get("_fragment_penalty") or 0),
                        -int(z.get("_clean_target_bonus") or 0),
                        str(z.get("metric_display") or z.get("metric_ref") or "").lower(),
                    ),
                )
                ui_info_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Promise_Progress_UI",
                        "severity": "info",
                        "message": f"tracker_ui_recall added={added_from_tracker}",
                        "source": "Promise_Tracker_UI",
                    }
                )
        finalized_rows: List[Dict[str, Any]] = []
        for z in selected_rows:
            finalized = _finalize_progress_item(dict(z))
            if finalized is None:
                ui_info_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Promise_Progress_UI",
                        "severity": "info",
                        "message": f"dropped_reason=weak_progress_basis | metric={str(z.get('metric_ref') or '')}",
                        "source": str(z.get("_source_doc") or ""),
                    }
                )
                continue
            metric_final = str(finalized.get("metric_ref") or finalized.get("metric") or "").strip()
            rationale_final = glx_normalize_text(str(finalized.get("rationale") or ""))
            if re.search(
                r"^\s*(which time|portion of|at which time|for which|that will|who will|where the|the partnership|the merger|the transactions?)\b",
                metric_final,
                re.I,
            ):
                continue
            if is_gpre_profile and str(metric_final).strip().lower() == "strategic milestone" and re.search(
                r"\b(online and ramping(?: up capture volumes)?|fully online delivering biogenic co2)\b",
                rationale_final,
                re.I,
            ) and not re.search(r"\b(york|advantage nebraska|central city|wood river|obion)\b", rationale_final, re.I):
                continue
            finalized_rows.append(finalized)
        actual_progress_count = len(
            [
                x
                for x in finalized_rows
                if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
            ]
        )
        if actual_progress_count < 3 and supplemental_progress_rows:
            existing_progress_keys = {
                _progress_identity_key_local(x)
                for x in finalized_rows
                if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
            }
            sparse_backfill_added = 0
            for rec in sorted(
                supplemental_progress_rows,
                key=lambda z: (
                    int(z.get("_status_pri") or 9),
                    -float(z.get("_score") or 0.0),
                    int(z.get("_fragment_penalty") or 0),
                ),
            ):
                metric_key = _progress_identity_key_local(rec)
                if metric_key in existing_progress_keys:
                    continue
                if not _promise_progress_keep_item(rec):
                    continue
                finalized = _finalize_progress_item(dict(rec))
                if finalized is None:
                    continue
                finalized_rows.append(finalized)
                existing_progress_keys.add(metric_key)
                sparse_backfill_added += 1
                if len(
                    [
                        x
                        for x in finalized_rows
                        if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
                    ]
                ) >= 3:
                    break
            if sparse_backfill_added > 0:
                ui_info_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Promise_Progress_UI",
                        "severity": "info",
                        "message": f"sparse_progress_backfill added={int(sparse_backfill_added)}",
                        "source": "Quarter_Notes_UI",
                    }
                )
        current_progress_count = len(
            [
                x
                for x in finalized_rows
                if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
            ]
        )
        if current_progress_count < 3 and tracker_rows:
            existing_progress_keys = {
                _progress_identity_key_local(x)
                for x in finalized_rows
                if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
            }
            tracker_backfill_added = 0
            for rec in tracker_rows:
                tracker_row = _build_tracker_progress_row(qd, rec)
                if tracker_row is None:
                    continue
                metric_key = _progress_identity_key_local(tracker_row)
                if metric_key in existing_progress_keys or not _promise_progress_keep_item(tracker_row):
                    continue
                finalized = _finalize_progress_item(dict(tracker_row))
                if finalized is None:
                    continue
                finalized_rows.append(finalized)
                existing_progress_keys.add(metric_key)
                tracker_backfill_added += 1
                if len(
                    [
                        x
                        for x in finalized_rows
                        if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
                    ]
                ) >= 3:
                    break
            if tracker_backfill_added > 0:
                ui_info_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Promise_Progress_UI",
                        "severity": "info",
                        "message": f"sparse_tracker_backfill added={int(tracker_backfill_added)}",
                        "source": "Promise_Tracker_UI",
                    }
                )
        current_progress_count = len(
            [
                x
                for x in finalized_rows
                if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
            ]
        )
        if current_progress_count < 3:
            raw_qn = _quarter_notes_view(quarter_mode="date")
            if isinstance(raw_qn, pd.DataFrame) and not raw_qn.empty and "quarter" in raw_qn.columns:
                existing_progress_keys = {
                    _progress_identity_key_local(x)
                    for x in finalized_rows
                    if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
                }
                raw_backfill_added = 0
                raw_qn_sub = raw_qn[pd.to_datetime(raw_qn["quarter"], errors="coerce").dt.date == qd]
                if "score" in raw_qn_sub.columns:
                    raw_qn_iter = raw_qn_sub.sort_values("score", ascending=False, na_position="last").iterrows()
                else:
                    raw_qn_iter = raw_qn_sub.iterrows()
                for _, raw_rec in raw_qn_iter:
                    raw_text = glx_normalize_text(
                        str(raw_rec.get("note") or raw_rec.get("claim") or raw_rec.get("evidence_snippet") or "")
                    )
                    if not raw_text:
                        continue
                    event = shared_build_evidence_event(
                        raw_text,
                        metric_hint=" | ".join(
                            [
                                str(raw_rec.get("metric_ref") or ""),
                                str(raw_rec.get("category") or ""),
                            ]
                        ),
                        source_type=str(raw_rec.get("doc_type") or "quarter_notes"),
                        source_doc=str(raw_rec.get("doc") or ""),
                        base_score=float(pd.to_numeric(raw_rec.get("score"), errors="coerce") or 0.0),
                        period_norm=str(raw_rec.get("period_norm") or ""),
                    )
                    if event is None:
                        continue
                    metric_name = _progress_metric_from_event(
                        {
                            "_event_type": event.event_type,
                            "_event_metric_family": event.metric_family,
                            "_event_entity_scope": event.entity_scope,
                        }
                    ) or _progress_metric_from_qnote(
                        {
                            "text_full": raw_text,
                            "metric_ref": raw_rec.get("metric_ref"),
                            "metric_display": raw_rec.get("metric_ref"),
                        }
                    )
                    if not metric_name:
                        continue
                    raw_seed = _build_qnote_progress_seed(
                        qd,
                        {
                            "note_id": raw_rec.get("note_id"),
                            "text_full": raw_text,
                            "_render_summary": event.summary or raw_text,
                            "score": float(pd.to_numeric(raw_rec.get("score"), errors="coerce") or 0.0),
                            "metric_ref": raw_rec.get("metric_ref"),
                            "metric_display": raw_rec.get("metric_ref"),
                            "doc": raw_rec.get("doc"),
                            "doc_type": raw_rec.get("doc_type"),
                            "_event_type": event.event_type,
                            "_event_metric_family": event.metric_family,
                            "_event_entity_scope": event.entity_scope,
                            "_event_period_norm": event.period_norm,
                        },
                    )
                    if raw_seed is None:
                        continue
                    metric_key = _progress_identity_key_local(raw_seed)
                    if metric_key in existing_progress_keys or not _promise_progress_keep_item(raw_seed):
                        continue
                    finalized = _finalize_progress_item(dict(raw_seed))
                    if finalized is None:
                        continue
                    finalized_rows.append(finalized)
                    existing_progress_keys.add(metric_key)
                    raw_backfill_added += 1
                    if len(
                        [
                            x
                            for x in finalized_rows
                            if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}
                        ]
                    ) >= 3:
                        break
                if raw_backfill_added > 0:
                    ui_info_rows.append(
                        {
                            "quarter": qd,
                            "metric": "Promise_Progress_UI",
                            "severity": "info",
                            "message": f"raw_qnote_progress_backfill added={int(raw_backfill_added)}",
                            "source": "quarter_notes",
                        }
                    )

        def _append_targeted_qnote_progress_rows(rows_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            if not isinstance(quarter_note_rows_map, dict):
                return rows_in
            existing_metric_names = {
                str(name).strip()
                for row in rows_in
                if str(row.get("row_type") or "").strip().lower() not in {"section", "blank"}
                for name in [
                    row.get("metric_ref"),
                    row.get("metric_display"),
                    row.get("metric"),
                ]
                if str(name or "").strip()
            }
            existing_progress_keys = {
                _progress_identity_key_local(row)
                for row in rows_in
                if str(row.get("row_type") or "").strip().lower() not in {"section", "blank"}
            }
            targeted_added = 0
            for note_item in _quarter_note_seed_rows_for_qd(qd):
                raw_note_txt = glx_normalize_text(
                    str(
                        note_item.get("_render_summary")
                        or note_item.get("text_full")
                        or note_item.get("comment_full_text")
                        or note_item.get("text")
                        or note_item.get("comment")
                        or note_item.get("rationale")
                        or ""
                    )
                )
                pbi_priority_note = bool(
                    is_pbi_profile
                    and "Strategic milestone" not in existing_metric_names
                    and re.search(r"\bstrategic review\b", raw_note_txt, re.I)
                    and (
                        re.search(r"\b(q[1-4]\s*20\d{2}|q[1-4]|20\d{2}|on track|by end of)\b", raw_note_txt, re.I)
                        or (
                            re.search(r"\b(?:phase 2|second phase)\b", raw_note_txt, re.I)
                            and re.search(r"\binitiated\b", raw_note_txt, re.I)
                        )
                    )
                )
                seed = _build_qnote_progress_seed(qd, note_item)
                if seed is None and pbi_priority_note:
                    seed = {
                        "promise_id": str(note_item.get("note_id") or hashlib.sha1(f"{qd}|qnote_priority|{raw_note_txt}".encode("utf-8")).hexdigest()[:12]),
                        "metric_ref": "Strategic milestone",
                        "metric_display": "Strategic milestone",
                        "target": "",
                        "latest": raw_note_txt,
                        "promise_key": "strategic_review",
                        "target_bucket": "quarter_notes_ui_seed",
                        "promise_type": "milestone",
                        "scorable": False,
                        "numeric_update_this_quarter": False,
                        "status": "in progress",
                        "rationale": raw_note_txt,
                        "guidance_type": "milestone",
                        "target_period_norm": "",
                        "target_period_label": "",
                        "first_seen_quarter_end": str(qd),
                        "last_seen_quarter_end": str(qd),
                        "first_seen_evidence_quarter_end": str(qd),
                        "last_seen_evidence_quarter_end": str(qd),
                        "last_seen_numeric_quarter_end": "",
                        "last_seen_text_quarter_end": str(qd),
                        "carried_to_quarter_end": str(qd),
                        "evaluated_through": "",
                        "qa_severity": "",
                        "qa_message": "quarter_notes_ui_priority_seed",
                        "_source_snip": raw_note_txt,
                        "_source_doc": str(dict(note_item.get("source") or {}).get("doc") or note_item.get("doc") or "Quarter_Notes_UI"),
                        "_source_type": str(dict(note_item.get("source") or {}).get("source_type") or note_item.get("doc_type") or "quarter_notes_ui"),
                        "_status_pri": 1,
                        "_score": float(note_item.get("score") or 0.0),
                        "_fragment_penalty": _text_fragment_penalty(raw_note_txt),
                        "_clean_target_bonus": _clean_target_bonus(raw_note_txt),
                        "statement_summary": raw_note_txt,
                    }
                if seed is None:
                    continue
                seed_blob = glx_normalize_text(
                    " | ".join(
                        [
                            str(seed.get("metric_ref") or ""),
                            str(seed.get("metric_display") or ""),
                            str(seed.get("target") or ""),
                            str(seed.get("latest") or ""),
                            str(seed.get("rationale") or ""),
                            str(seed.get("_source_snip") or ""),
                        ]
                    )
                )
                priority_seed = bool(
                    pbi_priority_note
                    or (
                        is_pbi_profile
                    and str(seed.get("metric_ref") or "").strip() == "Strategic milestone"
                    and re.search(r"\bstrategic review\b", seed_blob, re.I)
                    )
                )
                if not priority_seed and not _promise_progress_keep_item(seed):
                    continue
                finalized = _finalize_progress_item(dict(seed))
                if finalized is None:
                    continue
                metric_ref = str(finalized.get("metric_ref") or "").strip()
                metric_label = str(
                    finalized.get("metric_display")
                    or metric_ref
                    or finalized.get("metric")
                    or ""
                ).strip()
                if not metric_label and not metric_ref:
                    continue
                metric_key = _progress_identity_key_local(finalized)
                if metric_key in existing_progress_keys:
                    continue
                note_blob = glx_normalize_text(
                    " | ".join(
                        [
                            str(finalized.get("target") or ""),
                            str(finalized.get("latest") or ""),
                            str(finalized.get("rationale") or ""),
                            str(finalized.get("_source_snip") or ""),
                        ]
                    )
                )
                should_add = False
                if is_pbi_profile:
                    should_add = (
                        metric_ref == "Strategic milestone"
                        and "Strategic milestone" not in existing_metric_names
                        and re.search(r"\bstrategic review\b", note_blob, re.I)
                        and (
                            re.search(r"\b(q[1-4]\s*20\d{2}|q[1-4]|20\d{2}|on track|by end of)\b", note_blob, re.I)
                            or (
                                re.search(r"\b(?:phase 2|second phase)\b", note_blob, re.I)
                                and re.search(r"\binitiated\b", note_blob, re.I)
                            )
                        )
                    )
                elif is_gpre_profile:
                    should_add = (
                        metric_ref == "Interest expense outlook"
                        and "Interest expense outlook" not in existing_metric_names
                        and re.search(r"\binterest expense\b", note_blob, re.I)
                        and re.search(r"\b(expected|annualized|2026)\b", note_blob, re.I)
                    ) or (
                        metric_ref == "Capex guidance (FY 2026)"
                        and "Capex guidance (FY 2026)" not in existing_metric_names
                        and re.search(r"\b(capex|capital expenditures?|sustaining capital)\b", note_blob, re.I)
                        and re.search(r"\b(2026|expected|guidance|outlook)\b", note_blob, re.I)
                        and str(finalized.get("target") or "").strip() != ""
                    ) or (
                        metric_ref == "45Z monetization / EBITDA"
                        and "45Z monetization / EBITDA" not in existing_metric_names
                        and str(finalized.get("target") or "").strip() != ""
                        and re.search(r"\b45z\b", note_blob, re.I)
                        and re.search(r"\b(expected|outlook)\b", note_blob, re.I)
                    )
                if not should_add:
                    continue
                rows_in.append(finalized)
                existing_progress_keys.add(metric_key)
                existing_metric_names.add(metric_ref or metric_label)
                if metric_label:
                    existing_metric_names.add(metric_label)
                targeted_added += 1
            if targeted_added > 0:
                ui_info_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Promise_Progress_UI",
                        "severity": "info",
                        "message": f"targeted_qnote_progress_added count={int(targeted_added)}",
                        "source": "Quarter_Notes_UI",
                    }
                )
            return rows_in

        finalized_rows = _append_targeted_qnote_progress_rows(finalized_rows)
        selected_rows = finalized_rows
        selected_rows = _collapse_progress_rows_for_display(selected_rows)
        if is_gpre_profile:
            selected_rows = _gpre_trim_final_progress_rows(selected_rows)
            existing_metric_names = {
                str(name).strip()
                for row in selected_rows
                if str(row.get("row_type") or "").strip().lower() not in {"section", "blank"}
                for name in [
                    row.get("metric_ref"),
                    row.get("metric_display"),
                    row.get("metric"),
                ]
                if str(name or "").strip()
            }
            gpre_targeted_rows: List[Dict[str, Any]] = []
            for note_item in _quarter_note_seed_rows_for_qd(qd):
                note_txt = glx_normalize_text(
                    str(
                        note_item.get("_render_summary")
                        or note_item.get("text_full")
                        or note_item.get("comment_full_text")
                        or note_item.get("text")
                        or note_item.get("comment")
                        or note_item.get("rationale")
                        or ""
                    )
                )
                if not note_txt:
                    continue
                src = dict(note_item.get("source") or {})
                if (
                    "Interest expense outlook" not in existing_metric_names
                    and re.search(r"\binterest expense\b", note_txt, re.I)
                    and re.search(r"\b(expected|annualized|2026)\b", note_txt, re.I)
                ):
                    interest_hits = _extract_money_targets_for_display(note_txt)
                    interest_target = ""
                    if len(interest_hits) >= 2:
                        lo = min(float(interest_hits[0]), float(interest_hits[1]))
                        hi = max(float(interest_hits[0]), float(interest_hits[1]))
                        interest_target = f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
                    gpre_targeted_rows.append(
                        {
                            "promise_id": str(note_item.get("note_id") or hashlib.sha1(f"{qd}|gpre_interest|{note_txt}".encode("utf-8")).hexdigest()[:12]),
                            "metric_ref": "Interest expense outlook",
                            "metric_display": "Interest expense outlook",
                            "target": interest_target,
                            "latest": "not yet measurable",
                            "status": "pending",
                            "rationale": note_txt,
                            "promise_type": "operational",
                            "guidance_type": "period",
                            "first_seen_quarter_end": str(qd),
                            "last_seen_quarter_end": str(qd),
                            "first_seen_evidence_quarter_end": str(qd),
                            "last_seen_evidence_quarter_end": str(qd),
                            "last_seen_text_quarter_end": str(qd),
                            "carried_to_quarter_end": str(qd),
                            "evaluated_through": str(qd),
                            "_source_snip": note_txt,
                            "_source_doc": str(src.get("doc") or note_item.get("doc") or "Quarter_Notes_UI"),
                            "_source_type": str(src.get("source_type") or note_item.get("doc_type") or "quarter_notes_ui"),
                            "_score": float(note_item.get("score") or 0.0),
                            "_fragment_penalty": _text_fragment_penalty(note_txt),
                            "_clean_target_bonus": _clean_target_bonus(note_txt),
                            "statement_summary": note_txt,
                        }
                    )
                    existing_metric_names.add("Interest expense outlook")
                if (
                    "Capex guidance (FY 2026)" not in existing_metric_names
                    and re.search(r"\b(capex|capital expenditures?|sustaining capital)\b", note_txt, re.I)
                    and re.search(r"\b(2026|expected|guidance|outlook)\b", note_txt, re.I)
                ):
                    capex_hits = _extract_money_targets_for_display(note_txt)
                    capex_target = ""
                    if len(capex_hits) >= 2:
                        lo = min(float(capex_hits[0]), float(capex_hits[1]))
                        hi = max(float(capex_hits[0]), float(capex_hits[1]))
                        capex_target = f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
                    elif capex_hits:
                        capex_target = _fmt_short_money_value_local(float(max(capex_hits)))
                    if capex_target:
                        gpre_targeted_rows.append(
                            {
                                "promise_id": str(note_item.get("note_id") or hashlib.sha1(f"{qd}|gpre_capex|{note_txt}".encode("utf-8")).hexdigest()[:12]),
                                "metric_ref": "Capex guidance (FY 2026)",
                                "metric_display": "Capex guidance (FY 2026)",
                                "target": capex_target,
                                "latest": "not yet measurable",
                                "status": "pending",
                                "rationale": note_txt,
                                "promise_type": "operational",
                                "guidance_type": "period",
                                "first_seen_quarter_end": str(qd),
                                "last_seen_quarter_end": str(qd),
                                "first_seen_evidence_quarter_end": str(qd),
                                "last_seen_evidence_quarter_end": str(qd),
                                "last_seen_text_quarter_end": str(qd),
                                "carried_to_quarter_end": str(qd),
                                "evaluated_through": str(qd),
                                "_source_snip": note_txt,
                                "_source_doc": str(src.get("doc") or note_item.get("doc") or "Quarter_Notes_UI"),
                                "_source_type": str(src.get("source_type") or note_item.get("doc_type") or "quarter_notes_ui"),
                                "_score": float(note_item.get("score") or 0.0),
                                "_fragment_penalty": _text_fragment_penalty(note_txt),
                                "_clean_target_bonus": _clean_target_bonus(note_txt),
                                "statement_summary": note_txt,
                            }
                        )
                        existing_metric_names.add("Capex guidance (FY 2026)")
                if (
                    "45Z monetization / EBITDA" not in existing_metric_names
                    and re.search(r"\b45z\b", note_txt, re.I)
                    and re.search(r"\b(expected|outlook)\b", note_txt, re.I)
                    and str(_extract_45z_monetization_target_display(note_txt, qd, "") or "").strip()
                ):
                    gpre_targeted_rows.append(
                        {
                            "promise_id": str(note_item.get("note_id") or hashlib.sha1(f"{qd}|gpre_45z_monet|{note_txt}".encode("utf-8")).hexdigest()[:12]),
                            "metric_ref": "45Z monetization / EBITDA",
                            "metric_display": "45Z monetization / EBITDA",
                            "target": str(_extract_45z_monetization_target_display(note_txt, qd, "") or ""),
                            "latest": "not yet measurable",
                            "status": "pending",
                            "rationale": note_txt,
                            "promise_type": "operational",
                            "guidance_type": "period",
                            "first_seen_quarter_end": str(qd),
                            "last_seen_quarter_end": str(qd),
                            "first_seen_evidence_quarter_end": str(qd),
                            "last_seen_evidence_quarter_end": str(qd),
                            "last_seen_text_quarter_end": str(qd),
                            "carried_to_quarter_end": str(qd),
                            "evaluated_through": str(qd),
                            "_source_snip": note_txt,
                            "_source_doc": str(src.get("doc") or note_item.get("doc") or "Quarter_Notes_UI"),
                            "_source_type": str(src.get("source_type") or note_item.get("doc_type") or "quarter_notes_ui"),
                            "_score": float(note_item.get("score") or 0.0),
                            "_fragment_penalty": _text_fragment_penalty(note_txt),
                            "_clean_target_bonus": _clean_target_bonus(note_txt),
                            "statement_summary": note_txt,
                        }
                    )
                    existing_metric_names.add("45Z monetization / EBITDA")
            if gpre_targeted_rows:
                selected_rows = _gpre_trim_final_progress_rows(
                    _collapse_progress_rows_for_display(selected_rows + gpre_targeted_rows)
                )
        selected_rows = [
            item for item in selected_rows
            if not re.search(
                r"^\s*(which time|portion of|at which time|for which|that will|who will|where the|the partnership|the merger|the transactions?)\b",
                str(item.get("metric_display") or item.get("metric_ref") or item.get("metric") or ""),
                re.I,
            )
        ]
        g_rows = _build_guidance_accuracy_rows(qd)
        if g_rows:
            existing_by_lifecycle: Dict[str, Dict[str, Any]] = {}
            for row in selected_rows:
                lifecycle_key = str(row.get("lifecycle_subject_key") or row.get("promise_lifecycle_key") or "").strip()
                if lifecycle_key:
                    existing_by_lifecycle[lifecycle_key] = row
            for g_row in g_rows:
                g_row["route_reason"] = str(g_row.get("route_reason") or "promise_progress")
                g_row["candidate_type"] = str(g_row.get("candidate_type") or "follow_through_event")
                g_row["evidence_role"] = str(g_row.get("evidence_role") or "result_evidence")
                lifecycle_key = str(g_row.get("lifecycle_subject_key") or g_row.get("promise_lifecycle_key") or "").strip()
                existing = existing_by_lifecycle.get(lifecycle_key) if lifecycle_key else None
                if existing is not None:
                    existing_status_rank = shared_progress_status_rank(existing.get("status"))
                    g_status_rank = shared_progress_status_rank(g_row.get("status"))
                    existing_latest = str(existing.get("latest") or "").strip()
                    g_latest = str(g_row.get("latest") or "").strip()
                    existing_has_actual = bool(existing_latest) and existing_latest.lower() != "not yet measurable"
                    g_has_actual = bool(g_latest) and g_latest.lower() != "not yet measurable"
                    prefer_guidance_eval = (
                        g_status_rank > existing_status_rank
                        or (g_has_actual and not existing_has_actual)
                    )
                    if prefer_guidance_eval:
                        for field_name in ("status", "latest", "rationale", "target", "evaluated_through", "evaluated_through_quarter", "latest_evidence_quarter"):
                            if g_row.get(field_name) not in {None, ""}:
                                existing[field_name] = g_row.get(field_name)
                        existing["collapse_reason"] = "same_subject_same_block"
                        existing["conflict_resolution_reason"] = (
                            "status_precedence" if g_status_rank != existing_status_rank else "actual_over_text_progress"
                        )
                        existing["status_resolution_reason"] = str(
                            existing.get("status_resolution_reason")
                            or shared_derive_status_resolution_reason(
                                current_status=existing.get("status") or "",
                                latest_value=existing.get("latest") or "",
                                lifecycle_state=existing.get("lifecycle_state") or "",
                            )
                        )
                    continue
                selected_rows.append(g_row)
            selected_rows = _collapse_progress_rows_for_display(selected_rows)
        rows_by_quarter[qd] = selected_rows

    for qd_key, q_rows in list(rows_by_quarter.items()):
        cleaned_rows: List[Dict[str, Any]] = []
        for item in q_rows or []:
            metric_txt = str(item.get("metric_ref") or item.get("metric") or "").strip()
            if re.search(
                r"^\s*(which time|portion of|at which time|for which|that will|who will|where the|the partnership|the merger|the transactions?)\b",
                metric_txt,
                re.I,
            ):
                continue
            cleaned_rows.append(item)
        rows_by_quarter[qd_key] = cleaned_rows

    return PromiseProgressSelectionResult(
        rows_by_quarter=rows_by_quarter,
        qa_rows=qa_rows,
        ui_info_rows=ui_info_rows,
        milestone_suppressed_count=milestone_suppressed_count,
        progress_select_started=progress_select_started,
        collapse_progress_rows_for_display=_collapse_progress_rows_for_display,
        promise_progress_keep_item=_promise_progress_keep_item,
        build_tracker_progress_row=_build_tracker_progress_row,
        quarter_note_seed_rows_for_qd=_quarter_note_seed_rows_for_qd,
    )
