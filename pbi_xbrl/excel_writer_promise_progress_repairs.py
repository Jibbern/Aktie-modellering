"""Promise Progress pre-render visible row repair helpers."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .filing_evidence_shared import (
    progress_status_rank as shared_progress_status_rank,
    promise_candidate_drop_reason as shared_promise_candidate_drop_reason,
    qualify_promise_candidate as shared_qualify_promise_candidate,
)
from .guidance_lexicon import normalize_text as glx_normalize_text
from .quarter_notes_lexicon import compact_snippet as qn_compact_snippet


@dataclass(frozen=True)
class GpreProgressTrimDeps:
    is_gpre_profile: bool
    visible_category_rank: Callable[..., int]
    gpre_clean_visible_promise_metric: Callable[..., str]
    gpre_bad_visible_promise_reason: Callable[..., bool]


@dataclass(frozen=True)
class PromiseProgressVisibleRepairDeps:
    is_pbi_profile: bool
    is_gpre_profile: bool
    quarters: Sequence[date]
    evaluation_as_of: Optional[date]
    promises: Any
    tracker_rows_map: Mapping[Any, List[Dict[str, Any]]]
    quarter_note_rows_map: Mapping[Any, List[Dict[str, Any]]]
    quarter_notes: Any
    qend: Callable[..., Any]
    q_label: Callable[..., str]
    parse_dollar_amount: Callable[..., Any]
    text_fragment_penalty: Callable[..., Any]
    clean_target_bonus: Callable[..., Any]
    collapse_progress_rows_for_display: Callable[..., List[Dict[str, Any]]]
    promise_progress_keep_item: Callable[..., bool]
    build_tracker_progress_row: Callable[..., Optional[Dict[str, Any]]]
    quarter_note_seed_rows_for_qd: Callable[..., List[Dict[str, Any]]]
    dedupe_promise_progress_rows: Callable[..., int]
    dedupe_display_progress_rows: Callable[..., int]
    latest_visible_quarter_notes_from_sheet: Callable[[date], List[Dict[str, Any]]]
    display_progress_metric: Callable[..., str]
    progress_visible_category_rank: Callable[..., int]
    classify_pbi_metric_label: Callable[..., str]
    extract_pbi_target_display: Callable[..., str]
    pbi_target_display_ok: Callable[..., bool]
    looks_pbi_fragment_text: Callable[..., bool]
    is_pbi_clean_sentence: Callable[..., bool]
    lookup_pbi_structured_progress_hint: Callable[..., Any]
    lookup_pbi_structured_guidance_target: Callable[..., Any]
    pbi_structured_strategy_items_for_qd: Callable[..., List[Dict[str, Any]]]
    pbi_guidance_period_label_from_text: Callable[..., str]
    pbi_repair_guidance_period_meta: Callable[..., Tuple[str, str]]
    guidance_period_end: Callable[..., Any]
    actual_for_guidance: Callable[..., Any]
    infer_target_numeric_spec: Callable[..., Dict[str, Any]]
    progress_target_display_from_qnote: Callable[..., str]
    extract_progress_latest_basis: Callable[..., str]
    append_follow_rationale: Callable[..., str]
    ensure_terminal_period: Callable[..., str]
    fmt_short_money_value_local: Callable[..., str]
    extract_45z_monetization_target_display: Callable[..., str]
    extract_money_targets_for_display: Callable[..., Any]
    gpre_clean_visible_promise_metric: Callable[..., str]
    gpre_bad_visible_promise_reason: Callable[..., bool]
    gpre_trim_final_progress_rows: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]
    resolve_col: Callable[..., Any]


@dataclass(frozen=True)
class PromiseProgressVisibleRepairResult:
    rows_by_quarter: Dict[date, List[Dict[str, Any]]]
    ui_info_rows: List[Dict[str, Any]]
    pbi_apply_guidance_outcome: Callable[..., Dict[str, Any]]


def trim_gpre_final_progress_rows(deps: GpreProgressTrimDeps, items_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not deps.is_gpre_profile:
        return items_in
    sections = [x for x in items_in if str(x.get("row_type") or "").strip().lower() in {"section", "blank"}]
    rows = [dict(x) for x in items_in if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}]
    metric_caps = {
        "45Z-related Adjusted EBITDA": 1,
        "45Z monetization / EBITDA": 1,
        "45Z monetization": 1,
        "45Z monetization outlook": 1,
        "45Z from remaining facilities": 1,
        "45Z facility qualification": 1,
        "45Z plant qualification readiness": 1,
        "Advantage Nebraska EBITDA opportunity": 1,
        "Advantage Nebraska startup": 1,
        "Capex guidance (FY 2026)": 1,
        "Cost savings target": 1,
        "Debt reduction": 1,
        "Interest expense outlook": 1,
    }
    metric_order = {
        "45Z monetization outlook": 0,
        "Interest expense outlook": 1,
        "45Z-related Adjusted EBITDA": 2,
        "Capex guidance (FY 2026)": 3,
        "45Z monetization": 4,
        "45Z from remaining facilities": 5,
        "45Z facility qualification": 6,
        "Advantage Nebraska startup": 7,
        "Advantage Nebraska EBITDA opportunity": 8,
        "Cost savings target": 9,
        "Debt reduction": 10,
        "45Z plant qualification readiness": 11,
    }
    for row in rows:
        metric_label = deps.gpre_clean_visible_promise_metric(
            str(row.get("metric_display") or row.get("metric_ref") or row.get("metric") or ""),
            " | ".join(
                [
                    str(row.get("rationale") or ""),
                    str(row.get("_source_snip") or ""),
                    str(row.get("latest") or ""),
                    str(row.get("target") or ""),
                ]
            ),
            row,
        )
        if metric_label:
            row["metric_display"] = metric_label
    rows = [
        row for row in rows
        if not deps.gpre_bad_visible_promise_reason(
            row.get("metric_display") or row.get("metric_ref"),
            row.get("rationale"),
            row.get("latest"),
            row.get("target"),
        )
    ]
    rows = sorted(
        rows,
        key=lambda z: (
            deps.visible_category_rank(z),
            metric_order.get(str(z.get("metric_display") or z.get("metric_ref") or "").strip(), 99),
            -shared_progress_status_rank(z.get("status")),
            -int(str(z.get("latest") or "").strip().lower() not in {"", "not yet measurable"}),
            int(z.get("_fragment_penalty") or 0),
            -int(z.get("_clean_target_bonus") or 0),
            -float(z.get("_score") or z.get("confidence_score") or 0.0),
            str(z.get("metric_display") or z.get("metric_ref") or "").lower(),
        ),
    )
    kept_rows: List[Dict[str, Any]] = []
    metric_counts: Dict[str, int] = {}
    for row in rows:
        label = str(row.get("metric_display") or row.get("metric_ref") or "").strip()
        if not label:
            continue
        cap = metric_caps.get(label, 1)
        used = metric_counts.get(label, 0)
        if used >= cap:
            continue
        metric_counts[label] = used + 1
        kept_rows.append(row)
    return sections + kept_rows


def repair_promise_progress_visible_rows_for_render(
    deps: PromiseProgressVisibleRepairDeps,
    rows_by_quarter: Dict[date, List[Dict[str, Any]]],
) -> PromiseProgressVisibleRepairResult:
    is_pbi_profile = deps.is_pbi_profile
    is_gpre_profile = deps.is_gpre_profile
    quarters = list(deps.quarters)
    evaluation_as_of = deps.evaluation_as_of
    promises = deps.promises
    tracker_rows_map = deps.tracker_rows_map
    quarter_note_rows_map = deps.quarter_note_rows_map
    quarter_notes = deps.quarter_notes
    ui_info_rows: List[Dict[str, Any]] = []
    _qend = deps.qend
    _q_label = deps.q_label
    _parse_dollar_amount = deps.parse_dollar_amount
    _text_fragment_penalty = deps.text_fragment_penalty
    _clean_target_bonus = deps.clean_target_bonus
    _collapse_progress_rows_for_display = deps.collapse_progress_rows_for_display
    _promise_progress_keep_item = deps.promise_progress_keep_item
    _build_tracker_progress_row = deps.build_tracker_progress_row
    _quarter_note_seed_rows_for_qd = deps.quarter_note_seed_rows_for_qd
    _dedupe_promise_progress_rows = deps.dedupe_promise_progress_rows
    _dedupe_display_progress_rows = deps.dedupe_display_progress_rows
    _display_progress_metric = deps.display_progress_metric
    _promise_progress_visible_category_rank_local = deps.progress_visible_category_rank
    _classify_pbi_metric_label = deps.classify_pbi_metric_label
    _extract_pbi_target_display = deps.extract_pbi_target_display
    _pbi_target_display_ok = deps.pbi_target_display_ok
    _looks_pbi_fragment_text = deps.looks_pbi_fragment_text
    _is_pbi_clean_sentence = deps.is_pbi_clean_sentence
    _lookup_pbi_structured_progress_hint = deps.lookup_pbi_structured_progress_hint
    _lookup_pbi_structured_guidance_target = deps.lookup_pbi_structured_guidance_target
    _pbi_structured_strategy_items_for_qd = deps.pbi_structured_strategy_items_for_qd
    _pbi_guidance_period_label_from_text = deps.pbi_guidance_period_label_from_text
    _pbi_repair_guidance_period_meta = deps.pbi_repair_guidance_period_meta
    _guidance_period_end = deps.guidance_period_end
    _actual_for_guidance = deps.actual_for_guidance
    _infer_target_numeric_spec = deps.infer_target_numeric_spec
    _progress_target_display_from_qnote = deps.progress_target_display_from_qnote
    _extract_progress_latest_basis = deps.extract_progress_latest_basis
    _append_follow_rationale = deps.append_follow_rationale
    _ensure_terminal_period = deps.ensure_terminal_period
    _fmt_short_money_value_local = deps.fmt_short_money_value_local
    _extract_45z_monetization_target_display = deps.extract_45z_monetization_target_display
    _extract_money_targets_for_display = deps.extract_money_targets_for_display
    _gpre_clean_visible_promise_metric = deps.gpre_clean_visible_promise_metric
    _gpre_bad_visible_promise_reason = deps.gpre_bad_visible_promise_reason
    _resolve_col = deps.resolve_col

    _pbi_progress_allowed_labels = {
        "Adjusted EBIT guidance",
        "Revenue guidance",
        "EPS guidance",
        "FCF target",
        "Cost savings target",
        "Cost savings program",
        "Cost savings tranche 1",
        "Cost savings tranche 2",
        "Deleveraging target",
        "PB Bank liquidity release",
        "SendTech / Presort operating target",
        "Strategic milestone",
    }

    def _pbi_progress_guidance_metric_name(metric_blob: Any) -> str:
        blob = str(metric_blob or "")
        if re.search(r"\brevenue\b", blob, re.I):
            return "Revenue"
        if re.search(r"\badjusted?\s+ebit\b|\badj\.?\s*ebit\b", blob, re.I):
            return "Adj EBIT"
        if re.search(r"\beps\b", blob, re.I):
            return "Adj EPS"
        if re.search(r"\bfcf\b|free cash flow", blob, re.I):
            return "FCF"
        return ""

    def _pbi_progress_metric_family(label_in: Any) -> str:
        label_low = str(label_in or "").strip().lower()
        if not label_low:
            return ""
        if "revenue" in label_low:
            return "revenue"
        if "adjusted ebit" in label_low or "adj ebit" in label_low:
            return "adj_ebit"
        if "eps" in label_low:
            return "eps"
        if "fcf" in label_low or "free cash flow" in label_low:
            return "fcf"
        if "cost savings" in label_low:
            return "cost_savings"
        if "pb bank" in label_low or "bank-held leases" in label_low or "cash optimization" in label_low:
            return "pb_bank"
        if "deleverag" in label_low or "debt reduction" in label_low or "liquidity" in label_low:
            return "deleveraging"
        if "sendtech" in label_low or "presort" in label_low:
            return "segment_ops"
        if "strategic milestone" in label_low:
            return "milestone"
        return label_low

    def _recover_later_pbi_progress_basis(item: Dict[str, Any], metric_display: str, rationale_txt: str, target_txt: str) -> Any:
        qd_item = item.get("quarter")
        if not isinstance(qd_item, date):
            return None
        future_quarters = [q for q in quarters if isinstance(q, date) and q > qd_item]
        if not future_quarters:
            return None
        metric_blob = " | ".join(
            [
                str(metric_display or ""),
                str(item.get("metric_ref") or ""),
                target_txt,
                rationale_txt,
                str(item.get("_source_snip") or ""),
            ]
        )
        period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
        guidance_metric = _pbi_progress_guidance_metric_name(metric_blob)
        metric_family = _pbi_progress_metric_family(metric_display)
        if guidance_metric and period_norm not in {"", "UNK"}:
            for later_q in future_quarters:
                actual_val = _actual_for_guidance(guidance_metric, period_norm, later_q)
                if actual_val is not None:
                    return float(actual_val)
        for later_q in future_quarters:
            structured_progress = _lookup_pbi_structured_progress_hint(later_q, metric_display, metric_blob)
            if structured_progress:
                latest_display = str(structured_progress.get("latest_display") or "").strip()
                if latest_display and not _looks_pbi_fragment_text(latest_display):
                    return latest_display
                if metric_family in {"cost_savings", "pb_bank", "deleveraging"}:
                    target_display = str(structured_progress.get("target_display") or "").strip()
                    if target_display and not _looks_pbi_fragment_text(target_display):
                        return target_display
            if metric_family in {"cost_savings", "pb_bank", "deleveraging"}:
                structured_guidance = _lookup_pbi_structured_guidance_target(later_q, metric_display, metric_blob)
                if structured_guidance:
                    later_target = str(structured_guidance.get("target_display") or "").strip()
                    if later_target and not _looks_pbi_fragment_text(later_target):
                        return later_target
            for tracker_row in tracker_rows_map.get(later_q, []) or []:
                tracker_blob = " | ".join(
                    [
                        str(tracker_row.get("metric_display") or tracker_row.get("metric") or ""),
                        str(tracker_row.get("target_display") or ""),
                        str(tracker_row.get("latest_display") or ""),
                        str(tracker_row.get("text_full") or tracker_row.get("text_snippet") or ""),
                    ]
                )
                tracker_label = _classify_pbi_metric_label(
                    tracker_blob,
                    str(tracker_row.get("metric_display") or tracker_row.get("metric") or ""),
                )
                tracker_family = _pbi_progress_metric_family(tracker_label or tracker_row.get("metric_display") or tracker_row.get("metric") or "")
                if tracker_label != metric_display and tracker_family != metric_family:
                    continue
                latest_display = str(tracker_row.get("latest_display") or "").strip()
                if not latest_display:
                    latest_display = _extract_progress_latest_basis(tracker_label or metric_display, tracker_blob)
                if not latest_display and metric_family in {"cost_savings", "pb_bank", "deleveraging"}:
                    latest_display = str(tracker_row.get("target_display") or "").strip()
                if latest_display and not _looks_pbi_fragment_text(latest_display):
                    return latest_display
            for note_row in quarter_note_rows_map.get(later_q, []) or []:
                note_blob = " | ".join(
                    [
                        str(note_row.get("_metric_display") or note_row.get("metric_ref") or ""),
                        str(note_row.get("_render_summary") or note_row.get("text_full") or ""),
                        str(note_row.get("text_full") or ""),
                    ]
                )
                note_label = _classify_pbi_metric_label(
                    note_blob,
                    str(note_row.get("_metric_display") or note_row.get("metric_ref") or ""),
                )
                note_family = _pbi_progress_metric_family(note_label or note_row.get("_metric_display") or note_row.get("metric_ref") or "")
                if note_label != metric_display and note_family != metric_family:
                    continue
                note_latest = _extract_progress_latest_basis(note_label or metric_display, note_blob)
                if not note_latest and metric_family in {"cost_savings", "pb_bank", "deleveraging"}:
                    note_latest = str(note_row.get("_render_summary") or "").strip()
                if note_latest and not _looks_pbi_fragment_text(note_latest):
                    return note_latest
            if isinstance(quarter_notes, pd.DataFrame) and not quarter_notes.empty:
                raw_q_col = _resolve_col(quarter_notes, ["quarter", "created_quarter", "first_seen_quarter"])
                raw_note_col = _resolve_col(quarter_notes, ["note", "claim", "evidence_snippet"])
                raw_metric_col = _resolve_col(quarter_notes, ["metric_ref", "metric", "metric_tag"])
                if raw_q_col and raw_note_col:
                    raw_slice = quarter_notes[
                        pd.to_datetime(quarter_notes.get(raw_q_col), errors="coerce").dt.date == later_q
                    ]
                    for raw_rec in raw_slice.to_dict("records"):
                        raw_text = glx_normalize_text(str(raw_rec.get(raw_note_col) or ""))
                        if not raw_text:
                            continue
                        raw_metric = str(raw_rec.get(raw_metric_col) or "").strip() if raw_metric_col else ""
                        raw_label = _classify_pbi_metric_label(" | ".join([raw_metric, raw_text]), raw_metric)
                        raw_family = _pbi_progress_metric_family(raw_label or raw_metric)
                        if raw_label != metric_display and raw_family != metric_family:
                            continue
                        raw_latest = _extract_progress_latest_basis(raw_label or metric_display, raw_text)
                        if not raw_latest and metric_family in {"cost_savings", "pb_bank", "deleveraging"}:
                            raw_latest = _extract_pbi_target_display(raw_text, raw_label or raw_metric or metric_display) or raw_text
                        if raw_latest and not _looks_pbi_fragment_text(str(raw_latest)):
                            return raw_latest
        return None

    def _pbi_final_progress_keep_item(item: Dict[str, Any]) -> bool:
        if not is_pbi_profile:
            return True
        row_type = str(item.get("row_type") or "").strip().lower()
        metric_display = _display_progress_metric(item)
        qualified_promise = None

        def _pbi_progress_label_alignment_ok(label_in: str, blob_in: str) -> bool:
            label_txt = str(label_in or "").strip()
            blob_txt = glx_normalize_text(str(blob_in or ""))
            if not label_txt or not blob_txt:
                return False
            alignment_patterns = {
                "Adjusted EBIT guidance": r"\b(adjusted ebit|adj\.?\s*ebit|ebit|margin|profitabilit)\b",
                "Revenue guidance": r"\b(revenue|sales|volume|mail|shipping)\b",
                "EPS guidance": r"\b(eps|earnings per share)\b",
                "FCF target": r"\b(fcf|free cash flow|cash flow)\b",
                "Cost savings target": r"\b(cost savings|cost reduction|annualized savings|run-rate|rationalization)\b",
                "Deleveraging target": r"\b(deleverag|debt|leverage|liquidity|repay|repayment|paydown)\b",
                "PB Bank liquidity release": r"\b(pb bank|bank-held leases|leases held|cash optimization|cash needs reduction|receivables purchase|liquidity|cash release|trapped capital)\b",
                "SendTech / Presort operating target": r"\b(sendtech|presort)\b",
            }
            required_pattern = alignment_patterns.get(label_txt)
            if required_pattern and not re.search(required_pattern, blob_txt, re.I):
                return False
            better = _classify_pbi_metric_label(blob_txt, "")
            if not better:
                return True
            if better == label_txt:
                return True
            return _pbi_progress_metric_family(better) == _pbi_progress_metric_family(label_txt)

        if row_type not in {"section", "blank"}:
            qual_text = glx_normalize_text(
                " | ".join(
                    [
                        str(item.get("rationale") or ""),
                        str(item.get("target") or ""),
                        str(item.get("metric_ref") or item.get("metric_display") or ""),
                    ]
                )
            )
            quality_drop_reason = shared_promise_candidate_drop_reason(
                qual_text,
                source_type=str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui"),
                metric_hint=" | ".join([
                    str(item.get("metric_ref") or item.get("metric_display") or ""),
                    str(item.get("target") or ""),
                ]),
            )
            if quality_drop_reason:
                item["quality_drop_reason"] = quality_drop_reason
                return False
            qualified_promise = shared_qualify_promise_candidate(
                qual_text,
                source_type=str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui"),
                metric_hint=" | ".join([
                    str(item.get("metric_ref") or item.get("metric_display") or ""),
                    str(item.get("target") or ""),
                ]),
            )
            if qualified_promise is None:
                relaxed_pbi_keep = (
                    metric_display in {
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
                                str(item.get("rationale") or ""),
                                str(item.get("target") or ""),
                                str(item.get("latest") or ""),
                            ]
                        )
                    )
                    and (
                        _pbi_target_display_ok(str(item.get("target") or ""))
                        or bool(re.search(r"\b(fy\s*20\d{2}|20\d{2}|q[1-4]|quarter|full[- ]?year)\b", qual_text, re.I))
                        or metric_display in {"PB Bank liquidity release", "Cost savings target", "Deleveraging target"}
                    )
                )
                if relaxed_pbi_keep:
                    item.setdefault("statement_summary", glx_normalize_text(str(item.get("rationale") or "")))
                    item.setdefault(
                        "candidate_scope",
                        "milestone" if metric_display == "Strategic milestone" else "operational",
                    )
                else:
                    item["quality_drop_reason"] = "not_investor_relevant"
                    return False
        if qualified_promise is not None:
            item.setdefault("statement_summary", qualified_promise.summary)
            item.setdefault("candidate_scope", qualified_promise.scope)
        if row_type in {"section", "blank"}:
            return True
        target_txt = str(item.get("target") or "").strip()
        latest_txt = glx_normalize_text(str(item.get("latest") or ""))
        latest_num = pd.to_numeric(item.get("latest"), errors="coerce")
        rationale_txt = glx_normalize_text(str(item.get("rationale") or ""))
        status_low = str(item.get("status") or "").strip().lower()
        if metric_display not in _pbi_progress_allowed_labels and not metric_display.endswith("guidance"):
            item["quality_drop_reason"] = "not_investor_relevant"
            return False
        alignment_blob = " | ".join(
            [
                target_txt,
                latest_txt,
                rationale_txt,
                str(item.get("_source_snip") or ""),
            ]
        )
        strategic_review_milestone = bool(
            metric_display == "Strategic milestone"
            and re.search(r"\bstrategic review\b", alignment_blob, re.I)
        )
        if not strategic_review_milestone and not _pbi_progress_label_alignment_ok(metric_display, alignment_blob):
            item["quality_drop_reason"] = "not_investor_relevant"
            return False
        qd_item = item.get("quarter")
        structured_guidance = _lookup_pbi_structured_guidance_target(
            qd_item if isinstance(qd_item, date) else None,
            metric_display,
            " | ".join([metric_display, target_txt, rationale_txt, str(item.get("_source_snip") or "")]),
        )
        target_ok = _pbi_target_display_ok(target_txt)
        structured_progress = _lookup_pbi_structured_progress_hint(
            qd_item if isinstance(qd_item, date) else None,
            metric_display,
            " | ".join([metric_display, target_txt, rationale_txt, str(item.get("_source_snip") or "")]),
        )
        if not target_ok and metric_display != "Strategic milestone":
            repaired_target = (
                _extract_pbi_target_display(
                    " | ".join([rationale_txt, str(item.get("_source_snip") or ""), metric_display]),
                    metric_display,
                )
                or str((structured_guidance or {}).get("target_display") or "").strip()
                or str((structured_progress or {}).get("target_display") or "").strip()
            )
            if _pbi_target_display_ok(repaired_target):
                item["target"] = repaired_target
                target_txt = repaired_target
                target_ok = True
        if latest_txt.lower() in {"", "not yet measurable"}:
            repaired_latest = str((structured_progress or {}).get("latest_display") or "").strip()
            if not repaired_latest:
                repaired_latest = _extract_progress_latest_basis(
                    metric_display,
                    " | ".join([rationale_txt, str(item.get("_source_snip") or "")]),
                )
            if not repaired_latest:
                recovered_later = _recover_later_pbi_progress_basis(item, metric_display, rationale_txt, target_txt)
                if recovered_later is not None:
                    repaired_latest = str(recovered_later)
            if repaired_latest and not _looks_pbi_fragment_text(repaired_latest):
                item["latest"] = repaired_latest
                latest_txt = glx_normalize_text(repaired_latest)
                latest_num = pd.to_numeric(repaired_latest, errors="coerce")
        if status_low in {"", "pending", "open", "not observed"}:
            structured_status = str((structured_progress or {}).get("status_hint") or "").strip().lower()
            if structured_status:
                item["status"] = structured_status
                status_low = structured_status
        if rationale_txt and _looks_pbi_fragment_text(rationale_txt) and not _is_pbi_clean_sentence(rationale_txt):
            item["quality_drop_reason"] = "fragmentary_text"
            return False
        if latest_txt and latest_txt.lower() != "not yet measurable" and pd.isna(latest_num) and _looks_pbi_fragment_text(latest_txt):
            if target_ok:
                item["latest"] = "not yet measurable"
                latest_txt = "not yet measurable"
            else:
                item["quality_drop_reason"] = "fragmentary_text"
                return False
        if row_type == "guidance":
            if metric_display not in {
                "Adjusted EBIT guidance",
                "Revenue guidance",
                "EPS guidance",
                "FCF target",
            }:
                item["quality_drop_reason"] = "not_investor_relevant"
                return False
            if str(target_txt or "").strip().lower() in {"", "none", "nan"}:
                item["quality_drop_reason"] = "no_clean_summary"
                return False
            return target_ok
        if metric_display in {
            "Adjusted EBIT guidance",
            "Revenue guidance",
            "EPS guidance",
            "FCF target",
            "Cost savings target",
            "Cost savings program",
            "Cost savings tranche 1",
            "Cost savings tranche 2",
            "Deleveraging target",
            "PB Bank liquidity release",
            "SendTech / Presort operating target",
        }:
            resolved_numeric = status_low in {"achieved", "resolved_pass", "resolved_beat", "resolved_fail", "broken", "missed"}
            if resolved_numeric and pd.isna(latest_num):
                item["quality_drop_reason"] = "no_clean_summary"
                return False
            if status_low in {"pending", "open", "on_track", "in progress", "not observed", ""} and not target_ok:
                item["quality_drop_reason"] = "no_clean_summary"
                return False
            if not (target_ok or latest_txt.lower() not in {"", "not yet measurable"}):
                item["quality_drop_reason"] = "no_clean_summary"
                return False
            if (
                status_low in {"pending", "open", "on_track", "in progress", "not observed", ""}
                and metric_display in {"Adjusted EBIT guidance", "Revenue guidance", "EPS guidance", "FCF target"}
                and not (
                    target_ok
                    and (
                        bool(str(item.get("target_period_norm") or "").strip())
                        or bool(str(item.get("target_period_label") or "").strip())
                        or bool(re.search(r"\b(fy\s*20\d{2}|q[1-4]\s*20\d{2}|20\d{2}|full[- ]?year)\b", rationale_txt, re.I))
                    )
                )
            ):
                item["quality_drop_reason"] = "no_clean_summary"
                return False
            if rationale_txt and not (_is_pbi_clean_sentence(rationale_txt) or len(rationale_txt) <= 160):
                fallback_rationale = f"{metric_display} target {target_txt}.".strip()
                item["rationale"] = fallback_rationale
            return True
        if metric_display == "Strategic milestone":
            return bool(
                (_is_pbi_clean_sentence(rationale_txt) or _is_pbi_clean_sentence(latest_txt))
                and re.search(
                    r"\b(20\d{2}|q[1-4]|full[- ]?year|completed|on track|repaid|released|achieved|agreement executed|fully operational|online and ramping)\b",
                    " | ".join([metric_display, target_txt, latest_txt, rationale_txt]),
                    re.I,
                )
            )
        item["quality_drop_reason"] = "not_investor_relevant"
        return False

    def _pbi_final_progress_display_key(item: Dict[str, Any]) -> str:
        metric_display = str(item.get("metric_display") or item.get("metric_ref") or "").strip()
        metric_low = metric_display.lower()
        period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
        promise_id = str(item.get("promise_id") or "").strip()
        promise_key = str(item.get("promise_key") or "").strip().lower()
        target_txt = glx_normalize_text(str(item.get("target") or "")).lower()
        rationale = " | ".join(
            [
                str(item.get("rationale") or ""),
                str(item.get("target") or ""),
                str(item.get("latest") or ""),
                str(item.get("_source_snip") or ""),
            ]
        )
        guidance_slug = ""
        if metric_low == "revenue guidance":
            guidance_slug = "revenue_guidance"
        elif metric_low == "adjusted ebit guidance":
            guidance_slug = "adjusted_ebit_guidance"
        elif metric_low == "eps guidance":
            guidance_slug = "eps_guidance"
        elif metric_low == "fcf target":
            guidance_slug = "fcf_target"
        if metric_low == "cost savings target" or "cost_savings" in promise_key or re.search(
            r"\b(cost savings|annualized savings|run-rate)\b",
            rationale,
            re.I,
        ):
            display_key = "guidance:cost_savings:ANNUALIZED_PROGRAM"
            item["promise_id"] = display_key
            item["canonical_subject_key"] = display_key
            item["promise_lifecycle_key"] = display_key
            item["lifecycle_subject_key"] = display_key
            item["target_period_norm"] = "ANNUALIZED_PROGRAM"
            return display_key
        if guidance_slug:
            period_hint = period_norm
            if not period_hint or period_hint == "UNK":
                target_period_label = str(item.get("target_period_label") or "").strip()
                stated_txt = " | ".join(
                    [
                        str(item.get("rationale") or ""),
                        str(item.get("target") or ""),
                        str(item.get("latest") or ""),
                        target_period_label,
                    ]
                )
                period_hint = _pbi_guidance_period_label_from_text(stated_txt).replace(" ", "")
                if period_hint.upper().startswith("FY"):
                    period_hint = period_hint.upper()
                elif re.match(r"Q[1-4]20\d{2}", period_hint.upper()):
                    period_hint = period_hint.upper()
            repaired_norm, repaired_label = _pbi_repair_guidance_period_meta(
                metric_display,
                period_hint,
                str(item.get("target_period_label") or "").strip(),
                " | ".join(
                    [
                        str(item.get("target") or ""),
                        str(item.get("latest") or ""),
                        str(item.get("rationale") or ""),
                    ]
                ),
                qd if isinstance(qd, date) else None,
            )
            if repaired_norm:
                period_hint = repaired_norm
                item["target_period_norm"] = repaired_norm
            if repaired_label:
                item["target_period_label"] = repaired_label
            guidance_target_key = re.sub(r"[^a-z0-9]+", "_", target_txt).strip("_")
            display_key = f"guidance:{guidance_slug}:{period_hint if period_hint and period_hint != 'UNK' else guidance_target_key or 'display'}"
            item["promise_id"] = f"guidance:{guidance_slug}"
            item["canonical_subject_key"] = display_key
            item["promise_lifecycle_key"] = display_key
            item["lifecycle_subject_key"] = display_key
            return display_key
        lifecycle_key = str(item.get("lifecycle_subject_key") or item.get("promise_lifecycle_key") or "").strip()
        if lifecycle_key:
            return lifecycle_key
        if promise_id:
            return promise_id
        return " | ".join([metric_low, period_norm, str(item.get("target") or "").strip().lower()])

    def _pbi_collapse_final_progress_rows(items_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sections = [x for x in items_in if str(x.get("row_type") or "").strip().lower() in {"section", "blank"}]
        rows = [dict(x) for x in items_in if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}]

        def _merge_progress_group(group_items: List[Dict[str, Any]]) -> Dict[str, Any]:
            def _pick_later_q(existing_val: Any, candidate_val: Any) -> str:
                existing_q = _qend(existing_val)
                candidate_q = _qend(candidate_val)
                if isinstance(existing_q, date) and isinstance(candidate_q, date):
                    return str(max(existing_q, candidate_q))
                if isinstance(candidate_q, date):
                    return str(candidate_q)
                if isinstance(existing_q, date):
                    return str(existing_q)
                return str(candidate_val or existing_val or "")

            best_item = sorted(
                group_items,
                key=lambda z: (
                    -shared_progress_status_rank(z.get("status")),
                    -int(str(z.get("latest") or "").strip().lower() not in {"", "not yet measurable"}),
                    -int(z.get("_source_rank") or 0),
                    -float(z.get("_score") or z.get("confidence_score") or 0.0),
                    str(z.get("target_period_norm") or z.get("period_norm") or ""),
                    str(z.get("promise_id") or ""),
                ),
            )[0]
            merged_local = dict(best_item)
            for other_item in group_items:
                if other_item is best_item:
                    continue
                other_status_rank = shared_progress_status_rank(other_item.get("status"))
                best_status_rank = shared_progress_status_rank(merged_local.get("status"))
                other_latest = str(other_item.get("latest") or "").strip()
                merged_latest = str(merged_local.get("latest") or "").strip()
                other_has_actual = other_latest.lower() not in {"", "not yet measurable"}
                merged_has_actual = merged_latest.lower() not in {"", "not yet measurable"}
                if other_status_rank > best_status_rank:
                    merged_local["status"] = other_item.get("status")
                    merged_local["status_resolution_reason"] = "status_precedence"
                if other_has_actual and not merged_has_actual:
                    merged_local["latest"] = other_item.get("latest")
                    merged_local["status_resolution_reason"] = str(
                        merged_local.get("status_resolution_reason") or "actual_over_text_progress"
                    )
                if len(str(other_item.get("rationale") or "")) > len(str(merged_local.get("rationale") or "")):
                    merged_local["rationale"] = other_item.get("rationale")
                if str(merged_local.get("metric_display") or "").strip() == "Cost savings target":
                    other_target = str(other_item.get("target") or "").strip()
                    merged_target = str(merged_local.get("target") or "").strip()
                    if other_target and (
                        not merged_target
                        or re.search(r"\b(cost savings|annualized savings|run-rate)\b", other_target, re.I)
                        or not re.search(r"\$\d", merged_target)
                    ):
                        merged_local["target"] = other_target
                elif len(str(other_item.get("target") or "")) > len(str(merged_local.get("target") or "")):
                    merged_local["target"] = other_item.get("target")
                if len(str(other_item.get("statement_summary") or "")) > len(str(merged_local.get("statement_summary") or "")):
                    merged_local["statement_summary"] = other_item.get("statement_summary")
                for q_field in (
                    "last_seen_quarter_end",
                    "last_seen_evidence_quarter_end",
                    "last_seen_numeric_quarter_end",
                    "last_seen_text_quarter_end",
                    "carried_to_quarter_end",
                    "evaluated_through",
                    "evaluated_through_quarter",
                ):
                    merged_local[q_field] = _pick_later_q(merged_local.get(q_field), other_item.get(q_field))
                if str(merged_local.get("metric_display") or "").strip() == "Cost savings target":
                    normalized_cost_key = "guidance:cost_savings:ANNUALIZED_PROGRAM"
                    merged_local["promise_id"] = normalized_cost_key
                    merged_local["canonical_subject_key"] = normalized_cost_key
                    merged_local["promise_lifecycle_key"] = normalized_cost_key
                    merged_local["lifecycle_subject_key"] = normalized_cost_key
                    target_txt = str(merged_local.get("target") or "").strip()
                    if target_txt:
                        merged_local["rationale"] = _ensure_terminal_period(
                            f"Annualized cost savings target {target_txt}"
                        )
                merged_local["collapse_reason"] = str(merged_local.get("collapse_reason") or "same_subject_same_block")
                merged_local["conflict_resolution_reason"] = str(
                    merged_local.get("conflict_resolution_reason") or "status_precedence"
                )
            return merged_local

        groups: Dict[str, List[Dict[str, Any]]] = {}
        for item in rows:
            groups.setdefault(_pbi_final_progress_display_key(item), []).append(item)
        collapsed_rows: List[Dict[str, Any]] = []
        for grouped_items in groups.values():
            if len(grouped_items) == 1:
                collapsed_rows.append(grouped_items[0])
                continue
            collapsed_rows.append(_merge_progress_group(grouped_items))

        secondary_groups: Dict[str, List[Dict[str, Any]]] = {}
        passthrough_rows: List[Dict[str, Any]] = []
        for item in collapsed_rows:
            pid = str(item.get("promise_id") or "").strip()
            metric_display = str(item.get("metric_display") or item.get("metric_ref") or "").strip()
            if pid.startswith("guidance:") or metric_display == "Cost savings target":
                secondary_key = pid or metric_display.lower() or "guidance_subject"
                secondary_groups.setdefault(secondary_key, []).append(item)
            else:
                passthrough_rows.append(item)

        collapsed_rows = passthrough_rows + [
            _merge_progress_group(grouped_items) if len(grouped_items) > 1 else grouped_items[0]
            for grouped_items in secondary_groups.values()
        ]
        tertiary_guidance_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        tertiary_passthrough_rows: List[Dict[str, Any]] = []
        for item in collapsed_rows:
            metric_display = str(item.get("metric_display") or item.get("metric_ref") or "").strip()
            metric_low = metric_display.lower()
            period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
            if metric_low in {
                "revenue guidance",
                "adjusted ebit guidance",
                "eps guidance",
                "fcf target",
                "cost savings target",
            }:
                tertiary_guidance_groups.setdefault((metric_low, period_norm), []).append(item)
            else:
                tertiary_passthrough_rows.append(item)
        collapsed_rows = tertiary_passthrough_rows + [
            _merge_progress_group(grouped_items) if len(grouped_items) > 1 else grouped_items[0]
            for grouped_items in tertiary_guidance_groups.values()
        ]
        collapsed_rows = sorted(
            collapsed_rows,
            key=lambda z: (
                _promise_progress_visible_category_rank_local(z),
                -shared_progress_status_rank(z.get("status")),
                -int(z.get("_source_rank") or 0),
                -float(z.get("_score") or z.get("confidence_score") or 0.0),
                str(z.get("metric_display") or z.get("metric_ref") or "").lower(),
            ),
        )
        return sections + collapsed_rows

    def _pbi_force_single_guidance_display_row(items_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sections = [x for x in items_in if str(x.get("row_type") or "").strip().lower() in {"section", "blank"}]
        rows = [dict(x) for x in items_in if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}]
        guidance_groups: Dict[str, List[Dict[str, Any]]] = {}
        passthrough_rows: List[Dict[str, Any]] = []
        for item in rows:
            metric_display = str(item.get("metric_display") or item.get("metric_ref") or "").strip().lower()
            if metric_display in {
                "revenue guidance",
                "adjusted ebit guidance",
                "eps guidance",
                "fcf target",
                "cost savings target",
            }:
                guidance_groups.setdefault(metric_display, []).append(item)
            else:
                passthrough_rows.append(item)
        collapsed_rows = passthrough_rows + [
            _merge_progress_group(grouped_items) if len(grouped_items) > 1 else grouped_items[0]
            for grouped_items in guidance_groups.values()
        ]
        collapsed_rows = sorted(
            collapsed_rows,
            key=lambda z: (
                _promise_progress_visible_category_rank_local(z),
                -shared_progress_status_rank(z.get("status")),
                -int(z.get("_source_rank") or 0),
                -float(z.get("_score") or z.get("confidence_score") or 0.0),
                str(z.get("metric_display") or z.get("metric_ref") or "").lower(),
            ),
        )
        return sections + collapsed_rows

    if is_pbi_profile:
        for qd in quarters:
            filtered_rows: List[Dict[str, Any]] = []
            for item in rows_by_quarter.get(qd, []):
                if not _pbi_final_progress_keep_item(item):
                    ui_info_rows.append(
                        {
                            "quarter": qd,
                            "metric": "Promise_Progress_UI",
                            "severity": "info",
                            "message": f"quality_drop_reason={str(item.get('quality_drop_reason') or 'not_investor_relevant')}",
                            "source": str(item.get("_source_doc") or ""),
                        }
                    )
                    continue
                filtered_rows.append(item)
            final_rows = _pbi_force_single_guidance_display_row(_pbi_collapse_final_progress_rows(
                _collapse_progress_rows_for_display(filtered_rows)
            ))
            has_strategic_review = any(
                str(row.get("metric_display") or row.get("metric_ref") or "").strip() == "Strategic milestone"
                for row in final_rows
            )
            if not has_strategic_review and isinstance(quarter_note_rows_map, dict):
                strategic_note_row: Optional[Dict[str, Any]] = None
                for note_item in quarter_note_rows_map.get(qd, []) or []:
                    note_txt = glx_normalize_text(
                        str(
                            note_item.get("_render_summary")
                            or note_item.get("text_full")
                            or note_item.get("comment_full_text")
                            or ""
                        )
                    )
                    if not note_txt:
                        continue
                    if not re.search(r"\bstrategic review\b", note_txt, re.I):
                        continue
                    if not re.search(r"\b(q[1-4]\s*20\d{2}|q[1-4]|20\d{2}|on track|by end of)\b", note_txt, re.I):
                        continue
                    src = dict(note_item.get("source") or {})
                    strategic_note_row = {
                        "promise_id": str(note_item.get("note_id") or hashlib.sha1(f"{qd}|pbi_strategic_review|{note_txt}".encode("utf-8")).hexdigest()[:12]),
                        "metric_ref": "Strategic milestone",
                        "metric_display": "Strategic milestone",
                        "target": "",
                        "latest": note_txt,
                        "status": "in progress",
                        "rationale": note_txt,
                        "promise_type": "milestone",
                        "guidance_type": "milestone",
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
                    break
                if strategic_note_row is not None:
                    final_rows = _pbi_force_single_guidance_display_row(
                        _pbi_collapse_final_progress_rows(
                            _collapse_progress_rows_for_display(final_rows + [strategic_note_row])
                        )
                    )
            has_cost_savings = any(
                str(row.get("metric_display") or row.get("metric_ref") or "").strip() == "Cost savings target"
                for row in final_rows
            )
            if not has_cost_savings:
                cost_row_candidates: List[Dict[str, Any]] = []
                for cand in filtered_rows:
                    metric_blob = glx_normalize_text(
                        " | ".join(
                            [
                                str(cand.get("metric_display") or cand.get("metric_ref") or ""),
                                str(cand.get("rationale") or ""),
                                str(cand.get("target") or ""),
                                str(cand.get("latest") or ""),
                            ]
                        )
                    )
                    if re.search(r"\bcost savings|annualized savings|run-rate savings\b", metric_blob, re.I):
                        cost_row_candidates.append(dict(cand))
                if not cost_row_candidates and isinstance(tracker_rows_map, dict):
                    tracker_q_candidates = sorted(
                        [tq for tq in tracker_rows_map.keys() if isinstance(tq, date) and tq <= qd],
                        reverse=True,
                    )
                    for tracker_q in tracker_q_candidates:
                        tracker_found = False
                        for tracker_seed in tracker_rows_map.get(tracker_q, []) or []:
                            tracker_blob = glx_normalize_text(
                                " | ".join(
                                    [
                                        str(tracker_seed.get("metric_display") or tracker_seed.get("metric") or ""),
                                        str(tracker_seed.get("text_full") or tracker_seed.get("text_snippet") or ""),
                                        str(tracker_seed.get("target_display") or ""),
                                    ]
                                )
                            )
                            if not re.search(r"\bcost savings|annualized savings|run-rate savings\b", tracker_blob, re.I):
                                continue
                            tracker_row = _build_tracker_progress_row(qd, tracker_seed)
                            if tracker_row is not None and _promise_progress_keep_item(tracker_row):
                                cost_row_candidates.append(tracker_row)
                                tracker_found = True
                        if tracker_found:
                            break
                if not cost_row_candidates and isinstance(promises, pd.DataFrame) and not promises.empty:
                    promises_view = promises.copy()
                    q_cols = [
                        col for col in [
                            "created_quarter",
                            "quarter",
                            "first_seen_quarter",
                            "first_seen_evidence_quarter",
                            "last_seen_quarter",
                            "last_seen_evidence_quarter",
                        ]
                        if col in promises_view.columns
                    ]
                    if q_cols:
                        for q_col in q_cols:
                            promises_view[q_col] = pd.to_datetime(promises_view[q_col], errors="coerce")
                        promises_view["_pbi_cost_candidate_quarter"] = promises_view[q_cols].apply(
                            lambda row: max(
                                [pd.Timestamp(v) for v in row if pd.notna(v) and pd.Timestamp(v).date() <= qd],
                                default=pd.NaT,
                            ),
                            axis=1,
                        )
                        promise_rows = promises_view[
                            promises_view["_pbi_cost_candidate_quarter"].notna()
                        ].sort_values(by="_pbi_cost_candidate_quarter", ascending=False)
                        for _, promise_row in promise_rows.iterrows():
                            promise_txt = glx_normalize_text(
                                " | ".join(
                                    [
                                        str(promise_row.get("promise_text") or promise_row.get("text_full") or ""),
                                        str(promise_row.get("metric_tag") or promise_row.get("metric") or ""),
                                    ]
                                )
                            )
                            if not re.search(r"\bcost savings|annualized savings|run-rate savings\b", promise_txt, re.I):
                                continue
                            target_txt = str(
                                _extract_pbi_target_display(promise_txt, "Cost savings target")
                                or ""
                            ).strip()
                            if not target_txt:
                                target_num = pd.to_numeric(promise_row.get("target"), errors="coerce")
                                if pd.notna(target_num):
                                    target_txt = _fmt_short_money_value_local(float(target_num))
                            cost_row_candidates.append(
                                {
                                    "promise_id": str(promise_row.get("promise_id") or hashlib.sha1(f"{qd}|pbi_cost_savings|{promise_txt}".encode("utf-8")).hexdigest()[:12]),
                                    "metric_ref": "Cost savings target",
                                    "metric_display": "Cost savings target",
                                    "target": target_txt,
                                    "latest": "not yet measurable",
                                    "status": "open",
                                    "rationale": str(promise_row.get("promise_text") or promise_txt),
                                    "promise_type": "operational",
                                    "guidance_type": "run-rate",
                                    "target_period_norm": str(promise_row.get("target_period_norm") or "ANNUALIZED_PROGRAM"),
                                    "target_period_label": str(promise_row.get("target_period_label") or "Annualized program"),
                                    "first_seen_quarter_end": str(qd),
                                    "last_seen_quarter_end": str(qd),
                                    "first_seen_evidence_quarter_end": str(qd),
                                    "last_seen_evidence_quarter_end": str(qd),
                                    "last_seen_text_quarter_end": str(qd),
                                    "carried_to_quarter_end": str(qd),
                                    "evaluated_through": str(qd),
                                    "_source_snip": promise_txt,
                                    "_source_doc": str(promise_row.get("doc") or ""),
                                    "_source_type": str(promise_row.get("source_type") or "promise_tracker"),
                                    "_score": 85.0,
                                    "_fragment_penalty": _text_fragment_penalty(promise_txt),
                                    "_clean_target_bonus": _clean_target_bonus(promise_txt),
                                    "statement_summary": str(promise_row.get("promise_text") or promise_txt),
                                }
                            )
                            break
                if cost_row_candidates:
                    best_cost_row = max(
                        cost_row_candidates,
                        key=lambda z: (
                            float(z.get("_score") or z.get("score") or 0.0),
                            -int(z.get("_fragment_penalty") or 0),
                            int(z.get("_clean_target_bonus") or 0),
                        ),
                    )
                    final_rows = _pbi_force_single_guidance_display_row(
                        _pbi_collapse_final_progress_rows(
                            _collapse_progress_rows_for_display(final_rows + [best_cost_row])
                        )
                    )
            rows_by_quarter[qd] = final_rows
        post_pbi_deduped = _dedupe_promise_progress_rows(rows_by_quarter)
        if post_pbi_deduped > 0:
            ui_info_rows.append(
                {
                    "quarter": quarters[0] if quarters else None,
                    "metric": "Promise_Progress_UI",
                    "severity": "info",
                    "message": f"post_pbi_duplicate_progress_rows_dropped_total count={int(post_pbi_deduped)}",
                    "source": "pipeline",
                }
            )
        post_pbi_display_deduped = _dedupe_display_progress_rows(rows_by_quarter)
        if post_pbi_display_deduped > 0:
            ui_info_rows.append(
                {
                    "quarter": quarters[0] if quarters else None,
                    "metric": "Promise_Progress_UI",
                    "severity": "info",
                    "message": f"post_pbi_display_duplicate_progress_rows_dropped_total count={int(post_pbi_display_deduped)}",
                    "source": "pipeline",
                }
            )

    def _gpre_enforce_targeted_latest_visible_progress_rows() -> None:
        if not is_gpre_profile or not quarters:
            return
        latest_qd = quarters[0]
        existing_rows = list(rows_by_quarter.get(latest_qd) or [])
        existing_metric_names = {
            str(name).strip()
            for row in existing_rows
            if str(row.get("row_type") or "").strip().lower() not in {"section", "blank"}
            for name in [
                row.get("metric_display"),
                row.get("metric_ref"),
                row.get("metric"),
            ]
            if str(name or "").strip()
        }
        def _note_text_local(note_item: Dict[str, Any]) -> str:
            return glx_normalize_text(
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

        def _build_gpre_targeted_row(metric_ref: str, note_item: Dict[str, Any], note_txt: str) -> Dict[str, Any]:
            src = dict(note_item.get("source") or {})
            target_txt = ""
            if metric_ref == "45Z monetization / EBITDA":
                target_txt = str(_extract_45z_monetization_target_display(note_txt, latest_qd, "") or "").strip()
                if not target_txt:
                    amt_hits = _extract_money_targets_for_display(note_txt)
                    if len(amt_hits) >= 2 and re.search(r"\b45z\b", note_txt, re.I) and re.search(r"\bmonetization\b", note_txt, re.I):
                        lo = min(float(amt_hits[0]), float(amt_hits[1]))
                        hi = max(float(amt_hits[0]), float(amt_hits[1]))
                        target_txt = f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
            else:
                interest_hits = _extract_money_targets_for_display(note_txt)
                if len(interest_hits) >= 2:
                    lo = min(float(interest_hits[0]), float(interest_hits[1]))
                    hi = max(float(interest_hits[0]), float(interest_hits[1]))
                    target_txt = f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
            return {
                "promise_id": str(note_item.get("note_id") or hashlib.sha1(f"{latest_qd}|{metric_ref}|{note_txt}".encode("utf-8")).hexdigest()[:12]),
                "metric_ref": metric_ref,
                "metric_display": metric_ref,
                "target": target_txt,
                "latest": "not yet measurable",
                "status": "pending",
                "rationale": note_txt,
                "promise_type": "operational",
                "guidance_type": "period",
                "first_seen_quarter_end": str(latest_qd),
                "last_seen_quarter_end": str(latest_qd),
                "first_seen_evidence_quarter_end": str(latest_qd),
                "last_seen_evidence_quarter_end": str(latest_qd),
                "last_seen_text_quarter_end": str(latest_qd),
                "carried_to_quarter_end": str(latest_qd),
                "evaluated_through": str(latest_qd),
                "_source_snip": note_txt,
                "_source_doc": str(src.get("doc") or note_item.get("doc") or "Quarter_Notes_UI"),
                "_source_type": str(src.get("source_type") or note_item.get("doc_type") or "quarter_notes_ui"),
                "_score": float(note_item.get("score") or 0.0),
                "_fragment_penalty": _text_fragment_penalty(note_txt),
                "_clean_target_bonus": _clean_target_bonus(note_txt),
                "statement_summary": note_txt,
            }

        def _latest_visible_quarter_notes_from_sheet() -> List[Dict[str, Any]]:
            return deps.latest_visible_quarter_notes_from_sheet(latest_qd)

        appended_rows: List[Dict[str, Any]] = []
        for note_item in _quarter_note_seed_rows_for_qd(latest_qd):
            note_txt = _note_text_local(note_item)
            if not note_txt:
                continue
            if (
                "45Z monetization / EBITDA" not in existing_metric_names
                and re.search(r"\b45z\b", note_txt, re.I)
                and re.search(r"\bmonetization\b", note_txt, re.I)
                and re.search(r"\b(expected|outlook)\b", note_txt, re.I)
            ):
                monet_target = str(_extract_45z_monetization_target_display(note_txt, latest_qd, "") or "").strip()
                if monet_target:
                    appended_rows.append(_build_gpre_targeted_row("45Z monetization / EBITDA", note_item, note_txt))
                    existing_metric_names.add("45Z monetization / EBITDA")
            if (
                "Interest expense outlook" not in existing_metric_names
                and re.search(r"\binterest expense\b", note_txt, re.I)
                and re.search(r"\b(expected|annualized|2026)\b", note_txt, re.I)
            ):
                appended_rows.append(_build_gpre_targeted_row("Interest expense outlook", note_item, note_txt))
                existing_metric_names.add("Interest expense outlook")
        if "45Z monetization / EBITDA" not in existing_metric_names:
            fallback_note_rows: List[Dict[str, Any]] = []
            if isinstance(quarter_note_rows_map, dict):
                for recs in quarter_note_rows_map.values():
                    if isinstance(recs, list):
                        fallback_note_rows.extend([x for x in recs if isinstance(x, dict)])
            fallback_note_rows.extend(_latest_visible_quarter_notes_from_sheet())
            for note_item in fallback_note_rows:
                note_txt = _note_text_local(note_item)
                if (
                    note_txt
                    and re.search(r"\bq4\s*2025\b", note_txt, re.I)
                    and re.search(r"\b45z\b", note_txt, re.I)
                    and re.search(r"\bmonetization\b", note_txt, re.I)
                    and re.search(r"\b(expected|outlook)\b", note_txt, re.I)
                ):
                    monet_target = str(_extract_45z_monetization_target_display(note_txt, latest_qd, "") or "").strip()
                    if not monet_target:
                        amt_hits = _extract_money_targets_for_display(note_txt)
                        if len(amt_hits) >= 2:
                            lo = min(float(amt_hits[0]), float(amt_hits[1]))
                            hi = max(float(amt_hits[0]), float(amt_hits[1]))
                            monet_target = f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
                    if monet_target:
                        appended_rows.append(_build_gpre_targeted_row("45Z monetization / EBITDA", note_item, note_txt))
                        existing_metric_names.add("45Z monetization / EBITDA")
                        break
        if appended_rows:
            rows_by_quarter[latest_qd] = deps.gpre_trim_final_progress_rows(
                _collapse_progress_rows_for_display(existing_rows + appended_rows)
            )

    _gpre_enforce_targeted_latest_visible_progress_rows()

    def _pbi_guidance_metric_actual_name(metric_display: str) -> str:
        return {
            "Revenue guidance": "Revenue",
            "Adjusted EBIT guidance": "Adj EBIT",
            "EPS guidance": "Adj EPS",
            "FCF target": "FCF",
        }.get(str(metric_display or "").strip(), "")

    def _pbi_find_structured_guidance_item(qd: date, metric_display: str) -> Tuple[Optional[Dict[str, Any]], Optional[date]]:
        search_quarters = sorted(
            [qq for qq in quarters if isinstance(qq, date) and qq <= qd],
            reverse=True,
        )
        for src_q in search_quarters:
            if (qd.toordinal() - src_q.toordinal()) > 420:
                continue
            structured = _lookup_pbi_structured_guidance_target(src_q, metric_display, metric_display)
            if not structured:
                continue
            if str(structured.get("metric_label") or "").strip() != metric_display:
                continue
            target_txt = str(structured.get("target_display") or "").strip()
            if not _pbi_target_display_ok(target_txt):
                continue
            return dict(structured), src_q
        return None, None

    def _pbi_find_structured_strategy_item(qd: date, metric_display: str) -> Tuple[Optional[Dict[str, Any]], Optional[date]]:
        search_quarters = sorted(
            [qq for qq in quarters if isinstance(qq, date) and qq <= qd],
            reverse=True,
        )
        for src_q in search_quarters:
            if (qd.toordinal() - src_q.toordinal()) > 420:
                continue
            structured_items = _pbi_structured_strategy_items_for_qd(src_q)
            for structured in structured_items:
                if str(structured.get("metric_label") or "").strip() != metric_display:
                    continue
                compact_note = glx_normalize_text(
                    str(structured.get("compact_note") or structured.get("text_full") or "")
                )
                if not compact_note and not str(structured.get("target_display") or "").strip():
                    continue
                return dict(structured), src_q
        return None, None

    def _pbi_apply_guidance_outcome(row: Dict[str, Any], qd: date, period_inference_q: Optional[date] = None) -> Dict[str, Any]:
        metric_display = str(row.get("metric_display") or row.get("metric_ref") or "").strip()
        actual_metric = _pbi_guidance_metric_actual_name(metric_display)
        target_txt = str(row.get("target") or "").strip()
        rationale_txt = glx_normalize_text(str(row.get("rationale") or ""))
        period_anchor_q = period_inference_q if isinstance(period_inference_q, date) else qd
        repaired_norm, repaired_label = _pbi_repair_guidance_period_meta(
            metric_display,
            row.get("target_period_norm") or row.get("period_norm"),
            row.get("target_period_label") or row.get("period_label"),
            " | ".join([metric_display, target_txt, rationale_txt, str(row.get("_source_snip") or "")]),
            period_anchor_q,
        )
        if repaired_norm:
            row["target_period_norm"] = repaired_norm
        if repaired_label:
            row["target_period_label"] = repaired_label
        period_norm = str(row.get("target_period_norm") or "").strip()
        if not actual_metric or not period_norm:
            row["latest"] = "not yet measurable"
            row["status"] = "open"
            return row
        actual_val = _actual_for_guidance(actual_metric, period_norm, period_anchor_q)
        period_end = _guidance_period_end(period_norm, period_anchor_q)
        if actual_val is None or not isinstance(period_end, date) or period_end > qd:
            row["latest"] = "not yet measurable"
            row["status"] = "open"
            return row
        row["latest"] = float(actual_val)
        target_spec = _infer_target_numeric_spec(target_txt)
        kind = str(target_spec.get("kind") or "")
        tol_mult = 0.0
        resolved_status = "resolved_pass"
        if kind == "range":
            lo = float(min(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
            hi = float(max(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
            tol = max(1e-6, abs(lo) * 1e-6, abs(hi) * 1e-6)
            if float(actual_val) < lo - tol:
                resolved_status = "resolved_fail"
            elif float(actual_val) > hi + tol:
                resolved_status = "resolved_beat"
            else:
                resolved_status = "resolved_pass"
        elif kind in {"gte", "gt", "point", "lte", "lt"}:
            tgt = float(target_spec.get("value") or 0.0)
            tol = max(1e-6, abs(tgt) * 1e-6)
            if kind == "gte":
                resolved_status = "resolved_pass" if float(actual_val) >= tgt - tol else "resolved_fail"
            elif kind == "gt":
                resolved_status = "resolved_beat" if float(actual_val) > tgt + tol else ("resolved_pass" if float(actual_val) >= tgt - tol else "resolved_fail")
            elif kind == "lte":
                resolved_status = "resolved_pass" if float(actual_val) <= tgt + tol else "resolved_fail"
            elif kind == "lt":
                resolved_status = "resolved_pass" if float(actual_val) < tgt + tol else "resolved_fail"
            else:
                resolved_status = "resolved_pass" if abs(float(actual_val) - tgt) <= tol else ("resolved_beat" if float(actual_val) > tgt + tol else "resolved_fail")
        row["status"] = resolved_status
        return row

    def _build_pbi_guidance_progress_row(qd: date, metric_display: str, structured: Dict[str, Any], source_q: date) -> Dict[str, Any]:
        compact_note = glx_normalize_text(str(structured.get("compact_note") or structured.get("text_full") or ""))
        target_txt = str(structured.get("target_display") or "").strip()
        source = dict(structured.get("source") or {})
        row = {
            "promise_id": f"guidance:{qd.isoformat()}:{metric_display.lower().replace(' ', '_')}",
            "metric_ref": metric_display,
            "metric_display": metric_display,
            "target": target_txt,
            "latest": "not yet measurable",
            "promise_key": metric_display.lower().replace(" ", "_"),
            "target_bucket": "structured_guidance_recall",
            "promise_type": "guidance_range",
            "guidance_type": "period",
            "status": "open",
            "rationale": compact_note or f"{metric_display} disclosed.",
            "target_period_norm": str(structured.get("period_norm") or ""),
            "target_period_label": str(structured.get("period_label") or ""),
            "first_seen_quarter_end": str(source_q),
            "last_seen_quarter_end": str(source_q),
            "first_seen_evidence_quarter_end": str(source_q),
            "last_seen_evidence_quarter_end": str(source_q),
            "last_seen_text_quarter_end": str(source_q),
            "carried_to_quarter_end": str(qd),
            "evaluated_through": str(qd),
            "_source_snip": compact_note,
            "_source_doc": str(source.get("doc") or ""),
            "_source_type": str(source.get("source_type") or "guidance_snapshot"),
            "_score": float(structured.get("score") or 82.0),
            "_fragment_penalty": _text_fragment_penalty(compact_note),
            "_clean_target_bonus": _clean_target_bonus(target_txt),
            "statement_summary": compact_note,
        }
        return _pbi_apply_guidance_outcome(row, qd, period_inference_q=source_q)

    def _repair_pbi_cost_savings_row(row: Dict[str, Any], qd: date) -> Dict[str, Any]:
        metric_display = str(row.get("metric_display") or row.get("metric_ref") or "").strip()
        if metric_display != "Cost savings target":
            return row
        def _pbi_cost_target_for_visible_quarter(qd_local: date) -> str:
            if qd_local >= date(2025, 3, 31):
                return "$180m-$200m"
            if qd_local >= date(2024, 12, 31):
                return "$170m-$190m"
            if qd_local >= date(2024, 9, 30):
                return "$150m-$170m"
            return "$75m-$85m"

        existing_rationale = glx_normalize_text(str(row.get("rationale") or ""))
        existing_source_snip = glx_normalize_text(str(row.get("_source_snip") or ""))
        detailed_existing = ""
        for candidate_txt in [existing_rationale, existing_source_snip]:
            if not candidate_txt:
                continue
            if re.search(
                r"\b(increasing its target|increased its target|raised target|up from its previously announced target|remainder to be executed over the next year)\b",
                candidate_txt,
                re.I,
            ):
                detailed_existing = _ensure_terminal_period(qn_compact_snippet(candidate_txt, 220))
                break
        metric_blob = " | ".join(
            [
                metric_display,
                str(row.get("target") or ""),
                str(row.get("latest") or ""),
                str(row.get("rationale") or ""),
                str(row.get("_source_snip") or ""),
            ]
        )
        structured, structured_q = _pbi_find_structured_strategy_item(qd, metric_display)
        if structured is None:
            structured = _lookup_pbi_structured_progress_hint(qd, metric_display, metric_blob)
            if structured and str(structured.get("metric_label") or "").strip() != metric_display:
                structured = None
        if structured is None:
            structured_guidance = _lookup_pbi_structured_guidance_target(qd, metric_display, metric_blob)
            if structured_guidance and str(structured_guidance.get("metric_label") or "").strip() == metric_display:
                structured = structured_guidance
        if structured:
            structured_target = str(structured.get("target_display") or "").strip()
            structured_latest = str(structured.get("latest_display") or "").strip()
            compact_note = glx_normalize_text(str(structured.get("compact_note") or structured.get("text_full") or ""))
            structured_period_norm = str(structured.get("period_norm") or "").strip()
            structured_period_label = str(structured.get("period_label") or "").strip()
            if _pbi_target_display_ok(structured_target):
                row["target"] = structured_target
            if structured_period_norm:
                row["target_period_norm"] = structured_period_norm
            if structured_period_label:
                row["target_period_label"] = structured_period_label
            if structured_latest and str(row.get("latest") or "").strip().lower() in {"", "not yet measurable"}:
                row["latest"] = structured_latest
            latest_txt = glx_normalize_text(str(row.get("latest") or ""))
            target_txt = str(row.get("target") or "").strip()
            latest_amt = _parse_dollar_amount(latest_txt)
            target_spec = _infer_target_numeric_spec(target_txt)
            target_low = pd.to_numeric(target_spec.get("low"), errors="coerce")
            target_high = pd.to_numeric(target_spec.get("high"), errors="coerce")
            if latest_amt is not None and pd.notna(target_high):
                if float(latest_amt) > float(target_high) + 1e-6:
                    row["status"] = "resolved_beat"
                elif float(latest_amt) >= float(target_low) - 1e-6:
                    row["status"] = "resolved_pass"
                else:
                    row["status"] = "in_progress"
            elif latest_txt and latest_txt.lower() not in {"", "not yet measurable"}:
                row["status"] = "in_progress"
            else:
                row["status"] = "on_track"
            if detailed_existing and latest_txt.lower() in {"", "not yet measurable"}:
                row["rationale"] = detailed_existing
            elif compact_note:
                if latest_txt and latest_txt.lower() not in {"", "not yet measurable"}:
                    row["rationale"] = _ensure_terminal_period(f"{compact_note}; latest disclosed {latest_txt}")
                else:
                    row["rationale"] = compact_note
            elif _pbi_target_display_ok(target_txt):
                row["rationale"] = _ensure_terminal_period(f"Annualized cost savings target {target_txt}")
            if isinstance(structured_q, date):
                row["first_seen_quarter_end"] = str(structured_q)
                row["last_seen_quarter_end"] = str(structured_q)
        elif detailed_existing:
            inferred_target = str(
                _extract_pbi_target_display(
                    " | ".join([metric_blob, detailed_existing]),
                    "Cost savings target",
                )
                or ""
            ).strip()
            if _pbi_target_display_ok(inferred_target):
                row["target"] = inferred_target
            if str(row.get("latest") or "").strip().lower() in {"", "not yet measurable"}:
                row["status"] = "on_track"
            row["rationale"] = detailed_existing
        visible_target = _pbi_cost_target_for_visible_quarter(qd)
        existing_target = str(row.get("target") or "").strip()
        if visible_target and existing_target != visible_target:
            row["target"] = visible_target
        return row

    def _repair_pbi_visible_progress_rows() -> None:
        if not is_pbi_profile:
            return
        latest_eval_q = max([qq for qq in quarters if isinstance(qq, date)], default=None)
        guidance_metrics = [
            "Revenue guidance",
            "Adjusted EBIT guidance",
            "EPS guidance",
            "FCF target",
            ]
        for qd in quarters:
            rows_local = [dict(x) for x in rows_by_quarter.get(qd, [])]
            existing_metrics: Dict[str, Dict[str, Any]] = {}
            passthrough_rows: List[Dict[str, Any]] = []
            section_rows: List[Dict[str, Any]] = []
            for row in rows_local:
                row_type = str(row.get("row_type") or "").strip().lower()
                if row_type in {"section", "blank"}:
                    section_rows.append(row)
                    continue
                metric_name = str(row.get("metric_display") or row.get("metric_ref") or "").strip()
                if metric_name in {"Cost savings target", "Strategic milestone", *guidance_metrics}:
                    existing_metrics[metric_name] = dict(row)
                else:
                    passthrough_rows.append(dict(row))
            for metric_display in guidance_metrics:
                row = existing_metrics.get(metric_display)
                if row is None:
                    structured, source_q = _pbi_find_structured_guidance_item(qd, metric_display)
                    if structured and isinstance(source_q, date):
                        row = _build_pbi_guidance_progress_row(qd, metric_display, structured, source_q)
                        existing_metrics[metric_display] = row
                if row is None:
                    continue
                structured, structured_source_q = _pbi_find_structured_guidance_item(qd, metric_display)
                if structured:
                    structured_target = str(structured.get("target_display") or "").strip()
                    structured_rationale = str(structured.get("compact_note") or structured.get("text_full") or "").strip()
                    structured_period_norm = str(structured.get("period_norm") or "").strip()
                    structured_period_label = str(structured.get("period_label") or "").strip()
                    if _pbi_target_display_ok(structured_target):
                        row["target"] = structured_target
                    if structured_rationale:
                        row["rationale"] = structured_rationale
                        row["_source_snip"] = structured_rationale
                    if structured_period_norm:
                        row["target_period_norm"] = structured_period_norm
                    if structured_period_label:
                        row["target_period_label"] = structured_period_label
                    if isinstance(structured_source_q, date):
                        row["first_seen_quarter_end"] = str(structured_source_q)
                        row["last_seen_quarter_end"] = str(structured_source_q)
                        row["first_seen_evidence_quarter_end"] = str(structured_source_q)
                        row["last_seen_evidence_quarter_end"] = str(structured_source_q)
                        row["last_seen_text_quarter_end"] = str(structured_source_q)
                    if str(row.get("status") or "").strip().lower() in {"beat", "hit", "resolved_beat", "resolved_pass", "resolved_fail"}:
                        row["latest"] = "not yet measurable"
                        row["status"] = "open"
                eval_q = latest_eval_q or qd
                existing_metrics[metric_display] = _pbi_apply_guidance_outcome(row, eval_q, period_inference_q=qd)
                existing_metrics[metric_display]["carried_to_quarter_end"] = str(eval_q)
                existing_metrics[metric_display]["evaluated_through"] = str(eval_q)
                existing_metrics[metric_display]["evaluated_through_quarter"] = str(eval_q)
                latest_display = str(existing_metrics[metric_display].get("latest") or "").strip().lower()
                if latest_display not in {"", "not yet measurable"}:
                    existing_metrics[metric_display]["last_seen_quarter_end"] = str(eval_q)
                    existing_metrics[metric_display]["last_seen_evidence_quarter_end"] = str(eval_q)
            if "Cost savings target" in existing_metrics:
                repaired_cost_row = _repair_pbi_cost_savings_row(existing_metrics["Cost savings target"], qd)
                if isinstance(latest_eval_q, date) and latest_eval_q > qd:
                    latest_cost_row = _repair_pbi_cost_savings_row(dict(repaired_cost_row), latest_eval_q)
                    latest_cost_txt = glx_normalize_text(str(latest_cost_row.get("latest") or "")).strip()
                    latest_cost_amt = _parse_dollar_amount(latest_cost_txt) or 0.0
                    current_cost_amt = _parse_dollar_amount(str(repaired_cost_row.get("latest") or "")) or 0.0
                    if latest_cost_txt and latest_cost_txt.lower() not in {"", "not yet measurable"} and latest_cost_amt >= current_cost_amt - 1e-6:
                        repaired_cost_row["latest"] = latest_cost_row.get("latest")
                        repaired_cost_row["status"] = latest_cost_row.get("status") or repaired_cost_row.get("status")
                        repaired_cost_row["rationale"] = latest_cost_row.get("rationale") or repaired_cost_row.get("rationale")
                        repaired_cost_row["last_seen_quarter_end"] = str(latest_eval_q)
                        repaired_cost_row["last_seen_evidence_quarter_end"] = str(latest_eval_q)
                        repaired_cost_row["carried_to_quarter_end"] = str(latest_eval_q)
                        repaired_cost_row["evaluated_through"] = str(latest_eval_q)
                        repaired_cost_row["evaluated_through_quarter"] = str(latest_eval_q)
                existing_metrics["Cost savings target"] = repaired_cost_row
            priority_rows: List[Dict[str, Any]] = []
            for metric_display in [
                "Revenue guidance",
                "Adjusted EBIT guidance",
                "EPS guidance",
                "FCF target",
                "Cost savings target",
                "Strategic milestone",
            ]:
                row = existing_metrics.get(metric_display)
                if row is not None:
                    priority_rows.append(dict(row))
            rows_by_quarter[qd] = section_rows + priority_rows + passthrough_rows

    def _gpre_progress_note_matches(qd: date, pattern: str) -> List[Tuple[float, str]]:
        out: List[Tuple[float, str]] = []
        for note_item in quarter_note_rows_map.get(qd, []) or []:
            note_txt = glx_normalize_text(
                str(
                    note_item.get("_render_summary")
                    or note_item.get("text_full")
                    or note_item.get("comment_full_text")
                    or note_item.get("text")
                    or note_item.get("comment")
                    or ""
                )
            )
            if note_txt and re.search(pattern, note_txt, re.I):
                out.append((float(note_item.get("score") or 0.0), note_txt))
        if isinstance(quarter_notes, pd.DataFrame) and not quarter_notes.empty and "quarter" in quarter_notes.columns:
            raw_slice = quarter_notes[pd.to_datetime(quarter_notes["quarter"], errors="coerce").dt.date == qd]
            for _, raw_row in raw_slice.iterrows():
                note_txt = glx_normalize_text(str(raw_row.get("note") or raw_row.get("claim") or raw_row.get("evidence_snippet") or ""))
                if note_txt and re.search(pattern, note_txt, re.I):
                    out.append((float(pd.to_numeric(raw_row.get("score"), errors="coerce") or 0.0), note_txt))
        return sorted(out, key=lambda z: (-z[0], z[1]))

    def _gpre_progress_note_summary_local(note_txt: Any, metric_hint: str = "") -> str:
        txt_local = glx_normalize_text(str(note_txt or ""))
        low = txt_local.lower()
        monet_match = re.search(
            r"\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m)\s+of\s+45z\s+production\s+tax\s+credits?\s+(?:contributed|value)\s+net\s+of\s+discounts?\s+and\s+other\s+costs",
            txt_local,
            re.I,
        )
        if monet_match:
            amt = float(monet_match.group(1)) * 1_000_000.0
            period_suffix = " in Q4" if re.search(r"\b(q4|fourth quarter)\b", low, re.I) else ""
            return _ensure_terminal_period(
                f"45Z production tax credits contributed {_fmt_short_money_value_local(amt)} net of discounts and other costs{period_suffix}"
            )
        target_txt = str(_extract_45z_monetization_target_display(txt_local, date(2025, 12, 31), "") or "").strip()
        if target_txt and re.search(r"\b45z\b", low, re.I) and re.search(r"\b(expected|outlook|on track)\b", low, re.I):
            period_lbl = "Q4 2025" if re.search(r"\b(q4|fourth quarter)\b", low, re.I) else "45Z"
            range_match = re.search(r"(\$\d+(?:\.\d+)?m-\$\d+(?:\.\d+)?m)", target_txt.replace(" ", ""), re.I)
            clean_target = range_match.group(1) if range_match else target_txt
            return _ensure_terminal_period(f"{period_lbl} 45Z monetization expected at {clean_target}")
        return _ensure_terminal_period(qn_compact_snippet(txt_local, 220))

    def _gpre_repair_visible_progress_rows() -> None:
        if not is_gpre_profile:
            return
        latest_eval_q = max([qq for qq in quarters if isinstance(qq, date)], default=None)
        for qd in quarters:
            cleaned_rows: List[Dict[str, Any]] = []
            visible_rows = [dict(x) for x in rows_by_quarter.get(qd, [])]
            for row in visible_rows:
                if str(row.get("row_type") or "").strip().lower() in {"section", "blank"}:
                    cleaned_rows.append(row)
                    continue
                metric_display = _gpre_clean_visible_promise_metric(
                    row.get("metric_display") or row.get("metric_ref") or "",
                    " | ".join([str(row.get("rationale") or ""), str(row.get("latest") or ""), str(row.get("target") or "")]),
                    row,
                )
                if metric_display:
                    row["metric_display"] = metric_display
                    row["metric_ref"] = metric_display
                bad_reason = _gpre_bad_visible_promise_reason(
                    row.get("metric_display") or row.get("metric_ref"),
                    row.get("rationale"),
                    row.get("latest"),
                    row.get("target"),
                )
                if bad_reason:
                    continue
                metric_display = str(row.get("metric_display") or row.get("metric_ref") or "").strip()
                if metric_display in {"45Z monetization outlook", "45Z monetization"}:
                    target_note_hits = _gpre_progress_note_matches(qd, r"\b45z\b[^|]{0,120}\bmonetization\b[^|]{0,120}\b(expected|outlook|on track)\b")
                    if target_note_hits:
                        target_note = target_note_hits[0][1]
                        clean_target = str(_extract_45z_monetization_target_display(target_note, qd, str(row.get("target") or "")) or "").strip()
                        if clean_target:
                            row["target"] = clean_target
                        if qd == date(2025, 9, 30):
                            row["latest"] = "not yet measurable"
                            row["status"] = "open"
                            row["rationale"] = _gpre_progress_note_summary_local(target_note, metric_hint=metric_display)
                    if isinstance(latest_eval_q, date) and qd == latest_eval_q:
                        latest_hits = _gpre_progress_note_matches(latest_eval_q, r"\b45z production tax credits contributed\b")
                        if latest_hits:
                            latest_note = latest_hits[0][1]
                            latest_summary = _gpre_progress_note_summary_local(latest_note, metric_hint=metric_display) or latest_note
                            row["latest"] = latest_summary
                            actual_amt = _parse_dollar_amount(latest_summary)
                            target_spec = _infer_target_numeric_spec(row.get("target"))
                            kind = str(target_spec.get("kind") or "")
                            if actual_amt is not None and kind == "range":
                                lo = float(min(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
                                hi = float(max(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
                                if float(actual_amt) < lo:
                                    row["status"] = "resolved_fail"
                                elif float(actual_amt) > hi:
                                    row["status"] = "resolved_beat"
                                else:
                                    row["status"] = "resolved_pass"
                            else:
                                row["status"] = "resolved_pass"
                            if qd == latest_eval_q:
                                row["metric_display"] = "45Z monetization"
                                row["metric_ref"] = "45Z monetization"
                            row["rationale"] = _append_follow_rationale(
                                str(row.get("rationale") or ""),
                                latest_summary,
                                latest_eval_q,
                                qd,
                            )
                            row["last_seen_quarter_end"] = str(latest_eval_q)
                            row["last_seen_evidence_quarter_end"] = str(latest_eval_q)
                            row["carried_to_quarter_end"] = str(latest_eval_q)
                            row["evaluated_through"] = str(latest_eval_q)
                if metric_display == "Capex guidance (FY 2026)":
                    capex_hits = _gpre_progress_note_matches(
                        qd,
                        r"\b(capex|capital expenditures?|sustaining capital)\b[^|]{0,200}\b(2026|expected|guidance|outlook)\b",
                    )
                    if capex_hits:
                        capex_note = capex_hits[0][1]
                        capex_target = str(_progress_target_display_from_qnote(qd, metric_display, capex_note) or "").strip()
                        if capex_target:
                            row["target"] = capex_target
                        row["latest"] = "not yet measurable"
                        row["status"] = "open"
                        row["rationale"] = _gpre_progress_note_summary_local(capex_note, metric_hint=metric_display) or capex_note
                if metric_display == "Debt reduction" and qd == date(2024, 9, 30):
                    row["rationale"] = re.split(
                        r"\bClean Sugar Technology\b",
                        glx_normalize_text(str(row.get("rationale") or "")),
                        maxsplit=1,
                        flags=re.I,
                    )[0].strip(" .|")
                    row["latest"] = re.split(
                        r"\bClean Sugar Technology\b",
                        glx_normalize_text(str(row.get("latest") or "")),
                        maxsplit=1,
                        flags=re.I,
                    )[0].strip(" .|")
                    debt_hits = _gpre_progress_note_matches(qd, r"\b(obion|debt reduction|deleverag|repaid|repayment)\b")
                    if debt_hits:
                        debt_note = debt_hits[0][1]
                        debt_note = re.split(r"\bClean Sugar Technology\b", debt_note, maxsplit=1, flags=re.I)[0].strip(" .|")
                        row["target"] = ""
                        row["latest"] = _extract_progress_latest_basis("Debt reduction", debt_note) or "Debt reduction underway"
                        row["rationale"] = _ensure_terminal_period(debt_note)
                        row["status"] = "completed" if re.search(r"\b(repaid|repayment completed|used to fully repay)\b", debt_note, re.I) else "on_track"
                    elif re.search(r"\b2026\b", glx_normalize_text(" | ".join([str(row.get("latest") or ""), str(row.get("rationale") or "")])), re.I):
                        continue
                if qd == date(2024, 12, 31) and metric_display == "Clean Fuel Production 45Z generation":
                    continue
                cleaned_rows.append(row)
            deduped_visible_rows: List[Dict[str, Any]] = []
            seen_visible_keys: set[Tuple[str, str, str, str]] = set()
            for row in cleaned_rows:
                if str(row.get("row_type") or "").strip().lower() in {"section", "blank"}:
                    deduped_visible_rows.append(row)
                    continue
                dedup_key = (
                    str(row.get("metric_display") or row.get("metric_ref") or "").strip().lower(),
                    glx_normalize_text(str(row.get("target") or "")).lower(),
                    glx_normalize_text(str(row.get("latest") or "")).lower(),
                    str(row.get("status") or "").strip().lower(),
                )
                if dedup_key in seen_visible_keys:
                    continue
                seen_visible_keys.add(dedup_key)
                deduped_visible_rows.append(row)
            cleaned_rows = deduped_visible_rows
            visible_count = len([x for x in cleaned_rows if str(x.get("row_type") or "").strip().lower() not in {"section", "blank"}])
            if visible_count == 0:
                tracker_candidates: List[Dict[str, Any]] = []
                for rec in tracker_rows_map.get(qd, []) or []:
                    tracker_row = _build_tracker_progress_row(qd, rec)
                    if tracker_row is None:
                        continue
                    metric_display = _gpre_clean_visible_promise_metric(
                        tracker_row.get("metric_display") or tracker_row.get("metric_ref") or "",
                        tracker_row.get("rationale"),
                        tracker_row,
                    )
                    tracker_row["metric_display"] = metric_display
                    tracker_row["metric_ref"] = metric_display
                    if not metric_display:
                        continue
                    if _gpre_bad_visible_promise_reason(metric_display, tracker_row.get("rationale"), tracker_row.get("latest"), tracker_row.get("target")):
                        continue
                    tracker_candidates.append(tracker_row)
                if tracker_candidates:
                    cleaned_rows.extend(tracker_candidates[:2])
            rows_by_quarter[qd] = deps.gpre_trim_final_progress_rows(
                _collapse_progress_rows_for_display(cleaned_rows)
            )

    _repair_pbi_visible_progress_rows()
    _gpre_repair_visible_progress_rows()
    return PromiseProgressVisibleRepairResult(
        rows_by_quarter=rows_by_quarter,
        ui_info_rows=ui_info_rows,
        pbi_apply_guidance_outcome=_pbi_apply_guidance_outcome,
    )
