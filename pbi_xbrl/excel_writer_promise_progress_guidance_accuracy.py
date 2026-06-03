"""Promise Progress guidance accuracy row helpers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Mapping, Optional

import pandas as pd

from .filing_evidence_shared import (
    build_canonical_subject_key as shared_build_canonical_subject_key,
    build_lifecycle_subject_key as shared_build_lifecycle_subject_key,
    build_parent_subject_key as shared_build_parent_subject_key,
    build_promise_lifecycle_key as shared_build_promise_lifecycle_key,
    source_class as shared_source_class,
    statement_class as shared_statement_class,
)
from .guidance_lexicon import FORWARD_NOTES_LABEL


@dataclass(frozen=True)
class PromiseProgressGuidanceAccuracyDeps:
    ui_state: Mapping[str, Any]
    evaluation_as_of: Optional[date]
    guidance_target_text: Callable[[Dict[str, Any]], str]
    series_for_guidance_metric: Callable[[str], pd.DataFrame]
    actual_for_guidance: Callable[[str, str, date], Optional[float]]
    guidance_period_end: Callable[[str, date], Optional[date]]
    guidance_period_label: Callable[[str, date], str]
    guidance_actual_text: Callable[[str, float], str]


def build_guidance_accuracy_rows(
    deps: PromiseProgressGuidanceAccuracyDeps,
    qd: date,
) -> List[Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []
    ui_state = deps.ui_state
    gstore = ui_state.get("guidance_snapshot_by_q", {}) if isinstance(ui_state, dict) else {}
    items = gstore.get(str(qd), []) if isinstance(gstore, dict) else []
    if not isinstance(items, list):
        return out_rows
    for it in items:
        if not isinstance(it, dict):
            continue
        metric_name = str(it.get("metric") or "").strip()
        if metric_name in {"", FORWARD_NOTES_LABEL, "Other", "Unknown"}:
            continue
        guidance_type = str(it.get("guidance_type") or "").strip().lower()
        if guidance_type != "period":
            continue
        kind = str(it.get("kind") or "").strip().lower()
        if kind not in {"range", "point"}:
            continue
        period_norm = str(it.get("target_period_norm") or it.get("period_norm") or "UNK").strip()
        if period_norm in {"", "UNK"}:
            continue
        target_txt = deps.guidance_target_text(it)
        if not target_txt:
            continue
        series_df = deps.series_for_guidance_metric(metric_name)
        proxy_used = bool(series_df.get("_proxy_used", pd.Series([False])).iloc[0]) if not series_df.empty else False
        source_used = str(series_df.get("_source_used", pd.Series([""])).iloc[0]) if not series_df.empty else ""
        actual_val = deps.actual_for_guidance(metric_name, period_norm, qd)
        period_end = deps.guidance_period_end(period_norm, qd)
        if period_end is None:
            continue
        eval_asof = deps.evaluation_as_of or qd
        status = "pending"
        rationale = "Awaiting period actuals."
        latest: Any = "not yet measurable"
        if period_end > eval_asof:
            status = "pending"
            rationale = f"Guidance period {deps.guidance_period_label(period_norm, qd)} has not ended (see evaluated_through)."
        elif series_df.empty:
            status = "no_actual_available"
            rationale = "No actual series available for this guidance metric."
        elif actual_val is not None:
            latest = actual_val
            actual_disp = deps.guidance_actual_text(metric_name, actual_val)
            if metric_name == "FCF" and proxy_used:
                actual_disp = f"{actual_disp} (proxy CFO-Capex)"
            if kind == "range" and it.get("low") is not None and it.get("high") is not None:
                lo = float(min(float(it.get("low")), float(it.get("high"))))
                hi = float(max(float(it.get("low")), float(it.get("high"))))
                mid = (lo + hi) / 2.0
                within = lo <= float(actual_val) <= hi
                above = float(actual_val) > hi
                below = float(actual_val) < lo
                if within:
                    status = "resolved_pass"
                elif above:
                    status = "resolved_beat"
                elif below:
                    status = "resolved_fail"
                else:
                    status = "resolved_fail"
                delta_abs = float(actual_val) - mid
                delta_pct = (float(actual_val) / mid - 1.0) if abs(mid) > 1e-12 else None
                if metric_name in {"Revenue", "Adj EBIT", "Adj EBITDA", "FCF", "Capex"}:
                    delta_txt = f"Δ {delta_abs/1e6:+,.1f}m"
                elif metric_name == "Adj EPS":
                    delta_txt = f"Δ {delta_abs:+.2f}"
                else:
                    delta_txt = f"Δ {delta_abs:+.2f}"
                if delta_pct is not None:
                    delta_txt = f"{delta_txt} ({delta_pct:+.1%})"
                if within:
                    rationale = f"Actual {actual_disp} within range {target_txt}; {delta_txt} vs midpoint."
                elif above:
                    rationale = f"Actual {actual_disp} above range {target_txt}; {delta_txt} vs midpoint."
                else:
                    rationale = f"Actual {actual_disp} below range {target_txt}; {delta_txt} vs midpoint."
            elif kind == "point" and it.get("value") is not None:
                tgt = float(it.get("value"))
                tol = max(1e-9, abs(float(tgt)) * 0.005)
                status = "resolved_pass" if abs(float(actual_val) - tgt) <= tol else "resolved_fail"
                if metric_name in {"Revenue", "Adj EBIT", "Adj EBITDA", "FCF", "Capex"}:
                    d_txt = f"{(float(actual_val)-tgt)/1e6:+,.1f}m"
                elif metric_name == "Adj EPS":
                    d_txt = f"{float(actual_val)-tgt:+.2f}"
                else:
                    d_txt = f"{float(actual_val)-tgt:+.2f}"
                rationale = f"Actual {actual_disp} vs target {target_txt} (Δ {d_txt})."
        else:
            status = "no_actual_available"
            rationale = f"Guidance period ended ({period_end}), but matching actual value was not found."
        stated_q = str(it.get("first_seen_quarter_end") or "").strip()
        last_seen_q = str(it.get("last_seen_quarter_end") or "").strip()
        src = dict(it.get("source") or {})
        period_lbl = deps.guidance_period_label(period_norm, qd)
        progress_metric_display = {
            "Revenue": "Revenue guidance",
            "Adj EBIT": "Adjusted EBIT guidance",
            "Adjusted EBIT": "Adjusted EBIT guidance",
            "Adj EPS": "EPS guidance",
            "EPS": "EPS guidance",
            "FCF": "FCF target",
        }.get(metric_name, f"{metric_name} guidance".strip())
        metric_family = {
            "Revenue": "revenue",
            "Adj EBIT": "adj_ebit",
            "Adjusted EBIT": "adj_ebit",
            "Adj EPS": "eps",
            "EPS": "eps",
            "FCF": "fcf",
        }.get(metric_name, re.sub(r"[^a-z0-9]+", "_", metric_name.lower()).strip("_") or "guidance")
        parent_subject_key = shared_build_parent_subject_key(
            entity_scope="company_total",
            metric_family=metric_family,
            program_token="company_total",
            topic_family="guidance",
        )
        canonical_subject_key = shared_build_canonical_subject_key(
            entity_scope="company_total",
            metric_family=metric_family,
            target_period_norm=period_norm,
            stage_token="program_target",
        )
        promise_lifecycle_key = shared_build_promise_lifecycle_key(
            canonical_subject_key,
            stage_token="program_target",
            promise_type="guidance_range",
        )
        lifecycle_subject_key = shared_build_lifecycle_subject_key(
            parent_subject_key=parent_subject_key,
            canonical_subject_key=canonical_subject_key,
            stage_token="program_target",
            target_period_norm=period_norm,
        )
        out_rows.append(
            {
                "row_type": "guidance",
                "promise_id": f"guidance:{qd.isoformat()}:{metric_name}:{period_norm}",
                "metric_ref": progress_metric_display,
                "metric_display": progress_metric_display,
                "quarter": qd,
                "target": target_txt,
                "target_display": target_txt,
                "latest": latest,
                "promise_key": "",
                "target_bucket": "",
                "promise_type": "guidance_range",
                "scorable": bool(actual_val is not None),
                "numeric_update_this_quarter": False,
                "status": status,
                "rationale": rationale,
                "guidance_type": guidance_type,
                "target_period_norm": period_norm,
                "target_period_label": period_lbl,
                "first_seen_evidence_quarter_end": str(stated_q or qd),
                "last_seen_evidence_quarter_end": str(last_seen_q or stated_q or qd),
                "source": src,
                "qa_severity": "",
                "qa_message": f"actual_series={source_used}" if source_used else "",
                "evaluated_through": str(eval_asof),
                "candidate_type": "follow_through_event",
                "route_reason": "promise_progress",
                "routing_reason": "follow_through_update",
                "evidence_role": "result_evidence" if status.startswith("resolved") else "promise_origin",
                "parent_subject_key": parent_subject_key,
                "canonical_subject_key": canonical_subject_key,
                "promise_lifecycle_key": promise_lifecycle_key,
                "lifecycle_subject_key": lifecycle_subject_key,
                "source_class": shared_source_class(src.get("source_type") or "guidance_snapshot"),
                "statement_class": shared_statement_class(
                    rationale,
                    source_type=src.get("source_type") or "guidance_snapshot",
                    metric_hint=metric_name,
                ),
            }
        )
    return out_rows
