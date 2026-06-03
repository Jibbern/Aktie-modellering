"""Promise Progress UI bundle construction helpers."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .guidance_lexicon import normalize_text as glx_normalize_text


@dataclass(frozen=True)
class PromiseProgressUiBundleDeps:
    promise_progress: Any
    promise_evidence_df: Any
    hist: Any
    adj_metrics: Any
    is_pbi_profile: bool
    resolve_col: Callable[..., Any]
    hist_view: Callable[..., Any]
    adj_metrics_view: Callable[..., Any]
    classify_pbi_metric_label: Callable[..., str]
    extract_pbi_target_display: Callable[..., str]
    extract_45z_monetization_target_display: Callable[..., str]
    strong_45z_2026_target_display: Callable[..., str]
    extract_money_targets_for_display: Callable[..., Any]
    fmt_short_money_value_local: Callable[..., str]


def build_promise_progress_ui_bundle(
    deps: PromiseProgressUiBundleDeps,
    quarter_hint: Optional[Tuple[date, ...]] = None,
    cached_bundle: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    promise_progress = deps.promise_progress
    promise_evidence_df = deps.promise_evidence_df
    hist = deps.hist
    adj_metrics = deps.adj_metrics
    is_pbi_profile = deps.is_pbi_profile
    _resolve_col = deps.resolve_col
    _hist_view = deps.hist_view
    _adj_metrics_view = deps.adj_metrics_view
    _classify_pbi_metric_label = deps.classify_pbi_metric_label
    _extract_pbi_target_display = deps.extract_pbi_target_display
    _extract_45z_monetization_target_display = deps.extract_45z_monetization_target_display
    _strong_45z_2026_target_display = deps.strong_45z_2026_target_display
    _extract_money_targets_for_display = deps.extract_money_targets_for_display
    _fmt_short_money_value_local = deps.fmt_short_money_value_local

    quarter_key = tuple(q for q in (quarter_hint or ()) if isinstance(q, date))
    if (
        cached_bundle is not None
        and tuple(cached_bundle.get("quarter_key") or ()) == quarter_key
    ):
        return cached_bundle

    empty_guidance_df = pd.DataFrame(columns=["quarter", "value", "_proxy_used", "_source_used"])
    bundle: Dict[str, Any] = {
        "quarter_key": quarter_key,
        "valid": False,
        "prog": pd.DataFrame(),
        "prog_records": [],
        "prog_records_by_q": {},
        "quarters": [],
        "prog_groups": {},
        "cols": {},
        "ev_map_q": {},
        "ev_map_pid": {},
        "hist_local": pd.DataFrame(),
        "adj_local": pd.DataFrame(),
        "evaluation_as_of": None,
        "guidance_series_cache": {
            "Revenue": empty_guidance_df,
            "FCF": empty_guidance_df,
            "Capex": empty_guidance_df,
            "Adj EBIT": empty_guidance_df,
            "Adj EBITDA": empty_guidance_df,
            "Adj EPS": empty_guidance_df,
        },
    }
    if promise_progress is None or promise_progress.empty:
        return bundle

    prog = promise_progress.copy()
    pid_col = _resolve_col(prog, ["promise_id", "id"])
    q_col = _resolve_col(prog, ["quarter", "as_of"])
    st_col = _resolve_col(prog, ["status"])
    sc_col = _resolve_col(prog, ["status_score"])
    src_ev_col = _resolve_col(prog, ["source_evidence_json", "evidence_json"])
    cols = {
        "pid_col": pid_col,
        "q_col": q_col,
        "st_col": st_col,
        "sc_col": sc_col,
        "src_ev_col": src_ev_col,
    }
    bundle["cols"] = cols
    if pid_col is None or q_col is None or st_col is None:
        return bundle

    def _progress_metric_from_event(note_item: Dict[str, Any]) -> str:
        event_type = str(note_item.get("_event_type") or "").strip().lower()
        metric_family = str(note_item.get("_event_metric_family") or "").strip().lower()
        entity_scope = str(note_item.get("_event_entity_scope") or "").strip().lower()
        text_blob = glx_normalize_text(
            " | ".join(
                [
                    str(note_item.get("_render_summary") or ""),
                    str(note_item.get("text_full") or note_item.get("comment_full_text") or ""),
                    str(note_item.get("metric_ref") or ""),
                ]
            )
        )
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
        if re.search(r"\bannualized\b[^|]{0,80}\binterest expense\b|\binterest expense\b[^|]{0,80}\bexpected\b", text_blob, re.I):
            return "Interest expense outlook"
        if re.search(r"\bqualify for production tax credits\b|\bexpected to qualify\b", text_blob, re.I) and not re.search(
            r"\b(monetization|ebitda opportunity|agreement executed|expected q4 2025 monetization)\b",
            text_blob,
            re.I,
        ):
            return "45Z plant qualification readiness"
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
        txt_local = glx_normalize_text(
            str(
                note_item.get("text_full")
                or note_item.get("comment_full_text")
                or note_item.get("text")
                or note_item.get("comment")
                or note_item.get("rationale")
                or ""
            )
        )
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
            pbi_metric = _classify_pbi_metric_label(blob, "")
            if pbi_metric in pbi_allowed_labels_local:
                return pbi_metric
            if pbi_metric:
                return ""
        if re.search(r"\bannualized\b[^|]{0,80}\binterest expense\b|\binterest expense\b[^|]{0,80}\bexpected\b", blob, re.I):
            return "Interest expense outlook"
        if re.search(r"\bqualify for production tax credits\b|\bexpected to qualify\b", blob, re.I) and not re.search(
            r"\b(monetization|ebitda opportunity|agreement executed)\b",
            blob,
            re.I,
        ):
            return "45Z plant qualification readiness"
        if any(k in blob for k in ("45z", "tax credit monetization", "ebitda opportunity", "qualify for production tax credits")):
            return "45Z monetization / EBITDA"
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

    def _parse_source_json(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list) and raw and isinstance(raw[0], dict):
            return raw[0]
        if not isinstance(raw, str) or not raw.strip():
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
            return parsed[0]
        return {}

    prog["_qd"] = pd.to_datetime(prog[q_col], errors="coerce").dt.date
    prog = prog[prog["_qd"].notna()].copy()
    if quarter_key:
        quarters = list(quarter_key)
        prog = prog[prog["_qd"].isin(quarters)].copy()
    else:
        quarters = sorted(prog["_qd"].dropna().unique().tolist(), reverse=True)[:8]
        prog = prog[prog["_qd"].isin(quarters)].copy()
    if not prog.empty:
        prog["_pid"] = prog[pid_col].astype(str)
        prog["_status"] = prog[st_col].astype(str).str.strip().str.lower()
        prog["_status_pri"] = prog["_status"].map(status_priority).fillna(5).astype(int)
        if sc_col:
            prog["_score"] = pd.to_numeric(prog[sc_col], errors="coerce").fillna(0.0)
        else:
            prog["_score"] = 0.0
        if src_ev_col:
            prog["_src_ev"] = prog[src_ev_col].map(_parse_source_json)
        else:
            prog["_src_ev"] = [{} for _ in range(len(prog))]
        prog["_src_snip"] = prog["_src_ev"].map(lambda ev: glx_normalize_text(str(ev.get("snippet") or "")))
        prog["_src_doc"] = prog["_src_ev"].map(lambda ev: str(ev.get("doc_path") or ev.get("doc") or "").strip())
        prog["_src_source_type"] = prog["_src_ev"].map(
            lambda ev: str(ev.get("doc_type") or ev.get("source_type") or "").strip()
        )

    prog_groups: Dict[date, pd.DataFrame] = {}
    prog_records: List[Dict[str, Any]] = []
    prog_records_by_q: Dict[date, List[Dict[str, Any]]] = {}
    if not prog.empty:
        prog_records = [dict(rec) for rec in prog.to_dict("records")]
        for qd, sub in prog.groupby("_qd", sort=False):
            sub_local = sub.sort_values(["_status_pri", "_score"], ascending=[True, False], na_position="last")
            sub_local = sub_local.drop_duplicates(["_pid"], keep="first").copy()
            prog_groups[qd] = sub_local
            prog_records_by_q[qd] = [dict(rec) for rec in sub_local.to_dict("records")]

    ev_map_q: Dict[Tuple[str, date], int] = {}
    ev_map_pid: Dict[str, int] = {}
    if promise_evidence_df is not None and not promise_evidence_df.empty:
        for row_idx, rr in enumerate(promise_evidence_df.itertuples(index=False), start=2):
            rec = rr._asdict()
            pid = str(rec.get("promise_id") or "").strip()
            qd_val = pd.to_datetime(rec.get("quarter"), errors="coerce")
            if not pid:
                continue
            if pd.notna(qd_val):
                ev_map_q[(pid, pd.Timestamp(qd_val).to_period("Q").end_time.date())] = row_idx
            if pid not in ev_map_pid:
                ev_map_pid[pid] = row_idx

    hist_local = _hist_view().copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame()
    if not hist_local.empty:
        if "_quarter" in hist_local.columns:
            hist_local["quarter"] = pd.to_datetime(hist_local["_quarter"], errors="coerce")
        elif "quarter" in hist_local.columns:
            hist_local["quarter"] = pd.to_datetime(hist_local["quarter"], errors="coerce")
        hist_local = hist_local[hist_local["quarter"].notna()].sort_values("quarter")

    adj_local = _adj_metrics_view().copy() if isinstance(adj_metrics, pd.DataFrame) else pd.DataFrame()
    if not adj_local.empty:
        if "_quarter" in adj_local.columns:
            adj_local["quarter"] = pd.to_datetime(adj_local["_quarter"], errors="coerce")
        elif "quarter" in adj_local.columns:
            adj_local["quarter"] = pd.to_datetime(adj_local["quarter"], errors="coerce")
        adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")

    def _latest_non_empty_quarter(df_in: pd.DataFrame) -> Optional[date]:
        if df_in is None or df_in.empty or "quarter" not in df_in.columns:
            return None
        metric_cols = [c for c in df_in.columns if c != "quarter" and not str(c).startswith("_")]
        if not metric_cols:
            return None
        sub = df_in.loc[:, ["quarter"] + metric_cols].copy()
        numeric = sub[metric_cols].apply(pd.to_numeric, errors="coerce")
        non_empty = numeric.notna().any(axis=1)
        if not non_empty.any():
            return None
        latest = pd.to_datetime(sub.loc[non_empty, "quarter"], errors="coerce").dropna().max()
        if pd.isna(latest):
            return None
        return pd.Timestamp(latest).to_period("Q").end_time.date()

    evaluation_candidates = [d for d in [_latest_non_empty_quarter(hist_local), _latest_non_empty_quarter(adj_local)] if d is not None]
    evaluation_as_of = max(evaluation_candidates) if evaluation_candidates else None

    def _guidance_series_from_frame(
        df_in: pd.DataFrame,
        cols: List[str],
        *,
        source_used: str,
        proxy_used: bool = False,
    ) -> pd.DataFrame:
        if df_in is None or df_in.empty or "quarter" not in df_in.columns:
            return empty_guidance_df.copy()
        for col in cols:
            if col not in df_in.columns:
                continue
            dd = df_in[["quarter", col]].copy()
            dd["value"] = pd.to_numeric(dd[col], errors="coerce")
            dd = dd[["quarter", "value"]].dropna(subset=["value"])
            if dd.empty:
                continue
            dd["_proxy_used"] = proxy_used
            dd["_source_used"] = source_used
            return dd
        return empty_guidance_df.copy()

    guidance_series_cache: Dict[str, pd.DataFrame] = {
        "Revenue": _guidance_series_from_frame(hist_local, ["revenue"], source_used="history_q"),
        "Capex": _guidance_series_from_frame(hist_local, ["capex"], source_used="history_q"),
        "Adj EBIT": _guidance_series_from_frame(adj_local, ["adj_ebit"], source_used="non_gaap_adj_ebit"),
        "Adj EBITDA": _guidance_series_from_frame(adj_local, ["adj_ebitda"], source_used="non_gaap_adj_ebitda"),
        "Adj EPS": _guidance_series_from_frame(adj_local, ["adj_eps"], source_used="non_gaap_adj_eps"),
    }
    fcf_series = _guidance_series_from_frame(adj_local, ["adj_fcf"], source_used="non_gaap_adj_fcf")
    if fcf_series.empty and not hist_local.empty and {"cfo", "capex"}.issubset(set(hist_local.columns)):
        fcf_proxy = hist_local[["quarter", "cfo", "capex"]].copy()
        fcf_proxy["cfo"] = pd.to_numeric(fcf_proxy["cfo"], errors="coerce")
        fcf_proxy["capex"] = pd.to_numeric(fcf_proxy["capex"], errors="coerce")
        fcf_proxy = fcf_proxy[fcf_proxy["cfo"].notna() & fcf_proxy["capex"].notna()].copy()
        if not fcf_proxy.empty:
            fcf_proxy["value"] = fcf_proxy["cfo"] - fcf_proxy["capex"]
            fcf_series = fcf_proxy[["quarter", "value"]]
            fcf_series["_proxy_used"] = True
            fcf_series["_source_used"] = "proxy_cfo_capex"
    if fcf_series.empty:
        fcf_series = _guidance_series_from_frame(hist_local, ["fcf"], source_used="history_q")
    guidance_series_cache["FCF"] = fcf_series if not fcf_series.empty else empty_guidance_df.copy()

    bundle = {
        "quarter_key": quarter_key,
        "valid": True,
        "prog": prog,
        "prog_records": prog_records,
        "prog_records_by_q": prog_records_by_q,
        "quarters": quarters,
        "prog_groups": prog_groups,
        "cols": cols,
        "ev_map_q": ev_map_q,
        "ev_map_pid": ev_map_pid,
        "hist_local": hist_local,
        "adj_local": adj_local,
        "evaluation_as_of": evaluation_as_of,
        "guidance_series_cache": guidance_series_cache,
    }
    return bundle
