"""Non-render Valuation guidance/outlook/commentary model support.

The visible Valuation renderer remains with its owning writer. The writer injects
run-scoped source, profile, and cache dependencies through a runtime mapping so the
extracted helpers retain their existing closure and cache behavior.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class ValuationGuidanceSupportDeps:
    runtime: MutableMapping[str, Any]


_PUBLIC_ALIASES = {
    "build_guidance_snapshot": "_qh_build_guidance_snapshot",
    "collect_guidance": "_qh_collect_guidance",
    "extract_guidance_items": "_qh_extract_guidance_items",
    "sec_outlook_backfill": "_qh_sec_outlook_backfill",
    "guidance_value_text": "_qh_guidance_value_text",
    "visible_items_for_block": "_qh_visible_items_for_block",
    "item_comment": "_qh_item_comment",
    "items_current_for": "_qh_items_current_for",
}


class ValuationGuidanceSupport:
    def __init__(self, deps: ValuationGuidanceSupportDeps) -> None:
        self.runtime = deps.runtime
        self._namespace = _build_valuation_guidance_support_namespace(deps.runtime)

    def __getattr__(self, name: str) -> Any:
        key = _PUBLIC_ALIASES.get(name, name)
        try:
            return self._namespace[key]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def build_guidance_snapshot(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_build_guidance_snapshot"](*args, **kwargs)

    def collect_guidance(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_collect_guidance"](*args, **kwargs)

    def extract_guidance_items(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_extract_guidance_items"](*args, **kwargs)

    def sec_outlook_backfill(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_sec_outlook_backfill"](*args, **kwargs)

    def guidance_value_text(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_guidance_value_text"](*args, **kwargs)

    def visible_items_for_block(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_visible_items_for_block"](*args, **kwargs)

    def item_comment(self, *args: Any, **kwargs: Any) -> Any:
        return self._namespace["_qh_item_comment"](*args, **kwargs)


def _build_valuation_guidance_support_namespace(runtime: MutableMapping[str, Any]) -> Dict[str, Any]:
    __rt = runtime

    def _rt_get(name: str) -> Any:
        if name in __rt:
            return __rt[name]
        return globals().get(name)
    Any = _rt_get('Any')
    BeautifulSoup = _rt_get('BeautifulSoup')
    Dict = _rt_get('Dict')
    FORWARD_NOTES_LABEL = _rt_get('FORWARD_NOTES_LABEL')
    GUIDANCE_UI_METRIC_PRIORITY = _rt_get('GUIDANCE_UI_METRIC_PRIORITY')
    List = _rt_get('List')
    Optional = _rt_get('Optional')
    Path = _rt_get('Path')
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
    _pbi_guidance_period_label_from_text = _rt_get('_pbi_guidance_period_label_from_text')
    _pbi_repair_guidance_period_meta = _rt_get('_pbi_repair_guidance_period_meta')
    _pbi_structured_strategy_items_for_qd = _rt_get('_pbi_structured_strategy_items_for_qd')
    _period_label_to_norm = _rt_get('_period_label_to_norm')
    _period_sort_for_ui = _rt_get('_period_sort_for_ui')
    _prev_ref_for = _rt_get('_prev_ref_for')
    _promises_view = _rt_get('_promises_view')
    _quarter_notes_view = _rt_get('_quarter_notes_view')
    _read_cached_doc_text = _rt_get('_read_cached_doc_text')
    _read_local_doc_text_shared = _rt_get('_read_local_doc_text_shared')
    _resolve_cached_doc_path = _rt_get('_resolve_cached_doc_path')
    _resolve_col = _rt_get('_resolve_col')
    _sec_docs_for_accession = _rt_get('_sec_docs_for_accession')
    _slide_signal_noise = _rt_get('_slide_signal_noise')
    _submission_recent_row_quarter = _rt_get('_submission_recent_row_quarter')
    _submission_recent_rows = _rt_get('_submission_recent_rows')
    audit = _rt_get('audit')
    cache_root = _rt_get('cache_root')
    date = _rt_get('date')
    dt = _rt_get('dt')
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
    guidance_carry_lookback_quarters = _rt_get('guidance_carry_lookback_quarters')
    hist = _rt_get('hist')
    io = _rt_get('io')
    is_gpre_profile = _rt_get('is_gpre_profile')
    is_pbi_profile = _rt_get('is_pbi_profile')
    json = _rt_get('json')
    max_items_per_guidance_block = _rt_get('max_items_per_guidance_block')
    parse_date = _rt_get('parse_date')
    parse_metadata_key_values = _rt_get('parse_metadata_key_values')
    pd = _rt_get('pd')
    pri = _rt_get('pri')
    promise_progress = _rt_get('promise_progress')
    promises = _rt_get('promises')
    q0_ref = _rt_get('q0_ref')
    qhist_all = _rt_get('qhist_all')
    qn_compact_snippet = _rt_get('qn_compact_snippet')
    quarter_notes = _rt_get('quarter_notes')
    re = _rt_get('re')
    silence_pdfminer_warnings = _rt_get('silence_pdfminer_warnings')
    slides_guidance = _rt_get('slides_guidance')
    ui_state = _rt_get('ui_state')

    _qh_forward_intent_re = re.compile(
        r"\b(guidance|outlook|forecast|target|targets|expect|expects|anticipate|project|plan|intend|reaffirm|raise|lower|maintain|will)\b",
        re.I,
    )
    _qh_period_anchor_re = re.compile(
        r"\b(fy\s*[-/]?\s*(?:20\d{2}|\d{2})|fiscal\s+(?:20\d{2}|\d{2})|full[- ]?year|next fiscal year|next year|next quarter|q[1-4]\s*(?:20\d{2}|\d{2})?|by end of\s+20\d{2})\b",
        re.I,
    )
    def _qh_norm_txt(x: Any) -> str:
        return re.sub(r"\s+", " ", str(x or "").strip())

    def _qh_parse_json(raw: Any) -> Dict[str, Any]:
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

    def _qh_date_eq(v: Any, qref: pd.Timestamp) -> bool:
        t = pd.to_datetime(v, errors="coerce")
        if pd.isna(t):
            return False
        tt = pd.Timestamp(t)
        qq = pd.Timestamp(qref)
        if tt.date() == qq.date():
            return True
        # Accept same fiscal quarter to avoid dropping guidance with non-EoQ timestamps.
        try:
            return tt.to_period("Q") == qq.to_period("Q")
        except Exception:
            return False

    def _qh_source_comment(src: Dict[str, Any]) -> str:
        if not src:
            return "Source: N/A"
        st = str(src.get("source_type") or "").lower()
        form = str(src.get("form") or "")
        accn = str(src.get("accn") or "")
        filed = pd.to_datetime(src.get("filed"), errors="coerce")
        filed_txt = filed.strftime("%Y-%m-%d") if pd.notna(filed) else ""
        doc = str(src.get("doc") or src.get("doc_path") or "")
        section = str(src.get("section") or src.get("section_or_page") or "").strip()
        bits = [f"Source: {st or 'sec'}"]
        if form:
            bits.append(form)
        if accn:
            bits.append(f"accn={accn}")
        if filed_txt:
            bits.append(f"filed={filed_txt}")
        if doc:
            bits.append(f"doc={doc}")
        page = src.get("page")
        if page not in (None, ""):
            bits.append(f"page={page}")
        if section:
            bits.append(f"section={section}")
        return " ".join(bits)

    def _qh_parse_ranges(text_blob: str) -> Dict[str, Tuple[float, float]]:
        out: Dict[str, Tuple[float, float]] = {}
        if not text_blob:
            return out
        metric_kw: List[Tuple[str, List[str]]] = [
            ("Cost savings", ["cost savings", "savings", "run-rate savings", "net annualized cost savings"]),
            ("Revenue", ["revenue", "sales", "top line"]),
            ("Adj EBITDA", ["adjusted ebitda", "adj. ebitda", "adj ebitda", "ebitda"]),
            ("Adj EPS", ["adjusted eps", "adj eps", "eps", "earnings per share"]),
            ("FCF", ["free cash flow", "free-cash-flow", "fcf"]),
            ("Capex", ["capex", "capital expenditures", "capital spending"]),
            ("Restructuring charges", ["restructuring charge", "restructuring charges", "transformation charges", "restructuring costs"]),
            ("Net debt / leverage", ["net debt", "net leverage", "debt/ebitda", "leverage"]),
        ]
        money_metrics = {"Revenue", "Adj EBITDA", "FCF", "Capex", "Cost savings", "Restructuring charges"}

        def _classify_metric(ctx_raw: str) -> str:
            ctx = str(ctx_raw or "").lower()
            if "cost savings" in ctx:
                return "Cost savings"
            for metric_name, kws in metric_kw:
                if any(kw in ctx for kw in kws):
                    return metric_name
            return "Other"

        def _parse_num(raw: Any) -> Optional[float]:
            try:
                return float(str(raw).replace(",", "").strip())
            except Exception:
                return None

        pat = re.compile(
            r"(?:between\s+)?\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.\d+)?)\s*(billion|million|bn|m|%|x)?\s*(?:to|and|\-|–|—)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.\d+)?)\s*(billion|million|bn|m|%|x)?",
            re.I,
        )
        for m in pat.finditer(text_blob):
            ctx = text_blob[max(0, m.start()-80): min(len(text_blob), m.end()+80)]
            metric = _classify_metric(ctx)
            if metric == "Other":
                continue
            lo = _parse_num(m.group(1))
            hi = _parse_num(m.group(3))
            if lo is None or hi is None:
                continue
            u1 = str(m.group(2) or "").lower()
            u2 = str(m.group(4) or u1).lower()
            if metric in money_metrics:
                if u1 in {"billion", "bn"}:
                    lo *= 1e9
                elif u1 in {"million", "m"} or abs(lo) < 2000:
                    lo *= 1e6
                if u2 in {"billion", "bn"}:
                    hi *= 1e9
                elif u2 in {"million", "m"} or abs(hi) < 2000:
                    hi *= 1e6
            lo, hi = (lo, hi) if lo <= hi else (hi, lo)
            if metric == "Revenue":
                mid = (lo + hi) / 2.0
                if mid < 10_000_000 or mid > 10_000_000_000:
                    continue
            out[metric] = (float(lo), float(hi))
        return out

    def _qh_collect_guidance(qref: Optional[pd.Timestamp]) -> List[Dict[str, Any]]:
        if qref is None:
            return []

        cands: List[Dict[str, Any]] = []

        def _src_date(v: Any, fallback: Optional[pd.Timestamp] = None) -> pd.Timestamp:
            t = pd.to_datetime(v, errors="coerce")
            if pd.notna(t):
                return pd.Timestamp(t)
            return pd.Timestamp(fallback) if fallback is not None else pd.Timestamp("1900-01-01")

        def _doc_key(src: Dict[str, Any]) -> str:
            return "|".join(
                [
                    str(src.get("accn") or ""),
                    str(src.get("doc") or src.get("doc_path") or ""),
                    str(src.get("form") or ""),
                    str(src.get("filed") or ""),
                ]
            )

        _doc_text_cache: Dict[str, str] = {}

        def _resolve_doc_path(src: Dict[str, Any]) -> Optional[Path]:
            return _resolve_cached_doc_path(
                accn=src.get("accn"),
                doc_name=src.get("doc"),
                path_hint=src.get("doc_path") or src.get("doc"),
            )

        def _doc_plain_text(src: Dict[str, Any]) -> str:
            dp = _resolve_doc_path(src)
            if dp is None:
                return ""
            key = str(dp.resolve()) if dp.exists() else str(dp)
            if key in _doc_text_cache:
                return _doc_text_cache[key]
            plain = _qh_norm_txt(_read_cached_doc_text(dp))
            _doc_text_cache[key] = plain
            return plain

        def _expand_guidance_from_doc(src: Dict[str, Any], seed_text: str) -> str:
            plain = _doc_plain_text(src)
            if not plain:
                return ""
            plain_l = plain.lower()
            seeds: List[str] = []
            seed_norm = _qh_norm_txt(seed_text).lower()
            if seed_norm:
                seeds.append(seed_norm[:120])
            for s in [
                "full-year outlook",
                "full year outlook",
                "guidance and outlook",
                "provides the following guidance",
                "financial outlook",
            ]:
                if s not in seeds:
                    seeds.append(s)
            idx = -1
            for s in seeds:
                if not s:
                    continue
                j = plain_l.find(s)
                if j >= 0:
                    idx = j
                    break
            if idx < 0:
                return ""
            win = plain[max(0, idx - 120): min(len(plain), idx + 2200)]
            m_stop = re.search(r"\b(forward-looking statements|about pitney bowes|earnings conference call)\b", win, re.I)
            if m_stop:
                win = win[: m_stop.start()]
            return _qh_norm_txt(win)

        def _add_candidate(text: Any, category: str, src: Dict[str, Any], heading: Any = "", allow_expand: bool = True) -> None:
            txt = _qh_norm_txt(text)
            if not txt:
                return
            src = dict(src or {})
            heading_txt = _qh_norm_txt(heading or src.get("section") or src.get("section_or_page") or "")
            form_txt = str(src.get("form") or "")
            if not glx_is_preferred_section(form=form_txt, heading=heading_txt, text=txt):
                return
            score_info = glx_score_chunk(
                text=txt,
                heading=heading_txt,
                source_type=str(src.get("source_type") or ""),
                form=form_txt,
                doc_name=str(src.get("doc") or src.get("doc_path") or ""),
                category=category,
            )
            if bool(score_info.get("hard_exclude")):
                return
            score_val = float(score_info.get("score") or 0.0)
            has_table_like_guidance = bool(
                re.search(
                    r"\blow\b.{0,20}\bhigh\b.{0,220}\b(revenue|adjusted\s+ebit|adjusted\s+eps|free\s+cash\s+flow|fcf|capex)\b",
                    txt,
                    re.I,
                )
                and re.search(r"\$?\s*[0-9]{1,4}(?:,[0-9]{3})", txt)
            )
            if score_val < 35.0 and not has_table_like_guidance:
                return
            if has_table_like_guidance and score_val < 45.0:
                score_val = 45.0
            doc_type = str(score_info.get("doc_type_canon") or "other")
            doc_pri = int(score_info.get("doc_priority") or glx_doc_type_priority(doc_type))
            src["source_type"] = doc_type
            src["doc_type"] = doc_type
            src["doc_priority"] = doc_pri
            asof_q_end = str(pd.Timestamp(qref).date()) if qref is not None else ""
            src_doc_end = pd.to_datetime(src.get("source_doc_end"), errors="coerce")
            source_doc_end_txt = str(src_doc_end.date()) if pd.notna(src_doc_end) else asof_q_end
            source_filed_dt = _src_date(src.get("filed"), qref)
            period_label_hint, period_norm_hint = glx_normalize_period(
                txt,
                pd.Timestamp(qref).date() if qref is not None else None,
            )
            referenced_years = sorted({int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", txt)})
            src_rank = max(0, 100 - doc_pri)
            cands.append(
                {
                    "text": txt,
                    "score": score_val,
                    "source_rank": src_rank,
                    "source_priority": doc_pri,
                    "source_date": _src_date(src.get("filed"), qref),
                    "source": src,
                    "category": str(category or ""),
                    "heading": heading_txt,
                    "analysis": score_info,
                    "as_of_quarter_end": asof_q_end,
                    "source_doc_end": source_doc_end_txt,
                    "source_filed_date": source_filed_dt,
                    "first_seen_quarter_end": asof_q_end,
                    "last_seen_quarter_end": asof_q_end,
                    "referenced_years": referenced_years,
                    "has_forward_intent": bool(score_info.get("intent_hits") or []),
                    "has_period_anchor": bool(score_info.get("period_hits") or []),
                    "target_period_norm": str(period_norm_hint or "UNK"),
                    "target_period_label": str(period_label_hint or ""),
                    "guidance_type": "text",
                }
            )
            if allow_expand and re.search(r"\b(full[- ]year\s+outlook|guidance\s+and\s+outlook|provides?\s+the\s+following\s+guidance)\b", txt, re.I):
                has_numeric = bool(list(score_info.get("numeric_hits") or []))
                if not has_numeric:
                    expanded = _expand_guidance_from_doc(src, txt)
                    if expanded and glx_dedup_text_key(expanded) != glx_dedup_text_key(txt):
                        _add_candidate(
                            text=expanded,
                            category=category,
                            src=src,
                            heading=heading_txt,
                            allow_expand=False,
                        )

        if promises is not None and not promises.empty:
            d = _promises_view()
            q_col = "_quarter"
            t_col = _resolve_col(d, ["statement", "promise_text", "evidence_snippet"])
            cat_col = _resolve_col(d, ["category", "tag", "topic"])
            if t_col:
                for _, rr in d.iterrows():
                    if not _qh_date_eq(rr.get(q_col), qref):
                        continue
                    ev = _qh_parse_json(rr.get("source_evidence_json") or rr.get("evidence_history_json"))
                    src = {
                        "source_type": rr.get("source_type") or ev.get("source_type") or "promise",
                        "accn": rr.get("accn") or ev.get("accn"),
                        "form": rr.get("form") or ev.get("form"),
                        "filed": rr.get("filed") or ev.get("filed"),
                        "doc": rr.get("doc") or rr.get("doc_path") or ev.get("doc_path"),
                        "doc_path": rr.get("doc_path") or ev.get("doc_path"),
                        "page": rr.get("page") or ev.get("page"),
                        "section": rr.get("section") or rr.get("section_or_page") or ev.get("section") or ev.get("section_or_page"),
                        "source_doc_end": rr.get(q_col),
                    }
                    cat = str(rr.get(cat_col) or "") if cat_col else ""
                    _add_candidate(
                        text=rr.get(t_col),
                        category=cat,
                        src=src,
                        heading=src.get("section") or cat,
                    )

        if slides_guidance is not None and not slides_guidance.empty:
            d = slides_guidance.copy()
            q_col = _resolve_col(d, ["quarter", "quarter_end"])
            line_col = _resolve_col(d, ["line", "text"])
            nums_col = _resolve_col(d, ["numbers"])
            doc_col = _resolve_col(d, ["doc", "doc_path"])
            page_col = _resolve_col(d, ["page"])
            heading_col = _resolve_col(d, ["heading", "title", "section"])
            if q_col and line_col:
                for _, rr in d.iterrows():
                    if not _qh_date_eq(rr.get(q_col), qref):
                        continue
                    txt = _qh_norm_txt(f"{rr.get(line_col) or ''} {rr.get(nums_col) or ''}")
                    src = {
                        "source_type": rr.get("source_type") or rr.get("source") or "slides",
                        "accn": rr.get("accn"),
                        "form": rr.get("form"),
                        "filed": rr.get("filed"),
                        "doc": rr.get(doc_col) if doc_col else rr.get("doc"),
                        "doc_path": rr.get("doc_path"),
                        "page": rr.get(page_col) if page_col else rr.get("page"),
                        "section": rr.get(heading_col) if heading_col else rr.get("section"),
                        "source_doc_end": rr.get(q_col),
                    }
                    _add_candidate(
                        text=txt,
                        category="guidance",
                        src=src,
                        heading=src.get("section"),
                    )

        if quarter_notes is not None and not quarter_notes.empty:
            d = _quarter_notes_view()
            q_col = "_quarter"
            t_col = _resolve_col(d, ["claim", "headline", "note", "body", "evidence_snippet"])
            cat_col = _resolve_col(d, ["category", "tag", "topic"])
            if t_col:
                for _, rr in d.iterrows():
                    if not _qh_date_eq(rr.get(q_col), qref):
                        continue
                    ev = _qh_parse_json(rr.get("evidence_json"))
                    cat = str(rr.get(cat_col) or "") if cat_col else ""
                    src = {
                        "source_type": rr.get("source_type") or rr.get("method") or ev.get("source_type") or "quarter_notes",
                        "accn": rr.get("accn") or ev.get("accn"),
                        "form": rr.get("form") or ev.get("form"),
                        "filed": rr.get("filed") or ev.get("filed"),
                        "doc": rr.get("doc") or rr.get("doc_path") or ev.get("doc_path"),
                        "doc_path": rr.get("doc_path") or ev.get("doc_path"),
                        "page": rr.get("page") or rr.get("section_or_page") or ev.get("page"),
                        "section": rr.get("section_or_page") or ev.get("section") or ev.get("section_or_page"),
                        "source_doc_end": rr.get(q_col),
                    }
                    _add_candidate(
                        text=rr.get(t_col),
                        category=cat,
                        src=src,
                        heading=src.get("section") or cat,
                    )

        # Direct SEC document pass for the selected quarter (captures EX-99 outlook tables
        # that may be shortened in Promise/Slides extracts).
        if audit is not None and not audit.empty:
            aq = _audit_view()
            q_col = "_quarter"
            accn_col = _resolve_col(aq, ["accn"])
            form_col = _resolve_col(aq, ["form"])
            filed_col = _resolve_col(aq, ["filed"])
            if accn_col:
                aq_q = aq[aq[q_col].apply(lambda x: _qh_date_eq(x, qref))]
                if form_col:
                    aq_q = aq_q[aq_q[form_col].astype(str).str.upper().str.startswith(("8-K", "10-Q", "10-K"))]
                for _, rr in aq_q.dropna(subset=[accn_col]).iterrows():
                    accn = str(rr.get(accn_col) or "").strip()
                    if not accn:
                        continue
                    for dp in _sec_docs_for_accession(accn)[:16]:
                        name_l = dp.name.lower()
                        if not re.search(r"(ex99|ex-99|earnings|press|letter|presentation|pbi-|10q|10k)", name_l):
                            continue
                        src = {
                            "source_type": "sec_doc",
                            "accn": accn,
                            "form": rr.get(form_col) if form_col else "",
                            "filed": rr.get(filed_col) if filed_col else None,
                            "doc": dp.name,
                            "doc_path": str(dp),
                            "section": "guidance_window",
                            "source_doc_end": rr.get(q_col) if q_col else qref,
                        }
                        txt = _doc_plain_text(src)
                        if not txt:
                            continue
                        pats = [
                            re.compile(r"full[- ]year\s+outlook", re.I),
                            re.compile(r"guidance\s+and\s+outlook", re.I),
                            re.compile(r"provides?\s+the\s+following\s+guidance", re.I),
                            re.compile(r"financial\s+outlook", re.I),
                        ]
                        for ptn in pats:
                            n_seen = 0
                            for mm in ptn.finditer(txt):
                                n_seen += 1
                                if n_seen > 2:
                                    break
                                win = txt[max(0, mm.start() - 160): min(len(txt), mm.end() + 2400)]
                                _add_candidate(
                                    text=win,
                                    category="guidance",
                                    src=src,
                                    heading="guidance_window",
                                )

        if not cands:
            return []

        # Keep top N per document for debug and deterministic ordering.
        top_per_doc: Dict[str, List[Dict[str, Any]]] = {}
        for cand in sorted(
            cands,
            key=lambda z: (
                -float(z.get("score") or 0.0),
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                -int(z.get("source_priority") or 0),
            ),
        ):
            dkey = _doc_key(dict(cand.get("source") or {}))
            bucket = top_per_doc.setdefault(dkey, [])
            if len(bucket) < 50:
                bucket.append(cand)

        reduced = [row for rows in top_per_doc.values() for row in rows]

        def _better(cur: Dict[str, Any], old: Dict[str, Any]) -> bool:
            cur_dt = pd.Timestamp(cur.get("source_date")) if cur.get("source_date") is not None else pd.Timestamp("1900-01-01")
            old_dt = pd.Timestamp(old.get("source_date")) if old.get("source_date") is not None else pd.Timestamp("1900-01-01")
            if cur_dt != old_dt:
                return cur_dt > old_dt
            cur_pri = int(cur.get("source_priority") or 0)
            old_pri = int(old.get("source_priority") or 0)
            if cur_pri != old_pri:
                return cur_pri > old_pri
            cur_score = float(cur.get("score") or 0.0)
            old_score = float(old.get("score") or 0.0)
            if abs(cur_score - old_score) > 1e-9:
                return cur_score > old_score
            return len(str(cur.get("text") or "")) > len(str(old.get("text") or ""))

        uniq: Dict[str, Dict[str, Any]] = {}
        for cand in reduced:
            key = glx_dedup_text_key(cand.get("text"))
            if not key:
                continue
            prev = uniq.get(key)
            if prev is None or _better(cand, prev):
                uniq[key] = cand

        return sorted(
            uniq.values(),
            key=lambda z: (
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                -int(z.get("source_priority") or 0),
                -float(z.get("score") or 0.0),
                -len(str(z.get("text") or "")),
            ),
        )


    def _qh_extract_guidance_items(cands: List[Dict[str, Any]], qref: Optional[pd.Timestamp]) -> List[Dict[str, Any]]:
        if not cands:
            return []
        qts = pd.Timestamp(qref) if qref is not None else None
        qdate = qts.date() if qts is not None else None
        qyear = int(qts.year) if qts is not None else None
        money_metrics = {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Operating cash flow", "Capex", "Cost savings", "Restructuring charges", "Net debt / leverage", "Net income"}
        ui_metrics = set(GUIDANCE_UI_METRIC_PRIORITY)
        forward_driver_re = re.compile(
            r"\b(revenue|sales|margin|ebit|ebitda|eps|cash\s*flow|fcf|capex|cost|savings|debt|leverage|liquidity|volume|pricing|demand|churn|retention|backlog|pipeline|restructuring)\b",
            re.I,
        )
        metric_local_keywords: Dict[str, List[str]] = {
            "Revenue": ["revenue", "sales", "top line"],
            "Adj EBITDA": ["adjusted ebitda", "adj ebitda", "ebitda"],
            "Adj EPS": ["adjusted eps", "adj eps", "eps", "earnings per share"],
            "FCF": ["free cash flow", "fcf"],
            "Capex": ["capex", "capital expenditures", "capital spending"],
            "Cost savings": ["cost savings", "run-rate savings", "annualized savings", "savings"],
            "Restructuring charges": ["restructuring", "transformation charges", "special items"],
            "Net debt / leverage": ["net leverage", "leverage", "net debt", "debt/ebitda"],
        }
        asof_q_txt = str(qts.date()) if qts is not None else ""
        def _referenced_years_list(text_in: Any) -> List[int]:
            try:
                return sorted({int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", str(text_in or ""))})
            except Exception:
                return []

        def _period_norm_to_target_period(period_norm: str) -> str:
            p = str(period_norm or "").strip()
            if not p or p == "UNK":
                return "unspecified"
            if p == "FY+1":
                return "next_fy"
            return p

        def _execution_window(sent_l: str) -> str:
            sl = str(sent_l or "")
            if re.search(r"\bremainder\b[^.]{0,80}\bover\s+the?\s*next\s+year\b", sl):
                return "over_next_year"
            if re.search(r"\bover\s+the?\s*next\s+year\b", sl):
                return "over_next_year"
            m_through = re.search(r"\bthrough\s+(20\d{2})\b", sl)
            if m_through:
                return f"through_{m_through.group(1)}"
            m_into = re.search(r"\binto\s+(20\d{2})\b", sl)
            if m_into:
                return f"into_{m_into.group(1)}"
            return "unspecified"

        def _explicit_period_for_metric(sent_l: str, metric_name: str) -> bool:
            sl = str(sent_l or "")
            kws = metric_local_keywords.get(str(metric_name or ""), [])
            metric_hit = (str(metric_name or "") in {"", FORWARD_NOTES_LABEL}) or any(k in sl for k in kws)
            if not metric_hit:
                return False
            if re.search(r"\b(?:fy|fiscal)\s*[-/]?\s*(?:20\d{2}|\d{2})\b", sl):
                return True
            if re.search(r"\bq[1-4](?:\s*[- ]?\s*(?:20\d{2}|\d{2}))?\b", sl):
                return True
            if re.search(r"\b(first|second|third|fourth)\s+quarter\b", sl):
                return True
            # Explicit year tied to period wording (not "into 2026" timing carry text).
            if re.search(r"\b(in|for)\s+20\d{2}\b", sl):
                return True
            return False

        def _guidance_meta(
            metric_name: str,
            sent_text: str,
            period_name_in: str,
            period_norm_in: str,
            kind: str,
            unit: str,
        ) -> Tuple[str, str, Dict[str, Any]]:
            sent_l = str(sent_text or "").lower()
            period_name_out = str(period_name_in or "")
            period_norm_out = str(period_norm_in or "UNK")
            exec_window = _execution_window(sent_l)
            explicit_period = _explicit_period_for_metric(sent_l, metric_name)
            has_run_rate = bool(
                re.search(r"\b(annualized|annualised|run[- ]?rate)\b", sent_l)
                and "cost" in sent_l
                and "saving" in sent_l
            )
            has_one_time = bool(
                re.search(r"\bone[- ]time\b[^.]{0,30}\b(charge|charges|cost|costs)\b", sent_l)
                or ("restructuring" in sent_l and "charge" in sent_l)
            )
            is_ratio = metric_name == "Net debt / leverage" or str(unit or "") in {"x", "bps"}
            if re.search(r"\btracking midpoint\b", sent_l):
                display_action = "tracking_midpoint"
            elif re.search(r"\btracking low end\b", sent_l):
                display_action = "tracking_low_end"
            elif re.search(r"\b(reaffirm|reaffirmed|reaffirms|maintain|maintained|unchanged)\b", sent_l):
                display_action = "reaffirmed"
            elif re.search(r"\b(raise|raised|raising|increase|increased|increasing)\b", sent_l):
                display_action = "increasing"
            elif re.search(r"\b(lower|lowered|lowering|decrease|decreased|decreasing|reduce|reduced|reducing)\b", sent_l):
                display_action = "decreasing"
            elif re.search(r"\b(tighten|tightened|tightening)\b", sent_l):
                display_action = "tightening"
            elif re.search(r"\b(update|updated)\b", sent_l):
                display_action = "updated"
            else:
                display_action = ""

            # Critical period rule: do not force FY+1 from generic timing text.
            if qyear is not None and period_norm_out == f"FY{qyear + 1}":
                has_next_year_phrase = bool(re.search(r"\b(next fiscal year|next year)\b", sent_l))
                if has_next_year_phrase and not explicit_period:
                    period_name_out = "Next FY"
                    period_norm_out = "FY+1"

            # Cost-savings run-rate / implementation windows should not auto-map to FY+1.
            if metric_name == "Cost savings" and not explicit_period and (has_run_rate or exec_window != "unspecified"):
                if period_norm_out.startswith("FY"):
                    period_name_out = ""
                    period_norm_out = "UNK"

            if metric_name == FORWARD_NOTES_LABEL:
                guidance_type = "text"
            elif has_one_time:
                guidance_type = "one-time"
            elif is_ratio:
                guidance_type = "ratio"
            elif has_run_rate:
                guidance_type = "run-rate"
            elif exec_window != "unspecified" and not explicit_period:
                guidance_type = "ongoing"
            elif explicit_period or str(period_norm_out) not in {"", "UNK"}:
                guidance_type = "period"
            else:
                guidance_type = "text"

            if guidance_type == "run-rate":
                target_type = "run_rate"
            elif guidance_type == "one-time":
                target_type = "one_time"
            elif guidance_type == "ratio":
                target_type = "ratio"
            elif guidance_type == "period":
                if str(period_norm_out).startswith("Q"):
                    target_type = "quarterly"
                else:
                    target_type = "annual"
            else:
                target_type = "text_only"

            m_end = re.search(r"\bby\s+(?:the\s+)?end\s+of\s+(20\d{2})\b", sent_l)
            target_period = _period_norm_to_target_period(period_norm_out)
            if m_end:
                target_period = f"by_end_{m_end.group(1)}"

            meta = {
                "guidance_type": guidance_type,
                "target_type": target_type,
                "target_period": target_period,
                "target_period_norm": str(period_norm_out or "UNK"),
                "execution_window": exec_window,
                "display_action": display_action,
                "as_of_quarter": asof_q_txt,
                "last_mentioned_quarter": asof_q_txt,
                "as_of_quarter_end": asof_q_txt,
                "first_seen_quarter_end": asof_q_txt,
                "last_seen_quarter_end": asof_q_txt,
                "referenced_years": _referenced_years_list(sent_text),
                "has_forward_intent": bool(_qh_forward_intent_re.search(str(sent_text or ""))),
                "has_period_anchor": bool(_qh_period_anchor_re.search(str(sent_text or ""))),
            }
            return period_name_out, period_norm_out, meta

        def _kind_rank(it: Dict[str, Any]) -> int:
            kind = str(it.get("kind") or "")
            if kind == "range":
                return 3
            if kind == "point":
                return 2
            if kind == "qualitative_range":
                return 1
            return 0

        def _coerce_metric(raw_metric: str) -> str:
            metric_name = str(raw_metric or "").strip()
            if metric_name in ui_metrics and metric_name != FORWARD_NOTES_LABEL:
                return metric_name
            if metric_name in {"", "Other", "Unknown"}:
                return FORWARD_NOTES_LABEL
            return metric_name if metric_name in GUIDANCE_UI_METRIC_PRIORITY else FORWARD_NOTES_LABEL

        def _is_historical_plan(sent: str, period_key: str) -> bool:
            if qyear is None:
                return False
            sent_l = sent.lower()
            years = [int(x) for x in re.findall(r"(?<!\d)(20\d{2})(?!\d)", sent_l)]
            if not years:
                return False
            latest_year = max(years)
            if latest_year >= qyear:
                return False
            if not (period_key.startswith("FY") or "plan" in sent_l or "target" in sent_l):
                return False
            if re.search(r"\b(reaffirm|reaffirmed|maintain|maintained|updated guidance|guidance)\b", sent_l):
                return False
            return True

        def _is_stale_period(period_key: str, sent: str) -> bool:
            if qts is None:
                return False
            p = str(period_key or "UNK")
            sent_l = str(sent or "").lower()
            if p == "UNK":
                return False
            mq = re.match(r"Q(20\d{2})Q([1-4])$", p)
            if mq:
                yy = int(mq.group(1))
                qq = int(mq.group(2))
                cur_q = ((int(qts.month) - 1) // 3) + 1
                cur_ord = int(qts.year) * 4 + cur_q
                p_ord = yy * 4 + qq
                # Drop stale quarter references older than one quarter unless explicitly reaffirmed.
                if p_ord <= cur_ord - 2 and not re.search(r"\b(reaffirm|maintain|unchanged|updated guidance)\b", sent_l):
                    return True
                return False
            mfy = re.match(r"FY(20\d{2})$", p)
            if mfy:
                yy = int(mfy.group(1))
                if yy < int(qts.year) and not re.search(r"\b(reaffirm|maintain|unchanged)\b", sent_l):
                    return True
            return False

        def _is_stale_source_item(period_key: str, sent: str, src_date: Any) -> bool:
            if qts is None:
                return False
            sdt = pd.to_datetime(src_date, errors="coerce")
            if pd.isna(sdt):
                return False
            cur_ord = int(qts.year) * 4 + (((int(qts.month) - 1) // 3) + 1)
            src_ord = int(sdt.year) * 4 + (((int(sdt.month) - 1) // 3) + 1)
            age_q = cur_ord - src_ord
            if age_q <= 1:
                return False
            sent_l = str(sent or "").lower()
            if re.search(r"\b(reaffirm|reaffirmed|maintain|maintained|unchanged|updated guidance|we now expect)\b", sent_l):
                return False
            p = str(period_key or "UNK")
            mfy = re.match(r"FY(20\d{2})$", p)
            if mfy:
                fy = int(mfy.group(1))
                if re.search(rf"\b{fy}\b", sent_l):
                    return False
                # Drop stale "next year" carry-overs unless explicitly refreshed.
                if "next year" in sent_l or "next fiscal year" in sent_l:
                    return True
                return age_q >= 2
            # Unknown/quarter text from much older filings is usually noise.
            return age_q >= 3

        def _letters_count(s: str) -> int:
            return len(re.findall(r"[A-Za-z]", str(s or "")))

        def _alpha_ratio(s: str) -> float:
            txt = str(s or "")
            if txt == "":
                return 0.0
            return float(_letters_count(txt)) / float(max(1, len(txt)))

        def _long_word_count(s: str) -> int:
            return len(re.findall(r"\b[A-Za-z]{3,}\b", str(s or "")))

        def _normalize_forward_sentence(s: str) -> str:
            txt = _qh_norm_txt(s)
            txt = txt.replace(" ,", ",").replace(" .", ".")
            return txt.strip()

        def _looks_like_table_header(s: str) -> bool:
            txt = _normalize_forward_sentence(s).lower()
            if not txt:
                return True
            has_verb = bool(
                re.search(
                    r"\b(expect|expects|guidance|outlook|forecast|target|targets|will|reaffirm|maintain|raised|lowered|increase|decrease)\b",
                    txt,
                )
            )
            if has_verb:
                return False
            if re.search(r"\blow\b[^.]{0,40}\bhigh\b", txt) and re.search(
                r"\b(revenue|ebitda|eps|fcf|capex|cash flow|cost savings)\b", txt
            ):
                return True
            if re.search(r"\$\s*change|%\s*change", txt) and len(re.findall(r"\b20\d{2}\b", txt)) >= 2:
                return True
            if re.search(r"\bfourth quarter\b|\bthree months ended\b", txt) and re.search(
                r"\b20\d{2}\b[^.]{0,24}\b20\d{2}\b",
                txt,
            ):
                return True
            return False

        def _metric_keyword_near(metric_name: str, sent_txt: str, span: Optional[Tuple[int, int]]) -> bool:
            kws = metric_local_keywords.get(str(metric_name) or "", [])
            if not kws:
                return True
            local = str(sent_txt or "").lower()
            if span is not None:
                s0 = max(0, int(span[0]) - 100)
                s1 = min(len(local), int(span[1]) + 100)
                local = local[s0:s1]
            return any(kw in local for kw in kws)

        def _closest_metric_for_span(sent_txt: str, span: Optional[Tuple[int, int]]) -> Optional[str]:
            if span is None:
                return None
            local = str(sent_txt or "").lower()
            if not local:
                return None
            center = (int(span[0]) + int(span[1])) // 2
            best_metric: Optional[str] = None
            best_dist = 10**9
            for mname, kws in metric_local_keywords.items():
                for kw in kws:
                    for mm in re.finditer(re.escape(kw), local):
                        d = abs(((mm.start() + mm.end()) // 2) - center)
                        if d < best_dist:
                            best_dist = d
                            best_metric = mname
            # Keep remap strict so we do not overfit weak proximity.
            if best_metric is not None and best_dist <= 110:
                return best_metric
            return None

        def _valid_forward_sentence(s: str) -> bool:
            txt = _normalize_forward_sentence(s)
            if txt == "":
                return False
            if _alpha_ratio(txt) < 0.35:
                return False
            letters = _letters_count(txt)
            if letters < 25 and _long_word_count(txt) < 6:
                return False
            return True

        def _recover_forward_context(
            all_sentences: List[str],
            sent_idx: int,
            metric_guess: str,
            numeric_tokens: List[str],
            heading: str,
            src: Dict[str, Any],
            cat: str,
        ) -> Optional[str]:
            base = _normalize_forward_sentence(all_sentences[sent_idx] if 0 <= sent_idx < len(all_sentences) else "")
            if base == "":
                return None
            local_best: Optional[Tuple[float, str, int]] = None
            for j in range(max(0, sent_idx - 2), min(len(all_sentences), sent_idx + 3)):
                cand_txt = _normalize_forward_sentence(all_sentences[j])
                if cand_txt == "":
                    continue
                scj = glx_score_chunk(
                    text=cand_txt,
                    heading=heading,
                    source_type=str(src.get("source_type") or ""),
                    form=str(src.get("form") or ""),
                    doc_name=str(src.get("doc") or src.get("doc_path") or ""),
                    category=cat,
                )
                if bool(scj.get("hard_exclude")):
                    continue
                has_intent = bool(scj.get("intent_hits") or [])
                cand_l = cand_txt.lower()
                numeric_match = any(tok and tok.lower() in cand_l for tok in numeric_tokens)
                metric_match = (
                    metric_guess not in {"", FORWARD_NOTES_LABEL, "Other", "Unknown"}
                    and metric_guess.lower().split("/")[0].strip() in cand_l
                )
                if not (numeric_match or metric_match or has_intent):
                    continue
                bonus = 0.0
                if has_intent:
                    bonus += 8.0
                if numeric_match:
                    bonus += 4.0
                if metric_match:
                    bonus += 2.0
                cand_score = float(scj.get("score") or 0.0) + bonus
                if local_best is None or cand_score > local_best[0]:
                    local_best = (cand_score, cand_txt, j)
            if local_best is None:
                return base if _valid_forward_sentence(base) else None
            best_text = local_best[1]
            best_idx = int(local_best[2])
            if len(best_text) < 80 and best_idx + 1 < len(all_sentences):
                nxt = _normalize_forward_sentence(all_sentences[best_idx + 1])
                if nxt:
                    scn = glx_score_chunk(
                        text=nxt,
                        heading=heading,
                        source_type=str(src.get("source_type") or ""),
                        form=str(src.get("form") or ""),
                        doc_name=str(src.get("doc") or src.get("doc_path") or ""),
                        category=cat,
                    )
                    if not bool(scn.get("hard_exclude")) and float(scn.get("score") or 0.0) >= 20.0:
                        joined = _normalize_forward_sentence(f"{best_text} {nxt}")
                        if len(joined) <= 420:
                            best_text = joined
            return best_text if _valid_forward_sentence(best_text) else None

        def _extract_table_like_hits(sent: str) -> List[Dict[str, Any]]:
            out_hits: List[Dict[str, Any]] = []
            if not sent:
                return out_hits
            sl = sent.lower()
            if not re.search(r"\blow\b.{0,20}\bhigh\b", sl):
                return out_hits
            work = sent
            m_lh = re.search(r"\blow\b.{0,20}\bhigh\b", work, re.I)
            if m_lh:
                work = work[m_lh.end():]
            metric_map = [
                ("Revenue", r"revenue|sales|top line"),
                ("Adj EBIT", r"adjusted\s+ebit|adj\.?\s+ebit"),
                ("Adj EPS", r"adjusted\s+eps|adj\.?\s+eps|earnings\s+per\s+share|eps"),
                ("FCF", r"free\s+cash\s+flow|\bfcf\b"),
                ("Capex", r"capex|capital expenditures|capital spending"),
            ]

            def _parse_num(raw: Any) -> Optional[float]:
                try:
                    return float(str(raw or "").replace(",", ""))
                except Exception:
                    return None

            for metric_name, mpat in metric_map:
                pat = re.compile(
                    rf"(?:{mpat})[\s:\-,$()%]{{0,16}}\$?\s*([0-9]{{1,4}}(?:,[0-9]{{3}})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|bps|x)?"
                    rf"[\s:\-,$()%]{{0,12}}\$?\s*([0-9]{{1,4}}(?:,[0-9]{{3}})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|bps|x)?",
                    re.I,
                )
                for mm in pat.finditer(work):
                    lo = _parse_num(mm.group(1))
                    hi = _parse_num(mm.group(3))
                    if lo is None or hi is None:
                        continue
                    unit = str(mm.group(2) or mm.group(4) or "").lower()
                    if unit in {"bn", "billion"}:
                        lo *= 1e9
                        hi *= 1e9
                        unit_out = "$m"
                    elif unit in {"m", "million"}:
                        lo *= 1e6
                        hi *= 1e6
                        unit_out = "$m"
                    elif unit in {"%", "bps", "x"}:
                        unit_out = unit
                    else:
                        if metric_name == "Adj EPS":
                            unit_out = "$"
                        else:
                            lo *= 1e6 if abs(lo) < 10000 else 1.0
                            hi *= 1e6 if abs(hi) < 10000 else 1.0
                            unit_out = "$m"
                    if hi < lo:
                        lo, hi = hi, lo
                    out_hits.append(
                        {
                            "kind": "range",
                            "metric_canon": metric_name,
                            "value_low": float(lo),
                            "value_high": float(hi),
                            "value_mid": (float(lo) + float(hi)) / 2.0,
                            "value_point": None,
                            "unit": unit_out,
                            "span": (mm.start(), mm.end()),
                            "qualitative_range_text": None,
                            "raw_text": str(mm.group(0) or ""),
                        }
                    )
            return out_hits

        items: List[Dict[str, Any]] = []
        for cand in cands:
            txt = _qh_norm_txt(cand.get("text"))
            if not txt:
                continue
            src = dict(cand.get("source") or {})
            c_score = float(cand.get("score") or 0.0)
            c_rank = int(cand.get("source_rank") or 9)
            c_priority = int(cand.get("source_priority") or 0)
            c_date = pd.Timestamp(cand.get("source_date")) if cand.get("source_date") is not None else pd.Timestamp("1900-01-01")
            c_asof_q = str(cand.get("as_of_quarter_end") or asof_q_txt)
            c_source_doc_end = str(cand.get("source_doc_end") or src.get("source_doc_end") or c_asof_q)
            c_source_filed = pd.to_datetime(cand.get("source_filed_date"), errors="coerce")
            if pd.isna(c_source_filed):
                c_source_filed = c_date
            c_first_seen = str(cand.get("first_seen_quarter_end") or c_asof_q)
            c_last_seen = str(cand.get("last_seen_quarter_end") or c_asof_q)
            cat = str(cand.get("category") or "")
            heading = str(cand.get("heading") or src.get("section") or "")
            src_type_l = str(src.get("source_type") or "").lower()
            heading_l = str(heading or "").lower()
            sentences = glx_split_sentences(txt)
            if not sentences:
                sentences = [txt]
            for sent_idx, sent in enumerate(sentences):
                sc = glx_score_chunk(
                    text=sent,
                    heading=heading,
                    source_type=str(src.get("source_type") or ""),
                    form=str(src.get("form") or ""),
                    doc_name=str(src.get("doc") or src.get("doc_path") or ""),
                    category=cat,
                )
                if bool(sc.get("hard_exclude")):
                    continue
                sent_score = float(sc.get("score") or 0.0)
                if sent_score < 35.0:
                    continue
                sent_low = str(sent).lower()
                if _looks_like_table_header(sent):
                    continue
                if re.search(r"\b(not\s+anticipated\s+to\s+be\s+material|not\s+expected\s+to\s+be\s+material)\b", sent_low):
                    continue
                if re.search(r"\bprovides?\s+the\s+following\s+guidance\s+for\b", sent_low) and not re.search(r"\$|%|\bbps\b|\bto\b\s+\$|\bbetween\b\s+\$", sent_low):
                    continue
                is_retrospective = bool(
                    re.search(r"\b(compared\s+to|versus|vs\.?|decreased|declined|increased|grew)\b", sent_low)
                    and not re.search(r"\b(expect|expects|guidance|outlook|forecast|target|plan|will|anticipate|next|future)\b", sent_low)
                )
                if is_retrospective:
                    continue
                period_name, period_norm = glx_normalize_period(sent, qdate)
                if _is_historical_plan(sent, period_norm):
                    continue
                if _is_stale_period(period_norm, sent):
                    continue
                if _is_stale_source_item(period_norm, sent, c_date):
                    continue
                if (
                    period_norm == "UNK"
                    and qyear is not None
                    and re.search(r"\b(guidance|outlook|target|forecast)\b", sent_low)
                ):
                    period_name = f"FY {qyear}"
                    period_norm = f"FY{qyear}"
                numeric_hits = list(sc.get("numeric_hits") or glx_extract_numeric_patterns(sent))
                if not numeric_hits:
                    numeric_hits = _extract_table_like_hits(sent)
                saw_numeric_candidate = bool(numeric_hits)
                found_numeric = False
                for hit in numeric_hits:
                    metric_guess = str(hit.get("metric_canon") or sc.get("metric_hint") or glx_classify_metric(sent, hit.get("span")))
                    metric_name = _coerce_metric(metric_guess)
                    hit_span = hit.get("span")
                    near_metric = _closest_metric_for_span(sent, hit_span)
                    if near_metric and near_metric in ui_metrics:
                        metric_name = near_metric
                        metric_guess = near_metric
                    if "cost savings" in sent_low and metric_name == "Revenue":
                        metric_name = "Cost savings"
                    if "restructuring" in sent_low and metric_name == "Revenue":
                        metric_name = "Restructuring charges"
                    kind = str(hit.get("kind") or "point")
                    low = hit.get("value_low")
                    high = hit.get("value_high")
                    value = hit.get("value_point")
                    unit = str(hit.get("unit") or "")
                    raw_hit = str(hit.get("raw_text") or "")
                    if metric_name != FORWARD_NOTES_LABEL and not _metric_keyword_near(metric_name, sent, hit_span):
                        if metric_name not in {"Adj EPS", "Net debt / leverage"}:
                            continue
                    if metric_name == FORWARD_NOTES_LABEL and kind in {"range", "point"}:
                        raw_l = raw_hit.lower()
                        has_hint = bool(unit) or any(k in raw_l for k in ["$", "%", "bps", "x", "million", "billion", " bn", " m"])
                        if not has_hint:
                            continue
                        has_intent = bool(sc.get("intent_hits") or [])
                        has_anchor = "anchor_heading" in list(sc.get("reasons") or [])
                        if not (has_intent or has_anchor):
                            continue
                        numeric_tokens = re.findall(r"\$?\d[\d,]*(?:\.\d+)?%?", raw_hit or sent)
                        forward_text = _recover_forward_context(
                            all_sentences=sentences,
                            sent_idx=sent_idx,
                            metric_guess=metric_guess,
                            numeric_tokens=numeric_tokens,
                            heading=heading,
                            src=src,
                            cat=cat,
                        )
                        if not forward_text:
                            continue
                    else:
                        forward_text = _normalize_forward_sentence(sent)
                        if kind == "point" and metric_name != FORWARD_NOTES_LABEL:
                            if re.search(r"\blow\b[^.]{0,40}\bhigh\b", sent_low) and not re.search(
                                r"\b(expect|guidance|outlook|forecast|target|targets|reaffirm|raise|lower|maintain)\b",
                                sent_low,
                            ):
                                continue
                    if metric_guess == "Adj EPS":
                        vals = [x for x in [low, high, value] if x is not None]
                        if any(abs(float(v)) > 25 for v in vals):
                            # Table parsing can occasionally map revenue ranges to Adj EPS in dense "Low/High" blocks.
                            remap_metric = _closest_metric_for_span(sent, hit_span)
                            if remap_metric in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"}:
                                metric_guess = remap_metric
                                metric_name = remap_metric
                                if low is not None:
                                    low = float(low) * (1e6 if float(low) < 10000 else 1.0)
                                if high is not None:
                                    high = float(high) * (1e6 if float(high) < 10000 else 1.0)
                                if value is not None:
                                    value = float(value) * (1e6 if float(value) < 10000 else 1.0)
                                unit = "$m"
                            else:
                                continue
                    if metric_guess in money_metrics and kind in {"range", "point"}:
                        mid = (
                            (float(low) + float(high)) / 2.0
                            if kind == "range" and low is not None and high is not None
                            else (float(value) if value is not None else None)
                        )
                        if mid is not None and (mid < 10_000_000 or mid > 10_000_000_000):
                            continue
                        # Local PDF/OCR rows can mix segment-performance rows (value/value/%change)
                        # into guidance candidates; require cleaner shape for those sources.
                        if src_type_l in {"earnings_release", "slides", "other"}:
                            num_tokens = re.findall(r"\$?\d[\d,]*(?:\.\d+)?%?", sent_low)
                            has_pct_token = any(str(t).endswith("%") for t in num_tokens)
                            if has_pct_token and len(num_tokens) >= 3 and not re.search(r"\b(low|high|between|range)\b", sent_low):
                                continue
                            if "segment" in heading_l and has_pct_token and metric_name in {"Revenue", "Adj EBITDA", "Adj EBIT"}:
                                continue
                    # Avoid mixing actual-quarter points into guidance unless sentence is clearly forward-looking.
                    if kind == "point" and period_norm == "UNK":
                        if not re.search(r"\b(guidance|outlook|target|expect|expects|forecast|plan|will|next|future|range|between)\b", sent_low):
                            continue
                    period_name_eff, period_norm_eff, meta = _guidance_meta(
                        metric_name=metric_name,
                        sent_text=sent,
                        period_name_in=period_name,
                        period_norm_in=period_norm,
                        kind=kind,
                        unit=unit,
                    )
                    items.append(
                        {
                            "metric": metric_name,
                            "metric_raw": metric_guess,
                            "period": period_name_eff,
                            "period_norm": period_norm_eff,
                            "kind": kind,
                            "low": None if low is None else float(low),
                            "high": None if high is None else float(high),
                            "value": None if value is None else float(value),
                            "unit": unit,
                            "qualitative_range_text": hit.get("qualitative_range_text"),
                            "text": forward_text[:1000],
                            "source": src,
                            "score": c_score + sent_score,
                            "source_rank": c_rank,
                            "source_priority": c_priority,
                            "source_date": c_date,
                            "asof": qref,
                            "analysis": sc,
                            "as_of_quarter_end": c_asof_q,
                            "source_doc_end": c_source_doc_end,
                            "source_filed_date": c_source_filed,
                            "first_seen_quarter_end": c_first_seen,
                            "last_seen_quarter_end": c_last_seen,
                            "referenced_years": _referenced_years_list(forward_text),
                            "has_forward_intent": bool(sc.get("intent_hits") or []) or bool(_qh_forward_intent_re.search(forward_text)),
                            "has_period_anchor": bool(sc.get("period_hits") or []) or bool(_qh_period_anchor_re.search(forward_text)),
                            "target_period_norm": str(period_norm_eff or "UNK"),
                            **meta,
                        }
                    )
                    found_numeric = True
                if found_numeric:
                    continue
                metric_guess = str(sc.get("metric_hint") or glx_classify_metric(sent, None))
                metric_name = _coerce_metric(metric_guess)
                if saw_numeric_candidate and metric_name != FORWARD_NOTES_LABEL:
                    # If numeric candidates existed but were rejected (sanity/quality), avoid text-only fallback noise.
                    continue
                if metric_name == FORWARD_NOTES_LABEL and sent_score < 45.0:
                    continue
                text_for_item = _normalize_forward_sentence(sent)
                has_status = bool(sc.get("status_hits") or [])
                has_intent = bool(sc.get("intent_hits") or [])
                has_numeric_tokens = bool(re.search(r"\$|%|\bbps\b|\bx\b|million|billion|\bbetween\b|\bto\b\s+\$", text_for_item.lower()))
                if metric_name != FORWARD_NOTES_LABEL:
                    if re.search(
                        r"\b(involuntary restructuring initiative|culture of continual improvement|operational outputs and speed of execution)\b",
                        text_for_item,
                        re.I,
                    ):
                        continue
                    if re.search(r"\bprovides?\s+the\s+following\s+guidance\s+for\b", text_for_item, re.I):
                        continue
                    if re.search(r"\b(not\s+anticipated\s+to\s+be\s+material|not\s+expected\s+to\s+be\s+material)\b", text_for_item, re.I):
                        continue
                    if re.search(
                        r"\b(involuntary\s+restructuring\s+initiative|the\s+2025\s+plan|culture\s+of\s+continual\s+improvement|"
                        r"operational\s+outputs\s+and\s+speed\s+of\s+execution|do\s+not\s+foresee\s+a\s+near-term\s+repeat)\b",
                        text_for_item,
                        re.I,
                    ):
                        continue
                    if not has_numeric_tokens and re.search(
                        r"\b(provides?\s+the\s+following\s+guidance\s+for|financial\s+guidance\s+for)\b",
                        text_for_item,
                        re.I,
                    ):
                        continue
                    if _looks_like_table_header(text_for_item):
                        continue
                    if metric_name in {"Cost savings", "Restructuring charges"} and not has_numeric_tokens:
                        continue
                    # Keep text-only metric rows only when they carry explicit signal.
                    if not (has_status or has_intent):
                        continue
                if metric_name == FORWARD_NOTES_LABEL:
                    has_anchor = "anchor_heading" in list(sc.get("reasons") or [])
                    if not (has_intent or has_anchor):
                        continue
                    if not forward_driver_re.search(text_for_item):
                        continue
                    if not _valid_forward_sentence(text_for_item):
                        forward_text = _recover_forward_context(
                            all_sentences=sentences,
                            sent_idx=sent_idx,
                            metric_guess=metric_guess,
                            numeric_tokens=re.findall(r"\$?\d[\d,]*(?:\.\d+)?%?", sent),
                            heading=heading,
                            src=src,
                            cat=cat,
                        )
                        if not forward_text:
                            continue
                        text_for_item = forward_text
                    if len(text_for_item) < 80 and sent_idx + 1 < len(sentences):
                        nxt = _normalize_forward_sentence(sentences[sent_idx + 1])
                        if nxt:
                            scn = glx_score_chunk(
                                text=nxt,
                                heading=heading,
                                source_type=str(src.get("source_type") or ""),
                                form=str(src.get("form") or ""),
                                doc_name=str(src.get("doc") or src.get("doc_path") or ""),
                                category=cat,
                            )
                            if not bool(scn.get("hard_exclude")) and float(scn.get("score") or 0.0) >= 20.0:
                                joined = _normalize_forward_sentence(f"{text_for_item} {nxt}")
                                if len(joined) <= 420:
                                    text_for_item = joined
                period_name_eff, period_norm_eff, meta = _guidance_meta(
                    metric_name=metric_name,
                    sent_text=text_for_item,
                    period_name_in=period_name,
                    period_norm_in=period_norm,
                    kind="text",
                    unit="",
                )
                items.append(
                    {
                        "metric": metric_name,
                        "metric_raw": metric_guess,
                        "period": period_name_eff,
                        "period_norm": period_norm_eff,
                        "kind": "text",
                        "low": None,
                        "high": None,
                        "value": None,
                        "unit": "",
                        "qualitative_range_text": None,
                        "text": text_for_item[:1000],
                        "source": src,
                        "score": c_score + sent_score,
                        "source_rank": c_rank,
                        "source_priority": c_priority,
                        "source_date": c_date,
                        "asof": qref,
                        "analysis": sc,
                        "as_of_quarter_end": c_asof_q,
                        "source_doc_end": c_source_doc_end,
                        "source_filed_date": c_source_filed,
                        "first_seen_quarter_end": c_first_seen,
                        "last_seen_quarter_end": c_last_seen,
                        "referenced_years": _referenced_years_list(text_for_item),
                        "has_forward_intent": bool(sc.get("intent_hits") or []) or bool(_qh_forward_intent_re.search(text_for_item)),
                        "has_period_anchor": bool(sc.get("period_hits") or []) or bool(_qh_period_anchor_re.search(text_for_item)),
                        "target_period_norm": str(period_norm_eff or "UNK"),
                        **meta,
                    }
                )

        if not items:
            return []

        def _item_key(it: Dict[str, Any]) -> str:
            metric_name = str(it.get("metric") or FORWARD_NOTES_LABEL)
            period_key = str(it.get("period_norm") or str(it.get("period") or "UNK"))
            guidance_type = str(it.get("guidance_type") or "")
            if period_key in {"", "UNK"} and guidance_type in {"run-rate", "ongoing", "one-time", "ratio"}:
                period_key = f"TYPE:{guidance_type}"
            quarter_key = str(pd.Timestamp(it.get("asof")).date()) if it.get("asof") is not None else "NA"
            if metric_name == FORWARD_NOTES_LABEL:
                return "|".join([quarter_key, metric_name, period_key, glx_dedup_text_key(it.get("text"))[:140]])
            return "|".join([quarter_key, metric_name, period_key])

        def _better(cur: Dict[str, Any], old: Dict[str, Any]) -> bool:
            cur_dt = pd.Timestamp(cur.get("source_date")) if cur.get("source_date") is not None else pd.Timestamp("1900-01-01")
            old_dt = pd.Timestamp(old.get("source_date")) if old.get("source_date") is not None else pd.Timestamp("1900-01-01")
            if cur_dt != old_dt:
                return cur_dt > old_dt
            cur_pri = int(cur.get("source_priority") or 0)
            old_pri = int(old.get("source_priority") or 0)
            if cur_pri != old_pri:
                return cur_pri > old_pri
            cur_kind = _kind_rank(cur)
            old_kind = _kind_rank(old)
            if cur_kind != old_kind:
                return cur_kind > old_kind
            cur_score = float(cur.get("score") or 0.0)
            old_score = float(old.get("score") or 0.0)
            if abs(cur_score - old_score) > 1e-9:
                return cur_score > old_score
            return len(str(cur.get("text") or "")) > len(str(old.get("text") or ""))

        dedup: Dict[str, Dict[str, Any]] = {}
        for it in items:
            key = _item_key(it)
            prev = dedup.get(key)
            if prev is None or _better(it, prev):
                dedup[key] = it

        rows = list(dedup.values())

        # Secondary dedup: collapse duplicated text rows that only differ by period parsing.
        by_metric_text: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for it in rows:
            mk = str(it.get("metric") or FORWARD_NOTES_LABEL)
            tk = glx_dedup_text_key(it.get("text"))
            key_mt = (mk, tk)
            prev = by_metric_text.get(key_mt)
            if prev is None:
                by_metric_text[key_mt] = it
                continue
            cur_has_period = str(it.get("period_norm") or "UNK") != "UNK"
            prev_has_period = str(prev.get("period_norm") or "UNK") != "UNK"
            if cur_has_period and not prev_has_period:
                by_metric_text[key_mt] = it
                continue
            if cur_has_period == prev_has_period and _better(it, prev):
                by_metric_text[key_mt] = it

        rows = list(by_metric_text.values())

        # Collapse duplicate numeric values (same metric/value, unknown-period copy loses to known period).
        by_metric_value: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        passthrough: List[Dict[str, Any]] = []
        for it in rows:
            metric_name = str(it.get("metric") or FORWARD_NOTES_LABEL)
            if metric_name == FORWARD_NOTES_LABEL:
                passthrough.append(it)
                continue
            kind = str(it.get("kind") or "")
            low = it.get("low")
            high = it.get("high")
            val = it.get("value")
            unit = str(it.get("unit") or "")
            if low is None and high is None and val is None:
                passthrough.append(it)
                continue
            key_mv = (
                metric_name,
                kind,
                unit,
                None if low is None else round(float(low), 4),
                None if high is None else round(float(high), 4),
                None if val is None else round(float(val), 4),
            )
            prev = by_metric_value.get(key_mv)
            if prev is None:
                by_metric_value[key_mv] = it
                continue
            cur_has_period = str(it.get("period_norm") or "UNK") != "UNK"
            prev_has_period = str(prev.get("period_norm") or "UNK") != "UNK"
            if cur_has_period and not prev_has_period:
                by_metric_value[key_mv] = it
                continue
            if cur_has_period == prev_has_period and _better(it, prev):
                by_metric_value[key_mv] = it

        rows = list(by_metric_value.values()) + passthrough

        # Remove obvious duplicate mis-classifications (e.g., Adj EBIT copied from Revenue range).
        rev_ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        for it in rows:
            if str(it.get("metric") or "") != "Revenue":
                continue
            if str(it.get("kind") or "") != "range":
                continue
            pk = str(it.get("period_norm") or "")
            rev_ranges[pk] = (
                None if it.get("low") is None else float(it.get("low")),
                None if it.get("high") is None else float(it.get("high")),
            )
        filtered_rows: List[Dict[str, Any]] = []
        for it in rows:
            if str(it.get("metric") or "") == "Adj EBIT" and str(it.get("kind") or "") == "range":
                pk = str(it.get("period_norm") or "")
                rv = rev_ranges.get(pk)
                if rv is not None:
                    lo = None if it.get("low") is None else float(it.get("low"))
                    hi = None if it.get("high") is None else float(it.get("high"))
                    if lo is not None and hi is not None and rv[0] is not None and rv[1] is not None:
                        if abs(lo - rv[0]) <= 1e-6 and abs(hi - rv[1]) <= 1e-6:
                            continue
            filtered_rows.append(it)
        rows = filtered_rows

        def _period_sort_key(period_norm: str) -> Tuple[int, int, int]:
            p = str(period_norm or "UNK")
            m_fy = re.match(r"FY(20\d{2})$", p)
            if m_fy:
                return (0, int(m_fy.group(1)), 0)
            m_q = re.match(r"Q(20\d{2})Q([1-4])$", p)
            if m_q:
                return (1, int(m_q.group(1)), int(m_q.group(2)))
            if p == "UNK":
                return (9, 0, 0)
            return (8, 0, 0)

        metric_order = {name: idx for idx, name in enumerate(GUIDANCE_UI_METRIC_PRIORITY)}
        rows_sorted = sorted(
            rows,
            key=lambda z: (
                metric_order.get(str(z.get("metric") or FORWARD_NOTES_LABEL), 99),
                _period_sort_key(str(z.get("period_norm") or "UNK")),
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                -int(z.get("source_priority") or 0),
                -float(z.get("score") or 0.0),
            ),
        )

        forward_rows = [x for x in rows_sorted if str(x.get("metric") or "") == FORWARD_NOTES_LABEL]
        core_rows = [x for x in rows_sorted if str(x.get("metric") or "") != FORWARD_NOTES_LABEL]
        forward_rows = sorted(
            forward_rows,
            key=lambda z: (
                -float(z.get("score") or 0.0),
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
            ),
        )[:3]
        return core_rows + forward_rows

    def _qh_sec_outlook_backfill(qref: Optional[pd.Timestamp]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if qref is None or audit is None or audit.empty:
            return out
        qts = pd.Timestamp(qref)
        aq = _audit_view()
        q_col = "_quarter"
        accn_col = _resolve_col(aq, ["accn"])
        form_col = _resolve_col(aq, ["form"])
        filed_col = _resolve_col(aq, ["filed"])
        if accn_col is None:
            return out
        aq_q = aq[aq[q_col].apply(lambda x: _qh_date_eq(x, qts))]
        if form_col is not None:
            aq_q = aq_q[aq_q[form_col].astype(str).str.upper().str.startswith(("8-K", "10-Q", "10-K"))]
        if aq_q.empty:
            return out

        cache_dir = cache_root
        if not cache_dir.exists():
            return out

        accn_meta: Dict[str, Dict[str, Any]] = {}
        for _, rr in aq_q.iterrows():
            accn = str(rr.get(accn_col) or "").strip()
            if not accn:
                continue
            accn_meta[accn] = {
                "form": rr.get(form_col) if form_col else "",
                "filed": rr.get(filed_col) if filed_col else None,
            }
        # Also pull same-quarter 8-K/EX99 filings from submissions (guidance often lives there).
        for fr in _submission_recent_rows(max_files=8):
            form = str(fr.get("form") or "").upper().strip()
            if not form.startswith(("8-K", "10-Q", "10-K")):
                continue
            accn = str(fr.get("accn") or "").strip()
            if not accn or accn in accn_meta:
                continue
            q_guess = _submission_recent_row_quarter(fr)
            if q_guess is None or pd.Timestamp(q_guess).date() != qts.date():
                continue
            accn_meta[accn] = {"form": form, "filed": parse_date(fr.get("filed"))}

        metric_map = [
            ("Revenue", r"revenue|sales|top line", "$m"),
            ("Adj EBIT", r"adjusted\s+ebit|adj\.?\s+ebit", "$m"),
            ("Adj EPS", r"adjusted\s+eps|adj\.?\s+eps|earnings\s+per\s+share|eps", "$"),
            ("FCF", r"free\s+cash\s+flow|\bfcf\b", "$m"),
            ("Capex", r"capex|capital expenditures|capital spending", "$m"),
            ("Cost savings", r"cost savings|run[- ]?rate savings|annualized savings", "$m"),
            ("Restructuring charges", r"restructuring charges?|transformation charges?", "$m"),
            ("Net debt / leverage", r"adjusted net leverage ratio|net leverage|debt/ebitda|leverage ratio", "x"),
        ]

        metric_aliases: List[Tuple[str, List[str]]] = [
            ("Revenue", ["revenue", "net revenue", "sales", "top line"]),
            ("Adj EBITDA", ["adjusted ebitda", "adj ebitda"]),
            ("Adj EBIT", ["adjusted ebit", "adj ebit", "adjusted operating income"]),
            ("Adj EPS", ["adjusted eps", "adj eps", "adjusted diluted earnings per share", "earnings per share", "eps"]),
            ("FCF", ["free cash flow", "fcf"]),
            ("Capex", ["capex", "capital expenditures", "capital spending"]),
            ("Cost savings", ["cost savings", "run-rate savings", "annualized savings"]),
            ("Restructuring charges", ["restructuring charges", "restructuring costs", "transformation charges"]),
            ("Net debt / leverage", ["adjusted net leverage ratio", "net leverage ratio", "debt/ebitda", "leverage ratio"]),
        ]

        def _canon_metric(label: Any) -> str:
            s = _qh_norm_txt(label).lower()
            if not s:
                return "Other"
            for mk, kws in metric_aliases:
                if any(kw in s for kw in kws):
                    return mk
            return "Other"

        def _parse_num(raw: Any) -> Optional[float]:
            try:
                txt = str(raw or "").strip()
                if not txt:
                    return None
                txt = txt.replace(",", "")
                txt = txt.replace("$", "")
                txt = txt.replace("(", "-").replace(")", "")
                return float(txt)
            except Exception:
                return None

        def _to_metric_unit(metric_name: str, value: float, unit_hint: str, row_text: str) -> Tuple[float, str]:
            uh = str(unit_hint or "").lower()
            rt = str(row_text or "").lower()
            if metric_name == "Adj EPS":
                return float(value), "$"
            if metric_name == "Net debt / leverage":
                return float(value), "x"
            if uh in {"%", "bps", "x"}:
                return float(value), uh
            if uh in {"bn", "billion"}:
                return float(value) * 1e9, "$m"
            if uh in {"m", "million"}:
                return float(value) * 1e6, "$m"
            if "billion" in rt or re.search(r"\bbn\b", rt):
                return float(value) * 1e9, "$m"
            if "million" in rt or re.search(r"\bm\b", rt):
                return float(value) * 1e6, "$m"
            # Default money scaling for guidance tables.
            if metric_name in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"}:
                if abs(float(value)) <= 10000:
                    return float(value) * 1e6, "$m"
                return float(value), "$m"
            return float(value), ""

        def _infer_period_for_row(metric_name: str, row_text: str, doc_text: str) -> Tuple[str, str]:
            rt = str(row_text or "").lower()
            dtxt = str(doc_text or "").lower()
            # Explicit period on the same row first.
            p_name, p_norm = glx_normalize_period(rt, qts.date())
            if p_norm != "UNK" and qts is not None:
                # Guard against money amounts being misread as years (e.g., 1,900 -> FY1900).
                m_fy_bad = re.match(r"FY(\d{4})$", str(p_norm))
                if m_fy_bad:
                    yy = int(m_fy_bad.group(1))
                    if yy < 2000 or abs(yy - int(qts.year)) > 2:
                        p_name, p_norm = "", "UNK"
                m_q_bad = re.match(r"Q(\d{4})Q([1-4])$", str(p_norm))
                if m_q_bad:
                    yy = int(m_q_bad.group(1))
                    if yy < 2000 or abs(yy - int(qts.year)) > 2:
                        p_name, p_norm = "", "UNK"
            if p_norm != "UNK":
                return p_name, p_norm
            # Carry-forward/run-rate timing phrases are not explicit FY labels.
            if metric_name == "Cost savings":
                has_run_rate = bool(re.search(r"\b(annualized|annualised|run[- ]?rate)\b", rt))
                has_timing_only = bool(re.search(r"\b(remainder|over\s+the?\s*next\s+year|into\s+20\d{2}|through\s+20\d{2})\b", rt))
                has_explicit_fy = bool(re.search(r"\b(?:fy|fiscal)\s*(20\d{2}|\d{2})\b", rt))
                has_explicit_in_year = bool(re.search(r"\b(in|for)\s+20\d{2}\b", rt))
                if (has_run_rate or has_timing_only) and not (has_explicit_fy or has_explicit_in_year):
                    return "", "UNK"
            # Then table-level/section-level explicit year.
            m_exp_fy = re.search(r"\b(?:fy|fiscal)\s*(20\d{2})\b", dtxt)
            if m_exp_fy:
                yy = int(m_exp_fy.group(1))
                return f"FY {yy}", f"FY{yy}"
            m_full = re.search(r"\bfull[- ]year\s+(20\d{2})\s+guidance\b", dtxt)
            if m_full:
                yy = int(m_full.group(1))
                return f"FY {yy}", f"FY{yy}"
            m_outlook = re.search(r"\b(20\d{2})\s+full[- ]year\s+outlook\b", dtxt)
            if m_outlook:
                yy = int(m_outlook.group(1))
                return f"FY {yy}", f"FY{yy}"
            m_in_guidance = re.search(r"\bguidance[^.]{0,140}\bin\s+(20\d{2})\b", dtxt)
            if m_in_guidance:
                yy = int(m_in_guidance.group(1))
                return f"FY {yy}", f"FY{yy}"
            # Current fiscal year default for current guidance tables.
            return f"FY {int(qts.year)}", f"FY{int(qts.year)}"

        def _parse_low_high_table_df(
            table_df: pd.DataFrame,
            context_text: str,
            *,
            method_name: str,
        ) -> List[Dict[str, Any]]:
            items_tbl: List[Dict[str, Any]] = []
            if table_df is None or table_df.empty:
                return items_tbl

            t = table_df.copy()
            t.columns = [_qh_norm_txt(c) for c in t.columns]
            # Some filings put "Low/High" in the first row instead of header.
            if not t.empty:
                first_row_vals = [_qh_norm_txt(v).lower() for v in t.iloc[0].tolist()]
                if any(re.search(r"\blow\b|\bminimum\b|\bmin\b", v) for v in first_row_vals) and any(
                    re.search(r"\bhigh\b|\bmaximum\b|\bmax\b", v) for v in first_row_vals
                ):
                    try:
                        t.columns = [_qh_norm_txt(v) for v in t.iloc[0].tolist()]
                        t = t.iloc[1:].reset_index(drop=True)
                    except Exception:
                        pass

            # Guidance table gating: must look like guidance/outlook context or explicit low/high+metric grid.
            sample_lines: List[str] = []
            for rr in range(min(len(t), 12)):
                vals = [str(t.iat[rr, cc]) for cc in range(t.shape[1])]
                sample_lines.append(_qh_norm_txt(" ".join(vals)))
            table_blob = _qh_norm_txt(f"{context_text} {' '.join(sample_lines)}").lower()
            has_anchor = bool(
                re.search(
                    r"\b(guidance|outlook|financial guidance|financial outlook|full[- ]year outlook|updated guidance|reaffirmed guidance)\b",
                    table_blob,
                )
            )
            has_low_high = bool(re.search(r"\blow\b|\bminimum\b|\bmin\b", table_blob) and re.search(r"\bhigh\b|\bmaximum\b|\bmax\b", table_blob))
            has_metric_tokens = bool(
                re.search(
                    r"\b(revenue|adjusted\s+ebitda|adjusted\s+ebit|adjusted\s+eps|earnings per share|free cash flow|fcf|capex|cost savings|restructuring|leverage)\b",
                    table_blob,
                )
            )
            if not has_anchor and not (has_low_high and has_metric_tokens):
                return items_tbl

            cands = {str(c).lower(): c for c in t.columns}
            low_col = next((orig for k, orig in cands.items() if re.search(r"\blow\b|\bminimum\b|\bmin\b", k)), None)
            high_col = next((orig for k, orig in cands.items() if re.search(r"\bhigh\b|\bmaximum\b|\bmax\b", k)), None)
            range_col = next((orig for k, orig in cands.items() if re.search(r"\brange\b", k)), None)

            metric_col = None
            if low_col is not None and high_col is not None:
                for c in t.columns:
                    if c in {low_col, high_col}:
                        continue
                    metric_col = c
                    break
            elif range_col is not None:
                for c in t.columns:
                    if c == range_col:
                        continue
                    metric_col = c
                    break
            if metric_col is None:
                return items_tbl

            range_pat = re.compile(
                r"\$?\s*([\(\-]?[0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x)?\s*(?:to|and|\-|–|—)\s*\$?\s*([\(\-]?[0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x)?",
                re.I,
            )

            for _, rrow in t.iterrows():
                row_vals = [rrow.get(c) for c in t.columns]
                row_text = _qh_norm_txt(
                    " ".join([str(x) for x in row_vals if str(x).strip().lower() not in {"", "nan", "none"}])
                )
                if not row_text:
                    continue
                if re.search(
                    r"\b(performance obligations|transaction price allocated|recognized as follows|securities act|registration exempt)\b",
                    row_text,
                    re.I,
                ):
                    continue
                if re.search(r"\b(fasb\.org|us-gaap| xbrl |axis|member|lineitems|taxonomy|dei:)\b", row_text, re.I):
                    continue

                metric_name = _canon_metric(_qh_norm_txt(rrow.get(metric_col)))
                if metric_name == "Other":
                    metric_name = _canon_metric(row_text)
                if metric_name == "Other":
                    continue

                lo = hi = None
                unit_hint = ""
                if low_col is not None and high_col is not None:
                    lo = _parse_num(rrow.get(low_col))
                    hi = _parse_num(rrow.get(high_col))
                    row_low_txt = str(rrow.get(low_col) or "")
                    row_hi_txt = str(rrow.get(high_col) or "")
                    if "%" in row_low_txt or "%" in row_hi_txt:
                        unit_hint = "%"
                    elif "x" in row_low_txt.lower() or "x" in row_hi_txt.lower():
                        unit_hint = "x"
                    elif re.search(r"\bbn|billion|million|\bm\b", f"{row_low_txt} {row_hi_txt}", re.I):
                        uh = re.search(r"\b(bn|billion|million|m)\b", f"{row_low_txt} {row_hi_txt}", re.I)
                        unit_hint = str(uh.group(1)) if uh else ""
                elif range_col is not None:
                    range_txt = str(rrow.get(range_col) or "")
                    mm = range_pat.search(range_txt)
                    if mm:
                        lo = _parse_num(mm.group(1))
                        hi = _parse_num(mm.group(3))
                        unit_hint = str(mm.group(2) or mm.group(4) or "")
                    else:
                        mm2 = range_pat.search(row_text)
                        if mm2:
                            lo = _parse_num(mm2.group(1))
                            hi = _parse_num(mm2.group(3))
                            unit_hint = str(mm2.group(2) or mm2.group(4) or "")
                if lo is None or hi is None:
                    continue
                is_year_like = 1900 <= abs(float(lo)) <= 2100 and 1900 <= abs(float(hi)) <= 2100
                if is_year_like:
                    money_hint = bool(
                        metric_name in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"}
                        and (
                            "$" in row_text
                            or re.search(r"\b(million|billion|bn|m)\b", row_text, re.I)
                            or bool(re.search(r"\$\s*millions?|in\s+millions?\b|except\s+eps", table_blob, re.I))
                        )
                    )
                    if not money_hint:
                        continue

                period_name, period_norm = _infer_period_for_row(metric_name, row_text, context_text)
                lo2, unit_out = _to_metric_unit(metric_name, lo, unit_hint, f"{context_text} {row_text}")
                hi2, unit_out2 = _to_metric_unit(metric_name, hi, unit_hint, f"{context_text} {row_text}")
                unit_final = unit_out if unit_out else unit_out2
                if metric_name == "Adj EPS" and max(abs(float(lo2)), abs(float(hi2))) > 20:
                    continue
                if unit_final == "%" and max(abs(float(lo2)), abs(float(hi2))) > 200:
                    continue
                if hi2 < lo2:
                    lo2, hi2 = hi2, lo2
                items_tbl.append(
                    {
                        "metric": metric_name,
                        "period": period_name,
                        "period_norm": period_norm,
                        "kind": "range",
                        "low": float(lo2),
                        "high": float(hi2),
                        "value": None,
                        "unit": unit_final,
                        "text": row_text,
                        "analysis": {"method": method_name},
                    }
                )
            return items_tbl

        def _extract_low_high_grid_items(plain_txt: str, *, method_name: str) -> List[Dict[str, Any]]:
            out_items: List[Dict[str, Any]] = []
            if not plain_txt:
                return out_items
            txt = _qh_norm_txt(plain_txt)
            if not re.search(r"\b(low|minimum|min)\b.{0,60}\b(high|maximum|max)\b", txt, re.I):
                return out_items

            anchor_re = re.compile(
                r"(?:full[- ]year\s+outlook|financial\s+outlook|current\s+financial\s+guidance|provides?\s+the\s+following\s+guidance|updated\s+guidance|reaffirmed\s+guidance)",
                re.I,
            )
            windows: List[str] = []
            for m in anchor_re.finditer(txt):
                s = max(0, m.start() - 160)
                e = min(len(txt), m.start() + 2600)
                windows.append(txt[s:e])
            if not windows:
                windows = [txt[:3000]]

            metric_alt = (
                r"Revenue|Net Revenue|Sales|Top Line|Adjusted EBITDA|Adj\.?\s*EBITDA|Adjusted EBIT|Adj\.?\s*EBIT|"
                r"Adjusted EPS|Adj\.?\s*EPS|Adjusted Diluted Earnings Per Share|Earnings Per Share|EPS|"
                r"Free Cash Flow|FCF|Capex|Capital Expenditures|Capital Spending|Cost Savings|Run[- ]?Rate Savings|"
                r"Annualized Savings|Restructuring Charges?|Transformation Charges?|Net Leverage(?: Ratio)?|Debt/EBITDA"
            )
            row_re = re.compile(rf"(?P<label>{metric_alt})\s*(?P<body>.*?)(?=(?:{metric_alt})\s|$)", re.I)
            range_re = re.compile(
                r"\$?\s*([\(\-]?[0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*"
                r"(?:to|and|\-|\u2013|\u2014)?\s*\$?\s*([\(\-]?[0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*"
                r"(bn|billion|m|million|%|x|bps)?",
                re.I,
            )
            def _cleanup_guidance_row_text(s: str) -> str:
                z = _qh_norm_txt(s).replace("&nbsp;", " ")
                z = re.split(
                    r"\*\*\*|earnings conference call|about pitney bowes|to read and/or download a copy",
                    z,
                    maxsplit=1,
                    flags=re.I,
                )[0]
                z = re.split(r"\bq[1-4]\s+20\d{2}\b", z, maxsplit=1, flags=re.I)[0]
                return _qh_norm_txt(z)
            seen_key: set[Tuple[str, str, float, float]] = set()
            money_metrics = {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"}

            for win in windows:
                win = re.split(r"\b(forward-looking statements|safe harbor|risk factors)\b", win, flags=re.I)[0]
                if not win:
                    continue
                low_high_anchor = re.search(r"\b(low|minimum|min)\b.{0,40}\b(high|maximum|max)\b", win, re.I)
                if low_high_anchor is None:
                    continue
                if not re.search(r"\b(guidance|outlook|financial guidance|financial outlook|full[- ]year)\b", win, re.I):
                    continue
                block = win[low_high_anchor.end() :]
                end_re = re.search(
                    r"\b(earnings conference call|about pitney bowes|use of non-gaap|forward-looking statements|contacts|business segment reporting|consolidated statements)\b",
                    block,
                    re.I,
                )
                if end_re is not None:
                    block = block[: end_re.start()]
                block = _qh_norm_txt(block)[:1800]
                if not block:
                    continue
                has_money_context = bool(re.search(r"\$\s*millions?|in\s+millions?\b|except\s+eps", win, re.I))

                for mm in row_re.finditer(block):
                    label = _qh_norm_txt(mm.group("label") or "")
                    body = _cleanup_guidance_row_text(mm.group("body") or "")
                    if not body:
                        continue
                    row_text = _cleanup_guidance_row_text(f"{label} {body}")
                    metric_name = _canon_metric(label)
                    if metric_name == "Other":
                        metric_name = _canon_metric(row_text)
                    if metric_name == "Other":
                        continue
                    rm = range_re.search(body)
                    if rm is None:
                        continue
                    lo_raw = _parse_num(rm.group(1))
                    hi_raw = _parse_num(rm.group(3))
                    if lo_raw is None or hi_raw is None:
                        continue
                    is_year_like = 1900 <= abs(float(lo_raw)) <= 2100 and 1900 <= abs(float(hi_raw)) <= 2100
                    if is_year_like and metric_name not in money_metrics:
                        continue
                    unit_hint = str(rm.group(2) or rm.group(4) or "")
                    if metric_name in money_metrics:
                        if not (("$" in row_text) or unit_hint.lower() in {"m", "million", "bn", "billion"} or has_money_context):
                            continue
                    lo2, unit_out = _to_metric_unit(metric_name, lo_raw, unit_hint, f"{win} {row_text}")
                    hi2, unit_out2 = _to_metric_unit(metric_name, hi_raw, unit_hint, f"{win} {row_text}")
                    unit_final = unit_out if unit_out else unit_out2
                    if metric_name == "Adj EPS" and max(abs(float(lo2)), abs(float(hi2))) > 20:
                        continue
                    if metric_name in money_metrics and unit_final in {"%", "bps", "x"}:
                        continue
                    if unit_final == "%" and max(abs(float(lo2)), abs(float(hi2))) > 200:
                        continue
                    if hi2 < lo2:
                        lo2, hi2 = hi2, lo2
                    # Low/High table rows should inherit period from the guidance section context.
                    period_name, period_norm = _infer_period_for_row(metric_name, "", win)
                    k = (metric_name, str(period_norm), round(float(lo2), 2), round(float(hi2), 2))
                    if k in seen_key:
                        continue
                    seen_key.add(k)
                    out_items.append(
                        {
                        "metric": metric_name,
                        "period": period_name,
                        "period_norm": period_norm,
                        "kind": "range",
                        "low": float(lo2),
                            "high": float(hi2),
                            "value": None,
                            "unit": unit_final,
                            "text": row_text,
                            "analysis": {"method": method_name},
                        }
                    )
            return out_items

        def _extract_explicit_metric_pairs(plain_txt: str, *, method_name: str) -> List[Dict[str, Any]]:
            out_items: List[Dict[str, Any]] = []
            if not plain_txt:
                return out_items
            txt = _qh_norm_txt(plain_txt)
            if not re.search(r"\b(low|minimum|min)\b.{0,60}\b(high|maximum|max)\b", txt, re.I):
                return out_items

            anchor_re = re.compile(
                r"(?:full[- ]year\s+outlook|financial\s+outlook|current\s+financial\s+guidance|provides?\s+the\s+following\s+guidance|updated\s+guidance|reaffirmed\s+guidance)",
                re.I,
            )
            windows: List[str] = []
            for m in anchor_re.finditer(txt):
                s = max(0, m.start() - 160)
                e = min(len(txt), m.start() + 2600)
                windows.append(txt[s:e])
            if not windows:
                windows = [txt[:3000]]

            metric_patterns: List[Tuple[str, str]] = [
                ("Revenue", r"(?:\brevenue\b|\bnet\s+revenue\b|\bsales\b)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
                ("Adj EBITDA", r"(?:adjusted\s+ebitda|adj\.?\s*ebitda)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
                ("Adj EBIT", r"(?:adjusted\s+ebit|adj\.?\s*ebit|adjusted\s+operating\s+income)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
                ("Adj EPS", r"(?:adjusted\s+eps|adj\.?\s*eps|adjusted\s+diluted\s+earnings\s+per\s+share|eps)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
                ("FCF", r"(?:free\s+cash\s+flow|\bfcf\b)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
                ("Capex", r"(?:capex|capital\s+expenditures|capital\s+spending)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
                ("Cost savings", r"(?:cost\s+savings|run[- ]?rate\s+savings|annualized\s+savings)\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?\s*\$?\s*([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|x|bps)?"),
            ]
            seen_key: set[Tuple[str, str, float, float]] = set()
            for win in windows:
                win = re.split(r"\b(forward-looking statements|safe harbor|risk factors)\b", win, flags=re.I)[0]
                if not win:
                    continue
                low_high_anchor = re.search(r"\b(low|minimum|min)\b.{0,40}\b(high|maximum|max)\b", win, re.I)
                if low_high_anchor is None:
                    continue
                block = win[low_high_anchor.end() :]
                end_re = re.search(
                    r"\b(earnings conference call|about pitney bowes|use of non-gaap|forward-looking statements|contacts|business segment reporting|consolidated statements)\b",
                    block,
                    re.I,
                )
                if end_re is not None:
                    block = block[: end_re.start()]
                block = _qh_norm_txt(block)[:1800]
                if not block:
                    continue
                for metric_name, pat in metric_patterns:
                    for mm in re.finditer(pat, block, re.I):
                        row_text = _qh_norm_txt(mm.group(0) or "")
                        lo_raw = _parse_num(mm.group(1))
                        hi_raw = _parse_num(mm.group(3))
                        if lo_raw is None or hi_raw is None:
                            continue
                        is_year_like = 1900 <= abs(float(lo_raw)) <= 2100 and 1900 <= abs(float(hi_raw)) <= 2100
                        if is_year_like and metric_name not in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"}:
                            continue
                        unit_hint = str(mm.group(2) or mm.group(4) or "")
                        lo2, unit_out = _to_metric_unit(metric_name, lo_raw, unit_hint, f"{win} {row_text}")
                        hi2, unit_out2 = _to_metric_unit(metric_name, hi_raw, unit_hint, f"{win} {row_text}")
                        unit_final = unit_out if unit_out else unit_out2
                        if metric_name == "Adj EPS" and max(abs(float(lo2)), abs(float(hi2))) > 20:
                            continue
                        if metric_name in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"} and unit_final in {"%", "bps", "x"}:
                            continue
                        if hi2 < lo2:
                            lo2, hi2 = hi2, lo2
                        # Explicit low/high guidance rows inherit period from section context.
                        period_name, period_norm = _infer_period_for_row(metric_name, "", win)
                        row_text = _qh_norm_txt((mm.group(0) or "").replace("&nbsp;", " "))
                        row_text = re.split(
                            r"\*\*\*|earnings conference call|about pitney bowes|to read and/or download a copy",
                            row_text,
                            maxsplit=1,
                            flags=re.I,
                        )[0]
                        row_text = re.split(r"\bq[1-4]\s+20\d{2}\b", row_text, maxsplit=1, flags=re.I)[0]
                        row_text = _qh_norm_txt(row_text)
                        k = (metric_name, str(period_norm), round(float(lo2), 2), round(float(hi2), 2))
                        if k in seen_key:
                            continue
                        seen_key.add(k)
                        out_items.append(
                            {
                                "metric": metric_name,
                                "period": period_name,
                                "period_norm": period_norm,
                                "kind": "range",
                                "low": float(lo2),
                                "high": float(hi2),
                                "value": None,
                                "unit": unit_final,
                                "text": row_text,
                                "analysis": {"method": method_name},
                            }
                        )
            return out_items

        def _extract_ex99_table_items(raw_html: str, plain_txt: str) -> List[Dict[str, Any]]:
            items_tbl: List[Dict[str, Any]] = []
            if not raw_html:
                return items_tbl

            table_entries: List[Tuple[pd.DataFrame, str]] = []
            if BeautifulSoup is not None:
                try:
                    soup = BeautifulSoup(raw_html, "html.parser")

                    def _table_context(tbl: Any) -> str:
                        chunks: List[str] = []
                        node = tbl
                        for _ in range(10):
                            node = node.find_previous(["h1", "h2", "h3", "h4", "h5", "h6", "p", "div", "strong", "b"])
                            if node is None:
                                break
                            txt = _qh_norm_txt(node.get_text(" ", strip=True))
                            if txt and txt not in chunks:
                                chunks.append(txt)
                            if len(" ".join(chunks)) >= 600:
                                break
                        return _qh_norm_txt(" ".join(chunks[:6]))

                    for tbl in soup.find_all("table")[:80]:
                        rows_raw: List[List[str]] = []
                        for tr in tbl.find_all("tr"):
                            cells = tr.find_all(["th", "td"])
                            if not cells:
                                continue
                            row_vals = [_qh_norm_txt(c.get_text(" ", strip=True)) for c in cells]
                            if any(str(v).strip() != "" for v in row_vals):
                                rows_raw.append(row_vals)
                        if not rows_raw:
                            continue
                        max_len = max(len(r) for r in rows_raw)
                        rows_raw = [r + [""] * (max_len - len(r)) for r in rows_raw]
                        ctx = _table_context(tbl)
                        table_entries.append((pd.DataFrame(rows_raw), ctx))
                except Exception:
                    table_entries = []

            if not table_entries:
                try:
                    html_tables = pd.read_html(io.StringIO(raw_html))
                except Exception:
                    html_tables = []
                for tbl in html_tables[:60]:
                    if tbl is None or tbl.empty:
                        continue
                    table_entries.append((tbl.copy(), plain_txt[:900]))

            for tbl, ctx in table_entries[:60]:
                items_tbl.extend(_parse_low_high_table_df(tbl, f"{ctx} {plain_txt[:1400]}", method_name="ex99_table"))
            if not items_tbl and plain_txt:
                items_tbl.extend(_extract_low_high_grid_items(plain_txt, method_name="ex99_text_grid"))
            if plain_txt:
                existing = {
                    (
                        str(x.get("metric") or ""),
                        str(x.get("period_norm") or "UNK"),
                        round(float(x.get("low")), 2) if x.get("low") is not None else None,
                        round(float(x.get("high")), 2) if x.get("high") is not None else None,
                    )
                    for x in items_tbl
                }
                for ex in _extract_explicit_metric_pairs(plain_txt, method_name="ex99_text_pairs"):
                    ek = (
                        str(ex.get("metric") or ""),
                        str(ex.get("period_norm") or "UNK"),
                        round(float(ex.get("low")), 2) if ex.get("low") is not None else None,
                        round(float(ex.get("high")), 2) if ex.get("high") is not None else None,
                    )
                    if ek in existing:
                        continue
                    items_tbl.append(ex)
            return items_tbl

        def _extract_inline_low_high_items(plain_txt: str) -> List[Dict[str, Any]]:
            return _extract_low_high_grid_items(plain_txt, method_name="inline_low_high")

        def _extract_low_high_sequence_items(plain_txt: str) -> List[Dict[str, Any]]:
            return _extract_low_high_grid_items(plain_txt, method_name="inline_low_high_seq")

        def _extract_earnings_pdf_items(qref_local: pd.Timestamp) -> List[Dict[str, Any]]:
            out_pdf: List[Dict[str, Any]] = []
            er_dir = _first_existing_material_dir(
                "earnings_release",
                "Earnings Release",
                "Earnings Releases",
                "press_release",
                "Press Release",
            )
            if er_dir is None:
                return out_pdf
            try:
                import pdfplumber  # type: ignore
            except Exception:
                return out_pdf

            def _quarter_end_from_file_name(name: str) -> Optional[pd.Timestamp]:
                nl = str(name or "").lower()
                m = re.search(r"\bq([1-4])\s*[-_ ]*\s*(20\d{2})\b", nl)
                if m:
                    qn = int(m.group(1))
                    yy = int(m.group(2))
                    return pd.Timestamp(dt.date(yy, qn * 3, 1)) + pd.offsets.MonthEnd(0)
                m2 = re.search(r"\b(20\d{2})\s*[-_ ]*\s*q([1-4])\b", nl)
                if m2:
                    yy = int(m2.group(1))
                    qn = int(m2.group(2))
                    return pd.Timestamp(dt.date(yy, qn * 3, 1)) + pd.offsets.MonthEnd(0)
                return None

            seen_pdf_keys: set[Tuple[str, str, str, float, float]] = set()
            pdf_files = sorted(
                [p for p in er_dir.glob("*.pdf") if p.is_file()],
                key=lambda p: p.stat().st_mtime if p.exists() else 0,
                reverse=True,
            )[:30]
            for pdf_path in pdf_files:
                file_qe = _quarter_end_from_file_name(pdf_path.name)
                if file_qe is not None and not _qh_date_eq(file_qe, qref_local):
                    continue
                try:
                    with silence_pdfminer_warnings(enabled=config.quiet_pdf_warnings):
                        with pdfplumber.open(str(pdf_path)) as pdf:
                            for page_idx, page in enumerate(pdf.pages[:30], start=1):
                                page_text = _qh_norm_txt(page.extract_text() or "")
                                if not page_text:
                                    continue
                                if not re.search(
                                    r"\b(guidance|outlook|financial guidance|financial outlook|full[- ]year outlook|updated guidance|reaffirmed guidance)\b",
                                    page_text,
                                    re.I,
                                ):
                                    # keep explicit low/high pages if metrics are present
                                    if not (
                                        re.search(r"\blow\b.{0,30}\bhigh\b", page_text, re.I)
                                        and re.search(
                                            r"\b(revenue|adjusted\s+ebitda|adjusted\s+ebit|adjusted\s+eps|free cash flow|fcf|capex|cost savings)\b",
                                            page_text,
                                            re.I,
                                        )
                                    ):
                                        continue

                                page_items: List[Dict[str, Any]] = []

                                # Structured table pass.
                                try:
                                    raw_tables = page.extract_tables() or []
                                except Exception:
                                    raw_tables = []
                                for tb in raw_tables:
                                    if not tb:
                                        continue
                                    rows = []
                                    for rr in tb:
                                        if rr is None:
                                            continue
                                        row_vals = [_qh_norm_txt(v) for v in rr]
                                        if any(v for v in row_vals):
                                            rows.append(row_vals)
                                    if not rows:
                                        continue
                                    max_len = max(len(r) for r in rows)
                                    rows = [r + [""] * (max_len - len(r)) for r in rows]
                                    df_tb = pd.DataFrame(rows)
                                    page_items.extend(
                                        _parse_low_high_table_df(
                                            df_tb,
                                            f"{pdf_path.name} page {page_idx} {page_text[:1400]}",
                                            method_name="earnings_pdf_table",
                                        )
                                    )

                                # Text fallback on the same page (captures OCR-friendly Low/High lists).
                                if not page_items:
                                    page_items.extend(_extract_low_high_sequence_items(page_text))
                                    page_items.extend(_extract_inline_low_high_items(page_text))

                                for it in page_items:
                                    mk = str(it.get("metric") or "")
                                    pk = str(it.get("period_norm") or "UNK")
                                    uk = str(it.get("unit") or "")
                                    lo = float(it.get("low")) if it.get("low") is not None else float("nan")
                                    hi = float(it.get("high")) if it.get("high") is not None else float("nan")
                                    key = (mk, pk, uk, round(lo, 4), round(hi, 4))
                                    if key in seen_pdf_keys:
                                        continue
                                    seen_pdf_keys.add(key)
                                    src = {
                                        "source_type": "earnings_release_pdf",
                                        "accn": "",
                                        "form": "8-K",
                                        "filed": pd.Timestamp(qref_local),
                                        "doc": pdf_path.name,
                                        "doc_path": str(pdf_path),
                                        "page": page_idx,
                                        "section": "guidance_table",
                                    }
                                    gtype = "run-rate" if (
                                        str(it.get("metric") or "") == "Cost savings"
                                        and re.search(
                                            r"\b(annualized|annualised|run[- ]?rate)\b",
                                            str(it.get("text") or ""),
                                            re.I,
                                        )
                                    ) else "period"
                                    pnorm = str(it.get("period_norm") or "UNK")
                                    ptxt = str(it.get("period") or "")
                                    if (
                                        str(it.get("metric") or "") == "Cost savings"
                                        and gtype == "run-rate"
                                        and pnorm.startswith("FY")
                                    ):
                                        pnorm = "UNK"
                                        ptxt = ""
                                    out_pdf.append(
                                        {
                                            "metric": str(it.get("metric") or ""),
                                            "metric_raw": str(it.get("metric") or ""),
                                            "period": ptxt,
                                            "period_norm": pnorm,
                                            "kind": "range",
                                            "low": it.get("low"),
                                            "high": it.get("high"),
                                            "value": None,
                                            "unit": str(it.get("unit") or ""),
                                            "qualitative_range_text": None,
                                            "text": _qh_norm_txt(it.get("text") or ""),
                                            "source": src,
                                            "score": 88.0,
                                            "source_rank": 1,
                                            "source_priority": 95,
                                            "source_date": pd.Timestamp(qref_local),
                                            "asof": qref_local,
                                            "analysis": {"method": str((it.get("analysis") or {}).get("method") or "earnings_pdf_text")},
                                            "guidance_type": gtype,
                                            "target_type": ("run_rate" if gtype == "run-rate" else "annual"),
                                            "target_period": (pnorm if pnorm not in {"", "UNK"} else "unspecified"),
                                            "execution_window": (
                                                "over_next_year"
                                                if re.search(r"\bover\s+the?\s*next\s+year\b", str(it.get("text") or "").lower())
                                                else "unspecified"
                                            ),
                                            "as_of_quarter": str(qref_local.date()),
                                            "last_mentioned_quarter": str(qref_local.date()),
                                            "as_of_quarter_end": str(qref_local.date()),
                                            "source_doc_end": str(qref_local.date()),
                                            "source_filed_date": pd.Timestamp(qref_local),
                                            "first_seen_quarter_end": str(qref_local.date()),
                                            "last_seen_quarter_end": str(qref_local.date()),
                                            "referenced_years": sorted({int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", str(it.get("text") or ""))}),
                                            "has_forward_intent": bool(_qh_forward_intent_re.search(str(it.get("text") or ""))),
                                            "has_period_anchor": bool(_qh_period_anchor_re.search(str(it.get("text") or ""))),
                                            "target_period_norm": str(pnorm or "UNK"),
                                        }
                                    )
                except Exception:
                    continue
            return out_pdf

        seen_keys: set[Tuple[str, str, str]] = set()
        accn_items = sorted(
            list(accn_meta.items()),
            key=lambda kv: pd.to_datetime((kv[1] or {}).get("filed"), errors="coerce")
            if (kv[1] or {}).get("filed") is not None
            else pd.Timestamp("1900-01-01"),
            reverse=True,
        )
        for accn, meta in accn_items:
            for dp in _sec_docs_for_accession(accn)[:12]:
                nl = dp.name.lower()
                if not re.search(r"(ex99|ex-99|exhibit99|earnings|press|news\s*release|shareholder|ceo|letter|presentation|slides)", nl):
                    continue
                # Avoid parsing primary 10-Q/10-K inline XBRL HTML tables as "guidance tables".
                if re.search(r"(pbi-\d{8}\.htm|10q|10-k|10q|form10|annualreport|quarterlyreport)", nl) and not re.search(
                    r"(ex99|ex-99|earnings|press|news|shareholder|ceo|letter|presentation|slides)",
                    nl,
                ):
                    continue
                txt = _qh_norm_txt(_read_cached_doc_text(dp))
                if not txt:
                    continue
                if not re.search(r"\b(guidance|outlook|financial outlook|full[- ]year outlook|updated guidance|reaffirmed guidance)\b", txt, re.I):
                    continue
                src_base = {
                    "source_type": "sec_doc",
                    "accn": accn,
                    "form": meta.get("form") or "",
                    "filed": meta.get("filed"),
                    "doc": dp.name,
                    "doc_path": str(dp),
                    "section": "guidance_window",
                }

                parsed_rows: List[Dict[str, Any]] = []
                _seen_parsed: set[Tuple[str, str, str, float, float]] = set()
                for _fn in (_extract_ex99_table_items, _extract_low_high_sequence_items, _extract_inline_low_high_items):
                    try:
                        _rows = _fn(raw, txt) if _fn is _extract_ex99_table_items else _fn(txt)
                    except Exception:
                        _rows = []
                    for _pr in _rows or []:
                        _mk = str(_pr.get("metric") or "")
                        _pk = str(_pr.get("period_norm") or "UNK")
                        _uk = str(_pr.get("unit") or "")
                        _lo = float(_pr.get("low")) if _pr.get("low") is not None else float("nan")
                        _hi = float(_pr.get("high")) if _pr.get("high") is not None else float("nan")
                        _key = (_mk, _pk, _uk, round(_lo, 4), round(_hi, 4))
                        if _key in _seen_parsed:
                            continue
                        _seen_parsed.add(_key)
                        parsed_rows.append(_pr)
                if parsed_rows:
                    for pr in parsed_rows:
                        metric_name = str(pr.get("metric") or "")
                        period_norm = str(pr.get("period_norm") or "UNK")
                        row_text = _qh_norm_txt(pr.get("text") or "")
                        unit_txt = str(pr.get("unit") or "")
                        if metric_name in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"} and unit_txt in {"%", "bps", "x"}:
                            continue
                        if str(pr.get("kind") or "") == "range":
                            lo_raw = pr.get("low")
                            hi_raw = pr.get("high")
                            try:
                                lo_num = float(lo_raw) if lo_raw is not None else None
                                hi_num = float(hi_raw) if hi_raw is not None else None
                            except Exception:
                                lo_num = hi_num = None
                            if hi_num is not None and hi_num > 25_000_000_000:
                                continue
                            if metric_name == "Revenue" and lo_num is not None and lo_num <= 0:
                                continue
                            if lo_num is not None and hi_num is not None and abs(lo_num) < 1e-12:
                                if metric_name == "Adj EPS" and hi_num >= 0.5:
                                    continue
                                if metric_name in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Capex", "Cost savings", "Restructuring charges"} and hi_num >= 50_000_000:
                                    continue
                        # Keep period scope tight around as-of quarter to avoid pulling historical table years.
                        mfy = re.match(r"FY(20\d{2})$", period_norm)
                        if mfy:
                            fy_year = int(mfy.group(1))
                            if fy_year not in {int(qts.year), int(qts.year) + 1}:
                                continue
                        mq = re.match(r"Q(20\d{2})Q([1-4])$", period_norm)
                        if mq:
                            tgt_ord = int(mq.group(1)) * 4 + int(mq.group(2))
                            cur_ord = int(qts.year) * 4 + (((int(qts.month) - 1) // 3) + 1)
                            if abs(tgt_ord - cur_ord) > 2:
                                continue
                        gtype = "run-rate" if (
                            metric_name == "Cost savings"
                            and re.search(r"\b(annualized|annualised|run[- ]?rate)\b", row_text, re.I)
                        ) else "period"
                        if metric_name == "Cost savings" and gtype == "run-rate" and period_norm.startswith("FY"):
                            period_norm = "UNK"
                            pr["period"] = ""
                        k = (metric_name, period_norm, gtype)
                        if k in seen_keys:
                            continue
                        seen_keys.add(k)
                        out.append(
                            {
                                "metric": metric_name,
                                "metric_raw": metric_name,
                                "period": str(pr.get("period") or ""),
                                "period_norm": period_norm,
                                "kind": "range",
                                "low": pr.get("low"),
                                "high": pr.get("high"),
                                "value": None,
                                "unit": str(pr.get("unit") or ""),
                                "qualitative_range_text": None,
                                "text": row_text,
                                "source": dict(src_base),
                                "score": 90.0,
                                "source_rank": 0,
                                "source_priority": 100,
                                "source_date": pd.to_datetime(meta.get("filed"), errors="coerce"),
                                "asof": qref,
                                "analysis": dict(pr.get("analysis") or {"method": "ex99_table"}),
                                "guidance_type": gtype,
                                "target_type": ("run_rate" if gtype == "run-rate" else "annual"),
                                "target_period": (period_norm if period_norm not in {"", "UNK"} else "unspecified"),
                                "execution_window": (
                                    "over_next_year"
                                    if re.search(r"\bover\s+the?\s*next\s+year\b", row_text.lower())
                                    else "unspecified"
                                ),
                                "as_of_quarter": str(qts.date()),
                                "last_mentioned_quarter": str(qts.date()),
                                "as_of_quarter_end": str(qts.date()),
                                "source_doc_end": str(qts.date()),
                                "source_filed_date": pd.to_datetime(meta.get("filed"), errors="coerce"),
                                "first_seen_quarter_end": str(qts.date()),
                                "last_seen_quarter_end": str(qts.date()),
                                "referenced_years": sorted({int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", row_text)}),
                                "has_forward_intent": bool(_qh_forward_intent_re.search(row_text)),
                                "has_period_anchor": bool(_qh_period_anchor_re.search(row_text)),
                                "target_period_norm": str(period_norm or "UNK"),
                            }
                        )
                    # Table/inline extraction already handled this doc; skip weaker anchor regex fallback.
                    continue
        # Secondary numeric source: local earnings_release PDFs (lower priority than EX-99 tables).
        pdf_rows = _extract_earnings_pdf_items(qts)
        for pr in pdf_rows:
            metric_name = str(pr.get("metric") or "")
            period_norm = str(pr.get("period_norm") or "UNK")
            gtype = str(pr.get("guidance_type") or "period")
            key = (metric_name, period_norm, gtype)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(pr)
        return out

    def _qh_revenue_ttm_for(qref: Optional[pd.Timestamp]) -> Optional[float]:
        if qref is None or hist is None or hist.empty or "quarter" not in hist.columns or "revenue" not in hist.columns:
            return None
        hh = hist.copy()
        hh["quarter"] = pd.to_datetime(hh["quarter"], errors="coerce")
        hh = hh[hh["quarter"].notna()].sort_values("quarter")
        if hh.empty:
            return None
        qts_local = pd.Timestamp(qref).to_period("Q")
        hh = hh[hh["quarter"].dt.to_period("Q") <= qts_local]
        if len(hh) < 4:
            return None
        vals = pd.to_numeric(hh["revenue"], errors="coerce").tail(4)
        if vals.isna().any():
            return None
        try:
            return float(vals.sum())
        except Exception:
            return None

    def _qh_build_guidance_snapshot(qref: Optional[pd.Timestamp], prev_ref: Optional[pd.Timestamp]) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "status": "Unknown",
            "ranges": {},
            "exact_language": "N/A",
            "source": {},
            "exact_source": {},
            "compare_to_prev": None,
            "guidance_items": [],
            "key_metric": None,
            "key_period_norm": None,
        }
        qyear = int(pd.Timestamp(qref).year) if qref is not None else None
        cur = _qh_collect_guidance(qref)
        if not cur:
            out["exact_language"] = "N/A (no guidance candidate in Promise_Tracker/Slides_Guidance/Quarter_Notes)"
            return out
        items_cur = _qh_extract_guidance_items(cur, qref)
        sec_backfill = _qh_sec_outlook_backfill(qref)
        if sec_backfill:
            def _snap_key(it: Dict[str, Any]) -> Tuple[str, str]:
                mk = str(it.get("metric") or "")
                pk = str(it.get("period_norm") or "UNK")
                gt = str(it.get("guidance_type") or "")
                if pk in {"", "UNK"} and gt in {"run-rate", "ongoing", "one-time", "ratio"}:
                    pk = f"TYPE:{gt}"
                return (mk, pk)

            by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for _it in items_cur:
                k = _snap_key(_it)
                by_key[k] = _it
            for _it in sec_backfill:
                k = _snap_key(_it)
                prev = by_key.get(k)
                prev_kind = str(prev.get("kind") or "") if prev is not None else ""
                new_kind = str(_it.get("kind") or "")
                prev_method = str((prev.get("analysis") or {}).get("method") or "") if prev is not None else ""
                new_method = str((_it.get("analysis") or {}).get("method") or "")
                prev_pri = int(prev.get("source_priority") or 0) if prev is not None else -1
                new_pri = int(_it.get("source_priority") or 0)
                def _is_tabular_method(m: str) -> bool:
                    ml = str(m or "").lower()
                    return ml in {
                        "ex99_table",
                        "ex99_text_grid",
                        "earnings_pdf_table",
                        "inline_low_high_seq",
                        "inline_low_high",
                        "earnings_pdf_text",
                    }
                if (
                    prev is None
                    or prev_kind == "text"
                    or (new_kind == "range" and prev_kind in {"point", "qualitative_range"})
                    or (new_kind == "point" and prev_kind == "qualitative_range")
                    or (
                        new_kind == "range"
                        and prev_kind == "range"
                        and (
                            new_pri > prev_pri
                            or (new_method == "ex99_table" and prev_method != "ex99_table")
                            or (_is_tabular_method(new_method) and not _is_tabular_method(prev_method))
                        )
                    )
                ):
                    by_key[k] = _it
            items_cur = list(by_key.values())

        # Remove obvious Revenue mislabels copied from Adj EBIT/Adj EBITDA ranges.
        rev_ttm_ref = _qh_revenue_ttm_for(qref)
        if rev_ttm_ref is not None and rev_ttm_ref > 0:
            by_period_adj: Dict[str, List[Dict[str, Any]]] = {}
            for _it in items_cur:
                mk = str(_it.get("metric") or "")
                if mk not in {"Adj EBIT", "Adj EBITDA"}:
                    continue
                if str(_it.get("kind") or "") != "range":
                    continue
                pk = str(_it.get("period_norm") or "UNK")
                by_period_adj.setdefault(pk, []).append(_it)

            cleaned_items: List[Dict[str, Any]] = []
            for _it in items_cur:
                mk = str(_it.get("metric") or "")
                if mk == "Revenue" and str(_it.get("kind") or "") == "range":
                    pk = str(_it.get("period_norm") or "UNK")
                    lo = _it.get("low")
                    hi = _it.get("high")
                    if lo is not None and hi is not None:
                        mid = (float(lo) + float(hi)) / 2.0
                        # FY revenue guidance far below current run-rate and identical to an Adj EBIT/EBITDA
                        # range is almost always a metric-mapping error.
                        if pk.startswith("FY") and mid < (0.60 * float(rev_ttm_ref)):
                            adj_rows = by_period_adj.get(pk, [])
                            matched_adj = any(
                                ar.get("low") is not None
                                and ar.get("high") is not None
                                and abs(float(ar.get("low")) - float(lo)) <= 1e-6
                                and abs(float(ar.get("high")) - float(hi)) <= 1e-6
                                for ar in adj_rows
                            )
                            if matched_adj:
                                continue
                cleaned_items.append(_it)
            items_cur = cleaned_items
        if qref is not None:
            extra_text_items = [dict(x) for x in _qh_extra_text_guidance_items(pd.Timestamp(qref))]
            if extra_text_items:
                def _guidance_item_key(it: Dict[str, Any]) -> Tuple[str, str]:
                    mk = str(it.get("metric") or "")
                    pk = str(it.get("period_norm") or "UNK")
                    gt = str(it.get("guidance_type") or "")
                    if pk in {"", "UNK"} and gt in {"run-rate", "ongoing", "one-time", "ratio"}:
                        pk = f"TYPE:{gt}"
                    return mk, pk

                by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
                for _it in items_cur:
                    by_key[_guidance_item_key(_it)] = _it
                for _it in extra_text_items:
                    k = _guidance_item_key(_it)
                    prev = by_key.get(k)
                    if prev is None:
                        by_key[k] = _it
                        continue
                    prev_gtype = str(prev.get("guidance_type") or "").strip().lower()
                    if prev_gtype != "text":
                        continue
                    if float(_it.get("score") or 0.0) > float(prev.get("score") or 0.0):
                        by_key[k] = _it
                items_cur = list(by_key.values())
        out["guidance_items"] = items_cur
        best = items_cur[0] if items_cur else cur[0]
        out["source"] = dict(best.get("source") or {})
        text_blob = " ".join([str(c.get("text") or "") for c in cur[:4]])
        out["ranges"] = _qh_parse_ranges(text_blob)

        def _item_mid(item: Optional[Dict[str, Any]]) -> Optional[float]:
            if not item:
                return None
            if item.get("kind") == "range" and item.get("low") is not None and item.get("high") is not None:
                return (float(item.get("low")) + float(item.get("high"))) / 2.0
            if item.get("kind") == "point" and item.get("value") is not None:
                return float(item.get("value"))
            return None

        def _is_numeric_item(item: Optional[Dict[str, Any]]) -> bool:
            return _item_mid(item) is not None

        metric_priority = list(GUIDANCE_UI_METRIC_PRIORITY)

        def _period_pref(period_norm: str) -> Tuple[int, int, int]:
            p = str(period_norm or "UNK")
            if p == "FY+1":
                return (1, 0, (qyear + 1) if qyear is not None else 0)
            m_fy = re.match(r"FY(20\d{2})$", p)
            if m_fy:
                yr = int(m_fy.group(1))
                if qyear is None:
                    return (0, yr, 0)
                if yr == qyear:
                    return (0, 0, yr)
                if yr == qyear + 1:
                    return (1, 0, yr)
                if yr > qyear + 1:
                    return (2, yr - qyear, yr)
                return (5, qyear - yr, yr)
            m_q = re.match(r"Q(20\d{2})Q([1-4])$", p)
            if m_q:
                return (3, int(m_q.group(1)), int(m_q.group(2)))
            if p == "UNK":
                return (9, 0, 0)
            return (8, 0, 0)

        def _pick_key_item(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            for m in metric_priority:
                m_items = [x for x in items if str(x.get("metric") or "") == m]
                if not m_items:
                    continue
                m_items = sorted(
                    m_items,
                    key=lambda z: (
                        _period_pref(str(z.get("period_norm") or "UNK")),
                        0 if _is_numeric_item(z) else 1,
                        -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                        -int(z.get("source_priority") or 0),
                        -float(z.get("score") or 0),
                    ),
                )
                return m_items[0]
            return items[0] if items else None

        key_item = _pick_key_item(items_cur)
        if key_item is not None:
            out["key_metric"] = str(key_item.get("metric") or "")
            out["key_period_norm"] = str(key_item.get("period_norm") or "")
            out["exact_language"] = _qh_norm_txt(key_item.get("text") or "")[:500] or "N/A"
            out["exact_source"] = dict(key_item.get("source") or {})

        if qref is not None and prev_ref is not None and key_item is not None and _is_numeric_item(key_item):
            prev = _qh_collect_guidance(prev_ref)
            prev_items = _qh_extract_guidance_items(prev, prev_ref) if prev else []
            k_metric = str(key_item.get("metric") or "")
            k_period_norm = str(key_item.get("period_norm") or "")
            prev_item = next(
                (
                    x
                    for x in prev_items
                    if str(x.get("metric") or "") == k_metric
                    and str(x.get("period_norm") or "") == k_period_norm
                    and _is_numeric_item(x)
                ),
                None,
            )
            if prev_item is None:
                prev_item = next(
                    (
                        x
                        for x in prev_items
                        if str(x.get("metric") or "") == k_metric and _is_numeric_item(x)
                    ),
                    None,
                )
            c_mid = _item_mid(key_item)
            p_mid = _item_mid(prev_item)
            if c_mid is not None and p_mid is not None and abs(float(p_mid)) > 1e-12:
                d = c_mid / p_mid - 1.0
                out["compare_to_prev"] = {
                    "metric": k_metric,
                    "period": str(key_item.get("period") or "Unknown"),
                    "period_norm": k_period_norm,
                    "prev_mid": p_mid,
                    "curr_mid": c_mid,
                    "delta_pct": d,
                }
                out["status"] = "Raised" if d > 0.02 else ("Lowered" if d < -0.02 else "Maintained")

        if out["status"] == "Unknown":
            lang_src = out["exact_language"] if out.get("exact_language") not in (None, "", "N/A") else text_blob
            t = str(lang_src or "").lower()
            status_guess = glx_classify_status(t)
            if status_guess != "Unknown":
                out["status"] = status_guess

        if out.get("exact_language") in (None, "", "N/A"):
            fallback_line = next((x for x in cur if str(x.get("text") or "").strip()), None)
            if fallback_line is not None:
                out["exact_language"] = _qh_norm_txt(fallback_line.get("text"))[:500]
                out["exact_source"] = dict(fallback_line.get("source") or {})
        return out

    def _qh_fmt_money_compact(v: Optional[float]) -> str:
        if v is None or pd.isna(v):
            return "N/A"
        return f"${float(v)/1e6:,.1f}m"

    def _qh_fmt_money_signed(v: Optional[float]) -> str:
        if v is None or pd.isna(v):
            return "N/A"
        sign = "+" if float(v) >= 0 else "-"
        return f"{sign}${abs(float(v))/1e6:,.1f}m"

    def truncate_clean(s: Any, max_len: int = 100) -> str:
        t = _qh_norm_txt(s)
        if len(t) <= max_len:
            return t
        cut = t[: max(1, max_len - 3)]
        b = max(cut.rfind(" "), cut.rfind("."), cut.rfind(","), cut.rfind(";"), cut.rfind(":"))
        if b >= max(16, int(max_len * 0.85)):
            cut = cut[:b]
        return f"{cut.rstrip(' ,;:.')}..."

    def _qh_short(s: Any, max_len: int = 100) -> str:
        return truncate_clean(s, max_len)

    def _qh_guidance_value_text(item: Dict[str, Any]) -> str:
        kind = str(item.get("kind") or "")
        unit = str(item.get("unit") or "")
        gtype = str(item.get("guidance_type") or "").strip().lower()
        exec_window = str(item.get("execution_window") or "unspecified").strip().lower()
        is_carry = bool(item.get("carry_forward"))
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
        item_blob_low = item_blob.lower()
        metric_txt = str(item.get("metric") or FORWARD_NOTES_LABEL).strip()
        metric_low = metric_txt.lower()

        def _carry_direction_prefix() -> str:
            if not is_carry:
                return ""
            action = str(item.get("display_action") or "").strip().lower()
            if not action:
                txt_low = _qh_norm_txt(item.get("text") or "").lower()
                if not txt_low:
                    return ""
                if re.search(r"\btracking midpoint\b", txt_low):
                    action = "tracking_midpoint"
                elif re.search(r"\btracking low end\b", txt_low):
                    action = "tracking_low_end"
                elif re.search(r"\b(reaffirm|reaffirmed|reaffirms|maintain|maintained|unchanged)\b", txt_low):
                    action = "reaffirmed"
                elif re.search(r"\b(raise|raised|raising|increase|increased|increasing)\b", txt_low):
                    action = "increasing"
                elif re.search(r"\b(lower|lowered|lowering|decrease|decreased|decreasing|reduce|reduced|reducing)\b", txt_low):
                    action = "decreasing"
                elif re.search(r"\b(tighten|tightened|tightening)\b", txt_low):
                    action = "tightening"
                elif re.search(r"\b(update|updated)\b", txt_low):
                    action = "updated"
            if action == "tracking_midpoint":
                return "tracking midpoint of "
            if action == "tracking_low_end":
                return "tracking low end of "
            if action == "reaffirmed":
                return "reaffirmed at "
            if action == "increasing":
                return "increasing target to "
            if action == "decreasing":
                return "decreasing target to "
            if action == "tightening":
                return "tightening target to "
            if action == "updated":
                return "updated target to "
            return ""

        def _append_meta(base: str) -> str:
            out = str(base or "").strip()
            if not out:
                return out
            if gtype == "run-rate" and "run-rate" not in out.lower():
                out = f"{out} run-rate" if is_carry else f"annualized run-rate {out}"
            prefix = _carry_direction_prefix()
            if prefix and not out.lower().startswith(prefix.strip()):
                out = f"{prefix}{out}"
            if (not is_carry) and exec_window != "unspecified":
                if exec_window == "over_next_year":
                    tail = "implementation over next year"
                elif exec_window.startswith("through_"):
                    tail = f"through {exec_window.split('_', 1)[1]}"
                elif exec_window.startswith("into_"):
                    tail = f"into {exec_window.split('_', 1)[1]}"
                else:
                    tail = exec_window.replace("_", " ")
                if tail and tail not in out.lower():
                    out = f"{out} ({tail})"
            return out

        def _headline_text() -> str:
            txt = _qh_norm_txt(item.get("text") or item.get("qualitative_range_text") or "")
            if re.search(r"\b45z\b", metric_low, re.I) and re.search(r"\bstarting point\b", item_blob_low, re.I):
                return "Starting point ~$188m; upside from yield/energy/projects"
            if re.search(r"\bfarm[- ]practice\b", metric_low, re.I) or re.search(r"\bfarm[- ]practice\b", item_blob_low, re.I):
                return "Excluded from current base; final guidance expected in 2026"
            if (
                re.search(r"\b45z\b", metric_low, re.I)
                and re.search(r"\b(base case|facility qualification|indirect land use change|iluc)\b", item_blob_low, re.I)
            ):
                return "Base improved by ILUC removal, facility qualification, and Advantage Nebraska"
            if re.search(r"\bcredit marketing\b", metric_low, re.I) or (
                re.search(r"\bcredits\b", item_blob_low, re.I) and re.search(r"\bmarket", item_blob_low, re.I)
            ):
                return "2026 credits are being marketed; interest is strong"
            if re.search(r"\bco2\b", metric_low, re.I) and re.search(r"\b(logistics|truck|rail)\b", item_blob_low, re.I):
                return "Evaluating truck/rail liquid CO2 for non-pipeline plants"
            if re.search(r"\b(capital allocation|fcf priorit)\b", metric_low, re.I) or re.search(r"\bplant quality and utilization\b", item_blob_low, re.I):
                return "FCF priorities: plant quality/utilization, 45Z/base business, then debt/shareholder returns/M&A"
            if re.search(r"\bcost savings\b", metric_low, re.I) and kind in {"text", "qualitative_range"}:
                target_txt = str(_extract_pbi_target_display(item_blob, "Cost savings target") or "").strip()
                if target_txt:
                    return f"Raised annualized savings target to {target_txt}"
            if re.search(r"\binterest expense\b", metric_low, re.I) and re.search(r"\$[0-9]", item_blob):
                money_vals = re.findall(r"\$[0-9][0-9.,]*(?:bn|m)?", item_blob)
                if len(money_vals) >= 2:
                    return f"2026 interest expense expected at about {money_vals[0]}-{money_vals[1].lstrip('$')}"
            return txt

        if is_carry and re.search(r"\bcost savings\b", item_blob_low, re.I):
            target_txt = str(_extract_pbi_target_display(item_blob, "Cost savings target") or "").strip()
            if target_txt:
                return _append_meta(f"Raised target to {target_txt} annualized savings")

        if kind == "qualitative_range":
            qrt = _qh_norm_txt(item.get("qualitative_range_text") or "")
            if qrt:
                return _append_meta(qrt)
        if kind == "range" and item.get("low") is not None and item.get("high") is not None:
            lo = float(item.get("low"))
            hi = float(item.get("high"))
            if unit == "$m":
                return _append_meta(f"{_qh_fmt_money_compact(lo)}-{_qh_fmt_money_compact(hi)}")
            if unit == "$":
                return _append_meta(f"${lo:,.2f}-${hi:,.2f}")
            if unit == "x":
                return _append_meta(f"{lo:,.2f}x-{hi:,.2f}x")
            if unit == "%":
                return _append_meta(f"{lo:,.1f}% - {hi:,.1f}%")
            if unit == "bps":
                return _append_meta(f"{lo:,.0f}bps - {hi:,.0f}bps")
            return _append_meta(f"{lo:,.2f}-{hi:,.2f}")
        if kind == "point" and item.get("value") is not None:
            v = float(item.get("value"))
            if unit == "$m":
                return _append_meta(_qh_fmt_money_compact(v))
            if unit == "$":
                return _append_meta(f"${v:,.2f}")
            if unit == "x":
                return _append_meta(f"{v:,.2f}x")
            if unit == "%":
                return _append_meta(f"{v:,.1f}%")
            if unit == "bps":
                return _append_meta(f"{v:,.0f}bps")
            return _append_meta(f"{v:,.2f}")
        return _append_meta(_headline_text())

    def _qh_item_mid(item: Optional[Dict[str, Any]]) -> Optional[float]:
        if not item:
            return None
        if item.get("kind") == "range" and item.get("low") is not None and item.get("high") is not None:
            return (float(item.get("low")) + float(item.get("high"))) / 2.0
        if item.get("kind") == "point" and item.get("value") is not None:
            return float(item.get("value"))
        return None
    def _qh_display_period(item: Dict[str, Any]) -> str:
        p_norm = str(item.get("period_norm") or "").strip()
        p_label = _qh_norm_txt(item.get("period") or "")
        if p_norm:
            if p_norm == "FY+1":
                return "Next FY"
            m_fy = re.match(r"FY(20\d{2})$", p_norm)
            if m_fy:
                return f"FY {m_fy.group(1)}"
            m_q = re.match(r"Q(20\d{2})Q([1-4])$", p_norm)
            if m_q:
                return f"Q{m_q.group(2)} {m_q.group(1)}"
        if p_label:
            p_label = re.sub(r"\bFY\s*[, ]*(20\d{2})\b", r"FY \1", p_label, flags=re.I)
            p_label = re.sub(r"\bFY\s*(20)\s*,\s*([0-9]{2})\b", r"FY \1\2", p_label, flags=re.I)
            p_label = re.sub(r"\bQ\s*([1-4])\s*,\s*(20\d{2})\b", r"Q\1 \2", p_label, flags=re.I)
            p_label = re.sub(r"\s+", " ", p_label).strip(" ,")
        return p_label if p_label.lower() not in {"unknown", "n/a"} else ""

    def _qh_quarter_label(v: Any) -> str:
        t = pd.to_datetime(v, errors="coerce")
        if pd.isna(t):
            return "N/A"
        qn = ((int(t.month) - 1) // 3) + 1
        return f"Q{qn} {int(t.year)}"

    def _qh_display_stated_in(item: Dict[str, Any]) -> str:
        src = dict(item.get("source") or {})
        src_doc = str(
            src.get("doc")
            or src.get("doc_path")
            or src.get("source_file")
            or item.get("source_file")
            or ""
        ).strip()
        if src_doc:
            m_doc = re.search(r"(20\d{2})-(\d{2})-(\d{2})", src_doc)
            if m_doc and re.search(r"\bexternal/conferences\b", src_doc, re.I):
                lbl = _qh_quarter_label(f"{m_doc.group(1)}-{m_doc.group(2)}-{m_doc.group(3)}")
                if lbl != "N/A":
                    return lbl
        for key in (
            "last_mentioned_quarter",
            "first_seen_quarter_end",
            "statement_quarter_end",
            "source_quarter_end",
            "as_of_quarter_end",
            "quarter_end",
            "date",
        ):
            lbl = _qh_quarter_label(item.get(key))
            if lbl != "N/A":
                return lbl
        return ""

    def _qh_quarter_ord_from_label(label_in: Any) -> Optional[int]:
        txt = str(label_in or "").strip()
        m = re.fullmatch(r"Q([1-4])\s+(20\d{2})", txt)
        if not m:
            return None
        try:
            return int(m.group(2)) * 4 + int(m.group(1))
        except Exception:
            return None

    def _qh_display_horizon(item: Dict[str, Any]) -> str:
        period_txt = _qh_display_period(item)
        if period_txt:
            period_txt = period_txt.replace("FY ", "FY")
            return period_txt
        gtype = str(item.get("guidance_type") or "").strip().lower()
        exec_window = str(item.get("execution_window") or "").strip().lower()
        if gtype == "run-rate":
            return "Run-rate"
        if gtype == "annualized":
            return "Annualized"
        if exec_window == "over_next_year":
            return "Next year"
        if exec_window.startswith("through_"):
            return f"Through {exec_window.split('_', 1)[1]}"
        if exec_window.startswith("into_"):
            return f"Into {exec_window.split('_', 1)[1]}"
        return ""

    def _qh_display_type(item: Dict[str, Any]) -> str:
        gtype = str(item.get("guidance_type") or "text").strip().lower() or "text"
        shown = gtype
        fs_q = str(item.get("first_seen_quarter_end") or "").strip()
        ls_q = str(item.get("last_seen_quarter_end") or "").strip()
        if bool(item.get("carry_forward")):
            lm = _qh_quarter_label(item.get("last_mentioned_quarter"))
            shown = f"{shown} (carry-fwd {lm})"
        else:
            if fs_q:
                fs_lbl = _qh_quarter_label(fs_q)
                shown = f"{shown} (stated {fs_lbl})"
        return shown

    def _qh_item_comment(item: Dict[str, Any]) -> str:
        src = dict(item.get("source") or {})
        analysis = dict(item.get("analysis") or {})
        intent_hits = ", ".join(list(analysis.get("intent_hits") or [])[:5])
        fs_q = str(item.get("first_seen_quarter_end") or "").strip()
        ls_q = str(item.get("last_seen_quarter_end") or "").strip()
        stated_txt = ""
        if fs_q or ls_q:
            fs_lbl = _qh_quarter_label(fs_q) if fs_q else "N/A"
            ls_lbl = _qh_quarter_label(ls_q) if ls_q else fs_lbl
            stated_txt = fs_lbl if fs_lbl == ls_lbl else f"{fs_lbl} -> {ls_lbl}"
        parts = [
            f"Evidence: {_qh_norm_txt(item.get('text') or '')}",
            _qh_source_comment(src),
            f"Score={float(item.get('score') or 0.0):.1f}",
            (f"Stated: {stated_txt}" if stated_txt else ""),
        ]
        if intent_hits:
            parts.append(f"Intent: {intent_hits}")
        return "\\n\\n".join([x for x in parts if x])

    _qh_current_items_cache: Dict[str, List[Dict[str, Any]]] = {}

    def _qh_state_key(item: Dict[str, Any]) -> str:
        metric_name = str(item.get("metric") or FORWARD_NOTES_LABEL)
        if metric_name == FORWARD_NOTES_LABEL:
            return f"{metric_name}|{glx_dedup_text_key(item.get('text'))[:120]}"
        pnorm = str(item.get("period_norm") or "UNK")
        gtype = str(item.get("guidance_type") or "")
        if pnorm in {"", "UNK"} and gtype in {"run-rate", "ongoing", "one-time", "ratio"}:
            pnorm = f"TYPE:{gtype}"
        return f"{metric_name}|{pnorm}"

    def _qh_quarter_ord(q: pd.Timestamp) -> int:
        qq = ((int(q.month) - 1) // 3) + 1
        return int(q.year) * 4 + qq

    def _qh_is_fy_asof(asof_ref: pd.Timestamp) -> bool:
        qq = ((int(asof_ref.month) - 1) // 3) + 1
        return qq == 4

    def _qh_keep_for_fy_asof(item: Dict[str, Any], asof_ref: pd.Timestamp) -> bool:
        # For FY as-of blocks, keep only genuinely forward-looking guidance
        # (future period or ongoing/run-rate style guidance).
        gtype = str(item.get("guidance_type") or "").strip().lower()
        if gtype in {"run-rate", "ongoing"}:
            return True
        pnorm = str(item.get("period_norm") or "UNK")
        asof_year = int(asof_ref.year)
        asof_ord = _qh_quarter_ord(pd.Timestamp(asof_ref))
        if pnorm == "FY+1":
            return True
        mfy = re.match(r"FY(20\d{2})$", pnorm)
        if mfy:
            return int(mfy.group(1)) > asof_year
        mq = re.match(r"Q(20\d{2})Q([1-4])$", pnorm)
        if mq:
            tgt_ord = int(mq.group(1)) * 4 + int(mq.group(2))
            return tgt_ord > asof_ord
        txt = _qh_norm_txt(item.get("text") or "").lower()
        years = [int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", txt)]
        if any(y > asof_year for y in years):
            return True
        if re.search(r"\b(next fiscal year|next year|following year)\b", txt):
            return True
        m_into = re.search(r"\b(?:into|through|by end of|in)\s+(20\d{2})\b", txt)
        if m_into and int(m_into.group(1)) > asof_year:
            return True
        return False

    def _qh_is_futureish_item(item: Dict[str, Any], asof_ref: pd.Timestamp) -> bool:
        gtype = str(item.get("guidance_type") or "").strip().lower()
        if gtype in {"run-rate", "ongoing", "annualized", "ratio", "milestone"}:
            return True
        pnorm = str(item.get("target_period_norm") or item.get("period_norm") or "UNK").strip()
        asof_ord = _qh_quarter_ord(pd.Timestamp(asof_ref))
        asof_year = int(asof_ref.year)
        if pnorm == "FY+1":
            return True
        mfy = re.match(r"FY(20\d{2})$", pnorm)
        if mfy and int(mfy.group(1)) >= asof_year:
            return True
        mq = re.match(r"Q(20\d{2})Q([1-4])$", pnorm)
        if mq:
            tgt_ord = int(mq.group(1)) * 4 + int(mq.group(2))
            return tgt_ord >= asof_ord
        txt = _qh_norm_txt(
            " | ".join(
                [
                    str(item.get("text") or ""),
                    str(item.get("exact_language") or ""),
                    str(item.get("qualitative_range_text") or ""),
                ]
            )
        ).lower()
        if not txt:
            return False
        if re.search(r"\b(next quarter|next year|following year|over the next year|coming quarters|on track|expected|expects|outlook|target|targets|by end of|into 20\d{2}|in 20\d{2})\b", txt, re.I):
            return True
        return False

    def _qh_is_clean_text_guidance_item(item: Dict[str, Any], asof_ref: pd.Timestamp) -> bool:
        metric_name = str(item.get("metric") or "").strip()
        if metric_name in {"", FORWARD_NOTES_LABEL, "Other", "Unknown"}:
            return False
        intent_hits = list(((item.get("analysis") or {}).get("intent_hits")) or [])
        if "gpre_commercial_setup" in intent_hits:
            return True
        txt = _qh_norm_txt(item.get("text") or "")
        if not txt:
            return False
        blob = " | ".join([metric_name, txt, str(item.get("period") or ""), str(item.get("target_period_norm") or item.get("period_norm") or "")]).lower()
        if _slide_signal_noise(txt):
            return False
        if re.search(r"\b(remain focused|positioned well|on the right path|feel good about our direction|possible near-term announcement)\b", blob, re.I):
            return False
        if re.search(r"\b(all 2025 credits marketed; some cash already received|inventories are not building as expected|plants came through the cold spell)\b", blob, re.I):
            return False
        if re.search(r"\b(historical results|for the year ended|three months ended|nine months ended|historical fact)\b", blob, re.I):
            return False
        measurable_theme = bool(
            re.search(
                r"\b(interest expense|45z|monetization|tax credit|qualif|strategic review|cost savings|run-rate|hedge|booked margin|facility|facilities|carbon capture|fully operational|startup|annualized|liquidity|capital allocation|deleverag|debt reduction|working capital|farm[- ]practice|yield|energy use|utilization|capex|capital expenditures?|sustaining capital|low-cost|low-carbon|adjusted ebitda|ebitda)\b",
                blob,
                re.I,
            )
        )
        if not measurable_theme:
            return False
        period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
        if period_norm:
            return True
        asof_year = int(asof_ref.year)
        future_years = [int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", blob)]
        if any(y >= asof_year for y in future_years):
            return True
        return bool(
            re.search(
                r"\b(next quarter|coming quarters?|next 12 months|next year|next fiscal year|annualized|by end of q[1-4]|during 20\d{2}|in 20\d{2}|into 20\d{2}|through 20\d{2}|sometime in 20\d{2})\b",
                blob,
                re.I,
            )
        )

    def _qh_is_clean_guidance_item(item: Dict[str, Any], asof_ref: pd.Timestamp) -> bool:
        metric_name = str(item.get("metric") or FORWARD_NOTES_LABEL)
        if metric_name == FORWARD_NOTES_LABEL:
            return False
        gtype = str(item.get("guidance_type") or "").strip().lower()
        if gtype == "text":
            return _qh_is_clean_text_guidance_item(item, asof_ref)
        if gtype not in {"period", "run-rate", "ongoing", "ratio"}:
            return False
        kind = str(item.get("kind") or "").strip().lower()
        if kind not in {"range", "point", "qualitative_range"}:
            return False
        if gtype in {"run-rate", "ongoing", "ratio"}:
            return True
        period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "UNK").strip()
        asof_ord = _qh_quarter_ord(pd.Timestamp(asof_ref))
        asof_year = int(asof_ref.year)
        if period_norm == "FY+1":
            return True
        m_fy = re.match(r"FY(20\d{2})$", period_norm)
        if m_fy:
            fy = int(m_fy.group(1))
            if fy > asof_year:
                return True
            # Current FY guidance is still forward-looking until Q4 is complete.
            if fy == asof_year and not _qh_is_fy_asof(pd.Timestamp(asof_ref)):
                return True
            return False
        m_q = re.match(r"Q(20\d{2})Q([1-4])$", period_norm)
        if m_q:
            tgt_ord = int(m_q.group(1)) * 4 + int(m_q.group(2))
            return tgt_ord > asof_ord
        return False

    def _qh_is_soft_guidance_item(item: Dict[str, Any], asof_ref: pd.Timestamp) -> bool:
        metric_name = str(item.get("metric") or "").strip()
        if metric_name in {"", FORWARD_NOTES_LABEL, "Other", "Unknown"}:
            return False
        intent_hits = list(((item.get("analysis") or {}).get("intent_hits")) or [])
        if "gpre_commercial_setup" in intent_hits:
            return True
        txt = _qh_norm_txt(item.get("text") or "")
        if not txt or _slide_signal_noise(txt):
            return False
        blob = " | ".join(
            [
                metric_name,
                txt,
                str(item.get("period") or ""),
                str(item.get("target_period_norm") or item.get("period_norm") or ""),
            ]
        ).lower()
        if re.search(r"\b(historical results|for the year ended|three months ended|nine months ended)\b", blob, re.I):
            return False
        if re.search(r"\b(remain focused|positioned well|confident in our ability|feel good about our direction)\b", blob, re.I):
            return False
        if not re.search(
            r"\b(interest expense|45z|monetization|tax credit|qualif|strategic review|cost savings|run-rate|hedge|booked margin|facility|facilities|carbon capture|fully operational|startup|annualized|liquidity|capital allocation|deleverag|debt reduction|working capital|farm[- ]practice|yield|energy use|utilization|capex|capital expenditures?|sustaining capital|adjusted ebitda|ebitda|forecast|guidance)\b",
            blob,
            re.I,
        ):
            return False
        return _qh_is_futureish_item(item, asof_ref)

    def _qh_repair_display_item(item: Dict[str, Any], asof_ref: pd.Timestamp) -> Dict[str, Any]:
        out_item = dict(item)
        if is_gpre_profile:
            metric_txt = str(out_item.get("metric") or "").strip()
            source_doc = str(
                ((out_item.get("source") or {}).get("doc") or out_item.get("source_file") or "")
            ).strip()
            item_blob = _qh_norm_txt(
                " | ".join(
                    [
                        metric_txt,
                        str(out_item.get("text") or ""),
                        str(out_item.get("exact_language") or ""),
                        str(out_item.get("qualitative_range_text") or ""),
                    ]
                )
            )
            item_blob_low = item_blob.lower()
            metric_repaired = _gpre_normalize_metric_label(metric_txt, item_blob)
            if metric_repaired:
                out_item["metric"] = metric_repaired
            if str(out_item.get("metric") or "").strip() == "Commercial positioning":
                out_item["metric"] = "Commercial positioning / setup"
            elif str(out_item.get("metric") or "").strip() == "Risk management":
                out_item["metric"] = "Risk-management setup"
            if metric_repaired == "Capex guidance (FY 2026)":
                out_item["period"] = str(out_item.get("period") or "FY 2026").strip()
                out_item["period_norm"] = str(out_item.get("period_norm") or "FY2026").strip()
                out_item["target_period_norm"] = str(out_item.get("target_period_norm") or "FY2026").strip()
                if not re.search(r"\$[0-9]", item_blob):
                    doc_text = _read_local_doc_text_shared(source_doc)
                    capex_match = re.search(
                        r"for 2026,\s*we expect sustaining capital expenditures[^.]{0,220}\$15(?:\.\d+)?\s*million-\$25(?:\.\d+)?\s*million",
                        doc_text,
                        re.I,
                    )
                    if capex_match:
                        out_item["text"] = _ensure_terminal_period(glx_normalize_text(capex_match.group(0)))
                        out_item["kind"] = "range"
                        out_item["guidance_type"] = "text"
                        out_item["unit"] = "$m"
                        out_item["low"] = 15_000_000.0
                        out_item["high"] = 25_000_000.0
            elif metric_repaired == "Interest expense outlook":
                out_item["period"] = str(out_item.get("period") or "FY 2026").strip()
                out_item["period_norm"] = str(out_item.get("period_norm") or "FY2026").strip()
                out_item["target_period_norm"] = str(out_item.get("target_period_norm") or "FY2026").strip()
            elif re.search(r"\b188\b", item_blob_low, re.I) and re.search(r"\b(starting point|upside)\b", item_blob_low, re.I):
                out_item["metric"] = "45Z EBITDA starting point and upside levers"
                out_item["period"] = str(out_item.get("period") or "FY 2026").strip()
                out_item["period_norm"] = str(out_item.get("period_norm") or "FY2026").strip()
                out_item["target_period_norm"] = str(out_item.get("target_period_norm") or "FY2026").strip()
            elif re.search(r"\b45z\b", item_blob_low, re.I) and re.search(
                r"\b(base case|facility qualification|indirect land use change|iluc)\b",
                item_blob_low,
                re.I,
            ):
                out_item["metric"] = "45Z base-case improvement"
                out_item["period"] = str(out_item.get("period") or "FY 2026").strip()
                out_item["period_norm"] = str(out_item.get("period_norm") or "FY2026").strip()
                out_item["target_period_norm"] = str(out_item.get("target_period_norm") or "FY2026").strip()
            elif re.search(r"\bfarm[- ]practice\b", item_blob_low, re.I):
                out_item["metric"] = "Farm-practice upside timing"
                out_item["period"] = str(out_item.get("period") or "FY 2026").strip()
                out_item["period_norm"] = str(out_item.get("period_norm") or "FY2026").strip()
                out_item["target_period_norm"] = str(out_item.get("target_period_norm") or "FY2026").strip()
            return out_item
        if not is_pbi_profile:
            return out_item
        metric_map = {
            "Revenue": "Revenue guidance",
            "Adj EBIT": "Adjusted EBIT guidance",
            "Adj EPS": "EPS guidance",
            "FCF": "FCF target",
        }
        metric_label = metric_map.get(str(out_item.get("metric") or "").strip())
        if not metric_label:
            return out_item
        repaired_norm, repaired_label = _pbi_repair_guidance_period_meta(
            metric_label,
            out_item.get("target_period_norm") or out_item.get("period_norm"),
            out_item.get("target_period_label") or out_item.get("period"),
            " | ".join(
                [
                    str(out_item.get("text") or ""),
                    str(out_item.get("target_period_label") or ""),
                    str(out_item.get("period") or ""),
                ]
            ),
            asof_ref.date(),
        )
        if repaired_norm:
            out_item["period_norm"] = repaired_norm
            out_item["target_period_norm"] = repaired_norm
        if repaired_label:
            out_item["period"] = repaired_label
            out_item["target_period_label"] = repaired_label
        return out_item

    def _qh_is_clean_commentary_item(item: Dict[str, Any], asof_ref: pd.Timestamp) -> bool:
        item_local = _qh_repair_display_item(item, asof_ref)
        txt = _qh_norm_txt(item_local.get("text") or "")
        if not txt:
            return False
        intent_hits = list((((item_local.get("analysis") or {})).get("intent_hits")) or [])
        if "gpre_commercial_setup" in intent_hits:
            return True
        blob = " | ".join([str(item_local.get("metric") or ""), txt]).lower()
        if _slide_signal_noise(txt):
            return False
        if re.search(r"\b(remain focused|positioned well|confident in our ability|believe we are well positioned)\b", blob, re.I):
            return False
        if re.search(r"\b(historical results|for the year ended|three months ended|nine months ended)\b", blob, re.I):
            return False
        if re.search(r"\b(recorded in operating income|cash flow hedge accounting|unless the contracts qualify)\b", blob, re.I):
            return False
        measurable_theme = bool(
            re.search(
                r"\b(interest expense|45z|monetization|tax credit|qualify|strategic review|cost savings|run-rate|hedge|booked margin|facility|facilities|carbon capture|fully operational|startup|annualized|liquidity|capital allocation|deleverag|debt reduction|working capital|farm[- ]practice|yield|energy use|low-cost|low-carbon)\b",
                blob,
                re.I,
            )
        )
        if not measurable_theme:
            return False
        if bool(item_local.get("_force_commentary")):
            return True
        if _qh_is_clean_guidance_item(item_local, asof_ref):
            return str(item_local.get("metric") or "") not in {"Revenue", "Adj EBIT", "Adj EPS", "FCF"}
        return bool(
            re.search(
                r"\b(expected|expect|outlook|target|targets|opportunity|on track|by end of|into 20\d{2}|in 20\d{2}|fully operational|annualized)\b",
                blob,
                re.I,
            )
            or re.search(r"\b(fully operational|online and ramping|agreement executed)\b", blob, re.I)
        )

    def _qh_commentary_fallback_bucket(item: Dict[str, Any], asof_ref: pd.Timestamp) -> Optional[Tuple[int, str]]:
        txt = _qh_panel_commentary_text(item, asof_ref)
        blob = glx_normalize_text(" | ".join([str(item.get("metric") or ""), txt, str(item.get("text") or "")])).lower()
        if not txt or not _qh_is_futureish_item(item, asof_ref):
            return None
        if re.search(r"\b(improve forecasting|accurate guidance|guidance beginning with)\b", blob, re.I):
            return (0, "forecasting")
        if re.search(r"\b(pb bank|optimi[sz]e cash|strengthen the balance sheet|working capital)\b", blob, re.I):
            return (1, "balance_sheet")
        if re.search(r"\b(tuck-in acquisition|accretive tuck-in|acquisition opportunities)\b", blob, re.I):
            return (2, "tuck_in")
        if re.search(r"\b(buyback program|repurchase program|retire .*notes?\b|retire the 2027 notes?\b)\b", blob, re.I):
            return (3, "capital_return")
        if re.search(r"\b(deleverag|target(?:ing)?\s*~?\s*3\.0x|3\.0x net debt to adjusted ebitda)\b", blob, re.I):
            return (4, "deleveraging")
        return None

    _qh_gpre_external_commentary_cache: Dict[str, List[Dict[str, Any]]] = {}
    _qh_pbi_local_commentary_cache: Dict[str, List[Dict[str, Any]]] = {}

    def _qh_gpre_commercial_setup_records() -> List[Dict[str, Any]]:
        return _gpre_commercial_setup_records_shared()

    def _qh_gpre_commercial_guidance_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        if not is_gpre_profile:
            return []
        asof_date = pd.Timestamp(asof_ref).date()
        latest_date = pd.Timestamp(q0_ref).date()
        items_out: List[Dict[str, Any]] = []
        for rec in _qh_gpre_commercial_setup_records():
            if not bool(rec.get("show_in_guidance", True)):
                continue
            src_q = rec.get("source_quarter")
            include = src_q == asof_date
            if not include and bool(rec.get("_future_latest_only")) and asof_date == latest_date:
                include = True
            if not include:
                continue
            guidance_text = str(rec.get("guidance_text") or "").strip()
            if not guidance_text:
                continue
            items_out.append(
                {
                    "metric": str(rec.get("guidance_metric") or "Commercial positioning"),
                    "text": guidance_text,
                    "period": str(rec.get("horizon_quarter") or ""),
                    "period_norm": str(rec.get("horizon_period_norm") or ""),
                    "target_period_norm": str(rec.get("horizon_period_norm") or ""),
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": float(rec.get("guidance_score") or 82.0),
                    "source_rank": 7,
                    "source_priority": 2,
                    "source_date": pd.to_datetime(rec.get("source_date"), errors="coerce"),
                    "source": dict(rec.get("source") or {}),
                    "analysis": {"intent_hits": ["gpre_commercial_setup"]},
                }
            )
        items_out.sort(
            key=lambda z: (
                -float(z.get("score") or 0.0),
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                str(z.get("metric") or ""),
            )
        )
        return [dict(x) for x in items_out[:2]]

    def _qh_gpre_commercial_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        if not is_gpre_profile:
            return []
        asof_date = pd.Timestamp(asof_ref).date()
        latest_date = pd.Timestamp(q0_ref).date()
        items_out: List[Dict[str, Any]] = []
        for rec in _qh_gpre_commercial_setup_records():
            src_q = rec.get("source_quarter")
            include = src_q == asof_date
            if not include and bool(rec.get("_future_latest_only")) and asof_date == latest_date:
                include = True
            if not include:
                continue
            commentary_text = str(rec.get("commentary_text") or "").strip()
            if not commentary_text:
                continue
            items_out.append(
                {
                    "metric": str(rec.get("guidance_metric") or "Commercial positioning"),
                    "text": commentary_text,
                    "period": str(rec.get("horizon_quarter") or ""),
                    "period_norm": str(rec.get("horizon_period_norm") or ""),
                    "target_period_norm": str(rec.get("horizon_period_norm") or ""),
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": float(rec.get("guidance_score") or 82.0),
                    "source_rank": 7,
                    "source_priority": 2,
                    "source_date": pd.to_datetime(rec.get("source_date"), errors="coerce"),
                    "_force_commentary": True,
                    "_commentary_priority": int(rec.get("commentary_priority") or 50),
                    "_commentary_display_text": commentary_text,
                    "source": dict(rec.get("source") or {}),
                    "analysis": {"intent_hits": ["gpre_commercial_setup"]},
                }
            )
        items_out.sort(
            key=lambda z: (
                int(z.get("_commentary_priority") or 99),
                -pd.Timestamp(z.get("source_date") if z.get("source_date") is not None else pd.Timestamp("1900-01-01")).value,
                str(z.get("metric") or ""),
            )
        )
        return [dict(x) for x in items_out[:1]]

    def _qh_pbi_local_letter_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        if not is_pbi_profile:
            return []
        cache_key = str(asof_ref.date())
        if cache_key in _qh_pbi_local_commentary_cache:
            return [dict(x) for x in _qh_pbi_local_commentary_cache[cache_key]]

        root_dir = Path(__file__).resolve().parents[2]
        pbi_cache_dir = root_dir / "sec_cache" / "PBI"
        if not pbi_cache_dir.exists():
            _qh_pbi_local_commentary_cache[cache_key] = []
            return []

        doc_globs = [
            "*q32025earningsceoletter*.htm",
            "*a2025pbannualletter*.htm",
            "*q12025earningspressrelea*.htm",
            "*q42024earningspressrelea*.htm",
        ]
        local_docs: List[Path] = []
        for pat in doc_globs:
            local_docs.extend(sorted(pbi_cache_dir.glob(pat)))

        def _read_doc(path_in: Path) -> str:
            try:
                return glx_normalize_text(path_in.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                return ""

        local_candidates: List[Tuple[str, str, str, float, str, Path, str]] = [
            (
                "Forecasting improvement",
                "FY 2026",
                "Improve forecasting to provide more accurate guidance beginning with 2026 guidance.",
                72.0,
                r"provide more accurate guidance to investors beginning with our 2026 guidance",
                Path(),
                "",
            ),
            (
                "PB Bank strategy",
                "FY 2026",
                "Realizing the potential of PB Bank to optimize cash, strengthen the balance sheet, and drive profitable growth.",
                70.0,
                r"realizing the potential of pb bank.*?optimi[sz]e cash.*?strengthen.*?balance sheet.*?profitable growth",
                Path(),
                "",
            ),
            (
                "Presort tuck-in acquisitions",
                "FY 2026",
                "Presort will more aggressively pursue accretive tuck-in acquisition opportunities.",
                68.0,
                r"more aggressively pursuing accretive tuck-?in acquisition opportunities",
                Path(),
                "",
            ),
            (
                "Capital allocation / note retirement",
                "March 2026",
                "Management expects to continue the buyback program and retire the 2027 Notes in full when callable in March 2026.",
                69.0,
                r"continue our buyback program.*?retire our 2027 notes in full.*?march 2026",
                Path(),
                "",
            ),
            (
                "Deleveraging target",
                "FY 2026",
                "Management plans to continue deleveraging in 2026 and target ~3.0x Net Debt to Adjusted EBITDA over the long term.",
                67.0,
                r"continue to deleverage in 2026.*?3\.0x net debt to adjusted ebitda",
                Path(),
                "",
            ),
        ]

        out_items: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        for doc_path in local_docs:
            doc_text = _read_doc(doc_path)
            if not doc_text:
                continue
            doc_key = doc_path.name.lower()
            if "q32025earningsceoletter" in doc_key:
                doc_date = pd.Timestamp("2025-11-06")
            elif "a2025pbannualletter" in doc_key:
                doc_date = pd.Timestamp("2026-02-11")
            elif "q12025earningspressrelea" in doc_key:
                doc_date = pd.Timestamp("2025-05-06")
            elif "q42024earningspressrelea" in doc_key:
                doc_date = pd.Timestamp("2025-02-11")
            else:
                doc_date = pd.Timestamp(asof_ref)
            for metric_label, period_label, text_label, score_val, regex_pat, _, _ in local_candidates:
                if not re.search(regex_pat, doc_text, re.I | re.S):
                    continue
                item_key = f"{metric_label.lower()}|{text_label.lower()}"
                if item_key in seen_keys:
                    continue
                seen_keys.add(item_key)
                out_items.append(
                    {
                        "metric": metric_label,
                        "text": text_label,
                        "period": period_label,
                        "period_norm": "FY2026" if "2026" in period_label else "",
                        "target_period_norm": "FY2026" if "2026" in period_label else "",
                        "guidance_type": "text",
                        "kind": "text",
                        "score": score_val,
                        "source_rank": 7,
                        "source_priority": 1,
                        "source_date": doc_date,
                        "_force_commentary": True,
                        "source": {
                            "source_type": "local_letter_fallback",
                            "doc": str(doc_path),
                        },
                    }
                )
        _qh_pbi_local_commentary_cache[cache_key] = out_items
        return [dict(x) for x in out_items]

    def _qh_pbi_tracker_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        tracker_seed_rows = ui_state.get("promise_tracker_rows_by_q", {}) if isinstance(ui_state, dict) else {}
        if not is_pbi_profile or not isinstance(tracker_seed_rows, dict):
            return []
        qd = asof_ref.date()
        out_items: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        for rec in tracker_seed_rows.get(qd, []) or []:
            metric_label = str(rec.get("metric_display") or rec.get("metric") or "").strip().replace("_", " ")
            text_full = glx_normalize_text(
                str(
                    rec.get("text_full")
                    or rec.get("text_snippet")
                    or rec.get("text")
                    or rec.get("claim")
                    or rec.get("note")
                    or ""
                )
            )
            target_display = str(rec.get("target_display") or rec.get("target") or "").strip()
            blob = glx_normalize_text(" | ".join([metric_label, text_full, target_display])).lower()
            if not blob:
                continue
            if re.search(r"\bstrategic review\b", blob, re.I):
                metric_name = "Strategic review timing"
            elif re.search(r"\bcost savings|annualized savings|run-rate savings|cost reduction\b", blob, re.I):
                metric_name = "Cost savings target"
            elif re.search(r"\b(pb bank|bank-held leases|cash optimization|cash needs reduction|trapped capital)\b", blob, re.I):
                metric_name = "PB Bank liquidity release"
            else:
                continue
            compact_text = text_full or target_display
            if not compact_text:
                continue
            item_key = f"{metric_name.lower()}|{glx_normalize_text(compact_text).lower()}"
            if item_key in seen_keys:
                continue
            seen_keys.add(item_key)
            period_label = (
                str(rec.get("target_period_label") or "").strip()
                or _pbi_guidance_period_label_from_text(compact_text)
            )
            period_norm = (
                str(rec.get("target_period_norm") or "").strip()
                or _period_label_to_norm(period_label)
            )
            src_doc = str(rec.get("doc") or rec.get("_source_doc") or "")
            src_type = str(rec.get("source_type") or rec.get("_source_type") or "promise_tracker")
            out_items.append(
                {
                    "metric": metric_name,
                    "text": compact_text,
                    "period": period_label,
                    "period_norm": period_norm,
                    "target_period_norm": period_norm,
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": float(rec.get("score") or 84.0),
                    "source_rank": 7,
                    "source_priority": 1,
                    "source_date": pd.Timestamp(asof_ref),
                    "_force_commentary": True,
                    "source": {
                        "source_type": src_type,
                        "doc": src_doc,
                        "form": "",
                        "section": "",
                    },
                }
            )
        return out_items

    def _qh_pbi_progress_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        if not is_pbi_profile or not isinstance(promise_progress, pd.DataFrame) or promise_progress.empty:
            return []
        if "quarter" not in promise_progress.columns:
            return []
        out_items: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        prog = promise_progress.copy()
        prog["quarter"] = pd.to_datetime(prog["quarter"], errors="coerce")
        prog = prog[prog["quarter"].notna() & (prog["quarter"].dt.date <= asof_ref.date())]
        if prog.empty:
            return []

        def _best_family_latest_text_local(family_name: str) -> str:
            best_txt = ""
            best_score = (-1, -1.0)
            for _, fam_rec in prog.sort_values("quarter", ascending=False).iterrows():
                metric_blob = glx_normalize_text(
                    " | ".join(
                        [
                            str(fam_rec.get("metric_ref") or fam_rec.get("metric_display") or fam_rec.get("metric") or ""),
                            str(fam_rec.get("target") or ""),
                            str(fam_rec.get("latest") or fam_rec.get("actual") or ""),
                            str(fam_rec.get("rationale") or ""),
                        ]
                    )
                ).lower()
                if family_name == "cost_savings":
                    if not re.search(r"\bcost savings|annualized savings|run-rate savings|cost reduction\b", metric_blob, re.I):
                        continue
                else:
                    if not re.search(r"\bpb bank\b|\bbank-held leases\b|\bliquidity release\b", metric_blob, re.I):
                        continue
                latest_raw_local = fam_rec.get("latest") if fam_rec.get("latest") is not None else fam_rec.get("actual")
                if isinstance(latest_raw_local, (int, float)) and not pd.isna(latest_raw_local):
                    latest_txt_local = _fmt_short_money_value_local(float(latest_raw_local))
                else:
                    latest_txt_local = glx_normalize_text(str(latest_raw_local or "")).strip()
                if latest_txt_local.lower() in {"", "nan", "none", "null", "n/a", "not yet measurable"}:
                    continue
                latest_amt_local = _parse_dollar_amount(latest_txt_local) or 0.0
                rec_q = pd.Timestamp(fam_rec.get("quarter")).date()
                score_local = (rec_q.toordinal(), latest_amt_local)
                if score_local > best_score:
                    best_score = score_local
                    best_txt = latest_txt_local
            return best_txt

        for _, rec in prog.sort_values("quarter", ascending=False).iterrows():
            metric_label = str(rec.get("metric_ref") or rec.get("metric_refs") or rec.get("metric") or "").strip().replace("_", " ")
            target_display = str(rec.get("target") or "").strip()
            latest_display = glx_normalize_text(str(rec.get("latest") or rec.get("actual") or ""))
            if latest_display.lower() in {"nan", "none", "null", "n/a"}:
                latest_display = ""
            rationale = glx_normalize_text(str(rec.get("rationale") or ""))
            source_snippet = ""
            source_evidence = rec.get("source_evidence_json")
            if isinstance(source_evidence, str) and source_evidence.strip().startswith(("{", "[")):
                try:
                    parsed_source = json.loads(source_evidence)
                    if isinstance(parsed_source, dict):
                        source_snippet = str(parsed_source.get("snippet") or "")
                    elif isinstance(parsed_source, list) and parsed_source:
                        first_hit = parsed_source[0]
                        if isinstance(first_hit, dict):
                            source_snippet = str(first_hit.get("snippet") or "")
                except Exception:
                    source_snippet = ""
            source_snippet = glx_normalize_text(source_snippet)
            blob = glx_normalize_text(" | ".join([metric_label, target_display, latest_display, rationale, source_snippet])).lower()
            metric_name = ""
            line_text = ""
            if re.search(r"\bstrategic review\b", blob, re.I):
                metric_name = "Strategic review timing"
                line_text = latest_display or source_snippet or rationale
            elif metric_label == "Cost savings target" or re.search(r"\bcost savings|annualized savings|run-rate savings\b", blob, re.I):
                metric_name = "Cost savings target"
                pretty_target = str(
                    _extract_pbi_target_display(
                        " | ".join([metric_name, source_snippet, target_display]),
                        "Cost savings target",
                    )
                    or ""
                ).strip()
                if not pretty_target and target_display:
                    try:
                        target_num = float(str(target_display).replace(",", ""))
                    except Exception:
                        target_num = None
                    if target_num is not None and target_num >= 1_000_000:
                        pretty_target = _fmt_short_money_value_local(target_num)
                if pretty_target:
                    line_text = f"Raised target to {pretty_target} annualized savings"
                    resolved_latest_display = latest_display or _best_family_latest_text_local("cost_savings")
                    if resolved_latest_display and resolved_latest_display.lower() not in {"", "not yet measurable"}:
                        latest_display = resolved_latest_display
                        line_text += f"; latest disclosed {latest_display}"
                    elif source_snippet:
                        source_low = source_snippet.lower()
                        if re.search(r"\bup from\b", source_low, re.I):
                            line_text += "; up from the prior target"
                        if re.search(r"\bremainder to be executed over the next year\b", source_low, re.I):
                            line_text += "; the remaining actions are expected over the next year"
                else:
                    line_text = source_snippet or rationale
            elif metric_label == "PB Bank liquidity release" or re.search(r"\b(pb bank|bank-held leases|cash optimization|cash needs reduction)\b", blob, re.I):
                metric_name = "PB Bank liquidity release"
                line_text = source_snippet or rationale or latest_display or _best_family_latest_text_local("pb_bank_liquidity") or target_display
            if not metric_name or not line_text:
                continue
            item_key = f"{metric_name.lower()}|{glx_normalize_text(line_text).lower()}"
            if item_key in seen_keys:
                continue
            seen_keys.add(item_key)
            out_items.append(
                {
                    "metric": metric_name,
                    "text": _ensure_terminal_period(line_text),
                    "period": str(rec.get("target_period_label") or ""),
                    "period_norm": str(rec.get("target_period_norm") or ""),
                    "target_period_norm": str(rec.get("target_period_norm") or ""),
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": 85.0,
                    "source_rank": 7,
                    "source_priority": 1,
                    "source_date": pd.Timestamp(asof_ref),
                    "_force_commentary": True,
                    "source": {
                        "source_type": str(rec.get("source_type") or "promise_progress"),
                        "doc": str(rec.get("doc") or ""),
                        "form": "",
                        "section": "",
                    },
                }
            )
        return out_items

    def _qh_pbi_rendered_progress_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        if not is_pbi_profile:
            return []
        qd = asof_ref.date()
        rendered_progress_seed = ui_state.get("promise_tracker_rows_by_q", {}) if isinstance(ui_state, dict) else {}
        if not isinstance(rendered_progress_seed, dict):
            return []
        out_items: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        for rec in rendered_progress_seed.get(qd, []) or []:
            if str(rec.get("row_type") or "").strip().lower() in {"section", "blank"}:
                continue
            metric_label = str(rec.get("metric_display") or rec.get("metric_ref") or "").strip()
            latest_display = glx_normalize_text(str(rec.get("latest") or ""))
            rationale = glx_normalize_text(str(rec.get("rationale") or ""))
            blob = glx_normalize_text(" | ".join([metric_label, latest_display, rationale])).lower()
            metric_name = ""
            line_text = ""
            if metric_label == "Strategic milestone" and re.search(r"\bstrategic review\b", blob, re.I):
                metric_name = "Strategic review timing"
                line_text = latest_display or rationale
            elif metric_label == "Cost savings target" and re.search(r"\bcost savings|annualized savings|run-rate savings\b", blob, re.I):
                metric_name = "Cost savings target"
                line_text = rationale or latest_display
            elif metric_label == "PB Bank liquidity release" and re.search(r"\b(pb bank|bank-held leases|cash optimization|cash needs reduction)\b", blob, re.I):
                metric_name = "PB Bank liquidity release"
                line_text = rationale or latest_display
            if not metric_name or not line_text:
                continue
            item_key = f"{metric_name.lower()}|{glx_normalize_text(line_text).lower()}"
            if item_key in seen_keys:
                continue
            seen_keys.add(item_key)
            out_items.append(
                {
                    "metric": metric_name,
                    "text": _ensure_terminal_period(line_text),
                    "period": str(rec.get("target_period_label") or ""),
                    "period_norm": str(rec.get("target_period_norm") or ""),
                    "target_period_norm": str(rec.get("target_period_norm") or ""),
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": 86.0,
                    "source_rank": 7,
                    "source_priority": 1,
                    "source_date": pd.Timestamp(asof_ref),
                    "_force_commentary": True,
                    "source": {
                        "source_type": "rendered_progress",
                        "doc": "",
                        "form": "",
                        "section": "",
                    },
                }
            )
        return out_items

    def _qh_pbi_quarter_note_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        if not is_pbi_profile:
            return []
        qd = asof_ref.date()
        out_items: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        if not isinstance(quarter_notes, pd.DataFrame) or quarter_notes.empty or "quarter" not in quarter_notes.columns:
            return []
        note_slice = quarter_notes.copy()
        note_slice["quarter"] = pd.to_datetime(note_slice["quarter"], errors="coerce")
        note_slice = note_slice[note_slice["quarter"].dt.date == qd]
        if note_slice.empty:
            return []
        for _, rec in note_slice.iterrows():
            note_txt = glx_normalize_text(
                str(
                    rec.get("note")
                    or rec.get("claim")
                    or rec.get("evidence_snippet")
                    or rec.get("text")
                    or rec.get("comment")
                    or ""
                )
            )
            if not note_txt:
                continue
            metric_name = ""
            if re.search(r"\bstrategic review\b", note_txt, re.I):
                metric_name = "Strategic review timing"
            elif re.search(r"\bcost savings|cost reduction|cost optimization\b", note_txt, re.I) and re.search(r"\b(target|run-rate|annualized|over the next year)\b", note_txt, re.I):
                metric_name = "Cost savings target"
            else:
                continue
            item_key = f"{metric_name.lower()}|{note_txt.lower()}"
            if item_key in seen_keys:
                continue
            seen_keys.add(item_key)
            out_items.append(
                {
                    "metric": metric_name,
                    "text": _ensure_terminal_period(note_txt),
                    "period": "",
                    "period_norm": "",
                    "target_period_norm": "",
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": float(pd.to_numeric(rec.get("score"), errors="coerce") or 84.0),
                    "source_rank": 7,
                    "source_priority": 1,
                    "source_date": pd.Timestamp(asof_ref),
                    "_force_commentary": True,
                    "source": {
                        "source_type": str(rec.get("doc_type") or ""),
                        "doc": str(rec.get("doc") or ""),
                        "form": "",
                        "section": "",
                    },
                }
            )
        return out_items

    def _qh_gpre_external_commentary_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        cache_key = str(asof_ref.date())
        cached = _qh_gpre_external_commentary_cache.get(cache_key)
        if cached is not None:
            return [dict(x) for x in cached]
        conf_path = Path(__file__).resolve().parents[2] / "sec_cache" / "GPRE" / "external" / "conferences" / "2026-02-26_bofa" / "structured_statements.json"
        out_items: List[Dict[str, Any]] = []
        local_bofa_path = _gpre_local_bofa_conference_path_shared()
        local_bofa_text = _gpre_local_bofa_conference_text_shared()
        local_stephens_path = _gpre_local_stephens_conference_path_shared()
        local_stephens_text = _gpre_local_stephens_conference_text_shared()
        local_stephens_raw_text = _gpre_local_stephens_conference_raw_text_shared()
        local_bmo_path = _gpre_local_bmo_conference_path_shared()
        local_bmo_text = _gpre_local_bmo_conference_text_shared()
        if conf_path.exists():
            try:
                conf_rows = json.loads(conf_path.read_text(encoding="utf-8"))
            except Exception:
                conf_rows = []
            if not isinstance(conf_rows, list):
                conf_rows = []
        else:
            conf_rows = []

        def _append_external(
            metric: str,
            text: str,
            source_row: Dict[str, Any],
            *,
            priority: int,
            period: str = "FY 2026",
            intent_hit: str = "external_conference_commentary",
        ) -> None:
            text_norm = _ensure_terminal_period(glx_normalize_text(text))
            if not text_norm:
                return
            out_items.append(
                {
                    "metric": metric,
                    "text": text_norm,
                    "period": period,
                    "period_norm": "FY2026" if "2026" in period else "",
                    "guidance_type": "text",
                    "kind": "qualitative_range",
                    "score": 84.0 - float(priority),
                    "source_rank": 8,
                    "source_priority": 1,
                    "source_date": pd.Timestamp(str(source_row.get("date") or "2026-02-26")),
                    "_force_commentary": True,
                    "_commentary_priority": int(priority),
                    "source": {
                        "source_type": "conference",
                        "doc": str(source_row.get("doc") or "external/conferences/2026-02-26_bofa/transcript.md"),
                        "form": "conference",
                        "section": str(source_row.get("source_location") or ""),
                        "event": str(source_row.get("event") or "Bank of America 2026 Global Agriculture & Materials Conference"),
                    },
                    "analysis": {"intent_hits": [intent_hit]},
                }
            )

        if pd.Timestamp(asof_ref).date() >= date(2026, 3, 31):
            current_guidance_source = {
                "date": "2026-05-08",
                "doc": "GPRE_Q1_2026_earnings_release",
                "source_location": "Q1 2026 earnings release / presentation",
                "event": "Green Plains Q1 2026 results",
            }
            _append_external(
                "45Z EBITDA guidance",
                "FY2026 45Z EBITDA contribution guidance is $200m-$225m.",
                current_guidance_source,
                priority=0,
            )
            _append_external(
                "45Z facility contribution split",
                "Advantage Nebraska contributes $140m-$165m; remaining facilities about $60m.",
                current_guidance_source,
                priority=1,
            )
            _append_external(
                "Farm-practice upside timing",
                "On-farm practices are excluded from FY2026 45Z guidance pending final Treasury guidance/calculator.",
                current_guidance_source,
                priority=2,
            )
            out_items.append(
                {
                    "metric": "Capex guidance (FY 2026)",
                    "text": "FY2026 sustaining capex guidance is $15.0m-$25.0m.",
                    "period": "FY 2026",
                    "period_norm": "FY2026",
                    "target_period_norm": "FY2026",
                    "guidance_type": "period",
                    "kind": "range",
                    "unit": "$m",
                    "low": 15_000_000.0,
                    "high": 25_000_000.0,
                    "score": 84.0 - 3.0,
                    "source_rank": 8,
                    "source_priority": 1,
                    "source_date": pd.Timestamp("2026-05-08"),
                    "_force_commentary": True,
                    "_commentary_priority": 3,
                    "source": {
                        "source_type": "earnings_release",
                        "doc": "GPRE_Q1_2026_earnings_release",
                        "form": "8-K",
                        "section": "Q1 2026 earnings release / presentation",
                        "event": "Green Plains Q1 2026 results",
                    },
                    "analysis": {"intent_hits": ["external_conference_commentary"]},
                }
            )

        for row in conf_rows:
            if not isinstance(row, dict):
                continue
            topic = str(row.get("topic") or "")
            subtopic = str(row.get("subtopic") or "")
            fam = str(row.get("promise_family") or "")
            if fam == "45Z EBITDA outlook" or (topic == "45Z and carbon" and subtopic == "Current EBITDA contribution"):
                if pd.Timestamp(asof_ref).date() >= date(2026, 3, 31):
                    continue
                _append_external(
                    "45Z EBITDA starting point and upside levers",
                    "Roughly $188m of 2026 EBITDA tied to 45Z and carbon value is the current starting point, with upside from yield, lower energy use and fast-return projects.",
                    row,
                    priority=2,
                )
            elif topic == "45Z and carbon" and subtopic == "Drivers of improved guidance":
                _append_external(
                    "45Z base-case improvement",
                    "Removal of indirect land use change from CI calculations, qualification of all facilities for 45Z, and Advantage Nebraska being fully operational improved the 2026 base case.",
                    row,
                    priority=1,
                )
            elif fam == "Farm-practices upside timing":
                if pd.Timestamp(asof_ref).date() >= date(2026, 3, 31):
                    continue
                _append_external(
                    "Farm-practice upside timing",
                    "The current $188m base does not include farm-practice benefits, and final USDA/DOE-linked guidance is expected sometime in 2026.",
                    row,
                    priority=3,
                )
            elif fam == "Credit monetization outlook":
                _append_external(
                    "2026 credit marketing outlook",
                    "Management said 2026 credits are being marketed and that interest is strong.",
                    row,
                    priority=0,
                )
            elif fam == "CO2 logistics evaluation milestone":
                _append_external(
                    "CO2 logistics evaluation",
                    "The company is evaluating truck or rail movement of liquid CO2 to sequestration sites for non-pipeline plants.",
                    row,
                    priority=4,
                )
            elif topic == "Capital allocation" and subtopic == "Free cash flow priorities":
                _append_external(
                    "Capital allocation priorities",
                    "Free-cash-flow priorities are plant quality and utilization first, then improving 45Z and the base business, then debt reduction, shareholder returns and M&A, with the goal of becoming a low-cost, low-carbon Midwest biofuel producer.",
                    row,
                    priority=5,
                    period="next 12 months",
                )
        if local_bmo_text and pd.Timestamp(asof_ref).date() >= date(2026, 3, 31):
            bmo_stub = {
                "date": "2026-05-13",
                "source_location": str(local_bmo_path.name),
                "doc": str(local_bmo_path),
                "event": "BMO Farm to Market Conference 2026",
            }
            if re.search(r"\b97\b", local_bmo_text, re.I) and re.search(r"\butilization\b", local_bmo_text, re.I):
                _append_external(
                    "BMO utilization update",
                    "Management highlighted Q1 utilization around 97%.",
                    bmo_stub,
                    priority=6,
                    period="Q2/FY 2026 commentary",
                )
                _append_external(
                    "BMO execution discipline update",
                    "Management cited better plant execution, hedging discipline and corn procurement.",
                    bmo_stub,
                    priority=7,
                    period="Q2/FY 2026 commentary",
                    intent_hit="gpre_commercial_setup",
                )
            if re.search(r"full plant network|all facilities currently qualify|not only the Nebraska", local_bmo_text, re.I):
                _append_external(
                    "BMO 45Z network scope",
                    "Management framed the updated 45Z opportunity as a full-network item, not only an Advantage Nebraska contribution.",
                    bmo_stub,
                    priority=8,
                    period="FY 2026 commentary",
                )
            if re.search(r"\bE15\b", local_bmo_text, re.I) and re.search(r"\bexport", local_bmo_text, re.I):
                _append_external(
                    "BMO demand support signals",
                    "Management pointed to E15, exports and low-carbon fuel policies as demand supports, while keeping timing and adoption pace as execution risks.",
                    bmo_stub,
                    priority=9,
                    period="medium-term commentary",
                )
        if not out_items and local_bofa_text:
            local_stub = {
                "date": "2026-02-26",
                "source_location": str(local_bofa_path.name),
                "doc": str(local_bofa_path),
                "event": "Bank of America 2026 Global Agriculture & Materials Conference",
            }
            if re.search(
                r"advantage nebraska project fully operational.*indirect land use.*all of our plants are qualifying for 45z credits",
                local_bofa_text,
                re.I,
            ):
                _append_external(
                    "45Z base-case improvement",
                    "Removal of indirect land use change from CI calculations, qualification of all facilities for 45Z, and Advantage Nebraska being fully operational improved the 2026 base case.",
                    local_stub,
                    priority=1,
                )
            if re.search(r"\$188 million", local_bofa_text, re.I) and re.search(r"\b(starting point|upside)\b", local_bofa_text, re.I):
                _append_external(
                    "45Z EBITDA starting point and upside levers",
                    "Roughly $188m of 2026 45Z-related Adjusted EBITDA is the current starting point, with upside from yield, lower energy use and fast-return projects.",
                    local_stub,
                    priority=2,
                )
            if re.search(r"did not assume .* farm(?:ing)? practices", local_bofa_text, re.I) and re.search(
                r"sometime in 2026|won.?t be able to really put a number to it yet",
                local_bofa_text,
                re.I,
            ):
                _append_external(
                    "Farm-practice upside timing",
                    "The current $188m base does not include farm-practice benefits, and final USDA/DOE-linked guidance is expected sometime in 2026.",
                    local_stub,
                    priority=3,
                )
        qd_local = pd.Timestamp(asof_ref).date()
        if (
            int(qd_local.year) == 2025
            and (((int(qd_local.month) - 1) // 3) + 1) == 4
            and local_stephens_text
            and not any(str(item.get("metric") or "").strip() == "Capex guidance (FY 2026)" for item in out_items)
        ):
            stephens_metadata_values: Dict[str, str] = {}
            try:
                if str(local_stephens_path.name).upper().endswith("_METADATA_EN.TXT"):
                    stephens_metadata_values = parse_metadata_key_values(
                        local_stephens_path.read_text(encoding="utf-8", errors="ignore")
                    )
            except Exception:
                stephens_metadata_values = {}
            capex_source_path = local_stephens_path
            capex_match = bool(stephens_metadata_values.get("maintenance_capex_annual_guidance_usd_m"))
            if not capex_match:
                capex_source_path = _gpre_local_stephens_conference_raw_path_shared()
                capex_match = re.search(
                    r"maintenance capital spend anywhere between \$15 million and \$25 million annually",
                    local_stephens_raw_text,
                    re.I,
                )
            if not capex_match:
                capex_match = re.search(
                    r"manage that capex number[^.]{0,220}between \$15 million and \$25 million",
                    local_stephens_raw_text,
                    re.I,
                )
            if capex_match:
                out_items.append(
                    {
                        "metric": "Capex guidance (FY 2026)",
                        "text": "Management said maintenance capital should run about $15 million-$25 million in 2026.",
                        "period": "FY 2026",
                        "period_norm": "FY2026",
                        "target_period_norm": "FY2026",
                        "guidance_type": "period",
                        "kind": "range",
                        "unit": "$m",
                        "low": 15_000_000.0,
                        "high": 25_000_000.0,
                        "score": 86.0,
                        "source_rank": 8,
                        "source_priority": 1,
                        "source_date": pd.Timestamp("2025-11-01"),
                        "source": {
                            "source_type": "conference",
                            "doc": str(capex_source_path),
                            "form": "conference",
                            "section": "CapEx",
                            "event": "Stephens Annual Investment Conference 2025",
                        },
                    }
                )
        deduped: List[Dict[str, Any]] = []
        seen_text: set[str] = set()
        for item in sorted(out_items, key=lambda z: (int(z.get("_commentary_priority") or 99), str(z.get("metric") or ""))):
            txt_key = glx_normalize_text(str(item.get("text") or "")).lower()
            if not txt_key or txt_key in seen_text:
                continue
            seen_text.add(txt_key)
            deduped.append(item)
        _qh_gpre_external_commentary_cache[cache_key] = [dict(x) for x in deduped]
        return [dict(x) for x in deduped]

    def _qh_extra_text_guidance_items(asof_ref: pd.Timestamp) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        if is_gpre_profile:
            candidates.extend(_qh_gpre_commercial_guidance_items(asof_ref))
            candidates.extend(_qh_gpre_external_commentary_items(asof_ref))
        else:
            candidates.extend(_qh_pbi_tracker_commentary_items(asof_ref))
            candidates.extend(_qh_pbi_progress_commentary_items(asof_ref))
            candidates.extend(_qh_pbi_quarter_note_commentary_items(asof_ref))
            for pbi_item in _pbi_structured_strategy_items_for_qd(asof_ref.date()):
                metric_label = str(pbi_item.get("metric_label") or "").strip()
                compact_note = str(pbi_item.get("compact_note") or pbi_item.get("text_full") or "").strip()
                if not metric_label or not compact_note:
                    continue
                candidates.append(
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
                        "source_date": pd.Timestamp(asof_ref),
                        "source": dict(pbi_item.get("source") or {}),
                        "analysis": {"intent_hits": ["structured_strategy_text_guidance"]},
                    }
                )
        out_items: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[str, str, str]] = set()
        for item in candidates:
            item_local = dict(item)
            item_guidance_type = str(item_local.get("guidance_type") or "").strip().lower()
            if item_guidance_type == "text":
                if not _qh_is_clean_text_guidance_item(item_local, asof_ref):
                    continue
            else:
                if not _qh_is_clean_guidance_item(item_local, asof_ref):
                    continue
            metric_key = str(item_local.get("metric") or "").strip().lower()
            period_key = str(item_local.get("target_period_norm") or item_local.get("period_norm") or "").strip().lower()
            text_key = glx_normalize_text(str(item_local.get("text") or "")).lower()
            dedupe_key = (metric_key, period_key, text_key)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            out_items.append(item_local)
        return out_items

    def _qh_commentary_horizon_priority(item: Dict[str, Any], asof_ref: pd.Timestamp) -> int:
        item_local = _qh_repair_display_item(item, asof_ref)
        pnorm = str(item_local.get("target_period_norm") or item_local.get("period_norm") or "").strip()
        blob = glx_normalize_text(
            " | ".join(
                [
                    str(item_local.get("metric") or ""),
                    str(item_local.get("_commentary_display_text") or ""),
                    str(item_local.get("text") or ""),
                    str(item_local.get("period") or ""),
                ]
            )
        ).lower()
        asof_ord = _qh_quarter_ord(pd.Timestamp(asof_ref))
        asof_year = int(asof_ref.year)
        m_q = re.match(r"Q(20\d{2})Q([1-4])$", pnorm)
        if m_q:
            tgt_ord = int(m_q.group(1)) * 4 + int(m_q.group(2))
            gap = tgt_ord - asof_ord
            if gap <= 1:
                return 0
            if gap <= 4:
                return 1
            return 4
        m_fy = re.match(r"FY(20\d{2})$", pnorm)
        if m_fy:
            fy = int(m_fy.group(1))
            if fy <= asof_year + 1:
                return 2
            return 4
        if re.search(r"\b(next quarter|coming quarters?|by end of q[1-4]\s*20\d{2}|by end of q[1-4])\b", blob, re.I):
            return 0
        if re.search(r"\b(next 12 months|annualized 20\d{2}|in 20\d{2}|during 20\d{2}|sometime in 20\d{2}|fy\s*20\d{2})\b", blob, re.I):
            return 1
        if re.search(r"\b(2027|2028|2029|2030|post-2029|long-term)\b", blob, re.I):
            return 5
        return 3

    def _qh_compact_commentary_text(item: Dict[str, Any], asof_ref: pd.Timestamp) -> str:
        item_local = _qh_repair_display_item(item, asof_ref)
        txt = _qh_norm_txt(item_local.get("text") or "")
        if not txt:
            return ""
        if not is_gpre_profile:
            return txt
        txt_norm = glx_normalize_text(txt)
        txt_low = txt_norm.lower()
        money_hits = _extract_money_targets_for_display(txt_norm)
        if re.search(r"\binterest expense\b", txt_low, re.I) and re.search(r"\b(expected|annualized|2026)\b", txt_low, re.I):
            if len(money_hits) >= 2:
                lo = min(float(money_hits[0]), float(money_hits[1]))
                hi = max(float(money_hits[0]), float(money_hits[1]))
                return _ensure_terminal_period(
                    f"Annualized 2026 interest expense expected at about {_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
                )
        if re.search(r"\b45z\b", txt_low, re.I) and re.search(r"\b(monetization)\b", txt_low, re.I) and re.search(r"\b(expected|outlook)\b", txt_low, re.I):
            target_txt = str(_extract_45z_monetization_target_display(txt_norm, asof_ref.date(), "") or "").strip()
            if target_txt:
                period_txt = "Q4 2025" if re.search(r"\b(q4|fourth quarter)\b", txt_low, re.I) else "45Z"
                return _ensure_terminal_period(f"{period_txt} 45Z monetization expected at {target_txt}")
        if re.search(r"\b45z\b", txt_low, re.I) and re.search(r"\badjusted ebitda\b", txt_low, re.I):
            if money_hits:
                amt = max(float(x) for x in money_hits)
                return _ensure_terminal_period(
                    f"FY 2026 45Z-related Adjusted EBITDA outlook at least {_fmt_short_money_value_local(amt)}"
                )
        if re.search(r"\ball eight\b", txt_low, re.I) and re.search(r"\bqualif", txt_low, re.I) and re.search(r"\b2026\b", txt_low, re.I):
            return _ensure_terminal_period("All eight operating ethanol plants expected to qualify for production tax credits in 2026")
        if re.search(r"\bfully operational\b", txt_low, re.I) and re.search(r"\b(central city|wood river|york)\b", txt_low, re.I):
            return _ensure_terminal_period("Carbon capture fully operational at Central City, Wood River and York, Nebraska facilities")
        return txt

    def _qh_panel_commentary_text(item: Dict[str, Any], asof_ref: pd.Timestamp) -> str:
        item_local = _qh_repair_display_item(item, asof_ref)
        metric_txt = str(item_local.get("metric") or "").strip()
        candidate = str(
            item_local.get("_commentary_display_text")
            or _qh_compact_commentary_text(item_local, asof_ref)
            or _qh_norm_txt(item_local.get("text") or "")
        ).strip()
        if not candidate:
            return ""
        candidate_low = glx_normalize_text(candidate).lower()
        if re.search(r"\b(recorded in operating income|cash flow hedge accounting|unless the contracts qualify)\b", candidate_low, re.I):
            return ""
        if not is_gpre_profile:
            if candidate_low.startswith("statements about future "):
                return ""
            if re.search(r"\bprovides the following (?:guidance|management target)\b", candidate_low, re.I):
                return ""
            if re.search(r"\boffers physical and digital shipping and mailing technology solutions\b", candidate_low, re.I):
                return ""
            if re.search(r"\bmanagement target\b", candidate_low, re.I) and candidate_low.count(":") >= 1 and len(candidate_low) > 120:
                return ""
            if candidate_low.count("fy 20") >= 2 and candidate_low.count(":") >= 3:
                return ""
            if re.search(r"\bgrowth y/y\b", candidate_low, re.I) and re.search(r"\bfy 20\d{2}:", candidate_low, re.I):
                return ""
        money_hits = [float(x) for x in _extract_money_targets_for_display(candidate)]
        raw_num_hits = [
            float(str(x).replace(",", ""))
            for x in re.findall(r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)", candidate)
        ]
        if len(money_hits) < 2 and len(raw_num_hits) >= 2:
            money_hits = raw_num_hits
        if is_gpre_profile and re.search(r"\binterest expense\b", candidate_low, re.I) and len(money_hits) >= 2:
            lo = min(float(money_hits[0]), float(money_hits[1]))
            hi = max(float(money_hits[0]), float(money_hits[1]))
            if hi < 1_000_000:
                lo *= 1_000_000.0
                hi *= 1_000_000.0
            return _ensure_terminal_period(
                f"Annualized 2026 interest expense expected at about {_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
            )
        if not is_gpre_profile:
            metric_map = {
                "Revenue": "Revenue guidance",
                "Adj EBIT": "Adjusted EBIT guidance",
                "Adj EPS": "EPS guidance",
                "FCF": "FCF target",
            }
            metric_label = metric_map.get(metric_txt, metric_txt)
            if re.search(r"\bcost savings\b", metric_label, re.I) or re.search(
                r"\b(cost savings|annualized savings|run-rate savings|cost reduction)\b",
                candidate_low,
                re.I,
            ):
                target_txt = str(
                    _extract_pbi_target_display(
                        " | ".join(
                            [
                                candidate,
                                str(item_local.get("text") or ""),
                                metric_label or "Cost savings target",
                            ]
                        ),
                        "Cost savings target",
                    )
                    or ""
                ).strip()
                if target_txt:
                    return _ensure_terminal_period(f"Raised target to {target_txt} annualized savings")
            if re.search(r"\bstrategic\b", metric_label, re.I) or re.search(r"\bstrategic review\b", candidate_low, re.I):
                sent_parts = [str(x).strip() for x in glx_split_sentences(candidate) if str(x).strip()]
                if sent_parts:
                    return _ensure_terminal_period(qn_compact_snippet(sent_parts[0], 180))
            period_txt = _qh_display_period(item_local) or str(item_local.get("period") or "")
            if metric_label in metric_map.values():
                structured_target = str(
                    _extract_pbi_target_display(
                        " | ".join(
                            [
                                candidate,
                                metric_label,
                                str(item_local.get("text") or ""),
                            ]
                        ),
                        metric_label,
                    )
                    or ""
                ).strip()
                if not structured_target and metric_label in {"Revenue guidance", "Adjusted EBIT guidance", "FCF target"} and len(money_hits) >= 2:
                    lo = min(float(money_hits[0]), float(money_hits[1]))
                    hi = max(float(money_hits[0]), float(money_hits[1]))
                    if hi < 10_000:
                        lo *= 1_000_000.0
                        hi *= 1_000_000.0
                    structured_target = f"{_fmt_short_money_value_local(lo)}-{_fmt_short_money_value_local(hi)}"
                if not structured_target and metric_label == "EPS guidance":
                    num_hits = re.findall(r"([0-9]+(?:\.[0-9]+)?)", candidate)
                    if len(num_hits) >= 2:
                        lo = min(float(num_hits[0]), float(num_hits[1]))
                        hi = max(float(num_hits[0]), float(num_hits[1]))
                        structured_target = f"${lo:.2f}-${hi:.2f}"
                if structured_target:
                    period_suffix = f" for {period_txt}" if period_txt else ""
                    return _ensure_terminal_period(f"{metric_label}{period_suffix} set at {structured_target}")
            sent_parts = [str(x).strip() for x in glx_split_sentences(candidate) if str(x).strip()]
            if sent_parts:
                candidate = sent_parts[0]
        return _ensure_terminal_period(qn_compact_snippet(candidate, 220))

    def _qh_pbi_commentary_priority(item: Dict[str, Any], asof_ref: pd.Timestamp) -> int:
        txt = _qh_panel_commentary_text(item, asof_ref).lower()
        metric_txt = str(item.get("metric") or "").strip().lower()
        blob = glx_normalize_text(" | ".join([metric_txt, txt])).lower()
        if re.search(r"\bstrategic review\b", blob, re.I):
            return 0
        if re.search(r"\bcost savings|annualized savings|run-rate savings|cost reduction\b", blob, re.I):
            return 1
        if re.search(r"\b(liquidity|working capital|capital allocation|deleverag|debt reduction)\b", blob, re.I):
            return 2
        if re.search(r"\b(next quarter|q1 20\d{2}|q2 20\d{2}|q3 20\d{2}|q4 20\d{2}|by end of q[1-4])\b", blob, re.I):
            return 3
        if metric_txt in {"revenue", "adj ebit", "adj eps", "fcf"}:
            return 8
        return 4

    def _qh_gpre_commentary_priority(item: Dict[str, Any], asof_ref: pd.Timestamp) -> int:
        explicit_priority = pd.to_numeric(item.get("_commentary_priority"), errors="coerce")
        if pd.notna(explicit_priority):
            return int(explicit_priority)
        txt = _qh_compact_commentary_text(item, asof_ref).lower()
        if not txt:
            return 99
        if re.search(r"\binterest expense\b", txt, re.I):
            return 0
        if re.search(r"\bcredits are being marketed\b|\binterest is strong\b", txt, re.I):
            return 1
        if re.search(r"\bindirect land use change\b|\ball facilities\b|\ball eight\b", txt, re.I):
            return 2
        if re.search(r"\bfarm-practice\b|\bfarm practice\b", txt, re.I):
            return 3
        if re.search(r"\b45z\b", txt, re.I) and re.search(r"\bmonetization\b", txt, re.I):
            return 4
        if re.search(r"\b45z-related adjusted ebitda\b", txt, re.I):
            return 5
        if re.search(r"\btruck\b|\brail\b|\bliquid co2\b", txt, re.I):
            return 6
        if re.search(r"\bcarbon capture\b", txt, re.I):
            return 7
        return 50

    def _qh_keep_carry_item(item: Dict[str, Any], asof_ref: pd.Timestamp, last_ref: pd.Timestamp) -> bool:
        metric_name = str(item.get("metric") or "")
        if metric_name == FORWARD_NOTES_LABEL:
            return False
        gtype = str(item.get("guidance_type") or "text")
        if metric_name == "Revenue" and str(item.get("kind") or "") == "range" and str(item.get("unit") or "") == "$m":
            try:
                lo_v = float(item.get("low")) if item.get("low") is not None else None
                hi_v = float(item.get("high")) if item.get("high") is not None else None
            except Exception:
                lo_v = hi_v = None
            if lo_v is None or hi_v is None:
                return False
            # Reject obviously broken carry-forward parses (e.g. 0-90000m).
            if hi_v > 50_000_000_000 or lo_v < 0:
                return False
        age_q = _qh_quarter_ord(pd.Timestamp(asof_ref)) - _qh_quarter_ord(pd.Timestamp(last_ref))
        if age_q <= 0:
            return False
        if gtype == "period" and age_q > 1:
            return False
        if age_q > guidance_carry_lookback_quarters:
            return False
        pnorm = str(item.get("period_norm") or "UNK")
        if pnorm.startswith("FY"):
            mfy = re.match(r"FY(20\d{2})$", pnorm)
            if mfy:
                fy = int(mfy.group(1))
                if fy < int(asof_ref.year) and gtype not in {"run-rate", "ongoing"}:
                    return False
            return True
        if pnorm.startswith("Q"):
            mq = re.match(r"Q(20\d{2})Q([1-4])$", pnorm)
            if mq:
                tgt_ord = int(mq.group(1)) * 4 + int(mq.group(2))
                return tgt_ord >= _qh_quarter_ord(pd.Timestamp(asof_ref))
            return False
        if pnorm == "FY+1":
            return True
        if pnorm in {"", "UNK"}:
            return gtype in {"run-rate", "ongoing", "one-time", "ratio"} and age_q <= guidance_carry_lookback_quarters
        return age_q <= 4

    def _qh_items_current_for(q_ref: Optional[pd.Timestamp]) -> List[Dict[str, Any]]:
        if q_ref is None:
            return []
        qk = str(pd.Timestamp(q_ref).date())
        if qk in _qh_current_items_cache:
            return [dict(x) for x in _qh_current_items_cache[qk]]
        prev_ref = _prev_ref_for(pd.Timestamp(q_ref))
        snap_local = _qh_build_guidance_snapshot(pd.Timestamp(q_ref), prev_ref)
        raw_items = list(snap_local.get("guidance_items") or [])
        out_items: List[Dict[str, Any]] = []
        for _it in raw_items:
            z = _qh_repair_display_item(dict(_it), pd.Timestamp(q_ref))
            z.setdefault("guidance_type", "text")
            z.setdefault("target_type", "text_only")
            z.setdefault("target_period", str(z.get("period_norm") or "unspecified"))
            z.setdefault("target_period_norm", str(z.get("period_norm") or "UNK"))
            z.setdefault("execution_window", "unspecified")
            z["as_of_quarter"] = qk
            z["as_of_quarter_end"] = qk
            z["last_mentioned_quarter"] = qk
            z["first_seen_quarter_end"] = str(z.get("first_seen_quarter_end") or qk)
            z["last_seen_quarter_end"] = str(z.get("last_seen_quarter_end") or qk)
            z["carry_forward"] = False
            out_items.append(z)
        _qh_current_items_cache[qk] = out_items
        return [dict(x) for x in out_items]
    def _qh_visible_items_for_block(block_ref: Optional[pd.Timestamp]) -> List[Dict[str, Any]]:
        if block_ref is None:
            return []
        block_asof_ref = pd.Timestamp(block_ref)
        block_asof_key = str(block_asof_ref.date())
        local_state_items: Dict[str, Dict[str, Any]] = {}
        for it in _qh_items_current_for(block_asof_ref):
            z = dict(it)
            z["as_of_quarter"] = block_asof_key
            z["last_mentioned_quarter"] = block_asof_key
            z["carry_forward"] = False
            local_state_items[_qh_state_key(z)] = z

        for old_ref in sorted([x for x in qhist_all if x < block_asof_ref], reverse=True):
            age_q = _qh_quarter_ord(block_asof_ref) - _qh_quarter_ord(old_ref)
            if age_q > guidance_carry_lookback_quarters:
                break
            for pit in _qh_items_current_for(old_ref):
                k = _qh_state_key(pit)
                if k in local_state_items:
                    continue
                if not _qh_keep_carry_item(pit, block_asof_ref, old_ref):
                    continue
                z = dict(pit)
                z["as_of_quarter"] = block_asof_key
                z["as_of_quarter_end"] = block_asof_key
                z["last_mentioned_quarter"] = str(pd.Timestamp(old_ref).date())
                z["carry_forward"] = True
                local_state_items[k] = z

        if _qh_is_fy_asof(block_asof_ref):
            local_state_items = {
                k: v for k, v in local_state_items.items()
                if _qh_keep_for_fy_asof(v, block_asof_ref)
            }

        state_items_pre_clean_local = dict(local_state_items)
        state_items_local = {
            k: v for k, v in state_items_pre_clean_local.items()
            if _qh_is_clean_guidance_item(v, block_asof_ref)
        }
        state_items_soft_local = {
            k: v for k, v in state_items_pre_clean_local.items()
            if _qh_is_soft_guidance_item(v, block_asof_ref)
        }
        items_all_local = sorted(
            list(state_items_local.values()),
            key=lambda x: (
                0 if not bool(x.get("carry_forward")) else 1,
                pri.get(str(x.get("metric") or FORWARD_NOTES_LABEL), 99),
                _period_sort_for_ui(str(x.get("period_norm") or "UNK")),
                -int(x.get("source_priority") or 0),
                -float(x.get("score") or 0),
            ),
        )
        updated_items_local = [x for x in items_all_local if not bool(x.get("carry_forward"))]
        carry_items_local = [x for x in items_all_local if bool(x.get("carry_forward"))]
        shown_updated_local = updated_items_local[:max_items_per_guidance_block]
        slots_left_local = max(0, max_items_per_guidance_block - len(shown_updated_local))
        shown_carry_local = carry_items_local[: (slots_left_local if shown_updated_local else max_items_per_guidance_block)]
        latest_asof_ord_local = _qh_quarter_ord(pd.Timestamp(q0_ref))
        current_asof_ord_local = _qh_quarter_ord(block_asof_ref)

        def _qh_allow_guidance_item_for_block_local(item: Dict[str, Any]) -> bool:
            stated_ord = _qh_quarter_ord_from_label(_qh_display_stated_in(item))
            if stated_ord is None or stated_ord <= current_asof_ord_local:
                return True
            return current_asof_ord_local == latest_asof_ord_local

        shown_updated_local = [x for x in shown_updated_local if _qh_allow_guidance_item_for_block_local(x)]
        shown_carry_local = [x for x in shown_carry_local if _qh_allow_guidance_item_for_block_local(x)]
        if is_gpre_profile and current_asof_ord_local == latest_asof_ord_local:
            def _is_stale_gpre_45z_starting_point_carry(item_in: Dict[str, Any]) -> bool:
                if not bool(item_in.get("carry_forward")):
                    return False
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
                    and re.search(r"\b188m\b", blob_local, re.I)
                )

            shown_carry_local = [
                x for x in shown_carry_local
                if not _is_stale_gpre_45z_starting_point_carry(x)
            ]

        if is_gpre_profile and current_asof_ord_local == latest_asof_ord_local and (len(shown_updated_local) + len(shown_carry_local)) < 4:
            existing_guidance_keys = {
                (
                    str(it.get("metric") or "").strip().lower(),
                    str(it.get("target_period_norm") or it.get("period_norm") or "").strip(),
                    glx_normalize_text(str(it.get("text") or "")).lower(),
                )
                for it in (shown_updated_local + shown_carry_local)
            }
            soft_candidates_local = sorted(
                [dict(v) for v in state_items_soft_local.values()],
                key=lambda x: (
                    0 if not bool(x.get("carry_forward")) else 1,
                    pri.get(str(x.get("metric") or FORWARD_NOTES_LABEL), 99),
                    _period_sort_for_ui(str(x.get("period_norm") or "UNK")),
                    -int(x.get("source_priority") or 0),
                    -float(x.get("score") or 0),
                ),
            )
            for soft_item in soft_candidates_local:
                if bool(soft_item.get("carry_forward")):
                    continue
                if not _qh_allow_guidance_item_for_block_local(soft_item):
                    continue
                soft_key = (
                    str(soft_item.get("metric") or "").strip().lower(),
                    str(soft_item.get("target_period_norm") or soft_item.get("period_norm") or "").strip(),
                    glx_normalize_text(str(soft_item.get("text") or "")).lower(),
                )
                if soft_key in existing_guidance_keys:
                    continue
                shown_updated_local.append(soft_item)
                existing_guidance_keys.add(soft_key)
                if (len(shown_updated_local) + len(shown_carry_local)) >= min(max_items_per_guidance_block, 8):
                    break
        return shown_updated_local + shown_carry_local

    return dict(locals())
