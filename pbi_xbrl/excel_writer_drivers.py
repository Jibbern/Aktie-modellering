"""Operating-driver templates, source indexing, and commentary helper logic."""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

from .guidance_lexicon import normalize_text as glx_normalize_text
from .non_gaap import infer_quarter_end_from_text
from .quarter_notes_lexicon import (
    is_complete_signal_text as qn_is_complete_signal_text,
)
from .quarter_notes_lexicon import compact_snippet as qn_compact_snippet

if TYPE_CHECKING:
    from .excel_writer_context import WriterContext


def write_driver_sheets(ctx: "WriterContext") -> None:
    from .excel_writer_core import ensure_driver_inputs

    ensure_driver_inputs(ctx)
    if ctx.data.enable_operating_drivers_sheet:
        ctx.callbacks.write_operating_drivers_sheet(ctx.data.operating_driver_history_rows)
    if ctx.data.enable_economics_overlay_sheet:
        ctx.callbacks.write_economics_overlay_sheet(ctx.data.operating_driver_history_rows)


def driver_source_display(source_type: Any, source_doc: Any = "") -> str:
    low = " ".join([str(source_type or ""), str(source_doc or "")]).lower()
    if "internal_metric" in low or "history_q" in low or "adjusted_metrics" in low:
        return "internal_metric"
    if any(tok in low for tok in ("earnings_presentation", "presentation", "slides_text", "slides_ocr", "slides")):
        return "presentation"
    if any(tok in low for tok in ("earnings_release", "press_release", "press release", "release")):
        return "earnings_release"
    if "transcript" in low:
        return "transcript"
    if any(tok in low for tok in ("10-q", "10q")):
        return "10-Q"
    if any(tok in low for tok in ("10-k", "10k")):
        return "10-K"
    return str(source_type or "") or "text"


def load_operating_driver_source_records(
    *,
    slide_text_paths_fn: Callable[..., List[Path]],
    read_cached_doc_raw_fn: Callable[[Path], str],
    follow_source_dirs_fn: Callable[[], List[Tuple[str, Path]]],
    read_operating_driver_text_fn: Callable[[Path], str],
    parse_quarter_from_filename_fn: Callable[[str], Optional[date]],
    parse_quarter_from_follow_text_fn: Callable[[str], Optional[date]],
    financial_statement_files_fn: Callable[[], List[Path]],
    quarter_notes: Any,
    promises: Any,
    promise_progress: Any,
    resolve_col_fn: Callable[[Any, List[str]], Optional[str]],
    source_rank_fn: Callable[[Any, Any], int],
    text_fragment_penalty_fn: Callable[[str], int],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    seen: Set[Tuple[date, str, str]] = set()

    def _append_record(
        qd: Optional[date],
        source_type: Any,
        source_doc: Any,
        raw_text: Any,
    ) -> None:
        if not isinstance(qd, date):
            return
        txt = glx_normalize_text(raw_text)
        if not txt:
            return
        doc_txt = str(source_doc or "").strip()
        display_type = driver_source_display(source_type, doc_txt)
        dkey = (qd, display_type, doc_txt)
        if dkey in seen:
            return
        seen.add(dkey)
        records.append(
            {
                "quarter": qd,
                "source_type": display_type,
                "source_internal": str(source_type or ""),
                "source_doc": doc_txt,
                "source_rank": int(source_rank_fn(display_type, doc_txt)),
                "text": txt,
                "_text_low": txt.lower(),
                "_fragment_penalty": int(text_fragment_penalty_fn(txt) or 0),
                "_is_complete_signal": bool(qn_is_complete_signal_text(txt)),
            }
        )

    for path_in in slide_text_paths_fn(kind="text"):
        qd = parse_quarter_from_filename_fn(path_in.name)
        if not isinstance(qd, date):
            continue
        raw_txt = read_cached_doc_raw_fn(path_in)
        if not raw_txt:
            continue
        _append_record(qd, "earnings_presentation", str(path_in), raw_txt)

    for source_type, src_dir in follow_source_dirs_fn():
        try:
            src_files = sorted([p for p in src_dir.iterdir() if p.is_file()])
        except Exception:
            continue
        for path_in in src_files:
            raw_txt = read_operating_driver_text_fn(path_in)
            if not raw_txt:
                continue
            qd = (
                parse_quarter_from_filename_fn(path_in.name)
                or parse_quarter_from_follow_text_fn(raw_txt)
                or infer_quarter_end_from_text(raw_txt)
            )
            _append_record(qd, source_type, str(path_in), raw_txt)

    for path_in in financial_statement_files_fn():
        raw_txt = read_operating_driver_text_fn(path_in)
        if not raw_txt:
            continue
        qd = (
            parse_quarter_from_filename_fn(path_in.name)
            or parse_quarter_from_follow_text_fn(raw_txt)
            or infer_quarter_end_from_text(raw_txt)
        )
        doc_name = path_in.name.lower()
        source_type = "10-K" if "10-k" in doc_name or "10k" in doc_name else "10-Q" if "10-q" in doc_name or "10q" in doc_name else "financial_statement"
        _append_record(qd, source_type, str(path_in), raw_txt)

    def _append_df_records(df_in: Any, source_type: str, text_aliases: List[str], doc_aliases: List[str]) -> None:
        if df_in is None or getattr(df_in, "empty", True):
            return
        q_col = resolve_col_fn(df_in, ["quarter", "quarter_end", "as_of_quarter", "stated_quarter"])
        txt_col = resolve_col_fn(df_in, text_aliases)
        doc_col = resolve_col_fn(df_in, doc_aliases)
        if q_col is None or txt_col is None:
            return
        df_local = df_in.copy()
        df_local[q_col] = df_local[q_col].apply(lambda x: x)
        try:
            import pandas as pd

            df_local[q_col] = pd.to_datetime(df_local[q_col], errors="coerce")
            df_local = df_local[df_local[q_col].notna()]
        except Exception:
            return
        for _, rr in df_local.iterrows():
            try:
                qd = pd.Timestamp(rr[q_col]).date()
            except Exception:
                continue
            txt = rr.get(txt_col)
            doc_txt = rr.get(doc_col) if doc_col else source_type
            _append_record(qd, source_type, doc_txt, txt)

    _append_df_records(
        quarter_notes,
        "quarter_note",
        ["text_full", "note", "claim", "statement", "headline"],
        ["doc_name", "source_doc", "doc_path", "doc"],
    )
    _append_df_records(
        promises,
        "promise",
        ["text_full", "promise_text", "target", "latest", "rationale"],
        ["source_doc", "doc_name", "doc_path", "doc"],
    )
    _append_df_records(
        promise_progress,
        "promise_progress",
        ["rationale", "latest", "target"],
        ["evidence", "source_doc", "doc_name"],
    )

    records.sort(
        key=lambda rec: (
            rec.get("quarter") or date.min,
            int(rec.get("source_rank") or 99),
            str(rec.get("source_doc") or ""),
        )
    )
    return records


def group_operating_driver_source_records_by_quarter(
    records: List[Dict[str, Any]],
) -> Dict[date, List[Dict[str, Any]]]:
    grouped: Dict[date, List[Dict[str, Any]]] = {}
    for rec in records:
        qd = rec.get("quarter")
        if not isinstance(qd, date):
            continue
        grouped.setdefault(qd, []).append(rec)
    return grouped


def build_operating_driver_line_index(
    records: List[Dict[str, Any]],
    *,
    text_fragment_penalty_fn: Callable[[str], int],
) -> Tuple[Dict[date, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    grouped: Dict[date, List[Dict[str, Any]]] = {}
    flat: List[Dict[str, Any]] = []
    for rec in records:
        qd = rec.get("quarter")
        raw_text = str(rec.get("text") or "")
        raw_lines = [glx_normalize_text(line).strip() for line in raw_text.splitlines()]
        lines = [line for line in raw_lines if line]
        if not lines:
            whole_text = glx_normalize_text(raw_text).strip()
            if whole_text:
                lines = [whole_text]
        seen_lines: Set[str] = set()
        for line_txt in lines:
            line_low = line_txt.lower()
            if not line_low or line_low in seen_lines:
                continue
            seen_lines.add(line_low)
            entry = {
                "record": rec,
                "quarter": qd,
                "line_txt": line_txt,
                "line_low": line_low,
                "has_numeric": bool(re.search(r"\d", line_txt)),
                "source_rank": int(rec.get("source_rank") or 99),
                "source_type": str(rec.get("source_type") or ""),
                "fragment_penalty": int(text_fragment_penalty_fn(line_txt) or 0),
                "is_complete_signal": bool(qn_is_complete_signal_text(line_txt)),
                "has_sentence_end": bool(re.search(r"[.!?]$", line_txt)),
            }
            flat.append(entry)
            if isinstance(qd, date):
                grouped.setdefault(qd, []).append(entry)
    return grouped, flat


def operating_driver_order_map(templates_in: List[Any]) -> Dict[str, int]:
    order_map: Dict[str, int] = {}
    for idx, tpl in enumerate(templates_in):
        base_key = str(getattr(tpl, "key", "") or "").strip().lower()
        order_map[base_key] = idx * 10
        if base_key == "ethanol_gallons":
            order_map["ethanol_gallons_produced"] = idx * 10
            order_map["ethanol_gallons_sold"] = idx * 10 + 1
    return order_map


def load_operating_driver_template_index(
    company_profile: Any,
    *,
    timed_substage: Callable[[str], Any],
) -> Dict[str, Any]:
    with timed_substage("write_excel.derive.driver_inputs.template_index"):
        templates = list(getattr(company_profile, "operating_driver_history_templates", ()) or [])
        template_by_key: Dict[str, Any] = {
            str(getattr(tpl, "key", "") or "").strip().lower(): tpl for tpl in templates
        }
        order_map = operating_driver_order_map(templates)
        template_unit_map: Dict[str, str] = {}
        template_specs: Dict[str, Dict[str, Any]] = {}
        for tpl in templates:
            base_key = str(getattr(tpl, "key", "") or "").strip().lower()
            pref_unit = str(getattr(tpl, "preferred_unit", "") or "").strip()
            group = str(getattr(tpl, "group", "") or "")
            label = str(getattr(tpl, "label", "") or "")
            match_terms = tuple(str(x).strip() for x in (getattr(tpl, "match_terms", ()) or ()) if str(x).strip())
            aliases = tuple(str(x).strip() for x in (getattr(tpl, "aliases", ()) or ()) if str(x).strip())
            search_terms = tuple(sorted({str(x).lower().strip() for x in list(match_terms) + list(aliases) if str(x).strip()}))
            if base_key:
                template_unit_map[base_key] = pref_unit
                template_specs[base_key] = {
                    "template": tpl,
                    "key": base_key,
                    "group": group,
                    "label": label,
                    "preferred_unit": pref_unit,
                    "match_terms": match_terms,
                    "aliases": aliases,
                    "search_terms": search_terms,
                    "parser_kind": base_key if base_key else "generic",
                }
            if base_key == "ethanol_gallons":
                template_unit_map["ethanol_gallons_produced"] = pref_unit
                template_unit_map["ethanol_gallons_sold"] = pref_unit
        return {
            "templates": templates,
            "template_by_key": template_by_key,
            "order_map": order_map,
            "template_unit_map": template_unit_map,
            "template_specs": template_specs,
        }


def load_operating_driver_45z_guidance_docs_by_quarter(
    cache_root: Optional[Path],
    *,
    read_operating_driver_text_fn: Callable[[Path], str],
    infer_cached_doc_quarter_fn: Callable[..., Optional[date]],
    extract_45z_target_candidates_fn: Callable[[Any, date], List[Dict[str, Any]]],
    extract_45z_target_display_fn: Callable[[Any, date], str],
    timed_substage: Callable[[str], Any],
) -> Dict[date, List[Dict[str, Any]]]:
    with timed_substage("write_excel.derive.driver_inputs.template_doc_index"):
        grouped: Dict[date, List[Dict[str, Any]]] = {}
        if cache_root is None or not cache_root.exists():
            return grouped
        for path_in in sorted(cache_root.glob("doc_*")):
            if path_in.suffix.lower() not in {".htm", ".html", ".txt"}:
                continue
            raw_txt = read_operating_driver_text_fn(path_in)
            if not raw_txt or "45z" not in raw_txt.lower():
                continue
            doc_q = infer_cached_doc_quarter_fn(path_in, text=raw_txt, include_follow_text=True)
            if not isinstance(doc_q, date):
                continue
            name_low = path_in.name.lower()
            if "earningsrelease" in name_low or "exhibit991" in name_low or "pressrelease" in name_low:
                doc_source_type = "earnings_release"
            else:
                doc_source_type = "10-K" if doc_q.month == 12 else "10-Q"
            grouped.setdefault(doc_q, []).append(
                {
                    "path": str(path_in),
                    "source_doc": str(path_in),
                    "source_type": doc_source_type,
                    "text": raw_txt,
                    "strong_targets": [dict(x) for x in extract_45z_target_candidates_fn(raw_txt, doc_q)],
                    "target_display": extract_45z_target_display_fn(raw_txt, doc_q),
                }
            )
        return grouped


def driver_snippet(text_in: Any, terms: Tuple[str, ...], max_chars: int = 180) -> str:
    txt = glx_normalize_text(text_in)
    if not txt:
        return ""
    low = txt.lower()
    idx = -1
    for term in terms:
        term_low = str(term or "").strip().lower()
        if not term_low:
            continue
        pos = low.find(term_low)
        if pos >= 0 and (idx < 0 or pos < idx):
            idx = pos
    if idx < 0:
        return qn_compact_snippet(txt, max_chars)
    start = max(0, idx - 80)
    end = min(len(txt), idx + max_chars)
    return qn_compact_snippet(txt[start:end], max_chars)


def driver_best_text_record(
    qd: date,
    terms: Tuple[str, ...],
    *,
    operating_driver_best_text_cache: Dict[Tuple[date, Tuple[str, ...], bool], Optional[Dict[str, Any]]],
    line_index_by_quarter: Dict[date, List[Dict[str, Any]]],
    source_records_by_quarter: Dict[date, List[Dict[str, Any]]],
    require_numeric: bool = False,
    quarter_records: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    search_terms = tuple(sorted({str(x).lower().strip() for x in terms if str(x).strip()}))
    if not search_terms:
        return None
    cache_key = (qd, search_terms, bool(require_numeric))
    if cache_key in operating_driver_best_text_cache:
        cached = operating_driver_best_text_cache.get(cache_key)
        return dict(cached) if cached is not None else None
    best: Optional[Dict[str, Any]] = None
    best_key: Optional[Tuple[float, int, int, int]] = None
    line_records = line_index_by_quarter.get(qd, [])
    for line_entry in line_records:
        line_txt = str(line_entry.get("line_txt") or "")
        line_low = str(line_entry.get("line_low") or "")
        hits = sum(1 for term in search_terms if term in line_low)
        if hits <= 0:
            continue
        if require_numeric and not bool(line_entry.get("has_numeric")):
            continue
        frag_pen = int(line_entry.get("fragment_penalty") or 0)
        clean_bonus = int(bool(line_entry.get("is_complete_signal"))) + int(bool(line_entry.get("has_sentence_end")))
        source_rank = int(line_entry.get("source_rank") or 0)
        score = float(hits * 5 + clean_bonus * 2 - frag_pen * 3 - source_rank * 2)
        key = (score, -source_rank, -frag_pen, -len(line_txt))
        if best_key is None or key > best_key:
            best_key = key
            rec = dict(line_entry.get("record") or {})
            best = dict(rec)
            best["snippet"] = driver_snippet(line_txt, search_terms)
    if best is None:
        records = quarter_records if quarter_records is not None else source_records_by_quarter.get(qd, [])
        for rec in records:
            txt = str(rec.get("text") or "")
            low = str(rec.get("_text_low") or txt.lower())
            hits = sum(1 for term in search_terms if term in low)
            if hits <= 0:
                continue
            if require_numeric and not re.search(r"\d", txt):
                continue
            frag_pen = int(rec.get("_fragment_penalty") or 0)
            clean_bonus = int(bool(rec.get("_is_complete_signal"))) + int(bool(re.search(r"[.!?]", txt)))
            score = float(hits * 5 + clean_bonus * 2 - frag_pen * 3 - int(rec.get("source_rank") or 0) * 2)
            key = (score, -int(rec.get("source_rank") or 0), -frag_pen, -len(txt))
            if best_key is None or key > best_key:
                best_key = key
                best = dict(rec)
                best["snippet"] = driver_snippet(txt, search_terms)
    operating_driver_best_text_cache[cache_key] = dict(best) if best is not None else None
    return best


def operating_driver_template_spec(tpl: Any, *, template_index: Dict[str, Any]) -> Dict[str, Any]:
    key = str(getattr(tpl, "key", "") or "").strip().lower()
    template_specs = dict(template_index.get("template_specs") or {})
    spec = template_specs.get(key)
    if spec is not None:
        return dict(spec)
    match_terms = tuple(str(x).strip() for x in (getattr(tpl, "match_terms", ()) or ()) if str(x).strip())
    aliases = tuple(str(x).strip() for x in (getattr(tpl, "aliases", ()) or ()) if str(x).strip())
    return {
        "template": tpl,
        "key": key,
        "group": str(getattr(tpl, "group", "") or ""),
        "label": str(getattr(tpl, "label", "") or ""),
        "preferred_unit": str(getattr(tpl, "preferred_unit", "") or ""),
        "match_terms": match_terms,
        "aliases": aliases,
        "search_terms": tuple(sorted({str(x).lower().strip() for x in list(match_terms) + list(aliases) if str(x).strip()})),
        "parser_kind": key if key else "generic",
    }


def template_candidate_terms(template_spec: Dict[str, Any]) -> Tuple[str, ...]:
    key = str(template_spec.get("key") or "").strip().lower()
    search_terms = tuple(template_spec.get("search_terms") or ())
    special_map = {
        "utilization": ("utilization", "production at", "operating rate", "capacity utilization", "stated capacity"),
        "ethanol_gallons": ("ethanol", "gallons"),
        "distillers_grains": ("distillers grains",),
        "ultra_high_protein": ("ultra-high protein", "uhp"),
        "corn_consumed": ("bushels of corn", "corn processed", "corn consumed"),
        "rin_impact_accumulated_rin_sale": ("rin",),
        "consolidated_ethanol_crush_margin": ("crush margin", "ethanol"),
        "45z_value_realized": ("45z", "production tax", "income tax benefit"),
        "45z_value_guided": ("45z", "monetization", "guidance", "target", "expected"),
        "renewable_corn_oil": ("renewable corn oil", "corn oil"),
        "protein_coproduct_mix": ("distillers grains", "ultra-high protein", "uhp", "coproduct"),
    }
    return special_map.get(key, search_terms)


def text_matches_template_terms(text_low: str, template_spec: Dict[str, Any]) -> bool:
    if not text_low:
        return False
    key = str(template_spec.get("key") or "").strip().lower()
    terms = template_candidate_terms(template_spec)
    if any(term in text_low for term in terms if term):
        return True
    if key == "utilization":
        return bool(
            re.search(
                r"\b(utilization|production at|operating rate|capacity utilization|stated capacity)\b",
                text_low,
                re.I,
            )
        )
    return False


def candidate_records_for_template(
    qd: date,
    template_spec: Dict[str, Any],
    *,
    operating_driver_template_candidate_cache: Dict[Tuple[date, str], List[Dict[str, Any]]],
    line_index_by_quarter: Dict[date, List[Dict[str, Any]]],
    source_records_by_quarter: Dict[date, List[Dict[str, Any]]],
    quarter_records: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    key = str(template_spec.get("key") or "").strip().lower()
    cache_key = (qd, key)
    cached = operating_driver_template_candidate_cache.get(cache_key)
    if cached is not None:
        return cached
    if quarter_records is None:
        quarter_records = source_records_by_quarter.get(qd, [])
    candidate_records: List[Dict[str, Any]] = []
    seen_records: Set[int] = set()
    for line_entry in line_index_by_quarter.get(qd, []):
        line_low = str(line_entry.get("line_low") or "")
        if not text_matches_template_terms(line_low, template_spec):
            continue
        rec = line_entry.get("record")
        if not isinstance(rec, dict):
            continue
        rec_id = id(rec)
        if rec_id in seen_records:
            continue
        seen_records.add(rec_id)
        candidate_records.append(rec)
    if not candidate_records:
        candidate_records = [
            rec
            for rec in quarter_records
            if text_matches_template_terms(str(rec.get("_text_low") or str(rec.get("text") or "").lower()), template_spec)
        ]
    if not candidate_records:
        candidate_records = list(quarter_records)
    operating_driver_template_candidate_cache[cache_key] = candidate_records
    return candidate_records


def load_operating_driver_bridge_bundle_map(
    quarter_set: List[date],
    *,
    operating_driver_bridge_bundle_cache: Dict[Tuple[date, ...], Dict[date, Dict[str, Any]]],
    source_records_by_quarter: Dict[date, List[Dict[str, Any]]],
    timed_substage: Callable[[str], Any],
    build_bundle_fn: Callable[[date, List[Dict[str, Any]]], Optional[Dict[str, Any]]],
) -> Dict[date, Dict[str, Any]]:
    quarter_key = tuple(sorted(qd for qd in quarter_set if isinstance(qd, date)))
    if not quarter_key:
        return {}
    cached_bundle_map = operating_driver_bridge_bundle_cache.get(quarter_key)
    if cached_bundle_map is not None:
        return cached_bundle_map
    with timed_substage("write_excel.derive.driver_inputs.bridge_bundle"):
        built: Dict[date, Dict[str, Any]] = {}
        for qd in quarter_key:
            best_bundle = build_bundle_fn(qd, source_records_by_quarter.get(qd, []))
            if best_bundle is not None:
                built[qd] = best_bundle
        operating_driver_bridge_bundle_cache[quarter_key] = built
    return operating_driver_bridge_bundle_cache[quarter_key]
