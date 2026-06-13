"""Operating Drivers source/model support for workbook writer."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

import pandas as pd

from .excel_writer_drivers import (
    candidate_records_for_template as driver_candidate_records_for_template,
    driver_best_text_record as driver_driver_best_text_record,
    driver_snippet as driver_driver_snippet,
    driver_source_display,
    group_operating_driver_source_records_by_quarter,
    load_operating_driver_45z_guidance_docs_by_quarter as driver_load_operating_driver_45z_guidance_docs_by_quarter,
    load_operating_driver_bridge_bundle_map as driver_load_operating_driver_bridge_bundle_map,
    load_operating_driver_source_records as driver_load_operating_driver_source_records,
    load_operating_driver_template_index as driver_load_operating_driver_template_index,
    operating_driver_order_map as driver_operating_driver_order_map,
    operating_driver_template_spec as driver_operating_driver_template_spec,
    build_operating_driver_line_index as driver_build_operating_driver_line_index,
    template_candidate_terms as driver_template_candidate_terms,
    text_matches_template_terms as driver_text_matches_template_terms,
)
from .operating_drivers_runtime import (
    OperatingDriversDeps,
    build_operating_drivers_history_rows as runtime_build_operating_drivers_history_rows,
    extract_gpre_45z_accounting_memo,
    extract_operating_driver_rows_for_template as runtime_extract_operating_driver_rows_for_template,
    format_operating_driver_delta as runtime_format_operating_driver_delta,
    gpre_canonical_crush_series_for_drivers as runtime_gpre_canonical_crush_series_for_drivers,
    make_driver_row as runtime_make_driver_row,
    merge_driver_rows as runtime_merge_driver_rows,
)


@dataclass(frozen=True)
class OperatingDriversSupportDeps:
    runtime: MutableMapping[str, Any]


class OperatingDriversSupport:
    def __init__(self, deps: OperatingDriversSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime
        self._install_support_functions()

    def refresh_runtime(self, runtime: MutableMapping[str, Any]) -> None:
        self.runtime.update(runtime)

    def _rt(self, name: str) -> Any:
        return self.runtime[name]

    def _install_support_functions(self) -> None:
        company_profile = self._rt("company_profile")
        hist = self._rt("hist")
        quarter_notes = self._rt("quarter_notes")
        promises = self._rt("promises")
        promise_progress = self._rt("promise_progress")
        adj_metrics = self._rt("adj_metrics")
        slides_segments = self._rt("slides_segments")
        is_gpre_profile = self._rt("is_gpre_profile")
        is_anf_profile = self._rt("is_anf_profile")
        ctx_ref = self._rt("ctx_ref")
        material_roots = self._rt("material_roots")
        ticker = self._rt("ticker")
        ticker_roots = self._rt("ticker_roots")
        cache_dir = self._rt("cache_dir")
        ui_info_rows = self._rt("ui_info_rows")
        operating_drivers_runtime = self._rt("operating_drivers_runtime")
        operating_driver_template_index_cache = operating_drivers_runtime.template_index_cache
        operating_driver_bridge_bundle_cache = operating_drivers_runtime.bridge_bundle_cache
        operating_driver_line_index_by_quarter_cache = operating_drivers_runtime.line_index_by_quarter_cache
        operating_driver_flat_line_index_cache = operating_drivers_runtime.flat_line_index_cache
        operating_driver_best_text_cache = operating_drivers_runtime.best_text_cache
        operating_driver_template_rows_cache = operating_drivers_runtime.template_rows_cache
        operating_driver_template_candidate_cache = operating_drivers_runtime.template_candidate_cache
        operating_driver_text_cache = operating_drivers_runtime.text_cache
        operating_driver_45z_guidance_docs_by_quarter_cache = operating_drivers_runtime.guidance_45z_docs_by_quarter_cache
        glx_normalize_text = self._rt("glx_normalize_text")
        qn_compact_snippet = self._rt("qn_compact_snippet")
        qn_is_complete_signal_text = self._rt("qn_is_complete_signal_text")
        _hist_view = self._rt("_hist_view")
        _load_profile_slide_signals = self._rt("_load_profile_slide_signals")
        _profile_slide_signals_for_quarter = self._rt("_profile_slide_signals_for_quarter")
        _filter_anf_quarterly_segment_actual_rows = self._rt("_filter_anf_quarterly_segment_actual_rows")
        _parse_quarter_from_filename = self._rt("_parse_quarter_from_filename")
        _parse_quarter_from_follow_text = self._rt("_parse_quarter_from_follow_text")
        _path_belongs_to_ticker = self._rt("_path_belongs_to_ticker")
        _path_cache_key = self._rt("_path_cache_key")
        _read_cached_doc_raw = self._rt("_read_cached_doc_raw")
        _read_cached_doc_text = self._rt("_read_cached_doc_text")
        _infer_cached_doc_quarter = self._rt("_infer_cached_doc_quarter")
        _slide_text_paths = self._rt("_slide_text_paths")
        _resolve_col = self._rt("_resolve_col")
        _source_rank = self._rt("_source_rank")
        _text_fragment_penalty = self._rt("_text_fragment_penalty")
        _timed_writer_substage = self._rt("_timed_writer_substage")
        _parse_gpre_crush_margin_pair_local = self._rt("_parse_gpre_crush_margin_pair_local")
        _extract_45z_2026_target_candidates = self._rt("_extract_45z_2026_target_candidates")
        _extract_45z_monetization_target_display = self._rt("_extract_45z_monetization_target_display")
        _extract_money_targets_for_display = self._rt("_extract_money_targets_for_display")
        _anf_compact_driver_label = self._rt("_anf_compact_driver_label")
        _anf_visible_quarter_label = self._rt("_anf_visible_quarter_label")

        def _build_operating_driver_rows() -> List[Dict[str, Any]]:
            templates = list(getattr(company_profile, "operating_driver_templates", ()) or [])
            if not templates:
                return []
            preferred_driver_source_re = re.compile(
                r"\b(earnings[_ ]release|earnings[_ ]presentation|transcript|slides?|quarter[_ ]notes?)\b",
                re.I,
            )
            latest_q_ts = None
            if hist is not None and not hist.empty and "quarter" in hist.columns:
                latest_q_ts = pd.to_datetime(hist["quarter"], errors="coerce").dropna().max()
            latest_q = pd.Timestamp(latest_q_ts) if pd.notna(latest_q_ts) else None

            def _driver_signal_from_df(df_in: pd.DataFrame, terms: Tuple[str, ...], text_cols: List[str], kind_label: str) -> Tuple[str, str]:
                if df_in is None or df_in.empty:
                    return "", ""
                df_local = df_in.copy()
                if latest_q is not None and "quarter" in df_local.columns:
                    df_local["quarter"] = pd.to_datetime(df_local["quarter"], errors="coerce")
                    df_local = df_local[df_local["quarter"].notna()]
                    if not df_local.empty:
                        same_q = df_local[df_local["quarter"].dt.to_period("Q") == latest_q.to_period("Q")]
                        if not same_q.empty:
                            df_local = same_q
                rows_scored: List[Tuple[float, str, str]] = []
                for _, rr in df_local.iterrows():
                    text_bits = [str(rr.get(col) or "") for col in text_cols if col in df_local.columns]
                    txt = glx_normalize_text(" ".join(text_bits))
                    if not txt:
                        continue
                    txt_low = txt.lower()
                    if re.search(r"\b(safe harbor|forward-looking statements|indenture|private securities litigation reform act)\b", txt_low, re.I):
                        continue
                    term_hits = sum(1 for term in terms if str(term).lower() in txt_low)
                    if term_hits <= 0:
                        continue
                    score = float(term_hits)
                    src_txt = " ".join(
                        [
                            str(rr.get("source_type") or ""),
                            str(rr.get("doc_name") or rr.get("doc") or ""),
                            str(rr.get("form") or ""),
                        ]
                    ).lower()
                    if preferred_driver_source_re.search(src_txt):
                        score += 3.0
                    rows_scored.append((score, qn_compact_snippet(txt, 160), kind_label))
                if not rows_scored:
                    return "", ""
                rows_scored.sort(key=lambda z: (-float(z[0]), len(z[1])))
                return rows_scored[0][1], rows_scored[0][2]

            def _driver_signal_from_slide_signals(terms: Tuple[str, ...]) -> Tuple[str, str]:
                slide_signals = _load_profile_slide_signals()
                if not slide_signals or latest_q is None:
                    return "", ""
                rows_scored: List[Tuple[float, str, str]] = []
                for rec in slide_signals:
                    rec_q = rec.get("quarter")
                    if rec_q is None:
                        continue
                    q_ts = pd.Timestamp(rec_q)
                    q_gap = abs((latest_q.to_period("Q").ordinal - q_ts.to_period("Q").ordinal))
                    if q_gap > 2:
                        continue
                    txt = glx_normalize_text(rec.get("text") or "")
                    txt_low = txt.lower()
                    term_hits = sum(1 for term in terms if str(term).lower() in txt_low)
                    if term_hits <= 0:
                        continue
                    score = float(rec.get("score") or 0.0) + (6.0 - min(q_gap, 5))
                    rows_scored.append((score, qn_compact_snippet(txt, 160), "earnings_presentation"))
                if not rows_scored:
                    return "", ""
                rows_scored.sort(key=lambda z: (-float(z[0]), len(z[1])))
                return rows_scored[0][1], rows_scored[0][2]

            def _driver_delta(metric_candidates: Tuple[str, ...]) -> str:
                if latest_q is None or hist is None or hist.empty:
                    return ""
                h_local = _hist_view().copy()
                if "_quarter" in h_local.columns:
                    h_local["quarter"] = h_local["_quarter"]
                h_local = h_local[h_local["quarter"].notna()].sort_values("quarter")
                if h_local.empty:
                    return ""
                latest_row = h_local[h_local["quarter"] == latest_q]
                prior_q = h_local[h_local["quarter"] < latest_q]["quarter"].max() if len(h_local) >= 2 else pd.NaT
                prior_y = latest_q - pd.DateOffset(years=1)
                candidate_map = {
                    "revenue": ("revenue", "Revenue YoY"),
                    "adj_ebitda": ("adj_ebitda", "Adj EBITDA YoY"),
                    "adj_ebit": ("adj_ebit", "Adj EBIT YoY"),
                    "debt_core": ("debt_core", "Debt QoQ"),
                    "interest_paid": ("interest_paid", "Interest TTM YoY"),
                    "cash": ("cash", "Cash QoQ"),
                }
                for cand in metric_candidates:
                    if cand not in candidate_map:
                        continue
                    col_name, label = candidate_map[cand]
                    if col_name not in h_local.columns or latest_row.empty:
                        continue
                    latest_val = pd.to_numeric(latest_row.iloc[-1].get(col_name), errors="coerce")
                    if pd.isna(latest_val):
                        continue
                    if cand in {"revenue", "adj_ebitda", "adj_ebit", "interest_paid"}:
                        ly_row = h_local[h_local["quarter"] == prior_y]
                        if ly_row.empty:
                            continue
                        prev_val = pd.to_numeric(ly_row.iloc[-1].get(col_name), errors="coerce")
                        if pd.isna(prev_val) or float(prev_val) == 0.0:
                            continue
                        pct = (float(latest_val) - float(prev_val)) / abs(float(prev_val))
                        return f"{label}: {pct*100:.1f}%"
                    if pd.isna(prior_q):
                        continue
                    pq_row = h_local[h_local["quarter"] == prior_q]
                    if pq_row.empty:
                        continue
                    prev_val = pd.to_numeric(pq_row.iloc[-1].get(col_name), errors="coerce")
                    if pd.isna(prev_val) or float(prev_val) == 0.0:
                        continue
                    pct = (float(latest_val) - float(prev_val)) / abs(float(prev_val))
                    return f"{label}: {pct*100:.1f}%"
                return ""

            def _driver_metric_signal(metric_candidates: Tuple[str, ...]) -> Tuple[str, str]:
                if latest_q is None or hist is None or hist.empty:
                    return "", ""
                h_local = _hist_view().copy()
                if "_quarter" in h_local.columns:
                    h_local["quarter"] = h_local["_quarter"]
                h_local = h_local[h_local["quarter"].notna()].sort_values("quarter")
                if h_local.empty:
                    return "", ""
                latest_row = h_local[h_local["quarter"] == latest_q]
                if latest_row.empty:
                    return "", ""
                metric_display = {
                    "revenue": "Revenue",
                    "adj_ebitda": "Adj EBITDA",
                    "adj_ebit": "Adj EBIT",
                    "debt_core": "Debt core",
                    "interest_paid": "Interest paid",
                    "cash": "Cash",
                }
                for cand in metric_candidates:
                    if cand not in metric_display or cand not in h_local.columns:
                        continue
                    val = pd.to_numeric(latest_row.iloc[-1].get(cand), errors="coerce")
                    if pd.isna(val):
                        continue
                    label = metric_display[cand]
                    if cand in {"revenue", "adj_ebitda", "adj_ebit", "debt_core", "interest_paid", "cash"}:
                        return f"History_Q latest {label}: ${float(val)/1e6:,.1f}m", "history_q"
                return "", ""

            rows: List[Dict[str, Any]] = []
            for tpl in templates:
                signal_txt, signal_src = _driver_signal_from_slide_signals(tuple(tpl.match_terms or ()))
                if not signal_txt:
                    signal_txt, signal_src = _driver_signal_from_df(
                        quarter_notes,
                        tuple(tpl.match_terms or ()),
                        ["text_full", "metric_tag", "metric_canon", "doc_name", "section_name"],
                        "quarter_note",
                    )
                if not signal_txt:
                    signal_txt, signal_src = _driver_signal_from_df(
                        promises,
                        tuple(tpl.match_terms or ()),
                        ["text_full", "metric_ref", "promise_text", "metric"],
                        "promise",
                    )
                if not signal_txt:
                    signal_txt, signal_src = _driver_metric_signal(tuple(tpl.metric_candidates or ()))
                rows.append(
                    {
                        "group": tpl.group,
                        "driver": tpl.label,
                        "why": tpl.why_it_matters,
                        "signal": signal_txt,
                        "delta": _driver_delta(tuple(tpl.metric_candidates or ())),
                        "source_type": signal_src,
                    }
                )
            return rows

        _bridge_fy_adj_ebitda_cache: Optional[List[Dict[str, Any]]] = None


        _operating_driver_source_cache: Optional[List[Dict[str, Any]]] = None
        _operating_driver_source_by_quarter_cache: Optional[Dict[date, List[Dict[str, Any]]]] = None
        _operating_driver_crush_detail_cache: Dict[str, Dict[str, Any]] = {}
        _operating_driver_metric_parse_cache: Dict[Tuple[str, str], Any] = {}

        def _operating_driver_quarters() -> List[date]:
            qset: set[date] = set()
            hist_dates: List[date] = []
            if hist is not None and not hist.empty and "quarter" in hist.columns:
                for qv in pd.to_datetime(hist["quarter"], errors="coerce").dropna():
                    try:
                        hist_dates.append(pd.Timestamp(qv).date())
                    except Exception:
                        continue
            latest_hist_q = max(hist_dates) if hist_dates else None
            for df_in in (hist, adj_metrics):
                if df_in is None or df_in.empty or "quarter" not in df_in.columns:
                    continue
                q_series = pd.to_datetime(df_in["quarter"], errors="coerce").dropna()
                for qv in q_series:
                    try:
                        qd = pd.Timestamp(qv).date()
                        if latest_hist_q is not None and qd > latest_hist_q:
                            continue
                        qset.add(qd)
                    except Exception:
                        continue
            return sorted(qset, reverse=True)

        def _driver_source_display(source_type: Any, source_doc: Any = "") -> str:
            return driver_source_display(source_type, source_doc)

        def _driver_source_note(source_doc: Any, snippet: Any = "", extra: Any = "") -> str:
            bits = [str(source_doc or "").strip(), str(extra or "").strip(), str(snippet or "").strip()]
            return "\n\n".join([x for x in bits if x])[:32000]

        def _read_operating_driver_text(path_in: Path) -> str:
            cache_key = _path_cache_key(path_in)
            cached = operating_driver_text_cache.get(cache_key)
            if cached is not None:
                return cached
            txt = _read_cached_doc_text(path_in)
            operating_driver_text_cache[cache_key] = txt
            return txt

        def _operating_driver_follow_source_dirs() -> List[Tuple[str, Path]]:
            dirs: List[Tuple[str, Path]] = []
            seen: set[str] = set()
            source_name_map = [
                ("earnings_release", ["earnings_release", "Earnings Release", "Earnings Releases", "press_release", "Press Release"]),
                ("earnings_presentation", ["earnings_presentation", "Earnings Presentation", "slides"]),
                ("transcript", ["earnings_transcripts", "Earnings Transcripts", "transcripts"]),
                ("ceo_letter", ["CEO letters", "CEO_letters", "ceo_letters"]),
            ]
            for root in material_roots:
                for source_type, names in source_name_map:
                    for name in names:
                        src_dir = root / name
                        if not src_dir.exists() or not src_dir.is_dir():
                            continue
                        if not _path_belongs_to_ticker(src_dir, ticker, ticker_roots):
                            continue
                        try:
                            key = str(src_dir.resolve())
                        except Exception:
                            key = str(src_dir)
                        if key in seen:
                            continue
                        seen.add(key)
                        dirs.append((source_type, src_dir))
            return dirs

        def _operating_driver_financial_statement_files() -> List[Path]:
            files: List[Path] = []
            seen: set[str] = set()
            for root in material_roots:
                fs_dir = root / "financial_statement"
                if not fs_dir.exists() or not fs_dir.is_dir():
                    continue
                try:
                    cand_files = sorted([p for p in fs_dir.iterdir() if p.is_file()])
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
                    files.append(path_in)
            return files

        def _load_operating_driver_source_records() -> List[Dict[str, Any]]:
            nonlocal _operating_driver_source_cache
            if _operating_driver_source_cache is not None:
                return list(_operating_driver_source_cache)
            _operating_driver_source_cache = driver_load_operating_driver_source_records(
                slide_text_paths_fn=_slide_text_paths,
                read_cached_doc_raw_fn=_read_cached_doc_raw,
                follow_source_dirs_fn=_operating_driver_follow_source_dirs,
                read_operating_driver_text_fn=_read_operating_driver_text,
                parse_quarter_from_filename_fn=_parse_quarter_from_filename,
                parse_quarter_from_follow_text_fn=_parse_quarter_from_follow_text,
                financial_statement_files_fn=_operating_driver_financial_statement_files,
                quarter_notes=quarter_notes,
                promises=promises,
                promise_progress=promise_progress,
                resolve_col_fn=_resolve_col,
                source_rank_fn=_source_rank,
                text_fragment_penalty_fn=_text_fragment_penalty,
            )
            return list(_operating_driver_source_cache)

        def _load_operating_driver_source_records_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
            nonlocal _operating_driver_source_by_quarter_cache
            if _operating_driver_source_by_quarter_cache is not None:
                return _operating_driver_source_by_quarter_cache
            _operating_driver_source_by_quarter_cache = group_operating_driver_source_records_by_quarter(
                _load_operating_driver_source_records()
            )
            return _operating_driver_source_by_quarter_cache

        def _load_operating_driver_line_index_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
            nonlocal operating_driver_line_index_by_quarter_cache, operating_driver_flat_line_index_cache
            if operating_driver_line_index_by_quarter_cache is not None and operating_driver_flat_line_index_cache is not None:
                if ctx_ref is not None:
                    ctx_ref.derived.operating_driver_line_index_by_quarter = operating_driver_line_index_by_quarter_cache
                    ctx_ref.derived.operating_driver_flat_line_index = operating_driver_flat_line_index_cache
                    ctx_ref.derived.operating_driver_best_text_cache = operating_driver_best_text_cache
                    ctx_ref.derived.operating_driver_template_rows_cache = operating_driver_template_rows_cache
                    ctx_ref.derived.operating_driver_template_candidate_cache = operating_driver_template_candidate_cache
                return operating_driver_line_index_by_quarter_cache

            grouped, flat = driver_build_operating_driver_line_index(
                _load_operating_driver_source_records(),
                text_fragment_penalty_fn=_text_fragment_penalty,
            )
            operating_driver_line_index_by_quarter_cache = grouped
            operating_driver_flat_line_index_cache = flat
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_line_index_by_quarter = grouped
                ctx_ref.derived.operating_driver_flat_line_index = flat
                ctx_ref.derived.operating_driver_best_text_cache = operating_driver_best_text_cache
                ctx_ref.derived.operating_driver_template_rows_cache = operating_driver_template_rows_cache
                ctx_ref.derived.operating_driver_template_candidate_cache = operating_driver_template_candidate_cache
            return grouped

        def _load_operating_driver_flat_line_index() -> List[Dict[str, Any]]:
            _load_operating_driver_line_index_by_quarter()
            return list(operating_driver_flat_line_index_cache or [])

        def _is_crush_margin_bridge_candidate(text_in: Any) -> bool:
            low = glx_normalize_text(text_in).lower()
            if not low:
                return False
            return any(
                token in low
                for token in (
                    "crush margin",
                    "45z",
                    "rin",
                    "underlying crush",
                    "assets held for sale",
                    "decommissioning",
                    "inventory lower of cost",
                    "intercompany fees",
                    "nonethanol operating activities",
                )
            )

        def _parse_driver_number(token: Any) -> Optional[float]:
            tok = glx_normalize_text(str(token or ""))
            if not tok:
                return None
            try:
                tok = tok.replace("−", "-").replace("–", "-").replace("—", "-")
                tok = tok.replace("â€”", "-").replace("—", "-")
                neg = False
                if re.match(r"^\s*-\s*", tok):
                    neg = True
                if re.match(r"^\s*\$?\s*\(", tok) or re.match(r"^\s*\(", tok):
                    neg = True
                if re.search(r"\(\s*\$?\s*\d", tok):
                    neg = True
                tok = re.sub(r"(?i)\b(million|thousand|m|mm)\b", "", tok)
                num_match = re.search(r"[-+]?\d+(?:,\d{3})*(?:\.\d+)?", tok)
                if not num_match:
                    return None
                val = float(num_match.group(0).replace(",", ""))
                if neg and val > 0:
                    val *= -1.0
                return val
            except Exception:
                return None

        def _strip_inline_footnotes(text_in: Any) -> str:
            txt = glx_normalize_text(text_in)
            if not txt:
                return ""
            return re.sub(r"\(\d+\)", "", txt)

        def _extract_driver_numeric_values(text_in: Any) -> List[float]:
            txt = _strip_inline_footnotes(text_in)
            if not txt:
                return []
            pat = re.compile(
                r"(?<!\d)"
                r"(?:"
                r"\$?\s*\(\s*[0-9]{1,4}(?:,[0-9]{3})*(?:\.\d+)?\s*\)(?:\s*(?:million|m))?"
                r"|[-+]?\$?\s*[0-9]{1,4}(?:,[0-9]{3})*(?:\.\d+)?(?:\s*(?:million|m))?"
                r")"
                r"(?!\d)",
                re.I,
            )
            out: List[float] = []
            for m in pat.finditer(txt):
                token = str(m.group(0) or "").strip()
                if not token:
                    continue
                raw_digits = re.sub(r"[^\d]", "", token)
                if raw_digits.isdigit() and len(raw_digits) == 4:
                    try:
                        year_val = int(raw_digits)
                    except Exception:
                        year_val = 0
                    if 1900 <= year_val <= 2100 and not any(ch in token for ch in "$(),.-"):
                        continue
                val = _parse_driver_number(token)
                if val is None:
                    continue
                out.append(float(val))
            return out

        def _cached_driver_metric_parse(
            metric_key: str,
            text_in: Any,
            parser: Callable[..., Any],
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            txt = str(text_in or "")
            cache_key = (str(metric_key or ""), txt)
            if cache_key in _operating_driver_metric_parse_cache:
                return _operating_driver_metric_parse_cache[cache_key]
            parsed = parser(txt, *args, **kwargs)
            _operating_driver_metric_parse_cache[cache_key] = parsed
            return parsed

        def _crush_source_bundle_key(source_doc: Any) -> str:
            doc_txt = str(source_doc or "").strip().lower()
            if not doc_txt:
                return ""
            base = Path(doc_txt).name
            base = re.sub(r"_p\d+\.(txt|pdf|html?)$", r".\1", base, flags=re.I)
            base = re.sub(r"-p\d+\.(txt|pdf|html?)$", r".\1", base, flags=re.I)
            return base

        def _parse_named_driver_metric_value_m(
            text_in: Any,
            patterns: Tuple[str, ...],
            prefer_before: bool = False,
        ) -> Optional[float]:
            raw_txt = str(text_in or "")
            if not raw_txt:
                return None

            def _candidate_score(val: float, window_txt: str, side: str) -> int:
                score = 0
                if side == ("before" if prefer_before else "after"):
                    score += 3
                if 1900 <= abs(float(val)) <= 2100:
                    score -= 20
                if re.match(r"^\s*\d+\.\s*includes\b", window_txt, re.I) and abs(float(val)) < 10 and float(val).is_integer():
                    score -= 20
                if "million" in window_txt.lower() or "$" in window_txt:
                    score += 2
                if abs(float(val)) <= 500:
                    score += 1
                return score

            def _match_near_value(scope_txt: str, pat: str) -> Optional[float]:
                for m in re.finditer(pat, scope_txt, re.I):
                    after_txt = scope_txt[m.end() : min(len(scope_txt), m.end() + 100)]
                    before_txt = scope_txt[max(0, m.start() - 100) : m.start()]
                    after_vals = _extract_driver_numeric_values(after_txt)
                    before_vals = _extract_driver_numeric_values(before_txt)
                    candidates: List[Tuple[int, float]] = []
                    if before_vals:
                        candidates.append((_candidate_score(float(before_vals[0]), before_txt, "before"), float(before_vals[0])))
                    if after_vals:
                        candidates.append((_candidate_score(float(after_vals[0]), after_txt, "after"), float(after_vals[0])))
                    if candidates:
                        candidates.sort(key=lambda x: x[0], reverse=True)
                        return float(candidates[0][1])
                return None

            lines = [_strip_inline_footnotes(ln).strip() for ln in raw_txt.splitlines() if str(ln).strip()]
            for ln in lines:
                for pat in patterns:
                    val = _match_near_value(ln, pat)
                    if val is not None:
                        return val

            txt = _strip_inline_footnotes(raw_txt)
            if not txt:
                return None
            for pat in patterns:
                val = _match_near_value(txt, pat)
                if val is not None:
                    return val
            return None

        def _parse_crush_45z_component_m(text_in: Any, require_bridge_context: bool = False) -> Optional[float]:
            raw_txt = str(text_in or "")
            if not raw_txt:
                return None
            lines = [_strip_inline_footnotes(ln).strip() for ln in raw_txt.splitlines() if str(ln).strip()]
            for ln in lines:
                low = ln.lower()
                if "consolidated ethanol crush margin" in low and "45z" in low:
                    m = re.search(
                        r"45z production tax credits?(?:[^0-9$]{0,40})(?:of\s*)?(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)",
                        ln,
                        re.I,
                    )
                    if m:
                        val = _parse_driver_number(m.group(1))
                        if val is not None:
                            return float(val)
            txt = glx_normalize_text(raw_txt)
            if require_bridge_context and not (
                re.search(r"\bconsolidated crush margin\b", txt, re.I)
                and re.search(r"\bethanol production\b", txt, re.I)
            ):
                return None
            m = re.search(
                r"45z production tax credits?\s*(?:\(\d+\)\s*)?([0-9]{1,3}(?:\.\d+)?)\b",
                txt,
                re.I,
            )
            if m:
                val = _parse_driver_number(m.group(1))
                if val is not None:
                    return float(val)
            return None

        def _parse_crush_rin_component_m(text_in: Any, require_bridge_context: bool = False) -> Optional[float]:
            raw_txt = str(text_in or "")
            if not raw_txt:
                return None
            lines = [_strip_inline_footnotes(ln).strip() for ln in raw_txt.splitlines() if str(ln).strip()]
            for ln in lines:
                low = ln.lower()
                if "consolidated ethanol crush margin" in low and "rin" in low:
                    m = re.search(
                        r"(?:sale of accumulated rins?|accumulated rins?|rins?)\s+of\s+(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)",
                        ln,
                        re.I,
                    )
                    if m:
                        val = _parse_driver_number(m.group(1))
                        if val is not None:
                            return float(val)
            txt = glx_normalize_text(raw_txt)
            if require_bridge_context and not (
                re.search(r"\bconsolidated crush margin\b", txt, re.I)
                or re.search(r"\bconsolidated ethanol crush margin\b", txt, re.I)
            ):
                return None
            return _parse_rin_impact_value_m(txt, require_quarterly=True)

        def _parse_crush_decommissioning_component_m(text_in: Any) -> Optional[float]:
            raw_txt = str(text_in or "")
            if not raw_txt:
                return None
            patterns = (
                re.compile(
                    r"includes?\s*(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)\s*(?:and|,)\s*"
                    r"\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?[^\n]{0,120}"
                    r"for certain nonrecurring decommissioning costs",
                    re.I,
                ),
                re.compile(
                    r"includes?\s*certain nonrecurring decommissioning costs[^\n]{0,80}\bof\s*"
                    r"(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)",
                    re.I,
                ),
                re.compile(
                    r"nonrecurring decommissioning costs[^\n]{0,80}\bof\s*"
                    r"(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)",
                    re.I,
                ),
            )
            for scope_txt in list(raw_txt.splitlines()) + [glx_normalize_text(raw_txt)]:
                scope = _strip_inline_footnotes(scope_txt)
                if "decommissioning" not in scope.lower():
                    continue
                for pat in patterns:
                    m = pat.search(scope)
                    if not m:
                        continue
                    val = _parse_driver_number(m.group(1))
                    if val is not None:
                        return float(val)
            return None

        def _driver_snippet(text: Any, terms: Tuple[str, ...], max_chars: int = 180) -> str:
            return driver_driver_snippet(text, terms, max_chars)

        def _driver_best_text_record(
            qd: date,
            terms: Tuple[str, ...],
            require_numeric: bool = False,
            quarter_records: Optional[List[Dict[str, Any]]] = None,
        ) -> Optional[Dict[str, Any]]:
            best = driver_driver_best_text_record(
                qd,
                terms,
                operating_driver_best_text_cache=operating_driver_best_text_cache,
                line_index_by_quarter=_load_operating_driver_line_index_by_quarter(),
                source_records_by_quarter=_load_operating_driver_source_records_by_quarter(),
                require_numeric=require_numeric,
                quarter_records=quarter_records,
            )
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_best_text_cache = operating_driver_best_text_cache
            return best

        def _parse_utilization_value(text_in: Any) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            txt_one = re.sub(r"\s+", " ", txt).strip()
            pct_token = r"(\d{2,3}(?:\.\d+)?)\s*%(?:\*)?(?!\d)"
            patterns = [
                re.compile(rf"\bachieved[^.]{{0,120}}?{pct_token}\s*(?:capacity\s+)?utilization\b", re.I),
                re.compile(rf"\b{pct_token}\s*(?:capacity\s+)?utilization\b", re.I),
                re.compile(rf"\bproduction at\s+{pct_token}(?:\s+of\s+(?:production|stated)?\s*capacity)?", re.I),
                re.compile(rf"\butilization(?:\s+rate)?(?:\s+of)?[^.]{{0,120}}?{pct_token}", re.I),
                re.compile(rf"\b{pct_token}.{0,80}?\bof\s+(?:production|stated)\s+capacity\b", re.I),
            ]
            for pat in patterns:
                m = pat.search(txt_one)
                if m:
                    return _parse_driver_number(m.group(1))
            return None

        def _parse_crush_margin_value_m(text_in: Any) -> Optional[float]:
            raw_txt = str(text_in or "")
            if not raw_txt:
                return None

            num_token = r"\$?\s*\(?\s*[0-9]{1,4}(?:,\d{3})*(?:\.\d+)?\s*\)?\s*(?:million|m)?"
            for pat in (
                re.compile(rf"consolidated ethanol crush margin(?:\s+was)?\s*({num_token})", re.I),
                re.compile(rf"({num_token})\s+consolidated ethanol crush margin\b", re.I),
            ):
                m = pat.search(raw_txt)
                if not m:
                    continue
                val = _parse_driver_number(m.group(1))
                if val is not None:
                    return float(val)

            def _parse_scope(scope_txt: str) -> Optional[float]:
                for m in re.finditer(r"consolidated ethanol crush margin(?:\s+was)?", scope_txt, re.I):
                    after_txt = scope_txt[m.end() : min(len(scope_txt), m.end() + 100)]
                    after_vals = _extract_driver_numeric_values(after_txt)
                    if after_vals:
                        return float(after_vals[0])
                    before_txt = scope_txt[max(0, m.start() - 80) : m.start()]
                    before_vals = _extract_driver_numeric_values(before_txt)
                    if before_vals:
                        return float(before_vals[-1])
                return None

            for ln in raw_txt.splitlines():
                line_txt = _strip_inline_footnotes(ln).strip()
                if not line_txt or "consolidated ethanol crush margin" not in line_txt.lower():
                    continue
                val = _parse_scope(line_txt)
                if val is not None:
                    return val

            txt = _strip_inline_footnotes(raw_txt)
            if not txt:
                return None
            val = _parse_scope(txt)
            if val is not None:
                return val
            return None

        def _parse_distillers_grains_k_tons(text_in: Any) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            patterns = [
                re.compile(r"([0-9]{1,3}(?:\.\d+)?)\s+thousand tons of distillers grains(?:\s*\(dry equivalent\))?", re.I),
                re.compile(r"distillers grains\s*\((?:equivalent\s+)?dried tons\)\s*([0-9,]+(?:\.\d+)?)", re.I),
            ]
            for pat in patterns:
                m = pat.search(txt)
                if not m:
                    continue
                val = _parse_driver_number(m.group(1))
                if val is not None:
                    return float(val)
            return None

        def _parse_uhp_k_tons(text_in: Any) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            patterns = [
                re.compile(r"([0-9]{1,3}(?:\.\d+)?)\s+thousand tons of ultra-high protein", re.I),
                re.compile(r"ultra-high protein\s*\(tons\)\s*([0-9,]+(?:\.\d+)?)", re.I),
            ]
            for pat in patterns:
                m = pat.search(txt)
                if not m:
                    continue
                val = _parse_driver_number(m.group(1))
                if val is None:
                    continue
                if "tons" in pat.pattern.lower() and abs(val) > 1000.0:
                    return float(val) / 1000.0
                return float(val)
            return None

        def _parse_corn_consumed_m_bushels(text_in: Any) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            patterns = [
                re.compile(r"processed\s*([0-9]{1,3}(?:\.\d+)?)\s+million bushels of corn", re.I),
                re.compile(r"([0-9]{1,3}(?:\.\d+)?)\s+million bushels of corn processed", re.I),
                re.compile(r"([0-9]{1,3}(?:\.\d+)?)\s+million[^.]{0,120}\bbushels of corn processed\b", re.I),
            ]
            for pat in patterns:
                matches = list(pat.finditer(txt))
                if not matches:
                    continue
                val = _parse_driver_number(matches[-1].group(1))
                if val is not None:
                    return float(val)
            return None

        def _parse_renewable_corn_oil_m_lbs(text_in: Any) -> Optional[float]:
            txt = str(text_in or "")
            low = txt.lower()
            match = re.search(r"Renewable corn oil\s*\(pounds\)\s*([0-9,]+(?:\.\d+)?)", txt, re.I)
            if match:
                raw_val = _parse_driver_number(match.group(1))
                if raw_val is not None:
                    if "selected operating data" in low or "(in thousands)" in low:
                        return float(raw_val) / 1000.0
                    return float(raw_val)
            alt_match = re.search(r"([0-9]{1,3}(?:\.\d+)?)\s+million pounds of renewable corn oil", txt, re.I)
            if alt_match:
                alt_val = _parse_driver_number(alt_match.group(1))
                if alt_val is not None:
                    return float(alt_val)
            return None

        def _parse_rin_impact_value_m(text_in: Any, require_quarterly: bool = True) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            patterns = [
                re.compile(r"(?:sale of accumulated rins?|accumulated rins?|rins?)\s+of\s+(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)", re.I),
                re.compile(r"margins from a one-time sale of accumulated rins?\s+of\s+(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)", re.I),
            ]
            for pat in patterns:
                for m in pat.finditer(txt):
                    window = txt[max(0, m.start() - 80) : min(len(txt), m.end() + 160)]
                    if require_quarterly and re.search(r"\b(nine months ended|year[- ]ended|year ended|full year)\b", window, re.I):
                        if not re.search(r"\b(three months ended|quarter|quarterly highlights)\b", window, re.I):
                            continue
                    val = _parse_driver_number(m.group(1))
                    if val is not None:
                        return float(val)
            return None

        def _extract_crush_margin_bridge_details(text_in: Any) -> Dict[str, Any]:
            txt = _strip_inline_footnotes(text_in)
            low = txt.lower()
            components: Dict[str, float] = {}
            bridge_context = False
            direct_bridge_context = False
            component_context = False
            gpre_45z_memo = extract_gpre_45z_accounting_memo(txt) if is_gpre_profile else {}

            cons_val = _parse_crush_margin_value_m(txt)
            if cons_val is not None:
                components["consolidated"] = float(cons_val)

            if re.search(r"\b(?:inclusive of|includes?|benefited by)\b", low) and "consolidated ethanol crush margin" in low:
                direct_bridge_context = True

            if re.search(r"\bconsolidated crush margin\b", low) and re.search(r"\bethanol production\b", low):
                component_context = True

            if is_gpre_profile:
                gpre_cogs_45z = pd.to_numeric(
                    gpre_45z_memo.get("ethanol_production_45z_cogs_m") if gpre_45z_memo else None,
                    errors="coerce",
                )
                if pd.notna(gpre_cogs_45z):
                    components["45z"] = float(gpre_cogs_45z)
                    component_context = True
                    bridge_context = True

            if "45z" not in components and re.search(r"\b(?:inclusive of|includes?|benefited by)\b", low) and "45z" in low:
                val_45z = _parse_crush_45z_component_m(txt, require_bridge_context=False)
                if val_45z is None:
                    val_45z = _parse_45z_realized_value_m(txt)
                if val_45z is not None:
                    components["45z"] = float(val_45z)
                    bridge_context = True

            if re.search(r"\b(?:inclusive of|includes?|benefited by)\b", low) and re.search(r"\brins?\b", low):
                val_rin = _parse_crush_rin_component_m(txt, require_bridge_context=False)
                if val_rin is not None:
                    components["rin_sale"] = float(val_rin)
                    bridge_context = True

            if "45z" not in components and component_context:
                val_45z = _parse_crush_45z_component_m(txt, require_bridge_context=True)
                if val_45z is not None:
                    components["45z"] = float(val_45z)

            if "rin_sale" not in components and component_context:
                val_rin = _parse_crush_rin_component_m(txt, require_bridge_context=True)
                if val_rin is not None:
                    components["rin_sale"] = float(val_rin)

            impairment_val = _parse_named_driver_metric_value_m(
                txt,
                (
                    r"impairment (?:loss )?on assets held for sale",
                    r"impairment of assets held for sale",
                ),
            )
            if impairment_val is not None:
                components["impairment_assets_held_for_sale"] = float(impairment_val)
                component_context = True

            interco_val = _parse_named_driver_metric_value_m(
                txt,
                (
                    r"intercompany fees and nonethanol operating activities, net",
                    r"intercompany fees and nonethanol operating activities",
                ),
            )
            if interco_val is not None:
                components["intercompany_nonethanol_net"] = float(interco_val)
                component_context = True

            inventory_val = _parse_named_driver_metric_value_m(
                txt,
                (
                    r"inventory lower of cost or net realizable value adjustment",
                    r"inventory lower of costor net realizable value adjustment",
                    r"inventory lower of cost or net realizable value adjustments",
                ),
            )
            if inventory_val is not None:
                q1_2026_45z_context = bool(
                    is_gpre_profile
                    and (
                        (
                            gpre_45z_memo
                            and str(gpre_45z_memo.get("accounting_treatment_45z") or "") == "ASU2025-10_cogs_reduction"
                        )
                        or ("45z" in low and "cost of goods sold" in low and "ethanol production includes" in low)
                    )
                    and re.search(r"\b(?:March\s+31,\s*2026|first quarter of 2026|three months ended March\s+31,\s*2026)\b", txt, re.I)
                )
                if not q1_2026_45z_context:
                    components["inventory_lcnrv"] = float(inventory_val)

            decomm_val = _parse_crush_decommissioning_component_m(txt)
            if decomm_val is None:
                decomm_val = _parse_named_driver_metric_value_m(
                    txt,
                    (
                        r"certain nonrecurring decommissioning costs and nonethanol operating activities",
                        r"nonrecurring decommissioning costs",
                    ),
                    prefer_before=True,
                )
            if decomm_val is not None:
                components["nonrecurring_decommissioning"] = float(decomm_val)
                component_context = True

            for pat in (
                re.compile(r"underlying crush margin(?: was)?\s*(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)", re.I),
                re.compile(r"crush margin ex(?:cluding)?[- ]?45z(?: was)?\s*(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)", re.I),
                re.compile(r"crush margin ex(?:cluding)?[- ]?rin(?: was)?\s*(\$?\s*\(?\s*[0-9]{1,3}(?:\.\d+)?\s*\)?\s*(?:million|m)?)", re.I),
            ):
                m = pat.search(txt)
                if not m:
                    continue
                val = _parse_driver_number(m.group(1))
                if val is None:
                    continue
                pat_txt = pat.pattern.lower()
                if "underlying crush margin" in pat_txt:
                    components["underlying"] = float(val)
                elif "45z" in pat_txt:
                    components["ex_45z"] = float(val)
                elif "rin" in pat_txt:
                    components["ex_rin"] = float(val)

            explicit_bridge_components = {"45z", "rin_sale", "impairment_assets_held_for_sale", "intercompany_nonethanol_net"}
            if direct_bridge_context or (component_context and any(k in components for k in explicit_bridge_components)):
                bridge_context = True
            return {
                "components": components,
                "bridge_context": bool(bridge_context),
                "direct_bridge_context": bool(direct_bridge_context),
                "component_context": bool(component_context),
            }

        def _get_crush_margin_bridge_details(text_in: Any) -> Dict[str, Any]:
            txt = str(text_in or "")
            cached = _operating_driver_crush_detail_cache.get(txt)
            if cached is not None:
                return dict(cached)
            detail = _extract_crush_margin_bridge_details(txt)
            _operating_driver_crush_detail_cache[txt] = dict(detail)
            return dict(detail)

        def _prime_operating_driver_crush_detail_cache(records: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
            if records is None:
                records = _load_operating_driver_source_records()
            for rec in records:
                txt = str(rec.get("text") or "")
                if not txt or txt in _operating_driver_crush_detail_cache or not _is_crush_margin_bridge_candidate(rec.get("_text_low") or txt):
                    continue
                _operating_driver_crush_detail_cache[txt] = dict(_extract_crush_margin_bridge_details(txt))
            return dict(_operating_driver_crush_detail_cache)

        def _load_operating_driver_template_index() -> Dict[str, Any]:
            nonlocal operating_driver_template_index_cache
            if operating_driver_template_index_cache is not None:
                return operating_driver_template_index_cache
            operating_driver_template_index_cache = driver_load_operating_driver_template_index(
                company_profile,
                timed_substage=_timed_writer_substage,
            )
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_template_index = dict(operating_driver_template_index_cache)
            return operating_driver_template_index_cache

        def _load_operating_driver_45z_guidance_docs_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
            nonlocal operating_driver_45z_guidance_docs_by_quarter_cache
            if operating_driver_45z_guidance_docs_by_quarter_cache is not None:
                if ctx_ref is not None:
                    ctx_ref.derived.operating_driver_45z_guidance_docs_by_quarter = (
                        operating_driver_45z_guidance_docs_by_quarter_cache
                    )
                return operating_driver_45z_guidance_docs_by_quarter_cache
            try:
                cache_root_local = Path(cache_dir)
            except Exception:
                cache_root_local = None
            operating_driver_45z_guidance_docs_by_quarter_cache = driver_load_operating_driver_45z_guidance_docs_by_quarter(
                cache_root_local,
                read_operating_driver_text_fn=_read_operating_driver_text,
                infer_cached_doc_quarter_fn=_infer_cached_doc_quarter,
                extract_45z_target_candidates_fn=_extract_45z_2026_target_candidates,
                extract_45z_target_display_fn=_extract_45z_monetization_target_display,
                timed_substage=_timed_writer_substage,
            )
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_45z_guidance_docs_by_quarter = (
                    operating_driver_45z_guidance_docs_by_quarter_cache
                )
            return operating_driver_45z_guidance_docs_by_quarter_cache

        def _operating_driver_template_spec(tpl: Any) -> Dict[str, Any]:
            return driver_operating_driver_template_spec(
                tpl,
                template_index=_load_operating_driver_template_index(),
            )

        def _template_candidate_terms(template_spec: Dict[str, Any]) -> Tuple[str, ...]:
            return driver_template_candidate_terms(template_spec)

        def _text_matches_template_terms(text_low: str, template_spec: Dict[str, Any]) -> bool:
            return driver_text_matches_template_terms(text_low, template_spec)

        def _candidate_records_for_template(
            qd: date,
            template_spec: Dict[str, Any],
            quarter_records: Optional[List[Dict[str, Any]]] = None,
        ) -> List[Dict[str, Any]]:
            candidate_records = driver_candidate_records_for_template(
                qd,
                template_spec,
                operating_driver_template_candidate_cache=operating_driver_template_candidate_cache,
                line_index_by_quarter=_load_operating_driver_line_index_by_quarter(),
                source_records_by_quarter=_load_operating_driver_source_records_by_quarter(),
                quarter_records=quarter_records,
            )
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_template_candidate_cache = operating_driver_template_candidate_cache
            return candidate_records

        def _build_operating_driver_bridge_bundle(qd: date, quarter_records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            bundle_groups: Dict[str, Dict[str, Any]] = {}
            for rec in quarter_records:
                text_blob = str(rec.get("text") or "")
                if not _is_crush_margin_bridge_candidate(rec.get("_text_low") or text_blob):
                    continue
                detail = _get_crush_margin_bridge_details(text_blob)
                components = dict(detail.get("components") or {})
                if not components:
                    continue
                bundle_key = _crush_source_bundle_key(rec.get("source_doc")) or f"{str(rec.get('source_type') or '')}|{qd.isoformat()}"
                frag_pen = float(rec.get("_fragment_penalty") or 0.0)
                rec_score = 58.0 - float(rec.get("source_rank") or 0) * 5.0 - frag_pen * 3.0
                if bool(rec.get("_is_complete_signal")):
                    rec_score += 3.0
                if any(k in components for k in ("underlying", "ex_45z", "ex_rin")):
                    rec_score += 5.0
                if detail.get("bridge_context"):
                    rec_score += 4.0
                bundle = bundle_groups.setdefault(
                    bundle_key,
                    {
                        "components": {},
                        "component_meta": {},
                        "source_type": str(rec.get("source_type") or ""),
                        "source_doc": str(rec.get("source_doc") or ""),
                        "text": text_blob,
                        "bridge_context": False,
                        "direct_bridge_context": False,
                        "component_context": False,
                        "best_score": -10_000.0,
                        "best_source_rank": int(rec.get("source_rank") or 99),
                    },
                )
                if rec_score > float(bundle.get("best_score") or -10_000.0):
                    bundle["best_score"] = float(rec_score)
                    bundle["source_type"] = str(rec.get("source_type") or "")
                    bundle["source_doc"] = str(rec.get("source_doc") or "")
                    bundle["text"] = text_blob
                    bundle["best_source_rank"] = int(rec.get("source_rank") or 99)
                bundle["bridge_context"] = bool(bundle.get("bridge_context")) or bool(detail.get("bridge_context"))
                bundle["direct_bridge_context"] = bool(bundle.get("direct_bridge_context")) or bool(detail.get("direct_bridge_context"))
                bundle["component_context"] = bool(bundle.get("component_context")) or bool(detail.get("component_context"))
                for comp_key, comp_val in components.items():
                    comp_score = float(rec_score)
                    if comp_key in {"underlying", "ex_45z", "ex_rin"}:
                        comp_score += 6.0
                    if comp_key in {"45z", "rin_sale"} and bool(detail.get("direct_bridge_context")):
                        comp_score += 4.0
                    prev_meta = bundle["component_meta"].get(comp_key)
                    if prev_meta is None or comp_score > float(prev_meta.get("score") or -10_000.0):
                        bundle["components"][comp_key] = float(comp_val)
                        bundle["component_meta"][comp_key] = {
                            "score": comp_score,
                            "source_doc": str(rec.get("source_doc") or ""),
                            "source_type": str(rec.get("source_type") or ""),
                            "text": text_blob,
                        }
            if not bundle_groups:
                return None
            scored_bundles: List[Tuple[float, Dict[str, Any]]] = []
            for bundle in bundle_groups.values():
                bundle_components = dict(bundle.get("components") or {})
                if "consolidated" not in bundle_components and not any(
                    k in bundle_components for k in ("underlying", "ex_45z", "ex_rin")
                ):
                    continue
                component_count = len([k for k in bundle_components.keys() if k != "consolidated"])
                bundle_score = float(bundle.get("best_score") or -10_000.0)
                if "consolidated" in bundle_components:
                    bundle_score += 8.0
                if bundle.get("bridge_context"):
                    bundle_score += 5.0
                if bundle.get("direct_bridge_context"):
                    bundle_score += 4.0
                if bundle.get("component_context"):
                    bundle_score += 3.0
                if any(k in bundle_components for k in ("underlying", "ex_45z", "ex_rin")):
                    bundle_score += 6.0
                bundle_score += float(component_count) * 2.0
                scored_bundles.append((bundle_score, bundle))
            if not scored_bundles:
                return None
            preferred_scored = [
                (score, bundle)
                for score, bundle in scored_bundles
                if "consolidated" in dict(bundle.get("components") or {})
                and (bundle.get("component_context") or bundle.get("direct_bridge_context"))
            ]
            if any(str((bundle or {}).get("source_type") or "") == "presentation" for _, bundle in preferred_scored):
                preferred_scored = [
                    (score, bundle)
                    for score, bundle in preferred_scored
                    if str((bundle or {}).get("source_type") or "") == "presentation"
                ]
            return max(preferred_scored or scored_bundles, key=lambda item: item[0])[1]

        def _load_operating_driver_bridge_bundle_map(quarter_set: List[date]) -> Dict[date, Dict[str, Any]]:
            bundle_map = driver_load_operating_driver_bridge_bundle_map(
                quarter_set,
                operating_driver_bridge_bundle_cache=operating_driver_bridge_bundle_cache,
                source_records_by_quarter=_load_operating_driver_source_records_by_quarter(),
                timed_substage=_timed_writer_substage,
                build_bundle_fn=_build_operating_driver_bridge_bundle,
            )
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_bridge_bundle_map = dict(bundle_map)
            return bundle_map

        def _parse_threshold_amount_m(text_in: Any) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            patterns = [
                re.compile(r"\b(?:>=|at least)\s*\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(?:million|m)\b", re.I),
                re.compile(r"\b>\s*\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(?:million|m)\b", re.I),
            ]
            for pat in patterns:
                m = pat.search(txt)
                if m:
                    return _parse_driver_number(m.group(1))
            return None

        def _parse_45z_realized_value_m(text_in: Any) -> Optional[float]:
            txt = glx_normalize_text(text_in)
            patterns = [
                re.compile(r"45z production tax credits?\s*\(\d+\)\s*([0-9]{1,3}(?:\.\d+)?)\b", re.I),
                re.compile(r"45z production tax credits?(?:[^0-9$]{0,50})(?:of\s*)?\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(?:million|m)?\b", re.I),
                re.compile(r"45z production tax credit value net of discounts(?:[^0-9$]{0,60})\s*\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(?:million|m)\b", re.I),
                re.compile(r"inclusive of\s*\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(?:million|m)\s+in\s+45z production tax credit value", re.I),
                re.compile(r"income tax benefit(?:[^.]{0,100})45z(?:[^.]{0,80})\s*\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(?:million|m)\b", re.I),
            ]
            for pat in patterns:
                m = pat.search(txt)
                if m:
                    return _parse_driver_number(m.group(1))
            return None

        def _driver_quality_rank(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
            quality_order = {"exact": 0, "modeled": 1, "text-derived": 2, "inferred": 3}
            value_rank = 0 if pd.notna(pd.to_numeric(row.get("Value"), errors="coerce")) else 1
            return (
                value_rank,
                quality_order.get(str(row.get("Quality") or "").strip().lower(), 9),
                int(_source_rank(row.get("_source_type"), row.get("_source_doc"))),
                len(str(row.get("Commentary") or "")),
            )

        def _operating_drivers_runtime_deps() -> OperatingDriversDeps:
            return OperatingDriversDeps(
                is_gpre_profile=is_gpre_profile,
                source_rank_fn=_source_rank,
                driver_source_display_fn=_driver_source_display,
                driver_source_note_fn=_driver_source_note,
                load_source_records_by_quarter_fn=_load_operating_driver_source_records_by_quarter,
                load_template_index_fn=_load_operating_driver_template_index,
                operating_quarters_fn=_operating_driver_quarters,
                load_line_index_by_quarter_fn=_load_operating_driver_line_index_by_quarter,
                load_bridge_bundle_map_fn=_load_operating_driver_bridge_bundle_map,
                template_spec_fn=_operating_driver_template_spec,
                candidate_records_for_template_fn=_candidate_records_for_template,
                profile_slide_signals_for_quarter_fn=_profile_slide_signals_for_quarter,
                load_45z_guidance_docs_by_quarter_fn=_load_operating_driver_45z_guidance_docs_by_quarter,
                parse_gpre_crush_margin_pair_fn=_parse_gpre_crush_margin_pair_local,
                cached_metric_parse_fn=_cached_driver_metric_parse,
                driver_snippet_fn=_driver_snippet,
                qn_is_complete_signal_text_fn=qn_is_complete_signal_text,
                driver_best_text_record_fn=_driver_best_text_record,
                parse_utilization_value_fn=_parse_utilization_value,
                parse_driver_number_fn=_parse_driver_number,
                parse_distillers_grains_k_tons_fn=_parse_distillers_grains_k_tons,
                parse_uhp_k_tons_fn=_parse_uhp_k_tons,
                parse_corn_consumed_m_bushels_fn=_parse_corn_consumed_m_bushels,
                parse_rin_impact_value_m_fn=_parse_rin_impact_value_m,
                parse_crush_margin_value_m_fn=_parse_crush_margin_value_m,
                parse_45z_realized_value_m_fn=_parse_45z_realized_value_m,
                parse_renewable_corn_oil_m_lbs_fn=_parse_renewable_corn_oil_m_lbs,
                extract_45z_target_candidates_fn=_extract_45z_2026_target_candidates,
                extract_45z_target_display_fn=_extract_45z_monetization_target_display,
                text_fragment_penalty_fn=_text_fragment_penalty,
                extract_money_targets_for_display_fn=_extract_money_targets_for_display,
                parse_threshold_amount_m_fn=_parse_threshold_amount_m,
                timed_substage_fn=_timed_writer_substage,
            )

        def _merge_driver_rows(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
            return runtime_merge_driver_rows(existing, candidate, source_rank_fn=_source_rank)

        def _make_driver_row(
            qd: date,
            driver_key: str,
            driver_group: str,
            driver_label: str,
            source_type: str,
            source_doc: str,
            commentary: str = "",
            quality: str = "text-derived",
            value: Any = None,
            unit: str = "",
            scope: str = "",
            source_note: str = "",
        ) -> Dict[str, Any]:
            return runtime_make_driver_row(
                qd,
                driver_key,
                driver_group,
                driver_label,
                source_type,
                source_doc,
                driver_source_display_fn=_driver_source_display,
                driver_source_note_fn=_driver_source_note,
                commentary=commentary,
                quality=quality,
                value=value,
                unit=unit,
                scope=scope,
                source_note=source_note,
            )

        def _build_anf_operating_driver_rows() -> List[Dict[str, Any]]:
            rows_out: List[Dict[str, Any]] = []

            def _num(value: Any) -> Optional[float]:
                val = pd.to_numeric(value, errors="coerce")
                return float(val) if pd.notna(val) else None

            def _append(
                qd: date,
                key: str,
                group: str,
                label: str,
                value: Optional[float],
                unit: str,
                commentary: str,
                *,
                source_type: str = "model_metric",
                source_doc: str = "History_Q",
                scope: str = "",
                quality: str = "exact",
            ) -> None:
                if value is None:
                    return
                rows_out.append(
                    _make_driver_row(
                        qd,
                        key,
                        group,
                        label,
                        source_type,
                        source_doc,
                        commentary=commentary,
                        quality=quality,
                        value=float(value),
                        unit=unit,
                        scope=scope,
                    )
                )

            hist_local = hist.copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame()
            total_revenue_by_q: Dict[date, float] = {}
            if not hist_local.empty and "quarter" in hist_local.columns:
                hist_local["_quarter_date"] = pd.to_datetime(hist_local["quarter"], errors="coerce").dt.date
                for _, rec in hist_local.iterrows():
                    qd = rec.get("_quarter_date")
                    if not isinstance(qd, date):
                        continue
                    revenue = _num(rec.get("revenue"))
                    gross_profit = _num(rec.get("gross_profit"))
                    op_income = _num(rec.get("op_income"))
                    inventory = _num(rec.get("inventory"))
                    cfo_val = _num(rec.get("cfo"))
                    capex_val = _num(rec.get("capex"))
                    if revenue is not None and revenue > 0:
                        total_revenue_by_q[qd] = revenue
                        _append(
                            qd,
                            "anf_net_sales",
                            "Demand / brand momentum",
                            "Net sales",
                            revenue / 1_000_000.0,
                            "$m",
                            "Reported quarterly net sales from SEC facts and ANF earnings financials.",
                        )
                        if gross_profit is not None:
                            _append(
                                qd,
                                "anf_gross_margin",
                                "Margin / costs",
                                "Gross margin",
                                (gross_profit / revenue) * 100.0,
                                "%",
                                "Gross profit divided by reported net sales.",
                            )
                        if op_income is not None:
                            _append(
                                qd,
                                "anf_operating_margin",
                                "Margin / costs",
                                "Operating margin",
                                (op_income / revenue) * 100.0,
                                "%",
                                "Operating income divided by reported net sales.",
                            )
                    if inventory is not None:
                        _append(
                            qd,
                            "anf_inventory",
                            "Inventory / working capital",
                            "Inventory",
                            inventory / 1_000_000.0,
                            "$m",
                            "Quarter-end inventory from History_Q.",
                        )
                    if cfo_val is not None:
                        _append(
                            qd,
                            "anf_cfo",
                            "Cash conversion / capex",
                            "Operating cash flow",
                            cfo_val / 1_000_000.0,
                            "$m",
                            "Quarterly operating cash flow; Q4 derived from cumulative ANF earnings financial schedules.",
                        )
                    if capex_val is not None:
                        _append(
                            qd,
                            "anf_capex",
                            "Cash conversion / capex",
                            "Capital expenditures",
                            capex_val / 1_000_000.0,
                            "$m",
                            "Quarterly capital expenditures; Q4 derived from cumulative ANF earnings financial schedules.",
                        )

            ss = slides_segments.copy() if isinstance(slides_segments, pd.DataFrame) else pd.DataFrame()
            if not ss.empty:
                ss = _filter_anf_quarterly_segment_actual_rows(
                    ss,
                    history_revenue_by_quarter=hist if isinstance(hist, pd.DataFrame) else None,
                )
            if not ss.empty and "quarter" in ss.columns:
                ss["_quarter_date"] = pd.to_datetime(ss["quarter"], errors="coerce").dt.date
                for _, rec in ss.iterrows():
                    qd = rec.get("_quarter_date")
                    if not isinstance(qd, date):
                        continue
                    segment = str(rec.get("segment") or "").strip()
                    metric = str(rec.get("metric") or "").strip().lower()
                    value = _num(rec.get("value"))
                    if not segment or value is None:
                        continue
                    source_doc = str(rec.get("doc") or "Slides_Segments")
                    source_type = str(rec.get("source") or "slides")
                    safe_segment = re.sub(r"[^a-z0-9]+", "_", segment.lower()).strip("_") or "total"
                    driver_group = str(rec.get("driver_group") or "").strip()
                    source_commentary = str(rec.get("commentary") or rec.get("note") or rec.get("source_snippet") or "").strip()
                    if metric == "comparable_sales":
                        comp_value = value * 100.0 if abs(value) <= 1.5 else value
                        _append(
                            qd,
                            f"anf_{safe_segment}_comparable_sales",
                            "Demand / brand momentum",
                            f"{segment} comparable sales",
                            comp_value,
                            "%",
                            source_commentary or "Comparable-sales disclosure parsed from ANF earnings materials.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )
                    elif metric == "net_sales_growth":
                        growth_value = value * 100.0 if abs(value) <= 1.5 else value
                        _append(
                            qd,
                            f"anf_{safe_segment}_net_sales_growth",
                            "Demand / brand momentum",
                            f"{segment} net sales growth",
                            growth_value,
                            "%",
                            source_commentary or "Net-sales growth disclosure parsed from ANF earnings materials.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )
                    elif metric == "revenue" and value > 0:
                        total_rev = total_revenue_by_q.get(qd)
                        if total_rev is not None and value > total_rev * 1.25:
                            continue
                        _append(
                            qd,
                            f"anf_{safe_segment}_net_sales",
                            "Demand / brand momentum",
                            f"{segment} net sales",
                            value / 1_000_000.0,
                            "$m",
                            "Segment net sales parsed from ANF earnings materials.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )
                    elif metric in {
                        "store_count_beginning",
                        "new_stores",
                        "closed_stores",
                        "store_count_end",
                        "franchise_stores",
                        "total_stores_including_franchise",
                        "right_sized_stores",
                        "remodeled_stores",
                    }:
                        label_map = {
                            "store_count_beginning": "Company-owned stores, start",
                            "new_stores": "New stores",
                            "closed_stores": "Closed stores",
                            "store_count_end": "Company-owned stores, end",
                            "franchise_stores": "Franchise stores",
                            "total_stores_including_franchise": "Total stores incl. franchise",
                            "right_sized_stores": "Right-sized stores",
                            "remodeled_stores": "Remodeled stores",
                        }
                        _append(
                            qd,
                            f"anf_{safe_segment}_{metric}",
                            "Stores / real estate",
                            f"{segment} {label_map.get(metric, metric.replace('_', ' '))}",
                            value,
                            "stores",
                            source_commentary or "Store-count disclosure parsed from ANF quarterly history or transcripts.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )
                    elif metric in {
                        "digital_sales_mix",
                        "inventory_cost_growth",
                        "inventory_unit_growth",
                        "inventory_unit_growth_ex_erp",
                        "shares_repurchased_opening_share_pct",
                    }:
                        pct_value = value * 100.0 if abs(value) <= 1.5 else value
                        label_map = {
                            "digital_sales_mix": "Digital sales mix",
                            "inventory_cost_growth": "Inventory cost growth",
                            "inventory_unit_growth": "Inventory unit growth",
                            "inventory_unit_growth_ex_erp": "Inventory unit growth ex-ERP",
                            "shares_repurchased_opening_share_pct": "Repurchased shares / opening shares",
                        }
                        _append(
                            qd,
                            f"anf_{safe_segment}_{metric}",
                            driver_group or ("Digital / omnichannel" if "digital" in metric else "Inventory / working capital"),
                            f"{segment} {label_map.get(metric, metric.replace('_', ' '))}",
                            pct_value,
                            "%",
                            source_commentary or "ANF retail-driver disclosure parsed from local source materials.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )
                    elif metric in {
                        "inventory_cost_tariff_points",
                        "inventory_unit_growth_erp_points",
                        "q1_fy2026_tariff_headwind_bps",
                        "q1_fy2026_freight_tailwind_bps",
                        "q1_fy2026_erp_sales_headwind_low",
                        "q1_fy2026_erp_sales_headwind_high",
                        "q1_fy2026_erp_margin_headwind_bps",
                        "q1_fy2026_marketing_headwind_bps",
                        "fy2026_tariff_headwind_bps",
                    }:
                        unit = str(rec.get("unit") or ("bps" if metric.endswith("_bps") else "pts")).strip() or "pts"
                        label = metric.replace("q1_fy2026_", "Q1 FY2026 ").replace("fy2026_", "FY2026 ").replace("_", " ").title()
                        _append(
                            qd,
                            f"anf_{safe_segment}_{metric}",
                            driver_group or "FY2026 margin bridge",
                            label,
                            value,
                            unit,
                            source_commentary or "ANF margin/inventory bridge disclosure parsed from local source materials.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )
                    elif metric in {
                        "share_repurchases",
                        "shares_repurchased",
                        "average_buyback_price",
                        "remaining_buyback_authorization",
                        "q1_fy2026_tariff_headwind",
                        "fy2026_tariff_headwind",
                    }:
                        unit = str(rec.get("unit") or "").strip()
                        label_map = {
                            "share_repurchases": "Share repurchases",
                            "shares_repurchased": "Shares repurchased",
                            "average_buyback_price": "Average buyback price",
                            "remaining_buyback_authorization": "Remaining buyback authorization",
                            "q1_fy2026_tariff_headwind": "Q1 FY2026 tariff headwind",
                            "fy2026_tariff_headwind": "FY2026 tariff headwind",
                        }
                        _append(
                            qd,
                            f"anf_{safe_segment}_{metric}",
                            driver_group or "Capital allocation",
                            label_map.get(metric, metric.replace("_", " ").title()),
                            value,
                            unit,
                            source_commentary or "ANF capital-allocation or margin-bridge disclosure parsed from local source materials.",
                            source_type=source_type,
                            source_doc=source_doc,
                            scope=segment,
                        )

            grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
            for row in rows_out:
                grouped.setdefault((str(row.get("_driver_key") or ""), str(row.get("_driver_scope") or "")), []).append(row)
            for items in grouped.values():
                items.sort(key=lambda row: row.get("Quarter") or date.min)
                for idx, row in enumerate(items):
                    unit = str(row.get("Unit") or "")
                    val = _num(row.get("Value"))
                    if val is None:
                        continue
                    if idx > 0:
                        prev_val = _num(items[idx - 1].get("Value"))
                        if prev_val is not None:
                            if unit == "%":
                                row["QoQ change"] = f"{val - prev_val:+.1f} pts"
                            elif abs(prev_val) > 1e-9:
                                row["QoQ change"] = f"{((val - prev_val) / abs(prev_val)) * 100.0:+.1f}%"
                    prev_year = next(
                        (
                            item
                            for item in items
                            if isinstance(item.get("Quarter"), date)
                            and item["Quarter"].year == row["Quarter"].year - 1
                            and item["Quarter"].month == row["Quarter"].month
                        ),
                        None,
                    )
                    prev_yoy = _num((prev_year or {}).get("Value"))
                    if prev_yoy is not None:
                        if unit == "%":
                            row["YoY change"] = f"{val - prev_yoy:+.1f} pts"
                        elif abs(prev_yoy) > 1e-9:
                            row["YoY change"] = f"{((val - prev_yoy) / abs(prev_yoy)) * 100.0:+.1f}%"
            return rows_out

        def _gpre_canonical_crush_series_for_drivers_local() -> Dict[date, Dict[str, Any]]:
            return runtime_gpre_canonical_crush_series_for_drivers(
                operating_drivers_runtime,
                _operating_drivers_runtime_deps(),
            )

        def _extract_operating_driver_rows_for_template(
            qd: date,
            tpl: Any,
            quarter_records: Optional[List[Dict[str, Any]]] = None,
        ) -> List[Dict[str, Any]]:
            return runtime_extract_operating_driver_rows_for_template(
                operating_drivers_runtime,
                _operating_drivers_runtime_deps(),
                qd,
                tpl,
                quarter_records=quarter_records,
            )
            template_spec = _operating_driver_template_spec(tpl)
            key = str(template_spec.get("key") or "").strip().lower()
            group = str(template_spec.get("group") or "")
            label = str(template_spec.get("label") or "")
            search_terms = tuple(template_spec.get("search_terms") or ())
            if quarter_records is None:
                quarter_records = _load_operating_driver_source_records_by_quarter().get(qd, [])
            candidate_records = _candidate_records_for_template(qd, template_spec, quarter_records=quarter_records)

            if key == "utilization":
                def _polish_utilization_commentary_local(text_in: Any) -> str:
                    txt_local = glx_normalize_text(str(text_in or "")).strip()
                    if is_gpre_profile and re.search(r"\bspring maintenance season\b", txt_local, re.I):
                        return "Plant utilization reflected the normal spring maintenance season, with plants temporarily shut down for annual clean-out and restart."
                    return txt_local

                quarter_signals = _profile_slide_signals_for_quarter(qd)
                if quarter_signals:
                    signal_candidates = [
                        rec
                        for rec in quarter_signals
                        if re.search(r"(?<!\d)\d{2,3}\s*%", str(rec.get("text") or ""))
                        and re.search(r"\b(utilization|capacity|production at)\b", str(rec.get("text") or ""), re.I)
                    ]
                    if signal_candidates:
                        best_signal = max(signal_candidates, key=lambda rec: float(rec.get("score") or 0.0))
                        val = _parse_utilization_value(best_signal.get("text"))
                        if val is not None:
                            commentary_txt = _polish_utilization_commentary_local(best_signal.get("text"))
                            return [
                                _make_driver_row(
                                    qd,
                                    key,
                                    group,
                                    label,
                                    str(best_signal.get("source_type") or ""),
                                    str(best_signal.get("source_doc") or ""),
                                    commentary=commentary_txt,
                                    quality="exact",
                                    value=float(val),
                                    unit="%",
                                    source_note=_driver_source_note(best_signal.get("source_doc"), commentary_txt),
                                )
                            ]
                best_row: Optional[Dict[str, Any]] = None
                best_score = -10_000.0
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    if not re.search(r"\b(utilization|production at|operating rate|capacity utilization|stated capacity)\b", low, re.I):
                        continue
                    val = _cached_driver_metric_parse("utilization", text_blob, _parse_utilization_value)
                    if val is None:
                        continue
                    snippet = _driver_snippet(text_blob, ("utilization", "capacity", "production at"))
                    score = 60.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
                    if "production at" in low:
                        score += 5.0
                    if "capacity utilization" in low or "utilization in the quarter" in low:
                        score += 4.0
                    if qn_is_complete_signal_text(snippet):
                        score += 3.0
                    snippet = _polish_utilization_commentary_local(snippet)
                    if score > best_score:
                        best_score = score
                        best_row = _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary=snippet,
                            quality="exact",
                            value=float(val),
                            unit="%",
                            source_note=_driver_source_note(rec.get("source_doc"), snippet),
                        )
                return [best_row] if best_row is not None else []
            if key == "ethanol_gallons":
                out_rows: List[Dict[str, Any]] = []
                best_prod: Optional[Dict[str, Any]] = None
                best_sold: Optional[Dict[str, Any]] = None
                best_prod_score = -10_000.0
                best_sold_score = -10_000.0
                prod_table_re = re.compile(r"Ethanol production.*?Ethanol\s*\(gallons\)\s*([0-9,]+(?:\.\d+)?)", re.I | re.S)
                sold_table_re = re.compile(r"Agribusiness and energy services.*?Ethanol\s*\(gallons\)\s*([0-9,]+(?:\.\d+)?)", re.I | re.S)
                prod_sentence_re = re.compile(r"([0-9]{1,3}(?:\.\d+)?)\s+million gallons of ethanol", re.I)
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    if "ethanol" not in low or "gallons" not in low:
                        continue
                    prod_val = None
                    sold_val = None
                    prod_m = prod_table_re.search(text_blob)
                    sold_m = sold_table_re.search(text_blob)
                    if prod_m:
                        raw_prod = _parse_driver_number(prod_m.group(1))
                        if raw_prod is not None:
                            prod_val = raw_prod / 1000.0
                    if sold_m:
                        raw_sold = _parse_driver_number(sold_m.group(1))
                        if raw_sold is not None:
                            sold_val = raw_sold / 1000.0
                    if prod_val is None:
                        prod_sent = prod_sentence_re.search(text_blob)
                        if prod_sent:
                            prod_val = _parse_driver_number(prod_sent.group(1))
                    score = 50.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
                    if prod_val is not None and score > best_prod_score:
                        best_prod_score = score
                        best_prod = _make_driver_row(
                            qd,
                            "ethanol_gallons_produced",
                            group,
                            "Ethanol gallons produced",
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary="Produced gallons from selected operating data." if "selected operating data" in low else _driver_snippet(text_blob, ("ethanol", "gallons", "production")),
                            quality="exact",
                            value=float(prod_val),
                            unit="m gallons",
                            source_note=_driver_source_note(rec.get("source_doc"), _driver_snippet(text_blob, ("ethanol", "gallons", "production"))),
                        )
                    if sold_val is not None and score > best_sold_score:
                        best_sold_score = score
                        best_sold = _make_driver_row(
                            qd,
                            "ethanol_gallons_sold",
                            group,
                            "Ethanol gallons sold",
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary="Sold gallons from agribusiness and energy services operating data." if "agribusiness and energy services" in low else _driver_snippet(text_blob, ("agribusiness and energy services", "ethanol", "gallons")),
                            quality="exact",
                            value=float(sold_val),
                            unit="m gallons",
                            source_note=_driver_source_note(rec.get("source_doc"), _driver_snippet(text_blob, ("agribusiness and energy services", "ethanol", "gallons"))),
                        )
                if best_prod is not None:
                    out_rows.append(best_prod)
                if best_sold is not None:
                    out_rows.append(best_sold)
                return out_rows
            if key in {"distillers_grains", "ultra_high_protein", "corn_consumed", "rin_impact_accumulated_rin_sale"}:
                best: Optional[Dict[str, Any]] = None
                best_score = -10_000.0
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    val: Optional[float] = None
                    snippet_terms: Tuple[str, ...] = search_terms or (label.lower(),)
                    quality = "exact"
                    unit = str(getattr(tpl, "preferred_unit", "") or "")
                    if key == "distillers_grains":
                        if "distillers grains" not in low:
                            continue
                        val = _cached_driver_metric_parse("distillers_grains", text_blob, _parse_distillers_grains_k_tons)
                        snippet_terms = ("distillers grains",)
                    elif key == "ultra_high_protein":
                        if "ultra-high protein" not in low and "uhp" not in low:
                            continue
                        val = _cached_driver_metric_parse("ultra_high_protein", text_blob, _parse_uhp_k_tons)
                        snippet_terms = ("ultra-high protein", "uhp")
                    elif key == "corn_consumed":
                        if "bushels of corn" not in low and "corn processed" not in low and "corn consumed" not in low:
                            continue
                        val = _cached_driver_metric_parse("corn_consumed", text_blob, _parse_corn_consumed_m_bushels)
                        snippet_terms = ("corn processed", "bushels of corn", "corn consumed")
                    elif key == "rin_impact_accumulated_rin_sale":
                        if "rin" not in low:
                            continue
                        if (
                            re.search(r"\b(nine months ended|year[- ]ended|year ended|full year)\b", low, re.I)
                            and not re.search(r"\b(three months ended|quarter|quarterly highlights)\b", low, re.I)
                        ):
                            continue
                        val = _cached_driver_metric_parse("rin_impact_accumulated_rin_sale", text_blob, _parse_rin_impact_value_m)
                        snippet_terms = ("accumulated rins", "rin sale", "rins")
                        quality = "text-derived"
                    if val is None:
                        continue
                    snippet = _driver_snippet(text_blob, snippet_terms)
                    score = 52.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
                    if qn_is_complete_signal_text(snippet):
                        score += 3.0
                    if key == "rin_impact_accumulated_rin_sale" and re.search(r"\baccumulated rins?\b", low, re.I):
                        score += 4.0
                    if score > best_score:
                        best_score = score
                        best = _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary=snippet,
                            quality=quality,
                            value=float(val),
                            unit=unit,
                            source_note=_driver_source_note(rec.get("source_doc"), snippet),
                        )
                return [best] if best is not None else []
            if key == "consolidated_ethanol_crush_margin":
                if is_gpre_profile:
                    canonical_series = _gpre_canonical_crush_series_for_drivers_local()
                    canonical_rec = canonical_series.get(qd)
                    canonical_val = pd.to_numeric((canonical_rec or {}).get("value"), errors="coerce")
                    if pd.notna(canonical_val):
                        source_type = str((canonical_rec or {}).get("source_type") or "earnings_release")
                        source_doc = str((canonical_rec or {}).get("source_doc") or "")
                        commentary = str((canonical_rec or {}).get("commentary") or "")
                        return [
                            _make_driver_row(
                                qd,
                                key,
                                group,
                                label,
                                source_type,
                                source_doc,
                                commentary=commentary,
                                quality="exact",
                                value=float(canonical_val),
                                unit="$m",
                                source_note=_driver_source_note(source_doc, commentary),
                            )
                        ]
                margin_rec = _driver_best_text_record(qd, search_terms, require_numeric=True, quarter_records=candidate_records)
                if margin_rec is not None:
                    val = _cached_driver_metric_parse("consolidated_ethanol_crush_margin", margin_rec.get("text"), _parse_crush_margin_value_m)
                    if val is not None:
                        return [
                            _make_driver_row(
                                qd,
                                key,
                                group,
                                label,
                                str(margin_rec.get("source_type") or ""),
                                str(margin_rec.get("source_doc") or ""),
                                commentary=str(margin_rec.get("snippet") or ""),
                                quality="exact",
                                value=float(val),
                                unit="$m",
                                source_note=_driver_source_note(margin_rec.get("source_doc"), margin_rec.get("snippet")),
                            )
                        ]
            if key == "45z_value_realized":
                best: Optional[Dict[str, Any]] = None
                best_score = -10_000.0
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    if "45z" not in low or ("production tax" not in low and "income tax benefit" not in low):
                        continue
                    val = _cached_driver_metric_parse("45z_value_realized", text_blob, _parse_45z_realized_value_m)
                    if val is None:
                        continue
                    snippet = _driver_snippet(text_blob, ("45z", "production tax", "income tax benefit"))
                    score = 55.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
                    if qn_is_complete_signal_text(snippet):
                        score += 3.0
                    if score > best_score:
                        best_score = score
                        best = _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary=snippet,
                            quality="exact",
                            value=float(val),
                            unit="$m",
                            source_note=_driver_source_note(rec.get("source_doc"), snippet),
                        )
                return [best] if best is not None else []
            if key == "45z_value_guided":
                candidate_rows: List[Dict[str, Any]] = []
                def _valid_45z_guidance_display(txt_in: Any) -> bool:
                    txt_local = glx_normalize_text(str(txt_in or ""))
                    if not txt_local or not re.search(r"\$\s*[0-9]", txt_local):
                        return False
                    if len(txt_local) > 120:
                        return False
                    if re.search(r"\$0(?:\.0)?m?\s*-\s*\$?0(?:\.0)?m?\b", txt_local, re.I):
                        return False
                    if re.fullmatch(
                        r"\$[0-9.,]+m-\$[0-9.,]+m expected (?:Q[1-4] 20\d{2} )?monetization",
                        txt_local,
                        re.I,
                    ):
                        return True
                    if re.fullmatch(
                        r"(?:>=|>) \$[0-9.,]+m(?: expected)? in 20\d{2}",
                        txt_local,
                        re.I,
                    ):
                        return True
                    return False
                for rec in _profile_slide_signals_for_quarter(qd):
                    target_txt = str(rec.get("target_display") or "").strip()
                    if not target_txt or "45z" not in str(rec.get("theme_key") or "").lower():
                        continue
                    if not _valid_45z_guidance_display(target_txt):
                        continue
                    candidate_rows.append(
                        {
                            "text": target_txt,
                            "source_type": str(rec.get("source_type") or ""),
                            "source_doc": str(rec.get("source_doc") or ""),
                            "scope_kind": str(rec.get("scope_kind") or ""),
                            "score": float(rec.get("score") or 0.0),
                            "fragment_penalty": int(rec.get("fragment_penalty") or 0),
                        }
                    )
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    if "45z" not in low:
                        continue
                    strong_targets = _extract_45z_2026_target_candidates(text_blob, qd)
                    for strong in strong_targets:
                        target_txt = str(strong.get("display") or "").strip()
                        if not _valid_45z_guidance_display(target_txt):
                            continue
                        candidate_rows.append(
                            {
                                "text": target_txt,
                                "source_type": str(rec.get("source_type") or ""),
                                "source_doc": str(rec.get("source_doc") or ""),
                                "scope_kind": str(strong.get("scope_kind") or ""),
                                "score": 64.0 - float(rec.get("source_rank") or 0) * 5.0,
                                "fragment_penalty": int(_text_fragment_penalty(strong.get("window") or text_blob) or 0),
                            }
                        )
                    target_txt = _extract_45z_monetization_target_display(text_blob, qd)
                    if not _valid_45z_guidance_display(target_txt):
                        continue
                    candidate_rows.append(
                        {
                            "text": target_txt,
                            "source_type": str(rec.get("source_type") or ""),
                            "source_doc": str(rec.get("source_doc") or ""),
                            "scope_kind": "total",
                            "score": 58.0 - float(rec.get("source_rank") or 0) * 5.0,
                            "fragment_penalty": int(rec.get("_fragment_penalty") or 0),
                        }
                    )
                for doc_rec in _load_operating_driver_45z_guidance_docs_by_quarter().get(qd, []):
                    strong_targets = list(doc_rec.get("strong_targets") or [])
                    for strong in strong_targets:
                        target_txt = str(strong.get("display") or "").strip()
                        if not _valid_45z_guidance_display(target_txt):
                            continue
                        candidate_rows.append(
                            {
                                "text": target_txt,
                                "source_type": str(doc_rec.get("source_type") or ""),
                                "source_doc": str(doc_rec.get("source_doc") or ""),
                                "scope_kind": str(strong.get("scope_kind") or ""),
                                "score": 76.0,
                                "fragment_penalty": int(_text_fragment_penalty(str(strong.get("window") or doc_rec.get("text") or "")) or 0),
                            }
                        )
                    target_txt = str(doc_rec.get("target_display") or "")
                    if _valid_45z_guidance_display(target_txt):
                        candidate_rows.append(
                            {
                                "text": target_txt,
                                "source_type": str(doc_rec.get("source_type") or ""),
                                "source_doc": str(doc_rec.get("source_doc") or ""),
                                "scope_kind": "total",
                                "score": 70.0,
                                "fragment_penalty": int(_text_fragment_penalty(str(doc_rec.get("text") or "")) or 0),
                            }
                        )
                if candidate_rows:
                    def _guidance_key(rec: Dict[str, Any]) -> Tuple[int, float, int]:
                        scope_kind = str(rec.get("scope_kind") or "")
                        scope_pri = 0 if scope_kind == "total" else 1 if scope_kind == "component_named" else 2 if scope_kind == "component_remaining" else 3
                        txt_local = str(rec.get("text") or "")
                        amount_vals = _extract_money_targets_for_display(txt_local)
                        amount_pri = float(max(amount_vals)) if amount_vals else 0.0
                        return (-scope_pri, amount_pri, float(rec.get("score") or 0.0), -int(rec.get("fragment_penalty") or 0))

                    best = max(candidate_rows, key=_guidance_key)
                    target_txt = str(best.get("text") or "").strip()
                    value_m = None
                    if target_txt and not re.search(r"\$\s*[0-9].{0,10}\-\s*\$?\s*[0-9]", target_txt):
                        value_m = _parse_threshold_amount_m(target_txt)
                    return [
                        _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(best.get("source_type") or ""),
                            str(best.get("source_doc") or ""),
                            commentary=target_txt,
                            quality="text-derived",
                            value=value_m,
                            unit="$m" if value_m is not None else "",
                            source_note=_driver_source_note(best.get("source_doc"), target_txt),
                        )
                    ]
                return []
            if key == "renewable_corn_oil":
                best: Optional[Dict[str, Any]] = None
                best_score = -10_000.0
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    if "renewable corn oil" not in low and "corn oil" not in low:
                        continue
                    val = _cached_driver_metric_parse("renewable_corn_oil", text_blob, _parse_renewable_corn_oil_m_lbs)
                    if val is None or float(val) > 200.0:
                        continue
                    snippet = _driver_snippet(text_blob, ("renewable corn oil", "corn oil"))
                    score = 50.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
                    if score > best_score:
                        best_score = score
                        best = _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary=snippet,
                            quality="exact",
                            value=float(val),
                            unit="m lbs",
                            source_note=_driver_source_note(rec.get("source_doc"), snippet),
                        )
                return [best] if best is not None else []
            if key == "protein_coproduct_mix":
                best: Optional[Dict[str, Any]] = None
                best_score = -10_000.0
                for rec in candidate_records:
                    text_blob = str(rec.get("text") or "")
                    low = str(rec.get("_text_low") or text_blob.lower())
                    if not any(tok in low for tok in ("distillers grains", "ultra-high protein", "uhp", "coproduct")):
                        continue
                    dist_m = re.search(r"Distillers grains\s*\(equivalent dried tons\)\s*([0-9,]+(?:\.\d+)?)", text_blob, re.I)
                    uhp_m = re.search(r"Ultra-High Protein\s*\(tons\)\s*([0-9,]+(?:\.\d+)?)", text_blob, re.I)
                    comment_parts: List[str] = []
                    if dist_m:
                        dist_val = _parse_driver_number(dist_m.group(1))
                        if dist_val is not None:
                            comment_parts.append(f"Distillers grains {dist_val:.0f}k tons")
                    if uhp_m:
                        uhp_val = _parse_driver_number(uhp_m.group(1))
                        if uhp_val is not None:
                            comment_parts.append(f"Ultra-high protein {uhp_val:.0f}k tons")
                    if not comment_parts:
                        continue
                    snippet = "; ".join(comment_parts)
                    score = 48.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 2.0
                    if score > best_score:
                        best_score = score
                        best = _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(rec.get("source_type") or ""),
                            str(rec.get("source_doc") or ""),
                            commentary=snippet,
                            quality="exact",
                            source_note=_driver_source_note(rec.get("source_doc"), snippet),
                        )
                return [best] if best is not None else []
            if key in {"risk_management_support", "margin_cashflow_support"}:
                best = _driver_best_text_record(qd, search_terms, quarter_records=candidate_records)
                if best is not None:
                    return [
                        _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(best.get("source_type") or ""),
                            str(best.get("source_doc") or ""),
                            commentary=str(best.get("snippet") or ""),
                            quality="text-derived",
                            source_note=_driver_source_note(best.get("source_doc"), best.get("snippet")),
                        )
                    ]
            if key in {"45z_agreement_status", "carbon_capture_status", "plant_status", "input_cost_commentary", "distillers_grains_uhp_commentary"}:
                best = _driver_best_text_record(qd, search_terms, quarter_records=candidate_records)
                if best is not None:
                    return [
                        _make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(best.get("source_type") or ""),
                            str(best.get("source_doc") or ""),
                            commentary=str(best.get("snippet") or ""),
                            quality="text-derived",
                            source_note=_driver_source_note(best.get("source_doc"), best.get("snippet")),
                        )
                    ]
            best_generic = _driver_best_text_record(qd, search_terms, require_numeric=False, quarter_records=candidate_records)
            if best_generic is not None:
                return [
                    _make_driver_row(
                        qd,
                        key or re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_"),
                        group,
                        label,
                        str(best_generic.get("source_type") or ""),
                        str(best_generic.get("source_doc") or ""),
                        commentary=str(best_generic.get("snippet") or ""),
                        quality="text-derived",
                        source_note=_driver_source_note(best_generic.get("source_doc"), best_generic.get("snippet")),
                    )
                ]
            return []

        def _format_operating_driver_delta(current_val: Any, prior_val: Any, unit: str) -> str:
            return runtime_format_operating_driver_delta(current_val, prior_val, unit)

        def _build_operating_drivers_history_rows() -> List[Dict[str, Any]]:
            derivative_oci_bridge_df = self.runtime.get("derivative_oci_bridge_df", pd.DataFrame())
            rows = runtime_build_operating_drivers_history_rows(
                operating_drivers_runtime,
                _operating_drivers_runtime_deps(),
            )
            if is_anf_profile:
                anf_rows = _build_anf_operating_driver_rows()
                if anf_rows:
                    rows = list(rows or []) + anf_rows
            if is_gpre_profile:
                row_by_key_q = {
                    (str(row.get("_driver_key") or ""), row.get("Quarter")): row
                    for row in rows
                    if isinstance(row.get("Quarter"), date)
                }

                def _row_num(driver_key: str, qd: date) -> Optional[float]:
                    rec = row_by_key_q.get((driver_key, qd))
                    val = pd.to_numeric((rec or {}).get("Value"), errors="coerce")
                    return float(val) if pd.notna(val) else None

                for qd in sorted({row.get("Quarter") for row in rows if isinstance(row.get("Quarter"), date)}):
                    if not isinstance(qd, date):
                        continue
                    if qd < date(2026, 3, 31):
                        if qd < date(2025, 9, 30) and _row_num("45z_value_realized", qd) is not None:
                            ui_info_rows.append(
                                {
                                    "quarter": qd,
                                    "metric": "Operating_Drivers",
                                    "severity": "warning",
                                    "message": "pre-Q3 2025 period has a 45Z statement-of-operations value; verify source support.",
                                    "source": "",
                                }
                            )
                        continue
                    has_treatment_note = ("45z_accounting_treatment_cogs_reduction", qd) in row_by_key_q
                    total = _row_num("adjusted_ebitda_reported", qd)
                    base = _row_num("adjusted_ebitda_ex_45z_base_business", qd)
                    adj_45z = _row_num("45z_adjusted_ebitda_component", qd)
                    cogs_45z = _row_num("45z_ethanol_cogs_crush_component", qd)
                    reported_crush = _row_num("consolidated_ethanol_crush_margin", qd)
                    ex_45z_crush = _row_num("crush_margin_ex_45z", qd)
                    ptc_current = _row_num("production_tax_credits_current_asset", qd)
                    ptc_increase = _row_num("production_tax_credits_working_capital_increase", qd)
                    if not has_treatment_note and any(v is not None for v in (adj_45z, cogs_45z, ptc_current)):
                        ui_info_rows.append(
                            {
                                "quarter": qd,
                                "metric": "Operating_Drivers",
                                "severity": "warning",
                                "message": "Q1 2026+ has ASU 2025-10 45Z values but no workbook disclosure note.",
                                "source": "",
                            }
                        )
                    if total is not None and base is not None and base > total + 0.01:
                        ui_info_rows.append(
                            {
                                "quarter": qd,
                                "metric": "Operating_Drivers",
                                "severity": "warning",
                                "message": "base-business adjusted EBITDA is greater than adjusted EBITDA total.",
                                "source": "",
                            }
                        )
                    if reported_crush is not None and ex_45z_crush is None and cogs_45z is not None:
                        # The Q1 2026 release separately discloses the ethanol-production 45Z COGS/crush
                        # component; use that bridge for ex-45Z crush instead of the Adj EBITDA component.
                        ex_45z_crush = float(reported_crush) - float(cogs_45z)
                        row_by_key_q[("crush_margin_ex_45z", qd)] = {
                            "Quarter": qd,
                            "_driver_key": "crush_margin_ex_45z",
                            "Driver group": "Margin / spread",
                            "Driver": "Crush margin ex-45Z",
                            "Source type": "earnings_release",
                            "Source": "",
                            "Value": float(ex_45z_crush),
                            "Unit": "$m",
                            "Commentary": "Derived as reported consolidated crush margin less ethanol-production 45Z COGS/crush component.",
                            "Quality": "modeled",
                        }
                        rows.append(row_by_key_q[("crush_margin_ex_45z", qd)])
                    if (
                        reported_crush is not None
                        and ex_45z_crush is not None
                        and adj_45z is not None
                        and cogs_45z is not None
                        and abs(adj_45z - cogs_45z) > 0.05
                        and abs(ex_45z_crush - (reported_crush - adj_45z)) <= 0.05
                    ):
                        ui_info_rows.append(
                            {
                                "quarter": qd,
                                "metric": "Operating_Drivers",
                                "severity": "warning",
                                "message": "crush ex-45Z uses the adjusted EBITDA 45Z amount when an ethanol-production COGS/crush 45Z amount is separately disclosed.",
                                "source": "",
                            }
                        )
                    if ptc_current is not None and ptc_increase is not None and ptc_increase > 0 and _row_num("45z_cash_received", qd) is not None:
                        ui_info_rows.append(
                            {
                                "quarter": qd,
                                "metric": "Operating_Drivers",
                                "severity": "warning",
                                "message": "production tax credits asset increased while a 45Z cash-received value is populated; verify cash monetization source.",
                                "source": "",
                            }
                        )
                if isinstance(derivative_oci_bridge_df, pd.DataFrame) and not derivative_oci_bridge_df.empty:
                    def _fmt_derivative_millions(usd_value: Any) -> str:
                        val = pd.to_numeric(usd_value, errors="coerce")
                        if pd.isna(val):
                            return ""
                        sign = "-" if float(val) < 0 else ""
                        return f"{sign}${abs(float(val)) / 1_000_000.0:,.1f}m"

                    for _, der_row in derivative_oci_bridge_df.iterrows():
                        qd_val = pd.to_datetime(der_row.get("quarter"), errors="coerce")
                        if pd.isna(qd_val):
                            continue
                        der_qd = qd_val.date()
                        source_doc = str(der_row.get("derivative_source_document") or "").strip()
                        source_type = "10-K" if "10K" in source_doc.upper() else "10-Q"
                        pnl_total = pd.to_numeric(der_row.get("derivative_gain_loss_pnl_total_usd"), errors="coerce")
                        pnl_rev = pd.to_numeric(der_row.get("derivative_gain_loss_revenue_usd"), errors="coerce")
                        pnl_cogs = pd.to_numeric(der_row.get("derivative_gain_loss_cogs_usd"), errors="coerce")

                        def _add_derivative_driver_metric(
                            key: str,
                            label: str,
                            usd_value: Any,
                            *,
                            commentary: str = "",
                        ) -> None:
                            val = pd.to_numeric(usd_value, errors="coerce")
                            if pd.isna(val) or (key, der_qd) in row_by_key_q:
                                return
                            rec = _make_driver_row(
                                der_qd,
                                key,
                                "Derivative / hedge memo",
                                label,
                                source_type,
                                source_doc,
                                commentary=commentary,
                                quality="memo",
                                value=float(val) / 1_000_000.0,
                                unit="$m",
                                source_note=str(der_row.get("derivative_notes") or ""),
                            )
                            rows.append(rec)
                            row_by_key_q[(key, der_qd)] = rec

                        if pd.notna(pnl_total):
                            commentary = (
                                f"Derivative P&L impact was {_fmt_derivative_millions(pnl_total)}, split between "
                                f"revenue ({_fmt_derivative_millions(pnl_rev)}) and COGS ({_fmt_derivative_millions(pnl_cogs)}); "
                                "this is already included in reported earnings."
                            )
                            _add_derivative_driver_metric(
                                "derivative_pnl_impact",
                                "Total derivative P&L",
                                pnl_total,
                                commentary=commentary,
                            )
                        _add_derivative_driver_metric(
                            "derivative_pnl_revenue",
                            "Derivative P&L in revenue",
                            pnl_rev,
                            commentary="Revenue-side derivative P&L is already included in reported revenue.",
                        )
                        _add_derivative_driver_metric(
                            "derivative_pnl_cogs",
                            "Derivative P&L in COGS",
                            pnl_cogs,
                            commentary="COGS-side derivative P&L is already included in reported COGS.",
                        )
                        _add_derivative_driver_metric(
                            "cash_flow_hedge_reclass_to_pnl",
                            "Cash-flow hedge reclass to P&L",
                            der_row.get("cash_flow_hedge_reclass_total_usd"),
                            commentary="Pre-tax cash-flow hedge reclassification into revenue/COGS; positive is favorable to P&L, negative is unfavorable.",
                        )
                        _add_derivative_driver_metric(
                            "derivative_net_asset_liability",
                            "Net derivative asset/liability",
                            der_row.get("derivative_net_asset_liability_usd"),
                            commentary="Period-end derivative balance-sheet snapshot; not current-period P&L.",
                        )
                        oci_val = pd.to_numeric(der_row.get("derivative_oci_current_period_usd"), errors="coerce")
                        _add_derivative_driver_metric(
                            "derivative_oci_movement",
                            "Derivative OCI movement",
                            oci_val,
                            commentary=(
                                f"OCI derivative movement was {_fmt_derivative_millions(oci_val)}; "
                                "(unrealized hedge cash-flow)."
                            ) if pd.notna(oci_val) else "",
                        )
                        _add_derivative_driver_metric(
                            "derivative_aoci",
                            "Derivative AOCI",
                            der_row.get("derivative_aoci_ending_balance_usd"),
                            commentary="AOCI is the accumulated OCI balance in equity for cash-flow hedges.",
                        )
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_best_text_cache = operating_driver_best_text_cache
                ctx_ref.derived.operating_driver_template_rows_cache = operating_driver_template_rows_cache
                ctx_ref.derived.operating_driver_template_candidate_cache = operating_driver_template_candidate_cache
                if operating_driver_line_index_by_quarter_cache is not None:
                    ctx_ref.derived.operating_driver_line_index_by_quarter = operating_driver_line_index_by_quarter_cache
                if operating_driver_flat_line_index_cache is not None:
                    ctx_ref.derived.operating_driver_flat_line_index = operating_driver_flat_line_index_cache
            return rows
            template_index = _load_operating_driver_template_index()
            templates = list(template_index.get("templates") or [])
            if not templates:
                return []
            template_by_key: Dict[str, Any] = dict(template_index.get("template_by_key") or {})
            operating_quarters = _operating_driver_quarters()
            _load_operating_driver_line_index_by_quarter()
            source_records_by_quarter = _load_operating_driver_source_records_by_quarter()
            bridge_bundle_map = _load_operating_driver_bridge_bundle_map(operating_quarters)

            def _driver_template_meta(driver_key: str, default_group: str, default_label: str, default_unit: str) -> Tuple[str, str, str]:
                tpl = template_by_key.get(str(driver_key or "").strip().lower())
                if tpl is None:
                    return default_group, default_label, default_unit
                return (
                    str(getattr(tpl, "group", "") or default_group),
                    str(getattr(tpl, "label", "") or default_label),
                    str(getattr(tpl, "preferred_unit", "") or default_unit),
                )

            row_map: Dict[Tuple[date, str, str], Dict[str, Any]] = {}
            with _timed_writer_substage("write_excel.derive.driver_inputs.template_rows"):
                # Template rows are cached per (quarter, template) because this is one of
                # the heavier writer-side text-selection paths. The cached rows feed the
                # visible Operating_Drivers sheet without rerunning template extraction.
                for qd in operating_quarters:
                    quarter_records = source_records_by_quarter.get(qd, [])
                    for tpl in templates:
                        template_key = str(getattr(tpl, "key", "") or getattr(tpl, "label", "") or "").strip().lower()
                        cache_key = (qd, template_key)
                        cached_rows = operating_driver_template_rows_cache.get(cache_key)
                        if cached_rows is None:
                            cached_rows = [dict(row) for row in _extract_operating_driver_rows_for_template(qd, tpl, quarter_records=quarter_records)]
                            operating_driver_template_rows_cache[cache_key] = cached_rows
                        for row in cached_rows:
                            row_key = (
                                row.get("Quarter"),
                                str(row.get("_driver_key") or ""),
                                str(row.get("_driver_scope") or ""),
                            )
                            prev = row_map.get(row_key)
                            row_map[row_key] = _merge_driver_rows(prev, row) if prev is not None else dict(row)
            if ctx_ref is not None:
                ctx_ref.derived.operating_driver_template_rows_cache = operating_driver_template_rows_cache
                ctx_ref.derived.operating_driver_template_candidate_cache = operating_driver_template_candidate_cache
            for qd in operating_quarters:
                best_bundle = bridge_bundle_map.get(qd)
                if best_bundle is None:
                    continue
                bundle_components = dict(best_bundle.get("components") or {})
                source_type = str(best_bundle.get("source_type") or "")
                source_doc = str(best_bundle.get("source_doc") or "")
                source_text = str(best_bundle.get("text") or "")
                same_basis_bridge = bool(best_bundle.get("bridge_context"))

                def _add_derived_driver_row(
                    driver_key: str,
                    value: Optional[float],
                    quality: str,
                    commentary: str,
                ) -> None:
                    if value is None:
                        return
                    group, label, unit = _driver_template_meta(driver_key, "Margin / spread", driver_key.replace("_", " "), "$m")
                    new_row = _make_driver_row(
                        qd,
                        driver_key,
                        group,
                        label,
                        source_type,
                        source_doc,
                        commentary=commentary,
                        quality=quality,
                        value=float(value),
                        unit=unit,
                        source_note=_driver_source_note(source_doc, commentary or source_text),
                    )
                    row_key = (qd, driver_key, "")
                    prev = row_map.get(row_key)
                    row_map[row_key] = _merge_driver_rows(prev, new_row) if prev is not None else new_row

                consolidated_val = pd.to_numeric(row_map.get((qd, "consolidated_ethanol_crush_margin", ""), {}).get("Value"), errors="coerce")
                consolidated = float(consolidated_val) if pd.notna(consolidated_val) else bundle_components.get("consolidated")
                ex_45z_val = None
                ex_45z_quality = "modeled"
                if "ex_45z" in bundle_components:
                    ex_45z_val = float(bundle_components["ex_45z"])
                    ex_45z_quality = "exact"
                elif consolidated is not None and "45z" in bundle_components and same_basis_bridge:
                    ex_45z_val = float(consolidated) - float(bundle_components["45z"])
                if ex_45z_val is not None:
                    note_txt = "Direct ex-45Z crush margin disclosure." if ex_45z_quality == "exact" else "Derived as consolidated crush margin less explicit same-quarter 45Z bridge component."
                    _add_derived_driver_row("crush_margin_ex_45z", ex_45z_val, ex_45z_quality, note_txt)

                ex_rin_val = None
                ex_rin_quality = "modeled"
                if "ex_rin" in bundle_components:
                    ex_rin_val = float(bundle_components["ex_rin"])
                    ex_rin_quality = "exact"
                elif consolidated is not None and "rin_sale" in bundle_components and same_basis_bridge:
                    ex_rin_val = float(consolidated) - float(bundle_components["rin_sale"])
                if ex_rin_val is not None:
                    note_txt = "Direct ex-RIN crush margin disclosure." if ex_rin_quality == "exact" else "Derived as consolidated crush margin less explicit same-quarter accumulated RIN-sale benefit."
                    _add_derived_driver_row("crush_margin_ex_rin", ex_rin_val, ex_rin_quality, note_txt)

                underlying_val = None
                underlying_quality = "modeled"
                underlying_used_keys: List[str] = []
                if "underlying" in bundle_components:
                    underlying_val = float(bundle_components["underlying"])
                    underlying_quality = "exact"
                elif consolidated is not None and same_basis_bridge:
                    baseline_val: Optional[float] = None
                    baseline_keys: List[str] = []
                    if ex_45z_val is not None:
                        baseline_val = float(ex_45z_val)
                        baseline_keys.append("45z")
                    elif ex_rin_val is not None:
                        baseline_val = float(ex_rin_val)
                        baseline_keys.append("rin_sale")
                    elif "45z" in bundle_components:
                        baseline_val = float(consolidated) - float(bundle_components["45z"])
                        baseline_keys.append("45z")
                    elif "rin_sale" in bundle_components:
                        baseline_val = float(consolidated) - float(bundle_components["rin_sale"])
                        baseline_keys.append("rin_sale")

                    # Only move beyond the ex-policy / ex-RIN baseline when the same-quarter bridge
                    # clearly exposes multiple non-core bridge items on a compatible basis.
                    full_bridge_ok = bool(
                        ("45z" in bundle_components and "impairment_assets_held_for_sale" in bundle_components)
                        or ("ex_45z" in bundle_components and "impairment_assets_held_for_sale" in bundle_components)
                    )
                    if baseline_val is None and full_bridge_ok:
                        baseline_val = float(consolidated)
                    if baseline_val is not None:
                        underlying_val = float(baseline_val)
                        underlying_used_keys.extend(baseline_keys)
                        bridge_adjustments: List[Tuple[str, float]] = []
                        if full_bridge_ok:
                            if "impairment_assets_held_for_sale" in bundle_components:
                                bridge_adjustments.append(("impairment_assets_held_for_sale", -float(bundle_components["impairment_assets_held_for_sale"])))
                            if "inventory_lcnrv" in bundle_components:
                                bridge_adjustments.append(("inventory_lcnrv", float(bundle_components["inventory_lcnrv"])))
                            if "intercompany_nonethanol_net" in bundle_components:
                                bridge_adjustments.append(("intercompany_nonethanol_net", -float(bundle_components["intercompany_nonethanol_net"])))
                            elif "nonrecurring_decommissioning" in bundle_components:
                                bridge_adjustments.append(("nonrecurring_decommissioning", -float(bundle_components["nonrecurring_decommissioning"])))
                        if bridge_adjustments:
                            underlying_val = float(underlying_val) + sum(v for _, v in bridge_adjustments)
                            underlying_used_keys.extend([k for k, _ in bridge_adjustments])
                if underlying_val is not None:
                    if underlying_quality == "exact":
                        note_txt = "Direct underlying crush margin disclosure."
                    else:
                        used_labels = {
                            "45z": "45Z",
                            "rin_sale": "RIN sale",
                            "impairment_assets_held_for_sale": "impairment",
                            "inventory_lcnrv": "inventory LCM/NRV",
                            "intercompany_nonethanol_net": "intercompany/nonethanol net",
                            "nonrecurring_decommissioning": "decommissioning",
                        }
                        used_keys = [
                            used_labels[k]
                            for k in (
                                "45z",
                                "rin_sale",
                                "impairment_assets_held_for_sale",
                                "inventory_lcnrv",
                                "intercompany_nonethanol_net",
                                "nonrecurring_decommissioning",
                            )
                            if k in underlying_used_keys
                        ]
                        note_txt = "Derived from explicit same-quarter crush bridge"
                        if used_keys:
                            note_txt += f" less {', '.join(used_keys)}"
                        note_txt += "."
                    _add_derived_driver_row("underlying_crush_margin", underlying_val, underlying_quality, note_txt)
            rows = list(row_map.values())
            if not rows:
                return []
            driver_quarter_map: Dict[str, Dict[date, Dict[str, Any]]] = {}
            for row in rows:
                dkey = str(row.get("_driver_key") or "")
                qd = row.get("Quarter")
                if not isinstance(qd, date):
                    continue
                driver_quarter_map.setdefault(dkey, {})[qd] = row
            for dkey, quarter_map in driver_quarter_map.items():
                q_list = sorted(quarter_map)
                for idx, qd in enumerate(q_list):
                    row = quarter_map[qd]
                    unit = str(row.get("Unit") or "")
                    if idx > 0:
                        row["QoQ change"] = _format_operating_driver_delta(row.get("Value"), quarter_map[q_list[idx - 1]].get("Value"), unit)
                    prev_year = date(qd.year - 1, qd.month, qd.day)
                    if prev_year in quarter_map:
                        row["YoY change"] = _format_operating_driver_delta(row.get("Value"), quarter_map[prev_year].get("Value"), unit)
            order_map = dict(template_index.get("order_map") or {})
            rows.sort(
                key=lambda row: (
                    -(int(row["Quarter"].strftime("%Y%m%d")) if isinstance(row.get("Quarter"), date) else 0),
                    order_map.get(str(row.get("_driver_key") or ""), 999),
                    str(row.get("Driver") or ""),
                )
            )
            return rows

        def _driver_unit_label(unit_txt: Any) -> str:
            unit = str(unit_txt or "").strip()
            if not unit:
                return ""
            unit_map = {
                "%": "%",
                "$m": "$m",
                "m gallons": "million gallons",
                "m lbs": "million lbs",
                "m bushels": "million bushels",
                "k tons": "k tons",
                "basis points": "bps",
                "subscribers": "subscribers",
                "text": "",
            }
            return unit_map.get(unit, unit)

        def _driver_row_label(driver_label: Any, unit_txt: Any) -> str:
            label = str(driver_label or "").strip()
            if is_anf_profile:
                return _anf_compact_driver_label(label, unit_txt)
            unit_label = _driver_unit_label(unit_txt)
            if not label or not unit_label:
                return label
            if unit_label in label:
                return label
            return f"{label} ({unit_label})"

        def _truncate_driver_text(txt: Any, max_chars: int = 96) -> str:
            s = glx_normalize_text(str(txt or ""))
            if len(s) <= max_chars:
                return s
            window = s[: max_chars + 1]
            sent_idx = max(window.rfind(". "), window.rfind("! "), window.rfind("? "), window.rfind("; "))
            if sent_idx >= int(max_chars * 0.55):
                return f"{window[: sent_idx + 1].strip()}..."
            ws_idx = window.rfind(" ")
            if ws_idx >= int(max_chars * 0.55):
                return f"{window[:ws_idx].rstrip(' ,;:-')}..."
            return f"{s[:max_chars].rstrip(' ,;:-')}..."

        def _operating_driver_order_map(templates_in: List[Any]) -> Dict[str, int]:
            return driver_operating_driver_order_map(templates_in)

        def _quarter_label_short(qd: Optional[date]) -> str:
            if not isinstance(qd, date):
                return ""
            if is_anf_profile:
                label = _anf_visible_quarter_label(qd)
                if label:
                    return label
            return f"{qd.year}-Q{((qd.month - 1) // 3) + 1}"


        def _sync_runtime_cache_state() -> None:
            operating_drivers_runtime.template_index_cache = operating_driver_template_index_cache
            operating_drivers_runtime.bridge_bundle_cache = operating_driver_bridge_bundle_cache
            operating_drivers_runtime.line_index_by_quarter_cache = operating_driver_line_index_by_quarter_cache
            operating_drivers_runtime.flat_line_index_cache = operating_driver_flat_line_index_cache
            operating_drivers_runtime.best_text_cache = operating_driver_best_text_cache
            operating_drivers_runtime.template_rows_cache = operating_driver_template_rows_cache
            operating_drivers_runtime.template_candidate_cache = operating_driver_template_candidate_cache
            operating_drivers_runtime.text_cache = operating_driver_text_cache
            operating_drivers_runtime.guidance_45z_docs_by_quarter_cache = operating_driver_45z_guidance_docs_by_quarter_cache

        self._build_operating_driver_rows = _build_operating_driver_rows
        self._operating_driver_quarters = _operating_driver_quarters
        self._driver_source_display = _driver_source_display
        self._driver_source_note = _driver_source_note
        self._read_operating_driver_text = _read_operating_driver_text
        self._operating_driver_follow_source_dirs = _operating_driver_follow_source_dirs
        self._operating_driver_financial_statement_files = _operating_driver_financial_statement_files
        self._load_operating_driver_source_records = _load_operating_driver_source_records
        self._load_operating_driver_source_records_by_quarter = _load_operating_driver_source_records_by_quarter
        self._load_operating_driver_line_index_by_quarter = _load_operating_driver_line_index_by_quarter
        self._load_operating_driver_flat_line_index = _load_operating_driver_flat_line_index
        self._is_crush_margin_bridge_candidate = _is_crush_margin_bridge_candidate
        self._parse_driver_number = _parse_driver_number
        self._strip_inline_footnotes = _strip_inline_footnotes
        self._extract_driver_numeric_values = _extract_driver_numeric_values
        self._cached_driver_metric_parse = _cached_driver_metric_parse
        self._parse_named_driver_metric_value_m = _parse_named_driver_metric_value_m
        self._parse_crush_45z_component_m = _parse_crush_45z_component_m
        self._parse_crush_rin_component_m = _parse_crush_rin_component_m
        self._parse_crush_decommissioning_component_m = _parse_crush_decommissioning_component_m
        self._driver_snippet = _driver_snippet
        self._driver_best_text_record = _driver_best_text_record
        self._parse_utilization_value = _parse_utilization_value
        self._parse_crush_margin_value_m = _parse_crush_margin_value_m
        self._parse_distillers_grains_k_tons = _parse_distillers_grains_k_tons
        self._parse_uhp_k_tons = _parse_uhp_k_tons
        self._parse_corn_consumed_m_bushels = _parse_corn_consumed_m_bushels
        self._parse_renewable_corn_oil_m_lbs = _parse_renewable_corn_oil_m_lbs
        self._parse_rin_impact_value_m = _parse_rin_impact_value_m
        self._extract_crush_margin_bridge_details = _extract_crush_margin_bridge_details
        self._get_crush_margin_bridge_details = _get_crush_margin_bridge_details
        self._prime_operating_driver_crush_detail_cache = _prime_operating_driver_crush_detail_cache
        self._load_operating_driver_template_index = _load_operating_driver_template_index
        self._load_operating_driver_45z_guidance_docs_by_quarter = _load_operating_driver_45z_guidance_docs_by_quarter
        self._operating_driver_template_spec = _operating_driver_template_spec
        self._template_candidate_terms = _template_candidate_terms
        self._text_matches_template_terms = _text_matches_template_terms
        self._candidate_records_for_template = _candidate_records_for_template
        self._build_operating_driver_bridge_bundle = _build_operating_driver_bridge_bundle
        self._load_operating_driver_bridge_bundle_map = _load_operating_driver_bridge_bundle_map
        self._parse_threshold_amount_m = _parse_threshold_amount_m
        self._parse_45z_realized_value_m = _parse_45z_realized_value_m
        self._driver_quality_rank = _driver_quality_rank
        self._operating_drivers_runtime_deps = _operating_drivers_runtime_deps
        self._merge_driver_rows = _merge_driver_rows
        self._make_driver_row = _make_driver_row
        self._build_anf_operating_driver_rows = _build_anf_operating_driver_rows
        self._gpre_canonical_crush_series_for_drivers_local = _gpre_canonical_crush_series_for_drivers_local
        self._extract_operating_driver_rows_for_template = _extract_operating_driver_rows_for_template
        self._format_operating_driver_delta = _format_operating_driver_delta
        self._build_operating_drivers_history_rows = _build_operating_drivers_history_rows
        self._driver_unit_label = _driver_unit_label
        self._driver_row_label = _driver_row_label
        self._truncate_driver_text = _truncate_driver_text
        self._operating_driver_order_map = _operating_driver_order_map
        self._sync_runtime_cache_state = _sync_runtime_cache_state

    def build_operating_driver_rows(self) -> List[Dict[str, Any]]:
        return self._build_operating_driver_rows()

    def operating_driver_quarters(self) -> List[Any]:
        return self._operating_driver_quarters()

    def driver_source_display(self, source_type: Any, source_doc: Any = "") -> str:
        return self._driver_source_display(source_type, source_doc)

    def driver_source_note(self, source_doc: Any, snippet: Any = "", extra: Any = "") -> str:
        return self._driver_source_note(source_doc, snippet, extra)

    def read_operating_driver_text(self, path_in: Any) -> str:
        return self._read_operating_driver_text(path_in)

    def operating_driver_follow_source_dirs(self) -> List[Tuple[str, Path]]:
        return self._operating_driver_follow_source_dirs()

    def operating_driver_financial_statement_files(self) -> List[Path]:
        return self._operating_driver_financial_statement_files()

    def load_source_records(self) -> List[Dict[str, Any]]:
        return self._load_operating_driver_source_records()

    def load_source_records_by_quarter(self) -> Dict[Any, List[Dict[str, Any]]]:
        return self._load_operating_driver_source_records_by_quarter()

    def load_line_index_by_quarter(self) -> Dict[Any, List[Dict[str, Any]]]:
        return self._load_operating_driver_line_index_by_quarter()

    def load_flat_line_index(self) -> List[Dict[str, Any]]:
        return self._load_operating_driver_flat_line_index()

    def is_crush_margin_bridge_candidate(self, text_in: Any) -> bool:
        return self._is_crush_margin_bridge_candidate(text_in)

    def parse_driver_number(self, token: Any) -> Optional[float]:
        return self._parse_driver_number(token)

    def extract_driver_numeric_values(self, text_in: Any) -> List[float]:
        return self._extract_driver_numeric_values(text_in)

    def get_crush_margin_bridge_details(self, text_in: Any) -> Dict[str, Any]:
        return self._get_crush_margin_bridge_details(text_in)

    def prime_operating_driver_crush_detail_cache(self, records: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
        return self._prime_operating_driver_crush_detail_cache(records)

    def load_template_index(self) -> Dict[str, Any]:
        return self._load_operating_driver_template_index()

    def load_45z_guidance_docs_by_quarter(self) -> Dict[Any, List[Dict[str, Any]]]:
        return self._load_operating_driver_45z_guidance_docs_by_quarter()

    def operating_driver_template_spec(self, tpl: Any) -> Dict[str, Any]:
        return self._operating_driver_template_spec(tpl)

    def candidate_records_for_template(self, qd: Any, template_spec: Dict[str, Any], quarter_records: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        return self._candidate_records_for_template(qd, template_spec, quarter_records=quarter_records)

    def load_bridge_bundle_map(self, quarter_set: List[Any]) -> Dict[Any, Dict[str, Any]]:
        return self._load_operating_driver_bridge_bundle_map(quarter_set)

    def parse_threshold_amount_m(self, text_in: Any) -> Optional[float]:
        return self._parse_threshold_amount_m(text_in)

    def parse_45z_realized_value_m(self, text_in: Any) -> Optional[float]:
        return self._parse_45z_realized_value_m(text_in)

    def merge_driver_rows(self, existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
        return self._merge_driver_rows(existing, candidate)

    def make_driver_row(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self._make_driver_row(*args, **kwargs)

    def build_anf_operating_driver_rows(self) -> List[Dict[str, Any]]:
        return self._build_anf_operating_driver_rows()

    def gpre_canonical_crush_series_for_drivers_local(self) -> Dict[Any, Dict[str, Any]]:
        return self._gpre_canonical_crush_series_for_drivers_local()

    def extract_rows_for_template(self, qd: Any, tpl: Any, *, quarter_records: Any = None) -> List[Dict[str, Any]]:
        return self._extract_operating_driver_rows_for_template(qd, tpl, quarter_records=quarter_records)

    def format_operating_driver_delta(self, current_val: Any, prior_val: Any, unit: str) -> str:
        return self._format_operating_driver_delta(current_val, prior_val, unit)

    def build_operating_drivers_history_rows(self) -> List[Dict[str, Any]]:
        return self._build_operating_drivers_history_rows()

    def driver_unit_label(self, unit_txt: Any) -> str:
        return self._driver_unit_label(unit_txt)

    def driver_row_label(self, driver_label: Any, unit_txt: Any) -> str:
        return self._driver_row_label(driver_label, unit_txt)

    def truncate_driver_text(self, txt: Any, max_chars: int = 96) -> str:
        return self._truncate_driver_text(txt, max_chars)

    def operating_driver_order_map(self, templates_in: List[Any]) -> Dict[str, int]:
        return self._operating_driver_order_map(templates_in)

    def sync_runtime_cache_state(self) -> None:
        self._sync_runtime_cache_state()
