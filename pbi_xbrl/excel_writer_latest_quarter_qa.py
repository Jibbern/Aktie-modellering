"""Latest-quarter QA support for workbook writer."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple, Callable

import pandas as pd

from .conference_metadata import (
    metadata_audit_flags,
    metadata_source_file,
    parse_metadata_key_values,
    source_material_role,
)


@dataclass(frozen=True)
class LatestQuarterQADeps:
    ticker: str
    company_profile: Any
    is_pbi_profile: bool
    is_gpre_profile: bool
    is_anf_profile: bool
    hist: Any
    leverage_df: Any
    adj_metrics: Any
    audit: Any
    slides_guidance: Any
    slides_segments: Any
    debt_tranches_latest: Any
    debt_buckets: Any
    debt_profile: Any
    debt_recon: Any
    revolver_history: Any
    non_gaap_files: Any
    cache_root: Any
    material_roots: Any
    ticker_roots: Any
    document_cache: Any
    ui_state: MutableMapping[str, Any]
    ctx_ref: Any
    context_helpers: Mapping[str, Any]


def _is_quarter_financial_qa_source_candidate(
    source_kind: str,
    text_norm: Any,
    *,
    selection_reason: str = "",
    match_reasons: Sequence[str] = (),
) -> bool:
    """Return False for event/thematic EX-99 docs that only match via filing metadata.

    Latest-quarter financial QA should reconcile to the quarter filing/release corpus.
    Some subsequent-event decks or investor-day exhibits share an accession/audit path
    with model-relevant events, but they should not become the numeric source for
    reported-quarter revenue/FCF/cash/debt checks unless the document itself has a
    filename/text quarter match.
    """
    source_kind_l = str(source_kind or "").strip().lower()
    reasons_l = {str(x or "").strip().lower() for x in (match_reasons or ())}
    if not reasons_l:
        return True
    if source_kind_l not in {"earnings_release", "earnings_presentation", "press_release"}:
        return True
    if "filename quarter match" in reasons_l:
        return True
    text_l = re.sub(r"\s+", " ", str(text_norm or "").strip().lower())
    quarter_results = bool(
        re.search(
            r"\b(?:reports?|announces?|releases?)\s+(?:its\s+)?"
            r"(?:q[1-4]|first\s+quarter|second\s+quarter|third\s+quarter|fourth\s+quarter)"
            r"\s+20\d{2}\s+(?:financial\s+)?results\b"
            r"|\b(?:q[1-4]|first\s+quarter|second\s+quarter|third\s+quarter|fourth\s+quarter)"
            r"\s+20\d{2}\s+(?:financial\s+)?results\b"
            r"|\b(?:quarter|three\s+months)\s+ended\s+[a-z]+\s+\d{1,2},\s+20\d{2}\b",
            text_l,
            re.I,
        )
    )
    if quarter_results:
        return True
    selection_l = str(selection_reason or "").strip().lower()
    metadata_only_or_text_only = reasons_l.issubset({"submission quarter match", "text quarter match"})
    if metadata_only_or_text_only and ("accession doc" in selection_l or "submission" in selection_l):
        return False
    return True


class LatestQuarterQASupport:
    def __init__(self, deps: LatestQuarterQADeps) -> None:
        self.deps = deps

    def _qa_source_type_for_path_local(self, path_in: Path) -> str:
        blob = str(path_in).lower()
        if re.search(r"(presentation|slides)", blob, re.I):
            return "earnings_presentation"
        if re.search(r"(earningspressrelea|earnings[_ -]?press|earnings[_ -]?release|ex99)", blob, re.I):
            return "earnings_release"
        if re.search(r"(press[_ -]?release|businesswire|globenewswire)", blob, re.I):
            return "press_release"
        if re.search(r"(transcript|conference[_ -]?call|earnings_transcripts)", blob, re.I):
            return "transcript"
        return "support"

    def source_bundle(self, 
        qref: pd.Timestamp,
        *,
        include_transcripts: bool = False,
    ) -> List[Dict[str, Any]]:
        deps = self.deps
        helpers = deps.context_helpers
        ticker = deps.ticker
        audit = deps.audit
        cache_root = deps.cache_root
        material_roots = deps.material_roots
        ticker_roots = deps.ticker_roots
        document_cache = deps.document_cache
        _audit_view = helpers["_audit_view"]
        _resolve_col = helpers["_resolve_col"]
        _parse_quarter_from_filename = helpers["_parse_quarter_from_filename"]
        _parse_quarter_from_follow_text = helpers["_parse_quarter_from_follow_text"]
        infer_quarter_end_from_text = helpers["infer_quarter_end_from_text"]
        _path_belongs_to_ticker = helpers["_path_belongs_to_ticker"]
        _read_cached_doc_text = helpers["_read_cached_doc_text"]
        _sec_docs_for_accession = helpers["_sec_docs_for_accession"]
        _submission_recent_rows = helpers["_submission_recent_rows"]
        _submission_recent_row_quarter = helpers["_submission_recent_row_quarter"]
        _resolve_cached_doc_path = helpers["_resolve_cached_doc_path"]
        _qa_source_type_for_path_local = self._qa_source_type_for_path_local
        cache_key = pd.Timestamp(qref).normalize().date().isoformat()
        if not include_transcripts:
            cached_bundle = document_cache.latest_quarter_qa_bundle_by_quarter.get(cache_key)
            if cached_bundle is not None:
                return [dict(x) for x in cached_bundle]

        q0 = pd.Timestamp(qref)
        q0d = q0.date()

        def _qa_norm_txt(v: Any) -> str:
            return re.sub(r"\s+", " ", str(v or "").strip())

        def _quarter_match_reasons(
            path_in: Path,
            text_in: str,
            *,
            submission_quarter: Optional[date] = None,
        ) -> List[str]:
            reasons: List[str] = []
            q_name = _parse_quarter_from_filename(path_in.name)
            if isinstance(q_name, date) and q_name == q0d:
                reasons.append("filename quarter match")
            q_text = _parse_quarter_from_follow_text(text_in) or infer_quarter_end_from_text(text_in)
            if isinstance(q_text, date) and q_text == q0d:
                reasons.append("text quarter match")
            if isinstance(submission_quarter, date) and submission_quarter == q0d:
                reasons.append("submission quarter match")
            return reasons

        def _qa_text_matches_current_company_local(path_in: Path, text_in: Any = "") -> bool:
            text = _qa_norm_txt(text_in)
            if not text:
                return False
            return _path_belongs_to_ticker(path_in, ticker, ticker_roots)

        def _doc_family_local(path_in: Path) -> str:
            stem = str(path_in.stem or "").strip().lower()
            stem = re.sub(r"doc_[0-9]+_", "", stem)
            stem = re.sub(r"[^a-z0-9]+", "_", stem).strip("_")
            return stem or path_in.name.lower()

        def _source_sort_key(rec_in: Dict[str, Any]) -> Tuple[int, int, int, int, str]:
            source_type = str(rec_in.get("source_type") or "")
            source_rank = {
                "earnings_release": 0,
                "earnings_presentation": 1,
                "ceo_letter": 2,
                "press_release": 3,
                "transcript": 4,
                "conference": 5,
                "support": 6,
            }.get(source_type, 7)
            official_rank = 0 if bool(rec_in.get("is_official")) else 1
            metadata_rank = 0 if str(rec_in.get("source_role") or "") == "metadata_primary" else 1
            source_doc = str(rec_in.get("source_doc") or "").lower()
            ext_rank = 1 if source_doc.endswith(".pdf") else 0
            return (source_rank, official_rank, metadata_rank, ext_rank, source_doc)

        out_records: List[Dict[str, Any]] = []
        seen_paths: set[str] = set()

        def _add_source_record(
            path_in: Optional[Path],
            *,
            source_type: str = "",
            submission_quarter: Optional[date] = None,
            selection_reason: str = "",
            is_official: bool = True,
        ) -> None:
            if path_in is None or not path_in.exists():
                return
            if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                return
            if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                return
            resolved_key = str(path_in.resolve()) if path_in.exists() else str(path_in)
            if resolved_key in seen_paths:
                return
            source_kind = str(source_type or _qa_source_type_for_path_local(path_in)).strip().lower()
            if source_kind == "transcript" and not include_transcripts:
                return
            try:
                text_norm = _qa_norm_txt(_read_cached_doc_text(path_in))
            except Exception:
                text_norm = ""
            if not text_norm:
                return
            if not _qa_text_matches_current_company_local(path_in, text_norm):
                return
            reasons = _quarter_match_reasons(path_in, text_norm, submission_quarter=submission_quarter)
            if not reasons:
                return
            if not _is_quarter_financial_qa_source_candidate(
                source_kind,
                text_norm,
                selection_reason=selection_reason,
                match_reasons=reasons,
            ):
                return
            role = source_material_role(path_in)
            metadata_values: Dict[str, str] = {}
            if role == "metadata_primary":
                try:
                    metadata_values = parse_metadata_key_values(path_in.read_text(encoding="utf-8", errors="ignore"))
                except Exception:
                    metadata_values = {}
            seen_paths.add(resolved_key)
            out_records.append(
                {
                    "quarter": q0d,
                    "source_type": source_kind,
                    "source_doc": str(path_in),
                    "doc_family": _doc_family_local(path_in),
                    "text": text_norm,
                    "is_official": bool(is_official and source_kind != "transcript"),
                    "selection_reason": selection_reason or "; ".join(reasons),
                    "source_role": role,
                    "audit_flags": "; ".join(metadata_audit_flags(metadata_values)),
                    "metadata_source_file": metadata_source_file(metadata_values),
                    "replaced_by_metadata": False,
                }
            )

        if audit is not None and not audit.empty:
            aq = _audit_view()
            accn_col = _resolve_col(aq, ["accn"])
            form_col = _resolve_col(aq, ["form"])
            if accn_col:
                aq = aq[aq["_quarter"].notna()]
                aq = aq[aq["_quarter"].dt.to_period("Q") == q0.to_period("Q")]
                if form_col:
                    aq = aq[aq[form_col].astype(str).str.upper().str.startswith(("8-K", "10-Q", "10-K"))]
                for _, rr in aq.dropna(subset=[accn_col]).iterrows():
                    accn = str(rr.get(accn_col) or "").strip()
                    if not accn:
                        continue
                    for cand in _sec_docs_for_accession(accn):
                        source_type = _qa_source_type_for_path_local(cand)
                        if source_type == "support":
                            continue
                        _add_source_record(
                            cand,
                            source_type=source_type,
                            submission_quarter=q0d,
                            selection_reason="audit accession doc",
                            is_official=True,
                        )

        cache_dir = cache_root
        if cache_dir.exists():
            for fr in _submission_recent_rows(max_files=12):
                form = str(fr.get("form") or "").upper().strip()
                if not form.startswith(("8-K", "10-Q", "10-K")):
                    continue
                accn = str(fr.get("accn") or "").strip()
                if not accn:
                    continue
                q_guess = _submission_recent_row_quarter(fr)
                if q_guess is None or pd.Timestamp(q_guess).to_period("Q") != q0.to_period("Q"):
                    continue
                primary = str(fr.get("doc") or "").strip()
                if primary:
                    cand = _resolve_cached_doc_path(accn=accn, doc_name=primary, path_hint=primary)
                    if cand is not None:
                        _add_source_record(
                            cand,
                            source_type=_qa_source_type_for_path_local(cand),
                            submission_quarter=q_guess,
                            selection_reason="submission primary doc",
                            is_official=True,
                        )
                for cand in _sec_docs_for_accession(accn):
                    source_type = _qa_source_type_for_path_local(cand)
                    if source_type == "support":
                        continue
                    _add_source_record(
                        cand,
                        source_type=source_type,
                        submission_quarter=q_guess,
                        selection_reason="submission accession doc",
                        is_official=True,
                    )

        local_dir_specs: List[Tuple[str, Tuple[str, ...], bool]] = [
            ("earnings_release", ("earnings_release", "Earnings Release", "Earnings Releases"), True),
            ("earnings_presentation", ("earnings_presentation", "slides", "presentation", "presentations"), True),
            ("ceo_letter", ("CEO_letters", "CEO letters", "ceo_letters"), False),
            ("press_release", ("press_release", "Press Release"), True),
            ("transcript", ("earnings_transcripts", "transcripts"), False),
            ("conference", ("conferences", "Conferences"), False),
        ]
        for source_type, dir_names, is_official in local_dir_specs:
            if source_type == "transcript" and not include_transcripts:
                continue
            for root in material_roots:
                for dir_name in dir_names:
                    subdir = root / dir_name
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
                        _add_source_record(
                            path_in,
                            source_type=source_type,
                            selection_reason=f"local {source_type} dir",
                            is_official=is_official,
                        )

        metadata_source_paths: Set[str] = set()
        for rec in out_records:
            if str(rec.get("source_role") or "") != "metadata_primary":
                continue
            doc_path = Path(str(rec.get("source_doc") or ""))
            source_file = str(rec.get("metadata_source_file") or "").strip()
            candidate_paths: List[Path] = []
            if source_file:
                candidate_paths.append(doc_path.parent / source_file)
            base_stem = re.sub(r"_METADATA_EN$", "", doc_path.stem, flags=re.I)
            if base_stem and base_stem != doc_path.stem:
                candidate_paths.extend(
                    [
                        doc_path.with_name(f"{base_stem}.txt"),
                        doc_path.with_name(f"{base_stem}.pdf"),
                        doc_path.with_name(f"{base_stem}.htm"),
                        doc_path.with_name(f"{base_stem}.html"),
                    ]
                )
            for candidate_path in candidate_paths:
                try:
                    metadata_source_paths.add(str(candidate_path.resolve()).lower())
                except Exception:
                    metadata_source_paths.add(str(candidate_path).lower())

        for rec in out_records:
            if str(rec.get("source_role") or "") != "source_qa_raw":
                continue
            try:
                source_path_key = str(Path(str(rec.get("source_doc") or "")).resolve()).lower()
            except Exception:
                source_path_key = str(rec.get("source_doc") or "").lower()
            if source_path_key in metadata_source_paths:
                rec["replaced_by_metadata"] = True
                rec["selection_reason"] = f"{rec.get('selection_reason') or ''}; retained source QA, metadata primary".strip("; ")

        out_records = sorted(out_records, key=_source_sort_key)
        if not include_transcripts:
            document_cache.latest_quarter_qa_bundle_by_quarter[cache_key] = [dict(x) for x in out_records]
        return [dict(x) for x in out_records]

    def sec_text_corpus(self, qref: pd.Timestamp) -> str:
        deps = self.deps
        document_cache = deps.document_cache
        _latest_quarter_qa_source_bundle = self.source_bundle
        cache_key = pd.Timestamp(qref).normalize().date().isoformat()
        cached_text = document_cache.latest_quarter_sec_text_by_quarter.get(cache_key)
        if cached_text is not None:
            return cached_text
        source_bundle = _latest_quarter_qa_source_bundle(qref)
        sec_text = " ".join(
            re.sub(r"\s+", " ", str(rec.get("text") or "").strip())
            for rec in source_bundle
            if str(rec.get("text") or "").strip() and not bool(rec.get("replaced_by_metadata"))
        )
        document_cache.latest_quarter_sec_text_by_quarter[cache_key] = sec_text
        return sec_text

    def run(self) -> List[Dict[str, Any]]:
        deps = self.deps
        helpers = deps.context_helpers
        ticker = deps.ticker
        company_profile = deps.company_profile
        is_pbi_profile = deps.is_pbi_profile
        is_gpre_profile = deps.is_gpre_profile
        is_anf_profile = deps.is_anf_profile
        hist = deps.hist
        leverage_df = deps.leverage_df
        adj_metrics = deps.adj_metrics
        slides_guidance = deps.slides_guidance
        slides_segments = deps.slides_segments
        debt_tranches_latest = deps.debt_tranches_latest
        debt_buckets = deps.debt_buckets
        debt_profile = deps.debt_profile
        debt_recon = deps.debt_recon
        revolver_history = deps.revolver_history
        non_gaap_files = deps.non_gaap_files
        material_roots = deps.material_roots
        ticker_roots = deps.ticker_roots
        ui_state = deps.ui_state
        ctx_ref = deps.ctx_ref
        _hist_view = helpers["_hist_view"]
        _adj_metrics_view = helpers["_adj_metrics_view"]
        _resolve_col = helpers["_resolve_col"]
        _ensure_valuation_render_bundle = helpers["_ensure_valuation_render_bundle"]
        _ensure_valuation_precompute_bundle = helpers["_ensure_valuation_precompute_bundle"]
        _timed_writer_substage = helpers["_timed_writer_substage"]
        _pbi_reported_fcf_payload_for_qd = helpers["_pbi_reported_fcf_payload_for_qd"]
        _anf_financial_schedule_support_doc_for_quarter = helpers["_anf_financial_schedule_support_doc_for_quarter"]
        _slides_guidance_has_explicit_metric = helpers["_slides_guidance_has_explicit_metric"]
        _submission_recent_rows = helpers["_submission_recent_rows"]
        writer_qa_latest_quarter_support_gap_severity = helpers["writer_qa_latest_quarter_support_gap_severity"]
        _first_existing_material_dir = helpers["_first_existing_material_dir"]
        _path_belongs_to_ticker = helpers["_path_belongs_to_ticker"]
        ew_parse_quarterly_segment_data_from_workbook = helpers["ew_parse_quarterly_segment_data_from_workbook"]
        annual_segment_alias_patterns = helpers["annual_segment_alias_patterns"]
        quarterly_segment_labels = helpers["quarterly_segment_labels"]
        coerce_number = helpers["coerce_number"]
        _latest_quarter_qa_source_bundle = self.source_bundle
        qa_rows: List[Dict[str, Any]] = []
        if hist is None or hist.empty or "quarter" not in hist.columns:
            return qa_rows

        hq = _hist_view()
        hq = hq[hq["_quarter"].notna()].sort_values("_quarter")
        if hq.empty:
            return qa_rows
        q0 = pd.Timestamp(hq["_quarter"].max())
        q0d = q0.date()
        hq0 = hq[hq["_quarter"] == q0].iloc[-1]
        qa_quarters = tuple(pd.Timestamp(qv) for qv in hq["_quarter"].tolist())
        qa_render_bundle = dict(getattr(getattr(ctx_ref, "derived", None), "valuation_render_bundle", {}) or {})
        if not qa_render_bundle:
            qa_render_bundle = _ensure_valuation_render_bundle(qa_quarters, leverage_df)
        qa_precompute_bundle = dict(getattr(getattr(ctx_ref, "derived", None), "valuation_precompute_bundle", {}) or {})
        if not qa_precompute_bundle:
            qa_precompute_bundle = _ensure_valuation_precompute_bundle(qa_quarters, qa_render_bundle)
        ebitda_map = dict(qa_render_bundle.get("ebitda_map") or {})
        net_income_map = dict(qa_render_bundle.get("net_income_map") or {})
        buyback_map = dict(qa_precompute_bundle.get("buyback_map") or qa_render_bundle.get("buyback_map") or {})
        buyback_shares_map = dict(qa_precompute_bundle.get("buyback_shares_map") or {})
        buyback_avg_price_doc_map = dict(qa_precompute_bundle.get("buyback_avg_price_doc_map") or {})
        buyback_doc_note_map = dict(qa_precompute_bundle.get("buyback_doc_note_map") or {})
        dividend_map = dict(qa_precompute_bundle.get("dividend_map") or qa_render_bundle.get("dividend_map") or {})
        docs_by_quarter = dict(qa_precompute_bundle.get("docs_by_quarter") or {})

        def _add(sev: str, metric: str, msg: str, src: str = "") -> None:
            qa_rows.append(
                {
                    "quarter": q0d,
                    "metric": metric,
                    "severity": sev,
                    "message": msg,
                    "source": src,
                }
            )

        def _qa_norm_txt(v: Any) -> str:
            return re.sub(r"\s+", " ", str(v or "").strip())

        def _money_m(v: Any) -> str:
            try:
                fv = float(v)
            except Exception:
                return "N/A"
            if not pd.notna(fv):
                return "N/A"
            return f"${fv/1e6:,.1f}m"

        def _qa_shares_m(v: Any) -> str:
            try:
                fv = float(v)
            except Exception:
                return "N/A"
            if not pd.notna(fv):
                return "N/A"
            return f"{fv/1_000_000.0:+,.3f}m"

        def _qmap_lookup(map_in: Any, qref: Any) -> Any:
            if not isinstance(map_in, dict):
                return None
            candidates: List[Any] = []
            if qref is not None:
                candidates.append(qref)
                try:
                    qts = pd.Timestamp(qref)
                    candidates.extend([qts, qts.normalize(), qts.date()])
                except Exception:
                    pass
            seen_local: set[str] = set()
            for key in candidates:
                key_sig = f"{type(key).__name__}:{repr(key)}"
                if key_sig in seen_local:
                    continue
                seen_local.add(key_sig)
                if key in map_in:
                    return map_in.get(key)
            return None

        def _docs_for_quarter(map_in: Any, qref: Any) -> List[Dict[str, Any]]:
            docs_local = _qmap_lookup(map_in, qref)
            if isinstance(docs_local, list):
                return [dict(x) for x in docs_local if isinstance(x, dict)]
            return []

        with _timed_writer_substage("write_excel.ui.latest_quarter_qa.sec_text"):
            source_bundle = _latest_quarter_qa_source_bundle(q0)
            sec_text = " ".join(str(rec.get("text") or "").strip() for rec in source_bundle if str(rec.get("text") or "").strip())
        sec_text_l = sec_text.lower()

        def _bundle_doc_names(bundle_in: Sequence[Dict[str, Any]]) -> str:
            if not bundle_in:
                return "selected release/presentation corpus"

            source_priority = [
                "earnings_release",
                "earnings_presentation",
                "press_release",
                "transcript",
                "support",
            ]
            type_rank = {label: idx for idx, label in enumerate(source_priority)}

            def _doc_sort_key(rec_in: Dict[str, Any]) -> Tuple[int, int, int, str]:
                source_type = str(rec_in.get("source_type") or "").strip().lower()
                source_doc = str(rec_in.get("source_doc") or "").strip()
                source_doc_low = source_doc.replace("\\", "/").lower()
                selection_reason = str(rec_in.get("selection_reason") or "").strip().lower()
                sec_cache_rank = 0 if "/sec_cache/" in source_doc_low else 1
                accession_rank = 0 if ("submission" in selection_reason or "accession" in selection_reason) else 1
                return (
                    type_rank.get(source_type, len(type_rank)),
                    sec_cache_rank,
                    accession_rank,
                    source_doc_low,
                )

            records = sorted(
                [dict(rec) for rec in bundle_in if isinstance(rec, dict)],
                key=_doc_sort_key,
            )

            picked_names: List[str] = []
            seen_names: set[str] = set()
            seen_types: set[str] = set()
            for source_type in source_priority:
                for rec in records:
                    rec_type = str(rec.get("source_type") or "").strip().lower()
                    if rec_type != source_type or rec_type in seen_types:
                        continue
                    source_doc = str(rec.get("source_doc") or "").strip()
                    name = Path(source_doc).name if source_doc else ""
                    if not name or name in seen_names:
                        continue
                    picked_names.append(name)
                    seen_names.add(name)
                    seen_types.add(rec_type)
                    break
                if len(picked_names) >= 2:
                    break

            if len(picked_names) < 2:
                for rec in records:
                    source_doc = str(rec.get("source_doc") or "").strip()
                    name = Path(source_doc).name if source_doc else ""
                    if not name or name in seen_names:
                        continue
                    picked_names.append(name)
                    seen_names.add(name)
                    if len(picked_names) >= 2:
                        break

            if not picked_names:
                return "selected release/presentation corpus"
            return " | ".join(picked_names)

        bundle_source_label = _bundle_doc_names(source_bundle)

        def _qa_support_result(
            value_in: Optional[float],
            support_kind: str,
            doc_rec: Optional[Dict[str, Any]],
            excerpt_in: str,
            *,
            confidence: float = 1.0,
            source_doc_override: str = "",
            source_type_override: str = "",
            basis_kind: str = "",
            basis_note: str = "",
            same_basis: Optional[bool] = None,
        ) -> Dict[str, Any]:
            return {
                "value": float(value_in) if value_in is not None else None,
                "support_kind": str(support_kind or ""),
                "source_type": str(source_type_override or (doc_rec.get("source_type") if isinstance(doc_rec, dict) else "") or ""),
                "source_doc": str(source_doc_override or (doc_rec.get("source_doc") if isinstance(doc_rec, dict) else "") or ""),
                "excerpt": str(excerpt_in or "").strip(),
                "confidence": float(confidence),
                "basis_kind": str(basis_kind or "").strip(),
                "basis_note": str(basis_note or "").strip(),
                "same_basis": same_basis if same_basis is None else bool(same_basis),
            }

        def _qa_amount_from_token(
            token_in: Any,
            unit_in: Any,
            window_in: Any,
            *,
            default_small_unit: str = "million",
        ) -> Optional[float]:
            token_txt = str(token_in or "").strip()
            if not token_txt:
                return None
            negative = token_txt.startswith("(") or token_txt.endswith(")") or token_txt.startswith("-")
            num_txt = token_txt.strip("()").replace("$", "").replace(" ", "")
            num_val = coerce_number(num_txt)
            if num_val is None:
                return None
            unit_txt = str(unit_in or "").strip().lower()
            window = str(window_in or "")
            if unit_txt in {"billion", "bn", "b"}:
                scaled = float(num_val) * 1e9
            elif unit_txt in {"million", "m", "mm"}:
                scaled = float(num_val) * 1e6
            elif re.search(r"\b(?:in\s+thousands|unaudited;\s+in\s+thousands|thousands\)|\$?\s+in\s+thousands|000s)\b", window, re.I):
                scaled = float(num_val) * 1e3
            elif abs(float(num_val)) < 10_000.0 and default_small_unit == "million":
                scaled = float(num_val) * 1e6
            else:
                scaled = float(num_val)
            if negative:
                scaled = -abs(float(scaled))
            return float(scaled)

        def _extract_labeled_amount_from_doc(
            doc_rec: Dict[str, Any],
            label_patterns: Sequence[str],
            *,
            max_gap: int = 80,
            default_small_unit: str = "million",
            extra_guard: Optional[Callable[[str], bool]] = None,
            confidence: float = 1.0,
            support_kind: str = "explicit",
        ) -> Optional[Dict[str, Any]]:
            txt = str(doc_rec.get("text") or "")
            if not txt:
                return None
            amount_pat = r"(\(?-?[0-9]{1,4}(?:,[0-9]{3})*(?:\.\d+)?\)?)\s*(billion|million|bn|b|m|mm)?"
            for label_pat in label_patterns:
                rx = re.compile(rf"{label_pat}[^\d$()\-]{{0,{max_gap}}}(?:\$?\s*){amount_pat}", re.I)
                for m in rx.finditer(txt):
                    start = max(0, int(m.start()) - 280)
                    end = min(len(txt), int(m.end()) + 320)
                    window = txt[start:end]
                    unit_window = txt[max(0, int(m.start()) - 2200) : min(len(txt), int(m.end()) + 420)]
                    if extra_guard is not None and not extra_guard(window):
                        continue
                    val = _qa_amount_from_token(
                        m.group(1),
                        m.group(2),
                        unit_window,
                        default_small_unit=default_small_unit,
                    )
                    if val is None:
                        continue
                    return _qa_support_result(val, support_kind, doc_rec, window, confidence=confidence)
            return None

        def _extract_sum_from_doc(
            doc_rec: Dict[str, Any],
            label_pattern_groups: Sequence[Sequence[str]],
            *,
            allow_missing_last: bool = False,
        ) -> Optional[Dict[str, Any]]:
            parts: List[Dict[str, Any]] = []
            for idx, label_group in enumerate(label_pattern_groups):
                res = _extract_labeled_amount_from_doc(doc_rec, label_group, max_gap=72, default_small_unit="million")
                if res is None:
                    if allow_missing_last and idx == len(label_pattern_groups) - 1:
                        continue
                    return None
                parts.append(res)
            if not parts:
                return None
            total_val = sum(float(x.get("value") or 0.0) for x in parts)
            joined_excerpt = " | ".join(str(x.get("excerpt") or "") for x in parts if str(x.get("excerpt") or "").strip())
            return _qa_support_result(total_val, "explicit", doc_rec, joined_excerpt, confidence=1.0)

        def _pick_metric_support(
            extractor: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
        ) -> Dict[str, Any]:
            low_confidence_result: Optional[Dict[str, Any]] = None
            for doc_rec in source_bundle:
                result = extractor(doc_rec)
                if not isinstance(result, dict):
                    continue
                support_kind = str(result.get("support_kind") or "").strip().lower()
                if support_kind == "explicit":
                    return result
                if support_kind == "low_confidence" and low_confidence_result is None:
                    low_confidence_result = result
            if low_confidence_result is not None:
                return low_confidence_result
            return _qa_support_result(None, "not_found", None, bundle_source_label, confidence=0.0)

        def _extract_revenue_support() -> Dict[str, Any]:
            return _pick_metric_support(
                lambda doc_rec: (
                    _extract_labeled_amount_from_doc(
                        doc_rec,
                        [
                            r"\brevenues?\s+for\s+the\s+quarter\s+were\b",
                            r"\brevenues?\s+were\b",
                            r"\brevenue\s+was\b",
                            r"\b(?:total\s+)?revenue\b",
                        ],
                        max_gap=40,
                    )
                )
            )

        def _extract_adj_ebitda_support() -> Dict[str, Any]:
            def _local(doc_rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                patterns = [
                    r"\badjusted\s+ebitda\s+of\b",
                    r"\badjusted\s+ebitda\s+was\b",
                    r"\badjusted\s+ebitda\b(?!\s+margin)",
                ]
                if is_pbi_profile:
                    return _extract_labeled_amount_from_doc(
                        doc_rec,
                        patterns,
                        max_gap=56,
                        extra_guard=lambda window: bool(
                            re.search(
                                r"\b(reconciliation|adjusted\s+net\s+income|interest,\s*net|depreciation\s+and\s+amortization)\b",
                                window,
                                re.I,
                            )
                        ),
                    )
                return _extract_labeled_amount_from_doc(
                    doc_rec,
                    patterns,
                    max_gap=56,
                )

            return _pick_metric_support(_local)

        def _extract_net_income_support() -> Dict[str, Any]:
            def _local(doc_rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                explicit = _extract_labeled_amount_from_doc(
                    doc_rec,
                    [
                        r"\bnet\s+income\s+attributable\s+to[^$]{0,60}?\sof\b",
                        r"\bgaap\s+net\s+income\b",
                        r"\bnet\s+income\s*\(loss\)\s*-\s*gaap\b",
                        r"\bnet\s+income\s*\(loss\)\b",
                        r"\bnet\s+income\b",
                    ],
                    max_gap=56,
                )
                if explicit is None:
                    explicit = _extract_labeled_amount_from_doc(
                        doc_rec,
                        [r"\bnet\s+loss\b"],
                        max_gap=56,
                    )
                return explicit

            return _pick_metric_support(_local)

        def _extract_ebitda_support() -> Dict[str, Any]:
            if is_pbi_profile:
                return _qa_support_result(None, "not_found", None, bundle_source_label, confidence=0.0)
            return _pick_metric_support(
                lambda doc_rec: _extract_labeled_amount_from_doc(
                    doc_rec,
                    [r"(?<!adjusted\s)\bebitda\b"],
                    max_gap=48,
                )
            )

        def _extract_fcf_support() -> Dict[str, Any]:
            if is_pbi_profile:
                pbi_reported_fcf = _pbi_reported_fcf_payload_for_qd(q0d)
                if isinstance(pbi_reported_fcf, dict) and pbi_reported_fcf.get("current") is not None:
                    return _qa_support_result(
                        float(pbi_reported_fcf.get("current") or 0.0),
                        "explicit",
                        None,
                        f"Reported quarterly free cash flow {_money_m(float(pbi_reported_fcf.get('current') or 0.0))}",
                        confidence=1.0,
                        source_doc_override=str(pbi_reported_fcf.get("doc") or ""),
                        source_type_override="earnings_release",
                        basis_kind="company_defined_free_cash_flow",
                        basis_note="selected quarter text states company-defined free cash flow",
                        same_basis=False,
                    )

            def _local(doc_rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                txt = str(doc_rec.get("text") or "")
                if not txt or "free cash flow" not in txt.lower():
                    return None

                def _mark_company_defined_adjusted_fcf(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
                    if not isinstance(result, dict):
                        return result
                    blob = f"{result.get('excerpt') or ''} {txt}"
                    if not re.search(r"\badjusted\s+(?:free\s+cash\s+flow|fcf)\b", blob, re.I):
                        return result
                    out = dict(result)
                    out["basis_kind"] = "company_defined_free_cash_flow"
                    out["basis_note"] = "selected quarter text states company-defined free cash flow"
                    out["same_basis"] = False
                    return out

                if re.search(r"\b(guidance|outlook|full[- ]year|fy\s*20\d{2}|low\s+high|midpoint|target)\b", txt, re.I):
                    explicit = _extract_labeled_amount_from_doc(
                        doc_rec,
                        [r"\bfree\s+cash\s+flow\s+was\b"],
                        max_gap=40,
                    )
                    if explicit is not None:
                        return _mark_company_defined_adjusted_fcf(explicit)
                return _mark_company_defined_adjusted_fcf(
                    _extract_labeled_amount_from_doc(
                        doc_rec,
                        [
                            r"\bfree\s+cash\s+flow\s+was\b",
                            r"\bfree\s+cash\s+flow\b",
                        ],
                        max_gap=44,
                        extra_guard=lambda window: not bool(
                            re.search(r"\b(guidance|outlook|full[- ]year|fy\s*20\d{2}|low\s+high|midpoint|target)\b", window, re.I)
                        ),
                    )
                )

            return _pick_metric_support(_local)

        def _extract_cash_support() -> Dict[str, Any]:
            def _local(doc_rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                explicit = _extract_labeled_amount_from_doc(
                    doc_rec,
                    [r"\bcash\s+and\s+cash\s+equivalents\s+at\s+end\s+of\s+period\b"],
                    max_gap=20,
                    default_small_unit="million",
                )
                if explicit is not None:
                    return explicit
                explicit = _extract_labeled_amount_from_doc(
                    doc_rec,
                    [r"\bcash\s+and\s+cash\s+equivalents\b"],
                    max_gap=20,
                    default_small_unit="million",
                    extra_guard=lambda window: bool(
                        re.search(r"\b(balance\s+sheets?|current\s+assets|assets\s+december)\b", window, re.I)
                    )
                    and not bool(
                        re.search(
                            r"\b(available\s+under\s+a\s+committed\s+revolving\s+credit\s+facility|available\s+under\s+.*credit\s+facility)\b",
                            window,
                            re.I,
                        )
                    ),
                )
                if explicit is not None:
                    return explicit
                return _extract_labeled_amount_from_doc(
                    doc_rec,
                    [r"\btotal\s+cash\s+and\s+cash\s+equivalents,\s+and\s+restricted\s+cash\b"],
                    max_gap=40,
                    default_small_unit="million",
                    support_kind="low_confidence",
                    confidence=0.45,
                )

            return _pick_metric_support(_local)

        def _extract_debt_core_support() -> Dict[str, Any]:
            def _local(doc_rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                explicit = _extract_sum_from_doc(
                    doc_rec,
                    [
                        [
                            r"\bcurrent\s+portion\s+of\s+long[- ]term\s+debt\b",
                            r"\bcurrent\s+maturities\s+of\s+long[- ]term\s+debt\b",
                        ],
                        [r"(?<!current\sportion\sof\s)(?<!current\smaturities\sof\s)\blong[- ]term\s+debt\b"],
                    ],
                )
                if explicit is not None:
                    return explicit
                return _extract_labeled_amount_from_doc(
                    doc_rec,
                    [r"\blong[- ]term\s+debt\b"],
                    max_gap=40,
                    default_small_unit="million",
                    support_kind="low_confidence",
                    confidence=0.4,
                )

            return _pick_metric_support(_local)

        def _extract_total_debt_support() -> Dict[str, Any]:
            def _local(doc_rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                txt = str(doc_rec.get("text") or "")
                explicit = _extract_labeled_amount_from_doc(
                    doc_rec,
                    [r"\btotal\s+debt\s+outstanding(?:\s+at[^$]{0,40}?\bwas)?\b"],
                    max_gap=48,
                )
                if explicit is not None:
                    if is_gpre_profile and re.search(
                        r"\b(total\s+debt\s+outstanding)\b[\s\S]{0,200}?\b(including|includes?)\b[\s\S]{0,200}?\b(revolvers?|short[- ]term\s+borrowings?|working\s+capital)\b",
                        txt,
                        re.I,
                    ):
                        explicit = dict(explicit)
                        explicit["basis_kind"] = "total_debt_outstanding_including_short_term"
                        explicit["basis_note"] = "release total debt outstanding includes revolver/other short-term borrowings"
                        explicit["same_basis"] = False
                    return explicit
                return _extract_sum_from_doc(
                    doc_rec,
                    [
                        [
                            r"\bcurrent\s+portion\s+of\s+long[- ]term\s+debt\b",
                            r"\bcurrent\s+maturities\s+of\s+long[- ]term\s+debt\b",
                        ],
                        [r"(?<!current\sportion\sof\s)(?<!current\smaturities\sof\s)\blong[- ]term\s+debt\b"],
                        [r"\bshort[- ]term\s+notes\s+payable\s+and\s+other\s+borrowings\b"],
                    ],
                    allow_missing_last=True,
                )

            return _pick_metric_support(_local)

        rev_model = pd.to_numeric(hq0.get("revenue"), errors="coerce")
        cash_model = pd.to_numeric(hq0.get("cash"), errors="coerce")
        debt_model = pd.to_numeric(hq0.get("debt_core"), errors="coerce")
        total_debt_model = pd.to_numeric(hq0.get("total_debt"), errors="coerce")
        ebitda_model = pd.to_numeric(ebitda_map.get(q0), errors="coerce") if ebitda_map else pd.NA
        net_income_model = pd.to_numeric(net_income_map.get(q0), errors="coerce") if net_income_map else pd.NA
        cfo_q = pd.to_numeric(hq0.get("cfo"), errors="coerce")
        cap_q = pd.to_numeric(hq0.get("capex"), errors="coerce")
        fcf_model = (float(cfo_q) - float(cap_q)) if pd.notna(cfo_q) and pd.notna(cap_q) else None
        adj_ebitda_model = None
        if adj_metrics is not None and not adj_metrics.empty and "quarter" in adj_metrics.columns:
            am = _adj_metrics_view()
            am = am[am["_quarter"].notna()]
            am = am[am["_quarter"].dt.to_period("Q") == q0.to_period("Q")]
            if not am.empty and "adj_ebitda" in am.columns:
                adj_ebitda_model = pd.to_numeric(am.iloc[-1].get("adj_ebitda"), errors="coerce")

        revenue_support = _extract_revenue_support()
        ebitda_support = _extract_ebitda_support()
        net_income_support = _extract_net_income_support()
        adj_ebitda_support = _extract_adj_ebitda_support()
        fcf_support = _extract_fcf_support()
        cash_support = _extract_cash_support()
        debt_core_support = _extract_debt_core_support()
        total_debt_support = _extract_total_debt_support()
        if is_anf_profile:
            anf_schedule_doc = _anf_financial_schedule_support_doc_for_quarter(
                q0d,
                adj_metrics=adj_metrics if isinstance(adj_metrics, pd.DataFrame) else pd.DataFrame(),
                non_gaap_files=non_gaap_files if isinstance(non_gaap_files, pd.DataFrame) else pd.DataFrame(),
                slides_segments=slides_segments if isinstance(slides_segments, pd.DataFrame) else pd.DataFrame(),
            )
            if not anf_schedule_doc:
                anf_schedule_doc = "ANF earnings financial schedule"
            if pd.notna(rev_model):
                revenue_support = _qa_support_result(float(rev_model), "explicit", None, "ANF earnings financial schedule net sales row", confidence=1.0, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
            if pd.notna(ebitda_model):
                ebitda_support = _qa_support_result(float(ebitda_model), "explicit", None, "ANF earnings financial schedule EBITDA reconciliation row", confidence=1.0, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
            if adj_ebitda_model is not None and pd.notna(adj_ebitda_model):
                adj_ebitda_support = _qa_support_result(float(adj_ebitda_model), "explicit", None, "ANF earnings financial schedule adjusted EBITDA reconciliation row", confidence=1.0, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
            if fcf_model is not None:
                fcf_support = _qa_support_result(float(fcf_model), "explicit", None, "ANF earnings financial schedule CFO less capex from cash-flow schedule", confidence=1.0, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
            if pd.notna(cash_model):
                cash_support = _qa_support_result(float(cash_model), "explicit", None, "ANF earnings financial schedule balance-sheet cash and equivalents row", confidence=1.0, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
            if pd.notna(debt_model) and abs(float(debt_model)) < 1.0:
                debt_core_support = _qa_support_result(0.0, "explicit", None, "ANF balance sheet shows operating lease liabilities but no conventional core debt row", confidence=0.95, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
                if pd.isna(total_debt_model):
                    total_debt_support = _qa_support_result(0.0, "explicit", None, "ANF balance sheet shows no conventional total debt; operating lease liabilities are kept outside core debt", confidence=0.95, source_doc_override=anf_schedule_doc, source_type_override="earnings_presentation")
        net_debt_support = _qa_support_result(None, "not_found", None, bundle_source_label, confidence=0.0)
        if (
            str(debt_core_support.get("support_kind") or "") == "explicit"
            and str(cash_support.get("support_kind") or "") == "explicit"
            and debt_core_support.get("value") is not None
            and cash_support.get("value") is not None
        ):
            net_debt_support = _qa_support_result(
                float(debt_core_support.get("value") or 0.0) - float(cash_support.get("value") or 0.0),
                "explicit",
                None,
                f"Debt core {_money_m(float(debt_core_support.get('value') or 0.0))} less cash {_money_m(float(cash_support.get('value') or 0.0))}",
                confidence=min(float(debt_core_support.get("confidence") or 1.0), float(cash_support.get("confidence") or 1.0)),
                source_doc_override=" | ".join(
                    [x for x in [str(debt_core_support.get("source_doc") or "").strip(), str(cash_support.get("source_doc") or "").strip()] if x]
                ),
                source_type_override="earnings_release",
            )

        def _emit_metric_support_issue(
            metric_name: str,
            model_val: Optional[float],
            support_result: Dict[str, Any],
        ) -> None:
            def _finalize(issue_family: str, *, recommended_action: str = "") -> None:
                qa_rows[-1]["issue_family"] = issue_family
                qa_rows[-1]["raw_metric"] = metric_name
                if recommended_action:
                    qa_rows[-1]["recommended_action"] = recommended_action

            if model_val is None:
                _add(
                    "warn",
                    metric_name,
                    f"{metric_name}: workbook value missing for {q0d}",
                    str(support_result.get("source_doc") or bundle_source_label),
                )
                _finalize("quarter_text_no_explicit_support", recommended_action="review source coverage")
                return
            support_kind = str(support_result.get("support_kind") or "not_found").strip().lower()
            src_display = str(support_result.get("source_doc") or bundle_source_label)
            if support_kind == "not_found":
                _add(
                    writer_qa_latest_quarter_support_gap_severity(metric_name),
                    metric_name,
                    f"{metric_name}: workbook={_money_m(model_val)}; no explicit quarter-level statement found in selected release/presentation corpus",
                    src_display,
                )
                _finalize("quarter_text_no_explicit_support", recommended_action="review source coverage")
                return
            if support_kind == "low_confidence":
                _add(
                    "warn",
                    metric_name,
                    f"{metric_name}: extracted value came from weak heuristic match only",
                    src_display,
                )
                _finalize("quarter_text_low_confidence_support", recommended_action="watch only")
                return
            parsed_val = support_result.get("value")
            if parsed_val is None:
                _add(
                    writer_qa_latest_quarter_support_gap_severity(metric_name),
                    metric_name,
                    f"{metric_name}: workbook={_money_m(model_val)}; no explicit quarter-level statement found in selected release/presentation corpus",
                    src_display,
                )
                _finalize("quarter_text_no_explicit_support", recommended_action="review source coverage")
                return
            same_basis = support_result.get("same_basis")
            basis_kind = str(support_result.get("basis_kind") or "").strip().lower()
            if same_basis is False:
                if metric_name == "FCF (Q)" and basis_kind == "company_defined_free_cash_flow":
                    message = (
                        f"Workbook FCF (Q) is CFO-capex based at {_money_m(model_val)}; "
                        f"selected quarter text states company-defined free cash flow of {_money_m(float(parsed_val))}. "
                        "Likely definition mismatch rather than same-basis numeric conflict."
                    )
                elif metric_name == "Total debt (Q)" and basis_kind == "total_debt_outstanding_including_short_term":
                    message = (
                        f"Workbook Total debt (Q) is {_money_m(model_val)} on the modeled debt-profile basis; "
                        f"release total debt outstanding is {_money_m(float(parsed_val))} and includes revolver/other short-term borrowings. "
                        "Likely basis/presentation mismatch rather than same-basis numeric conflict."
                    )
                else:
                    basis_note = str(support_result.get("basis_note") or "").strip()
                    detail = f"{basis_note} " if basis_note else ""
                    message = (
                        f"{metric_name}: workbook={_money_m(model_val)}, extracted quarter text={_money_m(float(parsed_val))}; "
                        f"{detail}likely definition mismatch rather than same-basis numeric conflict"
                    ).strip()
                _add("warn", metric_name, message, src_display)
                _finalize("quarter_text_definition_mismatch", recommended_action="review metric definition")
                return
            diff_abs = abs(float(model_val) - float(parsed_val))
            tol_abs = 5_000_000.0
            tol_rel = 0.02
            rel = diff_abs / max(1.0, abs(float(model_val)))
            if diff_abs <= tol_abs or rel <= tol_rel:
                _add(
                    "info",
                    metric_name,
                    f"{metric_name}: PASS workbook={_money_m(model_val)} vs extracted quarter text={_money_m(float(parsed_val))}",
                    src_display,
                )
                _finalize("quarter_text_match")
                return
            sev = "fail" if diff_abs >= 25_000_000.0 and rel >= 0.10 else "warn"
            _add(
                sev,
                metric_name,
                (
                    f"{metric_name}: workbook={_money_m(model_val)}, extracted quarter text={_money_m(float(parsed_val))}; "
                    "likely conflicting extraction or source mismatch"
                ),
                src_display,
            )
            _finalize("quarter_text_numeric_conflict", recommended_action="fix parser")

        _emit_metric_support_issue("Revenue (Q)", float(rev_model) if pd.notna(rev_model) else None, revenue_support)
        _emit_metric_support_issue("EBITDA (Q)", float(ebitda_model) if pd.notna(ebitda_model) else None, ebitda_support)
        _emit_metric_support_issue("Net income (Q)", float(net_income_model) if pd.notna(net_income_model) else None, net_income_support)
        _emit_metric_support_issue("Adj EBITDA (Q)", float(adj_ebitda_model) if pd.notna(adj_ebitda_model) else None, adj_ebitda_support)
        _emit_metric_support_issue("FCF (Q)", fcf_model, fcf_support)
        _emit_metric_support_issue("Cash (Q)", float(cash_model) if pd.notna(cash_model) else None, cash_support)
        total_debt_metric_model = float(total_debt_model) if pd.notna(total_debt_model) else None
        if (
            is_anf_profile
            and total_debt_metric_model is None
            and pd.notna(debt_model)
            and abs(float(debt_model)) < 1.0
            and str(total_debt_support.get("support_kind") or "").strip().lower() == "explicit"
        ):
            total_debt_metric_model = 0.0
        _emit_metric_support_issue("Total debt (Q)", total_debt_metric_model, total_debt_support)
        _emit_metric_support_issue("Debt core (Q)", float(debt_model) if pd.notna(debt_model) else None, debt_core_support)
        _emit_metric_support_issue(
            "Net debt (Q)",
            (float(debt_model) - float(cash_model)) if pd.notna(debt_model) and pd.notna(cash_model) else None,
            net_debt_support,
        )

        resolved_cap_return_latest = dict((qa_precompute_bundle.get("capital_return_resolved") or {}).get(q0) or {})
        buyback_shares_latest = resolved_cap_return_latest.get("buyback_shares_q")
        buyback_cash_latest = resolved_cap_return_latest.get("buyback_cash_q")
        buyback_avg_price_latest = resolved_cap_return_latest.get("buyback_avg_price")
        buyback_qa_summary = str(resolved_cap_return_latest.get("buyback_qa_summary") or "").strip()
        buyback_note_latest = str(resolved_cap_return_latest.get("buyback_note_source") or _qmap_lookup(buyback_doc_note_map, q0) or "").strip()
        if buyback_shares_latest is not None or buyback_cash_latest is not None or buyback_note_latest:
            src_lbl = "sec_cache repurchase table/text"
            _add(
                "info",
                "QA_Buybacks",
                buyback_qa_summary or buyback_note_latest[:180],
                src_lbl,
            )
        elif re.search(r"\brepurchas\w*|buyback\b", sec_text_l, re.I):
            _add("warn", "QA_Buybacks", f"Buyback wording found for {q0d}, but explicit execution values were not parsed.", "sec_cache docs")
        else:
            _add("info", "QA_Buybacks", f"No explicit buyback execution disclosed for {q0d}.", "sec_cache docs")

        auth_text_available = bool(
            re.search(
                r"\b(remaining capacity|remaining buyback capacity|may yet be purchased|repurchase authorization|"
                r"authorization remain(?:ed|s)?|share repurchase authorization)\b",
                sec_text_l,
                re.I,
            )
        )
        if not auth_text_available:
            try:
                buyback_auth_recent_rows = list(_submission_recent_rows(max_files=8))
            except Exception:
                buyback_auth_recent_rows = []
            if not buyback_auth_recent_rows:
                _add(
                    "info",
                    "QA_BuybackAuthorization",
                    (
                        f"Buyback authorization health for {q0d}: submissions cache unavailable; "
                        "remaining-capacity scan skipped."
                    ),
                    "sec_cache submissions",
                )

        dividend_cash_latest = _qmap_lookup(dividend_map, q0)
        if dividend_cash_latest is not None:
            _add(
                "info",
                "QA_Dividends",
                f"Explicit common dividend cash support found for {q0d}: {_money_m(dividend_cash_latest)}.",
                "history/sec_cache common-dividend support",
            )
        elif re.search(r"\bdividends and distributions\b|\bpayments of dividends\b", sec_text_l, re.I):
            _add(
                "info",
                "QA_Dividends",
                f"Generic dividends/distributions disclosure found for {q0d}, but it was not mapped to common dividend logic.",
                "sec_cache docs",
            )
        else:
            _add("info", "QA_Dividends", f"No explicit common dividend disclosure detected for {q0d}.", "sec_cache docs")

        valuation_audit = dict(qa_precompute_bundle.get("valuation_audit") or {})
        latest_audit = dict(valuation_audit.get(q0) or {})
        suppress_metric_map = {
            "net_leverage": "Net leverage",
            "net_leverage_adj": "Net leverage (Adj)",
            "interest_coverage_pnl": "Interest coverage (P&L TTM)",
            "cash_interest_coverage": "Cash interest coverage (TTM)",
        }
        for audit_key, label in suppress_metric_map.items():
            audit_row = dict(latest_audit.get(audit_key) or {})
            suppress_reason = str(audit_row.get("suppress_reason") or "").strip()
            if not suppress_reason:
                continue
            sev = "warn" if "denominator <= 0" in suppress_reason.lower() else "info"
            _add(sev, "QA_Valuation", f"{label} suppressed: {suppress_reason}", "valuation_audit")

        # Guidance completeness: Low/High ranges expected for key metrics in EX-99 window.
        g_checks = {
            "Revenue": r"revenue[^\n\r]{0,80}([0-9]{2,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)[^\n\r]{0,40}([0-9]{2,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)",
            "Adj EBIT": r"adjusted\s+ebit[^\n\r]{0,80}([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)[^\n\r]{0,40}([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)",
            "Adj EPS": r"adjusted\s+(?:diluted\s+)?(?:earnings\s+per\s+share|eps)[^\n\r]{0,80}([0-9]+(?:\.[0-9]+)?)[^\n\r]{0,40}([0-9]+(?:\.[0-9]+)?)",
            "FCF": r"(?:free\s+cash\s+flow|\bfcf\b)[^\n\r]{0,80}([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)[^\n\r]{0,40}([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?)",
        }
        g_mentions = {
            "Revenue": r"\b(revenue|sales)\b",
            "Adj EBIT": r"\badjusted\s+ebit\b",
            "Adj EPS": r"\badjusted\s+(?:diluted\s+)?(?:earnings\s+per\s+share|eps)\b",
            "FCF": r"\b(free\s+cash\s+flow|fcf)\b",
        }
        snap_items_q0: List[Dict[str, Any]] = []
        try:
            gstore = ui_state.get("guidance_snapshot_by_q", {}) if isinstance(ui_state, dict) else {}
            if isinstance(gstore, dict):
                snap_items_q0 = list(gstore.get(pd.Timestamp(q0).date(), []))
                if not snap_items_q0:
                    snap_items_q0 = list(gstore.get(pd.Timestamp(q0), []))
        except Exception:
            snap_items_q0 = []

        def _guidance_metric_key(metric_name: str) -> str:
            m = str(metric_name or "").strip().lower()
            if not m:
                return ""
            if "revenue" in m or "sales" in m:
                return "revenue"
            if "ebitda" in m:
                return "adj ebitda"
            if "ebit" in m:
                return "adj ebit"
            if "eps" in m or "earnings per share" in m:
                return "adj eps"
            if "free cash flow" in m or re.search(r"\bfcf\b", m):
                return "fcf"
            if "capex" in m or "capital expenditure" in m:
                return "capex"
            return m

        def _snapshot_has_metric_range(metric_name: str) -> bool:
            metric_k = _guidance_metric_key(metric_name)
            for it in snap_items_q0:
                mname = str(it.get("metric_canon") or it.get("metric") or "")
                if _guidance_metric_key(mname) != metric_k:
                    continue
                lo = pd.to_numeric(it.get("low") if "low" in it else it.get("value_low"), errors="coerce")
                hi = pd.to_numeric(it.get("high") if "high" in it else it.get("value_high"), errors="coerce")
                if pd.notna(lo) and pd.notna(hi):
                    return True
            return False

        def _snapshot_has_metric(metric_name: str) -> bool:
            metric_k = _guidance_metric_key(metric_name)
            for it in snap_items_q0:
                mname = str(it.get("metric_canon") or it.get("metric") or "")
                if _guidance_metric_key(mname) == metric_k:
                    return True
            return False

        def _slides_has_metric(metric_name: str, *, require_range: bool = False) -> bool:
            return _slides_guidance_has_explicit_metric(
                slides_guidance if isinstance(slides_guidance, pd.DataFrame) else pd.DataFrame(),
                q0d,
                metric_name,
                require_range=require_range,
            )

        has_guidance_context = bool(snap_items_q0)
        if not has_guidance_context:
            has_guidance_context = any(
                _slides_has_metric(metric_name)
                for metric_name in ("Revenue", "Adj EBIT", "Adj EPS", "FCF", "Capex")
            )
        if not has_guidance_context:
            has_guidance_context = bool(
                re.search(
                    r"\b(provides?\s+the\s+following\s+guidance|full[- ]year\s+guidance|fiscal\s+20\d{2}\s+guidance|"
                    r"guidance\s+for\s+(?:fiscal|fy)\s+20\d{2}|outlook[^.\n]{0,120}(revenue|ebit|eps|cash\s+flow|fcf|capex))\b",
                    sec_text_l,
                    re.I,
                )
            )
        if not has_guidance_context:
            _add(
                "info",
                "QA_Guidance",
                f"No explicit numeric guidance disclosure detected for {q0d}; range completeness checks skipped.",
                "sec_cache docs/guidance_snapshot",
            )
        else:
            for mk, pat in g_checks.items():
                mention_pat = g_mentions.get(mk, "")
                mention_guidance_ctx = (
                    rf"{mention_pat}[^\n\r]{{0,120}}(guidance|outlook|target|targets|range|between|to\s+\$?[0-9]|low|high)"
                    if mention_pat
                    else ""
                )
                mentioned = bool(_snapshot_has_metric(mk))
                if not mentioned and mention_guidance_ctx:
                    mentioned = bool(re.search(mention_guidance_ctx, sec_text_l, re.I))
                if not mentioned:
                    mentioned = _slides_has_metric(mk)
                ok_text = bool(re.search(pat, sec_text_l, re.I))
                ok_snap = _snapshot_has_metric_range(mk)
                ok_slides = _slides_has_metric(mk, require_range=True)
                ok = bool(ok_text or ok_snap or ok_slides)
                src_lbl = "sec_cache docs" if ok_text else ("guidance_snapshot" if ok_snap else ("Slides_Guidance" if ok_slides else "sec_cache docs"))
                if not mentioned:
                    _add("info", "QA_Guidance", f"{mk} guidance not disclosed for {q0d}", "sec_cache docs/guidance_snapshot")
                else:
                    _add("info" if ok else "warn", "QA_Guidance", f"{mk} range {'PASS' if ok else 'MISSING'} for {q0d}", src_lbl)

        # Leverage text-vs-calc consistency.
        if leverage_df is not None and not leverage_df.empty:
            lv = leverage_df.copy()
            lv["quarter"] = pd.to_datetime(lv["quarter"], errors="coerce")
            lv0 = lv[lv["quarter"].dt.to_period("Q") == q0.to_period("Q")]
            if not lv0.empty:
                rr = lv0.iloc[-1]
                lev_txt = pd.to_numeric(rr.get("corporate_net_leverage_text"), errors="coerce")
                lev_calc = pd.to_numeric(rr.get("corporate_net_leverage_calc"), errors="coerce")
                if pd.notna(lev_txt) and pd.notna(lev_calc):
                    diff = abs(float(lev_txt) - float(lev_calc))
                    sev = "info" if diff <= 0.15 else "warn"
                    _add(sev, "QA_Leverage", f"Adjusted net leverage text {float(lev_txt):.2f}x vs calc {float(lev_calc):.2f}x (diff {diff:.2f}x)", "sec_cache/slides")
                elif pd.notna(lev_calc):
                    _add("info", "QA_Leverage", f"Adjusted net leverage uses calc {float(lev_calc):.2f}x (no text ratio found)", "computed")
                else:
                    _add("warn", "QA_Leverage", "Adjusted net leverage unavailable (calc/text missing)", "computed")

        # Debt QA: compare like-with-like and keep definition/presentation mismatches explicit.
        try:
            debt_core_latest = float(debt_model) if pd.notna(debt_model) else None
            tr_sum_unique = None
            table_total_debt_latest = None
            if debt_tranches_latest is not None and not debt_tranches_latest.empty:
                dtl = debt_tranches_latest.copy()
                if "quarter" in dtl.columns:
                    dtl["quarter"] = pd.to_datetime(dtl["quarter"], errors="coerce")
                    dtl = dtl[dtl["quarter"].notna()]
                    dtl = dtl[dtl["quarter"].dt.to_period("Q") == q0.to_period("Q")]
                if not dtl.empty:
                    amt_col = _resolve_col(dtl, ["amount_principal", "amount", "principal", "principal_amount"])
                    if amt_col:
                        dedup_cols = [c for c in ["tranche_key", "tranche_name", amt_col, "maturity_year", "coupon_pct", "spread_pct"] if c in dtl.columns]
                        dtl2 = dtl.drop_duplicates(subset=dedup_cols) if dedup_cols else dtl
                        tr_sum_unique = float(pd.to_numeric(dtl2[amt_col], errors="coerce").dropna().sum())
                    table_total_col = _resolve_col(dtl, ["table_total_debt", "table_total_long_term_debt", "table_total"])
                    if table_total_col:
                        ttd_series = pd.to_numeric(dtl[table_total_col], errors="coerce").dropna()
                        if not ttd_series.empty:
                            table_total_debt_latest = float(ttd_series.max())
            debt_basis_latest = debt_core_latest
            debt_basis_label = "debt_core"
            if (tr_sum_unique is None or abs(float(tr_sum_unique or 0.0)) < 1.0) and debt_buckets is not None and not debt_buckets.empty:
                db0 = debt_buckets.copy()
                asof_col_db0 = _resolve_col(db0, ["as_of", "quarter"])
                sum_col_db0 = _resolve_col(db0, ["Tranche_sum", "tranche_sum", "Total_bucketed", "total_bucketed"])
                if asof_col_db0 and sum_col_db0:
                    db0[asof_col_db0] = pd.to_datetime(db0[asof_col_db0], errors="coerce")
                    db0 = db0[db0[asof_col_db0].notna()]
                    db0 = db0[db0[asof_col_db0].dt.to_period("Q") == q0.to_period("Q")]
                    if not db0.empty:
                        db0_last = db0.sort_values(asof_col_db0).iloc[-1]
                        sum_num = pd.to_numeric(db0_last.get(sum_col_db0), errors="coerce")
                        if pd.notna(sum_num):
                            tr_sum_unique = float(sum_num)
                        src_db0 = str(db0_last.get(_resolve_col(db0, ["Source", "source"]) or "") or "").lower()
                        if "scheduled_repayments_fallback" in src_db0 and debt_profile is not None and not debt_profile.empty:
                            dpf0 = debt_profile.copy()
                            metric_col_dpf0 = _resolve_col(dpf0, ["metric"])
                            value_col_dpf0 = _resolve_col(dpf0, ["value"])
                            if metric_col_dpf0 and value_col_dpf0:
                                v_long = pd.to_numeric(
                                    dpf0.loc[dpf0[metric_col_dpf0] == "debt_long_term", value_col_dpf0],
                                    errors="coerce",
                                ).dropna()
                                if not v_long.empty:
                                    debt_basis_latest = float(v_long.iloc[-1])
                                    debt_basis_label = "debt_long_term"

            rev_drawn_latest = None
            if revolver_history is not None and not revolver_history.empty:
                rv = revolver_history.copy()
                q_col_rv = _resolve_col(rv, ["quarter", "quarter_end", "as_of"])
                drawn_col = _resolve_col(rv, ["revolver_drawn", "drawn", "drawn_amount"])
                if q_col_rv and drawn_col:
                    rv[q_col_rv] = pd.to_datetime(rv[q_col_rv], errors="coerce")
                    rv = rv[rv[q_col_rv].notna()]
                    rv = rv[rv[q_col_rv].dt.to_period("Q") == q0.to_period("Q")]
                    if not rv.empty:
                        rv_last = rv.sort_values(q_col_rv).iloc[-1]
                        rv_num = pd.to_numeric(rv_last.get(drawn_col), errors="coerce")
                        if pd.notna(rv_num):
                            rev_drawn_latest = float(rv_num)

            other_debt_items = 0.0
            if debt_recon is not None and not debt_recon.empty:
                dr = debt_recon.copy()
                q_col_dr = _resolve_col(dr, ["quarter", "as_of"])
                diff_col_dr = _resolve_col(dr, ["diff", "other_debt_items"])
                if q_col_dr and diff_col_dr:
                    dr[q_col_dr] = pd.to_datetime(dr[q_col_dr], errors="coerce")
                    dr = dr[dr[q_col_dr].notna()]
                    dr = dr[dr[q_col_dr].dt.to_period("Q") == q0.to_period("Q")]
                    if not dr.empty:
                        dr_last = dr.sort_values(q_col_dr).iloc[-1]
                        other_num = pd.to_numeric(dr_last.get(diff_col_dr), errors="coerce")
                        if pd.notna(other_num):
                            other_debt_items = float(other_num)
            def _infer_debt_scale_local(total_debt_in: Optional[float], table_total_in: Optional[float]) -> float:
                try:
                    td = float(total_debt_in) if total_debt_in is not None else None
                    ttd_val = float(table_total_in) if table_total_in is not None else None
                    if not td or not ttd_val:
                        return 1.0
                    ratio = td / ttd_val if ttd_val else 1.0
                    if 500 <= ratio <= 2000:
                        return 1000.0
                    if 500_000 <= ratio <= 2_000_000:
                        return 1_000_000.0
                except Exception:
                    pass
                return 1.0

            scale_applied = _infer_debt_scale_local(total_debt_model if pd.notna(total_debt_model) else None, table_total_debt_latest)
            table_total_debt_scaled = float(table_total_debt_latest) * float(scale_applied) if table_total_debt_latest is not None else None

            if tr_sum_unique is not None and table_total_debt_scaled is not None:
                diff_principal = float(table_total_debt_scaled) - float(tr_sum_unique)
                rel_principal = abs(diff_principal) / max(1.0, abs(float(tr_sum_unique)))
                if abs(diff_principal) > 5_000_000.0 and rel_principal > 0.02:
                    sev = "fail" if abs(diff_principal) >= 25_000_000.0 and rel_principal >= 0.10 else "warn"
                    _add(
                        sev,
                        "debt_tieout",
                        (
                            f"Tranche principal sum {_money_m(tr_sum_unique)} vs debt-table total {_money_m(table_total_debt_scaled)}; "
                            "principal table and tranche math do not align for the latest quarter."
                        ),
                        "Debt_Tranches_Latest",
                    )
                    qa_rows[-1]["issue_family"] = "principal_tranche_tieout"
                else:
                    _add(
                        "info",
                        "debt_tieout",
                        f"Tranche principal sum {_money_m(tr_sum_unique)} aligns with debt-table total {_money_m(table_total_debt_scaled)}.",
                        "Debt_Tranches_Latest",
                    )
                    qa_rows[-1]["issue_family"] = "principal_tranche_tieout"

            if debt_basis_latest is not None and table_total_debt_scaled is not None:
                diff_carry = float(table_total_debt_scaled) - float(debt_basis_latest)
                rel_carry = abs(diff_carry) / max(1.0, abs(float(debt_basis_latest)))
                if abs(diff_carry) > 5_000_000.0 and rel_carry > 0.02:
                    sev = "fail" if abs(diff_carry) >= 25_000_000.0 and rel_carry >= 0.10 else "warn"
                    msg = (
                        f"Debt-table total {_money_m(table_total_debt_scaled)} vs {debt_basis_label} {_money_m(debt_basis_latest)}; "
                        "likely presentation/carrying-value difference rather than tranche math."
                    )
                    if tr_sum_unique is not None and total_debt_model is not None and abs(float(tr_sum_unique) - float(total_debt_model)) <= max(25_000_000.0, abs(float(total_debt_model)) * 0.03):
                        msg = (
                            f"Tranche principal sum {_money_m(tr_sum_unique)} is close to XBRL total debt {_money_m(float(total_debt_model))}, "
                            f"but debt-table total {_money_m(table_total_debt_scaled)} is lower than {debt_basis_label} {_money_m(debt_basis_latest)}; "
                            "likely table presentation/carrying-value difference."
                        )
                    _add(sev, "debt_tieout", msg, "Debt_Tranches_Latest/History_Q")
                    qa_rows[-1]["issue_family"] = "carrying_debt_tieout"

            total_debt_text_val = total_debt_support.get("value")
            if (
                total_debt_text_val is not None
                and debt_basis_latest is not None
                and abs(float(total_debt_text_val) - float(debt_basis_latest)) > 5_000_000.0
            ):
                msg = (
                    f"Total debt from selected quarter source {_money_m(float(total_debt_text_val))} sits above {debt_basis_label} {_money_m(debt_basis_latest)}; "
                    "revolver/current portion/other debt appears to be handled outside the debt-core basis."
                )
                if rev_drawn_latest is not None and float(rev_drawn_latest) > 5_000_000.0:
                    msg = (
                        f"Selected quarter source shows total debt {_money_m(float(total_debt_text_val))} vs {debt_basis_label} {_money_m(debt_basis_latest)}; "
                        f"revolver draw {_money_m(rev_drawn_latest)} appears outside the tranche/debt-core basis."
                    )
                else:
                    other_debt_gap = max(0.0, float(total_debt_text_val) - float(debt_basis_latest))
                    if other_debt_gap > 5_000_000.0:
                        msg = (
                            f"Selected quarter source shows total debt {_money_m(float(total_debt_text_val))} vs {debt_basis_label} {_money_m(debt_basis_latest)}; "
                            f"short-term/revolver/other borrowings of about {_money_m(other_debt_gap)} appear outside the debt-core carrying basis."
                        )
                _add("warn", "debt_tieout", msg, str(total_debt_support.get("source_doc") or "selected quarter source"))
                qa_rows[-1]["issue_family"] = "revolver_and_other_debt_presence_check"

            if debt_buckets is not None and not debt_buckets.empty:
                db = debt_buckets.copy()
                asof_col = _resolve_col(db, ["as_of", "quarter"])
                cov_col = _resolve_col(db, ["Bucket_coverage_pct", "bucket_coverage_pct", "coverage_pct"])
                if asof_col and cov_col:
                    db[asof_col] = pd.to_datetime(db[asof_col], errors="coerce")
                    db = db[db[asof_col].notna()]
                    db = db[db[asof_col].dt.to_period("Q") == q0.to_period("Q")]
                    if db.empty:
                        db = debt_buckets.copy()
                    if not db.empty:
                        db_last = db.sort_values(asof_col).iloc[-1]
                        cov_val = pd.to_numeric(db_last.get(cov_col), errors="coerce")
                        if pd.notna(cov_val):
                            cov_f = float(cov_val)
                            sev = "info" if cov_f >= 0.90 else "warn"
                            _add(
                                sev,
                                "QA_DebtCoverage",
                                f"Debt_Buckets coverage {cov_f:.2%} (target >= 90%).",
                                "Debt_Buckets",
                            )
                        else:
                            _add("warn", "QA_DebtCoverage", "Debt_Buckets coverage metric missing.", "Debt_Buckets")
                else:
                    _add("warn", "QA_DebtCoverage", "Debt_Buckets missing as_of/coverage columns.", "Debt_Buckets")

            if debt_core_latest is not None:
                recon_total = None
                recon_source = ""
                if debt_tranches_latest is not None and not debt_tranches_latest.empty:
                    dtl_recon = debt_tranches_latest.copy()
                    if "quarter" in dtl_recon.columns:
                        dtl_recon["quarter"] = pd.to_datetime(dtl_recon["quarter"], errors="coerce")
                        dtl_recon = dtl_recon[dtl_recon["quarter"].notna()]
                        dtl_recon = dtl_recon[dtl_recon["quarter"].dt.to_period("Q") == q0.to_period("Q")]
                    if not dtl_recon.empty:
                        table_total_col = _resolve_col(dtl_recon, ["table_total_debt", "table_total_long_term_debt", "table_total"])
                        if table_total_col:
                            recon_series = pd.to_numeric(dtl_recon[table_total_col], errors="coerce").dropna()
                            if not recon_series.empty:
                                recon_total = float(recon_series.max())
                                recon_source = "Debt_Tranches_Latest"
                if recon_total is None and debt_buckets is not None and not debt_buckets.empty:
                    db_recon = debt_buckets.copy()
                    q_col_db = _resolve_col(db_recon, ["as_of", "quarter"])
                    total_col_db = _resolve_col(db_recon, ["Tranche_sum", "tranche_sum", "Total_bucketed", "total_bucketed"])
                    if q_col_db and total_col_db:
                        db_recon[q_col_db] = pd.to_datetime(db_recon[q_col_db], errors="coerce")
                        db_recon = db_recon[db_recon[q_col_db].notna()]
                        db_recon = db_recon[db_recon[q_col_db].dt.to_period("Q") == q0.to_period("Q")]
                        if not db_recon.empty:
                            recon_num = pd.to_numeric(db_recon.sort_values(q_col_db).iloc[-1].get(total_col_db), errors="coerce")
                            if pd.notna(recon_num):
                                recon_total = float(recon_num)
                                recon_source = "Debt_Buckets"
                if recon_total is None and debt_recon is not None and not debt_recon.empty:
                    dr = debt_recon.copy()
                    q_col_dr = _resolve_col(dr, ["quarter", "as_of"])
                    total_col_dr = _resolve_col(dr, ["total_debt", "debt_core", "table_total_debt"])
                    if q_col_dr and total_col_dr:
                        dr[q_col_dr] = pd.to_datetime(dr[q_col_dr], errors="coerce")
                        dr = dr[dr[q_col_dr].notna()]
                        dr = dr[dr[q_col_dr].dt.to_period("Q") == q0.to_period("Q")]
                        if not dr.empty:
                            dr_last = dr.sort_values(q_col_dr).iloc[-1]
                            recon_num = pd.to_numeric(dr_last.get(total_col_dr), errors="coerce")
                            if pd.notna(recon_num):
                                recon_total = float(recon_num)
                                recon_source = "Debt_Recon"
                if recon_total is not None:
                    diff_abs = abs(float(recon_total) - float(debt_core_latest))
                    diff_rel = diff_abs / max(1.0, abs(float(debt_core_latest)))
                    is_principal_schedule_source = str(recon_source or "").strip() in {
                        "Debt_Buckets",
                        "Debt_Tranches_Latest",
                    }
                    if is_principal_schedule_source and diff_rel <= 0.02:
                        sev = "info"
                        msg = (
                            f"Latest-quarter principal debt schedule {_money_m(recon_total)} vs carrying debt_core "
                            f"{_money_m(debt_core_latest)} (gap {_money_m(float(recon_total) - float(debt_core_latest))}, "
                            f"{diff_rel:.1%}). Difference is within 2% and is consistent with unamortized cost/swap "
                            "and carrying-value presentation, not a parser conflict."
                        )
                    else:
                        sev = "info" if diff_abs <= 5_000_000.0 else "warn"
                        msg = (
                            f"Latest-quarter debt coverage total {_money_m(recon_total)} vs History_Q debt_core "
                            f"{_money_m(debt_core_latest)}."
                        )
                    _add(
                        sev,
                        "Debt_Recon",
                        msg,
                        f"{recon_source}/History_Q" if recon_source else "History_Q",
                    )
                    qa_rows[-1]["issue_family"] = "debt_recon_coverage_check"
                else:
                    _add("warn", "Debt_Recon", "No latest-quarter debt reconciliation row found in selected source path.", "Debt_Recon")
                    qa_rows[-1]["issue_family"] = "debt_recon_coverage_check"
        except Exception as _debt_qa_ex:
            _add("warn", "debt_tieout", f"Debt QA failed for latest quarter: {_debt_qa_ex}", "pipeline")
            qa_rows[-1]["issue_family"] = "carrying_debt_tieout"

        # Source coverage checks.
        er_dir = _first_existing_material_dir(
            "earnings_release",
            "Earnings Release",
            "Earnings Releases",
            "press_release",
            "Press Release",
        )
        er_files = (
            sorted(
                [
                    p
                    for p in er_dir.glob("*")
                    if p.is_file() and _path_belongs_to_ticker(p, ticker, ticker_roots)
                ]
            )
            if er_dir is not None and er_dir.exists()
            else []
        )
        _add(
            "info" if er_files else "warn",
            "QA_Sources",
            f"earnings_release files: {len(er_files)}",
            str(er_dir) if er_dir is not None else "N/A",
        )
        seg_files: List[Path] = []
        for root in material_roots:
            seg_files.extend(
                [
                    p
                    for p in sorted(root.glob("*Segment*Q4*2025*.xlsx"))
                    if _path_belongs_to_ticker(p, ticker, ticker_roots)
                ]
            )
            for seg_dir_name in ("financial_statement", "segment_financials", "historical_segment"):
                seg_dir = root / seg_dir_name
                if not seg_dir.exists():
                    continue
                seg_files.extend(
                    [
                        p
                        for p in sorted(seg_dir.glob("*.xlsx"))
                        if _path_belongs_to_ticker(p, ticker, ticker_roots)
                    ]
                )
        _add(
            "info" if seg_files else "warn",
            "QA_Sources",
            f"segment workbook files: {len(seg_files)}",
            "ticker_root/*Segment*Q4*2025*.xlsx",
        )
        has_slides_seg = False
        seg_subset = pd.DataFrame()
        if slides_segments is not None and not slides_segments.empty and "quarter" in slides_segments.columns:
            ss = slides_segments.copy()
            ss["quarter"] = pd.to_datetime(ss["quarter"], errors="coerce")
            seg_subset = ss[ss["quarter"].dt.to_period("Q") == q0.to_period("Q")].copy()
            has_slides_seg = not seg_subset.empty
        _add("info" if has_slides_seg else "warn", "QA_Sources", f"slides segment rows for {q0d}: {'present' if has_slides_seg else 'missing'}", "Slides_Segments")

        # Segment workbook QA should use the same parsed quarterly segment dataset
        # that feeds Operating_Drivers, rather than fuzzy text-row scraping.
        if seg_files:
            try:
                seg_parsed = ew_parse_quarterly_segment_data_from_workbook(
                    seg_files[-1],
                    annual_segment_alias_patterns=annual_segment_alias_patterns,
                    company_segment_alias_patterns=company_profile.segment_alias_patterns,
                )
                seg_metrics = dict(seg_parsed.get("metrics") or {})
                seg_quarters = [
                    pd.Timestamp(qd).date()
                    for qd in list(seg_parsed.get("quarters") or [])
                    if isinstance(qd, (pd.Timestamp, date))
                ]
                if not seg_metrics or not seg_quarters:
                    _add("warn", "QA_Segment", "segment workbook comparison failed (parse error)", str(seg_files[-1]))
                else:
                    latest_seg_q = max(seg_quarters)
                    latest_seg_ts = pd.Timestamp(latest_seg_q)
                    parsed_any = False
                    missing_segments: List[str] = []
                    for seg_name in list(quarterly_segment_labels) or list((seg_metrics.get("Revenue") or {}).keys()):
                        has_latest_value = False
                        for metric_name in ("Revenue", "Adjusted EBIT"):
                            seg_series = dict((seg_metrics.get(metric_name) or {}).get(str(seg_name), {}) or {})
                            seg_val = pd.to_numeric(seg_series.get(latest_seg_ts), errors="coerce")
                            if pd.notna(seg_val):
                                has_latest_value = True
                                parsed_any = True
                                break
                        if not has_latest_value:
                            missing_segments.append(str(seg_name))
                    if parsed_any:
                        _add(
                            "info",
                            "QA_Segment",
                            f"Parsed latest-quarter segment workbook support available for {latest_seg_q.isoformat()}.",
                            str(seg_files[-1]),
                        )
                    else:
                        _add(
                            "warn",
                            "QA_Segment",
                            f"Segment workbook parsed but no latest-quarter segment values were found for {latest_seg_q.isoformat()}.",
                            str(seg_files[-1]),
                        )
                    if missing_segments:
                        _add(
                            "warn",
                            "QA_Segment",
                            "Latest-quarter segment workbook is missing expected segment labels: " + ", ".join(missing_segments),
                            str(seg_files[-1]),
                        )
            except Exception:
                _add("warn", "QA_Segment", "segment workbook comparison failed (parse error)", str(seg_files[-1]))

        return qa_rows


def run_latest_quarter_qa(
    deps: LatestQuarterQADeps,
) -> List[Dict[str, Any]]:
    return LatestQuarterQASupport(deps).run()
