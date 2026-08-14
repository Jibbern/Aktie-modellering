"""Quarter_Notes_UI source-rescue support helpers."""
from __future__ import annotations

import hashlib
import html
import json
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .cache_semantics import (
    GENERIC_SOURCE_NOTE_RESCUE_CACHE_VERSION,
    build_cache_identity,
    file_content_sha256,
)
from .longitudinal_memory.identity import (
    evidence_occurrence_identity,
    source_document_identity,
)


@dataclass(frozen=True)
class QuarterNotesUiSourceRescueDeps:
    _candidate_texts: Any
    _capital_allocation_split_summaries_local: Any
    _classify_pbi_metric_label: Any
    _ensure_terminal_period: Any
    _evidence_snippet_blob_local: Any
    _explicit_event_quarter_override_local: Any
    _extract_pbi_target_display: Any
    _fmt_note_share_count_local: Any
    _fmt_short_money_value_local: Any
    _gpre_quantified_note_summary_local: Any
    _gpre_structured_support_source_ok_local: Any
    _infer_doc_quarter_local: Any
    _is_pbi_clean_sentence: Any
    _is_preferred_narrative_source: Any
    _iter_quarter_scoped_material_texts_local: Any
    _iter_quarter_scoped_sec_cache_texts_local: Any
    _management_text_windows_local: Any
    _narrative_text_matches_current_company_local: Any
    _note_sector_pack_keys_local: Any
    _parse_buyback_money_local: Any
    _path_belongs_to_ticker: Any
    _pattern_match_windows_local: Any
    _pbi_contextual_note_summary_local: Any
    _pbi_detail_preserving_note_summary_local: Any
    _pbi_explicit_note_split_variants_local: Any
    _pbi_extra_note_labels_local: Any
    _pbi_guidance_self_contained_summary: Any
    _pbi_is_locked_capital_allocation_summary_local: Any
    _pbi_note_detail_score_local: Any
    _pbi_target_display_ok: Any
    _profile_sector_pack_keys_local: Any
    _promises_view: Any
    _quarter_end_for_month_local: Any
    _record_writer_elapsed: Any
    _resolve_col: Any
    _sec_cache_html_paths_local: Any
    _source_meta: Any
    cache_dir: Any
    cache_roots: Any
    company_profile: Any
    ctx_ref: Any
    data_root_from_sec_cache_path: Any
    df: Any
    glx_normalize_text: Any
    is_gpre_profile: Any
    is_pbi_profile: Any
    is_tabular_fragment: Any
    material_roots: Any
    metric_col: Any
    note_id_col: Any
    profile_ticker: Any
    q_col: Any
    qn_compact_snippet: Any
    quarter_notes: Any
    quarters: Any
    sev_score_col: Any
    shared_classify_statement_evidence_role: Any
    shared_renderable_note_drop_reason: Any
    ticker: Any
    ticker_cache_roots_from_base_dir: Any
    ticker_roots: Any


class QuarterNotesUiSourceRescueSupport:
    def __init__(self, deps: QuarterNotesUiSourceRescueDeps) -> None:
        self.deps = deps
        self._gpre_source_note_rescue_cache: Optional[List[Dict[str, Any]]] = None
        self._pbi_source_note_rescue_cache: Optional[List[Dict[str, Any]]] = None
        self._profile_milestone_source_rescue_cache: Optional[List[Dict[str, Any]]] = None

    def profile_milestone_source_rows(self) -> List[Dict[str, Any]]:
        """Resolve profile-declared reviewed milestone occurrences, fail closed on drift."""

        if self._profile_milestone_source_rescue_cache is not None:
            return [dict(row) for row in self._profile_milestone_source_rescue_cache]
        selectors = tuple(
            getattr(self.deps.company_profile, "promise_milestone_evidence_selectors", ()) or ()
        )
        selector_ids = tuple(str(getattr(row, "selector_id", "") or "") for row in selectors)
        if any(not value for value in selector_ids) or len(set(selector_ids)) != len(selector_ids):
            raise ValueError("Promise milestone evidence selectors must have unique stable identities.")

        allowed_quarters = set(self.deps.quarters or ())
        company_id = str(getattr(self.deps.company_profile, "ticker", "") or "").strip().upper()
        rows: List[Dict[str, Any]] = []
        for selector in sorted(selectors, key=lambda row: str(row.selector_id)):
            try:
                report_date = date.fromisoformat(str(selector.report_date))
                publication_date = date.fromisoformat(str(selector.publication_date))
            except (TypeError, ValueError):
                continue
            quarter = pd.Timestamp(report_date).to_period("Q").end_time.date()
            if quarter not in allowed_quarters:
                continue
            relative_path = Path(str(selector.relative_path or ""))
            if (
                not str(relative_path)
                or relative_path.is_absolute()
                or ".." in relative_path.parts
            ):
                continue
            expected_sha = str(selector.expected_sha256 or "").strip().lower()
            if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
                continue

            matched_paths: List[Path] = []
            for material_root in sorted(
                (Path(value) for value in self.deps.material_roots or ()),
                key=lambda value: str(value).casefold(),
            ):
                candidate = material_root / relative_path
                if not candidate.is_file():
                    continue
                try:
                    if hashlib.sha256(candidate.read_bytes()).hexdigest() != expected_sha:
                        continue
                except OSError:
                    continue
                matched_paths.append(candidate)
            if not matched_paths:
                continue
            selected_path = sorted(
                {path.resolve() for path in matched_paths},
                key=lambda value: str(value).casefold(),
            )[0]
            try:
                source_lines = selected_path.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError):
                continue
            locator_line = int(selector.locator_line or 0)
            if locator_line < 1 or locator_line > len(source_lines):
                continue
            excerpt = str(source_lines[locator_line - 1]).strip()
            required_phrases = tuple(str(value or "").strip() for value in selector.required_phrases)
            if not required_phrases or any(not phrase or phrase not in excerpt for phrase in required_phrases):
                continue

            source_document_id = source_document_identity(
                company_id=company_id,
                publisher_id=str(selector.publisher_id),
                document_type=str(selector.document_type),
                publication_date=publication_date.isoformat(),
                document_key=str(selector.document_key),
                revision=1,
            )
            source_occurrence_id = evidence_occurrence_identity(
                company_id=company_id,
                document_key=str(selector.document_key),
                document_revision=1,
                locator_kind=str(selector.locator_kind),
                locator_key=str(selector.locator_key),
                ordinal=1,
            )
            quarter_number = ((quarter.month - 1) // 3) + 1
            summary = str(selector.normalized_summary or "").strip()
            metric_label = str(selector.metric_label or "").strip()
            if not summary or not metric_label:
                continue
            rows.append(
                {
                    "quarter": quarter,
                    "bucket": "Programs / initiatives",
                    "text_full": summary,
                    "comment_full_text": excerpt,
                    "score": 100.0,
                    "candidate_type": "profile_reviewed_milestone_occurrence",
                    "metric_tag": metric_label,
                    "metric_canon": metric_label,
                    "_metric_display": metric_label,
                    "_pbi_compact_note": summary,
                    "_render_summary": summary,
                    "_render_summary_locked": True,
                    "_force_note_passthrough": True,
                    "_theme_scope_key": str(selector.event_id),
                    "note_id": source_occurrence_id,
                    "source_document_id": source_document_id,
                    "source_occurrence_id": source_occurrence_id,
                    "source_locator": f"{selector.locator_kind}:{locator_line}",
                    "period_norm": f"period:{company_id.lower()}:cy{quarter.year}-q{quarter_number}@1",
                    "disclosure_event_id": str(selector.event_id),
                    "event_role": str(selector.event_role),
                    "horizon": str(selector.horizon),
                    "evidence_role": "result_evidence",
                    "source": {
                        "source_type": "transcript",
                        "doc": str(selected_path),
                        "form": "transcript",
                        "source_document_id": source_document_id,
                        "source_occurrence_id": source_occurrence_id,
                        "source_locator": f"{selector.locator_kind}:{locator_line}",
                        "publication_date": publication_date.isoformat(),
                    },
                }
            )
        self._profile_milestone_source_rescue_cache = [dict(row) for row in rows]
        return [dict(row) for row in rows]

    def gpre_raw_note_rescue_rows(self) -> List[Dict[str, Any]]:
        _evidence_snippet_blob_local = self.deps._evidence_snippet_blob_local
        _gpre_quantified_note_summary_local = self.deps._gpre_quantified_note_summary_local
        _gpre_structured_support_source_ok_local = self.deps._gpre_structured_support_source_ok_local
        _resolve_col = self.deps._resolve_col
        glx_normalize_text = self.deps.glx_normalize_text
        is_gpre_profile = self.deps.is_gpre_profile
        is_tabular_fragment = self.deps.is_tabular_fragment
        quarter_notes = self.deps.quarter_notes
        quarters = self.deps.quarters
        shared_renderable_note_drop_reason = self.deps.shared_renderable_note_drop_reason
        if not is_gpre_profile or not isinstance(quarter_notes, pd.DataFrame) or quarter_notes.empty:
            return []
        def _local_summary_override(txt_local_in: Any, metric_hint_in: Any = "") -> str:
            return _gpre_quantified_note_summary_local(txt_local_in, metric_hint=metric_hint_in)
        fragment_drop_re = re.compile(
            r"^\s*\[(?:dropped|new|repeat)\]\s*|"
            r"\b(map|maps|permit list|county map|table of contents|legend|project map|site map)\b|"
            r"\b(latitude|longitude|parcel|township|range|section)\b",
            re.I,
        )
        context_poor_note_re = re.compile(
            r"(^\s*[a-z]?\s*(?:for the )?three months ended\b|"
            r"^\s*\d{1,2},\s*20\d{2}\s+compared to\b|"
            r"\bcompared to the same period\b|\bconsolidated results\b|"
            r"\bfor the (?:three|nine|twelve) months ended\b|"
            r"\binterest expense was\b.*\bcompared to\b)",
            re.I,
        )
        high_signal_note_re = re.compile(
            r"\b(utilization|risk management|45z|monetization|obion|york|central city|wood river|online and ramping|fully operational|"
            r"fcf|free cash flow|adjusted ebitda|ebitda|margin|net debt|revolver|availability|liquidity)\b",
            re.I,
        )
        rescue_rows: List[Dict[str, Any]] = []
        rescue_quarter_col = _resolve_col(quarter_notes, ["quarter", "created_quarter", "first_seen_quarter"])
        if not rescue_quarter_col:
            return []
        for rec in quarter_notes.to_dict("records"):
            q_ts = pd.to_datetime(rec.get(rescue_quarter_col), errors="coerce")
            if pd.isna(q_ts):
                continue
            q_raw = pd.Timestamp(q_ts).to_period("Q").end_time.date()
            if q_raw not in quarters:
                continue
            txt_rescue = glx_normalize_text(str(rec.get("note") or rec.get("claim") or rec.get("evidence_snippet") or ""))
            if not txt_rescue:
                continue
            metric_hint_rescue = str(rec.get("metric_ref") or rec.get("metric") or rec.get("metric_tag") or "")
            detail_rescue_blob = " | ".join(
                [
                    txt_rescue,
                    str(rec.get("evidence_snippet") or ""),
                    _evidence_snippet_blob_local(rec),
                ]
            ).strip()
            summary_override = _local_summary_override(detail_rescue_blob or txt_rescue, metric_hint_rescue)
            src_type_rescue = str(rec.get("doc_type") or rec.get("source_type") or "").lower()
            if fragment_drop_re.search(txt_rescue) or context_poor_note_re.search(txt_rescue) or (
                is_tabular_fragment(txt_rescue)
                and not summary_override
                and shared_renderable_note_drop_reason(txt_rescue, source_type=src_type_rescue)
            ):
                continue
            model_metric_source = src_type_rescue == "model_metric" or str(rec.get("doc") or "").strip().lower() == "history_q"
            capital_structure_signal = bool(re.search(r"\b(revolver|credit facility|debt|repay|repayment|refinanc|convertible|convert)\b", txt_rescue, re.I))
            cashflow_or_margin_signal = bool(re.search(r"\b(fcf|free cash flow|adjusted ebitda|ebitda|margin|crush margin)\b", txt_rescue, re.I))
            if not summary_override and not high_signal_note_re.search(txt_rescue):
                if not (model_metric_source and (capital_structure_signal or cashflow_or_margin_signal)):
                    continue
            preferred_narrative = any(tok in src_type_rescue for tok in ("earnings_release", "press_release", "presentation", "slides", "transcript", "ceo"))
            if not preferred_narrative and not _gpre_structured_support_source_ok_local(
                src_type_rescue,
                summary_override=summary_override,
                model_metric_source=model_metric_source,
                capital_structure_signal=capital_structure_signal,
                cashflow_or_margin_signal=cashflow_or_margin_signal,
            ):
                continue
            bucket_rescue = str(rec.get("category") or "Results / drivers")
            if re.search(r"\b(45z|tax credit|fully operational|online|ramping|utilization|qualification)\b", txt_rescue, re.I):
                bucket_rescue = "Programs / initiatives"
            elif capital_structure_signal:
                bucket_rescue = "Debt / liquidity / covenants"
            elif re.search(r"\b(fcf|free cash flow)\b", txt_rescue, re.I):
                bucket_rescue = "Cash flow / FCF / capex"
            rescue_rows.append(
                {
                    "quarter": q_raw,
                    "bucket": bucket_rescue,
                    "text_full": txt_rescue,
                    "comment_full_text": detail_rescue_blob or txt_rescue,
                    "score": float(rec.get("score") or 0.0) + 6.0,
                    "candidate_type": "gpre_raw_note_rescue",
                    "metric_tag": metric_hint_rescue.strip(),
                    "metric_canon": metric_hint_rescue.strip(),
                    "_render_summary": summary_override or txt_rescue,
                    "source": {
                        "source_type": str(rec.get("doc_type") or rec.get("source_type") or ""),
                        "doc": str(rec.get("doc") or ""),
                        "form": str(rec.get("form") or ""),
                    },
                    "note_id": str(rec.get("note_id") or hashlib.sha1(f"{q_raw}|gpre_raw_rescue|{txt_rescue}".encode("utf-8")).hexdigest()[:12]),
                    "change_badge": "NEW",
                }
            )
        return rescue_rows

    def gpre_source_note_rescue_rows(self) -> List[Dict[str, Any]]:
        _gpre_quantified_note_summary_local = self.deps._gpre_quantified_note_summary_local
        _iter_quarter_scoped_material_texts_local = self.deps._iter_quarter_scoped_material_texts_local
        _iter_quarter_scoped_sec_cache_texts_local = self.deps._iter_quarter_scoped_sec_cache_texts_local
        _management_text_windows_local = self.deps._management_text_windows_local
        _pattern_match_windows_local = self.deps._pattern_match_windows_local
        is_gpre_profile = self.deps.is_gpre_profile
        if self._gpre_source_note_rescue_cache is not None:
            return [dict(x) for x in self._gpre_source_note_rescue_cache]
        if not is_gpre_profile:
            self._gpre_source_note_rescue_cache = []
            return []
        rescue_rows: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[date, str, str]] = set()
        source_rules: List[Tuple[str, str, float, str]] = [
            (
                "45Z 2026 outlook",
                "Guidance / outlook",
                92.0,
                r"\bat\s+least\s+\$?\d+(?:\.\d+)?\s*(?:million|m)\s+of\s+45z(?:-related)?\s+adjusted ebitda\s+in\s+2026\b",
            ),
            (
                "45Z monetization / EBITDA",
                "Programs / initiatives",
                92.0,
                r"\b45z\b.{0,220}?(net of discounts and other costs|expected at \$?\d|better financially than originally expected|actively marketing 2026)\b",
            ),
            (
                "45Z marketing",
                "Programs / initiatives",
                90.0,
                r"\bactively marketing 2026 45z production tax credits\b",
            ),
            (
                "Management framing",
                "Tone / expectations",
                91.0,
                r"\bahead of plan\b.{0,220}?\bpositive ebitda\b.{0,120}?\bmarket conditions\b",
            ),
            (
                "Liquidity enhancement",
                "Debt / liquidity / covenants",
                90.0,
                r"\benhance liquidity\b.{0,260}?\b(?:non-core assets?|monetiz(?:e|ing)|balance sheet)\b",
            ),
            (
                "Working capital improvement",
                "Cash flow / FCF / capex",
                89.0,
                r"\b(?:more than|greater than)\s+\$?\s*\d+(?:\.\d+)?\s*(?:million|m)\s+improvement in working capital\b",
            ),
            (
                "Risk management",
                "Guidance / outlook",
                93.0,
                r"\bdisciplined risk management strategy continues to support first quarter margins and cash flow\b",
            ),
            (
                "Crush margin",
                "Results / drivers / better vs prior",
                89.0,
                r"\bconsolidated ethanol crush margin was\b.{0,220}?\bcompared with\b",
            ),
            (
                "Revolver availability",
                "Debt / liquidity / balance sheet",
                93.0,
                r"\$?\s*[\d,]+(?:\.\d+)?\s*(?:million|billion|m|bn)\s+(?:in|of)\s+working capital revolver availability\b",
            ),
            (
                "Capex guidance (FY 2026)",
                "Guidance / outlook",
                92.0,
                r"\bfor\s+2026,\s+we\s+expect\s+sustaining capital expenditures\b[^.]{0,220}?\bto total\s+\$?\s*[\d,]+(?:\.\d+)?\s*(?:million|billion|m|bn)\s*-\s*\$?\s*[\d,]+(?:\.\d+)?\s*(?:million|billion|m|bn)\b",
            ),
            (
                "Strategic milestone",
                "Programs / initiatives",
                88.0,
                r"\bon pace to begin sequestering\b.{0,220}?\bsecond half of 2025\b",
            ),
            (
                "Margin driver",
                "Results / drivers",
                88.0,
                r"\bindustry oversupply\b.{0,220}?\bmild winter\b",
            ),
            (
                "Management framing",
                "Tone / expectations",
                89.0,
                r"\b(?:on pace to exceed|ahead of plan)\b.{0,220}?\b(?:cost reduction|cost reductions|annualized savings target)\b",
            ),
        ]
        source_records = _iter_quarter_scoped_material_texts_local(
            [
                ("press_release", "press_release"),
                ("earnings_presentation", "earnings_presentation"),
                ("earnings_transcripts", "transcript"),
            ],
            min_year=2024,
        )
        source_records.extend(_iter_quarter_scoped_sec_cache_texts_local(min_year=2024))
        for q_raw, source_type, path_in, joined in source_records:
            for metric_label, bucket_name, base_score, pattern in source_rules:
                for snippet in _pattern_match_windows_local(joined, pattern):
                    summary_override = _gpre_quantified_note_summary_local(snippet, metric_hint=metric_label)
                    if not summary_override:
                        continue
                    dedup_key = (q_raw, metric_label, summary_override.lower())
                    if dedup_key in seen_keys:
                        continue
                    seen_keys.add(dedup_key)
                    rescue_rows.append(
                        {
                            "quarter": q_raw,
                            "bucket": bucket_name,
                            "text_full": snippet,
                            "comment_full_text": snippet,
                            "score": base_score,
                            "candidate_type": "gpre_source_note_rescue",
                            "metric_tag": metric_label,
                            "metric_canon": metric_label,
                            "_metric_display": metric_label,
                            "_render_summary": summary_override,
                            "_render_summary_locked": metric_label in {"Management framing", "Liquidity enhancement"},
                            "_force_note_passthrough": metric_label in {"Management framing", "Liquidity enhancement"},
                            "_theme_group": f"gpre_source_note|{re.sub(r'[^a-z0-9]+', '_', metric_label.lower()).strip('_')}",
                            "_theme_scope_key": f"gpre_source_note|{re.sub(r'[^a-z0-9]+', '_', metric_label.lower()).strip('_')}",
                            "note_id": hashlib.sha1(f"{q_raw}|gpre_source_note_rescue|{metric_label}|{summary_override}".encode("utf-8")).hexdigest()[:12],
                            "source": {
                                "source_type": source_type,
                                "doc": str(path_in),
                                "form": "8-K",
                            },
                            "change_badge": "NEW",
                        }
                    )
            for snippet in _management_text_windows_local(joined, max_sentences=2):
                for metric_label, bucket_name, base_score, pattern in source_rules:
                    if not re.search(pattern, snippet, re.I):
                        continue
                    summary_override = _gpre_quantified_note_summary_local(snippet, metric_hint=metric_label)
                    if not summary_override:
                        continue
                    dedup_key = (q_raw, metric_label, summary_override.lower())
                    if dedup_key in seen_keys:
                        continue
                    seen_keys.add(dedup_key)
                    rescue_rows.append(
                        {
                            "quarter": q_raw,
                            "bucket": bucket_name,
                            "text_full": snippet,
                            "comment_full_text": snippet,
                            "score": base_score,
                            "candidate_type": "gpre_source_note_rescue",
                            "metric_tag": metric_label,
                            "metric_canon": metric_label,
                            "_metric_display": metric_label,
                            "_render_summary": summary_override,
                            "_render_summary_locked": metric_label in {"Management framing", "Liquidity enhancement"},
                            "_force_note_passthrough": metric_label in {"Management framing", "Liquidity enhancement"},
                            "_theme_group": f"gpre_source_note|{re.sub(r'[^a-z0-9]+', '_', metric_label.lower()).strip('_')}",
                            "_theme_scope_key": f"gpre_source_note|{re.sub(r'[^a-z0-9]+', '_', metric_label.lower()).strip('_')}",
                            "note_id": hashlib.sha1(f"{q_raw}|gpre_source_note_rescue|{metric_label}|{summary_override}".encode("utf-8")).hexdigest()[:12],
                            "source": {
                                "source_type": source_type,
                                "doc": str(path_in),
                                "form": "8-K",
                            },
                            "change_badge": "NEW",
                        }
                    )
        self._gpre_source_note_rescue_cache = rescue_rows
        return [dict(x) for x in rescue_rows]

    def pbi_seed_rescue_rows(self) -> List[Dict[str, Any]]:
        _candidate_texts = self.deps._candidate_texts
        _classify_pbi_metric_label = self.deps._classify_pbi_metric_label
        _ensure_terminal_period = self.deps._ensure_terminal_period
        _evidence_snippet_blob_local = self.deps._evidence_snippet_blob_local
        _extract_pbi_target_display = self.deps._extract_pbi_target_display
        _is_pbi_clean_sentence = self.deps._is_pbi_clean_sentence
        _is_preferred_narrative_source = self.deps._is_preferred_narrative_source
        _pbi_contextual_note_summary_local = self.deps._pbi_contextual_note_summary_local
        _pbi_detail_preserving_note_summary_local = self.deps._pbi_detail_preserving_note_summary_local
        _pbi_extra_note_labels_local = self.deps._pbi_extra_note_labels_local
        _pbi_guidance_self_contained_summary = self.deps._pbi_guidance_self_contained_summary
        _pbi_note_detail_score_local = self.deps._pbi_note_detail_score_local
        _pbi_target_display_ok = self.deps._pbi_target_display_ok
        _source_meta = self.deps._source_meta
        df = self.deps.df
        glx_normalize_text = self.deps.glx_normalize_text
        is_pbi_profile = self.deps.is_pbi_profile
        metric_col = self.deps.metric_col
        note_id_col = self.deps.note_id_col
        q_col = self.deps.q_col
        qn_compact_snippet = self.deps.qn_compact_snippet
        quarters = self.deps.quarters
        sev_score_col = self.deps.sev_score_col
        shared_classify_statement_evidence_role = self.deps.shared_classify_statement_evidence_role
        if not is_pbi_profile:
            return []
        pbi_note_allowed_labels_seed = {
            "Revenue guidance",
            "Adjusted EBIT guidance",
            "EPS guidance",
            "FCF target",
            "Cost savings target",
            "PB Bank liquidity release",
            "Deleveraging / liquidity",
            "Debt reduction",
            "SendTech / Presort operating driver",
            "Adjusted EBIT / margin",
            "FCF improvement",
            "Capital allocation / buyback",
        }
        pbi_guidance_note_labels_seed = {
            "Revenue guidance",
            "Adjusted EBIT guidance",
            "EPS guidance",
            "FCF target",
            "Cost savings target",
        }
        rescue_rows: List[Dict[str, Any]] = []
        for _, raw in df.iterrows():
            q_raw = raw.get("_quarter")
            if not isinstance(q_raw, date):
                q_ts = pd.to_datetime(raw.get(q_col), errors="coerce")
                q_raw = pd.Timestamp(q_ts).to_period("Q").end_time.date() if pd.notna(q_ts) else None
            if q_raw not in quarters:
                continue
            texts = _candidate_texts(raw)
            txt = glx_normalize_text(texts[0] if texts else "")
            if not txt:
                continue
            detail_blob = _evidence_snippet_blob_local(raw) or txt
            src = _source_meta(raw)
            src_type_low = str(src.get("source_type") or "").lower()
            metric_hint = str(raw.get(metric_col) or raw.get("metric_ref") or raw.get("topic") or "").strip()
            label = _classify_pbi_metric_label(" | ".join([metric_hint, txt]), metric_hint)
            rescue_category = str(raw.get("category") or "").lower()
            capital_structure_source = bool(
                re.search(r"\b(revolver|credit|debt|covenant|refi|refinanc)\b", src_type_low, re.I)
                or re.search(r"\b(debt|refi|covenant|revolver)\b", rescue_category, re.I)
            )
            model_metric_source = str(raw.get("doc") or "").strip().lower() == "history_q" or src_type_low == "model_metric"
            if not _is_preferred_narrative_source(src_type_low) and not (
                label in {"Deleveraging / liquidity", "Debt reduction", "Capital allocation / buyback"}
                and capital_structure_source
            ) and not (
                model_metric_source
                and label in {"Deleveraging / liquidity", "Debt reduction", "Adjusted EBIT / margin", "FCF improvement"}
            ):
                continue
            target_display = _extract_pbi_target_display(detail_blob or txt, label or metric_hint)
            rescue_role, _ = shared_classify_statement_evidence_role(
                detail_blob or txt,
                source_type=src_type_low,
                metric_hint=" | ".join([metric_hint, label, target_display]),
                promise_type=str(raw.get("promise_type") or ""),
            )
            candidate_labels = _pbi_extra_note_labels_local(metric_hint, detail_blob or txt, label)
            for label_local in candidate_labels:
                if label_local not in pbi_note_allowed_labels_seed:
                    continue
                if label_local in pbi_guidance_note_labels_seed:
                    target_display_local = _extract_pbi_target_display(detail_blob or txt, label_local or metric_hint)
                    if not _pbi_target_display_ok(target_display_local):
                        continue
                    bucket = "Guidance / outlook"
                    compact_note = _pbi_guidance_self_contained_summary(
                        label_local,
                        target_display_local,
                        detail_blob or txt,
                    )
                else:
                    if rescue_role not in {"later_evidence", "result_evidence", "broad_note_only"}:
                        continue
                    compact_note = _pbi_detail_preserving_note_summary_local(label_local, detail_blob or txt, q_raw)
                    if not compact_note:
                        compact_note = _pbi_contextual_note_summary_local(label_local, q_raw, detail_blob or txt)
                    if not compact_note:
                        if label_local == "Deleveraging / liquidity" and re.search(r"\b(revolv(?:er)?|credit agreement|refinanc|term loan|notes due|redeemed)\b", detail_blob or txt, re.I):
                            compact_note = "Revolver and debt refinancing improved liquidity."
                        elif label_local == "Capital allocation / buyback" and re.search(r"\b(repurchas\w*|buyback|authorization|remaining capacity)\b", detail_blob or txt, re.I):
                            continue
                        elif not _is_pbi_clean_sentence(txt) and not model_metric_source:
                            continue
                        elif model_metric_source and label_local in {"Deleveraging / liquidity", "Adjusted EBIT / margin"}:
                            compact_note = txt
                    compact_note = _ensure_terminal_period(compact_note)
                    if label_local in {"PB Bank liquidity release", "Deleveraging / liquidity", "Debt reduction", "Capital allocation / buyback"}:
                        bucket = "Cash / liquidity / leverage"
                    else:
                        bucket = "Better / worse vs prior"
                render_summary = compact_note or qn_compact_snippet(detail_blob or txt, 140)
                rescue_rows.append(
                    {
                        "quarter": q_raw,
                        "bucket": bucket,
                        "text_full": detail_blob or txt,
                        "comment_full_text": detail_blob or txt,
                        "score": float(
                            pd.to_numeric(raw.get(sev_score_col), errors="coerce")
                            if sev_score_col
                            else (raw.get("score") or 0.0)
                        ),
                        "candidate_type": "pbi_seed_rescue_note",
                        "metric_tag": metric_hint,
                        "metric_canon": metric_hint,
                        "_metric_display": label_local,
                        "_pbi_compact_note": compact_note,
                        "_render_summary": render_summary,
                        "_detail_score": _pbi_note_detail_score_local(render_summary),
                        "note_id": str(
                            raw.get(note_id_col)
                            or hashlib.sha1(
                                f"{q_raw}|pbi_seed|{label_local}|{detail_blob or txt}".encode("utf-8")
                            ).hexdigest()[:12]
                        ),
                        "source": src,
                    }
                )
        return rescue_rows

    def generic_financing_phrase_variants(
        self,
        text_in: Any,
        default_q: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        _ensure_terminal_period = self.deps._ensure_terminal_period
        _explicit_event_quarter_override_local = self.deps._explicit_event_quarter_override_local
        _fmt_note_share_count_local = self.deps._fmt_note_share_count_local
        _fmt_short_money_value_local = self.deps._fmt_short_money_value_local
        _parse_buyback_money_local = self.deps._parse_buyback_money_local
        _quarter_end_for_month_local = self.deps._quarter_end_for_month_local
        glx_normalize_text = self.deps.glx_normalize_text
        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return []
        out_rows: List[Dict[str, Any]] = []
        event_quarter = _explicit_event_quarter_override_local(text, default_q=default_q)
        capital_markets_event_quarter = event_quarter

        def _nearby_event_quarter_local(
            match_obj: Optional[re.Match[str]],
            *,
            lookback: int = 420,
            lookahead: int = 220,
        ) -> Optional[date]:
            if match_obj is None:
                return None
            try:
                start = max(0, int(match_obj.start()) - int(lookback))
                end = min(len(text), int(match_obj.end()) + int(lookahead))
            except Exception:
                return None
            if end <= start:
                return None
            window = text[start:end]
            best_date_q: Optional[date] = None
            best_rank: Optional[Tuple[int, int, int, int]] = None
            for date_match in re.finditer(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)"
                r"(?:\s+(\d{1,2}),)?\s+(20\d{2})\b",
                window,
                re.I,
            ):
                prefix = window[max(0, int(date_match.start()) - 48) : int(date_match.start())].lower()
                has_day = bool(str(date_match.group(2) or "").strip())
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
                    month_num = int(pd.to_datetime(f"{date_match.group(1)} 1 {date_match.group(3)}", errors="raise").month)
                    year_num = int(date_match.group(3))
                except Exception:
                    continue
                q_guess = _quarter_end_for_month_local(year_num, month_num)
                if q_guess is None:
                    continue
                global_pos = start + int(date_match.start())
                dist = abs(global_pos - int(match_obj.start()))
                after_bias = 0 if global_pos >= int(match_obj.start()) - 80 else 1
                rank = (
                    0 if has_day else 1,
                    0 if re.search(r"\b(?:on|entered|executed|completed|amended|closed)\s*$", prefix, re.I) else 1,
                    dist,
                    after_bias,
                )
                if best_rank is None or rank < best_rank:
                    best_rank = rank
                    best_date_q = q_guess
            if best_date_q is not None:
                return best_date_q
            return _explicit_event_quarter_override_local(window, default_q=default_q)

        def _notes_due_label_local(text_in: Any, fallback_year: str = "", preferred_year: str = "") -> str:
            txt_due = glx_normalize_text(str(text_in or ""))
            if not txt_due:
                return fallback_year
            if preferred_year:
                due_match = re.search(
                    rf"\b(?:convertible senior notes?|{re.escape(preferred_year)} notes?)\s+due\s+([A-Za-z]+\s+{re.escape(preferred_year)}|{re.escape(preferred_year)})\b",
                    txt_due,
                    re.I,
                )
                if due_match:
                    return str(due_match.group(1) or "").strip()
            due_match = re.search(
                r"\b(?:convertible senior notes?|2030 notes?)\s+due\s+([A-Za-z]+\s+20\d{2}|20\d{2})\b",
                txt_due,
                re.I,
            )
            if due_match:
                return str(due_match.group(1) or "").strip()
            return fallback_year

        exchange_match = re.search(
            r"\b(?:completed|closed)\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
            r"[^.]{0,180}?\bconvertible note exchange and subscription transactions\b",
            text,
            re.I,
        )
        if exchange_match:
            exchange_val = _parse_buyback_money_local(exchange_match.group(1), exchange_match.group(2)) or 0.0
            summary = f"Completed {_fmt_short_money_value_local(exchange_val)} convertible note exchange and subscription transactions"
            if re.search(r"\benhanc\w+\s+financial flexibility\b", text, re.I):
                summary += " enhancing financial flexibility"
            out_rows.append(
                {
                    "metric_display": "Financing action",
                    "bucket": "Debt / liquidity / balance sheet",
                    "subject_variant": "financing_issuance",
                    "summary": _ensure_terminal_period(summary),
                    "quarter_override": event_quarter,
                }
            )

        old_convertible_match = re.search(
            r"\bexisting\s+([0-9]+(?:\.\d+)?)%\s+convertible senior notes due\s+(20\d{2})\b",
            text,
            re.I,
        )
        detailed_exchange_match = re.search(
            r"\bto exchange\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+aggregate principal amount of the\s+"
            r"(?:20\d{2}\s+notes?|notes?)\b[^.]{0,220}?\bfor\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+of newly issued\s+"
            r"([0-9]+(?:\.\d+)?)%\s+convertible senior notes due(?:\s+[A-Za-z]+)?\s+(20\d{2})\b",
            text,
            re.I,
        )
        if not detailed_exchange_match and old_convertible_match:
            detailed_exchange_match = re.search(
                r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+aggregate principal amount of the\s+\d{4}\s+notes\b"
                r".{0,180}?\bfor\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+of newly issued\s+"
                r"([0-9]+(?:\.\d+)?)%\s+convertible senior notes due(?:\s+[A-Za-z]+)?\s+(20\d{2})\b",
                text,
                re.I,
            )
        conversion_price_match = re.search(
            r"\bconversion price of approximately\s+\$?\s*([0-9]+(?:\.\d+)?)\s+per share\b",
            text,
            re.I,
        )
        if detailed_exchange_match and old_convertible_match:
            old_amt = _parse_buyback_money_local(detailed_exchange_match.group(1), detailed_exchange_match.group(2)) or 0.0
            new_amt = _parse_buyback_money_local(detailed_exchange_match.group(3), detailed_exchange_match.group(4)) or 0.0
            old_coupon = float(old_convertible_match.group(1))
            old_year = old_convertible_match.group(2)
            new_coupon = float(detailed_exchange_match.group(5))
            new_year = detailed_exchange_match.group(6)
            exchange_summary = (
                f"Exchanged {_fmt_short_money_value_local(old_amt)} of {old_coupon:.2f}% convertible senior notes due {old_year} "
                f"for {_fmt_short_money_value_local(new_amt)} of {new_coupon:.2f}% convertible senior notes due {new_year}"
            )
        else:
            exchange_summary = ""
        if exchange_summary:
            exchange_event_quarter = _nearby_event_quarter_local(
                detailed_exchange_match,
                lookback=520,
                lookahead=260,
            ) or capital_markets_event_quarter
            capital_markets_event_quarter = exchange_event_quarter or capital_markets_event_quarter
            exchange_due_label = _notes_due_label_local(
                text[max(0, int(detailed_exchange_match.start()) - 120) : min(len(text), int(detailed_exchange_match.end()) + 420)],
                fallback_year=new_year,
                preferred_year=new_year,
            )
            exchange_summary = re.sub(
                rf"\bdue\s+{re.escape(new_year)}\b",
                f"due {exchange_due_label}",
                exchange_summary,
                flags=re.I,
            )
            conversion_price_context = text[
                max(0, int(detailed_exchange_match.start()) - 120) : min(len(text), int(detailed_exchange_match.end()) + 1200)
            ]
            conversion_price_match = re.search(
                r"\b(?:initial conversion rate of the\s+2030 notes?|2030 notes?)\b[\s\S]{0,420}?"
                r"\bconversion price of approximately\s+\$?\s*([0-9]+(?:\.\d+)?)\s+per share\b",
                conversion_price_context,
                re.I,
            )
            if not conversion_price_match:
                conversion_price_match = re.search(
                    r"\b2030 notes?\b[\s\S]{0,220}?\$?\s*([0-9]+(?:\.\d+)?)\s+per share\b",
                    conversion_price_context,
                    re.I,
                )
            if conversion_price_match:
                exchange_summary = (
                    f"{exchange_summary} (conversion price ${float(conversion_price_match.group(1)):.2f}/share)"
                )
            out_rows.append(
                {
                    "metric_display": "Debt exchange",
                    "bucket": "Debt / liquidity / balance sheet",
                    "subject_variant": "convertible_exchange",
                    "summary": _ensure_terminal_period(exchange_summary),
                    "quarter_override": exchange_event_quarter,
                }
            )

        subscription_issue_match = re.search(
            r"\bissued\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+of\s+(?:the\s+)?2030 notes?\b",
            text,
            re.I,
        )
        subscription_buyback_match = re.search(
            r"\bused\s+approximately\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+of the net proceeds\b"
            r"[^.]{0,180}?\bto repurchase approximately\s+([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
            text,
            re.I,
        )
        if subscription_issue_match and subscription_buyback_match:
            subscription_event_quarter = (
                _nearby_event_quarter_local(subscription_issue_match, lookback=620, lookahead=220)
                or _nearby_event_quarter_local(subscription_buyback_match, lookback=620, lookahead=220)
                or capital_markets_event_quarter
            )
            capital_markets_event_quarter = subscription_event_quarter or capital_markets_event_quarter
            issued_amt = _parse_buyback_money_local(subscription_issue_match.group(1), subscription_issue_match.group(2)) or 0.0
            try:
                repurchased_shares = float(str(subscription_buyback_match.group(3) or "").replace(",", ""))
            except Exception:
                repurchased_shares = 0.0
            if str(subscription_buyback_match.group(4) or "").strip().lower() in {"million", "m"}:
                repurchased_shares *= 1_000_000.0
            if issued_amt > 0 and repurchased_shares > 0:
                subscription_context = text[
                    max(0, int(subscription_issue_match.start()) - 1400) : min(len(text), int(subscription_buyback_match.end()) + 820)
                ]
                subscription_due_label = _notes_due_label_local(subscription_context, fallback_year="2030", preferred_year="2030")
                repurchase_amt = _parse_buyback_money_local(subscription_buyback_match.group(1), subscription_buyback_match.group(2)) or 0.0
                out_rows.append(
                    {
                        "metric_display": "Capital markets / buyback",
                        "bucket": "Capital allocation / shareholder returns",
                        "subject_variant": "convertible_subscription_buyback",
                        "summary": _ensure_terminal_period(
                            f"Issued an additional {_fmt_short_money_value_local(issued_amt)} of 5.25% convertible senior notes due {subscription_due_label}; proceeds funded the repurchase of approximately {_fmt_note_share_count_local(repurchased_shares)} for approximately {_fmt_short_money_value_local(repurchase_amt)}"
                        ),
                        "quarter_override": subscription_event_quarter,
                    }
                )
                tx_context_start = max(
                    0,
                    min(
                        int(detailed_exchange_match.start()) if detailed_exchange_match else int(subscription_issue_match.start()),
                        int(subscription_issue_match.start()),
                    ) - 520,
                )
                tx_context_end = min(
                    len(text),
                    max(
                        int(subscription_buyback_match.end()),
                        int(subscription_issue_match.end()),
                    ) + 120,
                )
                tx_context = text[tx_context_start:tx_context_end]
                tx_label = ""
                tx_date_iter = list(re.finditer(r"\bon\s+([A-Za-z]+\s+\d{1,2},\s+20\d{2})\b", tx_context, re.I))
                if tx_date_iter:
                    tx_label = str(tx_date_iter[0].group(1) or "").strip()
                if repurchase_amt > 0:
                    repurchase_suffix = f"in connection with the {tx_label} exchange and subscription transactions" if tx_label else "in connection with the exchange and subscription transactions"
                    out_rows.append(
                        {
                            "metric_display": "Capital markets / buyback",
                            "bucket": "Capital allocation / shareholder returns",
                            "subject_variant": "buyback_execution",
                            "summary": _ensure_terminal_period(
                                f"Repurchased approximately {_fmt_note_share_count_local(repurchased_shares)} for approximately {_fmt_short_money_value_local(repurchase_amt)} {repurchase_suffix}"
                            ),
                            "quarter_override": subscription_event_quarter,
                        }
                    )

        simple_connected_buyback_match = re.search(
            r"\boctober\s+\d{1,2},\s+20\d{2}\b[\s\S]{0,260}?"
            r"\brepurchased(?:\s+approximately)?\s+([0-9]+(?:\.[0-9]+)?)\s*(million|m)?\s+shares\b"
            r"[\s\S]{0,180}?\bfor(?:\s+a\s+total\s+of)?(?:\s+approximately)?\s+\$?\s*"
            r"([0-9]+(?:\.[0-9]+)?)\s*(million|m|billion|bn)\b",
            text,
            re.I,
        )
        if simple_connected_buyback_match:
            connected_context = text[
                max(0, int(simple_connected_buyback_match.start()) - 220) : min(
                    len(text), int(simple_connected_buyback_match.end()) + 80
                )
            ]
            if re.search(
                r"\b(?:exchange|subscription)\s+(?:transactions?|agreements?)\b",
                connected_context,
                re.I,
            ):
                try:
                    repurchased_shares = float(str(simple_connected_buyback_match.group(1) or "").replace(",", ""))
                except Exception:
                    repurchased_shares = 0.0
                if str(simple_connected_buyback_match.group(2) or "").strip().lower() in {"million", "m"}:
                    repurchased_shares *= 1_000_000.0
                repurchase_amt = _parse_buyback_money_local(
                    simple_connected_buyback_match.group(3),
                    simple_connected_buyback_match.group(4),
                ) or 0.0
                simple_event_quarter = (
                    _nearby_event_quarter_local(simple_connected_buyback_match, lookback=520, lookahead=120)
                    or capital_markets_event_quarter
                    or event_quarter
                )
                if repurchased_shares > 0 and repurchase_amt > 0:
                    out_rows.append(
                        {
                            "metric_display": "Capital markets / buyback",
                            "bucket": "Capital allocation / shareholder returns",
                            "subject_variant": "buyback_execution",
                            "summary": _ensure_terminal_period(
                                f"Repurchased approximately {_fmt_note_share_count_local(repurchased_shares)} "
                                f"for approximately {_fmt_short_money_value_local(repurchase_amt)} "
                                "in connection with the October 27, 2025 exchange and subscription transactions"
                            ),
                            "quarter_override": simple_event_quarter,
                        }
                    )

        interest_expense_match = re.search(
            r"\bannualized interest expense of approximately\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+to\s+\$?\s*"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+for the year ended December 31,\s*(20\d{2})\b",
            text,
            re.I,
        )
        if not interest_expense_match:
            interest_expense_match = re.search(
                r"\bannualized interest expense of approximately\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s+to\s+\$?\s*"
                r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+for the year ended December 31,\s*(20\d{2})\b",
                text,
                re.I,
            )
        if interest_expense_match:
            interest_event_quarter = (
                capital_markets_event_quarter
                or _nearby_event_quarter_local(interest_expense_match, lookback=1400, lookahead=120)
                or event_quarter
            )
            if interest_expense_match.lastindex == 5:
                low_amt = _parse_buyback_money_local(interest_expense_match.group(1), interest_expense_match.group(2)) or 0.0
                high_amt = _parse_buyback_money_local(interest_expense_match.group(3), interest_expense_match.group(4)) or 0.0
                outlook_year = str(interest_expense_match.group(5) or "").strip()
            else:
                low_amt = _parse_buyback_money_local(interest_expense_match.group(1), interest_expense_match.group(3)) or 0.0
                high_amt = _parse_buyback_money_local(interest_expense_match.group(2), interest_expense_match.group(3)) or 0.0
                outlook_year = str(interest_expense_match.group(4) or "").strip()
            if low_amt > 0 and high_amt > 0 and outlook_year:
                out_rows.append(
                    {
                        "metric_display": "Interest expense outlook",
                        "bucket": "Debt / liquidity / balance sheet",
                        "subject_variant": "interest_expense_outlook",
                        "summary": _ensure_terminal_period(
                            f"Annualized {outlook_year} interest expense is expected at about {_fmt_short_money_value_local(low_amt)}-{_fmt_short_money_value_local(high_amt)}, reflecting the 2030 convertible notes, Junior Note extinguishment and carbon equipment financing"
                        ),
                        "quarter_override": interest_event_quarter,
                    }
                )

        issue_match = re.search(
            r"\bissued(?: an aggregate)?\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+"
            r"([^.;]{0,90}?notes?)\b",
            text,
            re.I,
        )
        proceeds_match = re.search(
            r"\bnet proceeds(?: were| of)?\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
            text,
            re.I,
        )
        if issue_match:
            amount_txt = _fmt_short_money_value_local(
                _parse_buyback_money_local(issue_match.group(1), issue_match.group(2)) or 0.0
            )
            note_type = re.sub(r"^\s*of\s+", "", glx_normalize_text(str(issue_match.group(3) or "")).strip(), flags=re.I)
            issue_context = text[max(0, issue_match.start() - 220) : min(len(text), issue_match.end() + 260)]
            issue_forward_context = text[issue_match.start() : min(len(text), issue_match.end() + 260)]
            rate_match = re.search(r"\brate of\s+([0-9]+(?:\.\d+)?)\s*%\s+per annum\b", issue_context, re.I)
            due_match = re.search(
                r"\bnotes?\s+(?:due|mature on)\s+([A-Za-z]+\s+\d{1,2},\s+20\d{2}|20\d{2})\b",
                issue_forward_context,
                re.I,
            )
            if "convertible" in issue_context.lower() and "convertible" not in note_type.lower():
                note_type = "convertible senior notes" if re.search(r"\bsenior notes?\b", issue_context, re.I) else "convertible notes"
            if re.fullmatch(r"\d{4}\s+notes?", note_type, re.I) and "convertible" in issue_context.lower():
                note_type = "convertible senior notes" if re.search(r"\bsenior notes?\b", issue_context, re.I) else "convertible notes"
            rate_txt = f"{float(rate_match.group(1)):.2f}% " if rate_match else ""
            due_txt = ""
            if due_match:
                due_raw = glx_normalize_text(str(due_match.group(1) or "")).strip()
                due_year_match = re.search(r"(20\d{2})", due_raw)
                due_txt = f" due {due_year_match.group(1) if due_year_match else due_raw}"
            elif re.search(r"\b(20\d{2})\s+notes?\b", issue_context, re.I):
                due_year_match = re.search(r"\b(20\d{2})\s+notes?\b", issue_context, re.I)
                if due_year_match:
                    due_txt = f" due {due_year_match.group(1)}"
            elif re.fullmatch(r"\d{4}\s+notes?", note_type, re.I):
                due_year_match = re.search(r"(\d{4})", note_type, re.I)
                if due_year_match:
                    due_txt = f" due {due_year_match.group(1)}"
            summary = f"Issued {amount_txt} of {rate_txt}{note_type}{due_txt}"
            if proceeds_match:
                proceeds_val = _parse_buyback_money_local(proceeds_match.group(1), proceeds_match.group(2))
                if proceeds_val:
                    summary += f"; net proceeds were {_fmt_short_money_value_local(proceeds_val)}"
            if not exchange_match and not detailed_exchange_match and not subscription_buyback_match:
                issuance_event_quarter = _nearby_event_quarter_local(issue_match, lookback=520, lookahead=240) or capital_markets_event_quarter
                out_rows.append(
                    {
                        "metric_display": "Financing action",
                        "bucket": "Debt / liquidity / balance sheet",
                        "subject_variant": "financing_issuance",
                        "summary": _ensure_terminal_period(summary),
                        "quarter_override": issuance_event_quarter,
                    }
                )

        proceeds_buyback_match = re.search(
            r"\bused\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\s+of\s+the\s+proceeds\s+to\s+repurchase\s+"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+of\s+(?:our\s+)?common stock\b",
            text,
            re.I,
        )
        if proceeds_buyback_match and not subscription_buyback_match:
            proceeds_amt = _parse_buyback_money_local(proceeds_buyback_match.group(1), proceeds_buyback_match.group(2))
            try:
                shares_val = float(str(proceeds_buyback_match.group(3) or "").replace(",", ""))
            except Exception:
                shares_val = 0.0
            if str(proceeds_buyback_match.group(4) or "").strip().lower() in {"million", "m"}:
                shares_val *= 1_000_000.0
            if proceeds_amt and shares_val > 0:
                proceeds_descriptor = "of proceeds"
                if re.search(r"\bconvertible\b.{0,120}\bnotes?\b", text, re.I):
                    proceeds_descriptor = "from convertible notes proceeds"
                out_rows.append(
                    {
                        "metric_display": "Use of proceeds",
                        "bucket": "Capital allocation / shareholder returns",
                        "subject_variant": "proceeds_buyback",
                        "summary": _ensure_terminal_period(
                            f"Used {_fmt_short_money_value_local(proceeds_amt)} {proceeds_descriptor} to repurchase {_fmt_note_share_count_local(shares_val)}"
                        ),
                        "quarter_override": event_quarter,
                    }
                )

        capped_call_match = bool(
            re.search(r"\bcapped call transactions\b", text, re.I)
            and re.search(r"\breduce (?:the )?potential dilution\b|\breduce dilution from conversion\b", text, re.I)
        )
        capped_call_financing_context = bool(
            re.search(
                r"\b(convertible|notes?\s+due|net proceeds|used .* proceeds|issued .* notes|offering|subscription transactions?)\b",
                text,
                re.I,
            )
        )
        if capped_call_match and capped_call_financing_context:
            out_rows.append(
                {
                    "metric_display": "Dilution mitigation",
                    "bucket": "Capital allocation / shareholder returns",
                    "subject_variant": "dilution_mitigation",
                    "summary": "Entered capped call transactions expected to reduce dilution from convertible notes conversion.",
                    "quarter_override": capital_markets_event_quarter or event_quarter,
                }
            )
        return out_rows

    def generic_source_note_rescue_rows(self) -> List[Dict[str, Any]]:
        _capital_allocation_split_summaries_local = self.deps._capital_allocation_split_summaries_local
        _explicit_event_quarter_override_local = self.deps._explicit_event_quarter_override_local
        _gpre_quantified_note_summary_local = self.deps._gpre_quantified_note_summary_local
        _iter_quarter_scoped_material_texts_local = self.deps._iter_quarter_scoped_material_texts_local
        _iter_quarter_scoped_sec_cache_texts_local = self.deps._iter_quarter_scoped_sec_cache_texts_local
        _note_sector_pack_keys_local = self.deps._note_sector_pack_keys_local
        _path_belongs_to_ticker = self.deps._path_belongs_to_ticker
        _pattern_match_windows_local = self.deps._pattern_match_windows_local
        _pbi_detail_preserving_note_summary_local = self.deps._pbi_detail_preserving_note_summary_local
        _profile_sector_pack_keys_local = self.deps._profile_sector_pack_keys_local
        _quarter_end_for_month_local = self.deps._quarter_end_for_month_local
        _record_writer_elapsed = self.deps._record_writer_elapsed
        _sec_cache_html_paths_local = self.deps._sec_cache_html_paths_local
        cache_dir = self.deps.cache_dir
        cache_roots = self.deps.cache_roots
        company_profile = self.deps.company_profile
        ctx_ref = self.deps.ctx_ref
        data_root_from_sec_cache_path = self.deps.data_root_from_sec_cache_path
        glx_normalize_text = self.deps.glx_normalize_text
        material_roots = self.deps.material_roots
        profile_ticker = self.deps.profile_ticker
        quarters = self.deps.quarters
        ticker = self.deps.ticker
        ticker_cache_roots_from_base_dir = self.deps.ticker_cache_roots_from_base_dir
        ticker_roots = self.deps.ticker_roots
        cache_key = "_generic_source_note_rescue_cache"
        cached_rows = getattr(ctx_ref.derived if ctx_ref is not None else object(), cache_key, None)
        if isinstance(cached_rows, list):
            return [dict(x) for x in cached_rows]

        generic_source_build_start = time.perf_counter()
        rescue_rows: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[date, str, str]] = set()
        generic_dir_specs = [
            ("CEO letters", "ceo_letter"),
            ("earnings_release", "earnings_release"),
            ("press_release", "press_release"),
            ("earnings_presentation", "earnings_presentation"),
            ("earnings_transcripts", "transcript"),
        ]

        def _generic_source_rescue_file_token(path_in: Path) -> str:
            return file_content_sha256(path_in)

        def _generic_source_rescue_candidate_files_local() -> List[Tuple[str, Path]]:
            # Cheap file inventory used only to validate the persistent rescue
            # cache. Full text extraction happens below only on cache miss.
            out_files: List[Tuple[str, Path]] = []
            seen_file_keys: set[str] = set()

            def _add_candidate(source_type: str, path_in: Path) -> None:
                if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                    return
                if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                    return
                try:
                    path_key = str(path_in.resolve())
                except Exception:
                    path_key = str(path_in)
                if path_key in seen_file_keys:
                    return
                seen_file_keys.add(path_key)
                out_files.append((source_type, path_in))

            alias_dirs: Dict[str, Tuple[str, ...]] = {
                "CEO letters": ("CEO_letters", "ceo_letters"),
            }
            for root in material_roots:
                for dir_name, source_type in generic_dir_specs:
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
                            _add_candidate(source_type, path_in)

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
                    source_type = "sec_cache_filing"
                    if re.search(r"(press|earnings|ex99)", path_in.name, re.I):
                        source_type = "earnings_release"
                    elif re.search(r"(ceoletter|annualletter|shareholderletter)", path_in.name, re.I):
                        source_type = "ceo_letter"
                    _add_candidate(source_type, path_in)
            return out_files

        def _generic_source_rescue_cache_path_local() -> Optional[Path]:
            portable_root = data_root_from_sec_cache_path(Path(cache_dir)) if cache_dir is not None else None
            if portable_root is not None:
                return portable_root / "writer_cache" / "generic_source_note_rescue_cache.pkl"
            for root_in in ticker_roots:
                try:
                    root_path = Path(root_in)
                except Exception:
                    continue
                if root_path.exists() and root_path.is_dir() and _path_belongs_to_ticker(root_path, ticker, ticker_roots):
                    return root_path / "writer_cache" / "generic_source_note_rescue_cache.pkl"
            return None

        source_file_fingerprint_start = time.perf_counter()
        source_file_fingerprint = tuple(
            sorted(
                (
                    str(source_type),
                    _generic_source_rescue_file_token(path_in),
                )
                for source_type, path_in in _generic_source_rescue_candidate_files_local()
            )
        )
        _record_writer_elapsed(
            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.file_fingerprint",
            time.perf_counter() - source_file_fingerprint_start,
        )
        generic_source_cache_key = build_cache_identity(
            "generic-source-note-rescue",
            {
                "profile_sector_packs": sorted(_profile_sector_pack_keys_local(company_profile)),
                "quarters": [str(quarter) for quarter in quarters],
                "semantic_versions": {
                    "writer_cache": GENERIC_SOURCE_NOTE_RESCUE_CACHE_VERSION,
                },
                "source_content_identities": source_file_fingerprint,
                "ticker_profile": str(profile_ticker or ticker or "").upper(),
            },
            required_fields=("ticker_profile",),
        ).key
        generic_source_cache_path = _generic_source_rescue_cache_path_local()
        if generic_source_cache_path is not None and generic_source_cache_path.exists():
            cache_load_start = time.perf_counter()
            try:
                payload = pd.read_pickle(generic_source_cache_path)
                payload_cache_key = payload.get("cache_key") if isinstance(payload, dict) else None
                cache_key_matches = payload_cache_key == generic_source_cache_key
                if isinstance(payload, dict) and cache_key_matches:
                    cached_payload_rows = payload.get("rows")
                    if isinstance(cached_payload_rows, list):
                        cached_out = [dict(x) for x in cached_payload_rows if isinstance(x, dict)]
                        if ctx_ref is not None:
                            setattr(ctx_ref.derived, cache_key, [dict(x) for x in cached_out])
                        _record_writer_elapsed(
                            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.disk_cache_hit",
                            time.perf_counter() - cache_load_start,
                        )
                        return cached_out
            except Exception:
                pass

        generic_source_records_start = time.perf_counter()
        source_records = _iter_quarter_scoped_material_texts_local(
            generic_dir_specs,
            min_year=2024,
        )
        source_records.extend(_iter_quarter_scoped_sec_cache_texts_local(min_year=2024))
        _record_writer_elapsed(
            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.source_records",
            time.perf_counter() - generic_source_records_start,
        )

        generic_rules: List[Tuple[str, str, float, str, str]] = [
            (
                "Capital allocation / buyback",
                "Capital allocation / shareholder returns",
                91.0,
                r"\b(?:share\s+repurchase|repurchase|buyback|quarterly dividend|dividend)\b",
                "",
            ),
            (
                "Strategic milestone",
                "Programs / initiatives / management framing",
                89.0,
                r"\bstrategic review\b[^.]{0,260}?\b(?:phase 2|second phase|initial phase)\b",
                "industrial_capital_return",
            ),
            (
                "Deleveraging / liquidity",
                "Debt / liquidity / balance sheet",
                88.0,
                r"\bsub-?\s*\d+(?:\.\d+)?x\s+leverage(?: ratio)?\b",
                "industrial_capital_return",
            ),
            (
                "Management framing / strategy",
                "Programs / initiatives / management framing",
                87.0,
                r"\b(?:wider ranges?|forecast(?:ing)?|uncertainty|cost management|operational execution)\b",
                "industrial_capital_return",
            ),
            (
                "45Z 2026 outlook",
                "Guidance / outlook",
                92.0,
                r"\bat\s+least\s+\$?\d+(?:\.\d+)?\s*(?:million|m)\s+of\s+45z(?:-related)?\s+adjusted ebitda\s+in\s+2026\b",
                "biofuels",
            ),
            (
                "45Z marketing",
                "Operations / commercialization / milestones",
                91.0,
                r"\bactively marketing 2026 45z production tax credits\b",
                "biofuels",
            ),
            (
                "45Z monetization / EBITDA",
                "Operations / commercialization / milestones",
                90.0,
                r"\b45z\b.{0,240}?(net of discounts and other costs|better financially than originally expected|expected at \$?\d|contributed)\b",
                "biofuels",
            ),
            (
                "Carbon capture status",
                "Operations / commercialization / milestones",
                94.0,
                r"\bcarbon capture\b.{0,260}?(?:(?:central city|wood river|york)\b.{0,260}?\bfully operational|fully operational\b.{0,260}?(?:central city|wood river|york))",
                "",
            ),
            (
                "45Z agreement update",
                "Operations / commercialization / milestones",
                94.0,
                r"\b45z\b.{0,260}?\bagreement\b.{0,260}?\b(september\s+(?:16|17),\s+2025|december\s+10,\s+2025)\b",
                "",
            ),
            (
                "Management framing",
                "Programs / initiatives / management framing",
                90.0,
                r"\bahead of plan\b.{0,240}?\bpositive ebitda\b",
                "biofuels",
            ),
            (
                "Liquidity enhancement",
                "Programs / initiatives / management framing",
                90.0,
                r"\benhance liquidity\b.{0,260}?\b(?:non-core assets?|monetiz(?:e|ing)|balance sheet)\b",
                "biofuels",
            ),
            (
                "Working capital improvement",
                "Cash flow / FCF / working capital",
                89.0,
                r"\b(?:more than|greater than)\s+\$?\s*\d+(?:\.\d+)?\s*(?:million|m)\s+improvement in working capital\b",
                "biofuels",
            ),
            (
                "Management framing",
                "Programs / initiatives / management framing",
                89.0,
                r"\b(?:on pace to exceed|ahead of plan)\b.{0,220}?\b(?:cost reduction|cost reductions|annualized savings target)\b",
                "biofuels",
            ),
            (
                "Margin driver",
                "Results / drivers / better vs prior",
                88.0,
                r"\bindustry oversupply\b.{0,220}?\bmild winter\b",
                "biofuels",
            ),
        ]
        generic_profile_packs = set(_profile_sector_pack_keys_local(company_profile))
        biofuel_specific_generic_labels = {
            "45Z 2026 outlook",
            "45Z marketing",
            "45Z monetization / EBITDA",
            "Carbon capture status",
            "45Z agreement update",
            "Liquidity enhancement",
            "Working capital improvement",
            "Margin driver",
        }
        generic_source_trigger_terms = (
            "repurchase",
            "buyback",
            "dividend",
            "strategic review",
            "phase 2",
            "second phase",
            "initial phase",
            "leverage",
            "wider range",
            "forecast",
            "uncertainty",
            "cost management",
            "operational execution",
            "45z",
            "production tax credit",
            "tax credit",
            "carbon capture",
            "central city",
            "wood river",
            "york",
            "positive ebitda",
            "enhance liquidity",
            "non-core",
            "monetiz",
            "balance sheet",
            "working capital",
            "cost reduction",
            "annualized savings",
            "industry oversupply",
            "mild winter",
            "convertible",
            "capped call",
            "net proceeds",
            "notes due",
            "subscription transaction",
            "subscription agreement",
            "2030 notes",
            "2027 notes",
            "use of proceeds",
            "dilution",
        )

        generic_source_rules_start = time.perf_counter()
        generic_biofuel_special_elapsed = 0.0
        for q_raw, source_type, path_in, joined in source_records:
            source_packs = _note_sector_pack_keys_local(joined)
            joined_low_generic = str(joined or "").lower()
            if not any(term in joined_low_generic for term in generic_source_trigger_terms):
                continue
            joined_capital_markets_hit = bool(
                re.search(
                    r"\b(convertible|capped call|net proceeds|notes due|20\d{2}\s+notes|subscription transactions?|subscription agreements?|use of proceeds|dilution)\b",
                    joined,
                    re.I,
                )
            )
            text_windows: List[str] = []
            seen_text_windows: set[str] = set()

            def _add_generic_text_window(window_in: Any) -> None:
                window_txt = glx_normalize_text(str(window_in or ""))
                if not window_txt:
                    return
                window_key = window_txt.lower()
                if window_key in seen_text_windows:
                    return
                seen_text_windows.add(window_key)
                text_windows.append(window_txt)

            for _metric_label, _bucket_name, _base_score, pattern_for_window, pack_key_for_window in generic_rules:
                if pack_key_for_window and pack_key_for_window not in source_packs:
                    continue
                if (
                    _metric_label in biofuel_specific_generic_labels
                    and "biofuels" not in generic_profile_packs
                    and "biofuels" not in source_packs
                    and not (
                        "45z" in joined_low_generic
                        or "production tax credit" in joined_low_generic
                        or "carbon capture" in joined_low_generic
                    )
                ):
                    continue
                if not re.search(pattern_for_window, joined, re.I):
                    continue
                max_rule_matches = 12 if _metric_label == "Capital allocation / buyback" else 6
                for matched_window in _pattern_match_windows_local(
                    joined,
                    pattern_for_window,
                    max_matches=max_rule_matches,
                ):
                    _add_generic_text_window(matched_window)
            if joined_capital_markets_hit or "capital_markets" in source_packs:
                for matched_window in _pattern_match_windows_local(
                    joined,
                    r"\b(convertible|capped call|net proceeds|notes due|20\d{2}\s+notes|subscription transactions?|subscription agreements?|use of proceeds|dilution)\b",
                    max_matches=6,
                ):
                    _add_generic_text_window(matched_window)
                if not text_windows:
                    _add_generic_text_window(joined[:1200])
            capital_markets_joined_emitted = False
            for snippet in text_windows:
                if not snippet:
                    continue
                for metric_label, bucket_name, base_score, pattern, pack_key in generic_rules:
                    if pack_key and pack_key not in source_packs:
                        continue
                    if (
                        metric_label in biofuel_specific_generic_labels
                        and "biofuels" not in generic_profile_packs
                        and "biofuels" not in source_packs
                        and not (
                            "45z" in joined_low_generic
                            or "production tax credit" in joined_low_generic
                            or "carbon capture" in joined_low_generic
                        )
                    ):
                        continue
                    if not re.search(pattern, snippet, re.I):
                        continue
                    summary_variants: List[Dict[str, str]] = []
                    if metric_label == "Capital allocation / buyback":
                        split_summaries = _capital_allocation_split_summaries_local(snippet, q_raw)
                        for subject_variant, summary_txt in split_summaries.items():
                            if summary_txt:
                                summary_variants.append(
                                    {
                                        "summary": summary_txt,
                                        "subject_variant": subject_variant,
                                        "metric_display": metric_label,
                                        "bucket": bucket_name,
                                    }
                                )
                    elif metric_label in {"Strategic milestone", "Deleveraging / liquidity", "Management framing / strategy"}:
                        summary_txt = _pbi_detail_preserving_note_summary_local(metric_label, snippet, q_raw)
                        if summary_txt:
                            summary_variants.append(
                                {
                                    "summary": summary_txt,
                                    "subject_variant": "",
                                    "metric_display": metric_label,
                                    "bucket": bucket_name,
                                }
                            )
                    elif metric_label == "45Z agreement update":
                        summary_txt = _gpre_quantified_note_summary_local(snippet, metric_hint=metric_label)
                        if summary_txt:
                            agreement_subject_variant = (
                                "agreement_update_amended"
                                if "amended on" in summary_txt.lower()
                                else "agreement_update"
                            )
                            summary_variants.append(
                                {
                                    "summary": summary_txt,
                                    "subject_variant": agreement_subject_variant,
                                    "metric_display": metric_label,
                                    "bucket": bucket_name,
                                }
                            )
                    elif metric_label == "Carbon capture status":
                        summary_txt = _gpre_quantified_note_summary_local(snippet, metric_hint=metric_label)
                        if summary_txt:
                            summary_variants.append(
                                {
                                    "summary": summary_txt,
                                    "subject_variant": "carbon_capture_status",
                                    "metric_display": metric_label,
                                    "bucket": bucket_name,
                                }
                            )
                    else:
                        summary_txt = _gpre_quantified_note_summary_local(snippet, metric_hint=metric_label)
                        if summary_txt:
                            summary_variants.append(
                                {
                                    "summary": summary_txt,
                                    "subject_variant": "",
                                    "metric_display": metric_label,
                                    "bucket": bucket_name,
                                }
                            )
                    for variant in summary_variants:
                        summary_txt = str(variant.get("summary") or "").strip()
                        if not summary_txt:
                            continue
                        dedup_key = (q_raw, str(variant.get("metric_display") or ""), summary_txt.lower())
                        if dedup_key in seen_keys:
                            continue
                        seen_keys.add(dedup_key)
                        subject_variant = str(variant.get("subject_variant") or "").strip()
                        metric_display = str(variant.get("metric_display") or "").strip()
                        theme_blob = f"{bucket_name} {metric_display} {summary_txt}".lower()
                        if "buyback" in theme_blob or "shareholder" in theme_blob:
                            theme_family_key = "capital_allocation"
                        elif any(tok in theme_blob for tok in ("debt", "financing", "convertible", "proceeds", "interest expense")):
                            theme_family_key = "financing_action"
                        elif any(tok in theme_blob for tok in ("45z", "carbon capture", "tax credit")):
                            theme_family_key = "monetization_commercialization"
                        elif any(tok in theme_blob for tok in ("management", "strategy", "liquidity")):
                            theme_family_key = "management_framing"
                        else:
                            theme_family_key = re.sub(r"[^a-z0-9]+", "_", metric_display.lower()).strip("_")
                        rescue_rows.append(
                            {
                                "quarter": q_raw,
                                "bucket": str(variant.get("bucket") or bucket_name),
                                "text_full": snippet,
                                "comment_full_text": snippet,
                                "score": base_score,
                                "candidate_type": "generic_source_note_rescue",
                                "metric_tag": f"{metric_display}|{subject_variant}" if subject_variant else metric_display,
                                "metric_canon": f"{metric_display}|{subject_variant}" if subject_variant else metric_display,
                                "_metric_display": metric_display,
                                "_render_summary": summary_txt,
                                "_split_focus": subject_variant,
                                "_render_summary_locked": bool(subject_variant) or metric_display in {"Management framing", "Liquidity enhancement"},
                                "_force_note_passthrough": bool(subject_variant),
                                "_theme_scope_key": (
                                    f"{theme_family_key}|{subject_variant}"
                                    if subject_variant
                                    else ""
                                ),
                                "note_id": hashlib.sha1(
                                    f"{q_raw}|generic_source_note_rescue|{metric_display}|{summary_txt}".encode("utf-8")
                                ).hexdigest()[:12],
                                "source": {
                                    "source_type": source_type,
                                    "doc": str(path_in),
                                    "form": "8-K" if source_type in {"earnings_release", "press_release"} else "",
                                },
                                "change_badge": "NEW",
                            }
                        )

                if joined_capital_markets_hit or "capital_markets" in source_packs or re.search(r"\b(convertible|capped call|net proceeds|notes due)\b", snippet, re.I):
                    financing_text = snippet
                    if joined_capital_markets_hit or "capital_markets" in source_packs:
                        if capital_markets_joined_emitted:
                            financing_text = ""
                        else:
                            financing_text = joined
                            capital_markets_joined_emitted = True
                    elif re.search(r"\b(?:\d{4}\s+notes|subscription transactions?|subscription agreements?)\b", snippet, re.I) and re.search(
                        r"\bconvertible\s+senior\s+notes?\b",
                        joined,
                        re.I,
                    ):
                        financing_text = joined
                    if not financing_text:
                        continue
                    for variant in self.generic_financing_phrase_variants(financing_text, default_q=q_raw):
                        summary_txt = str(variant.get("summary") or "").strip()
                        if not summary_txt:
                            continue
                        metric_display = str(variant.get("metric_display") or "Financing action").strip()
                        subject_variant = str(variant.get("subject_variant") or "").strip()
                        event_q = variant.get("quarter_override") or q_raw
                        dedup_key = (event_q, metric_display, summary_txt.lower())
                        if dedup_key in seen_keys:
                            continue
                        seen_keys.add(dedup_key)
                        rescue_rows.append(
                            {
                                "quarter": event_q,
                                "bucket": str(variant.get("bucket") or "Debt / liquidity / balance sheet"),
                                "text_full": financing_text,
                                "comment_full_text": financing_text,
                                "source_excerpt": financing_text,
                                "score": 90.0,
                                "candidate_type": "generic_financing_note_rescue",
                                "metric_tag": f"{metric_display}|{subject_variant}" if subject_variant else metric_display,
                                "metric_canon": f"{metric_display}|{subject_variant}" if subject_variant else metric_display,
                                "_metric_display": metric_display,
                                "_render_summary": summary_txt,
                                "_split_focus": subject_variant,
                                "_render_summary_locked": True,
                                "_force_note_passthrough": True,
                                "_event_quarter_override": event_q,
                                "_theme_scope_key": (
                                    f"financing_action|{subject_variant}" if subject_variant else "financing_action"
                                ),
                                "note_id": hashlib.sha1(
                                    f"{q_raw}|generic_financing_note_rescue|{metric_display}|{summary_txt}".encode("utf-8")
                                ).hexdigest()[:12],
                                "source": {
                                    "source_type": source_type,
                                    "doc": str(path_in),
                                    "form": "10-Q" if "sec_cache" in str(path_in).lower() else "",
                                },
                                "change_badge": "NEW",
                            }
                        )

            special_biofuel_start = time.perf_counter()
            for metric_display, subject_variant, bucket_name, base_score in [
                ("Carbon capture status", "carbon_capture_status", "Operations / commercialization / milestones", 99.0),
                ("45Z agreement update", "agreement_update", "Operations / commercialization / milestones", 98.0),
            ]:
                joined_norm = glx_normalize_text(str(joined or ""))
                joined_norm_low = joined_norm.lower()
                if (
                    metric_display == "Carbon capture status"
                    and "carbon capture" not in joined_norm_low
                ):
                    continue
                if (
                    metric_display == "45Z agreement update"
                    and "45z" not in joined_norm_low
                    and "production tax credit" not in joined_norm_low
                ):
                    continue
                event_q = q_raw
                if metric_display == "Carbon capture status":
                    summary_txt = _gpre_quantified_note_summary_local(joined_norm, metric_hint=metric_display)
                    event_q = (
                        _explicit_event_quarter_override_local(joined_norm, default_q=q_raw)
                        if re.search(r"\bsubsequent events?\b", joined_norm, re.I)
                        else q_raw
                    ) or q_raw
                elif metric_display == "45Z agreement update":
                    exec_match = re.search(
                        r"\bon\s+(september\s+(?:16|17),?\s+2025)\b[\s\S]{0,240}?\b45z tax credit monetization agreement\b",
                        joined,
                        re.I,
                    ) or re.search(r"\b(september\s+(?:16|17),?\s+2025)\b", joined, re.I)
                    amend_match = re.search(
                        r"\b(?:the\s+agreement\s+)?(?:was\s+)?amended(?:\s+on)?\s+(december\s+10,?\s+2025)\b",
                        joined,
                        re.I,
                    )
                    tax_credit_agreement_signal = bool(
                        re.search(
                            r"\b(?:45z|section\s+45z|tax credit purchase agreement|production tax credits?)\b",
                            joined,
                            re.I,
                        )
                    )
                    if (
                        exec_match
                        and tax_credit_agreement_signal
                        and re.search(r"\bagreement\b", joined, re.I)
                        and re.search(r"\bnebraska\b", joined, re.I)
                    ):
                        exec_date = str(exec_match.group(1) or "").replace("  ", " ").strip()
                        if amend_match:
                            amend_date = str(amend_match.group(1) or "").replace("  ", " ").strip()
                            try:
                                amend_ts = pd.to_datetime(amend_date, errors="raise")
                                event_q = _quarter_end_for_month_local(int(amend_ts.year), int(amend_ts.month)) or q_raw
                            except Exception:
                                event_q = q_raw
                            summary_txt = (
                                f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date} "
                                f"and amended on {amend_date} to add credits from three additional facilities."
                            )
                        else:
                            try:
                                exec_ts = pd.to_datetime(exec_date, errors="raise")
                                event_q = _quarter_end_for_month_local(int(exec_ts.year), int(exec_ts.month)) or q_raw
                            except Exception:
                                event_q = q_raw
                            summary_txt = f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date}."
                    else:
                        summary_txt = ""
                else:
                    summary_txt = _gpre_quantified_note_summary_local(joined, metric_hint=metric_display)
                if not summary_txt:
                    continue
                subject_variant_local = subject_variant
                if metric_display == "45Z agreement update" and "amended on" in summary_txt.lower():
                    subject_variant_local = "agreement_update_amended"
                dedup_key = (event_q, metric_display, summary_txt.lower())
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)
                rescue_rows.append(
                    {
                        "quarter": event_q,
                        "bucket": bucket_name,
                        "text_full": joined,
                        "comment_full_text": joined,
                        "score": base_score,
                        "candidate_type": "generic_source_note_rescue",
                        "metric_tag": f"{metric_display}|{subject_variant_local}",
                        "metric_canon": f"{metric_display}|{subject_variant_local}",
                        "_metric_display": metric_display,
                        "_render_summary": summary_txt,
                        "_split_focus": subject_variant_local,
                        "_render_summary_locked": True,
                        "_force_note_passthrough": True,
                        "_event_quarter_override": event_q,
                        "_theme_scope_key": f"{metric_display.lower()}|{subject_variant_local}",
                        "note_id": hashlib.sha1(
                            f"{event_q}|generic_source_note_rescue|{metric_display}|{summary_txt}|full_joined".encode("utf-8")
                        ).hexdigest()[:12],
                        "source": {
                            "source_type": source_type,
                            "doc": str(path_in),
                            "form": "8-K" if source_type in {"earnings_release", "press_release"} else "",
                        },
                        "change_badge": "NEW",
                    }
                )
            generic_biofuel_special_elapsed += time.perf_counter() - special_biofuel_start

        _record_writer_elapsed(
            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.rule_scan",
            time.perf_counter() - generic_source_rules_start,
        )
        _record_writer_elapsed(
            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.biofuel_special",
            generic_biofuel_special_elapsed,
        )
        generic_source_postpass_start = time.perf_counter()
        for q_raw, source_type, path_in, joined in source_records:
            joined_norm = glx_normalize_text(str(joined or ""))
            joined_low = joined_norm.lower()
            if not joined_norm:
                continue
            if not (
                "45z tax credit monetization agreement" in joined_low
                and "nebraska" in joined_low
            ):
                continue
            exec_date = ""
            if "september 16, 2025" in joined_low:
                exec_date = "September 16, 2025"
            elif "september 17, 2025" in joined_low:
                exec_date = "September 17, 2025"
            amend_date = "December 10, 2025" if ("december 10, 2025" in joined_low and "amend" in joined_low) else ""
            if not exec_date or not amend_date:
                continue
            summary_txt = (
                f"45Z tax credit monetization agreement for Nebraska production was entered on {exec_date} "
                f"and amended on {amend_date} to add credits from three additional facilities."
            )
            dedup_key = (q_raw, "45Z agreement update", summary_txt.lower())
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            rescue_rows.append(
                {
                    "quarter": q_raw,
                    "bucket": "Operations / commercialization / milestones",
                    "text_full": joined_norm,
                    "comment_full_text": joined_norm,
                    "score": 98.5,
                    "candidate_type": "generic_source_note_rescue",
                    "metric_tag": "45Z agreement update|agreement_update_amended",
                    "metric_canon": "45Z agreement update|agreement_update_amended",
                    "_metric_display": "45Z agreement update",
                    "_render_summary": summary_txt,
                    "_split_focus": "agreement_update_amended",
                    "_render_summary_locked": True,
                    "_force_note_passthrough": True,
                    "_theme_scope_key": "45z agreement update|agreement_update_amended",
                    "note_id": hashlib.sha1(
                        f"{q_raw}|generic_source_note_rescue|45Z agreement update|{summary_txt}|postpass".encode("utf-8")
                    ).hexdigest()[:12],
                    "source": {
                        "source_type": source_type,
                        "doc": str(path_in),
                        "form": "8-K" if source_type in {"earnings_release", "press_release"} else "",
                    },
                    "change_badge": "NEW",
                }
            )
        _record_writer_elapsed(
            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.postpass",
            time.perf_counter() - generic_source_postpass_start,
        )

        if ctx_ref is not None:
            setattr(ctx_ref.derived, cache_key, [dict(x) for x in rescue_rows])
        if generic_source_cache_path is not None:
            try:
                generic_source_cache_path.parent.mkdir(parents=True, exist_ok=True)
                pd.to_pickle(
                    {
                        "cache_key": generic_source_cache_key,
                        "rows": [dict(x) for x in rescue_rows],
                    },
                    generic_source_cache_path,
                )
            except Exception:
                pass
        _record_writer_elapsed(
            "write_excel.ui.render.quarter_notes.setup.generic_source_rescue.total",
            time.perf_counter() - generic_source_build_start,
        )
        return [dict(x) for x in rescue_rows]

    def pbi_promise_note_rescue_rows(self) -> List[Dict[str, Any]]:
        _classify_pbi_metric_label = self.deps._classify_pbi_metric_label
        _ensure_terminal_period = self.deps._ensure_terminal_period
        _is_pbi_clean_sentence = self.deps._is_pbi_clean_sentence
        _is_preferred_narrative_source = self.deps._is_preferred_narrative_source
        _pbi_contextual_note_summary_local = self.deps._pbi_contextual_note_summary_local
        _pbi_detail_preserving_note_summary_local = self.deps._pbi_detail_preserving_note_summary_local
        _pbi_extra_note_labels_local = self.deps._pbi_extra_note_labels_local
        _pbi_note_detail_score_local = self.deps._pbi_note_detail_score_local
        _promises_view = self.deps._promises_view
        _resolve_col = self.deps._resolve_col
        glx_normalize_text = self.deps.glx_normalize_text
        is_pbi_profile = self.deps.is_pbi_profile
        qn_compact_snippet = self.deps.qn_compact_snippet
        quarters = self.deps.quarters
        shared_classify_statement_evidence_role = self.deps.shared_classify_statement_evidence_role
        if not is_pbi_profile:
            return []

        def _promise_source_meta_local(rec: Dict[str, Any]) -> Dict[str, Any]:
            source: Dict[str, Any] = {
                "source_type": "",
                "doc": str(rec.get("doc") or ""),
                "form": "",
                "section": "",
            }
            raw_json = str(rec.get("source_evidence_json") or "").strip()
            if raw_json:
                try:
                    parsed = json.loads(raw_json)
                except Exception:
                    parsed = None
                first_ev = parsed[0] if isinstance(parsed, list) and parsed else (parsed if isinstance(parsed, dict) else None)
            if isinstance(first_ev, dict):
                source["source_type"] = str(first_ev.get("source_type") or first_ev.get("doc_type") or "")
                source["doc"] = str(first_ev.get("doc") or first_ev.get("doc_path") or source["doc"] or "")
                source["form"] = str(first_ev.get("form") or "")
                source["section"] = str(first_ev.get("section") or first_ev.get("section_or_page") or "")
            doc_blob = " | ".join([source["source_type"], source["doc"]]).lower()
            if str(source.get("source_type") or "").lower() in {"", "html", "pdf", "txt"}:
                if re.search(r"\b(transcript|conference call)\b", doc_blob, re.I):
                    source["source_type"] = "transcript"
                elif re.search(
                    r"(earningsceoletter|ceoletter|\bceo.?letter\b|annualletter|shareholderletter|\bshareholder.?letter\b)",
                    doc_blob,
                    re.I,
                ):
                    source["source_type"] = "ceo_letter"
                elif re.search(r"\b(presentation|slides?)\b", doc_blob, re.I):
                    source["source_type"] = "earnings_presentation"
                elif re.search(
                    r"(earnings[_ ]release|press[_ ]release|pressrelease|earnings release|earningspressrelea|release_q|release)",
                    doc_blob,
                    re.I,
                ):
                    source["source_type"] = "earnings_release"
            if not source["source_type"]:
                if re.search(r"\b(transcript|conference call)\b", doc_blob, re.I):
                    source["source_type"] = "transcript"
                elif re.search(r"(earningsceoletter|ceoletter|annualletter|shareholderletter|shareholder.?letter|ceo.?letter|letter)", doc_blob, re.I):
                    source["source_type"] = "ceo_letter"
                elif re.search(r"\b(presentation|slides?)\b", doc_blob, re.I):
                    source["source_type"] = "earnings_presentation"
                elif re.search(r"(earnings[_ ]release|press[_ ]release|pressrelease|earnings release|earningspressrelea|release_q|release)", doc_blob, re.I):
                    source["source_type"] = "earnings_release"
                else:
                    source["source_type"] = "filing_text"
            return source

        def _promise_text_local(rec: Dict[str, Any]) -> str:
            candidates: List[str] = []
            for key in ("statement", "promise_text", "evidence_snippet"):
                txt = glx_normalize_text(str(rec.get(key) or ""))
                if txt:
                    candidates.append(txt)
            if not candidates:
                return ""
            candidates = sorted(set(candidates), key=lambda z: (len(z), z))
            return candidates[-1]

        def _promise_note_summary_local(label: str, txt: str) -> str:
            detail_summary = _pbi_detail_preserving_note_summary_local(label, txt)
            if detail_summary:
                return detail_summary
            contextual_summary = _pbi_contextual_note_summary_local(label, q_raw, txt)
            if contextual_summary:
                return contextual_summary
            if label == "Capital allocation / buyback":
                has_buyback = bool(re.search(r"\b(repurchas\w*|buyback|authorization|remaining capacity)\b", txt, re.I))
                has_debt = bool(re.search(r"\b(reduc(?:ed|ing)? principal debt|debt reduction|deleverag|repaid|repayment)\b", txt, re.I))
                if has_buyback and has_debt:
                    return _ensure_terminal_period("Share repurchase and principal debt reduction in Q4")
                if has_buyback:
                    return ""
            if label in {"Deleveraging / liquidity", "Debt reduction"} and re.search(
                r"\b(revolv(?:er)?|credit agreement|refinanc|term loan|notes due|redeemed|repaid|principal debt)\b",
                txt,
                re.I,
            ):
                return _ensure_terminal_period("Revolver and debt position improved")
            if label == "Adjusted EBIT / margin" and re.search(
                r"\b(sendtech|presort)\b", txt, re.I
            ) and re.search(r"\b(margin|operating expenses declined|opex declined|pricing|mix)\b", txt, re.I):
                return _ensure_terminal_period("SendTech and Presort margin/opex improved")
            if label == "FCF improvement" and re.search(r"\b(fcf|free cash flow)\b", txt, re.I):
                return _ensure_terminal_period("Free cash flow improved")
            return ""

        p_local = _promises_view(quarter_mode="date")
        if not isinstance(p_local, pd.DataFrame) or p_local.empty:
            return []

        rescue_rows: List[Dict[str, Any]] = []
        quarter_col = _resolve_col(p_local, ["created_quarter", "first_seen_quarter", "quarter", "last_seen_quarter"])
        for rec in p_local.to_dict("records"):
            q_raw = None
            if quarter_col:
                q_ts = pd.to_datetime(rec.get(quarter_col), errors="coerce")
                if pd.notna(q_ts):
                    q_raw = pd.Timestamp(q_ts).to_period("Q").end_time.date()
            if q_raw not in quarters:
                continue
            txt = _promise_text_local(rec)
            if not txt:
                continue
            source = _promise_source_meta_local(rec)
            source_type = str(source.get("source_type") or "").lower()
            metric_hint = str(rec.get("metric") or rec.get("metric_display") or rec.get("metric_tag") or "").strip()
            label = _classify_pbi_metric_label(" | ".join([metric_hint, txt]), metric_hint)
            candidate_labels = [
                lab
                for lab in _pbi_extra_note_labels_local(metric_hint, txt, label)
                if lab in {
                    "Adjusted EBIT / margin",
                    "FCF improvement",
                    "PB Bank liquidity release",
                    "Deleveraging / liquidity",
                    "Debt reduction",
                    "SendTech / Presort operating driver",
                    "Capital allocation / buyback",
                }
            ]
            if not candidate_labels:
                continue
            rescue_role, rescue_drop = shared_classify_statement_evidence_role(
                txt,
                source_type=source_type,
                metric_hint=" | ".join([metric_hint, label]),
                target_period_norm=str(rec.get("target_period_norm") or rec.get("target_time") or ""),
                promise_type=str(rec.get("promise_type") or ""),
            )
            if rescue_role not in {"later_evidence", "result_evidence", "broad_note_only"}:
                continue
            capital_structure_source = bool(
                re.search(r"\b(revolver|credit|debt|covenant|refi|refinanc|notes due|term loan)\b", source_type, re.I)
                or re.search(r"\b(revolver|credit|debt|covenant|refi|refinanc|notes due|term loan)\b", txt, re.I)
            )
            if not _is_preferred_narrative_source(source_type) and not (
                label in {"Deleveraging / liquidity", "Debt reduction", "Capital allocation / buyback"}
                and capital_structure_source
            ):
                continue
            if rescue_drop in {"boilerplate", "scaffolding", "table_fragment"}:
                continue
            for label_local in candidate_labels:
                compact_note = _promise_note_summary_local(label_local, txt)
                if rescue_drop == "fragmentary_text" and not compact_note:
                    continue
                if not compact_note and not _is_pbi_clean_sentence(txt):
                    continue
                bucket = (
                    "Cash / liquidity / leverage"
                    if label_local in {"PB Bank liquidity release", "Deleveraging / liquidity", "Debt reduction", "Capital allocation / buyback"}
                    else "Better / worse vs prior"
                )
                render_summary = compact_note or qn_compact_snippet(txt, 140)
                rescue_rows.append(
                    {
                        "quarter": q_raw,
                        "bucket": bucket,
                        "text_full": txt,
                        "comment_full_text": txt,
                        "score": float(rec.get("confidence") == "high") * 10.0 + 88.0,
                        "candidate_type": "pbi_promise_note_rescue",
                        "metric_tag": metric_hint,
                        "metric_canon": metric_hint,
                        "_metric_display": label_local,
                        "_pbi_compact_note": compact_note,
                        "_render_summary": render_summary,
                        "_detail_score": _pbi_note_detail_score_local(render_summary),
                        "note_id": str(rec.get("promise_id") or hashlib.sha1(f"{q_raw}|pbi_promise_note|{label_local}|{txt}".encode("utf-8")).hexdigest()[:12]),
                        "source": source,
                        "evidence_role": rescue_role,
                        "drop_reason": rescue_drop,
                    }
                )
        return rescue_rows

    def pbi_source_note_rescue_rows(self) -> List[Dict[str, Any]]:
        _infer_doc_quarter_local = self.deps._infer_doc_quarter_local
        _iter_quarter_scoped_material_texts_local = self.deps._iter_quarter_scoped_material_texts_local
        _management_text_windows_local = self.deps._management_text_windows_local
        _narrative_text_matches_current_company_local = self.deps._narrative_text_matches_current_company_local
        _pattern_match_windows_local = self.deps._pattern_match_windows_local
        _pbi_contextual_note_summary_local = self.deps._pbi_contextual_note_summary_local
        _pbi_detail_preserving_note_summary_local = self.deps._pbi_detail_preserving_note_summary_local
        _pbi_explicit_note_split_variants_local = self.deps._pbi_explicit_note_split_variants_local
        _pbi_is_locked_capital_allocation_summary_local = self.deps._pbi_is_locked_capital_allocation_summary_local
        _pbi_note_detail_score_local = self.deps._pbi_note_detail_score_local
        _sec_cache_html_paths_local = self.deps._sec_cache_html_paths_local
        glx_normalize_text = self.deps.glx_normalize_text
        is_pbi_profile = self.deps.is_pbi_profile
        material_roots = self.deps.material_roots
        quarters = self.deps.quarters
        ticker_cache_roots_from_base_dir = self.deps.ticker_cache_roots_from_base_dir
        if self._pbi_source_note_rescue_cache is not None:
            return [dict(x) for x in self._pbi_source_note_rescue_cache]
        if not is_pbi_profile:
            self._pbi_source_note_rescue_cache = []
            return []
        rescue_rows: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[date, str, str]] = set()
        source_patterns: List[Tuple[str, str, float, str]] = [
            (
                "SendTech / Presort operating driver",
                "Better / worse vs prior",
                98.0,
                r"\bgross margin percentage increased to\s*\d+(?:\.\d+)?%\s*from\s*\d+(?:\.\d+)?%[^.]{0,220}?(?:driven by|due to)\s+[^.]+\.",
            ),
            (
                "Adjusted EBIT / margin",
                "Better / worse vs prior",
                96.0,
                r"\badjusted ebit margins?\s+improved\s+\d+(?:\.\d+)?\s+basis points\s+year[- ]over[- ]year\s+(?:due to|driven by)\s+[^.]+\.",
            ),
            (
                "Adjusted EBIT / margin",
                "Better / worse vs prior",
                96.0,
                r"\bgross margin expanded\s+\d+(?:\.\d+)?\s+basis points[^.]{0,220}?(?:driven by|due to)\s+[^.]+\.",
            ),
            (
                "Adjusted EBIT / margin",
                "Better / worse vs prior",
                94.0,
                r"\boperating expenses declined\s+\$?\s*\d+(?:\.\d+)?\s*million[^.]{0,220}?(?:primarily from|due to)\s+[^.]+\.",
            ),
            (
                "SendTech / Presort operating driver",
                "Better / worse vs prior",
                94.0,
                r"\bhigher revenue per piece,\s*improved productivity,\s*and cost reduction initiatives drove the increase in adjusted segment ebitda and ebit\.",
            ),
            (
                "Capital allocation / buyback",
                "Cash / liquidity / leverage",
                90.0,
                r"\b(?:in addition to\s+)?(?:we\s+are\s+also\s+)?(?:increas(?:ing|ed)|raising|raised|updated)\b[^.]{0,120}?\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\b[^.]{0,80}?\b(?:to|by)\s+\$?\s*\d+(?:\.\d+)?\s*million[^.]{0,260}\.",
            ),
            (
                "Capital allocation / buyback",
                "Cash / liquidity / leverage",
                91.0,
                r"\b(?:board[^.]{0,120}?)?(?:increas(?:ing|ed)|raising|raised|updated)\b[^.]{0,120}?\b(?:share\s+repurchase|repurchase|buyback)\s+authorization\b[^.]{0,80}?\b(?:to|by)\s+\$?\s*\d+(?:\.\d+)?\s*million[^.]{0,220}?\.\s*[^.]{0,220}?\b(?:remaining capacity|capacity remaining)\b[^.]*\.",
            ),
            (
                "Capital allocation / buyback",
                "Cash / liquidity / leverage",
                88.0,
                r"\b(?:increased|raising|raised)\s+(?:our\s+)?quarterly dividend\s+from\s+\$?\s*\d+(?:\.\d+)?\s+to\s+\$?\s*\d+(?:\.\d+)?\s+per share[^.]*\.",
            ),
            (
                "Capital allocation / buyback",
                "Cash / liquidity / leverage",
                88.0,
                r"\bboard approved a regular quarterly dividend of\s+\$?\s*\d+(?:\.\d+)?\s+per share[^.]*\.",
            ),
            (
                "Capital allocation / buyback",
                "Cash / liquidity / leverage",
                88.0,
                r"\brepurchas\w*\s+\$?\s*\d+(?:\.\d+)?\s*million\s+in\s+shares?\b[^.]{0,180}?\b(?:during|in)\s+the\s+(?:first|second|third|fourth)\s+quarter\b[^.]*\.",
            ),
            (
                "Strategic milestone",
                "Programs / initiatives",
                92.0,
                r"(?=[^.]{0,320}\bstrategic review\b)(?=[^.]{0,320}\b(?:phase 2|second phase)\b)(?=[^.]{0,320}\binitiated\b)[^.]{1,360}\.",
            ),
            (
                "Strategic milestone",
                "Programs / initiatives",
                90.0,
                r"\bstrategic review\b[^.]{0,240}?\b(?:phase 2|second phase)\b[^.]{0,240}?\b(?:by the end of the second quarter|end of q2(?:\s+2026)?|q2\s+2026)\b[^.]*\.",
            ),
            (
                "Strategic milestone",
                "Programs / initiatives",
                88.0,
                r"\bstrategic review\b[^.]{0,260}?\binitial phase\b[^.]{0,260}?\b(?:internal improvements|operational and personnel enhancements)\b[^.]*\.",
            ),
            (
                "Deleveraging / liquidity",
                "Cash / liquidity / leverage",
                89.0,
                r"\bsub-?\s*\d+(?:\.\d+)?x\s+leverage(?: ratio)?\b[^.]{0,220}?\b(?:greater flexibility under (?:our )?covenants|shareholder-friendly capital allocation policy)\b[^.]*\.",
            ),
            (
                "Management framing / strategy",
                "Tone / expectations",
                88.0,
                r"\b(?:wider ranges?|disclosing wider ranges?|forecast(?:ing)?|uncertainty)\b[^.]{0,260}?\b(?:wider ranges?|uncertainty|forecast(?:ing)?)\b[^.]*\.",
            ),
            (
                "Management framing / strategy",
                "Tone / expectations",
                87.0,
                r"\b(?:results reflect|focused on)\b[^.]{0,220}?\b(?:cost management|operational execution)\b[^.]*\.",
            ),
        ]
        preferred_doc_re = re.compile(r"(earningspressrelea|earningsceoletter|earningsrelease|pressrelea|ceoletter|annualletter|shareholderletter)", re.I)
        for root in material_roots:
            for sec_cache_dir in ticker_cache_roots_from_base_dir(root):
                if not sec_cache_dir.exists() or not sec_cache_dir.is_dir():
                    continue
                for path in _sec_cache_html_paths_local(sec_cache_dir):
                    if not preferred_doc_re.search(path.name):
                        continue
                    try:
                        raw_text = html.unescape(path.read_text(encoding="utf-8", errors="ignore"))
                    except Exception:
                        continue
                    joined = glx_normalize_text(raw_text)
                    if not joined:
                        continue
                    if not _narrative_text_matches_current_company_local(path, joined):
                        continue
                    q_raw = _infer_doc_quarter_local(path, joined)
                    if q_raw not in quarters or q_raw.year < 2025:
                        continue
                    source_type = (
                        "ceo_letter"
                        if re.search(r"(ceoletter|annualletter|shareholderletter)", path.name, re.I)
                        else "earnings_release"
                    )
                    for label_local, bucket_local, base_score_local, pattern_local in source_patterns:
                        for match in re.finditer(pattern_local, joined, re.I):
                            snippet = glx_normalize_text(match.group(0))
                            if not snippet:
                                continue
                            compact_note = _pbi_detail_preserving_note_summary_local(label_local, snippet, q_raw)
                            if not compact_note:
                                compact_note = _pbi_contextual_note_summary_local(label_local, q_raw, snippet)
                            if not compact_note:
                                continue
                            split_variants = _pbi_explicit_note_split_variants_local(label_local, snippet, q_raw)
                            variant_payloads = (
                                split_variants
                                if split_variants
                                else [{"subject_variant": "", "summary": compact_note}]
                            )
                            for variant in variant_payloads:
                                variant_summary = str(variant.get("summary") or "").strip()
                                if not variant_summary:
                                    continue
                                variant_focus = str(variant.get("subject_variant") or "").strip()
                                variant_theme_scope_key = str(variant.get("theme_scope_key") or "").strip()
                                variant_metric_key = f"{label_local}|{variant_focus}" if variant_focus else label_local
                                dedup_key = (q_raw, label_local, variant_summary.lower())
                                if dedup_key in seen_keys:
                                    continue
                                seen_keys.add(dedup_key)
                                rescue_rows.append(
                                    {
                                        "quarter": q_raw,
                                        "bucket": bucket_local,
                                        "text_full": snippet,
                                        "comment_full_text": snippet,
                                        "score": base_score_local,
                                        "candidate_type": "pbi_source_note_rescue",
                                        "metric_tag": variant_metric_key,
                                        "metric_canon": variant_metric_key,
                                        "_metric_display": label_local,
                                        "_pbi_compact_note": variant_summary,
                                        "_render_summary": variant_summary,
                                        "_detail_score": _pbi_note_detail_score_local(variant_summary),
                                        "_split_focus": variant_focus,
                                        "_render_summary_locked": _pbi_is_locked_capital_allocation_summary_local(variant_summary),
                                        "_force_note_passthrough": _pbi_is_locked_capital_allocation_summary_local(variant_summary),
                                        "_theme_scope_key": variant_theme_scope_key,
                                        "note_id": hashlib.sha1(
                                            f"{q_raw}|pbi_source_note_rescue|{label_local}|{variant_summary}".encode("utf-8")
                                        ).hexdigest()[:12],
                                        "source": {
                                        "source_type": source_type,
                                        "doc": str(path),
                                        "form": "8-K",
                                    },
                                }
                            )
        for q_raw, source_type, path_in, joined in _iter_quarter_scoped_material_texts_local(
            [("CEO letters", "ceo_letter"), ("earnings_release", "earnings_release")],
            min_year=2025,
        ):
            for label_local, bucket_local, base_score_local, pattern_local in source_patterns:
                for snippet in _pattern_match_windows_local(joined, pattern_local):
                    compact_note = _pbi_detail_preserving_note_summary_local(label_local, snippet, q_raw)
                    if not compact_note:
                        compact_note = _pbi_contextual_note_summary_local(label_local, q_raw, snippet)
                    if not compact_note:
                        continue
                    split_variants = _pbi_explicit_note_split_variants_local(label_local, snippet, q_raw)
                    variant_payloads = (
                        split_variants
                        if split_variants
                        else [{"subject_variant": "", "summary": compact_note}]
                    )
                    for variant in variant_payloads:
                        variant_summary = str(variant.get("summary") or "").strip()
                        if not variant_summary:
                            continue
                        variant_focus = str(variant.get("subject_variant") or "").strip()
                        variant_theme_scope_key = str(variant.get("theme_scope_key") or "").strip()
                        variant_metric_key = f"{label_local}|{variant_focus}" if variant_focus else label_local
                        dedup_key = (q_raw, label_local, variant_summary.lower())
                        if dedup_key in seen_keys:
                            continue
                        seen_keys.add(dedup_key)
                        rescue_rows.append(
                            {
                                "quarter": q_raw,
                                "bucket": bucket_local,
                                "text_full": snippet,
                                "comment_full_text": snippet,
                                "score": base_score_local,
                                "candidate_type": "pbi_source_note_rescue",
                                "metric_tag": variant_metric_key,
                                "metric_canon": variant_metric_key,
                                "_metric_display": label_local,
                                "_pbi_compact_note": variant_summary,
                                "_render_summary": variant_summary,
                                "_detail_score": _pbi_note_detail_score_local(variant_summary),
                                "_split_focus": variant_focus,
                                "_render_summary_locked": _pbi_is_locked_capital_allocation_summary_local(variant_summary),
                                "_force_note_passthrough": _pbi_is_locked_capital_allocation_summary_local(variant_summary),
                                "_theme_scope_key": variant_theme_scope_key,
                                "note_id": hashlib.sha1(
                                    f"{q_raw}|pbi_source_note_rescue|{label_local}|{variant_summary}".encode("utf-8")
                                ).hexdigest()[:12],
                                "source": {
                                    "source_type": source_type,
                                    "doc": str(path_in),
                                    "form": "8-K",
                                },
                            }
                        )
            for snippet in _management_text_windows_local(joined, max_sentences=2):
                for label_local, bucket_local, base_score_local, pattern_local in source_patterns:
                    if not re.search(pattern_local, snippet, re.I):
                        continue
                    compact_note = _pbi_detail_preserving_note_summary_local(label_local, snippet, q_raw)
                    if not compact_note:
                        compact_note = _pbi_contextual_note_summary_local(label_local, q_raw, snippet)
                    if not compact_note:
                        continue
                    split_variants = _pbi_explicit_note_split_variants_local(label_local, snippet, q_raw)
                    variant_payloads = (
                        split_variants
                        if split_variants
                        else [{"subject_variant": "", "summary": compact_note}]
                    )
                    for variant in variant_payloads:
                        variant_summary = str(variant.get("summary") or "").strip()
                        if not variant_summary:
                            continue
                        variant_focus = str(variant.get("subject_variant") or "").strip()
                        variant_theme_scope_key = str(variant.get("theme_scope_key") or "").strip()
                        variant_metric_key = f"{label_local}|{variant_focus}" if variant_focus else label_local
                        dedup_key = (q_raw, label_local, variant_summary.lower())
                        if dedup_key in seen_keys:
                            continue
                        seen_keys.add(dedup_key)
                        rescue_rows.append(
                            {
                                "quarter": q_raw,
                                "bucket": bucket_local,
                                "text_full": snippet,
                                "comment_full_text": snippet,
                                "score": base_score_local,
                                "candidate_type": "pbi_source_note_rescue",
                                "metric_tag": variant_metric_key,
                                "metric_canon": variant_metric_key,
                                "_metric_display": label_local,
                                "_pbi_compact_note": variant_summary,
                                "_render_summary": variant_summary,
                                "_detail_score": _pbi_note_detail_score_local(variant_summary),
                                "_split_focus": variant_focus,
                                "_render_summary_locked": _pbi_is_locked_capital_allocation_summary_local(variant_summary),
                                "_force_note_passthrough": _pbi_is_locked_capital_allocation_summary_local(variant_summary),
                                "_theme_scope_key": variant_theme_scope_key,
                                "note_id": hashlib.sha1(
                                    f"{q_raw}|pbi_source_note_rescue|{label_local}|{variant_summary}".encode("utf-8")
                                ).hexdigest()[:12],
                                "source": {
                                    "source_type": source_type,
                                    "doc": str(path_in),
                                    "form": "8-K",
                                },
                            }
                        )
        self._pbi_source_note_rescue_cache = rescue_rows
        return [dict(x) for x in rescue_rows]
