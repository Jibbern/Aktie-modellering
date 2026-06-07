"""Quarter Notes UI audit trace, canonicalization, and registration lifecycle."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class QuarterNotesUiAuditDeps:
    enabled: bool
    compact_mode: bool
    normalize_text: Callable[..., str]
    collapse_repeated_leading_ngram: Callable[..., str]
    dedupe_canonical_text_parts: Callable[..., List[str]]


@dataclass(frozen=True)
class QuarterNotesUiAuditSnapshot:
    raw_rows: List[Dict[str, Any]]
    canonical_rows: List[Dict[str, Any]]


class QuarterNotesUiAuditTrace:
    def __init__(self, deps: QuarterNotesUiAuditDeps) -> None:
        self.deps = deps
        self.raw_rows: List[Dict[str, Any]] = []
        self._seen: set[Tuple[str, str, str, str, str]] = set()

    def family_hint(self, category_hint: Any, metric_hint: Any, text_hint: Any) -> str:
        blob = " | ".join(
            [str(category_hint or ""), str(metric_hint or ""), self.deps.normalize_text(text_hint)]
        ).lower()
        if re.search(r"\b(guidance|target|reaffirmed|tracking midpoint|tracking low end|updated target|outlook)\b", blob, re.I):
            return "guidance_target"
        if re.search(r"\b(driven by|reflecting|due to|primarily from|stemming from|result of|ahead of plan)\b", blob, re.I):
            return "explanatory_driver_note"
        if re.search(r"\b(repurchas\w*|buyback|dividend|authorization|remaining capacity|capital allocation)\b", blob, re.I):
            return "capital_allocation_detail_note"
        if re.search(r"\b(revolver|liquidity|refinanc|debt|net debt|mezzanine|covenant|leverage|repay|repayment|balance sheet)\b", blob, re.I):
            return "debt_liquidity_rationale_note"
        if re.search(r"\b(45z|monetization|commercialization|qualification|tax credit|credits?|realized value|expected value)\b", blob, re.I):
            return "monetization_commercialization_note"
        if re.search(
            r"\b(strategic review|phase 2|phase two|positive ebitda|market conditions|enhance liquidity|"
            r"non-core asset monetization|working capital improvement|forecast(?:ing)?|uncertainty|wider ranges?|"
            r"cost management|operational execution|long-term deleveraging)\b",
            blob,
            re.I,
        ):
            return "specific_management_tone_note"
        if re.search(r"\b(start-?up|startup|fully operational|online|ramping|commissioning|construction|milestone|utilization|carbon capture)\b", blob, re.I):
            return "operational_milestone"
        if re.search(r"\b(fcf|free cash flow|cash flow|ebitda|adjusted ebitda|adjusted ebit|ebit|margin|gross margin|operating expenses|opex|crush margin)\b", blob, re.I):
            return "margin_ebitda_cashflow"
        if re.search(r"\b(improved|declined|increased|reduced|expanded|compressed|up|down)\b", blob, re.I):
            return "actual_performance_change"
        return "other"

    def subject_variant_hint(self, family_hint: str, text_hint: Any, metric_hint: Any = "") -> str:
        blob = " | ".join([str(metric_hint or ""), self.deps.normalize_text(text_hint)]).lower()
        if family_hint == "capital_allocation_detail_note":
            if re.search(r"\bquarterly dividend\b", blob, re.I):
                return "dividend_policy"
            if re.search(r"\b(authoriz|remaining capacity|capacity remaining)\b", blob, re.I):
                return "authorization_capacity"
            if re.search(r"\b(repurchas\w*|bought back|buyback)\b", blob, re.I):
                return "buyback_execution"
        if family_hint == "debt_liquidity_rationale_note":
            if re.search(r"\brevolver|availability\b", blob, re.I):
                return "revolver_liquidity"
            if re.search(r"\b(refinanc|convertible|convert)\b", blob, re.I):
                return "refinancing"
            if re.search(r"\b(covenant|leverage)\b", blob, re.I):
                return "covenant_leverage"
            if re.search(r"\b(repay|repaid|reduction|debt)\b", blob, re.I):
                return "debt_repayment"
        if family_hint == "specific_management_tone_note":
            if re.search(r"\bstrategic review|phase 2|phase two\b", blob, re.I):
                return "strategic_review"
            if re.search(r"\bforecast(?:ing)?|uncertainty|wider ranges?\b", blob, re.I):
                return "forecasting_uncertainty"
            if re.search(r"\bcost management|operational execution|ahead of plan|positive ebitda\b", blob, re.I):
                return "execution_framing"
            if re.search(r"\bleverage|balance sheet|liquidity|non-core asset|working capital\b", blob, re.I):
                return "balance_sheet_framing"
        if family_hint == "monetization_commercialization_note":
            if re.search(r"\bexpected\b", blob, re.I):
                return "expected_value"
            if re.search(r"\bcontributed|realized|recorded\b", blob, re.I):
                return "realized_value"
            if re.search(r"\bqualif|remaining facilities|all eight operating ethanol plants\b", blob, re.I):
                return "qualification_readiness"
        if family_hint == "margin_ebitda_cashflow":
            if re.search(r"\bworking capital\b", blob, re.I):
                return "working_capital"
            if re.search(r"\bfcf|free cash flow\b", blob, re.I):
                return "cash_flow"
            if re.search(r"\bcrush margin|gross margin|margin\b", blob, re.I):
                return "margin"
            if re.search(r"\bebitda|ebit\b", blob, re.I):
                return "earnings"
        return family_hint

    def capital_allocation_confidence(self, text_hint: Any) -> Dict[str, Any]:
        txt = self.deps.normalize_text(text_hint)
        low = txt.lower()
        is_cap_alloc = bool(re.search(r"\b(repurchas\w*|buyback|dividend|authorization|remaining capacity|capital allocation)\b", low, re.I))
        base = {
            "scope_confidence": "",
            "amount_confidence": "",
            "share_count_confidence": "",
            "authorization_confidence": "",
            "remaining_capacity_confidence": "",
            "dividend_change_confidence": "",
            "blocking_reason": "",
        }
        if not is_cap_alloc:
            return base
        has_amount = bool(re.search(r"\$\s*\d+(?:\.\d+)?\s*(?:m|mm|million|b|bn|billion)?", txt, re.I))
        has_share_count = bool(re.search(r"\b\d+(?:\.\d+)?\s*(?:m|mm|million)?\s+shares?\b", txt, re.I))
        has_authorization = bool(re.search(r"\bauthoriz(?:ation|ed)?\b", low, re.I))
        has_remaining_capacity = bool(re.search(r"\bremaining capacity|capacity remaining\b", low, re.I))
        has_dividend_change = bool(
            re.search(r"\bdividend\b", low, re.I)
            and (
                re.search(r"\bfrom\s+\$?\d+(?:\.\d+)?\s+to\s+\$?\d+(?:\.\d+)?", low, re.I)
                or re.search(r"\bincreased(?:\s+\w+){0,5}\s+to\s+\$?\d+(?:\.\d+)?", low, re.I)
            )
        )
        if re.search(r"\b(in q[1-4]|during q[1-4]|in the (?:first|second|third|fourth) quarter|during the (?:first|second|third|fourth) quarter)\b", low, re.I):
            scope_confidence = "quarter_specific"
        elif re.search(r"\b(since starting the program|year to date|through last|to date|since the beginning)\b", low, re.I):
            scope_confidence = "cumulative"
        elif has_authorization or has_remaining_capacity or has_dividend_change:
            scope_confidence = "policy_only"
        else:
            scope_confidence = "unsafe"
        blocking_reason = ""
        if has_amount and not has_share_count and re.search(r"\b(repurchas\w*|bought back|buyback)\b", low, re.I):
            blocking_reason = "missing_share_count"
        elif scope_confidence == "unsafe":
            blocking_reason = "no_quarter_or_cumulative_anchor"
        return {
            "scope_confidence": scope_confidence,
            "amount_confidence": ("present" if has_amount else "missing"),
            "share_count_confidence": ("present" if has_share_count else "missing"),
            "authorization_confidence": ("present" if has_authorization else "missing"),
            "remaining_capacity_confidence": ("present" if has_remaining_capacity else "missing"),
            "dividend_change_confidence": ("present" if has_dividend_change else "missing"),
            "blocking_reason": blocking_reason,
        }

    def clean_excerpt(self, text_in: Any, max_chars: int = 320) -> str:
        txt = self.deps.normalize_text(str(text_in or ""))
        if not txt:
            return ""

        def _collapse_repeats_local(raw_txt: str) -> str:
            cleaned = self.deps.normalize_text(raw_txt)
            if not cleaned:
                return ""
            for phrase_len in range(6, 1, -1):
                pattern = re.compile(rf"\b((?:\S+\s+){{{phrase_len - 1}}}\S+)(?:\s+\1\b)+", re.I)
                prev_val = None
                while prev_val != cleaned:
                    prev_val = cleaned
                    cleaned = pattern.sub(r"\1", cleaned)
            return cleaned

        parts: List[str] = []
        seen_parts: set[str] = set()
        for raw_part in re.split(r"\s*\|\s*", txt):
            part = _collapse_repeats_local(str(raw_part or ""))
            if not part:
                continue
            words = part.split()
            max_window = min(12, len(words) // 2)
            for size in range(max_window, 1, -1):
                lhs = " ".join(words[:size]).strip()
                rhs = " ".join(words[size : size * 2]).strip()
                if lhs and rhs and lhs.lower() == rhs.lower():
                    rest = " ".join(words[size * 2 :]).strip()
                    part = f"{lhs} {rest}".strip() if rest else lhs
                    break
            part_key = part.lower()
            if part_key in seen_parts:
                continue
            if any(part_key in existing or existing in part_key for existing in seen_parts):
                continue
            seen_parts.add(part_key)
            parts.append(part)
        txt = _collapse_repeats_local(" | ".join(parts) if parts else txt)
        if len(txt) <= max_chars:
            return txt
        kept_parts: List[str] = []
        cur_len = 0
        for part in parts:
            projected = cur_len + (3 if kept_parts else 0) + len(part)
            if projected > max_chars:
                break
            kept_parts.append(part)
            cur_len = projected
        if kept_parts:
            trimmed = " | ".join(kept_parts).rstrip(" ,;:-")
            return trimmed + ("..." if len(trimmed) < len(txt) else "")
        cut = txt[:max_chars]
        ws_cut = cut.rfind(" ")
        if ws_cut >= int(max_chars * 0.6):
            cut = cut[:ws_cut]
        return cut.rstrip(" ,;:-") + "..."

    def canonicalize_excerpt(self, text_in: Any) -> str:
        txt = self.clean_excerpt(text_in, max_chars=1200)
        if not txt:
            return ""
        txt = re.sub(r"\s+", " ", txt).strip(" |")
        txt = re.sub(r"\s*([|,:;])\s*", r" \1 ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        parts = self.deps.dedupe_canonical_text_parts(re.split(r"\s*\|\s*", txt))
        txt = " | ".join(parts) if parts else txt
        prior_txt = None
        while txt and txt != prior_txt:
            prior_txt = txt
            txt = re.sub(
                r"\b([A-Za-z][A-Za-z0-9/%$().,\- ]{3,90}?)\s+\1\b",
                r"\1",
                txt,
                flags=re.I,
            )
            txt = self.deps.collapse_repeated_leading_ngram(txt)
        parts = self.deps.dedupe_canonical_text_parts(re.split(r"\s*\|\s*", txt))
        txt = " | ".join(parts) if parts else txt
        return self.deps.normalize_text(txt)

    def doc_family(self, source_doc_in: Any) -> str:
        source_doc = str(source_doc_in or "").strip()
        if not source_doc:
            return ""
        try:
            name = Path(source_doc).name
        except Exception:
            name = source_doc.replace("\\", "/").split("/")[-1]
        name = self.deps.normalize_text(name).lower()
        name = re.sub(r"\.[a-z0-9]+$", "", name)
        name = re.sub(r"^doc_\d+_", "", name)
        name = re.sub(r"[-_]+", "_", name)
        return name

    def canonical_rows(self, rows_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows_in:
            return []

        def _safe_score_local(value_in: Any) -> float:
            score_val = pd.to_numeric(value_in, errors="coerce")
            if pd.isna(score_val):
                return 0.0
            try:
                return float(score_val)
            except Exception:
                return 0.0

        def _canonical_source_group_local(row_in: Dict[str, Any]) -> str:
            source_type = str(row_in.get("source_type") or "").strip().lower()
            idea_label = self.deps.normalize_text(
                str(
                    row_in.get("idea_label")
                    or row_in.get("metric_display")
                    or row_in.get("family")
                    or row_in.get("candidate_type")
                    or ""
                )
            ).lower()
            if source_type == "model_metric":
                return f"model_metric:{idea_label or 'unknown'}"
            doc_family = self.doc_family(row_in.get("source_doc"))
            if doc_family:
                return f"{source_type or 'source'}:{doc_family}"
            return f"{source_type or 'source'}:unknown"

        def _stage_priority_local(stage_in: Any) -> int:
            stage_low = str(stage_in or "").strip().lower()
            stage_rank = {
                "saved_workbook_visible": 90,
                "selection_kept": 80,
                "quality_filtered": 70,
                "routed_to_bucket": 60,
                "score_assigned": 50,
                "render_summary_generated": 40,
                "candidate_created": 30,
                "source_detected": 20,
                "quality_review": 10,
            }
            return stage_rank.get(stage_low, 0)

        grouped: Dict[Tuple[str, str, str, str], List[Tuple[int, Dict[str, Any]]]] = {}
        for idx, raw_row in enumerate(rows_in):
            row = dict(raw_row)
            row["source_excerpt"] = self.canonicalize_excerpt(row.get("source_excerpt"))
            row["normalized_source_doc_family"] = self.doc_family(row.get("source_doc"))
            row["canonical_source_group"] = _canonical_source_group_local(row)
            idea_label = self.deps.normalize_text(
                str(row.get("idea_label") or row.get("metric_display") or row.get("family") or "")
            ).lower()
            key = (
                str(row.get("quarter") or ""),
                idea_label,
                str(row.get("canonical_source_group") or ""),
            )
            grouped.setdefault(key, []).append((idx, row))

        rows_out: List[Tuple[int, Dict[str, Any]]] = []
        for _, group_rows in grouped.items():
            _, best_row = max(
                group_rows,
                key=lambda pair: (
                    _stage_priority_local(pair[1].get("stage")),
                    _safe_score_local(pair[1].get("score_total")),
                    len(str(pair[1].get("final_summary") or "")),
                    -pair[0],
                ),
            )
            group_only_rows = [dict(row) for _, row in group_rows]
            merged = dict(best_row)
            merged["support_count"] = len(group_only_rows)
            merged["source_count"] = len(
                {
                    str(row.get("source_doc") or row.get("normalized_source_doc_family") or row.get("canonical_source_group") or "")
                    for row in group_only_rows
                }
            )
            if not str(merged.get("final_summary") or "").strip():
                for row in group_only_rows:
                    final_summary = str(row.get("final_summary") or "").strip()
                    if final_summary:
                        merged["final_summary"] = final_summary
                        break
            rows_out.append((min(idx for idx, _ in group_rows), merged))

        rows_out.sort(key=lambda pair: pair[0])
        return [row for _, row in rows_out]

    def trace_id(
        self,
        quarter_hint: Any,
        source_doc: Any,
        source_excerpt: Any,
        family_hint: str,
        metric_hint: Any = "",
    ) -> str:
        trace_key = "|".join(
            [
                str(pd.to_datetime(quarter_hint, errors="coerce").date() if pd.notna(pd.to_datetime(quarter_hint, errors="coerce")) else quarter_hint or ""),
                self.deps.normalize_text(source_doc),
                self.deps.normalize_text(source_excerpt),
                str(family_hint or "").strip().lower(),
            ]
        )
        return hashlib.sha1(trace_key.encode("utf-8")).hexdigest()[:16]

    def attrition_class(self, stage: str, dropped_reason: str = "") -> str:
        stage_low = str(stage or "").strip().lower()
        reason_low = str(dropped_reason or "").strip().lower()
        if stage_low == "saved_workbook_missing" or reason_low == "export_provenance_mismatch":
            return "export mismatch"
        if stage_low in {"deduped_out", "theme_collapsed"}:
            return "rendered-text dedupe" if "dedupe" in reason_low or "duplicate" in reason_low else "winner selection"
        if stage_low == "selection_lost":
            return "winner selection"
        if stage_low in {"quality_filtered", "profile_filtered"}:
            if any(tok in reason_low for tok in ("bucket", "category", "segment_cap", "corporate_cap", "metric_cap")):
                return "category separation"
            if "score" in reason_low:
                return "candidate scoring"
            return "candidate generation"
        if stage_low == "routed_to_bucket":
            return "routing"
        if stage_low in {"candidate_created", "render_summary_generated", "score_assigned"}:
            return "candidate generation"
        return ""

    def emit(self, **payload: Any) -> None:
        if not self.deps.enabled:
            return
        row = dict(payload)
        quarter_val = row.get("quarter")
        quarter_txt = ""
        q_ts = pd.to_datetime(quarter_val, errors="coerce")
        if pd.notna(q_ts):
            quarter_txt = str(q_ts.date())
        elif quarter_val not in (None, ""):
            quarter_txt = str(quarter_val)
        row["quarter"] = quarter_txt
        row.setdefault("trace_id", "")
        row.setdefault("stage", "")
        row.setdefault("dropped_reason", "")
        row.setdefault("final_summary", "")
        row.setdefault("lost_to_trace_id", "")
        row.setdefault("merged_into_trace_id", "")
        row["source_excerpt"] = self.clean_excerpt(row.get("source_excerpt"))
        if not row.get("attrition_class"):
            row["attrition_class"] = self.attrition_class(row.get("stage", ""), row.get("dropped_reason", ""))
        dedupe_key = (
            str(row.get("quarter") or ""),
            str(row.get("trace_id") or ""),
            str(row.get("stage") or ""),
            str(row.get("dropped_reason") or ""),
            str(row.get("final_summary") or ""),
        )
        if dedupe_key in self._seen:
            return
        self._seen.add(dedupe_key)
        self.raw_rows.append(row)

    def ensure_trace_id(self, item: Dict[str, Any], qd_override: Optional[date] = None) -> str:
        trace_id = str(item.get("trace_id") or "").strip()
        if trace_id:
            return trace_id
        source_meta = dict(item.get("source") or {})
        text_hint = self.deps.normalize_text(
            item.get("comment_full_text")
            or item.get("text_full")
            or item.get("evidence_snippet")
            or item.get("_render_summary")
            or item.get("_pbi_compact_note")
            or ""
        )
        family_hint = self.family_hint(
            item.get("bucket") or item.get("category") or "",
            item.get("_metric_display") or item.get("metric_canon") or item.get("metric_tag") or "",
            text_hint,
        )
        trace_id = self.trace_id(
            qd_override or item.get("quarter") or item.get("source_quarter_end") or "",
            source_meta.get("doc") or item.get("doc") or "",
            text_hint,
            family_hint,
            item.get("_metric_display") or item.get("metric_canon") or item.get("metric_tag") or "",
        )
        item["trace_id"] = trace_id
        return trace_id

    def register_existing(self, item: Dict[str, Any], qd_override: Optional[date] = None) -> None:
        if not self.deps.enabled or bool(item.get("_audit_candidate_registered")):
            return
        trace_id = self.ensure_trace_id(item, qd_override)
        source_meta = dict(item.get("source") or {})
        excerpt = self.deps.normalize_text(
            item.get("comment_full_text")
            or item.get("text_full")
            or item.get("evidence_snippet")
            or item.get("_render_summary")
            or item.get("_pbi_compact_note")
            or ""
        )
        family_hint = self.family_hint(
            item.get("bucket") or item.get("category") or "",
            item.get("_metric_display") or item.get("metric_canon") or item.get("metric_tag") or "",
            excerpt,
        )
        subject_variant = self.subject_variant_hint(
            family_hint,
            excerpt,
            item.get("_metric_display") or item.get("metric_canon") or item.get("metric_tag") or "",
        )
        score_components = json.dumps(
            {
                "candidate_type": str(item.get("candidate_type") or ""),
                "preferred_source": bool(item.get("_preferred_source")),
                "priority_hits": int(item.get("_priority_hits") or 0),
                "high_signal": bool(item.get("_high_signal_note")),
            },
            sort_keys=True,
        ) if not self.deps.compact_mode else ""
        conf = self.capital_allocation_confidence(excerpt)
        common = {
            "quarter": qd_override or item.get("quarter") or item.get("source_quarter_end") or "",
            "trace_id": trace_id,
            "source_type": str(source_meta.get("source_type") or item.get("source_type") or ""),
            "source_doc": str(source_meta.get("doc") or item.get("doc") or ""),
            "source_excerpt": excerpt,
            "idea_label": str(item.get("_metric_display") or item.get("metric_canon") or item.get("metric_tag") or ""),
            "candidate_type": str(item.get("candidate_type") or ""),
            "family": family_hint,
            "subject_variant": subject_variant,
            "bucket": str(item.get("bucket") or ""),
            "metric_display": str(item.get("_metric_display") or item.get("metric_canon") or item.get("metric_tag") or ""),
            "score_total": float(item.get("_event_score") or item.get("score") or 0.0),
            "score_components": score_components,
            "final_summary": self.deps.normalize_text(str(item.get("_render_summary") or item.get("_pbi_compact_note") or excerpt)),
            **conf,
        }
        self.emit(stage="candidate_created", **common)
        if not self.deps.compact_mode:
            self.emit(stage="source_detected", **common)
            self.emit(stage="routed_to_bucket", **common)
            self.emit(stage="render_summary_generated", **common)
            self.emit(stage="score_assigned", **common)
        item["_audit_candidate_registered"] = True

    def snapshot(self) -> QuarterNotesUiAuditSnapshot:
        raw_rows = list(self.raw_rows)
        return QuarterNotesUiAuditSnapshot(
            raw_rows=raw_rows,
            canonical_rows=self.canonical_rows(raw_rows),
        )
