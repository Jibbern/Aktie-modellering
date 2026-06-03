"""Promise Progress follow-through resolution/model helpers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .filing_evidence_shared import (
    build_follow_through_signal as shared_build_follow_through_signal,
    derive_lifecycle_state as shared_derive_lifecycle_state,
    derive_status_resolution_reason as shared_derive_status_resolution_reason,
    merge_follow_through_signals as shared_merge_follow_through_signals,
)
from .guidance_lexicon import normalize_text as glx_normalize_text
from .quarter_notes_lexicon import compact_snippet as qn_compact_snippet


@dataclass(frozen=True)
class PromiseProgressFollowthroughDeps:
    is_pbi_profile: bool
    is_gpre_profile: bool
    evaluation_as_of: Optional[date]
    quarters: Sequence[date]
    progress_records: List[Dict[str, Any]]
    tracker_rows_map: Dict[date, List[Dict[str, Any]]]
    quarter_note_rows_map: Dict[date, List[Dict[str, Any]]]
    progress_columns: Dict[str, Optional[str]]
    milestone_progress_re: Any
    milestone_completion_re: Any
    source_rank: Callable[..., Any]
    candidate_quality_key: Callable[..., Any]
    qend: Callable[..., Any]
    q_label: Callable[..., str]
    parse_dollar_amount: Callable[..., Any]
    coerce_amount_with_unit: Callable[..., Any]
    coerce_amount_with_unit_local: Callable[..., Any]
    fmt_short_money_value: Callable[..., str]
    fmt_short_money_value_local: Callable[..., str]
    nearest_amount_for_pattern: Callable[..., Any]
    extract_progress_latest_basis: Callable[..., str]
    evidence_time_label: Callable[..., str]
    extract_45z_realized_progress_text: Callable[..., str]
    extract_45z_monetization_target_display: Callable[..., str]
    split_target_family_key: Callable[..., str]
    split_target_metric_display: Callable[..., str]
    split_target_qend: Callable[..., Any]
    split_target_scope_token: Callable[..., str]
    split_target_scope_is_broad: Callable[..., bool]
    derive_split_target_meta: Callable[..., Any]
    infer_target_period: Callable[..., Dict[str, Any]]
    infer_target_structure: Callable[..., Dict[str, Any]]
    target_period_is_closed: Callable[..., bool]
    management_theme_key: Callable[..., str]
    actual_for_guidance: Callable[..., Any]
    guidance_period_end: Callable[..., Any]
    load_local_cost_savings_follow_candidates: Callable[[], List[Dict[str, Any]]]
    load_local_45z_closed_period_outcome: Callable[[Optional[date]], Dict[str, Any]]
    load_profile_slide_signals: Callable[[], List[Dict[str, Any]]]
    progress_metric_from_event: Callable[..., str]
    progress_metric_from_qnote: Callable[..., str]
    progress_target_display_from_qnote: Callable[..., str]


@dataclass(frozen=True)
class PromiseProgressFollowthroughResult:
    rows_by_quarter: Dict[date, List[Dict[str, Any]]]
    resolved_count: int
    harmonized_count: int


class PromiseProgressFollowthroughModel:
    def __init__(self, deps: PromiseProgressFollowthroughDeps) -> None:
        self.deps = deps

    def _progress_context_key(self, metric_name: str, text_in: str, promise_type: Any = "") -> str:
        blob = glx_normalize_text(" ".join([str(metric_name or ""), str(promise_type or ""), str(text_in or "")]))
        low = blob.lower()
        if re.search(r"\b(cost savings|cost reduction|expense reduction|annualized savings|reorganization)\b", low, re.I):
            return "cost_savings_program"
        if re.search(r"\b(all (?:eight|nine) operating ethanol plants|qualify for production tax credits|45z.*qualif)\b", low, re.I):
            return "45z_plant_qualification"
        if (
            re.search(r"\b(45z|production tax credits?)\b", low, re.I)
            and re.search(r"\b(monetization|agreement executed|ebitda|opportunity)\b", low, re.I)
        ) or (
            "advantage nebraska" in low
            and re.search(r"\b(ebitda|opportunity|credits?|tax credit|generation)\b", low, re.I)
        ):
            return "45z_monetization"
        if "york" in low or "tallgrass trailblazer" in low:
            return "york_operational"
        if re.search(r"\b(central city|wood river)\b", low, re.I) or (
            re.search(r"\bcapture volumes?\b", low, re.I)
            and re.search(r"\b(online|ramping|commissioning)\b", low, re.I)
            and "york" not in low
        ):
            return "central_city_wood_river"
        if re.search(r"\b(class vi|construction management agreements?|ordered major equipment|construction progressing|start-?up)\b", low, re.I):
            return "construction_permit"
        if re.search(r"\b(advantage nebraska|carbon capture|sequestering co2)\b", low, re.I):
            return "advantage_platform"
        if re.search(r"\b(obion|junior mezz|debt reduction|deleverag|repaid?|balance sheet improvement)\b", low, re.I):
            return "debt_reduction"
        return ""

    def _follow_through_family_key(self, theme_key: str) -> str:
        d = self.deps
        _split_target_family_key = d.split_target_family_key
        return _split_target_family_key(theme_key)

    def _format_with_time(self, label: str, text_in: str, qd: Optional[date]) -> str:
        d = self.deps
        _evidence_time_label = d.evidence_time_label
        txt = str(label or "").strip()
        if not txt:
            return ""
        if re.search(r"\((?:Q[1-4]\s+20\d{2}|[A-Z][a-z]{2}\s+20\d{2})\)$", txt):
            return txt
        time_lbl = _evidence_time_label(text_in, qd)
        return f"{txt} ({time_lbl})" if time_lbl else txt

    def _parse_annualized_savings_follow_through(self, text_in: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        d = self.deps
        _coerce_amount_with_unit = d.coerce_amount_with_unit
        txt = glx_normalize_text(text_in)
        if not txt:
            return None, None, None
        realized_amt: Optional[float] = None
        addl_amt: Optional[float] = None
        accomplished_amt: Optional[float] = None
        for m in re.finditer(
            r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
            txt,
            re.I,
        ):
            amt = _coerce_amount_with_unit(m.group(1), m.group(2))
            if amt is None:
                continue
            start = max(0, m.start() - 70)
            end = min(len(txt), m.end() + 70)
            window = txt[start:end].lower()
            if re.search(r"\b(another|additional|incremental|unlocked)\b", window, re.I):
                addl_amt = max(addl_amt or 0.0, float(amt))
            elif re.search(r"\b(accomplished|achieved)\b", window, re.I) and re.search(r"\b(annualized|annualised)\b", window, re.I):
                accomplished_amt = max(accomplished_amt or 0.0, float(amt))
            elif re.search(r"\b(surpass(?:ed|ing)?|exceed(?:ed|ing)?|ahead of plan|above plan|on pace to exceed)\b", window, re.I) and re.search(r"\b(cost reduction|cost savings|annualized)\b", window, re.I):
                accomplished_amt = max(accomplished_amt or 0.0, float(amt))
            elif re.search(r"\b(realized|realised|implemented|executed on|already achieved|already done)\b", window, re.I):
                    realized_amt = max(realized_amt or 0.0, float(amt))
        return realized_amt, addl_amt, accomplished_amt

    def _period_label_for_meta(self, period_meta: Dict[str, Any]) -> str:
        period_type = str(period_meta.get("target_period_type") or "")
        label = str(period_meta.get("target_period_label") or "").strip()
        if label:
            return label
        norm = str(period_meta.get("target_period_norm") or "").strip()
        if period_type == "quarter":
            m_q = re.fullmatch(r"Q(20\d{2})Q([1-4])", norm, re.I)
            if m_q:
                return f"Q{int(m_q.group(2))} {int(m_q.group(1))}"
        if period_type == "year":
            m_fy = re.fullmatch(r"FY(20\d{2})", norm, re.I)
            if m_fy:
                return f"FY {int(m_fy.group(1))}"
        return norm

    def _infer_target_numeric_spec(self, text_in: Any) -> Dict[str, Any]:
        d = self.deps
        _coerce_amount_with_unit_local = d.coerce_amount_with_unit_local
        txt = glx_normalize_text(str(text_in or ""))
        if not txt:
            return {"kind": "", "low": None, "high": None, "value": None, "approx": False}
        low = txt.lower()
        approx = bool(re.search(r"\b(?:about|approximately|approx\.?|around|~)\b", low, re.I))
        m_range = re.search(
            r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|mm|mi|bn|b)?\s*(?:to|-)\s*"
            r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|mm|mi|bn|b)?",
            txt,
            re.I,
        )
        if m_range:
            lo = _coerce_amount_with_unit_local(m_range.group(1), m_range.group(2) or m_range.group(4))
            hi = _coerce_amount_with_unit_local(m_range.group(3), m_range.group(4) or m_range.group(2))
            if lo is not None and hi is not None:
                low_v = float(min(lo, hi))
                high_v = float(max(lo, hi))
                return {"kind": "range", "low": low_v, "high": high_v, "value": None, "approx": approx}
        m_cmp = re.search(
            r"\b(>=|>|<=|<|at least|more than|greater than|less than|up to)\s*\$?\s*"
            r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|mm|mi|bn|b)?",
            txt,
            re.I,
        )
        if m_cmp:
            val = _coerce_amount_with_unit_local(m_cmp.group(2), m_cmp.group(3))
            if val is not None:
                cmp_tok = str(m_cmp.group(1) or "").lower()
                kind = {
                    ">=": "gte",
                    "at least": "gte",
                    ">": "gt",
                    "more than": "gt",
                    "greater than": "gt",
                    "<=": "lte",
                    "<": "lt",
                    "less than": "lt",
                    "up to": "lte",
                }.get(cmp_tok, "point")
                return {"kind": kind, "low": None, "high": None, "value": float(val), "approx": approx}
        m_plain = re.search(
            r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|mm|mi|bn|b)\b",
            txt,
            re.I,
        )
        if m_plain and re.search(r"\b(target|guidance|expected|expect|opportunity|goal|program)\b", low, re.I):
            val = _coerce_amount_with_unit_local(m_plain.group(1), m_plain.group(2))
            if val is not None:
                return {"kind": "point", "low": None, "high": None, "value": float(val), "approx": approx}
        return {"kind": "", "low": None, "high": None, "value": None, "approx": approx}

    def _candidate_has_actual_language(self, text_in: Any) -> bool:
        txt = glx_normalize_text(str(text_in or "")).lower()
        if not txt:
            return False
        if re.search(r"\b(target|guidance|expected|expect|opportunity)\b", txt, re.I) and not re.search(
            r"\b(realized|realised|recorded|recognized|generated|accomplished|achieved|implemented|delivered|actual)\b",
            txt,
            re.I,
        ):
            return False
        return bool(
            re.search(
                r"\b(realized|realised|recorded|recognized|generated|accomplished|achieved|implemented|actual|came in at|was)\b",
                txt,
                re.I,
            )
        )

    def _extract_numeric_outcome_evidence(self, metric_name: str, text_in: Any, qd_hint: Optional[date] = None) -> Dict[str, Any]:
        d = self.deps
        _parse_dollar_amount = d.parse_dollar_amount
        _fmt_short_money_value = d.fmt_short_money_value
        _nearest_amount_for_pattern = d.nearest_amount_for_pattern
        _extract_progress_latest_basis = d.extract_progress_latest_basis
        _extract_45z_realized_progress_text = d.extract_45z_realized_progress_text
        _format_with_time = self._format_with_time
        _parse_annualized_savings_follow_through = self._parse_annualized_savings_follow_through
        _candidate_has_actual_language = self._candidate_has_actual_language
        _latest_basis_strength = self._latest_basis_strength
        txt = glx_normalize_text(str(text_in or ""))
        if not txt:
            return {"value": None, "latest": "", "quality": 0}
        metric_low = str(metric_name or "").strip().lower()
        if metric_low == "cost savings":
            realized_amt, addl_amt, accomplished_amt = _parse_annualized_savings_follow_through(txt)
            goal_amt = _nearest_amount_for_pattern(
                txt,
                r"\b(overall|full|program|goal|target|annualized program)\b",
            )
            ahead_words = bool(
                re.search(
                    r"\b(surpass(?:ed|ing)?|exceed(?:ed|ing)?|ahead of plan|above plan|on pace to exceed)\b",
                    txt,
                    re.I,
                )
            )
            if goal_amt is None and realized_amt is not None and addl_amt is not None:
                goal_amt = float(realized_amt) + float(addl_amt)
            if ahead_words and goal_amt is not None:
                latest_txt = _format_with_time(
                    f"On pace to exceed {_fmt_short_money_value(goal_amt)} target",
                    txt,
                    qd_hint,
                )
                return {
                    "value": float(goal_amt),
                    "latest": latest_txt,
                    "quality": 5,
                    "status_hint": "ahead_of_plan",
                    "realized_amt": realized_amt,
                    "increment_amt": addl_amt,
                    "program_progress_amt": accomplished_amt if accomplished_amt is not None else goal_amt,
                }
            if accomplished_amt is not None:
                return {
                    "value": float(accomplished_amt),
                    "latest": _format_with_time(f"~{_fmt_short_money_value(accomplished_amt)} annualized accomplished", txt, qd_hint),
                    "quality": 5,
                    "realized_amt": realized_amt,
                    "increment_amt": addl_amt,
                    "program_progress_amt": accomplished_amt,
                }
            if realized_amt is not None:
                return {
                    "value": float(realized_amt),
                    "latest": _format_with_time(f"{_fmt_short_money_value(realized_amt)} realized", txt, qd_hint),
                    "quality": 4,
                    "realized_amt": realized_amt,
                    "increment_amt": addl_amt,
                    "program_progress_amt": float(realized_amt + addl_amt) if addl_amt is not None else realized_amt,
                }
        realized_45z = _extract_45z_realized_progress_text(txt, qd_hint)
        if realized_45z:
            return {
                "value": _parse_dollar_amount(realized_45z),
                "latest": realized_45z,
                "quality": _latest_basis_strength(realized_45z),
            }
        if _candidate_has_actual_language(txt):
            amt = _parse_dollar_amount(txt)
            if amt is not None:
                label = _extract_progress_latest_basis(metric_name, txt) or _format_with_time(f"{_fmt_short_money_value(amt)} realized", txt, qd_hint)
                return {"value": float(amt), "latest": label, "quality": _latest_basis_strength(label)}
        label = _extract_progress_latest_basis(metric_name, txt)
        if label:
            return {"value": _parse_dollar_amount(label), "latest": label, "quality": _latest_basis_strength(label)}
        return {"value": None, "latest": "", "quality": 0}

    def _period_match_score(self, period_meta: Dict[str, Any], cand: Dict[str, Any], row_qd: Optional[date], final_only: bool = False) -> int:
        _period_label_for_meta = self._period_label_for_meta
        _candidate_has_actual_language = self._candidate_has_actual_language
        period_type = str(period_meta.get("target_period_type") or "")
        period_end = period_meta.get("target_period_end")
        period_norm = str(period_meta.get("target_period_norm") or "").strip()
        period_label = _period_label_for_meta(period_meta)
        cand_q = cand.get("quarter") if isinstance(cand.get("quarter"), date) else None
        cand_blob = glx_normalize_text(
            " | ".join(
                [
                    str(cand.get("target") or ""),
                    str(cand.get("latest") or ""),
                    str(cand.get("text") or ""),
                ]
            )
        )
        low = cand_blob.lower()
        if period_type in {"", "open_ended"}:
            return 1
        if period_type == "milestone":
            return 1
        if not isinstance(cand_q, date):
            return 0
        if isinstance(row_qd, date) and cand_q < row_qd:
            return 0
        explicit_hit = False
        if period_type == "quarter":
            m_q = re.fullmatch(r"Q(20\d{2})Q([1-4])", period_norm, re.I)
            if m_q:
                yy = int(m_q.group(1))
                qq = int(m_q.group(2))
                quarter_words = {1: "first", 2: "second", 3: "third", 4: "fourth"}
                explicit_hit = bool(
                    re.search(rf"\bq{qq}\s*{yy}\b", low, re.I)
                    or re.search(rf"\b{quarter_words.get(qq, '')}\s+quarter(?:\s+of)?\s*{yy}?\b", low, re.I)
                )
        elif period_type == "year":
            year_match = re.search(r"(20\d{2})", period_label or period_norm)
            if year_match:
                yy = int(year_match.group(1))
                explicit_hit = bool(
                    re.search(rf"\b(?:fy|full year|fiscal year)\s*{yy}\b", low, re.I)
                    or re.search(rf"\b(?:in|for|through)\s+{yy}\b", low, re.I)
                )
        elif period_type == "half":
            m_half = re.search(r"H([12])\s*(20\d{2})", f"{period_label} {period_norm}", re.I)
            if m_half:
                hh = int(m_half.group(1))
                yy = int(m_half.group(2))
                half_words = {1: "first", 2: "second"}
                explicit_hit = bool(
                    re.search(rf"\bh{hh}\s*{yy}\b", low, re.I)
                    or re.search(rf"\b{half_words.get(hh, '')}\s+half(?:\s+of)?\s*{yy}?\b", low, re.I)
                )
        if final_only:
            if not isinstance(period_end, date):
                return 0
            if cand_q < period_end:
                return 0
            if explicit_hit:
                return 3
            if cand_q == period_end:
                return 2
            return 1 if _candidate_has_actual_language(cand_blob) else 0
        if isinstance(period_end, date):
            if cand_q < period_end:
                return 2 if explicit_hit else 1
            if cand_q == period_end:
                return 3 if explicit_hit else 2
            if cand_q > period_end:
                return 2 if explicit_hit else 1
        return 1 if explicit_hit else 0

    def _target_structure_match_rank(self, item_local: Dict[str, Any], cand_local: Dict[str, Any]) -> int:
        item_program = str(item_local.get("program_key") or "").strip()
        cand_program = str(cand_local.get("program_key") or "").strip()
        item_role = str(item_local.get("target_structure_role") or item_local.get("stage_kind") or item_local.get("target_structure_kind") or "single").strip().lower()
        cand_role = str(cand_local.get("target_structure_role") or cand_local.get("stage_kind") or cand_local.get("target_structure_kind") or "single").strip().lower()
        if item_program and cand_program and item_program != cand_program:
            return 0
        if item_role == cand_role:
            return 3
        if item_role == "program_total" and cand_role in {"single", "stage", "first_tranche", "initial", "additional_tranche", "phase_1", "phase_2", "remaining"}:
            return 1
        if cand_role == "program_total" and item_role in {"single", "stage", "first_tranche", "initial", "additional_tranche", "phase_1", "phase_2", "remaining"}:
            return 1
        if item_role == "single" or cand_role == "single":
            return 1
        return 0

    def _follow_scope_match_rank(self, item_local: Dict[str, Any], cand_local: Dict[str, Any]) -> int:
        d = self.deps
        _split_target_scope_token = d.split_target_scope_token
        _split_target_scope_is_broad = d.split_target_scope_is_broad
        item_scope_key = _split_target_scope_token(item_local)
        cand_scope_key = _split_target_scope_token(cand_local)
        cand_text = str(cand_local.get("text") or "")
        if item_scope_key == cand_scope_key:
            return 3
        if item_scope_key == "company_total":
            return 2 if _split_target_scope_is_broad(cand_local, cand_text) else 0
        if _split_target_scope_is_broad(cand_local, cand_text):
            return 1
        return 0

    def _follow_context_match_rank(self, item_local: Dict[str, Any], cand_local: Dict[str, Any]) -> int:
        _progress_context_key = self._progress_context_key
        item_ctx = str(
            item_local.get("context_key")
            or _progress_context_key(
                item_local.get("metric_ref") or item_local.get("metric") or "",
                " | ".join(
                    [
                        str(item_local.get("target") or ""),
                        str(item_local.get("latest") or ""),
                        str(item_local.get("rationale") or item_local.get("text") or ""),
                        str(item_local.get("_source_snip") or ""),
                    ]
                ),
                item_local.get("promise_type"),
            )
            or ""
        ).strip().lower()
        cand_ctx = str(
            cand_local.get("context_key")
            or _progress_context_key(
                cand_local.get("metric_ref") or cand_local.get("metric") or "",
                " | ".join(
                    [
                        str(cand_local.get("target") or ""),
                        str(cand_local.get("latest") or ""),
                        str(cand_local.get("text") or cand_local.get("rationale") or ""),
                    ]
                ),
                cand_local.get("promise_type"),
            )
            or ""
        ).strip().lower()
        if not item_ctx or not cand_ctx:
            return 1
        if item_ctx == cand_ctx:
            return 3
        milestone_ctx = {"construction_permit", "york_operational", "central_city_wood_river", "advantage_platform"}
        if item_ctx in milestone_ctx and cand_ctx in milestone_ctx:
            if item_ctx == "advantage_platform" or cand_ctx == "advantage_platform":
                return 2
            return 0
        if item_ctx in {"45z_monetization", "45z_plant_qualification"} and cand_ctx in {"45z_monetization", "45z_plant_qualification"}:
            return 2
        if item_ctx == "45z_2026_ebitda" and cand_ctx in {"45z_monetization", "45z_plant_qualification", "advantage_platform"}:
            return 1
        if cand_ctx == "45z_2026_ebitda" and item_ctx in {"45z_monetization", "45z_plant_qualification"}:
            return 1
        return 0

    def _find_later_matching_outcome(self, 
        row: Dict[str, Any],
        later_candidates: List[Dict[str, Any]],
        eval_q: Optional[date],
        target_period: Dict[str, Any],
        target_structure: Dict[str, Any],
    ) -> Dict[str, Any]:
        d = self.deps
        _source_rank = d.source_rank
        _split_target_qend = d.split_target_qend
        _target_period_is_closed = d.target_period_is_closed
        _load_local_45z_closed_period_outcome = d.load_local_45z_closed_period_outcome
        _follow_through_family_key = self._follow_through_family_key
        _infer_target_numeric_spec = self._infer_target_numeric_spec
        _candidate_has_actual_language = self._candidate_has_actual_language
        _extract_numeric_outcome_evidence = self._extract_numeric_outcome_evidence
        _period_match_score = self._period_match_score
        _target_structure_match_rank = self._target_structure_match_rank
        _follow_scope_match_rank = self._follow_scope_match_rank
        _follow_context_match_rank = self._follow_context_match_rank
        _follow_through_theme_key = self._follow_through_theme_key
        _latest_basis_strength = self._latest_basis_strength
        target_spec = _infer_target_numeric_spec(row.get("target"))
        requires_numeric_final = bool(target_spec.get("kind"))
        row_qd = _split_target_qend(
            row.get("first_seen_evidence_quarter_end")
            or row.get("first_seen_quarter_end")
            or row.get("quarter")
        )
        progress_matches: List[Tuple[Tuple[int, int, int, int, int], Dict[str, Any], Dict[str, Any]]] = []
        final_matches: List[Tuple[Tuple[int, int, int, int, int], Dict[str, Any], Dict[str, Any]]] = []
        for cand in later_candidates:
            if not isinstance(cand, dict):
                continue
            scope_rank = _follow_scope_match_rank(row, cand)
            context_rank = _follow_context_match_rank(row, cand)
            struct_rank = _target_structure_match_rank(row, cand)
            if scope_rank <= 0 or context_rank <= 0 or struct_rank <= 0:
                continue
            evidence = _extract_numeric_outcome_evidence(
                str(row.get("metric_ref") or ""),
                " | ".join([str(cand.get("latest") or ""), str(cand.get("text") or "")]),
                cand.get("quarter") if isinstance(cand.get("quarter"), date) else None,
            )
            progress_rank = _period_match_score(target_period, cand, row_qd, final_only=False)
            final_rank = _period_match_score(target_period, cand, row_qd, final_only=True)
            base_key = (
                scope_rank,
                context_rank,
                struct_rank,
                int(evidence.get("quality") or 0),
                -_source_rank(cand.get("source_type"), cand.get("source_doc")),
                int(cand.get("quarter").toordinal()) if isinstance(cand.get("quarter"), date) else 0,
            )
            if progress_rank > 0:
                progress_matches.append(((progress_rank,) + base_key, cand, evidence))
            final_ok = evidence.get("value") is not None
            if (
                not final_ok
                and not requires_numeric_final
                and _candidate_has_actual_language(cand.get("text"))
            ):
                final_ok = True
            if final_rank > 0 and final_ok:
                final_matches.append(((final_rank,) + base_key, cand, evidence))
        final_matches.sort(key=lambda z: z[0], reverse=True)
        progress_matches.sort(key=lambda z: z[0], reverse=True)
        row_metric_low = str(row.get("metric_ref") or "").strip().lower()
        row_family_key = str(
            row.get("family_key")
            or _follow_through_family_key(
                _follow_through_theme_key(
                    row.get("metric_ref") or "",
                    row.get("promise_key") or "",
                    row.get("promise_type") or "",
                    " | ".join(
                        [
                            str(row.get("target") or ""),
                            str(row.get("latest") or ""),
                            str(row.get("rationale") or ""),
                            str(row.get("_source_snip") or ""),
                        ]
                    ),
                    row.get("target") or "",
                )
            )
            or ""
        ).strip().lower()
        if (
            row_family_key.startswith("45z")
            or "45z" in row_metric_low
        ) and str(target_period.get("target_period_type") or "").strip().lower() == "quarter":
            local_outcome = _load_local_45z_closed_period_outcome(target_period.get("target_period_end"))
            if local_outcome and local_outcome.get("value") is not None:
                local_q = local_outcome.get("quarter") if isinstance(local_outcome.get("quarter"), date) else target_period.get("target_period_end")
                synth_cand = {
                    "quarter": local_q,
                    "metric_ref": row.get("metric_ref") or "",
                    "promise_type": row.get("promise_type") or "",
                    "promise_key": row.get("promise_key") or "",
                    "theme_key": row.get("theme_key") or "",
                    "family_key": row_family_key,
                    "context_key": row.get("context_key") or "",
                    "text": str(local_outcome.get("text") or ""),
                    "source_type": str(local_outcome.get("source_type") or "financial_statement"),
                    "source_doc": str(local_outcome.get("source_doc") or ""),
                    "target": row.get("target") or "",
                    "latest": str(local_outcome.get("latest") or ""),
                }
                synth_evidence = {
                    "value": float(local_outcome.get("value") or 0.0),
                    "latest": str(local_outcome.get("latest") or ""),
                    "quality": int(local_outcome.get("quality") or _latest_basis_strength(str(local_outcome.get("latest") or ""))),
                }
                synth_rank = (
                    4,
                    3,
                    3,
                    3,
                    int(synth_evidence.get("quality") or 0),
                    -_source_rank(synth_cand.get("source_type"), synth_cand.get("source_doc")),
                    int(local_q.toordinal()) if isinstance(local_q, date) else 0,
                )
                final_matches.append((synth_rank, synth_cand, synth_evidence))
                progress_matches.append((synth_rank, synth_cand, synth_evidence))
                final_matches.sort(key=lambda z: z[0], reverse=True)
                progress_matches.sort(key=lambda z: z[0], reverse=True)
        best_final = final_matches[0] if final_matches else None
        best_progress = progress_matches[0] if progress_matches else None
        latest_txt = ""
        if best_final and str(best_final[2].get("latest") or "").strip():
            latest_txt = str(best_final[2].get("latest") or "").strip()
        elif best_progress and str(best_progress[2].get("latest") or "").strip():
            latest_txt = str(best_progress[2].get("latest") or "").strip()
        return {
            "target_period_closed": _target_period_is_closed(target_period, eval_q),
            "best_final_candidate": best_final[1] if best_final else None,
            "best_progress_candidate": best_progress[1] if best_progress else None,
            "final_evidence": best_final[2] if best_final else {},
            "progress_evidence": best_progress[2] if best_progress else {},
            "final_structure_rank": best_final[0][3] if best_final else 0,
            "progress_structure_rank": best_progress[0][3] if best_progress else 0,
            "latest": latest_txt,
        }

    def _classify_progress_status(self, 
        row: Dict[str, Any],
        latest_evidence: Dict[str, Any],
        target_period: Dict[str, Any],
        target_structure: Dict[str, Any],
    ) -> str:
        _infer_target_numeric_spec = self._infer_target_numeric_spec
        metric_low = str(row.get("metric_ref") or "").strip().lower()
        promise_type_low = str(row.get("promise_type") or "").strip().lower()
        target_spec = _infer_target_numeric_spec(row.get("target"))
        final_evidence = dict(latest_evidence.get("final_evidence") or {})
        progress_evidence = dict(latest_evidence.get("progress_evidence") or {})
        latest_txt = str(latest_evidence.get("latest") or row.get("latest") or "").strip()
        evidence_blob = glx_normalize_text(
            " | ".join(
                [
                    str(row.get("rationale") or ""),
                    str(row.get("_source_snip") or ""),
                    latest_txt,
                    str(dict(latest_evidence.get("best_final_candidate") or {}).get("text") or ""),
                    str(dict(latest_evidence.get("best_progress_candidate") or {}).get("text") or ""),
                ]
            )
        )
        completion_hit = bool(
            re.search(
                r"\b(fully operational|fully online|sale completed|achieved|closed|repaid|repayment completed|used to fully repay)\b",
                evidence_blob,
                re.I,
            )
        )
        progress_hit = bool(
            re.search(
                r"\b(on track|ahead of plan|on pace to exceed|ramping|commissioning|progressing|agreement executed|permit received|ordered major equipment|realized|accomplished|implemented|delivered)\b",
                evidence_blob,
                re.I,
            )
        )
        stage_role = str(target_structure.get("target_structure_role") or target_structure.get("stage_kind") or "").strip().lower()
        stage_target_amt = target_structure.get("stage_amount")
        final_value = final_evidence.get("value")
        progress_value = progress_evidence.get("value")
        final_structure_rank = int(latest_evidence.get("final_structure_rank") or 0)
        progress_structure_rank = int(latest_evidence.get("progress_structure_rank") or 0)
        status_hint = str(
            final_evidence.get("status_hint")
            or progress_evidence.get("status_hint")
            or ""
        ).strip().lower()
        period_closed = bool(latest_evidence.get("target_period_closed"))
        has_numeric_target = bool(target_spec.get("kind"))

        if stage_role in {"first_tranche", "initial", "additional_tranche", "phase_1", "phase_2", "remaining"} and stage_target_amt is not None:
            observed_val = None
            explicit_stage_completion = False
            if metric_low == "cost savings":
                if stage_role in {"first_tranche", "initial", "phase_1"}:
                    observed_val = final_evidence.get("realized_amt") if final_evidence.get("realized_amt") is not None else progress_evidence.get("realized_amt")
                    explicit_stage_completion = bool(
                        re.search(r"\b(realized|realised|implemented|accomplished|achieved)\b", evidence_blob, re.I)
                        and re.search(r"\b(cost reduction|cost savings|annualized)\b", evidence_blob, re.I)
                    )
                else:
                    observed_val = None
                    explicit_stage_completion = bool(
                        re.search(r"\b(additional|phase 2|second phase|remaining)\b", evidence_blob, re.I)
                        and re.search(r"\b(realized|realised|implemented|accomplished|achieved)\b", evidence_blob, re.I)
                    )
            else:
                observed_val = final_value if final_value is not None else progress_value
                explicit_stage_completion = progress_structure_rank >= 2 or final_structure_rank >= 2
            if (
                observed_val is not None
                and float(observed_val) >= float(stage_target_amt) * 0.995
                and explicit_stage_completion
            ):
                return "completed"

        if has_numeric_target and final_value is not None and period_closed:
            kind = str(target_spec.get("kind") or "")
            approx = bool(target_spec.get("approx"))
            tol_mult = 0.05 if approx else 0.01
            val = float(final_value)
            if kind == "range":
                lo = float(target_spec.get("low") or 0.0)
                hi = float(target_spec.get("high") or 0.0)
                if val < lo - abs(lo) * tol_mult:
                    return "resolved_fail"
                if val > hi + abs(hi) * tol_mult:
                    return "resolved_beat"
                return "resolved_pass"
            if kind in {"gte", "gt", "point", "lte", "lt"}:
                tgt = float(target_spec.get("value") or 0.0)
                tol = max(1e-9, abs(tgt) * tol_mult)
                if kind == "gte":
                    return "resolved_pass" if val >= tgt - tol else "resolved_fail"
                if kind == "gt":
                    if val > tgt + tol:
                        return "resolved_beat"
                    if val >= tgt - tol:
                        return "resolved_pass"
                    return "resolved_fail"
                if kind == "lte":
                    return "resolved_pass" if val <= tgt + tol else "resolved_fail"
                if kind == "lt":
                    return "resolved_pass" if val < tgt + tol else "resolved_fail"
                if abs(val - tgt) <= tol:
                    return "resolved_pass"
                return "resolved_beat" if val > tgt + tol else "resolved_fail"

        if status_hint == "ahead_of_plan":
            if stage_role in {"first_tranche", "initial", "additional_tranche", "phase_1", "phase_2", "remaining"}:
                return "on_track"
            return "ahead_of_plan"
        ahead_words = bool(re.search(r"\b(ahead of plan|ahead of target|on pace to exceed|tracking above|above plan|exceed(?:ing)? target)\b", evidence_blob, re.I))
        progress_value_for_target = progress_value if progress_value is not None else final_value
        if metric_low == "cost savings":
            if final_evidence.get("program_progress_amt") is not None:
                progress_value_for_target = final_evidence.get("program_progress_amt")
            elif progress_evidence.get("program_progress_amt") is not None:
                progress_value_for_target = progress_evidence.get("program_progress_amt")
        if has_numeric_target and progress_value_for_target is not None and not period_closed:
            kind = str(target_spec.get("kind") or "")
            tgt = target_spec.get("value")
            if kind == "range":
                hi = float(target_spec.get("high") or 0.0)
                if float(progress_value_for_target) > hi:
                    return "ahead_of_plan"
            elif kind in {"gte", "gt", "point"} and tgt is not None and float(progress_value_for_target) >= float(tgt):
                return "ahead_of_plan"
        if ahead_words:
            return "ahead_of_plan"

        if has_numeric_target:
            if latest_txt.lower() == "not yet measurable":
                return "not_observed"
            if progress_hit or latest_txt:
                return "on_track"
            return "not_observed"

        if promise_type_low == "milestone" or metric_low == "strategic milestone":
            if completion_hit:
                return "completed"
            if progress_hit or latest_txt and latest_txt.lower() != "not yet measurable":
                return "in_progress"
            return "not_observed"

        if latest_txt.lower() == "not yet measurable":
            return "not_observed"
        if progress_hit or latest_txt:
            if metric_low in {"cost savings", "45z monetization / ebitda", "45z adjusted ebitda / monetization", "debt reduction"}:
                return "on_track"
            return "in_progress" if promise_type_low == "milestone" else "on_track"
        return "not_observed"

    def _resolve_follow_through_latest(self, 
        item: Dict[str, Any],
        related_candidates: List[Dict[str, Any]],
        eval_q: Optional[date],
    ) -> str:
        d = self.deps
        _qend = d.qend
        _parse_dollar_amount = d.parse_dollar_amount
        _fmt_short_money_value = d.fmt_short_money_value
        _extract_progress_latest_basis = d.extract_progress_latest_basis
        _evidence_time_label = d.evidence_time_label
        _extract_45z_realized_progress_text = d.extract_45z_realized_progress_text
        _progress_context_key = self._progress_context_key
        _format_with_time = self._format_with_time
        _parse_annualized_savings_follow_through = self._parse_annualized_savings_follow_through
        _latest_basis_strength = self._latest_basis_strength
        _follow_candidate_sort_key = self._follow_candidate_sort_key
        metric_txt = str(item.get("metric_ref") or "")
        base_text = glx_normalize_text(" | ".join([str(item.get("rationale") or ""), str(item.get("_source_snip") or "")]))
        context_key = _progress_context_key(metric_txt, base_text, item.get("promise_type"))
        item.pop("_resolved_latest_quarter_end", None)
        if not related_candidates:
            return str(item.get("latest") or "") or _extract_progress_latest_basis(metric_txt, base_text) or "not yet measurable"

        same_q = pd.to_datetime(item.get("first_seen_evidence_quarter_end") or item.get("first_seen_quarter_end"), errors="coerce")
        same_q_date = same_q.date() if pd.notna(same_q) else None
        sorted_cands = sorted(related_candidates, key=_follow_candidate_sort_key)

        def _remember_qd(qd_in: Optional[date]) -> None:
            if isinstance(qd_in, date):
                item["_resolved_latest_quarter_end"] = str(qd_in)

        def _max_qd(*qd_vals: Any) -> Optional[date]:
            qds = [qd for qd in qd_vals if isinstance(qd, date)]
            return max(qds) if qds else None

        def _first_cand(pattern: str, *, theme_prefix: str = "", include_base: bool = True) -> Optional[Dict[str, Any]]:
            for cand in sorted_cands:
                txt = str(cand.get("text") or "")
                if not include_base and txt == base_text:
                    continue
                if theme_prefix and not str(cand.get("theme_key") or "").startswith(theme_prefix):
                    continue
                if re.search(pattern, txt, re.I):
                    return cand
            return None

        def _collect_follow_through_signals() -> List[Any]:
            signal_metric_hint = " | ".join(
                [
                    metric_txt,
                    str(item.get("target") or ""),
                    str(item.get("promise_type") or ""),
                ]
            )
            raw_signals: List[Any] = []
            source_type = str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui")
            if base_text:
                base_signal = shared_build_follow_through_signal(
                    base_text,
                    source_type=source_type,
                    metric_hint=signal_metric_hint,
                    theme_hint=str(item.get("metric_display") or item.get("metric_ref") or ""),
                    base_score=float(item.get("_score") or 0.0),
                    period_norm=str(item.get("target_period_norm") or ""),
                    source_doc=str(item.get("_source_doc") or ""),
                    display_text_hint=str(item.get("statement_summary") or ""),
                    quarter_end=str(same_q_date or ""),
                )
                if base_signal is not None:
                    raw_signals.append(base_signal)
            for cand in sorted_cands:
                cand_text = glx_normalize_text(str(cand.get("text") or ""))
                if not cand_text:
                    continue
                cand_signal = shared_build_follow_through_signal(
                    cand_text,
                    source_type=str(cand.get("source_type") or source_type),
                    metric_hint=signal_metric_hint,
                    theme_hint=str(cand.get("theme_key") or item.get("metric_display") or ""),
                    base_score=float(cand.get("score") or 0.0),
                    period_norm=str(item.get("target_period_norm") or ""),
                    source_doc=str(cand.get("source_doc") or cand.get("doc") or ""),
                    display_text_hint=str(cand.get("summary") or ""),
                    quarter_end=str(cand.get("quarter") or ""),
                )
                if cand_signal is not None:
                    raw_signals.append(cand_signal)
            return shared_merge_follow_through_signals(raw_signals, hard_cap=max(4, len(raw_signals))) if raw_signals else []

        def _best_generic_follow_signal(signals: List[Any]) -> Optional[Any]:
            if not signals:
                return None
            preferred_event_types = {
                "deleveraging",
                "liquidity_release",
                "cost_savings",
                "milestone",
                "regulatory_credit",
                "segment_driver",
                "margin_improvement",
                "fcf_improvement",
            }
            preferred_metric_families = {
                "debt",
                "liquidity",
                "cost_savings",
                "milestone",
                "regulatory_credit",
                "segment_ops",
                "adj_ebit",
                "fcf",
            }
            def _rank(signal: Any) -> Tuple[int, float]:
                return (
                    1
                    if str(signal.event_type or "") in preferred_event_types
                    or str(signal.metric_family or "") in preferred_metric_families
                    else 0,
                    float(signal.display_score or 0.0),
                )
            return sorted(signals, key=_rank, reverse=True)[0]

        merged_signals = _collect_follow_through_signals()

        if context_key == "cost_savings_program":
            best_accomplished: Optional[Tuple[float, date]] = None
            best_realized: Optional[Tuple[float, date]] = None
            for cand in sorted_cands:
                qd_c = cand.get("quarter") if isinstance(cand.get("quarter"), date) else None
                realized_amt, addl_amt, accomplished_amt = _parse_annualized_savings_follow_through(str(cand.get("text") or ""))
                if accomplished_amt is not None:
                    cand_pair = (float(accomplished_amt), qd_c or date.max)
                    if (
                        best_accomplished is None
                        or cand_pair[0] > best_accomplished[0]
                        or (abs(cand_pair[0] - best_accomplished[0]) <= 1e-9 and cand_pair[1] < best_accomplished[1])
                    ):
                        best_accomplished = cand_pair
                if realized_amt is not None:
                    cand_pair = (float(realized_amt), qd_c or date.max)
                    if (
                        best_realized is None
                        or cand_pair[0] > best_realized[0]
                            or (abs(cand_pair[0] - best_realized[0]) <= 1e-9 and cand_pair[1] < best_realized[1])
                    ):
                        best_realized = cand_pair
            target_amt = _parse_dollar_amount(str(item.get("target") or ""))
            if target_amt is None:
                target_candidates = [
                    _parse_dollar_amount(str(cand.get("target") or ""))
                    for cand in sorted_cands
                    if str(cand.get("metric_ref") or "").strip().lower() == metric_txt.lower()
                ]
                target_candidates = [x for x in target_candidates if x is not None]
                if target_candidates:
                    target_amt = max(target_candidates)
            if best_accomplished is not None and best_accomplished[0] > 0:
                prefix = "~" if target_amt and best_accomplished[0] < target_amt else ""
                _remember_qd(best_accomplished[1])
                return _format_with_time(f"{prefix}{_fmt_short_money_value(best_accomplished[0])} annualized accomplished", "", best_accomplished[1])
            if best_realized is not None and best_realized[0] > 0:
                _remember_qd(best_realized[1])
                return _format_with_time(f"{_fmt_short_money_value(best_realized[0])} realized", "", best_realized[1])

        generic_signal = _best_generic_follow_signal(merged_signals)
        if generic_signal is not None and context_key not in {"45z_monetization", "45z_plant_qualification"}:
            signal_qd = _qend(getattr(generic_signal, "quarter_end", ""))
            if isinstance(signal_qd, date):
                _remember_qd(signal_qd)
            if str(generic_signal.event_type or "") in {
                "deleveraging",
                "liquidity_release",
                "milestone",
                "segment_driver",
                "margin_improvement",
                "fcf_improvement",
            }:
                return str(generic_signal.summary or "")

        if context_key == "45z_plant_qualification":
            base_has_agreement = bool(re.search(r"\b45z\b[^.]{0,120}\bagreement executed\b|\bagreement executed\b[^.]{0,120}\b45z\b", base_text, re.I))
            base_has_ramp = bool(re.search(r"\bonline(?: and ramping)?\b|\bramping\b", base_text, re.I))
            base_has_full = bool(re.search(r"\b(fully operational|fully online)\b", base_text, re.I))
            agreement_cand = _first_cand(r"\b45z\b[^.]{0,120}\bagreement executed\b|\bagreement executed\b[^.]{0,120}\b45z\b")
            ramp_cand = _first_cand(r"\b(central city|wood river)\b[^.]{0,120}\bonline\b[^.]{0,120}\bramping\b|\bonline and ramping\b")
            full_cand = _first_cand(r"\b(fully operational|fully online)\b")
            if (agreement_cand or base_has_agreement) and (ramp_cand or base_has_ramp):
                _remember_qd(_max_qd(agreement_cand.get("quarter") if agreement_cand else None, ramp_cand.get("quarter") if ramp_cand else None, same_q_date))
                return "Nebraska systems online/ramping; 45Z agreement executed"
            if (agreement_cand or base_has_agreement) and (full_cand or base_has_full):
                _remember_qd(_max_qd(agreement_cand.get("quarter") if agreement_cand else None, full_cand.get("quarter") if full_cand else None, same_q_date))
                return "Advantage Nebraska fully operational; 45Z agreement executed"
            if agreement_cand:
                _remember_qd(agreement_cand.get("quarter"))
                return _format_with_time("45Z agreement executed", str(agreement_cand.get("text") or ""), agreement_cand.get("quarter"))
            if ramp_cand:
                _remember_qd(ramp_cand.get("quarter"))
                return "Nebraska systems online/ramping"
            if full_cand:
                _remember_qd(full_cand.get("quarter"))
                return _format_with_time("Advantage Nebraska fully operational", str(full_cand.get("text") or ""), full_cand.get("quarter"))

        if context_key == "45z_monetization":
            target_blob = glx_normalize_text(" | ".join([str(item.get("target") or ""), base_text])).lower()
            prefer_realized_basis = not re.search(r"\b2026\b", target_blob, re.I)
            realized_candidates: List[Tuple[Optional[date], str]] = []
            base_realized = _extract_45z_realized_progress_text(base_text, same_q_date)
            if base_realized:
                realized_candidates.append((same_q_date, base_realized))
            for cand in sorted_cands:
                realized_txt = _extract_45z_realized_progress_text(str(cand.get("text") or ""), cand.get("quarter"))
                if realized_txt:
                    realized_candidates.append((cand.get("quarter") if isinstance(cand.get("quarter"), date) else None, realized_txt))
            if prefer_realized_basis and realized_candidates:
                best_qd, best_realized = sorted(
                    realized_candidates,
                    key=lambda z: (
                        0 if isinstance(z[0], date) and z[0] == same_q_date else 1,
                        -(z[0].toordinal() if isinstance(z[0], date) else 0),
                        -_latest_basis_strength(z[1]),
                    ),
                )[0]
                _remember_qd(best_qd if isinstance(best_qd, date) else same_q_date)
                return best_realized
            base_has_agreement = bool(re.search(r"\b45z\b[^.]{0,120}\bagreement executed\b|\bagreement executed\b[^.]{0,120}\b45z\b", base_text, re.I))
            base_has_ramp = bool(re.search(r"\bonline(?: and ramping)?\b|\bramping\b", base_text, re.I))
            base_has_full = bool(re.search(r"\b(fully operational|fully online)\b", base_text, re.I))
            agreement_cand = _first_cand(r"\b45z\b[^.]{0,120}\bagreement executed\b|\bagreement executed\b[^.]{0,120}\b45z\b")
            ramp_cand = _first_cand(r"\bonline(?: and ramping)?\b|\bramping\b")
            full_cand = _first_cand(r"\b(fully operational|fully online)\b")
            if (agreement_cand or base_has_agreement) and (ramp_cand or base_has_ramp):
                _remember_qd(_max_qd(agreement_cand.get("quarter") if agreement_cand else None, ramp_cand.get("quarter") if ramp_cand else None, same_q_date))
                return "Nebraska systems online/ramping; 45Z agreement executed"
            if (agreement_cand or base_has_agreement) and (full_cand or base_has_full):
                _remember_qd(_max_qd(agreement_cand.get("quarter") if agreement_cand else None, full_cand.get("quarter") if full_cand else None, same_q_date))
                return "Advantage Nebraska fully operational; 45Z agreement executed"
            if full_cand:
                _remember_qd(full_cand.get("quarter"))
                return _format_with_time("Advantage Nebraska fully operational", str(full_cand.get("text") or ""), full_cand.get("quarter"))
            if base_has_full:
                _remember_qd(same_q_date)
                return _format_with_time("Advantage Nebraska fully operational", base_text, same_q_date)
            if agreement_cand:
                _remember_qd(agreement_cand.get("quarter"))
                return _format_with_time("45Z agreement executed", str(agreement_cand.get("text") or ""), agreement_cand.get("quarter"))

        if context_key == "central_city_wood_river":
            if re.search(r"\b(central city|wood river)\b", base_text, re.I) and re.search(r"\bonline\b[^.]{0,80}\bramping\b|\bonline and ramping\b", base_text, re.I):
                _remember_qd(same_q_date)
                return _format_with_time("Central City/Wood River online and ramping", base_text, same_q_date)
            ramp_cand = _first_cand(r"\b(central city|wood river)\b[^.]{0,160}\bonline\b[^.]{0,80}\bramping\b|\bonline and ramping\b")
            if ramp_cand:
                _remember_qd(ramp_cand.get("quarter"))
                return _format_with_time("Central City/Wood River online and ramping", str(ramp_cand.get("text") or ""), ramp_cand.get("quarter"))
            full_cand = _first_cand(r"\b(fully operational|fully online)\b")
            if full_cand:
                _remember_qd(full_cand.get("quarter"))
                return _format_with_time("Advantage Nebraska fully operational", str(full_cand.get("text") or ""), full_cand.get("quarter"))

        if context_key == "york_operational":
            if re.search(r"\byork\b[^|]{0,160}\b(fully operational|fully online|online)\b", base_text, re.I) or (
                "tallgrass trailblazer" in base_text.lower() and re.search(r"\b(fully operational|fully online|online)\b", base_text, re.I)
            ):
                _remember_qd(same_q_date)
                return _format_with_time("York fully operational", base_text, same_q_date)
            york_cand = _first_cand(r"\byork\b[^.]{0,120}\b(fully operational|fully online|online)\b")
            if york_cand:
                _remember_qd(york_cand.get("quarter"))
                return _format_with_time("York fully operational", str(york_cand.get("text") or ""), york_cand.get("quarter"))

        if context_key == "construction_permit":
            full_cand = _first_cand(r"\b(fully operational|fully online)\b")
            if re.search(r"\bclass vi\b", base_text, re.I):
                permit_cand = _first_cand(r"\bclass vi\b[^.]{0,120}\bpermit\b|\bpermit received\b")
                if permit_cand and full_cand:
                    _remember_qd(full_cand.get("quarter"))
                    return f"Construction advanced; later fully operational in {_evidence_time_label(str(full_cand.get('text') or ''), full_cand.get('quarter'))}"
                if permit_cand:
                    _remember_qd(permit_cand.get("quarter"))
                    return _format_with_time("Class VI permit received", str(permit_cand.get("text") or ""), permit_cand.get("quarter"))
            constr_cand = _first_cand(r"\b(construction progressing|ordered major equipment|construction management agreements?)\b")
            if constr_cand and full_cand:
                _remember_qd(full_cand.get("quarter"))
                return f"Construction advanced; later fully operational in {_evidence_time_label(str(full_cand.get('text') or ''), full_cand.get('quarter'))}"
            if re.search(r"\b(construction progressing|ordered major equipment|construction management agreements?)\b", base_text, re.I) and full_cand:
                _remember_qd(full_cand.get("quarter"))
                return f"Construction advanced; later fully operational in {_evidence_time_label(str(full_cand.get('text') or ''), full_cand.get('quarter'))}"
            if constr_cand:
                _remember_qd(constr_cand.get("quarter"))
                return _format_with_time("Construction advanced", str(constr_cand.get("text") or ""), constr_cand.get("quarter"))
            if full_cand:
                _remember_qd(full_cand.get("quarter"))
                return _format_with_time("Advantage Nebraska fully operational", str(full_cand.get("text") or ""), full_cand.get("quarter"))

        if context_key == "advantage_platform":
            full_cand = _first_cand(r"\badvantage nebraska\b[^.]{0,120}\b(fully operational|fully online)\b|\bsequestering co2\b")
            if full_cand:
                _remember_qd(full_cand.get("quarter"))
                return _format_with_time("Advantage Nebraska fully operational", str(full_cand.get("text") or ""), full_cand.get("quarter"))
            ramp_cand = _first_cand(r"\bonline(?: and ramping)?\b|\bramping\b")
            if ramp_cand:
                _remember_qd(ramp_cand.get("quarter"))
                return _format_with_time("Nebraska systems online/ramping", str(ramp_cand.get("text") or ""), ramp_cand.get("quarter"))

        basis_candidates: List[Tuple[int, str]] = []
        for cand in sorted_cands:
            basis_txt = _extract_progress_latest_basis(metric_txt, str(cand.get("text") or ""))
            if basis_txt:
                basis_candidates.append((_latest_basis_strength(basis_txt), basis_txt))
        if basis_candidates:
            basis_candidates.sort(key=lambda z: (-z[0], z[1]))
            return basis_candidates[0][1]

        return str(item.get("latest") or "") or _extract_progress_latest_basis(metric_txt, base_text) or "not yet measurable"

    def _resolve_follow_through_status(self, 
        item: Dict[str, Any],
        latest_txt: str,
        related_candidates: List[Dict[str, Any]],
    ) -> str:
        d = self.deps
        _parse_dollar_amount = d.parse_dollar_amount
        _progress_context_key = self._progress_context_key
        metric_txt = str(item.get("metric_ref") or "")
        base_text = glx_normalize_text(" | ".join([str(item.get("rationale") or ""), str(item.get("_source_snip") or "")]))
        context_key = _progress_context_key(metric_txt, base_text, item.get("promise_type"))
        evidence_blob = " | ".join([base_text] + [str(c.get("text") or "") for c in related_candidates[:6]])
        low_latest = str(latest_txt or "").strip().lower()
        target_amt = _parse_dollar_amount(str(item.get("target") or ""))
        latest_amt = _parse_dollar_amount(str(latest_txt or ""))
        signal_metric_hint = " | ".join(
            [
                metric_txt,
                str(item.get("target") or ""),
                str(item.get("promise_type") or ""),
            ]
        )
        status_signals: List[Any] = []
        for source_text, source_type, source_doc, base_score in [
            (
                base_text,
                str(item.get("_source_type") or item.get("source_type") or "promise_progress_ui"),
                str(item.get("_source_doc") or ""),
                float(item.get("_score") or 0.0),
            )
        ] + [
            (
                glx_normalize_text(str(cand.get("text") or "")),
                str(cand.get("source_type") or item.get("_source_type") or "promise_progress_ui"),
                str(cand.get("source_doc") or cand.get("doc") or ""),
                float(cand.get("score") or 0.0),
            )
            for cand in related_candidates[:8]
        ]:
            if not source_text:
                continue
            signal = shared_build_follow_through_signal(
                source_text,
                source_type=source_type,
                metric_hint=signal_metric_hint,
                theme_hint=str(item.get("metric_display") or item.get("metric_ref") or ""),
                base_score=base_score,
                period_norm=str(item.get("target_period_norm") or ""),
                source_doc=source_doc,
                display_text_hint=str(item.get("statement_summary") or ""),
            )
            if signal is not None:
                status_signals.append(signal)
        merged_status_signals = shared_merge_follow_through_signals(status_signals, hard_cap=max(4, len(status_signals))) if status_signals else []
        best_status_signal = merged_status_signals[0] if merged_status_signals else None

        if best_status_signal is not None:
            signal_type = str(best_status_signal.event_type or "")
            signal_summary = str(best_status_signal.summary or "")
            if signal_type == "deleveraging":
                if re.search(r"\b(fully repay|fully repaid|repaid|completed)\b", signal_summary, re.I):
                    return "completed"
                return "on_track"
            if signal_type in {"liquidity_release", "cost_savings", "margin_improvement", "fcf_improvement"}:
                if latest_amt is not None and target_amt is not None and latest_amt >= target_amt * 0.995:
                    return "completed"
                return "on_track"
            if signal_type in {"milestone", "segment_driver", "regulatory_credit"}:
                if re.search(r"\b(fully operational|fully online|completed|executed|repaid)\b", signal_summary, re.I):
                    return "completed"
                if re.search(r"\b(on track|online and ramping|ramping|progressing)\b", signal_summary, re.I):
                    return "on_track"

        if context_key == "45z_plant_qualification":
            if re.search(r"\b(all (?:eight|nine) operating plants?|all plants?)\b[^.]{0,120}\bqualif", evidence_blob, re.I):
                return "completed"
            if re.search(r"\b(agreement executed|online|ramping|fully operational|permit received)\b", evidence_blob, re.I):
                return "on_track"
            return "not_observed"

        if context_key == "cost_savings_program":
            if target_amt and latest_amt and latest_amt >= target_amt * 0.995:
                return "completed"
            if latest_amt or re.search(r"\b(realized|achieved|accomplished|annualized savings)\b", evidence_blob, re.I):
                return "on_track"
            return "not_observed"

        if context_key == "45z_monetization":
            if latest_amt is not None or re.search(r"\b(income tax benefit|net of discounts|recorded within income tax benefit|45z value realized)\b", evidence_blob, re.I):
                return "on_track"
            if re.search(r"\b(agreement executed|fully operational|online|ramping|permit received)\b", evidence_blob, re.I):
                return "on_track"
            return "not_observed"

        if context_key in {"construction_permit", "york_operational", "central_city_wood_river", "advantage_platform", "debt_reduction"}:
            if context_key == "central_city_wood_river":
                if re.search(r"\b(central city|wood river)\b[^|]{0,160}\bonline\b[^|]{0,120}\bramping\b|\bonline and ramping\b", evidence_blob, re.I):
                    return "in_progress"
                if re.search(r"\b(fully operational|fully online)\b", evidence_blob, re.I):
                    return "in_progress"
                return "not_observed"
            if context_key == "construction_permit":
                if re.search(r"\b(fully operational|fully online)\b", evidence_blob, re.I):
                    return "completed"
                if re.search(r"\b(construction progressing|agreement executed|permit received|ordered major equipment)\b", evidence_blob, re.I):
                    return "in_progress"
                return "not_observed"
            if re.search(r"\b(fully operational|fully online|sale completed|debt repaid|repaid|repayment completed|used to fully repay)\b", evidence_blob, re.I):
                return "completed"
            if re.search(r"\b(online|ramping|commissioning|started up|construction progressing|agreement executed|permit received|ordered major equipment)\b", evidence_blob, re.I):
                return "in_progress"
            return "not_observed"

        if low_latest == "not yet measurable":
            return "not_observed"
        return str(item.get("status") or "")

    def _follow_through_theme_key(self, 
        metric_name: str,
        promise_key: Any,
        promise_type: Any,
        text_in: str,
        target_in: Any = "",
    ) -> str:
        d = self.deps
        _management_theme_key = d.management_theme_key
        metric_low = str(metric_name or "").strip().lower()
        promise_type_low = str(promise_type or "").strip().lower()
        blob = glx_normalize_text(" ".join([str(metric_name or ""), str(promise_key or ""), str(target_in or ""), str(text_in or "")]))
        low = blob.lower()
        if re.search(r"\b(cost savings|cost reduction|expense reduction|annualized savings|reorganization)\b", low, re.I):
            return "cost_savings"
        if re.search(r"\b(obion|junior mezz|debt reduction|deleverag|repaid?|balance sheet improvement)\b", low, re.I):
            return "debt_reduction"
        if re.search(r"\b(45z|production tax credits?)\b", low, re.I):
            if re.search(r"\bqualif(?:y|ies|ied|ication)\b", low, re.I):
                return "45z_qualification"
            if re.search(r"\b(monetization|agreement executed|tax credit monetization)\b", low, re.I):
                return "45z_monetization"
            if re.search(r"\b2026\b", low, re.I) and re.search(r"\b(ebitda|opportunity|adjusted)\b", low, re.I):
                return "45z_2026_ebitda"
            return "45z_general"
        if (
            metric_low == "strategic milestone"
            or promise_type_low == "milestone"
            or re.search(r"\b(advantage nebraska|carbon capture|class vi|york|central city|wood river|sequestering co2|tallgrass trailblazer|biogenic co2)\b", low, re.I)
        ):
            if re.search(r"\b(advantage nebraska|carbon capture|class vi|york|central city|wood river|sequestering co2|tallgrass trailblazer|biogenic co2)\b", low, re.I):
                return "advantage_nebraska_milestone"
        theme = _management_theme_key(metric_name, blob)
        if theme:
            return theme
        pk = str(promise_key or "").strip().lower()
        if pk:
            pk = re.sub(r"\b(?:20\d{2}|q[1-4]|fy)\b", " ", pk)
            pk = re.sub(r"[_|]+", " ", pk)
            pk = re.sub(r"\s+", " ", pk).strip()
        return pk

    def _derive_progress_target_display(self, 
        metric_name: str,
        target_in: Any,
        text_in: str,
        quarter_hint: Any = None,
    ) -> Any:
        d = self.deps
        _parse_dollar_amount = d.parse_dollar_amount
        _coerce_amount_with_unit = d.coerce_amount_with_unit
        _fmt_short_money_value = d.fmt_short_money_value
        _extract_45z_monetization_target_display = d.extract_45z_monetization_target_display
        cur_target = str(target_in or "").strip()
        txt = glx_normalize_text(text_in)
        metric_low = str(metric_name or "").strip().lower()
        is_cost_savings = bool(re.search(r"\b(cost savings|cost reduction|expense reduction)\b", metric_low, re.I))
        if re.search(r"\b45z\b|tax credit", metric_low, re.I):
            strong_45z = _extract_45z_monetization_target_display(txt, quarter_hint, cur_target)
            if strong_45z:
                return strong_45z
        if cur_target and not is_cost_savings:
            return target_in
        if not txt:
            return target_in
        low = txt.lower()
        range_match = re.search(
            r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\s*(?:to|-)\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            txt,
            re.I,
        )
        if range_match:
            lo = _coerce_amount_with_unit(range_match.group(1), range_match.group(2) or range_match.group(4))
            hi = _coerce_amount_with_unit(range_match.group(3), range_match.group(4) or range_match.group(2))
            if lo is not None and hi is not None:
                return f"{_fmt_short_money_value(lo)}-{_fmt_short_money_value(hi)}"
        money_hits = [
            _coerce_amount_with_unit(m.group(1), m.group(2))
            for m in re.finditer(
                r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                txt,
                re.I,
            )
        ]
        money_hits = [x for x in money_hits if x is not None]
        if is_cost_savings and money_hits:
            realized_amt: Optional[float] = None
            addl_amt: Optional[float] = None
            target_amt: Optional[float] = None
            current_target_amt = _parse_dollar_amount(cur_target)
            current_is_stage = bool(re.search(r"\b(additional|remaining|phase\s*[12]|first tranche|second tranche|initial)\b", cur_target, re.I))
            for m in re.finditer(
                r"(realized|realised|accomplished|implemented|targeting|targets?|additional|target|reach(?:ing)?|pace to reach)\b[^.]{0,80}?\$?\s*"
                r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                txt,
                re.I,
            ):
                amt = _coerce_amount_with_unit(m.group(2), m.group(3))
                if amt is None:
                    continue
                verb = str(m.group(1) or "").lower()
                if verb in {"realized", "realised", "accomplished", "implemented"}:
                    realized_amt = max(realized_amt or 0.0, float(amt))
                elif verb in {"target", "targets", "targeting", "reach", "reaching", "pace to reach"}:
                    target_amt = max(target_amt or 0.0, float(amt))
                else:
                    addl_amt = max(addl_amt or 0.0, float(amt))
            if target_amt is None:
                m_overall = re.search(
                    r"\b(?:overall|full|program)\b[^.]{0,60}?\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b[^.]{0,40}?\b(?:cost reduction|cost savings|target|goal|program)\b",
                    txt,
                    re.I,
                )
                if m_overall:
                    target_amt = _coerce_amount_with_unit(m_overall.group(1), m_overall.group(2))
            if target_amt is None:
                m_target = re.search(
                    r"\b(?:reach|reaching|pace to reach|target(?:ing|s)?)\b[^.]{0,80}?\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b[^.]{0,30}\btarget\b",
                    txt,
                    re.I,
                )
                if m_target:
                    target_amt = _coerce_amount_with_unit(m_target.group(1), m_target.group(2))
            if target_amt is not None and (
                not current_is_stage
                and (
                    not cur_target
                    or current_target_amt is None
                    or float(target_amt) > float(current_target_amt) + 1e-6
                    or re.search(r"\b(program|annualized program|full target|goal)\b", cur_target, re.I)
                )
            ):
                return f">= {_fmt_short_money_value(target_amt)} annualized program"
            if current_is_stage and cur_target:
                return target_in
            if realized_amt is not None and addl_amt is not None:
                derived_total = float(realized_amt + addl_amt)
                if not cur_target or (current_target_amt is None or derived_total > float(current_target_amt) + 1e-6):
                    return f">= {_fmt_short_money_value(derived_total)} annualized program"
            if addl_amt is not None:
                return f">= {_fmt_short_money_value(addl_amt)} additional annualized savings"
            if realized_amt is not None:
                return f">= {_fmt_short_money_value(realized_amt)} annualized savings"
        if cur_target:
            return target_in
        return target_in

    def _follow_status_weight(self, status_in: Any) -> int:
        status_low = str(status_in or "").strip().lower().replace("_", " ")
        return {
            "completed": 5,
            "achieved": 5,
            "resolved pass": 5,
            "resolved beat": 5,
            "ahead of plan": 4,
            "on track": 3,
            "in progress": 2,
            "pending": 1,
            "open": 1,
            "not observed": 0,
            "resolved fail": 0,
            "": -1,
        }.get(status_low, 0)

    def _latest_basis_strength(self, latest_in: Any) -> int:
        txt = glx_normalize_text(str(latest_in or ""))
        if not txt:
            return 0
        low = txt.lower()
        if low == "not yet measurable":
            return 0
        if re.search(r"\b(net of discounts|income tax benefit)\b", low, re.I):
            return 6
        if re.search(r"\b(fully operational|sale completed|debt repaid|repaid|repayment completed)\b", low, re.I):
            return 5
        if re.search(r"\$\s*\d|\b\d+(?:\.\d+)?m\b|\b\d+(?:\.\d+)?bn\b", low, re.I):
            if re.search(r"\b(expected|guide|guidance|opportunity)\b", low, re.I):
                return 3
            return 5
        if re.search(r"\b(agreement executed|class vi permit received|capture system online|online and ramping|plant online|construction progressing)\b", low, re.I):
            return 4
        if re.search(r"\b(initiative launched|expected in 20\d{2}|expected in q[1-4])\b", low, re.I):
            return 2
        return 1

    def _finalize_progress_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        d = self.deps
        evaluation_as_of = d.evaluation_as_of
        milestone_progress_re = d.milestone_progress_re
        _qend = d.qend
        _parse_dollar_amount = d.parse_dollar_amount
        _extract_progress_latest_basis = d.extract_progress_latest_basis
        _actual_for_guidance = d.actual_for_guidance
        _guidance_period_end = d.guidance_period_end
        _infer_target_numeric_spec = self._infer_target_numeric_spec
        _derive_progress_target_display = self._derive_progress_target_display
        metric_low = str(item.get("metric_ref") or "").strip().lower()
        promise_type_low = str(item.get("promise_type") or "").strip().lower()
        rationale_txt = glx_normalize_text(str(item.get("rationale") or ""))
        source_txt = glx_normalize_text(str(item.get("_source_snip") or ""))
        evidence_txt = glx_normalize_text(" | ".join([x for x in [source_txt, rationale_txt] if x]))
        status_low = str(item.get("status") or "").strip().lower()
        item["target"] = _derive_progress_target_display(
            str(item.get("metric_ref") or ""),
            item.get("target"),
            evidence_txt,
            item.get("first_seen_evidence_quarter_end")
            or item.get("first_seen_quarter_end")
            or item.get("last_seen_evidence_quarter_end")
            or item.get("last_seen_quarter_end"),
        )

        latest_raw = item.get("latest")
        latest_num = pd.to_numeric(latest_raw, errors="coerce")
        latest_display: Any = latest_raw
        has_latest_basis = False
        if pd.notna(latest_num):
            latest_display = float(latest_num)
            has_latest_basis = True
        else:
            latest_str = str(latest_raw or "").strip()
            if latest_str:
                latest_display = latest_str
                has_latest_basis = latest_str.lower() != "not yet measurable"
            else:
                derived_basis = _extract_progress_latest_basis(str(item.get("metric_ref") or ""), evidence_txt)
                latest_display = derived_basis or "not yet measurable"
                has_latest_basis = bool(derived_basis)

        def _guidance_actual_metric_name() -> str:
            metric_blob = " | ".join(
                [
                    str(item.get("metric_display") or ""),
                    str(item.get("metric_ref") or ""),
                    evidence_txt,
                ]
            )
            if re.search(r"\brevenue\b", metric_blob, re.I):
                return "Revenue"
            if re.search(r"\badjusted?\s+ebit\b|\badj\.?\s*ebit\b", metric_blob, re.I):
                return "Adj EBIT"
            if re.search(r"\beps\b", metric_blob, re.I):
                return "Adj EPS"
            if re.search(r"\bfcf\b|free cash flow", metric_blob, re.I):
                return "FCF"
            return ""

        def _recover_observed_latest() -> Any:
            period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
            metric_name = _guidance_actual_metric_name()
            as_of_q = (
                evaluation_as_of
                or _qend(item.get("quarter"))
                or _qend(item.get("evaluated_through"))
                or _qend(item.get("last_seen_quarter_end"))
            )
            if metric_name and period_norm not in {"", "UNK"} and isinstance(as_of_q, date):
                actual_val = _actual_for_guidance(metric_name, period_norm, as_of_q)
                if actual_val is not None:
                    return float(actual_val)
            derived_basis = _extract_progress_latest_basis(str(item.get("metric_ref") or ""), evidence_txt)
            if derived_basis:
                if re.search(r"\b(cost savings|debt reduction|45z|liquidity release)\b", metric_low, re.I):
                    amt = _parse_dollar_amount(str(derived_basis)) or _parse_dollar_amount(evidence_txt)
                    if amt is not None:
                        return float(amt)
                return derived_basis
            return None

        def _resolve_closed_guidance_outcome() -> tuple[Any, str, bool]:
            period_norm = str(item.get("target_period_norm") or item.get("period_norm") or "").strip()
            metric_name = _guidance_actual_metric_name()
            as_of_q = (
                evaluation_as_of
                or _qend(item.get("quarter"))
                or _qend(item.get("evaluated_through"))
                or _qend(item.get("last_seen_quarter_end"))
            )
            if not metric_name or period_norm in {"", "UNK"} or not isinstance(as_of_q, date):
                return latest_display, normalized_status, False
            period_end = _guidance_period_end(period_norm, as_of_q)
            if period_end is None or period_end > as_of_q:
                return latest_display, normalized_status, False
            actual_val = _actual_for_guidance(metric_name, period_norm, as_of_q)
            if actual_val is None:
                return latest_display, normalized_status, False
            resolved_status = normalized_status
            target_spec = _infer_target_numeric_spec(item.get("target"))
            kind = str(target_spec.get("kind") or "")
            tol_mult = 0.01
            if kind == "range":
                lo = float(min(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
                hi = float(max(float(target_spec.get("low") or 0.0), float(target_spec.get("high") or 0.0)))
                if float(actual_val) < lo - abs(lo) * tol_mult:
                    resolved_status = "resolved_fail"
                elif float(actual_val) > hi + abs(hi) * tol_mult:
                    resolved_status = "resolved_beat"
                else:
                    resolved_status = "resolved_pass"
            elif kind in {"gte", "gt", "point", "lte", "lt"}:
                tgt = float(target_spec.get("value") or 0.0)
                tol = max(1e-9, abs(tgt) * tol_mult)
                if kind == "gte":
                    resolved_status = "resolved_pass" if float(actual_val) >= tgt - tol else "resolved_fail"
                elif kind == "gt":
                    if float(actual_val) > tgt + tol:
                        resolved_status = "resolved_beat"
                    elif float(actual_val) >= tgt - tol:
                        resolved_status = "resolved_pass"
                    else:
                        resolved_status = "resolved_fail"
                elif kind == "lte":
                    resolved_status = "resolved_pass" if float(actual_val) <= tgt + tol else "resolved_fail"
                elif kind == "lt":
                    resolved_status = "resolved_pass" if float(actual_val) < tgt + tol else "resolved_fail"
                else:
                    resolved_status = "resolved_pass" if abs(float(actual_val) - tgt) <= tol else ("resolved_beat" if float(actual_val) > tgt + tol else "resolved_fail")
            elif str(item.get("promise_type") or "").strip().lower() == "guidance_range":
                resolved_status = "resolved_pass"
            return float(actual_val), resolved_status, True

        completion_hit = bool(
            re.search(
                r"\b(fully operational|fully online|sale completed|achieved|closed|"
                r"repaid|repayment completed|used to fully repay)\b",
                evidence_txt,
                re.I,
            )
            or re.search(r"\bagreement executed\b", evidence_txt, re.I) and metric_low in {"45z monetization / ebitda", "debt reduction"}
        )
        progress_hit = bool(
            milestone_progress_re.search(evidence_txt)
            or re.search(
                r"\b(on track|expected|targeting|qualify|annualized savings|realized|progressing|received permit|"
                r"agreement executed|construction progressing|ordered major equipment|initiative launched)\b",
                evidence_txt,
                re.I,
            )
        )
        timing_hit = bool(re.search(r"\b(20\d{2}|fy\s*20\d{2}|q[1-4]\s*20\d{2}|quarter|full[- ]?year|annualized)\b", evidence_txt, re.I))

        if promise_type_low == "milestone" or metric_low == "strategic milestone":
            if metric_low in {"45z monetization / ebitda", "45z adjusted ebitda / monetization", "cost savings", "debt reduction"}:
                normalized_status = status_low
                if completion_hit:
                    normalized_status = "completed"
                elif has_latest_basis and (progress_hit or timing_hit):
                    normalized_status = "on_track"
                elif progress_hit:
                    normalized_status = "on_track"
                else:
                    normalized_status = ""
            else:
                if completion_hit:
                    normalized_status = "completed"
                elif progress_hit or has_latest_basis:
                    normalized_status = "in progress"
                else:
                    normalized_status = "not observed"
        else:
            normalized_status = status_low
            if status_low in {"achieved", "resolved_pass", "resolved_beat", "resolved_fail", "ahead_of_plan"}:
                normalized_status = status_low
            elif completion_hit:
                normalized_status = "completed"
            elif has_latest_basis and (progress_hit or timing_hit or metric_low in {"45z monetization / ebitda", "cost savings", "debt reduction", "management target"}):
                normalized_status = "on_track"
            else:
                normalized_status = ""

        if latest_display == "not yet measurable" and normalized_status in {"completed", "on_track", "ahead_of_plan", "in progress"} and not (progress_hit or completion_hit):
            normalized_status = "in progress" if promise_type_low == "milestone" or metric_low == "strategic milestone" else ""

        if normalized_status == "completed" and not completion_hit:
            normalized_status = "in progress" if promise_type_low == "milestone" or metric_low == "strategic milestone" else "on_track"
        if normalized_status in {"on_track", "ahead_of_plan"} and not (progress_hit or completion_hit or has_latest_basis):
            normalized_status = ""

        if (
            metric_low in {"revenue guidance", "adjusted ebit guidance", "eps guidance", "fcf target"}
            or str(item.get("promise_type") or "").strip().lower() == "guidance_range"
        ):
            resolved_latest, resolved_status, resolved_hit = _resolve_closed_guidance_outcome()
            if resolved_hit:
                latest_display = resolved_latest
                has_latest_basis = True
                if normalized_status not in {"achieved", "resolved_pass", "resolved_beat", "resolved_fail"}:
                    normalized_status = resolved_status

        if (
            (not has_latest_basis or str(latest_display or "").strip().lower() == "not yet measurable")
            and normalized_status in {"achieved", "resolved_pass", "resolved_beat", "resolved_fail", "broken", "missed"}
        ):
            recovered_latest = _recover_observed_latest()
            if recovered_latest is not None:
                latest_display = recovered_latest
                has_latest_basis = True

        resolved_numeric_target = bool(
            normalized_status in {"achieved", "resolved_pass", "resolved_beat", "resolved_fail", "broken", "missed"}
            and promise_type_low != "milestone"
            and metric_low != "strategic milestone"
            and (
                promise_type_low == "guidance_range"
                or re.search(
                    r"\b(revenue guidance|adjusted ebit guidance|eps guidance|fcf target|cost savings target|deleveraging target|pb bank liquidity release|sendtech / presort operating target)\b",
                    metric_low,
                    re.I,
                )
            )
        )
        if resolved_numeric_target and pd.isna(pd.to_numeric(latest_display, errors="coerce")):
            return None

        if not normalized_status and latest_display == "not yet measurable":
            return None

        item["latest"] = latest_display
        item["status"] = normalized_status
        return item

    def _progress_status_from_tracker(self, metric_name: str, txt_in: str) -> str:
        d = self.deps
        milestone_progress_re = d.milestone_progress_re
        milestone_completion_re = d.milestone_completion_re
        txt_local = glx_normalize_text(str(txt_in or ""))
        low_metric = str(metric_name or "").strip().lower()
        if milestone_completion_re.search(txt_local) or re.search(
            r"\b(repaid|repayment completed|fully operational|sale completed)\b",
            txt_local,
            re.I,
        ):
            return "completed"
        if low_metric in {"45z monetization / ebitda", "debt reduction"} and re.search(
            r"\b(agreement executed|executed)\b",
            txt_local,
            re.I,
        ):
            return "completed"
        if low_metric in {"45z monetization / ebitda", "management target", "cost savings"} and re.search(
            r"\b(expected|opportunity|on track|target|targets|annualized|qualify|progressing|ramping|realized|accomplished)\b",
            txt_local,
            re.I,
        ):
            return "on_track"
        if low_metric == "strategic milestone":
            if milestone_progress_re.search(txt_local):
                return "in progress"
            return "not observed"
        if low_metric == "debt reduction":
            return "completed" if re.search(r"\b(repaid|completed|closed|used to fully repay)\b", txt_local, re.I) else "on_track"
        if re.search(r"\b(on track|ramping|under construction|progressing|began|beginning|advancing|continuing)\b", txt_local, re.I):
            return "in progress"
        return "on_track"

    def _follow_candidate_sort_key(self, cand: Dict[str, Any]) -> Tuple[int, int, int, Any]:
        d = self.deps
        _source_rank = d.source_rank
        _candidate_quality_key = d.candidate_quality_key
        _follow_status_weight = self._follow_status_weight
        qd_c = cand.get("quarter")
        q_ord = int(qd_c.toordinal()) if isinstance(qd_c, date) else 0
        src_rank = _source_rank(cand.get("source_type"), cand.get("source_doc"))
        return (
            -q_ord,
            -_follow_status_weight(cand.get("status_hint")),
            int(src_rank),
            _candidate_quality_key(
                cand.get("text"),
                cand.get("source_type"),
                cand.get("source_doc"),
                cand.get("doc_priority"),
                cand.get("score"),
            ),
        )

    def _build_follow_through_candidate(self, 
        qd_c: Any,
        metric_name: Any,
        promise_key: Any,
        promise_type: Any,
        text_in: Any,
        source_type: Any,
        source_doc: Any,
        status_hint: Any = "",
        target_in: Any = "",
        latest_in: Any = "",
        score_in: Any = 0.0,
        doc_priority_in: Any = 0,
    ) -> Optional[Dict[str, Any]]:
        d = self.deps
        _qend = d.qend
        _split_target_metric_display = d.split_target_metric_display
        _derive_split_target_meta = d.derive_split_target_meta
        _progress_context_key = self._progress_context_key
        _follow_through_family_key = self._follow_through_family_key
        _follow_through_theme_key = self._follow_through_theme_key
        _derive_progress_target_display = self._derive_progress_target_display
        _progress_status_from_tracker = self._progress_status_from_tracker
        qd_val = _qend(qd_c)
        txt = glx_normalize_text(text_in)
        if qd_val is None or not txt:
            return None
        metric_txt = str(metric_name or "").strip()
        promise_type_txt = str(promise_type or "").strip()
        theme_key = _follow_through_theme_key(metric_txt, promise_key, promise_type_txt, txt, target_in)
        if not theme_key:
            return None
        status_txt = str(status_hint or "").strip().lower()
        if not status_txt:
            status_txt = _progress_status_from_tracker(metric_txt, txt)
        rec = {
            "quarter": qd_val,
            "metric_ref": metric_txt,
            "promise_type": promise_type_txt,
            "promise_key": str(promise_key or ""),
            "theme_key": theme_key,
            "family_key": _follow_through_family_key(theme_key),
            "context_key": _progress_context_key(metric_txt, txt, promise_type_txt),
            "text": txt,
            "source_type": str(source_type or ""),
            "source_doc": str(source_doc or ""),
            "status_hint": status_txt,
            "target": _derive_progress_target_display(metric_txt, target_in, txt, qd_val),
            "latest": latest_in,
            "score": float(pd.to_numeric(score_in, errors="coerce") if pd.notna(pd.to_numeric(score_in, errors="coerce")) else 0.0),
            "doc_priority": int(pd.to_numeric(doc_priority_in, errors="coerce") if pd.notna(pd.to_numeric(doc_priority_in, errors="coerce")) else 0),
        }
        rec.update(
            _derive_split_target_meta(
                metric_txt,
                " | ".join([str(target_in or ""), txt]),
                "",
                qd_val,
                source_type,
                source_doc,
                "",
            )
        )
        rec["metric_display"] = _split_target_metric_display(metric_txt, " | ".join([str(target_in or ""), txt]), rec)
        rec["promise_group"] = str(rec.get("target_group_key") or "")
        return rec

    def _append_follow_rationale(self, base_text: str, later_text: str, later_q: date, base_q: Optional[date] = None) -> str:
        d = self.deps
        _q_label = d.q_label
        base_norm = glx_normalize_text(base_text)
        later_norm = glx_normalize_text(later_text)
        if not later_norm:
            return base_norm
        later_snip = qn_compact_snippet(later_norm, 220)
        later_label = _q_label(later_q)
        update_prefix = "Same-quarter confirmation" if isinstance(base_q, date) and later_q == base_q else "Later update"
        update_txt = f"{update_prefix} ({later_label}): {later_snip}"
        if not base_norm:
            return update_txt
        if later_snip.lower() in base_norm.lower():
            return base_norm
        return f"{base_norm} | {update_txt}"

    def _apply_follow_through_resolution(self, 
        rows_map: Dict[date, List[Dict[str, Any]]],
        quarter_list: List[date],
    ) -> int:
        d = self.deps
        progress_records = d.progress_records
        tracker_rows_map = d.tracker_rows_map
        quarter_note_rows_map = d.quarter_note_rows_map
        q_col = d.progress_columns.get("q_col")
        mr_col = d.progress_columns.get("mr_col")
        pk_col = d.progress_columns.get("pk_col")
        ptype_col = d.progress_columns.get("ptype_col")
        ra_col = d.progress_columns.get("ra_col")
        st_col = d.progress_columns.get("st_col")
        tg_col = d.progress_columns.get("tg_col")
        ac_col = d.progress_columns.get("ac_col")
        sc_col = d.progress_columns.get("sc_col")
        _source_rank = d.source_rank
        _parse_dollar_amount = d.parse_dollar_amount
        _split_target_scope_token = d.split_target_scope_token
        _split_target_scope_is_broad = d.split_target_scope_is_broad
        _infer_target_period = d.infer_target_period
        _infer_target_structure = d.infer_target_structure
        _load_local_cost_savings_follow_candidates = d.load_local_cost_savings_follow_candidates
        _load_profile_slide_signals = d.load_profile_slide_signals
        _progress_metric_from_event = d.progress_metric_from_event
        _progress_metric_from_qnote = d.progress_metric_from_qnote
        _progress_target_display_from_qnote = d.progress_target_display_from_qnote
        _progress_context_key = self._progress_context_key
        _follow_through_family_key = self._follow_through_family_key
        _target_structure_match_rank = self._target_structure_match_rank
        _follow_scope_match_rank = self._follow_scope_match_rank
        _follow_context_match_rank = self._follow_context_match_rank
        _find_later_matching_outcome = self._find_later_matching_outcome
        _classify_progress_status = self._classify_progress_status
        _resolve_follow_through_latest = self._resolve_follow_through_latest
        _resolve_follow_through_status = self._resolve_follow_through_status
        _follow_through_theme_key = self._follow_through_theme_key
        _follow_status_weight = self._follow_status_weight
        _latest_basis_strength = self._latest_basis_strength
        _finalize_progress_item = self._finalize_progress_item
        _progress_status_from_tracker = self._progress_status_from_tracker
        _follow_candidate_sort_key = self._follow_candidate_sort_key
        _build_follow_through_candidate = self._build_follow_through_candidate
        _append_follow_rationale = self._append_follow_rationale
        if not quarter_list:
            return 0
        eval_q = max([q for q in quarter_list if isinstance(q, date)], default=None)
        candidates_by_theme: Dict[str, List[Dict[str, Any]]] = {}
        candidates_by_family: Dict[str, List[Dict[str, Any]]] = {}

        def _add_cand(cand: Optional[Dict[str, Any]]) -> None:
            if not cand:
                return
            theme_key = str(cand.get("theme_key") or "")
            family_key = str(cand.get("family_key") or theme_key)
            candidates_by_theme.setdefault(theme_key, []).append(cand)
            candidates_by_family.setdefault(family_key, []).append(cand)

        for rr in progress_records:
            metric_txt = str(rr.get(mr_col) or "").strip() if mr_col else ""
            promise_type_txt = str(rr.get(ptype_col) or "operational") if ptype_col else "operational"
            src_ev = dict(rr.get("_src_ev") or {})
            src_snip = glx_normalize_text(str(rr.get("_src_snip") or src_ev.get("snippet") or ""))
            rationale_txt = glx_normalize_text(str(rr.get(ra_col) or "")) if ra_col else ""
            basis_txt = src_snip or rationale_txt
            source_type_txt = str(rr.get("_src_source_type") or src_ev.get("doc_type") or src_ev.get("source_type") or "")
            source_doc_txt = str(rr.get("_src_doc") or src_ev.get("doc_path") or src_ev.get("doc") or "")
            _add_cand(
                _build_follow_through_candidate(
                    rr.get(q_col),
                    metric_txt,
                    rr.get(pk_col) if pk_col else "",
                    promise_type_txt,
                    basis_txt,
                    source_type_txt,
                    source_doc_txt,
                    rr.get(st_col),
                    rr.get(tg_col) if tg_col else "",
                    rr.get(ac_col) if ac_col else "",
                    rr.get(sc_col) if sc_col else 0.0,
                    _source_rank(source_type_txt, source_doc_txt),
                )
            )

        if isinstance(tracker_rows_map, dict):
            for qd_c, recs in tracker_rows_map.items():
                if not isinstance(qd_c, date) or not isinstance(recs, list):
                    continue
                for rec in recs:
                    if not isinstance(rec, dict):
                        continue
                    metric_txt = str(rec.get("metric") or "").strip()
                    txt_full = glx_normalize_text(str(rec.get("text_full") or rec.get("text_snippet") or ""))
                    _add_cand(
                        _build_follow_through_candidate(
                            qd_c,
                            metric_txt,
                            rec.get("theme_key") or metric_txt.lower().replace(" ", "_"),
                            "milestone" if metric_txt == "Strategic milestone" else "operational",
                            txt_full,
                            dict(rec.get("source") or {}).get("source_type") or rec.get("source_type") or "tracker_ui",
                            dict(rec.get("source") or {}).get("doc") or rec.get("source_doc") or "",
                            _progress_status_from_tracker(metric_txt, txt_full),
                            rec.get("target_display") or "",
                            "",
                            rec.get("score") or 0.0,
                            0,
                        )
                    )

        if isinstance(quarter_note_rows_map, dict):
            for qd_c, recs in quarter_note_rows_map.items():
                if not isinstance(qd_c, date) or not isinstance(recs, list):
                    continue
                for rec in recs:
                    if not isinstance(rec, dict):
                        continue
                    metric_txt = _progress_metric_from_event(rec) or _progress_metric_from_qnote(rec)
                    if not metric_txt:
                        continue
                    txt_full = glx_normalize_text(str(rec.get("text_full") or rec.get("comment_full_text") or ""))
                    summary_txt = glx_normalize_text(str(rec.get("_render_summary") or ""))
                    basis_txt = summary_txt or txt_full
                    if not basis_txt:
                        continue
                    src = dict(rec.get("source") or {})
                    promise_type_txt = "milestone" if metric_txt == "Strategic milestone" else "operational"
                    target_txt = _progress_target_display_from_qnote(qd_c, metric_txt, txt_full)
                    event_key = str(rec.get("_event_key") or rec.get("theme_key") or metric_txt.lower().replace(" ", "_"))
                    status_hint = _progress_status_from_tracker(metric_txt, txt_full)
                    cand = _build_follow_through_candidate(
                        qd_c,
                        metric_txt,
                        event_key,
                        promise_type_txt,
                        basis_txt,
                        src.get("source_type") or src.get("doc_type") or rec.get("source_type") or rec.get("doc_type") or "quarter_notes_ui",
                        src.get("doc") or rec.get("doc") or "Quarter_Notes_UI",
                        status_hint,
                        target_txt,
                        summary_txt if summary_txt and summary_txt != txt_full else "",
                        rec.get("score") or 0.0,
                        _source_rank(src.get("source_type") or rec.get("source_type"), src.get("doc") or rec.get("doc")),
                    )
                    _add_cand(cand)

        slide_signals = _load_profile_slide_signals()
        if slide_signals:
            for rec in slide_signals:
                if not isinstance(rec, dict):
                    continue
                _add_cand(
                    _build_follow_through_candidate(
                        rec.get("quarter"),
                        rec.get("metric"),
                        rec.get("theme_key") or rec.get("metric"),
                        "milestone" if bool(rec.get("is_milestone")) else "operational",
                        rec.get("text"),
                        rec.get("source_type") or "earnings_presentation",
                        rec.get("source_doc") or "",
                        rec.get("status_hint") or "",
                        rec.get("target_display") or "",
                        "",
                        rec.get("score") or 0.0,
                        rec.get("doc_priority") or 0,
                    )
                )

        def _follow_scope_match_rank(item_local: Dict[str, Any], cand_local: Dict[str, Any]) -> int:
            item_scope_key = _split_target_scope_token(item_local)
            cand_scope_key = _split_target_scope_token(cand_local)
            cand_text = str(cand_local.get("text") or "")
            if item_scope_key == cand_scope_key:
                return 3
            if item_scope_key == "company_total":
                return 2 if _split_target_scope_is_broad(cand_local, cand_text) else 0
            if _split_target_scope_is_broad(cand_local, cand_text):
                return 1
            return 0

        def _dedupe_follow_candidates(cands_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            seen: set[Tuple[Any, str, str]] = set()
            for cand in cands_in:
                key = (
                    cand.get("quarter"),
                    _split_target_scope_token(cand),
                    glx_normalize_text(str(cand.get("text") or "")).lower(),
                )
                if key in seen:
                    continue
                seen.add(key)
                out.append(cand)
            return out

        resolved_count = 0
        for qd in quarter_list:
            updated_items: List[Dict[str, Any]] = []
            for item in rows_map.get(qd, []):
                row_type = str(item.get("row_type") or "").strip().lower()
                if row_type == "section" or str(item.get("promise_type") or "").strip().lower() == "guidance_range":
                    updated_items.append(item)
                    continue
                metric_txt = str(item.get("metric_ref") or "").strip()
                if not metric_txt:
                    updated_items.append(item)
                    continue
                theme_key = _follow_through_theme_key(
                    metric_txt,
                    item.get("promise_key"),
                    item.get("promise_type"),
                    " | ".join(
                        [
                            str(item.get("target") or ""),
                            str(item.get("latest") or ""),
                            str(item.get("rationale") or ""),
                        ]
                    ),
                    item.get("target"),
                )
                if not theme_key:
                    updated_items.append(item)
                    continue
                item_context_key = _progress_context_key(
                    metric_txt,
                    " | ".join(
                        [
                            str(item.get("target") or ""),
                            str(item.get("latest") or ""),
                            str(item.get("rationale") or ""),
                        ]
                    ),
                    item.get("promise_type"),
                )
                family_key = _follow_through_family_key(theme_key)
                base_rationale = glx_normalize_text(str(item.get("rationale") or ""))
                later_candidates = [
                    cand
                    for cand in candidates_by_theme.get(theme_key, [])
                    if isinstance(cand.get("quarter"), date) and cand.get("quarter") > qd
                ]
                later_family_candidates = [
                    cand
                    for cand in candidates_by_family.get(family_key, [])
                    if isinstance(cand.get("quarter"), date) and cand.get("quarter") > qd
                ]
                prior_family_candidates = [
                    cand
                    for cand in candidates_by_family.get(family_key, [])
                    if isinstance(cand.get("quarter"), date) and cand.get("quarter") < qd
                ]
                if family_key == "cost_savings":
                    for cand in _load_local_cost_savings_follow_candidates():
                        cand_q = cand.get("quarter")
                        if not isinstance(cand_q, date):
                            continue
                        if cand_q > qd:
                            later_family_candidates.append(cand)
                        elif cand_q < qd:
                            prior_family_candidates.append(cand)
                same_q_family_candidates = [
                    cand
                    for cand in candidates_by_family.get(family_key, [])
                    if isinstance(cand.get("quarter"), date)
                    and cand.get("quarter") == qd
                    and glx_normalize_text(str(cand.get("text") or "")).lower() != base_rationale.lower()
                ]
                for peer_item in rows_map.get(qd, []):
                    if not isinstance(peer_item, dict):
                        continue
                    if str(peer_item.get("promise_id") or "") == str(item.get("promise_id") or ""):
                        continue
                    peer_metric = str(peer_item.get("metric_ref") or "").strip()
                    if not peer_metric:
                        continue
                    peer_theme_key = _follow_through_theme_key(
                        peer_metric,
                        peer_item.get("promise_key"),
                        peer_item.get("promise_type"),
                        " | ".join(
                            [
                                str(peer_item.get("target") or ""),
                                str(peer_item.get("latest") or ""),
                                str(peer_item.get("rationale") or ""),
                            ]
                        ),
                        peer_item.get("target"),
                    )
                    if _follow_through_family_key(peer_theme_key) != family_key:
                        continue
                    peer_text = glx_normalize_text(
                        " | ".join([str(peer_item.get("rationale") or ""), str(peer_item.get("_source_snip") or "")])
                    )
                    if not peer_text or peer_text.lower() == base_rationale.lower():
                        continue
                    peer_cand = _build_follow_through_candidate(
                        qd,
                        peer_metric,
                        peer_item.get("promise_key") or peer_theme_key,
                        peer_item.get("promise_type") or "operational",
                        peer_text,
                        "progress_ui_peer",
                        "",
                        peer_item.get("status") or "",
                        peer_item.get("target") or "",
                        peer_item.get("latest") or "",
                        0.0,
                        0,
                    )
                    if peer_cand:
                        same_q_family_candidates.append(peer_cand)
                if item_context_key in {"45z_monetization", "45z_plant_qualification"}:
                    for peer_item in rows_map.get(qd, []):
                        if not isinstance(peer_item, dict):
                            continue
                        if str(peer_item.get("promise_id") or "") == str(item.get("promise_id") or ""):
                            continue
                        peer_text = glx_normalize_text(
                            " | ".join([str(peer_item.get("rationale") or ""), str(peer_item.get("_source_snip") or "")])
                        )
                        if not peer_text:
                            continue
                        if not re.search(r"\b(advantage nebraska|45z|fully operational|online|ramping|agreement executed)\b", peer_text, re.I):
                            continue
                        peer_metric = str(peer_item.get("metric_ref") or "").strip()
                        peer_cand = _build_follow_through_candidate(
                            qd,
                            peer_metric or "Strategic milestone",
                            peer_item.get("promise_key") or peer_metric or "strategic_milestone",
                            peer_item.get("promise_type") or "operational",
                            peer_text,
                            "progress_ui_peer",
                            "",
                            peer_item.get("status") or "",
                            peer_item.get("target") or "",
                            peer_item.get("latest") or "",
                            0.0,
                            0,
                        )
                        if peer_cand:
                            same_q_family_candidates.append(peer_cand)
                if family_key == "cost_savings":
                    for cand in _load_local_cost_savings_follow_candidates():
                        cand_q = cand.get("quarter")
                        if not isinstance(cand_q, date):
                            continue
                        if cand_q == qd and glx_normalize_text(str(cand.get("text") or "")).lower() != base_rationale.lower():
                            same_q_family_candidates.append(cand)
                if not later_candidates and not later_family_candidates and not same_q_family_candidates:
                    updated_items.append(item)
                    continue
                raw_candidate_pool = _dedupe_follow_candidates(
                    later_candidates + later_family_candidates + same_q_family_candidates
                )
                target_context_pool = _dedupe_follow_candidates(
                    prior_family_candidates + raw_candidate_pool
                )
                if item_context_key == "45z_plant_qualification":
                    raw_candidate_pool = [
                        cand
                        for cand in raw_candidate_pool
                        if _progress_context_key(
                            cand.get("metric_ref") or cand.get("metric") or "",
                            " | ".join(
                                [
                                    str(cand.get("target") or ""),
                                    str(cand.get("latest") or ""),
                                    str(cand.get("text") or cand.get("rationale") or ""),
                                ]
                            ),
                            cand.get("promise_type"),
                        )
                        != "45z_monetization"
                    ]
                    target_context_pool = [
                        cand
                        for cand in target_context_pool
                        if _progress_context_key(
                            cand.get("metric_ref") or cand.get("metric") or "",
                            " | ".join(
                                [
                                    str(cand.get("target") or ""),
                                    str(cand.get("latest") or ""),
                                    str(cand.get("text") or cand.get("rationale") or ""),
                                ]
                            ),
                            cand.get("promise_type"),
                        )
                        != "45z_monetization"
                    ]
                exact_scope_candidates = [
                    cand for cand in raw_candidate_pool if _follow_scope_match_rank(item, cand) >= 3
                ]
                broad_scope_candidates = [
                    cand for cand in raw_candidate_pool if 0 < _follow_scope_match_rank(item, cand) < 3
                ]
                candidate_pool = sorted(exact_scope_candidates, key=_follow_candidate_sort_key) + sorted(
                    broad_scope_candidates,
                    key=_follow_candidate_sort_key,
                )
                if not candidate_pool:
                    updated_items.append(item)
                    continue
                best = candidate_pool[0]
                tmp = dict(item)
                base_target_period = _infer_target_period(
                    {
                        "target": tmp.get("target") or "",
                        "text_full": " | ".join(
                            [
                                str(tmp.get("rationale") or ""),
                                str(tmp.get("_source_snip") or ""),
                            ]
                        ),
                        "target_period_norm": tmp.get("target_period_norm") or tmp.get("period_key") or "",
                        "promise_type": tmp.get("promise_type") or "",
                    },
                    tmp.get("first_seen_evidence_quarter_end")
                    or tmp.get("first_seen_quarter_end")
                    or qd,
                )
                base_target_structure = _infer_target_structure(
                    {
                        "target": tmp.get("target") or "",
                        "text_full": " | ".join(
                            [
                                str(tmp.get("rationale") or ""),
                                str(tmp.get("_source_snip") or ""),
                            ]
                        ),
                        "target_family_key": tmp.get("target_family_key") or family_key,
                        "scope_key": tmp.get("scope_key") or "",
                        "target_period_norm": tmp.get("target_period_norm") or base_target_period.get("target_period_norm") or "",
                    },
                    tmp.get("first_seen_evidence_quarter_end")
                    or tmp.get("first_seen_quarter_end")
                    or qd,
                )
                same_metric_target = False
                current_target_txt = str(tmp.get("target") or "").strip()
                best_target_txt = str(best.get("target") or "").strip()
                strongest_target_txt = current_target_txt
                strongest_target_amt = _parse_dollar_amount(strongest_target_txt or "")
                strongest_target_has_amt = strongest_target_amt is not None
                for cand in target_context_pool:
                    if _follow_scope_match_rank(tmp, cand) < 3:
                        continue
                    if _follow_context_match_rank(tmp, cand) <= 0:
                        continue
                    struct_rank = _target_structure_match_rank(base_target_structure, cand)
                    if struct_rank <= 0 and str(base_target_structure.get("target_structure_kind") or "").strip().lower() not in {"", "single"}:
                        continue
                    cand_target_period = _infer_target_period(
                        {
                            "target": cand.get("target") or "",
                            "text_full": cand.get("text") or "",
                            "target_period_norm": cand.get("target_period_norm") or cand.get("period_key") or "",
                            "promise_type": cand.get("promise_type") or "",
                        },
                        cand.get("quarter"),
                    )
                    if (
                        str(base_target_period.get("target_period_norm") or "").strip()
                        and str(cand_target_period.get("target_period_norm") or "").strip()
                        and str(base_target_period.get("target_period_norm") or "").strip() != str(cand_target_period.get("target_period_norm") or "").strip()
                    ):
                        continue
                    same_metric_target = True
                    cand_target_txt = str(cand.get("target") or "").strip()
                    if not cand_target_txt:
                        continue
                    cand_target_amt = _parse_dollar_amount(cand_target_txt)
                    cand_has_amt = cand_target_amt is not None
                    cand_target_txt_norm = glx_normalize_text(cand_target_txt).lower()
                    strongest_target_txt_norm = glx_normalize_text(strongest_target_txt).lower()
                    if not strongest_target_txt:
                        strongest_target_txt = cand_target_txt
                        strongest_target_amt = cand_target_amt
                        strongest_target_has_amt = cand_has_amt
                        continue
                    if cand_has_amt and (
                        not strongest_target_has_amt
                        or (strongest_target_amt is not None and cand_target_amt is not None and cand_target_amt > strongest_target_amt + 1e-6)
                    ):
                        strongest_target_txt = cand_target_txt
                        strongest_target_amt = cand_target_amt
                        strongest_target_has_amt = True
                        continue
                    if (
                        cand_has_amt
                        and strongest_target_has_amt
                        and strongest_target_amt is not None
                        and cand_target_amt is not None
                        and abs(cand_target_amt - strongest_target_amt) <= 1e-6
                        and "annualized program" in cand_target_txt_norm
                        and "annualized program" not in strongest_target_txt_norm
                    ):
                        strongest_target_txt = cand_target_txt
                        strongest_target_amt = cand_target_amt
                        strongest_target_has_amt = True
                        continue
                    if not strongest_target_has_amt and cand_target_txt_norm and len(cand_target_txt_norm) > len(strongest_target_txt_norm):
                        strongest_target_txt = cand_target_txt
                        strongest_target_amt = cand_target_amt
                        strongest_target_has_amt = cand_has_amt
                if strongest_target_txt:
                    best_target_txt = strongest_target_txt
                best_target_is_stronger = False
                if best_target_txt and same_metric_target:
                    current_target_amt = _parse_dollar_amount(current_target_txt)
                    best_target_amt = _parse_dollar_amount(best_target_txt)
                    best_target_is_stronger = (
                        not current_target_txt
                        or (
                            best_target_amt is not None
                            and (current_target_amt is None or best_target_amt > current_target_amt + 1e-6)
                        )
                    )
                    if best_target_is_stronger:
                        tmp["target"] = best_target_txt
                target_period = _infer_target_period(
                    tmp,
                    tmp.get("first_seen_evidence_quarter_end")
                    or tmp.get("first_seen_quarter_end")
                    or qd,
                )
                target_structure = _infer_target_structure(
                    {
                        "target": tmp.get("target") or "",
                        "text_full": " | ".join(
                            [
                                str(tmp.get("rationale") or ""),
                                str(tmp.get("_source_snip") or ""),
                            ]
                        ),
                        "target_family_key": tmp.get("target_family_key") or family_key,
                        "scope_key": tmp.get("scope_key") or "",
                        "target_period_norm": tmp.get("target_period_norm") or target_period.get("target_period_norm") or "",
                    },
                    tmp.get("first_seen_evidence_quarter_end")
                    or tmp.get("first_seen_quarter_end")
                    or qd,
                )
                tmp.update(
                    {
                        "target_period_type": str(target_period.get("target_period_type") or tmp.get("target_period_type") or "open_ended"),
                        "target_period_norm": str(target_period.get("target_period_norm") or tmp.get("target_period_norm") or ""),
                        "target_period_label": str(target_period.get("target_period_label") or tmp.get("target_period_label") or ""),
                        "target_period_start": target_period.get("target_period_start"),
                        "target_period_end": target_period.get("target_period_end"),
                        "target_structure_kind": str(target_structure.get("target_structure_kind") or tmp.get("target_structure_kind") or "single"),
                        "target_structure_role": str(target_structure.get("target_structure_role") or tmp.get("target_structure_role") or "single"),
                        "stage_kind": str(target_structure.get("stage_kind") or tmp.get("stage_kind") or ""),
                        "stage_amount": target_structure.get("stage_amount") if target_structure.get("stage_amount") is not None else tmp.get("stage_amount"),
                        "increment_amount": target_structure.get("increment_amount") if target_structure.get("increment_amount") is not None else tmp.get("increment_amount"),
                        "program_total_amount": target_structure.get("program_total_amount") if target_structure.get("program_total_amount") is not None else tmp.get("program_total_amount"),
                        "program_key": str(target_structure.get("program_key") or tmp.get("program_key") or ""),
                    }
                )
                outcome_match = _find_later_matching_outcome(tmp, candidate_pool, eval_q, target_period, target_structure)
                resolved_latest = str(_resolve_follow_through_latest(tmp, candidate_pool, eval_q) or "").strip()
                outcome_latest = str(outcome_match.get("latest") or "").strip()
                final_evidence_value = dict(outcome_match.get("final_evidence") or {}).get("value")
                resolved_latest_strength = _latest_basis_strength(resolved_latest)
                outcome_latest_strength = _latest_basis_strength(outcome_latest)
                if final_evidence_value is not None and outcome_latest:
                    chosen_latest = outcome_latest
                elif resolved_latest and (
                    not outcome_latest
                    or resolved_latest_strength >= outcome_latest_strength
                ):
                    chosen_latest = resolved_latest
                else:
                    chosen_latest = outcome_latest
                tmp["latest"] = chosen_latest or resolved_latest or outcome_latest or "not yet measurable"
                resolved_status = _classify_progress_status(tmp, outcome_match, target_period, target_structure)
                if not resolved_status:
                    resolved_status = _resolve_follow_through_status(tmp, str(tmp.get("latest") or ""), candidate_pool)
                tmp["status"] = resolved_status
                tmp["rationale"] = str(best.get("text") or "")
                tmp["_source_snip"] = str(best.get("text") or "")
                finalized = _finalize_progress_item(tmp)
                if finalized is None:
                    updated_items.append(item)
                    continue
                if resolved_status:
                    finalized["status"] = resolved_status
                orig_status = str(item.get("status") or "").strip().lower().replace("_", " ")
                new_status = str(finalized.get("status") or "").strip().lower().replace("_", " ")
                orig_latest = str(item.get("latest") or "").strip()
                new_latest = str(finalized.get("latest") or "").strip()
                orig_latest_strength = _latest_basis_strength(orig_latest)
                new_latest_strength = _latest_basis_strength(new_latest)
                orig_status_strength = _follow_status_weight(orig_status)
                new_status_strength = _follow_status_weight(new_status)
                orig_last_seen_ts = pd.to_datetime(
                    item.get("last_seen_evidence_quarter_end") or item.get("last_seen_quarter_end"),
                    errors="coerce",
                )
                best_q_ts = pd.to_datetime(best.get("quarter"), errors="coerce")
                should_update = False
                if (not orig_latest or orig_latest.lower() == "not yet measurable") and new_latest_strength > 0:
                    should_update = True
                if new_status_strength > orig_status_strength and new_latest_strength >= max(2, orig_latest_strength):
                    should_update = True
                if new_latest_strength > orig_latest_strength and new_status_strength >= max(orig_status_strength, 1):
                    should_update = True
                if (
                    orig_status_strength > new_status_strength
                    and new_status_strength >= 0
                    and new_latest_strength >= max(orig_latest_strength, 2)
                    and new_latest.strip().lower() != orig_latest.strip().lower()
                ):
                    should_update = True
                if (
                    new_latest
                    and new_latest.strip().lower() != orig_latest.strip().lower()
                    and pd.notna(best_q_ts)
                    and (pd.isna(orig_last_seen_ts) or best_q_ts > orig_last_seen_ts)
                    and new_latest_strength >= max(orig_latest_strength, 3)
                ):
                    should_update = True
                if not should_update:
                    if best_target_is_stronger and str(finalized.get("target") or "").strip() and same_metric_target:
                        merged = dict(item)
                        merged["target"] = finalized.get("target")
                        updated_items.append(merged)
                        resolved_count += 1
                        continue
                    updated_items.append(item)
                    continue
                merged = dict(item)
                merged["target"] = finalized.get("target")
                merged["latest"] = finalized.get("latest")
                merged["status"] = finalized.get("status")
                merged["rationale"] = _append_follow_rationale(
                    str(item.get("rationale") or ""),
                    str(best.get("text") or ""),
                    best.get("quarter"),
                    qd,
                )
                final_cand = dict(outcome_match.get("best_final_candidate") or {})
                if (
                    str(final_cand.get("source_type") or "").strip().lower() == "financial_statement"
                    and str(final_cand.get("text") or "").strip()
                ):
                    merged["rationale"] = _append_follow_rationale(
                        str(merged.get("rationale") or ""),
                        str(final_cand.get("text") or ""),
                        final_cand.get("quarter") if isinstance(final_cand.get("quarter"), date) else qd,
                        qd,
                    )
                best_q = str(
                    tmp.get("_resolved_latest_quarter_end")
                    or finalized.get("_resolved_latest_quarter_end")
                    or best.get("quarter")
                    or ""
                )
                merged["last_seen_quarter_end"] = best_q
                merged["last_seen_evidence_quarter_end"] = best_q
                merged["last_seen_text_quarter_end"] = best_q
                if pd.notna(pd.to_numeric(finalized.get("latest"), errors="coerce")):
                    merged["last_seen_numeric_quarter_end"] = best_q
                merged["carried_to_quarter_end"] = str(eval_q or best.get("quarter") or qd)
                merged["evaluated_through"] = str(eval_q or best.get("quarter") or qd)
                merged["merge_reason"] = "same_subject_later_evidence"
                merged["latest_evidence_quarter"] = best_q
                merged["evaluated_through_quarter"] = str(eval_q or best.get("quarter") or qd)
                merged["carried_to_quarter"] = str(eval_q or best.get("quarter") or qd)
                merged["lifecycle_state"] = shared_derive_lifecycle_state(
                    target_period_norm=merged.get("target_period_norm") or "",
                    stated_quarter=merged.get("stated_quarter") or merged.get("first_seen_evidence_quarter_end") or qd,
                    latest_evidence_quarter=best_q,
                    evaluated_through_quarter=merged.get("evaluated_through_quarter"),
                    carried_to_quarter=merged.get("carried_to_quarter"),
                    current_status=merged.get("status"),
                )
                merged["status_resolution_reason"] = shared_derive_status_resolution_reason(
                    current_status=merged.get("status"),
                    latest_value=merged.get("latest"),
                    lifecycle_state=merged.get("lifecycle_state"),
                )
                updated_items.append(merged)
                resolved_count += 1
            rows_map[qd] = updated_items
        return resolved_count

    def _harmonize_same_quarter_progress(self, rows_map: Dict[date, List[Dict[str, Any]]]) -> int:
        d = self.deps
        _split_target_scope_token = d.split_target_scope_token
        _progress_context_key = self._progress_context_key
        _format_with_time = self._format_with_time
        _append_follow_rationale = self._append_follow_rationale
        updates = 0
        for qd, items in rows_map.items():
            if not isinstance(qd, date) or not isinstance(items, list):
                continue
            full_operational_latest = ""
            agreement_seen = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                blob = glx_normalize_text(
                    " | ".join(
                        [
                            str(item.get("latest") or ""),
                            str(item.get("rationale") or ""),
                            str(item.get("_source_snip") or ""),
                        ]
                    )
                )
                if re.search(r"\b(fully operational|fully online)\b", blob, re.I) and re.search(r"\badvantage nebraska\b", blob, re.I):
                    full_operational_latest = _format_with_time("Advantage Nebraska fully operational", blob, qd)
                if re.search(r"\b45z\b[^|]{0,120}\bagreement executed\b|\bagreement executed\b[^|]{0,120}\b45z\b", blob, re.I):
                    agreement_seen = True
            for item in items:
                if not isinstance(item, dict):
                    continue
                metric_txt = str(item.get("metric_ref") or "")
                context_key = _progress_context_key(
                    metric_txt,
                    " | ".join(
                        [
                            str(item.get("target") or ""),
                            str(item.get("latest") or ""),
                            str(item.get("rationale") or ""),
                        ]
                    ),
                    item.get("promise_type"),
                )
                if (
                    context_key != "45z_monetization"
                    or not str(item.get("target") or "").strip()
                    or _split_target_scope_token(item) != "company_total"
                ):
                    continue
                lifecycle_subject_key = str(item.get("lifecycle_subject_key") or item.get("promise_lifecycle_key") or "").strip().lower()
                if "monetization" in lifecycle_subject_key:
                    # Do not over-resolve monetization targets with operational/startup evidence.
                    continue
                latest_txt = str(item.get("latest") or "").strip()
                if latest_txt and latest_txt.lower() != "not yet measurable" and not re.search(r"\bdisclosed in 2026\b|\bopportunity\b", latest_txt, re.I):
                    continue
                if not full_operational_latest:
                    continue
                new_latest = (
                    "Advantage Nebraska fully operational; 45Z agreement executed"
                    if agreement_seen
                    else full_operational_latest
                )
                item["latest"] = new_latest
                item["status"] = "on_track"
                item["rationale"] = _append_follow_rationale(
                    str(item.get("rationale") or ""),
                    full_operational_latest,
                    qd,
                    qd,
                )
                item["last_seen_quarter_end"] = str(qd)
                item["last_seen_evidence_quarter_end"] = str(qd)
                item["last_seen_text_quarter_end"] = str(qd)
                item["carried_to_quarter_end"] = str(qd)
                item["evaluated_through"] = str(qd)
                item["merge_reason"] = str(item.get("merge_reason") or "same_subject_same_period")
                item["latest_evidence_quarter"] = str(qd)
                item["evaluated_through_quarter"] = str(qd)
                item["carried_to_quarter"] = str(qd)
                item["lifecycle_state"] = shared_derive_lifecycle_state(
                    target_period_norm=item.get("target_period_norm") or "",
                    stated_quarter=item.get("stated_quarter") or item.get("first_seen_evidence_quarter_end") or qd,
                    latest_evidence_quarter=str(qd),
                    evaluated_through_quarter=str(qd),
                    carried_to_quarter=str(qd),
                    current_status=item.get("status"),
                )
                item["status_resolution_reason"] = shared_derive_status_resolution_reason(
                    current_status=item.get("status"),
                    latest_value=item.get("latest"),
                    lifecycle_state=item.get("lifecycle_state"),
                )
                updates += 1
        return updates

    def apply(self, rows_by_quarter: Dict[date, List[Dict[str, Any]]]) -> PromiseProgressFollowthroughResult:
        quarter_list = list(self.deps.quarters or [])
        resolved_count = self._apply_follow_through_resolution(rows_by_quarter, quarter_list)
        harmonized_count = self._harmonize_same_quarter_progress(rows_by_quarter)
        return PromiseProgressFollowthroughResult(
            rows_by_quarter=rows_by_quarter,
            resolved_count=int(resolved_count),
            harmonized_count=int(harmonized_count),
        )


def apply_promise_progress_followthrough(
    deps: PromiseProgressFollowthroughDeps,
    rows_by_quarter: Dict[date, List[Dict[str, Any]]],
) -> PromiseProgressFollowthroughResult:
    return PromiseProgressFollowthroughModel(deps).apply(rows_by_quarter)
