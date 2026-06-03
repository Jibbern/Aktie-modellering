"""Promise Progress row post-processing helpers."""
from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Sequence, Tuple

import pandas as pd

from .guidance_lexicon import (
    dedup_text_key as glx_dedup_text_key,
    normalize_text as glx_normalize_text,
)


@dataclass(frozen=True)
class PromiseProgressRowsDeps:
    is_pbi_profile: bool
    is_gpre_profile: bool
    quarters: Sequence[date]
    ui_info_rows: List[Dict[str, Any]]
    record_writer_substage: Callable[..., None]
    classify_pbi_metric_label: Callable[..., str]
    gpre_clean_visible_promise_metric: Callable[..., str]
    gpre_bad_visible_promise_reason: Callable[..., bool]
    split_target_metric_display: Callable[..., str]
    source_rank: Callable[..., Any]
    text_fragment_penalty: Callable[..., Any]
    is_45z_crush_margin_support_only: Callable[..., bool]
    extract_45z_monetization_target_display: Callable[..., str]
    strong_45z_2026_target_display: Callable[..., str]
    extract_money_targets_for_display: Callable[..., Any]
    extract_45z_realized_progress_text: Callable[..., str]
    local_slide_45z_realized_text: Callable[..., str]
    load_local_45z_realized_basis: Callable[..., str]
    follow_through_family_key: Callable[..., str]
    follow_through_theme_key: Callable[..., str]
    progress_context_key: Callable[..., str]
    follow_status_weight: Callable[..., int]
    latest_basis_strength: Callable[..., int]
    format_with_time: Callable[..., str]
    shared_pick_best_subject_row_for_quarter: Callable[..., Any]


@dataclass(frozen=True)
class PromiseProgressRowsResult:
    rows_by_quarter: Dict[date, List[Dict[str, Any]]]
    repaired_split_progress: int
    deduped_progress_rows: int
    display_deduped_progress_rows: int
    progress_dedupe_started: float
def _repair_split_target_progress_rows(deps: PromiseProgressRowsDeps, rows_map: Dict[date, List[Dict[str, Any]]]) -> int:
    repaired = 0
    for qd, rows in rows_map.items():
        actual_rows = [r for r in rows if str(r.get("row_type") or "").strip().lower() not in {"section", "blank"}]
        if not actual_rows:
            continue
        has_total = False
        has_named = False
        has_remaining = False
        for item in actual_rows:
            blob = glx_normalize_text(
                " | ".join(
                    [
                        str(item.get("target") or ""),
                        str(item.get("latest") or ""),
                        str(item.get("rationale") or ""),
                        str(item.get("_source_snip") or ""),
                    ]
                )
            )
            family_key = str(item.get("target_family_key") or "").strip().lower()
            disp = str(item.get("metric_display") or _display_progress_metric(deps, item) or "").strip()
            disp_low = disp.lower()
            monetization_like = bool(
                re.search(r"\b45z\b", blob, re.I)
                and not re.search(r"\bqualif(?:y|ies|ied|ication)\b", blob, re.I)
                and (
                    re.search(r"\b(monetization|net of discounts|income tax benefit)\b", blob, re.I)
                    or re.search(r"\b45z\b[^|]{0,120}\bagreement executed\b|\bagreement executed\b[^|]{0,120}\b45z\b", blob, re.I)
                )
                and re.search(r"\b(q4|fourth quarter)\b", " ".join([str(item.get('target') or ''), blob]), re.I)
            )
            if disp == "45Z-related Adjusted EBITDA":
                has_total = True
            if "ebitda opportunity" in disp_low or re.search(r"\badvantage nebraska\b", blob, re.I) and re.search(r"\bebitda\b", blob, re.I):
                has_named = True
            if re.search(r"\b(?:remaining|other|legacy|non-core)\s+(?:facilities|plants|assets?|segments?|operations?|sites?)\b", " ".join([disp, blob]), re.I):
                has_remaining = True
            rationale_blob = glx_normalize_text(
                " | ".join(
                    [
                        str(item.get("rationale") or ""),
                        str(item.get("_source_snip") or ""),
                    ]
                )
            )
            if family_key == "advantage_nebraska_45z" or monetization_like:
                better_metric = deps.split_target_metric_display(str(item.get("metric_ref") or ""), rationale_blob or blob, item)
                if better_metric and better_metric != disp:
                    item["metric_display"] = better_metric
                    disp = better_metric
                    disp_low = disp.lower()
                    repaired += 1
                if monetization_like and disp != "45Z Adjusted EBITDA / monetization":
                    item["metric_display"] = "45Z Adjusted EBITDA / monetization"
                    disp = "45Z Adjusted EBITDA / monetization"
                    disp_low = disp.lower()
                    repaired += 1
                better_target = deps.extract_45z_monetization_target_display(rationale_blob or blob, qd, item.get("target"))
                if better_target and (
                    not str(item.get("target") or "").strip()
                    or deps.is_45z_crush_margin_support_only(" | ".join([str(item.get("target") or ""), rationale_blob]))
                    or re.search(r"\b59\.6\b", str(item.get("target") or ""))
                ):
                    item["target"] = better_target
                    repaired += 1
                better_latest = deps.extract_45z_realized_progress_text(rationale_blob or blob, qd)
                if not better_latest and re.search(r"\b(q4|fourth quarter)\b", str(item.get("target") or ""), re.I):
                    better_latest = deps.local_slide_45z_realized_text(qd)
                if not better_latest and re.search(r"\b(q4|fourth quarter)\b", str(item.get("target") or ""), re.I):
                    better_latest = deps.load_local_45z_realized_basis(qd)
                if not better_latest and re.search(r"\b45z\b", rationale_blob, re.I):
                    better_latest = deps.load_local_45z_realized_basis(qd)
                if (
                    better_latest
                    and re.search(r"\bnet of discounts\b", rationale_blob, re.I)
                    and "net of discounts" not in str(better_latest).lower()
                    and re.search(r"\b45z value realized\b", str(better_latest), re.I)
                ):
                    if "YTD" in str(better_latest):
                        better_latest = re.sub(
                            r"\b45Z value realized\b",
                            "45Z value realized (net of discounts)",
                            str(better_latest),
                            flags=re.I,
                        )
                    else:
                        better_latest = re.sub(
                            r"\b45Z value realized\b",
                            "YTD 45Z value realized (net of discounts)",
                            str(better_latest),
                            flags=re.I,
                        )
                if better_latest and (
                    not str(item.get("latest") or "").strip()
                    or str(item.get("latest") or "").strip().lower() in {"not yet measurable", "expected in 2026"}
                    or str(item.get("latest") or "").strip().lower().endswith("disclosed")
                    or re.search(r"\b(agreement executed|fully operational|online/ramping)\b", str(item.get("latest") or ""), re.I)
                ) and (
                    re.search(r"\b(q4|fourth quarter|net of discounts|income tax benefit|production tax credits?)\b", rationale_blob, re.I)
                    or re.search(r"\b45z\b", str(item.get("target") or ""), re.I)
                ):
                    item["latest"] = better_latest
                    repaired += 1
            if re.search(r"\b(?:remaining|other|legacy|non-core)\s+(?:facilities|plants|assets?|segments?|operations?|sites?)\b", blob, re.I):
                amounts = deps.extract_money_targets_for_display(rationale_blob)
                if amounts:
                    item["target"] = f"> ${max(amounts)/1e6:,.1f}m expected in 2026"
                    repaired += 1
                item["metric_display"] = "45Z from remaining facilities"
            elif ("ebitda opportunity" in disp_low) or re.search(r"\badvantage nebraska\b", blob, re.I) and re.search(r"\bebitda opportunity\b", blob, re.I):
                amounts = deps.extract_money_targets_for_display(rationale_blob)
                if amounts:
                    item["target"] = f"> ${max(amounts)/1e6:,.1f}m in 2026"
                    repaired += 1
                item["metric_display"] = "Advantage Nebraska EBITDA opportunity"
        if has_named and has_remaining and not has_total:
            target_display = deps.strong_45z_2026_target_display(
                "45Z-related Adjusted EBITDA in 2026",
                qd,
                "45Z-related Adjusted EBITDA in 2026",
            )
            if target_display:
                rows.insert(
                    1 if rows else 0,
                    {
                        "promise_id": hashlib.sha1(f"{qd}|progress_total_45z".encode("utf-8")).hexdigest()[:12],
                        "metric_ref": "45Z monetization / EBITDA",
                        "metric_display": "45Z-related Adjusted EBITDA",
                        "target": target_display,
                        "latest": deps.format_with_time("Advantage Nebraska fully operational", "Advantage Nebraska fully operational", qd),
                        "status": "on_track",
                        "rationale": "Company-wide 2026 45Z-related Adjusted EBITDA target supported by Nebraska and remaining-facilities component targets.",
                        "target_family_key": "advantage_nebraska_45z",
                        "scope_key": "company_total",
                        "scope_kind": "total",
                    },
                )
                repaired += 1
    return repaired
def _display_progress_metric(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> str:
    metric_display = str(item.get("metric_display") or "").strip()
    metric_display_low = metric_display.lower()
    rationale_txt = glx_normalize_text(
        " | ".join(
            [
                str(item.get("rationale") or ""),
                str(item.get("_source_snip") or ""),
                str(item.get("latest") or ""),
                str(item.get("target") or ""),
            ]
        )
    )
    if deps.is_pbi_profile:
        better_pbi = deps.classify_pbi_metric_label(
            " | ".join(
                [
                    metric_display,
                    str(item.get("metric_ref") or ""),
                    str(item.get("target") or ""),
                    str(item.get("latest") or ""),
                    str(item.get("rationale") or ""),
                ]
            ),
            metric_display,
        )
        if better_pbi and better_pbi.lower() not in {"management target"}:
            return better_pbi
    if deps.is_gpre_profile:
        cleaned_gpre = deps.gpre_clean_visible_promise_metric(metric_display or str(item.get("metric_ref") or ""), rationale_txt, item)
        if cleaned_gpre and not deps.gpre_bad_visible_promise_reason(cleaned_gpre, rationale_txt, item.get("latest"), item.get("target")):
            item["metric_display"] = cleaned_gpre
            return cleaned_gpre
    if metric_display and metric_display_low not in {
        "management target",
        "strategic milestone",
        "production tax 45z generation",
        "tax 45z generation",
        "qualify for production tax 45z generation",
        "fourth quarter 45z generation",
    }:
        return metric_display
    metric_txt = str(item.get("metric_ref") or "").strip()
    context_key = deps.progress_context_key(metric_txt, rationale_txt, item.get("promise_type"))
    if context_key == "cost_savings_program":
        structure_role = str(item.get("target_structure_role") or item.get("stage_kind") or "").strip().lower()
        structure_kind = str(item.get("target_structure_kind") or "").strip().lower()
        if structure_role in {"first_tranche", "initial", "phase_1"}:
            return "Cost savings tranche 1"
        if structure_role in {"additional_tranche", "phase_2", "remaining"}:
            return "Cost savings tranche 2"
        if structure_kind in {"program_total", "stage_and_total"} or re.search(r"\bannualized program\b", rationale_txt, re.I):
            return "Cost savings target"
        return "Cost savings target"
    if context_key == "45z_plant_qualification":
        return "45Z facility qualification"
    if context_key == "45z_monetization":
        if re.search(r"\b45z[- ]related\b[^.]{0,60}\badjusted ebitda\b|\bat least \$?\d", rationale_txt, re.I):
            return "45Z-related Adjusted EBITDA outlook"
        if re.search(
            r"\b(contributed|realized|recognized|recorded|net of discounts(?: and other costs)?)\b",
            rationale_txt,
            re.I,
        ):
            return "45Z monetization"
        if re.search(r"\b(monetization|income tax benefit|net of discounts|agreement executed|tax credit)\b", rationale_txt, re.I):
            return "45Z monetization outlook"
        return "45Z monetization outlook"
    if context_key == "debt_reduction":
        return "Debt reduction"
    if context_key == "york_operational":
        return "Advantage Nebraska startup"
    if context_key == "central_city_wood_river":
        return "Carbon capture commissioning"
    if context_key == "construction_permit":
        return "Carbon capture commissioning"
    if context_key == "advantage_platform":
        return "Advantage Nebraska startup"
    return metric_txt
def _promise_progress_subject_bucket(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> str:
    canonical_key = str(item.get("canonical_subject_key") or "").strip().lower()
    if canonical_key:
        return canonical_key
    scope_key = str(item.get("scope_key") or "").strip().lower()
    structure_role = str(item.get("target_structure_role") or item.get("stage_kind") or "").strip().lower()
    if scope_key and scope_key != "company_total":
        return f"{scope_key}|{structure_role}" if structure_role and structure_role not in {"", "single", "program_total"} else scope_key
    metric_txt = str(item.get("metric_ref") or "").strip()
    blob = glx_normalize_text(
        " | ".join(
            [
                str(item.get("target") or ""),
                str(item.get("latest") or ""),
                str(item.get("rationale") or ""),
                str(item.get("_source_snip") or ""),
            ]
        )
    )
    low = blob.lower()
    context_key = deps.progress_context_key(metric_txt, blob, item.get("promise_type"))
    if "york" in low or "tallgrass trailblazer" in low:
        return "york"
    if "central city" in low or "wood river" in low:
        return "central_city_wood_river"
    if context_key == "construction_permit":
        return "construction_permit"
    if context_key == "advantage_platform" or "advantage nebraska" in low:
        return "advantage_platform"
    if context_key == "debt_reduction":
        base_bucket = "debt_reduction"
        return f"{base_bucket}|{structure_role}" if structure_role and structure_role not in {"", "single", "program_total"} else base_bucket
    if context_key == "cost_savings_program":
        base_bucket = "cost_savings"
        return f"{base_bucket}|{structure_role}" if structure_role and structure_role not in {"", "single"} else base_bucket
    if context_key == "45z_monetization":
        base_bucket = "45z_monetization"
        return f"{base_bucket}|{structure_role}" if structure_role and structure_role not in {"", "single", "program_total"} else base_bucket
    if context_key == "45z_plant_qualification":
        return "45z_plant_qualification"
    base_bucket = context_key or glx_dedup_text_key(_display_progress_metric(deps, item))[:80]
    if structure_role and structure_role not in {"", "single", "program_total"}:
        return f"{base_bucket}|{structure_role}"
    return base_bucket
def _promise_progress_visible_category_local(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> str:
    metric_display = str(_display_progress_metric(deps, item) or item.get("metric_display") or item.get("metric_ref") or "").strip()
    metric_low = metric_display.lower()
    blob = glx_normalize_text(
        " | ".join(
            [
                metric_display,
                str(item.get("metric_ref") or ""),
                str(item.get("target") or ""),
                str(item.get("latest") or ""),
                str(item.get("rationale") or ""),
                str(item.get("_source_snip") or ""),
            ]
        )
    )
    low = blob.lower()
    if metric_low.endswith("guidance") or re.search(r"\b(guidance|outlook|target)\b", metric_low, re.I):
        return "Guidance / outlook"
    if re.search(r"\b(fcf|free cash flow|working capital|cash flow)\b", low, re.I):
        return "Cash flow / FCF / working capital"
    if re.search(r"\b(liquidity|balance sheet|debt|revolver|availability|interest expense|refinanc|deleverag|net debt|mezzanine)\b", low, re.I):
        return "Debt / liquidity / balance sheet"
    if re.search(r"\b(repurchase|buyback|dividend|shareholder returns?|capital allocation)\b", low, re.I):
        return "Capital allocation / shareholder returns"
    if re.search(r"\b(carbo[n]? capture|fully operational|online and ramping|startup|commissioning|milestone|qualification|commercialization|agreement executed|utilization)\b", low, re.I):
        return "Operations / commercialization / milestones"
    if re.search(r"\b(cost savings|strategic review|management framing|risk management|non-core asset monetization|positive ebitda)\b", low, re.I):
        return "Programs / initiatives / management framing"
    if re.search(r"\b(improved|declined|increased|decreased|up |down |yoy|qoq|better|worse|from .* prior|from .* yoy)\b", low, re.I):
        return "Results / drivers / better vs prior"
    if re.search(r"\b(results?|drivers?|contributed|realized|executed|completed|progressing)\b", low, re.I):
        return "Results / drivers"
    if re.search(r"\b(expected|expect|on track|will|continue to|continues to|ahead of plan)\b", low, re.I):
        return "Guidance / outlook"
    return "Programs / initiatives / management framing"
def _promise_progress_visible_category_rank_local(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> int:
    return {
        "Guidance / outlook": 0,
        "Results / drivers": 1,
        "Results / drivers / better vs prior": 2,
        "Cash flow / FCF / working capital": 3,
        "Debt / liquidity / balance sheet": 4,
        "Capital allocation / shareholder returns": 5,
        "Operations / commercialization / milestones": 6,
        "Programs / initiatives / management framing": 7,
        "One-time items / restructuring": 8,
        "Other / footnotes": 9,
    }.get(_promise_progress_visible_category_local(deps, item), 9)
def _promise_progress_theme_key(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> Tuple[str, str, str]:
    canonical_key = str(item.get("canonical_subject_key") or "").strip().lower()
    lifecycle_key = str(item.get("promise_lifecycle_key") or item.get("lifecycle_key") or "").strip().lower()
    if canonical_key:
        metric_disp = glx_dedup_text_key(_display_progress_metric(deps, item))
        return lifecycle_key or canonical_key, canonical_key, metric_disp
    metric_txt = str(item.get("metric_ref") or "").strip()
    blob = glx_normalize_text(
        " | ".join(
            [
                str(item.get("target") or ""),
                str(item.get("latest") or ""),
                str(item.get("rationale") or ""),
                str(item.get("_source_snip") or ""),
            ]
        )
    )
    theme_key = deps.follow_through_theme_key(
        metric_txt,
        item.get("promise_key"),
        item.get("promise_type"),
        blob,
        item.get("target"),
    )
    family_key = deps.follow_through_family_key(theme_key)
    subject_bucket = _promise_progress_subject_bucket(deps, item)
    metric_disp = glx_dedup_text_key(_display_progress_metric(deps, item))
    return family_key, subject_bucket, metric_disp
def _normalize_progress_text_for_dedupe(deps: PromiseProgressRowsDeps, text: Any, family_key: str) -> str:
    txt = glx_normalize_text(str(text or "")).lower()
    if not txt:
        return ""
    txt = re.sub(r"[^\w\s]", " ", txt)
    if family_key == "advantage_nebraska_45z":
        txt = re.sub(r"\bonline and ramping\b|\bramping up\b", " online_ramping ", txt)
        txt = re.sub(r"\bcommissioning\b|\bstartup\b", " startup_progress ", txt)
        txt = re.sub(r"\bfully operational\b|\bfully online\b", " fully_operational ", txt)
        txt = re.sub(
            r"\b(monetization agreement executed|tax credit agreement executed|agreement executed)\b",
            " 45z_agreement_executed ",
            txt,
        )
    txt = re.sub(
        r"\b(management|company|continues|remains|expected|update|progress|initiative)\b",
        " ",
        txt,
    )
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt
def _progress_result_bucket(deps: PromiseProgressRowsDeps, status_in: Any) -> str:
    status_low = str(status_in or "").strip().lower().replace("_", " ")
    return {
        "completed": "completed",
        "ahead of plan": "ahead of plan",
        "on track": "on track",
        "in progress": "in progress",
        "not observed": "not observed",
    }.get(status_low, status_low)
def _progress_token_overlap(deps: PromiseProgressRowsDeps, a_txt: str, b_txt: str) -> float:
    a_set = {tok for tok in str(a_txt or "").split() if len(tok) > 2}
    b_set = {tok for tok in str(b_txt or "").split() if len(tok) > 2}
    if not a_set or not b_set:
        return 0.0
    return float(len(a_set & b_set)) / float(max(1, min(len(a_set), len(b_set))))
def _rows_are_near_duplicates(deps: PromiseProgressRowsDeps, row_a: Dict[str, Any], row_b: Dict[str, Any], qd: date) -> bool:
    if str(row_a.get("row_type") or "").strip().lower() == "section":
        return False
    if str(row_b.get("row_type") or "").strip().lower() == "section":
        return False
    if str(row_a.get("promise_type") or "").strip().lower() == "guidance_range":
        return False
    if str(row_b.get("promise_type") or "").strip().lower() == "guidance_range":
        return False
    family_a, subject_a, metric_a = _promise_progress_theme_key(deps, row_a)
    family_b, subject_b, metric_b = _promise_progress_theme_key(deps, row_b)
    if family_a != family_b:
        return False
    if subject_a != subject_b:
        return False
    latest_a = _normalize_progress_text_for_dedupe(deps, row_a.get("latest"), family_a)
    latest_b = _normalize_progress_text_for_dedupe(deps, row_b.get("latest"), family_b)
    rationale_a = _normalize_progress_text_for_dedupe(deps, row_a.get("rationale"), family_a)
    rationale_b = _normalize_progress_text_for_dedupe(deps, row_b.get("rationale"), family_b)
    result_a = _progress_result_bucket(deps, row_a.get("status"))
    result_b = _progress_result_bucket(deps, row_b.get("status"))
    result_gap = abs(deps.follow_status_weight(result_a) - deps.follow_status_weight(result_b))
    target_txt_a = glx_normalize_text(str(row_a.get("target") or "")).lower()
    target_txt_b = glx_normalize_text(str(row_b.get("target") or "")).lower()
    same_target_text = bool(target_txt_a and target_txt_a == target_txt_b)
    exact_text = latest_a == latest_b and rationale_a == rationale_b and bool(latest_a or rationale_a)
    if exact_text:
        return True
    if result_gap > 1:
        return False
    metric_disp_a = _display_progress_metric(deps, row_a)
    metric_disp_b = _display_progress_metric(deps, row_b)
    generic_metric_pair = (
        _progress_metric_specificity(deps, metric_disp_a) <= 2
        or _progress_metric_specificity(deps, metric_disp_b) <= 2
    )
    metric_compatible = (
        metric_a == metric_b
        or metric_a in metric_b
        or metric_b in metric_a
        or generic_metric_pair
    )
    latest_match = bool(latest_a) and latest_a == latest_b
    latest_overlap = _progress_token_overlap(deps, latest_a, latest_b)
    rationale_contained = bool(rationale_a and rationale_b) and (rationale_a in rationale_b or rationale_b in rationale_a)
    rationale_overlap = _progress_token_overlap(deps, rationale_a, rationale_b)
    if (
        family_a == "advantage_nebraska_45z"
        and result_gap <= 1
        and bool(
            re.search(r"\b(45z|monetization|agreement executed|production tax credits?)\b", f"{latest_a} {rationale_a}", re.I)
            and re.search(r"\b(45z|monetization|agreement executed|production tax credits?)\b", f"{latest_b} {rationale_b}", re.I)
        )
    ):
        target_a = _target_informativeness(deps, row_a)
        target_b = _target_informativeness(deps, row_b)
        target_txt_a = glx_normalize_text(str(row_a.get("target") or "")).lower()
        target_txt_b = glx_normalize_text(str(row_b.get("target") or "")).lower()
        if target_txt_a and target_txt_a == target_txt_b:
            return True
        if subject_a == subject_b and (
            latest_overlap >= 0.40 or rationale_overlap >= 0.58 or (target_a > 0 and target_b == 0) or (target_b > 0 and target_a == 0)
        ):
            return True
    if same_target_text and subject_a == subject_b and result_gap <= 1:
        if latest_match or latest_overlap >= 0.38 or rationale_overlap >= 0.45 or rationale_contained:
            return True
    if metric_disp_a == metric_disp_b and same_target_text and result_gap <= 1:
        return True
    subject_compatible = subject_a == subject_b or not subject_a or not subject_b
    if metric_disp_a == metric_disp_b and subject_compatible and result_gap <= 1:
        if latest_match or latest_overlap >= 0.55:
            return True
        if rationale_contained and rationale_overlap >= 0.35:
            return True
    if not metric_compatible and not (
        (latest_match or latest_overlap >= 0.72) and rationale_overlap >= 0.55
    ) and not (
        rationale_contained and rationale_overlap >= 0.70
    ) and rationale_overlap < 0.9:
        return False
    if (latest_match or latest_overlap >= 0.72) and (rationale_contained or rationale_overlap >= 0.55 or not rationale_a or not rationale_b):
        return True
    if rationale_contained and result_gap <= 1:
        return True
    if rationale_overlap >= 0.82 and result_gap <= 1:
        return True
    return False
def _progress_source_rank(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> int:
    src_doc = str(item.get("_source_doc") or "")
    low = src_doc.lower()
    if "transcript" in low:
        src_type = "transcript"
    elif "presentation" in low or "slides" in low:
        src_type = "earnings_presentation"
    elif any(tok in low for tok in ["press", "release", "ex99", "exhibit991"]):
        src_type = "earnings_release"
    elif low.endswith(".htm") or low.endswith(".html"):
        src_type = "filing"
    else:
        src_type = ""
    try:
        return int(deps.source_rank(src_type, src_doc))
    except Exception:
        return 99
def _progress_metric_specificity(deps: PromiseProgressRowsDeps, metric_disp: str) -> int:
    return {
        "Cost savings program": 6,
        "Cost savings tranche 1": 6,
        "Cost savings tranche 2": 6,
        "45Z monetization / EBITDA": 6,
        "45Z Adjusted EBITDA / monetization": 6,
        "45Z plant qualification readiness": 6,
        "Debt reduction milestone": 6,
        "Carbon capture commissioning": 5,
        "Advantage Nebraska startup": 5,
        "Strategic milestone": 2,
        "Management target": 1,
        "production tax 45Z generation": 1,
        "tax 45Z generation": 1,
        "qualify for production tax 45Z generation": 1,
        "fourth quarter 45Z generation": 1,
    }.get(str(metric_disp or "").strip(), 3)
def _target_informativeness(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> int:
    tgt_txt = str(item.get("target") or "").strip()
    if not tgt_txt:
        return 0
    if pd.notna(pd.to_numeric(item.get("target"), errors="coerce")) or re.search(r"\$|>=|<=|between|to\s+\$", tgt_txt, re.I):
        return 2
    return 1
def _has_explicit_timing(deps: PromiseProgressRowsDeps, item: Dict[str, Any]) -> int:
    blob = " | ".join([str(item.get("latest") or ""), str(item.get("rationale") or "")])
    return int(bool(re.search(r"\b(q[1-4]\s*20\d{2}|fy\s*20\d{2}|20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", blob, re.I)))
def _pick_best_progress_row(deps: PromiseProgressRowsDeps, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _score(row: Dict[str, Any]) -> Tuple[int, int, int, int, int, int, float, int]:
        metric_disp = _display_progress_metric(deps, row)
        text_blob = glx_normalize_text(" | ".join([str(row.get("latest") or ""), str(row.get("rationale") or "")]))
        return (
            _progress_metric_specificity(deps, metric_disp),
            deps.latest_basis_strength(row.get("latest")),
            _has_explicit_timing(deps, row),
            _target_informativeness(deps, row),
            -int(deps.text_fragment_penalty(text_blob) or 0),
            -_progress_source_rank(deps, row),
            float(row.get("_score") or 0.0),
            -int(row.get("_status_pri") or 9),
        )

    return max(rows, key=_score)
def _dedupe_promise_progress_rows(deps: PromiseProgressRowsDeps, rows_map: Dict[date, List[Dict[str, Any]]]) -> int:
    dropped_total = 0
    for qd, items in rows_map.items():
        if not isinstance(qd, date) or not isinstance(items, list) or not items:
            continue
        progress_entries = [
            (idx, item)
            for idx, item in enumerate(items)
            if isinstance(item, dict)
            and str(item.get("row_type") or "").strip().lower() != "section"
            and (
                deps.is_pbi_profile
                or str(item.get("promise_type") or "").strip().lower() != "guidance_range"
            )
        ]
        replacement_at: Dict[int, Dict[str, Any]] = {}
        skip_idxs: set[int] = set()
        visited: set[int] = set()
        dropped_this_q = 0
        grouped_by_subject: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
        for orig_idx, item in progress_entries:
            subject_key = str(
                item.get("lifecycle_subject_key")
                or item.get("promise_lifecycle_key")
                or item.get("canonical_subject_key")
                or ""
            ).strip()
            if subject_key:
                grouped_by_subject.setdefault(subject_key, []).append((orig_idx, item))
        for subject_items in grouped_by_subject.values():
            if len(subject_items) <= 1:
                continue
            best_row = deps.shared_pick_best_subject_row_for_quarter([pair[1] for pair in subject_items])
            if best_row is None:
                continue
            group_orig_idxs = [pair[0] for pair in subject_items]
            start_idx = min(group_orig_idxs)
            best_row["merge_reason"] = str(best_row.get("merge_reason") or "canonical_subject_match")
            best_row["collapse_reason"] = str(best_row.get("collapse_reason") or "same_subject_same_block")
            replacement_at[start_idx] = best_row
            skip_idxs.update(group_orig_idxs)
            skip_idxs.discard(start_idx)
            dropped_this_q += len(group_orig_idxs) - 1
        grouped_by_promise_id: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
        for orig_idx, item in progress_entries:
            promise_id = str(item.get("promise_id") or "").strip()
            if promise_id:
                grouped_by_promise_id.setdefault(promise_id, []).append((orig_idx, item))
        for promise_items in grouped_by_promise_id.values():
            if len(promise_items) <= 1:
                continue
            best_row = deps.shared_pick_best_subject_row_for_quarter([pair[1] for pair in promise_items])
            if best_row is None:
                continue
            group_orig_idxs = [pair[0] for pair in promise_items]
            start_idx = min(group_orig_idxs)
            best_row["merge_reason"] = str(best_row.get("merge_reason") or "duplicate_weaker_row")
            best_row["collapse_reason"] = str(best_row.get("collapse_reason") or "same_subject_same_block")
            replacement_at[start_idx] = best_row
            skip_idxs.update(group_orig_idxs)
            skip_idxs.discard(start_idx)
            dropped_this_q += len(group_orig_idxs) - 1
        for pos, (orig_idx, item) in enumerate(progress_entries):
            if orig_idx in skip_idxs or orig_idx in replacement_at:
                continue
            if pos in visited:
                continue
            group_pos = {pos}
            changed = True
            while changed:
                changed = False
                for pos_b, (_, cand_b) in enumerate(progress_entries):
                    if pos_b in group_pos:
                        continue
                    if any(
                        _rows_are_near_duplicates(deps, progress_entries[g][1], cand_b, qd)
                        for g in group_pos
                    ):
                        group_pos.add(pos_b)
                        changed = True
            visited.update(group_pos)
            if len(group_pos) <= 1:
                continue
            group_pairs = [progress_entries[g] for g in sorted(group_pos)]
            best_row = _pick_best_progress_row(deps, [pair[1] for pair in group_pairs])
            group_orig_idxs = [pair[0] for pair in group_pairs]
            start_idx = min(group_orig_idxs)
            replacement_at[start_idx] = best_row
            skip_idxs.update(group_orig_idxs)
            dropped_this_q += len(group_orig_idxs) - 1
        if dropped_this_q <= 0:
            continue
        rebuilt: List[Dict[str, Any]] = []
        for idx, item in enumerate(items):
            if idx in replacement_at:
                rebuilt.append(replacement_at[idx])
                continue
            if idx in skip_idxs:
                continue
            rebuilt.append(item)
        rows_map[qd] = rebuilt
        dropped_total += dropped_this_q
        deps.ui_info_rows.append(
            {
                "quarter": qd,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"duplicate_progress_rows_dropped count={int(dropped_this_q)}",
                "source": "pipeline",
            }
        )
    return dropped_total
def _normalize_display_progress_text(deps: PromiseProgressRowsDeps, text_in: Any) -> str:
    txt = glx_normalize_text(str(text_in or "")).lower()
    if not txt:
        return ""
    txt = re.sub(r"[^\w\s]", " ", txt)
    txt = re.sub(r"\b(plant online / ramping|online and ramping|ramping up)\b", " online_ramping ", txt)
    txt = re.sub(r"\b(management|company|continues|remains|expected|update|progress|initiative|later update)\b", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt
def _rows_are_display_duplicates(deps: PromiseProgressRowsDeps, row_a: Dict[str, Any], row_b: Dict[str, Any]) -> bool:
    if str(row_a.get("row_type") or "").strip().lower() == "section":
        return False
    if str(row_b.get("row_type") or "").strip().lower() == "section":
        return False
    if str(row_a.get("promise_type") or "").strip().lower() == "guidance_range":
        return False
    if str(row_b.get("promise_type") or "").strip().lower() == "guidance_range":
        return False
    metric_a = _display_progress_metric(deps, row_a)
    metric_b = _display_progress_metric(deps, row_b)
    if metric_a != metric_b:
        return False
    result_gap = abs(
        deps.follow_status_weight(_progress_result_bucket(deps, row_a.get("status")))
        - deps.follow_status_weight(_progress_result_bucket(deps, row_b.get("status")))
    )
    if result_gap > 1:
        return False
    raw_target_a = str(row_a.get("target") or "").strip().lower()
    raw_target_b = str(row_b.get("target") or "").strip().lower()
    if raw_target_a and raw_target_a == raw_target_b:
        return True
    target_a = _normalize_display_progress_text(deps, row_a.get("target"))
    target_b = _normalize_display_progress_text(deps, row_b.get("target"))
    latest_a = _normalize_display_progress_text(deps, row_a.get("latest"))
    latest_b = _normalize_display_progress_text(deps, row_b.get("latest"))
    rationale_a = _normalize_display_progress_text(deps, row_a.get("rationale"))
    rationale_b = _normalize_display_progress_text(deps, row_b.get("rationale"))
    if target_a and target_a == target_b:
        if latest_a == latest_b:
            return True
        if latest_a and latest_b and (latest_a in latest_b or latest_b in latest_a):
            return True
        if rationale_a and rationale_b and (rationale_a in rationale_b or rationale_b in rationale_a):
            return True
    if not target_a and not target_b:
        if latest_a and latest_b and (latest_a == latest_b or latest_a in latest_b or latest_b in latest_a):
            return True
        if rationale_a and rationale_b and (rationale_a in rationale_b or rationale_b in rationale_a):
            return True
    return False
def _dedupe_display_progress_rows(deps: PromiseProgressRowsDeps, rows_map: Dict[date, List[Dict[str, Any]]]) -> int:
    dropped_total = 0
    for qd, items in rows_map.items():
        progress_entries = [
            (idx, item)
            for idx, item in enumerate(items)
            if isinstance(item, dict)
            and str(item.get("row_type") or "").strip().lower() != "section"
            and (
                deps.is_pbi_profile
                or str(item.get("promise_type") or "").strip().lower() != "guidance_range"
            )
        ]
        replacement_at: Dict[int, Dict[str, Any]] = {}
        skip_idxs: set[int] = set()
        visited: set[int] = set()
        dropped_this_q = 0
        for pos, (orig_idx, item) in enumerate(progress_entries):
            if pos in visited:
                continue
            group_pos = {pos}
            changed = True
            while changed:
                changed = False
                for pos_b, (_, cand_b) in enumerate(progress_entries):
                    if pos_b in group_pos:
                        continue
                    if any(_rows_are_display_duplicates(deps, progress_entries[g][1], cand_b) for g in group_pos):
                        group_pos.add(pos_b)
                        changed = True
            visited.update(group_pos)
            if len(group_pos) <= 1:
                continue
            group_pairs = [progress_entries[g] for g in sorted(group_pos)]
            best_row = _pick_best_progress_row(deps, [pair[1] for pair in group_pairs])
            group_orig_idxs = [pair[0] for pair in group_pairs]
            start_idx = min(group_orig_idxs)
            replacement_at[start_idx] = best_row
            skip_idxs.update(group_orig_idxs)
            dropped_this_q += len(group_orig_idxs) - 1
        if dropped_this_q <= 0:
            continue
        rebuilt: List[Dict[str, Any]] = []
        for idx, item in enumerate(items):
            if idx in replacement_at:
                rebuilt.append(replacement_at[idx])
                continue
            if idx in skip_idxs:
                continue
            rebuilt.append(item)
        rows_map[qd] = rebuilt
        dropped_total += dropped_this_q
        deps.ui_info_rows.append(
            {
                "quarter": qd,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"display_duplicate_progress_rows_dropped count={int(dropped_this_q)}",
                "source": "pipeline",
            }
        )
    return dropped_total
def normalize_promise_progress_rows_for_display(
    deps: PromiseProgressRowsDeps,
    rows_by_quarter: Dict[date, List[Dict[str, Any]]],
    *,
    progress_follow_started: float,
) -> PromiseProgressRowsResult:
    repaired_split_progress = _repair_split_target_progress_rows(deps, rows_by_quarter)
    if repaired_split_progress > 0:
        deps.ui_info_rows.append(
            {
                "quarter": deps.quarters[0] if deps.quarters else None,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"split_target_progress_repaired count={int(repaired_split_progress)}",
                "source": "pipeline",
            }
        )
    deps.record_writer_substage("write_excel.ui.progress_rows.follow_through", progress_follow_started)

    progress_dedupe_started = time.perf_counter()
    deduped_progress_rows = _dedupe_promise_progress_rows(deps, rows_by_quarter)
    if deduped_progress_rows > 0:
        deps.ui_info_rows.append(
            {
                "quarter": deps.quarters[0] if deps.quarters else None,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"duplicate_progress_rows_dropped_total count={int(deduped_progress_rows)}",
                "source": "pipeline",
            }
        )
    display_deduped_progress_rows = _dedupe_display_progress_rows(deps, rows_by_quarter)
    if display_deduped_progress_rows > 0:
        deps.ui_info_rows.append(
            {
                "quarter": deps.quarters[0] if deps.quarters else None,
                "metric": "Promise_Progress_UI",
                "severity": "info",
                "message": f"display_duplicate_progress_rows_dropped_total count={int(display_deduped_progress_rows)}",
                "source": "pipeline",
            }
        )
    return PromiseProgressRowsResult(
        rows_by_quarter=rows_by_quarter,
        repaired_split_progress=repaired_split_progress,
        deduped_progress_rows=deduped_progress_rows,
        display_deduped_progress_rows=display_deduped_progress_rows,
        progress_dedupe_started=progress_dedupe_started,
    )
