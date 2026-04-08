"""Run-scoped helpers for Operating_Drivers row construction.

The workbook-facing sheet render still lives in ``excel_writer_context.py``. This
module owns the heavier row-selection and row-building helpers that are safe to
reuse within one export run as long as the caller passes explicit dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .guidance_lexicon import normalize_text as glx_normalize_text


@dataclass
class OperatingDriversRuntime:
    template_index_cache: Optional[Dict[str, Any]] = None
    bridge_bundle_cache: Dict[Tuple[date, ...], Dict[date, Dict[str, Any]]] = field(default_factory=dict)
    line_index_by_quarter_cache: Optional[Dict[date, List[Dict[str, Any]]]] = None
    flat_line_index_cache: Optional[List[Dict[str, Any]]] = None
    best_text_cache: Dict[Tuple[date, Tuple[str, ...], bool], Optional[Dict[str, Any]]] = field(
        default_factory=dict
    )
    template_rows_cache: Dict[Tuple[date, str], List[Dict[str, Any]]] = field(default_factory=dict)
    template_candidate_cache: Dict[Tuple[date, str], List[Dict[str, Any]]] = field(default_factory=dict)
    text_cache: Dict[str, str] = field(default_factory=dict)
    profile_slide_signals_cache: Optional[List[Dict[str, Any]]] = None
    profile_slide_signals_by_quarter_cache: Optional[Dict[date, List[Dict[str, Any]]]] = None
    guidance_45z_docs_by_quarter_cache: Optional[Dict[date, List[Dict[str, Any]]]] = None
    canonical_crush_series_cache: Optional[Dict[date, Dict[str, Any]]] = None


@dataclass(frozen=True)
class OperatingDriversDeps:
    is_gpre_profile: bool
    source_rank_fn: Callable[..., Any]
    driver_source_display_fn: Callable[..., str]
    driver_source_note_fn: Callable[..., str]
    load_source_records_by_quarter_fn: Callable[[], Dict[date, List[Dict[str, Any]]]]
    load_template_index_fn: Callable[[], Dict[str, Any]]
    operating_quarters_fn: Callable[[], List[date]]
    load_line_index_by_quarter_fn: Callable[[], Dict[date, List[Dict[str, Any]]]]
    load_bridge_bundle_map_fn: Callable[[List[date]], Dict[date, Dict[str, Any]]]
    template_spec_fn: Callable[[Any], Dict[str, Any]]
    candidate_records_for_template_fn: Callable[..., List[Dict[str, Any]]]
    profile_slide_signals_for_quarter_fn: Callable[[date], List[Dict[str, Any]]]
    load_45z_guidance_docs_by_quarter_fn: Callable[[], Dict[date, List[Dict[str, Any]]]]
    parse_gpre_crush_margin_pair_fn: Callable[[Any], Optional[Tuple[float, float, str]]]
    cached_metric_parse_fn: Callable[..., Any]
    driver_snippet_fn: Callable[..., str]
    qn_is_complete_signal_text_fn: Callable[[Any], bool]
    driver_best_text_record_fn: Callable[..., Optional[Dict[str, Any]]]
    parse_utilization_value_fn: Callable[[Any], Optional[float]]
    parse_driver_number_fn: Callable[[Any], Optional[float]]
    parse_distillers_grains_k_tons_fn: Callable[[Any], Optional[float]]
    parse_uhp_k_tons_fn: Callable[[Any], Optional[float]]
    parse_corn_consumed_m_bushels_fn: Callable[[Any], Optional[float]]
    parse_rin_impact_value_m_fn: Callable[[Any], Optional[float]]
    parse_crush_margin_value_m_fn: Callable[[Any], Optional[float]]
    parse_45z_realized_value_m_fn: Callable[[Any], Optional[float]]
    parse_renewable_corn_oil_m_lbs_fn: Callable[[Any], Optional[float]]
    extract_45z_target_candidates_fn: Callable[[Any, date], List[Dict[str, Any]]]
    extract_45z_target_display_fn: Callable[[Any, date], str]
    text_fragment_penalty_fn: Callable[[Any], int]
    extract_money_targets_for_display_fn: Callable[[Any], List[float]]
    parse_threshold_amount_m_fn: Callable[[Any], Optional[float]]
    timed_substage_fn: Callable[[str], Any]


def _driver_quality_rank(row: Dict[str, Any], *, source_rank_fn: Callable[..., Any]) -> Tuple[int, int, int, int]:
    quality_order = {"exact": 0, "modeled": 1, "text-derived": 2, "inferred": 3}
    value_rank = 0 if pd.notna(pd.to_numeric(row.get("Value"), errors="coerce")) else 1
    return (
        value_rank,
        quality_order.get(str(row.get("Quality") or "").strip().lower(), 9),
        int(source_rank_fn(row.get("_source_type"), row.get("_source_doc"))),
        len(str(row.get("Commentary") or "")),
    )


def merge_driver_rows(
    existing: Dict[str, Any],
    candidate: Dict[str, Any],
    *,
    source_rank_fn: Callable[..., Any],
) -> Dict[str, Any]:
    keep = dict(existing)
    alt = dict(candidate)
    if _driver_quality_rank(candidate, source_rank_fn=source_rank_fn) < _driver_quality_rank(
        existing,
        source_rank_fn=source_rank_fn,
    ):
        keep, alt = alt, keep
    for fld in ("Value", "Unit", "Commentary", "Source", "Quality", "_source_doc", "_source_note", "_source_type"):
        keep_txt = str(keep.get(fld) or "").strip()
        alt_txt = str(alt.get(fld) or "").strip()
        if not keep_txt and alt_txt:
            keep[fld] = alt.get(fld)
    if pd.notna(pd.to_numeric(alt.get("Value"), errors="coerce")) and pd.isna(
        pd.to_numeric(keep.get("Value"), errors="coerce")
    ):
        keep["Value"] = alt.get("Value")
    return keep


def make_driver_row(
    qd: date,
    driver_key: str,
    driver_group: str,
    driver_label: str,
    source_type: str,
    source_doc: str,
    *,
    driver_source_display_fn: Callable[..., str],
    driver_source_note_fn: Callable[..., str],
    commentary: str = "",
    quality: str = "text-derived",
    value: Any = None,
    unit: str = "",
    scope: str = "",
    source_note: str = "",
) -> Dict[str, Any]:
    return {
        "Quarter": qd,
        "Driver group": driver_group,
        "Driver": driver_label,
        "Value": value,
        "Unit": unit,
        "QoQ change": "",
        "YoY change": "",
        "Source": driver_source_display_fn(source_type, source_doc),
        "Commentary": commentary,
        "Quality": quality,
        "_driver_key": driver_key,
        "_driver_scope": scope or "",
        "_source_type": source_type,
        "_source_doc": source_doc,
        "_source_note": source_note or driver_source_note_fn(source_doc, commentary),
    }


def gpre_canonical_crush_series_for_drivers(
    runtime: OperatingDriversRuntime,
    deps: OperatingDriversDeps,
) -> Dict[date, Dict[str, Any]]:
    if not deps.is_gpre_profile:
        return {}
    if isinstance(runtime.canonical_crush_series_cache, dict):
        return dict(runtime.canonical_crush_series_cache)

    def _candidate_rank_local(target_q: date, source_q: date, source_type: str, source_rank: int) -> Tuple[int, int, int, int]:
        source_type_low = str(source_type or "").strip().lower()
        official_rank = 0 if source_type_low == "earnings_release" else 1 if source_type_low == "presentation" else 2
        # Prefer the direct same-quarter disclosure over later quarter prior-period comparators.
        comparator_rank = 0 if target_q == source_q else 1
        return (
            comparator_rank,
            official_rank,
            -int(pd.Timestamp(source_q).value),
            int(source_rank),
        )

    series_out: Dict[date, Dict[str, Any]] = {}
    for source_q, recs in deps.load_source_records_by_quarter_fn().items():
        if not isinstance(source_q, date):
            continue
        prior_q = date(int(source_q.year) - 1, int(source_q.month), int(source_q.day))
        for rec in recs:
            source_type = str(rec.get("source_type") or "")
            if source_type not in {"earnings_release", "presentation"}:
                continue
            parsed_pair = deps.parse_gpre_crush_margin_pair_fn(rec.get("text"))
            if not parsed_pair:
                continue
            current_val, prior_val, snippet = parsed_pair
            source_doc = str(rec.get("source_doc") or "")
            source_rank = int(rec.get("source_rank") or 99)
            for target_q, target_val in ((source_q, current_val), (prior_q, prior_val)):
                rank = _candidate_rank_local(target_q, source_q, source_type, source_rank)
                existing = series_out.get(target_q)
                existing_rank = existing.get("_rank") if isinstance(existing, dict) else None
                if existing_rank is not None and tuple(existing_rank) <= rank:
                    continue
                series_out[target_q] = {
                    "value": float(target_val),
                    "source_type": source_type,
                    "source_doc": source_doc,
                    "commentary": snippet,
                    "_rank": rank,
                }
    for rec in series_out.values():
        rec.pop("_rank", None)
    runtime.canonical_crush_series_cache = dict(series_out)
    return dict(series_out)


def extract_operating_driver_rows_for_template(
    runtime: OperatingDriversRuntime,
    deps: OperatingDriversDeps,
    qd: date,
    tpl: Any,
    quarter_records: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    template_spec = deps.template_spec_fn(tpl)
    key = str(template_spec.get("key") or "").strip().lower()
    group = str(template_spec.get("group") or "")
    label = str(template_spec.get("label") or "")
    search_terms = tuple(template_spec.get("search_terms") or ())
    cache_key = (qd, key)
    candidate_records = runtime.template_candidate_cache.get(cache_key)
    if candidate_records is None:
        if quarter_records is None:
            quarter_records = deps.load_source_records_by_quarter_fn().get(qd, [])
        candidate_records = deps.candidate_records_for_template_fn(
            qd,
            template_spec,
            quarter_records=quarter_records,
        )
        runtime.template_candidate_cache[cache_key] = candidate_records

    if key == "utilization":

        def _polish_utilization_commentary_local(text_in: Any) -> str:
            txt_local = glx_normalize_text(str(text_in or "")).strip()
            if deps.is_gpre_profile and re.search(r"\bspring maintenance season\b", txt_local, re.I):
                return "Plant utilization reflected the normal spring maintenance season, with plants temporarily shut down for annual clean-out and restart."
            return txt_local

        quarter_signals = deps.profile_slide_signals_for_quarter_fn(qd)
        if quarter_signals:
            signal_candidates = [
                rec
                for rec in quarter_signals
                if re.search(r"(?<!\d)\d{2,3}\s*%", str(rec.get("text") or ""))
                and re.search(r"\b(utilization|capacity|production at)\b", str(rec.get("text") or ""), re.I)
            ]
            if signal_candidates:
                best_signal = max(signal_candidates, key=lambda rec: float(rec.get("score") or 0.0))
                val = deps.parse_utilization_value_fn(best_signal.get("text"))
                if val is not None:
                    commentary_txt = _polish_utilization_commentary_local(best_signal.get("text"))
                    return [
                        make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(best_signal.get("source_type") or ""),
                            str(best_signal.get("source_doc") or ""),
                            driver_source_display_fn=deps.driver_source_display_fn,
                            driver_source_note_fn=deps.driver_source_note_fn,
                            commentary=commentary_txt,
                            quality="exact",
                            value=float(val),
                            unit="%",
                            source_note=deps.driver_source_note_fn(best_signal.get("source_doc"), commentary_txt),
                        )
                    ]
        best_row: Optional[Dict[str, Any]] = None
        best_score = -10_000.0
        for rec in candidate_records:
            text_blob = str(rec.get("text") or "")
            low = str(rec.get("_text_low") or text_blob.lower())
            if not re.search(r"\b(utilization|production at|operating rate|capacity utilization|stated capacity)\b", low, re.I):
                continue
            val = deps.cached_metric_parse_fn("utilization", text_blob, deps.parse_utilization_value_fn)
            if val is None:
                continue
            snippet = deps.driver_snippet_fn(text_blob, ("utilization", "capacity", "production at"))
            score = 60.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
            if "production at" in low:
                score += 5.0
            if "capacity utilization" in low or "utilization in the quarter" in low:
                score += 4.0
            if deps.qn_is_complete_signal_text_fn(snippet):
                score += 3.0
            snippet = _polish_utilization_commentary_local(snippet)
            if score > best_score:
                best_score = score
                best_row = make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=snippet,
                    quality="exact",
                    value=float(val),
                    unit="%",
                    source_note=deps.driver_source_note_fn(rec.get("source_doc"), snippet),
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
                raw_prod = deps.parse_driver_number_fn(prod_m.group(1))
                if raw_prod is not None:
                    prod_val = raw_prod / 1000.0
            if sold_m:
                raw_sold = deps.parse_driver_number_fn(sold_m.group(1))
                if raw_sold is not None:
                    sold_val = raw_sold / 1000.0
            if prod_val is None:
                prod_sent = prod_sentence_re.search(text_blob)
                if prod_sent:
                    prod_val = deps.parse_driver_number_fn(prod_sent.group(1))
            score = 50.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
            if prod_val is not None and score > best_prod_score:
                best_prod_score = score
                best_prod = make_driver_row(
                    qd,
                    "ethanol_gallons_produced",
                    group,
                    "Ethanol gallons produced",
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=(
                        "Produced gallons from selected operating data."
                        if "selected operating data" in low
                        else deps.driver_snippet_fn(text_blob, ("ethanol", "gallons", "production"))
                    ),
                    quality="exact",
                    value=float(prod_val),
                    unit="m gallons",
                    source_note=deps.driver_source_note_fn(
                        rec.get("source_doc"),
                        deps.driver_snippet_fn(text_blob, ("ethanol", "gallons", "production")),
                    ),
                )
            if sold_val is not None and score > best_sold_score:
                best_sold_score = score
                best_sold = make_driver_row(
                    qd,
                    "ethanol_gallons_sold",
                    group,
                    "Ethanol gallons sold",
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=(
                        "Sold gallons from agribusiness and energy services operating data."
                        if "agribusiness and energy services" in low
                        else deps.driver_snippet_fn(text_blob, ("agribusiness and energy services", "ethanol", "gallons"))
                    ),
                    quality="exact",
                    value=float(sold_val),
                    unit="m gallons",
                    source_note=deps.driver_source_note_fn(
                        rec.get("source_doc"),
                        deps.driver_snippet_fn(text_blob, ("agribusiness and energy services", "ethanol", "gallons")),
                    ),
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
                val = deps.cached_metric_parse_fn("distillers_grains", text_blob, deps.parse_distillers_grains_k_tons_fn)
                snippet_terms = ("distillers grains",)
            elif key == "ultra_high_protein":
                if "ultra-high protein" not in low and "uhp" not in low:
                    continue
                val = deps.cached_metric_parse_fn("ultra_high_protein", text_blob, deps.parse_uhp_k_tons_fn)
                snippet_terms = ("ultra-high protein", "uhp")
            elif key == "corn_consumed":
                if "bushels of corn" not in low and "corn processed" not in low and "corn consumed" not in low:
                    continue
                val = deps.cached_metric_parse_fn("corn_consumed", text_blob, deps.parse_corn_consumed_m_bushels_fn)
                snippet_terms = ("corn processed", "bushels of corn", "corn consumed")
            elif key == "rin_impact_accumulated_rin_sale":
                if "rin" not in low:
                    continue
                if (
                    re.search(r"\b(nine months ended|year[- ]ended|year ended|full year)\b", low, re.I)
                    and not re.search(r"\b(three months ended|quarter|quarterly highlights)\b", low, re.I)
                ):
                    continue
                val = deps.cached_metric_parse_fn(
                    "rin_impact_accumulated_rin_sale",
                    text_blob,
                    deps.parse_rin_impact_value_m_fn,
                )
                snippet_terms = ("accumulated rins", "rin sale", "rins")
                quality = "text-derived"
            if val is None:
                continue
            snippet = deps.driver_snippet_fn(text_blob, snippet_terms)
            score = 52.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
            if deps.qn_is_complete_signal_text_fn(snippet):
                score += 3.0
            if key == "rin_impact_accumulated_rin_sale" and re.search(r"\baccumulated rins?\b", low, re.I):
                score += 4.0
            if score > best_score:
                best_score = score
                best = make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=snippet,
                    quality=quality,
                    value=float(val),
                    unit=unit,
                    source_note=deps.driver_source_note_fn(rec.get("source_doc"), snippet),
                )
        return [best] if best is not None else []

    if key == "consolidated_ethanol_crush_margin":
        if deps.is_gpre_profile:
            same_quarter_best: Optional[Tuple[Tuple[int, int], Dict[str, Any], float, str]] = None
            for rec in quarter_records or []:
                source_type = str(rec.get("source_type") or "")
                if source_type not in {"earnings_release", "presentation"}:
                    continue
                parsed_pair = deps.parse_gpre_crush_margin_pair_fn(rec.get("text"))
                if not parsed_pair:
                    continue
                current_val, _prior_val, snippet = parsed_pair
                source_type_low = source_type.strip().lower()
                official_rank = 0 if source_type_low == "earnings_release" else 1 if source_type_low == "presentation" else 2
                rank = (official_rank, int(rec.get("source_rank") or 99))
                if same_quarter_best is None or rank < same_quarter_best[0]:
                    same_quarter_best = (rank, rec, float(current_val), str(snippet or ""))
            if same_quarter_best is not None:
                _, best_rec, best_val, best_snippet = same_quarter_best
                source_type = str(best_rec.get("source_type") or "earnings_release")
                source_doc = str(best_rec.get("source_doc") or "")
                return [
                    make_driver_row(
                        qd,
                        key,
                        group,
                        label,
                        source_type,
                        source_doc,
                        driver_source_display_fn=deps.driver_source_display_fn,
                        driver_source_note_fn=deps.driver_source_note_fn,
                        commentary=best_snippet,
                        quality="exact",
                        value=float(best_val),
                        unit="$m",
                        source_note=deps.driver_source_note_fn(source_doc, best_snippet),
                    )
                ]
            margin_rec = deps.driver_best_text_record_fn(qd, search_terms, require_numeric=True, quarter_records=candidate_records)
            if margin_rec is not None:
                val = deps.cached_metric_parse_fn(
                    "consolidated_ethanol_crush_margin",
                    margin_rec.get("text"),
                    deps.parse_crush_margin_value_m_fn,
                )
                if val is not None:
                    return [
                        make_driver_row(
                            qd,
                            key,
                            group,
                            label,
                            str(margin_rec.get("source_type") or ""),
                            str(margin_rec.get("source_doc") or ""),
                            driver_source_display_fn=deps.driver_source_display_fn,
                            driver_source_note_fn=deps.driver_source_note_fn,
                            commentary=str(margin_rec.get("snippet") or ""),
                            quality="exact",
                            value=float(val),
                            unit="$m",
                            source_note=deps.driver_source_note_fn(
                                margin_rec.get("source_doc"),
                                margin_rec.get("snippet"),
                            ),
                        )
                    ]
            canonical_series = gpre_canonical_crush_series_for_drivers(runtime, deps)
            canonical_rec = canonical_series.get(qd)
            canonical_val = pd.to_numeric((canonical_rec or {}).get("value"), errors="coerce")
            if pd.notna(canonical_val):
                source_type = str((canonical_rec or {}).get("source_type") or "earnings_release")
                source_doc = str((canonical_rec or {}).get("source_doc") or "")
                commentary = str((canonical_rec or {}).get("commentary") or "")
                return [
                    make_driver_row(
                        qd,
                        key,
                        group,
                        label,
                        source_type,
                        source_doc,
                        driver_source_display_fn=deps.driver_source_display_fn,
                        driver_source_note_fn=deps.driver_source_note_fn,
                        commentary=commentary,
                        quality="exact",
                        value=float(canonical_val),
                        unit="$m",
                        source_note=deps.driver_source_note_fn(source_doc, commentary),
                    )
                ]
        margin_rec = deps.driver_best_text_record_fn(qd, search_terms, require_numeric=True, quarter_records=candidate_records)
        if margin_rec is not None:
            val = deps.cached_metric_parse_fn(
                "consolidated_ethanol_crush_margin",
                margin_rec.get("text"),
                deps.parse_crush_margin_value_m_fn,
            )
            if val is not None:
                return [
                    make_driver_row(
                        qd,
                        key,
                        group,
                        label,
                        str(margin_rec.get("source_type") or ""),
                        str(margin_rec.get("source_doc") or ""),
                        driver_source_display_fn=deps.driver_source_display_fn,
                        driver_source_note_fn=deps.driver_source_note_fn,
                        commentary=str(margin_rec.get("snippet") or ""),
                        quality="exact",
                        value=float(val),
                        unit="$m",
                        source_note=deps.driver_source_note_fn(
                            margin_rec.get("source_doc"),
                            margin_rec.get("snippet"),
                        ),
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
            val = deps.cached_metric_parse_fn("45z_value_realized", text_blob, deps.parse_45z_realized_value_m_fn)
            if val is None:
                continue
            snippet = deps.driver_snippet_fn(text_blob, ("45z", "production tax", "income tax benefit"))
            score = 55.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
            if deps.qn_is_complete_signal_text_fn(snippet):
                score += 3.0
            if score > best_score:
                best_score = score
                best = make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=snippet,
                    quality="exact",
                    value=float(val),
                    unit="$m",
                    source_note=deps.driver_source_note_fn(rec.get("source_doc"), snippet),
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
            if re.fullmatch(r"(?:>=|>) \$[0-9.,]+m(?: expected)? in 20\d{2}", txt_local, re.I):
                return True
            return False

        for rec in deps.profile_slide_signals_for_quarter_fn(qd):
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
            strong_targets = deps.extract_45z_target_candidates_fn(text_blob, qd)
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
                        "fragment_penalty": int(
                            deps.text_fragment_penalty_fn(strong.get("window") or text_blob) or 0
                        ),
                    }
                )
            target_txt = deps.extract_45z_target_display_fn(text_blob, qd)
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
        for doc_rec in deps.load_45z_guidance_docs_by_quarter_fn().get(qd, []):
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
                        "fragment_penalty": int(
                            deps.text_fragment_penalty_fn(str(strong.get("window") or doc_rec.get("text") or ""))
                            or 0
                        ),
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
                        "fragment_penalty": int(deps.text_fragment_penalty_fn(str(doc_rec.get("text") or "")) or 0),
                    }
                )
        if candidate_rows:

            def _guidance_key(rec: Dict[str, Any]) -> Tuple[int, float, int, int]:
                scope_kind = str(rec.get("scope_kind") or "")
                scope_pri = (
                    0
                    if scope_kind == "total"
                    else 1
                    if scope_kind == "component_named"
                    else 2
                    if scope_kind == "component_remaining"
                    else 3
                )
                txt_local = str(rec.get("text") or "")
                amount_vals = deps.extract_money_targets_for_display_fn(txt_local)
                amount_pri = float(max(amount_vals)) if amount_vals else 0.0
                return (
                    -scope_pri,
                    amount_pri,
                    float(rec.get("score") or 0.0),
                    -int(rec.get("fragment_penalty") or 0),
                )

            best = max(candidate_rows, key=_guidance_key)
            target_txt = str(best.get("text") or "").strip()
            value_m = None
            if target_txt and not re.search(r"\$\s*[0-9].{0,10}\-\s*\$?\s*[0-9]", target_txt):
                value_m = deps.parse_threshold_amount_m_fn(target_txt)
            return [
                make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(best.get("source_type") or ""),
                    str(best.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=target_txt,
                    quality="text-derived",
                    value=value_m,
                    unit="$m" if value_m is not None else "",
                    source_note=deps.driver_source_note_fn(best.get("source_doc"), target_txt),
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
            val = deps.cached_metric_parse_fn("renewable_corn_oil", text_blob, deps.parse_renewable_corn_oil_m_lbs_fn)
            if val is None or float(val) > 200.0:
                continue
            snippet = deps.driver_snippet_fn(text_blob, ("renewable corn oil", "corn oil"))
            score = 50.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 3.0
            if score > best_score:
                best_score = score
                best = make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=snippet,
                    quality="exact",
                    value=float(val),
                    unit="m lbs",
                    source_note=deps.driver_source_note_fn(rec.get("source_doc"), snippet),
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
                dist_val = deps.parse_driver_number_fn(dist_m.group(1))
                if dist_val is not None:
                    comment_parts.append(f"Distillers grains {dist_val:.0f}k tons")
            if uhp_m:
                uhp_val = deps.parse_driver_number_fn(uhp_m.group(1))
                if uhp_val is not None:
                    comment_parts.append(f"Ultra-high protein {uhp_val:.0f}k tons")
            if not comment_parts:
                continue
            snippet = "; ".join(comment_parts)
            score = 48.0 - float(rec.get("source_rank") or 0) * 5.0 - float(rec.get("_fragment_penalty") or 0) * 2.0
            if score > best_score:
                best_score = score
                best = make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(rec.get("source_type") or ""),
                    str(rec.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=snippet,
                    quality="exact",
                    source_note=deps.driver_source_note_fn(rec.get("source_doc"), snippet),
                )
        return [best] if best is not None else []

    if key in {"risk_management_support", "margin_cashflow_support"}:
        best = deps.driver_best_text_record_fn(qd, search_terms, quarter_records=candidate_records)
        if best is not None:
            return [
                make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(best.get("source_type") or ""),
                    str(best.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=str(best.get("snippet") or ""),
                    quality="text-derived",
                    source_note=deps.driver_source_note_fn(best.get("source_doc"), best.get("snippet")),
                )
            ]

    if key in {
        "45z_agreement_status",
        "carbon_capture_status",
        "plant_status",
        "input_cost_commentary",
        "distillers_grains_uhp_commentary",
    }:
        best = deps.driver_best_text_record_fn(qd, search_terms, quarter_records=candidate_records)
        if best is not None:
            return [
                make_driver_row(
                    qd,
                    key,
                    group,
                    label,
                    str(best.get("source_type") or ""),
                    str(best.get("source_doc") or ""),
                    driver_source_display_fn=deps.driver_source_display_fn,
                    driver_source_note_fn=deps.driver_source_note_fn,
                    commentary=str(best.get("snippet") or ""),
                    quality="text-derived",
                    source_note=deps.driver_source_note_fn(best.get("source_doc"), best.get("snippet")),
                )
            ]

    best_generic = deps.driver_best_text_record_fn(
        qd,
        search_terms,
        require_numeric=False,
        quarter_records=candidate_records,
    )
    if best_generic is not None:
        return [
            make_driver_row(
                qd,
                key or re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_"),
                group,
                label,
                str(best_generic.get("source_type") or ""),
                str(best_generic.get("source_doc") or ""),
                driver_source_display_fn=deps.driver_source_display_fn,
                driver_source_note_fn=deps.driver_source_note_fn,
                commentary=str(best_generic.get("snippet") or ""),
                quality="text-derived",
                source_note=deps.driver_source_note_fn(best_generic.get("source_doc"), best_generic.get("snippet")),
            )
        ]
    return []


def format_operating_driver_delta(current_val: Any, prior_val: Any, unit: str) -> str:
    cur = pd.to_numeric(current_val, errors="coerce")
    prev = pd.to_numeric(prior_val, errors="coerce")
    if pd.isna(cur) or pd.isna(prev):
        return ""
    cur_f = float(cur)
    prev_f = float(prev)
    if unit == "%":
        return f"{cur_f - prev_f:+.1f} pts"
    if unit == "basis points":
        return f"{cur_f - prev_f:+.0f} bps"
    if abs(prev_f) > 1e-9:
        return f"{((cur_f - prev_f) / abs(prev_f)) * 100:+.1f}%"
    return ""


def build_operating_drivers_history_rows(
    runtime: OperatingDriversRuntime,
    deps: OperatingDriversDeps,
) -> List[Dict[str, Any]]:
    template_index = deps.load_template_index_fn()
    templates = list(template_index.get("templates") or [])
    if not templates:
        return []
    template_by_key: Dict[str, Any] = dict(template_index.get("template_by_key") or {})
    operating_quarters = deps.operating_quarters_fn()
    deps.load_line_index_by_quarter_fn()
    source_records_by_quarter = deps.load_source_records_by_quarter_fn()
    bridge_bundle_map = deps.load_bridge_bundle_map_fn(operating_quarters)

    def _driver_template_meta(
        driver_key: str,
        default_group: str,
        default_label: str,
        default_unit: str,
    ) -> Tuple[str, str, str]:
        tpl = template_by_key.get(str(driver_key or "").strip().lower())
        if tpl is None:
            return default_group, default_label, default_unit
        return (
            str(getattr(tpl, "group", "") or default_group),
            str(getattr(tpl, "label", "") or default_label),
            str(getattr(tpl, "preferred_unit", "") or default_unit),
        )

    row_map: Dict[Tuple[date, str, str], Dict[str, Any]] = {}
    with deps.timed_substage_fn("write_excel.derive.driver_inputs.template_rows"):
        for qd in operating_quarters:
            quarter_records = source_records_by_quarter.get(qd, [])
            for tpl in templates:
                template_key = str(getattr(tpl, "key", "") or getattr(tpl, "label", "") or "").strip().lower()
                cache_key = (qd, template_key)
                cached_rows = runtime.template_rows_cache.get(cache_key)
                if cached_rows is None:
                    cached_rows = [
                        dict(row)
                        for row in extract_operating_driver_rows_for_template(
                            runtime,
                            deps,
                            qd,
                            tpl,
                            quarter_records=quarter_records,
                        )
                    ]
                    runtime.template_rows_cache[cache_key] = cached_rows
                for row in cached_rows:
                    row_key = (
                        row.get("Quarter"),
                        str(row.get("_driver_key") or ""),
                        str(row.get("_driver_scope") or ""),
                    )
                    prev = row_map.get(row_key)
                    row_map[row_key] = (
                        merge_driver_rows(prev, row, source_rank_fn=deps.source_rank_fn)
                        if prev is not None
                        else dict(row)
                    )
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
            new_row = make_driver_row(
                qd,
                driver_key,
                group,
                label,
                source_type,
                source_doc,
                driver_source_display_fn=deps.driver_source_display_fn,
                driver_source_note_fn=deps.driver_source_note_fn,
                commentary=commentary,
                quality=quality,
                value=float(value),
                unit=unit,
                source_note=deps.driver_source_note_fn(source_doc, commentary or source_text),
            )
            row_key = (qd, driver_key, "")
            prev = row_map.get(row_key)
            row_map[row_key] = (
                merge_driver_rows(prev, new_row, source_rank_fn=deps.source_rank_fn)
                if prev is not None
                else new_row
            )

        consolidated_val = pd.to_numeric(
            row_map.get((qd, "consolidated_ethanol_crush_margin", ""), {}).get("Value"),
            errors="coerce",
        )
        consolidated = float(consolidated_val) if pd.notna(consolidated_val) else bundle_components.get("consolidated")
        ex_45z_val = None
        ex_45z_quality = "modeled"
        if "ex_45z" in bundle_components:
            ex_45z_val = float(bundle_components["ex_45z"])
            ex_45z_quality = "exact"
        elif consolidated is not None and "45z" in bundle_components and same_basis_bridge:
            ex_45z_val = float(consolidated) - float(bundle_components["45z"])
        if ex_45z_val is not None:
            note_txt = (
                "Direct ex-45Z crush margin disclosure."
                if ex_45z_quality == "exact"
                else "Derived as consolidated crush margin less explicit same-quarter 45Z bridge component."
            )
            _add_derived_driver_row("crush_margin_ex_45z", ex_45z_val, ex_45z_quality, note_txt)

        ex_rin_val = None
        ex_rin_quality = "modeled"
        if "ex_rin" in bundle_components:
            ex_rin_val = float(bundle_components["ex_rin"])
            ex_rin_quality = "exact"
        elif consolidated is not None and "rin_sale" in bundle_components and same_basis_bridge:
            ex_rin_val = float(consolidated) - float(bundle_components["rin_sale"])
        if ex_rin_val is not None:
            note_txt = (
                "Direct ex-RIN crush margin disclosure."
                if ex_rin_quality == "exact"
                else "Derived as consolidated crush margin less explicit same-quarter accumulated RIN-sale benefit."
            )
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
                        bridge_adjustments.append(
                            ("impairment_assets_held_for_sale", -float(bundle_components["impairment_assets_held_for_sale"]))
                        )
                    if "inventory_lcnrv" in bundle_components:
                        bridge_adjustments.append(("inventory_lcnrv", float(bundle_components["inventory_lcnrv"])))
                    if "intercompany_nonethanol_net" in bundle_components:
                        bridge_adjustments.append(
                            ("intercompany_nonethanol_net", -float(bundle_components["intercompany_nonethanol_net"]))
                        )
                    elif "nonrecurring_decommissioning" in bundle_components:
                        bridge_adjustments.append(
                            ("nonrecurring_decommissioning", -float(bundle_components["nonrecurring_decommissioning"]))
                        )
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
    for _, quarter_map in driver_quarter_map.items():
        q_list = sorted(quarter_map)
        for idx, qd in enumerate(q_list):
            row = quarter_map[qd]
            unit = str(row.get("Unit") or "")
            if idx > 0:
                row["QoQ change"] = format_operating_driver_delta(
                    row.get("Value"),
                    quarter_map[q_list[idx - 1]].get("Value"),
                    unit,
                )
            prev_year = date(qd.year - 1, qd.month, qd.day)
            if prev_year in quarter_map:
                row["YoY change"] = format_operating_driver_delta(
                    row.get("Value"),
                    quarter_map[prev_year].get("Value"),
                    unit,
                )
    order_map = dict(template_index.get("order_map") or {})
    rows.sort(
        key=lambda row: (
            -(int(row["Quarter"].strftime("%Y%m%d")) if isinstance(row.get("Quarter"), date) else 0),
            order_map.get(str(row.get("_driver_key") or ""), 999),
            str(row.get("Driver") or ""),
        )
    )
    return rows
