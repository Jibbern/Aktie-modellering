"""Small explicit compatibility surface for legacy pipeline helpers.

This module intentionally re-exports only the legacy symbols still used by
newer orchestration/writer modules, so those modules do not depend on broad
namespace copying from ``pipeline.py``.
"""
from __future__ import annotations

from .pipeline import (
    PIPELINE_STAGE_CACHE_VERSION,
    _coerce_prev_quarter_end,
    _coerce_next_quarter_end,
    _extract_balance_sheet_from_html,
    _extract_balance_sheet_from_text,
    _is_quarter_end,
    _path_belongs_to_ticker,
    _prev_quarter_end_from_qend,
    _source_class,
    _source_label,
    _source_method,
    _source_qa,
    _source_tier,
    build_bridge_q,
    build_company_overview,
    build_debt_buckets,
    build_debt_credit_notes,
    build_debt_profile,
    build_debt_qa_checks,
    build_gaap_history,
    build_interest_qa_checks,
    build_local_main_revolver_history,
    build_qa_checks,
    build_revolver_availability,
    build_revolver_capacity_map,
    build_revolver_history,
    compute_long_term_debt_instant,
    compute_total_debt_instant,
)

__all__ = [
    "PIPELINE_STAGE_CACHE_VERSION",
    "_coerce_next_quarter_end",
    "_coerce_prev_quarter_end",
    "_extract_balance_sheet_from_html",
    "_extract_balance_sheet_from_text",
    "_is_quarter_end",
    "_path_belongs_to_ticker",
    "_prev_quarter_end_from_qend",
    "_source_class",
    "_source_label",
    "_source_method",
    "_source_qa",
    "_source_tier",
    "build_bridge_q",
    "build_company_overview",
    "build_debt_buckets",
    "build_debt_credit_notes",
    "build_debt_profile",
    "build_debt_qa_checks",
    "build_gaap_history",
    "build_interest_qa_checks",
    "build_local_main_revolver_history",
    "build_qa_checks",
    "build_revolver_availability",
    "build_revolver_capacity_map",
    "build_revolver_history",
    "compute_long_term_debt_instant",
    "compute_total_debt_instant",
]
