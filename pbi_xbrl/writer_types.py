from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .pipeline_types import WorkbookInputs
from .writer_runtime_cache import WriterRuntimeCache


@dataclass
class WriterDerivedData:
    """Run-scoped derived frames and lookup maps shared across writer sections."""

    leverage_df: Optional[pd.DataFrame] = None
    valuation_summary_df: Optional[pd.DataFrame] = None
    valuation_grid_df: Optional[pd.DataFrame] = None
    summary_df: Optional[pd.DataFrame] = None
    report_is: Optional[pd.DataFrame] = None
    report_bs: Optional[pd.DataFrame] = None
    report_cf: Optional[pd.DataFrame] = None
    signals_base_df: Optional[pd.DataFrame] = None
    flags_df: Optional[pd.DataFrame] = None
    flags_audit_df: Optional[pd.DataFrame] = None
    flags_recompute_df: Optional[pd.DataFrame] = None
    ng_bridge: Optional[pd.DataFrame] = None
    ng_bridge_relaxed: Optional[pd.DataFrame] = None
    facts_long: Optional[pd.DataFrame] = None
    lineitem_map: Optional[pd.DataFrame] = None
    period_index: Optional[pd.DataFrame] = None
    quarter_notes_evidence_df: Optional[pd.DataFrame] = None
    promise_evidence_df: Optional[pd.DataFrame] = None
    promise_progress_ui_bundle: Optional[Dict[str, Any]] = None
    valuation_hist_view: Optional[pd.DataFrame] = None
    valuation_adj_metrics_view: Optional[pd.DataFrame] = None
    valuation_hist_indexed: Optional[pd.DataFrame] = None
    valuation_latest_context: Optional[Dict[str, Any]] = None
    valuation_last4_context: Optional[Dict[str, Any]] = None
    valuation_core_maps: Optional[Dict[str, Any]] = None
    valuation_revolver_maps: Optional[Dict[str, Dict[pd.Timestamp, Any]]] = None
    valuation_adj_ebit_q: Optional[Dict[pd.Timestamp, Any]] = None
    valuation_adj_ebit_ttm_q: Optional[Dict[pd.Timestamp, Any]] = None
    valuation_adj_ebitda_q: Optional[Dict[pd.Timestamp, Any]] = None
    valuation_adj_ebitda_ttm_q: Optional[Dict[pd.Timestamp, Any]] = None
    valuation_net_leverage_text_map: Optional[Dict[pd.Timestamp, float]] = None
    valuation_style_bundle: Optional[Dict[str, Any]] = None
    valuation_render_bundle: Optional[Dict[str, Any]] = None
    valuation_precompute_bundle: Optional[Dict[str, Any]] = None
    valuation_export_expectation: Optional[Dict[str, Any]] = None
    summary_export_expectation: Optional[Dict[str, Any]] = None
    valuation_filing_docs_by_quarter: Optional[Dict[pd.Timestamp, List[Dict[str, Any]]]] = None
    operating_driver_template_index: Optional[Dict[str, Any]] = None
    operating_driver_bridge_bundle_map: Optional[Dict[date, Dict[str, Any]]] = None
    operating_driver_line_index_by_quarter: Optional[Dict[date, List[Dict[str, Any]]]] = None
    operating_driver_flat_line_index: Optional[List[Dict[str, Any]]] = None
    operating_driver_best_text_cache: Optional[Dict[Tuple[date, Tuple[str, ...], bool], Optional[Dict[str, Any]]]] = None
    operating_driver_template_rows_cache: Optional[Dict[Tuple[date, str], List[Dict[str, Any]]]] = None
    operating_driver_template_candidate_cache: Optional[Dict[Tuple[date, str], List[Dict[str, Any]]]] = None
    profile_slide_signals_by_quarter: Optional[Dict[date, List[Dict[str, Any]]]] = None
    operating_driver_45z_guidance_docs_by_quarter: Optional[Dict[date, List[Dict[str, Any]]]] = None
    valuation_local_material_index: Optional[List[Dict[str, Any]]] = None
    valuation_audit_doc_index: Optional[List[Dict[str, Any]]] = None


@dataclass
class WriterDocumentCache:
    """Per-export cache for repeated SEC/local document reads and path lookups."""

    accession_doc_paths: Dict[str, List[Path]] = field(default_factory=dict)
    raw_text_by_path: Dict[str, str] = field(default_factory=dict)
    plain_text_by_path: Dict[str, str] = field(default_factory=dict)
    normalized_text_by_path: Dict[str, str] = field(default_factory=dict)
    inferred_quarter_by_path: Dict[str, Optional[date]] = field(default_factory=dict)
    slide_paths_by_kind: Dict[str, List[Path]] = field(default_factory=dict)
    slide_paths_by_kind_and_quarter: Dict[Tuple[str, str], List[Path]] = field(default_factory=dict)
    submission_files: Optional[List[Path]] = None
    submission_recent_rows_by_file: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    submission_recent_rows_by_limit: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    latest_quarter_sec_text_by_quarter: Dict[str, str] = field(default_factory=dict)
    latest_quarter_qa_bundle_by_quarter: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    sec_cache_doc_paths_by_root: Dict[str, List[Path]] = field(default_factory=dict)
    sec_cache_doc_paths_by_token_by_root: Dict[str, Dict[str, List[Path]]] = field(default_factory=dict)
    sec_cache_html_paths_by_root: Dict[str, List[Path]] = field(default_factory=dict)
    sec_cache_html_paths_by_token_by_root: Dict[str, Dict[str, List[Path]]] = field(default_factory=dict)


@dataclass
class WriterRuntimeData:
    out_path: Path
    ticker: Optional[str]
    excel_mode: str
    profile_timings: bool
    quarter_notes_audit: bool = False
    enable_operating_drivers_sheet: bool = False
    enable_economics_overlay_sheet: bool = False
    enable_economics_market_raw_sheet: bool = False
    driver_inputs_ready: bool = False
    operating_driver_history_rows: List[Dict[str, Any]] = field(default_factory=list)
    economics_market_rows: List[Dict[str, Any]] = field(default_factory=list)
    qa_checks: pd.DataFrame = field(default_factory=pd.DataFrame)
    info_log: pd.DataFrame = field(default_factory=pd.DataFrame)
    data_is_rules_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    doc_cache: WriterDocumentCache = field(default_factory=WriterDocumentCache)
    frame_view_cache: Dict[Tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    runtime_cache: WriterRuntimeCache = field(default_factory=WriterRuntimeCache)
    extra_values: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WriterCallbacks:
    write_sheet: Callable[..., Any]
    write_flags_sheet: Callable[..., Any]
    write_report_sheet: Callable[..., Any]
    write_summary_sheet: Callable[..., Any]
    write_valuation_sheet: Callable[..., Any]
    write_bs_segments_sheet: Callable[..., Any]
    write_quarter_notes_ui_v2: Callable[..., Any]
    write_promise_tracker_ui_v2: Callable[..., Any]
    write_promise_progress_ui_v2: Callable[..., Any]
    write_operating_drivers_sheet: Callable[..., Any]
    write_economics_overlay_sheet: Callable[..., Any]
    write_operating_drivers_raw_sheet: Callable[..., Any]
    write_economics_market_raw_sheet: Callable[..., Any]
    build_report: Callable[..., Any]
    build_summary: Callable[..., Any]
    build_facts_long: Callable[..., Any]
    build_lineitem_map: Callable[..., Any]
    build_period_index: Callable[..., Any]
    build_ng_bridge: Callable[..., Any]
    build_qn_evidence_src: Callable[..., Any]
    build_promise_evidence_src: Callable[..., Any]
    extract_adj_net_leverage_text_map: Callable[..., Any]
    build_hidden_value_flags_fallback: Callable[..., Any]
    load_operating_driver_source_records: Callable[..., Any]
    load_operating_driver_source_records_by_quarter: Callable[..., Any]
    prime_operating_driver_crush_detail_cache: Callable[..., Any]
    build_operating_drivers_history_rows: Callable[..., Any]
    build_economics_market_rows: Callable[..., Any]
    run_latest_quarter_qa: Callable[..., Any]
    extra_callbacks: Dict[str, Any] = field(default_factory=dict)

    def as_state_mapping(self) -> Dict[str, Any]:
        mapping = {
            "_write_sheet": self.write_sheet,
            "_write_flags_sheet": self.write_flags_sheet,
            "_write_report_sheet": self.write_report_sheet,
            "_write_summary_sheet": self.write_summary_sheet,
            "_write_valuation_sheet": self.write_valuation_sheet,
            "_write_bs_segments_sheet": self.write_bs_segments_sheet,
            "_write_quarter_notes_ui_v2": self.write_quarter_notes_ui_v2,
            "_write_promise_tracker_ui_v2": self.write_promise_tracker_ui_v2,
            "_write_promise_progress_ui_v2": self.write_promise_progress_ui_v2,
            "_write_operating_drivers_sheet": self.write_operating_drivers_sheet,
            "_write_economics_overlay_sheet": self.write_economics_overlay_sheet,
            "_write_operating_drivers_raw_sheet": self.write_operating_drivers_raw_sheet,
            "_write_economics_market_raw_sheet": self.write_economics_market_raw_sheet,
            "_build_report": self.build_report,
            "_build_summary": self.build_summary,
            "_build_facts_long": self.build_facts_long,
            "_build_lineitem_map": self.build_lineitem_map,
            "_build_period_index": self.build_period_index,
            "_build_ng_bridge": self.build_ng_bridge,
            "_build_qn_evidence_src": self.build_qn_evidence_src,
            "_build_promise_evidence_src": self.build_promise_evidence_src,
            "_extract_adj_net_leverage_text_map": self.extract_adj_net_leverage_text_map,
            "_build_hidden_value_flags_fallback": self.build_hidden_value_flags_fallback,
            "_load_operating_driver_source_records": self.load_operating_driver_source_records,
            "_load_operating_driver_source_records_by_quarter": self.load_operating_driver_source_records_by_quarter,
            "_prime_operating_driver_crush_detail_cache": self.prime_operating_driver_crush_detail_cache,
            "_build_operating_drivers_history_rows": self.build_operating_drivers_history_rows,
            "_build_economics_market_rows": self.build_economics_market_rows,
            "_run_latest_quarter_qa": self.run_latest_quarter_qa,
        }
        mapping.update(self.extra_callbacks)
        return mapping


@dataclass
class WriterContext:
    inputs: WorkbookInputs
    wb: Any
    font_size: int
    header_size: int
    company_profile: Any
    data: WriterRuntimeData
    callbacks: WriterCallbacks
    derived: WriterDerivedData = field(default_factory=WriterDerivedData)
    writer_timings: Dict[str, float] = field(default_factory=dict)
    ui_info_rows: List[Dict[str, Any]] = field(default_factory=list)
    desired_sheet_order: Tuple[str, ...] = field(default_factory=tuple)
    raw_sheet_cluster: Tuple[str, ...] = field(default_factory=tuple)
    state: Dict[str, Any] = field(default_factory=dict)

    def require_derived_frame(self, attr: str) -> pd.DataFrame:
        value = getattr(self.derived, attr, None)
        if isinstance(value, pd.DataFrame):
            return value
        raise RuntimeError(f"Derived frame '{attr}' was not prepared before use")
