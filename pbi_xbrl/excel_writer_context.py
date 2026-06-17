"""Main workbook rendering context and visible-sheet write logic.

This module is the largest concentration of product behavior in the codebase. It turns
pipeline artifacts, local materials, SEC cache content, and market-data exports into
the actual saved workbook surfaces delivered to the user.

When debugging workbook output, this is usually the last stop before a value becomes
visible in the saved file.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import html
import io
import json
import math
import os
import re
import time
from contextlib import contextmanager
from copy import copy
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Pattern, Sequence, Set, Tuple

import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.chart import LineChart, Reference, ScatterChart, Series
from openpyxl.chart.axis import ChartLines, TextAxis
from openpyxl.chart.data_source import AxDataSource, StrRef
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.shapes import GraphicalProperties, LineProperties
from openpyxl.comments import Comment
from openpyxl.drawing.spreadsheet_drawing import AnchorMarker, TwoCellAnchor
from openpyxl.formatting.rule import CellIsRule, FormulaRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import range_boundaries
from openpyxl.utils.datetime import to_excel
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table, TableStyleInfo

from .conference_metadata import metadata_audit_flags, metadata_source_file, parse_metadata_key_values, source_material_role

from .excel_writer_bs_segments import (
    BsSegmentsWriterDeps,
    _series_has_nonblank_values,
    _should_render_carbon_equipment_liabilities,
    write_bs_segments_sheet,
)

from .excel_writer_coloring import (
    QuarterlyRowColorPolicy,
    _QUARTERLY_COLOR_BRACKET_SUFFIX_RE,
    _QUARTERLY_COLOR_COMPARISON_RE,
    _QUARTERLY_COLOR_REPEAT_SPACE_RE,
    _apply_quarterly_comparison_fills,
    _hidden_source_comparison_metric,
    _normalize_quarterly_color_label,
    _quarterly_bucket_fill,
    _quarterly_color_basis_for_label,
    _quarterly_color_directionality_for_label,
    _quarterly_color_label_key,
    _quarterly_color_metric_from_series,
    _quarterly_row_color_policy,
)
from .excel_writer_layout import (
    estimate_wrapped_line_count as _estimate_wrapped_line_count,
    estimate_wrapped_row_height as _estimate_wrapped_row_height,
)
from .excel_writer_operating_drivers import (
    OperatingDriversWriterDeps,
    write_operating_drivers_sheet,
)
from .excel_writer_economics_overlay_charts import (
    EconomicsOverlayChartWriterDeps,
    _build_visible_quarter_label_points,
    _quarter_bounds_from_end_date,
    write_economics_overlay_charts,
)
from .excel_writer_economics_overlay_commercial import (
    GpreEconomicsOverlayCommercialDeps,
    write_gpre_economics_overlay_commercial_sections,
)
from .excel_writer_economics_overlay_bridge import (
    GpreEconomicsOverlayBridgeDeps,
    write_gpre_economics_overlay_bridge_to_reported_section,
)
from .excel_writer_economics_overlay_current_qtd import (
    GpreEconomicsOverlayCurrentQtdDeps,
    write_gpre_economics_overlay_current_qtd_section,
)
from .excel_writer_economics_overlay_inputs import (
    GpreEconomicsOverlayInputRowsDeps,
    write_gpre_economics_overlay_input_rows,
)
from .excel_writer_economics_overlay_coproduct import (
    GpreEconomicsOverlayCoproductDeps,
    write_gpre_economics_overlay_coproduct_section,
)
from .excel_writer_economics_overlay_sources import (
    EconomicsOverlaySourceSupport,
    EconomicsOverlaySourceSupportDeps,
)
from .excel_writer_economics_overlay_market_state import (
    EconomicsOverlayMarketStateDeps,
    build_economics_overlay_market_state,
)
from .excel_writer_economics_overlay_orchestrator import (
    EconomicsOverlayOrchestratorDeps,
    write_economics_overlay_sheet,
)
from .excel_writer_economics_raw import (
    ECONOMICS_MARKET_RAW_COLUMN_WIDTHS,
    ECONOMICS_MARKET_RAW_HEADERS,
    ECONOMICS_MARKET_RAW_LARGE_FAST_PATH_THRESHOLD,
    EconomicsMarketRawWriterDeps,
    write_economics_market_raw_sheet,
)
from .excel_writer_market_data_sources import (
    EconomicsMarketRowsDeps,
    _convert_market_price_value,
    _economics_market_region_tags,
    _economics_market_series_meta,
    build_economics_market_rows,
)
from .excel_writer_styles import (
    get_analysis_sheet_style_bundle as _style_get_analysis_sheet_style_bundle,
    valuation_side_panel_style_bundle as _style_valuation_side_panel_style_bundle,
)
from .excel_writer_valuation import (
    build_valuation_history_source_maps,
    display_m_source_map,
    history_margin_source_map,
    history_numeric_source_map,
    normalize_capex_for_valuation,
    quarter_key_union,
    ttm_map,
    ttm_sparse_cashflow_map,
    valuation_hidden_comparison_metric,
)
from .excel_writer_valuation_precompute import (
    ValuationPrecomputeDeps,
    ValuationPrecomputeSupport,
)
from .excel_writer_valuation_orchestrator import (
    ValuationOrchestratorDeps,
    write_valuation_sheet,
)
from .excel_writer_summary_builder import (
    SummaryBuilderDeps,
    build_summary_dataframe,
)
from .excel_writer_latest_quarter_qa import (
    LatestQuarterQADeps,
    LatestQuarterQASupport,
    run_latest_quarter_qa,
)
from .excel_writer_gpre_commercial_setup import (
    GpreCommercialSetupDeps,
    GpreCommercialSetupSupport,
)
from .excel_writer_operating_drivers_support import (
    OperatingDriversSupport,
    OperatingDriversSupportDeps,
)
from .excel_writer_profile_signal_support import (
    ProfileSignalSupport,
    ProfileSignalSupportDeps,
)
from .excel_writer_promise_source_override_mutator import (
    PromiseSourceOverrideMutatorDeps,
    apply_source_backed_promise_mapping_overrides,
)
from .excel_writer_promise_progress_rewrite import (
    PromiseProgressRewriteDeps,
    rewrite_shared_promise_progress_ui_from_blocks,
)
from .excel_writer_promise_progress_worksheet_repairs import (
    PROMISE_TIMELINE_HEADERS,
    PROMISE_VISIBLE_MAX_COL,
    PromiseProgressWorksheetRepairDeps,
    final_repair_promise_progress_ui,
    insert_management_credibility_scorecard,
    polish_promise_scorecard_layout,
    repair_promise_table_header_merges,
)
from . import excel_writer_promise_progress_worksheet_repairs as _promise_progress_worksheet_repairs
from .excel_writer_shared_ui_conventions import (
    SharedUiConventionsDeps,
    apply_shared_ui_conventions_to_workbook,
)
from .excel_writer_anf_valuation_side_panel import (
    AnfValuationSidePanelDeps,
    clear_anf_valuation_side_panels,
    valuation_side_panel_style_bundle,
    write_anf_valuation_side_panel,
)
from .excel_writer_anf_visible_support import (
    AnfVisibleSupport,
    AnfVisibleSupportDeps,
)
from .excel_writer_financial_report_support import (
    FinancialReportSupport,
    FinancialReportSupportDeps,
)
from .excel_writer_investment_case_support import (
    InvestmentCaseSupport,
    InvestmentCaseSupportDeps,
)
from .excel_writer_sector_investment_case import (
    SectorInvestmentCaseRenderDeps,
    write_sector_investment_case_data_sheet,
    write_sector_investment_case_sheet,
)
from .excel_writer_anf_investment_case import (
    AnfInvestmentCaseRenderDeps,
    write_anf_investment_case_data_sheet,
    write_anf_investment_case_sheet,
)
from .excel_writer_investment_case_scenarios import (
    InvestmentCaseScenarioRenderDeps,
    SCENARIO_DRIVER_CAPITAL_STRUCTURE_INTEREST,
    SCENARIO_DRIVER_CASH_FLOW_CAPEX,
    SCENARIO_DRIVER_MANUAL_INCREMENTAL,
    SCENARIO_DRIVER_MARGIN_EBITDA,
    SCENARIO_DRIVER_REVENUE_VOLUME,
    SCENARIO_DRIVER_SHARE_COUNT_BUYBACK,
    SCENARIO_DRIVER_TAX_CREDIT_SUBSIDY,
    SCENARIO_TAX_CASH_ONLY,
    SCENARIO_TAX_DIRECT_EPS,
    SCENARIO_TAX_NON_TAXABLE,
    SCENARIO_TAX_NON_TAXABLE_CREDIT,
    SCENARIO_TAX_NO_EPS_IMPACT,
    SCENARIO_TAX_TAXABLE,
    SCENARIO_TAX_UNKNOWN_MANUAL_REQUIRED,
    _ScenarioDriverBridgeSpec,
    _SegmentScenarioInputSpec,
    _excel_manual_percent_active_formula,
    _excel_percent_value_expr,
    _excel_visible_value_range_formula,
    _scenario_bridge_active_value_formula,
    _scenario_bridge_default_eps_rule,
    _scenario_bridge_eps_manual_required,
    _scenario_bridge_eps_value_formula,
    _scenario_bridge_incremental_formula,
    _scenario_bridge_negative_impact_formula,
    _scenario_bridge_row_values,
    _scenario_bridge_same_impact_formula,
    _scenario_bridge_tax_audit_rows,
    _scenario_bridge_tax_conversion_label,
    _segment_scenario_margin_value,
    _segment_scenario_note_for_view,
    _segment_scenario_revenue_m,
    _segment_scenario_specs_from_records,
    _segment_scenario_view_basis,
    write_scenario_bridge_tax_treatment_sheet as _write_scenario_bridge_tax_treatment_sheet_impl,
    write_scenario_driver_assumptions_sheet as _write_scenario_driver_assumptions_sheet_impl,
)
from .excel_writer_quarter_notes_ui_orchestrator import (
    QuarterNotesUiOrchestratorDeps,
    write_quarter_notes_ui_sheet,
)
from .excel_writer_quarter_notes_context_adapter import (
    QuarterNotesContextAdapterDeps,
    standardize_quarter_notes_ui_categories as _standardize_quarter_notes_ui_categories_impl,
    write_quarter_narrative_data_surface as _write_quarter_narrative_data_surface_impl,
    write_quarter_notes_narrative_ui_surface as _write_quarter_notes_narrative_ui_surface_impl,
    write_quarter_notes_ui_v2 as _write_quarter_notes_ui_v2_impl,
)
from .excel_writer_quarter_narrative import (
    QuarterNarrativeRecord,
    QUARTER_NARRATIVE_DATA_HEADERS,
    _quarter_narrative_record_to_audit_row,
    _quarter_narrative_records_for_ticker,
    _write_quarter_narrative_data_sheet,
    _quarter_narrative_period_sort_key,
    _quarter_narrative_source_label,
    _quarter_narrative_compact_sentence,
    _quarter_narrative_row_height,
    _quarter_narrative_recent_history_periods,
    _quarter_narrative_recent_periods_from_frame,
    _quarter_narrative_clean_text,
    _quarter_narrative_period_from_source_quarter,
    _quarter_narrative_category_theme,
    _quarter_narrative_implications_for_row,
    _quarter_narrative_records_from_quarter_notes,
    _quarter_narrative_period_from_label_or_date,
    _quarter_narrative_format_surface_value,
    _quarter_narrative_amount_from_surface_value,
    _quarter_narrative_source_date_from_period,
    _quarter_narrative_surface_row_terms,
    _quarter_narrative_records_from_history_q,
    _quarter_narrative_records_from_operating_drivers,
    _quarter_narrative_records_from_promise_progress_ui,
    _quarter_narrative_records_from_workbook_surfaces,
    _quarter_narrative_records_for_context,
    _quarter_narrative_read_block,
    _write_quarter_notes_ui_narrative_sheet,
)
from .excel_writer_segment_sources import (
    _annual_segment_latest_year_for_qa,
    _anf_add_total_company_quarter_revenue_from_history,
    _anf_annual_segment_data_from_slides_segments,
    _anf_fill_brand_quarter_revenue_from_annual_segments_for_bs,
    _filter_anf_quarterly_segment_actual_rows,
    _pbi_add_corporate_reconciliation_from_release_text,
    _pbi_repair_total_reportable_segment_quarterly_totals_for_bs,
)


def _guidance_source_contract_label(ticker: Any) -> str:
    """Visible guidance-source contract for shared UI sheets.

    Long-term rule (Option B): Guidance_Normalized is canonical only when it
    contains clean, reliable normalized rows.  If a ticker is still handled by
    curated profile / Slides_Guidance logic, keep Guidance_Normalized empty
    rather than filling it with noisy rows, and do not cite it in visible UI.
    """
    ticker_txt = str(ticker or "").strip().upper()
    if ticker_txt in {"PBI", "GPRE"}:
        return "Slides_Guidance / curated guidance profile"
    return "Guidance_Normalized"


def _gpre_45z_all_facilities_confirmed(*parts: Any) -> bool:
    """Return True when source text confirms all GPRE 45Z facilities/plants."""

    blob = glx_normalize_text(" | ".join(str(part or "") for part in parts)).lower()
    if not blob:
        return False
    has_all = bool(re.search(r"\ball\s+(?:8|eight)\b", blob, re.I))
    has_facility = bool(re.search(r"\b(plants?|facilit(?:y|ies)|operating plants?)\b", blob, re.I))
    has_45z = bool(re.search(r"\b45z\b|production tax credits?|tax credits?", blob, re.I))
    has_confirming_status = bool(
        re.search(
            r"\b(qualified|qualify|qualifying|operational|operating|running|in\s+operation|from\s+jan(?:uary)?\.?\s*1)\b",
            blob,
            re.I,
        )
    )
    return has_all and has_facility and has_45z and has_confirming_status


def _date_is_missing_or_outside(value: Any, start: date, end: date) -> bool:
    try:
        parsed = pd.Timestamp(str(value or "")).date()
    except Exception:
        return True
    return parsed < start or parsed > end





def _company_operating_margin_proxy_from_workbook(wb: Workbook) -> Tuple[Optional[float], str]:
    """Return the latest visible company operating-margin proxy from Valuation.

    This deliberately returns a concrete value, not a defined-name formula, so
    Investment_Case segment scenarios never depend on undefined names.
    """

    if wb is None or "Valuation" not in getattr(wb, "sheetnames", []):
        return None, ""
    ws = wb["Valuation"]
    candidates = [
        ("Operating margin %", "Company operating margin proxy"),
        ("EBIT margin %", "EBIT margin proxy"),
        ("Adj EBIT margin %", "Adjusted operating margin proxy"),
        ("Adj EBITDA margin %", "Adjusted EBITDA margin proxy"),
    ]
    for label, basis in candidates:
        for rr in range(1, int(ws.max_row or 0) + 1):
            if str(ws.cell(rr, 1).value or "").strip().lower() != label.lower():
                continue
            for cc in range(int(ws.max_column or 1), 1, -1):
                raw = ws.cell(rr, cc).value
                val = pd.to_numeric(raw, errors="coerce")
                if pd.isna(val):
                    continue
                margin = float(val)
                if abs(margin) > 1.5:
                    margin /= 100.0
                if math.isfinite(margin) and -0.5 <= margin <= 0.5:
                    return margin, basis
    return None, ""


def _segment_scenario_label_aliases(label: Any) -> Set[str]:
    """Return normalized aliases used to match visible segment labels to BS_Segments.

    Company source sheets often use fuller names than the Investment_Case UI
    (for example, "Presort Services" vs "Presort").  Keep this matcher narrow:
    it should improve source-backed segment margin selection without turning
    into a fuzzy segment parser.
    """

    text = str(label or "").strip().lower()

    def _norm(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", value.lower())

    aliases = {_norm(text)}
    if "presort" in text:
        aliases.update({_norm("Presort"), _norm("Presort Services")})
    if "sendtech" in text or "send tech" in text:
        aliases.update({_norm("SendTech"), _norm("SendTech Solutions"), _norm("SendTech Services")})
    if "abercrombie" in text:
        aliases.update({_norm("Abercrombie"), _norm("Abercrombie brand")})
    if "hollister" in text:
        aliases.update({_norm("Hollister"), _norm("Hollister brand")})
    if "americas" in text:
        aliases.add(_norm("Americas"))
    if "emea" in text:
        aliases.add(_norm("EMEA"))
    if "apac" in text:
        aliases.add(_norm("APAC"))
    return {alias for alias in aliases if alias}


def _bs_segments_latest_segment_margin_from_workbook(wb: Workbook, label: Any) -> Tuple[Any, str]:
    """Return latest source-backed segment margin ref from BS_Segments if present."""

    if wb is None or "BS_Segments" not in getattr(wb, "sheetnames", []):
        return None, ""
    aliases = _segment_scenario_label_aliases(label)
    if not aliases:
        return None, ""
    ws = wb["BS_Segments"]
    margin_sections = [
        ("segment operating margin %", "Segment operating margin"),
        ("operating margin %", "Segment operating margin"),
        ("segment ebit margin %", "Segment EBIT margin"),
        ("ebit margin %", "Segment EBIT margin"),
        ("segment adjusted ebit margin %", "Segment adjusted EBIT margin"),
        ("adjusted ebit margin %", "Segment adjusted EBIT margin"),
        ("segment adjusted ebitda margin %", "Segment adjusted EBITDA margin"),
        ("adjusted ebitda margin %", "Segment adjusted EBITDA margin"),
        ("ebitda margin %", "Segment EBITDA margin proxy"),
    ]
    section_basis: Dict[int, str] = {}
    for rr in range(1, int(ws.max_row or 0) + 1):
        row_label = str(ws.cell(rr, 1).value or "").strip().lower()
        for section_label, basis in margin_sections:
            if row_label == section_label:
                section_basis[rr] = basis
                break
    if not section_basis:
        return None, ""

    max_row = int(ws.max_row or 0)
    max_col = int(ws.max_column or 1)
    for section_row, basis in sorted(section_basis.items()):
        next_section = min([r for r in section_basis if r > section_row] or [max_row + 1])
        for rr in range(section_row + 1, min(next_section, section_row + 30, max_row + 1)):
            row_name = str(ws.cell(rr, 1).value or "").strip()
            row_key = re.sub(r"[^a-z0-9]+", "", row_name.lower())
            if not row_key or not any(alias in row_key or row_key in alias for alias in aliases):
                continue
            for cc in range(max_col, 1, -1):
                raw = ws.cell(rr, cc).value
                val = pd.to_numeric(raw, errors="coerce")
                if pd.isna(val):
                    continue
                margin = float(val)
                if abs(margin) > 1.5:
                    margin /= 100.0
                if math.isfinite(margin) and -0.75 <= margin <= 1.0:
                    ref = f"='BS_Segments'!${get_column_letter(cc)}${rr}"
                    return ref, basis
    return None, ""


@dataclass(frozen=True)
class _FiscalPeriodProfile:
    year_end_month: int = 12
    year_end_day: int = 31
    year_label: str = "end"


def _date_or_none(value: Any) -> Optional[date]:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).date()


def _safe_date(year: int, month: int, day: int) -> date:
    month = max(1, min(12, int(month)))
    day = max(1, min(31, int(day)))
    while True:
        try:
            return date(int(year), month, day)
        except ValueError:
            day -= 1


def _fiscal_profile_from_workbook(
    wb: Optional[Workbook],
    ticker: Any = "",
    fiscal_profile: Any = None,
) -> _FiscalPeriodProfile:
    """Resolve fiscal-year-end behavior for visible period labels and annual defaults.

    Priority is explicit caller profile, workbook/profile text, known company
    profile fallback, then calendar-year reporting.  The year-label mode is
    intentionally explicit because retailers like ANF label the year ended
    January 2026 as 2025 year, while calendar reporters use the end year.
    """

    def _profile(month: Any, day: Any, label: Any = "") -> _FiscalPeriodProfile:
        m = int(month or 12)
        d = int(day or 31)
        mode = str(label or "").strip().lower()
        if mode not in {"start", "end"}:
            mode = "start" if m <= 2 else "end"
        return _FiscalPeriodProfile(m, d, mode)

    if isinstance(fiscal_profile, _FiscalPeriodProfile):
        return fiscal_profile
    if isinstance(fiscal_profile, Mapping):
        month = fiscal_profile.get("year_end_month") or fiscal_profile.get("fiscal_year_end_month")
        day = fiscal_profile.get("year_end_day") or fiscal_profile.get("fiscal_year_end_day")
        if month and day:
            return _profile(month, day, fiscal_profile.get("year_label") or fiscal_profile.get("fiscal_year_label"))
    if isinstance(fiscal_profile, (tuple, list)) and len(fiscal_profile) >= 2:
        return _profile(fiscal_profile[0], fiscal_profile[1], fiscal_profile[2] if len(fiscal_profile) > 2 else "")

    if wb is not None:
        for sheet_name in ("SUMMARY", "Summary", "Model_Info", "QA_Checks"):
            if sheet_name not in getattr(wb, "sheetnames", []):
                continue
            ws = wb[sheet_name]
            for row in ws.iter_rows(min_row=1, max_row=min(int(ws.max_row or 0), 80), min_col=1, max_col=min(int(ws.max_column or 0), 10), values_only=True):
                blob = " ".join(str(v) for v in row if v not in (None, ""))
                if not blob:
                    continue
                m = re.search(r"\b(?:FY|fiscal year|year)\s*end(?:ed)?\s*(?:\(|:)?\s*(20\d{2})-(\d{1,2})-(\d{1,2})", blob, re.I)
                if m:
                    return _profile(m.group(2), m.group(3), "")
                m = re.search(r"\b(?:FY|fiscal year|year)\s*end(?:ed)?\s*(?:\(|:)?\s*([A-Za-z]+)\s+(\d{1,2})", blob, re.I)
                if m:
                    try:
                        month = pd.to_datetime(m.group(1), format="%B", errors="coerce")
                        if pd.isna(month):
                            month = pd.to_datetime(m.group(1), format="%b", errors="coerce")
                        if not pd.isna(month):
                            return _profile(int(pd.Timestamp(month).month), int(m.group(2)), "")
                    except Exception:
                        pass

    ticker_txt = str(ticker or "").strip().upper()
    ticker_profiles = {
        "ANF": _FiscalPeriodProfile(1, 31, "start"),
    }
    return ticker_profiles.get(ticker_txt, _FiscalPeriodProfile())


def _explicit_quarter_label_key(value: Any) -> Optional[Tuple[int, int]]:
    txt = str(value or "").strip()
    m = re.search(r"\b(20\d{2})\s*[-_/ ]?\s*Q([1-4])\b", txt, flags=re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"\bQ([1-4])\s*[-_/ ]?\s*(20\d{2})\b", txt, flags=re.I)
    if m:
        return int(m.group(2)), int(m.group(1))
    return None


def _resolve_fiscal_period_from_date(qd: date, profile: _FiscalPeriodProfile) -> Tuple[int, int, str, date]:
    candidates = [
        _safe_date(int(qd.year) + year_offset, profile.year_end_month, profile.year_end_day)
        for year_offset in (-1, 0, 1)
    ]
    eligible = [cand for cand in candidates if -10 <= (cand - qd).days <= 370]
    fy_end = min(eligible or candidates, key=lambda cand: abs((cand - qd).days))
    days_to_fy_end = (fy_end - qd).days
    if days_to_fy_end <= 45:
        fq = 4
    elif days_to_fy_end <= 135:
        fq = 3
    elif days_to_fy_end <= 225:
        fq = 2
    else:
        fq = 1
    fy = int(fy_end.year) - 1 if profile.year_label == "start" else int(fy_end.year)
    return fy, fq, f"{fy}-Q{fq}", fy_end


def _resolve_history_q_fiscal_periods_from_workbook(
    wb: Workbook,
    *,
    ticker: Any = "",
    fiscal_profile: Any = None,
) -> List[Dict[str, Any]]:
    if wb is None or "History_Q" not in getattr(wb, "sheetnames", []):
        return []
    ws = wb["History_Q"]
    if int(ws.max_row or 0) < 2 or int(ws.max_column or 0) < 1:
        return []

    def _norm(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

    headers = {_norm(ws.cell(1, cc).value): cc for cc in range(1, int(ws.max_column or 0) + 1)}

    def _col(*aliases: str) -> Optional[int]:
        for alias in aliases:
            cc = headers.get(_norm(alias))
            if cc is not None:
                return cc
        return None

    quarter_col = _col("quarter", "period", "fiscal quarter", "fiscal_period")
    fiscal_year_col = _col("fiscal_year", "fiscal year", "fy")
    fiscal_quarter_col = _col("fiscal_quarter", "fiscal quarter", "fq")
    if quarter_col is None:
        return []
    profile = _fiscal_profile_from_workbook(wb, ticker=ticker, fiscal_profile=fiscal_profile)
    out: List[Dict[str, Any]] = []
    for rr in range(2, int(ws.max_row or 0) + 1):
        raw_quarter = ws.cell(rr, quarter_col).value
        explicit = _explicit_quarter_label_key(raw_quarter)
        qd = None if explicit is not None and isinstance(raw_quarter, str) else _date_or_none(raw_quarter)
        fy: Optional[int] = None
        fq: Optional[int] = None
        fy_end_date: Optional[date] = None
        if fiscal_year_col is not None and fiscal_quarter_col is not None:
            fy_val = pd.to_numeric(ws.cell(rr, fiscal_year_col).value, errors="coerce")
            fq_val = pd.to_numeric(ws.cell(rr, fiscal_quarter_col).value, errors="coerce")
            if pd.notna(fy_val) and pd.notna(fq_val) and 1 <= int(fq_val) <= 4:
                fy, fq = int(fy_val), int(fq_val)
        if fy is None or fq is None:
            if explicit is not None:
                fy, fq = explicit
            elif qd is not None:
                fy, fq, _label, fy_end_date = _resolve_fiscal_period_from_date(qd, profile)
        if fy is None or fq is None:
            continue
        label = f"{int(fy)}-Q{int(fq)}"
        if fy_end_date is None and qd is not None:
            _fy, _fq, _label, fy_end_date = _resolve_fiscal_period_from_date(qd, profile)
        out.append(
            {
                "row": rr,
                "quarter_date": qd,
                "fiscal_year": int(fy),
                "fiscal_quarter": int(fq),
                "label": label,
                "fy_end_date": fy_end_date,
            }
        )
    out.sort(
        key=lambda rec: (
            int(rec.get("fiscal_year") or 0),
            int(rec.get("fiscal_quarter") or 0),
            rec.get("quarter_date") or date.min,
            int(rec.get("row") or 0),
        )
    )
    return out


def _history_q_latest_full_year_period_set(
    wb: Workbook,
    *,
    ticker: Any = "",
    fiscal_profile: Any = None,
) -> Dict[str, Any]:
    periods = _resolve_history_q_fiscal_periods_from_workbook(wb, ticker=ticker, fiscal_profile=fiscal_profile)
    by_year: Dict[int, Dict[int, Dict[str, Any]]] = {}
    for rec in periods:
        fy = int(rec.get("fiscal_year") or 0)
        fq = int(rec.get("fiscal_quarter") or 0)
        if fy <= 0 or fq not in {1, 2, 3, 4}:
            continue
        existing = by_year.setdefault(fy, {}).get(fq)
        if existing is None or (rec.get("quarter_date") or date.min) >= (existing.get("quarter_date") or date.min):
            by_year[fy][fq] = rec
    full_years = [fy for fy, quarters in by_year.items() if all(q in quarters for q in (1, 2, 3, 4))]
    if not full_years:
        return {}
    latest_year = max(full_years)
    rows = [by_year[latest_year][q]["row"] for q in (1, 2, 3, 4)]
    quarter_dates = [by_year[latest_year][q].get("quarter_date") for q in (1, 2, 3, 4)]
    labels = [by_year[latest_year][q].get("label") for q in (1, 2, 3, 4)]
    quarter_criteria = [
        by_year[latest_year][q].get("quarter_date") or by_year[latest_year][q].get("label")
        for q in (1, 2, 3, 4)
    ]
    previous_quarter_dates: List[date] = []
    previous_quarter_criteria: List[Any] = []
    if latest_year - 1 in by_year and all(q in by_year[latest_year - 1] for q in (1, 2, 3, 4)):
        previous_quarter_dates = [by_year[latest_year - 1][q].get("quarter_date") for q in (1, 2, 3, 4)]
        previous_quarter_criteria = [
            by_year[latest_year - 1][q].get("quarter_date") or by_year[latest_year - 1][q].get("label")
            for q in (1, 2, 3, 4)
        ]
    return {
        "fiscal_year": latest_year,
        "rows": rows,
        "quarter_dates": [qd for qd in quarter_dates if isinstance(qd, date)],
        "previous_quarter_dates": [qd for qd in previous_quarter_dates if isinstance(qd, date)],
        "quarter_criteria": [crit for crit in quarter_criteria if crit not in (None, "")],
        "previous_quarter_criteria": [crit for crit in previous_quarter_criteria if crit not in (None, "")],
        "labels": labels,
    }


def _history_q_latest_full_year_actuals_from_workbook(
    wb: Workbook,
    *,
    ticker: Any = "",
    fiscal_profile: Any = None,
) -> Dict[str, float]:
    """Return conservative latest full-year actuals from History_Q in workbook units.

    Values returned for money metrics are in $m, matching Investment_Case
    manual-input conventions.  The helper only uses years with all four
    quarter labels present, so it does not turn a partial YTD period into a
    full-year default.
    """

    if wb is None or "History_Q" not in getattr(wb, "sheetnames", []):
        return {}
    ws = wb["History_Q"]
    if int(ws.max_row or 0) < 2 or int(ws.max_column or 0) < 2:
        return {}

    def _norm(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

    headers = {_norm(ws.cell(1, cc).value): cc for cc in range(1, int(ws.max_column or 0) + 1)}

    def _col(*aliases: str) -> Optional[int]:
        for alias in aliases:
            cc = headers.get(_norm(alias))
            if cc is not None:
                return cc
        return None

    quarter_col = _col("quarter", "period", "fiscal quarter", "fiscal_period")
    if quarter_col is None:
        return {}

    period_set = _history_q_latest_full_year_period_set(wb, ticker=ticker, fiscal_profile=fiscal_profile)
    if not period_set:
        return {}
    latest_year = int(period_set["fiscal_year"])
    rows = [int(rr) for rr in period_set.get("rows", [])]
    if len(rows) != 4:
        return {}

    def _num_at(row: int, col: Optional[int]) -> Optional[float]:
        if col is None:
            return None
        val = pd.to_numeric(ws.cell(row, col).value, errors="coerce")
        if pd.isna(val):
            return None
        out = float(val)
        return out if math.isfinite(out) else None

    def _sum_money(*aliases: str) -> Optional[float]:
        cc = _col(*aliases)
        vals = [_num_at(rr, cc) for rr in rows]
        clean = [float(v) for v in vals if v is not None]
        if not clean:
            return None
        total = sum(clean)
        if abs(total) > 10000.0:
            total /= 1_000_000.0
        return total if math.isfinite(total) else None

    def _latest_numeric(*aliases: str) -> Optional[float]:
        cc = _col(*aliases)
        for rr in reversed(rows):
            val = _num_at(rr, cc)
            if val is not None:
                return val
        return None

    def _shares_m() -> Optional[float]:
        shares = _latest_numeric("shares_diluted", "diluted shares", "weighted average diluted shares", "shares")
        if shares is None:
            return None
        if abs(shares) > 10000.0:
            shares /= 1_000_000.0
        return shares if math.isfinite(shares) and shares > 0 else None

    def _sum_money_for_rows(rows_in: Sequence[int], *aliases: str) -> Optional[float]:
        cc = _col(*aliases)
        vals = [_num_at(rr, cc) for rr in rows_in]
        clean = [float(v) for v in vals if v is not None]
        if not clean:
            return None
        total = sum(clean)
        if abs(total) > 10000.0:
            total /= 1_000_000.0
        return total if math.isfinite(total) else None

    revenue_m = _sum_money("revenue", "net sales", "sales")
    ebitda_m = _sum_money("adj_ebitda", "adjusted ebitda", "ebitda")
    net_income_m = _sum_money("net_income", "net income", "net income attributable")
    cfo_m = _sum_money("cfo", "cash from operations", "operating cash flow", "net cash provided by operating activities")
    capex_m = _sum_money("capex", "capital expenditures", "capital expenditure", "property and equipment additions")
    fcf_m = _sum_money("fcf", "free cash flow")
    if fcf_m is None and cfo_m is not None and capex_m is not None:
        fcf_m = cfo_m - abs(capex_m)
    op_income_m = _sum_money("op_income", "operating income", "operating profit")
    pretax_m = _sum_money("pretax_income", "pre-tax income", "income before taxes", "income before income taxes")
    tax_m = _sum_money("income_tax_expense", "provision for income taxes", "tax expense", "income tax provision")
    buybacks_m = _sum_money("buybacks_cash", "share repurchases", "stock repurchases", "repurchases of common stock")
    shares_m = _shares_m()
    out: Dict[str, float] = {"year": float(latest_year)}
    for key, value in {
        "revenue_m": revenue_m,
        "ebitda_m": ebitda_m,
        "fcf_m": fcf_m,
        "capex_m": abs(capex_m) if capex_m is not None else None,
        "buybacks_m": abs(buybacks_m) if buybacks_m is not None else None,
    }.items():
        if value is not None and math.isfinite(float(value)):
            out[key] = float(value)
    previous_year = latest_year - 1
    previous_periods = _resolve_history_q_fiscal_periods_from_workbook(wb, ticker=ticker, fiscal_profile=fiscal_profile)
    prev_rows_by_q = {
        int(rec.get("fiscal_quarter") or 0): int(rec.get("row") or 0)
        for rec in previous_periods
        if int(rec.get("fiscal_year") or 0) == previous_year
    }
    if all(q in prev_rows_by_q for q in (1, 2, 3, 4)):
        previous_revenue_m = _sum_money_for_rows([prev_rows_by_q[q] for q in (1, 2, 3, 4)], "revenue", "net sales", "sales")
        if revenue_m is not None and previous_revenue_m and previous_revenue_m > 0:
            growth = (float(revenue_m) / float(previous_revenue_m)) - 1.0
            if math.isfinite(growth):
                out["revenue_growth"] = growth
    if net_income_m is not None and shares_m:
        eps = float(net_income_m) / float(shares_m)
        if math.isfinite(eps):
            out["eps"] = eps
    if op_income_m is not None and revenue_m and revenue_m > 0:
        margin = float(op_income_m) / float(revenue_m)
        if math.isfinite(margin) and -0.75 <= margin <= 1.0:
            out["operating_margin"] = margin
    if tax_m is not None and pretax_m and pretax_m > 0:
        tax_rate = float(tax_m) / float(pretax_m)
        if math.isfinite(tax_rate) and 0.0 <= tax_rate <= 0.35:
            out["tax_rate"] = tax_rate
    return out


def _augment_history_q_frame_for_writer(
    df: pd.DataFrame,
    *,
    ticker: Any = "",
    fiscal_profile: Any = None,
) -> pd.DataFrame:
    """Add reusable fiscal-period and operating-margin columns to History_Q."""

    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    out = df.copy()

    def _norm(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

    col_by_norm = {_norm(col): col for col in out.columns}

    def _col(*aliases: str) -> Optional[Any]:
        for alias in aliases:
            col = col_by_norm.get(_norm(alias))
            if col is not None:
                return col
        return None

    quarter_col = _col("quarter", "period", "fiscal quarter", "fiscal_period")
    if quarter_col is not None and not {"fiscal_year", "fiscal_quarter", "fiscal_label"}.issubset(set(map(str, out.columns))):
        profile = _fiscal_profile_from_workbook(None, ticker=ticker, fiscal_profile=fiscal_profile)
        fiscal_years: List[Any] = []
        fiscal_quarters: List[Any] = []
        fiscal_labels: List[Any] = []
        for raw in out[quarter_col].tolist():
            explicit = _explicit_quarter_label_key(raw)
            qd = None if explicit is not None and isinstance(raw, str) else _date_or_none(raw)
            if explicit is not None:
                fy, fq = explicit
                label = f"{fy}-Q{fq}"
            elif qd is not None:
                fy, fq, label, _fy_end = _resolve_fiscal_period_from_date(qd, profile)
            else:
                fy = fq = label = pd.NA
            fiscal_years.append(fy)
            fiscal_quarters.append(fq)
            fiscal_labels.append(label)
        if "fiscal_year" not in out.columns:
            out["fiscal_year"] = fiscal_years
        if "fiscal_quarter" not in out.columns:
            out["fiscal_quarter"] = fiscal_quarters
        if "fiscal_label" not in out.columns:
            out["fiscal_label"] = fiscal_labels

    if "operating_margin" not in out.columns:
        revenue_col = _col("revenue", "net sales", "sales")
        numerator_col = _col("op_income", "operating income", "operating profit")
        basis = "operating income / revenue"
        if numerator_col is None:
            numerator_col = _col("ebit", "income from operations", "operating earnings")
            basis = "EBIT margin proxy"
        if numerator_col is None:
            numerator_col = _col("adj_ebit", "adjusted ebit", "adjusted operating income")
            basis = "adjusted EBIT margin proxy"
        if revenue_col is not None and numerator_col is not None:
            revenue = pd.to_numeric(out[revenue_col], errors="coerce")
            numerator = pd.to_numeric(out[numerator_col], errors="coerce")
            margin = numerator / revenue.replace({0: pd.NA})
            margin = margin.where(margin.between(-0.75, 1.0))
            out["operating_margin"] = margin
            if "operating_margin_basis" not in out.columns:
                out["operating_margin_basis"] = basis

    return out


def _history_q_year_default_formulas(
    start_date: Optional[Tuple[int, int, int]] = None,
    end_date: Optional[Tuple[int, int, int]] = None,
    *,
    fiscal_year: Optional[int] = None,
    quarter_dates: Optional[Sequence[date]] = None,
    previous_quarter_dates: Optional[Sequence[date]] = None,
    quarter_criteria: Optional[Sequence[Any]] = None,
    previous_quarter_criteria: Optional[Sequence[Any]] = None,
    start_exclusive: bool = False,
    end_inclusive: bool = False,
) -> Dict[str, str]:
    """Excel formulas for latest full-year defaults when History_Q is written later.

    Prefer exact fiscal-quarter dates from the resolver.  Date ranges are kept
    only as a fallback for legacy calendar-year callers.
    """

    exact_criteria = [crit for crit in (quarter_criteria or []) if crit not in (None, "")]
    exact_prev_criteria = [crit for crit in (previous_quarter_criteria or []) if crit not in (None, "")]
    if not exact_criteria:
        exact_criteria = [qd for qd in (quarter_dates or []) if isinstance(qd, date)]
    if not exact_prev_criteria:
        exact_prev_criteria = [qd for qd in (previous_quarter_dates or []) if isinstance(qd, date)]
    fiscal_year_int: Optional[int]
    try:
        fiscal_year_int = int(fiscal_year) if fiscal_year is not None else None
    except Exception:
        fiscal_year_int = None
    use_fiscal_columns = not exact_criteria and fiscal_year_int is not None
    if not exact_criteria:
        start_date = start_date or (2025, 1, 1)
        end_date = end_date or (2026, 1, 1)
        start = f"DATE({start_date[0]},{start_date[1]},{start_date[2]})"
        end = f"DATE({end_date[0]},{end_date[1]},{end_date[2]})"
        prev_start = f"DATE({start_date[0] - 1},{start_date[1]},{start_date[2]})"
        prev_end = f"DATE({end_date[0] - 1},{end_date[1]},{end_date[2]})"
        start_op = ">" if start_exclusive else ">="
        end_op = "<=" if end_inclusive else "<"
    dates = "History_Q!$A:$A"

    def _range(metric: str) -> str:
        return f'INDEX(History_Q!$A:$ZZ,0,MATCH("{metric}",History_Q!$1:$1,0))'

    fiscal_year_range = 'INDEX(History_Q!$A:$ZZ,0,MATCH("fiscal_year",History_Q!$1:$1,0))'
    fiscal_quarter_range = 'INDEX(History_Q!$A:$ZZ,0,MATCH("fiscal_quarter",History_Q!$1:$1,0))'

    def _date_expr(qd: date) -> str:
        return f"DATE({int(qd.year)},{int(qd.month)},{int(qd.day)})"

    def _criteria_expr(crit: Any) -> str:
        if isinstance(crit, date):
            return _date_expr(crit)
        txt = str(crit or "").replace('"', '""')
        return f'"{txt}"'

    def _sum_exact(metric: str, criteria: Sequence[Any]) -> str:
        terms = [f"SUMIFS({_range(metric)},{dates},{_criteria_expr(crit)})" for crit in criteria]
        if not terms:
            return "0"
        return "(" + "+".join(terms) + ")"

    def _sum(metric: str) -> str:
        if exact_criteria:
            return _sum_exact(metric, exact_criteria)
        if use_fiscal_columns and fiscal_year_int is not None:
            return (
                f'SUMIFS({_range(metric)},{fiscal_year_range},{fiscal_year_int},'
                f'{fiscal_quarter_range},">=1",{fiscal_quarter_range},"<=4")'
            )
        return f'SUMIFS({_range(metric)},{dates},"{start_op}"&{start},{dates},"{end_op}"&{end})'

    def _sum_prev(metric: str) -> str:
        if exact_criteria:
            return _sum_exact(metric, exact_prev_criteria)
        if use_fiscal_columns and fiscal_year_int is not None:
            return (
                f'SUMIFS({_range(metric)},{fiscal_year_range},{fiscal_year_int - 1},'
                f'{fiscal_quarter_range},">=1",{fiscal_quarter_range},"<=4")'
            )
        return f'SUMIFS({_range(metric)},{dates},"{start_op}"&{prev_start},{dates},"{end_op}"&{prev_end})'

    revenue = f'=IFERROR({_sum("revenue")}/1000000,"")'
    ebitda = f'=IFERROR({_sum("ebitda")}/1000000,"")'
    capex = f'=IFERROR(ABS({_sum("capex")})/1000000,"")'
    fcf = f'=IFERROR(({_sum("cfo")}-ABS({_sum("capex")}))/1000000,"")'
    buybacks = f'=IFERROR(ABS({_sum("buybacks_cash")})/1000000,"")'
    revenue_growth = f'=IFERROR(IF({_sum_prev("revenue")}>0,{_sum("revenue")}/{_sum_prev("revenue")}-1,""),"")'
    share_denominator = (
        f'({_sum("shares_diluted")}/{max(len(exact_criteria), 1)})'
        if exact_criteria
        else (
            f'AVERAGEIFS({_range("shares_diluted")},{fiscal_year_range},{fiscal_year_int},'
            f'{fiscal_quarter_range},">=1",{fiscal_quarter_range},"<=4")'
            if use_fiscal_columns and fiscal_year_int is not None
            else f'AVERAGEIFS({_range("shares_diluted")},{dates},"{start_op}"&{start},{dates},"{end_op}"&{end})'
        )
    )
    eps = (
        f'=IFERROR(({_sum("net_income")}/1000000)'
        f'/({share_denominator}/1000000),"")'
    )
    op_margin = f'=IFERROR({_sum("op_income")}/{_sum("revenue")},"")'
    tax_rate = f'=IFERROR(IF(AND({_sum("pretax_income")}>0,{_sum("income_tax_expense")}>=0,{_sum("income_tax_expense")}/{_sum("pretax_income")}<=0.35),{_sum("income_tax_expense")}/{_sum("pretax_income")},""),"")'
    return {
        "revenue_m": revenue,
        "ebitda_m": ebitda,
        "fcf_m": fcf,
        "capex_m": capex,
        "buybacks_m": buybacks,
        "revenue_growth": revenue_growth,
        "eps": eps,
        "operating_margin": op_margin,
        "tax_rate": tax_rate,
    }


def _investment_case_scenario_render_deps(wb: Workbook) -> InvestmentCaseScenarioRenderDeps:
    return InvestmentCaseScenarioRenderDeps(runtime={"wb": wb})


def _write_scenario_driver_assumptions_sheet(
    wb: Workbook,
    *,
    ticker: Any,
    segment_specs: Sequence[_SegmentScenarioInputSpec] = (),
    enabled: bool = True,
    disabled_note: str = "",
) -> None:
    return _write_scenario_driver_assumptions_sheet_impl(
        _investment_case_scenario_render_deps(wb),
        ticker=ticker,
        segment_specs=segment_specs,
        enabled=enabled,
        disabled_note=disabled_note,
    )


def _write_scenario_bridge_tax_treatment_sheet(
    wb: Workbook,
    *,
    ticker: Any,
    specs: Sequence[_ScenarioDriverBridgeSpec],
    after_tax_factor: Optional[float] = None,
    tax_rate_ref: str = "",
    tax_source_basis: str = "",
) -> None:
    return _write_scenario_bridge_tax_treatment_sheet_impl(
        _investment_case_scenario_render_deps(wb),
        ticker=ticker,
        specs=specs,
        after_tax_factor=after_tax_factor,
        tax_rate_ref=tax_rate_ref,
        tax_source_basis=tax_source_basis,
    )



def _operating_driver_ttm_sum_from_workbook(wb: Workbook, metric_label: str) -> Optional[float]:
    """Return the latest-four-quarter sum for an Operating_Drivers metric when clean."""

    if "Operating_Drivers" not in wb.sheetnames:
        return None
    ws = wb["Operating_Drivers"]
    metric_row: Optional[int] = None
    for rr in range(1, ws.max_row + 1):
        if str(ws.cell(rr, 1).value or "").strip().lower() == metric_label.strip().lower():
            metric_row = rr
            break
    if metric_row is None:
        return None

    quarter_row: Optional[int] = None
    for rr in range(max(1, metric_row - 25), metric_row + 1):
        if str(ws.cell(rr, 1).value or "").strip().lower() == "quarter":
            quarter_row = rr
            break
    if quarter_row is None:
        return None

    def _quarter_label_end_date(label: Any) -> Optional[date]:
        m = re.fullmatch(r"\s*(\d{4})-Q([1-4])\s*", str(label or ""), flags=re.I)
        if not m:
            return _date_or_none(label)
        year = int(m.group(1))
        qtr = int(m.group(2))
        month = qtr * 3
        day = 31 if month in {3, 12} else 30
        return date(year, month, day)

    latest_history_q: Optional[date] = None
    if "History_Q" in wb.sheetnames:
        try:
            hws = wb["History_Q"]
            h_headers = [str(hws.cell(1, cc).value or "").strip().lower() for cc in range(1, hws.max_column + 1)]
            if "quarter" in h_headers:
                q_col = h_headers.index("quarter") + 1
                hist_dates = []
                for rr in range(2, hws.max_row + 1):
                    qd = _date_or_none(hws.cell(rr, q_col).value)
                    if isinstance(qd, date):
                        hist_dates.append(qd)
                if hist_dates:
                    latest_history_q = max(hist_dates)
        except Exception:
            latest_history_q = None

    quarter_cols = []
    for cc in range(2, ws.max_column + 1):
        val = str(ws.cell(quarter_row, cc).value or "").strip()
        if re.fullmatch(r"\d{4}-Q[1-4]", val):
            qd = _quarter_label_end_date(val)
            if latest_history_q is not None and isinstance(qd, date) and qd > latest_history_q:
                continue
            quarter_cols.append(cc)
    if not quarter_cols:
        return None

    vals = []
    for cc in quarter_cols[-4:]:
        val = ws.cell(metric_row, cc).value
        if val is None or val == "":
            vals.append(0.0)
            continue
        try:
            vals.append(float(val))
        except (TypeError, ValueError):
            return None
    total = float(sum(vals))
    return total if any(abs(v) > 1e-9 for v in vals) else None


def _operating_driver_latest_full_year_sum_from_workbook(wb: Workbook, metric_label: str) -> Optional[float]:
    """Return the latest full-year sum for an Operating_Drivers metric when clean."""

    if "Operating_Drivers" not in wb.sheetnames:
        return None
    ws = wb["Operating_Drivers"]
    metric_row: Optional[int] = None
    for rr in range(1, ws.max_row + 1):
        if str(ws.cell(rr, 1).value or "").strip().lower() == metric_label.strip().lower():
            metric_row = rr
            break
    if metric_row is None:
        return None

    quarter_row: Optional[int] = None
    for rr in range(max(1, metric_row - 25), metric_row + 1):
        if str(ws.cell(rr, 1).value or "").strip().lower() == "quarter":
            quarter_row = rr
            break
    if quarter_row is None:
        return None

    def _quarter_label_end_date(label: Any) -> Optional[date]:
        m = re.fullmatch(r"\s*(\d{4})-Q([1-4])\s*", str(label or ""), flags=re.I)
        if not m:
            return _date_or_none(label)
        year = int(m.group(1))
        qtr = int(m.group(2))
        month = qtr * 3
        day = 31 if month in {3, 12} else 30
        return date(year, month, day)

    latest_history_q: Optional[date] = None
    if "History_Q" in wb.sheetnames:
        try:
            hws = wb["History_Q"]
            h_headers = [str(hws.cell(1, cc).value or "").strip().lower() for cc in range(1, hws.max_column + 1)]
            if "quarter" in h_headers:
                q_col = h_headers.index("quarter") + 1
                hist_dates = []
                for rr in range(2, hws.max_row + 1):
                    qd = _date_or_none(hws.cell(rr, q_col).value)
                    if isinstance(qd, date):
                        hist_dates.append(qd)
                if hist_dates:
                    latest_history_q = max(hist_dates)
        except Exception:
            latest_history_q = None

    values_by_year: Dict[int, List[float]] = {}
    quarters_by_year: Dict[int, Set[int]] = {}
    latest_year: Optional[int] = None
    for cc in range(2, ws.max_column + 1):
        label = str(ws.cell(quarter_row, cc).value or "").strip()
        m = re.fullmatch(r"(\d{4})-Q([1-4])", label)
        if not m:
            continue
        qd = _quarter_label_end_date(label)
        if latest_history_q is not None and isinstance(qd, date) and qd > latest_history_q:
            continue
        year = int(m.group(1))
        quarter = int(m.group(2))
        latest_year = year if latest_year is None else max(latest_year, year)
        val = ws.cell(metric_row, cc).value
        parsed = 0.0
        if val not in (None, ""):
            try:
                parsed = float(val)
            except (TypeError, ValueError):
                return None
        values_by_year.setdefault(year, []).append(parsed)
        quarters_by_year.setdefault(year, set()).add(quarter)

    if latest_year is None:
        return None
    candidate_years = [
        year
        for year, quarters in quarters_by_year.items()
        if year < latest_year or quarters == {1, 2, 3, 4}
    ]
    if not candidate_years:
        return None
    year = max(candidate_years)
    vals = values_by_year.get(year, [])
    total = float(sum(vals))
    return total if any(abs(v) > 1e-9 for v in vals) else None




def _anf_visible_support_runtime() -> Dict[str, Any]:
    return {
        "pd": pd,
        "re": re,
        "math": math,
        "date": date,
        "timedelta": timedelta,
        "glx_normalize_text": glx_normalize_text,
        "_shared_visible_period_text": _shared_visible_period_text,
        "_promise_metric_definition_key": _promise_metric_definition_key,
    }


def _anf_visible_support() -> AnfVisibleSupport:
    return AnfVisibleSupport(AnfVisibleSupportDeps(runtime=_anf_visible_support_runtime()))


def _anf_fiscal_year_from_quarter_end(qd: Any) -> Optional[int]:
    return _anf_visible_support().fiscal_year_from_quarter_end(qd)


def _anf_fiscal_quarter_from_quarter_end(qd: Any) -> Optional[int]:
    return _anf_visible_support().fiscal_quarter_from_quarter_end(qd)


def _anf_visible_quarter_label(qd: Any) -> str:
    return _anf_visible_support().visible_quarter_label(qd)


def _source_backed_debt_tranches_from_slides(
    slides_debt: Optional[pd.DataFrame],
    latest_quarter: Any,
) -> pd.DataFrame:
    """Return a deduped, source-backed tranche display table for debt detail.

    This is intentionally a display fallback for cases where the stricter tranche
    tie-out guardrail suppresses Debt_Tranches_Latest.  It does not override the
    carrying debt basis; it gives the user the current source schedule plus a
    reconciliation row.
    """
    if slides_debt is None or getattr(slides_debt, "empty", True):
        return pd.DataFrame()
    q = pd.to_datetime(latest_quarter, errors="coerce")
    if pd.isna(q):
        return pd.DataFrame()
    df = slides_debt.copy()
    if "quarter" not in df.columns:
        return pd.DataFrame()
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df = df[df["quarter"].dt.normalize().eq(pd.Timestamp(q).normalize())]
    if df.empty:
        return pd.DataFrame()
    if "is_table_total" in df.columns:
        df = df[~df["is_table_total"].fillna(False).astype(bool)]
    if "amount" not in df.columns or "tranche" not in df.columns:
        return pd.DataFrame()
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount_num"].notna() & (df["amount_num"] > 0)]
    if df.empty:
        return pd.DataFrame()
    # Prefer the parsed statement table over PDF/text fragments when both are
    # available for the same quarter.  The PDF extractor often emits footnote
    # rows such as "2.25% ... 1,897 --" or generic Tallgrass rows that are
    # useful evidence but must not be added to the tranche principal schedule.
    if "doc" in df.columns:
        doc_txt = df["doc"].astype(str).str.lower()
        html_mask = doc_txt.str.endswith((".htm", ".html"))
        if "asof_match_found" in df.columns:
            asof_mask = df["asof_match_found"].fillna(False).astype(bool)
        else:
            asof_mask = pd.Series(True, index=df.index)
        preferred = df[html_mask & asof_mask]
        if not preferred.empty:
            df = preferred
        else:
            matched = df[asof_mask]
            if not matched.empty:
                df = matched

    def _clean_tranche_name(v: Any) -> str:
        txt = str(v or "").strip()
        txt = re.sub(r"\s+\$\s*[0-9,]+(?:\s+\$\s*[0-9,]+)*\s*$", "", txt)
        txt = re.sub(r"\s+[0-9]{1,3}(?:,[0-9]{3})+(?:\s+[0-9]{1,3}(?:,[0-9]{3})+)*\s*$", "", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _dedup_key(row: pd.Series) -> Tuple[str, Optional[int], int]:
        name = _clean_tranche_name(row.get("tranche"))
        name_norm = re.sub(r"\s*\(\d+\)\s*", "", name.lower())
        name_norm = re.sub(r"[^a-z0-9.%]+", " ", name_norm).strip()
        mat = pd.to_numeric(row.get("maturity_year"), errors="coerce")
        mat_key = int(mat) if pd.notna(mat) else None
        amt_key = int(round(float(row.get("amount_num") or 0.0), -3))
        return name_norm, mat_key, amt_key

    def _priority(row: pd.Series) -> Tuple[int, int, int]:
        doc = str(row.get("doc") or "").lower()
        source = str(row.get("source") or "").lower()
        asof = bool(row.get("asof_match_found")) if "asof_match_found" in row.index else False
        htmlish = doc.endswith((".htm", ".html"))
        return (1 if asof else 0, 1 if htmlish else 0, 1 if source == "financial_statement" else 0)

    rows: Dict[Tuple[str, Optional[int], int], pd.Series] = {}
    for _, row in df.iterrows():
        key = _dedup_key(row)
        if not key[0]:
            continue
        prev = rows.get(key)
        if prev is None or _priority(row) > _priority(prev):
            rows[key] = row

    out_rows: List[Dict[str, Any]] = []
    latest_year = int(pd.Timestamp(q).year)
    for _, row in sorted(
        rows.items(),
        key=lambda kv: (
            9999 if kv[1].get("maturity_year") is None or pd.isna(pd.to_numeric(kv[1].get("maturity_year"), errors="coerce")) else int(pd.to_numeric(kv[1].get("maturity_year"), errors="coerce")),
            str(kv[1].get("tranche") or ""),
        ),
    ):
        name = _clean_tranche_name(row.get("tranche"))
        mat = pd.to_numeric(row.get("maturity_year"), errors="coerce")
        mat_year = int(mat) if pd.notna(mat) else None
        coupon = None
        m_coupon = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\s*%", name)
        if m_coupon:
            try:
                coupon = float(m_coupon.group(1)) / 100.0
            except Exception:
                coupon = None
        near_term = bool(mat_year is not None and mat_year <= latest_year + 1)
        out_rows.append(
            {
                "tranche_name": name,
                "amount_principal": float(row.get("amount_num")),
                "amount_carrying": None,
                "maturity_display": str(mat_year) if mat_year is not None else "",
                "maturity_year": mat_year,
                "rate_type": "fixed" if coupon is not None else None,
                "coupon_pct": coupon,
                "spread_pct": None,
                "near_term": near_term,
                "source_kind": "Slides_Debt_Profile",
                "source_basis": (
                    "source-backed principal; near-term = within 24 months of latest quarter end; "
                    "year-based conservative classification when exact maturity date is unavailable"
                ),
                "qa_status": "WARN",
                "review_note": "Fallback source-backed debt schedule shown because tranche tie-out guardrail suppressed Debt_Tranches_Latest.",
            }
        )
    return pd.DataFrame(out_rows)


def _shared_visible_period_text(text_in: Any) -> str:
    """Normalize user-facing period labels without touching source dates."""
    txt = str(text_in or "")
    if not txt:
        return ""
    txt = re.sub(r"\bQ([1-4])\s+FY\s*(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bQ([1-4])\s+fiscal\s+(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bQ([1-4])\s*[-/]\s*(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bQ([1-4])\s+(20\d{2})\b", r"\2-Q\1", txt, flags=re.I)
    txt = re.sub(r"\bFY\s*(20\d{2})\b", r"\1 year", txt, flags=re.I)
    txt = re.sub(r"\bfiscal\s+year\s+(20\d{2})\b", r"\1 year", txt, flags=re.I)
    return txt


def _shared_readable_source_label(text_in: Any) -> str:
    txt = str(text_in or "")
    if not txt:
        return ""
    replacements = [
        (r"\brel\s*/\s*qtr\s*hist\.?\b", "earnings release / quarterly history"),
        (r"\brel\s*/\s*qtr\b", "earnings release / quarterly history"),
        (r"\bqtr\s*hist\.?\b", "quarterly history"),
        (r"\brel\s*/\s*tr\.?\b", "earnings release / earnings transcript"),
        (r"\btr\.?\b", "earnings transcript"),
        (r"\bsec\b", "SEC filing"),
        (r"\bslides\b", "investor presentation"),
    ]
    out = txt
    for pattern, repl in replacements:
        out = re.sub(pattern, repl, out, flags=re.I)
    return out


def _shared_readable_source_type_label(text_in: Any) -> str:
    """Display compact internal source ids as reviewer-readable labels."""
    txt = str(text_in or "").strip()
    if not txt:
        return ""
    key = re.sub(r"[^a-z0-9]+", "_", txt.lower()).strip("_")
    mapping = {
        "adj_metrics": "adjusted metrics",
        "earnings_presentation": "earnings presentation",
        "earnings_release": "earnings release",
        "financial_statement": "financial schedule",
        "history_q": "quarterly history",
        "model_metric": "model-derived",
        "modeled": "model-derived",
        "promise": "management guidance",
        "promise_text": "management guidance",
        "quarter_note": "quarter notes",
        "quarter_notes": "quarter notes",
        "quarter_notes_ui": "quarter notes",
        "slides_debt_profile": "debt profile slides",
        "transcript": "earnings transcript",
    }
    if key in mapping:
        return mapping[key]
    return re.sub(r"[_-]+", " ", txt).strip()


def _sector_operating_driver_intro_tables(ticker: Any) -> List[Dict[str, Any]]:
    """Return sector-specific Operating_Drivers intro tables in shared order."""
    ticker_txt = str(ticker or "").strip().upper()
    guidance_source_label = _guidance_source_contract_label(ticker_txt)
    if ticker_txt == "ANF":
        return [
            {
                "title": "Current watchlist",
                "headers": ["Watch item", "Current read", "Why it matters"],
                "rows": [
                    ("Sales guide", "2026 sales guide +3-5%", "Demand and comps must hold against tougher comparable-sales laps."),
                    ("Margin durability", "2026 operating margin guide 12.0-12.5%", "Tariff, ERP and marketing headwinds drive the EPS debate."),
                    ("Inventory quality", "Inventory cost/units +5%; tariffs/ERP explain part", "Watch markdown risk rather than treating all inventory growth as excess stock."),
                    ("Capital returns", "2025 buybacks ~$450m vs FCF ~$378m", "EPS support is meaningful, but cash returns exceeded FCF."),
                ],
            },
            {
                "title": "Current/latest outlook",
                "headers": ["Topic", "Current read", "Source / use"],
                "rows": [
                    ("Q4 actuals", "Sales +5%, comp +1%, operating margin 14.1%", "Q4 release and History_Q."),
                    ("2026 guide", "Sales +3-5%; OM 12.0-12.5%; EPS $10.20-$11.00", f"{guidance_source_label} and Promise_Progress_UI."),
                    ("Margin bridge", "Q1 tariffs ~290 bps/~$30m; freight ~160 bps; ERP >100 bps", "Q4 earnings release and transcript."),
                    ("Stores / buybacks", "55 openings / 25 closures / 70 remodels; buybacks ~$450m", "Q4 outlook table."),
                ],
            },
        ]
    if ticker_txt == "PBI":
        return [
            {
                "title": "Current watchlist",
                "headers": ["Watch item", "Current read", "Why it matters"],
                "rows": [
                    ("FCF conversion", "FCF guide and cash conversion", "Cash generation must fund debt reduction and the equity case."),
                    ("Cost savings", "Run-rate savings and EBIT flow-through", "Savings need to show up in adjusted EBIT, not just targets."),
                    ("Debt / refinancing", "Maturities, revolver and leverage", "Balance-sheet risk still drives the turnaround multiple."),
                    ("Presort", "Volumes, pricing and margin", "Presort stabilization is central to durable adjusted EBIT."),
                    ("SendTech", "Decline control and customer retention", "A slower decline lowers pressure on the turnaround bridge."),
                ],
            },
            {
                "title": "Current/latest outlook",
                "headers": ["Topic", "Current read", "Source / use"],
                "rows": [
                    ("Guidance", "Revenue, adjusted EBIT, EPS and FCF", f"{guidance_source_label} and Valuation side-panel."),
                    ("Cost actions", "Annualized savings / productivity", "Earnings releases and management updates."),
                    ("Capital structure", "Debt reduction and refinancing watch", "SEC filings, debt schedules and liquidity notes."),
                    ("Segments", "Presort and SendTech execution", "Segment tables and operating commentary."),
                ],
            },
        ]
    if ticker_txt == "GPRE":
        return [
            {
                "title": "Current watchlist",
                "headers": ["Watch item", "Current read", "Why it matters"],
                "rows": [
                    ("Crush margins", "Ethanol/corn/coproduct spread", "Margin per gallon drives EBITDA and cash generation."),
                    ("Demand / policy", "Exports, E15, RVO/SRE/RIN setup", "Policy and demand determine whether margins sustain."),
                    ("45Z / carbon", "Credit value and qualification", "45Z monetization can materially lift EBITDA."),
                    ("Production", "Gallons, utilization and downtime", "Volume turns margin into dollars and reveals operating reliability."),
                    ("Capex / balance sheet", "Capex, cash and liquidity", "Commodity cycles require disciplined liquidity management."),
                ],
            },
            {
                "title": "Current/latest outlook",
                "headers": ["Topic", "Current read", "Source / use"],
                "rows": [
                    ("45Z guidance", "2026 contribution and qualification path", f"{guidance_source_label}, releases and policy notes."),
                    ("Crush economics", "Margin, coproducts and utilization", "Economics_Overlay and Operating_Drivers."),
                    ("Policy watch", "RVO/E15/export/RIN developments", "Policy/regulatory source notes."),
                    ("Cash flow", "Capex and liquidity through cycle", "History_Q and balance-sheet data."),
                ],
            },
        ]
    return []


def _standardize_quarter_notes_ui_categories(ws: Any, ticker: Any) -> None:
    return _standardize_quarter_notes_ui_categories_impl(
        QuarterNotesContextAdapterDeps(
            runtime={
                "_shared_visible_period_text": _shared_visible_period_text,
                "glx_normalize_text": glx_normalize_text,
            }
        ),
        ws,
        ticker=ticker,
    )


def _rewrite_shared_promise_progress_ui_from_blocks(ws: Any, ticker: Any = "") -> None:
    return rewrite_shared_promise_progress_ui_from_blocks(
        PromiseProgressRewriteDeps(runtime={**globals(), **locals()}),
        ws,
        ticker=ticker,
    )




def _promise_progress_worksheet_repair_deps() -> PromiseProgressWorksheetRepairDeps:
    return PromiseProgressWorksheetRepairDeps(runtime={**globals()})


def _management_credibility_scorecard_rows(ticker: Any = "") -> List[Tuple[str, str, str, str]]:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._management_credibility_scorecard_rows(ticker)


def _insert_management_credibility_scorecard(ws: Any, ticker: Any = "") -> None:
    return insert_management_credibility_scorecard(
        _promise_progress_worksheet_repair_deps(),
        ws,
        ticker,
    )


def _polish_promise_scorecard_layout(ws: Any) -> None:
    return polish_promise_scorecard_layout(
        _promise_progress_worksheet_repair_deps(),
        ws,
    )


def _promise_header_name(value: Any) -> str:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_header_name(value)


def _set_promise_row_semantics(
    ws: Any,
    row_idx: int,
    cols: Mapping[str, int],
    *,
    change_type: Any = None,
    actual: Any = None,
    progress: Any = None,
    status: Any = None,
    note: Any = None,
) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._set_promise_row_semantics(
        ws,
        row_idx,
        cols,
        change_type=change_type,
        actual=actual,
        progress=progress,
        status=status,
        note=note,
    )


def _promise_hidden_key_slug(value: Any) -> str:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_hidden_key_slug(value)


def _ensure_anf_promise_hidden_source_keys(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._ensure_anf_promise_hidden_source_keys(ws)


def _promise_stated_quarter_parts(value: Any) -> Tuple[Optional[int], Optional[int]]:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_stated_quarter_parts(value)


def _promise_annual_year(value: Any) -> Optional[int]:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_annual_year(value)


def _promise_progress_label(value: Any, *, metric: Any = "", stated: Any = "") -> str:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_progress_label(value, metric=metric, stated=stated)


def _promise_value_looks_like_progress(value: Any, metric: Any = "") -> bool:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_value_looks_like_progress(value, metric=metric)


def _promise_revision_event_from_section(section: Any) -> str:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_revision_event_from_section(section)


def _promise_event_sort_key(value: Any, source_date: Any = "") -> Tuple[date, int, str]:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_event_sort_key(value, source_date)


def _promise_metric_definition_key(metric: Any) -> str:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_metric_definition_key(metric)


def _promise_metric_order_rank(metric: Any) -> int:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_metric_order_rank(metric)


def _clean_gpre_45z_monetization_value(value: Any) -> Any:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._clean_gpre_45z_monetization_value(value)


def _finalize_promise_revision_semantics(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._finalize_promise_revision_semantics(ws)


def _promise_status_fill_for_label(status: Any) -> PatternFill:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._promise_status_fill_for_label(status)


def _apply_promise_grid_style(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._apply_promise_grid_style(ws)


def _cleanup_anf_promise_after_repair(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._cleanup_anf_promise_after_repair(ws)


def _repair_anf_promise_actual_progress_semantics(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._repair_anf_promise_actual_progress_semantics(ws)


def _clear_pre_release_promise_actuals(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._clear_pre_release_promise_actuals(ws)


def _remove_empty_promise_revision_blocks(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._remove_empty_promise_revision_blocks(ws)


def _standardize_promise_section_layout(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._standardize_promise_section_layout(ws)


def _is_promise_section_row(ws: Any, row_idx: int) -> bool:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._is_promise_section_row(ws, row_idx)


def _ensure_q4_annual_actual_revision_rows(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._ensure_q4_annual_actual_revision_rows(ws)


def _ensure_promise_block_spacing(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._ensure_promise_block_spacing(ws)


def _dedupe_promise_progress_rows(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._dedupe_promise_progress_rows(ws)


def _remove_actual_only_promise_rows(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._remove_actual_only_promise_rows(ws)


def _remove_promise_metric_stubs(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._remove_promise_metric_stubs(ws)


def _remove_pbi_duplicate_cost_savings_timeline_rows(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._remove_pbi_duplicate_cost_savings_timeline_rows(ws)


def _ensure_cost_savings_run_rate_revision_row(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._ensure_cost_savings_run_rate_revision_row(ws)


def _remove_blank_promise_rows(ws: Any) -> None:
    _promise_progress_worksheet_repairs._set_runtime(_promise_progress_worksheet_repair_deps())
    return _promise_progress_worksheet_repairs._remove_blank_promise_rows(ws)


def _repair_promise_table_header_merges(ws: Any) -> None:
    return repair_promise_table_header_merges(
        _promise_progress_worksheet_repair_deps(),
        ws,
    )


def _final_repair_promise_progress_ui(wb: Workbook, ticker: Any = "") -> None:
    return final_repair_promise_progress_ui(
        _promise_progress_worksheet_repair_deps(),
        wb,
        ticker,
    )






































def _polish_investment_case_readability(ws: Any) -> None:
    """Apply final Investment_Case readability fixes without touching formulas."""
    if ws is None or not str(getattr(ws, "title", "")).endswith("_Investment_Case"):
        return

    max_row = int(getattr(ws, "max_row", 0) or 0)
    max_col = int(getattr(ws, "max_column", 0) or 0)

    def _txt(row: int, col: int) -> str:
        return str(ws.cell(row, col).value or "").strip()

    def _row_text(row: int) -> str:
        return " ".join(_txt(row, cc) for cc in range(1, min(max_col, 12) + 1) if _txt(row, cc))

    def _is_section(row: int) -> bool:
        fill = str(ws.cell(row, 1).fill.fgColor.rgb or "").upper()
        return bool(_txt(row, 1)) and fill.endswith(("5B9BD5", "6FA8DC"))

    for rr in range(1, max_row + 1):
        if _txt(rr, 1).lower() != "investment snapshot":
            continue
        for body_rr in range(rr + 1, max_row + 1):
            if not _txt(body_rr, 1) or _is_section(body_rr):
                break
            ws.row_dimensions[body_rr].height = 24.0
        break

    note = "Uses Investment_Case manual inputs; may differ from Valuation Thesis Bridge."
    for rr in range(1, max_row + 1):
        if note.lower() in _row_text(rr).lower():
            ws.row_dimensions[rr].height = 13.5
            for cc in range(1, min(max_col, 10) + 1):
                ws.cell(rr, cc).alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

    # Layout-aware note/read columns: only extend right when the table already
    # reserves a notes/read column near the right edge and the cells to the right
    # are empty.  This keeps formulas and output columns untouched.
    note_headers = {
        "notes",
        "notes/source",
        "source / note",
        "source / confidence",
        "read",
        "source",
        "source / read",
        "investment read",
    }
    for rr in range(1, max_row + 1):
        note_col = 0
        for cc in range(1, min(max_col, 10) + 1):
            if _txt(rr, cc).lower() in note_headers:
                note_col = cc
                break
        if note_col < 8 or note_col >= 10:
            continue
        if any(_txt(rr, cc) for cc in range(note_col + 1, 11)):
            continue
        block_end = rr
        for body_rr in range(rr + 1, max_row + 1):
            if _is_section(body_rr):
                break
            if not _row_text(body_rr):
                break
            block_end = body_rr
        for body_rr in range(rr, block_end + 1):
            if any(_txt(body_rr, cc) for cc in range(note_col + 1, 11)):
                continue
            coord = ws.cell(body_rr, note_col).coordinate
            if any(coord in merged for merged in ws.merged_cells.ranges):
                continue
            try:
                ws.merge_cells(start_row=body_rr, start_column=note_col, end_row=body_rr, end_column=10)
            except ValueError:
                continue
            ws.cell(body_rr, note_col).alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)




































def _apply_source_backed_promise_mapping_overrides(wb: Workbook, ticker: Any = "") -> None:
    return apply_source_backed_promise_mapping_overrides(
        PromiseSourceOverrideMutatorDeps(runtime={**globals(), **locals()})
    )


def _shared_ui_conventions_deps() -> SharedUiConventionsDeps:
    return SharedUiConventionsDeps(
        runtime={
            "PatternFill": PatternFill,
            "Border": Border,
            "Side": Side,
            "Font": Font,
            "Alignment": Alignment,
            "Comment": Comment,
            "get_column_letter": get_column_letter,
            "copy": copy,
            "pd": pd,
            "math": math,
            "re": re,
            "date": date,
            "_shared_visible_period_text": _shared_visible_period_text,
            "_shared_readable_source_label": _shared_readable_source_label,
            "_standardize_quarter_notes_ui_categories": _standardize_quarter_notes_ui_categories,
            "_remove_empty_promise_revision_blocks": _remove_empty_promise_revision_blocks,
            "_polish_promise_scorecard_layout": _polish_promise_scorecard_layout,
            "_apply_source_backed_promise_mapping_overrides": _apply_source_backed_promise_mapping_overrides,
            "_polish_investment_case_readability": _polish_investment_case_readability,
            "_date_or_none": _date_or_none,
            "_promise_progress_label": _promise_progress_label,
            "PROMISE_TIMELINE_HEADERS": PROMISE_TIMELINE_HEADERS,
            "PROMISE_VISIBLE_MAX_COL": PROMISE_VISIBLE_MAX_COL,
        }
    )


def _apply_shared_ui_conventions_to_workbook(wb: Workbook, ticker: Any = "") -> None:
    return apply_shared_ui_conventions_to_workbook(
        _shared_ui_conventions_deps(),
        wb,
        ticker=ticker,
    )


def _anf_buyback_execution_is_year_or_ttm(
    qd: Any,
    note_text: Any = "",
    *,
    cash_amount: Optional[float] = None,
    shares_amount: Optional[float] = None,
) -> bool:
    """Return True when an ANF buyback disclosure is annual/TTM, not quarter-only."""
    q_ts = pd.to_datetime(qd, errors="coerce")
    note = str(note_text or "")
    note_low = note.lower()
    if any(
        token in note_low
        for token in (
            "fiscal year",
            "year ended",
            "for the year",
            "full year",
            "year-to-date",
            "year to date",
            " ytd",
            "during fiscal",
        )
    ):
        return True
    try:
        q_month = int(pd.Timestamp(q_ts).month) if not pd.isna(q_ts) else 0
    except Exception:
        q_month = 0
    try:
        cash_f = float(cash_amount) if cash_amount is not None and pd.notna(cash_amount) else None
    except Exception:
        cash_f = None
    try:
        shares_f = float(shares_amount) if shares_amount is not None and pd.notna(shares_amount) else None
    except Exception:
        shares_f = None
    # ANF's latest 10-K disclosure is annual repurchases. If the parser sees a
    # January/February Q4 period with a very large cash/share amount, treating it
    # as a quarter-only buyback overstates the precision of the workbook.
    return bool(
        q_month in (1, 2)
        and cash_f is not None
        and shares_f is not None
        and cash_f >= 300_000_000.0
        and shares_f >= 3_000_000.0
    )


def _anf_format_year_ttm_buyback_summary(
    qd: Any,
    *,
    shares_amount: Optional[float] = None,
    cash_amount: Optional[float] = None,
    avg_price: Optional[float] = None,
) -> str:
    fy = _anf_fiscal_year_from_quarter_end(qd)
    try:
        q_year = int(pd.Timestamp(pd.to_datetime(qd, errors="coerce")).year)
    except Exception:
        q_year = dt.date.today().year
    year_txt = str(fy or q_year)
    parts: List[str] = [f"{year_txt} year / TTM buybacks:"]
    try:
        if shares_amount is not None and pd.notna(shares_amount):
            parts.append(f"{float(shares_amount) / 1_000_000.0:,.1f}m shares")
    except Exception:
        pass
    try:
        if cash_amount is not None and pd.notna(cash_amount):
            if len(parts) > 1:
                parts.append("for")
            parts.append(f"~${float(cash_amount) / 1_000_000.0:,.0f}m")
    except Exception:
        pass
    try:
        if avg_price is not None and pd.notna(avg_price):
            parts.append(f"at ~${float(avg_price):.2f}/share")
    except Exception:
        pass
    return " ".join(parts).strip()


def _anf_normalized_quarter_ts(qd: Any) -> Optional[pd.Timestamp]:
    q_ts = pd.to_datetime(qd, errors="coerce")
    if pd.isna(q_ts):
        return None
    return pd.Timestamp(q_ts).normalize()


def _anf_quarter_sequence(quarters: Iterable[Any]) -> List[pd.Timestamp]:
    seen: Set[pd.Timestamp] = set()
    out: List[pd.Timestamp] = []
    if quarters is None:
        quarter_iter: Iterable[Any] = ()
    else:
        quarter_iter = list(quarters)
    for q in quarter_iter:
        q_ts = _anf_normalized_quarter_ts(q)
        if q_ts is None or q_ts in seen:
            continue
        seen.add(q_ts)
        out.append(q_ts)
    return sorted(out)


def _anf_prior_year_quarter(qd: Any, quarters: Iterable[Any]) -> Optional[pd.Timestamp]:
    q_ts = _anf_normalized_quarter_ts(qd)
    if q_ts is None:
        return None
    fiscal_year = _anf_fiscal_year_from_quarter_end(q_ts)
    fiscal_quarter = _anf_fiscal_quarter_from_quarter_end(q_ts)
    if fiscal_year is None or fiscal_quarter is None:
        return None
    for cand in _anf_quarter_sequence(quarters):
        if cand == q_ts:
            continue
        if (
            _anf_fiscal_year_from_quarter_end(cand) == fiscal_year - 1
            and _anf_fiscal_quarter_from_quarter_end(cand) == fiscal_quarter
        ):
            return cand
    return None


def _anf_previous_quarter(qd: Any, quarters: Iterable[Any]) -> Optional[pd.Timestamp]:
    q_ts = _anf_normalized_quarter_ts(qd)
    if q_ts is None:
        return None
    seq = _anf_quarter_sequence(quarters)
    try:
        idx = seq.index(q_ts)
    except ValueError:
        seq = sorted(set(seq + [q_ts]))
        idx = seq.index(q_ts)
    if idx <= 0:
        return None
    return seq[idx - 1]


def _anf_normalize_value_map(src: Dict[Any, Any]) -> Dict[pd.Timestamp, Any]:
    out: Dict[pd.Timestamp, Any] = {}
    for raw_q, raw_v in dict(src or {}).items():
        q_ts = _anf_normalized_quarter_ts(raw_q)
        if q_ts is None:
            continue
        out[q_ts] = raw_v
    return out


def _anf_is_missing_value(v: Any) -> bool:
    if v is None:
        return True
    try:
        missing = pd.isna(v)
        if isinstance(missing, (bool, np.bool_)):
            return bool(missing)
    except Exception:
        pass
    return False


def _anf_yoy_map_for_fiscal_periods(
    src: Dict[Any, Any],
    quarters: Iterable[Any],
    *,
    positive_prev_only: bool = False,
    positive_cur_only: bool = False,
) -> Dict[pd.Timestamp, Any]:
    values = _anf_normalize_value_map(src)
    quarter_items = [] if quarters is None else list(quarters)
    seq = _anf_quarter_sequence(quarter_items + list(values.keys()))
    value_by_label = {
        _anf_visible_quarter_label(q): v
        for q, v in values.items()
        if _anf_visible_quarter_label(q) and not _anf_is_missing_value(v)
    }
    out: Dict[pd.Timestamp, Any] = {}
    for q in seq:
        prev = _anf_prior_year_quarter(q, seq)
        v = values.get(q)
        p = values.get(prev) if prev is not None else None
        if _anf_is_missing_value(p):
            fy = _anf_fiscal_year_from_quarter_end(q)
            fq = _anf_fiscal_quarter_from_quarter_end(q)
            if fy is not None and fq is not None:
                p = value_by_label.get(f"{fy - 1}-Q{fq}")
        if _anf_is_missing_value(v) or _anf_is_missing_value(p):
            out[q] = None
            continue
        try:
            fv = float(v)
            fp = float(p)
        except Exception:
            out[q] = None
            continue
        if fp == 0:
            out[q] = None
            continue
        if positive_prev_only and fp <= 0:
            out[q] = None
        elif positive_cur_only and fv <= 0:
            out[q] = None
        else:
            out[q] = (fv - fp) / abs(fp)
    return out


def _anf_value_delta_map_for_fiscal_periods(
    src: Dict[Any, Any],
    quarters: Iterable[Any],
    *,
    comparison: str = "yoy",
) -> Dict[pd.Timestamp, Any]:
    values = _anf_normalize_value_map(src)
    quarter_items = [] if quarters is None else list(quarters)
    seq = _anf_quarter_sequence(quarter_items + list(values.keys()))
    value_by_label = {
        _anf_visible_quarter_label(q): v
        for q, v in values.items()
        if _anf_visible_quarter_label(q) and not _anf_is_missing_value(v)
    }
    out: Dict[pd.Timestamp, Any] = {}
    cmp_key = str(comparison or "yoy").strip().lower()
    for q in seq:
        prev = _anf_previous_quarter(q, seq) if cmp_key == "qoq" else _anf_prior_year_quarter(q, seq)
        v = values.get(q)
        p = values.get(prev) if prev is not None else None
        if _anf_is_missing_value(p):
            fy = _anf_fiscal_year_from_quarter_end(q)
            fq = _anf_fiscal_quarter_from_quarter_end(q)
            if fy is not None and fq is not None:
                if cmp_key == "qoq":
                    prev_fy = fy if fq > 1 else fy - 1
                    prev_fq = fq - 1 if fq > 1 else 4
                    p = value_by_label.get(f"{prev_fy}-Q{prev_fq}")
                else:
                    p = value_by_label.get(f"{fy - 1}-Q{fq}")
        if _anf_is_missing_value(v) or _anf_is_missing_value(p):
            out[q] = None
            continue
        try:
            out[q] = float(v) - float(p)
        except Exception:
            out[q] = None
    return out


def _anf_normalize_ytd_buyback_cash_map_for_valuation(
    src: Dict[Any, Any],
    quarters: Iterable[Any],
) -> Dict[pd.Timestamp, Any]:
    """Convert ANF cumulative YTD repurchase cash disclosures into quarter deltas.

    ANF earnings schedules often restate year-to-date repurchases in each quarterly
    update. Valuation TTM rows need period cash flows, otherwise a 200/250/350/450
    YTD series turns into a bogus 1,250 TTM.
    """
    values = _anf_normalize_value_map(src)
    if not values:
        return values
    quarter_items = [] if quarters is None else list(quarters)
    seq = _anf_quarter_sequence(quarter_items + list(values.keys()))
    by_fy: Dict[int, List[pd.Timestamp]] = {}
    for q in seq:
        if q not in values:
            continue
        fy = _anf_fiscal_year_from_quarter_end(q)
        if fy is None:
            continue
        by_fy.setdefault(int(fy), []).append(q)
    out = dict(values)
    for _, fy_quarters in by_fy.items():
        numeric: List[Tuple[pd.Timestamp, float]] = []
        for q in sorted(fy_quarters):
            v = values.get(q)
            try:
                if v is None or pd.isna(v):
                    continue
                numeric.append((q, float(v)))
            except Exception:
                continue
        if len(numeric) < 2:
            continue
        is_monotonic = all(numeric[idx][1] >= numeric[idx - 1][1] - 1e-6 for idx in range(1, len(numeric)))
        has_material_rollup = numeric[-1][1] > max(numeric[0][1], 1.0) and numeric[-1][1] >= sum(v for _, v in numeric[:-1]) * 0.45
        if not (is_monotonic and has_material_rollup):
            continue
        prior_cum = 0.0
        for q, cumulative_v in numeric:
            delta_v = cumulative_v - prior_cum
            out[q] = max(delta_v, 0.0) if delta_v >= -1e-6 else cumulative_v
            prior_cum = cumulative_v
    return out


def _anf_format_guidance_display_value(metric: Any, low: Any, high: Any, value: Any, unit: Any, line: Any = "") -> str:
    return _anf_visible_support().format_guidance_display_value(metric, low, high, value, unit, line)


def _anf_valuation_guidance_rows(guidance_df: pd.DataFrame) -> List[Dict[str, str]]:
    return _anf_visible_support().valuation_guidance_rows(guidance_df)


def _anf_normalize_qa_status_rows(checks: pd.DataFrame, *, is_anf_profile: bool = False) -> pd.DataFrame:
    if checks is None or checks.empty:
        return checks
    out = checks.copy()
    if "status" not in out.columns:
        out["status"] = ""
    if "severity" not in out.columns:
        out["severity"] = ""

    def _clean_status_token(value: Any) -> str:
        token = str(value if value is not None else "").strip()
        if token.lower() in {"", "nan", "none", "null", "<na>", "nat"}:
            return ""
        low = token.lower()
        return {
            "passed": "pass",
            "passing": "pass",
            "pass": "pass",
            "warn": "warn",
            "warning": "warn",
            "fail": "fail",
            "failed": "fail",
            "info": "info",
            "informational": "info",
            "skip": "skip",
            "skipped": "skip",
        }.get(low, token.lower())

    for idx, rr in out.iterrows():
        status = _clean_status_token(rr.get("status"))
        severity = _clean_status_token(rr.get("severity"))
        check = str(rr.get("check") or "").strip()
        message = str(rr.get("message") or "").strip()
        low = f"{check} {message}".lower()
        new_status = status
        if not new_status:
            if "pass" in low:
                new_status = "pass"
            elif "fail" in severity:
                new_status = "fail"
            elif "warn" in severity:
                new_status = "warn"
            elif "skip" in low:
                new_status = "skip"
            else:
                new_status = "info"
        if is_anf_profile:
            expected_gap = (
                ("hidden_flag" in low and any(tok in low for tok in ("shares_out", "market", "price", "fcf_yield", "dividend_ps", "interest_coverage")))
                or ("debt" in low and "coverage" in low and re.search(r"\$0\.[0-9]+m", low))
                or ("cash_identity" in low and any(tok in low for tok in ("approx", "coverage", "definition", "bridge")))
            )
            if expected_gap and str(new_status).lower() == "fail":
                new_status = "warn"
                if str(severity).lower() == "fail":
                    severity = "warn"
        if not severity:
            severity = new_status if new_status in {"fail", "warn"} else "info"
        out.at[idx, "severity"] = severity
        out.at[idx, "status"] = new_status
    return out


ANF_SEGMENT_BRAND_EXPLANATION = (
    "Americas / EMEA / APAC are geographic segments; Abercrombie / Hollister are brand families. "
    "Total Company is not additive across both views."
)


def _anf_valuation_side_panel_runtime() -> Dict[str, Any]:
    return {
        "copy": copy,
        "get_column_letter": get_column_letter,
        "PatternFill": PatternFill,
        "Border": Border,
        "Side": Side,
        "Font": Font,
        "Alignment": Alignment,
        "Comment": Comment,
        "_anf_clean_visible_ui_text": _anf_clean_visible_ui_text,
        "_shared_visible_period_text": _shared_visible_period_text,
        "_style_valuation_side_panel_style_bundle": _style_valuation_side_panel_style_bundle,
    }


def _anf_valuation_side_panel_deps() -> AnfValuationSidePanelDeps:
    return AnfValuationSidePanelDeps(runtime=_anf_valuation_side_panel_runtime())


def _anf_clear_valuation_side_panels(
    ws: Any,
    *,
    start_col: int = 15,
    end_col: Optional[int] = None,
    side_max_row: int = 125,
) -> None:
    return clear_anf_valuation_side_panels(
        _anf_valuation_side_panel_deps(),
        ws,
        start_col=start_col,
        end_col=end_col,
        side_max_row=side_max_row,
    )


def _valuation_side_panel_style_bundle() -> Dict[str, Any]:
    return valuation_side_panel_style_bundle(_anf_valuation_side_panel_deps())


def _write_anf_valuation_side_panel(ws: Any, *, start_row: int = 7, start_col: int = 15, end_col: int = 29) -> Dict[str, int]:
    return write_anf_valuation_side_panel(
        _anf_valuation_side_panel_deps(),
        ws,
        start_row=start_row,
        start_col=start_col,
        end_col=end_col,
    )


def _anf_clean_visible_ui_text(text_in: Any, *, max_chars: Optional[int] = None) -> str:
    return _anf_visible_support().clean_visible_ui_text(text_in, max_chars=max_chars)


def _anf_visible_quarter_note_summaries(
    text_in: Any,
    *,
    quarter_label: Any = "",
    latest_label: Any = "",
) -> List[str]:
    return _anf_visible_support().visible_quarter_note_summaries(text_in, quarter_label=quarter_label, latest_label=latest_label)


def _anf_compact_driver_label(label_in: Any, unit_txt: Any = "") -> str:
    return _anf_visible_support().compact_driver_label(label_in, unit_txt)


def _anf_compact_driver_group(group_in: Any, label_in: Any = "", driver_key: Any = "") -> str:
    return _anf_visible_support().compact_driver_group(group_in, label_in, driver_key)


def _anf_round_visible_driver_value(value_in: Any, unit_txt: Any = "", label_in: Any = "", driver_key: Any = "") -> Optional[float]:
    return _anf_visible_support().round_visible_driver_value(value_in, unit_txt, label_in, driver_key)


def _anf_polish_quarter_note_visible_fields(category_in: Any, metric_in: Any, note_in: Any) -> Tuple[str, str]:
    return _anf_visible_support().polish_quarter_note_visible_fields(category_in, metric_in, note_in)


def _anf_clean_visible_operating_driver_records(rows_in: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _anf_visible_support().clean_visible_operating_driver_records(rows_in)


def _investment_case_support_runtime() -> Dict[str, Any]:
    return {
        "pd": pd,
        "math": math,
        "re": re,
        "date": date,
        "glx_normalize_text": glx_normalize_text,
        "_anf_visible_guidance_normalized_frame": _anf_visible_guidance_normalized_frame,
        "_anf_visible_quarter_label": _anf_visible_quarter_label,
        "_guidance_source_contract_label": _guidance_source_contract_label,
        "_segment_scenario_revenue_m": _segment_scenario_revenue_m,
        "_shared_visible_period_text": _shared_visible_period_text,
        "_anf_clean_visible_ui_text": _anf_clean_visible_ui_text,
    }


def _investment_case_support() -> InvestmentCaseSupport:
    return InvestmentCaseSupport(
        InvestmentCaseSupportDeps(runtime=_investment_case_support_runtime())
    )


def _anf_investment_case_sheet_order(
    desired_sheet_order: Sequence[str],
    raw_sheet_cluster: Sequence[str],
    *,
    is_anf_profile: bool = False,
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    return _investment_case_support().anf_investment_case_sheet_order(
        desired_sheet_order,
        raw_sheet_cluster,
        is_anf_profile=is_anf_profile,
    )


def _investment_case_sheet_order(
    desired_sheet_order: Sequence[str],
    raw_sheet_cluster: Sequence[str],
    *,
    ticker: Any = "",
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    return _investment_case_support().investment_case_sheet_order(
        desired_sheet_order,
        raw_sheet_cluster,
        ticker=ticker,
    )


def _anf_build_investment_case_data(
    *,
    hist: Any,
    operating_driver_rows: Sequence[Dict[str, Any]],
    guidance_normalized: Any,
    slides_segments: Any,
    valuation_summary: Any = None,
    adjusted_metrics: Any = None,
) -> pd.DataFrame:
    return _investment_case_support().build_anf_investment_case_data(
        hist=hist,
        operating_driver_rows=operating_driver_rows,
        guidance_normalized=guidance_normalized,
        slides_segments=slides_segments,
        valuation_summary=valuation_summary,
        adjusted_metrics=adjusted_metrics,
    )


def _sector_build_investment_case_data(
    *,
    ticker: str,
    hist: Any,
    operating_driver_rows: Sequence[Dict[str, Any]] = (),
    guidance_normalized: Any = None,
    valuation_summary: Any = None,
    economics_market_rows: Any = None,
    slides_segments: Any = None,
) -> pd.DataFrame:
    return _investment_case_support().build_sector_investment_case_data(
        ticker=ticker,
        hist=hist,
        operating_driver_rows=operating_driver_rows,
        guidance_normalized=guidance_normalized,
        valuation_summary=valuation_summary,
        economics_market_rows=economics_market_rows,
        slides_segments=slides_segments,
    )



def _sector_investment_case_render_deps(wb: Workbook) -> SectorInvestmentCaseRenderDeps:
    return SectorInvestmentCaseRenderDeps(
        runtime={
            "wb": wb,
            "SCENARIO_DRIVER_CAPITAL_STRUCTURE_INTEREST": SCENARIO_DRIVER_CAPITAL_STRUCTURE_INTEREST,
            "SCENARIO_DRIVER_CASH_FLOW_CAPEX": SCENARIO_DRIVER_CASH_FLOW_CAPEX,
            "SCENARIO_DRIVER_MANUAL_INCREMENTAL": SCENARIO_DRIVER_MANUAL_INCREMENTAL,
            "SCENARIO_DRIVER_MARGIN_EBITDA": SCENARIO_DRIVER_MARGIN_EBITDA,
            "SCENARIO_DRIVER_TAX_CREDIT_SUBSIDY": SCENARIO_DRIVER_TAX_CREDIT_SUBSIDY,
            "SCENARIO_TAX_CASH_ONLY": SCENARIO_TAX_CASH_ONLY,
            "SCENARIO_TAX_NON_TAXABLE_CREDIT": SCENARIO_TAX_NON_TAXABLE_CREDIT,
            "SCENARIO_TAX_NO_EPS_IMPACT": SCENARIO_TAX_NO_EPS_IMPACT,
            "SCENARIO_TAX_TAXABLE": SCENARIO_TAX_TAXABLE,
            "_ScenarioDriverBridgeSpec": _ScenarioDriverBridgeSpec,
            "_SegmentScenarioInputSpec": _SegmentScenarioInputSpec,
            "_bs_segments_latest_segment_margin_from_workbook": _bs_segments_latest_segment_margin_from_workbook,
            "_company_operating_margin_proxy_from_workbook": _company_operating_margin_proxy_from_workbook,
            "_date_or_none": _date_or_none,
            "_excel_manual_percent_active_formula": _excel_manual_percent_active_formula,
            "_excel_percent_value_expr": _excel_percent_value_expr,
            "_excel_visible_value_range_formula": _excel_visible_value_range_formula,
            "_fiscal_profile_from_workbook": _fiscal_profile_from_workbook,
            "_guidance_source_contract_label": _guidance_source_contract_label,
            "_history_q_latest_full_year_actuals_from_workbook": _history_q_latest_full_year_actuals_from_workbook,
            "_history_q_latest_full_year_period_set": _history_q_latest_full_year_period_set,
            "_history_q_year_default_formulas": _history_q_year_default_formulas,
            "_operating_driver_latest_full_year_sum_from_workbook": _operating_driver_latest_full_year_sum_from_workbook,
            "_operating_driver_ttm_sum_from_workbook": _operating_driver_ttm_sum_from_workbook,
            "_scenario_bridge_eps_value_formula": _scenario_bridge_eps_value_formula,
            "_scenario_bridge_row_values": _scenario_bridge_row_values,
            "_segment_scenario_specs_from_records": _segment_scenario_specs_from_records,
            "_shared_visible_period_text": _shared_visible_period_text,
            "_write_scenario_bridge_tax_treatment_sheet": _write_scenario_bridge_tax_treatment_sheet,
            "_write_scenario_driver_assumptions_sheet": _write_scenario_driver_assumptions_sheet,
        }
    )


def _write_sector_investment_case_data_sheet(wb: Workbook, ticker: Any, data: pd.DataFrame) -> None:
    return write_sector_investment_case_data_sheet(
        _sector_investment_case_render_deps(wb),
        data,
        ticker=str(ticker or "").strip().upper(),
    )


def _write_sector_investment_case_sheet(wb: Workbook, ticker: Any, data: pd.DataFrame) -> None:
    return write_sector_investment_case_sheet(
        _sector_investment_case_render_deps(wb),
        data,
        ticker=str(ticker or "").strip().upper(),
    )


def _anf_investment_case_render_deps(wb: Workbook) -> AnfInvestmentCaseRenderDeps:
    return AnfInvestmentCaseRenderDeps(
        runtime={
            "wb": wb,
            "SCENARIO_DRIVER_CASH_FLOW_CAPEX": SCENARIO_DRIVER_CASH_FLOW_CAPEX,
            "SCENARIO_DRIVER_MARGIN_EBITDA": SCENARIO_DRIVER_MARGIN_EBITDA,
            "SCENARIO_DRIVER_SHARE_COUNT_BUYBACK": SCENARIO_DRIVER_SHARE_COUNT_BUYBACK,
            "SCENARIO_TAX_CASH_ONLY": SCENARIO_TAX_CASH_ONLY,
            "SCENARIO_TAX_NO_EPS_IMPACT": SCENARIO_TAX_NO_EPS_IMPACT,
            "SCENARIO_TAX_TAXABLE": SCENARIO_TAX_TAXABLE,
            "_ScenarioDriverBridgeSpec": _ScenarioDriverBridgeSpec,
            "_SegmentScenarioInputSpec": _SegmentScenarioInputSpec,
            "_anf_clean_visible_ui_text": _anf_clean_visible_ui_text,
            "_excel_manual_percent_active_formula": _excel_manual_percent_active_formula,
            "_excel_percent_value_expr": _excel_percent_value_expr,
            "_excel_visible_value_range_formula": _excel_visible_value_range_formula,
            "_history_q_latest_full_year_actuals_from_workbook": _history_q_latest_full_year_actuals_from_workbook,
            "_history_q_latest_full_year_period_set": _history_q_latest_full_year_period_set,
            "_history_q_year_default_formulas": _history_q_year_default_formulas,
            "_scenario_bridge_eps_value_formula": _scenario_bridge_eps_value_formula,
            "_scenario_bridge_row_values": _scenario_bridge_row_values,
            "_segment_scenario_revenue_m": _segment_scenario_revenue_m,
            "_segment_scenario_specs_from_records": _segment_scenario_specs_from_records,
            "_segment_scenario_view_basis": _segment_scenario_view_basis,
            "_write_scenario_bridge_tax_treatment_sheet": _write_scenario_bridge_tax_treatment_sheet,
            "_write_scenario_driver_assumptions_sheet": _write_scenario_driver_assumptions_sheet,
        }
    )


def _write_anf_investment_case_data_sheet(wb: Workbook, data: pd.DataFrame) -> None:
    return write_anf_investment_case_data_sheet(
        _anf_investment_case_render_deps(wb),
        data,
    )


def _write_anf_investment_case_sheet(wb: Workbook, data: pd.DataFrame) -> None:
    return write_anf_investment_case_sheet(
        _anf_investment_case_render_deps(wb),
        data,
    )
















def _anf_guidance_visible_period_label(period_label: Any, source_quarter: Any = None) -> str:
    return _anf_visible_support().guidance_visible_period_label(period_label, source_quarter)


def _anf_guidance_metric_unit_is_compatible(metric_hint: Any, unit: Any, line: Any = "") -> bool:
    return _anf_visible_support().guidance_metric_unit_is_compatible(metric_hint, unit, line)


def _anf_guidance_horizon_type(label_in: Any) -> str:
    return _anf_visible_support().guidance_horizon_type(label_in)


def _anf_reclassify_guidance_period_label(period_label: Any, metric_hint: Any, line: Any) -> str:
    return _anf_visible_support().reclassify_guidance_period_label(period_label, metric_hint, line)


def _anf_visible_guidance_normalized_frame(guidance_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return _anf_visible_support().visible_guidance_normalized_frame(guidance_df)


def _anf_format_guidance_value(metric: str, low: Any = None, high: Any = None, value: Any = None, unit: Any = "", numbers: Any = "") -> str:
    return _anf_visible_support().format_guidance_value(metric, low, high, value, unit, numbers)


def _anf_build_guidance_timeline_rows(guidance_df: Optional[pd.DataFrame] = None, hist_df: Optional[pd.DataFrame] = None) -> List[Dict[str, str]]:
    return _anf_visible_support().build_guidance_timeline_rows(guidance_df, hist_df)


def _anf_build_promise_progress_sections(guidance_df: Optional[pd.DataFrame], hist_df: Optional[pd.DataFrame] = None) -> Dict[str, List[Dict[str, str]]]:
    return _anf_visible_support().build_promise_progress_sections(guidance_df, hist_df)


def _anf_recent_operating_commentary_rows(
    hist_df: Optional[pd.DataFrame],
    slides_segments: Optional[pd.DataFrame],
    quarters: Sequence[Any],
) -> List[Dict[str, Any]]:
    return _anf_visible_support().recent_operating_commentary_rows(hist_df, slides_segments, quarters)




def _slides_guidance_metric_key(metric_name: Any) -> str:
    return _anf_visible_support().slides_guidance_metric_key(metric_name)


def _slides_guidance_has_explicit_metric(
    slides_guidance: pd.DataFrame,
    qd: date,
    metric_name: str,
    *,
    require_range: bool = False,
) -> bool:
    return _anf_visible_support().slides_guidance_has_explicit_metric(slides_guidance, qd, metric_name, require_range=require_range)


def _anf_financial_schedule_support_doc_for_quarter(
    qd: date,
    *,
    adj_metrics: pd.DataFrame,
    non_gaap_files: pd.DataFrame,
    slides_segments: pd.DataFrame,
) -> str:
    return _anf_visible_support().financial_schedule_support_doc_for_quarter(qd, adj_metrics=adj_metrics, non_gaap_files=non_gaap_files, slides_segments=slides_segments)


try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency in this pipeline
    BeautifulSoup = None

from .cache_layout import canonical_shared_cache_root, ticker_cache_candidates, ticker_cache_roots_from_base_dir
from .path_config import data_root_from_sec_cache_path
from .capital_return_notes import (
    build_buyback_note as capital_return_build_buyback_note,
    build_dividend_note as capital_return_build_dividend_note,
    build_dividend_note_from_text as capital_return_build_dividend_note_from_text,
    normalize_capital_return_note_item as capital_return_normalize_note_item,
    normalize_new_prefix as capital_return_normalize_new_prefix,
)
from .company_profiles import COMPANY_PROFILES, get_company_profile
from .debt_parser import coerce_number, read_html_tables_any
from .filing_evidence_shared import (
    classify_statement_evidence_role as shared_classify_statement_evidence_role,
    build_canonical_subject_key as shared_build_canonical_subject_key,
    build_evidence_event as shared_build_evidence_event,
    build_follow_through_event as shared_build_follow_through_event,
    build_follow_through_signal as shared_build_follow_through_signal,
    investor_note_candidate_from_event as shared_investor_note_candidate_from_event,
    build_lifecycle_subject_key as shared_build_lifecycle_subject_key,
    build_parent_subject_key as shared_build_parent_subject_key,
    build_promise_lifecycle_key as shared_build_promise_lifecycle_key,
    derive_lifecycle_state as shared_derive_lifecycle_state,
    derive_status_resolution_reason as shared_derive_status_resolution_reason,
    evidence_role as shared_evidence_role,
    infer_target_period_norm as shared_infer_target_period_norm,
    is_preferred_narrative_source as shared_is_preferred_narrative_source,
    merge_same_subject_events as shared_merge_same_subject_events,
    merge_follow_through_signals as shared_merge_follow_through_signals,
    merge_evidence_events as shared_merge_evidence_events,
    narrative_drop_reason as shared_narrative_drop_reason,
    pick_best_subject_row_for_quarter as shared_pick_best_subject_row_for_quarter,
    progress_status_rank as shared_progress_status_rank,
    promise_candidate_drop_reason as shared_promise_candidate_drop_reason,
    qualify_promise_candidate as shared_qualify_promise_candidate,
    qualify_renderable_note as shared_qualify_renderable_note,
    renderable_note_drop_reason as shared_renderable_note_drop_reason,
    route_to_measurable_promise_candidate as shared_route_to_measurable_promise_candidate,
    source_class as shared_source_class,
    statement_class as shared_statement_class,
    looks_like_tabular_fragment as shared_looks_like_tabular_fragment,
)
from .doc_intel import extract_pdf_text_cached
from .derivative_crush_tests import build_derivative_crush_tests
from .derivative_oci_bridge import DERIVATIVE_EXPOSURE_COLUMNS, build_derivative_oci_bridge_from_sources
from .excel_writer_drivers import (
    candidate_records_for_template as driver_candidate_records_for_template,
    driver_best_text_record as driver_driver_best_text_record,
    driver_snippet as driver_driver_snippet,
    driver_source_display as driver_source_display,
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
from .excel_writer_basis_proxy_sandbox import (
    BasisProxySandboxWriterDeps,
    write_basis_proxy_sandbox_sheet,
)
from .excel_writer_economics_overlay import (
    GpreOverlayQuarterComparisonDeps,
    GpreOverlaySupportInputs,
    _overlay_model_label,
    write_gpre_overlay_quarter_comparisons,
    write_gpre_basis_proxy_overlay_support,
)
from .excel_writer_economics_overlay_derivatives import (
    GpreEconomicsOverlayDerivativeSideEffectDeps,
    write_gpre_derivative_crush_tests_side_effect,
)
from .excel_writer_derivative_oci_bridge import (
    DerivativeOciBridgeRenderDeps,
    write_derivative_crush_tests_sheet,
    write_derivative_oci_bridge_sheet,
)
from .excel_writer_debt_convertible_enrichment import (
    DebtConvertibleEnrichmentDeps,
    DebtConvertibleEnrichmentSupport,
)
from .excel_writer_summary_sheet import (
    SummarySheetRenderDeps,
    write_summary_sheet,
)
from .excel_writer_hidden_value_flags import (
    HiddenValueFlagsSheetInputs,
    write_hidden_value_flags_sheet,
)
from .excel_writer_hidden_value_surface import (
    HiddenValueSurfaceModelInputs,
    NO_TRIGGER_DISPLAY_LABEL,
    NO_TRIGGER_DISPLAY_SCORE,
    NO_TRIGGER_DISPLAY_SEVERITY,
    NO_TRIGGER_DISPLAY_SUPPORT,
    NO_TRIGGER_DISPLAY_TITLE,
    build_hidden_value_surface_model,
    hidden_flag_field,
    hidden_flag_score,
    hidden_value_ai_helper_formula,
)
from .excel_writer_promise_progress import (
    PromiseProgressRenderHelpers,
    PromiseProgressSheetInputs,
    write_promise_progress_sheet,
)
from .excel_writer_promise_progress_anf import (
    AnfPromiseProgressWriterDeps,
    write_anf_promise_progress_ui_sheet,
)
from .excel_writer_promise_progress_render_adapter import (
    PromiseProgressRowWriterDeps,
    build_promise_progress_row_writer,
)
from .excel_writer_promise_progress_sources import (
    PromiseProgressSourceDeps,
    PromiseProgressSourceSupport,
)
from .excel_writer_promise_progress_bundle import (
    PromiseProgressUiBundleDeps,
    build_promise_progress_ui_bundle,
)
from .excel_writer_promise_progress_guidance_accuracy import (
    PromiseProgressGuidanceAccuracyDeps,
    build_guidance_accuracy_rows as promise_progress_build_guidance_accuracy_rows,
)
from .excel_writer_promise_progress_orchestrator import (
    PromiseProgressOrchestratorDeps,
    write_promise_progress_ui_v2 as promise_progress_write_promise_progress_ui_v2,
)
from .excel_writer_promise_progress_followthrough import (
    PromiseProgressFollowthroughDeps,
    PromiseProgressFollowthroughModel,
)
from .excel_writer_promise_progress_selection import (
    PromiseProgressSelectionDeps,
    select_promise_progress_rows_for_display,
)
from .excel_writer_promise_progress_rows import (
    PromiseProgressRowsDeps,
    normalize_promise_progress_rows_for_display,
    _dedupe_display_progress_rows as promise_rows_dedupe_display_progress_rows,
    _dedupe_promise_progress_rows as promise_rows_dedupe_promise_progress_rows,
    _display_progress_metric as promise_rows_display_progress_metric,
    _promise_progress_visible_category_rank_local as promise_rows_visible_category_rank_local,
)
from .excel_writer_promise_progress_repairs import (
    GpreProgressTrimDeps,
    PromiseProgressVisibleRepairDeps,
    repair_promise_progress_visible_rows_for_render,
    trim_gpre_final_progress_rows,
)
from .excel_writer_promise_tracker import (
    PromiseTrackerWriterDeps,
    write_promise_tracker_ui_sheet,
)
from .excel_writer_segments import (
    annual_segment_label as ew_annual_segment_label,
    extract_quarter_from_cell as ew_extract_quarter_from_cell,
    extract_segment_line_values as ew_extract_segment_line_values,
    extract_year_from_cell as ew_extract_year_from_cell,
    latest_segment_financials_workbook as ew_latest_segment_financials_workbook,
    parse_annual_segment_data_from_workbook as ew_parse_annual_segment_data_from_workbook,
    parse_quarterly_segment_data_from_workbook as ew_parse_quarterly_segment_data_from_workbook,
    quarterly_segment_label as ew_quarterly_segment_label,
)
from .excel_writer_sources import (
    build_leverage_audit_doc_index as source_build_leverage_audit_doc_index,
    build_leverage_local_material_index as source_build_leverage_local_material_index,
    build_valuation_filing_docs_by_quarter as source_build_valuation_filing_docs_by_quarter,
    docs_for_valuation_accn as source_docs_for_valuation_accn,
    extract_adj_net_leverage_text_map as source_extract_adj_net_leverage_text_map,
    first_existing_material_dir as source_first_existing_material_dir,
    hist_quarter_whitelist as source_hist_quarter_whitelist,
    infer_cached_doc_quarter as source_infer_cached_doc_quarter,
    infer_q_from_name as source_infer_q_from_name,
    looks_like_leverage_text as source_looks_like_leverage_text,
    normalize_leverage_quarter as source_normalize_leverage_quarter,
    normalize_leverage_text as source_normalize_leverage_text,
    path_cache_key as source_path_cache_key,
    read_cached_doc_raw as source_read_cached_doc_raw,
    read_cached_doc_text as source_read_cached_doc_text,
    resolve_cached_doc_path as source_resolve_cached_doc_path,
    sec_docs_for_accession as source_sec_docs_for_accession,
    slide_text_paths as source_slide_text_paths,
    submission_cache_files as source_submission_cache_files,
    submission_recent_rows as source_submission_recent_rows,
)
from .guidance_lexicon import (
    FORWARD_NOTES_LABEL,
    GUIDANCE_UI_METRIC_PRIORITY,
    classify_metric as glx_classify_metric,
    classify_status as glx_classify_status,
    dedup_text_key as glx_dedup_text_key,
    doc_type_priority as glx_doc_type_priority,
    extract_numeric_patterns as glx_extract_numeric_patterns,
    is_preferred_section as glx_is_preferred_section,
    normalize_text as glx_normalize_text,
    normalize_period as glx_normalize_period,
    score_chunk as glx_score_chunk,
    split_sentences as glx_split_sentences,
)
from .legacy_support import (
    _coerce_next_quarter_end,
    _coerce_prev_quarter_end,
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
)
from .market_data.service import (
    _build_gpre_proxy_implied_results_bundle as market_build_gpre_proxy_implied_results_bundle,
    _gpre_parse_snapshot_date_like,
    _gpre_footprint_for_quarter,
    _gpre_official_market_weights_for_quarter,
    _gpre_phase_preview_story as market_gpre_phase_preview_story,
    build_current_qtd_simple_crush_snapshot,
    build_gpre_basis_proxy_model,
    build_gpre_overlay_proxy_preview_bundle,
    build_gpre_plant_capacity_history,
    build_gpre_official_proxy_history_series,
    build_gpre_official_proxy_snapshot,
    build_simple_crush_history_series,
    build_prior_quarter_simple_crush_snapshot,
    build_next_quarter_thesis_snapshot,
    fetch_gpre_corn_bids_snapshot,
    load_or_download_gpre_corn_bids_snapshot,
    load_market_export_rows,
    market_input_fingerprint,
    persist_gpre_frozen_thesis_snapshot,
    resolve_gpre_quarter_open_snapshot,
)
from .non_gaap import infer_quarter_end_from_text, parse_adjusted_from_plain_text, strip_html
from .pdf_utils import silence_pdfminer_warnings
from .period_resolver import (
    PickResult,
    _duration_days,
    _filter_unit,
    build_quarter_calendar_from_revenue,
    classify_duration,
    choose_best_tag,
    derive_quarter_from_ytd,
    pick_best_duration,
    pick_best_instant,
    quarter_ends_for_fy,
    self_check_period_logic,
)


def _excel_string_ref(sheet_name: str, col_idx: int, start_row: int, end_row: int) -> str:
    safe_sheet = str(sheet_name or "").replace("'", "''")
    col_letter = get_column_letter(int(col_idx))
    return f"'{safe_sheet}'!${col_letter}${int(start_row)}:${col_letter}${int(end_row)}"


def _apply_chart_text_categories(chart_in: Any, *, sheet_name: str, col_idx: int, start_row: int, end_row: int) -> None:
    if chart_in is None or int(end_row) < int(start_row):
        return
    formula = _excel_string_ref(sheet_name, col_idx, start_row, end_row)
    try:
        if not isinstance(getattr(chart_in, "x_axis", None), TextAxis):
            chart_in.x_axis = TextAxis()
    except Exception:
        pass
    try:
        chart_in.x_axis.auto = False
        chart_in.x_axis.axPos = "b"
        chart_in.x_axis.delete = False
        chart_in.x_axis.tickLblPos = "low"
        chart_in.x_axis.majorTickMark = "out"
        chart_in.x_axis.minorTickMark = "none"
        chart_in.x_axis.tickLblSkip = 1
        chart_in.x_axis.tickMarkSkip = 1
        chart_in.x_axis.noMultiLvlLbl = True
        chart_in.x_axis.lblOffset = 100
        chart_in.x_axis.majorGridlines = ChartLines(
            spPr=GraphicalProperties(
                ln=LineProperties(
                    w=6350,
                    solidFill="D0D0D0",
                )
            )
        )
    except Exception:
        pass
    for series_in in list(getattr(chart_in, "series", ()) or ()):
        try:
            series_in.cat = AxDataSource(strRef=StrRef(f=formula))
        except Exception:
            continue
from .pipeline_types import WorkbookInputs
from .quarter_notes_lexicon import (
    compact_snippet as qn_compact_snippet,
    is_complete_signal_text as qn_is_complete_signal_text,
    score_promise_candidate as qn_score_promise_candidate,
    score_quarter_note_candidate as qn_score_quarter_note_candidate,
)
from .operating_drivers_runtime import (
    extract_gpre_45z_accounting_memo,
    OperatingDriversDeps,
    build_operating_drivers_history_rows as runtime_build_operating_drivers_history_rows,
    extract_operating_driver_rows_for_template as runtime_extract_operating_driver_rows_for_template,
    format_operating_driver_delta as runtime_format_operating_driver_delta,
    gpre_canonical_crush_series_for_drivers as runtime_gpre_canonical_crush_series_for_drivers,
    make_driver_row as runtime_make_driver_row,
    merge_driver_rows as runtime_merge_driver_rows,
)
from .sec_xbrl import SecClient, normalize_accession, parse_date
from .signals import build_hidden_value_flags, build_hidden_value_outputs, build_signals_base
from .valuation_precompute_runtime import (
    buyback_execution_scope_text as runtime_buyback_execution_scope_text,
    cap_alloc_unit_mult as runtime_cap_alloc_unit_mult,
    classify_distribution_signal as runtime_classify_distribution_signal,
    extract_cap_alloc_quarter_cash_sentence as runtime_extract_cap_alloc_quarter_cash_sentence,
    extract_cap_alloc_row_cash as runtime_extract_cap_alloc_row_cash,
    extract_valuation_filing_doc_text as runtime_extract_valuation_filing_doc_text,
    has_buyback_execution_table_context as runtime_has_buyback_execution_table_context,
    is_cumulative_buyback_context as runtime_is_cumulative_buyback_context,
    is_debt_repurchase_noise as runtime_is_debt_repurchase_noise,
    parse_cap_alloc_amount as runtime_parse_cap_alloc_amount,
)
from .excel_writer_placeholders import write_empty_sheet_placeholder
from .writer_types import (
    WriterCallbacks,
    WriterContext,
    WriterDerivedData,
    WriterDocumentCache,
    WriterRuntimeData,
)
from .writer_runtime_cache import WriterRuntimeCache
from .writer_qa_policy import latest_quarter_support_gap_severity as writer_qa_latest_quarter_support_gap_severity
from .valuation import valuation_engine, valuation_to_frames


def _net_debt_yoy_flag_label_and_status(delta: Optional[float]) -> Tuple[str, str]:
    """Return a visible net-debt trend label whose wording matches delta sign."""
    if delta is None or pd.isna(delta):
        return "Net debt trend (YoY)", "N/A"
    delta_f = float(delta)
    flat_threshold = 5_000_000.0
    pass_threshold = -50_000_000.0
    if abs(delta_f) <= flat_threshold:
        return "Stable: Net debt roughly flat (YoY)", "WARN"
    if delta_f < 0:
        status = "PASS" if delta_f <= pass_threshold else "WARN"
        return "Green: Net debt decreasing (YoY)", status
    return "Watch: Net debt increasing (YoY)", "FAIL"


def _net_debt_yoy_flag_label_and_status_for_position(
    delta: Optional[float],
    current_net_debt: Optional[float],
) -> Tuple[str, str]:
    """Use net-cash wording when debt increased only because net cash declined."""
    if delta is None or pd.isna(delta):
        return "Net debt trend (YoY)", "N/A"
    try:
        current_f = float(current_net_debt) if current_net_debt is not None and pd.notna(current_net_debt) else None
    except Exception:
        current_f = None
    if current_f is not None and current_f < 0 and float(delta) > 0:
        return "Watch: Net cash decreased YoY", "WARN"
    return _net_debt_yoy_flag_label_and_status(delta)


def _build_compat_state(ctx: WriterContext) -> Dict[str, Any]:
    state = dict(vars(ctx.inputs))
    state.update(
        {
            "out_path": ctx.data.out_path,
            "ticker": ctx.data.ticker,
            "excel_mode": ctx.data.excel_mode,
            "profile_timings": ctx.data.profile_timings,
            "quarter_notes_audit": ctx.data.quarter_notes_audit,
            "enable_operating_drivers_sheet": ctx.data.enable_operating_drivers_sheet,
            "enable_economics_overlay_sheet": ctx.data.enable_economics_overlay_sheet,
            "enable_economics_market_raw_sheet": ctx.data.enable_economics_market_raw_sheet,
            "_driver_inputs_ready": ctx.data.driver_inputs_ready,
            "operating_driver_history_rows": ctx.data.operating_driver_history_rows,
            "economics_market_rows": ctx.data.economics_market_rows,
            "qa_checks": ctx.data.qa_checks,
            "info_log": ctx.data.info_log,
            "data_is_rules_df": ctx.data.data_is_rules_df,
            "writer_timings": ctx.writer_timings,
        }
    )
    state.update(ctx.data.extra_values)
    state.update(ctx.callbacks.as_state_mapping())
    if "_ui_state" in state and "ui_state" not in state:
        state["ui_state"] = state["_ui_state"]
    return state


def _serialize_quarter_note_runtime_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime, date)):
        ts = pd.to_datetime(value, errors="coerce")
        return ts.isoformat() if pd.notna(ts) else str(value)
    if isinstance(value, dict):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)
        except Exception:
            return str(value)
    if isinstance(value, (list, tuple, set)):
        try:
            return json.dumps(list(value), ensure_ascii=True, default=str)
        except Exception:
            return str(value)
    return str(value)


def _quarter_note_runtime_signature(item: Dict[str, Any]) -> Tuple[str, ...]:
    return (
        _serialize_quarter_note_runtime_value(item.get("note_id")),
        _serialize_quarter_note_runtime_value(item.get("candidate_type")),
        _serialize_quarter_note_runtime_value(item.get("change_badge")),
        _serialize_quarter_note_runtime_value(item.get("bucket")),
        _serialize_quarter_note_runtime_value(item.get("_metric_display")),
        _serialize_quarter_note_runtime_value(item.get("metric_canon")),
        _serialize_quarter_note_runtime_value(item.get("metric_tag")),
        _serialize_quarter_note_runtime_value(item.get("_split_focus")),
        _serialize_quarter_note_runtime_value(item.get("_render_summary")),
        _serialize_quarter_note_runtime_value(item.get("_pbi_compact_note")),
        _serialize_quarter_note_runtime_value(item.get("target_display")),
        _serialize_quarter_note_runtime_value(item.get("target_period_norm") or item.get("period_key")),
        _serialize_quarter_note_runtime_value(item.get("doc_priority")),
        _serialize_quarter_note_runtime_value(item.get("score")),
        _serialize_quarter_note_runtime_value(item.get("_event_score")),
        _serialize_quarter_note_runtime_value(item.get("theme_key")),
        _serialize_quarter_note_runtime_value(item.get("idea_label")),
        _serialize_quarter_note_runtime_value(item.get("text_full")),
        _serialize_quarter_note_runtime_value(item.get("evidence_snippet")),
        _serialize_quarter_note_runtime_value(item.get("comment_full_text")),
        _serialize_quarter_note_runtime_value(item.get("source") or {}),
    )


def _quarter_note_runtime_qd_token(qd_ref: Optional[date]) -> str:
    q_ts = pd.to_datetime(qd_ref, errors="coerce")
    return q_ts.strftime("%Y-%m-%d") if pd.notna(q_ts) else ""


def _quarter_note_runtime_cache_key(
    cache_name: str,
    item: Dict[str, Any],
    qd_ref: Optional[date],
) -> Tuple[str, str, Tuple[str, ...]]:
    return (
        str(cache_name or ""),
        _quarter_note_runtime_qd_token(qd_ref),
        _quarter_note_runtime_signature(item),
    )


def build_writer_context(inputs: WorkbookInputs) -> WriterContext:
    """Create the run-scoped writer state used by every workbook section.

    This function is the main handoff from pipeline outputs to workbook rendering. It
    copies the normalized `WorkbookInputs`, initializes the workbook object, and sets
    up run-local caches/state that later sheet writers reuse so expensive document,
    valuation, operating-driver, and GPRE economics analysis only happens once per
    export.
    """
    out_path = inputs.out_path
    hist = inputs.hist
    audit = inputs.audit
    needs_review = inputs.needs_review
    debt_tranches = inputs.debt_tranches
    debt_recon = inputs.debt_recon
    adj_metrics = inputs.adj_metrics
    adj_breakdown = inputs.adj_breakdown
    non_gaap_files = inputs.non_gaap_files
    adj_metrics_relaxed = inputs.adj_metrics_relaxed
    adj_breakdown_relaxed = inputs.adj_breakdown_relaxed
    non_gaap_files_relaxed = inputs.non_gaap_files_relaxed
    info_log = inputs.info_log
    tag_coverage = inputs.tag_coverage
    period_checks = inputs.period_checks
    qa_checks = inputs.qa_checks
    bridge_q = inputs.bridge_q
    manifest_df = inputs.manifest_df
    ocr_log = inputs.ocr_log
    qfd_preview = inputs.qfd_preview
    qfd_unused = inputs.qfd_unused
    debt_profile = inputs.debt_profile
    debt_tranches_latest = inputs.debt_tranches_latest
    debt_maturity = inputs.debt_maturity
    debt_credit_notes = inputs.debt_credit_notes
    revolver_df = inputs.revolver_df
    revolver_history = inputs.revolver_history
    debt_buckets = inputs.debt_buckets
    slides_segments = inputs.slides_segments
    slides_debt = inputs.slides_debt
    slides_guidance = inputs.slides_guidance
    quarter_notes = inputs.quarter_notes
    promises = inputs.promises
    promise_progress = inputs.promise_progress
    non_gaap_cred = inputs.non_gaap_cred
    company_overview = inputs.company_overview
    ticker = inputs.ticker
    price = inputs.price
    strictness = inputs.strictness
    excel_mode = inputs.excel_mode
    is_rules = inputs.is_rules
    cache_dir = inputs.cache_dir
    quiet_pdf_warnings = inputs.quiet_pdf_warnings
    rebuild_doc_text_cache = inputs.rebuild_doc_text_cache
    profile_timings = inputs.profile_timings
    quarter_notes_audit = inputs.quarter_notes_audit
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    font_size = 12
    header_size = 13

    wb = Workbook()
    wb.remove(wb.active)
    state: Dict[str, Any] = {}
    ctx_ref: Optional[WriterContext] = None
    # Writer runtime caches are intentionally per-export. They help the many sheet
    # writers share heavy intermediate work inside one workbook build without turning
    # those intermediates into cross-run persisted state.
    runtime_cache = WriterRuntimeCache()
    operating_driver_history_rows: List[Dict[str, Any]] = []
    economics_market_rows: List[Dict[str, Any]] = []
    valuation_style_bundle_cache = runtime_cache.valuation_style_bundle_cache
    valuation_render_bundle_cache = runtime_cache.valuation_render_bundle_cache
    valuation_precompute_bundle_cache = runtime_cache.valuation_precompute_bundle_cache
    valuation_filing_docs_by_quarter_cache = runtime_cache.valuation_filing_docs_by_quarter_cache
    document_cache = WriterDocumentCache()
    frame_view_cache: Dict[Tuple[str, str], pd.DataFrame] = {}
    operating_drivers_runtime = runtime_cache.operating_drivers
    operating_driver_template_index_cache = operating_drivers_runtime.template_index_cache
    operating_driver_bridge_bundle_cache = operating_drivers_runtime.bridge_bundle_cache
    operating_driver_line_index_by_quarter_cache = operating_drivers_runtime.line_index_by_quarter_cache
    operating_driver_flat_line_index_cache = operating_drivers_runtime.flat_line_index_cache
    operating_driver_best_text_cache = operating_drivers_runtime.best_text_cache
    operating_driver_template_rows_cache = operating_drivers_runtime.template_rows_cache
    operating_driver_template_candidate_cache = operating_drivers_runtime.template_candidate_cache
    operating_driver_text_cache = operating_drivers_runtime.text_cache
    profile_slide_signals_cache = operating_drivers_runtime.profile_slide_signals_cache
    profile_slide_signals_by_quarter_cache = operating_drivers_runtime.profile_slide_signals_by_quarter_cache
    operating_driver_45z_guidance_docs_by_quarter_cache = operating_drivers_runtime.guidance_45z_docs_by_quarter_cache
    adj_net_leverage_text_map_cache = runtime_cache.adj_net_leverage_text_map_cache
    leverage_local_material_index_cache = runtime_cache.leverage_local_material_index_cache
    leverage_audit_doc_index_cache = runtime_cache.leverage_audit_doc_index_cache
    promise_progress_ui_bundle_cache = runtime_cache.promise_progress_ui_bundle_cache
    valuation_buyback_auth_source_bundle_cache = runtime_cache.valuation_buyback_auth_source_bundle_cache
    quarter_notes_runtime = runtime_cache.quarter_notes
    valuation_precompute_runtime = runtime_cache.valuation_precompute

    @contextmanager
    def _timed_writer_substage(name: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            _record_writer_substage(name, start)

    def _record_writer_substage(name: str, start: float) -> None:
        dt_s = time.perf_counter() - start
        timings = state.get("writer_timings") if isinstance(state, dict) else None
        if isinstance(timings, dict):
            timings[name] = timings.get(name, 0.0) + dt_s
        if profile_timings:
            print(f"[timing] {name}={dt_s:.2f}s", flush=True)

    def _record_writer_elapsed(name: str, elapsed_s: float) -> None:
        if elapsed_s <= 0:
            return
        timings = state.get("writer_timings") if isinstance(state, dict) else None
        if isinstance(timings, dict):
            timings[name] = timings.get(name, 0.0) + elapsed_s
        if profile_timings:
            print(f"[timing] {name}={elapsed_s:.2f}s", flush=True)

    def _build_hidden_value_flags_fallback(flags_audit_df: pd.DataFrame) -> pd.DataFrame:
        if flags_audit_df is None or flags_audit_df.empty:
            return pd.DataFrame(columns=["Flag", "Title", "Status", "Why it failed", "Key blocker"])
        audit_df = flags_audit_df.copy()
        audit_df["quarter"] = pd.to_datetime(audit_df.get("quarter"), errors="coerce")
        latest_q = audit_df["quarter"].dropna().max() if "quarter" in audit_df.columns else pd.NaT
        if pd.notna(latest_q):
            audit_df = audit_df[audit_df["quarter"] == latest_q].copy()
        rows: List[Dict[str, Any]] = []

        def _fmt_pct_local(v: float) -> str:
            return f"{float(v) * 100:.1f}%"

        def _parse_json_local(raw: Any) -> Dict[str, Any]:
            if raw is None:
                return {}
            if isinstance(raw, str) and not raw.strip():
                return {}
            try:
                if pd.isna(raw):
                    return {}
            except Exception:
                pass
            try:
                val = json.loads(str(raw))
            except Exception:
                return {}
            return val if isinstance(val, dict) else {}

        def _blocker_from_row(flag_id: str, msg: str, inputs: Dict[str, Any]) -> Tuple[str, str]:
            low_msg = str(msg or "").lower()
            code = str(flag_id or "").upper()
            if "fcf_yield" in low_msg or "market_cap" in low_msg or "price" in low_msg:
                return "Blocked by missing price", "Price-linked trigger unavailable"
            if code == "D":
                lev = pd.to_numeric(inputs.get("leverage_ratio"), errors="coerce")
                if pd.notna(lev):
                    return "Blocked by leverage", f"Leverage {float(lev):.2f}x above threshold"
            if code in {"A", "F"}:
                sh_yoy = pd.to_numeric(inputs.get("shares_yoy"), errors="coerce")
                if pd.notna(sh_yoy):
                    if float(sh_yoy) > 0:
                        return "Blocked by rising share count", f"Shares YoY {_fmt_pct_local(float(sh_yoy))}"
                    return "Near miss", f"Shares YoY {_fmt_pct_local(float(sh_yoy))}"
            if code == "G":
                dps = pd.to_numeric(inputs.get("dividend_ps_q"), errors="coerce")
                dps_ly = pd.to_numeric(inputs.get("dividend_ps_yoy"), errors="coerce")
                if pd.isna(dps):
                    return "Blocked by missing dividend/share", "no_current_dividend_signal"
                if float(dps) == 0.0:
                    return "Blocked by no dividend", "explicit_dividend=0"
                if pd.notna(dps_ly) and float(dps_ly) < 0:
                    return "Blocked by dividend_stopped", "implied_historical_dividend"
                return "Near miss", "explicit_dividend"
            if "missing required inputs" in low_msg:
                return "Near miss", str(msg).split("|")[0].strip()
            return "Near miss", str(msg).split("|")[0].strip() or "Threshold not met"

        for _, row in audit_df.iterrows():
            if bool(row.get("pass_fail")):
                continue
            inputs = _parse_json_local(row.get("inputs_json"))
            status, blocker = _blocker_from_row(str(row.get("flag_id") or ""), str(row.get("qa_message") or ""), inputs)
            why = str(row.get("qa_message") or "").strip() or "Threshold not met"
            rows.append(
                {
                    "Flag": str(row.get("flag_id") or ""),
                    "Title": str(row.get("flag_name") or row.get("title") or row.get("metric") or "").strip(),
                    "Status": status,
                    "Why it failed": why,
                    "Key blocker": blocker,
                }
            )
        return pd.DataFrame(rows[:7], columns=["Flag", "Title", "Status", "Why it failed", "Key blocker"])
    ui_info_rows: List[Dict[str, Any]] = []

    ticker_roots: List[Path] = []
    repo_root = Path(__file__).resolve().parents[2]

    def _path_within_scope(path_in: Any, root_in: Any) -> bool:
        try:
            Path(path_in).expanduser().resolve().relative_to(Path(root_in).expanduser().resolve())
            return True
        except Exception:
            return False

    def _company_material_roots() -> List[Path]:
        roots: List[Path] = []
        seen: set[str] = set()

        def _add_root(p: Path) -> None:
            if not _path_belongs_to_ticker(p, ticker, ticker_roots):
                return
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen:
                return
            seen.add(rp)
            roots.append(p)

        if out_path.parent.name.lower().endswith("model excel") and out_path.parent.parent.exists():
            _add_root(out_path.parent.parent)
        tkr = str(ticker or "").strip()
        explicit_material_scope = False
        allow_repo_material_fallback = True
        if cache_dir is not None:
            try:
                cache_base = Path(cache_dir).expanduser().resolve()
            except Exception:
                cache_base = Path(cache_dir)
            if tkr:
                repo_ticker_root = repo_root / tkr.upper()
                repo_shared_cache = canonical_shared_cache_root(repo_root)
                allow_repo_material_fallback = (
                    _path_within_scope(cache_base, repo_ticker_root)
                    or _path_within_scope(cache_base, repo_shared_cache)
                    or _path_within_scope(repo_root, cache_base)
                )
            legacy_company_root = cache_base.parent if cache_base.name.lower() == "sec_cache" else None
            if (
                legacy_company_root is not None
                and legacy_company_root.exists()
                and str(legacy_company_root.name or "").strip().upper() == str(tkr or "").strip().upper()
            ):
                ticker_roots.append(legacy_company_root)
                _add_root(legacy_company_root)
                explicit_material_scope = True
            if tkr:
                nearby_ancestors: List[Path] = []
                for ancestor in [cache_base.parent, *list(cache_base.parents)[:4]]:
                    try:
                        ancestor_key = str(Path(ancestor).expanduser().resolve())
                    except Exception:
                        ancestor_key = str(ancestor)
                    if any(str(x) == ancestor_key for x in nearby_ancestors):
                        continue
                    nearby_ancestors.append(Path(ancestor))
                for ancestor in nearby_ancestors:
                    for cand in [
                        ancestor / "tickers" / tkr.upper(),
                        ancestor / "tickers" / tkr,
                        ancestor / "tickers" / tkr.lower(),
                        ancestor / tkr.upper(),
                        ancestor / tkr,
                        ancestor / tkr.lower(),
                    ]:
                        if not cand.exists() or not cand.is_dir():
                            continue
                        ticker_roots.append(cand)
                        _add_root(cand)
                        explicit_material_scope = True
        if tkr:
            for cand in [repo_root / tkr.upper(), repo_root / tkr, repo_root / tkr.lower()]:
                if not allow_repo_material_fallback:
                    break
                if not cand.exists():
                    continue
                if explicit_material_scope:
                    break
                ticker_roots.append(cand)
                _add_root(cand)
        return roots

    material_roots = _company_material_roots()
    company_profile = get_company_profile(ticker)
    profile_ticker = str(getattr(company_profile, "ticker", "") or ticker or "").strip().upper()
    is_pbi_profile = profile_ticker == "PBI"
    is_gpre_profile = profile_ticker == "GPRE"
    is_anf_profile = profile_ticker == "ANF"

    def _is_repo_profile_cache_path(path_in: Any) -> bool:
        profile_key = str(profile_ticker or ticker or "").strip().upper()
        if not profile_key:
            return False
        repo_shared = canonical_shared_cache_root(repo_root)
        repo_profile_roots = [
            repo_shared,
            repo_shared / profile_key,
            repo_root / profile_key / "sec_cache",
        ]
        return any(_path_within_scope(path_in, root) for root in repo_profile_roots)

    def _allow_repo_profile_cache_fallback() -> bool:
        if cache_dir is None:
            return True
        try:
            cache_base = Path(cache_dir).expanduser().resolve()
        except Exception:
            cache_base = Path(cache_dir)
        if _path_within_scope(cache_base, canonical_shared_cache_root(repo_root)):
            return True
        profile_key = str(profile_ticker or ticker or "").strip().upper()
        if profile_key and _path_within_scope(cache_base, repo_root / profile_key / "sec_cache"):
            return True
        return _path_within_scope(repo_root, cache_base)
    enable_operating_drivers_sheet = bool(
        getattr(company_profile, "enable_operating_drivers_sheet", False)
    )
    enable_economics_overlay_sheet = bool(
        getattr(company_profile, "enable_economics_overlay_sheet", False)
    )
    enable_economics_market_raw_sheet = bool(
        getattr(company_profile, "enable_economics_market_raw_sheet", False)
    )
    enable_quarterly_segment_block = bool(
        getattr(company_profile, "enable_quarterly_segment_block", False)
    )
    quarterly_segment_labels = tuple(
        getattr(company_profile, "quarterly_segment_labels", tuple()) or tuple()
    )
    enable_annual_segment_block = bool(
        getattr(company_profile, "enable_annual_segment_block", True)
    )
    annual_segment_labels = tuple(getattr(company_profile, "annual_segment_labels", tuple()) or tuple())
    annual_segment_alias_patterns = tuple(
        getattr(company_profile, "annual_segment_alias_patterns", tuple()) or tuple()
    )
    pbi_summary_description_fallback = str(
        getattr(company_profile, "summary_description_fallback", "") or ""
    ).strip()
    pbi_summary_dependency_fallbacks = tuple(
        getattr(company_profile, "summary_dependency_fallbacks", tuple()) or tuple()
    )
    pbi_summary_wrong_thesis_fallbacks = tuple(
        getattr(company_profile, "summary_wrong_thesis_fallbacks", tuple()) or tuple()
    )
    profile_signal_runtime: Dict[str, Any] = {**globals(), **locals()}
    profile_signal_support: Optional[ProfileSignalSupport] = None

    def _refresh_profile_signal_runtime(runtime_update: Optional[Mapping[str, Any]] = None) -> None:
        if runtime_update is not None:
            profile_signal_runtime.update(runtime_update)

    def _get_profile_signal_support() -> ProfileSignalSupport:
        nonlocal profile_signal_support
        if profile_signal_support is None:
            profile_signal_support = ProfileSignalSupport(
                ProfileSignalSupportDeps(runtime=profile_signal_runtime)
            )
        return profile_signal_support

    def _is_preferred_narrative_source(source_type_in: Any) -> bool:
        return _get_profile_signal_support().is_preferred_narrative_source(source_type_in)

    def _looks_pbi_fragment_text(text_in: Any) -> bool:
        return _get_profile_signal_support().looks_pbi_fragment_text(text_in)

    def _is_pbi_clean_sentence(text_in: Any) -> bool:
        return _get_profile_signal_support().is_pbi_clean_sentence(text_in)

    def _pbi_target_display_ok(text_in: Any) -> bool:
        return _get_profile_signal_support().pbi_target_display_ok(text_in)

    def _profile_signal_helper(name: str, *args: Any, **kwargs: Any) -> Any:
        return _get_profile_signal_support().call_helper(name, *args, **kwargs)

    def _classify_pbi_metric_label(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_classify_pbi_metric_label", *args, **kwargs)

    def _extract_pbi_guidance_targets_multi(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_pbi_guidance_targets_multi", *args, **kwargs)

    def _extract_pbi_target_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_pbi_target_display", *args, **kwargs)

    def _first_existing_material_dir(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_first_existing_material_dir", *args, **kwargs)

    def _parse_quarter_from_filename(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_parse_quarter_from_filename", *args, **kwargs)

    def _pbi_reported_fcf_payload_for_qd(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_reported_fcf_payload_for_qd", *args, **kwargs)

    def _ensure_terminal_period(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_ensure_terminal_period", *args, **kwargs)

    def _parse_gpre_crush_margin_pair_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_parse_gpre_crush_margin_pair_local", *args, **kwargs)

    def _collapse_repeated_leading_ngram_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_collapse_repeated_leading_ngram_local", *args, **kwargs)

    def _dedupe_canonical_text_parts_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_dedupe_canonical_text_parts_local", *args, **kwargs)

    def _pbi_guidance_period_label_from_text(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_period_label_from_text", *args, **kwargs)

    def _pbi_structured_guidance_items_for_qd(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_structured_guidance_items_for_qd", *args, **kwargs)

    def _period_label_to_norm(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_period_label_to_norm", *args, **kwargs)

    def _pbi_repair_guidance_period_meta(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_repair_guidance_period_meta", *args, **kwargs)

    def _lookup_pbi_structured_guidance_target(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_lookup_pbi_structured_guidance_target", *args, **kwargs)

    def _lookup_pbi_structured_progress_hint(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_lookup_pbi_structured_progress_hint", *args, **kwargs)

    def _pbi_structured_strategy_items_for_qd(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_structured_strategy_items_for_qd", *args, **kwargs)

    def _parse_quarter_from_follow_text(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_parse_quarter_from_follow_text", *args, **kwargs)

    def _profile_slide_metric(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_profile_slide_metric", *args, **kwargs)

    def _extract_money_targets_for_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_money_targets_for_display", *args, **kwargs)

    def _extract_45z_2026_target_candidates(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_45z_2026_target_candidates", *args, **kwargs)

    def _strong_45z_2026_target_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_strong_45z_2026_target_display", *args, **kwargs)

    def _coerce_amount_with_unit_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_coerce_amount_with_unit_local", *args, **kwargs)

    def _fmt_short_money_value_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_fmt_short_money_value_local", *args, **kwargs)

    def _is_45z_crush_margin_support_only(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_is_45z_crush_margin_support_only", *args, **kwargs)

    def _extract_45z_monetization_target_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_45z_monetization_target_display", *args, **kwargs)

    def _extract_45z_realized_progress_text(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_45z_realized_progress_text", *args, **kwargs)

    def _slide_signal_noise(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_slide_signal_noise", *args, **kwargs)

    def _text_fragment_penalty(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_text_fragment_penalty", *args, **kwargs)

    def _clean_target_bonus(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_clean_target_bonus", *args, **kwargs)

    def _source_rank(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_source_rank", *args, **kwargs)

    def _management_theme_key(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_management_theme_key", *args, **kwargs)

    def _split_target_group_key(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_group_key", *args, **kwargs)

    def _split_target_qend(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_qend", *args, **kwargs)

    def _split_target_family_key(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_family_key", *args, **kwargs)

    def _derive_split_target_meta(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_derive_split_target_meta", *args, **kwargs)

    def _split_target_metric_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_metric_display", *args, **kwargs)

    def _gpre_normalize_metric_label(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_normalize_metric_label", *args, **kwargs)

    def _gpre_clean_visible_promise_metric(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_clean_visible_promise_metric", *args, **kwargs)

    def _gpre_bad_visible_promise_reason(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_bad_visible_promise_reason", *args, **kwargs)

    def _split_target_scope_token(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_scope_token", *args, **kwargs)

    def _split_target_scope_is_broad(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_scope_is_broad", *args, **kwargs)

    def _target_period_is_closed(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_target_period_is_closed", *args, **kwargs)

    def _infer_target_period(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_infer_target_period", *args, **kwargs)

    def _nearest_amount_for_pattern(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_nearest_amount_for_pattern", *args, **kwargs)

    def _infer_target_structure(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_infer_target_structure", *args, **kwargs)

    def _split_target_identity_key(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_identity_key", *args, **kwargs)

    def _candidate_quality_key(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_candidate_quality_key", *args, **kwargs)
    def _pbi_forced_release_backed_fcf_note_for_qd(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_forced_release_backed_fcf_note_for_qd", *args, **kwargs)

    def _pbi_guidance_period_for_text(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_period_for_text", *args, **kwargs)

    def _fmt_pbi_million_amount(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_fmt_pbi_million_amount", *args, **kwargs)

    def _pbi_guidance_table_target_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_table_target_display", *args, **kwargs)

    def _format_directional_fcf_summary_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_format_directional_fcf_summary_local", *args, **kwargs)

    def _format_directional_from_prior_summary_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_format_directional_from_prior_summary_local", *args, **kwargs)

    def _parse_signed_money_token_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_parse_signed_money_token_local", *args, **kwargs)

    def _pbi_guidance_self_contained_summary(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_self_contained_summary", *args, **kwargs)

    def _pbi_guidance_compact_note(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_compact_note", *args, **kwargs)

    def _pbi_compact_guidance_note(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_compact_guidance_note", *args, **kwargs)

    def _pbi_guidance_sentence(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_sentence", *args, **kwargs)

    def _pbi_period_label_from_norm(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_period_label_from_norm", *args, **kwargs)

    def _pbi_guidance_period_norm_is_reasonable(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_guidance_period_norm_is_reasonable", *args, **kwargs)

    def _pbi_default_guidance_period_label(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_default_guidance_period_label", *args, **kwargs)

    def _pbi_parse_money_amount(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_parse_money_amount", *args, **kwargs)

    def _pbi_parse_money_range(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_parse_money_range", *args, **kwargs)

    def _pbi_strategy_target_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_strategy_target_display", *args, **kwargs)

    def _pbi_structured_note_rows_for_qd(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_pbi_structured_note_rows_for_qd", *args, **kwargs)

    def _profile_slide_category(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_profile_slide_category", *args, **kwargs)

    def _fmt_short_money_value_with_parens_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_fmt_short_money_value_with_parens_local", *args, **kwargs)

    def _profile_slide_target_display(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_profile_slide_target_display", *args, **kwargs)

    def _slide_line_is_noise(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_slide_line_is_noise", *args, **kwargs)

    def _slide_line_is_heading(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_slide_line_is_heading", *args, **kwargs)

    def _extract_profile_signal_texts(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_profile_signal_texts", *args, **kwargs)

    def _clean_profile_signal_text(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_clean_profile_signal_text", *args, **kwargs)

    def _split_target_scope_key(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_split_target_scope_key", *args, **kwargs)

    def _clean_split_target_scope_label(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_clean_split_target_scope_label", *args, **kwargs)

    def _is_time_scope_label(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_is_time_scope_label", *args, **kwargs)

    def _extract_named_split_target_scope(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_named_split_target_scope", *args, **kwargs)

    def _gpre_clean_visible_note_metric(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_clean_visible_note_metric", *args, **kwargs)

    def _quarter_bounds(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_quarter_bounds", *args, **kwargs)

    def _half_bounds(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_half_bounds", *args, **kwargs)

    def _extract_target_amounts_with_spans(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_extract_target_amounts_with_spans", *args, **kwargs)

    def _slide_excerpt(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_slide_excerpt", *args, **kwargs)

    def _profile_slide_signals_cache_path(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_profile_slide_signals_cache_path", *args, **kwargs)

    def _profile_slide_signals_signature(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_profile_slide_signals_signature", *args, **kwargs)

    def _profile_slide_signal_jsonable(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_profile_slide_signal_jsonable", *args, **kwargs)

    def _read_profile_slide_signals_cache(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_read_profile_slide_signals_cache", *args, **kwargs)

    def _write_profile_slide_signals_cache(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_write_profile_slide_signals_cache", *args, **kwargs)

    def _quarter_matches_source_event_window_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_quarter_matches_source_event_window_local", *args, **kwargs)

    def _gpre_official_risk_management_note_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_official_risk_management_note_local", *args, **kwargs)

    def _gpre_transcript_risk_management_note_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_transcript_risk_management_note_local", *args, **kwargs)

    def _gpre_official_45z_realized_note_local(*args: Any, **kwargs: Any) -> Any:
        return _profile_signal_helper("_gpre_official_45z_realized_note_local", *args, **kwargs)
    def _load_profile_slide_signals() -> List[Dict[str, Any]]:
        nonlocal profile_slide_signals_cache
        rows = _get_profile_signal_support().load_profile_slide_signals()
        profile_slide_signals_cache = _get_profile_signal_support().profile_slide_signals_cache
        profile_signal_runtime["profile_slide_signals_cache"] = profile_slide_signals_cache
        return rows

    def _load_profile_slide_signals_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
        nonlocal profile_slide_signals_by_quarter_cache
        grouped = _get_profile_signal_support().load_profile_slide_signals_by_quarter()
        profile_slide_signals_by_quarter_cache = _get_profile_signal_support().profile_slide_signals_by_quarter_cache
        profile_signal_runtime["profile_slide_signals_by_quarter_cache"] = profile_slide_signals_by_quarter_cache
        return grouped

    def _profile_slide_signals_for_quarter(qd: date) -> List[Dict[str, Any]]:
        return _get_profile_signal_support().profile_slide_signals_for_quarter(qd)

    def _local_slide_driver_fallback(qd: date, driver_kind: str) -> str:
        return _get_profile_signal_support().local_slide_driver_fallback(qd, driver_kind)

    def _pbi_slide_pages_for_qd(qd: Optional[date]) -> List[Dict[str, Any]]:
        return _get_profile_signal_support().pbi_slide_pages_for_qd(qd)

    def _local_slide_45z_realized_text(qd: date) -> str:
        return _get_profile_signal_support().local_slide_45z_realized_text(qd)

    _pbi_note_theme_re = _get_profile_signal_support().pbi_note_theme_re
    _pbi_promise_theme_re = _get_profile_signal_support().pbi_promise_theme_re

    def _cache_roots() -> List[Path]:
        roots: List[Path] = []
        seen: set[str] = set()

        def _add_root(p: Path) -> None:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen:
                return
            seen.add(rp)
            roots.append(p)

        # Manifest-derived cache roots (highest confidence).
        if manifest_df is not None and not manifest_df.empty:
            pcol = None
            col_map = {str(c).strip().lower(): c for c in manifest_df.columns}
            for key in ("path", "cache_path", "file_path", "local_path"):
                if key in col_map:
                    pcol = col_map[key]
                    break
            if pcol is not None:
                for raw in manifest_df[pcol].dropna().astype(str).head(1000):
                    s = str(raw).strip()
                    if s.lower().startswith("file:///"):
                        s = s[8:]
                        if re.match(r"^[A-Za-z]:", s):
                            s = s.replace("/", "\\")
                    elif s.lower().startswith("file://"):
                        s = s[7:]
                    xp = Path(s)
                    if xp.exists():
                        _add_root(xp.parent)
                    else:
                        # Keep unresolved path parent for later probing.
                        _add_root(xp.parent)

        repo_root = Path(__file__).resolve().parents[2]
        tkr = str(ticker or "").strip()
        ticker_specific_candidates = ticker_cache_candidates(repo_root, tkr, Path(cache_dir) if cache_dir is not None else None)
        if not _allow_repo_profile_cache_fallback():
            ticker_specific_candidates = [
                cand for cand in ticker_specific_candidates if not _is_repo_profile_cache_path(cand)
            ]
        for cand in ticker_specific_candidates:
            if cand.exists():
                _add_root(cand)
        if cache_dir is not None and tkr:
            try:
                cache_base = Path(cache_dir).expanduser()
            except Exception:
                cache_base = Path(cache_dir)
            nearby_ancestors = [cache_base.parent, *list(cache_base.parents)[:4]]
            for ancestor in nearby_ancestors:
                for cand in [
                    ancestor / "sec_cache" / tkr.upper(),
                    ancestor / "sec_cache" / tkr,
                    ancestor / "sec_cache" / tkr.lower(),
                    ancestor / tkr.upper() / "sec_cache",
                    ancestor / tkr / "sec_cache",
                    ancestor / tkr.lower() / "sec_cache",
                ]:
                    if cand.exists():
                        _add_root(cand)
        for root in material_roots:
            for cand in ticker_cache_roots_from_base_dir(root):
                if cand.exists():
                    _add_root(cand)

        # Only fall back to global cache roots when no ticker-specific cache root exists.
        if _allow_repo_profile_cache_fallback() and not any(
            c.exists() and c != canonical_shared_cache_root(repo_root) for c in ticker_specific_candidates
        ):
            _add_root(canonical_shared_cache_root(repo_root))
            _add_root(Path("sec_cache"))
        return roots

    cache_roots = _cache_roots()
    cache_root = next((p for p in cache_roots if p.exists()), Path(__file__).resolve().parents[2] / "sec_cache")
    pdf_text_cache_root = Path(cache_dir) if cache_dir is not None else cache_root
    _shared_local_bs_payload_cache: Dict[date, Dict[str, Any]] = {}
    _shared_local_bs_file_index_cache: Optional[List[Dict[str, Any]]] = None
    _shared_local_bs_records_by_quarter_cache: Optional[Dict[date, List[Dict[str, Any]]]] = None
    _shared_local_bs_quarter_cache: Dict[str, Optional[date]] = {}
    _shared_local_bs_payload_by_path_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def _read_material_text(path_in: Path) -> str:
        return _read_cached_doc_text(path_in)

    def _shared_financial_statement_files() -> List[Path]:
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
                if path_in.suffix.lower() not in {".txt", ".htm", ".html"}:
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

    def _shared_local_balance_sheet_file_index() -> List[Dict[str, Any]]:
        nonlocal _shared_local_bs_file_index_cache
        if _shared_local_bs_file_index_cache is not None:
            return _shared_local_bs_file_index_cache
        indexed: List[Dict[str, Any]] = []
        for path_in in _shared_financial_statement_files():
            indexed.append(
                {
                    "path": path_in,
                    "path_key": _path_cache_key(path_in),
                    "suffix": path_in.suffix.lower(),
                    "quarter": _parse_quarter_from_filename(path_in.name),
                }
            )
        _shared_local_bs_file_index_cache = indexed
        return _shared_local_bs_file_index_cache

    def _shared_local_balance_sheet_quarter(rec: Dict[str, Any]) -> Optional[date]:
        path_in = rec.get("path")
        if not isinstance(path_in, Path):
            return None
        path_key = str(rec.get("path_key") or _path_cache_key(path_in))
        if path_key in _shared_local_bs_quarter_cache:
            return _shared_local_bs_quarter_cache.get(path_key)
        qd = rec.get("quarter")
        if not isinstance(qd, date):
            raw_txt = _read_material_text(path_in)
            qd = (
                _parse_quarter_from_follow_text(raw_txt)
                or infer_quarter_end_from_text(raw_txt)
            )
        qd_out = qd if isinstance(qd, date) else None
        _shared_local_bs_quarter_cache[path_key] = qd_out
        return qd_out

    def _shared_local_balance_sheet_records_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
        nonlocal _shared_local_bs_records_by_quarter_cache
        if _shared_local_bs_records_by_quarter_cache is not None:
            return _shared_local_bs_records_by_quarter_cache
        grouped: Dict[date, List[Dict[str, Any]]] = {}
        for rec in _shared_local_balance_sheet_file_index():
            qd = rec.get("quarter")
            if not isinstance(qd, date):
                qd = _shared_local_balance_sheet_quarter(rec)
            if not isinstance(qd, date):
                continue
            grouped.setdefault(qd, []).append(rec)
        _shared_local_bs_records_by_quarter_cache = grouped
        return _shared_local_bs_records_by_quarter_cache

    def _shared_local_balance_sheet_payload_for_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        path_in = rec.get("path")
        if not isinstance(path_in, Path):
            return None
        path_key = str(rec.get("path_key") or _path_cache_key(path_in))
        if path_key in _shared_local_bs_payload_by_path_cache:
            return _shared_local_bs_payload_by_path_cache.get(path_key)
        qd = _shared_local_balance_sheet_quarter(rec)
        if not isinstance(qd, date):
            _shared_local_bs_payload_by_path_cache[path_key] = None
            return None
        result = None
        try:
            if str(rec.get("suffix") or "").lower() in {".htm", ".html"}:
                result = _extract_balance_sheet_from_html(path_in.read_bytes(), qd)
            else:
                result = _extract_balance_sheet_from_text(_read_material_text(path_in), qd)
        except Exception:
            result = None
        if not result:
            _shared_local_bs_payload_by_path_cache[path_key] = None
            return None
        payload = dict(result)
        payload["source_doc"] = str(path_in)
        payload["_quarter"] = qd
        _shared_local_bs_payload_by_path_cache[path_key] = payload
        return payload

    def _shared_load_local_balance_sheet_detail_payloads(
        target_quarters: Optional[set[date]] = None,
    ) -> Dict[date, Dict[str, Any]]:
        target_qs = {qd for qd in (target_quarters or set()) if isinstance(qd, date)}
        if target_qs and all(qd in _shared_local_bs_payload_cache for qd in target_qs):
            return {
                qd: payload
                for qd, payload in _shared_local_bs_payload_cache.items()
                if qd in target_qs
            }
        with _timed_writer_substage("write_excel.valuation.bundle.local_bs.index"):
            records_by_quarter = _shared_local_balance_sheet_records_by_quarter()

        candidate_records: List[Dict[str, Any]] = []
        if target_qs:
            for qd in sorted(target_qs):
                candidate_records.extend(records_by_quarter.get(qd, []))
        else:
            for recs in records_by_quarter.values():
                candidate_records.extend(recs)

        with _timed_writer_substage("write_excel.valuation.bundle.local_bs.parse_selected"):
            parsed_payloads: List[Dict[str, Any]] = []
            for rec in candidate_records:
                payload = _shared_local_balance_sheet_payload_for_record(rec)
                if not payload:
                    continue
                parsed_payloads.append(payload)

        with _timed_writer_substage("write_excel.valuation.bundle.local_bs.pick_best"):
            for payload in parsed_payloads:
                qd = payload.get("_quarter")
                if not isinstance(qd, date):
                    continue
                current = _shared_local_bs_payload_cache.get(qd)
                if current is None or len(payload.get("values", {})) >= len(current.get("values", {})):
                    _shared_local_bs_payload_cache[qd] = payload
        if not target_qs:
            return dict(_shared_local_bs_payload_cache)
        return {
            qd: payload
            for qd, payload in _shared_local_bs_payload_cache.items()
            if qd in target_qs
        }

    def _carry_forward_low_change_series(
        src_map: Dict[pd.Timestamp, Optional[float]],
        q_series: List[Any],
        *,
        max_gap_quarters: int = 4,
        rel_tol: float = 1e-4,
        abs_tol: float = 1_000.0,
    ) -> Dict[pd.Timestamp, Optional[float]]:
        ordered = [pd.Timestamp(qv) for qv in q_series]
        out_map: Dict[pd.Timestamp, Optional[float]] = {
            pd.Timestamp(qv): (None if src_map.get(pd.Timestamp(qv)) is None else float(src_map.get(pd.Timestamp(qv))))
            for qv in ordered
        }
        explicit_idx = [idx for idx, qv in enumerate(ordered) if out_map.get(pd.Timestamp(qv)) is not None]
        if len(explicit_idx) < 2:
            return out_map

        def _sameish(a: Optional[float], b: Optional[float]) -> bool:
            if a is None or b is None:
                return False
            lim = max(abs_tol, rel_tol * max(abs(float(a)), abs(float(b)), 1.0))
            return abs(float(a) - float(b)) <= lim

        for idx, qv in enumerate(ordered):
            qk = pd.Timestamp(qv)
            if out_map.get(qk) is not None:
                continue
            prev_candidates = [ii for ii in explicit_idx if ii < idx]
            next_candidates = [ii for ii in explicit_idx if ii > idx]
            prev_idx = prev_candidates[-1] if prev_candidates else None
            next_idx = next_candidates[0] if next_candidates else None
            prev_val = out_map.get(pd.Timestamp(ordered[prev_idx])) if prev_idx is not None else None
            next_val = out_map.get(pd.Timestamp(ordered[next_idx])) if next_idx is not None else None
            if (
                prev_idx is not None
                and next_idx is not None
                and (idx - prev_idx) <= max_gap_quarters
                and (next_idx - idx) <= max_gap_quarters
                and _sameish(prev_val, next_val)
            ):
                out_map[qk] = None if prev_val is None else float(prev_val)
                continue
            if prev_idx is not None and (idx - prev_idx) <= max_gap_quarters and prev_val is not None:
                out_map[qk] = float(prev_val)
        return out_map

    if slides_segments is None:
        slides_segments = pd.DataFrame()
    if slides_debt is None:
        slides_debt = pd.DataFrame()
    if slides_guidance is None:
        slides_guidance = pd.DataFrame()
    if quarter_notes is None:
        quarter_notes = pd.DataFrame()
    if promises is None:
        promises = pd.DataFrame()
    if promise_progress is None:
        promise_progress = pd.DataFrame()
    if non_gaap_cred is None:
        non_gaap_cred = pd.DataFrame()
    if company_overview is None:
        company_overview = {
            "what_it_does": "N/A",
            "what_it_does_source": "Source: N/A (company overview not available)",
            "current_strategic_context": "N/A",
            "current_strategic_context_source": "Source: N/A (current strategic context not available)",
            "key_advantage": "N/A",
            "key_advantage_source": "Source: N/A (company overview not available)",
            "revenue_streams": [],
            "revenue_streams_source": "Source: N/A (company overview not available)",
            "asof_fy_end": None,
        }
    def _bank_metrics_enabled() -> bool:
        if bool(getattr(company_profile, "has_bank", False)):
            return True
        if hist is None or hist.empty:
            return False
        bank_metrics = ["bank_deposits", "bank_finance_receivables", "bank_net_funding"]
        h_chk = hist.copy()
        if "quarter" in h_chk.columns:
            h_chk["quarter"] = pd.to_datetime(h_chk["quarter"], errors="coerce")
            h_chk = h_chk[h_chk["quarter"].notna()].sort_values("quarter").tail(8)
        qualifying = 0
        for metric in bank_metrics:
            if metric not in h_chk.columns:
                continue
            vals = pd.to_numeric(h_chk[metric], errors="coerce")
            if int(vals.notna().sum()) >= 4:
                qualifying += 1
        return qualifying >= 2

    bank_metrics_enabled = _bank_metrics_enabled()

    def _build_numeric_quarter_map(df_in: Optional[pd.DataFrame], col_name: str) -> Dict[date, float]:
        if df_in is None or getattr(df_in, "empty", True) or col_name not in df_in.columns:
            return {}
        try:
            df_local = df_in.copy()
            df_local["quarter"] = pd.to_datetime(df_local.get("quarter"), errors="coerce").dt.date
            df_local[col_name] = pd.to_numeric(df_local.get(col_name), errors="coerce")
            df_local = df_local[df_local["quarter"].notna() & df_local[col_name].notna()]
            return {q: float(v) for q, v in zip(df_local["quarter"], df_local[col_name])}
        except Exception:
            return {}

    _pbi_hist_buybacks_cash_map = _build_numeric_quarter_map(hist, "buybacks_cash") if is_pbi_profile else {}
    _pbi_hist_debt_repayment_map = _build_numeric_quarter_map(hist, "debt_repayment") if is_pbi_profile else {}
    _pbi_adj_fcf_map = _build_numeric_quarter_map(adj_metrics, "adj_fcf") if is_pbi_profile else {}
    _pbi_revolver_availability_map = _build_numeric_quarter_map(revolver_history, "revolver_availability") if is_pbi_profile else {}

    def _prev_same_quarter_year(qd: Optional[date], value_map: Dict[date, float]) -> Optional[float]:
        if not qd or not value_map:
            return None
        try:
            candidate = qd.replace(year=qd.year - 1)
        except Exception:
            return None
        return value_map.get(candidate)

    def _prev_available_quarter(qd: Optional[date], value_map: Dict[date, float]) -> Optional[Tuple[date, float]]:
        if not qd or not value_map:
            return None
        prior_keys = [qq for qq in value_map.keys() if isinstance(qq, date) and qq < qd]
        if not prior_keys:
            return None
        best_q = max(prior_keys)
        return best_q, float(value_map.get(best_q))

    def _safe_cell(val: Any) -> Any:
        if isinstance(val, str):
            val = ILLEGAL_CHARACTERS_RE.sub("", val)
            if val.startswith("="):
                return "'" + val
        return val

    def _safe_cell_or_none(val: Any) -> Any:
        """Normalize pandas missing values and Excel-unsafe strings before writing.

        The workbook writer pushes many large diagnostic/raw-data frames through
        openpyxl. Keeping this tiny conversion centralized avoids slower
        DataFrame.iterrows access patterns while preserving the existing cell
        safety contract for formulas-as-text and illegal XML characters.
        """
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        return _safe_cell(val)

    def _excel_safe_text_local(text_in: Any, *, max_len: Optional[int] = None) -> str:
        txt = str(text_in or "")
        if not txt:
            return ""
        txt = txt.replace("\r\n", "\n").replace("\r", "\n")
        txt = ILLEGAL_CHARACTERS_RE.sub("", txt)
        if max_len is not None:
            txt = txt[:max_len]
        return txt

    def _excel_safe_comment_text_local(text_in: Any, *, max_len: int = 32000) -> str:
        txt = _excel_safe_text_local(text_in)
        if not txt:
            return ""
        txt = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", txt)
        txt = txt.replace("\r\n", "\n").replace("\r", "\n")
        if max_len is not None:
            txt = txt[:max_len]
        return txt

    def _comment_local(text_in: Any, author: str = "pipeline") -> Optional[Comment]:
        txt = _excel_safe_comment_text_local(text_in)
        if not txt:
            return None
        return Comment(txt, author)

    def _set_cell_comment_local(cell: Any, text_in: Any, author: str = "pipeline") -> None:
        comment = _comment_local(text_in, author=author)
        if comment is None:
            return
        cell.comment = comment

    def _updated_font(existing_font: Any, **changes: Any) -> Any:
        font_obj = copy(existing_font)
        for attr, value in changes.items():
            setattr(font_obj, attr, value)
        return font_obj

    def _apply_hyperlink_look(cell, target: str) -> None:
        if not target:
            return
        cell.hyperlink = target
        cell.font = _updated_font(cell.font, color="0563C1", underline="single")

    # Backward-compatible alias for older writer helpers.
    def _apply_hyperlink(cell, target: str) -> None:
        _apply_hyperlink_look(cell, target)

    def _normalize_accn_local(val: Any) -> str:
        return re.sub(r"[^0-9]", "", str(val or ""))

    def _sec_cache_roots_local() -> List[Path]:
        roots: List[Path] = []
        seen: set[str] = set()

        def _add_root(p: Path) -> None:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen or not p.exists():
                return
            seen.add(rp)
            roots.append(p)

        repo_root = Path(__file__).resolve().parents[2]
        for cand in ticker_cache_candidates(repo_root, str(ticker or "").strip(), Path(cache_dir) if cache_dir is not None else None):
            _add_root(cand)
        return roots

    def _sec_cache_doc_paths_local(root: Path) -> List[Path]:
        cache_key = str(root.resolve()) if root.exists() else str(root)
        cached = document_cache.sec_cache_doc_paths_by_root.get(cache_key)
        if cached is not None:
            return list(cached)
        doc_paths: List[Path] = []
        if root.exists():
            try:
                for path_in in root.rglob("doc_*"):
                    if not path_in.is_file():
                        continue
                    if path_in.suffix.lower() not in {".htm", ".html", ".txt"}:
                        continue
                    doc_paths.append(path_in)
            except Exception:
                doc_paths = []
        doc_paths = sorted(doc_paths, key=lambda z: z.stat().st_mtime if z.exists() else 0, reverse=True)
        document_cache.sec_cache_doc_paths_by_root[cache_key] = list(doc_paths)
        return list(doc_paths)

    def _sec_cache_html_paths_local(root: Path) -> List[Path]:
        cache_key = str(root.resolve()) if root.exists() else str(root)
        cached = document_cache.sec_cache_html_paths_by_root.get(cache_key)
        if cached is not None:
            return list(cached)
        html_paths: List[Path] = []
        if root.exists():
            try:
                html_paths = sorted(
                    (
                        path_in
                        for path_in in root.glob("*.htm")
                        if path_in.is_file()
                    ),
                    key=lambda z: z.stat().st_mtime if z.exists() else 0,
                    reverse=True,
                )
            except Exception:
                html_paths = []
        document_cache.sec_cache_html_paths_by_root[cache_key] = list(html_paths)
        return list(html_paths)

    def _sec_cache_docs_for_token_local(root: Path, token: str) -> List[Path]:
        token_txt = str(token or "").strip()
        if not token_txt:
            return []
        cache_key = str(root.resolve()) if root.exists() else str(root)
        token_index = document_cache.sec_cache_doc_paths_by_token_by_root.get(cache_key)
        if token_index is None:
            token_index = {}
            for path_in in _sec_cache_doc_paths_local(root):
                for token_hit in set(re.findall(r"20\d{6}", path_in.name)):
                    token_index.setdefault(token_hit, []).append(path_in)
            document_cache.sec_cache_doc_paths_by_token_by_root[cache_key] = token_index
        return list(token_index.get(token_txt) or [])

    def _sec_cache_html_paths_for_token_local(root: Path, token: str) -> List[Path]:
        token_txt = str(token or "").strip()
        if not token_txt:
            return []
        cache_key = str(root.resolve()) if root.exists() else str(root)
        token_index = document_cache.sec_cache_html_paths_by_token_by_root.get(cache_key)
        if token_index is None:
            token_index = {}
            for path_in in _sec_cache_html_paths_local(root):
                for token_hit in set(re.findall(r"20\d{6}", path_in.name)):
                    token_index.setdefault(token_hit, []).append(path_in)
            document_cache.sec_cache_html_paths_by_token_by_root[cache_key] = token_index
        return list(token_index.get(token_txt) or [])

    def _infer_doc_quarter_local(path_in: Any, raw_text: Any = "") -> Optional[date]:
        try:
            p = Path(path_in)
        except Exception:
            return None
        try:
            path_key = str(p.resolve())
        except Exception:
            path_key = str(p)
        if path_key in document_cache.inferred_quarter_by_path:
            return document_cache.inferred_quarter_by_path.get(path_key)
        text_in = str(raw_text or "")
        qd = _parse_quarter_from_filename(p.name)
        if not isinstance(qd, date):
            if not text_in:
                try:
                    text_in = _read_cached_doc_text(p)
                except Exception:
                    text_in = ""
            qd = _parse_quarter_from_follow_text(text_in) or infer_quarter_end_from_text(text_in)
        if not isinstance(qd, date):
            annual_letter_match = re.search(
                r"(?:^|[_-])(20\d{2})(?:[^0-9]{0,24})?(?:annualletter|shareholderletter|shareholder.?letter)\b",
                p.name,
                re.I,
            )
            if annual_letter_match:
                try:
                    qd = date(int(annual_letter_match.group(1)), 12, 31)
                except Exception:
                    qd = None
        qd_out = qd if isinstance(qd, date) else None
        document_cache.inferred_quarter_by_path[path_key] = qd_out
        return qd_out

    debt_convertible_support: Optional[DebtConvertibleEnrichmentSupport] = None

    def _htmlish_to_text(txt_in: str) -> str:
        txt = html.unescape(str(txt_in or ""))
        txt = re.sub(r"<br\s*/?>", "\n", txt, flags=re.I)
        txt = re.sub(r"</?(?:div|p|tr|li|td|th|table|span|font|b|strong|em|u)[^>]*>", " ", txt, flags=re.I)
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = txt.replace("\xa0", " ")
        return glx_normalize_text(txt)

    def _safe_text_value(v: Any) -> str:
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        return str(v or "").strip()

    def _get_debt_convertible_enrichment_support() -> DebtConvertibleEnrichmentSupport:
        nonlocal debt_convertible_support
        if debt_convertible_support is None:
            debt_convertible_support = DebtConvertibleEnrichmentSupport(
                DebtConvertibleEnrichmentDeps(
                    ticker=ticker,
                    cache_dir=cache_dir,
                    ticker_roots=tuple(ticker_roots),
                    document_cache=document_cache,
                    context_helpers={
                        "_normalize_accn_local": _normalize_accn_local,
                        "_sec_cache_roots_local": _sec_cache_roots_local,
                        "_sec_cache_doc_paths_local": _sec_cache_doc_paths_local,
                        "_infer_doc_quarter_local": _infer_doc_quarter_local,
                        "_path_belongs_to_ticker": _path_belongs_to_ticker,
                        "coerce_number": coerce_number,
                        "glx_normalize_text": glx_normalize_text,
                    },
                )
            )
        return debt_convertible_support

    def _enrich_latest_debt_convertibles(df_in: pd.DataFrame) -> pd.DataFrame:
        return _get_debt_convertible_enrichment_support().enrich_latest_debt_convertibles(df_in)

    def _build_operating_driver_rows() -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().build_operating_driver_rows()

    _bridge_fy_adj_ebitda_cache: Optional[List[Dict[str, Any]]] = None

    def _load_bridge_fy_adjusted_ebitda_records() -> List[Dict[str, Any]]:
        nonlocal _bridge_fy_adj_ebitda_cache
        if _bridge_fy_adj_ebitda_cache is not None:
            return list(_bridge_fy_adj_ebitda_cache)
        records: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[int, float, str]] = set()
        table_re = re.compile(
            r"Year Ended December 31,\s*(20\d{2})\s+(20\d{2})(?:\s+(20\d{2}))?.{0,1600}?"
            r"Adjusted EBITDA\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?)\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?)"
            r"(?:\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?))?",
            re.I | re.S,
        )
        narrative_re = re.compile(
            r"\b(?:full year|fiscal year|year ended december 31,?)\s*(20\d{2})\b[^.]{0,180}?"
            r"adjusted ebitda(?:\s+of)?\s+\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(million|m)\b",
            re.I,
        )

        def _parse_signed_amount(token: str) -> Optional[float]:
            tok = str(token or "").strip()
            if not tok:
                return None
            sign = -1.0 if tok.startswith("(") and tok.endswith(")") else 1.0
            try:
                return sign * float(tok.strip("()").replace(",", ""))
            except Exception:
                return None

        source_files: List[Tuple[str, Path]] = []
        for path_in in _operating_driver_financial_statement_files():
            source_files.append(("financial_statement", path_in))
        for source_type, src_dir in _operating_driver_follow_source_dirs():
            if source_type != "earnings_release":
                continue
            try:
                src_files.extend((source_type, p) for p in sorted([x for x in src_dir.iterdir() if x.is_file()]))
            except Exception:
                continue
        for source_type, path_in in source_files:
            raw_txt = _read_operating_driver_text(path_in)
            txt = glx_normalize_text(raw_txt)
            if not txt or "adjusted ebitda" not in txt.lower():
                continue
            for match in table_re.finditer(txt):
                years = [x for x in match.groups()[:3] if x]
                values = [x for x in match.groups()[3:] if x]
                if not years or not values:
                    continue
                snippet = qn_compact_snippet(txt[max(0, match.start() - 60) : min(len(txt), match.end() + 120)], 260)
                for yy, vv in zip(years, values):
                    amt = _parse_signed_amount(vv)
                    if amt is None:
                        continue
                    value_m = float(amt) / 1000.0
                    key = (int(yy), round(value_m, 3), str(path_in))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    records.append(
                        {
                            "fiscal_year": int(yy),
                            "value_m": value_m,
                            "source_type": source_type,
                            "source_doc": str(path_in),
                            "quality": "exact",
                            "snippet": snippet,
                        }
                    )
            for match in narrative_re.finditer(txt):
                year_txt, amt_txt, _ = match.groups()
                try:
                    value_m = float(str(amt_txt).replace(",", ""))
                except Exception:
                    continue
                key = (int(year_txt), round(value_m, 3), str(path_in))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                records.append(
                    {
                        "fiscal_year": int(year_txt),
                        "value_m": float(value_m),
                        "source_type": source_type,
                        "source_doc": str(path_in),
                        "quality": "text-derived",
                        "snippet": qn_compact_snippet(
                            txt[max(0, match.start() - 60) : min(len(txt), match.end() + 120)],
                            260,
                        ),
                    }
                )
        records.sort(
            key=lambda rec: (
                int(rec.get("fiscal_year") or 0),
                int(_source_rank(rec.get("source_type"), rec.get("source_doc"))),
                0 if str(rec.get("quality") or "") == "exact" else 1,
                -abs(float(rec.get("value_m") or 0.0)),
            )
        )
        _bridge_fy_adj_ebitda_cache = records
        return list(records)

    def _resolve_thesis_fy_base() -> Dict[str, Any]:
        latest_q = pd.NaT
        if hist is not None and not hist.empty and "quarter" in hist.columns:
            latest_q = pd.to_datetime(hist["quarter"], errors="coerce").dropna().max()
        if pd.isna(latest_q) and adj_metrics is not None and not adj_metrics.empty and "quarter" in adj_metrics.columns:
            latest_q = pd.to_datetime(adj_metrics["quarter"], errors="coerce").dropna().max()
        latest_fy_year: Optional[int] = None
        if pd.notna(latest_q):
            latest_q_ts = pd.Timestamp(latest_q)
            latest_fy_year = latest_q_ts.year if latest_q_ts.month == 12 else latest_q_ts.year - 1
        label_core = f"Base Adj EBITDA FY{latest_fy_year}" if latest_fy_year else "Base Adj EBITDA FY"
        if (
            adj_metrics is not None
            and not adj_metrics.empty
            and "quarter" in adj_metrics.columns
            and "adj_ebitda" in adj_metrics.columns
        ):
            adj_local = _adj_metrics_view().copy()
            if "_quarter" in adj_local.columns:
                adj_local["quarter"] = adj_local["_quarter"]
            adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")
            adj_local["adj_ebitda"] = pd.to_numeric(adj_local["adj_ebitda"], errors="coerce")
            adj_clean = adj_local.dropna(subset=["adj_ebitda"]).copy()
            recent = adj_clean.groupby(adj_clean["quarter"].dt.to_period("Q"), as_index=False).last().tail(4)
            if len(recent) == 4:
                raw_sum = float(recent["adj_ebitda"].sum())
                value_m = raw_sum / 1_000_000.0 if abs(raw_sum) > 10_000.0 else raw_sum
                latest_ttm_q = pd.to_datetime(recent["quarter"], errors="coerce").dropna().max()
                latest_ttm_label = (
                    f" through {pd.Timestamp(latest_ttm_q).date()}" if pd.notna(latest_ttm_q) else ""
                )
                return {
                    "label": "Base Adj EBITDA TTM (latest)",
                    "value_m": float(value_m),
                    "fallback": "latest TTM",
                    "source_type": "adj_metrics",
                    "source_doc": "",
                    "quality": "modeled",
                    "snippet": f"Latest four quarterly adjusted EBITDA observations{latest_ttm_label}.",
                }
        annual_records = _load_bridge_fy_adjusted_ebitda_records()
        fy_records = [rec for rec in annual_records if latest_fy_year is not None and int(rec.get("fiscal_year") or 0) == latest_fy_year]
        if fy_records:
            best = sorted(
                fy_records,
                key=lambda rec: (
                    int(_source_rank(rec.get("source_type"), rec.get("source_doc"))),
                    0 if str(rec.get("quality") or "") == "exact" else 1,
                ),
            )[0]
            return {
                "label": label_core,
                "value_m": float(best.get("value_m") or 0.0),
                "fallback": "",
                "source_type": str(best.get("source_type") or ""),
                "source_doc": str(best.get("source_doc") or ""),
                "quality": str(best.get("quality") or "exact"),
                "snippet": str(best.get("snippet") or ""),
            }

        if adj_metrics is not None and not adj_metrics.empty and latest_fy_year is not None and "adj_ebitda" in adj_metrics.columns:
            adj_local = _adj_metrics_view().copy()
            if "_quarter" in adj_local.columns:
                adj_local["quarter"] = adj_local["_quarter"]
            adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")
            adj_local["adj_ebitda"] = pd.to_numeric(adj_local["adj_ebitda"], errors="coerce")
            same_fy = adj_local[adj_local["quarter"].dt.year == latest_fy_year].dropna(subset=["adj_ebitda"])
            if same_fy["quarter"].dt.to_period("Q").nunique() >= 4:
                last_four = same_fy.groupby(same_fy["quarter"].dt.to_period("Q"), as_index=False)["adj_ebitda"].last()
                if len(last_four) >= 4:
                    return {
                        "label": f"{label_core} (fallback: summed quarters)",
                        "value_m": float(last_four["adj_ebitda"].tail(4).sum()),
                        "fallback": "summed quarters",
                        "source_type": "modeled",
                        "source_doc": "",
                        "quality": "modeled",
                        "snippet": "Summed four quarterly adjusted EBITDA observations for the latest completed fiscal year.",
                    }
            recent = adj_local.dropna(subset=["adj_ebitda"]).tail(4)
            if len(recent) == 4:
                return {
                    "label": f"{label_core} (fallback: TTM)",
                    "value_m": float(recent["adj_ebitda"].sum()),
                    "fallback": "TTM",
                    "source_type": "modeled",
                    "source_doc": "",
                    "quality": "modeled",
                    "snippet": "Summed latest four quarterly adjusted EBITDA observations as a TTM fallback.",
                }

        return {
            "label": f"{label_core} (fallback: unavailable)",
            "value_m": 0.0,
            "fallback": "unavailable",
            "source_type": "",
            "source_doc": "",
            "quality": "inferred",
            "snippet": "",
        }

    debt_tranches_latest = _enrich_latest_debt_convertibles(debt_tranches_latest)

    def _write_sheet(name: str, df: pd.DataFrame) -> None:
        ws = wb.create_sheet(name)
        if str(name or "") == "History_Q" and isinstance(df, pd.DataFrame) and not df.empty:
            df = _augment_history_q_frame_for_writer(df, ticker=ticker, fiscal_profile=company_profile)
        if df is None or df.empty:
            write_empty_sheet_placeholder(ws)
            return
        header_vals = []
        for c in df.columns:
            if isinstance(c, (pd.Timestamp, datetime, date)):
                header_vals.append(pd.to_datetime(c).date())
            else:
                header_vals.append(c)
        ws.append(header_vals)
        # itertuples avoids allocating a Series for every row. This matters for
        # DATA_Facts_Long and other large raw-data sheets, where row-by-row
        # pandas indexing can dominate workbook write time.
        for row_values in df.itertuples(index=False, name=None):
            ws.append([_safe_cell_or_none(value) for value in row_values])
        ws.freeze_panes = "A2"
        for c in ws[1]:
            c.font = Font(bold=True, size=header_size)
            c.alignment = Alignment(vertical="center")
        _autowidth(ws, len(df.columns))
        ws.sheet_format.defaultRowHeight = 18
        ws.sheet_view.zoomScale = 110
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                size = header_size if cell.row == 1 else font_size
                cell.font = _updated_font(cell.font, size=size, bold=cell.font.b)
        try:
            # Skip table if headers are not plain strings (Excel tables require string headers)
            headers = [c.value for c in ws[1]]
            if any(h is None or isinstance(h, (datetime, date)) or not isinstance(h, str) for h in headers):
                raise ValueError("Non-string headers; skip table")
            if len(headers) != len(set(headers)):
                raise ValueError("Duplicate headers; skip table")
            ref = f"A1:{get_column_letter(len(df.columns))}{ws.max_row}"
            t = Table(displayName=name.replace(" ", "").replace("-", ""), ref=ref)
            t.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
            ws.add_table(t)
        except Exception:
            pass

    def _derivative_oci_bridge_render_runtime() -> Dict[str, Any]:
        return {
            "wb": wb,
            "_write_sheet": _write_sheet,
            "_safe_cell": _safe_cell,
            "_get_analysis_sheet_style_bundle": _get_analysis_sheet_style_bundle,
            "font_size": font_size,
            "header_size": header_size,
            "ctx_ref": ctx_ref,
            "operating_driver_history_rows": operating_driver_history_rows,
        }

    def _write_derivative_oci_bridge_sheet(
        bridge_df: pd.DataFrame,
        exposure_df: Optional[pd.DataFrame] = None,
    ) -> None:
        write_derivative_oci_bridge_sheet(
            DerivativeOciBridgeRenderDeps(runtime=_derivative_oci_bridge_render_runtime()),
            bridge_df,
            exposure_df,
        )

    def _write_derivative_crush_tests_sheet(tables: Dict[str, pd.DataFrame]) -> None:
        write_derivative_crush_tests_sheet(
            DerivativeOciBridgeRenderDeps(runtime=_derivative_oci_bridge_render_runtime()),
            tables,
        )

    def _write_flags_sheet(name: str, df: pd.DataFrame) -> None:
        if str(name) == "Hidden_Value_Flags":
            write_hidden_value_flags_sheet(
                HiddenValueFlagsSheetInputs(
                    wb=wb,
                    sheet_name=str(name),
                    flags_df=df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
                    font_size=font_size,
                    header_size=header_size,
                    safe_cell=_safe_cell,
                )
            )
            return
        ws = wb.create_sheet(name)
        if df is None or df.empty:
            ws["A1"] = "No signals."
            return
        headers = list(df.columns)
        ws.append(headers)
        for _, r in df.iterrows():
            ws.append([None if pd.isna(r[c]) else _safe_cell(r[c]) for c in headers])
        ws.freeze_panes = "A2"
        for c in ws[1]:
            c.font = Font(bold=True, size=header_size)
            c.alignment = Alignment(vertical="center")
        ws.sheet_format.defaultRowHeight = 18
        ws.sheet_view.zoomScale = 110
        _autowidth(ws, len(headers))
        rng = None
        # widen evidence columns and wrap to keep sheet readable
        col_map = {h: i + 1 for i, h in enumerate(headers)}
        for col_name in ["evidence_1", "evidence_2", "evidence_3"]:
            idx = col_map.get(col_name)
            if not idx:
                continue
            letter = get_column_letter(idx)
            ws.column_dimensions[letter].width = max(34, min(38, ws.column_dimensions[letter].width or 34))
            for rr in range(2, ws.max_row + 1):
                ws[f"{letter}{rr}"].alignment = Alignment(wrap_text=True, vertical="top")
        # score heatmap
        score_idx = col_map.get("score")
        if score_idx:
            letter = get_column_letter(score_idx)
            rng = f"{letter}2:{letter}{ws.max_row}"
            ws.conditional_formatting.add(
                rng,
                CellIsRule(
                    operator="greaterThanOrEqual",
                    formula=["70"],
                    fill=PatternFill("solid", fgColor="C6EFCE"),
                ),
            )
        try:
            if len(headers) == len(set(headers)) and all(isinstance(h, str) for h in headers):
                ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"
                t = Table(displayName=name.replace(" ", "").replace("-", ""), ref=ref)
                t.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
                ws.add_table(t)
        except Exception:
            pass

    def _autowidth(ws, n_cols: int) -> None:
        for i in range(1, n_cols + 1):
            width = max(14, ws.column_dimensions[get_column_letter(i)].width or 14)
            if i == 1:
                width = max(width, 26)
            ws.column_dimensions[get_column_letter(i)].width = width

    def _norm_header_key(name: Any) -> str:
        s = str(name or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    def _resolve_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
        if df is None or df.empty:
            return None
        norm_map: Dict[str, str] = {}
        for c in df.columns:
            k = _norm_header_key(c)
            if k and k not in norm_map:
                norm_map[k] = c
        for alias in aliases:
            k = _norm_header_key(alias)
            if k in norm_map:
                return norm_map[k]
        return None

    def _cached_source_view(
        source_name: str,
        df_in: Any,
        *,
        quarter_aliases: List[str],
        quarter_mode: str = "timestamp",
    ) -> pd.DataFrame:
        cache_key = (source_name, quarter_mode)
        cached = frame_view_cache.get(cache_key)
        if cached is not None:
            return cached
        df = df_in.copy() if isinstance(df_in, pd.DataFrame) else pd.DataFrame()
        q_col = _resolve_col(df, quarter_aliases)
        if q_col is not None:
            q_series = pd.to_datetime(df[q_col], errors="coerce")
            if quarter_mode == "date":
                df["_quarter"] = q_series.dt.date
            elif quarter_mode == "quarter_end_date":
                df["_quarter"] = q_series.dt.to_period("Q").dt.end_time.dt.date
            else:
                df["_quarter"] = q_series
        frame_view_cache[cache_key] = df
        return df

    def _hist_view(*, quarter_mode: str = "timestamp") -> pd.DataFrame:
        return _cached_source_view("hist", hist, quarter_aliases=["quarter"], quarter_mode=quarter_mode)

    def _adj_metrics_view(*, quarter_mode: str = "timestamp") -> pd.DataFrame:
        return _cached_source_view("adj_metrics", adj_metrics, quarter_aliases=["quarter"], quarter_mode=quarter_mode)

    def _quarter_notes_view(*, quarter_mode: str = "timestamp") -> pd.DataFrame:
        return _cached_source_view(
            "quarter_notes",
            quarter_notes,
            quarter_aliases=["quarter", "quarter_end", "as_of_quarter"],
            quarter_mode=quarter_mode,
        )

    def _promises_view(*, quarter_mode: str = "timestamp") -> pd.DataFrame:
        return _cached_source_view(
            "promises",
            promises,
            quarter_aliases=["last_seen_quarter", "quarter", "created_quarter", "first_seen_quarter"],
            quarter_mode=quarter_mode,
        )

    def _audit_view(*, quarter_mode: str = "timestamp") -> pd.DataFrame:
        return _cached_source_view(
            "audit",
            audit,
            quarter_aliases=["quarter", "quarter_end", "period_end"],
            quarter_mode=quarter_mode,
        )

    def _qend_date(x: Any) -> Optional[date]:
        t = pd.to_datetime(x, errors="coerce")
        if pd.isna(t):
            return None
        return pd.Timestamp(t).to_period("Q").end_time.date()

    def _severity_label_weight(v: Any) -> Tuple[str, int]:
        s = str(v or "").strip().lower()
        if "fail" in s:
            return "FAIL", 3
        if "warn" in s:
            return "WARN", 2
        return "INFO", 1

    def _parse_first_evidence(row: pd.Series) -> Dict[str, Any]:
        for key in ["evidence_json", "source_evidence_json", "evidence"]:
            raw = row.get(key)
            if isinstance(raw, str) and raw.strip():
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list) and parsed:
                        first = parsed[0]
                        if isinstance(first, dict):
                            return first
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    continue
        return {}

    def _write_quarter_notes_ui(top_k: int = 5) -> List[Dict[str, Any]]:
        ws = wb.create_sheet("Quarter_Notes_UI")
        qa_rows: List[Dict[str, Any]] = []
        ts = datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        ws["A1"] = f"Generated at {ts} | Category #rank"
        ws["A1"].font = Font(bold=True, size=header_size)

        if quarter_notes is None or quarter_notes.empty:
            ws["A2"] = "No data."
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 30
            return qa_rows

        df = _quarter_notes_view()
        q_col = _resolve_col(df, ["quarter", "quarter_end", "as_of_quarter"])
        cat_col = _resolve_col(df, ["category", "tag", "topic"])
        claim_col = _resolve_col(df, ["claim", "headline", "note", "body", "statement"])
        sev_col = _resolve_col(df, ["severity", "qa_severity", "status"])
        score_col = _resolve_col(df, ["severity_score", "score"])
        metric_col = _resolve_col(df, ["metric_ref", "metric", "metric_tag"])
        metric_val_col = _resolve_col(df, ["metric_value", "value", "extracted_value"])
        note_id_col = _resolve_col(df, ["note_id", "id"])
        ev_doc_col = _resolve_col(df, ["evidence_doc", "doc_path", "doc"])
        ev_loc_col = _resolve_col(df, ["evidence_loc", "section_or_page", "page", "section"])
        ev_snip_col = _resolve_col(df, ["evidence_snippet", "snippet"])

        if q_col is None or cat_col is None or claim_col is None:
            ws["A2"] = "Missing required source columns."
            qa_rows.append(
                {
                    "quarter": None,
                    "metric": "Quarter_Notes_UI",
                    "check": "quarter_notes_ui_source_columns",
                    "status": "fail",
                    "message": "Quarter_Notes missing required columns for UI matrix.",
                }
            )
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 30
            return qa_rows

        time_anchor_re = re.compile(
            r"\b(by\s+20\d{2}|next quarter|this quarter|this year|next year|during\s+20\d{2}|in\s+20\d{2}|q[1-4]\s*20\d{2}|fy\s*20\d{2})\b",
            re.I,
        )
        records: List[Dict[str, Any]] = []

        def _extract_numeric_hint(text: str) -> str:
            if not text:
                return ""
            pat = re.compile(
                r"[$]?\s*\(?[-+]?\d[\d,]*(?:\.\d+)?\)?\s*(?:%|bps|x|m|mm|bn|b)?",
                re.I,
            )
            for m in pat.finditer(str(text)):
                tok = str(m.group(0) or "").strip()
                if not tok:
                    continue
                tok = tok.strip(",.;:")
                core = re.sub(r"[^0-9.]", "", tok)
                if not core:
                    continue
                try:
                    if re.fullmatch(r"\d{4}", core):
                        yr = int(core)
                        if 1900 <= yr <= 2100:
                            continue
                except Exception:
                    pass
                return tok
            return ""

        def _fmt_note_metric(metric_name: str, raw_val: Any) -> str:
            v = pd.to_numeric(raw_val, errors="coerce")
            if pd.isna(v):
                return ""
            x = float(v)
            m = str(metric_name or "").lower()
            if abs(x) >= 1_000_000:
                return f"${x / 1e6:,.1f}m"
            if "bps" in m:
                return f"{x:+.0f} bps"
            if "share" in m and abs(x) < 50:
                return f"${x:,.2f}/sh"
            if ("yoy" in m or "margin" in m or "growth" in m or "yield" in m) and abs(x) <= 5:
                return f"{x * 100:+.1f}%"
            if abs(x) < 1000:
                return f"{x:,.2f}"
            return f"{x:,.0f}"

        for _, row in df.iterrows():
            q = pd.to_datetime(row.get(q_col), errors="coerce")
            if pd.isna(q):
                continue
            qd = pd.Timestamp(q).date()
            category = str(row.get(cat_col) or "Uncategorized").strip() or "Uncategorized"
            claim = str(row.get(claim_col) or "").strip()
            sev_label, sev_weight = _severity_label_weight(row.get(sev_col) if sev_col else "INFO")
            score_val = pd.to_numeric(row.get(score_col), errors="coerce") if score_col else pd.NA
            score = float(score_val) if pd.notna(score_val) else 0.0
            metric = str(row.get(metric_col) or "").strip() if metric_col else ""
            ev = _parse_first_evidence(row)
            metric_value = row.get(metric_val_col) if metric_val_col else None
            if metric_value is None and isinstance(ev, dict):
                metric_value = ev.get("extracted_value")
            evidence_doc = str(row.get(ev_doc_col) or ev.get("doc_path") or ev.get("doc_name") or "").strip() if ev_doc_col else str(ev.get("doc_path") or ev.get("doc_name") or "").strip()
            evidence_loc = str(row.get(ev_loc_col) or ev.get("section_or_page") or ev.get("page") or "").strip() if ev_loc_col else str(ev.get("section_or_page") or ev.get("page") or "").strip()
            evidence_snippet = str(row.get(ev_snip_col) or ev.get("snippet") or claim).strip() if ev_snip_col else str(ev.get("snippet") or claim).strip()

            note_id = str(row.get(note_id_col) or "").strip() if note_id_col else ""
            generated_id = False
            if not note_id:
                generated_id = True
                note_id = hashlib.sha1(f"{qd.isoformat()}|{category}|{claim}".encode("utf-8")).hexdigest()[:12]

            if not claim:
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_missing_claim",
                        "status": "fail",
                        "message": f"UI note missing claim (note_id={note_id}).",
                    }
                )
                continue
            if generated_id:
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_note_id_generated",
                        "status": "fail",
                        "message": f"UI note had no source note_id; generated stable id {note_id}.",
                    }
                )

            if sev_label in {"FAIL", "WARN"} and (not evidence_doc or not evidence_snippet):
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_missing_evidence",
                        "status": "warn",
                        "message": f"UI note {note_id} ({sev_label}) missing evidence doc/snippet.",
                    }
                )
            if not bool(time_anchor_re.search(f"{claim} {evidence_snippet}")):
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_time_anchor",
                        "status": "warn",
                        "message": f"UI note {note_id} has no explicit time anchor.",
                    }
                )

            records.append(
                {
                    "quarter": qd,
                    "category": category,
                    "claim": claim,
                    "severity": sev_label,
                    "severity_weight": sev_weight,
                    "score": score,
                    "metric": metric,
                    "metric_value": metric_value,
                    "note_id": note_id,
                    "evidence_doc": evidence_doc,
                    "evidence_loc": evidence_loc,
                    "evidence_snippet": evidence_snippet,
                }
            )

        if not records:
            ws["A2"] = "No notes after filtering."
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 30
            return qa_rows

        rec_df = pd.DataFrame(records)
        rec_df["quarter"] = pd.to_datetime(rec_df["quarter"], errors="coerce").dt.date
        quarters = sorted(rec_df["quarter"].dropna().unique().tolist(), reverse=True)
        categories = sorted(rec_df["category"].dropna().astype(str).unique().tolist())
        for i, qd in enumerate(quarters, start=2):
            c = ws.cell(row=1, column=i, value=str(qd))
            c.font = Font(bold=True, size=header_size)
            c.alignment = Alignment(horizontal="center", vertical="center")

        rec_df = rec_df.sort_values(["category", "quarter", "score", "severity_weight"], ascending=[True, False, False, False]).reset_index(drop=True)
        grouped: Dict[Tuple[str, date], List[Dict[str, Any]]] = {}
        for _, r in rec_df.iterrows():
            qk = r.get("quarter")
            if pd.isna(qk):
                continue
            grouped.setdefault((str(r["category"]), qk), []).append(r.to_dict())

        row_idx = 2
        note_link_cells: List[Tuple[str, str]] = []
        used_notes: Dict[str, Dict[str, Any]] = {}
        for cat in categories:
            cat_max = 0
            for qd in quarters:
                cat_max = max(cat_max, len(grouped.get((cat, qd), [])))
            rank_max = max(1, min(top_k, cat_max))
            for rank in range(1, rank_max + 1):
                ws.cell(row=row_idx, column=1, value=f"{cat} #{rank}")
                ws.cell(row=row_idx, column=1).alignment = Alignment(vertical="top")
                for i, qd in enumerate(quarters, start=2):
                    notes = grouped.get((cat, qd), [])
                    if len(notes) < rank:
                        continue
                    n = notes[rank - 1]
                    metric_txt = _fmt_note_metric(str(n.get("metric") or ""), n.get("metric_value"))
                    if not metric_txt:
                        metric_txt = _extract_numeric_hint(
                            f"{n.get('claim') or ''} {n.get('evidence_snippet') or ''}"
                        )
                    txt = str(n["claim"])
                    if metric_txt:
                        txt += f" ({metric_txt})"
                    txt = txt[:220]
                    cell = ws.cell(row=row_idx, column=i, value=txt)
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
                    note_link_cells.append((cell.coordinate, str(n["note_id"])))
                    used_notes[str(n["note_id"])] = n
                row_idx += 1
            # compact layout: no extra spacer row between categories

        last_col = get_column_letter(max(2, 1 + len(quarters)))
        last_row = max(2, row_idx - 1)
        ws.freeze_panes = "B2"
        ws.column_dimensions["A"].width = 30
        for cidx in range(2, 2 + len(quarters)):
            ws.column_dimensions[get_column_letter(cidx)].width = 72
        for rr in range(2, last_row + 1):
            ws.row_dimensions[rr].height = 56

        if last_row >= 2 and len(quarters) > 0:
            rng = f"B2:{last_col}{last_row}"
            ws.conditional_formatting.add(
                rng,
                FormulaRule(formula=["ISNUMBER(SEARCH(\"[FAIL]\",B2))"], fill=PatternFill("solid", fgColor="FFC7CE")),
            )
            ws.conditional_formatting.add(
                rng,
                FormulaRule(
                    formula=["AND(ISNUMBER(SEARCH(\"[WARN]\",B2)),ISERROR(SEARCH(\"[FAIL]\",B2)))"],
                    fill=PatternFill("solid", fgColor="FFEB9C"),
                ),
            )

        evidence_rows = []
        for note_id, n in used_notes.items():
            evidence_rows.append(
                {
                    "note_id": note_id,
                    "quarter": n.get("quarter"),
                    "category": n.get("category"),
                    "claim": n.get("claim"),
                    "metric": n.get("metric"),
                    "doc_path": n.get("evidence_doc"),
                    "evidence_loc": n.get("evidence_loc"),
                    "snippet": n.get("evidence_snippet"),
                }
            )
        evidence_df = pd.DataFrame(evidence_rows).sort_values(["quarter", "category", "note_id"]).reset_index(drop=True) if evidence_rows else pd.DataFrame()
        _write_sheet("Quarter_Notes_Evidence", evidence_df)

        if not evidence_df.empty and "note_id" in evidence_df.columns and "Quarter_Notes_Evidence" in wb.sheetnames:
            note_to_row: Dict[str, int] = {}
            for i, nid in enumerate(evidence_df["note_id"].astype(str).tolist(), start=2):
                note_to_row[nid] = i
            for coord, nid in note_link_cells:
                rr = note_to_row.get(nid)
                if rr is None:
                    continue
                c = ws[coord]
                _apply_hyperlink_look(c, f"#'Quarter_Notes_Evidence'!A{rr}")

        return qa_rows

    def _write_promise_tracker_ui() -> List[Dict[str, Any]]:
        ws = wb.create_sheet("Promise_Tracker_UI")
        qa_rows: List[Dict[str, Any]] = []
        ts = datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        ws["A1"] = f"Promise (metric | text | target | created | id) | Generated at {ts}"
        ws["A1"].font = Font(bold=True, size=header_size)

        def _qend_date(x: Any) -> Optional[date]:
            t = pd.to_datetime(x, errors="coerce")
            if pd.isna(t):
                return None
            return pd.Timestamp(t).to_period("Q").end_time.date()

        if promises is None or promises.empty:
            ws["A2"] = "No data."
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 74
            return qa_rows

        p = _promises_view().copy()
        pid_col = _resolve_col(p, ["promise_id", "id"])
        metric_col = _resolve_col(p, ["metric_tag", "metric"])
        text_col = _resolve_col(p, ["promise_text", "statement", "claim"])
        target_time_col = _resolve_col(p, ["target_time", "deadline"])
        target_val_col = _resolve_col(p, ["target_value", "value"])
        units_col = _resolve_col(p, ["units", "target_unit", "unit"])
        created_col = _resolve_col(p, ["created_quarter", "first_seen_quarter", "quarter"])

        if pid_col is None:
            ws["A2"] = "Missing promise_id in source."
            qa_rows.append(
                {
                    "quarter": None,
                    "metric": "Promise_Tracker_UI",
                    "check": "promise_tracker_ui_source_columns",
                    "status": "fail",
                    "message": "Promise_Tracker missing promise_id column.",
                }
            )
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 74
            return qa_rows

        p["_pid"] = p[pid_col].astype(str)
        p["_metric"] = p[metric_col].astype(str) if metric_col else ""
        p["_text"] = p[text_col].astype(str) if text_col else ""
        p["_target_time"] = pd.to_datetime(p[target_time_col], errors="coerce") if target_time_col else pd.NaT
        p["_target_val"] = pd.to_numeric(p[target_val_col], errors="coerce") if target_val_col else pd.NA
        p["_units"] = p[units_col].astype(str) if units_col else ""
        p["_created"] = pd.to_datetime(p[created_col], errors="coerce") if created_col else pd.NaT
        p = p.sort_values(["_metric", "_created", "_pid"], na_position="last").reset_index(drop=True)

        prog = promise_progress.copy() if promise_progress is not None else pd.DataFrame()
        q_col = _resolve_col(prog, ["quarter", "as_of"])
        prog_pid_col = _resolve_col(prog, ["promise_id", "id"])
        status_col = _resolve_col(prog, ["status"])
        progress_col = _resolve_col(prog, ["progress_pct"])
        src_doc_col = _resolve_col(prog, ["doc_path", "doc", "evidence_doc"])
        src_loc_col = _resolve_col(prog, ["section_or_page", "evidence_loc", "page"])
        src_snip_col = _resolve_col(prog, ["evidence_snippet", "snippet"])
        src_json_col = _resolve_col(prog, ["source_evidence_json", "evidence_json", "evidence"])

        quarters: List[date] = []
        if prog is not None and not prog.empty and q_col and prog_pid_col and status_col:
            prog["_quarter"] = pd.to_datetime(prog[q_col], errors="coerce")
            prog = prog[prog["_quarter"].notna()].copy()
        qset: Set[date] = set()
        if prog is not None and not prog.empty and "_quarter" in prog.columns:
            for qv in pd.to_datetime(prog["_quarter"], errors="coerce").dropna().tolist():
                qe = _qend_date(qv)
                if qe is not None:
                    qset.add(qe)
        for qv in pd.to_datetime(p["_created"], errors="coerce").dropna().tolist():
            qe = _qend_date(qv)
            if qe is not None:
                qset.add(qe)
        quarters = sorted(qset, reverse=True)
        for i, qd in enumerate(quarters, start=2):
            h = ws.cell(row=1, column=i, value=str(qd))
            h.font = Font(bold=True, size=header_size)
            h.alignment = Alignment(horizontal="center", vertical="center")

        status_map: Dict[Tuple[str, date], Dict[str, Any]] = {}
        evidence_rows: List[Dict[str, Any]] = []
        if prog is not None and not prog.empty and q_col and prog_pid_col and status_col:
            for _, r in prog.iterrows():
                pid = str(r.get(prog_pid_col) or "").strip()
                qv = pd.to_datetime(r.get("_quarter"), errors="coerce")
                if not pid or pd.isna(qv):
                    continue
                qd = _qend_date(qv)
                if qd is None:
                    continue
                status = str(r.get(status_col) or "").strip().lower()
                if not status:
                    continue
                ev_obj = {}
                if src_json_col:
                    raw = r.get(src_json_col)
                    if isinstance(raw, str) and raw.strip():
                        try:
                            parsed = json.loads(raw)
                            if isinstance(parsed, dict):
                                ev_obj = parsed
                            elif isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                                ev_obj = parsed[0]
                        except Exception:
                            ev_obj = {}
                doc_path = str(r.get(src_doc_col) or ev_obj.get("doc_path") or "").strip() if src_doc_col else str(ev_obj.get("doc_path") or "").strip()
                loc = str(r.get(src_loc_col) or ev_obj.get("section_or_page") or "").strip() if src_loc_col else str(ev_obj.get("section_or_page") or "").strip()
                snippet = str(r.get(src_snip_col) or ev_obj.get("snippet") or "").strip() if src_snip_col else str(ev_obj.get("snippet") or "").strip()
                if not doc_path or not snippet:
                    qa_rows.append(
                        {
                            "quarter": qd,
                            "metric": "Promise_Tracker_UI",
                            "check": "promise_progress_missing_evidence",
                            "status": "fail",
                            "message": f"promise {pid} status {status} missing evidence doc/snippet.",
                        }
                    )
                status_map[(pid, qd)] = {
                    "status": status,
                    "progress": pd.to_numeric(r.get(progress_col), errors="coerce") if progress_col else pd.NA,
                    "doc_path": doc_path,
                    "evidence_loc": loc,
                    "snippet": snippet,
                    "qa_message": str(r.get("qa_message") or ""),
                }
                evidence_rows.append(
                    {
                        "promise_id": pid,
                        "quarter": qd,
                        "status": status,
                        "doc_path": doc_path,
                        "evidence_loc": loc,
                        "snippet": snippet,
                    }
                )

        row_idx = 2
        status_cell_refs: List[Tuple[str, str, date]] = []
        for idx, (_, r) in enumerate(p.iterrows(), start=1):
            pid = str(r["_pid"])
            metric = str(r["_metric"] or "").strip()
            txt = str(r["_text"] or "").strip()
            metric_pref = f"[{metric}] " if metric else ""
            promise_short = re.sub(r"\s+", " ", f"{metric_pref}{txt}".strip())
            if len(promise_short) > 140:
                promise_short = f"{promise_short[:137]}..."
            t_time = pd.to_datetime(r["_target_time"], errors="coerce")
            target_q = _qend_date(t_time)
            tv = pd.to_numeric(r["_target_val"], errors="coerce")
            units = str(r["_units"] or "").strip()
            created_q = pd.to_datetime(r["_created"], errors="coerce")
            created_qe = _qend_date(created_q)
            created_txt = created_qe.isoformat() if created_qe else "n/a"
            target_txt = f"{tv:,.3f} {units}".strip() if pd.notna(tv) else ("qualitative" if target_q else "n/a")
            left_txt = f"Promise #{idx} | id:{pid}"
            ws.cell(row=row_idx, column=1, value=left_txt[:140])
            ws.cell(row=row_idx, column=1).alignment = Alignment(wrap_text=True, vertical="top")

            if target_q and pd.isna(tv):
                qa_rows.append(
                    {
                        "quarter": created_qe if created_qe else None,
                        "metric": "Promise_Tracker_UI",
                        "check": "promise_qualitative",
                        "status": "warn",
                        "message": f"promise {pid} has target_time but no target_value (qualitative).",
                    }
                )

            unclear_run = 0
            for i, qd in enumerate(quarters, start=2):
                st = status_map.get((pid, qd))
                cell_txt = ""
                if st:
                    status_txt = str(st["status"])
                    pct = st.get("progress")
                    if pd.notna(pct):
                        cell_txt = f"{status_txt} ({float(pct) * 100:.0f}%)"
                    else:
                        cell_txt = status_txt
                    if target_q is not None and qd == target_q:
                        if status_txt == "achieved":
                            cell_txt += " | TARGET HIT"
                        elif status_txt in {"broken", "missed"}:
                            cell_txt += " | TARGET MISS"
                        else:
                            cell_txt += " | TARGET Q"
                if created_qe is not None and qd == created_qe:
                    created_block = f"{promise_short}\ncreated {created_qe.isoformat()}"
                    cell_txt = f"{created_block}\n{cell_txt}".strip() if cell_txt else created_block
                if cell_txt:
                    ws.cell(row=row_idx, column=i, value=cell_txt[:220])
                    ws.cell(row=row_idx, column=i).alignment = Alignment(wrap_text=True, vertical="top")
                    if st:
                        status_cell_refs.append((ws.cell(row=row_idx, column=i).coordinate, pid, qd))
                        is_unclear = (
                            status_txt == "unclear"
                            or ("fallback" in status_txt)
                            or ("derived" in status_txt)
                            or ("fallback" in st.get("qa_message", "").lower())
                            or ("derived" in st.get("qa_message", "").lower())
                        )
                        if is_unclear:
                            unclear_run += 1
                            if unclear_run >= 5:
                                qa_rows.append(
                                    {
                                        "quarter": qd,
                                        "metric": "Promise_Tracker_UI",
                                        "check": "promise_unclear_streak",
                                        "status": "warn",
                                        "message": f"promise {pid} has >4 quarters unclear/fallback streak.",
                                    }
                                )
                        else:
                            unclear_run = 0
                elif target_q is not None and qd == target_q:
                    ws.cell(row=row_idx, column=i, value="TARGET Q (no update)")
                    ws.cell(row=row_idx, column=i).alignment = Alignment(wrap_text=True, vertical="top")
            row_idx += 1

        ws.freeze_panes = "B2"
        ws.column_dimensions["A"].width = 78
        for cidx in range(2, 2 + len(quarters)):
            ws.column_dimensions[get_column_letter(cidx)].width = 44
        for rr in range(2, row_idx):
            ws.row_dimensions[rr].height = 86

        if row_idx > 2 and len(quarters) > 0:
            last_col = get_column_letter(1 + len(quarters))
            rng = f"B2:{last_col}{row_idx-1}"
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"broken\",B2))"], fill=PatternFill("solid", fgColor="FFC7CE")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"at_risk\",B2))"], fill=PatternFill("solid", fgColor="FFEB9C")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"achieved\",B2))"], fill=PatternFill("solid", fgColor="C6EFCE")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"on_track\",B2))"], fill=PatternFill("solid", fgColor="E2F0D9")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"unclear\",B2))"], fill=PatternFill("solid", fgColor="D9D9D9")))

        evidence_df = pd.DataFrame(evidence_rows)
        if not evidence_df.empty:
            evidence_df = evidence_df.sort_values(["promise_id", "quarter"]).drop_duplicates(["promise_id", "quarter"], keep="last").reset_index(drop=True)
        _write_sheet("Promise_Evidence", evidence_df)

        if not evidence_df.empty and "Promise_Evidence" in wb.sheetnames:
            ev_map: Dict[Tuple[str, date], int] = {}
            for i, rr in evidence_df.iterrows():
                qv = pd.to_datetime(rr.get("quarter"), errors="coerce")
                if pd.isna(qv):
                    continue
                qd = _qend_date(qv)
                if qd is None:
                    continue
                ev_map[(str(rr.get("promise_id")), qd)] = i + 2
            for coord, pid, qd in status_cell_refs:
                rnum = ev_map.get((pid, qd))
                if rnum is None:
                    continue
                c = ws[coord]
                _apply_hyperlink_look(c, f"#'Promise_Evidence'!A{rnum}")

        return qa_rows

    def _write_analysis_sheet_title_and_metadata(
        ws: Any,
        title: str,
        metadata_text: str,
        *,
        max_col: int,
        title_row: int = 1,
        metadata_row: int = 2,
    ) -> int:
        theme = _get_analysis_sheet_style_bundle()
        title_fill = copy(theme["title_fill"])
        section_fill = copy(theme["section_fill"])
        thin_border = copy(theme["thin_border"])
        title_font = Font(bold=True, size=15, color="FFFFFF")
        metadata_font = Font(size=10, color=str(theme["text_muted"]), italic=True)
        try:
            ws.merge_cells(
                start_row=title_row,
                start_column=1,
                end_row=title_row,
                end_column=max_col,
            )
        except Exception:
            pass
        tcell = ws.cell(row=title_row, column=1, value=title)
        tcell.font = title_font
        tcell.fill = title_fill
        tcell.alignment = Alignment(horizontal="center", vertical="center")
        tcell.border = thin_border
        ws.row_dimensions[title_row].height = 24.0
        for cc in range(1, max_col + 1):
            cell = ws.cell(row=title_row, column=cc)
            cell.fill = title_fill
            cell.border = thin_border

        try:
            ws.merge_cells(
                start_row=metadata_row,
                start_column=1,
                end_row=metadata_row,
                end_column=max_col,
            )
        except Exception:
            pass
        mcell = ws.cell(row=metadata_row, column=1, value=metadata_text)
        mcell.font = metadata_font
        mcell.fill = section_fill
        mcell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
        mcell.border = thin_border
        ws.row_dimensions[metadata_row].height = 18.0
        for cc in range(1, max_col + 1):
            cell = ws.cell(row=metadata_row, column=cc)
            cell.fill = section_fill
            cell.border = thin_border
        return metadata_row + 1

    def _render_stacked_quarter_blocks(
        ws: Any,
        quarters: List[date],
        rows_by_quarter: Dict[date, List[Dict[str, Any]]],
        max_col: int,
        block_title_fn: Any,
        row_writer: Any,
        block_header_writer: Optional[Any] = None,
        start_row: int = 2,
        blank_row_between: bool = True,
    ) -> int:
        theme = _get_analysis_sheet_style_bundle()
        hdr_fill = copy(theme["title_fill"])
        thin_border = copy(theme["thin_border"])
        sep_side = copy(theme["thin_side"])
        row_idx = start_row
        for qd in quarters:
            title = block_title_fn(qd) if callable(block_title_fn) else str(qd)
            h = ws.cell(row=row_idx, column=1, value=title)
            h.font = Font(bold=True, size=header_size, color="FFFFFF")
            h.fill = hdr_fill
            h.alignment = Alignment(horizontal="left", vertical="center")
            h.border = thin_border
            for cc in range(2, max_col + 1):
                ws.cell(row=row_idx, column=cc, value=None).fill = hdr_fill
                ws.cell(row=row_idx, column=cc).border = thin_border
            row_idx += 1

            if block_header_writer is not None:
                row_idx = int(block_header_writer(ws, row_idx, qd, max_col))

            items = rows_by_quarter.get(qd, [])
            if not items:
                c = ws.cell(row=row_idx, column=2, value="No high-signal items.")
                c.font = Font(size=11, italic=True, color="666666")
                c.alignment = Alignment(vertical="top")
                row_idx += 1
            else:
                for item in items:
                    row_writer(ws, row_idx, qd, item)
                    row_idx += 1

            sep_row = max(start_row, row_idx - 1)
            for cc in range(1, max_col + 1):
                cell = ws.cell(row=sep_row, column=cc)
                cell.border = Border(
                    left=cell.border.left,
                    right=cell.border.right,
                    top=cell.border.top,
                    bottom=sep_side,
                )
            if blank_row_between:
                row_idx += 1
        return row_idx

    _quarter_notes_ui_selection_outer_scope = dict(locals())

    def _quarter_notes_context_adapter_deps() -> QuarterNotesContextAdapterDeps:
        return QuarterNotesContextAdapterDeps(
            runtime={
                "QuarterNotesUiOrchestratorDeps": QuarterNotesUiOrchestratorDeps,
                "write_quarter_notes_ui_sheet": write_quarter_notes_ui_sheet,
                "wb": wb,
                "ticker": ticker,
                "company_profile": company_profile,
                "is_pbi_profile": is_pbi_profile,
                "is_gpre_profile": is_gpre_profile,
                "is_anf_profile": is_anf_profile,
                "quarter_notes": quarter_notes,
                "hist": hist,
                "promises": promises,
                "cache_root": cache_root,
                "inputs": inputs,
                "ui_state": ui_state,
                "ui_info_rows": ui_info_rows,
                "ctx_ref": ctx_ref,
                "quarter_notes_runtime": quarter_notes_runtime,
                "context_globals": globals(),
                "quarter_notes_ui_selection_outer_scope": _quarter_notes_ui_selection_outer_scope,
                "write_analysis_sheet_title_and_metadata": _write_analysis_sheet_title_and_metadata,
                "get_analysis_sheet_style_bundle": _get_analysis_sheet_style_bundle,
                "quarter_notes_view": _quarter_notes_view,
                "resolve_col": _resolve_col,
                "normalize_text": glx_normalize_text,
                "split_sentences": glx_split_sentences,
                "dedup_text_key": glx_dedup_text_key,
                "extract_numeric_patterns": glx_extract_numeric_patterns,
                "normalize_period": glx_normalize_period,
                "compact_snippet": qn_compact_snippet,
                "quarter_label_short": _quarter_label_short,
                "ensure_terminal_period": _ensure_terminal_period,
                "collapse_repeated_leading_ngram": _collapse_repeated_leading_ngram_local,
                "dedupe_canonical_text_parts": _dedupe_canonical_text_parts_local,
                "quarter_note_runtime_qd_token": _quarter_note_runtime_qd_token,
                "quarter_note_runtime_signature": _quarter_note_runtime_signature,
                "quarter_note_runtime_cache_key": _quarter_note_runtime_cache_key,
                "shared_build_evidence_event": shared_build_evidence_event,
                "audit_view": _audit_view,
                "submission_recent_rows": _submission_recent_rows,
                "submission_recent_row_quarter": _submission_recent_row_quarter,
                "sec_docs_for_accession": _sec_docs_for_accession,
                "resolve_cached_doc_path": _resolve_cached_doc_path,
                "path_cache_key": _path_cache_key,
                "read_cached_doc_text": _read_cached_doc_text,
                "parse_date": parse_date,
                "anf_visible_quarter_note_summaries": _anf_visible_quarter_note_summaries,
                "anf_clean_visible_ui_text": _anf_clean_visible_ui_text,
                "anf_polish_quarter_note_visible_fields": _anf_polish_quarter_note_visible_fields,
                "record_writer_substage": _record_writer_substage,
                "timed_writer_substage": _timed_writer_substage,
                "record_writer_elapsed": _record_writer_elapsed,
                "quarter_narrative_recent_periods_from_frame": _quarter_narrative_recent_periods_from_frame,
                "quarter_narrative_records_for_context": _quarter_narrative_records_for_context,
                "write_quarter_narrative_data_sheet": _write_quarter_narrative_data_sheet,
                "write_quarter_notes_ui_narrative_sheet": _write_quarter_notes_ui_narrative_sheet,
            }
        )

    def _write_quarter_notes_ui_v2(
        rank_cutoff: int = 8, severity_cutoff: float = 50.0, max_rows_per_category: int = 10, quarters_shown: int = 12
    ) -> List[Dict[str, Any]]:
        return _write_quarter_notes_ui_v2_impl(
            _quarter_notes_context_adapter_deps(),
            rank_cutoff=rank_cutoff,
            severity_cutoff=severity_cutoff,
            max_rows_per_category=max_rows_per_category,
            quarters_shown=quarters_shown,
        )

    def _write_promise_tracker_ui_v2(render_visible: bool = True) -> List[Dict[str, Any]]:
        deps = PromiseTrackerWriterDeps(
            wb=wb,
            promises=promises,
            slides_guidance=slides_guidance,
            promise_evidence_df=promise_evidence_df,
            ui_state=ui_state,
            ui_info_rows=ui_info_rows,
            company_profile=company_profile,
            ticker=ticker,
            is_pbi_profile=is_pbi_profile,
            is_gpre_profile=is_gpre_profile,
            header_size=header_size,
            apply_hyperlink_look=_apply_hyperlink_look,
            candidate_quality_key=_candidate_quality_key,
            classify_pbi_metric_label=_classify_pbi_metric_label,
            clean_target_bonus=_clean_target_bonus,
            derive_split_target_meta=_derive_split_target_meta,
            extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
            extract_money_targets_for_display=_extract_money_targets_for_display,
            extract_pbi_guidance_targets_multi=_extract_pbi_guidance_targets_multi,
            extract_pbi_target_display=_extract_pbi_target_display,
            fmt_short_money_value_local=_fmt_short_money_value_local,
            gpre_bad_visible_promise_reason=_gpre_bad_visible_promise_reason,
            gpre_clean_visible_promise_metric=_gpre_clean_visible_promise_metric,
            is_45z_crush_margin_support_only=_is_45z_crush_margin_support_only,
            is_pbi_clean_sentence=_is_pbi_clean_sentence,
            is_preferred_narrative_source=_is_preferred_narrative_source,
            load_profile_slide_signals=_load_profile_slide_signals,
            looks_pbi_fragment_text=_looks_pbi_fragment_text,
            management_theme_key=_management_theme_key,
            pbi_promise_theme_re=_pbi_promise_theme_re,
            pbi_structured_guidance_items_for_qd=_pbi_structured_guidance_items_for_qd,
            pbi_structured_strategy_items_for_qd=_pbi_structured_strategy_items_for_qd,
            pbi_target_display_ok=_pbi_target_display_ok,
            profile_slide_metric=_profile_slide_metric,
            promises_view=_promises_view,
            resolve_col=_resolve_col,
            set_cell_comment=_set_cell_comment_local,
            slide_signal_noise=_slide_signal_noise,
            source_rank=_source_rank,
            split_target_group_key=_split_target_group_key,
            split_target_identity_key=_split_target_identity_key,
            split_target_metric_display=_split_target_metric_display,
            split_target_scope_token=_split_target_scope_token,
            strong_45z_2026_target_display=_strong_45z_2026_target_display,
            text_fragment_penalty=_text_fragment_penalty,
        )
        return write_promise_tracker_ui_sheet(deps, render_visible=render_visible)

    def _ensure_promise_progress_ui_bundle(
        quarter_hint: Optional[Tuple[date, ...]] = None,
    ) -> Dict[str, Any]:
        nonlocal promise_progress_ui_bundle_cache
        deps = PromiseProgressUiBundleDeps(
            promise_progress=promise_progress,
            promise_evidence_df=promise_evidence_df,
            hist=hist,
            adj_metrics=adj_metrics,
            is_pbi_profile=is_pbi_profile,
            resolve_col=_resolve_col,
            hist_view=_hist_view,
            adj_metrics_view=_adj_metrics_view,
            classify_pbi_metric_label=_classify_pbi_metric_label,
            extract_pbi_target_display=_extract_pbi_target_display,
            extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
            strong_45z_2026_target_display=_strong_45z_2026_target_display,
            extract_money_targets_for_display=_extract_money_targets_for_display,
            fmt_short_money_value_local=_fmt_short_money_value_local,
        )
        bundle = build_promise_progress_ui_bundle(
            deps,
            quarter_hint=quarter_hint,
            cached_bundle=promise_progress_ui_bundle_cache,
        )
        promise_progress_ui_bundle_cache = bundle
        if ctx_ref is not None:
            ctx_ref.derived.promise_progress_ui_bundle = bundle
        return bundle

    def _write_promise_progress_ui_v2() -> List[Dict[str, Any]]:
        return promise_progress_write_promise_progress_ui_v2(
            PromiseProgressOrchestratorDeps(
                wb=wb,
                ticker=ticker,
                is_anf_profile=is_anf_profile,
                is_pbi_profile=is_pbi_profile,
                is_gpre_profile=is_gpre_profile,
                promise_progress=promise_progress,
                promises=promises,
                ui_state=ui_state if isinstance(ui_state, dict) else {},
                ui_info_rows=ui_info_rows,
                hist=hist,
                adj_metrics=adj_metrics,
                slides_guidance=slides_guidance,
                material_roots=material_roots,
                ticker_roots=ticker_roots,
                pdf_text_cache_root=pdf_text_cache_root,
                rebuild_doc_text_cache=rebuild_doc_text_cache,
                quiet_pdf_warnings=quiet_pdf_warnings,
                quarter_notes=quarter_notes,
                promise_visible_max_col=PROMISE_VISIBLE_MAX_COL,
                promise_timeline_headers=PROMISE_TIMELINE_HEADERS,
                anf_build_promise_progress_sections=_anf_build_promise_progress_sections,
                anf_clean_visible_ui_text=_anf_clean_visible_ui_text,
                apply_hyperlink_look=_apply_hyperlink_look,
                candidate_quality_key=_candidate_quality_key,
                classify_pbi_metric_label=_classify_pbi_metric_label,
                clean_target_bonus=_clean_target_bonus,
                coerce_amount_with_unit_local=_coerce_amount_with_unit_local,
                derive_split_target_meta=_derive_split_target_meta,
                ensure_promise_progress_ui_bundle=_ensure_promise_progress_ui_bundle,
                ensure_terminal_period=_ensure_terminal_period,
                estimate_wrapped_line_count=_estimate_wrapped_line_count,
                estimate_wrapped_row_height=_estimate_wrapped_row_height,
                excel_safe_text_local=_excel_safe_text_local,
                extract_45z_monetization_target_display=_extract_45z_monetization_target_display,
                extract_45z_realized_progress_text=_extract_45z_realized_progress_text,
                extract_money_targets_for_display=_extract_money_targets_for_display,
                extract_pbi_target_display=_extract_pbi_target_display,
                fmt_short_money_value_local=_fmt_short_money_value_local,
                get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
                gpre_bad_visible_promise_reason=_gpre_bad_visible_promise_reason,
                gpre_clean_visible_promise_metric=_gpre_clean_visible_promise_metric,
                infer_target_period=_infer_target_period,
                infer_target_structure=_infer_target_structure,
                is_45z_crush_margin_support_only=_is_45z_crush_margin_support_only,
                is_pbi_clean_sentence=_is_pbi_clean_sentence,
                is_preferred_narrative_source=_is_preferred_narrative_source,
                load_profile_slide_signals=_load_profile_slide_signals,
                local_slide_45z_realized_text=_local_slide_45z_realized_text,
                looks_pbi_fragment_text=_looks_pbi_fragment_text,
                lookup_pbi_structured_guidance_target=_lookup_pbi_structured_guidance_target,
                lookup_pbi_structured_progress_hint=_lookup_pbi_structured_progress_hint,
                management_credibility_scorecard_rows=_management_credibility_scorecard_rows,
                management_theme_key=_management_theme_key,
                nearest_amount_for_pattern=_nearest_amount_for_pattern,
                parse_quarter_from_filename=_parse_quarter_from_filename,
                parse_quarter_from_follow_text=_parse_quarter_from_follow_text,
                pbi_guidance_period_label_from_text=_pbi_guidance_period_label_from_text,
                pbi_promise_theme_re=_pbi_promise_theme_re,
                pbi_repair_guidance_period_meta=_pbi_repair_guidance_period_meta,
                pbi_structured_strategy_items_for_qd=_pbi_structured_strategy_items_for_qd,
                pbi_target_display_ok=_pbi_target_display_ok,
                quarter_label_short=_quarter_label_short,
                quarter_notes_view=_quarter_notes_view,
                read_cached_doc_raw=_read_cached_doc_raw,
                record_writer_substage=_record_writer_substage,
                render_stacked_quarter_blocks=_render_stacked_quarter_blocks,
                resolve_col=_resolve_col,
                rewrite_shared_promise_progress_ui_from_blocks=_rewrite_shared_promise_progress_ui_from_blocks,
                safe_cell=_safe_cell,
                set_cell_comment_local=_set_cell_comment_local,
                slide_signal_noise=_slide_signal_noise,
                slide_text_paths=_slide_text_paths,
                source_rank=_source_rank,
                split_target_family_key=_split_target_family_key,
                split_target_identity_key=_split_target_identity_key,
                split_target_metric_display=_split_target_metric_display,
                split_target_qend=_split_target_qend,
                split_target_scope_is_broad=_split_target_scope_is_broad,
                split_target_scope_token=_split_target_scope_token,
                strong_45z_2026_target_display=_strong_45z_2026_target_display,
                target_period_is_closed=_target_period_is_closed,
                text_fragment_penalty=_text_fragment_penalty,
                timed_writer_substage=_timed_writer_substage,
                write_analysis_sheet_title_and_metadata=_write_analysis_sheet_title_and_metadata,
            )
        )

    financial_report_support: Optional[FinancialReportSupport] = None

    def _financial_report_support_runtime() -> Dict[str, Any]:
        return {
            "wb": wb,
            "hist": hist,
            "audit": audit,
            "needs_review": needs_review,
            "company_profile": company_profile,
            "bank_metrics_enabled": bank_metrics_enabled,
            "font_size": font_size,
            "header_size": header_size,
            "pd": pd,
            "date": date,
            "datetime": datetime,
            "Font": Font,
            "Alignment": Alignment,
            "Table": Table,
            "TableStyleInfo": TableStyleInfo,
            "get_column_letter": get_column_letter,
            "classify_duration": classify_duration,
            "_hist_view": _hist_view,
            "_audit_view": _audit_view,
            "_source_class": _source_class,
            "_source_method": _source_method,
            "_source_qa": _source_qa,
            "_source_tier": _source_tier,
            "_source_label": _source_label,
            "_safe_cell": _safe_cell,
            "_autowidth": _autowidth,
            "_updated_font": _updated_font,
            "strictness": strictness,
        }

    def _get_financial_report_support() -> FinancialReportSupport:
        nonlocal financial_report_support
        if financial_report_support is None:
            financial_report_support = FinancialReportSupport(
                FinancialReportSupportDeps(runtime=_financial_report_support_runtime())
            )
        return financial_report_support

    def _period_type(row: pd.Series) -> str:
        return _get_financial_report_support().period_type(row)

    def _build_facts_long() -> pd.DataFrame:
        return _get_financial_report_support().build_facts_long()

    def _build_lineitem_map() -> pd.DataFrame:
        return _get_financial_report_support().build_lineitem_map()

    def _build_period_index(max_periods: int = 12) -> pd.DataFrame:
        return _get_financial_report_support().build_period_index(max_periods)

    def _build_report(statement: str, scale: float = 1e6) -> pd.DataFrame:
        return _get_financial_report_support().build_report(statement, scale=scale)

    def _write_report_sheet(name: str, df: pd.DataFrame, scale_label: str) -> None:
        return _get_financial_report_support().write_report_sheet(name, df, scale_label)

    def _write_summary_sheet(df: pd.DataFrame) -> None:
        return write_summary_sheet(
            SummarySheetRenderDeps(
                wb=wb,
                font_size=font_size,
                header_size=header_size,
                set_cell_comment=_set_cell_comment_local,
                normalize_text=glx_normalize_text,
                estimate_wrapped_line_count=_estimate_wrapped_line_count,
                estimate_wrapped_row_height=_estimate_wrapped_row_height,
            ),
            df,
        )
    _gpre_commercial_setup_support_cache: Optional[GpreCommercialSetupSupport] = None

    def _get_gpre_commercial_setup_support() -> GpreCommercialSetupSupport:
        nonlocal _gpre_commercial_setup_support_cache
        if _gpre_commercial_setup_support_cache is None:
            _gpre_commercial_setup_support_cache = GpreCommercialSetupSupport(
                GpreCommercialSetupDeps(
                    is_gpre_profile=is_gpre_profile,
                    ctx_ref=ctx_ref,
                    cache_dir=cache_dir,
                    load_operating_driver_source_records_by_quarter=_load_operating_driver_source_records_by_quarter,
                    normalize_text=glx_normalize_text,
                    split_sentences=glx_split_sentences,
                    compact_snippet=qn_compact_snippet,
                    ensure_terminal_period=_ensure_terminal_period,
                    data_root_from_sec_cache_path=data_root_from_sec_cache_path,
                )
            )
        return _gpre_commercial_setup_support_cache

    def _read_local_doc_text_shared(path_in: Any) -> str:
        return _get_gpre_commercial_setup_support().read_local_doc_text(path_in)

    def _gpre_local_bofa_conference_path_shared() -> Path:
        return _get_gpre_commercial_setup_support().local_bofa_conference_path()

    def _gpre_local_bofa_conference_text_shared() -> str:
        return _get_gpre_commercial_setup_support().local_bofa_conference_text()

    def _gpre_local_stephens_conference_path_shared() -> Path:
        return _get_gpre_commercial_setup_support().local_stephens_conference_path()

    def _gpre_local_stephens_conference_raw_path_shared() -> Path:
        return _get_gpre_commercial_setup_support().local_stephens_conference_raw_path()

    def _gpre_local_stephens_conference_text_shared() -> str:
        return _get_gpre_commercial_setup_support().local_stephens_conference_text()

    def _gpre_local_stephens_conference_raw_text_shared() -> str:
        return _get_gpre_commercial_setup_support().local_stephens_conference_raw_text()

    def _gpre_local_bmo_conference_path_shared() -> Path:
        return _get_gpre_commercial_setup_support().local_bmo_conference_path()

    def _gpre_local_bmo_conference_text_shared() -> str:
        return _get_gpre_commercial_setup_support().local_bmo_conference_text()

    def _gpre_local_bofa_conference_excerpt_shared(terms: Tuple[str, ...], max_len: int = 280) -> str:
        return _get_gpre_commercial_setup_support().local_bofa_conference_excerpt(terms, max_len)

    def _gpre_commercial_setup_records_shared() -> List[Dict[str, Any]]:
        return _get_gpre_commercial_setup_support().records()

    def _write_valuation_sheet() -> None:
        valuation_orchestrator_helpers = {
            "Alignment": Alignment,
            "Border": Border,
            "CellIsRule": CellIsRule,
            "DefinedName": DefinedName,
            "Font": Font,
            "Path": Path,
            "PatternFill": PatternFill,
            "Side": Side,
            "_anf_buyback_execution_is_year_or_ttm": _anf_buyback_execution_is_year_or_ttm,
            "_anf_format_year_ttm_buyback_summary": _anf_format_year_ttm_buyback_summary,
            "_anf_is_missing_value": _anf_is_missing_value,
            "_anf_normalize_ytd_buyback_cash_map_for_valuation": _anf_normalize_ytd_buyback_cash_map_for_valuation,
            "_anf_prior_year_quarter": _anf_prior_year_quarter,
            "_anf_value_delta_map_for_fiscal_periods": _anf_value_delta_map_for_fiscal_periods,
            "_anf_visible_quarter_label": _anf_visible_quarter_label,
            "_anf_yoy_map_for_fiscal_periods": _anf_yoy_map_for_fiscal_periods,
            "_audit_view": _audit_view,
            "_build_hidden_value_flags_fallback": _build_hidden_value_flags_fallback,
            "_build_operating_driver_rows": _build_operating_driver_rows,
            "_collapse_repeated_leading_ngram_local": _collapse_repeated_leading_ngram_local,
            "_dedupe_canonical_text_parts_local": _dedupe_canonical_text_parts_local,
            "_ensure_terminal_period": _ensure_terminal_period,
            "_ensure_valuation_precompute_bundle": _ensure_valuation_precompute_bundle,
            "_ensure_valuation_render_bundle": _ensure_valuation_render_bundle,
            "_estimate_wrapped_row_height": _estimate_wrapped_row_height,
            "_extract_45z_monetization_target_display": _extract_45z_monetization_target_display,
            "_extract_latest_buyback_remaining_from_sec": _extract_latest_buyback_remaining_from_sec,
            "_extract_money_targets_for_display": _extract_money_targets_for_display,
            "_extract_pbi_target_display": _extract_pbi_target_display,
            "_extract_valuation_filing_doc_text": _extract_valuation_filing_doc_text,
            "_first_existing_material_dir": _first_existing_material_dir,
            "_fmt_short_money_value_local": _fmt_short_money_value_local,
            "_gpre_commercial_setup_records_shared": _gpre_commercial_setup_records_shared,
            "_gpre_local_bmo_conference_path_shared": _gpre_local_bmo_conference_path_shared,
            "_gpre_local_bmo_conference_text_shared": _gpre_local_bmo_conference_text_shared,
            "_gpre_local_bofa_conference_path_shared": _gpre_local_bofa_conference_path_shared,
            "_gpre_local_bofa_conference_text_shared": _gpre_local_bofa_conference_text_shared,
            "_gpre_local_stephens_conference_path_shared": _gpre_local_stephens_conference_path_shared,
            "_gpre_local_stephens_conference_raw_path_shared": _gpre_local_stephens_conference_raw_path_shared,
            "_gpre_local_stephens_conference_raw_text_shared": _gpre_local_stephens_conference_raw_text_shared,
            "_gpre_local_stephens_conference_text_shared": _gpre_local_stephens_conference_text_shared,
            "_gpre_normalize_metric_label": _gpre_normalize_metric_label,
            "_htmlish_to_text": _htmlish_to_text,
            "_load_profile_slide_signals": _load_profile_slide_signals,
            "_operating_driver_financial_statement_files": _operating_driver_financial_statement_files,
            "_parse_quarter_from_filename": _parse_quarter_from_filename,
            "_parse_quarter_from_follow_text": _parse_quarter_from_follow_text,
            "_pbi_guidance_period_label_from_text": _pbi_guidance_period_label_from_text,
            "_pbi_repair_guidance_period_meta": _pbi_repair_guidance_period_meta,
            "_pbi_structured_strategy_items_for_qd": _pbi_structured_strategy_items_for_qd,
            "_period_label_to_norm": _period_label_to_norm,
            "_prev_quarter_end_from_qend": _prev_quarter_end_from_qend,
            "_profile_slide_signals_for_quarter": _profile_slide_signals_for_quarter,
            "_promises_view": _promises_view,
            "_quarter_notes_view": _quarter_notes_view,
            "_read_cached_doc_text": _read_cached_doc_text,
            "_read_local_doc_text_shared": _read_local_doc_text_shared,
            "_read_operating_driver_text": _read_operating_driver_text,
            "_resolve_cached_doc_path": _resolve_cached_doc_path,
            "_resolve_col": _resolve_col,
            "_resolve_thesis_fy_base": _resolve_thesis_fy_base,
            "_safe_text_value": _safe_text_value,
            "_sec_cache_docs_for_token_local": _sec_cache_docs_for_token_local,
            "_sec_docs_for_accession": _sec_docs_for_accession,
            "_slide_signal_noise": _slide_signal_noise,
            "_source_backed_debt_tranches_from_slides": _source_backed_debt_tranches_from_slides,
            "_submission_recent_row_quarter": _submission_recent_row_quarter,
            "_submission_recent_rows": _submission_recent_rows,
            "_updated_font": _updated_font,
            "annual_segment_alias_patterns": annual_segment_alias_patterns,
            "build_hidden_value_flags": build_hidden_value_flags,
            "build_valuation_history_source_maps": build_valuation_history_source_maps,
            "copy": copy,
            "display_m_source_map": display_m_source_map,
            "ew_latest_segment_financials_workbook": ew_latest_segment_financials_workbook,
            "ew_parse_quarterly_segment_data_from_workbook": ew_parse_quarterly_segment_data_from_workbook,
            "font_size": font_size,
            "get_column_letter": get_column_letter,
            "glx_normalize_text": glx_normalize_text,
            "header_size": header_size,
            "history_margin_source_map": history_margin_source_map,
            "history_numeric_source_map": history_numeric_source_map,
            "infer_quarter_end_from_text": infer_quarter_end_from_text,
            "normalize_capex_for_valuation": normalize_capex_for_valuation,
            "pd": pd,
            "qn_compact_snippet": qn_compact_snippet,
            "quarter_key_union": quarter_key_union,
            "re": re,
            "source_infer_q_from_name": source_infer_q_from_name,
            "strip_html": strip_html,
            "ttm_map": ttm_map,
            "ttm_sparse_cashflow_map": ttm_sparse_cashflow_map,
            "valuation_hidden_comparison_metric": valuation_hidden_comparison_metric,
        }
        return write_valuation_sheet(
            ValuationOrchestratorDeps(
                wb=wb,
                ticker=ticker,
                company_profile=company_profile,
                is_pbi_profile=is_pbi_profile,
                is_gpre_profile=is_gpre_profile,
                is_anf_profile=is_anf_profile,
                price=price,
                excel_mode=excel_mode,
                hist=hist,
                quarter_notes=quarter_notes,
                promises=promises,
                audit=audit,
                promise_progress=promise_progress,
                slides_guidance=slides_guidance,
                slides_debt=slides_debt,
                valuation_grid_df=valuation_grid_df,
                adj_metrics=adj_metrics,
                adj_metrics_relaxed=adj_metrics_relaxed,
                leverage_df=leverage_df,
                manifest_df=manifest_df,
                flags_df=flags_df,
                flags_audit_df=flags_audit_df,
                signals_base_df=signals_base_df,
                debt_tranches=debt_tranches,
                debt_tranches_latest=debt_tranches_latest,
                debt_credit_notes=debt_credit_notes,
                company_overview=company_overview,
                cache_root=cache_root,
                cache_dir=cache_dir,
                material_roots=material_roots,
                ctx_ref=ctx_ref,
                ui_state=ui_state,
                context_globals=globals(),
                get_valuation_style_bundle=_get_valuation_style_bundle,
                set_cell_comment=_set_cell_comment_local,
                timed_writer_substage=_timed_writer_substage,
                record_writer_substage=_record_writer_substage,
                record_writer_elapsed=_record_writer_elapsed,
                context_helpers=valuation_orchestrator_helpers,
            )
        )

    def _build_summary() -> pd.DataFrame:
        return build_summary_dataframe(
            SummaryBuilderDeps(
                hist=hist,
                leverage_df=leverage_df,
                needs_review=needs_review,
                company_overview=company_overview,
                price=price,
                ctx_ref=ctx_ref,
                hist_view=_hist_view,
                audit_view=_audit_view,
            )
        )

    facts_long = pd.DataFrame()
    lineitem_map = pd.DataFrame()
    period_index = pd.DataFrame()
    report_is = pd.DataFrame()
    report_bs = pd.DataFrame()
    report_cf = pd.DataFrame()

    def _normalize_leverage_text(text_in: Any) -> str:
        return source_normalize_leverage_text(text_in)

    def _infer_q_from_name(name: str) -> Optional[dt.date]:
        return source_infer_q_from_name(name)

    def _normalize_leverage_quarter(qv: Any) -> Optional[pd.Timestamp]:
        return source_normalize_leverage_quarter(qv)

    def _hist_quarter_whitelist() -> Set[pd.Timestamp]:
        return source_hist_quarter_whitelist(hist)

    def _read_leverage_material_text(path_in: Path) -> str:
        return _read_cached_doc_text(path_in, normalize=True)

    def _looks_like_leverage_text(text_in: Any) -> bool:
        return source_looks_like_leverage_text(text_in)

    def _load_leverage_local_material_index() -> List[Dict[str, Any]]:
        nonlocal leverage_local_material_index_cache
        if leverage_local_material_index_cache is not None:
            if ctx_ref is not None:
                ctx_ref.derived.valuation_local_material_index = list(leverage_local_material_index_cache)
            return leverage_local_material_index_cache
        leverage_local_material_index_cache = source_build_leverage_local_material_index(
            hist=hist,
            material_roots=tuple(material_roots),
            ticker=ticker,
            ticker_roots=tuple(ticker_roots),
            read_cached_doc_text_fn=_read_cached_doc_text,
            infer_cached_doc_quarter_fn=_infer_cached_doc_quarter,
        )
        if ctx_ref is not None:
            ctx_ref.derived.valuation_local_material_index = list(leverage_local_material_index_cache)
        return leverage_local_material_index_cache

    def _load_leverage_audit_doc_index() -> List[Dict[str, Any]]:
        nonlocal leverage_audit_doc_index_cache
        if leverage_audit_doc_index_cache is not None:
            if ctx_ref is not None:
                ctx_ref.derived.valuation_audit_doc_index = list(leverage_audit_doc_index_cache)
            return leverage_audit_doc_index_cache
        rows: List[Dict[str, Any]] = []
        if audit is None or audit.empty:
            leverage_audit_doc_index_cache = rows
            if ctx_ref is not None:
                ctx_ref.derived.valuation_audit_doc_index = list(rows)
            return rows
        leverage_audit_doc_index_cache = source_build_leverage_audit_doc_index(
            audit=audit,
            cache_root=cache_root,
            resolve_col=_resolve_col,
            accession_doc_lookup=_sec_docs_for_accession,
            read_leverage_material_text_fn=_read_leverage_material_text,
        )
        if ctx_ref is not None:
            ctx_ref.derived.valuation_audit_doc_index = list(leverage_audit_doc_index_cache)
        return leverage_audit_doc_index_cache

    def _extract_adj_net_leverage_text_map() -> Dict[pd.Timestamp, float]:
        nonlocal adj_net_leverage_text_map_cache
        if adj_net_leverage_text_map_cache is not None:
            if ctx_ref is not None:
                ctx_ref.derived.valuation_net_leverage_text_map = dict(adj_net_leverage_text_map_cache)
            return dict(adj_net_leverage_text_map_cache)
        out_map = source_extract_adj_net_leverage_text_map(
            promises=promises,
            quarter_notes=quarter_notes,
            slides_guidance=slides_guidance,
            ocr_log=ocr_log,
            hist=hist,
            resolve_col=_resolve_col,
            load_local_material_index_fn=_load_leverage_local_material_index,
            load_audit_doc_index_fn=_load_leverage_audit_doc_index,
            timed_substage=_timed_writer_substage,
        )
        adj_net_leverage_text_map_cache = dict(out_map)
        if ctx_ref is not None:
            ctx_ref.derived.valuation_net_leverage_text_map = dict(out_map)
            if leverage_local_material_index_cache is not None:
                ctx_ref.derived.valuation_local_material_index = list(leverage_local_material_index_cache)
            if leverage_audit_doc_index_cache is not None:
                ctx_ref.derived.valuation_audit_doc_index = list(leverage_audit_doc_index_cache)
        return out_map


    # Non-GAAP bridge (strict + relaxed)
    def _build_ng_bridge(adj_df: pd.DataFrame, breakdown_df: pd.DataFrame) -> pd.DataFrame:
        if hist is None or hist.empty:
            return pd.DataFrame()
        if adj_df is None or adj_df.empty:
            return pd.DataFrame()
        h = hist.copy()
        h["quarter"] = pd.to_datetime(h["quarter"], errors="coerce")
        adj = adj_df.copy()
        if "quarter" not in adj.columns:
            return pd.DataFrame()
        adj["quarter"] = pd.to_datetime(adj["quarter"], errors="coerce")
        rows = []
        for q in sorted(adj["quarter"].dropna().unique()):
            hq = h[h["quarter"] == q]
            if hq.empty:
                continue
            gaap_op = pd.to_numeric(hq["op_income"], errors="coerce").iloc[0] if "op_income" in hq.columns else None
            gaap_ebitda = pd.to_numeric(hq["ebitda"], errors="coerce").iloc[0] if "ebitda" in hq.columns else None
            sub = adj[adj["quarter"] == q]
            adj_ebit = pd.to_numeric(sub.get("adj_ebit"), errors="coerce").iloc[0] if "adj_ebit" in sub else None
            adj_ebitda = pd.to_numeric(sub.get("adj_ebitda"), errors="coerce").iloc[0] if "adj_ebitda" in sub else None
            adj_sum = None
            if breakdown_df is not None and not breakdown_df.empty:
                bd = breakdown_df.copy()
                if "quarter" in bd.columns:
                    bd["quarter"] = pd.to_datetime(bd["quarter"], errors="coerce")
                    bdq = bd[bd["quarter"] == q]
                    if not bdq.empty and "value" in bdq.columns:
                        adj_sum = pd.to_numeric(bdq["value"], errors="coerce").sum()
            rows.append({
                "quarter": q.date(),
                "gaap_op_income": gaap_op,
                "adj_ebit": adj_ebit,
                "adjustments_sum": adj_sum,
                "gaap_ebitda": gaap_ebitda,
                "adj_ebitda": adj_ebitda,
            })
        return pd.DataFrame(rows)

    leverage_df = pd.DataFrame()
    valuation_summary_df = pd.DataFrame()
    valuation_grid_df = pd.DataFrame()
    summary_df = pd.DataFrame()
    signals_base_df = pd.DataFrame()
    flags_df = pd.DataFrame()
    flags_audit_df = pd.DataFrame()
    flags_recompute_df = pd.DataFrame()
    ng_bridge = pd.DataFrame()
    ng_bridge_relaxed = pd.DataFrame()

    data_is_rules_df = pd.DataFrame()
    if is_rules:
        rows = []
        for k, v in is_rules.items():
            if isinstance(v, list):
                rows.append({"rule": k, "value": "; ".join(str(x) for x in v)})
            else:
                rows.append({"rule": k, "value": v})
        data_is_rules_df = pd.DataFrame(rows)

    def _build_qn_evidence_src() -> pd.DataFrame:
        if quarter_notes is None or quarter_notes.empty:
            return pd.DataFrame(columns=["note_id", "quarter", "category", "claim", "metric", "doc_path", "evidence_id", "snippet"])
        src = _quarter_notes_view()
        q_col = _resolve_col(src, ["quarter", "quarter_end", "as_of_quarter"])
        cat_col = _resolve_col(src, ["category", "tag", "topic"])
        claim_col = _resolve_col(src, ["claim", "headline", "note", "body"])
        metric_col = _resolve_col(src, ["metric_ref", "metric", "metric_tag"])
        note_id_col = _resolve_col(src, ["note_id", "id"])
        doc_col = _resolve_col(src, ["doc_path", "evidence_doc", "doc"])
        snip_col = _resolve_col(src, ["evidence_snippet", "snippet"])
        out_rows: List[Dict[str, Any]] = []
        for _, r in src.iterrows():
            qd = _qend_date(r.get(q_col) if q_col else None)
            if qd is None:
                continue
            claim = str(r.get(claim_col) or "").strip() if claim_col else ""
            note_id = str(r.get(note_id_col) or "").strip() if note_id_col else ""
            if not note_id:
                note_id = hashlib.sha1(f"{qd}|{claim}".encode("utf-8")).hexdigest()[:12]
            ev = _parse_first_evidence(r)
            doc_path = str(r.get(doc_col) or ev.get("doc_path") or "").strip() if doc_col else str(ev.get("doc_path") or "").strip()
            snippet = str(r.get(snip_col) or ev.get("snippet") or claim).strip() if snip_col else str(ev.get("snippet") or claim).strip()
            out_rows.append(
                {
                    "note_id": note_id,
                    "quarter": qd,
                    "category": str(r.get(cat_col) or "").strip() if cat_col else "",
                    "claim": claim,
                    "metric": str(r.get(metric_col) or "").strip() if metric_col else "",
                    "doc_path": doc_path,
                    "evidence_id": hashlib.sha1(f"{note_id}|{doc_path}|{snippet[:64]}".encode("utf-8")).hexdigest()[:12],
                    "snippet": snippet,
                }
            )
        out = pd.DataFrame(out_rows)
        if out.empty:
            return pd.DataFrame(columns=["note_id", "quarter", "category", "claim", "metric", "doc_path", "evidence_id", "snippet"])
        return out.sort_values(["quarter", "category", "note_id"]).reset_index(drop=True)

    def _build_promise_evidence_src() -> pd.DataFrame:
        out_rows: List[Dict[str, Any]] = []
        if promise_progress is not None and not promise_progress.empty:
            src = promise_progress.copy()
            pid_col = _resolve_col(src, ["promise_id", "id"])
            q_col = _resolve_col(src, ["quarter", "as_of"])
            st_col = _resolve_col(src, ["status"])
            doc_col = _resolve_col(src, ["doc_path", "evidence_doc", "doc"])
            snip_col = _resolve_col(src, ["evidence_snippet", "snippet"])
            if pid_col is not None and q_col is not None:
                for _, r in src.iterrows():
                    pid = str(r.get(pid_col) or "").strip()
                    qd = _qend_date(r.get(q_col))
                    if not pid or qd is None:
                        continue
                    ev = _parse_first_evidence(r)
                    doc_path = (
                        str(r.get(doc_col) or ev.get("doc_path") or "").strip()
                        if doc_col
                        else str(ev.get("doc_path") or "").strip()
                    )
                    snippet = (
                        str(r.get(snip_col) or ev.get("snippet") or "").strip()
                        if snip_col
                        else str(ev.get("snippet") or "").strip()
                    )
                    out_rows.append(
                        {
                            "promise_id": pid,
                            "quarter": qd,
                            "status": str(r.get(st_col) or "").strip() if st_col else "",
                            "doc_path": doc_path,
                            "evidence_id": hashlib.sha1(f"{pid}|{qd}|{doc_path}|{snippet[:64]}".encode("utf-8")).hexdigest()[:12],
                            "snippet": snippet,
                        }
                    )

        if promises is not None and not promises.empty:
            ps = _promises_view()
            pid_col = _resolve_col(ps, ["promise_id", "id"])
            q_col = _resolve_col(ps, ["first_seen_evidence_quarter", "created_quarter", "first_seen_q", "first_seen_quarter", "quarter"])
            ev_json_col = _resolve_col(ps, ["source_evidence_json", "evidence_history_json", "evidence_json"])
            snip_col = _resolve_col(ps, ["evidence_snippet", "snippet", "promise_text", "statement"])
            if pid_col is not None and q_col is not None:
                for _, r in ps.iterrows():
                    pid = str(r.get(pid_col) or "").strip()
                    qd = _qend_date(r.get(q_col))
                    if not pid or qd is None:
                        continue
                    doc_path = ""
                    snippet = str(r.get(snip_col) or "").strip() if snip_col else ""
                    if ev_json_col:
                        raw = r.get(ev_json_col)
                        if isinstance(raw, str) and raw.strip():
                            try:
                                parsed = json.loads(raw)
                                ev = parsed[0] if isinstance(parsed, list) and parsed else (parsed if isinstance(parsed, dict) else {})
                                if isinstance(ev, dict):
                                    doc_path = str(ev.get("doc_path") or ev.get("doc_name") or "").strip()
                                    snippet = str(ev.get("snippet") or snippet).strip()
                            except Exception:
                                pass
                    out_rows.append(
                        {
                            "promise_id": pid,
                            "quarter": qd,
                            "status": "",
                            "doc_path": doc_path,
                            "evidence_id": hashlib.sha1(f"{pid}|{qd}|{doc_path}|{snippet[:64]}".encode("utf-8")).hexdigest()[:12],
                            "snippet": snippet,
                        }
                    )

        if not out_rows:
            return pd.DataFrame(columns=["promise_id", "quarter", "status", "doc_path", "evidence_id", "snippet"])
        out = pd.DataFrame(out_rows)
        out = out.sort_values(["promise_id", "quarter"]).drop_duplicates(["promise_id", "quarter"], keep="first")
        return out.reset_index(drop=True)

    quarter_notes_evidence_df = pd.DataFrame()
    promise_evidence_df = pd.DataFrame()
    ui_state = {"quarters": [], "promise_rows": pd.DataFrame()}

    _latest_quarter_qa_support_cache: Optional[LatestQuarterQASupport] = None

    def _get_latest_quarter_qa_support() -> LatestQuarterQASupport:
        nonlocal _latest_quarter_qa_support_cache
        if _latest_quarter_qa_support_cache is None:
            _latest_quarter_qa_support_cache = LatestQuarterQASupport(
                LatestQuarterQADeps(
                    ticker=ticker,
                    company_profile=company_profile,
                    is_pbi_profile=is_pbi_profile,
                    is_gpre_profile=is_gpre_profile,
                    is_anf_profile=is_anf_profile,
                    hist=hist,
                    leverage_df=leverage_df,
                    adj_metrics=adj_metrics,
                    audit=audit,
                    slides_guidance=slides_guidance,
                    slides_segments=slides_segments,
                    debt_tranches_latest=debt_tranches_latest,
                    debt_buckets=debt_buckets,
                    debt_profile=debt_profile,
                    debt_recon=debt_recon,
                    revolver_history=revolver_history,
                    non_gaap_files=non_gaap_files,
                    cache_root=cache_root,
                    material_roots=material_roots,
                    ticker_roots=ticker_roots,
                    document_cache=document_cache,
                    ui_state=ui_state,
                    ctx_ref=ctx_ref,
                    context_helpers={
                        "_hist_view": _hist_view,
                        "_audit_view": _audit_view,
                        "_adj_metrics_view": _adj_metrics_view,
                        "_resolve_col": _resolve_col,
                        "_submission_recent_rows": _submission_recent_rows,
                        "_submission_recent_row_quarter": _submission_recent_row_quarter,
                        "_sec_docs_for_accession": _sec_docs_for_accession,
                        "_resolve_cached_doc_path": _resolve_cached_doc_path,
                        "_read_cached_doc_text": _read_cached_doc_text,
                        "_path_belongs_to_ticker": _path_belongs_to_ticker,
                        "_first_existing_material_dir": _first_existing_material_dir,
                        "_ensure_valuation_render_bundle": _ensure_valuation_render_bundle,
                        "_ensure_valuation_precompute_bundle": _ensure_valuation_precompute_bundle,
                        "_timed_writer_substage": _timed_writer_substage,
                        "_parse_quarter_from_filename": _parse_quarter_from_filename,
                        "_parse_quarter_from_follow_text": _parse_quarter_from_follow_text,
                        "_pbi_reported_fcf_payload_for_qd": _pbi_reported_fcf_payload_for_qd,
                        "_anf_financial_schedule_support_doc_for_quarter": _anf_financial_schedule_support_doc_for_quarter,
                        "_slides_guidance_has_explicit_metric": _slides_guidance_has_explicit_metric,
                        "coerce_number": coerce_number,
                        "infer_quarter_end_from_text": infer_quarter_end_from_text,
                        "writer_qa_latest_quarter_support_gap_severity": writer_qa_latest_quarter_support_gap_severity,
                        "ew_parse_quarterly_segment_data_from_workbook": ew_parse_quarterly_segment_data_from_workbook,
                        "annual_segment_alias_patterns": annual_segment_alias_patterns,
                        "quarterly_segment_labels": quarterly_segment_labels,
                    },
                )
            )
        return _latest_quarter_qa_support_cache

    def _latest_quarter_qa_source_bundle(
        qref: pd.Timestamp,
        *,
        include_transcripts: bool = False,
    ) -> List[Dict[str, Any]]:
        return _get_latest_quarter_qa_support().source_bundle(
            qref,
            include_transcripts=include_transcripts,
        )

    def _latest_quarter_sec_text_corpus(qref: pd.Timestamp) -> str:
        return _get_latest_quarter_qa_support().sec_text_corpus(qref)

    def _run_latest_quarter_qa() -> List[Dict[str, Any]]:
        return _get_latest_quarter_qa_support().run()

    def _write_bs_segments_sheet(quarters_shown: int = 8) -> List[Dict[str, Any]]:
        deps = BsSegmentsWriterDeps(
            wb=wb,
            hist=hist,
            audit=audit,
            ticker=ticker,
            company_profile=company_profile,
            slides_segments=slides_segments,
            material_roots=material_roots,
            ticker_roots=ticker_roots,
            ui_info_rows=ui_info_rows,
            font_size=font_size,
            header_size=header_size,
            is_pbi_profile=is_pbi_profile,
            is_gpre_profile=is_gpre_profile,
            is_anf_profile=is_anf_profile,
            bank_metrics_enabled=bank_metrics_enabled,
            enable_quarterly_segment_block=enable_quarterly_segment_block,
            enable_annual_segment_block=enable_annual_segment_block,
            quarterly_segment_labels=quarterly_segment_labels,
            annual_segment_labels=annual_segment_labels,
            annual_segment_alias_patterns=annual_segment_alias_patterns,
            anf_segment_brand_explanation=ANF_SEGMENT_BRAND_EXPLANATION,
            get_valuation_style_bundle=_get_valuation_style_bundle,
            hist_view=_hist_view,
            resolve_col=_resolve_col,
            set_cell_comment=_set_cell_comment_local,
            shared_load_local_balance_sheet_detail_payloads=_shared_load_local_balance_sheet_detail_payloads,
            carry_forward_low_change_series=_carry_forward_low_change_series,
            first_existing_material_dir=_first_existing_material_dir,
            parse_quarter_from_filename=_parse_quarter_from_filename,
            parse_quarter_from_follow_text=_parse_quarter_from_follow_text,
            read_operating_driver_text=_read_operating_driver_text,
            operating_driver_financial_statement_files=_operating_driver_financial_statement_files,
            sec_cache_roots_local=_sec_cache_roots_local,
            anf_visible_quarter_label=_anf_visible_quarter_label,
        )
        return write_bs_segments_sheet(deps, quarters_shown=quarters_shown)

    operating_drivers_support: Optional[OperatingDriversSupport] = None

    def _operating_drivers_support_runtime() -> Dict[str, Any]:
        try:
            derivative_oci_bridge_df_runtime = derivative_oci_bridge_df
        except NameError:
            derivative_oci_bridge_df_runtime = pd.DataFrame()
        return {
            **globals(),
            "company_profile": company_profile,
            "hist": hist,
            "quarter_notes": quarter_notes,
            "promises": promises,
            "promise_progress": promise_progress,
            "adj_metrics": adj_metrics,
            "slides_segments": slides_segments,
            "is_gpre_profile": is_gpre_profile,
            "is_anf_profile": is_anf_profile,
            "ctx_ref": ctx_ref,
            "material_roots": material_roots,
            "ticker": ticker,
            "ticker_roots": ticker_roots,
            "cache_dir": cache_dir,
            "ui_info_rows": ui_info_rows,
            "operating_drivers_runtime": operating_drivers_runtime,
            "glx_normalize_text": glx_normalize_text,
            "qn_compact_snippet": qn_compact_snippet,
            "qn_is_complete_signal_text": qn_is_complete_signal_text,
            "_hist_view": _hist_view,
            "_load_profile_slide_signals": _load_profile_slide_signals,
            "_profile_slide_signals_for_quarter": _profile_slide_signals_for_quarter,
            "_filter_anf_quarterly_segment_actual_rows": _filter_anf_quarterly_segment_actual_rows,
            "_parse_quarter_from_filename": _parse_quarter_from_filename,
            "_parse_quarter_from_follow_text": _parse_quarter_from_follow_text,
            "_path_belongs_to_ticker": _path_belongs_to_ticker,
            "_path_cache_key": _path_cache_key,
            "_read_cached_doc_raw": _read_cached_doc_raw,
            "_read_cached_doc_text": _read_cached_doc_text,
            "_infer_cached_doc_quarter": _infer_cached_doc_quarter,
            "_slide_text_paths": _slide_text_paths,
            "_resolve_col": _resolve_col,
            "_source_rank": _source_rank,
            "_text_fragment_penalty": _text_fragment_penalty,
            "_timed_writer_substage": _timed_writer_substage,
            "_parse_gpre_crush_margin_pair_local": _parse_gpre_crush_margin_pair_local,
            "_extract_45z_2026_target_candidates": _extract_45z_2026_target_candidates,
            "_extract_45z_monetization_target_display": _extract_45z_monetization_target_display,
            "_extract_money_targets_for_display": _extract_money_targets_for_display,
            "_anf_compact_driver_label": _anf_compact_driver_label,
            "_anf_visible_quarter_label": _anf_visible_quarter_label,
            "derivative_oci_bridge_df": derivative_oci_bridge_df_runtime,
        }

    def _get_operating_drivers_support() -> OperatingDriversSupport:
        nonlocal operating_drivers_support
        runtime = _operating_drivers_support_runtime()
        if operating_drivers_support is None:
            operating_drivers_support = OperatingDriversSupport(
                OperatingDriversSupportDeps(runtime=runtime)
            )
        else:
            operating_drivers_support.refresh_runtime(runtime)
        return operating_drivers_support

    def _sync_operating_drivers_support_cache_state() -> None:
        if operating_drivers_support is not None:
            operating_drivers_support.sync_runtime_cache_state()

    def _operating_driver_quarters() -> List[date]:
        return _get_operating_drivers_support().operating_driver_quarters()

    def _driver_source_display(source_type: Any, source_doc: Any = "") -> str:
        return _get_operating_drivers_support().driver_source_display(source_type, source_doc)

    def _driver_source_note(source_doc: Any, snippet: Any = "", extra: Any = "") -> str:
        return _get_operating_drivers_support().driver_source_note(source_doc, snippet, extra)

    def _path_cache_key(path_in: Path) -> str:
        return source_path_cache_key(path_in)

    def _read_cached_doc_raw(path_in: Path) -> str:
        return source_read_cached_doc_raw(
            path_in,
            document_cache=document_cache,
            pdf_text_cache_root=pdf_text_cache_root,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
        )

    def _read_cached_doc_text(path_in: Path, *, normalize: bool = False) -> str:
        return source_read_cached_doc_text(
            path_in,
            document_cache=document_cache,
            pdf_text_cache_root=pdf_text_cache_root,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            quiet_pdf_warnings=quiet_pdf_warnings,
            normalize=normalize,
        )

    def _infer_cached_doc_quarter(
        path_in: Path,
        *,
        text: Any = None,
        latest_q_hint: Any = None,
        include_follow_text: bool = False,
    ) -> Optional[date]:
        return source_infer_cached_doc_quarter(
            path_in,
            document_cache=document_cache,
            parse_quarter_from_filename=_parse_quarter_from_filename,
            parse_quarter_from_follow_text=_parse_quarter_from_follow_text,
            text=text,
            latest_q_hint=latest_q_hint,
            include_follow_text=include_follow_text,
        )

    def _sec_docs_for_accession(accn_in: Any) -> List[Path]:
        return source_sec_docs_for_accession(
            accn_in,
            cache_root=cache_root,
            document_cache=document_cache,
        )

    def _ticker_specific_submission_path(path_in: Path) -> bool:
        if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
            return False
        ticker_token = str(profile_ticker or ticker or "").strip().upper()
        if not ticker_token:
            return True
        try:
            path_parts = {str(part).strip().upper() for part in Path(path_in).resolve().parts}
        except Exception:
            path_parts = {str(part).strip().upper() for part in Path(path_in).parts}
        has_ticker_specific_root = False
        if cache_dir is not None:
            try:
                cache_parts = {str(part).strip().upper() for part in Path(cache_dir).expanduser().resolve().parts}
            except Exception:
                cache_parts = {str(part).strip().upper() for part in Path(cache_dir).parts}
            if ticker_token in cache_parts:
                has_ticker_specific_root = True
        for root_in in ticker_roots:
            try:
                root_parts = {str(part).strip().upper() for part in Path(root_in).resolve().parts}
            except Exception:
                root_parts = {str(part).strip().upper() for part in Path(root_in).parts}
            if ticker_token in root_parts:
                has_ticker_specific_root = True
                break
        if ticker_token in path_parts:
            return True
        # If no ticker-specific root is known, preserve the legacy broad-root
        # behavior; otherwise reject submissions from sibling ticker caches.
        return not has_ticker_specific_root

    def _submission_cache_files(*, max_files: Optional[int] = None) -> List[Path]:
        return source_submission_cache_files(
            cache_roots=tuple(cache_roots),
            document_cache=document_cache,
            max_files=max_files,
            path_filter=_ticker_specific_submission_path,
        )

    def _submission_recent_row_quarter(row: Dict[str, Any]) -> Optional[date]:
        rep_d = parse_date(row.get("report"))
        if rep_d is not None:
            return rep_d if _is_quarter_end(rep_d) else _coerce_prev_quarter_end(rep_d)
        filed_d = parse_date(row.get("filed"))
        if filed_d is not None:
            return _coerce_prev_quarter_end(filed_d)
        return None

    def _submission_recent_rows(*, max_files: Optional[int] = None) -> List[Dict[str, Any]]:
        return source_submission_recent_rows(
            cache_roots=tuple(cache_roots),
            document_cache=document_cache,
            raw_reader=_read_cached_doc_raw,
            max_files=max_files,
            path_filter=_ticker_specific_submission_path,
        )

    def _resolve_cached_doc_path(
        *,
        accn: Any = "",
        doc_name: Any = "",
        path_hint: Any = "",
    ) -> Optional[Path]:
        return source_resolve_cached_doc_path(
            cache_roots=tuple(cache_roots),
            accession_doc_lookup=_sec_docs_for_accession,
            accn=accn,
            doc_name=doc_name,
            path_hint=path_hint,
        )

    valuation_precompute_support = ValuationPrecomputeSupport(
        ValuationPrecomputeDeps(
            runtime={
                **globals(),
                **locals(),
            }
        )
    )
    _load_buyback_auth_source_bundle = valuation_precompute_support.load_buyback_auth_source_bundle
    _buyback_auth_docs_for_accession = valuation_precompute_support.buyback_auth_docs_for_accession
    _extract_latest_buyback_remaining_from_sec = valuation_precompute_support.extract_latest_buyback_remaining_from_sec
    _extract_valuation_filing_doc_text = valuation_precompute_support.extract_valuation_filing_doc_text
    _docs_for_valuation_accn = valuation_precompute_support.docs_for_valuation_accn
    _load_valuation_filing_docs_by_quarter = valuation_precompute_support.load_filing_docs_by_quarter
    _extract_cap_alloc_text_maps_by_quarter = valuation_precompute_support.extract_cap_alloc_text_maps_by_quarter
    _analyze_cap_alloc_doc = valuation_precompute_support.analyze_cap_alloc_doc
    _extract_buyback_dividend_from_doc_index = valuation_precompute_support.extract_buyback_dividend_from_doc_index
    _ensure_valuation_precompute_bundle = valuation_precompute_support.ensure_precompute_bundle

    def _slide_text_paths(
        *,
        kind: str = "all",
        quarter: Optional[date] = None,
    ) -> List[Path]:
        return source_slide_text_paths(
            material_roots=tuple(material_roots),
            document_cache=document_cache,
            parse_quarter_from_filename=_parse_quarter_from_filename,
            kind=kind,
            quarter=quarter,
        )

    def _read_operating_driver_text(path_in: Path) -> str:
        return _get_operating_drivers_support().read_operating_driver_text(path_in)

    def _operating_driver_follow_source_dirs() -> List[Tuple[str, Path]]:
        return _get_operating_drivers_support().operating_driver_follow_source_dirs()

    def _operating_driver_financial_statement_files() -> List[Path]:
        return _get_operating_drivers_support().operating_driver_financial_statement_files()

    def _load_operating_driver_source_records() -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().load_source_records()

    def _load_operating_driver_source_records_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
        return _get_operating_drivers_support().load_source_records_by_quarter()

    def _load_operating_driver_line_index_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
        return _get_operating_drivers_support().load_line_index_by_quarter()

    def _load_operating_driver_flat_line_index() -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().load_flat_line_index()

    def _is_crush_margin_bridge_candidate(text_in: Any) -> bool:
        return _get_operating_drivers_support().is_crush_margin_bridge_candidate(text_in)

    def _parse_driver_number(token: Any) -> Optional[float]:
        return _get_operating_drivers_support().parse_driver_number(token)

    def _extract_driver_numeric_values(text_in: Any) -> List[float]:
        return _get_operating_drivers_support().extract_driver_numeric_values(text_in)

    def _get_crush_margin_bridge_details(text_in: Any) -> Dict[str, Any]:
        return _get_operating_drivers_support().get_crush_margin_bridge_details(text_in)

    def _prime_operating_driver_crush_detail_cache(records: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
        return _get_operating_drivers_support().prime_operating_driver_crush_detail_cache(records)

    def _load_operating_driver_template_index() -> Dict[str, Any]:
        return _get_operating_drivers_support().load_template_index()

    def _load_operating_driver_45z_guidance_docs_by_quarter() -> Dict[date, List[Dict[str, Any]]]:
        return _get_operating_drivers_support().load_45z_guidance_docs_by_quarter()

    def _operating_driver_template_spec(tpl: Any) -> Dict[str, Any]:
        return _get_operating_drivers_support().operating_driver_template_spec(tpl)

    def _candidate_records_for_template(
        qd: date,
        template_spec: Dict[str, Any],
        quarter_records: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().candidate_records_for_template(
            qd,
            template_spec,
            quarter_records=quarter_records,
        )

    def _load_operating_driver_bridge_bundle_map(quarter_set: List[date]) -> Dict[date, Dict[str, Any]]:
        return _get_operating_drivers_support().load_bridge_bundle_map(quarter_set)

    def _get_analysis_sheet_style_bundle() -> Dict[str, Any]:
        return _style_get_analysis_sheet_style_bundle(header_size=header_size, font_size=font_size)

    def _get_valuation_style_bundle() -> Dict[str, Any]:
        nonlocal valuation_style_bundle_cache
        ws_val = wb["Valuation"] if "Valuation" in wb.sheetnames else None
        if valuation_style_bundle_cache is not None and (
            valuation_style_bundle_cache.get("harvested") or ws_val is None
        ):
            if ctx_ref is not None:
                ctx_ref.derived.valuation_style_bundle = dict(valuation_style_bundle_cache)
            return valuation_style_bundle_cache

        analysis_theme = _get_analysis_sheet_style_bundle()
        bundle: Dict[str, Any] = {
            "header_fill": copy(analysis_theme["header_fill"]),
            "section_fill": copy(analysis_theme["section_fill"]),
            "valuation_soft_section_fill": PatternFill("solid", fgColor="D9E7F3"),
            "title_fill": copy(analysis_theme["title_fill"]),
            "input_fill": copy(analysis_theme["input_fill"]),
            "thin_border": copy(analysis_theme["thin_border"]),
            "bold_font": copy(analysis_theme["bold_font"]),
            "norm_font": copy(analysis_theme["norm_font"]),
            "valuation_quarter_style_a": None,
            "valuation_quarter_style_col": None,
            "valuation_actuals_style_col": None,
            "valuation_section_label_style": None,
            "valuation_section_col_style": None,
            "valuation_label_style": None,
            "valuation_numeric_style": None,
            "valuation_bucket_fills": {
                "neg_strong": PatternFill("solid", fgColor="A63A00"),
                "neg_mild": PatternFill("solid", fgColor="D55E00"),
                "neutral": PatternFill("solid", fgColor="DDDDDD"),
                "pos_mild": PatternFill("solid", fgColor="9BD3F5"),
                "pos_strong": PatternFill("solid", fgColor="2F80ED"),
            },
            "valuation_col_widths": {},
            "valuation_row_height_actual": None,
            "valuation_row_height_quarter": None,
            "valuation_data_font_size": float(font_size),
            "harvested": False,
        }
        if ws_val is not None:
            with _timed_writer_substage("write_excel.valuation.styles"):
                for key, col_idx in {
                    "neg_strong": 3,
                    "neg_mild": 4,
                    "neutral": 5,
                    "pos_mild": 6,
                    "pos_strong": 7,
                }.items():
                    bundle["valuation_bucket_fills"][key] = copy(ws_val.cell(row=1, column=col_idx).fill)
                val_quarter_row: Optional[int] = None
                for rr in range(1, ws_val.max_row + 1):
                    if str(ws_val.cell(row=rr, column=1).value or "").strip().lower() == "quarter":
                        val_quarter_row = rr
                        break
                if val_quarter_row is not None:
                    bundle["valuation_quarter_style_a"] = copy(ws_val.cell(row=val_quarter_row, column=1)._style)
                    bundle["valuation_quarter_style_col"] = copy(ws_val.cell(row=val_quarter_row, column=2)._style)
                    prev_r = max(1, val_quarter_row - 1)
                    bundle["valuation_actuals_style_col"] = copy(ws_val.cell(row=prev_r, column=2)._style)
                    bundle["valuation_row_height_actual"] = ws_val.row_dimensions[prev_r].height
                    bundle["valuation_row_height_quarter"] = ws_val.row_dimensions[val_quarter_row].height
                    for rr in range(val_quarter_row + 1, min(ws_val.max_row, val_quarter_row + 220) + 1):
                        c0 = ws_val.cell(row=rr, column=1)
                        c1 = ws_val.cell(row=rr, column=2)
                        if str(c0.value or "").strip() == "":
                            continue
                        if bool(c0.font and c0.font.bold):
                            continue
                        bundle["valuation_label_style"] = copy(c0._style)
                        bundle["valuation_numeric_style"] = copy(c1._style)
                        if c0.font and c0.font.size:
                            bundle["valuation_data_font_size"] = float(c0.font.size)
                        elif c1.font and c1.font.size:
                            bundle["valuation_data_font_size"] = float(c1.font.size)
                        break
                    if bundle["valuation_label_style"] is None:
                        first_data_row = val_quarter_row + 2
                        if first_data_row <= ws_val.max_row:
                            bundle["valuation_label_style"] = copy(ws_val.cell(row=first_data_row, column=1)._style)
                            bundle["valuation_numeric_style"] = copy(ws_val.cell(row=first_data_row, column=2)._style)
                            c0 = ws_val.cell(row=first_data_row, column=1)
                            if c0.font and c0.font.size:
                                bundle["valuation_data_font_size"] = float(c0.font.size)
                    for rr in range(val_quarter_row + 1, min(ws_val.max_row, val_quarter_row + 160) + 1):
                        c0 = ws_val.cell(row=rr, column=1)
                        if str(c0.value or "").strip() == "":
                            continue
                        if not bool(c0.font and c0.font.bold):
                            continue
                        if c0.fill is None or getattr(c0.fill, "fill_type", None) != "solid":
                            continue
                        bundle["valuation_section_label_style"] = copy(c0._style)
                        bundle["valuation_section_col_style"] = copy(ws_val.cell(row=rr, column=2)._style)
                        break
                    for cc in range(1, 40):
                        letter = get_column_letter(cc)
                        bundle["valuation_col_widths"][letter] = ws_val.column_dimensions[letter].width
                bundle["harvested"] = True
        valuation_style_bundle_cache = bundle
        if ctx_ref is not None:
            ctx_ref.derived.valuation_style_bundle = dict(bundle)
        return bundle

    def _ensure_valuation_render_bundle(qs_local: Tuple[pd.Timestamp, ...], leverage_df_local: Optional[pd.DataFrame]) -> Dict[str, Any]:
        nonlocal valuation_render_bundle_cache
        # The render bundle is the lighter, quarter-keyed valuation substrate. It
        # normalizes history/leverage inputs once and memoizes the reusable maps that
        # visible valuation rows and downstream QA/precompute logic consume.
        quarter_key = tuple(pd.Timestamp(q).normalize() for q in qs_local if pd.notna(q))
        if (
            valuation_render_bundle_cache is not None
            and tuple(valuation_render_bundle_cache.get("quarter_key") or ()) == quarter_key
        ):
            if ctx_ref is not None:
                ctx_ref.derived.valuation_render_bundle = valuation_render_bundle_cache
            return valuation_render_bundle_cache

        hist_indexed = pd.DataFrame()
        leverage_indexed = pd.DataFrame()
        with _timed_writer_substage("write_excel.valuation.bundle.index_sources"):
            if ctx_ref is not None and ctx_ref.derived.valuation_hist_indexed is not None:
                hist_indexed = ctx_ref.derived.valuation_hist_indexed
            elif hist is not None and not hist.empty and "quarter" in hist.columns:
                hist_local = _hist_view().copy()
                if "_quarter" in hist_local.columns:
                    hist_local["quarter"] = hist_local["_quarter"]
                hist_indexed = hist_local[hist_local["quarter"].notna()].drop_duplicates(subset=["quarter"], keep="last").set_index("quarter")

            if leverage_df_local is not None and not leverage_df_local.empty and "quarter" in leverage_df_local.columns:
                lev_local = leverage_df_local.copy()
                lev_local["quarter"] = pd.to_datetime(lev_local["quarter"], errors="coerce")
                leverage_indexed = lev_local[lev_local["quarter"].notna()].drop_duplicates(subset=["quarter"], keep="last").set_index("quarter")
        quarter_index_map = {pd.Timestamp(q): idx for idx, q in enumerate(quarter_key)}
        last4_quarters_map: Dict[pd.Timestamp, Tuple[pd.Timestamp, ...]] = {}
        for idx, q in enumerate(quarter_key):
            if idx < 3:
                continue
            last4_quarters_map[pd.Timestamp(q)] = tuple(pd.Timestamp(v) for v in quarter_key[idx - 3 : idx + 1])

        def _series_map(df_in: pd.DataFrame, col: Optional[str]) -> Dict[pd.Timestamp, Any]:
            if df_in is None or df_in.empty or not col or col not in df_in.columns:
                return {}
            ser = pd.to_numeric(df_in[col], errors="coerce")
            return {pd.Timestamp(k): (float(v) if pd.notna(v) else None) for k, v in ser.items()}

        def _first_existing_numeric_col(df_in: pd.DataFrame, candidates: List[str]) -> Optional[str]:
            if df_in is None or df_in.empty:
                return None
            cols_lc = {str(c).strip().lower(): c for c in df_in.columns}
            for cand in candidates:
                resolved = cols_lc.get(str(cand).strip().lower())
                if resolved is None:
                    continue
                ser = pd.to_numeric(df_in[resolved], errors="coerce")
                if ser.notna().any():
                    return str(resolved)
            return None

        def _normalize_cash_outflow_sign(src: Dict[pd.Timestamp, Any]) -> Dict[pd.Timestamp, Any]:
            if not src:
                return src
            vals = [float(v) for v in src.values() if v is not None and pd.notna(v)]
            if not vals:
                return src
            neg = sum(1 for v in vals if v < 0)
            pos = sum(1 for v in vals if v > 0)
            if neg > pos:
                return {k: (-float(v) if v is not None and pd.notna(v) else None) for k, v in src.items()}
            return src

        with _timed_writer_substage("write_excel.valuation.bundle.local_bs_payloads"):
            # Local balance-sheet payloads are a narrow rescue path for goodwill and
            # intangibles when GAAP history does not carry enough quarter detail.
            goodwill_map = _series_map(hist_indexed, "goodwill")
            intangibles_map = _series_map(hist_indexed, "intangibles")
            for qv in quarter_key:
                gw_hist = goodwill_map.get(qv)
                if gw_hist is not None and abs(float(gw_hist)) < 1_000_000.0:
                    goodwill_map[qv] = None
                int_hist = intangibles_map.get(qv)
                if int_hist is not None and abs(float(int_hist)) < 1_000_000.0:
                    intangibles_map[qv] = None
            valuation_bs_payloads = _shared_load_local_balance_sheet_detail_payloads({q.date() for q in quarter_key})
            for qv in quarter_key:
                if goodwill_map.get(qv) is None:
                    payload_vals = (valuation_bs_payloads.get(qv.date()) or {}).get("values", {}) or {}
                    gw_val = payload_vals.get("goodwill")
                    if gw_val is not None:
                        goodwill_map[qv] = float(gw_val)
                if intangibles_map.get(qv) is None:
                    payload_vals = (valuation_bs_payloads.get(qv.date()) or {}).get("values", {}) or {}
                    int_val = payload_vals.get("intangibles")
                    if int_val is not None:
                        intangibles_map[qv] = float(int_val)
            goodwill_map = _carry_forward_low_change_series(goodwill_map, list(quarter_key))
            intangibles_map = _carry_forward_low_change_series(intangibles_map, list(quarter_key))

        with _timed_writer_substage("write_excel.valuation.bundle.return_capital_maps"):
            # These maps are the fast GAAP/facts-side capital-return baseline. The
            # heavier precompute bundle can later refine them with document-derived
            # execution evidence, but this bundle is the first pass.
            buyback_col = _first_existing_numeric_col(
                hist_indexed,
                [
                    "buybacks_cash",
                    "buybacks",
                    "share_repurchases",
                    "repurchase_of_common_stock",
                    "repurchases_of_common_stock",
                    "payments_for_repurchase_of_common_stock",
                    "treasury_stock_acquired",
                    "common_stock_repurchased",
                ],
            )
            dividend_col = _first_existing_numeric_col(
                hist_indexed,
                [
                    "dividends_cash",
                    "common_stock_dividends_paid",
                    "payments_of_dividends_common_stock",
                ],
            )
            buyback_map = _normalize_cash_outflow_sign(_series_map(hist_indexed, buyback_col)) if buyback_col else {}
            dividend_map = _normalize_cash_outflow_sign(_series_map(hist_indexed, dividend_col)) if dividend_col else {}

            buyback_shares_q_map: Dict[pd.Timestamp, Any] = {}
            shares_out_map = _series_map(hist_indexed, "shares_outstanding")
            for idx_q, qv in enumerate(quarter_key):
                if idx_q == 0:
                    buyback_shares_q_map[qv] = None
                    continue
                prev_q = quarter_key[idx_q - 1]
                sh_now = shares_out_map.get(qv)
                sh_prev = shares_out_map.get(prev_q)
                buyback_shares_q_map[qv] = (float(sh_prev) - float(sh_now)) if sh_now is not None and sh_prev is not None else None

        valuation_render_bundle_cache = {
            "quarter_key": quarter_key,
            "quarter_index_map": quarter_index_map,
            "last4_quarters_map": last4_quarters_map,
            "hist_indexed": hist_indexed,
            "leverage_indexed": leverage_indexed,
            "rev_map": _series_map(hist_indexed, "revenue"),
            "gross_profit_map": _series_map(hist_indexed, "gross_profit"),
            "ebitda_map": _series_map(hist_indexed, "ebitda"),
            "ebit_map": _series_map(hist_indexed, "op_income"),
            "net_income_map": _series_map(hist_indexed, "net_income"),
            "cfo_map": _series_map(hist_indexed, "cfo"),
            "capex_map": _series_map(hist_indexed, "capex"),
            "price_map": _series_map(hist_indexed, "price"),
            "market_cap_map": _series_map(hist_indexed, "market_cap"),
            "int_paid_map": _series_map(hist_indexed, "interest_paid"),
            "tax_paid_map": _series_map(hist_indexed, "tax_paid"),
            "cash_map": _series_map(hist_indexed, "cash"),
            "total_debt_map": _series_map(hist_indexed, "total_debt"),
            "debt_current_map": _series_map(hist_indexed, "debt_current"),
            "debt_core_map": _series_map(hist_indexed, "debt_core"),
            "shares_map": _series_map(hist_indexed, "shares_diluted"),
            "shares_out_map": shares_out_map,
            "total_equity_map": _series_map(hist_indexed, "total_equity"),
            "goodwill_map": goodwill_map,
            "intangibles_map": intangibles_map,
            "pension_map": _series_map(hist_indexed, "pension_obligation_net"),
            "assets_map": _series_map(hist_indexed, "assets"),
            "liabilities_map": _series_map(hist_indexed, "liabilities"),
            "assets_current_map": _series_map(hist_indexed, "assets_current"),
            "liabilities_current_map": _series_map(hist_indexed, "liabilities_current"),
            "ar_map": _series_map(hist_indexed, "accounts_receivable"),
            "inventory_map": _series_map(hist_indexed, "inventory"),
            "sti_map": _series_map(hist_indexed, "short_term_investments"),
            "rd_map": _series_map(hist_indexed, "research_and_development"),
            "acquisitions_map": _normalize_cash_outflow_sign(_series_map(hist_indexed, "acquisitions_cash")),
            "debt_repay_map": _normalize_cash_outflow_sign(_series_map(hist_indexed, "debt_repayment")),
            "debt_issuance_map": _series_map(hist_indexed, "debt_issuance"),
            "ebitda_ttm_map": _series_map(leverage_indexed, "ebitda_ttm"),
            "net_lev_map": _series_map(leverage_indexed, "corporate_net_leverage"),
            "cov_pnl_map": _series_map(leverage_indexed, "interest_coverage_pnl"),
            "rev_commit_map": _series_map(leverage_indexed, "revolver_commitment"),
            "rev_facility_map": _series_map(leverage_indexed, "revolver_facility_size"),
            "rev_drawn_map": _series_map(leverage_indexed, "revolver_drawn"),
            "rev_lc_map": _series_map(leverage_indexed, "revolver_letters_of_credit"),
            "rev_avail_map": _series_map(leverage_indexed, "revolver_availability"),
            "liquidity_map": _series_map(leverage_indexed, "liquidity"),
            "int_paid_ttm_map": _series_map(leverage_indexed, "interest_paid_ttm"),
            "buyback_map": buyback_map,
            "dividend_map": dividend_map,
            "buyback_cash_facts_map": dict(buyback_map),
            "dividend_cash_facts_map": dict(dividend_map),
            "buyback_shares_q_map": buyback_shares_q_map,
        }
        if ctx_ref is not None:
            ctx_ref.derived.valuation_render_bundle = valuation_render_bundle_cache
        return valuation_render_bundle_cache

    def _parse_threshold_amount_m(text_in: Any) -> Optional[float]:
        return _get_operating_drivers_support().parse_threshold_amount_m(text_in)

    def _parse_45z_realized_value_m(text_in: Any) -> Optional[float]:
        return _get_operating_drivers_support().parse_45z_realized_value_m(text_in)

    def _merge_driver_rows(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
        return _get_operating_drivers_support().merge_driver_rows(existing, candidate)

    def _make_driver_row(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _get_operating_drivers_support().make_driver_row(*args, **kwargs)

    def _build_anf_operating_driver_rows() -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().build_anf_operating_driver_rows()

    def _gpre_canonical_crush_series_for_drivers_local() -> Dict[date, Dict[str, Any]]:
        return _get_operating_drivers_support().gpre_canonical_crush_series_for_drivers_local()

    def _extract_operating_driver_rows_for_template(
        qd: date,
        tpl: Any,
        *,
        quarter_records: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().extract_rows_for_template(
            qd,
            tpl,
            quarter_records=quarter_records,
        )

    def _format_operating_driver_delta(current_val: Any, prior_val: Any, unit: str) -> str:
        return _get_operating_drivers_support().format_operating_driver_delta(current_val, prior_val, unit)

    def _build_operating_drivers_history_rows() -> List[Dict[str, Any]]:
        return _get_operating_drivers_support().build_operating_drivers_history_rows()

    def _driver_unit_label(unit_txt: Any) -> str:
        return _get_operating_drivers_support().driver_unit_label(unit_txt)

    def _driver_row_label(driver_label: Any, unit_txt: Any) -> str:
        return _get_operating_drivers_support().driver_row_label(driver_label, unit_txt)

    def _truncate_driver_text(txt: Any, max_chars: int = 96) -> str:
        return _get_operating_drivers_support().truncate_driver_text(txt, max_chars)

    def _operating_driver_order_map(templates_in: List[Any]) -> Dict[str, int]:
        return _get_operating_drivers_support().operating_driver_order_map(templates_in)

    def _quarter_label_short(qd: Optional[date]) -> str:
        if not isinstance(qd, date):
            return ""
        if is_anf_profile:
            label = _anf_visible_quarter_label(qd)
            if label:
                return label
        return f"{qd.year}-Q{((qd.month - 1) // 3) + 1}"

    def _build_economics_market_rows() -> List[Dict[str, Any]]:
        deps = EconomicsMarketRowsDeps(
            cache_dir=cache_dir,
            ticker=ticker,
            company_profile=company_profile,
            first_existing_material_dir=_first_existing_material_dir,
            load_market_export_rows=load_market_export_rows,
        )
        return build_economics_market_rows(deps)

    def _write_operating_drivers_raw_sheet(rows: List[Dict[str, Any]]) -> None:
        ws = wb.create_sheet("operating_drivers_raw")
        local_header_fill = PatternFill("solid", fgColor="F2F2F2")
        local_thin_border = Border(
            left=Side(style="thin", color="BFBFBF"),
            right=Side(style="thin", color="BFBFBF"),
            top=Side(style="thin", color="BFBFBF"),
            bottom=Side(style="thin", color="BFBFBF"),
        )
        headers = [
            "Quarter",
            "Driver group",
            "Driver",
            "Value",
            "Unit",
            "QoQ change",
            "YoY change",
            "Source",
            "Commentary",
            "Quality",
        ]
        if not rows:
            ws["A1"] = "No operating-driver history available."
            return
        ws.append(headers)
        for cc, header in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=cc, value=header)
            cell.font = Font(bold=True, size=header_size)
            cell.fill = local_header_fill
            cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
            cell.border = local_thin_border
        col_widths = {
            "A": 14,
            "B": 28,
            "C": 30,
            "D": 14,
            "E": 12,
            "F": 14,
            "G": 14,
            "H": 18,
            "I": 56,
            "J": 14,
        }
        for letter, width in col_widths.items():
            ws.column_dimensions[letter].width = width
        for row_idx, rec in enumerate(rows, start=2):
            for col_idx, header in enumerate(headers, start=1):
                value = rec.get(header)
                if isinstance(value, str):
                    value = ILLEGAL_CHARACTERS_RE.sub("", value)
                elif value is not None:
                    try:
                        value = _safe_cell(value)
                    except Exception:
                        pass
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                cell.border = local_thin_border
                if header == "Commentary":
                    cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
                else:
                    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
            quarter_cell = ws.cell(row=row_idx, column=1)
            quarter_cell.number_format = "yyyy-mm-dd"
            value_cell = ws.cell(row=row_idx, column=4)
            unit_txt = str(rec.get("Unit") or "")
            if pd.notna(pd.to_numeric(rec.get("Value"), errors="coerce")):
                if unit_txt == "%":
                    value_cell.number_format = "0.0"
                elif unit_txt in {"$m", "m gallons", "m lbs", "m bushels", "k tons"}:
                    value_cell.number_format = "#,##0.0"
                else:
                    value_cell.number_format = "#,##0.000"
            src_note = str(rec.get("_source_note") or "").strip()
            if src_note:
                try:
                    _set_cell_comment_local(ws.cell(row=row_idx, column=8), src_note)
                except Exception:
                    pass
            commentary = str(rec.get("Commentary") or "").strip()
            if commentary:
                ws.row_dimensions[row_idx].height = _estimate_wrapped_row_height(
                    commentary,
                    float(col_widths["I"]),
                    18,
                    12,
                    min_lines=1,
                    max_lines=5,
                )
            else:
                ws.row_dimensions[row_idx].height = 18
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:J{ws.max_row}"
        ws.sheet_format.defaultRowHeight = 18
        ws.sheet_view.zoomScale = 110

    def _write_economics_market_raw_sheet(rows: List[Dict[str, Any]]) -> None:
        deps = EconomicsMarketRawWriterDeps(
            wb=wb,
            header_size=header_size,
            safe_cell=_safe_cell,
            estimate_wrapped_row_height=_estimate_wrapped_row_height,
        )
        write_economics_market_raw_sheet(deps, rows)

    def _write_operating_drivers_sheet(rows: List[Dict[str, Any]]) -> None:
        deps = OperatingDriversWriterDeps(
            wb=wb,
            hist=hist,
            ticker=ticker,
            company_profile=company_profile,
            slides_segments=slides_segments,
            slides_guidance=slides_guidance,
            quarter_notes=quarter_notes,
            derivative_oci_bridge_df=derivative_oci_bridge_df,
            material_roots=material_roots,
            font_size=font_size,
            header_size=header_size,
            is_pbi_profile=is_pbi_profile,
            is_gpre_profile=is_gpre_profile,
            is_anf_profile=is_anf_profile,
            enable_quarterly_segment_block=enable_quarterly_segment_block,
            annual_segment_alias_patterns=annual_segment_alias_patterns,
            anf_segment_brand_explanation=ANF_SEGMENT_BRAND_EXPLANATION,
            get_valuation_style_bundle=_get_valuation_style_bundle,
            get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
            operating_driver_quarters=_operating_driver_quarters,
            load_operating_driver_template_index=_load_operating_driver_template_index,
            load_operating_driver_source_records_by_quarter=_load_operating_driver_source_records_by_quarter,
            load_operating_driver_flat_line_index=_load_operating_driver_flat_line_index,
            first_existing_material_dir=_first_existing_material_dir,
            parse_quarter_from_filename=_parse_quarter_from_filename,
            parse_quarter_from_follow_text=_parse_quarter_from_follow_text,
            read_operating_driver_text=_read_operating_driver_text,
            set_cell_comment=_set_cell_comment_local,
            driver_source_note=_driver_source_note,
            driver_row_label=_driver_row_label,
            truncate_driver_text=_truncate_driver_text,
            quarter_label_short=_quarter_label_short,
            source_rank=_source_rank,
            text_fragment_penalty=_text_fragment_penalty,
            ensure_terminal_period=_ensure_terminal_period,
            gpre_commercial_setup_records_shared=_gpre_commercial_setup_records_shared,
            anf_clean_visible_operating_driver_records=_anf_clean_visible_operating_driver_records,
            anf_clean_visible_ui_text=_anf_clean_visible_ui_text,
            anf_compact_driver_group=_anf_compact_driver_group,
            anf_compact_driver_label=_anf_compact_driver_label,
            anf_recent_operating_commentary_rows=_anf_recent_operating_commentary_rows,
            anf_round_visible_driver_value=_anf_round_visible_driver_value,
            anf_visible_quarter_label=_anf_visible_quarter_label,
            sector_operating_driver_intro_tables=_sector_operating_driver_intro_tables,
        )
        write_operating_drivers_sheet(deps, rows)



    def _write_economics_overlay_sheet(rows: List[Dict[str, Any]]) -> None:
        return write_economics_overlay_sheet(
            EconomicsOverlayOrchestratorDeps(
                BasisProxySandboxWriterDeps=BasisProxySandboxWriterDeps,
                EconomicsOverlayChartWriterDeps=EconomicsOverlayChartWriterDeps,
                EconomicsOverlayMarketStateDeps=EconomicsOverlayMarketStateDeps,
                EconomicsOverlaySourceSupport=EconomicsOverlaySourceSupport,
                EconomicsOverlaySourceSupportDeps=EconomicsOverlaySourceSupportDeps,
                GpreEconomicsOverlayBridgeDeps=GpreEconomicsOverlayBridgeDeps,
                GpreEconomicsOverlayCommercialDeps=GpreEconomicsOverlayCommercialDeps,
                GpreEconomicsOverlayCoproductDeps=GpreEconomicsOverlayCoproductDeps,
                GpreEconomicsOverlayCurrentQtdDeps=GpreEconomicsOverlayCurrentQtdDeps,
                GpreEconomicsOverlayDerivativeSideEffectDeps=GpreEconomicsOverlayDerivativeSideEffectDeps,
                GpreEconomicsOverlayInputRowsDeps=GpreEconomicsOverlayInputRowsDeps,
                GpreOverlayQuarterComparisonDeps=GpreOverlayQuarterComparisonDeps,
                GpreOverlaySupportInputs=GpreOverlaySupportInputs,
                _apply_chart_text_categories=_apply_chart_text_categories,
                _convert_market_price_value=_convert_market_price_value,
                _driver_source_display=_driver_source_display,
                _driver_source_note=_driver_source_note,
                _economics_market_region_tags=_economics_market_region_tags,
                _ensure_terminal_period=_ensure_terminal_period,
                _estimate_wrapped_row_height=_estimate_wrapped_row_height,
                _extract_operating_driver_rows_for_template=_extract_operating_driver_rows_for_template,
                _get_analysis_sheet_style_bundle=_get_analysis_sheet_style_bundle,
                _gpre_commercial_setup_records_shared=_gpre_commercial_setup_records_shared,
                _gpre_parse_snapshot_date_like=_gpre_parse_snapshot_date_like,
                _load_operating_driver_bridge_bundle_map=_load_operating_driver_bridge_bundle_map,
                _load_operating_driver_flat_line_index=_load_operating_driver_flat_line_index,
                _load_operating_driver_source_records_by_quarter=_load_operating_driver_source_records_by_quarter,
                _load_operating_driver_template_index=_load_operating_driver_template_index,
                _operating_driver_quarters=_operating_driver_quarters,
                _overlay_model_label=_overlay_model_label,
                _parse_driver_number=_parse_driver_number,
                _quarter_label_short=_quarter_label_short,
                _record_writer_substage=_record_writer_substage,
                _set_cell_comment_local=_set_cell_comment_local,
                _text_fragment_penalty=_text_fragment_penalty,
                _truncate_driver_text=_truncate_driver_text,
                _write_derivative_crush_tests_sheet=_write_derivative_crush_tests_sheet,
                build_current_qtd_simple_crush_snapshot=build_current_qtd_simple_crush_snapshot,
                build_derivative_crush_tests=build_derivative_crush_tests,
                build_economics_overlay_market_state=build_economics_overlay_market_state,
                build_gpre_basis_proxy_model=build_gpre_basis_proxy_model,
                build_gpre_official_proxy_history_series=build_gpre_official_proxy_history_series,
                build_gpre_official_proxy_snapshot=build_gpre_official_proxy_snapshot,
                build_gpre_overlay_proxy_preview_bundle=build_gpre_overlay_proxy_preview_bundle,
                build_gpre_plant_capacity_history=build_gpre_plant_capacity_history,
                build_next_quarter_thesis_snapshot=build_next_quarter_thesis_snapshot,
                build_prior_quarter_simple_crush_snapshot=build_prior_quarter_simple_crush_snapshot,
                build_simple_crush_history_series=build_simple_crush_history_series,
                cache_dir=cache_dir,
                company_profile=company_profile,
                data_root_from_sec_cache_path=data_root_from_sec_cache_path,
                derivative_oci_bridge_df=derivative_oci_bridge_df,
                derivative_oci_exposure_df=derivative_oci_exposure_df,
                economics_market_rows=economics_market_rows,
                fetch_gpre_corn_bids_snapshot=fetch_gpre_corn_bids_snapshot,
                font_size=font_size,
                glx_normalize_text=glx_normalize_text,
                header_size=header_size,
                info_log=info_log,
                is_gpre_profile=is_gpre_profile,
                is_pbi_profile=is_pbi_profile,
                load_or_download_gpre_corn_bids_snapshot=load_or_download_gpre_corn_bids_snapshot,
                market_build_gpre_proxy_implied_results_bundle=market_build_gpre_proxy_implied_results_bundle,
                market_gpre_phase_preview_story=market_gpre_phase_preview_story,
                market_input_fingerprint=market_input_fingerprint,
                operating_driver_history_rows=operating_driver_history_rows,
                persist_gpre_frozen_thesis_snapshot=persist_gpre_frozen_thesis_snapshot,
                qn_is_complete_signal_text=qn_is_complete_signal_text,
                resolve_gpre_quarter_open_snapshot=resolve_gpre_quarter_open_snapshot,
                state=state,
                ticker=ticker,
                ticker_roots=ticker_roots,
                wb=wb,
                write_basis_proxy_sandbox_sheet=write_basis_proxy_sandbox_sheet,
                write_economics_overlay_charts=write_economics_overlay_charts,
                write_gpre_basis_proxy_overlay_support=write_gpre_basis_proxy_overlay_support,
                write_gpre_derivative_crush_tests_side_effect=write_gpre_derivative_crush_tests_side_effect,
                write_gpre_economics_overlay_bridge_to_reported_section=write_gpre_economics_overlay_bridge_to_reported_section,
                write_gpre_economics_overlay_commercial_sections=write_gpre_economics_overlay_commercial_sections,
                write_gpre_economics_overlay_coproduct_section=write_gpre_economics_overlay_coproduct_section,
                write_gpre_economics_overlay_current_qtd_section=write_gpre_economics_overlay_current_qtd_section,
                write_gpre_economics_overlay_input_rows=write_gpre_economics_overlay_input_rows,
                write_gpre_overlay_quarter_comparisons=write_gpre_overlay_quarter_comparisons,
            ),
            rows,
        )

    def _write_anf_investment_case_surfaces() -> pd.DataFrame:
        if not is_anf_profile:
            return pd.DataFrame()
        guidance_for_case = slides_guidance
        try:
            guidance_for_case = _anf_visible_guidance_normalized_frame(guidance_for_case)
        except Exception:
            guidance_for_case = slides_guidance
        case_data = _anf_build_investment_case_data(
            hist=hist,
            operating_driver_rows=operating_driver_history_rows,
            guidance_normalized=guidance_for_case,
            slides_segments=slides_segments,
            valuation_summary=valuation_summary_df,
            adjusted_metrics=adj_metrics,
        )
        _write_anf_investment_case_sheet(wb, case_data)
        _write_anf_investment_case_data_sheet(wb, case_data)
        return case_data

    def _write_investment_case_surfaces() -> pd.DataFrame:
        ticker_txt = str(ticker or "").strip().upper()
        if is_anf_profile:
            return _write_anf_investment_case_surfaces()
        if ticker_txt not in {"PBI", "GPRE"}:
            return pd.DataFrame()
        guidance_for_case = slides_guidance
        try:
            from .excel_writer_ui import _shared_guidance_normalized_frame

            guidance_for_case = _shared_guidance_normalized_frame(guidance_for_case)
        except Exception:
            guidance_for_case = slides_guidance
        case_data = _sector_build_investment_case_data(
            ticker=ticker_txt,
            hist=hist,
            operating_driver_rows=operating_driver_history_rows,
            guidance_normalized=guidance_for_case,
            valuation_summary=valuation_summary_df,
            economics_market_rows=economics_market_rows,
            slides_segments=slides_segments,
        )
        _write_sector_investment_case_sheet(wb, ticker_txt, case_data)
        _write_sector_investment_case_data_sheet(wb, ticker_txt, case_data)
        return case_data

    def _write_quarter_narrative_data_surface() -> None:
        return _write_quarter_narrative_data_surface_impl(_quarter_notes_context_adapter_deps())

    def _write_quarter_notes_narrative_ui_surface() -> None:
        return _write_quarter_notes_narrative_ui_surface_impl(_quarter_notes_context_adapter_deps())

    enable_derivative_oci_bridge_sheet = bool(
        (is_gpre_profile or bool(getattr(company_profile, "enable_derivative_oci_bridge", False)))
        and not is_pbi_profile
    )
    # The accounting bridge belongs next to Promise_Progress_UI, while the
    # crush-testing surface sits after Basis_Proxy_Sandbox because it consumes
    # that sheet's market/proxy quarterly frame. PBI stays excluded until it has
    # an explicit derivative/OCI bridge use case.
    derivative_sheet_order_slot = ("Derivative_OCI_Bridge",) if enable_derivative_oci_bridge_sheet else tuple()
    derivative_crush_tests_order_slot = ("Derivative_Crush_Tests",) if (enable_derivative_oci_bridge_sheet and is_gpre_profile) else tuple()
    desired_sheet_order = (
        "SUMMARY",
        "Valuation",
        "BS_Segments",
        "Operating_Drivers",
        "Economics_Overlay",
        "Quarter_Notes_UI",
        "Promise_Progress_UI",
        *derivative_sheet_order_slot,
        "Basis_Proxy_Sandbox",
        *derivative_crush_tests_order_slot,
        "Hidden_Value_Flags",
        "Revolver_History",
        "Debt_Tranches_Latest",
        "Debt_Profile",
        "Debt_Maturity_Ladder",
        "Debt_Buckets",
        "Debt_Recon",
        "Debt_Tranches_Q",
        "Debt_Credit_Notes",
        "Leverage_Liquidity",
        "REPORT_IS_Q",
        "REPORT_BS_Q",
        "REPORT_CF_Q",
        "Quarter_Notes",
        "Quarter_Notes_Evidence",
        "Quarter_Narrative_Data",
        "Quarter_Notes_Audit",
    )
    raw_sheet_cluster = ("History_Q", "operating_drivers_raw", "economics_market_raw")
    desired_sheet_order, raw_sheet_cluster = _investment_case_sheet_order(
        desired_sheet_order,
        raw_sheet_cluster,
        ticker=ticker,
    )
    runtime_cache.valuation_style_bundle_cache = valuation_style_bundle_cache
    runtime_cache.valuation_render_bundle_cache = valuation_render_bundle_cache
    runtime_cache.valuation_precompute_bundle_cache = valuation_precompute_support.valuation_precompute_bundle_cache
    runtime_cache.valuation_filing_docs_by_quarter_cache = valuation_precompute_support.valuation_filing_docs_by_quarter_cache
    _sync_operating_drivers_support_cache_state()
    _refresh_profile_signal_runtime({**globals(), **locals()})
    operating_drivers_runtime.profile_slide_signals_cache = profile_slide_signals_cache
    operating_drivers_runtime.profile_slide_signals_by_quarter_cache = profile_slide_signals_by_quarter_cache
    runtime_cache.adj_net_leverage_text_map_cache = adj_net_leverage_text_map_cache
    runtime_cache.leverage_local_material_index_cache = leverage_local_material_index_cache
    runtime_cache.leverage_audit_doc_index_cache = leverage_audit_doc_index_cache
    runtime_cache.promise_progress_ui_bundle_cache = promise_progress_ui_bundle_cache
    runtime_cache.valuation_buyback_auth_source_bundle_cache = valuation_precompute_support.valuation_buyback_auth_source_bundle_cache
    derivative_oci_bridge_df = pd.DataFrame()
    derivative_oci_qa_df = pd.DataFrame()
    derivative_oci_exposure_df = pd.DataFrame()
    if enable_derivative_oci_bridge_sheet:
        try:
            derivative_oci_result = build_derivative_oci_bridge_from_sources(
                str(ticker or ""),
                hist=hist if isinstance(hist, pd.DataFrame) else None,
                adj_metrics=adj_metrics if isinstance(adj_metrics, pd.DataFrame) else None,
            )
            derivative_oci_bridge_df = derivative_oci_result.rows
            derivative_oci_qa_df = derivative_oci_result.qa_rows
            derivative_oci_exposure_df = derivative_oci_result.exposure_rows
        except Exception as exc:
            derivative_oci_qa_df = pd.DataFrame(
                [
                    {
                        "quarter": pd.NaT,
                        "metric": "Derivative & OCI Bridge",
                        "severity": "warn",
                        "message": f"Derivative/OCI bridge extraction failed; workbook actuals were left unchanged. Error: {exc}",
                        "source": "derivative_oci_bridge",
                        "issue_family": "derivative_disclosure_method",
                        "recommended_action": "review derivative/OCI source parsing",
                        "raw_metric": "derivative_notes",
                    }
                ]
            )
    runtime_data = WriterRuntimeData(
        out_path=out_path,
        ticker=ticker,
        excel_mode=excel_mode,
        profile_timings=profile_timings,
        quarter_notes_audit=quarter_notes_audit,
        enable_operating_drivers_sheet=enable_operating_drivers_sheet,
        enable_economics_overlay_sheet=enable_economics_overlay_sheet,
        enable_economics_market_raw_sheet=enable_economics_market_raw_sheet,
        operating_driver_history_rows=operating_driver_history_rows,
        economics_market_rows=economics_market_rows,
        qa_checks=qa_checks if isinstance(qa_checks, pd.DataFrame) else pd.DataFrame(),
        info_log=info_log if isinstance(info_log, pd.DataFrame) else pd.DataFrame(),
        data_is_rules_df=data_is_rules_df if isinstance(data_is_rules_df, pd.DataFrame) else pd.DataFrame(),
        doc_cache=document_cache,
        frame_view_cache=frame_view_cache,
        runtime_cache=runtime_cache,
        extra_values={
            "leverage_df": leverage_df,
            "valuation_summary_df": valuation_summary_df,
            "valuation_grid_df": valuation_grid_df,
            "summary_df": summary_df,
            "signals_base_df": signals_base_df,
            "flags_df": flags_df,
            "flags_audit_df": flags_audit_df,
            "flags_recompute_df": flags_recompute_df,
            "ng_bridge": ng_bridge,
            "ng_bridge_relaxed": ng_bridge_relaxed,
            "report_is": pd.DataFrame(),
            "report_bs": pd.DataFrame(),
            "report_cf": pd.DataFrame(),
            "facts_long": pd.DataFrame(),
            "lineitem_map": pd.DataFrame(),
            "period_index": pd.DataFrame(),
            "quarter_notes_evidence_df": pd.DataFrame(),
            "promise_evidence_df": pd.DataFrame(),
            "derivative_oci_bridge_df": derivative_oci_bridge_df,
            "derivative_oci_qa_df": derivative_oci_qa_df,
            "derivative_oci_exposure_df": derivative_oci_exposure_df,
            "company_profile": company_profile,
            "font_size": font_size,
            "header_size": header_size,
        },
    )
    callbacks = WriterCallbacks(
        write_sheet=_write_sheet,
        write_flags_sheet=_write_flags_sheet,
        write_report_sheet=_write_report_sheet,
        write_summary_sheet=_write_summary_sheet,
        write_valuation_sheet=_write_valuation_sheet,
        write_bs_segments_sheet=_write_bs_segments_sheet,
        write_quarter_notes_ui_v2=_write_quarter_notes_ui_v2,
        write_promise_tracker_ui_v2=_write_promise_tracker_ui_v2,
        write_promise_progress_ui_v2=_write_promise_progress_ui_v2,
        write_operating_drivers_sheet=_write_operating_drivers_sheet,
        write_economics_overlay_sheet=_write_economics_overlay_sheet,
        write_operating_drivers_raw_sheet=_write_operating_drivers_raw_sheet,
        write_economics_market_raw_sheet=_write_economics_market_raw_sheet,
        build_report=_build_report,
        build_summary=_build_summary,
        build_facts_long=_build_facts_long,
        build_lineitem_map=_build_lineitem_map,
        build_period_index=_build_period_index,
        build_ng_bridge=_build_ng_bridge,
        build_qn_evidence_src=_build_qn_evidence_src,
        build_promise_evidence_src=_build_promise_evidence_src,
        extract_adj_net_leverage_text_map=_extract_adj_net_leverage_text_map,
        build_hidden_value_flags_fallback=_build_hidden_value_flags_fallback,
        load_operating_driver_source_records=_load_operating_driver_source_records,
        load_operating_driver_source_records_by_quarter=_load_operating_driver_source_records_by_quarter,
        prime_operating_driver_crush_detail_cache=_prime_operating_driver_crush_detail_cache,
        build_operating_drivers_history_rows=_build_operating_drivers_history_rows,
        build_economics_market_rows=_build_economics_market_rows,
        run_latest_quarter_qa=_run_latest_quarter_qa,
        extra_callbacks={
            "_ui_state": ui_state,
            "_slide_text_paths": _slide_text_paths,
            "_pbi_slide_pages_for_qd": _pbi_slide_pages_for_qd,
            "_latest_quarter_qa_source_bundle": _latest_quarter_qa_source_bundle,
            "_latest_quarter_sec_text_corpus": _latest_quarter_sec_text_corpus,
            "_local_slide_driver_fallback": _local_slide_driver_fallback,
            "_load_profile_slide_signals": _load_profile_slide_signals,
            "_quarter_notes_view": _quarter_notes_view,
            "_promises_view": _promises_view,
            "_audit_view": _audit_view,
            "_hist_view": _hist_view,
            "_adj_metrics_view": _adj_metrics_view,
            "_load_operating_driver_line_index_by_quarter": _load_operating_driver_line_index_by_quarter,
            "_load_operating_driver_flat_line_index": _load_operating_driver_flat_line_index,
            "_load_profile_slide_signals_by_quarter": _load_profile_slide_signals_by_quarter,
            "_load_operating_driver_45z_guidance_docs_by_quarter": _load_operating_driver_45z_guidance_docs_by_quarter,
            "_ensure_valuation_render_bundle": _ensure_valuation_render_bundle,
            "_ensure_valuation_precompute_bundle": _ensure_valuation_precompute_bundle,
            "_write_derivative_oci_bridge_sheet": _write_derivative_oci_bridge_sheet,
            "_write_investment_case_surfaces": _write_investment_case_surfaces,
            "_write_anf_investment_case_surfaces": _write_anf_investment_case_surfaces,
            "_write_quarter_notes_narrative_ui_sheet": _write_quarter_notes_narrative_ui_surface,
            "_write_quarter_narrative_data_sheet": _write_quarter_narrative_data_surface,
            "_apply_shared_ui_conventions": lambda: _apply_shared_ui_conventions_to_workbook(wb, ticker),
            "_final_promise_progress_cleanup": lambda: _final_repair_promise_progress_ui(wb, ticker),
        },
    )
    _quarter_notes_ui_selection_outer_scope.update(locals())
    ctx = WriterContext(
        inputs=inputs,
        wb=wb,
        font_size=font_size,
        header_size=header_size,
        company_profile=company_profile,
        data=runtime_data,
        callbacks=callbacks,
        writer_timings={},
        ui_info_rows=ui_info_rows,
        desired_sheet_order=desired_sheet_order,
        raw_sheet_cluster=raw_sheet_cluster,
    )
    ctx_ref = ctx
    valuation_precompute_support.runtime["ctx_ref"] = ctx_ref
    state = _build_compat_state(ctx)
    ctx.state = state
    return ctx
