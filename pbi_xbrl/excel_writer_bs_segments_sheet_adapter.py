"""Thin BS_Segments visible-sheet adapter for workbook writer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class BSSegmentsSheetAdapterDeps:
    runtime: MutableMapping[str, Any]


class BSSegmentsSheetAdapter:
    def __init__(self, deps: BSSegmentsSheetAdapterDeps) -> None:
        self.deps = deps

    def _rt(self, name: str) -> Any:
        return self.deps.runtime[name]

    def write_bs_segments_sheet(self, quarters_shown: int = 8) -> Any:
        BsSegmentsWriterDeps = self._rt("BsSegmentsWriterDeps")
        write_bs_segments_sheet = self._rt("write_bs_segments_sheet")
        deps = BsSegmentsWriterDeps(
            wb=self._rt("wb"),
            hist=self._rt("hist"),
            audit=self._rt("audit"),
            ticker=self._rt("ticker"),
            company_profile=self._rt("company_profile"),
            slides_segments=self._rt("slides_segments"),
            material_roots=self._rt("material_roots"),
            ticker_roots=self._rt("ticker_roots"),
            ui_info_rows=self._rt("ui_info_rows"),
            font_size=self._rt("font_size"),
            header_size=self._rt("header_size"),
            is_pbi_profile=self._rt("is_pbi_profile"),
            is_gpre_profile=self._rt("is_gpre_profile"),
            is_anf_profile=self._rt("is_anf_profile"),
            bank_metrics_enabled=self._rt("bank_metrics_enabled"),
            enable_quarterly_segment_block=self._rt("enable_quarterly_segment_block"),
            enable_annual_segment_block=self._rt("enable_annual_segment_block"),
            quarterly_segment_labels=self._rt("quarterly_segment_labels"),
            annual_segment_labels=self._rt("annual_segment_labels"),
            annual_segment_alias_patterns=self._rt("annual_segment_alias_patterns"),
            anf_segment_brand_explanation=self._rt("ANF_SEGMENT_BRAND_EXPLANATION"),
            get_valuation_style_bundle=self._rt("_get_valuation_style_bundle"),
            hist_view=self._rt("_hist_view"),
            resolve_col=self._rt("_resolve_col"),
            set_cell_comment=self._rt("_set_cell_comment_local"),
            shared_load_local_balance_sheet_detail_payloads=self._rt(
                "_shared_load_local_balance_sheet_detail_payloads"
            ),
            carry_forward_low_change_series=self._rt("_carry_forward_low_change_series"),
            first_existing_material_dir=self._rt("_first_existing_material_dir"),
            parse_quarter_from_filename=self._rt("_parse_quarter_from_filename"),
            parse_quarter_from_follow_text=self._rt("_parse_quarter_from_follow_text"),
            read_operating_driver_text=self._rt("_read_operating_driver_text"),
            operating_driver_financial_statement_files=self._rt(
                "_operating_driver_financial_statement_files"
            ),
            sec_cache_roots_local=self._rt("_sec_cache_roots_local"),
            anf_visible_quarter_label=self._rt("_anf_visible_quarter_label"),
        )
        return write_bs_segments_sheet(deps, quarters_shown=quarters_shown)
