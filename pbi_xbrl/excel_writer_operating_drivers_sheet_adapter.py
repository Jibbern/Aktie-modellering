"""Thin Operating_Drivers visible-sheet adapter for workbook writer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class OperatingDriversSheetAdapterDeps:
    runtime: MutableMapping[str, Any]


class OperatingDriversSheetAdapter:
    def __init__(self, deps: OperatingDriversSheetAdapterDeps) -> None:
        self.deps = deps

    def _rt(self, name: str) -> Any:
        return self.deps.runtime[name]

    def write_operating_drivers_sheet(self, rows: Any) -> Any:
        OperatingDriversWriterDeps = self._rt("OperatingDriversWriterDeps")
        write_operating_drivers_sheet = self._rt("write_operating_drivers_sheet")
        deps = OperatingDriversWriterDeps(
            wb=self._rt("wb"),
            hist=self._rt("hist"),
            ticker=self._rt("ticker"),
            company_profile=self._rt("company_profile"),
            slides_segments=self._rt("slides_segments"),
            slides_guidance=self._rt("slides_guidance"),
            quarter_notes=self._rt("quarter_notes"),
            derivative_oci_bridge_df=self._rt("derivative_oci_bridge_df"),
            material_roots=self._rt("material_roots"),
            font_size=self._rt("font_size"),
            header_size=self._rt("header_size"),
            is_pbi_profile=self._rt("is_pbi_profile"),
            is_gpre_profile=self._rt("is_gpre_profile"),
            is_anf_profile=self._rt("is_anf_profile"),
            enable_quarterly_segment_block=self._rt("enable_quarterly_segment_block"),
            annual_segment_alias_patterns=self._rt("annual_segment_alias_patterns"),
            anf_segment_brand_explanation=self._rt("ANF_SEGMENT_BRAND_EXPLANATION"),
            get_valuation_style_bundle=self._rt("_get_valuation_style_bundle"),
            get_analysis_sheet_style_bundle=self._rt("_get_analysis_sheet_style_bundle"),
            operating_driver_quarters=self._rt("_operating_driver_quarters"),
            load_operating_driver_template_index=self._rt("_load_operating_driver_template_index"),
            load_operating_driver_source_records_by_quarter=self._rt(
                "_load_operating_driver_source_records_by_quarter"
            ),
            load_operating_driver_flat_line_index=self._rt("_load_operating_driver_flat_line_index"),
            first_existing_material_dir=self._rt("_first_existing_material_dir"),
            parse_quarter_from_filename=self._rt("_parse_quarter_from_filename"),
            parse_quarter_from_follow_text=self._rt("_parse_quarter_from_follow_text"),
            read_operating_driver_text=self._rt("_read_operating_driver_text"),
            set_cell_comment=self._rt("_set_cell_comment_local"),
            driver_source_note=self._rt("_driver_source_note"),
            driver_row_label=self._rt("_driver_row_label"),
            truncate_driver_text=self._rt("_truncate_driver_text"),
            quarter_label_short=self._rt("_quarter_label_short"),
            source_rank=self._rt("_source_rank"),
            text_fragment_penalty=self._rt("_text_fragment_penalty"),
            ensure_terminal_period=self._rt("_ensure_terminal_period"),
            gpre_commercial_setup_records_shared=self._rt("_gpre_commercial_setup_records_shared"),
            anf_clean_visible_operating_driver_records=self._rt(
                "_anf_clean_visible_operating_driver_records"
            ),
            anf_clean_visible_ui_text=self._rt("_anf_clean_visible_ui_text"),
            anf_compact_driver_group=self._rt("_anf_compact_driver_group"),
            anf_compact_driver_label=self._rt("_anf_compact_driver_label"),
            anf_recent_operating_commentary_rows=self._rt("_anf_recent_operating_commentary_rows"),
            anf_round_visible_driver_value=self._rt("_anf_round_visible_driver_value"),
            anf_visible_quarter_label=self._rt("_anf_visible_quarter_label"),
            sector_operating_driver_intro_tables=self._rt("_sector_operating_driver_intro_tables"),
        )
        return write_operating_drivers_sheet(deps, rows)
