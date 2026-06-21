"""Shared analysis-sheet title and stacked-quarter layout helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, MutableMapping, Optional


@dataclass(frozen=True)
class AnalysisSheetLayoutSupportDeps:
    runtime: MutableMapping[str, Any]


class AnalysisSheetLayoutSupport:
    def __init__(self, deps: AnalysisSheetLayoutSupportDeps) -> None:
        self._runtime = deps.runtime

    def write_analysis_sheet_title_and_metadata(
        self,
        ws: Any,
        title: str,
        metadata_text: str,
        *,
        max_col: int,
        title_row: int = 1,
        metadata_row: int = 2,
    ) -> int:
        runtime = self._runtime
        _get_analysis_sheet_style_bundle = runtime["_get_analysis_sheet_style_bundle"]
        copy = runtime["copy"]
        Font = runtime["Font"]
        Alignment = runtime["Alignment"]

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

    def render_stacked_quarter_blocks(
        self,
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
        runtime = self._runtime
        _get_analysis_sheet_style_bundle = runtime["_get_analysis_sheet_style_bundle"]
        copy = runtime["copy"]
        Font = runtime["Font"]
        Alignment = runtime["Alignment"]
        Border = runtime["Border"]
        header_size = runtime["header_size"]

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
