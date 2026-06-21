"""Operating_Drivers raw-sheet writer support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class OperatingDriversRawSheetDeps:
    runtime: MutableMapping[str, Any]


class OperatingDriversRawSheetWriter:
    def __init__(self, deps: OperatingDriversRawSheetDeps) -> None:
        self._runtime = deps.runtime

    def write_operating_drivers_raw_sheet(self, rows: Any) -> None:
        runtime = self._runtime
        wb = runtime["wb"]
        pd = runtime["pd"]
        illegal_characters_re = runtime["ILLEGAL_CHARACTERS_RE"]
        PatternFill = runtime["PatternFill"]
        Border = runtime["Border"]
        Side = runtime["Side"]
        Font = runtime["Font"]
        Alignment = runtime["Alignment"]
        header_size = runtime["header_size"]
        _safe_cell = runtime["_safe_cell"]
        _set_cell_comment_local = runtime["_set_cell_comment_local"]
        _estimate_wrapped_row_height = runtime["_estimate_wrapped_row_height"]

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
                    value = illegal_characters_re.sub("", value)
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
