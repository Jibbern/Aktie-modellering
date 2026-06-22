"""Chart text/category helpers for workbook chart rendering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class ChartTextSupportDeps:
    runtime: MutableMapping[str, Any]


def _excel_string_ref(
    sheet_name: str,
    col_idx: int,
    start_row: int,
    end_row: int,
    *,
    get_column_letter: Any,
) -> str:
    safe_sheet = str(sheet_name or "").replace("'", "''")
    col_letter = get_column_letter(int(col_idx))
    return f"'{safe_sheet}'!${col_letter}${int(start_row)}:${col_letter}${int(end_row)}"


class ChartTextSupport:
    def __init__(self, deps: ChartTextSupportDeps) -> None:
        self._runtime = deps.runtime

    def apply_chart_text_categories(
        self,
        chart_in: Any,
        *,
        sheet_name: str,
        col_idx: int,
        start_row: int,
        end_row: int,
    ) -> None:
        runtime = self._runtime
        get_column_letter = runtime["get_column_letter"]
        TextAxis = runtime["TextAxis"]
        ChartLines = runtime["ChartLines"]
        GraphicalProperties = runtime["GraphicalProperties"]
        LineProperties = runtime["LineProperties"]
        AxDataSource = runtime["AxDataSource"]
        StrRef = runtime["StrRef"]

        if chart_in is None or int(end_row) < int(start_row):
            return
        formula = _excel_string_ref(
            sheet_name,
            col_idx,
            start_row,
            end_row,
            get_column_letter=get_column_letter,
        )
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
