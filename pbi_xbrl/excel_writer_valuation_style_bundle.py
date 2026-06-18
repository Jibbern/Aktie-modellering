"""Valuation style-bundle cache support for the workbook writer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, MutableMapping


@dataclass(frozen=True)
class ValuationStyleBundleDeps:
    runtime: MutableMapping[str, Any]


def get_valuation_style_bundle(deps: ValuationStyleBundleDeps) -> Any:
    runtime = deps.runtime
    wb = runtime["wb"]
    ctx_ref = runtime.get("ctx_ref")
    valuation_style_bundle_cache = runtime.get("valuation_style_bundle_cache")
    copy = runtime["copy"]
    PatternFill = runtime["PatternFill"]
    get_column_letter = runtime["get_column_letter"]
    font_size = runtime["font_size"]
    _get_analysis_sheet_style_bundle = runtime["_get_analysis_sheet_style_bundle"]
    _timed_writer_substage = runtime["_timed_writer_substage"]

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
            val_quarter_row = None
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
    runtime["valuation_style_bundle_cache"] = bundle
    if ctx_ref is not None:
        ctx_ref.derived.valuation_style_bundle = dict(bundle)
    return bundle
