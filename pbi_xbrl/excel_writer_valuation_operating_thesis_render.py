"""Worksheet render adapter for Valuation Operating Drivers and Thesis Bridge panels."""
from __future__ import annotations

import builtins
from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class ValuationOperatingThesisRenderDeps:
    runtime: MutableMapping[str, Any]


@dataclass(frozen=True)
class ValuationOperatingThesisRenderResult:
    row_operating_hdr: int
    row_operating_end: int
    row_thesis_hdr: int
    row_thesis_end: int


def render_valuation_operating_thesis_panels(
    deps: ValuationOperatingThesisRenderDeps,
) -> ValuationOperatingThesisRenderResult:
    __rt = deps.runtime
    context_globals = dict(__rt.get("context_globals") or {})

    def _rt_get(name: str) -> Any:
        if name in __rt:
            return __rt[name]
        if name in context_globals:
            return context_globals[name]
        if name in globals():
            return globals()[name]
        return builtins.getattr(builtins, name, None)

    Alignment = _rt_get('Alignment')
    Border = _rt_get('Border')
    Exception = _rt_get('Exception')
    Font = _rt_get('Font')
    Side = _rt_get('Side')
    _build_operating_driver_rows = _rt_get('_build_operating_driver_rows')
    _normalize_thesis_bridge_basis = _rt_get('_normalize_thesis_bridge_basis')
    _resolve_thesis_fy_base = _rt_get('_resolve_thesis_fy_base')
    _set_cell_comment = _rt_get('_set_cell_comment')
    _set_cell_comment_local = _rt_get('_set_cell_comment_local')
    _set_formula_name = _rt_get('_set_formula_name')
    _shared_readable_source_type_label = _rt_get('_shared_readable_source_type_label')
    _overlaps = _rt_get('_overlaps')
    additive_panel_end = _rt_get('additive_panel_end')
    company_profile = _rt_get('company_profile')
    copy = _rt_get('copy')
    enumerate = _rt_get('enumerate')
    fair_denom = _rt_get('fair_denom')
    float = _rt_get('float')
    get_column_letter = _rt_get('get_column_letter')
    getattr = _rt_get('getattr')
    input_fill = _rt_get('input_fill')
    input_value_col = _rt_get('input_value_col')
    is_gpre_profile = _rt_get('is_gpre_profile')
    is_pbi_profile = _rt_get('is_pbi_profile')
    len = _rt_get('len')
    list = _rt_get('list')
    max = _rt_get('max')
    panel_col_start = _rt_get('panel_col_start')
    panel_row_start = _rt_get('panel_row_start')
    pd = _rt_get('pd')
    range = _rt_get('range')
    row_convert_hdr = _rt_get('row_convert_hdr')
    row_adj_eps_ttm = _rt_get('row_adj_eps_ttm')
    row_eps_ttm = _rt_get('row_eps_ttm')
    row_fcf_ttm = _rt_get('row_fcf_ttm')
    row_mi_hdr = _rt_get('row_mi_hdr')
    row_net_debt = _rt_get('row_net_debt')
    row_ptr = _rt_get('row_ptr')
    row_qadj_yield = _rt_get('row_qadj_yield')
    row_share_mode = _rt_get('row_share_mode')
    row_shares_dil = _rt_get('row_shares_dil')
    row_shares_out = _rt_get('row_shares_out')
    row_tgt_ev_adj = _rt_get('row_tgt_ev_adj')
    set = _rt_get('set')
    side_panel_style = _rt_get('side_panel_style')
    str = _rt_get('str')
    ws = _rt_get('ws')

    row_operating_hdr = 0
    row_operating_end = 0
    row_thesis_hdr = 0
    row_thesis_end = 0

    # Operating Drivers + Thesis Bridge (right-side additive blocks).
    operating_driver_rows = _build_operating_driver_rows()
    thesis_base_info = _resolve_thesis_fy_base()
    thesis_base_label = str(thesis_base_info.get("label") or "Base Adj EBITDA FY")
    thesis_input_labels = list(getattr(company_profile, "thesis_bridge_labels", ()) or [])
    if not thesis_input_labels:
        thesis_input_labels = [
            "Base Adj EBITDA FY",
            "Policy / regulatory uplift",
            "Price / mix uplift",
            "Coproduct / mix uplift",
            "Cost savings uplift",
            "Interest savings / debt-paydown uplift",
            "Other",
        ]
    thesis_target_multiple_label = "Thesis target EV / Adj EBITDA"
    thesis_target_yield_label = "Thesis target equity FCF yield"
    thesis_target_eps_label = "Thesis EPS"
    thesis_target_pe_label = "Thesis P/E multiple"
    thesis_input_labels = [
        x
        for x in thesis_input_labels
        if not str(x).strip().lower().startswith("base adj ebitda ")
    ]
    thesis_input_labels = [
        thesis_base_label,
        thesis_target_multiple_label,
        thesis_target_yield_label,
    ] + thesis_input_labels
    if is_pbi_profile:
        thesis_input_labels = [
            thesis_base_label,
            thesis_target_multiple_label,
            thesis_target_yield_label,
            thesis_target_eps_label,
            thesis_target_pe_label,
        ] + [
            x
            for x in thesis_input_labels
            if x
            not in {
                thesis_base_label,
                thesis_target_multiple_label,
                thesis_target_yield_label,
                thesis_target_eps_label,
                thesis_target_pe_label,
            }
        ]
    hidden_thesis_bridge_labels = {
        "protein / mix uplift",
    }
    if is_gpre_profile:
        filtered_thesis_input_labels: List[str] = []
        for label in thesis_input_labels:
            label_low = str(label or "").strip().lower()
            if "corn oil" in label_low or "coproduct" in label_low or label_low in hidden_thesis_bridge_labels:
                continue
            filtered_thesis_input_labels.append(label)
        thesis_input_labels = filtered_thesis_input_labels
    thesis_output_defs: List[Tuple[str, Optional[str], Optional[str]]] = [
        ("Thesis Adj EBITDA", None, "#,##0.000"),
        ("Thesis FCF", None, "#,##0.000"),
    ]
    if is_pbi_profile:
        thesis_output_defs.append(("Thesis EPS", None, "$#,##0.00"))
    thesis_output_defs.extend(
        [
            ("EV @ EV/Adj EBITDA", None, "#,##0.000"),
            ("Equity value @ EV/Adj EBITDA", None, "#,##0.000"),
        ]
    )
    if is_pbi_profile:
        thesis_output_defs.append(("Equity value @ P/E", None, "#,##0.000"))
    thesis_output_defs.extend(
        [
            ("Equity value @ FCF yield", None, "#,##0.000"),
            ("Range summary", None, "$#,##0.00"),
        ]
    )
    if is_pbi_profile:
        thesis_output_defs.append(("Value/share @ P/E", None, "$#,##0.00"))
    thesis_output_defs.extend(
        [
            ("Value/share @ EV/Adj EBITDA", None, "$#,##0.00"),
            ("Value/share @ FCF yield", None, "$#,##0.00"),
        ]
    )

    def _panel_has_content(r1: int, r2: int, c1: int, c2: int) -> bool:
        for mr in list(ws.merged_cells.ranges):
            try:
                if _overlaps(mr, r1, r2, c1, c2):
                    return True
            except Exception:
                continue
        for rr in range(r1, r2 + 1):
            for cc in range(c1, c2 + 1):
                if ws.cell(row=rr, column=cc).value not in (None, ""):
                    return True
        return False

    op_block_rows = max(3, len(operating_driver_rows)) + 3
    thesis_block_rows = len(thesis_input_labels) + len(thesis_output_defs) + 10
    total_right_block_rows = op_block_rows + thesis_block_rows + 2
    # Keep the intentional right-side blocks compact: one blank side-panel
    # row after guidance is enough.  The left-side valuation rows may still
    # contain quarterly data, so this only controls columns O:AC.
    preferred_right_start = max(row_ptr, panel_row_start)
    if preferred_right_start + total_right_block_rows <= row_mi_hdr - 2:
        candidate_rows = range(preferred_right_start, row_mi_hdr - total_right_block_rows)
    else:
        fallback_row = max(row_qadj_yield, row_convert_hdr + 8) + 3
        candidate_rows = range(fallback_row, fallback_row + 80)

    for candidate_row in candidate_rows:
        if not _panel_has_content(candidate_row, candidate_row + total_right_block_rows, panel_col_start, additive_panel_end):
            row_operating_hdr = candidate_row
            break
    if row_operating_hdr <= 0:
        row_operating_hdr = preferred_right_start

    neutral_fill = copy(side_panel_style["neutral_fill"])
    neutral_alt_fill = copy(side_panel_style["neutral_alt_fill"])
    side_panel_section_fill = copy(side_panel_style["section_fill"])
    side_panel_header_fill = copy(side_panel_style["header_fill"])
    side_panel_title_font = copy(side_panel_style["title_font"])
    side_panel_header_font = copy(side_panel_style["header_font"])
    side_panel_body_font = copy(side_panel_style["body_font"])
    side_panel_input_font = copy(side_panel_style["input_font"])
    side_panel_thin_border = copy(side_panel_style["thin_border"])
    operating_spans = [(15, 17), (18, 20), (21, 26), (27, 29)]
    thesis_spans = [(15, 20), (21, 23), (24, 29)]
    thesis_value_col = 21
    thesis_note_col = 24
    thesis_value_col_letter = get_column_letter(thesis_value_col)

    def _merge_panel_spans(row_idx: int, spans: Sequence[Tuple[int, int]]) -> None:
        for merge_start, merge_end in spans:
            if merge_end <= merge_start:
                continue
            try:
                ws.merge_cells(start_row=row_idx, start_column=merge_start, end_row=row_idx, end_column=merge_end)
            except Exception:
                pass

    def _style_side_panel_row(
        row_idx: int,
        *,
        fill: PatternFill,
        font: Font,
        height: float = 19.5,
        wrap_from_col: Optional[int] = None,
    ) -> None:
        for cc in range(panel_col_start, additive_panel_end + 1):
            cell = ws.cell(row=row_idx, column=cc)
            cell.fill = copy(fill)
            cell.font = copy(font)
            cell.border = side_panel_thin_border
            cell.alignment = Alignment(
                horizontal="left",
                vertical="center",
                wrap_text=bool(wrap_from_col is not None and cc >= wrap_from_col),
            )
        ws.row_dimensions[row_idx].height = float(height)

    side_panel_spacer_border = Border(
        top=Side(style="thin", color="D9E2EA"),
        bottom=Side(style="thin", color="D9E2EA"),
    )

    def _style_side_panel_spacer_row(row_idx: int, *, height: float = 19.5) -> None:
        for cc in range(panel_col_start, additive_panel_end + 1):
            cell = ws.cell(row=row_idx, column=cc)
            cell.fill = copy(neutral_fill)
            cell.font = copy(side_panel_body_font)
            cell.border = side_panel_spacer_border
            cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
        ws.row_dimensions[row_idx].height = float(height)

    # Operating Drivers.
    ws.merge_cells(start_row=row_operating_hdr, start_column=panel_col_start, end_row=row_operating_hdr, end_column=additive_panel_end)
    ws.cell(row=row_operating_hdr, column=panel_col_start, value="Operating Drivers").font = side_panel_title_font
    _style_side_panel_row(row_operating_hdr, fill=side_panel_section_fill, font=side_panel_title_font, height=19.5)
    operating_header_row = row_operating_hdr + 1
    operating_headers = [
        (15, "Driver group"),
        (18, "Driver"),
        (21, "Why it matters"),
        (27, "Source/type"),
    ]
    _style_side_panel_row(operating_header_row, fill=side_panel_header_fill, font=side_panel_header_font, height=19.5)
    for cc, label in operating_headers:
        ws.cell(row=operating_header_row, column=cc, value=label)
    _merge_panel_spans(operating_header_row, operating_spans)
    row_operating_data = operating_header_row + 1
    if not operating_driver_rows:
        ws.merge_cells(start_row=row_operating_data, start_column=panel_col_start, end_row=row_operating_data, end_column=additive_panel_end)
        ws.cell(row=row_operating_data, column=panel_col_start, value="No operating-driver map available for this ticker yet.")
        ws.cell(row=row_operating_data, column=panel_col_start).alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
        row_operating_end = row_operating_data
    else:
        for idx_driver, driver_row in enumerate(operating_driver_rows):
            ws.cell(row=row_operating_data, column=15, value=driver_row.get("group") or "")
            ws.cell(row=row_operating_data, column=18, value=driver_row.get("driver") or "")
            ws.cell(row=row_operating_data, column=21, value=driver_row.get("why") or "")
            ws.cell(
                row=row_operating_data,
                column=27,
                value=_shared_readable_source_type_label(driver_row.get("source_type") or ""),
            )
            _merge_panel_spans(row_operating_data, operating_spans)
            _style_side_panel_row(
                row_operating_data,
                fill=neutral_alt_fill if idx_driver % 2 == 0 else neutral_fill,
                font=side_panel_body_font,
                height=19.5,
                wrap_from_col=21,
            )
            signal_txt = str(driver_row.get("signal") or "").strip()
            delta_txt = str(driver_row.get("delta") or "").strip()
            comment_bits = [x for x in (signal_txt, delta_txt) if x]
            if comment_bits:
                try:
                    _set_cell_comment_local(ws.cell(row=row_operating_data, column=27), " | ".join(comment_bits))
                except Exception:
                    pass
            row_operating_data += 1
        row_operating_end = row_operating_data - 1

    # Thesis Bridge.
    row_thesis_hdr = row_operating_end + 2
    ws.merge_cells(start_row=row_thesis_hdr, start_column=panel_col_start, end_row=row_thesis_hdr, end_column=additive_panel_end)
    ws.cell(row=row_thesis_hdr, column=panel_col_start, value="Thesis Bridge").font = side_panel_title_font
    _style_side_panel_row(row_thesis_hdr, fill=side_panel_section_fill, font=side_panel_title_font, height=19.5)

    thesis_note_row = row_thesis_hdr + 1
    ws.merge_cells(start_row=thesis_note_row, start_column=panel_col_start, end_row=thesis_note_row, end_column=additive_panel_end)
    ws.cell(
        row=thesis_note_row,
        column=panel_col_start,
        value="Enter annual incremental EBITDA/equity effects, not raw rate changes.",
    )
    _style_side_panel_row(thesis_note_row, fill=neutral_alt_fill, font=side_panel_body_font, height=19.5)
    ws.cell(row=thesis_note_row, column=panel_col_start).alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

    thesis_header_row = thesis_note_row + 1
    _style_side_panel_row(thesis_header_row, fill=side_panel_header_fill, font=side_panel_header_font, height=19.5)
    for cc, label in ((15, "Bridge item"), (thesis_value_col, "Value"), (thesis_note_col, "Notes")):
        ws.cell(row=thesis_header_row, column=cc, value=label)
    _merge_panel_spans(thesis_header_row, thesis_spans)

    thesis_input_rows: Dict[str, int] = {}
    thesis_row = thesis_header_row + 1
    thesis_base_value_m = float(pd.to_numeric(thesis_base_info.get("value_m"), errors="coerce") or 0.0)
    thesis_base_value_m = float(_normalize_thesis_bridge_basis("ThesisBaseAdjEBITDA_FY", thesis_base_value_m) or 0.0)
    _set_formula_name("ThesisBaseAdjEBITDA_FY", thesis_base_value_m)
    for idx, label in enumerate(thesis_input_labels):
        _merge_panel_spans(thesis_row, thesis_spans)
        ws.cell(row=thesis_row, column=15, value=label)
        if idx == 0:
            ws.cell(row=thesis_row, column=thesis_value_col, value="=ThesisBaseAdjEBITDA_FY")
        elif label == thesis_target_multiple_label:
            ws.cell(row=thesis_row, column=thesis_value_col, value="=Target_EV_AdjEBITDA")
        elif label == thesis_target_yield_label:
            # Market-cap based Equity_FCF_Yield can be unavailable when live
            # market data is absent. Keep the quick thesis panel usable with
            # an explicit scenario input instead of surfacing #NAME? in Excel.
            ws.cell(row=thesis_row, column=thesis_value_col, value=0.07)
        elif label == thesis_target_eps_label:
            ws.cell(row=thesis_row, column=thesis_value_col, value='=IF(Adj_EPS_TTM<>"",Adj_EPS_TTM,EPS_TTM)')
        elif label == thesis_target_pe_label:
            ws.cell(row=thesis_row, column=thesis_value_col, value=10.0)
        else:
            ws.cell(row=thesis_row, column=thesis_value_col, value=0.0)
        for cc in range(panel_col_start, additive_panel_end + 1):
            cell = ws.cell(row=thesis_row, column=cc)
            cell.fill = copy(neutral_alt_fill if idx % 2 == 0 else neutral_fill)
            cell.font = copy(side_panel_body_font)
            cell.border = side_panel_thin_border
            cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=cc >= thesis_note_col)
        ws.cell(row=thesis_row, column=thesis_value_col).fill = input_fill
        ws.cell(row=thesis_row, column=thesis_value_col).font = side_panel_input_font
        ws.cell(row=thesis_row, column=thesis_value_col).number_format = (
            "0.00x"
            if label in {thesis_target_multiple_label, thesis_target_pe_label}
            else "0.0%"
            if label == thesis_target_yield_label
            else "$#,##0.00"
            if label == thesis_target_eps_label
            else "#,##0.000"
        )
        note_text = ""
        label_low = str(label).lower()
        if idx == 0:
            note_text = "Use incremental uplift vs the base adjusted EBITDA; avoid double counting."
            source_bits = []
            if thesis_base_info.get("source_type"):
                source_bits.append(str(thesis_base_info.get("source_type")))
            if thesis_base_info.get("fallback"):
                source_bits.append(f"fallback: {thesis_base_info.get('fallback')}")
            source_doc = str(thesis_base_info.get("source_doc") or "").strip()
            source_snip = str(thesis_base_info.get("snippet") or "").strip()
            comment_bits = [note_text]
            if source_bits:
                comment_bits.append(" | ".join(source_bits))
            if source_doc:
                comment_bits.append(source_doc)
            if source_snip:
                comment_bits.append(source_snip)
            _set_cell_comment(ws.cell(row=thesis_row, column=thesis_value_col), "\n\n".join(comment_bits))
        elif "45z" in label_low or "policy" in label_low:
            note_text = "Use for policy/tax-credit uplift that is not reflected in plain TTM history."
        elif "crush margin uplift" in label_low:
            note_text = "Enter annual EBITDA uplift from stronger crush margin versus the current base case."
        elif "corn oil" in label_low or "coproduct" in label_low:
            note_text = "Enter annual EBITDA uplift from stronger corn-oil or coproduct realizations versus the current base."
        elif "protein" in label_low:
            note_text = "Enter annual EBITDA uplift from protein/coproduct economics versus the current base."
        elif "presort" in label_low:
            note_text = "Enter annual EBITDA uplift from Presort volume, pricing or margin improvement."
        elif "sendtech" in label_low:
            note_text = "Enter annual EBITDA uplift from SendTech stabilization or slower decline."
        elif "fcf conversion" in label_low:
            note_text = "Enter annual equity-value uplift from better FCF conversion or debt paydown."
        elif " / mix uplift" in label_low:
            if is_pbi_profile:
                note_text = "Enter annual EBITDA uplift from PBI pricing, mix and segment margin improvement."
            else:
                note_text = "Enter annual EBITDA uplift from product mix versus the current base."
        elif "interest savings" in label_low or "debt-paydown" in label_low:
            note_text = "Bridge item for lower cash interest or debt reduction not yet visible in TTM."
        elif "cost savings" in label_low:
            note_text = "Use annualized savings not yet fully visible in reported TTM."
        elif label == thesis_target_multiple_label:
            note_text = "Explicit thesis EV/Adj EBITDA input; defaults to the current target multiple."
        elif label == thesis_target_yield_label:
            note_text = "Explicit thesis equity FCF yield input; default 7% when market-price yield is unavailable."
        elif label == thesis_target_eps_label:
            note_text = "PBI EPS method input; defaults to adjusted EPS TTM when available, otherwise GAAP EPS TTM."
        elif label == thesis_target_pe_label:
            note_text = "PBI P/E thesis input for the EPS method."
        if note_text:
            ws.cell(row=thesis_row, column=thesis_note_col, value=note_text)
            ws.cell(row=thesis_row, column=thesis_note_col).alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        ws.row_dimensions[thesis_row].height = 19.5
        thesis_input_rows[label] = thesis_row
        thesis_row += 1

    output_header_row = thesis_row
    _style_side_panel_row(output_header_row, fill=side_panel_header_fill, font=side_panel_header_font, height=19.5)
    for cc, label in ((15, "Output"), (thesis_value_col, "Value"), (thesis_note_col, "Interpretation")):
        ws.cell(row=output_header_row, column=cc, value=label)
    _merge_panel_spans(output_header_row, thesis_spans)
    thesis_row += 1

    thesis_base_row = thesis_input_rows[thesis_input_labels[0]]
    thesis_multiple_row = thesis_input_rows[thesis_target_multiple_label]
    thesis_yield_row = thesis_input_rows[thesis_target_yield_label]
    thesis_eps_row = thesis_input_rows.get(thesis_target_eps_label)
    thesis_pe_row = thesis_input_rows.get(thesis_target_pe_label)
    non_ebitda_input_labels = {
        thesis_input_labels[0],
        thesis_target_multiple_label,
        thesis_target_yield_label,
        thesis_target_eps_label,
        thesis_target_pe_label,
    }
    thesis_adjust_rows = [
        thesis_input_rows[label]
        for label in thesis_input_labels
        if label not in non_ebitda_input_labels and label in thesis_input_rows
    ]
    if thesis_adjust_rows:
        adjust_expr = ",".join(f"{thesis_value_col_letter}{rr}" for rr in thesis_adjust_rows)
        thesis_ebitda_formula = f"={thesis_value_col_letter}{thesis_base_row}+SUM({adjust_expr})"
    else:
        thesis_ebitda_formula = f"={thesis_value_col_letter}{thesis_base_row}"
    thesis_yield_norm_expr = (
        f"IF(OR({thesis_value_col_letter}{thesis_yield_row}=\"\","
        f"{thesis_value_col_letter}{thesis_yield_row}<=0),\"\","
        f"IF({thesis_value_col_letter}{thesis_yield_row}>1,"
        f"{thesis_value_col_letter}{thesis_yield_row}/100,"
        f"{thesis_value_col_letter}{thesis_yield_row}))"
    )
    thesis_blank_after: Set[str] = {"Equity value @ FCF yield"}
    thesis_output_rows: Dict[str, int] = {}
    thesis_spacer_rows: Set[int] = set()
    thesis_output_start_row = thesis_row
    for label, _, _ in thesis_output_defs:
        thesis_output_rows[label] = thesis_row
        thesis_row += 1
        if label in thesis_blank_after:
            thesis_spacer_rows.add(thesis_row)
            thesis_row += 1
    thesis_formula_map: Dict[str, str] = {
        "Thesis Adj EBITDA": thesis_ebitda_formula,
        "Thesis FCF": (
            f"=IF(FCF_TTM<>\"\",FCF_TTM,"
            f"IF(OR({thesis_value_col_letter}{thesis_output_rows['Thesis Adj EBITDA']}=\"\","
            f"InterestPaid_TTM=\"\",Capex_TTM=\"\",MaintCapexRatio=\"\",RecurringCashCosts=\"\",WCNormalization=\"\"),\"\","
            f"{thesis_value_col_letter}{thesis_output_rows['Thesis Adj EBITDA']}"
            f"-InterestPaid_TTM"
            f"-(Capex_TTM*MaintCapexRatio)"
            f"-RecurringCashCosts-WCNormalization))"
        ),
        "EV @ EV/Adj EBITDA": f"=IF(OR({thesis_value_col_letter}{thesis_output_rows['Thesis Adj EBITDA']}=\"\",{thesis_value_col_letter}{thesis_multiple_row}=\"\",{thesis_value_col_letter}{thesis_multiple_row}<=0),\"\",{thesis_value_col_letter}{thesis_output_rows['Thesis Adj EBITDA']}*{thesis_value_col_letter}{thesis_multiple_row})",
        "Equity value @ EV/Adj EBITDA": f"=IF(OR({thesis_value_col_letter}{thesis_output_rows['EV @ EV/Adj EBITDA']}=\"\",NetDebt=\"\"),\"\",{thesis_value_col_letter}{thesis_output_rows['EV @ EV/Adj EBITDA']}-NetDebt)",
        "Value/share @ EV/Adj EBITDA": f"=IF(OR({thesis_value_col_letter}{thesis_output_rows['Equity value @ EV/Adj EBITDA']}=\"\",{fair_denom}=\"\",{fair_denom}<=0),\"\",{thesis_value_col_letter}{thesis_output_rows['Equity value @ EV/Adj EBITDA']}/{fair_denom})",
        "Equity value @ FCF yield": f"=IF(OR({thesis_value_col_letter}{thesis_output_rows['Thesis FCF']}=\"\",({thesis_yield_norm_expr})=\"\",({thesis_yield_norm_expr})<=0),\"\",{thesis_value_col_letter}{thesis_output_rows['Thesis FCF']}/({thesis_yield_norm_expr}))",
        "Value/share @ FCF yield": f"=IF(OR({thesis_value_col_letter}{thesis_output_rows['Equity value @ FCF yield']}=\"\",{fair_denom}=\"\",{fair_denom}<=0),\"\",{thesis_value_col_letter}{thesis_output_rows['Equity value @ FCF yield']}/{fair_denom})",
    }
    if "Thesis EPS" in thesis_output_rows and thesis_eps_row:
        thesis_formula_map["Thesis EPS"] = f"=IF({thesis_value_col_letter}{thesis_eps_row}=\"\",\"\",{thesis_value_col_letter}{thesis_eps_row})"
    if "Equity value @ P/E" in thesis_output_rows and thesis_eps_row and thesis_pe_row:
        thesis_formula_map["Equity value @ P/E"] = f"=IF(OR({thesis_value_col_letter}{thesis_eps_row}=\"\",{thesis_value_col_letter}{thesis_pe_row}=\"\",{fair_denom}=\"\",{fair_denom}<=0),\"\",{thesis_value_col_letter}{thesis_eps_row}*{thesis_value_col_letter}{thesis_pe_row}*{fair_denom})"
    if "Value/share @ P/E" in thesis_output_rows and thesis_eps_row and thesis_pe_row:
        thesis_formula_map["Value/share @ P/E"] = f"=IF(OR({thesis_value_col_letter}{thesis_eps_row}=\"\",{thesis_value_col_letter}{thesis_pe_row}=\"\"),\"\",{thesis_value_col_letter}{thesis_eps_row}*{thesis_value_col_letter}{thesis_pe_row})"
    share_value_refs = [
        f"{thesis_value_col_letter}{thesis_output_rows[label]}"
        for label in ("Value/share @ P/E", "Value/share @ EV/Adj EBITDA", "Value/share @ FCF yield")
        if label in thesis_output_rows
    ]
    def _static_thesis_range_summary() -> str:
        def _num_local(v: Any) -> Optional[float]:
            if isinstance(v, str) and v.strip().startswith("="):
                return None
            out = pd.to_numeric(v, errors="coerce")
            return float(out) if pd.notna(out) else None

        def _cell_num(row_idx: int, col_idx: int = input_value_col) -> Optional[float]:
            return _num_local(ws.cell(row=row_idx, column=col_idx).value)

        denom_mode = str(ws.cell(row=row_share_mode, column=input_value_col).value or "").strip().lower()
        denom = _cell_num(row_shares_out) if denom_mode == "outstanding" else _cell_num(row_shares_dil)
        net_debt = _cell_num(row_net_debt)
        fcf_ttm = _cell_num(row_fcf_ttm)
        eps_default = _cell_num(row_adj_eps_ttm) or _cell_num(row_eps_ttm)
        multiple_default = _cell_num(row_tgt_ev_adj) or 6.0
        pe_default = 10.0
        yield_default = 0.07
        values: List[float] = []
        if eps_default is not None and is_pbi_profile and denom is not None and denom > 0:
            values.append(float(eps_default) * pe_default)
        if denom is not None and denom > 0 and net_debt is not None and multiple_default > 0:
            values.append(((float(thesis_base_value_m) * float(multiple_default)) - float(net_debt)) / float(denom))
        if denom is not None and denom > 0 and fcf_ttm is not None and yield_default > 0:
            values.append((float(fcf_ttm) / yield_default) / float(denom))
        values = [v for v in values if pd.notna(v) and abs(v) < 10_000]
        if len(values) < 2:
            return ""
        if max(values) < 1.0:
            return ""
        def _fmt_share(v: float) -> str:
            return f"-${abs(v):,.2f}" if v < 0 else f"${v:,.2f}"

        low_v = min(values)
        high_v = max(values)
        sep = " to " if low_v < 0 < high_v else "-"
        return f"{_fmt_share(low_v)}{sep}{_fmt_share(high_v)}"

    if len(share_value_refs) >= 2:
        nonblank_test = ",".join(ref + '=""' for ref in share_value_refs)
        refs_expr = ",".join(share_value_refs)
        thesis_formula_map["Range summary"] = (
            f"=IF(OR({nonblank_test}),\"\","
            f"TEXT(MIN({refs_expr}),\"$0.00\")&\"-\"&TEXT(MAX({refs_expr}),\"$0.00\"))"
        )
        static_range_summary = _static_thesis_range_summary()
        if static_range_summary:
            thesis_formula_map["Range summary"] = static_range_summary
    thesis_interp_map = {
        "Thesis Adj EBITDA": "Base FY adjusted EBITDA plus user thesis adjustments.",
        "Thesis FCF": "Additive thesis FCF bridge from thesis EBITDA less cash interest, maintenance capex, taxes and working-capital drag.",
        "EV @ EV/Adj EBITDA": "Applies the explicit thesis EV/Adj EBITDA input to thesis EBITDA.",
        "Equity value @ EV/Adj EBITDA": "Enterprise value less current net debt core.",
        "Equity value @ P/E": "PBI EPS method: thesis EPS times P/E times share denominator.",
        "Thesis EPS": "PBI thesis EPS input used in the P/E method.",
        "Value/share @ P/E": "Thesis EPS times thesis P/E.",
        "Value/share @ EV/Adj EBITDA": "Equity value divided by the current valuation share denominator mode.",
        "Equity value @ FCF yield": "Capitalizes thesis FCF as an equity FCF stream using the explicit thesis equity FCF yield.",
        "Value/share @ FCF yield": "Equity-FCF-yield thesis value divided by the current valuation share denominator mode.",
        "Range summary": "Low-to-high quick range across the per-share outputs below.",
    }
    for label, explicit_formula, num_fmt in thesis_output_defs:
        thesis_row = thesis_output_rows[label]
        _merge_panel_spans(thesis_row, thesis_spans)
        ws.cell(row=thesis_row, column=15, value=label)
        value_formula = explicit_formula or thesis_formula_map.get(label) or ""
        for cc in range(panel_col_start, additive_panel_end + 1):
            cell = ws.cell(row=thesis_row, column=cc)
            cell.fill = copy(neutral_alt_fill if (thesis_row - thesis_output_start_row) % 2 == 0 else neutral_fill)
            cell.font = copy(side_panel_body_font)
            cell.border = side_panel_thin_border
            cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=cc >= thesis_note_col)
        ws.cell(row=thesis_row, column=thesis_value_col, value=value_formula)
        ws.cell(row=thesis_row, column=thesis_value_col).number_format = str(num_fmt or "#,##0.000")
        ws.cell(row=thesis_row, column=thesis_value_col).font = Font(color="000000", size=12, bold=False)
        ws.cell(row=thesis_row, column=thesis_note_col, value=thesis_interp_map.get(label) or "")
        ws.cell(row=thesis_row, column=thesis_note_col).alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    for spacer_row in thesis_spacer_rows:
        _style_side_panel_spacer_row(spacer_row)
    row_thesis_end = max(thesis_output_rows.values())
    for rr in range(row_operating_hdr, row_thesis_end + 1):
        if rr in {row_operating_hdr, row_thesis_hdr, operating_header_row, thesis_note_row, thesis_header_row, output_header_row}:
            ws.row_dimensions[rr].height = 19.5
        elif rr in thesis_spacer_rows:
            ws.row_dimensions[rr].height = 19.5
        else:
            ws.row_dimensions[rr].height = 19.5


    return ValuationOperatingThesisRenderResult(
        row_operating_hdr=row_operating_hdr,
        row_operating_end=row_operating_end,
        row_thesis_hdr=row_thesis_hdr,
        row_thesis_end=row_thesis_end,
    )
