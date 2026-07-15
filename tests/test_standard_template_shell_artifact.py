from __future__ import annotations

from datetime import date, datetime
import importlib.util
import json
import re
import sys
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.utils import coordinate_to_tuple, range_boundaries


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "templates" / "standard_stock_model_template.xlsx"
ANF_LAB = ROOT / "templates" / "lab" / "ANF_template_lab.xlsx"
VISUAL_GAP_AUDIT_JSON = ROOT / "docs" / "standard_template_shell_visual_gap_audit.json"
VISUAL_GAP_AUDIT_MD = ROOT / "docs" / "standard_template_shell_visual_gap_audit.md"
HIDDEN_SUPPORT_AUDIT_JSON = ROOT / "docs" / "standard_template_hidden_support_audit.json"
HIDDEN_SUPPORT_AUDIT_MD = ROOT / "docs" / "standard_template_hidden_support_audit.md"
NEUTRALITY_AUDIT_JSON = ROOT / "docs" / "standard_template_shell_neutrality_audit.json"
NEUTRALITY_AUDIT_MD = ROOT / "docs" / "standard_template_shell_neutrality_audit.md"
SHEET_INVENTORY_JSON = ROOT / "docs" / "standard_template_sheet_inventory.json"
SUPPORT_LIFECYCLE_JSON = ROOT / "docs" / "support_sheet_lifecycle_contract.json"
RICH_VISIBLE_SHEETS = [
    "SUMMARY",
    "Valuation",
    "BS_Segments",
    "Operating_Drivers",
    "{ticker}_Investment_Case",
    "Quarter_Notes_UI",
    "Promise_Progress_UI",
]
COMPANY_SPECIFIC_TERMS = (
    "ANF",
    "Abercrombie",
    "Hollister",
    "Pitney Bowes",
    "Presort",
    "SendTech",
    "Green Plains",
    "45Z",
    "RIN",
    "crush margin",
)
FIXED_DIMENSION_MEMBERS = ("Americas", "EMEA", "APAC")
FIXED_SECTOR_LABEL_REGEXES = (
    r"\bclosures?\b",
    r"\bopenings?\b",
    r"\bremodels?\b",
    r"\bstores?\s*/\s*buybacks?\b",
    r"\bstores?\s*/\s*real estate\b",
    r"\bfranchise stores?\b",
    r"\bowned stores?\b",
    r"\btariffs?\b",
    r"\bERP\b",
    r"\bfreight tailwind\b",
    r"\bmarketing headwind\b",
    r"\bnet sales growth\b",
    r"\badjusted EPS\b",
    r"\bshare repurchases?\b",
    r"\breal estate activity\b",
)
SIGNAL_FILL_COLORS = {
    "002F80ED",
    "006FA8DC",
    "009BD3F5",
    "00A63A00",
    "00D55E00",
    "00E69F00",
    "00009E73",
    "0066C2A5",
    "0056B4E9",
    "00CC79A7",
}
ALLOWED_HIDDEN_SHELL_SHEETS = {
    "Hidden_Value_Flags",
    "Revolver_History",
    "Debt_Tranches_Latest",
    "Debt_Profile",
    "Guidance_Normalized",
    "Quarter_Notes",
    "Promise_Progress",
    "History_Q",
}
REQUIRED_PROMISE_ANNUAL_HEADERS = [
    "Metric",
    "Initial guide",
    "Q1 update",
    "Q2 update",
    "Q3 update",
    "Q4 update",
    "Actual",
    "Status",
    "Notes/source",
]
REQUIRED_PROMISE_REVISION_HEADERS = [
    "Metric",
    "Previous guide",
    "New/current guide",
    "Change type",
    "Actual",
    "Progress / run-rate",
    "Status",
    "Horizon",
    "Stated in",
    "Source date",
    "Source / note",
]
PROMISE_ANNUAL_HEADER_ROWS = (12, 23, 29, 34)
PROMISE_REVISION_HEADER_ROWS = (60, 70, 77, 85, 91, 98)
NEUTRAL_BLANK_FILLS = {"", "00000000", "00FFFFFF", "FFFFFFFF"}
GRAY_BLANK_FILLS = {"00DDDDDD", "00D9D9D9", "FFD9D9D9", "FFDDDDDD"}
VALUATION_GUIDANCE_SIDECAR_HEADERS = {
    "O7": "Guidance",
    "O28": "Metric",
    "Q28": "Stated in",
    "R28": "Applies to",
    "S28": "Guidance",
    "AA28": "Trend / realized",
    "O37": "Operating Drivers",
    "O38": "Driver group",
    "R38": "Driver",
    "U38": "Why it matters",
    "AA38": "Source/type",
    "O48": "Thesis Bridge",
    "O49": "Quick valuation bridge; no market price required.",
    "O50": "Bridge item",
    "U50": "Value",
    "X50": "Notes",
    "O63": "Output",
    "U63": "Value",
    "X63": "Interpretation",
}
VALUATION_STRUCTURAL_HEADERS = {
    "B123": "Principal due ($m)",
    "C123": "Rate type",
    "D123": "Coupon/Spread %",
    "F123": "Maturity",
    "G123": "Conversion price",
    "I123": "Added shares on full conversion (m)",
    "L123": "Concurrent repurchased shares (m)",
    "B138": "Summary",
    "F138": "Score",
    "G138": "Severity",
    "H138": "Result / support",
    "B159": "Δ",
    "C159": "Direction",
    "D159": "As-of",
    "B169": "Status",
    "C169": "Evidence",
    "I169": "As-of",
}
VALUATION_BLUE_SECTION_HEADERS = {
    "O7",
    "O37",
    "O48",
    "A122",
    "A137",
    "N137",
    "A145",
    "A151",
    "A158",
    "A168",
    "B192",
}
VALUATION_BLUE_SECTION_HEADER_RANGES = (
    "A122:N122",
    "A145:M145",
    "A151:M151",
    "A158:D158",
    "B192:S192",
)
STANDARD_RED_GREEN_FLAG_LABELS = {
    170: "Red: Revenue up but CFO down (YoY)",
    171: "Red: Earnings quality CFO/NI (TTM)",
    172: "Red: AR growing faster than revenue (YoY)",
    173: "Red: Inventory build without revenue growth",
    174: "Red: Debt growing faster than revenue (YoY)",
    175: "Red: Leverage rising (YoY Δ)",
    176: "Red: Interest coverage low (cash)",
    177: "Red: FCF negative while EBITDA positive (TTM)",
    178: "Watch: Buybacks exceeded FCF",
    179: "Red: Goodwill heavy",
    180: "Red: Share dilution (YoY)",
    181: "Red: Pension obligations pressure",
    183: "Green: Operating margin trend QoQ",
    184: "Green: FCF TTM growth (YoY)",
    185: "Green: Net debt decreasing (YoY)",
    186: "Green: Interest coverage improving (YoY)",
    187: "Green: Shares outstanding decreasing (YoY)",
    188: "Green: Liquidity improving (YoY)",
}
STATUS_OUTPUT_FILL_COLORS = {
    "00D9EAF7",
    "00F2F2F2",
    "00F4CCCC",
    "00FFF2CC",
}
SECTION_BLUE = "006FA8DC"
HEADER_BLUE = "00EAF3FB"
OPERATING_DRIVER_SHEET_HEADERS = {
    "A12": "Topic",
    "B12": "Current read",
    "H12": "Source / use",
    "A19": "Horizon",
    "B19": "Stated in",
    "C19": "Commentary",
}
REQUIRED_SUPPORT_SHELL_SHEETS = {
    "Hidden_Value_Flags",
    "Revolver_History",
    "Debt_Tranches_Latest",
    "Debt_Profile",
    "Guidance_Normalized",
    "Quarter_Notes",
    "Promise_Progress",
    "History_Q",
}


def _load_validator():
    path = ROOT / "scripts" / "validate_standard_template_shell.py"
    spec = importlib.util.spec_from_file_location("validate_standard_template_shell", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _binding_payload() -> dict:
    import json

    return json.loads((ROOT / "docs" / "workbook_binding_map.json").read_text(encoding="utf-8"))


def _manifest_payload() -> dict:
    import json

    return json.loads((ROOT / "docs" / "standard_template_shell_manifest.json").read_text(encoding="utf-8"))


def _sheet_name(template: str, ticker: str = "ANF") -> str:
    return template.replace("{ticker}", ticker)


def _nonempty_count(ws, target: str) -> int:
    min_col, min_row, max_col, max_row = range_boundaries(target)
    count = 0
    for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            if cell.value not in (None, ""):
                count += 1
    return count


def _coord_in_ranges(coord: str, ranges: tuple[str, ...]) -> bool:
    row_idx, col_idx = coordinate_to_tuple(coord)
    for range_ref in ranges:
        min_col, min_row, max_col, max_row = range_boundaries(range_ref)
        if min_row <= row_idx <= max_row and min_col <= col_idx <= max_col:
            return True
    return False


def _sheet_counts(wb, sheet: str) -> tuple[int, int, int]:
    ws = wb[sheet]
    nonempty = 0
    formulas = 0
    for row in ws.iter_rows():
        for cell in row:
            if cell.value is not None:
                nonempty += 1
                if isinstance(cell.value, str) and cell.value.startswith("="):
                    formulas += 1
    return len(ws.merged_cells.ranges), nonempty, formulas


def test_standard_template_shell_artifact_exists() -> None:
    assert TEMPLATE.exists()
    assert TEMPLATE.suffix == ".xlsx"


def test_standard_template_shell_validation_passes() -> None:
    validator = _load_validator()
    report = validator.validate_shell(template_path=TEMPLATE)

    assert report["status"] == "PASS", report
    assert report["issue_count"] == 0
    json.dumps(report)


def test_standard_template_shell_is_rich_visual_shell_not_wireframe() -> None:
    template_wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    lab_wb = load_workbook(ANF_LAB, data_only=False, read_only=False)
    try:
        too_sparse: list[str] = []
        for sheet in RICH_VISIBLE_SHEETS:
            template_sheet = sheet
            lab_sheet = _sheet_name(sheet)
            template_merges, template_nonempty, template_formulas = _sheet_counts(template_wb, template_sheet)
            lab_merges, lab_nonempty, lab_formulas = _sheet_counts(lab_wb, lab_sheet)
            if template_merges < max(1, int(lab_merges * 0.55)):
                too_sparse.append(f"{sheet} merges {template_merges} < {lab_merges}")
            if template_nonempty < max(5, int(lab_nonempty * 0.03)):
                too_sparse.append(f"{sheet} nonempty {template_nonempty} < {lab_nonempty}")
            if lab_formulas and template_formulas < max(1, int(lab_formulas * 0.04)):
                too_sparse.append(f"{sheet} formulas {template_formulas} < {lab_formulas}")

        assert too_sparse == []
    finally:
        template_wb.close()
        lab_wb.close()


def test_standard_template_shell_binding_targets_are_blank() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders: list[str] = []
        for binding in _binding_payload()["bindings"]:
            if not binding["writable"]:
                continue
            ws = wb[binding["sheet"]]
            count = _nonempty_count(ws, binding["target"])
            if count:
                offenders.append(f"{binding['binding_id']} {binding['sheet']}!{binding['target']} nonempty={count}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_has_no_company_specific_text_in_writable_zones() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders: list[str] = []
        for sheet in _manifest_payload()["sheets"]:
            ws = wb[sheet["sheet"]]
            for zone in sheet["writable_zones"]:
                min_col, min_row, max_col, max_row = range_boundaries(zone["target"])
                for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
                    for cell in row:
                        value = str(cell.value or "")
                        for term in COMPANY_SPECIFIC_TERMS:
                            if term.lower() in value.lower():
                                offenders.append(f"{sheet['sheet']}!{cell.coordinate}: {value}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_has_no_stale_qa_excel_tables() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders = {
            sheet_name: list(wb[sheet_name].tables.keys())
            for sheet_name in ("QA_Log", "Needs_Review", "QA_Checks")
            if wb[sheet_name].tables
        }

        assert offenders == {}
    finally:
        wb.close()


def test_standard_template_shell_clears_representative_source_value_leaks() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        representative_source_cells = {
            "SUMMARY": ["A3", "A5", "A7", "B27", "B28", "B29"],
            "Valuation": ["B6", "B9", "M18", "S10", "AA14"],
            "BS_Segments": ["B7", "B47", "I49", "A50"],
            "Operating_Drivers": ["B6", "H6"],
            "{ticker}_Investment_Case": ["A185", "B191", "B209", "F221", "H231"],
            "Quarter_Notes_UI": ["B3", "B4", "B5", "B6"],
            "Promise_Progress_UI": ["B5", "C5", "G5", "C7"],
            "QA_Log": ["A2", "D1001", "J1001"],
            "Needs_Review": ["A2", "G2"],
            "QA_Checks": ["A2", "E2"],
        }
        offenders: list[str] = []
        for sheet_name, cells in representative_source_cells.items():
            ws = wb[sheet_name]
            for coord in cells:
                value = ws[coord].value
                if value not in (None, ""):
                    offenders.append(f"{sheet_name}!{coord}={value!r}")

        assert offenders == []
        assert wb["Valuation"]["O7"].value == "Guidance"
    finally:
        wb.close()


def test_standard_template_shell_has_no_valuation_numeric_or_date_constants() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        offenders = [
            f"Valuation!{cell.coordinate}={cell.value!r}"
            for row in ws.iter_rows()
            for cell in row
            if isinstance(cell.value, (int, float, date, datetime))
        ]

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_clears_history_evidence_and_fixed_period_labels() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        quarter_notes = wb["Quarter_Notes_UI"]
        allowed_history_text = {
            "quarter read",
            "model read",
            "what changed",
            "watch next",
            "key caveat",
            "key developments",
            "theme",
            "what happened",
            "why it matters",
            "model / valuation implication",
            "source / confidence",
            "guidance / promise interpretation",
            "promise / guidance item",
            "read",
            "actual / progress interpretation",
            "status / caveat",
            "source",
            "model mapping / double-count guardrails",
            "driver",
            "model treatment",
            "double-count guardrail",
            "linked sheet / metric",
        }
        offenders: list[str] = []
        for row in quarter_notes.iter_rows(min_row=16):
            for cell in row:
                value = cell.value
                if value in (None, "") or (isinstance(value, str) and value.startswith("=")):
                    continue
                text = str(value).strip()
                if text.casefold() in allowed_history_text:
                    continue
                if cell.column == 1 and re.fullmatch(r"Historical quarter notes \d+", text, re.I):
                    continue
                offenders.append(f"Quarter_Notes_UI!{cell.coordinate}: {text}")

        assert offenders == []
        assert quarter_notes["C42"].value is None
        assert wb["Operating_Drivers"]["A14"].value == "Current guidance period"
        assert wb["Promise_Progress_UI"]["A11"].value == "Guidance progression - period block 1"
        assert wb["{ticker}_Investment_Case"]["A156"].value is None
    finally:
        wb.close()


def test_visible_shell_contains_no_internal_slot_placeholders() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders: list[str] = []
        for ws in wb.worksheets:
            if ws.sheet_state != "visible":
                continue
            for row in ws.iter_rows():
                for cell in row:
                    if not isinstance(cell.value, str):
                        continue
                    text = cell.value.strip()
                    if re.search(r"\[[^\]]*(?:slot|dimension member|quality of earnings item)[^\]]*\]", text, re.I):
                        offenders.append(f"{ws.title}!{cell.coordinate}: {text}")

        assert offenders == []
    finally:
        wb.close()


def test_valuation_lower_runtime_value_status_and_date_cells_are_blank() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        runtime_constant_ranges = (
            "D194:D216",
            "E236:E240",
            "D247:D250",
            "E253:E256",
            "L248:S250",
        )
        offenders: list[str] = []
        for range_ref in runtime_constant_ranges:
            min_col, min_row, max_col, max_row = range_boundaries(range_ref)
            for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
                for cell in row:
                    value = cell.value
                    if value in (None, ""):
                        continue
                    if isinstance(value, str) and value.startswith("="):
                        continue
                    offenders.append(f"Valuation!{cell.coordinate}={value!r}")

        assert ws["D195"].value in (None, "")
        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_has_no_visible_signal_leakage_in_valuation_outputs() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        offenders: list[str] = []
        for row in ws.iter_rows(min_row=6, max_row=ws.max_row):
            for cell in row:
                if cell.coordinate in VALUATION_BLUE_SECTION_HEADERS or _coord_in_ranges(cell.coordinate, VALUATION_BLUE_SECTION_HEADER_RANGES):
                    continue
                fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                if fill in SIGNAL_FILL_COLORS:
                    offenders.append(f"Valuation!{cell.coordinate} fill={fill} value={cell.value!r}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_has_no_red_green_status_outputs() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        status_terms = {"PASS", "WARN", "FAIL", "N/A"}
        offenders: list[str] = []
        for row in ws.iter_rows(min_row=170, max_row=188, min_col=2, max_col=13):
            for cell in row:
                value = str(cell.value or "").strip()
                if value in status_terms or any(term in value for term in ("CFO/NI", "FCF TTM", "Net debt YoY", "Shares YoY")):
                    offenders.append(f"Valuation!{cell.coordinate}={value!r}")

        assert offenders == []
    finally:
        wb.close()


def test_valuation_preserves_standard_structural_headers_and_section_fills() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        header_offenders = {
            coord: ws[coord].value
            for coord, expected in {**VALUATION_GUIDANCE_SIDECAR_HEADERS, **VALUATION_STRUCTURAL_HEADERS}.items()
            if ws[coord].value != expected
        }
        fill_offenders = {
            coord: ws[coord].fill.fgColor.rgb if ws[coord].fill and ws[coord].fill.fgColor.type == "rgb" else ""
            for coord in VALUATION_BLUE_SECTION_HEADERS
            if (ws[coord].fill.fgColor.rgb if ws[coord].fill and ws[coord].fill.fgColor.type == "rgb" else "") != "006FA8DC"
        }

        assert header_offenders == {}
        assert fill_offenders == {}
    finally:
        wb.close()


def test_valuation_lower_blocks_preserve_template_bands_merges_and_font_sizes() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        merged_ranges = {str(range_ref) for range_ref in ws.merged_cells.ranges}

        for col_idx in range(1, 15):
            cell = ws.cell(122, col_idx)
            assert cell.fill.fgColor.rgb == SECTION_BLUE
        assert ws["A122"].font.sz == 12

        assert "D123:E123" in merged_ranges
        assert "H138:I138" in merged_ranges
        for coord in ("B123", "C123", "D123", "F123", "G123", "I123", "L123"):
            assert ws[coord].fill.fgColor.rgb == HEADER_BLUE
            assert ws[coord].font.sz == 12
        assert ws["E123"].fill.fgColor.rgb in {HEADER_BLUE, "00000000"}

        for coord in ("B138", "F138", "G138", "H138"):
            assert ws[coord].font.sz == 12
            assert ws[coord].fill.fgColor.rgb == HEADER_BLUE

        for col_idx in range(1, 14):
            assert ws.cell(145, col_idx).fill.fgColor.rgb == SECTION_BLUE
            assert ws.cell(151, col_idx).fill.fgColor.rgb == SECTION_BLUE
        for col_idx in range(1, 5):
            assert ws.cell(158, col_idx).fill.fgColor.rgb == SECTION_BLUE

        assert ws["B192"].value == "Valuation"
        assert ws["B192"].fill.fgColor.rgb == SECTION_BLUE
        assert ws["B192"].font.sz == 18
    finally:
        wb.close()


def test_valuation_red_green_flag_column_uses_standard_rule_labels() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        offenders = {
            f"A{row_idx}": ws.cell(row_idx, 1).value
            for row_idx, expected in STANDARD_RED_GREEN_FLAG_LABELS.items()
            if ws.cell(row_idx, 1).value != expected
        }
        slot_labels = [
            f"A{row_idx}={ws.cell(row_idx, 1).value!r}"
            for row_idx in range(170, 189)
            if str(ws.cell(row_idx, 1).value or "").startswith("[Red/green flag slot")
        ]

        assert offenders == {}
        assert slot_labels == []
    finally:
        wb.close()


def test_valuation_red_green_headers_match_template_alignment_and_font_size() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]

        assert ws["A168"].fill.fgColor.rgb == SECTION_BLUE
        assert ws["A168"].font.sz == 12
        for coord in ("A169", "B169", "C169", "I169"):
            cell = ws[coord]
            assert cell.fill.fgColor.rgb == HEADER_BLUE
            assert cell.font.sz == 12
            assert cell.alignment.horizontal == "center"
            assert cell.alignment.vertical == "center"
    finally:
        wb.close()


def test_valuation_blank_status_and_value_cells_are_visually_neutral() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        offenders: list[str] = []
        for range_ref in ("B170:I188", "U51:U62"):
            min_col, min_row, max_col, max_row = range_boundaries(range_ref)
            for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
                for cell in row:
                    if cell.value not in (None, ""):
                        continue
                    fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                    if fill not in NEUTRAL_BLANK_FILLS:
                        offenders.append(f"Valuation!{cell.coordinate} fill={fill}")

        assert offenders == []
    finally:
        wb.close()


def test_promise_progress_ui_preserves_standard_guidance_revision_columns() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Promise_Progress_UI"]
        annual_headers = {
            row_idx: [ws.cell(row_idx, col).value for col in range(1, 10)]
            for row_idx in PROMISE_ANNUAL_HEADER_ROWS
        }
        revision_headers = {
            row_idx: [ws.cell(row_idx, col).value for col in range(1, 12)]
            for row_idx in PROMISE_REVISION_HEADER_ROWS
        }

        assert annual_headers == {row_idx: REQUIRED_PROMISE_ANNUAL_HEADERS for row_idx in PROMISE_ANNUAL_HEADER_ROWS}
        assert revision_headers == {row_idx: REQUIRED_PROMISE_REVISION_HEADERS for row_idx in PROMISE_REVISION_HEADER_ROWS}
    finally:
        wb.close()


def test_valuation_guidance_sidecar_preserves_repeated_standard_headers() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Valuation"]
        offenders = {
            coord: ws[coord].value
            for coord, expected in VALUATION_GUIDANCE_SIDECAR_HEADERS.items()
            if ws[coord].value != expected
        }

        assert offenders == {}
    finally:
        wb.close()


def test_operating_drivers_sheet_preserves_standard_subheaders() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Operating_Drivers"]
        offenders = {
            coord: ws[coord].value
            for coord, expected in OPERATING_DRIVER_SHEET_HEADERS.items()
            if ws[coord].value != expected
        }

        assert offenders == {}
    finally:
        wb.close()


def test_long_narrative_zones_are_wrapped_and_resized() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        drivers = wb["Operating_Drivers"]
        for row_idx in (6, 7, 8, 9, 14, 15):
            assert drivers[f"B{row_idx}"].alignment.wrap_text is True
            assert drivers.row_dimensions[row_idx].height == 42.0

        promise = wb["Promise_Progress_UI"]
        for coord in ("B19", "C19", "D19", "E19", "B67", "C67"):
            assert promise[coord].alignment.wrap_text is True
        assert promise.row_dimensions[19].height == 60.0
        assert promise.row_dimensions[67].height == 42.0
    finally:
        wb.close()


def test_operating_drivers_title_is_on_row_1_and_only_top_row_is_frozen() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        ws = wb["Operating_Drivers"]
        merged_ranges = {str(range_ref) for range_ref in ws.merged_cells.ranges}

        assert ws.freeze_panes == "A2"
        assert "A1:N1" in merged_ranges
        assert "A2:N2" not in merged_ranges
        assert ws["A1"].value == "Operating Drivers"
        assert ws["A2"].value in (None, "")
        assert ws["A1"].fill.fgColor.rgb == SECTION_BLUE
        assert ws["A1"].font.sz == 15
        assert ws["A1"].font.bold is True
        assert ws["A1"].alignment.horizontal == "center"
        assert ws["A1"].alignment.vertical == "center"
        assert ws.row_dimensions[1].height == 24
    finally:
        wb.close()


def test_standard_template_shell_required_support_sheets_are_neutral_shells() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        hidden_sheets = {ws.title for ws in wb.worksheets if ws.sheet_state != "visible"}

        assert REQUIRED_SUPPORT_SHELL_SHEETS <= hidden_sheets
        assert hidden_sheets <= ALLOWED_HIDDEN_SHELL_SHEETS

        for sheet_name in REQUIRED_SUPPORT_SHELL_SHEETS:
            ws = wb[sheet_name]
            assert ws.sheet_state != "visible"
            assert any(ws.cell(1, col).value not in (None, "") for col in range(1, min(ws.max_column, 12) + 1))
            assert _nonempty_count(ws, f"A2:{ws.cell(max(ws.max_row, 2), max(ws.max_column, 1)).coordinate}") == 0
    finally:
        wb.close()


def test_standard_template_shell_has_no_fixed_sector_or_dimension_rows() -> None:
    import re

    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders: list[str] = []
        patterns = [
            *(re.compile(r"\b" + re.escape(term) + r"\b", re.I) for term in FIXED_DIMENSION_MEMBERS),
            *(re.compile(pattern, re.I) for pattern in FIXED_SECTOR_LABEL_REGEXES),
        ]
        for ws in wb.worksheets:
            if ws.sheet_state != "visible":
                continue
            for row in ws.iter_rows():
                for cell in row:
                    if not isinstance(cell.value, str) or cell.value.startswith("="):
                        continue
                    value = cell.value
                    for pattern in patterns:
                        if pattern.search(value):
                            offenders.append(f"{ws.title}!{cell.coordinate}={value!r}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_blank_writable_zones_have_no_data_like_fills() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders: list[str] = []
        for sheet in _manifest_payload()["sheets"]:
            ws = wb[sheet["sheet"]]
            for zone in sheet["writable_zones"]:
                min_col, min_row, max_col, max_row = range_boundaries(zone["target"])
                for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
                    for cell in row:
                        if cell.value not in (None, ""):
                            continue
                        fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                        if fill in SIGNAL_FILL_COLORS or fill in GRAY_BLANK_FILLS:
                            offenders.append(f"{sheet['sheet']}!{cell.coordinate} fill={fill}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_visible_blank_cells_have_no_gray_data_fills() -> None:
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        offenders: list[str] = []
        for ws in wb.worksheets:
            if ws.sheet_state != "visible":
                continue
            for row in ws.iter_rows():
                for cell in row:
                    if cell.value not in (None, ""):
                        continue
                    fill = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor.type == "rgb" else ""
                    if fill in GRAY_BLANK_FILLS:
                        offenders.append(f"{ws.title}!{cell.coordinate} fill={fill}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_pass_does_not_create_gtx_workbook() -> None:
    data_root = ROOT.parent / "StockModelData"
    forbidden = [
        data_root / "outputs" / "stress_tests" / "GTX_new_ticker_engine" / "GTX_model.xlsx",
        data_root / "outputs" / "Excel stock models" / "GTX_model.xlsx",
        data_root / "outputs" / "Excel stock models" / "GTX_model.xlsm",
    ]

    assert [str(path) for path in forbidden if path.exists()] == []


def test_standard_template_visual_gap_audit_covers_all_visible_sheets() -> None:
    import json

    assert VISUAL_GAP_AUDIT_JSON.exists()
    assert VISUAL_GAP_AUDIT_MD.exists()

    payload = json.loads(VISUAL_GAP_AUDIT_JSON.read_text(encoding="utf-8"))
    sheet_reports = payload["sheet_reports"]
    assert {report["sheet"] for report in sheet_reports} >= set(_manifest_payload()["visible_sheet_order"])

    for report in sheet_reports:
        assert report["preview_mode"] == "openpyxl_static_not_excel_com"
        assert "used_range" in report
        assert "non_empty_cells" in report["source_lab"]
        assert "non_empty_cells" in report["standard_shell"]
        assert "static_template_label_count" in report["standard_shell"]
        assert "row_label_count" in report["standard_shell"]
        assert "formula_count" in report["standard_shell"]
        assert "merge_count" in report["standard_shell"]
        assert "blank_writable_cells" in report
        assert isinstance(report["visually_complete"], bool)


def test_standard_template_shell_hidden_package_is_neutral() -> None:
    import re

    term_re = re.compile("|".join(r"\b" + re.escape(term) + r"\b" for term in COMPANY_SPECIFIC_TERMS), re.I)
    filename_re = re.compile(r"\b(anf|pbi|gpre|gtx)[-_][^\s]*\.(htm|html|pdf|xlsx|xls)\b", re.I)
    wb = load_workbook(TEMPLATE, data_only=False, read_only=False)
    try:
        hidden_sheets = {ws.title for ws in wb.worksheets if ws.sheet_state != "visible"}
        assert hidden_sheets <= ALLOWED_HIDDEN_SHELL_SHEETS

        offenders: list[str] = []
        for ws in wb.worksheets:
            if ws.sheet_state == "visible":
                continue
            for row in ws.iter_rows():
                for cell in row:
                    value = cell.value
                    if value in (None, ""):
                        continue
                    text = str(value)
                    if term_re.search(text) or filename_re.search(text):
                        offenders.append(f"{ws.title}!{cell.coordinate}={text[:120]}")

        assert offenders == []
    finally:
        wb.close()


def test_standard_template_shell_package_has_no_source_specific_xml_leakage() -> None:
    validator = _load_validator()

    assert validator._package_source_leakage_parts(TEMPLATE) == []


def test_standard_template_hidden_support_audit_covers_deleted_and_retained_sheets() -> None:
    import json

    assert HIDDEN_SUPPORT_AUDIT_JSON.exists()
    assert HIDDEN_SUPPORT_AUDIT_MD.exists()

    payload = json.loads(HIDDEN_SUPPORT_AUDIT_JSON.read_text(encoding="utf-8"))
    assert payload["package_dependency_check"]["missing_defined_name_sheets"] == []
    assert payload["package_dependency_check"]["missing_visible_formula_sheets"] == []
    assert payload["post_neutralization_summary"]["company_source_leakage_cells"] == 0

    rows_by_sheet = {row["sheet_name"]: row for row in payload["hidden_support_sheets"]}
    assert "Hidden_Value_Flags" in rows_by_sheet
    assert rows_by_sheet["Hidden_Value_Flags"]["present_in_shell"] is True
    assert rows_by_sheet["Hidden_Value_Flags"]["classification"] == "keep_formula_dependency"

    deleted = [row for row in payload["hidden_support_sheets"] if row["classification"] == "delete_from_shell"]
    assert deleted
    assert all(row["present_in_shell"] is False for row in deleted)


def test_standard_template_neutrality_audit_has_no_remaining_non_neutral_items() -> None:
    import json

    assert NEUTRALITY_AUDIT_JSON.exists()
    assert NEUTRALITY_AUDIT_MD.exists()

    payload = json.loads(NEUTRALITY_AUDIT_JSON.read_text(encoding="utf-8"))
    summary = payload["post_neutrality_summary"]
    assert summary["company_specific_value_count"] == 0
    assert summary["company_specific_text_count"] == 0
    assert summary["sector_specific_label_count"] == 0
    assert summary["fixed_dimension_member_count"] == 0
    assert summary["source_specific_text_count"] == 0
    assert summary["valuation_numeric_constant_count"] == 0
    assert summary["signal_fill_without_value_count"] == 0
    assert summary["valuation_signal_fill_count"] == 0
    assert summary["blank_writable_non_neutral_fill_count"] == 0
    assert summary["visible_blank_gray_fill_count"] == 0
    assert summary["red_green_status_output_count"] == 0
    assert summary["blank_status_or_value_fill_count"] == 0
    assert summary["visible_value_date_status_constant_count"] == 0
    assert summary["visible_company_source_text_count"] == 0
    assert summary["missing_required_support_shell_sheet_count"] == 0


def test_standard_template_sheet_inventory_and_lifecycle_docs_exist() -> None:
    import json

    assert SHEET_INVENTORY_JSON.exists()
    assert SUPPORT_LIFECYCLE_JSON.exists()

    inventory = json.loads(SHEET_INVENTORY_JSON.read_text(encoding="utf-8"))
    lifecycle = json.loads(SUPPORT_LIFECYCLE_JSON.read_text(encoding="utf-8"))
    inventory_rows = {row["sheet_name"]: row for row in inventory["sheets"]}
    lifecycle_rows = {row["sheet_name"]: row for row in lifecycle["support_sheets"]}

    for sheet_name in REQUIRED_SUPPORT_SHELL_SHEETS:
        assert inventory_rows[sheet_name]["classification"] == "required_support_shell_sheet"
        assert inventory_rows[sheet_name]["present_in_standard_shell"] is True
        assert lifecycle_rows[sheet_name]["owner"] == "frozen_shell"
        assert lifecycle_rows[sheet_name]["neutral_shell_required"] is True

    for sheet_name in ("Guidance_Raw", "Promise_Evidence", "Quarter_Notes_Audit", "DATA_Facts_Long"):
        assert sheet_name in lifecycle_rows
        assert lifecycle_rows[sheet_name]["owner"] == "value_only_runtime"
        assert lifecycle_rows[sheet_name]["neutral_shell_required"] is False


def test_root_default_validation_tickers_remain_standard_three() -> None:
    text = (ROOT / "pbi_xbrl" / "workbook_validation_runner.py").read_text(encoding="utf-8")

    assert 'TICKERS: Sequence[str] = ("PBI", "GPRE", "ANF")' in text
    assert '"GTX"' not in text.split("TICKERS: Sequence[str] =", 1)[1].split("\n", 1)[0]
