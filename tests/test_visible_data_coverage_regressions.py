from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet


WORKBOOK_DIR = Path(
    os.environ.get(
        "STOCK_MODEL_WORKBOOK_DIR",
        r"C:\Users\Jibbe\Aktier\StockModelData\outputs\Excel stock models",
    )
)


def _model_path(ticker: str) -> Path:
    candidates = [WORKBOOK_DIR / f"{ticker}_model.xlsm", WORKBOOK_DIR / f"{ticker}_model.xlsx"]
    for path in candidates:
        if path.exists():
            return path
    pytest.skip(f"{ticker} model not found in {WORKBOOK_DIR}")


def _load_model(ticker: str):
    return load_workbook(_model_path(ticker), data_only=True, read_only=True, keep_vba=True)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except ValueError:
        return None


def _find_row(ws: Worksheet, label: str, *, start: int = 1, end: int | None = None, col: int = 1) -> int:
    max_row = end or int(ws.max_row or 0)
    for rr in range(start, max_row + 1):
        if _text(ws.cell(rr, col).value) == label:
            return rr
    raise AssertionError(f"{ws.title}: could not find row label {label!r}")


def _find_row_contains(ws: Worksheet, label: str, *, start: int = 1, end: int | None = None) -> int:
    max_row = end or int(ws.max_row or 0)
    needle = label.lower()
    for rr in range(start, max_row + 1):
        row_text = " ".join(_text(ws.cell(rr, cc).value) for cc in range(1, int(ws.max_column or 0) + 1))
        if needle in row_text.lower():
            return rr
    raise AssertionError(f"{ws.title}: could not find row containing {label!r}")


def _quarter_col(ws: Worksheet, header_row: int, quarter: str) -> int:
    for cc in range(1, int(ws.max_column or 0) + 1):
        if _text(ws.cell(header_row, cc).value) == quarter:
            return cc
    raise AssertionError(f"{ws.title}: quarter {quarter!r} missing from row {header_row}")


def _find_segment_row_after_group(ws: Worksheet, group: str, segment: str, *, start: int) -> int:
    group_row = _find_row(ws, group, start=start)
    for rr in range(group_row + 1, min(group_row + 10, int(ws.max_row or 0)) + 1):
        if _text(ws.cell(rr, 1).value) == segment:
            return rr
    raise AssertionError(f"{ws.title}: segment {segment!r} missing under {group!r}")


def _promise_row(ws: Worksheet, section: str, metric: str) -> int:
    section_row = _find_row(ws, section)
    header_row = _find_row(ws, "Metric", start=section_row)
    for rr in range(header_row + 1, min(header_row + 12, int(ws.max_row or 0)) + 1):
        if _text(ws.cell(rr, 1).value) == metric:
            return rr
    raise AssertionError(f"{ws.title}: metric {metric!r} missing under {section!r}")


def test_pbi_operating_drivers_includes_2026_q1_segment_support_from_release() -> None:
    wb = _load_model("PBI")
    ws = wb["Operating_Drivers"]
    section = _find_row_contains(ws, "Segment support", start=1)
    header = _find_row(ws, "Metric / segment", start=section)
    q1_2026 = _quarter_col(ws, header, "2026-Q1")

    checks = [
        ("Revenue ($m)", "SendTech Solutions", 313.947),
        ("Revenue ($m)", "Presort Services", 163.466),
        ("Adj EBIT / operating profit ($m)", "SendTech Solutions", 113.530),
        ("Adj EBIT / operating profit ($m)", "Presort Services", 39.178),
        ("D&A ($m)", "SendTech Solutions", 9.875),
        ("D&A ($m)", "Presort Services", 8.736),
        ("Adj EBITDA ($m)", "SendTech Solutions", 123.405),
        ("Adj EBITDA ($m)", "Presort Services", 47.914),
    ]
    for group, segment, expected in checks:
        row = _find_segment_row_after_group(ws, group, segment, start=header)
        assert _num(ws.cell(row, q1_2026).value) == pytest.approx(expected, abs=0.01), (
            f"PBI Operating_Drivers {group}/{segment} 2026-Q1 should come from the Q1 2026 release"
        )
    wb.close()


def test_pbi_promise_adjusted_ebit_and_eps_progress_are_source_backed() -> None:
    wb = _load_model("PBI")
    ws = wb["Promise_Progress_UI"]

    expected_rows = {
        ("2025-Q2 revisions", "Adjusted EBIT guidance"): ("$102.3m", "YTD: $222.0m"),
        ("2025-Q3 revisions", "Adjusted EBIT guidance"): ("$107.3m", "YTD: $329.3m"),
        ("2024-Q3 revisions", "Adjusted EBIT guidance"): ("$102.8m", "YTD: $270.8m"),
        ("2026-Q1 revisions", "Adjusted EPS guidance"): ("$0.47", "YTD: $0.47"),
    }
    for (section, metric), (actual, progress) in expected_rows.items():
        row = _promise_row(ws, section, metric)
        assert _text(ws.cell(row, 5).value) == actual
        assert _text(ws.cell(row, 6).value) == progress
        assert _text(ws.cell(row, 7).value) != "Completed"
    wb.close()


def test_anf_operating_drivers_keeps_brand_and_geography_q4_values() -> None:
    wb = _load_model("ANF")
    ws = wb["Operating_Drivers"]
    section = _find_row_contains(ws, "Actuals", start=1)
    header = _find_row(ws, "Quarter", start=section)

    expected_by_row = {
        "Americas sales": {"2023-Q4": 1193.3, "2024-Q4": 1319.7, "2025-Q4": 1383.9},
        "Abercrombie sales": {"2023-Q4": 755.2, "2024-Q4": 772.7, "2025-Q4": 806.5},
        "Hollister sales": {"2023-Q4": 697.7, "2024-Q4": 812.2, "2025-Q4": 863.3},
        "Net sales": {"2023-Q4": 1452.9, "2024-Q4": 1584.9, "2025-Q4": 1669.8},
    }
    for row_label, quarter_values in expected_by_row.items():
        row = _find_row(ws, row_label, start=header)
        for quarter, expected in quarter_values.items():
            col = _quarter_col(ws, header, quarter)
            assert _num(ws.cell(row, col).value) == pytest.approx(expected, abs=0.1), (
                f"ANF Operating_Drivers {row_label} {quarter} should not lose source-backed Q4 data"
            )

    for quarter in ("2023-Q4", "2024-Q4", "2025-Q4"):
        col = _quarter_col(ws, header, quarter)
        abercrombie = _num(ws.cell(_find_row(ws, "Abercrombie sales", start=header), col).value)
        hollister = _num(ws.cell(_find_row(ws, "Hollister sales", start=header), col).value)
        net_sales = _num(ws.cell(_find_row(ws, "Net sales", start=header), col).value)
        assert abercrombie is not None and hollister is not None and net_sales is not None
        assert abercrombie + hollister == pytest.approx(net_sales, abs=0.3)
    wb.close()


def test_anf_valuation_adjusted_eps_and_ttm_uses_source_backed_older_quarters() -> None:
    wb = _load_model("ANF")
    ws = wb["Valuation"]
    header = _find_row(ws, "Quarter")
    adj_eps_row = _find_row(ws, "Adj EPS", start=header)
    adj_eps_ttm_row = _find_row(ws, "Adj EPS (TTM)", start=header)
    ebitda_ttm_row = _find_row(ws, "EBITDA (TTM)", start=header)
    adj_ebitda_ttm_row = _find_row(ws, "Adj EBITDA (TTM)", start=header)
    adj_ebit_ttm_row = _find_row(ws, "Adj EBIT (TTM)", start=header)
    net_leverage_row = _find_row(ws, "Net leverage", start=header)

    for quarter, expected in {"2023-Q2": 1.10, "2023-Q3": 1.83, "2023-Q4": 2.97}.items():
        col = _quarter_col(ws, header, quarter)
        assert _num(ws.cell(adj_eps_row, col).value) == pytest.approx(expected, abs=0.01)
    assert _num(ws.cell(adj_eps_ttm_row, _quarter_col(ws, header, "2023-Q4")).value) == pytest.approx(6.29, abs=0.02)
    q1_2023_col = _quarter_col(ws, header, "2023-Q1")
    assert _num(ws.cell(ebitda_ttm_row, q1_2023_col).value) == pytest.approx(270.765, abs=0.002)
    assert _num(ws.cell(adj_ebitda_ttm_row, q1_2023_col).value) == pytest.approx(270.765, abs=0.002)
    assert _num(ws.cell(adj_ebit_ttm_row, q1_2023_col).value) == pytest.approx(151.427, abs=0.002)
    q1_2025_col = _quarter_col(ws, header, "2025-Q1")
    assert _num(ws.cell(net_leverage_row, q1_2025_col).value) == pytest.approx(-0.5888, abs=0.001)

    hist = wb["History_Q"]
    headers = [_text(hist.cell(1, cc).value) for cc in range(1, int(hist.max_column or 0) + 1)]
    idx = {h: i + 1 for i, h in enumerate(headers)}
    assert {"quarter", "da", "ebitda"}.issubset(idx), "History_Q should expose D&A and EBITDA source fields"
    q2_2022_row = None
    for rr in range(2, int(hist.max_row or 0) + 1):
        if str(hist.cell(rr, idx["quarter"]).value)[:10] == "2022-07-30":
            q2_2022_row = rr
            break
    assert q2_2022_row is not None, "ANF hidden history should keep 2022-Q2 for 2023-Q1 TTM support"
    assert _num(hist.cell(q2_2022_row, idx["da"]).value) == pytest.approx(31_655_000.0, abs=1.0)
    assert _num(hist.cell(q2_2022_row, idx["ebitda"]).value) == pytest.approx(29_464_000.0, abs=1.0)
    wb.close()


def test_gpre_visible_adjusted_metrics_do_not_show_micro_transcript_artifacts() -> None:
    wb = _load_model("GPRE")
    for sheet_name in ("Valuation", "Operating_Drivers"):
        ws = wb[sheet_name]
        for row in ws.iter_rows():
            row_label = _text(row[0].value).lower() if row else ""
            if not row_label or not any(token in row_label for token in ("adj ebitda", "adj fcf", "adj eps")):
                continue
            if any(token in row_label for token in ("margin", "yoy", "%")):
                continue
            for cell in row[1:]:
                value = _num(cell.value)
                assert value is None or not (0 < abs(value) < 0.001), (
                    f"GPRE {sheet_name} {cell.coordinate} still shows suspicious micro value {cell.value!r}"
                )
    wb.close()


def test_gpre_operating_drivers_excludes_spurious_future_quarters() -> None:
    wb = _load_model("GPRE")
    ws = wb["Operating_Drivers"]
    header = _find_row(ws, "Quarter")
    labels = [_text(ws.cell(header, cc).value) for cc in range(2, int(ws.max_column or 0) + 1)]
    assert "2027-Q1" not in labels
    assert "2029-Q2" not in labels
    assert "2049-Q3" not in labels
    assert "2026-Q1" in labels
    wb.close()


def test_gpre_promise_facility_qualification_remains_progress_not_completion() -> None:
    wb = _load_model("GPRE")
    ws = wb["Promise_Progress_UI"]
    row = _promise_row(ws, "2026-Q1 revisions", "45Z facility qualification")
    assert _text(ws.cell(row, 5).value) == ""
    progress = _text(ws.cell(row, 6).value)
    assert "All 8" in progress
    assert "operational" in progress.lower() or "qualifying" in progress.lower()
    assert _text(ws.cell(row, 7).value) == "On track"
    assert _text(ws.cell(row, 8).value) == "2026-Q1"
    assert _text(ws.cell(row, 9).value) == "2026-Q1"
    assert _text(ws.cell(row, 10).value) == "2026-03-31"
    assert "2049" not in " ".join(_text(ws.cell(row, cc).value) for cc in range(1, 12))
    wb.close()


def test_gpre_investment_case_45z_baseline_uses_reported_baseline_not_unknown() -> None:
    wb = _load_model("GPRE")
    wb_formula = load_workbook(_model_path("GPRE"), data_only=False, read_only=True, keep_vba=True)
    ws = wb["GPRE_Investment_Case"]
    ws_formula = wb_formula["GPRE_Investment_Case"]
    row = _find_row(ws, "Incremental 45Z uplift vs baseline")
    source_baseline = _num(ws.cell(28, 3).value)
    active_default = _num(ws.cell(28, 7).value) or _num(ws.cell(28, 4).value) or _num(ws.cell(28, 3).value)
    assert source_baseline is not None and source_baseline > 0
    assert active_default == pytest.approx(212.5, abs=0.1)
    assert "$C$28" in _text(ws_formula.cell(row, 2).value)
    assert "$G$28" in _text(ws_formula.cell(row, 3).value)
    assert f"C{row}-B{row}" in _text(ws_formula.cell(row, 4).value)
    wb_formula.close()
    wb.close()


def test_gpre_revolver_facility_size_is_not_below_reported_availability() -> None:
    wb = _load_model("GPRE")
    ws = wb["Valuation"]
    header = _find_row(ws, "Quarter")
    facility_row = _find_row(ws, "Revolver facility size", start=header)
    availability_row = _find_row(ws, "Revolver availability", start=header)
    for quarter in ("2023-Q4", "2025-Q4", "2026-Q1"):
        col = _quarter_col(ws, header, quarter)
        facility = _num(ws.cell(facility_row, col).value)
        availability = _num(ws.cell(availability_row, col).value)
        assert facility is None or availability is None or facility + 0.001 >= availability, (
            f"GPRE revolver facility size {quarter} should not be below availability"
        )
    wb.close()


def test_anf_bs_driver_yoy_uses_hidden_prior_quarters_for_visible_window() -> None:
    wb = _load_model("ANF")
    ws = wb["BS_Segments"]
    header = _find_row(ws, "Quarter")
    for row_label in ("Inventory YoY", "Sales YoY", "Diluted shares YoY"):
        row = _find_row(ws, row_label, start=header)
        for quarter in ("2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"):
            col = _quarter_col(ws, header, quarter)
            assert _num(ws.cell(row, col).value) is not None, (
                f"ANF BS_Segments {row_label} {quarter} should use hidden prior-year quarters"
            )
    wb.close()


def test_anf_older_promise_progressions_have_source_backed_actuals() -> None:
    wb = _load_model("ANF")
    ws = wb["Promise_Progress_UI"]
    checks = [
        ("2024 guidance progression", "Capex", "Mixed"),
        ("2023 guidance progression", "Q1 sales growth", "Hit"),
        ("2023 guidance progression", "Capex", "Hit"),
        ("2022 guidance progression", "Net sales growth", "Mixed"),
    ]
    for section, metric, status in checks:
        row = _promise_row(ws, section, metric)
        assert _text(ws.cell(row, 7).value), f"{section} {metric} should have Actual"
        assert _text(ws.cell(row, 8).value) == status
    wb.close()
