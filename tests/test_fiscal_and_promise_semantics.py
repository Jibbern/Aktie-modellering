import datetime as dt
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from openpyxl import Workbook, load_workbook

from pbi_xbrl.excel_writer_context import (
    _history_q_latest_full_year_actuals_from_workbook,
    _history_q_latest_full_year_period_set,
)


WORKBOOK_DIR = Path(os.environ.get("STOCK_MODEL_WORKBOOK_DIR", r"C:\Users\Jibbe\Aktier\Excel stock models"))
TICKERS = ("PBI", "GPRE", "ANF")


def _load_model(ticker: str):
    path = WORKBOOK_DIR / f"{ticker}_model.xlsx"
    if not path.exists():
        pytest.skip(f"{path} is not available for workbook semantic regression tests")
    return load_workbook(path, data_only=True, read_only=False)


def _cell_text(value: Any) -> str:
    return str(value or "").strip()


def _history_q_test_workbook(rows: List[Tuple[dt.date, float]]) -> Workbook:
    wb = Workbook()
    ws = wb.active
    ws.title = "History_Q"
    ws.append(["quarter", "revenue"])
    for qd, revenue_m in rows:
        ws.append([dt.datetime(qd.year, qd.month, qd.day), revenue_m * 1_000_000])
    return wb


def _sum_history_revenue_m(wb: Any, dates: List[dt.date]) -> float:
    ws = wb["History_Q"]
    headers = {
        re.sub(r"[^a-z0-9]+", "", _cell_text(ws.cell(1, cc).value).lower()): cc
        for cc in range(1, int(ws.max_column or 0) + 1)
    }
    q_col = headers["quarter"]
    revenue_col = headers["revenue"]
    wanted = set(dates)
    total = 0.0
    found: List[dt.date] = []
    for rr in range(2, int(ws.max_row or 0) + 1):
        raw_q = ws.cell(rr, q_col).value
        qd = raw_q.date() if hasattr(raw_q, "date") else raw_q
        if qd in wanted:
            found.append(qd)
            total += float(ws.cell(rr, revenue_col).value or 0.0)
    assert set(found) == wanted, f"History_Q missing revenue rows for {sorted(wanted - set(found))}"
    return total / 1_000_000.0 if abs(total) > 10_000.0 else total


def _section_event(title: str) -> str:
    return re.sub(r"\s+revisions$", "", title.strip(), flags=re.I)


def _promise_revision_blocks(ws: Any) -> Dict[str, List[Tuple[int, Dict[str, Any]]]]:
    blocks: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
    rr = 1
    while rr <= int(ws.max_row or 0):
        title = _cell_text(ws.cell(rr, 1).value)
        if not title.endswith("revisions"):
            rr += 1
            continue
        header_row = rr + 1
        headers = {
            _cell_text(ws.cell(header_row, cc).value).lower(): cc
            for cc in range(1, min(int(ws.max_column or 0), 13) + 1)
            if _cell_text(ws.cell(header_row, cc).value)
        }
        rows: List[Tuple[int, Dict[str, Any]]] = []
        body_row = header_row + 1
        while body_row <= int(ws.max_row or 0):
            first = _cell_text(ws.cell(body_row, 1).value)
            if first.endswith("revisions") or first.endswith("guidance progression") or first.endswith("open guidance"):
                break
            values = {name: ws.cell(body_row, col).value for name, col in headers.items()}
            metric = _cell_text(values.get("metric") or values.get("milestone"))
            if metric and metric.lower() not in {"metric", "milestone"}:
                rows.append((body_row, values))
            body_row += 1
        blocks[title] = rows
        rr = body_row
    return blocks


def _all_promise_revision_rows(ticker: str) -> List[Tuple[str, int, Dict[str, Any]]]:
    wb = _load_model(ticker)
    try:
        ws = wb["Promise_Progress_UI"]
        return [
            (block, row_idx, row)
            for block, rows in _promise_revision_blocks(ws).items()
            for row_idx, row in rows
        ]
    finally:
        wb.close()


def test_anf_real_workbook_latest_full_year_uses_exact_retail_fiscal_quarters() -> None:
    wb = _load_model("ANF")
    try:
        period_set = _history_q_latest_full_year_period_set(wb, ticker="ANF")
        assert period_set["fiscal_year"] == 2025
        assert period_set["labels"] == ["2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"]
        assert period_set["quarter_dates"] == [
            dt.date(2025, 5, 3),
            dt.date(2025, 8, 2),
            dt.date(2025, 11, 1),
            dt.date(2026, 1, 31),
        ]
        assert period_set["previous_quarter_dates"] == [
            dt.date(2024, 5, 4),
            dt.date(2024, 8, 3),
            dt.date(2024, 11, 2),
            dt.date(2025, 2, 1),
        ]
        assert dt.date(2025, 2, 1) in period_set["previous_quarter_dates"]
        assert dt.date(2025, 2, 1) not in period_set["quarter_dates"]

        fy2025_revenue = _sum_history_revenue_m(wb, period_set["quarter_dates"])
        fy2024_revenue = _sum_history_revenue_m(wb, period_set["previous_quarter_dates"])
        actuals = _history_q_latest_full_year_actuals_from_workbook(wb, ticker="ANF")
        assert fy2025_revenue == pytest.approx(5266.292, abs=0.001)
        assert fy2024_revenue == pytest.approx(4948.587, abs=0.001)
        assert actuals["revenue_m"] == pytest.approx(5266.292, abs=0.001)
        assert actuals["revenue_growth"] == pytest.approx(0.064201, abs=0.0001)
    finally:
        wb.close()


def test_real_calendar_reporters_keep_calendar_year_quarters() -> None:
    for ticker in ("PBI", "GPRE"):
        wb = _load_model(ticker)
        try:
            period_set = _history_q_latest_full_year_period_set(wb, ticker=ticker)
            assert period_set["fiscal_year"] == 2025, f"{ticker} latest full year"
            assert period_set["labels"] == ["2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"], ticker
            assert period_set["quarter_dates"] == [
                dt.date(2025, 3, 31),
                dt.date(2025, 6, 30),
                dt.date(2025, 9, 30),
                dt.date(2025, 12, 31),
            ], ticker
        finally:
            wb.close()


def test_fiscal_resolver_supports_future_calendar_and_non_calendar_profiles() -> None:
    calendar_wb = _history_q_test_workbook(
        [
            (dt.date(2025, 3, 31), 100),
            (dt.date(2025, 6, 30), 200),
            (dt.date(2025, 9, 30), 300),
            (dt.date(2025, 12, 31), 400),
            (dt.date(2026, 3, 31), 500),
        ]
    )
    calendar_set = _history_q_latest_full_year_period_set(calendar_wb, ticker="FAKECAL")
    assert calendar_set["fiscal_year"] == 2025
    assert calendar_set["labels"] == ["2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"]

    non_calendar_wb = _history_q_test_workbook(
        [
            (dt.date(2025, 5, 3), 100),
            (dt.date(2025, 8, 2), 200),
            (dt.date(2025, 11, 1), 300),
            (dt.date(2026, 1, 31), 400),
            (dt.date(2026, 5, 2), 500),
        ]
    )
    non_calendar_set = _history_q_latest_full_year_period_set(
        non_calendar_wb,
        ticker="FUTURE_RETAIL",
        fiscal_profile={"year_end_month": 1, "year_end_day": 31, "year_label": "start"},
    )
    assert non_calendar_set["fiscal_year"] == 2025
    assert non_calendar_set["quarter_dates"] == [
        dt.date(2025, 5, 3),
        dt.date(2025, 8, 2),
        dt.date(2025, 11, 1),
        dt.date(2026, 1, 31),
    ]


def test_promise_actual_header_is_exact_and_old_header_absent() -> None:
    for ticker in TICKERS:
        wb = _load_model(ticker)
        try:
            ws = wb["Promise_Progress_UI"]
            all_values = [_cell_text(cell.value) for row in ws.iter_rows(min_col=1, max_col=13) for cell in row]
            assert "Actual / latest actual" not in all_values, ticker
            assert "Final actual" not in all_values, ticker
            assert "Actual reported" not in all_values, ticker
            timeline_headers = [
                [_cell_text(ws.cell(rr, cc).value) for cc in range(1, 13)]
                for rr in range(1, int(ws.max_row or 0) + 1)
                if _cell_text(ws.cell(rr, 1).value) == "Metric"
                and _cell_text(ws.cell(rr, 2).value) == "Previous guide"
            ]
            assert timeline_headers, f"{ticker} has no Promise revision timeline header"
            for header in timeline_headers:
                assert header[4] == "Actual", f"{ticker} timeline header uses {header[4]!r}"
                assert header[5] == "Progress / run-rate", f"{ticker} timeline header missing progress column: {header!r}"
                assert header[6] == "Status", f"{ticker} status column shifted incorrectly: {header!r}"
        finally:
            wb.close()


def test_promise_interim_progress_is_separate_from_exact_actual() -> None:
    for ticker in TICKERS:
        for block, row_idx, row in _all_promise_revision_rows(ticker):
            metric = _cell_text(row.get("metric") or row.get("milestone"))
            horizon = _cell_text(row.get("horizon"))
            stated = _cell_text(row.get("stated in"))
            actual = _cell_text(row.get("actual"))
            progress = _cell_text(row.get("progress / run-rate"))
            status = _cell_text(row.get("status")).lower()
            if any(token in metric.lower() for token in ("cost savings", "facility qualification")):
                assert not (
                    actual and re.search(r"\brun[- ]rate\b|\bof\s+8\b|operational|qualified", actual, re.I)
                ), f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: progress-like value is in Actual"
            if horizon.endswith("year") and re.fullmatch(r"20\d{2}-Q[1-3]", stated):
                assert not actual, (
                    f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: interim annual row should use Progress / run-rate, "
                    f"not Actual={actual!r}"
                )
                assert status not in {"completed", "hit", "missed", "beat"}, (
                    f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: interim annual progress marked final"
                )
                if progress:
                    assert re.search(r"Q[1-3]:|YTD:|TTM:|Run[- ]rate:|progress|operational|qualified|\$", progress, re.I), (
                        f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: progress value lacks basis label: {progress!r}"
                    )


def test_promise_revision_rows_are_grouped_by_stated_in_event_and_not_actual_only() -> None:
    allowed_actual_only_notes = re.compile(r"\b(target|plan|expected|milestone|qualified|operational|run[- ]rate|progress)\b", re.I)
    for ticker in TICKERS:
        for block, row_idx, row in _all_promise_revision_rows(ticker):
            metric = _cell_text(row.get("metric") or row.get("milestone"))
            event = _section_event(block)
            stated = _cell_text(row.get("stated in"))
            assert stated == event, f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: stated in {stated!r} outside {block!r}"
            prev = _cell_text(row.get("previous guide"))
            new = _cell_text(row.get("new/current guide"))
            actual = _cell_text(row.get("actual"))
            change = _cell_text(row.get("change type"))
            note = _cell_text(row.get("source / note"))
            if actual and not prev and not new:
                assert allowed_actual_only_notes.search(f"{metric} {change} {note}"), (
                    f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: actual-only row without guide/target/milestone"
                )


def _metric_rank(metric: str) -> int:
    low = metric.lower()
    if any(tok in low for tok in ("revenue", "sales")):
        return 10
    if "operating margin" in low:
        return 20
    if "adjusted ebitda" in low or "adj ebitda" in low or re.search(r"\bebitda\b", low):
        return 30
    if "adjusted ebit" in low or "adj ebit" in low or re.search(r"\bebit\b", low):
        return 40
    if "eps" in low:
        return 50
    if "fcf" in low or "free cash" in low or "cash flow" in low:
        return 60
    if "capex" in low:
        return 70
    if any(tok in low for tok in ("buyback", "repurchase", "share count", "shares", "diluted shares")):
        return 80
    if any(tok in low for tok in ("cost savings", "restructuring", "cash optimization")):
        return 90
    if any(tok in low for tok in ("debt", "leverage", "liquidity")):
        return 100
    if any(tok in low for tok in ("45z", "policy", "facility", "segment", "tariff", "freight", "erp", "marketing")):
        return 110
    return 120


def test_promise_revision_metrics_use_stable_order_inside_blocks() -> None:
    for ticker in TICKERS:
        wb = _load_model(ticker)
        try:
            blocks = _promise_revision_blocks(wb["Promise_Progress_UI"])
            for block, rows in blocks.items():
                ranks = [_metric_rank(_cell_text(row.get("metric") or row.get("milestone"))) for _, row in rows]
                assert ranks == sorted(ranks), f"{ticker} {block}: metric order is unstable: {ranks}"
        finally:
            wb.close()


def test_promise_annual_actuals_are_horizon_matched_not_final_backfills() -> None:
    final_statuses = {"completed", "hit", "missed", "beat"}
    for ticker in TICKERS:
        for block, row_idx, row in _all_promise_revision_rows(ticker):
            metric = _cell_text(row.get("metric") or row.get("milestone"))
            horizon = _cell_text(row.get("horizon"))
            stated = _cell_text(row.get("stated in"))
            actual = _cell_text(row.get("actual"))
            status = _cell_text(row.get("status")).lower()
            if horizon.endswith("year"):
                h_year = int(horizon.split()[0])
                stated_match = re.fullmatch(r"(20\d{2})-Q([1-4])", stated)
                if stated_match:
                    stated_year = int(stated_match.group(1))
                    stated_q = int(stated_match.group(2))
                    if stated_year < h_year:
                        assert not actual, f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: future annual horizon has actual {actual!r}"
                        assert status not in final_statuses, f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: future annual horizon completed early"
                    elif stated_year == h_year and stated_q < 4:
                        assert status not in final_statuses, (
                            f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: interim annual row marked final with actual {actual!r}"
                        )
                if horizon == "2026 year":
                    assert status not in final_statuses, f"{ticker} Promise_Progress_UI!A{row_idx} {metric}: 2026 annual guidance completed before year-end"


def test_gpre_45z_monetization_revision_chain_and_facility_progress() -> None:
    wb = _load_model("GPRE")
    try:
        blocks = _promise_revision_blocks(wb["Promise_Progress_UI"])
        q3_rows = [row for _, row in blocks.get("2025-Q3 revisions", []) if _cell_text(row.get("metric")) == "45Z monetization"]
        q4_rows = [row for _, row in blocks.get("2025-Q4 revisions", []) if _cell_text(row.get("metric")) == "45Z monetization"]
        assert len(q3_rows) == 1, "GPRE 2025-Q3 should contain one initial 45Z monetization row for 2025-Q4 horizon"
        assert _cell_text(q3_rows[0].get("change type")) == "Initial"
        assert _cell_text(q3_rows[0].get("horizon")) == "2025-Q4"
        assert len(q4_rows) == 1, "GPRE 2025-Q4 should contain one updated/final 45Z monetization row"
        assert _cell_text(q4_rows[0].get("change type")) in {"Updated", "Maintained"}
        assert _cell_text(q4_rows[0].get("actual")) == "$23.4m"
        assert "$15.0m-$25.0m" in _cell_text(q4_rows[0].get("previous guide"))

        q1_2026 = [row for _, row in blocks.get("2026-Q1 revisions", []) if _cell_text(row.get("metric")) == "45Z facility qualification"]
        assert q1_2026, "GPRE 2026-Q1 should retain 45Z facility qualification progress row"
        facility = q1_2026[0]
        assert not _cell_text(facility.get("actual"))
        assert "3 of 8" in _cell_text(facility.get("progress / run-rate"))
        assert _cell_text(facility.get("status")) == "On track"
        assert _cell_text(facility.get("status")) != "Completed"
    finally:
        wb.close()


def test_pbi_fcf_eps_and_cost_savings_semantics_are_source_specific() -> None:
    wb = _load_model("PBI")
    try:
        ws = wb["Promise_Progress_UI"]
        all_text = "\n".join(_cell_text(cell.value) for row in ws.iter_rows(min_col=1, max_col=10) for cell in row)
        assert "Adjusted EPS / EPS" not in all_text
        blocks = _promise_revision_blocks(ws)
        q2_2024_cost = [row for _, row in blocks.get("2024-Q2 revisions", []) if _cell_text(row.get("metric")) == "Cost savings target"]
        assert q2_2024_cost, "PBI 2024-Q2 should show the source-backed initial cost-savings target"
        assert _cell_text(q2_2024_cost[0].get("new/current guide")) == "$120m-$160m"
        assert not _cell_text(q2_2024_cost[0].get("actual"))
        assert "$70m" in _cell_text(q2_2024_cost[0].get("progress / run-rate"))
        assert "$157m" not in _cell_text(q2_2024_cost[0].get("progress / run-rate")), "PBI later run-rate was backfilled into 2024-Q2"

        q1_2025_cost = [row for _, row in blocks.get("2025-Q1 revisions", []) if _cell_text(row.get("metric")) == "Cost savings target"]
        assert q1_2025_cost
        assert not _cell_text(q1_2025_cost[0].get("actual"))
        assert "$157m" in _cell_text(q1_2025_cost[0].get("progress / run-rate"))

        for block, row_idx, row in _all_promise_revision_rows("PBI"):
            metric = _cell_text(row.get("metric"))
            if metric == "FCF target":
                note = _cell_text(row.get("source / note")).lower()
                assert "free cash flow" in note or "fcf" in note, f"PBI Promise_Progress_UI!A{row_idx}: FCF row lacks definition note"
            assert metric != "Adjusted EPS / EPS", f"PBI Promise_Progress_UI!A{row_idx}: generic EPS label remains"

        narrative = wb["Quarter_Narrative_Data"]
        narrative_blob = "\n".join(_cell_text(cell.value) for row in narrative.iter_rows() for cell in row).lower()
        assert "$120m-$160m annual savings target" in narrative_blob
        assert "$70m annualized reductions" in narrative_blob
        assert "$136m" in narrative_blob and "gec" in narrative_blob
        assert "$240m" in narrative_blob and "cash optimization" in narrative_blob
    finally:
        wb.close()


def test_pbi_2026_q1_revenue_guidance_update_is_source_backed_and_same_horizon() -> None:
    wb = _load_model("PBI")
    try:
        blocks = _promise_revision_blocks(wb["Promise_Progress_UI"])
        rows = [row for _, row in blocks.get("2026-Q1 revisions", []) if _cell_text(row.get("metric")) == "Revenue guidance"]
        assert rows, "PBI 2026-Q1 should include the source-backed 2026-year Revenue guidance update"
        row = rows[0]
        assert _cell_text(row.get("horizon")) == "2026 year"
        assert _cell_text(row.get("previous guide")) == "$1.76bn-$1.86bn"
        assert _cell_text(row.get("new/current guide")) == "$1.8bn-$1.86bn"
        assert _cell_text(row.get("change type")) in {"Updated", "Raised", "Narrowed"}
        assert "2025 year" not in _cell_text(row.get("previous guide")).lower()
    finally:
        wb.close()


def test_anf_pre_release_can_show_final_actual_for_same_horizon_with_timing_note() -> None:
    wb = _load_model("ANF")
    try:
        blocks = _promise_revision_blocks(wb["Promise_Progress_UI"])
        pre_rows = blocks.get("2025-Q4 pre-release update revisions", [])
        final_rows = blocks.get("2025-Q4 revisions", [])
        assert pre_rows, "ANF pre-release block missing"
        assert final_rows, "ANF final 2025-Q4 block missing"
        pre_by_metric = {_cell_text(row.get("metric")): row for _, row in pre_rows}
        assert _cell_text(pre_by_metric["Net sales growth"].get("actual")) == "+6%"
        assert "Year result shown for comparison" in _cell_text(pre_by_metric["Net sales growth"].get("source / note"))
        final_actual_metrics = {_cell_text(row.get("metric")): _cell_text(row.get("actual")) for _, row in final_rows}
        assert final_actual_metrics.get("Net sales growth") == "+6%"
        assert final_actual_metrics.get("Adjusted EPS") == "$9.86 adjusted"
        assert "Adjusted EPS / EPS" not in "\n".join(
            _cell_text(cell.value) for row in wb["Promise_Progress_UI"].iter_rows(min_col=1, max_col=10) for cell in row
        )
    finally:
        wb.close()
