from __future__ import annotations

import json
import re
import shutil
from contextlib import contextmanager, nullcontext
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook

import pbi_xbrl.excel_writer_core as writer_core_module
import pbi_xbrl.excel_writer_context as writer_context_module
from pbi_xbrl.company_profiles import CompanyProfile, OperatingDriverTemplate
from pbi_xbrl.excel_writer import (
    validate_saved_workbook_export,
    validate_needs_review_export,
    validate_qa_export,
    read_quarter_notes_ui_snapshot,
    validate_quarter_notes_ui_export,
    validate_saved_workbook_integrity,
    validate_summary_export,
    validate_valuation_export,
    write_quarter_notes_audit_sheet,
    write_excel_from_inputs,
)
from pbi_xbrl.excel_writer_drivers import load_operating_driver_template_index
from pbi_xbrl.excel_writer_context import build_writer_context
from pbi_xbrl.excel_writer_core import (
    ensure_driver_inputs,
    ensure_hidden_value_inputs,
    ensure_raw_data_inputs,
    ensure_report_inputs,
    ensure_summary_inputs,
    ensure_ui_evidence,
    ensure_valuation_inputs,
    prepare_writer_inputs,
    write_raw_data_sheets,
)
from pbi_xbrl.excel_writer_financials import (
    write_debt_sheets,
    write_report_sheets,
    write_summary_sheets,
    write_valuation_sheets,
)
from pbi_xbrl.excel_writer_segments import (
    latest_segment_financials_workbook,
    parse_quarterly_segment_data_from_workbook,
)
from pbi_xbrl.excel_writer_sources import docs_for_valuation_accn
from pbi_xbrl.excel_writer_ui import write_ui_sheets
from pbi_xbrl.pipeline_types import WorkbookInputs


@contextmanager
def _case_dir() -> Path:
    root = Path(__file__).resolve().parents[2] / ".venv" / "tmp_excel_writer_refactor_tests"
    root.mkdir(parents=True, exist_ok=True)
    case_dir = root / uuid4().hex
    case_dir.mkdir()
    try:
        yield case_dir
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def _quarter_block_notes(ws, qtxt: str) -> list[str]:
    out: list[str] = []
    capture = False
    for rr in range(1, ws.max_row + 1):
        marker = str(ws.cell(row=rr, column=1).value or "")
        category = str(ws.cell(row=rr, column=2).value or "")
        if marker == qtxt:
            capture = True
            continue
        if capture and marker:
            break
        if capture and category == "Category":
            continue
        if capture:
            note = str(ws.cell(row=rr, column=3).value or "")
            if note:
                out.append(note)
    return out


def _make_hist() -> pd.DataFrame:
    quarters = pd.to_datetime(
        [
            "2025-03-31",
            "2025-06-30",
            "2025-09-30",
            "2025-12-31",
        ]
    )
    return pd.DataFrame(
        {
            "quarter": quarters,
            "revenue": [100.0, 110.0, 120.0, 130.0],
            "cfo": [10.0, 12.0, 13.0, 14.0],
            "capex": [2.0, 2.0, 3.0, 3.0],
            "ebitda": [15.0, 16.0, 18.0, 19.0],
            "ebit": [9.0, 10.0, 11.0, 12.0],
            "cash": [20.0, 21.0, 22.0, 23.0],
            "debt_core": [50.0, 49.0, 48.0, 47.0],
            "shares_outstanding": [10.0, 10.0, 10.0, 10.0],
            "shares_diluted": [10.0, 10.0, 10.0, 10.0],
            "market_cap": [100.0, 100.0, 100.0, 100.0],
            "interest_expense_net": [1.0, 1.0, 1.0, 1.0],
        }
    )


def _make_inputs(
    out_path: Path,
    *,
    excel_mode: str = "clean",
    profile_timings: bool = False,
    ticker: str = "TEST",
    hist: pd.DataFrame | None = None,
    quarter_notes: pd.DataFrame | None = None,
    promises: pd.DataFrame | None = None,
    promise_progress: pd.DataFrame | None = None,
    adj_metrics_relaxed: pd.DataFrame | None = None,
    adj_breakdown_relaxed: pd.DataFrame | None = None,
    non_gaap_files_relaxed: pd.DataFrame | None = None,
    quarter_notes_audit: bool = False,
    capture_saved_workbook_provenance: bool = True,
) -> WorkbookInputs:
    empty = pd.DataFrame()
    return WorkbookInputs(
        out_path=out_path,
        hist=hist if hist is not None else _make_hist(),
        audit=empty,
        needs_review=empty,
        debt_tranches=empty,
        debt_recon=empty,
        adj_metrics=empty,
        adj_breakdown=empty,
        non_gaap_files=empty,
        adj_metrics_relaxed=adj_metrics_relaxed if adj_metrics_relaxed is not None else empty,
        adj_breakdown_relaxed=adj_breakdown_relaxed if adj_breakdown_relaxed is not None else empty,
        non_gaap_files_relaxed=non_gaap_files_relaxed if non_gaap_files_relaxed is not None else empty,
        info_log=empty,
        tag_coverage=empty,
        period_checks=empty,
        qa_checks=empty,
        bridge_q=empty,
        manifest_df=empty,
        ocr_log=empty,
        qfd_preview=empty,
        qfd_unused=empty,
        debt_profile=empty,
        debt_tranches_latest=empty,
        debt_maturity=empty,
        debt_credit_notes=empty,
        revolver_df=empty,
        revolver_history=empty,
        debt_buckets=empty,
        slides_segments=empty,
        slides_debt=empty,
        slides_guidance=empty,
        quarter_notes=quarter_notes if quarter_notes is not None else empty,
        promises=promises if promises is not None else empty,
        promise_progress=promise_progress if promise_progress is not None else empty,
        non_gaap_cred=empty,
        ticker=ticker,
        price=10.0,
        strictness="ytd",
        excel_mode=excel_mode,
        cache_dir=out_path.parent,
        profile_timings=profile_timings,
        quarter_notes_audit=quarter_notes_audit,
        capture_saved_workbook_provenance=capture_saved_workbook_provenance,
    )


def _make_model_out_path(case_dir: Path, filename: str = "model.xlsx") -> Path:
    model_dir = case_dir / "TEST" / "TEST model excel"
    model_dir.mkdir(parents=True, exist_ok=True)
    return model_dir / filename


def _make_ticker_model_out_path(case_dir: Path, ticker: str, filename: str = "model.xlsx") -> Path:
    model_dir = case_dir / ticker / f"{ticker} model excel"
    model_dir.mkdir(parents=True, exist_ok=True)
    return model_dir / filename


def _current_delivered_model_path(ticker: str) -> Path:
    return Path(__file__).resolve().parents[2] / "Excel stock models" / f"{ticker}_model.xlsx"


def _find_row_with_value(ws, text: str, *, column: int | None = 1) -> int | None:
    for rr in range(1, ws.max_row + 1):
        if column is None:
            for cc in range(1, ws.max_column + 1):
                if ws.cell(row=rr, column=cc).value == text:
                    return rr
            continue
        if ws.cell(row=rr, column=column).value == text:
            return rr
    return None


def _sheet_metric_rows(path: Path, sheet_name: str, metric: str, *, quarters: list[str] | None = None) -> list[dict[str, str]]:
    wb = load_workbook(path, data_only=False, read_only=True)
    try:
        assert sheet_name in wb.sheetnames
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    assert rows
    header = [str(x or "").strip() for x in rows[0]]
    idx = {name: pos for pos, name in enumerate(header) if name}
    quarter_set = {str(x) for x in (quarters or []) if str(x).strip()}
    out: list[dict[str, str]] = []
    for vals in rows[1:]:
        if not vals:
            continue
        metric_val = str(vals[idx["metric"]] or "").strip() if "metric" in idx and idx["metric"] < len(vals) else ""
        if metric_val != metric:
            continue
        row: dict[str, str] = {}
        for col in ["quarter", "metric", "severity", "status", "message", "source"]:
            if col not in idx or idx[col] >= len(vals):
                continue
            raw = vals[idx[col]]
            if col == "quarter":
                qts = pd.to_datetime(raw, errors="coerce")
                row[col] = pd.Timestamp(qts).strftime("%Y-%m-%d") if pd.notna(qts) else str(raw or "").strip()
            else:
                row[col] = "" if raw is None else str(raw)
        if quarter_set and str(row.get("quarter") or "") not in quarter_set:
            continue
        out.append(row)
    return out


def _read_quarter_notes_audit_rows(path: Path) -> list[dict[str, str]]:
    wb = load_workbook(path, data_only=True)
    assert "Quarter_Notes_Audit" in wb.sheetnames
    ws = wb["Quarter_Notes_Audit"]
    rows = list(ws.iter_rows(values_only=True))
    assert rows
    header = [str(x or "") for x in rows[0]]
    out: list[dict[str, str]] = []
    for vals in rows[1:]:
        if not vals or all(v in (None, "") for v in vals):
            continue
        out.append({header[idx]: "" if vals[idx] is None else str(vals[idx]) for idx in range(len(header))})
    return out


@contextmanager
def _profile_override(monkeypatch: pytest.MonkeyPatch, profile_ticker: str):
    original = writer_context_module.get_company_profile
    monkeypatch.setattr(
        writer_context_module,
        "get_company_profile",
        lambda _ticker: original(profile_ticker),
    )
    try:
        yield
    finally:
        monkeypatch.setattr(writer_context_module, "get_company_profile", original)


def _write_pbi_segment_workbook(segment_dir: Path, *, include_quarters: bool = True) -> Path:
    segment_dir.mkdir(parents=True, exist_ok=True)
    path = segment_dir / "Historical Segment Financials up to Q4 2025.xlsx"
    wb = Workbook()
    ws_rev = wb.active
    ws_rev.title = "Revenue & Gross Profit"
    ws_adj = wb.create_sheet("Adj Segment Data")

    quarter_headers = ["Mar 2025", "Jun 2025", "Sep 2025", "Dec 2025"] if include_quarters else []
    annual_headers = ["2023", "2024", "2025"]
    header_values = quarter_headers + annual_headers
    header_start_col = 4
    for ws in (ws_rev, ws_adj):
        for idx, label in enumerate(header_values):
            ws.cell(row=1, column=header_start_col + idx, value=label)

    def _write_rows(ws, rows: list[list[object]]) -> None:
        for ridx, values in enumerate(rows, start=2):
            for cidx, value in enumerate(values, start=1):
                ws.cell(row=ridx, column=cidx, value=value)

    q_vals = {
        "SendTech Solutions": [480.0, 490.0, 500.0, 510.0],
        "Presort Services": [300.0, 305.0, 310.0, 315.0],
        "Other operations": [20.0, 22.0, 24.0, 26.0],
        "Corporate": [-15.0, -16.0, -17.0, -18.0],
    }
    a_vals = {
        "SendTech Solutions": [1700.0, 1825.0, 1980.0],
        "Presort Services": [1120.0, 1180.0, 1230.0],
        "Other operations": [70.0, 85.0, 92.0],
        "Corporate": [-55.0, -60.0, -66.0],
    }

    def _series(label: str) -> list[object]:
        return list(q_vals.get(label, [])) + list(a_vals.get(label, []))

    _write_rows(
        ws_rev,
        [
            ["Revenue by Segment"],
            ["SendTech Solutions", None, None, *_series("SendTech Solutions")],
            ["Presort Services", None, None, *_series("Presort Services")],
            ["Other operations", None, None, *_series("Other operations")],
            [
                "Total",
                None,
                None,
                *[
                    sum(vals[idx] for vals in [q_vals["SendTech Solutions"], q_vals["Presort Services"], q_vals["Other operations"]])
                    for idx in range(len(quarter_headers))
                ],
                *[
                    sum(vals[idx] for vals in [a_vals["SendTech Solutions"], a_vals["Presort Services"], a_vals["Other operations"]])
                    for idx in range(3)
                ],
            ],
            ["Gross Profit and Gross Profit Margin"],
        ],
    )

    _write_rows(
        ws_adj,
        [
            ["Adjusted EBIT"],
            ["SendTech Solutions", None, None, *( [120.0, 125.0, 130.0, 135.0] if include_quarters else [] ), 420.0, 455.0, 510.0],
            ["EBIT margin", None, None, *( [0.250, 0.255, 0.260, 0.265] if include_quarters else [] ), 0.247, 0.249, 0.258],
            ["Presort Services", None, None, *( [45.0, 47.0, 49.0, 52.0] if include_quarters else [] ), 165.0, 178.0, 193.0],
            ["EBIT margin", None, None, *( [0.150, 0.154, 0.158, 0.165] if include_quarters else [] ), 0.147, 0.151, 0.157],
            ["Corporate", None, None, *( [-10.0, -11.0, -12.0, -13.0] if include_quarters else [] ), -40.0, -44.0, -46.0],
            ["EBIT margin", None, None, *( [-0.021, -0.022, -0.023, -0.025] if include_quarters else [] ), -0.024, -0.024, -0.023],
            ["Other operations", None, None, *( [5.0, 4.0, 3.0, 2.0] if include_quarters else [] ), 10.0, 9.0, 8.0],
            ["EBIT margin", None, None, *( [0.250, 0.182, 0.125, 0.077] if include_quarters else [] ), 0.143, 0.106, 0.087],
            ["Adjusted segment EBIT"],
            ["Depreciation & amortization"],
            ["SendTech Solutions", None, None, *( [18.0, 19.0, 20.0, 21.0] if include_quarters else [] ), 67.0, 71.0, 78.0],
            ["Presort Services", None, None, *( [12.0, 12.0, 13.0, 13.0] if include_quarters else [] ), 43.0, 46.0, 50.0],
            ["Corporate", None, None, *( [4.0, 4.0, 4.0, 4.0] if include_quarters else [] ), 15.0, 16.0, 16.0],
            ["Other operations", None, None, *( [1.0, 1.0, 1.0, 1.0] if include_quarters else [] ), 3.0, 4.0, 4.0],
            ["Total"],
        ],
    )

    wb.save(path)
    return path


def test_segment_helper_contracts_select_latest_and_parse_quarters() -> None:
    with _case_dir() as case_dir:
        segment_dir = case_dir / "segment_financials"
        older_path = _write_pbi_segment_workbook(segment_dir, include_quarters=True)
        newer_path = segment_dir / "Historical Segment Financials up to Q4 2026.xlsx"
        newer_path.write_bytes(older_path.read_bytes())
        newer_path.touch()

        picked = latest_segment_financials_workbook(segment_dir)

        assert picked == newer_path

        parsed = parse_quarterly_segment_data_from_workbook(
            older_path,
            annual_segment_alias_patterns=[],
            company_segment_alias_patterns=[],
        )

        revenue_metrics = parsed["metrics"]["Revenue"]
        ebit_metrics = parsed["metrics"]["Adjusted EBIT"]
        margin_metrics = parsed["metrics"]["EBIT margin %"]
        da_metrics = parsed["metrics"]["Depreciation & amortization"]
        assert pd.Timestamp("2025-12-31") in parsed["quarters"]
        assert revenue_metrics["SendTech Solutions"][pd.Timestamp("2025-03-31")] == pytest.approx(480_000_000.0)
        assert ebit_metrics["Presort Services"][pd.Timestamp("2025-12-31")] == pytest.approx(52_000_000.0)
        assert margin_metrics["Presort Services"][pd.Timestamp("2025-06-30")] == pytest.approx(0.154)
        assert da_metrics["Corporate expense"][pd.Timestamp("2025-09-30")] == pytest.approx(4_000_000.0)


def test_driver_template_index_contract_preserves_order_and_units() -> None:
    profile = CompanyProfile(
        ticker="TEST",
        has_bank=False,
        industry_keywords=tuple(),
        segment_patterns=tuple(),
        segment_alias_patterns=tuple(),
        key_adv_require_keywords=tuple(),
        key_adv_deny_keywords=tuple(),
        operating_driver_history_templates=(
            OperatingDriverTemplate(
                group="production",
                key="ethanol_gallons",
                label="Ethanol gallons",
                why_it_matters="Volume",
                preferred_unit="mmgal",
                match_terms=("ethanol", "gallons"),
            ),
            OperatingDriverTemplate(
                group="production",
                key="utilization",
                label="Utilization",
                why_it_matters="Rate",
                preferred_unit="pct",
                match_terms=("utilization",),
            ),
        )
    )

    template_index = load_operating_driver_template_index(
        profile,
        timed_substage=lambda _label: nullcontext(),
    )

    assert template_index["order_map"]["ethanol_gallons"] == 0
    assert template_index["order_map"]["ethanol_gallons_produced"] == 0
    assert template_index["order_map"]["ethanol_gallons_sold"] == 1
    assert template_index["template_unit_map"]["utilization"] == "pct"
    assert template_index["template_specs"]["ethanol_gallons"]["search_terms"] == ("ethanol", "gallons")


def test_docs_for_valuation_accn_contract_sorts_relevant_docs_first() -> None:
    ordered = docs_for_valuation_accn(
        "0001234567-25-000010",
        accession_doc_lookup=lambda _accn: [
            Path("doc_000123456725000010_ex101_agreement.htm"),
            Path("doc_000123456725000010_pressrelease_ex99.htm"),
            Path("doc_000123456725000010_10q.htm"),
        ],
    )

    assert [path.name for path in ordered[:3]] == [
        "doc_000123456725000010_pressrelease_ex99.htm",
        "doc_000123456725000010_10q.htm",
        "doc_000123456725000010_ex101_agreement.htm",
    ]


def test_write_excel_minimal_workbook_has_core_sheets_and_order() -> None:
    with _case_dir() as case_dir:
        out_path = case_dir / "core.xlsx"
        write_excel_from_inputs(_make_inputs(out_path))

        wb = load_workbook(out_path, data_only=False)
        sheetnames = wb.sheetnames
        for name in [
            "SUMMARY",
            "Valuation",
            "Valuation_Grid",
            "History_Q",
            "Needs_Review",
            "QA_Checks",
            "Hidden_Value_Flags",
        ]:
            assert name in sheetnames
        assert sheetnames.index("SUMMARY") < sheetnames.index("Valuation")
        assert sheetnames.index("Valuation") < sheetnames.index("Hidden_Value_Flags")
        assert sheetnames.index("Hidden_Value_Flags") < sheetnames.index("History_Q")
        assert sheetnames.index("History_Q") < sheetnames.index("Needs_Review")
        assert sheetnames.index("Needs_Review") < sheetnames.index("QA_Checks")


def test_write_excel_snapshot_matches_saved_quarter_notes_ui_for_xlsx_and_xlsm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = case_dir / "quarter_notes_snapshot.xlsx"
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31"), pd.Timestamp("2025-06-30")],
                    "note_id": ["fcf-1", "debt-1", "rev-1"],
                    "category": [
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Debt / liquidity / covenants",
                    ],
                    "claim": ["FCF TTM accelerated", "Net debt declined", "Revolver utilization notable"],
                    "note": ["FCF TTM accelerated", "Net debt declined", "Revolver utilization notable"],
                    "metric_ref": ["fcf_ttm_delta_yoy", "net_debt_yoy_delta", "revolver_availability_change"],
                    "score": [92.0, 91.0, 89.0],
                    "doc_type": ["model_metric", "model_metric", "revolver"],
                    "doc": ["history_q", "history_q", "history_q"],
                    "source_type": ["model_metric", "model_metric", "revolver"],
                    "evidence_snippet": [
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "We also had $258.5 million available under our committed revolving credit agreement.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "FCF TTM YoY delta $198.7m"}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "Net debt delta $-77.9m"}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "revolver", "snippet": "We also had $258.5 million available under our committed revolving credit agreement."}]),
                    ],
                }
            )

            result = write_excel_from_inputs(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))

            validate_quarter_notes_ui_export(out_path, result.quarter_notes_ui_snapshot)
            snap = read_quarter_notes_ui_snapshot(out_path)
            assert any("FCF TTM improved by $198.7m YoY." in note for _, note in snap["2025-12-31"])

            xlsm_path = out_path.with_suffix(".xlsm")
            shutil.copyfile(out_path, xlsm_path)
            validate_quarter_notes_ui_export(xlsm_path, result.quarter_notes_ui_snapshot)
            validate_saved_workbook_integrity(out_path)
            validate_saved_workbook_integrity(xlsm_path)


def test_write_excel_sanitizes_invalid_comment_xml_characters() -> None:
    with _case_dir() as case_dir:
        out_path = case_dir / "comment_sanitized.xlsx"
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "note_id": ["bad-comment-1"],
                "category": ["Programs / initiatives"],
                "claim": ["Actively marketing 2026 45Z production tax credits"],
                "note": ["Actively marketing 2026 45Z production tax credits"],
                "metric_ref": ["45Z monetization / EBITDA"],
                "score": [95.0],
                "doc_type": ["press_release"],
                "doc": ["release_q4.txt"],
                "source_type": ["press_release"],
                "text_full": ["Evidence with \x00 invalid \x0b xml \x1f characters"],
                "comment_full_text": ["Evidence with \x00 invalid \x0b xml \x1f characters"],
                "evidence_snippet": ["Evidence with \x00 invalid \x0b xml \x1f characters"],
            }
        )

        result = write_excel_from_inputs(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))

        validate_saved_workbook_integrity(out_path)
        validate_quarter_notes_ui_export(out_path, result.quarter_notes_ui_snapshot)
        wb = load_workbook(out_path, data_only=False)
        assert "Quarter_Notes_UI" in wb.sheetnames


def test_write_excel_full_mode_emits_relaxed_sheets() -> None:
    with _case_dir() as case_dir:
        relaxed_metrics = pd.DataFrame({"quarter": [pd.Timestamp("2025-12-31")], "adj_ebitda": [12.0]})
        relaxed_breakdown = pd.DataFrame({"quarter": [pd.Timestamp("2025-12-31")], "label": ["Adj"], "value": [1.0]})
        relaxed_files = pd.DataFrame({"quarter": [pd.Timestamp("2025-12-31")], "doc": ["demo"]})
        out_path = case_dir / "full.xlsx"

        write_excel_from_inputs(
            _make_inputs(
                out_path,
                excel_mode="full",
                adj_metrics_relaxed=relaxed_metrics,
                adj_breakdown_relaxed=relaxed_breakdown,
                non_gaap_files_relaxed=relaxed_files,
            )
        )

        wb = load_workbook(out_path, data_only=False)
        for name in [
            "Adjusted_Metrics_Relaxed",
            "Adjustments_Breakdown_Relaxed",
            "NonGAAP_Files_Relaxed",
            "NonGAAP_Bridge_Relaxed",
            "Tag_Coverage",
            "Period_Self_Check",
        ]:
            assert name in wb.sheetnames

        clean_out = case_dir / "clean.xlsx"
        write_excel_from_inputs(_make_inputs(clean_out, excel_mode="clean"))
        wb_clean = load_workbook(clean_out, data_only=False)
        for name in [
            "Adjusted_Metrics_Relaxed",
            "Adjustments_Breakdown_Relaxed",
            "NonGAAP_Files_Relaxed",
            "NonGAAP_Bridge_Relaxed",
        ]:
            assert name not in wb_clean.sheetnames


def test_write_excel_profile_toggles_enable_driver_and_economics_sheets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = case_dir / "drivers.xlsx"
        profile = CompanyProfile(
            ticker="TEST",
            has_bank=False,
            industry_keywords=tuple(),
            segment_patterns=tuple(),
            segment_alias_patterns=tuple(),
            key_adv_require_keywords=tuple(),
            key_adv_deny_keywords=tuple(),
            enable_operating_drivers_sheet=True,
            enable_economics_overlay_sheet=True,
            enable_economics_market_raw_sheet=True,
            operating_driver_history_templates=(
                OperatingDriverTemplate(
                    group="Operations",
                    label="Plant status",
                    why_it_matters="demo",
                    match_terms=("plant status",),
                    aliases=("plant",),
                    key="plant_status",
                ),
            ),
        )
        monkeypatch.setattr(writer_context_module, "get_company_profile", lambda ticker: profile)
        monkeypatch.setattr(writer_context_module, "load_market_export_rows", lambda *args, **kwargs: [])

        write_excel_from_inputs(_make_inputs(out_path))

        wb = load_workbook(out_path, data_only=False)
        assert "Operating_Drivers" in wb.sheetnames
        assert "Economics_Overlay" in wb.sheetnames
        assert "economics_market_raw" in wb.sheetnames


def test_write_excel_profile_timings_only_emit_when_enabled(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        timed_out = case_dir / "timed.xlsx"
        profile = CompanyProfile(
            ticker="TEST",
            has_bank=False,
            industry_keywords=tuple(),
            segment_patterns=tuple(),
            segment_alias_patterns=tuple(),
            key_adv_require_keywords=tuple(),
            key_adv_deny_keywords=tuple(),
            enable_operating_drivers_sheet=True,
            enable_economics_overlay_sheet=True,
            enable_economics_market_raw_sheet=True,
            operating_driver_history_templates=(
                OperatingDriverTemplate(
                    group="Operations",
                    label="Plant status",
                    why_it_matters="demo",
                    match_terms=("plant status",),
                    aliases=("plant",),
                    key="plant_status",
                ),
            ),
        )
        monkeypatch.setattr(writer_context_module, "get_company_profile", lambda ticker: profile)
        monkeypatch.setattr(writer_context_module, "load_market_export_rows", lambda *args, **kwargs: [])
        write_excel_from_inputs(_make_inputs(timed_out, profile_timings=True))
        stdout_timed = capsys.readouterr().out
        assert "write_excel.prep" in stdout_timed
        assert "write_excel.summary" in stdout_timed
        assert "write_excel.derive.summary_inputs" in stdout_timed
        assert "write_excel.derive.valuation_inputs" in stdout_timed
        assert "write_excel.derive.valuation_inputs.normalize_sources" in stdout_timed
        assert "write_excel.derive.valuation_inputs.net_leverage_text_map" in stdout_timed
        assert "write_excel.derive.valuation_inputs.net_leverage_text_map.frames" in stdout_timed
        assert "write_excel.derive.valuation_inputs.net_leverage_text_map.local_docs" in stdout_timed
        assert "write_excel.derive.valuation_inputs.net_leverage_text_map.audit_docs" in stdout_timed
        assert "write_excel.valuation.precompute" in stdout_timed
        assert "write_excel.valuation.bundle" in stdout_timed
        assert "write_excel.valuation.render" in stdout_timed
        assert "write_excel.derive.driver_inputs.source_records" in stdout_timed
        assert "write_excel.derive.driver_inputs.line_index" in stdout_timed
        assert "write_excel.derive.driver_inputs.crush_bridge_cache" in stdout_timed
        assert "write_excel.derive.driver_inputs.template_rows" in stdout_timed
        assert "write_excel.derive.driver_inputs.operating_history" in stdout_timed
        assert "write_excel.derive.driver_inputs.economics_market" in stdout_timed
        assert "write_excel.derive.report_inputs" in stdout_timed
        assert "write_excel.derive.hidden_value_inputs" in stdout_timed
        assert "write_excel.derive.raw_data_inputs" in stdout_timed
        assert "write_excel.derive.ui_evidence" in stdout_timed
        assert "write_excel.save" in stdout_timed

        quiet_out = case_dir / "quiet.xlsx"
        write_excel_from_inputs(_make_inputs(quiet_out, profile_timings=False))
        stdout_quiet = capsys.readouterr().out
        assert "write_excel.prep" not in stdout_quiet
        assert "write_excel.summary" not in stdout_quiet


def test_build_writer_context_is_lazy_for_derived_frames() -> None:
    with _case_dir() as case_dir:
        ctx = build_writer_context(_make_inputs(case_dir / "lazy.xlsx"))
        assert all(value is None for value in vars(ctx.derived).values())

        prepare_writer_inputs(ctx)
        assert all(value is None for value in vars(ctx.derived).values())


def test_build_writer_context_exposes_explicit_state_contract() -> None:
    with _case_dir() as case_dir:
        out_path = case_dir / "explicit.xlsx"
        ctx = build_writer_context(_make_inputs(out_path))

        assert ctx.data.out_path == out_path
        assert ctx.data.ticker == "TEST"
        assert ctx.data.excel_mode == "clean"
        assert ctx.data.profile_timings is False
        assert isinstance(ctx.data.operating_driver_history_rows, list)
        assert isinstance(ctx.data.economics_market_rows, list)
        assert ctx.data.doc_cache.accession_doc_paths == {}

        assert ctx.callbacks.write_sheet is ctx.state["_write_sheet"]
        assert ctx.callbacks.build_report is ctx.state["_build_report"]
        assert ctx.callbacks.build_operating_drivers_history_rows is ctx.state["_build_operating_drivers_history_rows"]
        assert ctx.callbacks.build_economics_market_rows is ctx.state["_build_economics_market_rows"]
        assert ctx.state["writer_timings"] is ctx.writer_timings
        assert ctx.state["out_path"] == ctx.data.out_path
        assert ctx.state["qa_checks"] is ctx.data.qa_checks
        assert ctx.state["info_log"] is ctx.data.info_log
        assert ctx.state["company_profile"] is ctx.company_profile
        assert ctx.state["_load_profile_slide_signals_by_quarter"] is not None
        assert ctx.state["_load_operating_driver_45z_guidance_docs_by_quarter"] is not None
        assert "ctx_ref" not in ctx.state
        assert "valuation_style_bundle_cache" not in ctx.state


def test_ensure_driver_inputs_memoize_and_reuse_cached_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        ctx = build_writer_context(_make_inputs(case_dir / "driver_memo.xlsx"))
        ctx.data.enable_operating_drivers_sheet = True
        ctx.data.enable_economics_overlay_sheet = True
        ctx.data.enable_economics_market_raw_sheet = True
        ctx.state["enable_operating_drivers_sheet"] = True
        ctx.state["enable_economics_overlay_sheet"] = True
        ctx.state["enable_economics_market_raw_sheet"] = True
        ctx.callbacks.build_operating_drivers_history_rows = lambda: [{"Quarter": pd.Timestamp("2025-12-31").date(), "Driver": "Demo"}]
        ctx.callbacks.build_economics_market_rows = lambda: [{"quarter": pd.Timestamp("2025-12-31").date(), "series_key": "corn_cash_demo"}]
        ctx.callbacks.load_operating_driver_source_records = lambda: []
        ctx.callbacks.load_operating_driver_source_records_by_quarter = lambda: {}
        ctx.callbacks.prime_operating_driver_crush_detail_cache = lambda records=None: {}
        ctx.state.update(ctx.callbacks.as_state_mapping())

        ensure_driver_inputs(ctx)
        assert ctx.state["operating_driver_history_rows"] == [{"Quarter": pd.Timestamp("2025-12-31").date(), "Driver": "Demo"}]
        assert ctx.state["economics_market_rows"] == [{"quarter": pd.Timestamp("2025-12-31").date(), "series_key": "corn_cash_demo"}]
        assert ctx.data.operating_driver_history_rows == [{"Quarter": pd.Timestamp("2025-12-31").date(), "Driver": "Demo"}]
        assert ctx.data.economics_market_rows == [{"quarter": pd.Timestamp("2025-12-31").date(), "series_key": "corn_cash_demo"}]

        monkeypatch.setattr(ctx.callbacks, "build_operating_drivers_history_rows", lambda: (_ for _ in ()).throw(RuntimeError("driver recompute")))
        monkeypatch.setattr(ctx.callbacks, "build_economics_market_rows", lambda: (_ for _ in ()).throw(RuntimeError("market recompute")))
        ensure_driver_inputs(ctx)
        assert ctx.state["operating_driver_history_rows"] == [{"Quarter": pd.Timestamp("2025-12-31").date(), "Driver": "Demo"}]
        assert ctx.state["economics_market_rows"] == [{"quarter": pd.Timestamp("2025-12-31").date(), "series_key": "corn_cash_demo"}]


def test_ensure_helpers_memoize_and_reuse_cached_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        ctx = build_writer_context(_make_inputs(case_dir / "memo.xlsx"))

        ensure_report_inputs(ctx)
        report_id = id(ctx.derived.report_is)
        monkeypatch.setattr(ctx.callbacks, "build_report", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("report recompute")))
        ensure_report_inputs(ctx)
        assert id(ctx.derived.report_is) == report_id

        ensure_raw_data_inputs(ctx)
        raw_ids = (
            id(ctx.derived.facts_long),
            id(ctx.derived.lineitem_map),
            id(ctx.derived.period_index),
            id(ctx.derived.ng_bridge),
        )
        monkeypatch.setattr(ctx.callbacks, "build_facts_long", lambda: (_ for _ in ()).throw(RuntimeError("facts recompute")))
        monkeypatch.setattr(ctx.callbacks, "build_lineitem_map", lambda: (_ for _ in ()).throw(RuntimeError("lineitem recompute")))
        monkeypatch.setattr(ctx.callbacks, "build_period_index", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("period recompute")))
        monkeypatch.setattr(ctx.callbacks, "build_ng_bridge", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("bridge recompute")))
        ensure_raw_data_inputs(ctx)
        assert (
            id(ctx.derived.facts_long),
            id(ctx.derived.lineitem_map),
            id(ctx.derived.period_index),
            id(ctx.derived.ng_bridge),
        ) == raw_ids

        ensure_ui_evidence(ctx)
        ui_ids = (
            id(ctx.derived.quarter_notes_evidence_df),
            id(ctx.derived.promise_evidence_df),
        )
        monkeypatch.setattr(ctx.callbacks, "build_qn_evidence_src", lambda: (_ for _ in ()).throw(RuntimeError("qn recompute")))
        monkeypatch.setattr(ctx.callbacks, "build_promise_evidence_src", lambda: (_ for _ in ()).throw(RuntimeError("promise recompute")))
        ensure_ui_evidence(ctx)
        assert (
            id(ctx.derived.quarter_notes_evidence_df),
            id(ctx.derived.promise_evidence_df),
        ) == ui_ids

        ensure_valuation_inputs(ctx)
        valuation_ids = (
            id(ctx.derived.leverage_df),
            id(ctx.derived.valuation_summary_df),
            id(ctx.derived.valuation_grid_df),
        )
        monkeypatch.setattr(writer_core_module, "valuation_engine", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("valuation recompute")))
        ensure_valuation_inputs(ctx)
        assert (
            id(ctx.derived.leverage_df),
            id(ctx.derived.valuation_summary_df),
            id(ctx.derived.valuation_grid_df),
        ) == valuation_ids

        ensure_hidden_value_inputs(ctx)
        hidden_ids = (
            id(ctx.derived.signals_base_df),
            id(ctx.derived.flags_df),
            id(ctx.derived.flags_audit_df),
            id(ctx.derived.flags_recompute_df),
        )
        monkeypatch.setattr(
            writer_core_module,
            "build_hidden_value_outputs",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("hidden recompute")),
        )
        ensure_hidden_value_inputs(ctx)
        assert (
            id(ctx.derived.signals_base_df),
            id(ctx.derived.flags_df),
            id(ctx.derived.flags_audit_df),
            id(ctx.derived.flags_recompute_df),
        ) == hidden_ids

        ensure_summary_inputs(ctx)
        summary_id = id(ctx.derived.summary_df)
        monkeypatch.setitem(ctx.state, "_build_summary", lambda: (_ for _ in ()).throw(RuntimeError("summary recompute")))
        ensure_summary_inputs(ctx)
        assert id(ctx.derived.summary_df) == summary_id


def test_writer_front_modules_use_explicit_derived_frames_not_compat_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        ctx = build_writer_context(_make_inputs(case_dir / "front_explicit.xlsx"))

        ensure_driver_inputs(ctx)
        ensure_summary_inputs(ctx)
        ensure_ui_evidence(ctx)
        ensure_report_inputs(ctx)
        ensure_hidden_value_inputs(ctx)
        ensure_valuation_inputs(ctx)
        ensure_raw_data_inputs(ctx)

        removed_keys = [
            "summary_df",
            "valuation_summary_df",
            "valuation_grid_df",
            "leverage_df",
            "report_is",
            "report_bs",
            "report_cf",
            "flags_df",
            "flags_audit_df",
            "flags_recompute_df",
            "signals_base_df",
            "quarter_notes_evidence_df",
            "promise_evidence_df",
            "ng_bridge",
            "ng_bridge_relaxed",
            "facts_long",
            "lineitem_map",
            "period_index",
        ]
        for key in removed_keys:
            ctx.state.pop(key, None)

        written: dict[str, pd.DataFrame] = {}

        def capture_sheet(name: str, frame: pd.DataFrame) -> None:
            written[name] = frame

        def capture_report(name: str, frame: pd.DataFrame, _units: str) -> None:
            written[name] = frame

        monkeypatch.setattr(ctx.callbacks, "write_summary_sheet", lambda frame: written.__setitem__("SUMMARY", frame))
        monkeypatch.setattr(ctx.callbacks, "write_sheet", capture_sheet)
        monkeypatch.setattr(ctx.callbacks, "write_report_sheet", capture_report)
        monkeypatch.setattr(ctx.callbacks, "write_flags_sheet", capture_sheet)
        monkeypatch.setattr(ctx.callbacks, "write_valuation_sheet", lambda: None)
        monkeypatch.setattr(ctx.callbacks, "write_bs_segments_sheet", lambda quarters_shown=8: [])
        monkeypatch.setattr(ctx.callbacks, "write_quarter_notes_ui_v2", lambda quarters_shown=8: [])
        monkeypatch.setattr(ctx.callbacks, "write_promise_tracker_ui_v2", lambda: [])
        monkeypatch.setattr(ctx.callbacks, "write_promise_progress_ui_v2", lambda: [])
        monkeypatch.setattr(ctx.callbacks, "write_operating_drivers_raw_sheet", lambda rows: None)
        monkeypatch.setattr(ctx.callbacks, "write_economics_market_raw_sheet", lambda rows: None)

        write_summary_sheets(ctx)
        write_valuation_sheets(ctx)
        write_debt_sheets(ctx)
        write_report_sheets(ctx)
        write_ui_sheets(ctx)
        write_raw_data_sheets(ctx)

        assert written["SUMMARY"] is ctx.derived.summary_df
        assert written["Valuation_Summary"] is ctx.derived.valuation_summary_df
        assert written["Valuation_Grid"] is ctx.derived.valuation_grid_df
        assert written["Leverage_Liquidity"] is ctx.derived.leverage_df
        assert written["REPORT_IS_Q"] is ctx.derived.report_is
        assert written["REPORT_BS_Q"] is ctx.derived.report_bs
        assert written["REPORT_CF_Q"] is ctx.derived.report_cf
        assert written["Hidden_Value_Flags"] is ctx.derived.flags_df
        assert written["Hidden_Value_Audit"] is ctx.derived.flags_audit_df
        assert written["Hidden_Value_Recompute"] is ctx.derived.flags_recompute_df
        assert written["Hidden_Value_Base"] is ctx.derived.signals_base_df
        assert written["Quarter_Notes_Evidence"] is ctx.derived.quarter_notes_evidence_df
        assert written["Promise_Evidence"] is ctx.derived.promise_evidence_df
        assert written["NonGAAP_Bridge"] is ctx.derived.ng_bridge
        assert written["DATA_Facts_Long"] is ctx.derived.facts_long
        assert written["DATA_LineItem_Map"] is ctx.derived.lineitem_map
        assert written["DATA_Period_Index"] is ctx.derived.period_index


def test_summary_includes_current_strategic_context_row() -> None:
    with _case_dir() as case_dir:
        inputs = _make_inputs(case_dir / "summary_current_context.xlsx")
        inputs.company_overview = {
            "what_it_does": "Demo company description.",
            "what_it_does_source": "Source: SEC 10-K demo",
            "current_strategic_context": "Management is focused on capital allocation and cost discipline.",
            "current_strategic_context_source": "Source: SEC 8-K demo",
            "key_advantage": "Demo advantage.",
            "key_advantage_source": "Source: SEC 10-K competition demo",
            "segment_operating_model": [],
            "segment_operating_model_source": "Source: N/A",
            "key_dependencies": [],
            "key_dependencies_source": "Source: N/A",
            "wrong_thesis_bullets": [],
            "wrong_thesis_source": "Source: N/A",
            "revenue_streams": [],
            "revenue_streams_source": "Source: N/A",
            "asof_fy_end": None,
        }
        ctx = build_writer_context(inputs)
        ensure_summary_inputs(ctx)

        summary_df = ctx.derived.summary_df
        row = summary_df.loc[summary_df["Metric"] == "Current strategic context"].iloc[0]
        assert row["Value"] == "Management is focused on capital allocation and cost discipline."
        assert row["Note"] == "Source: SEC 8-K demo"


def test_operating_driver_line_index_and_template_rows_cache_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        profile = CompanyProfile(
            ticker="TEST",
            has_bank=False,
            industry_keywords=tuple(),
            segment_patterns=tuple(),
            segment_alias_patterns=tuple(),
            key_adv_require_keywords=tuple(),
            key_adv_deny_keywords=tuple(),
            enable_operating_drivers_sheet=True,
            operating_driver_history_templates=(
                OperatingDriverTemplate(
                    group="Operations",
                    label="Plant status",
                    why_it_matters="demo",
                    match_terms=("plant status", "operations"),
                    aliases=("plant", "operations"),
                    key="plant_status",
                ),
            ),
        )
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "text_full": ["Plant status remains strong.\nOperations continue normally across the fleet."],
                "doc_name": ["demo_note.txt"],
            }
        )
        monkeypatch.setattr(writer_context_module, "get_company_profile", lambda ticker: profile)
        ctx = build_writer_context(_make_inputs(case_dir / "driver_cache.xlsx", quarter_notes=quarter_notes))

        rows_first = ctx.callbacks.build_operating_drivers_history_rows()
        rows_second = ctx.callbacks.build_operating_drivers_history_rows()

        assert rows_first == rows_second
        assert ctx.derived.operating_driver_line_index_by_quarter
        assert ctx.derived.operating_driver_flat_line_index
        assert ctx.derived.operating_driver_best_text_cache
        assert ctx.derived.operating_driver_template_rows_cache
        assert ctx.derived.operating_driver_template_candidate_cache

        line_index_id = id(ctx.derived.operating_driver_line_index_by_quarter)
        template_rows_id = id(ctx.derived.operating_driver_template_rows_cache)
        best_text_cache_id = id(ctx.derived.operating_driver_best_text_cache)
        template_candidate_id = id(ctx.derived.operating_driver_template_candidate_cache)

        rows_third = ctx.callbacks.build_operating_drivers_history_rows()
        assert rows_third == rows_first
        assert id(ctx.derived.operating_driver_line_index_by_quarter) == line_index_id
        assert id(ctx.derived.operating_driver_template_rows_cache) == template_rows_id
        assert id(ctx.derived.operating_driver_best_text_cache) == best_text_cache_id
        assert id(ctx.derived.operating_driver_template_candidate_cache) == template_candidate_id


def test_valuation_source_views_and_render_bundle_are_reused() -> None:
    with _case_dir() as case_dir:
        ctx = build_writer_context(_make_inputs(case_dir / "valuation_cache.xlsx"))
        ensure_valuation_inputs(ctx)

        assert isinstance(ctx.derived.valuation_hist_indexed, pd.DataFrame)
        assert ctx.derived.valuation_latest_context is not None
        assert ctx.derived.valuation_last4_context is not None
        assert ctx.derived.valuation_core_maps is not None

        quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
        bundle_loader = ctx.state["_ensure_valuation_render_bundle"]
        bundle_first = bundle_loader(quarter_key, ctx.derived.leverage_df)
        bundle_second = bundle_loader(quarter_key, ctx.derived.leverage_df)

        assert bundle_first is bundle_second
        assert ctx.derived.valuation_render_bundle is bundle_first
        assert bundle_first["rev_map"]
        assert bundle_first["ebitda_ttm_map"]
        assert bundle_first["quarter_index_map"][quarter_key[0]] == 0
        assert len(bundle_first["last4_quarters_map"][quarter_key[3]]) == 4


def test_valuation_precompute_bundle_is_reused_and_reads_docs_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "valuation_precompute.xlsx")
        cache_dir = out_path.parent
        doc_path = cache_dir / "doc_000012345625000001_ex99.txt"
        doc_path.write_text(
            "In the fourth quarter, we repurchased $15 million of common stock. "
            "The board declared a regular quarterly dividend of $0.50 per share.",
            encoding="utf-8",
        )
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": ["0000123456-25-000001"],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )
        counts = {str(doc_path): 0}
        original_read_text = Path.read_text
        original_glob = Path.glob

        def counted_read_text(self: Path, *args, **kwargs):
            path_str = str(self)
            if path_str in counts:
                counts[path_str] += 1
            return original_read_text(self, *args, **kwargs)

        def counted_glob(self: Path, pattern: str):
            if pattern.startswith("doc_000012345625000001_"):
                return [doc_path]
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        monkeypatch.setattr(Path, "glob", counted_glob)
        inputs = _make_inputs(out_path)
        manifest_df = pd.DataFrame({"path": [str(doc_path)]})
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)

        quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
        render_loader = ctx.state["_ensure_valuation_render_bundle"]
        precompute_loader = ctx.state["_ensure_valuation_precompute_bundle"]
        render_bundle = render_loader(quarter_key, ctx.derived.leverage_df)
        bundle_first = precompute_loader(quarter_key, render_bundle)
        reads_after_first = counts[str(doc_path)]
        bundle_second = precompute_loader(quarter_key, render_bundle)

        assert bundle_first is bundle_second
        assert ctx.derived.valuation_precompute_bundle is bundle_first
        assert ctx.derived.valuation_filing_docs_by_quarter
        assert bundle_first["buyback_map"][pd.Timestamp("2025-12-31")] == pytest.approx(15_000_000.0)
        assert bundle_first["dividend_ps_doc_map"][pd.Timestamp("2025-12-31")] == pytest.approx(0.5)
        assert reads_after_first >= 1
        assert counts[str(doc_path)] == reads_after_first


def test_valuation_precompute_subtimings_emit_when_needed(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "valuation_precompute_timings.xlsx")
        cache_dir = out_path.parent
        doc_path = cache_dir / "doc_000012345625000001_ex99.txt"
        doc_path.write_text(
            "In the fourth quarter, we repurchased $15 million of common stock. "
            "The board declared a regular quarterly dividend of $0.50 per share.",
            encoding="utf-8",
        )
        original_glob = Path.glob

        def counted_glob(self: Path, pattern: str):
            if pattern.startswith("doc_000012345625000001_"):
                return [doc_path]
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "glob", counted_glob)
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": ["0000123456-25-000001"],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )
        inputs = _make_inputs(out_path, profile_timings=True)
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)

        quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
        render_loader = ctx.state["_ensure_valuation_render_bundle"]
        precompute_loader = ctx.state["_ensure_valuation_precompute_bundle"]
        render_bundle = render_loader(quarter_key, ctx.derived.leverage_df)
        precompute_loader(quarter_key, render_bundle)
        stdout = capsys.readouterr().out

        assert "write_excel.valuation.precompute.doc_index" in stdout
        assert "write_excel.valuation.precompute.keyword_maps" in stdout
        assert "write_excel.valuation.precompute.buyback_dividend_maps" in stdout
        assert "write_excel.valuation.precompute.buyback_share_maps" in stdout


def test_buyback_auth_prefers_newest_filing_match(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "buyback_auth.xlsx")
        cache_dir = out_path.parent
        new_accn = "0000123456-26-000002"
        old_accn = "0000123456-25-000001"
        new_doc = cache_dir / "doc_000012345626000002_new-20251231.htm"
        old_doc = cache_dir / "doc_000012345625000001_old-20241231.htm"
        new_doc.write_text(
            "<html><body>As of December 31, 2025, $125 million remained available under the share repurchase authorization.</body></html>",
            encoding="utf-8",
        )
        old_doc.write_text(
            "<html><body>As of December 31, 2024, $50 million remained available under the share repurchase authorization.</body></html>",
            encoding="utf-8",
        )
        (cache_dir / "submissions_test.json").write_text(
            json.dumps(
                {
                    "filings": {
                        "recent": {
                            "accessionNumber": [new_accn, old_accn],
                            "form": ["10-K", "10-K"],
                            "filingDate": ["2026-02-10", "2025-02-10"],
                            "reportDate": ["2025-12-31", "2024-12-31"],
                            "primaryDocument": ["new-20251231.htm", "old-20241231.htm"],
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        manifest_df = pd.DataFrame(
            {
                "cache_key": [
                    "submissions_test",
                    "doc_000012345626000002_new-20251231.htm",
                    "doc_000012345625000001_old-20241231.htm",
                ],
                "path": [
                    str(cache_dir / "submissions_test.json"),
                    str(new_doc),
                    str(old_doc),
                ],
                "status": ["ok", "ok", "ok"],
            }
        )
        original_read_text = Path.read_text
        new_doc_reads = 0
        new_doc_resolved = new_doc.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            nonlocal new_doc_reads
            try:
                resolved = self.resolve()
            except Exception:
                resolved = self
            if resolved == new_doc_resolved:
                new_doc_reads += 1
            return original_read_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        inputs = _make_inputs(out_path)
        inputs = inputs.__class__(**{**vars(inputs), "manifest_df": manifest_df})
        write_excel_from_inputs(inputs)
        stdout = capsys.readouterr().out

        assert out_path.exists()
        assert new_doc_reads >= 1
        assert f"[buyback_auth] match form=10-K accn={new_accn}" in stdout


def test_driver_45z_guidance_doc_index_is_reused_and_preserves_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "guidance.xlsx")
        cache_dir = out_path.parent
        guidance_doc = cache_dir / "doc_TEST_Q4_2025_guidance.txt"
        guidance_doc.write_text(
            "45Z expected monetization $250M-$300M expected monetization in 2026.",
            encoding="utf-8",
        )
        profile = CompanyProfile(
            ticker="TEST",
            has_bank=False,
            industry_keywords=tuple(),
            segment_patterns=tuple(),
            segment_alias_patterns=tuple(),
            key_adv_require_keywords=tuple(),
            key_adv_deny_keywords=tuple(),
            promise_priority_terms=("45z",),
            enable_operating_drivers_sheet=True,
            operating_driver_history_templates=(
                OperatingDriverTemplate(
                    group="Margin / spread",
                    label="45Z guided value",
                    why_it_matters="demo",
                    match_terms=("45z", "monetization"),
                    aliases=("45z",),
                    key="45z_value_guided",
                ),
            ),
        )
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "text_full": ["45Z monetization remains a core driver."],
                "doc_name": ["demo_note.txt"],
            }
        )
        monkeypatch.setattr(writer_context_module, "get_company_profile", lambda ticker: profile)
        ctx = build_writer_context(_make_inputs(out_path, quarter_notes=quarter_notes))

        docs_first = ctx.state["_load_operating_driver_45z_guidance_docs_by_quarter"]()
        docs_second = ctx.state["_load_operating_driver_45z_guidance_docs_by_quarter"]()
        rows_first = ctx.callbacks.build_operating_drivers_history_rows()
        rows_second = ctx.callbacks.build_operating_drivers_history_rows()

        assert docs_first is docs_second
        assert ctx.derived.operating_driver_45z_guidance_docs_by_quarter is docs_first
        assert rows_first == rows_second
        assert any(str(row.get("Commentary") or "").startswith("$250.0m-$300.0m expected monetization") for row in rows_first)


def test_driver_template_signal_and_doc_index_timings_emit_when_needed(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "driver_timings.xlsx")
        (out_path.parent / "doc_TEST_Q4_2025_guidance.txt").write_text(
            "45Z expected monetization $250M-$300M expected monetization in 2026.",
            encoding="utf-8",
        )
        profile = CompanyProfile(
            ticker="TEST",
            has_bank=False,
            industry_keywords=tuple(),
            segment_patterns=tuple(),
            segment_alias_patterns=tuple(),
            key_adv_require_keywords=tuple(),
            key_adv_deny_keywords=tuple(),
            promise_priority_terms=("45z",),
            enable_operating_drivers_sheet=True,
            operating_driver_history_templates=(
                OperatingDriverTemplate(
                    group="Margin / spread",
                    label="45Z guided value",
                    why_it_matters="demo",
                    match_terms=("45z", "monetization"),
                    aliases=("45z",),
                    key="45z_value_guided",
                ),
            ),
        )
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "text_full": ["45Z monetization remains a core driver."],
                "doc_name": ["demo_note.txt"],
            }
        )
        monkeypatch.setattr(writer_context_module, "get_company_profile", lambda ticker: profile)
        ctx = build_writer_context(_make_inputs(out_path, quarter_notes=quarter_notes, profile_timings=True))

        ctx.callbacks.build_operating_drivers_history_rows()
        stdout = capsys.readouterr().out

        assert "write_excel.derive.driver_inputs.template_signal_index" in stdout
        assert "write_excel.derive.driver_inputs.template_doc_index" in stdout


def test_valuation_net_leverage_text_map_prefers_adjusted_and_filters_quarters() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "leverage.xlsx")
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2024-12-31")],
                "claim": ["Net leverage was 3.5x.", "Adjusted net leverage was 1.9x."],
                "filed": [pd.Timestamp("2026-02-01"), pd.Timestamp("2025-02-01")],
            }
        )
        promises = pd.DataFrame(
            {
                "last_seen_quarter": [pd.Timestamp("2025-12-31")],
                "statement": ["Adjusted net leverage was 2.1x."],
                "filed": [pd.Timestamp("2026-02-15")],
                "source_type": ["promise"],
            }
        )
        inputs = _make_inputs(out_path, quarter_notes=quarter_notes)
        inputs = inputs.__class__(**{**vars(inputs), "promises": promises})
        ctx = build_writer_context(inputs)

        lev_map_first = ctx.callbacks.extract_adj_net_leverage_text_map()
        lev_map_second = ctx.callbacks.extract_adj_net_leverage_text_map()

        assert lev_map_first == lev_map_second
        assert lev_map_first[pd.Timestamp("2025-12-31")] == pytest.approx(2.1)
        assert pd.Timestamp("2024-12-31") not in lev_map_first


def test_valuation_net_leverage_text_map_reuses_doc_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "leverage_docs.xlsx")
        root = out_path.parent.parent
        local_dir = root / "earnings_release"
        local_dir.mkdir(parents=True, exist_ok=True)
        sec_cache_dir = root / "sec_cache"
        sec_cache_dir.mkdir(parents=True, exist_ok=True)
        local_doc = local_dir / "TEST_Q4_2025_release.txt"
        local_doc.write_text("Adjusted net leverage was 2.0x.", encoding="utf-8")
        audit_doc = sec_cache_dir / "doc_000012345625000001_ex99.txt"
        audit_doc.write_text("Net leverage was 3.5x.", encoding="utf-8")
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": ["0000123456-25-000001"],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )
        manifest_df = pd.DataFrame({"path": [str(audit_doc)]})
        counts = {str(local_doc): 0, str(audit_doc): 0}
        original_read_text = Path.read_text

        def counted_read_text(self: Path, *args, **kwargs):
            path_str = str(self)
            if path_str in counts:
                counts[path_str] += 1
            return original_read_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        inputs = _make_inputs(out_path)
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)

        lev_map_first = ctx.callbacks.extract_adj_net_leverage_text_map()
        lev_map_second = ctx.callbacks.extract_adj_net_leverage_text_map()

        assert lev_map_first == lev_map_second
        assert lev_map_first[pd.Timestamp("2025-12-31")] == pytest.approx(2.0)
        assert counts[str(local_doc)] == 1
        assert counts[str(audit_doc)] == 1


def test_writer_doc_cache_is_shared_across_leverage_and_valuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "shared_doc_cache.xlsx")
        cache_dir = out_path.parent
        doc_path = cache_dir / "doc_000012345625000001_ex99.txt"
        doc_path.write_text(
            "Adjusted net leverage was 2.0x. "
            "In the fourth quarter, we repurchased $15 million of common stock. "
            "The board declared a regular quarterly dividend of $0.50 per share.",
            encoding="utf-8",
        )
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": ["0000123456-25-000001"],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )
        counts = {"read": 0, "glob": 0}
        original_read_text = Path.read_text
        original_glob = Path.glob
        doc_resolved = doc_path.resolve()
        cache_resolved = cache_dir.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == doc_resolved:
                counts["read"] += 1
            return original_read_text(self, *args, **kwargs)

        def counted_glob(self: Path, pattern: str):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == cache_resolved and pattern.startswith("doc_000012345625000001_"):
                counts["glob"] += 1
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        monkeypatch.setattr(Path, "glob", counted_glob)
        inputs = _make_inputs(out_path)
        manifest_df = pd.DataFrame({"path": [str(doc_path)]})
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)

        lev_map = ctx.callbacks.extract_adj_net_leverage_text_map()
        ensure_valuation_inputs(ctx)
        quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
        render_loader = ctx.state["_ensure_valuation_render_bundle"]
        precompute_loader = ctx.state["_ensure_valuation_precompute_bundle"]
        render_bundle = render_loader(quarter_key, ctx.derived.leverage_df)
        precompute_bundle = precompute_loader(quarter_key, render_bundle)

        assert lev_map[pd.Timestamp("2025-12-31")] == pytest.approx(2.0)
        assert precompute_bundle["buyback_map"][pd.Timestamp("2025-12-31")] == pytest.approx(15_000_000.0)
        assert precompute_bundle["dividend_ps_doc_map"][pd.Timestamp("2025-12-31")] == pytest.approx(0.5)
        assert counts["glob"] == 3
        assert counts["read"] == 1
        assert ctx.data.doc_cache.accession_doc_paths["000012345625000001"] == [doc_path]


def test_latest_quarter_qa_reuses_shared_sec_doc_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "qa_shared_doc_cache.xlsx")
        cache_dir = out_path.parent
        doc_path = cache_dir / "doc_000012345625000001_ex99.txt"
        doc_path.write_text(
            "Adjusted net leverage was 2.0x. Revenue was $130 million in the fourth quarter.",
            encoding="utf-8",
        )
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": ["0000123456-25-000001"],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )
        manifest_df = pd.DataFrame({"path": [str(doc_path)]})
        counts = {"read": 0, "glob": 0}
        original_read_text = Path.read_text
        original_glob = Path.glob
        doc_resolved = doc_path.resolve()
        cache_resolved = cache_dir.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == doc_resolved:
                counts["read"] += 1
            return original_read_text(self, *args, **kwargs)

        def counted_glob(self: Path, pattern: str):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == cache_resolved and pattern.startswith("doc_000012345625000001_"):
                counts["glob"] += 1
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        monkeypatch.setattr(Path, "glob", counted_glob)
        inputs = _make_inputs(out_path)
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)

        lev_map = ctx.callbacks.extract_adj_net_leverage_text_map()
        reads_after_leverage = counts["read"]
        globs_after_leverage = counts["glob"]
        qa_rows = ctx.callbacks.run_latest_quarter_qa()

        assert lev_map[pd.Timestamp("2025-12-31")] == pytest.approx(2.0)
        assert qa_rows
        assert counts["read"] == reads_after_leverage == 1
        assert counts["glob"] == globs_after_leverage == 3


def test_submission_recent_rows_cache_is_shared_across_valuation_and_qa(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "submission_cache.xlsx")
        cache_dir = out_path.parent
        doc_path = cache_dir / "doc_000012345625000001_ex99.txt"
        sub_path = cache_dir / "submissions_test.json"
        doc_path.write_text(
            "In the fourth quarter, we repurchased $15 million of common stock. Revenue was $130 million.",
            encoding="utf-8",
        )
        sub_path.write_text(
            json.dumps(
                {
                    "filings": {
                        "recent": {
                            "accessionNumber": ["0000123456-25-000001"],
                            "form": ["8-K"],
                            "filingDate": ["2026-02-20"],
                            "reportDate": ["2025-12-31"],
                            "primaryDocument": ["ex99.txt"],
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": ["0000123456-25-000001"],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )
        counts = {"sub_read": 0, "sub_glob": 0}
        original_read_text = Path.read_text
        original_glob = Path.glob
        sub_resolved = sub_path.resolve()
        cache_resolved = cache_dir.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == sub_resolved:
                counts["sub_read"] += 1
            return original_read_text(self, *args, **kwargs)

        def counted_glob(self: Path, pattern: str):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == cache_resolved and pattern == "submissions_*.json":
                counts["sub_glob"] += 1
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        monkeypatch.setattr(Path, "glob", counted_glob)
        manifest_df = pd.DataFrame({"path": [str(doc_path), str(sub_path)]})
        inputs = _make_inputs(out_path)
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)

        ensure_valuation_inputs(ctx)
        quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
        render_loader = ctx.state["_ensure_valuation_render_bundle"]
        precompute_loader = ctx.state["_ensure_valuation_precompute_bundle"]
        render_bundle = render_loader(quarter_key, ctx.derived.leverage_df)
        precompute_loader(quarter_key, render_bundle)
        reads_after_val = counts["sub_read"]
        globs_after_val = counts["sub_glob"]
        qa_rows = ctx.callbacks.run_latest_quarter_qa()

        assert qa_rows
        assert counts["sub_read"] == reads_after_val == 1
        assert counts["sub_glob"] == globs_after_val == 1


def test_buyback_auth_reuses_submission_rows_during_full_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "buyback_auth_cache.xlsx")
        cache_dir = out_path.parent
        accn = "0000123456-26-000002"
        doc_path = cache_dir / "doc_000012345626000002_new-20251231.htm"
        sub_path = cache_dir / "submissions_test.json"
        doc_path.write_text(
            "<html><body>"
            "In the fourth quarter, we repurchased $15 million of common stock. "
            "As of December 31, 2025, $125 million remained available under the share repurchase authorization. "
            "The board declared a regular quarterly dividend of $0.50 per share."
            "</body></html>",
            encoding="utf-8",
        )
        sub_path.write_text(
            json.dumps(
                {
                    "filings": {
                        "recent": {
                            "accessionNumber": [accn],
                            "form": ["10-K"],
                            "filingDate": ["2026-02-10"],
                            "reportDate": ["2025-12-31"],
                            "primaryDocument": ["new-20251231.htm"],
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": [accn],
                "form": ["10-K"],
                "filed": [pd.Timestamp("2026-02-10")],
            }
        )
        manifest_df = pd.DataFrame(
            {
                "cache_key": [
                    "submissions_test",
                    "doc_000012345626000002_new-20251231.htm",
                ],
                "path": [
                    str(sub_path),
                    str(doc_path),
                ],
                "status": ["ok", "ok"],
            }
        )
        counts = {"sub_read": 0}
        original_read_text = Path.read_text
        sub_resolved = sub_path.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == sub_resolved:
                counts["sub_read"] += 1
            return original_read_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        inputs = _make_inputs(out_path)
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)
        write_valuation_sheets(ctx)

        assert "Valuation" in ctx.wb.sheetnames
        assert counts["sub_read"] == 1


def test_slide_text_cache_is_reused_across_quarter_filtered_writer_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "slide_cache.xlsx")
        material_root = out_path.parent.parent
        slide_dir = material_root / "sec_cache" / "slides_text"
        slide_dir.mkdir(parents=True, exist_ok=True)
        slide_path = slide_dir / "Q4_2025_p1.txt"
        slide_path.write_text(
            "Production at 95% of stated capacity\nUtilization improved in the quarter.",
            encoding="utf-8",
        )

        counts = {"read": 0, "glob": 0}
        original_read_text = Path.read_text
        original_glob = Path.glob
        slide_resolved = slide_path.resolve()
        slide_dir_resolved = slide_dir.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == slide_resolved:
                counts["read"] += 1
            return original_read_text(self, *args, **kwargs)

        def counted_glob(self: Path, pattern: str):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == slide_dir_resolved and pattern == "*.txt":
                counts["glob"] += 1
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "read_text", counted_read_text)
        monkeypatch.setattr(Path, "glob", counted_glob)
        ctx = build_writer_context(_make_inputs(out_path))
        quarter_end = pd.Timestamp("2025-12-31").date()

        pages = ctx.state["_pbi_slide_pages_for_qd"](quarter_end)
        util_text = ctx.state["_local_slide_driver_fallback"](quarter_end, "utilization")

        assert pages
        assert "95%" in str(pages[0]["text"])
        assert util_text.startswith("Production at 95%")
        assert counts["glob"] == 1
        assert counts["read"] == 1


def test_profile_slide_signals_are_lazy_and_reuse_persistent_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_ticker_model_out_path(case_dir, "TEST", "profile_slide_signals.xlsx")
        material_root = out_path.parent.parent
        slide_dir = material_root / "sec_cache" / "slides_text"
        slide_dir.mkdir(parents=True, exist_ok=True)
        slide_path = slide_dir / "Q4_2025_p1.txt"
        slide_path.write_text(
            "Production at 95% of stated capacity.\nUtilization improved in the quarter.",
            encoding="utf-8",
        )

        test_profile = CompanyProfile(
            ticker="TEST",
            has_bank=False,
            industry_keywords=tuple(),
            segment_patterns=tuple(),
            segment_alias_patterns=tuple(),
            key_adv_require_keywords=tuple(),
            key_adv_deny_keywords=tuple(),
            quarter_note_priority_terms=("utilization",),
            promise_priority_terms=tuple(),
        )

        counts = {"read": 0}
        original_read_text = Path.read_text
        slide_resolved = slide_path.resolve()

        def counted_read_text(self: Path, *args, **kwargs):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == slide_resolved:
                counts["read"] += 1
            return original_read_text(self, *args, **kwargs)

        monkeypatch.setattr(writer_context_module, "get_company_profile", lambda _ticker: test_profile)
        monkeypatch.setattr(Path, "read_text", counted_read_text)

        ctx_first = build_writer_context(_make_inputs(out_path, ticker="TEST"))

        assert counts["read"] == 0

        grouped_first = ctx_first.state["_load_profile_slide_signals_by_quarter"]()

        assert grouped_first[pd.Timestamp("2025-12-31").date()]
        assert counts["read"] == 1

        counts["read"] = 0
        ctx_second = build_writer_context(_make_inputs(out_path, ticker="TEST"))
        grouped_second = ctx_second.state["_load_profile_slide_signals_by_quarter"]()

        assert grouped_second == grouped_first
        assert counts["read"] == 0


def test_frame_view_cache_reuses_quarter_notes_view_without_mutating_inputs() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "frame_view_cache.xlsx")
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "note_id": ["note-1"],
                "category": ["Guidance"],
                "claim": ["Revenue should stabilize by year end."],
                "note": ["Revenue should stabilize by year end."],
                "metric_ref": ["revenue_yoy"],
                "doc_path": ["doc.html"],
                "evidence_snippet": ["Revenue should stabilize by year end."],
                "evidence_json": [
                    json.dumps(
                        [
                            {
                                "doc_path": "doc.html",
                                "section_or_page": "p1",
                                "snippet": "Revenue should stabilize by year end.",
                            }
                        ]
                    )
                ],
            }
        )
        ctx = build_writer_context(_make_inputs(out_path, quarter_notes=quarter_notes))

        assert "_quarter" not in quarter_notes.columns
        assert ("quarter_notes", "timestamp") not in ctx.data.frame_view_cache

        ctx.callbacks.build_qn_evidence_src()
        cached_view = ctx.data.frame_view_cache[("quarter_notes", "timestamp")]
        cached_id = id(cached_view)
        ctx.callbacks.write_quarter_notes_ui_v2()

        assert id(ctx.data.frame_view_cache[("quarter_notes", "timestamp")]) == cached_id
        assert "_quarter" in cached_view.columns
        assert "_quarter" not in quarter_notes.columns
        assert "_quarter" not in ctx.inputs.quarter_notes.columns


def test_quarter_note_runtime_signature_changes_when_semantics_change() -> None:
    base = {
        "note_id": "n-1",
        "candidate_type": "investor_note",
        "change_badge": "NEW",
        "bucket": "Capital allocation / shareholder returns",
        "_metric_display": "Buyback execution",
        "metric_canon": "Buyback execution",
        "metric_tag": "buybacks_cash",
        "text_full": "Repurchased 12.6m shares for $126.6m with an average price of $10.04/share in Q4.",
        "comment_full_text": "Repurchased 12.6m shares for $126.6m with an average price of $10.04/share in Q4.",
        "evidence_snippet": "Repurchased 12.6m shares for $126.6m with an average price of $10.04/share in Q4.",
        "score": 96.0,
        "_event_score": 101.0,
        "doc_priority": 5,
        "source": {
            "source_type": "issuer_purchases_table",
            "doc": "doc_000000000026000001_10k.htm",
            "form": "10-K",
            "section": "Issuer Purchases of Equity Securities",
        },
    }

    source_changed = dict(base)
    source_changed["source"] = {
        "source_type": "earnings_release",
        "doc": "ex99_1.htm",
        "form": "8-K",
        "section": "Press release",
    }
    score_changed = dict(base)
    score_changed["_event_score"] = 88.0
    summary_changed = dict(base)
    summary_changed["_render_summary"] = "Repurchase authorization increased by $250.0m."

    base_sig = writer_context_module._quarter_note_runtime_signature(base)

    assert writer_context_module._quarter_note_runtime_signature(source_changed) != base_sig
    assert writer_context_module._quarter_note_runtime_signature(score_changed) != base_sig
    assert writer_context_module._quarter_note_runtime_signature(summary_changed) != base_sig


def test_quarter_notes_runtime_cache_is_scoped_to_each_export_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            first_out = _make_model_out_path(case_dir, "runtime_cache_run_one.xlsx")
            second_out = _make_model_out_path(case_dir, "runtime_cache_run_two.xlsx")
            first_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "note_id": ["same-note-id"],
                    "category": ["Guidance / outlook"],
                    "claim": ["FY 2026 Revenue guidance $1,760m-$1,860m."],
                    "note": ["FY 2026 Revenue guidance $1,760m-$1,860m."],
                    "metric_ref": ["Revenue guidance"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q4_v1.txt"],
                    "source_type": ["earnings_release"],
                    "evidence_snippet": ["FY 2026 Revenue guidance $1,760m-$1,860m."],
                }
            )
            second_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "note_id": ["same-note-id"],
                    "category": ["Guidance / outlook"],
                    "claim": ["FY 2026 Revenue guidance $1,880m-$1,980m."],
                    "note": ["FY 2026 Revenue guidance $1,880m-$1,980m."],
                    "metric_ref": ["Revenue guidance"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q4_v2.txt"],
                    "source_type": ["earnings_release"],
                    "evidence_snippet": ["FY 2026 Revenue guidance $1,880m-$1,980m."],
                }
            )

            first_result = write_excel_from_inputs(
                _make_inputs(first_out, ticker="TEST", quarter_notes=first_notes)
            )
            second_result = write_excel_from_inputs(
                _make_inputs(second_out, ticker="TEST", quarter_notes=second_notes)
            )

            first_rows = list(first_result.quarter_notes_ui_snapshot.get("2025-12-31") or [])
            second_rows = list(second_result.quarter_notes_ui_snapshot.get("2025-12-31") or [])

            assert any("FY 2026 Revenue guidance $1,760m-$1,860m." in note for _, note in first_rows)
            assert not any("FY 2026 Revenue guidance $1,880m-$1,980m." in note for _, note in first_rows)
            assert any("FY 2026 Revenue guidance $1,880m-$1,980m." in note for _, note in second_rows)
            assert not any("FY 2026 Revenue guidance $1,760m-$1,860m." in note for _, note in second_rows)


def test_latest_quarter_sec_text_corpus_is_reused_across_repeated_qa_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "latest_q_corpus.xlsx")
        material_root = out_path.parent.parent
        cache_dir = material_root / "sec_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        accn = "0000123456-25-000001"
        doc_path = cache_dir / f"doc_{accn.replace('-', '')}_ex99.htm"
        doc_path.write_text("Updated full year guidance revenue $500 million to $520 million.", encoding="utf-8")

        audit_df = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "accn": [accn],
                "form": ["8-K"],
                "filed": [pd.Timestamp("2026-02-20")],
            }
        )

        counts = {"glob": 0}
        original_glob = Path.glob
        cache_resolved = cache_dir.resolve()

        def counted_glob(self: Path, pattern: str):
            try:
                current = self.resolve()
            except Exception:
                current = self
            if current == cache_resolved and pattern.startswith(f"doc_{accn.replace('-', '')}_"):
                counts["glob"] += 1
            return original_glob(self, pattern)

        monkeypatch.setattr(Path, "glob", counted_glob)
        manifest_df = pd.DataFrame({"path": [str(doc_path)]})
        inputs = _make_inputs(out_path)
        inputs = inputs.__class__(**{**vars(inputs), "audit": audit_df, "manifest_df": manifest_df})
        ctx = build_writer_context(inputs)

        qa_once = ctx.callbacks.run_latest_quarter_qa()
        globs_after_first = counts["glob"]
        qa_twice = ctx.callbacks.run_latest_quarter_qa()

        assert qa_once == qa_twice
        assert globs_after_first == counts["glob"]
        assert globs_after_first == 3
        assert ctx.data.doc_cache.latest_quarter_sec_text_by_quarter


def test_write_ui_sheets_records_ui_substage_timings() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "ui_timings.xlsx")
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-12-31")],
                "note_id": ["note-1"],
                "category": ["Guidance"],
                "claim": ["Revenue should stabilize by year end."],
                "note": ["Revenue should stabilize by year end."],
                "metric_ref": ["revenue_yoy"],
                "doc_path": ["doc.html"],
                "evidence_snippet": ["Revenue should stabilize by year end."],
                "evidence_json": [
                    json.dumps(
                        [
                            {
                                "doc_path": "doc.html",
                                "section_or_page": "p1",
                                "snippet": "Revenue should stabilize by year end.",
                            }
                        ]
                    )
                ],
            }
        )
        promises = pd.DataFrame(
            {
                "promise_id": ["p-1"],
                "first_seen_evidence_quarter": [pd.Timestamp("2025-12-31")],
                "promise_text": ["Revenue should stabilize by year end."],
                "source_evidence_json": [
                    json.dumps({"doc_path": "doc.html", "snippet": "Revenue should stabilize by year end."})
                ],
            }
        )
        progress = pd.DataFrame(
            {
                "promise_id": ["p-1"],
                "quarter": [pd.Timestamp("2025-12-31")],
                "status": ["open"],
                "source_evidence_json": [
                    json.dumps({"doc_path": "doc.html", "snippet": "Revenue should stabilize by year end."})
                ],
            }
        )
        inputs = _make_inputs(out_path, quarter_notes=quarter_notes)
        inputs = inputs.__class__(**{**vars(inputs), "promises": promises, "promise_progress": progress})
        ctx = build_writer_context(inputs)
        ctx.callbacks.write_quarter_notes_ui_v2()
        write_ui_sheets(ctx)

        assert {
            "write_excel.ui",
            "write_excel.ui.raw_frames",
            "write_excel.ui.render.quarter_notes",
            "write_excel.ui.render.promise_tracker",
            "write_excel.ui.render.promise_progress",
            "write_excel.ui.progress_bundle.build",
            "write_excel.ui.progress_rows.select",
            "write_excel.ui.progress_rows.follow_through",
            "write_excel.ui.progress_rows.dedupe",
            "write_excel.ui.progress_rows.render",
        }.issubset(ctx.writer_timings.keys())


def test_local_balance_sheet_payloads_parse_only_target_quarters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "local_bs_lazy.xlsx")
        material_root = out_path.parent.parent
        fs_dir = material_root / "financial_statement"
        fs_dir.mkdir(parents=True, exist_ok=True)

        target_path = fs_dir / "TEST_Q4_2025_balance_sheet.txt"
        stale_path = fs_dir / "TEST_Q4_2024_balance_sheet.txt"
        target_path.write_text("Balance sheet for quarter ended December 31, 2025.", encoding="utf-8")
        stale_path.write_text("Balance sheet for quarter ended December 31, 2024.", encoding="utf-8")

        parse_calls: list[pd.Timestamp] = []

        def fake_extract_balance_sheet_from_text(text_in: str, quarter_end: pd.Timestamp):
            qv = pd.Timestamp(quarter_end)
            parse_calls.append(qv)
            if qv == pd.Timestamp("2025-12-31"):
                return {"values": {"goodwill": 42_000_000.0, "intangibles": 18_000_000.0}}
            if qv == pd.Timestamp("2024-12-31"):
                return {"values": {"goodwill": 9_000_000.0}}
            return None

        monkeypatch.setattr(
            writer_context_module,
            "_extract_balance_sheet_from_text",
            fake_extract_balance_sheet_from_text,
        )

        ctx = build_writer_context(_make_inputs(out_path, profile_timings=True))
        ensure_valuation_inputs(ctx)
        quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
        render_loader = ctx.state["_ensure_valuation_render_bundle"]

        render_bundle = render_loader(quarter_key, ctx.derived.leverage_df)
        render_loader(quarter_key, ctx.derived.leverage_df)

        assert parse_calls == [pd.Timestamp("2025-12-31")]
        assert render_bundle["goodwill_map"][pd.Timestamp("2025-12-31")] == pytest.approx(42_000_000.0)
        assert render_bundle["intangibles_map"][pd.Timestamp("2025-12-31")] == pytest.approx(18_000_000.0)
        assert "write_excel.valuation.bundle.local_bs.index" in ctx.writer_timings
        assert "write_excel.valuation.bundle.local_bs.parse_selected" in ctx.writer_timings
        assert "write_excel.valuation.bundle.local_bs.pick_best" in ctx.writer_timings


def test_pbi_profile_omits_driver_and_economics_sheets_but_keeps_ui_sheets() -> None:
    with _case_dir() as case_dir:
        out_path = _make_ticker_model_out_path(case_dir, "PBI", "pbi_gating.xlsx")
        write_excel_from_inputs(_make_inputs(out_path, ticker="PBI"))

        wb = load_workbook(out_path, data_only=False)
        assert "Operating_Drivers" not in wb.sheetnames
        assert "Economics_Overlay" not in wb.sheetnames
        assert "economics_market_raw" not in wb.sheetnames
        assert "Quarter_Notes_UI" in wb.sheetnames
        assert "Promise_Tracker_UI" in wb.sheetnames
        assert "Promise_Progress_UI" in wb.sheetnames


def test_pbi_bs_segments_prefers_quarterly_segment_block_from_segment_workbook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_bs_segments.xlsx")
            material_root = out_path.parent.parent
            _write_pbi_segment_workbook(material_root / "segment_financials", include_quarters=True)

            write_excel_from_inputs(_make_inputs(out_path, ticker="TEST"))
            ws = load_workbook(out_path, data_only=False)["BS_Segments"]

        quarterly_row = _find_row_with_value(ws, "Quarterly segments")
        annual_row = _find_row_with_value(ws, "Annual segments")
        assert quarterly_row is not None
        assert annual_row is not None
        assert quarterly_row < annual_row
        assert str(ws["A4"].value).startswith("QA:")
        assert [ws.cell(row=11, column=cc).value for cc in range(2, 6)] == [
            "2025-Q1",
            "2025-Q2",
            "2025-Q3",
            "2025-Q4",
        ]

        def _find_after(start_row: int, label: str) -> int:
            for rr in range(start_row + 1, ws.max_row + 1):
                if ws.cell(row=rr, column=1).value == label:
                    return rr
            raise AssertionError(f"Could not find {label!r} after row {start_row}")

        revenue_section = _find_after(quarterly_row, "Revenue")
        revenue_sendtech = _find_after(revenue_section, "SendTech Solutions")
        revenue_presort = _find_after(revenue_sendtech, "Presort Services")
        assert [ws.cell(row=revenue_sendtech, column=cc).value for cc in range(2, 6)] == [480.0, 490.0, 500.0, 510.0]
        assert [ws.cell(row=revenue_presort, column=cc).value for cc in range(2, 6)] == [300.0, 305.0, 310.0, 315.0]

        ebit_section = _find_after(revenue_presort, "Adjusted EBIT")
        ebit_sendtech = _find_after(ebit_section, "SendTech Solutions")
        assert [ws.cell(row=ebit_sendtech, column=cc).value for cc in range(2, 6)] == [120.0, 125.0, 130.0, 135.0]

        margin_section = _find_after(ebit_sendtech, "EBIT margin %")
        margin_sendtech = _find_after(margin_section, "SendTech Solutions")
        assert [ws.cell(row=margin_sendtech, column=cc).value for cc in range(2, 6)] == [0.25, 0.255, 0.26, 0.265]

        da_section = _find_after(margin_sendtech, "Depreciation & amortization")
        da_sendtech = _find_after(da_section, "SendTech Solutions")
        assert [ws.cell(row=da_sendtech, column=cc).value for cc in range(2, 6)] == [18.0, 19.0, 20.0, 21.0]
        assert ws.cell(row=revenue_sendtech, column=2).fill.patternType == "solid"
        assert ws.cell(row=revenue_section, column=2).fill.patternType == "solid"


def test_pbi_bs_segments_falls_back_to_annual_only_when_quarterly_workbook_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_bs_segments_annual_only.xlsx")
            material_root = out_path.parent.parent
            _write_pbi_segment_workbook(material_root / "segment_financials", include_quarters=False)

            write_excel_from_inputs(_make_inputs(out_path, ticker="TEST"))
            ws = load_workbook(out_path, data_only=False)["BS_Segments"]

        assert _find_row_with_value(ws, "Quarterly segments") is None
        annual_row = _find_row_with_value(ws, "Annual segments")
        assert annual_row is not None
        assert _find_row_with_value(ws, "Revenues") is not None


def test_valuation_guidance_layout_uses_new_column_spans_and_preserves_type_wording() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "guidance_layout.xlsx")
        hist = _make_hist().iloc[:2].copy().reset_index(drop=True)
        slides_guidance = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-06-30")],
                "line": [
                    "Updated full-year 2025 revenue guidance $1900 to $1950 adjusted EBIT $450 to $465 adjusted EPS $1.20 to $1.40 free cash flow $330 to $370",
                    "Updated full-year 2025 revenue guidance $1910 to $1960 adjusted EBIT $455 to $470",
                ],
                "heading": ["guidance", "guidance"],
                "filed": [pd.Timestamp("2025-05-01"), pd.Timestamp("2025-08-01")],
                "doc": ["guidance_q1.txt", "guidance_q2.txt"],
            }
        )

        inputs = _make_inputs(out_path, hist=hist)
        inputs = inputs.__class__(**{**vars(inputs), "slides_guidance": slides_guidance})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)
        ctx.callbacks.write_valuation_sheet()
        ws = ctx.wb["Valuation"]
        merged_ranges = {str(rng) for rng in ws.merged_cells.ranges}

        guidance_header_row = _find_row_with_value(ws, "Range/Value", column=19)
        assert guidance_header_row is not None
        assert f"Q{guidance_header_row}:R{guidance_header_row}" in merged_ranges
        assert f"S{guidance_header_row}:X{guidance_header_row}" in merged_ranges
        assert f"Y{guidance_header_row}:Z{guidance_header_row}" in merged_ranges
        assert f"AA{guidance_header_row}:AB{guidance_header_row}" in merged_ranges
        assert ws.cell(row=guidance_header_row, column=25).value == "Δ vs prev"

        stated_row = None
        carry_row = None
        for rr in range(1, ws.max_row + 1):
            stated_type_value = str(ws.cell(row=rr, column=17).value or "")
            carry_type_value = str(ws.cell(row=rr, column=16).value or "")
            if "stated Q1 2025" in stated_type_value and stated_row is None:
                stated_row = rr
            if "carry-fwd Q1 2025" in carry_type_value and carry_row is None:
                carry_row = rr
        assert stated_row is not None
        assert carry_row is not None
        assert f"Q{stated_row}:R{stated_row}" in merged_ranges
        assert f"S{stated_row}:X{stated_row}" in merged_ranges
        assert f"Y{stated_row}:Z{stated_row}" in merged_ranges
        assert f"AA{stated_row}:AB{stated_row}" in merged_ranges
        assert ws.cell(row=stated_row, column=17).alignment.wrap_text is True
        assert ws.cell(row=stated_row, column=19).alignment.wrap_text is True
        assert ws.cell(row=stated_row, column=27).alignment.wrap_text is True
        assert f"P{carry_row}:S{carry_row}" in merged_ranges
        assert f"T{carry_row}:Z{carry_row}" in merged_ranges
        assert f"AA{carry_row}:AB{carry_row}" in merged_ranges
        assert f"Y{carry_row}:Z{carry_row}" not in merged_ranges
        assert ws.cell(row=carry_row, column=16).alignment.wrap_text is True
        assert ws.cell(row=carry_row, column=20).alignment.wrap_text is True
        assert ws.cell(row=carry_row, column=27).alignment.wrap_text is True
        carry_value = str(ws.cell(row=carry_row, column=20).value or "")
        assert carry_value.startswith("updated target to ")
        assert float(ws.row_dimensions[carry_row].height or 0.0) <= 58.0
        assert str(ws.freeze_panes) == "B7"


def test_valuation_thesis_bridge_adds_note_keeps_label_and_formulas() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "thesis_bridge.xlsx")
        ctx = build_writer_context(_make_inputs(out_path))
        ensure_valuation_inputs(ctx)
        ctx.callbacks.write_valuation_sheet()
        ws = ctx.wb["Valuation"]

        note_text = "Enter annual incremental EBITDA/equity effects, not raw rate changes."
        note_row = _find_row_with_value(ws, note_text, column=20)
        bridge_label_row = _find_row_with_value(ws, "Interest savings / debt-paydown uplift", column=15)
        thesis_output_row = _find_row_with_value(ws, "Thesis value/share @ target multiple", column=15)

        assert note_row is not None
        assert bridge_label_row is not None
        assert thesis_output_row is not None
        assert ws.cell(row=thesis_output_row, column=19).value.startswith("=")
        assert float(ws.column_dimensions["B"].width or 0.0) == pytest.approx(12.43, abs=0.02)
        assert float(ws.column_dimensions["C"].width or 0.0) == pytest.approx(12.43, abs=0.02)
        assert float(ws.column_dimensions["S"].width or 0.0) >= 24.0
        assert ws.cell(row=bridge_label_row, column=19).number_format == "#,##0.000"
        thesis_formula_cells = [
            ws.cell(row=_find_row_with_value(ws, "Thesis Adj EBITDA", column=15), column=19).value,
            ws.cell(row=_find_row_with_value(ws, "Thesis FCF", column=15), column=19).value,
            ws.cell(row=_find_row_with_value(ws, "Thesis EV @ target multiple", column=15), column=19).value,
            ws.cell(row=_find_row_with_value(ws, "Thesis equity value @ target multiple", column=15), column=19).value,
            ws.cell(row=_find_row_with_value(ws, "Thesis value/share @ target multiple", column=15), column=19).value,
            ws.cell(row=_find_row_with_value(ws, "Thesis equity value @ target FCF yield", column=15), column=19).value,
            ws.cell(row=_find_row_with_value(ws, "Thesis value/share @ target FCF yield", column=15), column=19).value,
        ]
        assert all("*1000000" not in str(v) and "/1000000" not in str(v) for v in thesis_formula_cells)


def test_valuation_columns_b_and_c_use_consistent_width_for_pbi_and_gpre(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        for ticker in ("PBI", "GPRE"):
            with _profile_override(monkeypatch, ticker):
                out_path = _make_model_out_path(case_dir, f"{ticker.lower()}_valuation_widths.xlsx")
                ctx = build_writer_context(_make_inputs(out_path, ticker="TEST"))
                ensure_valuation_inputs(ctx)
                ctx.callbacks.write_valuation_sheet()
                ws = ctx.wb["Valuation"]

                assert float(ws.column_dimensions["B"].width or 0.0) == pytest.approx(12.43, abs=0.02)
                assert float(ws.column_dimensions["C"].width or 0.0) == pytest.approx(12.43, abs=0.02)


def test_valuation_buybacks_can_use_explicit_sec_repurchase_disclosures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_ticker_model_out_path(case_dir, "GPRE", "gpre_valuation_buybacks.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. "
                "On October 27, 2025, in conjunction with the privately negotiated exchange and subscription agreements for the 2030 Notes, "
                "the company repurchased 2.9 million shares of its common stock for a total of $30.0 million under the repurchase program. "
                "At February 10, 2026, $77.2 million in share repurchase authorization remained.",
                encoding="utf-8",
            )

            inputs = _make_inputs(out_path, ticker="GPRE")
            inputs = inputs.__class__(**{**vars(inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ensure_valuation_inputs(ctx)
            quarter_key = tuple(pd.Timestamp(q) for q in ctx.inputs.hist["quarter"].tolist())
            render_loader = ctx.state["_ensure_valuation_render_bundle"]
            render_bundle = render_loader(quarter_key, ctx.derived.leverage_df)
            render_bundle["buyback_shares_q_map"][pd.Timestamp("2025-12-31")] = 2_101_000.0
            render_bundle["buyback_cash_facts_map"][pd.Timestamp("2025-12-31")] = 2_000_000.0
            ctx.derived.valuation_render_bundle = render_bundle
            ctx.callbacks.write_valuation_sheet()
            ws = ctx.wb["Valuation"]

            buybacks_row = _find_row_with_value(ws, "Buybacks (shares)")
            buybacks_note_row = _find_row_with_value(ws, "Buybacks note")
            obs5_row = _find_row_with_value(ws, "Obs 5")

            assert buybacks_row is not None
            assert buybacks_note_row is not None
            assert obs5_row is not None
            assert str(ws.cell(row=buybacks_row, column=2).value or "") != "n/a"
            assert "2.900m" in str(ws.cell(row=buybacks_row, column=2).value or "")
            assert "spent latest quarter $30.0m" in str(ws.cell(row=buybacks_note_row, column=2).value or "")
            assert "$10.34/share" in str(ws.cell(row=buybacks_note_row, column=2).value or "")
            obs5_text = str(ws.cell(row=obs5_row, column=2).value or "")
            assert (
                "explicit SEC repurchase disclosures" in obs5_text
                or "cash buybacks $30.0m" in obs5_text.lower()
            )


def test_pbi_quarter_notes_ui_keeps_clean_driver_note_alongside_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_quarter_notes_driver.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 3,
                    "note_id": ["guidance-1", "driver-1", "junk-1"],
                    "category": ["Guidance / outlook", "Results / drivers", "Other / footnotes"],
                    "claim": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                        "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                        "map permit list county parcel latitude longitude $12 million",
                    ],
                    "note": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                        "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                        "map permit list county parcel latitude longitude $12 million",
                    ],
                    "metric_ref": ["Adjusted EBIT guidance", "PB Bank liquidity release", "Other"],
                    "score": [95.0, 93.0, 96.0],
                    "doc_type": ["earnings_release", "transcript", "ocr"],
                    "doc": ["release_q4.txt", "transcript_q4.txt", "ocr_junk.txt"],
                    "evidence_snippet": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                        "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                        "map permit list county parcel latitude longitude $12 million",
                    ],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "release_q4.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                                }
                            ]
                        ),
                        json.dumps(
                            [
                                {
                                    "doc_path": "transcript_q4.txt",
                                    "doc_type": "transcript",
                                    "snippet": "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                                }
                            ]
                        ),
                        json.dumps(
                            [
                                {
                                    "doc_path": "ocr_junk.txt",
                                    "doc_type": "ocr",
                                    "snippet": "map permit list county parcel latitude longitude $12 million",
                                }
                            ]
                        ),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            metrics = [str(ws.cell(row=rr, column=4).value or "") for rr in range(1, ws.max_row + 1)]
            notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]

            assert "Adjusted EBIT guidance" in metrics
            assert "PB Bank liquidity release" in metrics
            assert not any("map permit list county parcel" in note.lower() for note in notes)


def test_pbi_quarter_notes_ui_does_not_relabel_generic_guidance_as_sendtech_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_quarter_notes_no_sendtech_mislabel.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 2,
                    "note_id": ["bad-guidance-1", "good-driver-1"],
                    "category": ["Guidance / outlook", "Results / drivers"],
                    "claim": [
                        "Full-year 2025 revenue guidance was $1.95 billion to $2.00 billion.",
                        "SendTech and Presort margins improved on pricing and mix while PB Bank liquidity improved.",
                    ],
                    "note": [
                        "Full-year 2025 revenue guidance was $1.95 billion to $2.00 billion.",
                        "SendTech and Presort margins improved on pricing and mix while PB Bank liquidity improved.",
                    ],
                    "metric_ref": ["SendTech / Presort operating driver", "SendTech / Presort operating driver"],
                    "score": [95.0, 91.0],
                    "doc_type": ["earnings_release", "transcript"],
                    "doc": ["release_q1.txt", "transcript_q1.txt"],
                    "evidence_snippet": [
                        "Full-year 2025 revenue guidance was $1.95 billion to $2.00 billion.",
                        "SendTech and Presort margins improved on pricing and mix while PB Bank liquidity improved.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q1.txt", "doc_type": "earnings_release", "snippet": "Full-year 2025 revenue guidance was $1.95 billion to $2.00 billion."}]),
                        json.dumps([{"doc_path": "transcript_q1.txt", "doc_type": "transcript", "snippet": "SendTech and Presort margins improved on pricing and mix while PB Bank liquidity improved."}]),
                    ],
                }
            )
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_rows = [
                (
                    str(ws.cell(row=rr, column=3).value or ""),
                    str(ws.cell(row=rr, column=4).value or ""),
                )
                for rr in range(1, ws.max_row + 1)
                if str(ws.cell(row=rr, column=3).value or "").strip()
            ]
            assert not any(
                metric == "SendTech / Presort operating driver" and "$1.95" in note
                for note, metric in visible_rows
            )
            assert all(
                "$1.95" not in note or metric != "SendTech / Presort operating driver"
                for note, metric in visible_rows
            )


def test_pbi_promise_progress_uses_observed_actual_for_resolved_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_latest.xlsx")
            hist = _make_hist().copy()
            hist["revenue"] = [100_000_000.0, 110_000_000.0, 120_000_000.0, 130_000_000.0]
            progress = pd.DataFrame(
                {
                    "promise_id": ["p-revenue"],
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "status": ["resolved_fail"],
                    "metric_ref": ["Revenue guidance"],
                    "target": ["$500.0m-$520.0m"],
                    "latest": ["not yet measurable"],
                    "rationale": ["FY 2025 revenue guidance was $500 million to $520 million."],
                    "promise_type": ["guidance_range"],
                    "guidance_type": ["period"],
                    "target_period_norm": ["FY2025"],
                    "source_evidence_json": [
                        json.dumps(
                            {
                                "doc_type": "earnings_release",
                                "snippet": "FY 2025 revenue guidance was $500 million to $520 million.",
                            }
                        )
                    ],
                }
            )
            inputs = _make_inputs(out_path, ticker="TEST", hist=hist)
            inputs = inputs.__class__(**{**vars(inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            target_row = None
            for rr in range(1, ws.max_row + 1):
                if ws.cell(row=rr, column=2).value == "Revenue guidance":
                    target_row = rr
                    break
            assert target_row is not None
            assert ws.cell(row=target_row, column=4).value != "not yet measurable"
            assert isinstance(ws.cell(row=target_row, column=4).value, (int, float))
            assert ws.cell(row=target_row, column=5).value == "miss"


def test_pbi_promise_progress_recovers_later_actual_for_older_fy2025_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_later_actual.xlsx")
            hist = _make_hist().copy()
            hist["revenue"] = [100_000_000.0, 110_000_000.0, 120_000_000.0, 530_000_000.0]
            progress = pd.DataFrame(
                {
                    "promise_id": ["p-revenue-q3"],
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "status": ["on_track"],
                    "metric_ref": ["Revenue guidance"],
                    "target": ["$500.0m-$520.0m"],
                    "latest": ["not yet measurable"],
                    "rationale": ["FY 2025 revenue guidance was $500 million to $520 million."],
                    "promise_type": ["guidance_range"],
                    "guidance_type": ["period"],
                    "target_period_norm": ["FY2025"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "FY 2025 revenue guidance was $500 million to $520 million."})
                    ],
                }
            )
            inputs = _make_inputs(out_path, ticker="TEST", hist=hist)
            inputs = inputs.__class__(**{**vars(inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            target_row = None
            for rr in range(1, ws.max_row + 1):
                if ws.cell(row=rr, column=2).value == "Revenue guidance":
                    target_row = rr
                    break
            assert target_row is not None
            assert ws.cell(row=target_row, column=4).value != "not yet measurable"
            assert isinstance(ws.cell(row=target_row, column=4).value, (int, float))


def test_pbi_promise_progress_drops_cost_savings_row_when_guidance_alignment_is_wrong(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_cost_savings_alignment.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["bad-cost-savings"],
                    "quarter": [pd.Timestamp("2024-12-31")],
                    "status": ["pending"],
                    "metric_ref": ["Cost savings target"],
                    "target": ["$450.0m-$480.0m"],
                    "latest": ["not yet measurable"],
                    "rationale": ["Adjusted EBIT guidance increased to $450 million to $480 million for FY 2025."],
                    "promise_type": ["guidance_range"],
                    "guidance_type": ["period"],
                    "target_period_norm": ["FY2025"],
                    "source_evidence_json": [
                        json.dumps(
                            {
                                "doc_type": "earnings_release",
                                "snippet": "Adjusted EBIT guidance increased to $450 million to $480 million for FY 2025.",
                            }
                        )
                    ],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [
                str(ws.cell(row=rr, column=cc).value or "")
                for rr in range(1, ws.max_row + 1)
                for cc in range(1, 8)
            ]
            assert all("Cost savings target" not in val for val in visible_values)
            assert all("$450.0m-$480.0m" not in val for val in visible_values)


def test_promise_progress_strips_illegal_control_characters_from_rationale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "promise_progress_illegal_chars.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["gpre-illegal-char"],
                    "quarter": [pd.Timestamp("2025-06-30")],
                    "status": ["on_track"],
                    "metric_ref": ["Cost reduction initiative"],
                    "target": [""],
                    "latest": ["Cost reductions ahead of plan"],
                    "rationale": [
                        "Cost reductions are on pace to exceed the $50.0m annualized savings target.\x00"
                    ],
                    "promise_type": ["operational"],
                    "guidance_type": [""],
                    "target_period_norm": [""],
                    "source_evidence_json": [
                        json.dumps(
                            {
                                "doc_type": "press_release",
                                "snippet": "Cost reductions are on pace to exceed the $50.0m annualized savings target.",
                            }
                        )
                    ],
                }
            )

            inputs = _make_inputs(out_path, ticker="TEST", promise_progress=progress)
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_text = [
                str(ws.cell(row=rr, column=cc).value or "")
                for rr in range(1, ws.max_row + 1)
                for cc in range(1, 12)
            ]
            assert all("\x00" not in val for val in visible_text)

            comments = [
                str(ws.cell(row=rr, column=cc).comment.text or "")
                for rr in range(1, ws.max_row + 1)
                for cc in range(1, 12)
                if ws.cell(row=rr, column=cc).comment is not None
            ]
            assert all("\x00" not in text for text in comments)


def test_gpre_quarter_notes_ui_filters_dropped_and_fragment_junk_but_keeps_high_signal_notes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_quarter_notes_cleanup.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 3,
                    "note_id": ["good-1", "junk-1", "junk-2"],
                    "category": ["Programs / initiatives", "What changed", "Other / footnotes"],
                    "claim": [
                        "Utilization improved to 95% as York is fully operational and Central City and Wood River are online and ramping, supporting $95 million of 45Z monetization opportunity.",
                        "[DROPPED] map permit list county parcel latitude longitude $12 million",
                        "permit map county parcel latitude longitude list $10 million",
                    ],
                    "note": [
                        "Utilization improved to 95% as York is fully operational and Central City and Wood River are online and ramping, supporting $95 million of 45Z monetization opportunity.",
                        "[DROPPED] map permit list county parcel latitude longitude $12 million",
                        "permit map county parcel latitude longitude list $10 million",
                    ],
                    "metric_ref": ["45Z monetization / EBITDA", "What changed", "Other"],
                    "score": [95.0, 97.0, 95.0],
                    "doc_type": ["earnings_release", "earnings_release", "ocr"],
                    "doc": ["release_q4.txt", "release_q4.txt", "ocr_bad.txt"],
                    "evidence_snippet": [
                        "Utilization improved to 95% as York is fully operational and Central City and Wood River are online and ramping, supporting $95 million of 45Z monetization opportunity.",
                        "[DROPPED] map permit list county parcel latitude longitude $12 million",
                        "permit map county parcel latitude longitude list $10 million",
                    ],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "release_q4.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": "Utilization improved to 95% as York is fully operational and Central City and Wood River are online and ramping, supporting $95 million of 45Z monetization opportunity.",
                                }
                            ]
                        ),
                        json.dumps(
                            [
                                {
                                    "doc_path": "release_q4.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": "[DROPPED] map permit list county parcel latitude longitude $12 million",
                                }
                            ]
                        ),
                        json.dumps(
                            [
                                {
                                    "doc_path": "ocr_bad.txt",
                                    "doc_type": "ocr",
                                    "snippet": "permit map county parcel latitude longitude list $10 million",
                                }
                            ]
                        ),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            visible_metrics = [str(ws.cell(row=rr, column=4).value or "") for rr in range(1, ws.max_row + 1)]
            assert any("Utilization" in note for note in visible_notes) or any(
                "45Z monetization opportunity estimated at $95.0m." in note for note in visible_notes
            ) or any(
                "45Z Monetization / EBITDA" in metric for metric in visible_metrics
            )
            assert all(not note.startswith("[DROPPED]") for note in visible_notes if note)
            assert not any("permit map county parcel" in note.lower() for note in visible_notes)


def test_pbi_quarter_notes_ui_allows_more_than_two_clean_guidance_rows_when_strong(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_quarter_notes_guidance_wide.xlsx")
            guidance_texts = [
                (
                    "Revenue guidance",
                    "Revenue guidance was reaffirmed at $1.90 billion to $1.95 billion for FY 2025 with improving parcel volumes.",
                ),
                (
                    "Adjusted EBIT guidance",
                    "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                ),
                (
                    "EPS guidance",
                    "EPS guidance was reaffirmed at $3.10 to $3.20 for FY 2025 as execution improved through the quarter.",
                ),
                (
                    "FCF target",
                    "FCF target improved to $370 million to $390 million for FY 2025 as working capital normalized.",
                ),
            ]
            guidance_rows = []
            for idx, (metric, sentence) in enumerate(guidance_texts, start=1):
                guidance_rows.append(
                    {
                        "quarter": pd.Timestamp("2025-12-31"),
                        "note_id": f"guidance-{idx}",
                        "category": "Guidance / outlook",
                        "claim": sentence,
                        "note": sentence,
                        "metric_ref": metric,
                        "score": 95.0 - idx,
                        "doc_type": "earnings_release",
                        "doc": f"release_{idx}.txt",
                        "evidence_snippet": sentence,
                        "evidence_json": json.dumps(
                            [
                                {
                                    "doc_path": f"release_{idx}.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": sentence,
                                }
                            ]
                        ),
                    }
                )

            quarter_notes = pd.DataFrame(guidance_rows)
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            guidance_rows_visible = [
                rr
                for rr in range(1, ws.max_row + 1)
                if ws.cell(row=rr, column=2).value == "Guidance / outlook" and str(ws.cell(row=rr, column=3).value or "").strip()
            ]
            assert len(guidance_rows_visible) >= 3


def test_pbi_promise_tracker_filters_malformed_management_target_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_tracker_quality.xlsx")
            promises = pd.DataFrame(
                {
                    "promise_id": ["good-1", "bad-1"],
                    "quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31")],
                    "created_quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31")],
                    "last_seen_quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31")],
                    "first_seen_evidence_quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31")],
                    "last_seen_evidence_quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31")],
                    "metric": ["Revenue guidance", "Management target"],
                    "metric_display": ["Revenue guidance", "Management target"],
                    "promise_text": [
                        "FY 2025 Revenue guidance $1.90bn-$1.95bn.",
                        "Bowes provided the following Management target",
                    ],
                    "text_full": [
                        "FY 2025 Revenue guidance $1.90bn-$1.95bn.",
                        "Bowes provided the following Management target",
                    ],
                    "text_snippet": [
                        "FY 2025 Revenue guidance $1.90bn-$1.95bn.",
                        "Bowes provided the following Management target",
                    ],
                    "target": ["$1.90bn-$1.95bn", ""],
                    "target_display": ["$1.90bn-$1.95bn", ""],
                    "target_time": [pd.Timestamp("2025-12-31"), pd.NaT],
                    "target_period_norm": ["FY2025", ""],
                    "promise_type": ["guidance_range", "operational"],
                    "theme_key": ["rev|fy2025", "bad|fy2025"],
                    "source": [{"source_type": "guidance_snapshot"}, {"source_type": "presentation"}],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promises": promises})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_tracker_ui_v2()
            ws = ctx.wb["Promise_Tracker_UI"]

            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 8)]
            assert all("Bowes provided the following Management target" not in val for val in visible_values)
            assert all(val != "Management target" for val in visible_values)


def test_pbi_promise_progress_filters_malformed_scaffolding_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_quality.xlsx")
            hist = _make_hist().copy()
            hist["revenue"] = [100_000_000.0, 110_000_000.0, 120_000_000.0, 130_000_000.0]
            progress = pd.DataFrame(
                {
                    "promise_id": ["good-1", "bad-1"],
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "status": ["resolved_fail", "at_risk"],
                    "metric_ref": ["Revenue guidance", "Management target"],
                    "target": ["$500.0m-$520.0m", ""],
                    "latest": ["not yet measurable", "not yet measurable"],
                    "rationale": [
                        "FY 2025 revenue guidance was $500 million to $520 million.",
                        "would be incremental to Management target",
                    ],
                    "promise_type": ["guidance_range", "operational"],
                    "guidance_type": ["period", "text"],
                    "target_period_norm": ["FY2025", "UNK"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "FY 2025 revenue guidance was $500 million to $520 million."}),
                        json.dumps({"doc_type": "presentation", "snippet": "would be incremental to Management target"}),
                    ],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST", hist=hist)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 8)]
            assert any("Revenue guidance" in val for val in visible_values)
            assert all("would be incremental to Management target" not in val for val in visible_values)
            assert all(val != "Management target" for val in visible_values)


def test_pbi_promise_progress_keeps_text_latest_for_clean_milestone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_milestone.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["m-1"],
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "status": ["achieved"],
                    "metric_ref": ["Strategic milestone"],
                    "target": ["York online by Q4 2025"],
                    "latest": ["fully operational"],
                    "rationale": ["York became fully operational in Q4 2025."],
                    "promise_type": ["milestone"],
                    "guidance_type": ["text"],
                    "target_period_norm": ["Q42025"],
                    "source_evidence_json": [json.dumps({"doc_type": "earnings_release", "snippet": "York became fully operational in Q4 2025."})],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 8)]
            assert "Strategic milestone" in visible_values
            assert any("fully operational" in val.lower() for val in visible_values)


def test_pbi_promise_progress_drops_pending_guidance_without_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_pending_blank_target.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["g-1"],
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "status": ["pending"],
                    "metric_ref": ["Revenue guidance"],
                    "target": [""],
                    "latest": ["not yet measurable"],
                    "rationale": ["Future revenue and profitability assumptions."],
                    "promise_type": ["guidance_range"],
                    "guidance_type": ["period"],
                    "target_period_norm": ["FY2026"],
                    "source_evidence_json": [json.dumps({"doc_type": "presentation", "snippet": "Future revenue and profitability assumptions."})],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 8)]
            assert all("Revenue guidance" not in val for val in visible_values)


def test_gpre_quarter_notes_ui_suppresses_context_light_bullets_and_generic_ramping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_quarter_notes_context_cleanup.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")] * 4,
                    "note_id": ["good-1", "bad-1", "bad-2", "bad-3"],
                    "category": ["Results / drivers", "Programs / initiatives", "Programs / initiatives", "Programs / initiatives"],
                    "claim": [
                        "Risk management supports margins and cash flow.",
                        "? Advantage Nebraska will reduce the carbon-intensity of Green Plains' biofuel; targeted in 2H25 with decarb gallons.",
                        "Omaha IA Cedar Rapids PA ? ~940K tons on Summit Carbon Solutions.",
                        "Online and ramping up capture volumes",
                    ],
                    "note": [
                        "Risk management supports margins and cash flow.",
                        "? Advantage Nebraska will reduce the carbon-intensity of Green Plains' biofuel; targeted in 2H25 with decarb gallons.",
                        "Omaha IA Cedar Rapids PA ? ~940K tons on Summit Carbon Solutions.",
                        "Online and ramping up capture volumes",
                    ],
                    "metric_ref": ["Risk management", "45Z monetization / EBITDA", "Management target", "Strategic milestone"],
                    "score": [95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["earnings_release", "presentation", "presentation", "presentation"],
                    "doc": ["release_q2.txt", "slides_q2.txt", "slides_q2.txt", "slides_q2.txt"],
                    "evidence_snippet": [
                        "Risk management supports margins and cash flow.",
                        "? Advantage Nebraska will reduce the carbon-intensity of Green Plains' biofuel; targeted in 2H25 with decarb gallons.",
                        "Omaha IA Cedar Rapids PA ? ~940K tons on Summit Carbon Solutions.",
                        "Online and ramping up capture volumes",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q2.txt", "doc_type": "earnings_release", "snippet": "Risk management supports margins and cash flow."}]),
                        json.dumps([{"doc_path": "slides_q2.txt", "doc_type": "presentation", "snippet": "? Advantage Nebraska will reduce the carbon-intensity of Green Plains' biofuel; targeted in 2H25 with decarb gallons."}]),
                        json.dumps([{"doc_path": "slides_q2.txt", "doc_type": "presentation", "snippet": "Omaha IA Cedar Rapids PA ? ~940K tons on Summit Carbon Solutions."}]),
                        json.dumps([{"doc_path": "slides_q2.txt", "doc_type": "presentation", "snippet": "Online and ramping up capture volumes"}]),
                    ],
                }
            )
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]

            assert any("Risk management supports margins and cash flow" in note for note in visible_notes)
            assert not any("Advantage Nebraska will reduce the carbon-intensity" in note for note in visible_notes)
            assert not any("Omaha IA Cedar Rapids PA" in note for note in visible_notes)
            assert not any(note.strip().endswith("Online and ramping up capture volumes") for note in visible_notes)


def test_gpre_promise_progress_suppresses_generic_ramping_seed_without_entity_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_progress_no_generic_ramping.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")],
                    "note_id": ["ramping-generic"],
                    "category": ["Programs / initiatives"],
                    "claim": ["Plant online and ramping up capture volumes."],
                    "note": ["Plant online and ramping up capture volumes."],
                    "metric_ref": ["Strategic milestone"],
                    "score": [90.0],
                    "doc_type": ["presentation"],
                    "doc": ["slides_q2.txt"],
                    "evidence_snippet": ["Plant online and ramping up capture volumes."],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "slides_q2.txt",
                                    "doc_type": "presentation",
                                    "snippet": "Plant online and ramping up capture volumes.",
                                }
                            ]
                        )
                    ],
                }
            )
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [
                str(ws.cell(row=rr, column=cc).value or "")
                for rr in range(1, ws.max_row + 1)
                for cc in range(1, 7)
            ]
            assert not any("Plant online and ramping up capture volumes" in val for val in visible_values)
            assert not any(val == "Online and ramping" for val in visible_values)


def test_gpre_promise_tracker_and_progress_drop_fragment_metric_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_fragment_metric_drop.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["bad-frag"],
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "status": ["in_progress"],
                    "metric_ref": ["which time the Partnership Strategic milestone"],
                    "target": ["1"],
                    "latest": ["Expected in 2024"],
                    "rationale": ["The Merger is expected to close on January 9, 2024, at which time the Partnership will commence the process."],
                    "promise_type": ["operational"],
                    "guidance_type": ["text"],
                    "target_period_norm": ["FY2024"],
                    "source_evidence_json": [
                        json.dumps(
                            {
                                "doc_type": "earnings_release",
                                "snippet": "The Merger is expected to close on January 9, 2024, at which time the Partnership will commence the process.",
                            }
                        )
                    ],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]
            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 7)]

            assert not any("which time the Partnership" in val for val in visible_values)

def test_gpre_tracker_and_progress_restore_clean_high_signal_rows_from_quarter_notes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_tracker_progress_recall.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 4,
                    "note_id": ["n-45z-q4", "n-45z-2026", "n-obion", "n-cc"],
                    "category": ["Tone / expectations", "Tone / expectations", "Debt / liquidity / covenants", "Programs / initiatives"],
                    "claim": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value net of discounts and other costs for the fourth quarter of 2025.",
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Central City and Wood River online and ramping.",
                    ],
                    "note": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value net of discounts and other costs for the fourth quarter of 2025.",
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Central City and Wood River online and ramping.",
                    ],
                    "metric_ref": ["Tone | Corporate", "Tone | Corporate", "Debt reduction", "Strategic milestone"],
                    "score": [96.0, 95.0, 94.0, 93.0],
                    "doc_type": ["earnings_release", "earnings_release", "earnings_release", "presentation"],
                    "doc": ["release_q3.txt", "release_q3.txt", "release_q3.txt", "slides_q3.txt"],
                    "evidence_snippet": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value net of discounts and other costs for the fourth quarter of 2025.",
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Central City and Wood River online and ramping.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "On track for $15 - $25 million of 45Z production tax credit monetization value net of discounts and other costs for the fourth quarter of 2025."}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "All eight operating ethanol plants expected to qualify for production tax credits in 2026."}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt."}]),
                        json.dumps([{"doc_path": "slides_q3.txt", "doc_type": "presentation", "snippet": "Central City and Wood River online and ramping."}]),
                    ],
                }
            )
            inputs = _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes)
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ctx.callbacks.write_promise_tracker_ui_v2()
            ctx.callbacks.write_promise_progress_ui_v2()

            qn_ws = ctx.wb["Quarter_Notes_UI"]
            tracker_ws = ctx.wb["Promise_Tracker_UI"]
            progress_ws = ctx.wb["Promise_Progress_UI"]
            qn_values = [
                str(qn_ws.cell(row=rr, column=cc).value or "")
                for rr in range(1, qn_ws.max_row + 1)
                for cc in range(1, 6)
            ]
            tracker_values = [
                str(tracker_ws.cell(row=rr, column=cc).value or "")
                for rr in range(1, tracker_ws.max_row + 1)
                for cc in range(1, 6)
            ]
            progress_values = [str(progress_ws.cell(row=rr, column=cc).value or "") for rr in range(1, progress_ws.max_row + 1) for cc in range(1, 7)]

            assert any("45Z" in val for val in tracker_values)
            assert not any("Obion" in val or "Debt reduction" in val for val in tracker_values)
            assert any("Obion" in val or "Debt reduction" in val for val in qn_values)
            assert any("45Z" in val for val in progress_values)


def test_promise_progress_groups_same_subject_wording_variants_into_one_visible_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_progress_same_subject_grouping.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "note_id": ["obion-1", "obion-2"],
                    "category": ["Debt / liquidity / covenants", "Debt / liquidity / covenants"],
                    "claim": [
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Obion sale proceeds fully repaid $130.7 million junior mezzanine debt.",
                    ],
                    "note": [
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Obion sale proceeds fully repaid $130.7 million junior mezzanine debt.",
                    ],
                    "metric_ref": ["Debt reduction", "Debt reduction"],
                    "score": [95.0, 92.0],
                    "doc_type": ["earnings_release", "presentation"],
                    "doc": ["release_q3.txt", "slides_q3.txt"],
                    "evidence_snippet": [
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Obion sale proceeds fully repaid $130.7 million junior mezzanine debt.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt."}]),
                        json.dumps([{"doc_path": "slides_q3.txt", "doc_type": "presentation", "snippet": "Obion sale proceeds fully repaid $130.7 million junior mezzanine debt."}]),
                    ],
                }
            )
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ctx.callbacks.write_promise_tracker_ui_v2()
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 7)]
            assert sum(1 for val in visible_values if "Obion" in val or "Debt reduction milestone" in val) <= 1
            assert not any("would be incremental to" in val.lower() for val in visible_values)
            assert not any("provided the following management target" in val.lower() for val in visible_values)


def test_gpre_45z_qualification_progress_does_not_use_monetization_range_as_latest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_progress_qualification_vs_monetization.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["q-45z-2026", "q-45z-q4"],
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "status": ["on_track", "on_track"],
                    "metric_ref": ["45Z plant qualification readiness", "45Z monetization / EBITDA"],
                    "target": ["All eight operating plants qualify in 2026", "$15.0m-$25.0m"],
                    "latest": ["not yet measurable", "not yet measurable"],
                    "rationale": [
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                    ],
                    "promise_type": ["operational", "guidance_range"],
                    "guidance_type": ["period", "period"],
                    "target_period_norm": ["FY2026", "Q42025"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "All eight operating ethanol plants expected to qualify for production tax credits in 2026."}),
                        json.dumps({"doc_type": "earnings_release", "snippet": "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025."}),
                    ],
                }
            )
            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            qualification_row = None
            for rr in range(1, ws.max_row + 1):
                if ws.cell(row=rr, column=2).value == "45Z plant qualification readiness":
                    qualification_row = rr
                    break
            assert qualification_row is not None
            latest_val = str(ws.cell(row=qualification_row, column=4).value or "")
            assert "$15.0m-$25.0m" not in latest_val
            assert "$15m-$25m" not in latest_val


def test_quarter_notes_ui_event_layer_dedupes_semantically_overlapping_notes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "quarter_notes_event_layer.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 2,
                    "note_id": ["rm-1", "rm-2"],
                    "category": ["Results / drivers", "Results / drivers"],
                    "claim": [
                        "Risk management supports margins and cash flow.",
                        "Risk management continued to support margins and cash flow through the quarter.",
                    ],
                    "note": [
                        "Risk management supports margins and cash flow.",
                        "Risk management continued to support margins and cash flow through the quarter.",
                    ],
                    "metric_ref": ["Risk management", "Risk management"],
                    "score": [88.0, 84.0],
                    "doc_type": ["earnings_release", "transcript"],
                    "doc": ["release_q3.txt", "transcript_q3.txt"],
                    "evidence_snippet": [
                        "Risk management supports margins and cash flow.",
                        "Risk management continued to support margins and cash flow through the quarter.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Risk management supports margins and cash flow."}]),
                        json.dumps([{"doc_path": "transcript_q3.txt", "doc_type": "transcript", "snippet": "Risk management continued to support margins and cash flow through the quarter."}]),
                    ],
                }
            )
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            rows = [
                (
                    str(ws.cell(row=rr, column=2).value or ""),
                    str(ws.cell(row=rr, column=3).value or ""),
                    str(ws.cell(row=rr, column=4).value or ""),
                )
                for rr in range(1, ws.max_row + 1)
                if str(ws.cell(row=rr, column=4).value or "") == "Risk management"
            ]
            assert len(rows) == 1
            assert "Risk management" in rows[0][1]


def test_pbi_recent_quarter_notes_keep_some_non_guidance_when_strong_candidates_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_diversity.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 3,
                    "note_id": ["guidance-1", "driver-1", "junk-1"],
                    "category": ["Guidance / outlook", "Results / drivers", "Other / footnotes"],
                    "claim": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                        "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                        "map permit list county parcel latitude longitude $12 million",
                    ],
                    "note": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                        "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                        "map permit list county parcel latitude longitude $12 million",
                    ],
                    "metric_ref": ["Adjusted EBIT guidance", "PB Bank liquidity release", "Other"],
                    "score": [95.0, 93.0, 96.0],
                    "doc_type": ["earnings_release", "transcript", "ocr"],
                    "doc": ["release_q4.txt", "transcript_q4.txt", "ocr_junk.txt"],
                    "evidence_snippet": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech.",
                        "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix.",
                        "map permit list county parcel latitude longitude $12 million",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": doc, "doc_type": doc_type, "snippet": s}])
                        for doc, doc_type, s in [
                            ("release_q4.txt", "earnings_release", "Adjusted EBIT guidance increased to $450 million to $465 million due to pricing and mix improvements in SendTech."),
                            ("transcript_q4.txt", "transcript", "PB Bank liquidity improved by $120 million as trapped capital was released and SendTech margins improved on pricing and mix."),
                            ("ocr_junk.txt", "ocr", "map permit list county parcel latitude longitude $12 million"),
                        ]
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_metrics = [str(ws.cell(row=rr, column=4).value or "") for rr in range(1, ws.max_row + 1)]
            assert any(metric == "PB Bank liquidity release" for metric in visible_metrics)
            assert any(metric == "Adjusted EBIT guidance" for metric in visible_metrics)


def test_pbi_recent_quarter_notes_can_recall_result_evidence_without_tracker_origination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_result_evidence_recall.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "note_id": ["guidance-1", "buyback-1"],
                    "category": ["Guidance / outlook", "Cash / liquidity / leverage"],
                    "claim": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025.",
                        "Pitney Bowes increased share repurchase authorization and maintained significant remaining capacity in Q4 2025.",
                    ],
                    "note": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025.",
                        "Pitney Bowes increased share repurchase authorization and maintained significant remaining capacity in Q4 2025.",
                    ],
                    "metric_ref": ["Adjusted EBIT guidance", "Capital allocation"],
                    "score": [95.0, 90.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["release_q4.txt", "release_q4.txt"],
                    "evidence_snippet": [
                        "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025.",
                        "Pitney Bowes increased share repurchase authorization and maintained significant remaining capacity in Q4 2025.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025."}]),
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "Pitney Bowes increased share repurchase authorization and maintained significant remaining capacity in Q4 2025."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_metrics = [str(ws.cell(row=rr, column=4).value or "") for rr in range(1, ws.max_row + 1)]
            assert "Capital allocation / buyback" in visible_metrics


def test_pbi_recent_quarter_notes_can_recall_result_evidence_from_promises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_result_evidence_from_promises.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "note_id": ["guidance-1"],
                    "category": ["Guidance / outlook"],
                    "claim": ["Adjusted EBIT guidance increased to $410 million to $460 million for FY 2026."],
                    "note": ["Adjusted EBIT guidance increased to $410 million to $460 million for FY 2026."],
                    "metric_ref": ["Adjusted EBIT guidance"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q4.txt"],
                    "evidence_snippet": ["Adjusted EBIT guidance increased to $410 million to $460 million for FY 2026."],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "Adjusted EBIT guidance increased to $410 million to $460 million for FY 2026."}])
                    ],
                }
            )
            promises = pd.DataFrame(
                {
                    "promise_id": ["buyback-1"],
                    "created_quarter": [pd.Timestamp("2025-12-31")],
                    "metric": ["capital_allocation"],
                    "statement": [
                        "The company repurchased 12.6 million shares for $127 million in Q4 2025 and increased remaining authorization capacity."
                    ],
                    "promise_text": [
                        "The company repurchased 12.6 million shares for $127 million in Q4 2025 and increased remaining authorization capacity."
                    ],
                    "evidence_snippet": [
                        "The company repurchased 12.6 million shares for $127 million in Q4 2025 and increased remaining authorization capacity."
                    ],
                    "promise_type": ["operational"],
                    "confidence": ["high"],
                    "doc": ["Q4 2025 Earnings Press Release.pdf"],
                    "source_evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "Q4 2025 Earnings Press Release.pdf",
                                    "doc_type": "earnings_release",
                                    "source_type": "earnings_release",
                                    "snippet": "The company repurchased 12.6 million shares for $127 million in Q4 2025 and increased remaining authorization capacity.",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, promises=promises))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_metrics = [str(ws.cell(row=rr, column=4).value or "") for rr in range(1, ws.max_row + 1)]
            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert "Capital allocation / buyback" in visible_metrics
            assert any("Repurchased 12.6m shares for $127.0m" in note for note in visible_notes)


def test_pbi_quarter_notes_preserve_revolver_from_to_detail_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_revolver_detail.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "note_id": ["revolver-1"],
                    "category": ["Debt / liquidity / covenants"],
                    "claim": ["Revolver availability changed."],
                    "note": ["Revolver availability changed."],
                    "metric_ref": ["revolver_availability_change"],
                    "score": [92.0],
                    "doc_type": ["filing_text"],
                    "doc": ["pbi-20250930.htm"],
                    "evidence_snippet": [
                        "In the third quarter of 2025, the revolving credit facility was increased from $265 million to $400 million."
                    ],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "pbi-20250930.htm",
                                    "doc_type": "filing_text",
                                    "snippet": "In the third quarter of 2025, the revolving credit facility was increased from $265 million to $400 million.",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert any("Revolver availability increased from $265.0m to $400.0m" in note for note in visible_notes)


def test_pbi_quarter_notes_infer_revolver_from_to_from_current_and_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_revolver_delta_detail.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "note_id": ["revolver-delta-1"],
                    "category": ["Debt / liquidity / covenants"],
                    "claim": ["Revolver availability changed."],
                    "note": ["Revolver availability changed."],
                    "metric_ref": ["revolver_availability_change"],
                    "score": [92.0],
                    "doc_type": ["revolver"],
                    "doc": ["revolver.csv"],
                    "comment_full_text": ["Revolver availability moved to $400.0m at 2025-09-30 (delta $135.0m)."],
                    "evidence_snippet": ["Revolver availability moved to $400.0m at 2025-09-30 (delta $135.0m)."],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "revolver.csv",
                                    "doc_type": "revolver",
                                    "snippet": "Revolver availability moved to $400.0m at 2025-09-30 (delta $135.0m).",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert any("Revolver availability increased from $265.0m to $400.0m" in note for note in visible_notes)


def test_pbi_quarter_notes_prefer_metric_aware_guidance_summary_over_updated_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_metric_aware_guidance_summary.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")],
                    "note_id": ["fcf-guidance-1"],
                    "category": ["Guidance / outlook"],
                    "claim": ["Updated target $330m-$370m"],
                    "note": ["Updated target $330m-$370m"],
                    "metric_ref": ["FCF target"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q2.txt"],
                    "evidence_snippet": ["FY 2025 free cash flow guidance updated to $330 million to $370 million."],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "release_q2.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": "FY 2025 free cash flow guidance updated to $330 million to $370 million.",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert any("FY 2025 FCF target updated to $330m-$370m." in note for note in visible_notes)
            assert not any(note == "Updated target $330m-$370m" for note in visible_notes)


def test_pbi_quarter_notes_do_not_repeat_target_wording_in_guidance_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_no_target_target.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")],
                    "note_id": ["fcf-guidance-target-1"],
                    "category": ["Guidance / outlook"],
                    "claim": ["FY 2025 free cash flow guidance of $330m-$370m."],
                    "note": ["FY 2025 free cash flow guidance of $330m-$370m."],
                    "metric_ref": ["FCF target"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q1.txt"],
                    "evidence_snippet": ["FY 2025 free cash flow guidance of $330m-$370m."],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "release_q1.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": "FY 2025 free cash flow guidance of $330m-$370m.",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert any("FY 2025 FCF target $330m-$370m." in note for note in visible_notes)
            assert not any("target target" in note.lower() for note in visible_notes)


def test_pbi_quarter_notes_prefer_richer_revolver_summary_over_generic_liquidity_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_prefer_richer_revolver.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "note_id": ["liq-generic", "liq-rich"],
                    "category": ["Debt / refi / covenants", "Debt / refi / covenants"],
                    "claim": ["Liquidity improved", "Revolver availability changed"],
                    "note": ["Liquidity improved", "Revolver availability changed"],
                    "metric_ref": ["Debt reduction", "revolver_availability_change"],
                    "score": [93.0, 89.0],
                    "doc_type": ["earnings_release", "filing_text"],
                    "doc": ["release_q4.txt", "pbi-20250930.htm"],
                    "evidence_snippet": [
                        "Debt refinancing improved liquidity.",
                        "Revolver availability moved to $400.0m at 2025-09-30 (delta $135.0m).",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "Debt refinancing improved liquidity."}]),
                        json.dumps([{"doc_path": "pbi-20250930.htm", "doc_type": "filing_text", "snippet": "Revolver availability moved to $400.0m at 2025-09-30 (delta $135.0m)."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]

            assert any("Revolver availability increased from $265.0m to $400.0m" in note for note in visible_notes)
            assert not any(note == "Liquidity improved" for note in visible_notes)


def test_pbi_recent_quarter_notes_keep_multiple_non_guidance_rows_when_fact_rich_candidates_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_multi_block_diversity.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [
                        pd.Timestamp("2025-12-31"),
                        pd.Timestamp("2025-12-31"),
                        pd.Timestamp("2025-09-30"),
                        pd.Timestamp("2025-09-30"),
                    ],
                    "note_id": ["g-1", "rev-1", "g-2", "rev-2"],
                    "category": [
                        "Guidance / outlook",
                        "Debt / refi / covenants",
                        "Guidance / outlook",
                        "Debt / refi / covenants",
                    ],
                    "claim": [
                        "FY 2026 Revenue guidance target $1.76bn-$1.86bn.",
                        "Revolver availability changed.",
                        "FY 2025 Revenue guidance target $1.90bn-$1.95bn.",
                        "Revolver availability changed.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance target $1.76bn-$1.86bn.",
                        "Revolver availability changed.",
                        "FY 2025 Revenue guidance target $1.90bn-$1.95bn.",
                        "Revolver availability changed.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "revolver_availability_change",
                        "Revenue guidance",
                        "revolver_availability_change",
                    ],
                    "score": [95.0, 90.0, 95.0, 90.0],
                    "doc_type": ["earnings_release", "filing_text", "earnings_release", "filing_text"],
                    "doc": ["release_q4.txt", "pbi-20251231.htm", "release_q3.txt", "pbi-20250930.htm"],
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance target $1.76bn-$1.86bn.",
                        "The revolving credit facility was increased from $265 million to $400 million.",
                        "FY 2025 Revenue guidance target $1.90bn-$1.95bn.",
                        "The revolving credit facility was increased from $265 million to $400 million.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "FY 2026 Revenue guidance target $1.76bn-$1.86bn."}]),
                        json.dumps([{"doc_path": "pbi-20251231.htm", "doc_type": "filing_text", "snippet": "The revolving credit facility was increased from $265 million to $400 million."}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "FY 2025 Revenue guidance target $1.90bn-$1.95bn."}]),
                        json.dumps([{"doc_path": "pbi-20250930.htm", "doc_type": "filing_text", "snippet": "The revolving credit facility was increased from $265 million to $400 million."}]),
                    ],
                }
            )
            promises = pd.DataFrame(
                {
                    "promise_id": ["p-q4", "p-q3"],
                    "created_quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")],
                    "metric": ["capital_allocation", "capital_allocation"],
                    "statement": [
                        "The company repurchased 12.6 million shares for $127 million in Q4 2025, reduced principal debt by $114 million and increased remaining authorization capacity to $359 million.",
                        "Through last Friday, the company repurchased 25.9 million shares at a total cost of $281.2 million and increased remaining authorization capacity.",
                    ],
                    "promise_text": [
                        "The company repurchased 12.6 million shares for $127 million in Q4 2025, reduced principal debt by $114 million and increased remaining authorization capacity to $359 million.",
                        "Through last Friday, the company repurchased 25.9 million shares at a total cost of $281.2 million and increased remaining authorization capacity.",
                    ],
                    "evidence_snippet": [
                        "The company repurchased 12.6 million shares for $127 million in Q4 2025, reduced principal debt by $114 million and increased remaining authorization capacity to $359 million.",
                        "Through last Friday, the company repurchased 25.9 million shares at a total cost of $281.2 million and increased remaining authorization capacity.",
                    ],
                    "promise_type": ["operational", "operational"],
                    "confidence": ["high", "high"],
                    "doc": ["release_q4.txt", "release_q3.txt"],
                    "source_evidence_json": [
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "source_type": "earnings_release", "snippet": "The company repurchased 12.6 million shares for $127 million in Q4 2025, reduced principal debt by $114 million and increased remaining authorization capacity to $359 million."}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "source_type": "earnings_release", "snippet": "Through last Friday, the company repurchased 25.9 million shares at a total cost of $281.2 million and increased remaining authorization capacity."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, promises=promises))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            def _quarter_block(qtxt: str) -> list[tuple[str, str]]:
                out: list[tuple[str, str]] = []
                capture = False
                for rr in range(1, ws.max_row + 1):
                    marker = str(ws.cell(row=rr, column=1).value or "")
                    if marker == qtxt:
                        capture = True
                        continue
                    if capture and marker:
                        break
                    if capture:
                        note = str(ws.cell(row=rr, column=3).value or "")
                        metric = str(ws.cell(row=rr, column=4).value or "")
                        if note or metric:
                            out.append((note, metric))
                return out

            q4_rows = _quarter_block("2025-12-31")
            q3_rows = _quarter_block("2025-09-30")
            assert sum(1 for _, metric in q4_rows if metric not in {"Revenue guidance", "Adjusted EBIT guidance", "EPS guidance", "FCF target", "Cost savings target"}) >= 2
            assert sum(1 for _, metric in q3_rows if metric not in {"Revenue guidance", "Adjusted EBIT guidance", "EPS guidance", "FCF target", "Cost savings target"}) >= 2
            assert any("Repurchased 12.6m shares for $127.0m" in note for note, _ in q4_rows)
            assert any("Revolver availability increased from $265.0m to $400.0m" in note for note, _ in q4_rows)
            assert any("Revolver availability increased from $265.0m to $400.0m" in note for note, _ in q3_rows)


def test_pbi_quarter_notes_quantify_fcf_summary_from_adj_metrics_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_contextual_fcf.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "note_id": ["fcf-1"],
                    "category": ["Cash flow / FCF / capex"],
                    "claim": ["Free cash flow improved."],
                    "note": ["Free cash flow improved."],
                    "metric_ref": ["FCF improvement"],
                    "score": [92.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q4.txt"],
                    "evidence_snippet": ["Free cash flow improved."],
                    "evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "release_q4.txt",
                                    "doc_type": "earnings_release",
                                    "snippet": "Free cash flow improved.",
                                }
                            ]
                        )
                    ],
                }
            )
            adj_metrics = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-12-31"), pd.Timestamp("2025-12-31")],
                    "adj_fcf": [131_800_000.0, 221_700_000.0],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "adj_metrics": adj_metrics})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert any("Free cash flow improved to $221.7m, up $89.9m YoY." in note for note in visible_notes)


def test_pbi_recent_quarter_notes_can_recall_ceo_letter_buyback_result_from_html_promise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_ceo_letter_buyback_result.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "note_id": ["guidance-1"],
                    "category": ["Guidance / outlook"],
                    "claim": ["Revenue guidance updated to $1.90bn-$1.95bn for FY 2025."],
                    "note": ["Revenue guidance updated to $1.90bn-$1.95bn for FY 2025."],
                    "metric_ref": ["Revenue guidance"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q3.txt"],
                    "evidence_snippet": ["Revenue guidance updated to $1.90bn-$1.95bn for FY 2025."],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Revenue guidance updated to $1.90bn-$1.95bn for FY 2025."}])
                    ],
                }
            )
            promises = pd.DataFrame(
                {
                    "promise_id": ["buyback-1"],
                    "created_quarter": [pd.Timestamp("2025-09-30")],
                    "metric": ["capital_allocation"],
                    "statement": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year."
                    ],
                    "promise_text": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year."
                    ],
                    "evidence_snippet": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year."
                    ],
                    "promise_type": ["operational"],
                    "confidence": ["high"],
                    "doc": ["doc_000162828025047122_q32025earningsceoletter.htm"],
                    "source_evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "doc_000162828025047122_q32025earningsceoletter.htm",
                                    "doc_type": "html",
                                    "source_type": "html",
                                    "snippet": "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year.",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, promises=promises))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_metrics = [str(ws.cell(row=rr, column=4).value or "") for rr in range(1, ws.max_row + 1)]
            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert "Capital allocation / buyback" in visible_metrics
            assert any("Repurchased 25.9m shares for $281.2m" in note for note in visible_notes)


def test_pbi_quarter_notes_preserve_buyback_since_start_detail_from_evidence_snippet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_buyback_since_start.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "note_id": ["bb-1"],
                    "category": ["Cash / liquidity / leverage"],
                    "claim": ["Share repurchase authorization and remaining capacity updated."],
                    "note": ["Share repurchase authorization and remaining capacity updated."],
                    "metric_ref": ["Capital allocation / buyback"],
                    "score": [92.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q3.txt"],
                    "evidence_snippet": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million "
                        "since starting the program earlier this year. In addition to increasing our share repurchase "
                        "authorization to $500 million, we increased our quarterly dividend from $0.08 to $0.09 per share."
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]
            assert any(
                "Repurchased 25.9m shares for $281.2m" in note
                and "since starting the program earlier this year" in note
                for note in visible_notes
            )
            assert any("Repurchase authorization increased to $500.0m" in note for note in visible_notes)


def test_pbi_quarter_notes_keep_quantified_fcf_actual_when_block_has_four_guidance_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_keep_fcf_actual.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 6,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "bb-1", "debt-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Cash / liquidity / leverage",
                        "Cash / liquidity / leverage",
                    ],
                    "claim": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                        "Liquidity improved.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                        "Liquidity improved.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "Capital allocation / buyback",
                        "Debt reduction",
                    ],
                    "score": [96.0, 95.0, 94.0, 97.0, 93.0, 92.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q4.txt"] * 6,
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Repurchased 12.6 million shares for $127.0 million in the fourth quarter.",
                        "Reduced principal debt by $114.1 million in the fourth quarter.",
                    ],
                }
            )
            adj_metrics = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-12-31"), pd.Timestamp("2025-12-31")],
                    "adj_fcf": [131_800_000.0, 221_700_000.0],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "adj_metrics": adj_metrics})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            def _quarter_block(qtxt: str) -> list[str]:
                out: list[str] = []
                capture = False
                for rr in range(1, ws.max_row + 1):
                    marker = str(ws.cell(row=rr, column=1).value or "")
                    if marker == qtxt:
                        capture = True
                        continue
                    if capture and marker:
                        break
                    if capture:
                        note = str(ws.cell(row=rr, column=3).value or "")
                        if note:
                            out.append(note)
                return out

            q4_rows = _quarter_block("2025-12-31")
            q4_note_rows = [x for x in q4_rows if x != "Note"]
            guidance_like = [x for x in q4_note_rows if re.search(r"\b(guidance|target)\b", x, re.I)]

            assert any("Free cash flow improved to $221.7m, up $89.9m YoY." in note for note in q4_note_rows)
            assert any("Reduced principal debt by $114.1m in Q4." in note for note in q4_note_rows)
            assert len(guidance_like) == 4
            assert all(re.search(r"\b(guidance|target)\b", note, re.I) for note in q4_note_rows[:4])


def test_pbi_quarter_notes_replace_generic_cap_alloc_with_richer_promise_rescue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_replace_generic_buyback.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 5,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "bb-generic"],
                    "category": ["Guidance / outlook"] * 4 + ["Cash / liquidity / leverage"],
                    "claim": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "Capital allocation / buyback",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0, 90.0],
                    "doc_type": ["earnings_release"] * 5,
                    "doc": ["release_q3.txt"] * 5,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                }
            )
            promises = pd.DataFrame(
                {
                    "promise_id": ["buyback-1"],
                    "created_quarter": [pd.Timestamp("2025-09-30")],
                    "metric": ["capital_allocation"],
                    "statement": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year."
                    ],
                    "promise_text": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year."
                    ],
                    "evidence_snippet": [
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year."
                    ],
                    "promise_type": ["operational"],
                    "confidence": ["high"],
                    "doc": ["doc_000162828025047122_q32025earningsceoletter.htm"],
                    "source_evidence_json": [
                        json.dumps(
                            [
                                {
                                    "doc_path": "doc_000162828025047122_q32025earningsceoletter.htm",
                                    "doc_type": "html",
                                    "source_type": "html",
                                    "snippet": "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year.",
                                }
                            ]
                        )
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, promises=promises))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            visible_notes = [str(ws.cell(row=rr, column=3).value or "") for rr in range(1, ws.max_row + 1)]

            assert any("Repurchased 25.9m shares for $281.2m" in note for note in visible_notes)
            assert not any("Share repurchase authorization and remaining capacity updated." == note for note in visible_notes)


def test_pbi_quarter_notes_can_build_q3_buyback_execution_from_sec_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q3_buyback_table.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828025047360_pbi-20250930.htm").write_text(
                "Item 2. Unregistered Sales of Equity Securities and Use of Proceeds. "
                "On February 11, 2025, our Board of Directors authorized a new $150 million share repurchase program. "
                "In July 2025, the Board authorized an increase in the program to $400 million, and in October 2025, "
                "the Board authorized an additional increase in the program to $500 million. "
                "We increased our quarterly dividend from $0.08 to $0.09 per share. "
                "The following table provides information about common stock purchases during the three months ended September 30, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "July 2025 4,174,838 $ 11.60 4,174,838 $261,280 "
                "August 2025 7,722,528 $ 11.27 7,722,528 $174,267 "
                "September 2025 2,220,772 $ 11.73 2,220,772 $148,226 "
                "14,118,138 $ 11.44 14,118,138 "
                "As of September 30, 2025, $148.226 million in capacity remained under the authorization.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 4,
                    "note_id": ["g-1", "g-2", "g-3", "g-4"],
                    "category": ["Guidance / outlook"] * 4,
                    "claim": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["earnings_release"] * 4,
                    "doc": ["release_q3.txt"] * 4,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert any(
                "Repurchased 14.1m shares for $161.5m with an average price of $11.44/share in Q3." in note
                for note in q3_rows
            )
            assert any(
                "Repurchase authorization increased to $500.0m, up from $400.0m." in note
                for note in q3_rows
            )
            assert any("Remaining share repurchase capacity was $148.2m at quarter-end." in note for note in q3_rows)
            assert any("Quarterly dividend increased to $0.09/share from $0.08/share." in note for note in q3_rows)


def test_pbi_quarter_notes_visible_q3_prefers_sec_table_buyback_over_cumulative_since_start_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q3_visible_prefers_sec_table_buyback.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828025047360_pbi-20250930.htm").write_text(
                "Item 2. Unregistered Sales of Equity Securities and Use of Proceeds. "
                "The following table provides information about common stock purchases during the three months ended September 30, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "July 2025 4,174,838 $ 11.60 4,174,838 $261,280 "
                "August 2025 7,722,528 $ 11.27 7,722,528 $174,267 "
                "September 2025 2,220,772 $ 11.73 2,220,772 $148,226 "
                "14,118,138 $ 11.44 14,118,138 "
                "As of September 30, 2025, $148.226 million in capacity remained under the authorization.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 5,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "bb-1"],
                    "category": ["Guidance / outlook"] * 4 + ["Cash / liquidity / leverage"],
                    "claim": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "Capital allocation / buyback",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0, 91.0],
                    "doc_type": ["earnings_release"] * 5,
                    "doc": ["release_q3.txt"] * 5,
                    "source_type": ["earnings_release"] * 5,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert any(
                "Repurchased 14.1m shares for $161.5m with an average price of $11.44/share in Q3." in note
                for note in q3_rows
            )
            assert not any("Repurchased 25.9m shares for $281.2m since starting the program earlier this year." in note for note in q3_rows)


def test_pbi_realistic_buyback_table_with_program_boilerplate_still_builds_quarter_safe_q3_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q3_realistic_table_with_program_boilerplate.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828025047360_pbi-20250930.htm").write_text(
                "Item 2. Unregistered Sales of Equity Securities and Use of Proceeds. "
                "On February 11, 2025, our Board of Directors authorized a new $150 million share repurchase program. "
                "In July 2025, the Board authorized an increase in the program to $400 million, and in October 2025, "
                "the Board authorized an additional increase in the program to $500 million. "
                "Subject to limitations in our New Credit Agreement, common stock repurchases may be made from time to time "
                "in open market or private transactions in such manner as may be deemed advisable from time to time. "
                "We may also repurchase shares of our common stock to manage the dilution created by shares issued under employee stock plans. "
                "The following table provides information about common stock purchases during the three months ended September 30, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "July 2025 4,174,838 $ 11.60 4,174,838 $261,280 "
                "August 2025 7,722,528 $ 11.27 7,722,528 $174,267 "
                "September 2025 2,220,772 $ 11.73 2,220,772 $148,226 "
                "14,118,138 $ 11.44 14,118,138 "
                "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 5,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "bb-1"],
                    "category": ["Guidance / outlook"] * 4 + ["Cash / liquidity / leverage"],
                    "claim": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Share repurchase authorization and remaining capacity updated.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "Capital allocation / buyback",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0, 91.0],
                    "doc_type": ["earnings_release"] * 5,
                    "doc": ["q3.txt"] * 5,
                    "source_type": ["earnings_release"] * 5,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert any(
                "Repurchased 14.1m shares for $161.5m with an average price of $11.44/share in Q3." in note
                for note in q3_rows
            )
            assert not any(
                "Repurchased 25.9m shares for $281.2m since starting the program earlier this year." in note
                for note in q3_rows
            )


def test_pbi_quarter_notes_can_parse_realistic_month_table_when_header_text_is_fragmented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q3_realistic_fragmented_table.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828025047360_pbi-20250930.htm").write_text(
                "ITEM 5. MARKET FOR THE COMPANY'S COMMON EQUITY RELATED STOCKHOLDER MATTERS AND ISSUER PURCHASES OF EQUITY SECURITIES "
                "Dividends and Share Repurchases The following table provides information about common stock purchases during the three months ended September 30, 2025. "
                "Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "July 2025 4,174,838 $ 11.60 4,174,838 $261,280 "
                "August 2025 7,722,528 $ 11.27 7,722,528 $174,267 "
                "September 2025 2,220,772 $ 11.73 2,220,772 $148,226 "
                "14,118,138 $ 11.44 14,118,138 "
                "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million since starting the program earlier this year.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "note_id": ["q3-guid"],
                    "category": ["Guidance / outlook"],
                    "claim": ["Q3 placeholder."],
                    "note": ["Q3 placeholder."],
                    "metric_ref": ["Guidance"],
                    "score": [90.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["q3.txt"],
                    "source_type": ["earnings_release"],
                    "evidence_snippet": ["Q3 placeholder."],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert any(
                "Repurchased 14.1m shares for $161.5m with an average price of $11.44/share in Q3." in note
                for note in q3_rows
            )


def test_pbi_q4_keeps_single_quarter_specific_buyback_execution_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q4_single_buyback_row.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828026008604_q42025earningspressrelea.htm").write_text(
                "During the quarter we repurchased 12.6 million shares for $127.0 million.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000162828026012345_pbi-20251231.htm").write_text(
                "The following table provides information about common stock purchases during the three months ended December 31, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "October 2025 3,203,100&#160; $ 11.30&#160; 3,203,100 $212,031 "
                "November 2025 7,926,090&#160; $ 9.52&#160; 7,926,090 $136,553 "
                "December 2025 1,484,407&#160; $ 10.05&#160; 1,484,407 $121,639 "
                "12,613,597&#160; $ 10.04&#160; 12,613,597 ",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 5,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "bb-1"],
                    "category": ["Guidance / outlook"] * 4 + ["Cash / liquidity / leverage"],
                    "claim": [
                        "FY 2026 Revenue guidance.",
                        "FY 2026 Adjusted EBIT guidance.",
                        "FY 2026 EPS guidance.",
                        "FY 2026 FCF guidance.",
                        "Share repurchases continued in Q4.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance.",
                        "FY 2026 Adjusted EBIT guidance.",
                        "FY 2026 EPS guidance.",
                        "FY 2026 FCF guidance.",
                        "Share repurchases continued in Q4.",
                    ],
                    "metric_ref": ["Revenue guidance", "Adjusted EBIT guidance", "EPS guidance", "FCF target", "Capital allocation / buyback"],
                    "score": [95.0, 94.0, 93.0, 92.0, 91.0],
                    "doc_type": ["earnings_release"] * 5,
                    "doc": ["q4.txt"] * 5,
                    "source_type": ["earnings_release"] * 5,
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance.",
                        "FY 2026 Adjusted EBIT guidance.",
                        "FY 2026 EPS guidance.",
                        "FY 2026 FCF guidance.",
                        "During the quarter we repurchased 12.6 million shares for $127.0 million.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q4_rows = _quarter_block_notes(ws, "2025-12-31")
            buyback_rows = [note for note in q4_rows if note.startswith("Repurchased 12.6m shares for $")]

            assert len(buyback_rows) == 1
            assert "with an average price of $" in buyback_rows[0]
            assert "in Q4." in buyback_rows[0]


def test_generic_quarter_notes_can_add_financing_and_use_of_proceeds_notes_from_sec_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "generic_notes_convertible_financing.xlsx")
            sec_cache_dir = case_dir / "PBI" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_generic-20250930-financing.htm").write_text(
                "Pitney Bowes Inc. In August 2025, we issued an aggregate $230 million convertible senior notes due 2030. "
                "The Convertible Notes accrue interest at a rate of 1.50% per annum. Net proceeds were $221 million. "
                "We used $61.9 million of the proceeds to repurchase 5.5 million of our common stock. "
                "We entered into capped call transactions that are expected to reduce the potential dilution of our common stock upon conversion. "
                "The remaining proceeds will be used for general corporate purposes and other strategic investments.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 2,
                    "note_id": ["g-1", "g-2"],
                    "category": ["Guidance / outlook", "Cash flow / FCF / capex"],
                    "claim": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $25.0m YoY.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $25.0m YoY.",
                    ],
                    "metric_ref": ["Revenue guidance", "FCF"],
                    "score": [95.0, 92.0],
                    "doc_type": ["earnings_release", "model_metric"],
                    "doc": ["release_q3.txt", "history_q"],
                    "source_type": ["earnings_release", "model_metric"],
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $25.0m YoY.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert any("Used $61.9m from convertible notes proceeds to repurchase 5.5m shares." in note for note in q3_rows)
            assert any("Entered capped call transactions expected to reduce dilution from convertible notes conversion." in note for note in q3_rows)
            assert not any("Remaining proceeds will fund general corporate purposes and other strategic investments." in note for note in q3_rows)


def test_gpre_quarter_notes_do_not_promote_static_buyback_program_text_or_note_exchange_as_share_repurchase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_cap_alloc_safety.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "In August 2014 and October 2019, our Board authorized a share repurchase program of up to $200.0 million of our common stock. "
                "The company used approximately $30 million of the net proceeds from the subscription transactions to repurchase approximately 2.9 million shares of its common stock. "
                "At November 5, 2025, $77.2 million in share repurchase authorization remained. "
                "The company entered into exchange transactions to exchange $170 million aggregate principal amount of the 2027 Notes for $170 million of newly issued 5.25% Convertible Senior Notes due November 2030. "
                "Additionally, the company issued $30 million of 2030 Notes for $30 million in cash.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 5,
                    "note_id": ["g-1", "g-2", "debt-1", "util-1", "milestone-1"],
                    "category": [
                        "Programs / initiatives",
                        "Programs / initiatives",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "45Z tax credit monetization agreement executed.",
                        "Junior mezzanine debt of $130.7 million was repaid from Obion sale proceeds.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%.",
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                    ],
                    "note": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "45Z tax credit monetization agreement executed.",
                        "Junior mezzanine debt of $130.7 million was repaid from Obion sale proceeds.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%.",
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                    ],
                    "metric_ref": [
                        "45Z Adjusted EBITDA / monetization",
                        "Strategic milestone",
                        "Debt reduction",
                        "Utilization",
                        "45Z qualification",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["earnings_release"] * 5,
                    "doc": ["release_q3.txt"] * 5,
                    "source_type": ["earnings_release"] * 5,
                    "evidence_snippet": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "45Z tax credit monetization agreement executed.",
                        "Junior mezzanine debt of $130.7 million was repaid from Obion sale proceeds.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%.",
                        "All eight operating ethanol plants expected to qualify for production tax credits in 2026.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert not any("Repurchase authorization increased to $200.0m." in note for note in q3_rows)
            assert not any("Repurchased $170.0m of shares" in note for note in q3_rows)
            assert not any("Repurchased $77.2m of shares" in note for note in q3_rows)
            assert any("45Z" in note or "Junior mezzanine debt" in note for note in q3_rows)


def test_gpre_q4_quarter_notes_add_convertible_exchange_subscription_and_interest_outlook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q4_convertible_exchange.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "Green Plains Inc. "
                "Convertible Debt Exchange "
                "On October 27, 2025, the company executed separate, privately negotiated exchange agreements with certain of the holders of its existing 2.25% Convertible Senior Notes due 2027 "
                "to exchange $170 million aggregate principal amount of the 2027 Notes for $170 million of newly issued 5.25% Convertible Senior Notes due November 2030. "
                "Additionally, the company completed separate, privately negotiated subscription agreements pursuant to which it issued $30 million of 2030 Notes for $30 million in cash. "
                "$200 million in aggregate principal amount of the 2030 Notes is now outstanding, and $60 million in aggregate principal amount of the 2027 Notes remains outstanding with existing terms unchanged. "
                "The company used approximately $30 million of the net proceeds from the subscription transactions to repurchase approximately 2.9 million shares of its common stock. "
                "The initial conversion rate of the 2030 Notes is 63.6132 shares of common stock per $1,000 principal amount of 2030 Notes (equivalent to an initial conversion price of approximately $15.72 per share of common stock). "
                "When considering the extinguishment of the Junior Notes, the increased interest rate on convertible notes, the increased amount of outstanding convertible notes and anticipated interest expense related to the carbon equipment financing, "
                "the company expects annualized interest expense of approximately $30 to $35 million for the year ended December 31, 2026.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. "
                "Convertible Debt Exchange "
                "On October 27, 2025, the company executed separate, privately negotiated exchange agreements with certain of the holders of its existing 2.25% Convertible Senior Notes due 2027 "
                "to exchange $170 million aggregate principal amount of the 2027 Notes for $170 million of newly issued 5.25% Convertible Senior Notes due November 2030. "
                "Additionally, the company completed separate, privately negotiated subscription agreements pursuant to which it issued $30 million of 2030 Notes for $30 million in cash. "
                "On October 27, 2025, in conjunction with the privately negotiated exchange and subscription transactions, the company repurchased approximately 2.9 million shares of its common stock for approximately $30.0 million. "
                "The company used approximately $30 million of the net proceeds from the subscription transactions to repurchase approximately 2.9 million shares of its common stock. "
                "The 2030 Notes are convertible at an initial conversion price of approximately $15.72 per share of common stock. "
                "When considering the extinguishment of the Junior Notes, the increased interest rate on convertible notes, the increased amount of outstanding convertible notes and anticipated interest expense related to the carbon equipment financing, "
                "the company expects annualized interest expense of approximately $30 to $35 million for the year ended December 31, 2026.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000016_gpre-q42025earningsrelease.htm").write_text(
                "Green Plains Inc. Issued an additional $30.0m of 5.25% convertible senior notes due November 2030. "
                "Repurchased approximately 2.9m shares for approximately $30.0m in connection with the exchange and subscription transactions.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 5 + [pd.Timestamp("2025-09-30")] * 2,
                    "note_id": ["q4-1", "q4-2", "q4-3", "q4-4", "q4-5", "q3-1", "q3-2"],
                    "category": [
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Strategy / segment",
                        "Programs / initiatives",
                        "Guidance / outlook",
                        "Debt / liquidity / covenants",
                    ],
                    "claim": [
                        "At least $188m of 45Z-related Adjusted EBITDA expected in 2026.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Adjusted EBITDA YoY 369.8%",
                        "Actively marketing 2026 45Z production tax credits.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "Junior mezzanine debt of $130.7 million was repaid from Obion sale proceeds.",
                    ],
                    "note": [
                        "At least $188m of 45Z-related Adjusted EBITDA expected in 2026.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Adjusted EBITDA YoY 369.8%",
                        "Actively marketing 2026 45Z production tax credits.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "Junior mezzanine debt of $130.7 million was repaid from Obion sale proceeds.",
                    ],
                    "metric_ref": [
                        "45Z Adjusted EBITDA / monetization",
                        "FCF",
                        "Net debt / leverage",
                        "Adjusted EBITDA",
                        "45Z marketing",
                        "45Z Adjusted EBITDA / monetization",
                        "Debt reduction",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0, 92.0, 91.0, 90.0],
                    "doc_type": ["earnings_release"] * 7,
                    "doc": ["release_q4.txt"] * 5 + ["release_q3.txt"] * 2,
                    "source_type": ["earnings_release"] * 7,
                    "evidence_snippet": [
                        "At least $188m of 45Z-related Adjusted EBITDA expected in 2026.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Adjusted EBITDA YoY 369.8%",
                        "Actively marketing 2026 45Z production tax credits.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "Junior mezzanine debt of $130.7 million was repaid from Obion sale proceeds.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q4_rows = _quarter_block_notes(ws, "2025-12-31")
            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            generic_rows = [dict(x) for x in getattr(ctx.derived, "_generic_source_note_rescue_cache", [])]
            q4_generic_summaries = [
                str(x.get("_render_summary") or "")
                for x in generic_rows
                if str(x.get("quarter")) == "2025-12-31"
            ]

            assert any(
                "Exchanged $170.0m of 2.25% convertible senior notes due 2027 for $170.0m of 5.25% convertible senior notes due November 2030 (conversion price $15.72/share)."
                in note
                for note in q4_generic_summaries
            )
            assert any(
                "Exchanged $170.0m of 2.25% convertible senior notes due 2027 for $170.0m of 5.25% convertible senior notes due November 2030 (conversion price $15.72/share)."
                in note
                for note in q4_rows
            )
            assert any(
                "Issued an additional $30.0m of 5.25% convertible senior notes due November 2030; proceeds funded the repurchase of approximately 2.9m shares for approximately $30.0m."
                in note
                for note in q4_generic_summaries
            )
            assert any(
                "Repurchased approximately 2.9m shares for approximately $30.0m in connection with the October 27, 2025 exchange and subscription transactions."
                in note
                for note in q4_rows
            )
            assert not any(
                "Repurchased 2.9m shares for $30.0m in Q4." in note
                or "with an average price of $10.34/share in Q4." in note
                for note in q4_rows
            )
            assert not any(
                note == "Repurchased approximately 2.9m shares for approximately $30.0m in connection with the exchange and subscription transactions."
                for note in q4_rows
            )
            assert any(
                "Annualized 2026 interest expense is expected at about $30.0m-$35.0m, reflecting the 2030 convertible notes, Junior Note extinguishment and carbon equipment financing."
                in note
                for note in q4_generic_summaries
            )
            assert not any("$31.62/share" in note for note in q4_generic_summaries)
            assert not any("Issued $30.0m of convertible senior notes due 2030." in note for note in q3_rows)
            assert not any("2030 Notes" in note for note in q3_rows)


def test_gpre_q4_quarter_notes_add_carbon_capture_and_source_accurate_monetization_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q4_carbon_capture_and_monetization.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. Carbon capture is now fully operational at our Central City, Wood River and York, Nebraska facilities. "
                "On September 16, 2025, we entered into a 45Z tax credit monetization agreement for our Nebraska production. "
                "The agreement was amended on December 10, 2025 to include additional facilities. "
                "The stronger balance sheet and operational excellence support future optionality.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000016_gpre-q42025earningsrelease.htm").write_text(
                "On September 17, 2025, the company entered into a 45Z tax credit monetization agreement. "
                "Carbon capture is fully operational at Central City, Wood River and York, Nebraska facilities.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 4,
                    "note_id": ["q4-1", "q4-2", "q4-3", "q4-4"],
                    "category": [
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "At least $188m of 45Z-related Adjusted EBITDA expected in 2026.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Actively marketing 2026 45Z production tax credits.",
                    ],
                    "note": [
                        "At least $188m of 45Z-related Adjusted EBITDA expected in 2026.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Actively marketing 2026 45Z production tax credits.",
                    ],
                    "metric_ref": [
                        "45Z Adjusted EBITDA / monetization",
                        "FCF",
                        "Net debt / leverage",
                        "45Z marketing",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0],
                    "doc_type": ["earnings_release"] * 4,
                    "doc": ["release_q4.txt"] * 4,
                    "source_type": ["earnings_release"] * 4,
                    "evidence_snippet": [
                        "At least $188m of 45Z-related Adjusted EBITDA expected in 2026.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Actively marketing 2026 45Z production tax credits.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert any(
                "Carbon capture was fully operational at Central City, Wood River and York, Nebraska facilities."
                in note
                for note in q4_rows
            )
            assert any(
                "45Z tax credit monetization agreement for Nebraska production was entered on September 16, 2025 and amended on December 10, 2025 to add credits from three additional facilities."
                in note
                for note in q4_rows
            )
            assert not any("September 17, 2025" in note for note in q4_rows)
            assert not any("operational excellence, and a $49.1 million $428.8 million stronger balance sheet." in note for note in q4_rows)


def test_gpre_quarter_notes_collapse_agreement_rows_to_source_faithful_filing_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_agreement_rows_collapse_to_filing_dates.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "Green Plains Inc. On September 16, 2025, the company entered into an agreement for its Nebraska production tax credits. "
                "Management expects this 45Z tax credit monetization agreement to support low-carbon ethanol value realization.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. On September 16, 2025, the company entered into an agreement for its Nebraska production tax credits. "
                "On December 10, 2025, the agreement was amended to add Section 45Z production tax credits produced at three more of the company's facilities.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000016_gpre-q42025earningsrelease.htm").write_text(
                "Green Plains Inc. On September 17, 2025, the company entered into a 45Z tax credit monetization agreement. "
                "45Z tax credit monetization agreement executed, advancing low-carbon ethanol value creation.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-12-31")],
                    "note_id": ["q3-guid", "q4-guid"],
                    "category": ["Guidance / outlook", "Guidance / outlook"],
                    "claim": ["Q3 placeholder.", "Q4 placeholder."],
                    "note": ["Q3 placeholder.", "Q4 placeholder."],
                    "metric_ref": ["Guidance", "Guidance"],
                    "score": [90.0, 90.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["q3.txt", "q4.txt"],
                    "source_type": ["earnings_release", "earnings_release"],
                    "evidence_snippet": ["Q3 placeholder.", "Q4 placeholder."],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            q3_agreement_rows = [note for note in q3_rows if "45Z tax credit monetization agreement" in note]
            assert len(q3_agreement_rows) == 1
            assert "September 16, 2025" in q3_agreement_rows[0]
            assert "September 17, 2025" not in q3_agreement_rows[0]
            assert not any("agreement executed, advancing low-carbon ethanol value creation" in note for note in q3_rows)
            assert any(
                "45Z tax credit monetization agreement for Nebraska production was entered on September 16, 2025 and amended on December 10, 2025 to add credits from three additional facilities."
                in note
                for note in q4_rows
            )
            assert not any("September 17, 2025" in note for note in q4_rows)


def test_gpre_q4_visible_rows_collapse_to_single_connected_buyback_and_single_filing_faithful_agreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q4_final_visible_event_cleanup.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. On October 27, 2025, in conjunction with the privately negotiated exchange and subscription transactions, "
                "the company repurchased approximately 2.9 million shares of its common stock for approximately $30.0 million. "
                "Tax Credit Purchase Agreement. On September 16, 2025, the company entered into an agreement for Nebraska production tax credits. "
                "On December 10, 2025, the agreement was amended to add credits from three additional facilities.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000016_gpre-q42025earningsrelease.htm").write_text(
                "Green Plains Inc. Repurchased approximately 2.9 million shares for approximately $30.0 million in connection with the exchange and subscription transactions. "
                "On September 17, 2025, the company entered into a 45Z tax credit monetization agreement, later amended on December 10, 2025.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 2,
                    "note_id": ["q4-guid-1", "q4-guid-2"],
                    "category": ["Guidance / outlook", "Guidance / outlook"],
                    "claim": ["Q4 placeholder 1.", "Q4 placeholder 2."],
                    "note": ["Q4 placeholder 1.", "Q4 placeholder 2."],
                    "metric_ref": ["Guidance", "Guidance"],
                    "score": [90.0, 89.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["q4a.txt", "q4b.txt"],
                    "source_type": ["earnings_release", "earnings_release"],
                    "evidence_snippet": ["Q4 placeholder 1.", "Q4 placeholder 2."],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            buyback_rows = [note for note in q4_rows if "repurchased" in note.lower() and "$30.0m" in note]
            assert len(buyback_rows) == 1
            assert (
                "Repurchased approximately 2.9m shares for approximately $30.0m in connection with the October 27, 2025 exchange and subscription transactions."
                in buyback_rows[0]
            )

            agreement_rows = [note for note in q4_rows if "45Z tax credit monetization agreement" in note]
            assert len(agreement_rows) == 1
            assert (
                "45Z tax credit monetization agreement for Nebraska production was entered on September 16, 2025 and amended on December 10, 2025 to add credits from three additional facilities."
                in agreement_rows[0]
            )


def test_gpre_q3_does_not_show_q4_subsequent_event_repurchase_or_carbon_capture_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q3_no_q4_subsequent_events.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "Green Plains Inc. Subsequent Events. On October 27, 2025, in conjunction with the privately negotiated exchange and "
                "subscription agreements for the 2030 Notes, the company repurchased approximately 2.9 million shares of its common stock "
                "for approximately $30.0 million. Carbon capture is now fully operational at our Central City, Wood River and York, Nebraska facilities.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. On October 27, 2025, in conjunction with the privately negotiated exchange and subscription agreements for the 2030 Notes, "
                "the company repurchased approximately 2.9 million shares of its common stock for approximately $30.0 million. "
                "Carbon capture is now fully operational at our Central City, Wood River and York, Nebraska facilities.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-12-31")],
                    "note_id": ["q3-guid", "q4-guid"],
                    "category": ["Guidance / outlook", "Guidance / outlook"],
                    "claim": ["Q3 placeholder.", "Q4 placeholder."],
                    "note": ["Q3 placeholder.", "Q4 placeholder."],
                    "metric_ref": ["Guidance", "Guidance"],
                    "score": [90.0, 90.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["q3.txt", "q4.txt"],
                    "source_type": ["earnings_release", "earnings_release"],
                    "evidence_snippet": ["Q3 placeholder.", "Q4 placeholder."],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert not any("October 27, 2025" in note and "repurchased approximately 2.9m shares" in note.lower() for note in q3_rows)
            assert not any(
                "carbon capture was fully operational at central city, wood river and york" in note.lower()
                for note in q3_rows
            )
            assert any("October 27, 2025" in note and "repurchased approximately 2.9m shares" in note.lower() for note in q4_rows)
            assert any(
                "carbon capture was fully operational at central city, wood river and york" in note.lower()
                for note in q4_rows
            )


def test_gpre_q3_negative_quarter_statement_blocks_later_dated_october_buyback_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q3_negative_statement_blocks_later_october_event.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "Green Plains Inc. We did not repurchase any shares of common stock during the third quarter of 2025. "
                "Subsequent Events. On October 27, 2025, in conjunction with the privately negotiated exchange and "
                "subscription agreements for the 2030 Notes, the company repurchased approximately 2.9 million shares "
                "of its common stock for approximately $30.0 million.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. On October 27, 2025, in conjunction with the privately negotiated exchange and "
                "subscription agreements for the 2030 Notes, the company repurchased approximately 2.9 million shares "
                "of its common stock for approximately $30.0 million.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-12-31")],
                    "note_id": ["q3-guid", "q4-guid"],
                    "category": ["Guidance / outlook", "Guidance / outlook"],
                    "claim": ["Q3 placeholder.", "Q4 placeholder."],
                    "note": ["Q3 placeholder.", "Q4 placeholder."],
                    "metric_ref": ["Guidance", "Guidance"],
                    "score": [90.0, 90.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["q3.txt", "q4.txt"],
                    "source_type": ["earnings_release", "earnings_release"],
                    "evidence_snippet": ["Q3 placeholder.", "Q4 placeholder."],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert not any("October 27, 2025" in note and "repurchased approximately 2.9m shares" in note.lower() for note in q3_rows)
            assert sum(
                1
                for note in q4_rows
                if "October 27, 2025" in note and "repurchased approximately 2.9m shares" in note.lower()
            ) == 1


def test_pbi_quarter_notes_ignore_foreign_sec_cache_docs_inside_ticker_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_ignore_foreign_sec_cache.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000107878225000001_gpre-20250331.htm").write_text(
                "Green Plains Inc. reported first quarter 2025 financial results. "
                "Cost reduction initiatives are progressing ahead of plan, supporting a positive EBITDA outlook under current market conditions. "
                "Management is pursuing non-core asset monetization to enhance liquidity and strengthen the balance sheet. "
                "45Z production tax credits are expected to contribute meaningfully in future periods.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 3,
                    "note_id": ["g-1", "g-2", "g-3"],
                    "category": [
                        "Guidance / outlook",
                        "Debt / liquidity / covenants",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                        "Net debt declined by $28.0m.",
                        "PB Bank note remains relevant in Q1.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                        "Net debt declined by $28.0m.",
                        "PB Bank note remains relevant in Q1.",
                    ],
                    "metric_ref": ["Revenue guidance", "Net debt / leverage", "PB Bank"],
                    "score": [95.0, 93.0, 92.0],
                    "doc_type": ["earnings_release", "model_metric", "earnings_release"],
                    "doc": ["release_q1.txt", "history_q", "release_q1.txt"],
                    "source_type": ["earnings_release", "model_metric", "earnings_release"],
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                        "Net debt declined by $28.0m.",
                        "PB Bank note remains relevant in Q1.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q1_rows = _quarter_block_notes(ws, "2025-03-31")

            assert any("FY 2025 Revenue guidance reaffirmed" in note for note in q1_rows)
            assert not any("45z" in note.lower() for note in q1_rows)
            assert not any("positive ebitda outlook" in note.lower() for note in q1_rows)
            assert not any("non-core asset monetization" in note.lower() for note in q1_rows)


def test_gpre_quarter_notes_recall_clean_model_metric_signals_across_multiple_quarters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_model_metric_recall.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [
                        pd.Timestamp("2025-12-31"),
                        pd.Timestamp("2025-06-30"),
                        pd.Timestamp("2024-12-31"),
                    ],
                    "note_id": ["debt-1", "revolver-1", "fcf-1"],
                    "category": [
                        "Debt / liquidity / covenants",
                        "Debt / liquidity / covenants",
                        "Cash flow / FCF / capex",
                    ],
                    "claim": [
                        "Net debt delta $-65.0m.",
                        "Revolver availability moved to $275.0m at 2025-06-30 (delta $50.0m).",
                        "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m.",
                    ],
                    "note": [
                        "Net debt delta $-65.0m.",
                        "Revolver availability moved to $275.0m at 2025-06-30 (delta $50.0m).",
                        "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m.",
                    ],
                    "metric_ref": [
                        "Net debt / leverage",
                        "revolver_availability_change",
                        "FCF",
                    ],
                    "score": [90.0, 91.0, 89.0],
                    "doc_type": ["model_metric", "model_metric", "model_metric"],
                    "doc": ["history_q", "history_q", "history_q"],
                    "source_type": ["model_metric", "model_metric", "model_metric"],
                    "evidence_snippet": [
                        "Net debt delta $-65.0m.",
                        "Revolver availability moved to $275.0m at 2025-06-30 (delta $50.0m).",
                        "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "Net debt delta $-65.0m."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "Revolver availability moved to $275.0m at 2025-06-30 (delta $50.0m)."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            def _quarter_block(qtxt: str) -> list[str]:
                out: list[str] = []
                capture = False
                for rr in range(1, ws.max_row + 1):
                    marker = str(ws.cell(row=rr, column=1).value or "")
                    if marker == qtxt:
                        capture = True
                        continue
                    if capture and marker:
                        break
                    if capture:
                        note = str(ws.cell(row=rr, column=3).value or "")
                        if note:
                            out.append(note)
                return out

            q4_rows = _quarter_block("2025-12-31")
            q2_rows = _quarter_block("2025-06-30")
            q424_rows = _quarter_block("2024-12-31")

            assert any("Net debt declined by $65" in note for note in q4_rows)
            assert any("Revolver availability increased from $225" in note for note in q2_rows)
            assert any("FCF TTM declined to $120" in note or "FCF TTM declined by $21" in note for note in q424_rows)


def test_gpre_quarter_notes_thin_block_gains_quantified_support_bridge_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_thin_block_quantified.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")] * 5,
                    "note_id": ["milestone-1", "risk-1", "util-1", "rev-1", "margin-1"],
                    "category": [
                        "Programs / initiatives",
                        "Results / drivers",
                        "Results / drivers",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                    ],
                    "claim": [
                        "Carbon capture infrastructure delivered; Q4 2025 start-up still on track.",
                        "Risk management supports margins and cash flow.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%.",
                        "Revolver utilization notable",
                        "EBITDA margin compressed",
                    ],
                    "note": [
                        "Carbon capture infrastructure delivered; Q4 2025 start-up still on track.",
                        "Risk management supports margins and cash flow.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%.",
                        "Revolver utilization notable",
                        "EBITDA margin compressed",
                    ],
                    "metric_ref": [
                        "Strategic milestone",
                        "Risk management",
                        "Utilization",
                        "revolver_utilization",
                        "ebitda_margin_ttm_yoy_bps",
                    ],
                    "score": [95.0, 92.0, 94.0, 89.0, 90.0],
                    "doc_type": ["earnings_release", "earnings_release", "earnings_release", "revolver", "model_metric"],
                    "doc": ["release_q2.txt", "release_q2.txt", "release_q2.txt", "history_q", "history_q"],
                    "source_type": ["earnings_release", "earnings_release", "earnings_release", "revolver", "model_metric"],
                    "evidence_snippet": [
                        "Carbon capture infrastructure equipment delivered and on track for start-up early in the fourth quarter of 2025.",
                        "Risk management supports margins and cash flow.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%.",
                        "We also had $258.5 million available under our committed revolving credit agreement.",
                        "EBITDA margin delta -143 bps",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q2.txt", "doc_type": "earnings_release", "snippet": "Carbon capture infrastructure equipment delivered and on track for start-up early in the fourth quarter of 2025."}]),
                        json.dumps([{"doc_path": "release_q2.txt", "doc_type": "earnings_release", "snippet": "Risk management supports margins and cash flow."}]),
                        json.dumps([{"doc_path": "release_q2.txt", "doc_type": "earnings_release", "snippet": "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "revolver", "snippet": "We also had $258.5 million available under our committed revolving credit agreement."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "EBITDA margin delta -143 bps"}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q2_rows = []
            capture = False
            for rr in range(1, ws.max_row + 1):
                marker = str(ws.cell(row=rr, column=1).value or "")
                if marker == "2025-06-30":
                    capture = True
                    continue
                if capture and marker:
                    break
                if capture:
                    note = str(ws.cell(row=rr, column=3).value or "")
                    if note:
                        q2_rows.append(note)

            assert len(q2_rows) >= 4
            assert any("Revolver availability ended the quarter at $258.5m." in note or "EBITDA margin compressed 143 bps YoY." in note for note in q2_rows)
            assert not any("Risk management supports margins and cash flow." in note for note in q2_rows)


def test_gpre_quarter_notes_rescue_can_compete_even_when_block_already_has_two_survivors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_two_rows_still_allow_rescue.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-12-31")] * 4,
                    "note_id": ["milestone-1", "tone-1", "fcf-1", "debt-1"],
                    "category": [
                        "Programs / initiatives",
                        "Results / drivers",
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                    ],
                    "claim": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                        "Risk management supports margins and cash flow.",
                        "FCF TTM accelerated",
                        "Net debt declined",
                    ],
                    "note": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                        "Risk management supports margins and cash flow.",
                        "FCF TTM accelerated",
                        "Net debt declined",
                    ],
                    "metric_ref": [
                        "Strategic milestone",
                        "Risk management",
                        "fcf_ttm_delta_yoy",
                        "net_debt_yoy_delta",
                    ],
                    "score": [95.0, 94.0, 78.0, 77.0],
                    "doc_type": ["earnings_release", "earnings_release", "model_metric", "model_metric"],
                    "doc": ["release_q4.txt", "release_q4.txt", "history_q", "history_q"],
                    "source_type": ["earnings_release", "earnings_release", "model_metric", "model_metric"],
                    "evidence_snippet": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                        "Risk management supports margins and cash flow.",
                        "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m.",
                        "Net debt delta $-65.0m.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming."}]),
                        json.dumps([{"doc_path": "release_q4.txt", "doc_type": "earnings_release", "snippet": "Risk management supports margins and cash flow."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "FCF TTM at 2024-12-31: $120.0m, yoy -15.0%, delta $-21.0m."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "Net debt delta $-65.0m."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q4_rows: list[str] = []
            capture = False
            for rr in range(1, ws.max_row + 1):
                marker = str(ws.cell(row=rr, column=1).value or "")
                if marker == "2024-12-31":
                    capture = True
                    continue
                if capture and marker:
                    break
                if capture:
                    note = str(ws.cell(row=rr, column=3).value or "")
                    if note:
                        q4_rows.append(note)

            assert len(q4_rows) >= 4
            assert any("FCF TTM declined" in note or "FCF TTM improved" in note for note in q4_rows)
            assert any("Net debt declined by $65" in note for note in q4_rows)


    def test_pbi_quarter_notes_promote_explanatory_driver_note_over_generic_block_fill(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with _case_dir() as case_dir:
            with _profile_override(monkeypatch, "PBI"):
                out_path = _make_model_out_path(case_dir, "pbi_notes_explanatory_driver.xlsx")
                sec_cache = case_dir / "TEST" / "sec_cache"
                sec_cache.mkdir(parents=True, exist_ok=True)
                source_doc = sec_cache / "doc_000162828025023187_q12025earningspressrelea.htm"
                source_doc.write_text(
                    "Gross margin declined $13 million compared to the prior year period primarily driven by lower revenue; "
                    "however, gross margin percentage increased to 68.9% from 66.6% driven by headcount reductions and "
                    "other cost savings initiatives. Higher revenue per piece, improved productivity, and cost reduction "
                    "initiatives drove the increase in Adjusted Segment EBITDA and EBIT.",
                    encoding="utf-8",
                )
                quarter_notes = pd.DataFrame(
                    {
                        "quarter": [pd.Timestamp("2025-03-31")] * 5,
                        "note_id": ["g-1", "g-2", "g-3", "bank-1", "debt-1"],
                        "category": [
                            "Guidance / outlook",
                            "Guidance / outlook",
                            "Guidance / outlook",
                            "Programs / initiatives",
                            "Cash / liquidity / leverage",
                        ],
                        "claim": [
                            "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                            "FY 2025 Adjusted EBIT guidance updated to $450m-$480m.",
                            "FY 2025 FCF guidance updated to $330m-$370m.",
                            "PB Bank target >= $120m bank-held leases.",
                            "Liquidity improved.",
                        ],
                        "note": [
                            "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                            "FY 2025 Adjusted EBIT guidance updated to $450m-$480m.",
                            "FY 2025 FCF guidance updated to $330m-$370m.",
                            "PB Bank target >= $120m bank-held leases.",
                            "Liquidity improved.",
                        ],
                        "metric_ref": [
                            "Revenue guidance",
                            "Adjusted EBIT guidance",
                            "FCF target",
                            "PB Bank liquidity release",
                            "Debt reduction",
                        ],
                        "score": [95.0, 94.0, 93.0, 90.0, 91.0],
                        "doc_type": ["earnings_release", "earnings_release", "earnings_release", "earnings_release", "earnings_release"],
                        "doc": ["release_q1.txt", "release_q1.txt", "release_q1.txt", "release_q1.txt", "release_q1.txt"],
                        "evidence_snippet": [
                            "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                            "FY 2025 Adjusted EBIT guidance updated to $450m-$480m.",
                            "FY 2025 FCF guidance updated to $330m-$370m.",
                            "The Bank held $84 million of associated leases at the end of Q1, and the Company aims to increase that figure to $120 million by the end of 2025.",
                            "Through the end of Q1, the Company repurchased $23 million of debt in the open market.",
                        ],
                    }
                )

                ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
                ctx.callbacks.write_quarter_notes_ui_v2()
                ws = ctx.wb["Quarter_Notes_UI"]

                q1_rows: list[str] = []
                capture = False
                for rr in range(1, ws.max_row + 1):
                    marker = str(ws.cell(row=rr, column=1).value or "")
                    if marker == "2025-03-31":
                        capture = True
                        continue
                    if capture and marker:
                        break
                    if capture:
                        note = str(ws.cell(row=rr, column=3).value or "")
                        if note:
                            q1_rows.append(note)

                assert any(
                    "Gross margin expanded to 68.9% from 66.6%, driven by headcount reductions and cost savings." in note
                    or "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction." in note
                    for note in q1_rows
                )
                assert not any(note == "Liquidity improved." for note in q1_rows)


def test_pbi_quarter_notes_can_recall_explanatory_driver_from_local_source_material(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_ticker_model_out_path(case_dir, "PBI", "pbi_notes_source_driver_rescue.xlsx")
            sec_cache = case_dir / "PBI" / "sec_cache"
            sec_cache.mkdir(parents=True, exist_ok=True)
            source_doc = sec_cache / "doc_000162828026008604_q42025earningspressrelea.htm"
            source_doc.write_text(
                "Gross margin expanded 180 basis points in the fourth quarter due to cost optimization actions and "
                "a shift to higher margin revenue streams. In the fourth quarter, operating expenses declined $28 "
                "million year-over-year primarily from cost reduction initiatives.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 6,
                    "note_id": ["g-1", "g-2", "g-3", "fcf-1", "bb-1", "debt-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Cash / liquidity / leverage",
                        "Cash / liquidity / leverage",
                    ],
                    "claim": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved.",
                        "Share repurchases continued.",
                        "Liquidity improved.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved.",
                        "Share repurchases continued.",
                        "Liquidity improved.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "FCF target",
                        "FCF improvement",
                        "Capital allocation / buyback",
                        "Debt reduction",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0, 91.0, 90.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q4.txt"] * 6,
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved.",
                        "Repurchased 12.6 million shares for $127.0 million in the fourth quarter.",
                        "Reduced principal debt by $114.1 million in the fourth quarter.",
                    ],
                }
            )
            adj_metrics = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-12-31"), pd.Timestamp("2025-12-31")],
                    "adj_fcf": [131_800_000.0, 221_700_000.0],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "adj_metrics": adj_metrics})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q4_rows: list[str] = []
            capture = False
            for rr in range(1, ws.max_row + 1):
                marker = str(ws.cell(row=rr, column=1).value or "")
                if marker == "2025-12-31":
                    capture = True
                    continue
                if capture and marker:
                    break
                if capture:
                    note = str(ws.cell(row=rr, column=3).value or "")
                    if note:
                        q4_rows.append(note)

            assert any(
                "Gross margin expanded 180 bps, driven by cost optimization and a shift to higher margin revenue streams." in note
                or "Operating expenses declined $28.0m YoY, primarily from cost reduction." in note
                for note in q4_rows
            )


def test_gpre_quarter_notes_surface_quantified_adjusted_ebitda_margin_and_obion_repayment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_multi_block_quantified.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [
                        pd.Timestamp("2025-12-31"),
                        pd.Timestamp("2025-12-31"),
                        pd.Timestamp("2025-12-31"),
                        pd.Timestamp("2025-09-30"),
                        pd.Timestamp("2025-09-30"),
                        pd.Timestamp("2025-06-30"),
                        pd.Timestamp("2025-03-31"),
                        pd.Timestamp("2024-03-31"),
                    ],
                    "note_id": ["adj-1", "fcf-1", "debt-1", "obion-1", "q3-margin-1", "q2-rev-1", "q1-oneoff-1", "q124-margin-1"],
                    "category": [
                        "Results / drivers",
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                        "Debt / liquidity / covenants",
                        "One-time items",
                        "Results / drivers",
                    ],
                    "claim": [
                        "Adjusted EBITDA moved materially",
                        "FCF TTM accelerated",
                        "Net debt declined",
                        "Obion sale proceeds used to fully repay junior mezzanine debt",
                        "EBITDA margin compressed",
                        "Revolver utilization notable",
                        "One-offs signal in filing text",
                        "EBITDA margin expanded",
                    ],
                    "note": [
                        "Adjusted EBITDA moved materially",
                        "FCF TTM accelerated",
                        "Net debt declined",
                        "Obion sale proceeds used to fully repay junior mezzanine debt",
                        "EBITDA margin compressed",
                        "Revolver utilization notable",
                        "One-offs signal in filing text",
                        "EBITDA margin expanded",
                    ],
                    "metric_ref": [
                        "adj_ebitda_yoy",
                        "fcf_ttm_delta_yoy",
                        "net_debt_yoy_delta",
                        "Debt reduction",
                        "ebitda_margin_ttm_yoy_bps",
                        "revolver_availability_change",
                        "text:one-offs",
                        "ebitda_margin_ttm_yoy_bps",
                    ],
                    "score": [93.0, 92.0, 91.0, 95.0, 90.0, 89.0, 88.0, 90.0],
                    "doc_type": ["non_gaap", "model_metric", "model_metric", "earnings_release", "model_metric", "revolver", "html", "model_metric"],
                    "doc": [
                        "GPRE-Q4-2025-Earnings-Slides-FINAL.pdf",
                        "history_q",
                        "history_q",
                        "release_q3.txt",
                        "history_q",
                        "history_q",
                        "gpre-20250331.htm",
                        "history_q",
                    ],
                    "source_type": ["non_gaap", "model_metric", "model_metric", "earnings_release", "model_metric", "revolver", "html", "model_metric"],
                    "evidence_snippet": [
                        "Adjusted EBITDA YoY 369.8%",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "EBITDA margin delta -405 bps",
                        "We also had $258.5 million available under our committed revolving credit agreement.",
                        "(2) Corporate activities includes $ 10.3 million of restructuring costs for the three months ended March 31, 2025 as a result of the company's cost reduction initiative, including severance related to the departure of its CEO.",
                        "EBITDA margin delta +172 bps",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "GPRE-Q4-2025-Earnings-Slides-FINAL.pdf", "doc_type": "non_gaap", "snippet": "Adjusted EBITDA YoY 369.8%"}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "FCF TTM YoY delta $198.7m"}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "Net debt delta $-77.9m"}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "EBITDA margin delta -405 bps"}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "revolver", "snippet": "We also had $258.5 million available under our committed revolving credit agreement."}]),
                        json.dumps([{"doc_path": "gpre-20250331.htm", "doc_type": "html", "snippet": "(2) Corporate activities includes $ 10.3 million of restructuring costs for the three months ended March 31, 2025 as a result of the company's cost reduction initiative, including severance related to the departure of its CEO."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "EBITDA margin delta +172 bps"}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            def _quarter_block(qtxt: str) -> list[str]:
                out: list[str] = []
                capture = False
                for rr in range(1, ws.max_row + 1):
                    marker = str(ws.cell(row=rr, column=1).value or "")
                    if marker == qtxt:
                        capture = True
                        continue
                    if capture and marker:
                        break
                    if capture:
                        note = str(ws.cell(row=rr, column=3).value or "")
                        if note:
                            out.append(note)
                return out

            q4_rows = _quarter_block("2025-12-31")
            q3_rows = _quarter_block("2025-09-30")
            q2_rows = _quarter_block("2025-06-30")
            q1_rows = _quarter_block("2025-03-31")
            q124_rows = _quarter_block("2024-03-31")

            assert any("Adjusted EBITDA improved 369.8% YoY." in note for note in q4_rows)
            assert any("Junior mezzanine debt of $130.7m was repaid from Obion sale proceeds." in note for note in q3_rows)
            assert any("Revolver availability ended the quarter at $258.5m." in note for note in q2_rows)
            assert any("Corporate activities included $10.3m of restructuring costs from the cost reduction initiative." in note for note in q1_rows)
            assert any("EBITDA margin expanded 172 bps YoY." in note for note in q124_rows)


def test_pbi_quarter_notes_soft_cap_expands_without_dropping_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_soft_cap_expansion.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 8,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "fcf-1", "bb-1", "debt-1", "driver-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Cash / liquidity / leverage",
                        "Cash / liquidity / leverage",
                        "Better / worse vs prior",
                    ],
                    "claim": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved.",
                        "Share repurchases continued.",
                        "Liquidity improved.",
                        "Gross margin expanded 180 basis points in the fourth quarter due to cost optimization actions and a shift to higher margin revenue streams.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved.",
                        "Share repurchases continued.",
                        "Liquidity improved.",
                        "Gross margin expanded 180 basis points in the fourth quarter due to cost optimization actions and a shift to higher margin revenue streams.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "FCF improvement",
                        "Capital allocation / buyback",
                        "Debt reduction",
                        "Adjusted EBIT / margin",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0, 92.0, 91.0, 90.0, 97.0],
                    "doc_type": ["earnings_release"] * 8,
                    "doc": ["release_q4.txt"] * 8,
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved.",
                        "Repurchased 12.6 million shares for $127.0 million in the fourth quarter.",
                        "Reduced principal debt by $114.1 million in the fourth quarter.",
                        "Gross margin expanded 180 basis points in the fourth quarter due to cost optimization actions and a shift to higher margin revenue streams.",
                    ],
                }
            )
            adj_metrics = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-12-31"), pd.Timestamp("2025-12-31")],
                    "adj_fcf": [131_800_000.0, 221_700_000.0],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "adj_metrics": adj_metrics})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q4_rows = _quarter_block_notes(ws, "2025-12-31")
            assert len(q4_rows) >= 7
            assert sum(1 for note in q4_rows if "FY 2026" in note) >= 4
            assert any("Free cash flow improved to $221.7m, up $89.9m YoY." in note for note in q4_rows)
            assert any(
                ("Reduced principal debt by $114.1m in Q4." in note)
                or ("Quarterly dividend" in note)
                or ("Repurchased" in note)
                for note in q4_rows
            )
            assert any(
                "Gross margin expanded 180 bps, driven by cost optimization and a shift to higher margin revenue streams." in note
                or "Operating expenses declined $28.0m YoY, primarily from cost reduction." in note
                for note in q4_rows
            )
            assert any(
                "Quarterly dividend set at $0.09/share." in note
                or "Repurchased 12.6m shares for $127.0m in Q4." in note
                for note in q4_rows
            )


def test_gpre_quarter_notes_soft_cap_prefers_strong_extras_over_weak_generic_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_soft_cap_expansion.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 7,
                    "note_id": ["m-1", "margin-1", "debt-1", "util-1", "fcf-1", "debt2-1", "tone-1"],
                    "category": [
                        "Programs / initiatives",
                        "Results / drivers",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                    ],
                    "claim": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "EBITDA margin compressed",
                        "Obion sale proceeds used to fully repay junior mezzanine debt.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%.",
                        "FCF TTM accelerated",
                        "Net debt declined",
                        "Risk management supports margins and cash flow.",
                    ],
                    "note": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "EBITDA margin compressed",
                        "Obion sale proceeds used to fully repay junior mezzanine debt.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%.",
                        "FCF TTM accelerated",
                        "Net debt declined",
                        "Risk management supports margins and cash flow.",
                    ],
                    "metric_ref": [
                        "45Z Adjusted EBITDA / monetization",
                        "ebitda_margin_ttm_yoy_bps",
                        "Debt reduction",
                        "Utilization",
                        "fcf_ttm_delta_yoy",
                        "net_debt_yoy_delta",
                        "Risk management",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0, 90.0, 89.0, 92.0],
                    "doc_type": ["earnings_release", "model_metric", "earnings_release", "earnings_release", "model_metric", "model_metric", "earnings_release"],
                    "doc": ["release_q3.txt", "history_q", "release_q3.txt", "release_q3.txt", "history_q", "history_q", "release_q3.txt"],
                    "source_type": ["earnings_release", "model_metric", "earnings_release", "earnings_release", "model_metric", "model_metric", "earnings_release"],
                    "evidence_snippet": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                        "EBITDA margin delta -405 bps",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%.",
                        "FCF TTM YoY delta $198.7m",
                        "Net debt delta $-77.9m",
                        "Risk management supports margins and cash flow.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "EBITDA margin delta -405 bps"}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt."}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%."}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "FCF TTM YoY delta $198.7m"}]),
                        json.dumps([{"doc_path": "history_q", "doc_type": "model_metric", "snippet": "Net debt delta $-77.9m"}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Risk management supports margins and cash flow."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            assert len(q3_rows) >= 6
            assert any("Q4 2025 45Z monetization expected at $15m-$25m." in note for note in q3_rows)
            assert any("EBITDA margin compressed 405 bps YoY." in note for note in q3_rows)
            assert any("Junior mezzanine debt of $130.7m was repaid from Obion sale proceeds." in note for note in q3_rows)
            assert any("FCF TTM improved by $198.7m YoY." in note for note in q3_rows)
            assert any("Net debt declined by $77.9m YoY." in note for note in q3_rows)
            assert not any("Risk management supports margins and cash flow." in note for note in q3_rows)


def test_pbi_quarter_notes_dedupe_identical_explanatory_summary_across_metric_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_duplicate_explanatory_summary.xlsx")
            shared_summary = "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction."
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 4,
                    "note_id": ["driver-1", "driver-2", "g-1", "debt-1"],
                    "category": [
                        "Better / worse vs prior",
                        "Better / worse vs prior",
                        "Guidance / outlook",
                        "Cash / liquidity / leverage",
                    ],
                    "claim": [shared_summary, shared_summary, "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.", "Liquidity improved."],
                    "note": [shared_summary, shared_summary, "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.", "Liquidity improved."],
                    "metric_ref": [
                        "SendTech / Presort operating driver",
                        "Adjusted EBIT / margin",
                        "Revenue guidance",
                        "Debt reduction",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0],
                    "doc_type": ["earnings_release"] * 4,
                    "doc": ["release_q1.txt"] * 4,
                    "evidence_snippet": [
                        shared_summary,
                        shared_summary,
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "Reduced principal debt by $787.2 million in the first quarter.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q1_rows = _quarter_block_notes(ws, "2025-03-31")
            assert sum(1 for note in q1_rows if shared_summary in note) == 1


def test_pbi_quarter_notes_realign_visible_metric_label_when_summary_is_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q1_visible_label_cleanup.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31")],
                    "note_id": ["guide-1", "driver-1"],
                    "category": ["Tone / expectations", "Better / worse vs prior"],
                    "claim": [
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction.",
                    ],
                    "metric_ref": ["Deleveraging target", "SendTech / Presort operating driver"],
                    "score": [94.0, 96.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["release_q1.txt", "release_q1.txt"],
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            capture = False
            matched = False
            for rr in range(1, ws.max_row + 1):
                marker = str(ws.cell(row=rr, column=1).value or "")
                if marker == "2025-03-31":
                    capture = True
                    continue
                if capture and marker:
                    break
                if not capture:
                    continue
                note = str(ws.cell(row=rr, column=3).value or "")
                metric = str(ws.cell(row=rr, column=4).value or "")
                bucket = str(ws.cell(row=rr, column=2).value or "")
                if "FY 2025 Revenue guidance" in note:
                    matched = True
                    assert metric == "Revenue guidance"
                    assert bucket == "Guidance / outlook"
            assert matched


def test_pbi_quarter_notes_do_not_render_eps_guidance_as_cost_savings_in_q2_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q2_eps_alignment.xlsx")
            sec_cache_dir = case_dir / "PBI" / "CEO letters"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "Q2 2025 Earnings CEO Letter.pdf").write_text(
                "Capital Returns – Based on our confidence in the Company's core businesses and our view that shares remain undervalued, "
                "we repurchased $75 million in shares on the open market during the second quarter. "
                "To give us appropriate flexibility, we have increased our existing share repurchase program from $150 million to $400 million. "
                "Additionally, we have again increased our quarterly dividend – from $0.07 per share to $0.08 per share. "
                "Finally, we are increasing our Adjusted EPS guidance from $1.10 - $1.30 to $1.20 - $1.40 primarily due to ongoing share repurchases. "
                "Paul has helped develop actionable initiatives to drive incremental cost reductions and return cash to shareholders.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")] * 4,
                    "note_id": ["g-1", "g-2", "g-3", "g-4"],
                    "category": ["Guidance / outlook"] * 4,
                    "claim": [
                        "FY 2025 Revenue guidance reaffirmed at $1,900m-$1,950m.",
                        "FY 2025 Adjusted EBIT guidance reaffirmed at $450m-$465m.",
                        "FY 2025 EPS guidance reaffirmed at $1.20-$1.40.",
                        "FY 2025 FCF target reaffirmed at $330m-$370m.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance reaffirmed at $1,900m-$1,950m.",
                        "FY 2025 Adjusted EBIT guidance reaffirmed at $450m-$465m.",
                        "FY 2025 EPS guidance reaffirmed at $1.20-$1.40.",
                        "FY 2025 FCF target reaffirmed at $330m-$370m.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "Cost savings target",
                        "FCF target",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["earnings_release"] * 4,
                    "doc": ["release_q2.txt"] * 4,
                    "source_type": ["earnings_release"] * 4,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance reaffirmed at $1,900m-$1,950m.",
                        "FY 2025 Adjusted EBIT guidance reaffirmed at $450m-$465m.",
                        "FY 2025 EPS guidance reaffirmed at $1.20-$1.40.",
                        "FY 2025 FCF target reaffirmed at $330m-$370m.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            capture = False
            matched_eps = False
            for rr in range(1, ws.max_row + 1):
                marker = str(ws.cell(row=rr, column=1).value or "")
                if marker == "2025-06-30":
                    capture = True
                    continue
                if capture and marker:
                    break
                if not capture:
                    continue
                note = str(ws.cell(row=rr, column=3).value or "")
                metric = str(ws.cell(row=rr, column=4).value or "")
                if "$1.20-$1.40" not in note:
                    continue
                assert "Cost savings target" not in note
                if "EPS guidance" in note:
                    matched_eps = True
                    assert metric == "EPS guidance"
            assert matched_eps


def test_pbi_quarter_notes_do_not_emit_legacy_auth_set_row_when_existing_program_is_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q2_auth_raise_only.xlsx")
            sec_cache_dir = case_dir / "PBI" / "CEO letters"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "Q2 2025 Earnings CEO Letter.pdf").write_text(
                "To give us appropriate flexibility, we have increased our existing share repurchase program from "
                "$150 million to $400 million. Additionally, we have again increased our quarterly dividend – from "
                "$0.07 per share to $0.08 per share.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")],
                    "note_id": ["q2-anchor"],
                    "category": ["Guidance / outlook"],
                    "claim": ["FY 2025 Revenue guidance reaffirmed at $1,900m-$1,950m."],
                    "note": ["FY 2025 Revenue guidance reaffirmed at $1,900m-$1,950m."],
                    "metric_ref": ["Revenue guidance"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q2.txt"],
                    "source_type": ["earnings_release"],
                    "evidence_snippet": ["FY 2025 Revenue guidance reaffirmed at $1,900m-$1,950m."],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            capture = False
            q2_notes: list[str] = []
            for rr in range(1, ws.max_row + 1):
                marker = str(ws.cell(row=rr, column=1).value or "")
                if marker == "2025-06-30":
                    capture = True
                    continue
                if capture and marker:
                    break
                if not capture:
                    continue
                q2_notes.append(str(ws.cell(row=rr, column=3).value or ""))

            joined = " | ".join(q2_notes)
            assert "Repurchase authorization increased to $400.0m" in joined
            assert "Share repurchase authorization and remaining capacity updated." not in joined


def test_pbi_quarter_notes_do_not_render_same_note_twice_across_buckets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_no_duplicate_visible_note.xlsx")
            note_txt = "Revolver availability changed materially in the quarter."
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "note_id": ["liq-1", "liq-2"],
                    "category": ["Debt / refi / covenants", "Cash / liquidity / leverage"],
                    "claim": [note_txt, note_txt],
                    "note": [note_txt, note_txt],
                    "metric_ref": ["revolver_availability_change", "revolver_availability_change"],
                    "score": [91.0, 90.0],
                    "doc_type": ["revolver", "revolver"],
                    "doc": ["revolver.csv", "revolver.csv"],
                    "evidence_snippet": [note_txt, note_txt],
                    "evidence_json": [
                        json.dumps([{"doc_path": "revolver.csv", "doc_type": "revolver", "snippet": note_txt}]),
                        json.dumps([{"doc_path": "revolver.csv", "doc_type": "revolver", "snippet": note_txt}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            visible = [
                (
                    str(ws.cell(row=rr, column=3).value or ""),
                    str(ws.cell(row=rr, column=4).value or ""),
                )
                for rr in range(1, ws.max_row + 1)
                if str(ws.cell(row=rr, column=4).value or "") == "Deleveraging / liquidity"
            ]
            assert len(visible) == 1


def test_pbi_quarter_notes_dedupe_same_visible_buyback_note_from_different_source_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_no_duplicate_buyback_preview.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")] + [pd.Timestamp("2025-09-30")] * 4,
                    "note_id": ["bb-1", "bb-2", "g-1", "g-2", "g-3", "g-4"],
                    "category": [
                        "Cash / liquidity / leverage",
                        "Cash / liquidity / leverage",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                    ],
                    "claim": [
                        "Repurchased 25.9 million shares for $281.2 million since starting the program earlier this year.",
                        "Repurchased 25.9 million shares for $281.2 million since starting the program earlier this year while increasing the authorization to $500 million.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.9bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                    ],
                    "note": [
                        "Repurchased 25.9 million shares for $281.2 million since starting the program earlier this year.",
                        "Repurchased 25.9 million shares for $281.2 million since starting the program earlier this year while increasing the authorization to $500 million.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.9bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                    ],
                    "metric_ref": [
                        "Capital allocation / buyback",
                        "Capital allocation / buyback",
                        "FCF target",
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                    ],
                    "score": [96.0, 95.0, 94.0, 93.0, 92.0, 91.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q3.txt"] * 6,
                    "evidence_snippet": [
                        "Repurchased 25.9 million shares for $281.2 million since starting the program earlier this year.",
                        "Repurchased 25.9 million shares for $281.2 million since starting the program earlier this year while increasing the authorization to $500 million.",
                        "FY 2025 FCF guidance tracking near the midpoint of $330m-$370m.",
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.9bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m.",
                        "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert sum(
                1
                for note in q3_rows
                if "Repurchased 25.9m shares for $281.2m" in note
                and "since starting the program earlier this year." in note
            ) == 1


def test_pbi_quarter_notes_add_ceo_letter_management_notes_without_dropping_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_ceo_letter_management_additions.xlsx")
            ceo_dir = case_dir / "TEST" / "CEO letters"
            ceo_dir.mkdir(parents=True, exist_ok=True)
            (ceo_dir / "Q4 2025 Earnings CEO Letter.txt").write_text(
                "In addition to increasing our share repurchase authorization to $500 million, "
                "we increased our quarterly dividend from $0.08 to $0.09 per share. "
                "We are still on track to commence our strategic review's second phase by the end of the second quarter. "
                "These factors, as well as our efforts to improve forecasting, contributed to us disclosing wider ranges "
                "for the current year's guidance in the Company's earnings press release.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 6,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "fcf-1", "debt-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Debt / refi / covenants",
                    ],
                    "claim": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "FCF improvement",
                        "Debt reduction",
                    ],
                    "score": [97.0, 96.0, 95.0, 98.0, 94.0, 93.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q4.txt"] * 6,
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert len(q4_rows) >= 5
            assert any("Free cash flow improved to $221.7m" in note for note in q4_rows)
            assert any("Reduced principal debt by $114.1m in Q4." in note for note in q4_rows)
            assert any("Repurchase authorization increased" in note for note in q4_rows)
            assert any("Quarterly dividend" in note for note in q4_rows)
            assert any("Strategic review phase 2 remains on track by end of Q2 2026." in note for note in q4_rows)
            assert not any(
                "reduced principal debt" in note.lower() and "quarterly dividend" in note.lower()
                for note in q4_rows
            )


def test_pbi_quarter_notes_can_add_auth_dividend_note_alongside_buyback_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q3_auth_dividend_additive.xlsx")
            ceo_dir = case_dir / "TEST" / "CEO letters"
            ceo_dir.mkdir(parents=True, exist_ok=True)
            (ceo_dir / "Q3 2025 Earnings CEO Letter.txt").write_text(
                "In addition to increasing our share repurchase authorization to $500 million, "
                "we increased our quarterly dividend from $0.08 to $0.09 per share. "
                "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million "
                "since starting the program earlier this year. In the third quarter alone, we bought back over 8% "
                "of the shares outstanding.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 6,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "rev-1", "fcf-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Debt / liquidity / covenants",
                        "Cash flow / FCF / capex",
                    ],
                    "claim": [
                        "FY 2025 Revenue guidance midpoint $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance midpoint $430m-$460m.",
                        "FY 2025 EPS guidance midpoint $1.20-$1.40.",
                        "FY 2025 FCF target $330m-$370m.",
                        "Revolver availability moved to $400.0m, delta $135.0m.",
                        "Free cash flow improved to $111.4m, up $31.5m YoY.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance midpoint $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance midpoint $430m-$460m.",
                        "FY 2025 EPS guidance midpoint $1.20-$1.40.",
                        "FY 2025 FCF target $330m-$370m.",
                        "Revolver availability moved to $400.0m, delta $135.0m.",
                        "Free cash flow improved to $111.4m, up $31.5m YoY.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "Deleveraging / liquidity",
                        "FCF improvement",
                    ],
                    "score": [97.0, 96.0, 95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q3.txt"] * 6,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance midpoint $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance midpoint $430m-$460m.",
                        "FY 2025 EPS guidance midpoint $1.20-$1.40.",
                        "FY 2025 FCF target $330m-$370m.",
                        "Revolver availability moved to $400.0m, delta $135.0m.",
                        "Free cash flow improved to $111.4m, up $31.5m YoY.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q3_rows = _quarter_block_notes(ws, "2025-09-30")

            assert any("Repurchase authorization increased to $500.0m" in note for note in q3_rows)
            assert any("Quarterly dividend increased to $0.09/share from $0.08/share" in note for note in q3_rows)


def test_write_excel_can_emit_quarter_notes_audit_sheet_and_saved_workbook_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "quarter_notes_audit_saved_truth.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 2,
                    "note_id": ["fcf-1", "debt-1"],
                    "category": ["Cash flow / FCF / capex", "Debt / liquidity / covenants"],
                    "claim": [
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                    "note": [
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                    "metric_ref": ["FCF improvement", "Debt reduction"],
                    "score": [95.0, 94.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["release_q4.txt", "release_q4.txt"],
                    "source_type": ["earnings_release", "earnings_release"],
                    "evidence_snippet": [
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                }
            )

            result = write_excel_from_inputs(
                _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, quarter_notes_audit=True)
            )

            assert result.saved_workbook_provenance["workbook_path"] == str(out_path)
            assert result.saved_workbook_provenance["workbook_sha1"]
            assert "Generated at" in result.saved_workbook_provenance["quarter_notes_header"]
            audit_rows = _read_quarter_notes_audit_rows(out_path)
            assert audit_rows
            assert any(row.get("stage") == "readback_verified" for row in audit_rows)
            wb = load_workbook(out_path, data_only=True)
            assert wb["Quarter_Notes_Audit"].sheet_state == "visible"
            assert wb.sheetnames[-1] == "Quarter_Notes_Audit"


def test_write_excel_can_skip_temp_saved_workbook_provenance_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "skip_temp_saved_truth.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "note_id": ["fcf-1"],
                    "category": ["Cash flow / FCF / capex"],
                    "claim": ["Free cash flow improved to $221.7m, up $89.9m YoY."],
                    "note": ["Free cash flow improved to $221.7m, up $89.9m YoY."],
                    "metric_ref": ["FCF improvement"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q4.txt"],
                    "source_type": ["earnings_release"],
                    "evidence_snippet": ["Free cash flow improved to $221.7m, up $89.9m YoY."],
                }
            )

            result = write_excel_from_inputs(
                _make_inputs(
                    out_path,
                    ticker="TEST",
                    quarter_notes=quarter_notes,
                    quarter_notes_audit=True,
                    capture_saved_workbook_provenance=False,
                )
            )

            assert result.saved_workbook_provenance == {}
            assert result.quarter_notes_header_text
            assert result.summary_export_expectation is not None
            assert result.valuation_export_expectation is not None
            assert out_path.exists()


def test_validate_saved_workbook_export_builds_provenance_and_validates_in_one_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "combined_saved_truth.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")],
                    "note_id": ["fcf-1"],
                    "category": ["Cash flow / FCF / capex"],
                    "claim": ["Free cash flow improved to $221.7m, up $89.9m YoY."],
                    "note": ["Free cash flow improved to $221.7m, up $89.9m YoY."],
                    "metric_ref": ["FCF improvement"],
                    "score": [95.0],
                    "doc_type": ["earnings_release"],
                    "doc": ["release_q4.txt"],
                    "source_type": ["earnings_release"],
                    "evidence_snippet": ["Free cash flow improved to $221.7m, up $89.9m YoY."],
                }
            )

            result = write_excel_from_inputs(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))

            provenance = validate_saved_workbook_export(
                out_path,
                quarter_notes_ui_snapshot=result.quarter_notes_ui_snapshot,
                summary_export_expectation=result.summary_export_expectation,
                valuation_export_expectation=result.valuation_export_expectation,
                qa_export_expectation=result.qa_export_expectation,
                needs_review_export_expectation=result.needs_review_export_expectation,
            )

            assert provenance["workbook_path"] == str(out_path)
            assert provenance["workbook_sha1"]
            assert provenance["summary_snapshot"]
            assert provenance["valuation_snapshot"]
            assert provenance["quarter_notes_ui_snapshot"] == result.quarter_notes_ui_snapshot


def test_quarter_notes_audit_sheet_strips_illegal_control_chars() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "audit_sheet_illegal_chars.xlsx")
        wb = Workbook()
        wb.save(out_path)
        write_quarter_notes_audit_sheet(
            out_path,
            [
                {
                    "quarter": "2025-03-31",
                    "trace_id": "trace-1",
                    "stage": "source_detected",
                    "source_excerpt": "Cost reductions ahead of plan e\x00ciency and O\x0fcer text",
                    "final_summary": "Ahead of plan.",
                }
            ],
        )
        wb2 = load_workbook(out_path, data_only=True)
        ws = wb2["Quarter_Notes_Audit"]
        vals = list(ws.iter_rows(values_only=True))
        header = [str(x or "") for x in vals[0]]
        source_excerpt_idx = header.index("source_excerpt")
        assert "\x00" not in str(vals[1][source_excerpt_idx] or "")
        assert "\x0f" not in str(vals[1][source_excerpt_idx] or "")


def test_pbi_quarter_notes_audit_traces_auth_dividend_note_to_saved_workbook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q3_auth_dividend_audit.xlsx")
            ceo_dir = case_dir / "TEST" / "CEO letters"
            ceo_dir.mkdir(parents=True, exist_ok=True)
            (ceo_dir / "Q3 2025 Earnings CEO Letter.txt").write_text(
                "In addition to increasing our share repurchase authorization to $500 million, "
                "we increased our quarterly dividend from $0.08 to $0.09 per share. "
                "Through last Friday, we repurchased 25.9 million shares at a total cost of $281.2 million "
                "since starting the program earlier this year.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 5,
                    "note_id": ["g-1", "g-2", "g-3", "liq-1", "fcf-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Debt / liquidity / covenants",
                        "Cash flow / FCF / capex",
                    ],
                    "claim": [
                        "FY 2025 Revenue guidance midpoint $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance midpoint $430m-$460m.",
                        "FY 2025 FCF target $330m-$370m.",
                        "Revolver availability moved to $400.0m, delta $135.0m.",
                        "Free cash flow improved to $111.4m, up $31.5m YoY.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance midpoint $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance midpoint $430m-$460m.",
                        "FY 2025 FCF target $330m-$370m.",
                        "Revolver availability moved to $400.0m, delta $135.0m.",
                        "Free cash flow improved to $111.4m, up $31.5m YoY.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "FCF target",
                        "Deleveraging / liquidity",
                        "FCF improvement",
                    ],
                    "score": [97.0, 96.0, 95.0, 94.0, 93.0],
                    "doc_type": ["earnings_release"] * 5,
                    "doc": ["release_q3.txt"] * 5,
                    "source_type": ["earnings_release"] * 5,
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance midpoint $1.90bn-$1.95bn.",
                        "FY 2025 Adjusted EBIT guidance midpoint $430m-$460m.",
                        "FY 2025 FCF target $330m-$370m.",
                        "Revolver availability moved to $400.0m, delta $135.0m.",
                        "Free cash flow improved to $111.4m, up $31.5m YoY.",
                    ],
                }
            )

            write_excel_from_inputs(
                _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, quarter_notes_audit=True)
            )
            audit_rows = _read_quarter_notes_audit_rows(out_path)
            auth_row = next(
                row
                for row in audit_rows
                if row.get("stage") == "readback_verified"
                and "Repurchase authorization increased to $500.0m" in row.get("final_summary", "")
            )
            dividend_row = next(
                row
                for row in audit_rows
                if row.get("stage") == "readback_verified"
                and "Quarterly dividend increased to $0.09/share from $0.08/share." in row.get("final_summary", "")
            )
            auth_trace_rows = [row for row in audit_rows if row.get("trace_id") == auth_row["trace_id"]]
            dividend_trace_rows = [row for row in audit_rows if row.get("trace_id") == dividend_row["trace_id"]]

            assert any(row.get("stage") == "source_detected" for row in auth_trace_rows)
            assert any(row.get("stage") == "candidate_created" for row in auth_trace_rows)
            assert any(
                row.get("stage") == "source_detected"
                and "quarterly dividend" in (row.get("source_excerpt", "") + " " + row.get("final_summary", "")).lower()
                for row in audit_rows
            )
            assert any(row.get("stage") == "readback_verified" for row in dividend_trace_rows)
            assert any(
                row.get("stage") == "candidate_created"
                and "quarterly dividend" in (row.get("source_excerpt", "") + " " + row.get("final_summary", "")).lower()
                for row in audit_rows
            )
            assert auth_row.get("authorization_confidence") == "present"
            assert dividend_row.get("dividend_change_confidence") == "present"
            assert auth_row.get("scope_confidence") in {"policy_only", "cumulative"}
            assert dividend_row.get("scope_confidence") in {"policy_only", "cumulative"}


def test_pbi_quarter_notes_saved_workbook_adds_q4_auth_capacity_from_html_sec_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q4_html_auth_capacity_saved.xlsx")
            sec_dir = case_dir / "TEST" / "sec_cache"
            sec_dir.mkdir(parents=True, exist_ok=True)
            (sec_dir / "doc_000000000026000001_q42025earningspressrelea.htm").write_text(
                "<html><body>"
                "Pitney Bowes&#8217; Board of Directors recently increased the Company&#8217;s repurchase authorization by $250 million. "
                "As of February 13, 2026, there was $359 million in capacity remaining under the authorization. "
                "The Board approved a regular quarterly dividend of $0.09 per share."
                "</body></html>",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 6,
                    "note_id": ["g-1", "g-2", "g-3", "g-4", "fcf-1", "debt-1"],
                    "category": [
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Debt / refi / covenants",
                    ],
                    "claim": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                    "note": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                    "metric_ref": [
                        "Revenue guidance",
                        "Adjusted EBIT guidance",
                        "EPS guidance",
                        "FCF target",
                        "FCF improvement",
                        "Debt reduction",
                    ],
                    "score": [97.0, 96.0, 95.0, 98.0, 94.0, 93.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q4.txt"] * 6,
                    "source_type": ["earnings_release"] * 6,
                    "evidence_snippet": [
                        "FY 2026 Revenue guidance updated to $1.76bn-$1.86bn.",
                        "FY 2026 Adjusted EBIT guidance updated to $410m-$460m.",
                        "FY 2026 EPS guidance updated to $1.40-$1.60.",
                        "FY 2026 FCF guidance updated to $340m-$370m.",
                        "Free cash flow improved to $221.7m, up $89.9m YoY.",
                        "Reduced principal debt by $114.1m in Q4.",
                    ],
                }
            )

            write_excel_from_inputs(
                _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, quarter_notes_audit=True)
            )
            wb = load_workbook(out_path, data_only=True)
            q4_rows = _quarter_block_notes(wb["Quarter_Notes_UI"], "2025-12-31")

            assert any(
                "Repurchase authorization increased by $250.0m." in note
                for note in q4_rows
            )
            assert any("Remaining share repurchase capacity was $359.0m at quarter-end." in note for note in q4_rows)
            assert any("Quarterly dividend set at $0.09/share." in note for note in q4_rows)


def test_pbi_quarter_notes_collapse_equivalent_guidance_targets_with_bn_and_m_formats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_q1_equivalent_guidance_formats.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 6,
                    "note_id": ["driver-1", "rev-bn", "rev-m", "fcf-1", "ebit-1", "debt-1"],
                    "category": [
                        "Better / worse vs prior",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Guidance / outlook",
                        "Debt / refi / covenants",
                    ],
                    "claim": [
                        "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction.",
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "FY 2025 Revenue guidance updated to $1,950m-$2,000m.",
                        "FY 2025 FCF target $330m-$370m.",
                        "FY 2025 Adjusted EBIT guidance $450m-$480m.",
                        "Reduced principal debt by $787.2m in Q1.",
                    ],
                    "note": [
                        "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction.",
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "FY 2025 Revenue guidance updated to $1,950m-$2,000m.",
                        "FY 2025 FCF target $330m-$370m.",
                        "FY 2025 Adjusted EBIT guidance $450m-$480m.",
                        "Reduced principal debt by $787.2m in Q1.",
                    ],
                    "metric_ref": [
                        "SendTech / Presort operating driver",
                        "Revenue guidance",
                        "Revenue guidance",
                        "FCF target",
                        "Adjusted EBIT guidance",
                        "Debt reduction",
                    ],
                    "score": [96.0, 94.0, 95.0, 93.0, 92.0, 91.0],
                    "doc_type": ["earnings_release"] * 6,
                    "doc": ["release_q1.txt"] * 6,
                    "evidence_snippet": [
                        "Presort EBIT improved, driven by higher revenue per piece, productivity and cost reduction.",
                        "FY 2025 Revenue guidance updated to $1.95bn-$2.00bn.",
                        "FY 2025 Revenue guidance updated to $1,950m-$2,000m.",
                        "FY 2025 FCF target $330m-$370m.",
                        "FY 2025 Adjusted EBIT guidance $450m-$480m.",
                        "Reduced principal debt by $787.2m in Q1.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q1_rows = _quarter_block_notes(ws, "2025-03-31")

            revenue_rows = [note for note in q1_rows if "FY 2025 Revenue guidance" in note]
            assert len(revenue_rows) == 1


def test_gpre_quarter_notes_audit_traces_management_note_and_attrition_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q1_management_audit.xlsx")
            press_dir = case_dir / "TEST" / "press_release"
            press_dir.mkdir(parents=True, exist_ok=True)
            (press_dir / "Green-Plains-Reports-First-Quarter-2025-Financial-Results-2025.txt").write_text(
                "With our cost reduction initiatives implemented and progressing ahead of plan, "
                "we are positioned to deliver positive EBITDA for the remainder of the year based on current market conditions. "
                "We have also taken decisive steps to enhance liquidity and remain focused on monetizing non-core assets "
                "to strengthen our balance sheet.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 4,
                    "note_id": ["fcf-1", "debt-1", "util-1", "ops-1"],
                    "category": [
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                        "Compression infrastructure under construction; Q4 2025 start-up still on track.",
                    ],
                    "note": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                        "Compression infrastructure under construction; Q4 2025 start-up still on track.",
                    ],
                    "metric_ref": ["FCF", "Net debt / leverage", "Utilization", "Strategic milestone"],
                    "score": [95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["model_metric", "model_metric", "earnings_release", "earnings_release"],
                    "doc": ["history_q", "history_q", "release_q1.txt", "release_q1.txt"],
                    "source_type": ["model_metric", "model_metric", "earnings_release", "earnings_release"],
                    "evidence_snippet": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                        "Compression infrastructure under construction; Q4 2025 start-up still on track.",
                    ],
                }
            )

            write_excel_from_inputs(
                _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes, quarter_notes_audit=True)
            )
            audit_rows = _read_quarter_notes_audit_rows(out_path)

            assert any(
                row.get("stage") == "readback_verified"
                and "ahead of plan" in row.get("final_summary", "").lower()
                and "positive ebitda" in row.get("final_summary", "").lower()
                for row in audit_rows
            )
            liquidity_rows = [
                row
                for row in audit_rows
                if "non-core asset" in row.get("source_excerpt", "").lower()
                or "enhance liquidity" in row.get("source_excerpt", "").lower()
            ]
            assert liquidity_rows
            assert any(
                row.get("stage") in {"source_detected", "candidate_created", "selection_lost", "theme_collapsed", "readback_verified"}
                for row in liquidity_rows
            )


def test_gpre_quarter_notes_add_q1_management_framing_from_press_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q1_management_framing.xlsx")
            press_dir = case_dir / "TEST" / "press_release"
            press_dir.mkdir(parents=True, exist_ok=True)
            (press_dir / "Green-Plains-Reports-First-Quarter-2025-Financial-Results-2025.txt").write_text(
                "With our cost reduction initiatives implemented and progressing ahead of plan, "
                "paired with a disciplined hedging program overseen by our newly formed Risk Committee, "
                "we are positioned to deliver positive EBITDA for the remainder of the year based on current market conditions. "
                "We have also taken decisive steps to enhance liquidity and remain focused on monetizing non-core assets "
                "to strengthen our balance sheet, improve capital access and further reduce our cost structure.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 4,
                    "note_id": ["fcf-1", "debt-1", "util-1", "ops-1"],
                    "category": [
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                        "Compression infrastructure under construction; Q4 2025 start-up still on track.",
                    ],
                    "note": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                        "Compression infrastructure under construction; Q4 2025 start-up still on track.",
                    ],
                    "metric_ref": [
                        "FCF",
                        "Net debt / leverage",
                        "Utilization",
                        "Strategic milestone",
                    ],
                    "score": [95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["model_metric", "model_metric", "earnings_release", "earnings_release"],
                    "doc": ["history_q", "history_q", "release_q1.txt", "release_q1.txt"],
                    "source_type": ["model_metric", "model_metric", "earnings_release", "earnings_release"],
                    "evidence_snippet": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                        "Compression infrastructure under construction; Q4 2025 start-up still on track.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q1_rows = _quarter_block_notes(ws, "2025-03-31")

            assert len(q1_rows) >= 5
            assert any("ahead of plan" in note.lower() and "positive EBITDA outlook".lower() in note.lower() for note in q1_rows)
            assert any("non-core asset monetization" in note.lower() and "enhance liquidity" in note.lower() for note in q1_rows)


def test_gpre_quarter_notes_keep_concrete_management_summary_from_noisy_press_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q1_noisy_management_framing.xlsx")
            press_dir = case_dir / "TEST" / "press_release"
            press_dir.mkdir(parents=True, exist_ok=True)
            (press_dir / "Green-Plains-Reports-First-Quarter-2025-Financial-Results-2025.txt").write_text(
                "With our cost reduction initiatives implemented and progressing ahead of plan, paired with a disciplined hedging "
                "program overseen by our newly formed Risk Committee, we are positioned to deliver positive EBITDA for the remainder "
                "of the year based on current market conditions. We have also taken decisive steps to enhance liquidity and remain "
                "focused on monetizing non-core assets to strengthen our balance sheet, improve capital access and further reduce our cost structure.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-03-31")] * 3,
                    "note_id": ["fcf-1", "debt-1", "util-1"],
                    "category": [
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                    ],
                    "claim": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                    ],
                    "note": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                    ],
                    "metric_ref": ["FCF", "Net debt / leverage", "Utilization"],
                    "score": [95.0, 94.0, 93.0],
                    "doc_type": ["model_metric", "model_metric", "earnings_release"],
                    "doc": ["history_q", "history_q", "release_q1.txt"],
                    "source_type": ["model_metric", "model_metric", "earnings_release"],
                    "text_full": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                    ],
                    "comment_full_text": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                    ],
                    "evidence_snippet": [
                        "FCF TTM YoY delta $-149.8m",
                        "Net debt delta $80.9m",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 100%.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ws = ctx.wb["Quarter_Notes_UI"] if "Quarter_Notes_UI" in ctx.wb.sheetnames else None
            if ws is not None:
                del ctx.wb["Quarter_Notes_UI"]
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q1_rows = _quarter_block_notes(ws, "2025-03-31")

            assert any("ahead of plan" in note.lower() and "positive EBITDA outlook".lower() in note.lower() for note in q1_rows)
            assert any("non-core asset monetization" in note.lower() and "enhance liquidity" in note.lower() for note in q1_rows)


def test_gpre_quarter_notes_add_q2_working_capital_note_from_press_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q2_working_capital.xlsx")
            press_dir = case_dir / "TEST" / "press_release"
            press_dir.mkdir(parents=True, exist_ok=True)
            (press_dir / "Green-Plains-Reports-Second-Quarter-2025-Financial-Results-2025.txt").write_text(
                "We executed several key initiatives this quarter to sustain reliable, safe operations, improve efficiencies "
                "and enhance our operating performance. We delivered greater than $50 million improvement in working capital, "
                "delivering scale, optimizing value and improving supply chain efficiencies.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-06-30")] * 4,
                    "note_id": ["ops-1", "margin-1", "rev-1", "util-1"],
                    "category": [
                        "Programs / initiatives",
                        "Strategy / segment",
                        "Debt / liquidity / covenants",
                        "Results / drivers",
                    ],
                    "claim": [
                        "Carbon capture infrastructure equipment delivered and Q4 2025 start-up remains on track.",
                        "EBITDA margin delta -143 bps",
                        "Revolver availability moved to $258.5m at 2025-06-30 (delta $58.5m).",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%.",
                    ],
                    "note": [
                        "Carbon capture infrastructure equipment delivered and Q4 2025 start-up remains on track.",
                        "EBITDA margin delta -143 bps",
                        "Revolver availability moved to $258.5m at 2025-06-30 (delta $58.5m).",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%.",
                    ],
                    "metric_ref": ["Strategic milestone", "EBITDA margin", "revolver_availability_change", "Utilization"],
                    "score": [94.0, 93.0, 92.0, 91.0],
                    "doc_type": ["earnings_release", "model_metric", "model_metric", "earnings_release"],
                    "doc": ["release_q2.txt", "history_q", "history_q", "release_q2.txt"],
                    "source_type": ["earnings_release", "model_metric", "model_metric", "earnings_release"],
                    "evidence_snippet": [
                        "Carbon capture infrastructure equipment delivered and Q4 2025 start-up remains on track.",
                        "EBITDA margin delta -143 bps",
                        "Revolver availability moved to $258.5m at 2025-06-30 (delta $58.5m).",
                        "Achieved strong utilization in the quarter from the nine operating ethanol plants of 99%.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q2_rows = _quarter_block_notes(ws, "2025-06-30")

            assert any("Working capital improved by more than $50.0m." in note for note in q2_rows)


def test_gpre_quarter_notes_add_q4_45z_contribution_and_crush_margin_notes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_q4_45z_contribution.xlsx")
            press_dir = case_dir / "TEST" / "press_release"
            press_dir.mkdir(parents=True, exist_ok=True)
            (press_dir / "Green-Plains-Reports-Fourth-Quarter-and-Full-Year-2025-Financial-Results-2026.txt").write_text(
                "Adjusted EBITDA of $49.1 million, inclusive of $23.4 million in 45Z production tax credit value net of discounts and other costs. "
                "The consolidated ethanol crush margin was $44.4 million for the fourth quarter of 2025, compared with $(15.5) million for the same period in 2024.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-12-31")] * 5,
                    "note_id": ["ebitda-1", "debt-1", "fcf-1", "util-1", "ops-1"],
                    "category": [
                        "Strategy / segment",
                        "Debt / liquidity / covenants",
                        "Cash flow / FCF / capex",
                        "Results / drivers",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "Adjusted EBITDA YoY 369.8%",
                        "Net debt delta $-77.9m",
                        "FCF TTM YoY delta $198.7m",
                        "Utilization 97% of stated capacity",
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                    ],
                    "note": [
                        "Adjusted EBITDA YoY 369.8%",
                        "Net debt delta $-77.9m",
                        "FCF TTM YoY delta $198.7m",
                        "Utilization 97% of stated capacity",
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                    ],
                    "metric_ref": ["Adjusted EBITDA", "Net debt / leverage", "FCF", "Utilization", "Strategic milestone"],
                    "score": [96.0, 95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["model_metric", "model_metric", "model_metric", "earnings_release", "earnings_release"],
                    "doc": ["history_q", "history_q", "history_q", "release_q4.txt", "release_q4.txt"],
                    "source_type": ["model_metric", "model_metric", "model_metric", "earnings_release", "earnings_release"],
                    "evidence_snippet": [
                        "Adjusted EBITDA YoY 369.8%",
                        "Net debt delta $-77.9m",
                        "FCF TTM YoY delta $198.7m",
                        "Utilization 97% of stated capacity",
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert len(q4_rows) >= 7
            assert any("45Z production tax credits contributed $23.4m net of discounts and other costs in Q4." in note for note in q4_rows)
            assert any("Consolidated ethanol crush margin improved to $44.4m" in note for note in q4_rows)
            assert any("FCF TTM improved by $198.7m YoY." in note for note in q4_rows)


def test_gpre_quarter_notes_add_2024_q1_margin_driver_from_press_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_2024_q1_margin_driver.xlsx")
            press_dir = case_dir / "TEST" / "press_release"
            press_dir.mkdir(parents=True, exist_ok=True)
            (press_dir / "Green-Plains-Reports-First-Quarter-2024-Financial-Results-2024.txt").write_text(
                "Margins in the first quarter were weaker across our product mix and we were impacted by industry oversupply "
                "during a mild winter leading to stock builds and lower prices realized, though margins have improved from the first quarter low. "
                "A few plants that were idled during the January cold snap had an impact on the quarter, in addition to significant planned maintenance programs.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-03-31")] * 4,
                    "note_id": ["fcf-1", "debt-1", "margin-1", "rev-1"],
                    "category": [
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Strategy / segment",
                        "Debt / liquidity / covenants",
                    ],
                    "claim": [
                        "FCF TTM YoY delta $93.1m",
                        "Net debt delta $112.1m",
                        "EBITDA margin delta 172 bps",
                        "Revolver availability moved to $230.0m at 2024-03-31 (delta $30.0m).",
                    ],
                    "note": [
                        "FCF TTM YoY delta $93.1m",
                        "Net debt delta $112.1m",
                        "EBITDA margin delta 172 bps",
                        "Revolver availability moved to $230.0m at 2024-03-31 (delta $30.0m).",
                    ],
                    "metric_ref": ["FCF", "Net debt / leverage", "EBITDA margin", "revolver_availability_change"],
                    "score": [95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["model_metric", "model_metric", "model_metric", "model_metric"],
                    "doc": ["history_q", "history_q", "history_q", "history_q"],
                    "source_type": ["model_metric", "model_metric", "model_metric", "model_metric"],
                    "evidence_snippet": [
                        "FCF TTM YoY delta $93.1m",
                        "Net debt delta $112.1m",
                        "EBITDA margin delta 172 bps",
                        "Revolver availability moved to $230.0m at 2024-03-31 (delta $30.0m).",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q1_2024_rows = _quarter_block_notes(ws, "2024-03-31")

            assert len(q1_2024_rows) >= 5
            assert any("Margins were pressured by industry oversupply" in note for note in q1_2024_rows)


def test_gpre_quarter_notes_keep_only_richer_near_duplicate_margin_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_near_duplicate_margin_driver.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-03-31")] * 6,
                    "note_id": ["m-1", "m-2", "fcf-1", "debt-1", "ebitda-1", "rev-1"],
                    "category": [
                        "Results / drivers",
                        "Results / drivers",
                        "Cash flow / FCF / capex",
                        "Debt / liquidity / covenants",
                        "Strategy / segment",
                        "Programs / initiatives",
                    ],
                    "claim": [
                        "Margins were pressured by industry oversupply and a mild winter.",
                        "Margins were pressured by industry oversupply, a mild winter and plant downtime/maintenance.",
                        "FCF TTM YoY delta $93.1m",
                        "Net debt delta $112.1m",
                        "EBITDA margin delta 172 bps",
                        "Revolver availability moved to $230.0m at 2024-03-31 (delta $30.0m).",
                    ],
                    "note": [
                        "Margins were pressured by industry oversupply and a mild winter.",
                        "Margins were pressured by industry oversupply, a mild winter and plant downtime/maintenance.",
                        "FCF TTM YoY delta $93.1m",
                        "Net debt delta $112.1m",
                        "EBITDA margin delta 172 bps",
                        "Revolver availability moved to $230.0m at 2024-03-31 (delta $30.0m).",
                    ],
                    "metric_ref": [
                        "Margin driver",
                        "Margin driver",
                        "FCF",
                        "Net debt / leverage",
                        "EBITDA margin",
                        "revolver_availability_change",
                    ],
                    "score": [96.0, 97.0, 95.0, 94.0, 93.0, 92.0],
                    "doc_type": ["press_release", "press_release", "model_metric", "model_metric", "model_metric", "model_metric"],
                    "doc": ["release_q1.txt", "release_q1.txt", "history_q", "history_q", "history_q", "history_q"],
                    "source_type": ["press_release", "press_release", "model_metric", "model_metric", "model_metric", "model_metric"],
                    "evidence_snippet": [
                        "Margins were pressured by industry oversupply and a mild winter.",
                        "Margins were pressured by industry oversupply, a mild winter and plant downtime/maintenance.",
                        "FCF TTM YoY delta $93.1m",
                        "Net debt delta $112.1m",
                        "EBITDA margin delta 172 bps",
                        "Revolver availability moved to $230.0m at 2024-03-31 (delta $30.0m).",
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]
            q1_rows = _quarter_block_notes(ws, "2024-03-31")

            assert sum(1 for note in q1_rows if "Margins were pressured by industry oversupply" in note) == 1


def test_pbi_tracker_is_stricter_than_notes_for_same_quarter_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_tracker_stricter_than_notes.xlsx")
            promises = pd.DataFrame(
                {
                    "promise_id": ["soft", "hard"],
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "created_quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "last_seen_quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "first_seen_evidence_quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "last_seen_evidence_quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "metric": ["Management tone", "Adjusted EBIT guidance"],
                    "metric_display": ["Management tone", "Adjusted EBIT guidance"],
                    "promise_text": [
                        "Management remains optimistic about execution and opportunities ahead.",
                        "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025.",
                    ],
                    "text_full": [
                        "Management remains optimistic about execution and opportunities ahead.",
                        "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025.",
                    ],
                    "text_snippet": [
                        "Management remains optimistic about execution and opportunities ahead.",
                        "Adjusted EBIT guidance increased to $450 million to $465 million for FY 2025.",
                    ],
                    "target": ["", "$450.0m-$465.0m"],
                    "target_display": ["", "$450.0m-$465.0m"],
                    "target_time": [pd.NaT, pd.Timestamp("2025-12-31")],
                    "target_period_norm": ["", "FY2025"],
                    "promise_type": ["operational", "guidance_range"],
                    "theme_key": ["tone|fy2025", "ebit|fy2025"],
                    "source": [{"source_type": "earnings_release"}, {"source_type": "guidance_snapshot"}],
                }
            )

            inputs = _make_inputs(out_path, ticker="TEST")
            inputs = inputs.__class__(**{**vars(inputs), "promises": promises})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_tracker_ui_v2()
            tracker_ws = ctx.wb["Promise_Tracker_UI"]
            tracker_values = [str(tracker_ws.cell(row=rr, column=cc).value or "") for rr in range(1, tracker_ws.max_row + 1) for cc in range(1, 6)]

            assert not any("optimistic about execution" in val.lower() for val in tracker_values)


def test_gpre_tracker_does_not_originate_completed_operational_result_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_tracker_rejects_completed_results.xlsx")
            promises = pd.DataFrame(
                {
                    "promise_id": ["result-row", "target-row"],
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")],
                    "created_quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")],
                    "last_seen_quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")],
                    "first_seen_evidence_quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")],
                    "last_seen_evidence_quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")],
                    "metric": ["Strategic milestone", "45Z monetization / EBITDA"],
                    "metric_display": ["Strategic milestone", "45Z monetization / EBITDA"],
                    "promise_text": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                    ],
                    "text_full": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                    ],
                    "text_snippet": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming.",
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025.",
                    ],
                    "target": ["", "$15.0m-$25.0m"],
                    "target_display": ["", "$15.0m-$25.0m"],
                    "target_time": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "target_period_norm": ["Q42025", "Q42025"],
                    "promise_type": ["milestone", "guidance_range"],
                    "theme_key": ["nebraska|ops", "45z|q42025"],
                    "source": [{"source_type": "earnings_release"}, {"source_type": "earnings_release"}],
                }
            )

            inputs = _make_inputs(out_path, ticker="TEST")
            inputs = inputs.__class__(**{**vars(inputs), "promises": promises})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_tracker_ui_v2()
            tracker_ws = ctx.wb["Promise_Tracker_UI"]
            tracker_values = [str(tracker_ws.cell(row=rr, column=cc).value or "") for rr in range(1, tracker_ws.max_row + 1) for cc in range(1, 6)]

            assert not any("fully operational and sequestering" in val.lower() for val in tracker_values)
            assert not any("Advantage Nebraska is fully operational" in val for val in tracker_values)


def test_gpre_tracker_does_not_originate_online_ramping_or_debt_repaid_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_tracker_result_rows_blocked.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 2,
                    "note_id": ["ramp-1", "debt-1"],
                    "category": ["Programs / initiatives", "Debt / liquidity / covenants"],
                    "claim": [
                        "Central City and Wood River online and ramping.",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                    ],
                    "note": [
                        "Central City and Wood River online and ramping.",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                    ],
                    "metric_ref": ["Strategic milestone", "Debt reduction"],
                    "score": [92.0, 94.0],
                    "doc_type": ["presentation", "earnings_release"],
                    "doc": ["slides_q3.txt", "release_q3.txt"],
                    "evidence_snippet": [
                        "Central City and Wood River online and ramping.",
                        "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "slides_q3.txt", "doc_type": "presentation", "snippet": "Central City and Wood River online and ramping."}]),
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Sale of Obion, Tennessee plant completed; proceeds used to fully repay $130.7 million junior mezzanine debt."}]),
                    ],
                }
            )

            inputs = _make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes)
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ctx.callbacks.write_promise_tracker_ui_v2()
            tracker_ws = ctx.wb["Promise_Tracker_UI"]
            tracker_values = [
                str(tracker_ws.cell(row=rr, column=cc).value or "")
                for rr in range(1, tracker_ws.max_row + 1)
                for cc in range(1, 6)
            ]

            assert not any("online and ramping" in val.lower() for val in tracker_values)
            assert not any("fully repay" in val.lower() or "Debt reduction" in val for val in tracker_values)


def test_gpre_tracker_does_not_originate_permit_or_equipment_execution_as_new_promises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_tracker_blocks_later_evidence_origins.xlsx")
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2024-09-30"), pd.Timestamp("2024-06-30")],
                    "note_id": ["permit-1", "equip-1"],
                    "category": ["Programs / initiatives", "Programs / initiatives"],
                    "claim": [
                        "Advantage Nebraska strategy on track with pipeline partner receiving their first Class VI well permit in Wyoming.",
                        "Executed construction management agreements and ordered major equipment necessary to capture carbon from Nebraska facilities as part of Advantage Nebraska strategy.",
                    ],
                    "note": [
                        "Advantage Nebraska strategy on track with pipeline partner receiving their first Class VI well permit in Wyoming.",
                        "Executed construction management agreements and ordered major equipment necessary to capture carbon from Nebraska facilities as part of Advantage Nebraska strategy.",
                    ],
                    "metric_ref": ["45Z monetization / EBITDA", "45Z monetization / EBITDA"],
                    "score": [91.0, 90.0],
                    "doc_type": ["earnings_release", "earnings_release"],
                    "doc": ["release_q3.txt", "release_q2.txt"],
                    "evidence_snippet": [
                        "Advantage Nebraska strategy on track with pipeline partner receiving their first Class VI well permit in Wyoming.",
                        "Executed construction management agreements and ordered major equipment necessary to capture carbon from Nebraska facilities as part of Advantage Nebraska strategy.",
                    ],
                    "evidence_json": [
                        json.dumps([{"doc_path": "release_q3.txt", "doc_type": "earnings_release", "snippet": "Advantage Nebraska strategy on track with pipeline partner receiving their first Class VI well permit in Wyoming."}]),
                        json.dumps([{"doc_path": "release_q2.txt", "doc_type": "earnings_release", "snippet": "Executed construction management agreements and ordered major equipment necessary to capture carbon from Nebraska facilities as part of Advantage Nebraska strategy."}]),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
            ctx.callbacks.write_quarter_notes_ui_v2()
            ctx.callbacks.write_promise_tracker_ui_v2()
            tracker_ws = ctx.wb["Promise_Tracker_UI"]
            tracker_values = [str(tracker_ws.cell(row=rr, column=cc).value or "") for rr in range(1, tracker_ws.max_row + 1) for cc in range(1, 6)]

            assert not any("Class VI well permit" in val for val in tracker_values)
            assert not any("ordered major equipment" in val for val in tracker_values)


def test_pbi_progress_unifies_guidance_without_separate_guidance_accuracy_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_unified_guidance.xlsx")
            hist = _make_hist().copy()
            hist["revenue"] = [100_000_000.0, 110_000_000.0, 120_000_000.0, 530_000_000.0]
            progress = pd.DataFrame(
                {
                    "promise_id": ["rev-guidance"],
                    "quarter": [pd.Timestamp("2025-09-30")],
                    "status": ["on_track"],
                    "metric_ref": ["Revenue guidance"],
                    "target": ["$500.0m-$520.0m"],
                    "latest": ["not yet measurable"],
                    "rationale": ["FY 2025 revenue guidance was $500 million to $520 million."],
                    "promise_type": ["guidance_range"],
                    "guidance_type": ["period"],
                    "target_period_norm": ["FY2025"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "FY 2025 revenue guidance was $500 million to $520 million."})
                    ],
                }
            )

            inputs = _make_inputs(out_path, ticker="TEST", hist=hist)
            inputs = inputs.__class__(**{**vars(inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_values = [str(ws.cell(row=rr, column=cc).value or "") for rr in range(1, ws.max_row + 1) for cc in range(1, 7)]
            assert not any("Guidance accuracy" in val for val in visible_values)
            assert sum(1 for val in visible_values if val == "Revenue guidance") == 1


def test_pbi_progress_uses_guidance_lifecycle_id_for_sheet_derived_guidance_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_guidance_id_unified.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["rev-guidance"],
                    "quarter": [pd.Timestamp("2025-03-31")],
                    "status": ["beat"],
                    "metric_ref": ["Revenue guidance"],
                    "target": ["$500m-$520m"],
                    "latest": ["$525m actual"],
                    "rationale": [
                        "FY 2025 revenue guidance was $500 million to $520 million.",
                    ],
                    "promise_type": ["guidance_range"],
                    "guidance_type": ["period"],
                    "target_period_norm": ["FY2025"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "FY 2025 revenue guidance was $500 million to $520 million."}),
                    ],
                }
            )
            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", promise_progress=progress))
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]
            ids = [str(ws.cell(row=rr, column=1).value or "") for rr in range(1, ws.max_row + 1)]
            assert any(val.startswith("guidance:revenue_guidance") for val in ids)
            assert not any(val.startswith("pbi_qn_sheet:") for val in ids)
            assert sum(1 for val in ids if "guidance:revenue_guidance" in val.lower()) == 1


def test_pbi_progress_collapses_duplicate_guidance_rows_for_same_metric_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_guidance_duplicate_collapse.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["pbi_qn_sheet:rev", "guidance:revenue_guidance:FY2025"],
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "status": ["pending", "beat"],
                    "metric_ref": ["Revenue guidance", "Revenue guidance"],
                    "target": ["$500m-$520m", "$500m-$520m"],
                    "latest": ["not yet measurable", "$525m actual"],
                    "rationale": [
                        "FY 2025 revenue guidance was $500 million to $520 million.",
                        "Revenue came in at $525 million versus FY 2025 guidance of $500 million to $520 million.",
                    ],
                    "promise_type": ["guidance_range", "guidance_range"],
                    "guidance_type": ["period", "period"],
                    "target_period_norm": ["FY2025", "FY2025"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "FY 2025 revenue guidance was $500 million to $520 million."}),
                        json.dumps({"doc_type": "earnings_release", "snippet": "Revenue came in at $525 million versus FY 2025 guidance of $500 million to $520 million."}),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", promise_progress=progress))
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]
            ids = [str(ws.cell(row=rr, column=1).value or "") for rr in range(1, ws.max_row + 1)]
            revenue_ids = [val for val in ids if val.lower().startswith("guidance:revenue_guidance")]
            assert len(revenue_ids) == 1
            assert not any("pbi_qn_sheet:" in val.lower() for val in ids)


def test_pbi_progress_rendered_sheet_keeps_one_guidance_row_per_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_rendered_metric_singleton.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["pbi_qn_sheet:ebit", "guidance_eval:ebit"],
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "status": ["on_track", "pending"],
                    "metric_ref": ["Adjusted EBIT guidance", "Adjusted EBIT guidance"],
                    "target": ["$410m-$460m", "$410m-$460m"],
                    "latest": ["not yet measurable", "not yet measurable"],
                    "rationale": [
                        "FY 2026 Adjusted EBIT guidance target $410m-$460m.",
                        "Guidance period FY 2026 has not ended (see evaluated_through).",
                    ],
                    "promise_type": ["guidance_range", "guidance_range"],
                    "guidance_type": ["period", "period"],
                    "target_period_norm": ["FY2026", "FY2026"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "FY 2026 Adjusted EBIT guidance target $410m-$460m."}),
                        json.dumps({"doc_type": "earnings_release", "snippet": "Guidance period FY 2026 has not ended (see evaluated_through)."}),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", promise_progress=progress))
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]
            metric_rows = [
                str(ws.cell(row=rr, column=2).value or "").strip()
                for rr in range(1, ws.max_row + 1)
                if str(ws.cell(row=rr, column=2).value or "").strip() == "Adjusted EBIT guidance"
            ]
            assert len(metric_rows) <= 1


def test_pbi_progress_collapses_cost_savings_rows_into_unified_guidance_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_progress_cost_savings_unified.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["3729b09231c8", "3bf26de8be1e"],
                    "quarter": [pd.Timestamp("2024-12-31"), pd.Timestamp("2024-12-31")],
                    "status": ["on_track", "pending"],
                    "metric_ref": ["Cost savings target", "Cost savings target"],
                    "target": ["$150m-$170m", "$150m-$170m"],
                    "latest": ["not yet measurable", "not yet measurable"],
                    "rationale": [
                        "Cost savings target target $150m-$170m annualized savings.",
                        "Cost savings target target $150m-$170m annualized savings. Guidance period FY 2025 has not ended.",
                    ],
                    "promise_type": ["operational", "guidance_range"],
                    "guidance_type": ["run-rate", "period"],
                    "target_period_norm": ["ANNUALIZED_PROGRAM", "ANNUALIZED_PROGRAM"],
                    "promise_key": ["cost_savings_run_rate", "cost_savings_run_rate"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "Cost savings target target $150m-$170m annualized savings."}),
                        json.dumps({"doc_type": "earnings_release", "snippet": "Cost savings target target $150m-$170m annualized savings. Guidance period FY 2025 has not ended."}),
                    ],
                }
            )

            ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", promise_progress=progress))
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]
            ids = [str(ws.cell(row=rr, column=1).value or "") for rr in range(1, ws.max_row + 1)]
            cost_ids = [val for val in ids if "cost_savings" in val.lower()]
            assert cost_ids == ["guidance:cost_savings:ANNUALIZED_PROGRAM"]


def test_gpre_progress_collapses_same_lifecycle_subject_to_one_row_per_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_progress_same_lifecycle_collapse.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["cc-1", "cc-2"],
                    "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-09-30")],
                    "status": ["completed", "in_progress"],
                    "metric_ref": ["Carbon capture milestone", "Carbon capture milestone"],
                    "target": ["Advantage Nebraska fully operational", "Advantage Nebraska fully operational"],
                    "latest": ["fully operational", "online and ramping"],
                    "rationale": [
                        "Advantage Nebraska is fully operational in Q3 2025.",
                        "Advantage Nebraska remained online and ramping in Q3 2025.",
                    ],
                    "promise_type": ["milestone", "milestone"],
                    "guidance_type": ["text", "text"],
                    "target_period_norm": ["Q32025", "Q32025"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "Advantage Nebraska is fully operational in Q3 2025."}),
                        json.dumps({"doc_type": "presentation", "snippet": "Advantage Nebraska remained online and ramping in Q3 2025."}),
                    ],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            rows = [
                rr
                for rr in range(1, ws.max_row + 1)
                if ws.cell(row=rr, column=2).value == "Advantage Nebraska startup"
            ]
            assert len(rows) == 1
            assert ws.cell(row=rows[0], column=5).value in {"completed", "achieved", "beat"}


def test_gpre_progress_does_not_keep_operational_update_as_parallel_monetization_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_progress_parent_child_collapse.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["startup", "monetization-noisy"],
                    "quarter": [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-12-31")],
                    "status": ["completed", "completed"],
                    "metric_ref": ["Advantage Nebraska startup", "45Z monetization / EBITDA"],
                    "target": ["Advantage Nebraska fully operational", "$15.0m-$25.0m expected Q4 2025 monetization"],
                    "latest": ["fully operational", "Advantage Nebraska fully operational"],
                    "rationale": [
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming in Q4 2025.",
                        "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming in Q4 2025.",
                    ],
                    "promise_type": ["milestone", "guidance_range"],
                    "guidance_type": ["text", "period"],
                    "target_period_norm": ["Q42025", "Q42025"],
                        "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming in Q4 2025."}),
                        json.dumps({"doc_type": "earnings_release", "snippet": "Advantage Nebraska is fully operational and sequestering CO2 in Wyoming in Q4 2025."}),
                    ],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_pairs = [
                (
                    str(ws.cell(row=rr, column=2).value or ""),
                    str(ws.cell(row=rr, column=4).value or ""),
                )
                for rr in range(1, ws.max_row + 1)
            ]
            assert ("Advantage Nebraska startup", "Advantage Nebraska fully operational (Q4 2025)") in visible_pairs
            assert not any(metric == "45Z monetization / EBITDA" and "fully operational" in latest.lower() for metric, latest in visible_pairs)


def test_gpre_progress_collapses_duplicate_same_promise_id_rows_after_hydration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_progress_same_promise_id_collapse.xlsx")
            progress = pd.DataFrame(
                {
                    "promise_id": ["neb-45z", "neb-45z"],
                    "quarter": [pd.Timestamp("2025-06-30"), pd.Timestamp("2025-06-30")],
                    "status": ["beat", "in_progress"],
                    "metric_ref": ["45Z Adjusted EBITDA / monetization", "45Z Adjusted EBITDA / monetization"],
                    "target": ["$15.0m-$25.0m expected Q4 2025 monetization", "$15.0m-$25.0m expected Q4 2025 monetization"],
                    "latest": ["~$27.7m realized in Q4 2025 (FY less 9M)", "Advantage Nebraska fully operational"],
                    "rationale": [
                        "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025. Later update: ~$27.7m realized in Q4 2025.",
                        "Executed construction management agreements and later update: Advantage Nebraska fully operational in Q4 2025.",
                    ],
                    "promise_type": ["operational", "operational"],
                    "guidance_type": ["", ""],
                    "target_period_norm": ["Q42025", "Q42025"],
                    "parent_subject_key": ["program:nebraska45z", "program:nebraska45z"],
                    "canonical_subject_key": ["program:nebraska45z|45z_monetization|q42025", "program:nebraska45z|45z_monetization|q42025"],
                    "lifecycle_subject_key": ["program:nebraska45z|monetization|q42025", "program:nebraska45z|monetization|q42025"],
                    "source_type": ["earnings_release", "presentation"],
                    "statement_class": ["structured_numeric_bridge", "result_evidence"],
                    "evidence_role": ["later_evidence", "result_evidence"],
                    "source_evidence_json": [
                        json.dumps({"doc_type": "earnings_release", "snippet": "On track for $15 - $25 million of 45Z production tax credit monetization value in Q4 2025. Later update: ~$27.7m realized in Q4 2025."}),
                        json.dumps({"doc_type": "presentation", "snippet": "Executed construction management agreements and later update: Advantage Nebraska fully operational in Q4 2025."}),
                    ],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "promise_progress": progress})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_promise_progress_ui_v2()
            ws = ctx.wb["Promise_Progress_UI"]

            visible_rows = [
                (
                    str(ws.cell(row=rr, column=1).value or ""),
                    str(ws.cell(row=rr, column=2).value or ""),
                    str(ws.cell(row=rr, column=4).value or ""),
                )
                for rr in range(1, ws.max_row + 1)
                if str(ws.cell(row=rr, column=2).value or "") == "45Z Adjusted EBITDA / monetization"
            ]
            assert len(visible_rows) == 1
            assert "$27.7m 45Z value realized" in visible_rows[0][2]


def test_gpre_valuation_ignores_generic_dividends_and_distributions_for_common_dividend_logic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_no_generic_common_dividend.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            base_inputs = _make_inputs(out_path, ticker="TEST")
            hist = base_inputs.hist.copy()
            hist["dividends_cash"] = pd.NA
            hist["payments_of_dividends"] = 12_000_000.0

            inputs = base_inputs.__class__(**{**vars(base_inputs), "hist": hist, "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ensure_valuation_inputs(ctx)
            ctx.callbacks.write_valuation_sheet()
            ws = ctx.wb["Valuation"]

            div_cash_row = _find_row_with_value(ws, "Dividends (TTM, cash)")
            div_note_row = _find_row_with_value(ws, "Dividends ($/share)")

            assert div_cash_row is not None
            assert div_note_row is not None
            assert "$" not in str(ws.cell(row=div_cash_row, column=2).value or "")
            assert "no current common dividend/share signal" in str(ws.cell(row=div_note_row, column=2).value or "").lower()


def test_gpre_quarter_notes_do_not_turn_cumulative_buyback_totals_into_q2_q3_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_notes_no_cumulative_q2_q3_buybacks.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000090_gpre-20250630.htm").write_text(
                "Green Plains Inc. To date, we have repurchased approximately 7.4 million shares of common stock "
                "for approximately $92.8 million under the program. No repurchases were made during Q2 2025.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "Green Plains Inc. To date, we have repurchased approximately 7.4 million shares of common stock "
                "for approximately $92.8 million under the program. No repurchases were made during Q3 2025.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. On October 27, 2025, in conjunction with the privately negotiated exchange and "
                "subscription agreements for the 2030 Notes, the company repurchased 2.9 million shares of its "
                "common stock for a total of $30.0 million under the repurchase program.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [
                        pd.Timestamp("2025-06-30"),
                        pd.Timestamp("2025-09-30"),
                        pd.Timestamp("2025-12-31"),
                    ],
                    "note_id": ["q2-guid", "q3-guid", "q4-guid"],
                    "category": ["Guidance / outlook"] * 3,
                    "claim": [
                        "Q2 block placeholder.",
                        "Q3 block placeholder.",
                        "Q4 block placeholder.",
                    ],
                    "note": [
                        "Q2 block placeholder.",
                        "Q3 block placeholder.",
                        "Q4 block placeholder.",
                    ],
                    "metric_ref": ["Guidance"] * 3,
                    "score": [90.0, 90.0, 90.0],
                    "doc_type": ["earnings_release"] * 3,
                    "doc": ["q2.txt", "q3.txt", "q4.txt"],
                    "source_type": ["earnings_release"] * 3,
                    "evidence_snippet": [
                        "Q2 block placeholder.",
                        "Q3 block placeholder.",
                        "Q4 block placeholder.",
                    ],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="GPRE", quarter_notes=quarter_notes)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q2_rows = _quarter_block_notes(ws, "2025-06-30")
            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert not any("7.4m shares" in note or "$92.8m" in note for note in q2_rows)
            assert not any("7.4m shares" in note or "$92.8m" in note for note in q3_rows)
            assert any("2.9m shares" in note and "$30.0m" in note for note in q4_rows)


def test_gpre_valuation_buyback_maps_ignore_cumulative_program_totals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_no_cumulative_buyback_valuation.xlsx")
            sec_cache_dir = case_dir / "TEST" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "Green Plains Inc. To date, we have repurchased approximately 7.4 million shares of common stock "
                "for approximately $92.8 million under the program. No repurchases were made during Q3 2025.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. Holders of the 2027 Notes will have the right, at their option, to require the "
                "company to repurchase their 2.25% Convertible Senior Notes due 2027 upon a fundamental change. "
                "On October 27, 2025, in conjunction with the privately negotiated exchange and subscription "
                "agreements for the 2030 Notes, the company repurchased 2.9 million shares of its common stock for "
                "a total of $30.0 million under the repurchase program. No other repurchase was made during 2025. "
                "We did not repurchase any common stock in 2024 or 2023. To date, we have repurchased approximately "
                "10.3 million shares of common stock for approximately $122.8 million under the program. At "
                "February 10, 2026, $77.2 million in share repurchase authorization remained.",
                encoding="utf-8",
            )

            base_inputs = _make_inputs(out_path, ticker="TEST")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ensure_valuation_inputs(ctx)
            ctx.callbacks.write_valuation_sheet()
            ws = ctx.wb["Valuation"]

            buybacks_note_row = _find_row_with_value(ws, "Buybacks note")
            assert buybacks_note_row is not None
            buyback_note = str(ws.cell(row=buybacks_note_row, column=2).value or "")
            assert "$2.0m" not in buyback_note
            assert "$92.8m" not in buyback_note
            assert "$122.8m" not in buyback_note
            assert "TTM $30.0m" not in buyback_note
            buybacks_shares_row = _find_row_with_value(ws, "Buybacks (shares)")
            assert buybacks_shares_row is not None
            buyback_shares_detail = str(ws.cell(row=buybacks_shares_row, column=2).value or "")
            assert "TTM +2.900m" not in buyback_shares_detail
            audit = dict((ctx.derived.valuation_precompute_bundle or {}).get("valuation_audit") or {})
            suppress_reason = str(audit[pd.Timestamp("2025-09-30")]["buyback_cash"]["suppress_reason"] or "")
            assert suppress_reason in {
                "context/program text blocked for execution metrics",
                "no explicit quarter-safe execution",
            }


def test_valuation_buyback_share_detail_does_not_fallback_to_share_count_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "valuation_buyback_shares_no_delta_fallback.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "Green Plains Inc. On October 27, 2025, in conjunction with the privately negotiated exchange and "
                "subscription agreements for the 2030 Notes, the company repurchased approximately 2.9 million shares "
                "of its common stock for approximately $30.0 million. No other repurchase was made during 2025.",
                encoding="utf-8",
            )
            hist = pd.DataFrame(
                {
                    "quarter": pd.to_datetime(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]),
                    "revenue": [500_000_000.0] * 4,
                    "ebitda": [50_000_000.0] * 4,
                    "ebit": [30_000_000.0] * 4,
                    "op_income": [25_000_000.0] * 4,
                    "cash": [20_000_000.0] * 4,
                    "debt_core": [150_000_000.0] * 4,
                    "interest_paid": [10_000_000.0] * 4,
                    "shares_outstanding": [100_000_000.0, 103_000_000.0, 107_000_000.0, 112_000_000.0],
                    "shares_diluted": [100_000_000.0, 103_000_000.0, 107_000_000.0, 112_000_000.0],
                    "market_cap": [1_000_000_000.0] * 4,
                }
            )

            base_inputs = _make_inputs(out_path, ticker="GPRE", hist=hist)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ensure_valuation_inputs(ctx)
            ctx.callbacks.write_valuation_sheet()
            ws = ctx.wb["Valuation"]

            buybacks_shares_row = _find_row_with_value(ws, "Buybacks (shares)")
            assert buybacks_shares_row is not None
            buyback_shares_detail = str(ws.cell(row=buybacks_shares_row, column=2).value or "")
            assert "QoQ +2.900m" in buyback_shares_detail
            assert "TTM -" not in buyback_shares_detail

            audit = dict((ctx.derived.valuation_precompute_bundle or {}).get("valuation_audit") or {})
            assert audit[pd.Timestamp("2025-09-30")]["buyback_shares"]["value"] is None
            assert audit[pd.Timestamp("2025-09-30")]["buyback_shares"]["suppress_reason"] == "derived share delta blocked for execution metrics"


def test_pbi_valuation_prefers_filing_table_buyback_truth_over_rounded_press_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_valuation_buyback_table_truth.xlsx")
            sec_cache_dir = case_dir / "PBI" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828026008604_q42025earningspressrelea.htm").write_text(
                "During the quarter we repurchased 12.6 million shares for $127.0 million.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000162828026012345_pbi-20251231.htm").write_text(
                "The following table provides information about common stock purchases during the three months ended December 31, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "October 2025 3,203,100 $ 11.30 3,203,100 $212,031 "
                "November 2025 7,926,090 $ 9.52 7,926,090 $136,553 "
                "December 2025 1,484,407 $ 10.05 1,484,407 $121,639 "
                "12,613,597&#160; $ 10.04&#160; 12,613,597 ",
                encoding="utf-8",
            )

            base_inputs = _make_inputs(out_path, ticker="PBI")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ensure_valuation_inputs(ctx)
            ctx.callbacks.write_valuation_sheet()
            ws = ctx.wb["Valuation"]

            buybacks_row = _find_row_with_value(ws, "Buybacks (shares)")
            buybacks_note_row = _find_row_with_value(ws, "Buybacks note")

            assert buybacks_row is not None
            assert buybacks_note_row is not None
            assert "QoQ +12.614m" in str(ws.cell(row=buybacks_row, column=2).value or "")

            buyback_note = str(ws.cell(row=buybacks_note_row, column=2).value or "")
            assert "$126.6m" in buyback_note
            assert "$10.04/share" in buyback_note
            assert "$127.0m" not in buyback_note
            assert "$10.08/share" not in buyback_note


def test_pbi_valuation_prefers_q4_issuer_purchases_table_over_other_repurchase_narrative_in_same_filing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_valuation_buyback_table_vs_same_doc_narrative.xlsx")
            sec_cache_dir = case_dir / "PBI" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828026008604_q42025earningspressrelea.htm").write_text(
                "During the quarter we repurchased 12.6 million shares for $127.0 million.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000162828026012345_pbi-20251231.htm").write_text(
                "During 2025, we used $61.9 million of proceeds to repurchase 5.5 million shares. "
                "The following table provides information about common stock purchases during the three months ended December 31, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "October 2025 3,203,100&#160; $ 11.30&#160; 3,203,100 $212,031 "
                "November 2025 7,926,090&#160; $ 9.52&#160; 7,926,090 $136,553 "
                "December 2025 1,484,407&#160; $ 10.05&#160; 1,484,407 $121,639 "
                "12,613,597&#160; $ 10.04&#160; 12,613,597 ",
                encoding="utf-8",
            )

            base_inputs = _make_inputs(out_path, ticker="PBI")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ensure_valuation_inputs(ctx)
            ctx.callbacks.write_valuation_sheet()
            ws = ctx.wb["Valuation"]

            buybacks_row = _find_row_with_value(ws, "Buybacks (shares)")
            buybacks_note_row = _find_row_with_value(ws, "Buybacks note")

            assert buybacks_row is not None
            assert buybacks_note_row is not None
            assert "QoQ +12.614m" in str(ws.cell(row=buybacks_row, column=2).value or "")

            buyback_note = str(ws.cell(row=buybacks_note_row, column=2).value or "")
            assert "$126.6m" in buyback_note
            assert "$10.04/share" in buyback_note
            assert "$61.9m" not in buyback_note
            assert "$127.0m" not in buyback_note


def test_pbi_capped_call_note_stays_in_origin_quarter_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_notes_capped_call_origin_only.xlsx")
            sec_cache_dir = case_dir / "PBI" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828025047122_pbi-20250930.htm").write_text(
                "Pitney Bowes Inc. In August 2025, we issued an aggregate $230 million convertible senior notes due 2030. "
                "The Convertible Notes accrue interest at a rate of 1.50% per annum. Net proceeds were $221 million. "
                "We used $61.9 million of the proceeds to repurchase 5.5 million of our common stock. "
                "We entered into capped call transactions that are expected to reduce the potential dilution of our common stock upon conversion. ",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000162828026000020_pbi-20251231.htm").write_text(
                "Pitney Bowes Inc. We entered into capped call transactions that are expected to reduce the potential "
                "dilution of our common stock upon conversion.",
                encoding="utf-8",
            )
            quarter_notes = pd.DataFrame(
                {
                    "quarter": [pd.Timestamp("2025-09-30")] * 2 + [pd.Timestamp("2025-12-31")] * 2,
                    "note_id": ["q3-guid", "q3-fcf", "q4-guid", "q4-fcf"],
                    "category": [
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                        "Guidance / outlook",
                        "Cash flow / FCF / capex",
                    ],
                    "claim": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $25.0m YoY.",
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $35.0m YoY.",
                    ],
                    "note": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $25.0m YoY.",
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $35.0m YoY.",
                    ],
                    "metric_ref": ["Revenue guidance", "FCF", "Revenue guidance", "FCF"],
                    "score": [95.0, 92.0, 95.0, 92.0],
                    "doc_type": ["earnings_release", "model_metric", "earnings_release", "model_metric"],
                    "doc": ["release_q3.txt", "history_q", "release_q4.txt", "history_q"],
                    "source_type": ["earnings_release", "model_metric", "earnings_release", "model_metric"],
                    "evidence_snippet": [
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $25.0m YoY.",
                        "FY 2025 Revenue guidance tracking near the midpoint of $1.90bn-$1.95bn.",
                        "FCF TTM improved by $35.0m YoY.",
                    ],
                }
            )

            base_inputs = _make_inputs(out_path, ticker="PBI", quarter_notes=quarter_notes)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            ctx = build_writer_context(inputs)
            ctx.callbacks.write_quarter_notes_ui_v2()
            ws = ctx.wb["Quarter_Notes_UI"]

            q3_rows = _quarter_block_notes(ws, "2025-09-30")
            q4_rows = _quarter_block_notes(ws, "2025-12-31")

            assert any(
                "Entered capped call transactions expected to reduce dilution from convertible notes conversion." in note
                for note in q3_rows
            )
            assert not any("capped call transactions" in note.lower() for note in q4_rows)


def test_valuation_keeps_adj_ebitda_and_adj_ebit_distinct_when_inputs_are_distinct() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "valuation_adj_ebitda_distinct.xlsx")
        base_inputs = _make_inputs(out_path)
        quarter_vals = list(pd.to_datetime(base_inputs.hist["quarter"], errors="coerce").dropna())
        adj_metrics = pd.DataFrame(
            {
                "quarter": quarter_vals,
                "adj_ebit": [40_000_000.0 + (i * 1_000_000.0) for i in range(len(quarter_vals))],
                "adj_ebitda": [55_000_000.0 + (i * 1_000_000.0) for i in range(len(quarter_vals))],
            }
        )
        inputs = base_inputs.__class__(**{**vars(base_inputs), "adj_metrics": adj_metrics})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)
        ctx.callbacks.write_valuation_sheet()
        ws = ctx.wb["Valuation"]

        adj_ebitda_row = _find_row_with_value(ws, "Adj EBITDA (TTM)")
        adj_ebit_row = _find_row_with_value(ws, "Adj EBIT (TTM)")

        assert adj_ebitda_row is not None
        assert adj_ebit_row is not None
        adj_ebitda_vals = [ws.cell(row=adj_ebitda_row, column=cc).value for cc in range(2, ws.max_column + 1)]
        adj_ebit_vals = [ws.cell(row=adj_ebit_row, column=cc).value for cc in range(2, ws.max_column + 1)]
        assert adj_ebitda_vals != adj_ebit_vals


def test_valuation_uses_gaap_and_adjusted_denominators_on_the_correct_rows() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "valuation_denominator_semantics.xlsx")
        hist = pd.DataFrame(
            {
                "quarter": pd.to_datetime(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]),
                "revenue": [500_000_000.0] * 4,
                "ebitda": [100_000_000.0] * 4,
                "ebit": [80_000_000.0] * 4,
                "op_income": [70_000_000.0] * 4,
                "cash": [20_000_000.0] * 4,
                "debt_core": [220_000_000.0] * 4,
                "shares_outstanding": [10_000_000.0] * 4,
                "shares_diluted": [10_000_000.0] * 4,
                "market_cap": [100_000_000.0] * 4,
                "interest_expense_net": [10_000_000.0] * 4,
                "interest_paid": [20_000_000.0] * 4,
            }
        )
        adj_metrics = pd.DataFrame(
            {
                "quarter": hist["quarter"],
                "adj_ebit": [120_000_000.0] * 4,
                "adj_ebitda": [200_000_000.0] * 4,
            }
        )
        inputs = _make_inputs(out_path, hist=hist)
        inputs = inputs.__class__(**{**vars(inputs), "adj_metrics": adj_metrics})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)
        ctx.callbacks.write_valuation_sheet()
        ws = ctx.wb["Valuation"]

        latest_col = 5
        net_lev_row = _find_row_with_value(ws, "Net leverage")
        net_lev_adj_row = _find_row_with_value(ws, "Net leverage (Adj)")
        cov_pnl_row = _find_row_with_value(ws, "Interest coverage (P&L TTM)")
        cov_cash_row = _find_row_with_value(ws, "Cash interest coverage (TTM)")

        assert net_lev_row is not None
        assert net_lev_adj_row is not None
        assert cov_pnl_row is not None
        assert cov_cash_row is not None
        assert ws.cell(row=net_lev_row, column=latest_col).value == pytest.approx(0.5)
        assert ws.cell(row=net_lev_adj_row, column=latest_col).value == pytest.approx(0.25)
        assert ws.cell(row=cov_pnl_row, column=latest_col).value == pytest.approx(10.0)
        assert ws.cell(row=cov_cash_row, column=latest_col).value == pytest.approx(5.0)

        audit = dict((ctx.derived.valuation_precompute_bundle or {}).get("valuation_audit") or {})
        latest_q = pd.Timestamp("2025-12-31")
        assert audit[latest_q]["net_leverage"]["scope"] == "gaap_ebitda_ttm"
        assert audit[latest_q]["net_leverage_adj"]["scope"] == "adjusted_ebitda_ttm"
        assert audit[latest_q]["interest_coverage_pnl"]["scope"] == "gaap_ebitda_ttm/pnl_interest_ttm"
        assert audit[latest_q]["cash_interest_coverage"]["scope"] == "gaap_ebitda_ttm/cash_interest_ttm"
        assert audit[latest_q]["adj_ebit_ttm"]["value"] == pytest.approx(480_000_000.0)
        assert audit[latest_q]["adj_ebitda_ttm"]["value"] == pytest.approx(800_000_000.0)


def test_valuation_displays_nm_when_ebitda_denominator_is_nonmeaningful() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "valuation_nm_on_negative_ebitda.xlsx")
        hist = pd.DataFrame(
            {
                "quarter": pd.to_datetime(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]),
                "revenue": [500_000_000.0] * 4,
                "ebitda": [-20_000_000.0] * 4,
                "ebit": [-35_000_000.0] * 4,
                "op_income": [-40_000_000.0] * 4,
                "cash": [20_000_000.0] * 4,
                "debt_core": [220_000_000.0] * 4,
                "shares_outstanding": [10_000_000.0] * 4,
                "shares_diluted": [10_000_000.0] * 4,
                "market_cap": [100_000_000.0] * 4,
                "interest_expense_net": [10_000_000.0] * 4,
                "interest_paid": [20_000_000.0] * 4,
            }
        )
        adj_metrics = pd.DataFrame(
            {
                "quarter": hist["quarter"],
                "adj_ebit": [-30_000_000.0] * 4,
                "adj_ebitda": [-10_000_000.0] * 4,
            }
        )
        inputs = _make_inputs(out_path, hist=hist)
        inputs = inputs.__class__(**{**vars(inputs), "adj_metrics": adj_metrics})
        ctx = build_writer_context(inputs)
        ensure_valuation_inputs(ctx)
        ctx.callbacks.write_valuation_sheet()
        ws = ctx.wb["Valuation"]

        latest_col = 5
        for label in ("Net leverage", "Net leverage (Adj)", "Interest coverage (P&L TTM)", "Cash interest coverage (TTM)"):
            row_idx = _find_row_with_value(ws, label)
            assert row_idx is not None
            assert ws.cell(row=row_idx, column=latest_col).value == "N/M"

        audit = dict((ctx.derived.valuation_precompute_bundle or {}).get("valuation_audit") or {})
        latest_q = pd.Timestamp("2025-12-31")
        assert audit[latest_q]["net_leverage"]["suppress_reason"] == "EBITDA denominator <= 0"
        assert audit[latest_q]["net_leverage_adj"]["suppress_reason"] == "Adjusted EBITDA denominator <= 0"
        assert audit[latest_q]["interest_coverage_pnl"]["suppress_reason"] == "EBITDA denominator <= 0"
        assert audit[latest_q]["cash_interest_coverage"]["suppress_reason"] == "EBITDA denominator <= 0"


def test_quarter_notes_ui_uses_reaffirmed_and_never_shows_repeat_badge() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "quarter_notes_no_repeat_badge.xlsx")
        quarter_notes = pd.DataFrame(
            {
                "quarter": [pd.Timestamp("2025-09-30"), pd.Timestamp("2025-12-31")],
                "note_id": ["q3-guid", "q4-guid"],
                "category": ["Guidance / outlook", "Guidance / outlook"],
                "claim": [
                    "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                    "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                ],
                "note": [
                    "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                    "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                ],
                "metric_ref": ["Revenue guidance", "Revenue guidance"],
                "score": [95.0, 95.0],
                "doc_type": ["earnings_release", "earnings_release"],
                "doc": ["release_q3.txt", "release_q4.txt"],
                "source_type": ["earnings_release", "earnings_release"],
                "evidence_snippet": [
                    "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                    "FY 2025 Revenue guidance reaffirmed at $1.90bn-$1.95bn.",
                ],
            }
        )

        ctx = build_writer_context(_make_inputs(out_path, ticker="TEST", quarter_notes=quarter_notes))
        ctx.callbacks.write_quarter_notes_ui_v2()
        ws = ctx.wb["Quarter_Notes_UI"]

        q4_rows = _quarter_block_notes(ws, "2025-12-31")
        assert not any("[REPEAT]" in note for note in q4_rows)
        assert not any("[REPEAT]" in note for note in _quarter_block_notes(ws, "2025-09-30"))
        if q4_rows:
            assert any(
                ("[REAFFIRMED]" in note)
                or ("[CONTINUED]" in note)
                or not note.startswith("[")
                for note in q4_rows
            )


def test_write_excel_saved_workbook_provenance_captures_summary_and_valuation_truth() -> None:
    with _case_dir() as case_dir:
        out_path = _make_model_out_path(case_dir, "saved_workbook_summary_valuation_truth.xlsx")
        inputs = _make_inputs(out_path, ticker="TEST", quarter_notes=pd.DataFrame())
        inputs.company_overview = {
            "what_it_does": "Demo company description.",
            "what_it_does_source": "Source: SEC 10-K demo",
            "current_strategic_context": "Management is focused on capital allocation and cost discipline.",
            "current_strategic_context_source": "Source: SEC 8-K demo",
            "key_advantage": "Demo advantage.",
            "key_advantage_source": "Source: SEC 10-K competition demo",
            "segment_operating_model": [],
            "segment_operating_model_source": "Source: N/A",
            "key_dependencies": [],
            "key_dependencies_source": "Source: N/A",
            "wrong_thesis_bullets": [],
            "wrong_thesis_source": "Source: N/A",
            "revenue_streams": [],
            "revenue_streams_source": "Source: N/A",
            "asof_fy_end": None,
        }

        result = write_excel_from_inputs(inputs)

        assert result.summary_export_expectation
        assert result.valuation_export_expectation
        validate_summary_export(out_path, result.summary_export_expectation)
        validate_valuation_export(out_path, result.valuation_export_expectation)

        summary_snapshot = result.saved_workbook_provenance.get("summary_snapshot") or {}
        valuation_snapshot = result.saved_workbook_provenance.get("valuation_snapshot") or {}
        assert summary_snapshot["What the company does"]["value"] == "Demo company description."
        assert summary_snapshot["Current strategic context"]["source"] == "Source: SEC 8-K demo"
        assert "Buybacks (TTM, cash)" in dict(valuation_snapshot.get("grid_rows") or {})


def test_pbi_saved_workbook_buyback_truth_is_synced_across_valuation_qa_and_needs_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "PBI"):
            out_path = _make_model_out_path(case_dir, "pbi_buyback_truth_sync.xlsx")
            sec_cache_dir = case_dir / "PBI" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000162828026008604_q42025earningspressrelea.htm").write_text(
                "During the quarter we repurchased 12.6 million shares for $127.0 million.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000162828026012345_pbi-20251231.htm").write_text(
                "The following table provides information about common stock purchases during the three months ended December 31, 2025. "
                "Total number of shares purchased Average price paid per share Total number of shares purchased as part of publicly announced plans or programs Approximate dollar value of shares that may yet be purchased under the plans or programs. "
                "October 2025 3,203,100 $ 11.30 3,203,100 $212,031 "
                "November 2025 7,926,090 $ 9.52 7,926,090 $136,553 "
                "December 2025 1,484,407 $ 10.05 1,484,407 $121,639 "
                "12,613,597&#160; $ 10.04&#160; 12,613,597 ",
                encoding="utf-8",
            )

            base_inputs = _make_inputs(out_path, ticker="PBI")
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            result = write_excel_from_inputs(inputs)

            validate_summary_export(out_path, result.summary_export_expectation)
            validate_valuation_export(out_path, result.valuation_export_expectation)
            validate_qa_export(out_path, result.qa_export_expectation)
            validate_needs_review_export(out_path, result.needs_review_export_expectation)

            valuation_snapshot = result.saved_workbook_provenance.get("valuation_snapshot") or {}
            hidden_rows = dict(valuation_snapshot.get("hidden_rows") or {})
            shares_text = str(hidden_rows.get("Buybacks (shares)") or "")
            note_text = str(hidden_rows.get("Buybacks note") or "")
            assert "QoQ +12.614m" in shares_text
            assert "TTM +48.422m" not in shares_text
            assert "YoY" not in shares_text
            assert "$126.6m" in note_text
            assert "$10.04/share" in note_text
            assert "$127.0m" not in note_text
            assert "$10.08/share" not in note_text
            assert "YoY" not in note_text

            qa_rows = result.saved_workbook_provenance.get("qa_checks_snapshot") or []
            assert len(qa_rows) == 1
            qa_msg = str(qa_rows[0].get("message") or "")
            assert "$126.6m" in qa_msg
            assert "$10.04/share" in qa_msg
            assert "$127.0m" not in qa_msg
            assert "$10.08/share" not in qa_msg
            assert "supported by explicit SEC repurchase disclosures" not in qa_msg

            needs_rows = result.saved_workbook_provenance.get("needs_review_snapshot") or []
            assert needs_rows == []
            assert _sheet_metric_rows(
                out_path,
                "Needs_Review",
                "buybacks_cash",
                quarters=list((result.needs_review_export_expectation or {}).get("quarters") or []),
            ) == []


def test_gpre_saved_workbook_buyback_truth_drops_historical_leakage_and_syncs_recent_truth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as case_dir:
        with _profile_override(monkeypatch, "GPRE"):
            out_path = _make_model_out_path(case_dir, "gpre_buyback_truth_sync.xlsx")
            sec_cache_dir = case_dir / "GPRE" / "sec_cache"
            sec_cache_dir.mkdir(parents=True, exist_ok=True)
            (sec_cache_dir / "doc_000130940224000019_gpre-20241231.htm").write_text(
                "To date, we have repurchased approximately 7.4 million shares of common stock for approximately $92.8 million under the program.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940225000120_gpre-20250630.htm").write_text(
                "We did not repurchase any common stock during the second quarter of 2025.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940225000163_gpre-20250930.htm").write_text(
                "To date, we have repurchased approximately 7.4 million shares of common stock for approximately $92.8 million under the program. No repurchases were made during Q3 2025.",
                encoding="utf-8",
            )
            (sec_cache_dir / "doc_000130940226000019_gpre-20251231.htm").write_text(
                "On October 27, 2025, in conjunction with the privately negotiated exchange and subscription agreements for the 2030 Notes, "
                "the company repurchased approximately 2.9 million shares of its common stock for approximately $30.0 million. "
                "No other repurchase was made during 2025. At February 10, 2026, $77.2 million in share repurchase authorization remained.",
                encoding="utf-8",
            )
            hist = pd.DataFrame(
                {
                    "quarter": pd.to_datetime(
                        [
                            "2024-03-31",
                            "2024-06-30",
                            "2024-09-30",
                            "2024-12-31",
                            "2025-03-31",
                            "2025-06-30",
                            "2025-09-30",
                            "2025-12-31",
                        ]
                    ),
                    "revenue": [500_000_000.0] * 8,
                    "ebitda": [50_000_000.0] * 8,
                    "ebit": [30_000_000.0] * 8,
                    "op_income": [25_000_000.0] * 8,
                    "cash": [20_000_000.0] * 8,
                    "debt_core": [150_000_000.0] * 8,
                    "interest_paid": [10_000_000.0] * 8,
                    "shares_outstanding": [100_000_000.0] * 8,
                    "shares_diluted": [100_000_000.0] * 8,
                    "market_cap": [1_000_000_000.0] * 8,
                }
            )

            base_inputs = _make_inputs(out_path, ticker="GPRE", hist=hist)
            inputs = base_inputs.__class__(**{**vars(base_inputs), "cache_dir": sec_cache_dir})
            result = write_excel_from_inputs(inputs)

            validate_summary_export(out_path, result.summary_export_expectation)
            validate_valuation_export(out_path, result.valuation_export_expectation)
            validate_qa_export(out_path, result.qa_export_expectation)
            validate_needs_review_export(out_path, result.needs_review_export_expectation)

            valuation_snapshot = result.saved_workbook_provenance.get("valuation_snapshot") or {}
            quarter_headers = list(valuation_snapshot.get("quarter_headers") or [])
            buyback_ttm_vals = list((valuation_snapshot.get("grid_rows") or {}).get("Buybacks (TTM, cash)") or [])
            ttm_by_q = dict(zip(quarter_headers, buyback_ttm_vals))
            assert ttm_by_q.get("2024-Q4") in (None, "")
            assert ttm_by_q.get("2025-Q1") in (None, "")
            assert ttm_by_q.get("2025-Q2") in (None, "")
            assert ttm_by_q.get("2025-Q3") in (None, "")
            assert ttm_by_q.get("2025-Q4") in (None, "")

            hidden_rows = dict(valuation_snapshot.get("hidden_rows") or {})
            shares_text = str(hidden_rows.get("Buybacks (shares)") or "")
            note_text = str(hidden_rows.get("Buybacks note") or "")
            assert "QoQ +2.900m" in shares_text
            assert "2.024" not in shares_text
            assert "TTM" not in shares_text
            assert "$30.0m" in note_text
            assert "$2.0m" not in note_text
            assert "94.8" not in note_text
            assert "YoY" not in note_text

            qa_rows = result.saved_workbook_provenance.get("qa_checks_snapshot") or []
            assert len(qa_rows) == 1
            qa_msg = str(qa_rows[0].get("message") or "")
            assert "2.900m" in qa_msg
            assert "$30.0m" in qa_msg
            assert "$2.0m" not in qa_msg
            assert "supported by explicit SEC repurchase disclosures" not in qa_msg

            needs_rows = result.saved_workbook_provenance.get("needs_review_snapshot") or []
            assert needs_rows == []
            assert _sheet_metric_rows(
                out_path,
                "Needs_Review",
                "buybacks_cash",
                quarters=list((result.needs_review_export_expectation or {}).get("quarters") or []),
            ) == []


def test_current_delivered_workbooks_match_visible_quarter_notes_ui_snapshots() -> None:
    expected_snapshots = {
        "PBI": {
            "2025-09-30": [
                ("Guidance / outlook", "FY 2025 Revenue guidance tracking near the midpoint of $1,900m-$1,950m."),
                ("Guidance / outlook", "FY 2025 Adjusted EBIT guidance tracking near the midpoint of $450m-$465m."),
                ("Guidance / outlook", "FY 2025 EPS guidance tracking near the midpoint of $1.20-$1.40."),
                ("Guidance / outlook", "FY 2025 FCF target tracking near the midpoint of $330m-$370m."),
                ("Capital allocation / shareholder returns", "Repurchase authorization increased to $500.0m, up from $400.0m."),
                ("Capital allocation / shareholder returns", "Remaining share repurchase capacity was $148.2m at quarter-end."),
                ("Capital allocation / shareholder returns", "Quarterly dividend increased to $0.09/share from $0.08/share."),
                ("Capital allocation / shareholder returns", "Repurchased 14.1m shares for $161.5m with an average price of $11.44/share in Q3."),
                ("Capital allocation / shareholder returns", "Used $61.9m from convertible notes proceeds to repurchase 5.5m shares."),
                ("Capital allocation / shareholder returns", "Entered capped call transactions expected to reduce dilution from convertible notes conversion."),
                ("Debt / liquidity / balance sheet", "Revolver availability increased from $265.0m to $400.0m."),
            ],
            "2025-12-31": [
                ("Guidance / outlook", "FY 2026 Revenue guidance $1,760m-$1,860m."),
                ("Guidance / outlook", "FY 2026 Adjusted EBIT guidance $410m-$460m."),
                ("Guidance / outlook", "FY 2026 EPS guidance $1.40-$1.60."),
                ("Guidance / outlook", "FY 2026 FCF target $340m-$370m."),
                ("Guidance / outlook", "Guidance ranges widened due to market uncertainty and forecasting changes."),
                ("Capital allocation / shareholder returns", "Repurchase authorization increased by $250.0m."),
                ("Capital allocation / shareholder returns", "Remaining share repurchase capacity was $359.0m at quarter-end."),
                ("Capital allocation / shareholder returns", "Repurchased 12.6m shares for $126.6m with an average price of $10.04/share in Q4."),
                ("Capital allocation / shareholder returns", "Quarterly dividend set at $0.09/share."),
                ("Cash flow / FCF / working capital", "Free cash flow improved to $221.7m, up $89.9m YoY."),
                ("Debt / liquidity / balance sheet", "Reduced principal debt by $114.1m in Q4."),
                ("Debt / liquidity / balance sheet", "Reached sub-3.0x leverage, improving covenant flexibility."),
                ("Results / drivers / better vs prior", "Gross margin expanded 180 bps, driven by cost optimization and a shift to higher margin revenue streams."),
                ("Results / drivers / better vs prior", "Operating expenses declined $28.0m YoY, primarily from cost reduction."),
                ("Programs / initiatives / management framing", "Strategic review phase 2 remains on track by end of Q2 2026."),
            ],
        },
        "GPRE": {
            "2025-09-30": [
                ("Guidance / outlook", "Q4 2025 45Z monetization expected at $15m-$25m."),
                ("Guidance / outlook", "All eight operating ethanol plants expected to qualify for production tax credits in 2026"),
                ("Capital allocation / shareholder returns", "Repurchase authorization increased to $200.0m."),
                ("Debt / liquidity / balance sheet", "Junior mezzanine debt of $130.7m was repaid from Obion sale proceeds."),
                ("Debt / liquidity / balance sheet", "Revolver availability ended the quarter at $325.0m."),
                ("Results / drivers / better vs prior", "EBITDA margin compressed 405 bps YoY."),
                ("Results / drivers / better vs prior", "45Z production tax credits contributed $25.0m net of discounts and other costs."),
                ("Operations / commercialization / milestones", "Achieved strong utilization in the quarter from the nine operating ethanol plants of 101%."),
                ("Operations / commercialization / milestones", "York carbon capture was fully operational; Central City and Wood River were online and ramping."),
                ("Operations / commercialization / milestones", "45Z tax credit monetization agreement for Nebraska production was entered on September 16, 2025."),
            ],
            "2025-12-31": [
                ("Guidance / outlook", "FY 2026 45Z-related Adjusted EBITDA outlook is at least $188.0m."),
                ("Capital allocation / shareholder returns", "Repurchase authorization increased to $200.0m."),
                ("Capital allocation / shareholder returns", "Repurchased approximately 2.9m shares for approximately $30.0m in connection with the October 27, 2025 exchange and subscription transactions."),
                ("Capital allocation / shareholder returns", "Issued an additional $30.0m of 5.25% convertible senior notes due November 2030; proceeds funded the repurchase of approximately 2.9m shares for approximately $30.0m."),
                ("Cash flow / FCF / working capital", "FCF TTM improved by $198.7m YoY."),
                ("Debt / liquidity / balance sheet", "Net debt declined by $77.9m YoY."),
                ("Debt / liquidity / balance sheet", "Exchanged $170.0m of 2.25% convertible senior notes due 2027 for $170.0m of 5.25% convertible senior notes due November 2030 (conversion price $15.72/share)."),
                ("Debt / liquidity / balance sheet", "Annualized 2026 interest expense is expected at about $30.0m-$35.0m, reflecting the 2030 convertible notes, Junior Note extinguishment and carbon equipment financing."),
                ("Results / drivers / better vs prior", "Adjusted EBITDA improved 369.8% YoY."),
                ("Results / drivers / better vs prior", "45Z production tax credits contributed $23.4m net of discounts and other costs."),
                ("Results / drivers / better vs prior", "Consolidated ethanol crush margin improved to $44.4m from $15.5m YoY."),
                ("Operations / commercialization / milestones", "Advantage Nebraska 2026 Adjusted >$150M EBITDA opportunity."),
                ("Operations / commercialization / milestones", "Carbon capture was fully operational at Central City, Wood River and York, Nebraska facilities."),
                ("Operations / commercialization / milestones", "45Z tax credit monetization agreement for Nebraska production was entered on September 16, 2025 and amended on December 10, 2025 to add credits from three additional facilities."),
                ("One-time items / restructuring", "Corporate activities included $16.1m of restructuring costs from the cost reduction initiative."),
            ],
        },
    }

    for ticker, expected_snapshot in expected_snapshots.items():
        workbook_path = _current_delivered_model_path(ticker)
        if not workbook_path.exists():
            pytest.skip(f"Current delivered workbook missing for snapshot test: {workbook_path}")
        actual_snapshot = read_quarter_notes_ui_snapshot(workbook_path)
        reduced_actual = {quarter: list(actual_snapshot.get(quarter) or []) for quarter in expected_snapshot}
        assert reduced_actual == expected_snapshot
