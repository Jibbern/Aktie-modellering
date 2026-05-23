"""Saved-workbook validation runner for delivered stock model workbooks.

The checks in this module are intentionally workbook-readback checks.  They
validate the files that a user opens, not only the in-memory writer objects.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from openpyxl import load_workbook


TICKERS: Sequence[str] = ("PBI", "GPRE", "ANF")
ERROR_TOKENS: Sequence[str] = ("#REF!", "#DIV/0!", "#VALUE!", "#NAME?", "#N/A", "#NULL!", "#NUM!")
REQUIRED_NAMED_RANGES: Sequence[str] = (
    "CompanyOperatingMargin_Latest",
    "OperatingMargin_Latest",
    "CompanyOperatingMargin_TTM",
)
BAD_MARKER_TERMS: Sequence[str] = (
    "[UPDATED]",
    "source_txt_file",
    "source_txt",
    "raw_json",
    "DEBUG",
    "TODO",
    "FIXME",
    "Actual / latest actual",
    "Adjusted EPS / EPS",
    "Found:",
    "Base active values",
    "Separate revenue cut; not summed",
)
GUIDANCE_NORMALIZED_MARKER_TICKERS = {"PBI", "GPRE"}
USER_FACING_SHEETS: Sequence[str] = (
    "SUMMARY",
    "Valuation",
    "{ticker}_Investment_Case",
    "Promise_Progress_UI",
    "Quarter_Notes_UI",
    "Operating_Drivers",
)
REQUIRED_SHARED_SHEETS: Sequence[str] = (
    "Valuation",
    "Promise_Progress_UI",
    "Quarter_Notes_UI",
    "History_Q",
    "Operating_Drivers",
    "Needs_Review",
    "QA_Log",
    "QA_Checks",
    "Scenario_Bridge_Tax_Treatment",
    "Scenario_Driver_Assumptions",
    "Quarter_Narrative_Data",
    "BS_Segments",
)
CROSS_COMPANY_PATTERNS: Mapping[str, Sequence[str]] = {
    "PBI": (
        r"\bethanol\b",
        r"\b45Z\b",
        r"\bRINs?\b",
        r"\bRVO\b",
        r"\bcrush margin\b",
        r"\bAbercrombie\b",
        r"\bHollister\b",
    ),
    "GPRE": (
        r"\bPitney Bowes\b",
        r"\bPresort\b",
        r"\bSendTech\b",
        r"\bGEC\b",
        r"\bAbercrombie\b",
        r"\bHollister\b",
    ),
    "ANF": (
        r"\bPitney Bowes\b",
        r"\bPresort\b",
        r"\bSendTech\b",
        r"\bGEC\b",
        r"\bethanol\b",
        r"\b45Z\b",
        r"\bRINs?\b",
        r"\bRVO\b",
        r"\bcrush margin\b",
    ),
}
BAD_QUARTER_LABEL_PATTERNS: Sequence[str] = (
    r"\bQ[1-4]\s+FY\s*20\d{2}\b",
    r"\bQ[1-4]\s+20\d{2}\b",
    r"\bFY\s*20\d{2}\s+Q[1-4]\b",
    r"\b20\d{2}\s+Q[1-4]\b",
)
GOOD_QUARTER_LABEL_PATTERN = re.compile(r"\b20\d{2}-Q[1-4]\b")


@dataclass(frozen=True)
class ValidationIssue:
    category: str
    sheet: str = ""
    cell: str = ""
    value: str = ""
    detail: str = ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "category": self.category,
            "sheet": self.sheet,
            "cell": self.cell,
            "value": self.value,
            "detail": self.detail,
        }


@dataclass
class WorkbookValidationResult:
    ticker: str
    path: str
    formula_error_count: int = 0
    needs_review_p1_count: int = 0
    qa_blank_nan_status_count: int = 0
    cross_company_leakage_count: int = 0
    bad_marker_count: int = 0
    quarter_label_issue_count: int = 0
    missing_required_sheets: List[str] = field(default_factory=list)
    missing_named_ranges: List[str] = field(default_factory=list)
    calc_settings_ok: bool = True
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def required_sheets_ok(self) -> bool:
        return not self.missing_required_sheets

    @property
    def named_ranges_ok(self) -> bool:
        return not self.missing_named_ranges

    @property
    def overall(self) -> str:
        counters_ok = all(
            count == 0
            for count in [
                self.formula_error_count,
                self.needs_review_p1_count,
                self.qa_blank_nan_status_count,
                self.cross_company_leakage_count,
                self.bad_marker_count,
                self.quarter_label_issue_count,
            ]
        )
        return "PASS" if counters_ok and self.required_sheets_ok and self.named_ranges_ok and self.calc_settings_ok else "FAIL"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "path": self.path,
            "formula_errors": self.formula_error_count,
            "needs_review_p1": self.needs_review_p1_count,
            "qa_blank_nan": self.qa_blank_nan_status_count,
            "cross_company_leakage": self.cross_company_leakage_count,
            "bad_markers": self.bad_marker_count,
            "quarter_label_issues": self.quarter_label_issue_count,
            "missing_required_sheets": self.missing_required_sheets,
            "missing_named_ranges": self.missing_named_ranges,
            "calc_settings_ok": self.calc_settings_ok,
            "overall": self.overall,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _required_sheets_for_ticker(ticker: str) -> List[str]:
    return [*REQUIRED_SHARED_SHEETS, f"{ticker}_Investment_Case"]


def _user_facing_sheets_for_ticker(ticker: str, sheetnames: Sequence[str]) -> List[str]:
    out: List[str] = []
    for sheet in USER_FACING_SHEETS:
        resolved = sheet.format(ticker=ticker)
        if resolved in sheetnames:
            out.append(resolved)
    return out


def _append_issue(result: WorkbookValidationResult, issue: ValidationIssue) -> None:
    result.issues.append(issue)


def _iter_visible_cells(wb: Any, sheet_names: Optional[Iterable[str]] = None) -> Iterable[Any]:
    names = list(sheet_names) if sheet_names is not None else list(wb.sheetnames)
    for sheet_name in names:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        if ws.sheet_state != "visible":
            continue
        for row in ws.iter_rows():
            for cell in row:
                yield cell


def _scan_formula_errors(wb: Any, result: WorkbookValidationResult) -> None:
    for cell in _iter_visible_cells(wb):
        value = _cell_text(cell.value)
        if not value:
            continue
        for token in ERROR_TOKENS:
            if token in value:
                result.formula_error_count += 1
                _append_issue(
                    result,
                    ValidationIssue(
                        category="formula_error",
                        sheet=cell.parent.title,
                        cell=cell.coordinate,
                        value=value,
                        detail=f"{cell.parent.title}!{cell.coordinate} contains {token}: {value}",
                    ),
                )
                break


def _header_map(ws: Any) -> Dict[str, int]:
    return {
        _cell_text(ws.cell(1, cc).value).strip().lower(): cc
        for cc in range(1, int(ws.max_column or 0) + 1)
        if _cell_text(ws.cell(1, cc).value).strip()
    }


def _count_needs_review_p1(wb: Any, result: WorkbookValidationResult) -> None:
    if "Needs_Review" not in wb.sheetnames:
        return
    ws = wb["Needs_Review"]
    headers = _header_map(ws)
    cols = [
        col
        for header, col in headers.items()
        if "priority" in header or header in {"severity", "level"}
    ] or list(range(1, min(int(ws.max_column or 0), 8) + 1))
    for rr in range(2, int(ws.max_row or 0) + 1):
        for cc in cols:
            value = _cell_text(ws.cell(rr, cc).value).strip()
            if value.upper() == "P1":
                result.needs_review_p1_count += 1
                _append_issue(
                    result,
                    ValidationIssue(
                        category="needs_review_p1",
                        sheet=ws.title,
                        cell=ws.cell(rr, cc).coordinate,
                        value=value,
                        detail=f"{ws.title}!{ws.cell(rr, cc).coordinate} has P1 Needs_Review priority.",
                    ),
                )
                break


def _count_qa_blank_nan_status(wb: Any, result: WorkbookValidationResult) -> None:
    for sheet_name in wb.sheetnames:
        if not sheet_name.startswith("QA"):
            continue
        ws = wb[sheet_name]
        if ws.sheet_state != "visible":
            continue
        headers = _header_map(ws)
        status_cols = [col for header, col in headers.items() if "status" in header]
        if not status_cols:
            continue
        for rr in range(2, int(ws.max_row or 0) + 1):
            row_has_data = any(
                _cell_text(ws.cell(rr, cc).value).strip()
                for cc in range(1, min(int(ws.max_column or 0), 8) + 1)
            )
            if not row_has_data:
                continue
            for cc in status_cols:
                value = _cell_text(ws.cell(rr, cc).value).strip()
                if value.lower() in {"", "nan", "none", "null"}:
                    result.qa_blank_nan_status_count += 1
                    _append_issue(
                        result,
                        ValidationIssue(
                            category="qa_blank_nan_status",
                            sheet=ws.title,
                            cell=ws.cell(rr, cc).coordinate,
                            value=value,
                            detail=f"{ws.title}!{ws.cell(rr, cc).coordinate} has blank/nan QA status: {value!r}",
                        ),
                    )


def _scan_cross_company_leakage(wb: Any, ticker: str, result: WorkbookValidationResult) -> None:
    patterns = [re.compile(pattern, flags=re.IGNORECASE) for pattern in CROSS_COMPANY_PATTERNS.get(ticker, ())]
    if not patterns:
        return
    for cell in _iter_visible_cells(wb, _user_facing_sheets_for_ticker(ticker, wb.sheetnames)):
        value = _cell_text(cell.value)
        if not value:
            continue
        for pattern in patterns:
            if pattern.search(value):
                result.cross_company_leakage_count += 1
                _append_issue(
                    result,
                    ValidationIssue(
                        category="cross_company_leakage",
                        sheet=cell.parent.title,
                        cell=cell.coordinate,
                        value=value,
                        detail=f"{cell.parent.title}!{cell.coordinate} matches forbidden {pattern.pattern}: {value}",
                    ),
                )


def _bad_marker_terms_for_ticker(ticker: str) -> List[str]:
    terms = list(BAD_MARKER_TERMS)
    if ticker in GUIDANCE_NORMALIZED_MARKER_TICKERS:
        terms.append("Guidance_Normalized")
    return terms


def _scan_bad_markers(wb: Any, ticker: str, result: WorkbookValidationResult) -> None:
    terms = _bad_marker_terms_for_ticker(ticker)
    for cell in _iter_visible_cells(wb, _user_facing_sheets_for_ticker(ticker, wb.sheetnames)):
        value = _cell_text(cell.value)
        if not value:
            continue
        low = value.lower()
        for term in terms:
            if term.lower() in low:
                result.bad_marker_count += 1
                _append_issue(
                    result,
                    ValidationIssue(
                        category="bad_marker",
                        sheet=cell.parent.title,
                        cell=cell.coordinate,
                        value=value,
                        detail=f"{cell.parent.title}!{cell.coordinate} contains bad marker {term!r}: {value}",
                    ),
                )


def _scan_quarter_labels(wb: Any, ticker: str, result: WorkbookValidationResult) -> None:
    patterns = [re.compile(pattern, flags=re.IGNORECASE) for pattern in BAD_QUARTER_LABEL_PATTERNS]
    for cell in _iter_visible_cells(wb, _user_facing_sheets_for_ticker(ticker, wb.sheetnames)):
        value = _cell_text(cell.value)
        if not value:
            continue
        for pattern in patterns:
            if pattern.search(value):
                # Keep the rule tight: only flag if there is no accepted YYYY-QN
                # label in the same visible cell.
                if GOOD_QUARTER_LABEL_PATTERN.search(value):
                    continue
                result.quarter_label_issue_count += 1
                _append_issue(
                    result,
                    ValidationIssue(
                        category="quarter_label",
                        sheet=cell.parent.title,
                        cell=cell.coordinate,
                        value=value,
                        detail=f"{cell.parent.title}!{cell.coordinate} has non-standard quarter label: {value}",
                    ),
                )
                break


def _check_required_sheets(wb: Any, ticker: str, result: WorkbookValidationResult) -> None:
    for sheet in _required_sheets_for_ticker(ticker):
        if sheet not in wb.sheetnames:
            result.missing_required_sheets.append(sheet)
            _append_issue(
                result,
                ValidationIssue(
                    category="required_sheet",
                    sheet=sheet,
                    detail=f"Missing required sheet: {sheet}",
                ),
            )


def _check_named_ranges(wb: Any, result: WorkbookValidationResult) -> None:
    names = set(wb.defined_names.keys())
    for name in REQUIRED_NAMED_RANGES:
        if name not in names:
            result.missing_named_ranges.append(name)
            _append_issue(
                result,
                ValidationIssue(
                    category="named_range",
                    value=name,
                    detail=f"Missing expected named range: {name}",
                ),
            )


def _check_calc_settings(wb: Any, result: WorkbookValidationResult) -> None:
    calc = wb.calculation
    calc_mode = getattr(calc, "calcMode", None)
    full_calc = bool(getattr(calc, "fullCalcOnLoad", False))
    force_full = bool(getattr(calc, "forceFullCalc", False))
    result.calc_settings_ok = (calc_mode in {None, "auto"}) and full_calc and force_full
    if not result.calc_settings_ok:
        _append_issue(
            result,
            ValidationIssue(
                category="calc_settings",
                value=f"calcMode={calc_mode}; fullCalcOnLoad={full_calc}; forceFullCalc={force_full}",
                detail="Workbook calculation settings should be automatic with fullCalcOnLoad and forceFullCalc enabled.",
            ),
        )


def validate_workbook(path: Path | str, ticker: str) -> WorkbookValidationResult:
    ticker_txt = str(ticker or "").strip().upper()
    workbook_path = Path(path)
    result = WorkbookValidationResult(ticker=ticker_txt, path=str(workbook_path))
    if not workbook_path.exists():
        _append_issue(
            result,
            ValidationIssue(
                category="workbook_missing",
                value=str(workbook_path),
                detail=f"Workbook does not exist: {workbook_path}",
            ),
        )
        result.missing_required_sheets = list(_required_sheets_for_ticker(ticker_txt))
        return result

    wb = load_workbook(workbook_path, data_only=False, read_only=False)
    try:
        _check_required_sheets(wb, ticker_txt, result)
        _scan_formula_errors(wb, result)
        _count_needs_review_p1(wb, result)
        _count_qa_blank_nan_status(wb, result)
        _scan_cross_company_leakage(wb, ticker_txt, result)
        _scan_bad_markers(wb, ticker_txt, result)
        _scan_quarter_labels(wb, ticker_txt, result)
        _check_named_ranges(wb, result)
        _check_calc_settings(wb, result)
    finally:
        wb.close()
    return result


def validate_workbooks(paths_by_ticker: Mapping[str, Path | str]) -> List[WorkbookValidationResult]:
    results: List[WorkbookValidationResult] = []
    for ticker in TICKERS:
        if ticker in paths_by_ticker:
            results.append(validate_workbook(paths_by_ticker[ticker], ticker))
    for ticker, path in paths_by_ticker.items():
        ticker_txt = str(ticker).upper()
        if ticker_txt not in TICKERS:
            results.append(validate_workbook(path, ticker_txt))
    return results


def summary_rows(results: Sequence[WorkbookValidationResult]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for result in results:
        rows.append(
            {
                "Ticker": result.ticker,
                "Formula errors": result.formula_error_count,
                "Needs_Review P1": result.needs_review_p1_count,
                "QA blank/nan": result.qa_blank_nan_status_count,
                "Cross-company leakage": result.cross_company_leakage_count,
                "Bad markers": result.bad_marker_count,
                "Required sheets": "pass" if result.required_sheets_ok else f"missing {len(result.missing_required_sheets)}",
                "Named ranges": "pass" if result.named_ranges_ok else f"missing {len(result.missing_named_ranges)}",
                "Calc flags": "pass" if result.calc_settings_ok else "fail",
                "Overall": result.overall,
            }
        )
    return rows


def format_summary_table(results: Sequence[WorkbookValidationResult]) -> str:
    rows = summary_rows(results)
    headers = [
        "Ticker",
        "Formula errors",
        "Needs_Review P1",
        "QA blank/nan",
        "Cross-company leakage",
        "Bad markers",
        "Required sheets",
        "Named ranges",
        "Calc flags",
        "Overall",
    ]
    widths = {header: max(len(header), *(len(str(row[header])) for row in rows)) for header in headers}
    lines = [" | ".join(header.ljust(widths[header]) for header in headers)]
    lines.append("-+-".join("-" * widths[header] for header in headers))
    for row in rows:
        lines.append(" | ".join(str(row[header]).ljust(widths[header]) for header in headers))
    return "\n".join(lines)


def write_validation_reports(
    results: Sequence[WorkbookValidationResult],
    output_dir: Path | str,
) -> Dict[str, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "workbook_validation_report.json"
    csv_path = out_dir / "workbook_validation_summary.csv"
    json_path.write_text(
        json.dumps([result.to_dict() for result in results], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    rows = summary_rows(results)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    return {"json": json_path, "csv": csv_path}


def default_workbook_paths(workbook_dir: Path | str) -> Dict[str, Path]:
    root = Path(workbook_dir)
    return {ticker: root / f"{ticker}_model.xlsx" for ticker in TICKERS}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Validate saved stock model workbooks.")
    parser.add_argument(
        "--workbook-dir",
        default=str(Path.cwd().parent / "Excel stock models"),
        help="Directory containing PBI_model.xlsx, GPRE_model.xlsx and ANF_model.xlsx.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path.cwd() / "validation_reports" / "workbook_validation"),
        help="Directory where JSON/CSV validation reports are written.",
    )
    parser.add_argument("--tickers", nargs="*", default=list(TICKERS), help="Tickers to validate.")
    args = parser.parse_args(argv)

    paths = {
        str(ticker).upper(): Path(args.workbook_dir) / f"{str(ticker).upper()}_model.xlsx"
        for ticker in args.tickers
    }
    results = validate_workbooks(paths)
    report_paths = write_validation_reports(results, args.output_dir)
    print(format_summary_table(results))
    print(f"\nJSON report: {report_paths['json']}")
    print(f"CSV report: {report_paths['csv']}")
    return 0 if all(result.overall == "PASS" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
