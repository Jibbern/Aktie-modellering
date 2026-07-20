"""Owned desktop-Excel validation for isolated new-engine workbooks."""
from __future__ import annotations

import gc
from pathlib import Path
from typing import Any, Iterable
import zipfile


class ExcelNativeValidationError(RuntimeError):
    """Raised when required desktop-Excel validation cannot complete safely."""


_EXCEL_ERROR_CODES = {
    2000: "#NULL!",
    2007: "#DIV/0!",
    2015: "#VALUE!",
    2023: "#REF!",
    2029: "#NAME?",
    2036: "#NUM!",
    2042: "#N/A",
    2043: "#GETTING_DATA",
    2045: "#SPILL!",
    2046: "#CALC!",
    2047: "#CONNECT!",
    2048: "#BLOCKED!",
    2049: "#UNKNOWN!",
    2050: "#FIELD!",
    2051: "#DATA!",
    2052: "#BUSY!",
}
_SIGNED_ERROR_VALUES = {(0x800A0000 + code) - (1 << 32): label for code, label in _EXCEL_ERROR_CODES.items()}


def _co_initialize() -> None:
    import pythoncom

    pythoncom.CoInitialize()


def _co_uninitialize() -> None:
    import pythoncom

    pythoncom.CoUninitialize()


def _dispatch_excel() -> Any:
    import win32com.client

    return win32com.client.DispatchEx("Excel.Application")


def _excel_process_id(excel: Any) -> int:
    import win32process

    _thread_id, process_id = win32process.GetWindowThreadProcessId(int(excel.Hwnd))
    return int(process_id)


def _wait_for_owned_process_exit(process_id: int, timeout_seconds: float = 15.0) -> dict[str, Any]:
    """Wait for, and if necessary terminate, exactly the DispatchEx-owned process."""

    import pywintypes
    import win32api
    import win32con
    import win32event

    try:
        handle = win32api.OpenProcess(
            win32con.SYNCHRONIZE | win32con.PROCESS_TERMINATE,
            False,
            int(process_id),
        )
    except pywintypes.error as exc:
        if getattr(exc, "winerror", None) in {87, 1168}:  # already exited / not found
            return {"status": "PASS", "forced_termination": False}
        raise ExcelNativeValidationError(f"Could not inspect owned Excel process {process_id}: {exc}") from exc
    try:
        result = win32event.WaitForSingleObject(handle, int(timeout_seconds * 1000))
        forced = False
        if result == win32event.WAIT_TIMEOUT:
            win32api.TerminateProcess(handle, 1)
            forced = True
            result = win32event.WaitForSingleObject(handle, int(timeout_seconds * 1000))
        if result != win32event.WAIT_OBJECT_0:
            raise ExcelNativeValidationError(f"Owned Excel process {process_id} could not be removed safely.")
        return {"status": "PASS", "forced_termination": forced}
    finally:
        win32api.CloseHandle(handle)


def _iter_used_values(used_range: Any) -> Iterable[tuple[int, int, Any]]:
    rows = int(used_range.Rows.Count)
    columns = int(used_range.Columns.Count)
    values = used_range.Value2
    if rows == 1 and columns == 1:
        yield 1, 1, values
        return
    if rows == 1:
        row_values = values if isinstance(values, (tuple, list)) else (values,)
        if row_values and isinstance(row_values[0], (tuple, list)):
            row_values = row_values[0]
        for column, value in enumerate(row_values, start=1):
            yield 1, column, value
        return
    for row_index, row_values in enumerate(values or (), start=1):
        if not isinstance(row_values, (tuple, list)):
            row_values = (row_values,)
        for column_index, value in enumerate(row_values, start=1):
            yield row_index, column_index, value


def _column_name(column: int) -> str:
    name = ""
    while column:
        column, remainder = divmod(column - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _scan_formula_errors(workbook: Any) -> tuple[list[dict[str, str]], int]:
    errors: list[dict[str, str]] = []
    worksheet_count = int(workbook.Worksheets.Count)
    for sheet_index in range(1, worksheet_count + 1):
        worksheet = workbook.Worksheets(sheet_index)
        used = worksheet.UsedRange
        start_row = int(used.Row)
        start_column = int(used.Column)
        for row_offset, column_offset, value in _iter_used_values(used):
            label = _SIGNED_ERROR_VALUES.get(value) if isinstance(value, int) else None
            if label is None and isinstance(value, str) and value.upper() in set(_EXCEL_ERROR_CODES.values()):
                label = value.upper()
            if label is None:
                continue
            row = start_row + row_offset - 1
            column = start_column + column_offset - 1
            formula = ""
            try:
                formula = str(worksheet.Cells(row, column).Formula2 or "")
            except Exception:
                formula = ""
            errors.append(
                {
                    "sheet": str(worksheet.Name),
                    "cell": f"{_column_name(column)}{row}",
                    "error": label,
                    "formula": formula,
                }
            )
    return errors, worksheet_count


def _inspect_package(path: Path) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(path, "r") as archive:
            names = [name.lower() for name in archive.namelist()]
    except zipfile.BadZipFile as exc:
        raise ExcelNativeValidationError(f"Excel-saved workbook is not a valid XLSX package: {path}") from exc
    macro_parts = [name for name in names if name.endswith("vbaproject.bin")]
    external_link_parts = [name for name in names if name.startswith("xl/externallinks/")]
    recovery_parts = [name for name in names if "recovery" in name]
    if macro_parts or external_link_parts or recovery_parts:
        raise ExcelNativeValidationError(
            "Excel roundtrip introduced forbidden package parts: "
            f"macros={macro_parts!r}, external_links={external_link_parts!r}, recovery={recovery_parts!r}."
        )
    return {
        "macro_part_count": 0,
        "external_link_part_count": 0,
        "recovery_part_count": 0,
    }


def run_excel_native_roundtrip(
    workbook_path: Path | str,
    *,
    ticker: str,
    required_locale_id: int | None = None,
) -> dict[str, Any]:
    """Recalculate, save, reopen and rescan one isolated workbook in owned Excel."""

    path = Path(workbook_path).resolve()
    if not path.is_file() or path.suffix.lower() != ".xlsx":
        raise ExcelNativeValidationError(f"Excel-native validation requires an existing .xlsx file: {path}")

    excel: Any = None
    active_book: Any = None
    process_id: int | None = None
    initialized = False
    worksheet_scan_count = 0
    formula_errors: list[dict[str, str]] = []
    locale_id: int | None = None
    primary_error: BaseException | None = None
    process_cleanup: dict[str, Any] = {"status": "NOT_STARTED", "forced_termination": False}
    try:
        _co_initialize()
        initialized = True
        excel = _dispatch_excel()
        process_id = _excel_process_id(excel)
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.AskToUpdateLinks = False
        locale_id = int(excel.LanguageSettings.LanguageID(2))
        if required_locale_id is not None and locale_id != int(required_locale_id):
            raise ExcelNativeValidationError(
                f"Desktop Excel locale {locale_id} differs from required locale {required_locale_id}."
            )

        for _pass_number in (1, 2):
            active_book = excel.Workbooks.Open(
                str(path),
                UpdateLinks=0,
                ReadOnly=False,
                IgnoreReadOnlyRecommended=True,
                CorruptLoad=0,
            )
            if bool(getattr(active_book, "HasVBProject", False)):
                raise ExcelNativeValidationError("Workbook contains or acquired a VBA project.")
            links = active_book.LinkSources(1)
            if links:
                raise ExcelNativeValidationError(f"Workbook contains external Excel links: {links!r}")
            excel.CalculateFullRebuild()
            errors, scanned = _scan_formula_errors(active_book)
            worksheet_scan_count += scanned
            formula_errors.extend(errors)
            if errors:
                raise ExcelNativeValidationError(f"Desktop Excel found formula errors: {errors[:20]!r}")
            active_book.Save()
            active_book.Close(SaveChanges=False)
            active_book = None
            _inspect_package(path)
    except Exception as exc:
        primary_error = exc
        if isinstance(exc, ExcelNativeValidationError):
            raise
        raise ExcelNativeValidationError(
            f"Desktop Excel validation failed with {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        cleanup_errors: list[str] = []
        if active_book is not None:
            try:
                active_book.Close(SaveChanges=False)
            except Exception as exc:  # pragma: no cover - COM cleanup detail
                cleanup_errors.append(f"workbook_close={exc}")
        if excel is not None:
            try:
                excel.Quit()
            except Exception as exc:  # pragma: no cover - COM cleanup detail
                cleanup_errors.append(f"excel_quit={exc}")
        active_book = None
        excel = None
        gc.collect()
        if initialized:
            try:
                _co_uninitialize()
            except Exception as exc:  # pragma: no cover - COM cleanup detail
                cleanup_errors.append(f"co_uninitialize={exc}")
        if process_id is not None:
            try:
                cleanup_result = _wait_for_owned_process_exit(process_id)
                if isinstance(cleanup_result, dict):
                    process_cleanup = cleanup_result
                else:
                    process_cleanup = {"status": "PASS", "forced_termination": False}
            except Exception as exc:
                cleanup_errors.append(f"process_exit={exc}")
        if cleanup_errors:
            detail = "; ".join(cleanup_errors)
            if primary_error is not None:
                raise ExcelNativeValidationError(
                    f"Desktop Excel validation failed and owned-resource cleanup also failed: {detail}"
                ) from primary_error
            raise ExcelNativeValidationError("Excel cleanup failed: " + detail)

    package = _inspect_package(path)
    return {
        "status": "PASS",
        "ticker": str(ticker).upper(),
        "locale_id": locale_id,
        "recalculation_count": 2,
        "worksheet_scan_count": worksheet_scan_count,
        "formula_error_count": len(formula_errors),
        "formula_errors": formula_errors,
        "owned_process_id": process_id,
        "owned_process_cleanup": "PASS",
        "owned_process_forced_termination": bool(process_cleanup.get("forced_termination")),
        **package,
    }
