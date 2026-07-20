from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook
import pytest

import pbi_xbrl.new_engine_excel as excel_runtime
from pbi_xbrl.new_engine_excel import ExcelNativeValidationError, run_excel_native_roundtrip


class _Count:
    def __init__(self, count: int) -> None:
        self.Count = count


class _Range:
    Row = 1
    Column = 1

    def __init__(self, value: object = "ok") -> None:
        self.Rows = _Count(1)
        self.Columns = _Count(1)
        self.Value2 = value


class _Cells:
    class _Cell:
        Formula2 = "=1"

    def __call__(self, _row: int, _column: int) -> object:
        return self._Cell()


class _Worksheet:
    def __init__(self, name: str, value: object = "ok") -> None:
        self.Name = name
        self.UsedRange = _Range(value)
        self.Cells = _Cells()


class _Worksheets:
    def __init__(self, sheets: list[_Worksheet]) -> None:
        self._sheets = sheets
        self.Count = len(sheets)

    def __call__(self, index: int) -> _Worksheet:
        return self._sheets[index - 1]


class _Book:
    HasVBProject = False

    def __init__(self, sheets: list[_Worksheet]) -> None:
        self.Worksheets = _Worksheets(sheets)
        self.saved = False
        self.closed = False

    def LinkSources(self, _kind: int) -> None:
        return None

    def Save(self) -> None:
        self.saved = True

    def Close(self, SaveChanges: bool = False) -> None:  # noqa: N803 - COM API spelling
        self.closed = True


class _Workbooks:
    def __init__(self, books: list[_Book]) -> None:
        self.books = books
        self.open_calls: list[dict[str, object]] = []

    def Open(self, path: str, **kwargs: object) -> _Book:  # noqa: N802 - COM API spelling
        self.open_calls.append({"path": path, **kwargs})
        return self.books[len(self.open_calls) - 1]


class _LanguageSettings:
    def LanguageID(self, _kind: int) -> int:  # noqa: N802 - COM API spelling
        return 1053


class _Excel:
    Hwnd = 42

    def __init__(self, books: list[_Book]) -> None:
        self.Workbooks = _Workbooks(books)
        self.LanguageSettings = _LanguageSettings()
        self.calculate_calls = 0
        self.quit_called = False

    def CalculateFullRebuild(self) -> None:  # noqa: N802 - COM API spelling
        self.calculate_calls += 1

    def Quit(self) -> None:  # noqa: N802 - COM API spelling
        self.quit_called = True


def _xlsx(path: Path) -> Path:
    workbook = Workbook()
    workbook.save(path)
    workbook.close()
    return path


def test_excel_roundtrip_opens_twice_recalculates_and_cleans_owned_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _xlsx(tmp_path / "candidate.xlsx")
    books = [_Book([_Worksheet("SUMMARY")]), _Book([_Worksheet("SUMMARY")])]
    excel = _Excel(books)
    waited: list[int] = []
    monkeypatch.setattr(excel_runtime, "_co_initialize", lambda: None)
    monkeypatch.setattr(excel_runtime, "_co_uninitialize", lambda: None)
    monkeypatch.setattr(excel_runtime, "_dispatch_excel", lambda: excel)
    monkeypatch.setattr(excel_runtime, "_excel_process_id", lambda _excel: 1234)
    monkeypatch.setattr(excel_runtime, "_wait_for_owned_process_exit", waited.append)

    result = run_excel_native_roundtrip(path, ticker="TEST", required_locale_id=1053)

    assert result["status"] == "PASS"
    assert result["formula_error_count"] == 0
    assert result["worksheet_scan_count"] == 2
    assert excel.calculate_calls == 2
    assert len(excel.Workbooks.open_calls) == 2
    assert all(call["UpdateLinks"] == 0 and call["CorruptLoad"] == 0 for call in excel.Workbooks.open_calls)
    assert all(book.saved and book.closed for book in books)
    assert excel.quit_called is True
    assert waited == [1234]


def test_excel_roundtrip_fails_on_any_worksheet_error_and_still_cleans_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _xlsx(tmp_path / "candidate.xlsx")
    excel_error = (0x800A0000 + 2023) - (1 << 32)
    books = [_Book([_Worksheet("Valuation", excel_error)]), _Book([_Worksheet("Valuation")])]
    excel = _Excel(books)
    waited: list[int] = []
    monkeypatch.setattr(excel_runtime, "_co_initialize", lambda: None)
    monkeypatch.setattr(excel_runtime, "_co_uninitialize", lambda: None)
    monkeypatch.setattr(excel_runtime, "_dispatch_excel", lambda: excel)
    monkeypatch.setattr(excel_runtime, "_excel_process_id", lambda _excel: 99)
    monkeypatch.setattr(excel_runtime, "_wait_for_owned_process_exit", waited.append)

    with pytest.raises(ExcelNativeValidationError, match="formula errors"):
        run_excel_native_roundtrip(path, ticker="TEST", required_locale_id=1053)

    assert excel.quit_called is True
    assert waited == [99]


@pytest.mark.parametrize(
    ("error_code", "label"),
    (
        (2007, "#DIV/0!"),
        (2045, "#SPILL!"),
        (2046, "#CALC!"),
        (2047, "#CONNECT!"),
        (2048, "#BLOCKED!"),
        (2049, "#UNKNOWN!"),
        (2050, "#FIELD!"),
        (2051, "#DATA!"),
        (2052, "#BUSY!"),
    ),
)
def test_formula_error_scan_recognizes_classic_and_dynamic_com_errors(
    error_code: int,
    label: str,
) -> None:
    signed_value = (0x800A0000 + error_code) - (1 << 32)
    workbook = _Book([_Worksheet("Valuation", signed_value)])

    errors, worksheet_count = excel_runtime._scan_formula_errors(workbook)

    assert worksheet_count == 1
    assert errors == [
        {
            "sheet": "Valuation",
            "cell": "A1",
            "error": label,
            "formula": "=1",
        }
    ]


def test_required_excel_dispatch_failure_is_not_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _xlsx(tmp_path / "candidate.xlsx")
    initialized: list[str] = []
    monkeypatch.setattr(excel_runtime, "_co_initialize", lambda: initialized.append("init"))
    monkeypatch.setattr(excel_runtime, "_co_uninitialize", lambda: initialized.append("uninit"))

    def fail_dispatch() -> object:
        raise RuntimeError("Excel is unavailable")

    monkeypatch.setattr(excel_runtime, "_dispatch_excel", fail_dispatch)
    with pytest.raises(ExcelNativeValidationError, match="RuntimeError.*unavailable"):
        run_excel_native_roundtrip(path, ticker="TEST", required_locale_id=1053)
    assert initialized == ["init", "uninit"]


def test_owned_process_cleanup_failure_is_blocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _xlsx(tmp_path / "candidate.xlsx")
    books = [_Book([_Worksheet("SUMMARY")]), _Book([_Worksheet("SUMMARY")])]
    excel = _Excel(books)
    monkeypatch.setattr(excel_runtime, "_co_initialize", lambda: None)
    monkeypatch.setattr(excel_runtime, "_co_uninitialize", lambda: None)
    monkeypatch.setattr(excel_runtime, "_dispatch_excel", lambda: excel)
    monkeypatch.setattr(excel_runtime, "_excel_process_id", lambda _excel: 1234)

    def fail_cleanup(_process_id: int) -> None:
        raise RuntimeError("owned process remains")

    monkeypatch.setattr(excel_runtime, "_wait_for_owned_process_exit", fail_cleanup)

    with pytest.raises(ExcelNativeValidationError, match="cleanup failed.*owned process remains"):
        run_excel_native_roundtrip(path, ticker="TEST", required_locale_id=1053)
