from __future__ import annotations

import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from pbi_xbrl import excel_vba


class FakeComError(Exception):
    def __init__(self, hresult: int, message: str) -> None:
        super().__init__(hresult, message)
        self.hresult = hresult


class FakePywintypes:
    com_error = FakeComError


class FakePythoncom:
    def __init__(self) -> None:
        self.init_calls = 0
        self.uninit_calls = 0

    def CoInitialize(self) -> None:
        self.init_calls += 1

    def CoUninitialize(self) -> None:
        self.uninit_calls += 1


class FakeCodeModule:
    def __init__(self) -> None:
        self.lines = ""

    @property
    def CountOfLines(self) -> int:
        return len(self.lines.splitlines()) if self.lines else 0

    def AddFromString(self, text: str) -> None:
        self.lines = text

    def DeleteLines(self, start: int, count: int) -> None:
        _ = start, count
        self.lines = ""


class FakeVBComponent:
    def __init__(self, name: str) -> None:
        self.Name = name
        self.CodeModule = FakeCodeModule()


class FakeVBComponents:
    def __init__(self) -> None:
        self._items = [FakeVBComponent("SheetValuation"), FakeVBComponent("modImpliedGT")]

    @property
    def Count(self) -> int:
        return len(self._items)

    def Item(self, index: int) -> FakeVBComponent:
        return self._items[index - 1]

    def Remove(self, component: FakeVBComponent) -> None:
        self._items = [item for item in self._items if item is not component]

    def Add(self, kind: int) -> FakeVBComponent:
        _ = kind
        component = FakeVBComponent(f"StdModule{len(self._items) + 1}")
        self._items.append(component)
        return component

    def __call__(self, name: str) -> FakeVBComponent:
        for item in self._items:
            if item.Name == name:
                return item
        raise KeyError(name)


class FakeVBProject:
    def __init__(self) -> None:
        self.VBComponents = FakeVBComponents()


class FakeWorksheet:
    def __init__(self, code_name: str) -> None:
        self.CodeName = code_name


class FakeWorkbook:
    def __init__(self, behavior: dict[str, Any]) -> None:
        self.behavior = behavior
        self.Name = "source.xlsx"
        self._vbproject = FakeVBProject()
        self._worksheet = FakeWorksheet("SheetValuation")
        self.save_as_calls: list[tuple[str, int]] = []
        self.save_calls = 0
        self.close_calls: list[bool] = []

    @property
    def VBProject(self) -> FakeVBProject:
        exc = self.behavior.get("vbproject_error")
        if exc is not None:
            raise exc
        return self._vbproject

    def SaveAs(self, path: str, FileFormat: int) -> None:
        exc = self.behavior.get("saveas_error")
        if exc is not None:
            raise exc
        self.save_as_calls.append((path, FileFormat))

    def Save(self) -> None:
        remaining = int(self.behavior.get("save_error_calls", 0) or 0)
        if remaining > 0:
            self.behavior["save_error_calls"] = remaining - 1
            raise self.behavior["save_error"]
        self.save_calls += 1

    def Close(self, SaveChanges: bool) -> None:
        self.close_calls.append(bool(SaveChanges))
        exc = self.behavior.get("close_error")
        if exc is not None:
            raise exc

    def Worksheets(self, name: str) -> FakeWorksheet:
        if name != "Valuation":
            raise KeyError(name)
        return self._worksheet


class FakeWorkbooks:
    def __init__(self, app: "FakeExcelApp", behavior: dict[str, Any]) -> None:
        self.app = app
        self.behavior = behavior

    def Open(self, path: str, *args: Any, **kwargs: Any) -> FakeWorkbook:
        _ = path, args, kwargs
        exc = self.behavior.get("open_error")
        if exc is not None:
            raise exc
        workbook = FakeWorkbook(self.behavior)
        self.app.workbook = workbook
        return workbook


class FakeExcelApp:
    def __init__(self, behavior: dict[str, Any]) -> None:
        self.behavior = behavior
        self.Workbooks = FakeWorkbooks(self, behavior)
        self.workbook: FakeWorkbook | None = None
        self.quit_calls = 0
        self.run_calls: list[str] = []
        self.calculate_calls = 0
        self.Visible = True
        self.DisplayAlerts = True
        self.EnableEvents = True
        self.ScreenUpdating = True
        self.AskToUpdateLinks = True
        self.AutomationSecurity = None

    def Run(self, macro_name: str) -> None:
        exc = self.behavior.get("run_error")
        if exc is not None:
            raise exc
        self.run_calls.append(macro_name)

    def CalculateFullRebuild(self) -> None:
        exc = self.behavior.get("calc_error")
        if exc is not None:
            raise exc
        self.calculate_calls += 1

    def Quit(self) -> None:
        self.quit_calls += 1
        exc = self.behavior.get("quit_error")
        if exc is not None:
            raise exc


class FakeWin32:
    def __init__(self, behaviors: list[dict[str, Any]]) -> None:
        self.behaviors = [dict(item) for item in behaviors]
        self.dispatch_names: list[str] = []
        self.apps: list[FakeExcelApp] = []

    def DispatchEx(self, name: str) -> FakeExcelApp:
        self.dispatch_names.append(name)
        if len(self.dispatch_names) > len(self.behaviors):
            raise AssertionError("Unexpected extra DispatchEx call")
        app = FakeExcelApp(self.behaviors[len(self.dispatch_names) - 1])
        self.apps.append(app)
        return app


def _make_source_workbook(tmp_path: Path) -> Path:
    source = tmp_path / "source.xlsx"
    source.write_bytes(b"fake xlsx")
    return source


def _patch_com(monkeypatch: pytest.MonkeyPatch, behaviors: list[dict[str, Any]]) -> tuple[FakeWin32, FakePythoncom]:
    fake_win32 = FakeWin32(behaviors)
    fake_pythoncom = FakePythoncom()
    monkeypatch.setattr(
        excel_vba,
        "_load_com_modules",
        lambda: (fake_win32, fake_pythoncom, FakePywintypes),
    )
    return fake_win32, fake_pythoncom


@contextmanager
def _case_dir() -> Path:
    root = Path(__file__).resolve().parents[2] / ".venv" / "tmp_excel_vba_tests"
    root.mkdir(parents=True, exist_ok=True)
    case_dir = root / uuid4().hex
    case_dir.mkdir()
    try:
        yield case_dir
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def test_inject_valuation_macros_success_logs_steps_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with _case_dir() as tmp_path:
        fake_win32, fake_pythoncom = _patch_com(monkeypatch, [{}])
        source = _make_source_workbook(tmp_path)
        target = tmp_path / "target.xlsm"
        debug_log = tmp_path / "xlsm_injection_debug.log"

        out_path = excel_vba.inject_valuation_macros(source, target, debug_log_path=debug_log)

        assert out_path == target.resolve()
        assert fake_win32.dispatch_names == ["Excel.Application"]
        assert fake_pythoncom.init_calls == 1
        assert fake_pythoncom.uninit_calls == 1
        assert len(fake_win32.apps) == 1
        workbook = fake_win32.apps[0].workbook
        assert workbook is not None
        assert workbook.save_as_calls == [(str(target.resolve()), 52)]
        assert workbook.close_calls == [False]
        assert fake_win32.apps[0].quit_calls == 1
        assert not debug_log.exists()

        stdout = capsys.readouterr().out
        steps = [
            "excel_start",
            "app_configure",
            "workbook_open",
            "saveas_xlsm",
            "vbproject_access",
            "macro_inject",
            "workbook_save",
            "workbook_close",
            "excel_quit",
            "com_cleanup",
        ]
        positions = [stdout.index(f"step={step}") for step in steps]
        assert positions == sorted(positions)


def test_inject_valuation_macros_retries_once_for_session_like_open_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with _case_dir() as tmp_path:
        fake_win32, fake_pythoncom = _patch_com(
            monkeypatch,
            [
                {"open_error": FakeComError(-2147418111, "Call was rejected by callee")},
                {},
            ],
        )
        source = _make_source_workbook(tmp_path)
        target = tmp_path / "target.xlsm"

        out_path = excel_vba.inject_valuation_macros(source, target, debug_log_path=tmp_path / "debug.log")

        assert out_path == target.resolve()
        assert fake_win32.dispatch_names == ["Excel.Application", "Excel.Application"]
        assert fake_pythoncom.init_calls == 2
        assert fake_pythoncom.uninit_calls == 2
        assert len(fake_win32.apps) == 2
        assert fake_win32.apps[0].quit_calls == 1
        assert fake_win32.apps[1].quit_calls == 1

        stdout = capsys.readouterr().out
        assert "Detected session-like COM failure; retrying once with a new Excel instance" in stdout
        assert "Retrying macro injection with a brand-new Excel instance" in stdout


def test_inject_valuation_macros_non_retryable_vbproject_failure_writes_debug_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as tmp_path:
        fake_win32, _ = _patch_com(
            monkeypatch,
            [
                {
                    "vbproject_error": FakeComError(
                        -2147352567,
                        "Programmatic access to Visual Basic Project is not trusted",
                    )
                }
            ],
        )
        source = _make_source_workbook(tmp_path)
        debug_log = tmp_path / "xlsm_injection_debug.log"

        with pytest.raises(excel_vba.MacroInjectionError) as exc_info:
            excel_vba.inject_valuation_macros(source, tmp_path / "target.xlsm", debug_log_path=debug_log)

        err = exc_info.value
        assert err.failed_step == "vbproject_access"
        assert err.retry_attempted is False
        assert err.debug_log_path == debug_log.resolve()
        assert len(fake_win32.apps) == 1

        log_text = debug_log.read_text(encoding="utf-8")
        assert "failed_step: vbproject_access" in log_text
        assert "retry_attempted: 0" in log_text
        assert "step=vbproject_access status=error" in log_text
        assert "Programmatic access to Visual Basic Project is not trusted" in str(err)


def test_inject_valuation_macros_cleanup_failures_do_not_mask_primary_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _case_dir() as tmp_path:
        _patch_com(
            monkeypatch,
            [
                {
                    "vbproject_error": FakeComError(
                        -2147352567,
                        "Programmatic access to Visual Basic Project is not trusted",
                    ),
                    "close_error": RuntimeError("close failed"),
                    "quit_error": RuntimeError("quit failed"),
                }
            ],
        )
        source = _make_source_workbook(tmp_path)
        debug_log = tmp_path / "xlsm_injection_debug.log"

        with pytest.raises(excel_vba.MacroInjectionError) as exc_info:
            excel_vba.inject_valuation_macros(source, tmp_path / "target.xlsm", debug_log_path=debug_log)

        err = exc_info.value
        assert err.failed_step == "vbproject_access"
        log_text = debug_log.read_text(encoding="utf-8")
        assert "step=workbook_close status=error" in log_text
        assert "step=excel_quit status=error" in log_text
        assert "close failed" in log_text
        assert "quit failed" in log_text
