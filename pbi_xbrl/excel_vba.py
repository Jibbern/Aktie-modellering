from __future__ import annotations

import gc
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


STD_MODULE_CODE = r'''
Option Explicit

Public Sub UpdateImpliedGT()
    Dim ws As Worksheet: Set ws = ThisWorkbook.Worksheets("Valuation")

    On Error Resume Next
    Dim autoFlag As Variant
    autoFlag = ws.Range("AutoImpliedGT").Value
    If VarType(autoFlag) = vbBoolean Then
        If autoFlag = False Then Exit Sub
    End If
    On Error GoTo 0

    Dim marketEV As Variant
    marketEV = ws.Range("EV").Value
    If IsEmpty(marketEV) Or marketEV <= 0 Then Exit Sub

    Dim baseWacc As Double, baseGT As Double
    baseWacc = ws.Range("J220").Value
    baseGT = ws.Range("J219").Value

    Dim waccRng As Range, gtRng As Range, stRng As Range
    Set waccRng = ws.Range("ImpliedGT_WACC")
    Set gtRng = ws.Range("ImpliedGT_Output")
    Set stRng = ws.Range("ImpliedGT_Status")

    gtRng.ClearContents
    stRng.ClearContents

    Dim prevCalc As XlCalculation
    prevCalc = Application.Calculation

    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationAutomatic

    Dim i As Long
    For i = 1 To waccRng.Rows.Count
        Dim w As Double: w = waccRng.Cells(i, 1).Value
        If w <= 0 Then
            stRng.Cells(i, 1).Value = "SKIP"
            GoTo ContinueLoop
        End If

        ws.Range("J220").Value = w

        Dim guess As Double: guess = baseGT
        If guess >= w - 0.005 Then guess = w - 0.01
        If guess < -0.05 Then guess = -0.05
        ws.Range("J219").Value = guess

        Dim ok As Boolean
        On Error Resume Next
        ok = ws.Range("J221").GoalSeek(Goal:=marketEV, ChangingCell:=ws.Range("J219"))
        On Error GoTo 0

        Dim solvedGT As Double: solvedGT = ws.Range("J219").Value
        If (ok = False) Or (solvedGT >= w - 0.001) Or (solvedGT > 0.06) Or (solvedGT < -0.1) Then
            stRng.Cells(i, 1).Value = "FAIL"
        Else
            gtRng.Cells(i, 1).Value = solvedGT
            stRng.Cells(i, 1).Value = "OK"
        End If

ContinueLoop:
    Next i

Cleanup:
    ws.Range("J220").Value = baseWacc
    ws.Range("J219").Value = baseGT
    Application.Calculation = prevCalc
    Application.EnableEvents = True
    Application.ScreenUpdating = True
End Sub
'''.strip()


SHEET_EVENT_CODE = r'''
Option Explicit

Private Sub Worksheet_Change(ByVal Target As Range)
    On Error GoTo SafeExit
    If Target Is Nothing Then Exit Sub
    If Target.CountLarge > 1 Then Exit Sub
    If Intersect(Target, Me.Range("Price")) Is Nothing Then Exit Sub

    Application.EnableEvents = False
    Call UpdateImpliedGT

SafeExit:
    Application.EnableEvents = True
End Sub
'''.strip()


_RETRYABLE_STEPS = {"excel_start", "app_configure", "workbook_open", "saveas_xlsm", "workbook_save"}
_RETRYABLE_HRESULTS = {-2147418111, -2147417848, -2147023174, -2147023170}
_RETRYABLE_MESSAGE_SNIPPETS = (
    "call was rejected by callee",
    "application is busy",
    "server execution failed",
    "rpc server is unavailable",
    "remote procedure call failed",
    "disconnected from its clients",
    "open method of workbooks class failed",
    "saveas method of workbook class failed",
    "save method of workbook class failed",
    "document not saved",
)


class MacroInjectionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        failed_step: str,
        debug_log_path: Optional[Path],
        retry_attempted: bool,
        original_exception: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.failed_step = failed_step
        self.debug_log_path = debug_log_path
        self.retry_attempted = retry_attempted
        self.original_exception = original_exception


class _AttemptFailure(Exception):
    def __init__(self, failed_step: str, exc: BaseException) -> None:
        super().__init__(f"{failed_step}: {type(exc).__name__}: {exc}")
        self.failed_step = failed_step
        self.exc = exc


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _flatten_message(value: Any) -> str:
    text = str(value or "").strip()
    return " ".join(text.split())


def _append_trace(
    trace: list[dict[str, Any]],
    *,
    attempt: int,
    step: str,
    status: str,
    message: str,
    exc: Optional[BaseException] = None,
) -> None:
    event: dict[str, Any] = {
        "timestamp": _now_iso(),
        "attempt": attempt,
        "step": step,
        "status": status,
        "message": message,
    }
    if exc is not None:
        event["exception_type"] = type(exc).__name__
        event["exception_message"] = _flatten_message(exc)
    trace.append(event)

    parts = [f"[excel_vba] attempt={attempt}", f"step={step}", f"status={status}"]
    if message:
        parts.append(message)
    if exc is not None:
        parts.append(f"{type(exc).__name__}: {_flatten_message(exc)}")
    print(" | ".join(parts), flush=True)


def _load_com_modules() -> tuple[Any, Any, Any]:
    try:
        import pythoncom  # type: ignore[import-not-found]
        import win32com.client as win32  # type: ignore[import-not-found]
    except Exception as exc:
        raise RuntimeError(
            "pywin32 is not installed. Install with: .\\.venv\\Scripts\\python.exe -m pip install pywin32"
        ) from exc

    try:
        import pywintypes  # type: ignore[import-not-found]
    except Exception:
        pywintypes = None
    return win32, pythoncom, pywintypes


def _extract_hresult(exc: BaseException) -> Optional[int]:
    for attr in ("hresult", "HRESULT"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    args = getattr(exc, "args", ())
    if args:
        first = args[0]
        if isinstance(first, int):
            return first
        if isinstance(first, tuple) and first and isinstance(first[0], int):
            return first[0]
    return None


def _is_com_error(exc: BaseException, pywintypes_module: Any) -> bool:
    if pywintypes_module is not None:
        com_error = getattr(pywintypes_module, "com_error", None)
        if com_error is not None and isinstance(exc, com_error):
            return True
    return exc.__class__.__name__.lower() == "com_error"


def _is_retryable_session_error(exc: BaseException, *, failed_step: str, pywintypes_module: Any) -> bool:
    if failed_step not in _RETRYABLE_STEPS:
        return False

    message = _flatten_message(exc).lower()
    hresult = _extract_hresult(exc)
    message_match = any(snippet in message for snippet in _RETRYABLE_MESSAGE_SNIPPETS)
    if not _is_com_error(exc, pywintypes_module) and hresult is None and not message_match:
        return False
    if hresult in _RETRYABLE_HRESULTS:
        return True
    return message_match


def _wrap_vbproject_access_error(exc: BaseException) -> RuntimeError:
    detail = _flatten_message(exc).lower()
    if "not trusted" in detail or "project object model" in detail or "access denied" in detail:
        return RuntimeError(
            "VBA project access denied. Enable Excel Trust Center setting: "
            "'Trust access to the VBA project object model'. "
            f"Original error: {type(exc).__name__}: {_flatten_message(exc)}"
        )
    return RuntimeError(f"Failed to access workbook.VBProject: {type(exc).__name__}: {_flatten_message(exc)}")


def _write_debug_log(
    debug_log_path: Path,
    *,
    source_path: Path,
    target_path: Path,
    failed_step: str,
    exc: BaseException,
    retry_attempted: bool,
    trace: list[dict[str, Any]],
) -> None:
    debug_log_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"timestamp: {_now_iso()}",
        f"source_path: {source_path}",
        f"target_path: {target_path}",
        f"failed_step: {failed_step}",
        f"retry_attempted: {int(bool(retry_attempted))}",
        f"exception_type: {type(exc).__name__}",
        f"exception_message: {_flatten_message(exc)}",
        "",
        "trace:",
    ]
    for event in trace:
        line = (
            f"[{event['timestamp']}] attempt={event['attempt']} step={event['step']} "
            f"status={event['status']} message={event['message']}"
        )
        if "exception_type" in event:
            line += f" exception={event['exception_type']}: {event['exception_message']}"
        lines.append(line)
    debug_log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _should_use_dynamic_dispatch_fallback(exc: BaseException) -> bool:
    message = _flatten_message(exc).lower()
    return isinstance(exc, AttributeError) and (
        "clsidtoclassmap" in message or "clsidtopackagemap" in message
    )


def _dynamic_dumb_dispatch(win32: Any, dispatch: Any, *, user_name: str, clsctx: Any) -> Any:
    base_dispatch = win32.dynamic.CDispatch

    class _RecursiveDumbDispatch(base_dispatch):
        def _wrap_dispatch_(self, ob: Any, userName: Optional[str] = None, returnCLSID: Any = None) -> Any:
            _ = returnCLSID
            return win32.dynamic.DumbDispatch(
                ob,
                userName,
                createClass=_RecursiveDumbDispatch,
                clsctx=clsctx,
            )

    return win32.dynamic.DumbDispatch(
        dispatch,
        userName=user_name,
        createClass=_RecursiveDumbDispatch,
        clsctx=clsctx,
    )


def _repair_gen_py_cache(*, attempt: int, trace: list[dict[str, Any]]) -> bool:
    try:
        import win32com
        import win32com.client.gencache as gencache  # type: ignore[import-not-found]
    except Exception as exc:
        _append_trace(
            trace,
            attempt=attempt,
            step="excel_start",
            status="warn",
            message="Could not import pywin32 gencache for repair",
            exc=exc,
        )
        return False

    try:
        gen_path = Path(gencache.GetGeneratePath())
        _append_trace(
            trace,
            attempt=attempt,
            step="excel_start",
            status="warn",
            message=f"Resetting pywin32 gen_py cache at {gen_path}",
        )
        shutil.rmtree(gen_path, ignore_errors=True)
        if hasattr(win32com, "__gen_path__"):
            Path(win32com.__gen_path__).mkdir(parents=True, exist_ok=True)
        gencache.GetGeneratePath()
        gencache.Rebuild(verbose=0)
        _append_trace(
            trace,
            attempt=attempt,
            step="excel_start",
            status="ok",
            message=f"Reset pywin32 gen_py cache at {gen_path}",
        )
        return True
    except Exception as exc:
        _append_trace(
            trace,
            attempt=attempt,
            step="excel_start",
            status="warn",
            message="pywin32 gen_py cache reset failed; continuing to dynamic fallback",
            exc=exc,
        )
        return False


def _start_excel_application(
    *,
    attempt: int,
    trace: list[dict[str, Any]],
    win32: Any,
    pythoncom: Any,
) -> Any:
    try:
        return win32.DispatchEx("Excel.Application")
    except Exception as exc:
        if not _should_use_dynamic_dispatch_fallback(exc):
            raise
        _append_trace(
            trace,
            attempt=attempt,
            step="excel_start",
            status="warn",
            message="DispatchEx makepy startup failed; retrying with dynamic COM dispatch",
            exc=exc,
        )
        repaired = _repair_gen_py_cache(attempt=attempt, trace=trace)
        if repaired:
            try:
                return win32.DispatchEx("Excel.Application")
            except Exception as retry_exc:
                if not _should_use_dynamic_dispatch_fallback(retry_exc):
                    raise
                _append_trace(
                    trace,
                    attempt=attempt,
                    step="excel_start",
                    status="warn",
                    message="DispatchEx still failed after gen_py reset; falling back to dynamic COM dispatch",
                    exc=retry_exc,
                )
        clsctx = getattr(pythoncom, "CLSCTX_LOCAL_SERVER", getattr(pythoncom, "CLSCTX_SERVER"))
        dispatch = pythoncom.CoCreateInstanceEx(
            "Excel.Application",
            None,
            clsctx,
            None,
            (pythoncom.IID_IDispatch,),
        )[0]
        return _dynamic_dumb_dispatch(win32, dispatch, user_name="Excel.Application", clsctx=clsctx)


def _remove_partial_target(target_path: Path, *, attempt: int, trace: list[dict[str, Any]]) -> None:
    if not target_path.exists():
        return
    try:
        target_path.unlink()
        _append_trace(
            trace,
            attempt=attempt,
            step="saveas_xlsm",
            status="retry",
            message=f"Removed partial target before retry: {target_path}",
        )
    except Exception as exc:
        _append_trace(
            trace,
            attempt=attempt,
            step="saveas_xlsm",
            status="warn",
            message=f"Could not remove partial target before retry: {target_path}",
            exc=exc,
        )


def _run_injection_attempt(
    source_path: Path,
    target_path: Path,
    worksheet_name: str,
    *,
    attempt: int,
    trace: list[dict[str, Any]],
    win32: Any,
    pythoncom: Any,
) -> Path:
    excel = None
    wb = None
    vbproj = None
    std_mod = None
    ws_obj = None
    ws_comp = None
    code_mod = None
    primary_failure: Optional[_AttemptFailure] = None
    com_initialized = False
    current_step = "excel_start"

    try:
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message="Starting dedicated Excel COM instance",
        )
        pythoncom.CoInitialize()
        com_initialized = True
        excel = _start_excel_application(
            attempt=attempt,
            trace=trace,
            win32=win32,
            pythoncom=pythoncom,
        )
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message="Dedicated Excel COM instance started",
        )

        current_step = "app_configure"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message="Configuring Excel automation state",
        )
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.EnableEvents = False
        excel.ScreenUpdating = False
        excel.AskToUpdateLinks = False
        automation_bits = [
            "Visible=False",
            "DisplayAlerts=False",
            "EnableEvents=False",
            "ScreenUpdating=False",
            "AskToUpdateLinks=False",
        ]
        try:
            excel.AutomationSecurity = 3
            automation_bits.append("AutomationSecurity=3")
        except Exception as exc:
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="warn",
                message="AutomationSecurity not available; continuing",
                exc=exc,
            )
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message=", ".join(automation_bits),
        )

        current_step = "workbook_open"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message=f"Opening workbook: {source_path}",
        )
        wb = excel.Workbooks.Open(
            str(source_path),
            0,
            False,
            None,
            None,
            None,
            True,
            None,
            None,
            None,
            False,
            None,
            False,
        )
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message=f"Workbook opened: {source_path}",
        )

        current_step = "saveas_xlsm"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message=f"Saving workbook as macro-enabled: {target_path}",
        )
        wb.SaveAs(str(target_path), 52)
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message=f"Workbook saved as macro-enabled: {target_path}",
        )

        current_step = "vbproject_access"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message="Accessing workbook VBProject",
        )
        try:
            vbproj = wb.VBProject
        except Exception as exc:
            raise _wrap_vbproject_access_error(exc) from exc
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message="Workbook VBProject accessed",
        )

        current_step = "macro_inject"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message="Replacing VBA standard and worksheet modules",
        )
        to_remove = []
        for i in range(1, vbproj.VBComponents.Count + 1):
            comp = vbproj.VBComponents.Item(i)
            if comp.Name == "modImpliedGT":
                to_remove.append(comp)
        for comp in to_remove:
            vbproj.VBComponents.Remove(comp)

        std_mod = vbproj.VBComponents.Add(1)  # vbext_ct_StdModule
        std_mod.Name = "modImpliedGT"
        std_mod.CodeModule.AddFromString(STD_MODULE_CODE)

        ws_obj = wb.Worksheets(worksheet_name)
        ws_comp = vbproj.VBComponents(ws_obj.CodeName)
        code_mod = ws_comp.CodeModule
        if code_mod.CountOfLines > 0:
            code_mod.DeleteLines(1, code_mod.CountOfLines)
        code_mod.AddFromString(SHEET_EVENT_CODE)
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message=f"Injected macros into worksheet '{worksheet_name}'",
        )

        current_step = "workbook_save"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message="Saving workbook after VBA injection",
        )
        wb.Save()
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="ok",
            message="Workbook saved after VBA injection",
        )

        _append_trace(
            trace,
            attempt=attempt,
            step="macro_smoke",
            status="start",
            message="Running best-effort UpdateImpliedGT smoke step",
        )
        try:
            excel.Run(f"'{wb.Name}'!UpdateImpliedGT")
            try:
                excel.CalculateFullRebuild()
            except Exception as exc:
                _append_trace(
                    trace,
                    attempt=attempt,
                    step="macro_smoke",
                    status="warn",
                    message="CalculateFullRebuild failed during smoke step; continuing",
                    exc=exc,
                )
            try:
                wb.Save()
            except Exception as exc:
                _append_trace(
                    trace,
                    attempt=attempt,
                    step="macro_smoke",
                    status="warn",
                    message="Workbook save failed during smoke step; continuing",
                    exc=exc,
                )
            _append_trace(
                trace,
                attempt=attempt,
                step="macro_smoke",
                status="ok",
                message="Smoke step completed",
            )
        except Exception as exc:
            _append_trace(
                trace,
                attempt=attempt,
                step="macro_smoke",
                status="warn",
                message="Smoke step failed but macro injection was preserved",
                exc=exc,
            )
    except Exception as exc:
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="error",
            message="Macro injection step failed",
            exc=exc,
        )
        primary_failure = _AttemptFailure(current_step, exc)
    finally:
        current_step = "workbook_close"
        if wb is None:
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="skip",
                message="Workbook was never opened; close skipped",
            )
        else:
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="start",
                message="Closing workbook",
            )
            try:
                wb.Close(False)
                _append_trace(
                    trace,
                    attempt=attempt,
                    step=current_step,
                    status="ok",
                    message="Workbook closed",
                )
            except Exception as exc:
                _append_trace(
                    trace,
                    attempt=attempt,
                    step=current_step,
                    status="error",
                    message="Workbook close failed",
                    exc=exc,
                )
                if primary_failure is None:
                    primary_failure = _AttemptFailure(current_step, exc)

        current_step = "excel_quit"
        if excel is None:
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="skip",
                message="Excel instance was never created; quit skipped",
            )
        else:
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="start",
                message="Quitting dedicated Excel instance",
            )
            try:
                excel.Quit()
                _append_trace(
                    trace,
                    attempt=attempt,
                    step=current_step,
                    status="ok",
                    message="Dedicated Excel instance quit",
                )
            except Exception as exc:
                _append_trace(
                    trace,
                    attempt=attempt,
                    step=current_step,
                    status="error",
                    message="Excel quit failed",
                    exc=exc,
                )
                if primary_failure is None:
                    primary_failure = _AttemptFailure(current_step, exc)

        current_step = "com_cleanup"
        _append_trace(
            trace,
            attempt=attempt,
            step=current_step,
            status="start",
            message="Releasing COM references and uninitializing COM",
        )
        try:
            code_mod = None
            ws_comp = None
            ws_obj = None
            std_mod = None
            vbproj = None
            wb = None
            excel = None
            gc.collect()
            if com_initialized:
                pythoncom.CoUninitialize()
                com_initialized = False
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="ok",
                message="COM references released",
            )
        except Exception as exc:
            _append_trace(
                trace,
                attempt=attempt,
                step=current_step,
                status="error",
                message="COM cleanup failed",
                exc=exc,
            )
            if primary_failure is None:
                primary_failure = _AttemptFailure(current_step, exc)

    if primary_failure is not None:
        raise primary_failure
    return target_path


def inject_valuation_macros(
    source_workbook_path: Path,
    target_workbook_path: Optional[Path] = None,
    worksheet_name: str = "Valuation",
    debug_log_path: Optional[Path] = None,
) -> Path:
    source_path = Path(source_workbook_path).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Workbook not found: {source_path}")
    target_path = (
        Path(target_workbook_path).expanduser().resolve()
        if target_workbook_path is not None
        else source_path.with_suffix(".xlsm")
    )
    log_path = (
        Path(debug_log_path).expanduser().resolve()
        if debug_log_path is not None
        else (target_path.parent / "xlsm_injection_debug.log").resolve()
    )

    win32, pythoncom, pywintypes_module = _load_com_modules()
    trace: list[dict[str, Any]] = []
    retry_attempted = False
    last_failure: Optional[_AttemptFailure] = None

    for attempt in (1, 2):
        if attempt == 2:
            retry_attempted = True
            _remove_partial_target(target_path, attempt=attempt, trace=trace)
            _append_trace(
                trace,
                attempt=attempt,
                step="excel_start",
                status="retry",
                message="Retrying macro injection with a brand-new Excel instance",
            )
        try:
            return _run_injection_attempt(
                source_path,
                target_path,
                worksheet_name,
                attempt=attempt,
                trace=trace,
                win32=win32,
                pythoncom=pythoncom,
            )
        except _AttemptFailure as exc:
            last_failure = exc
            if attempt == 1 and _is_retryable_session_error(
                exc.exc,
                failed_step=exc.failed_step,
                pywintypes_module=pywintypes_module,
            ):
                _append_trace(
                    trace,
                    attempt=attempt,
                    step=exc.failed_step,
                    status="retry",
                    message="Detected session-like COM failure; retrying once with a new Excel instance",
                    exc=exc.exc,
                )
                continue
            break

    if last_failure is None:
        raise RuntimeError("Macro injection failed without a captured COM attempt error.")

    debug_log_error: Optional[BaseException] = None
    try:
        _write_debug_log(
            log_path,
            source_path=source_path,
            target_path=target_path,
            failed_step=last_failure.failed_step,
            exc=last_failure.exc,
            retry_attempted=retry_attempted,
            trace=trace,
        )
    except Exception as exc:
        debug_log_error = exc
        _append_trace(
            trace,
            attempt=0,
            step="com_cleanup",
            status="warn",
            message=f"Failed to write debug log: {log_path}",
            exc=exc,
        )

    message = (
        f"Macro injection failed at step '{last_failure.failed_step}'. "
        f"{type(last_failure.exc).__name__}: {_flatten_message(last_failure.exc)}. "
        f"Debug log: {log_path}"
    )
    if retry_attempted:
        message += " Retry attempted with a fresh Excel instance."
    if debug_log_error is not None:
        message += f" Debug log write also failed: {type(debug_log_error).__name__}: {_flatten_message(debug_log_error)}."

    raise MacroInjectionError(
        message,
        failed_step=last_failure.failed_step,
        debug_log_path=log_path,
        retry_attempted=retry_attempted,
        original_exception=last_failure.exc,
    ) from last_failure.exc
