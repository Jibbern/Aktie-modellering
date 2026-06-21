"""Narrow Hidden Value fallback and flags-sheet adapter support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping, Tuple


@dataclass(frozen=True)
class HiddenValueSupportDeps:
    runtime: MutableMapping[str, Any]


class HiddenValueSupport:
    def __init__(self, deps: HiddenValueSupportDeps) -> None:
        self._runtime = deps.runtime

    def build_hidden_value_flags_fallback(self, flags_audit_df: Any) -> Any:
        runtime = self._runtime
        pd = runtime["pd"]
        json = runtime["json"]

        if flags_audit_df is None or flags_audit_df.empty:
            return pd.DataFrame(columns=["Flag", "Title", "Status", "Why it failed", "Key blocker"])
        audit_df = flags_audit_df.copy()
        audit_df["quarter"] = pd.to_datetime(audit_df.get("quarter"), errors="coerce")
        latest_q = audit_df["quarter"].dropna().max() if "quarter" in audit_df.columns else pd.NaT
        if pd.notna(latest_q):
            audit_df = audit_df[audit_df["quarter"] == latest_q].copy()
        rows: List[Dict[str, Any]] = []

        def _fmt_pct_local(v: float) -> str:
            return f"{float(v) * 100:.1f}%"

        def _parse_json_local(raw: Any) -> Dict[str, Any]:
            if raw is None:
                return {}
            if isinstance(raw, str) and not raw.strip():
                return {}
            try:
                if pd.isna(raw):
                    return {}
            except Exception:
                pass
            try:
                val = json.loads(str(raw))
            except Exception:
                return {}
            return val if isinstance(val, dict) else {}

        def _blocker_from_row(flag_id: str, msg: str, inputs: Dict[str, Any]) -> Tuple[str, str]:
            low_msg = str(msg or "").lower()
            code = str(flag_id or "").upper()
            if "fcf_yield" in low_msg or "market_cap" in low_msg or "price" in low_msg:
                return "Blocked by missing price", "Price-linked trigger unavailable"
            if code == "D":
                lev = pd.to_numeric(inputs.get("leverage_ratio"), errors="coerce")
                if pd.notna(lev):
                    return "Blocked by leverage", f"Leverage {float(lev):.2f}x above threshold"
            if code in {"A", "F"}:
                sh_yoy = pd.to_numeric(inputs.get("shares_yoy"), errors="coerce")
                if pd.notna(sh_yoy):
                    if float(sh_yoy) > 0:
                        return "Blocked by rising share count", f"Shares YoY {_fmt_pct_local(float(sh_yoy))}"
                    return "Near miss", f"Shares YoY {_fmt_pct_local(float(sh_yoy))}"
            if code == "G":
                dps = pd.to_numeric(inputs.get("dividend_ps_q"), errors="coerce")
                dps_ly = pd.to_numeric(inputs.get("dividend_ps_yoy"), errors="coerce")
                if pd.isna(dps):
                    return "Blocked by missing dividend/share", "no_current_dividend_signal"
                if float(dps) == 0.0:
                    return "Blocked by no dividend", "explicit_dividend=0"
                if pd.notna(dps_ly) and float(dps_ly) < 0:
                    return "Blocked by dividend_stopped", "implied_historical_dividend"
                return "Near miss", "explicit_dividend"
            if "missing required inputs" in low_msg:
                return "Near miss", str(msg).split("|")[0].strip()
            return "Near miss", str(msg).split("|")[0].strip() or "Threshold not met"

        for _, row in audit_df.iterrows():
            if bool(row.get("pass_fail")):
                continue
            inputs = _parse_json_local(row.get("inputs_json"))
            status, blocker = _blocker_from_row(str(row.get("flag_id") or ""), str(row.get("qa_message") or ""), inputs)
            why = str(row.get("qa_message") or "").strip() or "Threshold not met"
            rows.append(
                {
                    "Flag": str(row.get("flag_id") or ""),
                    "Title": str(row.get("flag_name") or row.get("title") or row.get("metric") or "").strip(),
                    "Status": status,
                    "Why it failed": why,
                    "Key blocker": blocker,
                }
            )
        return pd.DataFrame(rows[:7], columns=["Flag", "Title", "Status", "Why it failed", "Key blocker"])

    def write_flags_sheet(self, name: str, df: Any) -> None:
        runtime = self._runtime
        wb = runtime["wb"]
        pd = runtime["pd"]
        font_size = runtime["font_size"]
        header_size = runtime["header_size"]
        _safe_cell = runtime["_safe_cell"]
        _autowidth = runtime["_autowidth"]
        Alignment = runtime["Alignment"]
        Font = runtime["Font"]
        CellIsRule = runtime["CellIsRule"]
        PatternFill = runtime["PatternFill"]
        Table = runtime["Table"]
        TableStyleInfo = runtime["TableStyleInfo"]
        get_column_letter = runtime["get_column_letter"]
        HiddenValueFlagsSheetInputs = runtime["HiddenValueFlagsSheetInputs"]
        write_hidden_value_flags_sheet = runtime["write_hidden_value_flags_sheet"]

        if str(name) == "Hidden_Value_Flags":
            write_hidden_value_flags_sheet(
                HiddenValueFlagsSheetInputs(
                    wb=wb,
                    sheet_name=str(name),
                    flags_df=df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
                    font_size=font_size,
                    header_size=header_size,
                    safe_cell=_safe_cell,
                )
            )
            return
        ws = wb.create_sheet(name)
        if df is None or df.empty:
            ws["A1"] = "No signals."
            return
        headers = list(df.columns)
        ws.append(headers)
        for _, r in df.iterrows():
            ws.append([None if pd.isna(r[c]) else _safe_cell(r[c]) for c in headers])
        ws.freeze_panes = "A2"
        for c in ws[1]:
            c.font = Font(bold=True, size=header_size)
            c.alignment = Alignment(vertical="center")
        ws.sheet_format.defaultRowHeight = 18
        ws.sheet_view.zoomScale = 110
        _autowidth(ws, len(headers))
        rng = None
        # widen evidence columns and wrap to keep sheet readable
        col_map = {h: i + 1 for i, h in enumerate(headers)}
        for col_name in ["evidence_1", "evidence_2", "evidence_3"]:
            idx = col_map.get(col_name)
            if not idx:
                continue
            letter = get_column_letter(idx)
            ws.column_dimensions[letter].width = max(34, min(38, ws.column_dimensions[letter].width or 34))
            for rr in range(2, ws.max_row + 1):
                ws[f"{letter}{rr}"].alignment = Alignment(wrap_text=True, vertical="top")
        # score heatmap
        score_idx = col_map.get("score")
        if score_idx:
            letter = get_column_letter(score_idx)
            rng = f"{letter}2:{letter}{ws.max_row}"
            ws.conditional_formatting.add(
                rng,
                CellIsRule(
                    operator="greaterThanOrEqual",
                    formula=["70"],
                    fill=PatternFill("solid", fgColor="C6EFCE"),
                ),
            )
        try:
            if len(headers) == len(set(headers)) and all(isinstance(h, str) for h in headers):
                ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"
                t = Table(displayName=name.replace(" ", "").replace("-", ""), ref=ref)
                t.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
                ws.add_table(t)
        except Exception:
            pass
