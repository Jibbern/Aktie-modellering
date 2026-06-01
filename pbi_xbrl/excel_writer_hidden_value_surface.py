"""Pure-ish Hidden Value surface model helpers for the Valuation sheet."""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Set

import pandas as pd


NO_TRIGGER_DISPLAY_LABEL = "No triggered flags"
NO_TRIGGER_DISPLAY_TITLE = "No scored hidden-value flags currently triggered"
NO_TRIGGER_DISPLAY_SCORE = None
NO_TRIGGER_DISPLAY_SEVERITY = "Info"
NO_TRIGGER_DISPLAY_SUPPORT = "Audit candidates remain in Hidden_Value_Flags / Hidden_Value_Audit."


def _identity_normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _truncate_text(value: Any, max_chars: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip()


def _money_m(value: float) -> str:
    return f"${float(value) / 1_000_000.0:,.1f}m"


@dataclass(frozen=True)
class HiddenValueSurfaceModelInputs:
    flags_df: Optional[pd.DataFrame] = None
    flags_audit_df: Optional[pd.DataFrame] = None
    hist: Optional[pd.DataFrame] = None
    adj_metrics: Optional[pd.DataFrame] = None
    leverage_df: Optional[pd.DataFrame] = None
    debt_tranches: Optional[pd.DataFrame] = None
    signals_base_df: Optional[pd.DataFrame] = None
    price: Any = None
    build_hidden_value_flags: Optional[Callable[..., Any]] = None
    build_hidden_value_flags_fallback: Optional[Callable[[pd.DataFrame], Any]] = None
    normalize_text: Callable[[Any], str] = _identity_normalize_text
    truncate_text: Callable[..., str] = _truncate_text
    money_formatter: Callable[[float], str] = _money_m
    max_flags: Optional[int] = None


@dataclass(frozen=True)
class HiddenValueDisplayRow:
    label: str
    title: Any
    score: Any
    severity: Any
    support: Any
    source_row: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class HiddenValueSurfaceModel:
    rows_all: List[Dict[str, Any]] = field(default_factory=list)
    rows_triggered: List[Dict[str, Any]] = field(default_factory=list)
    triggered_keys: Set[str] = field(default_factory=set)
    price_linked_keys: Set[str] = field(default_factory=set)
    display_source_rows: List[Dict[str, Any]] = field(default_factory=list)
    display_rows: List[HiddenValueDisplayRow] = field(default_factory=list)
    visible_count: int = 1


def hidden_flag_field(flag_row: Mapping[str, Any], *names: str) -> Any:
    if not isinstance(flag_row, Mapping):
        return None
    for name in names:
        if name in flag_row:
            return flag_row.get(name)
    lower_map = {str(k).strip().lower(): v for k, v in flag_row.items()}
    for name in names:
        key = str(name or "").strip().lower()
        if key in lower_map:
            return lower_map.get(key)
    return None


def hidden_flag_score(value_in: Any) -> float:
    try:
        return float(pd.to_numeric(value_in, errors="coerce") or 0.0)
    except Exception:
        return 0.0


def hidden_flag_metric_piece(
    label: str,
    value: Any,
    fmt: str,
    *,
    money_formatter: Callable[[float], str] = _money_m,
) -> str:
    try:
        fval = float(value)
    except Exception:
        return ""
    if fmt == "pct":
        sign = "+" if fval > 0 else ""
        return f"{label} {sign}{fval * 100.0:.1f}%"
    if fmt == "bps":
        sign = "+" if fval > 0 else ""
        return f"{label} {sign}{fval:,.0f}bps"
    if fmt == "x":
        return f"{label} {fval:.2f}x"
    if fmt == "money":
        return f"{label} {money_formatter(fval)}"
    if fmt == "count":
        if str(label or "").strip().lower() == "margin streak":
            return f"Margin streak {fval:.0f} quarters"
        return f"{label} {fval:.0f}"
    if fmt == "float":
        return f"{label} {fval:.1f}"
    return ""


def hidden_flag_metrics_summary(
    flag_row: Mapping[str, Any],
    *,
    money_formatter: Callable[[float], str] = _money_m,
) -> str:
    raw_metrics = hidden_flag_field(flag_row, "metrics_json", "Metrics_json", "metrics", "Metrics")
    metrics_obj: Dict[str, Any] = {}
    if isinstance(raw_metrics, dict):
        metrics_obj = dict(raw_metrics)
    elif isinstance(raw_metrics, str):
        metrics_txt = raw_metrics.strip()
        if metrics_txt.startswith("{") and metrics_txt.endswith("}"):
            try:
                parsed = json.loads(metrics_txt)
                if isinstance(parsed, dict):
                    metrics_obj = parsed
            except Exception:
                metrics_obj = {}
    pieces: List[str] = []
    metric_order = [
        ("ebit_growth_yoy", "EBIT YoY", "pct"),
        ("ebitda_growth_yoy", "EBITDA YoY", "pct"),
        ("shares_yoy", "Shares YoY", "pct"),
        ("adj_margin_ttm", "Adj margin TTM", "pct"),
        ("margin_yoy_bps", "Margin YoY", "bps"),
        ("margin_streak", "Margin streak", "count"),
        ("fcf_ttm_pos_years", "Positive FCF years", "count"),
        ("pos_fcf_ratio", "Positive FCF ratio", "pct"),
        ("fcf_yield", "FCF yield", "pct"),
        ("interest_coverage", "Interest cover", "x"),
        ("debt_drop_pct", "Net debt change", "pct"),
        ("leverage_ratio", "Leverage", "x"),
        ("corporate_net_debt", "Net debt", "money"),
        ("ebitda_ttm", "EBITDA TTM", "money"),
        ("ebit_ttm", "EBIT TTM", "money"),
        ("dividend_ps_q", "Dividend/share", "float"),
    ]
    for metric_key, metric_label, metric_fmt in metric_order:
        metric_val = metrics_obj.get(metric_key)
        if metric_val in (None, "", "null"):
            continue
        piece = hidden_flag_metric_piece(metric_label, metric_val, metric_fmt, money_formatter=money_formatter)
        if piece:
            pieces.append(piece)
        if len(pieces) >= 3:
            break
    if pieces:
        return " | ".join(pieces)
    flag_code = str(hidden_flag_field(flag_row, "flag_code", "Flag code", "flag") or "").strip().upper()
    if flag_code in {"C", "E"}:
        return "Price-linked via Valuation input"
    if flag_code == "G":
        return "Requires active dividend yield or dividend growth support"
    return ""


def hidden_flag_visible_support(
    flag_row: Mapping[str, Any],
    *,
    normalize_text: Callable[[Any], str] = _identity_normalize_text,
    truncate_text: Callable[..., str] = _truncate_text,
    money_formatter: Callable[[float], str] = _money_m,
) -> str:
    metric_summary = hidden_flag_metrics_summary(flag_row, money_formatter=money_formatter)
    evidence_bits: List[str] = []
    for evidence_key in ("evidence_1", "evidence_2", "evidence_3"):
        raw_evidence = normalize_text(
            str(
                hidden_flag_field(
                    flag_row,
                    evidence_key,
                    evidence_key.title().replace("_", " "),
                    evidence_key.replace("_", ""),
                )
                or ""
            )
        ).strip()
        if not raw_evidence:
            continue
        raw_evidence = re.sub(r"\|\s*Quarter:\s*\d{4}-\d{2}-\d{2}\b", "", raw_evidence, flags=re.I).strip(" |")
        raw_evidence = re.sub(r"^Threshold:\s*", "", raw_evidence, flags=re.I)
        raw_evidence = re.sub(r"^Inputs:\s*", "", raw_evidence, flags=re.I)
        title_txt = str(hidden_flag_field(flag_row, "title", "Title") or "").strip().lower()
        if raw_evidence and raw_evidence.lower() not in {metric_summary.lower(), title_txt}:
            evidence_bits.append(raw_evidence)
    parts = [part for part in [metric_summary, *evidence_bits[:1]] if part]
    return truncate_text(" | ".join(parts), 160)


def hidden_flag_is_visible(
    flag_row: Mapping[str, Any],
    *,
    normalize_text: Callable[[Any], str] = _identity_normalize_text,
    money_formatter: Callable[[float], str] = _money_m,
) -> bool:
    score_raw = hidden_flag_field(flag_row, "score", "Score")
    score_val = hidden_flag_score(score_raw)
    if score_val >= 1.0:
        return True
    if str(score_raw or "").strip() not in {"", "nan", "None", "null"}:
        return False
    title_txt = normalize_text(str(hidden_flag_field(flag_row, "title", "Title") or "")).strip()
    if not title_txt:
        return False
    if hidden_flag_metrics_summary(flag_row, money_formatter=money_formatter):
        return True
    for evidence_key in ("evidence_1", "evidence_2", "evidence_3"):
        evidence_txt = normalize_text(
            str(
                hidden_flag_field(
                    flag_row,
                    evidence_key,
                    evidence_key.title().replace("_", " "),
                    evidence_key.replace("_", ""),
                )
                or ""
            )
        ).strip()
        if evidence_txt:
            return True
    return False


def _df_or_empty(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) and not df.empty else pd.DataFrame()


def _coerce_price(price: Any) -> Optional[float]:
    try:
        if price not in (None, "") and not pd.isna(price):
            return float(price)
    except Exception:
        return None
    return None


def visible_hidden_flag_rows(inputs: HiddenValueSurfaceModelInputs) -> List[Dict[str, Any]]:
    flags_df = inputs.flags_df
    flags_audit_df = inputs.flags_audit_df
    if isinstance(flags_df, pd.DataFrame) and not flags_df.empty:
        try:
            out_rows = flags_df.fillna("").to_dict("records")
            has_scored_rows = any(
                hidden_flag_score(hidden_flag_field(dict(x), "score", "Score")) >= 1.0
                for x in out_rows
            )
            if out_rows and has_scored_rows:
                return [dict(x) for x in out_rows]
            if isinstance(flags_audit_df, pd.DataFrame) and not flags_audit_df.empty and out_rows:
                audit_local = flags_audit_df.copy()
                active_codes: Set[str] = set()
                for _, audit_row in audit_local.iterrows():
                    code_txt = str(
                        audit_row.get("flag_id")
                        or audit_row.get("flag_code")
                        or audit_row.get("Flag")
                        or ""
                    ).strip().upper()
                    if not code_txt:
                        continue
                    out_val = audit_row.get("output_value")
                    pass_fail_val = audit_row.get("pass_fail")
                    is_active = False
                    try:
                        if pd.notna(pd.to_numeric(out_val, errors="coerce")):
                            is_active = float(pd.to_numeric(out_val, errors="coerce") or 0.0) >= 1.0
                    except Exception:
                        is_active = False
                    if not is_active and isinstance(pass_fail_val, (bool, int, float)):
                        is_active = bool(pass_fail_val)
                    if not is_active and str(pass_fail_val or "").strip().lower() in {"true", "1", "yes", "pass"}:
                        is_active = True
                    if is_active:
                        active_codes.add(code_txt)
                if active_codes:
                    filtered_rows: List[Dict[str, Any]] = []
                    for raw_row in out_rows:
                        raw_dict = dict(raw_row)
                        code_txt = str(hidden_flag_field(raw_dict, "flag_code", "flag_id", "Flag", "flag") or "").strip().upper()
                        if code_txt not in active_codes:
                            continue
                        if not hidden_flag_score(hidden_flag_field(raw_dict, "score", "Score")):
                            raw_dict["score"] = 100.0
                        filtered_rows.append(raw_dict)
                    if filtered_rows:
                        return filtered_rows
        except Exception:
            pass
    if inputs.build_hidden_value_flags is not None and inputs.max_flags is not None:
        try:
            rebuilt_rows = inputs.build_hidden_value_flags(
                hist=_df_or_empty(inputs.hist),
                adj_metrics=_df_or_empty(inputs.adj_metrics),
                leverage_df=_df_or_empty(inputs.leverage_df),
                debt_tranches=_df_or_empty(inputs.debt_tranches),
                signals_base=_df_or_empty(inputs.signals_base_df) if isinstance(inputs.signals_base_df, pd.DataFrame) and not inputs.signals_base_df.empty else None,
                price=_coerce_price(inputs.price),
                max_flags=inputs.max_flags,
            )
            if isinstance(rebuilt_rows, pd.DataFrame) and not rebuilt_rows.empty:
                return [dict(x) for x in rebuilt_rows.fillna("").to_dict("records")]
        except Exception:
            pass
    if inputs.build_hidden_value_flags_fallback is not None:
        fallback_input = flags_audit_df if isinstance(flags_audit_df, pd.DataFrame) else pd.DataFrame()
        fallback_rows = inputs.build_hidden_value_flags_fallback(fallback_input)
        if isinstance(fallback_rows, pd.DataFrame) and not fallback_rows.empty:
            renamed = fallback_rows.rename(
                columns={
                    "Title": "title",
                    "Score": "score",
                    "Severity": "severity",
                    "As of quarter": "as_of_quarter",
                }
            )
            try:
                return [dict(x) for x in renamed.fillna("").to_dict("records")]
            except Exception:
                return []
    return []


def select_hidden_value_display_rows(
    rows: List[Mapping[str, Any]],
    *,
    max_display_rows: int = 5,
) -> HiddenValueSurfaceModel:
    rows_all = sorted(
        [dict(x) for x in rows],
        key=lambda x: (
            int(pd.to_numeric(hidden_flag_field(x, "rank", "Rank"), errors="coerce") or 999),
            -hidden_flag_score(hidden_flag_field(x, "score", "Score")),
            str(hidden_flag_field(x, "title", "Title") or "").lower(),
        ),
    )
    rows_triggered = [
        dict(x)
        for x in rows_all
        if hidden_flag_score(hidden_flag_field(x, "triggered", "Triggered")) >= 1.0
    ]
    triggered_keys = {
        str(hidden_flag_field(x, "flag_code", "flag_id", "Flag", "flag") or "").strip().upper()
        or str(hidden_flag_field(x, "title", "Title") or "").strip().lower()
        for x in rows_triggered
    }
    price_linked_keys = {
        str(hidden_flag_field(x, "flag_code", "flag_id", "Flag", "flag") or "").strip().upper()
        for x in rows_all
        if str(hidden_flag_field(x, "flag_code", "flag_id", "Flag", "flag") or "").strip().upper() in {"C", "E"}
    }
    display_source_rows = rows_triggered[:max_display_rows]
    visible_count = max(1, min(max_display_rows, len(display_source_rows)))
    display_rows: List[HiddenValueDisplayRow] = []
    for idx in range(1, visible_count + 1):
        display_flag = display_source_rows[idx - 1] if idx <= len(display_source_rows) else None
        if display_flag is not None:
            display_rows.append(
                HiddenValueDisplayRow(
                    label=f"Flag {idx}",
                    title=hidden_flag_field(display_flag, "title", "Title", "flag_name", "Flag name") or "",
                    score=hidden_flag_score(hidden_flag_field(display_flag, "score", "Score")),
                    severity=hidden_flag_field(display_flag, "severity", "Severity") or "",
                    support=hidden_flag_field(
                        display_flag,
                        "visible_support",
                        "Visible support",
                        "support",
                        "Support",
                        "evidence_1",
                        "Evidence 1",
                    ) or "",
                    source_row=dict(display_flag),
                )
            )
        else:
            display_rows.append(
                HiddenValueDisplayRow(
                    label=NO_TRIGGER_DISPLAY_LABEL,
                    title=NO_TRIGGER_DISPLAY_TITLE,
                    score=NO_TRIGGER_DISPLAY_SCORE,
                    severity=NO_TRIGGER_DISPLAY_SEVERITY,
                    support=NO_TRIGGER_DISPLAY_SUPPORT,
                    source_row=None,
                )
            )
    return HiddenValueSurfaceModel(
        rows_all=rows_all,
        rows_triggered=rows_triggered,
        triggered_keys=triggered_keys,
        price_linked_keys=price_linked_keys,
        display_source_rows=display_source_rows,
        display_rows=display_rows,
        visible_count=visible_count,
    )


def hidden_value_ai_helper_formula(index: int, row_idx: int, helper_letter: str = "AI") -> str:
    if int(index) == 1:
        return '=IFERROR(MATCH(1,\'Hidden_Value_Flags\'!$L$2:$L$100,0)+1,"")'
    prev_helper = f"${helper_letter}{int(row_idx) - 1}"
    return (
        f'=IF({prev_helper}="","",IFERROR('
        f"MATCH(1,INDEX('Hidden_Value_Flags'!$L:$L,{prev_helper}+1):'Hidden_Value_Flags'!$L$100,0)"
        f"+{prev_helper},\"\"))"
    )


def build_hidden_value_surface_model(inputs: HiddenValueSurfaceModelInputs) -> HiddenValueSurfaceModel:
    return select_hidden_value_display_rows(visible_hidden_flag_rows(inputs))
