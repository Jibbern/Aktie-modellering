"""Readback-only workbook quality guardrails.

These checks inspect the saved workbook object and report historical bug
classes without changing workbook output or writer semantics.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


SEVERITIES = {"P0", "P1", "P2"}


@dataclass(frozen=True)
class WorkbookQualityIssue:
    severity: str
    ticker: str
    sheet: str
    row: int
    metric_label: str
    reason: str
    owner: str
    rule_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity,
            "ticker": self.ticker,
            "sheet": self.sheet,
            "row": self.row,
            "metric_label": self.metric_label,
            "reason": self.reason,
            "owner": self.owner,
            "rule_id": self.rule_id,
        }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _text(value).lower()).strip("_")


def _issue(
    *,
    severity: str,
    ticker: str,
    sheet: str,
    row: int = 0,
    metric_label: str = "",
    reason: str,
    owner: str,
    rule_id: str,
) -> WorkbookQualityIssue:
    sev = severity if severity in SEVERITIES else "P2"
    return WorkbookQualityIssue(
        severity=sev,
        ticker=str(ticker or "").strip().upper(),
        sheet=sheet,
        row=int(row or 0),
        metric_label=metric_label,
        reason=reason,
        owner=owner,
        rule_id=rule_id,
    )


def _header_map(ws: Any, row_idx: int = 1, *, max_col: Optional[int] = None) -> Dict[str, int]:
    limit = max_col or int(ws.max_column or 0)
    return {
        _text(ws.cell(row_idx, cc).value).lower(): cc
        for cc in range(1, limit + 1)
        if _text(ws.cell(row_idx, cc).value)
    }


def _section_event(title: str) -> str:
    match = re.search(r"\b20\d{2}-Q[1-4]\b", title)
    if match:
        return match.group(0)
    return re.sub(r"\s+revisions$", "", title.strip(), flags=re.I)


def _quarter_parts(value: str) -> Optional[Tuple[int, int]]:
    match = re.fullmatch(r"(20\d{2})-Q([1-4])", _text(value))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _annual_horizon_year(value: str) -> Optional[int]:
    match = re.fullmatch(r"(20\d{2})\s+year", _text(value), flags=re.I)
    return int(match.group(1)) if match else None


def _is_final_status(value: str) -> bool:
    return _text(value).lower() in {"completed", "hit", "missed", "beat"}


def _contains_fy_or_ytd(value: str) -> bool:
    return bool(re.search(r"\b(FY|YTD)\b|delta\s+ytd|\byear-to-date\b", _text(value), re.I))


def _annual_only_exempt(metric: str, horizon: str, note: str) -> bool:
    blob = f"{metric} {horizon} {note}".lower()
    return bool(re.search(r"\b(annual-only|annualized program|annualized run[- ]rate|run[- ]rate only)\b", blob))


def _has_safe_missing_exception(*values: Any) -> bool:
    blob = " ".join(_text(value) for value in values).lower()
    return bool(
        re.search(
            r"\b("
            r"source[_ -]?missing|unsafe[_ -]?residual|definition[_ -]?mismatch|"
            r"annual[_ -]?only|not[_ -]?applicable|intentionally[_ -]?suppressed"
            r")\b",
            blob,
        )
    )


def _quarter_label(value: Any) -> str:
    if isinstance(value, datetime):
        return f"{value.year}-Q{((value.month - 1) // 3) + 1}"
    if isinstance(value, date):
        return f"{value.year}-Q{((value.month - 1) // 3) + 1}"
    txt = _text(value)
    match = re.fullmatch(r"(20\d{2})-Q([1-4])", txt, flags=re.I)
    if match:
        return f"{match.group(1)}-Q{match.group(2)}"
    match = re.match(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", txt)
    if match:
        month = int(match.group(2))
        return f"{match.group(1)}-Q{((month - 1) // 3) + 1}"
    return ""


def _year_label(value: Any) -> Optional[int]:
    if isinstance(value, datetime):
        return value.year
    if isinstance(value, date):
        return value.year
    if isinstance(value, int) and 1900 <= value <= 2100:
        return int(value)
    txt = _text(value)
    match = re.search(r"\b(20\d{2})\b", txt)
    return int(match.group(1)) if match else None


def _quarter_columns(ws: Any, header_row: int) -> Dict[str, int]:
    columns: Dict[str, int] = {}
    for cc in range(2, int(ws.max_column or 0) + 1):
        quarter = _quarter_label(ws.cell(header_row, cc).value)
        if quarter:
            columns[quarter] = cc
    return columns


def _year_columns(ws: Any, header_row: int) -> Dict[int, int]:
    columns: Dict[int, int] = {}
    for cc in range(2, int(ws.max_column or 0) + 1):
        year = _year_label(ws.cell(header_row, cc).value)
        if year:
            columns[year] = cc
    return columns


def _find_label_row(ws: Any, label: str, *, start_row: int = 1) -> int:
    wanted = _slug(label)
    for rr in range(start_row, int(ws.max_row or 0) + 1):
        if _slug(ws.cell(rr, 1).value) == wanted:
            return rr
    return 0


def _source_value_present(value: Any) -> bool:
    return not _is_blank(value)


def _promise_revision_blocks(ws: Any) -> Dict[str, List[Tuple[int, Dict[str, Any]]]]:
    blocks: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
    rr = 1
    while rr <= int(ws.max_row or 0):
        title = _text(ws.cell(rr, 1).value)
        if not title.endswith("revisions"):
            rr += 1
            continue
        header_row = rr + 1
        headers = _header_map(ws, header_row, max_col=min(int(ws.max_column or 0), 15))
        rows: List[Tuple[int, Dict[str, Any]]] = []
        body_row = header_row + 1
        while body_row <= int(ws.max_row or 0):
            first = _text(ws.cell(body_row, 1).value)
            if first.endswith("revisions") or first.endswith("guidance progression") or first.endswith("open guidance"):
                break
            if first and all(_is_blank(ws.cell(body_row, cc).value) for cc in range(2, min(int(ws.max_column or 0), 15) + 1)):
                break
            values = {name: ws.cell(body_row, col).value for name, col in headers.items()}
            metric = _text(values.get("metric") or values.get("milestone"))
            if metric and metric.lower() not in {"metric", "milestone"}:
                values["hidden_key"] = ws.cell(body_row, 15).value
                rows.append((body_row, values))
            body_row += 1
        blocks[title] = rows
        rr = body_row
    return blocks


def _scan_promise_horizon_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "Promise_Progress_UI" not in getattr(wb, "sheetnames", []):
        return []
    ws = wb["Promise_Progress_UI"]
    issues: List[WorkbookQualityIssue] = []
    for section, rows in _promise_revision_blocks(ws).items():
        event = _section_event(section)
        event_parts = _quarter_parts(event)
        seen: Dict[Tuple[str, str], int] = {}
        for row_idx, row in rows:
            metric = _text(row.get("metric") or row.get("milestone"))
            horizon = _text(row.get("horizon"))
            stated = _text(row.get("stated in"))
            actual = _text(row.get("actual"))
            progress = _text(row.get("progress / run-rate"))
            status = _text(row.get("status"))
            note = _text(row.get("source / note"))
            key = (_slug(metric), _slug(horizon))
            if all(key):
                if key in seen:
                    issues.append(
                        _issue(
                            severity="P1",
                            ticker=ticker,
                            sheet=ws.title,
                            row=row_idx,
                            metric_label=metric,
                            reason=f"Duplicate metric+horizon row in {section}: {metric} / {horizon}.",
                            owner="Promise_Progress_UI row assembly / dedupe",
                            rule_id="promise_duplicate_metric_horizon",
                        )
                    )
                else:
                    seen[key] = row_idx

            stated_event = _section_event(stated) if stated else ""
            if event and stated and stated_event != event:
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Row stated in {stated!r} but lives in {section!r}.",
                        owner="Promise_Progress_UI stated-in routing",
                        rule_id="promise_stated_in_mismatch",
                    )
                )

            h_year = _annual_horizon_year(horizon)
            if event_parts and h_year and not _annual_only_exempt(metric, horizon, note):
                event_year, event_q = event_parts
                if event_q == 4 and event_year < h_year:
                    issues.append(
                        _issue(
                            severity="P1",
                            ticker=ticker,
                            sheet=ws.title,
                            row=row_idx,
                            metric_label=metric,
                            reason=f"Future annual horizon {horizon!r} appears under prior-year Q4 section {section!r}.",
                            owner="Promise_Progress_UI horizon routing",
                            rule_id="promise_future_annual_in_prior_year_q4",
                        )
                    )
                if event_year < h_year and (actual or _is_final_status(status)):
                    issues.append(
                        _issue(
                            severity="P1",
                            ticker=ticker,
                            sheet=ws.title,
                            row=row_idx,
                            metric_label=metric,
                            reason=f"Future annual horizon {horizon!r} has actual/final status before that year is complete.",
                            owner="Promise_Progress_UI horizon completion semantics",
                            rule_id="promise_future_annual_completed_early",
                        )
                    )

            if event_parts and event_parts[1] == 4 and not _annual_only_exempt(metric, horizon, note):
                event_year = event_parts[0]
                q4_annual_row = h_year == event_year and _is_final_status(status)
                q4_quarter_row = horizon == f"{event_year}-Q4" and progress
                if (q4_annual_row or q4_quarter_row) and (
                    _contains_fy_or_ytd(actual)
                    or (q4_annual_row and progress and not _contains_fy_or_ytd(progress))
                    or (q4_annual_row and not progress)
                ):
                    issues.append(
                        _issue(
                            severity="P1",
                            ticker=ticker,
                            sheet=ws.title,
                            row=row_idx,
                            metric_label=metric,
                            reason="Q4 final rows should keep quarter actual in Actual and FY/YTD value in Progress / run-rate.",
                            owner="Promise_Progress_UI Q4 actual/progress mapping",
                            rule_id="promise_q4_actual_progress_split",
                        )
                    )
    return issues


def _manual_no_source_exempt(row: Mapping[str, Any]) -> bool:
    blob = " ".join(_text(row.get(name)) for name in ("source / note", "source type", "new/current guide"))
    return bool(re.search(r"\b(manual no-source|no-source|no source|manual assumption|scenario assumption|model thesis)\b", blob, re.I))


def _promise_row_is_source_backed(row: Mapping[str, Any]) -> bool:
    if _manual_no_source_exempt(row):
        return False
    source_blob = " ".join(_text(row.get(name)) for name in ("source date", "source / note", "source type"))
    if _text(row.get("source date")):
        return True
    return bool(
        re.search(
            r"\b(source-backed|earnings|release|presentation|pre-release|filing|conference|operating_drivers|history_q|stated in|sec|10-[qk])\b",
            source_blob,
            re.I,
        )
    )


def _metric_key_matches(metric: str, hidden_key: str) -> bool:
    metric_slug = _slug(metric)
    key_slug = _slug(hidden_key)
    if not metric_slug:
        return True
    if metric_slug in key_slug:
        return True
    generic = {"guidance", "target", "metric", "milestone", "current", "new", "year", "annual"}
    tokens = [token for token in metric_slug.split("_") if token and token not in generic]
    return bool(tokens) and all(token in key_slug for token in tokens)


def _scan_promise_hidden_key_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "Promise_Progress_UI" not in getattr(wb, "sheetnames", []):
        return []
    ws = wb["Promise_Progress_UI"]
    issues: List[WorkbookQualityIssue] = []
    bad_visible_key_values = {"on track", "completed", "hit", "missed", "mixed", "open"}
    for _section, rows in _promise_revision_blocks(ws).items():
        for row_idx, row in rows:
            if not _promise_row_is_source_backed(row):
                continue
            metric = _text(row.get("metric") or row.get("milestone"))
            horizon = _text(row.get("horizon"))
            source_date = _text(row.get("source date"))
            hidden_key = _text(row.get("hidden_key"))
            if not hidden_key:
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason="Source-backed Promise row has a blank hidden source key.",
                        owner="Promise_Progress_UI hidden source-key hydration",
                        rule_id="promise_source_backed_missing_hidden_key",
                    )
                )
                continue
            key_low = hidden_key.lower()
            if key_low in bad_visible_key_values or re.fullmatch(r"20\d{2}-\d{2}-\d{2}", hidden_key) or hidden_key.startswith(("$", "+")):
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Hidden source key looks like a visible cell value: {hidden_key!r}.",
                        owner="Promise_Progress_UI hidden source-key hydration",
                        rule_id="promise_hidden_key_visible_value",
                    )
                )
            if not hidden_key.startswith("guidance:"):
                issues.append(
                    _issue(
                        severity="P2",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Hidden source key does not use the guidance namespace: {hidden_key!r}.",
                        owner="Promise_Progress_UI hidden source-key contract",
                        rule_id="promise_hidden_key_namespace",
                    )
                )
            if not _metric_key_matches(metric, hidden_key):
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Hidden key {hidden_key!r} does not align to visible metric {metric!r}.",
                        owner="Promise_Progress_UI hidden source-key hydration",
                        rule_id="promise_hidden_key_metric_mismatch",
                    )
                )
            horizon_slug = _slug(horizon)
            if horizon_slug and horizon_slug not in _slug(hidden_key):
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Hidden key {hidden_key!r} does not include horizon {horizon!r}.",
                        owner="Promise_Progress_UI hidden source-key hydration",
                        rule_id="promise_hidden_key_horizon_mismatch",
                    )
                )
            date_tokens = set(re.findall(r"20\d{2}_\d{2}_\d{2}", _slug(hidden_key)))
            source_date_slug = _slug(source_date)
            if date_tokens and source_date_slug and source_date_slug not in date_tokens:
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Hidden key date tokens {sorted(date_tokens)} do not match source date {source_date!r}.",
                        owner="Promise_Progress_UI hidden source-key hydration",
                        rule_id="promise_hidden_key_source_date_mismatch",
                    )
                )
    return issues


def _scan_quarter_narrative_amount_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "Quarter_Narrative_Data" not in getattr(wb, "sheetnames", []):
        return []
    ws = wb["Quarter_Narrative_Data"]
    headers = _header_map(ws)
    amount_col = headers.get("amount")
    theme_col = headers.get("theme")
    if not amount_col:
        return []
    issues: List[WorkbookQualityIssue] = []
    descriptor_patterns = (
        r"\bare geographic segments\b",
        r"\bare brand\b",
        r"\bgeographic segments\b",
        r"\bdescriptor\b",
    )
    allowed_short_text = {"watch item", "review item", "qualitative", "n/a", "na"}
    for rr in range(2, int(ws.max_row or 0) + 1):
        amount = _text(ws.cell(rr, amount_col).value)
        if not amount:
            continue
        metric = _text(ws.cell(rr, theme_col).value) if theme_col else ""
        low = amount.lower()
        if any(re.search(pattern, low, re.I) for pattern in descriptor_patterns):
            issues.append(
                _issue(
                    severity="P1",
                    ticker=ticker,
                    sheet=ws.title,
                    row=rr,
                    metric_label=metric,
                    reason=f"Amount contains descriptor prose instead of value text: {amount!r}.",
                    owner="Quarter_Narrative_Data amount extraction",
                    rule_id="narrative_amount_descriptor_prose",
                )
            )
            continue
        has_value_mark = bool(re.search(r"[$%0-9+\-]", amount))
        word_count = len(re.findall(r"[A-Za-z]+", amount))
        if not has_value_mark and low not in allowed_short_text and (len(amount) > 55 or word_count > 6):
            issues.append(
                _issue(
                    severity="P2",
                    ticker=ticker,
                    sheet=ws.title,
                    row=rr,
                    metric_label=metric,
                    reason=f"Amount should be numeric-like, compact value text, or blank: {amount!r}.",
                    owner="Quarter_Narrative_Data amount extraction",
                    rule_id="narrative_amount_long_prose",
                )
            )
    return issues


def _row_has_nonblank_values(ws: Any, row_idx: int, *, start_col: int = 2) -> bool:
    return any(_text(ws.cell(row_idx, cc).value) for cc in range(start_col, int(ws.max_column or 0) + 1))


def _scan_sector_specific_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "BS_Segments" not in getattr(wb, "sheetnames", []):
        return []
    ws = wb["BS_Segments"]
    ticker_u = str(ticker or "").strip().upper()
    carbon_rows = [
        rr
        for rr in range(1, int(ws.max_row or 0) + 1)
        if _text(ws.cell(rr, 1).value).lower() == "carbon equipment liabilities"
    ]
    issues: List[WorkbookQualityIssue] = []
    if ticker_u == "GPRE":
        if not carbon_rows:
            issues.append(
                _issue(
                    severity="P1",
                    ticker=ticker,
                    sheet=ws.title,
                    metric_label="Carbon equipment liabilities",
                    reason="GPRE should retain the sector-specific Carbon equipment liabilities row.",
                    owner="BS_Segments sector-specific row policy",
                    rule_id="sector_carbon_row_missing",
                )
            )
    elif ticker_u in {"PBI", "ANF"}:
        for rr in carbon_rows:
            if not _row_has_nonblank_values(ws, rr):
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=rr,
                        metric_label="Carbon equipment liabilities",
                        reason=f"{ticker_u} should suppress blank GPRE-specific Carbon equipment liabilities rows.",
                        owner="BS_Segments sector-specific row policy",
                        rule_id="sector_blank_carbon_row",
                    )
                )
    else:
        for rr in carbon_rows:
            if not _row_has_nonblank_values(ws, rr):
                issues.append(
                    _issue(
                        severity="P2",
                        ticker=ticker,
                        sheet=ws.title,
                        row=rr,
                        metric_label="Carbon equipment liabilities",
                        reason="Blank sector-specific Carbon equipment liabilities row should be suppressed unless relevant.",
                        owner="BS_Segments sector-specific row policy",
                        rule_id="sector_blank_carbon_row",
                )
            )
    return issues


def _row_values(ws: Any, row_idx: int) -> List[Any]:
    return [ws.cell(row_idx, cc).value for cc in range(1, int(ws.max_column or 0) + 1)]


def _trusted_facts_by_metric_quarter(wb: Any, metric_name: str) -> Dict[str, Any]:
    if "DATA_Facts_Long" not in getattr(wb, "sheetnames", []):
        return {}
    ws = wb["DATA_Facts_Long"]
    headers = _header_map(ws)
    metric_col = headers.get("metric")
    period_col = headers.get("period_end")
    value_col = headers.get("value")
    if not (metric_col and period_col and value_col):
        return {}
    qa_col = headers.get("qa_severity")
    source_class_col = headers.get("source_class")
    source_type_col = headers.get("source_type")
    facts: Dict[str, Any] = {}
    wanted_metric = _slug(metric_name)
    for rr in range(2, int(ws.max_row or 0) + 1):
        if _slug(ws.cell(rr, metric_col).value) != wanted_metric:
            continue
        value = ws.cell(rr, value_col).value
        if not _source_value_present(value):
            continue
        qa = _text(ws.cell(rr, qa_col).value).lower() if qa_col else ""
        source_class = _text(ws.cell(rr, source_class_col).value).lower() if source_class_col else ""
        source_type = _text(ws.cell(rr, source_type_col).value).lower() if source_type_col else ""
        if qa == "fail" or source_class == "missing" or source_type == "missing":
            continue
        quarter = _quarter_label(ws.cell(rr, period_col).value)
        if quarter:
            facts[quarter] = value
    return facts


def _annual_segment_asset_source_values(wb: Any) -> Dict[Tuple[str, int], Any]:
    source_values: Dict[Tuple[str, int], Any] = {}
    allowed_sheets = ("Segment_Source_Audit", "DATA_Segments_Long", "Slides_Segments")
    allowed_metrics = {"assets", "total_assets", "segment_assets", "segment_total_assets"}
    for sheet_name in allowed_sheets:
        if sheet_name not in getattr(wb, "sheetnames", []):
            continue
        ws = wb[sheet_name]
        headers = _header_map(ws)
        metric_col = headers.get("metric")
        segment_col = headers.get("segment")
        value_col = headers.get("value")
        if not (metric_col and segment_col and value_col):
            continue
        period_col = headers.get("period_end") or headers.get("quarter") or headers.get("year")
        period_type_col = headers.get("period_type")
        source_period_col = headers.get("source_period_label")
        if not period_col:
            continue
        for rr in range(2, int(ws.max_row or 0) + 1):
            metric = _slug(ws.cell(rr, metric_col).value)
            if metric not in allowed_metrics:
                continue
            value = ws.cell(rr, value_col).value
            if not _source_value_present(value):
                continue
            period_type = _text(ws.cell(rr, period_type_col).value).lower() if period_type_col else ""
            source_period = _text(ws.cell(rr, source_period_col).value).lower() if source_period_col else ""
            if period_type and "annual" not in period_type and "year" not in period_type:
                continue
            if not period_type and source_period and "annual" not in source_period and "year" not in source_period:
                continue
            segment = _slug(ws.cell(rr, segment_col).value)
            year = _year_label(ws.cell(rr, period_col).value)
            if segment and year:
                source_values[(segment, year)] = value
    return source_values


def _scan_source_backed_bs_missing_value_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "BS_Segments" not in getattr(wb, "sheetnames", []):
        return []
    ws = wb["BS_Segments"]
    issues: List[WorkbookQualityIssue] = []

    header_row = _find_label_row(ws, "Quarter")
    debt_sources = _trusted_facts_by_metric_quarter(wb, "debt_current")
    if header_row and debt_sources:
        quarter_cols = _quarter_columns(ws, header_row)
        debt_labels = {"current_maturities_of_long_term_debt"}
        for rr in range(header_row + 1, int(ws.max_row or 0) + 1):
            label = _text(ws.cell(rr, 1).value)
            if _slug(label) not in debt_labels:
                continue
            if _has_safe_missing_exception(*_row_values(ws, rr)):
                continue
            for quarter, cc in quarter_cols.items():
                if quarter in debt_sources and _is_blank(ws.cell(rr, cc).value):
                    issues.append(
                        _issue(
                            severity="P1",
                            ticker=ticker,
                            sheet=ws.title,
                            row=rr,
                            metric_label=label,
                            reason=f"Visible {label!r} is blank for {quarter} even though DATA_Facts_Long.debt_current has a trusted source value.",
                            owner="BS_Segments source-backed balance-sheet hydration",
                            rule_id="source_backed_missing_bs_segment_value",
                        )
                    )

    annual_asset_sources = _annual_segment_asset_source_values(wb)
    if annual_asset_sources:
        annual_start = _find_label_row(ws, "Annual segments")
        year_row = _find_label_row(ws, "Year", start_row=annual_start + 1) if annual_start else 0
        if year_row:
            year_cols = _year_columns(ws, year_row)
            in_total_assets = False
            for rr in range(year_row + 1, int(ws.max_row or 0) + 1):
                label = _text(ws.cell(rr, 1).value)
                label_slug = _slug(label)
                if not label_slug:
                    continue
                if label_slug == "total_assets":
                    in_total_assets = True
                    continue
                if label_slug in {"revenues", "revenue", "gross_margin", "depreciation_amortization", "operating_income_loss"}:
                    in_total_assets = False
                if not in_total_assets or _has_safe_missing_exception(*_row_values(ws, rr)):
                    continue
                segment = _slug(label)
                for year, cc in year_cols.items():
                    if (segment, year) in annual_asset_sources and _is_blank(ws.cell(rr, cc).value):
                        issues.append(
                            _issue(
                                severity="P1",
                                ticker=ticker,
                                sheet=ws.title,
                                row=rr,
                                metric_label=label,
                                reason=f"Annual segment Total assets is blank for {label!r} {year} even though a segment asset source row exists.",
                                owner="BS_Segments annual segment asset hydration",
                                rule_id="source_backed_missing_bs_segment_value",
                            )
                        )
    return issues


def _promise_source_audit_by_hidden_key(wb: Any) -> Dict[str, Dict[str, Any]]:
    audit: Dict[str, Dict[str, Any]] = {}
    for sheet_name in ("Promise_Source_Audit", "Promise_Progress_Audit", "Guidance_Audit", "Guidance_Normalized", "Slides_Guidance"):
        if sheet_name not in getattr(wb, "sheetnames", []):
            continue
        ws = wb[sheet_name]
        headers = _header_map(ws)
        key_col = (
            headers.get("hidden_key")
            or headers.get("source_key")
            or headers.get("guidance_key")
            or headers.get("key")
        )
        if not key_col:
            continue
        actual_col = headers.get("actual") or headers.get("actual_value") or headers.get("actual_text")
        progress_col = (
            headers.get("progress")
            or headers.get("progress / run-rate")
            or headers.get("progress_value")
            or headers.get("run_rate")
        )
        if not (actual_col or progress_col):
            continue
        for rr in range(2, int(ws.max_row or 0) + 1):
            key = _text(ws.cell(rr, key_col).value)
            if not key:
                continue
            row = {
                "actual": ws.cell(rr, actual_col).value if actual_col else None,
                "progress": ws.cell(rr, progress_col).value if progress_col else None,
            }
            if _source_value_present(row["actual"]) or _source_value_present(row["progress"]):
                audit[key.lower()] = row
    return audit


def _scan_source_backed_promise_missing_value_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "Promise_Progress_UI" not in getattr(wb, "sheetnames", []):
        return []
    audit = _promise_source_audit_by_hidden_key(wb)
    if not audit:
        return []
    ws = wb["Promise_Progress_UI"]
    issues: List[WorkbookQualityIssue] = []
    for _section, rows in _promise_revision_blocks(ws).items():
        for row_idx, row in rows:
            hidden_key = _text(row.get("hidden_key"))
            if not hidden_key or _has_safe_missing_exception(*row.values()):
                continue
            source_row = audit.get(hidden_key.lower())
            if not source_row:
                continue
            missing_fields: List[str] = []
            if _source_value_present(source_row.get("actual")) and _is_blank(row.get("actual")):
                missing_fields.append("Actual")
            if _source_value_present(source_row.get("progress")) and _is_blank(row.get("progress / run-rate")):
                missing_fields.append("Progress / run-rate")
            if missing_fields:
                metric = _text(row.get("metric") or row.get("milestone"))
                issues.append(
                    _issue(
                        severity="P1",
                        ticker=ticker,
                        sheet=ws.title,
                        row=row_idx,
                        metric_label=metric,
                        reason=f"Visible Promise {' and '.join(missing_fields)} is blank even though hidden source key {hidden_key!r} has source-backed value(s).",
                        owner="Promise_Progress_UI source-backed value hydration",
                        rule_id="source_backed_missing_promise_visible_value",
                    )
                )
    return issues


SEGMENT_GROUP_LABELS = {
    "revenue",
    "revenues",
    "adjusted_ebit",
    "segment_operating_margin",
    "segment_operating_margin_pct",
    "ebit_margin",
    "ebit_margin_pct",
    "gross_margin",
    "depreciation_amortization",
    "adjusted_ebitda",
    "operating_income_loss",
}


def _canonical_segment_group(label: Any) -> str:
    slug = _slug(label).replace("_percent", "_pct")
    if slug == "depreciation_amortization":
        return slug
    slug = slug.replace("_", " ")
    slug = slug.replace("&", "and")
    return _slug(slug).replace("_percent", "_pct")


def _segment_quarter_values(ws: Any, *, table_header: str, start_section: str = "") -> Tuple[Dict[Tuple[str, str, str], Any], Dict[Tuple[str, str, str], int]]:
    header_row = _find_label_row(ws, table_header)
    if not header_row:
        return {}, {}
    quarter_cols = _quarter_columns(ws, header_row)
    if not quarter_cols:
        return {}, {}
    start_row = header_row + 1
    if start_section:
        section_row = _find_label_row(ws, start_section)
        if section_row and section_row < header_row:
            start_row = section_row + 1
    values: Dict[Tuple[str, str, str], Any] = {}
    rows: Dict[Tuple[str, str, str], int] = {}
    current_group = ""
    stop_labels = {"annual_segments", "current_latest_outlook", "recent_quarter_commentary"}
    for rr in range(start_row, int(ws.max_row or 0) + 1):
        label = _text(ws.cell(rr, 1).value)
        label_slug = _slug(label)
        if not label_slug:
            continue
        if label_slug in stop_labels and rr > start_row:
            break
        group = _canonical_segment_group(label)
        if group in SEGMENT_GROUP_LABELS:
            current_group = group
            continue
        if not current_group or _has_safe_missing_exception(*_row_values(ws, rr)):
            continue
        segment = _slug(label)
        for quarter, cc in quarter_cols.items():
            key = (current_group, segment, quarter)
            values[key] = ws.cell(rr, cc).value
            rows[key] = rr
    return values, rows


def _scan_source_backed_operating_driver_missing_value_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "BS_Segments" not in getattr(wb, "sheetnames", []) or "Operating_Drivers" not in getattr(wb, "sheetnames", []):
        return []
    bs_values, _bs_rows = _segment_quarter_values(wb["BS_Segments"], table_header="Quarter", start_section="Quarterly segments")
    if not bs_values:
        return []
    od_values, od_rows = _segment_quarter_values(wb["Operating_Drivers"], table_header="Metric / segment")
    issues: List[WorkbookQualityIssue] = []
    for key, bs_value in bs_values.items():
        if not _source_value_present(bs_value):
            continue
        if key not in od_values or not _is_blank(od_values[key]):
            continue
        group, segment, quarter = key
        row_idx = od_rows.get(key, 0)
        issues.append(
            _issue(
                severity="P1",
                ticker=ticker,
                sheet="Operating_Drivers",
                row=row_idx,
                metric_label=f"{group} / {segment}",
                reason=f"Operating_Drivers is blank for {segment} {quarter} even though BS_Segments has the same metric/segment/quarter value.",
                owner="Operating_Drivers direct BS_Segments segment mapping",
                rule_id="source_backed_missing_operating_driver_value",
            )
        )
    return issues


COMPARISON_BUCKET_RGBS = {"A63A00", "D55E00", "DDDDDD", "9BD3F5", "2F80ED"}
VALUATION_COMPARISON_LABELS = {
    "revenue",
    "gross_margin_pct",
    "operating_margin_pct",
    "operating_margin_ttm",
    "ebitda_margin_pct",
    "adj_ebitda_margin_pct",
    "ebit_margin_pct",
    "net_income_attrib_to_a_and_f_margin_pct",
    "capex_pct_of_revenue",
    "fcf_cfo_capex",
    "owner_earnings_proxy",
    "fcf_margin_pct",
    "current_ratio",
    "eps_gaap",
    "adj_eps",
    "bv_share",
    "fcf_share_ttm",
    "debt_core_borrowings",
    "net_debt_core_borrowings",
    "net_leverage",
    "net_leverage_adj",
    "interest_coverage",
    "cash_interest_coverage",
}


def _fill_rgb6(cell: Any) -> str:
    try:
        rgb = _text(cell.fill.fgColor.rgb).upper()
    except Exception:
        return ""
    if not rgb or rgb == "00000000":
        return ""
    return rgb[-6:] if len(rgb) >= 6 else rgb


def _has_comparison_bucket_fill(cell: Any) -> bool:
    try:
        fill_type = _text(cell.fill.fill_type).lower()
    except Exception:
        return False
    return fill_type == "solid" and _fill_rgb6(cell) in COMPARISON_BUCKET_RGBS


def _coerce_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or _is_formula(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    txt = _text(value)
    if not txt or txt.lower() in {"n/m", "nm", "n/a", "na"}:
        return None
    cleaned = txt.replace(",", "").replace("$", "").replace("%", "").strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1].strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _comparison_label_key(label: Any) -> str:
    return _slug(_text(label).replace("&", " and ").replace("%", " pct"))


def _is_known_comparison_metric(label: Any, context_label: Any = "") -> bool:
    key = _comparison_label_key(f"{context_label} {label}")
    if not key:
        return False
    tokens = set(key.split("_"))
    if tokens.intersection({"inventory", "buybacks", "dividends", "acquisitions", "revolver"}):
        return False
    if tokens.intersection({"revenue", "revenues", "sales", "margin", "ratio", "eps", "fcf", "debt", "leverage", "coverage", "yoy", "qoq", "delta", "growth", "comp"}):
        return True
    return any(
        phrase in key
        for phrase in (
            "free_cash_flow",
            "owner_earnings",
            "net_debt",
            "ultra_high_protein",
            "renewable_corn_oil",
            "ethanol_gallons",
            "distillers",
        )
    )


def _is_known_valuation_comparison_metric(label: Any) -> bool:
    return _comparison_label_key(label) in VALUATION_COMPARISON_LABELS


def _is_direct_comparison_metric(label: Any, context_label: Any = "") -> bool:
    key = _comparison_label_key(f"{context_label} {label}")
    if not key:
        return False
    tokens = set(key.split("_"))
    if tokens.intersection({"yoy", "qoq", "delta", "growth"}):
        return True
    return "comp" in tokens and "company" not in tokens


def _comparison_basis_for_label(label: Any, context_label: Any = "") -> str:
    key = _comparison_label_key(f"{context_label} {label}")
    if _is_direct_comparison_metric(label, context_label):
        return "direct"
    if "qoq" in key:
        return "qoq"
    return "yoy"


def _prior_quarter_label(quarter: str, *, basis: str) -> str:
    parts = _quarter_parts(quarter)
    if not parts:
        return ""
    year, qtr = parts
    if basis == "qoq":
        if qtr == 1:
            return f"{year - 1}-Q4"
        return f"{year}-Q{qtr - 1}"
    return f"{year - 1}-Q{qtr}"


def _clean_comparator_state(
    row_values: Mapping[str, Any],
    quarter: str,
    *,
    basis: str,
) -> Tuple[bool, str]:
    current = _coerce_number(row_values.get(quarter))
    if current is None:
        return False, "current value is not numeric"
    if basis == "direct":
        return True, "direct YoY/QoQ/delta value exists"
    prior_quarter = _prior_quarter_label(quarter, basis=basis)
    if not prior_quarter:
        return False, "quarter label cannot be compared"
    if prior_quarter not in row_values:
        return False, f"missing {basis.upper()} comparator {prior_quarter}"
    previous = _coerce_number(row_values.get(prior_quarter))
    if previous is None:
        return False, f"invalid {basis.upper()} comparator {prior_quarter}"
    if abs(previous) <= 1e-12:
        return False, f"tiny/zero {basis.upper()} comparator {prior_quarter}"
    return True, f"clean {basis.upper()} comparator {prior_quarter}"


def _has_possible_hidden_comparison_source(wb: Any, sheet_name: str) -> bool:
    sheets = set(getattr(wb, "sheetnames", []))
    if sheet_name == "Valuation":
        return bool(sheets.intersection({"DATA_Facts_Long", "History_Q", "Hidden_Value_Base"}))
    if sheet_name == "Operating_Drivers":
        return bool(sheets.intersection({"Slides_Segments", "operating_drivers_raw", "History_Q"}))
    return False


def _comparison_header_rows(ws: Any) -> List[int]:
    rows: List[int] = []
    for rr in range(1, int(ws.max_row or 0) + 1):
        label = _text(ws.cell(rr, 1).value)
        if label in {"Quarter", "Metric / segment"} and _quarter_columns(ws, rr):
            rows.append(rr)
    return rows


def _row_quarter_values(ws: Any, row_idx: int, quarter_cols: Mapping[str, int]) -> Dict[str, Any]:
    return {quarter: ws.cell(row_idx, col).value for quarter, col in quarter_cols.items()}


def _scan_comparison_coloring_table(
    wb: Any,
    ticker: str,
    ws: Any,
    *,
    header_row: int,
) -> List[WorkbookQualityIssue]:
    quarter_cols = _quarter_columns(ws, header_row)
    if not quarter_cols:
        return []
    issues: List[WorkbookQualityIssue] = []
    current_group = ""
    stop_labels = {
        "current_latest_outlook",
        "recent_quarter_commentary",
        "hidden_value_flags",
        "scenario_sensitivity",
    }
    for rr in range(header_row + 1, int(ws.max_row or 0) + 1):
        label = _text(ws.cell(rr, 1).value)
        label_key = _slug(label)
        if not label_key:
            continue
        if label_key in stop_labels and rr > header_row + 1:
            break
        row_values = _row_quarter_values(ws, rr, quarter_cols)
        row_has_value = any(not _is_blank(value) for value in row_values.values())
        if ws.title == "Valuation":
            if not row_has_value or not _is_known_valuation_comparison_metric(label):
                continue
            context_label = ""
        else:
            if not row_has_value:
                if _is_known_comparison_metric(label):
                    current_group = label
                continue
            context_label = current_group if not _is_known_comparison_metric(label) else ""
            if not _is_known_comparison_metric(label, context_label):
                continue
        basis = _comparison_basis_for_label(label, context_label)
        metric_label = label if not context_label else f"{context_label} / {label}"
        for quarter, cc in quarter_cols.items():
            cell = ws.cell(rr, cc)
            if _is_blank(cell.value):
                continue
            has_bucket = _has_comparison_bucket_fill(cell)
            has_clean, reason = _clean_comparator_state(row_values, quarter, basis=basis)
            if has_bucket and not has_clean:
                if reason.startswith("missing") and _has_possible_hidden_comparison_source(wb, ws.title):
                    continue
                issues.append(
                    _issue(
                        severity="P2",
                        ticker=ticker,
                        sheet=ws.title,
                        row=rr,
                        metric_label=metric_label,
                        reason=f"Cell {quarter} has comparison bucket fill but no clean comparator evidence: {reason}.",
                        owner=f"{ws.title} comparison coloring readback",
                        rule_id="comparison_coloring_without_clean_comparator",
                    )
                )
            elif has_clean and not has_bucket:
                issues.append(
                    _issue(
                        severity="P2",
                        ticker=ticker,
                        sheet=ws.title,
                        row=rr,
                        metric_label=metric_label,
                        reason=f"Cell {quarter} is neutral even though {reason}.",
                        owner=f"{ws.title} comparison coloring readback",
                        rule_id="comparison_coloring_clean_comparator_neutral",
                    )
                )
    return issues


def _scan_comparison_coloring_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    issues: List[WorkbookQualityIssue] = []
    for sheet_name in ("Valuation", "Operating_Drivers"):
        if sheet_name not in getattr(wb, "sheetnames", []):
            continue
        ws = wb[sheet_name]
        for header_row in _comparison_header_rows(ws):
            issues.extend(_scan_comparison_coloring_table(wb, ticker, ws, header_row=header_row))
    deduped: Dict[Tuple[str, int, str, str], WorkbookQualityIssue] = {}
    for issue in issues:
        deduped.setdefault((issue.sheet, issue.row, issue.metric_label, issue.reason), issue)
    return list(deduped.values())


def _is_formula(value: Any) -> bool:
    return isinstance(value, str) and value.strip().startswith("=")


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _trigger_state(value: Any) -> Optional[bool]:
    if _is_formula(value):
        return None
    if value in (1, True):
        return True
    if value in (0, False):
        return False
    txt = _text(value).lower()
    if txt in {"1", "true", "yes"}:
        return True
    if txt in {"0", "false", "no"}:
        return False
    return None


def _hidden_value_rows(ws: Any) -> List[Tuple[int, Dict[str, Any]]]:
    headers = _header_map(ws)
    rows: List[Tuple[int, Dict[str, Any]]] = []
    for rr in range(2, int(ws.max_row or 0) + 1):
        row = {name: ws.cell(rr, col).value for name, col in headers.items()}
        if _text(row.get("flag_code")) or _text(row.get("title")):
            rows.append((rr, row))
    return rows


def _valuation_hidden_flag_titles(ws: Any) -> set[str]:
    header_row = 0
    for rr in range(1, int(ws.max_row or 0) + 1):
        if _text(ws.cell(rr, 1).value) == "Hidden value flags":
            header_row = rr
            break
    if not header_row:
        return set()
    titles: set[str] = set()
    stop_labels = {
        "Operating signals",
        "Cash Flow",
        "Leverage & Liquidity",
        "Equity / Per-share",
        "Scenario / sensitivity",
    }
    for rr in range(header_row + 2, int(ws.max_row or 0) + 1):
        flag_label = _text(ws.cell(rr, 1).value)
        title = _text(ws.cell(rr, 2).value)
        if flag_label in stop_labels:
            break
        if re.fullmatch(r"Flag\s+\d+", flag_label, flags=re.I) and title:
            titles.add(title.lower())
    return titles


def _price_linked_inputs_unavailable(row: Mapping[str, Any]) -> bool:
    metrics = _text(row.get("metrics_json"))
    support = _text(row.get("visible_support")).lower()
    if '"fcf_yield": null' in metrics.lower() or '"price": null' in metrics.lower():
        return True
    if "(price-linked)" in support and re.search(r"\bunavailable|missing|wait\b", support, re.I):
        return True
    return False


def _scan_hidden_value_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    if "Hidden_Value_Flags" not in getattr(wb, "sheetnames", []):
        return []
    flags_ws = wb["Hidden_Value_Flags"]
    valuation_ws = wb["Valuation"] if "Valuation" in getattr(wb, "sheetnames", []) else None
    valuation_titles = _valuation_hidden_flag_titles(valuation_ws) if valuation_ws is not None else set()
    issues: List[WorkbookQualityIssue] = []
    for rr, row in _hidden_value_rows(flags_ws):
        code = _text(row.get("flag_code"))
        title = _text(row.get("title"))
        score = row.get("score")
        triggered = row.get("triggered")
        trigger_state = _trigger_state(triggered)
        score_ok = _is_numeric(score) or _is_formula(score)
        trigger_ok = trigger_state is not None or _is_formula(triggered)
        if not score_ok or not trigger_ok:
            issues.append(
                _issue(
                    severity="P2",
                    ticker=ticker,
                    sheet=flags_ws.title,
                    row=rr,
                    metric_label=title or code,
                    reason="Hidden_Value_Flags score and triggered fields should be numeric/nonblank or formula-backed.",
                    owner="Hidden_Value_Flags scoring/readback contract",
                    rule_id="hidden_value_score_or_trigger_blank",
                )
            )
        if code in {"C", "E"} and trigger_state is True and _price_linked_inputs_unavailable(row):
            issues.append(
                _issue(
                    severity="P1",
                    ticker=ticker,
                    sheet=flags_ws.title,
                    row=rr,
                    metric_label=title or code,
                    reason="Price-linked hidden-value flag is triggered while required price/FCF yield inputs are unavailable.",
                    owner="Hidden_Value_Flags price-linked trigger contract",
                    rule_id="hidden_value_price_linked_trigger_without_inputs",
                )
            )
        if valuation_ws is None or not title:
            continue
        title_key = title.lower()
        if trigger_state is True and title_key not in valuation_titles:
            issues.append(
                _issue(
                    severity="P1",
                    ticker=ticker,
                    sheet="Valuation",
                    row=0,
                    metric_label=title,
                    reason="Triggered Hidden_Value_Flags row does not appear in the Valuation hidden-value display.",
                    owner="Valuation hidden-value display sync",
                    rule_id="hidden_value_triggered_missing_from_valuation",
                )
            )
        if trigger_state is False and title_key in valuation_titles:
            issues.append(
                _issue(
                    severity="P1",
                    ticker=ticker,
                    sheet="Valuation",
                    row=0,
                    metric_label=title,
                    reason="Non-triggered Hidden_Value_Flags row appears as an active Valuation hidden-value flag.",
                    owner="Valuation hidden-value display sync",
                    rule_id="hidden_value_nontriggered_leaked_to_valuation",
                )
            )
    return issues


def run_workbook_quality_guardrails(wb: Any, ticker: str) -> List[WorkbookQualityIssue]:
    ticker_u = str(ticker or "").strip().upper()
    issues: List[WorkbookQualityIssue] = []
    issues.extend(_scan_promise_horizon_guardrails(wb, ticker_u))
    issues.extend(_scan_promise_hidden_key_guardrails(wb, ticker_u))
    issues.extend(_scan_quarter_narrative_amount_guardrails(wb, ticker_u))
    issues.extend(_scan_sector_specific_guardrails(wb, ticker_u))
    issues.extend(_scan_source_backed_bs_missing_value_guardrails(wb, ticker_u))
    issues.extend(_scan_source_backed_promise_missing_value_guardrails(wb, ticker_u))
    issues.extend(_scan_source_backed_operating_driver_missing_value_guardrails(wb, ticker_u))
    issues.extend(_scan_comparison_coloring_guardrails(wb, ticker_u))
    issues.extend(_scan_hidden_value_guardrails(wb, ticker_u))
    return issues
