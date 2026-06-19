"""Operating_Drivers workbook-reader support helpers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, MutableMapping, Optional, Set


@dataclass(frozen=True)
class OperatingDriverWorkbookSupportDeps:
    runtime: MutableMapping[str, Any]


class OperatingDriverWorkbookSupport:
    def __init__(self, deps: OperatingDriverWorkbookSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    @property
    def _re(self) -> Any:
        return self.runtime.get("re", re)

    @property
    def _date_type(self) -> Any:
        return self.runtime.get("date", date)

    def _date_or_none(self, value: Any) -> Optional[date]:
        return self.runtime["_date_or_none"](value)

    def operating_driver_ttm_sum_from_workbook(self, wb: Any, metric_label: str) -> Optional[float]:
        """Return the latest-four-quarter sum for an Operating_Drivers metric when clean."""

        re_local = self._re
        date_type = self._date_type

        if "Operating_Drivers" not in wb.sheetnames:
            return None
        ws = wb["Operating_Drivers"]
        metric_row: Optional[int] = None
        for rr in range(1, ws.max_row + 1):
            if str(ws.cell(rr, 1).value or "").strip().lower() == metric_label.strip().lower():
                metric_row = rr
                break
        if metric_row is None:
            return None

        quarter_row: Optional[int] = None
        for rr in range(max(1, metric_row - 25), metric_row + 1):
            if str(ws.cell(rr, 1).value or "").strip().lower() == "quarter":
                quarter_row = rr
                break
        if quarter_row is None:
            return None

        def _quarter_label_end_date(label: Any) -> Optional[date]:
            m = re_local.fullmatch(r"\s*(\d{4})-Q([1-4])\s*", str(label or ""), flags=re_local.I)
            if not m:
                return self._date_or_none(label)
            year = int(m.group(1))
            qtr = int(m.group(2))
            month = qtr * 3
            day = 31 if month in {3, 12} else 30
            return date_type(year, month, day)

        latest_history_q: Optional[date] = None
        if "History_Q" in wb.sheetnames:
            try:
                hws = wb["History_Q"]
                h_headers = [str(hws.cell(1, cc).value or "").strip().lower() for cc in range(1, hws.max_column + 1)]
                if "quarter" in h_headers:
                    q_col = h_headers.index("quarter") + 1
                    hist_dates = []
                    for rr in range(2, hws.max_row + 1):
                        qd = self._date_or_none(hws.cell(rr, q_col).value)
                        if isinstance(qd, date_type):
                            hist_dates.append(qd)
                    if hist_dates:
                        latest_history_q = max(hist_dates)
            except Exception:
                latest_history_q = None

        quarter_cols = []
        for cc in range(2, ws.max_column + 1):
            val = str(ws.cell(quarter_row, cc).value or "").strip()
            if re_local.fullmatch(r"\d{4}-Q[1-4]", val):
                qd = _quarter_label_end_date(val)
                if latest_history_q is not None and isinstance(qd, date_type) and qd > latest_history_q:
                    continue
                quarter_cols.append(cc)
        if not quarter_cols:
            return None

        vals = []
        for cc in quarter_cols[-4:]:
            val = ws.cell(metric_row, cc).value
            if val is None or val == "":
                vals.append(0.0)
                continue
            try:
                vals.append(float(val))
            except (TypeError, ValueError):
                return None
        total = float(sum(vals))
        return total if any(abs(v) > 1e-9 for v in vals) else None

    def operating_driver_latest_full_year_sum_from_workbook(self, wb: Any, metric_label: str) -> Optional[float]:
        """Return the latest full-year sum for an Operating_Drivers metric when clean."""

        re_local = self._re
        date_type = self._date_type

        if "Operating_Drivers" not in wb.sheetnames:
            return None
        ws = wb["Operating_Drivers"]
        metric_row: Optional[int] = None
        for rr in range(1, ws.max_row + 1):
            if str(ws.cell(rr, 1).value or "").strip().lower() == metric_label.strip().lower():
                metric_row = rr
                break
        if metric_row is None:
            return None

        quarter_row: Optional[int] = None
        for rr in range(max(1, metric_row - 25), metric_row + 1):
            if str(ws.cell(rr, 1).value or "").strip().lower() == "quarter":
                quarter_row = rr
                break
        if quarter_row is None:
            return None

        def _quarter_label_end_date(label: Any) -> Optional[date]:
            m = re_local.fullmatch(r"\s*(\d{4})-Q([1-4])\s*", str(label or ""), flags=re_local.I)
            if not m:
                return self._date_or_none(label)
            year = int(m.group(1))
            qtr = int(m.group(2))
            month = qtr * 3
            day = 31 if month in {3, 12} else 30
            return date_type(year, month, day)

        latest_history_q: Optional[date] = None
        if "History_Q" in wb.sheetnames:
            try:
                hws = wb["History_Q"]
                h_headers = [str(hws.cell(1, cc).value or "").strip().lower() for cc in range(1, hws.max_column + 1)]
                if "quarter" in h_headers:
                    q_col = h_headers.index("quarter") + 1
                    hist_dates = []
                    for rr in range(2, hws.max_row + 1):
                        qd = self._date_or_none(hws.cell(rr, q_col).value)
                        if isinstance(qd, date_type):
                            hist_dates.append(qd)
                    if hist_dates:
                        latest_history_q = max(hist_dates)
            except Exception:
                latest_history_q = None

        values_by_year: Dict[int, List[float]] = {}
        quarters_by_year: Dict[int, Set[int]] = {}
        latest_year: Optional[int] = None
        for cc in range(2, ws.max_column + 1):
            label = str(ws.cell(quarter_row, cc).value or "").strip()
            m = re_local.fullmatch(r"(\d{4})-Q([1-4])", label)
            if not m:
                continue
            qd = _quarter_label_end_date(label)
            if latest_history_q is not None and isinstance(qd, date_type) and qd > latest_history_q:
                continue
            year = int(m.group(1))
            quarter = int(m.group(2))
            latest_year = year if latest_year is None else max(latest_year, year)
            val = ws.cell(metric_row, cc).value
            parsed = 0.0
            if val not in (None, ""):
                try:
                    parsed = float(val)
                except (TypeError, ValueError):
                    return None
            values_by_year.setdefault(year, []).append(parsed)
            quarters_by_year.setdefault(year, set()).add(quarter)

        if latest_year is None:
            return None
        candidate_years = [
            year
            for year, quarters in quarters_by_year.items()
            if year < latest_year or quarters == {1, 2, 3, 4}
        ]
        if not candidate_years:
            return None
        year = max(candidate_years)
        vals = values_by_year.get(year, [])
        total = float(sum(vals))
        return total if any(abs(v) > 1e-9 for v in vals) else None
