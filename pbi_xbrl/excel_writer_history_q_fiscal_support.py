"""History_Q fiscal-period and latest full-year support helpers."""
from __future__ import annotations

import math
import re
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class HistoryQFiscalSupportDeps:
    runtime: MutableMapping[str, Any]


@dataclass(frozen=True)
class _FiscalPeriodProfile:
    year_end_month: int = 12
    year_end_day: int = 31
    year_label: str = "end"


class HistoryQFiscalSupport:
    def __init__(self, deps: HistoryQFiscalSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    @property
    def _pd(self) -> Any:
        return self.runtime.get("pd", pd)

    @property
    def _math(self) -> Any:
        return self.runtime.get("math", math)

    @property
    def _re(self) -> Any:
        return self.runtime.get("re", re)

    def _date_or_none(self, value: Any) -> Optional[date]:
        return self.runtime["_date_or_none"](value)

    def _safe_date(self, year: int, month: int, day: int) -> date:
        month = max(1, min(12, int(month)))
        day = max(1, min(31, int(day)))
        while True:
            try:
                return date(int(year), month, day)
            except ValueError:
                day -= 1

    def fiscal_profile_from_workbook(
        self,
        wb: Any,
        ticker: Any = "",
        fiscal_profile: Any = None,
    ) -> _FiscalPeriodProfile:
        """Resolve fiscal-year-end behavior for visible period labels and annual defaults.

        Priority is explicit caller profile, workbook/profile text, known company
        profile fallback, then calendar-year reporting.  The year-label mode is
        intentionally explicit because retailers like ANF label the year ended
        January 2026 as 2025 year, while calendar reporters use the end year.
        """

        pd_local = self._pd
        re_local = self._re

        def _profile(month: Any, day: Any, label: Any = "") -> _FiscalPeriodProfile:
            m = int(month or 12)
            d = int(day or 31)
            mode = str(label or "").strip().lower()
            if mode not in {"start", "end"}:
                mode = "start" if m <= 2 else "end"
            return _FiscalPeriodProfile(m, d, mode)

        if isinstance(fiscal_profile, _FiscalPeriodProfile):
            return fiscal_profile
        if isinstance(fiscal_profile, Mapping):
            month = fiscal_profile.get("year_end_month") or fiscal_profile.get("fiscal_year_end_month")
            day = fiscal_profile.get("year_end_day") or fiscal_profile.get("fiscal_year_end_day")
            if month and day:
                return _profile(month, day, fiscal_profile.get("year_label") or fiscal_profile.get("fiscal_year_label"))
        if isinstance(fiscal_profile, (tuple, list)) and len(fiscal_profile) >= 2:
            return _profile(fiscal_profile[0], fiscal_profile[1], fiscal_profile[2] if len(fiscal_profile) > 2 else "")

        if wb is not None:
            for sheet_name in ("SUMMARY", "Summary", "Model_Info", "QA_Checks"):
                if sheet_name not in getattr(wb, "sheetnames", []):
                    continue
                ws = wb[sheet_name]
                for row in ws.iter_rows(min_row=1, max_row=min(int(ws.max_row or 0), 80), min_col=1, max_col=min(int(ws.max_column or 0), 10), values_only=True):
                    blob = " ".join(str(v) for v in row if v not in (None, ""))
                    if not blob:
                        continue
                    m = re_local.search(r"\b(?:FY|fiscal year|year)\s*end(?:ed)?\s*(?:\(|:)?\s*(20\d{2})-(\d{1,2})-(\d{1,2})", blob, re_local.I)
                    if m:
                        return _profile(m.group(2), m.group(3), "")
                    m = re_local.search(r"\b(?:FY|fiscal year|year)\s*end(?:ed)?\s*(?:\(|:)?\s*([A-Za-z]+)\s+(\d{1,2})", blob, re_local.I)
                    if m:
                        try:
                            month = pd_local.to_datetime(m.group(1), format="%B", errors="coerce")
                            if pd_local.isna(month):
                                month = pd_local.to_datetime(m.group(1), format="%b", errors="coerce")
                            if not pd_local.isna(month):
                                return _profile(int(pd_local.Timestamp(month).month), int(m.group(2)), "")
                        except Exception:
                            pass

        ticker_txt = str(ticker or "").strip().upper()
        ticker_profiles = {
            "ANF": _FiscalPeriodProfile(1, 31, "start"),
        }
        return ticker_profiles.get(ticker_txt, _FiscalPeriodProfile())

    def _explicit_quarter_label_key(self, value: Any) -> Optional[Tuple[int, int]]:
        txt = str(value or "").strip()
        re_local = self._re
        m = re_local.search(r"\b(20\d{2})\s*[-_/ ]?\s*Q([1-4])\b", txt, flags=re_local.I)
        if m:
            return int(m.group(1)), int(m.group(2))
        m = re_local.search(r"\bQ([1-4])\s*[-_/ ]?\s*(20\d{2})\b", txt, flags=re_local.I)
        if m:
            return int(m.group(2)), int(m.group(1))
        return None

    def _resolve_fiscal_period_from_date(self, qd: date, profile: _FiscalPeriodProfile) -> Tuple[int, int, str, date]:
        candidates = [
            self._safe_date(int(qd.year) + year_offset, profile.year_end_month, profile.year_end_day)
            for year_offset in (-1, 0, 1)
        ]
        eligible = [cand for cand in candidates if -10 <= (cand - qd).days <= 370]
        fy_end = min(eligible or candidates, key=lambda cand: abs((cand - qd).days))
        days_to_fy_end = (fy_end - qd).days
        if days_to_fy_end <= 45:
            fq = 4
        elif days_to_fy_end <= 135:
            fq = 3
        elif days_to_fy_end <= 225:
            fq = 2
        else:
            fq = 1
        fy = int(fy_end.year) - 1 if profile.year_label == "start" else int(fy_end.year)
        return fy, fq, f"{fy}-Q{fq}", fy_end

    def resolve_history_q_fiscal_periods_from_workbook(
        self,
        wb: Any,
        *,
        ticker: Any = "",
        fiscal_profile: Any = None,
    ) -> List[Dict[str, Any]]:
        if wb is None or "History_Q" not in getattr(wb, "sheetnames", []):
            return []
        ws = wb["History_Q"]
        if int(ws.max_row or 0) < 2 or int(ws.max_column or 0) < 1:
            return []

        re_local = self._re
        pd_local = self._pd

        def _norm(value: Any) -> str:
            return re_local.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

        headers = {_norm(ws.cell(1, cc).value): cc for cc in range(1, int(ws.max_column or 0) + 1)}

        def _col(*aliases: str) -> Optional[int]:
            for alias in aliases:
                cc = headers.get(_norm(alias))
                if cc is not None:
                    return cc
            return None

        quarter_col = _col("quarter", "period", "fiscal quarter", "fiscal_period")
        fiscal_year_col = _col("fiscal_year", "fiscal year", "fy")
        fiscal_quarter_col = _col("fiscal_quarter", "fiscal quarter", "fq")
        if quarter_col is None:
            return []
        profile = self.fiscal_profile_from_workbook(wb, ticker=ticker, fiscal_profile=fiscal_profile)
        out: List[Dict[str, Any]] = []
        for rr in range(2, int(ws.max_row or 0) + 1):
            raw_quarter = ws.cell(rr, quarter_col).value
            explicit = self._explicit_quarter_label_key(raw_quarter)
            qd = None if explicit is not None and isinstance(raw_quarter, str) else self._date_or_none(raw_quarter)
            fy: Optional[int] = None
            fq: Optional[int] = None
            fy_end_date: Optional[date] = None
            if fiscal_year_col is not None and fiscal_quarter_col is not None:
                fy_val = pd_local.to_numeric(ws.cell(rr, fiscal_year_col).value, errors="coerce")
                fq_val = pd_local.to_numeric(ws.cell(rr, fiscal_quarter_col).value, errors="coerce")
                if pd_local.notna(fy_val) and pd_local.notna(fq_val) and 1 <= int(fq_val) <= 4:
                    fy, fq = int(fy_val), int(fq_val)
            if fy is None or fq is None:
                if explicit is not None:
                    fy, fq = explicit
                elif qd is not None:
                    fy, fq, _label, fy_end_date = self._resolve_fiscal_period_from_date(qd, profile)
            if fy is None or fq is None:
                continue
            label = f"{int(fy)}-Q{int(fq)}"
            if fy_end_date is None and qd is not None:
                _fy, _fq, _label, fy_end_date = self._resolve_fiscal_period_from_date(qd, profile)
            out.append(
                {
                    "row": rr,
                    "quarter_date": qd,
                    "fiscal_year": int(fy),
                    "fiscal_quarter": int(fq),
                    "label": label,
                    "fy_end_date": fy_end_date,
                }
            )
        out.sort(
            key=lambda rec: (
                int(rec.get("fiscal_year") or 0),
                int(rec.get("fiscal_quarter") or 0),
                rec.get("quarter_date") or date.min,
                int(rec.get("row") or 0),
            )
        )
        return out

    def history_q_latest_full_year_period_set(
        self,
        wb: Any,
        *,
        ticker: Any = "",
        fiscal_profile: Any = None,
    ) -> Dict[str, Any]:
        periods = self.resolve_history_q_fiscal_periods_from_workbook(wb, ticker=ticker, fiscal_profile=fiscal_profile)
        by_year: Dict[int, Dict[int, Dict[str, Any]]] = {}
        for rec in periods:
            fy = int(rec.get("fiscal_year") or 0)
            fq = int(rec.get("fiscal_quarter") or 0)
            if fy <= 0 or fq not in {1, 2, 3, 4}:
                continue
            existing = by_year.setdefault(fy, {}).get(fq)
            if existing is None or (rec.get("quarter_date") or date.min) >= (existing.get("quarter_date") or date.min):
                by_year[fy][fq] = rec
        full_years = [fy for fy, quarters in by_year.items() if all(q in quarters for q in (1, 2, 3, 4))]
        if not full_years:
            return {}
        latest_year = max(full_years)
        rows = [by_year[latest_year][q]["row"] for q in (1, 2, 3, 4)]
        quarter_dates = [by_year[latest_year][q].get("quarter_date") for q in (1, 2, 3, 4)]
        labels = [by_year[latest_year][q].get("label") for q in (1, 2, 3, 4)]
        quarter_criteria = [
            by_year[latest_year][q].get("quarter_date") or by_year[latest_year][q].get("label")
            for q in (1, 2, 3, 4)
        ]
        previous_quarter_dates: List[date] = []
        previous_quarter_criteria: List[Any] = []
        if latest_year - 1 in by_year and all(q in by_year[latest_year - 1] for q in (1, 2, 3, 4)):
            previous_quarter_dates = [by_year[latest_year - 1][q].get("quarter_date") for q in (1, 2, 3, 4)]
            previous_quarter_criteria = [
                by_year[latest_year - 1][q].get("quarter_date") or by_year[latest_year - 1][q].get("label")
                for q in (1, 2, 3, 4)
            ]
        return {
            "fiscal_year": latest_year,
            "rows": rows,
            "quarter_dates": [qd for qd in quarter_dates if isinstance(qd, date)],
            "previous_quarter_dates": [qd for qd in previous_quarter_dates if isinstance(qd, date)],
            "quarter_criteria": [crit for crit in quarter_criteria if crit not in (None, "")],
            "previous_quarter_criteria": [crit for crit in previous_quarter_criteria if crit not in (None, "")],
            "labels": labels,
        }

    def history_q_latest_full_year_actuals_from_workbook(
        self,
        wb: Any,
        *,
        ticker: Any = "",
        fiscal_profile: Any = None,
    ) -> Dict[str, float]:
        """Return conservative latest full-year actuals from History_Q in workbook units.

        Values returned for money metrics are in $m, matching Investment_Case
        manual-input conventions.  The helper only uses years with all four
        quarter labels present, so it does not turn a partial YTD period into a
        full-year default.
        """

        if wb is None or "History_Q" not in getattr(wb, "sheetnames", []):
            return {}
        ws = wb["History_Q"]
        if int(ws.max_row or 0) < 2 or int(ws.max_column or 0) < 2:
            return {}

        re_local = self._re
        pd_local = self._pd
        math_local = self._math

        def _norm(value: Any) -> str:
            return re_local.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

        headers = {_norm(ws.cell(1, cc).value): cc for cc in range(1, int(ws.max_column or 0) + 1)}

        def _col(*aliases: str) -> Optional[int]:
            for alias in aliases:
                cc = headers.get(_norm(alias))
                if cc is not None:
                    return cc
            return None

        quarter_col = _col("quarter", "period", "fiscal quarter", "fiscal_period")
        if quarter_col is None:
            return {}

        period_set = self.history_q_latest_full_year_period_set(wb, ticker=ticker, fiscal_profile=fiscal_profile)
        if not period_set:
            return {}
        latest_year = int(period_set["fiscal_year"])
        rows = [int(rr) for rr in period_set.get("rows", [])]
        if len(rows) != 4:
            return {}

        def _num_at(row: int, col: Optional[int]) -> Optional[float]:
            if col is None:
                return None
            val = pd_local.to_numeric(ws.cell(row, col).value, errors="coerce")
            if pd_local.isna(val):
                return None
            out = float(val)
            return out if math_local.isfinite(out) else None

        def _sum_money(*aliases: str) -> Optional[float]:
            cc = _col(*aliases)
            vals = [_num_at(rr, cc) for rr in rows]
            clean = [float(v) for v in vals if v is not None]
            if not clean:
                return None
            total = sum(clean)
            if abs(total) > 10000.0:
                total /= 1_000_000.0
            return total if math_local.isfinite(total) else None

        def _latest_numeric(*aliases: str) -> Optional[float]:
            cc = _col(*aliases)
            for rr in reversed(rows):
                val = _num_at(rr, cc)
                if val is not None:
                    return val
            return None

        def _shares_m() -> Optional[float]:
            shares = _latest_numeric("shares_diluted", "diluted shares", "weighted average diluted shares", "shares")
            if shares is None:
                return None
            if abs(shares) > 10000.0:
                shares /= 1_000_000.0
            return shares if math_local.isfinite(shares) and shares > 0 else None

        def _sum_money_for_rows(rows_in: Sequence[int], *aliases: str) -> Optional[float]:
            cc = _col(*aliases)
            vals = [_num_at(rr, cc) for rr in rows_in]
            clean = [float(v) for v in vals if v is not None]
            if not clean:
                return None
            total = sum(clean)
            if abs(total) > 10000.0:
                total /= 1_000_000.0
            return total if math_local.isfinite(total) else None

        revenue_m = _sum_money("revenue", "net sales", "sales")
        ebitda_m = _sum_money("adj_ebitda", "adjusted ebitda", "ebitda")
        net_income_m = _sum_money("net_income", "net income", "net income attributable")
        cfo_m = _sum_money("cfo", "cash from operations", "operating cash flow", "net cash provided by operating activities")
        capex_m = _sum_money("capex", "capital expenditures", "capital expenditure", "property and equipment additions")
        fcf_m = _sum_money("fcf", "free cash flow")
        if fcf_m is None and cfo_m is not None and capex_m is not None:
            fcf_m = cfo_m - abs(capex_m)
        op_income_m = _sum_money("op_income", "operating income", "operating profit")
        pretax_m = _sum_money("pretax_income", "pre-tax income", "income before taxes", "income before income taxes")
        tax_m = _sum_money("income_tax_expense", "provision for income taxes", "tax expense", "income tax provision")
        buybacks_m = _sum_money("buybacks_cash", "share repurchases", "stock repurchases", "repurchases of common stock")
        shares_m = _shares_m()
        out: Dict[str, float] = {"year": float(latest_year)}
        for key, value in {
            "revenue_m": revenue_m,
            "ebitda_m": ebitda_m,
            "fcf_m": fcf_m,
            "capex_m": abs(capex_m) if capex_m is not None else None,
            "buybacks_m": abs(buybacks_m) if buybacks_m is not None else None,
        }.items():
            if value is not None and math_local.isfinite(float(value)):
                out[key] = float(value)
        previous_year = latest_year - 1
        previous_periods = self.resolve_history_q_fiscal_periods_from_workbook(wb, ticker=ticker, fiscal_profile=fiscal_profile)
        prev_rows_by_q = {
            int(rec.get("fiscal_quarter") or 0): int(rec.get("row") or 0)
            for rec in previous_periods
            if int(rec.get("fiscal_year") or 0) == previous_year
        }
        if all(q in prev_rows_by_q for q in (1, 2, 3, 4)):
            previous_revenue_m = _sum_money_for_rows([prev_rows_by_q[q] for q in (1, 2, 3, 4)], "revenue", "net sales", "sales")
            if revenue_m is not None and previous_revenue_m and previous_revenue_m > 0:
                growth = (float(revenue_m) / float(previous_revenue_m)) - 1.0
                if math_local.isfinite(growth):
                    out["revenue_growth"] = growth
        if net_income_m is not None and shares_m:
            eps = float(net_income_m) / float(shares_m)
            if math_local.isfinite(eps):
                out["eps"] = eps
        if op_income_m is not None and revenue_m and revenue_m > 0:
            margin = float(op_income_m) / float(revenue_m)
            if math_local.isfinite(margin) and -0.75 <= margin <= 1.0:
                out["operating_margin"] = margin
        if tax_m is not None and pretax_m and pretax_m > 0:
            tax_rate = float(tax_m) / float(pretax_m)
            if math_local.isfinite(tax_rate) and 0.0 <= tax_rate <= 0.35:
                out["tax_rate"] = tax_rate
        return out

    def augment_history_q_frame_for_writer(
        self,
        df: Any,
        *,
        ticker: Any = "",
        fiscal_profile: Any = None,
    ) -> Any:
        """Add reusable fiscal-period and operating-margin columns to History_Q."""

        pd_local = self._pd
        re_local = self._re
        if not isinstance(df, pd_local.DataFrame) or df.empty:
            return df
        out = df.copy()

        def _norm(value: Any) -> str:
            return re_local.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

        col_by_norm = {_norm(col): col for col in out.columns}

        def _col(*aliases: str) -> Optional[Any]:
            for alias in aliases:
                col = col_by_norm.get(_norm(alias))
                if col is not None:
                    return col
            return None

        quarter_col = _col("quarter", "period", "fiscal quarter", "fiscal_period")
        if quarter_col is not None and not {"fiscal_year", "fiscal_quarter", "fiscal_label"}.issubset(set(map(str, out.columns))):
            profile = self.fiscal_profile_from_workbook(None, ticker=ticker, fiscal_profile=fiscal_profile)
            fiscal_years: List[Any] = []
            fiscal_quarters: List[Any] = []
            fiscal_labels: List[Any] = []
            for raw in out[quarter_col].tolist():
                explicit = self._explicit_quarter_label_key(raw)
                qd = None if explicit is not None and isinstance(raw, str) else self._date_or_none(raw)
                if explicit is not None:
                    fy, fq = explicit
                    label = f"{fy}-Q{fq}"
                elif qd is not None:
                    fy, fq, label, _fy_end = self._resolve_fiscal_period_from_date(qd, profile)
                else:
                    fy = fq = label = pd_local.NA
                fiscal_years.append(fy)
                fiscal_quarters.append(fq)
                fiscal_labels.append(label)
            if "fiscal_year" not in out.columns:
                out["fiscal_year"] = fiscal_years
            if "fiscal_quarter" not in out.columns:
                out["fiscal_quarter"] = fiscal_quarters
            if "fiscal_label" not in out.columns:
                out["fiscal_label"] = fiscal_labels

        if "operating_margin" not in out.columns:
            revenue_col = _col("revenue", "net sales", "sales")
            numerator_col = _col("op_income", "operating income", "operating profit")
            basis = "operating income / revenue"
            if numerator_col is None:
                numerator_col = _col("ebit", "income from operations", "operating earnings")
                basis = "EBIT margin proxy"
            if numerator_col is None:
                numerator_col = _col("adj_ebit", "adjusted ebit", "adjusted operating income")
                basis = "adjusted EBIT margin proxy"
            if revenue_col is not None and numerator_col is not None:
                revenue = pd_local.to_numeric(out[revenue_col], errors="coerce")
                numerator = pd_local.to_numeric(out[numerator_col], errors="coerce")
                margin = numerator / revenue.replace({0: pd_local.NA})
                margin = margin.where(margin.between(-0.75, 1.0))
                out["operating_margin"] = margin
                if "operating_margin_basis" not in out.columns:
                    out["operating_margin_basis"] = basis

        return out

    def history_q_year_default_formulas(
        self,
        start_date: Optional[Tuple[int, int, int]] = None,
        end_date: Optional[Tuple[int, int, int]] = None,
        *,
        fiscal_year: Optional[int] = None,
        quarter_dates: Optional[Sequence[date]] = None,
        previous_quarter_dates: Optional[Sequence[date]] = None,
        quarter_criteria: Optional[Sequence[Any]] = None,
        previous_quarter_criteria: Optional[Sequence[Any]] = None,
        start_exclusive: bool = False,
        end_inclusive: bool = False,
    ) -> Dict[str, str]:
        """Excel formulas for latest full-year defaults when History_Q is written later.

        Prefer exact fiscal-quarter dates from the resolver.  Date ranges are kept
        only as a fallback for legacy calendar-year callers.
        """

        exact_criteria = [crit for crit in (quarter_criteria or []) if crit not in (None, "")]
        exact_prev_criteria = [crit for crit in (previous_quarter_criteria or []) if crit not in (None, "")]
        if not exact_criteria:
            exact_criteria = [qd for qd in (quarter_dates or []) if isinstance(qd, date)]
        if not exact_prev_criteria:
            exact_prev_criteria = [qd for qd in (previous_quarter_dates or []) if isinstance(qd, date)]
        fiscal_year_int: Optional[int]
        try:
            fiscal_year_int = int(fiscal_year) if fiscal_year is not None else None
        except Exception:
            fiscal_year_int = None
        use_fiscal_columns = not exact_criteria and fiscal_year_int is not None
        if not exact_criteria:
            start_date = start_date or (2025, 1, 1)
            end_date = end_date or (2026, 1, 1)
            start = f"DATE({start_date[0]},{start_date[1]},{start_date[2]})"
            end = f"DATE({end_date[0]},{end_date[1]},{end_date[2]})"
            prev_start = f"DATE({start_date[0] - 1},{start_date[1]},{start_date[2]})"
            prev_end = f"DATE({end_date[0] - 1},{end_date[1]},{end_date[2]})"
            start_op = ">" if start_exclusive else ">="
            end_op = "<=" if end_inclusive else "<"
        dates = "History_Q!$A:$A"

        def _range(metric: str) -> str:
            return f'INDEX(History_Q!$A:$ZZ,0,MATCH("{metric}",History_Q!$1:$1,0))'

        fiscal_year_range = 'INDEX(History_Q!$A:$ZZ,0,MATCH("fiscal_year",History_Q!$1:$1,0))'
        fiscal_quarter_range = 'INDEX(History_Q!$A:$ZZ,0,MATCH("fiscal_quarter",History_Q!$1:$1,0))'

        def _date_expr(qd: date) -> str:
            return f"DATE({int(qd.year)},{int(qd.month)},{int(qd.day)})"

        def _criteria_expr(crit: Any) -> str:
            if isinstance(crit, date):
                return _date_expr(crit)
            txt = str(crit or "").replace('"', '""')
            return f'"{txt}"'

        def _sum_exact(metric: str, criteria: Sequence[Any]) -> str:
            terms = [f"SUMIFS({_range(metric)},{dates},{_criteria_expr(crit)})" for crit in criteria]
            if not terms:
                return "0"
            return "(" + "+".join(terms) + ")"

        def _sum(metric: str) -> str:
            if exact_criteria:
                return _sum_exact(metric, exact_criteria)
            if use_fiscal_columns and fiscal_year_int is not None:
                return (
                    f'SUMIFS({_range(metric)},{fiscal_year_range},{fiscal_year_int},'
                    f'{fiscal_quarter_range},">=1",{fiscal_quarter_range},"<=4")'
                )
            return f'SUMIFS({_range(metric)},{dates},"{start_op}"&{start},{dates},"{end_op}"&{end})'

        def _sum_prev(metric: str) -> str:
            if exact_criteria:
                return _sum_exact(metric, exact_prev_criteria)
            if use_fiscal_columns and fiscal_year_int is not None:
                return (
                    f'SUMIFS({_range(metric)},{fiscal_year_range},{fiscal_year_int - 1},'
                    f'{fiscal_quarter_range},">=1",{fiscal_quarter_range},"<=4")'
                )
            return f'SUMIFS({_range(metric)},{dates},"{start_op}"&{prev_start},{dates},"{end_op}"&{prev_end})'

        revenue = f'=IFERROR({_sum("revenue")}/1000000,"")'
        ebitda = f'=IFERROR({_sum("ebitda")}/1000000,"")'
        capex = f'=IFERROR(ABS({_sum("capex")})/1000000,"")'
        fcf = f'=IFERROR(({_sum("cfo")}-ABS({_sum("capex")}))/1000000,"")'
        buybacks = f'=IFERROR(ABS({_sum("buybacks_cash")})/1000000,"")'
        revenue_growth = f'=IFERROR(IF({_sum_prev("revenue")}>0,{_sum("revenue")}/{_sum_prev("revenue")}-1,""),"")'
        share_denominator = (
            f'({_sum("shares_diluted")}/{max(len(exact_criteria), 1)})'
            if exact_criteria
            else (
                f'AVERAGEIFS({_range("shares_diluted")},{fiscal_year_range},{fiscal_year_int},'
                f'{fiscal_quarter_range},">=1",{fiscal_quarter_range},"<=4")'
                if use_fiscal_columns and fiscal_year_int is not None
                else f'AVERAGEIFS({_range("shares_diluted")},{dates},"{start_op}"&{start},{dates},"{end_op}"&{end})'
            )
        )
        eps = (
            f'=IFERROR(({_sum("net_income")}/1000000)'
            f'/({share_denominator}/1000000),"")'
        )
        op_margin = f'=IFERROR({_sum("op_income")}/{_sum("revenue")},"")'
        tax_rate = f'=IFERROR(IF(AND({_sum("pretax_income")}>0,{_sum("income_tax_expense")}>=0,{_sum("income_tax_expense")}/{_sum("pretax_income")}<=0.35),{_sum("income_tax_expense")}/{_sum("pretax_income")},""),"")'
        return {
            "revenue_m": revenue,
            "ebitda_m": ebitda,
            "fcf_m": fcf,
            "capex_m": capex,
            "buybacks_m": buybacks,
            "revenue_growth": revenue_growth,
            "eps": eps,
            "operating_margin": op_margin,
            "tax_rate": tax_rate,
        }
