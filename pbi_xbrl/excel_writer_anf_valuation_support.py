"""ANF valuation fiscal-period, YoY, and buyback support helpers."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Set, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AnfValuationSupportDeps:
    runtime: MutableMapping[str, Any]


class AnfValuationSupport:
    def __init__(self, deps: AnfValuationSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    def _rt(self, name: str) -> Any:
        return self.runtime[name]

    def _pd(self) -> Any:
        return self.runtime.get("pd", pd)

    def _np(self) -> Any:
        return self.runtime.get("np", np)

    def _dt(self) -> Any:
        return self.runtime.get("dt", dt)

    def _fiscal_year_from_quarter_end(self, qd: Any) -> Optional[int]:
        return self._rt("_anf_fiscal_year_from_quarter_end")(qd)

    def _fiscal_quarter_from_quarter_end(self, qd: Any) -> Optional[int]:
        return self._rt("_anf_fiscal_quarter_from_quarter_end")(qd)

    def _visible_quarter_label(self, qd: Any) -> str:
        return self._rt("_anf_visible_quarter_label")(qd)

    def buyback_execution_is_year_or_ttm(
        self,
        qd: Any,
        note_text: Any = "",
        *,
        cash_amount: Optional[float] = None,
        shares_amount: Optional[float] = None,
    ) -> bool:
        """Return True when an ANF buyback disclosure is annual/TTM, not quarter-only."""
        pd_mod = self._pd()
        q_ts = pd_mod.to_datetime(qd, errors="coerce")
        note = str(note_text or "")
        note_low = note.lower()
        if any(
            token in note_low
            for token in (
                "fiscal year",
                "year ended",
                "for the year",
                "full year",
                "year-to-date",
                "year to date",
                " ytd",
                "during fiscal",
            )
        ):
            return True
        try:
            q_month = int(pd_mod.Timestamp(q_ts).month) if not pd_mod.isna(q_ts) else 0
        except Exception:
            q_month = 0
        try:
            cash_f = float(cash_amount) if cash_amount is not None and pd_mod.notna(cash_amount) else None
        except Exception:
            cash_f = None
        try:
            shares_f = float(shares_amount) if shares_amount is not None and pd_mod.notna(shares_amount) else None
        except Exception:
            shares_f = None
        # ANF's latest 10-K disclosure is annual repurchases. If the parser sees a
        # January/February Q4 period with a very large cash/share amount, treating it
        # as a quarter-only buyback overstates the precision of the workbook.
        return bool(
            q_month in (1, 2)
            and cash_f is not None
            and shares_f is not None
            and cash_f >= 300_000_000.0
            and shares_f >= 3_000_000.0
        )

    def format_year_ttm_buyback_summary(
        self,
        qd: Any,
        *,
        shares_amount: Optional[float] = None,
        cash_amount: Optional[float] = None,
        avg_price: Optional[float] = None,
    ) -> str:
        pd_mod = self._pd()
        dt_mod = self._dt()
        fy = self._fiscal_year_from_quarter_end(qd)
        try:
            q_year = int(pd_mod.Timestamp(pd_mod.to_datetime(qd, errors="coerce")).year)
        except Exception:
            q_year = dt_mod.date.today().year
        year_txt = str(fy or q_year)
        parts: List[str] = [f"{year_txt} year / TTM buybacks:"]
        try:
            if shares_amount is not None and pd_mod.notna(shares_amount):
                parts.append(f"{float(shares_amount) / 1_000_000.0:,.1f}m shares")
        except Exception:
            pass
        try:
            if cash_amount is not None and pd_mod.notna(cash_amount):
                if len(parts) > 1:
                    parts.append("for")
                parts.append(f"~${float(cash_amount) / 1_000_000.0:,.0f}m")
        except Exception:
            pass
        try:
            if avg_price is not None and pd_mod.notna(avg_price):
                parts.append(f"at ~${float(avg_price):.2f}/share")
        except Exception:
            pass
        return " ".join(parts).strip()

    def normalized_quarter_ts(self, qd: Any) -> Optional[pd.Timestamp]:
        pd_mod = self._pd()
        q_ts = pd_mod.to_datetime(qd, errors="coerce")
        if pd_mod.isna(q_ts):
            return None
        return pd_mod.Timestamp(q_ts).normalize()

    def quarter_sequence(self, quarters: Iterable[Any]) -> List[pd.Timestamp]:
        seen: Set[pd.Timestamp] = set()
        out: List[pd.Timestamp] = []
        if quarters is None:
            quarter_iter: Iterable[Any] = ()
        else:
            quarter_iter = list(quarters)
        for q in quarter_iter:
            q_ts = self.normalized_quarter_ts(q)
            if q_ts is None or q_ts in seen:
                continue
            seen.add(q_ts)
            out.append(q_ts)
        return sorted(out)

    def prior_year_quarter(self, qd: Any, quarters: Iterable[Any]) -> Optional[pd.Timestamp]:
        q_ts = self.normalized_quarter_ts(qd)
        if q_ts is None:
            return None
        fiscal_year = self._fiscal_year_from_quarter_end(q_ts)
        fiscal_quarter = self._fiscal_quarter_from_quarter_end(q_ts)
        if fiscal_year is None or fiscal_quarter is None:
            return None
        for cand in self.quarter_sequence(quarters):
            if cand == q_ts:
                continue
            if (
                self._fiscal_year_from_quarter_end(cand) == fiscal_year - 1
                and self._fiscal_quarter_from_quarter_end(cand) == fiscal_quarter
            ):
                return cand
        return None

    def previous_quarter(self, qd: Any, quarters: Iterable[Any]) -> Optional[pd.Timestamp]:
        q_ts = self.normalized_quarter_ts(qd)
        if q_ts is None:
            return None
        seq = self.quarter_sequence(quarters)
        try:
            idx = seq.index(q_ts)
        except ValueError:
            seq = sorted(set(seq + [q_ts]))
            idx = seq.index(q_ts)
        if idx <= 0:
            return None
        return seq[idx - 1]

    def normalize_value_map(self, src: Dict[Any, Any]) -> Dict[pd.Timestamp, Any]:
        out: Dict[pd.Timestamp, Any] = {}
        for raw_q, raw_v in dict(src or {}).items():
            q_ts = self.normalized_quarter_ts(raw_q)
            if q_ts is None:
                continue
            out[q_ts] = raw_v
        return out

    def is_missing_value(self, v: Any) -> bool:
        pd_mod = self._pd()
        np_mod = self._np()
        if v is None:
            return True
        try:
            missing = pd_mod.isna(v)
            if isinstance(missing, (bool, np_mod.bool_)):
                return bool(missing)
        except Exception:
            pass
        return False

    def yoy_map_for_fiscal_periods(
        self,
        src: Dict[Any, Any],
        quarters: Iterable[Any],
        *,
        positive_prev_only: bool = False,
        positive_cur_only: bool = False,
    ) -> Dict[pd.Timestamp, Any]:
        values = self.normalize_value_map(src)
        quarter_items = [] if quarters is None else list(quarters)
        seq = self.quarter_sequence(quarter_items + list(values.keys()))
        value_by_label = {
            self._visible_quarter_label(q): v
            for q, v in values.items()
            if self._visible_quarter_label(q) and not self.is_missing_value(v)
        }
        out: Dict[pd.Timestamp, Any] = {}
        for q in seq:
            prev = self.prior_year_quarter(q, seq)
            v = values.get(q)
            p = values.get(prev) if prev is not None else None
            if self.is_missing_value(p):
                fy = self._fiscal_year_from_quarter_end(q)
                fq = self._fiscal_quarter_from_quarter_end(q)
                if fy is not None and fq is not None:
                    p = value_by_label.get(f"{fy - 1}-Q{fq}")
            if self.is_missing_value(v) or self.is_missing_value(p):
                out[q] = None
                continue
            try:
                fv = float(v)
                fp = float(p)
            except Exception:
                out[q] = None
                continue
            if fp == 0:
                out[q] = None
                continue
            if positive_prev_only and fp <= 0:
                out[q] = None
            elif positive_cur_only and fv <= 0:
                out[q] = None
            else:
                out[q] = (fv - fp) / abs(fp)
        return out

    def value_delta_map_for_fiscal_periods(
        self,
        src: Dict[Any, Any],
        quarters: Iterable[Any],
        *,
        comparison: str = "yoy",
    ) -> Dict[pd.Timestamp, Any]:
        values = self.normalize_value_map(src)
        quarter_items = [] if quarters is None else list(quarters)
        seq = self.quarter_sequence(quarter_items + list(values.keys()))
        value_by_label = {
            self._visible_quarter_label(q): v
            for q, v in values.items()
            if self._visible_quarter_label(q) and not self.is_missing_value(v)
        }
        out: Dict[pd.Timestamp, Any] = {}
        cmp_key = str(comparison or "yoy").strip().lower()
        for q in seq:
            prev = self.previous_quarter(q, seq) if cmp_key == "qoq" else self.prior_year_quarter(q, seq)
            v = values.get(q)
            p = values.get(prev) if prev is not None else None
            if self.is_missing_value(p):
                fy = self._fiscal_year_from_quarter_end(q)
                fq = self._fiscal_quarter_from_quarter_end(q)
                if fy is not None and fq is not None:
                    if cmp_key == "qoq":
                        prev_fy = fy if fq > 1 else fy - 1
                        prev_fq = fq - 1 if fq > 1 else 4
                        p = value_by_label.get(f"{prev_fy}-Q{prev_fq}")
                    else:
                        p = value_by_label.get(f"{fy - 1}-Q{fq}")
            if self.is_missing_value(v) or self.is_missing_value(p):
                out[q] = None
                continue
            try:
                out[q] = float(v) - float(p)
            except Exception:
                out[q] = None
        return out

    def normalize_ytd_buyback_cash_map_for_valuation(
        self,
        src: Dict[Any, Any],
        quarters: Iterable[Any],
    ) -> Dict[pd.Timestamp, Any]:
        """Convert ANF cumulative YTD repurchase cash disclosures into quarter deltas.

        ANF earnings schedules often restate year-to-date repurchases in each quarterly
        update. Valuation TTM rows need period cash flows, otherwise a 200/250/350/450
        YTD series turns into a bogus 1,250 TTM.
        """
        pd_mod = self._pd()
        values = self.normalize_value_map(src)
        if not values:
            return values
        quarter_items = [] if quarters is None else list(quarters)
        seq = self.quarter_sequence(quarter_items + list(values.keys()))
        by_fy: Dict[int, List[pd.Timestamp]] = {}
        for q in seq:
            if q not in values:
                continue
            fy = self._fiscal_year_from_quarter_end(q)
            if fy is None:
                continue
            by_fy.setdefault(int(fy), []).append(q)
        out = dict(values)
        for _, fy_quarters in by_fy.items():
            numeric: List[Tuple[pd.Timestamp, float]] = []
            for q in sorted(fy_quarters):
                v = values.get(q)
                try:
                    if v is None or pd_mod.isna(v):
                        continue
                    numeric.append((q, float(v)))
                except Exception:
                    continue
            if len(numeric) < 2:
                continue
            is_monotonic = all(numeric[idx][1] >= numeric[idx - 1][1] - 1e-6 for idx in range(1, len(numeric)))
            has_material_rollup = numeric[-1][1] > max(numeric[0][1], 1.0) and numeric[-1][1] >= sum(v for _, v in numeric[:-1]) * 0.45
            if not (is_monotonic and has_material_rollup):
                continue
            prior_cum = 0.0
            for q, cumulative_v in numeric:
                delta_v = cumulative_v - prior_cum
                out[q] = max(delta_v, 0.0) if delta_v >= -1e-6 else cumulative_v
                prior_cum = cumulative_v
        return out
