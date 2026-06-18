"""Local balance-sheet source payload support for workbook writer."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional

import pandas as pd


@dataclass(frozen=True)
class LocalBalanceSheetSupportDeps:
    runtime: MutableMapping[str, Any]


class LocalBalanceSheetSupport:
    def __init__(self, deps: LocalBalanceSheetSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    def _rt(self, name: str) -> Any:
        return self.runtime[name]

    def _pd(self) -> Any:
        return self.runtime.get("pd", pd)

    def _state(self) -> MutableMapping[str, Any]:
        return self.runtime["local_balance_sheet_support_state"]

    def shared_financial_statement_files(self) -> List[Path]:
        material_roots = self._rt("material_roots")
        ticker = self._rt("ticker")
        ticker_roots = self._rt("ticker_roots")
        _path_belongs_to_ticker = self._rt("_path_belongs_to_ticker")

        files: List[Path] = []
        seen: set[str] = set()
        for root in material_roots:
            fs_dir = root / "financial_statement"
            if not fs_dir.exists() or not fs_dir.is_dir():
                continue
            try:
                cand_files = sorted([p for p in fs_dir.iterdir() if p.is_file()])
            except Exception:
                continue
            for path_in in cand_files:
                if path_in.suffix.lower() not in {".txt", ".htm", ".html"}:
                    continue
                if not _path_belongs_to_ticker(path_in, ticker, ticker_roots):
                    continue
                try:
                    key = str(path_in.resolve())
                except Exception:
                    key = str(path_in)
                if key in seen:
                    continue
                seen.add(key)
                files.append(path_in)
        return files

    def _shared_local_balance_sheet_file_index(self) -> List[Dict[str, Any]]:
        state = self._state()
        cached = state.get("file_index_cache")
        if cached is not None:
            return cached
        _path_cache_key = self._rt("_path_cache_key")
        _parse_quarter_from_filename = self._rt("_parse_quarter_from_filename")
        indexed: List[Dict[str, Any]] = []
        for path_in in self.shared_financial_statement_files():
            indexed.append(
                {
                    "path": path_in,
                    "path_key": _path_cache_key(path_in),
                    "suffix": path_in.suffix.lower(),
                    "quarter": _parse_quarter_from_filename(path_in.name),
                }
            )
        state["file_index_cache"] = indexed
        return state["file_index_cache"]

    def shared_local_balance_sheet_quarter(self, record: Dict[str, Any]) -> Optional[date]:
        path_in = record.get("path")
        if not isinstance(path_in, Path):
            return None
        state = self._state()
        quarter_cache = state["quarter_cache"]
        _path_cache_key = self._rt("_path_cache_key")
        path_key = str(record.get("path_key") or _path_cache_key(path_in))
        if path_key in quarter_cache:
            return quarter_cache.get(path_key)
        qd = record.get("quarter")
        if not isinstance(qd, date):
            raw_txt = self._rt("_read_material_text")(path_in)
            qd = (
                self._rt("_parse_quarter_from_follow_text")(raw_txt)
                or self._rt("infer_quarter_end_from_text")(raw_txt)
            )
        qd_out = qd if isinstance(qd, date) else None
        quarter_cache[path_key] = qd_out
        return qd_out

    def shared_local_balance_sheet_records_by_quarter(self) -> Dict[date, List[Dict[str, Any]]]:
        state = self._state()
        cached = state.get("records_by_quarter_cache")
        if cached is not None:
            return cached
        grouped: Dict[date, List[Dict[str, Any]]] = {}
        for rec in self._shared_local_balance_sheet_file_index():
            qd = rec.get("quarter")
            if not isinstance(qd, date):
                qd = self.shared_local_balance_sheet_quarter(rec)
            if not isinstance(qd, date):
                continue
            grouped.setdefault(qd, []).append(rec)
        state["records_by_quarter_cache"] = grouped
        return state["records_by_quarter_cache"]

    def shared_local_balance_sheet_payload_for_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        path_in = record.get("path")
        if not isinstance(path_in, Path):
            return None
        state = self._state()
        payload_by_path_cache = state["payload_by_path_cache"]
        _path_cache_key = self._rt("_path_cache_key")
        path_key = str(record.get("path_key") or _path_cache_key(path_in))
        if path_key in payload_by_path_cache:
            return payload_by_path_cache.get(path_key)
        qd = self.shared_local_balance_sheet_quarter(record)
        if not isinstance(qd, date):
            payload_by_path_cache[path_key] = None
            return None
        result = None
        try:
            if str(record.get("suffix") or "").lower() in {".htm", ".html"}:
                result = self._rt("_extract_balance_sheet_from_html")(path_in.read_bytes(), qd)
            else:
                result = self._rt("_extract_balance_sheet_from_text")(self._rt("_read_material_text")(path_in), qd)
        except Exception:
            result = None
        if not result:
            payload_by_path_cache[path_key] = None
            return None
        payload = dict(result)
        payload["source_doc"] = str(path_in)
        payload["_quarter"] = qd
        payload_by_path_cache[path_key] = payload
        return payload

    def shared_load_local_balance_sheet_detail_payloads(
        self,
        target_quarters: Optional[set[date]] = None,
    ) -> Dict[date, Dict[str, Any]]:
        state = self._state()
        payload_cache = state["payload_cache"]
        target_qs = {qd for qd in (target_quarters or set()) if isinstance(qd, date)}
        if target_qs and all(qd in payload_cache for qd in target_qs):
            return {
                qd: payload
                for qd, payload in payload_cache.items()
                if qd in target_qs
            }
        with self._rt("_timed_writer_substage")("write_excel.valuation.bundle.local_bs.index"):
            records_by_quarter = self.shared_local_balance_sheet_records_by_quarter()

        candidate_records: List[Dict[str, Any]] = []
        if target_qs:
            for qd in sorted(target_qs):
                candidate_records.extend(records_by_quarter.get(qd, []))
        else:
            for recs in records_by_quarter.values():
                candidate_records.extend(recs)

        with self._rt("_timed_writer_substage")("write_excel.valuation.bundle.local_bs.parse_selected"):
            parsed_payloads: List[Dict[str, Any]] = []
            for rec in candidate_records:
                payload = self.shared_local_balance_sheet_payload_for_record(rec)
                if not payload:
                    continue
                parsed_payloads.append(payload)

        with self._rt("_timed_writer_substage")("write_excel.valuation.bundle.local_bs.pick_best"):
            for payload in parsed_payloads:
                qd = payload.get("_quarter")
                if not isinstance(qd, date):
                    continue
                current = payload_cache.get(qd)
                if current is None or len(payload.get("values", {})) >= len(current.get("values", {})):
                    payload_cache[qd] = payload
        if not target_qs:
            return dict(payload_cache)
        return {
            qd: payload
            for qd, payload in payload_cache.items()
            if qd in target_qs
        }

    def carry_forward_low_change_series(
        self,
        values_by_quarter: Dict[pd.Timestamp, Optional[float]],
        quarter_key: List[Any],
        *,
        max_gap_quarters: int = 4,
        rel_tol: float = 1e-4,
        abs_tol: float = 1_000.0,
    ) -> Dict[pd.Timestamp, Optional[float]]:
        pd_mod = self._pd()
        ordered = [pd_mod.Timestamp(qv) for qv in quarter_key]
        out_map: Dict[pd.Timestamp, Optional[float]] = {
            pd_mod.Timestamp(qv): (None if values_by_quarter.get(pd_mod.Timestamp(qv)) is None else float(values_by_quarter.get(pd_mod.Timestamp(qv))))
            for qv in ordered
        }
        explicit_idx = [idx for idx, qv in enumerate(ordered) if out_map.get(pd_mod.Timestamp(qv)) is not None]
        if len(explicit_idx) < 2:
            return out_map

        def _sameish(a: Optional[float], b: Optional[float]) -> bool:
            if a is None or b is None:
                return False
            lim = max(abs_tol, rel_tol * max(abs(float(a)), abs(float(b)), 1.0))
            return abs(float(a) - float(b)) <= lim

        for idx, qv in enumerate(ordered):
            qk = pd_mod.Timestamp(qv)
            if out_map.get(qk) is not None:
                continue
            prev_candidates = [ii for ii in explicit_idx if ii < idx]
            next_candidates = [ii for ii in explicit_idx if ii > idx]
            prev_idx = prev_candidates[-1] if prev_candidates else None
            next_idx = next_candidates[0] if next_candidates else None
            prev_val = out_map.get(pd_mod.Timestamp(ordered[prev_idx])) if prev_idx is not None else None
            next_val = out_map.get(pd_mod.Timestamp(ordered[next_idx])) if next_idx is not None else None
            if (
                prev_idx is not None
                and next_idx is not None
                and (idx - prev_idx) <= max_gap_quarters
                and (next_idx - idx) <= max_gap_quarters
                and _sameish(prev_val, next_val)
            ):
                out_map[qk] = None if prev_val is None else float(prev_val)
                continue
            if prev_idx is not None and (idx - prev_idx) <= max_gap_quarters and prev_val is not None:
                out_map[qk] = float(prev_val)
        return out_map
