"""Valuation operating thesis bridge support helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping, Optional, Tuple


@dataclass(frozen=True)
class ValuationBridgeSupportDeps:
    runtime: MutableMapping[str, Any]


class ValuationBridgeSupport:
    def __init__(self, deps: ValuationBridgeSupportDeps) -> None:
        self.runtime = deps.runtime
        self._bridge_fy_adj_ebitda_cache: Optional[List[Dict[str, Any]]] = None

    def load_bridge_fy_adjusted_ebitda_records(self) -> List[Dict[str, Any]]:
        runtime = self.runtime
        re = runtime["re"]
        Path = runtime["Path"]
        _operating_driver_financial_statement_files = runtime["_operating_driver_financial_statement_files"]
        _operating_driver_follow_source_dirs = runtime["_operating_driver_follow_source_dirs"]
        _read_operating_driver_text = runtime["_read_operating_driver_text"]
        glx_normalize_text = runtime["glx_normalize_text"]
        qn_compact_snippet = runtime["qn_compact_snippet"]
        _source_rank = runtime["_source_rank"]

        if self._bridge_fy_adj_ebitda_cache is not None:
            return list(self._bridge_fy_adj_ebitda_cache)
        records: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[int, float, str]] = set()
        table_re = re.compile(
            r"Year Ended December 31,\s*(20\d{2})\s+(20\d{2})(?:\s+(20\d{2}))?.{0,1600}?"
            r"Adjusted EBITDA\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?)\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?)"
            r"(?:\s+\$?\s*(\(?[0-9,]+(?:\.\d+)?\)?))?",
            re.I | re.S,
        )
        narrative_re = re.compile(
            r"\b(?:full year|fiscal year|year ended december 31,?)\s*(20\d{2})\b[^.]{0,180}?"
            r"adjusted ebitda(?:\s+of)?\s+\$?\s*([0-9]{1,3}(?:\.\d+)?)\s*(million|m)\b",
            re.I,
        )

        def _parse_signed_amount(token: str) -> Optional[float]:
            tok = str(token or "").strip()
            if not tok:
                return None
            sign = -1.0 if tok.startswith("(") and tok.endswith(")") else 1.0
            try:
                return sign * float(tok.strip("()").replace(",", ""))
            except Exception:
                return None

        source_files: List[Tuple[str, Path]] = []
        for path_in in _operating_driver_financial_statement_files():
            source_files.append(("financial_statement", path_in))
        for source_type, src_dir in _operating_driver_follow_source_dirs():
            if source_type != "earnings_release":
                continue
            try:
                src_files.extend((source_type, p) for p in sorted([x for x in src_dir.iterdir() if x.is_file()]))
            except Exception:
                continue
        for source_type, path_in in source_files:
            raw_txt = _read_operating_driver_text(path_in)
            txt = glx_normalize_text(raw_txt)
            if not txt or "adjusted ebitda" not in txt.lower():
                continue
            for match in table_re.finditer(txt):
                years = [x for x in match.groups()[:3] if x]
                values = [x for x in match.groups()[3:] if x]
                if not years or not values:
                    continue
                snippet = qn_compact_snippet(txt[max(0, match.start() - 60) : min(len(txt), match.end() + 120)], 260)
                for yy, vv in zip(years, values):
                    amt = _parse_signed_amount(vv)
                    if amt is None:
                        continue
                    value_m = float(amt) / 1000.0
                    key = (int(yy), round(value_m, 3), str(path_in))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    records.append(
                        {
                            "fiscal_year": int(yy),
                            "value_m": value_m,
                            "source_type": source_type,
                            "source_doc": str(path_in),
                            "quality": "exact",
                            "snippet": snippet,
                        }
                    )
            for match in narrative_re.finditer(txt):
                year_txt, amt_txt, _ = match.groups()
                try:
                    value_m = float(str(amt_txt).replace(",", ""))
                except Exception:
                    continue
                key = (int(year_txt), round(value_m, 3), str(path_in))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                records.append(
                    {
                        "fiscal_year": int(year_txt),
                        "value_m": float(value_m),
                        "source_type": source_type,
                        "source_doc": str(path_in),
                        "quality": "text-derived",
                        "snippet": qn_compact_snippet(
                            txt[max(0, match.start() - 60) : min(len(txt), match.end() + 120)],
                            260,
                        ),
                    }
                )
        records.sort(
            key=lambda rec: (
                int(rec.get("fiscal_year") or 0),
                int(_source_rank(rec.get("source_type"), rec.get("source_doc"))),
                0 if str(rec.get("quality") or "") == "exact" else 1,
                -abs(float(rec.get("value_m") or 0.0)),
            )
        )
        self._bridge_fy_adj_ebitda_cache = records
        return list(records)

    def resolve_thesis_fy_base(self) -> Dict[str, Any]:
        runtime = self.runtime
        pd = runtime["pd"]
        hist = runtime["hist"]
        adj_metrics = runtime["adj_metrics"]
        _adj_metrics_view = runtime["_adj_metrics_view"]
        _source_rank = runtime["_source_rank"]

        latest_q = pd.NaT
        if hist is not None and not hist.empty and "quarter" in hist.columns:
            latest_q = pd.to_datetime(hist["quarter"], errors="coerce").dropna().max()
        if pd.isna(latest_q) and adj_metrics is not None and not adj_metrics.empty and "quarter" in adj_metrics.columns:
            latest_q = pd.to_datetime(adj_metrics["quarter"], errors="coerce").dropna().max()
        latest_fy_year: Optional[int] = None
        if pd.notna(latest_q):
            latest_q_ts = pd.Timestamp(latest_q)
            latest_fy_year = latest_q_ts.year if latest_q_ts.month == 12 else latest_q_ts.year - 1
        label_core = f"Base Adj EBITDA FY{latest_fy_year}" if latest_fy_year else "Base Adj EBITDA FY"
        if (
            adj_metrics is not None
            and not adj_metrics.empty
            and "quarter" in adj_metrics.columns
            and "adj_ebitda" in adj_metrics.columns
        ):
            adj_local = _adj_metrics_view().copy()
            if "_quarter" in adj_local.columns:
                adj_local["quarter"] = adj_local["_quarter"]
            adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")
            adj_local["adj_ebitda"] = pd.to_numeric(adj_local["adj_ebitda"], errors="coerce")
            adj_clean = adj_local.dropna(subset=["adj_ebitda"]).copy()
            recent = adj_clean.groupby(adj_clean["quarter"].dt.to_period("Q"), as_index=False).last().tail(4)
            if len(recent) == 4:
                raw_sum = float(recent["adj_ebitda"].sum())
                value_m = raw_sum / 1_000_000.0 if abs(raw_sum) > 10_000.0 else raw_sum
                latest_ttm_q = pd.to_datetime(recent["quarter"], errors="coerce").dropna().max()
                latest_ttm_label = (
                    f" through {pd.Timestamp(latest_ttm_q).date()}" if pd.notna(latest_ttm_q) else ""
                )
                return {
                    "label": "Base Adj EBITDA TTM (latest)",
                    "value_m": float(value_m),
                    "fallback": "latest TTM",
                    "source_type": "adj_metrics",
                    "source_doc": "",
                    "quality": "modeled",
                    "snippet": f"Latest four quarterly adjusted EBITDA observations{latest_ttm_label}.",
                }
        annual_records = self.load_bridge_fy_adjusted_ebitda_records()
        fy_records = [rec for rec in annual_records if latest_fy_year is not None and int(rec.get("fiscal_year") or 0) == latest_fy_year]
        if fy_records:
            best = sorted(
                fy_records,
                key=lambda rec: (
                    int(_source_rank(rec.get("source_type"), rec.get("source_doc"))),
                    0 if str(rec.get("quality") or "") == "exact" else 1,
                ),
            )[0]
            return {
                "label": label_core,
                "value_m": float(best.get("value_m") or 0.0),
                "fallback": "",
                "source_type": str(best.get("source_type") or ""),
                "source_doc": str(best.get("source_doc") or ""),
                "quality": str(best.get("quality") or "exact"),
                "snippet": str(best.get("snippet") or ""),
            }

        if adj_metrics is not None and not adj_metrics.empty and latest_fy_year is not None and "adj_ebitda" in adj_metrics.columns:
            adj_local = _adj_metrics_view().copy()
            if "_quarter" in adj_local.columns:
                adj_local["quarter"] = adj_local["_quarter"]
            adj_local = adj_local[adj_local["quarter"].notna()].sort_values("quarter")
            adj_local["adj_ebitda"] = pd.to_numeric(adj_local["adj_ebitda"], errors="coerce")
            same_fy = adj_local[adj_local["quarter"].dt.year == latest_fy_year].dropna(subset=["adj_ebitda"])
            if same_fy["quarter"].dt.to_period("Q").nunique() >= 4:
                last_four = same_fy.groupby(same_fy["quarter"].dt.to_period("Q"), as_index=False)["adj_ebitda"].last()
                if len(last_four) >= 4:
                    return {
                        "label": f"{label_core} (fallback: summed quarters)",
                        "value_m": float(last_four["adj_ebitda"].tail(4).sum()),
                        "fallback": "summed quarters",
                        "source_type": "modeled",
                        "source_doc": "",
                        "quality": "modeled",
                        "snippet": "Summed four quarterly adjusted EBITDA observations for the latest completed fiscal year.",
                    }
            recent = adj_local.dropna(subset=["adj_ebitda"]).tail(4)
            if len(recent) == 4:
                return {
                    "label": f"{label_core} (fallback: TTM)",
                    "value_m": float(recent["adj_ebitda"].sum()),
                    "fallback": "TTM",
                    "source_type": "modeled",
                    "source_doc": "",
                    "quality": "modeled",
                    "snippet": "Summed latest four quarterly adjusted EBITDA observations as a TTM fallback.",
                }

        return {
            "label": f"{label_core} (fallback: unavailable)",
            "value_m": 0.0,
            "fallback": "unavailable",
            "source_type": "",
            "source_doc": "",
            "quality": "inferred",
            "snippet": "",
        }
