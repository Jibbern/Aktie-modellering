"""Local Barchart futures providers used for live GPRE forward surfaces."""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .base import BaseMarketProvider
from ..aggregations import quarter_end_from_date


_COMPACT_DATE_RE = re.compile(r"(20\d{2})(\d{2})(\d{2})")
_ISO_DATE_RE = re.compile(r"(20\d{2})[-_](\d{2})[-_](\d{2})")
_VENDOR_DATE_RE = re.compile(r"(?<!\d)(\d{2})[-_](\d{2})[-_](20\d{2})(?!\d)")
_TENOR_RE = re.compile(
    r"\b(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\s_/'`]*(?P<year>\d{2,4})\b",
    re.I,
)
_COMPACT_TENOR_RE = re.compile(r"\b(?P<year>20\d{2})[-_/]?(?P<month>0[1-9]|1[0-2])\b")
_SYMBOL_TENOR_RE = re.compile(r"\b[A-Z@]{0,3}(?P<month_code>[FGHJKMNQUVXZ])(?P<year>\d{1,2})\b", re.I)
_MONTH_TO_NUM = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_SYMBOL_MONTH_TO_NUM = {
    "f": 1,
    "g": 2,
    "h": 3,
    "j": 4,
    "k": 5,
    "m": 6,
    "n": 7,
    "q": 8,
    "u": 9,
    "v": 10,
    "x": 11,
    "z": 12,
}


def _slug(txt: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(txt or "").strip().lower()).strip("_")


def _parse_filename_date(path_like: Any) -> Optional[pd.Timestamp]:
    name = Path(str(path_like or "")).name
    for regex, order in (
        (_COMPACT_DATE_RE, ("year", "month", "day")),
        (_ISO_DATE_RE, ("year", "month", "day")),
        (_VENDOR_DATE_RE, ("month", "day", "year")),
    ):
        match = regex.search(name)
        if not match:
            continue
        try:
            if order == ("year", "month", "day"):
                year_num = int(match.group(1))
                month_num = int(match.group(2))
                day_num = int(match.group(3))
            else:
                month_num = int(match.group(1))
                day_num = int(match.group(2))
                year_num = int(match.group(3))
            return pd.Timestamp(year=year_num, month=month_num, day=day_num)
        except Exception:
            continue
    return None


def _tenor_from_parts(year_num: int, month_num: int) -> str:
    return f"{date(year_num, month_num, 1):%b}".lower() + f"{str(year_num)[-2:]}"


def _label_from_tenor(tenor: str) -> str:
    match = re.fullmatch(r"([a-z]{3})(\d{2})", str(tenor or "").strip().lower())
    if not match:
        return str(tenor or "").strip()
    month_num = _MONTH_TO_NUM.get(str(match.group(1) or "").lower())
    if month_num is None:
        return str(tenor or "").strip()
    year_num = 2000 + int(match.group(2))
    return f"{date(year_num, month_num, 1):%b %Y}"


def _normalize_contract_tenor(raw_value: Any) -> str:
    token = str(raw_value or "").strip()
    if not token:
        return ""
    match = _TENOR_RE.search(token)
    if match:
        month_num = _MONTH_TO_NUM.get(str(match.group("month") or "").lower())
        if month_num is None:
            return ""
        year_txt = str(match.group("year") or "").strip()
        year_num = int(year_txt)
        if year_num < 100:
            year_num += 2000
        return _tenor_from_parts(year_num, month_num)
    match = _COMPACT_TENOR_RE.search(token)
    if match:
        return _tenor_from_parts(int(match.group("year")), int(match.group("month")))
    match = _SYMBOL_TENOR_RE.search(token)
    if match:
        month_num = _SYMBOL_MONTH_TO_NUM.get(str(match.group("month_code") or "").lower())
        if month_num is None:
            return ""
        year_raw = int(match.group("year"))
        year_num = 2000 + year_raw if year_raw < 100 else year_raw
        return _tenor_from_parts(year_num, month_num)
    return ""


def _read_tabular_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    try:
        if suffix in {".csv", ".txt"}:
            for kwargs in (
                {"dtype": str, "keep_default_na": False},
                {"dtype": str, "keep_default_na": False, "sep": None, "engine": "python"},
            ):
                try:
                    df = pd.read_csv(path, **kwargs)
                    if not df.empty:
                        return df
                except Exception:
                    continue
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()


def _coerce_price(value_in: Any) -> Optional[float]:
    raw_txt = str(value_in or "").strip()
    if not raw_txt or raw_txt.lower() in {"nan", "n/a", "na", "--"}:
        return None
    value_num = pd.to_numeric(raw_txt.replace(",", ""), errors="coerce")
    if pd.isna(value_num):
        return None
    value = float(value_num)
    return value if value > 0.0 else None


def _normalize_market_price(*, market_family: str, value: float) -> float:
    value_num = float(value)
    # Barchart corn downloads are commonly quoted in cents/bu (e.g. 467.0 for
    # $4.67/bu), while the workbook paths expect $/bu. Keep already-normalized
    # test/manual inputs untouched and only rescale values that are obviously in
    # cents. Natural gas and other local futures stay in their native dollars.
    if str(market_family or "").strip().lower() == "corn_futures" and value_num > 25.0:
        return value_num / 100.0
    return value_num


def _source_file_priority(source_file: str) -> int:
    name = Path(str(source_file or "")).name.lower()
    if name.startswith("manual_"):
        return 2
    return 1


def _row_preference_tuple(source_path: Path) -> Tuple[int, int, float, str]:
    file_date = _parse_filename_date(source_path)
    file_ord = file_date.to_pydatetime().date().toordinal() if isinstance(file_date, pd.Timestamp) else 0
    try:
        mtime = float(source_path.stat().st_mtime)
    except Exception:
        mtime = 0.0
    name = source_path.name.lower()
    return (_source_file_priority(name), file_ord, mtime, name)


def _obs_row(
    *,
    source: str,
    observation_date: date,
    source_file: str,
    tenor: str,
    price_value: float,
    market_family: str,
    series_prefix: str,
    instrument: str,
    unit: str,
    source_type: str,
    source_label: str,
    parsed_note: str,
) -> Dict[str, Any]:
    clean_tenor = str(tenor or "").strip().lower()
    return {
        "observation_date": observation_date,
        "quarter": quarter_end_from_date(observation_date),
        "aggregation_level": "observation",
        "publication_date": observation_date,
        "source": source,
        "report_type": source_type,
        "source_type": source_type,
        "source_label": source_label,
        "market_family": market_family,
        "series_key": f"{series_prefix}_{clean_tenor}_usd",
        "instrument": instrument,
        "location": "barchart",
        "region": "barchart",
        "tenor": clean_tenor,
        "contract_tenor": clean_tenor,
        "contract_label": _label_from_tenor(clean_tenor),
        "price_value": float(price_value),
        "unit": unit,
        "quality": "high",
        "source_file": source_file,
        "parsed_note": parsed_note,
        "origin": "provider_raw",
        "_priority": 50,
        "_obs_count": 1,
    }


def parse_local_barchart_futures_table(
    path: Path,
    *,
    fallback_date: Optional[pd.Timestamp],
    source: str,
    source_type: str,
    source_label: str,
    market_family: str,
    series_prefix: str,
    instrument: str,
    unit: str,
) -> List[Dict[str, Any]]:
    df = _read_tabular_file(path)
    if df is None or df.empty:
        return []
    cols = list(df.columns)
    norm_map = {_slug(col): col for col in cols}
    contract_col = norm_map.get("contract")
    last_col = norm_map.get("last") or norm_map.get("latest") or norm_map.get("settlement") or norm_map.get("price")
    time_col = norm_map.get("time") or norm_map.get("date") or norm_map.get("trade_date")
    if not contract_col or not last_col:
        return []
    default_date = fallback_date or _parse_filename_date(path)
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        contract_txt = str(row.get(contract_col) or "").strip()
        if not contract_txt:
            continue
        contract_low = contract_txt.lower()
        if contract_low.startswith("downloaded from") or "cash" in contract_low:
            continue
        price_value = _coerce_price(row.get(last_col))
        if price_value is None:
            continue
        price_value = _normalize_market_price(market_family=market_family, value=price_value)
        obs_ts = pd.to_datetime(row.get(time_col), errors="coerce") if time_col else pd.NaT
        if pd.isna(obs_ts):
            obs_ts = default_date
        if obs_ts is None or pd.isna(obs_ts):
            continue
        tenor = _normalize_contract_tenor(contract_txt)
        if not tenor:
            continue
        rows.append(
            _obs_row(
                source=source,
                observation_date=pd.Timestamp(obs_ts).date(),
                source_file=path.name,
                tenor=tenor,
                price_value=float(price_value),
                market_family=market_family,
                series_prefix=series_prefix,
                instrument=instrument,
                unit=unit,
                source_type=source_type,
                source_label=source_label,
                parsed_note=f"{instrument} thesis input from local Barchart futures CSV.",
            )
        )
    return rows


class _LocalBarchartFuturesProvider(BaseMarketProvider):
    landing_page_url = "local_only"
    remote_timeout_seconds = 5
    market_family = ""
    instrument = ""
    unit = ""
    series_prefix = ""
    local_source_type = ""
    local_source_label = ""

    def discover_remote_assets(self, as_of: Optional[date] = None, cache_root: Optional[Path] = None) -> List[Dict[str, Any]]:
        self._write_remote_debug(
            cache_root,
            {
                "source": str(self.source or ""),
                "latest_refresh": {
                    "as_of": as_of,
                    "landing_page_url": "",
                    "landing_fetch": {
                        "status": "skipped",
                        "classification": "local_only_source",
                        "note": f"{self.instrument} thesis source is local-only in the active workflow.",
                    },
                    "selected_candidates": [],
                    "download_attempts": [],
                    "chosen_url": "",
                    "saved_local_path": "",
                    "final_classification": "local_only_source",
                },
            },
            merge=True,
        )
        return []

    @staticmethod
    def _date_from_name(path: Path) -> Optional[pd.Timestamp]:
        base = BaseMarketProvider._date_from_name(path)
        return base or _parse_filename_date(path)

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        del ticker_root
        out_rows: List[Dict[str, Any]] = []
        for entry in raw_entries:
            local_path = Path(str(entry.get("local_path") or "")).expanduser()
            if local_path.suffix.lower() not in {".csv", ".txt"} or not local_path.exists():
                continue
            if hasattr(self, "owns_local_asset") and not bool(self.owns_local_asset(local_path)):
                continue
            report_ts = self._date_from_value(entry.get("report_date")) or _parse_filename_date(local_path)
            rows = parse_local_barchart_futures_table(
                local_path,
                fallback_date=report_ts,
                source=self.source,
                source_type=self.local_source_type,
                source_label=self.local_source_label,
                market_family=self.market_family,
                series_prefix=self.series_prefix,
                instrument=self.instrument,
                unit=self.unit,
            )
            for rec in rows:
                rec["_source_path"] = str(local_path)
            out_rows.extend(rows)
        if not out_rows:
            self._record_parse_debug(cache_root, raw_entries, out_rows)
            return pd.DataFrame()

        preferred: Dict[Tuple[str, date], Dict[str, Any]] = {}
        for rec in out_rows:
            obs_date = rec.get("observation_date")
            series_key = str(rec.get("series_key") or "").strip()
            if not isinstance(obs_date, date) or not series_key:
                continue
            rec_copy = dict(rec)
            source_path = Path(str(rec_copy.get("_source_path") or rec_copy.get("source_file") or ""))
            preference = _row_preference_tuple(source_path)
            dedupe_key = (series_key, obs_date)
            current = preferred.get(dedupe_key)
            current_pref = current.get("_dedupe_preference") if isinstance(current, dict) else None
            if current is None or not isinstance(current_pref, tuple) or preference > current_pref:
                rec_copy["_dedupe_preference"] = preference
                preferred[dedupe_key] = rec_copy

        final_rows: List[Dict[str, Any]] = []
        for rec in preferred.values():
            clean = dict(rec)
            clean.pop("_dedupe_preference", None)
            clean.pop("_source_path", None)
            final_rows.append(clean)
        final_rows.sort(
            key=lambda rec: (
                pd.to_datetime(rec.get("observation_date"), errors="coerce")
                if rec.get("observation_date")
                else pd.Timestamp("1900-01-01"),
                str(rec.get("series_key") or ""),
                str(rec.get("source_file") or ""),
            )
        )
        self._record_parse_debug(cache_root, raw_entries, final_rows)
        return pd.DataFrame(final_rows)


class LocalBarchartCornFuturesProvider(_LocalBarchartFuturesProvider):
    source = "local_barchart_corn_futures"
    provider_parse_version = "v1"
    local_patterns = (
        "corn_futures/*.csv",
        "corn_futures/**/*.csv",
        "CBOT_corn_futures/*.csv",
        "CBOT_corn_futures/**/*.csv",
    )
    report_token = "local_barchart_corn_futures"
    stable_name_prefix = "local_barchart_corn_futures"
    local_dir_name = "corn_futures"
    market_family = "corn_futures"
    instrument = "Corn futures"
    unit = "$/bu"
    series_prefix = "cbot_corn"
    local_source_type = "local_barchart_corn_futures_csv"
    local_source_label = "local Barchart corn futures CSV"

    def owns_local_asset(self, path: Path) -> bool:
        parent_low = str(path.parent.name or "").strip().lower()
        if parent_low in {"corn_futures", "cbot_corn_futures"}:
            return True
        return "corn" in path.name.lower()


class LocalBarchartGasFuturesProvider(_LocalBarchartFuturesProvider):
    source = "local_barchart_gas_futures"
    provider_parse_version = "v1"
    local_patterns = (
        "naturalGas_futures/*.csv",
        "naturalGas_futures/**/*.csv",
    )
    report_token = "local_barchart_gas_futures"
    stable_name_prefix = "local_barchart_gas_futures"
    local_dir_name = "naturalGas_futures"
    market_family = "natural_gas_futures"
    instrument = "Natural gas futures"
    unit = "$/MMBtu"
    series_prefix = "nymex_gas"
    local_source_type = "local_barchart_gas_futures_csv"
    local_source_label = "local Barchart natural gas futures CSV"

    def owns_local_asset(self, path: Path) -> bool:
        parent_low = str(path.parent.name or "").strip().lower()
        if parent_low == "naturalgas_futures":
            return True
        if any(str(part or "").strip().lower() == "gas_futures" for part in path.parts):
            return False
        name_low = path.name.lower()
        return "gas" in name_low or "ng" in name_low


__all__ = [
    "LocalBarchartCornFuturesProvider",
    "LocalBarchartGasFuturesProvider",
    "parse_local_barchart_futures_table",
]
