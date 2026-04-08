"""Local Chicago ethanol futures thesis provider for GPRE.

The active workflow now treats the local Chicago ethanol futures CSV export as
the practical thesis source. We keep the historical provider/source id and
series-key prefix for compatibility, but normal refreshes are intentionally
local-only and write an explicit debug artifact instead of attempting fragile
remote discovery.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .base import BaseMarketProvider
from ..aggregations import quarter_end_from_date


_TENOR_RE = re.compile(
    r"\b(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\s_/'`]*(?P<year>\d{2,4})\b",
    re.I,
)
_COMPACT_TENOR_RE = re.compile(r"\b(?P<year>20\d{2})[-_/]?(?P<month>0[1-9]|1[0-2])\b")
_SYMBOL_TENOR_RE = re.compile(r"\b[A-Z]{2}(?P<month_code>[FGHJKMNQUVXZ])(?P<year>\d{2})\b", re.I)
_COMPACT_DATE_RE = re.compile(r"(20\d{2})(\d{2})(\d{2})")
_ISO_DATE_RE = re.compile(r"(20\d{2})[-_](\d{2})[-_](\d{2})")
_VENDOR_DATE_RE = re.compile(r"(?<!\d)(\d{2})[-_](\d{2})[-_](20\d{2})(?!\d)")
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
_HEADER_PRODUCT_CANDIDATES = {
    "asset",
    "asset_code",
    "product_code",
    "commodity_code",
    "commodity",
    "product",
    "product_name",
    "description",
    "symbol",
    "globex_symbol",
    "security_group",
}
_HEADER_TENOR_CANDIDATES = {
    "contract_month",
    "contractmonth",
    "contract_month_year",
    "contractmonthyear",
    "maturitymonthyear",
    "maturity_month_year",
    "month_year",
    "monthyear",
    "delivery_month",
    "deliverymonth",
    "expiration",
    "expiry",
    "contract",
}
_HEADER_SETTLE_CANDIDATES = {
    "settle",
    "settlement",
    "settlement_price",
    "settle_price",
    "final_settlement",
    "value",
    "price",
    "last",
}
_HEADER_UNIT_CANDIDATES = {"unit", "units", "price_unit", "uom"}
_LOCAL_SOURCE_TYPE = "local_chicago_ethanol_futures_csv"
_LOCAL_SOURCE_LABEL = "local Chicago ethanol futures CSV"
_MANUAL_QUARTER_OPEN_SOURCE_TYPE = "manual_local_snapshot"
_MANUAL_QUARTER_OPEN_SOURCE_LABEL = "local manual quarter-open ethanol snapshot"
_MANUAL_QUARTER_OPEN_PATTERNS = (
    "Ethanol_futures/manual_ethanol_chicago_quarter_open*.csv",
    "Ethanol_futures/manual_ethanol_chicago_snapshot*.csv",
    "Ethanol_futures/ethanol_chicago_futures_*_Q*.txt",
    "Ethanol_futures/ethanol_chicago_futures_*_Q*.csv",
)
_MANUAL_TARGET_QUARTER_RE = re.compile(r"^(?P<year>20\d{2})-Q(?P<quarter>[1-4])$", re.I)
_MANUAL_CONTRACT_MONTH_RE = re.compile(r"^(?P<year>20\d{2})-(?P<month>0[1-9]|1[0-2])$")


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


def _normalize_contract_tenor(raw_value: Any, raw_year: Any = None, raw_month: Any = None) -> str:
    pieces = [str(raw_value or "").strip(), str(raw_year or "").strip(), str(raw_month or "").strip()]
    token = " ".join(part for part in pieces if part)
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
        year_num = 2000 + int(match.group("year"))
        return _tenor_from_parts(year_num, month_num)
    return ""


def _coerce_settle_usd_per_gal(value_in: Any, unit_hint: str = "") -> Optional[float]:
    raw_txt = str(value_in or "").strip()
    if not raw_txt or raw_txt.lower() in {"nan", "n/a", "na", "--"}:
        return None
    value_num = pd.to_numeric(raw_txt.replace(",", ""), errors="coerce")
    if pd.isna(value_num):
        return None
    value = float(value_num)
    unit_txt = str(unit_hint or "").strip().lower()
    if "cent" in unit_txt or "c/gal" in unit_txt or "cts" in unit_txt:
        return value / 100.0
    if abs(value) >= 10.0:
        return value / 100.0
    return value


def _obs_row(
    *,
    observation_date: date,
    source_file: str,
    tenor: str,
    price_value: float,
    parsed_note: str,
    contract_label: Optional[str] = None,
    source_type: str = _LOCAL_SOURCE_TYPE,
    report_type: str = _LOCAL_SOURCE_TYPE,
    source_label: str = _LOCAL_SOURCE_LABEL,
    product_code: str = "FL",
) -> Dict[str, Any]:
    clean_tenor = str(tenor or "").strip().lower()
    return {
        "observation_date": observation_date,
        "quarter": quarter_end_from_date(observation_date),
        "aggregation_level": "observation",
        "publication_date": observation_date,
        "source": "cme_ethanol_platts",
        "report_type": report_type,
        "source_type": source_type,
        "source_label": source_label,
        "market_family": "ethanol_futures",
        "series_key": f"cme_ethanol_chicago_platts_{clean_tenor}_usd_per_gal",
        "instrument": "Chicago Ethanol Futures",
        "location": "chicago",
        "region": "chicago",
        "tenor": clean_tenor,
        "contract_tenor": clean_tenor,
        "contract_label": str(contract_label or _label_from_tenor(clean_tenor)),
        "product_code": str(product_code or "FL"),
        "price_value": float(price_value),
        "unit": "$/gal",
        "quality": "high",
        "source_file": source_file,
        "parsed_note": parsed_note,
        "origin": "provider_raw",
        "_priority": 50,
        "_obs_count": 1,
    }


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
            return pd.DataFrame()
        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(path, dtype=str)
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()


def _looks_like_local_futures_export(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    cols = {_slug(col) for col in df.columns}
    return {"contract", "last", "time"}.issubset(cols)


def _iter_legacy_candidate_rows(df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    cols = list(df.columns)
    norm_map = {_slug(col): col for col in cols}
    product_cols = [norm_map[key] for key in _HEADER_PRODUCT_CANDIDATES if key in norm_map]
    tenor_cols = [norm_map[key] for key in _HEADER_TENOR_CANDIDATES if key in norm_map]
    settle_cols = [norm_map[key] for key in _HEADER_SETTLE_CANDIDATES if key in norm_map]
    unit_cols = [norm_map[key] for key in _HEADER_UNIT_CANDIDATES if key in norm_map]
    for _, row in df.iterrows():
        row_dict = {str(col): row[col] for col in cols}
        joined = " | ".join(str(val or "") for val in row_dict.values())
        joined_low = joined.lower()
        product_hit = (
            any(str(row_dict.get(col) or "").strip().upper() == "CU" for col in product_cols)
            or ("chicago ethanol" in joined_low and "platts" in joined_low)
        )
        if not product_hit:
            continue
        tenor = ""
        for col in tenor_cols:
            tenor = _normalize_contract_tenor(row_dict.get(col))
            if tenor:
                break
        if not tenor:
            tenor = _normalize_contract_tenor(joined)
        if not tenor:
            continue
        settle_value = None
        unit_hint = ""
        for col in settle_cols:
            unit_hint = next(
                (str(row_dict.get(u_col) or "") for u_col in unit_cols if str(row_dict.get(u_col) or "").strip()),
                "",
            )
            settle_value = _coerce_settle_usd_per_gal(row_dict.get(col), unit_hint=unit_hint)
            if settle_value is not None:
                break
        if settle_value is None:
            continue
        yield {
            "observation_date": None,
            "tenor": tenor,
            "contract_label": _label_from_tenor(tenor),
            "settle_value": settle_value,
            "source_type": "cme_ethanol_settlement",
            "source_label": "official CME settlement file",
            "report_type": "cme_ethanol_settlement",
            "product_code": "CU",
            "parsed_note": "Chicago Ethanol (Platts) futures settlement from official CME settlement file.",
        }


def _iter_local_futures_rows(
    df: pd.DataFrame,
    *,
    path: Path,
    fallback_date: Optional[pd.Timestamp],
) -> Iterable[Dict[str, Any]]:
    cols = list(df.columns)
    norm_map = {_slug(col): col for col in cols}
    contract_col = norm_map.get("contract")
    last_col = norm_map.get("last")
    time_col = norm_map.get("time")
    if not contract_col or not last_col or not time_col:
        return []
    default_date = fallback_date or _parse_filename_date(path)
    out: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        contract_txt = str(row.get(contract_col) or "").strip()
        if not contract_txt:
            continue
        if contract_txt.lower().startswith("downloaded from"):
            continue
        price_value = _coerce_settle_usd_per_gal(row.get(last_col), unit_hint="$/gal")
        if price_value is None or price_value <= 0.0:
            continue
        obs_ts = pd.to_datetime(row.get(time_col), errors="coerce")
        if pd.isna(obs_ts):
            obs_ts = default_date
        if obs_ts is None or pd.isna(obs_ts):
            continue
        tenor = _normalize_contract_tenor(contract_txt)
        if not tenor:
            continue
        out.append(
            {
                "observation_date": pd.Timestamp(obs_ts).date(),
                "tenor": tenor,
                "contract_label": _label_from_tenor(tenor),
                "settle_value": float(price_value),
                "source_type": _LOCAL_SOURCE_TYPE,
                "source_label": _LOCAL_SOURCE_LABEL,
                "report_type": _LOCAL_SOURCE_TYPE,
                "product_code": str(re.match(r"([A-Z]{2})", contract_txt, re.I).group(1) if re.match(r"([A-Z]{2})", contract_txt, re.I) else "FL"),
                "parsed_note": "Chicago ethanol futures thesis input from local end-of-day futures CSV.",
            }
        )
    return out


def _manual_target_quarter_end(raw_value: Any) -> Optional[date]:
    match = _MANUAL_TARGET_QUARTER_RE.fullmatch(str(raw_value or "").strip())
    if not match:
        return None
    year_num = int(match.group("year"))
    quarter_num = int(match.group("quarter"))
    month_num = quarter_num * 3
    quarter_end_day = 31 if month_num in {3, 12} else 30
    try:
        return date(year_num, month_num, quarter_end_day)
    except Exception:
        return None


def _manual_contract_month_parts(raw_value: Any) -> tuple[Optional[date], str]:
    match = _MANUAL_CONTRACT_MONTH_RE.fullmatch(str(raw_value or "").strip())
    if not match:
        return None, ""
    year_num = int(match.group("year"))
    month_num = int(match.group("month"))
    try:
        month_start = date(year_num, month_num, 1)
    except Exception:
        return None, ""
    tenor = _tenor_from_parts(year_num, month_num)
    return month_start, tenor


def _manual_snapshot_file_priority(source_file: str) -> int:
    name = Path(str(source_file or "")).name.lower()
    if name.startswith("manual_ethanol_chicago_quarter_open"):
        return 3
    if name.startswith("manual_ethanol_chicago_snapshot"):
        return 2
    if name.startswith("ethanol_chicago_futures_"):
        return 1
    return 0


def _manual_snapshot_preference_tuple(path: Path, snapshot_date: Optional[date]) -> Tuple[int, int, int, float, str]:
    file_date = _parse_filename_date(path)
    file_ord = file_date.to_pydatetime().date().toordinal() if isinstance(file_date, pd.Timestamp) else 0
    snapshot_ord = int(snapshot_date.toordinal()) if isinstance(snapshot_date, date) else 0
    try:
        mtime = float(path.stat().st_mtime)
    except Exception:
        mtime = 0.0
    name = path.name.lower()
    return (_manual_snapshot_file_priority(name), snapshot_ord, file_ord, mtime, name)


def find_local_manual_ethanol_quarter_open_files(ticker_root: Optional[Path]) -> List[Path]:
    if not isinstance(ticker_root, Path):
        return []
    found: List[Path] = []
    seen: set[Path] = set()
    for pattern in _MANUAL_QUARTER_OPEN_PATTERNS:
        for path in sorted(ticker_root.glob(pattern)):
            if not path.is_file():
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            found.append(resolved)
    return found


def parse_manual_ethanol_quarter_open_snapshot_table(path: Path) -> List[Dict[str, Any]]:
    df = _read_tabular_file(path)
    if df is None or df.empty:
        return []
    cols = list(df.columns)
    norm_map = {_slug(col): col for col in cols}
    snapshot_col = norm_map.get("snapshot_date")
    target_quarter_col = norm_map.get("target_quarter")
    contract_month_col = norm_map.get("contract_month")
    settle_col = norm_map.get("settle_usd_per_gal")
    source_col = norm_map.get("source")
    if not snapshot_col or not target_quarter_col or not contract_month_col or not settle_col:
        return []
    parsed_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        snapshot_ts = pd.to_datetime(row.get(snapshot_col), errors="coerce")
        if pd.isna(snapshot_ts):
            continue
        snapshot_date = pd.Timestamp(snapshot_ts).date()
        target_quarter_txt = str(row.get(target_quarter_col) or "").strip()
        target_quarter_end = _manual_target_quarter_end(target_quarter_txt)
        if not isinstance(target_quarter_end, date):
            continue
        contract_month_start, contract_tenor = _manual_contract_month_parts(row.get(contract_month_col))
        if not isinstance(contract_month_start, date) or not contract_tenor:
            continue
        if quarter_end_from_date(contract_month_start) != target_quarter_end:
            continue
        settle_num = pd.to_numeric(row.get(settle_col), errors="coerce")
        if pd.isna(settle_num) or float(settle_num) <= 0.0:
            continue
        source_txt = str(row.get(source_col) or "").strip() if source_col else ""
        parsed_rows.append(
            {
                "snapshot_date": snapshot_date,
                "target_quarter": target_quarter_txt,
                "target_quarter_end": target_quarter_end,
                "contract_month": contract_month_start,
                "contract_tenor": contract_tenor,
                "contract_label": _label_from_tenor(contract_tenor),
                "settle_usd_per_gal": float(settle_num),
                "price_value": float(settle_num),
                "source": source_txt or "manual_local_snapshot",
                "source_file": path.name,
                "source_type": _MANUAL_QUARTER_OPEN_SOURCE_TYPE,
                "source_label": _MANUAL_QUARTER_OPEN_SOURCE_LABEL,
            }
        )
    return parsed_rows


def load_local_manual_ethanol_quarter_open_snapshot_rows(ticker_root: Optional[Path]) -> List[Dict[str, Any]]:
    preferred: Dict[Tuple[date, str], Dict[str, Any]] = {}
    for path in find_local_manual_ethanol_quarter_open_files(ticker_root):
        for rec in parse_manual_ethanol_quarter_open_snapshot_table(path):
            target_quarter_end = rec.get("target_quarter_end")
            contract_tenor = str(rec.get("contract_tenor") or "").strip().lower()
            if not isinstance(target_quarter_end, date) or not contract_tenor:
                continue
            candidate = dict(rec)
            candidate["_preference"] = _manual_snapshot_preference_tuple(path, rec.get("snapshot_date"))
            dedupe_key = (target_quarter_end, contract_tenor)
            current = preferred.get(dedupe_key)
            current_pref = current.get("_preference") if isinstance(current, dict) else None
            if current is None or not isinstance(current_pref, tuple) or candidate["_preference"] > current_pref:
                preferred[dedupe_key] = candidate
    out: List[Dict[str, Any]] = []
    for rec in preferred.values():
        clean = dict(rec)
        clean.pop("_preference", None)
        out.append(clean)
    out.sort(
        key=lambda rec: (
            rec.get("target_quarter_end") if isinstance(rec.get("target_quarter_end"), date) else date.min,
            rec.get("contract_month") if isinstance(rec.get("contract_month"), date) else date.min,
            rec.get("snapshot_date") if isinstance(rec.get("snapshot_date"), date) else date.min,
            str(rec.get("source_file") or ""),
        )
    )
    return out


def parse_cme_ethanol_settlement_table(
    path: Path,
    *,
    fallback_date: Optional[pd.Timestamp],
) -> List[Dict[str, Any]]:
    default_date = _parse_filename_date(path) or fallback_date
    df = _read_tabular_file(path)
    if df is None or df.empty:
        return []
    if _looks_like_local_futures_export(df):
        candidates = list(_iter_local_futures_rows(df, path=path, fallback_date=default_date))
    else:
        candidates = list(_iter_legacy_candidate_rows(df))
    rows: List[Dict[str, Any]] = []
    for candidate in candidates:
        obs_date = candidate.get("observation_date")
        if not isinstance(obs_date, date):
            obs_date = default_date.date() if isinstance(default_date, pd.Timestamp) else None
        if not isinstance(obs_date, date):
            continue
        rows.append(
            _obs_row(
                observation_date=obs_date,
                source_file=path.name,
                tenor=str(candidate.get("tenor") or ""),
                contract_label=str(candidate.get("contract_label") or ""),
                price_value=float(candidate.get("settle_value") or 0.0),
                parsed_note=str(candidate.get("parsed_note") or ""),
                source_type=str(candidate.get("source_type") or _LOCAL_SOURCE_TYPE),
                report_type=str(candidate.get("report_type") or _LOCAL_SOURCE_TYPE),
                source_label=str(candidate.get("source_label") or _LOCAL_SOURCE_LABEL),
                product_code=str(candidate.get("product_code") or "FL"),
            )
        )
    return rows


def _source_file_priority(source_file: str) -> int:
    name = Path(str(source_file or "")).name.lower()
    if name.startswith("manual_cme_ethanol_chicago_eod"):
        return 2
    if name.startswith("ethanol-chicago-prices-end-of-day"):
        return 1
    return 0


def _row_preference_tuple(source_path: Path) -> Tuple[int, int, float, str]:
    file_date = _parse_filename_date(source_path)
    file_ord = file_date.to_pydatetime().date().toordinal() if isinstance(file_date, pd.Timestamp) else 0
    try:
        mtime = float(source_path.stat().st_mtime)
    except Exception:
        mtime = 0.0
    name = source_path.name.lower()
    return (_source_file_priority(name), file_ord, mtime, name)


class CMEChicagoEthanolPlattsProvider(BaseMarketProvider):
    source = "cme_ethanol_platts"
    provider_parse_version = "v2"
    local_patterns = (
        "Ethanol_futures/manual_cme_ethanol_chicago_eod*.csv",
        "Ethanol_futures/ethanol-chicago-prices-end-of-day-*.csv",
        "CME_ethanol_settlements/*.csv",
        "CME_ethanol_settlements/**/*.csv",
        "cme_ethanol_platts/*.csv",
        "cme_ethanol_platts/**/*.csv",
    )
    landing_page_url = "local_only"
    report_token = "cme_ethanol_chicago_platts"
    stable_name_prefix = "cme_ethanol_platts"
    local_dir_name = "Ethanol_futures"

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
                        "note": "Chicago ethanol thesis source is local-only in the active workflow.",
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
            if local_path.suffix.lower() not in {".csv", ".xlsx", ".xls"} or not local_path.exists():
                continue
            report_ts = self._date_from_value(entry.get("report_date")) or _parse_filename_date(local_path)
            rows = parse_cme_ethanol_settlement_table(local_path, fallback_date=report_ts)
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

        final_rows = []
        for rec in preferred.values():
            clean = dict(rec)
            clean.pop("_dedupe_preference", None)
            clean.pop("_source_path", None)
            final_rows.append(clean)
        final_rows.sort(
            key=lambda rec: (
                pd.to_datetime(rec.get("observation_date"), errors="coerce") if rec.get("observation_date") else pd.Timestamp("1900-01-01"),
                str(rec.get("series_key") or ""),
                str(rec.get("source_file") or ""),
            )
        )
        self._record_parse_debug(cache_root, raw_entries, final_rows)
        return pd.DataFrame(final_rows)
