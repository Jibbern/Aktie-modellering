"""AMS 3617 provider for ethanol and co-product market observations.

This provider turns the restored PDF history in `sec_cache/market_data/raw` into
normalized observations and quarterly summaries that feed the GPRE economics
overlay.
"""
from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseMarketProvider
from ..aggregations import quarter_end_from_date


_CORN_REGIONS = (
    "South Dakota",
    "Iowa East",
    "Iowa West",
    "Nebraska",
    "Illinois",
    "Indiana",
    "Kansas",
    "Michigan",
    "Minnesota",
    "Missouri",
    "Ohio",
    "Wisconsin",
)
_DECIMAL_RE = re.compile(r"^\d+\.\d+$")
_PRICE_RANGE_RE = re.compile(r"^\d+\.\d+-\d+\.\d+$")
_BASIS_RANGE_RE = re.compile(
    r"^(?P<low>[+-]?\d+(?:\.\d+)?)[A-Z]?\s+to\s+(?P<high>[+-]?\d+(?:\.\d+)?)[A-Z]?\b",
    re.I,
)
_BASIS_SINGLE_RE = re.compile(r"^(?P<value>[+-]?\d+(?:\.\d+)?)[A-Z]?\b", re.I)


def _safe_pdf_text(pdf_path: Path) -> str:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ""
    pages: List[str] = []
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                if txt:
                    pages.append(txt)
    except Exception:
        return ""
    return "\n".join(pages)


def _ams3617_report_date(text: str, fallback: Optional[pd.Timestamp]) -> Optional[date]:
    match = re.search(r"Livestock,\s+Poultry,\s+and\s+Grain\s+Market\s+News\s+([A-Za-z]+\s+\d{1,2},\s+20\d{2})", str(text or ""))
    if match:
        ts = pd.to_datetime(match.group(1), errors="coerce")
        if not pd.isna(ts):
            return ts.date()
    if fallback is not None:
        return fallback.date()
    return None


def _obs_row(
    *,
    report_date: date,
    source_file: str,
    series_key: str,
    region: str,
    price_value: float,
    parsed_note: str,
    market_family: str = "corn_price",
    instrument: str = "Corn cash price",
    unit: str = "$/bushel",
    source_type: str = "ams_3617_pdf",
) -> Dict[str, Any]:
    return {
        "observation_date": report_date,
        "quarter": quarter_end_from_date(report_date),
        "aggregation_level": "observation",
        "publication_date": report_date,
        "source": "ams_3617",
        "report_type": source_type,
        "source_type": source_type,
        "market_family": market_family,
        "series_key": series_key,
        "instrument": instrument,
        "location": region,
        "region": region,
        "tenor": "",
        "price_value": float(price_value),
        "unit": unit,
        "quality": "high",
        "source_file": source_file,
        "parsed_note": parsed_note,
        "origin": "provider_raw",
        "_priority": 50,
        "_obs_count": 1,
    }


def _extract_average_from_corn_line(line: str) -> Optional[float]:
    tokens = [str(tok or "").strip() for tok in str(line or "").split() if str(tok or "").strip()]
    if len(tokens) < 4:
        return None
    price_idx: Optional[int] = None
    for idx, token in enumerate(tokens[1:], start=1):
        if _PRICE_RANGE_RE.match(token):
            price_idx = idx
    if price_idx is None:
        return None
    numeric_tail: List[float] = []
    for token in tokens[price_idx + 1 :]:
        if _DECIMAL_RE.match(token):
            try:
                numeric_tail.append(float(token))
            except Exception:
                continue
    if not numeric_tail:
        return None
    if len(numeric_tail) == 1:
        return numeric_tail[0]
    if len(numeric_tail) == 2:
        first, second = numeric_tail
        if first < 1.0 <= second:
            return second
        return first
    if numeric_tail[-1] >= 1.0 and numeric_tail[-2] >= 1.0:
        return numeric_tail[-2]
    return numeric_tail[-1]


def _extract_basis_midpoint_from_corn_line(line: str, region: str) -> Optional[float]:
    raw_line = str(line or "").strip()
    region_prefix = f"{str(region or '').strip()} "
    if not raw_line.startswith(region_prefix):
        return None
    tail = raw_line[len(region_prefix) :].strip()
    if tail.lower().startswith("bid "):
        tail = tail[4:].strip()
    match = _BASIS_RANGE_RE.match(tail)
    if match:
        try:
            low = float(match.group("low"))
            high = float(match.group("high"))
            return (low + high) / 2.0
        except Exception:
            return None
    match = _BASIS_SINGLE_RE.match(tail)
    if match:
        try:
            return float(match.group("value"))
        except Exception:
            return None
    return None


def parse_ams_3617_pdf_text(text: str, *, fallback_date: Optional[pd.Timestamp], source_file: str) -> List[Dict[str, Any]]:
    report_date = _ams3617_report_date(text, fallback=fallback_date)
    if report_date is None:
        return []
    rows: List[Dict[str, Any]] = []
    capture_corn = False
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if "Price ($/Bu)" in line and "State/Province/Region" in line:
            capture_corn = True
            continue
        if capture_corn and (line.startswith("Source:") or line.startswith("Explanatory Notes:")):
            capture_corn = False
            continue
        if not capture_corn or "DLVD" not in line:
            continue
        for region in _CORN_REGIONS:
            if not line.startswith(f"{region} "):
                continue
            avg_value = _extract_average_from_corn_line(line)
            if avg_value is None:
                continue
            basis_mid_cents = _extract_basis_midpoint_from_corn_line(line, region)
            region_key = region.lower().replace(" ", "_")
            rows.append(
                _obs_row(
                    report_date=report_date,
                    source_file=source_file,
                    series_key=f"corn_{region_key}",
                    region=region_key,
                    price_value=avg_value,
                    parsed_note=f"US #2 Yellow Corn - Bulk daily average for {region}.",
                )
            )
            if basis_mid_cents is not None:
                rows.append(
                    _obs_row(
                        report_date=report_date,
                        source_file=source_file,
                        series_key=f"corn_basis_{region_key}",
                        region=region_key,
                        price_value=float(basis_mid_cents) / 100.0,
                        parsed_note=f"Midpoint of reported daily corn basis range for {region} from AMS 3617.",
                        market_family="corn_basis",
                        instrument="Corn basis",
                        unit="$/bushel",
                    )
                )
            break
    return rows


def _float_value(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if pd.isna(out):
        return None
    return out


def _public_region_label(row: Dict[str, Any]) -> str:
    # USDA public_data splits Iowa into state + region fields; the legacy PDF parser
    # already uses `Iowa East` / `Iowa West`, so normalize to the same keys here.
    state_value = str(row.get("state/Province") or row.get("state_province") or "").strip()
    region_value = str(row.get("region") or "").strip()
    if state_value == "Iowa" and region_value in {"East", "West"}:
        return f"Iowa {region_value}"
    for key in ("state/Province", "state_province", "region", "trade_loc"):
        value = str(row.get(key) or "").strip()
        if value and value.upper() != "N/A":
            return value
    return ""


def parse_ams_3617_public_data_payload(payload: Dict[str, Any], *, source_file: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in list((payload or {}).get("results") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("commodity") or "").strip().lower() != "corn":
            continue
        if str(item.get("quote_type") or "").strip().lower() != "basis":
            continue
        report_ts = pd.to_datetime(item.get("report_end_date") or item.get("report_date") or item.get("report_begin_date"), errors="coerce")
        if pd.isna(report_ts):
            continue
        region_label = _public_region_label(item)
        if region_label not in _CORN_REGIONS:
            continue
        region_key = region_label.lower().replace(" ", "_")
        avg_value = _float_value(item.get("avg_price"))
        if avg_value is None:
            price_min = _float_value(item.get("price Min"))
            price_max = _float_value(item.get("price Max"))
            if price_min is not None and price_max is not None:
                avg_value = (price_min + price_max) / 2.0
        if avg_value is not None:
            rows.append(
                _obs_row(
                    report_date=report_ts.date(),
                    source_file=source_file,
                    series_key=f"corn_{region_key}",
                    region=region_key,
                    price_value=avg_value,
                    parsed_note=f"US #2 Yellow Corn - Bulk daily average for {region_label} from USDA public_data.",
                    source_type="ams_3617_public_data",
                )
            )
        basis_min = _float_value(item.get("basis Min"))
        basis_max = _float_value(item.get("basis Max"))
        if basis_min is None and basis_max is None:
            continue
        if basis_min is None:
            basis_mid = basis_max
        elif basis_max is None:
            basis_mid = basis_min
        else:
            basis_mid = (basis_min + basis_max) / 2.0
        rows.append(
            _obs_row(
                report_date=report_ts.date(),
                source_file=source_file,
                series_key=f"corn_basis_{region_key}",
                region=region_key,
                price_value=float(basis_mid) / 100.0,
                parsed_note=f"Midpoint of USDA public_data daily corn basis range for {region_label}.",
                market_family="corn_basis",
                instrument="Corn basis",
                unit="$/bushel",
                source_type="ams_3617_public_data",
            )
        )
    return rows


def _public_payload_slug_id(payload: Dict[str, Any]) -> str:
    for item in list((payload or {}).get("results") or []):
        if isinstance(item, dict) and str(item.get("slug_id") or "").strip():
            return str(item.get("slug_id") or "").strip()
    return ""


def _public_payload_report_date(payload: Dict[str, Any]) -> Optional[pd.Timestamp]:
    for item in list((payload or {}).get("results") or []):
        if not isinstance(item, dict):
            continue
        report_ts = pd.to_datetime(
            item.get("report_end_date") or item.get("report_date") or item.get("report_begin_date"),
            errors="coerce",
        )
        if not pd.isna(report_ts):
            return pd.Timestamp(report_ts)
    return None


class AMS3617Provider(BaseMarketProvider):
    source = "ams_3617"
    provider_parse_version = "v8"
    # New downloads live in the workbook-facing USDA folder, but we keep reading the
    # legacy provider-specific directory so older local restores continue to work.
    local_patterns = (
        "USDA_daily_data/*",
        "USDA_daily_data/**/*",
        "ams_3617_pdfs/*",
        "ams_3617_pdfs/**/*",
    )
    landing_page_url = "https://mymarketnews.ams.usda.gov/viewReport/3617"
    public_data_url = "https://mymarketnews.ams.usda.gov/public_data?slug_id=3617"
    public_data_slug_id = "3617"
    report_token = "/3617/"
    stable_name_prefix = "ams_3617"
    local_dir_name = "USDA_daily_data"

    def owns_local_asset(self, path: Path) -> bool:
        name_low = path.name.lower()
        return (
            name_low.startswith("ams_3617")
            or name_low in {"ams_3617_daily_corn.csv", "ams_3617_weekly_corn.csv"}
        )

    def infer_local_report_date(self, path: Path) -> Optional[pd.Timestamp]:
        base = self._date_from_name(path)
        if base is not None:
            return base
        if path.suffix.lower() == ".json" and path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return None
            if isinstance(payload, dict):
                return _public_payload_report_date(payload)
            return None
        if path.suffix.lower() != ".pdf" or not path.exists():
            return None
        text = _safe_pdf_text(path)
        report_date = _ams3617_report_date(text, fallback=None) if text else None
        return pd.Timestamp(report_date) if isinstance(report_date, date) else None

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        del ticker_root
        rows: List[Dict[str, Any]] = []
        grouped_entries: Dict[str, List[tuple[pd.Timestamp, Path]]] = {}
        for entry in raw_entries:
            report_ts = self._date_from_value(entry.get("report_date"))
            local_path = Path(str(entry.get("local_path") or "")).expanduser()
            if report_ts is None or not self.owns_local_asset(local_path):
                continue
            group_key = report_ts.date().isoformat()
            grouped_entries.setdefault(group_key, []).append((report_ts, local_path))
        for group_key in sorted(grouped_entries.keys()):
            group = list(grouped_entries.get(group_key) or [])
            json_entries = [(report_ts, path) for report_ts, path in group if path.suffix.lower() == ".json" and path.exists()]
            eligible_entries = json_entries if json_entries else group
            for report_ts, local_path in sorted(eligible_entries, key=lambda item: str(item[1]).lower()):
                if local_path.suffix.lower() == ".json" and local_path.exists():
                    try:
                        payload = json.loads(local_path.read_text(encoding="utf-8"))
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        slug_id = _public_payload_slug_id(payload)
                        if slug_id and slug_id != "3617":
                            continue
                        rows.extend(parse_ams_3617_public_data_payload(payload, source_file=local_path.name))
                    continue
                if local_path.suffix.lower() != ".pdf" or not local_path.exists():
                    continue
                text = _safe_pdf_text(local_path)
                if not text:
                    continue
                rows.extend(parse_ams_3617_pdf_text(text, fallback_date=report_ts, source_file=local_path.name))
        self._record_parse_debug(cache_root, raw_entries, rows)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
