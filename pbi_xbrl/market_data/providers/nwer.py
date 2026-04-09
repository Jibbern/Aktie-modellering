"""NWER provider for crush, DDGS, and related weekly market observations.

The parsed rows from this provider complement the AMS series so workbook
overlays can explain both market inputs and the bridge from unhedged economics
to reported results.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseMarketProvider
from ..aggregations import quarter_end_from_date


_MONTH_ABBREV = {
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
_ETHANOL_REGIONS = (
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Nebraska",
    "South Dakota",
    "Wisconsin",
)
_NWER_CORN_OIL_SERIES = (
    ("Illinois", "corn_oil_illinois"),
    ("Indiana", "corn_oil_indiana"),
    ("Iowa East", "corn_oil_iowa_east"),
    ("Iowa West", "corn_oil_iowa_west"),
    ("Kansas", "corn_oil_kansas"),
    ("Michigan", "corn_oil_michigan"),
    ("Minnesota", "corn_oil_minnesota"),
    ("Missouri", "corn_oil_missouri"),
    ("Nebraska", "corn_oil_nebraska"),
    ("Ohio", "corn_oil_ohio"),
    ("South Dakota", "corn_oil_south_dakota"),
    ("Wisconsin", "corn_oil_wisconsin"),
)
_NWER_DDGS_SERIES = (
    ("Illinois", "ddgs_10_illinois"),
    ("Indiana", "ddgs_10_indiana"),
    ("Iowa East", "ddgs_10_iowa_east"),
    ("Iowa West", "ddgs_10_iowa_west"),
    ("Kansas", "ddgs_10_kansas"),
    ("Michigan", "ddgs_10_michigan"),
    ("Minnesota", "ddgs_10_minnesota"),
    ("Missouri", "ddgs_10_missouri"),
    ("Nebraska", "ddgs_10_nebraska"),
    ("Ohio", "ddgs_10_ohio"),
    ("South Dakota", "ddgs_10_south_dakota"),
    ("Wisconsin", "ddgs_10_wisconsin"),
)
_DECIMAL_RE = re.compile(r"^\d+\.\d+$")
_PRICE_TOKEN_RE = re.compile(r"^\d+\.\d+(?:-\d+\.\d+)?$")


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


def _nwer_report_date(text: str, fallback: Optional[pd.Timestamp]) -> Optional[date]:
    match = re.search(r"Livestock,\s+Poultry,\s+and\s+Grain\s+Market\s+News\s+([A-Za-z]+\s+\d{1,2},\s+20\d{2})", str(text or ""))
    if match:
        ts = pd.to_datetime(match.group(1), errors="coerce")
        if not pd.isna(ts):
            return ts.date()
    if fallback is not None:
        return fallback.date()
    return None


def _tenor_key(month_txt: str, year_txt: str) -> str:
    return f"{str(month_txt or '').strip().lower()[:3]}{str(year_txt or '').strip()[-2:]}"


def _obs_row(
    *,
    report_date: date,
    source_file: str,
    series_key: str,
    market_family: str,
    instrument: str,
    region: str,
    unit: str,
    price_value: float,
    tenor: str = "",
    parsed_note: str = "",
) -> Dict[str, Any]:
    return {
        "observation_date": report_date,
        "quarter": quarter_end_from_date(report_date),
        "aggregation_level": "observation",
        "publication_date": report_date,
        "source": "nwer",
        "report_type": "nwer_pdf",
        "source_type": "nwer_pdf",
        "market_family": market_family,
        "series_key": series_key,
        "instrument": instrument,
        "location": region,
        "region": region,
        "tenor": tenor,
        "price_value": float(price_value),
        "unit": unit,
        "quality": "high",
        "source_file": source_file,
        "parsed_note": parsed_note,
        "origin": "provider_raw",
        "_priority": 50,
        "_obs_count": 1,
    }


def _extract_average_after_price_token(line: str) -> Optional[float]:
    tokens = [str(tok or "").strip() for tok in str(line or "").split() if str(tok or "").strip()]
    if len(tokens) < 3:
        return None
    price_idx: Optional[int] = None
    for idx, token in enumerate(tokens[1:], start=1):
        if _PRICE_TOKEN_RE.match(token):
            price_idx = idx
            break
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


def _parse_regional_section(
    text: str,
    *,
    start_markers: tuple[str, ...],
    stop_markers: tuple[str, ...],
    region_series: tuple[tuple[str, str], ...],
    unit: str,
    market_family: str,
    instrument: str,
    parsed_note_prefix: str,
    report_date: date,
    source_file: str,
) -> List[Dict[str, Any]]:
    capture = False
    rows_by_key: Dict[str, Dict[str, Any]] = {}
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if any(marker in line for marker in start_markers):
            capture = True
            continue
        if capture and any(marker in line for marker in stop_markers):
            capture = False
            continue
        if not capture:
            continue
        if line.startswith(("Ethanol Plant", "State/Province/Region", "Grain By-Products", "Source:", "National Weekly Ethanol Report", "Agricultural Marketing Service", "Livestock, Poultry, and Grain Market News", "Email us with accessibility issues", "Saint Joseph, MO", "www.ams.usda.gov", "https://mymarketnews.ams.usda.gov/", "Explanatory Notes:")):
            continue
        for region_label, series_key in region_series:
            if not line.startswith(f"{region_label} "):
                continue
            avg_value = _extract_average_after_price_token(line)
            if avg_value is None:
                break
            region_key = region_label.lower().replace(" ", "_")
            rows_by_key[series_key] = _obs_row(
                report_date=report_date,
                source_file=source_file,
                series_key=series_key,
                market_family=market_family,
                instrument=instrument,
                region=region_key,
                unit=unit,
                price_value=avg_value,
                parsed_note=f"{parsed_note_prefix} for {region_label}.",
            )
            break
    return list(rows_by_key.values())


def _parse_futures_line(
    text: str,
    *,
    prefix_pattern: str,
    parsed_prefix: str,
    series_prefix: str,
    unit: str,
    instrument: str,
    market_family: str,
    source_file: str,
    report_date: date,
    scale: float = 1.0,
) -> List[Dict[str, Any]]:
    match = re.search(rf"^{prefix_pattern}\s+(?P<body>.+)$", text, re.M)
    if not match:
        return []
    body = str(match.group("body") or "")
    pairs = re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+\(([A-Za-z]{3})\s+(\d{2})\)", body)
    out: List[Dict[str, Any]] = []
    for idx, (value_txt, month_txt, year_txt) in enumerate(pairs):
        try:
            value = float(value_txt) * scale
        except Exception:
            continue
        tenor = _tenor_key(month_txt, year_txt)
        series_key = f"{series_prefix}_{tenor}_usd"
        if idx == 0:
            front_series_key = "cbot_corn_usd_per_bu" if series_prefix == "cbot_corn" else "nymex_gas"
            out.append(
                _obs_row(
                    report_date=report_date,
                    source_file=source_file,
                    series_key=front_series_key,
                    market_family="corn_price" if series_prefix == "cbot_corn" else "natural_gas_price",
                    instrument="Corn price" if series_prefix == "cbot_corn" else "Natural gas price",
                    region="cbot" if series_prefix == "cbot_corn" else "nymex",
                    unit=unit,
                    price_value=value,
                    tenor="front",
                    parsed_note=f"Front-month futures settlement from {parsed_prefix}.",
                )
            )
        out.append(
            _obs_row(
                report_date=report_date,
                source_file=source_file,
                series_key=series_key,
                market_family=market_family,
                instrument=instrument,
                region="cbot" if series_prefix == "cbot_corn" else "nymex",
                unit=unit,
                price_value=value,
                tenor=tenor,
                parsed_note=f"{parsed_prefix} settlement for {month_txt} {year_txt}.",
            )
        )
    return out


def parse_nwer_pdf_text(text: str, *, fallback_date: Optional[pd.Timestamp], source_file: str) -> List[Dict[str, Any]]:
    report_date = _nwer_report_date(text, fallback=fallback_date)
    if report_date is None:
        return []
    rows: List[Dict[str, Any]] = []
    rows.extend(
        _parse_futures_line(
            text,
            prefix_pattern=r"CBOT Corn \((?:¢|Â¢)/bu\)",
            parsed_prefix="CBOT Corn (¢/bu)",
            series_prefix="cbot_corn",
            unit="$/bushel",
            instrument="Corn futures",
            market_family="corn_futures",
            source_file=source_file,
            report_date=report_date,
            scale=0.01,
        )
    )
    rows.extend(
        _parse_futures_line(
            text,
            prefix_pattern=r"NYMEX Natural Gas \(\$/MMBtu\)",
            parsed_prefix="NYMEX Natural Gas ($/MMBtu)",
            series_prefix="nymex_gas",
            unit="$/MMBtu",
            instrument="Natural gas futures",
            market_family="natural_gas_futures",
            source_file=source_file,
            report_date=report_date,
            scale=1.0,
        )
    )
    capture_ethanol = False
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if "Price ($ Per Gallon)" in line and "State/Province/Region" in line:
            capture_ethanol = True
            continue
        if capture_ethanol and (line.startswith("Source:") or line.startswith("Explanatory Notes:")):
            capture_ethanol = False
            continue
        if not capture_ethanol or "FOB" not in line:
            continue
        for region in _ETHANOL_REGIONS:
            if not line.startswith(f"{region} "):
                continue
            avg_value = _extract_average_after_price_token(line)
            if avg_value is None:
                continue
            rows.append(
                _obs_row(
                    report_date=report_date,
                    source_file=source_file,
                    series_key=f"ethanol_{region.lower().replace(' ', '_')}",
                    market_family="ethanol_price",
                    instrument="Ethanol price",
                    region=region.lower().replace(" ", "_"),
                    unit="$/gal",
                    price_value=avg_value,
                    parsed_note=f"Nebraska/Average weekly ethanol observation from {region}.",
                )
            )
            break
    rows.extend(
        _parse_regional_section(
            text,
            start_markers=("Distillers Corn Oil Feed Grade", "Distillers Corn Oil"),
            stop_markers=("Distillers Grain Dried 10%", "Distillers Grain Wet 65-70%", "Explanatory Notes:"),
            region_series=_NWER_CORN_OIL_SERIES,
            unit="c/lb",
            market_family="renewable_corn_oil_price",
            instrument="Renewable corn oil price",
            parsed_note_prefix="NWER distillers corn oil feed-grade average",
            report_date=report_date,
            source_file=source_file,
        )
    )
    rows.extend(
        _parse_regional_section(
            text,
            start_markers=("Distillers Grain Dried 10%",),
            stop_markers=("Distillers Grain Wet 65-70%", "Explanatory Notes:"),
            region_series=_NWER_DDGS_SERIES,
            unit="$/ton",
            market_family="ddgs_price",
            instrument="DDGS price",
            parsed_note_prefix="NWER distillers grain dried 10% average",
            report_date=report_date,
            source_file=source_file,
        )
    )
    return rows


class NWERProvider(BaseMarketProvider):
    source = "nwer"
    provider_parse_version = "v5"
    # New downloads live in the workbook-facing USDA folder, but we keep reading the
    # legacy provider-specific directory so older local restores continue to work.
    local_patterns = (
        "USDA_bioenergy_reports/*",
        "USDA_bioenergy_reports/**/*",
        "USDA_weekly_data/*",
        "USDA_weekly_data/**/*",
        "nwer_pdfs/*",
        "nwer_pdfs/**/*",
    )
    landing_page_url = "https://mymarketnews.ams.usda.gov/viewReport/3616"
    report_token = "/3616/"
    stable_name_prefix = "nwer"
    local_dir_name = "USDA_bioenergy_reports"

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        del ticker_root
        rows: List[Dict[str, Any]] = []
        for entry in raw_entries:
            report_ts = self._date_from_value(entry.get("report_date"))
            if report_ts is None:
                continue
            local_path = Path(str(entry.get("local_path") or "")).expanduser()
            if local_path.suffix.lower() != ".pdf" or not local_path.exists():
                continue
            text = _safe_pdf_text(local_path)
            if not text:
                continue
            rows.extend(parse_nwer_pdf_text(text, fallback_date=report_ts, source_file=local_path.name))
        self._record_parse_debug(cache_root, raw_entries, rows)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
