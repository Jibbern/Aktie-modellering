"""AMS 3618 provider for weekly grain co-product market observations.

Stage B.1 only parses the direct coproduct price tables we can wire cleanly
into GPRE today: distillers corn oil and DDGS 10%. Soybean rows stay out of
scope in this pass even though the report contains them.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseMarketProvider
from ..aggregations import quarter_end_from_date


_AMS3618_CORN_OIL_SERIES = (
    ("Eastern Cornbelt", "corn_oil_eastern_cornbelt"),
    ("Iowa", "corn_oil_iowa_avg"),
    ("Kansas", "corn_oil_kansas"),
    ("Minnesota", "corn_oil_minnesota"),
    ("Missouri", "corn_oil_missouri"),
    ("Nebraska", "corn_oil_nebraska"),
    ("South Dakota", "corn_oil_south_dakota"),
    ("Wisconsin", "corn_oil_wisconsin"),
)
_AMS3618_DDGS_SERIES = (
    ("Illinois", "ddgs_10_illinois"),
    ("Indiana", "ddgs_10_indiana"),
    ("Iowa", "ddgs_10_iowa"),
    ("Kansas", "ddgs_10_kansas"),
    ("Michigan", "ddgs_10_michigan"),
    ("Minnesota", "ddgs_10_minnesota"),
    ("Missouri", "ddgs_10_missouri"),
    ("Nebraska", "ddgs_10_nebraska"),
    ("Ohio", "ddgs_10_ohio"),
    ("South Dakota", "ddgs_10_south_dakota"),
    ("Wisconsin", "ddgs_10_wisconsin"),
)
_FIRST_DECIMAL_RE = re.compile(r"(?<!\d)(\d+\.\d+)\b")


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


def _ams3618_report_date(text: str, fallback: Optional[pd.Timestamp]) -> Optional[date]:
    match = re.search(
        r"Livestock,\s*Poultry(?:,\s*|\s+)and\s+Grain\s+Market\s+News\s+([A-Za-z]+\s+\d{1,2},\s+20\d{2})",
        str(text or ""),
    )
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
    market_family: str,
    instrument: str,
    unit: str,
) -> Dict[str, Any]:
    return {
        "observation_date": report_date,
        "quarter": quarter_end_from_date(report_date),
        "aggregation_level": "observation",
        "publication_date": report_date,
        "source": "ams_3618",
        "report_type": "ams_3618_pdf",
        "source_type": "ams_3618_pdf",
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


def _extract_first_numeric_after_region(line: str, region_label: str) -> Optional[float]:
    raw_line = str(line or "").strip()
    prefix = f"{str(region_label or '').strip()} "
    if not raw_line.startswith(prefix):
        return None
    tail = raw_line[len(prefix) :].strip()
    match = _FIRST_DECIMAL_RE.search(tail)
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def _parse_region_value_section(
    text: str,
    *,
    start_marker: str,
    stop_markers: tuple[str, ...],
    region_series: tuple[tuple[str, str], ...],
    market_family: str,
    instrument: str,
    unit: str,
    parsed_note_prefix: str,
    report_date: date,
    source_file: str,
) -> List[Dict[str, Any]]:
    capture = False
    section_lines: List[str] = []
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if start_marker in line:
            capture = True
            continue
        if capture and any(marker in line for marker in stop_markers):
            break
        if not capture:
            continue
        if line.startswith(("Region/Location", "Price (", "Value ($/Bu)", "Source:", "Page ", "USDA AMS Livestock, Poultry", "Saint Joseph, MO", "www.ams.usda.gov", "https://mymarketnews.ams.usda.gov/", "Email us with accessibility issues", "National Weekly Grain Co-Products Report", "Agricultural Marketing Service", "Future Settlements", "Average Input Price", "Corn Values", "Soybeans", "Ethanol ", "Ethanol")):
            continue
        section_lines.append(line)
    if not section_lines:
        return []
    section_text = re.sub(r"\s+", " ", " ".join(section_lines)).strip()
    rows_by_key: Dict[str, Dict[str, Any]] = {}
    for region_label, series_key in region_series:
        match = re.search(rf"{re.escape(region_label)}\s+(\d+\.\d+)\b", section_text)
        if not match:
            continue
        try:
            price_value = float(match.group(1))
        except Exception:
            continue
        region_key = region_label.lower().replace(" ", "_")
        rows_by_key[series_key] = _obs_row(
            report_date=report_date,
            source_file=source_file,
            series_key=series_key,
            region=region_key,
            price_value=price_value,
            parsed_note=f"{parsed_note_prefix} for {region_label}.",
            market_family=market_family,
            instrument=instrument,
            unit=unit,
        )
    return list(rows_by_key.values())


def parse_ams_3618_pdf_text(text: str, *, fallback_date: Optional[pd.Timestamp], source_file: str) -> List[Dict[str, Any]]:
    report_date = _ams3618_report_date(text, fallback=fallback_date)
    if report_date is None:
        return []
    rows: List[Dict[str, Any]] = []
    rows.extend(
        _parse_region_value_section(
            text,
            start_marker="Distillers Corn Oil",
            stop_markers=("Distillers Grain Dried 10%", "Distillers Grain Wet 65-70%", "Soybean Meal", "Soybean Oil", "Explanatory Notes:"),
            region_series=_AMS3618_CORN_OIL_SERIES,
            market_family="renewable_corn_oil_price",
            instrument="Renewable corn oil price",
            unit="c/lb",
            parsed_note_prefix="AMS 3618 distillers corn oil value",
            report_date=report_date,
            source_file=source_file,
        )
    )
    rows.extend(
        _parse_region_value_section(
            text,
            start_marker="Distillers Grain Dried 10%",
            stop_markers=("Distillers Grain Wet 65-70%", "Ethanol", "Soybean Meal", "Soybean Oil", "Explanatory Notes:"),
            region_series=_AMS3618_DDGS_SERIES,
            market_family="ddgs_price",
            instrument="DDGS price",
            unit="$/ton",
            parsed_note_prefix="AMS 3618 distillers grain dried 10% value",
            report_date=report_date,
            source_file=source_file,
        )
    )
    return rows


class AMS3618Provider(BaseMarketProvider):
    source = "ams_3618"
    provider_parse_version = "v2"
    local_patterns = (
        "USDA_bioenergy_reports/*",
        "USDA_bioenergy_reports/**/*",
        "ams_3618_pdfs/*",
        "ams_3618_pdfs/**/*",
    )
    landing_page_url = "https://mymarketnews.ams.usda.gov/viewReport/3618"
    report_token = "/3618/"
    stable_name_prefix = "ams_3618"
    local_dir_name = "USDA_bioenergy_reports"

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        del ticker_root
        rows: List[Dict[str, Any]] = []
        for entry in raw_entries:
            report_ts = self._date_from_value(entry.get("report_date"))
            local_path = Path(str(entry.get("local_path") or "")).expanduser()
            if local_path.suffix.lower() != ".pdf" or not local_path.exists():
                continue
            text = _safe_pdf_text(local_path)
            if not text:
                continue
            rows.extend(parse_ams_3618_pdf_text(text, fallback_date=report_ts, source_file=local_path.name))
        self._record_parse_debug(cache_root, raw_entries, rows)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
