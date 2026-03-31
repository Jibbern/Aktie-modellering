"""AMS 3617 provider for ethanol and co-product market observations.

This provider turns the restored PDF history in `sec_cache/market_data/raw` into
normalized observations and quarterly summaries that feed the GPRE economics
overlay.
"""
from __future__ import annotations

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
) -> Dict[str, Any]:
    return {
        "observation_date": report_date,
        "quarter": quarter_end_from_date(report_date),
        "aggregation_level": "observation",
        "publication_date": report_date,
        "source": "ams_3617",
        "report_type": "ams_3617_pdf",
        "source_type": "ams_3617_pdf",
        "market_family": "corn_price",
        "series_key": series_key,
        "instrument": "Corn cash price",
        "location": region,
        "region": region,
        "tenor": "",
        "price_value": float(price_value),
        "unit": "$/bushel",
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
            break
    return rows


class AMS3617Provider(BaseMarketProvider):
    source = "ams_3617"
    provider_parse_version = "v3"
    # New downloads live in the workbook-facing USDA folder, but we keep reading the
    # legacy provider-specific directory so older local restores continue to work.
    local_patterns = (
        "USDA_daily_data/*",
        "USDA_daily_data/**/*",
        "ams_3617_pdfs/*",
        "ams_3617_pdfs/**/*",
    )
    landing_page_url = "https://mymarketnews.ams.usda.gov/viewReport/3617"
    report_token = "/3617/"
    stable_name_prefix = "ams_3617"
    local_dir_name = "USDA_daily_data"

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        del cache_root, ticker_root
        q_start, q_end = self._quarter_bounds(as_of=self._today())
        rows: List[Dict[str, Any]] = []
        for entry in raw_entries:
            report_ts = self._date_from_value(entry.get("report_date"))
            if report_ts is None or not (q_start <= report_ts.date() <= q_end):
                continue
            local_path = Path(str(entry.get("local_path") or "")).expanduser()
            if local_path.suffix.lower() != ".pdf" or not local_path.exists():
                continue
            text = _safe_pdf_text(local_path)
            if not text:
                continue
            rows.extend(parse_ams_3617_pdf_text(text, fallback_date=report_ts, source_file=local_path.name))
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
