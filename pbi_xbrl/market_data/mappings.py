"""Provider label normalization rules for market-data inputs.

Workbook logic expects a consistent small vocabulary for regions, commodities,
and product names. These regex mappings collapse the noisier provider-specific
labels into that shared vocabulary before quarterly aggregation or overlay text
selection happens downstream.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Set


REGION_PATTERNS = [
    (r"^Iowa\s+East", "iowa_east"),
    (r"^Iowa\s+West", "iowa_west"),
    (r"^Iowa", "iowa"),
    (r"^Nebraska", "nebraska"),
    (r"^Illinois", "illinois"),
    (r"^Indiana", "indiana"),
    (r"^Minnesota", "minnesota"),
    (r"^Kansas", "kansas"),
    (r"^South Dakota", "south_dakota"),
    (r"^North Dakota", "north_dakota"),
    (r"^Wisconsin", "wisconsin"),
    (r"^Michigan", "michigan"),
    (r"^Missouri", "missouri"),
    (r"^Ohio", "ohio"),
    (r"^Texas", "texas"),
    (r"^California", "california"),
    (r"^Chicago Beyond", "chicago_beyond"),
    (r"^Chicago", "chicago"),
    (r"^Pacific Northwest", "pacific_northwest"),
    (r"^New Orleans", "new_orleans"),
    (r"^Lethbridge", "lethbridge_ab"),
]


def normalize_region(raw: str) -> str:
    text = re.sub(r"\s+", " ", str(raw or "").strip())
    if not text:
        return ""
    for pattern, value in REGION_PATTERNS:
        if re.match(pattern, text, re.I):
            return value
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def region_tags(region_txt: Any) -> Set[str]:
    norm = str(region_txt or "").strip().lower()
    if not norm:
        return set()
    tags = {norm}
    for part in re.split(r"[^a-z0-9]+", norm):
        if part:
            tags.add(part)
    midwest_terms = {
        "illinois",
        "indiana",
        "iowa",
        "kansas",
        "michigan",
        "minnesota",
        "missouri",
        "nebraska",
        "ohio",
        "south_dakota",
        "wisconsin",
        "gpre_core",
    }
    if tags & midwest_terms:
        tags.add("midwest")
    return tags


def series_meta_from_key(series_key: Any, source_type: str) -> Optional[Dict[str, str]]:
    key = str(series_key or "").strip().lower()
    if not key or key in {
        "week_end",
        "report_date",
        "quarter",
        "source_pdf",
        "weeks",
        "gpre_weight_coverage",
        "gpre_weight_coverage_core",
        "gpre_weight_coverage_cash",
        "gpre_weight_included",
        "gas_cost_gal",
    }:
        return None
    if key.startswith(("crush_", "board_crush_", "gpre_underlying_")) or key in {"cbot_corn_cents"}:
        return None

    def _meta(market_family: str, instrument: str, region: str, unit: str, tenor: str = "") -> Dict[str, str]:
        return {
            "market_family": market_family,
            "instrument": instrument,
            "location": region,
            "region": region,
            "tenor": tenor,
            "unit": unit,
        }

    m_corn_fut = re.match(r"cbot_corn(?:_([a-z]{3}\d{2}))?_usd(?:_per_bu)?$", key)
    if m_corn_fut:
        tenor = (m_corn_fut.group(1) or "front").lower()
        fam = "corn_futures" if tenor != "front" else "corn_price"
        instr = "Corn futures" if fam == "corn_futures" else "Corn price"
        return _meta(fam, instr, "cbot", "$/bushel", tenor)

    if key == "nymex_gas":
        return _meta("natural_gas_price", "Natural gas price", "nymex", "$/MMBtu", "front")
    m_gas = re.match(r"nymex_gas_([a-z]{3}\d{2})_usd$", key)
    if m_gas:
        tenor = (m_gas.group(1) or "").lower()
        return _meta("natural_gas_futures", "Natural gas futures", "nymex", "$/MMBtu", tenor)

    if key.startswith("corn_cash_"):
        return _meta("corn_price", "Corn cash price", key.replace("corn_cash_", "", 1), "$/bushel")
    if source_type.startswith("ams_3617") and key.startswith("corn_"):
        return _meta("corn_price", "Corn cash price", key.replace("corn_", "", 1), "$/bushel")
    if key.startswith("ethanol_"):
        return _meta("ethanol_price", "Ethanol price", key.replace("ethanol_", "", 1), "$/gal")
    if key.startswith("ddgs_"):
        return _meta("ddgs_price", "DDGS price", key.replace("ddgs_", "", 1), "$/ton")
    if key.startswith("corn_oil_"):
        return _meta("renewable_corn_oil_price", "Renewable corn oil price", key.replace("corn_oil_", "", 1), "c/lb")
    return None


def convert_market_price_value(value: Any, unit_from: str, unit_to: str) -> tuple[Optional[float], bool]:
    try:
        val = float(value)
    except Exception:
        return None, False
    from_u = str(unit_from or "").strip()
    to_u = str(unit_to or "").strip()
    if not to_u or from_u == to_u:
        return val, False
    if from_u == "$/ton" and to_u == "$/lb":
        return val / 2000.0, True
    if from_u == "c/lb" and to_u == "$/lb":
        return val / 100.0, True
    if from_u == "c/lb" and to_u == "c/lb":
        return val, False
    return None, False
