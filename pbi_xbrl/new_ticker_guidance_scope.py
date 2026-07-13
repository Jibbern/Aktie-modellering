"""Ticker-neutral guidance scope normalization and current-row selection."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


CURRENT_GUIDANCE_ROLES = frozenset({"current_primary", "current_secondary"})
ACTIVE_UPDATE_STAGES = frozenset({"initial", "update", "reaffirmed", "raised", "lowered"})

_METRIC_ALIASES = {
    "revenue": "revenue",
    "net sales": "revenue",
    "sales": "revenue",
    "revenue growth": "revenue_growth",
    "net sales growth": "revenue_growth",
    "sales growth": "revenue_growth",
    "operating margin": "operating_margin",
    "adjusted operating margin": "adjusted_operating_margin",
    "gross margin": "gross_margin",
    "eps": "eps",
    "earnings per share": "eps",
    "adjusted eps": "adjusted_eps",
    "adjusted earnings per share": "adjusted_eps",
    "capital expenditures": "capital_expenditures",
    "capital expenditure": "capital_expenditures",
    "capex": "capital_expenditures",
    "diluted shares": "diluted_shares",
    "diluted share count": "diluted_shares",
    "weighted average diluted shares": "diluted_shares",
    "share repurchases": "share_repurchases",
    "share repurchase": "share_repurchases",
    "buybacks": "share_repurchases",
}


@dataclass(frozen=True)
class GuidanceScope:
    metric: str
    horizon_type: str
    fiscal_year: int | None
    fiscal_quarter: int | None
    unit: str
    source_reporting_period: str

    @property
    def horizon(self) -> str:
        if self.horizon_type == "FY" and self.fiscal_year:
            return f"FY{self.fiscal_year}"
        if self.horizon_type == "Q" and self.fiscal_year and self.fiscal_quarter:
            return f"{self.fiscal_year}-Q{self.fiscal_quarter}"
        return "unspecified"

    @property
    def scope_key(self) -> tuple[str, str, int | None, int | None, str]:
        """Business scope for supersession; reporting period is ordering context."""

        return (self.metric, self.horizon_type, self.fiscal_year, self.fiscal_quarter, self.unit)

    @property
    def context_key(self) -> tuple[str, str, int | None, int | None, str, str]:
        return (*self.scope_key, self.source_reporting_period)


def normalize_guidance_scope(item: Mapping[str, Any]) -> GuidanceScope:
    metric = _canonical_metric(_field_text(item.get("metric")))
    horizon_type, fiscal_year, fiscal_quarter = _canonical_horizon(_field_text(item.get("horizon")))
    unit = _canonical_unit(_field_unit(item.get("value")) or _field_unit(item.get("horizon")))
    reporting_period = _canonical_reporting_period(str(item.get("stated_in_period") or ""))
    return GuidanceScope(
        metric=metric,
        horizon_type=horizon_type,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        unit=unit,
        source_reporting_period=reporting_period,
    )


def guidance_scope_key(item: Mapping[str, Any]) -> tuple[str, str, int | None, int | None, str]:
    return normalize_guidance_scope(item).scope_key


def guidance_scope_label(item: Mapping[str, Any]) -> str:
    scope = normalize_guidance_scope(item)
    return "|".join((scope.metric, scope.horizon, scope.unit or "unit_unspecified"))


def current_guidance_indexes(rows: Sequence[Mapping[str, Any]]) -> set[int]:
    """Return current-role rows that are truly latest and not superseded."""

    superseded = {
        str(key)
        for row in rows
        for key in (row.get("supersedes_evidence_keys") or [])
        if isinstance(key, str) and key
    }
    grouped: dict[tuple[str, str, int | None, int | None, str], list[tuple[int, Mapping[str, Any]]]] = {}
    for index, row in enumerate(rows):
        grouped.setdefault(guidance_scope_key(row), []).append((index, row))
    selected: set[int] = set()
    for scoped_rows in grouped.values():
        candidates = [
            (index, row)
            for index, row in scoped_rows
            if _valid_publication_date(str(row.get("publication_date") or ""))
            and str(row.get("update_stage") or "") in ACTIVE_UPDATE_STAGES
            and str(row.get("evidence_key") or "") not in superseded
            and not str(row.get("superseded_by_evidence_key") or "")
        ]
        if not candidates:
            continue
        latest_publication = max(str(row.get("publication_date") or "") for _index, row in candidates)
        for index, row in candidates:
            if (
                str(row.get("publication_date") or "") == latest_publication
                and str(row.get("display_role") or "") in CURRENT_GUIDANCE_ROLES
            ):
                selected.add(index)
    return selected


def latest_scope_publications(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str, int | None, int | None, str], str]:
    result: dict[tuple[str, str, int | None, int | None, str], str] = {}
    for row in rows:
        publication = str(row.get("publication_date") or "")
        if not _valid_publication_date(publication) or str(row.get("update_stage") or "") == "withdrawn":
            continue
        key = guidance_scope_key(row)
        result[key] = max(result.get(key, ""), publication)
    return result


def _canonical_metric(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip().casefold())
    normalized = re.sub(r"\badj\.?\b", "adjusted", normalized)
    return _METRIC_ALIASES.get(normalized, re.sub(r"[^a-z0-9]+", "_", normalized).strip("_") or "unspecified")


def _canonical_horizon(value: str) -> tuple[str, int | None, int | None]:
    normalized = re.sub(r"[\s_/.-]+", " ", value.strip().casefold())
    year_match = re.search(r"\b(20\d{2})\b", normalized)
    year = int(year_match.group(1)) if year_match else None
    quarter_match = re.search(r"\bq([1-4])\b|\bquarter\s*([1-4])\b", normalized)
    quarter_text = next((group for group in quarter_match.groups() if group), "") if quarter_match else ""
    if year and quarter_text:
        return "Q", year, int(quarter_text)
    if year and (
        re.search(r"\bfy\b", normalized)
        or re.search(r"\bfiscal year\b", normalized)
        or re.search(r"\bfull year\b", normalized)
        or re.search(r"\byear\b", normalized)
        or re.fullmatch(r"20\d{2}", normalized)
    ):
        return "FY", year, None
    compact = value.strip().upper()
    compact_match = re.fullmatch(r"FY(20\d{2})", compact)
    if compact_match:
        return "FY", int(compact_match.group(1)), None
    return "OTHER", year, None


def _canonical_unit(value: str) -> str:
    normalized = re.sub(r"\s+", "", value.strip().casefold())
    aliases = {
        "$m": "$m",
        "usd_millions": "$m",
        "usdmillions": "$m",
        "%": "%",
        "percent": "%",
        "percentage_points": "pp",
        "pp": "pp",
        "$": "$",
        "$/share": "$/share",
        "shares_m": "shares_m",
        "million_shares": "shares_m",
    }
    return aliases.get(normalized, normalized)


def _canonical_reporting_period(value: str) -> str:
    normalized = value.strip().upper().replace("_", "-").replace(" ", "-")
    match = re.fullmatch(r"(20\d{2})-?Q([1-4])", normalized)
    return f"{match.group(1)}-Q{match.group(2)}" if match else normalized


def _field_text(value: Any) -> str:
    return str(value.get("value") or "") if isinstance(value, Mapping) else str(value or "")


def _field_unit(value: Any) -> str:
    return str(value.get("unit") or "") if isinstance(value, Mapping) else ""


def _valid_publication_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))
