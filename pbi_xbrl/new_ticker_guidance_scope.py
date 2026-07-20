"""Ticker-neutral guidance scope normalization and current-row selection."""
from __future__ import annotations

import re
from dataclasses import dataclass
import hashlib
import json
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
    "real estate activity": "real_estate_activity",
    "tariffs": "tariffs",
}

_VISIBLE_GUIDANCE_METRICS = frozenset(
    {
        "revenue",
        "operating_margin",
        "adjusted_eps",
        "capital_expenditures",
        "diluted_shares",
        "share_repurchases",
        "real_estate_activity",
    }
)
_VISIBLE_GUIDANCE_UNITS = frozenset({"%", "$m", "$/share", "shares_m", "stores"})
_VISIBLE_EXPECTED_UNITS = {
    "revenue": "%",
    "operating_margin": "%",
    "adjusted_eps": "$/share",
    "capital_expenditures": "$m",
    "diluted_shares": "shares_m",
    "share_repurchases": "$m",
    "real_estate_activity": "stores",
}
_KNOWN_DEFERRED_METRICS = frozenset(set(_METRIC_ALIASES.values()) - _VISIBLE_GUIDANCE_METRICS)
_CURRENT_PRIMARY_SLOTS = (
    ("revenue", "FY"),
    ("revenue", "Q"),
    ("operating_margin", "FY"),
    ("operating_margin", "Q"),
    ("adjusted_eps", "FY"),
    ("adjusted_eps", "Q"),
    ("real_estate_activity", "FY"),
)
_CURRENT_SECONDARY_SLOTS = (
    ("capital_expenditures", "FY"),
    ("diluted_shares", "Q"),
    ("diluted_shares", "FY"),
    ("real_estate_activity", "Q"),
    ("share_repurchases", "Q"),
    ("share_repurchases", "FY"),
)
_HISTORICAL_METRIC_ORDER = (
    "revenue",
    "operating_margin",
    "adjusted_eps",
    "capital_expenditures",
    "diluted_shares",
    "real_estate_activity",
    "share_repurchases",
)


class GuidanceProjectionError(ValueError):
    """Fail-closed visible-guidance selection error."""

    def __init__(self, rule_id: str, message: str, **context: Any) -> None:
        self.rule_id = rule_id
        self.context = dict(context)
        detail = ", ".join(f"{key}={value!r}" for key, value in sorted(self.context.items()))
        super().__init__(f"{rule_id}: {message}" + (f" ({detail})" if detail else ""))


@dataclass(frozen=True)
class ResolvedGuidanceDisposition:
    row_key: str
    canonical_metric: str
    metric_display: str
    horizon: str
    unit: str
    role: str
    priority: int
    value: Any
    stated_period: str
    publication_date: str
    source_review_state: str
    display_state: str
    evidence_key: str
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]
    selection_reason: str
    conflict_disposition: str

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_key": self.row_key,
            "canonical_metric": self.canonical_metric,
            "metric_display": self.metric_display,
            "horizon": self.horizon,
            "unit": self.unit,
            "role": self.role,
            "priority": self.priority,
            "value": self.value,
            "stated_period": self.stated_period,
            "publication_date": self.publication_date,
            "source_review_state": self.source_review_state,
            "display_state": self.display_state,
            "evidence_key": self.evidence_key,
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_ref": self.source_ref,
            "selection_reason": self.selection_reason,
            "conflict_disposition": self.conflict_disposition,
        }


@dataclass(frozen=True)
class ValuationGuidanceProjection:
    current_primary_rows: tuple[ResolvedGuidanceDisposition, ...]
    current_secondary_rows: tuple[ResolvedGuidanceDisposition, ...]
    historical_rows: tuple[ResolvedGuidanceDisposition, ...]
    selection_audit: tuple[dict[str, Any], ...]
    projection_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_primary_rows": [row.to_dict() for row in self.current_primary_rows],
            "current_secondary_rows": [row.to_dict() for row in self.current_secondary_rows],
            "historical_rows": [row.to_dict() for row in self.historical_rows],
            "selection_audit": [dict(row) for row in self.selection_audit],
            "projection_digest": self.projection_digest,
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


def build_valuation_guidance_projection(
    rows: Sequence[Mapping[str, Any]],
    *,
    profile_pack_ids: set[str] | frozenset[str] | None = None,
) -> ValuationGuidanceProjection:
    """Resolve the bounded Valuation guidance projection without mutating source data."""

    packs = frozenset(str(value) for value in (profile_pack_ids or set()) if str(value))
    superseded_keys = {
        str(key)
        for row in rows
        if isinstance(row, Mapping)
        for key in row.get("supersedes_evidence_keys") or []
        if str(key)
    }
    candidates: list[tuple[int, Mapping[str, Any], dict[str, Any]]] = []
    audit: list[dict[str, Any]] = []
    seen_priorities: dict[str, dict[int, str]] = {}
    for source_index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise GuidanceProjectionError(
                "guidance_projection_row_invalid",
                "A visible guidance candidate must be an object.",
                source_index=source_index,
            )
        role = str(row.get("display_role") or "")
        if role not in {"current_primary", "current_secondary", "history"}:
            continue
        canonical_metric = _strict_visible_metric(_field_text(row.get("metric")), row=row, source_index=source_index)
        if canonical_metric in _KNOWN_DEFERRED_METRICS:
            audit.append(
                _guidance_audit_row(
                    row,
                    source_index=source_index,
                    disposition="deferred_metric_not_in_valuation_slots",
                    canonical_metric=canonical_metric,
                )
            )
            continue
        if canonical_metric == "real_estate_activity" and packs and "retail_operating_pack" not in packs:
            audit.append(
                _guidance_audit_row(
                    row,
                    source_index=source_index,
                    disposition="profile_slot_inactive",
                    canonical_metric=canonical_metric,
                )
            )
            continue
        normalized = _validated_visible_guidance_candidate(
            row,
            source_index=source_index,
            canonical_metric=canonical_metric,
        )
        priority = normalized["source_priority"]
        if role in CURRENT_GUIDANCE_ROLES:
            prior = seen_priorities.setdefault(role, {}).get(priority)
            if prior is not None and prior != normalized["evidence_key"]:
                raise GuidanceProjectionError(
                    "duplicate_guidance_display_priority",
                    "Visible current-guidance priorities must be unique within one display role.",
                    role=role,
                    priority=priority,
                    first_evidence_key=prior,
                    conflicting_evidence_key=normalized["evidence_key"],
                )
            seen_priorities[role][priority] = normalized["evidence_key"]
        evidence_key = normalized["evidence_key"]
        if (
            evidence_key in superseded_keys
            or str(row.get("superseded_by_evidence_key") or "")
            or str(row.get("update_stage") or "") == "withdrawn"
        ):
            audit.append(
                _guidance_audit_row(
                    row,
                    source_index=source_index,
                    disposition="superseded_or_withdrawn",
                    canonical_metric=canonical_metric,
                )
            )
            continue
        candidates.append((source_index, row, normalized))

    primary, primary_audit = _select_current_guidance_slots(
        candidates,
        role="current_primary",
        slots=_CURRENT_PRIMARY_SLOTS,
    )
    secondary, secondary_audit = _select_current_guidance_slots(
        candidates,
        role="current_secondary",
        slots=_CURRENT_SECONDARY_SLOTS,
    )
    history, history_audit = _select_historical_guidance(candidates)
    audit.extend(primary_audit)
    audit.extend(secondary_audit)
    audit.extend(history_audit)
    audit.sort(
        key=lambda row: (
            str(row.get("role") or ""),
            str(row.get("canonical_metric") or ""),
            str(row.get("horizon") or ""),
            str(row.get("publication_date") or ""),
            str(row.get("evidence_key") or ""),
            str(row.get("disposition") or ""),
        )
    )
    payload = {
        "current_primary_rows": [row.to_dict() for row in primary],
        "current_secondary_rows": [row.to_dict() for row in secondary],
        "historical_rows": [row.to_dict() for row in history],
        "selection_audit": audit,
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return ValuationGuidanceProjection(
        current_primary_rows=tuple(primary),
        current_secondary_rows=tuple(secondary),
        historical_rows=tuple(history),
        selection_audit=tuple(audit),
        projection_digest=digest,
    )


def _select_current_guidance_slots(
    candidates: Sequence[tuple[int, Mapping[str, Any], Mapping[str, Any]]],
    *,
    role: str,
    slots: Sequence[tuple[str, str]],
) -> tuple[list[ResolvedGuidanceDisposition], list[dict[str, Any]]]:
    selected: list[ResolvedGuidanceDisposition] = []
    audit: list[dict[str, Any]] = []
    used_keys: set[str] = set()
    for priority, (metric, horizon_type) in enumerate(slots, start=1):
        matches = [
            (source_index, row, normalized)
            for source_index, row, normalized in candidates
            if normalized["role"] == role
            and normalized["canonical_metric"] == metric
            and normalized["horizon_type"] == horizon_type
        ]
        if not matches:
            audit.append(
                {
                    "role": role,
                    "canonical_metric": metric,
                    "horizon_type": horizon_type,
                    "disposition": "slot_unavailable",
                }
            )
            continue
        latest_publication = max(str(normalized["publication_date"]) for _, _, normalized in matches)
        latest = [item for item in matches if str(item[2]["publication_date"]) == latest_publication]
        if len(latest) != 1:
            raise GuidanceProjectionError(
                "guidance_projection_conflict",
                "More than one accepted current guidance record can win the same visible slot.",
                role=role,
                canonical_metric=metric,
                horizon_type=horizon_type,
                publication_date=latest_publication,
                evidence_keys=sorted(str(item[2]["evidence_key"]) for item in latest),
            )
        _source_index, _row, normalized = latest[0]
        selected.append(
            _resolved_guidance_disposition(
                normalized,
                priority=priority,
                selection_reason="latest accepted current record for exact metric and horizon role",
            )
        )
        used_keys.add(str(normalized["evidence_key"]))
        for source_index, row, excluded in matches:
            if str(excluded["evidence_key"]) in used_keys:
                continue
            audit.append(
                _guidance_audit_row(
                    row,
                    source_index=source_index,
                    disposition="older_current_publication",
                    canonical_metric=str(excluded["canonical_metric"]),
                    normalized=excluded,
                )
            )
    for source_index, row, normalized in candidates:
        if normalized["role"] == role and str(normalized["evidence_key"]) not in used_keys:
            if not any(entry.get("evidence_key") == normalized["evidence_key"] for entry in audit):
                audit.append(
                    _guidance_audit_row(
                        row,
                        source_index=source_index,
                        disposition="no_declared_visible_slot",
                        canonical_metric=str(normalized["canonical_metric"]),
                        normalized=normalized,
                    )
                )
    return selected, audit


def _select_historical_guidance(
    candidates: Sequence[tuple[int, Mapping[str, Any], Mapping[str, Any]]],
) -> tuple[list[ResolvedGuidanceDisposition], list[dict[str, Any]]]:
    selected: list[ResolvedGuidanceDisposition] = []
    audit: list[dict[str, Any]] = []
    used_keys: set[str] = set()
    for priority, metric in enumerate(_HISTORICAL_METRIC_ORDER, start=1):
        matches = [
            item
            for item in candidates
            if item[2]["role"] == "history"
            and item[2]["canonical_metric"] == metric
            and item[2]["horizon_type"] == "FY"
        ]
        if not matches:
            audit.append(
                {
                    "role": "history",
                    "canonical_metric": metric,
                    "horizon_type": "FY",
                    "disposition": "slot_unavailable",
                }
            )
            continue
        latest_year = max(int(item[2]["fiscal_year"]) for item in matches)
        same_year = [item for item in matches if int(item[2]["fiscal_year"]) == latest_year]
        latest_publication = max(str(item[2]["publication_date"]) for item in same_year)
        winners = [item for item in same_year if str(item[2]["publication_date"]) == latest_publication]
        if len(winners) != 1:
            raise GuidanceProjectionError(
                "guidance_projection_conflict",
                "More than one accepted historical guidance record can win the same visible slot.",
                role="history",
                canonical_metric=metric,
                fiscal_year=latest_year,
                publication_date=latest_publication,
                evidence_keys=sorted(str(item[2]["evidence_key"]) for item in winners),
            )
        _source_index, _row, normalized = winners[0]
        selected.append(
            _resolved_guidance_disposition(
                normalized,
                priority=priority,
                selection_reason="latest accepted annual historical record for canonical metric",
            )
        )
        used_keys.add(str(normalized["evidence_key"]))
        for source_index, row, excluded in matches:
            if str(excluded["evidence_key"]) in used_keys:
                continue
            audit.append(
                _guidance_audit_row(
                    row,
                    source_index=source_index,
                    disposition="older_historical_record",
                    canonical_metric=str(excluded["canonical_metric"]),
                    normalized=excluded,
                )
            )
    return selected, audit


def _validated_visible_guidance_candidate(
    row: Mapping[str, Any],
    *,
    source_index: int,
    canonical_metric: str,
) -> dict[str, Any]:
    evidence_key = str(row.get("evidence_key") or "")
    context = {"source_index": source_index, "evidence_key": evidence_key}
    for field_name in ("metric", "value", "horizon"):
        field = row.get(field_name)
        if not isinstance(field, Mapping) or str(field.get("status") or "") != "populated" or field.get("value") in (None, ""):
            raise GuidanceProjectionError(
                "guidance_companion_field_invalid",
                f"Visible guidance requires a populated {field_name} field.",
                field=field_name,
                **context,
            )
    horizon_type, fiscal_year, fiscal_quarter = _canonical_horizon(_field_text(row.get("horizon")))
    if horizon_type not in {"FY", "Q"} or fiscal_year is None or (horizon_type == "Q" and fiscal_quarter is None):
        raise GuidanceProjectionError(
            "unknown_visible_guidance_horizon",
            "Visible guidance requires an exact fiscal-year or fiscal-quarter horizon.",
            raw_horizon=_field_text(row.get("horizon")),
            **context,
        )
    horizon = f"FY{fiscal_year}" if horizon_type == "FY" else f"{fiscal_year}-Q{fiscal_quarter}"
    unit = _strict_visible_unit(_field_unit(row.get("value")), canonical_metric=canonical_metric, **context)
    expected_unit = _VISIBLE_EXPECTED_UNITS[canonical_metric]
    if unit != expected_unit:
        raise GuidanceProjectionError(
            "incompatible_visible_guidance_unit",
            "Guidance unit is incompatible with the canonical metric.",
            canonical_metric=canonical_metric,
            canonical_unit=unit,
            expected_unit=expected_unit,
            **context,
        )
    publication_date = str(row.get("publication_date") or "")
    if not _valid_publication_date(publication_date):
        raise GuidanceProjectionError(
            "guidance_publication_date_invalid",
            "Visible guidance requires an exact publication date.",
            publication_date=publication_date,
            **context,
        )
    stated_period = _canonical_reporting_period(str(row.get("stated_in_period") or ""))
    if not re.fullmatch(r"20\d{2}-Q[1-4]", stated_period):
        raise GuidanceProjectionError(
            "guidance_stated_period_invalid",
            "Visible guidance requires an exact stated-in fiscal quarter.",
            raw_stated_period=str(row.get("stated_in_period") or ""),
            **context,
        )
    review_state = str(row.get("review_state") or "")
    if review_state != "accepted":
        raise GuidanceProjectionError(
            "guidance_source_status_rejected",
            "Only source-reviewed accepted guidance may enter the visible projection.",
            source_review_state=review_state,
            **context,
        )
    if str(row.get("update_stage") or "") not in ACTIVE_UPDATE_STAGES:
        raise GuidanceProjectionError(
            "guidance_update_stage_rejected",
            "Visible guidance has an unsupported update stage.",
            update_stage=str(row.get("update_stage") or ""),
            **context,
        )
    evidence_refs = tuple(sorted({str(value) for value in row.get("evidence_refs") or [] if str(value)}))
    source_refs = tuple(
        sorted(
            {
                str(value)
                for value in [row.get("source_ref"), *(row.get("evidence_refs") or [])]
                if str(value)
            }
        )
    )
    if not evidence_key or not evidence_refs or not source_refs:
        raise GuidanceProjectionError(
            "guidance_evidence_missing",
            "Visible guidance requires an evidence key and complete source lineage.",
            **context,
        )
    priority = row.get("display_priority")
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise GuidanceProjectionError(
            "guidance_display_priority_invalid",
            "Visible guidance display priority must be an integer.",
            raw_priority=priority,
            **context,
        )
    return {
        "canonical_metric": canonical_metric,
        "metric_display": _field_text(row.get("metric")),
        "horizon_type": horizon_type,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "horizon": horizon,
        "unit": unit,
        "role": str(row.get("display_role") or ""),
        "source_priority": priority,
        "value": _field_text(row.get("value")),
        "stated_period": stated_period,
        "publication_date": publication_date,
        "source_review_state": review_state,
        "display_state": f"{str(row.get('display_role') or '')} / {review_state}",
        "evidence_key": evidence_key,
        "evidence_refs": evidence_refs,
        "source_refs": source_refs,
    }


def _resolved_guidance_disposition(
    normalized: Mapping[str, Any],
    *,
    priority: int,
    selection_reason: str,
) -> ResolvedGuidanceDisposition:
    row_key = "|".join(
        (
            str(normalized["role"]),
            str(priority),
            str(normalized["canonical_metric"]),
            str(normalized["horizon"]),
            str(normalized["evidence_key"]),
        )
    )
    return ResolvedGuidanceDisposition(
        row_key=row_key,
        canonical_metric=str(normalized["canonical_metric"]),
        metric_display=str(normalized["metric_display"]),
        horizon=str(normalized["horizon"]),
        unit=str(normalized["unit"]),
        role=str(normalized["role"]),
        priority=priority,
        value=normalized["value"],
        stated_period=str(normalized["stated_period"]),
        publication_date=str(normalized["publication_date"]),
        source_review_state=str(normalized["source_review_state"]),
        display_state=str(normalized["display_state"]),
        evidence_key=str(normalized["evidence_key"]),
        evidence_refs=tuple(str(value) for value in normalized["evidence_refs"]),
        source_refs=tuple(str(value) for value in normalized["source_refs"]),
        selection_reason=selection_reason,
        conflict_disposition="unique_winner",
    )


def _guidance_audit_row(
    row: Mapping[str, Any],
    *,
    source_index: int,
    disposition: str,
    canonical_metric: str,
    normalized: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "role": str((normalized or {}).get("role") or row.get("display_role") or ""),
        "canonical_metric": canonical_metric,
        "horizon": str((normalized or {}).get("horizon") or _field_text(row.get("horizon"))),
        "publication_date": str((normalized or {}).get("publication_date") or row.get("publication_date") or ""),
        "evidence_key": str((normalized or {}).get("evidence_key") or row.get("evidence_key") or ""),
        "source_ref": str(row.get("source_ref") or ""),
        "disposition": disposition,
    }


def _strict_visible_metric(value: str, *, row: Mapping[str, Any], source_index: int) -> str:
    canonical = _canonical_metric(value)
    if canonical in _VISIBLE_GUIDANCE_METRICS or canonical in _KNOWN_DEFERRED_METRICS:
        return canonical
    raise GuidanceProjectionError(
        "unknown_visible_guidance_metric",
        "Visible guidance metric alias is not in the bounded vocabulary.",
        raw_metric=value,
        display_role=str(row.get("display_role") or ""),
        source_index=source_index,
        evidence_key=str(row.get("evidence_key") or ""),
        accepted_metrics=sorted(_VISIBLE_GUIDANCE_METRICS),
    )


def _strict_visible_unit(value: str, *, canonical_metric: str, **context: Any) -> str:
    raw = value.strip()
    if not raw and canonical_metric == "real_estate_activity":
        return "stores"
    normalized = re.sub(r"[\s_-]+", " ", raw.casefold()).strip()
    aliases = {
        "%": "%",
        "percent": "%",
        "percentage": "%",
        "$m": "$m",
        "usd millions": "$m",
        "usd million": "$m",
        "million dollars": "$m",
        "$/share": "$/share",
        "usd per share": "$/share",
        "m shares": "shares_m",
        "shares m": "shares_m",
        "million shares": "shares_m",
        "store": "stores",
        "stores": "stores",
    }
    canonical = aliases.get(normalized)
    if canonical not in _VISIBLE_GUIDANCE_UNITS:
        raise GuidanceProjectionError(
            "unknown_visible_guidance_unit",
            "Visible guidance unit alias is not in the bounded vocabulary.",
            raw_unit=value,
            canonical_metric=canonical_metric,
            accepted_units=sorted(_VISIBLE_GUIDANCE_UNITS),
            **context,
        )
    return canonical


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
