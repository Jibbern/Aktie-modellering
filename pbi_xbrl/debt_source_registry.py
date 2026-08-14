"""Declarative debt-evidence adapter routing for legacy workbook inputs.

The generic pipeline owns adapter selection and deterministic merging only.  Ticker-
specific source discovery, field mapping, and evidence semantics remain inside the
registered adapter.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from .anf_debt_source_adapter import (
    ANF_DEBT_EVIDENCE_ADAPTER_ID,
    build_anf_legacy_revolver_history,
)
from .company_profiles import get_company_profile
from .new_ticker_debt_scope import DebtResolutionError, canonical_debt_id


class DebtEvidenceRoutingError(ValueError):
    """Debt evidence cannot be routed without an unambiguous declared owner."""


@dataclass(frozen=True, order=True)
class DebtRevolverRowIdentity:
    """Semantic identity for one facility snapshot, independent of source rows."""

    reporting_date: str
    facility_id: str
    debt_concept: str
    basis: str
    scope: str

    @property
    def key(self) -> tuple[str, str, str, str, str]:
        return (
            self.reporting_date,
            self.facility_id,
            self.debt_concept,
            self.basis,
            self.scope,
        )

    @property
    def stable_id(self) -> str:
        return "|".join(self.key)


@dataclass(frozen=True)
class _DebtMetricSpec:
    value_column: str
    lineage_prefix: str


@dataclass(frozen=True)
class _DebtRowCandidate:
    row: Mapping[str, Any]
    origin: str
    identity: DebtRevolverRowIdentity


_DEBT_REVOLVER_METRICS: tuple[_DebtMetricSpec, ...] = (
    _DebtMetricSpec("revolver_commitment", "commitment"),
    _DebtMetricSpec("revolver_facility_size", "facility"),
    _DebtMetricSpec("revolver_drawn", "drawn"),
    _DebtMetricSpec("revolver_letters_of_credit", "lc"),
    _DebtMetricSpec("revolver_availability", "availability"),
    _DebtMetricSpec("revolver_utilization", "utilization"),
    _DebtMetricSpec("revolver_gross_capacity", "gross_capacity"),
    _DebtMetricSpec("revolver_minimum_excess_availability", "minimum_excess_availability"),
    _DebtMetricSpec("same_date_cash", "cash"),
    _DebtMetricSpec("same_date_liquidity", "liquidity"),
)

_DEBT_IDENTITY_COLUMNS = (
    "facility_id",
    "instrument_id",
    "economic_id",
    "debt_instrument_id",
)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return False
    try:
        result = pd.isna(value)
    except Exception:
        return False
    if result is pd.NA:
        return True
    if isinstance(result, bool) or getattr(result, "ndim", None) == 0:
        try:
            return bool(result)
        except (TypeError, ValueError):
            return False
    return False


def _scalar_text(value: Any) -> str:
    return "" if _is_missing(value) else str(value).strip()


def _canonical_component(value: Any, *, field: str, fallback: str) -> str:
    raw = _scalar_text(value) or fallback
    try:
        return canonical_debt_id(raw, field=field)
    except DebtResolutionError as exc:
        raise DebtEvidenceRoutingError(str(exc)) from exc


def _explicit_facility_id(row: Mapping[str, Any]) -> str:
    for column in _DEBT_IDENTITY_COLUMNS:
        value = _scalar_text(row.get(column))
        if value:
            return _canonical_component(value, field=column, fallback="")
    return ""


def _row_basis(row: Mapping[str, Any]) -> str:
    for column in ("debt_row_basis", "balance_basis", "basis", "principal_balance_type"):
        value = _scalar_text(row.get(column))
        if value:
            return _canonical_component(value, field=column, fallback="reported_or_resolved_balance")
    return "reported_or_resolved_balance"


def _row_scope(row: Mapping[str, Any]) -> str:
    for column in ("debt_row_scope", "scope", "debt_scope", "entity_scope"):
        value = _scalar_text(row.get(column))
        if value:
            return _canonical_component(value, field=column, fallback="consolidated")
    return "consolidated"


def canonical_debt_revolver_row_identity(
    row: Mapping[str, Any],
    *,
    fallback_facility_id: str = "legacy_primary_revolver",
) -> DebtRevolverRowIdentity:
    """Resolve the row's economic identity without using its source occurrence."""

    reporting_date = pd.to_datetime(row.get("quarter"), errors="coerce")
    if pd.isna(reporting_date):
        raise DebtEvidenceRoutingError("Debt revolver row has no valid reporting-date identity.")
    facility_id = _explicit_facility_id(row) or _canonical_component(
        fallback_facility_id,
        field="facility_id",
        fallback="legacy_primary_revolver",
    )
    return DebtRevolverRowIdentity(
        reporting_date=pd.Timestamp(reporting_date).normalize().strftime("%Y-%m-%d"),
        facility_id=facility_id,
        debt_concept="revolver_facility_snapshot",
        basis=_row_basis(row),
        scope=_row_scope(row),
    )


def _row_fingerprint(row: Mapping[str, Any]) -> str:
    payload = json.dumps(
        {str(key): value for key, value in row.items() if not str(key).startswith("__")},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _source_token(value: Any) -> str:
    return "_".join(part for part in "".join(
        char.lower() if char.isalnum() else " " for char in _scalar_text(value)
    ).split() if part)


def _source_rank(row: Mapping[str, Any], *, prefix: str | None = None) -> tuple[int, int]:
    classification = _source_token(
        row.get(f"{prefix}_evidence_classification") if prefix else row.get("evidence_classification")
    )
    source_type = _source_token(
        row.get(f"{prefix}_source_type") if prefix else row.get("source_type")
    )
    if not source_type and prefix:
        source_type = _source_token(row.get("source_type"))
    derived = int(
        "derived" in classification
        or "calculation" in classification
        or source_type in {"derived", "calculated", "calculation"}
    )
    if source_type == "xbrl":
        authority = 0
    elif source_type in {
        "table",
        "filing_table",
        "source_backed_fact",
        "direct_source",
        "10_k_debt_note",
        "10_q_debt_note",
    }:
        authority = 1
    elif source_type in {"text", "filing_text", "narrative"}:
        authority = 2
    elif derived:
        authority = 3
    elif source_type in {"", "missing", "not_applicable"}:
        authority = 4
    else:
        authority = 2
    return derived, authority


def _candidate_authority(candidate: _DebtRowCandidate, *, prefix: str | None = None) -> tuple[int, int, int]:
    # A declared profile adapter is the accepted source-native owner.  Within
    # one origin, direct facts outrank derived facts and the existing revolver
    # source hierarchy (XBRL, table, text, derived) remains authoritative.
    origin_rank = 0 if candidate.origin == "source_native" else 1
    derived_rank, source_rank = _source_rank(candidate.row, prefix=prefix)
    return origin_rank, derived_rank, source_rank


def _economic_value(value: Any, *, identity: DebtRevolverRowIdentity, metric: str) -> float | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise DebtEvidenceRoutingError(
            f"Debt value is not numeric for identity={identity.stable_id} metric={metric}: {value!r}"
        )
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        raise DebtEvidenceRoutingError(
            f"Debt value is not numeric for identity={identity.stable_id} metric={metric}: {value!r}"
        )
    return float(parsed)


def _metric_lineage_columns(prefix: str) -> tuple[str, ...]:
    return (
        f"{prefix}_source_type",
        f"{prefix}_source_ref",
        f"{prefix}_source_row_ref",
        f"{prefix}_evidence_classification",
        f"{prefix}_evidence_refs",
        f"{prefix}_snippet",
        f"{prefix}_derivation",
    )


def _candidate_lineage(candidate: _DebtRowCandidate) -> dict[str, Any]:
    row = candidate.row
    fields = (
        "source_document_id",
        "source_document_accession",
        "source_document_sha256",
        "source_ref",
        "source_row_ref",
        "evidence_key",
        "business_key",
        "source_type",
        "source_snippet",
        "source_backed_lineage_disposition",
    )
    evidence = {
        "origin": candidate.origin,
        "identity": candidate.identity.stable_id,
        **{
            field: candidate.row.get(field)
            for field in fields
            if not _is_missing(candidate.row.get(field))
        },
    }
    evidence["candidate_sha256"] = _row_fingerprint(row)
    return evidence


def _reconcile_candidate_group(candidates: Sequence[_DebtRowCandidate]) -> dict[str, Any]:
    identities = {candidate.identity for candidate in candidates}
    if len(identities) != 1:
        raise DebtEvidenceRoutingError("Internal debt reconciliation received mixed semantic identities.")
    identity = min(identities)
    ranked = sorted(
        candidates,
        key=lambda candidate: (_candidate_authority(candidate), _row_fingerprint(candidate.row)),
    )
    seed = min(
        ranked,
        key=lambda candidate: (_candidate_authority(candidate), _row_fingerprint(candidate.row)),
    )
    result = dict(seed.row)
    for candidate in ranked:
        for column, value in candidate.row.items():
            if (column not in result or _is_missing(result.get(column))) and not _is_missing(value):
                result[column] = value
    resolution = "selected" if len(candidates) == 1 else "corroborated"

    for spec in _DEBT_REVOLVER_METRICS:
        populated: list[tuple[_DebtRowCandidate, float]] = []
        for candidate in candidates:
            value = _economic_value(
                candidate.row.get(spec.value_column),
                identity=identity,
                metric=spec.value_column,
            )
            if value is not None:
                populated.append((candidate, value))
        if not populated:
            continue
        best_authority = min(
            _candidate_authority(candidate, prefix=spec.lineage_prefix)
            for candidate, _value in populated
        )
        winners = [
            (candidate, value)
            for candidate, value in populated
            if _candidate_authority(candidate, prefix=spec.lineage_prefix) == best_authority
        ]
        winner_values = {value for _candidate, value in winners}
        if len(winner_values) != 1:
            evidence = sorted(
                (_candidate_lineage(candidate) for candidate, _value in winners),
                key=lambda row: str(row["candidate_sha256"]),
            )
            raise DebtEvidenceRoutingError(
                "Conflicting same-authority debt facts for "
                f"identity={identity.stable_id} metric={spec.value_column} "
                f"values={sorted(winner_values)!r} evidence={json.dumps(evidence, sort_keys=True, default=str)}"
            )
        selected, selected_value = min(
            winners,
            key=lambda item: _row_fingerprint(item[0].row),
        )
        result[spec.value_column] = selected.row.get(spec.value_column)
        for column in _metric_lineage_columns(spec.lineage_prefix):
            if column in selected.row and not _is_missing(selected.row.get(column)):
                result[column] = selected.row.get(column)
        if any(value != selected_value for _candidate, value in populated):
            resolution = "authority_selected"

    lineage = sorted(
        (_candidate_lineage(candidate) for candidate in candidates),
        key=lambda row: (str(row.get("origin")), str(row.get("candidate_sha256"))),
    )
    result.update(
        {
            "quarter": pd.Timestamp(identity.reporting_date),
            "facility_id": identity.facility_id,
            "debt_row_concept": identity.debt_concept,
            "debt_row_basis": identity.basis,
            "debt_row_scope": identity.scope,
            "debt_row_identity": identity.stable_id,
            "debt_row_resolution": resolution,
            "debt_evidence_count": len(lineage),
            "debt_evidence_lineage": json.dumps(
                lineage,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
        }
    )
    return result


def reconcile_debt_revolver_rows(
    frame: pd.DataFrame | None,
    *,
    origin: str,
) -> pd.DataFrame:
    """Reconcile duplicate semantic rows without physical-order ownership."""

    if frame is None or frame.empty:
        return pd.DataFrame()
    if origin not in {"base", "source_native"}:
        raise DebtEvidenceRoutingError(f"Unsupported debt-row origin: {origin!r}")
    current = frame.copy()
    attrs = dict(frame.attrs)
    if "quarter" not in current.columns:
        raise DebtEvidenceRoutingError("Debt revolver history has no quarter identity.")
    current["quarter"] = pd.to_datetime(current["quarter"], errors="coerce").dt.normalize()
    if current["quarter"].isna().any():
        raise DebtEvidenceRoutingError("Debt revolver history contains an invalid quarter identity.")
    candidates = [
        _DebtRowCandidate(
            row=dict(row),
            origin=origin,
            identity=canonical_debt_revolver_row_identity(dict(row)),
        )
        for row in current.to_dict(orient="records")
    ]
    grouped: dict[DebtRevolverRowIdentity, list[_DebtRowCandidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.identity, []).append(candidate)
    result = pd.DataFrame(
        _reconcile_candidate_group(grouped[identity])
        for identity in sorted(grouped)
    ).reset_index(drop=True)
    result.attrs.update(attrs)
    return result


@dataclass(frozen=True)
class DebtEvidenceAdapter:
    adapter_id: str
    build_revolver_history: Callable[[Path], pd.DataFrame]


DEBT_EVIDENCE_ADAPTERS: tuple[DebtEvidenceAdapter, ...] = (
    DebtEvidenceAdapter(
        adapter_id=ANF_DEBT_EVIDENCE_ADAPTER_ID,
        build_revolver_history=build_anf_legacy_revolver_history,
    ),
)


def _adapter_registry(
    adapters: Iterable[DebtEvidenceAdapter] = DEBT_EVIDENCE_ADAPTERS,
) -> dict[str, DebtEvidenceAdapter]:
    registry: dict[str, DebtEvidenceAdapter] = {}
    for adapter in adapters:
        adapter_id = str(adapter.adapter_id or "").strip()
        if not adapter_id:
            raise DebtEvidenceRoutingError("Debt evidence adapters require a stable adapter ID.")
        if adapter_id in registry:
            raise DebtEvidenceRoutingError(f"Duplicate debt evidence adapter registration: {adapter_id}")
        registry[adapter_id] = adapter
    return registry


def _validate_source_native_history(frame: pd.DataFrame, *, adapter_id: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    required = {
        "quarter",
        "facility_id",
        "debt_evidence_adapter_id",
        "source_document_id",
        "source_document_sha256",
        "source_ref",
        "source_row_ref",
        "revolver_commitment",
        "revolver_facility_size",
        "revolver_drawn",
        "revolver_letters_of_credit",
        "revolver_availability",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise DebtEvidenceRoutingError(
            f"Debt evidence adapter {adapter_id} omitted required legacy columns: {missing}"
        )
    out = frame.copy()
    out["quarter"] = pd.to_datetime(out["quarter"], errors="coerce")
    if out["quarter"].isna().any():
        raise DebtEvidenceRoutingError(f"Debt evidence adapter {adapter_id} emitted an invalid quarter.")
    normalized_ids = out["debt_evidence_adapter_id"].astype(str).str.strip()
    if not normalized_ids.eq(adapter_id).all():
        raise DebtEvidenceRoutingError(
            f"Debt evidence adapter {adapter_id} emitted rows owned by another adapter."
        )
    sha = out["source_document_sha256"].astype(str).str.strip().str.lower()
    if not sha.str.fullmatch(r"[0-9a-f]{64}").all():
        raise DebtEvidenceRoutingError(
            f"Debt evidence adapter {adapter_id} emitted invalid document SHA-256 lineage."
        )
    for column in ("source_document_id", "source_ref", "source_row_ref"):
        if out[column].astype(str).str.strip().eq("").any():
            raise DebtEvidenceRoutingError(
                f"Debt evidence adapter {adapter_id} emitted empty {column} lineage."
            )
    attrs = dict(frame.attrs)
    out = reconcile_debt_revolver_rows(out, origin="source_native")
    out.attrs.update(attrs)
    return out


def resolve_profile_debt_revolver_history(
    *,
    ticker: str,
    cache_root: Path | None,
    adapters: Iterable[DebtEvidenceAdapter] = DEBT_EVIDENCE_ADAPTERS,
) -> pd.DataFrame:
    """Resolve one explicitly declared source adapter, or return no debt rows."""

    registry = _adapter_registry(adapters)
    profile = get_company_profile(ticker)
    adapter_id = str(profile.debt_evidence_adapter_id or "").strip()
    if not adapter_id or cache_root is None:
        return pd.DataFrame()
    adapter = registry.get(adapter_id)
    if adapter is None:
        raise DebtEvidenceRoutingError(
            f"Company profile {profile.ticker} declares unknown debt evidence adapter {adapter_id}."
        )
    frame = adapter.build_revolver_history(Path(cache_root))
    if not isinstance(frame, pd.DataFrame):
        raise DebtEvidenceRoutingError(
            f"Debt evidence adapter {adapter_id} did not return a DataFrame."
        )
    return _validate_source_native_history(frame, adapter_id=adapter_id)


def merge_source_native_revolver_history(
    base: pd.DataFrame | None,
    source_native: pd.DataFrame | None,
) -> pd.DataFrame:
    """Merge base and profile-owned rows by semantic facility identity.

    A unique typed overlay facility owns an otherwise untyped legacy primary-
    revolver row for the same period/basis/scope.  Explicitly identified other
    facilities remain separate.  Values are then reconciled per debt metric by
    source authority; physical DataFrame order never participates.
    """

    if (base is None or base.empty) and (source_native is None or source_native.empty):
        return pd.DataFrame()

    base_frame = pd.DataFrame() if base is None else base.copy()
    overlay_frame = pd.DataFrame() if source_native is None else source_native.copy()
    base_attrs = {} if base is None else dict(base.attrs)
    overlay_attrs = {} if source_native is None else dict(source_native.attrs)
    for label, frame in (("base", base_frame), ("source-native", overlay_frame)):
        if frame.empty:
            continue
        if "quarter" not in frame.columns:
            raise DebtEvidenceRoutingError(f"{label} revolver history has no quarter identity.")
        frame["quarter"] = pd.to_datetime(frame["quarter"], errors="coerce").dt.normalize()
        if frame["quarter"].isna().any():
            raise DebtEvidenceRoutingError(f"{label} revolver history contains an invalid quarter identity.")

    overlay_rows = overlay_frame.to_dict(orient="records") if not overlay_frame.empty else []
    overlay_identity_by_period: dict[tuple[str, str, str], set[str]] = {}
    overlay_candidates: list[_DebtRowCandidate] = []
    for row in overlay_rows:
        identity = canonical_debt_revolver_row_identity(row, fallback_facility_id="source_native_primary_revolver")
        overlay_identity_by_period.setdefault(
            (identity.reporting_date, identity.basis, identity.scope),
            set(),
        ).add(identity.facility_id)
        overlay_candidates.append(_DebtRowCandidate(row=row, origin="source_native", identity=identity))

    candidates: list[_DebtRowCandidate] = []
    for row in base_frame.to_dict(orient="records") if not base_frame.empty else []:
        provisional = canonical_debt_revolver_row_identity(row)
        explicit_facility = _explicit_facility_id(row)
        matching_overlay_ids = overlay_identity_by_period.get(
            (provisional.reporting_date, provisional.basis, provisional.scope),
            set(),
        )
        fallback_facility = (
            min(matching_overlay_ids)
            if not explicit_facility and len(matching_overlay_ids) == 1
            else "legacy_primary_revolver"
        )
        identity = canonical_debt_revolver_row_identity(
            row,
            fallback_facility_id=fallback_facility,
        )
        candidates.append(_DebtRowCandidate(row=row, origin="base", identity=identity))
    candidates.extend(overlay_candidates)

    grouped: dict[DebtRevolverRowIdentity, list[_DebtRowCandidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.identity, []).append(candidate)
    result = pd.DataFrame(
        _reconcile_candidate_group(grouped[identity])
        for identity in sorted(grouped)
    ).reset_index(drop=True)
    result.attrs.update(base_attrs)
    result.attrs.update(overlay_attrs)
    return result
