"""Typed, evidence-preserving Valuation thesis/debate projection."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping


_THESIS_FIELDS = (
    ("key_debate", "Key debate"),
    ("why_it_can_work", "Why it can work"),
    ("upside_factors", "Upside factors"),
    ("downside_factors", "Downside factors"),
    ("watch_next", "Watch next"),
    ("current_stance", "Current stance"),
)
_INVALIDATOR_KEYS = (
    ("sales-execution-breaks", "Sales-execution invalidator"),
    ("margin-durability-breaks", "Margin-durability invalidator"),
)
_ALLOWED_REVIEW_STATES = frozenset({"accepted", "manual_review_required", "unavailable"})


class ThesisProjectionError(ValueError):
    """Fail-closed typed thesis projection error."""


@dataclass(frozen=True)
class ResolvedThesisDisposition:
    row_key: str
    item_id: str
    item_type: str
    text: str
    review_state: str
    normalized_path: str
    evidence_classification: str
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_key": self.row_key,
            "item_id": self.item_id,
            "item_type": self.item_type,
            "text": self.text,
            "review_state": self.review_state,
            "normalized_path": self.normalized_path,
            "evidence_classification": self.evidence_classification,
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_ref": self.source_ref,
        }


@dataclass(frozen=True)
class ValuationThesisProjection:
    rows: tuple[ResolvedThesisDisposition, ...]
    projection_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": [row.to_dict() for row in self.rows],
            "projection_digest": self.projection_digest,
        }


def build_valuation_thesis_projection(investment_case: Mapping[str, Any]) -> ValuationThesisProjection:
    """Project only declared typed investment-case fields; never infer from prose."""

    rows: list[ResolvedThesisDisposition] = []
    for item_id, label in _THESIS_FIELDS:
        field = investment_case.get(item_id)
        if field in (None, ""):
            continue
        rows.append(
            _resolve_field(
                item_id=item_id,
                item_type=label,
                normalized_path=f"investment_case.{item_id}",
                field=field,
            )
        )

    raw_invalidators = investment_case.get("invalidators") or []
    if not isinstance(raw_invalidators, list):
        raise ThesisProjectionError("investment_case.invalidators must be a typed list.")
    invalidators: dict[str, Mapping[str, Any]] = {}
    for raw in raw_invalidators:
        if not isinstance(raw, Mapping):
            raise ThesisProjectionError("Every investment-case invalidator must be an object.")
        business_key = str(raw.get("business_key") or "")
        if not business_key:
            raise ThesisProjectionError("Every investment-case invalidator requires a business_key.")
        if business_key in invalidators:
            raise ThesisProjectionError(f"Duplicate investment-case invalidator business_key: {business_key!r}.")
        invalidators[business_key] = raw
    for business_key, label in _INVALIDATOR_KEYS:
        raw = invalidators.get(business_key)
        if raw is None:
            continue
        rows.append(
            _resolve_field(
                item_id=business_key,
                item_type=label,
                normalized_path=f"investment_case.invalidators.{business_key}",
                field=raw.get("text"),
            )
        )

    payload = {"rows": [row.to_dict() for row in rows]}
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return ValuationThesisProjection(rows=tuple(rows), projection_digest=digest)


def _resolve_field(
    *,
    item_id: str,
    item_type: str,
    normalized_path: str,
    field: Any,
) -> ResolvedThesisDisposition:
    if not isinstance(field, Mapping):
        raise ThesisProjectionError(f"{normalized_path} must be a typed normalized field.")
    status = str(field.get("status") or "")
    text = str(field.get("value") or "")
    review_state = str(field.get("review_state") or "")
    source_ref = str(field.get("source_ref") or "")
    evidence_refs = tuple(sorted({str(value) for value in field.get("evidence_refs") or [] if str(value)}))
    if status != "populated" or not text:
        raise ThesisProjectionError(f"{normalized_path} is not a populated typed field.")
    if review_state not in _ALLOWED_REVIEW_STATES:
        raise ThesisProjectionError(f"{normalized_path} has unsupported review_state {review_state!r}.")
    if not source_ref or not evidence_refs:
        raise ThesisProjectionError(f"{normalized_path} is missing exact source/evidence lineage.")
    source_refs = tuple(sorted({source_ref, *evidence_refs}))
    return ResolvedThesisDisposition(
        row_key=item_id,
        item_id=item_id,
        item_type=item_type,
        text=text,
        review_state=review_state,
        normalized_path=normalized_path,
        evidence_classification=str(field.get("evidence_classification") or ""),
        evidence_refs=evidence_refs,
        source_refs=source_refs,
    )
