"""Ticker-neutral segment source semantics and canonical identities."""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from .longitudinal_memory.identity import (
    build_identity,
    canonical_company_id,
    canonical_slug,
    sorted_reference_digest,
    validate_semantic_id,
)


SEGMENT_PERIOD_TYPE_ALIASES = {
    "quarter": "quarterly",
    "quarterly": "quarterly",
    "fiscal_quarter": "quarterly",
    "annual": "annual",
    "fiscal_year": "annual",
    "full_year": "annual",
}
SEGMENT_SOURCE_SCOPE_ALIASES = {
    "quarter": "quarterly",
    "quarterly": "quarterly",
    "fourth_quarter": "quarterly",
    "annual": "annual",
    "fiscal_year": "annual",
    "full_year": "annual",
}
SEGMENT_SOURCE_SCALES = {"ones", "thousands", "millions", "not_applicable"}
SEGMENT_AGGREGATION_ROLES = {"dimension_member", "reported_total"}

SEGMENT_RESIDUAL_LEDGER_CONTRACT_ID = "contract:segment-residual-derivation-ledger@1"
SEGMENT_EXACT_RESIDUAL_RULE_ID = "derivation:core:segment-reported-total-minus-components@1"
SEGMENT_EXACT_ZERO_CLASSIFICATION = "derived_exact_zero"
SEGMENT_REPORTED_BASIS_ID = "basis:core:reported@1"
SEGMENT_USD_MILLIONS_UNIT_ID = "unit:core:usd-millions@1"
SEGMENT_REPORTABLE_SCOPE = "reportable_segments"

_QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")
_ANNUAL_RE = re.compile(r"^\d{4}-FY$")
_TOTAL_COMPANY_ALIASES = {"total", "company total", "total company"}


class SegmentNormalizationError(ValueError):
    """Raised when source-table semantics cannot produce one exact segment fact."""

    def __init__(
        self,
        message: str,
        *,
        raw_pair: tuple[str, str] | None = None,
        canonical_pair: tuple[str, str] | None = None,
        source_row_ref: str = "",
        business_key: str = "",
    ) -> None:
        self.raw_pair = raw_pair
        self.canonical_pair = canonical_pair
        self.source_row_ref = source_row_ref
        self.business_key = business_key
        super().__init__(message)


def _decimal_text(value: Any) -> str:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SegmentNormalizationError(f"Segment trace value must be an exact decimal, received {value!r}.") from exc
    if not parsed.is_finite():
        raise SegmentNormalizationError(f"Segment trace value must be finite, received {value!r}.")
    normalized = parsed.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _document_key(value: Any) -> str:
    """Return a worktree-independent document key for legacy source rows."""

    raw = str(value or "").strip()
    if not raw:
        raise SegmentNormalizationError("Segment source lineage requires a document reference.")
    basename = re.split(r"[\\/]", raw)[-1].split("#", 1)[0]
    slug = re.sub(r"[^a-z0-9]+", "-", basename.lower()).strip("-")
    if not slug or not re.match(r"^[a-z]", slug):
        slug = f"source-{slug}" if slug else "source-document"
    return canonical_slug(slug)


@dataclass(frozen=True)
class SegmentResidualInputFact:
    """One direct source fact eligible for an exact segment residual.

    The readable record and economic identities intentionally contain business
    semantics rather than a workbook coordinate.  ``record_id`` adds the
    immutable document occurrence to the stable economic identity.
    """

    company_id: str
    metric_label: str
    metric_id: str
    segment_member: str
    value: str
    period_end: str
    period_id: str
    period_type: str
    basis_id: str
    unit_id: str
    currency: str
    scope: str
    aggregation_role: str
    source_document_id: str
    evidence_occurrence_id: str
    source_ref: str
    assertion_mode: str = "reported"

    def __post_init__(self) -> None:
        canonical_company_id(self.company_id)
        validate_semantic_id(self.metric_id, prefix="metric")
        validate_semantic_id(self.period_id, prefix="period")
        validate_semantic_id(self.basis_id, prefix="basis")
        validate_semantic_id(self.unit_id, prefix="unit")
        canonical_segment_period_type(self.period_type)
        _decimal_text(self.value)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(self.period_end or "")):
            raise SegmentNormalizationError(f"Segment trace requires an ISO period end, received {self.period_end!r}.")
        if not all(
            str(value or "").strip()
            for value in (
                self.metric_label,
                self.segment_member,
                self.period_id,
                self.currency,
                self.scope,
                self.source_document_id,
                self.evidence_occurrence_id,
                self.source_ref,
            )
        ):
            raise SegmentNormalizationError("Segment residual input lineage is incomplete.")
        if self.aggregation_role not in {"component", "reported_total"}:
            raise SegmentNormalizationError(
                f"Unsupported segment residual aggregation role {self.aggregation_role!r}."
            )
        if self.assertion_mode != "reported":
            raise SegmentNormalizationError("A segment residual may use only direct reported inputs.")

    @property
    def decimal_value(self) -> Decimal:
        return Decimal(_decimal_text(self.value))

    @property
    def compatibility_key(self) -> tuple[str, str, str, str, str, str, str, str, str]:
        return (
            canonical_company_id(self.company_id),
            self.metric_id,
            self.period_id,
            self.period_end,
            canonical_segment_period_type(self.period_type),
            self.basis_id,
            self.unit_id,
            self.currency,
            self.scope,
        )

    @property
    def economic_identity(self) -> str:
        return build_identity(
            "segment-economic",
            (
                ("co", canonical_company_id(self.company_id)),
                ("metric", self.metric_id),
                ("period", self.period_id),
                ("member", self.segment_member),
                ("basis", self.basis_id),
                ("unit", self.unit_id),
                ("ccy", self.currency),
                ("scope", self.scope),
            ),
        )

    @property
    def record_id(self) -> str:
        return build_identity(
            "segment-fact",
            (
                ("economic", self.economic_identity),
                ("occ", self.evidence_occurrence_id),
                ("mode", self.assertion_mode),
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "economic_identity": self.economic_identity,
            "assertion_mode": self.assertion_mode,
            "company_id": canonical_company_id(self.company_id),
            "metric_label": self.metric_label,
            "metric_id": self.metric_id,
            "segment_member": self.segment_member,
            "value": {"kind": "exact", "value": _decimal_text(self.value)},
            "period_end": self.period_end,
            "period_id": self.period_id,
            "period_type": canonical_segment_period_type(self.period_type),
            "basis_id": self.basis_id,
            "unit_id": self.unit_id,
            "currency": self.currency,
            "scope": self.scope,
            "aggregation_role": self.aggregation_role,
            "source_document_id": self.source_document_id,
            "evidence_occurrence_ids": [self.evidence_occurrence_id],
            "source_ref": self.source_ref,
        }


@dataclass(frozen=True)
class SegmentResidualDerivation:
    company_id: str
    metric_label: str
    metric_id: str
    target_member: str
    period_end: str
    period_id: str
    period_type: str
    basis_id: str
    unit_id: str
    currency: str
    scope: str
    value: str
    direct_total_input_id: str
    direct_component_input_ids: tuple[str, ...]
    source_document_ids: tuple[str, ...]
    evidence_occurrence_ids: tuple[str, ...]
    rule_id: str = SEGMENT_EXACT_RESIDUAL_RULE_ID
    classification: str = SEGMENT_EXACT_ZERO_CLASSIFICATION

    def __post_init__(self) -> None:
        canonical_company_id(self.company_id)
        validate_semantic_id(self.metric_id, prefix="metric")
        validate_semantic_id(self.period_id, prefix="period")
        validate_semantic_id(self.basis_id, prefix="basis")
        validate_semantic_id(self.unit_id, prefix="unit")
        validate_semantic_id(self.rule_id, prefix="derivation")
        canonical_segment_period_type(self.period_type)
        if _decimal_text(self.value) != "0" or self.classification != SEGMENT_EXACT_ZERO_CLASSIFICATION:
            raise SegmentNormalizationError("The exact-zero segment residual contract may emit only exact zero.")
        if not self.direct_total_input_id or len(self.direct_component_input_ids) < 2:
            raise SegmentNormalizationError("Segment residual lineage requires one total and complete components.")
        if len(self.direct_component_input_ids) != len(set(self.direct_component_input_ids)):
            raise SegmentNormalizationError("Segment residual component input identities must be unique.")
        if not self.source_document_ids or not self.evidence_occurrence_ids:
            raise SegmentNormalizationError("Segment residual lineage requires document occurrences.")

    @property
    def economic_identity(self) -> str:
        return build_identity(
            "segment-economic",
            (
                ("co", canonical_company_id(self.company_id)),
                ("metric", self.metric_id),
                ("period", self.period_id),
                ("member", self.target_member),
                ("basis", self.basis_id),
                ("unit", self.unit_id),
                ("ccy", self.currency),
                ("scope", self.scope),
            ),
        )

    @property
    def derivation_id(self) -> str:
        input_ids = (self.direct_total_input_id, *self.direct_component_input_ids)
        return build_identity(
            "segment-derivation",
            (
                ("target", self.economic_identity),
                ("rule", self.rule_id),
                ("inputs", sorted_reference_digest(input_ids)),
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        input_ids = sorted({self.direct_total_input_id, *self.direct_component_input_ids})
        return {
            "derivation_id": self.derivation_id,
            "economic_identity": self.economic_identity,
            "rule_id": self.rule_id,
            "classification": self.classification,
            "assertion_mode": "derived",
            "metric_label": self.metric_label,
            "metric_id": self.metric_id,
            "target_member": self.target_member,
            "value": {"kind": "exact", "value": _decimal_text(self.value)},
            "period_end": self.period_end,
            "period_id": self.period_id,
            "period_type": canonical_segment_period_type(self.period_type),
            "basis_id": self.basis_id,
            "unit_id": self.unit_id,
            "currency": self.currency,
            "scope": self.scope,
            "direct_total_input_id": self.direct_total_input_id,
            "direct_component_input_ids": list(self.direct_component_input_ids),
            "input_record_ids": input_ids,
            "source_document_ids": list(self.source_document_ids),
            "evidence_occurrence_ids": list(self.evidence_occurrence_ids),
        }


def segment_residual_input_fact_from_legacy_row(
    *,
    company_id: str,
    metric_label: str,
    metric_id: str,
    segment_member: str,
    value_millions: Any,
    period_end: Any,
    period_id: str,
    source_doc: Any,
    source_type: Any,
    source_locator: Any,
    aggregation_role: str,
    basis_id: str = SEGMENT_REPORTED_BASIS_ID,
    unit_id: str = SEGMENT_USD_MILLIONS_UNIT_ID,
    currency: str = "USD",
    scope: str = SEGMENT_REPORTABLE_SCOPE,
) -> SegmentResidualInputFact:
    company = canonical_company_id(company_id)
    document_key = _document_key(source_doc)
    document_type = re.sub(r"[^a-z0-9]+", "-", str(source_type or "source").strip().lower()).strip("-") or "source"
    document_id = build_identity(
        "doc",
        (
            ("co", company),
            ("type", canonical_slug(document_type)),
            ("key", document_key),
            ("rev", 1),
        ),
    )
    locator = str(source_locator or "").strip()
    if not locator:
        raise SegmentNormalizationError("Segment source lineage requires a document locator.")
    occurrence_id = build_identity(
        "occurrence",
        (
            ("doc", document_id),
            ("locator", locator),
        ),
    )
    period_ts = str(period_end)[:10]
    return SegmentResidualInputFact(
        company_id=company,
        metric_label=str(metric_label).strip(),
        metric_id=metric_id,
        segment_member=str(segment_member).strip(),
        value=_decimal_text(value_millions),
        period_end=period_ts,
        period_id=period_id,
        period_type="quarterly",
        basis_id=basis_id,
        unit_id=unit_id,
        currency=currency,
        scope=scope,
        aggregation_role=aggregation_role,
        source_document_id=document_id,
        evidence_occurrence_id=occurrence_id,
        source_ref=f"{str(source_doc).strip()}#{locator}",
    )


def derive_exact_zero_segment_residual(
    *,
    total: SegmentResidualInputFact,
    components: Iterable[SegmentResidualInputFact],
    target_member: str,
) -> SegmentResidualDerivation | None:
    """Derive an exact zero only from one direct total and complete direct components."""

    component_rows = tuple(components)
    if total.aggregation_role != "reported_total" or total.assertion_mode != "reported":
        raise SegmentNormalizationError("A segment residual requires one direct reported total.")
    if len(component_rows) < 2:
        raise SegmentNormalizationError("A segment residual requires the complete direct component set.")
    if any(row.aggregation_role != "component" or row.assertion_mode != "reported" for row in component_rows):
        raise SegmentNormalizationError("A segment residual may not consume a derived or non-component input.")
    component_members = [row.segment_member for row in component_rows]
    if len(component_members) != len(set(component_members)) or target_member in set(component_members):
        raise SegmentNormalizationError("Segment residual components must be unique and exclude the target member.")
    expected = total.compatibility_key
    incompatible = [row.record_id for row in component_rows if row.compatibility_key != expected]
    if incompatible:
        raise SegmentNormalizationError(
            "Segment residual inputs disagree on company, metric, period, basis, unit, currency, or scope."
        )
    residual = total.decimal_value - sum((row.decimal_value for row in component_rows), Decimal("0"))
    if residual != Decimal("0"):
        return None
    component_ids = tuple(sorted(row.record_id for row in component_rows))
    source_documents = tuple(sorted({total.source_document_id, *(row.source_document_id for row in component_rows)}))
    occurrences = tuple(
        sorted({total.evidence_occurrence_id, *(row.evidence_occurrence_id for row in component_rows)})
    )
    return SegmentResidualDerivation(
        company_id=total.company_id,
        metric_label=total.metric_label,
        metric_id=total.metric_id,
        target_member=str(target_member).strip(),
        period_end=total.period_end,
        period_id=total.period_id,
        period_type=total.period_type,
        basis_id=total.basis_id,
        unit_id=total.unit_id,
        currency=total.currency,
        scope=total.scope,
        value="0",
        direct_total_input_id=total.record_id,
        direct_component_input_ids=component_ids,
        source_document_ids=source_documents,
        evidence_occurrence_ids=occurrences,
    )


def validate_segment_residual_ledger_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Strictly validate and replay a serialized segment-residual ledger.

    A ledger is authoritative only when every serialized identity reconstructs,
    every derivation input resolves to one unique direct fact, and the exact
    residual arithmetic deterministically replays from those facts.
    """

    if not isinstance(payload, Mapping):
        raise SegmentNormalizationError("Segment residual ledger must be a mapping.")
    ledger = dict(payload)
    required_keys = {"contract_id", "source_facts", "derivations"}
    if set(ledger) != required_keys:
        raise SegmentNormalizationError("Segment residual ledger fields do not match its versioned contract.")
    if ledger.get("contract_id") != SEGMENT_RESIDUAL_LEDGER_CONTRACT_ID:
        raise SegmentNormalizationError("Segment residual ledger contract identity is invalid.")
    source_rows = ledger.get("source_facts")
    derivation_rows = ledger.get("derivations")
    if not isinstance(source_rows, list) or not isinstance(derivation_rows, list):
        raise SegmentNormalizationError("Segment residual ledger rows must be deterministic JSON arrays.")

    facts_by_id: dict[str, SegmentResidualInputFact] = {}
    canonical_fact_rows: dict[str, dict[str, Any]] = {}
    for row_in in source_rows:
        if not isinstance(row_in, Mapping):
            raise SegmentNormalizationError("Segment residual source-fact rows must be mappings.")
        row = dict(row_in)
        value_row = row.get("value")
        occurrence_ids = row.get("evidence_occurrence_ids")
        if (
            not isinstance(value_row, Mapping)
            or dict(value_row).get("kind") != "exact"
            or not isinstance(occurrence_ids, list)
            or len(occurrence_ids) != 1
        ):
            raise SegmentNormalizationError("Segment residual source-fact value or occurrence structure is invalid.")
        try:
            fact = SegmentResidualInputFact(
                company_id=row["company_id"],
                metric_label=row["metric_label"],
                metric_id=row["metric_id"],
                segment_member=row["segment_member"],
                value=dict(value_row)["value"],
                period_end=row["period_end"],
                period_id=row["period_id"],
                period_type=row["period_type"],
                basis_id=row["basis_id"],
                unit_id=row["unit_id"],
                currency=row["currency"],
                scope=row["scope"],
                aggregation_role=row["aggregation_role"],
                source_document_id=row["source_document_id"],
                evidence_occurrence_id=occurrence_ids[0],
                source_ref=row["source_ref"],
                assertion_mode=row["assertion_mode"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SegmentNormalizationError("Segment residual source-fact row is incomplete.") from exc
        canonical_row = fact.to_dict()
        if row != canonical_row:
            raise SegmentNormalizationError("Segment residual source-fact identity or fields do not reconstruct.")
        if fact.record_id in facts_by_id:
            raise SegmentNormalizationError("Segment residual source-fact identities must be unique.")
        facts_by_id[fact.record_id] = fact
        canonical_fact_rows[fact.record_id] = canonical_row

    canonical_derivation_rows: dict[str, dict[str, Any]] = {}
    referenced_fact_ids: set[str] = set()
    for row_in in derivation_rows:
        if not isinstance(row_in, Mapping):
            raise SegmentNormalizationError("Segment residual derivation rows must be mappings.")
        row = dict(row_in)
        value_row = row.get("value")
        if not isinstance(value_row, Mapping) or dict(value_row).get("kind") != "exact":
            raise SegmentNormalizationError("Segment residual derivation value structure is invalid.")
        try:
            derivation = SegmentResidualDerivation(
                company_id=row["company_id"] if "company_id" in row else facts_by_id[row["direct_total_input_id"]].company_id,
                metric_label=row["metric_label"],
                metric_id=row["metric_id"],
                target_member=row["target_member"],
                period_end=row["period_end"],
                period_id=row["period_id"],
                period_type=row["period_type"],
                basis_id=row["basis_id"],
                unit_id=row["unit_id"],
                currency=row["currency"],
                scope=row["scope"],
                value=dict(value_row)["value"],
                direct_total_input_id=row["direct_total_input_id"],
                direct_component_input_ids=tuple(row["direct_component_input_ids"]),
                source_document_ids=tuple(row["source_document_ids"]),
                evidence_occurrence_ids=tuple(row["evidence_occurrence_ids"]),
                rule_id=row["rule_id"],
                classification=row["classification"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SegmentNormalizationError("Segment residual derivation row is incomplete.") from exc
        canonical_row = derivation.to_dict()
        if row != canonical_row:
            raise SegmentNormalizationError("Segment residual derivation target, identity, or fields do not reconstruct.")
        if derivation.derivation_id in canonical_derivation_rows:
            raise SegmentNormalizationError("Segment residual derivation identities must be unique.")
        input_ids = tuple(canonical_row["input_record_ids"])
        missing_ids = [record_id for record_id in input_ids if record_id not in facts_by_id]
        if missing_ids:
            raise SegmentNormalizationError("Segment residual derivation inputs do not resolve to source facts.")
        total_fact = facts_by_id[derivation.direct_total_input_id]
        component_facts = tuple(facts_by_id[record_id] for record_id in derivation.direct_component_input_ids)
        replayed = derive_exact_zero_segment_residual(
            total=total_fact,
            components=component_facts,
            target_member=derivation.target_member,
        )
        if replayed is None or replayed.to_dict() != canonical_row:
            raise SegmentNormalizationError("Segment residual derivation does not replay exactly from its source facts.")
        canonical_derivation_rows[derivation.derivation_id] = canonical_row
        referenced_fact_ids.update(input_ids)

    if referenced_fact_ids != set(facts_by_id):
        raise SegmentNormalizationError("Segment residual ledger contains unreferenced or unexplained source facts.")
    return {
        "contract_id": SEGMENT_RESIDUAL_LEDGER_CONTRACT_ID,
        "source_facts": [canonical_fact_rows[key] for key in sorted(canonical_fact_rows)],
        "derivations": [canonical_derivation_rows[key] for key in sorted(canonical_derivation_rows)],
    }


def _token(value: Any) -> str:
    return re.sub(r"[\s_-]+", " ", str(value or "").strip().lower())


def canonical_segment_period_type(value: Any) -> str:
    token = _token(value).replace(" ", "_")
    try:
        return SEGMENT_PERIOD_TYPE_ALIASES[token]
    except KeyError as exc:
        raise SegmentNormalizationError(f"Unsupported segment period type {value!r}.") from exc


def canonical_segment_source_scope(value: Any) -> str:
    token = _token(value).replace(" ", "_")
    try:
        return SEGMENT_SOURCE_SCOPE_ALIASES[token]
    except KeyError as exc:
        raise SegmentNormalizationError(f"Unsupported segment source-table scope {value!r}.") from exc


def canonical_segment_dimension_member(dimension: Any, member: Any) -> tuple[str, str]:
    raw_pair = (str(dimension or ""), str(member or ""))
    dimension_token = _token(dimension).replace(" ", "_")
    member_token = _token(member)
    dimension_is_total = dimension_token == "total_company"
    member_is_total = member_token in _TOTAL_COMPANY_ALIASES
    canonical_pair = (dimension_token, "total_company" if member_is_total else member_token)
    if member_is_total and not dimension_is_total:
        raise SegmentNormalizationError(
            f"Invalid segment pair: raw_pair={raw_pair!r}, canonical_pair={canonical_pair!r}; "
            "a Total Company member alias requires dimension 'total_company'.",
            raw_pair=raw_pair,
            canonical_pair=canonical_pair,
        )
    if dimension_is_total and not member_is_total:
        raise SegmentNormalizationError(
            f"Invalid segment pair: raw_pair={raw_pair!r}, canonical_pair={canonical_pair!r}; "
            "dimension 'total_company' requires a Total Company member alias.",
            raw_pair=raw_pair,
            canonical_pair=canonical_pair,
        )
    return dimension_token, "total_company" if dimension_is_total else member_token


def canonical_segment_display_member(dimension: Any, member: Any) -> str:
    """Return the stable workbook label while retaining non-total source labels."""

    canonical_pair = canonical_segment_dimension_member(dimension, member)
    if canonical_pair == ("total_company", "total_company"):
        return "Total Company"
    return str(member or "").strip()


def canonical_segment_member(dimension: Any, member: Any) -> str:
    return canonical_segment_dimension_member(dimension, member)[1]


def segment_aggregation_role(dimension: Any, member: Any) -> str:
    canonical_dimension, _ = canonical_segment_dimension_member(dimension, member)
    return "reported_total" if canonical_dimension == "total_company" else "dimension_member"


def canonical_segment_business_identity(item: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    period_type = canonical_segment_period_type(item.get("period_type"))
    dimension, member = canonical_segment_dimension_member(item.get("dimension"), item.get("member"))
    return (
        period_type,
        str(item.get("period") or "").strip(),
        dimension,
        member,
        _token(item.get("metric")).replace(" ", "_"),
    )


def normalize_segment_currency_to_millions(
    value: Any,
    *,
    source_unit: Any,
    source_scale: Any,
) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise SegmentNormalizationError(f"Segment currency value must be numeric, received {value!r}.")
    unit = str(source_unit or "").strip().lower()
    scale = _token(source_scale).replace(" ", "_")
    if scale not in SEGMENT_SOURCE_SCALES - {"not_applicable"}:
        raise SegmentNormalizationError(f"Unsupported segment source scale {source_scale!r}.")
    if unit in {"$m", "usdm"}:
        if scale != "millions":
            raise SegmentNormalizationError(
                f"A source value declared in $m must use source_scale='millions', received {source_scale!r}."
            )
        multiplier = 1.0
    elif unit in {"$", "usd"}:
        multiplier = {"ones": 0.000001, "thousands": 0.001, "millions": 1.0}[scale]
    else:
        raise SegmentNormalizationError(f"Unsupported segment currency unit {source_unit!r}.")
    return round(float(value) * multiplier, 3)


@dataclass(frozen=True)
class SegmentSourceFact:
    metric: str
    value: float
    source_unit: str
    source_scale: str
    period_type: str
    period: str
    dimension: str
    member: str
    source_table_scope: str
    source_table_id: str
    source_row_ref: str
    source_ref: str

    def __post_init__(self) -> None:
        period_type = canonical_segment_period_type(self.period_type)
        scope = canonical_segment_source_scope(self.source_table_scope)
        if period_type != scope:
            raise SegmentNormalizationError(
                f"Segment source scope {scope!r} is incompatible with period type {period_type!r} "
                f"for {self.source_row_ref}."
            )
        period_re = _QUARTER_RE if period_type == "quarterly" else _ANNUAL_RE
        if not period_re.fullmatch(str(self.period or "")):
            raise SegmentNormalizationError(
                f"Segment period {self.period!r} is incompatible with period type {period_type!r} "
                f"for {self.source_row_ref}."
            )
        if _token(self.source_scale).replace(" ", "_") not in SEGMENT_SOURCE_SCALES:
            raise SegmentNormalizationError(f"Unsupported segment source scale {self.source_scale!r}.")
        for field_name in ("metric", "dimension", "member", "source_table_id", "source_row_ref", "source_ref"):
            if not str(getattr(self, field_name) or "").strip():
                raise SegmentNormalizationError(f"Segment source fact requires {field_name}.")
        try:
            canonical_segment_dimension_member(self.dimension, self.member)
        except SegmentNormalizationError as exc:
            raw_pair = (self.dimension, self.member)
            canonical_pair = exc.canonical_pair
            business_key = "|".join(
                (
                    period_type,
                    self.period,
                    *(canonical_pair or raw_pair),
                    _token(self.metric).replace(" ", "_"),
                )
            )
            raise SegmentNormalizationError(
                f"{exc} source_row_ref={self.source_row_ref!r}, business_key={business_key!r}.",
                raw_pair=raw_pair,
                canonical_pair=canonical_pair,
                source_row_ref=self.source_row_ref,
                business_key=business_key,
            ) from exc

    @property
    def normalized_value(self) -> float:
        return normalize_segment_currency_to_millions(
            self.value,
            source_unit=self.source_unit,
            source_scale=self.source_scale,
        )

    @property
    def business_identity(self) -> tuple[str, str, str, str, str]:
        return canonical_segment_business_identity(
            {
                "period_type": self.period_type,
                "period": self.period,
                "dimension": self.dimension,
                "member": self.member,
                "metric": self.metric,
            }
        )

    def metadata(self) -> dict[str, str]:
        period_type = canonical_segment_period_type(self.period_type)
        return {
            "unit": "$m",
            "source_unit": self.source_unit,
            "source_scale": _token(self.source_scale).replace(" ", "_"),
            "source_table_scope": canonical_segment_source_scope(self.source_table_scope),
            "source_table_id": self.source_table_id,
            "source_row_ref": self.source_row_ref,
            "source_ref": self.source_ref,
            "aggregation_role": segment_aggregation_role(self.dimension, self.member),
            "period_type": period_type,
        }


def canonicalize_segment_source_facts(
    facts: Iterable[SegmentSourceFact],
) -> tuple[SegmentSourceFact, ...]:
    """Return one deterministic fact per canonical segment business identity."""

    by_identity: dict[tuple[str, str, str, str, str], SegmentSourceFact] = {}
    for fact in facts:
        identity = fact.business_identity
        prior = by_identity.get(identity)
        if prior is not None:
            raw_pair = (fact.dimension, fact.member)
            canonical_pair = identity[2:4]
            business_key = "|".join(identity)
            raise SegmentNormalizationError(
                "Duplicate canonical segment business identity "
                f"{identity!r}; first_raw_pair={(prior.dimension, prior.member)!r}, "
                f"duplicate_raw_pair={raw_pair!r}, canonical_pair={canonical_pair!r}, "
                f"first_source_row_ref={prior.source_row_ref!r}, "
                f"duplicate_source_row_ref={fact.source_row_ref!r}, business_key={business_key!r}.",
                raw_pair=raw_pair,
                canonical_pair=canonical_pair,
                source_row_ref=fact.source_row_ref,
                business_key=business_key,
            )
        by_identity[identity] = fact
    return tuple(by_identity[identity] for identity in sorted(by_identity))
