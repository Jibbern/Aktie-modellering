"""Investor-information successor to the frozen Promise Progress Product@1.

``PromiseProgressProduct@2`` consumes the validated source-native longitudinal
package.  It owns investor row/block eligibility, block order, historical version
state, typed change labels, disclosure-event order, and presentation-safe source
metadata.  It does not select evidence, alter economics, or assign workbook cells.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .promise_progress_projection import (
    ACTUAL_FY_ID,
    ACTUAL_ROLE_SEMANTIC_CLASSES,
    STATUS_APPROX_ID,
    STATUS_LABELS,
    STATUS_MAX_ID,
    STATUS_MIN_ID,
    STATUS_OPEN_ID,
    STATUS_POINT_ID,
    STATUS_QUALITATIVE_ID,
    STATUS_RANGE_ID,
    STATUS_REVIEW_ID,
    ActualSelection,
    DisplayValue,
    SemanticIdentity,
    assess_status,
)
from .sector_packs.retail import derive_store_remodels_right_sizes


PRODUCT_TYPE = "PromiseProgressProduct@2"
PRODUCT_VERSION = "2.0.0-candidate"
CAPEX_EQUIVALENCE_TOPIC_ID = (
    "topic:core:capital-expenditure-property-equipment-equivalence@1"
)

OPEN_BLOCK_ID = "block:promise-progress:open-guidance@2"
PROGRESSION_BLOCK_ID = "block:promise-progress:guidance-progression@2"
TIMELINE_BLOCK_ID = "block:promise-progress:revision-timeline@2"
CREDIBILITY_BLOCK_ID = "block:promise-progress:management-credibility@2"
BLOCK_ORDER = (
    CREDIBILITY_BLOCK_ID,
    PROGRESSION_BLOCK_ID,
    OPEN_BLOCK_ID,
    TIMELINE_BLOCK_ID,
)

COVERAGE_STATES = frozenset(
    {
        "complete_for_reviewed_scope",
        "partial_reviewed_source_coverage",
        "material_mapping_gap",
        "needs_review",
    }
)
VERSION_STATES = frozenset({"Current", "Superseded", "Final", "Withdrawn", "Needs Review"})
CHANGE_TYPES = frozenset(
    {
        "Initial",
        "Raised",
        "Lowered",
        "Reaffirmed",
        "Lower bound raised",
        "Lower bound lowered",
        "Upper bound raised",
        "Upper bound lowered",
        "Range narrowed",
        "Range widened",
        "Range shifted higher",
        "Range shifted lower",
        "Qualitative → range",
        "Range → qualitative",
        "Range → minimum",
        "Range → approximate",
        "Updated — not directly comparable",
    }
)
ROW_KINDS = frozenset(
    {
        "open_guidance",
        "guidance_progression",
        "timeline_version",
        "assessment_unavailable",
    }
)
INELIGIBLE_ROW_KINDS = frozenset(
    {"diagnostic_coverage_gap", "parity_only", "reserved_capacity", "legacy_only"}
)

METRIC_ORDER = {
    "metric:core:revenue-growth@1": 0,
    "metric:core:operating-margin@1": 1,
    "metric:core:net-income-per-diluted-share@1": 2,
    "metric:core:diluted-weighted-average-shares@1": 3,
    "metric:core:capital-expenditures@1": 4,
    "metric:core:share-repurchases@1": 5,
    "metric:retail:net-store-openings@1": 6,
    "metric:retail:store-openings@1": 7,
    "metric:retail:store-closures-count@1": 8,
    "metric:retail:store-remodels-right-sizes@1": 9,
}
METRIC_LABELS = {
    "metric:core:revenue-growth@1": "Net-sales growth",
    "metric:core:operating-margin@1": "Operating margin",
    "metric:core:net-income-per-diluted-share@1": "Net income per diluted share",
    "metric:core:capital-expenditures@1": "Capital expenditures",
    "metric:core:share-repurchases@1": "Share repurchases",
    "metric:core:diluted-weighted-average-shares@1": "Diluted weighted average shares",
    "metric:retail:net-store-openings@1": "Net store openings",
    "metric:retail:store-openings@1": "Store openings",
    "metric:retail:store-closures-count@1": "Store closures",
    "metric:retail:store-remodels-right-sizes@1": "Store remodels / right-sizes",
}

FAVORABLE_DIRECTION_BY_METRIC = MappingProxyType(
    {
        "metric:core:revenue-growth@1": "higher",
        "metric:core:operating-margin@1": "higher",
        "metric:core:net-income-per-diluted-share@1": "higher",
        "metric:core:share-repurchases@1": "higher",
        "metric:retail:net-store-openings@1": "higher",
    }
)

NEEDS_REVIEW_REASONS = MappingProxyType(
    {
        "assessment_unavailable": (
            "C",
            "No reviewed management-credibility assessment is available.",
        ),
        "approximate_target_tolerance_unreviewed": (
            "C",
            "Approximate guidance has no reviewed comparison tolerance.",
        ),
        "definition_equivalence_unreviewed": (
            "B",
            "Capital-expenditure guidance and property/equipment purchases have different definitions.",
        ),
        "comparable_actual_unavailable": (
            "C",
            "No reviewed compatible full-year Actual is available.",
        ),
        "approximate_target_direction_ambiguous": (
            "C",
            "Approximate guidance is not exactly met and has no reviewed favorable direction or tolerance.",
        ),
    }
)

_INVESTOR_JARGON = (
    "guidanceseries",
    "canonical",
    "resolver",
    "binding",
    "occurrence",
    "legacy parity only",
    "unsupported mapping",
    "unresolved comparison",
)

TIMELINE_FACT_ROLES = frozenset(
    {
        "event_period_actual",
        "ytd_progress",
        "cumulative_progress",
        "annualized_run_rate",
        "delta_progress",
        "incompatible",
        "unavailable",
    }
)


class PromiseProgressProductV2Error(ValueError):
    """Raised instead of emitting an open, ambiguous, or diagnostic investor product."""


def _product_id(company_id: str) -> str:
    normalized = company_id.strip()
    if not normalized:
        raise PromiseProgressProductV2Error("Product@2 requires a company identity.")
    return f"promise-progress-product:{normalized.casefold()}@2"


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class ProductVersionValueV2:
    version_record_id: str
    publication_date: str
    canonical_value: Mapping[str, Any]
    display_text: str
    source_document_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_value", _freeze_mapping(self.canonical_value))
        if not self.version_record_id or not self.publication_date or not self.display_text:
            raise PromiseProgressProductV2Error("A progression value needs stable identity, date, and display.")
        if len(set(self.source_document_ids)) != len(self.source_document_ids):
            raise PromiseProgressProductV2Error("A progression value has duplicate source identities.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_record_id": self.version_record_id,
            "publication_date": self.publication_date,
            "canonical_value": dict(self.canonical_value),
            "display_text": self.display_text,
            "source_document_ids": list(self.source_document_ids),
        }


@dataclass(frozen=True, slots=True)
class DisclosureEventV2:
    event_id: str
    event_date: str
    source_document_ids: tuple[str, ...]
    display_label: str
    reviewed_relation_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.event_id or not self.event_date or not self.source_document_ids:
            raise PromiseProgressProductV2Error("A disclosure event requires identity, date, and source.")
        if len(set(self.source_document_ids)) != len(self.source_document_ids):
            raise PromiseProgressProductV2Error("A disclosure event cannot duplicate a source document.")

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class ProductRowV2:
    row_id: str
    block_id: str
    row_kind: str
    eligible: bool
    group_id: str | None
    metric_id: str | None
    metric_label: str
    horizon_period_id: str | None
    horizon_label: str
    current_value: Mapping[str, Any] | None
    current_display: str
    progression_values: tuple[ProductVersionValueV2, ...]
    previous_display: str
    actual_value: Mapping[str, Any] | None
    actual_display: str
    actual_candidate_record_ids: tuple[str, ...]
    actual_period_id: str | None
    actual_knowledge_date: str | None
    actual_source_document_ids: tuple[str, ...]
    progress_value: Mapping[str, Any] | None
    progress_display: str
    progress_candidate_record_ids: tuple[str, ...]
    progress_period_id: str | None
    progress_knowledge_date: str | None
    progress_source_document_ids: tuple[str, ...]
    version_state: str | None
    status_code_at_update: str | None
    status_at_update: str | None
    change_type: str | None
    comparison_reason_code: str | None
    investor_reason_code: str | None
    investor_reason_display: str
    event_id: str | None
    event_date: str | None
    stated_in_period_id: str | None
    stated_in_display: str
    current_source_document_ids: tuple[str, ...]
    predecessor_source_document_ids: tuple[str, ...]
    source_summary: str
    lineage_digest: str
    parity_locator: str | None = None

    def __post_init__(self) -> None:
        if self.row_kind not in ROW_KINDS or self.row_kind in INELIGIBLE_ROW_KINDS:
            raise PromiseProgressProductV2Error(f"Unknown or excluded Product@2 row kind {self.row_kind!r}.")
        if not self.eligible:
            raise PromiseProgressProductV2Error("Product@2 serializes investor-eligible rows only.")
        if self.block_id not in BLOCK_ORDER or not self.row_id:
            raise PromiseProgressProductV2Error("A Product@2 row has an invalid owner or identity.")
        if self.current_value is not None:
            object.__setattr__(self, "current_value", _freeze_mapping(self.current_value))
        if self.actual_value is not None:
            object.__setattr__(self, "actual_value", _freeze_mapping(self.actual_value))
        if self.progress_value is not None:
            object.__setattr__(self, "progress_value", _freeze_mapping(self.progress_value))
        if self.actual_value is None:
            if (
                self.actual_display
                or self.actual_period_id is not None
                or self.actual_knowledge_date is not None
                or self.actual_source_document_ids
            ):
                raise PromiseProgressProductV2Error(
                    "A missing Actual cannot carry display, period, knowledge date, or source identity."
                )
        elif not (
            self.actual_display
            and self.actual_candidate_record_ids
            and self.actual_period_id
            and self.actual_knowledge_date
            and self.actual_source_document_ids
        ):
            raise PromiseProgressProductV2Error(
                "A selected Actual requires complete typed identity and lineage."
            )
        if self.progress_value is None:
            if (
                self.progress_display
                or self.progress_candidate_record_ids
                or self.progress_period_id is not None
                or self.progress_knowledge_date is not None
                or self.progress_source_document_ids
            ):
                raise PromiseProgressProductV2Error(
                    "Missing Progress cannot carry display, period, knowledge date, or source identity."
                )
        elif not (
            self.progress_display
            and self.progress_candidate_record_ids
            and self.progress_period_id
            and self.progress_knowledge_date
            and self.progress_source_document_ids
        ):
            raise PromiseProgressProductV2Error(
                "Selected Progress requires complete typed identity and lineage."
            )
        if self.version_state is not None and self.version_state not in VERSION_STATES:
            raise PromiseProgressProductV2Error(f"Unknown version state {self.version_state!r}.")
        if (self.status_code_at_update is None) != (self.status_at_update is None):
            raise PromiseProgressProductV2Error(
                "Outcome status code and label must either both exist or both remain absent."
            )
        if self.status_code_at_update is not None:
            if self.status_code_at_update not in STATUS_LABELS:
                raise PromiseProgressProductV2Error(
                    f"Unknown outcome status {self.status_code_at_update!r}."
                )
            if STATUS_LABELS[self.status_code_at_update] != self.status_at_update:
                raise PromiseProgressProductV2Error(
                    "Outcome status code and visible label differ from the closed status registry."
                )
        if self.change_type is not None and self.change_type not in CHANGE_TYPES:
            raise PromiseProgressProductV2Error(f"Unknown change type {self.change_type!r}.")
        identities = (
            self.current_source_document_ids,
            self.predecessor_source_document_ids,
            self.actual_candidate_record_ids,
            self.actual_source_document_ids,
            self.progress_candidate_record_ids,
            self.progress_source_document_ids,
        )
        if any(len(values) != len(set(values)) for values in identities):
            raise PromiseProgressProductV2Error("Row lineage identity sets must be unique.")
        if set(self.actual_candidate_record_ids) & set(self.progress_candidate_record_ids):
            raise PromiseProgressProductV2Error(
                "One source fact cannot populate both Timeline Actual and Progress."
            )
        visible = " ".join(
            (
                self.metric_label,
                self.horizon_label,
                self.current_display,
                self.previous_display,
                self.actual_display,
                self.progress_display,
                self.status_at_update or "",
                self.stated_in_display,
                self.investor_reason_display,
                self.source_summary,
                self.change_type or "",
            )
        ).casefold()
        leaked = [term for term in _INVESTOR_JARGON if term in visible]
        if leaked:
            raise PromiseProgressProductV2Error(f"Investor row leaks engine language: {leaked}.")
        if self.row_kind == "timeline_version" and self.event_date is not None:
            event_day = date.fromisoformat(self.event_date)
            for role, knowledge_date in (
                ("Actual", self.actual_knowledge_date),
                ("Progress", self.progress_knowledge_date),
            ):
                if knowledge_date is not None and date.fromisoformat(knowledge_date) > event_day:
                    raise PromiseProgressProductV2Error(
                        f"Timeline {role} leaks evidence after its disclosure-event cutoff."
                    )
        _validate_row_eligibility(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_id": self.row_id,
            "block_id": self.block_id,
            "row_kind": self.row_kind,
            "eligible": self.eligible,
            "group_id": self.group_id,
            "metric_id": self.metric_id,
            "metric_label": self.metric_label,
            "horizon_period_id": self.horizon_period_id,
            "horizon_label": self.horizon_label,
            "current_value": None if self.current_value is None else dict(self.current_value),
            "current_display": self.current_display,
            "progression_values": [value.to_dict() for value in self.progression_values],
            "previous_display": self.previous_display,
            "actual_value": None if self.actual_value is None else dict(self.actual_value),
            "actual_display": self.actual_display,
            "actual_candidate_record_ids": list(self.actual_candidate_record_ids),
            "actual_period_id": self.actual_period_id,
            "actual_knowledge_date": self.actual_knowledge_date,
            "actual_source_document_ids": list(self.actual_source_document_ids),
            "progress_value": None if self.progress_value is None else dict(self.progress_value),
            "progress_display": self.progress_display,
            "progress_candidate_record_ids": list(self.progress_candidate_record_ids),
            "progress_period_id": self.progress_period_id,
            "progress_knowledge_date": self.progress_knowledge_date,
            "progress_source_document_ids": list(self.progress_source_document_ids),
            "version_state": self.version_state,
            "status_code_at_update": self.status_code_at_update,
            "status_at_update": self.status_at_update,
            "change_type": self.change_type,
            "comparison_reason_code": self.comparison_reason_code,
            "investor_reason_code": self.investor_reason_code,
            "investor_reason_display": self.investor_reason_display,
            "event_id": self.event_id,
            "event_date": self.event_date,
            "stated_in_period_id": self.stated_in_period_id,
            "stated_in_display": self.stated_in_display,
            "current_source_document_ids": list(self.current_source_document_ids),
            "predecessor_source_document_ids": list(self.predecessor_source_document_ids),
            "source_summary": self.source_summary,
            "lineage_digest": self.lineage_digest,
            "parity_locator": self.parity_locator,
        }


@dataclass(frozen=True, slots=True)
class ProductBlockV2:
    block_id: str
    title: str
    block_state: str
    rows: tuple[ProductRowV2, ...]

    def __post_init__(self) -> None:
        if self.block_id not in BLOCK_ORDER:
            raise PromiseProgressProductV2Error(f"Unknown Product@2 block {self.block_id!r}.")
        if not self.rows and self.block_state not in {"no_open_guidance", "assessment_unavailable"}:
            raise PromiseProgressProductV2Error("A normal Product@2 block cannot be an empty shell.")
        if any(row.block_id != self.block_id for row in self.rows):
            raise PromiseProgressProductV2Error("A Product@2 row is attached to the wrong block.")
        if self.block_state == "assessment_unavailable":
            if len(self.rows) != 1 or self.rows[0].row_kind != "assessment_unavailable":
                raise PromiseProgressProductV2Error(
                    "The unavailable credibility state must be one typed investor row."
                )
        elif self.block_state == "no_open_guidance":
            if self.block_id != OPEN_BLOCK_ID or self.rows:
                raise PromiseProgressProductV2Error(
                    "Only Open Guidance may expose a typed zero-row no-guidance state."
                )
        elif not self.rows:
            raise PromiseProgressProductV2Error("An eligible Product@2 block cannot be empty.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "block_id": self.block_id,
            "title": self.title,
            "block_state": self.block_state,
            "rows": [row.to_dict() for row in self.rows],
        }


def _validate_row_eligibility(row: ProductRowV2) -> None:
    """Fail closed on the versioned investor-row eligibility contract."""

    if not row.row_id or not row.block_id or not row.row_kind:
        raise PromiseProgressProductV2Error("An investor row lacks canonical identity.")
    if row.row_kind == "open_guidance":
        eligible = (
            row.current_value is not None
            and bool(row.current_display)
            and bool(row.horizon_period_id)
            and row.version_state == "Current"
            and row.status_code_at_update == "open"
            and row.status_at_update == "Open"
            and bool(row.current_source_document_ids)
        )
    elif row.row_kind == "guidance_progression":
        eligible = (
            bool(row.progression_values)
            or row.actual_value is not None
            or bool(row.investor_reason_code)
        ) and bool(
            row.metric_id
            and row.horizon_period_id
            and row.status_code_at_update
            and row.status_at_update
        )
    elif row.row_kind == "timeline_version":
        eligible = (
            row.current_value is not None
            and bool(row.current_display)
            and bool(row.event_id and row.event_date)
            and bool(row.stated_in_period_id and row.stated_in_display)
            and bool(row.version_state and row.change_type)
            and bool(row.status_code_at_update and row.status_at_update)
            and bool(row.current_source_document_ids)
        )
    elif row.row_kind == "assessment_unavailable":
        eligible = (
            row.block_id == CREDIBILITY_BLOCK_ID
            and row.investor_reason_code == "assessment_unavailable"
            and bool(row.investor_reason_display)
            and row.version_state == "Needs Review"
            and row.status_code_at_update == "needs_review"
            and row.status_at_update == "Needs Review"
        )
    else:  # protected by the closed row-kind vocabulary above
        eligible = False
    if row.eligible != eligible or not eligible:
        raise PromiseProgressProductV2Error(
            f"Product@2 row {row.row_id!r} fails its closed investor-eligibility contract."
        )


@dataclass(frozen=True, slots=True)
class PromiseProgressProductV2:
    product_type: str
    product_version: str
    product_id: str
    company_id: str
    knowledge_cutoff: str
    source_set_id: str
    coverage_state: str
    coverage_notice: str
    block_order: tuple[str, ...]
    disclosure_events: tuple[DisclosureEventV2, ...]
    blocks: tuple[ProductBlockV2, ...]
    ownership_statement: str

    def __post_init__(self) -> None:
        if self.product_type != PRODUCT_TYPE or self.product_version != PRODUCT_VERSION:
            raise PromiseProgressProductV2Error("Product@2 has the wrong closed version identity.")
        if self.product_id != _product_id(self.company_id):
            raise PromiseProgressProductV2Error(
                "Product@2 identity must derive exactly from the validated package company."
            )
        if self.coverage_state not in COVERAGE_STATES:
            raise PromiseProgressProductV2Error(f"Unknown coverage state {self.coverage_state!r}.")
        if self.block_order != BLOCK_ORDER or tuple(block.block_id for block in self.blocks) != BLOCK_ORDER:
            raise PromiseProgressProductV2Error("Workbook order must be inherited from Product@2.")
        rows = tuple(row for block in self.blocks for row in block.rows)
        row_ids = [row.row_id for row in rows]
        if len(row_ids) != len(set(row_ids)):
            raise PromiseProgressProductV2Error("Product@2 row identities are not unique.")
        event_ids = [event.event_id for event in self.disclosure_events]
        if len(event_ids) != len(set(event_ids)):
            raise PromiseProgressProductV2Error("Disclosure-event identities are not unique.")
        known_events = set(event_ids)
        if any(row.event_id is not None and row.event_id not in known_events for row in rows):
            raise PromiseProgressProductV2Error("A timeline row references an unknown disclosure event.")
        timeline = next(block for block in self.blocks if block.block_id == TIMELINE_BLOCK_ID)
        expected = sorted(
            timeline.rows,
            key=lambda row: (
                -(date.fromisoformat(str(row.event_date)).toordinal()),
                str(row.event_id),
                METRIC_ORDER.get(str(row.metric_id), 999),
                row.row_id,
            ),
        )
        if list(timeline.rows) != expected:
            raise PromiseProgressProductV2Error("Timeline order is not event-date-descending product order.")
        contexts_by_event: dict[str, set[tuple[str | None, str]]] = {}
        for row in timeline.rows:
            contexts_by_event.setdefault(str(row.event_id), set()).add(
                (row.stated_in_period_id, row.stated_in_display)
            )
        if any(len(contexts) != 1 for contexts in contexts_by_event.values()):
            raise PromiseProgressProductV2Error(
                "A disclosure event cannot carry conflicting reporting/update contexts."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "product_type": self.product_type,
            "product_version": self.product_version,
            "product_id": self.product_id,
            "company_id": self.company_id,
            "knowledge_cutoff": self.knowledge_cutoff,
            "source_set_id": self.source_set_id,
            "coverage_state": self.coverage_state,
            "coverage_notice": self.coverage_notice,
            "block_order": list(self.block_order),
            "disclosure_events": [event.to_dict() for event in self.disclosure_events],
            "blocks": [block.to_dict() for block in self.blocks],
            "ownership_statement": self.ownership_statement,
        }


def serialize_promise_progress_product_v2(product: PromiseProgressProductV2) -> bytes:
    return _canonical_json_bytes(product.to_dict())


def promise_progress_product_v2_sha256(product: PromiseProgressProductV2) -> str:
    return hashlib.sha256(serialize_promise_progress_product_v2(product)).hexdigest()


def _format_decimal(value: str, *, places: int | None = None) -> str:
    number = Decimal(value)
    if places is not None:
        return f"{number:.{places}f}"
    normalized = format(number.normalize(), "f")
    return "0" if normalized == "-0" else normalized


def display_value(value: Mapping[str, Any], *, unit_id: str) -> str:
    kind = str(value.get("kind"))
    percent = unit_id == "unit:core:percent@1"
    per_share = unit_id == "unit:core:currency-per-share@1"
    currency_million = unit_id == "unit:core:currency-million@1"
    shares_million = unit_id == "unit:core:shares-million@1"

    def atom(raw: str) -> str:
        if per_share:
            return f"${_format_decimal(raw, places=2)}"
        if currency_million:
            return f"${_format_decimal(raw)}m"
        if shares_million:
            return f"{_format_decimal(raw)}m shares"
        suffix = "%" if percent else ""
        return f"{_format_decimal(raw)}{suffix}"

    if kind == "range":
        return f"{atom(str(value['low']))}–{atom(str(value['high']))}"
    if kind == "approximate":
        return f"~{atom(str(value['value']))}"
    if kind == "bound":
        operator = {"gte": "≥", "lte": "≤", "gt": ">", "lt": "<"}.get(str(value["operator"]))
        if operator is None:
            raise PromiseProgressProductV2Error(f"Unknown bound operator {value['operator']!r}.")
        return f"{operator}{atom(str(value['value']))}"
    if kind == "exact":
        return atom(str(value["value"]))
    if kind == "qualitative":
        band = str(value.get("normalized_band"))
        if band == "negative-mid-single-digits":
            return "Down mid-single digits"
        text = " ".join(str(value.get("text") or "").split())
        if not text:
            raise PromiseProgressProductV2Error("A qualitative guidance value lacks display text.")
        return text[0].upper() + text[1:]
    raise PromiseProgressProductV2Error(f"Unknown Product@2 value form {kind!r}.")


def _interval(value: Mapping[str, Any]) -> tuple[Decimal, Decimal] | None:
    kind = value.get("kind")
    if kind == "range":
        return Decimal(str(value["low"])), Decimal(str(value["high"]))
    if kind in {"exact", "approximate"}:
        point = Decimal(str(value["value"]))
        return point, point
    if kind == "bound":
        return None
    return None


def classify_change(
    current: Mapping[str, Any], predecessor: Mapping[str, Any] | None
) -> tuple[str, str]:
    if predecessor is None:
        return "Initial", "origin"
    if dict(current) == dict(predecessor):
        return "Reaffirmed", "same_typed_value"
    prior_kind = str(predecessor.get("kind"))
    current_kind = str(current.get("kind"))
    if prior_kind == "range" and current_kind == "bound" and current.get("operator") == "gte":
        return "Range → minimum", "value_form_changed"
    if prior_kind == "range" and current_kind == "approximate":
        return "Range → approximate", "value_form_changed"
    if prior_kind == "qualitative" and current_kind == "range":
        return "Qualitative → range", "value_form_changed"
    if prior_kind == "range" and current_kind == "qualitative":
        return "Range → qualitative", "value_form_changed"
    if prior_kind == current_kind == "range":
        prior_low = Decimal(str(predecessor["low"]))
        prior_high = Decimal(str(predecessor["high"]))
        current_low = Decimal(str(current["low"]))
        current_high = Decimal(str(current["high"]))
        lower_delta = current_low.compare(prior_low)
        upper_delta = current_high.compare(prior_high)
        if lower_delta > 0 and upper_delta > 0:
            return "Range shifted higher", "both_bounds_raised"
        if lower_delta < 0 and upper_delta < 0:
            return "Range shifted lower", "both_bounds_lowered"
        if lower_delta > 0 and upper_delta < 0:
            return "Range narrowed", "bounds_moved_inward"
        if lower_delta < 0 and upper_delta > 0:
            return "Range widened", "bounds_moved_outward"
        if lower_delta > 0:
            return "Lower bound raised", "lower_bound_raised"
        if lower_delta < 0:
            return "Lower bound lowered", "lower_bound_lowered"
        if upper_delta > 0:
            return "Upper bound raised", "upper_bound_raised"
        if upper_delta < 0:
            return "Upper bound lowered", "upper_bound_lowered"
    if prior_kind == current_kind == "bound" and current.get("operator") == predecessor.get(
        "operator"
    ):
        prior_value = Decimal(str(predecessor["value"]))
        current_value = Decimal(str(current["value"]))
        operator = str(current["operator"])
        if operator in {"gte", "gt"} and current_value != prior_value:
            return (
                ("Lower bound raised", "lower_bound_raised")
                if current_value > prior_value
                else ("Lower bound lowered", "lower_bound_lowered")
            )
        if operator in {"lte", "lt"} and current_value != prior_value:
            return (
                ("Upper bound raised", "upper_bound_raised")
                if current_value > prior_value
                else ("Upper bound lowered", "upper_bound_lowered")
            )
    prior_interval = _interval(predecessor)
    current_interval = _interval(current)
    if prior_interval is not None and current_interval is not None:
        if current_interval[0] >= prior_interval[0] and current_interval[1] > prior_interval[1]:
            return "Raised", "direct_increase"
        if current_interval[0] < prior_interval[0] and current_interval[1] <= prior_interval[1]:
            return "Lowered", "direct_decrease"
    return "Updated — not directly comparable", "value_form_changed"


def _source_summary(documents: Iterable[Mapping[str, Any]]) -> str:
    rows = sorted(documents, key=lambda row: (str(row["publication_date"]), str(row["source_document_id"])))
    if not rows:
        return ""
    date_value = date.fromisoformat(str(rows[0]["publication_date"]))
    label = date_value.strftime("%b %d %Y").replace(" 0", " ")
    kinds = {str(row["document_type"]) for row in rows}
    if kinds == {"earnings-release", "earnings-transcript"}:
        return f"{label} release + transcript"
    kind = str(rows[0]["document_type"])
    kind_label = {
        "earnings-release": "release",
        "earnings-transcript": "transcript",
        "business-update": "business update",
    }.get(kind, kind.replace("-", " "))
    return f"{label} {kind_label}"


def _event_indexes(
    package: Mapping[str, Any],
    reviewed_links: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, DisclosureEventV2], dict[str, str]]:
    documents = {str(row["source_document_id"]): row for row in package["source_documents"]}
    by_key = {str(row["document_key"]): row for row in package["source_documents"]}
    same_event_links = [
        row
        for row in reviewed_links
        if row.get("relation_type") == "same-event"
        and row.get("review_state") in {"accepted", "reviewed"}
    ]
    adjacency: dict[str, set[str]] = {key: {key} for key in by_key}
    for link in same_event_links:
        left = str(link.get("from_document_key"))
        right = str(link.get("to_document_key"))
        if left not in by_key or right not in by_key or left == right:
            raise PromiseProgressProductV2Error("A reviewed same-event link has invalid endpoints.")
        adjacency[left].add(right)
        adjacency[right].add(left)
    groups: list[tuple[Mapping[str, Any], ...]] = []
    grouped: set[str] = set()
    visited_keys: set[str] = set()
    for key in sorted(by_key):
        if key in visited_keys:
            continue
        stack = [key]
        component: set[str] = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            stack.extend(sorted(adjacency[current] - component))
        visited_keys.update(component)
        rows = tuple(by_key[value] for value in sorted(component))
        groups.append(rows)
        grouped.update(str(row["source_document_id"]) for row in rows)
    for row in documents.values():
        if str(row["source_document_id"]) not in grouped:
            groups.append((row,))

    events: dict[str, DisclosureEventV2] = {}
    source_to_event: dict[str, str] = {}
    for rows in groups:
        event_date = str(rows[0]["publication_date"])
        if any(str(row["publication_date"]) != event_date for row in rows):
            raise PromiseProgressProductV2Error("A reviewed disclosure event crosses dates.")
        source_ids = tuple(sorted(str(row["source_document_id"]) for row in rows))
        event_id = f"disclosure-event:v2|date={event_date}|sources={_digest(source_ids)[:20]}"
        event = DisclosureEventV2(
            event_id=event_id,
            event_date=event_date,
            source_document_ids=source_ids,
            display_label=_source_summary(rows),
            reviewed_relation_ids=tuple(
                sorted(
                    str(row["link_key"])
                    for row in same_event_links
                    if {str(row["from_document_key"]), str(row["to_document_key"])}
                    <= {str(value["document_key"]) for value in rows}
                )
            ),
        )
        events[event_id] = event
        for source_id in source_ids:
            source_to_event[source_id] = event_id
    return events, source_to_event


def _actual_selection(package: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    observations = {str(row["header"]["record_id"]): row for row in package["observations"]}
    return {
        str(row["business_key"]): observations[str(row["selected_record_id"])]
        for row in package["resolutions"]
        if row["record_type"] == "NumericalFact" and row.get("selected_record_id") in observations
    }


def _matching_actuals(
    package: Mapping[str, Any], *, metric_id: str, period_id: str, as_of_date: str | None = None
) -> tuple[Mapping[str, Any], ...]:
    selected_ids = {
        str(row["selected_record_id"])
        for row in package["resolutions"]
        if row["record_type"] == "NumericalFact" and row.get("selected_record_id") is not None
    }
    rows = [
        row
        for row in package["observations"]
        if row["payload"]["kind"] == "NumericalFact"
        and row["payload"]["metric_id"] == metric_id
        and row["header"]["effective_period_id"] == period_id
        and row["header"]["record_id"] in selected_ids
        and (
            as_of_date is None
            or (
                str(row["header"]["publication_date"]) <= as_of_date
                and str(row["header"]["knowledge_date"]) <= as_of_date
            )
        )
    ]
    return tuple(sorted(rows, key=lambda row: str(row["header"]["record_id"])))


def _record_source_document_ids(
    package: Mapping[str, Any], record: Mapping[str, Any]
) -> tuple[str, ...]:
    occurrences = {
        str(row["evidence_occurrence_id"]): row for row in package["evidence_occurrences"]
    }
    return tuple(
        sorted(
            {
                str(occurrences[str(identity)]["source_document_id"])
                for identity in record["header"]["evidence_occurrence_ids"]
            }
        )
    )


def _selected_actual_result(
    package: Mapping[str, Any],
    record: Mapping[str, Any],
    *,
    candidate_ids: tuple[str, ...],
) -> tuple[
    Mapping[str, Any],
    tuple[str, ...],
    str,
    str,
    tuple[str, ...],
    None,
    str,
]:
    header = record["header"]
    return (
        record["payload"]["value"],
        candidate_ids,
        str(header["effective_period_id"]),
        str(header["knowledge_date"]),
        _record_source_document_ids(package, record),
        None,
        "",
    )


def _actual_for_series(
    package: Mapping[str, Any],
    series: Mapping[str, Any],
    *,
    as_of_date: str | None = None,
) -> tuple[
    Mapping[str, Any] | None,
    tuple[str, ...],
    str | None,
    str | None,
    tuple[str, ...],
    str | None,
    str,
]:
    payload = series["payload"]
    metric_id = str(payload["metric_id"])
    period_id = str(payload["horizon_period_id"])
    unit_id = str(payload["unit_id"])
    candidates = _matching_actuals(
        package, metric_id=metric_id, period_id=period_id, as_of_date=as_of_date
    )
    candidate_ids = tuple(str(row["header"]["record_id"]) for row in candidates)
    if metric_id == "metric:retail:store-remodels-right-sizes@1":
        component_metric_ids = (
            "metric:retail:store-right-sizes@1",
            "metric:retail:store-remodels@1",
        )
        component_rows = []
        for component_metric_id in component_metric_ids:
            matches = [
                row
                for row in _matching_actuals(
                    package,
                    metric_id=component_metric_id,
                    period_id=period_id,
                    as_of_date=as_of_date,
                )
                if row["payload"]["definition_id"]
                == "definition:core:company-reported@1"
                and row["payload"]["basis_id"] == "basis:core:reported@1"
                and row["payload"]["unit_id"] == unit_id
                and row["header"]["dimension_set_id"] == payload["dimension_set_id"]
            ]
            if len(matches) != 1:
                all_ids = tuple(
                    sorted(
                        str(row["header"]["record_id"])
                        for metric in component_metric_ids
                        for row in _matching_actuals(
                            package,
                            metric_id=metric,
                            period_id=period_id,
                            as_of_date=as_of_date,
                        )
                    )
                )
                return (
                    None,
                    all_ids,
                    None,
                    None,
                    (),
                    "comparable_actual_unavailable",
                    NEEDS_REVIEW_REASONS["comparable_actual_unavailable"][1],
                )
            component_rows.append(matches[0])
        component_ids = tuple(
            sorted(str(row["header"]["record_id"]) for row in component_rows)
        )
        source_sets = [set(_record_source_document_ids(package, row)) for row in component_rows]
        if len({frozenset(values) for values in source_sets}) != 1:
            raise PromiseProgressProductV2Error(
                "Remodel/right-size component facts must share reviewed source identity."
            )
        combined = derive_store_remodels_right_sizes(
            component_rows[0]["payload"]["value"],
            component_rows[1]["payload"]["value"],
        )
        return (
            combined,
            component_ids,
            period_id,
            max(str(row["header"]["knowledge_date"]) for row in component_rows),
            tuple(sorted(set().union(*source_sets))),
            None,
            "",
        )
    if metric_id in {
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:share-repurchases@1",
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
    } and candidates:
        reported = [
            row
            for row in candidates
            if row["payload"]["definition_id"] == "definition:core:company-reported@1"
            and row["payload"]["basis_id"] == "basis:core:reported@1"
        ]
        if len(reported) == 1:
            return _selected_actual_result(
                package, reported[0], candidate_ids=candidate_ids
            )
    if metric_id == "metric:core:capital-expenditures@1":
        related = _matching_actuals(
            package,
            metric_id="metric:core:property-equipment-purchases@1",
            period_id=period_id,
            as_of_date=as_of_date,
        )
        related_ids = tuple(str(row["header"]["record_id"]) for row in related)
        eligible_statements = [
            row
            for row in package["observations"]
            if row["payload"]["kind"] == "ManagementStatement"
            and row["payload"]["topic_id"] == CAPEX_EQUIVALENCE_TOPIC_ID
            and row["payload"]["statement_kind"] == "explanation"
            and row["header"]["effective_period_id"] == period_id
            and row["header"]["review_state"] in {"accepted", "reviewed"}
            and (
                as_of_date is None
                or (
                    str(row["header"]["publication_date"]) <= as_of_date
                    and str(row["header"]["knowledge_date"]) <= as_of_date
                )
            )
        ]
        if len(eligible_statements) == 1 and len(related) == 1:
            statement_sources = set(
                _record_source_document_ids(package, eligible_statements[0])
            )
            actual_sources = set(_record_source_document_ids(package, related[0]))
            if statement_sources == actual_sources:
                return _selected_actual_result(
                    package, related[0], candidate_ids=related_ids
                )
        return (
            None,
            related_ids,
            None,
            None,
            (),
            "definition_equivalence_unreviewed",
            NEEDS_REVIEW_REASONS["definition_equivalence_unreviewed"][1],
        )
    return (
        None,
        candidate_ids,
        None,
        None,
        (),
        "comparable_actual_unavailable",
        NEEDS_REVIEW_REASONS["comparable_actual_unavailable"][1],
    )


def classify_timeline_fact_role(
    *,
    period_type: str,
    same_target_fiscal_year: bool,
    eligible_by_event_cutoff: bool,
    progress_semantic: str | None = None,
) -> str:
    """Assign one typed Timeline role without consulting display text or workbook cells."""

    if not same_target_fiscal_year or not eligible_by_event_cutoff:
        return "incompatible"
    if progress_semantic is not None:
        progress_roles = {
            "ytd": "ytd_progress",
            "cumulative": "cumulative_progress",
            "annualized_run_rate": "annualized_run_rate",
            "delta_to_target": "delta_progress",
        }
        role = progress_roles.get(progress_semantic)
        if role is None:
            raise PromiseProgressProductV2Error(
                f"Unknown typed Timeline progress semantic {progress_semantic!r}."
            )
        return role
    if period_type == "quarter":
        return "event_period_actual"
    if period_type == "ytd":
        return "ytd_progress"
    return "incompatible"


@dataclass(frozen=True, slots=True)
class _TimelineFactSelection:
    role_id: str
    value: Mapping[str, Any]
    candidate_record_ids: tuple[str, ...]
    period_id: str
    knowledge_date: str
    source_document_ids: tuple[str, ...]
    display_text: str


def _event_actual_and_progress_for_series(
    package: Mapping[str, Any],
    series: Mapping[str, Any],
    *,
    event_date: str,
    event_source_document_ids: tuple[str, ...],
) -> tuple[_TimelineFactSelection | None, _TimelineFactSelection | None]:
    payload = series["payload"]
    target_period = next(
        row
        for row in package["periods"]
        if row["period_id"] == payload["horizon_period_id"]
    )
    periods = {str(row["period_id"]): row for row in package["periods"]}
    selected_ids = {
        str(row["selected_record_id"])
        for row in package["resolutions"]
        if row["record_type"] == "NumericalFact" and row.get("selected_record_id") is not None
    }
    matches: list[tuple[Mapping[str, Any], str]] = []
    for record in package["observations"]:
        header = record["header"]
        value = record["payload"]
        if (
            value["kind"] != "NumericalFact"
            or str(value["metric_id"]) != str(payload["metric_id"])
            or str(value["definition_id"]) != "definition:core:company-reported@1"
            or str(value["basis_id"]) != "basis:core:reported@1"
            or str(header["record_id"]) not in selected_ids
            or str(header["publication_date"]) > event_date
            or str(header["knowledge_date"]) > event_date
            or str(header["dimension_set_id"]) != str(payload["dimension_set_id"])
        ):
            continue
        sources = _record_source_document_ids(package, record)
        if not set(sources) <= set(event_source_document_ids):
            continue
        period = periods[str(header["effective_period_id"])]
        role = classify_timeline_fact_role(
            period_type=str(period["period_type"]),
            same_target_fiscal_year=(
                int(period["fiscal_year"]) == int(target_period["fiscal_year"])
            ),
            eligible_by_event_cutoff=(str(period["end_date"]) <= event_date),
        )
        if role != "incompatible":
            matches.append((record, role))

    def select(roles: frozenset[str]) -> _TimelineFactSelection | None:
        eligible = [(record, role) for record, role in matches if role in roles]
        if not eligible:
            return None
        eligible.sort(
            key=lambda item: (
                str(periods[str(item[0]["header"]["effective_period_id"])]["end_date"]),
                str(item[0]["header"]["record_id"]),
            )
        )
        latest_end = str(
            periods[str(eligible[-1][0]["header"]["effective_period_id"])]["end_date"]
        )
        latest = [
            item
            for item in eligible
            if str(periods[str(item[0]["header"]["effective_period_id"])]["end_date"])
            == latest_end
        ]
        if len(latest) != 1:
            raise PromiseProgressProductV2Error(
                "An event-time Timeline role does not resolve to one canonical source-backed fact."
            )
        selected, role = latest[0]
        header = selected["header"]
        rendered = display_value(
            selected["payload"]["value"], unit_id=str(payload["unit_id"])
        )
        prefix = {
            "ytd_progress": "YTD",
            "cumulative_progress": "Cumulative",
            "annualized_run_rate": "Run rate",
            "delta_progress": "Delta",
        }.get(role)
        return _TimelineFactSelection(
            role_id=role,
            value=selected["payload"]["value"],
            candidate_record_ids=(str(header["record_id"]),),
            period_id=str(header["effective_period_id"]),
            knowledge_date=str(header["knowledge_date"]),
            source_document_ids=_record_source_document_ids(package, selected),
            display_text=rendered if prefix is None else f"{prefix}: {rendered}",
        )

    actual = select(frozenset({"event_period_actual"}))
    progress = select(
        frozenset(
            {
                "ytd_progress",
                "cumulative_progress",
                "annualized_run_rate",
                "delta_progress",
            }
        )
    )
    if actual is not None and progress is not None and (
        set(actual.candidate_record_ids) & set(progress.candidate_record_ids)
    ):
        raise PromiseProgressProductV2Error(
            "One source fact cannot be assigned to both event-period Actual and Progress."
        )
    return actual, progress


def _status_rule_for_value(value: Mapping[str, Any] | None) -> str:
    if value is None:
        return STATUS_REVIEW_ID
    kind = str(value.get("kind"))
    if kind == "exact":
        return STATUS_POINT_ID
    if kind == "range":
        return STATUS_RANGE_ID
    if kind == "bound":
        return STATUS_MIN_ID if value.get("operator") in {"gt", "gte"} else STATUS_MAX_ID
    if kind == "approximate":
        return STATUS_APPROX_ID
    if kind == "qualitative":
        return STATUS_QUALITATIVE_ID
    return STATUS_REVIEW_ID


def _actual_display_value(value: Mapping[str, Any], *, display_text: str) -> DisplayValue:
    """Translate one selected numerical fact without changing its typed value."""

    kind = str(value.get("kind"))
    if kind == "exact":
        return DisplayValue("exact", display_text, str(value["value"]))
    raise PromiseProgressProductV2Error(
        f"Product@2 outcome assessment does not support non-exact Actual kind {kind!r}."
    )


def _reporting_update_context(
    *,
    event: DisclosureEventV2,
    source_rows: Iterable[Mapping[str, Any]],
    horizon_period: Mapping[str, Any],
) -> tuple[str, str]:
    """Return a typed investor update-period identity for one disclosure event.

    Normal disclosure events use the event's calendar quarter.  A reviewed business
    update issued shortly before an annual horizon closes is explicitly identified as
    the target fiscal year's Q4 pre-release update.  The rule uses typed document and
    period metadata; it does not parse labels or source prose.
    """

    source_rows = tuple(source_rows)
    event_day = date.fromisoformat(event.event_date)
    horizon_end = date.fromisoformat(str(horizon_period["end_date"]))
    fiscal_year = int(horizon_period["fiscal_year"])
    document_types = {str(row["document_type"]) for row in source_rows}
    if (
        "business-update" in document_types
        and event_day <= horizon_end
        and 0 <= (horizon_end - event_day).days <= 45
    ):
        return (
            f"reporting-update:v2|target-fy={fiscal_year}|phase=q4-pre-release",
            f"{fiscal_year}-Q4 pre-release",
        )
    quarter = (event_day.month - 1) // 3 + 1
    return (
        f"reporting-update:v2|calendar={event_day.year}-q{quarter}",
        f"{event_day.year}-Q{quarter}",
    )


def build_promise_progress_product_v2(
    package: Mapping[str, Any], *, source_set_id: str, reviewed_links: Iterable[Mapping[str, Any]] = ()
) -> PromiseProgressProductV2:
    company_id = str(package.get("company_id", "")).strip()
    if not company_id:
        raise PromiseProgressProductV2Error("The source-native package has no company identity.")
    periods = {str(row["period_id"]): row for row in package["periods"]}
    documents = {str(row["source_document_id"]): row for row in package["source_documents"]}
    occurrences = {str(row["evidence_occurrence_id"]): row for row in package["evidence_occurrences"]}
    observations = {str(row["header"]["record_id"]): row for row in package["observations"]}
    selected_by_series = {
        str(row["business_key"]): str(row["selected_record_id"])
        for row in package["resolutions"]
        if row["record_type"] == "GuidanceVersion" and row.get("selected_record_id") is not None
    }
    predecessor_by_record = {
        str(row["from_record_id"]): str(row["to_record_id"])
        for row in package["relations"]
        if row["relation_type"] == "supersedes"
    }
    series_rows = {
        str(row["header"]["entity_id"]): row
        for row in package["entities"]
        if row["payload"]["kind"] == "GuidanceSeries"
    }
    versions_by_series: dict[str, list[Mapping[str, Any]]] = {identity: [] for identity in series_rows}
    for row in observations.values():
        if row["payload"]["kind"] == "GuidanceVersion":
            versions_by_series[str(row["payload"]["guidance_series_id"])].append(row)
    for values in versions_by_series.values():
        values.sort(key=lambda row: (str(row["header"]["publication_date"]), str(row["header"]["record_id"])))

    event_by_id, source_to_event = _event_indexes(package, reviewed_links)

    def source_ids(record: Mapping[str, Any]) -> tuple[str, ...]:
        result = {
            str(occurrences[str(occurrence_id)]["source_document_id"])
            for occurrence_id in record["header"]["evidence_occurrence_ids"]
        }
        return tuple(sorted(result))

    def horizon_label(period_id: str) -> str:
        period = periods[period_id]
        return f"FY{period['fiscal_year']}"

    def version_state(series_id: str, record: Mapping[str, Any]) -> str:
        record_id = str(record["header"]["record_id"])
        if selected_by_series.get(series_id) != record_id:
            return "Superseded"
        period = periods[str(series_rows[series_id]["payload"]["horizon_period_id"])]
        if date.fromisoformat(str(period["end_date"])) < date.fromisoformat(str(package["knowledge_cutoff"])):
            return "Final"
        return "Current"

    def selected_actual_records(
        actual_value: Mapping[str, Any] | None,
        candidate_ids: tuple[str, ...],
        metric_id: str,
    ) -> tuple[Mapping[str, Any], ...]:
        if actual_value is None:
            return ()
        matches = [
            observations[record_id]
            for record_id in candidate_ids
            if observations[record_id]["payload"]["value"] == actual_value
        ]
        if len(matches) == 1:
            return (matches[0],)
        candidate_records = tuple(observations[record_id] for record_id in candidate_ids)
        if metric_id == "metric:retail:store-remodels-right-sizes@1":
            by_metric = {
                str(record["payload"]["metric_id"]): record
                for record in candidate_records
            }
            required = {
                "metric:retail:store-right-sizes@1",
                "metric:retail:store-remodels@1",
            }
            if set(by_metric) == required and derive_store_remodels_right_sizes(
                by_metric["metric:retail:store-right-sizes@1"]["payload"]["value"],
                by_metric["metric:retail:store-remodels@1"]["payload"]["value"],
            ) == actual_value:
                return tuple(by_metric[metric] for metric in sorted(required))
        raise PromiseProgressProductV2Error(
            "A compatible Product@2 Actual does not resolve to one direct fact or one "
            "reviewed typed component derivation."
        )

    def outcome_status(
        *,
        row_key: str,
        metric_id: str,
        target_record: Mapping[str, Any],
        target_period_id: str,
        actual_value: Mapping[str, Any] | None,
        actual_display: str,
        actual_candidate_ids: tuple[str, ...],
        reason_code: str | None,
        as_of_date: str,
    ) -> tuple[str, str, str | None, str]:
        horizon = periods[target_period_id]
        horizon_closed = date.fromisoformat(str(horizon["end_date"])) < date.fromisoformat(
            as_of_date
        )
        target_record_id = str(target_record["header"]["record_id"])
        actual_records = selected_actual_records(
            actual_value, actual_candidate_ids, metric_id
        )
        typed_actual: ActualSelection | None = None
        issue_ids: tuple[str, ...] = ()
        if actual_records:
            headers = tuple(record["header"] for record in actual_records)
            payloads = tuple(record["payload"] for record in actual_records)
            direct = len(actual_records) == 1
            semantic_payload = payloads[0]
            actual_sources = tuple(
                sorted({value for record in actual_records for value in source_ids(record)})
            )
            observation_ids = tuple(
                sorted(str(header["record_id"]) for header in headers)
            )
            occurrence_ids = tuple(
                sorted(
                    {
                        str(value)
                        for header in headers
                        for value in header["evidence_occurrence_ids"]
                    }
                )
            )
            actual_lineage = _digest(
                {
                    "owner": PRODUCT_TYPE,
                    "row_key": row_key,
                    "record_ids": observation_ids,
                    "value": actual_value,
                }
            )
            typed_actual = ActualSelection(
                actual_id=f"actual-selection:promise-progress-v2:{actual_lineage[:24]}@1",
                actual_role_id=ACTUAL_FY_ID,
                semantic_class=ACTUAL_ROLE_SEMANTIC_CLASSES[ACTUAL_FY_ID],
                selection_state="selected",
                canonical_observation_ids=observation_ids,
                semantic_identity=SemanticIdentity(
                    metric_id=(
                        str(semantic_payload["metric_id"]) if direct else metric_id
                    ),
                    definition_id=str(semantic_payload["definition_id"]),
                    basis_id=str(semantic_payload["basis_id"]),
                    unit_id=str(semantic_payload["unit_id"]),
                    dimensions=(),
                ),
                effective_or_fiscal_period_id=target_period_id,
                publication_date=max(str(header["publication_date"]) for header in headers),
                knowledge_date=max(str(header["knowledge_date"]) for header in headers),
                value_form=str(actual_value["kind"]),
                source_occurrence_ids=occurrence_ids,
                source_document_ids=actual_sources,
                display_value=_actual_display_value(actual_value, display_text=actual_display),
                milestone_state=None,
                selection_method_id=(
                    "selection:promise-progress-product-v2:compatible-actual@1"
                    if direct
                    else "selection:promise-progress-product-v2:typed-component-derivation@1"
                ),
                lineage_state="source-backed",
                lineage_digest=actual_lineage,
            )
        elif horizon_closed:
            issue_ids = (reason_code or "comparable_actual_unavailable",)
        target_value = target_record["payload"]["value"]
        if (
            horizon_closed
            and typed_actual is not None
            and not issue_ids
            and str(target_value.get("kind")) == "approximate"
            and target_value.get("tolerance") is None
        ):
            machine = typed_actual.display_value.machine_value
            actual_number = Decimal(
                str(machine if isinstance(machine, str) else machine["value"])
            )
            target_number = Decimal(str(target_value["value"]))
            favorable = FAVORABLE_DIRECTION_BY_METRIC.get(metric_id)
            if actual_number == target_number:
                return "hit", STATUS_LABELS["hit"], None, ""
            if favorable == "higher" and actual_number > target_number:
                return "beat", STATUS_LABELS["beat"], None, ""
            if favorable == "lower" and actual_number < target_number:
                return "beat", STATUS_LABELS["beat"], None, ""
            approximate_reason = (
                "approximate_target_direction_ambiguous"
                if favorable is None
                else "approximate_target_tolerance_unreviewed"
            )
            return (
                "needs_review",
                STATUS_LABELS["needs_review"],
                approximate_reason,
                NEEDS_REVIEW_REASONS[approximate_reason][1],
            )
        rule_id = (
            STATUS_OPEN_ID
            if not horizon_closed
            else STATUS_REVIEW_ID
            if typed_actual is None
            else _status_rule_for_value(target_record["payload"]["value"])
        )
        status = assess_status(
            product_id=_product_id(company_id),
            row_key=row_key,
            rule_id=rule_id,
            target_version_id=target_record_id,
            target_value=target_value,
            actual=typed_actual,
            progress=None,
            ui_as_of_date=as_of_date,
            horizon_closed=horizon_closed,
            target_period_or_horizon_id=target_period_id,
            review_issue_ids=issue_ids,
            favorable_direction=FAVORABLE_DIRECTION_BY_METRIC.get(metric_id),
        )
        outcome_reason_code: str | None = None
        outcome_reason_display = ""
        if status.status_code == "needs_review":
            target_value = target_record["payload"]["value"]
            if (
                str(target_value.get("kind")) == "approximate"
                and target_value.get("tolerance") is None
                and typed_actual is not None
            ):
                outcome_reason_code = "approximate_target_tolerance_unreviewed"
            elif horizon_closed:
                outcome_reason_code = reason_code or "comparable_actual_unavailable"
            if outcome_reason_code is not None:
                outcome_reason_display = NEEDS_REVIEW_REASONS[outcome_reason_code][1]
        return (
            status.status_code,
            status.visible_label,
            outcome_reason_code,
            outcome_reason_display,
        )

    open_rows: list[ProductRowV2] = []
    progression_rows: list[ProductRowV2] = []
    timeline_rows: list[ProductRowV2] = []
    cutoff = date.fromisoformat(str(package["knowledge_cutoff"]))

    for series_id, series in sorted(
        series_rows.items(),
        key=lambda item: (
            -int(periods[str(item[1]["payload"]["horizon_period_id"])]["fiscal_year"]),
            METRIC_ORDER.get(str(item[1]["payload"]["metric_id"]), 999),
            item[0],
        ),
    ):
        payload = series["payload"]
        metric_id = str(payload["metric_id"])
        if metric_id not in METRIC_ORDER:
            continue
        period_id = str(payload["horizon_period_id"])
        period = periods[period_id]
        fiscal_year = int(period["fiscal_year"])
        versions = versions_by_series[series_id]
        if not versions:
            continue
        selected_id = selected_by_series.get(series_id)
        selected = observations.get(str(selected_id)) if selected_id is not None else None
        if selected is None:
            continue
        versions_by_event: dict[str, list[Mapping[str, Any]]] = {}
        for version in versions:
            version_sources = source_ids(version)
            version_event_ids = {source_to_event[source_id] for source_id in version_sources}
            if len(version_event_ids) != 1:
                raise PromiseProgressProductV2Error(
                    "A guidance version crosses reviewed disclosure events."
                )
            versions_by_event.setdefault(next(iter(version_event_ids)), []).append(version)
        product_version_groups: list[dict[str, Any]] = []
        product_group_by_record_id: dict[str, dict[str, Any]] = {}
        for event_id, event_versions in versions_by_event.items():
            event = event_by_id[event_id]
            record_ids = tuple(
                sorted(str(version["header"]["record_id"]) for version in event_versions)
            )
            if len(event_versions) > 1:
                if not event.reviewed_relation_ids:
                    raise PromiseProgressProductV2Error(
                        "Multiple guidance versions share an event without a reviewed relation."
                    )
                if len({_digest(version["payload"]["value"]) for version in event_versions}) != 1:
                    raise PromiseProgressProductV2Error(
                        "A reviewed same-event guidance group contains conflicting values."
                    )
            selected_matches = [
                version
                for version in event_versions
                if str(version["header"]["record_id"]) == selected_id
            ]
            primary = (
                selected_matches[0]
                if selected_matches
                else min(
                    event_versions,
                    key=lambda version: (
                        min(
                            {
                                "earnings-release": 0,
                                "business-update": 1,
                                "earnings-transcript": 2,
                            }.get(str(documents[source_id]["document_type"]), 99)
                            for source_id in source_ids(version)
                        ),
                        str(version["header"]["record_id"]),
                    ),
                )
            )
            group = {
                "event": event,
                "primary": primary,
                "record_ids": record_ids,
                "source_document_ids": event.source_document_ids,
            }
            product_version_groups.append(group)
            for record_id in record_ids:
                product_group_by_record_id[record_id] = group
        product_version_groups.sort(
            key=lambda group: (
                str(group["event"].event_date),
                str(group["event"].event_id),
                str(group["primary"]["header"]["record_id"]),
            )
        )
        selected_group = product_group_by_record_id[str(selected_id)]
        unit_id = str(payload["unit_id"])
        (
            actual_value,
            actual_candidate_ids,
            actual_period_id,
            actual_knowledge_date,
            actual_source_document_ids,
            reason_code,
            reason_display,
        ) = _actual_for_series(package, series)
        actual_display = "" if actual_value is None else display_value(actual_value, unit_id=unit_id)
        (
            selected_status_code,
            selected_status_label,
            selected_reason_code,
            selected_reason_display,
        ) = outcome_status(
            row_key=f"{series_id}|selected",
            metric_id=metric_id,
            target_record=selected,
            target_period_id=period_id,
            actual_value=actual_value,
            actual_display=actual_display,
            actual_candidate_ids=actual_candidate_ids,
            reason_code=reason_code,
            as_of_date=str(package["knowledge_cutoff"]),
        )
        selected_sources = tuple(selected_group["source_document_ids"])
        selected_source_rows = tuple(documents[source_id] for source_id in selected_sources)
        lineage = {
            "series_id": series_id,
            "version_ids": [row["header"]["record_id"] for row in versions],
            "actual_candidate_record_ids": list(actual_candidate_ids),
            "actual_period_id": actual_period_id,
            "actual_knowledge_date": actual_knowledge_date,
            "actual_source_document_ids": list(actual_source_document_ids),
        }
        if date.fromisoformat(str(period["end_date"])) >= cutoff:
            open_rows.append(
                ProductRowV2(
                    row_id=f"pprow:v2|block=open-guidance|series={series_id}",
                    block_id=OPEN_BLOCK_ID,
                    row_kind="open_guidance",
                    eligible=True,
                    group_id=period_id,
                    metric_id=metric_id,
                    metric_label=METRIC_LABELS[metric_id],
                    horizon_period_id=period_id,
                    horizon_label=horizon_label(period_id),
                    current_value=selected["payload"]["value"],
                    current_display=display_value(selected["payload"]["value"], unit_id=unit_id),
                    progression_values=(),
                    previous_display="",
                    actual_value=None,
                    actual_display="",
                    actual_candidate_record_ids=(),
                    actual_period_id=None,
                    actual_knowledge_date=None,
                    actual_source_document_ids=(),
                    progress_value=None,
                    progress_display="",
                    progress_candidate_record_ids=(),
                    progress_period_id=None,
                    progress_knowledge_date=None,
                    progress_source_document_ids=(),
                    version_state="Current",
                    status_code_at_update=selected_status_code,
                    status_at_update=selected_status_label,
                    change_type=None,
                    comparison_reason_code=None,
                    investor_reason_code=None,
                    investor_reason_display="",
                    event_id=source_to_event[selected_sources[0]],
                    event_date=str(selected["header"]["publication_date"]),
                    stated_in_period_id=None,
                    stated_in_display="",
                    current_source_document_ids=selected_sources,
                    predecessor_source_document_ids=(),
                    source_summary=_source_summary(selected_source_rows),
                    lineage_digest=_digest(lineage),
                )
            )
        else:
            progression_values = tuple(
                ProductVersionValueV2(
                    version_record_id=str(group["primary"]["header"]["record_id"]),
                    publication_date=str(group["event"].event_date),
                    canonical_value=group["primary"]["payload"]["value"],
                    display_text=display_value(
                        group["primary"]["payload"]["value"], unit_id=unit_id
                    ),
                    source_document_ids=tuple(group["source_document_ids"]),
                )
                for group in product_version_groups
            )
            progression_rows.append(
                ProductRowV2(
                    row_id=f"pprow:v2|block=guidance-progression|series={series_id}",
                    block_id=PROGRESSION_BLOCK_ID,
                    row_kind="guidance_progression",
                    eligible=bool(progression_values or actual_value is not None or reason_code is not None),
                    group_id=period_id,
                    metric_id=metric_id,
                    metric_label=METRIC_LABELS[metric_id],
                    horizon_period_id=period_id,
                    horizon_label=horizon_label(period_id),
                    current_value=selected["payload"]["value"],
                    current_display=display_value(selected["payload"]["value"], unit_id=unit_id),
                    progression_values=progression_values,
                    previous_display="",
                    actual_value=actual_value,
                    actual_display=actual_display,
                    actual_candidate_record_ids=actual_candidate_ids,
                    actual_period_id=actual_period_id,
                    actual_knowledge_date=actual_knowledge_date,
                    actual_source_document_ids=actual_source_document_ids,
                    progress_value=None,
                    progress_display="",
                    progress_candidate_record_ids=(),
                    progress_period_id=None,
                    progress_knowledge_date=None,
                    progress_source_document_ids=(),
                    version_state="Final",
                    status_code_at_update=selected_status_code,
                    status_at_update=selected_status_label,
                    change_type=None,
                    comparison_reason_code=None,
                    investor_reason_code=selected_reason_code,
                    investor_reason_display=selected_reason_display,
                    event_id=None,
                    event_date=None,
                    stated_in_period_id=None,
                    stated_in_display="",
                    current_source_document_ids=selected_sources,
                    predecessor_source_document_ids=(),
                    source_summary=_source_summary(selected_source_rows),
                    lineage_digest=_digest(lineage),
                )
            )

        for group in product_version_groups:
            row = group["primary"]
            record_id = str(row["header"]["record_id"])
            predecessor_id = predecessor_by_record.get(record_id)
            while predecessor_id in set(group["record_ids"]):
                predecessor_id = predecessor_by_record.get(str(predecessor_id))
            predecessor_group = (
                None
                if predecessor_id is None
                else product_group_by_record_id.get(predecessor_id)
            )
            predecessor = (
                None
                if predecessor_id is None
                else predecessor_group["primary"]
                if predecessor_group is not None
                else observations.get(predecessor_id)
            )
            change_type, machine_reason = classify_change(
                row["payload"]["value"], None if predecessor is None else predecessor["payload"]["value"]
            )
            current_sources = tuple(group["source_document_ids"])
            predecessor_sources = (
                ()
                if predecessor is None
                else tuple(predecessor_group["source_document_ids"])
                if predecessor_group is not None
                else source_ids(predecessor)
            )
            source_rows = tuple(documents[source_id] for source_id in current_sources)
            event = group["event"]
            event_id = event.event_id
            stated_in_period_id, stated_in_display = _reporting_update_context(
                event=event,
                source_rows=source_rows,
                horizon_period=period,
            )
            (
                status_actual_value,
                status_actual_candidate_ids,
                _status_actual_period_id,
                _status_actual_knowledge_date,
                _status_actual_source_document_ids,
                status_actual_reason_code,
                _status_actual_reason_display,
            ) = _actual_for_series(package, series, as_of_date=event.event_date)
            status_actual_display = (
                ""
                if status_actual_value is None
                else display_value(status_actual_value, unit_id=unit_id)
            )
            event_actual, event_progress = _event_actual_and_progress_for_series(
                package,
                series,
                event_date=event.event_date,
                event_source_document_ids=current_sources,
            )
            (
                timeline_status_code,
                timeline_status_label,
                timeline_reason_code,
                timeline_reason_display,
            ) = outcome_status(
                row_key=f"{series_id}|version={record_id}",
                metric_id=metric_id,
                target_record=row,
                target_period_id=period_id,
                actual_value=status_actual_value,
                actual_display=status_actual_display,
                actual_candidate_ids=status_actual_candidate_ids,
                reason_code=status_actual_reason_code,
                as_of_date=event.event_date,
            )
            timeline_rows.append(
                ProductRowV2(
                    row_id=f"pprow:v2|block=revision-timeline|version={record_id}",
                    block_id=TIMELINE_BLOCK_ID,
                    row_kind="timeline_version",
                    eligible=True,
                    group_id=event_id,
                    metric_id=metric_id,
                    metric_label=METRIC_LABELS[metric_id],
                    horizon_period_id=period_id,
                    horizon_label=horizon_label(period_id),
                    current_value=row["payload"]["value"],
                    current_display=display_value(row["payload"]["value"], unit_id=unit_id),
                    progression_values=(),
                    previous_display=(
                        ""
                        if predecessor is None
                        else display_value(predecessor["payload"]["value"], unit_id=unit_id)
                    ),
                    actual_value=None if event_actual is None else event_actual.value,
                    actual_display="" if event_actual is None else event_actual.display_text,
                    actual_candidate_record_ids=(
                        () if event_actual is None else event_actual.candidate_record_ids
                    ),
                    actual_period_id=None if event_actual is None else event_actual.period_id,
                    actual_knowledge_date=(
                        None if event_actual is None else event_actual.knowledge_date
                    ),
                    actual_source_document_ids=(
                        () if event_actual is None else event_actual.source_document_ids
                    ),
                    progress_value=None if event_progress is None else event_progress.value,
                    progress_display=(
                        "" if event_progress is None else event_progress.display_text
                    ),
                    progress_candidate_record_ids=(
                        () if event_progress is None else event_progress.candidate_record_ids
                    ),
                    progress_period_id=(
                        None if event_progress is None else event_progress.period_id
                    ),
                    progress_knowledge_date=(
                        None if event_progress is None else event_progress.knowledge_date
                    ),
                    progress_source_document_ids=(
                        () if event_progress is None else event_progress.source_document_ids
                    ),
                    version_state=version_state(series_id, row),
                    status_code_at_update=timeline_status_code,
                    status_at_update=timeline_status_label,
                    change_type=change_type,
                    comparison_reason_code=machine_reason,
                    investor_reason_code=timeline_reason_code,
                    investor_reason_display=timeline_reason_display,
                    event_id=event_id,
                    event_date=event.event_date,
                    stated_in_period_id=stated_in_period_id,
                    stated_in_display=stated_in_display,
                    current_source_document_ids=current_sources,
                    predecessor_source_document_ids=predecessor_sources,
                    source_summary=_source_summary(source_rows),
                    lineage_digest=_digest(
                        {
                            "record_id": record_id,
                            "same_event_record_ids": group["record_ids"],
                            "predecessor_record_id": predecessor_id,
                            "status_code_at_update": timeline_status_code,
                            "status_actual_candidate_record_ids": status_actual_candidate_ids,
                            "actual_role_id": (
                                None if event_actual is None else event_actual.role_id
                            ),
                            "actual_candidate_record_ids": (
                                () if event_actual is None else event_actual.candidate_record_ids
                            ),
                            "actual_period_id": (
                                None if event_actual is None else event_actual.period_id
                            ),
                            "actual_knowledge_date": (
                                None if event_actual is None else event_actual.knowledge_date
                            ),
                            "actual_source_document_ids": (
                                () if event_actual is None else event_actual.source_document_ids
                            ),
                            "progress_role_id": (
                                None if event_progress is None else event_progress.role_id
                            ),
                            "progress_candidate_record_ids": (
                                () if event_progress is None else event_progress.candidate_record_ids
                            ),
                            "progress_period_id": (
                                None if event_progress is None else event_progress.period_id
                            ),
                            "progress_knowledge_date": (
                                None if event_progress is None else event_progress.knowledge_date
                            ),
                            "progress_source_document_ids": (
                                () if event_progress is None else event_progress.source_document_ids
                            ),
                            "stated_in_period_id": stated_in_period_id,
                            "current_sources": current_sources,
                            "predecessor_sources": predecessor_sources,
                        }
                    ),
                )
            )

    open_rows.sort(key=lambda row: (METRIC_ORDER.get(str(row.metric_id), 999), row.row_id))
    progression_rows.sort(
        key=lambda row: (
            -int(periods[str(row.horizon_period_id)]["fiscal_year"]),
            METRIC_ORDER.get(str(row.metric_id), 999),
            row.row_id,
        )
    )
    timeline_rows.sort(
        key=lambda row: (
            -(date.fromisoformat(str(row.event_date)).toordinal()),
            str(row.event_id),
            METRIC_ORDER.get(str(row.metric_id), 999),
            row.row_id,
        )
    )

    credibility_row = ProductRowV2(
        row_id="pprow:v2|block=management-credibility|state=assessment-unavailable",
        block_id=CREDIBILITY_BLOCK_ID,
        row_kind="assessment_unavailable",
        eligible=True,
        group_id=None,
        metric_id=None,
        metric_label="Management credibility assessment",
        horizon_period_id=None,
        horizon_label="",
        current_value=None,
        current_display="",
        progression_values=(),
        previous_display="",
        actual_value=None,
        actual_display="",
        actual_candidate_record_ids=(),
        actual_period_id=None,
        actual_knowledge_date=None,
        actual_source_document_ids=(),
        progress_value=None,
        progress_display="",
        progress_candidate_record_ids=(),
        progress_period_id=None,
        progress_knowledge_date=None,
        progress_source_document_ids=(),
        version_state="Needs Review",
        status_code_at_update="needs_review",
        status_at_update="Needs Review",
        change_type=None,
        comparison_reason_code=None,
        investor_reason_code="assessment_unavailable",
        investor_reason_display="Management credibility assessment pending reviewed evidence.",
        event_id=None,
        event_date=None,
        stated_in_period_id=None,
        stated_in_display="",
        current_source_document_ids=(),
        predecessor_source_document_ids=(),
        source_summary="",
        lineage_digest=_digest({"state": "assessment_unavailable"}),
    )

    relevant_events = {
        str(row.event_id) for row in timeline_rows if row.event_id is not None
    }
    events = tuple(
        sorted(
            (event_by_id[event_id] for event_id in relevant_events),
            key=lambda event: (-date.fromisoformat(event.event_date).toordinal(), event.event_id),
        )
    )
    open_years = {
        int(periods[str(row.horizon_period_id)]["fiscal_year"])
        for row in open_rows
        if row.horizon_period_id is not None
    }
    open_title = (
        f"{next(iter(open_years))} Open Guidance" if len(open_years) == 1 else "Open Guidance"
    )
    blocks = (
        ProductBlockV2(
            block_id=CREDIBILITY_BLOCK_ID,
            title="Management Credibility Scorecard",
            block_state="assessment_unavailable",
            rows=(credibility_row,),
        ),
        ProductBlockV2(
            block_id=PROGRESSION_BLOCK_ID,
            title="Guidance Progression",
            block_state="populated",
            rows=tuple(progression_rows),
        ),
        ProductBlockV2(
            block_id=OPEN_BLOCK_ID,
            title=open_title,
            block_state="populated" if open_rows else "no_open_guidance",
            rows=tuple(open_rows),
        ),
        ProductBlockV2(
            block_id=TIMELINE_BLOCK_ID,
            title="Quarterly Guidance Timeline / Revision Log",
            block_state="populated",
            rows=tuple(timeline_rows),
        ),
    )
    return PromiseProgressProductV2(
        product_type=PRODUCT_TYPE,
        product_version=PRODUCT_VERSION,
        product_id=_product_id(company_id),
        company_id=company_id,
        knowledge_cutoff=str(package["knowledge_cutoff"]),
        source_set_id=source_set_id,
        coverage_state="partial_reviewed_source_coverage",
        coverage_notice="Reviewed history is included; some full-year basis comparisons still need review.",
        block_order=BLOCK_ORDER,
        disclosure_events=events,
        blocks=blocks,
        ownership_statement=(
            "The validated source-native package owns evidence and economics; Product@2 owns "
            "investor eligibility, semantic order, version labels, and presentation-safe source roles."
        ),
    )


def build_product_v2_shadow(
    product: PromiseProgressProductV2, package: Mapping[str, Any]
) -> dict[str, Any]:
    rows = [row for block in product.blocks for row in block.rows]
    return {
        "shadow_type": "PromiseProgressProductShadow@2",
        "shadow_version": "2.0.0-candidate",
        "product_id": product.product_id,
        "product_sha256": promise_progress_product_v2_sha256(product),
        "source_package_sha256": _digest(package),
        "row_lineage": [
            {
                "row_id": row.row_id,
                "lineage_digest": row.lineage_digest,
                "version_state": row.version_state,
                "status_code_at_update": row.status_code_at_update,
                "status_at_update": row.status_at_update,
                "horizon_period_id": row.horizon_period_id,
                "stated_in_period_id": row.stated_in_period_id,
                "stated_in_display": row.stated_in_display,
                "event_id": row.event_id,
                "event_date": row.event_date,
                "current_source_document_ids": list(row.current_source_document_ids),
                "predecessor_source_document_ids": list(row.predecessor_source_document_ids),
                "actual_candidate_record_ids": list(row.actual_candidate_record_ids),
                "actual_period_id": row.actual_period_id,
                "actual_knowledge_date": row.actual_knowledge_date,
                "actual_source_document_ids": list(row.actual_source_document_ids),
                "progress_candidate_record_ids": list(row.progress_candidate_record_ids),
                "progress_period_id": row.progress_period_id,
                "progress_knowledge_date": row.progress_knowledge_date,
                "progress_source_document_ids": list(row.progress_source_document_ids),
                "investor_reason_code": row.investor_reason_code,
            }
            for row in rows
        ],
        "source_package": package,
    }


def serialize_product_v2_shadow(value: Mapping[str, Any]) -> bytes:
    return _canonical_json_bytes(value)
