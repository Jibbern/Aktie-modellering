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
import re
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
from .serialization import serialize_package


PRODUCT_TYPE = "PromiseProgressProduct@2"
PRODUCT_VERSION = "2.0.0-candidate"
SUCCESSOR_PRODUCT_VERSION = "2.1.0-candidate"
SUPPORTED_PRODUCT_VERSIONS = frozenset({PRODUCT_VERSION, SUCCESSOR_PRODUCT_VERSION})
EVIDENCE_FOUNDATION_ID = "evidence-foundation:anf:product-v2-1-successor@1"
EVIDENCE_FOUNDATION_SOURCE_SET_ID = (
    "source-set:anf:reviewed-evidence-foundation-successor@4"
)
CAPEX_EQUIVALENCE_TOPIC_ID = (
    "topic:core:capital-expenditure-property-equipment-equivalence@1"
)

Q4_ADD_FY_MINUS_YTD_RULE_ID = "derivation:promise-progress:q4-fy-minus-ytd@1"
Q4_ADD_FY_MINUS_QUARTERS_RULE_ID = (
    "derivation:promise-progress:q4-fy-minus-q1-q2-q3@1"
)
Q4_MARGIN_FROM_COMPONENTS_RULE_ID = (
    "derivation:promise-progress:q4-margin-from-components@1"
)
Q4_GROWTH_FROM_AMOUNTS_RULE_ID = (
    "derivation:promise-progress:q4-growth-from-current-prior-amounts@1"
)
STORE_COMPONENT_COMBINATION_RULE_ID = (
    "derivation:promise-progress:store-remodels-right-sizes-from-components@1"
)
NET_STORE_OPENINGS_RULE_ID = (
    "derivation:promise-progress:net-store-openings-from-components@1"
)
PERIOD_YTD_MINUS_PRIOR_RULE_ID = (
    "derivation:promise-progress:quarter-ytd-minus-prior-ytd@1"
)
YTD_GROWTH_FROM_AMOUNTS_RULE_ID = (
    "derivation:promise-progress:ytd-growth-from-current-prior-amounts@1"
)
YTD_MARGIN_FROM_COMPONENTS_RULE_ID = (
    "derivation:promise-progress:ytd-margin-from-components@1"
)
DERIVATION_RULE_IDS = frozenset(
    {
        Q4_ADD_FY_MINUS_YTD_RULE_ID,
        Q4_ADD_FY_MINUS_QUARTERS_RULE_ID,
        Q4_MARGIN_FROM_COMPONENTS_RULE_ID,
        Q4_GROWTH_FROM_AMOUNTS_RULE_ID,
        STORE_COMPONENT_COMBINATION_RULE_ID,
        NET_STORE_OPENINGS_RULE_ID,
        PERIOD_YTD_MINUS_PRIOR_RULE_ID,
        YTD_GROWTH_FROM_AMOUNTS_RULE_ID,
        YTD_MARGIN_FROM_COMPONENTS_RULE_ID,
    }
)
Q4_DERIVATION_RULE_IDS = frozenset(
    DERIVATION_RULE_IDS
    - {
        PERIOD_YTD_MINUS_PRIOR_RULE_ID,
        YTD_GROWTH_FROM_AMOUNTS_RULE_ID,
        YTD_MARGIN_FROM_COMPONENTS_RULE_ID,
    }
)
Q4_ADDITIVE_METRIC_IDS = frozenset(
    {
        "metric:core:property-equipment-purchases@1",
        "metric:core:share-repurchases@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels@1",
        "metric:retail:store-right-sizes@1",
    }
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
        "Outcome reported",
    }
)
GUIDANCE_UPDATE_ROW_KIND = "guidance_update"
PERIOD_RESULT_ROW_KIND = "period_result"
HORIZON_OUTCOME_ROW_KIND = "horizon_outcome"
ROW_KINDS = frozenset(
    {
        "open_guidance",
        "guidance_progression",
        "timeline_version",
        "timeline_outcome",
        GUIDANCE_UPDATE_ROW_KIND,
        PERIOD_RESULT_ROW_KIND,
        HORIZON_OUTCOME_ROW_KIND,
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
    "metric:anf:operating-income@1": 10,
    "metric:anf:tariff-impact@1": 11,
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
    "metric:anf:operating-income@1": "Operating income",
    "metric:anf:tariff-impact@1": "Tariff impact",
}

# Product metrics may intentionally use a ticker-facing identity while the reviewed
# Evidence Foundation retains the cross-company canonical metric.  Keep that bridge
# explicit and reusable so projection, completeness search, and lineage ownership all
# resolve the same semantic candidates without duplicating facts.
FOUNDATION_METRIC_COMPATIBILITY: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "metric:anf:operating-income@1": (
            "metric:core:operating-income@1",
        ),
    }
)


def compatible_foundation_metric_ids(metric_id: str) -> tuple[str, ...]:
    """Return the closed canonical metric identities compatible with a Product metric."""

    return tuple(
        dict.fromkeys(
            (metric_id, *FOUNDATION_METRIC_COMPATIBILITY.get(metric_id, ()))
        )
    )

FAVORABLE_DIRECTION_BY_METRIC = MappingProxyType(
    {
        "metric:core:revenue-growth@1": "higher",
        "metric:core:operating-margin@1": "higher",
        "metric:core:net-income-per-diluted-share@1": "higher",
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
        "qualitative_target_non_comparable": (
            "C",
            "The reviewed qualitative target cannot be deterministically compared with the compatible Actual.",
        ),
        "point_target_tolerance_unreviewed": (
            "C",
            "The reviewed point plan differs from Actual and has no reviewed tolerance or favorable-direction rule.",
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
    progression_slot: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_value", _freeze_mapping(self.canonical_value))
        if not self.version_record_id or not self.publication_date or not self.display_text:
            raise PromiseProgressProductV2Error("A progression value needs stable identity, date, and display.")
        if len(set(self.source_document_ids)) != len(self.source_document_ids):
            raise PromiseProgressProductV2Error("A progression value has duplicate source identities.")
        if self.progression_slot is not None and self.progression_slot not in {
            "initial",
            "q1",
            "q2",
            "q3",
            "q4",
        }:
            raise PromiseProgressProductV2Error(
                f"Unknown annual progression slot {self.progression_slot!r}."
            )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "version_record_id": self.version_record_id,
            "publication_date": self.publication_date,
            "canonical_value": dict(self.canonical_value),
            "display_text": self.display_text,
            "source_document_ids": list(self.source_document_ids),
        }
        if self.progression_slot is not None:
            result["progression_slot"] = self.progression_slot
        return result


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
    previous_value: Mapping[str, Any] | None = None
    actual_derivation_rule_id: str | None = None
    actual_derivation_input_record_ids: tuple[str, ...] = ()
    actual_derivation_support_record_ids: tuple[str, ...] = ()
    progress_derivation_rule_id: str | None = None
    progress_derivation_input_record_ids: tuple[str, ...] = ()
    progress_derivation_support_record_ids: tuple[str, ...] = ()
    unit_id: str | None = None
    status_target_guidance_version_id: str | None = None
    status_actual_candidate_record_ids: tuple[str, ...] = ()
    status_actual_period_id: str | None = None
    status_actual_knowledge_date: str | None = None
    status_actual_source_document_ids: tuple[str, ...] = ()
    status_actual_basis_id: str | None = None
    status_actual_unit_id: str | None = None
    status_rule_id: str | None = None

    def __post_init__(self) -> None:
        if self.row_kind not in ROW_KINDS or self.row_kind in INELIGIBLE_ROW_KINDS:
            raise PromiseProgressProductV2Error(f"Unknown or excluded Product@2 row kind {self.row_kind!r}.")
        if not self.eligible:
            raise PromiseProgressProductV2Error("Product@2 serializes investor-eligible rows only.")
        if self.block_id not in BLOCK_ORDER or not self.row_id:
            raise PromiseProgressProductV2Error("A Product@2 row has an invalid owner or identity.")
        if self.current_value is not None:
            object.__setattr__(self, "current_value", _freeze_mapping(self.current_value))
        if self.previous_value is not None:
            object.__setattr__(self, "previous_value", _freeze_mapping(self.previous_value))
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
            self.status_actual_candidate_record_ids,
            self.status_actual_source_document_ids,
        )
        if any(len(values) != len(set(values)) for values in identities):
            raise PromiseProgressProductV2Error("Row lineage identity sets must be unique.")
        if (
            self.actual_derivation_rule_id is None
            and self.progress_derivation_rule_id is None
            and set(self.actual_candidate_record_ids) & set(self.progress_candidate_record_ids)
        ):
            raise PromiseProgressProductV2Error(
                "One source fact cannot populate both Timeline Actual and Progress."
            )
        if self.actual_derivation_rule_id is None:
            if self.actual_derivation_input_record_ids:
                raise PromiseProgressProductV2Error(
                    "A direct or missing Actual cannot carry derivation inputs."
                )
            if self.actual_derivation_support_record_ids and self.actual_value is None:
                raise PromiseProgressProductV2Error(
                    "A missing Actual cannot carry definition/support lineage."
                )
        else:
            if self.actual_derivation_rule_id not in DERIVATION_RULE_IDS:
                raise PromiseProgressProductV2Error(
                    f"Unknown Actual derivation rule {self.actual_derivation_rule_id!r}."
                )
            if self.actual_value is None or len(self.actual_derivation_input_record_ids) < 2:
                raise PromiseProgressProductV2Error(
                    "A derived Actual requires a value and at least two input facts."
                )
            if (
                len(set(self.actual_derivation_input_record_ids))
                != len(self.actual_derivation_input_record_ids)
                or len(set(self.actual_derivation_support_record_ids))
                != len(self.actual_derivation_support_record_ids)
                or not set(self.actual_derivation_input_record_ids)
                <= set(self.actual_candidate_record_ids)
            ):
                raise PromiseProgressProductV2Error(
                    "Derived Actual lineage must be unique and contained in Actual candidates."
                )
        if self.progress_derivation_rule_id is None:
            if self.progress_derivation_input_record_ids:
                raise PromiseProgressProductV2Error(
                    "Direct or missing Progress cannot carry derivation inputs."
                )
            if self.progress_derivation_support_record_ids and self.progress_value is None:
                raise PromiseProgressProductV2Error(
                    "Missing Progress cannot carry definition/support lineage."
                )
        else:
            if self.progress_derivation_rule_id not in DERIVATION_RULE_IDS:
                raise PromiseProgressProductV2Error(
                    f"Unknown Progress derivation rule {self.progress_derivation_rule_id!r}."
                )
            if self.progress_value is None or len(self.progress_derivation_input_record_ids) < 2:
                raise PromiseProgressProductV2Error(
                    "Derived Progress requires a value and at least two input facts."
                )
            if (
                len(set(self.progress_derivation_input_record_ids))
                != len(self.progress_derivation_input_record_ids)
                or len(set(self.progress_derivation_support_record_ids))
                != len(self.progress_derivation_support_record_ids)
                or not set(self.progress_derivation_input_record_ids)
                <= set(self.progress_candidate_record_ids)
            ):
                raise PromiseProgressProductV2Error(
                    "Derived Progress lineage must be unique and contained in Progress candidates."
                )
        if self.unit_id is not None and not self.unit_id.startswith("unit:"):
            raise PromiseProgressProductV2Error("A Product@2 unit identity is malformed.")
        status_lineage_values = (
            self.status_target_guidance_version_id,
            self.status_actual_period_id,
            self.status_actual_knowledge_date,
            self.status_actual_basis_id,
            self.status_actual_unit_id,
            self.status_rule_id,
        )
        has_status_lineage = any(value is not None for value in status_lineage_values) or bool(
            self.status_actual_candidate_record_ids or self.status_actual_source_document_ids
        )
        if self.row_kind == "horizon_outcome" or (
            self.row_kind == "guidance_progression" and has_status_lineage
        ):
            if not all(value is not None for value in status_lineage_values) or not (
                self.status_actual_candidate_record_ids
                and self.status_actual_source_document_ids
                and self.actual_value is not None
            ):
                raise PromiseProgressProductV2Error(
                    "A horizon outcome requires explicit target, Actual, rule, and source lineage."
                )
            if (
                self.status_actual_candidate_record_ids != self.actual_candidate_record_ids
                or self.status_actual_period_id != self.actual_period_id
                or self.status_actual_knowledge_date != self.actual_knowledge_date
                or self.status_actual_source_document_ids != self.actual_source_document_ids
                or self.status_actual_unit_id != self.unit_id
                or self.status_actual_period_id != self.horizon_period_id
            ):
                raise PromiseProgressProductV2Error(
                    "Visible horizon-outcome Actual and Status evidence must be identical."
                )
        elif has_status_lineage:
            raise PromiseProgressProductV2Error(
                "Only a horizon-outcome row may carry closed outcome-status evidence."
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
        if self.row_kind in {
            "timeline_version",
            "timeline_outcome",
            "guidance_update",
            "period_result",
            "horizon_outcome",
        } and self.event_date is not None:
            event_day = date.fromisoformat(self.event_date)
            for role, knowledge_date in (
                ("Actual", self.actual_knowledge_date),
                ("Progress", self.progress_knowledge_date),
            ):
                if knowledge_date is not None and date.fromisoformat(knowledge_date) > event_day:
                    raise PromiseProgressProductV2Error(
                        f"Timeline {role} leaks evidence after its disclosure-event cutoff."
                    )
            if self.status_actual_knowledge_date is not None and date.fromisoformat(
                self.status_actual_knowledge_date
            ) > event_day:
                raise PromiseProgressProductV2Error(
                    "Timeline Status leaks evidence after its disclosure-event cutoff."
                )
        _validate_row_eligibility(self)

    def to_dict(self) -> dict[str, Any]:
        result = {
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
        # The accepted 2.0 golden remains byte-for-byte reproducible.  Successor-only
        # typed fields are serialized only when they carry information.
        if self.previous_value is not None:
            result["previous_value"] = dict(self.previous_value)
        if (
            self.actual_derivation_rule_id is not None
            or self.actual_derivation_support_record_ids
        ):
            result["actual_derivation_rule_id"] = self.actual_derivation_rule_id
            result["actual_derivation_input_record_ids"] = list(
                self.actual_derivation_input_record_ids
            )
            result["actual_derivation_support_record_ids"] = list(
                self.actual_derivation_support_record_ids
            )
        if (
            self.progress_derivation_rule_id is not None
            or self.progress_derivation_support_record_ids
        ):
            result["progress_derivation_rule_id"] = self.progress_derivation_rule_id
            result["progress_derivation_input_record_ids"] = list(
                self.progress_derivation_input_record_ids
            )
            result["progress_derivation_support_record_ids"] = list(
                self.progress_derivation_support_record_ids
            )
        if self.unit_id is not None:
            result["unit_id"] = self.unit_id
        if self.status_target_guidance_version_id is not None:
            result["status_target_guidance_version_id"] = (
                self.status_target_guidance_version_id
            )
            result["status_actual_candidate_record_ids"] = list(
                self.status_actual_candidate_record_ids
            )
            result["status_actual_period_id"] = self.status_actual_period_id
            result["status_actual_knowledge_date"] = self.status_actual_knowledge_date
            result["status_actual_source_document_ids"] = list(
                self.status_actual_source_document_ids
            )
            result["status_actual_basis_id"] = self.status_actual_basis_id
            result["status_actual_unit_id"] = self.status_actual_unit_id
            result["status_rule_id"] = self.status_rule_id
        return result


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
    elif row.row_kind == "timeline_outcome":
        eligible = (
            row.block_id == TIMELINE_BLOCK_ID
            and row.current_value is not None
            and bool(row.current_display)
            and bool(row.event_id and row.event_date)
            and bool(row.stated_in_period_id and row.stated_in_display)
            and row.version_state == "Final"
            and row.change_type == "Outcome reported"
            and bool(row.status_code_at_update and row.status_at_update)
            and bool(row.current_source_document_ids)
            and bool(row.predecessor_source_document_ids)
        )
    elif row.row_kind == "guidance_update":
        eligible = (
            row.block_id == TIMELINE_BLOCK_ID
            and row.current_value is not None
            and bool(row.current_display)
            and bool(row.event_id and row.event_date)
            and bool(row.stated_in_period_id and row.stated_in_display)
            and bool(row.version_state and row.change_type)
            and row.status_code_at_update == "open"
            and row.status_at_update == "Open"
            and row.actual_value is None
            and row.progress_value is None
            and bool(row.current_source_document_ids)
        )
    elif row.row_kind == "period_result":
        eligible = (
            row.block_id == TIMELINE_BLOCK_ID
            and row.current_value is None
            and not row.current_display
            and not row.previous_display
            and row.change_type is None
            and row.version_state is None
            and row.status_code_at_update is None
            and row.status_at_update is None
            and bool(row.event_id and row.event_date)
            and bool(row.stated_in_period_id and row.stated_in_display)
            and (row.actual_value is not None or row.progress_value is not None)
            and bool(row.current_source_document_ids)
        )
    elif row.row_kind == "horizon_outcome":
        eligible = (
            row.block_id == TIMELINE_BLOCK_ID
            and row.current_value is None
            and not row.current_display
            and not row.previous_display
            and row.change_type is None
            and row.version_state == "Final"
            and bool(row.event_id and row.event_date)
            and bool(row.stated_in_period_id and row.stated_in_display)
            and row.actual_value is not None
            and row.progress_value is None
            and bool(row.status_code_at_update and row.status_at_update)
            and bool(row.current_source_document_ids)
            and not row.predecessor_source_document_ids
            and bool(row.status_target_guidance_version_id)
            and bool(row.status_actual_candidate_record_ids)
            and bool(row.status_rule_id)
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
        if (
            self.product_type != PRODUCT_TYPE
            or self.product_version not in SUPPORTED_PRODUCT_VERSIONS
        ):
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
        if self.product_version == PRODUCT_VERSION and any(
            row.row_kind == "timeline_outcome" or row.unit_id is not None
            for row in rows
        ):
            raise PromiseProgressProductV2Error(
                "The immutable Product@2 2.0 golden cannot contain successor fields."
            )
        if self.product_version == SUCCESSOR_PRODUCT_VERSION and any(
            row.metric_id is not None and row.unit_id is None for row in rows
        ):
            raise PromiseProgressProductV2Error(
                "Every successor economic row must retain its typed unit identity."
            )
        expected = sorted(
            timeline.rows,
            key=lambda row: (
                -(date.fromisoformat(str(row.event_date)).toordinal()),
                str(row.event_id),
                METRIC_ORDER.get(str(row.metric_id), 999),
                (
                    {"guidance_update": 0, "period_result": 1, "horizon_outcome": 2}.get(
                        row.row_kind, 9
                    )
                    if self.product_version == SUCCESSOR_PRODUCT_VERSION
                    else 0
                ),
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
    currency_million = unit_id in {
        "unit:core:currency-million@1",
        "unit:core:currency-millions@1",
    }
    shares_million = unit_id in {
        "unit:core:shares-million@1",
        "unit:core:shares-millions@1",
    }
    basis_points = unit_id == "unit:core:basis-points@1"

    display_places = (
        int(value["display_decimals"])
        if value.get("display_decimals") is not None
        else None
    )

    def atom(raw: str) -> str:
        if per_share:
            return f"${_format_decimal(raw, places=2)}"
        if currency_million:
            return f"${_format_decimal(raw, places=display_places)}m"
        if shares_million:
            return f"{_format_decimal(raw, places=display_places)}m shares"
        if basis_points:
            return f"{_format_decimal(raw, places=display_places)} bps"
        suffix = "%" if percent else ""
        return f"{_format_decimal(raw, places=display_places)}{suffix}"

    def qualify(rendered: str) -> str:
        direction = value.get("direction")
        if direction in {"up", "down"} and not re.search(
            rf"\b{re.escape(str(direction))}\b", rendered, re.IGNORECASE
        ):
            rendered = f"{str(direction).title()} {rendered}"
        polarity = value.get("impact_polarity")
        if polarity in {"favorable", "unfavorable"} and str(polarity) not in rendered.casefold():
            rendered = f"{rendered} {polarity}"
        return rendered

    if kind == "range":
        return qualify(f"{atom(str(value['low']))}–{atom(str(value['high']))}")
    if kind == "approximate":
        return qualify(f"~{atom(str(value['value']))}")
    if kind == "bound":
        operator = {"gte": "≥", "lte": "≤", "gt": ">", "lt": "<"}.get(str(value["operator"]))
        if operator is None:
            raise PromiseProgressProductV2Error(f"Unknown bound operator {value['operator']!r}.")
        return qualify(f"{operator}{atom(str(value['value']))}")
    if kind == "exact":
        return qualify(atom(str(value["value"])))
    if kind == "qualitative":
        band = str(value.get("normalized_band"))
        if band == "negative-mid-single-digits":
            return "Down mid-single digits"
        text = " ".join(str(value.get("text") or value.get("value") or "").split())
        if not text:
            raise PromiseProgressProductV2Error("A qualitative guidance value lacks display text.")
        return qualify(text[0].upper() + text[1:])
    if kind == "composite":
        text = " ".join(str(value.get("source_text") or "").split())
        if not text:
            raise PromiseProgressProductV2Error("A composite guidance value lacks display text.")
        return qualify(text)
    raise PromiseProgressProductV2Error(f"Unknown Product@2 value form {kind!r}.")


def _interval(value: Mapping[str, Any]) -> tuple[Decimal, Decimal] | None:
    kind = value.get("kind")
    if kind == "range":
        low = Decimal(str(value["low"]))
        high = Decimal(str(value["high"]))
        if value.get("direction") == "down":
            return -high, -low
        return low, high
    if kind in {"exact", "approximate"}:
        point = Decimal(str(value["value"]))
        if value.get("direction") == "down":
            point = -point
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
        prior_interval = _interval(predecessor)
        current_interval = _interval(current)
        assert prior_interval is not None and current_interval is not None
        prior_low, prior_high = prior_interval
        current_low, current_high = current_interval
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


@dataclass(frozen=True, slots=True)
class Q4DerivationResult:
    """One closed, auditable Q4 derivation result.

    The product may use this only for additive flows or for ratios rebuilt from
    compatible underlying components.  A rate, margin, EPS, or weighted average
    is never accepted as an additive input merely because it is numeric.
    """

    value: Mapping[str, Any]
    derivation_rule_id: str
    input_record_ids: tuple[str, ...]
    effective_period_id: str
    knowledge_date: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _freeze_mapping(self.value))
        if self.derivation_rule_id not in Q4_DERIVATION_RULE_IDS:
            raise PromiseProgressProductV2Error("A Q4 derivation uses an unknown rule.")
        if len(self.input_record_ids) < 2 or len(set(self.input_record_ids)) != len(
            self.input_record_ids
        ):
            raise PromiseProgressProductV2Error(
                "A Q4 derivation requires unique, complete input fact identities."
            )


def _exact_fact_number(record: Mapping[str, Any]) -> Decimal:
    payload = record.get("payload", {})
    value = payload.get("value", {})
    if payload.get("kind") != "NumericalFact" or value.get("kind") != "exact":
        raise PromiseProgressProductV2Error(
            "Q4 additive derivation requires exact NumericalFact inputs."
        )
    return Decimal(str(value["value"]))


def _fact_semantic_identity(
    record: Mapping[str, Any],
) -> tuple[str, str, str, str, str, str, str]:
    payload = record["payload"]
    header = record["header"]
    value = payload.get("value", {})
    return (
        str(payload["metric_id"]),
        str(payload["definition_id"]),
        str(payload["basis_id"]),
        str(payload["unit_id"]),
        str(payload.get("currency") or "na"),
        str(value.get("unit") or payload.get("scale") or payload["unit_id"]),
        str(header["dimension_set_id"]),
    )


def _validate_q4_additive_inputs(
    records: tuple[Mapping[str, Any], ...],
    *,
    periods: Mapping[str, Mapping[str, Any]],
    q4_period_id: str,
) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
    if len(records) < 2:
        raise PromiseProgressProductV2Error("Q4 additive derivation lacks inputs.")
    identities = {_fact_semantic_identity(record) for record in records}
    if len(identities) != 1:
        raise PromiseProgressProductV2Error(
            "Q4 additive inputs have incompatible metric, definition, basis, unit, "
            "currency, scale, or scope."
        )
    metric_id = next(iter(identities))[0]
    if metric_id not in Q4_ADDITIVE_METRIC_IDS:
        raise PromiseProgressProductV2Error(
            "Q4 subtraction is forbidden for ratios, rates, EPS, and weighted averages."
        )
    try:
        q4_period = periods[q4_period_id]
        input_periods = tuple(
            periods[str(record["header"]["effective_period_id"])] for record in records
        )
    except KeyError as exc:
        raise PromiseProgressProductV2Error("Q4 derivation references an unknown period.") from exc
    if (
        q4_period.get("period_type") != "quarter"
        or int(q4_period.get("fiscal_quarter", 0)) != 4
        or any(
            int(period["fiscal_year"]) != int(q4_period["fiscal_year"])
            for period in input_periods
        )
        or any(
            period.get("calendar_id") != q4_period.get("calendar_id")
            for period in input_periods
        )
    ):
        raise PromiseProgressProductV2Error(
            "Q4 derivation inputs do not belong to one compatible fiscal year/calendar."
        )
    return q4_period, input_periods


def derive_q4_additive_from_fy_ytd(
    fy_record: Mapping[str, Any],
    ytd_record: Mapping[str, Any],
    *,
    periods: Mapping[str, Mapping[str, Any]],
    q4_period_id: str,
    event_cutoff: str,
) -> Q4DerivationResult:
    """Derive one additive Q4 flow as FY minus compatible 9M/YTD."""

    _q4_period, input_periods = _validate_q4_additive_inputs(
        (fy_record, ytd_record), periods=periods, q4_period_id=q4_period_id
    )
    fy_period, ytd_period = input_periods
    if fy_period.get("period_type") != "annual" or (
        ytd_period.get("period_type") != "ytd"
        or int(ytd_period.get("fiscal_quarter", 0)) != 3
    ):
        raise PromiseProgressProductV2Error(
            "FY-minus-YTD Q4 derivation requires one annual and one Q3 YTD input."
        )
    value = _exact_fact_number(fy_record) - _exact_fact_number(ytd_record)
    if value < 0:
        raise PromiseProgressProductV2Error(
            "An additive Q4 residual cannot be negative under the reviewed flow contract."
        )
    knowledge_date = max(
        str(fy_record["header"]["knowledge_date"]),
        str(ytd_record["header"]["knowledge_date"]),
    )
    if knowledge_date > event_cutoff:
        raise PromiseProgressProductV2Error(
            "A derived Q4 fact cannot use evidence learned after its disclosure event."
        )
    return Q4DerivationResult(
        value={"kind": "exact", "value": _format_decimal(str(value))},
        derivation_rule_id=Q4_ADD_FY_MINUS_YTD_RULE_ID,
        input_record_ids=(
            str(fy_record["header"]["record_id"]),
            str(ytd_record["header"]["record_id"]),
        ),
        effective_period_id=q4_period_id,
        knowledge_date=knowledge_date,
    )


def derive_q4_additive_from_fy_quarters(
    fy_record: Mapping[str, Any],
    quarter_records: tuple[Mapping[str, Any], ...],
    *,
    periods: Mapping[str, Mapping[str, Any]],
    q4_period_id: str,
    event_cutoff: str,
) -> Q4DerivationResult:
    """Derive one additive Q4 flow as FY minus exact Q1, Q2, and Q3 flows."""

    if len(quarter_records) != 3:
        raise PromiseProgressProductV2Error(
            "FY-minus-quarters Q4 derivation requires exactly Q1, Q2, and Q3."
        )
    _q4_period, input_periods = _validate_q4_additive_inputs(
        (fy_record, *quarter_records), periods=periods, q4_period_id=q4_period_id
    )
    fy_period, *quarter_periods = input_periods
    if fy_period.get("period_type") != "annual" or {
        (period.get("period_type"), int(period.get("fiscal_quarter", 0)))
        for period in quarter_periods
    } != {("quarter", 1), ("quarter", 2), ("quarter", 3)}:
        raise PromiseProgressProductV2Error(
            "FY-minus-quarters inputs are not the exact Q1/Q2/Q3 flow set."
        )
    value = _exact_fact_number(fy_record) - sum(
        (_exact_fact_number(record) for record in quarter_records), Decimal("0")
    )
    if value < 0:
        raise PromiseProgressProductV2Error(
            "An additive Q4 residual cannot be negative under the reviewed flow contract."
        )
    knowledge_date = max(
        str(record["header"]["knowledge_date"])
        for record in (fy_record, *quarter_records)
    )
    if knowledge_date > event_cutoff:
        raise PromiseProgressProductV2Error(
            "A derived Q4 fact cannot use evidence learned after its disclosure event."
        )
    return Q4DerivationResult(
        value={"kind": "exact", "value": _format_decimal(str(value))},
        derivation_rule_id=Q4_ADD_FY_MINUS_QUARTERS_RULE_ID,
        input_record_ids=tuple(
            str(record["header"]["record_id"])
            for record in (fy_record, *quarter_records)
        ),
        effective_period_id=q4_period_id,
        knowledge_date=knowledge_date,
    )


def derive_q4_margin_from_components(
    operating_income: Mapping[str, Any], net_sales: Mapping[str, Any]
) -> Mapping[str, Any]:
    """Build a Q4 margin from compatible Q4 amounts, never by subtracting margins."""

    if operating_income.get("kind") != "exact" or net_sales.get("kind") != "exact":
        raise PromiseProgressProductV2Error("Q4 margin components must be exact amounts.")
    denominator = Decimal(str(net_sales["value"]))
    if denominator <= 0:
        raise PromiseProgressProductV2Error("Q4 margin requires positive net sales.")
    value = Decimal(str(operating_income["value"])) / denominator * Decimal("100")
    return {"kind": "exact", "value": _format_decimal(str(value))}


def derive_q4_growth_from_amounts(
    current_sales: Mapping[str, Any], prior_sales: Mapping[str, Any]
) -> Mapping[str, Any]:
    """Build Q4 growth from current/prior Q4 sales amounts only."""

    if current_sales.get("kind") != "exact" or prior_sales.get("kind") != "exact":
        raise PromiseProgressProductV2Error("Q4 growth inputs must be exact amounts.")
    denominator = Decimal(str(prior_sales["value"]))
    if denominator <= 0:
        raise PromiseProgressProductV2Error("Q4 growth requires positive prior-period sales.")
    value = (Decimal(str(current_sales["value"])) / denominator - Decimal("1")) * Decimal(
        "100"
    )
    return {"kind": "exact", "value": _format_decimal(str(value))}


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
    derivation_rule_id: str | None = None
    derivation_input_record_ids: tuple[str, ...] = ()
    derivation_support_record_ids: tuple[str, ...] = ()


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


def _derived_q4_capex_for_series(
    package: Mapping[str, Any],
    series: Mapping[str, Any],
    *,
    event_date: str,
) -> _TimelineFactSelection | None:
    """Return the one reviewed FY2022 capex Q4 residual, or fail closed.

    Later years intentionally remain unavailable because their reviewed sources do
    not establish that property/equipment purchases are definition-equivalent to
    guided capital expenditures.
    """

    payload = series["payload"]
    if str(payload["metric_id"]) != "metric:core:capital-expenditures@1":
        return None
    periods = {str(row["period_id"]): row for row in package["periods"]}
    target_period = periods[str(payload["horizon_period_id"])]
    fiscal_year = int(target_period["fiscal_year"])
    q4_periods = [
        row
        for row in periods.values()
        if int(row["fiscal_year"]) == fiscal_year
        and row["period_type"] == "quarter"
        and int(row["fiscal_quarter"]) == 4
    ]
    ytd_periods = [
        row
        for row in periods.values()
        if int(row["fiscal_year"]) == fiscal_year
        and row["period_type"] == "ytd"
        and int(row["fiscal_quarter"]) == 3
    ]
    if len(q4_periods) != 1 or len(ytd_periods) != 1:
        return None
    annual = _matching_actuals(
        package,
        metric_id="metric:core:property-equipment-purchases@1",
        period_id=str(target_period["period_id"]),
        as_of_date=event_date,
    )
    ytd = _matching_actuals(
        package,
        metric_id="metric:core:property-equipment-purchases@1",
        period_id=str(ytd_periods[0]["period_id"]),
        as_of_date=event_date,
    )
    statements = [
        row
        for row in package["observations"]
        if row["payload"]["kind"] == "ManagementStatement"
        and row["payload"]["topic_id"] == CAPEX_EQUIVALENCE_TOPIC_ID
        and row["payload"]["statement_kind"] == "explanation"
        and row["header"]["effective_period_id"] == target_period["period_id"]
        and row["header"]["review_state"] in {"accepted", "reviewed"}
        and str(row["header"]["knowledge_date"]) <= event_date
    ]
    if len(annual) != 1 or len(ytd) != 1 or len(statements) != 1:
        return None
    annual_sources = set(_record_source_document_ids(package, annual[0]))
    statement_sources = set(_record_source_document_ids(package, statements[0]))
    if annual_sources != statement_sources:
        return None
    derived = derive_q4_additive_from_fy_ytd(
        annual[0],
        ytd[0],
        periods=periods,
        q4_period_id=str(q4_periods[0]["period_id"]),
        event_cutoff=event_date,
    )
    if derived.knowledge_date > event_date:
        raise PromiseProgressProductV2Error(
            "A derived Q4 capex fact leaks evidence after its disclosure event."
        )
    source_document_ids = tuple(
        sorted(
            set(_record_source_document_ids(package, annual[0]))
            | set(_record_source_document_ids(package, ytd[0]))
            | set(_record_source_document_ids(package, statements[0]))
        )
    )
    return _TimelineFactSelection(
        role_id="event_period_actual",
        value=derived.value,
        candidate_record_ids=derived.input_record_ids,
        period_id=derived.effective_period_id,
        knowledge_date=derived.knowledge_date,
        source_document_ids=source_document_ids,
        display_text=display_value(
            derived.value, unit_id=str(payload["unit_id"])
        ),
        derivation_rule_id=derived.derivation_rule_id,
        derivation_input_record_ids=derived.input_record_ids,
        derivation_support_record_ids=(str(statements[0]["header"]["record_id"]),),
    )


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
    if kind == "approximate":
        return DisplayValue(
            "approximate",
            display_text,
            {
                "value": str(value["value"]),
                "qualifier": str(value.get("qualifier") or "around"),
                "tolerance": value.get("tolerance"),
            },
        )
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


@dataclass(frozen=True, slots=True)
class _FoundationFactSelection:
    value: Mapping[str, Any]
    candidate_record_ids: tuple[str, ...]
    period_id: str
    period_key: str
    period_kind: str
    knowledge_date: str
    source_document_ids: tuple[str, ...]
    observation_ids: tuple[str, ...]
    occurrence_ids: tuple[str, ...]
    definition_id: str
    basis_id: str
    unit_id: str
    currency: str | None
    derivation_rule_id: str | None = None
    derivation_input_record_ids: tuple[str, ...] = ()
    derivation_support_record_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _freeze_mapping(self.value))


def _foundation_period_display(period_id: str) -> str:
    marker = period_id.casefold().removeprefix("period:anf:").removesuffix("@1")
    marker = marker.removeprefix("fy")
    if "-ytd-q" in marker:
        year, quarter = marker.split("-ytd-q", 1)
        return f"FY{year} YTD Q{quarter}"
    if "-q" in marker:
        year, quarter = marker.split("-q", 1)
        return f"{year}-Q{quarter}"
    return f"FY{marker}"


def _foundation_stated_display(period_id: str) -> str:
    marker = period_id.casefold().removeprefix("period:anf:").removesuffix("@1")
    marker = marker.removeprefix("fy")
    marker = marker.replace("-q4-pre-release", "-Q4 pre-release")
    marker = marker.replace("-q4-results", "-Q4 results")
    marker = marker.replace("-q3-results", "-Q3 results")
    marker = marker.replace("-q2-results", "-Q2 results")
    marker = marker.replace("-q1-results", "-Q1 results")
    return marker


def _foundation_source_summary(
    source_document_ids: Iterable[str],
    documents: Mapping[str, Mapping[str, Any]],
) -> str:
    rows = [documents[source_id] for source_id in source_document_ids if source_id in documents]
    if not rows:
        return ""
    rows.sort(key=lambda row: (str(row["publication_date"]), str(row["source_document_id"])))
    label = date.fromisoformat(str(rows[0]["publication_date"])).strftime("%b %d %Y")
    label = label.replace(" 0", " ")
    kinds = {str(row["source_type"]) for row in rows}
    kind_labels = {
        "earnings_release": "release",
        "sec_filing": "SEC filing",
        "earnings_call_transcript": "transcript",
        "other_issuer_source": "business update",
        "investor_presentation": "presentation",
    }
    rendered = sorted(kind_labels.get(kind, kind.replace("_", " ")) for kind in kinds)
    return f"{label} {' + '.join(rendered)}"


def normalize_product_unit_id(unit_id: str) -> str:
    return {
        "unit:core:currency-millions@1": "unit:core:currency-million@1",
        "unit:core:shares-millions@1": "unit:core:shares-million@1",
    }.get(unit_id, unit_id)


def _product_unit_id(unit_id: str) -> str:
    return normalize_product_unit_id(unit_id)


def _product_value_unit_id(value: Mapping[str, Any], unit_id: str) -> str:
    """Resolve an explicit scalar value unit before a mixed-series fallback unit."""

    explicit_unit = " ".join(str(value.get("unit") or "").casefold().split())
    if value.get("kind") != "composite" and explicit_unit in {
        "basis point",
        "basis points",
        "bp",
        "bps",
    }:
        return "unit:core:basis-points@1"
    return _product_unit_id(unit_id)


def _foundation_guidance_display(
    value: Mapping[str, Any], *, metric_id: str, unit_id: str
) -> str:
    """Render one compact investor label without changing its canonical value."""

    rendered = display_value(value, unit_id=unit_id)
    if value.get("kind") == "qualitative":
        prefixes = {
            "metric:core:revenue-growth@1": "Net sales ",
            "metric:core:operating-margin@1": "Operating margin ",
        }
        prefix = prefixes.get(metric_id)
        if prefix and rendered.casefold().startswith(prefix.casefold()):
            rendered = rendered[len(prefix) :]
            rendered = rendered[:1].upper() + rendered[1:]
        rendered = rendered.replace("-single-digits", " single digits")
        rendered = rendered.replace("-double-digits", " double digits")
        rendered = rendered.replace("Store count remain steady", "Store count steady")
    return rendered


def _foundation_period_metadata(
    package: Mapping[str, Any], foundation: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    periods = {str(row["period_id"]): dict(row) for row in package["periods"]}
    extra = {
        "period:anf:fy2026-q1@1": {
            "period_id": "period:anf:fy2026-q1@1",
            "period_type": "quarter",
            "fiscal_year": 2026,
            "fiscal_quarter": 1,
            "start_date": "2026-02-01",
            "end_date": "2026-05-02",
            "calendar_id": "calendar:anf:fiscal@1",
        },
        "period:anf:fy2026-ytd-q1@1": {
            "period_id": "period:anf:fy2026-ytd-q1@1",
            "period_type": "ytd",
            "fiscal_year": 2026,
            "fiscal_quarter": 1,
            "start_date": "2026-02-01",
            "end_date": "2026-05-02",
            "calendar_id": "calendar:anf:fiscal@1",
        },
        "period:anf:fy2026-q2@1": {
            "period_id": "period:anf:fy2026-q2@1",
            "period_type": "quarter",
            "fiscal_year": 2026,
            "fiscal_quarter": 2,
            "start_date": "2026-05-03",
            "end_date": "2026-08-01",
            "calendar_id": "calendar:anf:fiscal@1",
        },
    }
    periods.update({key: value for key, value in extra.items() if key not in periods})
    for fiscal_year in range(2022, 2027):
        annual_id = f"period:anf:fy{fiscal_year}@1"
        for fiscal_quarter in (1, 2, 3):
            quarter_id = f"period:anf:fy{fiscal_year}-q{fiscal_quarter}@1"
            ytd_id = f"period:anf:fy{fiscal_year}-ytd-q{fiscal_quarter}@1"
            if annual_id in periods and quarter_id in periods and ytd_id not in periods:
                periods[ytd_id] = {
                    "period_id": ytd_id,
                    "period_type": "ytd",
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": fiscal_quarter,
                    "start_date": periods[annual_id]["start_date"],
                    "end_date": periods[quarter_id]["end_date"],
                    "calendar_id": periods[annual_id]["calendar_id"],
                }
    relevant_metrics = set(METRIC_LABELS) | {
        "metric:core:property-equipment-purchases@1",
        "metric:retail:store-remodels@1",
        "metric:retail:store-right-sizes@1",
    }
    used_ids = {
        str(row["period_id"])
        for row in foundation["canonical_facts"]
        if str(row["metric_id"]) in relevant_metrics
        and str(row["period_kind"]) in {"annual", "quarter", "ytd"}
        and str(row["period_key"]).startswith(
            ("FY2022", "FY2023", "FY2024", "FY2025", "FY2026")
        )
    } | {
        str(row["horizon_period_id"]) for row in foundation["quarter_guidance_versions"]
    }
    missing = sorted(period_id for period_id in used_ids if period_id not in periods)
    if missing:
        raise PromiseProgressProductV2Error(
            f"Evidence Foundation references periods without a Product calendar: {missing}."
        )
    return periods


def _physical_occurrence_signature(observation: Mapping[str, Any]) -> tuple[str, ...]:
    locator = observation.get("locator") or {}
    coordinate = (
        locator.get("exact_position")
        or locator.get("source_coordinate")
        or locator.get("node_path")
        or locator.get("fact_id")
        or locator.get("a1_range")
        or locator.get("locator_key")
        or observation.get("occurrence_id")
    )
    excerpt_hash = locator.get("excerpt_sha256") or observation.get("excerpt_sha256") or ""
    return (
        str(observation.get("source_document_id") or ""),
        str(locator.get("locator_kind") or ""),
        str(coordinate or ""),
        str(excerpt_hash),
    )


def _foundation_direct_selection(
    foundation: Mapping[str, Any],
    *,
    metric_id: str,
    period_id: str,
    as_of_date: str,
) -> _FoundationFactSelection | None:
    facts = [
        row
        for row in foundation["canonical_facts"]
        if str(row["metric_id"]) == metric_id
        and str(row["period_id"]) == period_id
        and str(row["dimension_set_id"]) == "dimset:anf:total-company@1"
    ]
    if len(facts) > 1:
        raise PromiseProgressProductV2Error(
            f"Foundation fact {metric_id}/{period_id} is not semantically unique."
        )
    if not facts:
        return None
    fact = facts[0]
    observations = {
        str(row["observation_id"]): row for row in foundation["canonical_observations"]
    }
    eligible = [
        observations[observation_id]
        for observation_id in fact["observation_ids"]
        if observation_id in observations
        and str(observations[observation_id]["knowledge_date"]) <= as_of_date
    ]
    if not eligible:
        return None
    directness_rank = {
        "direct_exact": 0,
        "direct_range": 1,
        "direct_approximate": 2,
        "direct_composite": 3,
        "exact_same_metric_derivation": 4,
        "component_based_derivation": 5,
        "bounded_rounding_derivation": 6,
    }
    eligible.sort(
        key=lambda row: (
            str(row["knowledge_date"]),
            directness_rank.get(str(row.get("semantic_directness")), 99),
            int(row.get("source_authority_tier", 99)),
            str(row["observation_id"]),
        )
    )
    selected = eligible[0]
    derivation_rule_id = (
        None
        if selected.get("derivation_rule_id") is None
        else str(selected["derivation_rule_id"])
    )
    derivation_inputs = tuple(
        str(value) for value in selected.get("derivation_input_record_ids", ())
    )
    derivation_support = tuple(
        str(value) for value in selected.get("derivation_support_record_ids", ())
    )
    candidate_record_ids = tuple(
        sorted({str(fact["canonical_fact_id"]), *derivation_inputs})
    )
    selected_source_ids = tuple(
        sorted(
            str(value)
            for value in (
                selected.get("source_document_ids")
                or [selected["source_document_id"]]
            )
        )
    )
    selected_observation_ids = tuple(
        sorted(
            {
                str(selected["observation_id"]),
                *(
                    str(value)
                    for value in selected.get("derivation_input_observation_ids", ())
                ),
            }
        )
    )
    return _FoundationFactSelection(
        value=fact["canonical_value"],
        candidate_record_ids=candidate_record_ids,
        period_id=period_id,
        period_key=str(fact["period_key"]),
        period_kind=str(fact["period_kind"]),
        knowledge_date=str(selected["knowledge_date"]),
        source_document_ids=selected_source_ids,
        observation_ids=selected_observation_ids,
        occurrence_ids=(str(selected["occurrence_id"]),),
        definition_id=str(fact["definition_id"]),
        basis_id=str(fact["basis_id"]),
        unit_id=_product_value_unit_id(
            fact["canonical_value"], str(fact["unit_id"])
        ),
        currency=(None if fact.get("currency") is None else str(fact["currency"])),
        derivation_rule_id=derivation_rule_id,
        derivation_input_record_ids=derivation_inputs,
        derivation_support_record_ids=derivation_support,
    )


def _foundation_actual_for_metric(
    foundation: Mapping[str, Any],
    *,
    metric_id: str,
    period_id: str,
    as_of_date: str,
) -> _FoundationFactSelection | None:
    for compatible_metric_id in compatible_foundation_metric_ids(metric_id):
        direct = _foundation_direct_selection(
            foundation,
            metric_id=compatible_metric_id,
            period_id=period_id,
            as_of_date=as_of_date,
        )
        if direct is not None:
            return direct
    if metric_id == "metric:core:capital-expenditures@1":
        fact = _foundation_direct_selection(
            foundation,
            metric_id="metric:core:property-equipment-purchases@1",
            period_id=period_id,
            as_of_date=as_of_date,
        )
        relations = [
            row
            for row in foundation["definition_relations"]
            if str(row["period_id"]) == period_id
            and str(row["relation_type"]) == "reviewed-definition-equivalence"
            and str(row["knowledge_date"]) <= as_of_date
        ]
        if fact is None or len(relations) != 1:
            return None
        relation = relations[0]
        return dataclasses.replace(
            fact,
            knowledge_date=max(fact.knowledge_date, str(relation["knowledge_date"])),
            source_document_ids=tuple(
                sorted(
                    set(fact.source_document_ids)
                    | {str(relation["source_document_id"])}
                )
            ),
            definition_id="definition:anf:company-guided-capex@1",
            derivation_support_record_ids=(str(relation["relation_id"]),),
        )
    if metric_id == "metric:retail:store-remodels-right-sizes@1":
        components = tuple(
            _foundation_direct_selection(
                foundation,
                metric_id=component_metric,
                period_id=period_id,
                as_of_date=as_of_date,
            )
            for component_metric in (
                "metric:retail:store-remodels@1",
                "metric:retail:store-right-sizes@1",
            )
        )
        if any(component is None for component in components):
            return None
        remodels, right_sizes = components
        assert remodels is not None and right_sizes is not None
        if (
            remodels.basis_id != right_sizes.basis_id
            or remodels.unit_id != right_sizes.unit_id
            or remodels.currency != right_sizes.currency
        ):
            raise PromiseProgressProductV2Error(
                "Store remodel/right-size components have incompatible typed identities."
            )
        value = derive_store_remodels_right_sizes(remodels.value, right_sizes.value)
        inputs = tuple(sorted(remodels.candidate_record_ids + right_sizes.candidate_record_ids))
        return _FoundationFactSelection(
            value=value,
            candidate_record_ids=inputs,
            period_id=period_id,
            period_key=remodels.period_key,
            period_kind=remodels.period_kind,
            knowledge_date=max(remodels.knowledge_date, right_sizes.knowledge_date),
            source_document_ids=tuple(
                sorted(set(remodels.source_document_ids) | set(right_sizes.source_document_ids))
            ),
            observation_ids=tuple(
                sorted(set(remodels.observation_ids) | set(right_sizes.observation_ids))
            ),
            occurrence_ids=tuple(
                sorted(set(remodels.occurrence_ids) | set(right_sizes.occurrence_ids))
            ),
            definition_id="definition:core:company-reported@1",
            basis_id=remodels.basis_id,
            unit_id=remodels.unit_id,
            currency=remodels.currency,
            derivation_rule_id=STORE_COMPONENT_COMBINATION_RULE_ID,
            derivation_input_record_ids=inputs,
        )
    if metric_id == "metric:retail:net-store-openings@1":
        openings = _foundation_direct_selection(
            foundation,
            metric_id="metric:retail:store-openings@1",
            period_id=period_id,
            as_of_date=as_of_date,
        )
        closures = _foundation_direct_selection(
            foundation,
            metric_id="metric:retail:store-closures-count@1",
            period_id=period_id,
            as_of_date=as_of_date,
        )
        if openings is None or closures is None:
            return None
        if openings.unit_id != closures.unit_id or openings.basis_id != closures.basis_id:
            raise PromiseProgressProductV2Error(
                "Net-opening components have incompatible typed identities."
            )
        value = {
            "kind": "exact",
            "value": _format_decimal(
                str(Decimal(str(openings.value["value"])) - Decimal(str(closures.value["value"])))
            ),
        }
        inputs = tuple(sorted(openings.candidate_record_ids + closures.candidate_record_ids))
        return _FoundationFactSelection(
            value=value,
            candidate_record_ids=inputs,
            period_id=period_id,
            period_key=openings.period_key,
            period_kind=openings.period_kind,
            knowledge_date=max(openings.knowledge_date, closures.knowledge_date),
            source_document_ids=tuple(
                sorted(set(openings.source_document_ids) | set(closures.source_document_ids))
            ),
            observation_ids=tuple(
                sorted(set(openings.observation_ids) | set(closures.observation_ids))
            ),
            occurrence_ids=tuple(
                sorted(set(openings.occurrence_ids) | set(closures.occurrence_ids))
            ),
            definition_id="definition:core:company-reported@1",
            basis_id=openings.basis_id,
            unit_id=openings.unit_id,
            currency=openings.currency,
            derivation_rule_id=NET_STORE_OPENINGS_RULE_ID,
            derivation_input_record_ids=inputs,
        )
    return None


def _foundation_q4_selection(
    foundation: Mapping[str, Any],
    periods: Mapping[str, Mapping[str, Any]],
    *,
    metric_id: str,
    fiscal_year: int,
    as_of_date: str,
) -> _FoundationFactSelection | None:
    period_id = f"period:anf:fy{fiscal_year}-q4@1"
    direct = _foundation_actual_for_metric(
        foundation,
        metric_id=metric_id,
        period_id=period_id,
        as_of_date=as_of_date,
    )
    if direct is not None:
        return direct

    annual_id = f"period:anf:fy{fiscal_year}@1"
    ytd_id = f"period:anf:fy{fiscal_year}-ytd-q3@1"

    def require_additive_identity(
        annual: _FoundationFactSelection,
        ytd: _FoundationFactSelection,
        *,
        expected_definition: str,
    ) -> None:
        if (
            annual.definition_id != expected_definition
            or ytd.definition_id != expected_definition
            or annual.basis_id != ytd.basis_id
            or annual.unit_id != ytd.unit_id
            or annual.currency != ytd.currency
            or periods[annual_id]["calendar_id"] != periods[ytd_id]["calendar_id"]
            or periods[annual_id]["calendar_id"] != periods[period_id]["calendar_id"]
            or int(periods[annual_id]["fiscal_year"]) != fiscal_year
            or int(periods[ytd_id]["fiscal_year"]) != fiscal_year
        ):
            raise PromiseProgressProductV2Error(
                "Foundation Q4 inputs fail definition/basis/unit/scale/currency/"
                "scope/fiscal-year/fiscal-calendar identity."
            )

    def residual(component_metric_id: str) -> _FoundationFactSelection | None:
        annual = _foundation_direct_selection(
            foundation,
            metric_id=component_metric_id,
            period_id=annual_id,
            as_of_date=as_of_date,
        )
        ytd = _foundation_direct_selection(
            foundation,
            metric_id=component_metric_id,
            period_id=ytd_id,
            as_of_date=as_of_date,
        )
        if annual is None or ytd is None:
            return None
        require_additive_identity(
            annual,
            ytd,
            expected_definition=annual.definition_id,
        )
        if annual.value.get("kind") != "exact" or ytd.value.get("kind") != "exact":
            return None
        result_number = Decimal(str(annual.value["value"])) - Decimal(
            str(ytd.value["value"])
        )
        inputs = tuple(
            sorted(set(annual.candidate_record_ids) | set(ytd.candidate_record_ids))
        )
        support = tuple(
            sorted(
                set(annual.derivation_support_record_ids)
                | set(ytd.derivation_support_record_ids)
            )
        )
        return _FoundationFactSelection(
            value={"kind": "exact", "value": _format_decimal(str(result_number))},
            candidate_record_ids=inputs,
            period_id=period_id,
            period_key=f"FY{fiscal_year}-Q4",
            period_kind="quarter",
            knowledge_date=max(annual.knowledge_date, ytd.knowledge_date),
            source_document_ids=tuple(
                sorted(set(annual.source_document_ids) | set(ytd.source_document_ids))
            ),
            observation_ids=tuple(
                sorted(set(annual.observation_ids) | set(ytd.observation_ids))
            ),
            occurrence_ids=tuple(
                sorted(set(annual.occurrence_ids) | set(ytd.occurrence_ids))
            ),
            definition_id=annual.definition_id,
            basis_id=annual.basis_id,
            unit_id=annual.unit_id,
            currency=annual.currency,
            derivation_rule_id=Q4_ADD_FY_MINUS_YTD_RULE_ID,
            derivation_input_record_ids=inputs,
            derivation_support_record_ids=support,
        )

    if metric_id == "metric:core:capital-expenditures@1":
        annual = _foundation_direct_selection(
            foundation,
            metric_id="metric:core:property-equipment-purchases@1",
            period_id=annual_id,
            as_of_date=as_of_date,
        )
        ytd = _foundation_direct_selection(
            foundation,
            metric_id="metric:core:property-equipment-purchases@1",
            period_id=ytd_id,
            as_of_date=as_of_date,
        )
        if annual is None or ytd is None:
            return None
        require_additive_identity(
            annual,
            ytd,
            expected_definition=annual.definition_id,
        )
        relations = {
            str(relation["period_id"]): relation
            for relation in foundation["definition_relations"]
            if str(relation["period_id"]) in {annual_id, ytd_id}
            and str(relation["knowledge_date"]) <= as_of_date
        }
        if set(relations) != {annual_id, ytd_id}:
            return None
        inputs = tuple(
            sorted(set(annual.candidate_record_ids) | set(ytd.candidate_record_ids))
        )
        value = Decimal(str(annual.value["value"])) - Decimal(str(ytd.value["value"]))
        return _FoundationFactSelection(
            value={"kind": "exact", "value": _format_decimal(str(value))},
            candidate_record_ids=inputs,
            period_id=period_id,
            period_key=f"FY{fiscal_year}-Q4",
            period_kind="quarter",
            knowledge_date=max(
                annual.knowledge_date,
                ytd.knowledge_date,
                *(str(relation["knowledge_date"]) for relation in relations.values()),
            ),
            source_document_ids=tuple(
                sorted(
                    set(annual.source_document_ids)
                    | set(ytd.source_document_ids)
                    | {
                        str(relation["source_document_id"])
                        for relation in relations.values()
                    }
                )
            ),
            observation_ids=tuple(
                sorted(set(annual.observation_ids) | set(ytd.observation_ids))
            ),
            occurrence_ids=tuple(
                sorted(
                    set(annual.occurrence_ids)
                    | set(ytd.occurrence_ids)
                    | {
                        str(relation["source_occurrence_id"])
                        for relation in relations.values()
                    }
                )
            ),
            definition_id="definition:anf:company-guided-capex@1",
            basis_id=annual.basis_id,
            unit_id=annual.unit_id,
            currency=annual.currency,
            derivation_rule_id=Q4_ADD_FY_MINUS_YTD_RULE_ID,
            derivation_input_record_ids=inputs,
            derivation_support_record_ids=tuple(
                sorted(str(relation["relation_id"]) for relation in relations.values())
            ),
        )

    component_by_metric = {
        "metric:retail:store-openings@1": "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1": "metric:retail:store-closures-count@1",
    }
    if metric_id in component_by_metric:
        return residual(component_by_metric[metric_id])

    if metric_id == "metric:retail:net-store-openings@1":
        openings = _foundation_q4_selection(
            foundation,
            periods,
            metric_id="metric:retail:store-openings@1",
            fiscal_year=fiscal_year,
            as_of_date=as_of_date,
        )
        closures = _foundation_q4_selection(
            foundation,
            periods,
            metric_id="metric:retail:store-closures-count@1",
            fiscal_year=fiscal_year,
            as_of_date=as_of_date,
        )
        if openings is None or closures is None:
            return None
        if openings.unit_id != closures.unit_id or openings.basis_id != closures.basis_id:
            raise PromiseProgressProductV2Error(
                "Q4 net-opening components have incompatible typed identities."
            )
        inputs = tuple(
            sorted(set(openings.candidate_record_ids) | set(closures.candidate_record_ids))
        )
        value = Decimal(str(openings.value["value"])) - Decimal(
            str(closures.value["value"])
        )
        return _FoundationFactSelection(
            value={"kind": "exact", "value": _format_decimal(str(value))},
            candidate_record_ids=inputs,
            period_id=period_id,
            period_key=f"FY{fiscal_year}-Q4",
            period_kind="quarter",
            knowledge_date=max(openings.knowledge_date, closures.knowledge_date),
            source_document_ids=tuple(
                sorted(set(openings.source_document_ids) | set(closures.source_document_ids))
            ),
            observation_ids=tuple(
                sorted(set(openings.observation_ids) | set(closures.observation_ids))
            ),
            occurrence_ids=tuple(
                sorted(set(openings.occurrence_ids) | set(closures.occurrence_ids))
            ),
            definition_id="definition:anf:company-owned-store-activity@1",
            basis_id=openings.basis_id,
            unit_id=openings.unit_id,
            currency=None,
            derivation_rule_id=NET_STORE_OPENINGS_RULE_ID,
            derivation_input_record_ids=inputs,
            derivation_support_record_ids=tuple(
                sorted(
                    {
                        openings.derivation_rule_id or Q4_ADD_FY_MINUS_YTD_RULE_ID,
                        closures.derivation_rule_id or Q4_ADD_FY_MINUS_YTD_RULE_ID,
                    }
                )
            ),
        )

    if metric_id == "metric:retail:store-remodels-right-sizes@1":
        remodels = residual("metric:retail:store-remodels@1")
        right_sizes = residual("metric:retail:store-right-sizes@1")
        if remodels is None or right_sizes is None:
            return None
        value = Decimal(str(remodels.value["value"])) + Decimal(
            str(right_sizes.value["value"])
        )
        inputs = tuple(
            sorted(set(remodels.candidate_record_ids) | set(right_sizes.candidate_record_ids))
        )
        return _FoundationFactSelection(
            value={"kind": "exact", "value": _format_decimal(str(value))},
            candidate_record_ids=inputs,
            period_id=period_id,
            period_key=f"FY{fiscal_year}-Q4",
            period_kind="quarter",
            knowledge_date=max(remodels.knowledge_date, right_sizes.knowledge_date),
            source_document_ids=tuple(
                sorted(set(remodels.source_document_ids) | set(right_sizes.source_document_ids))
            ),
            observation_ids=tuple(
                sorted(set(remodels.observation_ids) | set(right_sizes.observation_ids))
            ),
            occurrence_ids=tuple(
                sorted(set(remodels.occurrence_ids) | set(right_sizes.occurrence_ids))
            ),
            definition_id="definition:anf:company-owned-store-activity@1",
            basis_id=remodels.basis_id,
            unit_id=remodels.unit_id,
            currency=None,
            derivation_rule_id=STORE_COMPONENT_COMBINATION_RULE_ID,
            derivation_input_record_ids=inputs,
            derivation_support_record_ids=tuple(
                sorted(
                    {
                        remodels.derivation_rule_id or Q4_ADD_FY_MINUS_YTD_RULE_ID,
                        right_sizes.derivation_rule_id or Q4_ADD_FY_MINUS_YTD_RULE_ID,
                    }
                )
            ),
        )
    return None


def _foundation_outcome_status(
    *,
    company_id: str,
    row_key: str,
    metric_id: str,
    target_version_id: str,
    target_value: Mapping[str, Any],
    target_period_id: str,
    actual: _FoundationFactSelection,
    event_date: str,
) -> tuple[str, str, str | None, str, str]:
    actual_display = display_value(actual.value, unit_id=actual.unit_id)
    typed_actual = ActualSelection(
        actual_id=f"actual-selection:promise-progress-v2.1:{_digest({'row': row_key, 'actual': actual.candidate_record_ids})[:24]}@1",
        actual_role_id=ACTUAL_FY_ID,
        semantic_class=ACTUAL_ROLE_SEMANTIC_CLASSES[ACTUAL_FY_ID],
        selection_state="selected",
        canonical_observation_ids=actual.candidate_record_ids,
        semantic_identity=SemanticIdentity(
            metric_id=metric_id,
            definition_id=actual.definition_id,
            basis_id=actual.basis_id,
            unit_id=actual.unit_id,
            dimensions=(),
        ),
        effective_or_fiscal_period_id=actual.period_id,
        publication_date=actual.knowledge_date,
        knowledge_date=actual.knowledge_date,
        value_form=str(actual.value["kind"]),
        source_occurrence_ids=actual.occurrence_ids,
        source_document_ids=actual.source_document_ids,
        display_value=_actual_display_value(actual.value, display_text=actual_display),
        milestone_state=None,
        selection_method_id="selection:promise-progress-product-v2.1:foundation-compatible-actual@1",
        lineage_state="source-backed",
        lineage_digest=_digest(
            {
                "actual_candidate_record_ids": actual.candidate_record_ids,
                "source_document_ids": actual.source_document_ids,
                "period_id": actual.period_id,
            }
        ),
    )
    target_kind = str(target_value.get("kind"))
    if str(actual.value.get("kind")) != "exact":
        reason = "approximate_target_tolerance_unreviewed"
        return (
            "needs_review",
            STATUS_LABELS["needs_review"],
            reason,
            NEEDS_REVIEW_REASONS[reason][1],
            STATUS_REVIEW_ID,
        )
    if target_kind == "qualitative":
        reason = "qualitative_target_non_comparable"
        return (
            "needs_review",
            STATUS_LABELS["needs_review"],
            reason,
            NEEDS_REVIEW_REASONS[reason][1],
            STATUS_QUALITATIVE_ID,
        )
    if target_value.get("comparison_contract") == "plan-point-without-reviewed-tolerance":
        if Decimal(str(actual.value["value"])) == Decimal(str(target_value["value"])):
            return "hit", STATUS_LABELS["hit"], None, "", STATUS_APPROX_ID
        reason = "point_target_tolerance_unreviewed"
        return (
            "needs_review",
            STATUS_LABELS["needs_review"],
            reason,
            NEEDS_REVIEW_REASONS[reason][1],
            STATUS_APPROX_ID,
        )
    if target_kind == "approximate" and target_value.get("tolerance") is None:
        actual_number = Decimal(str(actual.value["value"]))
        target_number = Decimal(str(target_value["value"]))
        favorable = FAVORABLE_DIRECTION_BY_METRIC.get(metric_id)
        if actual_number == target_number:
            return "hit", STATUS_LABELS["hit"], None, "", STATUS_APPROX_ID
        if favorable == "higher" and actual_number > target_number:
            return "beat", STATUS_LABELS["beat"], None, "", STATUS_APPROX_ID
        if favorable == "lower" and actual_number < target_number:
            return "beat", STATUS_LABELS["beat"], None, "", STATUS_APPROX_ID
        reason = (
            "approximate_target_direction_ambiguous"
            if favorable is None
            else "approximate_target_tolerance_unreviewed"
        )
        return (
            "needs_review",
            STATUS_LABELS["needs_review"],
            reason,
            NEEDS_REVIEW_REASONS[reason][1],
            STATUS_APPROX_ID,
        )
    if target_kind == "bound" and FAVORABLE_DIRECTION_BY_METRIC.get(metric_id) is None:
        actual_number = Decimal(str(actual.value["value"]))
        target_number = Decimal(str(target_value["value"]))
        operator = str(target_value["operator"])
        satisfied = {
            "gte": actual_number >= target_number,
            "gt": actual_number > target_number,
            "lte": actual_number <= target_number,
            "lt": actual_number < target_number,
        }.get(operator)
        if satisfied is None:
            raise PromiseProgressProductV2Error(
                f"Unsupported bound operator {operator!r}."
            )
        code = "hit" if satisfied else "missed"
        return code, STATUS_LABELS[code], None, "", _status_rule_for_value(target_value)
    comparison_target = dict(target_value)
    if target_value.get("direction") == "down":
        if target_kind == "range":
            comparison_target["low"] = _format_decimal(
                str(-Decimal(str(target_value["high"])))
            )
            comparison_target["high"] = _format_decimal(
                str(-Decimal(str(target_value["low"])))
            )
        elif target_kind in {"exact", "approximate", "bound"}:
            comparison_target["value"] = _format_decimal(
                str(-Decimal(str(target_value["value"])))
            )
        comparison_target.pop("direction", None)
    rule_id = _status_rule_for_value(comparison_target)
    status = assess_status(
        product_id=_product_id(company_id),
        row_key=row_key,
        rule_id=rule_id,
        target_version_id=target_version_id,
        target_value=comparison_target,
        actual=typed_actual,
        progress=None,
        ui_as_of_date=event_date,
        horizon_closed=True,
        target_period_or_horizon_id=target_period_id,
        review_issue_ids=(),
        favorable_direction=FAVORABLE_DIRECTION_BY_METRIC.get(metric_id),
    )
    reason_code = None
    reason_display = ""
    if status.status_code == "needs_review":
        reason_code = (
            "qualitative_target_non_comparable"
            if target_kind == "qualitative"
            else "comparable_actual_unavailable"
        )
        reason_display = NEEDS_REVIEW_REASONS[reason_code][1]
    return status.status_code, status.visible_label, reason_code, reason_display, rule_id


def build_promise_progress_product_v2(
    package: Mapping[str, Any],
    *,
    source_set_id: str,
    reviewed_links: Iterable[Mapping[str, Any]] = (),
    product_version: str = PRODUCT_VERSION,
    evidence_foundation: Mapping[str, Any] | None = None,
) -> PromiseProgressProductV2:
    if product_version not in SUPPORTED_PRODUCT_VERSIONS:
        raise PromiseProgressProductV2Error(
            f"Unsupported Product@2 version {product_version!r}."
        )
    if evidence_foundation is not None:
        if product_version != SUCCESSOR_PRODUCT_VERSION:
            raise PromiseProgressProductV2Error(
                "The reviewed Evidence Foundation is a Product@2.1 successor input only."
            )
        return _build_evidence_complete_successor(
            package,
            source_set_id=source_set_id,
            reviewed_links=reviewed_links,
            evidence_foundation=evidence_foundation,
        )
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
                    unit_id=(
                        None if product_version == PRODUCT_VERSION else unit_id
                    ),
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
                    unit_id=(
                        None if product_version == PRODUCT_VERSION else unit_id
                    ),
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
                    previous_value=(
                        None
                        if product_version == PRODUCT_VERSION or predecessor is None
                        else predecessor["payload"]["value"]
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
                    unit_id=(
                        None if product_version == PRODUCT_VERSION else unit_id
                    ),
                )
            )

        if (
            product_version == SUCCESSOR_PRODUCT_VERSION
            and date.fromisoformat(str(period["end_date"])) < cutoff
        ):
            # A result event is distinct from the last guidance update.  It owns
            # event-period Q4 Actuals and the annual outcome replay without
            # manufacturing a Q4 guidance version.
            candidate_records = tuple(
                observations[record_id]
                for record_id in actual_candidate_ids
                if record_id in observations
            )
            candidate_sources = {
                source_id
                for record in candidate_records
                for source_id in source_ids(record)
            }
            candidate_sources.update(actual_source_document_ids)
            candidate_event_ids = {
                source_to_event[source_id]
                for source_id in candidate_sources
                if source_id in source_to_event
                and event_by_id[source_to_event[source_id]].event_date
                > str(period["end_date"])
                and event_by_id[source_to_event[source_id]].event_date
                <= str(package["knowledge_cutoff"])
            }
            if len(candidate_event_ids) != 1:
                raise PromiseProgressProductV2Error(
                    "A closed successor series does not resolve to one reviewed result event."
                )
            outcome_event_id = next(iter(candidate_event_ids))
            outcome_event = event_by_id[outcome_event_id]
            outcome_event_sources = tuple(outcome_event.source_document_ids)
            outcome_source_rows = tuple(
                documents[source_id] for source_id in outcome_event_sources
            )
            outcome_stated_id, outcome_stated_display = _reporting_update_context(
                event=outcome_event,
                source_rows=outcome_source_rows,
                horizon_period=period,
            )
            outcome_actual, _unused_progress = _event_actual_and_progress_for_series(
                package,
                series,
                event_date=outcome_event.event_date,
                event_source_document_ids=outcome_event_sources,
            )
            if outcome_actual is None:
                outcome_actual = _derived_q4_capex_for_series(
                    package,
                    series,
                    event_date=outcome_event.event_date,
                )
            (
                outcome_status_actual,
                outcome_status_candidate_ids,
                _outcome_status_period,
                _outcome_status_knowledge,
                _outcome_status_sources,
                outcome_status_reason,
                _outcome_status_reason_display,
            ) = _actual_for_series(
                package, series, as_of_date=outcome_event.event_date
            )
            outcome_status_actual_display = (
                ""
                if outcome_status_actual is None
                else display_value(outcome_status_actual, unit_id=unit_id)
            )
            (
                outcome_status_code,
                outcome_status_label,
                outcome_reason_code,
                outcome_reason_display,
            ) = outcome_status(
                row_key=f"{series_id}|outcome={outcome_event_id}",
                metric_id=metric_id,
                target_record=selected,
                target_period_id=period_id,
                actual_value=outcome_status_actual,
                actual_display=outcome_status_actual_display,
                actual_candidate_ids=outcome_status_candidate_ids,
                reason_code=outcome_status_reason,
                as_of_date=outcome_event.event_date,
            )
            outcome_lineage = {
                "series_id": series_id,
                "selected_guidance_record_id": selected_id,
                "event_id": outcome_event_id,
                "event_source_document_ids": outcome_event_sources,
                "annual_status_actual_candidate_record_ids": outcome_status_candidate_ids,
                "q4_actual_role_id": (
                    None if outcome_actual is None else outcome_actual.role_id
                ),
                "q4_actual_candidate_record_ids": (
                    () if outcome_actual is None else outcome_actual.candidate_record_ids
                ),
                "q4_actual_period_id": (
                    None if outcome_actual is None else outcome_actual.period_id
                ),
                "q4_actual_knowledge_date": (
                    None if outcome_actual is None else outcome_actual.knowledge_date
                ),
                "q4_actual_source_document_ids": (
                    () if outcome_actual is None else outcome_actual.source_document_ids
                ),
                "q4_derivation_rule_id": (
                    None if outcome_actual is None else outcome_actual.derivation_rule_id
                ),
                "q4_derivation_input_record_ids": (
                    ()
                    if outcome_actual is None
                    else outcome_actual.derivation_input_record_ids
                ),
                "q4_derivation_support_record_ids": (
                    ()
                    if outcome_actual is None
                    else outcome_actual.derivation_support_record_ids
                ),
            }
            timeline_rows.append(
                ProductRowV2(
                    row_id=(
                        f"pprow:v2.1|block=revision-timeline|outcome-series={series_id}"
                        f"|event={outcome_event_id}"
                    ),
                    block_id=TIMELINE_BLOCK_ID,
                    row_kind="timeline_outcome",
                    eligible=True,
                    group_id=outcome_event_id,
                    metric_id=metric_id,
                    metric_label=METRIC_LABELS[metric_id],
                    horizon_period_id=period_id,
                    horizon_label=horizon_label(period_id),
                    current_value=selected["payload"]["value"],
                    current_display=display_value(
                        selected["payload"]["value"], unit_id=unit_id
                    ),
                    progression_values=(),
                    previous_display="",
                    actual_value=None if outcome_actual is None else outcome_actual.value,
                    actual_display=(
                        "" if outcome_actual is None else outcome_actual.display_text
                    ),
                    actual_candidate_record_ids=(
                        () if outcome_actual is None else outcome_actual.candidate_record_ids
                    ),
                    actual_period_id=(
                        None if outcome_actual is None else outcome_actual.period_id
                    ),
                    actual_knowledge_date=(
                        None if outcome_actual is None else outcome_actual.knowledge_date
                    ),
                    actual_source_document_ids=(
                        () if outcome_actual is None else outcome_actual.source_document_ids
                    ),
                    progress_value=None,
                    progress_display="",
                    progress_candidate_record_ids=(),
                    progress_period_id=None,
                    progress_knowledge_date=None,
                    progress_source_document_ids=(),
                    version_state="Final",
                    status_code_at_update=outcome_status_code,
                    status_at_update=outcome_status_label,
                    change_type="Outcome reported",
                    comparison_reason_code="outcome_reported",
                    investor_reason_code=outcome_reason_code,
                    investor_reason_display=outcome_reason_display,
                    event_id=outcome_event_id,
                    event_date=outcome_event.event_date,
                    stated_in_period_id=outcome_stated_id,
                    stated_in_display=outcome_stated_display,
                    current_source_document_ids=outcome_event_sources,
                    predecessor_source_document_ids=selected_sources,
                    source_summary=_source_summary(outcome_source_rows),
                    lineage_digest=_digest(outcome_lineage),
                    actual_derivation_rule_id=(
                        None if outcome_actual is None else outcome_actual.derivation_rule_id
                    ),
                    actual_derivation_input_record_ids=(
                        ()
                        if outcome_actual is None
                        else outcome_actual.derivation_input_record_ids
                    ),
                    actual_derivation_support_record_ids=(
                        ()
                        if outcome_actual is None
                        else outcome_actual.derivation_support_record_ids
                    ),
                    unit_id=unit_id,
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
        product_version=product_version,
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


def _build_evidence_complete_successor(
    package: Mapping[str, Any],
    *,
    source_set_id: str,
    reviewed_links: Iterable[Mapping[str, Any]],
    evidence_foundation: Mapping[str, Any],
) -> PromiseProgressProductV2:
    if (
        str(evidence_foundation.get("foundation_id")) != EVIDENCE_FOUNDATION_ID
        or str(evidence_foundation.get("source_set_id"))
        != EVIDENCE_FOUNDATION_SOURCE_SET_ID
        or source_set_id != EVIDENCE_FOUNDATION_SOURCE_SET_ID
        or len(evidence_foundation.get("quarter_guidance_versions", ())) != 60
    ):
        raise PromiseProgressProductV2Error(
            "Product@2.1 requires the exact reviewed Evidence Foundation @1/source set @4."
        )
    base = build_promise_progress_product_v2(
        package,
        source_set_id=source_set_id,
        reviewed_links=reviewed_links,
        product_version=SUCCESSOR_PRODUCT_VERSION,
        evidence_foundation=None,
    )
    company_id = base.company_id
    cutoff = str(evidence_foundation["knowledge_cutoff"])
    periods = _foundation_period_metadata(package, evidence_foundation)
    documents = {
        str(row["source_document_id"]): row
        for row in evidence_foundation["semantic_source_documents"]
    }
    base_blocks = {block.block_id: block for block in base.blocks}
    annual_versions = list(evidence_foundation["annual_guidance_versions"])
    annual_versions_by_series: dict[str, list[Mapping[str, Any]]] = {}
    for version in annual_versions:
        annual_versions_by_series.setdefault(
            str(version["guidance_series_id"]), []
        ).append(version)
    for versions in annual_versions_by_series.values():
        versions.sort(
            key=lambda value: (
                str(value["source_date"]),
                int(value["version_ordinal"]),
                str(value["guidance_version_id"]),
            )
        )
    may_annual_versions = {
        (str(version["metric_id"]), str(version["horizon_period_id"])): version
        for version in annual_versions
        if str(version["source_date"]) == "2026-05-27"
    }
    if len(may_annual_versions) != 10:
        raise PromiseProgressProductV2Error(
            f"Reviewed May FY2026 annual outlook count is {len(may_annual_versions)}, not 10."
        )

    progression_rows: list[ProductRowV2] = []
    annual_target_by_key: dict[tuple[str, str], ProductRowV2] = {}
    base_progression_slot_by_horizon: dict[str, dict[str, str]] = {}
    for base_row in base_blocks[PROGRESSION_BLOCK_ID].rows:
        if base_row.horizon_period_id is None:
            continue
        dates = sorted(
            {
                value.publication_date
                for peer in base_blocks[PROGRESSION_BLOCK_ID].rows
                if peer.horizon_period_id == base_row.horizon_period_id
                for value in peer.progression_values
            }
        )
        if len(dates) > 5:
            raise PromiseProgressProductV2Error(
                f"Accepted annual progression {base_row.horizon_period_id} exceeds five typed update slots."
            )
        base_progression_slot_by_horizon[base_row.horizon_period_id] = dict(
            zip(dates, ("initial", "q1", "q2", "q3", "q4"), strict=False)
        )
    for row in base_blocks[PROGRESSION_BLOCK_ID].rows:
        if row.metric_id is None or row.horizon_period_id is None:
            continue
        actual = _foundation_actual_for_metric(
            evidence_foundation,
            metric_id=row.metric_id,
            period_id=row.horizon_period_id,
            as_of_date=cutoff,
        )
        selected_version = max(
            row.progression_values,
            key=lambda value: (value.publication_date, value.version_record_id),
        )
        status_code = row.status_code_at_update
        status_label = row.status_at_update
        reason_code = row.investor_reason_code
        reason_display = row.investor_reason_display
        status_rule_id = None
        if actual is not None and row.current_value is not None:
            (
                status_code,
                status_label,
                reason_code,
                reason_display,
                status_rule_id,
            ) = _foundation_outcome_status(
                company_id=company_id,
                row_key=row.row_id,
                metric_id=row.metric_id,
                target_version_id=selected_version.version_record_id,
                target_value=row.current_value,
                target_period_id=row.horizon_period_id,
                actual=actual,
                event_date=max(actual.knowledge_date, str(periods[row.horizon_period_id]["end_date"])),
            )
        actual_sources = () if actual is None else actual.source_document_ids
        actual_candidates = () if actual is None else actual.candidate_record_ids
        actual_display = (
            "" if actual is None else display_value(actual.value, unit_id=actual.unit_id)
        )
        progression_row = dataclasses.replace(
            row,
            progression_values=tuple(
                dataclasses.replace(
                    value,
                    progression_slot=base_progression_slot_by_horizon[
                        row.horizon_period_id
                    ][value.publication_date],
                )
                for value in row.progression_values
            ),
            actual_value=None if actual is None else actual.value,
            actual_display=actual_display,
            actual_candidate_record_ids=actual_candidates,
            actual_period_id=None if actual is None else actual.period_id,
            actual_knowledge_date=None if actual is None else actual.knowledge_date,
            actual_source_document_ids=actual_sources,
            status_code_at_update=status_code,
            status_at_update=status_label,
            investor_reason_code=reason_code,
            investor_reason_display=reason_display,
            lineage_digest=_digest(
                {
                    "base_lineage_digest": row.lineage_digest,
                    "foundation_id": EVIDENCE_FOUNDATION_ID,
                    "actual_candidate_record_ids": actual_candidates,
                    "actual_source_document_ids": actual_sources,
                    "status_rule_id": status_rule_id,
                }
            ),
            actual_derivation_rule_id=(
                None if actual is None else actual.derivation_rule_id
            ),
            actual_derivation_input_record_ids=(
                () if actual is None else actual.derivation_input_record_ids
            ),
            actual_derivation_support_record_ids=(
                () if actual is None else actual.derivation_support_record_ids
            ),
            status_target_guidance_version_id=(
                None if actual is None else selected_version.version_record_id
            ),
            status_actual_candidate_record_ids=actual_candidates,
            status_actual_period_id=None if actual is None else actual.period_id,
            status_actual_knowledge_date=None if actual is None else actual.knowledge_date,
            status_actual_source_document_ids=actual_sources,
            status_actual_basis_id=None if actual is None else actual.basis_id,
            status_actual_unit_id=None if actual is None else actual.unit_id,
            status_rule_id=status_rule_id,
        )
        progression_rows.append(progression_row)
        annual_target_by_key[(row.metric_id, row.horizon_period_id)] = progression_row

    # Add the nine source-native historical annual store series that were absent
    # from the accepted Product@2 checkpoint.  Their explicit progression slots
    # preserve annual-update context without leaking quarter-horizon guidance.
    for series_id, versions in sorted(annual_versions_by_series.items()):
        final_version = versions[-1]
        horizon_id = str(final_version["horizon_period_id"])
        metric_id = str(final_version["metric_id"])
        if horizon_id == "period:anf:fy2026@1":
            continue
        key = (metric_id, horizon_id)
        if key in annual_target_by_key:
            raise PromiseProgressProductV2Error(
                f"Evidence Foundation annual series duplicates an accepted series {key}."
            )
        unit_id = _product_value_unit_id(
            final_version["canonical_value"], str(final_version["unit_id"])
        )
        progression_values = tuple(
            ProductVersionValueV2(
                version_record_id=str(version["guidance_version_id"]),
                publication_date=str(version["source_date"]),
                canonical_value=version["canonical_value"],
                display_text=_foundation_guidance_display(
                    version["canonical_value"],
                    metric_id=metric_id,
                    unit_id=unit_id,
                ),
                source_document_ids=(str(version["source_document_id"]),),
                progression_slot=str(version["progression_slot"]),
            )
            for version in versions
        )
        actual = _foundation_actual_for_metric(
            evidence_foundation,
            metric_id=metric_id,
            period_id=horizon_id,
            as_of_date=cutoff,
        )
        if actual is None:
            raise PromiseProgressProductV2Error(
                f"Reviewed historical annual store series lacks its Actual: {key}."
            )
        (
            status_code,
            status_label,
            reason_code,
            reason_display,
            status_rule_id,
        ) = _foundation_outcome_status(
            company_id=company_id,
            row_key=series_id,
            metric_id=metric_id,
            target_version_id=str(final_version["guidance_version_id"]),
            target_value=final_version["canonical_value"],
            target_period_id=horizon_id,
            actual=actual,
            event_date=actual.knowledge_date,
        )
        source_id = str(final_version["source_document_id"])
        actual_display = display_value(actual.value, unit_id=actual.unit_id)
        progression_row = ProductRowV2(
            row_id=f"pprow:v2.1|block=guidance-progression|annual-series={series_id}",
            block_id=PROGRESSION_BLOCK_ID,
            row_kind="guidance_progression",
            eligible=True,
            group_id=horizon_id,
            metric_id=metric_id,
            metric_label=METRIC_LABELS[metric_id],
            horizon_period_id=horizon_id,
            horizon_label=_foundation_period_display(horizon_id),
            current_value=final_version["canonical_value"],
            current_display=_foundation_guidance_display(
                final_version["canonical_value"],
                metric_id=metric_id,
                unit_id=unit_id,
            ),
            progression_values=progression_values,
            previous_display="",
            actual_value=actual.value,
            actual_display=actual_display,
            actual_candidate_record_ids=actual.candidate_record_ids,
            actual_period_id=actual.period_id,
            actual_knowledge_date=actual.knowledge_date,
            actual_source_document_ids=actual.source_document_ids,
            progress_value=None,
            progress_display="",
            progress_candidate_record_ids=(),
            progress_period_id=None,
            progress_knowledge_date=None,
            progress_source_document_ids=(),
            version_state="Final",
            status_code_at_update=status_code,
            status_at_update=status_label,
            change_type=None,
            comparison_reason_code="horizon_outcome_replay",
            investor_reason_code=reason_code,
            investor_reason_display=reason_display,
            event_id=None,
            event_date=None,
            stated_in_period_id=None,
            stated_in_display="",
            current_source_document_ids=(source_id,),
            predecessor_source_document_ids=(),
            source_summary=_foundation_source_summary((source_id,), documents),
            lineage_digest=_digest(
                {
                    "foundation_id": EVIDENCE_FOUNDATION_ID,
                    "annual_guidance_series_id": series_id,
                    "guidance_version_ids": [
                        version["guidance_version_id"] for version in versions
                    ],
                    "actual_candidate_record_ids": actual.candidate_record_ids,
                    "status_rule_id": status_rule_id,
                }
            ),
            actual_derivation_rule_id=actual.derivation_rule_id,
            actual_derivation_input_record_ids=actual.derivation_input_record_ids,
            actual_derivation_support_record_ids=actual.derivation_support_record_ids,
            unit_id=unit_id,
            status_target_guidance_version_id=str(final_version["guidance_version_id"]),
            status_actual_candidate_record_ids=actual.candidate_record_ids,
            status_actual_period_id=actual.period_id,
            status_actual_knowledge_date=actual.knowledge_date,
            status_actual_source_document_ids=actual.source_document_ids,
            status_actual_basis_id=actual.basis_id,
            status_actual_unit_id=actual.unit_id,
            status_rule_id=status_rule_id,
        )
        progression_rows.append(progression_row)
        annual_target_by_key[key] = progression_row

    open_rows = [
        dataclasses.replace(
            row,
            event_id=None,
            event_date=None,
            stated_in_period_id=None,
            stated_in_display="",
        )
        for row in base_blocks[OPEN_BLOCK_ID].rows
    ]
    replaced_may_keys: set[tuple[str, str]] = set()
    refreshed_open_rows: list[ProductRowV2] = []
    for row in open_rows:
        if row.metric_id is None or row.horizon_period_id is None:
            refreshed_open_rows.append(row)
            continue
        key = (row.metric_id, row.horizon_period_id)
        version = may_annual_versions.get(key)
        if version is None:
            refreshed_open_rows.append(row)
            continue
        source_id = str(version["source_document_id"])
        unit_id = _product_value_unit_id(
            version["canonical_value"], str(version["unit_id"])
        )
        refreshed_open_rows.append(
            dataclasses.replace(
                row,
                group_id=str(version["guidance_series_id"]),
                current_value=version["canonical_value"],
                current_display=_foundation_guidance_display(
                    version["canonical_value"],
                    metric_id=row.metric_id,
                    unit_id=unit_id,
                ),
                current_source_document_ids=(source_id,),
                predecessor_source_document_ids=row.current_source_document_ids,
                source_summary=_foundation_source_summary((source_id,), documents),
                lineage_digest=_digest(
                    {
                        "foundation_id": EVIDENCE_FOUNDATION_ID,
                        "selected_annual_guidance_version_id": version[
                            "guidance_version_id"
                        ],
                        "true_predecessor_lineage_digest": row.lineage_digest,
                    }
                ),
                unit_id=unit_id,
            )
        )
        replaced_may_keys.add(key)
    if replaced_may_keys != set(may_annual_versions):
        raise PromiseProgressProductV2Error(
            "The May FY2026 outlook did not replace exactly the ten annual Open rows."
        )
    open_rows = refreshed_open_rows
    timeline_rows: list[ProductRowV2] = []
    base_annual_timeline_by_key: dict[tuple[str, str], list[ProductRowV2]] = {}
    for row in base_blocks[TIMELINE_BLOCK_ID].rows:
        if row.row_kind != "timeline_version":
            continue
        if row.metric_id is None or row.horizon_period_id is None:
            raise PromiseProgressProductV2Error(
                "An accepted annual guidance row lacks metric/horizon identity."
            )
        key = (row.metric_id, row.horizon_period_id)
        base_annual_timeline_by_key.setdefault(key, []).append(row)
        timeline_rows.append(
            dataclasses.replace(
                row,
                row_kind="guidance_update",
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
                version_state=(
                    "Superseded"
                    if key in may_annual_versions and row.version_state == "Current"
                    else row.version_state
                ),
                status_code_at_update="open",
                status_at_update="Open",
                investor_reason_code=None,
                investor_reason_display="",
                lineage_digest=_digest(
                    {
                        "base_lineage_digest": row.lineage_digest,
                        "semantic_role": "guidance_update",
                        "foundation_id": EVIDENCE_FOUNDATION_ID,
                    }
                ),
            )
        )

    for rows in base_annual_timeline_by_key.values():
        rows.sort(key=lambda row: (str(row.event_date), row.row_id))

    # Add all 34 reviewed annual versions missing from the rejected successor:
    # 24 historical store-plan versions and the ten May 2026 replacement values.
    for series_id, versions in sorted(annual_versions_by_series.items()):
        final_version = versions[-1]
        key = (
            str(final_version["metric_id"]),
            str(final_version["horizon_period_id"]),
        )
        is_may_series = str(final_version["source_date"]) == "2026-05-27"
        base_predecessor = (
            base_annual_timeline_by_key.get(key, [])[-1]
            if is_may_series and base_annual_timeline_by_key.get(key)
            else None
        )
        if is_may_series and base_predecessor is None:
            raise PromiseProgressProductV2Error(
                f"May annual series has no true March predecessor: {key}."
            )
        by_id = {
            str(version["guidance_version_id"]): version for version in versions
        }
        for version in versions:
            predecessor = by_id.get(
                str(version.get("predecessor_guidance_version_id") or "")
            )
            previous_value: Mapping[str, Any] | None
            predecessor_sources: tuple[str, ...]
            predecessor_id: str | None
            if predecessor is not None:
                previous_value = predecessor["canonical_value"]
                predecessor_sources = (str(predecessor["source_document_id"]),)
                predecessor_id = str(predecessor["guidance_version_id"])
            elif base_predecessor is not None:
                previous_value = base_predecessor.current_value
                predecessor_sources = base_predecessor.current_source_document_ids
                predecessor_id = base_predecessor.row_id
            else:
                previous_value = None
                predecessor_sources = ()
                predecessor_id = None
            change_type, reason = classify_change(
                version["canonical_value"], previous_value
            )
            metric_id = str(version["metric_id"])
            horizon_id = str(version["horizon_period_id"])
            unit_id = _product_value_unit_id(
                version["canonical_value"], str(version["unit_id"])
            )
            source_id = str(version["source_document_id"])
            is_final = version is final_version
            is_current = horizon_id == "period:anf:fy2026@1" and is_final
            timeline_rows.append(
                ProductRowV2(
                    row_id=(
                        "pprow:v2.1|block=revision-timeline|annual-version="
                        f"{version['guidance_version_id']}"
                    ),
                    block_id=TIMELINE_BLOCK_ID,
                    row_kind="guidance_update",
                    eligible=True,
                    group_id=f"pending-event:{version['source_date']}",
                    metric_id=metric_id,
                    metric_label=METRIC_LABELS[metric_id],
                    horizon_period_id=horizon_id,
                    horizon_label=_foundation_period_display(horizon_id),
                    current_value=version["canonical_value"],
                    current_display=_foundation_guidance_display(
                        version["canonical_value"],
                        metric_id=metric_id,
                        unit_id=unit_id,
                    ),
                    progression_values=(),
                    previous_display=(
                        ""
                        if previous_value is None
                        else _foundation_guidance_display(
                            previous_value,
                            metric_id=metric_id,
                            unit_id=unit_id,
                        )
                    ),
                    previous_value=previous_value,
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
                    version_state=(
                        "Current"
                        if is_current
                        else "Final" if is_final else "Superseded"
                    ),
                    status_code_at_update="open",
                    status_at_update="Open",
                    change_type=change_type,
                    comparison_reason_code=reason,
                    investor_reason_code=None,
                    investor_reason_display="",
                    event_id=f"pending-event:{version['source_date']}",
                    event_date=str(version["source_date"]),
                    stated_in_period_id=str(version["stated_in_period_id"]),
                    stated_in_display=_foundation_stated_display(
                        str(version["stated_in_period_id"])
                    ),
                    current_source_document_ids=(source_id,),
                    predecessor_source_document_ids=predecessor_sources,
                    source_summary=_foundation_source_summary((source_id,), documents),
                    lineage_digest=_digest(
                        {
                            "foundation_id": EVIDENCE_FOUNDATION_ID,
                            "guidance_version_id": version["guidance_version_id"],
                            "source_assertion_id": version["source_assertion_id"],
                            "occurrence_id": version["occurrence_id"],
                            "true_predecessor_id": predecessor_id,
                        }
                    ),
                    unit_id=unit_id,
                )
            )

    quarter_versions = list(evidence_foundation["quarter_guidance_versions"])
    annual_metric_ids = {
        str(version["metric_id"]) for version in annual_versions
    }
    quarter_only_metric_ids = {
        str(version["metric_id"]) for version in quarter_versions
    } - annual_metric_ids
    quarter_versions_by_series: dict[str, list[Mapping[str, Any]]] = {}
    for version in quarter_versions:
        quarter_versions_by_series.setdefault(str(version["guidance_series_id"]), []).append(
            version
        )
    for versions in quarter_versions_by_series.values():
        versions.sort(key=lambda value: (str(value["source_date"]), str(value["guidance_version_id"])))

    for series_id, versions in sorted(quarter_versions_by_series.items()):
        final_version = versions[-1]
        period_id = str(final_version["horizon_period_id"])
        period = periods[period_id]
        is_current = date.fromisoformat(str(period["end_date"])) >= date.fromisoformat(cutoff)
        for version in versions:
            predecessor = next(
                (
                    candidate
                    for candidate in versions
                    if candidate["guidance_version_id"]
                    == version["predecessor_guidance_version_id"]
                ),
                None,
            )
            change_type, reason = classify_change(
                version["canonical_value"],
                None if predecessor is None else predecessor["canonical_value"],
            )
            source_id = str(version["source_document_id"])
            unit_id = _product_value_unit_id(
                version["canonical_value"], str(version["unit_id"])
            )
            timeline_rows.append(
                ProductRowV2(
                    row_id=f"pprow:v2.1|block=revision-timeline|quarter-version={version['guidance_version_id']}",
                    block_id=TIMELINE_BLOCK_ID,
                    row_kind="guidance_update",
                    eligible=True,
                    group_id=f"pending-event:{version['source_date']}",
                    metric_id=str(version["metric_id"]),
                    metric_label=METRIC_LABELS[str(version["metric_id"])],
                    horizon_period_id=period_id,
                    horizon_label=_foundation_period_display(period_id),
                    current_value=version["canonical_value"],
                    current_display=_foundation_guidance_display(
                        version["canonical_value"],
                        metric_id=str(version["metric_id"]),
                        unit_id=unit_id,
                    ),
                    progression_values=(),
                    previous_display=(
                        ""
                        if predecessor is None
                        else _foundation_guidance_display(
                            predecessor["canonical_value"],
                            metric_id=str(predecessor["metric_id"]),
                            unit_id=unit_id,
                        )
                    ),
                    previous_value=(
                        None if predecessor is None else predecessor["canonical_value"]
                    ),
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
                    version_state=(
                        "Superseded"
                        if version is not final_version
                        else "Current" if is_current else "Final"
                    ),
                    status_code_at_update="open",
                    status_at_update="Open",
                    change_type=change_type,
                    comparison_reason_code=reason,
                    investor_reason_code=None,
                    investor_reason_display="",
                    event_id=f"pending-event:{version['source_date']}",
                    event_date=str(version["source_date"]),
                    stated_in_period_id=str(version["stated_in_period_id"]),
                    stated_in_display=_foundation_stated_display(
                        str(version["stated_in_period_id"])
                    ),
                    current_source_document_ids=(source_id,),
                    predecessor_source_document_ids=(
                        () if predecessor is None else (str(predecessor["source_document_id"]),)
                    ),
                    source_summary=_foundation_source_summary((source_id,), documents),
                    lineage_digest=_digest(
                        {
                            "foundation_id": EVIDENCE_FOUNDATION_ID,
                            "guidance_version_id": version["guidance_version_id"],
                            "source_assertion_id": version["source_assertion_id"],
                            "occurrence_id": version["occurrence_id"],
                            "predecessor_guidance_version_id": version[
                                "predecessor_guidance_version_id"
                            ],
                        }
                    ),
                    unit_id=unit_id,
                )
            )
        if is_current:
            source_id = str(final_version["source_document_id"])
            unit_id = _product_value_unit_id(
                final_version["canonical_value"], str(final_version["unit_id"])
            )
            open_rows.append(
                ProductRowV2(
                    row_id=f"pprow:v2.1|block=open-guidance|quarter-series={series_id}",
                    block_id=OPEN_BLOCK_ID,
                    row_kind="open_guidance",
                    eligible=True,
                    group_id=series_id,
                    metric_id=str(final_version["metric_id"]),
                    metric_label=METRIC_LABELS[str(final_version["metric_id"])],
                    horizon_period_id=period_id,
                    horizon_label=_foundation_period_display(period_id),
                    current_value=final_version["canonical_value"],
                    current_display=_foundation_guidance_display(
                        final_version["canonical_value"],
                        metric_id=str(final_version["metric_id"]),
                        unit_id=unit_id,
                    ),
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
                    status_code_at_update="open",
                    status_at_update="Open",
                    change_type=None,
                    comparison_reason_code=None,
                    investor_reason_code=None,
                    investor_reason_display="",
                    event_id=None,
                    event_date=None,
                    stated_in_period_id=None,
                    stated_in_display="",
                    current_source_document_ids=(source_id,),
                    predecessor_source_document_ids=(),
                    source_summary=_foundation_source_summary((source_id,), documents),
                    lineage_digest=_digest(
                        {
                            "foundation_id": EVIDENCE_FOUNDATION_ID,
                            "selected_quarter_guidance_version_id": final_version[
                                "guidance_version_id"
                            ],
                        }
                    ),
                    unit_id=unit_id,
                )
            )
    foundation_observations = {
        str(row["observation_id"]): row
        for row in evidence_foundation["canonical_observations"]
    }
    result_metrics = (
        "metric:core:revenue-growth@1",
        "metric:core:operating-margin@1",
        "metric:core:net-income-per-diluted-share@1",
        "metric:core:diluted-weighted-average-shares@1",
        "metric:core:capital-expenditures@1",
        "metric:core:share-repurchases@1",
        "metric:retail:net-store-openings@1",
        "metric:retail:store-openings@1",
        "metric:retail:store-closures-count@1",
        "metric:retail:store-remodels-right-sizes@1",
        "metric:anf:operating-income@1",
    )
    result_slots: dict[
        tuple[str, int, int, str],
        dict[str, _FoundationFactSelection | None],
    ] = {}

    def has_promise_target_at_event(
        *, metric_id: str, fiscal_year: int, fiscal_quarter: int, event_date: str
    ) -> bool:
        """Keep result facts only once compatible Promise evidence exists.

        A fact is economically useful to the broader model before that point, but it
        is not yet a Promise Progress result.  This prevents, for example, FY2022 net
        openings and pre-guidance FY2024 net openings from being projected merely
        because a later annual target exists in the same fiscal year.
        """

        if metric_id not in {
            "metric:retail:net-store-openings@1",
            "metric:retail:store-openings@1",
            "metric:retail:store-closures-count@1",
            "metric:retail:store-remodels-right-sizes@1",
        }:
            return True
        if fiscal_quarter == 4:
            return True
        annual_period_id = f"period:anf:fy{fiscal_year}@1"
        quarter_period_id = (
            f"period:anf:fy{fiscal_year}-q{fiscal_quarter}@1"
        )
        return any(
            row.row_kind == GUIDANCE_UPDATE_ROW_KIND
            and row.metric_id == metric_id
            and row.horizon_period_id in {annual_period_id, quarter_period_id}
            and row.event_date is not None
            and row.event_date <= event_date
            for row in timeline_rows
        )

    def add_result_selection(
        *,
        metric_id: str,
        fiscal_year: int,
        fiscal_quarter: int,
        role: str,
        selection: _FoundationFactSelection,
    ) -> None:
        key = (metric_id, fiscal_year, fiscal_quarter, selection.knowledge_date)
        slot = result_slots.setdefault(key, {"actual": None, "progress": None})
        if slot[role] is not None:
            raise PromiseProgressProductV2Error(
                f"Result role {role} is duplicated for {key}."
            )
        slot[role] = selection

    for fiscal_year in range(2022, 2027):
        for fiscal_quarter in range(1, 5):
            quarter_period_id = f"period:anf:fy{fiscal_year}-q{fiscal_quarter}@1"
            if quarter_period_id not in periods:
                continue
            for metric_id in result_metrics:
                if (
                    fiscal_quarter != 4
                    and metric_id in quarter_only_metric_ids
                    and not any(
                        str(version["metric_id"]) == metric_id
                        and str(version["horizon_period_id"]) == quarter_period_id
                        for version in quarter_versions
                    )
                ):
                    continue
                actual = (
                    _foundation_q4_selection(
                        evidence_foundation,
                        periods,
                        metric_id=metric_id,
                        fiscal_year=fiscal_year,
                        as_of_date=cutoff,
                    )
                    if fiscal_quarter == 4
                    else _foundation_actual_for_metric(
                        evidence_foundation,
                        metric_id=metric_id,
                        period_id=quarter_period_id,
                        as_of_date=cutoff,
                    )
                )
                if actual is not None:
                    if not has_promise_target_at_event(
                        metric_id=metric_id,
                        fiscal_year=fiscal_year,
                        fiscal_quarter=fiscal_quarter,
                        event_date=actual.knowledge_date,
                    ):
                        actual = None
                if actual is not None:
                    add_result_selection(
                        metric_id=metric_id,
                        fiscal_year=fiscal_year,
                        fiscal_quarter=fiscal_quarter,
                        role="actual",
                        selection=actual,
                    )
                ytd_period_id = (
                    f"period:anf:fy{fiscal_year}-ytd-q{fiscal_quarter}@1"
                )
                if ytd_period_id not in periods:
                    continue
                progress = _foundation_actual_for_metric(
                    evidence_foundation,
                    metric_id=metric_id,
                    period_id=ytd_period_id,
                    as_of_date=cutoff,
                )
                if progress is None:
                    continue
                if not has_promise_target_at_event(
                    metric_id=metric_id,
                    fiscal_year=fiscal_year,
                    fiscal_quarter=fiscal_quarter,
                    event_date=progress.knowledge_date,
                ):
                    continue
                same_occurrence = False
                if actual is not None:
                    actual_signatures = {
                        _physical_occurrence_signature(foundation_observations[observation_id])
                        for observation_id in actual.observation_ids
                        if observation_id in foundation_observations
                    }
                    progress_signatures = {
                        _physical_occurrence_signature(foundation_observations[observation_id])
                        for observation_id in progress.observation_ids
                        if observation_id in foundation_observations
                    }
                    same_occurrence = (
                        actual.derivation_rule_id is None
                        and progress.derivation_rule_id is None
                        and bool(actual_signatures & progress_signatures)
                    )
                if not same_occurrence:
                    add_result_selection(
                        metric_id=metric_id,
                        fiscal_year=fiscal_year,
                        fiscal_quarter=fiscal_quarter,
                        role="progress",
                        selection=progress,
                    )

    for (metric_id, fiscal_year, fiscal_quarter, event_date), slot in sorted(
        result_slots.items()
    ):
        actual = slot["actual"]
        progress = slot["progress"]
        if actual is None and progress is None:
            continue
        period_id = f"period:anf:fy{fiscal_year}-q{fiscal_quarter}@1"
        unit_id = actual.unit_id if actual is not None else str(progress.unit_id)
        source_ids = tuple(
            sorted(
                set(() if actual is None else actual.source_document_ids)
                | set(() if progress is None else progress.source_document_ids)
            )
        )
        timeline_rows.append(
            ProductRowV2(
                row_id=(
                    f"pprow:v2.1|block=revision-timeline|period-result={period_id}"
                    f"|metric={metric_id}|date={event_date}"
                ),
                block_id=TIMELINE_BLOCK_ID,
                row_kind="period_result",
                eligible=True,
                group_id=f"pending-event:{event_date}",
                metric_id=metric_id,
                metric_label=METRIC_LABELS[metric_id],
                horizon_period_id=period_id,
                horizon_label=_foundation_period_display(period_id),
                current_value=None,
                current_display="",
                progression_values=(),
                previous_display="",
                actual_value=None if actual is None else actual.value,
                actual_display=(
                    "" if actual is None else display_value(actual.value, unit_id=actual.unit_id)
                ),
                actual_candidate_record_ids=(
                    () if actual is None else actual.candidate_record_ids
                ),
                actual_period_id=None if actual is None else actual.period_id,
                actual_knowledge_date=None if actual is None else actual.knowledge_date,
                actual_source_document_ids=(
                    () if actual is None else actual.source_document_ids
                ),
                progress_value=None if progress is None else progress.value,
                progress_display=(
                    ""
                    if progress is None
                    else f"YTD: {display_value(progress.value, unit_id=progress.unit_id)}"
                ),
                progress_candidate_record_ids=(
                    () if progress is None else progress.candidate_record_ids
                ),
                progress_period_id=None if progress is None else progress.period_id,
                progress_knowledge_date=(
                    None if progress is None else progress.knowledge_date
                ),
                progress_source_document_ids=(
                    () if progress is None else progress.source_document_ids
                ),
                version_state=None,
                status_code_at_update=None,
                status_at_update=None,
                change_type=None,
                comparison_reason_code=None,
                investor_reason_code=None,
                investor_reason_display="",
                event_id=f"pending-event:{event_date}",
                event_date=event_date,
                stated_in_period_id=f"period:anf:fy{fiscal_year}-q{fiscal_quarter}-results@1",
                stated_in_display=f"{fiscal_year}-Q{fiscal_quarter} results",
                current_source_document_ids=source_ids,
                predecessor_source_document_ids=(),
                source_summary=_foundation_source_summary(source_ids, documents),
                lineage_digest=_digest(
                    {
                        "foundation_id": EVIDENCE_FOUNDATION_ID,
                        "semantic_role": "period_result",
                        "actual_candidate_record_ids": (
                            () if actual is None else actual.candidate_record_ids
                        ),
                        "progress_candidate_record_ids": (
                            () if progress is None else progress.candidate_record_ids
                        ),
                    }
                ),
                actual_derivation_rule_id=(
                    None if actual is None else actual.derivation_rule_id
                ),
                actual_derivation_input_record_ids=(
                    () if actual is None else actual.derivation_input_record_ids
                ),
                actual_derivation_support_record_ids=(
                    () if actual is None else actual.derivation_support_record_ids
                ),
                progress_derivation_rule_id=(
                    None if progress is None else progress.derivation_rule_id
                ),
                progress_derivation_input_record_ids=(
                    () if progress is None else progress.derivation_input_record_ids
                ),
                progress_derivation_support_record_ids=(
                    () if progress is None else progress.derivation_support_record_ids
                ),
                unit_id=unit_id,
            )
        )

    for row in progression_rows:
        if row.actual_value is None or row.status_target_guidance_version_id is None:
            continue
        actual_sources = row.actual_source_document_ids
        target_sources = next(
            value.source_document_ids
            for value in row.progression_values
            if value.version_record_id == row.status_target_guidance_version_id
        )
        timeline_rows.append(
            ProductRowV2(
                row_id=f"pprow:v2.1|block=revision-timeline|annual-horizon-outcome={row.row_id}",
                block_id=TIMELINE_BLOCK_ID,
                row_kind="horizon_outcome",
                eligible=True,
                group_id=f"pending-event:{row.actual_knowledge_date}",
                metric_id=row.metric_id,
                metric_label=row.metric_label,
                horizon_period_id=row.horizon_period_id,
                horizon_label=row.horizon_label,
                current_value=None,
                current_display="",
                progression_values=(),
                previous_display="",
                actual_value=row.actual_value,
                actual_display=row.actual_display,
                actual_candidate_record_ids=row.actual_candidate_record_ids,
                actual_period_id=row.actual_period_id,
                actual_knowledge_date=row.actual_knowledge_date,
                actual_source_document_ids=actual_sources,
                progress_value=None,
                progress_display="",
                progress_candidate_record_ids=(),
                progress_period_id=None,
                progress_knowledge_date=None,
                progress_source_document_ids=(),
                version_state="Final",
                status_code_at_update=row.status_code_at_update,
                status_at_update=row.status_at_update,
                change_type=None,
                comparison_reason_code="horizon_outcome_replay",
                investor_reason_code=row.investor_reason_code,
                investor_reason_display=row.investor_reason_display,
                event_id=f"pending-event:{row.actual_knowledge_date}",
                event_date=str(row.actual_knowledge_date),
                stated_in_period_id=f"reporting-update:v2.1|date={row.actual_knowledge_date}",
                stated_in_display=str(row.actual_knowledge_date),
                current_source_document_ids=tuple(target_sources),
                predecessor_source_document_ids=(),
                source_summary=_foundation_source_summary(actual_sources, documents),
                lineage_digest=_digest(
                    {
                        "foundation_id": EVIDENCE_FOUNDATION_ID,
                        "semantic_role": "horizon_outcome",
                        "target_guidance_version_id": row.status_target_guidance_version_id,
                        "actual_candidate_record_ids": row.actual_candidate_record_ids,
                        "status_rule_id": row.status_rule_id,
                    }
                ),
                actual_derivation_rule_id=row.actual_derivation_rule_id,
                actual_derivation_input_record_ids=row.actual_derivation_input_record_ids,
                actual_derivation_support_record_ids=row.actual_derivation_support_record_ids,
                unit_id=row.unit_id,
                status_target_guidance_version_id=row.status_target_guidance_version_id,
                status_actual_candidate_record_ids=row.actual_candidate_record_ids,
                status_actual_period_id=row.actual_period_id,
                status_actual_knowledge_date=row.actual_knowledge_date,
                status_actual_source_document_ids=actual_sources,
                status_actual_basis_id=row.status_actual_basis_id,
                status_actual_unit_id=row.status_actual_unit_id,
                status_rule_id=row.status_rule_id,
            )
        )

    for series_id, versions in sorted(quarter_versions_by_series.items()):
        target = versions[-1]
        metric_id = str(target["metric_id"])
        period_id = str(target["horizon_period_id"])
        period = periods[period_id]
        if date.fromisoformat(str(period["end_date"])) >= date.fromisoformat(cutoff):
            continue
        fiscal_year = int(period["fiscal_year"])
        fiscal_quarter = int(period["fiscal_quarter"])
        actual = (
            _foundation_q4_selection(
                evidence_foundation,
                periods,
                metric_id=metric_id,
                fiscal_year=fiscal_year,
                as_of_date=cutoff,
            )
            if fiscal_quarter == 4
            else _foundation_actual_for_metric(
                evidence_foundation,
                metric_id=metric_id,
                period_id=period_id,
                as_of_date=cutoff,
            )
        )
        if actual is None:
            continue
        target_unit = _product_value_unit_id(
            target["canonical_value"], str(target["unit_id"])
        )
        target_kind = str(target["canonical_value"].get("kind"))
        if target_unit != actual.unit_id and target_kind != "qualitative":
            continue
        (
            status_code,
            status_label,
            reason_code,
            reason_display,
            status_rule_id,
        ) = _foundation_outcome_status(
            company_id=company_id,
            row_key=f"{series_id}|horizon-outcome",
            metric_id=metric_id,
            target_version_id=str(target["guidance_version_id"]),
            target_value=target["canonical_value"],
            target_period_id=period_id,
            actual=actual,
            event_date=actual.knowledge_date,
        )
        target_source = str(target["source_document_id"])
        timeline_rows.append(
            ProductRowV2(
                row_id=f"pprow:v2.1|block=revision-timeline|quarter-horizon-outcome={series_id}",
                block_id=TIMELINE_BLOCK_ID,
                row_kind="horizon_outcome",
                eligible=True,
                group_id=f"pending-event:{actual.knowledge_date}",
                metric_id=metric_id,
                metric_label=METRIC_LABELS[metric_id],
                horizon_period_id=period_id,
                horizon_label=_foundation_period_display(period_id),
                current_value=None,
                current_display="",
                progression_values=(),
                previous_display="",
                actual_value=actual.value,
                actual_display=display_value(actual.value, unit_id=actual.unit_id),
                actual_candidate_record_ids=actual.candidate_record_ids,
                actual_period_id=actual.period_id,
                actual_knowledge_date=actual.knowledge_date,
                actual_source_document_ids=actual.source_document_ids,
                progress_value=None,
                progress_display="",
                progress_candidate_record_ids=(),
                progress_period_id=None,
                progress_knowledge_date=None,
                progress_source_document_ids=(),
                version_state="Final",
                status_code_at_update=status_code,
                status_at_update=status_label,
                change_type=None,
                comparison_reason_code="horizon_outcome_replay",
                investor_reason_code=reason_code,
                investor_reason_display=reason_display,
                event_id=f"pending-event:{actual.knowledge_date}",
                event_date=actual.knowledge_date,
                stated_in_period_id=f"reporting-update:v2.1|date={actual.knowledge_date}",
                stated_in_display=actual.knowledge_date,
                current_source_document_ids=(target_source,),
                predecessor_source_document_ids=(),
                source_summary=_foundation_source_summary(
                    actual.source_document_ids, documents
                ),
                lineage_digest=_digest(
                    {
                        "foundation_id": EVIDENCE_FOUNDATION_ID,
                        "semantic_role": "horizon_outcome",
                        "target_guidance_version_id": target["guidance_version_id"],
                        "actual_candidate_record_ids": actual.candidate_record_ids,
                        "status_rule_id": status_rule_id,
                    }
                ),
                actual_derivation_rule_id=actual.derivation_rule_id,
                actual_derivation_input_record_ids=actual.derivation_input_record_ids,
                actual_derivation_support_record_ids=actual.derivation_support_record_ids,
                unit_id=(actual.unit_id if target_kind == "qualitative" else target_unit),
                status_target_guidance_version_id=str(target["guidance_version_id"]),
                status_actual_candidate_record_ids=actual.candidate_record_ids,
                status_actual_period_id=actual.period_id,
                status_actual_knowledge_date=actual.knowledge_date,
                status_actual_source_document_ids=actual.source_document_ids,
                status_actual_basis_id=actual.basis_id,
                status_actual_unit_id=actual.unit_id,
                status_rule_id=status_rule_id,
            )
        )

    context_by_date: dict[str, tuple[str, str]] = {}
    for version in quarter_versions:
        event_date = str(version["source_date"])
        context = (
            str(version["stated_in_period_id"]),
            _foundation_stated_display(str(version["stated_in_period_id"])),
        )
        prior = context_by_date.setdefault(event_date, context)
        if prior != context:
            raise PromiseProgressProductV2Error(
                f"Quarter guidance has conflicting event context on {event_date}."
            )
    for row in timeline_rows:
        if row.event_date is not None and row.stated_in_period_id is not None:
            context_by_date.setdefault(
                row.event_date,
                (row.stated_in_period_id, row.stated_in_display),
            )

    rows_by_date: dict[str, list[ProductRowV2]] = {}
    for row in timeline_rows:
        if row.event_date is None:
            raise PromiseProgressProductV2Error("A successor Timeline row lacks an event date.")
        rows_by_date.setdefault(row.event_date, []).append(row)
    normalized_timeline: list[ProductRowV2] = []
    disclosure_events: list[DisclosureEventV2] = []
    for event_date, event_rows in rows_by_date.items():
        source_ids = tuple(
            sorted(
                {
                    source_id
                    for row in event_rows
                    for source_id in (
                        row.current_source_document_ids
                        + row.actual_source_document_ids
                        + row.progress_source_document_ids
                        + row.status_actual_source_document_ids
                    )
                }
            )
        )
        if not source_ids:
            raise PromiseProgressProductV2Error(
                f"Disclosure event {event_date} has no source identity."
            )
        event_id = (
            f"disclosure-event:v2.1|date={event_date}|sources="
            f"{_digest(source_ids)[:20]}"
        )
        event_day_sources = [
            documents[source_id]
            for source_id in source_ids
            if source_id in documents
            and str(documents[source_id].get("publication_date")) == event_date
        ]
        sec_sources = [
            source
            for source in event_day_sources
            if source.get("form") in {"10-K", "10-Q"}
        ]
        context: tuple[str, str] | None = None
        if sec_sources:
            report_dates = {
                str(source.get("report_date"))
                for source in sec_sources
                if source.get("report_date")
            }
            matching_periods = [
                period
                for period in periods.values()
                if str(period.get("end_date")) in report_dates
                and period.get("period_type") in {"annual", "quarter"}
            ]
            if not matching_periods:
                raise PromiseProgressProductV2Error(
                    f"SEC disclosure event {event_date} has no typed reporting period."
                )
            reporting_period = max(
                matching_periods,
                key=lambda period: (
                    int(period["fiscal_year"]),
                    int(period.get("fiscal_quarter") or 0),
                ),
            )
            if reporting_period["period_type"] == "annual":
                fiscal_label = f"FY{reporting_period['fiscal_year']} annual SEC filing"
                phase = "annual-sec-filing"
            else:
                fiscal_label = (
                    f"{reporting_period['fiscal_year']}-Q"
                    f"{reporting_period['fiscal_quarter']} SEC filing"
                )
                phase = f"q{reporting_period['fiscal_quarter']}-sec-filing"
            context = (
                f"reporting-update:v2.1|target-fy={reporting_period['fiscal_year']}|phase={phase}",
                fiscal_label,
            )
        if context is None:
            context = context_by_date.get(event_date)
        if context is None:
            event_day = date.fromisoformat(event_date)
            quarter = (event_day.month - 1) // 3 + 1
            context = (
                f"reporting-update:v2.1|calendar={event_day.year}-q{quarter}",
                f"{event_day.year}-Q{quarter}",
            )
        disclosure_events.append(
            DisclosureEventV2(
                event_id=event_id,
                event_date=event_date,
                source_document_ids=source_ids,
                display_label=(
                    context[1]
                    if context[1].endswith("SEC filing")
                    else f"{context[1]} disclosures"
                ),
                reviewed_relation_ids=(),
            )
        )
        normalized_timeline.extend(
            dataclasses.replace(
                row,
                group_id=event_id,
                event_id=event_id,
                stated_in_period_id=context[0],
                stated_in_display=context[1],
            )
            for row in event_rows
        )

    open_rows.sort(
        key=lambda row: (
            0
            if row.horizon_period_id is not None
            and periods[row.horizon_period_id]["period_type"] == "annual"
            else 1,
            METRIC_ORDER.get(str(row.metric_id), 999),
            row.horizon_label,
            row.row_id,
        )
    )
    progression_rows.sort(
        key=lambda row: (
            -int(periods[str(row.horizon_period_id)]["fiscal_year"]),
            METRIC_ORDER.get(str(row.metric_id), 999),
            row.row_id,
        )
    )
    normalized_timeline.sort(
        key=lambda row: (
            -date.fromisoformat(str(row.event_date)).toordinal(),
            str(row.event_id),
            METRIC_ORDER.get(str(row.metric_id), 999),
            {"guidance_update": 0, "period_result": 1, "horizon_outcome": 2}.get(
                row.row_kind, 9
            ),
            row.row_id,
        )
    )
    disclosure_events.sort(
        key=lambda event: (-date.fromisoformat(event.event_date).toordinal(), event.event_id)
    )
    blocks = (
        base_blocks[CREDIBILITY_BLOCK_ID],
        ProductBlockV2(
            block_id=PROGRESSION_BLOCK_ID,
            title=base_blocks[PROGRESSION_BLOCK_ID].title,
            block_state="populated",
            rows=tuple(progression_rows),
        ),
        ProductBlockV2(
            block_id=OPEN_BLOCK_ID,
            title="2026 Open Guidance",
            block_state="populated",
            rows=tuple(open_rows),
        ),
        ProductBlockV2(
            block_id=TIMELINE_BLOCK_ID,
            title=base_blocks[TIMELINE_BLOCK_ID].title,
            block_state="populated",
            rows=tuple(normalized_timeline),
        ),
    )
    return PromiseProgressProductV2(
        product_type=PRODUCT_TYPE,
        product_version=SUCCESSOR_PRODUCT_VERSION,
        product_id=_product_id(company_id),
        company_id=company_id,
        knowledge_cutoff=cutoff,
        source_set_id=source_set_id,
        coverage_state="complete_for_reviewed_scope",
        coverage_notice=(
            "All reviewed Evidence Foundation guidance and Promise-relevant facts are "
            "projected or carry an explicit typed disposition."
        ),
        block_order=BLOCK_ORDER,
        disclosure_events=tuple(disclosure_events),
        blocks=blocks,
        ownership_statement=(
            "The reviewed Evidence Foundation owns evidence, source authority, and facts; "
            "Product@2.1 owns typed investor projection and workbook-safe row semantics."
        ),
    )



def build_product_v2_shadow(
    product: PromiseProgressProductV2,
    package: Mapping[str, Any],
    *,
    evidence_foundation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = [row for block in product.blocks for row in block.rows]
    row_lineage = []
    for row in rows:
        entry = {
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
        if product.product_version == SUCCESSOR_PRODUCT_VERSION:
            entry.update(
                {
                    "row_kind": row.row_kind,
                    "actual_derivation_rule_id": row.actual_derivation_rule_id,
                    "actual_derivation_input_record_ids": list(
                        row.actual_derivation_input_record_ids
                    ),
                    "actual_derivation_support_record_ids": list(
                        row.actual_derivation_support_record_ids
                    ),
                    "progress_derivation_rule_id": row.progress_derivation_rule_id,
                    "progress_derivation_input_record_ids": list(
                        row.progress_derivation_input_record_ids
                    ),
                    "progress_derivation_support_record_ids": list(
                        row.progress_derivation_support_record_ids
                    ),
                    "status_target_guidance_version_id": (
                        row.status_target_guidance_version_id
                    ),
                    "status_actual_candidate_record_ids": list(
                        row.status_actual_candidate_record_ids
                    ),
                    "status_actual_period_id": row.status_actual_period_id,
                    "status_actual_knowledge_date": row.status_actual_knowledge_date,
                    "status_actual_source_document_ids": list(
                        row.status_actual_source_document_ids
                    ),
                    "status_actual_basis_id": row.status_actual_basis_id,
                    "status_actual_unit_id": row.status_actual_unit_id,
                    "status_rule_id": row.status_rule_id,
                }
            )
        row_lineage.append(entry)
    result = {
        "shadow_type": "PromiseProgressProductShadow@2",
        "shadow_version": product.product_version,
        "product_id": product.product_id,
        "product_sha256": promise_progress_product_v2_sha256(product),
        "source_package_sha256": _digest(package),
        "row_lineage": row_lineage,
        "source_package": package,
    }
    if evidence_foundation is not None:
        result["evidence_foundation_id"] = evidence_foundation["foundation_id"]
        result["evidence_foundation_sha256"] = hashlib.sha256(
            serialize_package(evidence_foundation)
        ).hexdigest()
        result["evidence_foundation_semantic_digest"] = _digest(evidence_foundation)
        result["evidence_foundation"] = evidence_foundation
    return result


def serialize_product_v2_shadow(value: Mapping[str, Any]) -> bytes:
    return _canonical_json_bytes(value)
